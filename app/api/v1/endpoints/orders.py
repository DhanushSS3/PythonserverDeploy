# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, cast
import json
import uuid
import datetime
import time
from pydantic import BaseModel, Field, validator
from sqlalchemy import select
from fastapi.security import OAuth2PasswordBearer

from app.database.models import Group, ExternalSymbolInfo, User, DemoUser, UserOrder, DemoUserOrder, Wallet
from app.schemas.order import OrderUpdateRequest, OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate

from app.core.cache import (
    set_user_data_cache,
    set_user_portfolio_cache,
    DecimalEncoder,
    get_group_symbol_settings_cache,
    publish_account_structure_changed_event,
    get_user_portfolio_cache,
    get_user_data_cache,
    get_group_settings_cache,
)

from app.utils.validation import enforce_service_user_id_restriction
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution
)
from app.services.portfolio_calculator import _convert_to_usd, calculate_user_portfolio
from app.services.margin_calculator import calculate_single_order_margin, get_live_adjusted_buy_price_for_pair
from app.services.pending_orders import add_pending_order, remove_pending_order

from app.crud import crud_order, user as crud_user, group as crud_group
from app.crud.crud_order import OrderCreateInternal, get_order_model
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
from app.crud.group import get_all_symbols_for_group
from app.firebase_stream import get_latest_market_data
from app.core.security import get_user_from_service_or_user_token, get_current_user
from app.core.firebase import send_order_to_firebase
from app.core.logging_config import orders_logger

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

class OrderPlacementRequest(BaseModel):
    # Required fields
    symbol: str  # Corresponds to order_company_name
    order_type: str  # E.g., "MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT"
    order_quantity: Decimal = Field(..., gt=0)
    order_price: Decimal
    user_type: str  # "live" or "demo"
    user_id: int

    # Optional fields with defaults
    order_status: str = "OPEN"  # Default to OPEN for new orders
    status: str = "ACTIVE"  # Default to ACTIVE for new orders
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    contract_value: Optional[Decimal] = None
    margin: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    cancel_id: Optional[str] = None
    close_id: Optional[str] = None
    modify_id: Optional[str] = None
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    stoploss_cancel_id: Optional[str] = None
    takeprofit_cancel_id: Optional[str] = None

    @validator('order_type')
    def validate_order_type(cls, v):
        valid_types = ["MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]
        if v.upper() not in valid_types:
            raise ValueError(f"Invalid order type. Must be one of: {', '.join(valid_types)}")
        return v.upper()

    @validator('user_type')
    def validate_user_type(cls, v):
        valid_types = ["live", "demo"]
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid user type. Must be one of: {', '.join(valid_types)}")
        return v.lower()

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }

@router.post("/", response_model=OrderResponse)
async def place_order(
    order_request: OrderPlacementRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Place a new order.
    """
    try:
        orders_logger.info(f"Order placement request received - User ID: {current_user.id}, Symbol: {order_request.symbol}, Type: {order_request.order_type}, Quantity: {order_request.order_quantity}")
        
        # Convert order request to dict
        order_data = {
            'order_company_name': order_request.symbol,
            'order_type': order_request.order_type,
            'order_quantity': order_request.order_quantity,
            'stop_loss': order_request.stop_loss,
            'take_profit': order_request.take_profit
        }

        # Get user and group settings to determine if Barclays live user
        user_id_for_order = current_user.id
        user_data_cache = await get_user_data_cache(redis_client, user_id_for_order)
        group_name_cache = user_data_cache.get('group_name') if user_data_cache else None
        group_settings_cache = await get_group_settings_cache(redis_client, group_name_cache) if group_name_cache else None
        sending_orders_cache = group_settings_cache.get('sending_orders') if group_settings_cache else None

        is_barclays_live_user = (current_user.user_type == 'live' and sending_orders_cache == 'barclays')
        orders_logger.info(f"Order placement: user_id={user_id_for_order}, is_barclays_live_user={is_barclays_live_user}, user_type={current_user.user_type}, sending_orders_setting={sending_orders_cache}")

        # Prepare order data (all IDs, margin, etc.)
        order_create_internal = await process_new_order(
            db=db,
            redis_client=redis_client,
            user_id=user_id_for_order, # Use the defined user_id
            order_data=order_data,
            user_type=current_user.user_type,
            is_barclays_live_user=is_barclays_live_user # Pass the new flag
        )

        # Create order in database (OrderCreateInternal-compatible)
        order_model = get_order_model(current_user.user_type)
        db_order = await crud_order.create_order(db, order_create_internal, order_model)
        
        await db.commit()
        await db.refresh(db_order)

        # --- Margin update logic for non-barclays users ---
        user_id = db_order.order_user_id
        try:
            # user_data_cache, group_name_cache, sending_orders_cache were fetched earlier.
            # The flag is_barclays_live_user is also available.
            # user_id for logging/calculation here is db_order.order_user_id (which is the same as user_id_for_order)

            orders_logger.info(f"[MARGIN DEBUG] user_id={db_order.order_user_id}, group_name={group_name_cache}, sending_orders={sending_orders_cache}, user_type={current_user.user_type}, is_barclays_live_user_flag_check={is_barclays_live_user}")

            # Only update margin if NOT live/barclays (using the pre-calculated flag)
            if not is_barclays_live_user:
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                orders_logger.info(f"[MARGIN DEBUG] open_positions count: {len(open_positions)}")
                symbols = set(getattr(pos, 'order_company_name', None) for pos in open_positions)
                orders_logger.info(f"[MARGIN DEBUG] symbols found: {symbols}")
                total_margin = 0
                from app.services.margin_calculator import calculate_total_symbol_margin_contribution
                for symbol in symbols:
                    symbol_positions = [pos for pos in open_positions if getattr(pos, 'order_company_name', None) == symbol]
                    orders_logger.info(f"[MARGIN DEBUG] Calculating margin for symbol: {symbol} with {len(symbol_positions)} positions")
                    margin = await calculate_total_symbol_margin_contribution(db, redis_client, user_id, symbol, symbol_positions, order_model)
                    orders_logger.info(f"[MARGIN DEBUG] Margin for symbol {symbol}: {margin}")
                    total_margin += float(margin)
                orders_logger.info(f"[MARGIN DEBUG] Total margin to update for user {user_id}: {total_margin}")
                from app.crud.user import update_user_margin
                await update_user_margin(db, user_id, current_user.user_type, total_margin)
                orders_logger.info(f"[MARGIN DEBUG] Margin updated in DB for user {user_id}")
        except Exception as margin_exc:
            orders_logger.error(f"[MARGIN DEBUG] Exception during margin calculation/update for user {user_id}: {margin_exc}", exc_info=True)

        # --- Portfolio Update & Websocket Event ---
        try:
            user_id = db_order.order_user_id
            user_data = await get_user_data_cache(redis_client, user_id)
            if user_data:
                group_name = user_data.get('group_name')
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol in group_symbol_settings.keys():
                        prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                        if prices:
                            adjusted_market_prices[symbol] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after placing order.")
        except Exception as e:
            orders_logger.error(f"Error updating portfolio cache or publishing websocket event after order placement: {e}", exc_info=True)

        return OrderResponse(
            order_id=db_order.order_id,
            order_status=db_order.order_status,
            order_user_id=db_order.order_user_id,
            order_company_name=db_order.order_company_name,
            order_type=db_order.order_type,
            order_price=db_order.order_price,
            order_quantity=db_order.order_quantity,
            contract_value=db_order.contract_value,
            margin=db_order.margin,
            stop_loss=db_order.stop_loss,
            take_profit=db_order.take_profit
        )

    except OrderProcessingError as e:
        orders_logger.error(f"Order processing error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        orders_logger.error(f"Unexpected error in place_order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process order: {str(e)}")

@router.post("/close", response_model=OrderResponse)
async def close_order(
    close_request: CloseOrderRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token),
    token: str = Depends(oauth2_scheme)
):
    """
    Close an open order, updates its status to 'CLOSED', and adjusts the user's overall margin.
    """
    try:
        orders_logger.info(f"Close order request received - Order ID: {close_request.order_id}, User ID: {current_user.id}, User Type: {current_user.user_type}")
        orders_logger.info(f"Close request details - Price: {close_request.close_price}, Type: {close_request.order_type}, Symbol: {close_request.order_company_name}")

        target_user_id_to_operate_on = current_user.id
        user_to_operate_on = current_user

        if close_request.user_id is not None:
            is_service_account = getattr(current_user, 'is_service_account', False)
            if is_service_account:
                orders_logger.info(f"Service account operation - Target user ID: {close_request.user_id}")
                enforce_service_user_id_restriction(close_request.user_id, token)
                _user = await crud_user.get_user_by_id(db, close_request.user_id)
                if _user:
                    user_to_operate_on = _user
                else:
                    _demo_user = await crud_user.get_demo_user_by_id(db, close_request.user_id)
                    if _demo_user:
                        user_to_operate_on = _demo_user
                    else:
                        orders_logger.error(f"Target user not found for service operation - User ID: {close_request.user_id}")
                        raise HTTPException(status_code=404, detail="Target user not found for service op.")
                target_user_id_to_operate_on = close_request.user_id
            else:
                if close_request.user_id != current_user.id:
                    orders_logger.error(f"Unauthorized user_id specification - Current user: {current_user.id}, Requested user: {close_request.user_id}")
                    raise HTTPException(status_code=403, detail="Not authorized to specify user_id.")
        
        order_model_class = get_order_model(user_to_operate_on)
        order_id = close_request.order_id

        try:
            close_price = Decimal(str(close_request.close_price))
            if close_price <= Decimal("0"):
                raise HTTPException(status_code=400, detail="Close price must be positive.")
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="Invalid close price format.")

        orders_logger.info(f"Request to close order {order_id} for user {user_to_operate_on.id} ({type(user_to_operate_on).__name__}) with price {close_price}. Frontend provided type: {close_request.order_type}, company: {close_request.order_company_name}, status: {close_request.order_status}, frontend_status: {close_request.status}.")

        # Generate a unique 10-digit random close_order_id
        close_order_id = generate_10_digit_id() # Generate close_order_id

        try:
            if isinstance(user_to_operate_on, User):
                user_group = await crud_group.get_group_by_name(db, user_to_operate_on.group_name)
                if user_group and user_group[0].sending_orders == "barclays": # Fixed: user_group is a list, access first element
                    orders_logger.info(f"Live user {user_to_operate_on.id} from group '{user_group[0].group_name}' has 'sending_orders' set to 'barclays'. Pushing close request to Firebase and skipping local DB update.")
                    
                    firebase_close_data = {
                        "order_id": close_request.order_id,
                        "close_price": str(close_request.close_price),
                        "user_id": user_to_operate_on.id,
                        "order_type": close_request.order_type,
                        "order_company_name": close_request.order_company_name,
                        "order_status": close_request.order_status,
                        "status": close_request.status,
                        "action": "close_order",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "close_order_id": close_order_id # Include close_order_id for Firebase
                    }
                    background_tasks.add_task(send_order_to_firebase, firebase_close_data, "live")
                    
                    db_order_for_response = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                    if db_order_for_response:
                        db_order_for_response.order_status = "PENDING_CLOSE"
                        db_order_for_response.close_message = "Order sent to service provider for closure."
                        db_order_for_response.close_order_id = close_order_id # Save close_order_id in DB
                        await db.commit()
                        await db.refresh(db_order_for_response)
                        
                        # Log action in OrderActionHistory
                        user_type_str = "live" if isinstance(user_to_operate_on, User) else "demo"
                        update_fields_for_history = OrderUpdateRequest(
                            order_status="PENDING_CLOSE",
                            close_message="Order sent to service provider for closure.",
                            close_id=close_order_id, # Log the close_order_id here
                            close_price=close_price, # Log the requested close price
                        ).model_dump(exclude_unset=True)
                        await crud_order.update_order_with_tracking(
                            db,
                            db_order_for_response,
                            update_fields_for_history,
                            user_id=user_to_operate_on.id,
                            user_type=user_type_str,
                            action_type="CLOSE_REQUESTED" # New action type for history
                        )
                        await db.commit() # Commit history tracking
                        await db.refresh(db_order_for_response)
                        return OrderResponse.model_validate(db_order_for_response)
                    else:
                        raise HTTPException(status_code=404, detail="Order not found for external closure processing.")
                else:
                    orders_logger.info(f"Live user {user_to_operate_on.id} from group '{user_group[0].group_name if user_group else 'default'}' ('sending_orders' is NOT 'barclays'). Processing close locally.") # Fixed: user_group might be empty
                    async with db.begin_nested():
                        db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                        if db_order is None:
                            raise HTTPException(status_code=404, detail="Order not found.")
                        if db_order.order_user_id != user_to_operate_on.id and not getattr(current_user, 'is_admin', False):
                            raise HTTPException(status_code=403, detail="Not authorized to close this order.")
                        if db_order.order_status != 'OPEN':
                            raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

                        order_symbol = db_order.order_company_name.upper()
                        quantity = Decimal(str(db_order.order_quantity))
                        entry_price = Decimal(str(db_order.order_price))
                        order_type_db = db_order.order_type.upper()
                        user_group_name = getattr(user_to_operate_on, 'group_name', 'default')

                        db_user_locked = await get_user_by_id_with_lock_fn(db, user_to_operate_on.id, model_class=type(user_to_operate_on))
                        if db_user_locked is None:
                            orders_logger.error(f"Could not retrieve and lock user record for user ID: {user_to_operate_on.id}")
                            raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")
                        
                        all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                            db=db, user_id=db_user_locked.id, symbol=order_symbol, order_model=order_model_class
                        )
                        margin_before_recalc = await calculate_total_symbol_margin_contribution(
                            db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                            open_positions_for_symbol=all_open_orders_for_symbol, order_model_for_calc=order_model_class
                        )
                        current_overall_margin = Decimal(str(db_user_locked.margin))
                        non_symbol_margin = current_overall_margin - margin_before_recalc
                        remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
                        margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution(
                            db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                            open_positions_for_symbol=remaining_orders_for_symbol_after_close, order_model_for_calc=order_model_class
                        )
                        db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                        
                        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
                        symbol_info_result = await db.execute(symbol_info_stmt)
                        ext_symbol_info = symbol_info_result.scalars().first()
                        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                            raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
                        contract_size = Decimal(str(ext_symbol_info.contract_size))
                        profit_currency = ext_symbol_info.profit.upper()

                        group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
                        if not group_settings:
                            raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
                        
                        commission_type = int(group_settings.get('commision_type', -1))
                        commission_value_type = int(group_settings.get('commision_value_type', -1))
                        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                        exit_commission = Decimal("0.0")
                        if commission_type in [0, 2]:
                            if commission_value_type == 0: exit_commission = quantity * commission_rate
                            elif commission_value_type == 1:
                                calculated_exit_contract_value = quantity * contract_size * close_price
                                if calculated_exit_contract_value > Decimal("0.0"):
                                    exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                        entry_commission_for_total = Decimal("0.0")
                        if commission_type in [0, 1]:
                            if commission_value_type == 0: entry_commission_for_total = quantity * commission_rate
                            elif commission_value_type == 1:
                                initial_contract_value = quantity * contract_size * entry_price
                                if initial_contract_value > Decimal("0.0"):
                                    entry_commission_for_total = (commission_rate / Decimal("100")) * initial_contract_value
                        total_commission_for_trade = (entry_commission_for_total + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

                        if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
                        elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
                        else: raise HTTPException(status_code=500, detail="Invalid order type.")
                        
                        profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db) 
                        if profit_currency != "USD" and profit_usd == profit: 
                            orders_logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                            raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

                        db_order.order_status = "CLOSED"
                        db_order.close_price = close_price
                        db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        db_order.swap = db_order.swap or Decimal("0.0")
                        db_order.commission = total_commission_for_trade
                        db_order.close_order_id = close_order_id # Save close_order_id in DB

                        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
                        swap_amount = db_order.swap
                        db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

                        transaction_time = datetime.datetime.now(datetime.timezone.utc)
                        wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time}
                        if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
                        else: wallet_common_data["user_id"] = db_user_locked.id
                        if db_order.net_profit != Decimal("0.0"):
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                        if total_commission_for_trade > Decimal("0.0"):
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                        if swap_amount != Decimal("0.0"):
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))

                        await db.commit()
                        await db.refresh(db_order)

                        # --- Portfolio Update & Websocket Event ---
                        try:
                            user_id = db_order.order_user_id
                            user_data = await get_user_data_cache(redis_client, user_id)
                            if user_data:
                                group_name = user_data.get('group_name')
                                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model_class)
                                open_positions_dicts = [
                                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                                    for pos in open_positions
                                ]
                                adjusted_market_prices = {}
                                if group_symbol_settings:
                                    for symbol in group_symbol_settings.keys():
                                        prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                                        if prices:
                                            adjusted_market_prices[symbol] = prices
                                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                                await publish_account_structure_changed_event(redis_client, user_id)
                                orders_logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after closing order.")
                        except Exception as e:
                            orders_logger.error(f"Error updating portfolio cache or publishing websocket event after order close: {e}", exc_info=True)

                        return OrderResponse.model_validate(db_order)
            else:
                # Handle demo user case
                async with db.begin_nested():
                    db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                    if db_order is None:
                        raise HTTPException(status_code=404, detail="Order not found.")
                    if db_order.order_user_id != user_to_operate_on.id:
                        raise HTTPException(status_code=403, detail="Not authorized to close this order.")
                    if db_order.order_status != 'OPEN':
                        raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

                    order_symbol = db_order.order_company_name.upper()
                    quantity = Decimal(str(db_order.order_quantity))
                    entry_price = Decimal(str(db_order.order_price))
                    order_type_db = db_order.order_type.upper()

                    db_user_locked = await get_user_by_id_with_lock_fn(db, user_to_operate_on.id, model_class=type(user_to_operate_on))
                    if db_user_locked is None:
                        orders_logger.error(f"Could not retrieve and lock user record for user ID: {user_to_operate_on.id}")
                        raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

                    all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                        db=db, user_id=db_user_locked.id, symbol=order_symbol, order_model=order_model_class
                    )
                    margin_before_recalc = await calculate_total_symbol_margin_contribution(
                        db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                        open_positions_for_symbol=all_open_orders_for_symbol, order_model_for_calc=order_model_class
                    )
                    current_overall_margin = Decimal(str(db_user_locked.margin))
                    non_symbol_margin = current_overall_margin - margin_before_recalc
                    remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
                    margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution(
                        db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                        open_positions_for_symbol=remaining_orders_for_symbol_after_close, order_model_for_calc=order_model_class
                    )
                    db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

                    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
                    symbol_info_result = await db.execute(symbol_info_stmt)
                    ext_symbol_info = symbol_info_result.scalars().first()
                    if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                        raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
                    contract_size = Decimal(str(ext_symbol_info.contract_size))
                    profit_currency = ext_symbol_info.profit.upper()

                    group_settings = await get_group_symbol_settings_cache(redis_client, user_to_operate_on.group_name, order_symbol)
                    if not group_settings:
                        raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
                    
                    commission_type = int(group_settings.get('commision_type', -1))
                    commission_value_type = int(group_settings.get('commision_value_type', -1))
                    commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                    exit_commission = Decimal("0.0")
                    if commission_type in [0, 2]:
                        if commission_value_type == 0: exit_commission = quantity * commission_rate
                        elif commission_value_type == 1:
                            calculated_exit_contract_value = quantity * contract_size * close_price
                            if calculated_exit_contract_value > Decimal("0.0"):
                                exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                    entry_commission_for_total = Decimal("0.0")
                    if commission_type in [0, 1]:
                        if commission_value_type == 0: entry_commission_for_total = quantity * commission_rate
                        elif commission_value_type == 1:
                            initial_contract_value = quantity * contract_size * entry_price
                            if initial_contract_value > Decimal("0.0"):
                                entry_commission_for_total = (commission_rate / Decimal("100")) * initial_contract_value
                    total_commission_for_trade = (entry_commission_for_total + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

                    if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
                    elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
                    else: raise HTTPException(status_code=500, detail="Invalid order type.")
                    
                    profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db) 
                    if profit_currency != "USD" and profit_usd == profit: 
                        orders_logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                        raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

                    db_order.order_status = "CLOSED"
                    db_order.close_price = close_price
                    db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    db_order.swap = db_order.swap or Decimal("0.0")
                    db_order.commission = total_commission_for_trade
                    db_order.close_order_id = close_order_id # Save close_order_id in DB

                    original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
                    swap_amount = db_order.swap
                    db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

                    transaction_time = datetime.datetime.now(datetime.timezone.utc)
                    wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time}
                    if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
                    else: wallet_common_data["user_id"] = db_user_locked.id
                    if db_order.net_profit != Decimal("0.0"):
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                    if total_commission_for_trade > Decimal("0.0"):
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                    if swap_amount != Decimal("0.0"):
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))

                    await db.commit()
                    await db.refresh(db_order)
                    return OrderResponse.model_validate(db_order)
        except Exception as e:
            orders_logger.error(f"Error processing close order: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error processing close order: {str(e)}")
    except Exception as e:
        orders_logger.error(f"Error in close_order endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error in close_order endpoint: {str(e)}")

@router.patch("/{order_id}", response_model=OrderResponse)
async def update_order(
    order_id: str,
    order_update: OrderUpdateRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    """
    Update an order's status and details. Used by service provider to update order status.
    Can identify orders by order_id, cancel_id, close_id, stoploss_id, takeprofit_id,
    stoploss_cancel_id, or takeprofit_cancel_id.
    """
    try:
        # Try to find the order using various IDs
        order = None
        order_model = UserOrder  # Default to UserOrder

        # First try with order_id
        order = await crud_order.get_order_by_id(db, order_id, order_model)
        
        # If not found, try with other IDs
        if not order:
            # Try with cancel_id
            order = await crud_order.get_order_by_cancel_id(db, order_id, order_model)
        
        if not order:
            # Try with close_id
            order = await crud_order.get_order_by_close_id(db, order_id, order_model)
        
        if not order:
            # Try with stoploss_id
            order = await crud_order.get_order_by_stoploss_id(db, order_id, order_model)
        
        if not order:
            # Try with takeprofit_id
            order = await crud_order.get_order_by_takeprofit_id(db, order_id, order_model)
        
        if not order:
            # Try with stoploss_cancel_id
            order = await crud_order.get_order_by_stoploss_cancel_id(db, order_id, order_model)
        
        if not order:
            # Try with takeprofit_cancel_id
            order = await crud_order.get_order_by_takeprofit_cancel_id(db, order_id, order_model)

        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        # Get user data and group settings
        user_data = await get_user_data_cache(order.order_user_id)
        if not user_data:
            raise HTTPException(status_code=404, detail="User data not found")

        group_settings = await get_group_settings_cache(user_data.get('group_name'))
        if not group_settings:
            raise HTTPException(status_code=404, detail="Group settings not found")

        # Verify user is from Barclays group
        if group_settings.get('sending_orders') != 'barclays':
            raise HTTPException(status_code=403, detail="Only Barclays orders can be updated through this endpoint")

        # Update order status and details
        old_status = order.order_status
        if order_update.order_status:
            order.order_status = order_update.order_status
        
        if order_update.order_price:
            order.order_price = order_update.order_price
        if order_update.margin:
            order.margin = order_update.margin
        if order_update.profit_loss:
            order.profit_loss = order_update.profit_loss
        if order_update.close_price:
            order.close_price = order_update.close_price
        if order_update.close_time:
            order.close_time = datetime.fromisoformat(order_update.close_time)
        if order_update.net_profit:
            order.net_profit = order_update.net_profit
        if order_update.commission:
            order.commission = order_update.commission
        if order_update.swap:
            order.swap = order_update.swap
        if order_update.cancel_message:
            order.cancel_message = order_update.cancel_message
        if order_update.close_message:
            order.close_message = order_update.close_message

        # Margin recalculation logic, especially for Barclays live users moving from PROCESSING
        if old_status == "PROCESSING" and order_update.order_status and order_update.order_status not in ["PROCESSING", "CANCELLED", "REJECTED"]:
            # This order is now confirmed (e.g., OPEN, EXECUTED)
            target_user_id = order.order_user_id
            # We need to fetch the user's actual type (live/demo) to determine the model and if they are a live Barclays user.
            # The current_user in this endpoint is the service user, not the end-user of the order.
            # Let's fetch the end-user details.
            from app.crud.user import get_user_by_id # Assuming a function to get full user details
            end_user = await get_user_by_id(db, target_user_id) # This might need adjustment based on your CRUD setup
            
            if end_user:
                user_type_of_order_owner = end_user.user_type
                user_data_for_margin_calc = await get_user_data_cache(redis_client, target_user_id) # Re-fetch for safety or use existing if available
                group_name_for_margin_calc = user_data_for_margin_calc.get('group_name') if user_data_for_margin_calc else None
                group_settings_for_margin_calc = await get_group_settings_cache(redis_client, group_name_for_margin_calc) if group_name_for_margin_calc else None
                sending_orders_for_margin_calc = group_settings_for_margin_calc.get('sending_orders') if group_settings_for_margin_calc else None

                is_barclays_live_user_order_confirmed = (user_type_of_order_owner == 'live' and sending_orders_for_margin_calc == 'barclays')
                orders_logger.info(f"[MARGIN UPDATE - UPDATE_ORDER] Order {order.order_id} confirmed for user {target_user_id}. Is Barclays Live: {is_barclays_live_user_order_confirmed}")

                if is_barclays_live_user_order_confirmed:
                    orders_logger.info(f"[MARGIN UPDATE - UPDATE_ORDER] Triggering margin recalculation for Barclays live user {target_user_id} after order confirmation.")
                    order_model_for_margin = get_order_model(user_type_of_order_owner)
                    open_positions = await crud_order.get_all_open_orders_by_user_id(db, target_user_id, order_model_for_margin)
                    symbols = set(getattr(pos, 'order_company_name', None) for pos in open_positions)
                    total_calculated_margin = Decimal('0.0')
                    from app.services.margin_calculator import calculate_total_symbol_margin_contribution
                    
                    for symbol_name in symbols:
                        symbol_positions = [pos for pos in open_positions if getattr(pos, 'order_company_name', None) == symbol_name]
                        margin_for_symbol = await calculate_total_symbol_margin_contribution(db, redis_client, target_user_id, symbol_name, symbol_positions, order_model_for_margin)
                        total_calculated_margin += Decimal(str(margin_for_symbol)) # Ensure Decimal conversion
                    
                    from app.crud.user import update_user_margin
                    await update_user_margin(db, target_user_id, user_type_of_order_owner, float(total_calculated_margin)) # update_user_margin expects float
                    orders_logger.info(f"[MARGIN UPDATE - UPDATE_ORDER] Margin updated to {total_calculated_margin} for user {target_user_id}.")
                else:
                    orders_logger.info(f"[MARGIN UPDATE - UPDATE_ORDER] Order {order.order_id} confirmed for non-Barclays live user or demo user {target_user_id}. Standard portfolio update will occur.")
            else:
                orders_logger.error(f"[MARGIN UPDATE - UPDATE_ORDER] Could not find user {target_user_id} to perform margin update.")
        
        # Existing portfolio update logic (can run for all users after status change)
        # This part might need review to ensure it doesn't conflict with the specific Barclays logic above
        # For now, let it run, but be mindful of potential double updates or incorrect calculations if not handled carefully.
        if old_status != order_update.order_status: # General status change handling
            # Recalculate portfolio metrics (this is a broader update including P/L, equity etc.)
            # The user_data here is from the service user, which is incorrect for portfolio metrics of the order's user.
            # We should use order.order_user_id to fetch the correct user_data for portfolio calculations.
            portfolio_user_id = order.order_user_id
            portfolio_user_data = await get_user_data_cache(redis_client, portfolio_user_id)
            if portfolio_user_data:
                portfolio = await get_user_portfolio_cache(redis_client, portfolio_user_id) # Changed from get_portfolio_cache
                if portfolio: # Check if portfolio exists
                    updated_portfolio = await calculate_user_portfolio(
                        user_data=portfolio_user_data, # Use correct user_data
                        open_positions_dicts=[], # This needs to be populated correctly if we want a full recalc here
                        adjusted_market_prices={}, # This needs to be populated correctly
                        group_settings={}, # This needs to be populated correctly
                        redis_client=redis_client
                    )
                    # The calculate_user_portfolio and its dependencies need careful review for this context.
                    # For now, focusing on the Barclays margin update. This section may need further refinement.
                    # await set_user_portfolio_cache(redis_client, portfolio_user_id, updated_portfolio) # Changed from update_portfolio_cache
                    orders_logger.info(f"[PORTFOLIO_UPDATE - UPDATE_ORDER] Portfolio update logic triggered for user {portfolio_user_id}. Needs review for correctness.")
            else:
                orders_logger.warning(f"[PORTFOLIO_UPDATE - UPDATE_ORDER] User data not found for user {portfolio_user_id}, skipping portfolio metrics update.")

        # Save changes
        await db.commit()
        await db.refresh(order)

        # Log the update in OrderActionHistory
        update_fields_for_history = order_update.model_dump(exclude_unset=True)
        await crud_order.update_order_with_tracking(
            db,
            order,
            update_fields_for_history,
            user_id=order.order_user_id,
            user_type="live",
            action_type=f"ORDER_{order_update.order_status}" if order_update.order_status else "ORDER_UPDATED"
        )
        await db.commit()

        # --- Portfolio Update & Websocket Event ---
        try:
            user_id = order.order_user_id
            user_data = await get_user_data_cache(redis_client, user_id)
            if user_data:
                group_name = user_data.get('group_name')
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, type(order))
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol in group_symbol_settings.keys():
                        prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                        if prices:
                            adjusted_market_prices[symbol] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after updating order.")
        except Exception as e:
            orders_logger.error(f"Error updating portfolio cache or publishing websocket event after order update: {e}", exc_info=True)
        return order

    except Exception as e:
        orders_logger.error(f"Error updating order {order_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pending", response_model=OrderResponse)
async def create_pending_order(
    order_request: OrderPlacementRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
):
    """
    Create a pending order (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP).
    For Barclays users, the order will be sent to Firebase immediately.
    For other users, the order will be stored in Redis cache and executed when triggered.
    """
    orders_logger.info(f"Received pending order request for symbol {order_request.symbol}, type: {order_request.order_type}")

    if order_request.order_type not in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid order type for pending order. Must be one of: BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP"
        )

    if order_request.order_quantity <= 0:
        raise HTTPException(status_code=400, detail="Order quantity must be positive.")

    # Initialize target_user to None to satisfy Pylance
    target_user: User | DemoUser | None = None

    if order_request.user_id is not None and hasattr(current_user, 'is_service_account') and current_user.is_service_account:
        orders_logger.info(f"Service account {current_user.id} placing pending order for target user_id {order_request.user_id}")
        if order_request.user_type == "live":
            fetched_user = await crud_user.get_user_by_id(db, order_request.user_id, user_type="live")
            if not fetched_user:
                raise HTTPException(status_code=404, detail=f"Target live user {order_request.user_id} not found.")
            target_user = fetched_user
        elif order_request.user_type == "demo":
            fetched_demo_user = await crud_user.get_demo_user_by_id(db, order_request.user_id, user_type="demo")
            if not fetched_demo_user:
                raise HTTPException(status_code=404, detail=f"Target demo user {order_request.user_id} not found.")
            target_user = fetched_demo_user
        else:
            raise HTTPException(status_code=400, detail=f"Invalid user_type '{order_request.user_type}' for service account operation.")
    else:
        target_user = current_user
        if (order_request.user_type == "live" and not isinstance(target_user, User)) or \
           (order_request.user_type == "demo" and not isinstance(target_user, DemoUser)):
            raise HTTPException(status_code=400, detail=f"Mismatch between requested user_type '{order_request.user_type}' and authenticated user type.")

    if target_user is None:
        orders_logger.error("Critical internal error: target_user was not assigned prior to use.")
        raise HTTPException(status_code=500, detail="Internal server error processing user context.")

    try:
        # Create the order in database with PENDING status
        order_model_class = get_order_model(target_user)
        
        # Calculate initial margin and contract value
        margin, adjusted_price, contract_value = await calculate_single_order_margin(
            db=db,
            redis_client=redis_client,
            user_id=target_user.id,
            order_quantity=order_request.order_quantity,
            order_price=order_request.order_price,
            symbol=order_request.symbol,
            order_type=order_request.order_type
        )

        # Create order data
        order_data_internal = OrderCreateInternal(
            order_id=generate_10_digit_id(),
            order_user_id=target_user.id,
            order_company_name=order_request.symbol,
            order_type=order_request.order_type,
            order_status="PENDING",
            order_price=order_request.order_price,
            order_quantity=order_request.order_quantity,
            contract_value=contract_value,
            margin=margin,
            stop_loss=order_request.stop_loss,
            take_profit=order_request.take_profit
        )

        # Create order in database
        new_db_order = await crud_order.create_order(
            db=db,
            order_data=order_data_internal.model_dump(),
            order_model=order_model_class
        )

        # For Barclays users, send to Firebase immediately
        if isinstance(target_user, User):
            group = await crud_group.get_group_by_name(db, target_user.group_name)
            if group and group[0].sending_orders == "barclays":
                background_tasks.add_task(send_order_to_firebase, new_db_order, account_type="live")
                orders_logger.info(f"Sent pending order {new_db_order.order_id} to Firebase for Barclays user {target_user.id}")
            else:
                # For non-Barclays live users, add to Redis cache
                order_dict = {
                    'order_id': new_db_order.order_id,
                    'order_user_id': new_db_order.order_user_id,
                    'order_company_name': new_db_order.order_company_name,
                    'order_type': new_db_order.order_type,
                    'order_price': new_db_order.order_price,
                    'order_quantity': new_db_order.order_quantity,
                    'margin': new_db_order.margin,
                    'contract_value': new_db_order.contract_value,
                    'stop_loss': new_db_order.stop_loss,
                    'take_profit': new_db_order.take_profit,
                    'user_type': order_request.user_type
                }
                await add_pending_order(order_dict)
        else:
            # For demo users, add to Redis cache
            order_dict = {
                'order_id': new_db_order.order_id,
                'order_user_id': new_db_order.order_user_id,
                'order_company_name': new_db_order.order_company_name,
                'order_type': new_db_order.order_type,
                'order_price': new_db_order.order_price,
                'order_quantity': new_db_order.order_quantity,
                'margin': new_db_order.margin,
                'contract_value': new_db_order.contract_value,
                'stop_loss': new_db_order.stop_loss,
                'take_profit': new_db_order.take_profit,
                'user_type': order_request.user_type
            }
            await add_pending_order(order_dict)

        # Update user portfolio cache
        user_portfolio = await get_user_portfolio_cache(redis_client, target_user.id)
        if user_portfolio is None:
            user_portfolio = {
                "balance": str(target_user.wallet_balance),
                "equity": str(target_user.wallet_balance),
                "margin": str(target_user.margin),
                "free_margin": str(target_user.wallet_balance - target_user.margin),
                "profit_loss": "0.0",
                "positions": [],
                "pending_positions": [],
                "processing_positions": []
            }

        # Add to pending_positions
        pending_order_dict = {
            k: str(v) if isinstance(v := getattr(new_db_order, k), Decimal) else v
            for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                     'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']
        }
        pending_order_dict['order_status'] = 'PENDING'
        user_portfolio['pending_positions'].append(pending_order_dict)

        await set_user_portfolio_cache(redis_client, target_user.id, user_portfolio)
        await publish_account_structure_changed_event(redis_client, target_user.id)

        return OrderResponse.model_validate(new_db_order, from_attributes=True)

    except Exception as e:
        orders_logger.error(f"Error creating pending order: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))