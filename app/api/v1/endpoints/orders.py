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
from app.schemas.order import OrderUpdateRequest, OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest, PendingOrderPlacementRequest
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
    get_last_known_price, # Added import
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
            'order_price': order_request.order_price,
            'user_type': order_request.user_type,
            'status': order_request.status,
            'stop_loss': order_request.stop_loss,
            'take_profit': order_request.take_profit
        }

        # Get user and group settings to determine if Barclays live user
        user_id_for_order = current_user.id
        # Use user_type from request (not just current_user)
        user_type = order_request.user_type.lower() if hasattr(order_request, 'user_type') else current_user.user_type
        user_data_cache = await get_user_data_cache(redis_client, user_id_for_order, db, user_type)
        group_name = user_data_cache.get('group_name') if user_data_cache else None
        # Fallback: If group_name is missing, fetch from DB
        if not group_name:
            from app.crud import user as crud_user
            db_user = await crud_user.get_user_by_id(db, user_id_for_order, user_type=user_type)
            group_name = getattr(db_user, 'group_name', None) if db_user else None
        group_settings_cache = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders_cache = group_settings_cache.get('sending_orders') if group_settings_cache else None

        # If not found in cache, fetch group settings from DB
        if (group_settings_cache is None or sending_orders_cache is None) and group_name:
            from app.crud import group as crud_group
            db_group_result = await crud_group.get_group_by_name(db, group_name)
            orders_logger.info(f"[DEBUG] db_group_result fetched from DB: {db_group_result}")
            sending_orders_extracted = None
            if db_group_result:
                # If it's a list (multiple group-symbol records), extract from the first
                if isinstance(db_group_result, list):
                    orders_logger.info(f"[DEBUG] Number of group records found: {len(db_group_result)}")
                    if len(db_group_result) > 0 and hasattr(db_group_result[0], 'sending_orders'):
                        sending_orders_extracted = getattr(db_group_result[0], 'sending_orders', None)
                # If it's a single record
                elif hasattr(db_group_result, 'sending_orders'):
                    sending_orders_extracted = getattr(db_group_result, 'sending_orders', None)
            if sending_orders_extracted is not None:
                sending_orders_cache = sending_orders_extracted
                orders_logger.info(f"[DEBUG] sending_orders_cache extracted from DB: {sending_orders_cache}")
            else:
                orders_logger.warning(f"[WARNING] Group '{group_name}' not found in DB or missing 'sending_orders' attribute. db_group_result={db_group_result}")

        # Normalize for robust comparison
        sending_orders_normalized = sending_orders_cache.lower() if isinstance(sending_orders_cache, str) else sending_orders_cache
        orders_logger.info(f"[DEBUG] group_settings_cache: {group_settings_cache}")
        orders_logger.info(f"[DEBUG] sending_orders_cache value: {sending_orders_cache} (type: {type(sending_orders_cache)})")
        is_barclays_live_user = (user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"Order placement: user_id={user_id_for_order}, is_barclays_live_user={is_barclays_live_user}, user_type={user_type}, group_name={group_name}, sending_orders_setting={sending_orders_cache}")

        # Force order_status to 'PROCESSING' for Barclays live users
        if is_barclays_live_user:
            order_data['order_status'] = 'PROCESSING'

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
        orders_logger.info(f"[PRE-CRUD] About to call create_order with: {order_create_internal}")
        db_order = await crud_order.create_order(db, order_create_internal, order_model)

        # Log user margin from session before commit
        try:
            user_to_check = await db.get(type(current_user), current_user.id) # Assumes current_user is the correct user model type
            if user_to_check:
                orders_logger.info(f"[MARGIN_COMMIT_CHECK] User {current_user.id} margin in session BEFORE commit: {user_to_check.margin}")
            else:
                orders_logger.warning(f"[MARGIN_COMMIT_CHECK] User {current_user.id} not found in session before commit.")
        except Exception as e_check:
            orders_logger.error(f"[MARGIN_COMMIT_CHECK] Error checking user margin before commit: {e_check}", exc_info=True)
        
        await db.commit()
        await db.refresh(db_order)

        # Margin update for non-Barclays users is now handled within process_new_order.
        # The database commit for the order itself is sufficient here.

        # --- Portfolio Update & Websocket Event ---
        try:
            user_id = db_order.order_user_id
            user_data = await get_user_data_cache(redis_client, user_id, db, current_user.user_type)
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
                    for symbol_key in group_symbol_settings.keys(): # Renamed symbol to symbol_key to avoid conflict with outer scope symbol if any
                        # Assuming symbol_key is the actual symbol string we need for price fetching
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after placing order.")
        except Exception as e:
            orders_logger.error(f"Error updating portfolio cache or publishing websocket event after order placement: {e}", exc_info=True)

        # --- Barclays Firebase Push Logic ---
        if is_barclays_live_user:
            try:
                # For Barclays live users, ensure order_status is PROCESSING and do NOT update margin in DB
                # Check margin and send to Firebase if sufficient, but do not update user margin
                user_id = db_order.order_user_id
                user_data = await get_user_data_cache(redis_client, user_id, db, current_user.user_type)
                group_name = user_data.get('group_name') if user_data else None
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL") if group_name else None
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                free_margin = Decimal(str(portfolio.get('free_margin', '0')))
                order_margin = Decimal(str(db_order.margin or 0))
                # Do NOT update user margin for Barclays users; just check
                if free_margin > order_margin:
                    firebase_order_data = {
                        'order_id': db_order.order_id,
                        'order_user_id': db_order.order_user_id,
                        'order_company_name': db_order.order_company_name,
                        'order_type': db_order.order_type,
                        'order_status': db_order.order_status,  # Should be PROCESSING
                        'order_price': db_order.order_price,
                        'order_quantity': db_order.order_quantity,
                        'contract_value': db_order.contract_value,
                        'margin': db_order.margin,
                        'stop_loss': db_order.stop_loss,
                        'take_profit': db_order.take_profit,

                        'status': getattr(db_order, 'status', None),
                    }
                    orders_logger.info(f"[FIREBASE] Payload being sent to Firebase: {firebase_order_data}")
                    await send_order_to_firebase(firebase_order_data, "live")
                    orders_logger.info(f"[FIREBASE] Barclays order sent to Firebase: {db_order.order_id} (order_status=PROCESSING, margin not updated in DB)")
                else:
                    orders_logger.warning(f"[FIREBASE] Barclays order NOT sent to Firebase due to insufficient free margin. free_margin={free_margin}, order_margin={order_margin}")
            except Exception as e:
                orders_logger.error(f"[FIREBASE] Error sending Barclays order to Firebase: {e}", exc_info=True)

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

@router.post("/pending-place", response_model=OrderResponse)
async def place_pending_order(
    order_request: PendingOrderPlacementRequest, # Use the new schema here
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Place a new PENDING order (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP).
    """
    try:
        orders_logger.info(f"Pending order placement request received - User ID: {current_user.id}, Symbol: {order_request.symbol}, Type: {order_request.order_type}, Quantity: {order_request.order_quantity}")
        
        user_id_for_order = current_user.id
        user_type = order_request.user_type.lower()

        # Generate a unique order_id for the new pending order
        new_order_id = str(uuid.uuid4())

        # Prepare order data for internal processing
        order_data_for_internal_processing = {
            'order_id': new_order_id, # Assign the generated order_id
            'order_company_name': order_request.symbol,
            'order_type': order_request.order_type,
            'order_quantity': order_request.order_quantity,
            'order_price': order_request.order_price, # This is the limit/stop price
            'user_type': user_type,
            'status': order_request.status,
            'stop_loss': order_request.stop_loss,
            'take_profit': order_request.take_profit,
            'order_user_id': user_id_for_order,
            'order_status': order_request.order_status, # This will be PENDING from the schema default
            'contract_value': None, # Not known at placement, will be set on trigger
            'margin': None, # Not known at placement, will be set on trigger
            'open_time': None # Not open yet
        }

        orders_logger.info(f"Placing PENDING order: {order_request.order_type} for user {user_id_for_order} at price {order_request.order_price}")

        # Create order in database with PENDING status
        # Convert Pydantic model to dictionary using .model_dump()
        order_create_internal_dict = OrderCreateInternal(**order_data_for_internal_processing).model_dump()
        order_model = get_order_model(user_type)
        # Pass the dictionary to crud_order.create_order
        db_order = await crud_order.create_order(db, order_create_internal_dict, order_model) 
        await db.commit()
        await db.refresh(db_order)

        # Add to Redis pending orders
        # Ensure the order dict passed to add_pending_order has all necessary fields
        order_dict_for_redis = {
            'order_id': db_order.order_id,
            'order_user_id': db_order.order_user_id,
            'order_company_name': db_order.order_company_name,
            'order_type': db_order.order_type,
            'order_status': db_order.order_status, # Should be PENDING
            'order_price': str(db_order.order_price), # Store as string for JSON serialization
            'order_quantity': str(db_order.order_quantity), # Store as string
            'contract_value': str(db_order.contract_value) if db_order.contract_value else None,
            'margin': str(db_order.margin) if db_order.margin else None,
            'stop_loss': str(db_order.stop_loss) if db_order.stop_loss else None,
            'take_profit': str(db_order.take_profit) if db_order.take_profit else None,
            'user_type': user_type,
            'status': db_order.status,
            'open_time': db_order.open_time.isoformat() if db_order.open_time else None,
            # Add any other fields that might be needed by trigger_pending_order
        }
        await add_pending_order(redis_client, order_dict_for_redis)
        orders_logger.info(f"Pending order {db_order.order_id} added to Redis.")

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
        orders_logger.error(f"Unexpected error in place_pending_order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process pending order: {str(e)}")


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

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
from app.crud import crud_order
from app.dependencies.redis_client import get_redis_client
from app.database.session import get_db
from app.schemas.order import OrderUpdateRequest
from app.core.security import get_user_from_service_or_user_token

from app.crud.crud_order import get_order_model

async def handle_barclays_order_open_transition(
    db: AsyncSession,
    redis_client: Redis,
    db_order,
    user_type: str
):
    from app.crud import crud_order
    from app.crud.user import update_user_margin
    from app.services.margin_calculator import calculate_total_symbol_margin_contribution
    from app.core.cache import get_user_data_cache
    from decimal import Decimal
    import logging

    logger = logging.getLogger(__name__)

    try:
        user_id = db_order.order_user_id
        symbol = db_order.order_company_name.upper()
        order_model = get_order_model(user_type)

        # Fetch all current open orders for the symbol (including the one being transitioned)
        open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
        if db_order not in open_orders:
            open_orders.append(db_order)

        # Calculate total margin using hedging logic
        total_margin = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=user_id,
            symbol=symbol,
            open_positions_for_symbol=open_orders,
            order_model_for_calc=order_model
        )

        # Update user total margin in DB
        await update_user_margin(db, user_id, user_type, total_margin)
        logger.info(f"Margin updated for Barclays user {user_id} on symbol {symbol} to {total_margin}")

    except Exception as e:
        logger.error(f"Error in Barclays margin update during OPEN transition: {e}", exc_info=True)
        raise


@router.patch("/orders/{order_id}/status")
async def update_order_status(
    order_id: str,
    update_request: OrderUpdateRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user = Depends(get_user_from_service_or_user_token)
):
    user_type = current_user.user_type
    order_model = get_order_model(user_type)

    db_order = await crud_order.get_order_by_id(db, order_id, order_model)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")

    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Not authorized to modify this order")

    # Check and apply transition logic
    if db_order.order_status == "PROCESSING" and update_request.order_status == "OPEN":
        await handle_barclays_order_open_transition(db, redis_client, db_order, user_type)

    update_fields = update_request.model_dump(exclude_unset=True)
    updated_order = await crud_order.update_order_with_tracking(
        db=db,
        db_order=db_order,
        update_fields=update_fields,
        user_id=current_user.id,
        user_type=user_type
    )
    return {"status": "success", "updated_order": updated_order.order_id}
