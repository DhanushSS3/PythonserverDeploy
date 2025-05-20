# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Any, List, Dict
import uuid
import datetime
from pydantic import BaseModel, Field
import json # Import json for publishing

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution
)
from app.services.portfolio_calculator import _convert_to_usd

from app.crud import crud_order
from app.crud import user as crud_user

from sqlalchemy.future import select
from app.core.cache import (
    get_adjusted_market_price_cache,
    get_group_symbol_settings_cache,
    set_user_data_cache, # Import for cache update
    set_user_portfolio_cache, # Import for cache update
    DecimalEncoder # Import for JSON serialization
)

from app.core.security import get_current_user
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # Import the channel name


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

@router.post(
    "/",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
    description="Allows an authenticated user to place a new trading order. Processing, including margin calculation and hedging, is handled by the backend service."
)
async def place_order(
    order_request: OrderPlacementRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    """
    Receives a new order request and delegates processing to the order_processing service.
    Handles service-specific exceptions and translates them to HTTP responses.
    """
    logger.info(f"Received order placement request for user {current_user.id}, symbol {order_request.symbol}")

    if order_request.order_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order quantity must be positive."
        )

    try:
        new_db_order = await process_new_order(
            db=db,
            redis_client=redis_client,
            user=current_user,
            order_request=order_request
        )

        logger.info(f"Order {new_db_order.order_id} successfully processed by service.")

        # --- START: New logic for WebSocket responsiveness ---

        # Refresh the user object to get the latest balance and margin after order processing
        await db.refresh(current_user)

        # 1. Update user_data in Redis cache
        # This includes wallet_balance, leverage, and overall margin
        user_data_to_cache = {
            "id": current_user.id,
            "group_name": getattr(current_user, 'group_name', 'default'),
            "leverage": current_user.leverage,
            "wallet_balance": current_user.wallet_balance,
            "margin": current_user.margin
        }
        await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
        logger.debug(f"Updated user data cache for user {current_user.id} after order placement.")

        # 2. Fetch all current open positions for this user (including the new one)
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, current_user.id)
        updated_positions_data = []
        for pos in open_positions_orm:
            pos_dict = {}
            for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
                value = getattr(pos, attr, None)
                if isinstance(value, Decimal):
                    pos_dict[attr] = str(value) # Store Decimal as string for JSON safety
                else:
                    pos_dict[attr] = value
            pos_dict['profit_loss'] = "0.0" # Placeholder, will be calculated by broadcaster
            updated_positions_data.append(pos_dict)

        # 3. Update user_portfolio in Redis cache
        # This includes balance, overall margin, and the latest list of positions
        user_portfolio_data = {
            "balance": str(current_user.wallet_balance),
            "equity": "0.0", # Will be recalculated by broadcaster
            "margin": str(current_user.margin), # Overall user margin
            "free_margin": "0.0", # Will be recalculated by broadcaster
            "profit_loss": "0.0", # Will be recalculated by broadcaster (total PnL)
            "positions": updated_positions_data
        }
        await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)
        logger.debug(f"Updated user portfolio cache for user {current_user.id} after order placement.")

        # 4. Signal the broadcaster to send account updates for this specific user
        # Publish a message to the market data channel, but with a special type.
        account_update_signal = {
            "type": "account_update_signal", # New message type
            "user_id": current_user.id
        }
        await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal))
        logger.info(f"Published account update signal for user {current_user.id} after order placement.")

        # --- END: New logic for WebSocket responsiveness ---

        return OrderResponse.model_validate(new_db_order)

    except InsufficientFundsError as e:
        logger.warning(f"Order placement failed for user {current_user.id} due to insufficient funds: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except OrderProcessingError as e:
        logger.error(f"Order processing failed for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the order."
        )
    except Exception as e:
        logger.error(f"Unexpected error during order placement for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred."
        )

# Endpoint to get order by ID (kept as is)
@router.get("/{order_id}", response_model=OrderResponse)
async def read_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    db_order = await crud_order.get_order_by_id(db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
         raise HTTPException(status_code=403, detail="Not authorized to view this order")
    return OrderResponse.model_validate(db_order)


# Endpoint to get user's orders (kept as is)
@router.get("/", response_model=List[OrderResponse])
async def read_user_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, skip=skip, limit=limit)
    return [OrderResponse.model_validate(order) for order in orders]

# --- Endpoints for Specific Order Statuses (kept as is) ---

@router.get("/open", response_model=List[OrderResponse])
async def read_user_open_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id=current_user.id)
    return [OrderResponse.model_validate(order) for order in open_orders[skip:skip+limit]]

@router.get("/closed", response_model=List[OrderResponse])
async def read_user_closed_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
    closed_orders = [order for order in orders if order.order_status == 'CLOSED']
    return [OrderResponse.model_validate(order) for order in closed_orders[skip:skip+limit]]


# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Any, List, Dict
import uuid
import datetime
from pydantic import BaseModel, Field
import json # Import json for publishing


from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
# Import Wallet model
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet
# Import WalletCreate schema
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate # Import WalletCreate schema

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution
)
from app.services.portfolio_calculator import _convert_to_usd

from app.crud import crud_order
from app.crud import user as crud_user

from sqlalchemy.future import select
from app.core.cache import (
    get_adjusted_market_price_cache,
    get_group_symbol_settings_cache,
    set_user_data_cache, # Import for cache update
    set_user_portfolio_cache, # Import for cache update
    DecimalEncoder # Import for JSON serialization
)

from app.core.security import get_current_user
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # Import the channel name


logger = logging.getLogger(__name__)


# CloseOrderRequest schema is now defined in app.schemas.order


@router.post(
    "/close",
    response_model=OrderResponse,
    summary="Close an open order",
    description="Closes an open order, updates its status to 'CLOSED', and adjusts the user's overall margin based on hedging logic. Requires the order ID and closing price in the request body."
)
async def close_order(
    close_request: CloseOrderRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    from app.core.cache import get_group_symbol_settings_cache, get_adjusted_market_price_cache
    from app.services.portfolio_calculator import _convert_to_usd
    from app.services.order_processing import calculate_total_symbol_margin_contribution

    order_id = close_request.order_id
    close_price = Decimal(str(close_request.close_price))

    logger.info(f"Received request to close order {order_id} for user {current_user.id} with close price {close_price}.")

    # 1. Validate and Fetch Order
    db_order = await crud_order.get_order_by_id(db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found.")
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Not authorized to close this order.")
    if db_order.order_status != 'OPEN':
        raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

    order_symbol = db_order.order_company_name.upper()
    quantity = Decimal(str(db_order.order_quantity))
    entry_price = Decimal(str(db_order.order_price))
    order_type = db_order.order_type.upper()
    user_group_name = getattr(current_user, 'group_name', 'default')

    # 2. Lock user record
    db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
    if db_user_locked is None:
        raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

    # 3. Margin Recalculation Logic (existing logic preserved)
    all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
        db=db,
        user_id=current_user.id,
        symbol=order_symbol
    )
    margin_before = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=current_user.id,
        symbol=order_symbol,
        open_positions_for_symbol=all_open_orders_for_symbol
    )
    non_symbol_margin = Decimal(str(db_user_locked.margin)) - margin_before

    remaining_orders = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
    margin_after = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=current_user.id,
        symbol=order_symbol,
        open_positions_for_symbol=remaining_orders
    )
    db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


    # 4. Fetch symbol info
    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
    symbol_info_result = await db.execute(symbol_info_stmt)
    ext_symbol_info = symbol_info_result.scalars().first()
    if not ext_symbol_info or not ext_symbol_info.contract_size:
        raise HTTPException(status_code=500, detail="Missing contract size for symbol.")
    contract_size = Decimal(str(ext_symbol_info.contract_size))
    profit_currency = ext_symbol_info.profit.upper() if ext_symbol_info.profit else "USD"

    # 5. Calculate commission (USD)
    group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
    if not group_settings:
        logger.error(f"Group settings not found for group '{user_group_name}', symbol '{order_symbol}'. Cannot calculate commission.")
        raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")

    commission_type = int(group_settings.get('commision_type', 0))
    commission_value_type = int(group_settings.get('commision_value_type', 0))
    commission_rate = Decimal(str(group_settings.get('commision', 0)))

    entry_commission = Decimal("0.0")
    exit_commission = Decimal("0.0")

    if commission_type in [0, 1]: # 0: Every Trade, 1: In
        if commission_value_type == 0: # Per lot
            entry_commission = quantity * commission_rate
        elif commission_value_type == 1: # Percent
            entry_commission = ((commission_rate * entry_price) / Decimal("100")) * quantity

    if commission_type in [0, 2]: # 0: Every Trade, 2: Out
        if commission_value_type == 0: # Per lot
            exit_commission = quantity * commission_rate
        elif commission_value_type == 1: # Percent
            calculated_exit_contract_value = quantity * contract_size * close_price
            if calculated_exit_contract_value <= Decimal("0.0"):
                 logger.warning(f"Calculated exit contract value is zero or negative ({calculated_exit_contract_value}) for order {order_id}. Cannot calculate percentage exit commission.")
                 exit_commission = Decimal("0.0")
            else:
                 exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value


    total_commission = (entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # 6. Calculate raw PnL (in native currency)
    if order_type == "BUY":
        profit = (close_price - entry_price) * quantity * contract_size
    elif order_type == "SELL":
        profit = (entry_price - close_price) * quantity * contract_size
    else:
        raise HTTPException(status_code=500, detail="Invalid order type.")

    # 7. Convert profit to USD
    adjusted_market_prices_for_conversion: Dict[str, Dict[str, float]] = {}

    direct_pair = f"{profit_currency}USD"
    indirect_pair = f"USD{profit_currency}"

    direct_pair_data = await get_adjusted_market_price_cache(redis_client, user_group_name, direct_pair)
    if direct_pair_data:
        adjusted_market_prices_for_conversion[direct_pair] = {
            'buy': float(direct_pair_data.get('buy', Decimal("0.0"))),
            'sell': float(direct_pair_data.get('sell', Decimal("0.0")))
        }
        logger.debug(f"Fetched direct conversion pair {direct_pair} for PnL conversion for user {current_user.id}, order {order_id}.")
    else:
        logger.warning(f"No cached adjusted market price found for direct conversion pair: {direct_pair} for group {user_group_name}.")

    indirect_pair_data = await get_adjusted_market_price_cache(redis_client, user_group_name, indirect_pair)
    if indirect_pair_data:
        adjusted_market_prices_for_conversion[indirect_pair] = {
            'buy': float(indirect_pair_data.get('buy', Decimal("0.0"))),
            'sell': float(indirect_pair_data.get('sell', Decimal("0.0")))
        }
        logger.debug(f"Fetched indirect conversion pair {indirect_pair} for PnL conversion for user {current_user.id}, order {order_id}.")
    else:
        logger.warning(f"No cached adjusted market price found for indirect conversion pair: {indirect_pair} for group {user_group_name}.")

    if not adjusted_market_prices_for_conversion and profit_currency != "USD":
        logger.error(f"Could not find any conversion rates for profit currency {profit_currency} to USD for group {user_group_name}. Cannot convert PnL.")
        raise HTTPException(status_code=500, detail=f"No conversion rates found for {profit_currency} to USD.")

    profit_usd = await _convert_to_usd(
        amount=profit,
        from_currency=profit_currency,
        adjusted_market_prices=adjusted_market_prices_for_conversion,
        user_id=current_user.id,
        position_id=db_order.order_id,
        value_description="PnL"
    )

    # 8. Final updates to order and user balance
    db_order.order_status = "CLOSED"
    db_order.close_price = close_price
    db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    db_order.swap = db_order.swap or Decimal("0.0")
    db_order.commission = total_commission

    db_user_locked.wallet_balance = (
        Decimal(str(db_user_locked.wallet_balance)) + db_order.net_profit - db_order.commission
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


    # 9. Create Wallet Transaction Records
    if db_order.net_profit is not None and db_order.net_profit != Decimal("0.0"):
        profit_loss_wallet_entry_data = WalletCreate(
            user_id=current_user.id,
            symbol=order_symbol,
            order_quantity=quantity,
            transaction_type="Profit/Loss",
            is_approved=1,
            order_type=order_type,
            transaction_amount=db_order.net_profit,
            description=f"P/L for closing order {db_order.order_id}",
        )
        wallet_profit_loss = Wallet(**profit_loss_wallet_entry_data.model_dump())
        wallet_profit_loss.transaction_id = str(uuid.uuid4())
        db.add(wallet_profit_loss)
        logger.info(f"Prepared Wallet record for Profit/Loss for order {db_order.order_id}, amount {db_order.net_profit}, transaction_id {wallet_profit_loss.transaction_id}.")


    if db_order.commission is not None and db_order.commission > Decimal("0.0"):
         commission_wallet_entry_data = WalletCreate(
            user_id=current_user.id,
            symbol=order_symbol,
            order_quantity=quantity,
            transaction_type="Commission",
            is_approved=1,
            order_type=order_type,
            transaction_amount=-db_order.commission,
            description=f"Commission for closing order {db_order.order_id}",
        )
         wallet_commission = Wallet(**commission_wallet_entry_data.model_dump())
         wallet_commission.transaction_id = str(uuid.uuid4())
         db.add(wallet_commission)
         logger.info(f"Prepared Wallet record for Commission for order {db_order.order_id}, amount {db_order.commission}, transaction_id {wallet_commission.transaction_id}.")

    # Commit the transaction (order update, user balance update, and new wallet records)
    await db.commit()

    # Refresh the order and user objects after the commit
    await db.refresh(db_order)
    await db.refresh(db_user_locked)

    logger.info(f"Order {db_order.order_id} closed successfully for user {current_user.id}. Wallet balance updated to {db_user_locked.wallet_balance}.")

    # --- START: New logic for WebSocket responsiveness ---

    # 1. Update user_data in Redis cache with the latest balance and margin
    user_data_to_cache = {
        "id": db_user_locked.id,
        "group_name": getattr(db_user_locked, 'group_name', 'default'),
        "leverage": db_user_locked.leverage,
        "wallet_balance": db_user_locked.wallet_balance,
        "margin": db_user_locked.margin
    }
    await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)
    logger.debug(f"Updated user data cache for user {db_user_locked.id} after order closing.")

    # 2. Fetch all current open positions for this user (excluding the one just closed)
    open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_locked.id)
    updated_positions_data = []
    for pos in open_positions_orm:
         pos_dict = {}
         for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
              value = getattr(pos, attr, None)
              if isinstance(value, Decimal):
                   pos_dict[attr] = str(value) # Store Decimal as string for JSON safety
              else:
                   pos_dict[attr] = value
         pos_dict['profit_loss'] = "0.0" # Placeholder, will be calculated by broadcaster
         updated_positions_data.append(pos_dict)

    # 3. Update user_portfolio in Redis cache
    user_portfolio_data = {
         "balance": str(db_user_locked.wallet_balance),
         "equity": "0.0", # Will be recalculated by broadcaster
         "margin": str(db_user_locked.margin), # Overall user margin
         "free_margin": "0.0", # Will be recalculated by broadcaster
         "profit_loss": "0.0", # Will be recalculated by broadcaster (total PnL)
         "positions": updated_positions_data
    }
    await set_user_portfolio_cache(redis_client, db_user_locked.id, user_portfolio_data)
    logger.debug(f"Updated user portfolio cache for user {db_user_locked.id} after order closing.")

    # 4. Signal the broadcaster to send account updates for this specific user
    account_update_signal = {
        "type": "account_update_signal", # New message type
        "user_id": db_user_locked.id
    }
    await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal))
    logger.info(f"Published account update signal for user {db_user_locked.id} after order closing.")

    # --- END: New logic for WebSocket responsiveness ---

    return OrderResponse.model_validate(db_order)

