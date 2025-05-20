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
from app.core.cache import get_adjusted_market_price_cache, get_group_symbol_settings_cache

from app.core.security import get_current_user # Ensure get_current_user is imported


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

# CloseOrderRequest schema is now defined in app.schemas.order


# Endpoint to place a new order (kept as is)
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
            user=current_user, # Pass the current_user object
            order_request=order_request
        )

        logger.info(f"Order {new_db_order.order_id} successfully processed by service.")

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


# @router.post(
#     "/close",
#     response_model=OrderResponse,
#     summary="Close an open order",
#     description="Closes an open order, updates its status to 'CLOSED', and adjusts the user's overall margin based on hedging logic. Requires the order ID and closing price in the request body."
# )
# async def close_order(
#     close_request: CloseOrderRequest,
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user: User = Depends(get_current_user)
# ):
#     from app.core.cache import get_group_symbol_settings_cache, get_adjusted_market_price_cache
#     from app.services.portfolio_calculator import _convert_to_usd

#     order_id = close_request.order_id
#     close_price = Decimal(str(close_request.close_price))

#     logger.info(f"Received request to close order {order_id} for user {current_user.id} with close price {close_price}.")

#     # 1. Validate and Fetch Order
#     db_order = await crud_order.get_order_by_id(db, order_id=order_id)
#     if db_order is None:
#         raise HTTPException(status_code=404, detail="Order not found.")
#     if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
#         raise HTTPException(status_code=403, detail="Not authorized to close this order.")
#     if db_order.order_status != 'OPEN':
#         raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

#     order_symbol = db_order.order_company_name.upper()
#     quantity = Decimal(str(db_order.order_quantity))
#     entry_price = Decimal(str(db_order.order_price))
#     order_type = db_order.order_type.upper()
#     user_group_name = getattr(current_user, 'group_name', 'default')

#     # 2. Lock user record
#     db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
#     if db_user_locked is None:
#         raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

#     # 3. Margin Recalculation Logic (existing logic preserved)
#     all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
#         db=db,
#         user_id=current_user.id,
#         symbol=order_symbol
#     )
#     margin_before = await calculate_total_symbol_margin_contribution(
#         db=db,
#         redis_client=redis_client,
#         user_id=current_user.id,
#         symbol=order_symbol,
#         open_positions_for_symbol=all_open_orders_for_symbol
#     )
#     non_symbol_margin = Decimal(str(db_user_locked.margin)) - margin_before

#     remaining_orders = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
#     margin_after = await calculate_total_symbol_margin_contribution(
#         db=db,
#         redis_client=redis_client,
#         user_id=current_user.id,
#         symbol=order_symbol,
#         open_positions_for_symbol=remaining_orders
#     )
#     db_user_locked.margin = max(Decimal(0), non_symbol_margin + margin_after)

#     # 4. Fetch symbol info
#     symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
#     symbol_info_result = await db.execute(symbol_info_stmt)
#     ext_symbol_info = symbol_info_result.scalars().first()
#     if not ext_symbol_info or not ext_symbol_info.contract_size:
#         raise HTTPException(status_code=500, detail="Missing contract size for symbol.")
#     contract_size = Decimal(str(ext_symbol_info.contract_size))
#     profit_currency = ext_symbol_info.profit.upper() if ext_symbol_info.profit else "USD"

#     # 5. Calculate commission (USD)
#     group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
#     commission_type = int(group_settings.get('commision_type', 0))
#     commission_value_type = int(group_settings.get('commision_value_type', 0))
#     commission_rate = Decimal(str(group_settings.get('commision', 0)))

#     entry_commission = Decimal("0.0")
#     exit_commission = Decimal("0.0")

#     if commission_type in [0, 1]:
#         if commission_value_type == 0:
#             entry_commission = quantity * commission_rate
#         elif commission_value_type == 1:
#             entry_commission = ((commission_rate * entry_price) / Decimal("100")) * quantity

#     if commission_type in [0, 2]:
#         if commission_value_type == 0:
#             exit_commission = quantity * commission_rate
#         elif commission_value_type == 1:
#             contract_value = quantity * contract_size * close_price
#             exit_commission = (commission_rate / Decimal("100")) * contract_value

#     total_commission = (entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

#     # 6. Calculate raw PnL (in native currency)
#     if order_type == "BUY":
#         profit = (close_price - entry_price) * quantity * contract_size
#     elif order_type == "SELL":
#         profit = (entry_price - close_price) * quantity * contract_size
#     else:
#         raise HTTPException(status_code=500, detail="Invalid order type.")

#     # 7. Convert profit to USD
#     conversion_symbol = profit_currency + "USD"
#     adjusted_market_prices = {
#         conversion_symbol: await get_adjusted_market_price_cache(redis_client, conversion_symbol)
#     }

#     profit_usd = await _convert_to_usd(
#         amount=profit,
#         from_currency=profit_currency,
#         adjusted_market_prices=adjusted_market_prices,
#         user_id=current_user.id,
#         position_id=db_order.order_id,
#         value_description="PnL"
#     )

#     # 8. Final updates
#     db_order.order_status = "CLOSED"
#     db_order.close_price = close_price
#     db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
#     db_order.swap = db_order.swap or Decimal("0.0")
#     db_order.commission = total_commission

#     await db.commit()
#     await db.refresh(db_order)

#     return OrderResponse.model_validate(db_order)

# app/api/v1/endpoints/orders.py

# ... (existing imports) ...

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
    # Correctly get user_group_name here
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
    db_user_locked.margin = max(Decimal(0), non_symbol_margin + margin_after)

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

    if commission_type in [0, 1]:
        if commission_value_type == 0:
            entry_commission = quantity * commission_rate
        elif commission_value_type == 1:
            entry_commission = ((commission_rate * entry_price) / Decimal("100")) * quantity

    if commission_type in [0, 2]:
        if commission_value_type == 0:
            exit_commission = quantity * commission_rate
        elif commission_value_type == 1:
            # Re-calculate contract_value based on close price for exit commission
            calculated_exit_contract_value = quantity * contract_size * close_price
            exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value

    total_commission = (entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # 6. Calculate raw PnL (in native currency)
    if order_type == "BUY":
        profit = (close_price - entry_price) * quantity * contract_size
    elif order_type == "SELL":
        profit = (entry_price - close_price) * quantity * contract_size
    else:
        raise HTTPException(status_code=500, detail="Invalid order type.")

    # 7. Convert profit to USD - Corrected section
    # The _convert_to_usd function in portfolio_calculator.py expects a dictionary
    # containing all potentially relevant conversion rates.
    # It tries both direct (PROFIT_CURR_USD) and indirect (USD_PROFIT_CURR) pairs.

    adjusted_market_prices_for_conversion: Dict[str, Dict[str, float]] = {}

    # Define the two possible conversion symbols
    direct_pair = f"{profit_currency}USD"  # e.g., AUDUSD
    indirect_pair = f"USD{profit_currency}"  # e.g., USDCAD

    # Fetch data for the direct pair, passing all required arguments
    direct_pair_data = await get_adjusted_market_price_cache(redis_client, user_group_name, direct_pair)
    if direct_pair_data:
        adjusted_market_prices_for_conversion[direct_pair] = {
            'buy': float(direct_pair_data.get('buy', Decimal("0.0"))),
            'sell': float(direct_pair_data.get('sell', Decimal("0.0")))
        }
        logger.debug(f"Fetched direct conversion pair {direct_pair} for PnL conversion for user {current_user.id}, order {order_id}.")
    else:
        logger.warning(f"No cached adjusted market price found for direct conversion pair: {direct_pair} for group {user_group_name}.")

    # Fetch data for the indirect pair, passing all required arguments
    indirect_pair_data = await get_adjusted_market_price_cache(redis_client, user_group_name, indirect_pair)
    if indirect_pair_data:
        adjusted_market_prices_for_conversion[indirect_pair] = {
            'buy': float(indirect_pair_data.get('buy', Decimal("0.0"))),
            'sell': float(indirect_pair_data.get('sell', Decimal("0.0")))
        }
        logger.debug(f"Fetched indirect conversion pair {indirect_pair} for PnL conversion for user {current_user.id}, order {order_id}.")
    else:
        logger.warning(f"No cached adjusted market price found for indirect conversion pair: {indirect_pair} for group {user_group_name}.")

    # Check if any conversion data was found at all
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

    # 8. Final updates
    db_order.order_status = "CLOSED"
    db_order.close_price = close_price
    db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    db_order.swap = db_order.swap or Decimal("0.0")
    db_order.commission = total_commission

    # Update user's wallet balance with net profit/loss and commission
    # Ensure all Decimal operations are performed correctly
    db_user_locked.wallet_balance = (
        Decimal(str(db_user_locked.wallet_balance)) + profit_usd - total_commission
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


    await db.commit()
    await db.refresh(db_order)
    await db.refresh(db_user_locked) # Refresh user after balance update

    return OrderResponse.model_validate(db_order)