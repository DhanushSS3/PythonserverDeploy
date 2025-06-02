# app/services/order_processing.py

import logging
import random

def generate_10_digit_id():
    return str(random.randint(10**9, 10**10-1))

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # Import ROUND_HALF_UP for quantization
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import uuid # Import uuid

# Import necessary components
from app.database.models import User, UserOrder
from app.schemas.order import OrderPlacementRequest, OrderCreateInternal
# Import updated crud_order and user crud
from app.crud import crud_order
from app.crud import user as crud_user
# Import the margin calculator service and its helper
from app.services.margin_calculator import calculate_single_order_margin

logger = logging.getLogger(__name__)

# Define custom exceptions for the service
class OrderProcessingError(Exception):
    """Custom exception for errors during order processing."""
    pass

class InsufficientFundsError(Exception):
    """Custom exception for insufficient funds during order placement."""
    pass

# # UPDATED HELPER FUNCTION
# async def calculate_total_symbol_margin_contribution(
#     db: AsyncSession,
#     redis_client: Redis,
#     user_id: int,
#     symbol: str,
#     open_positions_for_symbol: List[UserOrder]
# ) -> Decimal:
#     """
#     Calculates the total margin attributed to a specific symbol for a user,
#     considering the "dominant side" hedging logic (highest margin per max of buy/sell quantity).
#     This function processes a list of *existing* open positions for a given symbol.
#     """
#     total_buy_quantity = Decimal(0)
#     total_sell_quantity = Decimal(0)
#     all_margins_per_lot: List[Decimal] = []

#     for position in open_positions_for_symbol:
#         position_quantity = Decimal(str(position.order_quantity))
#         position_type = position.order_type.upper()
#         position_full_margin = Decimal(str(position.margin)) # Full non-hedged margin of the individual order

#         if position_quantity > 0:
#             margin_per_lot_of_position = position_full_margin / position_quantity
#             all_margins_per_lot.append(margin_per_lot_of_position)
#         else:
#             logger.warning(f"Position {position.order_id} for user {user_id} has zero quantity. Skipping for margin calculation in total symbol margin.")

#         if position_type == 'BUY':
#             total_buy_quantity += position_quantity
#         elif position_type == 'SELL':
#             total_sell_quantity += position_quantity

#     # --- CHANGE HERE: Use max(buy_qty, sell_qty) for net_quantity ---
#     net_quantity = max(total_buy_quantity, total_sell_quantity)

#     highest_margin_per_lot = Decimal(0)
#     if all_margins_per_lot:
#         highest_margin_per_lot = max(all_margins_per_lot)

#     calculated_symbol_margin = highest_margin_per_lot * net_quantity
#     return calculated_symbol_margin.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP) # Quantize for consistency

import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import uuid

from app.database.models import User, UserOrder
from app.schemas.order import OrderPlacementRequest, OrderCreateInternal
from app.crud import crud_order
from app.crud import user as crud_user
from app.services.margin_calculator import calculate_single_order_margin

logger = logging.getLogger(__name__)

class OrderProcessingError(Exception):
    pass

class InsufficientFundsError(Exception):
    pass

async def calculate_total_symbol_margin_contribution(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_positions_for_symbol: list,
    order_model=None
) -> Decimal:
    total_buy_quantity = Decimal(0)
    total_sell_quantity = Decimal(0)
    all_margins_per_lot: List[Decimal] = []

    for position in open_positions_for_symbol:
        position_quantity = Decimal(str(position.order_quantity))
        position_type = position.order_type.upper()
        position_full_margin = Decimal(str(position.margin))

        if position_quantity > 0:
            margin_per_lot_of_position = position_full_margin / position_quantity
            all_margins_per_lot.append(margin_per_lot_of_position)

        if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
            total_buy_quantity += position_quantity
        elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
            total_sell_quantity += position_quantity

    net_quantity = max(total_buy_quantity, total_sell_quantity)
    highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal(0)
    return (highest_margin_per_lot * net_quantity).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)

async def process_new_order(
    db: AsyncSession,
    redis_client: Redis,
    user: User,
    order_request: OrderPlacementRequest,
    order_model,
    create_order_fn=None,
    get_user_by_id_with_lock_fn=None
) -> object:
    logger.info(f"Processing new order for user {user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

    new_order_quantity = Decimal(str(order_request.order_quantity))
    new_order_type = order_request.order_type.upper()
    order_symbol = order_request.symbol.upper()

    full_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        order_quantity=new_order_quantity,
        order_price=order_request.order_price,
        symbol=order_symbol,
        order_type=new_order_type
    )

    if full_margin_usd is None or adjusted_order_price is None or contract_value is None:
        raise OrderProcessingError("Margin calculation failed.")

    if new_order_quantity <= 0:
        raise OrderProcessingError("Invalid order quantity.")

    # Fetch existing open and pending orders
    existing_orders = await crud_order.get_open_and_pending_orders_by_user_id_and_symbol(
        db=db,
        user_id=user.id,
        symbol=order_symbol,
        order_model=order_model
    )

    margin_before = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        symbol=order_symbol,
        open_positions_for_symbol=existing_orders,
        order_model=order_model
    )

    dummy_order = order_model(
        order_quantity=new_order_quantity,
        order_type=new_order_type,
        margin=full_margin_usd
    )
    orders_after = existing_orders + [dummy_order]

    margin_after = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        symbol=order_symbol,
        open_positions_for_symbol=orders_after,
        order_model=order_model
    )

    additional_margin = max(Decimal("0.0"), margin_after - margin_before)

    # Use the provided lock function for demo/live user
    if get_user_by_id_with_lock_fn is None:
        db_user_locked = await crud_user.get_user_by_id_with_lock(db, user.id)
    else:
        db_user_locked = await get_user_by_id_with_lock_fn(db, user.id)
    if db_user_locked is None:
        raise OrderProcessingError("Could not lock user record.")

    if db_user_locked.margin + additional_margin > db_user_locked.wallet_balance:
        raise InsufficientFundsError("Insufficient margin to place the order.")

    db_user_locked.margin += additional_margin
    await db.flush()

    # Generate a unique 10-digit random order_id for new orders

    new_order_data = OrderCreateInternal(
        order_id=generated_order_id,
        order_status="PENDING" if new_order_type in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"] else "OPEN",
        order_user_id=user.id,
        order_company_name=order_symbol,
        order_type=new_order_type,
        order_price=adjusted_order_price,
        order_quantity=new_order_quantity,
        contract_value=contract_value,
        margin=full_margin_usd,
        stop_loss=order_request.stop_loss,
        take_profit=order_request.take_profit
    )

    # Use the provided create_order_fn for demo/live user
    if create_order_fn is None:
        from app.crud.crud_order import create_order
        db_order = await create_order(db, new_order_data.model_dump(), order_model)
    else:
        db_order = await create_order_fn(db, new_order_data.model_dump(), order_model)
    return db_order


# # MAIN PROCESSING FUNCTION FOR NEW ORDER (remains the same in logic, but will use updated helper)
# async def process_new_order(
#     db: AsyncSession,
#     redis_client: Redis,
#     user: User,
#     order_request: OrderPlacementRequest
# ) -> UserOrder:
#     """
#     Processes a new order request, calculates the margin, updates the user's margin,
#     and creates a new order in the database, considering commission and hedging logic.
#     """
#     logger.info(f"Processing new order for user {user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

#     new_order_quantity = Decimal(str(order_request.order_quantity))
#     new_order_type = order_request.order_type.upper()
#     order_symbol = order_request.symbol.upper()

#     # Step 1: Calculate full margin and contract value
#     from app.services.margin_calculator import calculate_single_order_margin
#     full_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
#         db=db,
#         redis_client=redis_client,
#         user_id=user.id,
#         order_quantity=new_order_quantity,
#         order_price=order_request.order_price,
#         symbol=order_symbol,
#         order_type=new_order_type
#     )

#     if full_margin_usd is None or adjusted_order_price is None or contract_value is None:
#         logger.error(f"Failed to calculate margin or adjusted price for user {user.id}, symbol {order_symbol}")
#         raise OrderProcessingError("Margin calculation failed.")

#     if new_order_quantity <= 0:
#         raise OrderProcessingError("Invalid order quantity.")

#     # Step 2: Calculate margin before and after new order for hedging
#     existing_open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(
#         db=db,
#         user_id=user.id,
#         symbol=order_symbol
#     )

#     margin_before = await calculate_total_symbol_margin_contribution(
#         db=db,
#         redis_client=redis_client,
#         user_id=user.id,
#         symbol=order_symbol,
#         open_positions_for_symbol=existing_open_orders
#     )

#     dummy_order = UserOrder(
#         order_quantity=new_order_quantity,
#         order_type=new_order_type,
#         margin=full_margin_usd
#     )
#     orders_after = existing_open_orders + [dummy_order]

#     margin_after = await calculate_total_symbol_margin_contribution(
#         db=db,
#         redis_client=redis_client,
#         user_id=user.id,
#         symbol=order_symbol,
#         open_positions_for_symbol=orders_after
#     )

#     additional_margin = margin_after - margin_before
#     additional_margin = max(Decimal("0.0"), additional_margin)

#     # Step 3: Lock user and update margin
#     db_user_locked = await crud_user.get_user_by_id_with_lock(db, user.id)
#     if db_user_locked is None:
#         raise OrderProcessingError("Could not lock user record.")

#     db_user_locked.margin = (Decimal(str(db_user_locked.margin)) + additional_margin).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

#     # Step 4: Calculate commission if applicable
#     from app.core.cache import get_group_symbol_settings_cache
#     commission = Decimal("0.0")

#     group_symbol_settings = await get_group_symbol_settings_cache(redis_client, getattr(user, 'group_name', 'default'), order_symbol)
#     if group_symbol_settings:
#         commission_type = int(group_symbol_settings.get('commision_type', 0))
#         commission_value_type = int(group_symbol_settings.get('commision_value_type', 0))
#         commission_rate = Decimal(str(group_symbol_settings.get('commision', 0)))

#         if commission_type in [0, 1]:  # "Every Trade" or "In"
#             if commission_value_type == 0:  # Per lot
#                 commission = new_order_quantity * commission_rate
#             elif commission_value_type == 1:  # Percent of price
#                 commission = ((commission_rate * adjusted_order_price) / Decimal("100")) * new_order_quantity

#         commission = commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

#     # Step 5: Create order record
#     from app.schemas.order import OrderCreateInternal
#     order_data_internal = OrderCreateInternal(
#         order_id=order_request.order_id,
#         order_status="OPEN",
#         order_user_id=user.id,
#         order_company_name=order_symbol,
#         order_type=new_order_type,
#         order_price=adjusted_order_price,
#         order_quantity=new_order_quantity,
#         contract_value=contract_value,
#         margin=full_margin_usd,
#         commission=commission,
#         stop_loss=order_request.stop_loss,
#         take_profit=order_request.take_profit
#     )

#     new_order = await crud_order.create_user_order(db=db, order_data=order_data_internal.dict())

#     await db.commit()
#     await db.refresh(new_order)

#     return new_order

