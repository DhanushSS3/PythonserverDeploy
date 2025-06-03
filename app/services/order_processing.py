# app/services/order_processing.py

import logging
import random

async def generate_unique_10_digit_id(db, model, column):
    import random
    from sqlalchemy.future import select
    while True:
        candidate = str(random.randint(10**9, 10**10-1))
        stmt = select(model).where(getattr(model, column) == candidate)
        result = await db.execute(stmt)
        if not result.scalar():
            return candidate


from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # Import ROUND_HALF_UP for quantization
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import uuid # Import uuid

# Import necessary components
from app.database.models import User, UserOrder, ExternalSymbolInfo, DemoUserOrder
from app.schemas.order import OrderPlacementRequest, OrderCreateInternal
# Import updated crud_order and user crud
from app.crud import crud_order
from app.crud import user as crud_user
# Import the margin calculator service and its helper
from app.services.margin_calculator import calculate_single_order_margin
from app.core.logging_config import orders_logger
from sqlalchemy.future import select
from app.core.cache import get_user_data_cache, get_group_symbol_settings_cache, set_user_data_cache
from app.core.firebase import get_latest_market_data

logger = logging.getLogger(__name__)

def get_order_model(user_type: str):
    """
    Get the appropriate order model based on user type.
    """
    if user_type.lower() == 'demo':
        return DemoUserOrder
    return UserOrder

# Define custom exceptions for the service
class OrderProcessingError(Exception):
    """Custom exception for errors during order processing."""
    pass

class InsufficientFundsError(Exception):
    """Custom exception for insufficient funds during order placement."""
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

async def get_external_symbol_info(db: AsyncSession, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Get external symbol info from the database.
    """
    try:
        stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        result = await db.execute(stmt)
        symbol_info = result.scalars().first()
        
        if symbol_info:
            return {
                'contract_size': symbol_info.contract_size,
                'profit_currency': symbol_info.profit,
                'digit': symbol_info.digit
            }
        return None
    except Exception as e:
        orders_logger.error(f"Error getting external symbol info for {symbol}: {e}", exc_info=True)
        return None

async def process_new_order(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    order_data: Dict[str, Any],
    user_type: str,
    is_barclays_live_user: bool = False  # New parameter
) -> dict:
    """
    Prepare all data for a new order (generate IDs, calculate margin, etc.), and return a dict compatible with OrderCreateInternal.
    """
    try:
        # Get user data
        user_data = await get_user_data_cache(redis_client, user_id)
        if not user_data:
            raise OrderProcessingError("User data not found")

        # Get order details
        symbol = order_data.get('order_company_name', '').upper()
        order_type = order_data.get('order_type', '').upper()
        quantity = Decimal(str(order_data.get('order_quantity', '0.0')))
        
        # Get group settings
        group_name = user_data.get('group_name')
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            raise OrderProcessingError(f"Group settings not found for symbol {symbol}")

        # Get external symbol info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            raise OrderProcessingError(f"External symbol info not found for {symbol}")

        # Get raw market data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            raise OrderProcessingError("Failed to get market data")

        # Calculate margin for new order
        margin, price, contract_value = await calculate_single_order_margin(
            redis_client=redis_client,
            symbol=symbol,
            order_type=order_type,
            quantity=quantity,
            user_leverage=Decimal(str(user_data.get('leverage', '1.0'))),
            group_settings=group_settings,
            external_symbol_info=external_symbol_info,
            raw_market_data=raw_market_data
        )

        if not margin or not price or not contract_value:
            raise OrderProcessingError("Margin calculation returned invalid values")

        # Get all open orders for this symbol
        order_model = get_order_model(user_type)
        all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
            db=db, user_id=user_id, symbol=symbol, order_model=order_model
        )

        # Calculate current margin contribution for this symbol
        current_symbol_margin = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=user_id,
            symbol=symbol,
            open_positions_for_symbol=all_open_orders_for_symbol,
            order_model=order_model
        )

        # Calculate new margin contribution including the new order
        new_order_margin = margin
        new_symbol_margin = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=user_id,
            symbol=symbol,
            open_positions_for_symbol=[*all_open_orders_for_symbol, type('obj', (object,), {
                'order_quantity': quantity,
                'order_type': order_type,
                'margin': new_order_margin
            })],
            order_model=order_model
        )

        # Calculate margin difference
        margin_difference = new_symbol_margin - current_symbol_margin
        margin_difference = max(Decimal("0.0"), margin_difference)

        # Update user's margin
        current_user_margin = Decimal(str(user_data.get('margin', '0.0')))
        new_user_margin = (current_user_margin + margin_difference).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Update user data in cache
        user_data['margin'] = str(new_user_margin)
        await set_user_data_cache(redis_client, user_id, user_data)

        # Calculate commission if applicable
        commission = Decimal("0.0")
        commission_type = int(group_settings.get('commision_type', 0))
        commission_value_type = int(group_settings.get('commision_value_type', 0))
        commission_rate = Decimal(str(group_settings.get('commision', '0.0')))

        if commission_type in [0, 1]:  # "Every Trade" or "In"
            if commission_value_type == 0:  # Per lot
                commission = quantity * commission_rate
            elif commission_value_type == 1:  # Percent of price
                commission = ((commission_rate * price) / Decimal("100")) * quantity
            commission = commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Prepare OrderCreateInternal-compatible dict
        order_status = "PROCESSING" if is_barclays_live_user else "OPEN"

        order_create_internal = {
            'order_id': await generate_unique_10_digit_id(db, order_model, 'order_id'),
            'order_status': order_status,
            'order_user_id': user_id,
            'order_company_name': symbol,
            'order_type': order_type,
            'order_price': price,
            'order_quantity': quantity,
            'contract_value': contract_value,
            'margin': new_order_margin,
            'stop_loss': order_data.get('stop_loss'),
            'take_profit': order_data.get('take_profit'),
            'close_id': await generate_unique_10_digit_id(db, order_model, 'close_id'),
        }
        # If you need to return extra IDs (like cancel_id, etc.), you can return them as a secondary dict or log them as needed.
        return order_create_internal

    except Exception as e:
        logger.error(f"Error processing new order: {e}", exc_info=True)
        raise OrderProcessingError(f"Failed to process order: {str(e)}")


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

