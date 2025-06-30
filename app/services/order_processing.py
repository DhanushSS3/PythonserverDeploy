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
from typing import Optional, Dict, Any, List, Tuple
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
from app.core.cache import (
    get_user_data_cache, 
    get_group_symbol_settings_cache, 
    set_user_data_cache,
    get_live_adjusted_buy_price_for_pair,
    get_live_adjusted_sell_price_for_pair
)
from app.core.firebase import get_latest_market_data

logger = logging.getLogger(__name__)

def get_order_model(user_type: str):
    """
    Get the appropriate order model based on user type.
    
    NOTE: This is a simplified version. When possible, use the more comprehensive
    get_order_model function from app.api.v1.endpoints.orders which handles
    both string and User/DemoUser objects.
    """
    if isinstance(user_type, str) and user_type.lower() == 'demo':
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
    open_positions_for_symbol: list, # List of order objects or dicts
    order_model=None,
    user_type: str = 'live'
) -> Dict[str, Any]: 
    # logger.debug(f"[MARGIN_TOTAL_CONTRIB_ENTRY] User {user_id}, Symbol {symbol}, Positions count: {len(open_positions_for_symbol)}")
    # logger.debug(f"[MARGIN_TOTAL_CONTRIB_ENTRY] Positions data: {open_positions_for_symbol}") # Can be very verbose

    total_buy_quantity = Decimal(0)
    total_sell_quantity = Decimal(0)
    all_margins_per_lot: List[Decimal] = []
    contributing_orders_count = 0

    if not open_positions_for_symbol:
        # logger.debug(f"[MARGIN_TOTAL_CONTRIB] No open positions for User {user_id}, Symbol {symbol}. Returning zero margin.")
        return {"total_margin": Decimal("0.0"), "contributing_orders_count": 0}

    for i, position in enumerate(open_positions_for_symbol):
        try:
            # Handle both ORM objects (like DemoUserOrder/UserOrder) and dicts (like OrderCreateInternal)
            order_id_log = getattr(position, 'id', getattr(position, 'order_id', 'NEW_UNSAVED'))
            if isinstance(position, dict):
                position_quantity_str = str(position.get('quantity') or position.get('order_quantity', '0'))
                position_type = str(position.get('order_type', '')).upper()
                position_full_margin_str = str(position.get('margin', '0'))
            else: # Assuming ORM object
                position_quantity_str = str(position.order_quantity)
                position_type = position.order_type.upper()
                position_full_margin_str = str(position.margin)

            position_quantity = Decimal(position_quantity_str)
            position_full_margin = Decimal(position_full_margin_str)

            # logger.debug(f"[MARGIN_TOTAL_CONTRIB_POS_DETAIL] User {user_id}, Symbol {symbol}, Pos {i+1} (ID: {order_id_log}): Type={position_type}, Qty={position_quantity}, StoredMargin={position_full_margin}")

            if position_quantity > 0:
                margin_per_lot_of_position = Decimal("0.0")
                if position_quantity != Decimal("0"): # Avoid division by zero if quantity is somehow zero
                    margin_per_lot_of_position = position_full_margin / position_quantity
                all_margins_per_lot.append(margin_per_lot_of_position)
                # logger.debug(f"[MARGIN_TOTAL_CONTRIB_POS_DETAIL] User {user_id}, Symbol {symbol}, Pos {i+1}: MarginPerLot={margin_per_lot_of_position}")
                if position_full_margin > Decimal("0.0"):
                    contributing_orders_count +=1 # Count if this position itself contributes margin

            if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                total_buy_quantity += position_quantity
            elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
                total_sell_quantity += position_quantity
        except Exception as e:
            logger.error(f"[MARGIN_TOTAL_CONTRIB_POS_ERROR] Error processing position {i}: {position}. Error: {e}", exc_info=True)
            continue

    net_quantity = max(total_buy_quantity, total_sell_quantity)
    highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal(0)
    
    calculated_total_margin = (highest_margin_per_lot * net_quantity).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Changed precision to 0.01
    
    # logger.debug(f"[MARGIN_TOTAL_CONTRIB_CALC] User {user_id}, Symbol {symbol}: TotalBuyQty={total_buy_quantity}, TotalSellQty={total_sell_quantity}, NetQty={net_quantity}")
    # logger.debug(f"[MARGIN_TOTAL_CONTRIB_CALC] User {user_id}, Symbol {symbol}: AllMarginsPerLot={all_margins_per_lot}, HighestMarginPerLot={highest_margin_per_lot}")
    # logger.debug(f"[MARGIN_TOTAL_CONTRIB_EXIT] User {user_id}, Symbol {symbol}: CalculatedTotalMargin={calculated_total_margin}, ContributingOrders={contributing_orders_count} (based on individual stored margins)")
    
    # The contributing_orders_count here might be misleading if highest_margin_per_lot is zero.
    # The logic of this function implies that if highest_margin_per_lot is 0, total margin is 0.
    # The count should reflect orders that *would* contribute if their margin per lot was the highest.
    # For now, returning the count of positions that had non-zero margin themselves.
    return {"total_margin": calculated_total_margin, "contributing_orders_count": contributing_orders_count}

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
    is_barclays_live_user: bool = False
) -> dict:
    from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
    from app.crud.user import get_user_by_id_with_lock, get_demo_user_by_id_with_lock
    
    try:
        # Step 1: Load user data and settings
            user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
            if not user_data:
                raise OrderProcessingError("User data not found")

            symbol = order_data.get('order_company_name', '').upper()
            order_type = order_data.get('order_type', '').upper()
            quantity = Decimal(str(order_data.get('order_quantity', '0.0')))
            group_name = user_data.get('group_name')
            leverage = Decimal(str(user_data.get('leverage', '1.0')))

            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
            if not group_settings:
                raise OrderProcessingError(f"Group settings not found for symbol {symbol}")

            external_symbol_info = await get_external_symbol_info(db, symbol)
            if not external_symbol_info:
                raise OrderProcessingError(f"External symbol info not found for {symbol}")

            raw_market_data = await get_latest_market_data()
            if not raw_market_data:
                raise OrderProcessingError("Failed to get market data")

            # Step 2: Calculate standalone margin using the centralized function
            full_margin_usd, price, contract_value, commission = await calculate_single_order_margin(
                redis_client=redis_client,
                symbol=symbol,
                order_type=order_type,
                quantity=quantity,
                user_leverage=leverage,
                group_settings=group_settings,
                external_symbol_info=external_symbol_info,
                raw_market_data=raw_market_data,
                db=db,
                user_id=user_id
            )
            if full_margin_usd is None:
                raise OrderProcessingError("Margin calculation failed")

            # Log the calculated commission
            logger.info(f"[COMMISSION_CALC] User {user_id} Symbol {symbol}: Calculated commission={commission:.2f}")

            order_model = get_order_model(user_type)

            # Step 3: Hedged margin change for symbol
            open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                db, user_id, symbol, order_model
            )

            margin_before_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders_for_symbol, order_model, user_type
            )
            margin_before = margin_before_data["total_margin"]

            # Create simulated order with all necessary attributes for calculation
            simulated_order = type('Obj', (object,), {
                'order_quantity': quantity,
                'order_type': order_type,
                'margin': full_margin_usd,
                'id': None,  # Add id attribute for consistent debugging
                'order_id': 'NEW_ORDER_SIMULATED'  # Add order_id attribute for logging
            })()

            margin_after_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol,
                open_orders_for_symbol + [simulated_order],
                order_model, user_type
            )
            margin_after = margin_after_data["total_margin"]

            additional_margin = max(Decimal("0.0"), margin_after - margin_before)
            logger.info(f"[MARGIN_PROCESS] User {user_id} Symbol {symbol}: MarginBefore={margin_before:.2f}, MarginAfter={margin_after:.2f}, AdditionalMargin={additional_margin:.2f}")
            
            # Add more detailed logging to help track down margin calculation issues
            logger.info(f"[MARGIN_PROCESS_DEBUG] User {user_id} Symbol {symbol}")
            logger.info(f"[MARGIN_PROCESS_DEBUG] Full margin for this order: {full_margin_usd}")
            logger.info(f"[MARGIN_PROCESS_DEBUG] Existing open orders count: {len(open_orders_for_symbol)}")
            logger.info(f"[MARGIN_PROCESS_DEBUG] Total margin before: {margin_before}")
            logger.info(f"[MARGIN_PROCESS_DEBUG] Total margin after: {margin_after}")
            logger.info(f"[MARGIN_PROCESS_DEBUG] Additional margin needed: {additional_margin}")

            # Step 4: Lock user and update margin
            if not is_barclays_live_user:
                if user_type == 'demo':
                    db_user_locked = await get_demo_user_by_id_with_lock(db, user_id)
                else:
                    db_user_locked = await get_user_by_id_with_lock(db, user_id)

                if db_user_locked is None:
                    raise OrderProcessingError("Could not lock user record.")

                if db_user_locked.wallet_balance < db_user_locked.margin + additional_margin:
                    raise InsufficientFundsError("Not enough wallet balance to cover additional margin.")

                original_user_margin = db_user_locked.margin
                db_user_locked.margin = (Decimal(str(db_user_locked.margin)) + additional_margin).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                logger.info(f"[MARGIN_PROCESS] User {user_id}: OriginalMarginDB={original_user_margin}, CalculatedNewMargin={db_user_locked.margin}")
                db.add(db_user_locked)  # Ensure changes to user's margin are tracked for commit
                await db.commit()
                await db.refresh(db_user_locked)
                # --- Refresh user data cache after DB update ---
                user_data_to_cache = {
                    "wallet_balance": db_user_locked.wallet_balance,
                    "leverage": db_user_locked.leverage,
                    "group_name": db_user_locked.group_name,
                    "margin": db_user_locked.margin,
                    # Add any other fields you want cached
                }
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            # For Barclays users, skip user locking and margin update until order is confirmed

            # Step 5: Return order dict
            order_status = "PROCESSING" if is_barclays_live_user else "OPEN"
            
            stoploss_id = None
            if order_data.get('stop_loss') is not None:
                stoploss_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_id')

            takeprofit_id = None
            if order_data.get('take_profit') is not None:
                takeprofit_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_id')

            return {
                'order_id': await generate_unique_10_digit_id(db, order_model, 'order_id'),
                'order_status': order_status,
                'order_user_id': user_id,
                'order_company_name': symbol,
                'order_type': order_type,
                'order_price': price,
                'order_quantity': quantity,
                'contract_value': contract_value,
                'margin': full_margin_usd,
                'commission': commission,  # Include the calculated commission
                'stop_loss': order_data.get('stop_loss'),
                'take_profit': order_data.get('take_profit'),
                'stoploss_id': stoploss_id,
                'takeprofit_id': takeprofit_id,
                'status': order_data.get('status'),
            }
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

