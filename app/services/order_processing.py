# app/services/order_processing.py

import logging
import random
import asyncio

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
    get_live_adjusted_sell_price_for_pair,
    get_order_placement_data_batch_ultra,
    RedisConnectionPool
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

async def process_new_order_ultra_optimized(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    order_data: Dict[str, Any],
    user_type: str,
    is_barclays_live_user: bool = False
) -> dict:
    """
    ULTRA-OPTIMIZED order processing for sub-500ms performance.
    Uses batch cache operations, connection pooling, and parallel processing.
    """
    from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
    from app.crud.user import get_user_by_id_with_lock, get_demo_user_by_id_with_lock
    from app.core.cache import get_order_placement_data_batch_ultra, RedisConnectionPool
    
    try:
        # Step 1: Extract order details
        symbol = order_data.get('order_company_name', '').upper()
        order_type = order_data.get('order_type', '').upper()
        quantity = Decimal(str(order_data.get('order_quantity', '0.0')))

        # Step 2: ULTRA-OPTIMIZED batch data fetching
        # Get user data first to determine group_name
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            raise OrderProcessingError("User data not found")
        
        group_name = user_data.get('group_name')
        
        # Create Redis connection pool for batch operations
        redis_pool = RedisConnectionPool(redis_client)
        
        # Step 3: ULTRA-PARALLEL operations with batch caching
        tasks = {
            'batch_cache_data': get_order_placement_data_batch_ultra(
                redis_client, user_id, symbol, group_name, db, user_type
            ),
            'external_symbol_info': get_external_symbol_info(db, symbol),
            'raw_market_data': get_latest_market_data(),
            'open_orders': crud_order.get_open_orders_by_user_id_and_symbol(
                db, user_id, symbol, get_order_model(user_type)
            ),
            'order_id': generate_unique_10_digit_id(db, get_order_model(user_type), 'order_id')
        }
        
        # Add optional ID generation tasks
        if order_data.get('stop_loss') is not None:
            tasks['stoploss_id'] = generate_unique_10_digit_id(db, get_order_model(user_type), 'stoploss_id')
        if order_data.get('take_profit') is not None:
            tasks['takeprofit_id'] = generate_unique_10_digit_id(db, get_order_model(user_type), 'takeprofit_id')
        
        # Execute ALL tasks in parallel
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # Extract results and handle exceptions
        batch_cache_data = results[0] if not isinstance(results[0], Exception) else None
        external_symbol_info = results[1] if not isinstance(results[1], Exception) else None
        raw_market_data = results[2] if not isinstance(results[2], Exception) else None
        open_orders_for_symbol = results[3] if not isinstance(results[3], Exception) else []
        
        # Extract generated IDs
        generated_ids = results[4:]  # All remaining results are IDs
        order_id = generated_ids[0] if not isinstance(generated_ids[0], Exception) else None
        stoploss_id = generated_ids[1] if len(generated_ids) > 1 and not isinstance(generated_ids[1], Exception) else None
        takeprofit_id = generated_ids[2] if len(generated_ids) > 2 and not isinstance(generated_ids[2], Exception) else None
        
        # Validate critical data
        if not external_symbol_info:
            raise OrderProcessingError(f"External symbol info not found for {symbol}")
        if not raw_market_data:
            raise OrderProcessingError("Failed to get market data")
        if not order_id:
            raise OrderProcessingError("Failed to generate order ID")
        
        leverage = Decimal(str(user_data.get('leverage', '1.0')))
        
        # Step 4: Use batch cache data or fallback to individual calls
        group_settings = None
        if batch_cache_data and batch_cache_data.get('group_symbol_settings'):
            group_settings = batch_cache_data['group_symbol_settings']
        else:
            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        
        if not group_settings:
            raise OrderProcessingError(f"Group settings not found for symbol {symbol}")

        # Step 5: ULTRA-OPTIMIZED margin calculation
        order_price = Decimal(str(order_data.get('order_price', '0')))
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
            user_id=user_id,
            order_price=order_price
        )
        if full_margin_usd is None:
            raise OrderProcessingError("Margin calculation failed")

        # Step 6: ULTRA-OPTIMIZED hedged margin calculation
        # Create simulated order object for margin calculation
        simulated_order = type('Obj', (object,), {
            'order_quantity': quantity,
            'order_type': order_type,
            'margin': full_margin_usd,
            'id': None,
            'order_id': 'NEW_ORDER_SIMULATED'
        })()
        
        # Calculate margin before and after in parallel
        margin_tasks = [
            calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders_for_symbol, get_order_model(user_type), user_type
            ),
            calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders_for_symbol + [simulated_order], get_order_model(user_type), user_type
            )
        ]
        
        margin_before_data, margin_after_data = await asyncio.gather(*margin_tasks)
        margin_before = margin_before_data["total_margin"]
        margin_after = margin_after_data["total_margin"]
        additional_margin = max(Decimal("0.0"), margin_after - margin_before)

        # Step 7: ULTRA-OPTIMIZED user locking and margin update
        if not is_barclays_live_user:
            # Lock user and update margin in one operation
            if user_type == 'demo':
                db_user_locked = await get_demo_user_by_id_with_lock(db, user_id)
            else:
                db_user_locked = await get_user_by_id_with_lock(db, user_id)

            if db_user_locked is None:
                raise OrderProcessingError("Could not lock user record.")

            if db_user_locked.wallet_balance < db_user_locked.margin + additional_margin:
                raise InsufficientFundsError("Not enough wallet balance to cover additional margin.")

            # Update margin
            original_user_margin = db_user_locked.margin
            db_user_locked.margin = (Decimal(str(db_user_locked.margin)) + additional_margin).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            db.add(db_user_locked)
            await db.commit()
            await db.refresh(db_user_locked)
            
            # Update cache in background using batch operations
            user_data_to_cache = {
                "wallet_balance": db_user_locked.wallet_balance,
                "leverage": db_user_locked.leverage,
                "group_name": db_user_locked.group_name,
                "margin": db_user_locked.margin,
            }
            asyncio.create_task(redis_pool.set_batch({
                f"user_data:{user_type}:{user_id}": user_data_to_cache
            }))

        # Step 8: Return ultra-optimized result
        order_status = "PROCESSING" if is_barclays_live_user else "OPEN"
        
        return {
            'order_id': order_id,
            'order_status': order_status,
            'order_user_id': user_id,
            'order_company_name': symbol,
            'order_type': order_type,
            'order_price': price,
            'order_quantity': quantity,
            'contract_value': contract_value,
            'margin': full_margin_usd,
            'commission': commission,
            'stop_loss': order_data.get('stop_loss'),
            'take_profit': order_data.get('take_profit'),
            'stoploss_id': stoploss_id,
            'takeprofit_id': takeprofit_id,
            'status': order_data.get('status'),
        }
    except Exception as e:
        logger.error(f"Error processing new order: {e}", exc_info=True)
        raise OrderProcessingError(f"Failed to process order: {str(e)}")

# Replace the original function with the ultra-optimized version
process_new_order = process_new_order_ultra_optimized

