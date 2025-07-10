# app/services/pending_orders.py

from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from redis.asyncio import Redis
import logging
from datetime import datetime, timezone
import json
from pydantic import BaseModel 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select  # Add this import
import asyncio
import time
import uuid
import inspect  # Added for debug function

# Import AsyncSessionLocal for database sessions
from app.database.session import AsyncSessionLocal


# Import epsilon configuration
from app.main import SLTP_EPSILON

from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,
    set_user_static_orders_cache, get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache, get_user_dynamic_portfolio_cache,
    DecimalEncoder, decode_decimal,
    publish_order_update, publish_user_data_update,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_static_orders_cache,
    get_user_static_orders_cache,
    DecimalEncoder,  # Import for JSON serialization of decimals
    # Balance/margin cache for websocket
    set_user_balance_margin_cache,
    get_user_balance_margin_cache,
)
from app.services.margin_calculator import calculate_single_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet
from app.crud import crud_order
from app.crud.user import update_user_margin, get_user_by_id, get_demo_user_by_id
from app.crud.crud_order import get_open_orders_by_user_id_and_symbol, get_order_model
from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest
from app.schemas.wallet import WalletCreate
# Import ALL necessary functions from order_processing to ensure consistency
from app.services.order_processing import (
    calculate_total_symbol_margin_contribution,  # Use THIS implementation, not the one from margin_calculator
    get_external_symbol_info,
    OrderProcessingError,
    InsufficientFundsError,
    generate_unique_10_digit_id,
    calculate_total_user_margin
)
from app.core.logging_config import orders_logger, redis_logger

logger = orders_logger

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"

# Redis key prefix for users with open orders per symbol
REDIS_USERS_WITH_ORDERS_PREFIX = "users_with_orders"

# --- Redis ZSET/HASH based Pending Orders System ---
# Key formats:
#   ZSET: pending_zset:{symbol}:{type} (score=price, value=order_id)
#   HASH: pending_hash:{order_id} (full order data as JSON)
#   Stream: pending_trigger_queue (for triggered orders)

PENDING_ZSET_PREFIX = "pending_zset"
PENDING_HASH_PREFIX = "pending_hash"
PENDING_TRIGGER_QUEUE = "pending_trigger_queue"

async def add_pending_order_redis(redis: Redis, order: dict):
    """
    Add a pending order to Redis ZSET and HASH.
    order must contain: order_id, order_company_name, order_type, order_price
    """
    symbol = order["order_company_name"]
    order_type = order["order_type"]
    order_id = order["order_id"]
    # Convert to float with 6 decimal precision
    price = round(float(order["order_price"]), 6)
    zset_key = f"{PENDING_ZSET_PREFIX}:{symbol}:{order_type}"
    hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
    

    
    # Add to ZSET (score=price, value=order_id)
    await redis.zadd(zset_key, {order_id: price})
    # Add to HASH (full order data)
    order_json = json.dumps(order, cls=DecimalEncoder)
    await redis.hset(hash_key, mapping={"data": order_json})
    


async def remove_pending_order_redis(redis: Redis, order_id: str, symbol: str, order_type: str):
    zset_key = f"{PENDING_ZSET_PREFIX}:{symbol}:{order_type}"
    hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
    # Convert order_id to string to avoid Redis encoding issues
    order_id_str = str(order_id)
    await redis.zrem(zset_key, order_id_str)
    await redis.delete(hash_key)

async def get_pending_orders_by_price(redis: Redis, symbol: str, order_type: str, price: float):
    zset_key = f"{PENDING_ZSET_PREFIX}:{symbol}:{order_type}"
    # Convert price to float with 6 decimal precision
    price_rounded = round(float(price), 6)
    
    # FIXED LOGIC: Using adjusted_ask_price for all order types
    # BUY_LIMIT: trigger when adjusted_ask_price <= pending_price (market goes down)
    # SELL_STOP: trigger when adjusted_ask_price <= pending_price (market goes down)  
    # BUY_STOP: trigger when adjusted_ask_price >= pending_price (market goes up)
    # SELL_LIMIT: trigger when adjusted_ask_price >= pending_price (market goes up)
    
    if order_type in ["BUY_LIMIT", "SELL_STOP"]:
        # For BUY_LIMIT and SELL_STOP: find orders where pending_price >= adjusted_ask_price
        # This means the market price (adjusted_ask_price) has gone down to or below the pending price
        order_ids = await redis.zrangebyscore(zset_key, price_rounded, "+inf")
    else:
        # For BUY_STOP and SELL_LIMIT: find orders where pending_price <= adjusted_ask_price
        # This means the market price (adjusted_ask_price) has gone up to or above the pending price
        order_ids = await redis.zrangebyscore(zset_key, "-inf", price_rounded)
    return [oid.decode() if isinstance(oid, bytes) else oid for oid in order_ids]

async def get_order_hash(redis: Redis, order_id: str):
    hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
    data = await redis.hget(hash_key, "data")
    if data:
        return json.loads(data, object_hook=decode_decimal)
    else:
        logger.warning(f"Order {order_id} not found in hash: {hash_key}")
        return None

async def queue_triggered_order(redis: Redis, order_id: str, trigger_price: float = None):
    """
    Queue a triggered order for processing with the trigger price.
    This ensures the execution price matches the trigger price.
    """
    if trigger_price is not None:
        await redis.xadd(PENDING_TRIGGER_QUEUE, {
            "order_id": order_id,
            "trigger_price": str(trigger_price)
        })
    else:
        await redis.xadd(PENDING_TRIGGER_QUEUE, {"order_id": order_id})

async def fetch_triggered_orders(redis: Redis, last_id: str = "0"):
    """
    Fetch triggered orders from the Redis stream.
    last_id: The ID to start reading from. Use "0" to read from the beginning.
    """
    try:
        # Use XREAD to fetch new triggered orders
        result = await redis.xread({PENDING_TRIGGER_QUEUE: last_id}, count=10, block=1000)
        # Returns list of (stream, [(id, {order_id: ...}), ...])
        if not result:
            return []
        
        stream_name, entries = result[0]
        processed_entries = []
        
        for entry_id, entry_data in entries:
            try:
                # Handle both bytes and string formats
                entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                
                # Extract order_id from entry_data - handle different formats
                order_id = None
                trigger_price = None
                if b'order_id' in entry_data:
                    order_id = entry_data[b'order_id']
                elif 'order_id' in entry_data:
                    order_id = entry_data['order_id']
                else:
                    logger.warning(f"Stream entry {entry_id} has no order_id field")
                    continue
                
                # Extract trigger_price if available
                if b'trigger_price' in entry_data:
                    trigger_price = entry_data[b'trigger_price']
                elif 'trigger_price' in entry_data:
                    trigger_price = entry_data['trigger_price']
                
                # Decode order_id if it's bytes
                if isinstance(order_id, bytes):
                    order_id = order_id.decode()
                
                # Decode trigger_price if it's bytes
                if isinstance(trigger_price, bytes):
                    trigger_price = trigger_price.decode()
                
                processed_entries.append((entry_id_str, order_id, trigger_price))
                
            except Exception as e:
                logger.error(f"Error processing stream entry {entry_id}: {e}")
                continue
                
        return processed_entries
    except Exception as e:
        logger.error(f"Error fetching triggered orders: {e}")
        return []

async def get_users_with_open_orders_for_symbol(redis_client: Redis, symbol: str) -> List[Tuple[int, str]]:
    """
    Get users with open orders for a specific symbol from cache.
    Returns list of (user_id, user_type) tuples.
    """
    try:
        cache_key = f"{REDIS_USERS_WITH_ORDERS_PREFIX}:{symbol}"
        cached_data = await redis_client.get(cache_key)
        
        if cached_data:
            try:
                data = json.loads(cached_data)
                return [(int(uid), utype) for uid, utype in data]
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Invalid cached data for symbol {symbol}")
        
        return []
    except Exception as e:
        logger.error(f"Error getting users with orders for symbol {symbol}: {e}")
        return []

async def update_users_with_orders_cache(redis_client: Redis, symbol: str, users_data: List[Tuple[int, str]]):
    """
    Update the cache of users with open orders for a specific symbol.
    """
    try:
        cache_key = f"{REDIS_USERS_WITH_ORDERS_PREFIX}:{symbol}"
        data = [[str(uid), utype] for uid, utype in users_data]
        await redis_client.setex(cache_key, 300, json.dumps(data))  # 5 minutes expiry
    except Exception as e:
        logger.error(f"Error updating users with orders cache for symbol {symbol}: {e}")

async def get_users_with_open_orders_for_symbol_from_db(db: AsyncSession, symbol: str) -> List[Tuple[int, str]]:
    """
    Get users with open orders for a specific symbol from database.
    Returns list of (user_id, user_type) tuples.
    """
    try:
        users_with_orders = []
        
        # Get live users with open orders for this symbol
        live_orders = await db.execute(
            select(UserOrder.order_user_id, User.group_name)
            .join(User)
            .where(
                UserOrder.order_company_name == symbol,
                UserOrder.order_status == 'OPEN'
            )
            .distinct()
        )
        
        for order_user_id, group_name in live_orders:
            users_with_orders.append((order_user_id, 'live'))
        
        # Get demo users with open orders for this symbol
        demo_orders = await db.execute(
            select(DemoUserOrder.order_user_id, DemoUser.group_name)
            .join(DemoUser)
            .where(
                DemoUserOrder.order_company_name == symbol,
                DemoUserOrder.order_status == 'OPEN'
            )
            .distinct()
        )
        
        for order_user_id, group_name in demo_orders:
            users_with_orders.append((order_user_id, 'demo'))
        
        return users_with_orders
    except Exception as e:
        logger.error(f"Error getting users with orders from DB for symbol {symbol}: {e}")
        return []

async def process_sltp_for_symbol(symbol: str, redis_client: Redis):
    """
    Process SL/TP for a specific symbol only.
    This is much more efficient than processing all users.
    """
    try:
        # Get users with open orders for this symbol from cache
        users_with_orders = await get_users_with_open_orders_for_symbol(redis_client, symbol)
        
        if not users_with_orders:
            return
        
        logger.debug(f"Processing SL/TP for symbol {symbol} with {len(users_with_orders)} users")
        
        # Process each user's orders for this symbol
        for user_id, user_type in users_with_orders:
            try:
                await process_user_sltp_for_symbol(user_id, user_type, symbol, redis_client)
            except Exception as e:
                logger.error(f"Error processing SL/TP for user {user_id} ({user_type}) symbol {symbol}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in process_sltp_for_symbol for {symbol}: {e}")

async def process_user_sltp_for_symbol(user_id: int, user_type: str, symbol: str, redis_client: Redis):
    """
    Process SL/TP for a specific user and symbol.
    """
    try:
        # Create a new database session for this user
        from app.database.session import AsyncSessionLocal
        async with AsyncSessionLocal() as db:
            # Get user's open orders for this symbol
            order_model = get_order_model(user_type)
            open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(
                db, user_id, symbol, order_model
            )
            
            if not open_orders:
                return
            
            # Filter orders that have SL or TP set
            sl_tp_orders = [
                order for order in open_orders
                if (order.stop_loss and Decimal(str(order.stop_loss)) > 0) or
                   (order.take_profit and Decimal(str(order.take_profit)) > 0)
            ]
            
            if not sl_tp_orders:
                return
            
            # Process each order for SL/TP
            for order in sl_tp_orders:
                try:
                    await process_order_stoploss_takeprofit(db, redis_client, order, user_type)
                except Exception as e:
                    logger.error(f"Error processing SL/TP for order {order.order_id}: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in process_user_sltp_for_symbol for user {user_id} symbol {symbol}: {e}")

async def process_sltp_batch(symbols: List[str], redis_client: Redis):
    """
    Process SL/TP for multiple symbols in batches to reduce database load.
    """
    try:
        if not symbols:
            return
        
        # logger.info(f"Processing SL/TP batch for {len(symbols)} symbols: {symbols}")
        
        # Process symbols in parallel with limited concurrency
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent symbol processing
        
        async def process_symbol_with_semaphore(symbol: str):
            async with semaphore:
                await process_sltp_for_symbol(symbol, redis_client)
        
        # Create tasks for all symbols
        tasks = [process_symbol_with_semaphore(symbol) for symbol in symbols]
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"Error in process_sltp_batch: {e}")

async def update_users_with_orders_cache_periodic(redis_client: Redis):
    """
    Periodic task to update the cache of users with open orders per symbol.
    This runs every few minutes to keep the cache fresh.
    """
    try:
        from app.database.session import AsyncSessionLocal
        async with AsyncSessionLocal() as db:
            # Get all symbols that have open orders
            symbols_with_orders = await get_all_symbols_with_open_orders(db)
            
            for symbol in symbols_with_orders:
                try:
                    users_data = await get_users_with_open_orders_for_symbol_from_db(db, symbol)
                    await update_users_with_orders_cache(redis_client, symbol, users_data)
                except Exception as e:
                    logger.error(f"Error updating cache for symbol {symbol}: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in update_users_with_orders_cache_periodic: {e}")

async def get_all_symbols_with_open_orders(db: AsyncSession) -> List[str]:
    """
    Get all symbols that have open orders.
    """
    try:
        symbols = set()
        
        # Get symbols from live orders
        live_symbols = await db.execute(
            select(UserOrder.order_company_name)
            .where(UserOrder.order_status == 'OPEN')
            .distinct()
        )
        symbols.update([s[0] for s in live_symbols])
        
        # Get symbols from demo orders
        demo_symbols = await db.execute(
            select(DemoUserOrder.order_company_name)
            .where(DemoUserOrder.order_status == 'OPEN')
            .distinct()
        )
        symbols.update([s[0] for s in demo_symbols])
        
        return list(symbols)
    except Exception as e:
        logger.error(f"Error getting symbols with open orders: {e}")
        return []

async def check_order_priority(order, current_price: Decimal) -> float:
    """
    Calculate priority for an order based on how close it is to SL/TP levels.
    Lower values indicate higher priority.
    """
    try:
        min_distance = float('inf')
        
        if order.stop_loss and Decimal(str(order.stop_loss)) > 0:
            stop_loss = Decimal(str(order.stop_loss))
            distance_to_sl = abs(current_price - stop_loss)
            min_distance = min(min_distance, distance_to_sl)
        
        if order.take_profit and Decimal(str(order.take_profit)) > 0:
            take_profit = Decimal(str(order.take_profit))
            distance_to_tp = abs(current_price - take_profit)
            min_distance = min(min_distance, distance_to_tp)
        
        return min_distance if min_distance != float('inf') else 0.0
    except Exception as e:
        logger.error(f"Error calculating order priority: {e}")
        return 0.0

async def remove_pending_order(redis_client: Redis, order_id: str, symbol: str, order_type: str, user_id: str):
    """Removes a pending order from Redis using ZSET/HASH for non-Barclays users."""
    # TODO: If Barclays, use old logic. For now, always use new logic.
    await remove_pending_order_redis(redis_client, order_id, symbol, order_type)

async def get_all_pending_orders_from_redis(redis_client: Redis) -> List[Dict[str, Any]]:
    """
    Get all pending orders from Redis for cleanup purposes.
    Updated for ZSET/HASH system.
    """
    try:
        all_pending_orders = []
        
        # Pattern for keys that store pending orders as Hashes in new system
        pattern = f"{PENDING_HASH_PREFIX}:*"
        keys = await redis_client.keys(pattern)
        
        for key in keys:
            try:
                # Get order data from hash
                order_data = await redis_client.hget(key, "data")
                if order_data:
                    try:
                        order = json.loads(order_data, object_hook=decode_decimal)
                        all_pending_orders.append(order)
                    except json.JSONDecodeError:
                        logger.error(f"[REDIS_CLEANUP] Failed to decode JSON for key {key}: {order_data}")
                        continue
            except Exception as e:
                logger.error(f"[REDIS_CLEANUP] Error processing Redis key {key}: {e}")
                continue
        
        return all_pending_orders
        
    except Exception as e:
        redis_logger.error(f"[REDIS_CLEANUP] Error getting all pending orders from Redis: {e}")
        return []

async def add_pending_order(
    redis_client: Redis, 
    pending_order_data: Dict[str, Any]
) -> None:
    """Adds a pending order to Redis using ZSET/HASH for non-Barclays users."""
    # TODO: If Barclays, use old logic. For now, always use new logic.
    await add_pending_order_redis(redis_client, pending_order_data)

def decimal_to_float_6dp(value) -> float:
    """
    Convert Decimal or any numeric value to float with 6 decimal precision.
    """
    if isinstance(value, Decimal):
        return round(float(value), 6)
    elif isinstance(value, (int, float)):
        return round(float(value), 6)
    else:
        return round(float(str(value)), 6)

async def trigger_pending_order(
    db,
    redis_client: 'Redis',
    order: dict,
    current_price: 'Decimal'
) -> None:
    """
    Trigger a pending order that has already been matched by price.
    Updates the order status to 'OPEN' in the database,
    adjusts user margin, and updates portfolio caches.
    Uses the same margin/hedging logic as process_new_order.
    """
    from decimal import Decimal, ROUND_HALF_UP
    from app.core.logging_config import orders_logger
    from app.crud.user import get_user_by_id, get_demo_user_by_id, update_user_margin
    from app.crud import crud_order
    from app.crud.crud_order import get_order_model
    from app.services.margin_calculator import calculate_single_order_margin
    from app.services.order_processing import calculate_total_symbol_margin_contribution
    from app.core.cache import get_user_data_cache, get_group_symbol_settings_cache, set_user_balance_margin_cache
    from app.core.firebase import get_latest_market_data
    import asyncio
    
    order_id = order['order_id']
    user_id = order['order_user_id']
    user_type = order['user_type']
    symbol = order['order_company_name']
    order_type_original = order['order_type']
    placed_price = Decimal(str(order.get('order_price', '0')))
    quantity = Decimal(str(order.get('order_quantity', '0')))

    # Convert order_type from pending to market order type
    order_type_mapping = {
        'BUY_LIMIT': 'BUY',
        'BUY_STOP': 'BUY', 
        'SELL_LIMIT': 'SELL',
        'SELL_STOP': 'SELL'
    }
    new_order_type = order_type_mapping.get(order_type_original, order_type_original)

    try:
        logger = orders_logger
        logger.info(f"[PENDING_ORDER] Starting execution of order {order_id} for user {user_id}")
        logger.info(f"[PENDING_ORDER] Order {order_id}: Placed at {placed_price}, Triggered at {current_price}")
        logger.info(f"[PENDING_ORDER] Order {order_id}: Type {order_type_original} -> {new_order_type}")

        # 1. Fetch user data
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.error(f"[PENDING_ORDER] User data not found for user {user_id}")
            return
        leverage = Decimal(str(user_data.get('leverage', '1.0')))
        group_name = user_data.get('group_name')

        # 2. Fetch group settings
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            logger.error(f"[PENDING_ORDER] Group settings not found for symbol {symbol}")
            return

        # 3. Fetch external symbol info
        from app.services.margin_calculator import get_external_symbol_info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            logger.error(f"[PENDING_ORDER] External symbol info not found for {symbol}")
            return

        # 4. Fetch raw market data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            logger.error(f"[PENDING_ORDER] Failed to get market data")
            return

        # 5. Calculate margin/commission for this order (as in process_new_order)
        full_margin_usd, exec_price, contract_value, commission = await calculate_single_order_margin(
            redis_client=redis_client,
            symbol=symbol,
            order_type=new_order_type,
            quantity=quantity,
            user_leverage=leverage,
            group_settings=group_settings,
            external_symbol_info=external_symbol_info,
            raw_market_data=raw_market_data,
            db=db,
            user_id=user_id,
            order_price=current_price
        )
        if full_margin_usd is None:
            logger.error(f"[PENDING_ORDER] Margin calculation failed for order {order_id}")
            return

        # 6. Fetch all open orders for this symbol
        order_model = get_order_model(user_type)
        open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
            db, user_id, symbol, order_model
        )

        # 7. Simulate the triggered order as an object
        simulated_order = type('Obj', (object,), {
            'order_quantity': quantity,
            'order_type': new_order_type,
            'margin': full_margin_usd,
            'id': None,
            'order_id': order_id
        })()

        # 8. Calculate margin before and after (hedging logic)
        # margin_before_data, margin_after_data = await asyncio.gather(
        #     calculate_total_symbol_margin_contribution(
        #         db, redis_client, user_id, symbol, open_orders_for_symbol, order_model, user_type
        #     ),
        #     calculate_total_symbol_margin_contribution(
        #         db, redis_client, user_id, symbol, open_orders_for_symbol + [simulated_order], order_model, user_type
        #     )
        # )
        # margin_before = margin_before_data["total_margin"]
        # margin_after = margin_after_data["total_margin"]
        # additional_margin = max(Decimal("0.0"), margin_after - margin_before)
        # logger.info(f"[PENDING_ORDER] Margin before: {margin_before}, after: {margin_after}, additional: {additional_margin}")

        # 9. Update user margin - Calculate total user margin including all symbols
        # Get current user margin from database
        if user_type == "live":
            db_user = await get_user_by_id(db, user_id)
        else:
            db_user = await get_demo_user_by_id(db, user_id)

        # Calculate total margin across all symbols
        from app.services.order_processing import calculate_total_user_margin
        total_user_margin = await calculate_total_user_margin(db, redis_client, user_id, user_type)
        # total_user_margin = margin_after + additional_margin
        logger.info(f"[PENDING_ORDER] Total user margin: {total_user_margin}")
        # Update margin in DB with the total user margin
        await update_user_margin(db, user_id, user_type, total_user_margin)
        # await db.commit()
        await set_user_balance_margin_cache(redis_client, user_id, db_user.wallet_balance, total_user_margin)
        logger.info(f"[PENDING_ORDER] Updated user {user_id} total margin to {total_user_margin} (includes all symbols)")

        # 10. Update order in database (status, margin, commission, price, etc.)
        db_order = await crud_order.get_order_by_id(db, order_id, order_model)
        if not db_order:
            logger.error(f"[PENDING_ORDER] DB order not found for order_id {order_id}")
            return
        update_fields = {
            'order_status': 'OPEN',
            'order_type': new_order_type,
            'order_price': current_price,
            'margin': full_margin_usd,
            'commission': commission,
            'triggered_price': current_price,
            'stop_loss': None,
            'take_profit': None,
        }
        updated_order = await crud_order.update_order_with_tracking(
            db, db_order, update_fields, user_id, user_type, "PENDING_TO_OPEN"
        )
        await db.commit()
        await db.refresh(updated_order)
        await db.refresh(db_user)
        logger.info(f"[PENDING_ORDER] Order {order_id} status is OPEN, margin/commission updated")

        # 11. Remove from Redis pending orders
        from app.services.pending_orders import remove_pending_order
        await remove_pending_order(redis_client, order_id, symbol, order_type_original, str(user_id))

        # 12. Update user data cache and balance/margin cache
        db_user = await (get_demo_user_by_id(db, user_id) if user_type == 'demo' else get_user_by_id(db, user_id))
        await set_user_balance_margin_cache(redis_client, user_id, db_user.wallet_balance, total_user_margin)
        user_data_to_cache = {
            "id": db_user.id,
            "email": getattr(db_user, 'email', None),
            "group_name": db_user.group_name,
            "leverage": db_user.leverage,
            "user_type": user_type,
            "account_number": getattr(db_user, 'account_number', None),
            "wallet_balance": db_user.wallet_balance,
            "margin": total_user_margin,  # Use the calculated total margin
        }
        from app.core.cache import set_user_data_cache
        await set_user_data_cache(redis_client, user_id, user_data_to_cache, user_type)

        # 13. Update users with orders cache for SL/TP processing
        from app.services.pending_orders import add_user_to_symbol_cache
        await add_user_to_symbol_cache(redis_client, symbol, user_id, user_type)

        # 14. Publish updates to websocket
        from app.core.cache import publish_order_update, publish_user_data_update, publish_market_data_trigger
        await publish_order_update(redis_client, user_id)
        await publish_user_data_update(redis_client, user_id)
        await publish_market_data_trigger(redis_client)  # Trigger immediate websocket update

        # 15. Update static orders cache for websocket
        from app.api.v1.endpoints.market_data_ws import update_static_orders_cache
        await update_static_orders_cache(user_id, db, redis_client, user_type)
        
        # 16. FIXED: Ensure balance/margin cache is properly updated and verified
        try:
            # Verify the cache was set correctly
            verify_cache = await get_user_balance_margin_cache(redis_client, user_id)
            if verify_cache:
                cached_margin = verify_cache.get("margin", "0.0")
                if cached_margin != str(total_user_margin):
                    logger.warning(f"[PENDING_ORDER] Cache verification failed for user {user_id}, retrying...")
                    await set_user_balance_margin_cache(redis_client, user_id, db_user.wallet_balance, total_user_margin)
                else:
                    logger.info(f"[PENDING_ORDER] Cache verification successful for user {user_id}")
            else:
                logger.error(f"[PENDING_ORDER] Cache verification failed for user {user_id}: cache not found")
                # Retry setting the cache
                await set_user_balance_margin_cache(redis_client, user_id, db_user.wallet_balance, total_user_margin)
        except Exception as cache_error:
            logger.error(f"[PENDING_ORDER] Error during cache verification for user {user_id}: {cache_error}")

        logger.info(f"[PENDING_ORDER] Successfully executed order {order_id} for user {user_id}")
        logger.info(f"[PENDING_ORDER] Order {order_id}: Final price={exec_price}, margin={full_margin_usd}, commission={commission}")
        logger.info(f"[PENDING_ORDER_SUMMARY] Order {order_id} executed: {order_type_original} -> {new_order_type}, Placed: {placed_price}, Triggered: {current_price}, Margin: {full_margin_usd}, Commission: {commission}, AdditionalMargin: {None}")

    except Exception as e:
        logger.error(f"[PENDING_ORDER] Error triggering pending order {order_id}: {e}", exc_info=True)
        raise

async def check_and_trigger_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis
) -> None:
    """
    Check all open orders for stop loss and take profit conditions.
    This function is called by the background task triggered by market data updates.
    Optimized to fetch open orders from cache if available.
    Only checks orders that have SL or TP set (>0).
    """
    logger = orders_logger
    try:
        from app.crud.user import get_all_active_users_both
        live_users, demo_users = await get_all_active_users_both(db)
        all_users = [(u.id, "live") for u in live_users] + [(u.id, "demo") for u in demo_users]
        for user_id, user_type in all_users:
            try:
                static_orders = await get_user_static_orders_cache(redis_client, user_id)
                open_orders = []
                if static_orders and static_orders.get("open_orders"):
                    open_orders = static_orders["open_orders"]
                else:
                    order_model = get_order_model(user_type)
                    open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                    for o in open_orders_orm:
                        open_orders.append({
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', None),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        })
                # Only check orders with SL or TP set (>0)
                def get_attr(o, key):
                    if isinstance(o, dict):
                        return o.get(key)
                    return getattr(o, key, None)
                sl_tp_orders = [
                    o for o in open_orders
                    if (get_attr(o, 'stop_loss') and Decimal(str(get_attr(o, 'stop_loss'))) > 0)
                    or (get_attr(o, 'take_profit') and Decimal(str(get_attr(o, 'take_profit'))) > 0)
                ]
                for order in sl_tp_orders:
                    try:
                        # Verify order has required fields
                        order_id = get_attr(order, 'order_id')
                        order_user_id = get_attr(order, 'order_user_id')
                        
                        if not order_id or not order_user_id:
                            logger.error(f"[SLTP_CHECK] Order {order_id or 'unknown'} has missing required fields: order_id={order_id}, order_user_id={order_user_id}")
                            continue
                            
                        # Verify user exists before processing
                        user = None
                        if user_type == 'live':
                            from app.crud.user import get_user_by_id
                            user = await get_user_by_id(db, order_user_id, user_type='live')
                            if not user:
                                # Try demo table if live lookup fails
                                from app.crud.user import get_demo_user_by_id
                                user = await get_demo_user_by_id(db, order_user_id)
                                if user:
                                    # If found in demo table, update user_type
                                    user_type = 'demo'
                        else:
                            from app.crud.user import get_demo_user_by_id
                            user = await get_demo_user_by_id(db, order_user_id)
                            if not user:
                                # Try live table if demo lookup fails
                                from app.crud.user import get_user_by_id
                                user = await get_user_by_id(db, order_user_id, user_type='live')
                                if user:
                                    # If found in live table, update user_type
                                    user_type = 'live'
                            
                        if not user:
                            logger.error(f"[SLTP_CHECK] Skipping order {order_id} - User {order_user_id} not found in either live or demo tables")
                            continue
                            
                        await process_order_stoploss_takeprofit(db, redis_client, order, user_type)
                    except Exception as e:
                        logger.error(f"[SLTP_CHECK] Error processing order {get_attr(order, 'order_id')}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"[SLTP_CHECK] Error processing user {user_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[SLTP_CHECK] Error in check_and_trigger_stoploss_takeprofit: {e}", exc_info=True)

async def process_order_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis,
    order,
    user_type: str
) -> None:
    """
    Process a single order for stop loss and take profit conditions.
    Enhanced with epsilon accuracy to handle floating-point precision issues.
    Accepts both ORM and dict order objects.
    """
    logger = orders_logger
    try:
        # Support both ORM and dict order objects
        def get_attr(o, key):
            if isinstance(o, dict):
                return o.get(key)
            return getattr(o, key, None)
            
        # Verify required fields
        order_id = get_attr(order, 'order_id')
        order_user_id = get_attr(order, 'order_user_id')
        symbol = get_attr(order, 'order_company_name')
        
        if not all([order_id, order_user_id, symbol]):
            logger.error(f"[SLTP_CHECK] Order {order_id or 'unknown'} missing required fields: order_id={order_id}, order_user_id={order_user_id}, symbol={symbol}")
            return
            
        # Get user's group name
        user = None
        if user_type == 'live':
            from app.crud.user import get_user_by_id
            user = await get_user_by_id(db, order_user_id, user_type='live')
            if not user:
                # Try demo table if live lookup fails
                from app.crud.user import get_demo_user_by_id
                user = await get_demo_user_by_id(db, order_user_id)
                if user:
                    # If found in demo table, update user_type
                    user_type = 'demo'
        else:
            from app.crud.user import get_demo_user_by_id
            user = await get_demo_user_by_id(db, order_user_id)
            if not user:
                # Try live table if demo lookup fails
                from app.crud.user import get_user_by_id
                user = await get_user_by_id(db, order_user_id, user_type='live')
                if user:
                    # If found in live table, update user_type for this order
                    user_type = 'live'

        if not user:
            logger.error(f"[SLTP_CHECK] User {order_user_id} not found in either live or demo tables for order {order_id}")
            return
        group_name = user.group_name
        if not group_name:
            logger.error(f"[SLTP_CHECK] No group name found for user {order_user_id}")
            return
        # Get adjusted market price
        adjusted_price = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        if not adjusted_price:
            logger.warning(f"[SLTP_CHECK] No adjusted price available for {symbol} in group {group_name}")
            return
        buy_price = Decimal(str(adjusted_price.get('buy', '0')))
        sell_price = Decimal(str(adjusted_price.get('sell', '0')))
        epsilon = Decimal(SLTP_EPSILON)
        # Check stop loss
        stop_loss = get_attr(order, 'stop_loss')
        order_type = get_attr(order, 'order_type')
        take_profit = get_attr(order, 'take_profit')
        if stop_loss and Decimal(str(stop_loss)) > 0:
            stop_loss = Decimal(str(stop_loss))
            if order_type == 'BUY':
                price_diff = abs(sell_price - stop_loss)
                exact_match = sell_price <= stop_loss
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Stop loss triggered for BUY order {order_id} at {sell_price} <= {stop_loss} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, sell_price, 'STOP_LOSS', user_type)
            elif order_type == 'SELL':
                price_diff = abs(buy_price - stop_loss)
                exact_match = buy_price >= stop_loss
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Stop loss triggered for SELL order {order_id} at {buy_price} >= {stop_loss} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, buy_price, 'STOP_LOSS', user_type)
        # Check take profit
        if take_profit and Decimal(str(take_profit)) > 0:
            take_profit = Decimal(str(take_profit))
            if order_type == 'BUY':
                price_diff = abs(sell_price - take_profit)
                exact_match = sell_price >= take_profit
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Take profit triggered for BUY order {order_id} at {sell_price} >= {take_profit} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, sell_price, 'TAKE_PROFIT', user_type)
            elif order_type == 'SELL':
                price_diff = abs(buy_price - take_profit)
                exact_match = buy_price <= take_profit
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Take profit triggered for SELL order {order_id} at {buy_price} <= {take_profit} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, buy_price, 'TAKE_PROFIT', user_type)
    except Exception as e:
        logger.error(f"[SLTP_CHECK] Error processing order {get_attr(order, 'order_id')}: {e}", exc_info=True)

async def get_last_known_price(redis_client: Redis, symbol: str) -> dict:
    """
    Get the last known price for a symbol from Redis.
    Returns a dictionary with bid (b) and ask (a) prices.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for getting last known price")
            return None
            
        # Try to get the price from Redis
        price_key = f"market_data:{symbol}"
        price_data = await redis_client.get(price_key)
        
        if not price_data:
            return None
            
        try:
            price_dict = json.loads(price_data)
            return price_dict
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in price data for {symbol}: {price_data}")
            return None
    except Exception as e:
        logger.error(f"Error getting last known price for {symbol}: {e}", exc_info=True)
        return None

async def get_user_data_cache(redis_client: Redis, user_id: int, db: AsyncSession, user_type: str = 'live') -> dict:
    """
    Get user data from cache or database.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for getting user data for user {user_id}")
            # Fallback to database
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            
            if user_type == 'live':
                user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, user_id)
                
            if not user:
                return {}
                
            return {
                "id": user.id,
                "group_name": getattr(user, 'group_name', None),
                "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
                "margin": str(user.margin) if hasattr(user, 'margin') else "0"
            }
        
        # Try to get from cache
        user_key = f"user_data:{user_type}:{user_id}"
        user_data = await redis_client.get(user_key)
        
        if user_data:
            try:
                return json.loads(user_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in user data for user {user_id}: {user_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.user import get_user_by_id, get_demo_user_by_id
        
        if user_type == 'live':
            user = await get_user_by_id(db, user_id, user_type=user_type)
        else:
            user = await get_demo_user_by_id(db, user_id)
            
        if not user:
            return {}
            
        user_data = {
            "id": user.id,
            "group_name": getattr(user, 'group_name', None),
            "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
            "margin": str(user.margin) if hasattr(user, 'margin') else "0"
        }
        
        # Cache the user data
        try:
            await redis_client.set(user_key, json.dumps(user_data), ex=300)  # 5 minutes expiry
        except Exception as e:
            logger.error(f"Error caching user data for user {user_id}: {e}", exc_info=True)
        
        return user_data
    except Exception as e:
        logger.error(f"Error getting user data for user {user_id}: {e}", exc_info=True)
        return {}

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> dict:
    """
    Get group settings from cache or database.
    """
    try:
        if not group_name:
            return {}
            
        if not redis_client:
            logger.warning(f"Redis client not available for getting group settings for group {group_name}")
            # Fallback to database
            from app.crud.group import get_group_by_name
            from sqlalchemy.ext.asyncio import AsyncSession
            from app.database.session import AsyncSessionLocal
            
            async with AsyncSessionLocal() as db:
                group = await get_group_by_name(db, group_name)
                if not group:
                    return {}
                
                # Handle case where get_group_by_name returns a list
                group_obj = group[0] if isinstance(group, list) else group
                    
                return {
                    "id": group_obj.id,
                    "name": group_obj.name,
                    "sending_orders": getattr(group_obj, 'sending_orders', None)
                }
        
        # Try to get from cache
        group_key = f"group_settings:{group_name}"
        group_data = await redis_client.get(group_key)
        
        if group_data:
            try:
                return json.loads(group_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in group data for group {group_name}: {group_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.group import get_group_by_name
        from sqlalchemy.ext.asyncio import AsyncSession
        from app.database.session import AsyncSessionLocal
        
        async with AsyncSessionLocal() as db:
            group = await get_group_by_name(db, group_name)
            if not group:
                return {}
            
            # Handle case where get_group_by_name returns a list
            group_obj = group[0] if isinstance(group, list) else group
                
            group_data = {
                "id": group_obj.id,
                "name": group_obj.name,
                "sending_orders": getattr(group_obj, 'sending_orders', None)
            }
            
            # Cache the group data
            try:
                await redis_client.set(group_key, json.dumps(group_data), ex=300)  # 5 minutes expiry
            except Exception as e:
                logger.error(f"Error caching group data for group {group_name}: {e}", exc_info=True)
            
            return group_data
    except Exception as e:
        logger.error(f"Error getting group settings for group {group_name}: {e}", exc_info=True)
        return {}

async def update_user_static_orders(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user after order changes.
    This includes both open and pending orders.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        order_model = get_order_model(user_type)
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                       for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                   'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.now().isoformat()}

async def update_users_with_orders_cache_on_order_change(redis_client: Redis, symbol: str):
    """
    Update the cache of users with open orders for a specific symbol when an order is created or closed.
    This ensures the cache stays fresh for immediate SL/TP processing.
    """
    try:
        from app.database.session import AsyncSessionLocal
        async with AsyncSessionLocal() as db:
            users_data = await get_users_with_open_orders_for_symbol_from_db(db, symbol)
            await update_users_with_orders_cache(redis_client, symbol, users_data)
    except Exception as e:
        logger.error(f"Error updating users with orders cache for symbol {symbol}: {e}")

async def remove_user_from_symbol_cache(redis_client: Redis, symbol: str, user_id: int, user_type: str):
    """
    Remove a specific user from the symbol cache when they no longer have open orders for that symbol.
    """
    try:
        cache_key = f"{REDIS_USERS_WITH_ORDERS_PREFIX}:{symbol}"
        cached_data = await redis_client.get(cache_key)
        
        if cached_data:
            try:
                data = json.loads(cached_data)
                # Remove the specific user
                data = [[uid, utype] for uid, utype in data if not (int(uid) == user_id and utype == user_type)]
                await redis_client.setex(cache_key, 300, json.dumps(data))
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Invalid cached data for symbol {symbol}, refreshing cache")
                await update_users_with_orders_cache_on_order_change(redis_client, symbol)
    except Exception as e:
        logger.error(f"Error removing user from symbol cache: {e}")

async def add_user_to_symbol_cache(redis_client: Redis, symbol: str, user_id: int, user_type: str):
    """
    Add a specific user to the symbol cache when they get their first open order for that symbol.
    """
    try:
        cache_key = f"{REDIS_USERS_WITH_ORDERS_PREFIX}:{symbol}"
        cached_data = await redis_client.get(cache_key)
        
        if cached_data:
            try:
                data = json.loads(cached_data)
                # Check if user already exists
                user_exists = any(int(uid) == user_id and utype == user_type for uid, utype in data)
                if not user_exists:
                    data.append([str(user_id), user_type])
                    await redis_client.setex(cache_key, 300, json.dumps(data))
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Invalid cached data for symbol {symbol}, refreshing cache")
                await update_users_with_orders_cache_on_order_change(redis_client, symbol)
        else:
            # Cache doesn't exist, create it
            await update_users_with_orders_cache_on_order_change(redis_client, symbol)
    except Exception as e:
        logger.error(f"Error adding user to symbol cache: {e}")

async def close_order(
    db: AsyncSession,
    redis_client: Redis,
    order,
    execution_price: Decimal,
    close_reason: str,
    user_type: str
) -> None:
    """
    Close an order with the given execution price and reason.
    This function includes robust margin calculations, commission handling, and wallet transactions.
    """
    logger = orders_logger
    
    def get_attr(o, key):
        if isinstance(o, dict):
            return o.get(key)
        return getattr(o, key, None)
    
    try:
        # Extract all needed fields using get_attr
        order_id = get_attr(order, 'order_id')
        order_user_id = get_attr(order, 'order_user_id')
        order_company_name = get_attr(order, 'order_company_name')
        order_type = get_attr(order, 'order_type')
        order_status = get_attr(order, 'order_status')
        order_price = get_attr(order, 'order_price')
        order_quantity = get_attr(order, 'order_quantity')
        margin = get_attr(order, 'margin')
        contract_value = get_attr(order, 'contract_value')
        stop_loss = get_attr(order, 'stop_loss')
        take_profit = get_attr(order, 'take_profit')
        close_price = execution_price
        net_profit = get_attr(order, 'net_profit')
        swap = get_attr(order, 'swap')
        commission = get_attr(order, 'commission')
        cancel_message = get_attr(order, 'cancel_message')
        close_message = get_attr(order, 'close_message')
        cancel_id = get_attr(order, 'cancel_id')
        close_id = get_attr(order, 'close_id')
        modify_id = get_attr(order, 'modify_id')
        stoploss_id = get_attr(order, 'stoploss_id')
        takeprofit_id = get_attr(order, 'takeprofit_id')
        stoploss_cancel_id = get_attr(order, 'stoploss_cancel_id')
        takeprofit_cancel_id = get_attr(order, 'takeprofit_cancel_id')
        order_status = get_attr(order, 'order_status')

        # Always use ORM order object for margin and DB updates
        db_order_obj = order if not isinstance(order, dict) else None
        if db_order_obj is None:
            from app.crud.crud_order import get_order_by_id
            db_order_obj = await get_order_by_id(db, order_id, get_order_model(user_type))
            if db_order_obj is None:
                logger.error(f"[ORDER_CLOSE] Could not fetch ORM order object for order_id={order_id} (user_id={order_user_id})")
                return

        # Lock user for atomic operations
        if user_type == 'live':
            from app.crud.user import get_user_by_id_with_lock
            db_user_locked = await get_user_by_id_with_lock(db, order_user_id)
        else:
            from app.crud.user import get_demo_user_by_id_with_lock
            db_user_locked = await get_demo_user_by_id_with_lock(db, order_user_id)
        if db_user_locked is None:
            logger.error(f"[ORDER_CLOSE] Could not retrieve and lock user record for user ID: {order_user_id}")
            return

        # Get all open orders for this symbol to recalculate margin
        from app.crud.crud_order import get_open_orders_by_user_id_and_symbol, update_order_with_tracking
        order_model_class = get_order_model(user_type)
        all_open_orders_for_symbol = await get_open_orders_by_user_id_and_symbol(
            db=db, user_id=db_user_locked.id, symbol=order_company_name, order_model=order_model_class
        )

        # Calculate margin before closing this order
        margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=order_company_name,
            open_positions_for_symbol=all_open_orders_for_symbol,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_before_recalc = margin_before_recalc_dict["total_margin"]
        current_overall_margin = Decimal(str(db_user_locked.margin))
        non_symbol_margin = current_overall_margin - margin_before_recalc

        # Calculate margin after closing this order
        remaining_orders_for_symbol_after_close = [
            o for o in all_open_orders_for_symbol
            if str(get_attr(o, 'order_id')) != str(order_id)
        ]
        margin_after_symbol_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=order_company_name,
            open_positions_for_symbol=remaining_orders_for_symbol_after_close,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_after_symbol_recalc = margin_after_symbol_recalc_dict["total_margin"]

        # Update user's margin
        db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

        # Get contract size and profit currency from ExternalSymbolInfo
        from app.database.models import ExternalSymbolInfo
        from sqlalchemy.future import select
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_company_name))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()
        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
            logger.error(f"[ORDER_CLOSE] Missing critical ExternalSymbolInfo for symbol {order_company_name}.")
            return
        contract_size = Decimal(str(ext_symbol_info.contract_size))
        profit_currency = ext_symbol_info.profit.upper()

        # Get group settings for commission calculation
        from app.core.cache import get_group_symbol_settings_cache
        group_settings = await get_group_symbol_settings_cache(redis_client, db_user_locked.group_name, order_company_name)
        if not group_settings:
            logger.error("[ORDER_CLOSE] Group settings not found for commission calculation.")
            return
        commission_type = int(group_settings.get('commision_type', -1))
        commission_value_type = int(group_settings.get('commision_value_type', -1))
        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
        existing_entry_commission = Decimal(str(commission or "0.0"))
        exit_commission = Decimal("0.0")
        quantity = Decimal(str(order_quantity))
        entry_price = Decimal(str(order_price))
        if commission_type in [0, 2]:
            if commission_value_type == 0:
                exit_commission = quantity * commission_rate
            elif commission_value_type == 1:
                calculated_exit_contract_value = quantity * contract_size * close_price
                if calculated_exit_contract_value > Decimal("0.0"):
                    exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
        total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Calculate profit/loss
        if order_type == "BUY":
            profit = (close_price - entry_price) * quantity * contract_size
        elif order_type == "SELL":
            profit = (entry_price - close_price) * quantity * contract_size
        else:
            logger.error("[ORDER_CLOSE] Invalid order type.")
            return
        profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, order_id, "PnL on Close", db=db, redis_client=redis_client)
        if profit_currency != "USD" and profit_usd == profit:
            logger.error(f"[ORDER_CLOSE] Order {order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
            return
        from app.services.order_processing import generate_unique_10_digit_id
        close_id_val = await generate_unique_10_digit_id(db, order_model_class, 'close_id')

        # Update order fields on ORM object
        db_order_obj.order_status = "CLOSED"
        db_order_obj.close_price = close_price
        db_order_obj.net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        db_order_obj.swap = swap or Decimal("0.0")
        db_order_obj.commission = total_commission_for_trade
        db_order_obj.close_id = close_id_val
        db_order_obj.close_message = f"Closed automatically due to {close_reason}"

        # Update order with tracking
        await update_order_with_tracking(
            db=db,
            db_order=db_order_obj,
            update_fields={
                "order_status": db_order_obj.order_status,
                "close_price": db_order_obj.close_price,
                "close_id": db_order_obj.close_id,
                "net_profit": db_order_obj.net_profit,
                "swap": db_order_obj.swap,
                "commission": db_order_obj.commission,
                "close_message": db_order_obj.close_message
            },
            user_id=db_user_locked.id,
            user_type=user_type,
            action_type=f"AUTO_{close_reason.upper()}_CLOSE"
        )

        # Update user's wallet balance
        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
        swap_amount = db_order_obj.swap
        db_user_locked.wallet_balance = (original_wallet_balance + db_order_obj.net_profit - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

        # Create wallet transactions
        from app.database.models import Wallet
        from app.schemas.wallet import WalletCreate
        transaction_time = datetime.now(timezone.utc)
        wallet_common_data = {
            "symbol": order_company_name,
            "order_quantity": quantity,
            "is_approved": 1,
            "order_type": order_type,
            "transaction_time": transaction_time,
            "order_id": str(order_id)
        }
        if user_type == 'demo':
            wallet_common_data["demo_user_id"] = db_user_locked.id
        else:
            wallet_common_data["user_id"] = db_user_locked.id
        if db_order_obj.net_profit != Decimal("0.0"):
            transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order_obj.net_profit, description=f"P/L for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
        if total_commission_for_trade > Decimal("0.0"):
            transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
        if swap_amount != Decimal("0.0"):
            transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))

        await db.commit()
        await db.refresh(db_order_obj)
        await db.refresh(db_user_locked)
        logger.info(f"[ORDER_CLOSE] Successfully closed order {order_id} for user {order_user_id}")

        # --- Websocket and cache updates ---
        user_type_str = 'demo' if user_type == 'demo' else 'live'
        user_data_to_cache = {
            "id": db_user_locked.id,
            "email": getattr(db_user_locked, 'email', None),
            "group_name": db_user_locked.group_name,
            "leverage": db_user_locked.leverage,
            "user_type": user_type_str,
            "account_number": getattr(db_user_locked, 'account_number', None),
            "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin,
            "first_name": getattr(db_user_locked, 'first_name', None),
            "last_name": getattr(db_user_locked, 'last_name', None),
            "country": getattr(db_user_locked, 'country', None),
            "phone_number": getattr(db_user_locked, 'phone_number', None),
        }
        from app.api.v1.endpoints.market_data_ws import update_static_orders_cache
        # await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)
        # from app.api.v1.endpoints.orders import update_user_static_orders, publish_order_update, publish_user_data_update, publish_market_data_trigger
        # await update_user_static_orders(db_user_locked.id, db, redis_client, user_type_str)
        # await publish_order_update(redis_client, db_user_locked.id)
        # await publish_user_data_update(redis_client, db_user_locked.id)
        # await publish_market_data_trigger(redis_client)
        await set_user_data_cache(redis_client, order_user_id, user_data_to_cache, user_type)
        
        # Update balance/margin cache for websocket
        await set_user_balance_margin_cache(redis_client, order_user_id, db_user_locked.wallet_balance, db_user_locked.margin)
        logger.info(f"Balance/margin cache updated for user {order_user_id}: balance={db_user_locked.wallet_balance}, margin={db_user_locked.margin}")
        
        # Update users with orders cache for this symbol
        await update_users_with_orders_cache_on_order_change(redis_client, order_company_name)
        
        await update_static_orders_cache(order_user_id, db, redis_client, user_type)
        await publish_order_update(redis_client, order_user_id)
        await publish_user_data_update(redis_client, order_user_id)
        await publish_market_data_trigger(redis_client)  # Trigger immediate websocket update
    except Exception as e:
        logger.error(f"[ORDER_CLOSE] Error closing order {get_attr(order, 'order_id')}: {e}", exc_info=True)

async def check_and_trigger_pending_orders_redis(redis: Redis, symbol: str, order_type: str, adjusted_price: float):
    """
    On each price tick, find all matching pending orders for symbol/order_type using ZSET,
    fetch their data from HASH, and queue them for processing.
    """
    # Convert adjusted_price to float with 6 decimal precision
    adjusted_price_rounded = round(float(adjusted_price), 6)
    
    # 1. Find matching order_ids
    order_ids = await get_pending_orders_by_price(redis, symbol, order_type, adjusted_price_rounded)
    if not order_ids:
        return 0
    
    logger.info(f"[PENDING_CHECK] Found {len(order_ids)} matching {order_type} orders for {symbol} at price {adjusted_price_rounded}")
    triggered = 0
    for order_id in order_ids:
        # 2. Fetch order data from HASH
        order = await get_order_hash(redis, order_id)
        if not order:
            logger.warning(f"[PENDING_CHECK] Order {order_id} not found in hash, skipping")
            continue
        
        # 3. Queue for processing with the trigger price
        await queue_triggered_order(redis, order_id, adjusted_price_rounded)
        triggered += 1
    
    return triggered

async def pending_order_trigger_worker(redis: Redis, db_factory, concurrency: int = 10):
    """
    Background worker to process triggered pending orders from the Redis stream.
    db_factory: a callable that returns a new AsyncSession (e.g. AsyncSessionLocal)
    concurrency: max number of concurrent triggers
    """
    import asyncio
    from app.services.pending_orders import trigger_pending_order
    last_id = "0"
    sem = asyncio.Semaphore(concurrency)
    error_count = 0
    max_errors = 50  # Reset stream if too many errors
    
    logger.info("[PENDING_TRIGGER_WORKER] Starting pending order trigger worker")
    
    while True:
        try:
            # Use fetch_triggered_orders to get new entries from the stream
            entries = await fetch_triggered_orders(redis, last_id)
            
            if not entries:
                await asyncio.sleep(0.1)
                continue
            
            # Process each entry
            for entry_id, order_id, trigger_price in entries:
                async with sem:
                    try:
                        # Get order data from hash
                        order_data = await get_order_hash(redis, order_id)
                        if not order_data:
                            logger.warning(f"Order {order_id} not found in hash")
                            error_count += 1
                            continue
                        
                        # Create new DB session
                        async with db_factory() as db:
                            try:
                                # Get current price for the order
                                symbol = order_data.get('order_company_name')
                                placed_price = decimal_to_float_6dp(order_data.get('order_price', 0))
                                
                                # FIXED: Use the stored trigger price for execution
                                # This ensures the execution price matches the trigger price exactly
                                execution_price = None
                                if trigger_price is not None:
                                    try:
                                        execution_price = float(trigger_price)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Invalid trigger price {trigger_price}, falling back to placed price")
                                        execution_price = placed_price
                                else:
                                    # Fallback to placed price if no trigger price stored
                                    execution_price = placed_price
                                    logger.warning(f"No trigger price stored for order {order_id}, using placed price {placed_price}")
                                
                                # FIXED: Validate that the trigger price is correct for the order type
                                order_type = order_data.get('order_type', '')
                                price_validation_passed = True
                                
                                if order_type in ['BUY_LIMIT', 'SELL_STOP']:
                                    # These should trigger when market price <= pending price
                                    if execution_price > placed_price:
                                        logger.warning(f"Invalid trigger for {order_type}: execution_price ({execution_price}) > placed_price ({placed_price})")
                                        price_validation_passed = False
                                elif order_type in ['BUY_STOP', 'SELL_LIMIT']:
                                    # These should trigger when market price >= pending price
                                    if execution_price < placed_price:
                                        logger.warning(f"Invalid trigger for {order_type}: execution_price ({execution_price}) < placed_price ({placed_price})")
                                        price_validation_passed = False
                                
                                if not price_validation_passed:
                                    logger.error(f"Price validation failed for order {order_id}, skipping execution")
                                    error_count += 1
                                    continue
                                
                                if symbol and placed_price > 0:
                                    # Use the stored trigger price for execution to ensure consistency
                                    await trigger_pending_order(
                                        db=db,
                                        redis_client=redis,
                                        order=order_data,
                                        current_price=Decimal(str(execution_price))
                                    )
                                    error_count = 0  # Reset error count on success
                                else:
                                    logger.warning(f"Invalid order data for {order_id}: symbol={symbol}, price={placed_price}")
                                    error_count += 1
                            except Exception as trigger_error:
                                logger.error(f"Error triggering order {order_id}: {trigger_error}", exc_info=True)
                                error_count += 1
                            finally:
                                await db.close()
                        
                        # Update last_id to mark this entry as processed
                        last_id = entry_id
                        
                    except Exception as e:
                        logger.error(f"Error processing stream entry {entry_id}: {e}", exc_info=True)
                        error_count += 1
                        # Still update last_id to avoid infinite loop
                        last_id = entry_id
            
            # If too many errors, reset the stream
            if error_count >= max_errors:
                logger.warning(f"Too many errors ({error_count}), resetting stream")
                try:
                    # Clear the stream and start fresh
                    await redis.xtrim(PENDING_TRIGGER_QUEUE, maxlen=0)
                    last_id = "0"
                    error_count = 0
                    logger.info("Stream reset successfully")
                except Exception as reset_error:
                    logger.error(f"Error resetting stream: {reset_error}")
                    # If reset fails, just continue with current last_id
                    error_count = 0
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            await asyncio.sleep(1)  # Wait before retrying
        
async def cleanup_pending_order_stream(redis: Redis):
    """
    Clean up the pending order stream and reset the system.
    This can be called to fix issues with old stream entries.
    """
    try:
        logger.info("[CLEANUP] Starting pending order stream cleanup")
        
        # Clear the trigger queue stream
        await redis.xtrim(PENDING_TRIGGER_QUEUE, maxlen=0)
        logger.info("[CLEANUP] Cleared pending_trigger_queue stream")
        
        # Optionally, you can also clear all pending order hashes and zsets
        # This is more aggressive and will remove all pending orders from Redis
        # Uncomment the following lines if you want to clear everything:
        
        # # Clear all pending order hashes
        # pattern = f"{PENDING_HASH_PREFIX}:*"
        # keys = await redis.keys(pattern)
        # if keys:
        #     await redis.delete(*keys)
        #     logger.info(f"[CLEANUP] Cleared {len(keys)} pending order hashes")
        
        # # Clear all pending order zsets
        # pattern = f"{PENDING_ZSET_PREFIX}:*"
        # keys = await redis.keys(pattern)
        # if keys:
        #     await redis.delete(*keys)
        #     logger.info(f"[CLEANUP] Cleared {len(keys)} pending order zsets")
        
        logger.info("[CLEANUP] Pending order stream cleanup completed")
        return True
    except Exception as e:
        logger.error(f"[CLEANUP] Error during cleanup: {e}", exc_info=True)
        return False

async def cleanup_pending_order_stream_with_trigger_prices(redis: Redis):
    """
    Clean up the pending order stream and ensure all entries have trigger prices.
    This is needed after updating to the new trigger price format.
    """
    try:
        logger.info("[CLEANUP] Starting pending order stream cleanup with trigger price validation")
        
        # Clear the trigger queue stream to remove old format entries
        await redis.xtrim(PENDING_TRIGGER_QUEUE, maxlen=0)
        logger.info("[CLEANUP] Cleared pending_trigger_queue stream to remove old format entries")
        
        logger.info("[CLEANUP] Pending order stream cleanup with trigger price validation completed")
        return True
    except Exception as e:
        logger.error(f"[CLEANUP] Error during cleanup with trigger prices: {e}", exc_info=True)
        return False

# --- New Optimized Pending Order Processing System ---

async def process_pending_orders_for_symbol(symbol: str, redis_client: Redis):
    """
    Main orchestrator function for processing pending orders for a specific symbol.
    Similar to process_sltp_for_symbol but specifically for pending orders.
    """
    try:
        # Get users with pending orders for this symbol
        users_data = await get_users_with_pending_orders_for_symbol(redis_client, symbol)
        
        if not users_data:
            return
            
        # Process each user's pending orders
        for user_id, user_type in users_data:
            try:
                await process_user_pending_orders_for_symbol(
                    user_id=user_id,
                    user_type=user_type,
                    symbol=symbol,
                    redis_client=redis_client
                )
            except Exception as e:
                orders_logger.error(
                    f"Error processing pending orders for user {user_id} "
                    f"symbol {symbol}: {str(e)}", 
                    exc_info=True
                )
                continue
                
    except Exception as e:
        orders_logger.error(
            f"Error in process_pending_orders_for_symbol for {symbol}: {str(e)}", 
            exc_info=True
        )

async def process_user_pending_orders_for_symbol(
    user_id: int,
    user_type: str,
    symbol: str,
    redis_client: Redis
):
    """
    Process all pending orders for a specific user and symbol.
    Uses the new trigger processing system with proper margin validation.
    """
    try:
        # Get user's group name from cache
        user_data = await get_user_data_cache(redis_client, user_id, None, user_type)
        if not user_data or not user_data.get('group_name'):
            return
            
        group_name = user_data['group_name']
        
        # Get adjusted prices for the symbol
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        if not adjusted_prices:
            return
            
        # We only use adjusted ask price (buy price) for pending order triggers
        adjusted_ask_price = adjusted_prices.get('buy')
        if not adjusted_ask_price:
            return
            
        # Get all pending orders for this user and symbol
        pending_orders = await get_user_pending_orders_for_symbol(
            redis_client, 
            str(user_id), 
            symbol
        )
        
        if not pending_orders:
            return

        async with AsyncSessionLocal() as db:
            for order in pending_orders:
                try:
                    await process_pending_order_trigger(redis_client, order, adjusted_ask_price, db)
                except Exception as order_error:
                    orders_logger.error(
                        f"Error processing order {order.get('order_id', 'unknown')}: {str(order_error)}",
                        exc_info=True
                    )
                    continue
                
    except Exception as e:
        orders_logger.error(
            f"Error in process_user_pending_orders_for_symbol: "
            f"user={user_id}, symbol={symbol}: {str(e)}",
            exc_info=True
        )

async def get_users_with_pending_orders_for_symbol(
    redis_client: Redis,
    symbol: str
) -> List[Tuple[int, str]]:
    """
    Get list of user IDs and types who have pending orders for a given symbol.
    Uses the actual Redis ZSET structure that's being used to store pending orders.
    """
    try:
        users_data = []
        
        # Check for each order type that can be pending
        order_types = ['BUY_LIMIT', 'SELL_LIMIT', 'BUY_STOP', 'SELL_STOP']
        
        for order_type in order_types:
            zset_key = f"{PENDING_ZSET_PREFIX}:{symbol}:{order_type}"
            
            # Get all order IDs from this ZSET
            order_ids = await redis_client.zrange(zset_key, 0, -1)
            
            for order_id in order_ids:
                try:
                    order_id_str = order_id.decode() if isinstance(order_id, bytes) else order_id
                    
                    # Get order details from hash
                    order_data = await get_order_hash(redis_client, order_id_str)
                    if order_data:
                        user_id = int(order_data.get('order_user_id', 0))
                        user_type = order_data.get('user_type', 'live')
                        
                        if user_id > 0:
                            users_data.append((user_id, user_type))
                            
                except Exception as e:
                    orders_logger.error(f"Error processing order {order_id}: {str(e)}")
                    continue
        
        # Remove duplicates while preserving order
        unique_users = []
        seen = set()
        for user_id, user_type in users_data:
            if (user_id, user_type) not in seen:
                unique_users.append((user_id, user_type))
                seen.add((user_id, user_type))
        
        return unique_users
        
    except Exception as e:
        orders_logger.error(f"Error getting users with pending orders: {str(e)}", exc_info=True)
        return []

async def get_user_pending_orders_for_symbol(
    redis_client: Redis,
    user_id: str,
    symbol: str
) -> List[Dict[str, Any]]:
    """
    Get all pending orders for a specific user and symbol.
    Uses the actual Redis ZSET structure that's being used to store pending orders.
    """
    try:
        orders = []
        
        # Check for each order type that can be pending
        order_types = ['BUY_LIMIT', 'SELL_LIMIT', 'BUY_STOP', 'SELL_STOP']
        
        for order_type in order_types:
            zset_key = f"{PENDING_ZSET_PREFIX}:{symbol}:{order_type}"
            
            # Get all order IDs from this ZSET
            order_ids = await redis_client.zrange(zset_key, 0, -1)
            
            for order_id in order_ids:
                try:
                    order_id_str = order_id.decode() if isinstance(order_id, bytes) else order_id
                    
                    # Get order details from hash
                    order_data = await get_order_hash(redis_client, order_id_str)
                    if order_data and str(order_data.get('order_user_id', '')) == str(user_id):
                        orders.append(order_data)
                        
                except Exception as e:
                    orders_logger.error(f"Error processing order {order_id}: {str(e)}")
                    continue
        
        return orders
        
    except Exception as e:
        orders_logger.error(
            f"Error getting pending orders for user {user_id}, symbol {symbol}: {str(e)}", 
            exc_info=True
        )
        return []

# --- End New Optimized Pending Order Processing System ---

async def recalculate_margin_for_pending_order(
    redis_client: Redis,
    order_data: Dict[str, Any],
    db: AsyncSession
) -> Optional[Decimal]:
    """
    Recalculates margin for a pending order after price modification.
    Returns the new margin value or None if calculation fails.
    """
    try:
        user_id = int(order_data.get('order_user_id'))
        user_type = order_data.get('user_type', 'live')
        symbol = order_data.get('order_company_name')
        order_type = order_data.get('order_type')
        quantity = Decimal(str(order_data.get('order_quantity', '0')))
        price = Decimal(str(order_data.get('order_price', '0')))

        # Get user data and group settings
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data or not user_data.get('group_name'):
            orders_logger.error(f"No user data found for user {user_id}")
            return None

        group_name = user_data['group_name']
        leverage = Decimal(str(user_data.get('leverage', '1')))
        
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            orders_logger.error(f"No group settings found for {group_name}")
            return None

        # Get external symbol info
        from app.services.margin_calculator import get_external_symbol_info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            orders_logger.error(f"No symbol info found for {symbol}")
            return None

        # Get raw market data for margin calculation
        from app.core.firebase import get_latest_market_data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            orders_logger.error(f"No market data available for margin calculation")
            return None

        # Calculate margin using the correct function
        from app.services.margin_calculator import calculate_single_order_margin
        margin_result = await calculate_single_order_margin(
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
            order_price=price
        )

        if margin_result and len(margin_result) >= 1:
            margin = margin_result[0]  # First element is the margin
            orders_logger.info(f"Recalculated margin for order: {margin}")
            return margin
        else:
            orders_logger.error(f"Margin calculation failed for order")
            return None

    except Exception as e:
        orders_logger.error(f"Error recalculating margin: {str(e)}", exc_info=True)
        return None

async def update_pending_order_in_redis(
    redis_client: Redis,
    order_data: Dict[str, Any],
    new_margin: Optional[Decimal] = None
) -> bool:
    """
    Updates a pending order in Redis with new price and margin values.
    Returns True if successful, False otherwise.
    """
    try:
        order_id = order_data.get('order_id')
        symbol = order_data.get('order_company_name')
        order_type = order_data.get('order_type')
        user_id = str(order_data.get('order_user_id'))
        price = Decimal(str(order_data.get('order_price', '0')))

        # Remove old order from Redis
        await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)

        # Update margin if provided
        if new_margin is not None:
            order_data['margin'] = str(new_margin)

        # Add updated order back to Redis
        await add_pending_order(redis_client, order_data)

        orders_logger.info(
            f"Updated pending order in Redis - Order: {order_id}, "
            f"Symbol: {symbol}, Price: {price}, Margin: {new_margin}"
        )
        return True

    except Exception as e:
        orders_logger.error(f"Error updating pending order in Redis: {str(e)}", exc_info=True)
        return False

async def process_pending_order_trigger(
    redis_client: Redis,
    order: Dict[str, Any],
    adjusted_ask_price: Decimal,
    db: AsyncSession
) -> bool:
    """
    Processes a pending order trigger with proper margin validation.
    Returns True if order was triggered successfully.
    """
    try:
        order_id = order.get('order_id')
        user_id = int(order.get('order_user_id'))
        user_type = order.get('user_type', 'live')
        symbol = order.get('order_company_name')
        order_type = order.get('order_type', '').upper()
        pending_price = Decimal(str(order.get('order_price', '0')))

        # Check trigger conditions
        triggered = False
        if order_type == 'BUY_LIMIT':
            triggered = adjusted_ask_price <= pending_price
        elif order_type == 'SELL_LIMIT':
            triggered = adjusted_ask_price >= pending_price

        if triggered:

            # Validate margin before triggering
            new_margin = await recalculate_margin_for_pending_order(redis_client, order, db)
            if new_margin is None:
                orders_logger.error(f"Failed to recalculate margin for order {order_id}")
                return False

            # Update order with new margin before queuing
            order['margin'] = str(new_margin)

            # Atomically remove from pending and queue for processing
            async with redis_client.pipeline(transaction=True) as pipe:
                await remove_pending_order(
                    redis_client,
                    order_id,
                    symbol,
                    order_type,
                    str(user_id)
                )
                await queue_triggered_order(redis_client, order_id)
                await pipe.execute()

            return True

        return False

    except Exception as e:
        orders_logger.error(f"Error processing pending order trigger: {str(e)}", exc_info=True)
        return False

# Update the process_user_pending_orders_for_symbol function to use the new trigger processing
async def process_user_pending_orders_for_symbol(
    user_id: int,
    user_type: str,
    symbol: str,
    redis_client: Redis
):
    """
    Process all pending orders for a specific user and symbol.
    Uses the new trigger processing system with proper margin validation.
    """
    try:
        # Get user's group name from cache
        user_data = await get_user_data_cache(redis_client, user_id, None, user_type)
        if not user_data or not user_data.get('group_name'):
            return
            
        group_name = user_data['group_name']
        
        # Get adjusted prices for the symbol
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        if not adjusted_prices:
            return
            
        # We only use adjusted ask price (buy price) for pending order triggers
        adjusted_ask_price = adjusted_prices.get('buy')
        if not adjusted_ask_price:
            return
            
        # Get all pending orders for this user and symbol
        pending_orders = await get_user_pending_orders_for_symbol(
            redis_client, 
            str(user_id), 
            symbol
        )
        
        if not pending_orders:
            return

        async with AsyncSessionLocal() as db:
            for order in pending_orders:
                try:
                    await process_pending_order_trigger(redis_client, order, adjusted_ask_price, db)
                except Exception as order_error:
                    orders_logger.error(
                        f"Error processing order {order.get('order_id', 'unknown')}: {str(order_error)}",
                        exc_info=True
                    )
                    continue
                
    except Exception as e:
        orders_logger.error(
            f"Error in process_user_pending_orders_for_symbol: "
            f"user={user_id}, symbol={symbol}: {str(e)}",
            exc_info=True
        )


        
        