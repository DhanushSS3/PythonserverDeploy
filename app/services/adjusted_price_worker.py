import asyncio
import logging
from typing import Dict, Any
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.cache import set_adjusted_market_price_cache, get_adjusted_market_price_cache, get_group_symbol_settings_cache, REDIS_MARKET_DATA_CHANNEL, get_last_known_price, set_last_known_price
from app.crud import group as crud_group
from app.database.session import AsyncSessionLocal
import json
import time
import hashlib
from concurrent.futures import ProcessPoolExecutor
from app.shared_state import adjusted_prices_in_memory

# Remove the circular import - we'll call the function directly when needed
# from app.services.pending_orders import check_and_trigger_pending_orders_redis

logger = logging.getLogger("adjusted_price_worker")

# In-memory group settings cache
GROUP_SETTINGS_REFRESH_INTERVAL = 300  # 5 minutes

group_settings_cache = {}  # {group_name: {symbol: settings}}
group_settings_last_refresh = 0

# --- Float-based adjusted price calculation for process pool ---
def calc_adjusted_prices_for_group_float(raw_market_data, group_settings, group_name):
    adjusted_prices = {}
    for symbol, settings in group_settings.items():
        symbol_upper = symbol.upper()
        prices = raw_market_data.get(symbol_upper)
        if not prices or not isinstance(prices, dict):
            continue
        raw_ask_price = prices.get('b')
        raw_bid_price = prices.get('o')
        if raw_ask_price is not None and raw_bid_price is not None:
            try:
                ask = float(raw_ask_price)
                bid = float(raw_bid_price)
                spread_setting = float(settings.get('spread', 0))
                spread_pip_setting = float(settings.get('spread_pip', 0))
                configured_spread_amount = spread_setting * spread_pip_setting
                half_spread = configured_spread_amount / 2.0
                adjusted_buy_price = ask + half_spread
                adjusted_sell_price = bid - half_spread
                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                effective_spread_in_pips = 0.0
                if spread_pip_setting > 0.0:
                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting
                adjusted_prices[symbol_upper] = {
                    'buy': adjusted_buy_price,
                    'sell': adjusted_sell_price,
                    'spread': effective_spread_in_pips,
                    'spread_value': configured_spread_amount
                }
            except Exception as e:
                # Can't log from process pool, return error info if needed
                continue
    return group_name, adjusted_prices

def hash_market_data(raw_market_data: Dict[str, Any]) -> str:
    # Hash only the symbol->price part, ignore meta keys
    relevant = {k: v for k, v in raw_market_data.items() if k not in ["type", "_timestamp"]}
    return hashlib.sha256(json.dumps(relevant, sort_keys=True, default=str).encode()).hexdigest()

async def refresh_group_settings(redis_client: Redis):
    global group_settings_cache, group_settings_last_refresh
    start = time.perf_counter()
    async with AsyncSessionLocal() as db:
        groups = await crud_group.get_groups(db, skip=0, limit=1000)
        group_names = set(g.name for g in groups if g.name)
        new_cache = {}
        for group_name in group_names:
            settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
            if settings:
                new_cache[group_name] = settings
    group_settings_cache = new_cache
    group_settings_last_refresh = time.time()
    logger.info(f"[adjusted_price_worker] Refreshed group settings for {len(group_settings_cache)} groups in {time.perf_counter()-start:.4f}s")

async def adjusted_price_worker(redis_client: Redis):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    logger.info("Adjusted price worker started. Listening for market data updates.")
    latest_market_data = None
    last_market_data_hash = None
    refresh_task = None
    process_pool = ProcessPoolExecutor()

    async def refresh_settings_periodically():
        while True:
            await refresh_group_settings(redis_client)
            await asyncio.sleep(GROUP_SETTINGS_REFRESH_INTERVAL)

    async def process_latest():
        nonlocal latest_market_data, last_market_data_hash
        if not latest_market_data:
            return
        tick_start = time.perf_counter()
        raw_market_data = {k: v for k, v in latest_market_data.items() if k not in ["type", "_timestamp"]}
        new_hash = hash_market_data(raw_market_data)
        if new_hash == last_market_data_hash:
            logger.debug("[adjusted_price_worker] Market data unchanged, skipping recalculation.")
            return
        last_market_data_hash = new_hash
        group_names = list(group_settings_cache.keys())
        group_settings_list = [group_settings_cache[g] for g in group_names]
        calc_start = time.perf_counter()
        # Offload to process pool
        loop = asyncio.get_running_loop()
        group_results = await asyncio.gather(*[
            loop.run_in_executor(process_pool, calc_adjusted_prices_for_group_float, raw_market_data, group_settings, group_name)
            for group_settings, group_name in zip(group_settings_list, group_names)
        ])
        calc_time = time.perf_counter() - calc_start
        # Update in-memory dict and batch Redis writes in background
        write_count = 0
        redis_write_start = time.perf_counter()
        pipe = redis_client.pipeline()
        
        # Track symbols that were updated for pending order triggers
        updated_symbols = set()
        
        for group_name, adjusted_prices in group_results:
            # Update in-memory dict
            if group_name not in adjusted_prices_in_memory:
                adjusted_prices_in_memory[group_name] = {}
            for symbol, prices in adjusted_prices.items():
                # Only update if changed
                prev = adjusted_prices_in_memory[group_name].get(symbol)
                if not prev or (
                    prev['buy'] != prices['buy'] or
                    prev['sell'] != prices['sell'] or
                    prev['spread'] != prices['spread']
                ):
                    adjusted_prices_in_memory[group_name][symbol] = {
                        'buy': prices['buy'],
                        'sell': prices['sell'],
                        'spread': prices['spread']
                    }
                    # Redis persistence (not hot path)
                    await set_adjusted_market_price_cache(pipe, group_name, symbol, prices['buy'], prices['sell'], prices['spread_value'])
                    write_count += 1
                    
                    # Track this symbol for pending order triggers
                    updated_symbols.add(symbol)
                    
                    # --- NEW: Set last known price for this symbol (raw bid/offer from latest_market_data) ---
                    raw_prices = raw_market_data.get(symbol)
                    if raw_prices and isinstance(raw_prices, dict):
                        price_data = {}
                        if 'b' in raw_prices:
                            price_data['b'] = raw_prices['b']
                        if 'o' in raw_prices:
                            price_data['o'] = raw_prices['o']
                        if price_data:
                            await set_last_known_price(redis_client, symbol, price_data)
        
        await pipe.execute()
        redis_write_time = time.perf_counter() - redis_write_start
        
        # FIXED: Trigger pending orders with group-specific prices
        if updated_symbols:
            logger.debug(f"[PENDING_TRIGGER] Processing {len(updated_symbols)} symbols with updated prices")
            trigger_start_time = time.time()
            
            # Import the function here to avoid circular imports
            from app.services.pending_orders import check_and_trigger_pending_orders_redis, get_users_with_pending_orders_for_symbol
            
            
            # FIXED: Get users with pending orders for each symbol and use their group-specific prices
            for symbol in updated_symbols:
                try:
                    # Get all users who have pending orders for this symbol
                    users_with_pending_orders = await get_users_with_pending_orders_for_symbol(redis_client, symbol)
                    
                    if not users_with_pending_orders:
                        logger.debug(f"[PENDING_TRIGGER] No users with pending orders for {symbol}")
                        continue
                    
                    logger.debug(f"[PENDING_TRIGGER] Found {len(users_with_pending_orders)} users with pending orders for {symbol}")
                    
                    # Group users by their group name to use correct group-specific prices
                    users_by_group = {}
                    for user_id, user_type in users_with_pending_orders:
                        try:
                            # Get user's group name from cache
                            from app.core.cache import get_user_data_cache
                            user_data = await get_user_data_cache(redis_client, user_id, None, user_type)
                            if user_data and user_data.get('group_name'):
                                group_name = user_data['group_name']
                                if group_name not in users_by_group:
                                    users_by_group[group_name] = []
                                users_by_group[group_name].append((user_id, user_type))
                            else:
                                logger.warning(f"[PENDING_TRIGGER] Could not get group name for user {user_id} ({user_type})")
                        except Exception as e:
                            logger.error(f"[PENDING_TRIGGER] Error getting group for user {user_id}: {e}")
                            continue
                    
                    # Process each group separately with their specific adjusted prices
                    for group_name, users in users_by_group.items():
                        # Get this group's adjusted prices for this symbol
                        group_adjusted_prices = adjusted_prices_in_memory.get(group_name, {})
                        symbol_prices = group_adjusted_prices.get(symbol)
                        
                        if not symbol_prices:
                            logger.warning(f"[PENDING_TRIGGER] No adjusted prices for {symbol} in group {group_name}")
                            continue
                        
                        adjusted_ask_price = symbol_prices.get('buy', 0.0)
                        adjusted_bid_price = symbol_prices.get('sell', 0.0)
                        
                        if adjusted_ask_price > 0 and adjusted_bid_price > 0:
                            logger.debug(f"[PENDING_TRIGGER] Checking {symbol} for group {group_name}: ask={adjusted_ask_price}, bid={adjusted_bid_price}")
                            
                            # FIXED: Use adjusted_ask_price for ALL order types as requested
                            # This ensures consistent price comparison across all pending order types
                            trigger_price = adjusted_ask_price
                            logger.debug(f"[PENDING_TRIGGER] Using adjusted_ask_price={trigger_price} for all order types on {symbol} (group: {group_name})")
                            
                            # Trigger all pending order types with the same adjusted_ask_price
                            await check_and_trigger_pending_orders_redis(redis_client, symbol, 'BUY_LIMIT', trigger_price)
                            await check_and_trigger_pending_orders_redis(redis_client, symbol, 'SELL_STOP', trigger_price)
                            await check_and_trigger_pending_orders_redis(redis_client, symbol, 'BUY_STOP', trigger_price)
                            await check_and_trigger_pending_orders_redis(redis_client, symbol, 'SELL_LIMIT', trigger_price)
                            
                            logger.debug(f"[PENDING_TRIGGER] Completed triggering all order types for {symbol} at price {trigger_price} (group: {group_name})")
                        else:
                            logger.warning(f"[PENDING_TRIGGER] Invalid prices for {symbol} in group {group_name}: ask={adjusted_ask_price}, bid={adjusted_bid_price}")
                            
                except Exception as e:
                    logger.error(f"[PENDING_TRIGGER] Error processing pending orders for {symbol}: {e}", exc_info=True)
                    continue
            
            trigger_time = time.time() - trigger_start_time
            logger.debug(f"[PENDING_TRIGGER] Completed in {trigger_time:.3f}s for {len(updated_symbols)} symbols")

        total_time = time.perf_counter() - tick_start
        logger.info(f"[adjusted_price_worker] Tick: {write_count} writes | calc: {calc_time*1000:.2f}ms | redis: {redis_write_time*1000:.2f}ms | total: {total_time*1000:.2f}ms")

    refresh_task = asyncio.create_task(refresh_settings_periodically())
    await refresh_group_settings(redis_client)  # Initial load

    try:
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not message:
                    await asyncio.sleep(0.01)
                    continue
                try:
                    message_data = json.loads(message['data'])
                except Exception:
                    continue
                latest_market_data = message_data
                await process_latest()
            except Exception as e:
                logger.error(f"Error in adjusted_price_worker main loop: {e}", exc_info=True)
            await asyncio.sleep(0.01)
    finally:
        refresh_task.cancel()
        process_pool.shutdown(wait=True)
        try:
            await refresh_task
        except Exception:
            pass 