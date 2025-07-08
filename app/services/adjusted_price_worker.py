import asyncio
import logging
from decimal import Decimal
from typing import Dict, Any
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.cache import set_adjusted_market_price_cache, get_adjusted_market_price_cache, get_group_symbol_settings_cache, REDIS_MARKET_DATA_CHANNEL
from app.crud import group as crud_group
from app.database.session import AsyncSessionLocal
import json
import time
import hashlib

logger = logging.getLogger("adjusted_price_worker")

# In-memory group settings cache
GROUP_SETTINGS_REFRESH_INTERVAL = 300  # 5 minutes

group_settings_cache = {}  # {group_name: {symbol: settings}}
group_settings_last_refresh = 0

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

async def calculate_adjusted_prices_for_group(raw_market_data: Dict[str, Any], group_settings: Dict[str, Any], group_name: str) -> Dict[str, Dict[str, float]]:
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
                ask_decimal = Decimal(str(raw_ask_price))
                bid_decimal = Decimal(str(raw_bid_price))
                spread_setting = Decimal(str(settings.get('spread', 0)))
                spread_pip_setting = Decimal(str(settings.get('spread_pip', 0)))
                configured_spread_amount = spread_setting * spread_pip_setting
                half_spread = configured_spread_amount / Decimal(2)
                adjusted_buy_price = ask_decimal + half_spread
                adjusted_sell_price = bid_decimal - half_spread
                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                effective_spread_in_pips = Decimal("0.0")
                if spread_pip_setting > Decimal("0.0"):
                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting
                adjusted_prices[symbol_upper] = {
                    'buy': adjusted_buy_price,
                    'sell': adjusted_sell_price,
                    'spread': effective_spread_in_pips,
                    'spread_value': configured_spread_amount
                }
            except Exception as e:
                logger.error(f"Error adjusting price for {symbol_upper}: {e}", exc_info=True)
    return adjusted_prices

async def adjusted_price_worker(redis_client: Redis):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    logger.info("Adjusted price worker started. Listening for market data updates.")
    latest_market_data = None
    last_market_data_hash = None
    debounce_delay = 0.05  # 50ms
    last_update_time = 0
    update_event = asyncio.Event()
    debounce_task = None
    refresh_task = None

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
        # Fast-path skip: hash and compare
        new_hash = hash_market_data(raw_market_data)
        if new_hash == last_market_data_hash:
            logger.debug("[adjusted_price_worker] Market data unchanged, skipping recalculation.")
            return
        last_market_data_hash = new_hash
        # Parallelize group calculations
        group_names = list(group_settings_cache.keys())
        group_settings_list = [group_settings_cache[g] for g in group_names]
        calc_start = time.perf_counter()
        group_results = await asyncio.gather(*[
            calculate_adjusted_prices_for_group(raw_market_data, group_settings, group_name)
            for group_settings, group_name in zip(group_settings_list, group_names)
        ])
        calc_time = time.perf_counter() - calc_start
        # Batch Redis writes in a single pipeline
        pipe = redis_client.pipeline()
        write_count = 0
        redis_write_start = time.perf_counter()
        for group_name, adjusted_prices in zip(group_names, group_results):
            for symbol, prices in adjusted_prices.items():
                # Check cache before writing
                cached = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                should_write = False
                if not cached:
                    should_write = True
                else:
                    if (
                        Decimal(str(prices['buy'])) != cached['buy'] or
                        Decimal(str(prices['sell'])) != cached['sell'] or
                        Decimal(str(prices['spread_value'])) != cached['spread_value']
                    ):
                        should_write = True
                if should_write:
                    await set_adjusted_market_price_cache(pipe, group_name, symbol, prices['buy'], prices['sell'], prices['spread_value'])
                    write_count += 1
        await pipe.execute()
        redis_write_time = time.perf_counter() - redis_write_start
        total_time = time.perf_counter() - tick_start
        logger.info(f"[adjusted_price_worker] Tick: {write_count} writes | calc: {calc_time*1000:.2f}ms | redis: {redis_write_time*1000:.2f}ms | total: {total_time*1000:.2f}ms")

    async def debounce_loop():
        nonlocal last_update_time
        while True:
            await update_event.wait()
            now = time.time()
            # Rolling debounce: if another update comes within 50ms, keep waiting
            while True:
                await asyncio.sleep(0.005)
                if update_event.is_set():
                    if (time.time() - now) < debounce_delay:
                        now = time.time()
                        update_event.clear()
                        continue
                break
            await process_latest()
            update_event.clear()

    debounce_task = asyncio.create_task(debounce_loop())
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
                update_event.set()
            except Exception as e:
                logger.error(f"Error in adjusted_price_worker main loop: {e}", exc_info=True)
            await asyncio.sleep(0.01)
    finally:
        debounce_task.cancel()
        refresh_task.cancel()
        try:
            await debounce_task
        except Exception:
            pass
        try:
            await refresh_task
        except Exception:
            pass 