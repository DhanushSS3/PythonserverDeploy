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

logger = logging.getLogger("adjusted_price_worker")

async def calculate_adjusted_prices_for_group(raw_market_data: Dict[str, Any], group_settings: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
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
    debounce_delay = 0.05  # 50ms debounce window
    last_update_time = None
    debounce_task = None
    update_event = asyncio.Event()

    async def process_latest():
        nonlocal latest_market_data
        if not latest_market_data:
            return
        try:
            raw_market_data = {k: v for k, v in latest_market_data.items() if k not in ["type", "_timestamp"]}
            async with AsyncSessionLocal() as db:
                groups = await crud_group.get_groups(db, skip=0, limit=1000)
                group_names = set(g.name for g in groups if g.name)
                for group_name in group_names:
                    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                    if not group_settings:
                        continue
                    adjusted_prices = await calculate_adjusted_prices_for_group(raw_market_data, group_settings)
                    # Pipeline writes, but only if changed
                    async with redis_client.pipeline() as pipe:
                        for symbol, prices in adjusted_prices.items():
                            # Check cache before writing
                            cached = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                            should_write = False
                            if not cached:
                                should_write = True
                            else:
                                # Compare buy, sell, spread_value
                                if (
                                    Decimal(str(prices['buy'])) != cached['buy'] or
                                    Decimal(str(prices['sell'])) != cached['sell'] or
                                    Decimal(str(prices['spread_value'])) != cached['spread_value']
                                ):
                                    should_write = True
                            if should_write:
                                await set_adjusted_market_price_cache(pipe, group_name, symbol, prices['buy'], prices['sell'], prices['spread_value'])
                        await pipe.execute()
                    logger.debug(f"Adjusted prices updated for group {group_name} ({len(adjusted_prices)} symbols)")
        except Exception as e:
            logger.error(f"Error in process_latest: {e}", exc_info=True)

    async def debounce_loop():
        while True:
            await update_event.wait()
            await asyncio.sleep(debounce_delay)
            await process_latest()
            update_event.clear()

    debounce_task = asyncio.create_task(debounce_loop())

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
        try:
            await debounce_task
        except Exception:
            pass 