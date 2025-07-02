# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends, HTTPException
import asyncio
import logging
from app.core.logging_config import websocket_logger, orders_logger

from app.crud.crud_order import get_order_model
import json
# import threading # No longer needed for active_connections_lock
from typing import Dict, Any, List, Optional, Set
import decimal
from starlette.websockets import WebSocketState
from decimal import Decimal
import datetime
import random


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db, AsyncSessionLocal # Import AsyncSessionLocal for db sessions in tasks
from app.crud.user import get_user_by_account_number, get_demo_user_by_account_number
from app.crud import group as crud_group
from app.crud import crud_order

# Import security functions for token validation
from app.core.security import decode_token

# Import the Redis client type
from redis.asyncio import Redis

# Import the caching helper functions
from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    # get_user_positions_from_cache, # Will be part of get_user_portfolio_cache
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,  # <-- For last known price caching
    # New cache functions
    set_user_static_orders_cache, get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache, get_user_dynamic_portfolio_cache,
    DecimalEncoder, decode_decimal,
    # Redis channels
    REDIS_MARKET_DATA_CHANNEL,
    REDIS_ORDER_UPDATES_CHANNEL,
    REDIS_USER_DATA_UPDATES_CHANNEL
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue (for Firebase data -> Redis)
from app.shared_state import redis_publish_queue

# Import the new portfolio calculation service
from app.services.portfolio_calculator import calculate_user_portfolio

# Import the Symbol and ExternalSymbolInfo models
from app.database.models import Symbol, ExternalSymbolInfo, User, DemoUser # Import User/DemoUser for type hints
from sqlalchemy.future import select
from app.services.pending_orders import process_order_stoploss_takeprofit
from app.crud.crud_order import get_all_open_orders_by_user_id, get_order_model
from app.database.models import UserOrder, DemoUserOrder

# Configure logging for this module
logger = websocket_logger

# REMOVE: active_websocket_connections and active_connections_lock
# active_websocket_connections: Dict[int, Dict[str, Any]] = {}
# active_connections_lock = threading.Lock() 

# Redis channel for RAW market data updates from Firebase via redis_publisher_task
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'


router = APIRouter(
    tags=["market_data"]
)


async def _calculate_and_cache_adjusted_prices(
    raw_market_data: Dict[str, Any],
    group_name: str,
    relevant_symbols: Set[str],
    group_settings: Dict[str, Any],
    redis_client: Redis
) -> Dict[str, Dict[str, float]]:
    """
    Calculates adjusted prices based on group settings and caches them.
    Returns a dictionary of adjusted prices for symbols in raw_market_data.
    """
    adjusted_prices_payload = {}
    
    # Get all unique currencies from the group settings
    all_currencies = set()
    for symbol in group_settings.keys():
        if len(symbol) == 6:  # Only process 6-character currency pairs
            all_currencies.add(symbol[:3])  # First currency
            all_currencies.add(symbol[3:])  # Second currency
    
    # Add USD conversion pairs to relevant symbols
    for currency in all_currencies:
        if currency != 'USD':
            relevant_symbols.add(f"{currency}USD")  # Direct conversion
            relevant_symbols.add(f"USD{currency}")  # Indirect conversion
    
    for symbol, prices in raw_market_data.items():
        symbol_upper = symbol.upper()
        if symbol_upper not in relevant_symbols or not isinstance(prices, dict):
            continue

        # --- Persist last known price for this symbol ---
        await set_last_known_price(redis_client, symbol_upper, prices)

        # Firebase 'b' is Ask, 'o' is Bid
        raw_ask_price = prices.get('b')  # Ask from Firebase
        raw_bid_price = prices.get('o')  # Bid from Firebase
        symbol_group_settings = group_settings.get(symbol_upper)

        if raw_ask_price is not None and raw_bid_price is not None and symbol_group_settings:
            try:
                ask_decimal = Decimal(str(raw_ask_price))
                bid_decimal = Decimal(str(raw_bid_price))
                spread_setting = Decimal(str(symbol_group_settings.get('spread', 0)))
                spread_pip_setting = Decimal(str(symbol_group_settings.get('spread_pip', 0)))
                
                configured_spread_amount = spread_setting * spread_pip_setting
                half_spread = configured_spread_amount / Decimal(2)

                # Adjusted prices for user display and trading
                adjusted_buy_price = ask_decimal + half_spread  # User buys at adjusted ask
                adjusted_sell_price = bid_decimal - half_spread  # User sells at adjusted bid

                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                effective_spread_in_pips = Decimal("0.0")
                if spread_pip_setting > Decimal("0.0"):
                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting

                # Cache the adjusted prices
                await set_adjusted_market_price_cache(
                    redis_client=redis_client,
                    group_name=group_name,
                    symbol=symbol_upper,
                    buy_price=adjusted_buy_price,
                    sell_price=adjusted_sell_price,
                    spread_value=configured_spread_amount
                )

                # Add to payload for immediate response
                adjusted_prices_payload[symbol_upper] = {
                    'buy': float(adjusted_buy_price),
                    'sell': float(adjusted_sell_price),
                    'spread': float(effective_spread_in_pips)
                }

                logger.debug(f"Adjusted prices for {symbol_upper}: Buy={adjusted_buy_price}, Sell={adjusted_sell_price}, Spread={effective_spread_in_pips}")

            except Exception as e:
                logger.error(f"Error adjusting price for {symbol_upper} in group {group_name}: {e}", exc_info=True)
                # Optionally send raw if calculation fails
                if raw_ask_price is not None and raw_bid_price is not None:
                    raw_spread = (Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price)))
                    raw_spread_pips = raw_spread / spread_pip_setting if spread_pip_setting > Decimal("0.0") else raw_spread
                    adjusted_prices_payload[symbol_upper] = {
                        'buy': float(raw_ask_price),
                        'sell': float(raw_bid_price),
                        'spread': float(raw_spread_pips)
                    }

    return adjusted_prices_payload


async def _get_full_portfolio_details(
    user_id: int,
    group_name: str, # User's group name
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
) -> Optional[Dict[str, Any]]:
    """
    Fetches all necessary data from cache and calculates the full user portfolio.
    """
    user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
    user_portfolio_cache = await get_user_portfolio_cache(redis_client, user_id) # Contains positions
    group_symbol_settings_all = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

    if not user_data or not group_symbol_settings_all:
        logger.warning(f"Missing user_data or group_settings for user {user_id}, group {group_name}. Cannot calculate portfolio.")
        return None

    open_positions = user_portfolio_cache.get('positions', []) if user_portfolio_cache else []
    
    # Construct the market_prices dict for calculate_user_portfolio using cached adjusted prices
    market_prices_for_calc = {}
    relevant_symbols_for_group = set(group_symbol_settings_all.keys())
    for sym_upper in relevant_symbols_for_group:
        cached_adj_price = await get_adjusted_market_price_cache(redis_client, group_name, sym_upper)
        if cached_adj_price:
            # calculate_user_portfolio expects 'buy' and 'sell' keys
            market_prices_for_calc[sym_upper] = {
                'buy': Decimal(str(cached_adj_price.get('buy'))),
                'sell': Decimal(str(cached_adj_price.get('sell')))
            }

    portfolio_metrics = await calculate_user_portfolio(
        user_data=user_data, # This is a dict
        open_positions=open_positions, # List of dicts
        adjusted_market_prices=market_prices_for_calc, # Dict of symbol -> {'buy': Decimal, 'sell': Decimal}
        group_symbol_settings=group_symbol_settings_all, # Dict of symbol -> settings dict
        redis_client=redis_client
    )
    
    # Ensure all values in portfolio_metrics are JSON serializable
    account_data_payload = {
        "balance": portfolio_metrics.get("balance", "0.0"),
        "equity": portfolio_metrics.get("equity", "0.0"),
        "margin": user_data.get("margin", "0.0"), # User's OVERALL margin from cached user_data
        "free_margin": portfolio_metrics.get("free_margin", "0.0"),
        "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
        "margin_level": portfolio_metrics.get("margin_level", "0.0"),
        "positions": portfolio_metrics.get("positions", []) # This should be serializable list of dicts
    }
    return account_data_payload


async def check_and_trigger_pending_orders(redis_client, db, symbol, adjusted_prices, group_name):
    """
    Check if any pending orders should be triggered based on current market prices.
    This is called when market data updates are received.
    """
    try:
        # Get all pending orders for this symbol from Redis
        redis_keys = [
            f"pending_orders:{symbol}:BUY_LIMIT",
            f"pending_orders:{symbol}:SELL_LIMIT",
            f"pending_orders:{symbol}:BUY_STOP",
            f"pending_orders:{symbol}:SELL_STOP"
        ]
        
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION] Adjusted buy price missing for symbol {symbol} in check_and_trigger_pending_orders. Skipping all pending orders for this symbol.")
        # Check each order type key
        for redis_key in redis_keys:
            try:
                # Get all user orders for this key
                all_user_orders = await redis_client.hgetall(redis_key)
                if not all_user_orders:
                    continue
                
                order_type = redis_key.split(":")[-1]
                logger.debug(f"Checking {len(all_user_orders)} users with {order_type} orders for symbol {symbol}")
                
                # Process each user's orders
                for user_id_bytes, orders_json in all_user_orders.items():
                    user_id = user_id_bytes
                    orders_list = json.loads(orders_json)
                    for order in orders_list:
                        # Normalize decimal values for comparison - round to 5 decimal places
                        try:
                            order_price = Decimal(str(order.get('order_price', '0')))
                            adjusted_buy_price_str = str(adjusted_buy_price)
                            
                            # Ensure the adjusted_buy_price has at least 5 decimal places
                            if '.' in adjusted_buy_price_str:
                                integer_part, decimal_part = adjusted_buy_price_str.split('.')
                                if len(decimal_part) < 5:
                                    decimal_part = decimal_part.ljust(5, '0')
                                adjusted_buy_price_str = f"{integer_part}.{decimal_part}"
                            
                            # Round to 5 decimal places for consistent comparison
                            order_price_normalized = Decimal(str(round(order_price, 5)))
                            adjusted_buy_price_normalized = Decimal(str(round(Decimal(adjusted_buy_price_str), 5)))
                            
                            # Log the raw values
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Raw values - order_price: {order_price}, adjusted_buy_price: {adjusted_buy_price}")
                        except Exception as e:
                            orders_logger.error(f"[PENDING_ORDER_EXECUTION] Error normalizing decimal values: {str(e)}", exc_info=True)
                            continue
                        
                        should_trigger = False
                        if order_type in ['BUY_LIMIT', 'SELL_STOP']:
                            should_trigger = adjusted_buy_price_normalized <= order_price_normalized
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] BUY_LIMIT/SELL_STOP check: {adjusted_buy_price_normalized} <= {order_price_normalized} = {should_trigger}")
                        elif order_type in ['SELL_LIMIT', 'BUY_STOP']:
                            should_trigger = adjusted_buy_price_normalized >= order_price_normalized
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] SELL_LIMIT/BUY_STOP check: {adjusted_buy_price_normalized} >= {order_price_normalized} = {should_trigger}")
                        else:
                            # orders_logger.error(f"[PENDING_ORDER_EXECUTION] Unknown order type {order_type} for order {order.get('order_id')}. Skipping.")
                            continue
                        
                        # Additional debug logging for price comparison
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Price comparison values - adjusted_buy_price_normalized: {adjusted_buy_price_normalized} ({type(adjusted_buy_price_normalized)}), order_price_normalized: {order_price_normalized} ({type(order_price_normalized)})")
                        
                        # Compare as strings for consistent comparison
                        adjusted_price_str = str(adjusted_buy_price_normalized)
                        order_price_str = str(order_price_normalized)
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] String comparison for prices: '{adjusted_price_str}' vs '{order_price_str}'")
                        
                        # Compare numeric difference
                        price_diff = abs(adjusted_buy_price_normalized - order_price_normalized)
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Absolute price difference: {price_diff}")
                        
                        # Compare with small epsilon tolerance to catch very close values
                        epsilon = Decimal('0.00001')  # Small tolerance
                        is_close = price_diff < epsilon
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Prices within epsilon tolerance: {is_close} (epsilon={epsilon})")
                        
                        # Consider using epsilon for near-exact matches
                        should_trigger_with_epsilon = False
                        if order_type in ['BUY_LIMIT', 'SELL_STOP']:
                            should_trigger_with_epsilon = (adjusted_buy_price_normalized <= order_price_normalized) or is_close
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] BUY_LIMIT/SELL_STOP with epsilon: {should_trigger_with_epsilon}")
                        elif order_type in ['SELL_LIMIT', 'BUY_STOP']:
                            should_trigger_with_epsilon = (adjusted_buy_price_normalized >= order_price_normalized) or is_close
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] SELL_LIMIT/BUY_STOP with epsilon: {should_trigger_with_epsilon}")
                        
                        # Use the epsilon-based trigger when prices are very close
                        if should_trigger_with_epsilon and not should_trigger:
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Using epsilon-based trigger since prices are very close")
                            should_trigger = True
                        
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Checking order {order.get('order_id')}: type={order_type}, adjusted_buy_price={adjusted_buy_price_normalized}, order_price={order_price_normalized}, should_trigger={should_trigger}")
                        if should_trigger:
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Trigger condition met for order {order.get('order_id')}. Executing trigger_pending_order.")
                            from app.services.pending_orders import trigger_pending_order
                            # Use a new database session for trigger_pending_order to ensure fresh data
                            from app.database.session import AsyncSessionLocal
                            async with AsyncSessionLocal() as trigger_db:
                                from app.services.pending_orders import trigger_pending_order
                                await trigger_pending_order(
                                    db=trigger_db,
                                    redis_client=redis_client,
                                    order=order,
                                    current_price=adjusted_buy_price_normalized
                                )
                        # else:
                        #     orders_logger.info(f"[PENDING_ORDER_EXECUTION] Order {order.get('order_id')} conditions not met for execution. Skipping.")
            
            except Exception as e:
                logger.error(f"Error processing pending orders for key {redis_key}: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Error in check_and_trigger_pending_orders for symbol {symbol}: {e}", exc_info=True)


async def check_and_trigger_sl_tp_orders(redis_client, db, symbol, adjusted_prices, group_name):
    """
    For each open order (live and demo) for the symbol, check if SL/TP should trigger using adjusted prices.
    Always fetches latest open orders from DB and refreshes each order before checking.
    """
    from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
    from app.services.pending_orders import process_order_stoploss_takeprofit
    from sqlalchemy.future import select
    import logging
    logger = logging.getLogger("orders")

    # LIVE ORDERS
    live_open_orders = (await db.execute(
        select(UserOrder).join(User).where(
            UserOrder.order_company_name == symbol,
            UserOrder.order_status == 'OPEN',
            User.group_name == group_name
        )
    )).scalars().all()
    for order in live_open_orders:
        await db.refresh(order)
        if order.order_status != 'OPEN':
            logger.info(f"[SLTP_FLOW] Skipping order {order.order_id} as it is not OPEN (status={order.order_status})")
            continue
        logger.info(f"[SLTP_FLOW] After refresh: order {order.order_id} stop_loss={order.stop_loss}, take_profit={order.take_profit}")
        await process_order_stoploss_takeprofit(db, redis_client, order, user_type='live')

    # DEMO ORDERS
    demo_open_orders = (await db.execute(
        select(DemoUserOrder).join(DemoUser).where(
            DemoUserOrder.order_company_name == symbol,
            DemoUserOrder.order_status == 'OPEN',
            DemoUser.group_name == group_name
        )
    )).scalars().all()
    for order in demo_open_orders:
        await db.refresh(order)
        if order.order_status != 'OPEN':
            logger.info(f"[SLTP_FLOW] Skipping order {order.order_id} as it is not OPEN (status={order.order_status})")
            continue
        logger.info(f"[SLTP_FLOW] After refresh: order {order.order_id} stop_loss={order.stop_loss}, take_profit={order.take_profit}")
        await process_order_stoploss_takeprofit(db, redis_client, order, user_type='demo')


async def per_connection_redis_listener(
    websocket: WebSocket,
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
):
    pubsub = redis_client.pubsub()
    # Subscribe to all relevant channels
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    await pubsub.subscribe(REDIS_ORDER_UPDATES_CHANNEL)
    await pubsub.subscribe(REDIS_USER_DATA_UPDATES_CHANNEL)
    logger.info(f"User {user_id}: Subscribed to Redis channels for market data and updates")

    # Initial setup - fetch and cache static order data
    await update_static_orders_cache(user_id, db, redis_client, user_type)

    # Flag to track if this is the initial connection
    is_initial_connection = True
    # Cache to store all symbols data for initial connection
    all_symbols_cache = {}

    # Log WebSocket state
    logger.info(f"User {user_id}: WebSocket state: {websocket.client_state}")

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                # Periodically check WebSocket state
                if random.random() < 0.01:  # Log state approximately once every 100 iterations
                    logger.info(f"User {user_id}: WebSocket state: {websocket.client_state}")
                await asyncio.sleep(0.01)
                continue

            try:
                message_data = json.loads(message['data'], object_hook=decode_decimal)
                channel = message['channel'].decode('utf-8') if isinstance(message['channel'], bytes) else message['channel']
                
                # Log all received messages for debugging - use DecimalEncoder for JSON serialization
                logger.info(f"User {user_id}: Received message on channel {channel}: {json.dumps(message_data, cls=DecimalEncoder)[:200]}...")
                
                # Handle different message types based on channel
                if channel == REDIS_MARKET_DATA_CHANNEL:
                    if message_data.get("type") == "market_data_update":
                        # DIAGNOSTICS: Log delay from Firebase
                        firebase_ts = message_data.get("_timestamp")
                        if firebase_ts:
                            import time
                            delay = time.time() - firebase_ts
                            logger.info(f"User {user_id}: Market data delay from Firebase to WS listener entry: {delay:.4f} seconds.")

                        # Process market data update (existing code)
                        price_data_content = {k: v for k, v in message_data.items() if k not in ["type", "_timestamp"]}
                        
                        # Get group settings
                        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                        relevant_symbols = set(group_settings.keys())
                        
                        # Update and cache adjusted prices
                        adjusted_prices = await _calculate_and_cache_adjusted_prices(
                            raw_market_data=price_data_content,
                            group_name=group_name,
                            relevant_symbols=relevant_symbols,
                            group_settings=group_settings,
                            redis_client=redis_client
                        )
                        
                        # Update the all_symbols_cache with new data
                        all_symbols_cache.update(adjusted_prices)
                        
                        # Update dynamic portfolio data
                        await process_portfolio_update(
                            user_id=user_id,
                            group_name=group_name,
                            redis_client=redis_client,
                            db=db,
                            user_type=user_type,
                            adjusted_prices=adjusted_prices,
                            websocket=websocket,
                            is_initial_connection=is_initial_connection,
                            all_symbols_cache=all_symbols_cache
                        )
                        
                        # After first market data update, set initial connection to False
                        if is_initial_connection:
                            is_initial_connection = False
                            logger.info(f"User {user_id}: Initial connection completed, switching to incremental updates")
                
                elif channel == REDIS_ORDER_UPDATES_CHANNEL:
                    # Handle order updates
                    logger.info(f"User {user_id}: ORDER UPDATE CHANNEL - Full message data: {json.dumps(message_data, cls=DecimalEncoder)}")
                    if message_data.get("type") == "ORDER_UPDATE" and str(message_data.get("user_id")) == str(user_id):
                        logger.info(f"User {user_id}: Received order update notification, timestamp: {message_data.get('timestamp')}")
                        
                        # Force refresh of static orders cache from database
                        logger.info(f"User {user_id}: Forcing refresh of static orders cache from database")
                        
                        # IMPORTANT: Create a new database session for this operation to ensure fresh data
                        async with AsyncSessionLocal() as refresh_db:
                            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
                        
                        # Log the static orders that were fetched
                        open_orders_count = len(static_orders.get("open_orders", []))
                        pending_orders_count = len(static_orders.get("pending_orders", []))
                        logger.info(f"User {user_id}: Refreshed static orders cache: {open_orders_count} open orders, {pending_orders_count} pending orders")
                        logger.info(f"User {user_id}: Static orders content: {json.dumps(static_orders, cls=DecimalEncoder)}")
                        
                        # Get fresh user data to ensure we have the latest balance and margin
                        async with AsyncSessionLocal() as refresh_db:
                            user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
                            logger.info(f"User {user_id}: Fresh user data fetched for order update: {json.dumps(user_data, cls=DecimalEncoder) if user_data else None}")
                        
                        # Get the latest dynamic portfolio data
                        dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
                        if not dynamic_portfolio:
                            dynamic_portfolio = {
                                "balance": user_data.get("wallet_balance", "0.0") if user_data else "0.0",
                                "margin": user_data.get("margin", "0.0") if user_data else "0.0",
                                "free_margin": "0.0",
                                "profit_loss": "0.0",
                                "margin_level": "0.0"
                            }
                        
                        # Send update to client - use market_update type to match existing frontend handling
                        if websocket.client_state == WebSocketState.CONNECTED:
                            logger.info(f"User {user_id}: Sending order update to WebSocket client")
                            
                            # IMPORTANT: Use the values directly from user_data for balance and margin
                            # This ensures we're sending the most up-to-date values from the database
                            balance_value = user_data.get("wallet_balance", "0.0") if user_data else dynamic_portfolio.get("balance", "0.0")
                            margin_value = user_data.get("margin", "0.0") if user_data else dynamic_portfolio.get("margin", "0.0")
                            
                            # Ensure balance and margin are properly formatted as strings
                            if isinstance(balance_value, Decimal):
                                balance_value = str(balance_value)
                            if isinstance(margin_value, Decimal):
                                margin_value = str(margin_value)
                                
                            logger.info(f"User {user_id}: ORDER UPDATE - Formatted values for WebSocket: balance={balance_value} (type: {type(balance_value)}), margin={margin_value} (type: {type(margin_value)})")
                            
                            response_data = {
                                "type": "market_update",
                                "data": {
                                    "market_prices": all_symbols_cache,  # Send all symbols data for order updates
                                    "account_summary": {
                                        "balance": balance_value,
                                        "margin": margin_value,
                                        "open_orders": static_orders.get("open_orders", []) if static_orders else [],
                                        "pending_orders": static_orders.get("pending_orders", []) if static_orders else []
                                    }
                                }
                            }
                            logger.info(f"User {user_id}: WebSocket response data: {json.dumps(response_data, cls=DecimalEncoder)[:500]}...")
                            await websocket.send_text(json.dumps(response_data, cls=DecimalEncoder))
                            logger.info(f"User {user_id}: Sent orders update using market_update type")
                
                elif channel == REDIS_USER_DATA_UPDATES_CHANNEL:
                    # Handle user data updates
                    if message_data.get("type") == "USER_DATA_UPDATE" and str(message_data.get("user_id")) == str(user_id):
                        logger.info(f"User {user_id}: Received user data update notification")
                        
                        # Refresh user data cache with a fresh database session
                        async with AsyncSessionLocal() as refresh_db:
                            logger.info(f"User {user_id}: Fetching fresh user data from database")
                            user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
                            logger.info(f"User {user_id}: User data fetched from database: {json.dumps(user_data, cls=DecimalEncoder) if user_data else None}")
                            
                            # Log specific wallet_balance and margin values
                            if user_data:
                                wallet_balance = user_data.get("wallet_balance", "0.0")
                                margin = user_data.get("margin", "0.0")
                                logger.info(f"User {user_id}: IMPORTANT - Fresh wallet_balance={wallet_balance}, margin={margin}")
                        
                        # Get the latest dynamic portfolio data
                        dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
                        logger.info(f"User {user_id}: Dynamic portfolio from cache: {json.dumps(dynamic_portfolio, cls=DecimalEncoder) if dynamic_portfolio else None}")
                        
                        if not dynamic_portfolio:
                            logger.info(f"User {user_id}: No dynamic portfolio found in cache, creating default")
                            dynamic_portfolio = {
                                "balance": user_data.get("wallet_balance", "0.0") if user_data else "0.0",
                                "margin": user_data.get("margin", "0.0") if user_data else "0.0",
                                "free_margin": "0.0",
                                "profit_loss": "0.0",
                                "margin_level": "0.0"
                            }
                        
                        # Get static orders to include in the response - use fresh database session
                        async with AsyncSessionLocal() as refresh_db:
                            logger.info(f"User {user_id}: Fetching fresh static orders from database")
                            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
                            logger.info(f"User {user_id}: Static orders fetched from database: {len(static_orders.get('open_orders', []))} open orders, {len(static_orders.get('pending_orders', []))} pending orders")
                        
                        # Send update to client using market_update type for consistency
                        if websocket.client_state == WebSocketState.CONNECTED:
                            # IMPORTANT: Use the values directly from user_data for balance and margin
                            # This ensures we're sending the most up-to-date values from the database
                            balance_value = user_data.get("wallet_balance", "0.0") if user_data else dynamic_portfolio.get("balance", "0.0")
                            margin_value = user_data.get("margin", "0.0") if user_data else dynamic_portfolio.get("margin", "0.0")
                            
                            # Ensure balance and margin are properly formatted as strings
                            if isinstance(balance_value, Decimal):
                                balance_value = str(balance_value)
                            if isinstance(margin_value, Decimal):
                                margin_value = str(margin_value)
                                
                            logger.info(f"User {user_id}: IMPORTANT - Using balance_value={balance_value}, margin_value={margin_value} for WebSocket response")
                            
                            response_data = {
                                "type": "market_update",
                                "data": {
                                    "market_prices": all_symbols_cache,  # Send all symbols data for user data updates
                                    "account_summary": {
                                        "balance": balance_value,
                                        "margin": margin_value,
                                        "open_orders": static_orders.get("open_orders", []) if static_orders else [],
                                        "pending_orders": static_orders.get("pending_orders", []) if static_orders else []
                                    }
                                }
                            }
                            logger.info(f"User {user_id}: Sending user data update with balance={balance_value}, margin={margin_value}, {len(static_orders.get('open_orders', []))} open orders and {len(static_orders.get('pending_orders', []))} pending orders")
                            await websocket.send_text(json.dumps(response_data, cls=DecimalEncoder))
                            logger.info(f"User {user_id}: Sent user data update using market_update type")
            
            except json.JSONDecodeError as e:
                logger.error(f"User {user_id}: JSON decode error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"User {user_id}: Error in message processing: {e}", exc_info=True)

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected.")
    except Exception as e:
        logger.error(f"User {user_id}: Unexpected error: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.unsubscribe(REDIS_ORDER_UPDATES_CHANNEL)
        await pubsub.unsubscribe(REDIS_USER_DATA_UPDATES_CHANNEL)
        await pubsub.close()
        logger.info(f"User {user_id}: Unsubscribed from Redis and cleaned up.")

async def update_static_orders_cache(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user (open and pending orders without PnL).
    This is called when initializing the WebSocket connection and when order status changes.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        order_model = get_order_model(user_type)
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        logger.info(f"User {user_id}: Fetched {len(open_orders_orm)} open orders directly from database")
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                    'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            # Add updated_at field
            updated_at = getattr(pos, 'updated_at', None)
            if updated_at:
                pos_dict['updated_at'] = updated_at.isoformat() if isinstance(updated_at, datetime.datetime) else str(updated_at)
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        logger.info(f"User {user_id}: Fetched {len(pending_orders_orm)} pending orders directly from database")
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            # Add updated_at field
            updated_at = getattr(po, 'updated_at', None)
            if updated_at:
                po_dict['updated_at'] = updated_at.isoformat() if isinstance(updated_at, datetime.datetime) else str(updated_at)
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        logger.info(f"User {user_id}: Updated static orders cache with {len(open_orders_data)} open orders and {len(pending_orders_data)} pending orders")
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.datetime.now().isoformat()}

async def update_dynamic_portfolio_cache(
    user_id: int,
    group_name: str,
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, float]],
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
):
    """
    Update the dynamic portfolio cache for a user (free margin, positions with PnL, margin level).
    This is called whenever market data changes.
    """
    try:
        # Get user data for balance, leverage, etc.
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.warning(f"User data not found for user {user_id}. Cannot update dynamic portfolio.")
            return
        
        # Get group symbol settings for contract sizes, etc.
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
        if not group_symbol_settings:
            logger.warning(f"Group symbol settings not found for group {group_name}. Cannot update dynamic portfolio.")
            return
        
        # Calculate portfolio metrics using the portfolio calculator
        portfolio_metrics = await calculate_user_portfolio(
            user_data=user_data,
            open_positions=open_positions,
            adjusted_market_prices=adjusted_market_prices,
            group_symbol_settings=group_symbol_settings,
            redis_client=redis_client
        )
        
        # Cache the dynamic portfolio data
        dynamic_portfolio_data = {
            "balance": portfolio_metrics.get("balance", "0.0"),
            "equity": portfolio_metrics.get("equity", "0.0"),
            "margin": portfolio_metrics.get("margin", "0.0"),
            "free_margin": portfolio_metrics.get("free_margin", "0.0"),
            "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
            "margin_level": portfolio_metrics.get("margin_level", "0.0"),
            "positions_with_pnl": portfolio_metrics.get("positions", [])  # Positions with PnL calculations
        }
        await set_user_dynamic_portfolio_cache(redis_client, user_id, dynamic_portfolio_data)
        logger.debug(f"Updated dynamic portfolio cache for user {user_id}")
        
        return dynamic_portfolio_data
    except Exception as e:
        logger.error(f"Error updating dynamic portfolio cache for user {user_id}: {e}", exc_info=True)
        return None

async def process_portfolio_update(
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession,
    user_type: str,
    adjusted_prices: Dict[str, Dict[str, float]],
    websocket: WebSocket,
    is_initial_connection: bool,
    all_symbols_cache: Dict[str, Dict[str, float]]
):
    """
    Process market data updates, update dynamic portfolio data, and send updates to the client.
    """
    try:
        # PERFORMANCE FIX: Get static orders from cache. Do NOT fetch from DB on every market tick.
        # The cache is updated on connection and when an order update event is received.
        static_orders = await get_user_static_orders_cache(redis_client, user_id)
        if not static_orders:
            logger.warning(f"User {user_id}: Static orders cache empty. Forcing a refresh from DB (this should be rare).")
        async with AsyncSessionLocal() as refresh_db:
            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
        
        # Get open positions from static orders
        open_positions = static_orders.get("open_orders", []) if static_orders else []
        pending_orders = static_orders.get("pending_orders", []) if static_orders else []

        # Update dynamic portfolio data (PnL, free margin, etc.)
        await update_dynamic_portfolio_cache(
            user_id=user_id,
            group_name=group_name,
            open_positions=open_positions,
            adjusted_market_prices=adjusted_prices,
            redis_client=redis_client,
            db=db,
            user_type=user_type
        )
        
        # Get the latest dynamic portfolio data
        dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
        
        # Get fresh user data to ensure we have the latest balance and margin
        async with AsyncSessionLocal() as refresh_db:
            user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
            logger.debug(f"User {user_id}: Fresh user data fetched for market update: {json.dumps(user_data, cls=DecimalEncoder) if user_data else None}")
        
        if not dynamic_portfolio:
            # Get user data for basic account info
            dynamic_portfolio = {
                "balance": user_data.get("wallet_balance", "0.0") if user_data else "0.0",
                "margin": user_data.get("margin", "0.0") if user_data else "0.0",
                "free_margin": "0.0",
                "profit_loss": "0.0",
                "margin_level": "0.0"
            }

        if websocket.client_state == WebSocketState.CONNECTED:
            # IMPORTANT: Use the values directly from user_data for balance and margin
            # This ensures we're sending the most up-to-date values from the database
            balance_value = user_data.get("wallet_balance", "0.0") if user_data else dynamic_portfolio.get("balance", "0.0")
            margin_value = user_data.get("margin", "0.0") if user_data else dynamic_portfolio.get("margin", "0.0")
            
            # Ensure balance and margin are properly formatted as strings
            if isinstance(balance_value, Decimal):
                balance_value = str(balance_value)
            if isinstance(margin_value, Decimal):
                margin_value = str(margin_value)
                
            logger.info(f"User {user_id}: IMPORTANT - Using balance_value={balance_value}, margin_value={margin_value} for WebSocket response")
            
            # Determine which market prices to send based on connection state
            if is_initial_connection:
                # Send all symbols data on initial connection
                market_prices_to_send = all_symbols_cache
                logger.info(f"User {user_id}: Initial connection - sending all {len(all_symbols_cache)} symbols data")
            else:
                # Send only the updated symbols on subsequent updates
                market_prices_to_send = adjusted_prices
                logger.info(f"User {user_id}: Incremental update - sending {len(adjusted_prices)} updated symbols")
            
            response_data = {
                "type": "market_update",
                "data": {
                    "market_prices": market_prices_to_send,
                    "account_summary": {
                        "balance": balance_value,
                        "margin": margin_value,
                        "open_orders": open_positions,  # Static open orders without PnL
                        "pending_orders": pending_orders  # Static pending orders
                    }
                }
            }
            logger.info(f"User {user_id}: Sending market data update with balance={balance_value}, margin={margin_value}, {len(open_positions)} open orders and {len(pending_orders)} pending orders")
            await websocket.send_text(json.dumps(response_data, cls=DecimalEncoder))
            logger.debug(f"User {user_id}: Sent positions + market prices update")
    except Exception as e:
        logger.error(f"User {user_id}: Error processing portfolio update: {e}", exc_info=True)


# app/api/v1/endpoints/market_data_ws.py
# ... other imports ...
logger = websocket_logger # or temporarily: import logging; logger = logging.getLogger(__name__)
print("DEBUG: market_data_ws.py module imported!")



from fastapi import WebSocket, Depends
from sqlalchemy.orm import Session
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client

@router.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    logger.info("--- MINIMAL TEST: ENTERED websocket_endpoint ---")
    for handler in logger.handlers:
        handler.flush()
    db_user_instance: Optional[User | DemoUser] = None

    # Extract token from query params
    token = websocket.query_params.get("token")
    if token is None:
        logger.warning(f"WebSocket connection attempt without token from {websocket.client.host}:{websocket.client.port}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing token")
        return

    # Clean up token - remove any trailing quotes or encoded characters
    token = token.strip('"').strip("'").replace('%22', '').replace('%27', '')

    # Initialize Redis client
    redis_client = await get_redis_client()

    try:
        from jose import JWTError, ExpiredSignatureError
        try:
            payload = decode_token(token)
            account_number = payload.get("account_number")
            user_type = payload.get("user_type", "live")
            if not account_number:
                logger.warning(f"WebSocket auth failed: Invalid token payload - missing account_number")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: Account number missing")
                return

            # Strictly fetch from correct table based on user_type and account_number
            logger.info(f"WebSocket auth: token payload={payload}, account_number={account_number}, user_type={user_type}")
            if user_type == "demo":
                logger.info(f"WebSocket auth: About to call get_demo_user_by_account_number with account_number={account_number}, user_type={user_type}")
                db_user_instance = await get_demo_user_by_account_number(db, account_number, user_type)
                logger.info(f"WebSocket auth: get_demo_user_by_account_number({account_number}, {user_type}) returned: {db_user_instance}")
            else:
                logger.info(f"WebSocket auth: About to call get_user_by_account_number with account_number={account_number}, user_type={user_type}")
                db_user_instance = await get_user_by_account_number(db, account_number, user_type)
                logger.info(f"WebSocket auth: get_user_by_account_number({account_number}, {user_type}) returned: {db_user_instance}")

            if not db_user_instance:
                logger.warning(f"Authentication failed for account_number {account_number} (type {user_type}): User not found in correct table.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
                return
            
            if not getattr(db_user_instance, 'isActive', True):
                logger.warning(f"Authentication failed for user ID {user_id}: User inactive.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User inactive")
                return

        except ExpiredSignatureError:
            logger.warning(f"WebSocket auth failed: Token expired for {websocket.client.host}:{websocket.client.port}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token expired")
            return
        except JWTError as jwt_err:
            logger.warning(f"WebSocket auth failed: JWT error for {websocket.client.host}:{websocket.client.port}: {str(jwt_err)}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")
            return

        group_name = getattr(db_user_instance, 'group_name', 'default')
        
        # Get user ID from the user instance
        db_user_id = getattr(db_user_instance, 'id', None)
        if not db_user_id:
            logger.warning(f"Authentication failed: User instance missing ID field. Account: {account_number}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid user data")
            return
            
        # Initial caching of user data, portfolio, and group-symbol settings
        user_data_to_cache = {
            "id": getattr(db_user_instance, 'id', None),
            "email": getattr(db_user_instance, 'email', None),
            "account_number": account_number,
            "group_name": group_name,
            "leverage": Decimal(str(getattr(db_user_instance, 'leverage', 1.0))),
            "wallet_balance": Decimal(str(getattr(db_user_instance, 'wallet_balance', 0.0))),
            "margin": Decimal(str(getattr(db_user_instance, 'margin', 0.0))),
            "user_type": user_type,
            "first_name": getattr(db_user_instance, 'first_name', None),
            "last_name": getattr(db_user_instance, 'last_name', None),
            "country": getattr(db_user_instance, 'country', None),
            "phone_number": getattr(db_user_instance, 'phone_number', None)
        }
        await set_user_data_cache(redis_client, db_user_id, user_data_to_cache)

        # Always use user_type to select the correct order model
        order_model_class = get_order_model(user_type)
        logger.info(f"[WS] Using order model: {order_model_class.__name__} for user_type={user_type}, account_number={account_number}")
        # Use DB user_id (int) for querying open orders
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_id, order_model_class)
        logger.info(f"[WS] Open positions from DB for user_id={db_user_id}: {open_positions_orm}")

        initial_positions_data = []
        for pos in open_positions_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['commission'] = "0.0"  # Add commission field
            # Removed 'profit_loss', 'commission_applied', 'applied_commission' fields to match pending_orders format
            initial_positions_data.append(pos_dict)

        # Dynamically calculate margin from open positions
        total_margin = sum(Decimal(pos['margin']) for pos in initial_positions_data if 'margin' in pos)
        user_portfolio_data = {
            "balance": str(user_data_to_cache["wallet_balance"]),
            "equity": "0.0",
            "margin": str(total_margin),
            "free_margin": "0.0",
            "profit_loss": "0.0",
            "margin_level": "0.0",
            "positions": initial_positions_data
        }
        await set_user_portfolio_cache(redis_client, db_user_id, user_portfolio_data)
        await update_group_symbol_settings(group_name, db, redis_client)

    except Exception as e:
        logger.error(f"Unexpected WS auth error for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error")
        return

    # Accept the WebSocket connection
    try:
        await websocket.accept()
        logger.info(f"WebSocket connection accepted for user {account_number} (Group: {group_name}).")
    except Exception as accept_error:
        logger.error(f"Failed to accept WebSocket connection for user {account_number}: {accept_error}")
        return

    # Initialize static orders cache
    static_orders = await update_static_orders_cache(db_user_id, db, redis_client, user_type)
    
    # Initialize dynamic portfolio cache with empty data
    # This will be updated with the first market data update
    initial_dynamic_portfolio = {
        "balance": str(user_data_to_cache["wallet_balance"]),
        "equity": str(user_data_to_cache["wallet_balance"]),
        "margin": str(user_data_to_cache["margin"]),
        "free_margin": str(user_data_to_cache["wallet_balance"]),
        "profit_loss": "0.0",
        "margin_level": "0.0",
        "positions_with_pnl": []
    }
    await set_user_dynamic_portfolio_cache(redis_client, db_user_id, initial_dynamic_portfolio)

    # Send initial connection data with all available symbols
    try:
        # Check if the connection is still alive before proceeding
        if websocket.client_state == WebSocketState.DISCONNECTED:
            logger.warning(f"User {account_number}: Client disconnected before sending initial data")
            return

        # Get all available symbols for the group
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
        initial_symbols_data = {}
        
        if group_settings:
            # Import Firebase function to get latest market data
            from app.firebase_stream import get_latest_market_data as get_latest_market_data_sync
            
            # Get all symbols for this group
            group_symbols = list(group_settings.keys())
            logger.info(f"User {account_number}: Fetching initial market data for {len(group_symbols)} symbols from Firebase")
            
            # Fetch fresh market data from Firebase for all symbols
            for symbol in group_symbols:
                try:
                    # Get raw market data from Firebase
                    raw_market_data = get_latest_market_data_sync(symbol)
                    
                    if raw_market_data and raw_market_data.get('b') and raw_market_data.get('o'):
                        # Calculate adjusted prices using the same logic as _calculate_and_cache_adjusted_prices
                        symbol_group_settings = group_settings.get(symbol)
                        if symbol_group_settings:
                            try:
                                raw_ask_price = raw_market_data.get('b')  # Ask from Firebase
                                raw_bid_price = raw_market_data.get('o')  # Bid from Firebase
                                
                                ask_decimal = Decimal(str(raw_ask_price))
                                bid_decimal = Decimal(str(raw_bid_price))
                                spread_setting = Decimal(str(symbol_group_settings.get('spread', 0)))
                                spread_pip_setting = Decimal(str(symbol_group_settings.get('spread_pip', 0)))
                                
                                configured_spread_amount = spread_setting * spread_pip_setting
                                half_spread = configured_spread_amount / Decimal(2)

                                # Adjusted prices for user display and trading
                                adjusted_buy_price = ask_decimal + half_spread  # User buys at adjusted ask
                                adjusted_sell_price = bid_decimal - half_spread  # User sells at adjusted bid

                                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                                effective_spread_in_pips = Decimal("0.0")
                                if spread_pip_setting > Decimal("0.0"):
                                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting

                                # Store in initial symbols data
                                initial_symbols_data[symbol] = {
                                    'buy': float(adjusted_buy_price),
                                    'sell': float(adjusted_sell_price),
                                    'spread': float(effective_spread_in_pips)
                                }
                                
                                # Also update the cache with fresh data
                                await set_adjusted_market_price_cache(
                                    redis_client=redis_client,
                                    group_name=group_name,
                                    symbol=symbol,
                                    buy_price=adjusted_buy_price,
                                    sell_price=adjusted_sell_price,
                                    spread_value=configured_spread_amount
                                )
                                
                                logger.debug(f"User {account_number}: Fetched fresh data for {symbol}: Buy={adjusted_buy_price}, Sell={adjusted_sell_price}")
                                
                            except Exception as calc_error:
                                logger.error(f"User {account_number}: Error calculating adjusted prices for {symbol}: {calc_error}")
                                # Fallback to cached data if available
                                cached_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                                if cached_prices:
                                    initial_symbols_data[symbol] = {
                                        'buy': float(cached_prices.get('buy', 0)),
                                        'sell': float(cached_prices.get('sell', 0)),
                                        'spread': float(cached_prices.get('spread', 0))
                                    }
                    else:
                        logger.warning(f"User {account_number}: No market data from Firebase for {symbol}, trying cache")
                        # Fallback to cached data if Firebase has no data
                        cached_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                        if cached_prices:
                            initial_symbols_data[symbol] = {
                                'buy': float(cached_prices.get('buy', 0)),
                                'sell': float(cached_prices.get('sell', 0)),
                                'spread': float(cached_prices.get('spread', 0))
                            }
                        
                except Exception as symbol_error:
                    logger.error(f"User {account_number}: Error fetching data for {symbol}: {symbol_error}")
                    # Fallback to cached data
                    cached_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                    if cached_prices:
                        initial_symbols_data[symbol] = {
                            'buy': float(cached_prices.get('buy', 0)),
                            'sell': float(cached_prices.get('sell', 0)),
                            'spread': float(cached_prices.get('spread', 0))
                        }
        
        # Check connection state again before sending
        if websocket.client_state == WebSocketState.CONNECTED:
            # Send initial connection message with all symbols data
            initial_response = {
                "type": "market_update",
                "data": {
                    "market_prices": initial_symbols_data,
                    "account_summary": {
                        "balance": str(user_data_to_cache["wallet_balance"]),
                        "margin": str(user_data_to_cache["margin"]),
                        "open_orders": static_orders.get("open_orders", []) if static_orders else [],
                        "pending_orders": static_orders.get("pending_orders", []) if static_orders else []
                    }
                }
            }
            
            try:
                await websocket.send_text(json.dumps(initial_response, cls=DecimalEncoder))
                logger.info(f"User {account_number}: Sent initial connection data with {len(initial_symbols_data)} symbols (fresh from Firebase)")
            except WebSocketDisconnect:
                logger.warning(f"User {account_number}: Client disconnected during initial data send")
                return
            except Exception as send_error:
                logger.error(f"User {account_number}: Error sending initial data: {send_error}")
                return
        else:
            logger.warning(f"User {account_number}: Client disconnected before sending initial data")
            return

    except Exception as e:
        logger.error(f"User {account_number}: Error sending initial connection data: {e}", exc_info=True)

    # Create and manage the per-connection Redis listener task
    listener_task = asyncio.create_task(
        per_connection_redis_listener(websocket, db_user_id, group_name, redis_client, db, user_type)
    )

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info(f"User {account_number}: WebSocket disconnected by client.")
    except Exception as e:
        logger.error(f"User {account_number}: Error in main WebSocket loop: {e}", exc_info=True)
    finally:
        logger.info(f"User {account_number}: Cleaning up WebSocket connection.")
        if not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                logger.info(f"User {account_number}: Listener task successfully cancelled.")
            except Exception as task_e:
                logger.error(f"User {account_number}: Error during listener task cleanup: {task_e}", exc_info=True)
        
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            except Exception as close_e:
                logger.error(f"User {account_number}: Error explicitly closing WebSocket: {close_e}", exc_info=True)
        logger.info(f"User {account_number}: WebSocket connection fully closed.")


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
# This function remains largely the same.
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return
    try:
        group_settings_list = await crud_group.get_groups(db, search=group_name)
        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'.")
             return
        for group_setting in group_settings_list:
            symbol_name = getattr(group_setting, 'symbol', None)
            if symbol_name:
                settings = {
                    # ... (all your settings fields) ...
                    "commision_type": getattr(group_setting, 'commision_type', None),"commision_value_type": getattr(group_setting, 'commision_value_type', None),"type": getattr(group_setting, 'type', None),"pip_currency": getattr(group_setting, 'pip_currency', "USD"),"show_points": getattr(group_setting, 'show_points', None),"swap_buy": getattr(group_setting, 'swap_buy', decimal.Decimal(0.0)),"swap_sell": getattr(group_setting, 'swap_sell', decimal.Decimal(0.0)),"commision": getattr(group_setting, 'commision', decimal.Decimal(0.0)),"margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)),"spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)),"deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)),"min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)),"max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)),"pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)),"spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)),"contract_size": getattr(group_setting, 'contract_size', decimal.Decimal("100000")),
                }
                # Fetch profit_currency from Symbol model
                symbol_obj_stmt = select(Symbol).filter_by(name=symbol_name.upper())
                symbol_obj_result = await db.execute(symbol_obj_stmt)
                symbol_obj = symbol_obj_result.scalars().first()
                if symbol_obj and symbol_obj.profit_currency:
                    settings["profit_currency"] = symbol_obj.profit_currency
                else: # Fallback
                    settings["profit_currency"] = getattr(group_setting, 'pip_currency', 'USD')
                # Fetch contract_size from ExternalSymbolInfo (overrides group if found)
                external_symbol_obj_stmt = select(ExternalSymbolInfo).filter_by(fix_symbol=symbol_name) # Case-sensitive match?
                external_symbol_obj_result = await db.execute(external_symbol_obj_stmt)
                external_symbol_obj = external_symbol_obj_result.scalars().first()
                if external_symbol_obj and external_symbol_obj.contract_size is not None:
                    settings["contract_size"] = external_symbol_obj.contract_size
                
                await set_group_symbol_settings_cache(redis_client, group_name, symbol_name.upper(), settings)
            else:
                 logger.warning(f"Group setting symbol is None for group '{group_name}'.")
        logger.debug(f"Cached/updated group-symbol settings for group '{group_name}'.")
    except Exception as e:
        logger.error(f"Error caching group-symbol settings for '{group_name}': {e}", exc_info=True)


# --- Redis Publisher Task (Publishes from Firebase queue to general market data channel) ---
# This function remains the same.
async def redis_publisher_task(redis_client: Redis):
    logger.info("Redis publisher task started. Publishing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
    if not redis_client:
        logger.critical("Redis client not provided for publisher task. Exiting.")
        return
    try:
        while True:
            raw_market_data_message = await redis_publish_queue.get()
            if raw_market_data_message is None: # Shutdown signal
                logger.info("Publisher task received shutdown signal. Exiting.")
                break
            try:
                # DIAGNOSTICS: Keep the _timestamp key to measure delays.
                message_to_publish_data = raw_market_data_message.copy()
                
                # Check if there is meaningful data besides the timestamp
                if any(k != '_timestamp' for k in message_to_publish_data.keys()):
                     message_to_publish_data["type"] = "market_data_update" # Standardize type for raw updates
                     message_to_publish = json.dumps(message_to_publish_data, cls=DecimalEncoder)
                else: # Skip if only timestamp was present
                     redis_publish_queue.task_done()
                     continue
            except Exception as e:
                logger.error(f"Publisher failed to serialize message: {e}. Skipping.", exc_info=True)
                redis_publish_queue.task_done()
                continue
            try:
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
            except Exception as e:
                logger.error(f"Publisher failed to publish to Redis: {e}. Msg: {message_to_publish[:100]}...", exc_info=True)
            redis_publish_queue.task_done()
    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)
    finally:
        logger.info("Redis publisher task finished.")

# REMOVE redis_market_data_broadcaster function entirely
# Its functionality is now distributed into per_connection_redis_listener tasks managed by websocket_endpoint

# Add a debug endpoint to manually trigger order updates
@router.post("/debug/publish-order-update/{user_id}")
async def debug_publish_order_update(
    user_id: int,
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Debug endpoint to manually publish an order update message to Redis.
    This is useful for testing WebSocket functionality.
    """
    try:
        message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        
        result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
        logger.info(f"DEBUG: Published order update for user {user_id} to {REDIS_ORDER_UPDATES_CHANNEL}, received by {result} subscribers")
        
        return {"status": "success", "message": f"Order update published for user {user_id}", "subscribers": result}
    except Exception as e:
        logger.error(f"Error in debug_publish_order_update: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error publishing order update: {str(e)}")

# Add a debug endpoint to test the WebSocket order updates
@router.post("/debug/refresh-orders/{user_id}")
async def debug_refresh_orders(
    user_id: int,
    user_type: str = Query("demo"),
    redis_client: Redis = Depends(get_redis_client),
    db: AsyncSession = Depends(get_db)
):
    """
    Debug endpoint to manually refresh the static orders cache from the database.
    This bypasses Redis and ensures fresh data is fetched.
    """
    try:
        # Create a new database session for this operation
        async with AsyncSessionLocal() as refresh_db:
            # Force refresh of static orders cache from database
            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
            
        # Log the static orders that were fetched
        open_orders_count = len(static_orders.get("open_orders", []))
        pending_orders_count = len(static_orders.get("pending_orders", []))
        
        # Publish an order update message to Redis
        message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        
        result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
        logger.info(f"DEBUG: Published order update for user {user_id} to {REDIS_ORDER_UPDATES_CHANNEL}, received by {result} subscribers")
        
        return {
            "status": "success", 
            "message": f"Orders refreshed for user {user_id}", 
            "open_orders": open_orders_count,
            "pending_orders": pending_orders_count,
            "subscribers": result
        }
    except Exception as e:
        logger.error(f"Error in debug_refresh_orders: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error refreshing orders: {str(e)}")

# Add a debug endpoint to manually publish a user data update message to Redis
@router.post("/debug/publish-user-data-update/{user_id}")
async def debug_publish_user_data_update(
    user_id: int,
    redis_client: Redis = Depends(get_redis_client),
    db: AsyncSession = Depends(get_db)
):
    """
    Debug endpoint to manually publish a user data update message to Redis.
    This is useful for testing WebSocket functionality.
    """
    try:
        # Publish a user data update message
        message = json.dumps({
            "type": "USER_DATA_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        
        result = await redis_client.publish(REDIS_USER_DATA_UPDATES_CHANNEL, message)
        logger.info(f"DEBUG: Published user data update for user {user_id} to {REDIS_USER_DATA_UPDATES_CHANNEL}, received by {result} subscribers")
        
        # Force refresh of user data cache from database
        async with AsyncSessionLocal() as refresh_db:
            user_type = "demo"  # Default to demo for testing
            user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
            logger.info(f"DEBUG: Refreshed user data cache for user {user_id}: {user_data}")
        
        return {
            "status": "success", 
            "message": f"User data update published for user {user_id}", 
            "subscribers": result,
            "user_data": user_data
        }
    except Exception as e:
        logger.error(f"Error in debug_publish_user_data_update: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error publishing user data update: {str(e)}")

# Add a debug endpoint to test the entire WebSocket update flow
@router.post("/debug/test-all-updates/{user_id}")
async def debug_test_all_updates(
    user_id: int,
    user_type: str = Query("demo"),
    redis_client: Redis = Depends(get_redis_client),
    db: AsyncSession = Depends(get_db)
):
    """
    Debug endpoint to test the entire WebSocket update flow.
    This will:
    1. Refresh the user data cache
    2. Refresh the static orders cache
    3. Publish a user data update
    4. Publish an order update
    """
    try:
        # Step 1: Refresh user data cache
        async with AsyncSessionLocal() as refresh_db:
            user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
            logger.info(f"DEBUG: Refreshed user data cache for user {user_id}: {json.dumps(user_data, cls=DecimalEncoder) if user_data else None}")
        
        # Step 2: Refresh static orders cache
        async with AsyncSessionLocal() as refresh_db:
            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
            open_orders_count = len(static_orders.get("open_orders", []))
            pending_orders_count = len(static_orders.get("pending_orders", []))
            logger.info(f"DEBUG: Refreshed static orders cache: {open_orders_count} open orders, {pending_orders_count} pending orders")
        
        # Step 3: Publish user data update
        user_data_message = json.dumps({
            "type": "USER_DATA_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        user_data_result = await redis_client.publish(REDIS_USER_DATA_UPDATES_CHANNEL, user_data_message)
        logger.info(f"DEBUG: Published user data update, received by {user_data_result} subscribers")
        
        # Step 4: Publish order update
        order_update_message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        order_update_result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, order_update_message)
        logger.info(f"DEBUG: Published order update, received by {order_update_result} subscribers")
        
        return {
            "status": "success",
            "message": "All updates published successfully",
            "user_data_subscribers": user_data_result,
            "order_update_subscribers": order_update_result,
            "user_data": user_data,
            "static_orders": {
                "open_orders_count": open_orders_count,
                "pending_orders_count": pending_orders_count
            }
        }
    except Exception as e:
        logger.error(f"Error in debug_test_all_updates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error testing updates: {str(e)}")