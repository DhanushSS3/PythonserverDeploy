# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
from app.crud.crud_order import get_order_model
import json
# import threading # No longer needed for active_connections_lock
from typing import Dict, Any, List, Optional, Set
import decimal
from starlette.websockets import WebSocketState
from decimal import Decimal


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db, AsyncSessionLocal # Import AsyncSessionLocal for db sessions in tasks
from app.crud import user as crud_user
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
    DecimalEncoder, decode_decimal
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

# Configure logging for this module
logger = logging.getLogger(__name__)


# REMOVE: active_websocket_connections and active_connections_lock
# active_websocket_connections: Dict[int, Dict[str, Any]] = {}
# active_connections_lock = threading.Lock() 

# Redis channel for RAW market data updates from Firebase via redis_publisher_task
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'


router = APIRouter(
    tags=["market_data_ws"]
)


async def _calculate_and_cache_adjusted_prices(
    raw_market_data: Dict[str, Any], # e.g. {'EURUSD': {'o': 1.1, 'b': 1.09}}
    group_name: str,
    relevant_symbols: Set[str], # Symbols relevant to the user's group
    group_settings: Dict[str, Any], # All settings for the user's group
    redis_client: Redis
) -> Dict[str, Dict[str, float]]:
    """
    Calculates adjusted prices based on group settings and caches them.
    Returns a dictionary of adjusted prices for symbols in raw_market_data.
    Input raw_market_data keys: 'o' for original bid, 'b' for original ask (as per firebase_stream logic for data_for_queue)
    Output keys for frontend: 'buy', 'sell', 'spread'
    """
    adjusted_prices_payload = {}
    for symbol, prices in raw_market_data.items():
        symbol_upper = symbol.upper()
        if symbol_upper not in relevant_symbols or not isinstance(prices, dict):
            continue

        # Firebase 'b' is Ask, 'o' is Bid (as per process_single_user_update and firebase_stream.py)
        raw_ask_price = prices.get('b') # Ask from Firebase
        raw_bid_price = prices.get('o') # Bid from Firebase
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
                adjusted_sell_price = bid_decimal - half_spread # User sells at adjusted bid

                effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                effective_spread_in_pips = Decimal("0.0")
                if spread_pip_setting > Decimal("0.0"):
                    effective_spread_in_pips = effective_spread_price_units / spread_pip_setting
                
                adjusted_prices_payload[symbol_upper] = {
                    'buy': float(adjusted_buy_price),
                    'sell': float(adjusted_sell_price),
                    'spread': float(effective_spread_in_pips),
                }
                await set_adjusted_market_price_cache(
                    redis_client=redis_client, group_name=group_name, symbol=symbol_upper,
                    buy_price=adjusted_buy_price, sell_price=adjusted_sell_price,
                    spread_value=configured_spread_amount # Cache configured spread for reference
                )
            except Exception as e:
                logger.error(f"Error adjusting price for {symbol_upper} in group {group_name}: {e}", exc_info=True)
                # Optionally send raw if calculation fails
                if raw_ask_price is not None and raw_bid_price is not None:
                    raw_spread = (Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price)))
                    raw_spread_pips = raw_spread / spread_pip_setting if spread_pip_setting > Decimal("0.0") else raw_spread
                    adjusted_prices_payload[symbol_upper] = {
                        'buy': float(raw_ask_price), 'sell': float(raw_bid_price), 'spread': float(raw_spread_pips)
                    }
    return adjusted_prices_payload


async def _get_full_portfolio_details(
    user_id: int,
    group_name: str, # User's group name
    redis_client: Redis,
    # db_session_factory: Callable[[], AsyncGenerator[AsyncSession, None]] # For DB operations if needed directly
) -> Optional[Dict[str, Any]]:
    """
    Fetches all necessary data from cache and calculates the full user portfolio.
    """
    user_data = await get_user_data_cache(redis_client, user_id)
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
        else:
            # logger.debug(f"No cached adjusted price for {sym_upper} for portfolio calc of user {user_id}")
            # If a symbol relevant to the group has no cached price, P&L for positions in it can't be live.
            # Consider how to handle this: error, skip, use last known. For now, it will be missing from input.
            pass


    portfolio_metrics = await calculate_user_portfolio(
        user_data=user_data, # This is a dict
        open_positions=open_positions, # List of dicts
        adjusted_market_prices=market_prices_for_calc, # Dict of symbol -> {'buy': Decimal, 'sell': Decimal}
        group_symbol_settings=group_symbol_settings_all # Dict of symbol -> settings dict
    )
    
    # Ensure all values in portfolio_metrics are JSON serializable (e.g. Decimals to str/float)
    # calculate_user_portfolio should ideally return serializable data or we convert here.
    # Assuming calculate_user_portfolio already handles this or returns data that DecimalEncoder can handle.
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


async def per_connection_redis_listener(
    websocket: WebSocket,
    user_id: int,
    group_name: str, # User's group name
    redis_client: Redis,
    # db_session_factory: Callable[[], AsyncGenerator[AsyncSession, None]] # For DB ops if needed
):
    """
    A per-WebSocket task that subscribes to general market data and user-specific account updates.
    """
    user_account_channel = f"user_updates:{user_id}"
    raw_market_data_feed_channel = REDIS_MARKET_DATA_CHANNEL # For all users

    pubsub = redis_client.pubsub()
    await pubsub.subscribe(raw_market_data_feed_channel)
    await pubsub.subscribe(user_account_channel)
    logger.info(f"User {user_id}: Subscribed to {raw_market_data_feed_channel} and {user_account_channel}")

    # Initial full data send
    try:
        # Send initial market prices (all relevant cached)
        group_settings_initial = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
        if group_settings_initial:
            initial_market_payload = {}
            for sym_upper in group_settings_initial.keys():
                cached_price = await get_adjusted_market_price_cache(redis_client, group_name, sym_upper)
                if cached_price:
                    initial_market_payload[sym_upper] = {
                        'buy': float(cached_price.get('buy', 0.0)),
                        'sell': float(cached_price.get('sell', 0.0)),
                        'spread': float( (Decimal(str(cached_price.get('buy',0.0))) - Decimal(str(cached_price.get('sell',0.0)))) / Decimal(str(group_settings_initial[sym_upper].get('spread_pip', 1))) if Decimal(str(group_settings_initial[sym_upper].get('spread_pip', 1))) > 0 else 0.0)
                    }
            if initial_market_payload and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(json.dumps({"type": "market_prices", "data": initial_market_payload}, cls=DecimalEncoder))
        
        # Send initial account summary
        initial_portfolio = await _get_full_portfolio_details(user_id, group_name, redis_client)
        if initial_portfolio and websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_text(json.dumps({"type": "account_summary", "data": initial_portfolio}, cls=DecimalEncoder))
            logger.debug(f"User {user_id}: Sent initial portfolio and market data.")

    except Exception as e_init:
        logger.error(f"User {user_id}: Error sending initial data: {e_init}", exc_info=True)


    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                await asyncio.sleep(0.01) # Short sleep to prevent tight loop on timeout
                continue

            received_channel = message['channel']
            
            # Always fetch fresh group settings and relevant symbols inside the loop,
            # in case they are updated dynamically elsewhere (though not shown in current code).
            # For performance, these could be cached with a TTL within this listener's scope if they rarely change.
            current_group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
            if not current_group_settings:
                logger.warning(f"User {user_id}: No group settings found for group {group_name}. Skipping message processing.")
                continue
            current_relevant_symbols = set(current_group_settings.keys())

            should_send_market_update = False
            market_prices_to_send = {}

            if received_channel == raw_market_data_feed_channel:
                try:
                    raw_market_data_update = json.loads(message['data'], object_hook=decode_decimal)
                    # The type "market_data_update" is from redis_publisher_task
                    if raw_market_data_update.get("type") == "market_data_update":
                        # Filter out the "type" key to get the actual price data
                        price_data_content = {k: v for k, v in raw_market_data_update.items() if k != "type"}
                        
                        adjusted_prices_for_this_update = await _calculate_and_cache_adjusted_prices(
                            price_data_content, group_name, current_relevant_symbols, current_group_settings, redis_client
                        )
                        if adjusted_prices_for_this_update:
                            market_prices_to_send = adjusted_prices_for_this_update
                            should_send_market_update = True
                except Exception as e_market_proc:
                    logger.error(f"User {user_id}: Error processing raw market data: {e_market_proc}", exc_info=True)
            
            # elif received_channel == user_account_channel:
                # The event from orders.py (ACCOUNT_STRUCTURE_CHANGED) primarily signals that
                # user_data_cache or user_portfolio_cache (positions) has changed.
                # The portfolio recalculation below will pick up these changes.
                # No market prices are sent from this branch, but portfolio will be.
                # logger.debug(f"User {user_id}: Received signal on user channel {user_account_channel}. Triggering portfolio update.")
                # pass # Fall through to portfolio update

            # Send market price updates if there are any from this message
            if should_send_market_update and market_prices_to_send and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(json.dumps({"type": "market_prices", "data": market_prices_to_send}, cls=DecimalEncoder))
                # logger.debug(f"User {user_id}: Sent market price update for symbols: {list(market_prices_to_send.keys())}")

            # Always (try to) send portfolio details if there was any message from subscribed channels,
            # as P&L changes with market data, and structure changes with user channel signals.
            full_portfolio_details = await _get_full_portfolio_details(user_id, group_name, redis_client)
            if full_portfolio_details and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(json.dumps({"type": "account_summary", "data": full_portfolio_details}, cls=DecimalEncoder))
                # logger.debug(f"User {user_id}: Sent full portfolio update. Triggered by channel: {received_channel}")

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected during Redis listening.")
    except asyncio.CancelledError:
        logger.info(f"User {user_id}: Redis listener task cancelled.")
    except Exception as e:
        logger.error(f"User {user_id}: Unexpected error in Redis listener: {e}", exc_info=True)
    finally:
        logger.info(f"User {user_id}: Cleaning up Redis subscriptions for {raw_market_data_feed_channel} and {user_account_channel}.")
        if pubsub: # Check if pubsub was initialized
            try:
                await pubsub.unsubscribe(raw_market_data_feed_channel)
                await pubsub.unsubscribe(user_account_channel)
            except Exception as unsub_e:
                logger.error(f"User {user_id}: Error during pubsub unsubscribe: {unsub_e}")
            try:
                await pubsub.close() # Close the pubsub connection object
            except Exception as close_e:
                 logger.error(f"User {user_id}: Error during pubsub.close(): {close_e}")


@router.websocket("/ws/market-data")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
    db: AsyncSession = Depends(get_db), # Keep DB for initial setup if needed
    redis_client: Redis = Depends(get_redis_client)
):
    logger.info(f"Attempting to accept WebSocket connection from {websocket.client.host}:{websocket.client.port} with token...")
    user_id: Optional[int] = None
    group_name: Optional[str] = None
    db_user_instance: Optional[User | DemoUser] = None

    try:
        from jose import JWTError
        payload = decode_token(token)
        user_id = int(payload.get("sub"))
        if user_id is None:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: User ID missing")

        # Try fetching as User, then as DemoUser if not found
        db_user = await crud_user.get_user_by_id(db, user_id)
        if db_user:
            db_user_instance = db_user
        else:
            db_demo_user = await crud_user.get_demo_user_by_id(db, user_id) # Assumes this function exists
            if db_demo_user:
                db_user_instance = db_demo_user
            else: # User not found in either table
                logger.warning(f"Authentication failed for user ID {user_id}: User not found in any table.")
                raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
        
        if not getattr(db_user_instance, 'isActive', True): # Check isActive for both types
             logger.warning(f"Authentication failed for user ID {user_id}: User inactive.")
             raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User inactive")

        group_name = getattr(db_user_instance, 'group_name', 'default')
        
        # Initial caching of user data, portfolio, and group-symbol settings
        # This is important as the per-connection listener will rely on these caches.
        user_data_to_cache = {
            "id": user_id, "group_name": group_name,
            "leverage": Decimal(str(getattr(db_user_instance, 'leverage', 1.0))),
            "wallet_balance": Decimal(str(getattr(db_user_instance, 'wallet_balance', 0.0))),
            "margin": Decimal(str(getattr(db_user_instance, 'margin', 0.0))) # Overall margin
        }
        await set_user_data_cache(redis_client, user_id, user_data_to_cache)

        order_model_class = get_order_model(db_user_instance)
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model_class)
        initial_positions_data = []
        for pos in open_positions_orm:
             pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                         for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
             pos_dict['profit_loss'] = "0.0" # Will be calculated by portfolio calculator
             initial_positions_data.append(pos_dict)
        
        # Also fetch pending positions for the initial cache state
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_positions_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model_class)
        initial_pending_positions_data = []
        for pos in pending_positions_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                         for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['margin'] = "0.0" # Pending orders have 0 margin
            initial_pending_positions_data.append(pos_dict)

        user_portfolio_data = {
             "balance": str(user_data_to_cache["wallet_balance"]), "equity": "0.0",
             "margin": str(user_data_to_cache["margin"]), "free_margin": "0.0",
             "profit_loss": "0.0", "margin_level": "0.0",
             "positions": initial_positions_data,
             "pending_positions": initial_pending_positions_data # Add pending positions
        }
        await set_user_portfolio_cache(redis_client, user_id, user_portfolio_data)
        await update_group_symbol_settings(group_name, db, redis_client) # Cache group settings

    except JWTError:
        logger.warning(f"WS Auth failed {websocket.client.host}:{websocket.client.port}: Invalid token.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION) # Close before raising
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid authentication token")
    except WebSocketException as wse:
        logger.warning(f"WS Auth failed for {user_id} from {websocket.client.host}:{websocket.client.port}: {wse.reason}")
        if websocket.client_state != WebSocketState.DISCONNECTED: # Ensure close if not already closed by raise
            await websocket.close(code=wse.code)
        raise # Re-raise to FastAPI to handle
    except Exception as e:
        logger.error(f"Unexpected WS auth error for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        if websocket.client_state != WebSocketState.DISCONNECTED:
             await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        raise WebSocketException(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error")

    await websocket.accept()
    logger.info(f"WebSocket connection accepted for user {user_id} (Group: {group_name}).")

    # Create and manage the per-connection Redis listener task
    listener_task = asyncio.create_task(
        per_connection_redis_listener(websocket, user_id, group_name, redis_client)
    )

    try:
        # Keep the connection alive and detect client-side disconnects
        while True:
            # This receive_text is primarily to detect if the client closes the connection.
            # Actual data from client isn't processed by this template.
            await websocket.receive_text() 
            # If you expect messages from client, process them here.
            # logger.debug(f"User {user_id}: Received text (usually keep-alive or command): {data}")

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected by client.")
    except Exception as e:
        # Log other errors that might occur in the receive_text loop
        logger.error(f"User {user_id}: Error in main WebSocket loop: {e}", exc_info=True)
    finally:
        logger.info(f"User {user_id}: Cleaning up WebSocket connection.")
        if not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task # Allow cancellation to complete
            except asyncio.CancelledError:
                logger.info(f"User {user_id}: Listener task successfully cancelled.")
            except Exception as task_e:
                logger.error(f"User {user_id}: Error during listener task cleanup: {task_e}", exc_info=True)
        
        # Ensure WebSocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            except Exception as close_e:
                logger.error(f"User {user_id}: Error explicitly closing WebSocket: {close_e}", exc_info=True)
        logger.info(f"User {user_id}: WebSocket connection for user {user_id} fully closed.")


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
                # Filter out the _timestamp key before publishing, ensure "type" is added
                message_to_publish_data = {k: v for k, v in raw_market_data_message.items() if k != '_timestamp'}
                if message_to_publish_data: # Ensure there's data other than just timestamp
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