# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
from app.core.logging_config import websocket_logger

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
    set_last_known_price, get_last_known_price,  # <-- Add these for last known price caching
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
logger = websocket_logger

# REMOVE: active_websocket_connections and active_connections_lock
# active_websocket_connections: Dict[int, Dict[str, Any]] = {}
# active_connections_lock = threading.Lock() 

# Redis channel for RAW market data updates from Firebase via redis_publisher_task
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'


router = APIRouter(
    tags=["market_data_ws"]
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


async def per_connection_redis_listener(
    websocket: WebSocket,
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession
):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    logger.info(f"User {user_id}: Subscribed to {REDIS_MARKET_DATA_CHANNEL}")

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                await asyncio.sleep(0.01)
                continue

            if message['channel'] == REDIS_MARKET_DATA_CHANNEL:
                try:
                    raw_market_data_update = json.loads(message['data'], object_hook=decode_decimal)
                    if raw_market_data_update.get("type") == "market_data_update":
                        price_data_content = {k: v for k, v in raw_market_data_update.items() if k != "type"}

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

                        # Get open positions from portfolio cache
                        portfolio_cache = await get_user_portfolio_cache(redis_client, user_id)
                        positions = portfolio_cache.get("positions", []) if portfolio_cache else []

                        # Fallback to DB if positions are empty
                        if not positions:
                            order_model = get_order_model("live")  # adjust if using user_type
                            open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                            for pos in open_positions_orm:
                                positions.append({
                                    "order_id": pos.order_id,
                                    "order_company_name": pos.order_company_name,
                                    "order_type": pos.order_type,
                                    "order_quantity": str(pos.order_quantity),
                                    "order_price": str(pos.order_price),
                                    "margin": str(pos.margin),
                                    "contract_value": str(pos.contract_value),
                                    "profit_loss": "0.0",
                                    "commission": "0.0"
                                })

                        # Attach current price from adjusted prices
                        for pos in positions:
                            symbol = pos.get("order_company_name", "").upper()
                            pos["current_price"] = str(adjusted_prices.get(symbol, {}).get("buy", "0.0"))

                        if websocket.client_state == WebSocketState.CONNECTED:
                            await websocket.send_text(json.dumps({
                                "type": "market_update",
                                "data": {
                                    "market_prices": adjusted_prices,
                                    "positions": positions
                                }
                            }, cls=DecimalEncoder))
                            logger.debug(f"User {user_id}: Sent positions + market prices update")

                except Exception as e:
                    logger.error(f"User {user_id}: Error in market data processing: {e}", exc_info=True)

    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected.")
    except Exception as e:
        logger.error(f"User {user_id}: Unexpected error: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.close()
        logger.info(f"User {user_id}: Unsubscribed from Redis and cleaned up.")


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
                db_user_instance = await crud_user.get_demo_user_by_account_number(db, account_number, user_type)
                logger.info(f"WebSocket auth: get_demo_user_by_account_number({account_number}, {user_type}) returned: {db_user_instance}")
            else:
                logger.info(f"WebSocket auth: About to call get_user_by_account_number with account_number={account_number}, user_type={user_type}")
                db_user_instance = await crud_user.get_user_by_account_number(db, account_number, user_type)
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
        
        # Initial caching of user data, portfolio, and group-symbol settings
        user_data_to_cache = {
            "account_number": account_number, "group_name": group_name,
            "leverage": Decimal(str(getattr(db_user_instance, 'leverage', 1.0))),
            "wallet_balance": Decimal(str(getattr(db_user_instance, 'wallet_balance', 0.0))),
            "margin": Decimal(str(getattr(db_user_instance, 'margin', 0.0)))
        }
        await set_user_data_cache(redis_client, account_number, user_data_to_cache)

        # Always use user_type to select the correct order model
        order_model_class = get_order_model(user_type)
        logger.info(f"[WS] Using order model: {order_model_class.__name__} for user_type={user_type}, account_number={account_number}")
        # Use DB user_id (int) for querying open orders
        db_user_id = getattr(db_user_instance, 'id', None)
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_id, order_model_class)
        logger.info(f"[WS] Open positions from DB for user_id={db_user_id}: {open_positions_orm}")

        initial_positions_data = []
        for pos in open_positions_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['profit_loss'] = "0.0"
            pos_dict['commission'] = "0.0"  # Add commission field
            pos_dict['commission_applied'] = "0.0"  # Add commission_applied field
            pos_dict['applied_commission'] = "0.0"  # Add applied_commission field
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
    await websocket.accept()
    logger.info(f"WebSocket connection accepted for user {account_number} (Group: {group_name}).")

    # Create and manage the per-connection Redis listener task
    # listener_task = asyncio.create_task(
    #     per_connection_redis_listener(websocket, account_number, group_name, redis_client, db)
    # )
    listener_task = asyncio.create_task(
        per_connection_redis_listener(websocket, db_user_id, group_name, redis_client, db)
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