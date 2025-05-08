# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
import json
import threading
from typing import Dict, Any, List, Optional
import decimal

# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.crud import user as crud_user
from app.crud import group as crud_group # Ensure you have this CRUD module/functions

# Import security functions for token validation
from app.core.security import decode_token

# Import the Redis client type
from redis.asyncio import Redis

# Import the caching helper functions (updated)
from app.core.cache import (
    set_user_data_cache,
    get_user_data_cache,
    set_user_portfolio_cache,
    get_user_portfolio_cache,
    get_user_positions_from_cache,
    set_group_symbol_settings_cache, # New import
    get_group_symbol_settings_cache  # New import
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue
from app.shared_state import redis_publish_queue


# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Dictionary to keep track of active WebSocket connections for *this process*
# Maps WebSocket object to a dictionary containing essential user info (primarily user_id)
active_websocket_connections: Dict[WebSocket, Dict[str, Any]] = {}
connections_lock = threading.Lock()

# Define the Redis channel for market data updates
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'

# --- Placeholder Functions for Calculations ---

# Placeholder: Calculate half of the spread value (updated to take spread/pip as Decimal)
def calculate_half_spread(spread: decimal.Decimal, spread_pip: decimal.Decimal) -> decimal.Decimal:
    """
    Calculates half of the spread based on group and symbol specific settings.
    Ensure correct handling of financial precision using Decimal.
    """
    try:
        spread_value = spread * spread_pip
        half_spread = spread_value / decimal.Decimal('2')
        # You might want to round or quantize half_spread based on instrument precision
        return half_spread
    except (ValueError, TypeError, decimal.InvalidOperation) as e:
        logger.error(f"Error calculating half_spread: spread={spread}, spread_pip={spread_pip}", exc_info=True)
        return decimal.Decimal('0.0')

# Placeholder: Perform margin, equity, P&L, etc. calculations (updated signature)
# This function takes adjusted market prices, user portfolio, and user data
# It will need symbol-specific settings (like margin rates)
def calculate_account_metrics(
    adjusted_market_data: Dict[str, Dict[str, decimal.Decimal]],
    user_portfolio: Dict[str, Any],
    user_data: Dict[str, Any],
    # Pass group-symbol settings needed for margin calculation etc.
    # This could be a dictionary mapping symbols to their settings for this user's group
    group_symbol_settings: Dict[str, Dict[str, decimal.Decimal]]
) -> Dict[str, decimal.Decimal]:
    """
    Calculates user's margin, equity, P&L, etc. based on adjusted prices, portfolio, user data,
    and group-symbol specific settings.
    """
    # --- REPLACE WITH YOUR ACTUAL CALCULATION LOGIC ---
    logger.debug(f"Calculating account metrics for user {user_data.get('user_id')}.")

    balance = user_portfolio.get("balance", decimal.Decimal('0.0'))
    leverage = user_data.get("leverage", decimal.Decimal('1.0'))
    open_positions = user_portfolio.get("positions", [])

    equity = balance
    margin_used = decimal.Decimal('0.0')
    pnl = decimal.Decimal('0.0')

    # Example simplistic P&L calculation
    # You will need symbol-specific information (e.g., contract size, tick value)
    # from the group_symbol_settings or another cache.
    for position in open_positions:
        symbol = position.get("symbol")
        direction = position.get("direction")
        volume = position.get("volume", decimal.Decimal('0.0'))
        open_price = position.get("open_price", decimal.Decimal('0.0'))

        latest_adjusted_bid = adjusted_market_data.get(symbol, {}).get('b', None)
        latest_adjusted_ask = adjusted_market_data.get(symbol, {}).get('o', None)

        # Get symbol-specific settings for this user's group
        symbol_settings = group_symbol_settings.get(symbol.upper(), {})
        # Example: Assuming contract_size is available in symbol settings or a separate cache
        contract_size = symbol_settings.get("contract_size", decimal.Decimal('100000')) # Placeholder, implement actual fetch

        if symbol and volume > 0 and open_price > 0:
             if direction == "buy" and latest_adjusted_bid is not None:
                  pnl += (latest_adjusted_bid - open_price) * volume * contract_size

             elif direction == "sell" and latest_adjusted_ask is not None:
                   pnl += (open_price - latest_adjusted_ask) * volume * contract_size

        # Calculate margin for this position
        # This also needs symbol-specific margin rates/formulas from group_symbol_settings or another cache
        # Example: margin_per_position = calculate_position_margin(position, adjusted_market_data, leverage, symbol_settings)
        # margin_used += margin_per_position

    equity = balance + pnl

    # Total margin used calculation (sum of individual position margins)
    # free_margin = equity - margin_used
    # margin_level = (equity / margin_used * decimal.Decimal('100')) if margin_used > 0 else decimal.Decimal('0.0')

    return {
        "equity": equity,
        "margin_used": margin_used, # Implement actual calculation
        "free_margin": equity, # Placeholder until margin_used is calculated
        "pnl": pnl,
        "balance": balance,
        "margin_level": decimal.Decimal('0.0') # Placeholder until margin_used is calculated
    }
    # --- END PLACEHOLDER ---


# Background task to subscribe to Redis Pub/Sub, perform calculations, and send personalized data
async def redis_market_data_broadcaster(redis_client: Redis):
    """
    Background task that subscribes to the Redis market data channel,
    performs per-user spread adjustment and calculations using cached
    group-symbol settings, and sends personalized data to active WebSocket
    connections in this process.
    Accepts the initialized redis_client instance.
    """
    logger.info(f"Redis market data broadcaster task started. Subscribing to channel '{REDIS_MARKET_DATA_CHANNEL}'.")

    if not redis_client:
        logger.critical("Redis client not provided to broadcaster task. Cannot subscribe to market data channel.")
        while True: await asyncio.sleep(60)
        return

    pubsub = None

    try:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
        logger.info(f"Successfully subscribed to Redis channel '{REDIS_MARKET_DATA_CHANNEL}'.")

        # We need access to the DB session *within* this async task if group-symbol
        # settings are not in cache. This is complex.
        # A better approach for production is to have a separate task or service
        # that ensures the group_symbol_settings cache is *always* populated
        # or updated whenever settings change in the DB.
        # For this step, let's assume we can get a DB session pool or similar here,
        # or that the cache fetch function handles DB fallback internally.
        # Let's modify the get_group_symbol_settings_cache to handle DB fallback for now (simplified).

        # To get a DB session within the broadcaster task, you would typically
        # pass the sessionmaker or a connection pool from main.py.
        # For now, let's call get_db() inside the loop, which is NOT ideal
        # for performance but works for demonstration. A better way is needed for production.
        # Alternatively, modify cache functions to accept db session or pool.
        # Let's modify get_group_symbol_settings_cache to accept db for fallback.
        # This means the broadcaster needs the db session dependency too. This is also tricky
        # for a background task not tied to a request.

        # Let's stick to the plan: Modify get_group_symbol_settings_cache to
        # accept redis_client and potentially a DB session/pool or rely on
        # a separate cache warming/updating process. For simplicity *now*,
        # let's make get_group_symbol_settings_cache *only* check the cache.
        # The DB fallback logic needs to be outside this hot path or in a different service.
        # We'll add a note that cache warming is required.


        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            if message and message['type'] == 'message':
                logger.debug(f"Received message from Redis: {message['data']}")
                try:
                    raw_market_data: Dict[str, Dict[str, float]] = json.loads(message['data'])

                    raw_market_data_decimal: Dict[str, Dict[str, decimal.Decimal]] = {}
                    updated_symbols = []
                    for symbol, prices in raw_market_data.items():
                         symbol_upper = symbol.upper()
                         raw_market_data_decimal[symbol_upper] = {
                            'o': decimal.Decimal(str(prices.get('o'))) if prices.get('o') is not None else None,
                            'b': decimal.Decimal(str(prices.get('b'))) if prices.get('b') is not None else None,
                         }
                         updated_symbols.append(symbol_upper)

                    if not updated_symbols:
                         continue

                    connections_to_remove = []
                    with connections_lock:
                         for websocket, user_connection_info in active_websocket_connections.copy().items():
                             user_id = user_connection_info.get('user_id')

                             if user_id is None:
                                 logger.error(f"User ID missing for WebSocket {websocket.client.host}:{websocket.client.port}. Removing connection.")
                                 connections_to_remove.append(websocket)
                                 continue

                             try:
                                 # --- Fetch User Data (includes group_name) from CACHE ---
                                 user_data = await get_user_data_cache(redis_client, user_id)
                                 if not user_data:
                                      logger.warning(f"User data not found in cache for user ID {user_id}. Skipping update for this user.")
                                      continue

                                 group_name = user_data.get('group_name')
                                 if not group_name:
                                      logger.warning(f"Group name not found in cache for user ID {user_id}. Skipping update.")
                                      continue

                                 # --- Fetch User Portfolio from CACHE ---
                                 user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
                                 user_positions = await get_user_positions_from_cache(redis_client, user_id)

                                 # --- Optimization: Check if user has positions in updated symbols ---
                                 # Determine if a full account metrics recalculation is needed
                                 needs_account_recalculation = False
                                 if user_positions:
                                     position_symbols = {pos.get('symbol').upper() for pos in user_positions if pos.get('symbol')}
                                     if any(symbol in position_symbols for symbol in updated_symbols):
                                         needs_account_recalculation = True
                                         logger.debug(f"User {user_id} has positions in updated symbols ({updated_symbols}). Recalculating account metrics.")

                                 # Decide if you want to send price updates and potentially minimal account data (balance)
                                 # even if no positions. Let's assume yes for now.
                                 if not user_positions and updated_symbols:
                                      needs_account_recalculation = True # Send updates even without positions

                                 # If no relevant update is needed and no positions, skip this user for this tick
                                 if not needs_account_recalculation and not user_positions:
                                      logger.debug(f"No relevant update for user {user_id}. Skipping.")
                                      continue


                                 # --- 1. Calculate Adjusted Market Prices per Symbol for this User's Group ---
                                 adjusted_market_prices: Dict[str, Dict[str, decimal.Decimal]] = {}
                                 # Collect group-symbol settings needed for calculation for updated symbols
                                 current_tick_group_symbol_settings: Dict[str, Dict[str, decimal.Decimal]] = {}

                                 for symbol in updated_symbols:
                                     raw_prices = raw_market_data_decimal.get(symbol)
                                     if raw_prices:
                                         # *** Fetch group-symbol settings from CACHE (or DB fallback - CACHE WARMING NEEDED) ***
                                         symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)

                                         if not symbol_settings:
                                              logger.warning(f"Group-symbol settings not found in cache for group '{group_name}', symbol '{symbol}'. Cannot adjust price.")
                                              # Decide how to handle missing settings: skip symbol, use default spread, etc.
                                              # For now, we'll skip price adjustment but include raw price if needed, or just skip the symbol.
                                              # To keep it simple, if settings are missing, we skip this symbol's adjustment for this user.
                                              continue # Skip price adjustment for this symbol

                                         # Add settings to the collection for account metrics calculation later
                                         current_tick_group_symbol_settings[symbol] = symbol_settings

                                         spread = symbol_settings.get('spread', decimal.Decimal('0.0'))
                                         spread_pip = symbol_settings.get('spread_pip', decimal.Decimal('0.0'))

                                         half_spread = calculate_half_spread(spread, spread_pip)

                                         raw_ask = raw_prices.get('o')
                                         raw_bid = raw_prices.get('b')

                                         if raw_ask is not None and raw_bid is not None:
                                             adjusted_ask = raw_ask + half_spread
                                             adjusted_bid = raw_bid - half_spread
                                             # You might need to quantize based on symbol's tick size
                                             adjusted_market_prices[symbol] = {'o': adjusted_ask, 'b': adjusted_bid}
                                         # If one price is None, handle consistently (e.g., don't include the symbol)


                                 # --- 2. Perform Account Calculations if needed ---
                                 calculated_account_data = {}
                                 # Ensure portfolio data is available if needed for calculation
                                 if needs_account_recalculation and user_portfolio:
                                    calculated_account_data = calculate_account_metrics(
                                        adjusted_market_prices, # Pass adjusted prices for relevant symbols
                                        user_portfolio,
                                        user_data,
                                        current_tick_group_symbol_settings # Pass settings for updated symbols
                                    )
                                 elif not user_portfolio and needs_account_recalculation:
                                      logger.warning(f"User portfolio not found in cache for user ID {user_id} for account calculation.")
                                      # Optionally, send a message to the client indicating data is missing or error


                                 # --- 3. Prepare and Send Personalized Data to User ---
                                 # Only send data if there are adjusted prices OR account data
                                 if adjusted_market_prices or calculated_account_data:
                                      logger.debug(f"Prepared adjusted market prices for user {user_id}: {adjusted_market_prices}")
                                      logger.debug(f"Prepared calculated account data for user {user_id}: {calculated_account_data}")
                                      personalized_data = {
                                          "type": "personalized_update",
                                          "data": {
                                              "market_prices": {
                                                  symbol: {
                                                      'o': str(prices.get('o')) if prices.get('o') is not None else None,
                                                      'b': str(prices.get('b')) if prices.get('b') is not None else None,
                                                  } for symbol, prices in adjusted_market_prices.items()
                                              },
                                              "account_data": {
                                                  key: str(value) for key, value in calculated_account_data.items()
                                              } if calculated_account_data else {}
                                          }
                                      }
                                      logger.debug(f"Sending personalized data to user {user_id}")
                                      
                                      await websocket.send_json(personalized_data)
                                      # logger.debug(f"Sent personalized data to user {user_id}")
                                 # else: logger.debug(f"No data to send for user {user_id} on this tick.")


                             except WebSocketDisconnect:
                                 logger.info(f"WebSocket client disconnected: User ID {user_id} from {websocket.client.host}:{websocket.client.port}")
                                 connections_to_remove.append(websocket)
                             except Exception as e:
                                 logger.error(f"Error processing update or sending data to user ID {user_id} on WebSocket {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
                                 connections_to_remove.append(websocket)


                         # Remove disconnected/errored connections
                         for connection_to_remove in connections_to_remove:
                             if connection_to_remove in active_websocket_connections:
                                 try:
                                     del active_websocket_connections[connection_to_remove]
                                     logger.info(f"WebSocket connection removed. Total active connections: {len(active_websocket_connections)}")
                                 except KeyError:
                                      logger.warning("Attempted to remove WebSocket connection, but it was already gone.")


                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from Redis message: {message['data']}")
                except Exception as e:
                    logger.error(f"Unexpected error processing Redis message: {e}", exc_info=True)

            await asyncio.sleep(0.01)

    except asyncio.CancelledError:
        logger.info("Redis market data broadcaster task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Unexpected error in Redis Pub/Sub listener: {e}", exc_info=True)
        logger.warning("Attempting to reconnect to Redis Pub/Sub in 5 seconds...")
        await asyncio.sleep(5)
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
            logger.info("Successfully re-subscribed to Redis Pub/Sub.")
        except Exception as reconnect_e:
            logger.error(f"Failed to re-subscribe to Redis Pub/Sub: {reconnect_e}", exc_info=True)

    finally:
        logger.info("Cleaning up Redis Pub/Sub connection.")
        if pubsub:
            try:
                await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                logger.info("Redis Pub/Sub unsubscribe completed.")
            except Exception as e:
                logger.error(f"Error during Redis Pub/Sub cleanup: {e}", exc_info=True)


# --- WebSocket Endpoint ---

router = APIRouter(
    tags=["market_data"]
)

# New asynchronous function to fetch and cache group-symbol settings
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    """
    Fetches group-symbol settings from the database and caches them.
    """
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return

    try:
        # Fetch all groups with the user's group_name
        # Assuming Group model represents group-symbol settings with 'symbol' field
        group_settings_list = await crud_group.get_groups(db, search=group_name)

        all_group_settings: Dict[str, Dict[str, Any]] = {}
        for group_setting in group_settings_list:
            symbol = group_setting.symbol
            if symbol:  # Only process if a symbol is defined for this group entry
                # Extract relevant settings from the group object
                # You might need to adjust which attributes are considered settings
                settings = {
                    "commision_type": group_setting.commision_type,
                    "commision_value_type": group_setting.commision_value_type,
                    "type": group_setting.type,
                    "pip_currency": group_setting.pip_currency,
                    "show_points": group_setting.show_points,
                    "swap_buy": decimal.Decimal(str(group_setting.swap_buy)),
                    "swap_sell": decimal.Decimal(str(group_setting.swap_sell)),
                    "commision": decimal.Decimal(str(group_setting.commision)),
                    "margin": decimal.Decimal(str(group_setting.margin)),
                    "spread": decimal.Decimal(str(group_setting.spread)),
                    "deviation": decimal.Decimal(str(group_setting.deviation)),
                    "min_lot": decimal.Decimal(str(group_setting.min_lot)),
                    "max_lot": decimal.Decimal(str(group_setting.max_lot)),
                    "pips": decimal.Decimal(str(group_setting.pips)),
                    "spread_pip": decimal.Decimal(str(group_setting.spread_pip)),
                    # Add other symbol-specific settings from the Group model here
                }
                all_group_settings[symbol] = settings

        # Now iterate through the populated dictionary and cache
        for symbol, settings in all_group_settings.items():
            await set_group_symbol_settings_cache(redis_client, group_name, symbol, settings)
        logger.debug(f"Initially cached {len(all_group_settings)} group-symbol settings for group '{group_name}'.")

    except Exception as e:
        logger.error(f"Error fetching or caching initial group-symbol settings for group '{group_name}': {e}", exc_info=True)


@router.websocket("/ws/market-data")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="Authentication token"),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    WebSocket endpoint for clients to connect and receive real-time market data.
    Authenticates the user, fetches necessary user/group data and portfolio,
    caches it in Redis, and handles connections/disconnections.
    """
    client_host, client_port = websocket.client.host, websocket.client.port
    logger.info(f"Attempting to accept WebSocket connection from {client_host}:{client_port} with token...")
    user = None
    user_id = None
    group_name = None # Initialize group_name

    try:
        # --- Authentication ---
        try:
             payload = decode_token(token)
             user_id_from_token = payload.get("sub")
             if user_id_from_token:
                  user_id = int(user_id_from_token)
                  # Fetch user from DB - needed here to get group_name, leverage etc.
                  # Cache this user object or essential parts on login/auth if possible.
                  user = await crud_user.get_user_by_id(db, user_id=user_id)

             if not user or user.isActive != 1:
                 logger.warning(f"WebSocket authentication failed for token (user_id: {user_id}). User not found or inactive.")
                 raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed or user inactive")

        except Exception as e:
            logger.error(f"Error during WebSocket authentication for token: {token[:20]}... : {e}", exc_info=True)
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
        # --- End Authentication ---

        # --- Fetch User Essentials (including group_name) & Cache in Redis ---
        group_name = getattr(user, 'group_name', None)
        if not group_name:
             logger.warning(f"User ID {user_id} has no group_name assigned.")
             # Decide how to handle users without a group - maybe use a default group?
             # For now, proceed, but they won't get group-specific settings.

        user_data_to_cache = {
            "user_id": user.id,
            "group_name": group_name, # Store the group name
            "leverage": decimal.Decimal(str(getattr(user, 'leverage', '1.0'))),
            # Add other static user data here if needed
        }
        await set_user_data_cache(redis_client, user.id, user_data_to_cache)
        logger.debug(f"Cached user data (including group_name) for user ID {user.id}")

        # --- Fetch and Cache Initial User Portfolio ---
        try:
            # *** You need to implement these CRUD functions ***
            # user_balance = await crud_user_portfolio.get_user_balance(db, user_id)
            # user_open_positions = await crud_user_positions.get_user_open_positions(db, user_id)

            initial_portfolio_data = {
                "balance": decimal.Decimal(str(getattr(user, 'wallet_balance', '0.0'))), # Example: Using user model's wallet_balance
                "positions": [
                    # Populate with actual open positions fetched from DB
                    # Example placeholder:
                    # {"symbol": "AUDCAD", "direction": "buy", "volume": decimal.Decimal('0.1'), "open_price": decimal.Decimal('0.6490')},
                ],
                # Add other initial portfolio data needed
            }
            await set_user_portfolio_cache(redis_client, user.id, initial_portfolio_data)
            logger.debug(f"Cached initial user portfolio for user ID {user.id}")

        except Exception as e:
            logger.error(f"Error fetching or caching initial user portfolio for user ID {user.id}: {e}", exc_info=True)
            # Continue connection but log the error

        # --- Cache Group-Symbol Settings (Initial Load) ---
        # Call the new function to fetch and cache group-symbol settings
        await update_group_symbol_settings(group_name, db, redis_client)

        # --- Cache Group-Symbol Settings (Needs Careful Implementation) ---
        # This is where you would ideally fetch ALL relevant group-symbol settings
        # for this user's group and populate the cache (group_settings:group_name:symbol).
        # This could be done:
        # 1. On the first connection of a user from a specific group (check cache first).
        # 2. Periodically by a separate background task that keeps the group settings cache warm.
        # 3. Just-in-time in the broadcaster (as implemented above, with DB fallback if necessary - NOT IDEAL).
        # Option 2 (Cache Warming Task) is recommended for production scalability.

        # For now, the broadcaster implements a *basic* just-in-time fetch from cache.
        # The DB fallback logic *within the broadcaster* is commented out as it adds
        # DB query latency to the critical path per tick, per user.
        # You *must* implement cache warming/updating for group-symbol settings.


        await websocket.accept()
        logger.info(f"WebSocket connection accepted and authenticated for user ID {user.id} (Group: {group_name}) from {client_host}:{client_port}.")

        # Add the new connection with minimal user info to the list
        with connections_lock:
            active_websocket_connections[websocket] = {"user_id": user.id}
            logger.info(f"New authenticated WebSocket connection added for user {user.id}. Total active connections: {len(active_websocket_connections)}")

        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)

            except asyncio.TimeoutError:
                pass # Keep-alive
            except WebSocketDisconnect as e:
                logger.info(f"WebSocket client disconnected gracefully (User ID: {user_id}): {client_host}:{client_port}. Code: {e.code}, Reason: {e.reason}")
                break

            except Exception as e:
                logger.error(f"Error receiving message from WebSocket client (User ID: {user_id}) {client_host}:{client_port}: {e}", exc_info=True)
                try:
                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                    logger.info(f"Closed WebSocket connection due to error for user ID {user_id}.")
                except Exception as close_e:
                    logger.error(f"Error during WebSocket close after receive error for user ID {user_id}: {close_e}")
                break

    except WebSocketException as e:
         user_id_for_log = user_id if user_id else 'N/A (auth failed)'
         logger.warning(f"WebSocket connection rejected or closed due to exception: {e.reason} (Code: {e.code}) for user ID {user_id_for_log} from {client_host}:{client_port}")
         pass

    except Exception as e:
        user_id_for_log = user_id if user_id else 'N/A (setup error)'
        logger.error(f"Error accepting or setting up WebSocket connection for user ID {user_id_for_log} from {client_host}:{client_port}: {e}", exc_info=True)

    finally:
        user_id_for_log = user_id if user_id else 'N/A (cleanup)'
        logger.info(f"Cleaning up WebSocket connection for user ID {user_id_for_log} from {client_host}:{client_port}.")
        with connections_lock:
            if websocket in active_websocket_connections:
                try:
                    del active_websocket_connections[websocket]
                    logger.info(f"WebSocket connection removed from active list. Total active connections: {len(active_websocket_connections)}")
                except KeyError:
                     logger.warning(f"Attempted to remove WebSocket connection for user ID {user_id_for_log} from list, but it was already gone.")

        logger.info(f"WebSocket endpoint finally block finished for user ID {user_id_for_log}.")

# --- Background Task to Publish Data to Redis ---
async def redis_publisher_task(redis_client: Redis):
    """
    Background task that reads market data from the shared queue
    and publishes it to a Redis Pub/Sub channel.
    Accepts the initialized redis_client instance.
    """
    logger.info(f"Redis publisher task started. Publishing to channel '{REDIS_MARKET_DATA_CHANNEL}'.")

    if not redis_client:
        logger.critical("Redis client not provided to publisher task. Cannot publish messages.")
        while True: await asyncio.sleep(60)
        return

    try:
        while True:
            raw_market_data_message = await redis_publish_queue.get()

            if raw_market_data_message is None:
                logger.info("Publisher task received shutdown signal (None). Exiting.")
                break

            try:
                # Ensure the message is a JSON string before publishing
                if not isinstance(raw_market_data_message, str):
                    message_to_publish = json.dumps(raw_market_data_message)
                else:
                     message_to_publish = raw_market_data_message
            except Exception as e:
                logger.error(f"Publisher failed to serialize message to JSON: {e}. Skipping message.", exc_info=True)
                redis_publish_queue.task_done()
                continue

            try:
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
            except Exception as e:
                logger.error(f"Publisher failed to publish message to Redis: {e}. Message: {message_to_publish[:100]}...", exc_info=True)

            redis_publish_queue.task_done()

    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)

    finally:
        logger.info("Redis publisher task finished.")

# ... (rest of your market_data_ws.py file, including the router definition) ...