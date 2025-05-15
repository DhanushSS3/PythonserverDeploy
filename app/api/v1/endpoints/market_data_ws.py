# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
import json
import threading
from typing import Dict, Any, List, Optional
import decimal # Import decimal for Decimal type and encoder
# Import WebSocketState for checking WebSocket client state
from starlette.websockets import WebSocketState


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db # Assuming get_db is in app.database.session
from app.crud import user as crud_user # Assuming user CRUD is in app.crud.user
from app.crud import group as crud_group # Assuming group CRUD is in app.crud.group
# Import the updated crud_order with the new function
from app.crud import crud_order


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
    get_user_positions_from_cache, # Keep this if still needed elsewhere, but broadcaster will fetch all
    set_adjusted_market_price_cache,
    get_adjusted_market_price_cache, # Import the function to get cached adjusted prices
    set_group_symbol_settings_cache,
    get_group_symbol_settings_cache,
    DecimalEncoder, # Import DecimalEncoder
    decode_decimal # Import decode_decimal
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue
from app.shared_state import redis_publish_queue # This queue is consumed by the publisher

# Import the new portfolio calculation service
from app.services.portfolio_calculator import calculate_user_portfolio

# Configure logging for this module
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG) # Level set in main.py for granular control


# Dictionary to store active WebSocket connections
# Key: user_id, Value: {'websocket': WebSocket, 'group_name': str, 'db_session': AsyncSession}
# Need a lock as this dictionary is accessed from the WebSocket endpoint (main thread)
# and the broadcaster task (main thread)
active_websocket_connections: Dict[int, Dict[str, Any]] = {}
active_connections_lock = threading.Lock() # Use a threading Lock for thread-safe access

# Redis channel for market data updates
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'


router = APIRouter(
    tags=["market_data_ws"]
)

# --- WebSocket Endpoint for Market Data ---
@router.websocket("/ws/market-data")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...), # Expect token as query parameter
    db: AsyncSession = Depends(get_db), # Inject DB session
    redis_client: Redis = Depends(get_redis_client) # Inject Redis client
):
    """
    WebSocket endpoint to stream market data and personalized account updates
    to authenticated users. Requires a valid JWT token.
    """
    logger.info(f"Attempting to accept WebSocket connection from {websocket.client.host}:{websocket.client.port} with token...")

    user_id: Optional[int] = None
    group_name: Optional[str] = None

    # --- Authentication ---
    try:
        from jose import JWTError # Import here to avoid circular dependency if security imports this file
        payload = decode_token(token) # Assuming decode_token handles signature verification
        user_id = int(payload.get("sub")) # 'sub' is the standard claim for subject (user ID)

        if user_id is None:
            logger.warning(f"Authentication failed for connection from {websocket.client.host}:{websocket.client.port}: User ID not found in token.")
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: User ID missing")

        # Verify user exists in DB and is active
        db_user = await crud_user.get_user_by_id(db, user_id)
        if db_user is None or not getattr(db_user, 'isActive', True): # Check if user exists and is active (assuming 'isActive' attribute)
             logger.warning(f"Authentication failed for user ID {user_id} from {websocket.client.host}:{websocket.client.port}: User not found or inactive.")
             raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User not found or inactive")

        # Get user's group name
        group_name = getattr(db_user, 'group_name', 'default') # Use default group if not specified

        # Cache user data (including group_name, balance, leverage)
        user_data_to_cache = {
            "id": user_id,
            "group_name": group_name,
            # Ensure these are Decimal types for consistent calculations later
            "leverage": decimal.Decimal(str(getattr(db_user, 'leverage', 1.0))),
            "wallet_balance": decimal.Decimal(str(getattr(db_user, 'wallet_balance', 0.0)))
            # Add other user attributes needed for market data/account calculation
        }
        await set_user_data_cache(redis_client, user_id, user_data_to_cache)
        logger.debug(f"Cached user data (including group_name, balance, leverage) for user ID {user_id}")

        # Fetch and cache initial user portfolio (specifically open positions)
        # Use the new CRUD function to get all open orders for this user
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id)

        # Convert ORM objects to dictionaries for caching
        # Ensure Decimal values are handled during conversion
        initial_positions_data = []
        for pos in open_positions_orm:
             pos_dict = {}
             # Manually copy relevant attributes, converting Decimal to str for JSON safety in cache
             for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
                  value = getattr(pos, attr, None)
                  if isinstance(value, decimal.Decimal):
                       pos_dict[attr] = str(value) # Store Decimal as string in cache
                  else:
                       pos_dict[attr] = value
             # Add a placeholder for profit_loss, which will be calculated dynamically
             pos_dict['profit_loss'] = "0.0" # Store as string "0.0" initially
             initial_positions_data.append(pos_dict)


        # Initial portfolio data structure (balance and positions are fetched/cached)
        # Equity, margin, free_margin, profit_loss will be CALCULATED dynamically
        user_portfolio_data = {
             # Balance is part of user_data cache now, but include here for the portfolio structure
             "balance": str(user_data_to_cache["wallet_balance"]), # Store balance as string in portfolio cache
             "equity": "0.0", # Placeholder - will be calculated
             "margin": "0.0", # Placeholder - will be calculated (total used margin)
             "free_margin": "0.0", # Placeholder - will be calculated
             "profit_loss": "0.0", # Placeholder - will be calculated (total PnL)
             "positions": initial_positions_data # List of open positions (from DB)
        }
        # Cache the initial portfolio data
        await set_user_portfolio_cache(redis_client, user_id, user_portfolio_data)
        logger.debug(f"Cached initial user portfolio (with {len(initial_positions_data)} open positions) for user ID {user_id}")

        # Cache Group-Symbol Settings (Initial Load)
        # Fetch and cache all settings for the user's group
        await update_group_symbol_settings(group_name, db, redis_client)
        logger.debug(f"Initially cached group-symbol settings for group '{group_name}'.")


    except JWTError:
        logger.warning(f"Authentication failed for connection from {websocket.client.host}:{websocket.client.port}: Invalid token.")
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid authentication token")
    except WebSocketException:
         # Re-raise existing WebSocket exceptions (e.g., invalid token, user not found)
         raise
    except Exception as e:
        # Catch any other unexpected authentication errors
        logger.error(f"Unexpected authentication error for connection from {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        raise WebSocketException(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication failed due to internal error")


    await websocket.accept()
    logger.info(f"WebSocket connection accepted and authenticated for user ID {user_id} (Group: {group_name}).")

    # Add the new connection to the active connections dictionary (thread-safe)
    with active_connections_lock:
        active_websocket_connections[user_id] = {
            'websocket': websocket,
            'group_name': group_name,
            'user_id': user_id,
            # Note: We are NOT storing the db session here.
            # DB access in the broadcaster task will need a new session instance per tick if needed,
            # but we aim to rely on cached data for performance.
        }
        logger.info(f"New authenticated WebSocket connection added for user {user_id}. Total active connections: {len(active_websocket_connections)}")


    # --- WebSocket Communication Loop ---
    try:
        # This loop keeps the connection open and listens for messages from the client
        while True:
            try:
                # Use a small timeout to allow the task to be cancelled gracefully if needed
                # Client messages are not expected for market data stream currently
                # This receive is mainly to detect disconnects or potential control messages later
                message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0) # Increased timeout slightly
                logger.debug(f"Received message from user {user_id}: {message}")
                # Process client message if necessary (e.g., parse JSON command)

            except asyncio.TimeoutError:
                 # Handle timeout if receive has a timeout - simply continue the loop
                 pass

            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected by client {user_id}.")
                break # Exit the loop on disconnect
            except Exception as e:
                 logger.error(f"Error receiving message from user {user_id}: {e}", exc_info=True)
                 break # Exit the loop on other errors

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected by client {user_id} outside of receive loop.")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket connection for user {user_id}: {e}", exc_info=True)

    finally:
        # Clean up the connection when the loop breaks or an error occurs
        with active_connections_lock:
            if user_id in active_websocket_connections:
                del active_websocket_connections[user_id]
                logger.info(f"WebSocket connection closed for user {user_id}. Total active connections: {len(active_websocket_connections)}")
        # Close the WebSocket gracefully, if not already closed
        try:
             if websocket.client_state != WebSocketState.DISCONNECTED:
                 await websocket.close() # Ensure the WebSocket is closed gracefully
             else:
                 logger.debug(f"WebSocket for user {user_id} already disconnected, no need to close.")
        except Exception as close_e:
             logger.error(f"Error during WebSocket close for user {user_id}: {close_e}", exc_info=True)


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
# This helper fetches group settings from DB and caches them in Redis.
# It's defined here for now, called from websocket_endpoint.
# It requires database access (AsyncSession) and Redis client (Redis).
# Ensure crud_group and caching functions are imported at the top.

async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    """
    Fetches group-symbol settings from the database and caches them in Redis.
    Called during WebSocket connection setup and potentially on admin updates.
    """
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return

    try:
        # Assuming crud_group.get_groups can filter by name and return list of Group models
        # Also assuming Group model has a 'symbol' attribute and other setting attributes
        group_settings_list = await crud_group.get_groups(db, search=group_name)

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return

        # Cache settings per symbol for this group
        for group_setting in group_settings_list:
            symbol = getattr(group_setting, 'symbol', None)
            if symbol:
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None),
                    "pip_currency": getattr(group_setting, 'pip_currency', None),
                    "show_points": getattr(group_setting, 'show_points', None),
                    "swap_buy": getattr(group_setting, 'swap_buy', decimal.Decimal(0.0)),
                    "swap_sell": getattr(group_setting, 'swap_sell', decimal.Decimal(0.0)),
                    "commision": getattr(group_setting, 'commision', decimal.Decimal(0.0)),
                    "margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)), # Base margin for calculation
                    "spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)),
                    "deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)),
                    "min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)),
                    "max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)),
                    "pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)), # Pips value
                    "spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)), # Spread pip value
                    # Assuming contract_size is also part of group settings per symbol
                    "contract_size": getattr(group_setting, 'contract_size', decimal.Decimal("100000")), # Default standard lot size
                }
                await set_group_symbol_settings_cache(redis_client, group_name, symbol.upper(), settings)

        logger.debug(f"Initially cached group-symbol settings for group '{group_name}'.")

    except Exception as e:
        logger.error(f"Error fetching or caching initial group-symbol settings for group '{group_name}': {e}", exc_info=True)


# --- Redis Publisher Task (No changes needed here) ---
async def redis_publisher_task(redis_client: Redis):
    """
    Asynchronously consumes market data from the shared queue and publishes it to Redis Pub/Sub.
    Runs as an asyncio background task.
    Filters out the _timestamp key.
    """
    logger.info("Redis publisher task started. Publishing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)

    if not redis_client:
        logger.critical("Redis client not provided for publisher task. Exiting.")
        return

    try:
        while True:
            raw_market_data_message = await redis_publish_queue.get()

            if raw_market_data_message is None:
                logger.info("Publisher task received shutdown signal (None). Exiting.")
                break

            try:
                message_to_publish_data = {k: v for k, v in raw_market_data_message.items() if k != '_timestamp'}

                if message_to_publish_data:
                     # Use DecimalEncoder to safely serialize Decimal values to strings
                     message_to_publish = json.dumps(message_to_publish_data, cls=DecimalEncoder)
                else:
                     logger.debug("Publisher: Message contained only _timestamp, skipping publishing.")
                     redis_publish_queue.task_done()
                     continue

            except Exception as e:
                logger.error(f"Publisher failed to serialize message to JSON: {e}. Skipping message.", exc_info=True)
                redis_publish_queue.task_done()
                continue

            try:
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
            except Exception as e:
                logger.error(f"Publisher failed to publish message to Redis: {e}. Message: {message_to_publish[:100]}... Skipping.", exc_info=True)

            redis_publish_queue.task_done()

    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)

    finally:
        logger.info("Redis publisher task finished.")


# --- Redis Market Data Broadcaster Task (Modified) ---
async def redis_market_data_broadcaster(redis_client: Redis):
    """
    Subscribes to Redis Pub/Sub for market data and broadcasts personalized data
    and calculated account updates to connected WebSocket clients.
    Retrieves full set of relevant symbols from cache, applies latest updates,
    and uses cached user data/portfolio/group settings for calculations.
    """
    logger.info("Redis market data broadcaster task started. Subscribing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)

    if not redis_client:
        logger.critical("Redis client not provided for broadcaster task. Exiting.")
        return

    try:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
        logger.info("Successfully subscribed to Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)

        async for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            try:
                # Decode the market data message from Redis (handles Decimal strings)
                # This update contains ONLY the symbols whose prices changed in Firebase
                market_data_update = json.loads(message['data'], object_hook=decode_decimal)
                # market_data_update is now a dictionary like {'SYMBOL': {'o': Decimal, 'b': Decimal}, ...}
                logger.debug(f"Broadcaster received and decoded market data update. Symbols updated: {list(market_data_update.keys())}")

            except Exception as e:
                logger.error(f"Broadcaster failed to decode market data message: {e}. Skipping message.", exc_info=True)
                continue

            # Get a snapshot of currently active connections to iterate over
            connections_snapshot = list(active_websocket_connections.items())
            logger.debug(f"Broadcaster processing message for {len(connections_snapshot)} active connections.")


            # Iterate through all active connections and send personalized data
            for user_id, websocket_info in connections_snapshot:
                try:
                    websocket = websocket_info['websocket']
                    group_name = websocket_info['group_name']

                    # Retrieve cached user data, portfolio, and group settings
                    user_data = await get_user_data_cache(redis_client, user_id)
                    user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
                    # Fetch ALL group settings for the user's group from cache
                    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

                    if not user_data:
                         logger.warning(f"User data not found in cache for user {user_id}. Skipping market data update for this user.")
                         continue # Skip this user if data is not cached

                    # Identify relevant symbols for this user's group
                    # Assuming group_settings is a dict like {SYMBOL: {settings}, ...}
                    relevant_symbols = set(group_settings.keys()) if isinstance(group_settings, dict) else set()

                    if not relevant_symbols:
                        logger.debug(f"No relevant symbols found for group '{group_name}' for user {user_id}. Skipping market data send.")
                        # Still attempt to send account data if available? Maybe send a message
                        # indicating no symbols are configured for their group.
                        # For now, let's skip sending anything if no symbols.
                        continue

                    # Dictionary to hold the market data payload for this specific user
                    # This will contain the latest price for each relevant symbol
                    user_market_data_payload: Dict[str, Dict[str, float]] = {}

                    # --- Populate with Latest Prices (Updated or Cached) ---

                    # First, process symbols from the latest update (these take precedence)
                    symbols_processed_from_update = set()
                    if isinstance(market_data_update, dict):
                        for symbol, prices in market_data_update.items():
                            symbol_upper = symbol.upper()
                            symbols_processed_from_update.add(symbol_upper) # Track symbols processed from update

                            # Only process updated symbols that are relevant to this user's group
                            if symbol_upper in relevant_symbols and isinstance(prices, dict):
                                raw_ask_price = prices.get('o') # 'o' for Ask
                                raw_bid_price = prices.get('b') # 'b' for Bid

                                symbol_settings = group_settings.get(symbol_upper) # Get settings for this symbol

                                if raw_ask_price is not None and raw_bid_price is not None and symbol_settings:
                                    try:
                                        # Ensure Decimal types for calculations
                                        spread_setting = decimal.Decimal(str(symbol_settings.get('spread', 0)))
                                        spread_pip_setting = decimal.Decimal(str(symbol_settings.get('spread_pip', 0)))
                                        ask_decimal = decimal.Decimal(str(raw_ask_price))
                                        bid_decimal = decimal.Decimal(str(raw_bid_price))

                                        # Apply the spread formula
                                        spread_value = spread_setting * spread_pip_setting
                                        half_spread = spread_value / decimal.Decimal(2)

                                        # Calculate adjusted prices
                                        adjusted_buy_price = ask_decimal + half_spread
                                        adjusted_sell_price = bid_decimal - half_spread

                                        # Add to user's payload (as float for JSON)
                                        user_market_data_payload[symbol_upper] = {
                                            'buy': float(adjusted_buy_price),
                                            'sell': float(adjusted_sell_price),
                                            'spread_value': float(spread_value),
                                        }

                                        # Cache the newly calculated adjusted prices for this group and symbol
                                        # This ensures the latest price is available for subsequent cycles
                                        await set_adjusted_market_price_cache(
                                            redis_client=redis_client,
                                            group_name=group_name,
                                            symbol=symbol_upper,
                                            buy_price=adjusted_buy_price,
                                            sell_price=adjusted_sell_price,
                                            spread_value=spread_value
                                        )
                                        logger.debug(f"Cached and added newly adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from update)")

                                    except Exception as calc_e:
                                        logger.error(f"Error calculating/caching adjusted prices for user {user_id}, group '{group_name}', symbol {symbol}: {calc_e}", exc_info=True)
                                        # If calculation fails, try to use raw prices as fallback for the payload
                                        if raw_ask_price is not None and raw_bid_price is not None:
                                             user_market_data_payload[symbol_upper] = {
                                                'buy': float(raw_ask_price),
                                                'sell': float(raw_bid_price),
                                                # spread_value is not calculated on fallback
                                             }
                                             logger.warning(f"Falling back to raw prices (buy/sell) for user {user_id}, symbol {symbol_upper} due to calculation error.")
                                        else:
                                             logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Cannot send price update.")

                                elif raw_ask_price is not None and raw_bid_price is not None:
                                     # If no group settings found for this symbol, send raw prices as a fallback
                                     user_market_data_payload[symbol_upper] = {
                                         'buy': float(raw_ask_price),
                                         'sell': float(raw_bid_price),
                                         # spread_value is not calculated on fallback
                                     }
                                     logger.warning(f"No group settings found for user {user_id}, group '{group_name}', symbol {symbol_upper}. Sending raw prices (buy/sell) for updated symbol.")
                                else:
                                     logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Skipping price update.")


                    # Second, populate with cached prices for relevant symbols NOT in the latest update
                    for symbol_upper in relevant_symbols:
                        # If the symbol was not in the latest update and is relevant to the user
                        if symbol_upper not in symbols_processed_from_update:
                             cached_adjusted_price = await get_adjusted_market_price_cache(redis_client, group_name, symbol_upper)
                             if cached_adjusted_price:
                                 # Add cached prices to user's payload (they are already Decimals from cache, convert to float for JSON)
                                 user_market_data_payload[symbol_upper] = {
                                     'buy': float(cached_adjusted_price.get('buy', 0.0)),
                                     'sell': float(cached_adjusted_price.get('sell', 0.0)),
                                     'spread_value': float(cached_adjusted_price.get('spread_value', 0.0)),
                                 }
                                 logger.debug(f"Added cached adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from cache)")
                             # Else: If no cached price and not in update, the symbol won't be in the payload for this cycle.

                    # --- Calculate Dynamic Account Data ---
                    # Use the portfolio_calculator service to calculate dynamic metrics
                    open_positions_from_cache = user_portfolio.get('positions', []) if isinstance(user_portfolio, dict) else []

                    # Pass the complete set of market data prices (updated + cached) for calculation
                    calculated_account_data = await calculate_user_portfolio(
                        user_data=user_data, # Contains balance, leverage
                        open_positions=open_positions_from_cache, # Contains entry_price, quantity, type etc.
                        adjusted_market_prices=user_market_data_payload, # Use the combined updated/cached prices
                        group_symbol_settings=group_settings # Contains margin, pips, contract_size etc.
                    )

                    # --- Prepare and Send Data to WebSocket ---
                    # Only send if there is market data or calculated account data
                    if user_market_data_payload or calculated_account_data:
                        payload = {
                            "type": "market_data_update",
                            "market_data": user_market_data_payload, # Sending the complete set of relevant market data
                            "account_data": calculated_account_data # Sending calculated account data (personal details)
                        }

                        # Check if the websocket is still connected before sending
                        if websocket.client_state == WebSocketState.CONNECTED:
                            try:
                                # Use send_text after manual JSON dumping with DecimalEncoder
                                await websocket.send_text(json.dumps(payload, cls=DecimalEncoder))
                                logger.debug(f"Sent personalized data to user {user_id}. Symbols sent: {list(user_market_data_payload.keys())}")
                            except Exception as send_e:
                                # Log sending errors but don't necessarily disconnect immediately
                                logger.error(f"Error sending data to user {user_id}: {send_e}", exc_info=True)
                        else:
                            logger.warning(f"WebSocket for user {user_id} is not connected. Skipping send.")

                    else:
                         logger.debug(f"No relevant market prices or account data to send for user {user_id} on this tick.")


                except WebSocketDisconnect:
                    logger.info(f"User {user_id} disconnected.")
                    # Clean up connection: remove from active_websocket_connections (thread-safe)
                    with active_connections_lock:
                        if user_id in active_websocket_connections:
                            del active_websocket_connections[user_id]
                            logger.info(f"User {user_id} connection removed. Total active connections: {len(active_websocket_connections)}")
                except Exception as e:
                    # This catches any other exception during processing *for a single client*
                    logger.error(f"Error processing or sending data to user {user_id} in broadcaster loop: {e}", exc_info=True)
                    # Consider closing the websocket or marking it for removal on error
                    try:
                        # Attempt to close the websocket gracefully if it's still open
                        if websocket.client_state == WebSocketState.CONNECTED:
                             await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                             logger.info(f"Closed websocket for user {user_id} after processing error.")
                    except Exception as close_e:
                         logger.error(f"Error during WebSocket close for user {user_id} after processing error: {close_e}", exc_info=True)
                    # Ensure connection is removed from active connections (thread-safe)
                    with active_connections_lock:
                         if user_id in active_websocket_connections:
                            del active_websocket_connections[user_id]
                            logger.info(f"User {user_id} connection removed after processing error. Total active connections: {len(active_websocket_connections)}")


    except asyncio.CancelledError:
        logger.info("Redis market data broadcaster task cancelled.")
    except Exception as e:
        # This catches errors from pubsub.listen() or unexpected issues in the outer async for loop
        logger.critical(f"FATAL ERROR: Redis market data broadcaster task failed: {e}", exc_info=True)

    finally:
        # Cleanup when the broadcaster task finishes
        logger.info("Redis market data broadcaster task finished.")
        # Unsubscribe from the channel on task finish
        try:
             if redis_client:
                if 'pubsub' in locals() and pubsub:
                    await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                    logger.info("Successfully unsubscribed from Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
                else:
                     logger.warning("Pubsub object not available during unsubscribe attempt.")
        except Exception as unsub_e:
             logger.error(f"Error during Redis unsubscribe: {unsub_e}", exc_info=True)
        # Close the pubsub connection
        try:
            if 'pubsub' in locals() and pubsub:
                 await pubsub.close()
                 logger.info("Redis pubsub connection closed.")
            else:
                 logger.warning("Pubsub object not available during close attempt.")
        except Exception as close_e:
             logger.error(f"Error during Redis pubsub close: {close_e}", exc_info=True)