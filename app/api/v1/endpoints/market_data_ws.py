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
    get_group_symbol_settings_cache,  # New import
    DecimalEncoder, # Import DecimalEncoder
    decode_decimal # Import decode_decimal
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue
from app.shared_state import redis_publish_queue # This queue is consumed by the publisher

# Configure logging for this module
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG) # Level set in main.py for granular control


# Dictionary to store active WebSocket connections
# Key: user_id, Value: {'websocket': WebSocket, 'group_name': str}
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
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client) # Inject Redis client
):
    """
    WebSocket endpoint to stream market data to authenticated users.
    Requires a valid JWT token as a query parameter for authentication.
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

        # Optionally verify user exists in DB and is active
        # Assuming crud_user has a get_user_by_id function that takes db session and user_id
        # Ensure crud_user is imported at the top
        db_user = await crud_user.get_user_by_id(db, user_id)
        if db_user is None or not getattr(db_user, 'isActive', True): # Check if user exists and is active (assuming 'isActive' attribute)
             logger.warning(f"Authentication failed for user ID {user_id} from {websocket.client.host}:{websocket.client.port}: User not found or inactive.")
             raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User not found or inactive")

        # Get user's group name (assuming user model has group_name attribute)
        group_name = getattr(db_user, 'group_name', 'default') # Use default group if not specified

        # Cache user data (including group_name) and initial portfolio
        # Assuming user model has attributes like leverage, margin, wallet_balance
        user_data_to_cache = {
            "id": user_id,
            "group_name": group_name,
            "leverage": float(getattr(db_user, 'leverage', 1)), # Cache leverage as float
            "margin": float(getattr(db_user, 'margin', 0)), # Cache margin as float
            # Add other user attributes needed for market data/account calculation
            "wallet_balance": float(getattr(db_user, 'wallet_balance', 0)) # Cache balance as float
        }
        await set_user_data_cache(redis_client, user_id, user_data_to_cache)
        logger.debug(f"Cached user data (including group_name) for user ID {user_id}")

        # Fetch and cache initial user portfolio (positions, etc.)
        # Assuming crud_user has a get_user_portfolio function or similar
        # For this example, I'll assume positions are fetched and part of the initial portfolio cache
        user_positions = [] # REPLACE THIS WITH ACTUAL DB FETCH FOR USER'S OPEN POSITIONS

        user_portfolio_data = {
             "balance": float(getattr(db_user, 'wallet_balance', 0)), # Use cached balance from user_data or refetch
             "equity": float(getattr(db_user, 'wallet_balance', 0)), # Placeholder: Equity usually includes PnL
             "margin": 0.0, # Placeholder: Calculate margin based on positions
             "free_margin": float(getattr(db_user, 'wallet_balance', 0)), # Placeholder: Free margin = Equity - Used Margin
             "profit_loss": 0.0, # Placeholder: Calculate PnL from positions and current market data
             "positions": user_positions # Include fetched positions here
        }
        await set_user_portfolio_cache(redis_client, user_id, user_portfolio_data)
        logger.debug(f"Cached initial user portfolio for user ID {user_id}")

        # Cache Group-Symbol Settings (Initial Load)
        # Fetch and cache all settings for the user's group
        # Assuming crud_group has a get_groups function that can filter by name or return all
        # The helper function update_group_symbol_settings is defined below or imported
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
    logger.info(f"WebSocket connection accepted and authenticated for user ID {user_id} (Group: {group_name}) from 127.0.0.1:{websocket.client.port}.") # Updated log to include port

    # Add the new connection to the active connections dictionary (thread-safe)
    with active_connections_lock:
        active_websocket_connections[user_id] = {
            'websocket': websocket,
            'group_name': group_name,
            # Store other necessary user info if needed by the broadcaster
            'user_id': user_id # Store user_id here for easy access in broadcaster loop
        }
        logger.info(f"New authenticated WebSocket connection added for user {user_id}. Total active connections: {len(active_websocket_connections)}")


    # --- WebSocket Communication Loop ---
    try:
        # This loop keeps the connection open and listens for messages from the client
        # You might handle control messages (e.g., subscribe/unsubscribe to specific symbols) here
        # For market data streaming, the server primarily sends data, so this loop might be simple.
        while True:
            # Listen for incoming messages from the client (e.g., control signals)
            # If you don't expect messages from the client, you could perhaps remove this or add a timeout
            # Use a small timeout to allow the task to be cancelled gracefully if needed
            try:
                # Adjusted timeout based on common market data update frequency
                message = await asyncio.wait_for(websocket.receive_text(), timeout=0.1) # Added timeout
                logger.debug(f"Received message from user {user_id}: {message}")
                # Process client message if necessary (e.g., parse JSON command)
                # Example: if message is a JSON string like {"command": "subscribe", "symbol": "EURUSD"}
                # You would update the user's subscription list here if implementing symbol filtering per user
                # For now, we assume all connected users get all published data (filtered by group settings)

            except asyncio.TimeoutError:
                 # Handle timeout if receive has a timeout
                 pass # Continue waiting for messages after timeout

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
                # Fix: Call len() on the dictionary, not the lock
                logger.info(f"WebSocket connection closed for user {user_id}. Total active connections: {len(active_websocket_connections)}")
        # Close the WebSocket gracefully, if not already closed
        try:
             # Fix: Check client_state against the WebSocketState enum
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

# Assuming crud_group.get_groups exists and returns models with necessary attributes
# and assuming set_group_symbol_settings_cache exists in app.core.cache
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    """
    Fetches group-symbol settings from the database and caches them in Redis.
    Called during WebSocket connection setup and potentially on admin updates.
    """
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return

    try:
        # Fetch group settings from the database based on group_name
        # Assuming crud_group.get_groups takes a database session and a search parameter (like name)
        # It should return a list of group setting objects/models
        # Adjust this call based on your actual crud_group implementation
        group_settings_list = await crud_group.get_groups(db, search=group_name)

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return

        # Prepare a dictionary of all settings for this group, keyed by symbol
        # Ensure that the attributes accessed here ('symbol', 'spread', etc.) match your Group model
        all_group_settings: Dict[str, Dict[str, Any]] = {}
        for group_setting in group_settings_list:
            # Safely get the symbol, ensuring it's not None
            symbol = getattr(group_setting, 'symbol', None)
            if symbol:
                # Extract relevant settings for the symbol from the group setting object/model
                # Use getattr with a default value (like Decimal(0.0) or None) for safety
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None),
                    "pip_currency": getattr(group_setting, 'pip_currency', None),
                    "show_points": getattr(group_setting, 'show_points', None),
                    "swap_buy": getattr(group_setting, 'swap_buy', decimal.Decimal(0.0)), # Keep as Decimal
                    "swap_sell": getattr(group_setting, 'swap_sell', decimal.Decimal(0.0)), # Keep as Decimal
                    "commision": getattr(group_setting, 'commision', decimal.Decimal(0.0)), # Keep as Decimal
                    "margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)), # Keep as Decimal
                    "spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)), # Keep as Decimal
                    "deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)), # Keep as Decimal
                    "min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)), # Keep as Decimal
                    "max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)), # Keep as Decimal
                    "pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)), # Keep as Decimal
                    "spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)), # Keep as Decimal
                    # Add other symbol-specific settings from the Group model here
                }
                all_group_settings[symbol.upper()] = settings # Use upper symbol for consistency

        # Now iterate through the populated dictionary and cache each symbol's settings
        # Ensure set_group_symbol_settings_cache is imported from app.core.cache
        # from app.core.cache import set_group_symbol_settings_cache # Already imported at the top
        for symbol, settings in all_group_settings.items():
            await set_group_symbol_settings_cache(redis_client, group_name, symbol, settings)


        logger.debug(f"Initially cached {len(all_group_settings)} group-symbol settings for group '{group_name}'.")

    except Exception as e:
        logger.error(f"Error fetching or caching initial group-symbol settings for group '{group_name}': {e}", exc_info=True)


# --- Redis Publisher Task ---
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
        # Consume messages from the queue
        # This loop will block when the queue is empty and wake up when new data is available
        while True:
            raw_market_data_message = await redis_publish_queue.get() # Get data from the shared queue

            if raw_market_data_message is None:
                logger.info("Publisher task received shutdown signal (None). Exiting.")
                break # Exit the loop if a None message is received (shutdown signal)

            try:
                # Ensure the message is a JSON string before publishing
                # The data received from the queue is likely a dictionary from the Firebase listener
                # Use DecimalEncoder to safely serialize Decimal values to strings
                # Filter out the _timestamp key before publishing
                message_to_publish_data = {k: v for k, v in raw_market_data_message.items() if k != '_timestamp'}

                # Only publish if there is actual market data after filtering
                if message_to_publish_data:
                     message_to_publish = json.dumps(message_to_publish_data, cls=DecimalEncoder) # Use DecimalEncoder for safety
                else:
                     # Skip publishing if only _timestamp was present
                     logger.debug("Publisher: Message contained only _timestamp, skipping publishing.")
                     redis_publish_queue.task_done() # Indicate the task is done for this item
                     continue # Skip to the next message

            except Exception as e:
                logger.error(f"Publisher failed to serialize message to JSON: {e}. Skipping message.", exc_info=True)
                redis_publish_queue.task_done() # Indicate the task is done for this item, even if failed
                continue # Skip to the next message

            try:
                # Publish the JSON string to the Redis channel
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
                # logger.debug(f"Published message to Redis channel '{REDIS_MARKET_DATA_CHANNEL}'. Message: {message_to_publish[:100]}...") # Debug log for published message snippet
            except Exception as e:
                logger.error(f"Publisher failed to publish message to Redis: {e}. Message: {message_to_publish[:100]}... Skipping.", exc_info=True)
                # Do not break here, try to publish next messages

            redis_publish_queue.task_done() # Indicate the task is done for this item

    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)

    finally:
        logger.info("Redis publisher task finished.")


# --- Redis Market Data Broadcaster Task ---
async def redis_market_data_broadcaster(redis_client: Redis):
    """
    Asynchronously subscribes to Redis Pub/Sub market data channel and broadcasts
    personalized market data and account updates to connected WebSocket clients.
    Runs as an asyncio background task. Skips processing of the _timestamp key.
    """
    logger.info("Redis market data broadcaster task started. Subscribing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)

    if not redis_client:
        logger.critical("Redis client not provided for broadcaster task. Exiting.")
        return

    try:
        # Subscribe to the market data channel
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
        logger.info("Successfully subscribed to Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)

        # Listen for messages on the subscribed channel
        # This loop will block waiting for new messages
        async for message in pubsub.listen():
            if message['type'] == 'message':
                # A new message (market data update) is received from Redis
                # The message['data'] is already decoded to string by the Redis client
                # No need to filter _timestamp here if the publisher already does it,
                # but keeping the check below adds robustness.
                logger.debug(f"Received message from Redis: {message['data']}")

                try:
                    # Parse the JSON message received from Redis
                    # Use object_hook to handle potential Decimal values if the publisher sent them as strings
                    # The data received here should NOT contain the _timestamp if filtered by the publisher
                    processed_data = json.loads(message['data'], object_hook=decode_decimal) # Decode potential Decimals back

                    # --- NEW DEBUG LOG: Inspect processed_data ---
                    logger.debug(f"Broadcaster: Successfully decoded message from Redis. Processed data type: {type(processed_data)}. Data preview: {str(processed_data)[:500]}...")
                    # --- END NEW DEBUG LOG ---

                except json.JSONDecodeError as e:
                     logger.error(f"Broadcaster failed to decode JSON message from Redis: {e}. Skipping message.", exc_info=True)
                     continue # Skip to the next message from Redis
                except Exception as e:
                     logger.error(f"Broadcaster failed to process message from Redis: {e}. Skipping message.", exc_info=True)
                     continue # Catch any other errors during initial message processing


                # Get a snapshot of currently active connections
                # Iterate over a copy of the dictionary as connections might be removed during iteration
                connections_snapshot = list(active_websocket_connections.items())

                # --- NEW DEBUG LOG: Check connection snapshot size ---
                logger.debug(f"Broadcaster: Processing message. Active connections snapshot size: {len(connections_snapshot)}")
                # --- END NEW DEBUG LOG ---


                # Iterate through all active connections and send personalized data
                for user_id, websocket_info in connections_snapshot:

                    # --- ADD THIS TRY...EXCEPT BLOCK HERE to catch errors per client ---
                    try:
                        websocket = websocket_info['websocket']
                        group_name = websocket_info['group_name']
                        # user_id = websocket_info['user_id'] # Already have user_id from iterating items()


                        # Retrieve cached user data, portfolio, and group settings for personalization
                        # These need to be awaited as they are async Redis calls
                        user_data = await get_user_data_cache(redis_client, user_id)
                        user_portfolio = await get_user_portfolio_cache(redis_client, user_id)

                        # --- ADD LOGGING FOR CACHE RETRIEVAL ---
                        logger.debug(f"Attempting to retrieve group symbol settings from cache for user {user_id}, group '{group_name}', symbol 'ALL'.")
                        # This call should now correctly return a dictionary of settings due to the fix in cache.py
                        user_group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                        logger.debug(f"Retrieved group symbol settings from cache for user {user_id}: Type={type(user_group_symbol_settings)}. Value preview: {str(user_group_symbol_settings)[:500]}...") # Limit value preview


                        if not user_data:
                             logger.warning(f"User data not found in cache for user {user_id}. Skipping market data update for this user.")
                             continue # Skip this user if data is not cached


                        # --- Calculate Personalized Data ---
                        # Apply group settings (spread, pips, commission etc.) to raw market data (processed_data)
                        # to get adjusted_market_prices for this user.
                        adjusted_market_prices: Dict[str, Any] = {}

                        # Fix: Ensure processed_data is a dictionary before iterating
                        if isinstance(processed_data, dict):
                             for symbol, market_data in processed_data.items():
                                 # Explicitly skip the _timestamp key if it was somehow not filtered earlier
                                 if symbol == '_timestamp':
                                     continue # Skip the timestamp entry

                                 # Ensure market_data for the symbol is a dictionary
                                 if isinstance(market_data, dict):
                                      # Use .get() with a default of None for safety
                                      raw_ask_price = market_data.get('o') # 'o' for Ask
                                      raw_bid_price = market_data.get('b') # 'b' for Bid

                                      # Get group settings for this symbol (case-insensitive lookup for symbol)
                                      # Ensure user_group_symbol_settings is a dictionary before accessing
                                      symbol_settings = None
                                      if isinstance(user_group_symbol_settings, dict):
                                          symbol_settings = user_group_symbol_settings.get(symbol.upper())
                                      else:
                                           # Log if group settings are not a dictionary (shouldn't happen after cache.py fix, but good for debugging)
                                           # This warning should now be less frequent/eliminated if the cache fix works
                                           logger.warning(f"User {user_id}: user_group_symbol_settings is not a dictionary (Type: {type(user_group_symbol_settings)}). Cannot apply group settings for symbol {symbol}.")


                                      # Perform spread calculation if we have raw prices and symbol settings
                                      if raw_ask_price is not None and raw_bid_price is not None and symbol_settings:
                                           try:
                                                # Ensure spread and spread_pip are Decimal types for accurate calculation
                                                # Convert from string or other types if necessary (caching should handle this)
                                                # Use Decimal() with str() for safe conversion
                                                spread_setting = decimal.Decimal(str(symbol_settings.get('spread', 0)))
                                                spread_pip_setting = decimal.Decimal(str(symbol_settings.get('spread_pip', 0)))
                                                ask_decimal = decimal.Decimal(str(raw_ask_price))
                                                bid_decimal = decimal.Decimal(str(raw_bid_price))

                                                # Apply the spread formula
                                                spread_value = spread_setting * spread_pip_setting
                                                half_spread = spread_value / decimal.Decimal(2) # Use Decimal(2) for accurate division

                                                # Calculate adjusted prices
                                                adjusted_sell_price = bid_decimal - half_spread
                                                adjusted_buy_price = ask_decimal + half_spread

                                                # Store adjusted prices (and maybe original raw prices if needed by the client)
                                                # Convert Decimal to float for JSON serialization if not using a custom encoder
                                                # Since you have DecimalEncoder, you can keep them as Decimal here if the encoder supports it.
                                                # Sending as floats is often simpler for frontend consumption.
                                                adjusted_market_prices[symbol.upper()] = {
                                                     'raw_ask': float(ask_decimal), # Original Ask price
                                                     'raw_bid': float(bid_decimal), # Original Bid price
                                                     'buy': float(adjusted_buy_price), # Adjusted BUY price for the user
                                                     'sell': float(adjusted_sell_price), # Adjusted SELL price for the user
                                                     # You can add other symbol-specific data from market_data or settings here
                                                     'spread_value': float(spread_value), # Include calculated spread value
                                                     'half_spread': float(half_spread), # Include calculated half spread
                                                     # Include other group/symbol settings relevant to the client (e.g., pips, commission)
                                                     'pips': float(symbol_settings.get('pips', 0)), # Example of including another setting
                                                     # Add other settings here as needed by the frontend, converting Decimals to float
                                                     'commision': float(symbol_settings.get('commision', 0)),
                                                     'margin_factor': float(symbol_settings.get('margin', 0)) # Assuming 'margin' is a factor
                                                }
                                           except Exception as calc_e:
                                                logger.error(f"Error calculating spread for user {user_id}, group '{group_name}', symbol {symbol}: {calc_e}", exc_info=True)
                                                # If calculation fails, send the raw prices if available as a fallback
                                                if raw_ask_price is not None and raw_bid_price is not None:
                                                    adjusted_market_prices[symbol.upper()] = {
                                                        'raw_ask': float(raw_ask_price),
                                                        'raw_bid': float(raw_bid_price),
                                                        'buy': float(raw_ask_price), # Fallback to raw ask if calculation fails
                                                        'sell': float(raw_bid_price) # Fallback to raw bid if calculation fails
                                                    }
                                                logger.warning(f"Falling back to raw prices for user {user_id}, symbol {symbol} due to calculation error.")
                                      elif raw_ask_price is not None and raw_bid_price is not None:
                                           # If no group settings found for this symbol, send raw prices as a fallback
                                            adjusted_market_prices[symbol.upper()] = {
                                                'raw_ask': float(raw_ask_price),
                                                'raw_bid': float(raw_bid_price),
                                                'buy': float(raw_ask_price), # Fallback to raw ask
                                                'sell': float(raw_bid_price) # Fallback to raw bid
                                            }
                                            # Log this specific fallback reason
                                            logger.warning(f"No group settings found for user {user_id}, group '{group_name}', symbol {symbol} in the retrieved group settings dictionary. Sending raw prices for this symbol.")
                                      else:
                                           logger.warning(f"Incomplete market data for user {user_id}, symbol {symbol}. Skipping.")
                                 else:
                                      # This warning is now specifically for entries that are not dictionaries and are not _timestamp
                                      logger.warning(f"Market data for symbol {symbol} is not in expected dictionary format for user {user_id}. Type: {type(market_data)}. Data: {market_data}")
                        elif not isinstance(processed_data, dict):
                             logger.warning(f"Processed data from Redis is not a dictionary for user {user_id}. Type: {type(processed_data)}")
                        # The case where user_group_symbol_settings is not a dictionary is now handled inside the symbol loop


                        # 2. Use the user's portfolio (user_portfolio) and current market prices (processed_data or adjusted_market_prices)
                        #    to calculate account stats (PnL, margin, equity, free_margin etc.) for calculated_account_data.
                        #    You NEED to implement the calculation logic for account data here
                        calculated_account_data: Dict[str, Any] = {
                             "balance": user_portfolio.get("balance", 0.0), # Use cached balance (ensure float)
                             "equity": user_portfolio.get("equity", 0.0), # Use cached equity or calculate (ensure float)
                             "margin": user_portfolio.get("margin", 0.0), # Use cached margin or calculate (ensure float)
                             "free_margin": user_portfolio.get("free_margin", 0.0), # Use cached free_margin or calculate (ensure float)
                             "profit_loss": user_portfolio.get("profit_loss", 0.0), # Use cached PnL or calculate (ensure float)
                             "positions": user_portfolio.get("positions", []) # Use cached positions or fetch/calculate
                             # You MUST implement the calculation of equity, margin, free_margin, and profit_loss
                             # based on the user's open positions (from user_portfolio['positions']) and the
                             # current *adjusted* market prices (adjusted_market_prices).
                             # This requires significant trading logic based on position entry price, size, type (buy/sell), etc.
                             # Refer to trading documentation or examples for calculating PnL, margin, and equity.
                        }


                        # --- DEBUG LOGS SHOULD BE HERE ---
                        # These logs will show the data before we check if it's non-empty
                        logger.debug(f"User {user_id}: adjusted_market_prices before check: {adjusted_market_prices}")
                        logger.debug(f"User {user_id}: calculated_account_data before check: {calculated_account_data}")
                        # --- END DEBUG LOGS ---

                        # Check if there is data to send (either market prices or account data)
                        # Only send if there are adjusted market prices OR calculated account data
                        if adjusted_market_prices or calculated_account_data:
                            data_to_send = {
                                "type": "market_data_update",
                                "data": adjusted_market_prices, # Sending the processed/adjusted market data
                                "account": calculated_account_data # Sending calculated account data
                            }

                            # --- NEW DEBUG LOGS BEFORE SEND ---
                            try:
                                 # Attempt to serialize to JSON manually using DecimalEncoder
                                 # Ensure all Decimal values are converted to float or handled by the encoder
                                 data_to_send_json_string = json.dumps(data_to_send, cls=DecimalEncoder)
                                 logger.debug(f"User {user_id}: Preparing to send data. Data size (approx): {len(data_to_send_json_string)} bytes.")
                                 # Log a preview of the data being sent (be mindful of logging sensitive info)
                                 # logger.debug(f"User {user_id}: Data to send preview: {data_to_send_json_string[:500]}...") # Log first 500 chars

                            except Exception as json_e:
                                 # This catches serialization errors *before* attempting to send
                                 logger.error(f"User {user_id}: Failed to serialize data_to_send to JSON: {json_e}", exc_info=True)
                                 # If data cannot be serialized, we cannot send it, continue to the next client or message
                                 continue # Skip sending to this client for this message


                            # Use send_text after manual JSON dumping to handle Decimal values
                            await websocket.send_text(data_to_send_json_string)
                            logger.info(f"Sending personalized data to user {user_id}.") # This log should now appear if send is successful

                        else:
                             # This log appears if both adjusted_market_prices and calculated_account_data are empty/falsey
                             logger.info(f"No market prices or account data to send for user {user_id} on this tick.")


                    except WebSocketDisconnect:
                        logger.info(f"User {user_id} disconnected.")
                        # Clean up connection: remove from active_websocket_connections (thread-safe)
                        with active_connections_lock:
                            if user_id in active_websocket_connections:
                                del active_websocket_connections[user_id]
                                # Fix: Call len() on the dictionary, not the lock
                                logger.info(f"User {user_id} connection removed. Total active connections: {len(active_websocket_connections)}")
                    # --- ADD THIS EXCEPTION CATCH FOR ERRORS WITHIN THE LOOP ---
                    except Exception as e:
                        # This catches any other exception during processing *for a single client*
                        logger.error(f"Error processing or sending data to user {user_id} in broadcaster loop: {e}", exc_info=True)
                        # Consider closing the websocket or marking it for removal on error
                        try:
                            # Attempt to close the websocket gracefully
                            # Check if websocket is still in a state that can be closed
                            if websocket.client_state != WebSocketState.DISCONNECTED:
                                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                                logger.info(f"Closed websocket for user {user_id} after processing error.")
                        except Exception as close_e:
                            # Log errors during the websocket closing attempt
                            logger.error(f"Error during WebSocket close for user {user_id} after processing error: {close_e}", exc_info=True)
                        # Ensure connection is removed from active connections (thread-safe)
                        with active_connections_lock:
                             if user_id in active_websocket_connections:
                                del active_websocket_connections[user_id]
                                # Fix: Call len() on the dictionary, not the lock
                                logger.info(f"User {user_id} connection removed after processing error. Total active connections: {len(active_websocket_connections)}")
                    # --- END ADD TRY...EXCEPT BLOCK HERE ---


    except asyncio.CancelledError:
        logger.info("Redis market data broadcaster task cancelled.")
    except Exception as e:
        # This catches errors from pubsub.listen() or unexpected issues in the outer async for loop
        # These are typically critical errors for the broadcaster task itself
        logger.critical(f"FATAL ERROR: Redis market data broadcaster task failed: {e}", exc_info=True)

    finally:
        # Cleanup when the broadcaster task finishes
        logger.info("Redis market data broadcaster task finished.")
        # Unsubscribe from the channel on task finish
        try:
             # Fix: Check if redis_client is not None instead of using .closed
             if redis_client:
                # Ensure pubsub object exists before unsubscribing
                if 'pubsub' in locals() and pubsub:
                    await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                    logger.info("Successfully unsubscribed from Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
                else:
                     logger.warning("Pubsub object not available during unsubscribe attempt.")
        except Exception as unsub_e:
             logger.error(f"Error during Redis unsubscribe: {unsub_e}", exc_info=True)
        # Close the pubsub connection
        try:
            # Fix: Check if pubsub is not None instead of using .closed
            if 'pubsub' in locals() and pubsub:
                 await pubsub.close()
                 logger.info("Redis pubsub connection closed.")
            else:
                 logger.warning("Pubsub object not available during close attempt.")
        except Exception as close_e:
             logger.error(f"Error during Redis pubsub close: {close_e}", exc_info=True)

# --- The update_group_symbol_settings helper function remains as is ---
# Its implementation is included in the original file you provided.
# I have kept it below for completeness, assuming it was already there.

async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    """
    Fetches group-symbol settings from the database and caches them in Redis.
    Called during WebSocket connection setup and potentially on admin updates.
    """
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return

    try:
        group_settings_list = await crud_group.get_groups(db, search=group_name)

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return

        all_group_settings: Dict[str, Dict[str, Any]] = {}
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
                    "margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)),
                    "spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)),
                    "deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)),
                    "min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)),
                    "max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)),
                    "pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)),
                    "spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)),
                }
                all_group_settings[symbol.upper()] = settings

        for symbol, settings in all_group_settings.items():
            await set_group_symbol_settings_cache(redis_client, group_name, symbol, settings)

        logger.debug(f"Initially cached {len(all_group_settings)} group-symbol settings for group '{group_name}'.")

    except Exception as e:
        logger.error(f"Error fetching or caching initial group-symbol settings for group '{group_name}': {e}", exc_info=True)