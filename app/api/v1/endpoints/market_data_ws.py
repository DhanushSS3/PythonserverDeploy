# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
import json
import threading
from typing import Dict, Any, List, Optional
import decimal # Import decimal for Decimal type and encoder

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
        db_user = await crud_user.get_user_by_id(db, user_id) # Use get_user_by_id now
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
        # For now, you might fetch positions here if they are stored in the DB separately from the main user model
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
        await update_group_symbol_settings(group_name, db, redis_client) # Call the helper function

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
    logger.info(f"WebSocket connection accepted and authenticated for user ID {user_id} (Group: {group_name}) from {websocket.client.host}:{websocket.client.port}.")

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
                message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0) # Added timeout
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
                logger.info(f"WebSocket connection closed for user {user_id}. Total active connections: {len(active_websocket_connections)}")
        # Close the WebSocket gracefully, if not already closed
        try:
             # Check if websocket is still in a state that can be closed (status not 1000 or 1001)
             if websocket.client_state != status.WS_DISCONNECTED: # Check client_state directly
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

# The implementation was provided in a previous turn (May 8, 2025 at 12:05:35 PM IST).
# Copy that implementation here or ensure it's correctly imported.

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
        # Ensure crud_group is imported
        # from app.crud import group as crud_group # Already imported at the top

        # Fetch all group settings for this group name
        # Assuming crud_group.get_groups can filter by name and return a list of Group models
        # And assuming Group model has 'symbol' and relevant setting attributes
        group_settings_list = await crud_group.get_groups(db, search=group_name) # Assuming search param works like this

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return


        all_group_settings: Dict[str, Dict[str, Any]] = {}
        for group_setting in group_settings_list:
            symbol = getattr(group_setting, 'symbol', None)
            if symbol:  # Only process if a symbol is defined for this group entry
                # Extract relevant settings from the group object
                # Adjust attributes based on your Group model. Convert Decimal to float for JSON/Redis if needed later.
                # The caching functions now handle Decimal serialization/deserialization.
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None), # e.g., 'forex', 'crypto', 'indices'
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
                message_to_publish = json.dumps(raw_market_data_message, cls=DecimalEncoder) # Use DecimalEncoder for safety
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
    Runs as an asyncio background task.
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
                logger.debug(f"Received message from Redis: {message['data']}")

                try:
                    # Parse the JSON message received from Redis
                    # Use object_hook to handle potential Decimal values if the publisher sent them as strings
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
                        # Assuming a function to get all group-symbol settings for the user's group
                        # Ensure this function exists and works correctly in cache.py
                        user_group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL") # Example: get all settings for the group

                        if not user_data:
                             logger.warning(f"User data not found in cache for user {user_id}. Skipping market data update for this user.")
                             continue # Skip this user if data is not cached


                        # --- Calculate Personalized Data ---
                        # This is where you will implement your core trading logic:
                        # 1. Apply group settings (spread, pips, commission etc.) to raw market data (processed_data)
                        #    to get adjusted_market_prices for this user.
                        # 2. Use the user's portfolio (user_portfolio) and current market prices (processed_data or adjusted_market_prices)
                        #    to calculate account stats (PnL, margin, equity, free_margin etc.) for calculated_account_data.
                        #
                        # The current implementation is a placeholder:
                        adjusted_market_prices: Dict[str, Any] = processed_data # Placeholder: Pass raw data for now
                        calculated_account_data: Dict[str, Any] = {
                             "balance": user_portfolio.get("balance", 0), # Use cached balance
                             "equity": user_portfolio.get("equity", 0), # Use cached equity or calculate
                             "margin": user_portfolio.get("margin", 0), # Use cached margin or calculate
                             "free_margin": user_portfolio.get("free_margin", 0), # Use cached free_margin or calculate
                             "profit_loss": user_portfolio.get("profit_loss", 0), # Use cached PnL or calculate
                             "positions": user_portfolio.get("positions", []) # Use cached positions or fetch/calculate
                             # You NEED to implement the calculation logic here using processed_data, user_portfolio, and user_group_symbol_settings
                             # Make sure calculated values are compatible with JSON (e.g., floats or strings for Decimals)
                        }


                        # --- DEBUG LOGS SHOULD BE HERE ---
                        # These logs will show the data before we check if it's non-empty
                        logger.debug(f"User {user_id}: adjusted_market_prices before check: {adjusted_market_prices}")
                        logger.debug(f"User {user_id}: calculated_account_data before check: {calculated_account_data}")
                        # --- END DEBUG LOGS ---

                        # Check if there is data to send (either market prices or account data)
                        if adjusted_market_prices or calculated_account_data:
                            data_to_send = {
                                "type": "market_data_update",
                                "data": adjusted_market_prices, # Sending the processed/adjusted market data
                                "account": calculated_account_data # Sending calculated account data
                            }

                            # --- NEW DEBUG LOGS BEFORE SEND ---
                            try:
                                 # Attempt to serialize to JSON manually using DecimalEncoder
                                 data_to_send_json_string = json.dumps(data_to_send, cls=DecimalEncoder)
                                 logger.debug(f"User {user_id}: Preparing to send data. Data size (approx): {len(data_to_send_json_string)} bytes.")
                                 # Log a preview of the data being sent
                                 logger.debug(f"User {user_id}: Data to send preview: {data_to_send_json_string[:500]}...") # Log first 500 chars

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
                                logger.info(f"User {user_id} connection removed. Total active connections: {len(active_websocket_connections)}")
                    # --- ADD THIS EXCEPTION CATCH FOR ERRORS WITHIN THE LOOP ---
                    except Exception as e:
                        # This catches any other exception during processing *for a single client*
                        logger.error(f"Error processing or sending data to user {user_id} in broadcaster loop: {e}", exc_info=True)
                        # Consider closing the websocket or marking it for removal on error
                        try:
                            # Attempt to close the websocket gracefully
                            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                            logger.info(f"Closed websocket for user {user_id} after processing error.")
                        except Exception as close_e:
                            # Log errors during the websocket closing attempt
                            logger.error(f"Error during WebSocket close for user {user_id} after processing error: {close_e}", exc_info=True)
                        # Ensure connection is removed from active connections (thread-safe)
                        with active_connections_lock:
                             if user_id in active_websocket_connections:
                                del active_websocket_connections[user_id]
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
             # Only attempt to unsubscribe and close if redis_client is still valid
             if redis_client and not redis_client.closed:
                await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                logger.info("Successfully unsubscribed from Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
        except Exception as unsub_e:
             logger.error(f"Error during Redis unsubscribe: {unsub_e}", exc_info=True)
        # Close the pubsub connection
        try:
            if pubsub and not pubsub.closed:
                 await pubsub.close()
                 logger.info("Redis pubsub connection closed.")
        except Exception as close_e:
             logger.error(f"Error during Redis pubsub close: {close_e}", exc_info=True)


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
# This helper fetches group settings from DB and caches them in Redis.
# It's defined here for now, called from websocket_endpoint.
# It requires database access (AsyncSession) and Redis client (Redis).
# Ensure crud_group and caching functions are imported at the top.

# The implementation was provided in a previous turn (May 8, 2025 at 12:05:35 PM IST).
# Copy that implementation here or ensure it's correctly imported.

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
        # Ensure crud_group is imported
        # from app.crud import group as crud_group # Already imported at the top

        # Fetch all group settings for this group name
        # Assuming crud_group.get_groups can filter by name and return a list of Group models
        # And assuming Group model has 'symbol' and relevant setting attributes
        group_settings_list = await crud_group.get_groups(db, search=group_name) # Assuming search param works like this

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return


        all_group_settings: Dict[str, Dict[str, Any]] = {}
        for group_setting in group_settings_list:
            symbol = getattr(group_setting, 'symbol', None)
            if symbol:  # Only process if a symbol is defined for this group entry
                # Extract relevant settings from the group object
                # Adjust attributes based on your Group model. Convert Decimal to float for JSON/Redis if needed later.
                # The caching functions now handle Decimal serialization/deserialization.
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None), # e.g., 'forex', 'crypto', 'indices'
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

