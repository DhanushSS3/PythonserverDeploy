# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends
import asyncio
import logging
from app.crud.crud_order import get_order_model
import json
import threading
from typing import Dict, Any, List, Optional
import decimal
from starlette.websockets import WebSocketState
from decimal import Decimal


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.crud import user as crud_user
from app.crud import group as crud_group
from app.crud import crud_order

# Import security functions for token validation
from app.core.security import decode_token

# Import the Redis client type
from redis.asyncio import Redis

# Import the caching helper functions
from app.core.cache import (
    set_user_data_cache,
    get_user_data_cache,
    set_user_portfolio_cache,
    get_user_portfolio_cache,
    get_user_positions_from_cache,
    set_adjusted_market_price_cache,
    get_adjusted_market_price_cache,
    set_group_symbol_settings_cache,
    get_group_symbol_settings_cache,
    DecimalEncoder,
    decode_decimal
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue
from app.shared_state import redis_publish_queue

# Import the new portfolio calculation service
from app.services.portfolio_calculator import calculate_user_portfolio

# Import the Symbol and ExternalSymbolInfo models
from app.database.models import Symbol, ExternalSymbolInfo
from sqlalchemy.future import select

# Configure logging for this module
logger = logging.getLogger(__name__)


# Dictionary to store active WebSocket connections
# Key: user_id, Value: {'websocket': WebSocket, 'group_name': str, 'user_id': int}
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
    token: str = Query(...),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
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
        from jose import JWTError
        payload = decode_token(token)
        user_id = int(payload.get("sub"))

        if user_id is None:
            logger.warning(f"Authentication failed for connection from {websocket.client.host}:{websocket.client.port}: User ID not found in token.")
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: User ID missing")

        db_user = await crud_user.get_user_by_id(db, user_id)
        if db_user is None or not getattr(db_user, 'isActive', True):
             logger.warning(f"Authentication failed for user ID {user_id} from {websocket.client.host}:{websocket.client.port}: User not found or inactive.")
             raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="User not found or inactive")

        group_name = getattr(db_user, 'group_name', 'default')
        user_overall_margin = getattr(db_user, 'margin', Decimal("0.0"))

        # Cache user data (including group_name, balance, leverage, and OVERALL MARGIN)
        user_data_to_cache = {
            "id": user_id,
            "group_name": group_name,
            "leverage": decimal.Decimal(str(getattr(db_user, 'leverage', 1.0))),
            "wallet_balance": decimal.Decimal(str(getattr(db_user, 'wallet_balance', 0.0))),
            "margin": decimal.Decimal(str(user_overall_margin))
        }
        await set_user_data_cache(redis_client, user_id, user_data_to_cache)
        logger.debug(f"Cached user data (including group_name, balance, leverage, overall margin) for user ID {user_id}")

        user_model_instance = db_user

        order_model_class = get_order_model(user_model_instance)

        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model_class)

        initial_positions_data = []
        for pos in open_positions_orm:
             pos_dict = {}
             for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
                  value = getattr(pos, attr, None)
                  if isinstance(value, decimal.Decimal):
                       pos_dict[attr] = str(value)
                  else:
                       pos_dict[attr] = value
             pos_dict['profit_loss'] = "0.0"
             initial_positions_data.append(pos_dict)

        user_portfolio_data = {
             "balance": str(user_data_to_cache["wallet_balance"]),
             "equity": "0.0",
             "margin": str(user_data_to_cache["margin"]),
             "free_margin": "0.0",
             "profit_loss": "0.0",
             "margin_level": "0.0", # Initialize margin_level here
             "positions": initial_positions_data
        }
        await set_user_portfolio_cache(redis_client, user_id, user_portfolio_data)
        logger.debug(f"Cached initial user portfolio (with {len(initial_positions_data)} open positions and overall margin) for user ID {user_id}")

        # Cache Group-Symbol Settings (Initial Load)
        await update_group_symbol_settings(group_name, db, redis_client)
        logger.debug(f"Initially cached group-symbol settings for group '{group_name}'.")

    except JWTError:
        logger.warning(f"Authentication failed for connection from {websocket.client.host}:{websocket.client.port}: Invalid token.")
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid authentication token")
    except WebSocketException:
         raise
    except Exception as e:
        logger.error(f"Unexpected authentication error for connection from {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        raise WebSocketException(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication failed due to internal error")


    await websocket.accept()
    logger.info(f"WebSocket connection accepted and authenticated for user ID {user_id} (Group: {group_name}).")

    with active_connections_lock:
        active_websocket_connections[user_id] = {
            'websocket': websocket,
            'group_name': group_name,
            'user_id': user_id,
        }
        logger.info(f"New authenticated WebSocket connection added for user {user_id}. Total active connections: {len(active_websocket_connections)}")

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                logger.debug(f"Received message from user {user_id}: {message}")

            except asyncio.TimeoutError:
                 pass

            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected by client {user_id}.")
                break
            except Exception as e:
                 logger.error(f"Error receiving message from user {user_id}: {e}", exc_info=True)
                 break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected by client {user_id} outside of receive loop.")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket connection for user {user_id}: {e}", exc_info=True)

    finally:
        with active_connections_lock:
            if user_id in active_websocket_connections:
                del active_websocket_connections[user_id]
                logger.info(f"WebSocket connection closed for user {user_id}. Total active connections: {len(active_websocket_connections)}")
        try:
             if websocket.client_state != WebSocketState.DISCONNECTED:
                 await websocket.close()
             else:
                 logger.debug(f"WebSocket for user {user_id} already disconnected, no need to close.")
        except Exception as close_e:
             logger.error(f"Error during WebSocket close for user {user_id}: {close_e}", exc_info=True)


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    """
    Fetches group-symbol settings from the database and caches them in Redis.
    Now also fetches profit_currency from the Symbol model and contract_size from ExternalSymbolInfo.
    """
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return

    try:
        group_settings_list = await crud_group.get_groups(db, search=group_name)

        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'. Cannot cache settings.")
             return

        for group_setting in group_settings_list:
            symbol = getattr(group_setting, 'symbol', None)
            if symbol:
                settings = {
                    "commision_type": getattr(group_setting, 'commision_type', None),
                    "commision_value_type": getattr(group_setting, 'commision_value_type', None),
                    "type": getattr(group_setting, 'type', None),
                    "pip_currency": getattr(group_setting, 'pip_currency', "USD"),
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
                    "contract_size": getattr(group_setting, 'contract_size', decimal.Decimal("100000")),
                }

                symbol_obj_stmt = select(Symbol).filter_by(name=symbol.upper())
                symbol_obj_result = await db.execute(symbol_obj_stmt)
                symbol_obj = symbol_obj_result.scalars().first()

                if symbol_obj and symbol_obj.profit_currency:
                    settings["profit_currency"] = symbol_obj.profit_currency
                    logger.debug(f"User group settings: Fetched profit_currency '{symbol_obj.profit_currency}' from Symbol model for '{symbol}'.")
                else:
                    settings["profit_currency"] = getattr(group_setting, 'pip_currency', 'USD')
                    logger.warning(f"User group settings: Could not fetch profit_currency from Symbol model for '{symbol}'. Falling back to Group's pip_currency: {settings['profit_currency']}.")

                external_symbol_obj_stmt = select(ExternalSymbolInfo).filter_by(fix_symbol=symbol)
                external_symbol_obj_result = await db.execute(external_symbol_obj_stmt)
                external_symbol_obj = external_symbol_obj_result.scalars().first()

                if external_symbol_obj and external_symbol_obj.contract_size is not None:
                    settings["contract_size"] = external_symbol_obj.contract_size
                    logger.debug(f"User group settings: Fetched contract_size '{external_symbol_obj.contract_size}' from ExternalSymbolInfo for '{symbol}'.")
                else:
                    logger.warning(f"User group settings: Could not fetch contract_size from ExternalSymbolInfo for '{symbol}'. Keeping existing value (from Group or default).")

                await set_group_symbol_settings_cache(redis_client, group_name, symbol.upper(), settings)

            else:
                 logger.warning(f"Group setting symbol is None for group '{group_name}'. Skipping caching for this entry.")

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
                # Filter out the _timestamp key before publishing
                message_to_publish_data = {k: v for k, v in raw_market_data_message.items() if k != '_timestamp'}

                if message_to_publish_data:
                     # Add a type identifier for market data updates
                     message_to_publish_data["type"] = "market_data_update"
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


# # --- Helper Function for processing and sending updates to a single user ---
# async def process_single_user_update(
#     user_id: int,
#     websocket_info: Dict[str, Any],
#     redis_client: Redis,
#     market_data_update: Dict[str, Any], # Contains updated market prices, could be empty if only account update
#     force_account_recalc: bool = False # Flag to force recalculation even without market_data_update
# ):
#     """
#     Processes and sends personalized data to a single WebSocket client.
#     Now sends only buy, sell, and effective_spread_in_pips (as 'spread').
#     """
#     websocket = websocket_info['websocket']
#     group_name = websocket_info['group_name']

#     # Retrieve cached user data, portfolio, and group settings
#     user_data = await get_user_data_cache(redis_client, user_id)
#     user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
#     group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

#     if not user_data:
#         logger.warning(f"User data not found in cache for user {user_id}. Skipping data update for this user.")
#         return

#     relevant_symbols = set(group_settings.keys()) if isinstance(group_settings, dict) else set()
#     user_market_data_payload: Dict[str, Dict[str, float]] = {}

#     # --- Populate with Latest Prices (Updated or Cached) ---
#     # Prioritize prices from the market_data_update if present
#     if market_data_update:
#         symbols_processed_from_update = set()
#         if isinstance(market_data_update, dict):
#             for symbol, prices in market_data_update.items():
#                 symbol_upper = symbol.upper()
#                 symbols_processed_from_update.add(symbol_upper)

#                 # Only process updated symbols that are relevant to this user's group
#                 if symbol_upper in relevant_symbols and isinstance(prices, dict):
#                     raw_ask_price = prices.get('o')
#                     raw_bid_price = prices.get('b')
#                     symbol_settings = group_settings.get(symbol_upper)

#                     if raw_ask_price is not None and raw_bid_price is not None and symbol_settings:
#                         try:
#                             # Ensure Decimal types for calculations
#                             spread_setting = decimal.Decimal(str(symbol_settings.get('spread', 0))) # e.g., 2 (number of pips)
#                             spread_pip_setting = decimal.Decimal(str(symbol_settings.get('spread_pip', 0))) # e.g., 0.0001 (value of 1 pip)
#                             ask_decimal = decimal.Decimal(str(raw_ask_price))
#                             bid_decimal = decimal.Decimal(str(raw_bid_price))

#                             # Configured spread amount in price units (e.g., 0.0002 for 2 pips EURUSD)
#                             configured_spread_amount = spread_setting * spread_pip_setting
#                             half_spread = configured_spread_amount / decimal.Decimal(2)

#                             # Adjusted buy/sell prices that are sent to the frontend
#                             adjusted_buy_price = ask_decimal + half_spread
#                             adjusted_sell_price = bid_decimal - half_spread

#                             # --- CALCULATE SPREAD FOR FRONTEND DISPLAY ---

#                             # Effective Spread in Price Units (e.g., 0.0002 for EURUSD)
#                             effective_spread_price_units = adjusted_buy_price - adjusted_sell_price

#                             # Effective Spread in Pips (e.g., 2.0 pips for EURUSD)
#                             effective_spread_in_pips = Decimal("0.0")
#                             if spread_pip_setting > Decimal("0.0"):
#                                 effective_spread_in_pips = effective_spread_price_units / spread_pip_setting
#                             else:
#                                 logger.warning(f"spread_pip_setting is zero for {symbol_upper}. Cannot calculate effective spread in pips.")


#                             # --- Update the user_market_data_payload with requested fields ---
#                             user_market_data_payload[symbol_upper] = {
#                                 'buy': float(adjusted_buy_price),
#                                 'sell': float(adjusted_sell_price),
#                                 'spread': float(effective_spread_in_pips), # Renamed to 'spread' as requested

#                             }

#                             await set_adjusted_market_price_cache(
#                                 redis_client=redis_client,
#                                 group_name=group_name,
#                                 symbol=symbol_upper,
#                                 buy_price=adjusted_buy_price,
#                                 sell_price=adjusted_sell_price,
#                                 spread_value=configured_spread_amount # Still caching the configured spread amount
#                             )
#                             logger.debug(f"Cached and added newly adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from update)")

#                         except Exception as calc_e:
#                             logger.error(f"Error calculating/caching adjusted prices for user {user_id}, group '{group_name}', symbol {symbol}: {calc_e}", exc_info=True)
#                             # Fallback to raw prices if calculation fails
#                             if raw_ask_price is not None and raw_bid_price is not None:
#                                  # Calculate raw spread for fallback
#                                  raw_effective_spread_price_units = Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price))
#                                  raw_effective_spread_in_pips = Decimal("0.0")
#                                  if spread_pip_setting > Decimal("0.0"):
#                                      raw_effective_spread_in_pips = raw_effective_spread_price_units / spread_pip_setting

#                                  user_market_data_payload[symbol_upper] = {
#                                     'buy': float(raw_ask_price),
#                                     'sell': float(raw_bid_price),
#                                     'spread': float(raw_effective_spread_in_pips), # Fallback spread in pips
#                                  }
#                                  logger.warning(f"Falling back to raw prices (buy/sell) for user {user_id}, symbol {symbol_upper} due to calculation error.")
#                             else:
#                                  logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Cannot send price update.")

#                     elif raw_ask_price is not None and raw_bid_price is not None:
#                          # If no group settings found for this symbol, send raw prices as a fallback
#                          raw_effective_spread_price_units = Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price))
#                          raw_effective_spread_in_pips = Decimal("0.0")
#                          # Try to get spread_pip_setting from group_settings if symbol_settings was None
#                          # This might be tricky if symbol_settings is None, but let's try to get it if available
#                          if group_settings.get(symbol_upper) and decimal.Decimal(str(group_settings.get(symbol_upper).get('spread_pip', 0))) > Decimal("0.0"):
#                              raw_spread_pip_setting_for_fallback = decimal.Decimal(str(group_settings.get(symbol_upper).get('spread_pip', 0)))
#                              raw_effective_spread_in_pips = raw_effective_spread_price_units / raw_spread_pip_setting_for_fallback

#                          user_market_data_payload[symbol_upper] = {
#                              'buy': float(raw_ask_price),
#                              'sell': float(raw_bid_price),
#                              'spread': float(raw_effective_spread_in_pips), # Fallback spread in pips
#                          }
#                          logger.warning(f"No group settings found for user {user_id}, group '{group_name}', symbol {symbol_upper}. Sending raw prices (buy/sell) and calculated raw spreads.")
#                     else:
#                          logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Skipping price update.")

#     # Always populate with cached prices for relevant symbols if not already in payload
#     # This covers symbols not in the current market_data_update OR when force_account_recalc is true
#     for symbol_upper in relevant_symbols:
#         if symbol_upper not in user_market_data_payload: # Only add if not already added from current update
#              cached_adjusted_price = await get_adjusted_market_price_cache(redis_client, group_name, symbol_upper)
#              symbol_settings = group_settings.get(symbol_upper) # Re-fetch symbol settings for cached symbol

#              if cached_adjusted_price:
#                  adjusted_buy_price = decimal.Decimal(str(cached_adjusted_price.get('buy', 0.0)))
#                  adjusted_sell_price = decimal.Decimal(str(cached_adjusted_price.get('sell', 0.0)))
#                  configured_spread_amount = decimal.Decimal(str(cached_adjusted_price.get('spread_value', 0.0))) # This was cached configured spread

#                  # Calculate effective spreads from cached adjusted prices
#                  effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
#                  effective_spread_in_pips = Decimal("0.0")
#                  if symbol_settings and decimal.Decimal(str(symbol_settings.get('spread_pip', 0))) > Decimal("0.0"):
#                      effective_spread_in_pips = effective_spread_price_units / decimal.Decimal(str(symbol_settings.get('spread_pip', 0)))

#                  user_market_data_payload[symbol_upper] = {
#                      'buy': float(raw_ask_price),
#                      'sell': float(raw_bid_price),
#                      'spread': float(effective_spread_in_pips), # Renamed to 'spread'
                     
#                  }
#                  logger.debug(f"Added cached adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from cache)")
#              else:
#                   logger.warning(f"No cached market price available for {symbol_upper} for user {user_id} during forced account recalculation (or if not in update).")


#     # --- Calculate Dynamic Account Data ---
#     open_positions_from_cache = user_portfolio.get('positions', []) if isinstance(user_portfolio, dict) else []

#     calculated_portfolio_metrics = await calculate_user_portfolio(
#         user_data=user_data,
#         open_positions=open_positions_from_cache,
#         adjusted_market_prices=user_market_data_payload, # Use the combined updated/cached prices
#         group_symbol_settings=group_settings
#     )

#     # --- Prepare and Send Data to WebSocket ---
#     if user_market_data_payload or calculated_portfolio_metrics:
#         account_data_payload = {
#             "balance": calculated_portfolio_metrics.get("balance", 0.0),
#             "equity": calculated_portfolio_metrics.get("equity", 0.0),
#             "margin": float(user_data.get("margin", 0.0)), # User's OVERALL margin from cached user_data
#             "free_margin": calculated_portfolio_metrics.get("free_margin", 0.0),
#             "profit_loss": calculated_portfolio_metrics.get("profit_loss", 0.0),
#             "margin_level": calculated_portfolio_metrics.get("margin_level", 0.0), # Add margin_level here
#             "positions": calculated_portfolio_metrics.get("positions", [])
#         }

#         payload = {
#             "type": "market_data_update", # Use same type for now, client can distinguish content
#             "market_data": user_market_data_payload,
#             "account_data": account_data_payload
#         }

#         # Check if the websocket is still connected before sending
#         if websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 await websocket.send_text(json.dumps(payload, cls=DecimalEncoder))
#                 logger.debug(f"Sent personalized data to user {user_id}. Symbols sent: {list(user_market_data_payload.keys())}. Account data sent: {account_data_payload}")
#             except Exception as send_e:
#                 # Log sending errors but don't necessarily disconnect immediately
#                 logger.error(f"Error sending data to user {user_id}: {send_e}", exc_info=True)
#         else:
#             logger.warning(f"WebSocket for user {user_id} is not connected. Skipping send.")

#     else:
#          logger.debug(f"No relevant market prices or account data to send for user {user_id} on this tick.")

# app/api/v1/endpoints/market_data_ws.py

# ... (other imports and code from the file) ...
import decimal # Ensure decimal is imported if not already at the top level of the file
from decimal import Decimal # Ensure Decimal is imported
# ...

# --- Helper Function for processing and sending updates to a single user ---
# async def process_single_user_update(
#     user_id: int,
#     websocket_info: Dict[str, Any],
#     redis_client: Redis,
#     market_data_update: Dict[str, Any], # Contains updated market prices, could be empty if only account update
#     force_account_recalc: bool = False # Flag to force recalculation even without market_data_update
# ):
#     """
#     Processes and sends personalized data to a single WebSocket client.
#     Now sends only buy, sell, and effective_spread_in_pips (as 'spread').
#     """
#     websocket = websocket_info['websocket']
#     group_name = websocket_info['group_name']

#     # Retrieve cached user data, portfolio, and group settings
#     user_data = await get_user_data_cache(redis_client, user_id)
#     user_portfolio = await get_user_portfolio_cache(redis_client, user_id) #
#     group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

#     if not user_data:
#         logger.warning(f"User data not found in cache for user {user_id}. Skipping data update for this user.")
#         return

#     relevant_symbols = set(group_settings.keys()) if isinstance(group_settings, dict) else set()
#     user_market_data_payload: Dict[str, Dict[str, float]] = {}

#     # --- Populate with Latest Prices (Updated or Cached) ---
#     # Prioritize prices from the market_data_update if present
#     if market_data_update: #
#         symbols_processed_from_update = set()
#         if isinstance(market_data_update, dict):
#             for symbol, prices in market_data_update.items():
#                 symbol_upper = symbol.upper()
#                 symbols_processed_from_update.add(symbol_upper)

#                 # Only process updated symbols that are relevant to this user's group
#                 if symbol_upper in relevant_symbols and isinstance(prices, dict):
#                     raw_ask_price = prices.get('o') #
#                     raw_bid_price = prices.get('b') #
#                     symbol_settings = group_settings.get(symbol_upper)

#                     if raw_ask_price is not None and raw_bid_price is not None and symbol_settings:
#                         try:
#                             # Ensure Decimal types for calculations
#                             spread_setting = decimal.Decimal(str(symbol_settings.get('spread', 0))) # e.g., 2 (number of pips) #
#                             spread_pip_setting = decimal.Decimal(str(symbol_settings.get('spread_pip', 0))) # e.g., 0.0001 (value of 1 pip) #
#                             ask_decimal = decimal.Decimal(str(raw_ask_price))
#                             bid_decimal = decimal.Decimal(str(raw_bid_price))

#                             # Configured spread amount in price units (e.g., 0.0002 for 2 pips EURUSD)
#                             configured_spread_amount = spread_setting * spread_pip_setting #
#                             half_spread = configured_spread_amount / decimal.Decimal(2)

#                             # Adjusted buy/sell prices that are sent to the frontend
#                             adjusted_buy_price = ask_decimal + half_spread #
#                             adjusted_sell_price = bid_decimal - half_spread #

#                             # --- CALCULATE SPREAD FOR FRONTEND DISPLAY ---

#                             # Effective Spread in Price Units (e.g., 0.0002 for EURUSD)
#                             effective_spread_price_units = adjusted_buy_price - adjusted_sell_price #

#                             # Effective Spread in Pips (e.g., 2.0 pips for EURUSD)
#                             effective_spread_in_pips = Decimal("0.0") #
#                             if spread_pip_setting > Decimal("0.0"): #
#                                 effective_spread_in_pips = effective_spread_price_units / spread_pip_setting #
#                             else:
#                                 logger.warning(f"spread_pip_setting is zero for {symbol_upper}. Cannot calculate effective spread in pips.")


#                             # --- Update the user_market_data_payload with requested fields ---
#                             user_market_data_payload[symbol_upper] = { #
#                                 'buy': float(adjusted_buy_price), #
#                                 'sell': float(adjusted_sell_price), #
#                                 'spread': float(effective_spread_in_pips), # Renamed to 'spread' as requested #

#                             }

#                             await set_adjusted_market_price_cache( #
#                                 redis_client=redis_client,
#                                 group_name=group_name,
#                                 symbol=symbol_upper,
#                                 buy_price=adjusted_buy_price,
#                                 sell_price=adjusted_sell_price,
#                                 spread_value=configured_spread_amount # Still caching the configured spread amount #
#                             )
#                             logger.debug(f"Cached and added newly adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from update)")

#                         except Exception as calc_e:
#                             logger.error(f"Error calculating/caching adjusted prices for user {user_id}, group '{group_name}', symbol {symbol}: {calc_e}", exc_info=True)
#                             # Fallback to raw prices if calculation fails
#                             if raw_ask_price is not None and raw_bid_price is not None:
#                                  # Calculate raw spread for fallback
#                                  raw_effective_spread_price_units = Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price)) #
#                                  raw_effective_spread_in_pips = Decimal("0.0") #
#                                  if spread_pip_setting > Decimal("0.0"): #
#                                      raw_effective_spread_in_pips = raw_effective_spread_price_units / spread_pip_setting #

#                                  user_market_data_payload[symbol_upper] = { #
#                                     'buy': float(raw_ask_price), #
#                                     'sell': float(raw_bid_price), #
#                                     'spread': float(raw_effective_spread_in_pips), # Fallback spread in pips #
#                                  }
#                                  logger.warning(f"Falling back to raw prices (buy/sell) for user {user_id}, symbol {symbol_upper} due to calculation error.")
#                             else:
#                                  logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Cannot send price update.")

#                     elif raw_ask_price is not None and raw_bid_price is not None:
#                          # If no group settings found for this symbol, send raw prices as a fallback
#                          raw_effective_spread_price_units = Decimal(str(raw_ask_price)) - Decimal(str(raw_bid_price)) #
#                          raw_effective_spread_in_pips = Decimal("0.0") #
#                          # Try to get spread_pip_setting from group_settings if symbol_settings was None
#                          # This might be tricky if symbol_settings is None, but let's try to get it if available
#                          if group_settings.get(symbol_upper) and decimal.Decimal(str(group_settings.get(symbol_upper).get('spread_pip', 0))) > Decimal("0.0"): #
#                              raw_spread_pip_setting_for_fallback = decimal.Decimal(str(group_settings.get(symbol_upper).get('spread_pip', 0))) #
#                              raw_effective_spread_in_pips = raw_effective_spread_price_units / raw_spread_pip_setting_for_fallback #

#                          user_market_data_payload[symbol_upper] = { #
#                              'buy': float(raw_ask_price), #
#                              'sell': float(raw_bid_price), #
#                              'spread': float(raw_effective_spread_in_pips), # Fallback spread in pips #
#                          }
#                          logger.warning(f"No group settings found for user {user_id}, group '{group_name}', symbol {symbol_upper}. Sending raw prices (buy/sell) and calculated raw spreads.")
#                     else:
#                          logger.warning(f"Incomplete raw market data for updated symbol {symbol_upper} for user {user_id}. Skipping price update.")

#     # Always populate with cached prices for relevant symbols if not already in payload
#     # This covers symbols not in the current market_data_update OR when force_account_recalc is true
#     for symbol_upper in relevant_symbols:
#         if symbol_upper not in user_market_data_payload: # Only add if not already added from current update
#              cached_adjusted_price_data = await get_adjusted_market_price_cache(redis_client, group_name, symbol_upper) #
#              symbol_settings = group_settings.get(symbol_upper) # Re-fetch symbol settings for cached symbol

#              if cached_adjusted_price_data:
#                  # Use prices from the cache for this specific symbol
#                  buy_price_from_cache = decimal.Decimal(str(cached_adjusted_price_data.get('buy', 0.0)))
#                  sell_price_from_cache = decimal.Decimal(str(cached_adjusted_price_data.get('sell', 0.0)))
#                  # The 'spread_value' in cache is configured_spread_amount, not effective_spread_in_pips
#                  # We need to recalculate effective_spread_in_pips for the cached prices

#                  effective_spread_price_units_cache = buy_price_from_cache - sell_price_from_cache
#                  effective_spread_in_pips_cache = Decimal("0.0")
#                  if symbol_settings and decimal.Decimal(str(symbol_settings.get('spread_pip', 0))) > Decimal("0.0"):
#                      spread_pip_for_cache_calc = decimal.Decimal(str(symbol_settings.get('spread_pip', 0)))
#                      effective_spread_in_pips_cache = effective_spread_price_units_cache / spread_pip_for_cache_calc

#                  user_market_data_payload[symbol_upper] = {
#                      'buy': float(buy_price_from_cache),    # CORRECTED
#                      'sell': float(sell_price_from_cache),  # CORRECTED
#                      'spread': float(effective_spread_in_pips_cache), # Use spread calculated from these cached prices
#                  }
#                  logger.debug(f"Added cached adjusted price for user {user_id}, group '{group_name}', symbol {symbol_upper} (from cache using its own cached buy/sell)")
#              else:
#                   logger.warning(f"No cached market price available for {symbol_upper} for user {user_id} during forced account recalculation (or if not in update).")


#     # --- Calculate Dynamic Account Data ---
#     open_positions_from_cache = user_portfolio.get('positions', []) if isinstance(user_portfolio, dict) else [] #

#     calculated_portfolio_metrics = await calculate_user_portfolio( #
#         user_data=user_data,
#         open_positions=open_positions_from_cache,
#         adjusted_market_prices=user_market_data_payload, # Use the combined updated/cached prices
#         group_symbol_settings=group_settings
#     )

#     # --- Prepare and Send Data to WebSocket ---
#     if user_market_data_payload or calculated_portfolio_metrics:
#         account_data_payload = {
#             "balance": calculated_portfolio_metrics.get("balance", 0.0), #
#             "equity": calculated_portfolio_metrics.get("equity", 0.0), #
#             "margin": float(user_data.get("margin", 0.0)), # User's OVERALL margin from cached user_data #
#             "free_margin": calculated_portfolio_metrics.get("free_margin", 0.0), #
#             "profit_loss": calculated_portfolio_metrics.get("profit_loss", 0.0), #
#             "margin_level": calculated_portfolio_metrics.get("margin_level", 0.0), # Add margin_level here #
#             "positions": calculated_portfolio_metrics.get("positions", []) #
#         }

#         payload = {
#             "type": "market_data_update", # Use same type for now, client can distinguish content #
#             "market_data": user_market_data_payload,
#             "account_data": account_data_payload
#         }

#         # Check if the websocket is still connected before sending
#         if websocket.client_state == WebSocketState.CONNECTED: #
#             try:
#                 await websocket.send_text(json.dumps(payload, cls=DecimalEncoder)) #
#                 logger.debug(f"Sent personalized data to user {user_id}. Symbols sent: {list(user_market_data_payload.keys())}. Account data sent: {account_data_payload}")
#             except Exception as send_e:
#                 # Log sending errors but don't necessarily disconnect immediately
#                 logger.error(f"Error sending data to user {user_id}: {send_e}", exc_info=True)
#         else:
#             logger.warning(f"WebSocket for user {user_id} is not connected. Skipping send.")

#     else:
#          logger.debug(f"No relevant market prices or account data to send for user {user_id} on this tick.")

# # ... (rest of the market_data_ws.py file) ...


# app/api/v1/endpoints/market_data_ws.py

# ... (other imports and existing code from the file remain unchanged) ...
import decimal # Ensure decimal is imported
from decimal import Decimal # Ensure Decimal is imported

# --- Helper Function for processing and sending updates to a single user (TESTING VERSION MODIFIED) ---
# async def process_single_user_update(
#     user_id: int,
#     websocket_info: Dict[str, Any],
#     redis_client: Redis,
#     market_data_update: Dict[str, Any], # Contains updated market prices, could be empty if only account update
#     force_account_recalc: bool = False # Flag to force recalculation even without market_data_update
# ):
#     """
#     Processes and sends personalized data to a single WebSocket client.
#     MODIFIED TESTING: Assumes Firebase 'b' is Ask and 'o' is Bid. Sends these potentially swapped raw prices.
#     Spread calculation is adjusted accordingly. Caching still uses adjusted prices based on this assumption.
#     """
#     websocket = websocket_info['websocket']
#     group_name = websocket_info['group_name']

#     user_data = await get_user_data_cache(redis_client, user_id)
#     user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
#     group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

#     if not user_data:
#         logger.warning(f"User data not found in cache for user {user_id}. Skipping data update for this user.")
#         return

#     relevant_symbols = set(group_settings.keys()) if isinstance(group_settings, dict) else set()
#     user_market_data_payload: Dict[str, Dict[str, Any]] = {}

#     if market_data_update:
#         if isinstance(market_data_update, dict):
#             for symbol, prices in market_data_update.items():
#                 symbol_upper = symbol.upper()
#                 if symbol_upper in relevant_symbols and isinstance(prices, dict):
                    
#                     raw_ask_price_from_firebase = prices.get('b') # Swapped: Use Firebase 'b' as Ask
#                     raw_bid_price_from_firebase = prices.get('o') # Swapped: Use Firebase 'o' as Bid
                    
#                     logger.info(f"PRICE SWAP LOGIC APPLIED for {symbol_upper}: Firebase 'o' raw = {prices.get('o')}, 'b' raw = {prices.get('b')}. Interpreted Ask (from 'b') = {raw_ask_price_from_firebase}, Bid (from 'o') = {raw_bid_price_from_firebase}")

#                     symbol_settings = group_settings.get(symbol_upper)

#                     if raw_ask_price_from_firebase is not None and raw_bid_price_from_firebase is not None and symbol_settings:
#                         try:
#                             raw_ask_decimal = Decimal(str(raw_ask_price_from_firebase))
#                             raw_bid_decimal = Decimal(str(raw_bid_price_from_firebase))

#                             spread_setting = Decimal(str(symbol_settings.get('spread', 0)))
#                             spread_pip_setting = Decimal(str(symbol_settings.get('spread_pip', 0)))
                            
#                             configured_spread_amount = spread_setting * spread_pip_setting
#                             half_spread = configured_spread_amount / Decimal(2)
                            
#                             adjusted_buy_price_for_cache = raw_ask_decimal + half_spread
#                             adjusted_sell_price_for_cache = raw_bid_decimal - half_spread

#                             await set_adjusted_market_price_cache(
#                                 redis_client=redis_client,
#                                 group_name=group_name,
#                                 symbol=symbol_upper,
#                                 buy_price=adjusted_buy_price_for_cache,
#                                 sell_price=adjusted_sell_price_for_cache,
#                                 spread_value=configured_spread_amount 
#                             )
#                             logger.debug(f"MODIFIED TEST MODE: Cached ADJUSTED price for user {user_id}, symbol {symbol_upper} (buy: {adjusted_buy_price_for_cache}, sell: {adjusted_sell_price_for_cache})")

#                             raw_effective_spread_price_units = raw_ask_decimal - raw_bid_decimal
#                             raw_effective_spread_in_pips = Decimal("0.0")
#                             if spread_pip_setting > Decimal("0.0"):
#                                 raw_effective_spread_in_pips = raw_effective_spread_price_units / spread_pip_setting
#                             else:
#                                 logger.warning(f"spread_pip_setting is zero for {symbol_upper}. Cannot calculate raw spread in pips.")
                            
#                             user_market_data_payload[symbol_upper] = {
#                                 'o': float(raw_ask_decimal),
#                                 'b': float(raw_bid_decimal),
#                                 'spread': float(raw_effective_spread_in_pips),
#                             }
#                             logger.debug(f"MODIFIED TEST MODE: Added interpreted RAW price for user {user_id}, symbol {symbol_upper} (o: {raw_ask_decimal}, b: {raw_bid_decimal})")

#                         except Exception as calc_e:
#                             logger.error(f"Error during MODIFIED TEST MODE price processing for user {user_id}, symbol {symbol_upper}: {calc_e}", exc_info=True)
#                             if raw_ask_price_from_firebase is not None and raw_bid_price_from_firebase is not None:
#                                 raw_ask_fallback = Decimal(str(raw_ask_price_from_firebase))
#                                 raw_bid_fallback = Decimal(str(raw_bid_price_from_firebase))
#                                 spread_val_fallback = Decimal("0.0")
#                                 if symbol_settings and Decimal(str(symbol_settings.get('spread_pip', 0))) > Decimal("0.0"):
#                                      spread_pip_val = Decimal(str(symbol_settings.get('spread_pip', 0)))
#                                      spread_val_fallback = (raw_ask_fallback - raw_bid_fallback) / spread_pip_val
                                
#                                 user_market_data_payload[symbol_upper] = {
#                                     'o': float(raw_ask_fallback),
#                                     'b': float(raw_bid_fallback),
#                                     'spread': float(spread_val_fallback), 
#                                 }
#                             else:
#                                  logger.warning(f"MODIFIED TEST MODE: Incomplete raw market data for {symbol_upper} in fallback.")
                    
#                     elif raw_ask_price_from_firebase is not None and raw_bid_price_from_firebase is not None: # No symbol_settings
#                         raw_ask_no_settings = Decimal(str(raw_ask_price_from_firebase))
#                         raw_bid_no_settings = Decimal(str(raw_bid_price_from_firebase))
#                         spread_no_settings_pips = Decimal("0.0")
#                         gs_for_symbol = group_settings.get(symbol_upper)
#                         if gs_for_symbol and Decimal(str(gs_for_symbol.get('spread_pip', "0"))) > Decimal("0.0"):
#                             spread_pip_no_settings = Decimal(str(gs_for_symbol.get('spread_pip')))
#                             spread_no_settings_pips = (raw_ask_no_settings - raw_bid_no_settings) / spread_pip_no_settings
#                         else:
#                             spread_no_settings_pips = raw_ask_no_settings - raw_bid_no_settings 
#                             logger.warning(f"MODIFIED TEST MODE: spread_pip_setting not found or zero for {symbol_upper} (no settings branch). Spread will be price difference.")

#                         user_market_data_payload[symbol_upper] = {
#                             'o': float(raw_ask_no_settings),
#                             'b': float(raw_bid_no_settings),
#                             'spread': float(spread_no_settings_pips), 
#                         }
#                         logger.warning(f"MODIFIED TEST MODE: No specific symbol settings for {symbol_upper}. Sending interpreted RAW prices.")
#                     else:
#                          logger.warning(f"MODIFIED TEST MODE: Incomplete interpreted raw data for {symbol_upper}. Skipping.")

#     for symbol_upper in relevant_symbols:
#         if symbol_upper not in user_market_data_payload: 
#              cached_adjusted_price_data = await get_adjusted_market_price_cache(redis_client, group_name, symbol_upper)
#              symbol_settings_for_cache = group_settings.get(symbol_upper)

#              if cached_adjusted_price_data:
#                  buy_price_from_cache = Decimal(str(cached_adjusted_price_data.get('buy', 0.0)))
#                  sell_price_from_cache = Decimal(str(cached_adjusted_price_data.get('sell', 0.0)))
                 
#                  effective_spread_price_units_cache = buy_price_from_cache - sell_price_from_cache
#                  effective_spread_in_pips_cache = Decimal("0.0")
#                  if symbol_settings_for_cache and Decimal(str(symbol_settings_for_cache.get('spread_pip', 0))) > Decimal("0.0"):
#                      spread_pip_for_cache_calc = Decimal(str(symbol_settings_for_cache.get('spread_pip', 0)))
#                      effective_spread_in_pips_cache = effective_spread_price_units_cache / spread_pip_for_cache_calc
#                  else: 
#                      effective_spread_in_pips_cache = effective_spread_price_units_cache 
#                      logger.warning(f"MODIFIED TEST MODE: spread_pip_setting zero/missing for cached {symbol_upper}. Spread is price diff.")

#                  # For cached data, we send what was cached (adjusted 'buy'/'sell')
#                  # To align with index.html expecting 'o' and 'b' for raw data:
#                  # If you want index.html to *always* use 'o' and 'b', then cache should store 'o' and 'b'
#                  # or this part needs to decide if it sends 'buy'/'sell' or 'o'/'b'.
#                  # For now, sending as 'o' and 'b' but they are the cached adjusted prices.
#                  # This could be confusing. A better approach might be to ensure frontend handles
#                  # the presence of 'o'/'b' OR 'buy'/'sell' keys. # Corrected Comment
#                  # However, current frontend logic prioritizes 'o'/'b'.
#                  user_market_data_payload[symbol_upper] = {
#                      'o': float(buy_price_from_cache), 
#                      'b': float(sell_price_from_cache),  
#                      'spread': float(effective_spread_in_pips_cache), 
#                  }
#                  logger.debug(f"MODIFIED TEST MODE: Added CACHED (ADJUSTED as o/b) price for user {user_id}, symbol {symbol_upper}")
#              else:
#                   logger.warning(f"MODIFIED TEST MODE: No cached market price for {symbol_upper}.")

#     open_positions_from_cache = user_portfolio.get('positions', []) if isinstance(user_portfolio, dict) else []
#     prices_for_portfolio_calc = {}
#     for sym, data_val in user_market_data_payload.items():
#         prices_for_portfolio_calc[sym] = {
#             'buy': data_val.get('o'), 
#             'sell': data_val.get('b') 
#         }

#     calculated_portfolio_metrics = await calculate_user_portfolio(
#         user_data=user_data,
#         open_positions=open_positions_from_cache,
#         adjusted_market_prices=prices_for_portfolio_calc, 
#         group_symbol_settings=group_settings
#     )

#     if user_market_data_payload or calculated_portfolio_metrics: 
#         account_data_payload = {
#             "balance": calculated_portfolio_metrics.get("balance", 0.0),
#             "equity": calculated_portfolio_metrics.get("equity", 0.0),
#             "margin": float(user_data.get("margin", 0.0)), 
#             "free_margin": calculated_portfolio_metrics.get("free_margin", 0.0),
#             "profit_loss": calculated_portfolio_metrics.get("profit_loss", 0.0),
#             "margin_level": calculated_portfolio_metrics.get("margin_level", 0.0), 
#             "positions": calculated_portfolio_metrics.get("positions", [])
#         }

#         payload = {
#             "type": "market_data_update", 
#             "market_data": user_market_data_payload, 
#             "account_data": account_data_payload
#         }

#         if websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 await websocket.send_text(json.dumps(payload, cls=DecimalEncoder))
#                 logger.debug(f"MODIFIED TEST MODE: Sent data to user {user_id}. Market keys: {list(user_market_data_payload.keys())}")
#             except Exception as send_e:
#                 logger.error(f"MODIFIED TEST MODE: Error sending data to user {user_id}: {send_e}", exc_info=True)
#         else:
#             logger.warning(f"MODIFIED TEST MODE: WebSocket for user {user_id} not connected.")
#     else:
#          logger.debug(f"MODIFIED TEST MODE: No relevant data to send for user {user_id}.")

# # ... (the rest of your market_data_ws.py file, including redis_market_data_broadcaster, etc.)

async def process_single_user_update(
    user_id: int,
    websocket_info: Dict[str, Any],
    redis_client: Redis,
    market_data_update: Dict[str, Any],
    force_account_recalc: bool = False
):
    websocket = websocket_info['websocket']
    group_name = websocket_info['group_name']

    user_data = await get_user_data_cache(redis_client, user_id)
    user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

    if not user_data:
        logger.warning(f"User data not found in cache for user {user_id}. Skipping data update.")
        return

    relevant_symbols = set(group_settings.keys()) if isinstance(group_settings, dict) else set()
    user_market_data_payload: Dict[str, Dict[str, float]] = {}

    if market_data_update:
        for symbol, prices in market_data_update.items():
            symbol_upper = symbol.upper()
            if symbol_upper in relevant_symbols and isinstance(prices, dict):
                raw_ask = prices.get('b')  # Firebase: 'b' is ask
                raw_bid = prices.get('o')  # Firebase: 'o' is bid
                symbol_settings = group_settings.get(symbol_upper)

                if raw_ask is not None and raw_bid is not None and symbol_settings:
                    try:
                        ask_decimal = Decimal(str(raw_ask))
                        bid_decimal = Decimal(str(raw_bid))
                        spread_setting = Decimal(str(symbol_settings.get('spread', 0)))
                        spread_pip_setting = Decimal(str(symbol_settings.get('spread_pip', 0)))

                        configured_spread_amount = spread_setting * spread_pip_setting
                        half_spread = configured_spread_amount / Decimal(2)

                        adjusted_buy_price = ask_decimal + half_spread
                        adjusted_sell_price = bid_decimal - half_spread

                        effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                        effective_spread_in_pips = Decimal("0.0")
                        if spread_pip_setting > Decimal("0.0"):
                            effective_spread_in_pips = effective_spread_price_units / spread_pip_setting

                        user_market_data_payload[symbol_upper] = {
                            'buy': float(adjusted_buy_price),
                            'sell': float(adjusted_sell_price),
                            'spread': float(effective_spread_in_pips),
                        }

                        await set_adjusted_market_price_cache(
                            redis_client=redis_client,
                            group_name=group_name,
                            symbol=symbol_upper,
                            buy_price=adjusted_buy_price,
                            sell_price=adjusted_sell_price,
                            spread_value=configured_spread_amount
                        )

                    except Exception as e:
                        logger.error(f"Error processing adjusted prices for {symbol_upper}: {e}", exc_info=True)

    for symbol_upper in relevant_symbols:
        if symbol_upper not in user_market_data_payload:
            cached = await get_adjusted_market_price_cache(redis_client, group_name, symbol_upper)
            symbol_settings = group_settings.get(symbol_upper)

            if cached:
                buy = Decimal(str(cached.get('buy', 0.0)))
                sell = Decimal(str(cached.get('sell', 0.0)))
                spread_pip = Decimal(str(symbol_settings.get('spread_pip', 0))) if symbol_settings else Decimal("0.0")

                effective_spread = (buy - sell) / spread_pip if spread_pip > Decimal("0.0") else (buy - sell)

                user_market_data_payload[symbol_upper] = {
                    'buy': float(buy),
                    'sell': float(sell),
                    'spread': float(effective_spread),
                }

    prices_for_portfolio_calc = {
        sym: {
            'buy': val.get('buy'),
            'sell': val.get('sell')
        }
        for sym, val in user_market_data_payload.items()
    }

    open_positions = user_portfolio.get('positions', []) if isinstance(user_portfolio, dict) else []
    portfolio = await calculate_user_portfolio(
        user_data=user_data,
        open_positions=open_positions,
        adjusted_market_prices=prices_for_portfolio_calc,
        group_symbol_settings=group_settings
    )

    if user_market_data_payload or portfolio:
        account_data_payload = {
            "balance": portfolio.get("balance", 0.0),
            "equity": portfolio.get("equity", 0.0),
            "margin": float(user_data.get("margin", 0.0)),
            "free_margin": portfolio.get("free_margin", 0.0),
            "profit_loss": portfolio.get("profit_loss", 0.0),
            "margin_level": portfolio.get("margin_level", 0.0),
            "positions": portfolio.get("positions", [])
        }

        payload = {
            "type": "market_data_update",
            "market_data": user_market_data_payload,
            "account_data": account_data_payload
        }

        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_text(json.dumps(payload, cls=DecimalEncoder))
                logger.debug(f"Sent adjusted price update to user {user_id}")
            except Exception as e:
                logger.error(f"Error sending WebSocket data to user {user_id}: {e}", exc_info=True)


# --- Redis Market Data Broadcaster Task (Modified) ---
async def redis_market_data_broadcaster(redis_client: Redis):
    """
    Subscribes to Redis Pub/Sub for market data and broadcasts personalized data
    and calculated account updates to connected WebSocket clients.
    Retrieves full set of relevant symbols from cache, applies latest updates,
    and uses cached user data/portfolio/group settings for calculations.
    Includes the user's overall margin from the User table in account_data.
    Now also handles explicit 'account_update_signal' messages.
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
                # Decode the message from Redis (handles Decimal strings)
                decoded_message = json.loads(message['data'], object_hook=decode_decimal)
                # Determine message type and any specific user ID if present
                message_type = decoded_message.get("type", "market_data_update") # Default to market_data_update
                triggered_user_id = decoded_message.get("user_id") # For account_update_signal
                logger.debug(f"Broadcaster received message of type: {message_type}. Triggered user ID: {triggered_user_id}")

            except Exception as e:
                logger.error(f"Broadcaster failed to decode message from Redis: {e}. Skipping message.", exc_info=True)
                continue

            # Get a snapshot of currently active connections to iterate over
            # This is done once per incoming message to ensure consistent iteration
            connections_snapshot = list(active_websocket_connections.items())
            logger.debug(f"Broadcaster processing message for {len(connections_snapshot)} active connections.")

            # Process based on message type
            if message_type == "market_data_update":
                market_data_update = decoded_message # This is the main market data payload
                # Iterate through all active connections and send personalized data
                for user_id, websocket_info in connections_snapshot:
                    try:
                        await process_single_user_update(
                            user_id, websocket_info, redis_client, market_data_update=market_data_update, force_account_recalc=False
                        )
                    except WebSocketDisconnect:
                        logger.info(f"User {user_id} disconnected during market data update processing.")
                        # Clean up connection: remove from active_websocket_connections (thread-safe)
                        with active_connections_lock:
                            if user_id in active_websocket_connections:
                                del active_websocket_connections[user_id]
                                logger.info(f"User {user_id} connection removed. Total active connections: {len(active_websocket_connections)}")
                    except Exception as e:
                        logger.error(f"Error processing market data for user {user_id} in broadcaster: {e}", exc_info=True)
                        # Handle error by attempting to close the websocket and remove from active connections
                        websocket = websocket_info['websocket']
                        try:
                            if websocket.client_state == WebSocketState.CONNECTED:
                                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                                logger.info(f"Closed websocket for user {user_id} after market data error.")
                        except Exception as close_e:
                            logger.error(f"Error during WebSocket close after market data error for user {user_id}: {close_e}", exc_info=True)
                        with active_connections_lock:
                            if user_id in active_websocket_connections:
                                del active_websocket_connections[user_id]
                                logger.info(f"User {user_id} connection removed after error. Total active connections: {len(active_websocket_connections)}")


            elif message_type == "account_update_signal":
                # When an account update signal is received, process only for the specific user
                if triggered_user_id:
                    websocket_info = active_websocket_connections.get(triggered_user_id)
                    if websocket_info:
                        logger.info(f"Received account update signal for user {triggered_user_id}. Forcing account recalculation.")
                        try:
                            # Pass an empty market_data_update, but force account recalculation
                            await process_single_user_update(
                                triggered_user_id, websocket_info, redis_client, market_data_update={}, force_account_recalc=True
                            )
                        except WebSocketDisconnect:
                            logger.info(f"User {triggered_user_id} disconnected during account update processing.")
                            with active_connections_lock:
                                if triggered_user_id in active_websocket_connections:
                                    del active_websocket_connections[triggered_user_id]
                                    logger.info(f"User {triggered_user_id} connection removed. Total active connections: {len(active_websocket_connections)}")
                        except Exception as e:
                            logger.error(f"Error processing account update for user {triggered_user_id} in broadcaster: {e}", exc_info=True)
                            # Handle error by attempting to close the websocket and remove from active connections
                            websocket = websocket_info['websocket']
                            try:
                                if websocket.client_state == WebSocketState.CONNECTED:
                                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                                    logger.info(f"Closed websocket for user {triggered_user_id} after account update error.")
                            except Exception as close_e:
                                logger.error(f"Error during WebSocket close after account update error for user {triggered_user_id}: {close_e}", exc_info=True)
                            with active_connections_lock:
                                if triggered_user_id in active_websocket_connections:
                                    del active_websocket_connections[triggered_user_id]
                                    logger.info(f"User {triggered_user_id} connection removed after error. Total active connections: {len(active_websocket_connections)}")
                    else:
                        logger.debug(f"Account update signal for user {triggered_user_id}, but user is not currently connected via WebSocket.")
                else:
                    logger.warning("Received account_update_signal without a specific user_id. Skipping processing.")
            else:
                logger.warning(f"Received unhandled message type: {message_type}. Message: {decoded_message}")


    except asyncio.CancelledError:
        logger.info("Redis market data broadcaster task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis market data broadcaster task failed: {e}", exc_info=True)

    finally:
        logger.info("Redis market data broadcaster task finished.")
        try:
             if redis_client:
                if 'pubsub' in locals() and pubsub:
                    await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                    logger.info("Successfully unsubscribed from Redis channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
                else:
                     logger.warning("Pubsub object not available during unsubscribe attempt.")
        except Exception as unsub_e:
             logger.error(f"Error during Redis unsubscribe: {unsub_e}", exc_info=True)
        try:
            if 'pubsub' in locals() and pubsub:
                 await pubsub.close()
                 logger.info("Redis pubsub connection closed.")
            else:
                 logger.warning("Pubsub object not available during close attempt.")
        except Exception as close_e:
             logger.error(f"Error during Redis pubsub close: {close_e}", exc_info=True)