# app/core/cache.py

import json
import logging
from typing import Dict, Any, Optional, List
from redis.asyncio import Redis
import decimal # Import Decimal for type hinting and serialization
import datetime
from app.firebase_stream import get_latest_market_data

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
from app.core.logging_config import cache_logger
# Keys for storing data in Redis
REDIS_USER_DATA_KEY_PREFIX = "user_data:" # Stores group_name, leverage, etc.
REDIS_USER_PORTFOLIO_KEY_PREFIX = "user_portfolio:" # Stores balance, positions
# New key prefix for static orders data (open and pending orders)
REDIS_USER_STATIC_ORDERS_KEY_PREFIX = "user_static_orders:" # Stores open and pending orders without PnL
# New key prefix for dynamic portfolio metrics
REDIS_USER_DYNAMIC_PORTFOLIO_KEY_PREFIX = "user_dynamic_portfolio:" # Stores free_margin, positions with PnL, margin_level
# New key prefix for group settings per symbol
REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX = "group_symbol_settings:" # Stores spread, pip values, etc. per group and symbol
# New key prefix for general group settings
REDIS_GROUP_SETTINGS_KEY_PREFIX = "group_settings:" # Stores general group settings like sending_orders
# New key prefix for last known price
LAST_KNOWN_PRICE_KEY_PREFIX = "last_price:"

# Redis channels for real-time updates
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'
REDIS_ORDER_UPDATES_CHANNEL = 'order_updates'
REDIS_USER_DATA_UPDATES_CHANNEL = 'user_data_updates'

# Expiry times (adjust as needed)
USER_DATA_CACHE_EXPIRY_SECONDS = 7 * 24 * 60 * 60 # Example: User session length
# USER_DATA_CACHE_EXPIRY_SECONDS = 10
USER_PORTFOLIO_CACHE_EXPIRY_SECONDS = 5 * 60 # Example: Short expiry, updated frequently
USER_STATIC_ORDERS_CACHE_EXPIRY_SECONDS = 30 * 60 # Static order data expires after 30 minutes
USER_DYNAMIC_PORTFOLIO_CACHE_EXPIRY_SECONDS = 60 # Dynamic portfolio metrics expire after 60 seconds
GROUP_SYMBOL_SETTINGS_CACHE_EXPIRY_SECONDS = 30 * 24 * 60 * 60 # Example: Group settings change infrequently
GROUP_SETTINGS_CACHE_EXPIRY_SECONDS = 30 * 24 * 60 * 60 # Example: Group settings change infrequently

# --- Last Known Price Cache ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

def decode_decimal(obj):
    """Recursively decode dictionary values, attempting to convert strings to Decimal."""
    if isinstance(obj, dict):
        return {k: decode_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_decimal(elem) for elem in obj]
    elif isinstance(obj, str):
        try:
            return decimal.Decimal(obj)
        except decimal.InvalidOperation:
            return obj
    else:
        return obj


# --- User Data Cache (Modified) ---
async def set_user_data_cache(redis_client: Redis, user_id: int, data: Dict[str, Any]):
    """
    Stores relatively static user data (like group_name, leverage) in Redis.
    """
    if not redis_client:
        cache_logger.warning(f"Redis client not available for setting user data cache for user {user_id}.")
        return

    key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
    try:
        # Ensure all Decimal values are handled by DecimalEncoder
        data_serializable = json.dumps(data, cls=DecimalEncoder)
        await redis_client.set(key, data_serializable, ex=USER_DATA_CACHE_EXPIRY_SECONDS)
        cache_logger.debug(f"User data cached for user {user_id}")
    except Exception as e:
        logger.error(f"Error setting user data cache for user {user_id}: {e}", exc_info=True)


async def get_user_data_cache(
    redis_client: Redis,
    user_id: int,
    db: 'AsyncSession',  # REQUIRED
    user_type: str       # REQUIRED
) -> Optional[Dict[str, Any]]:
    """
    Retrieves user data from Redis cache. If not found, fetches from DB,
    caches it, and then returns it. Expected data includes 'group_name', 'leverage', etc.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting user data cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
    try:
        data_json = await redis_client.get(key)
        if data_json:
            data = json.loads(data_json, object_hook=decode_decimal)
            cache_logger.debug(f"User data retrieved from cache for user {user_id}")
            return data
        # If not in cache, try fetching from DB if db and user_type are provided
        if db is not None and user_type is not None:
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            cache_logger.info(f"User data for user {user_id} (type: {user_type}) not in cache. Fetching from DB.")
            db_user_instance = None
            actual_user_type = user_type.lower()
            if actual_user_type == 'live':
                db_user_instance = await get_user_by_id(db, user_id, user_type=actual_user_type)
            elif actual_user_type == 'demo':
                db_user_instance = await get_demo_user_by_id(db, user_id, user_type=actual_user_type)
            if db_user_instance:
                user_data_to_cache = {
                    "id": db_user_instance.id,
                    "email": db_user_instance.email,
                    "group_name": db_user_instance.group_name,
                    "leverage": db_user_instance.leverage,
                    "user_type": db_user_instance.user_type,
                    "account_number": getattr(db_user_instance, 'account_number', None),
                    "wallet_balance": db_user_instance.wallet_balance,
                    "margin": db_user_instance.margin,
                    "first_name": getattr(db_user_instance, 'first_name', None),
                    "last_name": getattr(db_user_instance, 'last_name', None),
                    "country": getattr(db_user_instance, 'country', None),
                    "phone_number": getattr(db_user_instance, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                logger.info(f"User data for user {user_id} (type: {actual_user_type}) fetched from DB and cached.")
                return user_data_to_cache
            else:
                logger.warning(f"User {user_id} (type: {actual_user_type}) not found in DB. Cannot cache.")
                return None
        return None
    except Exception as e:
        logger.error(f"Error getting user data cache for user {user_id}: {e}", exc_info=True)
        return None


# --- User Portfolio Cache (Keep as is) ---
async def set_user_portfolio_cache(redis_client: Redis, user_id: int, portfolio_data: Dict[str, Any]):
    """
    Stores dynamic user portfolio data (balance, positions) in Redis.
    This should be called whenever the user's balance or positions change.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting user portfolio cache for user {user_id}.")
        return

    key = f"{REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        portfolio_serializable = json.dumps(portfolio_data, cls=DecimalEncoder)
        await redis_client.set(key, portfolio_serializable, ex=USER_PORTFOLIO_CACHE_EXPIRY_SECONDS)
        cache_logger.info(f"Writing portfolio cache for user_id={user_id}: {portfolio_data}")
    except Exception as e:
        cache_logger.error(f"Error setting user portfolio cache for user {user_id}: {e}", exc_info=True)


async def get_user_portfolio_cache(redis_client: Redis, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves user portfolio data from Redis cache.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting user portfolio cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        portfolio_json = await redis_client.get(key)
        if portfolio_json:
            portfolio_data = json.loads(portfolio_json, object_hook=decode_decimal)
            cache_logger.info(f"Read portfolio cache for user_id={user_id}: {portfolio_data}")
            return portfolio_data
        return None
    except Exception as e:
        cache_logger.error(f"Error getting user portfolio cache for user {user_id}: {e}", exc_info=True)
        return None

async def get_user_positions_from_cache(redis_client: Redis, user_id: int) -> List[Dict[str, Any]]:
    """
    Retrieves only the list of open positions from the user's cached portfolio data.
    Returns an empty list if data is not found or positions list is empty.
    """
    portfolio = await get_user_portfolio_cache(redis_client, user_id)
    if portfolio and 'positions' in portfolio and isinstance(portfolio['positions'], list):
        # The decode_decimal in get_user_portfolio_cache should handle Decimal conversion within positions
        return portfolio['positions']
    return []

# --- New Static Orders Cache ---
async def set_user_static_orders_cache(redis_client: Redis, user_id: int, static_orders_data: Dict[str, Any]):
    """
    Stores static order data (open and pending orders without PnL) in Redis.
    This should be called whenever orders are added, modified, or removed.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting static orders cache for user {user_id}.")
        return

    key = f"{REDIS_USER_STATIC_ORDERS_KEY_PREFIX}{user_id}"
    try:
        # Ensure all Decimal values are handled by DecimalEncoder
        data_serializable = json.dumps(static_orders_data, cls=DecimalEncoder)
        await redis_client.set(key, data_serializable, ex=USER_STATIC_ORDERS_CACHE_EXPIRY_SECONDS)
        cache_logger.debug(f"Static orders cached for user {user_id}")
    except Exception as e:
        logger.error(f"Error setting static orders cache for user {user_id}: {e}", exc_info=True)

async def get_user_static_orders_cache(redis_client: Redis, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves static order data from Redis cache.
    Returns None if data is not found.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting static orders cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_STATIC_ORDERS_KEY_PREFIX}{user_id}"
    try:
        data_json = await redis_client.get(key)
        if data_json:
            data = json.loads(data_json, object_hook=decode_decimal)
            cache_logger.debug(f"Static orders retrieved from cache for user {user_id}")
            return data
        return None
    except Exception as e:
        logger.error(f"Error getting static orders cache for user {user_id}: {e}", exc_info=True)
        return None

# --- New Dynamic Portfolio Cache ---
async def set_user_dynamic_portfolio_cache(redis_client: Redis, user_id: int, dynamic_portfolio_data: Dict[str, Any]):
    """
    Stores dynamic portfolio metrics (free_margin, positions with PnL, margin_level) in Redis.
    This should be called whenever market data changes affect the user's portfolio.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting dynamic portfolio cache for user {user_id}.")
        return

    key = f"{REDIS_USER_DYNAMIC_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        # Ensure all Decimal values are handled by DecimalEncoder
        data_serializable = json.dumps(dynamic_portfolio_data, cls=DecimalEncoder)
        await redis_client.set(key, data_serializable, ex=USER_DYNAMIC_PORTFOLIO_CACHE_EXPIRY_SECONDS)
        cache_logger.debug(f"Dynamic portfolio cached for user {user_id}")
    except Exception as e:
        logger.error(f"Error setting dynamic portfolio cache for user {user_id}: {e}", exc_info=True)

async def get_user_dynamic_portfolio_cache(redis_client: Redis, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves dynamic portfolio metrics from Redis cache.
    Returns None if data is not found.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting dynamic portfolio cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_DYNAMIC_PORTFOLIO_KEY_PREFIX}{user_id}"
    try:
        data_json = await redis_client.get(key)
        if data_json:
            data = json.loads(data_json, object_hook=decode_decimal)
            cache_logger.debug(f"Dynamic portfolio retrieved from cache for user {user_id}")
            return data
        return None
    except Exception as e:
        logger.error(f"Error getting dynamic portfolio cache for user {user_id}: {e}", exc_info=True)
        return None

# --- New Group Symbol Settings Cache ---

async def set_group_symbol_settings_cache(redis_client: Redis, group_name: str, symbol: str, settings: Dict[str, Any]):
    """
    Stores group-specific settings for a given symbol in Redis.
    Settings include spread, spread_pip, margin, etc.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting group-symbol settings cache for group '{group_name}', symbol '{symbol}'.")
        return

    # Use a composite key: prefix:group_name:symbol
    key = f"{REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}" # Use lower/upper for consistency
    try:
        settings_serializable = json.dumps(settings, cls=DecimalEncoder)
        await redis_client.set(key, settings_serializable, ex=GROUP_SYMBOL_SETTINGS_CACHE_EXPIRY_SECONDS)
        logger.debug(f"Group-symbol settings cached for group '{group_name}', symbol '{symbol}'.")
    except Exception as e:
        logger.error(f"Error setting group-symbol settings cache for group '{group_name}', symbol '{symbol}': {e}", exc_info=True)

async def get_group_symbol_settings_cache(redis_client: Redis, group_name: str, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves group-specific settings for a given symbol from Redis cache.
    If symbol is "ALL", retrieves settings for all symbols for the group.
    Returns None if no settings found for the specified symbol or group.
    """
    if not group_name:
        logger.warning(f"get_group_symbol_settings_cache called with group_name=None. Returning None.")
        return None
    if not redis_client:
        logger.warning(f"Redis client not available for getting group-symbol settings cache for group '{group_name}', symbol '{symbol}'.")
        return None

    if symbol.upper() == "ALL":
        # --- Handle retrieval of ALL settings for the group ---
        all_settings: Dict[str, Dict[str, Any]] = {}
        # Scan for all keys related to this group's symbol settings
        # Use a cursor for efficient scanning of many keys
        cursor = '0'
        prefix = f"{REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX}{group_name.lower()}:"
        try:
            while cursor != 0:
                # Use scan instead of keys for production environments
                # The keys are already strings if decode_responses is True
                cursor, keys = await redis_client.scan(cursor=cursor, match=f"{prefix}*", count=100) # Adjust count as needed

                if keys:
                    # Retrieve all found keys in a pipeline for efficiency
                    pipe = redis_client.pipeline()
                    for key in keys:
                        pipe.get(key)
                    results = await pipe.execute()

                    # Process the results
                    for key, settings_json in zip(keys, results):
                        if settings_json:
                            try:
                                settings = json.loads(settings_json, object_hook=decode_decimal)
                                # Extract symbol from the key (key format: prefix:group_name:symbol)
                                # Key is already a string, no need to decode
                                key_parts = key.split(':')
                                if len(key_parts) == 3 and key_parts[0] == REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX.rstrip(':'):
                                     symbol_from_key = key_parts[2]
                                     all_settings[symbol_from_key] = settings
                                else:
                                     # Key is already a string, no need to decode for logging
                                     logger.warning(f"Skipping incorrectly formatted Redis key: {key}")
                            except json.JSONDecodeError:
                                 # Key is already a string, no need to decode for logging
                                 logger.error(f"Failed to decode JSON for settings key: {key}. Data: {settings_json}", exc_info=True)
                            except Exception as e:
                                # Key is already a string, no need to decode for logging
                                logger.error(f"Unexpected error processing settings key {key}: {e}", exc_info=True)

            if all_settings:
                 cache_logger.debug(f"Aggregated {len(all_settings)} group-symbol settings for group '{group_name}'.")
                 return all_settings
            else:
                 cache_logger.debug(f"No group-symbol settings found for group '{group_name}' using scan.")
                 return None # Return None if no settings were found for the group

        except Exception as e:
             logger.error(f"Error scanning or retrieving group-symbol settings for group '{group_name}': {e}", exc_info=True)
             return None # Return None on error

    else:
        # --- Handle retrieval of settings for a single symbol ---
        key = f"{REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}" # Use lower/upper for consistency
        try:
            settings_json = await redis_client.get(key)
            if settings_json:
                settings = json.loads(settings_json, object_hook=decode_decimal)
                cache_logger.debug(f"Group-symbol settings retrieved from cache for group '{group_name}', symbol '{symbol}'.")
                return settings
            cache_logger.debug(f"Group-symbol settings not found in cache for group '{group_name}', symbol '{symbol}'.")
            return None # Return None if settings for the specific symbol are not found
        except Exception as e:
            cache_logger.error(f"Error getting group-symbol settings cache for group '{group_name}', symbol '{symbol}': {e}", exc_info=True)
            return None

# You might also want a function to cache ALL settings for a group,
# or cache ALL group-symbol settings globally if the dataset is small enough.
# For now, fetching per symbol on demand from cache/DB is a good start.

# Add these functions to your app/core/cache.py file

# New key prefix for adjusted market prices per group and symbol
REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX = "adjusted_market_price:"

# Increase cache expiry for adjusted market prices to 30 seconds
ADJUSTED_MARKET_PRICE_CACHE_EXPIRY_SECONDS = 30  # Cache for 30 seconds

async def set_adjusted_market_price_cache(
    redis_client: Redis,
    group_name: str,
    symbol: str,
    buy_price: decimal.Decimal,
    sell_price: decimal.Decimal,
    spread_value: decimal.Decimal
) -> None:
    """
    Caches the adjusted market buy and sell prices (and spread value)
    for a specific group and symbol in Redis.
    Key structure: adjusted_market_price:{group_name}:{symbol}
    Value is a JSON string: {"buy": "...", "sell": "...", "spread_value": "..."}
    """
    cache_key = f"{REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX}{group_name}:{symbol}"
    cache_logger.debug(f"Setting cache key: {cache_key}")
    try:
        # Create a dictionary with Decimal values
        adjusted_prices = {
            "buy": str(buy_price),  # Convert to string for JSON serialization
            "sell": str(sell_price),
            "spread_value": str(spread_value)
        }
        # Serialize the dictionary to a JSON string
        await redis_client.set(
            cache_key,
            json.dumps(adjusted_prices),
            ex=ADJUSTED_MARKET_PRICE_CACHE_EXPIRY_SECONDS
        )
        cache_logger.debug(f"Successfully cached adjusted market price for key {cache_key}: {adjusted_prices}")
    except Exception as e:
        cache_logger.error(f"Error setting adjusted market price in cache for key {cache_key}: {e}", exc_info=True)

async def get_adjusted_market_price_cache(redis_client: Redis, user_group_name: str, symbol: str) -> Optional[Dict[str, decimal.Decimal]]:
    """
    Retrieves the cached adjusted market prices for a specific group and symbol.
    Returns None if the cache is empty or expired.
    """
    cache_key = f"{REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX}{user_group_name}:{symbol}"
    # cache_logger.debug(f"Looking up cache key: {cache_key}")
    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            price_data = json.loads(cached_data)
            # Convert string values back to Decimal
            return {
                "buy": decimal.Decimal(price_data["buy"]),
                "sell": decimal.Decimal(price_data["sell"]),
                "spread_value": decimal.Decimal(price_data["spread_value"])
            }
        # else:
        #     cache_logger.debug(f"No cached data found for key {cache_key}")
        #     return None
    except Exception as e:
        cache_logger.error(f"Error fetching adjusted market price from cache for key {cache_key}: {e}", exc_info=True)
        return None

async def publish_account_structure_changed_event(redis_client: Redis, user_id: int):
    """
    Publishes an event to a Redis channel indicating that a user's account structure (e.g., portfolio, balance) has changed.
    This can be used by WebSocket clients to trigger UI updates.
    """
    channel = f"user_updates:{user_id}"
    message = json.dumps({"type": "ACCOUNT_STRUCTURE_CHANGED", "user_id": user_id})
    try:
        await redis_client.publish(channel, message)
        cache_logger.info(f"Published ACCOUNT_STRUCTURE_CHANGED event to {channel} for user_id {user_id}")
    except Exception as e:
        cache_logger.error(f"Error publishing ACCOUNT_STRUCTURE_CHANGED event for user {user_id}: {e}", exc_info=True)

async def get_live_adjusted_buy_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[decimal.Decimal]:
    """
    Fetches the live *adjusted* buy price for a given symbol, using group-specific cache.
    Falls back to raw Firebase in-memory market data if Redis cache is cold.

    Cache Key Format: adjusted_market_price:{group}:{symbol}
    Value: {"buy": "1.12345", "sell": "...", "spread_value": "..."}
    """
    cache_key = f"adjusted_market_price:{user_group_name}:{symbol.upper()}"
    try:
        cached_data_json = await redis_client.get(cache_key)
        if cached_data_json:
            price_data = json.loads(cached_data_json)
            buy_price_str = price_data.get("buy")
            if buy_price_str and isinstance(buy_price_str, (str, int, float)):
                return decimal.Decimal(str(buy_price_str))
            else:
                logger.warning(f"'buy' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted buy price found for key: {cache_key}")
    except (json.JSONDecodeError, decimal.InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try raw Firebase price ---
    try:
        fallback_data = get_latest_market_data(symbol)
        # For BUY price, typically use the 'offer' or 'ask' price from market data ('o' in your Firebase structure)
        if fallback_data and 'o' in fallback_data:
            logger.warning(f"Fallback: Using raw Firebase 'o' price for {symbol}")
            return decimal.Decimal(str(fallback_data['o']))
        else:
            logger.warning(f"Fallback: No 'o' price found in Firebase for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from Firebase for {symbol}: {fallback_error}", exc_info=True)

    return None

async def get_live_adjusted_sell_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[decimal.Decimal]:
    """
    Fetches the live *adjusted* sell price for a given symbol, using group-specific cache.
    Falls back to raw Firebase in-memory market data if Redis cache is cold.

    Cache Key Format: adjusted_market_price:{group}:{symbol}
    Value: {"buy": "1.12345", "sell": "...", "spread_value": "..."}
    """
    cache_key = f"adjusted_market_price:{user_group_name}:{symbol.upper()}"
    try:
        cached_data_json = await redis_client.get(cache_key)
        if cached_data_json:
            price_data = json.loads(cached_data_json)
            sell_price_str = price_data.get("sell")
            if sell_price_str and isinstance(sell_price_str, (str, int, float)):
                return decimal.Decimal(str(sell_price_str))
            else:
                logger.warning(f"'sell' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted sell price found for key: {cache_key}")
    except (json.JSONDecodeError, decimal.InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try raw Firebase price ---
    try:
        fallback_data = get_latest_market_data(symbol)
        # For SELL price, typically use the 'bid' price from market data ('b' in your Firebase structure)
        if fallback_data and 'b' in fallback_data:
            logger.warning(f"Fallback: Using raw Firebase 'b' price for {symbol}")
            return decimal.Decimal(str(fallback_data['b']))
        else:
            logger.warning(f"Fallback: No 'b' price found in Firebase for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from Firebase for {symbol}: {fallback_error}", exc_info=True)

    return None

async def set_group_settings_cache(redis_client: Redis, group_name: str, settings: Dict[str, Any]):
    """
    Stores general group settings in Redis.
    Settings include sending_orders, etc.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for setting group settings cache for group '{group_name}'.")
        return

    key = f"{REDIS_GROUP_SETTINGS_KEY_PREFIX}{group_name.lower()}" # Use lower for consistency
    try:
        settings_serializable = json.dumps(settings, cls=DecimalEncoder)
        await redis_client.set(key, settings_serializable, ex=GROUP_SETTINGS_CACHE_EXPIRY_SECONDS)
        cache_logger.debug(f"Group settings cached for group '{group_name}'.")
    except Exception as e:
        cache_logger.error(f"Error setting group settings cache for group '{group_name}': {e}", exc_info=True)

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves general group settings from Redis cache.
    Returns None if no settings found for the specified group.
    
    Expected settings include:
    - sending_orders: str (e.g., 'barclays' or other values)
    - other group-level settings
    """
    if not redis_client:
        cache_logger.warning(f"Redis client not available for getting group settings cache for group '{group_name}'.")
        return None

    key = f"{REDIS_GROUP_SETTINGS_KEY_PREFIX}{group_name.lower()}" # Use lower for consistency
    try:
        settings_json = await redis_client.get(key)
        if settings_json:
            settings = json.loads(settings_json, object_hook=decode_decimal)
            cache_logger.debug(f"Group settings retrieved from cache for group '{group_name}'.")
            return settings
        cache_logger.debug(f"Group settings not found in cache for group '{group_name}'.")
        return None
    except Exception as e:
        cache_logger.error(f"Error getting group settings cache for group '{group_name}': {e}", exc_info=True)
        return None

# --- Last Known Price Cache ---
async def set_last_known_price(redis_client: Redis, symbol: str, price_data: dict):
    """
    Store the last known price data for a symbol in Redis.
    """
    if not redis_client:
        cache_logger.warning(f"Redis client not available for setting last known price for {symbol}.")
        return
    key = f"last_price:{symbol.upper()}"
    try:
        await redis_client.set(key, json.dumps(price_data, cls=DecimalEncoder))
        cache_logger.debug(f"Last known price cached for symbol {symbol}")
    except Exception as e:
        cache_logger.error(f"Error setting last known price for symbol {symbol}: {e}", exc_info=True)

async def get_last_known_price(redis_client: Redis, symbol: str) -> Optional[dict]:
    """
    Retrieve the last known price data for a symbol from Redis.
    """
    if not redis_client:
        cache_logger.warning(f"Redis client not available for getting last known price for {symbol}.")
        return None
    key = f"last_price:{symbol.upper()}"
    try:
        data_json = await redis_client.get(key)
        if data_json:
            data = json.loads(data_json, object_hook=decode_decimal)
            cache_logger.debug(f"Last known price retrieved from cache for symbol {symbol}")
            return data
        return None
    except Exception as e:
        cache_logger.error(f"Error getting last known price for symbol {symbol}: {e}", exc_info=True)
        return None

async def publish_order_update(redis_client: Redis, user_id: int):
    """
    Publishes an event to notify that a user's orders have been updated.
    WebSocket connections can listen to this channel to refresh order data.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for publishing order update for user {user_id}.")
        return

    try:
        message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
        cache_logger.info(f"Published order update for user {user_id} to {REDIS_ORDER_UPDATES_CHANNEL}, received by {result} subscribers")
    except Exception as e:
        logger.error(f"Error publishing order update for user {user_id}: {e}", exc_info=True)

async def publish_user_data_update(redis_client: Redis, user_id: int):
    """
    Publishes an event to notify that a user's data has been updated.
    WebSocket connections can listen to this channel to refresh user data.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for publishing user data update for user {user_id}.")
        return

    try:
        message = json.dumps({
            "type": "USER_DATA_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_USER_DATA_UPDATES_CHANNEL, message)
        cache_logger.info(f"Published user data update for user {user_id} to {REDIS_USER_DATA_UPDATES_CHANNEL}, received by {result} subscribers")
    except Exception as e:
        logger.error(f"Error publishing user data update for user {user_id}: {e}", exc_info=True)

async def publish_market_data_trigger(redis_client: Redis, symbol: str = "TRIGGER"):
    """
    Publishes a market data trigger event to force recalculation of dynamic portfolio metrics.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for publishing market data trigger.")
        return

    try:
        message = json.dumps({
            "type": "market_data_update",
            "symbol": symbol,
            "b": "0",
            "o": "0",
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
        cache_logger.info(f"Published market data trigger for symbol {symbol} to {REDIS_MARKET_DATA_CHANNEL}, received by {result} subscribers")
    except Exception as e:
        logger.error(f"Error publishing market data trigger: {e}", exc_info=True)