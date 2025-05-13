# app/core/cache.py

import json
import logging
from typing import Dict, Any, Optional, List
from redis.asyncio import Redis
import decimal # Import Decimal for type hinting and serialization

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Keys for storing data in Redis
REDIS_USER_DATA_KEY_PREFIX = "user_data:" # Stores group_name, leverage, etc.
REDIS_USER_PORTFOLIO_KEY_PREFIX = "user_portfolio:" # Stores balance, positions
# New key prefix for group settings per symbol
REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX = "group_symbol_settings:" # Stores spread, pip values, etc. per group and symbol

# Expiry times (adjust as needed)
USER_DATA_CACHE_EXPIRY_SECONDS = 7 * 24 * 60 * 60 # Example: User session length
# USER_DATA_CACHE_EXPIRY_SECONDS = 10
USER_PORTFOLIO_CACHE_EXPIRY_SECONDS = 5 * 60 # Example: Short expiry, updated frequently
GROUP_SYMBOL_SETTINGS_CACHE_EXPIRY_SECONDS = 30 * 24 * 60 * 60 # Example: Group settings change infrequently


# Helper function to handle Decimal serialization/deserialization (keep as is)
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
        logger.warning(f"Redis client not available for setting user data cache for user {user_id}.")
        return

    key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
    try:
        data_serializable = json.dumps(data, cls=DecimalEncoder)
        await redis_client.set(key, data_serializable, ex=USER_DATA_CACHE_EXPIRY_SECONDS)
        logger.debug(f"User data cached for user {user_id}")
    except Exception as e:
        logger.error(f"Error setting user data cache for user {user_id}: {e}", exc_info=True)

async def get_user_data_cache(redis_client: Redis, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves user data from Redis cache. Expected data includes 'group_name'.
    """
    if not redis_client:
        logger.warning(f"Redis client not available for getting user data cache for user {user_id}.")
        return None

    key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
    try:
        data_json = await redis_client.get(key)
        if data_json:
            data = json.loads(data_json, object_hook=decode_decimal)
            logger.debug(f"User data retrieved from cache for user {user_id}")
            return data
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
        logger.debug(f"User portfolio cached for user {user_id}")
    except Exception as e:
        logger.error(f"Error setting user portfolio cache for user {user_id}: {e}", exc_info=True)


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
            logger.debug(f"User portfolio retrieved from cache for user {user_id}")
            return portfolio_data
        return None
    except Exception as e:
        logger.error(f"Error getting user portfolio cache for user {user_id}: {e}", exc_info=True)
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
                 logger.debug(f"Aggregated {len(all_settings)} group-symbol settings for group '{group_name}'.")
                 return all_settings
            else:
                 logger.debug(f"No group-symbol settings found for group '{group_name}' using scan.")
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
                logger.debug(f"Group-symbol settings retrieved from cache for group '{group_name}', symbol '{symbol}'.")
                return settings
            logger.debug(f"Group-symbol settings not found in cache for group '{group_name}', symbol '{symbol}'.")
            return None # Return None if settings for the specific symbol are not found
        except Exception as e:
            logger.error(f"Error getting group-symbol settings cache for group '{group_name}', symbol '{symbol}': {e}", exc_info=True)
            return None

# You might also want a function to cache ALL settings for a group,
# or cache ALL group-symbol settings globally if the dataset is small enough.
# For now, fetching per symbol on demand from cache/DB is a good start.

# Add these functions to your app/core/cache.py file

# New key prefix for adjusted market prices per group and symbol
REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX = "adjusted_market_price:"

# Example expiry for adjusted market prices (adjust based on how frequently data updates)
ADJUSTED_MARKET_PRICE_CACHE_EXPIRY_SECONDS = 10 # Example: Cache for 10 seconds

async def set_adjusted_market_price_cache(
    redis_client: Redis,
    group_name: str,
    symbol: str,
    buy_price: decimal.Decimal,
    sell_price: decimal.Decimal,
    spread_value: decimal.Decimal # Include spread value as well
):
    """
    Caches the adjusted market buy and sell prices (and spread value)
    for a specific group and symbol in Redis.
    Key structure: adjusted_market_price:{group_name}:{symbol}
    Value is a JSON string: {"buy": "...", "sell": "...", "spread_value": "..."}
    """
    key = f"{REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}" # Use lower/upper for consistency
    try:
        # Create a dictionary with Decimal values
        adjusted_prices_data = {
            "buy": buy_price,
            "sell": sell_price,
            "spread_value": spread_value # Store spread value
        }
        # Serialize the dictionary to a JSON string, using DecimalEncoder to handle Decimal types
        adjusted_prices_json = json.dumps(adjusted_prices_data, cls=DecimalEncoder)

        # Use setex to set the key with an expiry time
        await redis_client.setex(
            key,
            ADJUSTED_MARKET_PRICE_CACHE_EXPIRY_SECONDS,
            adjusted_prices_json
        )
        logger.debug(f"Cached adjusted market price for group '{group_name}', symbol '{symbol}'. Key: {key}")

    except Exception as e:
        logger.error(f"Error caching adjusted market price for group '{group_name}', symbol '{symbol}': {e}", exc_info=True)

async def get_adjusted_market_price_cache(
    redis_client: Redis,
    group_name: str,
    symbol: str
) -> Optional[Dict[str, decimal.Decimal]]:
    """
    Retrieves the cached adjusted market buy and sell prices (and spread value)
    for a specific group and symbol from Redis.
    Returns a dictionary {"buy": Decimal, "sell": Decimal, "spread_value": Decimal} or None if not found/error.
    """
    key = f"{REDIS_ADJUSTED_MARKET_PRICE_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}" # Use lower/upper for consistency
    try:
        adjusted_prices_json = await redis_client.get(key)
        if adjusted_prices_json:
            # Decode the JSON string, using decode_decimal to convert strings back to Decimal
            adjusted_prices_data = json.loads(adjusted_prices_json, object_hook=decode_decimal)

            # Ensure the expected keys are present and have Decimal values
            # Check if it's a dictionary and contains 'buy', 'sell', and 'spread_value' keys with Decimal values
            if isinstance(adjusted_prices_data, dict) and \
               'buy' in adjusted_prices_data and isinstance(adjusted_prices_data['buy'], decimal.Decimal) and \
               'sell' in adjusted_prices_data and isinstance(adjusted_prices_data['sell'], decimal.Decimal) and \
               'spread_value' in adjusted_prices_data and isinstance(adjusted_prices_data['spread_value'], decimal.Decimal):
                logger.debug(f"Adjusted market price retrieved from cache for group '{group_name}', symbol '{symbol}'.")
                return adjusted_prices_data
            else:
                 logger.warning(f"Cached data for key '{key}' is not in expected format: {adjusted_prices_data}")
                 return None

        logger.debug(f"Adjusted market price not found in cache for group '{group_name}', symbol '{symbol}'. Key: {key}")
        return None # Return None if the key is not found

    except Exception as e:
        logger.error(f"Error retrieving adjusted market price cache for group '{group_name}', symbol '{symbol}': {e}", exc_info=True)
        return None # Return None on error