# app/services/margin_calculator.py

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import logging
from typing import Optional, Tuple, Dict, Any
import json # Import json

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload # For eager loading if needed
from redis.asyncio import Redis

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache, # To cache if fetched from DB
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache, # To cache if fetched from DB
    DecimalEncoder # For serializing Decimal to JSON for cache
)
from app.firebase_stream import get_latest_market_data # Import for fallback market data

logger = logging.getLogger(__name__)

async def get_live_adjusted_buy_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[Decimal]:
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
                return Decimal(str(buy_price_str))
            else:
                logger.warning(f"'buy' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted buy price found for key: {cache_key}")
    except (json.JSONDecodeError, InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try raw Firebase price ---
    try:
        fallback_data = get_latest_market_data(symbol)
        # For BUY price, typically use the 'offer' or 'ask' price from market data ('o' in your Firebase structure)
        if fallback_data and 'o' in fallback_data:
            logger.warning(f"Fallback: Using raw Firebase 'o' price for {symbol}")
            return Decimal(str(fallback_data['o']))
        else:
            logger.warning(f"Fallback: No 'o' price found in Firebase for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from Firebase for {symbol}: {fallback_error}", exc_info=True)

    return None

async def get_live_adjusted_sell_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[Decimal]:
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
                return Decimal(str(sell_price_str))
            else:
                logger.warning(f"'sell' price not found or invalid in cache for {cache_key}: {price_data}")
        else:
            logger.warning(f"No cached adjusted sell price found for key: {cache_key}")
    except (json.JSONDecodeError, InvalidOperation) as e:
        logger.error(f"Error decoding cached data for {cache_key}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error accessing Redis for {cache_key}: {e}", exc_info=True)

    # --- Fallback: Try raw Firebase price ---
    try:
        fallback_data = get_latest_market_data(symbol)
        # For SELL price, typically use the 'bid' price from market data ('b' in your Firebase structure)
        if fallback_data and 'b' in fallback_data:
            logger.warning(f"Fallback: Using raw Firebase 'b' price for {symbol}")
            return Decimal(str(fallback_data['b']))
        else:
            logger.warning(f"Fallback: No 'b' price found in Firebase for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from Firebase for {symbol}: {fallback_error}", exc_info=True)

    return None


# --- HELPER FUNCTION: Calculate Base Margin Per Lot (Used in Hedging) ---
# This helper calculates a per-lot value based on Group margin setting, price, and leverage.
# This is used in the hedging logic in order_processing.py for comparison.
async def calculate_base_margin_per_lot(
    redis_client: Redis,
    user_id: int,
    symbol: str,
    price: Decimal # Price at which margin per lot is calculated (order price or current market price)
) -> Optional[Decimal]:
    """
    Calculates a base margin value per standard lot for a given symbol and price,
    considering user's group settings and leverage.
    This value is used for comparison in hedging calculations.
    Returns the base margin value per lot or None if calculation fails.
    """
    # Retrieve user data from cache to get group_name and leverage
    user_data = await get_user_data_cache(redis_client, user_id)
    if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
        logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
        return None

    group_name = user_data['group_name']
    # Ensure user_leverage is Decimal
    user_leverage_raw = user_data.get('leverage', 1)
    user_leverage = Decimal(str(user_leverage_raw)) if user_leverage_raw is not None else Decimal(1)


    # Retrieve group-symbol settings from cache
    # Need settings for the specific symbol
    group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
    # We need the 'margin' setting from the group for this calculation
    if not group_symbol_settings or 'margin' not in group_symbol_settings:
        logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
        return None

    # Ensure margin_setting is Decimal
    margin_setting_raw = group_symbol_settings.get('margin', 0)
    margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


    if user_leverage <= 0:
         logger.error(f"User leverage is zero or negative for user {user_id}.")
         return None

    # Calculation based on Group Base Margin Setting, Price, and Leverage
    # This formula seems to be the one needed for the per-lot comparison in hedging.
    try:
        # Ensure price is Decimal
        price_decimal = Decimal(str(price))
        base_margin_per_lot = (margin_setting * price_decimal) / user_leverage
        logger.debug(f"Calculated base margin per lot (for hedging) for user {user_id}, symbol {symbol}, price {price}: {base_margin_per_lot}")
        return base_margin_per_lot
    except Exception as e:
        logger.error(f"Error calculating base margin per lot (for hedging) for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None


# async def calculate_single_order_margin(
#     db: AsyncSession,
#     redis_client: Redis,
#     user_id: int,
#     order_quantity: Decimal,
#     order_price: Decimal, # Original price from user or market
#     symbol: str,
#     order_type: str, # "BUY" or "SELL"
# ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
#     """
#     Calculates the margin, adjusted price, and contract value for a single order,
#     calculating margin in the symbol's profit currency and converting to USD.

#     Returns:
#         A tuple: (calculated_margin_usd, adjusted_order_price, contract_value_for_order)
#         Returns (None, None, None) if calculation fails at any critical step.
#     """
#     try:
#         # 1. Fetch User data (leverage, group_name) - Try cache first
#         user_data_from_cache = await get_user_data_cache(redis_client, user_id)
#         user_leverage: Optional[Decimal] = None
#         user_group_name: Optional[str] = None

#         if user_data_from_cache:
#             logger.debug(f"User data found in cache for user_id {user_id}")
#             try:
#                 user_leverage = Decimal(str(user_data_from_cache.get('leverage')))
#                 user_group_name = user_data_from_cache.get('group_name')
#             except (InvalidOperation, TypeError) as e:
#                 logger.warning(f"Could not parse leverage from cached user data for user {user_id}: {e}. Will fetch from DB.")
#                 user_data_from_cache = None # Force DB fetch

#         if not user_data_from_cache: # Not in cache or parsing failed
#             logger.debug(f"User data not in cache or cache parse failed for user_id {user_id}. Fetching from DB.")
#             user_db = await db.get(User, user_id)
#             if not user_db:
#                 logger.error(f"User not found in DB for ID: {user_id}")
#                 return None, None, None
#             user_leverage = user_db.leverage
#             user_group_name = user_db.group_name
#             # Cache it for next time
#             await set_user_data_cache(redis_client, user_id, {
#                 "leverage": str(user_leverage), # Store as string for consistency
#                 "group_name": user_group_name,
#                 "id": user_id, # Include other relevant fields as in market_data_ws.py
#                 "margin": str(user_db.margin), # Cache the user's overall margin too
#                 "wallet_balance": str(user_db.wallet_balance) # Cache wallet balance too
#             })
#             logger.debug(f"User data for {user_id} fetched from DB and cached.")


#         if not user_leverage or user_leverage <= 0:
#             logger.error(f"Invalid leverage for user ID {user_id}: {user_leverage}")
#             return None, None, None
#         if not user_group_name:
#             logger.error(f"User {user_id} does not have an assigned group name.")
#             return None, None, None

#         # 2. Fetch Group settings (spread, spread_pip) for the user's group and symbol - Try cache first
#         # We need spread and spread_pip for adjusted price calculation.
#         group_symbol_settings_cache_key = symbol.upper() # Key within the group's settings hash
#         group_settings_from_cache = await get_group_symbol_settings_cache(redis_client, user_group_name, group_symbol_settings_cache_key)

#         spread: Optional[Decimal] = None
#         spread_pip: Optional[Decimal] = None

#         if group_settings_from_cache and isinstance(group_settings_from_cache, dict):
#             logger.debug(f"Group settings for group '{user_group_name}', symbol '{symbol}' found in cache.")
#             try:
#                 spread_str = group_settings_from_cache.get('spread')
#                 spread_pip_str = group_settings_from_cache.get('spread_pip')
#                 if spread_str is not None and spread_pip_str is not None:
#                     spread = Decimal(str(spread_str))
#                     spread_pip = Decimal(str(spread_pip_str))
#                 else:
#                     logger.warning(f"Spread/Spread_pip missing in cached group settings for {user_group_name}/{symbol}. Data: {group_settings_from_cache}")
#             except (InvalidOperation, TypeError) as e:
#                 logger.warning(f"Could not parse spread/spread_pip from cached group settings for {user_group_name}/{symbol}: {e}. Will fetch from DB.")
#                 # Invalidate this specific cache entry or re-fetch all group settings if parse fails
#                 await redis_client.hdel(f"group_symbol_settings:{user_group_name}", group_symbol_settings_cache_key)
#                 logger.info(f"Invalidated cache for group '{user_group_name}', symbol '{symbol}' due to parse failure.")
#                 group_settings_from_cache = None # Force DB fetch for this symbol's group settings

#         if spread is None or spread_pip is None: # Not in cache or parsing failed
#             logger.debug(f"Group settings for '{user_group_name}/{symbol}' not in cache or cache parse failed. Fetching from DB.")
#             group_stmt = select(Group).filter_by(name=user_group_name, symbol=symbol)
#             group_result = await db.execute(group_stmt)
#             group_settings_db = group_result.scalars().first()

#             if not group_settings_db:
#                 logger.error(f"No group settings found in DB for group '{user_group_name}' and symbol '{symbol}'.")
#                 return None, None, None

#             spread = group_settings_db.spread
#             spread_pip = group_settings_db.spread_pip

#             # Cache it for next time (cache the specific symbol's settings)
#             # Ensure all relevant fields from the Group model for this symbol are cached.
#             settings_to_cache = {
#                 "spread": str(spread),
#                 "spread_pip": str(spread_pip),
#                 # Include other relevant fields from Group model if needed for caching
#                 "commision_type": group_settings_db.commision_type,
#                 "commision_value_type": group_settings_db.commision_value_type,
#                 "type": group_settings_db.type,
#                 "pip_currency": group_settings_db.pip_currency,
#                 "show_points": group_settings_db.show_points,
#                 "swap_buy": str(group_settings_db.swap_buy),
#                 "swap_sell": str(group_settings_db.swap_sell),
#                 "commision": str(group_settings_db.commision),
#                 "margin": str(group_settings_db.margin), # Cache the group's margin setting
#                 "deviation": str(group_settings_db.deviation),
#                 "min_lot": str(group_settings_db.min_lot),
#                 "max_lot": str(group_settings_db.max_lot),
#                 "pips": str(group_settings_db.pips),
#                 "spread_pip": str(group_settings_db.spread_pip), # Ensure spread_pip is cached
#             }
#             await set_group_symbol_settings_cache(redis_client, user_group_name, symbol.upper(), settings_to_cache)
#             logger.debug(f"Group settings for '{user_group_name}/{symbol}' fetched from DB and cached.")


#         if spread is None or spread_pip is None: # Final check for required settings
#              logger.error(f"Spread or spread_pip is not set or could not be parsed for group '{user_group_name}' and symbol '{symbol}'.")
#              return None, None, None


#         # 3. Fetch ExternalSymbolInfo (contract_size, profit_currency, digit) - This is static, could be cached too
#         # We need contract_size for contract value, profit_currency for conversion, and digit for rounding.
#         symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol)) # Case-insensitive
#         symbol_info_result = await db.execute(symbol_info_stmt)
#         ext_symbol_info = symbol_info_result.scalars().first()

#         if not ext_symbol_info or not ext_symbol_info.contract_size or not ext_symbol_info.profit:
#             logger.error(f"ExternalSymbolInfo, contract_size, or profit currency not found for symbol: {symbol}")
#             return None, None, None
#         contract_size = ext_symbol_info.contract_size
#         profit_currency = ext_symbol_info.profit.upper() # Profit currency is required now
#         symbol_digits = ext_symbol_info.digit # Number of decimal places for the symbol's price


#         # 4. Calculate half_spread
#         spread_value = spread * spread_pip
#         half_spread = spread_value / Decimal(2)

#         # 5. Calculate contract_value for the order
#         # Contract value is typically quantity * contract size * price
#         contract_value_for_order = order_quantity * contract_size * order_price # Use the original order price

#         # 6. Adjust order_price based on order_type
#         # This is the price stored with the order record.
#         adjusted_order_price: Decimal
#         if order_type.upper() == "BUY":
#              adjusted_order_price = order_price + half_spread # Assuming execution price is order_price + half_spread
#         elif order_type.upper() == "SELL":
#              adjusted_order_price = order_price - half_spread # Assuming execution price is order_price - half_spread
#         else:
#             logger.error(f"Invalid order_type: {order_type}")
#             return None, None, None

#         # 7. Calculate margin in the symbol's profit currency
#         # Assuming the formula (Quantity * Contract Size * Order Price) / Leverage results in the profit currency.
#         if user_leverage <= 0:
#              logger.error(f"User leverage is zero or negative for user {user_id}.")
#              return None, None, None

#         # Calculate margin amount in the profit currency
#         margin_amount_in_profit_currency = (order_quantity * contract_size * order_price) / user_leverage
#         logger.debug(f"Calculated margin amount in profit currency ({profit_currency}): {margin_amount_in_profit_currency}")


#         # 8. Convert margin to USD if profit_currency is not USD
#         calculated_margin_usd: Decimal
#         if profit_currency == "USD":
#             calculated_margin_usd = margin_amount_in_profit_currency
#             logger.debug("Profit currency is USD, no conversion needed for margin.")
#         else:
#             # Need to convert from profit_currency to USD
#             # Look for conversion pair USD{profit_currency} or {profit_currency}USD
#             conversion_pair_option1 = f"USD{profit_currency}"
#             conversion_pair_option2 = f"{profit_currency}USD"
#             live_buy_price_conversion_pair: Optional[Decimal] = None
#             is_usd_base = False # Flag to indicate if USD is the base in the conversion pair

#             # Get live ADJUSTED buy price for the conversion pair, considering the USER'S group for that pair.
#             # This assumes conversion pairs (e.g., USDCAD) are also configured with group settings.
#             live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option1, user_group_name)
#             if live_buy_price_conversion_pair and live_buy_price_conversion_pair > 0:
#                 is_usd_base = True
#                 logger.debug(f"Found conversion pair {conversion_pair_option1} with price {live_buy_price_conversion_pair}")
#             else:
#                 live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option2, user_group_name)
#                 if not (live_buy_price_conversion_pair and live_buy_price_conversion_pair > 0) :
#                     logger.error(f"Could not find live ADJUSTED buy price for conversion pairs: {conversion_pair_option1} or {conversion_pair_option2} for group '{user_group_name}'")
#                     return None, None, None
#                 logger.debug(f"Found conversion pair {conversion_pair_option2} with price {live_buy_price_conversion_pair}")
#                 # is_usd_base remains False

#             if is_usd_base: # Pair is USDXXX (e.g., USDCAD), price is XXX per USD. We have margin in XXX (profit_currency).
#                             # We have XXX margin, want USD. Price is XXX/USD. So, XXX_margin / (XXX/USD) = USD_margin
#                 calculated_margin_usd = margin_amount_in_profit_currency / live_buy_price_conversion_pair
#                 logger.debug(f"Converting from {profit_currency} to USD using division by {conversion_pair_option1} price.")
#             else: # Pair is XXXUSD (e.g., AUDUSD, but here profit_currency is XXX, so example CADUSD)
#                   # Price is USD per XXX. We have margin in XXX (profit_currency).
#                   # We have XXX margin, want USD. Price is USD/XXX. So, XXX_margin * (USD/XXX) = USD_margin
#                 calculated_margin_usd = margin_amount_in_profit_currency * live_buy_price_conversion_pair
#                 logger.debug(f"Converting from {profit_currency} to USD using multiplication by {conversion_pair_option2} price.")


#         # 9. Quantize results
#         # Precision for the original symbol's price and contract value
#         if symbol_digits is not None and symbol_digits >= 0:
#             price_precision_str = '1e-' + str(int(symbol_digits))
#         else:
#             price_precision_str = '0.00001' # Default to 5 decimal places if not specified
#         price_precision = Decimal(price_precision_str)

#         # Precision for USD margin - typically 2 decimal places, but your output showed 8.
#         # Let's use 8 for consistency with your previous output and wallet_balance.
#         usd_precision = Decimal('0.00000001') # Use 8 decimal places for USD margin

#         calculated_margin_usd = calculated_margin_usd.quantize(usd_precision, rounding=ROUND_HALF_UP)
#         adjusted_order_price = adjusted_order_price.quantize(price_precision, rounding=ROUND_HALF_UP)
#         # Contract value precision can be higher, or based on symbol's currency norms
#         contract_value_precision = Decimal('0.00000001') # Example: 8 decimal places for contract value
#         contract_value_for_order = contract_value_for_order.quantize(contract_value_precision, rounding=ROUND_HALF_UP)


#         logger.info(f"Margin calculated for user {user_id}, symbol {symbol}, order_type {order_type}: "
#                     f"Margin_USD={calculated_margin_usd}, Adj_Price={adjusted_order_price}, Contract_Val={contract_value_for_order}, "
#                     f"User_Leverage={user_leverage}, User_Group='{user_group_name}', Spread_Val={spread_value}, Half_Spread={half_spread}, "
#                     f"Contract_Size={contract_size}, Profit_Currency={profit_currency}") # Included Profit_Currency in log
#         return calculated_margin_usd, adjusted_order_price, contract_value_for_order

#     except InvalidOperation as e: # Catch Decimal conversion errors explicitly
#         logger.error(f"Decimal operation error during margin calculation for user {user_id}, symbol {symbol}: {e}", exc_info=True)
#         return None, None, None
#     except Exception as e:
#         logger.error(f"Unexpected error during margin calculation for user {user_id}, symbol {symbol}: {e}", exc_info=True)
#         return None, None, None

# app/services/margin_calculator.py

import asyncio
import json
import logging
import threading
import decimal # Import decimal
import time # Import time for timestamping
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation # Import InvalidOperation

from firebase_admin import db
from firebase_admin.db import Event

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis.asyncio import Redis
from typing import Optional, Tuple, Dict, Any

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache,
    DecimalEncoder,
    decode_decimal,
    get_adjusted_market_price_cache
)
from app.firebase_stream import get_latest_market_data

logger = logging.getLogger(__name__)

# ... (Keep existing helper functions get_live_adjusted_buy_price_for_pair and get_live_adjusted_sell_price_for_pair) ...


async def calculate_single_order_margin(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    order_quantity: Decimal,
    order_price: Decimal, # Original price from user or market
    symbol: str,
    order_type: str, # "BUY" or "SELL"
) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Calculates the margin, adjusted price, and contract value for a single order,
    calculating margin in the symbol's profit currency and converting to USD.

    Returns:
        A tuple: (calculated_margin_usd, adjusted_order_price, contract_value_for_order)
        Returns (None, None, None) if calculation fails at any critical step.
    """
    try:
        # 1. Fetch User data (leverage, group_name) - Try cache first, fallback to DB
        user_data_from_cache = await get_user_data_cache(redis_client, user_id)
        user_leverage: Optional[Decimal] = None
        user_group_name: Optional[str] = None

        if user_data_from_cache:
            logger.debug(f"User data found in cache for user_id {user_id}")
            cached_leverage_raw = user_data_from_cache.get('leverage')
            cached_group_name = user_data_from_cache.get('group_name')

            if cached_leverage_raw is not None:
                try:
                    user_leverage = Decimal(str(cached_leverage_raw))
                    # Check if leverage is non-positive even if parsed
                    if user_leverage <= 0:
                         user_leverage = None # Treat non-positive leverage from cache as invalid
                         logger.warning(f"Cached leverage for user {user_id} is zero or negative ({cached_leverage_raw}). Will fetch from DB.")
                except (InvalidOperation, TypeError):
                    logger.warning(f"Could not parse leverage '{cached_leverage_raw}' from cached user data for user {user_id}. Will fetch from DB.")

            user_group_name = cached_group_name # Get group_name from cache

        # Fallback to DB if leverage is missing/invalid or group_name is missing
        if user_leverage is None or user_group_name is None:
             logger.debug(f"Leverage missing/invalid ({user_leverage}) or group_name missing ({user_group_name}) from cache for user_id {user_id}. Fetching user data from DB.")

             user_db = await db.get(User, user_id)
             if not user_db:
                 logger.error(f"User not found in DB for ID: {user_id}")
                 return None, None, None

             user_leverage = user_db.leverage # Decimal directly from DB
             user_group_name = user_db.group_name # String directly from DB

             # Re-cache the user data from DB (ensure Decimal values are strings)
             await set_user_data_cache(redis_client, user_id, {
                 "id": user_id,
                 "group_name": user_group_name,
                 "leverage": str(user_leverage) if user_leverage is not None else None,
                 "margin": str(user_db.margin) if user_db.margin is not None else None,
                 "wallet_balance": str(user_db.wallet_balance) if user_db.wallet_balance is not None else None,
                 "status": user_db.status,
                 "isActive": user_db.isActive,
                 "user_type": user_db.user_type,
                 "account_number": user_db.account_number,
             })
             logger.debug(f"User data for {user_id} fetched from DB and re-cached.")
        else:
             logger.debug(f"User data (leverage={user_leverage}, group_name='{user_group_name}') used from cache for user_id {user_id}.")

        # Final Check for required user data AFTER attempting fetch from cache/DB
        if user_leverage is None or user_leverage <= 0:
             logger.error(f"Invalid, zero, or negative leverage ({user_leverage}) for user ID {user_id} after attempting fetch from cache/DB.")
             return None, None, None
        if user_group_name is None:
             logger.error(f"User {user_id} does not have an assigned group name after attempting fetch from cache/DB.")
             return None, None, None


        # 2. Fetch Group settings (spread, spread_pip) - Try cache first, fallback to DB
        group_symbol_settings_cache_key = symbol.upper()
        group_settings_from_cache = await get_group_symbol_settings_cache(redis_client, user_group_name, group_symbol_settings_cache_key)

        spread: Optional[Decimal] = None
        spread_pip: Optional[Decimal] = None

        if group_settings_from_cache and isinstance(group_settings_from_cache, dict):
            logger.debug(f"Group settings for group '{user_group_name}', symbol '{symbol}' found in cache.")
            cached_spread_raw = group_settings_from_cache.get('spread')
            cached_spread_pip_raw = group_settings_from_cache.get('spread_pip')

            if cached_spread_raw is not None and cached_spread_pip_raw is not None:
                 try:
                     spread = Decimal(str(cached_spread_raw))
                     spread_pip = Decimal(str(cached_spread_pip_raw))
                 except (InvalidOperation, TypeError) as e:
                     logger.warning(f"Could not parse spread/spread_pip from cached group settings for {user_group_name}/{symbol}: {e}. Will fetch from DB.")
                     spread = None # Reset to None to force DB fetch
                     spread_pip = None # Reset to None to force DB fetch
            else:
                 logger.warning(f"Spread/Spread_pip missing in cached group settings for {user_group_name}/{symbol}. Data: {group_settings_from_cache}. Will fetch from DB.")

        if spread is None or spread_pip is None: # Not successfully retrieved/parsed from cache
            logger.debug(f"Group settings for '{user_group_name}/{symbol}' not in cache, cache parse failed, or missing fields. Fetching from DB.")
            group_stmt = select(Group).filter_by(name=user_group_name, symbol=symbol)
            group_result = await db.execute(group_stmt)
            group_settings_db = group_result.scalars().first()

            if not group_settings_db:
                logger.error(f"No group settings found in DB for group '{user_group_name}' and symbol '{symbol}'. Cannot calculate margin.")
                return None, None, None

            spread = group_settings_db.spread # Decimal directly from DB
            spread_pip = group_settings_db.spread_pip # Decimal directly from DB

            # Re-cache group settings from DB (ensure Decimal values are strings)
            settings_to_cache = {
                "spread": str(spread) if spread is not None else None,
                "spread_pip": str(spread_pip) if spread_pip is not None else None,
                "commision_type": group_settings_db.commision_type,
                "commision_value_type": group_settings_db.commision_value_type,
                "type": group_settings_db.type,
                "pip_currency": group_settings_db.pip_currency,
                "show_points": group_settings_db.show_points,
                "swap_buy": str(group_settings_db.swap_buy) if group_settings_db.swap_buy is not None else None,
                "swap_sell": str(group_settings_db.swap_sell) if group_settings_db.swap_sell is not None else None,
                "commision": str(group_settings_db.commision) if group_settings_db.commision is not None else None,
                "margin": str(group_settings_db.margin) if group_settings_db.margin is not None else None,
                "deviation": str(group_settings_db.deviation) if group_settings_db.deviation is not None else None,
                "min_lot": str(group_settings_db.min_lot) if group_settings_db.min_lot is not None else None,
                "max_lot": str(group_settings_db.max_lot) if group_settings_db.max_lot is not None else None,
                "pips": str(group_settings_db.pips) if group_settings_db.pips is not None else None,
                 # spread_pip is already handled above
            }
            await set_group_symbol_settings_cache(redis_client, user_group_name, symbol.upper(), settings_to_cache)
            logger.debug(f"Group settings for '{user_group_name}/{symbol}' fetched from DB and cached.")
        else:
             logger.debug(f"Group settings (spread={spread}, spread_pip={spread_pip}) used from cache for group '{user_group_name}', symbol '{symbol}'.")

        # Final check for required group settings AFTER attempting fetch from cache/DB
        # Allow zero, but not None or negative
        if spread is None:
             logger.error(f"Group setting 'spread' is missing (None) for group '{user_group_name}' and symbol '{symbol}'. Cannot calculate margin.")
             return None, None, None
        if spread < 0: # Changed from <= 0 to < 0
             logger.error(f"Group setting 'spread' ({spread}) is negative for group '{user_group_name}' and symbol '{symbol}'. Cannot calculate margin.")
             return None, None, None

        if spread_pip is None:
             logger.error(f"Group setting 'spread_pip' is missing (None) for group '{user_group_name}' and symbol '{symbol}'. Cannot calculate margin.")
             return None, None, None
        if spread_pip < 0: # Changed from <= 0 to < 0
             logger.error(f"Group setting 'spread_pip' ({spread_pip}) is negative for group '{user_group_name}' and symbol '{symbol}'. Cannot calculate margin.")
             return None, None, None


        # 3. Fetch ExternalSymbolInfo (contract_size, profit_currency, digit) - From DB
        # ... (Existing logic - This part seems correct) ...
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()

        if not ext_symbol_info or not ext_symbol_info.contract_size or not ext_symbol_info.profit:
            logger.error(f"ExternalSymbolInfo, contract_size, or profit currency not found/valid for symbol: {symbol}. Cannot calculate margin.")
            return None, None, None
        contract_size = ext_symbol_info.contract_size
        if contract_size is None or contract_size <= 0: # Contract size must be positive
             logger.error(f"Contract size ({contract_size}) is missing, zero, or negative for symbol: {symbol}. Cannot calculate margin.")
             return None, None, None

        profit_currency = ext_symbol_info.profit.upper()
        symbol_digits = ext_symbol_info.digit # This can be None


        # 4. Calculate half_spread (Will be 0 if spread or spread_pip is 0)
        spread_value = spread * spread_pip
        half_spread = spread_value / Decimal(2)

        # 5. Calculate contract_value for the order (Matches your description, uses original price)
        if order_quantity <= 0:
             logger.error(f"Order quantity ({order_quantity}) is zero or negative for symbol {symbol}. Cannot calculate margin.")
             return None, None, None
        if order_price is None or order_price <= 0: # Ensure order_price is not None and positive
             logger.error(f"Order price ({order_price}) is missing, zero, or negative for symbol {symbol}. Cannot calculate margin.")
             return None, None, None
        contract_value_for_order = order_quantity * contract_size # Use the original order price


        # 6. Adjust order_price based on order_type (Will be original price if half_spread is 0)
        # This is the price stored with the order record.
        adjusted_order_price: Decimal
        if order_type.upper() == "BUY":
             adjusted_order_price = order_price + half_spread
        elif order_type.upper() == "SELL":
             adjusted_order_price = order_price - half_spread
        else:
            logger.error(f"Invalid order_type: {order_type}. Cannot calculate margin.")
            return None, None, None
        logger.debug(f"Original Order Price: {order_price}, Calculated Half Spread: {half_spread}, Adjusted Order Price ({order_type}): {adjusted_order_price}")

        # Add check for adjusted_order_price <= 0 if that's invalid after adjustment
        # This check remains important even if half_spread is 0 if the original price was close to 0
        if adjusted_order_price is None or adjusted_order_price <= 0:
             logger.error(f"Adjusted order price ({adjusted_order_price}) is zero or negative for symbol {symbol}. Cannot calculate margin.")
             return None, None, None


        # 7. Calculate margin in the symbol's profit currency (Matches your description, uses original price)
        # Formula: (Quantity * Contract Size * Order Price) / Leverage
        # Leverage, quantity, contract_size, order_price checks added above.
        margin_amount_in_profit_currency = (order_quantity * contract_size * order_price) / user_leverage
        logger.debug(f"Calculated margin amount in profit currency ({profit_currency}): {margin_amount_in_profit_currency}")


        # 8. Convert margin to USD if profit_currency is not USD (Matches your description, uses live adjusted price)
        calculated_margin_usd: Decimal
        if profit_currency == "USD":
            calculated_margin_usd = margin_amount_in_profit_currency
            logger.debug("Profit currency is USD, no conversion needed for margin.")
        else:
            # ... (Existing conversion logic using get_live_adjusted_buy_price_for_pair) ...
            conversion_pair_option1 = f"USD{profit_currency}"
            conversion_pair_option2 = f"{profit_currency}USD"
            live_buy_price_conversion_pair: Optional[Decimal] = None
            is_usd_base = False

            live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option1, user_group_name)
            if live_buy_price_conversion_pair is not None and live_buy_price_conversion_pair > 0: # Check None and positive
                is_usd_base = True
                logger.debug(f"Found conversion pair {conversion_pair_option1} with price {live_buy_price_conversion_pair} for group '{user_group_name}'.")
            else:
                live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option2, user_group_name)
                if not (live_buy_price_conversion_pair is not None and live_buy_price_conversion_pair > 0) : # Check None and positive
                    logger.error(f"Could not find live ADJUSTED buy price for conversion pairs: {conversion_pair_option1} or {conversion_pair_option2} for group '{user_group_name}'. Cannot convert margin to USD.")
                    return None, None, None
                logger.debug(f"Found conversion pair {conversion_pair_option2} with price {live_buy_price_conversion_pair} for group '{user_group_name}'.")

            if is_usd_base:
                # Prevent division by zero if the price is somehow zero (though get_live_adjusted_buy_price_for_pair checks > 0)
                if live_buy_price_conversion_pair <= 0:
                    logger.error(f"Conversion price ({live_buy_price_conversion_pair}) for pair {conversion_pair_option1} is non-positive. Cannot convert margin to USD.")
                    return None, None, None
                calculated_margin_usd = margin_amount_in_profit_currency / live_buy_price_conversion_pair
                logger.debug(f"Converting from {profit_currency} to USD using division by {conversion_pair_option1} price.")
            else:
                calculated_margin_usd = margin_amount_in_profit_currency * live_buy_price_conversion_pair
                logger.debug(f"Converting from {profit_currency} to USD using multiplication by {conversion_pair_option2} price.")

        # Add check for calculated_margin_usd <= 0 if that's invalid
        if calculated_margin_usd is None or calculated_margin_usd <= 0: # Margin should ideally be positive for an open position
             logger.error(f"Calculated USD margin ({calculated_margin_usd}) is missing, zero, or negative for user {user_id}, symbol {symbol}. Cannot calculate margin.")
             return None, None, None


        # 9. Quantize results
        # ... (Existing quantization logic) ...
        if symbol_digits is not None and symbol_digits >= 0:
            price_precision_str = '1e-' + str(int(symbol_digits))
            price_precision = Decimal(price_precision_str)
        else:
            price_precision = Decimal('0.00001')
            logger.warning(f"Symbol digits is missing or invalid for symbol {symbol}. Using default price precision {price_precision}.")

        usd_precision = Decimal('0.00000001') # 8 decimal places for USD

        calculated_margin_usd = calculated_margin_usd.quantize(usd_precision, rounding=ROUND_HALF_UP)
        adjusted_order_price = adjusted_order_price.quantize(price_precision, rounding=ROUND_HALF_UP)
        contract_value_precision = Decimal('0.00000001')
        contract_value_for_order = contract_value_for_order.quantize(contract_value_precision, rounding=ROUND_HALF_UP)

        # Final check for calculated values not being None - should be redundant with earlier checks but kept for safety
        if calculated_margin_usd is None or adjusted_order_price is None or contract_value_for_order is None:
             logger.error(f"Final calculated values are None after quantization for user {user_id}, symbol {symbol}. This indicates a calculation error.")
             return None, None, None

        logger.info(f"Margin calculated successfully for user {user_id}, symbol {symbol}, order_type {order_type}: "
                    f"Margin_USD={calculated_margin_usd}, Adj_Price={adjusted_order_price}, Contract_Val={contract_value_for_order}, "
                    f"User_Leverage={user_leverage}, User_Group='{user_group_name}', Spread_Val={spread_value}, Half_Spread={half_spread}, "
                    f"Contract_Size={contract_size}, Profit_Currency={profit_currency}, Symbol_Digits={symbol_digits}")
        return calculated_margin_usd, adjusted_order_price, contract_value_for_order

    except InvalidOperation as e:
        logger.error(f"Decimal operation error during margin calculation for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None, None, None
    except Exception as e:
        logger.error(f"Unexpected error during margin calculation for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None, None, None