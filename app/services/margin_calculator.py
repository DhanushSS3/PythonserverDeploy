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

import asyncio # Added if not present
import json # Added if not present
import logging
import threading # Added if not present
import decimal # Import decimal
import time # Import time for timestamping # Added if not present
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload # Added if not present
from redis.asyncio import Redis
from typing import Optional, Tuple, Dict, Any

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache,
    DecimalEncoder, # Added if not present
    decode_decimal, # Added if not present
    get_adjusted_market_price_cache # Added if not present
)
from app.firebase_stream import get_latest_market_data # Added if not present

# Import the refactored _convert_to_usd that uses raw prices
from app.services.portfolio_calculator import _convert_to_usd as convert_currency_to_usd_raw_prices


logger = logging.getLogger(__name__)

# Keep existing helper functions like get_live_adjusted_buy_price_for_pair, etc., if they are used elsewhere
# For this function, currency conversion will use convert_currency_to_usd_raw_prices

async def calculate_single_order_margin(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    order_quantity: Decimal,
    order_price: Decimal, # For market orders, this is current market. For pending, this is the limit/stop price.
    symbol: str,
    order_type: str, # e.g., "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"
) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Calculates the margin, adjusted price (or target price for pending), and contract value for an order.
    Uses the order_price directly for pending orders when determining the price basis for margin.
    Converts margin to USD using raw market rates.
    """
    try:
        # Step 1: Fetch User data (leverage, group_name)
        user_data_from_cache = await get_user_data_cache(redis_client, user_id)
        user_leverage: Optional[Decimal] = None
        user_group_name: Optional[str] = None

        if user_data_from_cache:
            cached_leverage_raw = user_data_from_cache.get('leverage')
            user_group_name = user_data_from_cache.get('group_name')
            if cached_leverage_raw is not None:
                try:
                    user_leverage = Decimal(str(cached_leverage_raw))
                    if user_leverage <= 0: # Leverage must be positive
                        logger.warning(f"User {user_id} cached leverage {user_leverage} is not positive. Will try DB.")
                        user_leverage = None 
                except (InvalidOperation, TypeError):
                    logger.warning(f"User {user_id} could not parse cached leverage '{cached_leverage_raw}'. Will try DB.")
                    user_leverage = None
        
        if user_leverage is None or user_group_name is None:
            logger.debug(f"Fetching user {user_id} data from DB for margin calculation.")
            user_db = await db.get(User, user_id)
            if not user_db:
                logger.error(f"User not found in DB for ID: {user_id}")
                return None, None, None
            user_leverage = user_db.leverage
            user_group_name = user_db.group_name
            # Optionally re-cache user data here if fetched from DB
            await set_user_data_cache(redis_client, user_id, {
                 "id": user_id, "group_name": user_group_name, "leverage": str(user_leverage),
                 "margin": str(user_db.margin), "wallet_balance": str(user_db.wallet_balance) # Add other relevant fields
            })

        if user_leverage is None or user_leverage <= 0:
            logger.error(f"Invalid leverage ({user_leverage}) for user ID {user_id}.")
            return None, None, None
        if user_group_name is None:
            logger.error(f"User {user_id} does not have an assigned group name.")
            return None, None, None

        # Step 2: Fetch Group settings (spread, spread_pip)
        group_symbol_settings_from_cache = await get_group_symbol_settings_cache(redis_client, user_group_name, symbol.upper())
        spread: Optional[Decimal] = None
        spread_pip: Optional[Decimal] = None

        if group_symbol_settings_from_cache:
            # ... (parsing logic as in your uploaded file, ensure spread/spread_pip are Decimals or None)
            raw_spread = group_symbol_settings_from_cache.get('spread')
            raw_spread_pip = group_symbol_settings_from_cache.get('spread_pip')
            try:
                if raw_spread is not None: spread = Decimal(str(raw_spread))
                if raw_spread_pip is not None: spread_pip = Decimal(str(raw_spread_pip))
            except (InvalidOperation, TypeError):
                 logger.warning(f"Could not parse spread/spread_pip from cache for {user_group_name}/{symbol}. Will try DB.")
                 spread = spread_pip = None
        
        if spread is None or spread_pip is None: # Or if parsing failed
            logger.debug(f"Fetching group settings for '{user_group_name}/{symbol}' from DB.")
            group_stmt = select(Group).filter_by(name=user_group_name, symbol=symbol) # Assuming symbol in Group table is specific enough
            group_result = await db.execute(group_stmt)
            group_settings_db = group_result.scalars().first()
            if not group_settings_db:
                logger.error(f"No group settings found in DB for group '{user_group_name}' and symbol '{symbol}'.")
                return None, None, None
            spread = group_settings_db.spread
            spread_pip = group_settings_db.spread_pip
            # Optionally re-cache here
            await set_group_symbol_settings_cache(redis_client, user_group_name, symbol.upper(), {
                "spread": str(spread), "spread_pip": str(spread_pip) # Add all other relevant group settings
            })

        if spread is None or spread < 0 or spread_pip is None or spread_pip < 0: # Allow zero, but not negative
            logger.error(f"Invalid spread ({spread}) or spread_pip ({spread_pip}) for group '{user_group_name}', symbol '{symbol}'.")
            return None, None, None

        # Step 3: Fetch ExternalSymbolInfo (contract_size, profit_currency, digit)
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()

        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None or ext_symbol_info.contract_size <= 0:
            logger.error(f"ExternalSymbolInfo, contract_size, or profit currency not found/valid for symbol: {symbol}.")
            return None, None, None
        contract_size = ext_symbol_info.contract_size
        profit_currency = ext_symbol_info.profit.upper()
        symbol_digits = ext_symbol_info.digit


        # Step 4: Calculate adjusted_order_price and contract_value
        half_spread = (spread * spread_pip) / Decimal(2)
        adjusted_order_price: Decimal
        order_type_upper = order_type.upper()

        if order_type_upper == "BUY":
            adjusted_order_price = order_price + half_spread
        elif order_type_upper == "SELL":
            adjusted_order_price = order_price - half_spread
        elif order_type_upper in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
            # For pending orders, the 'order_price' from the request is the target price.
            # Margin is calculated based on this target price.
            # Spreads are typically applied at the time of execution by the broker against the market feed,
            # not by adjusting the user's specified limit/stop price itself for the pending order's margin.
            adjusted_order_price = order_price 
            logger.debug(f"Pending order type {order_type_upper}: using request order_price {order_price} as the basis for margin calculation price.")
        else:
            logger.error(f"Invalid order_type: {order_type} in calculate_single_order_margin")
            return None, None, None

        if adjusted_order_price <= 0:
            logger.error(f"Adjusted order price ({adjusted_order_price}) is not positive for symbol {symbol}, order_type {order_type}.")
            return None, None, None

        # Contract value (total units of base currency for the trade, as per your existing formula structure)
        # This `contract_value_for_order` seems to represent total units (e.g. 1 lot * 100,000 units/lot = 100,000 units)
        # The monetary value for margin calc is based on `order_price` from request.
        contract_value_for_order = order_quantity * contract_size 

        # Step 5: Calculate margin in the symbol's profit currency
        # Margin calculation uses the `order_price` from the request (market price for market orders, limit/stop price for pending)
        if order_price <= 0: # This is the price from the original request
             logger.error(f"Request order price ({order_price}) is zero or negative for symbol {symbol}. Cannot calculate margin.")
             return None, None, None
        margin_amount_in_profit_currency = (order_quantity * contract_size * order_price) / user_leverage
        logger.debug(f"MarginCalc: Margin in {profit_currency}: {margin_amount_in_profit_currency} for symbol {symbol}, order_type {order_type_upper}")

        # Step 6: Convert margin to USD using raw rates
        calculated_margin_usd: Decimal
        if profit_currency == "USD":
            calculated_margin_usd = margin_amount_in_profit_currency
        else:
            logger.debug(f"MarginCalc: Calling convert_currency_to_usd_raw_prices for margin. Amount: {margin_amount_in_profit_currency}, From: {profit_currency}")
            calculated_margin_usd = await convert_currency_to_usd_raw_prices(
                amount=margin_amount_in_profit_currency,
                from_currency=profit_currency,
                user_id=user_id,
                position_id=f"margin_calc_for_{symbol}_{order_type_upper}",
                value_description="Order Margin"
            )
            # Check if conversion failed (returned original amount for non-USD currency)
            if calculated_margin_usd == margin_amount_in_profit_currency and profit_currency != "USD":
                logger.error(f"MarginCalc: Critical failure to convert margin from {profit_currency} to USD for user {user_id}, symbol {symbol}, order {order_type_upper}. Raw rates likely missing.")
                return None, None, None 

        if calculated_margin_usd <= Decimal("0.0"): # Margin must be positive
            logger.error(f"MarginCalc: Calculated USD margin is not positive ({calculated_margin_usd}) for user {user_id}, symbol {symbol}, order {order_type_upper}.")
            return None, None, None

        # Step 7: Quantize results
        price_precision_str = '1e-' + str(int(symbol_digits)) if symbol_digits is not None and symbol_digits >= 0 else '0.00001'
        price_precision = Decimal(price_precision_str)
        usd_precision = Decimal('0.01') # Margin in USD is typically 2 decimal places

        final_calculated_margin_usd = calculated_margin_usd.quantize(usd_precision, rounding=ROUND_HALF_UP)
        # For pending orders, `adjusted_order_price` is the user's specified limit/stop price.
        # For market orders, it's the execution price including spread.
        final_adjusted_order_price = adjusted_order_price.quantize(price_precision, rounding=ROUND_HALF_UP)
        # `contract_value_for_order` (total units)
        final_contract_value_for_order = contract_value_for_order.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)


        logger.info(
            f"Margin calculated for user {user_id}, symbol {symbol}, order_type {order_type_upper}: "
            f"Margin_USD={final_calculated_margin_usd}, PriceBasisForMargin={order_price}, "
            f"AdjustedExecPrice/LimitPrice={final_adjusted_order_price}, Contract_Units={final_contract_value_for_order}"
        )
        return final_calculated_margin_usd, final_adjusted_order_price, final_contract_value_for_order

    except InvalidOperation as e:
        logger.error(f"Decimal operation error during margin calculation for user {user_id}, symbol {symbol}, order_type {order_type}: {e}", exc_info=True)
        return None, None, None
    except Exception as e:
        logger.error(f"Unexpected error during margin calculation for user {user_id}, symbol {symbol}, order_type {order_type}: {e}", exc_info=True)
        return None, None, None