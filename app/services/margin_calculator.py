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

logger = logging.getLogger(__name__)

# async def get_live_adjusted_buy_price_for_pair(redis_client: Redis, symbol: str, user_group_name: str) -> Optional[Decimal]:
#     """
#     Fetches the live *adjusted* buy price for a given symbol, considering the user's group.
#     This function assumes that the market_data_ws.py broadcaster or a related service
#     caches user-group-specific adjusted prices in Redis.

#     A potential Redis key structure: "adjusted_market_price:{group_name}:{symbol}"
#     The value would be a JSON string: {"buy": "1.12345", "sell": "1.12340", "spread_value": "0.00005"}
#     """
    
#     cache_key = f"adjusted_market_price:{user_group_name}:{symbol.upper()}"
#     try:
#         cached_data_json = await redis_client.get(cache_key)
#         if cached_data_json:
#             price_data = json.loads(cached_data_json)
#             if 'buy' in price_data:
#                 buy_price_str = price_data['buy']
#                 if isinstance(buy_price_str, (str, int, float)):
#                     return Decimal(str(buy_price_str))
#                 else:
#                     logger.warning(f"Cached buy price for {cache_key} is not a valid number format: {buy_price_str}")
#             else:
#                 logger.warning(f"'buy' price not found in cached data for {cache_key}. Data: {price_data}")
#         else:
#             logger.warning(f"No cached adjusted buy price found for key: {cache_key}. "
#                            f"This symbol might not be broadcasted for group '{user_group_name}' or not currently in Redis.")
#         return None
#     except json.JSONDecodeError as e:
#         logger.error(f"JSONDecodeError fetching live buy price for {cache_key} from Redis: {e}. Data: {cached_data_json}")
#         return None
#     except InvalidOperation as e: # Handles errors during Decimal conversion
#         logger.error(f"InvalidOperation (Decimal conversion) for buy price from {cache_key}: {e}")
#         return None
#     except Exception as e:
#         logger.error(f"Error fetching live adjusted buy price for {cache_key} from Redis: {e}", exc_info=True)
#         return None

from decimal import Decimal, InvalidOperation
import json
import logging
from typing import Optional
from redis.asyncio import Redis
from app.firebase_stream import get_latest_market_data

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
        if fallback_data and 'o' in fallback_data:
            return Decimal(str(fallback_data['o']))
        else:
            logger.warning(f"Fallback: No 'o' price found in Firebase for symbol {symbol}")
    except Exception as fallback_error:
        logger.error(f"Fallback error fetching from Firebase for {symbol}: {fallback_error}", exc_info=True)

    return None

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
    Calculates the margin for a single order, utilizing Redis cache for user and group data.

    Returns:
        A tuple: (calculated_margin_usd, adjusted_order_price, contract_value_for_order)
        Returns (None, None, None) if calculation fails at any critical step.
    """
    try:
        # 1. Fetch User data (leverage, group_name) - Try cache first
        user_data_from_cache = await get_user_data_cache(redis_client, user_id)
        user_leverage: Optional[Decimal] = None
        user_group_name: Optional[str] = None

        if user_data_from_cache:
            logger.debug(f"User data found in cache for user_id {user_id}")
            try:
                user_leverage = Decimal(str(user_data_from_cache.get('leverage')))
                user_group_name = user_data_from_cache.get('group_name')
            except (InvalidOperation, TypeError) as e:
                logger.warning(f"Could not parse leverage from cached user data for user {user_id}: {e}. Will fetch from DB.")
                user_data_from_cache = None # Force DB fetch

        if not user_data_from_cache: # Not in cache or parsing failed
            logger.debug(f"User data not in cache or cache parse failed for user_id {user_id}. Fetching from DB.")
            user_db = await db.get(User, user_id)
            if not user_db:
                logger.error(f"User not found in DB for ID: {user_id}")
                return None, None, None
            user_leverage = user_db.leverage
            user_group_name = user_db.group_name
            # Cache it for next time
            await set_user_data_cache(redis_client, user_id, {
                "leverage": str(user_leverage), # Store as string for consistency
                "group_name": user_group_name,
                "id": user_id, # Include other relevant fields as in market_data_ws.py
                "margin": str(user_db.margin),
                "wallet_balance": str(user_db.wallet_balance)
            })
            logger.debug(f"User data for {user_id} fetched from DB and cached.")


        if not user_leverage or user_leverage <= 0:
            logger.error(f"Invalid leverage for user ID {user_id}: {user_leverage}")
            return None, None, None
        if not user_group_name:
            logger.error(f"User {user_id} does not have an assigned group name.")
            return None, None, None

        # 2. Fetch Group settings (spread, spread_pip) for the user's group and symbol - Try cache first
        # `get_group_symbol_settings_cache` expects group_name and symbol.
        # The structure in cache is Dict[str, Dict[str, Any]] for all symbols in a group,
        # or specific settings if symbol is provided.
        group_symbol_settings_cache_key = symbol.upper() # Key within the group's settings hash
        group_settings_from_cache = await get_group_symbol_settings_cache(redis_client, user_group_name, group_symbol_settings_cache_key)

        spread: Optional[Decimal] = None
        spread_pip: Optional[Decimal] = None

        if group_settings_from_cache and isinstance(group_settings_from_cache, dict):
            logger.debug(f"Group settings for group '{user_group_name}', symbol '{symbol}' found in cache.")
            try:
                spread_str = group_settings_from_cache.get('spread')
                spread_pip_str = group_settings_from_cache.get('spread_pip')
                if spread_str is not None and spread_pip_str is not None:
                    spread = Decimal(str(spread_str))
                    spread_pip = Decimal(str(spread_pip_str))
                else:
                    logger.warning(f"Spread/Spread_pip missing in cached group settings for {user_group_name}/{symbol}. Data: {group_settings_from_cache}")
            except (InvalidOperation, TypeError) as e:
                logger.warning(f"Could not parse spread/spread_pip from cached group settings for {user_group_name}/{symbol}: {e}. Will fetch from DB.")
                # Invalidate this specific cache entry or re-fetch all group settings if parse fails
                # Invalidate cache entry for this group and symbol
                await redis_client.hdel(f"group_symbol_settings:{user_group_name}", group_symbol_settings_cache_key)
                logger.info(f"Invalidated cache for group '{user_group_name}', symbol '{symbol}' due to parse failure.")
                group_settings_from_cache = None # Force DB fetch for this symbol's group settings

        if spread is None or spread_pip is None: # Not in cache or parsing failed
            logger.debug(f"Group settings for '{user_group_name}/{symbol}' not in cache or cache parse failed. Fetching from DB.")
            group_stmt = select(Group).filter_by(name=user_group_name, symbol=symbol)
            group_result = await db.execute(group_stmt)
            group_settings_db = group_result.scalars().first()

            if not group_settings_db:
                logger.error(f"No group settings found in DB for group '{user_group_name}' and symbol '{symbol}'.")
                return None, None, None

            spread = group_settings_db.spread
            spread_pip = group_settings_db.spread_pip

            # Cache it for next time (cache the specific symbol's settings)
            # The set_group_symbol_settings_cache should handle storing this correctly.
            # Ensure all relevant fields from the Group model for this symbol are cached.
            settings_to_cache = {
                "spread": str(spread),
                "spread_pip": str(spread_pip),
                "commision_type": group_settings_db.commision_type,
                "commision_value_type": group_settings_db.commision_value_type,
                "type": group_settings_db.type,
                "pip_currency": group_settings_db.pip_currency,
                "show_points": group_settings_db.show_points,
                "swap_buy": str(group_settings_db.swap_buy),
                "swap_sell": str(group_settings_db.swap_sell),
                "commision": str(group_settings_db.commision),
                "margin": str(group_settings_db.margin), # This is group's base margin, not user's calculated margin
                "deviation": str(group_settings_db.deviation),
                "min_lot": str(group_settings_db.min_lot),
                "max_lot": str(group_settings_db.max_lot),
                "pips": str(group_settings_db.pips),
            }
            await set_group_symbol_settings_cache(redis_client, user_group_name, symbol.upper(), settings_to_cache)
            logger.debug(f"Group settings for '{user_group_name}/{symbol}' fetched from DB and cached.")


        if spread is None or spread_pip is None: # Final check
            logger.error(f"Spread or spread_pip is not set or could not be parsed for group '{user_group_name}' and symbol '{symbol}'.")
            return None, None, None

        # 3. Fetch ExternalSymbolInfo (contract_size, profit_currency, digit) - This is static, could be cached too
        # For simplicity, keeping DB fetch for ExternalSymbolInfo as it's less frequently changing.
        # Consider caching this if it becomes a bottleneck.
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol)) # Case-insensitive
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()

        if not ext_symbol_info or not ext_symbol_info.contract_size:
            logger.error(f"ExternalSymbolInfo or contract_size not found for symbol: {symbol}")
            return None, None, None
        contract_size = ext_symbol_info.contract_size
        profit_currency = ext_symbol_info.profit.upper() if ext_symbol_info.profit else "USD" # Default to USD
        symbol_digits = ext_symbol_info.digit # Number of decimal places for the symbol's price

        # 4. Calculate half_spread
        spread_value = spread * spread_pip
        half_spread = spread_value / Decimal(2)

        # 5. Calculate contract_value for the order
        contract_value_for_order = order_quantity * contract_size

        # 6. Adjust order_price based on order_type
        adjusted_order_price: Decimal
        if order_type.upper() == "BUY":
            adjusted_order_price = order_price - half_spread
        elif order_type.upper() == "SELL":
            adjusted_order_price = order_price + half_spread
        else:
            logger.error(f"Invalid order_type: {order_type}")
            return None, None, None

        # 7. Calculate margin in the symbol's profit currency
        margin_in_profit_currency = (contract_value_for_order * adjusted_order_price) / user_leverage

        # 8. Convert margin to USD if profit_currency is not USD
        calculated_margin_usd: Decimal
        if profit_currency == "USD":
            calculated_margin_usd = margin_in_profit_currency
        else:
            conversion_pair_option1 = f"USD{profit_currency}"
            conversion_pair_option2 = f"{profit_currency}USD"
            live_buy_price_conversion_pair: Optional[Decimal] = None
            is_usd_base = False

            # Get live ADJUSTED buy price for the conversion pair, considering the USER'S group for that pair.
            # This assumes conversion pairs (e.g., USDCAD) are also configured with group settings.
            # If a conversion pair like USDCAD has its own group settings, user_group_name is appropriate.
            # If conversion pairs use a "default" group, that logic would be needed here.
            live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option1, user_group_name)
            if live_buy_price_conversion_pair and live_buy_price_conversion_pair > 0:
                is_usd_base = True
            else:
                live_buy_price_conversion_pair = await get_live_adjusted_buy_price_for_pair(redis_client, conversion_pair_option2, user_group_name)
                if not (live_buy_price_conversion_pair and live_buy_price_conversion_pair > 0) :
                    logger.error(f"Could not find live ADJUSTED buy price for conversion pairs: {conversion_pair_option1} or {conversion_pair_option2} for group '{user_group_name}'")
                    return None, None, None
                # is_usd_base remains False

            if is_usd_base: # Pair is USDXXX (e.g., USDCAD), price is XXX per USD. Margin is in XXX.
                            # We have XXX margin, want USD. Price is XXX/USD. So, XXX_margin / (XXX/USD) = USD_margin
                calculated_margin_usd = margin_in_profit_currency / live_buy_price_conversion_pair
            else: # Pair is XXXUSD (e.g., EURUSD, but here profit_currency is XXX, so example CADUSD)
                  # Price is USD per XXX. Margin is in XXX.
                  # We have XXX margin, want USD. Price is USD/XXX. So, XXX_margin * (USD/XXX) = USD_margin
                calculated_margin_usd = margin_in_profit_currency * live_buy_price_conversion_pair
        
        # 9. Quantize results
        # Precision for the original symbol's price and contract value
        if symbol_digits is not None and symbol_digits >= 0:
            price_precision_str = '1e-' + str(int(symbol_digits))
        else:
            price_precision_str = '0.00001' # Default to 5 decimal places if not specified
        price_precision = Decimal(price_precision_str)

        usd_precision = Decimal('0.01') # For USD margin, typically 2 decimal places

        calculated_margin_usd = calculated_margin_usd.quantize(usd_precision, rounding=ROUND_HALF_UP)
        adjusted_order_price = adjusted_order_price.quantize(price_precision, rounding=ROUND_HALF_UP)
        # Contract value precision can be higher, or based on symbol's currency norms
        contract_value_precision = Decimal('0.00000001') # Example: 8 decimal places for contract value
        contract_value_for_order = contract_value_for_order.quantize(contract_value_precision, rounding=ROUND_HALF_UP)

        logger.info(f"Margin calculated for user {user_id}, symbol {symbol}, order_type {order_type}: "
                    f"Margin_USD={calculated_margin_usd}, Adj_Price={adjusted_order_price}, Contract_Val={contract_value_for_order}, "
                    f"User_Leverage={user_leverage}, User_Group='{user_group_name}', Spread_Val={spread_value}, Half_Spread={half_spread}")
        return calculated_margin_usd, adjusted_order_price, contract_value_for_order

    except InvalidOperation as e: # Catch Decimal conversion errors explicitly
        logger.error(f"Decimal operation error during margin calculation for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None, None, None
    except Exception as e:
        logger.error(f"Unexpected error calculating margin for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None, None, None