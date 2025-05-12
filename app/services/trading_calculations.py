# app/services/trading_calculations.py

import decimal
import logging
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

# Import the database CRUD function to get external symbol info
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
# Import the Redis caching function to get group-symbol settings (if needed in calculation)
# from app.core.cache import get_group_symbol_settings_cache # Uncomment if you need group settings like spread directly

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set level to DEBUG for detailed logs

# Define instrument types as constants for clarity
INSTRUMENT_TYPE_FOREX = '1'
INSTRUMENT_TYPE_METALS = '2'
INSTRUMENT_TYPE_INDICES = '3'
INSTRUMENT_TYPE_CRYPTO = '4'
# Add other types if necessary


async def calculate_single_order_margin(
    volume: decimal.Decimal,
    raw_ask_price: decimal.Decimal,
    user_leverage: decimal.Decimal, # User's account leverage
    symbol: str,
    # group_symbol_settings: Optional[Dict[str, Any]], # Pass group settings if calculation needs them
    live_prices_cache: Dict[str, Dict[str, decimal.Decimal]], # Needs live prices for conversion
    db: AsyncSession # Database session to fetch static symbol info
) -> decimal.Decimal:
    """
    Calculates the margin required for a single order based on symbol type,
    contract size (from DB), user leverage, live price, and currency conversion.

    Args:
        volume: The trading volume (lot size).
        raw_ask_price: The current market ask price for the symbol.
        user_leverage: The user's account leverage (e.g., 100 for 1:100).
                       Should be a Decimal > 0.
        symbol: The trading symbol (e.g., "GBPUSD", "BTCUSD", "XAUUSD", "US30").
        # group_symbol_settings: Group-specific settings for the symbol (e.g., spread, min/max lot).
                               # Pass this if specific calculation rules depend on these.
        live_prices_cache: A dictionary containing live market prices.
                           Expected format: {"SYMBOL": {"ask": Decimal, "bid": Decimal, ...}, ...}
                           Also needs conversion rates like {"USDJPY": {"ask": Decimal, ...}}.
        db: The asynchronous database session.

    Returns:
        The calculated margin required for the order in the user's account currency (USD, assuming).
        Returns Decimal('0.0') if calculation fails or data is missing/invalid.
    """
    # Input validation
    if not isinstance(volume, decimal.Decimal) or volume <= 0:
        logger.error(f"Invalid volume provided: {volume} for symbol {symbol}")
        return decimal.Decimal('0.0')
    if not isinstance(raw_ask_price, decimal.Decimal) or raw_ask_price <= 0:
         logger.error(f"Invalid raw_ask_price provided: {raw_ask_price} for symbol {symbol}")
         return decimal.Decimal('0.0')
    if not isinstance(user_leverage, decimal.Decimal) or user_leverage <= 0:
         logger.error(f"Invalid user_leverage provided: {user_leverage} for symbol {symbol}")
         return decimal.Decimal('0.0')
    if not isinstance(symbol, str) or not symbol:
         logger.error(f"Invalid symbol provided: {symbol}")
         return decimal.Decimal('0.0')
    if not isinstance(live_prices_cache, dict):
         logger.error("Invalid live_prices_cache provided.")
         return decimal.Decimal('0.0')


    logger.debug(f"Calculating margin for order: Symbol={symbol}, Volume={volume}, Price={raw_ask_price}, Leverage={user_leverage}")

    # --- 1. Get static symbol data from the database ---
    symbol_info_db = await get_external_symbol_info_by_symbol(db, symbol)

    if not symbol_info_db:
        logger.error(f"Static symbol info not found in DB for symbol: {symbol}")
        return decimal.Decimal('0.0')

    # Extract necessary data from database model instance
    contract_size = symbol_info_db.contract_size if symbol_info_db.contract_size is not None else decimal.Decimal('0.0')
    instrument_type = symbol_info_db.instrument_type if symbol_info_db.instrument_type is not None else INSTRUMENT_TYPE_FOREX # Default to Forex if type is null
    # For crypto, 'margin' field from DB holds the margin currency or value
    crypto_margin_value_str = symbol_info_db.margin if symbol_info_db.margin is not None else ''
    # For indices, 'margin_leverage' from DB holds the factor (e.g., "0.01")
    index_margin_leverage_str = symbol_info_db.margin_leverage if symbol_info_db.margin_leverage is not None else ''
    profit_currency = symbol_info_db.profit # Profit currency from DB
    base_currency = symbol_info_db.base # Base currency from DB

    logger.debug(f"DB Info for {symbol}: Type={instrument_type}, ContractSize={contract_size}, ProfitCurrency={profit_currency}, BaseCurrency={base_currency}, CryptoMarginStr={crypto_margin_value_str}, IndexMarginStr={index_margin_leverage_str}")


    # Ensure contract size is valid
    if contract_size <= 0 and instrument_type not in [INSTRUMENT_TYPE_CRYPTO, INSTRUMENT_TYPE_INDICES]: # Crypto/Indices calculation doesn't always use contract size directly in the same way
         logger.warning(f"Invalid or zero contract size ({contract_size}) from DB for non-Crypto/Index symbol '{symbol}'. Margin calculation might be incorrect.")
         # Depending on your logic, you might return 0.0 here or proceed if logic allows for zero contract size scenarios.
         # Let's allow it to proceed for now, but log a warning.

    # --- 2. Get live prices from cache ---
    symbol_live_data = live_prices_cache.get(symbol)
    if not symbol_live_data:
        logger.error(f"Live market data not found in cache for symbol: {symbol}")
        return decimal.Decimal('0.0')

    live_ask_price = symbol_live_data.get('ask')
    live_bid_price = symbol_live_data.get('bid') # Needed for conversion rate calculation

    if live_ask_price is None or not isinstance(live_ask_price, decimal.Decimal) or live_ask_price <= 0:
         logger.error(f"Invalid or missing live ask price in cache for symbol '{symbol}': {live_ask_price}")
         return decimal.Decimal('0.0')

    # Note: We are using raw_ask_price provided to the function for calculation
    # instead of live_ask_price from cache, based on previous context.
    # Ensure consistency in your application logic.

    logger.debug(f"Live Ask Price for {symbol}: {live_ask_price}")


    # --- 3. Calculate margin based on instrument type ---

    required_margin_usd = decimal.Decimal('0.0') # Initialize margin in USD

    try:
        if instrument_type == INSTRUMENT_TYPE_FOREX:
            logger.debug(f"Calculating Forex margin for {symbol}")
            # Formula: (volume * contract_size / leverage) * ConversionRate(ProfitCurrency/USD)
            # Assuming account currency is USD. If not, adjust conversion rate.

            # Determine conversion rate for ProfitCurrency to USD
            conversion_rate_profit_to_usd = decimal.Decimal('1.0') # Default if ProfitCurrency is USD
            if profit_currency and profit_currency.upper() != 'USD':
                # Need the Ask price for PROFIT/USD or Bid for USD/PROFIT
                conversion_pair_ask = f"{profit_currency.upper()}USD"
                conversion_pair_bid = f"USD{profit_currency.upper()}"

                conversion_pair_data_ask = live_prices_cache.get(conversion_pair_ask)
                conversion_pair_data_bid = live_prices_cache.get(conversion_pair_bid)

                if conversion_pair_data_ask and conversion_pair_data_ask.get('ask') is not None:
                    # Use Ask price if PROFIT/USD pair exists (e.g., AUDUSD)
                    conversion_rate_profit_to_usd = conversion_pair_data_ask['ask']
                    logger.debug(f"Using Ask price from {conversion_pair_ask} for conversion: {conversion_rate_profit_to_usd}")
                elif conversion_pair_data_bid and conversion_pair_data_bid.get('bid') is not None and conversion_pair_data_bid['bid'] > 0:
                    # Use 1 / Bid price if USD/PROFIT pair exists (e.g., USDJPY)
                    conversion_rate_profit_to_usd = decimal.Decimal('1.0') / conversion_pair_data_bid['bid']
                    logger.debug(f"Using 1 / Bid price from {conversion_pair_bid} for conversion: {conversion_rate_profit_to_usd}")
                else:
                    logger.error(f"Could not find live price data for currency conversion from Profit Currency '{profit_currency}' to USD for symbol '{symbol}'. Checked {conversion_pair_ask} (ask) and {conversion_pair_bid} (bid).")
                    return decimal.Decimal('0.0')

            if conversion_rate_profit_to_usd <= 0:
                 logger.error(f"Invalid conversion rate calculated ({conversion_rate_profit_to_usd}) for Profit Currency '{profit_currency}' to USD for symbol '{symbol}'.")
                 return decimal.Decimal('0.0')


            # Ensure valid contract size for Forex
            if contract_size <= 0:
                 logger.error(f"Invalid contract size ({contract_size}) from DB for Forex symbol '{symbol}'. Cannot calculate margin.")
                 return decimal.Decimal('0.0')

            # Ensure valid user leverage
            if user_leverage <= 0:
                logger.error(f"Invalid user leverage ({user_leverage}). Cannot calculate margin for symbol '{symbol}'.")
                return decimal.Decimal('0.0')

            # Margin formula for Forex
            required_margin_usd = (volume * contract_size / user_leverage) * conversion_rate_profit_to_usd
            logger.debug(f"Forex Margin Calculation: ({volume} * {contract_size} / {user_leverage}) * {conversion_rate_profit_to_usd} = {required_margin_usd}")


        elif instrument_type == INSTRUMENT_TYPE_METALS:
            logger.debug(f"Calculating Metals margin for {symbol}")
            # Formula: (volume * contract_size / leverage) * Price(Symbol) * ConversionRate(ProfitCurrency/USD)
            # Assuming account currency is USD. If not, adjust conversion rate.

            # Determine conversion rate for ProfitCurrency to USD
            conversion_rate_profit_to_usd = decimal.Decimal('1.0') # Default if ProfitCurrency is USD
            if profit_currency and profit_currency.upper() != 'USD':
                 # Need the Ask price for PROFIT/USD or Bid for USD/PROFIT
                conversion_pair_ask = f"{profit_currency.upper()}USD"
                conversion_pair_bid = f"USD{profit_currency.upper()}"

                conversion_pair_data_ask = live_prices_cache.get(conversion_pair_ask)
                conversion_pair_data_bid = live_prices_cache.get(conversion_pair_bid)

                if conversion_pair_data_ask and conversion_pair_data_ask.get('ask') is not None:
                    # Use Ask price if PROFIT/USD pair exists (e.g., AUDUSD)
                    conversion_rate_profit_to_usd = conversion_pair_data_ask['ask']
                    logger.debug(f"Using Ask price from {conversion_pair_ask} for conversion: {conversion_rate_profit_to_usd}")
                elif conversion_pair_data_bid and conversion_pair_data_bid.get('bid') is not None and conversion_pair_data_bid['bid'] > 0:
                     # Use 1 / Bid price if USD/PROFIT pair exists (e.g., USDJPY)
                     conversion_rate_profit_to_usd = decimal.Decimal('1.0') / conversion_pair_data_bid['bid']
                     logger.debug(f"Using 1 / Bid price from {conversion_pair_bid} for conversion: {conversion_rate_profit_to_usd}")
                else:
                    logger.error(f"Could not find live price data for currency conversion from Profit Currency '{profit_currency}' to USD for symbol '{symbol}'. Checked {conversion_pair_ask} (ask) and {conversion_pair_bid} (bid).")
                    return decimal.Decimal('0.0')

            if conversion_rate_profit_to_usd <= 0:
                 logger.error(f"Invalid conversion rate calculated ({conversion_rate_profit_to_usd}) for Profit Currency '{profit_currency}' to USD for symbol '{symbol}'.")
                 return decimal.Decimal('0.0')


            # Ensure valid contract size for Metals
            if contract_size <= 0:
                 logger.error(f"Invalid contract size ({contract_size}) from DB for Metals symbol '{symbol}'. Cannot calculate margin.")
                 return decimal.Decimal('0.0')

            # Ensure valid user leverage
            if user_leverage <= 0:
                logger.error(f"Invalid user leverage ({user_leverage}). Cannot calculate margin for symbol '{symbol}'.")
                return decimal.Decimal('0.0')

            # Margin formula for Metals
            required_margin_usd = (volume * contract_size / user_leverage) * raw_ask_price * conversion_rate_profit_to_usd
            logger.debug(f"Metals Margin Calculation: ({volume} * {contract_size} / {user_leverage}) * {raw_ask_price} * {conversion_rate_profit_to_usd} = {required_margin_usd}")


        elif instrument_type == INSTRUMENT_TYPE_INDICES:
            logger.debug(f"Calculating Indices margin for {symbol}")
            # Formula: (volume * contract_size * Price(Symbol)) * MarginFactor
            # Margin factor is obtained from 'margin_leverage' from DB, which is often a direct percentage (e.g., "0.01")

            margin_factor = decimal.Decimal('0.0')
            if index_margin_leverage_str:
                try:
                    margin_factor = decimal.Decimal(index_margin_leverage_str)
                except (decimal.InvalidOperation, TypeError):
                    logger.error(f"Invalid margin_leverage string '{index_margin_leverage_str}' from DB for Index symbol '{symbol}'. Expected a numeric string.")
                    return decimal.Decimal('0.0')

            if margin_factor <= 0:
                 logger.error(f"Invalid or zero margin factor ({margin_factor}) derived from DB for Index symbol '{symbol}'. Cannot calculate margin.")
                 return decimal.Decimal('0.0')

            # Ensure valid contract size for Indices (often 1, but check your data)
            if contract_size <= 0:
                 logger.warning(f"Invalid or zero contract size ({contract_size}) from DB for Index symbol '{symbol}'. Using 1 for calculation.")
                 contract_size = decimal.Decimal('1.0') # Default to 1 if zero/invalid

            # For Indices, we typically don't use user_leverage in the same way.
            # The 'margin_leverage' from the API is the direct margin requirement factor.
            # The result is already in the profit currency (or base if no profit currency).
            # Need to convert to USD if account currency is USD and profit/base is not USD.

            conversion_rate_to_usd = decimal.Decimal('1.0') # Default if ProfitCurrency or BaseCurrency is USD
            currency_to_convert = profit_currency if profit_currency else base_currency # Use profit first, then base

            if currency_to_convert and currency_to_convert.upper() != 'USD':
                 # Need the Ask price for CURRENCY/USD or Bid for USD/CURRENCY
                conversion_pair_ask = f"{currency_to_convert.upper()}USD"
                conversion_pair_bid = f"USD{currency_to_convert.upper()}"

                conversion_pair_data_ask = live_prices_cache.get(conversion_pair_ask)
                conversion_pair_data_bid = live_prices_cache.get(conversion_pair_bid)


                if conversion_pair_data_ask and conversion_pair_data_ask.get('ask') is not None:
                    # Use Ask price if CURRENCY/USD pair exists (e.g., AUDUSD)
                    conversion_rate_to_usd = conversion_pair_data_ask['ask']
                    logger.debug(f"Using Ask price from {conversion_pair_ask} for conversion: {conversion_rate_to_usd}")
                elif conversion_pair_data_bid and conversion_pair_data_bid.get('bid') is not None and conversion_pair_data_bid['bid'] > 0:
                     # Use 1 / Bid price if USD/CURRENCY pair exists (e.g., USDJPY)
                     conversion_rate_to_usd = decimal.Decimal('1.0') / conversion_pair_data_bid['bid']
                     logger.debug(f"Using 1 / Bid price from {conversion_pair_bid} for conversion: {conversion_rate_to_usd}")
                else:
                    logger.error(f"Could not find live price data for currency conversion from '{currency_to_convert}' to USD for symbol '{symbol}'. Checked {conversion_pair_ask} (ask) and {conversion_pair_bid} (bid).")
                    return decimal.Decimal('0.0')

            if conversion_rate_to_usd <= 0:
                 logger.error(f"Invalid conversion rate calculated ({conversion_rate_to_usd}) for '{currency_to_convert}' to USD for symbol '{symbol}'.")
                 return decimal.Decimal('0.0')


            required_margin_usd = (volume * contract_size * raw_ask_price) * margin_factor * conversion_rate_to_usd
            logger.debug(f"Indices Margin Calculation: ({volume} * {contract_size} * {raw_ask_price}) * {margin_factor} * {conversion_rate_to_usd} = {required_margin_usd}")


        elif instrument_type == INSTRUMENT_TYPE_CRYPTO:
            logger.debug(f"Calculating Crypto margin for {symbol}")
            # Formula: (volume * contract_size * Price(Symbol)) / CryptoMarginValue
            # CryptoMarginValue is usually the number associated with the margin currency (e.g., 50 for "BTC" margin, 10 for "1:10")

            crypto_margin_value = decimal.Decimal('0.0')
            if crypto_margin_value_str:
                # Attempt to extract the numeric value. Handle "BTC" or "1:10" formats.
                # This requires parsing the string from the 'margin' column.
                # Based on your sample data, the 'margin' column contains the currency ("BTC", "ETH") or leverage ("1:10").
                # Assuming the numeric value for the margin requirement for crypto is stored/derivable.
                # Let's assume for "BTC" margin type, the value is derived from 'margin_leverage' string like "1:10" -> 10.
                # Or perhaps the API 'margin' field itself *is* the value if it's numeric.
                # Your sample data shows 'margin' as "BTC" or "ETH" for crypto, and 'margin_leverage' as "1:10".
                # The common calculation is (volume * price) / leverage.
                # Let's use 'margin_leverage' string and parse it for Crypto leverage value.

                leverage_value = decimal.Decimal('1.0') # Default leverage for calculation
                if index_margin_leverage_str: # Re-using index_margin_leverage_str as it seems to hold leverage like "1:10"
                    parts = index_margin_leverage_str.split(':')
                    if len(parts) == 2 and parts[0] == '1':
                        try:
                            leverage_value = decimal.Decimal(parts[1])
                        except (decimal.InvalidOperation, TypeError):
                            logger.error(f"Invalid crypto margin leverage string format '{index_margin_leverage_str}' from DB for symbol '{symbol}'. Expected '1:Value'.")
                            return decimal.Decimal('0.0')
                    else:
                         # Handle cases where margin_leverage isn't "1:Value", if applicable
                         logger.warning(f"Unexpected crypto margin leverage format '{index_margin_leverage_str}' for symbol '{symbol}'. Attempting direct conversion.")
                         try:
                              leverage_value = decimal.Decimal(index_margin_leverage_str)
                              if leverage_value <= 0: raise ValueError("Leverage must be positive")
                         except (decimal.InvalidOperation, TypeError, ValueError):
                              logger.error(f"Invalid crypto margin leverage value '{index_margin_leverage_str}' from DB for symbol '{symbol}'.")
                              return decimal.Decimal('0.0')


                if leverage_value <= 0:
                    logger.error(f"Calculated crypto leverage value is zero or negative ({leverage_value}) for symbol '{symbol}'. Cannot calculate margin.")
                    return decimal.Decimal('0.0')


                # Ensure valid contract size for Crypto (often 1, but check your data)
                if contract_size <= 0:
                    logger.warning(f"Invalid or zero contract size ({contract_size}) from DB for Crypto symbol '{symbol}'. Using 1 for calculation.")
                    contract_size = decimal.Decimal('1.0') # Default to 1 if zero/invalid


                # Crypto Margin formula: (volume * contract_size * Price(Symbol)) / LeverageValue
                # Assuming account currency is USD. The result is in Profit/Base currency, need conversion.
                # For BTCUSD, ETHUSD, profit currency is USD, so conversion rate is 1.0.
                # If other crypto pairs exist (e.g., BTC/EUR), need EUR/USD conversion.

                conversion_rate_to_usd = decimal.Decimal('1.0') # Default if ProfitCurrency or BaseCurrency is USD
                currency_to_convert = profit_currency if profit_currency else base_currency # Use profit first, then base

                if currency_to_convert and currency_to_convert.upper() != 'USD':
                    # Need the Ask price for CURRENCY/USD or Bid for USD/CURRENCY
                    conversion_pair_ask = f"{currency_to_convert.upper()}USD"
                    conversion_pair_bid = f"USD{currency_to_convert.upper()}"

                    conversion_pair_data_ask = live_prices_cache.get(conversion_pair_ask)
                    conversion_pair_data_bid = live_prices_cache.get(conversion_pair_bid)


                    if conversion_pair_data_ask and conversion_pair_data_ask.get('ask') is not None:
                        # Use Ask price if CURRENCY/USD pair exists (e.g., EURUSD)
                        conversion_rate_to_usd = conversion_pair_data_ask['ask']
                        logger.debug(f"Using Ask price from {conversion_pair_ask} for conversion: {conversion_rate_to_usd}")
                    elif conversion_pair_data_bid and conversion_pair_data_bid.get('bid') is not None and conversion_pair_data_bid['bid'] > 0:
                         # Use 1 / Bid price if USD/CURRENCY pair exists (e.g., USDJPY)
                         conversion_rate_to_usd = decimal.Decimal('1.0') / conversion_pair_data_bid['bid']
                         logger.debug(f"Using 1 / Bid price from {conversion_pair_bid} for conversion: {conversion_rate_to_usd}")
                    else:
                         logger.error(f"Could not find live price data for currency conversion from '{currency_to_convert}' to USD for symbol '{symbol}'. Checked {conversion_pair_ask} (ask) and {conversion_pair_bid} (bid).")
                         return decimal.Decimal('0.0')

                if conversion_rate_to_usd <= 0:
                     logger.error(f"Invalid conversion rate calculated ({conversion_rate_to_usd}) for '{currency_to_convert}' to USD for symbol '{symbol}'.")
                     return decimal.Decimal('0.0')


                required_margin_usd = (volume * contract_size * raw_ask_price) / leverage_value * conversion_rate_to_usd
                logger.debug(f"Crypto Margin Calculation: ({volume} * {contract_size} * {raw_ask_price}) / {leverage_value} * {conversion_rate_to_usd} = {required_margin_usd}")

            else:
                logger.error(f"'margin' field (or relevant field for crypto margin value) is missing or empty in DB for Crypto symbol '{symbol}'. Cannot calculate margin.")
                return decimal.Decimal('0.0')


        else:
            logger.error(f"Unsupported instrument type '{instrument_type}' from DB for symbol '{symbol}'. Cannot calculate margin.")
            return decimal.Decimal('0.0')

    except decimal.InvalidOperation as e:
        logger.error(f"Decimal calculation error for symbol '{symbol}': {e}", exc_info=True)
        return decimal.Decimal('0.0')
    except Exception as e:
        logger.error(f"An unexpected error occurred during margin calculation for symbol '{symbol}': {e}", exc_info=True)
        return decimal.Decimal('0.0')

    # Return the calculated margin, rounded to an appropriate precision (e.g., 2 decimal places for USD)
    # Adjust rounding as needed based on your application's requirements
    return required_margin_usd.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)


# Helper function to get conversion rate (if needed elsewhere)
# This function is now integrated directly into calculate_single_order_margin
# async def get_conversion_rate_to_usd(
#     currency: str,
#     live_prices_cache: Dict[str, Dict[str, decimal.Decimal]]
# ) -> Optional[decimal.Decimal]:
#     """
#     Gets the conversion rate from the given currency to USD from the live prices cache.
#     Assumes the cache contains pairs like EURUSD, USDJPY, etc.
#     """
#     if currency.upper() == 'USD':
#         return decimal.Decimal('1.0')

#     # Try direct pair like EURUSD (Base/USD)
#     direct_pair = f"{currency.upper()}USD"
#     direct_pair_data = live_prices_cache.get(direct_pair)
#     if direct_pair_data and direct_pair_data.get('ask') is not None:
#         # Use Ask price for Base/USD pair
#         logger.debug(f"Using Ask price from {direct_pair} for conversion: {direct_pair_data['ask']}")
#         return direct_pair_data['ask']

#     # Try inverse pair like USDJPY (USD/Quote)
#     inverse_pair = f"USD{currency.upper()}"
#     inverse_pair_data = live_prices_cache.get(inverse_pair)
#     if inverse_pair_data and inverse_pair_data.get('bid') is not None and inverse_pair_data['bid'] > 0:
#         # Use 1 / Bid price for USD/Quote pair
#         conversion_rate = decimal.Decimal('1.0') / inverse_pair_data['bid']
#         logger.debug(f"Using 1 / Bid price from {inverse_pair} for conversion: {conversion_rate}")
#         return conversion_rate

#     logger.warning(f"Could not find live price data for currency conversion from '{currency}' to USD. Checked {direct_pair} (ask) and {inverse_pair} (bid).")
#     return None # Conversion rate not found or invalid