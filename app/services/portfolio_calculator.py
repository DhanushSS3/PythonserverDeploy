# import logging
# from typing import Dict, Any, List
# from decimal import Decimal, InvalidOperation

# logger = logging.getLogger(__name__)

# # Assuming a standard lot size (e.g., 100,000 units per standard lot)
# # This might need to be configurable per symbol or group
# STANDARD_LOT_SIZE = Decimal("100000")

# async def calculate_user_portfolio(
#     user_data: Dict[str, Any],
#     open_positions: List[Dict[str, Any]], # List of open position dictionaries (from cached portfolio)
#     adjusted_market_prices: Dict[str, Dict[str, float]], # Adjusted market prices from broadcaster
#     group_symbol_settings: Dict[str, Dict[str, Any]] # Group settings for relevant symbols
# ) -> Dict[str, Any]:
#     """
#     Calculates dynamic user portfolio details (equity, margin, free_margin, profit_loss)
#     including commission deduction based on group settings and updated currency conversion.

#     Args:
#         user_data: Dictionary containing user details (e.g., balance, leverage).
#         open_positions: List of dictionaries representing the user's open orders/positions.
#                         Each dict should contain keys like 'order_id', 'order_company_name' (symbol),
#                         'order_type' ('Buy'/'Sell'), 'order_quantity', 'order_price'.
#         adjusted_market_prices: Dictionary keyed by symbol (uppercase) with adjusted
#                                 buy/sell prices and spread_value {'symbol': {'buy': float, 'sell': float, 'spread_value': float}}.
#         group_symbol_settings: Dictionary keyed by symbol (uppercase) with group settings
#                                 {'symbol': {'margin': Decimal, 'pips': Decimal, 'spread_pip': Decimal, 
#                                             'commision_type': int, 'commision_value_type': int, 'commission': Decimal, 
#                                             'profit_currency': str, 'contract_size': Decimal, ...}}.

#     Returns:
#         A dictionary containing the calculated portfolio metrics:
#         'balance', 'equity', 'margin', 'free_margin', 'profit_loss', 'positions' (updated with PnL).
#     """
#     # Ensure Decimal types for calculations
#     try:
#         balance = Decimal(str(user_data.get("wallet_balance", 0.0)))
#         leverage = Decimal(str(user_data.get("leverage", 1.0)))
#         if leverage == Decimal("0.0"): # Avoid division by zero
#             leverage = Decimal("1.0")
#         logger.debug(f"User {user_data.get('id')}: Initial Balance: {balance}, Leverage: {leverage}")
#     except InvalidOperation as e:
#         logger.error(f"Error converting user data to Decimal: {e}. User data: {user_data}", exc_info=True)
#         balance = Decimal("0.0")
#         leverage = Decimal("1.0") # Default to 1.0 on error

#     total_profit_loss = Decimal("0.0")
#     total_margin_used = Decimal("0.0")
#     updated_positions = [] # To store positions with calculated PnL

#     logger.debug(f"User {user_data.get('id')}: Calculating portfolio for {len(open_positions)} open positions.")

#     # Iterate through each open position
#     for position in open_positions:
#         try:
#             symbol = position.get("order_company_name") # Assuming this is the symbol
#             order_type = position.get("order_type") # 'Buy' or 'Sell'
#             # Ensure quantity and price are Decimal
#             quantity = Decimal(str(position.get("order_quantity", 0.0)))
#             entry_price = Decimal(str(position.get("order_price", 0.0)))
#             position_id = position.get('order_id', 'N/A') # Get position ID for logging

#             logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Processing position - Symbol: {symbol}, Type: {order_type}, Qty: {quantity}, Entry Price: {entry_price}")

#             if not symbol or not order_type or quantity <= Decimal("0.0") or entry_price <= Decimal("0.0"):
#                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Skipping invalid position data: {position}")
#                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
#                 updated_positions.append(pos_to_append)
#                 continue

#             symbol_upper = symbol.upper()
#             current_market_price_info = adjusted_market_prices.get(symbol_upper)
#             symbol_settings = group_symbol_settings.get(symbol_upper) if isinstance(group_symbol_settings, dict) else None

#             logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Market data available: {current_market_price_info is not None}, Group settings available: {symbol_settings is not None}")

#             if not current_market_price_info or not symbol_settings:
#                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Market data or group settings missing for symbol {symbol_upper}. Cannot calculate PnL/Margin/Commission.")
#                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
#                 updated_positions.append(pos_to_append)
#                 continue

#             # Ensure market prices and settings are Decimal
#             try:
#                 current_buy_price = Decimal(str(current_market_price_info.get('buy', 0.0)))
#                 current_sell_price = Decimal(str(current_market_price_info.get('sell', 0.0)))
#                 group_margin_setting = Decimal(str(symbol_settings.get('margin', 0.0))) # Not directly used for margin calculation, user leverage is used
#                 pips_setting = Decimal(str(symbol_settings.get('pips', 0.0)))
#                 spread_pip_setting = Decimal(str(symbol_settings.get('spread_pip', 0.0)))
#                 contract_size = Decimal(str(symbol_settings.get('contract_size', STANDARD_LOT_SIZE)))

#                 # Corrected commission keys
#                 commision_type = symbol_settings.get('commision_type') 
#                 commision_value_type = symbol_settings.get('commision_value_type')
#                 commission_setting = Decimal(str(symbol_settings.get('commission', 0.0)))

#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Market Prices - Buy: {current_buy_price}, Sell: {current_sell_price}. Settings - Contract Size: {contract_size}. Commission - Type: {commision_type}, Value Type: {commision_value_type}, Setting: {commission_setting}")

#             except InvalidOperation as e:
#                 logger.error(f"User {user_data.get('id')}, Pos {position_id}: Error converting market data or settings to Decimal for symbol {symbol_upper}: {e}", exc_info=True)
#                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
#                 updated_positions.append(pos_to_append)
#                 continue

#             # --- Calculate Profit/Loss for the position (before commission and USD conversion) ---
#             position_profit_loss_native = Decimal("0.0")
#             if order_type.lower() == 'buy':
#                 position_profit_loss_native = (current_sell_price - entry_price) * quantity * contract_size
#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated PnL (Buy, Native): ({current_sell_price} - {entry_price}) * {quantity} * {contract_size} = {position_profit_loss_native}")
#             elif order_type.lower() == 'sell':
#                 position_profit_loss_native = (entry_price - current_buy_price) * quantity * contract_size
#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated PnL (Sell, Native): ({entry_price} - {current_buy_price}) * {quantity} * {contract_size} = {position_profit_loss_native}")
#             else:
#                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown order type '{order_type}'. Cannot calculate PnL.")

#             # --- Convert profit/loss to USD if the profit currency is not USD ---
#             profit_currency = symbol_settings.get('profit_currency', 'USD').upper()
#             position_profit_loss_usd = position_profit_loss_native # Initialize with native PnL

#             if profit_currency != 'USD' and position_profit_loss_native != Decimal("0.0"):
#                 conversion_successful = False
#                 # Try to find PROFITCURRENCY/USD (e.g., CADUSD)
#                 direct_conversion_symbol = f"{profit_currency}USD"
#                 conversion_data_direct = adjusted_market_prices.get(direct_conversion_symbol)

#                 if conversion_data_direct:
#                     try:
#                         live_buy_price_direct = Decimal(str(conversion_data_direct.get('buy', 0.0)))
#                         if live_buy_price_direct > Decimal("0.0"):
#                             position_profit_loss_usd = position_profit_loss_native * live_buy_price_direct
#                             conversion_successful = True
#                             logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Converted PnL using {direct_conversion_symbol} buy price: {position_profit_loss_native} {profit_currency} * {live_buy_price_direct} = {position_profit_loss_usd} USD")
#                         else:
#                             logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Invalid buy price (<=0) for direct conversion symbol {direct_conversion_symbol}: {live_buy_price_direct}")
#                     except InvalidOperation as e:
#                         logger.error(f"User {user_data.get('id')}, Pos {position_id}: Error converting direct conversion rate for {direct_conversion_symbol} to Decimal: {e}", exc_info=True)
                
#                 if not conversion_successful:
#                     # Try to find USD/PROFITCURRENCY (e.g., USDCAD)
#                     indirect_conversion_symbol = f"USD{profit_currency}"
#                     conversion_data_indirect = adjusted_market_prices.get(indirect_conversion_symbol)
#                     if conversion_data_indirect:
#                         try:
#                             live_buy_price_indirect = Decimal(str(conversion_data_indirect.get('buy', 0.0)))
#                             if live_buy_price_indirect > Decimal("0.0"):
#                                 position_profit_loss_usd = position_profit_loss_native / live_buy_price_indirect
#                                 conversion_successful = True
#                                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Converted PnL using {indirect_conversion_symbol} buy price: {position_profit_loss_native} {profit_currency} / {live_buy_price_indirect} = {position_profit_loss_usd} USD")
#                             else:
#                                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Invalid buy price (<=0) for indirect conversion symbol {indirect_conversion_symbol}: {live_buy_price_indirect}. Cannot divide by zero.")
#                         except InvalidOperation as e:
#                             logger.error(f"User {user_data.get('id')}, Pos {position_id}: Error converting indirect conversion rate for {indirect_conversion_symbol} to Decimal: {e}", exc_info=True)
                    
#                 if not conversion_successful:
#                     logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Could not find valid conversion rate from {profit_currency} to USD. PnL for this position will remain in {profit_currency} for total PnL sum if it's the only non-USD currency, or might cause issues if mixed. Current PnL (native): {position_profit_loss_native}")
#                     # If no conversion is possible, PnL remains in its native currency.
#                     # This could lead to issues if multiple non-USD PnLs are summed without further handling.
#                     # For now, we'll add the native PnL to total_profit_loss, but this assumes total_profit_loss should be in USD.
#                     # A more robust solution would require all PnLs to be in a common currency (USD) or handle mixed currencies carefully.
#                     # Given the requirement "if there are no combination of live prices keep it as it is",
#                     # we proceed with the unconverted PnL. The impact on total_profit_loss will depend on other positions.
#                     # For simplicity in this function, if a position's PnL cannot be converted to USD,
#                     # we are still adding it to total_profit_loss. This implies total_profit_loss might become a mix if not careful.
#                     # However, the final portfolio object expects profit_loss in USD.
#                     # Let's assume if conversion fails, we can't reliably state its USD value for the sum yet.
#                     # To ensure total_profit_loss remains in USD, we should ideally skip adding unconverted PnL
#                     # or use a placeholder / log more critically.
#                     # For now, the code will use position_profit_loss_usd which will be same as position_profit_loss_native if conversion failed.
#                     pass # PnL remains position_profit_loss_usd (which is position_profit_loss_native)
            
#             elif profit_currency == 'USD':
#                  logger.debug(f"User {user_data.get('id')}, Pos {position_id}: PnL is already in USD: {position_profit_loss_usd}")


#             # --- Calculate Commission and Deduct from PnL (USD) ---
#             commission_amount_usd = Decimal("0.0") # Commission should also be in USD
            
#             # Commission calculation itself might be based on native currency values, then converted if needed.
#             # Assuming commission_setting is in the base currency of the traded pair or a fixed USD value.
#             # For simplicity here, let's assume commission calculation is straightforward and results in USD.
#             # If commission is calculated in profit_currency, it would also need conversion.
#             # The prompt doesn't specify commission currency, assuming it's effectively USD or calculated to be.

#             if commision_type in [0, 1]: # 0: Every Trade, 1: In
#                 if commision_value_type == 0: # Total Value (per lot)
#                     commission_amount_usd = quantity * commission_setting # Assuming commission_setting is in USD or directly applicable
#                     logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated commission (Total Value): {quantity} * {commission_setting} = {commission_amount_usd} USD")
#                 elif commision_value_type == 1: # Percent
#                     # Percent of what? If entry_price is in a non-USD currency, this needs care.
#                     # ((commission_setting * entry_price_in_usd_equivalent) / 100) * quantity
#                     # For now, assuming this calculation is handled correctly to result in USD:
#                     if entry_price > Decimal("0.0"):
#                         # This calculation implies commission is a % of the value in the quote currency of the entry price.
#                         # If entry price is for EURUSD, then (commission_setting * entry_price_eurusd) / 100 is in USD.
#                         # If entry price is for AUDCAD, then (commission_setting * entry_price_audcad) / 100 is in CAD.
#                         # This amount would then need conversion to USD.
#                         # For simplicity, assuming the setup provides commission_setting that results in USD or the PnL currency.
#                         commission_on_trade_value = ((commission_setting * entry_price) / Decimal("100")) * quantity * contract_size # This is in the profit_currency
                        
#                         # Convert this commission to USD if profit_currency is not USD
#                         if profit_currency != 'USD':
#                             temp_commission_usd = commission_on_trade_value
#                             conv_success_comm = False
#                             direct_conv_sym_comm = f"{profit_currency}USD"
#                             conv_data_direct_comm = adjusted_market_prices.get(direct_conv_sym_comm)
#                             if conv_data_direct_comm:
#                                 try:
#                                     buy_price_direct_comm = Decimal(str(conv_data_direct_comm.get('buy', 0.0)))
#                                     if buy_price_direct_comm > Decimal("0.0"):
#                                         commission_amount_usd = commission_on_trade_value * buy_price_direct_comm
#                                         conv_success_comm = True
#                                 except InvalidOperation: pass
                            
#                             if not conv_success_comm:
#                                 indirect_conv_sym_comm = f"USD{profit_currency}"
#                                 conv_data_indirect_comm = adjusted_market_prices.get(indirect_conv_sym_comm)
#                                 if conv_data_indirect_comm:
#                                     try:
#                                         buy_price_indirect_comm = Decimal(str(conv_data_indirect_comm.get('buy', 0.0)))
#                                         if buy_price_indirect_comm > Decimal("0.0"):
#                                             commission_amount_usd = commission_on_trade_value / buy_price_indirect_comm
#                                             conv_success_comm = True
#                                     except InvalidOperation: pass
                            
#                             if not conv_success_comm:
#                                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Could not convert commission from {profit_currency} to USD. Commission set to 0 USD for safety.")
#                                 commission_amount_usd = Decimal("0.0") # Default to 0 if conversion fails
#                             else:
#                                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Converted commission from {commission_on_trade_value} {profit_currency} to {commission_amount_usd} USD")

#                         else: # Profit currency is USD, so commission is already USD
#                             commission_amount_usd = commission_on_trade_value
                        
#                         logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated commission (Percent): Value in {profit_currency} {commission_on_trade_value}, converted to {commission_amount_usd} USD")
#                     else:
#                         logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Entry price is zero or less, cannot calculate percentage commission.")
#                 else:
#                     logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown commision_value_type '{commision_value_type}'. Cannot calculate commission.")

#                 position_profit_loss_usd -= commission_amount_usd # Deduct USD commission from USD PnL
#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Deducted commission {commission_amount_usd} USD. Position PnL (USD) after commission: {position_profit_loss_usd}")

#             elif commision_type == 2: # Out (Commission on close)
#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Commission Type is 'Out' (2). Commission will be applied on close.")
#             else:
#                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown commision_type '{commision_type}'. No commission applied.")

#             # Add the USD PnL of the position to the total USD PnL
#             total_profit_loss += position_profit_loss_usd 
#             logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Added position PnL (USD) to total. Current Total PnL (USD): {total_profit_loss}")

#             # --- Calculate Margin Used for the position (in USD) ---
#             # Margin formula: (Quantity * Contract Size * Market Price of base currency vs USD) / Leverage
#             # The margin price should be based on the base currency of the pair converted to USD.
#             # E.g., for EURUSD, margin price is current_market_price_of_EURUSD (which is already USD rate for EUR)
#             # E.g., for AUDCAD, margin price is current_market_price_of_AUDUSD.
#             # Assuming symbol is like "BASEQUOTE", e.g. "EURUSD", "AUDCAD"
            
#             position_margin_used = Decimal("0.0")
#             base_currency = symbol_upper[:3] # e.g., EUR from EURUSD, AUD from AUDCAD
#             quote_currency = symbol_upper[3:]# e.g., USD from EURUSD, CAD from AUDCAD

#             margin_conversion_rate_to_usd = Decimal("1.0") # Default if base is USD

#             if base_currency != 'USD':
#                 margin_conversion_symbol_direct = f"{base_currency}USD" # e.g. AUDUSD
#                 margin_conv_data_direct = adjusted_market_prices.get(margin_conversion_symbol_direct)
                
#                 found_margin_rate = False
#                 if margin_conv_data_direct:
#                     try:
#                         # For margin, typically use the current market price of the base currency against USD
#                         # For a buy position of BASE/QUOTE, you are long BASE. Margin is on BASE.
#                         # For a sell position of BASE/QUOTE, you are short BASE. Margin is on BASE.
#                         # Use mid-price of BASE/USD for margin rate
#                         buy_rate = Decimal(str(margin_conv_data_direct.get('buy', 0.0)))
#                         sell_rate = Decimal(str(margin_conv_data_direct.get('sell', 0.0)))
#                         if buy_rate > Decimal("0.0") and sell_rate > Decimal("0.0"):
#                              margin_conversion_rate_to_usd = (buy_rate + sell_rate) / Decimal("2")
#                              found_margin_rate = True
#                              logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Margin conversion rate for {base_currency} from {margin_conversion_symbol_direct} (mid): {margin_conversion_rate_to_usd}")
#                         else:
#                             logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Invalid rates for {margin_conversion_symbol_direct} for margin calc.")
#                     except InvalidOperation:
#                         logger.error(f"User {user_data.get('id')}, Pos {position_id}: InvalidOperation for {margin_conversion_symbol_direct} rates.")
                
#                 if not found_margin_rate:
#                     margin_conversion_symbol_indirect = f"USD{base_currency}" # e.g. USDAUD
#                     margin_conv_data_indirect = adjusted_market_prices.get(margin_conversion_symbol_indirect)
#                     if margin_conv_data_indirect:
#                         try:
#                             buy_rate = Decimal(str(margin_conv_data_indirect.get('buy', 0.0)))
#                             sell_rate = Decimal(str(margin_conv_data_indirect.get('sell', 0.0)))
#                             if buy_rate > Decimal("0.0") and sell_rate > Decimal("0.0"):
#                                 mid_rate_indirect = (buy_rate + sell_rate) / Decimal("2")
#                                 if mid_rate_indirect > Decimal("0.0"):
#                                     margin_conversion_rate_to_usd = Decimal("1.0") / mid_rate_indirect
#                                     found_margin_rate = True
#                                     logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Margin conversion rate for {base_currency} from {margin_conversion_symbol_indirect} (inverted mid): {margin_conversion_rate_to_usd}")
#                                 else:
#                                      logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Invalid mid rate for {margin_conversion_symbol_indirect} for margin calc.")
#                             else:
#                                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Invalid rates for {margin_conversion_symbol_indirect} for margin calc.")
#                         except InvalidOperation:
#                              logger.error(f"User {user_data.get('id')}, Pos {position_id}: InvalidOperation for {margin_conversion_symbol_indirect} rates.")
                
#                 if not found_margin_rate:
#                     logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Could not find USD conversion rate for base currency {base_currency} for margin calculation. Margin for this position will be 0.")
#                     margin_conversion_rate_to_usd = Decimal("0.0") # Cannot calculate margin accurately

#             # Nominal value of the trade in USD = Quantity * Contract Size * Rate_of_BaseCurrency_to_USD
#             nominal_value_usd = quantity * contract_size * margin_conversion_rate_to_usd
            
#             if leverage > Decimal("0.0"):
#                 position_margin_used = nominal_value_usd / leverage
#                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated Margin Used (USD): ({quantity} * {contract_size} * {margin_conversion_rate_to_usd}) / {leverage} = {position_margin_used}")
#             else:
#                 position_margin_used = Decimal("0.0") # Should not happen due to earlier check, but as a safeguard.
#                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Leverage is zero or less ({leverage}). Margin calculation results in zero.")

#             total_margin_used += position_margin_used
#             logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Added position margin to total. Current Total Margin Used (USD): {total_margin_used}")

#             # Add calculated PnL (USD) and commission amount (USD) to the position data for the client
#             position['profit_loss'] = float(position_profit_loss_usd)
#             position['commission_applied'] = float(commission_amount_usd) 

#             pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
#             updated_positions.append(pos_to_append)

#         except Exception as e:
#             logger.error(f"User {user_data.get('id')}, Pos {position.get('order_id', 'N/A')}: Error calculating PnL/Margin/Commission: {e}", exc_info=True)
#             pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
#             # Add default PnL and commission to avoid missing keys if an error occurs mid-calculation for a position
#             pos_to_append.setdefault('profit_loss', 0.0)
#             pos_to_append.setdefault('commission_applied', 0.0)
#             updated_positions.append(pos_to_append)

#     # --- Calculate Total Portfolio Metrics ---
#     equity = balance + total_profit_loss # total_profit_loss is now in USD
#     free_margin = equity - total_margin_used # total_margin_used is now in USD

#     logger.debug(f"User {user_data.get('id')}: Final Calculations - Balance (USD): {balance}, Total PnL (USD): {total_profit_loss}, Equity (USD): {equity}, Total Margin Used (USD): {total_margin_used}, Free Margin (USD): {free_margin}")

#     calculated_portfolio = {
#         "balance": float(balance), 
#         "equity": float(equity),   
#         "margin": float(total_margin_used), 
#         "free_margin": float(free_margin), 
#         "profit_loss": float(total_profit_loss), 
#         "positions": updated_positions 
#     }

#     logger.debug(f"User {user_data.get('id')}: Calculated portfolio result: {calculated_portfolio}")

#     return calculated_portfolio

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation

# For logging to a specific file (e.g., app/logger.py),
# this needs to be configured when the application starts up,
# for example, in your main.py or a dedicated logging setup module.
# Example (place in your app's entry point, not here):
# import logging.handlers
# app_logger = logging.getLogger() # Get the root logger
# app_logger.setLevel(logging.DEBUG)
# # Path assuming 'app' is in the Python path or relative to where the app runs
# handler = logging.handlers.RotatingFileHandler('app/logger.py', maxBytes=10*1024*1024, backupCount=5)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# app_logger.addHandler(handler)

logger = logging.getLogger(__name__)

STANDARD_LOT_SIZE = Decimal("100000")

async def _convert_to_usd(
    amount: Decimal,
    from_currency: str,
    adjusted_market_prices: Dict[str, Dict[str, float]],
    user_id: Any,
    position_id: Any,
    value_description: str # e.g., "PnL", "Commission"
) -> Decimal:
    """Helper function to convert an amount from a given currency to USD."""
    from_currency_upper = from_currency.upper()

    # ADD DETAILED LOGGING HERE
    logger.debug(
        f"User {user_id}, Pos {position_id}: _convert_to_usd CALLED for '{value_description}'. "
        f"Amount: {amount}, FromCurrency: {from_currency_upper}. "
        f"Is from_currency_upper 'USD'? {from_currency_upper == 'USD'}. Is amount zero? {amount == Decimal('0.0')}"
    )

    if from_currency_upper == 'USD' or amount == Decimal("0.0"):
        logger.debug(f"User {user_id}, Pos {position_id}: _convert_to_usd - No conversion needed for {value_description}.")
        return amount

    converted_amount_usd = amount # Default to original amount if conversion fails
    conversion_successful = False

    # Try to find FROMCURRENCY/USD (e.g., CADUSD)
    direct_conversion_symbol = f"{from_currency_upper}USD"
    logger.debug(
        f"User {user_id}, Pos {position_id}: _convert_to_usd - Attempting direct conversion with {direct_conversion_symbol}. "
        f"Is '{direct_conversion_symbol}' in adjusted_market_prices? {direct_conversion_symbol in adjusted_market_prices}"
    )
    if direct_conversion_symbol in adjusted_market_prices:
        logger.debug(f"User {user_id}, Pos {position_id}: _convert_to_usd - Data for '{direct_conversion_symbol}': {adjusted_market_prices.get(direct_conversion_symbol)}")

    conversion_data_direct = adjusted_market_prices.get(direct_conversion_symbol)

    if conversion_data_direct:
        try:
            live_buy_price_direct = Decimal(str(conversion_data_direct.get('buy', 0.0)))
            logger.debug(f"User {user_id}, Pos {position_id}: _convert_to_usd - Direct {direct_conversion_symbol} buy price: {live_buy_price_direct}")
            if live_buy_price_direct > Decimal("0.0"):
                converted_amount_usd = amount * live_buy_price_direct
                conversion_successful = True
                logger.info(f"User {user_id}, Pos {position_id}: CONVERTED (direct) {value_description} using {direct_conversion_symbol} buy price: {amount} {from_currency_upper} * {live_buy_price_direct} = {converted_amount_usd} USD") # Changed to INFO for visibility
            else:
                logger.warning(f"User {user_id}, Pos {position_id}: _convert_to_usd - Invalid buy price (<=0) for direct conversion symbol {direct_conversion_symbol} for {value_description}: {live_buy_price_direct}")
        except InvalidOperation as e:
            logger.error(f"User {user_id}, Pos {position_id}: _convert_to_usd - Error converting direct rate for {direct_conversion_symbol} to Decimal for {value_description}: {e}", exc_info=True)

    if not conversion_successful:
        indirect_conversion_symbol = f"USD{from_currency_upper}"
        logger.debug(
            f"User {user_id}, Pos {position_id}: _convert_to_usd - Attempting indirect conversion with {indirect_conversion_symbol}. "
            f"Is '{indirect_conversion_symbol}' in adjusted_market_prices? {indirect_conversion_symbol in adjusted_market_prices}"
        )
        if indirect_conversion_symbol in adjusted_market_prices:
             logger.debug(f"User {user_id}, Pos {position_id}: _convert_to_usd - Data for '{indirect_conversion_symbol}': {adjusted_market_prices.get(indirect_conversion_symbol)}")

        conversion_data_indirect = adjusted_market_prices.get(indirect_conversion_symbol)
        if conversion_data_indirect:
            try:
                live_buy_price_indirect = Decimal(str(conversion_data_indirect.get('buy', 0.0)))
                logger.debug(f"User {user_id}, Pos {position_id}: _convert_to_usd - Indirect {indirect_conversion_symbol} buy price: {live_buy_price_indirect}")
                if live_buy_price_indirect > Decimal("0.0"):
                    converted_amount_usd = amount / live_buy_price_indirect
                    conversion_successful = True
                    logger.info(f"User {user_id}, Pos {position_id}: CONVERTED (indirect) {value_description} using {indirect_conversion_symbol} buy price: {amount} {from_currency_upper} / {live_buy_price_indirect} = {converted_amount_usd} USD") # Changed to INFO for visibility
                else:
                    logger.warning(f"User {user_id}, Pos {position_id}: _convert_to_usd - Invalid buy price (<=0) for indirect conversion symbol {indirect_conversion_symbol} for {value_description}: {live_buy_price_indirect}. Cannot divide by zero.")
            except InvalidOperation as e:
                logger.error(f"User {user_id}, Pos {position_id}: _convert_to_usd - Error converting indirect rate for {indirect_conversion_symbol} to Decimal for {value_description}: {e}", exc_info=True)

    if not conversion_successful:
        logger.warning(f"User {user_id}, Pos {position_id}: _convert_to_usd - FAILED to find valid conversion rate from {from_currency_upper} to USD for {value_description}. {value_description} will remain {amount} {from_currency_upper}.")
        return amount 

    return converted_amount_usd


async def calculate_user_portfolio(
    user_data: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, float]],
    group_symbol_settings: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculates dynamic user portfolio details.
    Commission is calculated in native currency and deducted from native PnL.
    The resulting net PnL is then converted to USD.
    Commission applied is also converted to USD for reporting.
    """
    user_id = user_data.get('id', 'UNKNOWN_USER')
    try:
        balance = Decimal(str(user_data.get("wallet_balance", 0.0)))
        leverage = Decimal(str(user_data.get("leverage", 1.0)))
        if leverage <= Decimal("0.0"):
            logger.warning(f"User {user_id}: Leverage is {leverage}, defaulting to 1.0.")
            leverage = Decimal("1.0")
        logger.debug(f"User {user_id}: Initial Balance: {balance}, Leverage: {leverage}")
    except InvalidOperation as e:
        logger.error(f"User {user_id}: Error converting user data to Decimal: {e}. User data: {user_data}", exc_info=True)
        balance = Decimal("0.0")
        leverage = Decimal("1.0")

    total_pnl_usd = Decimal("0.0")
    total_margin_used_usd = Decimal("0.0")
    updated_positions_list = []

    logger.debug(f"User {user_id}: Calculating portfolio for {len(open_positions)} open positions.")

    for position in open_positions:
        position_id = position.get('order_id', 'N/A')
        pnl_for_position_usd = Decimal("0.0")
        commission_for_position_usd = Decimal("0.0")

        try:
            symbol = position.get("order_company_name")
            order_type = position.get("order_type")
            quantity = Decimal(str(position.get("order_quantity", 0.0)))
            entry_price = Decimal(str(position.get("order_price", 0.0)))

            logger.debug(f"User {user_id}, Pos {position_id}: Processing - Sym: {symbol}, Type: {order_type}, Qty: {quantity}, Entry: {entry_price}")

            if not symbol or not order_type or quantity <= Decimal("0.0") or entry_price <= Decimal("0.0"):
                logger.warning(f"User {user_id}, Pos {position_id}: Skipping invalid position data: {position}")
                pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                pos_to_append.setdefault('profit_loss', 0.0)
                pos_to_append.setdefault('commission_applied', 0.0)
                updated_positions_list.append(pos_to_append)
                continue

            symbol_upper = symbol.upper()
            market_price_info = adjusted_market_prices.get(symbol_upper)
            symbol_settings = group_symbol_settings.get(symbol_upper) if isinstance(group_symbol_settings, dict) else None

            if not market_price_info or not symbol_settings:
                logger.warning(f"User {user_id}, Pos {position_id}: Market data or group settings missing for {symbol_upper}.")
                pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                pos_to_append.setdefault('profit_loss', 0.0)
                pos_to_append.setdefault('commission_applied', 0.0)
                updated_positions_list.append(pos_to_append)
                continue
            
            current_buy_price = Decimal(str(market_price_info.get('buy', 0.0)))
            current_sell_price = Decimal(str(market_price_info.get('sell', 0.0)))
            contract_size = Decimal(str(symbol_settings.get('contract_size', STANDARD_LOT_SIZE)))
            profit_currency = symbol_settings.get('profit_currency', 'USD').upper()

            # --- 1. Calculate PnL in Native Currency ---
            pnl_native = Decimal("0.0")
            if order_type.lower() == 'buy':
                pnl_native = (current_sell_price - entry_price) * quantity * contract_size
            elif order_type.lower() == 'sell':
                pnl_native = (entry_price - current_buy_price) * quantity * contract_size
            else:
                logger.warning(f"User {user_id}, Pos {position_id}: Unknown order type '{order_type}'.")
            logger.debug(f"User {user_id}, Pos {position_id}: PnL (Native: {profit_currency}): {pnl_native}")

            # --- 2. Calculate Commission in Native Currency ---
            # Using original typo'd keys as requested
            commission_type_setting = symbol_settings.get('commision_type')
            commission_value_type_setting = symbol_settings.get('commision_value_type')
            commission_rate_setting = Decimal(str(symbol_settings.get('commision', 0.0)))
            
            commission_native = Decimal("0.0")
            if commission_type_setting in [0, 1]: # 0: Every Trade, 1: In
                if commission_value_type_setting == 0: # Total Value (per lot)
                    commission_native = quantity * commission_rate_setting
                elif commission_value_type_setting == 1: # Percent (of entry_price * quantity, as per original logic)
                    if entry_price > Decimal("0.0"):
                        commission_native = ((commission_rate_setting * entry_price) / Decimal("100")) * quantity
                    else:
                        logger.warning(f"User {user_id}, Pos {position_id}: Entry price is zero, cannot calculate percentage commission.")
                else:
                    logger.warning(f"User {user_id}, Pos {position_id}: Unknown commission_value_type '{commission_value_type_setting}'.")
            elif commission_type_setting == 2: # Out
                logger.debug(f"User {user_id}, Pos {position_id}: Commission Type 'Out', applied on close.")
            else:
                logger.warning(f"User {user_id}, Pos {position_id}: Unknown commission_type '{commission_type_setting}'.")
            
            logger.debug(f"User {user_id}, Pos {position_id}: Commission (Native: {profit_currency}): {commission_native}")

            # --- 3. Deduct Native Commission from Native PnL ---
            net_pnl_native = pnl_native - commission_native
            logger.debug(f"User {user_id}, Pos {position_id}: Net PnL (Native: {profit_currency}, After Commission): {net_pnl_native}")

            # --- 4. Convert Net Native PnL to USD ---
            pnl_for_position_usd = await _convert_to_usd(net_pnl_native, profit_currency, adjusted_market_prices, user_id, position_id, "Net PnL")
            logger.debug(f"User {user_id}, Pos {position_id}: Net PnL (USD): {pnl_for_position_usd}")
            
            # --- 5. Convert Native Commission to USD (for reporting) ---
            commission_for_position_usd = await _convert_to_usd(commission_native, profit_currency, adjusted_market_prices, user_id, position_id, "Commission")
            logger.debug(f"User {user_id}, Pos {position_id}: Commission (USD): {commission_for_position_usd}")

            total_pnl_usd += pnl_for_position_usd

            # --- Calculate Margin Used for the position (in USD) ---
            margin_used_for_position_usd = Decimal("0.0")
            base_currency = symbol_upper[:3]
            
            margin_conversion_rate_to_usd = Decimal("1.0") # Default if base is USD
            if base_currency != 'USD':
                # Try BASE/USD first (e.g., EURUSD)
                base_usd_pair = f"{base_currency}USD"
                base_usd_data = adjusted_market_prices.get(base_usd_pair)
                found_rate = False
                if base_usd_data:
                    try:
                        buy_r = Decimal(str(base_usd_data.get('buy', 0.0)))
                        sell_r = Decimal(str(base_usd_data.get('sell', 0.0)))
                        if buy_r > Decimal(0) and sell_r > Decimal(0):
                            margin_conversion_rate_to_usd = (buy_r + sell_r) / Decimal(2)
                            found_rate = True
                            logger.debug(f"User {user_id}, Pos {position_id}: Margin rate for {base_currency} via {base_usd_pair} (mid): {margin_conversion_rate_to_usd}")
                    except InvalidOperation: pass # log implicitly by _convert_to_usd if used there
                
                if not found_rate:
                    # Try USD/BASE second (e.g., USDJPY)
                    usd_base_pair = f"USD{base_currency}"
                    usd_base_data = adjusted_market_prices.get(usd_base_pair)
                    if usd_base_data:
                        try:
                            buy_r = Decimal(str(usd_base_data.get('buy', 0.0)))
                            sell_r = Decimal(str(usd_base_data.get('sell', 0.0)))
                            if buy_r > Decimal(0) and sell_r > Decimal(0):
                                mid_rate_inv = (buy_r + sell_r) / Decimal(2)
                                if mid_rate_inv > Decimal(0):
                                    margin_conversion_rate_to_usd = Decimal(1) / mid_rate_inv
                                    found_rate = True
                                    logger.debug(f"User {user_id}, Pos {position_id}: Margin rate for {base_currency} via {usd_base_pair} (inverted mid): {margin_conversion_rate_to_usd}")
                        except InvalidOperation: pass
                
                if not found_rate:
                    logger.warning(f"User {user_id}, Pos {position_id}: Could not find USD conversion rate for base currency {base_currency} for margin. Margin calc may be inaccurate (rate defaulted to 1.0 or previous value). Setting to 0 for safety.")
                    margin_conversion_rate_to_usd = Decimal("0.0") # Safer to use 0 if no rate

            nominal_value_usd = quantity * contract_size * margin_conversion_rate_to_usd
            if leverage > Decimal("0.0"):
                margin_used_for_position_usd = nominal_value_usd / leverage
            
            logger.debug(f"User {user_id}, Pos {position_id}: Margin Used (USD): {margin_used_for_position_usd} (Nominal USD: {nominal_value_usd}, Rate: {margin_conversion_rate_to_usd})")
            total_margin_used_usd += margin_used_for_position_usd

            # Update position dict
            current_pos_data = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            current_pos_data['profit_loss'] = float(pnl_for_position_usd)
            current_pos_data['commission_applied'] = float(commission_for_position_usd)
            updated_positions_list.append(current_pos_data)

        except InvalidOperation as e:
            logger.error(f"User {user_id}, Pos {position_id}: Invalid Decimal operation: {e}. Position data: {position}", exc_info=True)
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)
        except Exception as e:
            logger.error(f"User {user_id}, Pos {position_id}: Generic error processing position: {e}. Data: {position}", exc_info=True)
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)

    equity = balance + total_pnl_usd
    free_margin = equity - total_margin_used_usd

    logger.debug(f"User {user_id}: Final Portfolio - Balance: {balance}, Total PnL (USD): {total_pnl_usd}, Equity: {equity}, Total Margin (USD): {total_margin_used_usd}, Free Margin: {free_margin}")

    return {
        "balance": float(balance),
        "equity": float(equity),
        "margin": float(total_margin_used_usd),
        "free_margin": float(free_margin),
        "profit_loss": float(total_pnl_usd),
        "positions": updated_positions_list
    }