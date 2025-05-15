# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# Assuming a standard lot size (e.g., 100,000 units per standard lot)
# This might need to be configurable per symbol or group
STANDARD_LOT_SIZE = Decimal("100000")

async def calculate_user_portfolio(
    user_data: Dict[str, Any],
    open_positions: List[Dict[str, Any]], # List of open position dictionaries (from cached portfolio)
    adjusted_market_prices: Dict[str, Dict[str, float]], # Adjusted market prices from broadcaster
    group_symbol_settings: Dict[str, Dict[str, Any]] # Group settings for relevant symbols
) -> Dict[str, Any]:
    """
    Calculates dynamic user portfolio details (equity, margin, free_margin, profit_loss)
    including commission deduction based on group settings.

    Args:
        user_data: Dictionary containing user details (e.g., balance, leverage).
        open_positions: List of dictionaries representing the user's open orders/positions.
                        Each dict should contain keys like 'order_id', 'order_company_name' (symbol),
                        'order_type' ('Buy'/'Sell'), 'order_quantity', 'order_price'.
        adjusted_market_prices: Dictionary keyed by symbol (uppercase) with adjusted
                                buy/sell prices and spread_value {'symbol': {'buy': float, 'sell': float, 'spread_value': float}}.
        group_symbol_settings: Dictionary keyed by symbol (uppercase) with group settings
                                {'symbol': {'margin': Decimal, 'pips': Decimal, 'spread_pip': Decimal, 'commission_type': int, 'commission_value_type': int, 'commission': Decimal, ...}}.

    Returns:
        A dictionary containing the calculated portfolio metrics:
        'balance', 'equity', 'margin', 'free_margin', 'profit_loss', 'positions' (updated with PnL).
    """
    # Ensure Decimal types for calculations
    try:
        balance = Decimal(str(user_data.get("wallet_balance", 0.0)))
        leverage = Decimal(str(user_data.get("leverage", 1.0)))
        if leverage == Decimal("0.0"): # Avoid division by zero
             leverage = Decimal("1.0")
        logger.debug(f"User {user_data.get('id')}: Initial Balance: {balance}, Leverage: {leverage}")
    except InvalidOperation as e:
        logger.error(f"Error converting user data to Decimal: {e}. User data: {user_data}", exc_info=True)
        balance = Decimal("0.0")
        leverage = Decimal("1.0") # Default to 1.0 on error

    total_profit_loss = Decimal("0.0")
    total_margin_used = Decimal("0.0")
    updated_positions = [] # To store positions with calculated PnL

    logger.debug(f"User {user_data.get('id')}: Calculating portfolio for {len(open_positions)} open positions.")

    # Iterate through each open position
    for position in open_positions:
        try:
            symbol = position.get("order_company_name") # Assuming this is the symbol
            order_type = position.get("order_type") # 'Buy' or 'Sell'
            # Ensure quantity and price are Decimal
            quantity = Decimal(str(position.get("order_quantity", 0.0)))
            entry_price = Decimal(str(position.get("order_price", 0.0)))
            position_id = position.get('order_id', 'N/A') # Get position ID for logging

            logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Processing position - Symbol: {symbol}, Type: {order_type}, Qty: {quantity}, Entry Price: {entry_price}")


            if not symbol or not order_type or quantity <= Decimal("0.0") or entry_price <= Decimal("0.0"):
                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Skipping invalid position data: {position}")
                 # Append original position data, ensuring Decimal fields are converted back to str for consistency with cache
                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                 updated_positions.append(pos_to_append)
                 continue

            symbol_upper = symbol.upper()
            current_market_price_info = adjusted_market_prices.get(symbol_upper)
            symbol_settings = group_symbol_settings.get(symbol_upper) if isinstance(group_symbol_settings, dict) else None

            logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Market data available: {current_market_price_info is not None}, Group settings available: {symbol_settings is not None}")


            if not current_market_price_info or not symbol_settings:
                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Market data or group settings missing for symbol {symbol_upper}. Cannot calculate PnL/Margin/Commission.")
                 # Append original position data, ensuring Decimal fields are converted back to str
                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                 updated_positions.append(pos_to_append)
                 continue

            # Ensure market prices and settings are Decimal
            try:
                current_buy_price = Decimal(str(current_market_price_info.get('buy', 0.0)))
                current_sell_price = Decimal(str(current_market_price_info.get('sell', 0.0)))
                # Use group settings margin for calculation
                group_margin_setting = Decimal(str(symbol_settings.get('margin', 0.0)))
                pips_setting = Decimal(str(symbol_settings.get('pips', 0.0))) # Pips value for symbol
                spread_pip_setting = Decimal(str(symbol_settings.get('spread_pip', 0.0))) # Spread pip value
                contract_size = Decimal(str(symbol_settings.get('contract_size', STANDARD_LOT_SIZE))) # Assuming contract_size is in settings or use default

                # Get commission related settings
                commission_type = symbol_settings.get('commision_type') # Note the typo 'commision_type' from models
                commission_value_type = symbol_settings.get('commision_value_type') # Note the typo 'commision_value_type' from models
                commission_setting = Decimal(str(symbol_settings.get('commision', 0.0))) # Note the typo 'commision' from models

                logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Market Prices - Buy: {current_buy_price}, Sell: {current_sell_price}. Settings - Margin: {group_margin_setting}, Pips: {pips_setting}, Spread Pip: {spread_pip_setting}, Contract Size: {contract_size}. Commission - Type: {commission_type}, Value Type: {commission_value_type}, Setting: {commission_setting}")


            except InvalidOperation as e:
                 logger.error(f"User {user_data.get('id')}, Pos {position_id}: Error converting market data or settings to Decimal for symbol {symbol_upper}: {e}", exc_info=True)
                 # Append original position data, ensuring Decimal fields are converted back to str
                 pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                 updated_positions.append(pos_to_append)
                 continue


            # --- Calculate Profit/Loss for the position (before commission) ---
            position_profit_loss = Decimal("0.0")
            if order_type.lower() == 'buy':
                # PnL for Buy = (Current Sell Price - Entry Price) * Quantity * Contract Size
                position_profit_loss = (current_sell_price - entry_price) * quantity * contract_size
                logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated PnL (Buy): ({current_sell_price} - {entry_price}) * {quantity} * {contract_size} = {position_profit_loss}")
            elif order_type.lower() == 'sell':
                # PnL for Sell = (Entry Price - Current Buy Price) * Quantity * Contract Size
                position_profit_loss = (entry_price - current_buy_price) * quantity * contract_size
                logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated PnL (Sell): ({entry_price} - {current_buy_price}) * {quantity} * {contract_size} = {position_profit_loss}")
            else:
                logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown order type '{order_type}'. Cannot calculate PnL.")


            # --- Calculate Commission and Deduct from PnL ---
            commission_amount = Decimal("0.0")
            # commission_type = 0 (Every Trade), 1 (In), 2 (Out)
            # We only deduct commission for types 0 and 1 here (on open)
            if commission_type in [0, 1]:
                # commission_value_type = 0 (Total Value), 1 (Percent)
                if commission_value_type == 0: # Total Value
                    # Formula: quantity * commission_setting
                    commission_amount = quantity * commission_setting
                    logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated commission (Total Value): {quantity} * {commission_setting} = {commission_amount}")
                elif commission_value_type == 1: # Percent
                    # Formula: ((commission_setting * entry_price) / 100) * quantity
                    if entry_price > Decimal("0.0"): # Avoid division by zero
                        commission_amount = ((commission_setting * entry_price) / Decimal("100")) * quantity
                        logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated commission (Percent): (({commission_setting} * {entry_price}) / 100) * {quantity} = {commission_amount}")
                    else:
                         logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Entry price is zero or less, cannot calculate percentage commission.")
                else:
                    logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown commission_value_type '{commission_value_type}'. Cannot calculate commission.")

                # Deduct commission from the position's PnL
                position_profit_loss -= commission_amount
                logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Deducted commission {commission_amount}. Position PnL after commission: {position_profit_loss}")

            elif commission_type == 2: # Out (Commission on close)
                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Commission Type is 'Out' (2). Commission will be applied on close.")
                 pass # No commission deduction here

            else:
                logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Unknown commission_type '{commission_type}'. No commission applied.")

            total_profit_loss += position_profit_loss
            logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Added position PnL to total. Current Total PnL: {total_profit_loss}")


            # --- Calculate Margin Used for the position ---
            # Margin formula: (Quantity * Contract Size * Market Price) / Leverage
            # Use the relevant market price based on order type for margin calculation
            margin_price = current_buy_price if order_type.lower() == 'buy' else current_sell_price
            if leverage > Decimal("0.0"):
                 position_margin_used = (quantity * contract_size * margin_price) / leverage
                 logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Calculated Margin Used: ({quantity} * {contract_size} * {margin_price}) / {leverage} = {position_margin_used}")
            else:
                 position_margin_used = Decimal("0.0")
                 logger.warning(f"User {user_data.get('id')}, Pos {position_id}: Leverage is zero or less ({leverage}). Margin calculation skipped.")

            total_margin_used += position_margin_used
            logger.debug(f"User {user_data.get('id')}, Pos {position_id}: Added position margin to total. Current Total Margin Used: {total_margin_used}")


            # Add calculated PnL and commission amount to the position data for the client
            # Convert Decimal to float for JSON serialization
            position['profit_loss'] = float(position_profit_loss)
            position['commission_applied'] = float(commission_amount) # Add commission amount applied on open

            # Append the updated position dictionary (ensure Decimal fields are converted back to str)
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            updated_positions.append(pos_to_append)


        except Exception as e:
            logger.error(f"User {user_data.get('id')}, Pos {position.get('order_id', 'N/A')}: Error calculating PnL/Margin/Commission: {e}", exc_info=True)
            # Append original position data on error, ensuring Decimal fields are converted back to str
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            updated_positions.append(pos_to_append)


    # --- Calculate Total Portfolio Metrics ---
    equity = balance + total_profit_loss
    free_margin = equity - total_margin_used
    # Ensure free_margin doesn't go below zero (though in practice it can become negative leading to margin call)
    # free_margin = max(Decimal("0.0"), equity - total_margin_used) # Optional: if you want to show 0 instead of negative

    logger.debug(f"User {user_data.get('id')}: Final Calculations - Balance: {balance}, Total PnL: {total_profit_loss}, Equity: {equity}, Total Margin Used: {total_margin_used}, Free Margin: {free_margin}")

    calculated_portfolio = {
        "balance": float(balance), # Convert Decimal to float for JSON
        "equity": float(equity),   # Convert Decimal to float for JSON
        "margin": float(total_margin_used), # Convert Decimal to float for JSON
        "free_margin": float(free_margin), # Convert Decimal to float for JSON
        "profit_loss": float(total_profit_loss), # Convert Decimal to float for JSON
        "positions": updated_positions # List of positions with PnL and commission
    }

    logger.debug(f"User {user_data.get('id')}: Calculated portfolio result: {calculated_portfolio}")

    return calculated_portfolio
