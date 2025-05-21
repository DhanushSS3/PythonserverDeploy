# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # Ensure ROUND_HALF_UP is imported

# Import for raw market data
from app.firebase_stream import get_latest_market_data

logger = logging.getLogger(__name__)

async def _convert_to_usd(
    amount: Decimal,
    from_currency: str,
    # removed: adjusted_market_prices: Dict[str, Dict[str, float]],
    user_id: Any,
    position_id: Any,
    value_description: str
) -> Decimal:
    """Helper function to convert an amount from a given currency to USD using raw market prices."""
    from_currency_upper = from_currency.upper()
    logger.debug(
        f"User {user_id}, Pos {position_id}: RAW _convert_to_usd CALLED for '{value_description}'. "
        f"Amount: {amount}, FromCurrency: {from_currency_upper}."
    )

    if from_currency_upper == 'USD' or amount == Decimal("0.0"):
        logger.debug(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - No conversion needed for {value_description}.")
        return amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Ensure consistent precision even if no conversion

    converted_amount_usd = amount # Default to original amount if conversion fails
    conversion_successful = False

    # Option 1: Direct pair like EURUSD (Price is USD per 1 EUR)
    # To convert EUR_amount to USD_amount: EUR_amount * EURUSD_rate (use bid for selling EUR)
    direct_conversion_symbol = f"{from_currency_upper}USD"
    raw_direct_prices = get_latest_market_data(direct_conversion_symbol) # Fetch raw prices
    logger.debug(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Attempting direct with {direct_conversion_symbol}. Data: {raw_direct_prices}")

    if raw_direct_prices and 'b' in raw_direct_prices: # 'b' for bid price
        try:
            rate_str = raw_direct_prices['b']
            if rate_str is None: # Explicit check for None before Decimal conversion
                logger.warning(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Raw bid price for {direct_conversion_symbol} is None.")
            else:
                rate = Decimal(str(rate_str))
                if rate > Decimal("0.0"):
                    converted_amount_usd = amount * rate
                    conversion_successful = True
                    logger.info(f"User {user_id}, Pos {position_id}: RAW CONVERTED (direct) {value_description} using {direct_conversion_symbol} bid price: {amount} {from_currency_upper} * {rate} = {converted_amount_usd} USD")
                else:
                    logger.warning(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Invalid raw bid price (<=0) for {direct_conversion_symbol}: {rate}")
        except (InvalidOperation, TypeError) as e:
            logger.error(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Error converting direct raw rate for {direct_conversion_symbol}: {e}", exc_info=True)
    
    if not conversion_successful:
        # Option 2: Indirect pair like USDCAD (Price is CAD per 1 USD)
        # To convert CAD_amount to USD_amount: CAD_amount / USDCAD_rate (use offer for buying USD in denominator)
        indirect_conversion_symbol = f"USD{from_currency_upper}"
        raw_indirect_prices = get_latest_market_data(indirect_conversion_symbol) # Fetch raw prices
        logger.debug(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Attempting indirect with {indirect_conversion_symbol}. Data: {raw_indirect_prices}")

        if raw_indirect_prices and 'o' in raw_indirect_prices: # 'o' for offer price
            try:
                rate_str = raw_indirect_prices['o']
                if rate_str is None:  # Explicit check for None
                    logger.warning(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Raw offer price for {indirect_conversion_symbol} is None.")
                else:
                    rate = Decimal(str(rate_str))
                    if rate > Decimal("0.0"):
                        converted_amount_usd = amount / rate
                        conversion_successful = True
                        logger.info(f"User {user_id}, Pos {position_id}: RAW CONVERTED (indirect) {value_description} using {indirect_conversion_symbol} offer price: {amount} {from_currency_upper} / {rate} = {converted_amount_usd} USD")
                    else:
                        logger.warning(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Invalid raw offer price (<=0) for {indirect_conversion_symbol}: {rate}. Cannot divide by zero.")
            except (InvalidOperation, TypeError) as e:
                logger.error(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Error converting indirect raw rate for {indirect_conversion_symbol}: {e}", exc_info=True)

    if not conversion_successful:
        logger.warning(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - FAILED to find valid raw conversion rate from {from_currency_upper} to USD for {value_description}. {value_description} will remain {amount} {from_currency_upper}.")
        return amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Return original amount, quantized

    return converted_amount_usd.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


async def calculate_user_portfolio(
    user_data: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, float]], # Still needed for PnL with user's spreads
    group_symbol_settings: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    user_id = user_data.get('id', 'UNKNOWN_USER')
    try:
        balance = Decimal(str(user_data.get("wallet_balance", 0.0)))
        leverage = Decimal(str(user_data.get("leverage", 1.0)))
        if leverage <= Decimal("0.0"):
            logger.warning(f"User {user_id}: Leverage is {leverage}, defaulting to 1.0.")
            leverage = Decimal("1.0")
    except InvalidOperation as e:
        logger.error(f"User {user_id}: Error converting user data to Decimal: {e}. User data: {user_data}", exc_info=True)
        balance = Decimal("0.0")
        leverage = Decimal("1.0")

    total_pnl_usd = Decimal("0.0")
    total_margin_used_usd = Decimal("0.0") # This will be the sum of individual position margins in USD
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
            entry_price = Decimal(str(position.get("order_price", 0.0))) # This is the adjusted entry price

            if not symbol or not order_type or quantity <= Decimal("0.0") or entry_price <= Decimal("0.0"):
                # ... (skip invalid position) ...
                pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                pos_to_append.setdefault('profit_loss', 0.0)
                pos_to_append.setdefault('commission_applied', 0.0)
                updated_positions_list.append(pos_to_append)
                continue

            symbol_upper = symbol.upper()
            # Use adjusted_market_prices for PnL calculation (reflects user's spread)
            market_price_info_for_pnl = adjusted_market_prices.get(symbol_upper)
            symbol_settings = group_symbol_settings.get(symbol_upper) if isinstance(group_symbol_settings, dict) else None

            if not market_price_info_for_pnl or not symbol_settings:
                # ... (skip if data missing) ...
                logger.warning(f"User {user_id}, Pos {position_id}: PnL Market data or group settings missing for {symbol_upper}.")
                pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                pos_to_append.setdefault('profit_loss', 0.0)
                pos_to_append.setdefault('commission_applied', 0.0)
                updated_positions_list.append(pos_to_append)
                continue

            current_buy_price_for_pnl = Decimal(str(market_price_info_for_pnl.get('buy', 0.0)))
            current_sell_price_for_pnl = Decimal(str(market_price_info_for_pnl.get('sell', 0.0)))
            contract_size = Decimal(str(symbol_settings.get('contract_size', Decimal("0.0"))))

            if contract_size <= Decimal("0.0"):
                logger.warning(f"User {user_id}, Pos {position_id}: Invalid contract_size '{contract_size}' for symbol {symbol_upper}. Setting to 1.0 for calculation safety.")
                contract_size = Decimal("1.0")

            profit_currency = symbol_settings.get('profit_currency', 'USD').upper()

            # --- 1. Calculate PnL in Native Currency using ADJUSRTED prices ---
            pnl_native = Decimal("0.0")
            if order_type.lower() == 'buy': # Buy order closes at current sell price
                pnl_native = (current_sell_price_for_pnl - entry_price) * quantity * contract_size
            elif order_type.lower() == 'sell': # Sell order closes at current buy price
                pnl_native = (entry_price - current_buy_price_for_pnl) * quantity * contract_size
            
            # --- 2. Convert PnL to USD (using new _convert_to_usd with RAW prices) ---
            pnl_for_position_usd = await _convert_to_usd(
                pnl_native, profit_currency, user_id, position_id, "Raw PnL"
            )
            
            # --- 3. Calculate Commission (assumed to be in USD) ---
            commission_usd = Decimal("0.0")
            # ... (existing commission calculation logic, ensure it results in USD) ...
            commission_type_setting = symbol_settings.get('commision_type')
            commission_value_type_setting = symbol_settings.get('commision_value_type')
            commission_rate_setting = Decimal(str(symbol_settings.get('commision', 0.0)))

            if commission_type_setting in [0, 1]: # 0: Every Trade, 1: In
                if commission_value_type_setting == 0: # Total Value (per lot)
                    commission_usd = quantity * commission_rate_setting # Assumed USD
                elif commission_value_type_setting == 1: # Percent
                    # This commission is on entry, so use entry_price. Assume this calc is USD.
                    commission_usd = ((commission_rate_setting * entry_price) / Decimal("100")) * quantity 
            # Ensure commission_usd is correctly calculated/converted if not already in USD.
            # For now, assuming the rate/logic implies USD as per existing structure.

            # --- 4. Deduct Commission from PnL ---
            pnl_for_position_usd -= commission_usd
            commission_for_position_usd = commission_usd # For reporting

            total_pnl_usd += pnl_for_position_usd

            # --- Calculate Margin Used for the position (in USD) ---
            # The margin for each position was already calculated in USD and stored in `position.get('margin')`
            # when the order was placed by `process_new_order` calling `calculate_single_order_margin`.
            # That `calculate_single_order_margin` will be updated to use raw rates for its *own* USD conversion.
            margin_for_this_position_usd = Decimal(str(position.get('margin', 0.0)))
            total_margin_used_usd += margin_for_this_position_usd
            logger.debug(f"User {user_id}, Pos {position_id}: Using pre-calculated margin (USD): {margin_for_this_position_usd}")


            current_pos_data = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            current_pos_data['profit_loss'] = float(pnl_for_position_usd)
            current_pos_data['commission_applied'] = float(commission_for_position_usd) # Commission applied at entry
            updated_positions_list.append(current_pos_data)

        except InvalidOperation as e:
            logger.error(f"User {user_id}, Pos {position_id}: Invalid Decimal operation in portfolio calc: {e}. Position: {position}", exc_info=True)
            # ... (error handling) ...
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)
        except Exception as e:
            logger.error(f"User {user_id}, Pos {position_id}: Generic error processing position in portfolio: {e}. Data: {position}", exc_info=True)
            # ... (error handling) ...
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)


    equity = balance + total_pnl_usd
    # The 'total_margin_used_usd' is the sum of margins of individual open positions.
    # For "hedged" margin display, if required at the account level, further logic might be needed
    # based on the overall portfolio, but individual position margins are summed up here.
    # The `user_data.get("margin", 0.0)` from market_data_ws is the *overall hedged margin* already.
    # Let's use the sum of individual margins for `total_margin_used_usd` here as calculated.
    # The `account_data.margin` sent over WebSocket uses `user_data.get("margin")`
    # which is the overall hedged margin updated by order placement/closing.
    # This `total_margin_used_usd` is more like "sum of individual required margins before overall hedging".
    # To be consistent with how margin is updated and displayed, we should use the overall margin
    # from the user_data if that's the expectation for the 'margin' field in the portfolio summary.
    # However, the prompt is about calculation. `total_margin_used_usd` as sum of individual margins is a valid metric.
    # For the `free_margin = equity - total_margin_used_usd` calculation, it depends on which margin is used.
    # Let's assume `user_data.get("margin")` IS the definitive 'total margin used' for account summary.
    
    overall_hedged_margin_usd = Decimal(str(user_data.get("margin", 0.0))) # Get the hedged margin from user_data
    free_margin = equity - overall_hedged_margin_usd

    logger.debug(f"User {user_id}: Final Portfolio - Balance: {balance}, Total PnL (USD): {total_pnl_usd}, Equity: {equity}, Overall Hedged Margin (USD): {overall_hedged_margin_usd}, Free Margin: {free_margin}")

    return {
        "balance": float(balance),
        "equity": float(equity),
        "margin": float(overall_hedged_margin_usd), # Use the overall hedged margin
        "free_margin": float(free_margin),
        "profit_loss": float(total_pnl_usd),
        "positions": updated_positions_list
    }