# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# Removed STANDARD_LOT_SIZE as it's now fetched from ExternalSymbolInfo via cache

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
                logger.info(f"User {user_id}, Pos {position_id}: CONVERTED (direct) {value_description} using {direct_conversion_symbol} buy price: {amount} {from_currency_upper} * {live_buy_price_direct} = {converted_amount_usd} USD")
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
                    logger.info(f"User {user_id}, Pos {position_id}: CONVERTED (indirect) {value_description} using {indirect_conversion_symbol} buy price: {amount} {from_currency_upper} / {live_buy_price_indirect} = {converted_amount_usd} USD")
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
    Commission is calculated and is assumed to be in USD.
    It is deducted from the PnL *after* the PnL has been converted to USD.
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
        commission_for_position_usd = Decimal("0.0") # For reporting the commission in USD

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
            contract_size = Decimal(str(symbol_settings.get('contract_size', Decimal("0.0"))))
            if contract_size <= Decimal("0.0"):
                logger.warning(f"User {user_id}, Pos {position_id}: Invalid contract_size '{contract_size}' for symbol {symbol_upper}. Setting to 1.0 for calculation safety.")
                contract_size = Decimal("1.0")

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

            # --- 2. Convert PnL to USD (before commission deduction) ---
            pnl_for_position_usd = await _convert_to_usd(pnl_native, profit_currency, adjusted_market_prices, user_id, position_id, "Raw PnL")
            logger.debug(f"User {user_id}, Pos {position_id}: PnL (USD, before commission): {pnl_for_position_usd}")


            # --- 3. Calculate Commission (assumed to be in USD as per requirement) ---
            commission_usd = Decimal("0.0") # Renamed from commission_native to reflect it's USD
            commission_type_setting = symbol_settings.get('commision_type')
            commission_value_type_setting = symbol_settings.get('commision_value_type')
            commission_rate_setting = Decimal(str(symbol_settings.get('commision', 0.0)))

            if commission_type_setting in [0, 1]: # 0: Every Trade, 1: In
                if commission_value_type_setting == 0: # Total Value (per lot)
                    commission_usd = quantity * commission_rate_setting
                elif commission_value_type_setting == 1: # Percent
                    if entry_price > Decimal("0.0"):
                        # Assuming this calculation directly results in USD
                        commission_usd = ((commission_rate_setting * entry_price) / Decimal("100")) * quantity
                    else:
                        logger.warning(f"User {user_id}, Pos {position_id}: Entry price is zero, cannot calculate percentage commission.")
                else:
                    logger.warning(f"User {user_id}, Pos {position_id}: Unknown commission_value_type '{commission_value_type_setting}'.")
            elif commission_type_setting == 2: # Out
                logger.debug(f"User {user_id}, Pos {position_id}: Commission Type 'Out', applied on close.")
            else:
                logger.warning(f"User {user_id}, Pos {position_id}: Unknown commission_type '{commission_type_setting}'.")

            logger.debug(f"User {user_id}, Pos {position_id}: Commission (USD): {commission_usd}")

            # --- 4. Deduct Commission from PnL (after PnL is in USD) ---
            pnl_for_position_usd -= commission_usd
            logger.debug(f"User {user_id}, Pos {position_id}: Net PnL (USD) after commission deduction: {pnl_for_position_usd}")

            # The commission for reporting is simply the calculated commission_usd
            commission_for_position_usd = commission_usd


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
                    except InvalidOperation: pass

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