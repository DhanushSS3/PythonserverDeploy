# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# Import for raw market data
from app.firebase_stream import get_latest_market_data

logger = logging.getLogger(__name__)

async def _convert_to_usd(
    amount: Decimal,
    from_currency: str,
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
        return amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    converted_amount_usd = amount
    conversion_successful = False

    direct_conversion_symbol = f"{from_currency_upper}USD"
    raw_direct_prices = get_latest_market_data(direct_conversion_symbol)
    logger.debug(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Attempting direct with {direct_conversion_symbol}. Data: {raw_direct_prices}")

    if raw_direct_prices and 'b' in raw_direct_prices:
        try:
            rate_str = raw_direct_prices['b']
            if rate_str is None:
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
        indirect_conversion_symbol = f"USD{from_currency_upper}"
        raw_indirect_prices = get_latest_market_data(indirect_conversion_symbol)
        logger.debug(f"User {user_id}, Pos {position_id}: RAW _convert_to_usd - Attempting indirect with {indirect_conversion_symbol}. Data: {raw_indirect_prices}")

        if raw_indirect_prices and 'o' in raw_indirect_prices:
            try:
                rate_str = raw_indirect_prices['o']
                if rate_str is None:
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
        return amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    return converted_amount_usd.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


async def calculate_user_portfolio(
    user_data: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, float]],
    group_symbol_settings: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    user_id = user_data.get('id', 'UNKNOWN_USER')
    
    # Initialize these variables before the try-except block
    balance = Decimal("0.0")
    leverage = Decimal("1.0")
    overall_hedged_margin_usd = Decimal("0.0") # Initialize here

    try:
        balance = Decimal(str(user_data.get("wallet_balance", 0.0)))
        leverage = Decimal(str(user_data.get("leverage", 1.0)))
        if leverage <= Decimal("0.0"):
            logger.warning(f"User {user_id}: Leverage is {leverage}, defaulting to 1.0.")
            leverage = Decimal("1.0")
        
        # Move this assignment inside the try block, after balance/leverage
        # Or keep it here if you want it always assigned regardless of other user_data parsing errors
        overall_hedged_margin_usd = Decimal(str(user_data.get("margin", 0.0))) 
        
    except InvalidOperation as e:
        logger.error(f"User {user_id}: Error converting user data to Decimal: {e}. User data: {user_data}", exc_info=True)
        # balance and leverage already defaulted to "0.0" and "1.0"
        # overall_hedged_margin_usd already defaulted to "0.0"
    
    # Rest of your function remains the same from here
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

            if not symbol or not order_type or quantity <= Decimal("0.0") or entry_price <= Decimal("0.0"):
                pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
                pos_to_append.setdefault('profit_loss', 0.0)
                pos_to_append.setdefault('commission_applied', 0.0)
                updated_positions_list.append(pos_to_append)
                continue

            symbol_upper = symbol.upper()
            market_price_info_for_pnl = adjusted_market_prices.get(symbol_upper)
            symbol_settings = group_symbol_settings.get(symbol_upper) if isinstance(group_symbol_settings, dict) else None

            if not market_price_info_for_pnl or not symbol_settings:
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

            pnl_native = Decimal("0.0")
            if order_type.lower() == 'buy':
                pnl_native = (current_sell_price_for_pnl - entry_price) * quantity * contract_size
            elif order_type.lower() == 'sell':
                pnl_native = (entry_price - current_buy_price_for_pnl) * quantity * contract_size
            
            pnl_for_position_usd = await _convert_to_usd(
                pnl_native, profit_currency, user_id, position_id, "Raw PnL"
            )
            
            commission_usd = Decimal("0.0")
            commission_type_setting = symbol_settings.get('commision_type')
            commission_value_type_setting = symbol_settings.get('commision_value_type')
            commission_rate_setting = Decimal(str(symbol_settings.get('commision', 0.0)))

            if commission_type_setting in [0, 1]:
                if commission_value_type_setting == 0:
                    commission_usd = quantity * commission_rate_setting
                elif commission_value_type_setting == 1:
                    commission_usd = ((commission_rate_setting * entry_price) / Decimal("100")) * quantity 

            pnl_for_position_usd -= commission_usd
            commission_for_position_usd = commission_usd

            total_pnl_usd += pnl_for_position_usd

            margin_for_this_position_usd = Decimal(str(position.get('margin', 0.0)))
            total_margin_used_usd += margin_for_this_position_usd
            logger.debug(f"User {user_id}, Pos {position_id}: Using pre-calculated margin (USD): {margin_for_this_position_usd}")


            current_pos_data = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            current_pos_data['profit_loss'] = float(pnl_for_position_usd)
            current_pos_data['commission_applied'] = float(commission_for_position_usd)
            updated_positions_list.append(current_pos_data)

        except InvalidOperation as e:
            logger.error(f"User {user_id}, Pos {position_id}: Invalid Decimal operation in portfolio calc: {e}. Position: {position}", exc_info=True)
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)
        except Exception as e:
            logger.error(f"User {user_id}, Pos {position_id}: Generic error processing position in portfolio: {e}. Data: {position}", exc_info=True)
            pos_to_append = {k: str(v) if isinstance(v, Decimal) else v for k, v in position.items()}
            pos_to_append.setdefault('profit_loss', 0.0)
            pos_to_append.setdefault('commission_applied', 0.0)
            updated_positions_list.append(pos_to_append)


    equity = balance + total_pnl_usd
    free_margin = equity - overall_hedged_margin_usd

    margin_level = Decimal("0.0")
    if overall_hedged_margin_usd > Decimal("0.0"):
        margin_level = (equity / overall_hedged_margin_usd) * Decimal("100.0")
    else:
        logger.warning(f"User {user_id}: Overall hedged margin is zero or negative ({overall_hedged_margin_usd}), margin_level cannot be calculated. Setting to 0.0 or a suitable default.")
        margin_level = Decimal("0.0")

    logger.debug(f"User {user_id}: Final Portfolio - Balance: {balance}, Total PnL (USD): {total_pnl_usd}, Equity: {equity}, Overall Hedged Margin (USD): {overall_hedged_margin_usd}, Free Margin: {free_margin}, Margin Level: {margin_level}")

    return {
        "balance": float(balance),
        "equity": float(equity),
        "margin": float(overall_hedged_margin_usd),
        "free_margin": float(free_margin),
        "profit_loss": float(total_pnl_usd),
        "margin_level": float(margin_level),
        "positions": updated_positions_list
    }