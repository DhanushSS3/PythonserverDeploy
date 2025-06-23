# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from sqlalchemy.ext.asyncio import AsyncSession

# Import for raw market data
# from app.firebase_stream import get_latest_market_data
from app.core.firebase import get_latest_market_data
from app.core.cache import get_adjusted_market_price_cache, get_last_known_price
from redis import Redis
from app.core.logging_config import orders_logger

logger = logging.getLogger(__name__)

async def _convert_to_usd(
    amount: Decimal,
    from_currency: str,
    user_id: int,
    position_id: str,
    value_description: str,
    db: AsyncSession,
    redis_client
) -> Decimal:
    """
    Converts an amount from a given currency to USD using raw market prices.
    """
    try:
        orders_logger.info(f"[CURRENCY_CONVERT] Starting conversion of {amount} {from_currency} to USD for {value_description} (user_id: {user_id}, position: {position_id})")
        
        if from_currency == "USD":
            orders_logger.info(f"[CURRENCY_CONVERT] Currency is already USD, no conversion needed")
            return amount

        # Try direct conversion first (e.g., EURUSD)
        direct_conversion_symbol = f"{from_currency}USD"
        orders_logger.info(f"[CURRENCY_CONVERT] Trying direct conversion with symbol: {direct_conversion_symbol}")
        
        raw_direct_prices = await get_last_known_price(redis_client, direct_conversion_symbol)
        orders_logger.info(f"[CURRENCY_CONVERT] Direct conversion prices from cache: {raw_direct_prices}")
        
        if raw_direct_prices and 'b' in raw_direct_prices:
            rate_str = raw_direct_prices['b']
            orders_logger.info(f"[CURRENCY_CONVERT] Found direct conversion rate (bid): {rate_str}")
            
            try:
                rate = Decimal(str(rate_str))
                if rate <= 0:
                    orders_logger.error(f"[CURRENCY_CONVERT] Invalid direct conversion rate for {direct_conversion_symbol}: {rate}")
                    return amount
                
                converted_amount_usd = amount * rate
                orders_logger.info(f"[CURRENCY_CONVERT] Direct conversion successful: {amount} {from_currency} * {rate} = {converted_amount_usd} USD")
                return converted_amount_usd
            except (InvalidOperation, TypeError) as e:
                orders_logger.error(f"[CURRENCY_CONVERT] Error converting direct rate to Decimal: {e}, rate_str: {rate_str}")
                # Continue to try inverse conversion
        else:
            orders_logger.info(f"[CURRENCY_CONVERT] No direct conversion rate found for {direct_conversion_symbol}, trying inverse")

        # Try inverse conversion (e.g., USDCAD)
        inverse_conversion_symbol = f"USD{from_currency}"
        orders_logger.info(f"[CURRENCY_CONVERT] Trying inverse conversion with symbol: {inverse_conversion_symbol}")
        
        raw_inverse_prices = await get_last_known_price(redis_client, inverse_conversion_symbol)
        orders_logger.info(f"[CURRENCY_CONVERT] Inverse conversion prices from cache: {raw_inverse_prices}")
        
        if raw_inverse_prices and 'b' in raw_inverse_prices:
            rate_str = raw_inverse_prices['b']
            orders_logger.info(f"[CURRENCY_CONVERT] Found inverse conversion rate (bid): {rate_str}")
            
            try:
                rate = Decimal(str(rate_str))
                if rate <= 0:
                    orders_logger.error(f"[CURRENCY_CONVERT] Invalid inverse conversion rate for {inverse_conversion_symbol}: {rate}")
                    return amount
                
                converted_amount_usd = amount / rate
                orders_logger.info(f"[CURRENCY_CONVERT] Inverse conversion successful: {amount} {from_currency} / {rate} = {converted_amount_usd} USD")
                return converted_amount_usd
            except (InvalidOperation, TypeError) as e:
                orders_logger.error(f"[CURRENCY_CONVERT] Error converting inverse rate to Decimal: {e}, rate_str: {rate_str}")
                # Fall through to error case
        else:
            orders_logger.info(f"[CURRENCY_CONVERT] No inverse conversion rate found for {inverse_conversion_symbol}")

        orders_logger.error(f"[CURRENCY_CONVERT] No conversion rate found for {from_currency} to USD for {value_description}")
        orders_logger.error(f"[CURRENCY_CONVERT] Returning original amount without conversion: {amount} {from_currency}")
        return amount

    except Exception as e:
        orders_logger.error(f"[CURRENCY_CONVERT] Error converting {value_description} from {from_currency} to USD: {e}", exc_info=True)
        orders_logger.error(f"[CURRENCY_CONVERT] Returning original amount due to error: {amount} {from_currency}")
        return amount

async def _calculate_adjusted_prices_from_raw(
    symbol: str,
    raw_market_data: Dict[str, Any],
    group_symbol_settings: Dict[str, Any]
) -> Dict[str, Decimal]:
    """
    Calculate adjusted prices from raw market data using group settings.
    Returns a dictionary with 'buy' and 'sell' prices.
    """
    try:
        # Get spread settings from group settings
        spread = Decimal(str(group_symbol_settings.get('spread', '0')))
        spread_pip = Decimal(str(group_symbol_settings.get('spread_pip', '0.00001')))
        
        # Get raw prices
        raw_ask = Decimal(str(raw_market_data.get('ask', '0')))
        raw_bid = Decimal(str(raw_market_data.get('bid', '0')))
        
        # Calculate half spread
        half_spread = (spread * spread_pip) / Decimal('2')
        
        # Calculate adjusted prices
        adjusted_buy = raw_ask + half_spread
        adjusted_sell = raw_bid - half_spread
        
        return {
            'buy': adjusted_buy,
            'sell': adjusted_sell
        }
    except Exception as e:
        logger.error(f"Error calculating adjusted prices for {symbol}: {e}", exc_info=True)
        return {'buy': Decimal('0'), 'sell': Decimal('0')}

async def calculate_user_portfolio(
    user_data: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, Decimal]],
    group_symbol_settings: Dict[str, Any],
    redis_client: Redis,
    margin_call_threshold: Decimal = Decimal('100.0')  # Default threshold for margin call (100%)
) -> Dict[str, Any]:
    """
    Calculates the user's portfolio metrics including equity, margin, and PnL.
    This function focuses on dynamic metrics that change with market prices.
    
    Args:
        user_data: User account information including balance and leverage
        open_positions: List of open positions
        adjusted_market_prices: Dictionary of current market prices by symbol
        group_symbol_settings: Dictionary of symbol settings by group
        redis_client: Redis client for caching
        margin_call_threshold: Threshold for margin call (percentage, default 100%)
        
    Returns:
        Dictionary containing portfolio metrics including:
        - balance: Current wallet balance
        - equity: Balance + unrealized PnL
        - margin: Total margin used
        - free_margin: Equity - margin
        - profit_loss: Total unrealized PnL
        - margin_level: Equity / margin * 100 (percentage)
        - positions: List of positions with PnL
        - margin_call: Boolean indicating if margin_level is below threshold
    """
    try:
        # Initialize portfolio metrics
        balance = Decimal(str(user_data.get('wallet_balance', '0.0')))
        leverage = Decimal(str(user_data.get('leverage', '1.0')))
        overall_hedged_margin_usd = Decimal(str(user_data.get('margin', '0.0')))
        total_pnl_usd = Decimal('0.0')

        # Get raw market data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            logger.error("Failed to get raw market data")
            return {
                "balance": str(balance),
                "equity": str(balance),
                "margin": str(overall_hedged_margin_usd),
                "free_margin": str(balance),
                "profit_loss": "0.0",
                "margin_level": "0.0",
                "positions": open_positions,
                "margin_call": False
            }

        # Process each position to calculate PnL
        positions_with_pnl = []
        for position in open_positions:
            symbol = position.get('order_company_name', '').upper()
            order_type = position.get('order_type', '')
            quantity = Decimal(str(position.get('order_quantity', '0.0')))
            entry_price = Decimal(str(position.get('order_price', '0.0')))
            margin = Decimal(str(position.get('margin', '0.0')))
            contract_value = Decimal(str(position.get('contract_value', '0.0')))

            # Get symbol settings
            symbol_settings = group_symbol_settings.get(symbol, {})
            contract_size = Decimal(str(symbol_settings.get('contract_size', '100000')))
            spread_pip = Decimal(str(symbol_settings.get('spread_pip', '0.00001')))
            profit_currency = symbol_settings.get('profit_currency', 'USD')
            
            # Get commission settings
            commission_type = int(symbol_settings.get('commision_type', 0))
            commission_value_type = int(symbol_settings.get('commision_value_type', 0))
            commission_rate = Decimal(str(symbol_settings.get('commision', '0.0')))

            # Get current prices from adjusted market prices
            current_prices = adjusted_market_prices.get(symbol, {})
            if not current_prices:
                logger.warning(f"No adjusted prices found for symbol {symbol}")
                # Skip this position in PnL calculation but include it in the result
                position_with_pnl = position.copy()
                position_with_pnl['profit_loss'] = "0.0"
                position_with_pnl['current_price'] = "0.0"
                positions_with_pnl.append(position_with_pnl)
                continue

            current_buy = Decimal(str(current_prices.get('buy', '0')))
            current_sell = Decimal(str(current_prices.get('sell', '0')))

            if current_buy <= 0 or current_sell <= 0:
                logger.warning(f"Invalid current prices for {symbol}: buy={current_buy}, sell={current_sell}")
                # Skip this position in PnL calculation but include it in the result
                position_with_pnl = position.copy()
                position_with_pnl['profit_loss'] = "0.0"
                position_with_pnl['current_price'] = "0.0"
                positions_with_pnl.append(position_with_pnl)
                continue

            # Calculate PnL based on order type and contract size
            if order_type == 'BUY':
                price_diff = current_sell - entry_price
                pnl = price_diff * quantity * contract_size
            else:  # SELL
                price_diff = entry_price - current_buy
                pnl = price_diff * quantity * contract_size

            # Use stored commission value if available, otherwise use 0
            commission_usd = Decimal(str(position.get('commission', '0.0')))
            
            # Convert PnL to USD if needed
            pnl_usd = pnl
            if profit_currency != 'USD':
                try:
                    # Try direct conversion first (e.g., EURUSD)
                    direct_pair = f"{profit_currency}USD"
                    direct_data = raw_market_data.get(direct_pair, {})
                    if not direct_data:
                        direct_data = await get_last_known_price(redis_client, direct_pair)
                    direct_rate = Decimal(str(direct_data.get('b', 0))) if direct_data else Decimal(0)
                    if direct_rate > 0:
                        pnl_usd = pnl * direct_rate
                    else:
                        # Try indirect conversion (e.g., USDEUR)
                        indirect_pair = f"USD{profit_currency}"
                        indirect_data = raw_market_data.get(indirect_pair, {})
                        if not indirect_data:
                            indirect_data = await get_last_known_price(redis_client, indirect_pair)
                        indirect_rate = Decimal(str(indirect_data.get('b', 0))) if indirect_data else Decimal(0)
                        if indirect_rate > 0:
                            pnl_usd = pnl / indirect_rate
                        else:
                            logger.error(f"Could not convert PnL from {profit_currency} to USD")
                            # Skip this position in PnL calculation but include it in the result
                            position_with_pnl = position.copy()
                            position_with_pnl['profit_loss'] = "0.0"
                            position_with_pnl['current_price'] = str(current_sell if order_type == 'BUY' else current_buy)
                            positions_with_pnl.append(position_with_pnl)
                            continue
                except Exception as e:
                    logger.error(f"Error converting PnL to USD for {symbol}: {e}", exc_info=True)
                    # Skip this position in PnL calculation but include it in the result
                    position_with_pnl = position.copy()
                    position_with_pnl['profit_loss'] = "0.0"
                    position_with_pnl['current_price'] = str(current_sell if order_type == 'BUY' else current_buy)
                    positions_with_pnl.append(position_with_pnl)
                    continue

            # Calculate final PnL after commission
            final_pnl = pnl_usd - commission_usd

            # Create position with PnL
            position_with_pnl = position.copy()
            position_with_pnl['profit_loss'] = str(final_pnl)  # PnL after commission
            position_with_pnl['current_price'] = str(current_sell if order_type == 'BUY' else current_buy)
            positions_with_pnl.append(position_with_pnl)

            # Accumulate totals
            total_pnl_usd += final_pnl  # Using final PnL (after commission)

            logger.debug(
                f"Position calculation for {symbol}: Type={order_type}, "
                f"Entry={entry_price}, Current={current_sell if order_type == 'BUY' else current_buy}, "
                f"Quantity={quantity}, Contract Size={contract_size}, "
                f"PnL={pnl}, PnL USD={pnl_usd}, Commission={commission_usd}, Final PnL={final_pnl}"
            )

        # Calculate final portfolio metrics
        equity = balance + total_pnl_usd  # total_pnl_usd already includes commission deduction
        free_margin = equity - overall_hedged_margin_usd
        margin_level = (equity / overall_hedged_margin_usd * 100) if overall_hedged_margin_usd > 0 else Decimal('0.0')
        
        # Check for margin call condition
        margin_call = False
        if margin_level > Decimal('0') and margin_level < margin_call_threshold:
            margin_call = True
            logger.warning(f"MARGIN CALL ALERT: User has margin level {margin_level}% which is below threshold {margin_call_threshold}%")

        # Create account summary
        account_summary = {
            "balance": str(balance),
            "equity": str(equity),
            "margin": str(overall_hedged_margin_usd),
            "free_margin": str(free_margin),
            "profit_loss": str(total_pnl_usd),  # This is already net of commission
            "margin_level": str(margin_level),
            "positions": positions_with_pnl,  # Include positions with PnL
            "margin_call": margin_call  # Flag indicating if margin call condition is met
        }

        return account_summary

    except Exception as e:
        logger.error(f"Error calculating portfolio: {e}", exc_info=True)
        return {
            "balance": str(balance),
            "equity": str(balance),
            "margin": str(overall_hedged_margin_usd),
            "free_margin": str(balance),
            "profit_loss": "0.0",
            "margin_level": "0.0",
            "positions": open_positions,  # Include positions even in error case
            "margin_call": False
        }