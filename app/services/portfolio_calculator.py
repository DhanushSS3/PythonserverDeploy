# app/services/portfolio_calculator.py

import logging
from typing import Dict, Any, List
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from sqlalchemy.ext.asyncio import AsyncSession

# Import for raw market data
from app.firebase_stream import get_latest_market_data
from app.core.cache import get_adjusted_market_price_cache
from redis import Redis
from app.core.logging_config import orders_logger

logger = logging.getLogger(__name__)

async def _convert_to_usd(
    amount: Decimal,
    from_currency: str,
    user_id: int,
    position_id: str,
    value_description: str,
    db: AsyncSession
) -> Decimal:
    """
    Converts an amount from a given currency to USD using raw market prices.
    """
    try:
        if from_currency == "USD":
            return amount

        # Get raw market data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            orders_logger.error(f"Failed to get raw market data for {value_description} conversion")
            return amount

        # Try direct conversion first (e.g., EURUSD)
        direct_conversion_symbol = f"{from_currency}USD"
        raw_direct_prices = raw_market_data.get(direct_conversion_symbol, {})
        if raw_direct_prices and 'b' in raw_direct_prices:
            rate_str = raw_direct_prices['b']
            rate = Decimal(str(rate_str))
            if rate <= 0:
                orders_logger.error(f"Invalid direct conversion rate for {direct_conversion_symbol}: {rate}")
                return amount
            converted_amount_usd = amount * rate
            orders_logger.debug(f"Direct conversion for {value_description}: {amount} {from_currency} * {rate} = {converted_amount_usd} USD")
            return converted_amount_usd

        # Try indirect conversion through USD (e.g., EUR -> USD -> JPY)
        indirect_conversion_symbol = f"USD{from_currency}"
        raw_indirect_prices = raw_market_data.get(indirect_conversion_symbol, {})
        if raw_indirect_prices and 'a' in raw_indirect_prices:
            rate_str = raw_indirect_prices['a']
            rate = Decimal(str(rate_str))
            if rate <= 0:
                orders_logger.error(f"Invalid indirect conversion rate for {indirect_conversion_symbol}: {rate}")
                return amount
            converted_amount_usd = amount / rate
            orders_logger.debug(f"Indirect conversion for {value_description}: {amount} {from_currency} / {rate} = {converted_amount_usd} USD")
            return converted_amount_usd

        orders_logger.error(f"No conversion rate found for {from_currency} to USD for {value_description}")
        return amount

    except Exception as e:
        orders_logger.error(f"Error converting {value_description} from {from_currency} to USD: {e}", exc_info=True)
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
    redis_client: Redis
) -> Dict[str, Any]:
    """
    Calculates the user's portfolio metrics including equity, margin, and PnL.
    """
    try:
        # Initialize portfolio metrics
        balance = Decimal(str(user_data.get('wallet_balance', '0.0')))
        leverage = Decimal(str(user_data.get('leverage', '1.0')))
        overall_hedged_margin_usd = Decimal('0.0')
        total_pnl_usd = Decimal('0.0')

        # Get raw market data
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            logger.error("Failed to get raw market data")
            return {
                "balance": str(balance),
                "equity": str(balance),
                "margin": "0.0",
                "free_margin": str(balance),
                "profit_loss": "0.0",
                "margin_level": "0.0",
                "positions": open_positions
            }

        # Process each position
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
                continue

            current_buy = current_prices.get('buy', Decimal('0'))
            current_sell = current_prices.get('sell', Decimal('0'))

            if current_buy <= 0 or current_sell <= 0:
                logger.warning(f"Invalid current prices for {symbol}: buy={current_buy}, sell={current_sell}")
                continue

            # Calculate PnL based on order type and contract size
            if order_type == 'BUY':
                price_diff = current_sell - entry_price
                pnl = price_diff * quantity * contract_size
            else:  # SELL
                price_diff = entry_price - current_buy
                pnl = price_diff * quantity * contract_size

            # Calculate commission based on settings
            commission_usd = Decimal('0.0')
            if commission_type in [0, 1]:  # If commission is enabled
                if commission_value_type == 0:  # Fixed commission per lot
                    commission_usd = commission_rate * quantity
                elif commission_value_type == 1:  # Percentage of price
                    commission_usd = (commission_rate * entry_price * quantity * contract_size) / Decimal('100')

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
                            continue
                except Exception as e:
                    logger.error(f"Error converting PnL to USD for {symbol}: {e}", exc_info=True)
                    continue

            # Calculate final PnL after commission
            final_pnl = pnl_usd - commission_usd

            # Update position with current PnL, price, and commission
            position['profit_loss'] = str(final_pnl)  # PnL after commission
            position['current_price'] = str(current_sell if order_type == 'BUY' else current_buy)
            position['commission'] = str(commission_usd)  # Add commission field
            position['commission_applied'] = str(commission_usd)  # For backward compatibility
            position['applied_commission'] = str(commission_usd)  # For backward compatibility

            # Accumulate totals
            total_pnl_usd += final_pnl  # Using final PnL (after commission)
            overall_hedged_margin_usd += margin

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

        # Create account summary
        account_summary = {
            "balance": str(balance),
            "equity": str(equity),
            "margin": str(overall_hedged_margin_usd),
            "free_margin": str(free_margin),
            "profit_loss": str(total_pnl_usd),  # This is already net of commission
            "margin_level": str(margin_level),
            "positions": open_positions  # Include the updated positions
        }

        return account_summary

    except Exception as e:
        logger.error(f"Error calculating portfolio: {e}", exc_info=True)
        return {
            "balance": str(balance),
            "equity": str(balance),
            "margin": "0.0",
            "free_margin": str(balance),
            "profit_loss": "0.0",
            "margin_level": "0.0",
            "positions": open_positions  # Include positions even in error case
        }