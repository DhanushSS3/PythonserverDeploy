# app/services/margin_calculator.py

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import logging
from typing import Optional, Tuple, Dict, Any, List
import json

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis.asyncio import Redis
from app.core.cache import get_adjusted_market_price_cache

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache,
    DecimalEncoder,
    get_live_adjusted_buy_price_for_pair,
    get_live_adjusted_sell_price_for_pair,
    get_adjusted_market_price_cache
)
from app.firebase_stream import get_latest_market_data
from app.crud.crud_symbol import get_symbol_type
from app.services.portfolio_calculator import _convert_to_usd, _calculate_adjusted_prices_from_raw
from app.core.logging_config import orders_logger

logger = logging.getLogger(__name__)

# --- HELPER FUNCTION: Calculate Base Margin Per Lot (Used in Hedging) ---
# This helper calculates a per-lot value based on Group margin setting, price, and leverage.
# This is used in the hedging logic in order_processing.py for comparison.
async def calculate_base_margin_per_lot(
    redis_client: Redis,
    user_id: int,
    symbol: str,
    price: Decimal
) -> Optional[Decimal]:
    """
    Calculates a base margin value per standard lot for a given symbol and price,
    considering user's group settings and leverage.
    This value is used for comparison in hedging calculations.
    Returns the base margin value per lot or None if calculation fails.
    """
    # Retrieve user data from cache to get group_name and leverage
    user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
    if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
        orders_logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
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
        orders_logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
        return None

    # Ensure margin_setting is Decimal
    margin_setting_raw = group_symbol_settings.get('margin', 0)
    margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


    if user_leverage <= 0:
         orders_logger.error(f"User leverage is zero or negative for user {user_id}.")
         return None

    # Calculation based on Group Base Margin Setting, Price, and Leverage
    # This formula seems to be the one needed for the per-lot comparison in hedging.
    try:
        # Ensure price is Decimal
        price_decimal = Decimal(str(price))
        base_margin_per_lot = (margin_setting * price_decimal) / user_leverage
        orders_logger.debug(f"Calculated base margin per lot (for hedging) for user {user_id}, symbol {symbol}, price {price}: {base_margin_per_lot}")
        return base_margin_per_lot
    except Exception as e:
        orders_logger.error(f"Error calculating base margin per lot (for hedging) for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None


from app.core.cache import get_last_known_price

def calculate_single_order_margin(
    order_type: str,
    order_quantity: Decimal,
    order_price: Decimal,
    symbol_settings: Dict[str, Any]
) -> Decimal:
    """
    Calculate margin for a single order based on order details and symbol settings.
    This simplified version is used for pending order processing.
    Returns the calculated margin.
    """
    try:
        # Get required settings from symbol_settings
        contract_size = Decimal(str(symbol_settings.get('contract_size', 100000)))
        leverage = Decimal(str(symbol_settings.get('leverage', 1)))
        
        # Calculate contract value
        contract_value = order_quantity * contract_size * order_price
        
        # Calculate margin based on contract value and leverage
        margin = (contract_value / leverage).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        
        return margin
    except Exception as e:
        orders_logger.error(f"Error calculating margin for order: {e}", exc_info=True)
        return Decimal('0')

async def calculate_total_symbol_margin_contribution(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_positions_for_symbol: list,
    user_type: str,
    order_model=None
) -> Dict[str, Any]:
    """
    Calculate total margin contribution for a symbol considering hedged positions.
    Returns a dictionary with total_margin and other details.
    """
    try:
        total_buy_quantity = Decimal('0.0')
        total_sell_quantity = Decimal('0.0')
        all_margins_per_lot: List[Decimal] = []

        # Get user data for leverage
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.error(f"User data not found for user {user_id}")
            return {"total_margin": Decimal('0.0')}

        user_leverage = Decimal(str(user_data.get('leverage', '1.0')))
        if user_leverage <= 0:
            logger.error(f"Invalid leverage for user {user_id}: {user_leverage}")
            return {"total_margin": Decimal('0.0')}

        # Get group settings for margin calculation
        group_name = user_data.get('group_name')
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            logger.error(f"Group settings not found for symbol {symbol}")
            return {"total_margin": Decimal('0.0')}

        # Get external symbol info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            logger.error(f"External symbol info not found for {symbol}")
            return {"total_margin": Decimal('0.0')}

        # Get raw market data for price calculations
        raw_market_data = await get_latest_market_data()
        if not raw_market_data:
            logger.error("Failed to get market data")
            return {"total_margin": Decimal('0.0')}

        # Process each position
        for position in open_positions_for_symbol:
            position_quantity = Decimal(str(position.order_quantity))
            position_type = position.order_type.upper()
            position_margin = Decimal(str(position.margin))

            if position_quantity > 0:
                # Calculate margin per lot for this position
                margin_per_lot = position_margin / position_quantity
                all_margins_per_lot.append(margin_per_lot)

                # Add to total quantities
                if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                    total_buy_quantity += position_quantity
                elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
                    total_sell_quantity += position_quantity

        # Calculate net quantity (for hedged positions)
        net_quantity = max(total_buy_quantity, total_sell_quantity)
        
        # Get the highest margin per lot (for hedged positions)
        highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal('0.0')

        # Calculate total margin contribution
        total_margin = (highest_margin_per_lot * net_quantity).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)

        # Return the result
        return {"total_margin": total_margin, "net_quantity": net_quantity}

    except Exception as e:
        logger.error(f"Error calculating total symbol margin contribution: {e}", exc_info=True)
        return {"total_margin": Decimal('0.0')}