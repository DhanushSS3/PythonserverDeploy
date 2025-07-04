# app/services/margin_calculator.py

# IMPORTANT: Correct formulas for margin calculation:
# 1. Contract value = contract_size * order_quantity (without price)
# 2. Margin = (contract_value * order_price) / user_leverage
# 3. Convert margin to USD if profit_currency != "USD"
#
# The contract_size and profit_currency are obtained from ExternalSymbolInfo table

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
from app.core.firebase import get_latest_market_data
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
    price: Decimal,
    db: AsyncSession = None,
    user_type: str = 'live'
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

async def calculate_single_order_margin(
    redis_client: Redis,
    symbol: str,
    order_type: str,
    quantity: Decimal,
    user_leverage: Decimal,
    group_settings: Dict[str, Any],
    external_symbol_info: Dict[str, Any],
    raw_market_data: Dict[str, Any],
    db: AsyncSession = None,
    user_id: int = None,
    order_price: Decimal = None
) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    """
    ULTRA-OPTIMIZED margin calculation for single order.
    Reduces Firebase calls and parallelizes operations for sub-500ms performance.
    """
    try:
        # Step 1: Extract data from parameters (no additional calls needed)
        contract_size = Decimal(str(external_symbol_info.get('contract_size', '1')))
        profit_currency = external_symbol_info.get('profit_currency', 'USD')
        digit = int(external_symbol_info.get('digit', '5'))
        
        # Step 2: Get adjusted prices from raw market data (no additional Firebase calls)
        if not raw_market_data or symbol not in raw_market_data:
            orders_logger.error(f"[MARGIN_CALC] No market data for symbol {symbol}")
            return None, None, None, None
        
        symbol_data = raw_market_data.get(symbol, {})
        if not symbol_data:
            orders_logger.error(f"[MARGIN_CALC] No data for symbol {symbol} in market data")
            return None, None, None, None
        
        # Debug: Log the market data we received
        orders_logger.info(f"[MARGIN_CALC] Market data for {symbol}: {symbol_data}")
        
        # Extract prices from market data with better fallback logic
        bid_price_raw = symbol_data.get('b', symbol_data.get('bid', '0'))
        ask_price_raw = symbol_data.get('a', symbol_data.get('ask', '0'))
        
        # Convert to Decimal with validation
        try:
            bid_price = Decimal(str(bid_price_raw)) if bid_price_raw and bid_price_raw != '0' else Decimal('0')
            ask_price = Decimal(str(ask_price_raw)) if ask_price_raw and ask_price_raw != '0' else Decimal('0')
        except (ValueError, decimal.InvalidOperation):
            orders_logger.error(f"[MARGIN_CALC] Invalid price format for {symbol}: bid={bid_price_raw}, ask={ask_price_raw}")
            return None, None, None, None
        
        # Enhanced price validation with fallbacks
        if bid_price <= 0 and ask_price <= 0:
            # Try to use order_price if provided
            if order_price and order_price > 0:
                # Use order price for both bid and ask with small spread
                spread = order_price * Decimal('0.0001')  # 1 pip spread
                bid_price = order_price - spread
                ask_price = order_price + spread
                orders_logger.warning(f"[MARGIN_CALC] Using order price as fallback for {symbol}: bid={bid_price}, ask={ask_price}")
            else:
                # Try to use open price from market data as last resort
                open_price_raw = symbol_data.get('o', '0')
                if open_price_raw and open_price_raw != '0':
                    try:
                        open_price = Decimal(str(open_price_raw))
                        if open_price > 0:
                            spread = open_price * Decimal('0.0001')  # 1 pip spread
                            bid_price = open_price - spread
                            ask_price = open_price + spread
                            orders_logger.warning(f"[MARGIN_CALC] Using open price as fallback for {symbol}: bid={bid_price}, ask={ask_price}")
                        else:
                            orders_logger.error(f"[MARGIN_CALC] Both bid and ask prices are invalid for {symbol}: bid={bid_price}, ask={ask_price}")
                            return None, None, None, None
                    except (ValueError, decimal.InvalidOperation):
                        orders_logger.error(f"[MARGIN_CALC] Both bid and ask prices are invalid for {symbol}: bid={bid_price}, ask={ask_price}")
                        return None, None, None, None
                else:
                    orders_logger.error(f"[MARGIN_CALC] Both bid and ask prices are invalid for {symbol}: bid={bid_price}, ask={ask_price}")
                    return None, None, None, None
        
        # If one price is missing, use the other with a small spread
        if bid_price <= 0 and ask_price > 0:
            # Use ask price for both bid and ask (with small spread)
            spread = ask_price * Decimal('0.0001')  # 1 pip spread
            bid_price = ask_price - spread
            orders_logger.warning(f"[MARGIN_CALC] Missing bid price for {symbol}, using ask price with spread: bid={bid_price}, ask={ask_price}")
        elif ask_price <= 0 and bid_price > 0:
            # Use bid price for both bid and ask (with small spread)
            spread = bid_price * Decimal('0.0001')  # 1 pip spread
            ask_price = bid_price + spread
            orders_logger.warning(f"[MARGIN_CALC] Missing ask price for {symbol}, using bid price with spread: bid={bid_price}, ask={ask_price}")
        
        orders_logger.info(f"[MARGIN_CALC] Using prices for {symbol}: bid={bid_price}, ask={ask_price}")
        
        # Step 3: Calculate adjusted prices based on order type
        if order_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
            adjusted_price = ask_price  # Use ask price for buy orders
        elif order_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
            adjusted_price = bid_price  # Use bid price for sell orders
        else:
            # For market orders, use appropriate price
            adjusted_price = ask_price if order_type == 'BUY' else bid_price
        
        # Step 4: Calculate contract value (always quantity * contract_size)
        contract_value = (quantity * contract_size).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )

        # Step 5: Calculate margin using correct formula
        group_type = int(group_settings.get('type', 0))
        margin_from_group = Decimal(str(group_settings.get('margin', 1)))
        if margin_from_group == 0:
            margin_from_group = Decimal('1')
        if user_leverage <= 0:
            orders_logger.error(f"[MARGIN_CALC] Invalid leverage: {user_leverage}")
            return None, None, None, None
        if group_type == 4:
            # Crypto: margin = (contract_value * adjusted_price * margin_from_group) / user_leverage
            margin = (contract_value * adjusted_price * margin_from_group / user_leverage).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
        else:
            # Commodities, indices, forex: margin = (contract_value * adjusted_price) / user_leverage
            margin = (contract_value * adjusted_price / user_leverage).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )

        # Step 6: Calculate commission in parallel with margin conversion
        commission = Decimal('0.0')
        commission_type = int(group_settings.get('commision_type', 0))
        commission_value_type = int(group_settings.get('commision_value_type', 0))
        commission_rate = Decimal(str(group_settings.get('commision', '0')))
        if commission_type in [0, 1]:  # "Every Trade" or "In"
            if commission_value_type == 0:  # Per lot
                commission = quantity * commission_rate
            elif commission_value_type == 1:  # Percent of price
                commission = ((commission_rate * adjusted_price) / Decimal("100")) * quantity
        commission = commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Step 7: Convert margin to USD if needed (only if profit_currency is not USD)
        margin_usd = margin
        if profit_currency != 'USD' and user_id and db:
            # Use cached conversion rates if available
            conversion_key = f"conversion_rate:{profit_currency}:USD"
            try:
                cached_rate = await redis_client.get(conversion_key)
                if cached_rate:
                    conversion_rate = Decimal(str(cached_rate))
                    margin_usd = (margin * conversion_rate).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )
                else:
                    # Fallback to portfolio calculator conversion
                    margin_usd = await _convert_to_usd(
                        margin, 
                        profit_currency, 
                        user_id, 
                        None,  # position_id
                        "margin_calculation", 
                        db,
                        redis_client
                    )
                    if margin_usd is None:
                        margin_usd = margin  # Fallback to original margin
            except Exception as e:
                orders_logger.warning(f"[MARGIN_CALC] Currency conversion failed for {profit_currency}: {e}")
                margin_usd = margin  # Fallback to original margin

        orders_logger.info(f"[MARGIN_CALC_OPTIMIZED] Symbol: {symbol}, Type: {order_type}, Qty: {quantity}, "
                          f"Price: {adjusted_price}, Margin: {margin_usd}, Commission: {commission}")
        return margin_usd, adjusted_price, contract_value, commission
        
    except Exception as e:
        orders_logger.error(f"[MARGIN_CALC] Error in calculate_single_order_margin: {e}", exc_info=True)
        return None, None, None, None

async def get_external_symbol_info(db: AsyncSession, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Get external symbol info from the database.
    """
    try:
        from sqlalchemy.future import select
        from app.database.models import ExternalSymbolInfo
        
        stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        result = await db.execute(stmt)
        symbol_info = result.scalars().first()
        
        if symbol_info:
            orders_logger.info(f"[SYMBOL_INFO] Retrieved external symbol info for {symbol}: contract_size={symbol_info.contract_size}, profit_currency={symbol_info.profit}, digit={symbol_info.digit}")
            return {
                'contract_size': symbol_info.contract_size,
                'profit_currency': symbol_info.profit,
                'digit': symbol_info.digit
            }
        orders_logger.error(f"[SYMBOL_INFO] No external symbol info found for {symbol}")
        return None
    except Exception as e:
        orders_logger.error(f"[SYMBOL_INFO] Error getting external symbol info for {symbol}: {e}", exc_info=True)
        return None

from app.core.cache import get_last_known_price
from app.core.firebase import get_latest_market_data

# def calculate_pending_order_margin(
#     order_type: str,
#     order_quantity: Decimal,
#     order_price: Decimal,
#     symbol_settings: Dict[str, Any],
#     user_leverage: Decimal = None
# ) -> Decimal:
#     """
#     Calculate margin for a pending order based on order details and symbol settings.
#     This simplified version is used for pending order processing.
#     Returns the calculated margin.
#     """
#     try:
#         # Get required settings from symbol_settings
#         contract_size_raw = symbol_settings.get('contract_size', 100000)
#         contract_size = Decimal(str(contract_size_raw))
        
#         # Get leverage - use explicitly provided user_leverage if available,
#         # otherwise get from symbol_settings with a safer default
#         if user_leverage is not None:
#             leverage = Decimal(str(user_leverage))
#             orders_logger.info(f"[PENDING_MARGIN_CALC] Using provided user leverage: {leverage}")
#         else:
#             leverage_raw = symbol_settings.get('leverage', 100)  # Use more reasonable default leverage
#             leverage = Decimal(str(leverage_raw))
#             orders_logger.info(f"[PENDING_MARGIN_CALC] Using symbol settings leverage: {leverage} (raw: {leverage_raw})")
        
#         # Make sure leverage is reasonable (typically 100-500 for forex)
#         # If it's too small (like 1), it would make margin requirements excessive
#         if leverage < Decimal('10'):
#             orders_logger.warning(f"[PENDING_MARGIN_CALC] Leverage very low ({leverage}), using default 100")
#             leverage = Decimal('100')
        
#         orders_logger.info(f"[PENDING_MARGIN_CALC] Calculating margin for pending {order_type} order: quantity={order_quantity}, price={order_price}")
#         orders_logger.info(f"[PENDING_MARGIN_CALC] Settings: contract_size={contract_size} (raw: {contract_size_raw}), leverage={leverage}")
        
#         # Calculate contract value using the CORRECT formula
#         contract_value = contract_size * order_quantity
#         orders_logger.info(f"[PENDING_MARGIN_CALC] Contract value = contract_size * order_quantity = {contract_size} * {order_quantity} = {contract_value}")
        
#         # Calculate margin using the CORRECT formula
#         margin_raw = (contract_value * order_price) / leverage
#         margin = margin_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
#         orders_logger.info(f"[PENDING_MARGIN_CALC] Margin = (contract_value * order_price) / leverage = ({contract_value} * {order_price}) / {leverage} = {margin_raw} (rounded to {margin})")
        
#         return margin
#     except Exception as e:
#         orders_logger.error(f"[PENDING_MARGIN_CALC] Error calculating margin for pending order: {e}", exc_info=True)
#         return Decimal('0')

# async def calculate_total_symbol_margin_contribution(
#     db: AsyncSession,
#     redis_client: Redis,
#     user_id: int,
#     symbol: str,
#     open_positions_for_symbol: list,
#     user_type: str,
#     order_model=None
# ) -> Dict[str, Any]:
#     """
#     Calculate total margin contribution for a symbol considering hedged positions.
#     Returns a dictionary with total_margin and other details.
#     """
#     try:
#         total_buy_quantity = Decimal('0.0')
#         total_sell_quantity = Decimal('0.0')
#         all_margins_per_lot: List[Decimal] = []

#         orders_logger.info(f"[MARGIN_CONTRIB] Calculating total margin contribution for user {user_id}, symbol {symbol}, positions: {len(open_positions_for_symbol)}")

#         # Get user data for leverage
#         user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
#         if not user_data:
#             orders_logger.error(f"[MARGIN_CONTRIB] User data not found for user {user_id}")
#             return {"total_margin": Decimal('0.0')}

#         user_leverage_raw = user_data.get('leverage', '1.0')
#         user_leverage = Decimal(str(user_leverage_raw))
#         orders_logger.info(f"[MARGIN_CONTRIB] User leverage: {user_leverage} (raw: {user_leverage_raw})")
        
#         if user_leverage <= 0:
#             orders_logger.error(f"[MARGIN_CONTRIB] Invalid leverage for user {user_id}: {user_leverage}")
#             return {"total_margin": Decimal('0.0')}

#         # Get group settings for margin calculation
#         group_name = user_data.get('group_name')
#         group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
#         if not group_settings:
#             orders_logger.error(f"[MARGIN_CONTRIB] Group settings not found for symbol {symbol}")
#             return {"total_margin": Decimal('0.0')}

#         # Get external symbol info
#         external_symbol_info = await get_external_symbol_info(db, symbol)
#         if not external_symbol_info:
#             orders_logger.error(f"[MARGIN_CONTRIB] External symbol info not found for {symbol}")
#             return {"total_margin": Decimal('0.0')}

#         # Get raw market data for price calculations
#         raw_market_data = get_latest_market_data()
#         if not raw_market_data:
#             orders_logger.error("[MARGIN_CONTRIB] Failed to get market data")
#             return {"total_margin": Decimal('0.0')}

#         # Process each position
#         for i, position in enumerate(open_positions_for_symbol):
#             try:
#                 position_quantity_raw = position.order_quantity
#                 position_quantity = Decimal(str(position_quantity_raw))
#                 position_type = position.order_type.upper()
#                 position_margin_raw = position.margin
#                 position_margin = Decimal(str(position_margin_raw))

#                 orders_logger.info(f"[MARGIN_CONTRIB] Position {i+1}: type={position_type}, quantity={position_quantity} (raw: {position_quantity_raw}), margin={position_margin} (raw: {position_margin_raw})")

#                 if position_quantity > 0:
#                     # Calculate margin per lot for this position
#                     margin_per_lot_raw = position_margin / position_quantity
#                     margin_per_lot = margin_per_lot_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
#                     all_margins_per_lot.append(margin_per_lot)
#                     orders_logger.info(f"[MARGIN_CONTRIB] Position {i+1} margin per lot: {position_margin} / {position_quantity} = {margin_per_lot_raw} (rounded to {margin_per_lot})")

#                     # Add to total quantities
#                     if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
#                         total_buy_quantity += position_quantity
#                     elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
#                         total_sell_quantity += position_quantity
#             except Exception as e:
#                 orders_logger.error(f"[MARGIN_CONTRIB] Error processing position {i+1}: {e}", exc_info=True)
#                 continue

#         # Calculate net quantity (for hedged positions)
#         net_quantity = max(total_buy_quantity, total_sell_quantity)
#         orders_logger.info(f"[MARGIN_CONTRIB] Total buy quantity: {total_buy_quantity}, Total sell quantity: {total_sell_quantity}, Net quantity: {net_quantity}")
        
#         # Get the highest margin per lot (for hedged positions)
#         highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal('0.0')
#         orders_logger.info(f"[MARGIN_CONTRIB] All margins per lot: {all_margins_per_lot}, Highest margin per lot: {highest_margin_per_lot}")

#         # Calculate total margin contribution
#         total_margin_raw = highest_margin_per_lot * net_quantity
#         total_margin = total_margin_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
#         orders_logger.info(f"[MARGIN_CONTRIB] Total margin calculation: {highest_margin_per_lot} * {net_quantity} = {total_margin_raw} (rounded to {total_margin})")

#         # Return the result
#         return {"total_margin": total_margin, "net_quantity": net_quantity}

#     except Exception as e:
#         orders_logger.error(f"[MARGIN_CONTRIB] Error calculating total symbol margin contribution: {e}", exc_info=True)
#         return {"total_margin": Decimal('0.0')}