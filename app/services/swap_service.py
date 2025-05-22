# app/services/swap_service.py

import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database.models import UserOrder, User
from app.crud.crud_order import get_all_system_open_orders
from app.core.cache import get_group_symbol_settings_cache
from app.firebase_stream import get_latest_market_data

logger = logging.getLogger(__name__)

async def apply_daily_swap_charges_for_all_open_orders(db: AsyncSession, redis_client: Redis):
    """
    Applies daily swap charges to all open orders.
    This function is intended to be called daily at UTC 00:00 by a scheduler.
    Formula: (Lot * swap_rate * market_close_price) / 365
    """
    logger.info("Starting daily swap charge application process.")
    open_orders: List[UserOrder] = await get_all_system_open_orders(db)

    if not open_orders:
        logger.info("No open orders found. Exiting swap charge process.")
        return

    processed_count = 0
    failed_count = 0

    for order in open_orders:
        try:
            # Access the eager-loaded user object
            user = order.user
            if not user:
                # Fallback if user was not loaded for some reason (should not happen with eager loading)
                logger.warning(f"User data not loaded for order {order.order_id} (user_id: {order.order_user_id}). Attempting direct fetch.")
                user_db_obj = await db.get(User, order.order_user_id)
                if not user_db_obj:
                    logger.warning(f"User ID {order.order_user_id} not found for order {order.order_id}. Skipping swap.")
                    failed_count += 1
                    continue
                user = user_db_obj

            user_group_name = getattr(user, 'group_name', 'default')
            order_symbol = order.order_company_name.upper()
            order_quantity = Decimal(str(order.order_quantity))
            order_type = order.order_type.upper()

            # 1. Get Group Settings for swap rates
            group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
            if not group_settings:
                logger.warning(f"Group settings not found for group '{user_group_name}', symbol '{order_symbol}'. Skipping swap for order {order.order_id}.")
                failed_count += 1
                continue

            swap_buy_rate_str = group_settings.get('swap_buy', "0.0")
            swap_sell_rate_str = group_settings.get('swap_sell', "0.0")

            try:
                swap_buy_rate = Decimal(str(swap_buy_rate_str))
                swap_sell_rate = Decimal(str(swap_sell_rate_str))
            except InvalidOperation as e:
                logger.error(f"Error converting swap rates from group settings for order {order.order_id}: {e}. Rates: buy='{swap_buy_rate_str}', sell='{swap_sell_rate_str}'. Skipping.")
                failed_count +=1
                continue

            swap_rate_to_use = swap_buy_rate if order_type == "BUY" else swap_sell_rate

            # 2. Get Market Close Price (using only the offer price)
            market_data = get_latest_market_data(order_symbol)

             # --- ADDED LOGGING HERE ---
            if market_data:
                logger.info(f"Retrieved market data for symbol '{order_symbol}' for order {order.order_id}: {market_data}")
            else:
                logger.info(f"No market data retrieved for symbol '{order_symbol}' for order {order.order_id}.")
            # --- END ADDED LOGGING ---

            if not market_data or 'o' not in market_data or market_data.get('o') is None:
                logger.warning(f"Market data (offer price) not found or incomplete for symbol '{order_symbol}' for order {order.order_id}. Skipping swap.")
                failed_count += 1
                continue

            try:
                offer_price = Decimal(str(market_data['o']))
                if offer_price <= Decimal("0"):
                     logger.warning(f"Invalid (non-positive) market offer price: ({offer_price}) for {order_symbol} for order {order.order_id}. Skipping swap.")
                     failed_count += 1
                     continue
                market_close_price = offer_price
            except (InvalidOperation, TypeError) as e:
                logger.error(f"Error processing market offer price for symbol '{order_symbol}' for order {order.order_id}: {e}. Data: {market_data}. Skipping.")
                failed_count += 1
                continue

            # 3. Calculate Daily Swap Charge
            # Formula: (Lot * swap_rate * market_close_price) / 365
            daily_swap_charge = (order_quantity * swap_rate_to_use * market_close_price) / Decimal(365)
            # Quantize to match UserOrder.swap field's precision
            daily_swap_charge = daily_swap_charge.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)

            # 4. Update Order's Swap Field
            current_swap_value = order.swap if order.swap is not None else Decimal("0.0")
            order.swap = current_swap_value + daily_swap_charge

            logger.info(f"Order {order.order_id}: Applied daily swap charge: {daily_swap_charge}. Old Swap: {current_swap_value}, New Swap: {order.swap}.")
            processed_count += 1

        except Exception as e:
            logger.error(f"General failure to process swap for order {order.order_id}: {e}", exc_info=True)
            failed_count += 1
            # Continue to the next order even if one fails

    if open_orders:
        try:
            await db.commit()
            logger.info(f"Daily swap charges committed to DB. Processed: {processed_count}, Failed: {failed_count}.")
        except Exception as e:
            logger.error(f"Failed to commit swap charges to DB: {e}", exc_info=True)
            await db.rollback()
            logger.info("Database transaction rolled back due to commit error in swap service.")
    else:
        logger.info("No open orders were processed, so no database commit was attempted for swap charges.")