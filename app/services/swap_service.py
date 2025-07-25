# app/services/swap_service.py

import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database.models import UserOrder, User
from app.crud.crud_order import get_all_system_open_orders
from app.core.cache import get_group_symbol_settings_cache
from app.core.firebase import get_latest_market_data
from app.crud import group as crud_group  # Add this import to fetch group info

# logger = logging.getLogger(__name__)
from app.core.logging_config import swap_logger as logger

async def apply_daily_swap_charges_for_all_open_orders(db: AsyncSession, redis_client: Redis):
    """
    Applies daily swap charges to all open orders.
    This function is intended to be called daily at UTC 00:00 by a scheduler.
    Formula: (Lot * swap_rate * market_close_price) / 365
    """
    logger.info("[SWAP] Starting daily swap charge application process.")
    open_orders: List[UserOrder] = await get_all_system_open_orders(db)

    logger.info(f"[SWAP] Number of open orders fetched: {len(open_orders)}")

    if not open_orders:
        logger.info("[SWAP] No open orders found. Exiting swap charge process.")
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

            # --- Fetch pips from Group table for this group_name and symbol ---
            group_obj = await crud_group.get_group_by_symbol_and_name(db, symbol=order_symbol, name=user_group_name)
            if not group_obj or group_obj.pips is None:
                logger.warning(f"Pips not found for group '{user_group_name}', symbol '{order_symbol}'. Skipping swap for order {order.order_id}.")
                failed_count += 1
                continue
            try:
                pips = Decimal(str(group_obj.pips))
            except Exception as e:
                logger.error(f"Error converting pips for group '{user_group_name}', symbol '{order_symbol}': {e}. Skipping swap for order {order.order_id}.")
                failed_count += 1
                continue

            # --- New Swap Formula ---
            # ((order_quantity * pips) * (order_quantity * swap_rate))/10
            logger.info(f"[SWAP] Calculating daily swap charge for order {order.order_id}: ((order_quantity={order_quantity} * pips={pips}) * (order_quantity={order_quantity} * swap_rate_to_use={swap_rate_to_use}))/10")
            try:
                daily_swap_charge = ((order_quantity * pips) * (order_quantity * swap_rate_to_use)) / Decimal('10')
            except Exception as e:
                logger.error(f"Error calculating new swap formula for order {order.order_id}: {e}")
                failed_count += 1
                continue
            logger.info(f"[SWAP] Raw daily swap charge for order {order.order_id}: {daily_swap_charge}")
            # Quantize to match UserOrder.swap field's precision
            daily_swap_charge = daily_swap_charge.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
            logger.info(f"[SWAP] Quantized daily swap charge for order {order.order_id}: {daily_swap_charge}")

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
            logger.info(f"[SWAP] Daily swap charges committed to DB. Processed: {processed_count}, Failed: {failed_count}.")
        except Exception as e:
            logger.error(f"[SWAP] Failed to commit swap charges to DB: {e}", exc_info=True)
            await db.rollback()
            logger.info("[SWAP] Database transaction rolled back due to commit error in swap service.")
    else:
        logger.info("[SWAP] No open orders were processed, so no database commit was attempted for swap charges.")
    logger.info("[SWAP] Daily swap charge application process completed.")