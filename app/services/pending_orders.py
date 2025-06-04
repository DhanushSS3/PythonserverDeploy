# app/services/pending_orders.py

from typing import Dict, List, Optional, Any
from decimal import Decimal
from redis.asyncio import Redis
import logging
from datetime import datetime
import json
from pydantic import BaseModel # Added BaseModel import


from app.core.cache import (
    get_live_adjusted_buy_price_for_pair,
    get_user_data_cache,
    get_group_settings_cache,
    set_user_data_cache,
    set_user_portfolio_cache,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, # Added for margin calculation
    get_last_known_price # Added for portfolio calculation
)
from app.services.margin_calculator import calculate_single_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio # Added for portfolio calculation
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
from app.crud import crud_order
from app.crud.crud_order import get_order_model
from app.crud.user import update_user_margin # Added for direct user margin update

from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest

logger = logging.getLogger(__name__)

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"

async def add_pending_order(redis_client: Redis, order: Dict[str, Any]) -> None:
    """
    Add a new pending order to Redis.
    Orders are stored in a Redis Hash:
    Key: pending_orders:{symbol}:{order_type}
    Hash Field: user_id
    Hash Value: JSON string of a list of orders for that user
    """
    symbol = order['order_company_name']
    order_type = order['order_type']
    user_id = str(order['order_user_id'])
    
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"

    # Fetch existing orders for this user from Redis
    existing_orders_json = await redis_client.hget(redis_key, user_id)
    existing_orders = json.loads(existing_orders_json) if existing_orders_json else []

    # Append the new order
    existing_orders.append(order)

    # Store the updated list back in Redis
    await redis_client.hset(redis_key, user_id, json.dumps(existing_orders))
    logger.info(f"Added pending order {order['order_id']} for user {user_id}, {symbol} {order_type} to Redis.")

async def remove_pending_order(redis_client: Redis, order_id: str, symbol: str, order_type: str, user_id: str) -> None:
    """
    Remove a pending order from Redis.
    """
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"
    user_id_str = str(user_id)

    existing_orders_json = await redis_client.hget(redis_key, user_id_str)
    if not existing_orders_json:
        logger.warning(f"Attempted to remove pending order {order_id} but no orders found for user {user_id_str} under {redis_key}")
        return

    existing_orders = json.loads(existing_orders_json)
    
    # Filter out the order to be removed
    updated_orders = [
        order_item for order_item in existing_orders
        if order_item['order_id'] != order_id
    ]
    
    if not updated_orders:
        # If no orders left for this user, delete the field from the hash
        await redis_client.hdel(redis_key, user_id_str)
        logger.info(f"Removed all pending orders for user {user_id_str} under {redis_key} from Redis.")
    else:
        # Otherwise, store the updated list back
        await redis_client.hset(redis_key, user_id_str, json.dumps(updated_orders))
        logger.info(f"Removed pending order {order_id} for user {user_id_str}, {symbol} {order_type} from Redis.")

async def check_and_trigger_pending_orders(
    db,
    redis_client: Redis,
    symbol: str,
    current_price: Decimal
) -> None:
    """
    Check and trigger pending orders based on current market price for ALL users.
    Iterates through pending orders stored in Redis.
    """
    pending_order_types = ['BUY_LIMIT', 'SELL_LIMIT', 'BUY_STOP', 'SELL_STOP']

    for order_type in pending_order_types:
        redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"
        
        # Get all users and their pending orders for this symbol and order type
        all_users_orders_json = await redis_client.hgetall(redis_key)
        
        if not all_users_orders_json:
            continue

        # Process orders for each user
        for user_id_str, orders_json in all_users_orders_json.items():
            orders = json.loads(orders_json)
            orders_to_trigger = []
            orders_to_keep = []

            for order in orders:
                order_price = Decimal(str(order['order_price']))
                trigger_condition_met = False

                if order_type == 'BUY_LIMIT' and current_price <= order_price:
                    trigger_condition_met = True
                elif order_type == 'SELL_LIMIT' and current_price >= order_price:
                    trigger_condition_met = True
                elif order_type == 'BUY_STOP' and current_price >= order_price:
                    trigger_condition_met = True
                elif order_type == 'SELL_STOP' and current_price <= order_price:
                    trigger_condition_met = True
                
                if trigger_condition_met:
                    orders_to_trigger.append(order)
                else:
                    orders_to_keep.append(order)
            
            # Trigger orders that met the condition
            for triggered_order in orders_to_trigger:
                try:
                    await trigger_pending_order(db, redis_client, triggered_order, current_price)
                    # Removal from Redis is now handled inside trigger_pending_order
                except Exception as e:
                    logger.error(f"Failed to trigger pending order {triggered_order['order_id']}: {e}", exc_info=True)
                    # If triggering fails, keep the order in Redis for retry or manual intervention
                    orders_to_keep.append(triggered_order) 
            
            # Update Redis with remaining orders (those not triggered or failed to trigger)
            if not orders_to_keep:
                await redis_client.hdel(redis_key, user_id_str)
            else:
                await redis_client.hset(redis_key, user_id_str, json.dumps(orders_to_keep))


async def trigger_pending_order(
    db,
    redis_client: Redis,
    order: Dict[str, Any],
    current_price: Decimal
) -> None:
    """
    Trigger a pending order for any user type.
    Updates the order status to 'OPEN' or 'PROCESSING' in the database,
    adjusts user margin (for non-Barclays), and updates portfolio caches.
    """
    order_id = order['order_id']
    user_id = order['order_user_id']
    user_type = order['user_type']
    symbol = order['order_company_name']
    order_type = order['order_type']

    try:
        # Get user data and group settings to determine Barclays status
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.error(f"User data not found for user {user_id} during trigger of order {order_id}")
            # Remove from Redis if user data is missing (corrupted state)
            await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)
            return

        group_name = user_data.get('group_name')
        group_settings = await get_group_settings_cache(redis_client, group_name)
        if not group_settings:
            logger.error(f"Group settings not found for user {user_id} during trigger of order {order_id}")
            # Remove from Redis if group settings are missing
            await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)
            return

        sending_orders_setting = group_settings.get('sending_orders')
        is_barclays_live_user = (user_type == 'live' and sending_orders_setting == 'barclays')
        
        order_model = get_order_model(User if user_type == 'live' else DemoUser)
        db_order = await crud_order.get_order_by_id(db, order_id, order_model)
        if not db_order:
            logger.error(f"Order {order_id} not found in database during trigger. It might have been removed or processed by another instance.")
            # Remove from Redis if DB order is missing
            await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)
            return

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            logger.warning(f"Order {order_id} already processed (status: {db_order.order_status}). Skipping trigger.")
            # Remove from Redis if it's no longer pending in DB
            await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)
            return

        # --- Common calculations for both Barclays and Non-Barclays ---
        margin, adjusted_price, contract_value = await calculate_single_order_margin(
            db=db,
            redis_client=redis_client,
            user_id=user_id,
            order_quantity=Decimal(str(order['order_quantity'])),
            order_price=current_price, # Use current_price as the execution price
            symbol=symbol,
            order_type=order_type
        )

        # Update the DB order object with calculated values
        db_order.order_price = current_price # The actual price at which it was opened
        db_order.margin = margin
        db_order.contract_value = contract_value
        db_order.open_time = datetime.now() # Set open time

        if is_barclays_live_user:
            logger.info(f"Triggering Barclays pending order {order_id} for user {user_id}. Setting status to PROCESSING and sending to Firebase.")
            db_order.order_status = 'PROCESSING'
            # No margin check or update here for Barclays users, as it's external
            # The margin check and update will happen when the status transitions to OPEN via the PATCH endpoint.

            # Prepare data for Firebase
            firebase_order_data = {
                'order_id': db_order.order_id,
                'order_user_id': db_order.order_user_id,
                'order_company_name': db_order.order_company_name,
                'order_type': db_order.order_type,
                'order_status': db_order.order_status,  # Should be PROCESSING
                'order_price': str(db_order.order_price),
                'order_quantity': str(db_order.order_quantity),
                'contract_value': str(db_order.contract_value) if db_order.contract_value else None,
                'margin': str(db_order.margin) if db_order.margin else None,
                'stop_loss': str(db_order.stop_loss) if db_order.stop_loss else None,
                'take_profit': str(db_order.take_profit) if db_order.take_profit else None,
                'status': getattr(db_order, 'status', None),
                'open_time': db_order.open_time.isoformat() if db_order.open_time else None,
            }
            logger.info(f"[FIREBASE] Triggered pending order payload sent to Firebase: {firebase_order_data}")
            await send_order_to_firebase(firebase_order_data, "live")
            logger.info(f"[FIREBASE] Barclays pending order {order_id} sent to Firebase.")

        else: # Non-Barclays user
            logger.info(f"Triggering non-Barclays pending order {order_id} for user {user_id}. Setting status to OPEN.")
            
            # Check if user has enough free margin *before* updating DB
            current_wallet_balance = Decimal(str(user_data.get('wallet_balance', '0')))
            current_total_margin = Decimal(str(user_data.get('margin', '0')))
            new_total_margin = current_total_margin + margin
            
            if new_total_margin > current_wallet_balance:
                logger.warning(f"Insufficient free margin for user {user_id} to trigger pending order {order_id}. Wallet: {current_wallet_balance}, Current Margin: {current_total_margin}, Order Margin: {margin}")
                # Update order status to 'CANCELED' in DB
                db_order.order_status = 'CANCELED'
                db_order.cancel_message = "Insufficient margin to open order."
                await db.commit()
                await db.refresh(db_order)
                logger.info(f"Pending order {order_id} for user {user_id} canceled due to insufficient margin.")
                # Remove from Redis here as well since it's canceled
                await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)
                return # Exit early as order is canceled

            db_order.order_status = 'OPEN'
            
            # Update user's total margin in the database and cache
            await update_user_margin(db, user_id, user_type, new_total_margin)
            logger.info(f"User {user_id} margin updated to {new_total_margin} after triggering order {order_id}.")

        # Commit DB changes for the order status and updated fields
        await db.commit()
        await db.refresh(db_order)

        # --- Portfolio Update & Websocket Event (for non-Barclays or after Barclays confirmation) ---
        # Note: For Barclays users, the portfolio update will primarily be triggered by the PATCH /orders/{order_id}/status endpoint
        # when the status transitions from PROCESSING to OPEN.
        # This block here will still run for Barclays users, but the key margin/portfolio updates
        # will be based on the PATCH endpoint. For non-Barclays, this is where it happens.
        try:
            # Fetch user data again to get the latest margin if it was updated
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id, db, user_type)
            if user_data_for_portfolio:
                group_name = user_data_for_portfolio.get('group_name')
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                portfolio = await calculate_user_portfolio(user_data_for_portfolio, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after triggering order {order_id}.")
        except Exception as e:
            logger.error(f"Error updating portfolio cache or publishing websocket event after triggering order {order_id}: {e}", exc_info=True)

        logger.info(f"Successfully processed triggered pending order {order_id} for user {user_id}. Status set to {db_order.order_status}.")
        
        # Remove the order from Redis pending list AFTER successful processing
        await remove_pending_order(redis_client, order_id, symbol, order_type, user_id)

    except Exception as e:
        logger.error(f"Critical error in trigger_pending_order for order {order.get('order_id', 'N/A')}: {str(e)}", exc_info=True)
        # Re-raise to ensure it's caught by check_and_trigger_pending_orders for potential re-queuing
        raise
