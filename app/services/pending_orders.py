# app/services/pending_orders.py

from typing import Dict, List, Optional, Any
from decimal import Decimal
from redis.asyncio import Redis
import logging
from datetime import datetime
import json
from pydantic import BaseModel 


from app.core.cache import (
    get_live_adjusted_buy_price_for_pair,
    get_user_data_cache,
    get_group_settings_cache,
    set_user_data_cache,
    set_user_portfolio_cache,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_last_known_price 
)
from app.services.margin_calculator import calculate_single_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio 
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
from app.crud import crud_order
from app.crud.crud_order import get_order_model
from app.crud.user import update_user_margin 

from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest

logger = logging.getLogger(__name__)

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"


async def remove_pending_order(redis_client: Redis, order_id: str, symbol: str, order_type: str, user_id: str):
    """Removes a single pending order from Redis."""
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"
    try:
        # Fetch all orders for the key
        all_user_orders_json = await redis_client.hgetall(redis_key)
        
        updated_user_orders = {}
        order_removed = False

        for user_key, orders_list_str in all_user_orders_json.items():
            current_user_id = user_key.decode('utf-8')
            orders_list = json.loads(orders_list_str)
            
            # Filter out the specific order to be removed
            filtered_orders = [order for order in orders_list if order.get('order_id') != order_id]
            
            if len(filtered_orders) < len(orders_list):
                order_removed = True
            
            if filtered_orders:
                updated_user_orders[user_key] = json.dumps(filtered_orders)
        
        if order_removed:
            if updated_user_orders:
                # Atomically update the hash for the users whose orders changed
                pipe = redis_client.pipeline()
                for user_key, orders_json in updated_user_orders.items():
                    pipe.hset(redis_key, user_key, orders_json)
                await pipe.execute()
            else:
                # If no orders remain for this key after filtering, delete the hash
                await redis_client.delete(redis_key)
            logger.info(f"Pending order {order_id} removed from Redis for symbol {symbol} type {order_type} and user {user_id}.")
        else:
            logger.warning(f"Attempted to remove pending order {order_id} from Redis, but it was not found for symbol {symbol} type {order_type} and user {user_id}.")

    except Exception as e:
        logger.error(f"Error removing pending order {order_id} from Redis: {e}", exc_info=True)


async def add_pending_order(
    redis_client: Redis, 
    pending_order_data: Dict[str, Any]
) -> None:
    """Adds a pending order to Redis."""
    symbol = pending_order_data['order_company_name']
    order_type = pending_order_data['order_type']
    user_id = pending_order_data['order_user_id']
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"

    try:
        all_user_orders_json = await redis_client.hget(redis_key, user_id)
        current_orders = json.loads(all_user_orders_json) if all_user_orders_json else []

        # Check if an order with the same ID already exists
        if any(order.get('order_id') == pending_order_data['order_id'] for order in current_orders):
            logger.warning(f"Pending order {pending_order_data['order_id']} already exists in Redis. Skipping add.")
            return

        current_orders.append(pending_order_data)
        await redis_client.hset(redis_key, user_id, json.dumps(current_orders))
        logger.info(f"Pending order {pending_order_data['order_id']} added to Redis for user {user_id} under key {redis_key}.")
    except Exception as e:
        logger.error(f"Error adding pending order {pending_order_data['order_id']} to Redis: {e}", exc_info=True)
        raise

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
    order_type_original = order['order_type'] # Store original order_type

    try:
        user_data = await get_user_data_cache(redis_client, user_id)
        if not user_data:
            user_model = User if user_type == 'live' else DemoUser
            user_data = await user_model.by_id(db, user_id)
            if not user_data:
                logger.error(f"User data not found for user {user_id} when triggering order {order_id}. Skipping.")
                return

        group_settings = await get_group_settings_cache(redis_client, user_data.group_id)
        if not group_settings:
            logger.error(f"Group settings not found for group {user_data.group_id} when triggering order {order_id}. Skipping.")
            return

        order_model = get_order_model(user_type)
        db_order = await crud_order.get_order_by_id(db, order_id, order_model)

        if not db_order:
            logger.error(f"Database order {order_id} not found when triggering pending order. Skipping.")
            return

        is_barclays_live_user = user_type == 'live' and group_settings.get('is_barclays_live', False)

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            logger.warning(f"Order {order_id} already processed (status: {db_order.order_status}). Skipping trigger.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id) 
            return

        # Calculate margin and contract value based on the current market price at trigger
        order_price_decimal = Decimal(str(db_order.order_price)) # Use original order price for margin calculation if needed
        order_quantity_decimal = Decimal(str(db_order.order_quantity))
        
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, user_data.group_id, symbol)
        
        margin = calculate_single_order_margin(
            order_type=db_order.order_type, # Use original order_type for margin calculation
            order_quantity=order_quantity_decimal,
            order_price=current_price, # Margin based on the current trigger price
            symbol_settings=group_symbol_settings
        )
        
        contract_value = (order_quantity_decimal * current_price).quantize(Decimal('0.01'))

        # --- Determine the new order_type for opened order ---
        new_order_type = order_type_original
        if order_type_original == 'BUY_LIMIT' or order_type_original == 'BUY_STOP':
            new_order_type = 'BUY'
        elif order_type_original == 'SELL_LIMIT' or order_type_original == 'SELL_STOP':
            new_order_type = 'SELL'
        
        # Update the DB order object with calculated values and the new order_type
        db_order.order_price = current_price 
        db_order.margin = margin
        db_order.contract_value = contract_value
        db_order.open_time = datetime.now() 
        db_order.order_type = new_order_type 

        if is_barclays_live_user:
            logger.info(f"Triggering Barclays pending order {order_id} for user {user_id}. Setting status to PROCESSING and sending to Firebase.")
            db_order.order_status = 'PROCESSING'
            
            firebase_order_data = {
                'order_id': db_order.order_id,
                'order_user_id': db_order.order_user_id,
                'order_company_name': db_order.order_company_name,
                'order_type': db_order.order_type, 
                'order_status': db_order.order_status,
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
            current_wallet_balance_decimal = Decimal(str(user_data.wallet.balance)) if user_data.wallet else Decimal('0')
            current_total_margin_decimal = Decimal(str(user_data.total_margin)) if user_data.total_margin else Decimal('0')

            new_total_margin = current_total_margin_decimal + margin
            if new_total_margin > current_wallet_balance_decimal:
                db_order.order_status = 'CANCELED'
                db_order.cancel_message = "InsufficientFreeMargin"
                logger.warning(f"Order {order_id} for user {user_id} canceled due to insufficient margin. Required: {new_total_margin}, Available: {current_wallet_balance_decimal}")
                await db.commit()
                await db.refresh(db_order)
                await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
                return

            await update_user_margin(
                db,
                redis_client,
                user_id,
                new_total_margin,
                user_type
            )
            db_order.order_status = 'OPEN'
            logger.info(f"Non-Barclays pending order {order_id} for user {user_id} opened successfully. New total margin: {new_total_margin}")

        # Commit DB changes for the order status and updated fields
        await db.commit()
        await db.refresh(db_order)

        try:
            # --- Portfolio Update & Websocket Event ---
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id) # Re-fetch updated user data
            if user_data_for_portfolio:
                open_positions_dicts = [o.to_dict() for o in await crud_order.get_user_open_orders(db, user_id, order_model)]
                
                # Fetch current prices for all open positions to calculate portfolio correctly
                adjusted_market_prices = {}
                if group_symbol_settings: # Ensure group_symbol_settings is not None
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

        logger.info(f"Successfully processed triggered pending order {order_id} for user {user_id}. Status set to {db_order.order_status}. Order Type changed from {order_type_original} to {new_order_type}.")
        
        # Remove the order from Redis pending list AFTER successful processing
        # Use the original_order_type for removal as that's how it's stored in Redis
        await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)

    except Exception as e:
        logger.error(f"Critical error in trigger_pending_order for order {order.get('order_id', 'N/A')}: {str(e)}", exc_info=True)
        raise