# app/services/pending_orders.py

from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation
from redis.asyncio import Redis
import logging
from datetime import datetime, timezone
import json
from pydantic import BaseModel 
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import time
import uuid

from app.core.cache import (
    get_live_adjusted_buy_price_for_pair,
    get_user_data_cache,
    get_group_settings_cache,
    set_user_data_cache,
    set_user_portfolio_cache,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_last_known_price,
    get_adjusted_market_price_cache,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_static_orders_cache,
    get_user_static_orders_cache
)
from app.services.margin_calculator import calculate_single_order_margin, calculate_pending_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
from app.crud import crud_order
from app.crud.crud_order import get_order_model
from app.crud.user import update_user_margin 

from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest

logger = logging.getLogger("pending_orders")

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
            # Handle both bytes and string user keys
            if isinstance(user_key, bytes):
                current_user_id = user_key.decode('utf-8')
            else:
                current_user_id = user_key
                
            # Handle both bytes and string order lists
            if isinstance(orders_list_str, bytes):
                orders_list_str = orders_list_str.decode('utf-8')
                
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
    user_id = str(pending_order_data['order_user_id'])  # Ensure user_id is a string
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"

    try:
        all_user_orders_json = await redis_client.hget(redis_key, user_id)
        
        # Handle both bytes and string JSON
        if all_user_orders_json:
            if isinstance(all_user_orders_json, bytes):
                all_user_orders_json = all_user_orders_json.decode('utf-8')
            current_orders = json.loads(all_user_orders_json)
        else:
            current_orders = []

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
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            user_model = User if user_type == 'live' else DemoUser
            user_data = await user_model.by_id(db, user_id)
            if not user_data:
                logger.error(f"User data not found for user {user_id} when triggering order {order_id}. Skipping.")
                return

        group_name = user_data.get('group_name')
        group_settings = await get_group_settings_cache(redis_client, group_name)
        if not group_settings:
            logger.error(f"Group settings not found for group {group_name} when triggering order {order_id}. Skipping.")
            return

        order_model = get_order_model(user_type)
        db_order = await crud_order.get_order_by_id(db, order_id, order_model)

        if not db_order:
            logger.error(f"Database order {order_id} not found when triggering pending order. Skipping.")
            return

        is_barclays_live_user = user_type == 'live' and group_settings.get('sending_orders', '').lower() == 'barclays'

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            logger.warning(f"Order {order_id} already processed (status: {db_order.order_status}). Skipping trigger.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id) 
            return

        # Get the adjusted buy price (ask price) for the symbol from cache
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        
        if not adjusted_prices:
            logger.error(f"Adjusted market prices not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return
        
        # Use the adjusted buy price for all trigger conditions
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            logger.error(f"Adjusted buy price not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return
        
        adjusted_sell_price = adjusted_prices.get('sell')
        if not adjusted_sell_price:
            logger.error(f"Adjusted sell price not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return

        order_price = Decimal(str(db_order.order_price))
        
        # Check if the order should be triggered based on the order type and adjusted buy price
        should_trigger = False
        
        if order_type_original == 'BUY_LIMIT':
            # Trigger when the market's SELL price (bid) drops to or below the limit price.
            should_trigger = adjusted_buy_price <= order_price
            logger.info(f"BUY_LIMIT order {order_id}: adjusted_sell_price ({adjusted_sell_price}) <= order_price ({order_price})? {should_trigger}")
        
        elif order_type_original == 'SELL_STOP':
            # Trigger when adjusted_sell_price falls to or below order_price
            should_trigger = adjusted_buy_price <= order_price
            logger.info(f"SELL_STOP order {order_id}: adjusted_sell_price ({adjusted_sell_price}) <= order_price ({order_price})? {should_trigger}")
        
        elif order_type_original == 'SELL_LIMIT':
            # Trigger when the market's BUY price (ask) rises to or above the limit price.
            should_trigger = adjusted_buy_price >= order_price
            logger.info(f"SELL_LIMIT order {order_id}: adjusted_buy_price ({adjusted_buy_price}) >= order_price ({order_price})? {should_trigger}")
        
        elif order_type_original == 'BUY_STOP':
            # Trigger when adjusted_buy_price rises to or above order_price
            should_trigger = adjusted_buy_price >= order_price
            logger.info(f"BUY_STOP order {order_id}: adjusted_buy_price ({adjusted_buy_price}) >= order_price ({order_price})? {should_trigger}")
        
        else:
            logger.error(f"Unknown order type {order_type_original} for order {order_id}. Skipping trigger.")
            return
        
        if not should_trigger:
            logger.debug(f"Order {order_id} conditions not met for triggering. Skipping.")
            return
        
        logger.info(f"Triggering order {order_id} of type {order_type_original} with price {order_price} (adjusted_buy_price: {adjusted_buy_price})")
        
        # Calculate margin and contract value based on the current market price at trigger
        order_quantity_decimal = Decimal(str(db_order.order_quantity))
        
        # Get contract size and profit currency from symbol settings
        contract_size = Decimal(str(group_symbol_settings.get('contract_size', 100000)))
        profit_currency = group_symbol_settings.get('profit', 'USD')
        
        # Use adjusted_buy_price for margin calculation
        margin = calculate_pending_order_margin(
            order_type=order_type_original, # Use original order_type for margin calculation
            order_quantity=order_quantity_decimal,
            order_price=adjusted_buy_price, # Use adjusted_buy_price for margin calculation
            symbol_settings=group_symbol_settings
        )
        
        # Convert margin to USD if profit_currency is not USD
        if profit_currency != 'USD':
            logger.info(f"Converting margin from {profit_currency} to USD for order {order_id}")
            position_id = order_id
            value_description = f"margin for {symbol} {order_type_original} order"
            margin_usd = await _convert_to_usd(
                margin,
                profit_currency,
                user_id,
                position_id,
                value_description,
                db,
                redis_client
            )
            logger.info(f"Margin after USD conversion: {margin} {profit_currency} -> {margin_usd} USD")
            margin = margin_usd
        
        # Calculate contract value using the CORRECT formula
        contract_value = order_quantity_decimal * contract_size

        # --- Determine the new order_type for opened order ---
        new_order_type = order_type_original
        if order_type_original == 'BUY_LIMIT' or order_type_original == 'BUY_STOP':
            new_order_type = 'BUY'
        elif order_type_original == 'SELL_LIMIT' or order_type_original == 'SELL_STOP':
            new_order_type = 'SELL'
        
        # Update the DB order object with calculated values and the new order_type
        db_order.order_price = adjusted_buy_price  # Use adjusted_buy_price as the execution price
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
            current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
            current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))

            new_total_margin = current_total_margin_decimal + margin
            if new_total_margin > current_wallet_balance_decimal:
                db_order.order_status = 'CANCELLED'
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
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id, db, user_type) # Re-fetch updated user data
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

async def check_and_trigger_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis
) -> None:
    """
    Continuously checks all open orders for non-Barclays users to see if stop loss or take profit conditions are met.
    This function should be run in a background task or scheduled job.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for SL/TP check")
            return
            
        # Check for pending orders in Redis
        try:
            # List all keys matching the pending_orders pattern
            pending_keys = await redis_client.keys(f"{REDIS_PENDING_ORDERS_PREFIX}:*")
            logger.info(f"Found {len(pending_keys)} pending order keys in Redis")
            
            # Log details about each key
            for key in pending_keys:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = key  # Already a string
                
                # Get all user IDs for this key
                user_ids = await redis_client.hkeys(key_str)
                logger.info(f"Key: {key_str}, Users: {len(user_ids)}")
                
                # For each user, count their pending orders
                for user_id in user_ids:
                    # Handle both bytes and string user IDs
                    if isinstance(user_id, bytes):
                        user_id_str = user_id.decode('utf-8')
                    else:
                        user_id_str = user_id  # Already a string
                    
                    orders_json = await redis_client.hget(key_str, user_id_str)
                    if orders_json:
                        # Handle both bytes and string JSON
                        if isinstance(orders_json, bytes):
                            orders_json = orders_json.decode('utf-8')
                            
                        orders = json.loads(orders_json)
                        logger.info(f"  User {user_id_str} has {len(orders)} pending orders for {key_str}")
                        
                        # Log details about each order
                        for order in orders:
                            order_id = order.get('order_id', 'unknown')
                            order_type = order.get('order_type', 'unknown')
                            user_type = order.get('user_type', 'unknown')
                            symbol = order.get('order_company_name', 'unknown')
                            price = order.get('order_price', 'unknown')
                            logger.info(f"    Order {order_id}: {symbol} {order_type}, price={price}, user_type={user_type}")
                            
                            # Process each pending order
                            try:
                                # Get current price for the symbol
                                current_price = await get_last_known_price(redis_client, symbol)
                                if current_price:
                                    # Trigger the order if conditions are met
                                    await trigger_pending_order(db, redis_client, order, current_price)
                            except Exception as e:
                                logger.error(f"Error processing pending order {order_id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error checking pending orders in Redis: {e}", exc_info=True)
            
        # Get all open orders from both user and demo user tables
        from app.database.models import UserOrder, DemoUserOrder
        from sqlalchemy.future import select
        from app.crud import crud_order
        
        # First get all live user orders with SL/TP
        live_orders_stmt = select(UserOrder).where(
            (UserOrder.order_status == "OPEN") & 
            ((UserOrder.stop_loss != None) | (UserOrder.take_profit != None))
        )
        live_result = await db.execute(live_orders_stmt)
        live_orders = live_result.scalars().all()
        
        # Then get all demo user orders with SL/TP
        demo_orders_stmt = select(DemoUserOrder).where(
            (DemoUserOrder.order_status == "OPEN") & 
            ((DemoUserOrder.stop_loss != None) | (DemoUserOrder.take_profit != None))
        )
        demo_result = await db.execute(demo_orders_stmt)
        demo_orders = demo_result.scalars().all()
        
        # Process live user orders
        for order in live_orders:
            try:
                # Skip Barclays users - they are handled by the service provider
                user_data = await get_user_data_cache(redis_client, order.order_user_id, db, 'live')
                group_name = user_data.get('group_name') if user_data else None
                group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
                sending_orders = group_settings.get('sending_orders') if group_settings else None
                sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
                
                if sending_orders_normalized == 'barclays':
                    logger.debug(f"Skipping Barclays user order {order.order_id} for SL/TP check")
                    continue
                
                # Process the order for SL/TP
                await process_order_stoploss_takeprofit(db, redis_client, order, 'live')
            except Exception as e:
                logger.error(f"Error processing live order {order.order_id} for SL/TP: {e}", exc_info=True)
                continue
        
        # Process demo user orders
        for order in demo_orders:
            try:
                # Demo users are never Barclays users
                await process_order_stoploss_takeprofit(db, redis_client, order, 'demo')
            except Exception as e:
                logger.error(f"Error processing demo order {order.order_id} for SL/TP: {e}", exc_info=True)
                continue
            
        logger.debug(f"Completed SL/TP check cycle for {len(live_orders)} live orders and {len(demo_orders)} demo orders")
        
    except Exception as e:
        logger.error(f"Error in check_and_trigger_stoploss_takeprofit: {e}", exc_info=True)

async def process_order_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis,
    order,
    user_type: str
) -> None:
    """
    Process a single order to check if stop loss or take profit conditions are met.
    If conditions are met, close the order.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for processing order {order.order_id}")
            return
            
        symbol = order.order_company_name
        order_type = order.order_type
        stop_loss = order.stop_loss
        take_profit = order.take_profit
        
        if not stop_loss and not take_profit:
            return  # No SL/TP to check
        
        # Get current market price for the symbol
        try:
            current_prices = await get_last_known_price(redis_client, symbol)
            if not current_prices:
                logger.warning(f"No current price found for {symbol} when checking SL/TP for order {order.order_id}")
                return
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}", exc_info=True)
            return
        
        # For BUY orders:
        # - Stop loss triggers when price falls to or below stop_loss
        # - Take profit triggers when price rises to or above take_profit
        #
        # For SELL orders:
        # - Stop loss triggers when price rises to or above stop_loss
        # - Take profit triggers when price falls to or below take_profit
        
        try:
            current_bid = Decimal(str(current_prices.get('b', '0')))
            current_ask = Decimal(str(current_prices.get('a', '0')))
        except (InvalidOperation, TypeError) as e:
            logger.error(f"Error converting price values for {symbol}: {e}", exc_info=True)
            return
            
        if current_bid <= 0 or current_ask <= 0:
            logger.warning(f"Invalid price values for {symbol}: bid={current_bid}, ask={current_ask}")
            return
        
        # Use bid price for selling (when closing BUY orders)
        # Use ask price for buying (when closing SELL orders)
        close_price = current_bid if order_type == "BUY" else current_ask
        
        # Check stop loss condition
        sl_triggered = False
        tp_triggered = False
        
        if stop_loss:
            if order_type == "BUY" and close_price <= Decimal(str(stop_loss)):
                sl_triggered = True
                logger.info(f"Stop loss triggered for BUY order {order.order_id}: price {close_price} <= SL {stop_loss}")
            elif order_type == "SELL" and close_price >= Decimal(str(stop_loss)):
                sl_triggered = True
                logger.info(f"Stop loss triggered for SELL order {order.order_id}: price {close_price} >= SL {stop_loss}")
        
        # Check take profit condition
        if take_profit and not sl_triggered:  # Only check TP if SL wasn't triggered
            if order_type == "BUY" and close_price >= Decimal(str(take_profit)):
                tp_triggered = True
                logger.info(f"Take profit triggered for BUY order {order.order_id}: price {close_price} >= TP {take_profit}")
            elif order_type == "SELL" and close_price <= Decimal(str(take_profit)):
                tp_triggered = True
                logger.info(f"Take profit triggered for SELL order {order.order_id}: price {close_price} <= TP {take_profit}")
        
        # If either condition is triggered, close the order
        if sl_triggered or tp_triggered:
            trigger_type = "stop_loss" if sl_triggered else "take_profit"
            
            # Import here to avoid circular imports
            from app.api.v1.endpoints.orders import close_order
            from app.schemas.order import CloseOrderRequest
            from fastapi import Request, BackgroundTasks
            
            # Create a close order request
            close_request = CloseOrderRequest(
                order_id=order.order_id,
                close_price=close_price,
                user_id=order.order_user_id,
                order_type=order.order_type,
                order_company_name=order.order_company_name,
                order_status="OPEN"
            )
            
            # Create background tasks object
            background_tasks = BackgroundTasks()
            
            # Create a dummy request object
            request = Request(scope={"type": "http"})
            
            # Get the user object
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            if user_type == 'live':
                user = await get_user_by_id(db, order.order_user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, order.order_user_id)
            
            if not user:
                logger.error(f"User {order.order_user_id} not found when closing order {order.order_id}")
                return
            
            # Close the order
            from app.core.security import create_access_token
            import datetime
            
            # Create a service token for the operation
            access_token_expires = datetime.timedelta(minutes=5)  # Short-lived token
            token = create_access_token(
                data={"sub": str(user.id), "user_type": user_type, "is_service_account": True},
                expires_delta=access_token_expires
            )
            
            logger.info(f"Closing order {order.order_id} due to {trigger_type} trigger at price {close_price}")
            
            # Use the close_order function directly
            try:
                # We can't use the API endpoint directly, so we need to simulate the process
                # by updating the order status and other fields
                from app.crud.crud_order import update_order_with_tracking
                
                # Generate a close_id
                from app.services.order_processing import generate_unique_10_digit_id
                order_model = UserOrder if user_type == 'live' else DemoUserOrder
                close_id = await generate_unique_10_digit_id(db, order_model, 'close_id')
                
                # Calculate profit/loss
                entry_price = Decimal(str(order.order_price))
                quantity = Decimal(str(order.order_quantity))
                contract_size = Decimal(str(100000))  # Default contract size, should be fetched from symbol settings
                
                if order_type == "BUY":
                    profit = (close_price - entry_price) * quantity * contract_size
                else:  # SELL
                    profit = (entry_price - close_price) * quantity * contract_size
                
                # Update order
                update_fields = {
                    "order_status": "CLOSED",
                    "close_price": close_price,
                    "close_id": close_id,
                    "net_profit": profit,
                    "close_message": f"Closed automatically due to {trigger_type}"
                }
                
                # Update the order
                updated_order = await update_order_with_tracking(
                    db=db,
                    db_order=order,
                    update_fields=update_fields,
                    user_id=order.order_user_id,
                    user_type=user_type,
                    action_type=f"AUTO_{trigger_type.upper()}_CLOSE"
                )
                
                # Update user's wallet balance
                if user_type == 'live':
                    user.wallet_balance += profit
                else:
                    user.wallet_balance += profit
                
                # Adjust user's margin
                user.margin -= Decimal(str(order.margin or 0))
                if user.margin < 0:
                    user.margin = Decimal(0)
                
                # Commit changes
                await db.commit()
                
                # Update caches and publish events
                from app.api.v1.endpoints.orders import update_user_static_orders, publish_order_update, publish_user_data_update
                
                try:
                    await update_user_static_orders(order.order_user_id, db, redis_client, user_type)
                    await publish_order_update(redis_client, order.order_user_id)
                    await publish_user_data_update(redis_client, order.order_user_id)
                except Exception as e:
                    logger.error(f"Error updating caches after closing order {order.order_id}: {e}", exc_info=True)
                
                logger.info(f"Successfully closed order {order.order_id} due to {trigger_type} trigger")
                
            except Exception as close_error:
                logger.error(f"Error closing order {order.order_id} due to {trigger_type}: {close_error}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Error processing SL/TP for order {order.order_id}: {e}", exc_info=True)

async def get_last_known_price(redis_client: Redis, symbol: str) -> dict:
    """
    Get the last known price for a symbol from Redis.
    Returns a dictionary with bid (b) and ask (a) prices.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for getting last known price")
            return None
            
        # Try to get the price from Redis
        price_key = f"market_data:{symbol}"
        price_data = await redis_client.get(price_key)
        
        if not price_data:
            logger.warning(f"No price data found for {symbol}")
            return None
            
        try:
            price_dict = json.loads(price_data)
            return price_dict
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in price data for {symbol}: {price_data}")
            return None
    except Exception as e:
        logger.error(f"Error getting last known price for {symbol}: {e}", exc_info=True)
        return None

async def get_user_data_cache(redis_client: Redis, user_id: int, db: AsyncSession, user_type: str = 'live') -> dict:
    """
    Get user data from cache or database.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for getting user data for user {user_id}")
            # Fallback to database
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            
            if user_type == 'live':
                user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, user_id)
                
            if not user:
                return {}
                
            return {
                "id": user.id,
                "group_name": getattr(user, 'group_name', None),
                "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
                "margin": str(user.margin) if hasattr(user, 'margin') else "0"
            }
        
        # Try to get from cache
        user_key = f"user_data:{user_type}:{user_id}"
        user_data = await redis_client.get(user_key)
        
        if user_data:
            try:
                return json.loads(user_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in user data for user {user_id}: {user_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.user import get_user_by_id, get_demo_user_by_id
        
        if user_type == 'live':
            user = await get_user_by_id(db, user_id, user_type=user_type)
        else:
            user = await get_demo_user_by_id(db, user_id)
            
        if not user:
            return {}
            
        user_data = {
            "id": user.id,
            "group_name": getattr(user, 'group_name', None),
            "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
            "margin": str(user.margin) if hasattr(user, 'margin') else "0"
        }
        
        # Cache the user data
        try:
            await redis_client.set(user_key, json.dumps(user_data), ex=300)  # 5 minutes expiry
        except Exception as e:
            logger.error(f"Error caching user data for user {user_id}: {e}", exc_info=True)
        
        return user_data
    except Exception as e:
        logger.error(f"Error getting user data for user {user_id}: {e}", exc_info=True)
        return {}

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> dict:
    """
    Get group settings from cache or database.
    """
    try:
        if not group_name:
            return {}
            
        if not redis_client:
            logger.warning(f"Redis client not available for getting group settings for group {group_name}")
            # Fallback to database
            from app.crud.group import get_group_by_name
            from sqlalchemy.ext.asyncio import AsyncSession
            from app.database.session import AsyncSessionLocal
            
            async with AsyncSessionLocal() as db:
                group = await get_group_by_name(db, group_name)
                if not group:
                    return {}
                
                # Handle case where get_group_by_name returns a list
                group_obj = group[0] if isinstance(group, list) else group
                    
                return {
                    "id": group_obj.id,
                    "name": group_obj.name,
                    "sending_orders": getattr(group_obj, 'sending_orders', None)
                }
        
        # Try to get from cache
        group_key = f"group_settings:{group_name}"
        group_data = await redis_client.get(group_key)
        
        if group_data:
            try:
                return json.loads(group_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in group data for group {group_name}: {group_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.group import get_group_by_name
        from sqlalchemy.ext.asyncio import AsyncSession
        from app.database.session import AsyncSessionLocal
        
        async with AsyncSessionLocal() as db:
            group = await get_group_by_name(db, group_name)
            if not group:
                return {}
            
            # Handle case where get_group_by_name returns a list
            group_obj = group[0] if isinstance(group, list) else group
                
            group_data = {
                "id": group_obj.id,
                "name": group_obj.name,
                "sending_orders": getattr(group_obj, 'sending_orders', None)
            }
            
            # Cache the group data
            try:
                await redis_client.set(group_key, json.dumps(group_data), ex=300)  # 5 minutes expiry
            except Exception as e:
                logger.error(f"Error caching group data for group {group_name}: {e}", exc_info=True)
            
            return group_data
    except Exception as e:
        logger.error(f"Error getting group settings for group {group_name}: {e}", exc_info=True)
        return {}

async def update_user_static_orders(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user after order changes.
    This includes both open and pending orders.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        logger.info(f"Starting update_user_static_orders for user {user_id}, user_type {user_type}")
        order_model = get_order_model(user_type)
        logger.info(f"Using order model: {order_model.__name__}")
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        logger.info(f"Fetched {len(open_orders_orm)} open orders for user {user_id}")
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                       for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                   'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        logger.info(f"Fetched {len(pending_orders_orm)} pending orders for user {user_id}")
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        logger.info(f"Updated static orders cache for user {user_id} with {len(open_orders_data)} open orders and {len(pending_orders_data)} pending orders")
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.now().isoformat()}