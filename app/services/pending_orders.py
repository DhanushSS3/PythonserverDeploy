# app/services/pending_orders.py

from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from redis.asyncio import Redis
import logging
from datetime import datetime, timezone
import json
from pydantic import BaseModel 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select  # Add this import
import asyncio
import time
import uuid
import inspect  # Added for debug function

from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,
    set_user_static_orders_cache, get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache, get_user_dynamic_portfolio_cache,
    DecimalEncoder, decode_decimal,
    publish_order_update, publish_user_data_update,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_static_orders_cache,
    get_user_static_orders_cache,
    DecimalEncoder  # Import for JSON serialization of decimals
)
from app.services.margin_calculator import calculate_single_order_margin, calculate_pending_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet
from app.crud import crud_order
from app.crud.user import update_user_margin, get_user_by_id_with_lock, get_demo_user_by_id_with_lock
from app.crud.crud_order import get_open_orders_by_user_id_and_symbol, get_order_model
from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest
from app.schemas.wallet import WalletCreate
# Import ALL necessary functions from order_processing to ensure consistency
from app.services.order_processing import (
    calculate_total_symbol_margin_contribution,  # Use THIS implementation, not the one from margin_calculator
    get_external_symbol_info,
    get_order_model,
    OrderProcessingError,
    InsufficientFundsError,
    generate_unique_10_digit_id
)
from app.core.logging_config import orders_logger

logger = logging.getLogger("orders")

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"

async def debug_margin_calculations(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_orders: list,
    simulated_order,
    user_type: str
):
    """
    Debug helper function to compare margin calculation implementations
    between order_processing.py and margin_calculator.py
    """
    try:
        # Load the different implementations
        from app.services.order_processing import calculate_total_symbol_margin_contribution as processing_calc
        from app.services.margin_calculator import calculate_total_symbol_margin_contribution as calculator_calc
        
        # Get the order model
        order_model = get_order_model(user_type)
        
        # Run calculations with both implementations
        processing_margin_before = await processing_calc(db, redis_client, user_id, symbol, open_orders, order_model, user_type)
        calculator_margin_before = await calculator_calc(db, redis_client, user_id, symbol, open_orders, user_type, order_model)
        
        # Add new order
        orders_with_new = open_orders + [simulated_order]
        processing_margin_after = await processing_calc(db, redis_client, user_id, symbol, orders_with_new, order_model, user_type)
        calculator_margin_after = await calculator_calc(db, redis_client, user_id, symbol, orders_with_new, user_type, order_model)
        
        # Calculate additional margins
        processing_additional = max(Decimal("0"), processing_margin_after["total_margin"] - processing_margin_before["total_margin"])
        calculator_additional = max(Decimal("0"), calculator_margin_after["total_margin"] - calculator_margin_before["total_margin"])
        
        # Log the results for comparison
        from app.core.logging_config import orders_logger
        orders_logger.info(f"[MARGIN_DEBUG] === IMPLEMENTATION COMPARISON ===")
        orders_logger.info(f"[MARGIN_DEBUG] User {user_id}, Symbol {symbol}, Orders: {len(open_orders)} + 1 new")
        orders_logger.info(f"[MARGIN_DEBUG] order_processing.py implementation:")
        orders_logger.info(f"[MARGIN_DEBUG]   - Margin before: {processing_margin_before['total_margin']}")
        orders_logger.info(f"[MARGIN_DEBUG]   - Margin after: {processing_margin_after['total_margin']}")
        orders_logger.info(f"[MARGIN_DEBUG]   - Additional margin: {processing_additional}")
        orders_logger.info(f"[MARGIN_DEBUG] margin_calculator.py implementation:")
        orders_logger.info(f"[MARGIN_DEBUG]   - Margin before: {calculator_margin_before['total_margin']}")
        orders_logger.info(f"[MARGIN_DEBUG]   - Margin after: {calculator_margin_after['total_margin']}")
        orders_logger.info(f"[MARGIN_DEBUG]   - Additional margin: {calculator_additional}")
        orders_logger.info(f"[MARGIN_DEBUG] Difference in additional margin: {abs(processing_additional - calculator_additional)}")
        
        # Check parameter order differences
        processing_signature = inspect.signature(processing_calc)
        calculator_signature = inspect.signature(calculator_calc)
        
        orders_logger.info(f"[MARGIN_DEBUG] order_processing.py parameter order: {list(processing_signature.parameters.keys())}")
        orders_logger.info(f"[MARGIN_DEBUG] margin_calculator.py parameter order: {list(calculator_signature.parameters.keys())}")
        
        # Using order_processing implementation for consistency
        return processing_additional
    except Exception as e:
        from app.core.logging_config import orders_logger
        orders_logger.error(f"[MARGIN_DEBUG] Error comparing implementations: {str(e)}", exc_info=True)
        return None


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
    Updates the order status to 'OPEN' in the database,
    adjusts user margin, and updates portfolio caches.
    """
    order_id = order['order_id']
    user_id = order['order_user_id']
    user_type = order['user_type']
    symbol = order['order_company_name']
    order_type_original = order['order_type'] # Store original order_type
    from app.core.logging_config import orders_logger
    
    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Starting trigger_pending_order for order {order_id}, user {user_id}, symbol {symbol}, type {order_type_original}")
    
    try:
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            user_model = User if user_type == 'live' else DemoUser
            user_data = await user_model.by_id(db, user_id)
            if not user_data:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] User data not found for user {user_id} when triggering order {order_id}. Skipping.")
                return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] User data loaded for user {user_id}: group={user_data.get('group_name')}, wallet_balance={user_data.get('wallet_balance')}")

        group_name = user_data.get('group_name')
        group_settings = await get_group_settings_cache(redis_client, group_name)
        if not group_settings:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Group settings not found for group {group_name} when triggering order {order_id}. Skipping.")
            return

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Group settings loaded for group {group_name}")

        order_model = get_order_model(user_type)
        
        # Add enhanced retry logic for database order fetch
        max_retries = 5  # Increase from 3 to 5
        initial_retry_delay = 0.5  # Start with a shorter delay (seconds)
        retry_delay = initial_retry_delay
        db_order = None
        
        for retry_count in range(max_retries):
            # Try the standard method first
            db_order = await crud_order.get_order_by_id(db, order_id, order_model)
            
            if db_order:
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} found on attempt {retry_count + 1}")
                break
                
            # On the last attempt, try a direct SQL query as a backup method
            if retry_count == max_retries - 1:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Last attempt: trying direct SQL query for order {order_id}")
                try:
                    from sqlalchemy import text
                    table_name = "demo_user_orders" if user_type == "demo" else "user_orders"
                    result = await db.execute(text(f"SELECT * FROM {table_name} WHERE order_id = :order_id"), {"order_id": order_id})
                    row = result.fetchone()
                    if row:
                        # Create order object from raw SQL result
                        db_order = order_model()
                        for key, value in row._mapping.items():
                            setattr(db_order, key, value)
                        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Found order {order_id} with direct SQL query")
                        break
                except Exception as sql_error:
                    orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error in direct SQL query: {str(sql_error)}", exc_info=True)
            
            if retry_count < max_retries - 1:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} not found on attempt {retry_count + 1}/{max_retries}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 1.5  # Less aggressive exponential backoff

        if not db_order:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} not found after {max_retries} attempts when triggering pending order. Skipping.")
            # Remove the order from Redis since it cannot be found in the database
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Removing order {order_id} from Redis since it cannot be found in the database.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            return

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order loaded: status={db_order.order_status}, price={db_order.order_price}")

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} already processed (status: {db_order.order_status}). Skipping trigger.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id) 
            return

        # Get the adjusted buy price (ask price) for the symbol from cache
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        
        if not adjusted_prices:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted market prices not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted prices loaded: {adjusted_prices}")
        
        # Use the adjusted buy price for all trigger conditions
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        adjusted_sell_price = adjusted_prices.get('sell')
        if not adjusted_sell_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted sell price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return

        # Normalize decimal values for comparison - round to 5 decimal places
        try:
            order_price = Decimal(str(db_order.order_price))
            # Round to 5 decimal places for consistent comparison
            order_price_normalized = Decimal(str(round(order_price, 5)))
            
            adjusted_buy_price_str = str(adjusted_buy_price)
            # Ensure the price has at least 5 decimal places
            if '.' in adjusted_buy_price_str:
                integer_part, decimal_part = adjusted_buy_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_buy_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_buy_price_normalized = Decimal(str(round(Decimal(adjusted_buy_price_str), 5)))
            
            # Similar normalization for adjusted_sell_price if needed
            adjusted_sell_price_str = str(adjusted_sell_price)
            if '.' in adjusted_sell_price_str:
                integer_part, decimal_part = adjusted_sell_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_sell_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_sell_price_normalized = Decimal(str(round(Decimal(adjusted_sell_price_str), 5)))
            
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Normalized prices for comparison - order_price: {order_price_normalized}, adjusted_buy_price: {adjusted_buy_price_normalized}, adjusted_sell_price: {adjusted_sell_price_normalized}")
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error normalizing prices for comparison: {str(e)}", exc_info=True)
            return
        
        # Only use adjusted_buy_price for all pending order types
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        # Determine if the order should be triggered
        should_trigger = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger = adjusted_buy_price_normalized <= order_price_normalized
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] BUY_LIMIT/SELL_STOP check: {adjusted_buy_price_normalized} <= {order_price_normalized} = {should_trigger}")
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger = adjusted_buy_price_normalized >= order_price_normalized
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] SELL_LIMIT/BUY_STOP check: {adjusted_buy_price_normalized} >= {order_price_normalized} = {should_trigger}")
        else:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Unknown order type {order_type_original} for order {order_id}. Skipping execution.")
            return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Checking order {order_id}: type={order_type_original}, adjusted_buy_price={adjusted_buy_price_normalized}, order_price={order_price_normalized}, should_trigger={should_trigger}")
        
        # Additional debug logging for price comparison
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Price comparison values - adjusted_buy_price_normalized: {adjusted_buy_price_normalized} ({type(adjusted_buy_price_normalized)}), order_price_normalized: {order_price_normalized} ({type(order_price_normalized)})")
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Raw price values - adjusted_buy_price: {adjusted_buy_price} ({type(adjusted_buy_price)}), order_price: {order_price} ({type(order_price)})")
        
        # Convert to strings for consistent comparison (handles potential float vs Decimal issues)
        adjusted_price_str = str(adjusted_buy_price_normalized)
        order_price_str = str(order_price_normalized)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] String comparison for prices: '{adjusted_price_str}' vs '{order_price_str}'")
        
        # Compare numeric difference
        price_diff = abs(adjusted_buy_price_normalized - order_price_normalized)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Absolute price difference: {price_diff}")

        # Compare with small epsilon tolerance to catch very close values
        epsilon = Decimal('0.00001')  # Small tolerance
        is_close = price_diff < epsilon
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Prices within epsilon tolerance: {is_close} (epsilon={epsilon})")
        
        # Consider using epsilon for near-exact matches
        should_trigger_with_epsilon = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized <= order_price_normalized) or is_close
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] BUY_LIMIT/SELL_STOP with epsilon: {should_trigger_with_epsilon}")
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized >= order_price_normalized) or is_close
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] SELL_LIMIT/BUY_STOP with epsilon: {should_trigger_with_epsilon}")
        
        # Use epsilon-based trigger when prices are very close
        if should_trigger_with_epsilon and not should_trigger:
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Using epsilon-based trigger since prices are very close: {adjusted_buy_price_normalized} vs {order_price_normalized}")
            should_trigger = True
        
        if not should_trigger:
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} conditions not met for execution. Skipping.")
            return
        
        # Calculate the required margin for the order using calculate_single_order_margin
        order_quantity_decimal = Decimal(str(db_order.order_quantity))
        user_leverage = Decimal(str(user_data.get('leverage', '1.0')))
        # Get external symbol info from DB
        external_symbol_info = await get_external_symbol_info(db, symbol)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] External symbol info loaded: {external_symbol_info}")
        
        # Get raw market data (for margin calculation) using the synchronous version
        from app.firebase_stream import get_latest_market_data as get_latest_market_data_sync
        raw_market_data = get_latest_market_data_sync(symbol)
        if not raw_market_data or not raw_market_data.get('o'):
            # Fallback to last known price from Redis
            last_known = await get_last_known_price(redis_client, symbol)
            if last_known:
                raw_market_data = last_known
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Fallback to last known price for {symbol}: {raw_market_data}")
            else:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] No market data or last known price for {symbol}. Cannot calculate margin for order {order_id}.")
                return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Raw market data loaded: {raw_market_data}")
        
        try:
            # Wrap the calculate_single_order_margin call in a try block to catch any exceptions
            margin, exec_price, contract_value, commission = await calculate_single_order_margin(
                redis_client=redis_client,
                symbol=symbol,
                order_type=order_type_original,
                quantity=order_quantity_decimal,
                user_leverage=user_leverage,
                group_settings=group_symbol_settings,
                external_symbol_info=external_symbol_info or {},
                raw_market_data={symbol: raw_market_data},
                db=db,
                user_id=user_id
            )
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Margin calculated successfully for order {order_id}: margin={margin}, exec_price={exec_price}, contract_value={contract_value}")
            
            # Implement proper margin calculation using the same approach as order_processing.py
            try:
                # Step 1: Get all open orders for the symbol
                open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                    db, user_id, symbol, order_model
                )
                
                # Store original calculated margin for the individual order record
                original_order_margin = margin
                
                # Step 2: Calculate total margin before adding the new order
                margin_before_data = await calculate_total_symbol_margin_contribution(
                    db, redis_client, user_id, symbol, open_orders_for_symbol, order_model, user_type
                )
                margin_before = margin_before_data["total_margin"]
                
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Current symbol margin contribution (before): {margin_before}")
                
                # Step 3: Create a simulated order object for the new order
                new_order_type = 'BUY' if order_type_original.startswith('BUY') else 'SELL'
                simulated_order = type('Obj', (object,), {
                    'order_quantity': order_quantity_decimal,
                    'order_type': new_order_type,
                    'margin': original_order_margin,
                    'id': None,  # Add id attribute to match real orders
                    'order_id': 'NEW_PENDING_TRIGGERED'  # Add order_id attribute for logging
                })()
                
                # Step 4: Calculate total margin after adding the new order
                margin_after_data = await calculate_total_symbol_margin_contribution(
                    db, redis_client, user_id, symbol, 
                    open_orders_for_symbol + [simulated_order],
                    order_model, user_type
                )
                margin_after = margin_after_data["total_margin"]
                
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Symbol margin contribution after adding order: {margin_after}")
                
                # Step 5: Calculate additional margin required (can be negative if hedging reduces margin)
                # Let the calculate_total_symbol_margin_contribution function handle all the hedging logic
                margin_difference = max(Decimal("0.0"), margin_after - margin_before)
                
                # For logging and debugging
                if margin_after < margin_before:
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Hedging detected: order {order_id} reduces total margin by {margin_before - margin_after}")
                
                # Use margin_difference for updating user's total margin
                # This can be positive (adding margin) or negative (reducing margin due to hedging)
                margin = margin_difference
                
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Margin change required: {margin} " +
                                 f"(Before: {margin_before}, After: {margin_after})")
                
            except Exception as margin_calc_error:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error calculating margin effect for user {user_id}: {str(margin_calc_error)}", exc_info=True)
                # If the advanced calculation fails, fall back to using the original calculated margin
                margin = original_order_margin 
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Falling back to basic margin calculation: {margin}")
        except Exception as margin_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error calculating margin for order {order_id}: {str(margin_error)}", exc_info=True)
            return
        
        # Fetch the latest dynamic portfolio to get up-to-date free_margin
        try:
            dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
            user_free_margin = dynamic_portfolio.get('free_margin', 'N/A') if dynamic_portfolio else 'N/A'
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Latest free_margin for user {user_id}: {user_free_margin}")
        except Exception as portfolio_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error fetching dynamic portfolio for user {user_id}: {str(portfolio_error)}", exc_info=True)
            return
        
        # We'll skip the free margin check since we're already handling the hedging logic
        # and checking against wallet_balance later in the function
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Executing order {order_id} for user {user_id} at price {adjusted_buy_price_normalized} (order_price={order_price_normalized}, type={order_type_original})")
        
        # Get contract size and profit currency from symbol settings
        contract_size = Decimal(str(group_symbol_settings.get('contract_size', 100000)))
        profit_currency = group_symbol_settings.get('profit', 'USD')
        
        # Calculate contract value using the CORRECT formula - this should match what we calculated above
        contract_value = order_quantity_decimal * contract_size
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Contract value calculated: {contract_value}")

        # Update the order properties with the calculated values
        try:
            # Store the original values for logging
            original_margin = db_order.margin if db_order.margin else "None"
            
            # Store the original calculated margin in the order (without any hedging adjustments)
            # This ensures we always record the true margin requirement for this individual order
            db_order.margin = original_order_margin
            db_order.contract_value = contract_value
            db_order.commission = commission
            db_order.order_price = exec_price  # Use the execution price from margin calculation
            
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated order object with calculated values: " 
                             f"margin={original_order_margin} (was {original_margin}), contract_value={contract_value}, "
                             f"commission={commission}, execution_price={exec_price}")
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Note: Order margin stored is the individual margin requirement ({original_order_margin}), while margin change applied to user ({margin}) considers total symbol margin effect")
        except Exception as value_update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating order with calculated values: {str(value_update_error)}", exc_info=True)

        # Determine the new order type (removing LIMIT/STOP)
        try:
            new_order_type = 'BUY' if order_type_original in ['BUY_LIMIT', 'BUY_STOP'] else 'SELL'
            db_order.open_time = datetime.now(timezone.utc) 
            db_order.order_type = new_order_type
            # Set stop_loss and take_profit to None
            db_order.stop_loss = None
            db_order.take_profit = None
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated order object with new values and cleared SL/TP")
        except Exception as update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating order object with new values: {str(update_error)}", exc_info=True)
            return

        # Non-Barclays user
        # FIX: Refresh user data right before margin calculation to get current state
        try:
            # Get fresh user data from database right before margin calculation
            user_model = User if user_type == 'live' else DemoUser
            fresh_user_result = await db.execute(select(user_model).filter(user_model.id == user_id))
            fresh_user = fresh_user_result.scalars().first()
            
            if not fresh_user:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Could not fetch fresh user data for user {user_id}")
                return
                
            current_wallet_balance_decimal = Decimal(str(fresh_user.wallet_balance))
            current_total_margin_decimal = Decimal(str(fresh_user.margin))
            
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Fresh user data loaded: wallet_balance={current_wallet_balance_decimal}, current_margin={current_total_margin_decimal}")
        except Exception as user_refresh_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error refreshing user data: {str(user_refresh_error)}", exc_info=True)
            # Fallback to original user data
            current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
            current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Using fallback user data: wallet_balance={current_wallet_balance_decimal}, current_margin={current_total_margin_decimal}")

        # FIX: Only update the margin if there's a real change needed
        # For perfect hedging, if margin is zero, we don't need to update the user's overall margin at all
        if margin > Decimal('0'):
            new_total_margin = current_total_margin_decimal + margin
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Margin check: current_margin={current_total_margin_decimal}, new_margin={new_total_margin}, wallet_balance={current_wallet_balance_decimal}")
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Using margin with hedging applied: {margin}. Adding to current total margin: {current_total_margin_decimal}")
            
            if new_total_margin > current_wallet_balance_decimal:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} for user {user_id} canceled due to insufficient margin. Required: {new_total_margin}, Available: {current_wallet_balance_decimal}")
                db_order.order_status = 'CANCELLED'
                db_order.cancel_message = "InsufficientFreeMargin"
                try:
                    await db.commit()
                    await db.refresh(db_order)
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} status updated to CANCELLED in database")
                except Exception as commit_error:
                    orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error committing order cancellation to database: {str(commit_error)}", exc_info=True)
                return
        else:
            # Perfect hedging case - no need to update the user's margin
            new_total_margin = current_total_margin_decimal
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Perfect hedging detected: order {order_id} requires no additional margin. Keeping current margin of {current_total_margin_decimal}")

        try:
            # Update user's margin with the new total that includes hedging adjustments
            # Only update margin in the database if there's a change to avoid unnecessary DB operations
            if new_total_margin != current_total_margin_decimal:
                await update_user_margin(
                    db,
                    user_id,
                    user_type,
                    new_total_margin
                )
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] User margin updated successfully for user {user_id}: {new_total_margin} (includes hedging adjustments)")
            else:
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] No margin update needed for user {user_id} - margin remains at {current_total_margin_decimal}")
            
            # Always refresh user data in the cache, even for a perfect hedge
            try:
                # Get updated user data from the database
                user_model = User if user_type == 'live' else DemoUser
                updated_user = await db.execute(select(user_model).filter(user_model.id == user_id))
                updated_user = updated_user.scalars().first()
                
                if updated_user:
                    # Update the user data cache with fresh data from database
                    user_data_to_cache = {
                        "id": updated_user.id,
                        "email": getattr(updated_user, 'email', None),
                        "group_name": updated_user.group_name,
                        "leverage": updated_user.leverage,
                        "user_type": user_type,
                        "account_number": getattr(updated_user, 'account_number', None),
                        "wallet_balance": updated_user.wallet_balance,
                        "margin": updated_user.margin,  # This contains the new margin value
                        "first_name": getattr(updated_user, 'first_name', None),
                        "last_name": getattr(updated_user, 'last_name', None),
                        "country": getattr(updated_user, 'country', None),
                        "phone_number": getattr(updated_user, 'phone_number', None),
                    }
                    await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated user data cache with new margin value: {updated_user.margin}")
            except Exception as cache_error:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating user data cache: {str(cache_error)}", exc_info=True)
            
            # Publish user data update notification to WebSocket clients using the existing cache function
            await publish_user_data_update(redis_client, user_id)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Published user data update notification for user {user_id}")
        except Exception as margin_update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating user margin: {str(margin_update_error)}", exc_info=True)
            return
        
        db_order.order_status = 'OPEN'
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Pending order {order_id} for user {user_id} opened successfully. New total margin: {new_total_margin}")

        # Commit DB changes for the order status and updated fields
        try:
            await db.commit()
            await db.refresh(db_order)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} status updated to OPEN in database")
        except Exception as commit_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error committing order status change to database: {str(commit_error)}", exc_info=True)
            return

        try:
            # --- Portfolio Update & Websocket Event ---
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Starting portfolio update for user {user_id}")
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id, db, user_type) # Re-fetch updated user data
            if user_data_for_portfolio:
                open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = []
                
                # Convert order objects to dictionaries safely
                for o in open_orders:
                    if hasattr(o, 'to_dict'):
                        open_positions_dicts.append(o.to_dict())
                    else:
                        # Create a dictionary manually if to_dict method is not available
                        order_dict = {
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', '0.0'),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        }
                        open_positions_dicts.append(order_dict)
                
                # Fetch current prices for all open positions to calculate portfolio correctly
                adjusted_market_prices = {}
                if group_symbol_settings: # Ensure group_symbol_settings is not None
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Fetched market prices for {len(adjusted_market_prices)} symbols")
                
                portfolio = await calculate_user_portfolio(user_data_for_portfolio, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Portfolio cache updated and websocket event published for user {user_id}")
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating portfolio cache or publishing websocket event: {str(e)}", exc_info=True)
            # Continue execution - don't return here as the order is already opened

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Successfully processed triggered pending order {order_id} for user {user_id}. Status set to {db_order.order_status}. Order Type changed from {order_type_original} to {new_order_type}.")
        
        # Remove the order from Redis pending list AFTER successful processing
        # Use the original_order_type for removal as that's how it's stored in Redis
        try:
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Successfully removed order {order_id} from Redis pending orders")
            
            # Notify clients about order execution through websockets
            await publish_order_execution_notification(redis_client, user_id, order_id, symbol, new_order_type, exec_price)
        except Exception as remove_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error removing order from Redis: {str(remove_error)}", exc_info=True)

    except Exception as e:
        orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Critical error in trigger_pending_order for order {order.get('order_id', 'N/A')}: {str(e)}", exc_info=True)
        raise

# New function to publish order execution notification
async def publish_order_execution_notification(redis_client: Redis, user_id: str, order_id: str, symbol: str, order_type: str, execution_price: Decimal):
    """
    Publishes a notification that a pending order has been executed to the Redis pub/sub channel.
    This allows websocket clients to be notified of order execution in real-time.
    """
    try:
        from app.core.logging_config import orders_logger
        
        # Create the notification payload
        notification = {
            "event": "pending_order_executed",
            "user_id": user_id,
            "order_id": order_id,
            "symbol": symbol,
            "order_type": order_type,
            "execution_price": str(execution_price),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to the user's channel
        channel = f"user:{user_id}:notifications"
        await redis_client.publish(channel, json.dumps(notification))
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Published order execution notification for order {order_id} to channel {channel}")
    
    except Exception as e:
        from app.core.logging_config import orders_logger
        orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error publishing order execution notification: {str(e)}", exc_info=True)

async def check_and_trigger_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis
) -> None:
    """
    Continuously checks all open orders to see if stop loss or take profit conditions are met.
    Also checks for pending orders to be triggered based on price conditions.
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
            
            # Process each pending order key
            for key in pending_keys:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = key  # Already a string
                
                # Get all user IDs for this key
                user_ids = await redis_client.hkeys(key_str)
                logger.info(f"Key: {key_str}, Users: {len(user_ids)}")
                
                # For each user, process their pending orders
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
                        
                        # Process each order
                        for order in orders:
                            order_id = order.get('order_id', 'unknown')
                            order_type = order.get('order_type', 'unknown')
                            user_type = order.get('user_type', 'unknown')
                            symbol = order.get('order_company_name', 'unknown')
                            price = order.get('order_price', 'unknown')
                            logger.info(f"    Order {order_id}: {symbol} {order_type}, price={price}, user_type={user_type}")
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
                # Process the order for SL/TP
                await process_order_stoploss_takeprofit(db, redis_client, order, 'live')
            except Exception as e:
                logger.error(f"Error processing live order {order.order_id} for SL/TP: {e}", exc_info=True)
                continue
        
        # Process demo user orders
        for order in demo_orders:
            try:
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
    If conditions are met, close the order using robust margin/wallet logic.
    Enhanced: Adds detailed logging and aligns trigger logic with pending order epsilon/normalization.
    """
    try:
        symbol = order.order_company_name
        order_type = order.order_type
        stop_loss = order.stop_loss
        take_profit = order.take_profit
        user_id = order.order_user_id
        group_name = getattr(order, 'group_name', None)
        order_id = getattr(order, 'order_id', None)
        logger.info(f"[SLTP_CHECK] Checking order {order_id}: symbol={symbol}, type={order_type}, stop_loss={stop_loss}, take_profit={take_profit}, user_id={user_id}")
        if not stop_loss and not take_profit:
            logger.info(f"[SLTP_CHECK] Order {order_id} has no SL/TP set. Skipping.")
            return
        # Get adjusted prices from cache
        if not group_name:
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            if user_type == 'live':
                user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, user_id)
            group_name = getattr(user, 'group_name', None)
        adjusted_prices = None
        if group_name:
            from app.core.cache import get_adjusted_market_price_cache
            adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        if not adjusted_prices:
            current_prices = await get_last_known_price(redis_client, symbol)
            if not current_prices:
                logger.warning(f"[SLTP_CHECK] No price found for {symbol} when checking SL/TP for order {order_id}")
                return
            adjusted_buy_price = Decimal(str(current_prices.get('b', '0')))
            adjusted_sell_price = Decimal(str(current_prices.get('a', '0')))
        else:
            adjusted_buy_price = Decimal(str(adjusted_prices.get('buy', '0')))
            adjusted_sell_price = Decimal(str(adjusted_prices.get('sell', '0')))
        if adjusted_buy_price <= 0 or adjusted_sell_price <= 0:
            logger.warning(f"[SLTP_CHECK] Invalid adjusted prices for {symbol}: buy={adjusted_buy_price}, sell={adjusted_sell_price}")
            return
        # --- Normalize prices and use epsilon as in pending order logic ---
        def normalize(val):
            val = Decimal(str(val))
            return Decimal(str(round(val, 5)))
        adjusted_buy_price_n = normalize(adjusted_buy_price)
        adjusted_sell_price_n = normalize(adjusted_sell_price)
        stop_loss_n = normalize(stop_loss) if stop_loss else None
        take_profit_n = normalize(take_profit) if take_profit else None
        epsilon = Decimal('0.00001')
        # Add debug logs for types and values
        logger.info(f"[SLTP_DEBUG] Types: adjusted_sell_price_n={type(adjusted_sell_price_n)}, stop_loss_n={type(stop_loss_n)}, adjusted_buy_price_n={type(adjusted_buy_price_n)}, take_profit_n={type(take_profit_n)}")
        logger.info(f"[SLTP_DEBUG] Values: adjusted_sell_price_n={adjusted_sell_price_n}, stop_loss_n={stop_loss_n}, adjusted_buy_price_n={adjusted_buy_price_n}, take_profit_n={take_profit_n}")
        # SL/TP trigger logic with epsilon
        sl_triggered = False
        tp_triggered = False
        # Stoploss logic
        if stop_loss_n is not None:
            if order_type == "BUY":
                price_diff = abs(adjusted_sell_price_n - stop_loss_n)
                logger.info(f"[SLTP_DEBUG] BUY SL: adjusted_sell_price_n={adjusted_sell_price_n}, stop_loss_n={stop_loss_n}, price_diff={price_diff}, epsilon={epsilon}")
                sl_triggered = (adjusted_sell_price_n <= stop_loss_n) or (price_diff < epsilon)
                logger.info(f"[SLTP_DEBUG] BUY SL triggered: (adjusted_sell_price_n <= stop_loss_n)={adjusted_sell_price_n <= stop_loss_n}, (price_diff < epsilon)={price_diff < epsilon}, sl_triggered={sl_triggered}")
            elif order_type == "SELL":
                price_diff = abs(adjusted_buy_price_n - stop_loss_n)
                logger.info(f"[SLTP_DEBUG] SELL SL: adjusted_buy_price_n={adjusted_buy_price_n}, stop_loss_n={stop_loss_n}, price_diff={price_diff}, epsilon={epsilon}")
                sl_triggered = (adjusted_buy_price_n >= stop_loss_n) or (price_diff < epsilon)
                logger.info(f"[SLTP_DEBUG] SELL SL triggered: (adjusted_buy_price_n >= stop_loss_n)={adjusted_buy_price_n >= stop_loss_n}, (price_diff < epsilon)={price_diff < epsilon}, sl_triggered={sl_triggered}")
        # Takeprofit logic (only if SL not triggered)
        if take_profit_n is not None and not sl_triggered:
            if order_type == "BUY":
                price_diff = abs(adjusted_sell_price_n - take_profit_n)
                logger.info(f"[SLTP_DEBUG] BUY TP: adjusted_sell_price_n={adjusted_sell_price_n}, take_profit_n={take_profit_n}, price_diff={price_diff}, epsilon={epsilon}")
                tp_triggered = (adjusted_sell_price_n >= take_profit_n) or (price_diff < epsilon)
                logger.info(f"[SLTP_DEBUG] BUY TP triggered: (adjusted_sell_price_n >= take_profit_n)={adjusted_sell_price_n >= take_profit_n}, (price_diff < epsilon)={price_diff < epsilon}, tp_triggered={tp_triggered}")
            elif order_type == "SELL":
                price_diff = abs(adjusted_buy_price_n - take_profit_n)
                logger.info(f"[SLTP_DEBUG] SELL TP: adjusted_buy_price_n={adjusted_buy_price_n}, take_profit_n={take_profit_n}, price_diff={price_diff}, epsilon={epsilon}")
                tp_triggered = (adjusted_buy_price_n <= take_profit_n) or (price_diff < epsilon)
                logger.info(f"[SLTP_DEBUG] SELL TP triggered: (adjusted_buy_price_n <= take_profit_n)={adjusted_buy_price_n <= take_profit_n}, (price_diff < epsilon)={price_diff < epsilon}, tp_triggered={tp_triggered}")
        logger.info(f"[SLTP_CHECK] Order {order_id} SL triggered: {sl_triggered}, TP triggered: {tp_triggered}")
        if not (sl_triggered or tp_triggered):
            logger.info(f"[SLTP_CHECK] No SL/TP trigger for order {order_id}. Skipping.")
            return
        trigger_type = "stop_loss" if sl_triggered else "take_profit"
        logger.info(f"[SLTP_TRIGGER] Triggering {trigger_type} for order {order_id} (user {user_id}, symbol {symbol}, type {order_type})")
        # --- Continue with robust close logic as before ---
        # Lock user
        if user_type == 'live':
            db_user_locked = await get_user_by_id_with_lock(db, user_id)
        else:
            db_user_locked = await get_demo_user_by_id_with_lock(db, user_id)
        if db_user_locked is None:
            logger.error(f"[SLTP_TRIGGER] Could not retrieve and lock user record for user ID: {user_id}")
            return
        # Get all open orders for this symbol
        from app.crud.crud_order import get_order_model, get_open_orders_by_user_id_and_symbol, update_order_with_tracking
        order_model_class = get_order_model(user_type)
        all_open_orders_for_symbol = await get_open_orders_by_user_id_and_symbol(
            db=db, user_id=db_user_locked.id, symbol=symbol, order_model=order_model_class
        )
        margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=symbol,
            open_positions_for_symbol=all_open_orders_for_symbol,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_before_recalc = margin_before_recalc_dict["total_margin"]
        current_overall_margin = Decimal(str(db_user_locked.margin))
        non_symbol_margin = current_overall_margin - margin_before_recalc
        remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order.order_id]
        margin_after_symbol_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=symbol,
            open_positions_for_symbol=remaining_orders_for_symbol_after_close,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_after_symbol_recalc = margin_after_symbol_recalc_dict["total_margin"]
        db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        # Get contract size and profit currency
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()
        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
            logger.error(f"[SLTP_TRIGGER] Missing critical ExternalSymbolInfo for symbol {symbol}.")
            return
        contract_size = Decimal(str(ext_symbol_info.contract_size))
        profit_currency = ext_symbol_info.profit.upper()
        # Get group settings for commission
        from app.core.cache import get_group_symbol_settings_cache
        group_settings = await get_group_symbol_settings_cache(redis_client, db_user_locked.group_name, symbol)
        if not group_settings:
            logger.error("[SLTP_TRIGGER] Group settings not found for commission calculation.")
            return
        commission_type = int(group_settings.get('commision_type', -1))
        commission_value_type = int(group_settings.get('commision_value_type', -1))
        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
        existing_entry_commission = Decimal(str(order.commission or "0.0"))
        exit_commission = Decimal("0.0")
        quantity = Decimal(str(order.order_quantity))
        close_price = adjusted_sell_price_n if order_type == "BUY" else adjusted_buy_price_n
        entry_price = Decimal(str(order.order_price))
        if commission_type in [0, 2]:
            if commission_value_type == 0:
                exit_commission = quantity * commission_rate
            elif commission_value_type == 1:
                calculated_exit_contract_value = quantity * contract_size * close_price
                if calculated_exit_contract_value > Decimal("0.0"):
                    exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
        total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if order_type == "BUY":
            profit = (close_price - entry_price) * quantity * contract_size
        elif order_type == "SELL":
            profit = (entry_price - close_price) * quantity * contract_size
        else:
            logger.error("[SLTP_TRIGGER] Invalid order type.")
            return
        profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, order.order_id, "PnL on Close", db=db, redis_client=redis_client)
        if profit_currency != "USD" and profit_usd == profit:
            logger.error(f"[SLTP_TRIGGER] Order {order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
            return
        from app.services.order_processing import generate_unique_10_digit_id
        close_id = await generate_unique_10_digit_id(db, order_model_class, 'close_id')
        order.order_status = "CLOSED"
        order.close_price = close_price
        order.net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        order.swap = order.swap or Decimal("0.0")
        order.commission = total_commission_for_trade
        order.close_id = close_id
        order.close_message = f"Closed automatically due to {trigger_type}"
        await update_order_with_tracking(
            db=db,
            db_order=order,
            update_fields={
                "order_status": order.order_status,
                "close_price": order.close_price,
                "close_id": order.close_id,
                "net_profit": order.net_profit,
                "swap": order.swap,
                "commission": order.commission,
                "close_message": order.close_message
            },
            user_id=db_user_locked.id,
            user_type=user_type,
            action_type=f"AUTO_{trigger_type.upper()}_CLOSE"
        )
        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
        swap_amount = order.swap
        db_user_locked.wallet_balance = (original_wallet_balance + order.net_profit - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        transaction_time = datetime.now(timezone.utc)
        wallet_common_data = {"symbol": symbol, "order_quantity": quantity, "is_approved": 1, "order_type": order.order_type, "transaction_time": transaction_time, "order_id": order.order_id}
        if user_type == 'demo':
            wallet_common_data["demo_user_id"] = db_user_locked.id
        else:
            wallet_common_data["user_id"] = db_user_locked.id
        if order.net_profit != Decimal("0.0"):
            transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=order.net_profit, description=f"P/L for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
        if total_commission_for_trade > Decimal("0.0"):
            transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
        if swap_amount != Decimal("0.0"):
            transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))
        await db.commit()
        await db.refresh(order)
        await db.refresh(db_user_locked)
        user_type_str = 'demo' if user_type == 'demo' else 'live'
        user_data_to_cache = {
            "id": db_user_locked.id,
            "email": getattr(db_user_locked, 'email', None),
            "group_name": db_user_locked.group_name,
            "leverage": db_user_locked.leverage,
            "user_type": user_type_str,
            "account_number": getattr(db_user_locked, 'account_number', None),
            "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin,
            "first_name": getattr(db_user_locked, 'first_name', None),
            "last_name": getattr(db_user_locked, 'last_name', None),
            "country": getattr(db_user_locked, 'country', None),
            "phone_number": getattr(db_user_locked, 'phone_number', None),
        }
        await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)
        from app.api.v1.endpoints.orders import update_user_static_orders, publish_order_update, publish_user_data_update, publish_market_data_trigger
        await update_user_static_orders(db_user_locked.id, db, redis_client, user_type_str)
        await publish_order_update(redis_client, db_user_locked.id)
        await publish_user_data_update(redis_client, db_user_locked.id)
        await publish_market_data_trigger(redis_client)
        logger.info(f"[SLTP_TRIGGER] Successfully closed order {order.order_id} due to {trigger_type} trigger with robust margin/wallet logic.")
    except Exception as e:
        logger.error(f"[SLTP_TRIGGER] Error processing SL/TP for order {getattr(order, 'order_id', 'N/A')}: {e}", exc_info=True)

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

# async def process_pending_order(
#     db: AsyncSession,
#     redis_client: Redis,
#     user_id: int,
#     order_data: Dict[str, Any],
#     user_type: str
# ) -> Dict[str, Any]:
#     """
#     Process a pending order by correctly calculating the margin before and after
#     the order, updating the user's total margin with locking.
    
#     This function:
#     1. Calculates the individual order margin using calculate_single_order_margin
#     2. Gets all open orders for the symbol
#     3. Calculates total margin contribution before adding new order
#     4. Adds the new order (simulated) and recalculates total margin
#     5. Determines additional margin needed by finding the difference
#     6. Updates the user's total margin with proper locking
    
#     Args:
#         db: Database session
#         redis_client: Redis client
#         user_id: User ID
#         order_data: Order data dictionary
#         user_type: User type ('live' or 'demo')
        
#     Returns:
#         Dictionary with the processed order data
#     """
#     try:
#         orders_logger.info(f"[PENDING_ORDER] Processing pending order for user {user_id}, symbol {order_data.get('order_company_name')}")
        
#         # Step 1: Load user data and settings
#         user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
#         if not user_data:
#             orders_logger.error(f"[PENDING_ORDER] User data not found for user {user_id}")
#             raise OrderProcessingError("User data not found")

#         symbol = order_data.get('order_company_name', '').upper()
#         order_type = order_data.get('order_type', '').upper()
#         quantity = Decimal(str(order_data.get('order_quantity', '0.0')))
#         group_name = user_data.get('group_name')
#         leverage = Decimal(str(user_data.get('leverage', '1.0')))

#         orders_logger.info(f"[PENDING_ORDER] Order details - Symbol: {symbol}, Type: {order_type}, Quantity: {quantity}, Group: {group_name}, Leverage: {leverage}")
        
#         # Step 2: Get settings and market data
#         group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
#         if not group_settings:
#             orders_logger.error(f"[PENDING_ORDER] Group settings not found for symbol {symbol}")
#             raise OrderProcessingError(f"Group settings not found for symbol {symbol}")
            
#         external_symbol_info = await get_external_symbol_info(db, symbol)
#         if not external_symbol_info:
#             orders_logger.error(f"[PENDING_ORDER] External symbol info not found for {symbol}")
#             raise OrderProcessingError(f"External symbol info not found for {symbol}")
            
#         raw_market_data = await get_latest_market_data()
#         if not raw_market_data:
#             orders_logger.error("[PENDING_ORDER] Failed to get market data")
#             raise OrderProcessingError("Failed to get market data")
        
#         # Step 3: Calculate individual order margin using calculate_single_order_margin
#         orders_logger.info(f"[PENDING_ORDER] Calculating individual order margin for {symbol}")
#         full_margin_usd, price, contract_value, commission = await calculate_single_order_margin(
#             redis_client=redis_client,
#             symbol=symbol,
#             order_type=order_type,
#             quantity=quantity,
#             user_leverage=leverage,
#             group_settings=group_settings,
#             external_symbol_info=external_symbol_info,
#             raw_market_data=raw_market_data,
#             db=db,
#             user_id=user_id
#         )
        
#         if full_margin_usd is None:
#             orders_logger.error(f"[PENDING_ORDER] Margin calculation failed for {symbol}")
#             raise OrderProcessingError("Margin calculation failed")
            
#         orders_logger.info(f"[PENDING_ORDER] Individual margin: {full_margin_usd}, Price: {price}, Contract Value: {contract_value}, Commission: {commission}")
        
#         # Step 4: Get the order model based on user type
#         order_model = get_order_model(user_type)
        
#         # Step 5: Calculate total symbol margin contribution before adding new order
#         open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
#             db, user_id, symbol, order_model
#         )
        
#         orders_logger.info(f"[PENDING_ORDER] Found {len(open_orders_for_symbol)} existing open orders for symbol {symbol}")
        
#         # Ensure we're calling this with the correct parameters
#         margin_before_data = await calculate_total_symbol_margin_contribution(
#             db, redis_client, user_id, symbol, open_orders_for_symbol, order_model, user_type
#         )
#         margin_before = margin_before_data["total_margin"]
        
#         orders_logger.info(f"[PENDING_ORDER] Total margin contribution before: {margin_before}")
        
#         # Step 6: Create a simulated order object to include in the calculation
#         # Make sure object properties match exactly what calculate_total_symbol_margin_contribution expects
#         simulated_order = type('Obj', (object,), {
#             'order_quantity': quantity,
#             'order_type': order_type,
#             'margin': full_margin_usd,
#             'id': None,  # Add id attribute to match real orders
#             'order_id': 'NEW_PENDING'  # Add order_id attribute for logging
#         })()
        
#         # Step 7: Calculate total symbol margin contribution after adding new order
#         margin_after_data = await calculate_total_symbol_margin_contribution(
#             db, redis_client, user_id, symbol, 
#             open_orders_for_symbol + [simulated_order],
#             order_model, user_type
#         )
#         margin_after = margin_after_data["total_margin"]
        
#         orders_logger.info(f"[PENDING_ORDER] Total margin contribution after: {margin_after}")
        
#         # Step 8: Calculate additional margin required
#         additional_margin = max(Decimal("0.0"), margin_after - margin_before)
#         orders_logger.info(f"[PENDING_ORDER] Additional margin required: {additional_margin}, calculated as margin_after ({margin_after}) - margin_before ({margin_before})")
        
#         # Log detailed information about the margin calculation
#         orders_logger.info(f"[PENDING_ORDER_DEBUG] Full margin for this order: {full_margin_usd}")
#         orders_logger.info(f"[PENDING_ORDER_DEBUG] Margin before adding order: {margin_before}")
#         orders_logger.info(f"[PENDING_ORDER_DEBUG] Margin after adding order: {margin_after}")
#         orders_logger.info(f"[PENDING_ORDER_DEBUG] Additional margin needed: {additional_margin}")
        
#         # Debug: Compare implementations and parameters between the two different margin calculation functions
#         await debug_margin_calculations(
#             db=db,
#             redis_client=redis_client,
#             user_id=user_id,
#             symbol=symbol,
#             open_orders=open_orders_for_symbol,
#             simulated_order=simulated_order,
#             user_type=user_type
#         )
        
#         # Step 9: Lock user and update margin
#         if user_type == 'demo':
#             db_user_locked = await get_demo_user_by_id_with_lock(db, user_id)
#         else:
#             db_user_locked = await get_user_by_id_with_lock(db, user_id)
            
#         if db_user_locked is None:
#             orders_logger.error(f"[PENDING_ORDER] Could not lock user record for user {user_id}")
#             raise OrderProcessingError("Could not lock user record")
            
#         # Check if user has enough balance to cover the margin
#         if db_user_locked.wallet_balance < db_user_locked.margin + additional_margin:
#             orders_logger.error(f"[PENDING_ORDER] Insufficient funds for user {user_id}. Wallet: {db_user_locked.wallet_balance}, Current Margin: {db_user_locked.margin}, Additional Required: {additional_margin}")
#             raise InsufficientFundsError("Not enough wallet balance to cover additional margin")
            
#         # Update user margin with additional margin
#         original_user_margin = db_user_locked.margin
#         db_user_locked.margin = (Decimal(str(db_user_locked.margin)) + additional_margin).quantize(
#             Decimal("0.01"), rounding=ROUND_HALF_UP
#         )
        
#         orders_logger.info(f"[PENDING_ORDER] Updating user margin: {original_user_margin} + {additional_margin} = {db_user_locked.margin}")
        
#         # Step 10: Save changes to database
#         db.add(db_user_locked)
#         await db.commit()
#         await db.refresh(db_user_locked)
        
#         # Step 11: Update user data cache with full user information
#         user_data_to_cache = {
#             "id": db_user_locked.id,
#             "email": getattr(db_user_locked, 'email', None),
#             "wallet_balance": db_user_locked.wallet_balance,
#             "leverage": db_user_locked.leverage,
#             "group_name": db_user_locked.group_name,
#             "margin": db_user_locked.margin,
#             "user_type": user_type,
#             "account_number": getattr(db_user_locked, 'account_number', None),
#             "first_name": getattr(db_user_locked, 'first_name', None),
#             "last_name": getattr(db_user_locked, 'last_name', None),
#             "country": getattr(db_user_locked, 'country', None),
#             "phone_number": getattr(db_user_locked, 'phone_number', None),
#         }
#         await set_user_data_cache(redis_client, user_id, user_data_to_cache)
        
#         # Publish user data update notification
#         try:
#             from app.core.cache import publish_user_data_update
#             await publish_user_data_update(redis_client, user_id)
#             orders_logger.info(f"[PENDING_ORDER] Published user data update for user {user_id}")
#         except Exception as e:
#             orders_logger.error(f"[PENDING_ORDER] Failed to publish user data update: {str(e)}")
        
#         # Step 12: Generate unique IDs for SL/TP if needed
#         stoploss_id = None
#         if order_data.get('stop_loss') is not None:
#             stoploss_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_id')
            
#         takeprofit_id = None
#         if order_data.get('take_profit') is not None:
#             takeprofit_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_id')
            
#         # Step 13: Return the processed order data
#         return {
#             'order_id': await generate_unique_10_digit_id(db, order_model, 'order_id'),
#             'order_status': order_data.get('order_status', 'PENDING'),
#             'order_user_id': user_id,
#             'order_company_name': symbol,
#             'order_type': order_type,
#             'order_price': price,
#             'order_quantity': quantity,
#             'contract_value': contract_value,
#             'margin': full_margin_usd,
#             'commission': commission,
#             'stop_loss': order_data.get('stop_loss'),
#             'take_profit': order_data.get('take_profit'),
#             'stoploss_id': stoploss_id,
#             'takeprofit_id': takeprofit_id,
#             'status': order_data.get('status', 'PENDING'),
#         }
    
#     except InsufficientFundsError as e:
#         orders_logger.error(f"[PENDING_ORDER] Insufficient funds error: {str(e)}")
#         raise
#     except OrderProcessingError as e:
#         orders_logger.error(f"[PENDING_ORDER] Order processing error: {str(e)}")
#         raise
#     except Exception as e:
#         orders_logger.error(f"[PENDING_ORDER] Unexpected error: {str(e)}", exc_info=True)
#         raise OrderProcessingError(f"Failed to process pending order: {str(e)}")

        