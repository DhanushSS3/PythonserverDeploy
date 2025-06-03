from typing import Dict, List, Optional, Any
from decimal import Decimal
from redis.asyncio import Redis
import logging
from datetime import datetime
import json

from app.core.cache import (
    get_live_adjusted_buy_price_for_pair,
    get_user_data_cache,
    get_group_settings_cache,
    set_user_data_cache,
    set_user_portfolio_cache,
    publish_account_structure_changed_event
)
from app.services.margin_calculator import calculate_single_order_margin
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
from app.crud import crud_order
from app.crud.crud_order import get_order_model

logger = logging.getLogger(__name__)

# Structure to store pending orders (only for non-Barclays users)
# {
#   "symbol": {
#     "BUY_LIMIT": {
#       "user_id": [order1, order2],
#       "user_id2": [order3, order4]
#     },
#     "SELL_LIMIT": {
#       "user_id": [order5, order6],
#       "user_id2": [order7, order8]
#     },
#     "BUY_STOP": {
#       "user_id": [order9, order10],
#       "user_id2": [order11, order12]
#     },
#     "SELL_STOP": {
#       "user_id": [order13, order14],
#       "user_id2": [order15, order16]
#     }
#   }
# }
pending_orders: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}

async def add_pending_order(order: Dict[str, Any]) -> None:
    """Add a new pending order to the tracking structure (only for non-Barclays users)"""
    # Check if this is a Barclays user
    user_data = await get_user_data_cache(order['order_user_id'])
    if not user_data:
        logger.error(f"User data not found for user {order['order_user_id']}")
        return

    group_settings = await get_group_settings_cache(user_data.get('group_name'))
    if not group_settings:
        logger.error(f"Group settings not found for user {order['order_user_id']}")
        return

    # Skip Redis cache for Barclays users
    if order['user_type'] == 'live' and group_settings.get('sending_orders') == 'barclays':
        logger.info(f"Skipping Redis cache for Barclays pending order {order['order_id']}")
        return

    symbol = order['order_company_name']
    order_type = order['order_type']
    user_id = str(order['order_user_id'])
    
    if symbol not in pending_orders:
        pending_orders[symbol] = {
            'BUY_LIMIT': {},
            'SELL_LIMIT': {},
            'BUY_STOP': {},
            'SELL_STOP': {}
        }
    
    if user_id not in pending_orders[symbol][order_type]:
        pending_orders[symbol][order_type][user_id] = []
    
    pending_orders[symbol][order_type][user_id].append(order)
    logger.info(f"Added pending order {order['order_id']} for user {user_id}, {symbol} {order_type}")

async def remove_pending_order(order_id: str, symbol: str, order_type: str, user_id: str) -> None:
    """Remove a pending order from the tracking structure"""
    if (symbol in pending_orders and 
        order_type in pending_orders[symbol] and 
        user_id in pending_orders[symbol][order_type]):
        
        pending_orders[symbol][order_type][user_id] = [
            order for order in pending_orders[symbol][order_type][user_id]
            if order['order_id'] != order_id
        ]
        
        # Clean up empty lists
        if not pending_orders[symbol][order_type][user_id]:
            del pending_orders[symbol][order_type][user_id]
            if not pending_orders[symbol][order_type]:
                del pending_orders[symbol][order_type]
                if not pending_orders[symbol]:
                    del pending_orders[symbol]
        
        logger.info(f"Removed pending order {order_id} for user {user_id}, {symbol} {order_type}")

async def check_and_trigger_pending_orders(
    db,
    redis_client: Redis,
    symbol: str,
    current_price: Decimal
) -> None:
    """Check and trigger pending orders based on current market price (only for non-Barclays users)"""
    if symbol not in pending_orders:
        return

    # Check BUY_LIMIT orders (trigger when price falls to or below limit price)
    if 'BUY_LIMIT' in pending_orders[symbol]:
        for user_id, orders in pending_orders[symbol]['BUY_LIMIT'].items():
            for order in orders[:]:  # Copy list to allow modification during iteration
                if current_price <= Decimal(str(order['order_price'])):
                    await trigger_pending_order(db, redis_client, order, current_price)
                    await remove_pending_order(order['order_id'], symbol, 'BUY_LIMIT', user_id)

    # Check SELL_LIMIT orders (trigger when price rises to or above limit price)
    if 'SELL_LIMIT' in pending_orders[symbol]:
        for user_id, orders in pending_orders[symbol]['SELL_LIMIT'].items():
            for order in orders[:]:
                if current_price >= Decimal(str(order['order_price'])):
                    await trigger_pending_order(db, redis_client, order, current_price)
                    await remove_pending_order(order['order_id'], symbol, 'SELL_LIMIT', user_id)

    # Check BUY_STOP orders (trigger when price rises to or above stop price)
    if 'BUY_STOP' in pending_orders[symbol]:
        for user_id, orders in pending_orders[symbol]['BUY_STOP'].items():
            for order in orders[:]:
                if current_price >= Decimal(str(order['order_price'])):
                    await trigger_pending_order(db, redis_client, order, current_price)
                    await remove_pending_order(order['order_id'], symbol, 'BUY_STOP', user_id)

    # Check SELL_STOP orders (trigger when price falls to or below stop price)
    if 'SELL_STOP' in pending_orders[symbol]:
        for user_id, orders in pending_orders[symbol]['SELL_STOP'].items():
            for order in orders[:]:
                if current_price <= Decimal(str(order['order_price'])):
                    await trigger_pending_order(db, redis_client, order, current_price)
                    await remove_pending_order(order['order_id'], symbol, 'SELL_STOP', user_id)

async def trigger_pending_order(
    db,
    redis_client: Redis,
    order: Dict[str, Any],
    current_price: Decimal
) -> None:
    """Trigger a pending order (only for non-Barclays users)"""
    try:
        user_id = order['order_user_id']
        user_type = order['user_type']
        
        # Get user data and group settings
        user_data = await get_user_data_cache(user_id)
        if not user_data:
            logger.error(f"User data not found for user {user_id}")
            return

        group_settings = await get_group_settings_cache(user_data.get('group_name'))
        if not group_settings:
            logger.error(f"Group settings not found for user {user_id}")
            return

        # Skip if this is a Barclays user
        if user_type == 'live' and group_settings.get('sending_orders') == 'barclays':
            logger.info(f"Skipping trigger for Barclays pending order {order['order_id']}")
            return

        # Calculate margin for the order
        margin, adjusted_price, contract_value = await calculate_single_order_margin(
            db=db,
            redis_client=redis_client,
            user_id=user_id,
            order_quantity=Decimal(str(order['order_quantity'])),
            order_price=current_price,
            symbol=order['order_company_name'],
            order_type=order['order_type']
        )

        # Check if user has enough margin
        if Decimal(str(user_data.get('margin', 0))) + margin > Decimal(str(user_data.get('wallet_balance', 0))):
            logger.warning(f"Insufficient margin for user {user_id} to trigger pending order {order['order_id']}")
            return

        # Update order in database
        order_model = get_order_model(User if user_type == 'live' else DemoUser)
        db_order = await crud_order.get_order_by_id(db, order['order_id'], order_model)
        if not db_order:
            logger.error(f"Order {order['order_id']} not found in database")
            return

        db_order.order_status = 'OPEN'
        db_order.order_price = current_price
        db_order.margin = margin
        db_order.contract_value = contract_value

        # Update user margin
        user_data['margin'] = str(Decimal(str(user_data.get('margin', 0))) + margin)
        await set_user_data_cache(redis_client, user_id, user_data)

        # Update user portfolio cache
        user_portfolio = await get_user_portfolio_cache(redis_client, user_id)
        if user_portfolio:
            # Move order from pending_positions to positions
            pending_positions = user_portfolio.get('pending_positions', [])
            positions = user_portfolio.get('positions', [])
            
            # Find and remove the order from pending_positions
            for i, pos in enumerate(pending_positions):
                if pos['order_id'] == order['order_id']:
                    pos['order_status'] = 'OPEN'
                    pos['margin'] = str(margin)
                    positions.append(pos)
                    pending_positions.pop(i)
                    break
            
            user_portfolio['positions'] = positions
            user_portfolio['pending_positions'] = pending_positions
            user_portfolio['margin'] = str(Decimal(str(user_data.get('margin', 0))))
            user_portfolio['free_margin'] = str(Decimal(str(user_data.get('wallet_balance', 0))) - Decimal(str(user_data.get('margin', 0))))
            
            await set_user_portfolio_cache(redis_client, user_id, user_portfolio)
            await publish_account_structure_changed_event(redis_client, user_id)

        await db.commit()
        await db.refresh(db_order)
        logger.info(f"Triggered pending order {order['order_id']} for user {user_id}")

    except Exception as e:
        logger.error(f"Error triggering pending order {order['order_id']}: {str(e)}")
        raise 