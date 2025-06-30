# crud_order.py
from typing import List, Optional, Type, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from decimal import Decimal
from app.database.models import UserOrder, DemoUserOrder, OrderActionHistory
from app.schemas.order import OrderCreateInternal
from sqlalchemy.orm import selectinload
from datetime import datetime
from sqlalchemy import and_, or_
from typing import Dict
import logging

orders_crud_logger = logging.getLogger('orders_crud')

# Utility to get the appropriate model class
def get_order_model(user_type: str):
    orders_crud_logger.debug(f"[get_order_model] Called with user_type: '{user_type}'")
    if user_type == "demo":
        orders_crud_logger.debug("[get_order_model] Returning DemoUserOrder")
        return DemoUserOrder
    else:
        orders_crud_logger.debug("[get_order_model] Returning UserOrder")
        return UserOrder

# Create a new order
async def create_order(db: AsyncSession, order_data: dict, order_model: Type[UserOrder | DemoUserOrder]):
    orders_logger = logging.getLogger('orders')
    orders_logger.info(f"[ENTER-CRUD] create_order called with: {order_data}")
    orders_logger.debug(f"[DEBUG][create_order] Received order_data: {order_data}")
    # Ensure 'status' is present and valid
    if 'status' not in order_data or not isinstance(order_data['status'], str) or not (0 <= len(order_data['status']) <= 30):
        orders_logger.debug(f"[DEBUG][create_order] status field value: {order_data.get('status')}")
        raise ValueError("'status' is required and must be a string of length 10-30.")
    db_order = order_model(**order_data)
    db.add(db_order)
    
    # Create a log entry in OrderActionHistory
    user_id = order_data.get('order_user_id')
    user_type = 'demo' if order_model.__name__ == 'DemoUserOrder' else 'live'
    
    # Create history record only if we have the required fields
    if user_id and 'order_id' in order_data:
        history = OrderActionHistory(
            user_id=user_id,
            user_type=user_type,
            order_id=order_data['order_id'],
            cancel_id=order_data.get('cancel_id'),
            close_id=order_data.get('close_id'),
            action_type="CREATE",
            modify_id=order_data.get('modify_id'),
            stoploss_id=order_data.get('stoploss_id'),
            takeprofit_id=order_data.get('takeprofit_id'),
            stoploss_cancel_id=order_data.get('stoploss_cancel_id'),
            takeprofit_cancel_id=order_data.get('takeprofit_cancel_id')
        )
        db.add(history)
        orders_logger.debug(f"[DEBUG][create_order] Created OrderActionHistory record for order_id: {order_data['order_id']}")
    
    await db.commit()
    await db.refresh(db_order)
    return db_order

# Get order by order_id
async def get_order_by_id(db: AsyncSession, order_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by order_id"""
    result = await db.execute(select(order_model).filter(order_model.order_id == order_id))
    return result.scalars().first()

# Get orders for a user
async def get_orders_by_user_id(
    db: AsyncSession, user_id: int, order_model: Type[UserOrder | DemoUserOrder],
    skip: int = 0, limit: int = 100
):
    result = await db.execute(
        select(order_model)
        .filter(order_model.order_user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(order_model.created_at.desc())
    )
    orders = result.scalars().all()
    orders_crud_logger.debug(f"[get_orders_by_user_id] Retrieved {len(orders)} orders for user {user_id} using model {order_model.__name__}")
    return orders

# Get all open orders for user
async def get_all_open_orders_by_user_id(
    db: AsyncSession, user_id: int, order_model: Type[UserOrder | DemoUserOrder]
):
    orders_crud_logger.debug(f"[get_all_open_orders_by_user_id] Called for user {user_id} with order_model: {order_model.__name__}")
    result = await db.execute(
        select(order_model).filter(
            order_model.order_user_id == user_id,
            order_model.order_status == 'OPEN'
        )
    )
    orders = result.scalars().all()
    orders_crud_logger.debug(f"[get_all_open_orders_by_user_id] Retrieved {len(orders)} open orders for user {user_id} using model {order_model.__name__}")
    return orders

# Get all open orders from UserOrder table (system-wide)
async def get_all_system_open_orders(db: AsyncSession):
    result = await db.execute(
        select(UserOrder).filter(UserOrder.order_status == 'OPEN')
    )
    return result.scalars().all()

# Get all open orders for both live and demo users
async def get_all_open_orders(db: AsyncSession):
    """
    Get all open orders for both live and demo users.
    Returns a tuple of (live_orders, demo_orders).
    """
    orders_crud_logger.debug("[get_all_open_orders] Fetching all open orders for both live and demo users")
    
    try:
        # Get live user orders
        live_orders_stmt = select(UserOrder).filter(UserOrder.order_status == 'OPEN')
        live_orders_result = await db.execute(live_orders_stmt)
        live_orders = live_orders_result.scalars().all()
        orders_crud_logger.debug(f"[get_all_open_orders] Found {len(live_orders)} live open orders")
        
        # Get demo user orders
        demo_orders_stmt = select(DemoUserOrder).filter(DemoUserOrder.order_status == 'OPEN')
        demo_orders_result = await db.execute(demo_orders_stmt)
        demo_orders = demo_orders_result.scalars().all()
        orders_crud_logger.debug(f"[get_all_open_orders] Found {len(demo_orders)} demo open orders")
        
        return live_orders, demo_orders
    except Exception as e:
        orders_crud_logger.error(f"[get_all_open_orders] Error getting all open orders: {str(e)}", exc_info=True)
        return [], []

# Get open and pending orders
async def get_open_and_pending_orders_by_user_id_and_symbol(
    db: AsyncSession, user_id: int, symbol: str, order_model: Type[UserOrder | DemoUserOrder]
):
    pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
    result = await db.execute(
        select(order_model).filter(
            order_model.order_user_id == user_id,
            order_model.order_company_name == symbol,
            order_model.order_status.in_(["OPEN"] + pending_statuses)
        )
    )
    return result.scalars().all()

# Update order fields and track changes in OrderActionHistory
async def update_order_with_tracking(
    db: AsyncSession,
    db_order: UserOrder | DemoUserOrder,
    update_fields: dict,
    user_id: int,
    user_type: str,
    action_type: Optional[str] = "UPDATE"
):
    for field, value in update_fields.items():
        if hasattr(db_order, field):
            setattr(db_order, field, value)

    # Create a log entry in OrderActionHistory
    history = OrderActionHistory(
        user_id=user_id,
        user_type=user_type,
        order_id=db_order.order_id,
        cancel_id=update_fields.get("cancel_id") or getattr(db_order, 'cancel_id', None),
        close_id=update_fields.get("close_id") or getattr(db_order, 'close_id', None),
        action_type=action_type,
        modify_id=update_fields.get("modify_id"),
        stoploss_id=update_fields.get("stoploss_id"),
        takeprofit_id=update_fields.get("takeprofit_id"),
        stoploss_cancel_id=update_fields.get("stoploss_cancel_id"),
        takeprofit_cancel_id=update_fields.get("takeprofit_cancel_id"),
    )
    db.add(history)

    await db.commit()
    await db.refresh(db_order)
    return db_order


from typing import List, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database.models import UserOrder, DemoUserOrder # Make sure these imports are correct based on your models.py

async def get_orders_by_user_id_and_statuses(
    db: AsyncSession,
    user_id: int,
    statuses: List[str],
    order_model: Type[UserOrder | DemoUserOrder]
):
    """
    Retrieves orders for a given user ID with specified statuses.

    Args:
        db: The SQLAlchemy asynchronous session.
        user_id: The ID of the user whose orders are to be fetched.
        statuses: A list of strings representing the desired order statuses (e.g., ["OPEN", "PENDING", "CANCELLED", "CLOSED"]).
        order_model: The SQLAlchemy model for orders (UserOrder or DemoUserOrder).

    Returns:
        A list of order objects matching the criteria.
    """
    result = await db.execute(
        select(order_model).filter(
            order_model.order_user_id == user_id,
            order_model.order_status.in_(statuses)
        )
    )
    orders = result.scalars().all()
    orders_crud_logger.debug(f"[get_orders_by_user_id_and_statuses] Retrieved {len(orders)} orders for user {user_id} with statuses {statuses} using model {order_model.__name__}")
    return orders

async def get_order_by_cancel_id(db: AsyncSession, cancel_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by cancel_id"""
    result = await db.execute(select(order_model).filter(order_model.cancel_id == cancel_id))
    return result.scalars().first()

async def get_order_by_close_id(db: AsyncSession, close_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by close_id"""
    result = await db.execute(select(order_model).filter(order_model.close_id == close_id))
    return result.scalars().first()

async def get_order_by_stoploss_id(db: AsyncSession, stoploss_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by stoploss_id"""
    result = await db.execute(select(order_model).filter(order_model.stoploss_id == stoploss_id))
    return result.scalars().first()

async def get_order_by_takeprofit_id(db: AsyncSession, takeprofit_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by takeprofit_id"""
    result = await db.execute(select(order_model).filter(order_model.takeprofit_id == takeprofit_id))
    return result.scalars().first()

async def get_order_by_stoploss_cancel_id(db: AsyncSession, stoploss_cancel_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by stoploss_cancel_id"""
    result = await db.execute(select(order_model).filter(order_model.stoploss_cancel_id == stoploss_cancel_id))
    return result.scalars().first()

async def get_order_by_takeprofit_cancel_id(db: AsyncSession, takeprofit_cancel_id: str, order_model: Type[Any]) -> Optional[Any]:
    """Get order by takeprofit_cancel_id"""
    result = await db.execute(select(order_model).filter(order_model.takeprofit_cancel_id == takeprofit_cancel_id))
    return result.scalars().first()

async def get_order_by_any_id(db: AsyncSession, generic_id: str, order_model: Type[Any]) -> Optional[Any]:
    """
    Get order by matching the given ID against any of the possible ID fields.
    Searches order_id, cancel_id, close_id, stoploss_id, takeprofit_id, etc.
    """
    result = await db.execute(
        select(order_model).filter(
            or_(
                order_model.order_id == generic_id,
                order_model.cancel_id == generic_id,
                order_model.close_id == generic_id,
                order_model.modify_id == generic_id,
                order_model.stoploss_id == generic_id,
                order_model.takeprofit_id == generic_id,
                order_model.stoploss_cancel_id == generic_id,
                order_model.takeprofit_cancel_id == generic_id
            )
        )
    )
    return result.scalars().first()

async def get_open_orders_by_user_id_and_symbol(
    db: AsyncSession,
    user_id: int,
    symbol: str,
    order_model=UserOrder
) -> List[Any]:
    """
    Get all open orders for a specific user and symbol.
    """
    try:
        # Query for open orders
        stmt = select(order_model).where(
            and_(
                order_model.order_user_id == user_id,
                order_model.order_company_name == symbol,
                order_model.order_status == 'OPEN'
            )
        )
        result = await db.execute(stmt)
        orders = result.scalars().all()
        return list(orders)
    except Exception as e:
        print(f"Error getting open orders: {e}")
        return []

async def get_order_by_id_and_user_id(
    db: AsyncSession,
    order_id: str,
    user_id: int,
    order_model
) -> Any:
    """
    Retrieve a single order by order_id and user_id for the given order model.
    """
    try:
        stmt = select(order_model).where(
            and_(
                order_model.order_id == order_id,
                order_model.order_user_id == user_id
            )
        )
        result = await db.execute(stmt)
        return result.scalars().first()
    except Exception as e:
        print(f"Error getting order by order_id and user_id: {e}")
        return None

async def create_user_order(
    db: AsyncSession,
    order_data: Dict[str, Any],
    order_model=UserOrder
) -> Any:
    """
    Create a new order in the database.
    """
    orders_logger = logging.getLogger('orders')
    orders_logger.debug(f"[DEBUG][create_user_order] Received order_data: {order_data}")
    try:
        # Ensure 'status' is present and valid
        if 'status' not in order_data or not isinstance(order_data['status'], str) or not (0 <= len(order_data['status']) <= 30):
            orders_logger.debug(f"[DEBUG][create_user_order] status field value: {order_data.get('status')}")
            raise ValueError("'status' is required and must be a string of length 10-30.")
        db_order = order_model(**order_data)
        db.add(db_order)
        
        # Create a log entry in OrderActionHistory
        user_id = order_data.get('order_user_id')
        user_type = 'demo' if order_model.__name__ == 'DemoUserOrder' else 'live'
        
        # Create history record only if we have the required fields
        if user_id and 'order_id' in order_data:
            history = OrderActionHistory(
                user_id=user_id,
                user_type=user_type,
                order_id=order_data['order_id'],
                cancel_id=order_data.get('cancel_id'),
                close_id=order_data.get('close_id'),
                action_type="CREATE",
                modify_id=order_data.get('modify_id'),
                stoploss_id=order_data.get('stoploss_id'),
                takeprofit_id=order_data.get('takeprofit_id'),
                stoploss_cancel_id=order_data.get('stoploss_cancel_id'),
                takeprofit_cancel_id=order_data.get('takeprofit_cancel_id')
            )
            db.add(history)
            orders_logger.debug(f"[DEBUG][create_user_order] Created OrderActionHistory record for order_id: {order_data['order_id']}")
        
        await db.commit()
        await db.refresh(db_order)
        return db_order
    except Exception as e:
        await db.rollback()
        orders_logger.error(f"Error creating order: {e}")
        raise e

async def update_order(
    db: AsyncSession,
    order_id: str,
    order_data: Dict[str, Any],
    order_model=UserOrder
):
    result = await db.execute(select(order_model).filter(order_model.order_id == order_id))
    db_order = result.scalars().first()
    if not db_order:
        return None
    # If 'status' is being updated, validate it
    if 'status' in order_data and order_data['status'] is not None:
        if not isinstance(order_data['status'], str) or not (10 <= len(order_data['status']) <= 30):
            raise ValueError("'status' must be a string of length 10-30.")
    for key, value in order_data.items():
        setattr(db_order, key, value)
    await db.commit()
    await db.refresh(db_order)
    return db_order

async def delete_order(
    db: AsyncSession,
    order_id: str,
    order_model=UserOrder
) -> bool:
    """
    Delete an order.
    """
    try:
        stmt = select(order_model).where(order_model.order_id == order_id)
        result = await db.execute(stmt)
        db_order = result.scalars().first()
        
        if db_order:
            await db.delete(db_order)
            await db.commit()
            return True
        return False
    except Exception as e:
        await db.rollback()
        print(f"Error deleting order: {e}")
        raise e

async def get_all_orders(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 100,
    order_model=UserOrder
) -> List[Any]:
    """
    Get all orders with pagination.
    """
    try:
        stmt = select(order_model).offset(skip).limit(limit)
        result = await db.execute(stmt)
        return list(result.scalars().all())
    except Exception as e:
        print(f"Error getting all orders: {e}")
        return []

async def get_user_orders(
    db: AsyncSession,
    user_id: int,
    skip: int = 0,
    limit: int = 100,
    order_model=UserOrder
) -> List[Any]:
    """
    Get all orders for a specific user with pagination.
    """
    try:
        stmt = select(order_model).where(
            order_model.order_user_id == user_id
        ).offset(skip).limit(limit)
        result = await db.execute(stmt)
        return list(result.scalars().all())
    except Exception as e:
        print(f"Error getting user orders: {e}")
        return []