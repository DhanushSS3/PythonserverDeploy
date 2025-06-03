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

# Utility to get the appropriate model class
def get_order_model(user_type: str):
    return DemoUserOrder if user_type == "demo" else UserOrder

# Create a new order
async def create_order(db: AsyncSession, order_data: dict, order_model: Type[UserOrder | DemoUserOrder]):
    db_order = order_model(**order_data)
    db.add(db_order)
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
    return result.scalars().all()

# Get all open orders for user
async def get_all_open_orders_by_user_id(
    db: AsyncSession, user_id: int, order_model: Type[UserOrder | DemoUserOrder]
):
    result = await db.execute(
        select(order_model).filter(
            order_model.order_user_id == user_id,
            order_model.order_status == 'OPEN'
        )
    )
    return result.scalars().all()

# Get all open orders from UserOrder table (system-wide)
async def get_all_system_open_orders(db: AsyncSession):
    result = await db.execute(
        select(UserOrder).filter(UserOrder.order_status == 'OPEN')
    )
    return result.scalars().all()

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
    user_type: str
):
    for field, value in update_fields.items():
        if hasattr(db_order, field):
            setattr(db_order, field, value)

    # Create a log entry in OrderActionHistory
    history = OrderActionHistory(
        user_id=user_id,
        user_type=user_type,
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
    return result.scalars().all()

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

async def create_user_order(
    db: AsyncSession,
    order_data: Dict[str, Any],
    order_model=UserOrder
) -> Any:
    """
    Create a new order in the database.
    """
    try:
        db_order = order_model(**order_data)
        db.add(db_order)
        await db.commit()
        await db.refresh(db_order)
        return db_order
    except Exception as e:
        await db.rollback()
        print(f"Error creating order: {e}")
        raise e

async def update_order(
    db: AsyncSession,
    order_id: str,
    order_data: Dict[str, Any],
    order_model=UserOrder
) -> Optional[Any]:
    """
    Update an existing order.
    """
    try:
        stmt = select(order_model).where(order_model.order_id == order_id)
        result = await db.execute(stmt)
        db_order = result.scalars().first()
        
        if db_order:
            for key, value in order_data.items():
                setattr(db_order, key, value)
            await db.commit()
            await db.refresh(db_order)
            return db_order
        return None
    except Exception as e:
        await db.rollback()
        print(f"Error updating order: {e}")
        raise e

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