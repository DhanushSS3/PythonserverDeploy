# crud_order.py
from typing import List, Optional, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from decimal import Decimal
from app.database.models import UserOrder, DemoUserOrder, OrderActionHistory
from app.schemas.order import OrderCreateInternal
from sqlalchemy.orm import selectinload
from datetime import datetime

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
async def get_order_by_id(db: AsyncSession, order_id: str, order_model: Type[UserOrder | DemoUserOrder]):
    result = await db.execute(
        select(order_model).filter(order_model.order_id == order_id)
    )
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