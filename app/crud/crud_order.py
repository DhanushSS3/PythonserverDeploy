# app/crud/crud_order.py

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
import uuid

from app.database.models import UserOrder, User
from app.schemas.order import OrderCreateInternal # Use OrderCreateInternal for creation input

async def create_user_order(db: AsyncSession, order_data: dict) -> UserOrder:
    """
    Creates a new user order in the database.
    Expects order_data to be a dictionary with all necessary fields for UserOrder.
    """
    
    db_order = UserOrder(**order_data)
    db.add(db_order)
    await db.commit()
    await db.refresh(db_order)
    return db_order

async def get_order_by_id(db: AsyncSession, order_id: str) -> Optional[UserOrder]:
    """
    Retrieves an order by its unique order_id.
    """
    result = await db.execute(
        select(UserOrder).filter(UserOrder.order_id == order_id)
    )
    return result.scalars().first()

async def get_orders_by_user_id(
    db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100
) -> List[UserOrder]:
    """
    Retrieves all orders for a specific user with pagination.
    """
    result = await db.execute(
        select(UserOrder)
        .filter(UserOrder.order_user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(UserOrder.created_at.desc()) # Optional: order by creation time
    )
    return result.scalars().all()

# --- NEW FUNCTION: Get open orders by user ID and symbol ---
async def get_open_orders_by_user_id_and_symbol(
    db: AsyncSession, user_id: int, symbol: str
) -> List[UserOrder]:
    """
    Retrieves all open orders for a specific user and symbol.
    This is used for hedging logic to find existing opposing positions efficiently.
    """
    result = await db.execute(
        select(UserOrder)
        .filter(
            UserOrder.order_user_id == user_id,
            UserOrder.order_company_name == symbol, # Assuming order_company_name stores the symbol
            UserOrder.order_status == 'OPEN' # Filter for only open orders
        )
        # No limit or offset needed here as we need all open positions for the symbol
    )
    return result.scalars().all()


async def get_all_orders(
    db: AsyncSession, skip: int = 0, limit: int = 100
) -> List[UserOrder]:
    """
    Retrieves all orders with pagination (admin use).
    """
    result = await db.execute(
        select(UserOrder)
        .offset(skip)
        .limit(limit)
        .order_by(UserOrder.created_at.desc()) # Optional: order by creation time
    )
    return result.scalars().all()

async def update_order_status(db: AsyncSession, order_id: str, new_status: str, close_price: Optional[float] = None, net_profit: Optional[float] = None) -> Optional[UserOrder]:
    """
    Updates the status of an order.
    Optionally updates close_price and net_profit if the order is being closed.
    """
    db_order = await get_order_by_id(db, order_id=order_id)
    if db_order:
        db_order.order_status = new_status
        if close_price is not None:
            db_order.close_price = close_price
        if net_profit is not None:
            db_order.net_profit = net_profit
        # Add more fields to update as needed (e.g., close_message)
        await db.commit()
        await db.refresh(db_order)
    return db_order

# Add other CRUD operations as needed (e.g., delete_order)
