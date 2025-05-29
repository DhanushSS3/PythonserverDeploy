# app/crud/crud_order.py

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
import uuid
from decimal import Decimal
from app.database.models import UserOrder, User # Ensure User is imported
from app.schemas.order import OrderCreateInternal # Use OrderCreateInternal for creation input
from app.core.config import get_settings # Import settings to potentially use cache expiry

# Import caching functions
from app.core.cache import (
    set_user_portfolio_cache, # To update portfolio cache after order changes
    USER_PORTFOLIO_CACHE_EXPIRY_SECONDS # Use the expiry from cache.py
)

settings = get_settings()

async def create_user_order(db: AsyncSession, order_data: dict) -> UserOrder:
    """
    Creates a new user order in the database.
    Expects order_data to be a dictionary with all necessary fields for UserOrder.
    After creation, it attempts to update the user's portfolio cache.
    """

    db_order = UserOrder(**order_data) #
    db.add(db_order)
    await db.commit()
    await db.refresh(db_order)

    # Attempt to update the user's portfolio cache after a new order is created
    # This is a basic implementation; a more robust approach might involve a dedicated
    # service that handles cache updates based on various order/trade events.
    # This requires access to redis_client, which is not available directly here.
    # A better place for this cache update would be in the order processing service.
    # For now, we'll leave this out of the CRUD layer to keep it focused on DB operations.
    # The broadcaster will fetch the latest positions from DB or rely on the
    # order processing service to update the cache.

    return db_order

async def get_order_by_id(db: AsyncSession, order_id: str) -> Optional[UserOrder]:
    """
    Retrieves an order by its unique order_id.
    """
    result = await db.execute(
        select(UserOrder).filter(UserOrder.order_id == order_id) #
    )
    return result.scalars().first()

async def get_orders_by_user_id(
    db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100
) -> List[UserOrder]:
    """
    Retrieves all orders for a specific user with pagination.
    """
    result = await db.execute(
        select(UserOrder) #
        .filter(UserOrder.order_user_id == user_id) #
        .offset(skip)
        .limit(limit)
        .order_by(UserOrder.created_at.desc()) # Optional: order by creation time
    )
    return result.scalars().all()

# --- NEW FUNCTION: Get all open orders by user ID ---
async def get_all_open_orders_by_user_id(
    db: AsyncSession, user_id: int
) -> List[UserOrder]:
    """
    Retrieves all open orders for a specific user.
    Used to get the list of positions for portfolio calculation.
    """
    result = await db.execute(
        select(UserOrder) #
        .filter(
            UserOrder.order_user_id == user_id, #
            UserOrder.order_status == 'OPEN' # Filter for only open orders
        )
        # No limit or offset needed here as we need all open positions for the user's portfolio
    )
    return result.scalars().all()

# --- Existing function: Get open orders by user ID and symbol ---
async def get_open_orders_by_user_id_and_symbol(
    db: AsyncSession, user_id: int, symbol: str
) -> List[UserOrder]:
    """
    Retrieves all open orders for a specific user and symbol.
    This is used for hedging logic to find existing opposing positions efficiently.
    """
    result = await db.execute(
        select(UserOrder) #
        .filter(
            UserOrder.order_user_id == user_id, #
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
        select(UserOrder) #
        .offset(skip)
        .limit(limit)
        .order_by(UserOrder.created_at.desc()) # Optional: order by creation time
    )
    return result.scalars().all()

async def update_order_status(db: AsyncSession, order_id: str, new_status: str, close_price: Optional[float] = None, net_profit: Optional[float] = None) -> Optional[UserOrder]:
    """
    Updates the status of an order.
    Optionally updates close_price and net_profit if the order is being closed.
    After updating status to 'CLOSED' or 'CANCELLED', it attempts to update the user's portfolio cache.
    """
    db_order = await get_order_by_id(db, order_id=order_id)
    if db_order:
        db_order.order_status = new_status #
        if close_price is not None:
            # Ensure close_price is stored as Decimal if the model expects it
            from decimal import Decimal # Import Decimal locally if needed for conversion
            db_order.close_price = Decimal(str(close_price)) if not isinstance(close_price, Decimal) else close_price #
        if net_profit is not None:
            # Ensure net_profit is stored as Decimal
            from decimal import Decimal
            db_order.net_profit = Decimal(str(net_profit)) if not isinstance(net_profit, Decimal) else net_profit #
        # Add more fields to update as needed (e.g., close_message)
        await db.commit()
        await db.refresh(db_order)

        # Attempt to update the user's portfolio cache after order status change
        # Similar to create_user_order, this is better handled in a service layer
        # that has access to redis_client and can refetch/recalculate portfolio.
        # For now, we rely on the broadcaster fetching from cache (which should be
        # updated by the order processing service upon order closure/cancellation).

    return db_order

# Add other CRUD operations as needed (e.g., delete_order)

async def get_open_and_pending_orders_by_user_id_and_symbol(
    db: AsyncSession, user_id: int, symbol: str
) -> List[UserOrder]:
    result = await db.execute(
        select(UserOrder).filter(
            UserOrder.order_user_id == user_id,
            UserOrder.order_company_name == symbol,
            UserOrder.order_status.in_(["OPEN", "PENDING"])  # Include both
        )
    )
    return result.scalars().all()

async def update_tp_sl_for_order(
    db: AsyncSession,
    order_id: str,
    stop_loss: Decimal,
    take_profit: Decimal,
    stoploss_id: str,
    takeprofit_id: str
) -> Optional[UserOrder]:
    order = await get_order_by_id(db, order_id)
    if not order:
        return None

    order.stop_loss = stop_loss
    order.take_profit = take_profit
    order.stoploss_id = stoploss_id
    order.takeprofit_id = takeprofit_id

    await db.commit()
    await db.refresh(order)
    return order

# --- NEW FUNCTION: Get all system-wide open orders ---
async def get_all_system_open_orders(db: AsyncSession) -> List[UserOrder]:
    """
    Retrieves all orders with 'OPEN' status across all users.
    Eager loads the related User object to access user.group_name efficiently.
    """
    result = await db.execute(
        select(UserOrder)
        .filter(UserOrder.order_status == 'OPEN')
        .options(selectinload(UserOrder.user)) # Eager load the 'user' relationship
    )
    return result.scalars().all()

async def get_orders_by_user_id_and_statuses(
    db: AsyncSession, user_id: int, statuses: List[str], skip: int = 0, limit: int = 100
) -> List[UserOrder]:
    """
    Retrieves orders for a specific user that match any of the given statuses, with pagination.
    """
    result = await db.execute(
        select(UserOrder)
        .filter(
            UserOrder.order_user_id == user_id,
            UserOrder.order_status.in_(statuses)
        )
        .offset(skip)
        .limit(limit)
        .order_by(UserOrder.created_at.desc())
    )
    return result.scalars().all()