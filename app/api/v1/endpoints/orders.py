# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis # Import Redis
import uuid
import logging
from decimal import Decimal
from typing import Optional, Any, List

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client # Import redis client dependency
from app.database.models import UserOrder, User # Import UserOrder and User models
# Updated schema imports
from app.schemas.order import OrderPlacementRequest, OrderResponse, OrderCreateInternal
from app.schemas.user import StatusResponse
from app.core.security import get_current_user
# Import the new margin calculator service
from app.services.margin_calculator import calculate_single_order_margin
# Import the new CRUD operations for orders
from app.crud import crud_order

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders", # Add prefix for consistency
    tags=["orders"]
)

# Endpoint to place a new order
@router.post(
    "/",
    response_model=OrderResponse, # Use the new OrderResponse schema
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
    description="Allows an authenticated user to place a new trading order. Margin and contract value are calculated by the backend."
)
async def place_order(
    order_request: OrderPlacementRequest, # Use the new request schema
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client), # Inject Redis client
    current_user: User = Depends(get_current_user)
):
    """
    Places a new order for the authenticated user.
    Calculates margin and contract value.
    """
    logger.info(f"Attempting to place order for user {current_user.id}, symbol {order_request.symbol}")

    # Validate order quantity (example: must be positive)
    if order_request.order_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order quantity must be positive."
        )

    # 1. Calculate margin and adjusted price
    calculated_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
        db=db,
        redis_client=redis_client,
        user_id=current_user.id,
        order_quantity=order_request.order_quantity,
        order_price=order_request.order_price, # Original price from user
        symbol=order_request.symbol,
        order_type=order_request.order_type
    )

    if calculated_margin_usd is None or adjusted_order_price is None or contract_value is None:
        logger.error(f"Margin calculation failed for user {current_user.id}, symbol {order_request.symbol}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to calculate margin for the order. Please check symbol configuration and user data."
        )

    # TODO: Add pre-order checks:
    # 1. Check if user has sufficient wallet_balance >= calculated_margin_usd
    #    (This might involve locking the user's balance temporarily)
    #    user_wallet_balance = current_user.wallet_balance # Assuming it's up-to-date
    #    if user_wallet_balance < calculated_margin_usd:
    #        raise HTTPException(
    #            status_code=status.HTTP_400_BAD_REQUEST,
    #            detail=f"Insufficient funds. Required margin: {calculated_margin_usd:.2f} USD."
    #        )

    # 2. Other risk checks (e.g., max exposure, max open orders for symbol, etc.)

    # Generate a unique order_id
    unique_order_id = str(uuid.uuid4())
    order_status = "OPEN" # Or "PENDING_EXECUTION" depending on your flow

    # Prepare data for creating the UserOrder record using OrderCreateInternal
    order_data_internal = OrderCreateInternal(
        order_id=unique_order_id,
        order_status=order_status,
        order_user_id=current_user.id,
        order_company_name=order_request.symbol, # Storing symbol as company_name
        order_type=order_request.order_type.upper(),
        order_price=adjusted_order_price, # Use the adjusted price
        order_quantity=order_request.order_quantity,
        contract_value=contract_value, # Store calculated contract value
        margin=calculated_margin_usd, # Store calculated margin in USD
        stop_loss=order_request.stop_loss,
        take_profit=order_request.take_profit,
        # Initialize other fields as needed
        net_profit=None,
        close_price=None,
        swap=None, # Swap might be calculated later or at end of day
        commission=None, # Commission might be calculated here or upon closing
        status=1 # Default status for an open order
    )

    try:
        # Create the order in the database using the new CRUD function
        # The crud_order.create_user_order expects a dictionary
        new_db_order = await crud_order.create_user_order(db=db, order_data=order_data_internal.model_dump())

        # TODO: Post-order creation actions:
        # 1. Deduct margin from user's wallet_balance or update used_margin field.
        #    (This requires careful handling of concurrency and transactions)
        #    current_user.wallet_balance -= calculated_margin_usd # Simplified; use a locked update
        #    await db.commit()
        #    await db.refresh(current_user)

        # 2. Publish order event to a queue for further processing (e.g., matching engine, audit log)

        logger.info(f"Order {new_db_order.order_id} placed successfully for user ID {current_user.id} with margin {calculated_margin_usd} USD.")

        # Return the created order details using OrderResponse
        # Need to convert the ORM object (new_db_order) to the OrderResponse Pydantic model
        return OrderResponse.model_validate(new_db_order) # For Pydantic V2
        # For Pydantic V1: return OrderResponse.from_orm(new_db_order)


    except HTTPException as http_exc: # Re-raise known HTTP exceptions
        raise http_exc
    except Exception as e:
        await db.rollback()
        logger.error(f"Error placing order for user ID {current_user.id}, symbol {order_request.symbol}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while placing the order."
        )

# Example: Endpoint to get order by ID (using new CRUD)
@router.get("/{order_id}", response_model=OrderResponse)
async def read_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user) # Ensure user owns order or is admin
):
    db_order = await crud_order.get_order_by_id(db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    # Add authorization: check if current_user.id == db_order.order_user_id or if user is admin
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False): # Example admin check
         raise HTTPException(status_code=403, detail="Not authorized to view this order")
    return OrderResponse.model_validate(db_order)


# Example: Endpoint to get user's orders (using new CRUD)
@router.get("/", response_model=List[OrderResponse])
async def read_user_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, skip=skip, limit=limit)
    return [OrderResponse.model_validate(order) for order in orders]


# You would also add other endpoints like cancel_order, modify_order, etc.
# These would use functions from crud_order.py and potentially margin_calculator.py if needed.