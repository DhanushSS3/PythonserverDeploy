# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal
from typing import Optional, Any, List

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, UserOrder # Import UserOrder model
# Updated schema imports
from app.schemas.order import OrderPlacementRequest, OrderResponse # Only need request and response schemas here
from app.schemas.user import StatusResponse # Keep if used elsewhere in this file
from app.core.security import get_current_user

# Import the new order processing service and its custom exceptions
from app.services.order_processing import process_new_order, OrderProcessingError, InsufficientFundsError
# Import CRUD for fetching orders
from app.crud import crud_order

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

# Endpoint to place a new order
@router.post(
    "/",
    response_model=OrderResponse, # Use the new OrderResponse schema
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
    description="Allows an authenticated user to place a new trading order. Processing, including margin calculation and hedging, is handled by the backend service."
)
async def place_order(
    order_request: OrderPlacementRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    """
    Receives a new order request and delegates processing to the order_processing service.
    Handles service-specific exceptions and translates them to HTTP responses.
    """
    logger.info(f"Received order placement request for user {current_user.id}, symbol {order_request.symbol}")

    # Validate order quantity (example: must be positive) - Keep basic validation here
    if order_request.order_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order quantity must be positive."
        )

    try:
        # Delegate the complex processing to the service layer
        new_db_order = await process_new_order(
            db=db,
            redis_client=redis_client,
            user=current_user, # Pass the user ORM object
            order_request=order_request
        )

        # If the service function completes without raising an exception,
        # the order was successfully created and user margin updated.
        logger.info(f"Order {new_db_order.order_id} successfully processed by service.")

        # Return the created order details using OrderResponse
        return OrderResponse.model_validate(new_db_order) # For Pydantic V2

    except InsufficientFundsError as e:
        # Catch the specific insufficient funds error from the service
        logger.warning(f"Order placement failed for user {current_user.id} due to insufficient funds: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e) # Use the message from the service exception
        )
    except OrderProcessingError as e:
        # Catch other processing errors from the service
        logger.error(f"Order processing failed for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the order."
        )
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Unexpected error during order placement for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred."
        )


# Endpoint to get order by ID (using crud_order)
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


# Endpoint to get user's orders (using crud_order)
# This endpoint can be used for fetching ALL orders, including closed and pending for historical view.
@router.get("/", response_model=List[OrderResponse])
async def read_user_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, skip=skip, limit=limit)
    # Convert ORM objects to Pydantic models for the response
    return [OrderResponse.model_validate(order) for order in orders]

# --- New Endpoints for Specific Order Statuses (Optional but Recommended for Clarity) ---

@router.get("/open", response_model=List[OrderResponse])
async def read_user_open_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # You'll need a CRUD function to get orders by user_id and status
    # Assuming crud_order has a function like get_orders_by_user_id_and_status
    # If not, you can filter the result from get_orders_by_user_id or add a new CRUD function.
    # Example (filtering after fetching all):
    all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, limit=skip+limit) # Fetch enough to cover limit+skip
    open_orders = [order for order in all_orders if order.order_status == 'OPEN'][skip:skip+limit]

    # A dedicated CRUD function would be more efficient:
    # open_orders = await crud_order.get_orders_by_user_id_and_status(db, user_id=current_user.id, status='OPEN', skip=skip, limit=limit)

    return [OrderResponse.model_validate(order) for order in open_orders]

@router.get("/closed", response_model=List[OrderResponse])
async def read_user_closed_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Similar to open orders, fetch and filter or use a dedicated CRUD function
    all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, limit=skip+limit)
    closed_orders = [order for order in all_orders if order.order_status == 'CLOSED'][skip:skip+limit]
    # closed_orders = await crud_order.get_orders_by_user_id_and_status(db, user_id=current_user.id, status='CLOSED', skip=skip, limit=limit)
    return [OrderResponse.model_validate(order) for order in closed_orders]

@router.get("/pending", response_model=List[OrderResponse])
async def read_user_pending_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Similar to open orders, fetch and filter or use a dedicated CRUD function
    all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, limit=skip+limit)
    pending_orders = [order for order in all_orders if order.order_status == 'PENDING'][skip:skip+limit]
    # pending_orders = await crud_order.get_orders_by_user_id_and_status(db, user_id=current_user.id, status='PENDING', skip=skip, limit=limit)
    return [OrderResponse.model_validate(order) for order in pending_orders]


# You would also add other endpoints like cancel_order, modify_order, etc.,
# which would call corresponding functions in the order_processing service.
