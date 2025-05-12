# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import uuid
import logging
from decimal import Decimal # Ensure Decimal is imported if needed

from app.database.session import get_db
from app.database.models import UserOrder, User # Import UserOrder and User models
from app.schemas.order import OrderCreate # Import the OrderCreate schema
from app.schemas.user import StatusResponse # Assuming StatusResponse is in user schema
from app.core.security import get_current_user # Assuming get_current_user is in security.py


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["orders"]
)

# Endpoint to place a new order
@router.post(
    "/",
    response_model=OrderCreate, # Adjust if you create a separate OrderResponse schema
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
    description="Allows an authenticated user to place a new trading order."
)
async def place_order(
    order_details: OrderCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user) # Requires authentication
):
    """
    Places a new order for the authenticated user.
    """
    # Ensure the order is being placed for the authenticated user
    # You might remove order_user_id from the schema and get it directly from current_user
    # to prevent the client from specifying it incorrectly.
    # If keeping it, validate it matches the authenticated user.
    if order_details.order_user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot place order for a different user."
        )

    # Generate a unique order_id (example using UUID)
    # In a real application, you might have a more sophisticated order ID generation system
    unique_order_id = str(uuid.uuid4())

    # Create a new UserOrder database object
    new_order = UserOrder(
        order_id=unique_order_id,
        order_status=order_details.order_status,
        order_user_id=order_details.order_user_id, # Use validated user ID
        order_company_name=order_details.order_company_name,
        order_type=order_details.order_type,
        order_price=order_details.order_price,
        order_quantity=order_details.order_quantity,
        contract_value=order_details.contract_value,
        margin=order_details.margin,
        net_profit=order_details.net_profit,
        close_price=order_details.close_price,
        swap=order_details.swap,
        commission=order_details.commission,
        stop_loss=order_details.stop_loss,
        take_profit=order_details.take_profit,
        cancel_message=order_details.cancel_message,
        close_message=order_details.close_message,
        status=order_details.status
    )

    try:
        # Add the new order to the database session and commit
        db.add(new_order)
        await db.commit()
        await db.refresh(new_order) # Refresh to get the generated id and timestamps

        logger.info(f"Order placed successfully for user ID {current_user.id}: {new_order.order_id}")

        # Return the created order details
        return OrderCreate(**new_order.__dict__) # Convert ORM object back to Pydantic model

    except Exception as e:
        await db.rollback() # Rollback the transaction in case of error
        logger.error(f"Error placing order for user ID {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while placing the order."
        )