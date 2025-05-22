# app/api/v1/endpoints/firebase_orders.py (New File)

import logging
import uuid # For potentially generating a unique ID if Firebase push key isn't sufficient for some internal logging
from decimal import Decimal, ROUND_HALF_UP
import time # For timestamp

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.core.security import get_current_user
from app.database.models import User
from app.schemas.firebase_order import FirebaseOrderPlacementRequest, FirebaseOrderDataStructure
from app.services.margin_calculator import calculate_single_order_margin
from app.firebase_stream import firebase_db # Assuming firebase_db is the initialized RTDB instance
from firebase_admin import db as firebase_rtdb

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post(
    "/place",
    # response_model=FirebaseOrderDataStructure, # Or a simple status response
    status_code=status.HTTP_200_OK, # Or 201 if we consider it a "creation" in Firebase
    summary="Calculate order details and send to Firebase RTDB",
    description="Receives basic order details, calculates margin and other necessary values, "
                "and then pushes the complete order structure to Firebase RTDB 'trade_data' path."
)
async def place_firebase_order(
    order_request: FirebaseOrderPlacementRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    logger.info(f"Firebase order placement request received for user {current_user.id}, symbol {order_request.symbol}")

    if order_request.user_id != current_user.id:
        # This check might be redundant if user_id is not part of the request
        # and solely taken from current_user.id.
        # If frontend sends user_id, we should validate it against the token.
        logger.warning(f"User ID mismatch: token user ID {current_user.id}, request user ID {order_request.user_id}")
        # For now, we trust the current_user.id from the token.
        
    user_id_for_order = current_user.id

    # Determine is_limit and order_status based on order_type
    is_limit_bool: bool
    order_status_str: str
    price_for_margin_calc = order_request.price # Default to user-provided price
    price_for_firebase = order_request.price   # Price to be sent to Firebase

    if order_request.order_type in ["BUY", "SELL"]: # Instant execution
        is_limit_bool = False
        order_status_str = "open"
        # For instant orders, order_request.price is the current market price used for margin calculation.
        # The adjusted price will be calculated by calculate_single_order_margin.
    elif order_request.order_type in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]: # Pending orders
        is_limit_bool = True
        order_status_str = "pending"
        # For pending orders, order_request.price is the user-defined trigger price.
        # This trigger price is used for margin calculation and is also the price sent to Firebase.
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid order type: {order_request.order_type}"
        )

    # 1. Calculate full margin, adjusted price (for instant), and contract value
    # For pending orders, `order_request.price` (the trigger price) is used as the basis.
    # For instant orders, `order_request.price` (current market price) is used, and an adjusted execution price is returned.
    calculated_margin_usd, execution_or_target_price, contract_val = await calculate_single_order_margin(
        db=db,
        redis_client=redis_client,
        user_id=user_id_for_order,
        order_quantity=order_request.order_quantity,
        order_price=price_for_margin_calc, # This is the user-provided price
        symbol=order_request.symbol,
        order_type=order_request.order_type # Pass the full order_type (e.g., BUY_LIMIT)
    )

    if calculated_margin_usd is None or execution_or_target_price is None or contract_val is None:
        logger.error(
            f"Failed to calculate margin/price/contract value for user {user_id_for_order}, symbol {order_request.symbol}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Order processing failed: Could not calculate margin, price, or contract value."
        )

    # For instant orders, the price sent to Firebase is the adjusted execution price.
    # For pending orders, the price sent to Firebase is the user-defined trigger price.
    if not is_limit_bool: # Instant order
        price_for_firebase = execution_or_target_price
    else: # Pending order
        price_for_firebase = order_request.price # This is the user-defined target price

    # 2. Prepare data structure for Firebase
    # Ensuring all numeric values that need to be strings are converted appropriately
    firebase_order_payload = FirebaseOrderDataStructure(
        account_type=order_request.account_type,
        contract_value=str(contract_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), # Match frontend's toFixed(2)
        is_limit=str(is_limit_bool).lower(), # 'true' or 'false' as string
        margin=str(calculated_margin_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), # Match frontend's toFixed(2)
        order_status=order_status_str,
        order_type=order_request.order_type,
        price=str(price_for_firebase.quantize(Decimal("0.00000"), rounding=ROUND_HALF_UP)), # Match formatPrice or to 5 decimal places
        qnt=str(order_request.order_quantity),
        swap=order_request.swap if order_request.swap is not None else "50",
        symbol=order_request.symbol,
        user_id=str(user_id_for_order),
        timestamp=int(time.time() * 1000) # Current time in milliseconds, similar to Date.now()
    )

    # 3. Send data to Firebase RTDB
    try:
        if firebase_db is None:
            logger.critical("Firebase Realtime Database instance (firebase_db) is not initialized.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Firebase service is unavailable."
            )
              
        trade_data_ref = firebase_rtdb.reference('trade_data')
        new_order_firebase_ref = trade_data_ref.push(firebase_order_payload.model_dump(exclude_none=True))
        # new_order_firebase_key = new_order_firebase_ref.key # The unique key generated by push()

        logger.info(
            f"Order for user {user_id_for_order}, symbol {order_request.symbol} successfully sent to Firebase RTDB. "
            f"Firebase Key: {new_order_firebase_ref.key}"
        )
        
        return {
            "message": "Order data successfully calculated and sent to Firebase.",
            "firebase_key": new_order_firebase_ref.key,
            "sent_data": firebase_order_payload
        }

    except HTTPException as http_exc: # Re-raise HTTPException
        raise http_exc
    except Exception as e:
        logger.error(f"Error sending data to Firebase RTDB for user {user_id_for_order}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send order data to Firebase."
        )