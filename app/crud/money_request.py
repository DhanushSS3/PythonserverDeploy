# app/crud/money_request.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload 
from decimal import Decimal
import datetime
from typing import Optional, List

from app.database.models import MoneyRequest, User # Wallet model is used via crud_user
from app.schemas.money_request import MoneyRequestCreate, MoneyRequestUpdateStatus
# WalletCreate schema is used by crud_user.update_user_wallet_balance
# from app.schemas.wallet import WalletCreate 

from app.crud import user as crud_user # For updating user balance and getting user with lock
# from app.crud import wallet as crud_wallet # For creating wallet records directly, now handled by crud_user

import logging
from app.core.logging_config import money_requests_logger

# Import Redis client dependency and cache functions
from app.dependencies.redis_client import get_redis_client
from app.core.cache import publish_user_data_update

logger = logging.getLogger(__name__)

async def create_money_request(db: AsyncSession, request_data: MoneyRequestCreate, user_id: int) -> MoneyRequest:
    """
    Creates a new money request for a user.
    Status defaults to 0 (requested).
    """
    # Log the incoming request data for debugging
    money_requests_logger.info(f"Creating money request - User ID: {user_id}, Type: {request_data.type}, Amount: {request_data.amount}")
    money_requests_logger.debug(f"Money request schema validation passed? True (since we got here)")
    
    try:
        db_request = MoneyRequest(
            user_id=user_id,
            amount=request_data.amount,
            type=request_data.type,
            status=0  # Default status is 'requested'
        )
        
        money_requests_logger.debug(f"Constructed MoneyRequest object: user_id={db_request.user_id}, type={db_request.type}, amount={db_request.amount}")
        
        db.add(db_request)
        money_requests_logger.debug("Added money request to session")
        
        await db.commit()
        money_requests_logger.debug("Committed transaction to database")
        
        await db.refresh(db_request)
        money_requests_logger.info(f"Money request created successfully: ID {db_request.id}, User ID {user_id}, Type {request_data.type}, Amount {request_data.amount}")
        return db_request
    except Exception as e:
        money_requests_logger.error(f"Failed to create money request: {str(e)}", exc_info=True)
        await db.rollback()
        money_requests_logger.debug("Transaction rolled back due to error")
        raise e

async def get_money_request_by_id(db: AsyncSession, request_id: int) -> Optional[MoneyRequest]:
    """
    Retrieves a money request by its ID.
    """
    result = await db.execute(
        select(MoneyRequest).filter(MoneyRequest.id == request_id)
        # Optionally, eager load the user if needed frequently with the request
        # .options(selectinload(MoneyRequest.user)) 
    )
    return result.scalars().first()

async def get_money_requests_by_user_id(db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100) -> List[MoneyRequest]:
    """
    Retrieves all money requests for a specific user with pagination.
    """
    result = await db.execute(
        select(MoneyRequest)
        .filter(MoneyRequest.user_id == user_id)
        .order_by(MoneyRequest.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

async def get_all_money_requests(db: AsyncSession, skip: int = 0, limit: int = 100, status: Optional[int] = None) -> List[MoneyRequest]:
    """
    Retrieves all money requests, optionally filtered by status, with pagination (for admins).
    """
    query = select(MoneyRequest).order_by(MoneyRequest.created_at.desc())
    if status is not None:
        query = query.filter(MoneyRequest.status == status)
    
    result = await db.execute(query.offset(skip).limit(limit))
    return result.scalars().all()

async def update_money_request_status(
    db: AsyncSession,
    request_id: int,
    new_status: int,
    admin_id: Optional[int] = None,
    redis_client = None  # Add Redis client parameter
) -> Optional[MoneyRequest]:
    """
    Updates the status of a money request.
    If approved (new_status == 1):
        - Atomically updates the user's wallet balance (with row-level lock on User).
        - Creates a corresponding wallet transaction record.
        - Publishes user data update to WebSocket clients.
    All these operations are performed within a single transaction.
    """
    money_requests_logger.info(f"Starting money request status update - ID: {request_id}, New Status: {new_status}, Admin ID: {admin_id}")
    
    # Get the money request
    money_request = await db.get(MoneyRequest, request_id)

    if not money_request:
        money_requests_logger.warning(f"Money request ID {request_id} not found for status update.")
        return None

    # Prevent reprocessing if the request is not in 'requested' state (status 0)
    if money_request.status != 0:
        money_requests_logger.warning(f"Money request ID {request_id} has already been processed or is not in a pending state (current status: {money_request.status}). Cannot update.")
        return None 

    original_status = money_request.status
    money_request.status = new_status
    money_request.updated_at = datetime.datetime.utcnow()
    
    # Explicitly add the money_request object to the session to mark it as modified
    db.add(money_request)
    money_requests_logger.debug(f"Updated money request ID {request_id} status from {original_status} to {new_status} and marked for commit")

    if new_status == 1:  # Approved
        money_requests_logger.info(f"Money request ID {request_id} approved by admin ID {admin_id if admin_id else 'N/A'}. Processing wallet update for user ID {money_request.user_id}.")
        
        amount_to_change = money_request.amount
        transaction_description = f"Money request ID {money_request.id} (type: {money_request.type}) approved by admin."
        
        if money_request.type == "withdraw":
            amount_to_change = -money_request.amount # Negative for withdrawal
        
        try:
            # Call update_user_wallet_balance without nested transaction
            # The outer transaction from the API endpoint will handle everything
            updated_user = await crud_user.update_user_wallet_balance(
                db=db,
                user_id=money_request.user_id,
                amount=amount_to_change,
                transaction_type=money_request.type,
                description=transaction_description
            )


            from app.core.cache import set_user_data_cache

            # After updating the user's wallet balance:
            db_user = await crud_user.get_user_by_id(db, money_request.user_id, user_type="live")  # or "demo" if needed
            if db_user:
                user_data_to_cache = {
                    "id": db_user.id,
                    "email": getattr(db_user, 'email', None),
                    "group_name": db_user.group_name,
                    "leverage": db_user.leverage,
                    "user_type": db_user.user_type,
                    "account_number": getattr(db_user, 'account_number', None),
                    "wallet_balance": db_user.wallet_balance,
                    "margin": db_user.margin,
                    "first_name": getattr(db_user, 'first_name', None),
                    "last_name": getattr(db_user, 'last_name', None),
                    "country": getattr(db_user, 'country', None),
                    "phone_number": getattr(db_user, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, db_user.id, user_data_to_cache)
                        
            if not updated_user:
                money_requests_logger.error(f"Wallet balance update failed for user ID {money_request.user_id} for money request ID {request_id}, but no exception was raised by crud_user.")
                return None 

            money_requests_logger.info(f"Wallet balance updated successfully for user ID {money_request.user_id} due to money request ID {request_id} approval.")

            # Publish user data update to WebSocket clients if Redis client is available
            if redis_client:
                try:
                    await publish_user_data_update(redis_client, money_request.user_id)
                    money_requests_logger.info(f"Published user data update for user ID {money_request.user_id} after wallet balance update.")
                except Exception as e:
                    money_requests_logger.warning(f"Failed to publish user data update for user ID {money_request.user_id}: {e}")
            else:
                money_requests_logger.warning(f"No Redis client available to publish user data update for user ID {money_request.user_id}.")

        except ValueError as ve:
            money_requests_logger.error(f"Processing approved money request ID {request_id} failed: {ve}", exc_info=True)
            raise ve 
        except Exception as e:
            money_requests_logger.error(f"Unexpected error processing approved money request ID {request_id}: {e}", exc_info=True)
            raise e

    elif new_status == 2: # Rejected
        money_requests_logger.info(f"Money request ID {request_id} rejected by admin ID {admin_id if admin_id else 'N/A'}.")
        # Create a wallet transaction record for the rejected request
        try:
            from app.schemas.wallet import WalletCreate
            from app.crud.wallet import create_wallet_record
            
            transaction_description = f"Money request ID {money_request.id} (type: {money_request.type}) rejected by admin."
            
            # Create a wallet record for tracking purposes, with is_approved=0
            wallet_data = WalletCreate(
                user_id=money_request.user_id,
                transaction_type=money_request.type,
                transaction_amount=money_request.amount,
                description=transaction_description,
                is_approved=0  # Set to 0 for rejected transactions
            )
            
            wallet_record = await create_wallet_record(db, wallet_data)
            if wallet_record:
                money_requests_logger.info(f"Wallet record created for rejected money request ID {request_id}, transaction ID: {wallet_record.transaction_id}")
            else:
                money_requests_logger.warning(f"Failed to create wallet record for rejected money request ID {request_id}")
            
            # Publish user data update to WebSocket clients if Redis client is available
            if redis_client:
                try:
                    await publish_user_data_update(redis_client, money_request.user_id)
                    money_requests_logger.info(f"Published user data update for user ID {money_request.user_id} after money request rejection.")
                except Exception as e:
                    money_requests_logger.warning(f"Failed to publish user data update for user ID {money_request.user_id}: {e}")
        except Exception as e:
            money_requests_logger.error(f"Error creating wallet record for rejected money request ID {request_id}: {e}", exc_info=True)
            # Continue processing, don't fail the whole transaction if wallet record creation fails

    # If new_status is 0 (back to requested), it's unusual for an admin action but handled.
    elif new_status == 0 and original_status != 0:
         money_requests_logger.info(f"Money request ID {request_id} status changed to 'requested' by admin ID {admin_id if admin_id else 'N/A'}.")

    # Refresh to get the latest state from the DB
    await db.refresh(money_request)
    money_requests_logger.info(f"Money request ID {request_id} status updated successfully to {new_status}")
    return money_request
