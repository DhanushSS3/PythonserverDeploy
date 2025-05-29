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

logger = logging.getLogger(__name__)

async def create_money_request(db: AsyncSession, request_data: MoneyRequestCreate, user_id: int) -> MoneyRequest:
    """
    Creates a new money request for a user.
    Status defaults to 0 (requested).
    """
    db_request = MoneyRequest(
        user_id=user_id,
        amount=request_data.amount,
        type=request_data.type,
        status=0  # Default status is 'requested'
    )
    db.add(db_request)
    await db.commit()
    await db.refresh(db_request)
    logger.info(f"Money request created: ID {db_request.id}, User ID {user_id}, Type {request_data.type}, Amount {request_data.amount}")
    return db_request

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
    admin_id: Optional[int] = None 
) -> Optional[MoneyRequest]:
    """
    Updates the status of a money request.
    If approved (new_status == 1):
        - Atomically updates the user's wallet balance (with row-level lock on User).
        - Creates a corresponding wallet transaction record.
    All these operations are performed within a nested transaction.
    """
    # Start a nested transaction to ensure atomicity of updating MoneyRequest status
    # and performing wallet operations if approved.
    async with db.begin_nested():
        # It's generally a good idea to lock the specific money request if multiple admins
        # could try to process it simultaneously, though the status check below offers some protection.
        # For simplicity and focusing on the user wallet lock, we'll fetch it normally first.
        # If contention on the same money request is a high concern, add .with_for_update() here.
        money_request = await db.get(MoneyRequest, request_id) # Simpler way to get by PK

        if not money_request:
            logger.warning(f"Money request ID {request_id} not found for status update.")
            return None # Or raise an error to be handled by the API layer

        # Prevent reprocessing if the request is not in 'requested' state (status 0)
        if money_request.status != 0:
            logger.warning(f"Money request ID {request_id} has already been processed or is not in a pending state (current status: {money_request.status}). Cannot update.")
            # This indicates an attempt to re-process. The API layer should handle this, possibly with a 409 Conflict.
            return None 

        original_status = money_request.status
        money_request.status = new_status
        money_request.updated_at = datetime.datetime.utcnow() 

        if new_status == 1:  # Approved
            logger.info(f"Money request ID {request_id} approved by admin ID {admin_id if admin_id else 'N/A'}. Processing wallet update for user ID {money_request.user_id}.")
            
            amount_to_change = money_request.amount
            transaction_description = f"Money request ID {money_request.id} (type: {money_request.type}) approved by admin."
            
            if money_request.type == "withdraw":
                amount_to_change = -money_request.amount # Negative for withdrawal
            
            try:
                # This call to update_user_wallet_balance handles:
                # 1. Getting the User with a row-level lock (.with_for_update()).
                # 2. Performing balance calculations.
                # 3. Updating User.wallet_balance.
                # 4. Creating a Wallet record.
                # 5. All within its own nested transaction (or the one we started if not nested there).
                #    `crud_user.update_user_wallet_balance` should ideally use `db.begin_nested()`
                #    or rely on the caller (this function) to manage the transaction.
                #    Assuming `crud_user.update_user_wallet_balance` correctly handles its part of the transaction.
                
                updated_user = await crud_user.update_user_wallet_balance(
                    db=db, # Pass the current session
                    user_id=money_request.user_id,
                    amount=amount_to_change,
                    transaction_type=money_request.type, # 'deposit' or 'withdrawal'
                    description=transaction_description 
                    # Optional: pass symbol, order_quantity, order_type if relevant from MoneyRequest
                )
                
                # If update_user_wallet_balance itself raises an error (e.g., ValueError for insufficient funds),
                # the `db.begin_nested()` context manager in this function will catch it,
                # rollback changes (including the money_request.status update), and re-raise the exception.
                
                if not updated_user: # Should not happen if update_user_wallet_balance raises on failure
                    logger.error(f"Wallet balance update failed for user ID {money_request.user_id} for money request ID {request_id}, but no exception was raised by crud_user.")
                    # This indicates an issue in crud_user.update_user_wallet_balance's error handling.
                    # Explicitly rollback to be safe, though the context manager should handle it.
                    await db.rollback() 
                    return None 

                logger.info(f"Wallet balance updated successfully for user ID {money_request.user_id} due to money request ID {request_id} approval.")

            except ValueError as ve: # Specifically catch insufficient funds or other validation errors from wallet update
                logger.error(f"Processing approved money request ID {request_id} failed: {ve}", exc_info=True)
                # The `async with db.begin_nested():` will automatically rollback.
                # Re-raise to be handled by the API endpoint.
                raise ve 
            except Exception as e: # Catch any other unexpected errors during wallet processing
                logger.error(f"Unexpected error processing approved money request ID {request_id}: {e}", exc_info=True)
                # The `async with db.begin_nested():` will automatically rollback.
                # Re-raise.
                raise e

        elif new_status == 2: # Rejected
            logger.info(f"Money request ID {request_id} rejected by admin ID {admin_id if admin_id else 'N/A'}.")
            # No wallet action needed for rejection.
        
        # If new_status is 0 (back to requested), it's unusual for an admin action but handled.
        elif new_status == 0 and original_status != 0:
             logger.info(f"Money request ID {request_id} status changed to 'requested' by admin ID {admin_id if admin_id else 'N/A'}.")


        db.add(money_request) # Add to session to mark as dirty for the status update.
        # The commit for this nested transaction will happen when the `async with` block exits successfully.
        # If an exception occurred (like ValueError from wallet update), a rollback already happened.
    
    # Refresh to get the latest state from the DB, especially if committed.
    await db.refresh(money_request)
    return money_request
