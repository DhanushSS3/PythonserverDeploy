from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from sqlalchemy import select

from app.database.session import get_db
from app.core.security import get_current_user
from app.database.models import User, DemoUser, Wallet
from app.schemas.wallet import WalletResponse, TotalDepositResponse  # Adjust if needed
from app.crud.wallet import get_wallet_records_by_user_id, get_wallet_records_by_order_id, get_wallet_records_by_demo_user_id, get_total_deposit_amount_for_live_user

router = APIRouter(
    prefix="/wallets",
    tags=["wallets"]
)

@router.get(
    "/my-wallets",
    response_model=List[WalletResponse],
    summary="Get all wallet records of the authenticated user",
    description="Fetches all wallet transaction records for the currently logged-in user, filtered to show only withdraw and deposit transactions."
)
async def get_my_wallets(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Retrieves all wallet transaction records associated with the logged-in user,
    filtered to show only withdraw and deposit transactions.
    """
    wallet_records = []
    
    # Define the transaction types to filter for
    transaction_types = ["withdraw", "deposit"]
    
    # Check if the current user is a demo user or regular user
    if isinstance(current_user, DemoUser):
        wallet_records = await get_wallet_records_by_demo_user_id(
            db=db, 
            demo_user_id=current_user.id,
            transaction_types=transaction_types
        )
    else:
        wallet_records = await get_wallet_records_by_user_id(
            db=db, 
            user_id=current_user.id,
            transaction_types=transaction_types
        )

    if not wallet_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No withdraw or deposit wallet records found for this user."
        )

    return wallet_records

@router.get(
    "/order/{order_id}",
    response_model=List[WalletResponse],
    summary="Get wallet records for a specific order",
    description="Fetches all wallet transaction records related to a specific order."
)
async def get_wallet_records_by_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Retrieves all wallet transaction records associated with a specific order.
    """
    # Use the CRUD function to get wallet records by order_id for the current user
    wallet_records = []
    
    # Check if the current user is a demo user or regular user
    if isinstance(current_user, DemoUser):
        wallet_records = await get_wallet_records_by_order_id(
            db=db, 
            order_id=order_id, 
            demo_user_id=current_user.id
        )
    else:
        wallet_records = await get_wallet_records_by_order_id(
            db=db, 
            order_id=order_id, 
            user_id=current_user.id
        )

    if not wallet_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No wallet records found for order {order_id}."
        )

    return wallet_records

@router.get(
    "/total-deposits",
    response_model=TotalDepositResponse,
    summary="Get total deposit amount for live user",
    description="Calculates and returns the total amount deposited by the authenticated live user."
)
async def get_total_deposits(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Retrieves the total deposit amount for the authenticated live user.
    Only accessible for live users (not demo users).
    """
    # Ensure this endpoint is only accessible for live users
    if isinstance(current_user, DemoUser):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint is only accessible for live users."
        )
    
    # Get total deposit amount for the live user
    total_deposit_amount = await get_total_deposit_amount_for_live_user(
        db=db, 
        user_id=current_user.id
    )
    
    return TotalDepositResponse(
        user_id=current_user.id,
        total_deposit_amount=total_deposit_amount,
        message="Total deposit amount retrieved successfully"
    )
