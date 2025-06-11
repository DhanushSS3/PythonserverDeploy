from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from sqlalchemy import select

from app.database.session import get_db
from app.core.security import get_current_user
from app.database.models import User, DemoUser, Wallet
from app.schemas.wallet import WalletResponse  # Adjust if needed
from app.crud.wallet import get_wallet_records_by_user_id, get_wallet_records_by_order_id, get_wallet_records_by_demo_user_id

router = APIRouter(
    prefix="/wallets",
    tags=["wallets"]
)

@router.get(
    "/my-wallets",
    response_model=List[WalletResponse],
    summary="Get all wallet records of the authenticated user",
    description="Fetches all wallet transaction records for the currently logged-in user."
)
async def get_my_wallets(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Retrieves all wallet transaction records associated with the logged-in user.
    """
    wallet_records = []
    
    # Check if the current user is a demo user or regular user
    if isinstance(current_user, DemoUser):
        wallet_records = await get_wallet_records_by_demo_user_id(db=db, demo_user_id=current_user.id)
    else:
        wallet_records = await get_wallet_records_by_user_id(db=db, user_id=current_user.id)

    if not wallet_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No wallet records found for this user."
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
