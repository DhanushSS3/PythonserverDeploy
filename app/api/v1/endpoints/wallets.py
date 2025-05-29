from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.database.session import get_db
from app.core.security import get_current_user
from app.database.models import User
from app.schemas.wallet import WalletResponse  # Adjust if needed
from app.crud.wallet import get_wallet_records_by_user_id

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
    current_user: User = Depends(get_current_user)
):
    """
    Retrieves all wallet transaction records associated with the logged-in user.
    """
    wallet_records = await get_wallet_records_by_user_id(db=db, user_id=current_user.id)

    if not wallet_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No wallet records found for this user."
        )

    return wallet_records
