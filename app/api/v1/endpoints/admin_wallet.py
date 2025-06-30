from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.core.security import get_current_admin_user
from app.database.models import User
from app.schemas.wallet import AdminWalletActionRequest, AdminWalletActionResponse
from app.crud.wallet import add_funds_to_wallet, withdraw_funds_from_wallet

router = APIRouter()

@router.post("/admin/wallet/add-funds", response_model=AdminWalletActionResponse)
async def admin_add_funds(
    req: AdminWalletActionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    balance = await add_funds_to_wallet(db, req.user_id, req.amount, req.currency, req.reason, by_admin=True)
    return AdminWalletActionResponse(status=True, message="Funds added successfully", balance=balance)

@router.post("/admin/wallet/withdraw-funds", response_model=AdminWalletActionResponse)
async def admin_withdraw_funds(
    req: AdminWalletActionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    try:
        wallet_balance = await withdraw_funds_from_wallet(db, req.user_id, req.amount, req.currency, req.reason, by_admin=True)
        return AdminWalletActionResponse(status=True, message="Funds withdrawn successfully", balance=wallet_balance)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 