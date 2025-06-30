from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database.models import CryptoPayment
from typing import Optional, Dict, Any

async def create_payment_record(db: AsyncSession, user_id: int, merchant_order_id: str, payment_data: Dict[str, Any]) -> CryptoPayment:
    db_payment = CryptoPayment(
        user_id=user_id,
        merchant_order_id=merchant_order_id,
        base_amount=payment_data['baseAmount'],
        base_currency=payment_data['baseCurrency'],
        settled_currency=payment_data['settledCurrency'],
        network_symbol=payment_data['networkSymbol'],
        status='PENDING'
    )
    db.add(db_payment)
    await db.commit()
    await db.refresh(db_payment)
    return db_payment

async def get_payment_by_merchant_order_id(db: AsyncSession, merchant_order_id: str) -> Optional[CryptoPayment]:
    result = await db.execute(select(CryptoPayment).filter(CryptoPayment.merchant_order_id == merchant_order_id))
    return result.scalars().first()

async def update_payment_status(db: AsyncSession, payment: CryptoPayment, status: str, details: Optional[Dict[str, Any]] = None) -> CryptoPayment:
    payment.status = status
    if details:
        payment.transaction_details = str(details)
    await db.commit()
    await db.refresh(payment)
    return payment 