# app/crud/otp.py
import datetime
import random
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete

from app.database.models import OTP, User
from app.core.config import get_settings

settings = get_settings()

def generate_otp_code(length: int = 6) -> str:
    return "".join(random.choices("0123456789", k=length))

async def create_otp(db: AsyncSession, user_id: int, force_otp_code: Optional[str] = None) -> OTP:
    await db.execute(delete(OTP).where(OTP.user_id == user_id))

    otp_code_to_use = force_otp_code if force_otp_code else generate_otp_code()
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=settings.OTP_EXPIRATION_MINUTES)

    db_otp = OTP(
        user_id=user_id,
        otp_code=otp_code_to_use,
        expires_at=expires_at
    )

    db.add(db_otp)
    await db.commit()
    await db.refresh(db_otp)

    return db_otp

async def get_valid_otp(db: AsyncSession, user_id: int, otp_code: str) -> Optional[OTP]:
    current_time = datetime.datetime.utcnow()
    query = select(OTP).filter(
        OTP.user_id == user_id,
        OTP.otp_code == otp_code,
        OTP.expires_at > current_time
    )

    result = await db.execute(query)
    return result.scalars().first()

async def delete_otp(db: AsyncSession, otp_id: int):
    await db.execute(delete(OTP).where(OTP.id == otp_id))
    await db.commit()

async def delete_all_user_otps(db: AsyncSession, user_id: int):
    await db.execute(delete(OTP).where(OTP.user_id == user_id))
    await db.commit()

# Helper to get user by email and user_type
async def get_user_by_email_and_type(db: AsyncSession, email: str, user_type: str) -> Optional[User]:
    result = await db.execute(
        select(User).filter(User.email == email, User.user_type == user_type)
    )
    return result.scalars().first()

# Redis OTP flag key format

def get_otp_flag_key(email: str, user_type: str) -> str:
    return f"otp_verified:{email}:{user_type}"
