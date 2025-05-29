# app/crud/otp.py
import datetime
import random
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete

from app.database.models import OTP, User, DemoUser # Import DemoUser
from app.core.config import get_settings

settings = get_settings()

def generate_otp_code(length: int = 6) -> str:
    return "".join(random.choices("0123456789", k=length))

async def create_otp(
    db: AsyncSession,
    user_id: Optional[int] = None,
    demo_user_id: Optional[int] = None,
    force_otp_code: Optional[str] = None
) -> OTP:
    """
    Creates a new OTP record, associating it with either a regular user or a demo user.
    Deletes any existing OTPs for the specified user/demo user.
    """
    if user_id and demo_user_id:
        raise ValueError("OTP record cannot be associated with both a user and a demo user.")
    if not user_id and not demo_user_id:
        raise ValueError("OTP record must be associated with either a user or a demo user.")

    # Delete existing OTPs for the specified user/demo user
    if user_id:
        await db.execute(delete(OTP).where(OTP.user_id == user_id))
    elif demo_user_id:
        await db.execute(delete(OTP).where(OTP.demo_user_id == demo_user_id))

    otp_code_to_use = force_otp_code if force_otp_code else generate_otp_code()
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=settings.OTP_EXPIRATION_MINUTES)

    db_otp = OTP(
        user_id=user_id,
        demo_user_id=demo_user_id, # Assign demo_user_id if present
        otp_code=otp_code_to_use,
        expires_at=expires_at
    )

    db.add(db_otp)
    await db.commit()
    await db.refresh(db_otp)

    return db_otp

async def get_valid_otp(
    db: AsyncSession,
    otp_code: str,
    user_id: Optional[int] = None,
    demo_user_id: Optional[int] = None
) -> Optional[OTP]:
    """
    Retrieves a valid OTP for a given user_id or demo_user_id and OTP code.
    """
    if user_id and demo_user_id:
        raise ValueError("Cannot validate OTP for both a user and a demo user simultaneously.")
    if not user_id and not demo_user_id:
        raise ValueError("OTP validation requires either a user_id or a demo_user_id.")

    current_time = datetime.datetime.utcnow()
    query = select(OTP).filter(
        OTP.otp_code == otp_code,
        OTP.expires_at > current_time
    )

    if user_id:
        query = query.filter(OTP.user_id == user_id)
    elif demo_user_id:
        query = query.filter(OTP.demo_user_id == demo_user_id)

    result = await db.execute(query)
    return result.scalars().first()

async def delete_otp(db: AsyncSession, otp_id: int):
    """
    Deletes an OTP record by its ID.
    """
    await db.execute(delete(OTP).where(OTP.id == otp_id))
    await db.commit()

async def delete_all_user_otps(db: AsyncSession, user_id: int):
    """
    Deletes all OTP records for a specific regular user.
    """
    await db.execute(delete(OTP).where(OTP.user_id == user_id))
    await db.commit()

async def delete_all_demo_user_otps(db: AsyncSession, demo_user_id: int):
    """
    Deletes all OTP records for a specific demo user.
    """
    await db.execute(delete(OTP).where(OTP.demo_user_id == demo_user_id))
    await db.commit()

# Helper to get user by email and user_type (kept for existing functionality, if any)
async def get_user_by_email_and_type(db: AsyncSession, email: str, user_type: str) -> Optional[User]:
    result = await db.execute(
        select(User).filter(User.email == email, User.user_type == user_type)
    )
    return result.scalars().first()

# Redis OTP flag key format (kept as is)
def get_otp_flag_key(email: str, user_type: str) -> str:
    return f"otp_verified:{email}:{user_type}"
