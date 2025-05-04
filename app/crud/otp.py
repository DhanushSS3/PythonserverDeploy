# app/crud/otp.py

import datetime
import random
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete # Import delete

from app.database.models import OTP, User # Import OTP and User models
from app.core.config import get_settings # Import settings

settings = get_settings()

# Function to generate a random OTP code
def generate_otp_code(length: int = 6) -> str:
    """
    Generates a random numeric OTP code of specified length.
    """
    return "".join(random.choices("0123456789", k=length))

# Function to create a new OTP record
async def create_otp(db: AsyncSession, user_id: int) -> OTP:
    """
    Creates a new OTP record for a user, deleting any existing ones.

    Args:
        db: The asynchronous database session.
        user_id: The ID of the user to create the OTP for.

    Returns:
        The newly created OTP SQLAlchemy model instance.
    """
    # Delete any existing OTPs for this user to ensure only one is active
    await db.execute(delete(OTP).where(OTP.user_id == user_id))
    await db.commit() # Commit the deletion

    # Generate OTP and calculate expiry time
    otp_code = generate_otp_code()
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=settings.OTP_EXPIRATION_MINUTES)

    # Create the new OTP record
    db_otp = OTP(
        user_id=user_id,
        otp_code=otp_code,
        expires_at=expires_at
    )

    db.add(db_otp)
    await db.commit()
    await db.refresh(db_otp)

    return db_otp

# Function to get a valid OTP for a user
async def get_valid_otp(db: AsyncSession, user_id: int, otp_code: str) -> OTP | None:
    """
    Retrieves a valid (non-expired) OTP record for a user.

    Args:
        db: The asynchronous database session.
        user_id: The ID of the user.
        otp_code: The OTP code provided by the user.

    Returns:
        The OTP object if found and valid, otherwise None.
    """
    current_time = datetime.datetime.utcnow()
    result = await db.execute(
        select(OTP).filter(
            OTP.user_id == user_id,
            OTP.otp_code == otp_code,
            OTP.expires_at > current_time # Check if the OTP has not expired
        )
    )
    return result.scalars().first()

# Function to delete an OTP record
async def delete_otp(db: AsyncSession, otp_id: int):
    """
    Deletes an OTP record by its ID.

    Args:
        db: The asynchronous database session.
        otp_id: The ID of the OTP record to delete.
    """
    await db.execute(delete(OTP).where(OTP.id == otp_id))
    await db.commit()
