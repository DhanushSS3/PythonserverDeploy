# app/crud/crud_demo_user.py

from sqlalchemy.orm import Session
from sqlalchemy import or_
# Ensure DemoUser and OTP models are correctly imported from your models file
# from app.database.models import DemoUser, OTP
# For the purpose of this generation, I'll assume they are available.
# Replace with actual imports:
from app.database.models import DemoUser, OTP, User # User needed for type hints if functions are combined
from app.schemas.demo_user import DemoUserCreate # Import DemoUserCreate
from app.schemas.live_user import UserLogin # Re-using UserLogin schema for structure
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import secrets
import string
from decimal import Decimal
from datetime import datetime, timedelta
from app.core.logging_config import user_logger, error_logger
import uuid # Import uuid for generating unique tokens
from typing import Optional

# Password hashing context (can be shared with crud_live_user)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    """Hashes a password using bcrypt."""
    user_logger.debug("Hashing password for demo user.")
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password."""
    user_logger.debug("Verifying password for demo user.")
    return pwd_context.verify(plain_password, hashed_password)

async def _generate_unique_account_number_for_demo(db: Session, length: int = 5, max_attempts: int = 10) -> str:
    """
    Generates a unique alpha-numeric account number for demo users,
    ensuring it doesn't already exist in the DemoUser table.
    """
    characters = string.ascii_uppercase + string.digits
    for attempt in range(max_attempts):
        account_number = 'DEMO' + ''.join(secrets.choice(characters) for _ in range(length)) # Prefix for demo
        
        existing_user = await db.run_sync(
            lambda session: session.query(DemoUser)
            .filter(DemoUser.account_number == account_number)
            .with_for_update(nowait=True) 
            .first()
        )
        
        if not existing_user:
            user_logger.debug(f"Generated unique demo account number: {account_number} on attempt {attempt + 1}")
            return account_number
        user_logger.warning(f"Demo account number collision for '{account_number}', retrying. Attempt {attempt + 1}/{max_attempts}")
    
    error_message = f"Could not generate a unique demo account number after {max_attempts} attempts."
    user_logger.error(error_message)
    error_logger.critical(error_message)
    raise Exception(error_message)


async def create_demo_user(db: Session, user_data: DemoUserCreate) -> DemoUser:
    """
    Creates a new demo user in the database with default values.
    """
    user_logger.info(f"Starting demo user creation process for email: {user_data.email}")
    
    hashed_password = get_password_hash(user_data.password)

    try:
        account_number = await _generate_unique_account_number_for_demo(db)
    except Exception as e:
        user_logger.error(f"Failed to generate unique account number for demo user {user_data.email}: {e}", exc_info=True)
        raise

    default_status = 1  # Or 0 if OTP verification is mandatory for activation
    default_user_type = "demo"
    default_wallet_balance = Decimal("10000.00") # Demo users might get a default large sum
    default_net_profit = Decimal("0.00")
    default_leverage = Decimal("100.00")
    default_margin = Decimal("0.00")
    default_isActive = 0 # Demo users also start inactive until OTP verification, if implemented

    db_user = DemoUser(
        name=user_data.name,
        phone_number=user_data.phone_number,
        email=user_data.email,
        hashed_password=hashed_password,
        city=user_data.city,
        state=user_data.state,
        pincode=user_data.pincode,
        group_name=user_data.group,
        security_question=user_data.security_question,
        security_answer=user_data.security_answer,
        reffered_code=user_data.referral_code,
        status=default_status,
        user_type=default_user_type,
        account_number=account_number,
        wallet_balance=default_wallet_balance,
        net_profit=default_net_profit,
        leverage=default_leverage,
        margin=default_margin,
        isActive=default_isActive # Default from Pydantic model or explicit here
    )

    try:
        await db.run_sync(lambda session: session.add(db_user))
        await db.run_sync(lambda session: session.commit())
        await db.run_sync(lambda session: session.refresh(db_user))
        user_logger.info(f"Demo user {db_user.email} successfully added with ID: {db_user.id}")
        return db_user
    except Exception as e:
        await db.run_sync(lambda session: session.rollback())
        user_logger.error(f"Database transaction failed for demo user {user_data.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical database error during demo user creation for {user_data.email}: {e}", exc_info=True)
        raise

async def get_demo_user_by_username(db: Session, username: str) -> Optional[DemoUser]:
    """
    Fetches a demo user by email or phone number (user_type is implicitly "demo").
    """
    user_logger.debug(f"Attempting to fetch demo user by username: '{username}'")
    
    query = db.run_sync(
        lambda session: session.query(DemoUser).filter(
            DemoUser.user_type == "demo",
            or_(DemoUser.email == username, DemoUser.phone_number == username)
        ).first()
    )
    user = await query
    
    if user:
        user_logger.debug(f"Demo user found: ID {user.id}, Email: {user.email}")
    else:
        user_logger.debug(f"No demo user found for username: '{username}'")
    return user

async def get_demo_user_by_email(db: Session, email: str) -> Optional[DemoUser]:
    """
    Fetches a demo user by email (user_type is implicitly "demo").
    """
    user_logger.debug(f"Attempting to fetch demo user by email: '{email}'")
    user = await db.run_sync(
        lambda session: session.query(DemoUser)
        .filter(DemoUser.email == email, DemoUser.user_type == "demo")
        .first()
    )
    if user:
        user_logger.debug(f"Demo user found by email: ID {user.id}, Email: {user.email}")
    else:
        user_logger.debug(f"No demo user found for email: '{email}'")
    return user


async def authenticate_demo_user(db: Session, user_login_data: UserLogin) -> Optional[DemoUser]:
    """
    Authenticates a demo user based on username (email/phone) and password.
    Ensures user_type from input is "demo".
    """
    username = user_login_data.username
    password = user_login_data.password
    
    if user_login_data.user_type != "demo":
        user_logger.warning(f"Authentication attempt with incorrect user_type for demo: {user_login_data.user_type}")
        return None

    user_logger.info(f"Attempting to authenticate demo user: '{username}'")
    user = await get_demo_user_by_username(db, username)

    if not user:
        user_logger.warning(f"Authentication failed for demo user '{username}': User not found.")
        return None

    if not verify_password(password, user.hashed_password):
        user_logger.warning(f"Authentication failed for demo user '{username}': Invalid credentials.")
        return None

    if user.status != 1 or user.isActive != 1:
        user_logger.warning(f"Authentication failed for demo user '{username}': Account not active/verified (status={user.status}, isActive={user.isActive}).")
        return None 

    user_logger.info(f"Demo user '{username}' (ID: {user.id}) authenticated successfully.")
    return user

async def generate_and_store_otp_for_demo_user(db: Session, demo_user_id: int, otp_length: int = 6, otp_expire_minutes: int = 5) -> str:
    """Generates and stores OTP for a demo user, associating with OTP.demo_user_id."""
    otp_code = ''.join(secrets.choice(string.digits) for _ in range(otp_length))
    expires_at = datetime.utcnow() + timedelta(minutes=otp_expire_minutes)

    existing_otp = await db.run_sync(
        lambda session: session.query(OTP).filter(OTP.demo_user_id == demo_user_id).first()
    )

    if existing_otp:
        existing_otp.otp_code = otp_code
        existing_otp.created_at = datetime.utcnow()
        existing_otp.expires_at = expires_at
        existing_otp.reset_token = None
        user_logger.info(f"Updated OTP for demo_user_id {demo_user_id}.")
    else:
        new_otp = OTP(
            demo_user_id=demo_user_id, # Use demo_user_id
            otp_code=otp_code,
            created_at=datetime.utcnow(),
            expires_at=expires_at
        )
        await db.run_sync(lambda session: session.add(new_otp))
        user_logger.info(f"Created new OTP for demo_user_id {demo_user_id}.")
    
    await db.run_sync(lambda session: session.commit())
    return otp_code

async def verify_demo_user_otp(db: Session, demo_user_id: int, otp_code: str) -> bool:
    """Verifies OTP for demo user and activates account (sets isActive=1)."""
    otp_record = await db.run_sync(
        lambda session: session.query(OTP).filter(OTP.demo_user_id == demo_user_id).first()
    )

    if not otp_record or otp_record.otp_code != otp_code or datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"OTP verification failed for demo_user_id {demo_user_id}.")
        if otp_record and datetime.utcnow() > otp_record.expires_at: # Delete expired OTP
            await db.run_sync(lambda session: session.delete(otp_record))
            await db.run_sync(lambda session: session.commit())
        return False

    demo_user = await db.run_sync(
        lambda session: session.query(DemoUser).filter(DemoUser.id == demo_user_id).with_for_update(nowait=True).first()
    )
    if demo_user:
        demo_user.isActive = 1
        await db.run_sync(lambda session: session.add(demo_user))
        await db.run_sync(lambda session: session.delete(otp_record)) # Delete used OTP
        await db.run_sync(lambda session: session.commit())
        user_logger.info(f"Demo_user_id {demo_user_id} email verified. isActive set to 1. OTP deleted.")
        return True
    
    user_logger.error(f"Critical: DemoUser ID {demo_user_id} not found during OTP verification update.")
    return False

async def verify_otp_for_demo_password_reset(db: Session, demo_user_id: int, otp_code: str) -> Optional[str]:
    """Verifies OTP for demo user password reset and returns a reset_token."""
    otp_record = await db.run_sync(
        lambda session: session.query(OTP).filter(OTP.demo_user_id == demo_user_id).first()
    )

    if not otp_record or otp_record.otp_code != otp_code or datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"Password reset OTP verification failed for demo_user_id {demo_user_id}.")
        if otp_record and datetime.utcnow() > otp_record.expires_at:
            await db.run_sync(lambda session: session.delete(otp_record))
            await db.run_sync(lambda session: session.commit())
        return None

    reset_token = str(uuid.uuid4())
    otp_record.reset_token = reset_token
    otp_record.expires_at = datetime.utcnow() + timedelta(minutes=10) # Token validity
    await db.run_sync(lambda session: session.add(otp_record))
    await db.run_sync(lambda session: session.commit())
    user_logger.info(f"Password reset OTP verified for demo_user_id {demo_user_id}. Reset token generated.")
    return reset_token

async def validate_and_consume_reset_token_for_demo_user(db: Session, demo_user_id: int, reset_token: str) -> bool:
    """Validates and consumes reset_token for a demo user."""
    otp_record = await db.run_sync(
        lambda session: session.query(OTP)
        .filter(OTP.demo_user_id == demo_user_id, OTP.reset_token == reset_token)
        .first()
    )

    if not otp_record or datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"Reset token validation failed for demo_user_id {demo_user_id}.")
        if otp_record: # Expired
            await db.run_sync(lambda session: session.delete(otp_record))
            await db.run_sync(lambda session: session.commit())
        return False

    await db.run_sync(lambda session: session.delete(otp_record)) # Consume token
    await db.run_sync(lambda session: session.commit())
    user_logger.info(f"Reset token consumed for demo_user_id {demo_user_id}.")
    return True

async def update_demo_user_password(db: Session, demo_user: DemoUser, new_password: str) -> DemoUser:
    """Updates a demo user's password."""
    user_logger.info(f"Attempting to update password for demo_user_id: {demo_user.id}")
    hashed_password = get_password_hash(new_password)
    
    try:
        user_to_update = await db.run_sync(
            lambda session: session.query(DemoUser).filter(DemoUser.id == demo_user.id).with_for_update(nowait=True).first()
        )
        
        if not user_to_update:
            user_logger.error(f"DemoUser ID {demo_user.id} not found for password update.")
            raise Exception("DemoUser not found for password update.")

        user_to_update.hashed_password = hashed_password
        await db.run_sync(lambda session: session.add(user_to_update))
        await db.run_sync(lambda session: session.commit())
        await db.run_sync(lambda session: session.refresh(user_to_update))
        user_logger.info(f"Password updated for demo_user_id: {demo_user.id}")
        return user_to_update
    except Exception as e:
        await db.run_sync(lambda session: session.rollback())
        user_logger.error(f"Failed to update password for demo_user_id {demo_user.id}: {e}", exc_info=True)
        raise

async def get_user_by_id_with_lock(db: AsyncSession, user_id: int, model_class) -> Optional[User | DemoUser]:
    result = await db.execute(
        select(model_class)
        .filter(model_class.id == user_id)
        .with_for_update()
    )
    return result.scalars().first()

async def get_user_margin_by_id(db: AsyncSession, user_id: int, model_class) -> Optional[Decimal]:
    result = await db.execute(
        select(model_class.margin).filter(model_class.id == user_id)
    )
    return result.scalar_one_or_none()

async def get_user_by_email(db: AsyncSession, email: str, model_class) -> Optional[User | DemoUser]:
    result = await db.execute(select(model_class).filter(model_class.email == email))
    return result.scalars().first()

async def get_user_by_phone_number(db: AsyncSession, phone_number: str, model_class) -> Optional[User | DemoUser]:
    result = await db.execute(select(model_class).filter(model_class.phone_number == phone_number))
    return result.scalars().first()
