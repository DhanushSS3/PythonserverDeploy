# app/crud/crud_live_user.py

from sqlalchemy.orm import Session
from app.database.models import DemoUser, OTP, User
from sqlalchemy import or_
from app.database.models import User, OTP # Import OTP model
from app.schemas.live_user import UserCreate, UserLogin # Import UserCreate and UserLogin from the schemas file
from passlib.context import CryptContext
import secrets
import string
from decimal import Decimal
from datetime import datetime, timedelta
from app.core.logging_config import user_logger, error_logger
import uuid # Import uuid for generating unique tokens
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    """Hashes a password using bcrypt."""
    user_logger.debug("Hashing password.")
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password."""
    user_logger.debug("Verifying password.")
    return pwd_context.verify(plain_password, hashed_password)

async def _generate_unique_account_number(db: Session, length: int = 5, max_attempts: int = 10) -> str:
    """
    Generates a unique alpha-numeric account number, ensuring it doesn't
    already exist in the database, using row-level locking during check.
    """
    characters = string.ascii_uppercase + string.digits
    for attempt in range(max_attempts):
        account_number = ''.join(secrets.choice(characters) for _ in range(length))
        
        # Check if the generated account number already exists with a row-level lock
        # This prevents a race condition where two concurrent transactions might try to insert
        # the same account number if the check is not atomic.
        existing_user = await db.run_sync(
            lambda session: session.query(User)
            .filter(User.account_number == account_number)
            .with_for_update(nowait=True) # Use nowait=True to raise error if lock cannot be acquired immediately
            .first()
        )
        
        if not existing_user:
            user_logger.debug(f"Generated unique account number: {account_number} on attempt {attempt + 1}")
            return account_number
        user_logger.warning(f"Account number collision detected for '{account_number}', retrying. Attempt {attempt + 1}/{max_attempts}")
    
    error_message = f"Could not generate a unique account number after {max_attempts} attempts."
    user_logger.error(error_message)
    error_logger.critical(error_message)
    raise Exception(error_message)


async def create_user(db: Session, user_data: UserCreate) -> User:
    """
    Creates a new live user in the database with default values.
    Generates a unique account number and ensures transaction safety.
    """
    user_logger.info(f"Starting user creation process for email: {user_data.email}")
    
    # Hash the password (synchronous operation, no await needed)
    hashed_password = get_password_hash(user_data.password)

    # Generate a unique account number
    try:
        account_number = await _generate_unique_account_number(db)
    except Exception as e:
        user_logger.error(f"Failed to generate unique account number for {user_data.email}: {e}", exc_info=True)
        raise # Re-raise to be caught by the endpoint

    # Set default values for new live users
    default_status = 1
    default_user_type = "live"
    default_wallet_balance = Decimal("0.00")
    default_net_profit = Decimal("0.00")
    default_leverage = Decimal("100.00")
    default_margin = Decimal("0.00")

    # Create the new User object
    db_user = User(
        name=user_data.name,
        phone_number=user_data.phone_number,
        email=user_data.email,
        hashed_password=hashed_password,
        city=user_data.city,
        state=user_data.state,
        pincode=user_data.pincode,
        group_name=user_data.group,
        bank_account_number=user_data.bank_account_number,
        bank_ifsc_code=user_data.bank_ifsc_code,
        bank_holder_name=user_data.bank_holder_name,
        bank_branch_name=user_data.bank_branch_name,
        security_question=user_data.security_question,
        security_answer=user_data.security_answer,
        address_proof=user_data.address_proof,
        address_proof_image=user_data.address_proof_image,
        id_proof=user_data.id_proof,
        id_proof_image=user_data.id_proof_image,
        is_self_trading=user_data.is_self_trading,
        fund_manager=user_data.fund_manager,
        isActive=user_data.isActive, # isActive comes from the Pydantic model
        reffered_code=user_data.referral_code,
        status=default_status, # status is 1 by default for registered users
        user_type=default_user_type,
        account_number=account_number,
        wallet_balance=default_wallet_balance,
        net_profit=default_net_profit,
        leverage=default_leverage,
        margin=default_margin
    )

    try:
        # Add to session and commit within a synchronous block
        await db.run_sync(lambda session: session.add(db_user))
        await db.run_sync(lambda session: session.commit())
        await db.run_sync(lambda session: session.refresh(db_user))
        user_logger.info(f"User {db_user.email} successfully added to database with ID: {db_user.id}")
        return db_user
    except Exception as e:
        await db.run_sync(lambda session: session.rollback()) # Rollback in case of an error during commit
        user_logger.error(f"Database transaction failed for user {user_data.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical database error during user creation for {user_data.email}: {e}", exc_info=True)
        raise # Re-raise to be caught by the endpoint

async def get_user_by_username(db: Session, username: str, user_type: Optional[str] = "live") -> Optional[User]:
    """
    Fetches a user by email or phone number and user_type asynchronously.
    """
    user_logger.debug(f"Attempting to fetch user by username: '{username}' with user_type: '{user_type}'")
    
    user = None
    if '@' in username: # Likely an email
        user = await db.run_sync(
            lambda session: session.query(User).filter(
                User.email == username,
                User.user_type == user_type
            ).first()
        )
    else: # Assume it's a phone number
        user = await db.run_sync(
            lambda session: session.query(User).filter(
                User.phone_number == username,
                User.user_type == user_type
            ).first()
        )
    
    if user:
        user_logger.debug(f"User found: ID {user.id}, Email: {user.email}")
    else:
        user_logger.debug(f"No user found for username: '{username}' with user_type: '{user_type}'")
    return user

async def get_user_by_email_and_type(db: Session, email: str, user_type: Optional[str] = "live") -> Optional[User]:
    """
    Fetches a user by email and user_type asynchronously.
    """
    user_logger.debug(f"Attempting to fetch user by email: '{email}' with user_type: '{user_type}'")
    user = await db.run_sync(
        lambda session: session.query(User)
        .filter(User.email == email, User.user_type == user_type)
        .first()
    )
    if user:
        user_logger.debug(f"User found by email: ID {user.id}, Email: {user.email}")
    else:
        user_logger.debug(f"No user found for email: '{email}' with user_type: '{user_type}'")
    return user


async def authenticate_user(db: Session, user_login_data: UserLogin) -> Optional[User]:
    """
    Authenticates a user based on username (email/phone) and password asynchronously.
    Returns the user object if authenticated and verified, otherwise None.
    The endpoint will then determine the specific error message based on status/isActive.
    """
    username = user_login_data.username
    password = user_login_data.password
    user_type = user_login_data.user_type

    user_logger.info(f"Attempting to authenticate user: '{username}' (Type: {user_type})")

    user = await get_user_by_username(db, username, user_type)

    if not user:
        user_logger.warning(f"Authentication failed for '{username}': User not found.")
        return None # User not found

    # Verify password (synchronous operation, no await needed)
    if not verify_password(password, user.hashed_password):
        user_logger.warning(f"Authentication failed for '{username}': Invalid credentials (password mismatch).")
        return None # Invalid password

    # Log specific status for debugging, but let the endpoint handle the specific HTTP error
    if user.status == 0:
        user_logger.warning(f"Authentication failed for '{username}': User blocked (status=0).")
    elif user.status != 1:
        user_logger.warning(f"Authentication failed for '{username}': Invalid status ({user.status}).")

    if user.isActive == 0:
        user_logger.warning(f"Authentication failed for '{username}': User email not verified (isActive=0).")
    elif user.isActive != 1:
        user_logger.warning(f"Authentication failed for '{username}': Invalid isActive status ({user.isActive}).")

    # If status is not 1 OR isActive is not 1, return None to indicate failure
    if user.status != 1 or user.isActive != 1:
        return None # User not fully verified/active/blocked

    user_logger.info(f"User '{username}' (ID: {user.id}) authenticated successfully.")
    return user

async def generate_and_store_otp(db: Session, user_id: int, otp_length: int = 6, otp_expire_minutes: int = 5) -> str:
    """
    Generates a new OTP, stores it in the OTP table, and sets its expiry.
    If an OTP already exists for the user, it updates it.
    """
    otp_code = ''.join(secrets.choice(string.digits) for _ in range(otp_length))
    expires_at = datetime.utcnow() + timedelta(minutes=otp_expire_minutes)

    # Check if an OTP already exists for this user
    existing_otp = await db.run_sync(
        lambda session: session.query(OTP)
        .filter(OTP.user_id == user_id)
        .first()
    )

    if existing_otp:
        # Update existing OTP
        existing_otp.otp_code = otp_code
        existing_otp.created_at = datetime.utcnow()
        existing_otp.expires_at = expires_at
        existing_otp.reset_token = None # Clear any previous reset token
        user_logger.info(f"Updated OTP for user ID {user_id}.")
    else:
        # Create new OTP
        new_otp = OTP(
            user_id=user_id,
            otp_code=otp_code,
            created_at=datetime.utcnow(),
            expires_at=expires_at,
            reset_token=None # No reset token initially
        )
        await db.run_sync(lambda session: session.add(new_otp))
        user_logger.info(f"Created new OTP for user ID {user_id}.")
    
    await db.run_sync(lambda session: session.commit())
    return otp_code

async def verify_user_otp(db: Session, user_id: int, otp_code: str) -> bool:
    """
    Verifies the provided OTP for a given user (for signup/email verification).
    Returns True if OTP is valid and not expired, False otherwise.
    Also updates user's isActive status to 1 on successful verification.
    """
    otp_record = await db.run_sync(
        lambda session: session.query(OTP)
        .filter(OTP.user_id == user_id)
        .first()
    )

    if not otp_record:
        user_logger.warning(f"OTP verification failed for user ID {user_id}: No OTP record found.")
        return False # No OTP sent or found

    if otp_record.otp_code != otp_code:
        user_logger.warning(f"OTP verification failed for user ID {user_id}: OTP mismatch.")
        return False # OTP does not match

    if datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"OTP verification failed for user ID {user_id}: OTP expired.")
        # Optionally delete expired OTP here
        await db.run_sync(lambda session: session.delete(otp_record))
        await db.run_sync(lambda session: session.commit())
        return False # OTP expired

    # OTP is valid, update user's isActive status to 1
    user = await db.run_sync(
        lambda session: session.query(User)
        .filter(User.id == user_id)
        .with_for_update(nowait=True) # Acquire lock to prevent race conditions during update
        .first()
    )
    if user:
        user.isActive = 1 # Set isActive to 1
        await db.run_sync(lambda session: session.add(user)) # Add to session for update
        await db.run_sync(lambda session: session.commit())
        user_logger.info(f"User ID {user_id} email verified successfully. isActive set to 1.")
    else:
        user_logger.error(f"Critical: User ID {user_id} not found during OTP verification update.")
        error_logger.critical(f"User ID {user_id} not found during OTP verification update.")
        return False

    # Delete the used OTP record
    await db.run_sync(lambda session: session.delete(otp_record))
    await db.run_sync(lambda session: session.commit())
    user_logger.info(f"OTP record deleted for user ID {user_id}.")

    return True

async def verify_otp_for_password_reset(db: Session, user_id: int, otp_code: str) -> Optional[str]:
    """
    Verifies the provided OTP for a given user (for password reset).
    Returns a unique reset_token if OTP is valid and not expired, None otherwise.
    Does NOT modify user's isActive status.
    """
    otp_record = await db.run_sync(
        lambda session: session.query(OTP)
        .filter(OTP.user_id == user_id)
        .first()
    )

    if not otp_record:
        user_logger.warning(f"Password reset OTP verification failed for user ID {user_id}: No OTP record found.")
        return None # No OTP sent or found

    if otp_record.otp_code != otp_code:
        user_logger.warning(f"Password reset OTP verification failed for user ID {user_id}: OTP mismatch.")
        return None # OTP does not match

    if datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"Password reset OTP verification failed for user ID {user_id}: OTP expired.")
        # Delete expired OTP
        await db.run_sync(lambda session: session.delete(otp_record))
        await db.run_sync(lambda session: session.commit())
        return None # OTP expired

    # OTP is valid. Generate and store a unique reset token.
    reset_token = str(uuid.uuid4()) # Generate a UUID as the reset token
    otp_record.reset_token = reset_token
    # Extend OTP expiry slightly for the reset token validity, or manage separately
    otp_record.expires_at = datetime.utcnow() + timedelta(minutes=10) # Token valid for 10 minutes
    await db.run_sync(lambda session: session.add(otp_record))
    await db.run_sync(lambda session: session.commit())
    user_logger.info(f"Password reset OTP successfully verified for user ID {user_id}. Reset token generated.")

    return reset_token

async def validate_and_consume_reset_token(db: Session, user_id: int, reset_token: str) -> bool:
    """
    Validates the provided reset_token for a given user and consumes it.
    Returns True if the token is valid and not expired, False otherwise.
    Consumes the token by deleting it from the database.
    """
    otp_record = await db.run_sync(
        lambda session: session.query(OTP)
        .filter(OTP.user_id == user_id, OTP.reset_token == reset_token)
        .first()
    )

    if not otp_record:
        user_logger.warning(f"Reset token validation failed for user ID {user_id}: Token not found or already used.")
        return False

    if datetime.utcnow() > otp_record.expires_at:
        user_logger.warning(f"Reset token validation failed for user ID {user_id}: Token expired.")
        # Delete expired token
        await db.run_sync(lambda session: session.delete(otp_record))
        await db.run_sync(lambda session: session.commit())
        return False

    # Token is valid. Consume it by deleting the record.
    await db.run_sync(lambda session: session.delete(otp_record))
    await db.run_sync(lambda session: session.commit())
    user_logger.info(f"Reset token successfully consumed for user ID {user_id}.")
    return True


async def update_user_password(db: Session, user: User, new_password: str) -> User:
    """
    Updates a user's password in the database.
    """
    user_logger.info(f"Attempting to update password for user ID: {user.id}, email: {user.email}")
    hashed_password = get_password_hash(new_password)
    
    try:
        # Acquire a lock on the user row before updating
        user_to_update = await db.run_sync(
            lambda session: session.query(User)
            .filter(User.id == user.id)
            .with_for_update(nowait=True)
            .first()
        )
        
        if not user_to_update:
            user_logger.error(f"User ID {user.id} not found for password update.")
            raise Exception("User not found for password update.")

        user_to_update.hashed_password = hashed_password
        await db.run_sync(lambda session: session.add(user_to_update))
        await db.run_sync(lambda session: session.commit())
        await db.run_sync(lambda session: session.refresh(user_to_update))
        user_logger.info(f"Password successfully updated for user ID: {user.id}")
        return user_to_update
    except Exception as e:
        await db.run_sync(lambda session: session.rollback())
        user_logger.error(f"Failed to update password for user ID {user.id}: {e}", exc_info=True)
        error_logger.critical(f"Critical error updating password for user ID {user.id}: {e}", exc_info=True)
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
