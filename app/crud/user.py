# app/crud/user.py

from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from decimal import Decimal

from app.database.models import User, DemoUser # Import both User and DemoUser models
from app.schemas.user import UserCreate, UserUpdate # Import UserCreate and UserUpdate
from app.schemas.demo_user import DemoUserCreate, DemoUserUpdate # Assuming you'll create these schemas
from app.schemas.wallet import WalletCreate
from app.schemas.otp import OTPCreate # Assuming you'll create this schema
from app.core.security import get_password_hash # Import the password hashing utility

import random
import string

# --- CRUD Operations for User Model ---

async def update_user_margin(db: AsyncSession, user_id: int, user_type: str, new_margin) -> None:
    """
    Updates only the margin field for a user (live or demo) by user_id and user_type.
    """
    from app.database.models import User, DemoUser
    model = User if user_type == 'live' else DemoUser
    result = await db.execute(select(model).filter(model.id == user_id))
    db_user = result.scalars().first()
    if db_user is not None:
        db_user.margin = new_margin
        await db.commit()
        await db.refresh(db_user)

async def get_user_by_account_number(db: AsyncSession, account_number: str, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their account_number AND user_type.
    """
    result = await db.execute(
        select(User).filter(User.account_number == account_number, User.user_type == user_type)
    )
    return result.scalars().first()

async def get_demo_user_by_account_number(db: AsyncSession, account_number: str, user_type: str = "demo") -> DemoUser | None:
    """
    Retrieves a demo user from the database by their account_number AND user_type.
    """
    result = await db.execute(
        select(DemoUser).filter(DemoUser.account_number == account_number, DemoUser.user_type == user_type)
    )
    return result.scalars().first()

async def get_user(db: AsyncSession, user_id: int) -> Optional[User]:
    """
    Retrieves a user from the database by their ID.
    """
    result = await db.execute(select(User).filter(User.id == user_id))
    return result.scalars().first()

async def get_user_by_id(db: AsyncSession, user_id: int, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their ID AND user_type (must match both).
    Args:
        db: The asynchronous database session.
        user_id: The ID to search for.
        user_type: The user type to match (e.g., 'live').
    Returns:
        The User SQLAlchemy model instance if found, otherwise None.
    """
    result = await db.execute(select(User).filter(User.id == user_id, User.user_type == user_type))
    return result.scalars().first()

async def get_user_by_id_with_lock(db: AsyncSession, user_id: int) -> User | None:
    """
    Retrieves a user from the database by their ID with a row-level lock.
    Use this when updating sensitive fields like wallet balance or margin.
    Includes the user's margin and wallet_balance.
    """
    result = await db.execute(
        select(User)
        .filter(User.id == user_id)
        .with_for_update()
    )
    return result.scalars().first()

async def get_user_margin_by_id(db: AsyncSession, user_id: int) -> Optional[Decimal]:
    """
    Retrieves only the margin value for a specific user from the database.
    """
    result = await db.execute(
        select(User.margin)
        .filter(User.id == user_id)
    )
    return result.scalar_one_or_none()

async def get_all_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all users from the database with pagination.
    """
    result = await db.execute(select(User).offset(skip).limit(limit))
    return result.scalars().all()

async def get_live_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all live users from the database with pagination.
    """
    result = await db.execute(
        select(User)
        .filter(User.user_type == 'live')
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    """
    Retrieves a user from the database by their email address.
    """
    result = await db.execute(select(User).filter(User.email == email))
    return result.scalars().first()

async def get_user_by_email_and_type(db: AsyncSession, email: str, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their email address and user_type.
    """
    result = await db.execute(
        select(User).filter(User.email == email, User.user_type == user_type)
    )
    return result.scalars().first()

async def get_user_by_phone_number(db: AsyncSession, phone_number: str) -> User | None:
    """
    Retrieves a user from the database by their phone number.
    """
    result = await db.execute(select(User).filter(User.phone_number == phone_number))
    return result.scalars().first()

async def get_user_by_phone_number_and_type(db: AsyncSession, phone_number: str, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their phone number and user_type.
    """
    result = await db.execute(
        select(User).filter(User.phone_number == phone_number, User.user_type == user_type)
    )
    return result.scalars().first()

async def get_user_by_email_phone_type(db: AsyncSession, email: str, phone_number: str, user_type: str) -> Optional[User]:
    """
    Retrieves a user from the database by their email, phone number, and user type.
    """
    result = await db.execute(
        select(User).filter(
            User.email == email,
            User.phone_number == phone_number,
            User.user_type == user_type
        )
    )
    return result.scalars().first()

async def create_user(
    db: AsyncSession,
    user_data: dict,
    hashed_password: str,
    id_proof_path: Optional[str] = None,
    id_proof_image_path: Optional[str] = None,
    address_proof_path: Optional[str] = None,
    address_proof_image_path: Optional[str] = None
) -> User:
    """
    Creates a new user in the database with optional proof types and file paths.
    """
    db_user = User(
        **user_data,
        hashed_password=hashed_password
    )

    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

async def update_user(db: AsyncSession, db_user: User, user_update: UserUpdate) -> User:
    """
    Updates an existing user in the database.
    """
    update_data = user_update.model_dump(exclude_unset=True)

    for field, value in update_data.items():
        setattr(db_user, field, value)

    await db.commit()
    await db.refresh(db_user)
    return db_user

async def delete_user(db: AsyncSession, db_user: User):
    """
    Deletes a user from the database.
    """
    await db.delete(db_user)
    await db.commit()

async def update_user_wallet_balance(
    db: AsyncSession,
    user_id: int,
    amount: Decimal,
    transaction_type: str,
    description: Optional[str] = None,
    symbol: Optional[str] = None,
    order_quantity: Optional[Decimal] = None,
    order_type: Optional[str] = None,
) -> User:
    """
    Updates a user's wallet balance by adding or deducting an amount.
    Applies row-level locking to ensure data consistency.
    Also creates a corresponding wallet transaction record.
    """
    # Get user with row-level lock
    user = await get_user_by_id_with_lock(db, user_id)

    if not user:
        raise ValueError(f"User with ID {user_id} not found.")

    current_balance = user.wallet_balance
    amount_decimal = Decimal(str(amount))

    new_balance = current_balance + amount_decimal

    if new_balance < 0:
        raise ValueError("Insufficient funds for this deduction.")

    user.wallet_balance = new_balance
    db.add(user)

    from app.crud.wallet import create_wallet_record # Import here to avoid circular dependency

    wallet_data = WalletCreate(
        user_id=user_id,
        transaction_type=transaction_type,
        transaction_amount=abs(amount_decimal),
        description=description,
        is_approved=1,
        symbol=symbol,
        order_quantity=order_quantity,
        order_type=order_type
    )
    wallet_record = await create_wallet_record(db, wallet_data)

    if not wallet_record:
        raise Exception("Failed to create wallet transaction record.")

    await db.refresh(user)
    return user

async def generate_unique_account_number(db: AsyncSession) -> str:
    """
    Generate a unique 5-character alphanumeric account number for a User.
    """
    while True:
        account_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        existing = await db.execute(
            select(User).filter(User.account_number == account_number)
        )
        if not existing.scalars().first():
            return account_number

# --- CRUD Operations for DemoUser Model ---

async def get_demo_user(db: AsyncSession, demo_user_id: int) -> Optional[DemoUser]:
    """
    Retrieves a demo user from the database by their ID.
    """
    result = await db.execute(select(DemoUser).filter(DemoUser.id == demo_user_id))
    return result.scalars().first()

async def get_demo_user_by_id(db: AsyncSession, demo_user_id: int, user_type: str = "demo") -> DemoUser | None:
    """
    Retrieves a demo user from the database by their ID AND user_type (must match both).
    Args:
        db: The asynchronous database session.
        demo_user_id: The ID to search for.
        user_type: The user type to match (default: 'demo').
    Returns:
        The DemoUser SQLAlchemy model instance if found, otherwise None.
    """
    result = await db.execute(select(DemoUser).filter(DemoUser.id == demo_user_id, DemoUser.user_type == user_type))
    return result.scalars().first()

async def get_demo_user_by_id_with_lock(db: AsyncSession, demo_user_id: int) -> DemoUser | None:
    """
    Retrieves a demo user from the database by their ID with a row-level lock.
    Use this when updating sensitive fields like wallet balance or margin for demo users.
    """
    result = await db.execute(
        select(DemoUser)
        .filter(DemoUser.id == demo_user_id)
        .with_for_update()
    )
    return result.scalars().first()

async def get_all_demo_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[DemoUser]:
    """
    Retrieves a list of all demo users from the database with pagination.
    """
    result = await db.execute(select(DemoUser).offset(skip).limit(limit))
    return result.scalars().all()

async def get_demo_user_by_email(db: AsyncSession, email: str) -> DemoUser | None:
    """
    Retrieves a demo user from the database by their email address.
    """
    result = await db.execute(select(DemoUser).filter(DemoUser.email == email))
    return result.scalars().first()

async def get_demo_user_by_phone_number(db: AsyncSession, phone_number: str) -> DemoUser | None:
    """
    Retrieves a demo user from the database by their phone number.
    """
    result = await db.execute(select(DemoUser).filter(DemoUser.phone_number == phone_number))
    return result.scalars().first()

async def create_demo_user(
    db: AsyncSession,
    demo_user_data: dict,
    hashed_password: str,
) -> DemoUser:
    """
    Creates a new demo user in the database.
    """
    db_demo_user = DemoUser(
        **demo_user_data,
        hashed_password=hashed_password,
    )

    db.add(db_demo_user)
    await db.commit()
    await db.refresh(db_demo_user)
    return db_demo_user

async def update_demo_user(db: AsyncSession, db_demo_user: DemoUser, demo_user_update: DemoUserUpdate) -> DemoUser:
    """
    Updates an existing demo user in the database.
    """
    update_data = demo_user_update.model_dump(exclude_unset=True)

    for field, value in update_data.items():
        setattr(db_demo_user, field, value)

    await db.commit()
    await db.refresh(db_demo_user)
    return db_demo_user

async def delete_demo_user(db: AsyncSession, db_demo_user: DemoUser):
    """
    Deletes a demo user from the database.
    """
    await db.delete(db_demo_user)
    await db.commit()

async def update_demo_user_wallet_balance(
    db: AsyncSession,
    demo_user_id: int,
    amount: Decimal,
    transaction_type: str,
    description: Optional[str] = None,
    symbol: Optional[str] = None,
    order_quantity: Optional[Decimal] = None,
    order_type: Optional[str] = None,
) -> DemoUser:
    """
    Updates a demo user's wallet balance by adding or deducting an amount.
    Applies row-level locking to ensure data consistency.
    Also creates a corresponding wallet transaction record for the demo user.
    """
    async with db.begin_nested():
        demo_user = await get_demo_user_by_id_with_lock(db, demo_user_id)

        if not demo_user:
            raise ValueError(f"Demo User with ID {demo_user_id} not found.")

        current_balance = demo_user.wallet_balance
        amount_decimal = Decimal(str(amount))

        new_balance = current_balance + amount_decimal

        if new_balance < 0:
            raise ValueError("Insufficient funds for this deduction for demo user.")

        demo_user.wallet_balance = new_balance
        db.add(demo_user)

        from app.crud.wallet import create_wallet_record # Import here to avoid circular dependency

        wallet_data = WalletCreate(
            demo_user_id=demo_user_id, # Link to demo_user_id
            transaction_type=transaction_type,
            transaction_amount=abs(amount_decimal),
            description=description,
            is_approved=1,
            symbol=symbol,
            order_quantity=order_quantity,
            order_type=order_type
        )
        wallet_record = await create_wallet_record(db, wallet_data)

        if not wallet_record:
            raise Exception("Failed to create wallet transaction record for demo user.")

        await db.refresh(demo_user)
        return demo_user

async def generate_unique_demo_account_number(db: AsyncSession) -> str:
    """
    Generate a unique 5-character alphanumeric account number for a DemoUser.
    """
    while True:
        account_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        existing = await db.execute(
            select(DemoUser).filter(DemoUser.account_number == account_number)
        )
        if not existing.scalars().first():
            return account_number

# --- CRUD Operations for Wallet and OTP (Modified for User/DemoUser) ---

# Note: The actual `create_wallet_record` and `create_otp_record` functions
# will likely reside in `app/crud/wallet.py` and `app/crud/otp.py` respectively.
# These functions will need to be updated to accept either `user_id` or `demo_user_id`.

# Example of how `create_wallet_record` in app/crud/wallet.py would need to be updated:
# async def create_wallet_record(
#     db: AsyncSession,
#     wallet_data: WalletCreate,
#     user_id: Optional[int] = None,
#     demo_user_id: Optional[int] = None
# ) -> Wallet:
#     # ... logic to create wallet record ...
#     # Ensure that either user_id or demo_user_id is provided, but not both.
#     if user_id and demo_user_id:
#         raise ValueError("Cannot associate wallet record with both a user and a demo user.")
#     if user_id:
#         db_wallet = Wallet(**wallet_data.model_dump(), user_id=user_id)
#     elif demo_user_id:
#         db_wallet = Wallet(**wallet_data.model_dump(), demo_user_id=demo_user_id)
#     else:
#         raise ValueError("Wallet record must be associated with either a user or a demo user.")
#     # ... rest of the function ...


# Example of how `create_otp_record` in app/crud/otp.py would need to be updated:
# async def create_otp_record(
#     db: AsyncSession,
#     otp_data: OTPCreate,
#     user_id: Optional[int] = None,
#     demo_user_id: Optional[int] = None
# ) -> OTP:
#     # ... logic to create OTP record ...
#     if user_id and demo_user_id:
#         raise ValueError("Cannot associate OTP record with both a user and a demo user.")
#     if user_id:
#         db_otp = OTP(**otp_data.model_dump(), user_id=user_id)
#     elif demo_user_id:
#         db_otp = OTP(**otp_data.model_dump(), demo_user_id=demo_user_id)
#     else:
#         raise ValueError("OTP record must be associated with either a user or a demo user.")
#     # ... rest of the function ...

async def get_all_active_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all active live users from the database with pagination.
    """
    result = await db.execute(
        select(User)
        .filter(User.status == 1)
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

async def get_all_active_demo_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[DemoUser]:
    """
    Retrieves a list of all active demo users from the database with pagination.
    """
    result = await db.execute(
        select(DemoUser)
        .filter(DemoUser.status == 1)
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

async def get_all_active_users_both(db: AsyncSession, skip: int = 0, limit: int = 100):
    """
    Retrieves all active users from both live and demo tables.
    Returns a tuple: (live_users, demo_users)
    """
    live_users = await get_all_active_users(db, skip, limit)
    demo_users = await get_all_active_demo_users(db, skip, limit)
    return live_users, demo_users

