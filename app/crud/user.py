# app/crud/user.py

from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from decimal import Decimal

# REMOVE THIS LINE: from sqlalchemy.orm import with_for_update # Incorrect import

# with_for_update is used as an option directly in the select statement,
# it is not imported from sqlalchemy.orm

from app.database.models import User # Import the User model
from app.schemas.user import UserCreate, UserUpdate # Import UserCreate and UserUpdate
from app.core.security import get_password_hash # Import the password hashing utility


async def get_user(db: AsyncSession, user_id: int) -> Optional[User]:
    """
    Retrieves a user from the database by their ID.
    """
    result = await db.execute(select(User).filter(User.id == user_id))
    return result.scalars().first()




# Function to get a user by ID (useful for authentication dependency and update/delete)
async def get_user_by_id(db: AsyncSession, user_id: int) -> User | None:
    """
    Retrieves a user from the database by their ID.

    Args:
        db: The asynchronous database session.
        user_id: The ID to search for.

    Returns:
        The User SQLAlchemy model instance if found, otherwise None.
    """
    result = await db.execute(select(User).filter(User.id == user_id))
    return result.scalars().first()

# Function to get a user by ID with locking (for updates involving sensitive fields)
# async def get_user_by_id_with_lock(db: AsyncSession, user_id: int) -> User | None:
#     """
#     Retrieves a user from the database by their ID with a row-level lock.
#     Use this when updating sensitive fields like wallet balance.

#     Args:
#         db: The asynchronous database session.
#         user_id: The ID to search for.

#     Returns:
#         The User SQLAlchemy model instance if found, otherwise None.
#     """
#     # Use with_for_update() as an option on the select statement
#     # No direct import is needed for with_for_update() itself.
#     result = await db.execute(
#         select(User)
#         .filter(User.id == user_id)
#         .with_for_update() # <-- Apply the lock here using the method
#     )
#     return result.scalars().first()


# Function to get all users
async def get_all_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all users from the database with pagination.

    Args:
        db: The asynchronous database session.
        skip: The number of records to skip (for pagination).
        limit: The maximum number of records to return (for pagination).

    Returns:
        A list of User SQLAlchemy model instances.
    """
    result = await db.execute(select(User).offset(skip).limit(limit))
    return result.scalars().all()

# Function to get all demo users
async def get_demo_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all demo users from the database with pagination.

    Args:
        db: The asynchronous database session.
        skip: The number of records to skip (for pagination).
        limit: The maximum number of records to return (for pagination).

    Returns:
        A list of User SQLAlchemy model instances with user_type='demo'.
    """
    # Select users and filter by user_type = 'demo'
    result = await db.execute(
        select(User)
        .filter(User.user_type == 'demo') # <-- Filter by user_type
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

# Function to get all live users
async def get_live_users(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
    """
    Retrieves a list of all live users from the database with pagination.

    Args:
        db: The asynchronous database session.
        skip: The number of records to skip (for pagination).
        limit: The maximum number of records to return (for pagination).

    Returns:
        A list of User SQLAlchemy model instances with user_type='live'.
    """
    # Select users and filter by user_type = 'live'
    result = await db.execute(
        select(User)
        .filter(User.user_type == 'live') # <-- Filter by user_type
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()


# Function to create a new user (updated to accept file paths and proof types)
async def create_user(
    db: AsyncSession,
    user_data: dict, # Accept user data as a dictionary (excluding password, proof types, and file paths)
    hashed_password: str, # Accept hashed password separately
    id_proof_path: Optional[str] = None, # Accept ID proof type string
    id_proof_image_path: Optional[str] = None, # Accept ID proof image file path
    address_proof_path: Optional[str] = None, # Accept address proof type string
    address_proof_image_path: Optional[str] = None # Accept address proof image file path
) -> User:
    """
    Creates a new user in the database with optional proof types and file paths.

    Args:
        db: The asynchronous database session.
        user_data: A dictionary containing user data (excluding password, proof types, and files).
        hashed_password: The hashed password string.
        id_proof_path: String indicating the type of ID proof.
        id_proof_image_path: Path to the ID proof image file.
        address_proof_path: String indicating the type of address proof.
        address_proof_image_path: Path to the address proof image file.

    Returns:
        The newly created User SQLAlchemy model instance.

    Raises:
        IntegrityError: If a user with the same email or phone number already exists.
    """
    # Create a new SQLAlchemy User model instance
    # Unpack user_data, then explicitly set hashed_password and proof fields
    db_user = User(
        **user_data, # Unpack user data dictionary
        hashed_password=hashed_password,
        id_proof=id_proof_path, # Set the string value for id_proof column
        id_proof_image=id_proof_image_path, # Set the file path for id_proof_image column
        address_proof=address_proof_path, # Set the string value for address_proof column
        address_proof_image=address_proof_image_path # Set the file path for address_proof_image column
    )

    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)

    return db_user

# Function to update an existing user
async def update_user(db: AsyncSession, db_user: User, user_update: UserUpdate) -> User:
    """
    Updates an existing user in the database.

    Args:
        db: The asynchronous database session.
        db_user: The User SQLAlchemy model instance to update.
        user_update: A UserUpdate Pydantic model containing the update data.

    Returns:
        The updated User SQLAlchemy model instance.
    """
    # Convert the UserUpdate Pydantic model to a dictionary, excluding unset fields
    update_data = user_update.model_dump(exclude_unset=True) # Use model_dump for Pydantic V2+

    # Iterate over the update data and set the corresponding attributes on the SQLAlchemy model
    for field, value in update_data.items():
        # You might add specific logic here for certain fields if needed
        # For example, if updating password (though a separate endpoint is better)
        # if field == "password" and value:
        #     setattr(db_user, "hashed_password", get_password_hash(value))
        # elif field == "email" and value != db_user.email:
        #     # Add logic to handle email change (e.g., re-verification)
        #     setattr(db_user, field, value)
        # else:
        setattr(db_user, field, value)

    await db.commit()
    await db.refresh(db_user) # Refresh to get the updated values

    return db_user

# Function to delete a user
async def delete_user(db: AsyncSession, db_user: User):
    """
    Deletes a user from the database.

    Args:
        db: The asynchronous database session.
        db_user: The User SQLAlchemy model instance to delete.
    """
    await db.delete(db_user)
    await db.commit()

from sqlalchemy.future import select
from sqlalchemy import func # Need func for count if implementing count functions

# ... (existing get_user, get_user_by_email, get_user_by_phone_number, get_user_by_id) ...

# Function to get user by ID with locking (for updates involving sensitive fields)
async def get_user_by_id_with_lock(db: AsyncSession, user_id: int) -> User | None:
    """
    Retrieves a user from the database by their ID with a row-level lock.
    Use this when updating sensitive fields like wallet balance or margin.
    Includes the user's margin and wallet_balance.
    """
    # Use with_for_update() as an option on the select statement
    # This correctly fetches the User object including margin and wallet_balance
    result = await db.execute(
        select(User)
        .filter(User.id == user_id)
        .with_for_update() # <-- Apply the lock here using the method
    )
    return result.scalars().first()

# --- Optional: Dedicated function to get only the user's margin ---
# This is not strictly necessary as get_user_by_id already provides it,
# but added as per your request for a function specifically for margin.
async def get_user_margin_by_id(db: AsyncSession, user_id: int) -> Optional[Decimal]:
    """
    Retrieves only the margin value for a specific user from the database.
    """
    result = await db.execute(
        select(User.margin)
        .filter(User.id == user_id)
    )
    # Use scalar_one_or_none() to get the single margin value or None
    return result.scalar_one_or_none()


import random
import string
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.models import User

async def generate_unique_account_number(db: AsyncSession) -> str:
    """
    Generate a unique 5-character alphanumeric account number.
    Retries until a unique one is found (rare collisions).
    """
    while True:
        account_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        existing = await db.execute(
            select(User).filter(User.account_number == account_number)
        )
        if not existing.scalars().first():
            return account_number

# app/crud/user.py

# ... other imports ...

# Existing get_user_by_email (can be kept for other purposes or deprecated for registration checks)
async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).filter(User.email == email))
    return result.scalars().first() # Returns the first match, irrespective of type

# New function for type-specific email check
async def get_user_by_email_and_type(db: AsyncSession, email: str, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their email address and user_type.
    """
    result = await db.execute(
        select(User).filter(User.email == email, User.user_type == user_type)
    )
    return result.scalars().first()

# Existing get_user_by_phone_number (can be kept or deprecated for registration checks)
async def get_user_by_phone_number(db: AsyncSession, phone_number: str) -> User | None:
    result = await db.execute(select(User).filter(User.phone_number == phone_number))
    return result.scalars().first() # Returns the first match, irrespective of type

# New function for type-specific phone number check
async def get_user_by_phone_number_and_type(db: AsyncSession, phone_number: str, user_type: str) -> User | None:
    """
    Retrieves a user from the database by their phone number and user_type.
    """
    result = await db.execute(
        select(User).filter(User.phone_number == phone_number, User.user_type == user_type)
    )
    return result.scalars().first()

# ... rest of crud/user.py ...
# The create_user function itself doesn't need to change,
# as the IntegrityError from the DB will handle uniqueness violations.


async def get_user_by_email_phone_type(db: AsyncSession, email: str, phone_number: str, user_type: str) -> Optional[User]:
    result = await db.execute(
        select(User).filter(
            User.email == email,
            User.phone_number == phone_number,
            User.user_type == user_type
        )
    )
    return result.scalars().first()
