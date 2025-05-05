# app/crud/user.py

from typing import Optional # Import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select # Import select
from sqlalchemy.exc import IntegrityError

from app.database.models import User # Import the User model
# from app.schemas.user import UserCreate # Not strictly needed here, but can be useful for type hinting if preferred
from app.core.security import get_password_hash # Import the password hashing utility

# Function to get a user by email
async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    """
    Retrieves a user from the database by their email address.

    Args:
        db: The asynchronous database session.
        email: The email address to search for.

    Returns:
        The User SQLAlchemy model instance if found, otherwise None.
    """
    # Use select to build the query
    result = await db.execute(select(User).filter(User.email == email))
    # scalars().first() gets the first result as a scalar (the User object)
    return result.scalars().first()

# Function to get a user by phone number
async def get_user_by_phone_number(db: AsyncSession, phone_number: str) -> User | None:
    """
    Retrieves a user from the database by their phone number.

    Args:
        db: The asynchronous database session.
        phone_number: The phone number to search for.

    Returns:
        The User SQLAlchemy model instance if found, otherwise None.
    """
    result = await db.execute(select(User).filter(User.phone_number == phone_number))
    return result.scalars().first()

# Function to get a user by ID (useful for authentication dependency)
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

# You can add more CRUD functions here (e.g., update_user, delete_user)
