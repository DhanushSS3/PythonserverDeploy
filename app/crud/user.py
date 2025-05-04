# app/crud/user.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select # Use select from sqlalchemy.future for async
from sqlalchemy.exc import IntegrityError # Import for handling unique constraints

from app.database.models import User # Import the SQLAlchemy User model
from app.schemas.user import UserCreate # Import the Pydantic schema for creation
from app.core.security import get_password_hash # Import the password hashing utility

# Function to get a user by email
async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    """
    Retrieves a user from the database by their email address.

    Args:
        db: The asynchronous database session.
        email: The email address to search for.

    Returns:
        The User object if found, otherwise None.
    """
    # Use select to build the query
    result = await db.execute(select(User).filter(User.email == email))
    # fetchone() returns a tuple (User,), so get the first element
    return result.scalars().first()

# Function to get a user by phone number
async def get_user_by_phone_number(db: AsyncSession, phone_number: str) -> User | None:
    """
    Retrieves a user from the database by their phone number.

    Args:
        db: The asynchronous database session.
        phone_number: The phone number to search for.

    Returns:
        The User object if found, otherwise None.
    """
    result = await db.execute(select(User).filter(User.phone_number == phone_number))
    return result.scalars().first()


# Function to create a new user
async def create_user(db: AsyncSession, user: UserCreate) -> User:
    """
    Creates a new user in the database.

    Args:
        db: The asynchronous database session.
        user: A UserCreate Pydantic model containing the user data.

    Returns:
        The newly created User SQLAlchemy model instance.

    Raises:
        IntegrityError: If a user with the same email or phone number already exists.
    """
    # Hash the password before storing it
    hashed_password = get_password_hash(user.password)

    # Create a dictionary from the UserCreate schema
    user_data = user.model_dump() # Use model_dump() for Pydantic V2+
    # user_data = user.dict() # Use dict() for Pydantic V1

    # --- FIX: Remove the raw 'password' from the dictionary ---
    # We only want to pass the hashed password to the SQLAlchemy model.
    user_data.pop("password", None) # Remove 'password' key if it exists
    # --- END FIX ---

    # Create a new SQLAlchemy User model instance
    db_user = User(
        **user_data,
        hashed_password=hashed_password # Add the hashed password
        # No need to pass password=None explicitly now
    )

    # Add the new user to the session and commit
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user) # Refresh to get the generated ID and default values

    return db_user

# You can add more CRUD functions here (e.g., get_user_by_id, update_user, delete_user)
