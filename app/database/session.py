# app/database/session.py

import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator

# Import the Base from base.py. This is needed for creating tables.
from .base import Base

# Import configuration settings
from app.core.config import get_settings # Import get_settings

# --- Database Configuration ---
# We'll get these from environment variables using our config module.
settings = get_settings() # Get the settings instance

# Construct the asynchronous database URL using settings
# Using aiomysql driver for async MySQL/MariaDB support
# Ensure you have aiomysql installed: pip install aiomysql
DATABASE_URL = settings.DATABASE_URL # Use the property from settings

# --- ADD THIS PRINT STATEMENT ---
print(f"Attempting to connect to database using URL: {DATABASE_URL}")
# --- END OF PRINT STATEMENT ---


# --- Database Engine ---
# Create the asynchronous engine.
# echo=True will print SQL statements to the console (useful for debugging)
engine = create_async_engine(DATABASE_URL, echo=True)

# --- Database Session Local ---
# Create a configured "SessionLocal" class.
# expire_on_commit=False is often used with async sessions
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# --- Dependency to get a database session ---
# This function will be used in your FastAPI path operations
# to get an asynchronous database session.
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function to get an asynchronous database session.
    Yields a session and ensures it's closed after the request.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            # You might need to explicitly commit in your CRUD operations
            # or service layer depending on your transaction management strategy.
            # await session.commit() # Example commit if managing transactions here
        except Exception as e:
            # await session.rollback() # Example rollback
            print(f"Database session error: {e}") # Basic error logging
            raise # Re-raise the exception
        finally:
            await session.close()

# --- Function to create all tables ---
# This is useful for initial setup or development.
# In production, you would typically use database migration tools like Alembic.
async def create_all_tables():
    """
    Creates all tables defined in the SQLAlchemy models.
    Use with caution in production; migrations are preferred.
    """
    async with engine.begin() as conn:
        # Import all models here to ensure they are registered with Base.metadata
        # Make sure all your model files (user.py, group.py, etc.) are imported
        # or that app/database/models.py imports them if they are split.
        # The import below assumes all models are in app/database/models.py
        from . import models # Adjust import if models are in separate files
        print("Running Base.metadata.create_all...")
        await conn.run_sync(Base.metadata.create_all)
        print("Base.metadata.create_all finished.")

# Example of how to use create_all_tables (e.g., in main.py startup event)
# import asyncio
# async def main():
#     await create_all_tables()
#
# if __name__ == "__main__":
#     # Ensure you have an event loop running for async functions
#     asyncio.run(main())
