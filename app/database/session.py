# app/database/session.py

import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator, Optional
import logging # Import logging
import warnings
import asyncio # Import asyncio for Lock

# Create a database logger
db_logger = logging.getLogger("database")

logger = logging.getLogger(__name__) # Get logger for this module

# Import the Base from base.py. This is needed for creating tables.
from .base import Base

# Import configuration settings
from app.core.config import get_settings # Import get_settings

# --- Database Configuration ---
# We'll get these from environment variables using our config module.
settings = get_settings() # Get the settings instance

# Construct the asynchronous database URL using settings property
# Using aiomysql driver for async MySQL/MariaDB support
# Ensure you have aiomysql installed: pip install aiomysql
DATABASE_URL = settings.ASYNC_DATABASE_URL # Use the new property from settings

# --- ADD THIS PRINT STATEMENT ---
logger.info(f"Attempting to connect to database using URL: {DATABASE_URL[:20]}...") # Log part of the URL
# --- END OF PRINT STATEMENT ---


# --- Database Engine ---
# Create the asynchronous engine.
# echo=True will print SQL statements to the console (useful for debugging)
engine = create_async_engine(
    DATABASE_URL, 
    echo=settings.ECHO_SQL,
    pool_size=10,  # Reduced from 20 to 10
    max_overflow=5,  # Reduced from 10 to 5
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_pre_ping=True,
    pool_timeout=30,  # Add timeout for connection acquisition
    pool_reset_on_return='commit'  # Reset connection state on return
)

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
    db_logger.debug("Creating new database session")
    async with AsyncSessionLocal() as session:
        try:
            yield session
            db_logger.debug("Database session used successfully")
        except Exception as e:
            db_logger.error(f"Error in database session: {e}", exc_info=True)
            await session.rollback()
            db_logger.info("Session rolled back due to error")
            raise
        finally:
            db_logger.debug("Closing database session")

# --- Connection Pool Monitoring ---
async def get_connection_pool_status():
    """
    Get the current status of the database connection pool.
    """
    pool = engine.pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "invalid": pool.invalid()
    }

async def log_connection_pool_status():
    """
    Log the current connection pool status for monitoring.
    """
    try:
        status = await get_connection_pool_status()
        db_logger.info(f"Connection pool status: {status}")
        
        # Check for potential connection exhaustion
        if status["checked_out"] > (status["pool_size"] * 0.8):
            db_logger.warning(f"High connection usage: {status['checked_out']}/{status['pool_size']} connections in use")
        
        if status["overflow"] > 0:
            db_logger.warning(f"Connection pool overflow: {status['overflow']} connections")
            
    except Exception as e:
        db_logger.error(f"Error getting connection pool status: {e}")

async def emergency_connection_recovery():
    """
    Emergency function to recover from connection pool exhaustion.
    """
    try:
        pool = engine.pool
        status = await get_connection_pool_status()
        
        if status["checked_out"] > (status["pool_size"] * 0.9):
            db_logger.error(f"CRITICAL: Connection pool nearly exhausted. Status: {status}")
            
            # Force close some connections if possible
            if hasattr(pool, '_pool') and pool._pool:
                # This is a last resort - force close some connections
                db_logger.warning("Attempting emergency connection recovery...")
                
    except Exception as e:
        db_logger.error(f"Error in emergency connection recovery: {e}")

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
        logger.info("Running Base.metadata.create_all...")
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Base.metadata.create_all finished.")

# Example of how to use create_all_tables (e.g., in main.py startup event)
# import asyncio
# async def main():
#     await create_all_tables()
#
# if __name__ == "__main__":
#     # Ensure you have an event loop running for async functions
#     asyncio.run(main())

# Add the async_session_factory function after AsyncSessionLocal definition
async def async_session_factory() -> AsyncSession:
    """
    Creates and returns a new async database session.
    This is useful for background tasks that need to create their own sessions.
    """
    session = AsyncSessionLocal()
    try:
        return session
    except Exception as e:
        await session.close()
        logger.error(f"Error creating async session: {e}", exc_info=True)
        raise

# --- Shared Database Session for Background Tasks ---
# This helps reduce the number of database connections by sharing sessions
_shared_session: Optional[AsyncSession] = None
_session_lock = asyncio.Lock()

async def get_shared_db_session() -> AsyncSession:
    """
    Get a shared database session for background tasks.
    This reduces the number of concurrent database connections.
    """
    global _shared_session
    
    async with _session_lock:
        if _shared_session is None or _shared_session.is_closed:
            _shared_session = AsyncSessionLocal()
        return _shared_session

async def close_shared_db_session():
    """
    Close the shared database session.
    """
    global _shared_session
    
    async with _session_lock:
        if _shared_session and not _shared_session.is_closed:
            await _shared_session.close()
            _shared_session = None

# --- Context Manager for Shared Sessions ---
class SharedDatabaseSession:
    """
    Context manager for using shared database sessions in background tasks.
    """
    def __init__(self):
        self.session = None
    
    async def __aenter__(self):
        self.session = await get_shared_db_session()
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Don't close the session here, let it be reused
        if exc_type is not None:
            await self.session.rollback()
        else:
            await self.session.commit()
