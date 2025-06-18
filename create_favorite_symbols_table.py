"""
Script to create the user_favorite_symbols table in the database.
Run this with python create_favorite_symbols_table.py
"""
import asyncio
import logging
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

from app.core.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get database URL from settings
settings = get_settings()
DATABASE_URL = settings.ASYNC_DATABASE_URL

async def create_table_manually():
    """Create user_favorite_symbols table using raw SQL."""
    try:
        # Create engine and connection
        logger.info(f"Connecting to database with URL: {DATABASE_URL}")
        engine = create_async_engine(DATABASE_URL, echo=True)
        
        async with engine.begin() as conn:
            # First, drop the table if it exists
            logger.info("Dropping user_favorite_symbols table if it exists...")
            await conn.execute(text("DROP TABLE IF EXISTS user_favorite_symbols"))
            
            # Create the table using raw SQL
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_favorite_symbols (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                symbol VARCHAR(30) NOT NULL,
                is_demo BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, symbol, is_demo)
            );
            """
            
            logger.info("Creating user_favorite_symbols table...")
            await conn.execute(text(create_table_sql))
            
            logger.info("[SUCCESS] user_favorite_symbols table created successfully")
        
        # Properly dispose the engine
        await engine.dispose()
            
    except Exception as e:
        logger.error(f"❌ Error creating table: {e}")
        raise

def main():
    """Main function that runs the async code."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(create_table_manually())
        loop.close()
        print("Table creation completed successfully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 