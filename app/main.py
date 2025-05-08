# app/main.py

# Import necessary components from fastapi, including Query
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os
import json # Import json for the publisher task
from typing import Optional # Import Optional if not already imported

# --- REMOVE THESE LINES IF THEY ARE STILL HERE ---
# >>> Remove this import <<<
# import aioredis # Import aioredis for type hinting
# >>> Add this import for the type hint <<<
# Import the Redis client type from the standard redis library's asyncio module
# from redis.asyncio import Redis # This import is now typically in redis_client.py
# --- END REMOVE ---

import logging # Import logging

# Configure basic logging early
# In a real production setup, you'd use a more advanced logger configuration
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db # Alias db to avoid conflict with SQLAlchemy db variable

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import your database models to ensure they are registered with SQLAlchemy metadata
from app.database import models # Ensure models are imported so SQLAlchemy knows about them

# Import the Redis client dependency and the global instance from your new file
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance # <--- MODIFIED IMPORT

# Import security functions for Redis connection/disconnection
from app.core.security import connect_to_redis, close_redis_connection # <--- Ensure these are imported from security.py

# Import the API router from app.api.v1.api
from app.api.v1.api import api_router

# Import the shared state (for the redis publish queue)
from app.shared_state import redis_publish_queue # Ensure this is imported


# Get application settings
settings = get_settings()


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Initialize FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Mount static files directory
# This serves files from the 'uploads' directory (where proofs are saved) under the '/static' URL path
# Make sure the UPLOAD_DIRECTORY in users.py matches this path structure
app.mount("/static", StaticFiles(directory="uploads"), name="static")


# --- Database Startup Event ---
@app.on_event("startup")
async def startup_event():
    """
    Application startup event: Initialize Firebase, Database, and Redis.
    """
    logger.info("Application startup event triggered.")

    # --- Initialize Firebase Admin SDK ---
    try:
        # Ensure the path to the service account key is correct
        cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
        if not os.path.exists(cred_path):
             logger.critical(f"Firebase service account key file not found at: {cred_path}")
             # Depending on criticality, you might want to raise an exception or exit
             # For now, we log and continue, but Firebase functionality will fail.
        else:
            cred = credentials.Certificate(cred_path)
            # Check if Firebase app is already initialized (e.g., in case of --reload)
            if not firebase_admin._apps:
                 firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL
                 })
                 logger.info("Firebase Admin SDK initialized successfully.")
            else:
                 logger.info("Firebase Admin SDK already initialized.")

    except Exception as e:
        logger.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)
        # Decide how to handle Firebase initialization failure


    # --- Create Database Tables (if not using migrations) ---
    # Use migrations (like Alembic) in production! This is for quick setup/dev.
    try:
        await create_all_tables()
        logger.info("Database tables ensured/created.")
    except Exception as e:
        logger.critical(f"Failed to create database tables: {e}", exc_info=True)
        # Decide how to handle DB initialization failure


    # --- Initialize Redis Client ---
    global global_redis_client_instance # Declare global instance to modify it
    try:
        global_redis_client_instance = await connect_to_redis() # Use the function from security.py
        if global_redis_client_instance:
             logger.info("Redis client initialized successfully.")
             # Optional: Ping Redis to confirm connection
             # await global_redis_client_instance.ping()
             # logger.info("Redis server ping successful.")
        else:
             logger.critical("Failed to initialize Redis client.")
             # Decide how to handle Redis initialization failure

    except Exception as e:
        logger.critical(f"Failed to initialize Redis client: {e}", exc_info=True)
        # Decide how to handle Redis initialization failure

    # --- Start Background Tasks ---
    # Import tasks here to avoid circular imports at top level if tasks import other app modules
    from app.firebase_stream import process_firebase_stream_events
    from app.api.v1.endpoints.market_data_ws import redis_market_data_broadcaster
    # from app.core.security import redis_publisher_task # Assuming you have this task in security.py or another file
    # app/main.py

# ... (Keep existing imports) ...

    # --- Import Background Tasks from their correct locations ---
    from app.firebase_stream import process_firebase_stream_events
    from app.api.v1.endpoints.market_data_ws import redis_market_data_broadcaster
    # --- CHANGE THIS LINE ---
    from app.api.v1.endpoints.market_data_ws import redis_publisher_task # <--- Import from market_data_ws.py


# ... (Rest of main.py including startup_event where the task is scheduled) ...


    # Start the Firebase streaming task
    # This task reads from Firebase and puts messages onto the redis_publish_queue
    asyncio.create_task(process_firebase_stream_events(
        firebase_db_instance=firebase_db,
        path=settings.FIREBASE_DATA_PATH # Use setting for data path
    ))
    logger.info("Firebase stream processing task scheduled.")

    # Start the Redis publisher task
    # This task reads from redis_publish_queue and publishes to Redis Pub/Sub
    if global_redis_client_instance:
        asyncio.create_task(redis_publisher_task(redis_client=global_redis_client_instance))
        logger.info("Redis publisher task scheduled.")
    else:
        logger.warning("Redis client not initialized. Skipping Redis publisher task.")


    # Start the Redis market data broadcaster task
    # This task subscribes to Redis Pub/Sub and sends data to WS clients
    if global_redis_client_instance:
         asyncio.create_task(redis_market_data_broadcaster(redis_client=global_redis_client_instance))
         logger.info("Redis market data broadcaster task scheduled.")
    else:
         logger.warning("Redis client not initialized. Skipping Redis market data broadcaster task.")


    logger.info("Application startup event finished.")


# --- Database Shutdown Event ---
@app.on_event("shutdown")
async def shutdown_event():
    """
    Application shutdown event: Close Database and Redis connections.
    """
    logger.info("Application shutdown event triggered.")
    # Close database connection pool (if applicable to your engine configuration)
    # await engine.dispose() # Use engine.dispose() if using AsyncEngine
    # logger.info("Database connection pool closed.")

    # Close Redis connection
    global global_redis_client_instance # Declare global instance to access it
    if global_redis_client_instance:
        await close_redis_connection(global_redis_client_instance) # Use the function from security.py
        global_redis_client_instance = None # Set to None after closing
        logger.info("Redis client connection closed.")
    else:
        logger.warning("Redis client was not initialized or already closed during shutdown.")

    logger.info("Application shutdown event finished.")


# --- Dependency to get the Redis client instance ---
# REMOVE THE DEFINITION OF get_redis_client HERE IF IT EXISTS.
# It is now defined in app.dependencies.redis_client.py

# --- API Routers ---
# Include your API routers here
app.include_router(api_router, prefix=settings.API_V1_STR) # Include the main API router

# Example root endpoint (optional)
@app.get("/")
async def read_root():
    """
    Root endpoint of the application.
    """
    return {"message": "Welcome to the Trading App Backend!"}


# Dependency to get the Redis client instance (already imported from app.dependencies.redis_client)
# This function is used by endpoints needing a Redis client via FastAPI's Depends()

# The redis_publisher_task should be imported from where it's defined (e.g., security.py or a separate tasks module)
# The redis_market_data_broadcaster should be imported from where it's defined (e.g., market_data_ws.py)

# Ensure these tasks are imported and scheduled in the startup event
# from app.core.security import redis_publisher_task # Example: if publisher is in security.py
# from app.api.v1.endpoints.market_data_ws import redis_market_data_broadcaster # Example: if broadcaster is in market_data_ws.py