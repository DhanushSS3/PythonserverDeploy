# app/main.py

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os
import json # Import json
from typing import Optional # Import Optional if not already imported

import logging # Import logging

# Configure basic logging early
# Set root logger level to INFO to avoid hang caused by excessive debug logs from other libs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Get specific loggers and set their levels to DEBUG to see detailed logs from those modules
# This helps focus debug output on relevant parts and avoids hanging due to excessive logging
logging.getLogger('app.api.v1.endpoints.market_data_ws').setLevel(logging.DEBUG)
logging.getLogger('app.firebase_stream').setLevel(logging.DEBUG) # <-- ENSURE THIS IS DEBUG
# Optionally set other loggers to DEBUG if needed, e.g., for database or redis issues:
# logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
# logging.getLogger('redis').setLevel(logging.DEBUG)


logger = logging.getLogger(__name__)
# Keep the logger for main.py at INFO or DEBUG as desired for main's logs
logger.setLevel(logging.DEBUG)


# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db # Alias db to avoid conflict with SQLAlchemy db variable

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import API router
from app.api.v1.api import api_router

# Import background tasks
from app.firebase_stream import process_firebase_events # Import the firebase processing task
from app.api.v1.endpoints.market_data_ws import redis_publisher_task, redis_market_data_broadcaster # Import Redis tasks

# Import Redis dependency and global instance
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.core.security import close_redis_connection # Import the Redis close function

# Import shared state (for the queue)
from app.shared_state import redis_publish_queue # Import the queue


# Get application settings
settings = get_settings()

# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- Static Files (if you have a frontend) ---
# Mount a directory to serve static files (e.g., HTML, CSS, JS for a frontend)
# Example: If your static files are in a directory named 'static' at the project root
# app.mount("/static", StaticFiles(directory="static"), name="static")


# --- Application Startup Event ---
@app.on_event("startup")
async def startup_event():
    """
    Application startup event: Initialize Firebase Admin SDK, connect to Redis,
    create database tables, and start background tasks (Firebase listener, Redis publisher, Broadcaster).
    """
    logger.info("Application startup event triggered.")

    # 1. Initialize Firebase Admin SDK
    firebase_app_instance = None # Initialize to None
    try:
        cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
        if not os.path.exists(cred_path):
             logger.critical(f"Firebase service account key file not found at: {cred_path}")
             # Depending on your deployment, you might raise an error or handle this differently
             # For now, we'll log and continue, but Firebase listener will likely fail
        else:
            cred = credentials.Certificate(cred_path)
            # Check if Firebase app is already initialized (important for --reload)
            if not firebase_admin._apps:
                 firebase_app_instance = firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL
                 })
                 logger.info("Firebase Admin SDK initialized successfully.")
            else:
                 # If already initialized (e.g., due to --reload), get the default app
                 firebase_app_instance = firebase_admin.get_app()
                 logger.info("Firebase Admin SDK already initialized.")

    except Exception as e:
        logger.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)
        # firebase_app_instance remains None if initialization fails


    # 2. Connect to Redis
    # The get_redis_client dependency handles the connection, but we can trigger it here
    # to ensure the global instance is set early.
    global global_redis_client_instance # Ensure we are modifying the global instance
    try:
        # Call the dependency function to establish the connection and set the global instance
        global_redis_client_instance = await get_redis_client()
        if global_redis_client_instance:
             logger.info("Redis client initialized successfully.")
        else:
             logger.critical("Redis client failed to initialize.") # This will be logged if get_redis_client returns None

    except Exception as e:
        logger.critical(f"Failed to connect to Redis during startup: {e}", exc_info=True)
        global_redis_client_instance = None # Ensure instance is None if connection fails


    # 3. Create Database Tables (if they don't exist)
    # Use the async function to create tables
    try:
        await create_all_tables()
        logger.info("Database tables ensured/created.")
    except Exception as e:
        logger.critical(f"Failed to create database tables: {e}", exc_info=True)
        # Depending on criticality, you might want to exit or handle this differently


    # 4. Start Background Tasks
    # Ensure Redis client and Firebase app instance are available before starting tasks
    if global_redis_client_instance and firebase_app_instance:
        # Start the Firebase stream processing task
        # It needs the firebase_db instance (aliased from firebase_admin.db) and the path
        # ***** CORRECTED LINE BELOW *****
        asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        logger.info("Firebase stream processing task scheduled.")

        # Start the Redis publisher task
        # It needs the Redis client instance and the shared queue
        asyncio.create_task(redis_publisher_task(global_redis_client_instance))
        logger.info("Redis publisher task scheduled.")

        # Start the Redis market data broadcaster task
        # It needs the Redis client instance
        asyncio.create_task(redis_market_data_broadcaster(global_redis_client_instance))
        logger.info("Redis market data broadcaster task scheduled.")

    else:
        missing_services = []
        if not global_redis_client_instance:
            missing_services.append("Redis client")
        if not firebase_app_instance:
            missing_services.append("Firebase app instance")
        logger.warning(f"Background tasks (Firebase listener, Redis publisher/broadcaster) not started due to missing: {', '.join(missing_services)}.")


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
# The get_redis_client function is now imported from app.dependencies.redis_client


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


# The redis_publisher_task and redis_market_data_broadcaster are now imported from market_data_ws.py
# The process_firebase_events task is now imported from firebase_stream.py

# Ensure these tasks are imported and scheduled in the startup event (done in startup_event function)
