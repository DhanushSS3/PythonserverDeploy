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
# logging.getLogger('app.api.v1.endpoints.market_data_ws').setLevel(logging.DEBUG)
# logging.getLogger('app.firebase_stream').setLevel(logging.DEBUG) # <-- ENSURE THIS IS DEBUG
logging.getLogger('app.services.portfolio_calculator').setLevel(logging.DEBUG)
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
print(f"DEBUG: API_V1_STR is set to: {settings.API_V1_STR}")
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
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        firebase_task.set_name("firebase_listener")
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        logger.info("Firebase stream processing task scheduled.")

        # Start the Redis publisher task
        # It needs the Redis client instance and the queue
        publisher_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
        publisher_task.set_name("redis_publisher_task")
        background_tasks.add(publisher_task)
        publisher_task.add_done_callback(background_tasks.discard)
        logger.info("Redis publisher task scheduled.")

        # Start the Redis market data broadcaster task
        # It needs the Redis client instance
        broadcaster_task = asyncio.create_task(redis_market_data_broadcaster(global_redis_client_instance))
        broadcaster_task.set_name("redis_market_data_broadcaster")
        background_tasks.add(broadcaster_task)
        broadcaster_task.add_done_callback(background_tasks.discard)
        logger.info("Redis market data broadcaster task scheduled.")

    else:
        missing_services = []
        if not global_redis_client_instance:
            missing_services.append("Redis client")
        if not firebase_app_instance:
            missing_services.append("Firebase app instance")
        logger.warning(f"Background tasks (Firebase listener, Redis publisher/broadcaster) not started due to missing: {', '.join(missing_services)}.")


    logger.info("Application startup event finished.")


# Store background tasks
background_tasks = set()

# --- Database Shutdown Event ---
@app.on_event("shutdown")
async def shutdown_event():
    """
    Application shutdown event: Cancel background tasks and close connections.
    """
    logger.info("Application shutdown event triggered.")
    
    # Cancel all background tasks
    logger.info("Cancelling background tasks...")
    for task in background_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Background task {task.get_name()} cancelled successfully.")
            except Exception as e:
                logger.error(f"Error cancelling background task {task.get_name()}: {e}", exc_info=True)
    
    # Close Redis connection
    global global_redis_client_instance
    if global_redis_client_instance:
        await close_redis_connection(global_redis_client_instance)
        global_redis_client_instance = None
        logger.info("Redis client connection closed.")
    else:
        logger.warning("Redis client was not initialized or already closed during shutdown.")
    
    # Clean up Firebase resources
    from app.firebase_stream import cleanup_firebase
    cleanup_firebase()
    
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


# app/main.py

# import logging
# import logging.handlers
# import os
# import asyncio
# import json
# from typing import Optional

# from fastapi import FastAPI, Depends, HTTPException, status, Query
# from fastapi.staticfiles import StaticFiles

# # --- 1. DEFINE AND CALL LOGGING SETUP EARLY ---
# def setup_logging():
#     """
#     Configures specialized logging:
#     - Portfolio calculator logs (DEBUG+) go exclusively to 'app/portfolio_calc.log'.
#     - Other 'app' module logs (DEBUG+) go to 'app/general_app.log'.
#     - Console shows INFO+ from all modules except portfolio calculator.
#     """
#     log_dir = "app"
#     if not os.path.exists(log_dir):
#         try:
#             os.makedirs(log_dir)
#             print(f"Log directory '{log_dir}' created.")
#         except OSError as e:
#             print(f"Error creating log directory '{log_dir}': {e}")
#             # Depending on the error, you might want to return or raise
#             # For now, we'll proceed, but file logging might fail for specific handlers

#     log_formatter = logging.Formatter(
#         "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
#     )

#     # --- Clear existing handlers from root to prevent duplication ---
#     # This is important if the script reloads or if basicConfig was implicitly called
#     logging.getLogger().handlers.clear()
#     print("Cleared existing root logger handlers (if any).")

#     # --- A. Portfolio Calculator Logger (Dedicated File) ---
#     # !!! IMPORTANT: Update this name if your portfolio_calculator.py logger name is different !!!
#     # If portfolio_calculator.py uses `logging.getLogger(__name__)` and is located at
#     # `app/services/portfolio_calculator.py`, then this is correct.
#     PC_LOGGER_NAME = 'app.services.portfolio_calculator'
#     pc_logger = logging.getLogger(PC_LOGGER_NAME)
#     pc_logger.setLevel(logging.DEBUG)
#     pc_logger.propagate = False  # VERY IMPORTANT: Prevents pc_logger messages from going to root handlers

#     pc_log_file_path = os.path.join(log_dir, "portfolio_calc.log")
#     try:
#         pc_file_handler = logging.handlers.RotatingFileHandler(
#             pc_log_file_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
#         )
#         pc_file_handler.setFormatter(log_formatter)
#         pc_file_handler.setLevel(logging.DEBUG) # Log DEBUG and above for portfolio calculator
#         pc_logger.addHandler(pc_file_handler)
#         print(f"Portfolio calculator logs (DEBUG+) configured for: {pc_log_file_path}")
#     except Exception as e:
#         print(f"Error setting up file handler for '{PC_LOGGER_NAME}' at '{pc_log_file_path}': {e}")

#     # --- B. General 'app' Logger (for other app modules, outputting to a general debug file) ---
#     # This logger will handle things like 'app.api.v1.endpoints.market_data_ws', 'app.firebase_stream', etc.
#     app_general_logger = logging.getLogger('app') # Base logger for 'app' package
#     app_general_logger.setLevel(logging.DEBUG) # Allow DEBUG messages from 'app.*' modules
#     # app_general_logger.propagate = True # Default, will allow INFO+ to go to root's console handler

#     general_app_log_file_path = os.path.join(log_dir, "general_app.log")
#     try:
#         general_app_file_handler = logging.handlers.RotatingFileHandler(
#             general_app_log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
#         )
#         general_app_file_handler.setFormatter(log_formatter)
#         general_app_file_handler.setLevel(logging.DEBUG) # Log DEBUG and above for general app
#         app_general_logger.addHandler(general_app_file_handler)
#         print(f"General 'app' logs (DEBUG+) configured for: {general_app_log_file_path}")
#     except Exception as e:
#         print(f"Error setting up file handler for 'app' general logs at '{general_app_log_file_path}': {e}")


#     # --- C. Root Logger (primarily for Console output for non-pc_logger INFO+) ---
#     # The root logger itself can be set to DEBUG to not filter anything before handlers.
#     # Handlers will then decide what to output.
#     root_logger = logging.getLogger()
#     root_logger.setLevel(logging.DEBUG) # Root is permissive

#     console_handler = logging.StreamHandler()
#     console_handler.setFormatter(log_formatter)
#     console_handler.setLevel(logging.INFO)  # Console shows INFO and above from propagating loggers
#     root_logger.addHandler(console_handler)
#     print("Console logging configured for INFO+ (from propagating loggers).")

#     # --- Set levels for specific app modules as per original main.py if needed ---
#     # These will now log DEBUG to general_app.log and INFO+ to console.
#     logging.getLogger('app.api.v1.endpoints.market_data_ws').setLevel(logging.DEBUG)
#     logging.getLogger('app.firebase_stream').setLevel(logging.DEBUG)
#     logging.getLogger('app.main').setLevel(logging.DEBUG) # For main.py's own logger

#     # --- Test logging ---
#     logging.getLogger("app.main.setup").info("Logging setup finalized. Check console, portfolio_calc.log, and general_app.log.")
#     pc_logger.debug("TEST: This DEBUG message should ONLY be in portfolio_calc.log.")
#     pc_logger.info("TEST: This INFO message should ONLY be in portfolio_calc.log.")
#     logging.getLogger('app.api.v1.endpoints.market_data_ws').debug("TEST: market_data_ws DEBUG - should be in general_app.log, NOT console.")
#     logging.getLogger('app.api.v1.endpoints.market_data_ws').info("TEST: market_data_ws INFO - should be in general_app.log AND console.")
#     logging.getLogger('some_other_module_not_in_app').info("TEST: Other module INFO - should be on console.")

# # CALL THE SETUP FUNCTION IMMEDIATELY WHEN THE MODULE IS LOADED
# setup_logging()
# # --- END OF LOGGING SETUP ---


# # This logger will inherit settings from the 'app' logger or root logger
# logger = logging.getLogger(__name__) # __name__ will be 'app.main'

# # Import Firebase Admin SDK components
# import firebase_admin
# from firebase_admin import credentials, db as firebase_db

# # Import configuration settings
# from app.core.config import get_settings

# # Import database session dependency and table creation function
# from app.database.session import get_db, create_all_tables

# # Import API router
# from app.api.v1.api import api_router

# # Import background tasks
# from app.firebase_stream import process_firebase_events, cleanup_firebase
# from app.api.v1.endpoints.market_data_ws import redis_publisher_task, redis_market_data_broadcaster

# # Import Redis dependency and global instance
# from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
# from app.core.security import close_redis_connection

# # Import shared state (for the queue)
# from app.shared_state import redis_publish_queue

# # Get application settings
# settings = get_settings()
# logger.debug(f"API_V1_STR is set to: {settings.API_V1_STR}") # Using the logger instance from app.main

# # Initialize FastAPI app
# app = FastAPI(
#     title=settings.PROJECT_NAME,
#     openapi_url=f"{settings.API_V1_STR}/openapi.json"
# )

# # --- Static Files (if you have a frontend) ---
# # app.mount("/static", StaticFiles(directory="static"), name="static")

# # Store background tasks
# background_tasks = set()

# # --- Application Startup Event ---
# @app.on_event("startup")
# async def startup_event():
#     logger.info("Application startup event triggered.")
#     # ... (rest of your startup_event code remains the same as your last version) ...
#     # 1. Initialize Firebase Admin SDK
#     firebase_app_instance = None
#     try:
#         cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
#         if not os.path.exists(cred_path):
#             logger.critical(f"Firebase service account key file not found at: {cred_path}")
#         else:
#             cred = credentials.Certificate(cred_path)
#             if not firebase_admin._apps:
#                 firebase_app_instance = firebase_admin.initialize_app(cred, {
#                     'databaseURL': settings.FIREBASE_DATABASE_URL
#                 })
#                 logger.info("Firebase Admin SDK initialized successfully.")
#             else:
#                 firebase_app_instance = firebase_admin.get_app()
#                 logger.info("Firebase Admin SDK already initialized.")
#     except Exception as e:
#         logger.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)

#     # 2. Connect to Redis
#     global global_redis_client_instance
#     try:
#         global_redis_client_instance = await get_redis_client()
#         if global_redis_client_instance:
#             logger.info("Redis client initialized successfully.")
#         else:
#             logger.critical("Redis client failed to initialize (get_redis_client returned None).")
#     except Exception as e:
#         logger.critical(f"Failed to connect to Redis during startup: {e}", exc_info=True)
#         global_redis_client_instance = None

#     # 3. Create Database Tables
#     try:
#         await create_all_tables()
#         logger.info("Database tables ensured/created.")
#     except Exception as e:
#         logger.critical(f"Failed to create database tables: {e}", exc_info=True)

#     # 4. Start Background Tasks
#     if global_redis_client_instance and firebase_app_instance:
#         firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
#         firebase_task.set_name("firebase_listener")
#         background_tasks.add(firebase_task) 
#         firebase_task.add_done_callback(background_tasks.discard) 
#         logger.info("Firebase stream processing task scheduled.")

#         publisher_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
#         publisher_task.set_name("redis_publisher_task")
#         background_tasks.add(publisher_task)
#         publisher_task.add_done_callback(background_tasks.discard)
#         logger.info("Redis publisher task scheduled.")

#         broadcaster_task = asyncio.create_task(redis_market_data_broadcaster(global_redis_client_instance))
#         broadcaster_task.set_name("redis_market_data_broadcaster")
#         background_tasks.add(broadcaster_task)
#         broadcaster_task.add_done_callback(background_tasks.discard)
#         logger.info("Redis market data broadcaster task scheduled.")
#     else:
#         missing_services = []
#         if not global_redis_client_instance:
#             missing_services.append("Redis client")
#         if not firebase_app_instance:
#             missing_services.append("Firebase app instance")
#         logger.warning(f"Background tasks not started due to missing: {', '.join(missing_services) if missing_services else 'unknown reasons'}.")
#     logger.info("Application startup event finished.")

# # --- Application Shutdown Event ---
# @app.on_event("shutdown")
# async def shutdown_event():
#     logger.info("Application shutdown event triggered.")
#     # ... (rest of your shutdown_event code remains the same as your last version) ...
#     logger.info(f"Cancelling {len(background_tasks)} background tasks...")
#     for task in list(background_tasks): 
#         if not task.done():
#             task.cancel()
#             try:
#                 await task
#             except asyncio.CancelledError:
#                 logger.info(f"Background task '{task.get_name()}' cancelled successfully.")
#             except Exception as e:
#                 logger.error(f"Error during cancellation of background task '{task.get_name()}': {e}", exc_info=True)
    
#     global global_redis_client_instance
#     if global_redis_client_instance:
#         try:
#             await close_redis_connection(global_redis_client_instance)
#             logger.info("Redis client connection closed.")
#         except Exception as e:
#             logger.error(f"Error closing Redis connection: {e}", exc_info=True)
#         finally:
#              global_redis_client_instance = None 
#     else:
#         logger.warning("Redis client was not initialized or already closed during shutdown.")
    
#     try:
#         cleanup_firebase() 
#         logger.info("Firebase resources cleanup called.")
#     except Exception as e:
#         logger.error(f"Error during Firebase cleanup: {e}", exc_info=True)
#     logger.info("Application shutdown event finished.")

# # --- API Routers ---
# app.include_router(api_router, prefix=settings.API_V1_STR)

# # Example root endpoint
# @app.get("/")
# async def read_root():
#     return {"message": "Welcome to the Trading App Backend!"}

# if __name__ == "__main__":
#     logger.info("Running main.py directly (e.g., for development using 'python app/main.py').")
#     # For Uvicorn, run: uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
#     # import uvicorn
#     # uvicorn.run(app, host="0.0.0.0", port=8000)