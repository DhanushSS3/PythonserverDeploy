# app/main.py

# Import necessary components from fastapi, including Query
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os
import json # Import json for the publisher task
from typing import Optional # Import Optional if not already imported

# >>> Remove this import <<<
# import aioredis # Import aioredis for type hinting

# >>> Add this import for the type hint <<<
# Import the Redis client type from the standard redis library's asyncio module
from redis.asyncio import Redis

import logging # Import logging

# Configure basic logging early
# In a real production setup, you'd use a more advanced logger configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db # Alias db to avoid conflict with SQLAlchemy db variable

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import your database models to ensure they are registered with SQLAlchemy metadata
from app.database import models # Ensure models are imported so SQLAlchemy knows about them

# Import the main API router for version 1
from app.api.v1.api import api_router

# Import Redis connection functions from security module
# ONLY import the functions, NOT the global redis_client variable anymore
from app.core.security import connect_to_redis, close_redis_connection

# Import Firebase async event processing function and the get_latest_market_data snapshot function
# process_firebase_stream_events now puts data onto redis_publish_queue
from app.firebase_stream import process_firebase_stream_events, get_latest_market_data
logger.info("Successfully imported firebase_stream module.")

# Import the market data WebSocket router and the Redis broadcaster task function
try:
    # Import the router and the task function from market_data_ws.py
    from app.api.v1.endpoints import market_data_ws
    market_data_ws_router = market_data_ws.router
    # Import the function definition, it will be called with the redis client instance
    redis_market_data_broadcaster_func = market_data_ws.redis_market_data_broadcaster
    logger.info("Successfully imported market_data_ws module and router.")
except ImportError:
    logger.critical("FATAL ERROR: Could not import market_data_ws module. WebSocket endpoints and broadcasting will not work.")
    market_data_ws_router = None
    # Define a dummy broadcaster function that accepts redis_client but does nothing
    def redis_market_data_broadcaster_func(redis_client): # Dummy function needs to accept client
        async def dummy_broadcaster():
            logger.warning("Dummy Redis broadcaster running because market_data_ws could not be imported.")
            await asyncio.Future() # An awaitable that never completes
        return dummy_broadcaster() # Return the awaitable coroutine object


# Import the Redis publish queue from shared_state
try:
    from app.shared_state import redis_publish_queue # Corrected queue name from websocket_queue
    logger.info("Successfully imported redis_publish_queue from shared_state.")
except ImportError:
    logger.critical("FATAL ERROR: Could not import redis_publish_queue from app.shared_state. Redis publisher task cannot function.")
    # Define a dummy queue if import fails
    class DummyQueue:
        async def get(self):
            logger.warning("DummyQueue: get called, waiting indefinitely.")
            await asyncio.Future() # Wait forever
        def put_nowait(self, item):
             logger.warning("DummyQueue: put_nowait called, data discarded.")
    redis_publish_queue = DummyQueue()


# Define the Redis publisher task function here in main.py as it uses the global redis_client instance
async def redis_publisher_task(redis_client): # This task function accepts the redis client instance
    """
    Background task that reads from redis_publish_queue and publishes to Redis Pub/Sub.
    Requires an active Redis client instance.
    """
    logger.info("Redis publisher task started.")
    if not redis_client:
        logger.critical("Redis client not provided to publisher task. Redis publisher task cannot function.")
        while True:
            await asyncio.sleep(60) # Keep task alive but inactive
        # If Redis is strictly mandatory and failure to get client is fatal, you could raise:
        # raise ConnectionError("Redis client not available for publishing.")


    REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'
    logger.info(f"Redis publisher task publishing to channel '{REDIS_MARKET_DATA_CHANNEL}'.")

    while True:
        try:
            # Get data from the queue. This will block until data is available.
            # Data is expected to be in a format ready to be JSON dumped (dict or list).
            data_to_publish = await redis_publish_queue.get()
            # logger.debug(f"Got data from redis_publish_queue: {data_to_publish}") # Debug log


            if data_to_publish is not None:
                try:
                    message = json.dumps(data_to_publish)
                    # Use the passed async redis_client instance to publish
                    await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
                    # logger.debug(f"Published message to Redis channel '{REDIS_MARKET_DATA_CHANNEL}'.") # Debug log
                except Exception as e:
                    logger.error(f"Error publishing message to Redis: {e}", exc_info=True)
            else:
                logger.debug("Received None from redis_publish_queue, skipping publish.")

            # Mark the task as done for this item (important if queue is joined later)
            redis_publish_queue.task_done()

        except asyncio.CancelledError:
            logger.info("Redis publisher task cancelled.")
            break # Exit the loop
        except Exception as e:
            logger.critical(f"FATAL ERROR: Unexpected error in Redis publisher task: {e}", exc_info=True)
            # Add a small delay to prevent tight loop on errors
            await asyncio.sleep(1)


# Get application settings
settings = get_settings()

# Define tags metadata for Swagger UI
tags_metadata = [
    {"name": "users", "description": "Operations related to user accounts and authentication."},
    {"name": "groups", "description": "Trading group and portfolio management."},
    # The market_data tag is likely used by the WebSocket endpoint included via api_router now
    # {"name": "market_data", "description": "Real-time market data streaming via WebSockets."},
    # Add metadata for other tags as you create them
]

# Define the directory for static files (where uploaded proofs will be served from)
STATIC_DIRECTORY = "uploads"
# Ensure the directory exists
os.makedirs(STATIC_DIRECTORY, exist_ok=True)
logger.info(f"Ensured static directory '{STATIC_DIRECTORY}' exists.")

# --- Global variables for dependencies and background tasks ---
firebase_app = None # To hold the initialized Firebase app instance
firebase_db_instance = None # To hold the Firebase Realtime Database instance
# >>> Use the imported Redis type from redis.asyncio <<<
global_redis_client_instance: Optional[Redis] = None # To hold the *returned* Redis client instance
# >>> END CHANGE <<<

# Background task instances
firebase_event_processing_task: Optional[asyncio.Task] = None
redis_publisher_task_instance: Optional[asyncio.Task] = None
redis_broadcaster_task_instance: Optional[asyncio.Task] = None


# Create the FastAPI application instance
app = FastAPI(
    title="Trading App Backend",
    description="FastAPI backend for a real-time trading application",
    version="1.0.0",
    openapi_tags=tags_metadata,
    # Disable debug mode in production
    debug=False # Set to False for production
)

# --- Mount Static Files Directory ---
app.mount("/static", StaticFiles(directory=STATIC_DIRECTORY), name="static")
logger.info(f"Mounted static files from '{STATIC_DIRECTORY}' at '/static'.")


# --- Application Event Handlers ---

@app.on_event("startup")
async def startup_event():
    """
    Handles application startup events.
    Connects to dependencies, initializes Firebase, and starts background tasks.
    """
    logger.info("Application starting up...")

    # Database setup
    logger.info("Creating database tables (if they don't exist)...")
    try:
        await create_all_tables() # Assuming this awaits the table creation process
        logger.info("Database tables checked/created.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Failed to create database tables: {e}", exc_info=True)
        # In production, you might want to exit here if DB setup fails
        # import sys
        # sys.exit(1)
        raise # Re-raise to stop startup


    # Redis connection
    logger.info("Connecting to Redis...")
    global global_redis_client_instance # Declare intention to modify global variable
    try:
        # Call the function that *returns* the client instance
        global_redis_client_instance = await connect_to_redis()
        if global_redis_client_instance:
             logger.info("Redis connection status checked.")
        else:
             # Handle the case where connect_to_redis returned None (connection failed)
             logger.critical("FATAL ERROR: Failed to connect to Redis, connect_to_redis returned None.")
             # Depending on criticality, you might want to exit here
             # import sys
             # sys.exit(1)
             raise ConnectionError("Failed to connect to Redis") # Re-raise to stop startup if mandatory


    except Exception as e:
         logger.critical(f"FATAL ERROR: Unexpected error during Redis connection attempt: {e}", exc_info=True)
         # In production, you might want to exit here if Redis is mandatory
         # import sys
         # sys.exit(1)
         raise # Re-raise to stop startup


    # --- Initialize Firebase Admin SDK and Start Tasks ---
    global firebase_app, firebase_db_instance, firebase_event_processing_task, redis_publisher_task_instance, redis_broadcaster_task_instance
    try:
        # Use the service account key file path from settings
        # Ensure the file exists and is accessible in production
        if not os.path.exists(settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH):
             logger.critical(f"FATAL ERROR: Firebase service account key file not found at {settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH}")
             # In production, you must exit here
             # import sys
             # sys.exit(1)
             raise FileNotFoundError(f"Firebase service account key file not found at {settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH}")


        # Check if Firebase app is already initialized (important for reloader or multiple startups)
        if not firebase_admin._apps:
            cred = credentials.Certificate(settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
            firebase_app = firebase_admin.initialize_app(
                cred,
                {'databaseURL': settings.FIREBASE_DATABASE_URL}
            )
            logger.info("Firebase Admin SDK initialized successfully.")
        else:
            # If already initialized, get the default app
            firebase_app = firebase_admin.get_app()
            logger.info("Firebase Admin SDK already initialized.")


        firebase_db_instance = firebase_db # Get the database instance


        # --- Start Firebase Async Event Processing Task ---
        # This task now puts data onto the redis_publish_queue
        firebase_data_path = 'datafeeds' # <<< Configure this path based on your Firebase structure

        if firebase_db_instance and process_firebase_stream_events:
             logger.info(f"Starting Firebase async event processing task for path: {firebase_data_path}")
             # Create an asyncio task to run the async event processing function
             firebase_event_processing_task = asyncio.create_task(
                 process_firebase_stream_events(firebase_db_instance, firebase_data_path)
             )
             logger.info("Firebase async event processing task initiated.")
        else:
             logger.warning("Firebase DB instance or event processing function not available, skipping event processing task start.")


        # --- Start Redis Publisher Task ---
        # This task reads from the redis_publish_queue and publishes to Redis
        # Use the captured global_redis_client_instance
        if global_redis_client_instance: # Check if the client instance is available
            logger.info("Starting Redis publisher task...")
            # Pass the captured client instance to the task function
            redis_publisher_task_instance = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
            logger.info("Redis publisher task initiated.")
        else:
             # This should only happen if connect_to_redis failed AND didn't raise an exception
             logger.critical("Redis client not available, skipping Redis publisher task start.")


        # --- Start Redis Market Data Broadcaster Task ---
        # This task subscribes to Redis and broadcasts to local WebSockets
        # Use the captured global_redis_client_instance
        if global_redis_client_instance and redis_market_data_broadcaster_func: # Check if client instance and function are available
            logger.info("Starting Redis market data broadcaster task...")
            # Pass the captured client instance to the broadcaster task function
            redis_broadcaster_task_instance = asyncio.create_task(redis_market_data_broadcaster_func(global_redis_client_instance))
            logger.info("Redis market data broadcaster task initiated.")
        else:
             # This should only happen if connect_to_redis failed OR the broadcaster function wasn't imported
             logger.critical("Redis client or broadcaster function not available, skipping Redis broadcaster task start.")


    except Exception as e:
        logger.critical(f"FATAL ERROR: Error initializing Firebase Admin SDK or starting tasks: {e}", exc_info=True)
        # In production, you might want to exit here if Firebase is mandatory
        # import sys
        # sys.exit(1)
        raise # Re-raise to stop startup


    logger.info("Application startup complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Handles application shutdown events.
    Cancels background tasks and closes connections.
    """
    logger.info("Application shutting down...")

    # Cancel Firebase Event Processing Task
    logger.info("Cancelling Firebase event processing task...")
    global firebase_event_processing_task
    if firebase_event_processing_task and not firebase_event_processing_task.cancelled():
        firebase_event_processing_task.cancel()
        try:
            # Give it a short time to cancel gracefully
            await asyncio.wait_for(firebase_event_processing_task, timeout=5.0)
            logger.info("Firebase event processing task cancelled successfully.")
        except asyncio.CancelledError:
            logger.info("Firebase event processing task cancellation confirmed.")
        except asyncio.TimeoutError:
             logger.warning("Firebase event processing task did not cancel within timeout.")
        except Exception as e:
            logger.error(f"Error during Firebase event processing task cancellation: {e}", exc_info=True)


    # Cancel Redis Publisher Task
    logger.info("Cancelling Redis publisher task...")
    global redis_publisher_task_instance
    if redis_publisher_task_instance and not redis_publisher_task_instance.cancelled():
        redis_publisher_task_instance.cancel()
        try:
            await asyncio.wait_for(redis_publisher_task_instance, timeout=5.0)
            logger.info("Redis publisher task cancelled successfully.")
        except asyncio.CancelledError:
            logger.info("Redis publisher task cancellation confirmed.")
        except asyncio.TimeoutError:
             logger.warning("Redis publisher task did not cancel within timeout.")
        except Exception as e:
            logger.error(f"Error during Redis publisher task cancellation: {e}", exc_info=True)


    # Cancel Redis Market Data Broadcaster Task
    logger.info("Cancelling Redis market data broadcaster task...")
    global redis_broadcaster_task_instance
    if redis_broadcaster_task_instance and not redis_broadcaster_task_instance.cancelled():
        redis_broadcaster_task_instance.cancel()
        try:
            # Give it a short time to cancel gracefully
            await asyncio.wait_for(redis_broadcaster_task_instance, timeout=5.0)
            logger.info("Redis market data broadcaster task cancelled successfully.")
        except asyncio.CancelledError:
            logger.info("Redis market data broadcaster task cancellation confirmed.")
        except asyncio.TimeoutError:
             logger.warning("Redis market data broadcaster task did not cancel within timeout.")
        except Exception as e:
            logger.error(f"Error during Redis market data broadcaster task cancellation: {e}", exc_info=True)


    # Close Redis connection
    logger.info("Closing Redis connection...")
    global global_redis_client_instance # Declare intention to use global variable
    try:
        # Pass the captured client instance to the close function
        await close_redis_connection(global_redis_client_instance)
        logger.info("Redis connection closed.")
    except Exception as e:
        logger.error(f"Error closing Redis connection: {e}", exc_info=True)


    # Note: Firebase Admin SDK doesn't have an explicit shutdown method
    # that needs to be called for basic usage like this.

    logger.info("Application shutdown complete.")


# --- Include API Routers ---
# Include the main API router for v1 (which should include the WebSocket router)
app.include_router(api_router, prefix="/api/v1")

# The market_data_ws_router is typically included within api_router in the v1 api.py file.
# If you prefer to include it here, uncomment the block below and adjust api.py
# if market_data_ws_router:
#     app.include_router(market_data_ws_router, prefix="/api/v1") # Use the same prefix
#     logger.info("Market data WebSocket router included.")
# else:
#     logger.warning("Market data WebSocket router not available, skipping inclusion.")


# --- Root Endpoint (for testing) ---
@app.get("/")
async def read_root():
    """
    A simple root endpoint to verify the application is running.
    """
    return {"message": "Trading App Backend is running!"}

# --- Endpoint to get the latest market data (for testing/debugging) ---
@app.get("/api/v1/market-data", summary="Get Latest Market Data (Snapshot)",
         description="Retrieves a snapshot of the latest real-time market data from the in-memory store.")
async def get_latest_market_prices_snapshot(
    symbol: Optional[str] = Query(None, description="Optional symbol to get data for (e.g., AUDCAD)")
):
    """
    Retrieves a snapshot of the latest market data from the in-memory store.
    """
    # Access the live_market_data dictionary via the function in the firebase_stream module
    # Use the dummy function if the real one wasn't imported
    if get_latest_market_data:
        data = get_latest_market_data(symbol=symbol)
    else:
        logger.warning("get_latest_market_data function not available.")
        data = {}


    if data is None and symbol is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Market data not found for symbol: {symbol}")

    # Convert Decimal objects in data to strings for JSON serialization if necessary
    # (get_latest_market_data should ideally return JSON-serializable data)
    # Example conversion (if get_latest_market_data returned Decimals):
    # if isinstance(data, dict):
    #     def convert_decimals(obj):
    #         if isinstance(obj, Decimal):
    #             return str(obj)
    #         if isinstance(obj, dict):
    #             return {k: convert_decimals(v) for k, v in obj.items()}
    #         if isinstance(obj, list):
    #             return [convert_decimals(item) for item in obj]
    #         return obj
    #     data = convert_decimals(data)


    return data


# To run this application:
# 1. Make sure you are in your virtual environment.
# 2. Make sure you have installed uvicorn, python-jose[cryptography], redis, python-multipart, firebase_admin, qrcode, pillow, websockets, aioredis, pydantic-settings, python-dotenv, sqlalchemy, aiomysql.
# 3. Make sure your database and Redis servers are running.
# 4. Make sure your Firebase service account key file is accessible and the path is correct in .env.
# 5. Make sure your .env file has correct config for DB, Redis, Firebase, Email, Security.
# 6. Ensure app/database/models.py defines your SQLAlchemy models.
# 7. Ensure app/database/session.py has the get_db dependency and create_all_tables function.
# 8. Ensure app/api/v1/api.py includes the necessary routers (including market_data_ws_router).
# 9. Ensure app/shared_state.py defines the redis_publish_queue.
# 10. Configure the firebase_data_path variable in main.py to match your Firebase RTDB structure.
# 11. Run from your project root directory (the one containing the 'app' folder): uvicorn app.main:app --reload