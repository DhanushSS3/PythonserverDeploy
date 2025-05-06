# app/main.py

# Import necessary components from fastapi, including Query
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os
# Remove threading import as Firebase processing is now async
# import threading
from typing import Optional # Import Optional if not already imported

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db # Alias db to avoid conflict with SQLAlchemy db variable

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import your database models to ensure they are registered with SQLAlchemy metadata
from app.database import models

# Import the main API router for version 1 (which now includes the WebSocket router)
from app.api.v1.api import api_router

# Import Redis connection functions from security module
from app.core.security import connect_to_redis, close_redis_connection # Corrected import name

# Import Firebase async event processing function and the shared data dictionary
# Using the async processing function from firebase_stream
from app.firebase_stream import process_firebase_stream_events, get_latest_market_data # Import get_latest_market_data

# Import WebSocket data sender task (assuming you have market_data_ws.py)
# This import is only needed for the background task, not for router inclusion anymore.
try:
    from app.api.v1.endpoints.market_data_ws import websocket_data_sender
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("Could not import websocket_data_sender from app.api.v1.endpoints.market_data_ws. WebSocket sending will not start.")
    # Define a dummy task creator
    def websocket_data_sender():
        async def dummy_sender():
            await asyncio.Future() # An awaitable that never completes
        return dummy_sender()


# Get application settings
settings = get_settings()

# Define tags metadata for Swagger UI
tags_metadata = [
    {
        "name": "users",
        "description": "Operations related to user accounts and authentication.",
    },
    {
        "name": "groups",
        "description": "Trading group and portfolio management.",
    },
    # Add metadata for other tags as you create them:
    # {
    #     "name": "orders",
    #     "description": "Trade order management.",
    # },
    # {
    #     "name": "wallets",
    #     "description": "User wallet and transaction management.",
    # },
    # {
    #     "name": "symbols",
    #     "description": "Trading symbol information.",
    # },
    {
        "name": "market_data", # Add tag for market data (used by WebSocket endpoint)
        "description": "Real-time market data streaming via WebSockets.",
    },
]

# Define the directory for static files (where uploaded proofs will be served from)
STATIC_DIRECTORY = "uploads"
os.makedirs(STATIC_DIRECTORY, exist_ok=True)

# --- Global variables for Firebase and background tasks ---
firebase_app = None # To hold the initialized Firebase app instance
firebase_db_instance = None # To hold the Firebase Realtime Database instance
firebase_event_processing_task = None # To hold the asyncio task for Firebase event processing
websocket_sender_task = None # To hold the asyncio task for sending data


# Create the FastAPI application instance
app = FastAPI(
    title="Trading App Backend",
    description="FastAPI backend for a real-time trading application",
    version="1.0.0",
    openapi_tags=tags_metadata,
)

# --- Mount Static Files Directory ---
app.mount("/static", StaticFiles(directory=STATIC_DIRECTORY), name="static")


# --- Application Event Handlers ---

@app.on_event("startup")
async def startup_event():
    """
    Handles application startup events.
    Connects to the database and Redis, initializes Firebase,
    and starts background tasks for Firebase streaming and WebSocket sending.
    """
    print("Application starting up...")

    # Database setup
    print("Creating database tables (if they don't exist)...")
    await create_all_tables()
    print("Database tables checked/created.")

    # Redis connection
    print("Connecting to Redis...")
    await connect_to_redis()
    print("Redis connection status checked.")

    # --- Initialize Firebase Admin SDK ---
    global firebase_app, firebase_db_instance, firebase_event_processing_task, websocket_sender_task
    try:
        # Use the service account key file path from settings
        cred = credentials.Certificate(settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
        firebase_app = firebase_admin.initialize_app(
            cred,
            {'databaseURL': settings.FIREBASE_DATABASE_URL}
        )
        firebase_db_instance = firebase_db # Get the database instance
        print("Firebase Admin SDK initialized successfully.")

        # --- Start Firebase Async Event Processing Task ---
        # Define the path in your Firebase Realtime Database to stream from
        # This should match where your financial data provider is writing data.
        # Example: 'datafeeds'
        firebase_data_path = 'datafeeds' # <<< Configure this path based on your Firebase structure

        if firebase_db_instance:
             print(f"Starting Firebase async event processing task for path: {firebase_data_path}")
             # Create an asyncio task to run the async event processing function
             firebase_event_processing_task = asyncio.create_task(
                 process_firebase_stream_events(firebase_db_instance, firebase_data_path)
             )
             print("Firebase async event processing task initiated.")
        else:
             print("Firebase DB instance not available, skipping event processing task start.")


        # --- Start WebSocket Data Sender Background Task ---
        # Start the task if the function was successfully imported
        print("Starting WebSocket data sender task...")
        websocket_sender_task = asyncio.create_task(websocket_data_sender())
        print("WebSocket data sender task initiated.")


    except FileNotFoundError:
        print(f"ERROR: Firebase service account key file not found at {settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH}")
        print("Firebase initialization and streaming will not start.")
    except Exception as e:
        print(f"ERROR initializing Firebase Admin SDK or starting tasks: {e}")
        print("Firebase initialization and streaming will not start.")


    print("Application startup complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Handles application shutdown events.
    Cancels background tasks and closes connections.
    """
    print("Application shutting down...")

    # Cancel Firebase Event Processing Task
    print("Cancelling Firebase event processing task...")
    global firebase_event_processing_task
    if firebase_event_processing_task:
        firebase_event_processing_task.cancel()
        try:
            await firebase_event_processing_task # Await cancellation
            print("Firebase event processing task cancelled successfully.")
        except asyncio.CancelledError:
            print("Firebase event processing task cancellation confirmed.")
        except Exception as e:
            print(f"Error during Firebase event processing task cancellation: {e}")


    # Cancel WebSocket Data Sender Task
    print("Cancelling WebSocket data sender task...")
    global websocket_sender_task
    if websocket_sender_task:
        websocket_sender_task.cancel()
        try:
            await websocket_sender_task # Await cancellation
            print("WebSocket data sender task cancelled successfully.")
        except asyncio.CancelledError:
            print("WebSocket data sender task cancellation confirmed.")
        except Exception as e:
            print(f"Error during WebSocket data sender task cancellation: {e}")


    # Close Redis connection
    print("Closing Redis connection...")
    await close_redis_connection() # Corrected function name back to close_redis_connection
    print("Redis connection closed.")

    # Note: Firebase Admin SDK doesn't have an explicit shutdown method
    # that needs to be called for basic usage like this.

    print("Application shutdown complete.")


# --- Include API Routers ---
# Include the main API router for v1 (which now includes the WebSocket router)
app.include_router(api_router, prefix="/api/v1")


# --- Root Endpoint (for testing) ---
@app.get("/")
async def read_root():
    """
    A simple root endpoint to verify the application is running.
    """
    return {"message": "Trading App Backend is running!"}

# --- Endpoint to get the latest market data (for testing/debugging) ---
# This endpoint still works by reading from the shared live_market_data dictionary,
# which is now populated by the async Firebase event processor.
@app.get("/api/v1/market-data", summary="Get Latest Market Data (Snapshot)",
         description="Retrieves a snapshot of the latest real-time market data from the in-memory store.")
async def get_latest_market_prices_snapshot(
    symbol: Optional[str] = Query(None, description="Optional symbol to get data for (e.g., AUDCAD)")
):
    """
    Retrieves a snapshot of the latest market data from the in-memory store.
    """
    # Access the live_market_data dictionary via the function in the firebase_stream module
    from app.firebase_stream import get_latest_market_data # Import the function to access data safely
    data = get_latest_market_data(symbol=symbol) # Use the function to get data

    if data is None and symbol is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Market data not found for symbol: {symbol}")
    return data


# To run this application:
# 1. Make sure you are in your virtual environment.
# 2. Make sure you have installed uvicorn, python-jose[cryptography], redis, python-multipart, firebase_admin, qrcode, pillow, websockets.
# 3. Make sure your database and Redis servers are running.
# 4. Make sure your Firebase service account key file is accessible and the path is correct in .env.
# 5. Make sure your .env file has correct config.
# 6. Configure the firebase_data_path variable in this file to match your Firebase RTDB structure.
# 7. Run from your project root directory: uvicorn app.main:app --reload
