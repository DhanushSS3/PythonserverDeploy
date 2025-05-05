# app/main.py

# Import necessary components from fastapi, including Query
from fastapi import FastAPI, Depends, HTTPException, status, Query # <-- Added Query
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os
import threading # Import threading for the Firebase stream
from typing import Optional # Import Optional if not already imported

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db # Alias db to avoid conflict with SQLAlchemy db variable

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import your database models to ensure they are registered with SQLAlchemy metadata
# This is necessary for create_all_tables to find them.
# If you have models in separate files, import them here or in app.database.models.__init__.py
from app.database import models # Assuming models are in app/database/models.py

# Import the main API router for version 1
from app.api.v1.api import api_router # Import the main API router

# Import Redis connection functions from security module
from app.core.security import connect_to_redis, close_redis_connection

# Import Firebase streaming functions and the shared data dictionary
from app.firebase_stream import start_firebase_streaming, stop_firebase_streaming, live_market_data # Import live_market_data for potential access

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
]

# Define the directory for static files (where uploaded proofs will be served from)
STATIC_DIRECTORY = "uploads" # This should match the parent directory of proofs
# Ensure the static directory exists
os.makedirs(STATIC_DIRECTORY, exist_ok=True)

# --- Firebase Initialization ---
firebase_app = None # To hold the initialized Firebase app instance
firebase_db_instance = None # To hold the Firebase Realtime Database instance
firebase_stream_thread = None # To hold the Firebase streaming thread

# Create the FastAPI application instance
app = FastAPI(
    title="Trading App Backend",
    description="FastAPI backend for a real-time trading application",
    version="1.0.0",
    openapi_tags=tags_metadata, # Include the tags metadata here
    # Add other FastAPI configurations here if needed
)

# --- Mount Static Files Directory ---
# This will serve files from the STATIC_DIRECTORY under the /static URL path
app.mount("/static", StaticFiles(directory=STATIC_DIRECTORY), name="static")


# --- Application Event Handlers ---

@app.on_event("startup")
async def startup_event():
    """
    Handles application startup events.
    Connects to the database and Redis, initializes Firebase,
    and starts the Firebase streaming thread.
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
    global firebase_app, firebase_db_instance, firebase_stream_thread
    try:
        # Use the service account key file path from settings
        cred = credentials.Certificate(settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
        firebase_app = firebase_admin.initialize_app(
            cred,
            {'databaseURL': settings.FIREBASE_DATABASE_URL}
        )
        firebase_db_instance = firebase_db # Get the database instance
        print("Firebase Admin SDK initialized successfully.")

        # --- Start Firebase Streaming Thread ---
        # Define the path in your Firebase Realtime Database to stream from
        # This should match where your financial data provider is writing data.
        # Example: 'datafeeds'
        firebase_data_path = 'datafeeds' # <<< Configure this path based on your Firebase structure

        if firebase_db_instance:
             print(f"Starting Firebase streaming thread for path: {firebase_data_path}")
             # Create and start the streaming thread
             firebase_stream_thread = threading.Thread(
                 target=start_firebase_streaming,
                 args=(firebase_db_instance, firebase_data_path),
                 daemon=True # Daemon threads exit automatically when the main program exits
             )
             firebase_stream_thread.start()
             print("Firebase streaming thread started.")
        else:
             print("Firebase DB instance not available, skipping stream start.")


    except FileNotFoundError:
        print(f"ERROR: Firebase service account key file not found at {settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH}")
        print("Firebase streaming will not start.")
    except Exception as e:
        print(f"ERROR initializing Firebase Admin SDK or starting stream: {e}")
        print("Firebase streaming will not start.")


    print("Application startup complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Handles application shutdown events.
    Stops the Firebase streaming thread and closes connections.
    """
    print("Application shutting down...")

    # Stop Firebase streaming thread
    print("Stopping Firebase streaming...")
    stop_firebase_streaming()
    if firebase_stream_thread and firebase_stream_thread.is_alive():
        # Give the thread a moment to finish, but don't block indefinitely
        firebase_stream_thread.join(timeout=5)
        if firebase_stream_thread.is_alive():
            print("Warning: Firebase streaming thread did not terminate gracefully.")

    # Close Redis connection
    print("Closing Redis connection...")
    await close_redis_connection()
    print("Redis connection closed.")

    # Note: Firebase Admin SDK doesn't have an explicit shutdown method
    # that needs to be called for basic usage like this.

    print("Application shutdown complete.")


# --- Include API Routers ---
# Include the main API router for version 1
app.include_router(api_router, prefix="/api/v1")

# --- Root Endpoint (for testing) ---
@app.get("/")
async def read_root():
    """
    A simple root endpoint to verify the application is running.
    """
    return {"message": "Trading App Backend is running!"}

# --- Endpoint to get the latest market data (for testing/debugging) ---
@app.get("/api/v1/market-data", summary="Get Latest Market Data",
         description="Retrieves the latest real-time market data fetched from Firebase.")
async def get_latest_market_prices(
    symbol: Optional[str] = Query(None, description="Optional symbol to get data for (e.g., AUDCAD)")
):
    """
    Retrieves the latest market data from the in-memory store.
    """
    # Access the live_market_data dictionary via the function in firebase_stream
    from app.firebase_stream import get_latest_market_data # Import the function to access data safely
    data = get_latest_market_data(symbol=symbol) # Use the function to get data

    if data is None and symbol is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Market data not found for symbol: {symbol}")
    # If symbol is None, get_latest_market_data returns the whole dictionary, which is fine.
    return data


# To run this application:
# 1. Make sure you are in your virtual environment.
# 2. Make sure you have installed uvicorn, python-jose[cryptography], redis, python-multipart, firebase_admin, qrcode, pillow.
# 3. Make sure your database and Redis servers are running.
# 4. Make sure your Firebase service account key file is accessible and the path is correct in .env.
# 5. Make sure your .env file has correct Firebase config (DATABASE_URL, API_KEY, etc.).
# 6. Configure the firebase_data_path variable in this file to match your Firebase RTDB structure.
# 7. Run from your project root directory: uvicorn app.main:app --reload
