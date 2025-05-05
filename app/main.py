# app/main.py

from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles # Import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os # Import os

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import your database models to ensure they are registered with SQLAlchemy metadata
from app.database import models

# Import the main API router for version 1
from app.api.v1.api import api_router

# Import Redis connection functions from security module
from app.core.security import connect_to_redis, close_redis_connection

# Get application settings
settings = get_settings()

# Define tags metadata for Swagger UI
tags_metadata = [
    {
        "name": "users",
        "description": "Operations related to user accounts and authentication.",
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
    #     "name": "groups",
    #     "description": "Trading group/portfolio management.",
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

# Create the FastAPI application instance
app = FastAPI(
    title="Trading App Backend",
    description="FastAPI backend for a real-time trading application",
    version="1.0.0",
    openapi_tags=tags_metadata,
)

# --- Mount Static Files Directory ---
# This will serve files from the STATIC_DIRECTORY under the /static URL path
app.mount("/static", StaticFiles(directory=STATIC_DIRECTORY), name="static")


# --- Application Event Handlers ---

@app.on_event("startup")
async def startup_event():
    """
    Handles application startup events.
    Connects to the database and Redis, and creates tables (for development).
    """
    print("Application starting up...")
    print("Creating database tables (if they don't exist)...")
    await create_all_tables()
    print("Database tables checked/created.")

    print("Connecting to Redis...")
    await connect_to_redis()
    print("Redis connection status checked.")

    print("Application startup complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Handles application shutdown events.
    Clean up resources, e.g., close database and Redis connections.
    """
    print("Application shutting down...")
    # Close database connections if necessary (SQLAlchemy engine manages pool)

    print("Closing Redis connection...")
    await close_redis_connection()
    print("Redis connection closed.")

    print("Application shutdown complete.")


# --- Include API Routers ---
app.include_router(api_router, prefix="/api/v1")

# --- Root Endpoint (for testing) ---
@app.get("/")
async def read_root():
    """
    A simple root endpoint to verify the application is running.
    """
    return {"message": "Trading App Backend is running!"}

# --- Example of how to use a database session in an endpoint (Placeholder) ---
# @app.get("/test-db")
# async def test_db_connection(db: AsyncSession = Depends(get_db)):
#     """
#     Endpoint to test the database connection by querying the user count.
#     """
#     try:
#         from sqlalchemy import select
#         user_count = await db.execute(select(models.User).select_from(models.User))
#         count = len(user_count.scalars().all())
#         return {"database_connection_ok": True, "user_count": count}
#     except Exception as e:
#         print(f"Database connection test failed: {e}")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# --- Example of how to use settings in an endpoint (Placeholder) ---
# @app.get("/settings")
# async def show_settings():
#     """
#     Endpoint to show some application settings (for debugging).
#     Be careful not to expose sensitive information!
#     """
#     return {
#         "database_name": settings.DATABASE_NAME,
#         "email_host": settings.EMAIL_HOST,
#         "otp_expiry_minutes": settings.OTP_EXPIRATION_MINUTES
#     }

# To run this application:
# 1. Make sure you are in your virtual environment.
# 2. Make sure you have installed uvicorn, python-jose[cryptography], redis, python-multipart.
# 3. Make sure your database and Redis servers are running.
# 4. Run from your project root directory: uvicorn app.main:app --reload
