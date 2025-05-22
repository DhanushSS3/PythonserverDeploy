# app/main.py

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os
import json
from typing import Optional, Any

import logging

# --- APScheduler Imports ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# --- Custom Service and DB Session for Scheduler ---
from app.services.swap_service import apply_daily_swap_charges_for_all_open_orders
# Corrected import: Use AsyncSessionLocal instead of SessionLocal
from app.database.session import AsyncSessionLocal # To create a new session for the scheduled task


# Configure basic logging early
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.getLogger('app.services.portfolio_calculator').setLevel(logging.DEBUG)
logging.getLogger('app.services.swap_service').setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables # get_db and create_all_tables are still fine

# Import API router
from app.api.v1.api import api_router

# Import background tasks
from app.firebase_stream import process_firebase_events
from app.api.v1.endpoints.market_data_ws import redis_publisher_task, redis_market_data_broadcaster

# Import Redis dependency and global instance
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.core.security import close_redis_connection

# Import shared state (for the queue)
from app.shared_state import redis_publish_queue


# Get application settings
settings = get_settings()
# print(f"DEBUG: API_V1_STR is set to: {settings.API_V1_STR}") # Already present
# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- Global Scheduler Instance ---
scheduler: Optional[AsyncIOScheduler] = None

# --- Scheduled Job Function ---
async def daily_swap_charge_job():
    """
    Scheduled job to apply daily swap charges.
    Creates its own database session and uses the global Redis client.
    """
    logger.info("APScheduler: Executing daily_swap_charge_job...")
    # Corrected usage: Use AsyncSessionLocal
    async with AsyncSessionLocal() as db: # Create a new DB session for this job
        if global_redis_client_instance:
            try:
                await apply_daily_swap_charges_for_all_open_orders(db, global_redis_client_instance)
                logger.info("APScheduler: Daily swap charge job completed successfully.")
            except Exception as e:
                logger.error(f"APScheduler: Error during daily_swap_charge_job: {e}", exc_info=True)
        else:
            logger.error("APScheduler: Cannot execute daily_swap_charge_job - Global Redis client not available.")


# --- Application Startup Event ---
@app.on_event("startup")
async def startup_event():
    """
    Application startup event: Initialize Firebase Admin SDK, connect to Redis,
    create database tables, start background tasks, and initialize APScheduler.
    """
    global scheduler, global_redis_client_instance
    logger.info("Application startup event triggered.")

    # 1. Initialize Firebase Admin SDK
    firebase_app_instance = None
    try:
        cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
        if not os.path.exists(cred_path):
             logger.critical(f"Firebase service account key file not found at: {cred_path}")
        else:
            cred = credentials.Certificate(cred_path)
            if not firebase_admin._apps:
                 firebase_app_instance = firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL
                 })
                 logger.info("Firebase Admin SDK initialized successfully.")
            else:
                 firebase_app_instance = firebase_admin.get_app()
                 logger.info("Firebase Admin SDK already initialized.")
    except Exception as e:
        logger.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)

    # 2. Connect to Redis
    try:
        global_redis_client_instance = await get_redis_client()
        if global_redis_client_instance:
             logger.info("Redis client initialized successfully.")
        else:
             logger.critical("Redis client failed to initialize.")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis during startup: {e}", exc_info=True)
        global_redis_client_instance = None

    # 3. Create Database Tables
    try:
        await create_all_tables()
        logger.info("Database tables ensured/created.")
    except Exception as e:
        logger.critical(f"Failed to create database tables: {e}", exc_info=True)

    # --- Initialize and Start APScheduler ---
    if global_redis_client_instance:
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(
            daily_swap_charge_job,
            trigger=CronTrigger(hour=0, minute=0, second=0, timezone="UTC"),
            # trigger=CronTrigger(hour=12, minute=33, second=0, timezone="UTC"),
            id="daily_swap_task",
            name="Daily Swap Charges",
            replace_existing=True
        )
        try:
            scheduler.start()
            logger.info("APScheduler started and daily_swap_charge_job scheduled for 00:00 UTC.")
        except Exception as e:
            logger.error(f"Failed to start APScheduler: {e}", exc_info=True)
            scheduler = None
    else:
        logger.warning("APScheduler for daily swap charges not started because Redis client is not available.")

    # 4. Start Other Background Tasks
    if global_redis_client_instance and firebase_app_instance:
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        firebase_task.set_name("firebase_listener")
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        logger.info("Firebase stream processing task scheduled.")

        publisher_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
        publisher_task.set_name("redis_publisher_task")
        background_tasks.add(publisher_task)
        publisher_task.add_done_callback(background_tasks.discard)
        logger.info("Redis publisher task scheduled.")

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
        logger.warning(f"Other background tasks (Firebase listener, Redis publisher/broadcaster) not started due to missing: {', '.join(missing_services)}.")

    logger.info("Application startup event finished.")


# Store background tasks
background_tasks = set()

# --- Application Shutdown Event ---
@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, global_redis_client_instance
    logger.info("Application shutdown event triggered.")

    # Shut down APScheduler
    if scheduler and scheduler.running:
        try:
            scheduler.shutdown(wait=True)
            logger.info("APScheduler shut down gracefully.")
        except Exception as e:
            logger.error(f"Error shutting down APScheduler: {e}", exc_info=True)

    # Cancel other background tasks
    logger.info(f"Cancelling {len(background_tasks)} other background tasks...")
    for task in list(background_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Background task '{task.get_name()}' cancelled successfully.")
            except Exception as e:
                logger.error(f"Error during cancellation of background task '{task.get_name()}': {e}", exc_info=True)

    # Close Redis connection
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


# --- API Routers ---
app.include_router(api_router, prefix=settings.API_V1_STR)

# Example root endpoint
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Trading App Backend!"}