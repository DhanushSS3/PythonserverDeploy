# app/main.py

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os
import json
from typing import Optional, Any
from dotenv import load_dotenv
from datetime import datetime
from decimal import Decimal
from redis.asyncio import Redis

import logging

# --- APScheduler Imports ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# --- Custom Service and DB Session for Scheduler ---
from app.services.swap_service import apply_daily_swap_charges_for_all_open_orders
from app.database.session import AsyncSessionLocal

# Import portfolio calculator
from app.services.portfolio_calculator import calculate_user_portfolio
from app.core.cache import (
    get_user_data_cache, 
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache, 
    set_user_dynamic_portfolio_cache,
    get_last_known_price,
    publish_order_update
)
from app.crud import crud_order, user as crud_user

# --- CORS Middleware Import ---
from fastapi.middleware.cors import CORSMiddleware

# Configure basic logging early
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('app.services.portfolio_calculator').setLevel(logging.DEBUG)
logging.getLogger('app.services.swap_service').setLevel(logging.DEBUG)

# Configure file logging for specific modules to logs/orders.log
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'orders.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Orders endpoint logger to file
orders_ep_logger = logging.getLogger('app.api.v1.endpoints.orders')
orders_ep_logger.setLevel(logging.DEBUG)
orders_fh = logging.FileHandler(log_file_path)
orders_fh.setFormatter(file_formatter)
orders_ep_logger.addHandler(orders_fh)
orders_ep_logger.propagate = False # Prevent console output from basicConfig

# Order processing service logger to file
order_proc_logger = logging.getLogger('app.services.order_processing')
order_proc_logger.setLevel(logging.DEBUG)
order_proc_fh = logging.FileHandler(log_file_path) # Use the same file handler or a new one if separate formatting is needed
order_proc_fh.setFormatter(file_formatter)
order_proc_logger.addHandler(order_proc_fh)
order_proc_logger.propagate = False # Prevent console output from basicConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import API router
from app.api.v1.api import api_router

# Import background tasks
from app.firebase_stream import process_firebase_events
# REMOVE: from app.api.v1.endpoints.market_data_ws import redis_market_data_broadcaster
from app.api.v1.endpoints.market_data_ws import redis_publisher_task # Keep publisher

# Import Redis dependency and global instance
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.core.security import close_redis_connection, create_service_account_token

# Import shared state (for the queue)
from app.shared_state import redis_publish_queue

# Import orders logger
from app.core.logging_config import orders_logger

settings = get_settings()
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- CORS Settings (Allow All Origins) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=False, # Must be False if allow_origins is ["*"]
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
)
# --- End CORS Settings ---

scheduler: Optional[AsyncIOScheduler] = None

load_dotenv() 

# Now, you can safely print and access them
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

print(f"--- Application Startup ---")
print(f"Loaded SECRET_KEY (from code): '{SECRET_KEY}'")
print(f"Loaded ALGORITHM (from code): '{ALGORITHM}'")
print(f"---------------------------")

# Log application startup
orders_logger.info("Application starting up - Orders logging initialized")

# --- Scheduled Job Functions ---
async def daily_swap_charge_job():
    logger.info("APScheduler: Executing daily_swap_charge_job...")
    async with AsyncSessionLocal() as db:
        if global_redis_client_instance:
            try:
                await apply_daily_swap_charges_for_all_open_orders(db, global_redis_client_instance)
                logger.info("APScheduler: Daily swap charge job completed successfully.")
            except Exception as e:
                logger.error(f"APScheduler: Error during daily_swap_charge_job: {e}", exc_info=True)
        else:
            logger.error("APScheduler: Cannot execute daily_swap_charge_job - Global Redis client not available.")

# --- New Dynamic Portfolio Update Job ---
async def update_all_users_dynamic_portfolio():
    """
    Background task that updates the dynamic portfolio data (free_margin, margin_level)
    for all users, regardless of whether they are connected via WebSockets.
    This is critical for autocutoff and validation.
    """
    try:
        logger.debug("Starting update_all_users_dynamic_portfolio job")
        async with AsyncSessionLocal() as db:
            if not global_redis_client_instance:
                logger.error("Cannot update dynamic portfolios - Redis client not available")
                return
                
            # Get all active users (both live and demo)
            live_users = await crud_user.get_all_active_users(db)
            demo_users = await crud_user.get_all_active_demo_users(db)
            
            all_users = []
            for user in live_users:
                all_users.append({"id": user.id, "user_type": "live", "group_name": user.group_name})
            for user in demo_users:
                all_users.append({"id": user.id, "user_type": "demo", "group_name": user.group_name})
            
            logger.debug(f"Found {len(all_users)} active users to update portfolios")
            
            # Process each user
            for user_info in all_users:
                user_id = user_info["id"]
                user_type = user_info["user_type"]
                group_name = user_info["group_name"]
                
                try:
                    # Get user data from cache or DB
                    user_data = await get_user_data_cache(global_redis_client_instance, user_id, db, user_type)
                    if not user_data:
                        logger.warning(f"No user data found for user {user_id} ({user_type}). Skipping portfolio update.")
                        continue
                    
                    # Get group symbol settings
                    group_symbol_settings = await get_group_symbol_settings_cache(global_redis_client_instance, group_name, "ALL")
                    if not group_symbol_settings:
                        logger.warning(f"No group settings found for group {group_name}. Skipping portfolio update for user {user_id}.")
                        continue
                    
                    # Get open orders for this user
                    order_model = crud_order.get_order_model(user_type)
                    open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                    open_positions = [o.to_dict() for o in open_orders_orm]
                    
                    if not open_positions:
                        # Skip portfolio calculation for users without open positions
                        continue
                    
                    # Get adjusted market prices for all relevant symbols
                    adjusted_market_prices = {}
                    for symbol in group_symbol_settings.keys():
                        # Try to get adjusted prices from cache
                        adjusted_prices = await get_adjusted_market_price_cache(global_redis_client_instance, group_name, symbol)
                        if adjusted_prices:
                            adjusted_market_prices[symbol] = {
                                'buy': adjusted_prices.get('buy'),
                                'sell': adjusted_prices.get('sell')
                            }
                        else:
                            # Fallback to last known price
                            last_price = await get_last_known_price(global_redis_client_instance, symbol)
                            if last_price:
                                adjusted_market_prices[symbol] = {
                                    'buy': last_price.get('b'),  # Use raw price as fallback
                                    'sell': last_price.get('o')
                                }
                    
                    # Define margin thresholds based on group settings or defaults
                    # These could be stored in the database or group settings
                    margin_call_threshold = Decimal('100.0')  # Default 100%
                    margin_cutoff_threshold = Decimal('50.0')  # Default 50%
                    
                    # Calculate portfolio metrics with margin call detection
                    portfolio_metrics = await calculate_user_portfolio(
                        user_data=user_data,
                        open_positions=open_positions,
                        adjusted_market_prices=adjusted_market_prices,
                        group_symbol_settings=group_symbol_settings,
                        redis_client=global_redis_client_instance,
                        margin_call_threshold=margin_call_threshold
                    )
                    
                    # Cache the dynamic portfolio data
                    dynamic_portfolio_data = {
                        "balance": portfolio_metrics.get("balance", "0.0"),
                        "equity": portfolio_metrics.get("equity", "0.0"),
                        "margin": portfolio_metrics.get("margin", "0.0"),
                        "free_margin": portfolio_metrics.get("free_margin", "0.0"),
                        "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
                        "margin_level": portfolio_metrics.get("margin_level", "0.0"),
                        "positions_with_pnl": portfolio_metrics.get("positions", []),
                        "margin_call": portfolio_metrics.get("margin_call", False)
                    }
                    await set_user_dynamic_portfolio_cache(global_redis_client_instance, user_id, dynamic_portfolio_data)
                    
                    # Check for margin call conditions
                    margin_level = Decimal(portfolio_metrics.get("margin_level", "0.0"))
                    if margin_level > Decimal('0') and margin_level < margin_cutoff_threshold:
                        logger.warning(f"CRITICAL: User {user_id} margin level {margin_level}% below cutoff threshold {margin_cutoff_threshold}%. Initiating auto-cutoff.")
                        await handle_margin_cutoff(db, global_redis_client_instance, user_id, user_type, margin_level)
                    elif portfolio_metrics.get("margin_call", False):
                        logger.warning(f"User {user_id} has margin call condition: margin level {margin_level}%")
                    
                except Exception as user_error:
                    logger.error(f"Error updating portfolio for user {user_id}: {user_error}", exc_info=True)
                    continue
            
            logger.debug("Finished update_all_users_dynamic_portfolio job")
    except Exception as e:
        logger.error(f"Error in update_all_users_dynamic_portfolio job: {e}", exc_info=True)

# --- Auto-cutoff function for margin calls ---
async def handle_margin_cutoff(db: AsyncSession, redis_client: Redis, user_id: int, user_type: str, margin_level: Decimal):
    """
    Handle auto-cutoff for users whose margin level falls below the critical threshold.
    This function will close all open positions for the user.
    
    Args:
        db: Database session
        redis_client: Redis client
        user_id: User ID
        user_type: User type (live or demo)
        margin_level: Current margin level
    """
    try:
        logger.warning(f"AUTO-CUTOFF: Closing all positions for user {user_id} due to low margin level ({margin_level}%)")
        
        # Get all open positions for this user
        order_model = crud_order.get_order_model(user_type)
        open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        
        if not open_orders:
            logger.info(f"No open positions found for user {user_id} during auto-cutoff")
            return
            
        logger.warning(f"AUTO-CUTOFF: Found {len(open_orders)} positions to close for user {user_id}")
        
        # Close each position
        for order in open_orders:
            try:
                # Get current market price for the symbol
                symbol = order.order_company_name
                adjusted_prices = await get_adjusted_market_price_cache(redis_client, order.group_name, symbol)
                
                # Determine close price based on order type
                close_price = None
                if adjusted_prices:
                    if order.order_type == 'BUY':
                        close_price = adjusted_prices.get('sell')  # Sell at bid price
                    else:
                        close_price = adjusted_prices.get('buy')  # Buy at ask price
                
                if not close_price:
                    # Fallback to last known price
                    last_price = await get_last_known_price(redis_client, symbol)
                    if last_price:
                        if order.order_type == 'BUY':
                            close_price = last_price.get('o')  # Sell at bid price
                        else:
                            close_price = last_price.get('b')  # Buy at ask price
                
                if not close_price:
                    logger.error(f"Cannot close position {order.order_id} - no price available for {symbol}")
                    continue
                
                # Calculate PnL
                entry_price = order.order_price
                contract_size = Decimal('100000')  # Default
                quantity = order.order_quantity
                
                # Close the position
                order.close_price = close_price
                order.close_time = datetime.now()
                order.order_status = 'CLOSED'
                order.close_message = f"Auto-cutoff: margin level {margin_level}% below threshold"
                
                # Calculate net profit
                if order.order_type == 'BUY':
                    price_diff = close_price - entry_price
                else:
                    price_diff = entry_price - close_price
                
                net_profit = price_diff * quantity * contract_size
                order.net_profit = net_profit
                
                # Update the database
                await db.commit()
                logger.info(f"AUTO-CUTOFF: Closed position {order.order_id} for user {user_id} at price {close_price}")
            
            except Exception as e:
                logger.error(f"Error closing position {order.order_id} for user {user_id}: {e}", exc_info=True)
        
        # Update user's margin
        await crud_user.update_user_margin(db, redis_client, user_id, Decimal('0'), user_type)
        
        # Notify the user via WebSocket
        await publish_order_update(redis_client, user_id)
        
        logger.warning(f"AUTO-CUTOFF: Completed for user {user_id}. All positions closed.")
        
    except Exception as e:
        logger.error(f"Error in handle_margin_cutoff for user {user_id}: {e}", exc_info=True)

# --- New JWT Rotation Job ---
async def rotate_service_account_jwt():
    try:
        token = create_service_account_token("python_bridge", expires_minutes=30)
        jwt_ref = firebase_db.reference("trade_data/service_auth_token")
        jwt_ref.set({"token": token})
        logger.info("Service account JWT pushed to Firebase.")
        logger.info(f"Generated service account JWT: {token}")
    except Exception as e:
        logger.error(f"Error generating or pushing service JWT to Firebase: {e}", exc_info=True)

@app.on_event("startup")
async def startup_event():
    global scheduler, global_redis_client_instance
    logger.info("Application startup event triggered.")

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

    try:
        global_redis_client_instance = await get_redis_client()
        if global_redis_client_instance:
            logger.info("Redis client initialized successfully.")
        else:
            logger.critical("Redis client failed to initialize.")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis during startup: {e}", exc_info=True)
        global_redis_client_instance = None

    try:
        await create_all_tables()
        logger.info("Database tables ensured/created.")
    except Exception as e:
        logger.critical(f"Failed to create database tables: {e}", exc_info=True)

    # Initialize and Start APScheduler
    if global_redis_client_instance:
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(
            daily_swap_charge_job,
            trigger=CronTrigger(hour=0, minute=0, second=0, timezone="UTC"),
            id="daily_swap_task",
            name="Daily Swap Charges",
            replace_existing=True
        )
        scheduler.add_job(
            rotate_service_account_jwt,
            trigger=CronTrigger(minute="*/30", timezone="UTC"),
            id="rotate_service_jwt",
            name="Rotate service account JWT for python_bridge",
            replace_existing=True
        )
        # Add the new dynamic portfolio update job - runs every 5 seconds
        scheduler.add_job(
            update_all_users_dynamic_portfolio,
            trigger=IntervalTrigger(seconds=5, timezone="UTC"),
            id="dynamic_portfolio_update",
            name="Update all users' dynamic portfolio data",
            replace_existing=True
        )
        try:
            scheduler.start()
            logger.info("APScheduler started with jobs for daily swap, JWT rotation, and dynamic portfolio updates.")
            await rotate_service_account_jwt() # Run once on startup
        except Exception as e:
            logger.error(f"Failed to start APScheduler: {e}", exc_info=True)
            scheduler = None
    else:
        logger.warning("Scheduler not started: Redis client unavailable.")

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

        # REMOVE broadcaster_task scheduling
        # broadcaster_task = asyncio.create_task(redis_market_data_broadcaster(global_redis_client_instance))
        # broadcaster_task.set_name("redis_market_data_broadcaster")
        # background_tasks.add(broadcaster_task)
        # broadcaster_task.add_done_callback(background_tasks.discard)
        # logger.info("Redis market data broadcaster task scheduled.")
    else:
        missing_services = []
        if not global_redis_client_instance:
            missing_services.append("Redis client")
        if not firebase_app_instance:
            missing_services.append("Firebase app instance")
        logger.warning(f"Other background tasks not started due to missing: {', '.join(missing_services)}.")

    logger.info("Application startup event finished.")

    if global_redis_client_instance:
        try:
            pong = await global_redis_client_instance.ping()
            logger.info(f"Redis ping response: {pong}")
        except Exception as e:
            logger.critical(f"Redis ping failed post-init: {e}", exc_info=True)

background_tasks = set()

@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, global_redis_client_instance
    logger.info("Application shutdown event triggered.")

    if scheduler and scheduler.running:
        try:
            scheduler.shutdown(wait=True)
            logger.info("APScheduler shut down gracefully.")
        except Exception as e:
            logger.error(f"Error shutting down APScheduler: {e}", exc_info=True)

    logger.info(f"Cancelling {len(background_tasks)} other background tasks...")
    for task in list(background_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Background task '{task.get_name()}' cancelled successfully.")
            except Exception as e:
                logger.error(f"Error during cancellation of task '{task.get_name()}': {e}", exc_info=True)

    if global_redis_client_instance:
        await close_redis_connection(global_redis_client_instance)
        global_redis_client_instance = None
        logger.info("Redis client connection closed.")
    else:
        logger.warning("Redis client was not initialized or already closed.")

    from app.firebase_stream import cleanup_firebase
    cleanup_firebase()

    logger.info("Application shutdown event finished.")

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Trading App Backend!"}