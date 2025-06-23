# app/main.py

# --- Environment Variable Loading ---
# This must be at the very top, before any other app modules are imported.
from dotenv import load_dotenv
load_dotenv()

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os
import json
from typing import Optional, Any
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
from app.services.order_processing import generate_unique_10_digit_id
from app.database.models import UserOrder

# Import stop loss and take profit checker
from app.services.pending_orders import check_and_trigger_stoploss_takeprofit

settings = get_settings()
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- CORS Settings ---
# Define specific origins for better security
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5500",
    "http://localhost:8000",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8080",
    # Add your production domains here
    "https://yourdomain.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Use specific origins
    allow_credentials=False,  # Allow credentials
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["Content-Type", "Authorization", "X-Total-Count"]
)
# --- End CORS Settings ---

scheduler: Optional[AsyncIOScheduler] = None

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
                
            # Get all active users (both live and demo) using the new unified function
            live_users, demo_users = await crud_user.get_all_active_users_both(db)
            
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
                    if not group_name:
                        logger.warning(f"User {user_id} has no group_name set. Skipping portfolio update.")
                        continue
                    group_symbol_settings = await get_group_symbol_settings_cache(global_redis_client_instance, group_name, "ALL")
                    if not group_symbol_settings:
                        logger.warning(f"No group settings found for group {group_name}. Skipping portfolio update for user {user_id}.")
                        continue
                    
                    # Get open orders for this user
                    order_model = crud_order.get_order_model(user_type)
                    open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                    open_positions = []
                    for o in open_orders_orm:
                        open_positions.append({
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', None),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        })
                    
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
                    
                    # After portfolio update or order execution, log details if relevant
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION][PORTFOLIO_UPDATE] user_id={user_id}, user_type={user_type}, group_name={group_name}, free_margin={dynamic_portfolio_data.get('free_margin', 'N/A')}, margin_level={dynamic_portfolio_data.get('margin_level', 'N/A')}, balance={dynamic_portfolio_data.get('balance', 'N/A')}, equity={dynamic_portfolio_data.get('equity', 'N/A')}")
                    
                except Exception as user_error:
                    logger.error(f"Error updating portfolio for user {user_id}: {user_error}", exc_info=True)
                    continue
            
            logger.debug("Finished update_all_users_dynamic_portfolio job")
    except Exception as e:
        logger.error(f"Error in update_all_users_dynamic_portfolio job: {e}", exc_info=True)

# --- Auto-cutoff function for margin calls ---
async def handle_margin_cutoff(db: AsyncSession, redis_client: Redis, user_id: int, user_type: str, margin_level: Decimal):
    """
    Handles auto-cutoff for users whose margin level falls below the critical threshold.
    - For non-Barclays users, it closes all open positions locally.
    - For Barclays users, it sends close requests to the service provider via Firebase.
    """
    try:
        logger.warning(f"AUTO-CUTOFF: Initiating for user {user_id} ({user_type}) with margin level {margin_level}%")

        # Determine if the user is a Barclays live user
        is_barclays_live_user = False
        user_for_cutoff = None
        if user_type == "live":
            user_for_cutoff = await crud_user.get_user_by_id(db, user_id=user_id)
            if user_for_cutoff and user_for_cutoff.group_name:
                group_settings = await get_group_symbol_settings_cache(redis_client, user_for_cutoff.group_name)
                if group_settings.get('sending_orders', '').lower() == 'barclays':
                    is_barclays_live_user = True
        else: # demo user
            user_for_cutoff = await crud_user.get_demo_user_by_id(db, user_id)

        if not user_for_cutoff:
            logger.error(f"AUTO-CUTOFF: Could not find user {user_id} to perform cutoff.")
            return

        order_model = crud_order.get_order_model(user_type)
        open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)

        if not open_orders:
            logger.info(f"No open positions found for user {user_id} during auto-cutoff.")
            return

        logger.warning(f"AUTO-CUTOFF: Found {len(open_orders)} positions for user {user_id}. Barclays user: {is_barclays_live_user}")

        # --- Conditional logic based on user type ---
        if is_barclays_live_user:
            # For Barclays users, send close requests to Firebase
            logger.info(f"AUTO-CUTOFF: Processing Barclays user {user_id}. Sending close requests to Firebase.")
            for order in open_orders:
                try:
                    close_id = await generate_unique_10_digit_id(db, UserOrder, 'close_id')
                    
                    firebase_close_data = {
                        "action": "close_order",
                        "close_id": close_id,
                        "order_id": order.order_id,
                        "user_id": user_id,
                        "symbol": order.order_company_name,
                        "order_type": order.order_type,
                        "order_status": order.order_status,
                        "status": "close", # As requested
                        "order_quantity": str(order.order_quantity),
                        "contract_value": str(order.contract_value),
                        "timestamp": datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    
                    await send_order_to_firebase(firebase_close_data, "live")
                    
                    # Update the local order to reflect the cutoff request
                    update_fields = {
                        "close_id": close_id,
                        "close_message": f"Auto-cutoff triggered at margin level {margin_level}%. Close request sent to provider."
                    }
                    await crud_order.update_order_with_tracking(
                        db, order, update_fields, user_id, user_type, "AUTO_CUTOFF_REQUESTED"
                    )
                    await db.commit()

                    logger.info(f"AUTO-CUTOFF: Close request sent for Barclays order {order.order_id} with close_id {close_id}.")

                except Exception as e:
                    logger.error(f"AUTO-CUTOFF: Error sending close request for Barclays order {order.order_id}: {e}", exc_info=True)
            
            # After sending all requests, publish an update to notify the user's frontend.
            await publish_order_update(redis_client, user_id)
            logger.warning(f"AUTO-CUTOFF: Finished sending close requests for Barclays user {user_id}.")

        else:
            # For non-Barclays users (live or demo), close orders directly
            logger.info(f"AUTO-CUTOFF: Processing non-Barclays user {user_id}. Closing orders locally.")
            from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
            
            total_net_profit = Decimal('0.0')

            for order in open_orders:
                try:
                    symbol = order.order_company_name
                    last_price = await get_last_known_price(redis_client, symbol)
                    
                    if not last_price:
                        logger.error(f"Cannot close position {order.order_id} - no price available for {symbol}")
                        continue

                    # Determine close price based on order type
                    close_price_str = last_price.get('o') if order.order_type == 'BUY' else last_price.get('b')
                    close_price = Decimal(str(close_price_str))

                    if not close_price or close_price <= 0:
                        logger.error(f"Cannot close position {order.order_id} - invalid price {close_price} for {symbol}")
                        continue
                    
                    # Simplified P/L calculation for cutoff
                    entry_price = order.order_price
                    ext_symbol_info = await get_external_symbol_info_by_symbol(db, symbol)
                    contract_size = Decimal(str(ext_symbol_info.contract_size)) if ext_symbol_info else Decimal('100000')
                    quantity = order.order_quantity
                    
                    price_diff = (close_price - entry_price) if order.order_type == 'BUY' else (entry_price - close_price)
                    
                    # Note: This simplified P/L does not account for commission or currency conversion.
                    # For a simple cutoff, this is acceptable. The main goal is liquidation.
                    net_profit = price_diff * quantity * contract_size
                    
                    # Update order fields for closure
                    order.close_price = close_price
                    order.order_status = 'CLOSED'
                    order.close_message = f"Auto-cutoff: margin level {margin_level}%"
                    order.net_profit = net_profit
                    total_net_profit += net_profit
                    
                    logger.info(f"AUTO-CUTOFF: Closing local order {order.order_id} with P/L: {net_profit}")

                except Exception as e:
                    logger.error(f"Error processing local closure for order {order.order_id}: {e}", exc_info=True)

            # After processing all orders, update user's financials
            user_for_cutoff.wallet_balance += total_net_profit
            user_for_cutoff.margin = Decimal('0') # Reset margin to zero
            await db.commit()
            
            # Notify the user via WebSocket
            await publish_order_update(redis_client, user_id)
            await publish_user_data_update(redis_client, user_id)
            
            logger.warning(f"AUTO-CUTOFF: Completed for non-Barclays user {user_id}. All positions closed locally.")
            
    except Exception as e:
        logger.error(f"Error in handle_margin_cutoff for user {user_id}: {e}", exc_info=True)

# --- Service Provider JWT Rotation Job ---
async def rotate_service_account_jwt():
    """
    Generates a JWT for the Barclays service provider, prints it to the console,
    and pushes it to Firebase. This job is scheduled to run periodically.
    """
    logger.info("APScheduler: Starting rotate_service_account_jwt job...")
    try:
        service_name = "barclays_service_provider"
        # Generate a token valid for 35 minutes. It will be refreshed every 30 minutes.
        token = create_service_account_token(service_name, expires_minutes=35)

        # Path in Firebase to store the token
        jwt_ref = firebase_db.reference(f"service_provider_credentials/{service_name}")
        
        # Payload to store in Firebase
        payload = {
            "jwt": token,
            "updated_at": datetime.utcnow().isoformat()
        }
        jwt_ref.set(payload)
        
        logger.info(f"SUCCESS: Service account JWT for '{service_name}' was generated and pushed to Firebase.")
        
        # Print the token to the console for debugging, as requested
        print("\n" + "="*50)
        print("     NEW SERVICE ACCOUNT JWT TOKEN (refreshed)     ")
        print("="*50)
        print(f"\nService Name: {service_name}")
        print(f"Generated at: {datetime.now().isoformat()}")
        print("\nToken:")
        print(token)
        print("\n" + "="*50 + "\n")
        logger.info("APScheduler: Finished rotate_service_account_jwt job successfully.")

    except Exception as e:
        logger.error(f"FAILURE: Error in rotate_service_account_jwt job. Could not push token to Firebase: {e}", exc_info=True)

# Add this line after the app initialization
background_tasks = set()

@app.on_event("startup")
async def startup_event():
    global scheduler
    global background_tasks  # Make sure we're using the global set
    global global_redis_client_instance
    logger.info("Application startup event triggered.")
    
    # Initialize Firebase
    try:
        cred_path = os.path.join(os.path.dirname(__file__), '..', settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
        if not os.path.exists(cred_path):
            logger.error(f"Firebase credentials file not found at: {cred_path}")
            raise FileNotFoundError(f"Firebase credentials file not found at: {cred_path}")
            
        cred = credentials.Certificate(cred_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'databaseURL': settings.FIREBASE_DATABASE_URL
            })
            logger.info("Firebase Admin SDK initialized successfully")
        else:
            logger.info("Firebase Admin SDK already initialized")
    except Exception as e:
        logger.error(f"Error initializing Firebase Admin SDK: {e}", exc_info=True)
    
    # Initialize Redis connection pool
    redis_available = False
    try:
        redis_client = await get_redis_client()
        if redis_client:
            ping_result = await redis_client.ping()
            if ping_result:
                logger.info("Redis connection pool initialized successfully")
                redis_available = True
                global_redis_client_instance = redis_client
            else:
                logger.warning("Redis ping returned False - continuing without Redis")
    except Exception as e:
        logger.warning(f"Redis connection failed - continuing without Redis: {e}")
    
    # Initialize APScheduler
    try:
        scheduler = AsyncIOScheduler()
        
        # Add daily swap charges job (runs at 00:05 UTC every day)
        scheduler.add_job(
            daily_swap_charge_job,
            CronTrigger(hour=0, minute=5),  # 00:05 UTC
            id='daily_swap_charge_job',
            replace_existing=True
        )
        
        # Add dynamic portfolio update job (runs every minute)
        scheduler.add_job(
            update_all_users_dynamic_portfolio,
            IntervalTrigger(minutes=1),
            id='update_all_users_dynamic_portfolio',
            replace_existing=True
        )
        
        # Add service account JWT rotation job (runs every 30 minutes)
        scheduler.add_job(
            rotate_service_account_jwt,
            IntervalTrigger(minutes=30),
            id='rotate_service_account_jwt',
            replace_existing=True
        )
        
        # Start the scheduler
        scheduler.start()
        logger.info("APScheduler started successfully with jobs configured")
    except Exception as e:
        logger.error(f"Error initializing APScheduler: {e}", exc_info=True)
    
    # Start the Firebase events processor background task
    try:
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        logger.info("Firebase events processor background task started")
    except Exception as e:
        logger.error(f"Error starting Firebase events processor: {e}", exc_info=True)
    
    # Start Redis-dependent tasks only if Redis is available
    if redis_available and global_redis_client_instance:
        try:
            # Start the Redis publisher background task
            redis_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
            background_tasks.add(redis_task)
            redis_task.add_done_callback(background_tasks.discard)
            logger.info("Redis publisher background task started")
            
            # Start the stop loss/take profit checker background task
            stoploss_task = asyncio.create_task(run_stoploss_takeprofit_checker())
            background_tasks.add(stoploss_task)
            stoploss_task.add_done_callback(background_tasks.discard)
            logger.info("Stop loss/take profit checker background task started")
        except Exception as e:
            logger.error(f"Error starting Redis-dependent tasks: {e}", exc_info=True)
    else:
        logger.warning("Redis is not available - skipping Redis-dependent tasks")
    
    # Create initial service account token and push to Firebase on startup
    try:
        logger.info("Generating initial service account token...")
        await rotate_service_account_jwt()
    except Exception as e:
        logger.error(f"Error creating initial service account token: {e}", exc_info=True)
    
    logger.info("Application startup event finished.")

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

async def run_stoploss_takeprofit_checker():
    """Background task to continuously check for stop loss and take profit conditions"""
    logger = logging.getLogger("stoploss_takeprofit_checker")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.database.session import AsyncSessionLocal
    
    logger.info("Starting stop loss/take profit checker background task")
    
    while True:
        try:
            # Create a new session for each check
            try:
                async with AsyncSessionLocal() as db:
                    # Get Redis client
                    try:
                        redis_client = await get_redis_client()
                        if not redis_client:
                            logger.warning("Redis client not available for SL/TP check - skipping this cycle")
                            await asyncio.sleep(10)  # Wait longer if Redis is not available
                            continue
                        
                        # Run the check
                        from app.services.pending_orders import check_and_trigger_stoploss_takeprofit
                        await check_and_trigger_stoploss_takeprofit(db, redis_client)
                    except Exception as e:
                        logger.error(f"Error getting Redis client for SL/TP check: {e}", exc_info=True)
                        await asyncio.sleep(10)  # Wait longer if there was an error
                        continue
            except Exception as session_error:
                logger.error(f"Error creating database session: {session_error}", exc_info=True)
                await asyncio.sleep(10)  # Wait longer if there was a session error
                continue
                
            # Sleep for a short time before the next check
            await asyncio.sleep(5)  # Check every 5 seconds
            
        except Exception as e:
            logger.error(f"Error in stop loss/take profit checker: {e}", exc_info=True)
            await asyncio.sleep(10)  # Wait longer if there was an error

# --- New Pending Order Checker Task ---
async def run_pending_order_checker():
    """
    Continuously runs the pending order and SL/TP checker in the background.
    """

    # Give the application a moment to initialize everything else
    await asyncio.sleep(5) 
    logger.info("Starting the pending order and SL/TP checker background task.")
    
    while True:
        try:
            async with AsyncSessionLocal() as db:
                if global_redis_client_instance:
                    logger.debug("Executing check_and_trigger_stoploss_takeprofit...")
                    await check_and_trigger_stoploss_takeprofit(db, global_redis_client_instance)
                else:
                    logger.warning("Pending order checker: Global Redis client not available, skipping run.")
        except Exception as e:
            logger.error(f"Error in pending order checker loop: {e}", exc_info=True)
        
        # Wait for 1 second before the next check to avoid overloading the system
        await asyncio.sleep(1)