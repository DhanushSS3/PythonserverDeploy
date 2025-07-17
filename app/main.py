# app/main.py
# Version 1.3.5
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

# --- Trading Configuration ---
# Epsilon value for SL/TP accuracy (floating-point precision tolerance)
# For forex (5 decimal places), use 0.00001 as tolerance
SLTP_EPSILON = Decimal('0.00001')

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
    set_user_data_cache,
    get_user_data_cache, 
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache, 
    set_user_dynamic_portfolio_cache,
    get_last_known_price,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_balance_margin_cache,
    REDIS_MARKET_DATA_CHANNEL,
    decode_decimal,
    get_group_settings_cache,
    set_group_settings_cache
)
from app.crud import crud_order, user as crud_user
from app.core.firebase import send_order_to_firebase

# --- CORS Middleware Import ---
from fastapi.middleware.cors import CORSMiddleware

# Import pre-configured loggers from logging_config
from app.core.logging_config import (
    orders_logger, 
    autocutoff_logger, 
    app_logger,
    database_logger,
    firebase_logger,
    redis_logger,
    security_logger,
    market_data_logger,
    cache_logger,
    frontend_orders_logger,
    service_provider_logger,
    firebase_comm_logger,
    orders_crud_logger,
    jwt_security_logger,
    error_logger,
    money_requests_logger,
    websocket_logger
)

# Use the main app logger for this module
logger = app_logger

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

# Import order processing utilities
from app.services.order_processing import generate_unique_10_digit_id
from app.database.models import UserOrder, DemoUser

# Import stop loss and take profit checker - moved inside function to avoid circular import

# Import adjusted price worker
from app.services.adjusted_price_worker import adjusted_price_worker

settings = get_settings()

IS_PRODUCTION = os.getenv("ENVIRONMENT", "development").lower() == "production"

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json", # Keep this as it is for now, as it just exposes the JSON schema
    # docs_url=None if IS_PRODUCTION else "/docs",        # Disables Swagger UI in production
    # redoc_url=None if IS_PRODUCTION else "/redoc"      # Disables ReDoc in production
)


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
    "https://livefxhub.com"
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
HOST = os.getenv("DATABASE_HOST")

print(f"--- Application Startup ---")
print(f"Loaded SECRET_KEY (from code): '{SECRET_KEY}'")
print(f"Loaded ALGORITHM (from code): '{ALGORITHM}'")
print(f"Loaded HOST (from code): '{HOST}'")
print(f"---------------------------")

# Log application startup
logger.info("Application starting up - Trading system initialized")
logger.info(f"SL/TP Epsilon accuracy configured: {SLTP_EPSILON}")
logger.info(f"Environment: {'PRODUCTION' if os.getenv('ENVIRONMENT', 'development').lower() == 'production' else 'DEVELOPMENT'}")

# --- Scheduled Job Functions ---
async def daily_swap_charge_job():
    logger.info("Executing daily swap charge job...")
    async with AsyncSessionLocal() as db:
        if global_redis_client_instance:
            try:
                await apply_daily_swap_charges_for_all_open_orders(db, global_redis_client_instance)
                logger.info("Daily swap charge job completed successfully")
            except Exception as e:
                logger.error(f"Error during daily swap charge job: {e}", exc_info=True)
        else:
            logger.error("Cannot execute daily swap charge job - Redis client not available")

# --- New Dynamic Portfolio Update Job ---
async def update_all_users_dynamic_portfolio():
    """
    Background task that updates the dynamic portfolio data (free_margin, margin_level)
    for all users, regardless of whether they are connected via WebSockets.
    This is critical for autocutoff and validation.
    """
    from app.services.email import send_email
    logger.info("[AUTO-CUTOFF] Starting update_all_users_dynamic_portfolio job...")
    try:
        async with AsyncSessionLocal() as db:
            if not global_redis_client_instance:
                logger.error("[AUTO-CUTOFF] Cannot update dynamic portfolios - Redis client not available")
                return
            
            # Heartbeat log to ensure this task is still running
            logger.info("[AUTO-CUTOFF] update_all_users_dynamic_portfolio heartbeat: task is alive and running.")
            
            # Get all active users (both live and demo) using the new unified function
            logger.info("[AUTO-CUTOFF] Fetching all active users (live and demo)...")
            live_users, demo_users = await crud_user.get_all_active_users_both(db)
            logger.info(f"[AUTO-CUTOFF] Found {len(live_users)} live users and {len(demo_users)} demo users.")
            
            all_users = []
            for user in live_users:
                all_users.append({"id": user.id, "user_type": "live", "group_name": user.group_name})
            for user in demo_users:
                all_users.append({"id": user.id, "user_type": "demo", "group_name": user.group_name})
            
            logger.info(f"[AUTO-CUTOFF] Processing {len(all_users)} users for dynamic portfolio update...")
            # Process each user
            for user_info in all_users:
                user_id = user_info["id"]
                user_type = user_info["user_type"]
                group_name = user_info["group_name"]
                logger.info(f"[AUTO-CUTOFF] Processing user {user_id} ({user_type}), group: {group_name}")
                
                try:
                    # Get user data from cache or DB
                    logger.debug(f"[AUTO-CUTOFF] Getting user data for user {user_id} ({user_type}) from cache/DB...")
                    user_data = await get_user_data_cache(global_redis_client_instance, user_id, db, user_type)
                    if not user_data:
                        logger.warning(f"[AUTO-CUTOFF] No user data found for user {user_id} ({user_type}). Skipping portfolio update.")
                        continue
                    
                    # Get group symbol settings
                    if not group_name:
                        logger.warning(f"[AUTO-CUTOFF] User {user_id} has no group_name set. Skipping portfolio update.")
                        continue
                    logger.debug(f"[AUTO-CUTOFF] Getting group symbol settings for group {group_name}...")
                    group_symbol_settings = await get_group_symbol_settings_cache(global_redis_client_instance, group_name, "ALL")
                    logger.debug(f"[AUTO-CUTOFF] Group symbol settings for group '{group_name}' from cache: {group_symbol_settings}")
                    # Robust fallback: If cache is missing or sending_orders is None, fetch from DB and update cache
                    if group_symbol_settings is None or group_symbol_settings.get('sending_orders') is None:
                        logger.warning(f"[AUTO-CUTOFF] Group symbol settings missing or incomplete in cache for '{group_name}', fetching from DB.")
                        from app.crud.group import get_group_by_name
                        db_group = await get_group_by_name(db, group_name)
                        logger.debug(f"[AUTO-CUTOFF] DB group fetch result for '{group_name}': {db_group}")
                        if db_group:
                            settings = {
                                "sending_orders": getattr(db_group[0] if isinstance(db_group, list) else db_group, 'sending_orders', None),
                                # Add more group-level settings here if needed
                            }
                            await set_group_settings_cache(global_redis_client_instance, group_name, settings)
                            group_symbol_settings = settings
                            logger.info(f"[AUTO-CUTOFF] Group symbol settings fetched from DB and cached for group '{group_name}': {settings}")
                        else:
                            logger.error(f"[AUTO-CUTOFF] Group symbol settings not found in DB for group '{group_name}'. Skipping portfolio update for user {user_id}.")
                            continue
                    if not group_symbol_settings:
                        logger.warning(f"[AUTO-CUTOFF] No group settings found for group {group_name}. Skipping portfolio update for user {user_id}.")
                        continue
                    
                    # Get open orders for this user
                    logger.debug(f"[AUTO-CUTOFF] Getting open orders for user {user_id}...")
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
                    logger.debug(f"[AUTO-CUTOFF] User {user_id} has {len(open_positions)} open orders.")
                    
                    # Also include users with pending orders
                    pending_statuses = ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]
                    logger.debug(f"[AUTO-CUTOFF] Getting pending orders for user {user_id}...")
                    pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
                    pending_positions = []
                    for po in pending_orders_orm:
                        pending_positions.append({
                            'order_id': getattr(po, 'order_id', None),
                            'order_company_name': getattr(po, 'order_company_name', None),
                            'order_type': getattr(po, 'order_type', None),
                            'order_quantity': getattr(po, 'order_quantity', None),
                            'order_price': getattr(po, 'order_price', None),
                            'margin': getattr(po, 'margin', None),
                            'contract_value': getattr(po, 'contract_value', None),
                            'stop_loss': getattr(po, 'stop_loss', None),
                            'take_profit': getattr(po, 'take_profit', None),
                            'commission': getattr(po, 'commission', None),
                            'order_status': getattr(po, 'order_status', None),
                            'order_user_id': getattr(po, 'order_user_id', None)
                        })
                    logger.debug(f"[AUTO-CUTOFF] User {user_id} has {len(pending_positions)} pending orders.")

                    if not open_positions and not pending_positions:
                        logger.warning(f"[AUTO-CUTOFF] User {user_id} has no open or pending orders. Skipping portfolio update.")
                        continue

                    # Combine open and pending positions for portfolio calculation if needed
                    all_positions = open_positions + pending_positions
                    logger.debug(f"[AUTO-CUTOFF] User {user_id} total positions for portfolio calculation: {len(all_positions)}")
                    
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
                    logger.debug(f"[AUTO-CUTOFF] User {user_id} adjusted market prices: {adjusted_market_prices}")
                    
                    # Define margin thresholds based on group settings or defaults
                    margin_call_threshold = Decimal('100.0')  # Default 100%
                    margin_cutoff_threshold = Decimal('10.0')  # Default 10%
                    logger.debug(f"[AUTO-CUTOFF] User {user_id} margin thresholds: call={margin_call_threshold}, cutoff={margin_cutoff_threshold}")
                    
                    # Calculate portfolio metrics with margin call detection
                    logger.info(f"[AUTO-CUTOFF] Calculating portfolio metrics for user {user_id}...")
                    portfolio_metrics = await calculate_user_portfolio(
                        user_data=user_data,
                        open_positions=open_positions,
                        adjusted_market_prices=adjusted_market_prices,
                        group_symbol_settings=group_symbol_settings,
                        redis_client=global_redis_client_instance,
                        margin_call_threshold=margin_call_threshold
                    )
                    logger.info(f"[AUTO-CUTOFF] Portfolio metrics for user {user_id}: {portfolio_metrics}")
                    
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
                    logger.debug(f"[AUTO-CUTOFF] Caching dynamic portfolio data for user {user_id}: {dynamic_portfolio_data}")
                    await set_user_dynamic_portfolio_cache(global_redis_client_instance, user_id, dynamic_portfolio_data)
                    
                    # Check for margin call conditions
                    margin_level = Decimal(portfolio_metrics.get("margin_level", "0.0"))
                    margin_call_email_key = f"margin_call_email_sent:{user_id}"
                    logger.info(f"[AUTO-CUTOFF] User {user_id} margin_level: {margin_level}")
                    if margin_level > Decimal('0') and margin_level < margin_cutoff_threshold:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] User {user_id} margin level {margin_level}% below cutoff threshold {margin_cutoff_threshold}%. Initiating auto-cutoff.")
                        await handle_margin_cutoff(db, global_redis_client_instance, user_id, user_type, margin_level)
                        # Optionally clear the email flag so user can be notified again after recovery
                        # await global_redis_client_instance.delete(margin_call_email_email_key)
                        await global_redis_client_instance.delete(margin_call_email_key)
                    elif margin_level > margin_cutoff_threshold and margin_level < margin_call_threshold:
                        # Only send margin call warning email once per event
                        already_sent = await global_redis_client_instance.get(margin_call_email_key)
                        if not already_sent:
                            from app.crud.user import get_user_email
                            user_email = await get_user_email(global_redis_client_instance, db, user_id, user_type)
                            if user_email:
                                # Use the HTML template email
                                from app.services.email import send_margin_call_email
                                dashboard_url = "https://livefxhub.com"  # TODO: Replace with actual dashboard URL
                                try:
                                    await send_margin_call_email(user_email, str(round(margin_level, 2)), dashboard_url)
                                    logger.info(f"[AUTO-CUTOFF] Sent margin call warning email (HTML) to user {user_id} at {user_email}")
                                    # Set flag for 24 hours (or until margin recovers)
                                    await global_redis_client_instance.set(margin_call_email_key, "1", ex=24*60*60)
                                except Exception as e:
                                    logger.error(f"[AUTO-CUTOFF] Failed to send margin call warning email to user {user_id} at {user_email}: {e}")
                            else:
                                logger.warning(f"[AUTO-CUTOFF] Could not send margin call warning email: No email found for user {user_id}")
                    elif margin_level >= margin_call_threshold:
                        # Margin has recovered, clear the flag so user can be notified again in the future
                        await global_redis_client_instance.delete(margin_call_email_key)
                    elif portfolio_metrics.get("margin_call", False):
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] User {user_id} has margin call condition: margin level {margin_level}%")
                    
                    logger.info(f"[AUTO-CUTOFF] Portfolio update completed for user {user_id}")
                    
                except Exception as user_error:
                    logger.error(f"[AUTO-CUTOFF] Error updating portfolio for user {user_id}: {user_error}", exc_info=True)
                    continue
            
    except Exception as e:
        logger.error(f"[AUTO-CUTOFF] Error in update_all_users_dynamic_portfolio job: {e}", exc_info=True)

# --- Auto-cutoff function for margin calls ---
async def handle_margin_cutoff(db: AsyncSession, redis_client: Redis, user_id: int, user_type: str, margin_level: Decimal):
    """
    Handles auto-cutoff for users whose margin level falls below the critical threshold.
    """
    from app.core.cache import get_group_settings_cache, set_group_settings_cache
    from app.services.order_processing import generate_unique_10_digit_id
    from datetime import datetime, timezone
    try:
        autocutoff_logger.debug(f"[AUTO-CUTOFF] handle_margin_cutoff called for user {user_id} (type: {user_type}), margin_level={margin_level}")
        is_barclays_live_user = False
        user_for_cutoff = None
        if user_type == "live":
            autocutoff_logger.debug(f"[AUTO-CUTOFF] Fetching live user {user_id} from DB...")
            user_for_cutoff = await crud_user.get_user_by_id(db, user_id=user_id, user_type=user_type)
            if user_for_cutoff and user_for_cutoff.group_name:
                autocutoff_logger.debug(f"[AUTO-CUTOFF] User {user_id} group_name: {user_for_cutoff.group_name}")
                group_settings = await get_group_settings_cache(redis_client, user_for_cutoff.group_name)
                autocutoff_logger.debug(f"[AUTO-CUTOFF] Group settings for group '{user_for_cutoff.group_name}': {group_settings}")
                # Fallback: fetch from DB if not found in cache
                if group_settings is None:
                    autocutoff_logger.warning(f"[AUTO-CUTOFF] Group settings not found in cache for group '{user_for_cutoff.group_name}', fetching from DB.")
                    from app.crud.group import get_group_by_name
                    db_group = await get_group_by_name(db, user_for_cutoff.group_name)
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] DB group fetch result: {db_group}")
                    if db_group:
                        settings = {
                            "sending_orders": getattr(db_group[0] if isinstance(db_group, list) else db_group, 'sending_orders', None),
                            # Add more group-level settings here if needed
                        }
                        await set_group_settings_cache(redis_client, user_for_cutoff.group_name, settings)
                        group_settings = settings
                        autocutoff_logger.info(f"[AUTO-CUTOFF] Group settings fetched from DB and cached for group '{user_for_cutoff.group_name}'")
                    else:
                        autocutoff_logger.error(f"[AUTO-CUTOFF] Group settings not found in DB for group '{user_for_cutoff.group_name}'. Cannot proceed with auto-cutoff for user {user_id}.")
                        return
                if group_settings.get('sending_orders', '').lower() == 'barclays':
                    is_barclays_live_user = True
                    autocutoff_logger.info(f"[AUTO-CUTOFF] User {user_id} identified as Barclays live user")
                else:
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] User {user_id} is not a Barclays live user (sending_orders={group_settings.get('sending_orders')})")
            else:
                autocutoff_logger.debug(f"[AUTO-CUTOFF] User {user_id} has no group_name, skipping Barclays check.")
        else:
            autocutoff_logger.debug(f"[AUTO-CUTOFF] Fetching demo user {user_id} from DB...")
            user_for_cutoff = await crud_user.get_demo_user_by_id(db, user_id)

        if not user_for_cutoff:
            autocutoff_logger.error(f"[AUTO-CUTOFF] User {user_id} not found in database")
            return

        order_model = crud_order.get_order_model(user_type)
        open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        autocutoff_logger.debug(f"[AUTO-CUTOFF] User {user_id} has {len(open_orders) if open_orders else 0} open orders.")

        if not open_orders:
            autocutoff_logger.warning(f"[AUTO-CUTOFF] No open orders found for user {user_id}")
            return

        autocutoff_logger.warning(f"[AUTO-CUTOFF] Found {len(open_orders)} open orders for user {user_id}")

        if is_barclays_live_user:
            autocutoff_logger.warning(f"[AUTO-CUTOFF] Processing Barclays live user {user_id} - sending close requests to Firebase")
            for order in open_orders:
                try:
                    # --- NEW: Check Redis flag before sending close request ---
                    autocutoff_flag_key = f"autocutoff_close_sent:{order.order_id}"
                    already_sent = await redis_client.get(autocutoff_flag_key)
                    if already_sent:
                        autocutoff_logger.info(f"[AUTO-CUTOFF] Close request already sent for order {order.order_id}, skipping.")
                        continue
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Preparing to close order {order.order_id} for user {user_id}")
                    close_id = await generate_unique_10_digit_id(db, UserOrder, 'close_id')
                    firebase_close_data = {
                        "action": "close_order",
                        "close_id": close_id,
                        "order_id": order.order_id,
                        "user_id": user_id,
                        "order_company_name": order.order_company_name,
                        "order_type": order.order_type,
                        "order_status": order.order_status,
                        "status": "CLOSED",
                        "order_quantity": str(order.order_quantity),
                        "contract_value": str(order.contract_value),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Firebase payload for order {order.order_id}: {firebase_close_data}")
                    await send_order_to_firebase(firebase_close_data, "live")
                    autocutoff_logger.warning(f"[AUTO-CUTOFF] SUCCESSFULLY SENT TO FIREBASE: User {user_id}, Order {order.order_id}, Close ID {close_id}")
                    # --- NEW: Set Redis flag after sending ---
                    await redis_client.set(autocutoff_flag_key, "1", ex=48*60*60)  # 48 hours expiry
                    update_fields = {
                        "close_id": close_id,
                        "close_message": "Auto-cutoff"
                    }
                    await crud_order.update_order_with_tracking(
                        db, order, update_fields, user_id, user_type, "AUTO_CUTOFF_REQUESTED"
                    )
                    await db.commit()
                    autocutoff_logger.warning(f"[AUTO-CUTOFF] ORDER STATUS UPDATED: User {user_id}, Order {order.order_id}, Status: AUTO_CUTOFF_REQUESTED, Close ID: {close_id}")
                except Exception as e:
                    autocutoff_logger.error(f"[AUTO-CUTOFF] ERROR processing order {order.order_id} for user {user_id}: {e}", exc_info=True)
                    continue
            autocutoff_logger.warning(f"[AUTO-CUTOFF] Completed Firebase requests for user {user_id}, publishing updates")
            await publish_order_update(redis_client, user_id)
            user_type_str = 'live'
            user_data_to_cache = {
                "id": user_for_cutoff.id,
                "email": getattr(user_for_cutoff, 'email', None),
                "group_name": user_for_cutoff.group_name,
                "leverage": user_for_cutoff.leverage,
                "user_type": user_type_str,
                "account_number": getattr(user_for_cutoff, 'account_number', None),
                "wallet_balance": user_for_cutoff.wallet_balance,
                "margin": user_for_cutoff.margin,
                "first_name": getattr(user_for_cutoff, 'first_name', None),
                "last_name": getattr(user_for_cutoff, 'last_name', None),
                "country": getattr(user_for_cutoff, 'country', None),
                "phone_number": getattr(user_for_cutoff, 'phone_number', None)
            }
            autocutoff_logger.debug(f"[AUTO-CUTOFF] Setting user data cache for user {user_id}: {user_data_to_cache}")
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            from app.api.v1.endpoints.orders import update_user_static_orders
            await update_user_static_orders(user_id, db, redis_client, user_type_str)
            await publish_user_data_update(redis_client, user_id)
            await publish_market_data_trigger(redis_client)
            autocutoff_logger.warning(f"[AUTO-CUTOFF] COMPLETED: User {user_id} auto-cutoff process finished, waiting for Firebase confirmation")
        else:
            autocutoff_logger.warning(f"[AUTO-CUTOFF] Processing non-Barclays user {user_id} - closing orders locally")
            from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
            from app.services.portfolio_calculator import _convert_to_usd
            from app.services.order_processing import calculate_total_symbol_margin_contribution
            from app.database.models import ExternalSymbolInfo
            from sqlalchemy import select
            from app.schemas.wallet import WalletCreate
            from app.crud.wallet import generate_unique_10_digit_id
            from app.database.models import Wallet
            from decimal import ROUND_HALF_UP
            import datetime
            total_net_profit = Decimal('0.0')
            for order in open_orders:
                try:
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Preparing to close order {order.order_id} for user {user_id}")
                    symbol = order.order_company_name
                    last_price = await get_last_known_price(redis_client, symbol)
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Last price for symbol {symbol}: {last_price}")
                    if not last_price:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] No last price found for symbol {symbol}, skipping order {order.order_id}")
                        continue
                    close_price_str = last_price.get('o') if order.order_type == 'BUY' else last_price.get('b')
                    close_price = Decimal(str(close_price_str))
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Calculated close_price for order {order.order_id}: {close_price}")
                    if not close_price or close_price <= 0:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] Invalid close price {close_price} for order {order.order_id}, skipping")
                        continue
                    close_id = await generate_unique_10_digit_id(db, order_model, 'close_id')
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Generated close_id {close_id} for order {order.order_id}")
                    quantity = Decimal(str(order.order_quantity))
                    entry_price = Decimal(str(order.order_price))
                    order_type_db = order.order_type.upper()
                    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
                    symbol_info_result = await db.execute(symbol_info_stmt)
                    ext_symbol_info = symbol_info_result.scalars().first()
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] External symbol info for {symbol}: {ext_symbol_info}")
                    if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] Symbol info not found for {symbol}, skipping order {order.order_id}")
                        continue
                    contract_size = Decimal(str(ext_symbol_info.contract_size))
                    profit_currency = ext_symbol_info.profit.upper()
                    group_settings = await get_group_settings_cache(redis_client, user_for_cutoff.group_name)
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] Group settings for {symbol}: {group_settings}")
                    if not group_settings:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] Group settings not found for {symbol}, skipping order {order.order_id}")
                        continue
                    commission_type = int(group_settings.get('commision_type', -1))
                    commission_value_type = int(group_settings.get('commision_value_type', -1))
                    commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                    existing_entry_commission = Decimal(str(order.commission or "0.0"))
                    exit_commission = Decimal("0.0")
                    if commission_type in [0, 2]:
                        if commission_value_type == 0:
                            exit_commission = quantity * commission_rate
                        elif commission_value_type == 1:
                            calculated_exit_contract_value = quantity * contract_size * close_price
                            if calculated_exit_contract_value > Decimal("0.0"):
                                exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                    total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if order_type_db == "BUY":
                        profit = (close_price - entry_price) * quantity * contract_size
                    elif order_type_db == "SELL":
                        profit = (entry_price - close_price) * quantity * contract_size
                    else:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] Invalid order type {order_type_db} for order {order.order_id}, skipping")
                        continue
                    profit_usd = await _convert_to_usd(profit, profit_currency, user_for_cutoff.id, order.order_id, "PnL on Auto-Cutoff", db=db, redis_client=redis_client)
                    autocutoff_logger.debug(f"[AUTO-CUTOFF] profit_usd for order {order.order_id}: {profit_usd}")
                    if profit_currency != "USD" and profit_usd == profit:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] Currency conversion failed for order {order.order_id}, skipping")
                        continue
                    net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    swap_amount = order.swap or Decimal("0.0")
                    order.close_price = close_price
                    order.order_status = 'CLOSED'
                    order.close_message = "Auto-cutoff"
                    order.net_profit = net_profit
                    order.commission = total_commission_for_trade
                    order.close_id = close_id
                    order.swap = swap_amount
                    total_net_profit += (net_profit - swap_amount)
                    autocutoff_logger.warning(f"[AUTO-CUTOFF] ORDER CLOSED: User {user_id}, Order {order.order_id}, Net Profit {net_profit}, Commission {total_commission_for_trade}, Swap {swap_amount}")
                    transaction_time = datetime.datetime.now(datetime.timezone.utc)
                    wallet_common_data = {"symbol": symbol, "order_quantity": quantity, "is_approved": 1, "order_type": order.order_type, "transaction_time": transaction_time, "order_id": order.order_id}
                    if hasattr(user_for_cutoff, 'id'):
                        if hasattr(user_for_cutoff, 'user_type') and user_for_cutoff.user_type == 'demo':
                            wallet_common_data["demo_user_id"] = user_for_cutoff.id
                        else:
                            wallet_common_data["user_id"] = user_for_cutoff.id
                    if net_profit != Decimal("0.0"):
                        transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=net_profit, description=f"P/L for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
                    if total_commission_for_trade > Decimal("0.0"):
                        transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
                    if swap_amount != Decimal("0.0"):
                        transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))
                    await db.commit()
                except Exception as e:
                    autocutoff_logger.error(f"[AUTO-CUTOFF] ERROR closing order {order.order_id} for user {user_id}: {e}", exc_info=True)
                    continue
            try:
                autocutoff_logger.debug(f"[AUTO-CUTOFF] Calculating new total margin and updating user balance for user {user_id}")
                remaining_open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                new_total_margin = Decimal('0.0')
                for remaining_order in remaining_open_orders:
                    symbol = remaining_order.order_company_name
                    symbol_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
                    margin_data = await calculate_total_symbol_margin_contribution(
                        db, redis_client, user_id, symbol, symbol_orders, order_model, user_type
                    )
                    new_total_margin += margin_data["total_margin"]
                original_wallet_balance = Decimal(str(user_for_cutoff.wallet_balance))
                user_for_cutoff.wallet_balance = (original_wallet_balance + total_net_profit).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
                user_for_cutoff.margin = new_total_margin.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                await db.commit()
                autocutoff_logger.warning(f"[AUTO-CUTOFF] USER BALANCE UPDATED: User {user_id}, Original Balance {original_wallet_balance}, Total Net Profit {total_net_profit}, New Balance {user_for_cutoff.wallet_balance}, New Margin {user_for_cutoff.margin}")
                user_type_str = 'demo' if hasattr(user_for_cutoff, 'user_type') and user_for_cutoff.user_type == 'demo' else 'live'
                user_data_to_cache = {
                    "id": user_for_cutoff.id,
                    "email": getattr(user_for_cutoff, 'email', None),
                    "group_name": user_for_cutoff.group_name,
                    "leverage": user_for_cutoff.leverage,
                    "user_type": user_type_str,
                    "account_number": getattr(user_for_cutoff, 'account_number', None),
                    "wallet_balance": user_for_cutoff.wallet_balance,
                    "margin": user_for_cutoff.margin,
                    "first_name": getattr(user_for_cutoff, 'first_name', None),
                    "last_name": getattr(user_for_cutoff, 'last_name', None),
                    "country": getattr(user_for_cutoff, 'country', None),
                    "phone_number": getattr(user_for_cutoff, 'phone_number', None)
                }
                autocutoff_logger.debug(f"[AUTO-CUTOFF] Setting user data cache for user {user_id}: {user_data_to_cache}")
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                from app.api.v1.endpoints.orders import update_user_static_orders
                await update_user_static_orders(user_id, db, redis_client, user_type_str)
                try:
                    from app.services.order_processing import calculate_total_user_margin
                    total_user_margin = await calculate_total_user_margin(db, redis_client, user_id, user_type_str)
                    await set_user_balance_margin_cache(redis_client, user_id, user_data_to_cache["wallet_balance"], total_user_margin)
                    autocutoff_logger.info(f"[AUTO-CUTOFF] User {user_id}: Updated balance/margin cache - balance={user_data_to_cache['wallet_balance']}, margin={total_user_margin}")
                except Exception as e:
                    autocutoff_logger.error(f"[AUTO-CUTOFF] User {user_id}: Error updating balance/margin cache: {e}", exc_info=True)
                await publish_order_update(redis_client, user_id)
                await publish_user_data_update(redis_client, user_id)
                await publish_market_data_trigger(redis_client)
                autocutoff_logger.warning(f"[AUTO-CUTOFF] COMPLETED: Non-Barclays user {user_id} auto-cutoff process finished successfully")
            except Exception as e:
                autocutoff_logger.error(f"[AUTO-CUTOFF] ERROR updating user balance for user {user_id}: {e}", exc_info=True)
                await db.rollback()
    except Exception as e:
        autocutoff_logger.error(f"[AUTO-CUTOFF] FATAL ERROR in handle_margin_cutoff for user {user_id}: {e}", exc_info=True)

# --- Service Provider JWT Rotation Job ---
async def rotate_service_account_jwt():
    """
    Generates a JWT for the Barclays service provider and pushes it to Firebase.
    This job is scheduled to run periodically.
    """
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
        
        logger.info(f"Service account JWT for '{service_name}' was generated and pushed to Firebase.")

    except Exception as e:
        logger.error(f"Error in rotate_service_account_jwt job: {e}")

# Add this line after the app initialization
background_tasks = set()

@app.on_event("startup")
async def startup_event():
    global scheduler
    global background_tasks
    global global_redis_client_instance
    logger.info("Application startup initiated")
    logger.info("Initializing core services...")
    import redis.asyncio as redis

    # r = redis.Redis(host="127.0.0.1", port=6379)
    # await r.flushall()
    # print("Redis flushed")
    # Print Redis connection info for debugging
    redis_url = os.getenv("REDIS_URL")
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")
    redis_password = os.getenv("REDIS_PASSWORD")
    print(f"[DEBUG] Redis connection info:")
    print(f"  REDIS_URL: {redis_url}")
    print(f"  REDIS_HOST: {redis_host}")
    print(f"  REDIS_PORT: {redis_port}")
    print(f"  REDIS_PASSWORD: {redis_password}")

    # Initialize Firebase

    try:
        cred_path = os.path.join(os.path.dirname(__file__), '..', settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
        if not os.path.exists(cred_path):
            raise FileNotFoundError("Firebase credentials file not found")
            
        cred = credentials.Certificate(cred_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'databaseURL': settings.FIREBASE_DATABASE_URL
            })
            logger.info("Firebase initialized")
        else:
            logger.info("Firebase already initialized")
    except Exception as e:
        logger.error("Firebase initialization error")
    
    # Initialize Redis connection pool

    from app.core.cache import cache_all_groups_and_symbols
    

    redis_available = False
    try:
        redis_client = await get_redis_client()
        if redis_client:
            ping_result = await redis_client.ping()
            if ping_result:
                redis_available = True
                global_redis_client_instance = redis_client
                logger.info("Redis initialized")
                # --- Print all Redis keys at startup ---
                try:
                    async with AsyncSessionLocal() as db:
                        await cache_all_groups_and_symbols(global_redis_client_instance, db)
                    logger.info("All group settings and group-symbol settings cached in Redis at startup.")
                except Exception as e:
                    logger.error(f"Error caching all group settings at startup: {e}", exc_info=True)

                try:
                    keys = await redis_client.keys("*")
                    print(f"[STARTUP] Redis contains {len(keys)} keys:")
                    for k in keys:
                        print(f"  - {k}")
                except Exception as e:
                    print(f"[STARTUP] Error fetching Redis keys: {e}")
    except Exception:
        logger.warning("Redis initialization failed")


    # Initialize APScheduler
    try:
        scheduler = AsyncIOScheduler()
        
        scheduler.add_job(
            daily_swap_charge_job,
            CronTrigger(hour=3, minute=53, timezone='utc'),
            # IntervalTrigger(minutes=1),
            logger.info("[SWAP] daily_swap_charge_job triggered"),
            id='daily_swap_charge_job',
            replace_existing=True
        )
        
        # REMOVE the update_all_users_dynamic_portfolio APScheduler job
        # scheduler.add_job(
        #     update_all_users_dynamic_portfolio,
        #     IntervalTrigger(minutes=1),  # Changed from 1 minute to 5 minutes
        #     id='update_all_users_dynamic_portfolio',
        #     replace_existing=True
        # )
        logger.info("[AUTO-CUTOFF] update_all_users_dynamic_portfolio will run as a background asyncio task, not via APScheduler.")
        
        scheduler.add_job(
            rotate_service_account_jwt,
            IntervalTrigger(minutes=30),
            id='rotate_service_account_jwt',
            replace_existing=True
        )
        
        # Add connection pool monitoring
        from app.database.session import log_connection_pool_status
        scheduler.add_job(
            log_connection_pool_status,
            IntervalTrigger(minutes=2),
            id='connection_pool_monitoring',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Scheduler initialized")
    except Exception:
        logger.error("Scheduler initialization error")
    
    # Start background tasks
    try:
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        
        if redis_available and global_redis_client_instance:
            redis_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
            background_tasks.add(redis_task)
            redis_task.add_done_callback(background_tasks.discard)
            
            # Start the centralized adjusted price worker
            adjusted_price_task = asyncio.create_task(adjusted_price_worker(global_redis_client_instance))
            background_tasks.add(adjusted_price_task)
            adjusted_price_task.add_done_callback(background_tasks.discard)
            
            pending_orders_task = asyncio.create_task(run_pending_order_checker())
            background_tasks.add(pending_orders_task)
            pending_orders_task.add_done_callback(background_tasks.discard)
            
            # Start the optimized SL/TP checker task
            sltp_task = asyncio.create_task(run_sltp_checker_on_market_update())
            background_tasks.add(sltp_task)
            sltp_task.add_done_callback(background_tasks.discard)
            
            # Start the cache updater task for users with orders
            cache_updater_task = asyncio.create_task(run_users_with_orders_cache_updater())
            background_tasks.add(cache_updater_task)
            cache_updater_task.add_done_callback(background_tasks.discard)
            
            redis_cleanup_task = asyncio.create_task(cleanup_orphaned_redis_orders())
            background_tasks.add(redis_cleanup_task)
            redis_cleanup_task.add_done_callback(background_tasks.discard)
            
            # Start the pending order trigger worker (processes triggered orders from Redis stream)
            from app.services.pending_orders import pending_order_trigger_worker, cleanup_pending_order_stream
            # Clean up old stream entries on startup
            await cleanup_pending_order_stream(global_redis_client_instance)
            pending_trigger_worker_task = asyncio.create_task(
                pending_order_trigger_worker(
                    redis=global_redis_client_instance,
                    db_factory=AsyncSessionLocal,
                    concurrency=10
                )
            )
            background_tasks.add(pending_trigger_worker_task)
            pending_trigger_worker_task.add_done_callback(background_tasks.discard)
            
            # Start the stale cache cleanup task
            stale_cache_task = asyncio.create_task(cleanup_stale_cache_entries(global_redis_client_instance))
            background_tasks.add(stale_cache_task)
            stale_cache_task.add_done_callback(background_tasks.discard)
            
            # Clean up pending order stream to ensure new trigger price format
            try:
                from app.services.pending_orders import cleanup_pending_order_stream_with_trigger_prices
                await cleanup_pending_order_stream_with_trigger_prices(global_redis_client_instance)
                logger.info("Cleaned up pending order stream for new trigger price format")
            except Exception as e:
                logger.error(f"Error cleaning up pending order stream: {e}", exc_info=True)
            
            # Start periodic cache cleanup task
            cache_cleanup_task = asyncio.create_task(run_cache_cleanup())
            background_tasks.add(cache_cleanup_task)
            cache_cleanup_task.add_done_callback(background_tasks.discard)
            
            # Start periodic cache validation task
            cache_validation_task = asyncio.create_task(run_cache_validation())
            background_tasks.add(cache_validation_task)
            cache_validation_task.add_done_callback(background_tasks.discard)
            
            # Register Barclays pending order margin checker
            barclays_pending_task = asyncio.create_task(barclays_pending_order_margin_checker())
            background_tasks.add(barclays_pending_task)
            barclays_pending_task.add_done_callback(background_tasks.discard)
            
            # Start auto-cutoff order monitoring task
            auto_cutoff_monitor_task = asyncio.create_task(monitor_auto_cutoff_orders())
            background_tasks.add(auto_cutoff_monitor_task)
            auto_cutoff_monitor_task.add_done_callback(background_tasks.discard)
            
            # Start the new autocutoff/dynamic portfolio update background loop
            update_portfolio_task = asyncio.create_task(run_update_all_users_dynamic_portfolio_loop())
            background_tasks.add(update_portfolio_task)
            update_portfolio_task.add_done_callback(background_tasks.discard)
            
        logger.info("Background tasks initialized")
            
    except Exception:
        logger.error("Background tasks initialization error")
    
    # Create initial service account token
    try:
        await rotate_service_account_jwt()
    except Exception:
        logger.error("Initial service account token creation failed")
    
    logger.info("Application startup completed successfully")

@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, global_redis_client_instance
    logger.info("Application shutdown initiated")

    if scheduler and scheduler.running:
        try:
            scheduler.shutdown(wait=True)
            logger.info("Scheduler shutdown completed")
        except Exception:
            logger.error("Scheduler shutdown error")

    for task in list(background_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.error("Background task cancellation error")

    if global_redis_client_instance:
        await close_redis_connection(global_redis_client_instance)
        global_redis_client_instance = None
        logger.info("Redis connection closed")

    from app.firebase_stream import cleanup_firebase
    cleanup_firebase()

    logger.info("Application shutdown completed")

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Trading App Backend!"}

async def run_stoploss_takeprofit_checker():
    """Background task to continuously check for stop loss and take profit conditions"""
    logger = orders_logger
    
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
    Simplified pending order checker that just ensures the cache is up to date.
    The actual triggering is now handled in the adjusted_price_worker.py
    """
    logger = orders_logger

    await asyncio.sleep(5)
    logger.info("Starting the simplified pending order checker background task.")
    
    while True:
        try:
            if global_redis_client_instance:
                # Just update the cache periodically to ensure it's fresh
                from app.services.pending_orders import update_users_with_orders_cache_periodic
                await update_users_with_orders_cache_periodic(global_redis_client_instance)
                logger.debug("Updated users with orders cache")
            else:
                await asyncio.sleep(60)
                continue
                    
        except Exception as e:
            logger.error(f"Error in pending order checker loop: {str(e)}", exc_info=True)
            await asyncio.sleep(60)
            continue
        
        # Update cache every 10 minutes (increased from 5 minutes)
        await asyncio.sleep(600)

# --- New SL/TP Checker Task (triggered by market data updates) ---
async def run_sltp_checker_on_market_update():
    """
    Optimized SL/TP checker that processes only symbols that have market data updates.
    This ensures SL/TP checks happen on every price tick with minimal database load.
    """
    logger = orders_logger

    # Give the application a moment to initialize everything else
    await asyncio.sleep(5) 
    logger.debug("Starting the optimized SL/TP checker task (triggered by market updates).")
    
    # Subscribe to market data updates
    pubsub = global_redis_client_instance.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                continue
                
            try:
                message_data = json.loads(message['data'], object_hook=decode_decimal)
                if message_data.get("type") == "market_data_update":
                    # Extract symbols from market data
                    symbols = [key for key in message_data.keys() 
                             if key not in ['type', '_timestamp'] and 
                             isinstance(message_data[key], dict)]
                    
                    if symbols:
                        logger.debug(f"Market data update received for symbols: {symbols}, triggering optimized SL/TP check")
                        
                        # Process SL/TP for only the symbols that updated
                        from app.services.pending_orders import process_sltp_batch
                        await process_sltp_batch(symbols, global_redis_client_instance)
                        
            except Exception as e:
                logger.error(f"Error processing market data for SL/TP check: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error in optimized SL/TP checker task: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.close()

# --- New Periodic Cache Update Task ---
async def run_users_with_orders_cache_updater():
    """
    Periodic task to update the cache of users with open orders per symbol.
    This keeps the cache fresh for efficient SL/TP processing.
    """
    logger = orders_logger
    
    # Give the application a moment to initialize everything else
    await asyncio.sleep(10)  # Wait a bit longer to let other tasks start
    logger.info("Starting the users with orders cache updater task.")
    
    while True:
        try:
            if global_redis_client_instance:
                from app.services.pending_orders import update_users_with_orders_cache_periodic
                await update_users_with_orders_cache_periodic(global_redis_client_instance)
                logger.debug("Updated users with orders cache for all symbols")
            else:
                await asyncio.sleep(60)
                continue
                
        except Exception as e:
            logger.error(f"Error in users with orders cache updater: {e}", exc_info=True)
        
        # Update cache every 5 minutes
        await asyncio.sleep(300)

# --- Redis Cleanup Function ---
async def cleanup_orphaned_redis_orders():
    """
    Periodically clean up orphaned orders in Redis that no longer exist in the database.
    """
    logger = redis_logger
    
    while True:
        try:
            if not global_redis_client_instance:
                await asyncio.sleep(60)
                continue
                
            async with AsyncSessionLocal() as db:
                from app.services.pending_orders import get_all_pending_orders_from_redis
                pending_orders = await get_all_pending_orders_from_redis(global_redis_client_instance)
                
                if not pending_orders:
                    await asyncio.sleep(600)  # Increased from 300 to 600 seconds (10 minutes)
                    continue
                
                from app.crud.crud_order import get_order_model
                cleaned_count = 0
                
                for order_data in pending_orders:
                    try:
                        order_id = order_data.get('order_id')
                        user_id = order_data.get('order_user_id')
                        user_type = order_data.get('user_type', 'live')
                        symbol = order_data.get('order_company_name')
                        order_type = order_data.get('order_type')
                        
                        if not all([order_id, user_id, symbol, order_type]):
                            continue
                        
                        order_model = get_order_model(user_type)
                        
                        from app.crud.crud_order import get_order_by_id
                        db_order = await get_order_by_id(db, order_id, order_model)
                        
                        if not db_order or db_order.order_status != 'PENDING':
                            from app.services.pending_orders import remove_pending_order
                            await remove_pending_order(
                                global_redis_client_instance,
                                str(order_id),
                                symbol,
                                order_type,
                                str(user_id)
                            )
                            cleaned_count += 1
                            
                    except Exception:
                        continue
                    
        except Exception as e:
            logger.error("Error in cleanup process")
        
        await asyncio.sleep(300)

# --- Periodic Cache Cleanup Task ---
async def run_cache_cleanup():
    """
    Periodic task to clean up stale cache entries and ensure cache consistency.
    This prevents orders from disappearing due to cache issues.
    """
    logger = redis_logger
    
    # Give the application a moment to initialize everything else
    await asyncio.sleep(15)  # Wait a bit longer to let other tasks start
    logger.info("Starting the cache cleanup task.")
    
    while True:
        try:
            if global_redis_client_instance:
                await cleanup_stale_cache_entries(global_redis_client_instance)
                logger.debug("Completed cache cleanup cycle")
            else:
                await asyncio.sleep(60)
                continue
                
        except Exception as e:
            logger.error(f"Error in cache cleanup task: {e}", exc_info=True)
        
        # Run cleanup every 15 minutes (increased from 10 minutes)
        await asyncio.sleep(900)

async def cleanup_stale_cache_entries(redis_client: Redis):
    """
    Periodically clean up stale cache entries and ensure cache consistency.
    This prevents orders from disappearing due to cache issues.
    """
    try:
        logger.info("Starting stale cache cleanup task")
        
        # Get all static orders cache keys
        from app.core.cache import REDIS_USER_STATIC_ORDERS_KEY_PREFIX
        pattern = f"{REDIS_USER_STATIC_ORDERS_KEY_PREFIX}*"
        cache_keys = await redis_client.keys(pattern)
        
        logger.info(f"Found {len(cache_keys)} static orders cache entries to check")
        
        for key in cache_keys:
            try:
                # Check if cache entry is valid
                cache_data = await redis_client.get(key)
                if cache_data:
                    data = json.loads(cache_data)
                    
                    # If cache has error or is empty but shouldn't be, mark for refresh
                    if data.get("error") or (not data.get("open_orders") and not data.get("pending_orders")):
                        user_id = key.replace(REDIS_USER_STATIC_ORDERS_KEY_PREFIX, "")
                        logger.warning(f"Found stale cache entry for user {user_id}, will be refreshed on next access")
                        
            except Exception as e:
                logger.error(f"Error checking cache key {key}: {e}")
                continue
        
        logger.info("Completed stale cache cleanup task")
        
    except Exception as e:
        logger.error(f"Error in cleanup_stale_cache_entries: {e}", exc_info=True)

async def run_cache_validation():
    """
    Periodic task to validate and fix cache inconsistencies.
    This helps prevent the websocket account summary data from vanishing.
    """
    try:
        logger.info("Starting cache validation task")
        
        if not global_redis_client_instance:
            logger.error("Cannot run cache validation - Redis client not available")
            return
        
        async with AsyncSessionLocal() as db:
            # Get all active users
            live_users, demo_users = await crud_user.get_all_active_users_both(db)
            
            all_users = []
            for user in live_users:
                all_users.append({"id": user.id, "user_type": "live"})
            for user in demo_users:
                all_users.append({"id": user.id, "user_type": "demo"})
            
            logger.info(f"Validating cache for {len(all_users)} users")
            
            for user_info in all_users:
                try:
                    user_id = user_info["id"]
                    user_type = user_info["user_type"]
                    
                    # Check balance/margin cache
                    from app.core.cache import get_user_balance_margin_cache, get_user_static_orders_cache
                    balance_margin_cache = await get_user_balance_margin_cache(global_redis_client_instance, user_id)
                    
                    if not balance_margin_cache:
                        logger.warning(f"User {user_id}: Missing balance/margin cache, refreshing...")
                        from app.core.cache import refresh_balance_margin_cache_with_fallback
                        await refresh_balance_margin_cache_with_fallback(global_redis_client_instance, user_id, user_type, db)
                        continue
                    
                    balance = balance_margin_cache.get("wallet_balance", "0.0")
                    margin = balance_margin_cache.get("margin", "0.0")
                    
                    # Check for suspicious values
                    if balance == "0.0" and margin == "0.0":
                        # Check if user actually has orders
                        order_model = get_order_model(user_type)
                        open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                        
                        if open_orders:
                            logger.warning(f"User {user_id}: Cache shows 0 balance/margin but has {len(open_orders)} open orders, refreshing cache...")
                            from app.core.cache import refresh_balance_margin_cache_with_fallback
                            await refresh_balance_margin_cache_with_fallback(global_redis_client_instance, user_id, user_type, db)
                    
                    # Check static orders cache
                    static_orders_cache = await get_user_static_orders_cache(global_redis_client_instance, user_id)
                    if not static_orders_cache:
                        logger.warning(f"User {user_id}: Missing static orders cache, refreshing...")
                        from app.api.v1.endpoints.orders import update_user_static_orders
                        await update_user_static_orders(user_id, db, global_redis_client_instance, user_type)
                    
                except Exception as e:
                    logger.error(f"Error validating cache for user {user_info.get('id')}: {e}")
                    continue
            
            logger.info("Cache validation completed")
            
    except Exception as e:
        logger.error(f"Error in cache validation task: {e}", exc_info=True)
    
    # Schedule next validation in 5 minutes
    await asyncio.sleep(300)
    asyncio.create_task(run_cache_validation())

# --- Auto-cutoff Order Monitoring Task ---
async def monitor_auto_cutoff_orders():
    """
    Monitors auto-cutoff orders to identify any that might be stuck in AUTO_CUTOFF_REQUESTED status.
    This helps identify orders that were sent to Firebase but never received confirmation.
    """
    logger.info("Starting auto-cutoff order monitoring task.")
    
    try:
        while True:
            try:
                if not global_redis_client_instance:
                    await asyncio.sleep(60)
                    continue
                    
                async with AsyncSessionLocal() as db:
                    # Find orders stuck in AUTO_CUTOFF_REQUESTED status for more than 5 minutes
                    from sqlalchemy import select, and_
                    from app.database.models import UserOrder
                    from datetime import datetime, timedelta
                    
                    # Get orders with AUTO_CUTOFF_REQUESTED status
                    cutoff_time = datetime.utcnow() - timedelta(minutes=5)
                    
                    stuck_orders_query = select(UserOrder).where(
                        and_(
                            UserOrder.order_status == "AUTO_CUTOFF_REQUESTED",
                            UserOrder.updated_at < cutoff_time
                        )
                    )
                    
                    result = await db.execute(stuck_orders_query)
                    stuck_orders = result.scalars().all()
                    
                    if stuck_orders:
                        autocutoff_logger.error(f"[AUTO-CUTOFF] Found {len(stuck_orders)} stuck auto-cutoff orders:")
                        for order in stuck_orders:
                            autocutoff_logger.error(f"[AUTO-CUTOFF] STUCK ORDER: User {order.order_user_id}, Order {order.order_id}, Close ID {order.close_id}, Updated {order.updated_at}, Close Message: {order.close_message}")
                            
                            # Check if we should retry sending to Firebase
                            if order.updated_at < (datetime.utcnow() - timedelta(minutes=10)):
                                autocutoff_logger.warning(f"[AUTO-CUTOFF] RETRYING FIREBASE SEND: User {order.order_user_id}, Order {order.order_id}, Close ID {order.close_id}")
                                
                                try:
                                    firebase_close_data = {
                                        "action": "close_order",
                                        "close_id": order.close_id,
                                        "order_id": order.order_id,
                                        "user_id": order.order_user_id,
                                        "symbol": order.order_company_name,
                                        "order_type": order.order_type,
                                        "order_status": order.order_status,
                                        "status": "close",
                                        "order_quantity": str(order.order_quantity),
                                        "contract_value": str(order.contract_value) if order.contract_value else None,
                                        "timestamp": datetime.now(datetime.timezone.utc).isoformat(),
                                    }
                                    
                                    await send_order_to_firebase(firebase_close_data, "live")
                                    autocutoff_logger.warning(f"[AUTO-CUTOFF] RETRY SENT TO FIREBASE: User {order.order_user_id}, Order {order.order_id}, Close ID {order.close_id}")
                                    
                                except Exception as retry_error:
                                    autocutoff_logger.error(f"[AUTO-CUTOFF] RETRY FAILED: User {order.order_user_id}, Order {order.order_id}, Error: {retry_error}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"Error in auto-cutoff order monitoring: {e}", exc_info=True)
                
            # Check every 2 minutes
            await asyncio.sleep(120)
            
    except Exception as fatal:
        logger.critical(f"FATAL ERROR in auto-cutoff order monitoring: {fatal}", exc_info=True)

# --- Barclays Pending Order Margin Checker ---
async def barclays_pending_order_margin_checker():
    """
    Periodically checks all Barclays live users' free margin against their total pending order margin.
    If free margin is insufficient, cancels pending orders one by one (lowest margin first) until sufficient.
    """
    logger.info("Starting Barclays pending order margin checker task.")
    from app.core.cache import get_user_data_cache, get_user_dynamic_portfolio_cache
    from app.crud.crud_order import get_order_model, get_orders_by_user_id_and_statuses, get_order_by_id
    from app.services.order_processing import generate_unique_10_digit_id
    from app.core.firebase import send_order_to_firebase
    from app.crud.user import get_user_by_id
    from app.crud.group import get_group_by_name
    from app.core.cache import set_user_data_cache, set_user_balance_margin_cache
    from app.api.v1.endpoints.orders import update_user_static_orders
    import datetime
    
    # Track recently cancelled orders to prevent duplicates
    recently_cancelled_orders = set()
    
    try:
        while True:
            logger.debug("BarclaysMarginChecker heartbeat: loop is alive")
            try:
                if not global_redis_client_instance:
                    logger.debug("Redis client not available, sleeping.")
                    await asyncio.sleep(1)
                    continue
                
                async with AsyncSessionLocal() as db:
                    live_users = await crud_user.get_all_users_with_pending_orders(db)
                    logger.debug(f"Fetched {len(live_users)} live users for Barclays margin check.")
                    for user in live_users:
                        user_id = user.id
                        user_type = 'live'
                        logger.debug(f"Checking user {user_id} (live)...")
                        user_data = await get_user_data_cache(global_redis_client_instance, user_id, db, user_type)
                        if not user_data:
                            logger.debug(f"No user data for user {user_id}, skipping.")
                            continue
                        group_name = user_data.get('group_name')
                        if not group_name:
                            logger.debug(f"User {user_id} has no group_name, skipping.")
                            continue
                        from app.core.cache import get_group_settings_cache
                        group_settings = await get_group_settings_cache(global_redis_client_instance, group_name)
                        sending_orders = group_settings.get('sending_orders') if group_settings else None
                        logger.debug(f"User {user_id} group '{group_name}' sending_orders: {sending_orders}")
                        if not sending_orders or sending_orders.lower() != 'barclays':
                            logger.debug(f"User {user_id} is not a Barclays user, skipping.")
                            continue
                        dynamic_portfolio = await get_user_dynamic_portfolio_cache(global_redis_client_instance, user_id)
                        if not dynamic_portfolio:
                            logger.debug(f"No dynamic portfolio for user {user_id}, skipping.")
                            continue
                        try:
                            free_margin = float(dynamic_portfolio.get('free_margin', 0.0))
                            margin = float(dynamic_portfolio.get('margin', 0.0))
                        except Exception:
                            free_margin = 0.0
                            margin = 0.0
                        logger.debug(f"User {user_id} free_margin: {free_margin}, margin: {margin}")
                        order_model = get_order_model(user_type)
                        pending_statuses = ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]
                        pending_orders = await get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
                        logger.debug(f"User {user_id} has {len(pending_orders)} pending orders before filtering.")
                        active_pending_orders = []
                        for order in pending_orders:
                            order_key = f"{user_id}:{order.order_id}"
                            if order_key not in recently_cancelled_orders:
                                active_pending_orders.append(order)
                        logger.debug(f"User {user_id} has {len(active_pending_orders)} active pending orders after filtering.")
                        if not active_pending_orders:
                            logger.debug(f"User {user_id} has no active pending orders, skipping.")
                            continue
                        pending_orders_sorted = sorted(active_pending_orders, key=lambda o: float(getattr(o, 'margin', 0.0) or 0.0))
                        total_pending_margin = sum([float(getattr(o, 'margin', 0.0) or 0.0) for o in pending_orders_sorted])
                        logger.info(f"[BarclaysMarginChecker] User {user_id}: free_margin={free_margin}, total_pending_margin={total_pending_margin}, num_pending_orders={len(pending_orders_sorted)}")
                        logger.debug(f"User {user_id} total_pending_margin: {total_pending_margin}")
                        idx = 0
                        cancelled_any = False
                        while free_margin < total_pending_margin and idx < len(pending_orders_sorted):
                            order = pending_orders_sorted[idx]
                            order_margin = float(getattr(order, 'margin', 0.0) or 0.0)
                            order_key = f"{user_id}:{order.order_id}"
                            logger.info(f"[BarclaysMarginChecker] Considering cancellation: user {user_id}, order {order.order_id}, order_margin={order_margin}, free_margin={free_margin}, total_pending_margin={total_pending_margin}")
                            current_order = await get_order_by_id(db, order.order_id, order_model)
                            if not current_order or current_order.order_status not in pending_statuses:
                                logger.debug(f"Order {order.order_id} is not in a pending status, skipping.")
                                idx += 1
                                continue
                            recently_cancelled_orders.add(order_key)
                            logger.warning(f"[BarclaysMarginChecker] Cancelling order {order.order_id} for user {user_id} (order_margin={order_margin}) due to insufficient margin.")
                            try:
                                cancel_id = await generate_unique_10_digit_id(db, order_model, 'cancel_id')
                                cancel_message = "Auto-cancelled due to insufficient margin"
                                update_fields = {
                                    "order_status": "CANCELLED-PROCESSING",
                                    "cancel_id": cancel_id,
                                    "cancel_message": cancel_message
                                }
                                from app.crud.crud_order import update_order_with_tracking
                                await update_order_with_tracking(
                                    db,
                                    order,
                                    update_fields=update_fields,
                                    user_id=user_id,
                                    user_type=user_type,
                                    action_type="CANCEL"
                                )
                                await db.commit()
                                firebase_cancel_data = {
                                    "order_id": order.order_id,
                                    "cancel_id": cancel_id,
                                    "user_id": user_id,
                                    "order_type": order.order_type,
                                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                    "status": "CANCELLED",
                                    "order_quantity": str(order.order_quantity),
                                    "order_status": order.order_status,
                                    "contract_value": str(order.contract_value) if order.contract_value else None,
                                    "cancel_message": cancel_message
                                }
                                await send_order_to_firebase(firebase_cancel_data, "live")
                                await update_user_static_orders(user_id, db, global_redis_client_instance, user_type)
                                db_user = await get_user_by_id(db, user_id=user_id, user_type=user_type)
                                if db_user:
                                    from app.services.order_processing import calculate_total_user_margin
                                    total_user_margin = await calculate_total_user_margin(db, global_redis_client_instance, user_id, user_type)
                                    user_data_to_cache = {
                                        "id": db_user.id,
                                        "email": getattr(db_user, 'email', None),
                                        "group_name": db_user.group_name,
                                        "leverage": db_user.leverage,
                                        "user_type": user_type,
                                        "account_number": getattr(db_user, 'account_number', None),
                                        "wallet_balance": db_user.wallet_balance,
                                        "margin": total_user_margin,
                                        "first_name": getattr(db_user, 'first_name', None),
                                        "last_name": getattr(db_user, 'last_name', None),
                                        "country": getattr(db_user, 'country', None),
                                        "phone_number": getattr(db_user, 'phone_number', None),
                                    }
                                    await set_user_data_cache(global_redis_client_instance, user_id, user_data_to_cache, user_type)
                                    await set_user_balance_margin_cache(global_redis_client_instance, user_id, db_user.wallet_balance, total_user_margin)
                                await publish_order_update(global_redis_client_instance, user_id)
                                await publish_user_data_update(global_redis_client_instance, user_id)
                                cancelled_any = True
                                logger.info(f"[BarclaysMarginChecker] Successfully cancelled order {order.order_id} for user {user_id}")
                            except Exception as cancel_error:
                                logger.error(f"[BarclaysMarginChecker] Error cancelling order {order.order_id} for user {user_id}: {cancel_error}", exc_info=True)
                                recently_cancelled_orders.discard(order_key)
                            try:
                                dynamic_portfolio = await get_user_dynamic_portfolio_cache(global_redis_client_instance, user_id)
                                if dynamic_portfolio:
                                    free_margin = float(dynamic_portfolio.get('free_margin', 0.0))
                                else:
                                    free_margin = 0.0
                            except Exception:
                                free_margin = 0.0
                            remaining_orders = pending_orders_sorted[idx+1:]
                            total_pending_margin = sum([
                                float(getattr(o, 'margin', 0.0) or 0.0)
                                for o in remaining_orders
                                if f"{user_id}:{o.order_id}" not in recently_cancelled_orders
                            ])
                            logger.debug(f"After cancellation, user {user_id} free_margin: {free_margin}, total_pending_margin: {total_pending_margin}")
                            idx += 1
                        if not cancelled_any:
                            logger.debug(f"[BarclaysMarginChecker] User {user_id}: No cancellation needed.")
            except Exception as e:
                logger.error(f"Error in barclays_pending_order_margin_checker loop: {e}", exc_info=True)
            if len(recently_cancelled_orders) > 1000:
                recently_cancelled_orders.clear()
            await asyncio.sleep(1)
    except Exception as fatal:
        logger.critical(f"[BarclaysMarginChecker] FATAL: {fatal}", exc_info=True)


async def run_update_all_users_dynamic_portfolio_loop():
    logger.info("[AUTO-CUTOFF] Starting update_all_users_dynamic_portfolio background loop")
    while True:
        try:
            await update_all_users_dynamic_portfolio()
        except Exception as e:
            logger.error(f"[AUTO-CUTOFF] Exception in update_all_users_dynamic_portfolio loop: {e}", exc_info=True)
        await asyncio.sleep(60)  # 1 minute interval