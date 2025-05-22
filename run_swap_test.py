# run_swap_test.py

import asyncio
import logging
from app.database.session import AsyncSessionLocal, create_all_tables
# CORRECTED IMPORT: close_redis_connection is in app.core.security
from app.dependencies.redis_client import get_redis_client
from app.core.security import close_redis_connection # Corrected line
from app.services.swap_service import apply_daily_swap_charges_for_all_open_orders
from app.core.config import get_settings
import firebase_admin
from firebase_admin import credentials, db as firebase_db
import os

# Configure logging for the test script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger('app.services.swap_service').setLevel(logging.DEBUG) # Ensure swap service logs are visible

async def run_swap_charges_immediately():
    """
    Sets up necessary dependencies and directly calls the swap charge function.
    """
    settings = get_settings()
    db = None
    redis_client = None
    firebase_app_instance = None

    logger.info("Starting immediate swap charge test.")

    try:
        # 1. Initialize Firebase Admin SDK (needed by get_latest_market_data in swap_service)
        cred_path = settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH
        if not os.path.exists(cred_path):
            logger.critical(f"Firebase service account key file not found at: {cred_path}")
            return
        
        # Only initialize if not already initialized
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_path)
            firebase_app_instance = firebase_admin.initialize_app(cred, {
                'databaseURL': settings.FIREBASE_DATABASE_URL
            })
            logger.info("Firebase Admin SDK initialized for test.")
        else:
            firebase_app_instance = firebase_admin.get_app()
            logger.info("Firebase Admin SDK already initialized, using existing app.")

        # 2. Connect to Redis
        redis_client = await get_redis_client()
        if not redis_client:
            logger.critical("Failed to connect to Redis. Cannot run swap test.")
            return

        # 3. Ensure database tables exist (optional, but good for a fresh test env)
        await create_all_tables()
        logger.info("Database tables ensured/created for test.")

        # 4. Get a database session
        db = AsyncSessionLocal()
        
        # 5. Call the swap charge function
        logger.info("Calling apply_daily_swap_charges_for_all_open_orders directly...")
        await apply_daily_swap_charges_for_all_open_orders(db, redis_client)
        logger.info("Immediate swap charge application finished.")

    except Exception as e:
        logger.error(f"An error occurred during the immediate swap charge test: {e}", exc_info=True)
    finally:
        # Clean up resources
        if db:
            await db.close()
            logger.info("Database session closed.")
        if redis_client:
            await close_redis_connection(redis_client)
            logger.info("Redis client connection closed.")
        # Only delete the Firebase app if this script initialized it
        if firebase_app_instance and firebase_admin._apps:
            try:
                firebase_admin.delete_app(firebase_app_instance)
                logger.info("Firebase app instance deleted.")
            except ValueError: # App might have been deleted already if called multiple times
                pass


if __name__ == "__main__":
    asyncio.run(run_swap_charges_immediately())