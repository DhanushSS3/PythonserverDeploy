# app/core/firebase.py

import logging
import json
from typing import Dict, Any, Optional
import decimal
from datetime import datetime

# Ensure firebase_admin is initialized and imported correctly
try:
    import firebase_admin
    from firebase_admin import db, firestore, credentials
except ImportError as e:
    # This line will be an issue if firebase_admin is not available during static analysis or runtime.
    # Consider handling this more gracefully if it's a potential deployment issue.
    raise ImportError("firebase_admin is not installed or not accessible: " + str(e))

# Import firebase_db from firebase_stream (should be db from firebase_admin)
try:
    # This import implies that firebase_stream.py initializes and exposes firebase_db.
    # Ensure this is the case and firebase_db is the Firebase Realtime Database reference.
    from app.firebase_stream import firebase_db
except ImportError as e:
    raise ImportError("Could not import firebase_db from app.firebase_stream: " + str(e))

# Initialize firebase_admin lazily
_firebase_initialized = False

def _ensure_firebase_initialized():
    """Ensure Firebase is initialized before use."""
    global _firebase_initialized
    if not _firebase_initialized:
        import os
        service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH")
        database_url = os.getenv("FIREBASE_DATABASE_URL")

        if not service_account_path:
            raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_KEY_PATH is not set in environment or .env file!")
        if not database_url:
            raise RuntimeError("FIREBASE_DATABASE_URL is not set in environment or .env file!")

        if not firebase_admin._apps:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred, {'databaseURL': database_url})
        
        _firebase_initialized = True

# Use the logger defined for this module
logger = logging.getLogger(__name__)

# Import the specialized firebase communication logger
from app.core.logging_config import firebase_comm_logger

def _stringify_value(value: Any) -> str:
    """
    Converts a single value to its string representation.
    Handles None, numbers (including Decimal), dicts/lists (with nested Decimal handling).
    """
    if value is None:
        return ""
    if isinstance(value, (decimal.Decimal, float, int)):
        return str(value)
    if isinstance(value, (dict, list)):
        # json.dumps with default=str ensures nested Decimals are also converted
        return json.dumps(value, default=str)
    return str(value)

async def send_order_to_firebase(order_data: Dict[str, Any], account_type: str = "live") -> bool:
    """
    Sends order data to Firebase Realtime Database under 'trade_data'.
    Only the fields present in order_data are sent (plus account_type and timestamp).
    All field values are converted to strings before sending.
    Returns True if successful, False otherwise.
    """
    try:
        _ensure_firebase_initialized()
        # Log the original order data received
        firebase_comm_logger.info(f"OUTGOING ORDER DATA: {json.dumps(order_data, default=str)}")
        
        # Stringify all present fields
        payload = {k: _stringify_value(v) for k, v in order_data.items()}
        # Always add account_type and timestamp
        payload["account_type"] = account_type
        payload["timestamp"] = _stringify_value(datetime.utcnow().isoformat())
        
        # Log the fully stringified payload that will be pushed to Firebase.
        logger.info(f"[FIREBASE] Payload being pushed to Firebase (all stringified): {payload}")
        firebase_comm_logger.info(f"FIREBASE PUSH: trade_data/{account_type} - {json.dumps(payload, default=str)}")
        
        firebase_database_ref = db.reference("trade_data")
        push_result = firebase_database_ref.push(payload)
        
        if push_result and hasattr(push_result, 'key'):
            firebase_comm_logger.info(f"FIREBASE PUSH RESULT: Key={push_result.key}")
        log_order_id = order_data.get('order_id') or order_data.get('user_id', 'N/A')
        logger.info(f"Order data (ID: {log_order_id}) sent to Firebase successfully.")
        return True
    except Exception as e:
        error_msg = f"Error sending order data to Firebase (ID: {order_data.get('order_id', 'N/A')}): {e}"
        logger.error(error_msg, exc_info=True)
        firebase_comm_logger.error(f"FIREBASE ERROR: {error_msg}", exc_info=True)
        return False

async def get_latest_market_data(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        _ensure_firebase_initialized()
        # Ensure db refers to firebase_admin.db
        ref = db.reference('datafeeds')
        if symbol:
            firebase_comm_logger.debug(f"FIREBASE GET: datafeeds/{symbol.upper()}")
            data = ref.child(symbol.upper()).get()
            firebase_comm_logger.debug(f"FIREBASE RESPONSE: datafeeds/{symbol.upper()} - {json.dumps(data, default=str)}")
            return data
        else:
            firebase_comm_logger.debug(f"FIREBASE GET: datafeeds (all symbols)")
            data = ref.get()
            # Don't log the full response as it could be very large
            firebase_comm_logger.debug(f"FIREBASE RESPONSE: datafeeds - received data for {len(data) if data else 0} symbols")
            return data
    except Exception as e:
        error_msg = f"Error getting market data from Firebase: {e}"
        logger.error(error_msg, exc_info=True)
        firebase_comm_logger.error(f"FIREBASE ERROR: {error_msg}", exc_info=True)
        return None

def get_latest_market_data_sync(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Synchronous version of get_latest_market_data.
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        _ensure_firebase_initialized()
        # Ensure db refers to firebase_admin.db
        ref = db.reference('datafeeds')
        if symbol:
            firebase_comm_logger.debug(f"FIREBASE GET (sync): datafeeds/{symbol.upper()}")
            data = ref.child(symbol.upper()).get()
            firebase_comm_logger.debug(f"FIREBASE RESPONSE (sync): datafeeds/{symbol.upper()} - {json.dumps(data, default=str)}")
            return data
        else:
            firebase_comm_logger.debug(f"FIREBASE GET (sync): datafeeds (all symbols)")
            data = ref.get()
            # Don't log the full response as it could be very large
            firebase_comm_logger.debug(f"FIREBASE RESPONSE (sync): datafeeds - received data for {len(data) if data else 0} symbols")
            return data
    except Exception as e:
        error_msg = f"Error getting market data from Firebase: {e}"
        logger.error(error_msg, exc_info=True)
        firebase_comm_logger.error(f"FIREBASE ERROR (sync): {error_msg}", exc_info=True)
        return None