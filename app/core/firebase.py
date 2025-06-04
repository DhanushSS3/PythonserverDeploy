# # app/core/firebase.py

# import logging
# import json
# from typing import Dict, Any, Optional
# import decimal
# from datetime import datetime

# # Ensure firebase_admin is initialized and imported correctly
# try:
#     import firebase_admin
#     from firebase_admin import db, firestore, credentials
# except ImportError as e:
#     raise ImportError("firebase_admin is not installed or not accessible: " + str(e))

# # Import firebase_db from firebase_stream (should be db from firebase_admin)
# try:
#     from app.firebase_stream import firebase_db
# except ImportError as e:
#     raise ImportError("Could not import firebase_db from app.firebase_stream: " + str(e))

# # Initialize firebase_admin if not already initialized
# import os
# service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH")
# database_url = os.getenv("FIREBASE_DATABASE_URL")
# if not service_account_path:
#     raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_KEY_PATH is not set in environment or .env file!")
# if not database_url:
#     raise RuntimeError("FIREBASE_DATABASE_URL is not set in environment or .env file!")
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credential=credentials.Certificate(service_account_path), options={'databaseURL': database_url})

# logger = logging.getLogger(__name__)

# def _stringify_payload(data: Dict[str, Any]) -> Dict[str, str]:
#     result = {}
#     for k, v in data.items():
#         if v is None:
#             result[k] = ""
#         elif isinstance(v, (float, int, decimal.Decimal)):
#             result[k] = str(v)
#         elif isinstance(v, (dict, list)):
#             result[k] = json.dumps(v)
#         else:
#             result[k] = str(v)
#     return result

# async def send_order_to_firebase(order_data: Dict[str, Any], account_type: str = "live") -> bool:
#     """
#     Sends an order to Firebase Realtime Database under 'trade_data'.
#     Works for both UserOrder and DemoUserOrder, but should be called only for live users.
#     Returns True if successful, False otherwise.
#     """
#     try:
#         # Define all possible order fields to ensure all are present and sent as strings
#         all_order_fields = [
#             "order_id", "order_user_id", "order_company_name", "order_type", "order_status",
#             "order_price", "order_quantity", "contract_value", "margin", "stop_loss", "take_profit",
#             "close_price", "net_profit", "swap", "commission", "cancel_message", "close_message",
#             "takeprofit_id", "stoploss_id", "cancel_id", "modify_id", "stoploss_cancel_id", "takeprofit_cancel_id",
#             "status", "timestamp", "account_type"
#         ]
#         import decimal, json
#         def stringify_value(v):
#             if v is None:
#                 return ""
#             if isinstance(v, (float, int, decimal.Decimal)):
#                 return str(v)
#             if isinstance(v, (dict, list)):
#                 return json.dumps(v, default=str)
#             return str(v)
#         payload = {}
#         for field in all_order_fields:
#             if field == "timestamp":
#                 payload[field] = str(datetime.utcnow().isoformat())
#             elif field == "account_type":
#                 payload[field] = stringify_value(account_type)
#             else:
#                 payload[field] = stringify_value(order_data.get(field))
#         logger.info(f"[FIREBASE] Payload being sent to Firebase (stringified): {payload}")
#         firebase_ref = firebase_db.reference("trade_data")
#         firebase_ref.push(payload)

#         # Ensure every value is a string, including Decimals and nested types
#         import decimal, json
#         def stringify_value(v):
#             if v is None:
#                 return ""
#             if isinstance(v, (float, int, decimal.Decimal)):
#                 return str(v)
#             if isinstance(v, (dict, list)):
#                 return json.dumps(v, default=str)
#             return str(v)
#         payload = {k: stringify_value(v) for k, v in payload.items()}
#         logger.info(f"[FIREBASE] Payload being sent to Firebase (stringified): {payload}")
#         firebase_ref = firebase_db.reference("trade_data")
#         firebase_ref.push(payload)
#         logger.info(f"Order sent to Firebase successfully: {order_data.get('order_id')}")
#         return True
#     except Exception as e:
#         logger.error(f"Error sending order to Firebase: {e}", exc_info=True)
#         return False

# async def get_latest_market_data(symbol: str = None) -> Optional[Dict[str, Any]]:
#     """
#     Gets the latest market data from Firebase for a specific symbol or all symbols.
#     Returns None if data is not available.
#     """
#     try:
#         ref = db.reference('datafeeds')
#         if symbol:
#             data = ref.child(symbol.upper()).get()
#             return data
#         else:
#             data = ref.get()
#             return data
#     except Exception as e:
#         logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
#         return None

# def get_latest_market_data_sync(symbol: str = None) -> Optional[Dict[str, Any]]:
#     """
#     Synchronous version of get_latest_market_data.
#     Gets the latest market data from Firebase for a specific symbol or all symbols.
#     Returns None if data is not available.
#     """
#     try:
#         ref = db.reference('datafeeds')
#         if symbol:
#             data = ref.child(symbol.upper()).get()
#             return data
#         else:
#             data = ref.get()
#             return data
#     except Exception as e:
#         logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
#         return None 


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

# Initialize firebase_admin if not already initialized
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

# Use the logger defined for this module
logger = logging.getLogger(__name__)

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
    All field values are converted to strings before sending.
    Returns True if successful, False otherwise.
    """
    try:
        # Define all possible order fields to ensure all are present and sent as strings.
        # This list should be comprehensive for all data structures passed to this function.
        all_order_fields = [
            "order_id", "order_user_id", "user_id", # user_id can be an alias for order_user_id
            "order_company_name", "order_type", "order_status",
            "order_price", "order_quantity", "contract_value", "margin",
            "stop_loss", "take_profit", "close_price", "net_profit",
            "swap", "commission", "cancel_message", "close_message",
            "takeprofit_id", "stoploss_id", "cancel_id", "close_order_id", "modify_id",
            "stoploss_cancel_id", "takeprofit_cancel_id", "status",
            "timestamp", "account_type", "action"
        ]

        payload = {}
        for field in all_order_fields:
            current_value = order_data.get(field)

            if field == "timestamp":
                # Always generate a fresh UTC timestamp string for the Firebase record
                payload[field] = _stringify_value(datetime.utcnow().isoformat())
            elif field == "account_type":
                payload[field] = _stringify_value(account_type)
            elif field == "user_id" and current_value is None and 'order_user_id' in order_data:
                # If 'user_id' is expected but not present, use 'order_user_id' if available
                payload[field] = _stringify_value(order_data.get('order_user_id'))
            else:
                payload[field] = _stringify_value(current_value)
        
        # Log the fully stringified payload that will be pushed to Firebase.
        logger.info(f"[FIREBASE] Payload being pushed to Firebase (all stringified): {payload}")
        
        # Ensure firebase_db is the correct Realtime Database reference
        firebase_database_ref = db.reference("trade_data") # Use the db from firebase_admin
        firebase_database_ref.push(payload) # Single push operation
        
        # Use a consistent key for logging the order identifier
        log_order_id = order_data.get('order_id') or order_data.get('user_id', 'N/A')
        logger.info(f"Order data (ID: {log_order_id}) sent to Firebase successfully.")
        return True
    except Exception as e:
        # Log the specific order_data as well for better debugging if it's not too large
        logger.error(f"Error sending order data to Firebase (ID: {order_data.get('order_id', 'N/A')}): {e}", exc_info=True)
        # logger.debug(f"Problematic order_data: {order_data}") # Uncomment for verbose debugging
        return False

async def get_latest_market_data(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        # Ensure db refers to firebase_admin.db
        ref = db.reference('datafeeds')
        if symbol:
            data = ref.child(symbol.upper()).get()
            return data
        else:
            data = ref.get()
            return data
    except Exception as e:
        logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
        return None

def get_latest_market_data_sync(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Synchronous version of get_latest_market_data.
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        # Ensure db refers to firebase_admin.db
        ref = db.reference('datafeeds')
        if symbol:
            data = ref.child(symbol.upper()).get()
            return data
        else:
            data = ref.get()
            return data
    except Exception as e:
        logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
        return None