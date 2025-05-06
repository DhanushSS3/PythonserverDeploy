# # app/firebase_stream.py

# # Import threading for the data_lock
# import threading
# import time # Still useful for potential delays if needed
# import asyncio
# from typing import Dict, Any, Optional
# from decimal import Decimal # Use Decimal for price values to maintain precision
# import logging

# # Import the firebase_admin.db module and the Event type
# from firebase_admin import db # <-- Import the db module
# from firebase_admin.db import Event # Import the Event type

# # Import the websocket_queue from your shared state module
# try:
#     from app.shared_state import websocket_queue
# except ImportError:
#     import logging
#     logger = logging.getLogger(__name__)
#     logger.error("Could not import websocket_queue from app.shared_state. "
#                  "Please ensure app/shared_state.py exists and is configured correctly.")
#     class DummyQueue:
#         async def put(self, item): # Dummy async put for the async queue
#             logger.warning("Websocket queue not available. Data updates are not being queued.")
#             await asyncio.sleep(0.01) # Simulate some async work
#         def put_nowait(self, item):
#              logger.warning("Websocket queue not available. Data updates are not being queued.")

#     # Create an asyncio queue even for the dummy, as the consumer expects it
#     websocket_queue = asyncio.Queue(maxsize=1) # Small dummy queue
#     logger = logging.getLogger(__name__) # Re-initialize logger


# # This dictionary will store the latest live market data, keyed by symbol.
# # Example: {"AUDCAD": {"bid": Decimal("0.9000"), "ask": Decimal("0.9005")}, ...}
# # Using Decimal for precision, Optional[Decimal] if price might be missing
# live_market_data: Dict[str, Dict[str, Optional[Decimal]]] = {}
# # data_lock is still needed if get_latest_market_data is called from other threads/sync contexts
# # However, if all modifications happen within the single async task, the lock might be simplified.
# # But keeping it for safety if other sync parts of the app might access live_market_data.
# data_lock = threading.Lock() # Lock for thread-safe access to live_market_data

# # Configure logging for this module
# try:
#     from app.logger import logger # Try importing from your app's logger
# except ImportError:
#     import logging
#     # Ensure logging level is set to DEBUG to see detailed stream messages
#     logging.basicConfig(level=logging.DEBUG) # <-- Set to DEBUG for troubleshooting
#     logger = logging.getLogger(__name__)


# def convert_to_decimal(value: Any) -> Optional[Decimal]:
#     """
#     Attempts to convert a value to a Decimal, handling None and potential errors.
#     """
#     if value is None:
#         return None
#     try:
#         # Convert to string first to handle potential float precision issues
#         # Ensure value is not an empty string or other non-numeric type before converting
#         if isinstance(value, str) and not value.strip():
#              return None
#         # Handle scientific notation like '1.23e-4'
#         return Decimal(str(value))
#     except Exception:
#         logger.warning(f"Could not convert value '{value}' to Decimal.", exc_info=True)
#         return None


# async def process_firebase_stream_events(firebase_db_instance, data_path: str):
#     """
#     Asynchronous task that reads events from the Firebase event queue,
#     parses data, updates the shared market data, and queues updates
#     for WebSocket clients.
#     """
#     if not firebase_db_instance:
#         logger.warning("Firebase DB instance not available. Cannot start event processing.")
#         return

#     logger.info(f"Starting Firebase Admin event processing task for path: {data_path}")

#     try:
#         # Get the event queue for the specified path
#         ref = firebase_db_instance.reference(data_path)
#         # CORRECTED CALL: Use getattr to access get_event_queue from the 'db' module
#         # This is a workaround if direct import or db.get_event_queue fails.
#         try:
#             get_event_queue_func = getattr(db, 'get_event_queue')
#             event_queue = get_event_queue_func(ref) # <-- Corrected call using getattr
#             logger.info(f"Firebase Admin event queue obtained for path: {data_path}")
#         except AttributeError:
#             logger.error("Could not access 'get_event_queue' from firebase_admin.db. "
#                          "Your firebase_admin version might be too old or the function is not accessible.")
#             # Re-raise the error or handle it, depending on desired behavior
#             raise # Re-raise to stop startup if essential


#         # Process events from the queue continuously
#         # The get() method on the queue is asynchronous and will wait for events.
#         while True:
#             try:
#                 event: Event = await event_queue.get() # Wait for the next event
#                 # logger.debug(f"Dequeued Firebase event: {event}")

#                 event_type = event.event_type # 'put' or 'patch' or 'keep'
#                 path = event.path # Path relative to the listened path
#                 data = event.data # The actual data received

#                 # --- ENHANCED LOGGING ---
#                 logger.debug(f"--- Firebase Admin Stream Event Received ---")
#                 logger.debug(f"Event Type: {event_type}")
#                 logger.debug(f"Path: {path}")
#                 logger.debug(f"Raw Data: {data}")
#                 logger.debug(f"------------------------------------------")
#                 # --- END ENHANCED LOGGING ---

#                 updated_symbols = {} # Dictionary to hold changes to be sent

#                 # Acquire the data lock before modifying the shared dictionary
#                 with data_lock:
#                     if event_type in ['put', 'patch'] and data is not None:
#                         # logger.info(f"Processing Firebase Admin Stream Event: {event_type} at Path: {path}")

#                         if path == '/':
#                             # This is a patch or put at the root of the listened path (e.g., 'datafeeds').
#                             # Data here should be a dictionary where keys are symbols or symbol/price_type.
#                             if isinstance(data, dict):
#                                 for key, value in data.items():
#                                     # --- Parsing Logic to Handle Both Formats ---

#                                     # Check for format 1: "SYMBOL": { "o": price, "b": price }
#                                     if isinstance(value, dict) and 'o' in value and 'b' in value:
#                                         symbol = key
#                                         bid_price = convert_to_decimal(value.get('o'))
#                                         ask_price = convert_to_decimal(value.get('b'))

#                                         live_market_data[symbol] = {
#                                             "bid": bid_price,
#                                             "ask": ask_price
#                                         }
#                                         updated_symbols[symbol] = live_market_data[symbol]
#                                         logger.debug(f"Parsed format 1 for {symbol}: Bid={bid_price}, Ask={ask_price}")

#                                     # Check for format 2: "SYMBOL/price_type": price
#                                     elif isinstance(key, str) and '/' in key:
#                                          parts = key.split('/')
#                                          if len(parts) == 2:
#                                              symbol, price_type = parts
#                                              symbol = symbol.strip('/')
#                                              price_type = price_type.strip('/')

#                                              if price_type in ['o', 'b']: # Check if the price type is 'o' (bid) or 'b' (ask)
#                                                  price_value = convert_to_decimal(value)

#                                                  if symbol not in live_market_data:
#                                                      live_market_data[symbol] = {"bid": None, "ask": None}

#                                                  if price_type == 'o':
#                                                      live_market_data[symbol]["bid"] = price_value
#                                                  elif price_type == 'b':
#                                                      live_market_data[symbol]["ask"] = price_value

#                                                  updated_symbols[symbol] = live_market_data[symbol] # Mark for update
#                                                  logger.debug(f"Parsed format 2 for {symbol}/{price_type}: Price={price_value}")
#                                              else:
#                                                  logger.warning(f"Unknown price type '{price_type}' in key '{key}' for root path update. Data: {value}")
#                                          else:
#                                              logger.warning(f"Unexpected key format in root path update: '{key}'. Data: {value}")
#                                     else:
#                                         logger.warning(f"Unexpected data structure for key '{key}' in root path update: {value}")
#                             else:
#                                 logger.warning(f"Received root path update with unexpected data type: {type(data)}. Data: {data}")

#                         # --- Handling for child path updates (e.g., /AUDCAD or /AUDCAD/o) ---
#                         elif path.startswith('/'):
#                              path_parts = path.strip('/').split('/')
#                              symbol = path_parts[0]

#                              # Check if the path is a direct price update like /SYMBOL/price_type
#                              if len(path_parts) == 2 and path_parts[1] in ['o', 'b']:
#                                   price_type = path_parts[1]
#                                   value = convert_to_decimal(data) # Convert the incoming data

#                                   if symbol not in live_market_data:
#                                        live_market_data[symbol] = {"bid": None, "ask": None}

#                                   if price_type == 'o':
#                                        live_market_data[symbol]["bid"] = value
#                                   elif price_type == 'b':
#                                        live_market_data[symbol]["ask"] = value

#                                   updated_symbols[symbol] = live_market_data[symbol] # Mark for update
#                                   logger.debug(f"Parsed child path format /SYMBOL/price_type for {symbol}/{price_type}: Price={value}")

#                              # Check if the path is a symbol update with nested bid/ask
#                              elif len(path_parts) == 1 and isinstance(data, dict) and 'o' in data and 'b' in data:
#                                   bid_price = convert_to_decimal(data.get('o'))
#                                   ask_price = convert_to_decimal(data.get('b'))

#                                   live_market_data[symbol] = {
#                                       "bid": bid_price,
#                                       "ask": ask_price
#                                   }
#                                   updated_symbols[symbol] = live_market_data[symbol] # Mark for update
#                                   logger.debug(f"Parsed child path format /SYMBOL for {symbol}: Bid={bid_price}, Ask={ask_price}")

#                              elif data is None and symbol in live_market_data:
#                                    # Handle cases where a symbol node is deleted (though 'remove' event is more common)
#                                    del live_market_data[symbol]
#                                    updated_symbols[symbol] = None # Signal deletion to clients
#                                    logger.info(f"Data for symbol {symbol} removed from live_market_data via put/patch with None data.")
#                              else:
#                                    logger.warning(f"Received child path update for {symbol} with unexpected data structure or value.")
#                                    logger.debug(f"Unexpected child path data for {symbol}: {data}")

#                         else:
#                             logger.warning(f"Received unexpected stream path format: {path}. Data: {data}")

#                     # Handle 'remove' events (data is None for remove events)
#                     elif event_type == 'remove':
#                          logger.info(f"Processing Firebase Admin Stream Event: {event_type} at Path: {path}")
#                          if path and path.startswith('/'):
#                               if path == '/':
#                                   # If the root path is removed, clear all data
#                                   live_market_data.clear()
#                                   updated_symbols = {} # Signal all cleared
#                                   logger.info("Datafeeds path removed, clearing live_market_data.")
#                               elif len(path) > 1:
#                                    # A child path (symbol) is removed (e.g., /AUDCAD)
#                                    symbol = path[1:]
#                                    if symbol in live_market_data:
#                                        del live_market_data[symbol]
#                                        updated_symbols[symbol] = None # Signal deletion
#                                        logger.info(f"Removed symbol {symbol} from live_market_data due to remove event.")
#                               else:
#                                    logger.warning(f"Received remove event with unexpected path format: {path}")

#                     # Handle 'keep' events (sent periodically to keep the connection alive)
#                     elif event_type == 'keep':
#                         logger.debug("Received Firebase stream 'keep' event.")
#                         pass # No action needed for keep-alive events

#                     else:
#                          logger.debug(f"Received unhandled stream event type: {event_type}. Event: {event}")
#                          pass # Ignore unhandled events


#                 # --- Put updated_symbols onto the asyncio queue ---
#                 # This is called from an async task, so we can use put() or put_nowait()
#                 if updated_symbols:
#                     # Use put_nowait to avoid blocking this task if the queue is full.
#                     # The WebSocket sender task should be fast enough to keep up,
#                     # but if not, dropping some updates is better than blocking.
#                     try:
#                         websocket_queue.put_nowait(updated_symbols)
#                         # logger.debug(f"Queued batch update for {len(updated_symbols)} symbols.")
#                     except asyncio.QueueFull:
#                         logger.warning("Websocket queue is full. Skipping sending some updates.")
#                     except Exception as e:
#                         logger.error(f"Error putting data onto websocket queue: {e}", exc_info=True)

#             except asyncio.CancelledError:
#                 logger.info("Firebase event processing task cancelled.")
#                 break # Exit the loop when the task is cancelled
#             except Exception as e:
#                 logger.error(f"Unexpected error processing Firebase event: {e}", exc_info=True)
#                 # Add a small delay before processing the next event on unexpected errors
#                 await asyncio.sleep(0.1)

#     except Exception as e:
#         # This outer catch block will now also catch the AttributeError from getattr
#         logger.error(f"Error getting Firebase event queue or starting processing loop: {e}", exc_info=True)

#     logger.info("Firebase Admin event processing task finished.")


# # We no longer need start_firebase_streaming or stop_firebase_streaming as
# # separate thread management functions. The event processing is an async task.

# # The get_latest_market_data function remains the same for accessing the data dictionary.
# def get_latest_market_data(symbol: str = None) -> Dict[str, Optional[Decimal]] | Dict[str, Dict[str, Optional[Decimal]]]:
#     """
#     Retrieves the latest market data from the shared dictionary.
#     Optionally get data for a specific symbol.
#     Ensures thread-safe access using data_lock.

#     Returns:
#         If symbol is provided, returns a dictionary like {"bid": Decimal, "ask": Decimal}.
#         If symbol is None, returns the entire live_market_data dictionary.
#         Returns copies to prevent external modification.
#     """
#     with data_lock:
#         if symbol:
#             # Return data for a specific symbol
#             # Return a copy of the symbol's data to prevent external modification
#             return live_market_data.get(symbol, {}).copy()
#         else:
#             # Return all live market data (a copy to avoid external modification)
#             return live_market_data.copy()


# app/firebase_stream.py

import asyncio
import json
from firebase_admin import db
from app.shared_state import websocket_queue

# In-memory store for latest market prices
live_market_data = {}

def get_latest_market_data(symbol: str = None):
    """
    Returns the latest market data. If a symbol is provided, returns only that symbol's data.
    """
    if symbol:
        return live_market_data.get(symbol.upper())
    return live_market_data


async def process_firebase_stream_events(firebase_db_instance, path: str):
    """
    Asynchronously listens to changes at a specific path in Firebase Realtime Database.
    Pushes updates to an in-memory store and a shared async queue.
    """
    print(f"✅ Listening for Firebase updates at path: '{path}'")

    # Firebase callback handler (runs in a background thread)
    def listener(event):
        # This function is called on data updates in the Firebase DB
        try:
            if event.event_type in ['put', 'patch']:
                data = event.data
                path_key = event.path.lstrip('/')  # e.g., 'AUDCAD'

                if isinstance(data, dict):
                    if path_key == "":
                        # Full data dump (initial put)
                        for symbol, item in data.items():
                            symbol = symbol.upper()
                            live_market_data[symbol] = item
                            # Push to queue (for WebSocket use)
                            try:
                                websocket_queue.put_nowait({symbol: item})
                            except asyncio.QueueFull:
                                pass
                    else:
                        symbol = path_key.upper()
                        live_market_data[symbol] = data
                        try:
                            websocket_queue.put_nowait({symbol: data})
                        except asyncio.QueueFull:
                            pass
        except Exception as e:
            print(f"🔥 Error in Firebase listener: {e}")

    # Start listening (runs on a thread internally)
    firebase_db_instance.reference(path).listen(listener)

    # Keep this task alive
    try:
        while True:
            await asyncio.sleep(3600)  # Keep the coroutine alive
    except asyncio.CancelledError:
        print("🛑 Firebase stream listener cancelled")
