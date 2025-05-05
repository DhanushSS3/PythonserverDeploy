# app/firebase_stream.py

import threading
import time
from typing import Dict, Any, Optional
import asyncio
from decimal import Decimal # Use Decimal for price values to maintain precision

# Import Firebase Admin SDK components needed for listening
from firebase_admin import db as firebase_db # Alias db to avoid conflict if needed

# Import the websocket_queue from your shared state module
# Ensure this file exists and is configured correctly.
try:
    from app.shared_state import websocket_queue
except ImportError:
    # Log an error and create a dummy queue for graceful failure
    import logging
    logger = logging.getLogger(__name__)
    logger.error("Could not import websocket_queue from app.shared_state. "
                 "Please ensure app/shared_state.py exists and is configured correctly.")
    class DummyQueue:
        def put_nowait(self, item):
            logger.warning("Websocket queue not available. Data updates are not being queued.")
    websocket_queue = DummyQueue()
    # Re-initialize logger for this module if it wasn't imported via app.logger
    logger = logging.getLogger(__name__)


# This dictionary will store the latest live market data, keyed by symbol.
# Example: {"AUDCAD": {"bid": Decimal("0.9000"), "ask": Decimal("0.9005")}, ...}
# Using Decimal for precision, Optional[Decimal] if price might be missing
live_market_data: Dict[str, Dict[str, Optional[Decimal]]] = {}
data_lock = threading.Lock() # Lock for thread-safe access to live_market_data

# We no longer need pyrebase_stream or stream_active event for firebase_admin.db.listen
# firebase_stream = None # To hold the active Firebase stream instance (using firebase_admin)
# stream_active = threading.Event() # Event to signal the streaming thread to stop

# Configure logging for this module if not already configured by app.logger
# This assumes app.logger is configured in app/logger.py or elsewhere
try:
    from app.logger import logger # Try importing from your app's logger
except ImportError:
    import logging
    # Ensure logging level is set to DEBUG to see detailed stream messages
    logging.basicConfig(level=logging.DEBUG) # <-- Set to DEBUG for troubleshooting
    logger = logging.getLogger(__name__)


def convert_to_decimal(value: Any) -> Optional[Decimal]:
    """
    Attempts to convert a value to a Decimal, handling None and potential errors.
    """
    if value is None:
        return None
    try:
        # Convert to string first to handle potential float precision issues
        # Ensure value is not an empty string or other non-numeric type before converting
        if isinstance(value, str) and not value.strip():
             return None
        return Decimal(str(value))
    except Exception:
        logger.warning(f"Could not convert value '{value}' to Decimal.", exc_info=True)
        return None


def firebase_admin_stream_listener(event):
    """
    Callback function to handle incoming data changes from the firebase_admin stream.
    This listener receives DataSnapshot objects from the Realtime Database.
    Parses the data, updates the live_market_data dictionary, and queues updates.
    """
    # The event object from firebase_admin.db.listen contains type, path, and data
    event_type = event.event_type # 'put' or 'patch'
    path = event.path # Path relative to the listened path (e.g., '/' for root, '/AUDCAD')
    data = event.data # The actual data received

    # --- ENHANCED LOGGING ---
    logger.debug(f"--- Firebase Admin Stream Event Received ---")
    logger.debug(f"Event Type: {event_type}")
    logger.debug(f"Path: {path}")
    logger.debug(f"Raw Data: {data}")
    logger.debug(f"------------------------------------------")
    # --- END ENHANCED LOGGING ---


    updated_symbols = {} # Dictionary to hold changes to be sent

    # Acquire the data lock before modifying the shared dictionary
    with data_lock:
        if event_type in ['put', 'patch'] and data is not None:
            # logger.info(f"Processing Firebase Admin Stream Event: {event_type} at Path: {path}")

            if path == '/':
                # This is a patch or put at the root of the listened path (e.g., 'datafeeds').
                # Data here should be a dictionary where keys are symbols or symbol/price_type.
                if isinstance(data, dict):
                    for key, value in data.items():
                        # --- Parsing Logic to Handle Both Formats ---

                        # Check for format 1: "SYMBOL": { "o": price, "b": price }
                        if isinstance(value, dict) and 'o' in value and 'b' in value:
                            symbol = key
                            bid_price = convert_to_decimal(value.get('o'))
                            ask_price = convert_to_decimal(value.get('b'))

                            live_market_data[symbol] = {
                                "bid": bid_price,
                                "ask": ask_price
                            }
                            updated_symbols[symbol] = live_market_data[symbol]
                            logger.debug(f"Parsed format 1 for {symbol}: Bid={bid_price}, Ask={ask_price}")

                        # Check for format 2: "SYMBOL/price_type": price
                        elif isinstance(key, str) and '/' in key:
                             parts = key.split('/')
                             if len(parts) == 2:
                                 symbol, price_type = parts
                                 symbol = symbol.strip('/')
                                 price_type = price_type.strip('/')

                                 if price_type in ['o', 'b']: # Check if the price type is 'o' (bid) or 'b' (ask)
                                     price_value = convert_to_decimal(value)

                                     if symbol not in live_market_data:
                                         live_market_data[symbol] = {"bid": None, "ask": None}

                                     if price_type == 'o':
                                         live_market_data[symbol]["bid"] = price_value
                                     elif price_type == 'b':
                                         live_market_data[symbol]["ask"] = price_value

                                     updated_symbols[symbol] = live_market_data[symbol] # Mark for update
                                     logger.debug(f"Parsed format 2 for {symbol}/{price_type}: Price={price_value}")
                                 else:
                                     logger.warning(f"Unknown price type '{price_type}' in key '{key}' for root path update. Data: {value}")
                             else:
                                 logger.warning(f"Unexpected key format in root path update: '{key}'. Data: {value}")
                        else:
                            logger.warning(f"Unexpected data structure for key '{key}' in root path update: {value}")
                else:
                    logger.warning(f"Received root path update with unexpected data type: {type(data)}. Data: {data}")

            # --- Handling for child path updates (e.g., /AUDCAD or /AUDCAD/o) ---
            elif path.startswith('/'):
                 path_parts = path.strip('/').split('/')
                 symbol = path_parts[0]

                 # Check if the path is a direct price update like /SYMBOL/price_type
                 if len(path_parts) == 2 and path_parts[1] in ['o', 'b']:
                      price_type = path_parts[1]
                      value = convert_to_decimal(data) # Convert the incoming data

                      if symbol not in live_market_data:
                           live_market_data[symbol] = {"bid": None, "ask": None}

                      if price_type == 'o':
                           live_market_data[symbol]["bid"] = value
                      elif price_type == 'b':
                           live_market_data[symbol]["ask"] = value

                      updated_symbols[symbol] = live_market_data[symbol] # Mark for update
                      logger.debug(f"Parsed child path format /SYMBOL/price_type for {symbol}/{price_type}: Price={value}")

                 # Check if the path is a symbol update with nested bid/ask
                 elif len(path_parts) == 1 and isinstance(data, dict) and 'o' in data and 'b' in data:
                      bid_price = convert_to_decimal(data.get('o'))
                      ask_price = convert_to_decimal(data.get('b'))

                      live_market_data[symbol] = {
                          "bid": bid_price,
                          "ask": ask_price
                      }
                      updated_symbols[symbol] = live_market_data[symbol] # Mark for update
                      logger.debug(f"Parsed child path format /SYMBOL for {symbol}: Bid={bid_price}, Ask={ask_price}")

                 elif data is None and symbol in live_market_data:
                       # Handle cases where a symbol node is deleted (though 'remove' event is more common)
                       del live_market_data[symbol]
                       updated_symbols[symbol] = None # Signal deletion to clients
                       logger.info(f"Data for symbol {symbol} removed from live_market_data via put/patch with None data.")
                 else:
                       logger.warning(f"Received child path update for {symbol} with unexpected data structure or value.")
                       logger.debug(f"Unexpected child path data for {symbol}: {data}")

            else:
                logger.warning(f"Received unexpected stream path format: {path}. Data: {data}")


        # Handle 'remove' events (data is None for remove events)
        elif event_type == 'remove':
             logger.info(f"Processing Firebase Admin Stream Event: {event_type} at Path: {path}")
             if path and path.startswith('/'):
                  if path == '/':
                      # If the root path is removed, clear all data
                      live_market_data.clear()
                      updated_symbols = {} # Signal all cleared
                      logger.info("Datafeeds path removed, clearing live_market_data.")
                  elif len(path) > 1:
                       # A child path (symbol) is removed (e.g., /AUDCAD)
                       symbol = path[1:]
                       if symbol in live_market_data:
                           del live_market_data[symbol]
                           updated_symbols[symbol] = None # Signal deletion
                           logger.info(f"Removed symbol {symbol} from live_market_data due to remove event.")
                  else:
                       logger.warning(f"Received remove event with unexpected path format: {path}")

        else:
             logger.debug(f"Received unhandled stream event type: {event_type}. Message: {event}")
             pass # Ignore unhandled events


    # --- Put updated_symbols onto the asyncio queue ---
    # This is a synchronous operation and won't block the stream listener thread.
    # The background FastAPI task (in main.py) will pick it up asynchronously
    # and send it to WebSocket clients.
    if updated_symbols:
        # logger.debug(f"Queueing updates for {len(updated_symbols)} symbols.") # Keep debug if needed
        try:
            # Use put_nowait as this is called from a synchronous thread.
            # The updated_symbols dictionary contains only the symbols that changed.
            # Queue the dictionary containing the updated symbols and their data
            websocket_queue.put_nowait(updated_symbols)
        except asyncio.QueueFull:
            logger.warning("Websocket queue is full. Skipping sending some updates.")
        except Exception as e:
            logger.error(f"Error putting data onto websocket queue: {e}", exc_info=True)


def start_firebase_streaming(firebase_db_instance, data_path: str):
    """
    Starts the firebase_admin streaming listener in this thread.
    This function is designed to be run as the target of a thread.
    The .listen() method is blocking in the thread it's called from.
    It will automatically handle reconnections.
    """
    if firebase_db_instance:
        logger.info(f"Attempting to start Firebase Admin streaming for path: {data_path}")
        try:
            # The listen() method attaches a listener and blocks the current thread.
            # It will automatically handle reconnections.
            # The listener function (firebase_admin_stream_listener) will be called
            # whenever data changes at the specified path or its children.
            logger.info("Calling firebase_db_instance.reference(data_path).listen(...)")
            firebase_db_instance.reference(data_path).listen(firebase_admin_stream_listener)
            # This line will only be reached if listen() somehow returns (which it shouldn't normally)
            logger.warning("Firebase Admin stream listener stopped unexpectedly.")

        except Exception as e:
            # Catch exceptions during listener setup or unexpected stops
            logger.error(f"Firebase Admin stream listener error: {e}", exc_info=True)
            # The thread will likely exit after this unhandled exception
    else:
        logger.warning("Firebase DB instance not available for streaming. Cannot start streaming.")


def stop_firebase_streaming():
    """
    Note: firebase_admin.db.listen() does not return a stream object with a .close() method
    like pyrebase. The listener runs indefinitely in its thread.
    To stop it gracefully, you would typically need a mechanism within the listener
    or the thread's target function to check a stop signal (like a threading.Event)
    and break out of the listening loop if listen() supported one, or if you were
    using a different listening pattern (e.g., get_event_queue).

    For listen(), stopping the thread often requires more forceful methods
    or relying on the main process exiting (if daemon=True).
    We'll keep the function signature but acknowledge the limitation for listen().
    """
    # With firebase_admin.db.listen(), there isn't a direct way to "unlisten"
    # or gracefully stop the blocking call from another thread using a method.
    # The thread will typically run until the main process exits if it's a daemon thread.
    # If you needed more control, you might explore firebase_admin.db.get_event_queue()
    # and process events from that queue in a loop that checks a stop event.
    logger.info("Attempted to stop Firebase Admin streaming. Note: firebase_admin.db.listen() does not support graceful stopping via a .close() method in another thread.")
    logger.info("The streaming thread (if daemon=True) will exit when the main application process exits.")


def get_latest_market_data(symbol: str = None) -> Dict[str, Optional[Decimal]] | Dict[str, Dict[str, Optional[Decimal]]]:
    """
    Retrieves the latest market data from the shared dictionary.
    Optionally get data for a specific symbol.
    Ensures thread-safe access using data_lock.

    Returns:
        If symbol is provided, returns a dictionary like {"bid": Decimal, "ask": Decimal}.
        If symbol is None, returns the entire live_market_data dictionary.
        Returns copies to prevent external modification.
    """
    with data_lock:
        if symbol:
            # Return data for a specific symbol
            # Return a copy of the symbol's data to prevent external modification
            return live_market_data.get(symbol, {}).copy()
        else:
            # Return all live market data (a copy to avoid external modification)
            return live_market_data.copy()

