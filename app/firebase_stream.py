# app/firebase_stream.py

import asyncio
import json
import logging
import threading
import decimal # Import decimal
import time # Import time for timestamping

from firebase_admin import db
from firebase_admin.db import Event

import firebase_admin
from firebase_admin import db

# This will get the database service for the default app that was initialized in main.py
# Ensure that firebase_admin.initialize_app() in main.py runs before this module is imported.
firebase_db = db

# Import the redis_publish_queue from your shared state module
try:
    from app.shared_state import redis_publish_queue
    logger = logging.getLogger(__name__)
    # logger.info("Successfully imported redis_publish_queue from shared_state.") # Optional: keep if needed
except ImportError:
    logger = logging.getLogger(__name__)
    logger.critical("FATAL ERROR: Could not import redis_publish_queue from app.shared_state. Cannot queue data for Redis publishing.")
    class DummyQueue:
        async def put(self, item):
            logger.warning("DummyQueue: put called, data discarded.")
            await asyncio.sleep(0.001)
        def put_nowait(self, item):
             logger.warning("DummyQueue: put_nowait called, data discarded.")
    redis_publish_queue = DummyQueue()


# In-memory store for latest market prices
# This is still useful for the snapshot endpoint (/api/v1/market-data)
live_market_data = {}
data_lock = threading.Lock()

# Counter for listener triggers
listener_trigger_count = 0

def get_latest_market_data(symbol: str = None):
    with data_lock:
        if symbol:
            return live_market_data.get(symbol.upper(), {}).copy()
        return live_market_data.copy()

# Event to keep the main async task alive while the listener runs in a background thread
_keep_alive_event = asyncio.Event()

# Store the Firebase listener handle for cleanup
_listener_handle = None


# --- Main async task for Firebase Stream Processing ---
async def process_firebase_events(firebase_db_instance, path: str = 'datafeeds'):
    """
    Listens to the specified Firebase Realtime Database path and puts incoming data onto a shared queue.
    Runs in a background asyncio task (in the main event loop's thread).
    """
    logger.info(f"Starting Firebase Admin event processing task for path: '{path}'. Queuing data for Redis publishing.")

    if not firebase_db_instance:
        logger.critical("Firebase database instance not provided. Cannot start listener.")
        return

    # Get the running asyncio event loop from the main thread
    try:
        loop = asyncio.get_running_loop()
        logger.debug("Successfully got running event loop in process_firebase_events task.")
    except RuntimeError:
        logger.critical("FATAL ERROR: Could not get running event loop in process_firebase_events task.", exc_info=True)
        return # Cannot proceed if we can't get the loop

    # --- Firebase callback handler (runs in a background thread managed by Firebase Admin SDK) ---
    # Define listener inside to capture the 'loop' variable from the outer scope
    def listener(event: Event):
        """
        Callback function executed by Firebase Admin SDK on data updates.
        Runs in a separate thread. Puts updates onto the redis_publish_queue
        using call_soon_threadsafe to schedule on the main event loop.
        """
        global listener_trigger_count
        listener_trigger_count += 1

        logger.debug(f"Firebase listener triggered ({listener_trigger_count}). Event Type: {event.event_type}, Path: {event.path}. Timestamp: {time.time()}")

        try:
            data_for_queue = None # Data to be put onto the queue

            if event.event_type in ['put', 'patch']:
                data = event.data
                path_key = event.path.lstrip('/')

                with data_lock:
                    if path_key == "": # Root path update (e.g., path is '/')
                        if isinstance(data, dict):
                            updated_symbols_batch = {}
                            for key, value in data.items(): # key might be 'USDJPY/o', 'EURUSD/b', or just 'EURUSD'
                                symbol_upper = None
                                price_type = None # 'o' or 'b'

                                if '/' in key: # Handles keys like 'USDJPY/o' or 'EURUSD/b'
                                    parts = key.split('/', 1)
                                    symbol_upper = parts[0].upper()
                                    if len(parts) > 1 and parts[1] in ['o', 'b']:
                                        price_type = parts[1]
                                    else:
                                        logger.warning(f"Malformed key '{key}' at root path. Expected format 'SYMBOL/o' or 'SYMBOL/b'. Skipping.")
                                        continue
                                else: # Handles keys like 'EURUSD' where value is expected to be a dict {'o': ..., 'b': ...}
                                    symbol_upper = key.upper()

                                if not symbol_upper:
                                    logger.warning(f"Could not parse symbol from key '{key}' at root path. Skipping.")
                                    continue

                                # Ensure the entry for the symbol in live_market_data is a dictionary
                                if symbol_upper not in live_market_data or not isinstance(live_market_data[symbol_upper], dict):
                                    live_market_data[symbol_upper] = {"o": None, "b": None}
                                
                                # Ensure the entry for the symbol in updated_symbols_batch is a dictionary (or will be)
                                if symbol_upper not in updated_symbols_batch or not isinstance(updated_symbols_batch.get(symbol_upper), dict):
                                     # Initialize from live_market_data if exists and is dict, else fresh dict
                                    updated_symbols_batch[symbol_upper] = live_market_data[symbol_upper].copy() if isinstance(live_market_data.get(symbol_upper), dict) else {"o": None, "b": None}


                                if price_type: # Individual price update like 'USDJPY/o': 123.45
                                    if isinstance(value, (str, int, float, decimal.Decimal)):
                                        live_market_data[symbol_upper][price_type] = value
                                        updated_symbols_batch[symbol_upper][price_type] = value
                                    else:
                                        logger.warning(f"Received non-numeric data for individual price '{key}': {value} at root path. Skipping.")
                                elif isinstance(value, dict): # Full symbol update like 'EURUSD': {'o': ..., 'b': ...}
                                    # This case assumes 'value' is a dict like {'o': price, 'b': price}
                                    live_market_data[symbol_upper] = value # Update the whole symbol's data
                                    updated_symbols_batch[symbol_upper] = value # Batch the whole symbol's data
                                else:
                                    logger.warning(f"Received non-dict and non-individual price data for key '{key}' at root path: {value}. Skipping update for this key.")

                            if updated_symbols_batch:
                                # Filter out any entries that might still not be proper dicts, though logic above tries to prevent this
                                data_for_queue = {k: v for k, v in updated_symbols_batch.items() if isinstance(v, dict)}
                                if data_for_queue:
                                    logger.debug(f"Prepared batch update for {len(data_for_queue)} symbols from root path for queue. Preview: {str(data_for_queue)[:200]}")
                                else:
                                    logger.debug("No valid symbol data to queue from root path update after filtering.")
                        else:
                            logger.warning(f"Received non-dict data at root path '{event.path}': {data}. Expected dict.")

                    elif path_key: # Specific symbol path update (e.g., path is '/EURUSD' or '/EURUSD/o')
                        path_parts = path_key.split('/')
                        symbol = path_parts[0].upper()

                        if len(path_parts) == 1: # Path is like '/EURUSD'
                            if isinstance(data, dict): # Expecting data like {'o': 1.23, 'b': 1.24}
                                live_market_data[symbol] = data
                                data_for_queue = {symbol: data}
                                logger.debug(f"Prepared update for symbol '{symbol}' from child path for queue.")
                            elif data is None and symbol in live_market_data: # Deletion of a symbol
                                del live_market_data[symbol]
                                data_for_queue = {symbol: None} # Signal deletion
                                logger.info(f"Prepared deletion signal for symbol '{symbol}' for queue.")
                            else:
                                logger.warning(f"Received unexpected data type or value for symbol path '{path_key}': {data}. Expected dict or None.")

                        elif len(path_parts) == 2 and path_parts[1] in ['o', 'b']: # Path is like '/EURUSD/o'
                             price_type = path_parts[1]
                             if isinstance(data, (str, int, float, decimal.Decimal)): # Expecting direct price value
                                if symbol not in live_market_data or not isinstance(live_market_data[symbol], dict):
                                    live_market_data[symbol] = {"o": None, "b": None} # Initialize if not exists or not dict
                                
                                live_market_data[symbol][price_type] = data
                                data_for_queue = {symbol: live_market_data[symbol].copy()} # Send the full symbol data
                                logger.debug(f"Prepared partial update for symbol '{symbol}' ({price_type}) for queue.")
                             else:
                                  logger.warning(f"Received unexpected data type for price update for symbol '{symbol}' ({price_type}): {type(data)}. Data: {data}. Expected string, int, float or Decimal.")
                        else:
                            logger.warning(f"Received update for unhandled child path format '{path_key}': {data}")
                    else:
                         logger.warning(f"Received put/patch event with unexpected path format: '{event.path}'. Data: {data}")


            elif event.event_type == 'remove':
                 logger.info(f"Processing Firebase Admin Stream Event: {event.event_type} at Path: {event.path}")
                 path_key = event.path.lstrip('/')
                 with data_lock:
                     if path_key == "":
                         live_market_data.clear()
                         data_for_queue = {"_all_removed": True}
                         logger.info("Prepared signal for all data removed for queue.")
                     elif path_key and path_key in live_market_data:
                          del live_market_data[path_key]
                          data_for_queue = {path_key: None}
                          logger.info(f"Prepared deletion signal for symbol '{path_key}' for queue.")
                     else:
                          logger.warning(f"Received remove event for unknown path or format: '{event.path}'")

            elif event.event_type == 'keep-alive': # Firebase uses 'keep-alive' not 'keep'
                 logger.debug(f"Firebase listener received keep-alive event. Timestamp: {time.time()}")
                 pass # No action needed for keep-alive events

            else:
                 logger.debug(f"Received unhandled stream event type: {event.event_type}. Event: {event}")
                 pass

            if data_for_queue is not None:
                try:
                    if isinstance(data_for_queue, dict):
                         data_for_queue['_timestamp'] = time.time()

                    loop.call_soon_threadsafe(redis_publish_queue.put_nowait, data_for_queue)
                    logger.debug(f"Successfully queued data for path '{event.path}'. Queue size (approx): {redis_publish_queue.qsize()}. Data preview: {str(data_for_queue)[:200]}...")
                except asyncio.QueueFull:
                    logger.warning(f"redis_publish_queue is full. Dropping data for path '{event.path}'. Publisher task might be stuck or slow.")
                except Exception as e:
                    logger.error(f"Error putting data onto redis_publish_queue using call_soon_threadsafe for path '{event.path}': {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Unexpected error in Firebase listener callback for event type '{event.event_type}' at path '{event.path}': {e}", exc_info=True)

        logger.debug(f"Firebase listener callback finished for event type {event.event_type} at path {event.path}.")


    # --- Start Listening and Keep Task Alive ---
    global _listener_handle
    try:
        ref = firebase_db_instance.reference(path)
        logger.info(f"Attempting to start Firebase Admin listener for path: '{path}'.")
        _listener_handle = ref.listen(listener)
        logger.info(f"Firebase Admin listener started successfully for path: '{path}'. Queuing data for Redis publishing.")

        try:
            await _keep_alive_event.wait()
            logger.info("Firebase event processing task finished waiting on keep-alive event.")
        except asyncio.CancelledError:
            logger.info("Firebase event processing task cancelled.")
            raise
        finally:
            # Ensure cleanup happens even if we're cancelled
            if _listener_handle is not None:
                _listener_handle.close()
                _listener_handle = None
                logger.info("Firebase listener closed.")

    except Exception as e:
        logger.critical(f"FATAL ERROR: Firebase event processing task failed during setup or main loop for path '{path}': {e}", exc_info=True)
        raise
    finally:
        logger.info("Firebase Admin event processing task finished.")


def cleanup_firebase():
    """Clean up Firebase resources. Call this during application shutdown."""
    global _listener_handle
    if _listener_handle is not None:
        try:
            _listener_handle.close()
            logger.info("Firebase listener closed during cleanup.")
        except Exception as e:
            logger.error(f"Error closing Firebase listener during cleanup: {e}", exc_info=True)
        finally:
            _listener_handle = None
    
    # Signal any waiting tasks to complete
    _keep_alive_event.set()
