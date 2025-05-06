# app/firebase_stream.py

import asyncio
import json
import logging
import threading # Still needed for data_lock if get_latest_market_data is used sync

from firebase_admin import db
from firebase_admin.db import Event # Import Event type for clarity

# Removed the import of redis_client from app.core.security
# This module no longer needs direct access to the redis client.

# Import the redis_publish_queue from your shared state module
try:
    from app.shared_state import redis_publish_queue
    logger = logging.getLogger(__name__) # Get logger after importing shared_state
    logger.info("Successfully imported redis_publish_queue from shared_state.")
except ImportError:
    # This is a critical error, logging it and providing a dummy queue
    logger = logging.getLogger(__name__) # Define logger even if import fails
    logger.critical("FATAL ERROR: Could not import redis_publish_queue from app.shared_state. Cannot queue data for Redis publishing.")
    class DummyQueue:
        async def put(self, item):
            logger.warning("DummyQueue: put called, data discarded.")
            await asyncio.sleep(0.001) # Small delay to simulate async
        def put_nowait(self, item):
             logger.warning("DummyQueue: put_nowait called, data discarded.")

    redis_publish_queue = DummyQueue() # Use the dummy queue if import fails


# In-memory store for latest market prices
# This is still useful for the snapshot endpoint (/api/v1/market-data)
live_market_data = {}
# A lock is needed for thread-safe access to live_market_data
data_lock = threading.Lock()

def get_latest_market_data(symbol: str = None):
    """
    Returns the latest market data from the in-memory store.
    Optionally returns data for a specific symbol.
    Uses a lock for thread-safe access.
    """
    with data_lock:
        if symbol:
            # Return a copy to prevent external modification
            return live_market_data.get(symbol.upper(), {}).copy()
        # Return a copy of the entire dictionary
        return live_market_data.copy()


async def process_firebase_stream_events(firebase_db_instance, path: str):
    """
    Asynchronously listens to changes at a specific path in Firebase Realtime Database.
    Updates an in-memory store and puts updates onto the redis_publish_queue.
    Runs as an asyncio background task.
    """
    if not firebase_db_instance:
        logger.error("Firebase DB instance not provided. Cannot start event processing.")
        return

    # We no longer check for redis_client here, as publishing is handled by a separate task

    logger.info(f"Starting Firebase Admin event processing task for path: '{path}'. Queuing data for Redis publishing.")

    # Firebase callback handler (runs in a background thread managed by Firebase Admin SDK)
    def listener(event: Event):
        """
        Callback function executed by Firebase Admin SDK on data updates.
        Runs in a separate thread. Puts updates onto the redis_publish_queue.
        """
        try:
            # logger.debug(f"Firebase event received: Type={event.event_type}, Path={event.path}, Data={event.data}")

            data_for_queue = None # Data to be put onto the queue

            if event.event_type in ['put', 'patch']:
                data = event.data
                # path is relative to the listened path, strip leading slash
                path_key = event.path.lstrip('/')

                # Acquire the lock before modifying the shared in-memory dictionary
                with data_lock:
                    if path_key == "":
                        # This is a put/patch at the root of the listened path ('datafeeds')
                        # Data is expected to be a dictionary of symbols
                        if isinstance(data, dict):
                            updated_symbols_batch = {}
                            for symbol, item in data.items():
                                symbol_upper = symbol.upper()
                                live_market_data[symbol_upper] = item # Update in-memory store
                                updated_symbols_batch[symbol_upper] = item # Collect for queue
                            if updated_symbols_batch:
                                data_for_queue = updated_symbols_batch
                                logger.debug(f"Prepared batch update for {len(updated_symbols_batch)} symbols from root path for queue.")
                        else:
                            logger.warning(f"Received non-dict data at root path '{path}': {data}")

                    elif path_key:
                        # This is a put/patch on a child path (e.g., 'AUDCAD' or 'AUDCAD/o')
                        path_parts = path_key.split('/')
                        symbol = path_parts[0].upper()

                        if len(path_parts) == 1:
                            # Update for a whole symbol object (e.g., /AUDCAD)
                            if isinstance(data, dict):
                                live_market_data[symbol] = data # Update in-memory store
                                data_for_queue = {symbol: data} # Collect for queue
                                logger.debug(f"Prepared update for symbol '{symbol}' from child path for queue.")
                            elif data is None and symbol in live_market_data:
                                # Handle deletion if data is None (though 'remove' is more common)
                                del live_market_data[symbol]
                                data_for_queue = {symbol: None} # Signal deletion for queue
                                logger.info(f"Prepared deletion signal for symbol '{symbol}' for queue.")
                            else:
                                logger.warning(f"Received unexpected data type or value for symbol path '{path_key}': {data}")

                        elif len(path_parts) == 2 and path_parts[1] in ['o', 'b']:
                             # Update for a specific price type (e.g., /AUDCAD/o)
                             price_type = path_parts[1] # 'o' for bid, 'b' for ask
                             # Ensure symbol exists in live_market_data before updating price type
                             if symbol not in live_market_data:
                                 live_market_data[symbol] = {"o": None, "b": None} # Initialize if needed

                             # Update the specific price type
                             live_market_data[symbol][price_type] = data # Update in-memory store

                             # Prepare the whole symbol's data after partial update for queue
                             data_for_queue = {symbol: live_market_data[symbol]}
                             logger.debug(f"Prepared partial update for symbol '{symbol}' ({price_type}) for queue.")
                        else:
                            logger.warning(f"Received update for unhandled child path format '{path_key}': {data}")

                    else:
                         logger.warning(f"Received put/patch event with unexpected path format: '{event.path}'. Data: {data}")


            elif event.event_type == 'remove':
                 logger.info(f"Processing Firebase Admin Stream Event: {event.event_type} at Path: {event.path}")
                 path_key = event.path.lstrip('/')
                 with data_lock:
                     if path_key == "":
                         # Root path ('datafeeds') removed
                         live_market_data.clear()
                         # Signal to clients that all data is removed (optional, depends on client handling)
                         data_for_queue = {"_all_removed": True} # Signal deletion for queue
                         logger.info("Prepared signal for all data removed for queue.")
                         logger.info("Cleared live_market_data due to root path removal.")
                     elif path_key and path_key in live_market_data:
                          # Specific symbol removed
                          del live_market_data[path_key]
                          data_for_queue = {path_key: None} # Signal deletion of specific symbol for queue
                          logger.info(f"Prepared deletion signal for symbol '{path_key}' for queue.")
                          logger.info(f"Removed symbol '{path_key}' from live_market_data due to remove event.")
                     else:
                          logger.warning(f"Received remove event for unknown path or format: '{event.path}'")

            elif event.event_type == 'keep':
                # logger.debug("Received Firebase stream 'keep' event.")
                pass # No action needed for keep-alive events

            else:
                 logger.debug(f"Received unhandled stream event type: {event.event_type}. Event: {event}")
                 pass # Ignore unhandled events

            # --- Put data_for_queue onto the redis_publish_queue ---
            if data_for_queue is not None:
                try:
                    # Put the data onto the Redis publish queue (non-blocking from this thread)
                    redis_publish_queue.put_nowait(data_for_queue)
                    logger.debug(f"Put data onto redis_publish_queue. Queue size: {redis_publish_queue.qsize()}")
                except asyncio.QueueFull:
                    logger.warning("Redis publish queue is full, skipping update.")
                except Exception as e:
                    logger.error(f"Error putting data onto redis_publish_queue: {e}", exc_info=True)


        except Exception as e:
            # Catch any unexpected errors within the listener callback
            logger.error(f"Unexpected error in Firebase listener callback for event type '{event.event_type}' at path '{event.path}': {e}", exc_info=True)


    # --- Start Listening and Keep Task Alive ---
    try:
        ref = firebase_db_instance.reference(path)
        # The listen() method runs the callback in a background thread.
        # This async function just needs to keep itself alive.
        ref.listen(listener)
        logger.info(f"Firebase Admin listener started for path: '{path}'. Queuing data for Redis publishing.")

        # Keep this async task alive indefinitely or until cancelled
        while True:
            await asyncio.sleep(3600) # Sleep for a long time, task is kept alive by the loop

    except asyncio.CancelledError:
        logger.info("Firebase event processing task cancelled.")
    except Exception as e:
        # Catch errors that occur during listener setup or the async loop
        logger.critical(f"FATAL ERROR: Firebase event processing task failed: {e}", exc_info=True)
        # Depending on criticality, you might want to implement reconnection logic here
        # or rely on the process manager (like Gunicorn) to restart the worker.

    logger.info("Firebase Admin event processing task finished.")
