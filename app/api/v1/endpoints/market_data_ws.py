# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
import asyncio
import logging
import json
from typing import Dict, Any, List # Import List for type hinting
import threading

# Configure logging for this module
logger = logging.getLogger(__name__) # Get logger instance

# We no longer import redis_client directly here.
# It will be passed as an argument to redis_market_data_broadcaster.


router = APIRouter(
    tags=["market_data"] # Use a new tag for WebSocket endpoints if desired
)

# List to keep track of active WebSocket connections for *this process*
# This list is local to the process and needs a lock for thread-safe access
# if accessed from multiple async tasks or threads within this process.
active_websocket_connections: List[WebSocket] = []
connections_lock = threading.Lock() # Lock for thread-safe access to active_websocket_connections

# Define the Redis channel for market data updates - must match the publisher
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'

# Background task to subscribe to Redis Pub/Sub and broadcast to local WebSockets
async def redis_market_data_broadcaster(redis_client):
    """
    Background task that subscribes to the Redis market data channel
    and broadcasts received messages to all active WebSocket connections
    in this process. Accepts the initialized redis_client.
    """
    logger.info(f"Redis market data broadcaster task started. Subscribing to channel '{REDIS_MARKET_DATA_CHANNEL}'.")

    if not redis_client:
        logger.critical("Redis client not provided to broadcaster task. Cannot subscribe to market data channel.")
        # Keep the task alive but inactive if Redis client is missing
        while True:
            await asyncio.sleep(60)
        # Or, if Redis is mandatory, raise an exception to stop the task/process
        # raise ConnectionError("Redis client not available for Pub/Sub.")


    try:
        # Get a Pub/Sub connection
        # Need to handle potential connection errors here
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
            logger.info(f"Successfully subscribed to Redis channel '{REDIS_MARKET_DATA_CHANNEL}'.")
        except Exception as e:
             logger.critical(f"FATAL ERROR: Failed to subscribe to Redis channel '{REDIS_MARKET_DATA_CHANNEL}': {e}", exc_info=True)
             # If subscription fails at startup, the task cannot function.
             # Depending on criticality, you might want to exit here or implement reconnect logic.
             while True: # Keep the task alive but non-functional
                 await asyncio.sleep(60)


        # Continuously read messages from the Pub/Sub channel
        while True:
            try:
                # Get the next message. This is an async operation.
                message = await pubsub.get_message(ignore_subscribe_messages=True)

                if message and message['type'] == 'message':
                    # logger.debug(f"Received message from Redis Pub/Sub: {message['data']}")
                    try:
                        # Parse the JSON data from the message
                        updated_data = json.loads(message['data'])
                        # logger.debug(f"Parsed data from Redis Pub/Sub: {updated_data}")

                        # Broadcast the data to all active connections in this process
                        connections_to_remove = []
                        # Acquire the lock only while accessing the list of connections
                        with connections_lock:
                             # Iterate over a copy of the list
                             for connection in active_websocket_connections.copy():
                                 try:
                                     # Send data as JSON
                                     await connection.send_json(updated_data)
                                     # logger.debug(f"Sent data to WebSocket connection {connection.client.host}:{connection.client.port}")
                                 except WebSocketDisconnect:
                                     # Client disconnected gracefully
                                     logger.info(f"WebSocket client disconnected: {connection.client.host}:{connection.client.port}")
                                     connections_to_remove.append(connection)
                                 except Exception as e:
                                     # Catch any other errors during sending
                                     logger.error(f"Error sending data to WebSocket connection {connection.client.host}:{connection.client.port}: {e}", exc_info=True)
                                     connections_to_remove.append(connection) # Mark for removal on error

                             # Remove disconnected/errored connections outside the iteration loop
                             for connection in connections_to_remove:
                                 # Check if the connection is still in the original list before removing
                                 if connection in active_websocket_connections:
                                     try:
                                         active_websocket_connections.remove(connection)
                                         logger.info(f"Removed WebSocket connection: {connection.client.host}:{connection.client.port}. Total active connections: {len(active_websocket_connections)}")
                                     except ValueError:
                                         # Handle case where connection was already removed concurrently
                                         logger.warning(f"Attempted to remove WebSocket connection {connection.client.host}:{connection.client.port} from list, but it was already gone.")

                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode JSON from Redis message: {message['data']}")
                    except Exception as e:
                        logger.error(f"Unexpected error processing Redis message: {e}", exc_info=True)

            except asyncio.CancelledError:
                logger.info("Redis market data broadcaster task cancelled.")
                break # Exit the loop when the task is cancelled
            except Exception as e:
                # Catch any unexpected errors in the Pub/Sub get_message loop
                logger.critical(f"FATAL ERROR: Unexpected error in Redis Pub/Sub listener: {e}", exc_info=True)
                # Implement robust reconnection logic here for production
                logger.warning("Attempting to reconnect to Redis Pub/Sub in 5 seconds...")
                await asyncio.sleep(5)
                try:
                    # Attempt to resubscribe
                    # Need to get a new pubsub instance if the old one is broken
                    pubsub = redis_client.pubsub()
                    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
                    logger.info("Successfully re-subscribed to Redis Pub/Sub.")
                except Exception as reconnect_e:
                    logger.error(f"Failed to re-subscribe to Redis Pub/Sub: {reconnect_e}", exc_info=True)
                    # If reconnection fails, continue the loop and try again after the sleep


    finally:
        # Clean up Pub/Sub connection on task cancellation or exit
        logger.info("Cleaning up Redis Pub/Sub connection.")
        # The `pubsub` object might not exist if subscription failed initially
        if 'pubsub' in locals() and pubsub:
            try:
                # Unsubscribe first
                await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
                # Then close the connection associated with this pubsub instance
                # Note: Closing the pubsub instance might not close the underlying client connection
                # if the client is used elsewhere. The close_redis_connection in main.py handles the main client.
                # Aioredis pubsub instances might manage their own connection.
                # Awaiting unsubscribe and allowing the task to finish is usually sufficient cleanup.
                # await pubsub.close() # Aioredis might not have a specific close for pubsub object itself
                logger.info("Redis Pub/Sub unsubscribe completed.")
            except Exception as e:
                logger.error(f"Error during Redis Pub/Sub cleanup: {e}", exc_info=True)


@router.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for clients to connect and receive real-time market data.
    Handles new connections and disconnections.
    """
    logger.info(f"Attempting to accept WebSocket connection from {websocket.client.host}:{websocket.client.port}...")
    try:
        await websocket.accept()
        logger.info(f"WebSocket connection accepted from {websocket.client.host}:{websocket.client.port}.")

        # Add the new connection to the list in a thread-safe manner
        with connections_lock:
            active_websocket_connections.append(websocket)
            logger.info(f"New WebSocket connection added. Total active connections: {len(active_websocket_connections)}")

        # Keep the connection open. Data sending is handled by the background broadcaster task.
        # This loop is primarily to keep the connection alive and detect client disconnects.
        while True:
            # Wait for any message from the client. This helps detect disconnects.
            # If you don't expect messages, simply waiting is enough.
            try:
                # Await receiving any message to keep the connection open and detect closure
                # You can add logic here if clients send control messages (e.g., subscribe to specific symbols)
                await websocket.receive_text() # Or receive_bytes(), or receive_any()
                logger.debug(f"Received message from {websocket.client.host}:{websocket.client.port}, keeping connection alive.")

            except WebSocketDisconnect as e:
                # Client initiated disconnect
                logger.info(f"WebSocket client disconnected gracefully: {websocket.client.host}:{websocket.client.port}. Code: {e.code}, Reason: {e.reason}")
                # The finally block will handle removal from the list
                break # Exit the loop on disconnect

            except Exception as e:
                # Catch any other errors during message reception
                logger.error(f"Error receiving message from WebSocket client {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
                # Consider closing the connection on unexpected errors
                try:
                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                    logger.info(f"Closed WebSocket connection due to error for {websocket.client.host}:{websocket.client.port}.")
                except Exception as close_e:
                    logger.error(f"Error during WebSocket close after receive error for {websocket.client.host}:{websocket.client.port}: {close_e}")
                break # Exit the loop on error

    except Exception as e:
        # Catch errors during the initial accept() or setup
        logger.error(f"Error accepting or setting up WebSocket connection for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        # If accept() failed, the connection might not be fully established,
        # but the finally block should still attempt cleanup.


    finally:
        # Ensure the connection is removed from the list when it closes (either gracefully or due to error)
        logger.info(f"Cleaning up WebSocket connection for {websocket.client.host}:{websocket.client.port}.")
        with connections_lock:
            if websocket in active_websocket_connections:
                try:
                    active_websocket_connections.remove(websocket)
                    logger.info(f"WebSocket connection removed from active list. Total active connections: {len(active_websocket_connections)}")
                except ValueError:
                     logger.warning(f"Attempted to remove WebSocket connection {websocket.client.host}:{websocket.client.port} from list, but it was already gone.")

        # FastAPI automatically closes the connection on exception or when the endpoint function finishes.
        logger.info(f"WebSocket endpoint function finished for {websocket.client.host}:{websocket.client.port}.")

