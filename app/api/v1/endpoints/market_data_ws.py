# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
import logging
from typing import Dict, Any
import threading 

# Import the websocket_queue from your shared state module
try:
    from app.shared_state import websocket_queue
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.error("Could not import websocket_queue from app.shared_state. "
                 "Please ensure app/shared_state.py exists and is configured correctly.")
    # Define a dummy queue for graceful failure
    class DummyQueue:
        async def get(self):
            await asyncio.sleep(1) # Simulate waiting
            return {} # Return empty data
    websocket_queue = DummyQueue()
    logger = logging.getLogger(__name__) # Re-initialize logger


# Configure logging for this module
try:
    from app.logger import logger # Try importing from your app's logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO) # Default basic config
    logger = logging.getLogger(__name__)


router = APIRouter(
    tags=["market_data"] # Use a new tag for WebSocket endpoints if desired
)

# List to keep track of active WebSocket connections
active_websocket_connections: list[WebSocket] = []
connections_lock = threading.Lock() # Lock for thread-safe access to active_websocket_connections

# Background task to send data from the queue to connected WebSockets
async def websocket_data_sender():
    """
    Background task that continuously reads from the websocket_queue
    and sends data to all active WebSocket connections.
    """
    logger.info("WebSocket data sender task started.")
    while True:
        try:
            # Read data from the queue. This will block until data is available.
            # The data is expected to be a dictionary of updated symbols and their prices.
            updated_data = await websocket_queue.get()
            # logger.debug(f"Dequeued data for WebSocket sender: {updated_data}")

            # Send the updated data to all active connections
            # Iterate over a copy of the list in case connections are removed during iteration
            with connections_lock:
                 connections_to_remove = []
                 for connection in active_websocket_connections.copy():
                     try:
                         # Send data as JSON
                         await connection.send_json(updated_data)
                         # logger.debug(f"Sent data to WebSocket connection {connection.client.host}:{connection.client.port}")
                     except WebSocketDisconnect:
                         logger.info(f"WebSocket client disconnected: {connection.client.host}:{connection.client.port}")
                         connections_to_remove.append(connection)
                     except Exception as e:
                         logger.error(f"Error sending data to WebSocket connection {connection.client.host}:{connection.client.port}: {e}", exc_info=True)
                         connections_to_remove.append(connection) # Mark for removal on error

                 # Remove disconnected/errored connections
                 for connection in connections_to_remove:
                     if connection in active_websocket_connections: # Check if it's still there
                         active_websocket_connections.remove(connection)
                         logger.info(f"Removed WebSocket connection: {connection.client.host}:{connection.client.port}. Active connections: {len(active_websocket_connections)}")

        except asyncio.CancelledError:
            logger.info("WebSocket data sender task cancelled.")
            break # Exit the loop when the task is cancelled
        except Exception as e:
            logger.error(f"Unexpected error in WebSocket data sender task: {e}", exc_info=True)
            await asyncio.sleep(1) # Prevent tight loop on unexpected errors


@router.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for clients to connect and receive real-time market data.
    """
    await websocket.accept()
    logger.info(f"WebSocket connection accepted from {websocket.client.host}:{websocket.client.port}. Adding to active connections.")

    # Add the new connection to the list
    with connections_lock:
        active_websocket_connections.append(websocket)
        logger.info(f"New WebSocket connection added. Total active connections: {len(active_websocket_connections)}")

    try:
        # Keep the connection open. The data sending is handled by the background task.
        # You can add logic here to receive messages from the client if needed (e.g., subscribing to specific symbols).
        # For now, we'll just keep the connection alive.
        while True:
            # You can receive messages here if clients send them
            # data = await websocket.receive_text()
            # logger.debug(f"Received message from WebSocket client: {data}")
            await asyncio.sleep(60) # Keep the connection alive, maybe send a periodic ping/heartbeat

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {websocket.client.host}:{websocket.client.port}")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket endpoint for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
    finally:
        # Ensure the connection is removed from the list when it closes
        with connections_lock:
            if websocket in active_websocket_connections:
                active_websocket_connections.remove(websocket)
                logger.info(f"WebSocket connection removed. Total active connections: {len(active_websocket_connections)}")
        # The connection is automatically closed by FastAPI on exception or return
        logger.info(f"WebSocket connection closed for {websocket.client.host}:{websocket.client.port}.")

