# app/shared_state.py

import asyncio
import logging

logger = logging.getLogger(__name__)

# This asyncio Queue will be used to pass market data updates from the
# synchronous Firebase streaming thread to an asynchronous background task
# (e.g., a WebSocket sender task) in the FastAPI application.
# The maxsize can be adjusted based on expected data volume and processing speed.
websocket_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

logger.info("Initialized websocket_queue in shared_state.")

# You can add other shared state variables here if needed later,
# ensuring thread-safe access if modified from multiple threads/tasks.
