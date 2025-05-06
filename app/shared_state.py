# app/shared_state.py

import asyncio
import logging

logger = logging.getLogger(__name__)

# This asyncio Queue will be used to pass market data updates from the
# synchronous Firebase streaming thread to an asynchronous task
# that publishes messages to Redis Pub/Sub.
# The maxsize can be adjusted based on expected data volume and publishing speed.
redis_publish_queue: asyncio.Queue = asyncio.Queue(maxsize=500) # Increased size as example

# This queue is no longer used for market data streaming with Redis Pub/Sub.
# websocket_queue: asyncio.Queue = asyncio.Queue(maxsize=100) # Can be removed

logger.info(f"Initialized redis_publish_queue in shared_state with maxsize={redis_publish_queue.maxsize}.")
# logger.info("websocket_queue is not used for market data with Redis Pub/Sub.")


# You can add other shared state variables here if needed later,
# ensuring thread-safe access if modified from multiple threads/tasks.
