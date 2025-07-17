# # app/dependencies/redis_client.py
# from redis.asyncio import Redis
# from fastapi import HTTPException, status
# from app.core.security import connect_to_redis
# import logging

# logger = logging.getLogger(__name__)

# # Shared global instance
# global_redis_client_instance: Redis | None = None

# async def get_redis_client() -> Redis:
#     global global_redis_client_instance
#     if global_redis_client_instance is None:
#         logger.warning("Redis client not initialized, attempting late connection.")
#         global_redis_client_instance = await connect_to_redis()
#         # logger.info(f"Connecting to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
#         if global_redis_client_instance is None:
#             raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Redis unavailable")
#     return global_redis_client_instance


# app/dependencies/redis_client.py
# app/dependencies/redis_client.py

# app/dependencies/redis_client.py

from redis.asyncio import Redis
from fastapi import HTTPException, status
from app.core.security import connect_to_redis
import logging

logger = logging.getLogger(__name__)

# ✅ Restore global instance for app-wide singleton
global_redis_client_instance: Redis | None = None

async def get_redis_client() -> Redis:
    global global_redis_client_instance
    if global_redis_client_instance is None:
        logger.warning("[Redis] Client not initialized, attempting late connection.")
        global_redis_client_instance = await connect_to_redis()
        if global_redis_client_instance is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis unavailable"
            )
    return global_redis_client_instance
