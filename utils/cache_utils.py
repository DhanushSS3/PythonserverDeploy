# # app/utils/cache_utils.py
# from redis.asyncio import Redis
# from app.core.cache import REDIS_USER_DATA_KEY_PREFIX
# import logging

# logger = logging.getLogger(__name__)

# async def clear_user_data_cache(redis_client: Redis, user_id: int) -> bool:
#     """
#     Clears the user data cache entry for a specific user ID.

#     Args:
#         redis_client: The asynchronous Redis client instance.
#         user_id: The ID of the user whose cache should be cleared.

#     Returns:
#         True if the key was deleted, False otherwise (e.g., key not found or error).
#     """
#     if not redis_client:
#         logger.warning(f"Redis client not available to clear user data cache for user {user_id}.")
#         return False

#     key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
#     try:
#         # The delete command returns the number of keys that were removed.
#         deleted_count = await redis_client.delete(key)
#         if deleted_count > 0:
#             logger.info(f"User data cache cleared for user {user_id}. Key: {key}")
#             return True
#         else:
#             logger.warning(f"User data cache key not found for user {user_id}. Key: {key}")
#             return False
#     except Exception as e:
#         logger.error(f"Error clearing user data cache for user {user_id} (Key: {key}): {e}", exc_info=True)
#         return False

# # You might also want functions to clear other cache types, e.g.:
# # async def clear_user_portfolio_cache(redis_client: Redis, user_id: int):
# #     key = f"{app.core.cache.REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
# #     await redis_client.delete(key)
# # async def clear_group_symbol_settings_cache(redis_client: Redis, group_name: str, symbol: str):
# #     key = f"{app.core.cache.REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}"
# #     await redis_client.delete(key)