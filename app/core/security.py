# app/core/security.py

import datetime
from typing import Any, Union, Optional
from passlib.context import CryptContext
from jose import jwt, JWTError
from redis import asyncio as aioredis
import json
import logging

# Import necessary components from fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from fastapi import Request # Import Request if needed in dependencies

# Import necessary components for database interaction
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Import your database models and session dependency
from app.database.models import User # Assuming User model is imported here
from app.database.session import get_db # Assuming get_db dependency is imported here

from app.core.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO) # Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
logger = logging.getLogger(__name__) # Get a logger for this module

# Configure the password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Get application settings
settings = get_settings()

# --- Password Hashing Functions ---
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies a plain password against a hashed password.
    """
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """
    Generates a hash for a given plain password.
    """
    return pwd_context.hash(password)

# --- JWT Functions ---

def create_access_token(data: dict, expires_delta: Union[datetime.timedelta, None] = None) -> str:
    """
    Creates a JWT access token.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "iat": datetime.datetime.utcnow()})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict, expires_delta: Union[datetime.timedelta, None] = None) -> str:
    """
    Creates a JWT refresh token.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "iat": datetime.datetime.utcnow()})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def decode_token(token: str) -> dict[str, Any]:
    """
    Decodes a JWT token and returns the payload.
    """
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        raise JWTError("Could not validate credentials")

# --- Redis Integration for Refresh Tokens ---

# Global Redis client instance (initialized on startup)
redis_client: Optional[aioredis.Redis] = None

async def connect_to_redis():
    """
    Establishes connection to the Redis server.
    Called during application startup.
    """
    global redis_client
    logger.info("Attempting to connect to Redis...")
    try:
        logger.info(f"Redis connection parameters: host={settings.REDIS_HOST}, port={settings.REDIS_PORT}, db={settings.REDIS_DB}, password={'<set>' if settings.REDIS_PASSWORD else '<not set>'}")

        redis_client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True # Decode responses to get strings instead of bytes
        )
        await redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        # Consider raising the exception here if Redis is mandatory for startup:
        # raise e


async def close_redis_connection():
    """
    Closes the Redis connection.
    Called during application shutdown.
    """
    global redis_client
    if redis_client:
        logger.info("Closing Redis connection...")
        await redis_client.close()
        logger.info("Redis connection closed.")

async def store_refresh_token(user_id: int, refresh_token: str):
    """
    Stores a refresh token in Redis associated with a user ID.
    """
    if not redis_client:
        logger.warning("Redis client not initialized. Cannot store refresh token.")
        return

    redis_key = f"refresh_token:{refresh_token}"
    expiry_seconds = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    token_data = {"user_id": user_id, "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(seconds=expiry_seconds)).isoformat()}
    token_data_json = json.dumps(token_data)

    logger.info(f"Storing refresh token for user ID {user_id}. Key: {redis_key}, Data: {token_data_json}, Expiry (seconds): {expiry_seconds}")

    try:
        await redis_client.set(redis_key, token_data_json, ex=expiry_seconds)
        logger.info(f"Refresh token stored successfully for user ID: {user_id}")
    except Exception as e:
        logger.error(f"Error storing refresh token in Redis for user ID {user_id}: {e}")

async def get_refresh_token_data(refresh_token: str) -> dict[str, Any] | None:
    """
    Retrieves refresh token data from Redis.
    """
    if not redis_client:
        logger.warning("Redis client not initialized. Cannot retrieve refresh token.")
        return None

    redis_key = f"refresh_token:{refresh_token}"
    logger.info(f"Attempting to retrieve refresh token data for key: {redis_key}")

    try:
        token_data_json = await redis_client.get(redis_key)
        logger.info(f"Redis GET result for key {redis_key}: {token_data_json}")

        if token_data_json:
            token_data = json.loads(token_data_json)
            logger.info(f"Parsed token data from Redis: {token_data}")
            # Optional: Add an extra check for expiry time stored in the value
            # expires_at = datetime.datetime.fromisoformat(token_data["expires_at"])
            # if expires_at > datetime.datetime.utcnow():
            #     return token_data
            # else:
            #     logger.warning(f"Refresh token data found but expired based on value for key: {redis_key}")
            #     await redis_client.delete(redis_key) # Clean up expired token
            #     return None
            return token_data # Relying on Redis TTL for expiry
        else:
            logger.info(f"No refresh token data found in Redis for key: {redis_key}")
            return None # Token not found in Redis
    except Exception as e:
        logger.error(f"Error retrieving or parsing refresh token from Redis for key {redis_key}: {e}")
        return None

async def delete_refresh_token(refresh_token: str):
    """
    Deletes a refresh token from Redis.
    """
    if not redis_client:
        logger.warning("Redis client not initialized. Cannot delete refresh token.")
        return

    redis_key = f"refresh_token:{refresh_token}"
    logger.info(f"Attempting to delete refresh token for key: {redis_key}")

    try:
        deleted_count = await redis_client.delete(redis_key)
        if deleted_count > 0:
            logger.info(f"Refresh token deleted from Redis for key: {redis_key}")
        else:
            logger.warning(f"Attempted to delete refresh token, but key not found in Redis: {redis_key}")
    except Exception as e:
        logger.error(f"Error deleting refresh token from Redis for key {redis_key}: {e}")

# --- Authentication Dependency (for protecting routes) ---

# OAuth2PasswordBearer is a FastAPI utility for handling OAuth2 token flow
from fastapi.security import OAuth2PasswordBearer

# Define the OAuth2 scheme. The tokenUrl points to your login endpoint.
# The auto_error=False allows us to handle authentication errors manually
# (e.g., to return a custom response or require 2FA verification).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/users/login", auto_error=False)

async def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
) -> User:
    """
    FastAPI dependency to get the current authenticated user from the access token.
    Does NOT enforce 2FA check here. 2FA check is done after initial login.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if token is None:
         logger.warning("Access token is missing.")
         raise credentials_exception

    try:
        payload = decode_token(token)
        user_id: int = payload.get("sub")
        if user_id is None:
            logger.warning("Access token payload missing 'sub' claim.")
            raise credentials_exception

        logger.info(f"Access token validated for user ID: {user_id}")

        # Fetch the user from the database
        result = await db.execute(select(User).filter(User.id == user_id))
        user = result.scalars().first()

        if user is None:
            logger.warning(f"User ID {user_id} from access token not found in database.")
            raise credentials_exception

        # Optional: Check if the user is active/verified
        if user.isActive != 1:
             logger.warning(f"User ID {user_id} is not active or verified.")
             raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is not active or verified."
            )

        logger.info(f"Authenticated user ID {user_id} retrieved successfully.")
        return user

    except JWTError:
        logger.warning("JWTError during access token validation.")
        raise credentials_exception
    except Exception as e:
        logger.error(f"Unexpected error in get_current_user dependency for token: {token[:20]}... : {e}") # Log part of token
        raise credentials_exception

# --- Dependency specifically for admin users ---
async def get_current_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """
    FastAPI dependency to get the current authenticated user and check if they are an admin.
    """
    # The get_current_user dependency handles token validation and fetching the user
    # We just need to check the user's role/type
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to access admin resource without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource. Admin privileges required."
        )
    # If the user is an admin, return the user object
    return current_user

