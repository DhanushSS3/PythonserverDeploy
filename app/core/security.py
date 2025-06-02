# app/core/security.py

import datetime
from typing import Any, Union, Optional
from passlib.context import CryptContext
from jose import jwt, JWTError
from redis import asyncio as aioredis # Use async Redis client
import json
import logging

from app.database.models import User, DemoUser

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
# Basic config is fine for development, use a more advanced one for production
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        # Log the specific JWT error for debugging if needed
        logger.warning("JWTError during token decoding/validation.", exc_info=True)
        raise JWTError("Could not validate credentials")

# --- Redis Integration ---

# Remove the global client variable here. It will be managed in main.py now.
# redis_client: Optional[aioredis.Redis] = None # REMOVE THIS LINE

async def connect_to_redis() -> Optional[aioredis.Redis]: # Add return type hint
    """
    Establishes connection to the Redis server and returns the client instance.
    Called during application startup. Returns None if connection fails.
    """
    # Remove the global declaration inside the function
    # global redis_client # REMOVE THIS LINE
    logger.info("Attempting to connect to Redis...")
    try:
        logger.info(f"Redis connection parameters: host={settings.REDIS_HOST}, port={settings.REDIS_PORT}, db={settings.REDIS_DB}, password={'<set>' if settings.REDIS_PASSWORD else '<not set>'}")

        # Create the client instance
        client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True # Decode responses to get strings instead of bytes
        )
        # Ping the server to verify the connection is live
        await client.ping()
        logger.info("Connected to Redis successfully.")
        return client # RETURN the client instance

    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
        # Do NOT re-raise here if you want startup to potentially continue without Redis (e.g., for features not requiring Redis)
        # If Redis is *mandatory* for the app to function, re-raise the exception:
        # raise e
        return None # Return None if connection fails


async def close_redis_connection(client: Optional[aioredis.Redis]): # Accept client as argument
    """
    Closes the Redis connection.
    Called during application shutdown. Accepts the client instance to close.
    """
    # Remove global declaration
    # global redis_client # REMOVE THIS LINE
    if client: # Use the passed client
        logger.info("Closing Redis connection...")
        try:
            await client.close()
            logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}", exc_info=True)


import datetime
import json
import logging
from typing import Optional

# Change this import
import redis.asyncio as redis_client # <-- New import and alias

async def store_refresh_token(
    client: redis_client.Redis, # <-- Update the type hint here
    user_id: int,
    refresh_token: str,
    user_type: Optional[str] = None
):
    """
    Stores a refresh token in Redis associated with a user ID.
    Requires an active Redis client instance.
    Now also accepts an optional user_type.
    """
    if not client:
        logger.warning("Redis client not provided to store_refresh_token. Cannot store refresh token.")
        return

    redis_key = f"refresh_token:{refresh_token}"
    expiry_seconds = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    
    # Include user_type in token_data if it's provided
    token_data = {
        "user_id": user_id,
        "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(seconds=expiry_seconds)).isoformat()
    }
    if user_type:
        token_data["user_type"] = user_type # Add user_type to the data stored in Redis

    token_data_json = json.dumps(token_data)
    
    logger.info(f"Storing refresh token for user ID {user_id} (Type: {user_type or 'N/A'}). Key: {redis_key}, Expiry (seconds): {expiry_seconds}")

    try:
        await client.set(redis_key, token_data_json, ex=expiry_seconds)
        logger.info(f"Refresh token stored successfully for user ID: {user_id}")
    except Exception as e:
        logger.error(f"Error storing refresh token in Redis for user ID {user_id}: {e}", exc_info=True)


async def get_refresh_token_data(client: aioredis.Redis, refresh_token: str) -> dict[str, Any] | None: # Add client argument
     """
     Retrieves refresh token data from Redis.
     Requires an active Redis client instance.
     """
     if not client:
         logger.warning("Redis client not provided to get_refresh_token_data. Cannot retrieve refresh token.")
         return None

     redis_key = f"refresh_token:{refresh_token}"
     logger.info(f"Attempting to retrieve refresh token data for key: {redis_key}")

     try:
         # Use the passed client instance
         token_data_json = await client.get(redis_key)
         # logger.debug(f"Redis GET result for key {redis_key}: {token_data_json}") # Debug log

         if token_data_json:
             token_data = json.loads(token_data_json)
             # logger.debug(f"Parsed token data from Redis: {token_data}") # Debug log
             return token_data # Relying on Redis TTL for expiry
         else:
             logger.info(f"No refresh token data found in Redis for key: {redis_key}")
             return None # Token not found in Redis
     except json.JSONDecodeError:
         logger.error(f"Failed to decode JSON from Redis data for key {redis_key}: {token_data_json}", exc_info=True)
         return None
     except Exception as e:
         logger.error(f"Error retrieving or parsing refresh token from Redis for key {redis_key}: {e}", exc_info=True)
         return None

async def delete_refresh_token(client: aioredis.Redis, refresh_token: str): # Add client argument
    """
    Deletes a refresh token from Redis.
    Requires an active Redis client instance.
    """
    if not client:
        logger.warning("Redis client not provided to delete_refresh_token. Cannot delete refresh token.")
        return

    redis_key = f"refresh_token:{refresh_token}"
    logger.info(f"Attempting to delete refresh token for key: {redis_key}")

    try:
        # Use the passed client instance
        deleted_count = await client.delete(redis_key)
        if deleted_count > 0:
            logger.info(f"Refresh token deleted from Redis for key: {redis_key}")
        else:
            logger.warning(f"Attempted to delete refresh token, but key not found in Redis: {redis_key}")
    except Exception as e:
        logger.error(f"Error deleting refresh token from Redis for key {redis_key}: {e}", exc_info=True)


# --- Authentication Dependency (for protecting routes) ---

# OAuth2PasswordBearer is a FastAPI utility for handling OAuth2 token flow
# Define the OAuth2 scheme. The tokenUrl points to your login endpoint.
# The auto_error=False allows us to handle authentication errors manually
# (e.g., to return a custom response or require 2FA verification).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/users/login", auto_error=False)

async def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db) # Assuming get_db is the dependency for DB session
) -> User:
    """
    FastAPI dependency to get the current authenticated user from the access token.
    Does NOT enforce 2FA check here. 2FA check is typically done after initial login.
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
        payload = decode_token(token) # Use the local decode_token function
        # The 'sub' claim should contain the user identifier, usually the user ID
        user_id: int = payload.get("sub")
        if user_id is None:
            logger.warning("Access token payload missing 'sub' claim.")
            raise credentials_exception

        # logger.info(f"Access token validated for user ID: {user_id}") # Debug log

        # Fetch the user from the database using the provided session
        result = await db.execute(select(User).filter(User.id == user_id))
        user = result.scalars().first()

        if user is None:
            logger.warning(f"User ID {user_id} from access token not found in database.")
            raise credentials_exception

        # Optional: Check if the user is active/verified - crucial for security
        if user.isActive != 1: # Assuming 'isActive' is the correct attribute/column name
             logger.warning(f"User ID {user_id} is not active or verified.")
             raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is not active or verified."
            )

        # logger.info(f"Authenticated user ID {user_id} retrieved successfully.") # Debug log
        return user

    except JWTError:
        logger.warning("JWTError during access token validation.", exc_info=True)
        raise credentials_exception
    except Exception as e:
        logger.error(f"Unexpected error in get_current_user dependency for token: {token[:20]}... : {e}", exc_info=True) # Log part of token
        raise credentials_exception

# --- Dependency specifically for admin users ---
async def get_current_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """
    FastAPI dependency to get the current authenticated user and check if they are an admin.
    Requires successful authentication via get_current_user first.
    """
    # The get_current_user dependency handles token validation and fetching the user
    # We just need to check the user's role/type
    if current_user.user_type != 'admin': # Assuming 'user_type' is the attribute/column name for user role
        logger.warning(f"User ID {current_user.id} attempted to access admin resource without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource. Admin privileges required."
        )
    # If the user is an admin, return the user object
    return current_user

# You might add other dependencies here, e.g., get_active_user, verify_2fa etc.


# --- security.py ---

from fastapi import Request, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from jose import JWTError
from app.database.models import User
from app.database.session import get_db
from app.core.config import get_settings
from app.core.security import decode_token, get_current_user

settings = get_settings()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# NEW FUNCTION TO SUPPORT SERVICE ACCOUNT JWT
def create_service_account_token(service_name: str, expires_minutes: int = 60):
    import datetime
    from app.core.security import create_access_token
    data = {"sub": "service", "service_name": service_name}
    return create_access_token(data=data, expires_delta=datetime.timedelta(minutes=expires_minutes))

# NEW DEPENDENCY FUNCTION - MODIFIED to handle service accounts targeting demo/live users
async def get_user_from_service_or_user_token(
    request: Request,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(oauth2_scheme)
) -> Union[User, DemoUser]: # <<< MODIFIED: Return type can be User or DemoUser
    """
    Authenticates a user either directly from their token or
    by a service account token operating on behalf of a specific user.
    """
    try:
        payload = decode_token(token)
        if payload.get("sub") == "service":
            # If a service account token, extract target user_id and user_type from request body or query params
            try:
                # Attempt to parse as JSON for body, if not, check query params
                body = await request.json()
            except Exception:
                body = {} # If no JSON body, it's an empty dict
            
            user_id = body.get("user_id") or request.query_params.get("user_id")
            target_user_type = body.get("user_type") or request.query_params.get("user_type") # <<< NEW: Get target user type

            if not user_id:
                raise HTTPException(status_code=400, detail="Missing user_id for service account operation.")
            
            # Validate target_user_type
            if target_user_type not in ["live", "demo"]:
                raise HTTPException(status_code=400, detail="Invalid target_user_type. Must be 'live' or 'demo'.")

            # Query the appropriate table based on target_user_type
            if target_user_type == "demo":
                stmt = select(DemoUser).where(DemoUser.id == int(user_id))
            else: # Default to live if not explicitly 'demo'
                stmt = select(User).where(User.id == int(user_id))

            result = await db.execute(stmt)
            user = result.scalars().first()
            if not user:
                raise HTTPException(status_code=404, detail=f"Target user (ID: {user_id}, Type: {target_user_type}) not found.")
            return user
        else:
            # For regular user tokens, defer to get_current_user
            return await get_current_user(db=db, token=token)
    except HTTPException:
        # Re-raise HTTPExceptions directly
        raise
    except Exception as e:
        logger.error(f"Authentication error in get_user_from_service_or_user_token: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
