from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, Form, Query
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import datetime
import shutil
import os
import uuid
from typing import Optional, List, Dict, Any # Import Dict, Any for type hinting
import logging

# Import select for database queries
from sqlalchemy.future import select
# Import JWTError for exception handling
from jose import JWTError

from app.database.session import get_db
from app.database.models import User, UserOrder # Import User and UserOrder models if positions are handled here
from app.schemas.user import (
    UserCreate,
    UserResponse,
    UserUpdate,
    SendOTPRequest,
    VerifyOTPRequest,
    RequestPasswordReset, # Ensure this is imported
    ResetPasswordConfirm, # Ensure this is imported
    StatusResponse,
    UserLogin,
    Token,
    TokenRefresh
)
from app.crud import user as crud_user
from app.crud import otp as crud_otp
from app.crud.user import generate_unique_account_number
# Assuming you have an email service module
from app.services import email as email_service
from app.core.security import (
    get_password_hash,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    get_current_admin_user, # Import the admin dependency
    store_refresh_token,
    get_refresh_token_data,
    delete_refresh_token
)
from app.core.config import get_settings

# Import the Redis client type and the dependency function from the new location
from redis.asyncio import Redis # Import Redis type
from app.dependencies.redis_client import get_redis_client # <--- CORRECTED IMPORT LOCATION

from app.schemas.user import SignupVerifyOTPRequest, SignupSendOTPRequest

# Configure logging for this module
logger = logging.getLogger(__name__)

# Create an API router for user-related endpoints
router = APIRouter(
    tags=["users"]
)

# Get application settings
settings = get_settings()

# Define the directory for storing uploaded files
# Create the directory if it doesn't exist
UPLOAD_DIRECTORY = "./uploads/proofs"
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True) # Create the directory if it doesn't exist

# Helper function to save uploaded files
async def save_upload_file(upload_file: Optional[UploadFile]) -> Optional[str]:
    """
    Saves an uploaded file to the UPLOAD_DIRECTORY with a unique filename.

    Args:
        upload_file: The UploadFile object, or None.

    Returns:
        The path to the saved file relative to a static serving directory, or None.
    """
    if not upload_file:
        return None

    # Generate a unique filename while preserving the original extension
    file_extension = os.path.splitext(upload_file.filename)[1]
    # Use a more descriptive filename prefix if possible, e.g., include user ID or proof type
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    # Construct the full path to save the file on the server
    full_file_path = os.path.join(UPLOAD_DIRECTORY, unique_filename)

    # Ensure the upload directory exists
    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)

    # Save the file asynchronously
    try:
        # Use async file writing for better performance with async frameworks
        # Alternatively, you could use `await aiofiles.open(...)` if you install aiofiles
        contents = await upload_file.read()
        with open(full_file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        logger.error(f"Error saving uploaded file {upload_file.filename}: {e}", exc_info=True)
        # Decide if you want to raise an exception here or return None
        # For now, we'll re-raise after logging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save uploaded file.")
    finally:
        # Ensure the uploaded file object is closed
        await upload_file.close()

    # Return the path that will be stored in the database (relative to static serving)
    # Assuming UPLOAD_DIRECTORY is served under '/static/proofs/'
    static_path = os.path.join("/static/proofs", unique_filename)
    logger.info(f"Saved uploaded file {upload_file.filename} to {full_file_path}, storing path {static_path}")
    return static_path


# app/api/v1/endpoints/users.py
@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user with proofs",
    description="Creates a new user account with the provided details and uploads identity/address proofs."
)
async def register_user_with_proofs(
    name: str = Form(...),
    email: str = Form(...),
    phone_number: str = Form(..., max_length=20),
    password: str = Form(..., min_length=8),
    city: str = Form(...),
    state: str = Form(...),
    pincode: int = Form(...),
    user_type: str = Form(..., max_length=100),
    security_question: Optional[str] = Form(None),
    fund_manager: Optional[str] = Form(None),
    is_self_trading: Optional[int] = Form(1),
    group_name: Optional[str] = Form(None),
    bank_ifsc_code: Optional[str] = Form(None),
    bank_holder_name: Optional[str] = Form(None),
    bank_branch_name: Optional[str] = Form(None),
    bank_account_number: Optional[str] = Form(None),
    id_proof: Optional[str] = Form(None),
    address_proof: Optional[str] = Form(None),
    id_proof_image: Optional[UploadFile] = File(None),
    address_proof_image: Optional[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db)
):
    # 🚫 New uniqueness check (email, phone, user_type)
    existing_user = await crud_user.get_user_by_email_phone_type(db, email=email, phone_number=phone_number, user_type=user_type)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email and phone number already exists for this user type."
        )

    # Save uploaded files
    id_proof_image_path = await save_upload_file(id_proof_image)
    address_proof_image_path = await save_upload_file(address_proof_image)

    user_data = {
        "name": name,
        "email": email,
        "phone_number": phone_number,
        "city": city,
        "state": state,
        "pincode": pincode,
        "user_type": user_type,
        "security_question": security_question,
        "fund_manager": fund_manager,
        "is_self_trading": is_self_trading,
        "group_name": group_name,
        "bank_ifsc_code": bank_ifsc_code,
        "bank_holder_name": bank_holder_name,
        "bank_branch_name": bank_branch_name,
        "bank_account_number": bank_account_number,
        "wallet_balance": 0.0,
        "leverage": 1.0,
        "margin": 0.0,
        "status": 0,
        "isActive": 0,
        "account_number": await generate_unique_account_number(db),
    }

    hashed_password = get_password_hash(password)

    try:
        new_user = await crud_user.create_user(
            db=db,
            user_data=user_data,
            hashed_password=hashed_password,
            id_proof_path=id_proof,
            id_proof_image_path=id_proof_image_path,
            address_proof_path=address_proof,
            address_proof_image_path=address_proof_image_path
        )
        return new_user

    except IntegrityError:
        await db.rollback()
        if id_proof_image_path: os.remove(id_proof_image_path)
        if address_proof_image_path: os.remove(address_proof_image_path)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or phone number already exists."
        )

    except Exception as e:
        await db.rollback()
        if id_proof_image_path: os.remove(id_proof_image_path)
        if address_proof_image_path: os.remove(address_proof_image_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration."
        )
    
#
@router.post("/login", response_model=Token, summary="User Login (JSON)")
async def login_with_user_type(
    credentials: UserLogin,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    user = await crud_user.get_user_by_email_and_type(db, email=credentials.username, user_type=credentials.user_type)
    if not user:
        user = await crud_user.get_user_by_phone_and_type(db, phone_number=credentials.username, user_type=credentials.user_type)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username, user type, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if getattr(user, 'isActive', 0) != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is not active or verified."
        )

    access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = datetime.timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={"sub": str(user.id)},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id)},
        expires_delta=refresh_token_expires
    )

    await store_refresh_token(client=redis_client, user_id=user.id, refresh_token=refresh_token)

    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")


@router.post("/refresh-token", response_model=Token, summary="Refresh Access Token")
async def refresh_access_token(
    token_refresh: TokenRefresh,
    db: AsyncSession = Depends(get_db), # Assuming you need db here, though not used in logic below
    redis_client: Redis = Depends(get_redis_client) # <--- ADDED DEPENDENCY
):
    """
    Refreshes an access token using a valid refresh token.
    """
    try:
        # Decode the refresh token to get the user ID
        payload = decode_token(token_refresh.refresh_token)
        # Use get() with None default for safer access
        user_id_from_payload_str: Optional[str] = payload.get("sub")
        if user_id_from_payload_str is None:
            logger.warning("Refresh token payload missing 'sub' claim.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Convert to int for comparison or DB lookup later if needed
        # Although comparing as strings is often safer if they were stored as strings
        # user_id_from_payload = int(user_id_from_payload_str)


        logger.info(f"Refresh token decoded. User ID from token payload: {user_id_from_payload_str}")

        # Check if the refresh token exists and is valid in Redis, passing the redis_client
        # Ensure get_refresh_token_data is imported from app.core.security
        redis_token_data = await get_refresh_token_data(client=redis_client, refresh_token=token_refresh.refresh_token) # <--- MODIFIED CALL

        # Add debugging log before the comparison
        if redis_token_data:
             logger.debug(f"Comparing Redis user_id (type: {type(redis_token_data.get('user_id'))}, value: {redis_token_data.get('user_id')}) with decoded user_id (type: {type(user_id_from_payload_str)}, value: {user_id_from_payload_str})")
        else:
             logger.debug("redis_token_data is None. Cannot perform user_id comparison.")


        # Check if redis_token_data is not None AND the user ID from Redis matches the user ID from the token payload
        # Ensure consistent type comparison (both strings or both ints)
        # Assuming user_id is stored as an int in Redis by store_refresh_token
        if not redis_token_data or (redis_token_data and str(redis_token_data.get("user_id")) != user_id_from_payload_str):
             logger.warning(f"Refresh token validation failed for user ID {user_id_from_payload_str}. Data found: {bool(redis_token_data)}, User ID match: {str(redis_token_data.get('user_id')) == user_id_from_payload_str if redis_token_data else False}")
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(f"Refresh token validated successfully for user ID: {user_id_from_payload_str}")

        # Fetch the user to ensure they still exist and are active
        # Use the ID from the payload, converted to int
        user = await crud_user.get_user_by_id(db, user_id=int(user_id_from_payload_str)) # Convert to int for DB lookup

        if user is None or getattr(user, 'isActive', 0) != 1: # Use getattr for safety
             logger.warning(f"Refresh token valid, but user ID {user_id_from_payload_str} not found or inactive.")
             # Invalidate the refresh token in Redis if the user is no longer valid
             try:
                 await delete_refresh_token(client=redis_client, refresh_token=token_refresh.refresh_token)
                 logger.info(f"Invalidated refresh token for invalid user ID {user_id_from_payload_str}.")
             except Exception as delete_e:
                 logger.error(f"Error deleting invalid refresh token for user ID {user_id_from_payload_str}: {delete_e}", exc_info=True)
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Create a new access token
        access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        new_access_token = create_access_token(
            data={"sub": str(user.id)}, # Ensure user.id is converted to string
            expires_delta=access_token_expires
        )

        logger.info(f"New access token generated for user ID {user.id} using refresh token.")
        # Return the new access token along with the original refresh token
        return Token(access_token=new_access_token, refresh_token=token_refresh.refresh_token, token_type="bearer")

    except JWTError:
        logger.warning("JWTError during refresh token validation.", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error(f"Unexpected error during refresh token process for token: {token_refresh.refresh_token[:20]}... : {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while refreshing token."
        )


@router.post("/logout", response_model=StatusResponse, summary="User Logout")
async def logout_user(
    token_refresh: TokenRefresh, # Expect the refresh token in the request body
    current_user: User = Depends(get_current_user), # Requires a valid access token to identify the user
    redis_client: Redis = Depends(get_redis_client) # <--- ADDED DEPENDENCY
):
    """
    Logs out a user by invalidating their refresh token in Redis.
    Requires a valid access token for the user to be authenticated
    and the refresh token to be provided in the request body.
    """
    try:
        # Delete the refresh token from Redis, passing the redis_client
        # Ensure delete_refresh_token is imported from app.core.security
        await delete_refresh_token(client=redis_client, refresh_token=token_refresh.refresh_token) # <--- MODIFIED CALL
        logger.info(f"Logout successful for user ID {current_user.id} by invalidating refresh token.")
        return StatusResponse(message="Logout successful.")
    except Exception as e:
        logger.error(f"Error during logout for user ID {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during logout."
        )

# Endpoint to get all users (Admin Only)
@router.get(
    "/users",
    response_model=List[UserResponse],
    summary="Get all users (Admin Only)",
    description="Retrieves a list of all registered users (requires admin authentication)."
)
async def read_users(
    skip: int = Query(0, description="Number of users to skip"),
    limit: int = Query(100, description="Maximum number of users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Use the admin dependency from security.py
):
    """
    Retrieves a paginated list of all users. Restricted to administrators.
    """
    # Admin check is now handled by Depends(get_current_admin_user)

    users = await crud_user.get_all_users(db, skip=skip, limit=limit)
    return users

# Endpoint to get all demo users (Admin Only)
@router.get(
    "/users/demo",
    response_model=List[UserResponse],
    summary="Get all demo users (Admin Only)",
    description="Retrieves a list of all registered demo users (requires admin authentication)."
)
async def read_demo_users(
    skip: int = Query(0, description="Number of demo users to skip"),
    limit: int = Query(100, description="Maximum number of demo users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Use the admin dependency
):
    """
    Retrieves a paginated list of all demo users. Restricted to administrators.
    """
     # Admin check is now handled by Depends(get_current_admin_user)

    users = await crud_user.get_demo_users(db, skip=skip, limit=limit)
    return users

# Endpoint to get all live users (Admin Only)
@router.get(
    "/users/live",
    response_model=List[UserResponse],
    summary="Get all live users (Admin Only)",
    description="Retrieves a list of all registered live users (requires admin authentication)."
)
async def read_live_users(
    skip: int = Query(0, description="Number of live users to skip"),
    limit: int = Query(100, description="Maximum number of live users to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Use the admin dependency
):
    """
    Retrieves a paginated list of all live users. Restricted to administrators.
    """
    # Admin check is now handled by Depends(get_current_admin_user)

    users = await crud_user.get_live_users(db, skip=skip, limit=limit)
    return users


@router.get("/me", response_model=UserResponse, summary="Get current user details")
async def read_users_me(current_user: User = Depends(get_current_user)):
    """
    Retrieves the details of the currently authenticated user.
    """
    # This endpoint is accessible to any authenticated user
    return current_user

# Endpoint to update a user by ID (Admin Only)
@router.patch(
    "/users/{user_id}", # Using PATCH for partial updates
    response_model=UserResponse,
    summary="Update a user by ID (Admin Only)",
    description="Updates the details of a specific user by ID (requires admin authentication)."
)
async def update_user_by_id(
    user_id: int, # User ID from the path
    user_update: UserUpdate, # Update data from the request body
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Use the admin dependency
):
    """
    Updates a user's information. Includes database locking if sensitive fields are updated. Restricted to administrators.
    """
    # Admin check is now handled by Depends(get_current_admin_user)


    # Determine if sensitive financial fields are being updated to apply locking
    # Use model_dump for Pydantic V2+ to get a dictionary of fields that are actually set in the update model
    update_data_dict = user_update.model_dump(exclude_unset=True)
    sensitive_fields = ["wallet_balance", "leverage", "margin", "group_name", "status", "isActive"] # Add more if needed
    needs_locking = any(field in update_data_dict for field in sensitive_fields)

    # Fetch the user, applying a lock if necessary
    if needs_locking:
        # Assuming you have this CRUD function for row-level locking
        db_user = await crud_user.get_user_by_id_with_lock(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        logger.info(f"Acquired lock for user ID {user_id} during update.")
    else:
        # Use the standard CRUD function
        db_user = await crud_user.get_user_by_id(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )


    try:
        # Update the user using the CRUD function
        updated_user = await crud_user.update_user(db=db, db_user=db_user, user_update=user_update)

        # TODO: If group_name, leverage, wallet_balance, status, or isActive change,
        # you *might* need to update the user's cache entry in Redis if it's currently active.
        # This depends on how critical it is for changes to apply immediately to the WS feed.
        # await set_user_data_cache(redis_client, user_id, {'group_name': updated_user.group_name, 'leverage': updated_user.leverage, ...}) # Requires redis_client dependency here
        # This endpoint would need the redis_client dependency if it updates cache.

        logger.info(f"User ID {user_id} updated successfully by admin {current_user.id}.")
        return updated_user

    except Exception as e:
        await db.rollback() # Rollback on any error during update
        logger.error(f"Error updating user ID {user_id} by admin {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating the user."
        )

# Endpoint to delete a user by ID (Admin Only)
@router.delete(
    "/users/{user_id}",
    response_model=StatusResponse,
    summary="Delete a user by ID (Admin Only)",
    description="Deletes a specific user by ID (requires admin authentication)."
)
async def delete_user_by_id(
    user_id: int, # User ID from the path
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Use the admin dependency
):
    """
    Deletes a user account. Restricted to administrators.
    """
    # Admin check is now handled by Depends(get_current_admin_user)

    # Fetch the user to delete
    db_user = await crud_user.get_user_by_id(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    try:
        # Delete the user using the CRUD function
        await crud_user.delete_user(db=db, db_user=db_user)

        # TODO: If the user was connected via WebSocket, their connection will eventually break.
        # You might need a mechanism to explicitly remove them from active_websocket_connections
        # and potentially clean up their cache entries if they won't reconnect.
        # This is complex in a distributed system.

        logger.info(f"User ID {user_id} deleted successfully by admin {current_user.id}.")
        return StatusResponse(message=f"User with ID {user_id} deleted successfully.")

    except Exception as e:
        await db.rollback() # Rollback on any error during delete
        logger.error(f"Error deleting user ID {user_id} by admin {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the user."
        )


@router.post(
    "/signup/send-otp",
    response_model=StatusResponse,
    summary="Send OTP for new user email verification by account type",
    # ... (description can be updated)
)
async def signup_send_otp(
    request_data: SignupSendOTPRequest, # Schema now includes user_type
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    # Use the new type-specific check
    user = await crud_user.get_user_by_email_and_type(db, email=request_data.email, user_type=request_data.user_type)

    if user and getattr(user, 'isActive', 0) == 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered and active for this account type."
        )

    otp_code = crud_otp.generate_otp_code()
    email_subject = f"Verify Your Email for {request_data.user_type.capitalize()} Account"
    email_body = f"Your One-Time Password (OTP) for email verification for your {request_data.user_type} account is: {otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."

    if user and getattr(user, 'isActive', 0) == 0:
        # Email and user_type match an existing inactive user
        await crud_otp.create_otp(db, user_id=user.id, force_otp_code=otp_code)
        logger.info(f"Sent OTP to existing inactive user {request_data.email} (type: {request_data.user_type}) for activation.")
    else:
        # New email for this user_type
        redis_key = f"signup_otp:{request_data.email}:{request_data.user_type}" # Include user_type in Redis key
        await redis_client.set(redis_key, otp_code, ex=int(settings.OTP_EXPIRATION_MINUTES * 60))
        logger.info(f"Stored OTP in Redis for new email {request_data.email} (type: {request_data.user_type}).")

    try:
        await email_service.send_email(
            to_email=request_data.email,
            subject=email_subject,
            body=email_body
        )
        logger.info(f"Signup OTP sent successfully to {request_data.email} for account type {request_data.user_type}.")
    except Exception as e:
        logger.error(f"Error sending signup OTP to {request_data.email} for type {request_data.user_type}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )
    return StatusResponse(message="OTP sent successfully to your email.")


@router.post(
    "/signup/verify-otp",
    response_model=StatusResponse,
    summary="Verify OTP for new user email by account type or activate existing",
    # ... (description can be updated)
)
async def signup_verify_otp(
    request_data: SignupVerifyOTPRequest, # Schema now includes user_type
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    redis_key_signup_otp = f"signup_otp:{request_data.email}:{request_data.user_type}" # Include user_type
    stored_otp_in_redis = await redis_client.get(redis_key_signup_otp)

    if stored_otp_in_redis:
        if stored_otp_in_redis == request_data.otp_code:
            await redis_client.delete(redis_key_signup_otp)
            redis_key_preverified = f"preverified_email:{request_data.email}:{request_data.user_type}" # Include user_type
            await redis_client.set(redis_key_preverified, "1", ex=15 * 60)
            logger.info(f"OTP for new email {request_data.email} (type: {request_data.user_type}) verified via Redis.")
            return StatusResponse(message="Email verified successfully. Please complete your registration.")
        else:
            logger.warning(f"Invalid Redis OTP for {request_data.email} (type: {request_data.user_type}).")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
    else:
        user = await crud_user.get_user_by_email_and_type(db, email=request_data.email, user_type=request_data.user_type)
        if user and getattr(user, 'isActive', 0) == 0:
            otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=request_data.otp_code)
            if not otp_record:
                logger.warning(f"Invalid DB OTP for inactive user {request_data.email} (type: {request_data.user_type}).")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")
            user.status = 1
            user.isActive = 1
            try:
                await db.commit()
                await crud_otp.delete_otp(db, otp_id=otp_record.id)
                logger.info(f"Existing inactive user {request_data.email} (type: {request_data.user_type}, ID: {user.id}) activated.")
                return StatusResponse(message="Account activated successfully. You can now login.")
            except Exception as e:
                await db.rollback()
                # ... (error logging and HTTPException) ...
                logger.error(f"Error activating user ID {user.id} (type: {request_data.user_type}): {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An error occurred during account activation.")
        else:
            logger.warning(f"No OTP in Redis and no matching inactive user for {request_data.email} (type: {request_data.user_type}).")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OTP, or email not eligible for this verification process.")
        

# API Endpoints
from app.crud.otp import get_user_by_email_and_type, create_otp, get_valid_otp, get_otp_flag_key, delete_all_user_otps


@router.post("/request-password-reset", response_model=StatusResponse)
async def request_password_reset(
    payload: RequestPasswordReset,
    db: AsyncSession = Depends(get_db),
):
    user = await get_user_by_email_and_type(db, payload.email, payload.user_type)
    if not user:
        return StatusResponse(message="If a user exists, an OTP has been sent.")

    otp = await create_otp(db, user_id=user.id)
    await email_service.send_email(
        user.email,
        "Password Reset OTP",
        f"Your OTP is: {otp.otp_code}"
    )
    return StatusResponse(message="Password reset OTP sent successfully.")

@router.post("/verify-password-reset-otp", response_model=StatusResponse)
async def verify_password_reset_otp(
    payload: VerifyOTPRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user = await get_user_by_email_and_type(db, payload.email, payload.user_type)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials.")

    otp = await get_valid_otp(db, user.id, payload.otp_code)
    if not otp:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")

    await redis.setex(get_otp_flag_key(user.email, user.user_type), settings.OTP_EXPIRATION_MINUTES * 60, "1")
    return StatusResponse(message="OTP verified successfully.")


@router.post("/reset-password-confirm", response_model=StatusResponse)
async def confirm_password_reset(
    payload: ResetPasswordConfirm,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_client)
):
    user = await get_user_by_email_and_type(db, payload.email, payload.user_type)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or user type.")

    redis_key = get_otp_flag_key(user.email, user.user_type)
    otp_verified = await redis.get(redis_key)
    if not otp_verified:
        raise HTTPException(status_code=403, detail="OTP verification required before resetting password.")

    user.hashed_password = get_password_hash(payload.new_password)
    await db.commit()
    await delete_all_user_otps(db, user.id)
    await redis.delete(redis_key)

    return StatusResponse(message="Password reset successful.")


from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal # Import Decimal
from typing import Optional

from app.database.session import get_db
from app.database.models import User
from app.schemas.user import StatusResponse # Assuming StatusResponse is in user schema
from app.schemas.wallet import WalletTransactionRequest, WalletBalanceResponse, WalletCreate # Import new wallet schemas
from app.crud import user as crud_user
from app.crud import wallet as crud_wallet # Import crud_wallet
from app.core.security import get_current_user # For user authentication

import logging

logger = logging.getLogger(__name__)

# Assuming 'router' is already defined as APIRouter() in users.py

# NEW: Endpoint to add funds to user's wallet
@router.post(
    "/wallet/add",
    response_model=WalletBalanceResponse,
    summary="Add funds to user's wallet",
    description="Adds a specified amount to the authenticated user's wallet balance and records the transaction."
)
async def add_funds_to_wallet(
    request: WalletTransactionRequest,
    current_user: User = Depends(get_current_user), # Authenticate user
    db: AsyncSession = Depends(get_db)
):
    """
    Adds funds to the authenticated user's wallet balance.
    Ensures atomicity using transactions and row-level locking.
    """
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to add must be positive."
        )

    try:
        # Use the new CRUD function to update balance and create record
        updated_user = await crud_user.update_user_wallet_balance(
            db=db,
            user_id=current_user.id,
            amount=request.amount,
            transaction_type="deposit",
            description=request.description or "Funds added by user request"
        )

        # Assuming update_user_wallet_balance returns the updated user and handles wallet record creation
        # We need the transaction_id from the created wallet record.
        # This requires a slight modification to update_user_wallet_balance to return it,
        # or fetch the latest record. For simplicity, let's assume update_user_wallet_balance
        # can return the wallet_record as well, or we fetch it.
        # For now, we'll just return a generic success message and the new balance.
        # If transaction_id is critical for the response, update_user_wallet_balance needs to return it.

        # To get the transaction_id, we need to fetch the latest wallet record for the user
        # This is not ideal as it adds another query. A better approach is to return it from the CRUD.
        # Let's modify update_user_wallet_balance to return both user and wallet_record.

        # Re-fetch the latest wallet record to get its transaction_id
        latest_wallet_record = await crud_wallet.get_wallet_records_by_user_id(
            db, user_id=current_user.id, limit=1, skip=0
        )
        transaction_id = latest_wallet_record[0].transaction_id if latest_wallet_record else None


        return WalletBalanceResponse(
            user_id=current_user.id,
            new_balance=updated_user.wallet_balance,
            message=f"Successfully added {request.amount} to wallet.",
            transaction_id=transaction_id
        )

    except ValueError as ve:
        logger.warning(f"Wallet add funds failed for user {current_user.id}: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error adding funds to wallet for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while adding funds."
        )


# --- Wallet Endpoints Modified for Money Request Flow ---
from app.schemas.money_request import MoneyRequestResponse, MoneyRequestCreate
from app.crud import money_request as crud_money_request

@router.post(
    "/wallet/deposit",  # Changed from /wallet/add
    response_model=MoneyRequestResponse, # Returns the created money request
    summary="Request to deposit funds to user's wallet",
    description="Creates a money request for depositing funds. Admin approval is required to update the wallet."
)
async def request_deposit_funds( # Renamed function
    request: WalletTransactionRequest, # Re-using this schema for amount and description
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to deposit must be positive."
        )

    money_request_data = MoneyRequestCreate(
        amount=request.amount,
        type="deposit", # Explicitly set type
        # description can be passed to money_request if schema is updated, or handled differently
    )
    # Note: The current MoneyRequestCreate schema doesn't have a description field.
    # If description is needed for the money request itself, the schema and model should be updated.
    # For now, the description from WalletTransactionRequest is not directly used in MoneyRequest.

    try:
        new_money_request = await crud_money_request.create_money_request(
            db=db,
            request_data=money_request_data,
            user_id=current_user.id
        )
        logger.info(f"Deposit request created: ID {new_money_request.id} for user {current_user.id}, amount {request.amount}")
        # If you want to include the description in the log or somewhere, you can access request.description
        return new_money_request
    except Exception as e:
        logger.error(f"Error creating deposit request for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the deposit request."
        )

@router.post(
    "/wallet/withdraw", # Changed from /wallet/deduct
    response_model=MoneyRequestResponse, # Returns the created money request
    summary="Request to withdraw funds from user's wallet",
    description="Creates a money request for withdrawing funds. Admin approval is required to update the wallet."
)
async def request_withdraw_funds( # Renamed function
    request: WalletTransactionRequest, # Re-using this schema for amount and description
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if request.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount to withdraw must be positive."
        )

    # Optional: Check if user has sufficient balance for withdrawal request here,
    # though final check will be done upon approval.
    # This is a soft check to prevent obviously impossible requests.
    # user_wallet = await crud_user.get_user_by_id(db, current_user.id) # Fetch user to check balance
    # if user_wallet and user_wallet.wallet_balance < request.amount:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail="Insufficient funds to request this withdrawal amount."
    #     )

    money_request_data = MoneyRequestCreate(
        amount=request.amount,
        type="withdraw", # Explicitly set type
    )

    try:
        new_money_request = await crud_money_request.create_money_request(
            db=db,
            request_data=money_request_data,
            user_id=current_user.id
        )
        logger.info(f"Withdrawal request created: ID {new_money_request.id} for user {current_user.id}, amount {request.amount}")
        return new_money_request
    except Exception as e:
        logger.error(f"Error creating withdrawal request for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the withdrawal request."
        )
