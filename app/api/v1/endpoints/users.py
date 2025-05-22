# app/api/v1/endpoints/users.py

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
    RequestPasswordReset,
    ResetPasswordConfirm,
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


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user with proofs",
    description="Creates a new user account with the provided details and uploads identity/address proofs."
)
async def register_user_with_proofs(
    name: str = Form(..., description="Full name of the user."),
    email: str = Form(..., description="User's email address (must be unique)."),
    phone_number: str = Form(..., max_length=20, description="User's phone number (must be unique)."),
    password: str = Form(..., min_length=8, description="User's password (will be hashed)."),
    city: str = Form(..., description="City of the user."),
    state: str = Form(..., description="State of the user."),
    pincode: int = Form(..., description="Pincode of the user's location."),
    user_type: str = Form(..., max_length=100, description="Type of user (e.g., 'trader', 'investor')."),
    security_question: Optional[str] = Form(None, max_length=255, description="Security question for recovery."),
    fund_manager: Optional[str] = Form(None, max_length=255, description="Name of the assigned fund manager."),
    is_self_trading: Optional[int] = Form(1, description="Flag indicating if the user is self-trading (0 or 1). Defaults to 1."),
    group_name: Optional[str] = Form(None, max_length=255, description="Name of the trading group the user belongs to."),
    bank_ifsc_code: Optional[str] = Form(None, max_length=50, description="Bank IFSC code."),
    bank_holder_name: Optional[str] = Form(None, max_length=255, description="Bank account holder name."),
    bank_branch_name: Optional[str] = Form(None, max_length=255, description="Bank branch name."),
    bank_account_number: Optional[str] = Form(None, max_length=100, description="Bank account number."),

    id_proof: Optional[str] = Form(None, description="Type of ID proof (e.g., Aadhaar, Passport)."),
    address_proof: Optional[str] = Form(None, description="Type of address proof (e.g., Utility Bill, Bank Statement)."),

    id_proof_image: Optional[UploadFile] = File(None, description="ID proof image file."),
    address_proof_image: Optional[UploadFile] = File(None, description="Address proof image file."),

    db: AsyncSession = Depends(get_db)
):
    """
    Handles new user registration with file uploads for proofs.
    """
    existing_user_email = await crud_user.get_user_by_email(db, email=email)
    if existing_user_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    existing_user_phone = await crud_user.get_user_by_phone_number(db, phone_number=phone_number)
    if existing_user_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phone number already registered"
        )

    # Save uploaded files - errors are now raised in save_upload_file
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
         "wallet_balance": 0.0, # Initialize wallet balance
         "leverage": 1.0, # Initialize leverage
         "margin": 0.0, # Initialize margin
         "status": 0, # Initialize status (e.g., 0 for pending verification)
         "isActive": 0, # Initialize isActive (e.g., 0 for inactive)
         "account_number": await generate_unique_account_number(db),  # New unique 5-char alphanumeric
    }

    hashed_password = get_password_hash(password)

    # Try to create the user in the database
    try:
        new_user = await crud_user.create_user(
            db=db,
            user_data=user_data,
            hashed_password=hashed_password,
            id_proof_path=id_proof, # Pass the string type
            id_proof_image_path=id_proof_image_path, # Pass the file path
            address_proof_path=address_proof, # Pass the string type
            address_proof_image_path=address_proof_image_path # Pass the file path
        )
        logger.info(f"New user registered successfully with ID: {new_user.id}, email: {new_user.email}")
        return new_user

    except IntegrityError:
        await db.rollback() # Rollback the transaction on integrity error
        # Clean up uploaded files if DB creation fails due to integrity error
        if id_proof_image_path and os.path.exists(os.path.join(UPLOAD_DIRECTORY, os.path.basename(id_proof_image_path))):
             os.remove(os.path.join(UPLOAD_DIRECTORY, os.path.basename(id_proof_image_path)))
        if address_proof_image_path and os.path.exists(os.path.join(UPLOAD_DIRECTORY, os.path.basename(address_proof_image_path))):
             os.remove(os.path.join(UPLOAD_DIRECTORY, os.path.basename(address_proof_image_path)))
        logger.error(f"Integrity error during user registration for email {email} or phone {phone_number}.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or phone number already exists."
        )
    except Exception as e:
        await db.rollback() # Rollback on any other error during DB operation
        # Clean up uploaded files on other errors
        if id_proof_image_path and os.path.exists(os.path.join(UPLOAD_DIRECTORY, os.path.basename(id_proof_image_path))):
             os.remove(os.path.join(UPLOAD_DIRECTORY, os.path.basename(id_proof_image_path)))
        if address_proof_image_path and os.path.exists(os.path.join(UPLOAD_DIRECTORY, os.path.basename(address_proof_image_path))):
             os.remove(os.path.join(UPLOAD_DIRECTORY, os.path.basename(address_proof_image_path)))
        logger.error(f"Unexpected error during user registration: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration."
        )

@router.post(
    "/send-otp",
    response_model=StatusResponse,
    summary="Send OTP for email verification or password reset",
    description="Generates and sends a one-time password (OTP) to the user's email for verification or password reset."
)
async def send_otp_for_verification(
    otp_request: SendOTPRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Generates and sends an OTP to the user's email for initial verification or password reset.
    """
    user = await crud_user.get_user_by_email(db, email=otp_request.email)
    if not user:
        logger.warning(f"Attempted to send OTP to non-existent email: {otp_request.email}")
        # Return a generic success message even if user not found for security
        return StatusResponse(message="If a user with that email exists, an OTP has been sent.")

    try:
        # Create a new OTP for the user (reusing the create_otp function)
        otp_record = await crud_otp.create_otp(db, user_id=user.id)

        # Determine if this is for initial verification or password reset
        subject = "Your Trading App OTP"
        body = f"Your One-Time Password (OTP) is: {otp_record.otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes.\n\nUse this OTP for email verification or password reset." # Generic body


        await email_service.send_email(
            to_email=user.email,
            subject=subject,
            body=body
        )
        logger.info(f"OTP sent successfully to {user.email}.")

        return StatusResponse(message="OTP sent successfully.")

    except Exception as e:
        # Log the error and return a generic error response
        logger.error(f"Error sending OTP to {user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )


@router.post(
    "/verify-otp",
    response_model=StatusResponse,
    summary="Verify OTP for email verification",
    description="Verifies the provided OTP code against the one sent to the user's email and activates the user account."
)
async def verify_user_otp(
    verify_request: VerifyOTPRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Verifies the user-provided OTP for initial email verification and activates the user.
    """
    # Find the user by email
    user = await crud_user.get_user_by_email(db, email=verify_request.email)
    if not user:
        # Return a generic error message for security
        logger.warning(f"Verification attempt for non-existent email: {verify_request.email}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email or OTP."
        )

    # Get the valid OTP record for the user
    otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=verify_request.otp_code)

    if not otp_record:
        # If no valid OTP found (either incorrect or expired)
        logger.warning(f"Invalid or expired OTP provided for email: {verify_request.email}")
        # Consider adding rate limiting here to prevent brute-force attacks
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP."
        )

    # If OTP is valid, set user status to active
    user.status = 1 # Assuming status 1 means verified/active
    user.isActive = 1 # Assuming isActive 1 means active
    try:
        await db.commit() # Commit the status update
        # await db.refresh(user) # Not needed unless you return the user object

        # Delete the used OTP record
        await crud_otp.delete_otp(db, otp_id=otp_record.id)
        logger.info(f"Email verified and user account activated successfully for user ID: {user.id}")

    except Exception as e:
        await db.rollback() # Rollback on error during DB operations
        logger.error(f"Error activating user ID {user.id} after OTP verification: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during verification."
        )

    return StatusResponse(message="Email verified and account activated successfully.")

# NOTE: The /request-password-reset and /reset-password endpoints
# are already correctly implemented in your original file using crud_otp
# for fetching/validating OTPs for password reset.

# --- Authentication Endpoints ---

@router.post("/login", response_model=Token, summary="User Login")
async def login_for_access_tokens(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client) # <--- ADDED DEPENDENCY
):
    """
    Authenticates a user and returns JWT access and refresh tokens.
    Uses OAuth2PasswordRequestForm for standard form-data login.
    """
    # Attempt to find the user by email or phone number
    user = await crud_user.get_user_by_email(db, email=form_data.username)
    if not user:
        # If not found by email, try phone number
        user = await crud_user.get_user_by_phone_number(db, phone_number=form_data.username)

    # If user is still not found
    if not user:
        logger.warning(f"Login attempt failed for username: {form_data.username} - User not found.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email, phone number, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify password
    if not verify_password(form_data.password, user.hashed_password):
        logger.warning(f"Login attempt failed for username: {form_data.username} - Incorrect password.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email, phone number, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if the user account is active (assuming 1 means active)
    # Use getattr for safety if 'isActive' might be missing or None
    if getattr(user, 'isActive', 0) != 1:
        logger.warning(f"Login attempt failed for username: {form_data.username} - Account not active.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is not active or verified. Please verify your email."
        )

    # Generate tokens
    # Ensure settings are correctly loaded and have these attributes
    access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = datetime.timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)

    access_token = create_access_token(
        data={"sub": str(user.id)}, # Ensure user.id is converted to string
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id)}, # Ensure user.id is converted to string
        expires_delta=refresh_token_expires
    )

    # Store the refresh token in Redis, passing the redis_client
    # Ensure store_refresh_token is imported from app.core.security
    try:
        await store_refresh_token(client=redis_client, user_id=user.id, refresh_token=refresh_token) # <--- MODIFIED CALL
        logger.info(f"Login successful for user ID {user.id}. Tokens generated and refresh token stored in Redis.")
    except Exception as e:
        logger.error(f"Failed to store refresh token for user ID {user.id} in Redis: {e}", exc_info=True)
        # Decide if login should fail if refresh token storage fails.
        # For robustness, you might still return tokens but log the error.
        # For now, we'll log and continue to allow login.

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

# You might also need endpoints for:
# - User profile view (for the user themselves) - '/me' covers this
# - User profile update (for the user themselves) - You could adapt update_user_by_id or create a new endpoint
# - Handling referrals (if 'referred_by_id' and 'reffered_code' are used)
# - Endpoints related to bank details management (if not covered by update)
# - Endpoints for managing user status (approval workflow) if needed
# - Endpoints related to UserOrder or Wallet (trading history, deposit/withdrawal requests)