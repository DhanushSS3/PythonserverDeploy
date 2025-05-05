# app/api/v1/endpoints/users.py

from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, Form, Query
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import datetime
import shutil
import os
import uuid
from typing import Optional, List
import logging

# Import select for database queries
from sqlalchemy.future import select
# Import JWTError for exception handling
from jose import JWTError

from app.database.session import get_db
from app.database.models import User
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
from app.services import email as email_service
from app.core.security import (
    get_password_hash,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    store_refresh_token,
    get_refresh_token_data,
    delete_refresh_token
)
from app.core.config import get_settings

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
async def save_upload_file(upload_file: UploadFile) -> str:
    """
    Saves an uploaded file to the UPLOAD_DIRECTORY with a unique filename.

    Args:
        upload_file: The UploadFile object.

    Returns:
        The path to the saved file relative to the project root.
    """
    # Generate a unique filename while preserving the original extension
    file_extension = os.path.splitext(upload_file.filename)[1]
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    file_path = os.path.join(UPLOAD_DIRECTORY, unique_filename)

    # Save the file asynchronously
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
    finally:
        await upload_file.close()

    return file_path

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

    id_proof_image_path = await save_upload_file(id_proof_image) if id_proof_image else None
    address_proof_image_path = await save_upload_file(address_proof_image) if address_proof_image else None

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
        if id_proof_image_path and os.path.exists(id_proof_image_path): os.remove(id_proof_image_path)
        if address_proof_image_path and os.path.exists(address_proof_image_path): os.remove(address_proof_image_path)
        logger.error(f"Error during user registration with file upload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or phone number already exists."
        )
    except Exception as e:
        await db.rollback()
        if id_proof_image_path and os.path.exists(id_proof_image_path): os.remove(id_proof_image_path)
        if address_proof_image_path and os.path.exists(address_proof_image_path): os.remove(address_proof_image_path)
        logger.error(f"Error during user registration with file upload: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration."
        )

@router.post(
    "/send-otp",
    response_model=StatusResponse,
    summary="Send OTP for email verification",
    description="Generates and sends a one-time password (OTP) to the user's email for verification."
)
async def send_otp_for_verification(
    otp_request: SendOTPRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Generates and sends an OTP to the user's email for initial verification.
    """
    user = await crud_user.get_user_by_email(db, email=otp_request.email)
    if not user:
        logger.warning(f"Attempted to send OTP to non-existent email: {otp_request.email}")
        return StatusResponse(message="If a user with that email exists, an OTP has been sent.")

    try:
        # Create a new OTP for the user (reusing the create_otp function)
        otp_record = await crud_otp.create_otp(db, user_id=user.id)

        # Send the email with the password reset OTP
        subject = "Your Trading App Password Reset OTP"
        body = f"Your One-Time Password (OTP) for password reset is: {otp_record.otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."
        await email_service.send_email(
            to_email=user.email,
            subject=subject,
            body=body
        )

        return StatusResponse(message="Password reset OTP sent successfully.")

    except Exception as e:
        # Log the error and return a generic error response
        logger.error(f"Error sending password reset OTP to {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send password reset OTP. Please try again later."
        )

@router.post(
    "/verify-otp",
    response_model=StatusResponse,
    summary="Verify OTP for email verification",
    description="Verifies the provided OTP code against the one sent to the user's email."
)
async def verify_user_otp(
    verify_request: VerifyOTPRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Verifies the user-provided OTP for initial email verification.
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP."
        )

    # If OTP is valid, set user status to active
    user.status = 1
    user.isActive = 1
    await db.commit() # Commit the status update
    # await db.refresh(user) # No need to refresh if not returning the user object

    # Delete the used OTP record
    await crud_otp.delete_otp(db, otp_id=otp_record.id)
    logger.info(f"Email verified successfully for user ID: {user.id}")

    return StatusResponse(message="Email verified successfully.")

@router.post(
    "/request-password-reset",
    response_model=StatusResponse,
    summary="Request password reset OTP",
    description="Sends a one-time password (OTP) to the user's email for password reset."
)
async def request_password_reset(
    reset_request: RequestPasswordReset,
    db: AsyncSession = Depends(get_db)
):
    """
    Handles the request to initiate a password reset by sending an OTP.
    """
    user = await crud_user.get_user_by_email(db, email=reset_request.email)
    if not user:
        logger.warning(f"Attempted password reset request for non-existent email: {reset_request.email}")
        return StatusResponse(message="If a user with that email exists, a password reset OTP has been sent.")

    try:
        otp_record = await crud_otp.create_otp(db, user_id=user.id)
        subject = "Your Trading App Password Reset OTP"
        body = f"Your One-Time Password (OTP) for password reset is: {otp_record.otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."
        await email_service.send_email(
            to_email=user.email,
            subject=subject,
            body=body
        )
        logger.info(f"Password reset OTP sent successfully to {user.email}.")
        return StatusResponse(message="Password reset OTP sent successfully.")
    except Exception as e:
        logger.error(f"Error sending password reset OTP to {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send password reset OTP. Please try again later."
        )

@router.post(
    "/reset-password",
    response_model=StatusResponse,
    summary="Reset password with OTP",
    description="Verifies the OTP and sets a new password for the user."
)
async def reset_password_confirm(
    reset_confirm: ResetPasswordConfirm,
    db: AsyncSession = Depends(get_db)
):
    """
    Handles the password reset confirmation using OTP.
    """
    user = await crud_user.get_user_by_email(db, email=reset_confirm.email)
    if not user:
        logger.warning(f"Password reset confirmation attempt for non-existent email: {reset_confirm.email}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email or OTP."
        )

    otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=reset_confirm.otp_code)

    if not otp_record:
        logger.warning(f"Invalid or expired OTP provided for password reset for email: {reset_confirm.email}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP."
        )

    hashed_password = get_password_hash(reset_confirm.new_password)
    user.hashed_password = hashed_password
    await db.commit()
    await crud_otp.delete_otp(db, otp_id=otp_record.id)
    logger.info(f"Password reset successfully for user ID: {user.id}")

    return StatusResponse(message="Password reset successfully.")

# --- Authentication Endpoints ---

@router.post("/login", response_model=Token, summary="User Login")
async def login_for_access_tokens(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db)
):
    """
    Authenticates a user and returns JWT access and refresh tokens.
    Uses OAuth2PasswordRequestForm for standard form-data login.
    """
    user = await crud_user.get_user_by_email(db, email=form_data.username)
    if not user:
        user = await crud_user.get_user_by_phone_number(db, phone_number=form_data.username)

    if not user:
        logger.warning(f"Login attempt failed for username: {form_data.username} - User not found.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email, phone number, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(form_data.password, user.hashed_password):
        logger.warning(f"Login attempt failed for username: {form_data.username} - Incorrect password.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email, phone number, or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if user.isActive != 1:
        logger.warning(f"Login attempt failed for username: {form_data.username} - Account not active.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is not active or verified. Please verify your email."
        )

    access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = datetime.timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)

    access_token = create_access_token(
        data={"sub": str(user.id)},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id)},
        expires_delta=refresh_token_expires
    )

    await store_refresh_token(user_id=user.id, refresh_token=refresh_token)
    logger.info(f"Login successful for user ID {user.id}. Tokens generated and refresh token stored.")

    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")

@router.post("/refresh-token", response_model=Token, summary="Refresh Access Token")
async def refresh_access_token(
    token_refresh: TokenRefresh,
    db: AsyncSession = Depends(get_db)
):
    """
    Refreshes an access token using a valid refresh token.
    """
    try:
        # Decode the refresh token to get the user ID
        payload = decode_token(token_refresh.refresh_token)
        user_id_from_payload: int = payload.get("sub")
        if user_id_from_payload is None:
            logger.warning("Refresh token payload missing 'sub' claim.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(f"Refresh token decoded. User ID from token payload: {user_id_from_payload}")

        # Check if the refresh token exists and is valid in Redis
        redis_token_data = await get_refresh_token_data(token_refresh.refresh_token)

        # Add debugging log before the comparison
        if redis_token_data:
             logger.debug(f"Comparing Redis user_id (type: {type(redis_token_data.get('user_id'))}, value: {redis_token_data.get('user_id')}) with decoded user_id (type: {type(user_id_from_payload)}, value: {user_id_from_payload})")
        else:
             logger.debug("redis_token_data is None. Cannot perform user_id comparison.")


        # Check if redis_token_data is not None AND the user ID from Redis matches the user ID from the token payload
        if not redis_token_data or (redis_token_data and str(redis_token_data.get("user_id")) != str(user_id_from_payload)):
             logger.warning(f"Refresh token validation failed for user ID {user_id_from_payload}. Data found: {bool(redis_token_data)}, User ID match: {str(redis_token_data.get('user_id')) == str(user_id_from_payload) if redis_token_data else False}")
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(f"Refresh token validated successfully for user ID: {user_id_from_payload}")

        # Fetch the user to ensure they still exist and are active
        result = await db.execute(select(User).filter(User.id == user_id_from_payload))
        user = result.scalars().first()

        if user is None or user.isActive != 1:
             logger.warning(f"Refresh token valid, but user ID {user_id_from_payload} not found or inactive.")
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Create a new access token
        access_token_expires = datetime.timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        new_access_token = create_access_token(
            data={"sub": str(user.id)},
            expires_delta=access_token_expires
        )

        logger.info(f"New access token generated for user ID {user.id} using refresh token.")
        return Token(access_token=new_access_token, refresh_token=token_refresh.refresh_token, token_type="bearer")

    except JWTError:
        logger.warning("JWTError during refresh token validation.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error(f"Unexpected error during refresh token process for token: {token_refresh.refresh_token[:20]}... : {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while refreshing token."
        )


@router.post("/logout", response_model=StatusResponse, summary="User Logout")
async def logout_user(
    token_refresh: TokenRefresh,
    current_user: User = Depends(get_current_user)
):
    """
    Logs out a user by invalidating their refresh token in Redis.
    Requires a valid access token for the user to be authenticated.
    """
    try:
        await delete_refresh_token(token_refresh.refresh_token)
        logger.info(f"Logout successful for user ID {current_user.id}.")
        return StatusResponse(message="Logout successful.")
    except Exception as e:
        logger.error(f"Error during logout for user ID {current_user.id}: {e}")
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
    current_user: User = Depends(get_current_user) # Ensure user is authenticated
):
    """
    Retrieves a paginated list of all users. Restricted to administrators.
    """
    # --- Admin Authorization Check ---
    # Assuming 'admin' is a specific value in the user_type field for administrators
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to access /users without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource. Admin privileges required."
        )
    # --- End Admin Authorization Check ---

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
    current_user: User = Depends(get_current_user) # Ensure user is authenticated
):
    """
    Retrieves a paginated list of all demo users. Restricted to administrators.
    """
    # --- Admin Authorization Check ---
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to access /users/demo without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource. Admin privileges required."
        )
    # --- End Admin Authorization Check ---

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
    current_user: User = Depends(get_current_user) # Ensure user is authenticated
):
    """
    Retrieves a paginated list of all live users. Restricted to administrators.
    """
    # --- Admin Authorization Check ---
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to access /users/live without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource. Admin privileges required."
        )
    # --- End Admin Authorization Check ---

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
    current_user: User = Depends(get_current_user) # Ensure user is authenticated
):
    """
    Updates a user's information. Includes database locking if sensitive fields are updated. Restricted to administrators.
    """
    # --- Admin Authorization Check ---
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to update user ID {user_id} without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to perform this action. Admin privileges required."
        )
    # --- End Admin Authorization Check ---


    # Determine if sensitive financial fields are being updated to apply locking
    update_data_dict = user_update.model_dump(exclude_unset=True)
    sensitive_fields = ["wallet_balance", "leverage", "margin"]
    needs_locking = any(field in update_data_dict for field in sensitive_fields)

    # Fetch the user, applying a lock if necessary
    if needs_locking:
        db_user = await crud_user.get_user_by_id_with_lock(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        logger.info(f"Acquired lock for user ID {user_id} during update.")
    else:
        db_user = await crud_user.get_user_by_id(db, user_id=user_id)
        if db_user is None:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )


    try:
        updated_user = await crud_user.update_user(db=db, db_user=db_user, user_update=user_update)
        logger.info(f"User ID {user_id} updated successfully by admin {current_user.id}.")
        return updated_user

    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating user ID {user_id} by admin {current_user.id}: {e}")
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
    current_user: User = Depends(get_current_user) # Ensure user is authenticated
):
    """
    Deletes a user account. Restricted to administrators.
    """
    # --- Admin Authorization Check ---
    if current_user.user_type != 'admin':
        logger.warning(f"User ID {current_user.id} attempted to delete user ID {user_id} without admin privileges.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to perform this action. Admin privileges required."
        )
    # --- End Admin Authorization Check ---

    # Fetch the user to delete
    db_user = await crud_user.get_user_by_id(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    try:
        await crud_user.delete_user(db=db, db_user=db_user)
        logger.info(f"User ID {user_id} deleted successfully by admin {current_user.id}.")
        return StatusResponse(message=f"User with ID {user_id} deleted successfully.")

    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting user ID {user_id} by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the user."
        )
