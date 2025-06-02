# app/api/endpoints/live_users.py

import os
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession # Import AsyncSession
from sqlalchemy import select, or_ # Import select for async queries

from app.database.session import get_db # get_db now yields AsyncSession
from app.database.models import User
from app.schemas.live_user import (
    UserCreate, UserResponse, UserLogin, UserLoginResponse,
    RefreshTokenRequest, Token, # RefreshTokenRequest will still be defined but not used for this endpoint
    OTPSendRequest, OTPSendResponse, OTPVerifyRequest, OTPVerifyResponse,
    PasswordResetRequest, PasswordResetResponse,
    PasswordResetVerifyRequest, PasswordResetVerifyResponse,
    ResetPasswordConfirmRequest, ResetPasswordConfirmResponse
)
from app.crud.crud_live_user import (
    create_user, authenticate_user, verify_password,
    get_user_by_email_and_type, generate_and_store_otp, verify_user_otp,
    verify_otp_for_password_reset, update_user_password, validate_and_consume_reset_token
)
from app.core.logging_config import user_logger, error_logger
from app.core.security import create_access_token, create_refresh_token, decode_token
from app.services.email import send_email
from app.core.config import get_settings

security = HTTPBearer()

router = APIRouter(
    prefix="/live-users",
    tags=["Live Users"],
    responses={404: {"description": "Not found"}},
)

settings = get_settings()

async def get_current_user(
    db: AsyncSession = Depends(get_db), # Changed to AsyncSession
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """
    Dependency to get the current authenticated user from the JWT token.
    Raises HTTPException if the token is invalid, expired, or user not found.
    """
    token = credentials.credentials
    user_logger.debug(f"Attempting to authenticate user with token: {token[:30]}...")

    payload = decode_token(token)
    if not payload:
        user_logger.warning("Authentication failed: Invalid or expired token.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = payload.get("user_id")
    user_email = payload.get("sub")
    user_type = payload.get("user_type")

    if not user_id or not user_email or not user_type:
        user_logger.warning(f"Authentication failed: Token payload missing required user info. Payload: {payload}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Use async query pattern: await db.execute(select(User).filter(...)).scalar_one_or_none()
    result = await db.execute(
        select(User).filter(User.id == user_id, User.email == user_email, User.user_type == user_type)
    )
    user = result.scalar_one_or_none()

    if not user:
        user_logger.warning(f"Authentication failed: User not found in DB for ID {user_id}, Email {user_email}.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if user.status != 1 or user.isActive != 1:
        user_logger.warning(f"Authentication failed for user {user_email}: Account not active/verified (status={user.status}, isActive={user.isActive}).")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account is not active or verified. Please contact support.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_logger.info(f"User {user_email} (ID: {user.id}) successfully authenticated via JWT.")
    return user

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_live_user(
    name: str,
    phone_number: str,
    email: str,
    password: str,
    city: str,
    state: str,
    pincode: int,
    group: str,
    bank_account_number: str,
    bank_ifsc_code: str,
    bank_holder_name: str,
    bank_branch_name: str,
    security_question: str,
    security_answer: str,
    address_proof: str,
    id_proof: str,
    is_self_trading: bool,
    isActive: bool,
    fund_manager: Optional[str] = None,
    address_proof_image: UploadFile = File(...),
    id_proof_image: UploadFile = File(...),
    referral_code: Optional[str] = None,
    db: AsyncSession = Depends(get_db) # Changed to AsyncSession
):
    """
    Registers a new live user in the system, handling image uploads locally.
    """
    user_logger.info(f"Attempting to register new live user: {email}")

    # Use async query patterns
    existing_user_email_result = await db.execute(select(User).filter(User.email == email))
    existing_user_email = existing_user_email_result.scalar_one_or_none()
    if existing_user_email:
        user_logger.warning(f"Registration failed: Email '{email}' already registered.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email already registered."
        )

    existing_user_phone_result = await db.execute(select(User).filter(User.phone_number == phone_number))
    existing_user_phone = existing_user_phone_result.scalar_one_or_none()
    if existing_user_phone:
        user_logger.warning(f"Registration failed: Phone number '{phone_number}' already registered.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this phone number already registered."
        )

    # --- Handle Image Uploads (remains largely the same as it's file I/O) ---
    address_image_filename = None
    if address_proof_image:
        file_extension = os.path.splitext(address_proof_image.filename)[1] if address_proof_image.filename else ".bin"
        address_image_filename = f"{uuid.uuid4()}{file_extension}"
        full_address_path = os.path.join(settings.LOCAL_IMAGE_STORAGE_PATH, address_image_filename)
        try:
            os.makedirs(settings.LOCAL_IMAGE_STORAGE_PATH, exist_ok=True)
            with open(full_address_path, "wb") as buffer:
                buffer.write(await address_proof_image.read())
            user_logger.info(f"Address proof image saved: {full_address_path}")
        except Exception as e:
            error_logger.error(f"Failed to save address proof image: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload address proof image: {e}"
            )

    id_image_filename = None
    if id_proof_image:
        file_extension = os.path.splitext(id_proof_image.filename)[1] if id_proof_image.filename else ".bin"
        id_image_filename = f"{uuid.uuid4()}{file_extension}"
        full_id_path = os.path.join(settings.LOCAL_IMAGE_STORAGE_PATH, id_image_filename)
        try:
            os.makedirs(settings.LOCAL_IMAGE_STORAGE_PATH, exist_ok=True)
            with open(full_id_path, "wb") as buffer:
                buffer.write(await id_proof_image.read())
            user_logger.info(f"ID proof image saved: {full_id_path}")
        except Exception as e:
            error_logger.error(f"Failed to save ID proof image: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload ID proof image: {e}"
            )

    user_create_data = UserCreate(
        name=name, phone_number=phone_number, email=email, password=password,
        city=city, state=state, pincode=pincode, group=group,
        bank_account_number=bank_account_number, bank_ifsc_code=bank_ifsc_code,
        bank_holder_name=bank_holder_name, bank_branch_name=bank_branch_name,
        security_question=security_question, security_answer=security_answer,
        address_proof=address_proof, address_proof_image=address_image_filename,
        id_proof=id_proof, id_proof_image=id_image_filename,
        is_self_trading=is_self_trading, fund_manager=fund_manager,
        isActive=isActive, referral_code=referral_code
    )

    try:
        db_user = await create_user(db=db, user_data=user_create_data)
        user_logger.info(f"Successfully registered new live user with ID: {db_user.id}, email: {db_user.email}")
        return db_user
    except Exception as e:
        error_logger.error(f"Error during live user registration for email {email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during user registration. Please try again later."
        )

@router.post("/login", response_model=UserLoginResponse)
async def login_user(user_login: UserLogin, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Login attempt for username: {user_login.username} (Type: {user_login.user_type})")

    user = await authenticate_user(db, user_login)

    if not user:
        retrieved_user_result = await db.execute(
            select(User).filter(
                or_(User.email == user_login.username, User.phone_number == user_login.username),
                User.user_type == user_login.user_type
            )
        )
        retrieved_user = retrieved_user_result.scalar_one_or_none()

        if not retrieved_user or not verify_password(user_login.password, retrieved_user.hashed_password):
            user_logger.warning(f"Login failed for {user_login.username}: Invalid username or password.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password."
            )
        elif retrieved_user.status == 0:
            user_logger.warning(f"Login failed for {user_login.username}: User blocked (status=0).")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your account has been blocked. Please contact support."
            )
        elif retrieved_user.isActive == 0:
            user_logger.warning(f"Login failed for {user_login.username}: User email not verified (isActive=0).")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your email is not verified. Please check your inbox for verification instructions."
            )
        else:
            user_logger.warning(f"Login failed for {user_login.username}: Account not active/verified (status={retrieved_user.status}, isActive={retrieved_user.isActive}).")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your account is not active or verified. Please contact support."
            )

    user_logger.info(f"User {user.email} (ID: {user.id}) logged in successfully.")

    access_token = create_access_token(data={"sub": user.email, "user_id": user.id, "user_type": user.user_type})
    refresh_token = create_refresh_token(data={"sub": user.email, "user_id": user.id, "user_type": user.user_type})

    # Correctly convert SQLAlchemy User object to Pydantic UserResponse, then unpack
    user_response_data = UserResponse.model_validate(user).model_dump() # Use model_validate and model_dump

    return UserLoginResponse(
        **user_response_data, # Unpack the dictionary from the Pydantic UserResponse
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer"
    )

@router.post("/refresh-token", response_model=Token)
async def refresh_token(
    db: AsyncSession = Depends(get_db), # Changed to AsyncSession
    credentials: HTTPAuthorizationCredentials = Depends(security) # Inject HTTPAuthorizationCredentials
):
    refresh_token = credentials.credentials # Get the refresh token from the Bearer header
    user_logger.info("Attempting to refresh token.")

    payload = decode_token(refresh_token)

    if not payload:
        user_logger.warning("Refresh token validation failed: Invalid or expired token.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token."
        )

    user_email = payload.get("sub")
    user_id = payload.get("user_id")
    user_type = payload.get("user_type")

    if not user_email or not user_id or not user_type:
        user_logger.warning(f"Refresh token payload missing required fields: {payload}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token payload."
        )

    # Optionally, you might want to fetch the user from the DB to ensure they still exist and are active
    # For now, we'll assume the refresh token's payload is sufficient for generating new tokens.
    # If you need to check user status, uncomment and adapt:
    # result = await db.execute(
    #     select(User).filter(User.id == user_id, User.email == user_email, User.user_type == user_type)
    # )
    # user = result.scalar_one_or_none()
    # if not user or user.status != 1 or user.isActive != 1:
    #     user_logger.warning(f"Refresh token failed for user {user_email}: Account not active/verified.")
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="Account not active or verified. Please log in again."
    #     )


    new_access_token = create_access_token(data={"sub": user_email, "user_id": user_id, "user_type": user_type})
    new_refresh_token = create_refresh_token(data={"sub": user_email, "user_id": user_id, "user_type": user_type})

    user_logger.info(f"New tokens generated for user: {user_email}")
    return Token(access_token=new_access_token, refresh_token=new_refresh_token)


@router.get("/me", response_model=UserResponse)
async def read_current_user(current_user: User = Depends(get_current_user)):
    user_logger.info(f"User {current_user.email} (ID: {current_user.id}) accessed their own profile.")
    return current_user

@router.post("/send-otp", response_model=OTPSendResponse)
async def send_otp_for_verification(request: OTPSendRequest, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Attempting to send OTP to email: {request.email} (Type: {request.user_type})")

    # get_user_by_email_and_type will need to be async
    user = await get_user_by_email_and_type(db, request.email, request.user_type)

    if not user:
        user_logger.warning(f"Send OTP failed for {request.email}: User not found. Please register first.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found. Please register first."
        )

    if user.isActive == 1:
        user_logger.warning(f"Send OTP failed for {request.email}: User already exists and is verified.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User already exists and is verified."
        )
    
    try:
        # generate_and_store_otp will need to be async
        otp_code = await generate_and_store_otp(db, user.id)
        
        email_subject = "Your OTP for Account Verification"
        email_body = f"Dear {user.name},\n\nYour One-Time Password (OTP) for account verification is: {otp_code}\n\nThis OTP is valid for 5 minutes. Please do not share it with anyone.\n\nRegards,\nYour Trading Platform Team"
        await send_email(to_email=request.email, subject=email_subject, body=email_body)
        
        user_logger.info(f"OTP generated and email sent to user {user.email}.")
        return OTPSendResponse(message="OTP sent successfully to your email.")
    except Exception as e:
        user_logger.error(f"Error sending OTP for {request.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical error sending OTP for {request.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )

@router.post("/verify-otp", response_model=OTPVerifyResponse)
async def verify_otp(request: OTPVerifyRequest, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Attempting to verify OTP for email: {request.email} (Type: {request.user_type})")

    # get_user_by_email_and_type will need to be async
    user = await get_user_by_email_and_type(db, request.email, request.user_type)

    if not user:
        user_logger.warning(f"OTP verification failed for {request.email}: User not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found."
        )
    
    if user.isActive == 1:
        user_logger.warning(f"OTP verification failed for {request.email}: User already verified.")
        return OTPVerifyResponse(verified=True, message="User already verified.")

    try:
        # verify_user_otp will need to be async
        is_verified = await verify_user_otp(db, user.id, request.otp)

        if is_verified:
            user_logger.info(f"OTP successfully verified for user {request.email}. Account activated.")
            return OTPVerifyResponse(verified=True, message="Email successfully verified. Your account is now active.")
        else:
            user_logger.warning(f"OTP verification failed for user {request.email}: Invalid or expired OTP.")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired OTP."
            )
    except Exception as e:
        user_logger.error(f"Error during OTP verification for {request.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical error during OTP verification for {request.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during OTP verification. Please try again later."
        )


@router.post("/request-password-reset", response_model=PasswordResetResponse)
async def request_password_reset(request: PasswordResetRequest, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Requesting password reset for email: {request.email} (Type: {request.user_type})")

    # get_user_by_email_and_type will need to be async
    user = await get_user_by_email_and_type(db, request.email, request.user_type)

    if not user:
        user_logger.warning(f"Password reset request failed for {request.email}: User not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found."
        )
    
    try:
        # generate_and_store_otp will need to be async
        otp_code = await generate_and_store_otp(db, user.id)
        
        email_subject = "Your Password Reset OTP"
        email_body = f"Dear {user.name},\n\nWe received a request to reset your password. Your One-Time Password (OTP) is: {otp_code}\n\nThis OTP is valid for 5 minutes. Please do not share it with anyone.\n\nIf you did not request a password reset, please ignore this email.\n\nRegards,\nYour Trading Platform Team"
        await send_email(to_email=request.email, subject=email_subject, body=email_body)
        
        user_logger.info(f"Password reset OTP generated and email sent to user {user.email}.")
        return PasswordResetResponse(message="Password reset OTP sent successfully to your email.")
    except Exception as e:
        user_logger.error(f"Error sending password reset OTP for {request.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical error sending password reset OTP for {request.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send password reset OTP. Please try again later."
        )

@router.post("/verify-password-reset", response_model=PasswordResetVerifyResponse)
async def verify_password_reset(request: PasswordResetVerifyRequest, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Attempting to verify password reset OTP for email: {request.email} (Type: {request.user_type})")

    # get_user_by_email_and_type will need to be async
    user = await get_user_by_email_and_type(db, request.email, request.user_type)

    if not user:
        user_logger.warning(f"Password reset OTP verification failed for {request.email}: User not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found."
        )

    try:
        # verify_otp_for_password_reset will need to be async
        reset_token = await verify_otp_for_password_reset(db, user.id, request.otp)

        if reset_token:
            user_logger.info(f"Password reset OTP successfully verified for user {request.email}. Reset token issued.")
            return PasswordResetVerifyResponse(
                verified=True,
                message="OTP verified successfully. You can now reset your password.",
                reset_token=reset_token
            )
        else:
            user_logger.warning(f"OTP verification failed for user {request.email}: Invalid or expired OTP.")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired OTP."
            )
    except Exception as e:
        user_logger.error(f"Error during password reset OTP verification for {request.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical error during password reset OTP verification for {request.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during OTP verification. Please try again later."
        )

@router.post("/reset-password-confirm", response_model=ResetPasswordConfirmResponse)
async def reset_password_confirm(request: ResetPasswordConfirmRequest, db: AsyncSession = Depends(get_db)): # Changed to AsyncSession
    user_logger.info(f"Attempting to confirm password reset for email: {request.email} (Type: {request.user_type})")

    # get_user_by_email_and_type will need to be async
    user = await get_user_by_email_and_type(db, request.email, request.user_type)

    if not user:
        user_logger.warning(f"Password reset confirmation failed for {request.email}: User not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found."
        )
    
    # validate_and_consume_reset_token will need to be async
    is_token_valid_and_consumed = await validate_and_consume_reset_token(db, user.id, request.reset_token)

    if not is_token_valid_and_consumed:
        user_logger.warning(f"Password reset confirmation failed for {request.email}: Invalid or expired reset token.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired password reset token. Please restart the password reset process."
        )

    try:
        # update_user_password will need to be async
        await update_user_password(db, user, request.new_password)
        user_logger.info(f"Password successfully reset for user: {request.email}.")
        return ResetPasswordConfirmResponse(message="Password has been successfully reset.")
    except Exception as e:
        user_logger.error(f"Error confirming password reset for {request.email}: {e}", exc_info=True)
        error_logger.critical(f"Critical error confirming password reset for {request.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset password. Please try again later."
        )

@router.get("/images/{filename}")
async def get_image(filename: str):
    """
    Serves locally stored images.
    """
    file_path = os.path.join(settings.LOCAL_IMAGE_STORAGE_PATH, filename)
    
    # Basic security: Prevent directory traversal
    if not os.path.abspath(file_path).startswith(os.path.abspath(settings.LOCAL_IMAGE_STORAGE_PATH)):
        user_logger.warning(f"Attempted directory traversal: {filename}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid filename")

    if not os.path.exists(file_path):
        user_logger.warning(f"Image not found: {filename}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Image not found")
    
    user_logger.info(f"Serving image: {filename}")
    return FileResponse(file_path)
