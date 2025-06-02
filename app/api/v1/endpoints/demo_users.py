# app/api/endpoints/demo_users_endpoints.py

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_

from app.database.session import get_db
# Replace with actual import of DemoUser model
# from app.database.models import DemoUser
from app.database.models import DemoUser, User # User for type hints if shared components
from app.schemas.demo_user import (
    DemoUserCreate, DemoUserResponse, UserLogin, UserLoginResponse,
    RefreshTokenRequest, Token,
    OTPSendRequest, OTPSendResponse, OTPVerifyRequest, OTPVerifyResponse,
    PasswordResetRequest, PasswordResetResponse,
    PasswordResetVerifyRequest, PasswordResetVerifyResponse,
    ResetPasswordConfirmRequest, ResetPasswordConfirmResponse
)
from app.crud.crud_demo_user import (
    create_demo_user, authenticate_demo_user,
    get_demo_user_by_email, # Renamed from get_demo_user_by_email_and_type as type is fixed
    generate_and_store_otp_for_demo_user, verify_demo_user_otp,
    verify_otp_for_demo_password_reset, update_demo_user_password,
    validate_and_consume_reset_token_for_demo_user, verify_password as verify_demo_password # Alias if needed
)
from app.core.logging_config import user_logger, error_logger
from app.core.security import create_access_token, create_refresh_token, decode_token
from app.services.email import send_email # Assuming email service is generic
# from app.core.config import get_settings # Not needed for image paths in demo

security = HTTPBearer()

router = APIRouter(
    prefix="/demo-users",
    tags=["Demo Users"],
    responses={404: {"description": "Not found"}},
)

# settings = get_settings() # Not needed if no local image storage for demo

async def get_current_demo_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> DemoUser:
    """Dependency to get the current authenticated demo user."""
    token = credentials.credentials
    payload = decode_token(token)
    if not payload:
        user_logger.warning("Demo Auth: Invalid or expired token.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.")

    user_id = payload.get("user_id")
    user_email = payload.get("sub")
    token_user_type = payload.get("user_type")

    if not all([user_id, user_email, token_user_type]):
        user_logger.warning(f"Demo Auth: Token missing required info. Payload: {payload}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload.")

    if token_user_type != "demo":
        user_logger.warning(f"Demo Auth: Expected 'demo' user type, got '{token_user_type}'.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access forbidden.")

    result = await db.execute(
        select(DemoUser).filter(DemoUser.id == user_id, DemoUser.email == user_email, DemoUser.user_type == "demo")
    )
    user = result.scalar_one_or_none()

    if not user:
        user_logger.warning(f"Demo Auth: User not found for ID {user_id}, Email {user_email}.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Demo user not found.")
    
    if user.status != 1 or user.isActive != 1:
        user_logger.warning(f"Demo Auth failed for {user_email}: Account not active/verified.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account not active or verified.")

    user_logger.info(f"Demo user {user_email} (ID: {user.id}) authenticated via JWT.")
    return user

@router.post("/register", response_model=DemoUserResponse, status_code=status.HTTP_201_CREATED)
async def register_demo_user_endpoint(
    # Directly use fields from DemoUserCreate, FastAPI handles this if body is DemoUserCreate
    user_data: DemoUserCreate,
    db: AsyncSession = Depends(get_db)
):
    """Registers a new demo user in the system."""
    user_logger.info(f"Attempting to register new demo user: {user_data.email}")

    existing_email_result = await db.execute(select(DemoUser).filter(DemoUser.email == user_data.email, DemoUser.user_type == "demo"))
    if existing_email_result.scalar_one_or_none():
        user_logger.warning(f"Demo registration failed: Email '{user_data.email}' already registered.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered.")

    existing_phone_result = await db.execute(select(DemoUser).filter(DemoUser.phone_number == user_data.phone_number, DemoUser.user_type == "demo"))
    if existing_phone_result.scalar_one_or_none():
        user_logger.warning(f"Demo registration failed: Phone '{user_data.phone_number}' already registered.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Phone number already registered.")
    
    try:
        # isActive is not part of DemoUserCreate by default, it's handled by OTP or defaulted in CRUD
        db_user = await create_demo_user(db=db, user_data=user_data)
        user_logger.info(f"Successfully registered demo user: {db_user.email}")
        # Send OTP for verification
        otp_code = await generate_and_store_otp_for_demo_user(db, db_user.id)
        email_subject = "Verify Your Demo Account OTP"
        email_body = f"Dear {db_user.name},\n\nYour OTP for demo account verification is: {otp_code}\nThis OTP is valid for 5 minutes."
        await send_email(to_email=db_user.email, subject=email_subject, body=email_body)
        user_logger.info(f"OTP sent to demo user {db_user.email} for verification.")
        
        # The response model will map the db_user object
        return db_user
    except Exception as e:
        error_logger.error(f"Error during demo user registration for {user_data.email}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Registration error.")


@router.post("/login", response_model=UserLoginResponse) # Reuses UserLoginResponse (Token)
async def login_demo_user_endpoint(user_login: UserLogin, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Demo login attempt for: {user_login.username}")

    if user_login.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type for demo login.")

    user = await authenticate_demo_user(db, user_login)

    if not user: # authenticate_demo_user now returns None for various reasons (not found, wrong pass, not active)
        # Check user existence and password separately to give more specific errors if desired
        # For simplicity, relying on authenticate_demo_user's checks for now.
        # A more granular check could be:
        retrieved_user_result = await db.execute(
            select(DemoUser).filter(
                or_(DemoUser.email == user_login.username, DemoUser.phone_number == user_login.username),
                DemoUser.user_type == "demo"
            )
        )
        retrieved_user = retrieved_user_result.scalar_one_or_none()

        if not retrieved_user or not verify_demo_password(user_login.password, retrieved_user.hashed_password):
            user_logger.warning(f"Demo login failed for {user_login.username}: Invalid username or password.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password.")
        elif retrieved_user.status == 0: # status might mean 'blocked'
            user_logger.warning(f"Demo login failed for {user_login.username}: User blocked.")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account blocked.")
        elif retrieved_user.isActive == 0:
            user_logger.warning(f"Demo login failed for {user_login.username}: Email not verified.")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Email not verified.")
        else: # Other non-active cases
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed.")


    access_token = create_access_token(data={"sub": user.email, "user_id": user.id, "user_type": "demo"})
    refresh_token = create_refresh_token(data={"sub": user.email, "user_id": user.id, "user_type": "demo"})
    
    user_logger.info(f"Demo user {user.email} logged in successfully.")
    return UserLoginResponse(access_token=access_token, refresh_token=refresh_token, token_type="bearer")

@router.post("/refresh-token", response_model=Token)
async def refresh_demo_token_endpoint(
    # Use RefreshTokenRequest if you want it from body, or directly from header
    # request_data: RefreshTokenRequest, # if from body
    credentials: HTTPAuthorizationCredentials = Depends(security), # if from header
    db: AsyncSession = Depends(get_db) # db might be needed if re-validating user
):
    refresh_token = credentials.credentials # if from header
    # refresh_token = request_data.refresh_token # if from body
    user_logger.info("Attempting to refresh token for demo user.")
    payload = decode_token(refresh_token)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token.")

    user_email = payload.get("sub")
    user_id = payload.get("user_id")
    token_user_type = payload.get("user_type")

    if not all([user_email, user_id, token_user_type]) or token_user_type != "demo":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token payload for demo user.")

    # Optional: Re-verify user exists and is active in DB
    # user = await get_demo_user_by_email(db, user_email) # Assuming DemoUser here
    # if not user or user.id != user_id or user.status != 1 or user.isActive != 1:
    #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User no longer valid.")

    new_access_token = create_access_token(data={"sub": user_email, "user_id": user_id, "user_type": "demo"})
    new_refresh_token = create_refresh_token(data={"sub": user_email, "user_id": user_id, "user_type": "demo"})
    
    user_logger.info(f"New tokens generated for demo user: {user_email}")
    return Token(access_token=new_access_token, refresh_token=new_refresh_token, token_type="bearer")

@router.get("/me", response_model=DemoUserResponse)
async def read_current_demo_user_endpoint(current_user: DemoUser = Depends(get_current_demo_user)):
    user_logger.info(f"Demo user {current_user.email} accessed their profile.")
    return current_user

@router.post("/send-otp", response_model=OTPSendResponse)
async def send_otp_for_demo_verification(request: OTPSendRequest, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Attempting to send OTP to demo email: {request.email}")
    if request.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type for demo OTP.")

    user = await get_demo_user_by_email(db, request.email)
    if user:
        if user.isActive == 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Demo user already verified.")
    
    try:
        otp_code = await generate_and_store_otp_for_demo_user(db, user.id)
        await send_email(to_email=request.email, subject="Your Demo Account OTP", body=f"Dear {user.name}, Your OTP is: {otp_code}")
        return OTPSendResponse(message="OTP sent successfully to your email for demo account verification.")
    except Exception as e:
        error_logger.critical(f"Error sending OTP for demo {request.email}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to send OTP.")

@router.post("/verify-otp", response_model=OTPVerifyResponse)
async def verify_demo_otp_endpoint(request: OTPVerifyRequest, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Attempting to verify OTP for demo email: {request.email}")
    if request.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type for demo OTP.")

    user = await get_demo_user_by_email(db, request.email)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Demo user not found.")
    if user.isActive == 1: # Already verified
         return OTPVerifyResponse(verified=True, message="Demo user already verified.")

    is_verified = await verify_demo_user_otp(db, user.id, request.otp)
    if is_verified:
        return OTPVerifyResponse(verified=True, message="Email successfully verified for demo account.")
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")

@router.post("/request-password-reset", response_model=PasswordResetResponse)
async def request_demo_password_reset_endpoint(request: PasswordResetRequest, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Requesting password reset for demo email: {request.email}")
    if request.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type.")

    user = await get_demo_user_by_email(db, request.email)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Demo user not found.")
    
    try:
        otp_code = await generate_and_store_otp_for_demo_user(db, user.id) # Reuses OTP for reset purpose
        await send_email(to_email=request.email, subject="Demo Account Password Reset OTP", body=f"Dear {user.name}, Your password reset OTP is: {otp_code}")
        return PasswordResetResponse(message="Password reset OTP sent to your email for demo account.")
    except Exception as e:
        error_logger.critical(f"Error sending password reset OTP for demo {request.email}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to send OTP.")

@router.post("/verify-password-reset", response_model=PasswordResetVerifyResponse)
async def verify_demo_password_reset_otp_endpoint(request: PasswordResetVerifyRequest, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Verifying password reset OTP for demo email: {request.email}")
    if request.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type.")

    user = await get_demo_user_by_email(db, request.email)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Demo user not found.")

    reset_token = await verify_otp_for_demo_password_reset(db, user.id, request.otp)
    if reset_token:
        return PasswordResetVerifyResponse(verified=True, message="OTP verified. You can now reset demo password.", reset_token=reset_token)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired OTP.")

@router.post("/reset-password-confirm", response_model=ResetPasswordConfirmResponse)
async def reset_demo_password_confirm_endpoint(request: ResetPasswordConfirmRequest, db: AsyncSession = Depends(get_db)):
    user_logger.info(f"Confirming password reset for demo email: {request.email}")
    if request.user_type != "demo":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_type.")

    user = await get_demo_user_by_email(db, request.email)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Demo user not found.")

    is_token_valid = await validate_and_consume_reset_token_for_demo_user(db, user.id, request.reset_token)
    if not is_token_valid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired reset token.")

    await update_demo_user_password(db, user, request.new_password)
    return ResetPasswordConfirmResponse(message="Demo account password has been successfully reset.")