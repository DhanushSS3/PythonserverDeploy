# app/api/v1/endpoints/users.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import datetime

from app.database.session import get_db
from app.database.models import User # Import User model
from app.schemas.user import (
    UserCreate,
    UserResponse,
    SendOTPRequest,
    VerifyOTPRequest,
    RequestPasswordReset, # Import new schema
    ResetPasswordConfirm, # Import new schema
    StatusResponse # Use generic StatusResponse
)
from app.crud import user as crud_user # Import user CRUD
from app.crud import otp as crud_otp # Import OTP CRUD
from app.services import email as email_service # Import email service
from app.core.security import get_password_hash # Import password hashing utility
from app.core.config import get_settings

# Create an API router for user-related endpoints
router = APIRouter(
    tags=["users"]
)

# Get application settings
settings = get_settings()

@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user",
    description="Creates a new user account with the provided details."
)
async def register_user(
    user_in: UserCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Handles new user registration.
    """
    # Check if user with this email already exists
    existing_user_email = await crud_user.get_user_by_email(db, email=user_in.email)
    if existing_user_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Check if user with this phone number already exists
    existing_user_phone = await crud_user.get_user_by_phone_number(db, phone_number=user_in.phone_number)
    if existing_user_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phone number already registered"
        )

    try:
        # Create the user using the CRUD function
        new_user = await crud_user.create_user(db=db, user=user_in)
        return new_user # FastAPI will serialize this using UserResponse
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or phone number already exists."
        )
    except Exception as e:
        await db.rollback()
        print(f"Error during user registration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration."
        )

@router.post(
    "/send-otp",
    response_model=StatusResponse, # Use generic StatusResponse
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
    # Find the user by email
    user = await crud_user.get_user_by_email(db, email=otp_request.email)
    if not user:
        # Return a generic message to avoid revealing if an email is registered or not
        # In a real app, you might still send an email saying "if this email is registered..."
        print(f"Attempted to send OTP to non-existent email: {otp_request.email}")
        return StatusResponse(message="If a user with that email exists, an OTP has been sent.")

    # Check if the user is already verified (optional, depending on flow)
    # if user.is_verified: # Assuming a boolean field for verification status
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail="Email already verified."
    #     )

    try:
        # Create a new OTP for the user (deletes existing ones)
        otp_record = await crud_otp.create_otp(db, user_id=user.id)

        # Send the email with the OTP
        subject = "Your Trading App Email Verification OTP"
        body = f"Your One-Time Password (OTP) for email verification is: {otp_record.otp_code}\n\nThis OTP is valid for {settings.OTP_EXPIRATION_MINUTES} minutes."
        await email_service.send_email(
            to_email=user.email,
            subject=subject,
            body=body
        )

        return StatusResponse(message="OTP sent successfully for email verification.")

    except Exception as e:
        # Log the error and return a generic error response
        print(f"Error sending OTP for verification to {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP. Please try again later."
        )


@router.post(
    "/verify-otp",
    response_model=StatusResponse, # Use generic StatusResponse
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email or OTP."
        )

    # Get the valid OTP record for the user
    otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=verify_request.otp_code)

    if not otp_record:
        # If no valid OTP found (either incorrect or expired)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP."
        )

    # If OTP is valid, mark the user's email as verified
    # Assuming status=1 and isActive=1 means verified and active
    user.status = 1 # Example: Set status to 1 for verified
    user.isActive = 1 # Example: Set isActive to 1 for active
    await db.commit() # Commit the user status update
    # await db.refresh(user) # No need to refresh if not returning the user object

    # Delete the used OTP record
    await crud_otp.delete_otp(db, otp_id=otp_record.id)

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
    # Find the user by email
    user = await crud_user.get_user_by_email(db, email=reset_request.email)
    if not user:
        # Return a generic message for security
        print(f"Attempted password reset request for non-existent email: {reset_request.email}")
        return StatusResponse(message="If a user with that email exists, a password reset OTP has been sent.")

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
        print(f"Error sending password reset OTP to {user.email}: {e}")
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
    # Find the user by email
    user = await crud_user.get_user_by_email(db, email=reset_confirm.email)
    if not user:
        # Return a generic error message for security
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email or OTP."
        )

    # Get the valid OTP record for the user
    otp_record = await crud_otp.get_valid_otp(db, user_id=user.id, otp_code=reset_confirm.otp_code)

    if not otp_record:
        # If no valid OTP found (either incorrect or expired)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP."
        )

    # If OTP is valid, hash the new password and update the user record
    hashed_password = get_password_hash(reset_confirm.new_password)
    user.hashed_password = hashed_password
    await db.commit() # Commit the password update
    # await db.refresh(user) # No need to refresh if not returning the user object

    # Delete the used OTP record
    await crud_otp.delete_otp(db, otp_id=otp_record.id)

    return StatusResponse(message="Password reset successfully.")

# You can add other user-related endpoints here (e.g., /login, /me, /update-profile)
