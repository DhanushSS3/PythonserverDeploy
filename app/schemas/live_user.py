# app/schemas/live_user.py

from pydantic import BaseModel, EmailStr, Field, validator, ConfigDict # Import ConfigDict
from typing import Optional, Union
from decimal import Decimal
import datetime

class UserCreate(BaseModel):
    """
    Pydantic model for user registration (signup) request.
    Defines the required and optional fields for creating a new live user.
    """
    name: str = Field(..., min_length=2, max_length=255)
    phone_number: str = Field(..., min_length=10, max_length=20)
    email: EmailStr
    password: str = Field(..., min_length=8) # Password will be hashed
    city: str = Field(..., max_length=100)
    state: str = Field(..., max_length=100)
    pincode: int
    group: str = Field(..., max_length=255) # Maps to 'group_name' in User model
    bank_account_number: str = Field(..., max_length=100)
    bank_ifsc_code: str = Field(..., max_length=50)
    bank_holder_name: str = Field(..., max_length=255)
    bank_branch_name: str = Field(..., max_length=255)
    security_question: str = Field(..., max_length=255)
    security_answer: str = Field(..., max_length=255)
    address_proof: str = Field(..., max_length=255) # e.g., "Aadhar Card", "Driving License"
    address_proof_image: str = Field(..., max_length=255) # Path/URL to the uploaded image
    id_proof: str = Field(..., max_length=255) # e.g., "PAN Card", "Passport"
    id_proof_image: str = Field(..., max_length=255) # Path/URL to the uploaded image
    is_self_trading: bool
    fund_manager: Optional[str] = Field(None, max_length=255) # Made fund_manager optional
    isActive: bool # Maps to 'isActive' integer column (1 for True, 0 for False)
    referral_code: Optional[str] = Field(None, max_length=20) # Optional field

    @validator('pincode')
    def validate_pincode(cls, value):
        """Validates that the pincode is a 6-digit number."""
        if not (100000 <= value <= 999999): # Assuming 6-digit Indian pincode
            raise ValueError('Pincode must be a 6-digit number')
        return value

    @validator('is_self_trading', 'isActive', pre=True)
    def convert_bool_to_int(cls, value):
        """Converts boolean input to 1 or 0 for database storage."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

class UserResponse(BaseModel):
    """
    Pydantic model for returning user data after creation.
    Excludes sensitive information like hashed password.
    """
    id: int
    name: str
    email: EmailStr
    phone_number: str
    account_number: str
    user_type: str
    wallet_balance: Decimal
    net_profit: Decimal
    leverage: Decimal
    margin: Decimal
    status: int
    city: str
    state: str
    pincode: int
    group_name: str # Renamed to match User model's 'group_name'
    bank_account_number: str
    bank_ifsc_code: str
    bank_holder_name: str
    bank_branch_name: str
    security_question: str
    security_answer: str
    address_proof: str
    address_proof_image: str
    id_proof: str
    id_proof_image: str
    is_self_trading: int
    fund_manager: Optional[str] # Changed to Optional[str] to match the database and input
    isActive: int
    reffered_code: Optional[str] # Renamed to match User model's 'reffered_code'
    created_at: datetime.datetime
    updated_at: datetime.datetime

    model_config = ConfigDict(from_attributes=True, json_encoders={
        Decimal: lambda v: str(v) # Ensure Decimal is serialized as string
    })


# --- Pydantic Models for Login ---

class UserLogin(BaseModel):
    """
    Pydantic model for user login request.
    Accepts email or phone_number as username.
    """
    username: str = Field(..., description="User's email or phone number")
    password: str = Field(..., min_length=8)
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class Token(BaseModel):
    """
    Pydantic model for JWT token response.
    """
    access_token: str
    token_type: str = "bearer"
    refresh_token: str

class UserLoginResponse(Token): # Changed to inherit from Token
    """
    Pydantic model for user login response.
    Now only includes JWT tokens.
    """
    pass # No additional fields needed, as it now only contains tokens

class RefreshTokenRequest(BaseModel):
    """
    Pydantic model for refresh token request.
    """
    refresh_token: str

# --- Pydantic Models for OTP Verification ---

class OTPSendRequest(BaseModel):
    """
    Pydantic model for sending OTP request.
    """
    email: EmailStr
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class OTPSendResponse(BaseModel):
    """
    Pydantic model for sending OTP response.
    """
    message: str

class OTPVerifyRequest(BaseModel):
    """
    Pydantic model for verifying OTP request.
    """
    email: EmailStr
    otp: str = Field(..., min_length=6, max_length=6, description="6-digit OTP")
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class OTPVerifyResponse(BaseModel):
    """
    Pydantic model for verifying OTP response.
    """
    verified: bool
    message: str

# --- New Pydantic Models for Forgot Password Flow ---

class PasswordResetRequest(BaseModel):
    """
    Pydantic model for requesting a password reset OTP.
    """
    email: EmailStr
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class PasswordResetResponse(BaseModel):
    """
    Pydantic model for the response after requesting password reset.
    """
    message: str

class PasswordResetVerifyRequest(BaseModel):
    """
    Pydantic model for verifying the password reset OTP.
    """
    email: EmailStr
    otp: str = Field(..., min_length=6, max_length=6, description="6-digit OTP")
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class PasswordResetVerifyResponse(BaseModel):
    """
    Pydantic model for the response after verifying password reset OTP.
    Now includes a temporary reset_token.
    """
    verified: bool
    message: str
    reset_token: Optional[str] = None # New field for the temporary token

class ResetPasswordConfirmRequest(BaseModel):
    """
    Pydantic model for confirming the password reset with a new password.
    Now requires the reset_token.
    """
    email: EmailStr
    reset_token: str = Field(..., description="Temporary token received after OTP verification.")
    new_password: str = Field(..., min_length=8)
    user_type: Optional[str] = Field("live", description="Type of user, defaults to 'live'")

class ResetPasswordConfirmResponse(BaseModel):
    """
    Pydantic model for the response after confirming password reset.
    """
    message: str

class StatusResponse(BaseModel):
    """
    Generic Pydantic model for simple status responses.
    """
    message: str = Field(..., description="Response message.")