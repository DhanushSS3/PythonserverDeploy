# app/core/config.py

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from functools import lru_cache
from urllib.parse import quote_plus # Import quote_plus for URL encoding
from typing import Optional # Import Optional for nullable fields
import logging # Import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

# Load environment variables from a .env file if it exists.
# Use verbose=True to see if and where the .env file is found.
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env') # Construct path relative to config.py
loaded = load_dotenv(dotenv_path=dotenv_path, verbose=True) # Explicitly provide path and set verbose=True

if loaded:
    logger.info(".env file loaded successfully.")
else:
    logger.warning(".env file not found or not loaded.")

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    # --- Database Settings ---
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "u43658492_forex")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "u43658492_forex")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "Setupdev@1998")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "127.0.0.1") # IMPORTANT: Change this for production!
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "3306")

    # Use a different name for the property to avoid conflict with env var DATABASE_URL
    @property
    def ASYNC_DATABASE_URL(self) -> str:
        """
        Constructs the asynchronous database URL, URL-encoding the password.
        """
        encoded_password = quote_plus(self.DATABASE_PASSWORD)
        # Using mysql+aiomysql driver for async MySQL/MariaDB support
        return f"mysql+aiomysql://{self.DATABASE_USER}:{encoded_password}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"

    # --- Email Settings ---
    EMAIL_HOST: str = os.getenv("EMAIL_HOST", "smtp.hostinger.com")
    EMAIL_PORT: int = int(os.getenv("EMAIL_PORT", 465))
    EMAIL_USE_SSL: bool = os.getenv("EMAIL_USE_SSL", "True").lower() in ('true', '1', 't')
    EMAIL_HOST_USER: str = os.getenv("EMAIL_HOST_USER", 'noreply@livefxhub.com')
    EMAIL_HOST_PASSWORD: str = os.getenv("EMAIL_HOST_PASSWORD", 'India@555')
    # Add DEFAULT_FROM_EMAIL as an attribute
    DEFAULT_FROM_EMAIL: str = os.getenv("DEFAULT_FROM_EMAIL", 'noreply@livefxhub.com')
    # Add MAIL_FROM if you are using that specific variable name in .env
    MAIL_FROM: Optional[str] = os.getenv("MAIL_FROM") # Assuming this might be in your .env

    # --- OTP and Password Reset Settings ---
    OTP_EXPIRATION_MINUTES: int = int(os.getenv("OTP_EXPIRATION_MINUTES", 5))
    PASSWORD_RESET_TIMEOUT_HOURS: int = int(os.getenv("PASSWORD_RESET_TIMEOUT_HOURS", 1))

    # --- Security Settings (JWT) ---
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-super-secret-key-change-this!")
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))

    # --- Redis Settings ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", None)

    # --- Firebase Configuration ---
    # Add Firebase attributes to the Settings class
    FIREBASE_SERVICE_ACCOUNT_KEY_PATH: str = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", "") # Make sure this is set in .env
    FIREBASE_DATABASE_URL: str = os.getenv("FIREBASE_DATABASE_URL", "") # Make sure this is set in .env
    FIREBASE_API_KEY: Optional[str] = os.getenv("FIREBASE_API_KEY") # Optional, often not needed for Admin SDK
    FIREBASE_STORAGE_BUCKET: Optional[str] = os.getenv("FIREBASE_STORAGE_BUCKET") # Optional
    FIREBASE_AUTH_DOMAIN: Optional[str] = os.getenv("FIREBASE_AUTH_DOMAIN") # Optional


    class Config:
        env_file = ".env" # This tells BaseSettings to look for a .env file
        env_nested_delimiter = '__'
        # Allow extra fields if needed (though it's better to define them)
        # extra = "allow" # Uncomment this if you want to allow extra env vars without defining them

# Add print statements to see the values read from environment variables
logger.info(f"Environment variable REDIS_HOST: {os.getenv('REDIS_HOST')}")
logger.info(f"Environment variable REDIS_PORT: {os.getenv('REDIS_PORT')}")
logger.info(f"Environment variable REDIS_DB: {os.getenv('REDIS_DB')}")
logger.info(f"Environment variable REDIS_PASSWORD: {'<set>' if os.getenv('REDIS_PASSWORD') else '<not set>'}")
logger.info(f"Environment variable FIREBASE_SERVICE_ACCOUNT_KEY_PATH: {os.getenv('FIREBASE_SERVICE_ACCOUNT_KEY_PATH')}")
logger.info(f"Environment variable FIREBASE_DATABASE_URL: {os.getenv('FIREBASE_DATABASE_URL')}")
logger.info(f"Environment variable DEFAULT_FROM_EMAIL: {os.getenv('DEFAULT_FROM_EMAIL')}")


@lru_cache()
def get_settings():
    """
    Returns a cached instance of the Settings class.
    """
    # Add print statements to see the values used by Settings
    settings_instance = Settings() # Create the instance
    logger.info(f"Settings instance REDIS_HOST: {settings_instance.REDIS_HOST}")
    logger.info(f"Settings instance REDIS_PORT: {settings_instance.REDIS_PORT}")
    logger.info(f"Settings instance REDIS_DB: {settings_instance.REDIS_DB}")
    logger.info(f"Settings instance REDIS_PASSWORD: {'<set>' if settings_instance.REDIS_PASSWORD else '<not set>'}")
    logger.info(f"Settings instance FIREBASE_SERVICE_ACCOUNT_KEY_PATH: {settings_instance.FIREBASE_SERVICE_ACCOUNT_KEY_PATH}")
    logger.info(f"Settings instance FIREBASE_DATABASE_URL: {settings_instance.FIREBASE_DATABASE_URL}")
    logger.info(f"Settings instance DEFAULT_FROM_EMAIL: {settings_instance.DEFAULT_FROM_EMAIL}")
    logger.info(f"Settings instance ASYNC_DATABASE_URL: {settings_instance.ASYNC_DATABASE_URL[:20]}...") # Log part of the URL

    return settings_instance

