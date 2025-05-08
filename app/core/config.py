# app/core/config.py

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings # Make sure pydantic-settings is installed
from functools import lru_cache
from urllib.parse import quote_plus
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
loaded = load_dotenv(dotenv_path=dotenv_path, verbose=True)

if loaded:
    logger.info(".env file loaded successfully.")
else:
    logger.warning(".env file not found or not loaded.")

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    # --- Project Settings ---
    # Add these lines:
    PROJECT_NAME: str = "Trading App" # Default project name
    API_V1_STR: str = "/api/v1"      # Default API version prefix
    # --- End Project Settings ---


    # --- Database Settings ---
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "u43658492_forex")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "u43658492_forex")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "Setupdev@1998")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "localhost")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "3306") # Keep as string if building URL
    DATABASE_DRIVER: str = "mysql+aiomysql" # Explicitly set async driver

    @property
    def ASYNC_DATABASE_URL(self) -> str:
        # Safely quote password and database name for the URL
        quoted_password = quote_plus(self.DATABASE_PASSWORD)
        quoted_dbname = quote_plus(self.DATABASE_NAME)
        return f"{self.DATABASE_DRIVER}://{self.DATABASE_USER}:{quoted_password}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{quoted_dbname}"


    # --- Redis Settings ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379")) # Cast to int
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))      # Cast to int
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD") # Use Optional for nullable password


    # --- Security Settings ---
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your_super_secret_key_change_this") # CHANGE THIS IN PRODUCTION!
    ALGORITHM: str = "HS256" # Example algorithm
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30")) # Example expiry
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7")) # Example expiry

    # Refresh token settings for Redis (key prefix and expiry)
    REFRESH_TOKEN_REDIS_KEY_PREFIX: str = "refresh_token:"
    # Use the expiry from settings, but store as seconds
    REFRESH_TOKEN_REDIS_EXPIRE_SECONDS: int = REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60


    # --- Email Settings (for OTP, etc.) ---
    SMTP_TLS: bool = os.getenv("SMTP_TLS", "True").lower() == "true" # Convert string to boolean
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_USER: str = os.getenv("SMTP_USER", "your_email@gmail.com") # CHANGE THIS IN PRODUCTION!
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "your_email_password") # CHANGE THIS IN PRODUCTION!
    EMAILS_FROM_NAME: str = os.getenv("EMAILS_FROM_NAME", "Trading App Support")
    DEFAULT_FROM_EMAIL: str = os.getenv("DEFAULT_FROM_EMAIL", "noreply@tradingapp.com") # CHANGE THIS

    # --- OTP Settings ---
    OTP_EXPIRATION_MINUTES: int = int(os.getenv("OTP_EXPIRATION_MINUTES", "5")) # OTP expiry in minutes


    # --- Firebase Settings ---
    FIREBASE_SERVICE_ACCOUNT_KEY_PATH: str = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", "/app/firebase-service-account.json") # Update path
    FIREBASE_DATABASE_URL: str = os.getenv("FIREBASE_DATABASE_URL", "https://your-firebase-project.firebaseio.com") # Update URL
    FIREBASE_DATA_PATH: str = os.getenv("FIREBASE_DATA_PATH", "datafeeds") # Path to market data in Firebase


    # --- Other Settings ---
    # Add any other settings your application needs here


    class Config:
        # Specifies that pydantic-settings should read from environment variables
        # and ignore extra fields if found in env but not in the class.
        env_file = ".env"
        extra = "ignore" # Or "forbid" if you want strict env var checking


@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached instance of the Settings class.
    """
    settings_instance = Settings()
    # Logging settings upon loading (optional, useful for debugging)
    logger.info(f"Settings instance loaded. Project: {settings_instance.PROJECT_NAME}, API Prefix: {settings_instance.API_V1_STR}")
    # logger.info(f"Settings instance REDIS_HOST: {settings_instance.REDIS_HOST}") # Example logging
    # logger.info(f"Settings instance ASYNC_DATABASE_URL: {settings_instance.ASYNC_DATABASE_URL[:20]}...") # Log part of DB URL
    # ... log other critical settings if needed ...
    return settings_instance

# Call get_settings once to load and cache settings on import
# settings = get_settings() # It's common to call this once at the module level