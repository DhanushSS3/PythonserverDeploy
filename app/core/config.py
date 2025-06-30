# app/core/config.py

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict # Import SettingsConfigDict for Pydantic V2
from functools import lru_cache
from urllib.parse import quote_plus
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
loaded = load_dotenv(dotenv_path=dotenv_path, verbose=True)

LOADED_SECRET_KEY = os.getenv("SECRET_KEY")
LOADED_ALGORITHM = os.getenv("ALGORITHM")
print(f"DEBUG: Loaded SECRET_KEY from env: '{LOADED_SECRET_KEY}'")
print(f"DEBUG: Loaded ALGORITHM from env: '{LOADED_ALGORITHM}'")

if loaded:
    logger.info(".env file loaded successfully.")
else:
    logger.warning(".env file not found or not loaded.")

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """
    # Pydantic V2 way to configure settings
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Project Settings ---
    PROJECT_NAME: str = "Trading App"
    API_V1_STR: str = "/api/v1"

    # --- Database Settings ---
    # Read raw values from environment variables first
    _db_name = os.getenv("DATABASE_NAME", "")
    _db_user = os.getenv("DATABASE_USER", "")
    _db_password = os.getenv("DATABASE_PASSWORD", "")
    _db_host = os.getenv("DATABASE_HOST", "")
    _db_port = os.getenv("DATABASE_PORT", "")

    DATABASE_NAME: str = _db_name
    DATABASE_USER: str = _db_user
    DATABASE_PASSWORD: str = _db_password
    DATABASE_HOST: str = _db_host
    DATABASE_PORT: str = _db_port

    # Construct ASYNC_DATABASE_URL using the read values
    ASYNC_DATABASE_URL: str = f"mysql+aiomysql://{quote_plus(DATABASE_USER)}:{quote_plus(DATABASE_PASSWORD)}@{DATABASE_HOST}:{DATABASE_PORT}/{quote_plus(DATABASE_NAME)}"
    ECHO_SQL: bool = os.getenv("ECHO_SQL", "False").lower() in ("true", "1", "t")

    # --- JWT Settings ---
    SECRET_KEY: str = LOADED_SECRET_KEY or "fallback_if_you_must_but_not_recommended_for_secret"
    ALGORITHM: str = LOADED_ALGORITHM or "HS256" # Ensure a default if it can be empty
    # Ensure integer conversion for values from environment variables
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

    # Corrected: Read REFRESH_TOKEN_EXPIRE_DAYS as int, then calculate REFRESH_TOKEN_EXPIRE_MINUTES
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
    REFRESH_TOKEN_EXPIRE_MINUTES: int = REFRESH_TOKEN_EXPIRE_DAYS * 60 * 24


    # --- OTP Settings ---
    OTP_EXPIRATION_MINUTES: int = int(os.getenv("OTP_EXPIRATION_MINUTES", "5"))
    PASSWORD_RESET_TIMEOUT_HOURS: int = int(os.getenv("PASSWORD_RESET_TIMEOUT_HOURS", "1"))


    # --- Redis Settings ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")

    # --- Firebase Settings ---
    # Use raw string for path to handle backslashes correctlyFIREBASE_PRI
    FIREBASE_SERVICE_ACCOUNT_KEY_PATH: str = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", r"C:\Users\Dhanush\OneDrive\Desktop\livefxhub-cb49c-firebase-adminsdk-dyf73-51beafa5c6.json")
    FIREBASE_DATABASE_URL: str = os.getenv("FIREBASE_DATABASE_URL", "https://-default-rtdb.firebaseio.com")
    FIREBASE_API_KEY: str = os.getenv("FIREBASE_API_KEY", "AIzaSyBKk-")
    FIREBASE_STORAGE_BUCKET: str = os.getenv("FIREBASE_STORAGE_BUCKET", "livefxhub-.appspot.com")
    FIREBASE_AUTH_DOMAIN: str = os.getenv("FIREBASE_AUTH_DOMAIN", "livefxhub-.firebaseapp.com")
    FIREBASE_DATA_PATH: str = os.getenv("FIREBASE_DATA_PATH", "datafeeds")

    # --- Email Settings ---
    EMAIL_HOST: str = os.getenv("EMAIL_HOST", "smtp.hostinger.com")
    EMAIL_PORT: int = int(os.getenv("EMAIL_PORT", "465"))
    EMAIL_USE_SSL: bool = os.getenv("EMAIL_USE_SSL", "True").lower() in ("true", "1", "t")
    EMAIL_HOST_USER: str = os.getenv("EMAIL_HOST_USER", "noreply@.com")
    EMAIL_HOST_PASSWORD: str = os.getenv("EMAIL_HOST_PASSWORD", "India555")
    DEFAULT_FROM_EMAIL: str = os.getenv("DEFAULT_FROM_EMAIL", "noreply@.com")
    MAIL_FROM: str = os.getenv("MAIL_FROM", "noreply@.")

    SLTP_EPSILON: float = 0.00001
    
    # Tylt.money Payment Gateway API Credentials
    TYLT_API_KEY: str = os.getenv("TLP_API_KEY", "")
    TYLT_API_SECRET: str = os.getenv("TLP_API_SECRET", "")


@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached instance of the Settings class.
    """
    settings_instance = Settings()
    logger.info(f"Settings instance loaded. Project: {settings_instance.PROJECT_NAME}, API Prefix: {settings_instance.API_V1_STR}")
    return settings_instance


settings = Settings()