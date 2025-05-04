# app/core/config.py

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from functools import lru_cache
from urllib.parse import quote_plus # Import quote_plus for URL encoding

# Load environment variables from a .env file if it exists.
load_dotenv()

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    # --- Database Settings ---
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "u436589492_forex")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "u436589492_forex")
    # Get password from environment variable, ensure it's treated as raw string
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "Setupdev@1998")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "127.0.0.1") # IMPORTANT: Change this for production!
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "3306")

    # Construct the asynchronous database URL.
    @property
    def DATABASE_URL(self) -> str:
        """
        Constructs the database URL, URL-encoding the password.
        """
        # URL-encode the password to handle special characters like '@'
        encoded_password = quote_plus(self.DATABASE_PASSWORD)
        # Using mysql+aiomysql driver for async MySQL/MariaDB support
        return f"mysql+aiomysql://{self.DATABASE_USER}:{encoded_password}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"

    # --- Email Settings ---
    EMAIL_HOST: str = os.getenv("EMAIL_HOST", "smtp.hostinger.com")
    EMAIL_PORT: int = int(os.getenv("EMAIL_PORT", 465))
    EMAIL_USE_SSL: bool = os.getenv("EMAIL_USE_SSL", "True").lower() in ('true', '1', 't')
    EMAIL_HOST_USER: str = os.getenv("EMAIL_HOST_USER", 'noreply@livefxhub.com')
    EMAIL_HOST_PASSWORD: str = os.getenv("EMAIL_HOST_PASSWORD", 'India@555')
    DEFAULT_FROM_EMAIL: str = os.getenv("DEFAULT_FROM_EMAIL", 'noreply@livefxhub.com')

    # --- OTP and Password Reset Settings ---
    OTP_EXPIRATION_MINUTES: int = int(os.getenv("OTP_EXPIRATION_MINUTES", 5))
    PASSWORD_RESET_TIMEOUT_HOURS: int = int(os.getenv("PASSWORD_RESET_TIMEOUT_HOURS", 1))

    # --- Security Settings (Add these as needed) ---
    # SECRET_KEY: str = os.getenv("SECRET_KEY", "your-super-secret-key-change-this!")
    # ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    # ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
    # REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))

    # --- Redis Settings (Add these when integrating Redis) ---
    # REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    # REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    # REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    # REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")

    class Config:
        env_file = ".env"
        env_nested_delimiter = '__'

@lru_cache()
def get_settings():
    """
    Returns a cached instance of the Settings class.
    """
    return Settings()
