# app/core/logging_config.py

import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Constants
MAX_LOG_SIZE_MB = 10  # 10MB per log file
BACKUP_COUNT = 5      # Keep 5 backup files

# Define log directory
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Production environment detection
IS_PRODUCTION = os.getenv("ENVIRONMENT", "development").lower() == "production"

def setup_file_logger(name: str, filename: str, level=logging.INFO) -> logging.Logger:
    """
    Creates a rotating file logger with specified filename and level.
    """
    log_path = os.path.join(LOG_DIR, filename)
    handler = RotatingFileHandler(
        log_path, 
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024, 
        backupCount=BACKUP_COUNT,
        delay=True  # Delay file creation until first write
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def setup_stream_logger(name: str, level=logging.ERROR) -> logging.Logger:
    """
    Creates a logger that outputs to the console (stdout).
    Production: Only ERROR level logs
    Development: INFO level logs
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False
    return logger

# --- Production-Optimized Loggers ---

# === ESSENTIAL OPERATIONAL LOGGERS (INFO level) ===
# These loggers track critical business operations and should remain at INFO level

# Order lifecycle tracking
orders_logger = setup_file_logger("orders", "orders.log", logging.INFO)
autocutoff_logger = setup_file_logger("autocutoff", "autocutoff.log", logging.INFO)
app_logger = setup_file_logger("app", "app.log", logging.INFO)

# Service provider communication (critical for audit trail)
service_provider_logger = setup_file_logger("service_provider", "service_provider.log", logging.INFO)
firebase_comm_logger = setup_file_logger("firebase_comm", "firebase_comm.log", logging.INFO)

# External service communication
firebase_logger = setup_file_logger("firebase", "firebase.log", logging.INFO)

# Error tracking
error_logger = setup_file_logger("error", "error.log", logging.ERROR)

# === REDUCED FREQUENCY LOGGERS (WARNING level) ===
# These loggers are set to WARNING to reduce disk usage while retaining error visibility

# High-frequency operational loggers
cache_logger = setup_file_logger("cache", "cache.log", logging.WARNING)
websocket_logger = setup_file_logger("websocket", "websocket.log", logging.WARNING)
market_data_logger = setup_file_logger("market_data", "market_data.log", logging.WARNING)
redis_logger = setup_file_logger("redis", "redis.log", logging.WARNING)

# Database operations (only log errors and warnings)
database_logger = setup_file_logger('database', 'database.log', level=logging.WARNING if IS_PRODUCTION else logging.INFO)
swap_logger = setup_file_logger("swap", "swap.log", logging.INFO)
# === DEVELOPMENT-ONLY LOGGERS (DEBUG level) ===
# These loggers are only active in development environment

if not IS_PRODUCTION:
    # Development-specific loggers
    security_logger = setup_file_logger("app.core.security", "security.log", logging.INFO)
    frontend_orders_logger = setup_file_logger("frontend_orders", "frontend_orders.log", logging.DEBUG)
    orders_crud_logger = setup_file_logger("orders_crud", "orders_crud.log", logging.DEBUG)
    jwt_security_logger = setup_file_logger("jwt_security", "jwt_security.log", logging.DEBUG)
    money_requests_logger = setup_file_logger("money_requests", "money_requests.log", logging.DEBUG)
else:
    # In production, create minimal loggers for these components
    security_logger = setup_file_logger("app.core.security", "security.log", logging.INFO)
    frontend_orders_logger = setup_file_logger("frontend_orders", "frontend_orders.log", logging.WARNING)
    orders_crud_logger = setup_file_logger("orders_crud", "orders_crud.log", logging.WARNING)
    jwt_security_logger = setup_file_logger("jwt_security", "jwt_security.log", logging.WARNING)
    money_requests_logger = setup_file_logger("money_requests", "money_requests.log", logging.WARNING)

# === CONSOLE LOGGER (ERROR level only) ===
# Console output for container visibility - only ERROR logs in production
console_logger = setup_stream_logger("console", logging.ERROR if IS_PRODUCTION else logging.INFO)

# === THIRD-PARTY LIBRARY LOGGING ===
# Suppress noisy internal logs from third-party libraries
logging.getLogger("redis").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# === MODULE-SPECIFIC LOGGING LEVELS ===
# Set appropriate levels for specific modules based on their importance

# Critical modules - INFO level for operational visibility
logging.getLogger("app.api.v1.endpoints.orders").setLevel(logging.INFO)
logging.getLogger("app.services.order_processing").setLevel(logging.INFO)
logging.getLogger("app.core.firebase").setLevel(logging.INFO)

# High-frequency modules - WARNING level to reduce noise
logging.getLogger("app.api.v1.endpoints.market_data_ws").setLevel(logging.WARNING)
logging.getLogger("app.core.cache").setLevel(logging.WARNING)
logging.getLogger("app.dependencies.redis_client").setLevel(logging.WARNING)

# Security modules - WARNING level (only log security issues)
logging.getLogger("app.core.security").setLevel(logging.INFO)

# === SERVICE PROVIDER REQUEST LOGGER ===
# Specialized logger for service provider incoming requests
def setup_service_provider_request_logger():
    """
    Creates a specialized logger for service provider incoming requests.
    This ensures all external requests are logged for audit and debugging.
    """
    log_path = os.path.join(LOG_DIR, "service_provider_requests.log")
    handler = RotatingFileHandler(
        log_path, 
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024, 
        backupCount=BACKUP_COUNT,
        delay=True
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - SERVICE_PROVIDER_REQUEST - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger("service_provider_requests")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

# Create the service provider request logger
service_provider_request_logger = setup_service_provider_request_logger()

# === LOGGING CONFIGURATION SUMMARY ===
if IS_PRODUCTION:
    print("=== PRODUCTION LOGGING CONFIGURATION ===")
    print("✅ Essential operational logs: INFO level")
    print("✅ Service provider requests: INFO level")
    print("✅ High-frequency logs: WARNING level")
    print("✅ Console output: ERROR level only")
    print("✅ Rotating file handlers: 10MB files, 5 backups")
    print("========================================")
else:
    print("=== DEVELOPMENT LOGGING CONFIGURATION ===")
    print("✅ All logs: DEBUG level")
    print("✅ Console output: INFO level")
    print("✅ Full debugging enabled")
    print("=========================================")

order_audit_logger = setup_file_logger("order_audit", "order_audit.log", logging.INFO)