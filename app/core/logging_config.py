# app/core/logging_config.py

import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Constants
MAX_LOG_SIZE_MB = 10  # Increased from 5MB to 10MB
BACKUP_COUNT = 5      # Increased from 3 to 5

# Define log directory (e.g., /path/to/app/logs)
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Create a reusable logger setup function
def setup_file_logger(name: str, filename: str, level=logging.INFO) -> logging.Logger:
    """
    Creates a rotating file logger with specified filename and level.
    """
    log_path = os.path.join(LOG_DIR, filename)
    handler = RotatingFileHandler(log_path, maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024, backupCount=BACKUP_COUNT)
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
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False
    return logger

# --- Loggers by Component ---

# File loggers
database_logger   = setup_file_logger("database", "database.log", logging.INFO)
firebase_logger   = setup_file_logger("firebase", "firebase.log", logging.INFO)
redis_logger      = setup_file_logger("redis", "redis.log", logging.DEBUG)
security_logger   = setup_file_logger("app.core.security", "security.log", logging.DEBUG)
app_logger        = setup_file_logger("app", "app.log", logging.INFO)
orders_logger     = setup_file_logger("orders", "orders.log", logging.DEBUG)
market_data_logger = setup_file_logger("market_data", "market_data.log", logging.DEBUG)
cache_logger      = setup_file_logger("cache", "cache.log", logging.DEBUG)

# New specialized loggers
frontend_orders_logger = setup_file_logger("frontend_orders", "frontend_orders.log", logging.DEBUG)
service_provider_logger = setup_file_logger("service_provider", "service_provider.log", logging.DEBUG)
firebase_comm_logger = setup_file_logger("firebase_comm", "firebase_comm.log", logging.DEBUG)
orders_crud_logger = setup_file_logger("orders_crud", "orders_crud.log", logging.INFO)
jwt_security_logger = setup_file_logger("jwt_security", "jwt_security.log", logging.DEBUG)
error_logger = setup_file_logger("error", "error.log", logging.ERROR)
money_requests_logger = setup_file_logger("money_requests", "money_requests.log", logging.DEBUG)
autocutoff_logger = setup_file_logger("autocutoff", "autocutoff.log", logging.DEBUG)

# WebSocket logger with stream output
websocket_logger  = setup_stream_logger("websocket_logger", logging.ERROR)

# Optionally force DEBUG level for specific modules globally
logging.getLogger("app.core.security").setLevel(logging.DEBUG)
logging.getLogger("app.dependencies.redis_client").setLevel(logging.DEBUG)
logging.getLogger("redis").setLevel(logging.WARNING)  # Suppress noisy internal logs if needed

# Set up logging for specific paths
logging.getLogger("app.api.v1.endpoints.orders").setLevel(logging.DEBUG)
logging.getLogger("app.services.order_processing").setLevel(logging.DEBUG)
logging.getLogger("app.core.firebase").setLevel(logging.DEBUG)