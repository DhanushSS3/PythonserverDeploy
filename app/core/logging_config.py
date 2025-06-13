# app/core/logging_config.py

import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Constants
MAX_LOG_SIZE_MB = 5
BACKUP_COUNT = 3

# Define log directory (e.g., /path/to/app/logs)
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Create a reusable logger setup function
def setup_file_logger(name: str, filename: str, level=logging.INFO) -> logging.Logger:
    """
    Creates a rotating file logger with specified filename and level.
    """
    log_path = os.path.join(LOG_DIR, filename)
    logger = logging.getLogger(name)
    
    # Only add handler if it doesn't already have handlers
    if not logger.handlers:
        handler = RotatingFileHandler(log_path, maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024, backupCount=BACKUP_COUNT)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    logger.setLevel(level)
    logger.propagate = False
    return logger

def setup_stream_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Creates a logger that outputs to the console (stdout).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Only add handler if it doesn't already have handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    logger.propagate = False
    return logger

# --- Loggers by Component ---

# File loggers
firebase_logger   = setup_file_logger("firebase", "firebase.log", logging.INFO)
redis_logger      = setup_file_logger("redis", "redis.log", logging.DEBUG)
security_logger   = setup_file_logger("app.core.security", "security.log", logging.DEBUG)
app_logger        = setup_file_logger("app", "app.log", logging.INFO)

# Important: Use 'orders-log' as the logger name to match what's used in the orders.py file
orders_logger     = setup_file_logger("orders-log", "orders.log", logging.DEBUG)

market_data_logger = setup_file_logger("market_data", "market_data.log", logging.DEBUG)
cache_logger      = setup_file_logger("cache", "cache.log", logging.DEBUG)
users_logger      = setup_file_logger("users", "users.log", logging.INFO)
server_logger     = setup_file_logger("server", "server.log", logging.INFO)
websocket_logger  = setup_file_logger("websocket", "websocket.log", logging.DEBUG)

# WebSocket stream logger (in addition to file)
websocket_stream_logger = setup_stream_logger("websocket_stream", logging.DEBUG)

# Optionally force DEBUG level for specific modules globally
logging.getLogger("app.core.security").setLevel(logging.DEBUG)
logging.getLogger("app.dependencies.redis_client").setLevel(logging.DEBUG)
logging.getLogger("redis").setLevel(logging.WARNING)  # Suppress noisy internal logs if needed

# Configure root logger for general application logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Configure security logger
security_logger = logging.getLogger('security')
security_file_handler = RotatingFileHandler(
    'logs/security.log',
    maxBytes=10485760,  # 10MB
    backupCount=3
)
security_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
security_logger.addHandler(security_file_handler)
security_logger.setLevel(logging.INFO)

# Configure orders logger
orders_logger = logging.getLogger('orders-log')
# Check if the logger already has handlers to avoid duplicates
if not orders_logger.handlers:
    orders_file_handler = RotatingFileHandler(
        'logs/orders.log',
        maxBytes=10485760,  # 10MB
        backupCount=3
    )
    orders_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    orders_logger.addHandler(orders_file_handler)
orders_logger.setLevel(logging.INFO)
# Ensure propagation is False to prevent duplicate logs
orders_logger.propagate = False

# Configure websocket logger
websocket_logger = logging.getLogger('websocket')
websocket_file_handler = RotatingFileHandler(
    'logs/websocket.log',
    maxBytes=10485760,  # 10MB
    backupCount=3
)
websocket_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
websocket_logger.addHandler(websocket_file_handler)
websocket_logger.setLevel(logging.INFO)

# Configure redis logger
redis_logger = logging.getLogger('redis')
redis_file_handler = RotatingFileHandler(
    'logs/redis.log',
    maxBytes=10485760,  # 10MB
    backupCount=3
)
redis_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
redis_logger.addHandler(redis_file_handler)
redis_logger.setLevel(logging.INFO)

# Configure users logger
users_logger = logging.getLogger('users')
users_file_handler = RotatingFileHandler(
    'logs/users.log',
    maxBytes=10485760,  # 10MB
    backupCount=3
)
users_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
users_logger.addHandler(users_file_handler)
users_logger.setLevel(logging.INFO)

# Configure server logger
server_logger = logging.getLogger('server')
server_file_handler = RotatingFileHandler(
    'logs/server.log',
    maxBytes=10485760,  # 10MB
    backupCount=3
)
server_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
server_logger.addHandler(server_file_handler)
server_logger.setLevel(logging.INFO)

