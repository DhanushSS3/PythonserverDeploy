# app/core/logging_config.py

import logging
from logging.handlers import RotatingFileHandler
import os
import sys

LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name: str, log_file: str, level=logging.INFO):
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, log_file), maxBytes=5*1024*1024, backupCount=3
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

# Pre-configured loggers
firebase_logger = setup_logger("firebase", "firebase.log", logging.INFO)
redis_logger = setup_logger("redis", "redis.log", logging.INFO)
# websocket_logger = setup_logger("websocket", "websocket.log", logging.INFO)
security_logger = setup_logger("security", "security.log", logging.INFO)
app_logger = setup_logger("app", "app.log", logging.INFO)

websocket_logger = logging.getLogger("websocket_logger")
websocket_logger.setLevel(logging.DEBUG) # Or logging.INFO
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
websocket_logger.addHandler(handler)
websocket_logger.propagate = False