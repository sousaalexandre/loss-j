import logging
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging

LOG_DIR = "outputs/logs"
FILENAME = "app.log"
LOG_PATH = os.path.join(LOG_DIR, FILENAME)
LOG_LEVEL = logging.INFO
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 3

logger = logging.getLogger("loss-j")
logger.setLevel(LOG_LEVEL)

if not logger.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(LOG_LEVEL)

    fh = ConcurrentRotatingFileHandler(
        LOG_PATH, 
        maxBytes=MAX_BYTES, 
        backupCount=BACKUP_COUNT, 
        encoding="utf-8"
    )
    fh.setFormatter(formatter)
    fh.setLevel(LOG_LEVEL)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logger.info(f"Logger initialized. Writing logs to {LOG_PATH}")

def get_logger() -> logging.Logger:
    return logger

def log(message: str, level: str = "info"):
    logger = get_logger()
    level = level.lower()
    if hasattr(logger, level):
        getattr(logger, level)(message)
    else:
        logger.info(message)

def info(message: str):
    get_logger().info(message)

def warning(message: str):
    get_logger().warning(message)

def error(message: str):
    get_logger().error(message)