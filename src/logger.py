import logging
import os
import datetime
from logging.handlers import RotatingFileHandler

_logger = None


def init_logger(log_dir: str = "outputs/logs", filename: str | None = None, level: int = logging.INFO, max_bytes: int = 5 * 1024 * 1024, backup_count: int = 3):
    global _logger
    if _logger:
        return _logger

    os.makedirs(log_dir, exist_ok=True)
    if filename is None:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ts}.log"

    log_path = os.path.join(log_dir, filename)

    logger = logging.getLogger("loss-j")
    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(level)

    fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(level)

    if not logger.handlers:
        logger.addHandler(sh)
        logger.addHandler(fh)

    _logger = logger
    logger.info(f"Logger initialized. Writing logs to {log_path}")
    return _logger


def get_logger() -> logging.Logger:
    global _logger
    if not _logger:
        return init_logger()
    return _logger


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
