import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that writes to both console and a daily log file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Preventing Duplicate Handlers (Critical)
    if logger.handlers:
        return logger  # Already configured

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)

    # File handler — one log file per day
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{datetime.today().strftime('%Y-%m-%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger