"""
Logging utilities for the application.
Configures logging with both file and console handlers.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Configure logger with file and console handlers.

    Args:
        name: Logger name (typically __name__ of the calling module)
        log_file: Path to log file (default: logs/app.log)
        level: Logging level (default: INFO)
        format_string: Custom format string (default: standard format)

    Returns:
        Configured logger instance

    Examples:
        >>> logger = setup_logger(__name__)
        >>> logger.info("Application started")

        >>> logger = setup_logger(__name__, log_file='logs/custom.log', level=logging.DEBUG)
        >>> logger.debug("Debug information")
    """
    if log_file is None:
        log_file = 'logs/app.log'

    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    # Default format
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    formatter = logging.Formatter(format_string)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # Capture all levels to file
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)  # Use specified level for console
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger by name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LoggerContext:
    """
    Context manager for temporary logging level changes.

    Examples:
        >>> logger = setup_logger(__name__)
        >>> with LoggerContext(logger, logging.DEBUG):
        ...     logger.debug("This will be logged")
        >>> logger.debug("This will not be logged if level is INFO")
    """

    def __init__(self, logger: logging.Logger, level: int):
        """
        Initialize context manager.

        Args:
            logger: Logger instance
            level: Temporary logging level
        """
        self.logger = logger
        self.level = level
        self.old_level = None

    def __enter__(self):
        """Save current level and set new level."""
        self.old_level = self.logger.level
        self.logger.setLevel(self.level)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original logging level."""
        self.logger.setLevel(self.old_level)
        return False


def configure_root_logger(level: int = logging.INFO):
    """
    Configure the root logger with basic settings.
    Useful for quick setup during development.

    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
