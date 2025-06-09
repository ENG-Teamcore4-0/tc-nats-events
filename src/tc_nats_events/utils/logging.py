"""
Logging Configuration
=====================

Structured logging setup for TC NATS Events.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields from record.__dict__
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
                "exc_info",
                "exc_text",
                "stack_info",
                "asctime",
            ]:
                log_data[key] = value

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class ColoredConsoleFormatter(logging.Formatter):
    """Colored formatter for console output."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        levelname = record.levelname

        # Color the level name
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"

        # Format the message
        formatted = super().format(record)

        # Reset levelname for other handlers
        record.levelname = levelname

        return formatted


def setup_logging(
    level: str = "INFO",
    structured: bool = False,
    log_file: Optional[str] = None,
    service_name: Optional[str] = None,
) -> None:
    """
    Setup logging configuration for TC NATS Events.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        structured: Use structured JSON logging
        log_file: Optional file path for logging
        service_name: Optional service name to include in logs
    """
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)

    if structured:
        console_formatter = StructuredFormatter()
    else:
        console_format = (
            "%(asctime)s | %(levelname)-8s | %(name)-20s | "
            "%(funcName)-15s | %(message)s"
        )
        console_formatter = ColoredConsoleFormatter(console_format)

    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_formatter = (
            StructuredFormatter()
            if structured
            else logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s"
            )
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Package logger configuration
    package_logger = logging.getLogger("tc_nats_events")
    package_logger.setLevel(getattr(logging, level.upper()))

    # Add service name to all logs if provided
    if service_name:

        class ServiceFilter(logging.Filter):
            def filter(self, record):
                record.service_name = service_name
                return True

        for handler in root_logger.handlers:
            handler.addFilter(ServiceFilter())

    # Suppress noisy loggers
    logging.getLogger("nats").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    package_logger.info(
        f"Logging configured: level={level}, structured={structured}, "
        f"service_name={service_name}"
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LogContext:
    """Context manager for adding context to log messages."""

    def __init__(self, logger: logging.Logger, **context):
        """
        Initialize log context.

        Args:
            logger: Logger instance
            **context: Context key-value pairs
        """
        self.logger = logger
        self.context = context
        self._old_factory = None

    def __enter__(self):
        """Enter context."""
        old_factory = logging.getLogRecordFactory()
        context = self.context

        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            for key, value in context.items():
                setattr(record, key, value)
            return record

        logging.setLogRecordFactory(record_factory)
        self._old_factory = old_factory
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        if self._old_factory:
            logging.setLogRecordFactory(self._old_factory)
