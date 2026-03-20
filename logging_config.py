"""
Centralized logging configuration for all services
This module provides optimized logging settings to prevent log bloat
with enhanced features for monitoring, metrics, and structured logging
"""

import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any
import traceback

# Ensure logs directory exists
LOG_DIR = os.environ.get('LOG_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs'))
os.makedirs(LOG_DIR, exist_ok=True)

# Configuration constants
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB per file
BACKUP_COUNT = 3  # Keep 3 backup files (increased from 2)
LOG_LEVEL = logging.INFO

# Enhanced configuration options
ENABLE_JSON_LOGGING = os.environ.get('JSON_LOGS', 'false').lower() == 'true'
ENABLE_METRICS = True  # Track performance metrics
CONSOLE_LOG_LEVEL = logging.WARNING  # Console verbosity


class StructuredFormatter(logging.Formatter):
    """
    Enhanced formatter that supports both standard and JSON output
    Includes context information and structured fields
    """
    def __init__(self, json_format=False, include_context=True):
        super().__init__()
        self.json_format = json_format
        self.include_context = include_context

    def format(self, record):
        if self.json_format:
            # JSON structured logging
            log_data = {
                'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'module': record.module,
                'function': record.funcName,
                'line': record.lineno,
            }

            # Add extra context fields if available
            if hasattr(record, 'context'):
                log_data['context'] = record.context
            if hasattr(record, 'duration'):
                log_data['duration_ms'] = record.duration
            if hasattr(record, 'operation'):
                log_data['operation'] = record.operation

            # Add exception info if present
            if record.exc_info:
                log_data['exception'] = {
                    'type': record.exc_info[0].__name__,
                    'message': str(record.exc_info[1]),
                    'traceback': traceback.format_exception(*record.exc_info)
                }

            return json.dumps(log_data)
        else:
            # Standard formatted logging with enhanced information
            timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
            base_msg = f'{timestamp} - {record.levelname:8s} - [{record.name}] - {record.getMessage()}'

            # Add context information if available
            if self.include_context:
                extras = []
                if hasattr(record, 'operation'):
                    extras.append(f"op={record.operation}")
                if hasattr(record, 'duration'):
                    extras.append(f"duration={record.duration:.2f}ms")
                if hasattr(record, 'context'):
                    extras.append(f"ctx={record.context}")

                if extras:
                    base_msg += f" | {' '.join(extras)}"

            # Add exception info if present
            if record.exc_info:
                base_msg += '\n' + ''.join(traceback.format_exception(*record.exc_info))

            return base_msg


class MetricsLogger:
    """
    Track and log performance metrics for operations
    Usage:
        metrics = MetricsLogger(logger, 'operation_name')
        with metrics:
            # do work
            pass
        # Automatically logs duration
    """
    def __init__(self, logger, operation: str, context: Optional[Dict[str, Any]] = None):
        self.logger = logger
        self.operation = operation
        self.context = context or {}
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"Starting {self.operation}", extra={
            'operation': self.operation,
            'context': self.context
        })
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000

        if exc_type is None:
            self.logger.info(f"Completed {self.operation}", extra={
                'operation': self.operation,
                'duration': duration_ms,
                'context': self.context,
                'status': 'success'
            })
        else:
            self.logger.error(f"Failed {self.operation}: {exc_val}", extra={
                'operation': self.operation,
                'duration': duration_ms,
                'context': self.context,
                'status': 'error'
            }, exc_info=(exc_type, exc_val, exc_tb))

        return False  # Don't suppress exceptions


def get_logger(name, log_file=None, enable_json=None, enable_metrics=None):
    """
    Get a configured logger with rotation and efficient settings

    Args:
        name: Logger name (usually __name__)
        log_file: Optional specific log file name (defaults to {name}.log)
        enable_json: Override JSON logging setting (default: use ENABLE_JSON_LOGGING)
        enable_metrics: Override metrics setting (default: use ENABLE_METRICS)

    Returns:
        Configured logger instance with optional metrics logger
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)

    # Determine log file name
    if log_file is None:
        log_file = f"{name.lower().replace('.', '_')}.log"

    log_path = os.path.join(LOG_DIR, log_file)

    # Determine if JSON logging is enabled
    use_json = enable_json if enable_json is not None else ENABLE_JSON_LOGGING

    # Create enhanced formatter
    file_formatter = StructuredFormatter(json_format=use_json, include_context=True)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler (less verbose, always human-readable)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(CONSOLE_LOG_LEVEL)
    console_formatter = StructuredFormatter(json_format=False, include_context=False)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    # Attach metrics logger helper if enabled
    use_metrics = enable_metrics if enable_metrics is not None else ENABLE_METRICS
    if use_metrics:
        logger.metrics = lambda operation, context=None: MetricsLogger(logger, operation, context)

    return logger

def setup_service_logging(service_name, log_level=None, enable_json=None):
    """
    Setup logging for a specific service with appropriate handlers

    Args:
        service_name: Name of the service (e.g., 'rss_monitor', 'gmail_monitor')
        log_level: Optional log level override (default: INFO)
        enable_json: Optional JSON logging override

    Returns:
        Configured logger instance
    """
    # Configure root logger to prevent library logs from being too verbose
    logging.getLogger().setLevel(logging.WARNING)

    # Configure specific loggers that tend to be noisy
    noisy_loggers = [
        'urllib3', 'requests', 'httplib2', 'googleapiclient',
        'cloudscraper', 'feedparser', 'PIL', 'httpx', 'asyncio',
        'playwright', 'charset_normalizer'
    ]

    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # Get configured service logger
    logger = get_logger(service_name, f"{service_name}.log", enable_json=enable_json)

    # Override log level if specified
    if log_level is not None:
        logger.setLevel(log_level)

    # Log startup message
    logger.info(f"=== {service_name} logging initialized ===", extra={
        'operation': 'startup',
        'context': {
            'service': service_name,
            'log_level': logging.getLevelName(logger.level),
            'json_logging': enable_json or ENABLE_JSON_LOGGING,
            'log_dir': LOG_DIR
        }
    })

    return logger


# Convenience function to get metrics logger
def get_metrics_logger(logger, operation, context=None):
    """
    Get a metrics logger for tracking operation performance

    Args:
        logger: The logger instance to use
        operation: Name of the operation being tracked
        context: Optional context dictionary

    Returns:
        MetricsLogger context manager
    """
    return MetricsLogger(logger, operation, context) 