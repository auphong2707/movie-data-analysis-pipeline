"""
Structured logging utility for Batch Layer jobs.

Provides JSON-formatted logging with context information for better observability.
"""

import logging
import json
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any
from functools import wraps
import traceback


class JsonFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    
    Outputs logs as JSON with standard fields:
    - timestamp: ISO 8601 timestamp
    - level: Log level (INFO, ERROR, etc.)
    - message: Log message
    - logger: Logger name
    - context: Additional contextual information
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception information if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields from record
        if hasattr(record, 'context'):
            log_data["context"] = record.context
        
        return json.dumps(log_data)


def get_logger(
    name: str,
    level: Optional[str] = None,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Get a configured logger with JSON formatting.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for file logging
    
    Returns:
        Configured logger instance
    
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started", extra={"context": {"job": "bronze_ingest"}})
    """
    logger = logging.getLogger(name)
    
    # Set level from argument or environment
    if level is None:
        level = os.getenv('LOG_LEVEL', 'INFO')
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler with JSON formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(console_handler)
    
    # Optional file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def log_execution(logger: logging.Logger, job_name: str):
    """
    Decorator to log function execution with timing and error handling.
    
    Args:
        logger: Logger instance
        job_name: Name of the job for context
    
    Example:
        >>> logger = get_logger(__name__)
        >>> @log_execution(logger, "bronze_ingest")
        >>> def ingest_data():
        >>>     # ... processing logic
        >>>     pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            context = {
                "job_name": job_name,
                "function": func.__name__,
                "start_time": start_time.isoformat() + "Z"
            }
            
            logger.info(
                f"Starting {job_name} - {func.__name__}",
                extra={"context": context}
            )
            
            try:
                result = func(*args, **kwargs)
                
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                context.update({
                    "end_time": end_time.isoformat() + "Z",
                    "duration_seconds": duration,
                    "status": "success"
                })
                
                logger.info(
                    f"Completed {job_name} - {func.__name__} in {duration:.2f}s",
                    extra={"context": context}
                )
                
                return result
                
            except Exception as e:
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                context.update({
                    "end_time": end_time.isoformat() + "Z",
                    "duration_seconds": duration,
                    "status": "failed",
                    "error": str(e)
                })
                
                logger.error(
                    f"Failed {job_name} - {func.__name__}: {str(e)}",
                    extra={"context": context},
                    exc_info=True
                )
                
                raise
        
        return wrapper
    return decorator


class JobMetrics:
    """
    Track and log job metrics for monitoring.
    
    Example:
        >>> metrics = JobMetrics("bronze_ingest")
        >>> metrics.add_metric("records_processed", 1000)
        >>> metrics.add_metric("files_written", 5)
        >>> metrics.log(logger)
    """
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.metrics: Dict[str, Any] = {}
        self.start_time = datetime.utcnow()
    
    def add_metric(self, key: str, value: Any):
        """Add a metric."""
        self.metrics[key] = value
    
    def increment(self, key: str, value: int = 1):
        """Increment a counter metric."""
        self.metrics[key] = self.metrics.get(key, 0) + value
    
    def log(self, logger: logging.Logger):
        """Log all metrics."""
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()
        
        context = {
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat() + "Z",
            "end_time": end_time.isoformat() + "Z",
            "duration_seconds": duration,
            "metrics": self.metrics
        }
        
        logger.info(
            f"Job metrics for {self.job_name}",
            extra={"context": context}
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Return metrics as dictionary."""
        return {
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat() + "Z",
            "metrics": self.metrics
        }
