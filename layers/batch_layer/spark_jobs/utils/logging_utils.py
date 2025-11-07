"""
Logging utilities for batch layer.
"""
import logging
import json
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import DataFrame


def get_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Get a configured logger with JSON formatting.
    
    Args:
        name: Logger name (typically __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting batch job", extra={"job_id": "123"})
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler with JSON formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # JSON formatter
    formatter = JSONFormatter()
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, 'job_id'):
            log_data['job_id'] = record.job_id
        if hasattr(record, 'execution_date'):
            log_data['execution_date'] = record.execution_date
        if hasattr(record, 'row_count'):
            log_data['row_count'] = record.row_count
        if hasattr(record, 'duration'):
            log_data['duration'] = record.duration
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def log_dataframe_info(
    logger: logging.Logger,
    df: DataFrame,
    name: str,
    sample: bool = False,
    extra: Optional[Dict[str, Any]] = None
):
    """
    Log DataFrame information (schema, count, sample).
    
    Args:
        logger: Logger instance
        df: DataFrame to log
        name: Name/description of the DataFrame
        sample: Whether to log sample data
        extra: Extra fields to include in log
        
    Example:
        >>> logger = get_logger(__name__)
        >>> log_dataframe_info(logger, df, "movies_bronze", sample=True)
    """
    try:
        count = df.count()
        
        log_msg = {
            "dataframe": name,
            "row_count": count,
            "columns": df.columns,
            "partitions": df.rdd.getNumPartitions()
        }
        
        if extra:
            log_msg.update(extra)
        
        logger.info(f"DataFrame info: {name}", extra=log_msg)
        
        # Log schema
        logger.debug(f"Schema for {name}:\n{df.schema.simpleString()}")
        
        # Log sample if requested
        if sample and count > 0:
            sample_data = df.limit(5).toPandas().to_dict('records')
            logger.debug(f"Sample data for {name}: {json.dumps(sample_data, default=str)}")
            
    except Exception as e:
        logger.error(f"Failed to log DataFrame info for {name}: {e}")


class JobMetrics:
    """
    Track and log job metrics.
    """
    
    def __init__(self, job_name: str, logger: logging.Logger):
        """
        Initialize job metrics tracker.
        
        Args:
            job_name: Name of the job
            logger: Logger instance
        """
        self.job_name = job_name
        self.logger = logger
        self.start_time = datetime.now()
        self.metrics = {
            "job_name": job_name,
            "start_time": self.start_time.isoformat()
        }
    
    def add_metric(self, key: str, value: Any):
        """
        Add a metric.
        
        Args:
            key: Metric key
            value: Metric value
        """
        self.metrics[key] = value
    
    def increment(self, key: str, amount: int = 1):
        """
        Increment a counter metric.
        
        Args:
            key: Metric key
            amount: Amount to increment
        """
        self.metrics[key] = self.metrics.get(key, 0) + amount
    
    def finish(self, success: bool = True):
        """
        Finish job and log metrics.
        
        Args:
            success: Whether job succeeded
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self.metrics.update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "success": success
        })
        
        if success:
            self.logger.info(f"Job {self.job_name} completed successfully", extra=self.metrics)
        else:
            self.logger.error(f"Job {self.job_name} failed", extra=self.metrics)
        
        return self.metrics
