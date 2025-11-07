"""
Spark session management utilities.
"""
import os
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
from config.config import config


def configure_spark(app_name: str, conf_overrides: Optional[Dict[str, Any]] = None) -> SparkConf:
    """
    Create Spark configuration with best practices for batch processing.
    
    Args:
        app_name: Name of the Spark application
        conf_overrides: Optional dictionary of configuration overrides
        
    Returns:
        Configured SparkConf object
    """
    conf = SparkConf()
    
    # Basic configuration
    conf.setAppName(app_name)
    conf.setMaster(config.spark_master_url)
    
    # Memory configuration
    conf.set("spark.executor.memory", config.spark_executor_memory)
    conf.set("spark.driver.memory", config.spark_driver_memory)
    conf.set("spark.executor.cores", str(config.spark_executor_cores))
    
    # Performance tuning
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.broadcastTimeout", "300")
    
    # Serialization
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "false")
    
    # Shuffle and I/O
    conf.set("spark.sql.shuffle.partitions", "200")
    conf.set("spark.default.parallelism", "200")
    conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    
    # Parquet optimization
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    conf.set("spark.sql.parquet.mergeSchema", "false")
    conf.set("spark.sql.parquet.filterPushdown", "true")
    
    # HDFS configuration
    conf.set("spark.hadoop.fs.defaultFS", config.hdfs_namenode)
    conf.set("spark.hadoop.dfs.replication", str(config.hdfs_replication))
    
    # Dynamic allocation (if available)
    conf.set("spark.dynamicAllocation.enabled", "false")
    
    # Apply overrides
    if conf_overrides:
        for key, value in conf_overrides.items():
            conf.set(key, str(value))
    
    return conf


def get_spark_session(
    app_name: str,
    conf_overrides: Optional[Dict[str, Any]] = None,
    enable_hive: bool = False
) -> SparkSession:
    """
    Get or create a Spark session with optimized configuration.
    
    Args:
        app_name: Name of the Spark application
        conf_overrides: Optional dictionary of configuration overrides
        enable_hive: Whether to enable Hive support
        
    Returns:
        Configured SparkSession
        
    Example:
        >>> spark = get_spark_session("bronze_ingestion")
        >>> df = spark.read.parquet("hdfs://...")
    """
    spark_conf = configure_spark(app_name, conf_overrides)
    
    builder = SparkSession.builder.config(conf=spark_conf)
    
    if enable_hive:
        builder = builder.enableHiveSupport()
    
    spark = builder.getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark: SparkSession):
    """
    Gracefully stop Spark session.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()
