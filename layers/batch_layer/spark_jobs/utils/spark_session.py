"""
Spark Session Builder with MinIO/S3A configuration.

Provides a standardized way to create Spark sessions for batch processing
with proper S3A configuration for MinIO access.
"""

import os
from typing import Optional, Dict
from pyspark.sql import SparkSession


class SparkSessionBuilder:
    """
    Builder class for creating configured Spark sessions.
    
    Features:
    - S3A configuration for MinIO access
    - Optimized Spark SQL settings
    - Memory and executor configuration
    - Delta Lake / Iceberg support (optional)
    """
    
    def __init__(self, app_name: str = "movie-analytics-batch"):
        self.app_name = app_name
        self.configs = {}
        self._load_default_configs()
    
    def _load_default_configs(self):
        """Load default Spark configurations from environment."""
        # S3A / MinIO Configuration
        s3_endpoint = os.getenv('S3_ENDPOINT', os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
        s3_access_key = os.getenv('S3_ACCESS_KEY', os.getenv('MINIO_ACCESS_KEY', 'minioadmin'))
        s3_secret_key = os.getenv('S3_SECRET_KEY', os.getenv('MINIO_SECRET_KEY', 'minioadmin'))
        
        self.configs = {
            # S3A Configuration for MinIO
            "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.attempts.maximum": "3",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
            "spark.hadoop.fs.s3a.connection.timeout": "200000",
            
            # Spark SQL Optimization
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128 MB
            
            # Memory Configuration
            "spark.executor.memory": os.getenv('SPARK_EXECUTOR_MEMORY', '4g'),
            "spark.driver.memory": os.getenv('SPARK_DRIVER_MEMORY', '2g'),
            "spark.executor.memoryOverhead": "1g",
            
            # Serialization
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "512m",
            
            # Dynamic Allocation (optional)
            "spark.dynamicAllocation.enabled": "false",
            
            # Checkpoint and temp directories
            "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        }
    
    def with_master(self, master_url: Optional[str] = None) -> 'SparkSessionBuilder':
        """Set Spark master URL. Defaults to local mode if no cluster available."""
        if master_url is None:
            # Try to get from environment, default to local mode with all cores
            master_url = os.getenv('SPARK_MASTER_URL', 'local[*]')
        self.master_url = master_url
        return self
    
    def with_config(self, key: str, value: str) -> 'SparkSessionBuilder':
        """Add custom configuration."""
        self.configs[key] = value
        return self
    
    def with_configs(self, configs: Dict[str, str]) -> 'SparkSessionBuilder':
        """Add multiple configurations."""
        self.configs.update(configs)
        return self
    
    def with_iceberg(self) -> 'SparkSessionBuilder':
        """Enable Apache Iceberg support."""
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'movie_catalog')
        warehouse_path = os.getenv('ICEBERG_WAREHOUSE_PATH', 's3a://iceberg-warehouse')
        s3_endpoint = os.getenv('S3_ENDPOINT', os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
        
        iceberg_configs = {
            # Iceberg Catalog
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": "hadoop",
            f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
            f"spark.sql.catalog.{catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"spark.sql.catalog.{catalog_name}.s3.endpoint": s3_endpoint,
            f"spark.sql.catalog.{catalog_name}.s3.path-style-access": "true",
            
            # Iceberg Extensions
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": catalog_name,
        }
        
        self.configs.update(iceberg_configs)
        return self
    
    def build(self) -> SparkSession:
        """Build and return the configured Spark session."""
        builder = SparkSession.builder.appName(self.app_name)
        
        # Set master if specified
        if hasattr(self, 'master_url'):
            builder = builder.master(self.master_url)
        
        # Apply all configurations
        for key, value in self.configs.items():
            builder = builder.config(key, value)
        
        # Get or create session
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(os.getenv('SPARK_LOG_LEVEL', 'WARN'))
        
        return spark


def get_spark_session(
    app_name: str = "movie-analytics-batch",
    master_url: Optional[str] = None,
    enable_iceberg: bool = False,
    additional_configs: Optional[Dict[str, str]] = None
) -> SparkSession:
    """
    Convenience function to get a configured Spark session.
    
    Args:
        app_name: Name of the Spark application
        master_url: Spark master URL (default: from SPARK_MASTER_URL env)
        enable_iceberg: Whether to enable Iceberg support
        additional_configs: Additional Spark configurations
    
    Returns:
        Configured SparkSession
    
    Example:
        >>> spark = get_spark_session("bronze_ingest")
        >>> df = spark.read.parquet("s3a://bronze-data/movies/")
    """
    builder = SparkSessionBuilder(app_name).with_master(master_url)
    
    if enable_iceberg:
        builder = builder.with_iceberg()
    
    if additional_configs:
        builder = builder.with_configs(additional_configs)
    
    return builder.build()


def stop_spark_session(spark: SparkSession):
    """Safely stop Spark session."""
    if spark:
        spark.stop()
