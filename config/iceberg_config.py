"""
Apache Iceberg configuration for the movie analytics pipeline.
Provides ACID transactions, schema evolution, and time travel capabilities.
"""
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class IcebergConfig:
    """Configuration class for Apache Iceberg table format."""
    
    def __init__(self):
        # Iceberg Catalog Configuration
        self.catalog_type = os.getenv('ICEBERG_CATALOG_TYPE', 'hive')  # Options: hive, hadoop, rest, glue
        self.catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'movie_catalog')
        
        # Warehouse location (MinIO/S3)
        self.warehouse_path = os.getenv('ICEBERG_WAREHOUSE_PATH', 's3a://iceberg-warehouse')
        
        # Hive Metastore Configuration (if using Hive catalog)
        self.hive_metastore_uri = os.getenv('HIVE_METASTORE_URI', 'thrift://hive-metastore:9083')
        
        # REST Catalog Configuration (if using REST catalog)
        self.rest_catalog_uri = os.getenv('ICEBERG_REST_CATALOG_URI', 'http://iceberg-rest:8181')
        
        # S3/MinIO Configuration for Iceberg
        self.s3_endpoint = os.getenv('ICEBERG_S3_ENDPOINT', 'http://minio:9000')
        self.s3_access_key = os.getenv('ICEBERG_S3_ACCESS_KEY', 'minioadmin')
        self.s3_secret_key = os.getenv('ICEBERG_S3_SECRET_KEY', 'minioadmin')
        self.s3_path_style_access = os.getenv('ICEBERG_S3_PATH_STYLE_ACCESS', 'true')
        
        # Table Configuration
        self.default_file_format = os.getenv('ICEBERG_DEFAULT_FILE_FORMAT', 'parquet')  # parquet, avro, orc
        self.compression_codec = os.getenv('ICEBERG_COMPRESSION_CODEC', 'snappy')  # snappy, gzip, zstd
        
        # Performance Tuning
        self.target_file_size_bytes = int(os.getenv('ICEBERG_TARGET_FILE_SIZE_BYTES', '134217728'))  # 128MB
        self.delete_mode = os.getenv('ICEBERG_DELETE_MODE', 'merge-on-read')  # copy-on-write, merge-on-read
        
        # Retention and Cleanup
        self.snapshot_retention_days = int(os.getenv('ICEBERG_SNAPSHOT_RETENTION_DAYS', '7'))
        self.metadata_delete_after_commit_enabled = os.getenv(
            'ICEBERG_METADATA_DELETE_AFTER_COMMIT_ENABLED', 'true'
        ).lower() == 'true'
        
        # Feature Flags
        self.enable_time_travel = os.getenv('ICEBERG_ENABLE_TIME_TRAVEL', 'true').lower() == 'true'
        self.enable_schema_evolution = os.getenv('ICEBERG_ENABLE_SCHEMA_EVOLUTION', 'true').lower() == 'true'
        self.enable_partition_evolution = os.getenv('ICEBERG_ENABLE_PARTITION_EVOLUTION', 'true').lower() == 'true'
        
        # Database/Schema Names for Layers
        self.bronze_database = os.getenv('ICEBERG_BRONZE_DATABASE', 'bronze')
        self.silver_database = os.getenv('ICEBERG_SILVER_DATABASE', 'silver')
        self.gold_database = os.getenv('ICEBERG_GOLD_DATABASE', 'gold')
    
    def get_spark_iceberg_configs(self) -> Dict[str, str]:
        """Get Spark configurations for Iceberg integration."""
        configs = {
            # Iceberg Catalog Configuration
            f"spark.sql.catalog.{self.catalog_name}": self._get_catalog_implementation(),
            f"spark.sql.catalog.{self.catalog_name}.warehouse": self.warehouse_path,
            f"spark.sql.catalog.{self.catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            
            # S3/MinIO Configuration
            f"spark.sql.catalog.{self.catalog_name}.s3.endpoint": self.s3_endpoint,
            f"spark.sql.catalog.{self.catalog_name}.s3.path-style-access": self.s3_path_style_access,
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": self.s3_path_style_access,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Iceberg Extensions
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            
            # Default Catalog
            "spark.sql.defaultCatalog": self.catalog_name,
            
            # Table Properties
            "spark.sql.iceberg.default-file-format": self.default_file_format,
            "spark.sql.iceberg.compression-codec": self.compression_codec,
            "spark.sql.iceberg.vectorization.enabled": "true",
            
            # Performance
            "spark.sql.iceberg.planning.mode": "distributed",
            "spark.sql.iceberg.delete.mode": self.delete_mode,
        }
        
        # Add catalog-specific configurations
        if self.catalog_type == 'hive':
            configs.update({
                f"spark.sql.catalog.{self.catalog_name}.uri": self.hive_metastore_uri,
                "spark.hadoop.hive.metastore.uris": self.hive_metastore_uri,
            })
        elif self.catalog_type == 'rest':
            configs.update({
                f"spark.sql.catalog.{self.catalog_name}.uri": self.rest_catalog_uri,
            })
        
        return configs
    
    def _get_catalog_implementation(self) -> str:
        """Get the appropriate catalog implementation class."""
        catalog_implementations = {
            'hive': 'org.apache.iceberg.spark.SparkCatalog',
            'hadoop': 'org.apache.iceberg.spark.SparkCatalog',
            'rest': 'org.apache.iceberg.spark.SparkCatalog',
            'glue': 'org.apache.iceberg.spark.SparkCatalog',
        }
        return catalog_implementations.get(self.catalog_type, 'org.apache.iceberg.spark.SparkCatalog')
    
    def get_table_properties(self, layer: str = None) -> Dict[str, str]:
        """Get default table properties for Iceberg tables."""
        properties = {
            'format-version': '2',  # Iceberg format version (2 for row-level operations)
            'write.format.default': self.default_file_format,
            'write.parquet.compression-codec': self.compression_codec,
            'write.target-file-size-bytes': str(self.target_file_size_bytes),
            'write.delete.mode': self.delete_mode,
            'write.metadata.delete-after-commit.enabled': str(self.metadata_delete_after_commit_enabled).lower(),
            'history.expire.max-snapshot-age-ms': str(self.snapshot_retention_days * 24 * 60 * 60 * 1000),
        }
        
        # Layer-specific properties
        if layer == 'bronze':
            properties.update({
                'write.metadata.metrics.default': 'full',  # Full metrics for bronze layer
                'write.parquet.row-group-size-bytes': '134217728',  # 128MB
            })
        elif layer == 'silver':
            properties.update({
                'write.metadata.metrics.default': 'counts',  # Count metrics for silver
                'write.parquet.row-group-size-bytes': '268435456',  # 256MB
            })
        elif layer == 'gold':
            properties.update({
                'write.metadata.metrics.default': 'truncate(16)',  # Truncated metrics for gold
                'write.parquet.row-group-size-bytes': '536870912',  # 512MB
                'commit.retry.num-retries': '3',
            })
        
        return properties
    
    def get_database_for_layer(self, layer: str) -> str:
        """Get the Iceberg database name for a specific layer."""
        layer_databases = {
            'bronze': self.bronze_database,
            'silver': self.silver_database,
            'gold': self.gold_database,
        }
        return layer_databases.get(layer.lower(), layer.lower())
    
    def get_table_identifier(self, layer: str, table_name: str) -> str:
        """Get fully qualified Iceberg table identifier."""
        database = self.get_database_for_layer(layer)
        return f"{self.catalog_name}.{database}.{table_name}"
    
    def validate(self) -> bool:
        """Validate Iceberg configuration."""
        required_configs = [
            ('ICEBERG_WAREHOUSE_PATH', self.warehouse_path),
            ('ICEBERG_S3_ENDPOINT', self.s3_endpoint),
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)
        
        if missing_configs:
            raise ValueError(f"Missing required Iceberg configuration: {', '.join(missing_configs)}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'catalog_type': self.catalog_type,
            'catalog_name': self.catalog_name,
            'warehouse_path': self.warehouse_path,
            'default_file_format': self.default_file_format,
            'compression_codec': self.compression_codec,
            'snapshot_retention_days': self.snapshot_retention_days,
            'bronze_database': self.bronze_database,
            'silver_database': self.silver_database,
            'gold_database': self.gold_database,
        }

# Global Iceberg configuration instance
iceberg_config = IcebergConfig()
