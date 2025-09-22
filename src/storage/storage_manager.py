"""
Storage layer management for the movie analytics pipeline.
Implements Bronze, Silver, Gold data lake architecture.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from minio import Minio
from minio.error import S3Error
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config

logger = logging.getLogger(__name__)

class StorageManager:
    """Manages data storage across Bronze, Silver, and Gold layers."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark = spark_session
        self.minio_client = None
        self.storage_config = config.minio_buckets
        self._initialize_storage()
    
    def _initialize_storage(self):
        """Initialize storage connections and create buckets if needed."""
        logger.info("Initializing storage layer...")
        
        try:
            # Initialize MinIO client
            self.minio_client = Minio(
                config.minio_endpoint,
                access_key=config.minio_access_key,
                secret_key=config.minio_secret_key,
                secure=False  # Set to True for HTTPS
            )
            
            # Create buckets if they don't exist
            for layer, bucket_name in self.storage_config.items():
                if not self.minio_client.bucket_exists(bucket_name):
                    self.minio_client.make_bucket(bucket_name)
                    logger.info(f"Created bucket: {bucket_name}")
                else:
                    logger.info(f"Bucket exists: {bucket_name}")
            
            logger.info("Storage layer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize storage: {e}")
            raise
    
    def get_s3_path(self, layer: str, data_type: str, partition_spec: Optional[Dict] = None) -> str:
        """Generate S3 path for data storage."""
        bucket = self.storage_config[layer]
        base_path = f"s3a://{bucket}/{data_type}"
        
        if partition_spec:
            partition_path = "/".join([f"{k}={v}" for k, v in partition_spec.items()])
            return f"{base_path}/{partition_path}"
        
        return base_path
    
    def write_to_bronze(self, df: DataFrame, data_type: str, partition_cols: List[str] = None):
        """Write raw data to Bronze layer."""
        logger.info(f"Writing {data_type} to Bronze layer...")
        
        path = self.get_s3_path('bronze', data_type)
        
        writer = df.write.mode("append").format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.option("path", path).save()
        logger.info(f"Successfully wrote to Bronze: {path}")
    
    def write_to_silver(self, df: DataFrame, data_type: str, partition_cols: List[str] = None):
        """Write cleaned and enriched data to Silver layer."""
        logger.info(f"Writing {data_type} to Silver layer...")
        
        path = self.get_s3_path('silver', data_type)
        
        writer = df.write.mode("append").format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.option("path", path).save()
        logger.info(f"Successfully wrote to Silver: {path}")
    
    def write_to_gold(self, df: DataFrame, data_type: str, partition_cols: List[str] = None):
        """Write aggregated business-ready data to Gold layer."""
        logger.info(f"Writing {data_type} to Gold layer...")
        
        path = self.get_s3_path('gold', data_type)
        
        writer = df.write.mode("append").format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.option("path", path).save()
        logger.info(f"Successfully wrote to Gold: {path}")
    
    def read_from_layer(self, layer: str, data_type: str, 
                       filters: Optional[Dict] = None) -> DataFrame:
        """Read data from specified layer."""
        if not self.spark:
            raise RuntimeError("Spark session not available")
        
        path = self.get_s3_path(layer, data_type)
        
        logger.info(f"Reading {data_type} from {layer} layer: {path}")
        
        df = self.spark.read.format("parquet").load(path)
        
        # Apply filters if provided
        if filters:
            for column, value in filters.items():
                df = df.filter(col(column) == value)
        
        return df
    
    def list_partitions(self, layer: str, data_type: str) -> List[str]:
        """List all partitions for a given data type in a layer."""
        try:
            bucket_name = self.storage_config[layer]
            prefix = f"{data_type}/"
            
            objects = self.minio_client.list_objects(
                bucket_name, prefix=prefix, recursive=True
            )
            
            partitions = set()
            for obj in objects:
                # Extract partition info from object path
                path_parts = obj.object_name.split('/')
                if len(path_parts) > 2:  # Has partition directories
                    partition_parts = []
                    for part in path_parts[1:-1]:  # Skip data_type and filename
                        if '=' in part:
                            partition_parts.append(part)
                    if partition_parts:
                        partitions.add('/'.join(partition_parts))
            
            return list(partitions)
        
        except Exception as e:
            logger.error(f"Error listing partitions for {layer}/{data_type}: {e}")
            return []
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for all layers."""
        stats = {
            'layers': {},
            'total_size_mb': 0,
            'total_objects': 0
        }
        
        try:
            for layer, bucket_name in self.storage_config.items():
                layer_stats = {
                    'bucket': bucket_name,
                    'size_mb': 0,
                    'object_count': 0,
                    'data_types': {}
                }
                
                # List all objects in bucket
                objects = self.minio_client.list_objects(bucket_name, recursive=True)
                
                for obj in objects:
                    layer_stats['size_mb'] += obj.size / (1024 * 1024)  # Convert to MB
                    layer_stats['object_count'] += 1
                    
                    # Extract data type from path
                    data_type = obj.object_name.split('/')[0]
                    if data_type not in layer_stats['data_types']:
                        layer_stats['data_types'][data_type] = {
                            'size_mb': 0,
                            'object_count': 0
                        }
                    
                    layer_stats['data_types'][data_type]['size_mb'] += obj.size / (1024 * 1024)
                    layer_stats['data_types'][data_type]['object_count'] += 1
                
                stats['layers'][layer] = layer_stats
                stats['total_size_mb'] += layer_stats['size_mb']
                stats['total_objects'] += layer_stats['object_count']
        
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
            stats['error'] = str(e)
        
        return stats
    
    def cleanup_old_data(self, layer: str, data_type: str, days_to_keep: int = 30):
        """Clean up old data based on retention policy."""
        logger.info(f"Cleaning up old data for {layer}/{data_type} (keeping {days_to_keep} days)")
        
        try:
            bucket_name = self.storage_config[layer]
            prefix = f"{data_type}/"
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            objects_to_delete = []
            objects = self.minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
            
            for obj in objects:
                if obj.last_modified < cutoff_date:
                    objects_to_delete.append(obj.object_name)
            
            if objects_to_delete:
                for obj_name in objects_to_delete:
                    self.minio_client.remove_object(bucket_name, obj_name)
                
                logger.info(f"Deleted {len(objects_to_delete)} old objects from {layer}/{data_type}")
            else:
                logger.info(f"No old data found to clean up in {layer}/{data_type}")
        
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")

class DataLakeOptimizer:
    """Optimizes data lake performance through compaction and optimization."""
    
    def __init__(self, spark_session: SparkSession, storage_manager: StorageManager):
        self.spark = spark_session
        self.storage_manager = storage_manager
    
    def compact_small_files(self, layer: str, data_type: str, 
                           target_file_size_mb: int = 128):
        """Compact small files to improve query performance."""
        logger.info(f"Compacting small files for {layer}/{data_type}")
        
        try:
            # Read all data
            df = self.storage_manager.read_from_layer(layer, data_type)
            
            # Repartition based on target file size
            total_size_mb = df.count() * 0.001  # Rough estimate
            num_partitions = max(1, int(total_size_mb / target_file_size_mb))
            
            compacted_df = df.repartition(num_partitions)
            
            # Write back with overwrite mode
            path = self.storage_manager.get_s3_path(layer, data_type)
            compacted_df.write.mode("overwrite").format("parquet").save(path)
            
            logger.info(f"Compaction completed for {layer}/{data_type}")
        
        except Exception as e:
            logger.error(f"Error during compaction: {e}")
    
    def optimize_partitions(self, layer: str, data_type: str, 
                           partition_cols: List[str]):
        """Optimize partition structure for better query performance."""
        logger.info(f"Optimizing partitions for {layer}/{data_type}")
        
        try:
            df = self.storage_manager.read_from_layer(layer, data_type)
            
            # Analyze partition distribution
            partition_stats = df.groupBy(*partition_cols).count().collect()
            
            # Log partition statistics
            for stat in partition_stats:
                partition_values = [str(stat[col]) for col in partition_cols]
                logger.info(f"Partition {'/'.join(partition_values)}: {stat['count']} records")
            
            # Re-write with optimized partitioning
            path = self.storage_manager.get_s3_path(layer, data_type)
            df.write.mode("overwrite").partitionBy(*partition_cols).format("parquet").save(path)
            
            logger.info(f"Partition optimization completed for {layer}/{data_type}")
        
        except Exception as e:
            logger.error(f"Error during partition optimization: {e}")
    
    def create_summary_tables(self):
        """Create summary tables for Gold layer analytics."""
        logger.info("Creating summary tables for Gold layer...")
        
        try:
            # Movie summary table
            movies_df = self.storage_manager.read_from_layer('silver', 'movies')
            
            movie_summary = movies_df.groupBy(
                "release_year", "language", "popularity_tier"
            ).agg(
                count("*").alias("movie_count"),
                avg("vote_average").alias("avg_rating"),
                avg("popularity").alias("avg_popularity"),
                sum("revenue").alias("total_revenue"),
                sum("budget").alias("total_budget")
            ).withColumn(
                "avg_profit_margin",
                when(col("total_budget") > 0, 
                     (col("total_revenue") - col("total_budget")) / col("total_budget"))
                .otherwise(0)
            )
            
            self.storage_manager.write_to_gold(
                movie_summary, 
                "movie_summary_by_year_language",
                ["release_year", "language"]
            )
            
            # Genre popularity summary
            genres_exploded = movies_df.select(
                "movie_id", "release_year", "vote_average", "popularity",
                explode("genres").alias("genre")
            ).select(
                "movie_id", "release_year", "vote_average", "popularity",
                col("genre.id").alias("genre_id"),
                col("genre.name").alias("genre_name")
            )
            
            genre_summary = genres_exploded.groupBy(
                "release_year", "genre_name"
            ).agg(
                count("*").alias("movie_count"),
                avg("vote_average").alias("avg_rating"),
                avg("popularity").alias("avg_popularity")
            ).filter(col("movie_count") >= 5)  # Filter out genres with few movies
            
            self.storage_manager.write_to_gold(
                genre_summary,
                "genre_summary_by_year",
                ["release_year"]
            )
            
            logger.info("Summary tables created successfully")
        
        except Exception as e:
            logger.error(f"Error creating summary tables: {e}")

class StorageHealthChecker:
    """Monitors storage health and data quality."""
    
    def __init__(self, storage_manager: StorageManager):
        self.storage_manager = storage_manager
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness across all layers."""
        freshness_report = {}
        
        for layer in ['bronze', 'silver', 'gold']:
            layer_report = {}
            bucket_name = self.storage_manager.storage_config[layer]
            
            try:
                objects = self.storage_manager.minio_client.list_objects(
                    bucket_name, recursive=True
                )
                
                latest_timestamps = {}
                for obj in objects:
                    data_type = obj.object_name.split('/')[0]
                    if data_type not in latest_timestamps:
                        latest_timestamps[data_type] = obj.last_modified
                    else:
                        if obj.last_modified > latest_timestamps[data_type]:
                            latest_timestamps[data_type] = obj.last_modified
                
                for data_type, timestamp in latest_timestamps.items():
                    hours_old = (datetime.now() - timestamp.replace(tzinfo=None)).total_seconds() / 3600
                    layer_report[data_type] = {
                        'last_updated': timestamp.isoformat(),
                        'hours_old': round(hours_old, 2),
                        'is_stale': hours_old > 24  # Consider stale if older than 24 hours
                    }
                
                freshness_report[layer] = layer_report
            
            except Exception as e:
                freshness_report[layer] = {'error': str(e)}
        
        return freshness_report
    
    def validate_data_integrity(self, layer: str, data_type: str) -> Dict[str, Any]:
        """Validate data integrity for a specific dataset."""
        if not self.storage_manager.spark:
            return {'error': 'Spark session not available'}
        
        try:
            df = self.storage_manager.read_from_layer(layer, data_type)
            
            integrity_report = {
                'total_records': df.count(),
                'null_counts': {},
                'duplicate_records': 0,
                'schema_validation': True
            }
            
            # Check for null values in key columns
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    integrity_report['null_counts'][column] = null_count
            
            # Check for duplicates (if id column exists)
            if 'id' in df.columns or 'movie_id' in df.columns:
                id_col = 'id' if 'id' in df.columns else 'movie_id'
                unique_count = df.select(id_col).distinct().count()
                total_count = df.count()
                integrity_report['duplicate_records'] = total_count - unique_count
            
            return integrity_report
        
        except Exception as e:
            return {'error': str(e)}