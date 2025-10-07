"""
Iceberg-based storage manager for the movie analytics pipeline.
Provides ACID transactions, schema evolution, time travel, and improved performance.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from config.iceberg_config import iceberg_config

logger = logging.getLogger(__name__)


class IcebergStorageManager:
    """Manages data storage using Apache Iceberg table format."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.catalog_name = iceberg_config.catalog_name
        self._initialize_databases()
    
    def _initialize_databases(self):
        """Initialize Iceberg databases for Bronze, Silver, and Gold layers."""
        logger.info("Initializing Iceberg databases...")
        
        databases = [
            iceberg_config.bronze_database,
            iceberg_config.silver_database,
            iceberg_config.gold_database,
        ]
        
        for database in databases:
            try:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{database}")
                logger.info(f"Database initialized: {self.catalog_name}.{database}")
            except Exception as e:
                logger.error(f"Failed to create database {database}: {e}")
                raise
    
    def get_table_identifier(self, layer: str, table_name: str) -> str:
        """Get fully qualified Iceberg table identifier."""
        return iceberg_config.get_table_identifier(layer, table_name)
    
    def create_table(self, layer: str, table_name: str, df: DataFrame, 
                    partition_cols: List[str] = None, overwrite: bool = False):
        """Create an Iceberg table from a DataFrame."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Creating Iceberg table: {table_id}")
        
        try:
            # Check if table exists
            table_exists = self._table_exists(layer, table_name)
            
            if table_exists and not overwrite:
                logger.info(f"Table {table_id} already exists. Skipping creation.")
                return
            
            # Create table with partitioning
            writer = df.writeTo(table_id)
            
            # Set table properties
            table_props = iceberg_config.get_table_properties(layer)
            for key, value in table_props.items():
                writer = writer.tableProperty(key, value)
            
            # Add partitioning
            if partition_cols:
                for col in partition_cols:
                    writer = writer.partitionedBy(col)
            
            # Create or replace table
            if overwrite and table_exists:
                writer.createOrReplace()
                logger.info(f"Table replaced: {table_id}")
            else:
                writer.create()
                logger.info(f"Table created: {table_id}")
        
        except Exception as e:
            logger.error(f"Failed to create table {table_id}: {e}")
            raise
    
    def write_to_bronze(self, df: DataFrame, table_name: str, 
                       partition_cols: List[str] = None, mode: str = "append"):
        """Write data to Bronze layer using Iceberg."""
        self._write_to_layer('bronze', df, table_name, partition_cols, mode)
    
    def write_to_silver(self, df: DataFrame, table_name: str,
                       partition_cols: List[str] = None, mode: str = "append"):
        """Write data to Silver layer using Iceberg."""
        self._write_to_layer('silver', df, table_name, partition_cols, mode)
    
    def write_to_gold(self, df: DataFrame, table_name: str,
                     partition_cols: List[str] = None, mode: str = "append"):
        """Write data to Gold layer using Iceberg."""
        self._write_to_layer('gold', df, table_name, partition_cols, mode)
    
    def _write_to_layer(self, layer: str, df: DataFrame, table_name: str,
                       partition_cols: List[str] = None, mode: str = "append"):
        """Internal method to write data to any layer."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Writing to {layer} layer: {table_id}")
        
        try:
            # Check if table exists
            if not self._table_exists(layer, table_name):
                logger.info(f"Table {table_id} does not exist. Creating it...")
                self.create_table(layer, table_name, df, partition_cols)
            
            # Write data
            if mode == "append":
                df.writeTo(table_id).append()
            elif mode == "overwrite":
                df.writeTo(table_id).overwritePartitions()
            elif mode == "upsert":
                # For upsert, we need to use merge operation
                self._upsert_data(layer, table_name, df)
            else:
                raise ValueError(f"Unsupported write mode: {mode}")
            
            logger.info(f"Successfully wrote {df.count()} records to {table_id}")
        
        except Exception as e:
            logger.error(f"Failed to write to {table_id}: {e}")
            raise
    
    def _upsert_data(self, layer: str, table_name: str, df: DataFrame, 
                    merge_key: str = "id"):
        """Perform upsert operation using Iceberg merge."""
        table_id = self.get_table_identifier(layer, table_name)
        
        # Create temp view for merge operation
        temp_view = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        df.createOrReplaceTempView(temp_view)
        
        # Perform merge
        merge_sql = f"""
        MERGE INTO {table_id} AS target
        USING {temp_view} AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
        logger.info(f"Upsert completed for {table_id}")
    
    def read_from_layer(self, layer: str, table_name: str,
                       snapshot_id: Optional[int] = None,
                       timestamp: Optional[datetime] = None,
                       filters: Optional[Dict] = None) -> DataFrame:
        """Read data from Iceberg table with optional time travel."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Reading from {table_id}")
        
        try:
            # Base read
            df = self.spark.table(table_id)
            
            # Time travel - read from specific snapshot
            if snapshot_id:
                df = self.spark.read \
                    .option("snapshot-id", snapshot_id) \
                    .table(table_id)
                logger.info(f"Reading from snapshot {snapshot_id}")
            
            # Time travel - read from specific timestamp
            elif timestamp:
                timestamp_ms = int(timestamp.timestamp() * 1000)
                df = self.spark.read \
                    .option("as-of-timestamp", timestamp_ms) \
                    .table(table_id)
                logger.info(f"Reading from timestamp {timestamp}")
            
            # Apply filters
            if filters:
                for column, value in filters.items():
                    df = df.filter(col(column) == value)
            
            return df
        
        except Exception as e:
            logger.error(f"Failed to read from {table_id}: {e}")
            raise
    
    def _table_exists(self, layer: str, table_name: str) -> bool:
        """Check if an Iceberg table exists."""
        table_id = self.get_table_identifier(layer, table_name)
        
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_id}")
            return True
        except Exception:
            return False
    
    def get_table_history(self, layer: str, table_name: str) -> DataFrame:
        """Get snapshot history for an Iceberg table."""
        table_id = self.get_table_identifier(layer, table_name)
        
        try:
            history_df = self.spark.sql(f"SELECT * FROM {table_id}.history")
            return history_df
        except Exception as e:
            logger.error(f"Failed to get history for {table_id}: {e}")
            raise
    
    def get_table_snapshots(self, layer: str, table_name: str) -> DataFrame:
        """Get all snapshots for an Iceberg table."""
        table_id = self.get_table_identifier(layer, table_name)
        
        try:
            snapshots_df = self.spark.sql(f"SELECT * FROM {table_id}.snapshots")
            return snapshots_df
        except Exception as e:
            logger.error(f"Failed to get snapshots for {table_id}: {e}")
            raise
    
    def get_table_files(self, layer: str, table_name: str) -> DataFrame:
        """Get file-level metadata for an Iceberg table."""
        table_id = self.get_table_identifier(layer, table_name)
        
        try:
            files_df = self.spark.sql(f"SELECT * FROM {table_id}.files")
            return files_df
        except Exception as e:
            logger.error(f"Failed to get files for {table_id}: {e}")
            raise
    
    def evolve_schema(self, layer: str, table_name: str, 
                     columns_to_add: List[tuple] = None,
                     columns_to_drop: List[str] = None,
                     columns_to_rename: Dict[str, str] = None):
        """Evolve table schema (add, drop, or rename columns)."""
        if not iceberg_config.enable_schema_evolution:
            raise RuntimeError("Schema evolution is disabled in configuration")
        
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Evolving schema for {table_id}")
        
        try:
            # Add columns
            if columns_to_add:
                for col_name, col_type in columns_to_add:
                    self.spark.sql(f"ALTER TABLE {table_id} ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Added column {col_name} to {table_id}")
            
            # Drop columns
            if columns_to_drop:
                for col_name in columns_to_drop:
                    self.spark.sql(f"ALTER TABLE {table_id} DROP COLUMN {col_name}")
                    logger.info(f"Dropped column {col_name} from {table_id}")
            
            # Rename columns
            if columns_to_rename:
                for old_name, new_name in columns_to_rename.items():
                    self.spark.sql(f"ALTER TABLE {table_id} RENAME COLUMN {old_name} TO {new_name}")
                    logger.info(f"Renamed column {old_name} to {new_name} in {table_id}")
        
        except Exception as e:
            logger.error(f"Failed to evolve schema for {table_id}: {e}")
            raise
    
    def compact_table(self, layer: str, table_name: str):
        """Compact small files in an Iceberg table."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Compacting table: {table_id}")
        
        try:
            # Call Iceberg's rewrite data files procedure
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{table_id}',
                    options => map(
                        'target-file-size-bytes', '{iceberg_config.target_file_size_bytes}'
                    )
                )
            """)
            logger.info(f"Compaction completed for {table_id}")
        except Exception as e:
            logger.error(f"Failed to compact {table_id}: {e}")
            raise
    
    def expire_snapshots(self, layer: str, table_name: str, 
                        older_than_days: int = None):
        """Expire old snapshots to save storage space."""
        table_id = self.get_table_identifier(layer, table_name)
        
        if older_than_days is None:
            older_than_days = iceberg_config.snapshot_retention_days
        
        logger.info(f"Expiring snapshots older than {older_than_days} days for {table_id}")
        
        try:
            timestamp_ms = int((datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000)
            
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.expire_snapshots(
                    table => '{table_id}',
                    older_than => TIMESTAMP '{datetime.fromtimestamp(timestamp_ms/1000).isoformat()}',
                    retain_last => 1
                )
            """)
            logger.info(f"Snapshot expiration completed for {table_id}")
        except Exception as e:
            logger.error(f"Failed to expire snapshots for {table_id}: {e}")
            raise
    
    def remove_orphan_files(self, layer: str, table_name: str):
        """Remove orphan files that are not referenced by any snapshot."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Removing orphan files for {table_id}")
        
        try:
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.remove_orphan_files(
                    table => '{table_id}'
                )
            """)
            logger.info(f"Orphan file removal completed for {table_id}")
        except Exception as e:
            logger.error(f"Failed to remove orphan files for {table_id}: {e}")
            raise
    
    def get_table_statistics(self, layer: str, table_name: str) -> Dict[str, Any]:
        """Get detailed statistics for an Iceberg table."""
        table_id = self.get_table_identifier(layer, table_name)
        
        try:
            # Get basic table info
            df = self.spark.table(table_id)
            record_count = df.count()
            
            # Get file statistics
            files_df = self.get_table_files(layer, table_name)
            file_stats = files_df.agg(
                count("*").alias("file_count"),
                sum("file_size_in_bytes").alias("total_size_bytes"),
                avg("file_size_in_bytes").alias("avg_file_size_bytes"),
                min("file_size_in_bytes").alias("min_file_size_bytes"),
                max("file_size_in_bytes").alias("max_file_size_bytes")
            ).collect()[0]
            
            # Get snapshot count
            snapshots_df = self.get_table_snapshots(layer, table_name)
            snapshot_count = snapshots_df.count()
            
            return {
                'table_id': table_id,
                'record_count': record_count,
                'file_count': file_stats['file_count'],
                'total_size_mb': file_stats['total_size_bytes'] / (1024 * 1024) if file_stats['total_size_bytes'] else 0,
                'avg_file_size_mb': file_stats['avg_file_size_bytes'] / (1024 * 1024) if file_stats['avg_file_size_bytes'] else 0,
                'min_file_size_mb': file_stats['min_file_size_bytes'] / (1024 * 1024) if file_stats['min_file_size_bytes'] else 0,
                'max_file_size_mb': file_stats['max_file_size_bytes'] / (1024 * 1024) if file_stats['max_file_size_bytes'] else 0,
                'snapshot_count': snapshot_count
            }
        
        except Exception as e:
            logger.error(f"Failed to get statistics for {table_id}: {e}")
            return {'error': str(e)}
    
    def rollback_to_snapshot(self, layer: str, table_name: str, snapshot_id: int):
        """Rollback table to a specific snapshot."""
        table_id = self.get_table_identifier(layer, table_name)
        logger.info(f"Rolling back {table_id} to snapshot {snapshot_id}")
        
        try:
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rollback_to_snapshot(
                    table => '{table_id}',
                    snapshot_id => {snapshot_id}
                )
            """)
            logger.info(f"Rollback completed for {table_id}")
        except Exception as e:
            logger.error(f"Failed to rollback {table_id}: {e}")
            raise
    
    def get_all_tables(self, layer: str = None) -> List[str]:
        """Get list of all Iceberg tables in a layer or all layers."""
        tables = []
        
        try:
            if layer:
                database = iceberg_config.get_database_for_layer(layer)
                tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{database}")
                tables = [row['tableName'] for row in tables_df.collect()]
            else:
                # Get tables from all layers
                for layer_name in ['bronze', 'silver', 'gold']:
                    database = iceberg_config.get_database_for_layer(layer_name)
                    tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{database}")
                    layer_tables = [f"{layer_name}.{row['tableName']}" for row in tables_df.collect()]
                    tables.extend(layer_tables)
        
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
        
        return tables


class IcebergMaintenanceManager:
    """Manages maintenance operations for Iceberg tables."""
    
    def __init__(self, storage_manager: IcebergStorageManager):
        self.storage_manager = storage_manager
    
    def run_full_maintenance(self, layer: str = None):
        """Run full maintenance on all tables (compaction, snapshot expiration, orphan removal)."""
        logger.info(f"Starting full maintenance for {layer if layer else 'all layers'}...")
        
        tables = self.storage_manager.get_all_tables(layer)
        
        for table_id in tables:
            if '.' in table_id:
                layer_name, table_name = table_id.split('.')
            else:
                layer_name = layer
                table_name = table_id
            
            try:
                logger.info(f"Maintaining table: {layer_name}.{table_name}")
                
                # Compact small files
                self.storage_manager.compact_table(layer_name, table_name)
                
                # Expire old snapshots
                self.storage_manager.expire_snapshots(layer_name, table_name)
                
                # Remove orphan files
                self.storage_manager.remove_orphan_files(layer_name, table_name)
                
                logger.info(f"Maintenance completed for {layer_name}.{table_name}")
            
            except Exception as e:
                logger.error(f"Maintenance failed for {layer_name}.{table_name}: {e}")
                continue
        
        logger.info("Full maintenance completed")
    
    def generate_maintenance_report(self, layer: str = None) -> Dict[str, Any]:
        """Generate a comprehensive maintenance report."""
        logger.info("Generating maintenance report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'tables': []
        }
        
        tables = self.storage_manager.get_all_tables(layer)
        
        for table_id in tables:
            if '.' in table_id:
                layer_name, table_name = table_id.split('.')
            else:
                layer_name = layer
                table_name = table_id
            
            try:
                stats = self.storage_manager.get_table_statistics(layer_name, table_name)
                report['tables'].append(stats)
            except Exception as e:
                logger.error(f"Failed to get stats for {layer_name}.{table_name}: {e}")
                continue
        
        # Calculate totals
        report['total_tables'] = len(report['tables'])
        report['total_size_mb'] = sum(t.get('total_size_mb', 0) for t in report['tables'])
        report['total_files'] = sum(t.get('file_count', 0) for t in report['tables'])
        report['total_records'] = sum(t.get('record_count', 0) for t in report['tables'])
        
        return report
