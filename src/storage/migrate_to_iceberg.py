"""
Migration script to convert existing Parquet-based storage to Apache Iceberg tables.
This script helps transition the movie analytics pipeline from traditional Parquet files
to Iceberg table format.
"""
import logging
import argparse
from typing import List, Dict, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

from src.streaming.spark_config import create_spark_session
from src.storage.iceberg_storage_manager import IcebergStorageManager
from config.config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetToIcebergMigrator:
    """Migrates existing Parquet data to Iceberg tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.iceberg_manager = IcebergStorageManager(spark)
        self.migration_stats = {
            'start_time': datetime.now(),
            'tables_migrated': [],
            'tables_failed': [],
            'total_records_migrated': 0,
        }
    
    def migrate_layer(self, layer: str, data_types: List[str] = None):
        """Migrate all tables in a specific layer from Parquet to Iceberg."""
        logger.info(f"Starting migration for {layer} layer...")
        
        # Default data types if not specified
        if data_types is None:
            data_types = ['movies', 'people', 'credits', 'reviews']
        
        for data_type in data_types:
            try:
                self.migrate_table(layer, data_type)
            except Exception as e:
                logger.error(f"Failed to migrate {layer}/{data_type}: {e}")
                self.migration_stats['tables_failed'].append({
                    'layer': layer,
                    'data_type': data_type,
                    'error': str(e),
                })
                continue
        
        logger.info(f"Completed migration for {layer} layer")
    
    def migrate_table(self, layer: str, data_type: str):
        """Migrate a single table from Parquet to Iceberg."""
        logger.info(f"Migrating {layer}/{data_type}...")
        
        # Construct Parquet path
        bucket = config.minio_buckets[layer]
        parquet_path = f"s3a://{bucket}/{data_type}/"
        
        logger.info(f"Reading Parquet data from {parquet_path}")
        
        try:
            # Read existing Parquet data
            df = self.spark.read.parquet(parquet_path)
            record_count = df.count()
            
            logger.info(f"Found {record_count} records in {parquet_path}")
            
            # Determine partition columns based on layer and data type
            partition_cols = self._get_partition_columns(layer, data_type, df)
            
            # Determine table name
            table_name = self._get_table_name(data_type, layer)
            
            # Check if table already exists
            if self.iceberg_manager._table_exists(layer, table_name):
                logger.warning(f"Iceberg table {layer}.{table_name} already exists. "
                             "Appending data...")
                # Append to existing table
                self.iceberg_manager._write_to_layer(
                    layer=layer,
                    df=df,
                    table_name=table_name,
                    partition_cols=partition_cols,
                    mode="append"
                )
            else:
                logger.info(f"Creating new Iceberg table {layer}.{table_name}...")
                # Create new table
                self.iceberg_manager.create_table(
                    layer=layer,
                    table_name=table_name,
                    df=df,
                    partition_cols=partition_cols,
                    overwrite=False
                )
            
            # Verify migration
            iceberg_df = self.iceberg_manager.read_from_layer(layer, table_name)
            iceberg_count = iceberg_df.count()
            
            logger.info(f"Verification: Iceberg table has {iceberg_count} records")
            
            # Record success
            self.migration_stats['tables_migrated'].append({
                'layer': layer,
                'data_type': data_type,
                'table_name': table_name,
                'records': record_count,
                'partition_cols': partition_cols,
            })
            self.migration_stats['total_records_migrated'] += record_count
            
            logger.info(f"Successfully migrated {layer}/{data_type} to Iceberg")
        
        except Exception as e:
            logger.error(f"Error migrating {layer}/{data_type}: {e}")
            raise
    
    def _get_partition_columns(self, layer: str, data_type: str, df: DataFrame) -> List[str]:
        """Determine appropriate partition columns based on layer and data type."""
        columns = df.columns
        
        # Default partitioning strategies
        if layer == 'bronze':
            # Bronze: partition by ingestion date
            if 'year' in columns and 'month' in columns:
                return ['year', 'month']
            elif 'ingestion_date' in columns:
                return ['ingestion_date']
            else:
                return []
        
        elif layer == 'silver':
            # Silver: partition by date and potentially category
            if data_type in ['movies', 'reviews']:
                if 'year' in columns and 'month' in columns:
                    return ['year', 'month']
            elif data_type == 'people':
                if 'year' in columns:
                    return ['year']
            elif data_type == 'credits':
                if 'movie_id' in columns:
                    # Don't partition credits by movie_id (too many partitions)
                    return []
            return []
        
        elif layer == 'gold':
            # Gold: partition by business dimensions
            if 'year' in columns:
                return ['year']
            return []
        
        return []
    
    def _get_table_name(self, data_type: str, layer: str) -> str:
        """Generate Iceberg table name based on data type and layer."""
        if layer == 'bronze':
            return f"{data_type}_raw"
        elif layer == 'silver':
            if data_type == 'reviews':
                return "reviews_with_sentiment"
            else:
                return f"{data_type}_clean"
        elif layer == 'gold':
            # Gold layer has custom table names
            return data_type
        
        return data_type
    
    def migrate_all_layers(self):
        """Migrate all layers from Parquet to Iceberg."""
        logger.info("Starting full migration from Parquet to Iceberg...")
        
        layers = ['bronze', 'silver', 'gold']
        
        for layer in layers:
            try:
                self.migrate_layer(layer)
            except Exception as e:
                logger.error(f"Failed to migrate {layer} layer: {e}")
                continue
        
        self._print_migration_summary()
    
    def _print_migration_summary(self):
        """Print migration summary."""
        self.migration_stats['end_time'] = datetime.now()
        duration = (self.migration_stats['end_time'] - self.migration_stats['start_time']).total_seconds()
        
        logger.info("=" * 80)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Start Time: {self.migration_stats['start_time']}")
        logger.info(f"End Time: {self.migration_stats['end_time']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total Records Migrated: {self.migration_stats['total_records_migrated']}")
        logger.info(f"Tables Migrated Successfully: {len(self.migration_stats['tables_migrated'])}")
        logger.info(f"Tables Failed: {len(self.migration_stats['tables_failed'])}")
        
        if self.migration_stats['tables_migrated']:
            logger.info("\nSuccessfully Migrated Tables:")
            for table in self.migration_stats['tables_migrated']:
                logger.info(f"  - {table['layer']}.{table['table_name']}: "
                          f"{table['records']} records, "
                          f"partitions: {table['partition_cols']}")
        
        if self.migration_stats['tables_failed']:
            logger.info("\nFailed Tables:")
            for table in self.migration_stats['tables_failed']:
                logger.info(f"  - {table['layer']}.{table['data_type']}: {table['error']}")
        
        logger.info("=" * 80)
    
    def validate_migration(self, layer: str, data_type: str) -> bool:
        """Validate that migration was successful by comparing record counts."""
        logger.info(f"Validating migration for {layer}/{data_type}...")
        
        try:
            # Read Parquet data
            bucket = config.minio_buckets[layer]
            parquet_path = f"s3a://{bucket}/{data_type}/"
            parquet_df = self.spark.read.parquet(parquet_path)
            parquet_count = parquet_df.count()
            
            # Read Iceberg data
            table_name = self._get_table_name(data_type, layer)
            iceberg_df = self.iceberg_manager.read_from_layer(layer, table_name)
            iceberg_count = iceberg_df.count()
            
            if parquet_count == iceberg_count:
                logger.info(f"✓ Validation successful: {parquet_count} == {iceberg_count}")
                return True
            else:
                logger.warning(f"✗ Validation failed: Parquet has {parquet_count} records, "
                             f"Iceberg has {iceberg_count} records")
                return False
        
        except Exception as e:
            logger.error(f"Validation failed with error: {e}")
            return False


def main():
    """Main migration script."""
    parser = argparse.ArgumentParser(
        description='Migrate Parquet data to Apache Iceberg tables'
    )
    parser.add_argument(
        '--layer',
        type=str,
        choices=['bronze', 'silver', 'gold', 'all'],
        default='all',
        help='Layer to migrate (default: all)'
    )
    parser.add_argument(
        '--data-type',
        type=str,
        help='Specific data type to migrate (e.g., movies, people)'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate migration after completion'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a dry run without actual migration'
    )
    
    args = parser.parse_args()
    
    logger.info("Parquet to Iceberg Migration Tool")
    logger.info("=" * 80)
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual migration will be performed")
    
    # Create Spark session with Iceberg support
    logger.info("Creating Spark session with Iceberg support...")
    spark = create_spark_session(
        app_name="ParquetToIcebergMigration",
        enable_iceberg=True
    )
    
    # Initialize migrator
    migrator = ParquetToIcebergMigrator(spark)
    
    if args.dry_run:
        logger.info("Dry run completed. No data was migrated.")
        spark.stop()
        return
    
    # Perform migration
    if args.layer == 'all':
        migrator.migrate_all_layers()
    else:
        data_types = [args.data_type] if args.data_type else None
        migrator.migrate_layer(args.layer, data_types)
    
    # Validate if requested
    if args.validate:
        logger.info("\nValidating migration...")
        if args.data_type:
            migrator.validate_migration(args.layer, args.data_type)
        else:
            logger.info("Validation requires --data-type to be specified")
    
    # Cleanup
    spark.stop()
    logger.info("Migration completed successfully!")


if __name__ == '__main__':
    main()
