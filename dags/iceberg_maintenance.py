"""
Apache Iceberg Maintenance DAG
Performs regular maintenance on Iceberg tables including:
- File compaction
- Snapshot expiration
- Orphan file removal
- Performance monitoring
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def compact_iceberg_tables(**context):
    """Compact small files in Iceberg tables for better query performance."""
    from src.streaming.spark_config import create_spark_session
    from src.storage.iceberg_storage_manager import IcebergStorageManager
    
    logger.info("Starting Iceberg table compaction...")
    
    try:
        # Create Spark session with Iceberg support
        spark = create_spark_session(
            app_name="IcebergMaintenance-Compaction",
            enable_iceberg=True
        )
        
        storage_manager = IcebergStorageManager(spark)
        
        # Get all tables
        tables_to_compact = [
            ('bronze', 'movies_raw'),
            ('bronze', 'people_raw'),
            ('bronze', 'credits_raw'),
            ('bronze', 'reviews_raw'),
            ('silver', 'movies_clean'),
            ('silver', 'people_clean'),
            ('silver', 'credits_clean'),
            ('silver', 'reviews_with_sentiment'),
            ('gold', 'movies_by_year_genre'),
            ('gold', 'trending_movies'),
            ('gold', 'actor_networks'),
        ]
        
        compaction_results = []
        for layer, table_name in tables_to_compact:
            try:
                logger.info(f"Compacting {layer}.{table_name}...")
                
                # Get stats before compaction
                stats_before = storage_manager.get_table_statistics(layer, table_name)
                
                # Compact table
                storage_manager.compact_table(layer, table_name)
                
                # Get stats after compaction
                stats_after = storage_manager.get_table_statistics(layer, table_name)
                
                result = {
                    'table': f"{layer}.{table_name}",
                    'files_before': stats_before.get('file_count', 0),
                    'files_after': stats_after.get('file_count', 0),
                    'size_mb': stats_after.get('total_size_mb', 0),
                }
                compaction_results.append(result)
                
                logger.info(f"Compacted {layer}.{table_name}: "
                          f"{stats_before.get('file_count', 0)} -> {stats_after.get('file_count', 0)} files")
            
            except Exception as e:
                logger.error(f"Failed to compact {layer}.{table_name}: {e}")
                continue
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='compaction_results', value=compaction_results)
        
        spark.stop()
        logger.info("Compaction completed successfully")
        
    except Exception as e:
        logger.error(f"Compaction failed: {e}")
        raise


def expire_old_snapshots(**context):
    """Expire old snapshots to save storage space."""
    from src.streaming.spark_config import create_spark_session
    from src.storage.iceberg_storage_manager import IcebergStorageManager
    from config.iceberg_config import iceberg_config
    
    logger.info("Starting snapshot expiration...")
    
    try:
        spark = create_spark_session(
            app_name="IcebergMaintenance-SnapshotExpiration",
            enable_iceberg=True
        )
        
        storage_manager = IcebergStorageManager(spark)
        
        # Different retention policies for different layers
        retention_policies = {
            'bronze': 3,   # Keep 3 days for raw data
            'silver': 7,   # Keep 7 days for cleaned data
            'gold': 30,    # Keep 30 days for aggregated data
        }
        
        expiration_results = []
        
        for layer, retention_days in retention_policies.items():
            try:
                # Get all tables in the layer
                tables = storage_manager.get_all_tables(layer)
                
                for table_name in tables:
                    try:
                        logger.info(f"Expiring snapshots for {layer}.{table_name} "
                                  f"(older than {retention_days} days)...")
                        
                        # Get snapshot count before expiration
                        snapshots_before = storage_manager.get_table_snapshots(layer, table_name)
                        count_before = snapshots_before.count()
                        
                        # Expire old snapshots
                        storage_manager.expire_snapshots(layer, table_name, retention_days)
                        
                        # Get snapshot count after expiration
                        snapshots_after = storage_manager.get_table_snapshots(layer, table_name)
                        count_after = snapshots_after.count()
                        
                        result = {
                            'table': f"{layer}.{table_name}",
                            'snapshots_before': count_before,
                            'snapshots_after': count_after,
                            'expired': count_before - count_after,
                        }
                        expiration_results.append(result)
                        
                        logger.info(f"Expired {count_before - count_after} snapshots from {layer}.{table_name}")
                    
                    except Exception as e:
                        logger.error(f"Failed to expire snapshots for {layer}.{table_name}: {e}")
                        continue
            
            except Exception as e:
                logger.error(f"Failed to process layer {layer}: {e}")
                continue
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='expiration_results', value=expiration_results)
        
        spark.stop()
        logger.info("Snapshot expiration completed successfully")
        
    except Exception as e:
        logger.error(f"Snapshot expiration failed: {e}")
        raise


def remove_orphan_files(**context):
    """Remove orphan files that are not referenced by any snapshot."""
    from src.streaming.spark_config import create_spark_session
    from src.storage.iceberg_storage_manager import IcebergStorageManager
    
    logger.info("Starting orphan file removal...")
    
    try:
        spark = create_spark_session(
            app_name="IcebergMaintenance-OrphanRemoval",
            enable_iceberg=True
        )
        
        storage_manager = IcebergStorageManager(spark)
        
        # Get all tables
        all_tables = []
        for layer in ['bronze', 'silver', 'gold']:
            tables = storage_manager.get_all_tables(layer)
            all_tables.extend([(layer, t) for t in tables])
        
        orphan_removal_results = []
        
        for layer, table_name in all_tables:
            try:
                logger.info(f"Removing orphan files from {layer}.{table_name}...")
                
                # Remove orphan files
                storage_manager.remove_orphan_files(layer, table_name)
                
                result = {
                    'table': f"{layer}.{table_name}",
                    'status': 'success',
                }
                orphan_removal_results.append(result)
                
                logger.info(f"Orphan removal completed for {layer}.{table_name}")
            
            except Exception as e:
                logger.error(f"Failed to remove orphan files from {layer}.{table_name}: {e}")
                result = {
                    'table': f"{layer}.{table_name}",
                    'status': 'failed',
                    'error': str(e),
                }
                orphan_removal_results.append(result)
                continue
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='orphan_removal_results', value=orphan_removal_results)
        
        spark.stop()
        logger.info("Orphan file removal completed successfully")
        
    except Exception as e:
        logger.error(f"Orphan file removal failed: {e}")
        raise


def generate_maintenance_report(**context):
    """Generate a comprehensive maintenance report."""
    from src.streaming.spark_config import create_spark_session
    from src.storage.iceberg_storage_manager import (
        IcebergStorageManager,
        IcebergMaintenanceManager
    )
    
    logger.info("Generating maintenance report...")
    
    try:
        spark = create_spark_session(
            app_name="IcebergMaintenance-Report",
            enable_iceberg=True
        )
        
        storage_manager = IcebergStorageManager(spark)
        maintenance_mgr = IcebergMaintenanceManager(storage_manager)
        
        # Generate report
        report = maintenance_mgr.generate_maintenance_report()
        
        # Get previous task results
        ti = context['task_instance']
        compaction_results = ti.xcom_pull(task_ids='compact_tables', key='compaction_results')
        expiration_results = ti.xcom_pull(task_ids='expire_snapshots', key='expiration_results')
        orphan_removal_results = ti.xcom_pull(task_ids='remove_orphan_files', key='orphan_removal_results')
        
        # Add task results to report
        report['maintenance_operations'] = {
            'compaction': compaction_results,
            'snapshot_expiration': expiration_results,
            'orphan_removal': orphan_removal_results,
        }
        
        # Log summary
        logger.info("=" * 80)
        logger.info("ICEBERG MAINTENANCE REPORT")
        logger.info("=" * 80)
        logger.info(f"Timestamp: {report['timestamp']}")
        logger.info(f"Total Tables: {report['total_tables']}")
        logger.info(f"Total Size: {report['total_size_mb']:.2f} MB")
        logger.info(f"Total Files: {report['total_files']}")
        logger.info(f"Total Records: {report['total_records']}")
        logger.info("-" * 80)
        
        if compaction_results:
            logger.info("Compaction Results:")
            for result in compaction_results:
                logger.info(f"  {result['table']}: {result['files_before']} -> {result['files_after']} files")
        
        if expiration_results:
            logger.info("Snapshot Expiration Results:")
            for result in expiration_results:
                logger.info(f"  {result['table']}: Expired {result['expired']} snapshots")
        
        logger.info("=" * 80)
        
        # Push complete report to XCom
        context['task_instance'].xcom_push(key='maintenance_report', value=report)
        
        spark.stop()
        logger.info("Report generation completed successfully")
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise


# Create DAG
with DAG(
    dag_id='iceberg_maintenance',
    default_args=default_args,
    description='Regular maintenance for Apache Iceberg tables',
    schedule_interval='0 2 * * 0',  # Weekly on Sunday at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['iceberg', 'maintenance', 'storage'],
) as dag:
    
    # Task 1: Compact small files
    compact_task = PythonOperator(
        task_id='compact_tables',
        python_callable=compact_iceberg_tables,
        provide_context=True,
    )
    
    # Task 2: Expire old snapshots
    expire_task = PythonOperator(
        task_id='expire_snapshots',
        python_callable=expire_old_snapshots,
        provide_context=True,
    )
    
    # Task 3: Remove orphan files
    orphan_task = PythonOperator(
        task_id='remove_orphan_files',
        python_callable=remove_orphan_files,
        provide_context=True,
    )
    
    # Task 4: Generate maintenance report
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_maintenance_report,
        provide_context=True,
    )
    
    # Define task dependencies
    # All maintenance tasks run in parallel, then generate report
    [compact_task, expire_task, orphan_task] >> report_task
