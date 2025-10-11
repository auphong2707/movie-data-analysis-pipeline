"""
Airflow DAG for DataHub metadata management and lineage tracking.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    'datahub_metadata_management',
    default_args=default_args,
    description='DataHub metadata management and lineage tracking',
    schedule_interval='@daily',  # Run daily to sync metadata
    catchup=False,
    max_active_runs=1,
    tags=['datahub', 'metadata', 'governance'],
)

def initialize_datahub_metadata(**context):
    """Initialize DataHub with pipeline metadata."""
    from src.governance.datahub_lineage import get_lineage_tracker
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get DataHub lineage tracker
        tracker = get_lineage_tracker()
        
        # Track TMDB API as source
        tracker.emit_dataset_metadata(
            platform='rest',
            dataset_name='api.themoviedb.org',
            description='The Movie Database (TMDB) API - External data source',
            tags=['external-api', 'source', 'movie-data'],
            properties={
                'api_version': '3',
                'base_url': 'https://api.themoviedb.org/3',
                'data_types': 'movies,people,credits,reviews'
            }
        )
        
        # Track Kafka topics
        kafka_topics = ['movies', 'people', 'credits', 'reviews', 'ratings']
        for topic in kafka_topics:
            tracker.emit_dataset_metadata(
                platform='kafka',
                dataset_name=topic,
                description=f'Kafka topic for streaming {topic} data',
                tags=['kafka', 'streaming', 'real-time'],
                properties={
                    'topic': topic,
                    'bootstrap_servers': 'kafka:29092',
                    'partitions': '3',
                    'replication_factor': '1'
                }
            )
        
        # Track MinIO/S3 storage layers
        storage_layers = {
            'bronze-data': 'Raw data with minimal processing',
            'silver-data': 'Cleaned and enriched data',
            'gold-data': 'Business-ready aggregated data'
        }
        
        for layer, description in storage_layers.items():
            for data_type in ['movies', 'people', 'credits', 'reviews']:
                tracker.emit_dataset_metadata(
                    platform='s3',
                    dataset_name=f'{layer}/{data_type}',
                    description=f'{description} - {data_type}',
                    tags=['storage', layer.split('-')[0], 'parquet'],
                    properties={
                        'bucket': layer,
                        'format': 'parquet',
                        'partitioning': 'year/month' if data_type == 'movies' else 'processing_date',
                        'compression': 'snappy'
                    }
                )
        
        # Track MongoDB collections
        mongodb_collections = {
            'movies': 'Movie documents for serving layer',
            'people': 'People documents for serving layer',
            'analytics': 'Pre-computed analytics and metrics',
            'trends': 'Trending data and recommendations'
        }
        
        for collection, description in mongodb_collections.items():
            tracker.emit_dataset_metadata(
                platform='mongodb',
                dataset_name=f'moviedb.{collection}',
                description=description,
                tags=['mongodb', 'serving', 'nosql'],
                properties={
                    'database': 'moviedb',
                    'collection': collection,
                    'connection': 'mongodb://mongodb:27017'
                }
            )
        
        logger.info("DataHub metadata initialization completed")
        return {"status": "success", "message": "Metadata initialized"}
        
    except Exception as e:
        logger.error(f"DataHub metadata initialization failed: {e}")
        raise

def track_pipeline_lineage(**context):
    """Track complete pipeline lineage in DataHub."""
    from src.governance.datahub_lineage import get_lineage_tracker
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        tracker = get_lineage_tracker()
        
        # Track ingestion jobs: TMDB API -> Kafka
        kafka_topics = ['movies', 'people', 'credits', 'reviews', 'ratings']
        tmdb_urn = 'urn:li:dataset:(urn:li:dataPlatform:rest,movie-pipeline.api.themoviedb.org,PROD)'
        
        for topic in kafka_topics:
            topic_urn = f'urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)'
            
            # Create ingestion job
            job_urn = tracker.emit_job_metadata(
                platform='kafka',
                job_name=f'ingest_{topic}',
                job_type='INGESTION',
                inputs=[tmdb_urn],
                outputs=[topic_urn],
                description=f'Ingestion job for {topic} from TMDB API'
            )
            
            # Emit lineage
            tracker.emit_lineage(topic_urn, [tmdb_urn])
        
        # Track Spark processing jobs: Kafka -> Storage Layers
        for topic in ['movies', 'people', 'credits', 'reviews']:
            kafka_urn = f'urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)'
            bronze_urn = f'urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.bronze-data/{topic},PROD)'
            silver_urn = f'urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.silver-data/{topic},PROD)'
            gold_urn = f'urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.gold-data/{topic},PROD)'
            
            # Bronze processing job
            tracker.emit_job_metadata(
                platform='spark',
                job_name=f'process_{topic}_bronze',
                job_type='PROCESSING',
                inputs=[kafka_urn],
                outputs=[bronze_urn],
                description=f'Spark job to process {topic} data to Bronze layer'
            )
            tracker.emit_lineage(bronze_urn, [kafka_urn])
            
            # Silver processing job
            tracker.emit_job_metadata(
                platform='spark',
                job_name=f'process_{topic}_silver',
                job_type='PROCESSING',
                inputs=[bronze_urn],
                outputs=[silver_urn],
                description=f'Spark job to process {topic} data to Silver layer'
            )
            tracker.emit_lineage(silver_urn, [bronze_urn])
            
            # Gold processing job
            tracker.emit_job_metadata(
                platform='spark',
                job_name=f'process_{topic}_gold',
                job_type='PROCESSING',
                inputs=[silver_urn],
                outputs=[gold_urn],
                description=f'Spark job to process {topic} data to Gold layer'
            )
            tracker.emit_lineage(gold_urn, [silver_urn])
        
        # Track MongoDB serving jobs: Storage -> MongoDB
        collections_mapping = {
            'movies': ['gold-data/movies', 'silver-data/movies'],
            'people': ['gold-data/people', 'silver-data/people'],
            'analytics': ['gold-data/movies', 'gold-data/people', 'gold-data/credits'],
            'trends': ['gold-data/movies', 'gold-data/reviews']
        }
        
        for collection, source_layers in collections_mapping.items():
            collection_urn = f'urn:li:dataset:(urn:li:dataPlatform:mongodb,movie-pipeline.moviedb.{collection},PROD)'
            source_urns = [f'urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.{layer},PROD)' for layer in source_layers]
            
            tracker.emit_job_metadata(
                platform='mongodb',
                job_name=f'serve_{collection}',
                job_type='SERVING',
                inputs=source_urns,
                outputs=[collection_urn],
                description=f'Serving job for {collection} collection'
            )
            tracker.emit_lineage(collection_urn, source_urns)
        
        logger.info("Pipeline lineage tracking completed")
        return {"status": "success", "message": "Lineage tracked"}
        
    except Exception as e:
        logger.error(f"Pipeline lineage tracking failed: {e}")
        raise

def validate_datahub_health(**context):
    """Validate DataHub services health."""
    import requests
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Check DataHub GMS health
        gms_response = requests.get('http://datahub-gms:8080/health', timeout=30)
        gms_health = gms_response.status_code == 200
        
        # Check DataHub Frontend health
        frontend_response = requests.get('http://datahub-frontend:9002/api/v2/graphql', timeout=30)
        frontend_health = frontend_response.status_code in [200, 405]  # GraphQL endpoint may return 405 for GET
        
        health_status = {
            'gms_healthy': gms_health,
            'frontend_healthy': frontend_health,
            'overall_healthy': gms_health and frontend_health
        }
        
        if health_status['overall_healthy']:
            logger.info("DataHub services are healthy")
        else:
            logger.warning(f"DataHub health issues detected: {health_status}")
        
        return health_status
        
    except Exception as e:
        logger.error(f"DataHub health check failed: {e}")
        return {
            'gms_healthy': False,
            'frontend_healthy': False,
            'overall_healthy': False,
            'error': str(e)
        }

def cleanup_stale_metadata(**context):
    """Clean up stale metadata in DataHub."""
    from src.governance.datahub_lineage import get_lineage_tracker
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # This is a placeholder for metadata cleanup logic
        # In a real implementation, you would:
        # 1. Query DataHub for entities older than X days
        # 2. Check if corresponding data sources still exist
        # 3. Remove or mark as deprecated stale entities
        
        logger.info("Metadata cleanup completed (placeholder)")
        return {"status": "success", "message": "Cleanup completed"}
        
    except Exception as e:
        logger.error(f"Metadata cleanup failed: {e}")
        raise

# Define tasks
start_task = PythonOperator(
    task_id='start_metadata_management',
    python_callable=lambda: logger.info("Starting DataHub metadata management"),
    dag=dag,
)

health_check_task = PythonOperator(
    task_id='validate_datahub_health',
    python_callable=validate_datahub_health,
    dag=dag,
)

initialize_metadata_task = PythonOperator(
    task_id='initialize_datahub_metadata',
    python_callable=initialize_datahub_metadata,
    dag=dag,
)

track_lineage_task = PythonOperator(
    task_id='track_pipeline_lineage',
    python_callable=track_pipeline_lineage,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_stale_metadata',
    python_callable=cleanup_stale_metadata,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_metadata_management',
    python_callable=lambda: logger.info("DataHub metadata management completed"),
    dag=dag,
)

# Define task dependencies
start_task >> health_check_task >> initialize_metadata_task >> track_lineage_task >> cleanup_task >> end_task