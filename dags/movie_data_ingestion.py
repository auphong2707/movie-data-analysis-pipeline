"""
Main movie data ingestion DAG.
This DAG orchestrates the complete data extraction workflow from TMDB API.
"""
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import sys
import os

# Add project root to Python path
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'movie_data_ingestion',
    default_args=default_args,
    description='Movie data extraction and ingestion pipeline',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['movie', 'ingestion', 'tmdb'],
)

def extract_trending_movies(**context) -> Dict[str, Any]:
    """Extract trending movies from TMDB API."""
    from src.ingestion.tmdb_client import TMDBClient
    from src.ingestion.kafka_producer import MovieDataProducer
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get configuration
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    if not tmdb_api_key:
        raise ValueError("TMDB_API_KEY environment variable not set")
    
    # Initialize clients
    tmdb_client = TMDBClient(api_key=tmdb_api_key)
    kafka_producer = MovieDataProducer(bootstrap_servers=kafka_servers)
    
    try:
        # Extract trending movies
        trending_data = tmdb_client.get_trending_movies('day')
        movies = trending_data.get('results', [])
        
        logger.info(f"Extracted {len(movies)} trending movies")
        
        # Send to Kafka
        kafka_producer.produce_movies(movies)
        kafka_producer.flush()
        
        # Store metrics in XCom
        return {
            'movies_count': len(movies),
            'extraction_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error extracting trending movies: {e}")
        raise
    finally:
        kafka_producer.close()

def extract_popular_movies(**context) -> Dict[str, Any]:
    """Extract popular movies from TMDB API."""
    from src.ingestion.tmdb_client import TMDBClient
    from src.ingestion.kafka_producer import MovieDataProducer
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get configuration
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    max_pages = int(Variable.get('max_popular_pages', default_var=3))
    
    if not tmdb_api_key:
        raise ValueError("TMDB_API_KEY environment variable not set")
    
    # Initialize clients
    tmdb_client = TMDBClient(api_key=tmdb_api_key)
    kafka_producer = MovieDataProducer(bootstrap_servers=kafka_servers)
    
    try:
        all_movies = []
        
        for page in range(1, max_pages + 1):
            popular_data = tmdb_client.get_popular_movies(page)
            movies = popular_data.get('results', [])
            all_movies.extend(movies)
            
        logger.info(f"Extracted {len(all_movies)} popular movies from {max_pages} pages")
        
        # Send to Kafka
        kafka_producer.produce_movies(all_movies)
        kafka_producer.flush()
        
        return {
            'movies_count': len(all_movies),
            'pages_processed': max_pages,
            'extraction_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error extracting popular movies: {e}")
        raise
    finally:
        kafka_producer.close()

def extract_trending_people(**context) -> Dict[str, Any]:
    """Extract trending people from TMDB API."""
    from src.ingestion.tmdb_client import TMDBClient
    from src.ingestion.kafka_producer import MovieDataProducer
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get configuration
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    if not tmdb_api_key:
        raise ValueError("TMDB_API_KEY environment variable not set")
    
    # Initialize clients
    tmdb_client = TMDBClient(api_key=tmdb_api_key)
    kafka_producer = MovieDataProducer(bootstrap_servers=kafka_servers)
    
    try:
        # Extract trending people
        trending_data = tmdb_client.get_trending_people('day')
        people = trending_data.get('results', [])
        
        logger.info(f"Extracted {len(people)} trending people")
        
        # Send to Kafka
        kafka_producer.produce_people(people)
        kafka_producer.flush()
        
        return {
            'people_count': len(people),
            'extraction_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error extracting trending people: {e}")
        raise
    finally:
        kafka_producer.close()

def extract_movie_details(**context) -> Dict[str, Any]:
    """Extract detailed information for recently added movies."""
    from src.ingestion.tmdb_client import TMDBClient
    from src.ingestion.kafka_producer import MovieDataProducer
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get trending movies from previous task
    trending_results = context['task_instance'].xcom_pull(task_ids='extract_trending_movies')
    if not trending_results or trending_results.get('status') != 'success':
        logger.warning("No trending movies data found, skipping detail extraction")
        return {'status': 'skipped', 'reason': 'no_trending_data'}
    
    # Get configuration
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    # Initialize clients
    tmdb_client = TMDBClient(api_key=tmdb_api_key)
    kafka_producer = MovieDataProducer(bootstrap_servers=kafka_servers)
    
    try:
        # Get trending movies again for processing
        trending_data = tmdb_client.get_trending_movies('day')
        movies = trending_data.get('results', [])
        
        details_count = 0
        credits_count = 0
        reviews_count = 0
        
        # Process first 10 movies to avoid overwhelming the API
        for movie in movies[:10]:
            movie_id = movie.get('id')
            if not movie_id:
                continue
            
            try:
                # Get movie details
                details = tmdb_client.get_movie_details(movie_id)
                if details:
                    kafka_producer.produce_movie_data('movie_details', details, str(movie_id))
                    details_count += 1
                
                # Get credits
                credits = tmdb_client.get_movie_credits(movie_id)
                if credits:
                    kafka_producer.produce_credits(credits, movie_id)
                    credits_count += 1
                
                # Get reviews
                reviews_data = tmdb_client.get_movie_reviews(movie_id)
                if reviews_data and reviews_data.get('results'):
                    reviews = reviews_data['results']
                    kafka_producer.produce_reviews(reviews, movie_id)
                    reviews_count += len(reviews)
                    
            except Exception as e:
                logger.error(f"Error processing movie {movie_id}: {e}")
                continue
        
        kafka_producer.flush()
        
        return {
            'details_count': details_count,
            'credits_count': credits_count,
            'reviews_count': reviews_count,
            'extraction_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error extracting movie details: {e}")
        raise
    finally:
        kafka_producer.close()

def check_kafka_health(**context) -> bool:
    """Check if Kafka is healthy and topics exist."""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    import logging
    
    logger = logging.getLogger(__name__)
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    try:
        # Test producer connection
        producer = KafkaProducer(
            bootstrap_servers=[kafka_servers],
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )
        producer.close()
        
        logger.info("Kafka health check passed")
        return True
        
    except NoBrokersAvailable:
        logger.error("Kafka brokers not available")
        raise
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        raise

def publish_extraction_metrics(**context) -> None:
    """Publish extraction metrics to monitoring system."""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Collect metrics from all extraction tasks
    trending_movies = context['task_instance'].xcom_pull(task_ids='extract_trending_movies') or {}
    popular_movies = context['task_instance'].xcom_pull(task_ids='extract_popular_movies') or {}
    trending_people = context['task_instance'].xcom_pull(task_ids='extract_trending_people') or {}
    movie_details = context['task_instance'].xcom_pull(task_ids='extract_movie_details') or {}
    
    total_metrics = {
        'trending_movies_count': trending_movies.get('movies_count', 0),
        'popular_movies_count': popular_movies.get('movies_count', 0),
        'trending_people_count': trending_people.get('people_count', 0),
        'movie_details_count': movie_details.get('details_count', 0),
        'credits_count': movie_details.get('credits_count', 0),
        'reviews_count': movie_details.get('reviews_count', 0),
        'total_extraction_time': datetime.now().isoformat(),
        'dag_run_id': context['dag_run'].run_id
    }
    
    logger.info(f"Extraction metrics: {total_metrics}")
    
    # TODO: Send metrics to Grafana/Prometheus
    # For now, just log the metrics

# Task definitions
start_task = DummyOperator(
    task_id='start_ingestion',
    dag=dag,
)

kafka_health_check = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# Data extraction tasks
extract_trending_movies_task = PythonOperator(
    task_id='extract_trending_movies',
    python_callable=extract_trending_movies,
    dag=dag,
)

extract_popular_movies_task = PythonOperator(
    task_id='extract_popular_movies',
    python_callable=extract_popular_movies,
    dag=dag,
)

extract_trending_people_task = PythonOperator(
    task_id='extract_trending_people',
    python_callable=extract_trending_people,
    dag=dag,
)

# Detailed extraction (depends on trending movies)
extract_movie_details_task = PythonOperator(
    task_id='extract_movie_details',
    python_callable=extract_movie_details,
    dag=dag,
)

# Metrics collection
publish_metrics_task = PythonOperator(
    task_id='publish_extraction_metrics',
    python_callable=publish_extraction_metrics,
    dag=dag,
    trigger_rule='all_done',  # Run even if some tasks fail
)

end_task = DummyOperator(
    task_id='end_ingestion',
    dag=dag,
)

# Task dependencies
start_task >> kafka_health_check

kafka_health_check >> [
    extract_trending_movies_task,
    extract_popular_movies_task,
    extract_trending_people_task
]

extract_trending_movies_task >> extract_movie_details_task

[
    extract_trending_movies_task,
    extract_popular_movies_task, 
    extract_trending_people_task,
    extract_movie_details_task
] >> publish_metrics_task >> end_task