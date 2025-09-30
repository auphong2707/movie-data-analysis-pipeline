"""
Movie data processing DAG.
This DAG orchestrates the Spark-based data processing and analysis.
"""
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
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
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'movie_data_processing',
    default_args=default_args,
    description='Movie data processing and analysis pipeline',
    schedule_interval='@daily',  # Run daily after ingestion
    catchup=False,
    max_active_runs=1,
    tags=['movie', 'processing', 'spark'],
)

def check_spark_cluster(**context) -> bool:
    """Check if Spark cluster is available."""
    from pyspark.sql import SparkSession
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
        
        spark = SparkSession.builder \
            .appName("HealthCheck") \
            .master(spark_master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Simple test
        test_df = spark.range(10)
        count = test_df.count()
        
        spark.stop()
        
        logger.info(f"Spark cluster health check passed. Test count: {count}")
        return True
        
    except Exception as e:
        logger.error(f"Spark cluster health check failed: {e}")
        raise

def process_movie_data(**context) -> Dict[str, Any]:
    """Process raw movie data from Kafka."""
    from src.streaming.data_cleaner import DataCleaner
    from src.streaming.spark_config import get_spark_session
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get Spark session
        spark = get_spark_session("MovieDataProcessing")
        
        # Initialize data cleaner
        cleaner = DataCleaner(spark)
        
        # Read from Kafka
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        
        # Process movies
        movie_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "movies") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Clean and process data
        processed_movies = cleaner.clean_movie_data(movie_df)
        
        # Write to MongoDB (batch mode for this DAG)
        mongodb_uri = os.getenv('MONGODB_URI')
        
        # Convert to batch processing for DAG
        batch_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "movies") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        if batch_df.count() > 0:
            # Process the batch
            processed_batch = cleaner.clean_movie_data(batch_df)
            
            # Write to MongoDB
            processed_batch.write \
                .format("mongo") \
                .option("uri", mongodb_uri) \
                .option("database", "moviedb") \
                .option("collection", "processed_movies") \
                .mode("append") \
                .save()
            
            records_processed = processed_batch.count()
            logger.info(f"Processed {records_processed} movie records")
        else:
            records_processed = 0
            logger.info("No new movie records to process")
        
        spark.stop()
        
        return {
            'records_processed': records_processed,
            'processing_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error processing movie data: {e}")
        raise

def analyze_sentiment(**context) -> Dict[str, Any]:
    """Perform sentiment analysis on movie reviews."""
    from src.streaming.sentiment_analyzer import SentimentAnalyzer
    from src.streaming.spark_config import get_spark_session
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get Spark session
        spark = get_spark_session("SentimentAnalysis")
        
        # Initialize sentiment analyzer
        analyzer = SentimentAnalyzer(spark)
        
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        
        # Read reviews from Kafka
        reviews_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "reviews") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        if reviews_df.count() > 0:
            # Analyze sentiment
            analyzed_reviews = analyzer.analyze_review_sentiment(reviews_df)
            
            # Write results to MongoDB
            mongodb_uri = os.getenv('MONGODB_URI')
            analyzed_reviews.write \
                .format("mongo") \
                .option("uri", mongodb_uri) \
                .option("database", "moviedb") \
                .option("collection", "sentiment_analysis") \
                .mode("append") \
                .save()
            
            reviews_analyzed = analyzed_reviews.count()
            logger.info(f"Analyzed sentiment for {reviews_analyzed} reviews")
        else:
            reviews_analyzed = 0
            logger.info("No new reviews to analyze")
        
        spark.stop()
        
        return {
            'reviews_analyzed': reviews_analyzed,
            'analysis_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        raise

def generate_analytics(**context) -> Dict[str, Any]:
    """Generate analytics and aggregations."""
    from src.streaming.spark_config import get_spark_session
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get Spark session
        spark = get_spark_session("MovieAnalytics")
        
        mongodb_uri = os.getenv('MONGODB_URI')
        
        # Read processed data from MongoDB
        movies_df = spark.read \
            .format("mongo") \
            .option("uri", mongodb_uri) \
            .option("database", "moviedb") \
            .option("collection", "processed_movies") \
            .load()
        
        if movies_df.count() == 0:
            logger.info("No movie data available for analytics")
            return {'status': 'skipped', 'reason': 'no_data'}\n        \n        # Generate various analytics\n        analytics = {}\n        \n        # Top genres by count\n        if 'genres' in movies_df.columns:\n            from pyspark.sql.functions import explode, col, count, desc\n            \n            genre_stats = movies_df \\\n                .select(explode(col('genres')).alias('genre')) \\\n                .groupBy('genre') \\\n                .agg(count('*').alias('count')) \\\n                .orderBy(desc('count')) \\\n                .limit(10)\n            \n            analytics['top_genres'] = genre_stats.collect()\n        \n        # Average ratings by year\n        if 'release_date' in movies_df.columns and 'vote_average' in movies_df.columns:\n            from pyspark.sql.functions import year, avg\n            \n            yearly_ratings = movies_df \\\n                .filter(col('vote_average').isNotNull()) \\\n                .groupBy(year(col('release_date')).alias('year')) \\\n                .agg(avg('vote_average').alias('avg_rating')) \\\n                .orderBy('year')\n            \n            analytics['yearly_ratings'] = yearly_ratings.collect()\n        \n        # Most popular movies\n        if 'popularity' in movies_df.columns:\n            from pyspark.sql.functions import desc\n            \n            popular_movies = movies_df \\\n                .select('title', 'popularity', 'vote_average', 'release_date') \\\n                .orderBy(desc('popularity')) \\\n                .limit(20)\n            \n            analytics['popular_movies'] = popular_movies.collect()\n        \n        # Save analytics to MongoDB\n        analytics_doc = {\n            'generated_at': datetime.now().isoformat(),\n            'analytics': analytics,\n            'dag_run_id': context['dag_run'].run_id\n        }\n        \n        analytics_df = spark.createDataFrame([analytics_doc])\n        analytics_df.write \\\n            .format("mongo") \\\n            .option("uri", mongodb_uri) \\\n            .option("database", "moviedb") \\\n            .option("collection", "analytics") \\\n            .mode("append") \\\n            .save()\n        \n        spark.stop()\n        \n        logger.info("Analytics generation completed")\n        \n        return {\n            'analytics_generated': len(analytics),\n            'generation_time': datetime.now().isoformat(),\n            'status': 'success'\n        }\n        \n    except Exception as e:\n        logger.error(f"Error generating analytics: {e}")\n        raise\n\ndef cleanup_old_data(**context) -> Dict[str, Any]:\n    """Clean up old data to manage storage."""\n    from pymongo import MongoClient\n    import logging\n    \n    logger = logging.getLogger(__name__)\n    \n    try:\n        mongodb_uri = os.getenv('MONGODB_URI')\n        client = MongoClient(mongodb_uri)\n        db = client.moviedb\n        \n        # Calculate cutoff date (keep last 30 days)\n        cutoff_date = datetime.now() - timedelta(days=30)\n        \n        # Clean up old analytics\n        analytics_deleted = db.analytics.delete_many({\n            'generated_at': {'$lt': cutoff_date.isoformat()}\n        })\n        \n        # Clean up old sentiment analysis\n        sentiment_deleted = db.sentiment_analysis.delete_many({\n            'analyzed_at': {'$lt': cutoff_date.isoformat()}\n        })\n        \n        client.close()\n        \n        logger.info(f"Cleanup completed: {analytics_deleted.deleted_count} analytics, {sentiment_deleted.deleted_count} sentiment records")\n        \n        return {\n            'analytics_deleted': analytics_deleted.deleted_count,\n            'sentiment_deleted': sentiment_deleted.deleted_count,\n            'cleanup_time': datetime.now().isoformat(),\n            'status': 'success'\n        }\n        \n    except Exception as e:\n        logger.error(f"Error during cleanup: {e}")\n        raise\n\n# Task definitions\nstart_task = DummyOperator(\n    task_id='start_processing',\n    dag=dag,\n)\n\nspark_health_check = PythonOperator(\n    task_id='check_spark_cluster',\n    python_callable=check_spark_cluster,\n    dag=dag,\n)\n\n# Data processing tasks\nprocess_movies_task = PythonOperator(\n    task_id='process_movie_data',\n    python_callable=process_movie_data,\n    dag=dag,\n)\n\nanalyze_sentiment_task = PythonOperator(\n    task_id='analyze_sentiment',\n    python_callable=analyze_sentiment,\n    dag=dag,\n)\n\n# Analytics generation\ngenerate_analytics_task = PythonOperator(\n    task_id='generate_analytics',\n    python_callable=generate_analytics,\n    dag=dag,\n)\n\n# Cleanup task\ncleanup_task = PythonOperator(\n    task_id='cleanup_old_data',\n    python_callable=cleanup_old_data,\n    dag=dag,\n)\n\nend_task = DummyOperator(\n    task_id='end_processing',\n    dag=dag,\n)\n\n# Task dependencies\nstart_task >> spark_health_check\n\nspark_health_check >> [\n    process_movies_task,\n    analyze_sentiment_task\n]\n\n[\n    process_movies_task,\n    analyze_sentiment_task\n] >> generate_analytics_task\n\ngenerate_analytics_task >> cleanup_task >> end_task