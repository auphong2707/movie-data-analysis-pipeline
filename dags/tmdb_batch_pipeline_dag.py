"""
TMDB Batch Pipeline DAG

Complete batch layer pipeline that runs every 4 hours.

Tasks:
1. ingest_bronze: Extract from TMDB API and write to Bronze layer
2. transform_silver: Transform Bronze → Silver (cleaning, enrichment, sentiment)
3. aggregate_gold: Aggregate Silver → Gold (analytics, trends)
4. export_batch_views: Export Gold → MongoDB batch_views

Features:
- 4-hour schedule
- Idempotent execution
- SLA monitoring
- Retry logic with exponential backoff
- Task dependencies with proper ordering

Author: Data Engineering Team
Created: 2025-01-15
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import sys

# Add project root to path (Docker-aware)
PROJECT_ROOT = os.getenv('AIRFLOW_PROJECT_ROOT', '/app')
sys.path.append(PROJECT_ROOT)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'sla': timedelta(hours=2),  # Must complete within 2 hours
    'execution_timeout': timedelta(hours=3),  # Kill after 3 hours
}

# Define DAG
dag = DAG(
    'tmdb_batch_pipeline',
    default_args=default_args,
    description='TMDB Batch Layer Pipeline - Bronze → Silver → Gold → MongoDB',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    start_date=days_ago(1),
    catchup=False,  # Don't backfill on first deploy
    max_active_runs=1,  # Only one run at a time
    tags=['batch', 'tmdb', 'pipeline'],
)

# Project paths (Docker-aware)
PROJECT_ROOT = os.getenv('AIRFLOW_PROJECT_ROOT', '/app')
BRONZE_SCRIPT = f'{PROJECT_ROOT}/layers/batch_layer/master_dataset/ingestion.py'
SILVER_SCRIPT = f'{PROJECT_ROOT}/layers/batch_layer/spark_jobs/silver/run.py'
GOLD_SCRIPT = f'{PROJECT_ROOT}/layers/batch_layer/spark_jobs/gold/run.py'
MONGO_EXPORT_SCRIPT = f'{PROJECT_ROOT}/layers/batch_layer/batch_views/export_to_mongo.py'


def get_execution_date(**context):
    """Get execution date from Airflow context."""
    return context['execution_date'].strftime('%Y-%m-%d')


# Task 1: Ingest Bronze Layer
ingest_bronze = BashOperator(
    task_id='ingest_bronze',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {BRONZE_SCRIPT} \
        --pages 20 \
        --categories popular,top_rated,now_playing \
        2>&1 | tee logs/bronze_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 2: Transform to Silver Layer - Movies
transform_silver_movies = BashOperator(
    task_id='transform_silver_movies',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {SILVER_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --data-type movies \
        2>&1 | tee logs/silver_movies_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 3: Transform to Silver Layer - Reviews
transform_silver_reviews = BashOperator(
    task_id='transform_silver_reviews',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {SILVER_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --data-type reviews \
        2>&1 | tee logs/silver_reviews_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 4: Aggregate to Gold Layer - Genre Analytics
aggregate_gold_genre = BashOperator(
    task_id='aggregate_gold_genre',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {GOLD_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type genre_analytics \
        2>&1 | tee logs/gold_genre_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 5: Aggregate to Gold Layer - Trending
aggregate_gold_trending = BashOperator(
    task_id='aggregate_gold_trending',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {GOLD_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type trending \
        2>&1 | tee logs/gold_trending_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 6: Aggregate to Gold Layer - Temporal Analytics
aggregate_gold_temporal = BashOperator(
    task_id='aggregate_gold_temporal',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {GOLD_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type temporal_analytics \
        2>&1 | tee logs/gold_temporal_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 7: Export to MongoDB - Genre Analytics
export_mongo_genre = BashOperator(
    task_id='export_mongo_genre',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {MONGO_EXPORT_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type genre_analytics \
        2>&1 | tee logs/mongo_genre_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 8: Export to MongoDB - Trending
export_mongo_trending = BashOperator(
    task_id='export_mongo_trending',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {MONGO_EXPORT_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type trending \
        2>&1 | tee logs/mongo_trending_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Task 9: Export to MongoDB - Temporal Analytics
export_mongo_temporal = BashOperator(
    task_id='export_mongo_temporal',
    bash_command=f"""
    cd {PROJECT_ROOT} && \
    python {MONGO_EXPORT_SCRIPT} \
        --execution-date {{{{ ds }}}} \
        --metric-type temporal_analytics \
        2>&1 | tee logs/mongo_temporal_{{{{ ds }}}}.log
    """,
    dag=dag,
)

# Define task dependencies
# Bronze → Silver (both movies and reviews in parallel)
ingest_bronze >> [transform_silver_movies, transform_silver_reviews]

# Silver → Gold (all aggregations in parallel)
transform_silver_movies >> [aggregate_gold_genre, aggregate_gold_trending, aggregate_gold_temporal]
transform_silver_reviews >> [aggregate_gold_genre, aggregate_gold_trending, aggregate_gold_temporal]

# Gold → MongoDB (exports in parallel)
aggregate_gold_genre >> export_mongo_genre
aggregate_gold_trending >> export_mongo_trending
aggregate_gold_temporal >> export_mongo_temporal

# Pipeline visualization:
#
#                    ingest_bronze
#                         |
#         +---------------+---------------+
#         |                               |
#  transform_silver_movies     transform_silver_reviews
#         |                               |
#         +---------------+---------------+
#                         |
#         +---------------+---------------+
#         |               |               |
#  aggregate_gold_   aggregate_gold_  aggregate_gold_
#      genre           trending        temporal
#         |               |               |
#  export_mongo_    export_mongo_   export_mongo_
#      genre           trending        temporal
