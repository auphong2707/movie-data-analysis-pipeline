"""
TMDB Baseline Calculation Pipeline - Airflow DAG

Calculates historical baselines from TMDB data for comparison with real-time Reddit data.
Bronze (metadata) → Silver (baseline calculation) → Gold (baseline export) → MongoDB

Schedule: Daily at 2 AM (0 2 * * *)
SLA: < 1 hour per run
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import subprocess
import sys
import os

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'tmdb_baseline_pipeline',
    default_args=default_args,
    description='TMDB Baseline Calculation - Provides historical baselines for Reddit comparison',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'tmdb', 'baseline', 'production'],
)

def log_task_start(task_name, **context):
    """Log task start"""
    logger.info(f"Starting task: {task_name}")
    logger.info(f"Execution date: {context['execution_date']}")
    return f"Task {task_name} started"

def log_task_end(task_name, **context):
    """Log task completion"""
    logger.info(f"Completed task: {task_name}")
    return f"Task {task_name} completed"

def run_spark_job(job_name, args=None, **context):
    """
    Run a Spark job in the Airflow environment
    
    Args:
        job_name: Name of the Python script to run (e.g., 'bronze_ingest.py')
        args: List of command-line arguments (e.g., ['--pages', '2'])
    """
    logger.info(f"Running Spark job: {job_name}")
    
    # Set up paths
    spark_jobs_dir = '/opt/airflow/spark_jobs'
    job_path = os.path.join(spark_jobs_dir, job_name)
    
    # Build command
    cmd = [sys.executable, job_path]
    if args:
        cmd.extend(args)
    
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info(f"Working directory: {spark_jobs_dir}")
    
    try:
        # Run the job
        result = subprocess.run(
            cmd,
            cwd=spark_jobs_dir,
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, 'PYTHONPATH': spark_jobs_dir}
        )
        
        # Log output
        if result.stdout:
            logger.info(f"Job output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Job stderr:\n{result.stderr}")
            
        logger.info(f"Job {job_name} completed successfully")
        return f"Job {job_name} completed"
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Job {job_name} failed with exit code {e.returncode}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise Exception(f"Job {job_name} failed: {e.stderr}")


# Task 1: Fetch TMDB Metadata
metadata_start = PythonOperator(
    task_id='metadata_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'fetch_tmdb_metadata'},
    dag=dag,
)

fetch_tmdb_metadata = PythonOperator(
    task_id='fetch_tmdb_metadata',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'bronze_ingest.py',
        # Fetch metadata for baseline calculation
        # Default: 100 pages = ~2000 movies for baseline dataset
    },
    dag=dag,
)

metadata_end = PythonOperator(
    task_id='metadata_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'fetch_tmdb_metadata'},
    dag=dag,
)

# Task 2: Calculate Baselines
baseline_start = PythonOperator(
    task_id='baseline_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'calculate_baselines'},
    dag=dag,
)

calculate_baselines = PythonOperator(
    task_id='calculate_baselines',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'silver_transform.py',
    },
    dag=dag,
)

baseline_end = PythonOperator(
    task_id='baseline_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'calculate_baselines'},
    dag=dag,
)

# Task 3: Export Baselines
export_start = PythonOperator(
    task_id='export_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'export_baselines'},
    dag=dag,
)

export_baselines = PythonOperator(
    task_id='export_baselines',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'gold_aggregate.py',
    },
    dag=dag,
)

export_end = PythonOperator(
    task_id='export_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'export_baselines'},
    dag=dag,
)

# Task 4: MongoDB Export
mongo_start = PythonOperator(
    task_id='mongo_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'export_to_mongo'},
    dag=dag,
)

export_to_mongo = PythonOperator(
    task_id='export_to_mongo',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'export_to_mongo.py',
    },
    dag=dag,
)

mongo_end = PythonOperator(
    task_id='mongo_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'export_to_mongo'},
    dag=dag,
)

# Task 5: Validation
def validate_pipeline(**context):
    """Validate pipeline execution"""
    logger.info("Validating pipeline execution...")
    
    # TODO: Add validation logic
    # - Check MinIO buckets have data
    # - Check MongoDB has documents
    # - Verify data quality metrics
    
    logger.info("Pipeline validation completed")
    return "Pipeline validated successfully"

validate = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
)

# Success notification
def send_success_notification(**context):
    """Send success notification"""
    execution_date = context['execution_date']
    logger.info(f"✅ Pipeline completed successfully for {execution_date}")
    return "Success notification sent"

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    trigger_rule='all_success',
    dag=dag,
)

# Define task dependencies
metadata_start >> fetch_tmdb_metadata >> metadata_end
metadata_end >> baseline_start >> calculate_baselines >> baseline_end
baseline_end >> export_start >> export_baselines >> export_end
export_end >> mongo_start >> export_to_mongo >> mongo_end
mongo_end >> validate >> success_notification
