"""
TMDB Batch Processing Pipeline - Airflow DAG

Orchestrates the complete batch layer pipeline:
Bronze → Silver → Gold → MongoDB Export

Schedule: Every 4 hours (0 */4 * * *)
SLA: < 2 hours per run
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
    'tmdb_batch_pipeline',
    default_args=default_args,
    description='TMDB Batch Layer Pipeline - Bronze → Silver → Gold → MongoDB',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'tmdb', 'etl', 'production'],
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


# Task 1: Bronze Ingestion
bronze_start = PythonOperator(
    task_id='bronze_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'bronze_ingest'},
    dag=dag,
)

bronze_ingest = PythonOperator(
    task_id='bronze_ingest',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'bronze_ingest.py',
        # Default: 250 pages/category = 5000 movies/category = ~10,000 total movies
        # To fetch maximum, add: 'args': ['--pages-per-category', '500']
        # To fetch less for testing: 'args': ['--pages-per-category', '5']
    },
    dag=dag,
)

bronze_end = PythonOperator(
    task_id='bronze_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'bronze_ingest'},
    dag=dag,
)

# Task 2: Silver Transformation
silver_start = PythonOperator(
    task_id='silver_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'silver_transform'},
    dag=dag,
)

silver_transform = PythonOperator(
    task_id='silver_transform',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'silver_transform.py',
    },
    dag=dag,
)

silver_end = PythonOperator(
    task_id='silver_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'silver_transform'},
    dag=dag,
)

# Task 3: Gold Aggregation
gold_start = PythonOperator(
    task_id='gold_start_log',
    python_callable=log_task_start,
    op_kwargs={'task_name': 'gold_aggregate'},
    dag=dag,
)

gold_aggregate = PythonOperator(
    task_id='gold_aggregate',
    python_callable=run_spark_job,
    op_kwargs={
        'job_name': 'gold_aggregate.py',
    },
    dag=dag,
)

gold_end = PythonOperator(
    task_id='gold_end_log',
    python_callable=log_task_end,
    op_kwargs={'task_name': 'gold_aggregate'},
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
bronze_start >> bronze_ingest >> bronze_end
bronze_end >> silver_start >> silver_transform >> silver_end
silver_end >> gold_start >> gold_aggregate >> gold_end
gold_end >> mongo_start >> export_to_mongo >> mongo_end
mongo_end >> validate >> success_notification
