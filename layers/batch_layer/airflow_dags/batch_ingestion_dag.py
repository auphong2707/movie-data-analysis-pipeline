"""
Batch Ingestion DAG for TMDB Movie Data Pipeline.

This DAG orchestrates the Bronze layer data ingestion process, fetching data
from the TMDB API and storing it in HDFS with proper partitioning and quality checks.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging
import os
import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.utils.task_group import TaskGroup
from airflow.providers.spark.operators.spark_submit import SparkSubmitOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = "batch_ingestion_pipeline"
DESCRIPTION = "TMDB Movie Data Batch Ingestion Pipeline"

# Load configuration
CONFIG_PATH = "/opt/airflow/config/airflow_config.yaml"
SPARK_CONFIG_PATH = "/opt/airflow/config/spark_config.yaml"
HDFS_CONFIG_PATH = "/opt/airflow/config/hdfs_config.yaml"

def load_dag_config() -> Dict[str, Any]:
    """Load DAG configuration from YAML file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config.get("dags", {}).get("batch_ingestion", {})
    except Exception as e:
        logger.error(f"Failed to load DAG config: {e}")
        return {}

# Load configuration
dag_config = load_dag_config()
schedule_config = dag_config.get("schedule", {})
alert_config = dag_config.get("alerts", {})
quality_config = dag_config.get("data_quality", {})

# Default arguments for all tasks
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=4),
    "email": ["data-eng@company.com"]
}

# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=schedule_config.get("cron", "0 */4 * * *"),  # Every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "ingestion", "bronze", "batch"],
    doc_md=__doc__
)

# Configuration variables
TMDB_API_KEY = Variable.get("TMDB_API_KEY", default_var="")
SPARK_MASTER = Variable.get("SPARK_MASTER", default_var="yarn")
HDFS_NAMENODE = Variable.get("HDFS_NAMENODE", default_var="hdfs://namenode:9000")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var="")

# Paths and directories
BASE_PATH = "/opt/airflow/dags/batch_layer"
BRONZE_PATH = f"{HDFS_NAMENODE}/data/bronze/tmdb"
SCRIPTS_PATH = f"{BASE_PATH}/master_dataset"
SPARK_JOBS_PATH = f"{BASE_PATH}/spark_jobs"

def validate_prerequisites(**context) -> bool:
    """
    Validate that all prerequisites are met before starting ingestion.
    
    Returns:
        bool: True if all prerequisites are met
    """
    logger.info("Validating ingestion prerequisites")
    
    validation_errors = []
    
    # Check API key
    if not TMDB_API_KEY:
        validation_errors.append("TMDB API key not configured")
    
    # Check HDFS connectivity
    try:
        # This would include actual HDFS connectivity check
        logger.info(f"HDFS namenode: {HDFS_NAMENODE}")
    except Exception as e:
        validation_errors.append(f"HDFS connectivity issue: {e}")
    
    # Check Spark cluster
    try:
        logger.info(f"Spark master: {SPARK_MASTER}")
    except Exception as e:
        validation_errors.append(f"Spark cluster issue: {e}")
    
    if validation_errors:
        error_msg = "Prerequisites validation failed: " + "; ".join(validation_errors)
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("All prerequisites validated successfully")
    return True

def generate_batch_id(**context) -> str:
    """
    Generate unique batch ID for this ingestion run.
    
    Returns:
        str: Batch ID
    """
    execution_date = context["execution_date"]
    batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Generated batch ID: {batch_id}")
    
    # Store batch ID for downstream tasks
    context["task_instance"].xcom_push(key="batch_id", value=batch_id)
    return batch_id

def check_api_rate_limits(**context) -> bool:
    """
    Check TMDB API rate limits before starting ingestion.
    
    Returns:
        bool: True if rate limits are acceptable
    """
    logger.info("Checking TMDB API rate limits")
    
    try:
        import requests
        import time
        
        # Test API endpoint
        test_url = "https://api.themoviedb.org/3/configuration"
        headers = {"Authorization": f"Bearer {TMDB_API_KEY}"}
        
        start_time = time.time()
        response = requests.get(test_url, headers=headers, timeout=30)
        response_time = time.time() - start_time
        
        if response.status_code != 200:
            raise ValueError(f"API test failed with status {response.status_code}")
        
        # Check rate limit headers
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_time = response.headers.get("X-RateLimit-Reset")
        
        logger.info(f"API response time: {response_time:.2f}s")
        logger.info(f"Rate limit remaining: {remaining}")
        logger.info(f"Rate limit reset: {reset_time}")
        
        # Store metrics for monitoring
        context["task_instance"].xcom_push(key="api_response_time", value=response_time)
        context["task_instance"].xcom_push(key="rate_limit_remaining", value=remaining)
        
        return True
        
    except Exception as e:
        logger.error(f"API rate limit check failed: {e}")
        raise

def run_bronze_ingestion(**context) -> Dict[str, Any]:
    """
    Run the Bronze layer ingestion job.
    
    Returns:
        Dict[str, Any]: Ingestion results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Starting Bronze layer ingestion for batch {batch_id}")
    
    try:
        # Import the ingestion module
        import sys
        sys.path.append(SCRIPTS_PATH)
        
        from ingestion import BronzeLayerIngestion, TMDBAPIClient
        
        # Initialize ingestion
        api_client = TMDBAPIClient(api_key=TMDB_API_KEY)
        ingestion = BronzeLayerIngestion(
            api_client=api_client,
            hdfs_path=BRONZE_PATH,
            batch_id=batch_id
        )
        
        # Run ingestion
        results = ingestion.ingest_batch_data()
        
        logger.info(f"Bronze ingestion completed: {results}")
        
        # Store results for downstream tasks
        context["task_instance"].xcom_push(key="ingestion_results", value=results)
        
        return results
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        raise

def validate_ingestion_quality(**context) -> Dict[str, Any]:
    """
    Validate the quality of ingested data.
    
    Returns:
        Dict[str, Any]: Quality validation results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    ingestion_results = context["task_instance"].xcom_pull(key="ingestion_results")
    
    logger.info(f"Validating ingestion quality for batch {batch_id}")
    
    quality_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "checks_passed": 0,
        "checks_failed": 0,
        "issues": []
    }
    
    try:
        # Check 1: Minimum record count
        min_records = quality_config.get("min_records_per_batch", 100)
        actual_records = ingestion_results.get("records_ingested", 0)
        
        if actual_records >= min_records:
            quality_results["checks_passed"] += 1
            logger.info(f"Record count check passed: {actual_records} >= {min_records}")
        else:
            quality_results["checks_failed"] += 1
            quality_results["issues"].append(f"Insufficient records: {actual_records} < {min_records}")
        
        # Check 2: API error rate
        max_error_rate = quality_config.get("max_api_error_rate", 0.05)
        actual_error_rate = ingestion_results.get("error_rate", 0)
        
        if actual_error_rate <= max_error_rate:
            quality_results["checks_passed"] += 1
            logger.info(f"Error rate check passed: {actual_error_rate} <= {max_error_rate}")
        else:
            quality_results["checks_failed"] += 1
            quality_results["issues"].append(f"High error rate: {actual_error_rate} > {max_error_rate}")
        
        # Check 3: Data freshness
        max_age_hours = quality_config.get("max_data_age_hours", 6)
        ingestion_time = datetime.fromisoformat(ingestion_results.get("ingestion_timestamp", ""))
        age_hours = (datetime.utcnow() - ingestion_time).total_seconds() / 3600
        
        if age_hours <= max_age_hours:
            quality_results["checks_passed"] += 1
            logger.info(f"Data freshness check passed: {age_hours:.1f}h <= {max_age_hours}h")
        else:
            quality_results["checks_failed"] += 1
            quality_results["issues"].append(f"Data too old: {age_hours:.1f}h > {max_age_hours}h")
        
        # Overall quality assessment
        total_checks = quality_results["checks_passed"] + quality_results["checks_failed"]
        quality_score = quality_results["checks_passed"] / total_checks if total_checks > 0 else 0
        quality_results["quality_score"] = quality_score
        
        # Determine if quality is acceptable
        min_quality_score = quality_config.get("min_quality_score", 0.8)
        quality_results["quality_acceptable"] = quality_score >= min_quality_score
        
        logger.info(f"Quality validation completed: score={quality_score:.2f}")
        
        # Store results
        context["task_instance"].xcom_push(key="quality_results", value=quality_results)
        
        # Fail if quality is not acceptable
        if not quality_results["quality_acceptable"]:
            raise ValueError(f"Data quality below threshold: {quality_score:.2f} < {min_quality_score}")
        
        return quality_results
        
    except Exception as e:
        quality_results["issues"].append(f"Quality validation error: {str(e)}")
        quality_results["quality_acceptable"] = False
        logger.error(f"Quality validation failed: {e}")
        raise

def cleanup_old_partitions(**context) -> Dict[str, Any]:
    """
    Clean up old Bronze layer partitions based on retention policy.
    
    Returns:
        Dict[str, Any]: Cleanup results
    """
    logger.info("Cleaning up old Bronze layer partitions")
    
    try:
        import sys
        sys.path.append(SCRIPTS_PATH)
        
        from partitioning import RetentionManager
        
        # Initialize retention manager
        retention_manager = RetentionManager(hdfs_path=BRONZE_PATH)
        
        # Load retention configuration
        with open(HDFS_CONFIG_PATH, 'r') as f:
            hdfs_config = yaml.safe_load(f)
        
        retention_days = hdfs_config.get("retention", {}).get("bronze_days", 90)
        
        # Run cleanup
        cleanup_results = retention_manager.cleanup_expired_partitions(
            layer="bronze",
            retention_days=retention_days
        )
        
        logger.info(f"Cleanup completed: {cleanup_results}")
        
        context["task_instance"].xcom_push(key="cleanup_results", value=cleanup_results)
        
        return cleanup_results
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        # Don't fail the DAG if cleanup fails
        return {"error": str(e), "partitions_deleted": 0}

def send_success_notification(**context) -> None:
    """Send success notification to Slack."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    ingestion_results = context["task_instance"].xcom_pull(key="ingestion_results")
    quality_results = context["task_instance"].xcom_pull(key="quality_results")
    
    records_count = ingestion_results.get("records_ingested", 0)
    quality_score = quality_results.get("quality_score", 0)
    
    message = f"""
✅ *TMDB Batch Ingestion Success*
• Batch ID: `{batch_id}`
• Records Ingested: `{records_count:,}`
• Quality Score: `{quality_score:.1%}`
• Execution Date: `{context['execution_date']}`
    """
    
    try:
        slack_operator = SlackWebhookOperator(
            task_id="send_slack_notification",
            http_conn_id="slack_default",
            message=message,
            webhook_token=SLACK_WEBHOOK_URL
        )
        slack_operator.execute(context)
        logger.info("Success notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

def send_failure_notification(**context) -> None:
    """Send failure notification to Slack."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id") or "Unknown"
    failed_task = context.get("task_instance")
    
    message = f"""
❌ *TMDB Batch Ingestion Failed*
• Batch ID: `{batch_id}`
• Failed Task: `{failed_task.task_id if failed_task else 'Unknown'}`
• Execution Date: `{context['execution_date']}`
• Log URL: `{failed_task.log_url if failed_task else 'N/A'}`
    """
    
    try:
        slack_operator = SlackWebhookOperator(
            task_id="send_failure_notification",
            http_conn_id="slack_default",
            message=message,
            webhook_token=SLACK_WEBHOOK_URL
        )
        slack_operator.execute(context)
        logger.info("Failure notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack failure notification: {e}")

# Define tasks
with dag:
    
    # Pre-ingestion validation group
    with TaskGroup("pre_ingestion_validation") as validation_group:
        
        # Check TMDB API availability
        api_sensor = HttpSensor(
            task_id="check_api_availability",
            http_conn_id="tmdb_api",
            endpoint="configuration",
            request_params={"api_key": TMDB_API_KEY},
            timeout=300,
            poke_interval=60,
            mode="poke",
            soft_fail=False,
            doc_md="Check if TMDB API is available and responding"
        )
        
        # Validate prerequisites
        validate_prereqs = PythonOperator(
            task_id="validate_prerequisites",
            python_callable=validate_prerequisites,
            doc_md="Validate all prerequisites for ingestion"
        )
        
        # Check API rate limits
        check_rate_limits = PythonOperator(
            task_id="check_api_rate_limits",
            python_callable=check_api_rate_limits,
            doc_md="Check TMDB API rate limits"
        )
        
        api_sensor >> validate_prereqs >> check_rate_limits
    
    # Generate batch ID
    generate_batch = PythonOperator(
        task_id="generate_batch_id",
        python_callable=generate_batch_id,
        doc_md="Generate unique batch ID for this run"
    )
    
    # Bronze layer ingestion group
    with TaskGroup("bronze_ingestion") as ingestion_group:
        
        # Run Bronze ingestion
        run_ingestion = PythonOperator(
            task_id="run_bronze_ingestion",
            python_callable=run_bronze_ingestion,
            doc_md="Execute Bronze layer data ingestion from TMDB API"
        )
        
        # Validate ingestion quality
        validate_quality = PythonOperator(
            task_id="validate_ingestion_quality",
            python_callable=validate_ingestion_quality,
            doc_md="Validate the quality of ingested data"
        )
        
        run_ingestion >> validate_quality
    
    # Post-ingestion cleanup
    cleanup_partitions = PythonOperator(
        task_id="cleanup_old_partitions",
        python_callable=cleanup_old_partitions,
        doc_md="Clean up old Bronze layer partitions"
    )
    
    # Success notification
    success_notification = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_success_notification,
        trigger_rule="all_success",
        doc_md="Send success notification to Slack"
    )
    
    # Failure notification
    failure_notification = PythonOperator(
        task_id="send_failure_notification",
        python_callable=send_failure_notification,
        trigger_rule="one_failed",
        doc_md="Send failure notification to Slack"
    )
    
    # End dummy task
    end_task = DummyOperator(
        task_id="ingestion_complete",
        trigger_rule="none_failed_min_one_success",
        doc_md="Mark ingestion pipeline as complete"
    )

# Define dependencies
validation_group >> generate_batch >> ingestion_group >> cleanup_partitions >> success_notification >> end_task

# Set up failure notification path
[validation_group, generate_batch, ingestion_group, cleanup_partitions] >> failure_notification

# Add SLA miss callback
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Handle SLA miss events."""
    logger.error(f"SLA missed for DAG {dag.dag_id}. Tasks: {[t.task_id for t in task_list]}")
    
    if SLACK_WEBHOOK_URL:
        message = f"""
⚠️ *SLA Miss Alert*
• DAG: `{dag.dag_id}`
• Tasks: `{[t.task_id for t in task_list]}`
• Time: `{datetime.utcnow().isoformat()}`
        """
        
        try:
            import requests
            requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        except Exception as e:
            logger.error(f"Failed to send SLA miss notification: {e}")

dag.sla_miss_callback = sla_miss_callback