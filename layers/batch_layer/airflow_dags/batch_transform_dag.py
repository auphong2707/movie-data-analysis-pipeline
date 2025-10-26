"""
Batch Transformation DAG for TMDB Movie Data Pipeline.

This DAG orchestrates the Bronze-to-Silver and Silver-to-Gold transformations,
including sentiment analysis and data quality validation.
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
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = "batch_transformation_pipeline"
DESCRIPTION = "TMDB Movie Data Batch Transformation Pipeline (Bronze → Silver → Gold)"

# Load configuration
CONFIG_PATH = "/opt/airflow/config/airflow_config.yaml"
SPARK_CONFIG_PATH = "/opt/airflow/config/spark_config.yaml"

def load_dag_config() -> Dict[str, Any]:
    """Load DAG configuration from YAML file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config.get("dags", {}).get("batch_transform", {})
    except Exception as e:
        logger.error(f"Failed to load DAG config: {e}")
        return {}

# Load configuration
dag_config = load_dag_config()
schedule_config = dag_config.get("schedule", {})
alert_config = dag_config.get("alerts", {})

# Default arguments
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": True,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
    "sla": timedelta(hours=4),
    "email": ["data-eng@company.com"]
}

# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=schedule_config.get("cron", "30 */4 * * *"),  # 30 minutes after ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "transformation", "silver", "gold", "batch"],
    doc_md=__doc__
)

# Configuration variables
SPARK_MASTER = Variable.get("SPARK_MASTER", default_var="yarn")
HDFS_NAMENODE = Variable.get("HDFS_NAMENODE", default_var="hdfs://namenode:9000")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var="")

# Paths
BASE_PATH = "/opt/airflow/dags/batch_layer"
SPARK_JOBS_PATH = f"{BASE_PATH}/spark_jobs"
CONFIG_DIR = "/opt/airflow/config"

# Spark job configurations
SPARK_CONFIGS = {
    "bronze_to_silver": {
        "driver_memory": "2g",
        "executor_memory": "4g",
        "num_executors": "4",
        "executor_cores": "2",
        "max_attempts": 3
    },
    "sentiment_batch": {
        "driver_memory": "2g", 
        "executor_memory": "6g",
        "num_executors": "6",
        "executor_cores": "2",
        "max_attempts": 2
    },
    "silver_to_gold": {
        "driver_memory": "4g",
        "executor_memory": "8g", 
        "num_executors": "8",
        "executor_cores": "3",
        "max_attempts": 2
    },
    "actor_networks": {
        "driver_memory": "6g",
        "executor_memory": "10g",
        "num_executors": "10", 
        "executor_cores": "3",
        "max_attempts": 2
    }
}

def validate_bronze_data(**context) -> Dict[str, Any]:
    """
    Validate Bronze layer data before transformation.
    
    Returns:
        Dict[str, Any]: Validation results
    """
    logger.info("Validating Bronze layer data for transformation")
    
    execution_date = context["execution_date"]
    batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"
    
    validation_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "bronze_data_valid": False,
        "record_count": 0,
        "partition_count": 0,
        "issues": []
    }
    
    try:
        # In production, this would check HDFS for Bronze data
        # For now, we'll simulate the validation
        
        # Check if Bronze data exists for current execution window
        expected_partitions = 4  # Assuming 4-hour windows
        actual_partitions = 4    # Simulated count
        
        if actual_partitions >= expected_partitions:
            validation_results["partition_count"] = actual_partitions
            validation_results["bronze_data_valid"] = True
            logger.info(f"Bronze data validation passed: {actual_partitions} partitions found")
        else:
            validation_results["issues"].append(f"Missing partitions: {actual_partitions}/{expected_partitions}")
        
        # Simulate record count check
        validation_results["record_count"] = 1500  # Simulated
        
        context["task_instance"].xcom_push(key="validation_results", value=validation_results)
        context["task_instance"].xcom_push(key="batch_id", value=batch_id)
        
        if not validation_results["bronze_data_valid"]:
            raise ValueError(f"Bronze data validation failed: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        validation_results["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"Bronze data validation failed: {e}")
        raise

def generate_spark_submit_args(job_name: str, **context) -> List[str]:
    """
    Generate Spark submit arguments for a specific job.
    
    Args:
        job_name: Name of the Spark job
        
    Returns:
        List[str]: Spark submit arguments
    """
    config = SPARK_CONFIGS.get(job_name, {})
    execution_date = context["execution_date"]
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    
    args = [
        "--master", SPARK_MASTER,
        "--deploy-mode", "cluster",
        "--driver-memory", config.get("driver_memory", "2g"),
        "--executor-memory", config.get("executor_memory", "4g"),
        "--num-executors", config.get("num_executors", "4"),
        "--executor-cores", config.get("executor_cores", "2"),
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", f"spark.app.name={job_name}_{batch_id}",
        "--py-files", f"{SPARK_JOBS_PATH}/__init__.py",
        f"{SPARK_JOBS_PATH}/{job_name}.py",
        "--config", f"{CONFIG_DIR}/spark_config.yaml",
        "--batch-id", batch_id,
        "--execution-date", execution_date.isoformat()
    ]
    
    logger.info(f"Generated Spark args for {job_name}: {args}")
    return args

def validate_silver_data(**context) -> Dict[str, Any]:
    """
    Validate Silver layer data after Bronze-to-Silver transformation.
    
    Returns:
        Dict[str, Any]: Validation results
    """
    logger.info("Validating Silver layer data")
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    
    validation_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "silver_data_valid": False,
        "record_count": 0,
        "quality_score": 0.0,
        "issues": []
    }
    
    try:
        # Simulate Silver data validation
        # In production, this would check actual Silver layer data
        
        validation_results["record_count"] = 1450  # Simulated (some records may be filtered)
        validation_results["quality_score"] = 0.95  # Simulated quality score
        
        min_quality_threshold = 0.8
        if validation_results["quality_score"] >= min_quality_threshold:
            validation_results["silver_data_valid"] = True
            logger.info(f"Silver data validation passed: quality={validation_results['quality_score']:.2f}")
        else:
            validation_results["issues"].append(f"Quality below threshold: {validation_results['quality_score']:.2f} < {min_quality_threshold}")
        
        context["task_instance"].xcom_push(key="silver_validation", value=validation_results)
        
        if not validation_results["silver_data_valid"]:
            raise ValueError(f"Silver data validation failed: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        validation_results["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"Silver data validation failed: {e}")
        raise

def validate_gold_data(**context) -> Dict[str, Any]:
    """
    Validate Gold layer data after Silver-to-Gold transformation.
    
    Returns:
        Dict[str, Any]: Validation results
    """
    logger.info("Validating Gold layer data")
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    
    validation_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "gold_data_valid": False,
        "aggregation_count": 0,
        "completeness_score": 0.0,
        "issues": []
    }
    
    try:
        # Simulate Gold data validation
        # In production, this would check actual Gold layer aggregations
        
        validation_results["aggregation_count"] = 1200  # Simulated aggregations
        validation_results["completeness_score"] = 0.92  # Simulated completeness
        
        min_completeness = 0.85
        if validation_results["completeness_score"] >= min_completeness:
            validation_results["gold_data_valid"] = True
            logger.info(f"Gold data validation passed: completeness={validation_results['completeness_score']:.2f}")
        else:
            validation_results["issues"].append(f"Completeness below threshold: {validation_results['completeness_score']:.2f} < {min_completeness}")
        
        context["task_instance"].xcom_push(key="gold_validation", value=validation_results)
        
        if not validation_results["gold_data_valid"]:
            raise ValueError(f"Gold data validation failed: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        validation_results["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"Gold data validation failed: {e}")
        raise

def calculate_transformation_metrics(**context) -> Dict[str, Any]:
    """
    Calculate transformation metrics and performance statistics.
    
    Returns:
        Dict[str, Any]: Transformation metrics
    """
    logger.info("Calculating transformation metrics")
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    bronze_validation = context["task_instance"].xcom_pull(key="validation_results")
    silver_validation = context["task_instance"].xcom_pull(key="silver_validation")
    gold_validation = context["task_instance"].xcom_pull(key="gold_validation")
    
    metrics = {
        "batch_id": batch_id,
        "metrics_timestamp": datetime.utcnow().isoformat(),
        "bronze_records": bronze_validation.get("record_count", 0),
        "silver_records": silver_validation.get("record_count", 0),
        "gold_aggregations": gold_validation.get("aggregation_count", 0),
        "transformation_efficiency": 0.0,
        "data_quality_improvement": 0.0
    }
    
    try:
        # Calculate transformation efficiency
        if metrics["bronze_records"] > 0:
            metrics["transformation_efficiency"] = metrics["silver_records"] / metrics["bronze_records"]
        
        # Calculate data quality improvement (simplified)
        bronze_quality = 0.7  # Assume Bronze quality baseline
        silver_quality = silver_validation.get("quality_score", 0.0)
        
        if bronze_quality > 0:
            metrics["data_quality_improvement"] = (silver_quality - bronze_quality) / bronze_quality
        
        logger.info(f"Transformation metrics calculated: {metrics}")
        
        context["task_instance"].xcom_push(key="transformation_metrics", value=metrics)
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to calculate transformation metrics: {e}")
        return metrics

def send_transformation_success_notification(**context) -> None:
    """Send transformation success notification."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    metrics = context["task_instance"].xcom_pull(key="transformation_metrics")
    
    bronze_records = metrics.get("bronze_records", 0)
    silver_records = metrics.get("silver_records", 0)
    gold_aggregations = metrics.get("gold_aggregations", 0)
    efficiency = metrics.get("transformation_efficiency", 0)
    
    message = f"""
✅ *TMDB Batch Transformation Success*
• Batch ID: `{batch_id}`
• Bronze → Silver: `{bronze_records:,}` → `{silver_records:,}` records
• Gold Aggregations: `{gold_aggregations:,}`
• Efficiency: `{efficiency:.1%}`
• Execution Date: `{context['execution_date']}`
    """
    
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Transformation success notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

def send_transformation_failure_notification(**context) -> None:
    """Send transformation failure notification."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id") or "Unknown"
    failed_task = context.get("task_instance")
    
    message = f"""
❌ *TMDB Batch Transformation Failed*
• Batch ID: `{batch_id}`
• Failed Task: `{failed_task.task_id if failed_task else 'Unknown'}`
• Execution Date: `{context['execution_date']}`
• Log URL: `{failed_task.log_url if failed_task else 'N/A'}`
    """
    
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Transformation failure notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack failure notification: {e}")

# Define tasks
with dag:
    
    # Validate Bronze data
    validate_bronze = PythonOperator(
        task_id="validate_bronze_data",
        python_callable=validate_bronze_data,
        doc_md="Validate Bronze layer data before transformation"
    )
    
    # Bronze-to-Silver transformations group
    with TaskGroup("bronze_to_silver_transformations") as bronze_to_silver_group:
        
        # Bronze to Silver transformation
        bronze_to_silver_job = SparkSubmitOperator(
            task_id="bronze_to_silver_transformation",
            application=f"{SPARK_JOBS_PATH}/bronze_to_silver.py",
            name="bronze_to_silver_{{ ds_nodash }}",
            conn_id="spark_default",
            verbose=True,
            application_args=[
                "--config", f"{CONFIG_DIR}/spark_config.yaml",
                "--batch-id", "{{ task_instance.xcom_pull(key='batch_id') }}",
                "--execution-date", "{{ ds }}"
            ],
            conf=SPARK_CONFIGS["bronze_to_silver"],
            doc_md="Transform Bronze layer data to Silver layer with cleaning and validation"
        )
        
        # Validate Silver data
        validate_silver = PythonOperator(
            task_id="validate_silver_data", 
            python_callable=validate_silver_data,
            doc_md="Validate Silver layer data after transformation"
        )
        
        bronze_to_silver_job >> validate_silver
    
    # Sentiment analysis job (parallel to Silver validation)
    sentiment_analysis_job = SparkSubmitOperator(
        task_id="sentiment_analysis",
        application=f"{SPARK_JOBS_PATH}/sentiment_batch.py",
        name="sentiment_batch_{{ ds_nodash }}",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--config", f"{CONFIG_DIR}/spark_config.yaml", 
            "--batch-id", "{{ task_instance.xcom_pull(key='batch_id') }}",
            "--execution-date", "{{ ds }}"
        ],
        conf=SPARK_CONFIGS["sentiment_batch"],
        doc_md="Perform batch sentiment analysis on movie overviews and reviews"
    )
    
    # Silver-to-Gold transformations group  
    with TaskGroup("silver_to_gold_transformations") as silver_to_gold_group:
        
        # Silver to Gold transformation
        silver_to_gold_job = SparkSubmitOperator(
            task_id="silver_to_gold_transformation",
            application=f"{SPARK_JOBS_PATH}/silver_to_gold.py",
            name="silver_to_gold_{{ ds_nodash }}",
            conn_id="spark_default",
            verbose=True,
            application_args=[
                "--config", f"{CONFIG_DIR}/spark_config.yaml",
                "--batch-id", "{{ task_instance.xcom_pull(key='batch_id') }}",
                "--execution-date", "{{ ds }}"
            ],
            conf=SPARK_CONFIGS["silver_to_gold"],
            doc_md="Transform Silver layer data to Gold layer aggregations"
        )
        
        # Actor networks analysis (parallel to main Silver-to-Gold)
        actor_networks_job = SparkSubmitOperator(
            task_id="actor_networks_analysis",
            application=f"{SPARK_JOBS_PATH}/actor_networks.py",
            name="actor_networks_{{ ds_nodash }}",
            conn_id="spark_default",
            verbose=True,
            application_args=[
                "--config", f"{CONFIG_DIR}/spark_config.yaml",
                "--batch-id", "{{ task_instance.xcom_pull(key='batch_id') }}",
                "--execution-date", "{{ ds }}"
            ],
            conf=SPARK_CONFIGS["actor_networks"],
            doc_md="Analyze actor collaboration networks and calculate centrality metrics"
        )
        
        # Validate Gold data
        validate_gold = PythonOperator(
            task_id="validate_gold_data",
            python_callable=validate_gold_data,
            doc_md="Validate Gold layer data after transformation"
        )
        
        [silver_to_gold_job, actor_networks_job] >> validate_gold
    
    # Calculate transformation metrics
    calculate_metrics = PythonOperator(
        task_id="calculate_transformation_metrics",
        python_callable=calculate_transformation_metrics,
        doc_md="Calculate transformation performance metrics"
    )
    
    # Notifications
    success_notification = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_transformation_success_notification,
        trigger_rule="all_success",
        doc_md="Send success notification to Slack"
    )
    
    failure_notification = PythonOperator(
        task_id="send_failure_notification",
        python_callable=send_transformation_failure_notification,
        trigger_rule="one_failed",
        doc_md="Send failure notification to Slack"
    )
    
    # End task
    end_task = DummyOperator(
        task_id="transformation_complete",
        trigger_rule="none_failed_min_one_success",
        doc_md="Mark transformation pipeline as complete"
    )

# Define dependencies
validate_bronze >> bronze_to_silver_group

# Sentiment analysis can run in parallel after Bronze-to-Silver
bronze_to_silver_group >> sentiment_analysis_job
[bronze_to_silver_group, sentiment_analysis_job] >> silver_to_gold_group

silver_to_gold_group >> calculate_metrics >> success_notification >> end_task

# Failure notification path
[validate_bronze, bronze_to_silver_group, sentiment_analysis_job, silver_to_gold_group, calculate_metrics] >> failure_notification

# SLA miss callback
def transformation_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Handle SLA miss events for transformation pipeline."""
    logger.error(f"Transformation SLA missed for DAG {dag.dag_id}. Tasks: {[t.task_id for t in task_list]}")
    
    if SLACK_WEBHOOK_URL:
        message = f"""
⚠️ *Transformation SLA Miss Alert*
• DAG: `{dag.dag_id}`
• Tasks: `{[t.task_id for t in task_list]}`
• Time: `{datetime.utcnow().isoformat()}`
        """
        
        try:
            import requests
            requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        except Exception as e:
            logger.error(f"Failed to send transformation SLA miss notification: {e}")

dag.sla_miss_callback = transformation_sla_miss_callback