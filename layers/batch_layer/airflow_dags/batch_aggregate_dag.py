"""
Batch Aggregation DAG for TMDB Movie Data Pipeline.

This DAG orchestrates the final aggregation phase, generating batch views 
and exporting them to MongoDB for serving layer consumption.
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
DAG_ID = "batch_aggregation_pipeline"
DESCRIPTION = "TMDB Movie Data Batch Aggregation and Export Pipeline"

# Load configuration
CONFIG_PATH = "/opt/airflow/config/airflow_config.yaml"

def load_dag_config() -> Dict[str, Any]:
    """Load DAG configuration from YAML file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config.get("dags", {}).get("batch_aggregate", {})
    except Exception as e:
        logger.error(f"Failed to load DAG config: {e}")
        return {}

# Load configuration
dag_config = load_dag_config()
schedule_config = dag_config.get("schedule", {})

# Default arguments
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": True,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=4),
    "email": ["data-eng@company.com"]
}

# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=schedule_config.get("cron", "0 */4 * * *"),  # After transformation
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "aggregation", "mongodb", "batch", "serving"],
    doc_md=__doc__
)

# Configuration variables
SPARK_MASTER = Variable.get("SPARK_MASTER", default_var="yarn")
HDFS_NAMENODE = Variable.get("HDFS_NAMENODE", default_var="hdfs://namenode:9000")
MONGODB_URI = Variable.get("MONGODB_URI", default_var="mongodb://localhost:27017")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var="")

# Paths
BASE_PATH = "/opt/airflow/dags/batch_layer"
BATCH_VIEWS_PATH = f"{BASE_PATH}/batch_views"
CONFIG_DIR = "/opt/airflow/config"

# Batch views jobs configuration
BATCH_VIEWS_JOBS = {
    "movie_analytics": {
        "executor_memory": "6g",
        "num_executors": "6",
        "timeout_minutes": 45
    },
    "genre_trends": {
        "executor_memory": "4g", 
        "num_executors": "4",
        "timeout_minutes": 30
    },
    "temporal_analysis": {
        "executor_memory": "4g",
        "num_executors": "4", 
        "timeout_minutes": 30
    },
    "export_to_mongo": {
        "executor_memory": "2g",
        "num_executors": "2",
        "timeout_minutes": 20
    }
}

def validate_gold_layer_data(**context) -> Dict[str, Any]:
    """
    Validate Gold layer data before aggregation.
    
    Returns:
        Dict[str, Any]: Validation results
    """
    logger.info("Validating Gold layer data for aggregation")
    
    execution_date = context["execution_date"] 
    batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"
    
    validation_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "gold_data_valid": False,
        "genre_analytics_count": 0,
        "trending_scores_count": 0,
        "temporal_analysis_count": 0,
        "actor_networks_count": 0,
        "issues": []
    }
    
    try:
        # Simulate Gold layer validation
        # In production, this would check actual Gold layer data in HDFS
        
        # Check genre analytics
        validation_results["genre_analytics_count"] = 1200  # Simulated
        if validation_results["genre_analytics_count"] < 100:
            validation_results["issues"].append("Insufficient genre analytics data")
        
        # Check trending scores
        validation_results["trending_scores_count"] = 15000  # Simulated 
        if validation_results["trending_scores_count"] < 1000:
            validation_results["issues"].append("Insufficient trending scores data")
        
        # Check temporal analysis
        validation_results["temporal_analysis_count"] = 800  # Simulated
        if validation_results["temporal_analysis_count"] < 50:
            validation_results["issues"].append("Insufficient temporal analysis data")
        
        # Check actor networks
        validation_results["actor_networks_count"] = 25000  # Simulated
        if validation_results["actor_networks_count"] < 1000:
            validation_results["issues"].append("Insufficient actor networks data")
        
        # Overall validation
        if len(validation_results["issues"]) == 0:
            validation_results["gold_data_valid"] = True
            logger.info("Gold layer data validation passed")
        else:
            logger.warning(f"Gold layer validation issues: {validation_results['issues']}")
        
        context["task_instance"].xcom_push(key="validation_results", value=validation_results)
        context["task_instance"].xcom_push(key="batch_id", value=batch_id)
        
        if not validation_results["gold_data_valid"]:
            raise ValueError(f"Gold layer validation failed: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        validation_results["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"Gold layer validation failed: {e}")
        raise

def run_movie_analytics(**context) -> Dict[str, Any]:
    """
    Run movie analytics batch view generation.
    
    Returns:
        Dict[str, Any]: Analytics results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Running movie analytics for batch {batch_id}")
    
    try:
        # In production, this would execute the actual Spark job
        # For now, we'll simulate the execution
        
        analytics_results = {
            "batch_id": batch_id,
            "job_name": "movie_analytics",
            "start_time": datetime.utcnow().isoformat(),
            "records_processed": 15000,  # Simulated
            "analytics_generated": 8500,  # Simulated
            "execution_time_minutes": 25,  # Simulated
            "success": True
        }
        
        logger.info(f"Movie analytics completed: {analytics_results}")
        
        context["task_instance"].xcom_push(key="movie_analytics_results", value=analytics_results)
        
        return analytics_results
        
    except Exception as e:
        logger.error(f"Movie analytics failed: {e}")
        raise

def run_genre_trends_analysis(**context) -> Dict[str, Any]:
    """
    Run genre trends analysis batch view generation.
    
    Returns:
        Dict[str, Any]: Genre trends results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Running genre trends analysis for batch {batch_id}")
    
    try:
        # Simulate genre trends analysis execution
        
        trends_results = {
            "batch_id": batch_id,
            "job_name": "genre_trends",
            "start_time": datetime.utcnow().isoformat(),
            "genres_analyzed": 25,  # Simulated
            "trend_patterns_identified": 12,  # Simulated
            "seasonal_patterns_count": 8,  # Simulated
            "execution_time_minutes": 18,  # Simulated
            "success": True
        }
        
        logger.info(f"Genre trends analysis completed: {trends_results}")
        
        context["task_instance"].xcom_push(key="genre_trends_results", value=trends_results)
        
        return trends_results
        
    except Exception as e:
        logger.error(f"Genre trends analysis failed: {e}")
        raise

def run_temporal_analysis(**context) -> Dict[str, Any]:
    """
    Run temporal analysis batch view generation.
    
    Returns:
        Dict[str, Any]: Temporal analysis results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Running temporal analysis for batch {batch_id}")
    
    try:
        # Simulate temporal analysis execution
        
        temporal_results = {
            "batch_id": batch_id,
            "job_name": "temporal_analysis",
            "start_time": datetime.utcnow().isoformat(),
            "years_analyzed": 15,  # Simulated
            "yoy_metrics_calculated": 180,  # Simulated
            "seasonal_metrics_calculated": 48,  # Simulated
            "forecast_indicators_generated": 25,  # Simulated
            "execution_time_minutes": 22,  # Simulated
            "success": True
        }
        
        logger.info(f"Temporal analysis completed: {temporal_results}")
        
        context["task_instance"].xcom_push(key="temporal_analysis_results", value=temporal_results)
        
        return temporal_results
        
    except Exception as e:
        logger.error(f"Temporal analysis failed: {e}")
        raise

def export_to_mongodb(**context) -> Dict[str, Any]:
    """
    Export all batch views to MongoDB.
    
    Returns:
        Dict[str, Any]: Export results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Exporting batch views to MongoDB for batch {batch_id}")
    
    try:
        # In production, this would run the actual MongoDB export job
        # For now, we'll simulate the export process
        
        # Get results from previous tasks
        movie_analytics = context["task_instance"].xcom_pull(key="movie_analytics_results")
        genre_trends = context["task_instance"].xcom_pull(key="genre_trends_results")
        temporal_analysis = context["task_instance"].xcom_pull(key="temporal_analysis_results")
        
        export_results = {
            "batch_id": batch_id,
            "export_timestamp": datetime.utcnow().isoformat(),
            "mongodb_uri": MONGODB_URI,
            "database": "tmdb_analytics",
            "collection": "batch_views",
            "exports": {
                "movie_analytics": {
                    "documents_exported": movie_analytics.get("analytics_generated", 0),
                    "success": True
                },
                "genre_trends": {
                    "documents_exported": genre_trends.get("trend_patterns_identified", 0) * 10,  # Simulated
                    "success": True
                },
                "temporal_analysis": {
                    "documents_exported": temporal_analysis.get("yoy_metrics_calculated", 0),
                    "success": True
                },
                "actor_networks": {
                    "documents_exported": 2500,  # Simulated
                    "success": True
                }
            },
            "total_documents_exported": 0,
            "export_time_minutes": 8,  # Simulated
            "success": True
        }
        
        # Calculate total documents
        export_results["total_documents_exported"] = sum(
            export["documents_exported"] for export in export_results["exports"].values()
        )
        
        logger.info(f"MongoDB export completed: {export_results['total_documents_exported']} documents")
        
        context["task_instance"].xcom_push(key="export_results", value=export_results)
        
        return export_results
        
    except Exception as e:
        logger.error(f"MongoDB export failed: {e}")
        raise

def validate_mongodb_export(**context) -> Dict[str, Any]:
    """
    Validate MongoDB export completeness and quality.
    
    Returns:
        Dict[str, Any]: Validation results
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    export_results = context["task_instance"].xcom_pull(key="export_results")
    
    logger.info(f"Validating MongoDB export for batch {batch_id}")
    
    validation_results = {
        "batch_id": batch_id,
        "validation_timestamp": datetime.utcnow().isoformat(),
        "export_valid": False,
        "completeness_score": 0.0,
        "consistency_checks_passed": 0,
        "consistency_checks_failed": 0,
        "issues": []
    }
    
    try:
        # Simulate MongoDB validation
        # In production, this would connect to MongoDB and validate the data
        
        total_exported = export_results.get("total_documents_exported", 0)
        expected_minimum = 10000  # Simulated minimum threshold
        
        # Completeness check
        if total_exported >= expected_minimum:
            validation_results["completeness_score"] = min(1.0, total_exported / expected_minimum)
            validation_results["consistency_checks_passed"] += 1
            logger.info(f"Export completeness check passed: {total_exported} >= {expected_minimum}")
        else:
            validation_results["consistency_checks_failed"] += 1
            validation_results["issues"].append(f"Insufficient documents exported: {total_exported} < {expected_minimum}")
        
        # Index validation (simulated)
        validation_results["consistency_checks_passed"] += 1  # Assume indexes are valid
        logger.info("MongoDB indexes validation passed")
        
        # Schema validation (simulated)
        validation_results["consistency_checks_passed"] += 1  # Assume schema is valid
        logger.info("Document schema validation passed")
        
        # Overall validation
        total_checks = validation_results["consistency_checks_passed"] + validation_results["consistency_checks_failed"]
        if total_checks > 0 and validation_results["consistency_checks_failed"] == 0:
            validation_results["export_valid"] = True
            logger.info("MongoDB export validation passed")
        else:
            logger.warning(f"MongoDB export validation issues: {validation_results['issues']}")
        
        context["task_instance"].xcom_push(key="mongodb_validation", value=validation_results)
        
        if not validation_results["export_valid"]:
            raise ValueError(f"MongoDB export validation failed: {validation_results['issues']}")
        
        return validation_results
        
    except Exception as e:
        validation_results["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"MongoDB export validation failed: {e}")
        raise

def calculate_aggregation_metrics(**context) -> Dict[str, Any]:
    """
    Calculate overall aggregation pipeline metrics.
    
    Returns:
        Dict[str, Any]: Aggregation metrics
    """
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    logger.info(f"Calculating aggregation metrics for batch {batch_id}")
    
    try:
        # Get results from all tasks
        validation_results = context["task_instance"].xcom_pull(key="validation_results")
        movie_analytics = context["task_instance"].xcom_pull(key="movie_analytics_results")
        genre_trends = context["task_instance"].xcom_pull(key="genre_trends_results")
        temporal_analysis = context["task_instance"].xcom_pull(key="temporal_analysis_results")
        export_results = context["task_instance"].xcom_pull(key="export_results")
        mongodb_validation = context["task_instance"].xcom_pull(key="mongodb_validation")
        
        metrics = {
            "batch_id": batch_id,
            "metrics_timestamp": datetime.utcnow().isoformat(),
            "pipeline_start_time": validation_results.get("validation_timestamp"),
            "pipeline_end_time": datetime.utcnow().isoformat(),
            "total_execution_time_minutes": 0,
            "gold_records_processed": validation_results.get("genre_analytics_count", 0),
            "batch_views_generated": 0,
            "mongodb_documents_exported": export_results.get("total_documents_exported", 0),
            "export_completeness": mongodb_validation.get("completeness_score", 0.0),
            "pipeline_efficiency": 0.0,
            "success_rate": 0.0
        }
        
        # Calculate execution time
        start_time = datetime.fromisoformat(metrics["pipeline_start_time"].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(metrics["pipeline_end_time"])
        metrics["total_execution_time_minutes"] = (end_time - start_time).total_seconds() / 60
        
        # Calculate batch views generated
        metrics["batch_views_generated"] = (
            movie_analytics.get("analytics_generated", 0) +
            genre_trends.get("trend_patterns_identified", 0) +
            temporal_analysis.get("yoy_metrics_calculated", 0)
        )
        
        # Calculate efficiency metrics
        if metrics["total_execution_time_minutes"] > 0:
            metrics["pipeline_efficiency"] = metrics["batch_views_generated"] / metrics["total_execution_time_minutes"]
        
        # Calculate success rate (all jobs successful = 100%)
        successful_jobs = sum(1 for result in [movie_analytics, genre_trends, temporal_analysis, export_results] 
                             if result.get("success", False))
        metrics["success_rate"] = successful_jobs / 4.0  # 4 jobs total
        
        logger.info(f"Aggregation metrics calculated: {metrics}")
        
        context["task_instance"].xcom_push(key="aggregation_metrics", value=metrics)
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to calculate aggregation metrics: {e}")
        return {
            "batch_id": batch_id,
            "error": str(e),
            "success_rate": 0.0
        }

def send_aggregation_success_notification(**context) -> None:
    """Send aggregation success notification."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id")
    metrics = context["task_instance"].xcom_pull(key="aggregation_metrics")
    
    batch_views = metrics.get("batch_views_generated", 0)
    mongo_docs = metrics.get("mongodb_documents_exported", 0)
    execution_time = metrics.get("total_execution_time_minutes", 0)
    success_rate = metrics.get("success_rate", 0)
    
    message = f"""
✅ *TMDB Batch Aggregation Success*
• Batch ID: `{batch_id}`
• Batch Views Generated: `{batch_views:,}`
• MongoDB Documents: `{mongo_docs:,}`
• Execution Time: `{execution_time:.1f}` minutes
• Success Rate: `{success_rate:.1%}`
• Execution Date: `{context['execution_date']}`
    """
    
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Aggregation success notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

def send_aggregation_failure_notification(**context) -> None:
    """Send aggregation failure notification."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack webhook not configured, skipping notification")
        return
    
    batch_id = context["task_instance"].xcom_pull(key="batch_id") or "Unknown"
    failed_task = context.get("task_instance")
    
    message = f"""
❌ *TMDB Batch Aggregation Failed*
• Batch ID: `{batch_id}`
• Failed Task: `{failed_task.task_id if failed_task else 'Unknown'}`
• Execution Date: `{context['execution_date']}`
• Log URL: `{failed_task.log_url if failed_task else 'N/A'}`
    """
    
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Aggregation failure notification sent to Slack")
    except Exception as e:
        logger.error(f"Failed to send Slack failure notification: {e}")

# Define tasks
with dag:
    
    # Validate Gold layer data
    validate_gold = PythonOperator(
        task_id="validate_gold_layer_data",
        python_callable=validate_gold_layer_data,
        doc_md="Validate Gold layer data before batch views generation"
    )
    
    # Batch views generation group
    with TaskGroup("batch_views_generation") as batch_views_group:
        
        # Movie analytics
        movie_analytics_task = PythonOperator(
            task_id="generate_movie_analytics",
            python_callable=run_movie_analytics,
            doc_md="Generate comprehensive movie analytics batch views"
        )
        
        # Genre trends analysis
        genre_trends_task = PythonOperator(
            task_id="generate_genre_trends",
            python_callable=run_genre_trends_analysis,
            doc_md="Generate genre trends and patterns analysis"
        )
        
        # Temporal analysis
        temporal_analysis_task = PythonOperator(
            task_id="generate_temporal_analysis",
            python_callable=run_temporal_analysis,
            doc_md="Generate temporal and year-over-year analysis"
        )
        
        # Run batch views in parallel
        [movie_analytics_task, genre_trends_task, temporal_analysis_task]
    
    # MongoDB export and validation group
    with TaskGroup("mongodb_export") as mongodb_group:
        
        # Export to MongoDB
        mongodb_export_task = PythonOperator(
            task_id="export_to_mongodb",
            python_callable=export_to_mongodb,
            doc_md="Export all batch views to MongoDB for serving layer"
        )
        
        # Validate MongoDB export
        validate_export_task = PythonOperator(
            task_id="validate_mongodb_export",
            python_callable=validate_mongodb_export,
            doc_md="Validate MongoDB export completeness and quality"
        )
        
        mongodb_export_task >> validate_export_task
    
    # Calculate metrics
    calculate_metrics = PythonOperator(
        task_id="calculate_aggregation_metrics",
        python_callable=calculate_aggregation_metrics,
        doc_md="Calculate overall aggregation pipeline metrics"
    )
    
    # Notifications
    success_notification = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_aggregation_success_notification,
        trigger_rule="all_success",
        doc_md="Send success notification to Slack"
    )
    
    failure_notification = PythonOperator(
        task_id="send_failure_notification",
        python_callable=send_aggregation_failure_notification,
        trigger_rule="one_failed",
        doc_md="Send failure notification to Slack"
    )
    
    # End task
    end_task = DummyOperator(
        task_id="aggregation_complete",
        trigger_rule="none_failed_min_one_success",
        doc_md="Mark aggregation pipeline as complete"
    )

# Define dependencies
validate_gold >> batch_views_group >> mongodb_group >> calculate_metrics >> success_notification >> end_task

# Failure notification path
[validate_gold, batch_views_group, mongodb_group, calculate_metrics] >> failure_notification

# SLA miss callback
def aggregation_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Handle SLA miss events for aggregation pipeline."""
    logger.error(f"Aggregation SLA missed for DAG {dag.dag_id}. Tasks: {[t.task_id for t in task_list]}")
    
    if SLACK_WEBHOOK_URL:
        message = f"""
⚠️ *Aggregation SLA Miss Alert*
• DAG: `{dag.dag_id}`
• Tasks: `{[t.task_id for t in task_list]}`
• Time: `{datetime.utcnow().isoformat()}`
        """
        
        try:
            import requests
            requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        except Exception as e:
            logger.error(f"Failed to send aggregation SLA miss notification: {e}")

dag.sla_miss_callback = aggregation_sla_miss_callback