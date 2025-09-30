"""
Airflow configuration for movie data pipeline.
Custom configuration, connections, and variables setup.
"""
from airflow.models import Connection, Variable
from airflow.utils.db import create_session
from airflow import settings
import os

def setup_connections():
    """Setup Airflow connections for the movie data pipeline."""
    
    connections = [
        # TMDB Connection
        {
            'conn_id': 'tmdb_default',
            'conn_type': 'http',
            'host': 'https://api.themoviedb.org/3',
            'password': os.getenv('TMDB_API_KEY'),
            'extra': '{"api_version": "3"}'
        },
        
        # Kafka Connection
        {
            'conn_id': 'kafka_default',
            'conn_type': 'kafka',
            'host': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'extra': '{"security_protocol": "PLAINTEXT"}'
        },
        
        # MongoDB Connection
        {
            'conn_id': 'mongodb_default',
            'conn_type': 'mongodb',
            'host': os.getenv('MONGODB_URI', 'mongodb://admin:password@mongodb:27017/moviedb?authSource=admin'),
            'extra': '{"database": "moviedb"}'
        },
        
        # Spark Connection
        {
            'conn_id': 'spark_default',
            'conn_type': 'spark',
            'host': os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077'),
            'extra': '{"deploy_mode": "client", "spark_version": "3.4.1"}'
        },
        
        # Email Connection for Alerts
        {
            'conn_id': 'email_default',
            'conn_type': 'email',
            'host': os.getenv('SMTP_HOST', 'smtp.gmail.com'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'login': os.getenv('EMAIL_USER'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'extra': '{"use_tls": true}'
        }
    ]
    
    session = settings.Session()
    
    for conn_config in connections:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == conn_config['conn_id']
        ).first()
        
        if existing_conn:
            # Update existing connection
            for key, value in conn_config.items():
                if key != 'conn_id':
                    setattr(existing_conn, key, value)
        else:
            # Create new connection
            new_conn = Connection(**conn_config)
            session.add(new_conn)
    
    session.commit()
    session.close()

def setup_variables():
    """Setup Airflow variables for the movie data pipeline."""
    
    variables = {
        # Data extraction settings
        'max_popular_pages': '3',
        'extraction_interval_hours': '1',
        'detail_extraction_limit': '10',
        
        # Data quality settings
        'quality_threshold': '0.8',
        'schema_validation_sample_size': '100',
        'data_freshness_hours': '24',
        
        # Processing settings
        'spark_executor_memory': '2g',
        'spark_executor_cores': '2',
        'spark_driver_memory': '1g',
        
        # Monitoring settings
        'alert_email_recipients': os.getenv('ALERT_EMAIL_RECIPIENTS', 'admin@company.com'),
        'slack_webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
        'enable_slack_alerts': 'true',
        'enable_email_alerts': 'true',
        
        # Cleanup settings
        'data_retention_days': '30',
        'log_retention_days': '7',
        
        # API rate limiting
        'tmdb_api_rate_limit': '40',  # requests per 10 seconds
        'api_retry_attempts': '3',
        'api_retry_delay_seconds': '5',
        
        # Feature flags
        'enable_sentiment_analysis': 'true',
        'enable_real_time_processing': 'true',
        'enable_data_validation': 'true'
    }
    
    for key, value in variables.items():
        Variable.set(key, value, serialize_json=False)

def setup_email_config():
    """Setup email configuration for Airflow."""
    email_config = {
        'AIRFLOW__EMAIL__EMAIL_BACKEND': 'airflow.utils.email.send_email_smtp',
        'AIRFLOW__SMTP__SMTP_HOST': os.getenv('SMTP_HOST', 'smtp.gmail.com'),
        'AIRFLOW__SMTP__SMTP_STARTTLS': 'True',
        'AIRFLOW__SMTP__SMTP_SSL': 'False',
        'AIRFLOW__SMTP__SMTP_PORT': os.getenv('SMTP_PORT', '587'),
        'AIRFLOW__SMTP__SMTP_MAIL_FROM': os.getenv('EMAIL_FROM', 'airflow@company.com'),
        'AIRFLOW__SMTP__SMTP_USER': os.getenv('EMAIL_USER'),
        'AIRFLOW__SMTP__SMTP_PASSWORD': os.getenv('EMAIL_PASSWORD')
    }
    
    # Set environment variables for email configuration
    for key, value in email_config.items():
        if value:
            os.environ[key] = value

# Custom email templates
def create_failure_email_template():
    """Create custom failure email template."""
    return '''
    <h2>🚨 Airflow Task Failure Alert</h2>
    
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Task:</strong> {{ task.task_id }}</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Run ID:</strong> {{ dag_run.run_id }}</p>
    <p><strong>Log URL:</strong> <a href="{{ task_instance.log_url }}">View Logs</a></p>
    
    <h3>Failure Details:</h3>
    <pre>{{ task_instance.log }}</pre>
    
    <h3>Recommended Actions:</h3>
    <ul>
        <li>Check the task logs for detailed error information</li>
        <li>Verify external system connectivity (TMDB API, Kafka, MongoDB, Spark)</li>
        <li>Check data quality and volume metrics</li>
        <li>Review recent configuration changes</li>
    </ul>
    
    <p><em>This is an automated alert from the Movie Data Pipeline.</em></p>
    '''

def create_success_email_template():
    """Create custom success email template."""
    return '''
    <h2>✅ Airflow Pipeline Success</h2>
    
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Duration:</strong> {{ dag_run.duration }}</p>
    
    <h3>Execution Summary:</h3>
    <ul>
        <li>All tasks completed successfully</li>
        <li>Data extraction, processing, and validation completed</li>
        <li>Quality checks passed</li>
    </ul>
    
    <p><em>This is an automated notification from the Movie Data Pipeline.</em></p>
    '''

if __name__ == "__main__":
    """Run setup when executed directly."""
    print("Setting up Airflow connections...")
    setup_connections()
    
    print("Setting up Airflow variables...")
    setup_variables()
    
    print("Setting up email configuration...")
    setup_email_config()
    
    print("Airflow configuration setup completed!")