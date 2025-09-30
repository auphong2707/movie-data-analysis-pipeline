# Apache Airflow Integration Guide

## Overview

This document outlines the Apache Airflow integration for the Movie Data Analysis Pipeline. Airflow provides advanced orchestration, scheduling, monitoring, and dependency management for all data pipeline operations.

## Architecture

### Airflow Components

- **Webserver**: Web UI for monitoring and managing workflows (Port: 8090)
- **Scheduler**: Core component that schedules and executes tasks
- **Worker**: Executes tasks using Celery executor
- **Triggerer**: Handles deferrable operators and sensors
- **PostgreSQL**: Metadata database for Airflow
- **Redis**: Message broker for Celery executor

### Integration Points

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TMDB API      │    │     Kafka       │    │    MongoDB      │
│                 │    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      │                      │
┌─────────▼──────────────────────▼──────────────────────▼───────┐
│                    Apache Airflow                             │
│  ┌───────────────┐ ┌───────────────┐ ┌─────────────────────┐  │
│  │  Ingestion    │ │  Processing   │ │   Quality           │  │
│  │     DAG       │ │     DAG       │ │  Monitoring DAG     │  │
│  └───────────────┘ └───────────────┘ └─────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Setup Instructions

### 1. Prerequisites

Ensure the following environment variables are set in your `.env` file:

```bash
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./

# TMDB API
TMDB_API_KEY=your_tmdb_api_key_here

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Email Configuration (for alerts)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
EMAIL_USER=your_email@domain.com
EMAIL_PASSWORD=your_app_password
EMAIL_FROM=airflow@company.com
ALERT_EMAIL_RECIPIENTS=admin@company.com

# Slack Configuration (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/your/webhook/url
```

### 2. Initialize Airflow

1. **Create required directories:**
   ```bash
   mkdir -p dags logs plugins
   ```

2. **Set proper permissions:**
   ```bash
   # On Linux/macOS
   sudo chown -R 50000:0 dags logs plugins
   
   # On Windows (run as Administrator)
   icacls dags /grant "Users:(OI)(CI)F"
   icacls logs /grant "Users:(OI)(CI)F"
   icacls plugins /grant "Users:(OI)(CI)F"
   ```

3. **Start Airflow services:**
   ```bash
   docker-compose up airflow-init
   docker-compose up -d
   ```

### 3. Access Airflow UI

1. Open your browser and navigate to: `http://localhost:8090`
2. Login with:
   - Username: `admin`
   - Password: `admin`

### 4. Setup Connections and Variables

Run the configuration script to set up connections and variables:

```bash
docker-compose exec airflow-webserver python /opt/airflow/config/airflow_config.py
```

Or manually configure through the Airflow UI:

#### Connections (Admin → Connections)

| Connection ID | Type | Host | Extra |
|---------------|------|------|-------|
| `tmdb_default` | HTTP | `https://api.themoviedb.org/3` | `{"api_version": "3"}` |
| `kafka_default` | Kafka | `kafka:29092` | `{"security_protocol": "PLAINTEXT"}` |
| `mongodb_default` | MongoDB | `mongodb://admin:password@mongodb:27017/moviedb?authSource=admin` | `{"database": "moviedb"}` |
| `spark_default` | Spark | `spark://spark-master:7077` | `{"deploy_mode": "client"}` |

#### Variables (Admin → Variables)

| Key | Value | Description |
|-----|-------|-------------|
| `max_popular_pages` | `3` | Number of pages to extract from popular movies |
| `quality_threshold` | `0.8` | Data quality threshold for alerts |
| `data_retention_days` | `30` | Days to retain processed data |
| `enable_sentiment_analysis` | `true` | Enable/disable sentiment analysis |

## DAG Descriptions

### 1. Movie Data Ingestion DAG (`movie_data_ingestion`)

**Schedule**: Hourly (`@hourly`)
**Purpose**: Extract data from TMDB API and stream to Kafka

#### Tasks:
- `start_ingestion`: Dummy start task
- `check_kafka_health`: Verify Kafka connectivity
- `extract_trending_movies`: Extract trending movies
- `extract_popular_movies`: Extract popular movies
- `extract_trending_people`: Extract trending people
- `extract_movie_details`: Extract detailed movie information
- `publish_extraction_metrics`: Collect and publish metrics
- `end_ingestion`: Dummy end task

#### Configuration:
```python
default_args = {
    'owner': 'data-team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'email_on_failure': True
}
```

### 2. Movie Data Processing DAG (`movie_data_processing`)

**Schedule**: Daily (`@daily`)
**Purpose**: Process raw data using Spark and perform analytics

#### Tasks:
- `start_processing`: Dummy start task
- `check_spark_cluster`: Verify Spark connectivity
- `process_movie_data`: Clean and process movie data
- `analyze_sentiment`: Perform sentiment analysis on reviews
- `generate_analytics`: Create analytics and aggregations
- `cleanup_old_data`: Remove old data beyond retention period
- `end_processing`: Dummy end task

### 3. Data Quality Monitoring DAG (`data_quality_monitoring`)

**Schedule**: Every 6 hours (`0 */6 * * *`)
**Purpose**: Monitor data quality and system health

#### Tasks:
- `start_quality_monitoring`: Dummy start task
- `validate_data_schema`: Validate data schemas
- `check_data_freshness`: Check data recency
- `check_data_volume`: Monitor data volume anomalies
- `check_kafka_lag`: Monitor Kafka consumer lag
- `generate_quality_report`: Create comprehensive quality report
- `send_quality_alert`: Send alerts if quality issues detected
- `end_quality_monitoring`: Dummy end task

## Custom Operators and Hooks

### Custom Operators

1. **TMDBExtractOperator**: Specialized TMDB data extraction
2. **SparkProcessOperator**: Spark job execution with monitoring
3. **MongoDBValidationOperator**: Data validation in MongoDB
4. **DataQualityBranchOperator**: Conditional branching based on quality
5. **KafkaHealthCheckOperator**: Kafka connectivity verification

### Custom Hooks

1. **TMDBHook**: TMDB API connection management
2. **KafkaHook**: Kafka producer/consumer management
3. **MongoDBHook**: MongoDB connection and operations
4. **SparkHook**: Spark session management

Example usage:
```python
from plugins.movie_operators import TMDBExtractOperator

extract_task = TMDBExtractOperator(
    task_id='extract_trending_movies',
    api_key='{{ var.value.tmdb_api_key }}',
    extraction_type='trending_movies',
    dag=dag
)
```

## Monitoring and Alerting

### Dashboard

Access the custom Movie Pipeline Dashboard at:
`http://localhost:8090/movie_pipeline/dashboard`

Features:
- Real-time pipeline status
- Data quality metrics
- Processing volume charts
- Failed task alerts
- System health indicators

### Email Alerts

Configured for:
- Task failures
- Data quality issues
- System health problems
- Daily summary reports

### Slack Integration

Configure Slack webhook URL in variables for:
- Critical failure notifications
- Quality threshold breaches
- Daily status updates

## Operational Procedures

### Starting the Pipeline

1. **Full system startup:**
   ```bash
   docker-compose up -d
   ```

2. **Enable DAGs** in Airflow UI (toggle switches)

3. **Trigger initial runs** (optional):
   ```bash
   # Via CLI
   docker-compose exec airflow-webserver airflow dags trigger movie_data_ingestion
   
   # Via REST API
   curl -X POST "http://localhost:8090/api/v1/dags/movie_data_ingestion/dagRuns" \
        -H "Content-Type: application/json" \
        -d '{"dag_run_id": "manual_'$(date +%Y%m%d_%H%M%S)'"}'
   ```

### Monitoring Operations

1. **Check DAG status:**
   ```bash
   docker-compose exec airflow-webserver airflow dags state movie_data_ingestion
   ```

2. **View task logs:**
   ```bash
   docker-compose exec airflow-webserver airflow tasks log movie_data_ingestion extract_trending_movies 2024-01-01
   ```

3. **Monitor resource usage:**
   ```bash
   docker stats airflow-scheduler airflow-webserver airflow-worker
   ```

### Troubleshooting

#### Common Issues

1. **DAG Import Errors:**
   ```bash
   # Check DAG syntax
   docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/movie_data_ingestion.py
   
   # View DAG import errors in UI: Browse → DAG Import Errors
   ```

2. **Task Failures:**
   - Check task logs in Airflow UI
   - Verify external system connectivity (TMDB, Kafka, MongoDB, Spark)
   - Check resource availability and limits

3. **Connection Issues:**
   ```bash
   # Test connections
   docker-compose exec airflow-webserver airflow connections test tmdb_default
   docker-compose exec airflow-webserver airflow connections test kafka_default
   ```

4. **Performance Issues:**
   - Monitor Celery worker queue: `Browse → Celery Flower` (if configured)
   - Check database connection pool: `Admin → Configurations`
   - Review DAG scheduling and concurrency settings

### Scaling Operations

#### Horizontal Scaling

Add more Celery workers:
```yaml
# docker-compose.yml
airflow-worker-2:
  <<: *airflow-worker
  container_name: airflow-worker-2
```

#### Vertical Scaling

Increase resource limits:
```yaml
# docker-compose.yml
airflow-scheduler:
  deploy:
    resources:
      limits:
        memory: 2G
        cpus: '1.0'
```

### Backup and Recovery

#### Database Backup

```bash
# Backup Airflow metadata
docker-compose exec postgres pg_dump -U airflow airflow > airflow_backup.sql

# Restore from backup
docker-compose exec -T postgres psql -U airflow airflow < airflow_backup.sql
```

#### Configuration Backup

```bash
# Backup DAGs, plugins, and configurations
tar -czf airflow_config_backup.tar.gz dags/ plugins/ config/ .env
```

## Security Considerations

### Authentication

- Default authentication is enabled with username/password
- Consider integrating with LDAP/OAuth for production
- Enable RBAC for role-based access control

### Network Security

- All services communicate within Docker network
- External access only through defined ports
- Consider using TLS/SSL for production deployments

### Secrets Management

- Use Airflow Connections for sensitive credentials
- Consider external secret management (HashiCorp Vault, AWS Secrets Manager)
- Avoid hardcoding secrets in DAG files

## Performance Optimization

### DAG Optimization

1. **Reduce DAG file size** - keep imports minimal
2. **Use connection pooling** - configure appropriate pool sizes
3. **Implement proper task dependencies** - avoid unnecessary blocking
4. **Use appropriate retry strategies** - balance reliability vs. performance

### Resource Management

1. **Configure executor settings:**
   ```python
   # airflow.cfg
   [celery]
   worker_concurrency = 16
   
   [core]
   dag_concurrency = 16
   max_active_runs_per_dag = 1
   ```

2. **Monitor and tune database connections**
3. **Implement proper logging levels**
4. **Use task pools for resource-intensive operations**

## Maintenance

### Regular Tasks

1. **Weekly:**
   - Review failed tasks and investigate root causes
   - Check disk space usage
   - Monitor performance metrics

2. **Monthly:**
   - Clean up old logs and task instances
   - Review and update DAG configurations
   - Update dependencies and security patches

3. **Quarterly:**
   - Review and optimize DAG performance
   - Assess resource requirements and scaling needs
   - Update documentation and procedures

### Log Management

Configure log rotation:
```python
# airflow.cfg
[logging]
base_log_folder = /opt/airflow/logs
dag_processor_manager_log_location = /opt/airflow/logs/dag_processor_manager/dag_processor_manager.log

# Log retention
max_log_files_per_dag = 10
```

## Integration with Existing Pipeline

### Migration Strategy

1. **Phase 1**: Deploy Airflow alongside existing pipeline
2. **Phase 2**: Migrate ingestion workflows to Airflow DAGs
3. **Phase 3**: Migrate processing workflows
4. **Phase 4**: Migrate monitoring and alerting
5. **Phase 5**: Decommission legacy scheduling mechanisms

### Compatibility

- Existing TMDB, Kafka, MongoDB, and Spark components remain unchanged
- Airflow acts as orchestration layer on top of existing services
- Gradual migration allows for testing and validation at each step

## Support and Resources

### Documentation

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Airflow API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

### Community

- [Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)
- [Stack Overflow - Apache Airflow](https://stackoverflow.com/questions/tagged/airflow)
- [GitHub Issues](https://github.com/apache/airflow/issues)

### Internal Support

- Data Team: `data-team@company.com`
- DevOps Team: `devops@company.com`
- Emergency Escalation: Follow incident response procedures