# DataHub Integration for Data Governance

This document explains the DataHub integration for comprehensive data governance, lineage tracking, and metadata management in the Movie Data Analytics Pipeline.

## Overview

DataHub is an open-source metadata platform that provides:
- **Data Discovery**: Catalog and search data assets
- **Data Lineage**: Track data flow across systems  
- **Schema Management**: Monitor schema changes and evolution
- **Data Quality**: Track data quality metrics and issues
- **Governance**: Apply tags, ownership, and documentation

## Architecture Integration

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TMDB API      │───▶│     Kafka       │───▶│  Apache Spark   │
│   (External)    │    │  (Streaming)    │    │  (Processing)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DataHub                                  │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│  │   Data Sources  │ │   Data Lineage  │ │  Data Quality   │   │
│  │   & Schemas     │ │   & Pipelines   │ │  & Monitoring   │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  MinIO Storage  │    │  MongoDB Serving│    │  DataHub UI     │
│  (Bronze/Silver │    │     Layer       │    │  (Discovery)    │
│     /Gold)      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Components

### 1. DataHub GMS (Graph Metadata Service)
- REST API for metadata operations
- Stores metadata in MySQL/PostgreSQL
- Indexes data in Elasticsearch

### 2. DataHub Frontend
- React-based web UI
- Data discovery and exploration
- Lineage visualization

### 3. DataHub Actions
- Real-time metadata processing
- Automated governance workflows
- Integration with external systems

## Setup Instructions

### Local Development (Docker Compose)

1. **Start DataHub services:**
```bash
# Start all services including DataHub
docker-compose up -d

# Verify DataHub services
docker-compose ps | grep datahub
```

2. **Access DataHub:**
- Frontend: http://localhost:9002
- GMS API: http://localhost:8080
- Default credentials: datahub/datahub

### Production (Kubernetes)

1. **Deploy DataHub:**
```bash
# Apply DataHub configuration
kubectl apply -f kubernetes/datahub.yaml

# Check deployment status
kubectl get pods -n datahub
```

2. **Access DataHub:**
```bash
# Get external IP
kubectl get service datahub-frontend -n datahub

# Port forward for local access
kubectl port-forward service/datahub-frontend -n datahub 9002:9002
```

## Data Lineage Tracking

### Automatic Lineage

The pipeline automatically tracks lineage at these points:

1. **Data Ingestion** (TMDB API → Kafka)
2. **Stream Processing** (Kafka → MinIO Storage)
3. **Serving Layer** (MinIO → MongoDB)

### Manual Lineage

For custom lineage tracking:

```python
from src.governance.datahub_lineage import get_lineage_tracker

# Get tracker instance
tracker = get_lineage_tracker()

# Track custom dataset
dataset_urn = tracker.emit_dataset_metadata(
    platform='custom',
    dataset_name='my_dataset',
    description='Custom dataset description',
    tags=['custom', 'analytics'],
    properties={'owner': 'data-team'}
)

# Track job lineage
job_urn = tracker.emit_job_metadata(
    platform='custom',
    job_name='my_processing_job',
    job_type='PROCESSING',
    inputs=[input_urn],
    outputs=[output_urn],
    description='Custom processing job'
)

# Emit lineage relationship
tracker.emit_lineage(output_urn, [input_urn])
```

## Schema Registry Integration

DataHub integrates with Confluent Schema Registry for Kafka:

```python
# Schema evolution tracking
tracker.emit_dataset_metadata(
    platform='kafka',
    dataset_name='movies',
    schema_fields=[
        {'name': 'movie_id', 'type': 'long', 'description': 'Movie identifier'},
        {'name': 'title', 'type': 'string', 'description': 'Movie title'},
        {'name': 'release_date', 'type': 'date', 'description': 'Release date'}
    ]
)
```

## Data Quality Monitoring

### Built-in Metrics

DataHub automatically tracks:
- **Dataset freshness**: Last update timestamps
- **Schema changes**: Field additions/removals/modifications
- **Volume metrics**: Record counts and sizes

### Custom Quality Metrics

```python
# Custom data quality assertions
from datahub.metadata.schema_classes import DatasetPropertiesClass

properties = DatasetPropertiesClass(
    customProperties={
        'quality_score': '95',
        'completeness_check': 'passed',
        'data_freshness': '2024-01-15T10:30:00Z',
        'row_count': '1000000'
    }
)
```

## Governance Features

### 1. Data Classification

```python
# Apply governance tags
tracker.emit_dataset_metadata(
    platform='mongodb',
    dataset_name='user_data',
    tags=[
        'PII',           # Personal Identifiable Information
        'GDPR',          # GDPR compliance required
        'restricted',    # Access restricted
        'production'     # Production data
    ]
)
```

### 2. Data Ownership

```python
# Assign data owners
from datahub.metadata.schema_classes import OwnershipClass, OwnerClass

ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner='urn:li:corpuser:data-team',
            type='TECHNICAL_OWNER'
        )
    ]
)
```

### 3. Documentation

```python
# Add documentation
tracker.emit_dataset_metadata(
    platform='s3',
    dataset_name='gold-movies',
    description="""
    Gold layer movies dataset containing:
    - Cleaned and validated movie records
    - Enriched with genre and popularity data
    - Partitioned by release year and genre
    - Updated daily via Spark streaming jobs
    """,
    properties={
        'SLA': '99.9% availability',
        'retention_policy': '7 years',
        'backup_frequency': 'daily'
    }
)
```

## Airflow Integration

The pipeline includes a dedicated Airflow DAG for DataHub management:

```python
# DAG: datahub_metadata_management
# Schedule: @daily
# Tasks:
# 1. validate_datahub_health - Check service health
# 2. initialize_datahub_metadata - Setup base metadata
# 3. track_pipeline_lineage - Update lineage information
# 4. cleanup_stale_metadata - Remove outdated entries
```

## Monitoring and Alerting

### DataHub Metrics

Monitor these key metrics:

1. **Metadata ingestion rate**
2. **API response times**
3. **Storage utilization**
4. **Failed lineage updates**

### Integration with Grafana

```yaml
# Grafana dashboard panels
- Panel: DataHub API Response Time
  Query: avg(datahub_api_response_time)
  
- Panel: Metadata Objects Count
  Query: sum(datahub_metadata_objects_total)
  
- Panel: Lineage Updates
  Query: rate(datahub_lineage_updates[5m])
```

## Best Practices

### 1. Metadata Naming Conventions

```python
# Use consistent naming patterns
dataset_name = f"{environment}.{domain}.{entity}.{version}"
# Example: prod.movie.analytics.v1
```

### 2. Lineage Granularity

- Track at dataset level for overview
- Track at field level for critical data
- Use job-level tracking for complex pipelines

### 3. Schema Evolution

```python
# Always track schema changes
tracker.emit_dataset_metadata(
    platform='kafka',
    dataset_name='movies_v2',
    schema_fields=updated_schema,
    properties={
        'schema_version': '2.0',
        'backward_compatible': 'true',
        'migration_guide': 'https://docs.company.com/schema-migration'
    }
)
```

### 4. Data Quality

```python
# Implement quality gates
def validate_data_quality(dataset_urn, metrics):
    if metrics['completeness'] < 0.95:
        tracker.emit_dataset_metadata(
            platform='quality',
            dataset_name=dataset_urn,
            tags=['quality-issue'],
            properties={'issue': 'low_completeness'}
        )
```

## Troubleshooting

### Common Issues

1. **DataHub services not starting**
   ```bash
   # Check service logs
   docker-compose logs datahub-gms
   docker-compose logs datahub-frontend
   
   # Verify dependencies
   docker-compose ps elasticsearch mysql
   ```

2. **Lineage not appearing**
   ```python
   # Check lineage tracker initialization
   from src.governance.datahub_lineage import get_lineage_tracker
   tracker = get_lineage_tracker()
   print(f"Tracker initialized: {tracker.emitter is not None}")
   ```

3. **Metadata not updating**
   ```bash
   # Check DataHub logs for errors
   kubectl logs -n datahub deployment/datahub-gms
   
   # Verify network connectivity
   curl http://datahub-gms:8080/health
   ```

### Performance Optimization

1. **Batch metadata updates**
2. **Use async emitters for high-volume**
3. **Implement retry mechanisms**
4. **Monitor Elasticsearch performance**

## Configuration Reference

### Environment Variables

```bash
# DataHub Configuration
DATAHUB_GMS_HOST=localhost
DATAHUB_GMS_PORT=8080
DATAHUB_FRONTEND_URL=http://localhost:9002
DATAHUB_TOKEN=your-api-token
ENABLE_DATAHUB_LINEAGE=true

# Database Configuration
MYSQL_DATABASE=datahub
MYSQL_USER=datahub
MYSQL_PASSWORD=datahub
MYSQL_ROOT_PASSWORD=datahub

# Elasticsearch Configuration
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
```

### DataHub CLI

```bash
# Install DataHub CLI
pip install acryl-datahub

# Ingest metadata from file
datahub ingest -c recipe.yml

# Delete metadata
datahub delete --urn "urn:li:dataset:..."

# Check system health
datahub check
```

## Resources

- [DataHub Documentation](https://datahubproject.io/)
- [DataHub GitHub Repository](https://github.com/datahub-project/datahub)
- [DataHub Python API](https://datahubproject.io/docs/metadata-ingestion/)
- [DataHub GraphQL API](https://datahubproject.io/docs/api/graphql-api/)

## Next Steps

1. **Customize metadata schemas** for domain-specific needs
2. **Implement custom extractors** for additional data sources
3. **Set up automated data quality** monitoring
4. **Create governance policies** and enforcement rules
5. **Build custom DataHub actions** for workflow automation