# Airbyte Integration Setup Guide

This guide explains how to set up and use Airbyte for ETL operations in the Movie Data Analysis Pipeline.

## Overview

Airbyte provides a robust, scalable ETL solution that replaces the custom `data_extractor.py` with:
- Better error handling and retry mechanisms
- Built-in connectors for various data sources
- Web UI for managing data pipelines
- Incremental sync capabilities
- Data transformation support

## Architecture

```
TMDB API → Airbyte Source → Airbyte Server → Destinations
                                              ├── Kafka Topics
                                              └── MongoDB Collections
```

## Setup Options

### Option 1: Docker Compose (Recommended for Development)

1. **Start the main infrastructure:**
   ```bash
   docker-compose up -d zookeeper kafka mongodb minio
   ```

2. **Start Airbyte services:**
   ```bash
   docker-compose -f airbyte/docker-compose.airbyte.yml up -d
   ```

3. **Access Airbyte UI:**
   - URL: http://localhost:8000
   - Username: airbyte
   - Password: password

### Option 2: Kubernetes (Production)

1. **Deploy infrastructure:**
   ```bash
   kubectl apply -f kubernetes/namespace.yaml
   kubectl apply -f kubernetes/kafka.yaml
   kubectl apply -f kubernetes/mongodb.yaml
   ```

2. **Deploy Airbyte:**
   ```bash
   kubectl apply -f kubernetes/airbyte.yaml
   ```

3. **Access Airbyte UI:**
   ```bash
   kubectl port-forward -n airbyte svc/airbyte-webapp-svc 8000:80
   ```

## Configuration

### Environment Variables

Add these to your `.env` file:

```bash
# Airbyte Configuration
AIRBYTE_HOST=localhost
AIRBYTE_PORT=8001
AIRBYTE_WORKSPACE_ID=your-workspace-id
AIRBYTE_WEBAPP_URL=http://localhost:8000

# TMDB API (Required for Airbyte source)
TMDB_API_KEY=your-tmdb-api-key
```

### Initial Setup Steps

1. **Create a Workspace:**
   - Go to http://localhost:8000
   - Create a new workspace or use the default one
   - Note the workspace ID for configuration

2. **Configure TMDB Source:**
   - Navigate to Sources → New Source
   - Select "HTTP API" connector
   - Upload the configuration from `airbyte/sources/tmdb_source_config.json`
   - Test the connection

3. **Configure Destinations:**
   
   **Kafka Destination:**
   - Navigate to Destinations → New Destination
   - Select "Kafka" connector
   - Upload the configuration from `airbyte/destinations/kafka_destination_config.json`
   
   **MongoDB Destination:**
   - Select "MongoDB" connector
   - Upload the configuration from `airbyte/destinations/mongodb_destination_config.json`

4. **Create Connections:**
   - Navigate to Connections → New Connection
   - Select your TMDB source and Kafka/MongoDB destination
   - Configure sync schedules and data mapping
   - Use the configurations from `airbyte/connections/`

## Usage

### Running with Airbyte (Default)

```bash
# Batch extraction via Airbyte
python -m src.ingestion.main --airbyte batch

# Continuous monitoring
python -m src.ingestion.main --airbyte continuous

# Check sync status
python -m src.ingestion.main --airbyte status
```

### Running with Legacy Extractor

```bash
# Use the original data_extractor.py
python -m src.ingestion.main --legacy batch
```

## Data Flows

### Supported TMDB Endpoints

The Airbyte configuration includes these TMDB API endpoints:
- `popular_movies` - Most popular movies
- `top_rated_movies` - Highest rated movies  
- `now_playing_movies` - Currently playing movies
- `upcoming_movies` - Upcoming releases

### Sync Modes

- **Full Refresh**: Replaces all data in destination
- **Incremental**: Only syncs new/changed records
- **Upsert**: Updates existing records and inserts new ones

### Data Destinations

1. **Kafka Topics:**
   - `movies-popular_movies`
   - `movies-top_rated_movies`
   - `movies-now_playing_movies`
   - `movies-upcoming_movies`

2. **MongoDB Collections:**
   - `popular_movies`
   - `top_rated_movies`
   - `now_playing_movies`
   - `upcoming_movies`

## Monitoring & Troubleshooting

### Health Checks

```bash
# Check Airbyte service health
curl http://localhost:8001/api/v1/health

# Monitor active syncs
python -c "
from src.ingestion.airbyte_manager import AirbyteManager
manager = AirbyteManager()
workspaces = manager.get_workspaces()
print(workspaces)
"
```

### Common Issues

1. **Connection Failed:**
   - Verify TMDB API key is valid
   - Check network connectivity
   - Ensure Airbyte services are running

2. **Sync Stuck:**
   - Check Airbyte worker logs: `docker logs airbyte-worker`
   - Restart Airbyte services if needed
   - Verify destination connectivity

3. **Schema Errors:**
   - Update schema definitions in source configurations
   - Clear cache and retry sync
   - Check data transformation rules

### Log Locations

- **Docker Logs:** `docker logs <container-name>`
- **Kubernetes Logs:** `kubectl logs -n airbyte <pod-name>`
- **Airbyte UI:** View sync logs in the Connections tab

## Data Transformations

Airbyte supports SQL-based transformations using dbt. Example transformations are in `airbyte/transformations/`:

- `clean_movies.sql` - Data cleaning and validation
- Custom transformations for sentiment analysis prep
- Aggregation queries for analytics

## Migration from Legacy Extractor

### Step-by-Step Migration

1. **Set up Airbyte as described above**

2. **Run both systems in parallel:**
   ```bash
   # Terminal 1: Legacy extractor
   python -m src.ingestion.main --legacy batch
   
   # Terminal 2: Airbyte sync
   python -m src.ingestion.main --airbyte batch
   ```

3. **Compare data quality and completeness**

4. **Switch to Airbyte-only:**
   ```bash
   # Update docker-compose.yml profiles
   docker-compose --profile legacy-ingestion down
   ```

### Benefits of Migration

- **Reliability:** Built-in retry mechanisms and error handling
- **Scalability:** Better resource management and parallel processing
- **Monitoring:** Rich UI for tracking sync status and history
- **Maintenance:** Reduces custom code maintenance burden
- **Features:** Incremental syncs, data deduplication, schema evolution

## Production Considerations

### Resource Requirements

- **Memory:** 4GB minimum for Airbyte services
- **CPU:** 2+ cores recommended
- **Storage:** 20GB+ for logs and temporary data
- **Network:** Stable internet for TMDB API calls

### Security

- Use environment variables for sensitive data
- Enable HTTPS for production deployments
- Configure proper authentication for databases
- Set up VPC/network security groups

### High Availability

- Deploy Airbyte on Kubernetes with multiple replicas
- Use managed databases (RDS, Atlas) for metadata storage
- Set up monitoring and alerting
- Implement backup and disaster recovery

## Integration with Existing Pipeline

Airbyte integrates seamlessly with the existing components:

- **Kafka:** Continues to receive movie data via Airbyte
- **Spark Streaming:** Processes data from Kafka topics as before
- **MongoDB:** Stores processed results from both Airbyte and Spark
- **APIs:** Serve data from MongoDB collections
- **Visualization:** Superset/Grafana connect to same data sources

The pipeline remains functionally identical while gaining improved reliability and maintainability.