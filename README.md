# Movie Data Analytics Pipeline - Updated with Apache Airflow

A comprehensive big data analytics pipeline for movie data using Kappa Architecture with **Apache Airflow** orchestration.

## Architecture Overview

This project implements a real-time movie data analytics pipeline with advanced orchestration using Apache Airflow:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TMDB API      │    │     Kafka       │    │    MongoDB      │
│   (Data Source) │    │  (Streaming)    │    │   (Storage)     │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      │                      │
┌─────────▼──────────────────────▼──────────────────────▼───────┐
│                    Apache Airflow                             │
│                  (Orchestration Layer)                        │
│  ┌───────────────┐ ┌───────────────┐ ┌─────────────────────┐  │
│  │  Data         │ │  Processing   │ │   Quality           │  │
│  │ Ingestion     │ │  & Analytics  │ │  Monitoring         │  │
│  │     DAG       │ │     DAG       │ │     DAG             │  │
│  └───────────────┘ └───────────────┘ └─────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
          │                      │                      │
          │                      │                      │
┌─────────▼───────┐    ┌─────────▼───────┐    ┌─────────▼───────┐
│  Apache Spark   │    │  Apache Superset│    │    Grafana      │
│  (Processing)   │    │ (Visualization) │    │  (Monitoring)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 New Features with Airflow

### Advanced Orchestration
- **Dependency Management**: Complex task dependencies with conditional execution
- **Error Handling**: Sophisticated retry mechanisms and failure notifications
- **Resource Management**: Intelligent task scheduling and resource allocation
- **Monitoring**: Real-time pipeline monitoring with custom dashboards

### Scheduling Capabilities
- **Flexible Scheduling**: Cron-based scheduling with timezone support
- **Backfilling**: Historical data processing capabilities
- **Sensor-based Triggers**: Data availability-driven execution
- **Manual Triggers**: On-demand pipeline execution

### Quality Assurance
- **Data Quality Monitoring**: Automated schema validation and data integrity checks
- **Health Checks**: System component health monitoring
- **Alerting**: Email and Slack notifications for failures and quality issues
- **Reporting**: Comprehensive quality and performance reports

## Components

- **Data Ingestion**: Airbyte ETL platform with TMDB API connectors (or legacy direct API clients)
- **Orchestration**: Apache Airflow for workflow management and scheduling
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage**: MinIO (S3-compatible) with Bronze/Silver/Gold layers
- **Serving**: MongoDB for fast queries
- **Visualization**: Apache Superset and Grafana
- **Deployment**: Docker Compose for local development, Kubernetes for production

## Project Structure

```
movie-data-analysis-pipeline/
├── src/
│   ├── ingestion/          # Data ingestion (Airbyte integration + legacy)
│   ├── streaming/          # Spark streaming jobs
│   ├── storage/           # Storage layer management
│   └── serving/           # NoSQL serving layer
├── dags/                  # Airflow DAG definitions
├── plugins/               # Airflow custom operators and hooks
├── logs/                  # Airflow execution logs
├── airbyte/               # Airbyte ETL configurations
│   ├── sources/           # TMDB API source configs
│   ├── destinations/      # Kafka/MongoDB destination configs
│   ├── connections/       # Connection and sync configs
│   └── transformations/   # dbt transformation models
├── config/                # Configuration files (including Airflow)
├── kubernetes/            # K8s deployment manifests
├── spark/                 # Spark applications and JARs
├── tests/                 # Test suites
├── docs/                  # Documentation (including Airflow setup)
└── docker-compose.yml     # Local development setup with Airflow
```

## Services and Ports

| Service | Port | Description |
|---------|------|-------------|
| **Airflow Webserver** | 8090 | Workflow management UI |
| **Kafka** | 9092 | Message streaming |
| **Schema Registry** | 8081 | Avro schema management |
| **MongoDB** | 27017 | Document database |
| **MinIO** | 9000, 9001 | S3-compatible storage |
| **Spark Master** | 8080, 7077 | Spark cluster management |
| **Apache Superset** | 8088 | Data visualization |
| **Grafana** | 3000 | Monitoring dashboards |
| **Movie API** | 8000 | REST API for movie data |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Kubernetes cluster (for production)

### Local Development with Airflow

1. **Configure environment variables:**
```bash
cp .env.example .env
# Edit .env with your TMDB API key and email settings
```

2. **Initialize Airflow:**
```bash
# Create required directories
mkdir -p dags logs plugins

# Set permissions (Linux/macOS)
sudo chown -R 50000:0 dags logs plugins

# Initialize Airflow database
docker-compose up airflow-init
```

3. **Start the infrastructure:**
```bash
docker-compose up -d
```

4. **Access Airflow UI:**
- URL: `http://localhost:8090`
- Username: `admin`
- Password: `admin`

5. **Setup Airflow connections and variables:**
```bash
docker-compose exec airflow-webserver python /opt/airflow/config/airflow_config.py
```

6. **Enable and trigger DAGs:**
- Navigate to Airflow UI
- Enable the desired DAGs (toggle switches)
- Trigger manually or wait for scheduled execution

### Airflow DAGs

The pipeline includes three main DAGs:

1. **movie_data_ingestion** (Hourly)
   - Extract trending and popular movies from TMDB
   - Stream data to Kafka topics
   - Monitor extraction metrics

2. **movie_data_processing** (Daily)
   - Process raw data with Spark
   - Perform sentiment analysis
   - Generate analytics and store in MongoDB

3. **data_quality_monitoring** (Every 6 hours)
   - Validate data schemas and integrity
   - Monitor system health
   - Generate quality reports and alerts

2. Start Airbyte services:
```bash
docker-compose -f airbyte/docker-compose.airbyte.yml up -d
```

3. Install Python dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys and Airbyte configuration
```

5. Configure Airbyte (see [Airbyte Setup Guide](docs/AIRBYTE_SETUP.md)):
   - Access UI at http://localhost:8000
   - Configure TMDB source and Kafka/MongoDB destinations
   - Create connections and sync schedules

6. Run the ingestion pipeline:
```bash
# Using Airbyte (recommended)
python -m src.ingestion.main --airbyte batch

# Using legacy extractor
python -m src.ingestion.main --legacy batch
```

## Components

### Data Ingestion
- **Airbyte ETL Platform** (Recommended): Robust, scalable ETL with built-in connectors
  - TMDB API source with HTTP connector
  - Error handling and retry mechanisms
  - Incremental sync capabilities
  - Web UI for pipeline management
- **Legacy Direct Integration**: Custom TMDB API clients with Kafka producers
- Support for multiple data types (movies, people, credits, reviews)

### Stream Processing
- Real-time data cleansing and enrichment
- Sentiment analysis using Spark NLP
- Trending calculations with windowing
- Graph analytics for actor networks

### Storage Layers
- **Bronze**: Raw data from APIs
- **Silver**: Cleaned and enriched data
- **Gold**: Aggregated and business-ready data

### Serving Layer
- MongoDB collections for fast queries
- Pre-computed aggregations for dashboards
- RESTful API for data access

## Monitoring

- Grafana dashboards for system metrics
- Kafka lag monitoring
- Spark job performance tracking
- Data quality validation

## Deployment

See `kubernetes/` directory for production deployment manifests.

### Airbyte Integration Benefits

- **Reliability**: Built-in error handling, retries, and monitoring
- **Scalability**: Better resource management and parallel processing  
- **Maintainability**: Reduces custom code maintenance burden
- **Features**: Incremental syncs, data deduplication, schema evolution
- **UI**: Rich web interface for managing data pipelines

For detailed Airbyte setup instructions, see [docs/AIRBYTE_SETUP.md](docs/AIRBYTE_SETUP.md).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.