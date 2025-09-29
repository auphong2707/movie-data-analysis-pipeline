# Movie Data Analytics Pipeline

A comprehensive big data analytics pipeline for movie data using Kappa Architecture.

## Architecture Overview

This project implements a real-time movie data analytics pipeline with the following components:

- **Data Ingestion**: Airbyte ETL platform with TMDB API connectors (or legacy direct API clients)
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage**: MinIO (S3-compatible) with Bronze/Silver/Gold layers
- **Serving**: MongoDB for fast queries
- **Visualization**: Apache Superset and Grafana
- **Orchestration**: Kubernetes deployment

## Project Structure

```
movie-data-analysis-pipeline/
├── src/
│   ├── ingestion/          # Data ingestion (Airbyte integration + legacy)
│   ├── streaming/          # Spark streaming jobs
│   ├── storage/           # Storage layer management
│   └── serving/           # NoSQL serving layer
├── airbyte/               # Airbyte ETL configurations
│   ├── sources/           # TMDB API source configs
│   ├── destinations/      # Kafka/MongoDB destination configs
│   ├── connections/       # Connection and sync configs
│   └── transformations/   # dbt transformation models
├── config/                # Configuration files
├── kubernetes/            # K8s deployment manifests
├── spark/                 # Spark applications and JARs
├── tests/                 # Test suites
├── docs/                  # Documentation
└── docker-compose.yml     # Local development setup
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Kubernetes cluster (for production)

### Local Development

1. Start the infrastructure:
```bash
docker-compose up -d
```

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