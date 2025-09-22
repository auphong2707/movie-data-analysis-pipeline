# Movie Data Analytics Pipeline

A comprehensive big data analytics pipeline for movie data using Kappa Architecture.

## Architecture Overview

This project implements a real-time movie data analytics pipeline with the following components:

- **Data Ingestion**: TMDB/IMDb API clients with Kafka producers
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage**: MinIO (S3-compatible) with Bronze/Silver/Gold layers
- **Serving**: MongoDB for fast queries
- **Visualization**: Apache Superset and Grafana
- **Orchestration**: Kubernetes deployment

## Project Structure

```
movie-data-analysis-pipeline/
├── src/
│   ├── ingestion/          # Data ingestion from APIs
│   ├── streaming/          # Spark streaming jobs
│   ├── storage/           # Storage layer management
│   └── serving/           # NoSQL serving layer
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

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys
```

4. Run the ingestion pipeline:
```bash
python src/ingestion/main.py
```

## Components

### Data Ingestion
- TMDB API integration for movie metadata
- Kafka producers for real-time streaming
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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.