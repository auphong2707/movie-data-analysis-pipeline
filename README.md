# Movie Data Analytics Pipeline - Lambda Architecture

A production-ready big data analytics pipeline implementing **Lambda Architecture** to analyze TMDB movie data for real-time sentiment tracking, trend prediction, and comprehensive analytics.

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Core Features](#-core-features)
- [Data Pipeline Architecture](#-data-pipeline-architecture)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Implementation Status](#-implementation-status)
- [Documentation](#-documentation)
- [Deployment](#-deployment)
- [Monitoring & Operations](#-monitoring--operations)
- [Contributing](#-contributing)
- [License](#-license)

## 🎯 Project Overview

### Business Problems Solved

1. **Movie Popularity & Trend Prediction**
   - Analyze time-series signals (popularity, vote counts, rating velocity)
   - Detect rising/declining titles in real-time
   - Forecast short-term demand using historical patterns

2. **Genre-Based Sentiment Insights**
   - Real-time sentiment scoring on new reviews
   - Historical sentiment trends by genre, year, and popularity tier
   - Track audience perception shifts across blockbuster vs. niche films

3. **Recommendation System**
   - Content-based filtering using metadata (genres, cast, keywords)
   - Re-rank by current trends and sentiment scores
   - Combine historical accuracy with real-time relevance

### Data Scope

- **Source**: The Movie Database (TMDB) API
- **Volume**: ~50K movies, 100K+ reviews/month
- **Languages**: English-language content
- **Update Frequency**: 
  - Batch Layer: Every 4 hours (historical accuracy)
  - Speed Layer: <5 minute latency (real-time freshness)
- **API Rate Limit**: 4 requests/second (TMDB constraint)

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────┐
                    │         TMDB API                    │
                    │    (4 requests/second limit)        │
                    └────────────┬──────────────┬─────────┘
                                 │              │
                                 │              │
                    ┌────────────▼──────┐  ┌────▼─────────────┐
                    │   BATCH LAYER     │  │   SPEED LAYER    │
                    │ (Historical Data) │  │ (Real-time Data) │
                    │                   │  │                  │
                    │ • HDFS Storage    │  │ • Kafka Streaming│
                    │ • Spark Batch     │  │ • Cassandra      │
                    │ • Airflow         │  │ • Spark Streaming│
                    │                   │  │                  │
                    │ Every 4 hours     │  │ 5-min windows    │
                    │ Complete accuracy │  │ Low latency      │
                    │ (> 48 hours old)  │  │ (≤ 48 hours old) │
                    └────────────┬──────┘  └────┬─────────────┘
                                 │              │
                                 │              │
                                 └──────┬───────┘
                                        │
                              ┌─────────▼──────────┐
                              │  SERVING LAYER     │
                              │                    │
                              │ • MongoDB (merged  │
                              │   batch + speed    │
                              │   views)           │
                              │ • FastAPI REST API │
                              │ • Apache Superset  │
                              │ • Grafana          │
                              │                    │
                              │ Query-time merge   │
                              └────────────────────┘
```

### Lambda Architecture Components

The pipeline implements Nathan Marz's Lambda Architecture pattern with three distinct layers:

**Batch Layer**: Processes complete historical datasets for accuracy (>48 hours old)
- Reprocessing capability for corrections
- Complete data accuracy
- Higher latency acceptable

**Speed Layer**: Processes recent data for low latency (≤48 hours old)  
- Real-time incremental updates
- Approximations acceptable
- Sub-5-minute latency

**Serving Layer**: Merges batch accuracy with speed freshness
- 48-hour cutoff merge strategy
- Unified query interface
- Best of both worlds

## 🛠️ Technology Stack

### Batch Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | Schedule & manage batch jobs (4-hour intervals) |
| **Processing** | Apache Spark (Batch) | Transform data through Bronze → Silver → Gold |
| **Storage** | HDFS (Hadoop 3.x) | Distributed storage for all data layers |
| **Data Quality** | Great Expectations | Validate data at each transformation stage |

### Speed Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Streaming** | Apache Kafka | Message queue for real-time data ingestion |
| **Processing** | Spark Structured Streaming | Process data in 5-minute windows |
| **Storage** | Apache Cassandra | Low-latency writes with 48h TTL auto-expiration |
| **Schema** | Confluent Schema Registry | Avro schema management for Kafka topics |

### Serving Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | MongoDB | Unified storage for batch + speed views |
| **API** | FastAPI | High-performance async REST API endpoints |
| **Caching** | Redis | Response caching for frequently accessed data |
| **BI Dashboards** | Apache Superset | Business intelligence and analytics dashboards |
| **Monitoring** | Grafana | Real-time system monitoring and alerting |

### Cross-Cutting Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Metadata Management** | DataHub | Data catalog, lineage tracking, governance |
| **Orchestration** | Kubernetes | Container orchestration for all services |
| **Development** | Docker Compose | Lightweight local development environment |
| **Monitoring** | Prometheus | Metrics collection and alerting |
| **Version Control** | Git | Source code management |

## 🎨 Core Features

### Real-Time Analytics
- **Sentiment Analysis**: VADER-based sentiment scoring on movie reviews
- **Trending Detection**: Identify hot movies based on velocity and acceleration
- **Live Statistics**: Real-time aggregations of ratings, votes, and popularity

### Historical Analysis
- **Genre Analytics**: Comprehensive statistics by genre, year, and tier
- **Temporal Trends**: Year-over-year and seasonal pattern analysis
- **Actor Networks**: Collaboration graphs using GraphX
- **Revenue Analysis**: Budget vs. revenue performance tracking

### Query Capabilities
- **Fast Queries**: <100ms p95 latency through MongoDB + Redis caching
- **Fresh Data**: 5-minute freshness from speed layer
- **Deep History**: 5-year historical data from batch layer
- **Flexible Search**: Full-text search with multiple filter dimensions

### Data Quality
- **Schema Validation**: Automated validation at each layer
- **Deduplication**: Intelligent duplicate removal by movie_id
- **Completeness Checks**: >95% data quality target
- **Anomaly Detection**: Statistical outlier identification

## 📊 Data Pipeline Architecture

### Batch Layer Flow

```
TMDB API (scheduled extraction)
    ↓ (Airflow DAG - every 4 hours)
┌───────────────────────────────────────┐
│         BRONZE LAYER (HDFS)           │
│  • Raw JSON → Parquet                 │
│  • Partition: /year/month/day/hour    │
│  • Retention: 90 days                 │
│  • No transformations (immutable)     │
└────────────────┬──────────────────────┘
                 ↓ (Spark Batch Job)
┌───────────────────────────────────────┐
│         SILVER LAYER (HDFS)           │
│  • Deduplication by movie_id          │
│  • Schema validation & enrichment     │
│  • Genre/cast joins                   │
│  • Historical sentiment analysis      │
│  • Partition: /year/month/genre       │
│  • Retention: 2 years                 │
└────────────────┬──────────────────────┘
                 ↓ (Spark Aggregations)
┌───────────────────────────────────────┐
│          GOLD LAYER (HDFS)            │
│  • Aggregations by genre/year/tier    │
│  • Trend scores (7d, 30d, 90d)        │
│  • Popularity metrics                 │
│  • Partition: /metric_type/year/month │
│  • Retention: 5 years                 │
└────────────────┬──────────────────────┘
                 ↓ (Export to Serving)
┌───────────────────────────────────────┐
│      MONGODB (Batch Views)            │
│  • Collection: batch_views            │
│  • Updated every 4 hours              │
│  • Indexed for fast queries           │
└───────────────────────────────────────┘
```

### Speed Layer Flow

```
TMDB API (real-time stream)
    ↓ (Kafka Producer - streaming)
┌───────────────────────────────────────┐
│          KAFKA TOPICS                 │
│  • movie.reviews (new reviews)        │
│  • movie.ratings (new ratings)        │
│  • movie.metadata (updates)           │
│  • Replication factor: 3              │
│  • Retention: 7 days                  │
└────────────────┬──────────────────────┘
                 ↓ (Spark Structured Streaming)
┌───────────────────────────────────────┐
│      REAL-TIME PROCESSING             │
│  • 5-minute tumbling windows          │
│  • Real-time sentiment (VADER)        │
│  • Incremental aggregations           │
│  • Hot movie detection (velocity)     │
└────────────────┬──────────────────────┘
                 ↓ (Write to Cassandra)
┌───────────────────────────────────────┐
│      CASSANDRA (Speed Views)          │
│  • Table: speed_views                 │
│  • TTL: 48 hours (auto-expire)        │
│  • Partition: (movie_id, hour)        │
│  • Replication factor: 3              │
└────────────────┬──────────────────────┘
                 ↓ (Periodic sync - 5 min)
┌───────────────────────────────────────┐
│      MONGODB (Speed Views)            │
│  • Collection: speed_views            │
│  • Synced every 5 minutes             │
│  • TTL index: 48 hours                │
└───────────────────────────────────────┘
```

### Serving Layer Flow

```
┌──────────────┐         ┌──────────────┐
│   MongoDB    │         │   MongoDB    │
│ batch_views  │         │ speed_views  │
│ (historical) │         │ (last 48h)   │
│ (>48h old)   │         │ (≤48h old)   │
└──────┬───────┘         └──────┬───────┘
       │                        │
       └────────┬───────────────┘
                ↓
        ┌───────────────┐
        │ Query Router  │  • 48-hour cutoff logic
        │ & Merger      │  • Merge batch + speed
        └───────┬───────┘  • Deduplicate results
                │
                ↓
        ┌───────────────┐
        │  Redis Cache  │  • 5-15 minute TTL
        │               │  • Frequently accessed data
        └───────┬───────┘
                │
                ↓
        ┌───────────────┐
        │   FastAPI     │  • REST API endpoints
        │               │  • <100ms p95 latency
        └───────┬───────┘  • Authentication & rate limiting
                │
                ↓
    ┌───────────────────────────┐
    │                           │
┌───▼─────┐            ┌────────▼──────┐
│ Superset│            │    Grafana    │
│Dashboards│            │  Monitoring   │
└─────────┘            └───────────────┘
```

## 📁 Project Structure

```
movie-data-analysis-pipeline/
├── README.md                          # This file
├── LICENSE                            # MIT License
├── requirements.txt                   # Python dependencies
├── docker-compose.yml                 # Local development setup
│
├── config/                           # Configuration files
│   ├── __init__.py
│   ├── config.py                     # Application configuration
│   ├── airflow_config.py             # Airflow DAG configs
│   ├── kafka_config.py               # Kafka settings
│   ├── kafka_setup.py                # Kafka topic initialization
│   ├── iceberg_config.py             # Apache Iceberg configs
│   └── schemas.py                    # Data schemas (Avro, Parquet)
│
├── layers/                           # Lambda Architecture layers
│   ├── batch_layer/                  # Historical processing
│   │   ├── README.md                 # Detailed batch layer docs
│   │   ├── airflow_dags/            # Orchestration workflows
│   │   ├── spark_jobs/              # Bronze → Silver → Gold
│   │   ├── master_dataset/          # TMDB ingestion
│   │   │   └── ingestion.py         # Raw data extraction
│   │   ├── batch_views/             # Pre-computed views
│   │   ├── config/                  # Spark/HDFS configs
│   │   └── tests/                   # Unit tests
│   │
│   ├── speed_layer/                 # Real-time processing
│   │   ├── README.md                # Detailed speed layer docs
│   │   ├── kafka_producers/         # Data streaming
│   │   │   └── tmdb_stream_producer.py
│   │   ├── streaming_jobs/          # Spark Structured Streaming
│   │   ├── cassandra_views/         # Speed view schemas
│   │   ├── connectors/              # Cassandra → MongoDB sync
│   │   ├── config/                  # Kafka/Cassandra configs
│   │   └── tests/                   # Unit tests
│   │
│   └── serving_layer/               # Query interface
│       ├── README.md                # Detailed serving layer docs
│       ├── api/                     # FastAPI REST endpoints
│       │   └── main.py              # API entry point
│       ├── query_engine/            # View merger logic
│       ├── mongodb/                 # Database layer
│       ├── visualization/           # Superset & Grafana
│       ├── config/                  # API/MongoDB configs
│       └── tests/                   # API & integration tests
│
├── kubernetes/                       # Production deployment
│   ├── README.md                    # Kubernetes deployment guide
│   ├── namespace.yaml               # Namespace definition
│   ├── configmap.yaml              # Configuration & secrets
│   ├── kafka.yaml                  # Kafka cluster
│   ├── minio.yaml                  # Object storage (HDFS alternative)
│   ├── mongodb.yaml                # MongoDB replica set
│   ├── spark.yaml                  # Spark cluster
│   ├── applications.yaml           # Application deployments
│   ├── monitoring.yaml             # Prometheus & Grafana
│   ├── visualization.yaml          # Apache Superset
│   └── deploy.sh                   # Automated deployment script
│
├── docs/                            # Additional documentation
│   └── Movie Data Analysis Pipeline.drawio  # Architecture diagrams
│
└── tests/                           # Integration tests
    └── (test files)
```

## 🚀 Quick Start

> **✨ NEW: Unified Setup Available!**  
> The batch and speed layers are now combined into a single setup at the project root.  
> See [QUICKSTART.md](QUICKSTART.md) for the fastest way to get started, or [SETUP.md](SETUP.md) for detailed instructions.

### Prerequisites

- **Docker Desktop** or **Docker Engine** (version 20.10+)
- **Docker Compose** (version 1.29+)
- **At least 8GB RAM** allocated to Docker
- **TMDB API Key** (free from [themoviedb.org](https://www.themoviedb.org/settings/api))

### Unified Setup (Recommended)

The unified setup runs both Batch Layer and Speed Layer with a single command:

1. **Clone the Repository**
   ```bash
   git clone https://github.com/auphong2707/movie-data-analysis-pipeline.git
   cd movie-data-analysis-pipeline
   ```

2. **Configure Environment Variables**
   ```bash
   # Copy template and add your TMDB API key
   cp .env.example .env
   nano .env  # Set TMDB_API_KEY=your_key_here
   ```

3. **Start All Services**
   ```bash
   # Start complete infrastructure (Batch + Speed layers)
   docker-compose up -d
   
   # Verify all services are running
   docker-compose ps
   ```

4. **Access Web Interfaces**
   - **Airflow (Batch Layer)**: http://localhost:8088 (admin/admin)
   - **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
   - **Schema Registry**: http://localhost:8081

For detailed instructions and troubleshooting, see:
- **Quick Reference**: [QUICKSTART.md](QUICKSTART.md)
- **Complete Setup Guide**: [SETUP.md](SETUP.md)

### Running the Pipeline

**Batch Layer** (historical data processing):
```bash
# Trigger Airflow DAG manually or wait for scheduled run
# Access Airflow UI at http://localhost:8088
```

**Speed Layer** (real-time streaming):
```bash
# Automatically starts with docker-compose
# View logs: docker-compose logs -f tmdb-producer sentiment-stream
```

**Query Results**:
```bash
# Connect to MongoDB
docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin

# View merged data
use moviedb
db.batch_views.find().limit(5)  # Historical (>48h)
db.speed_views.find().limit(5)  # Recent (≤48h)
```

For complete instructions, see [SETUP.md](SETUP.md).

## ✅ Implementation Status

### Phase 1: Setup & Planning - ✅ COMPLETED
- [x] Lambda Architecture design
- [x] Directory structure (`layers/batch_layer`, `layers/speed_layer`, `layers/serving_layer`)
- [x] Documentation (12+ markdown files)
- [x] Template code for all layers

### Phase 2: Batch Layer - 🔲 TODO
- [ ] Deploy HDFS cluster (3 datanodes + namenode)
- [ ] Implement TMDB → HDFS ingestion
- [ ] Create Airflow DAGs (batch orchestration)
- [ ] Bronze → Silver transformations (deduplication, validation)
- [ ] Silver → Gold aggregations (genre, trends, ratings)
- [ ] Sentiment analysis (batch processing)
- [ ] Export batch views to MongoDB

### Phase 3: Speed Layer - 🔲 TODO
- [ ] Deploy Kafka cluster (3 brokers + Zookeeper)
- [ ] Deploy Schema Registry (Avro schemas)
- [ ] Implement Kafka producers (real-time)
- [ ] Deploy Cassandra cluster (3 nodes, 48h TTL)
- [ ] Spark Structured Streaming jobs
- [ ] Real-time sentiment analysis
- [ ] Write to Cassandra speed views

### Phase 4: Serving Layer - 🔲 TODO
- [ ] Deploy MongoDB (materialized views)
- [ ] Implement FastAPI REST API
- [ ] View merger (batch + speed merge logic)
- [ ] Redis caching layer
- [ ] Apache Superset dashboards
- [ ] Grafana monitoring
- [ ] API authentication & rate limiting

### Phase 5: Integration & Testing - 🔲 TODO
- [ ] End-to-end integration
- [ ] 48-hour merge strategy implementation
- [ ] Performance benchmarking
- [ ] Unit & integration tests
- [ ] Data quality validation

### Phase 6: Production Deployment - 🔲 TODO
- [ ] Kubernetes manifests (all services)
- [ ] Persistent volumes (HDFS storage)
- [ ] Monitoring & alerting setup
- [ ] Security hardening
- [ ] Deployment automation

## 📚 Documentation

### Architecture Documentation
- **[Batch Layer Guide](layers/batch_layer/README.md)**: Complete guide to HDFS storage, Spark batch jobs, Airflow DAGs, and Bronze → Silver → Gold transformations
- **[Speed Layer Guide](layers/speed_layer/README.md)**: Kafka streaming, Spark Structured Streaming, Cassandra setup, and real-time processing
- **[Serving Layer Guide](layers/serving_layer/README.md)**: FastAPI endpoints, MongoDB schema, query merger logic, and caching strategies
- **[Kubernetes Deployment](kubernetes/README.md)**: Production deployment guide with monitoring, scaling, and troubleshooting

### Presentation Materials
- **[First Presentation](First%20Presentation%2028accfcd991180e7889cd9dc5e83ca02.md)**: Project overview, business problems, and architecture explanation

### Technical Specifications
- **Configuration Files**: See `config/` directory for all service configurations
- **API Documentation**: Interactive docs at `/docs` endpoint when API is running
- **Architecture Diagrams**: See `docs/Movie Data Analysis Pipeline.drawio`

## 🚢 Deployment (Dummy, didn't work yet)

### Docker Compose (Development)

Best for local development and testing:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Clean up (including volumes)
docker-compose down -v
```

### Kubernetes (Production) (Dummy, didn't work yet)

Production-ready deployment with high availability:

```bash
# Navigate to kubernetes directory
cd kubernetes

# Deploy complete stack
./deploy.sh deploy

# Check deployment status
kubectl get pods -n movie-analytics

# Access services via port forwarding
kubectl port-forward -n movie-analytics service/movie-api-service 8000:8000
kubectl port-forward -n movie-analytics service/grafana-service 3000:3000
kubectl port-forward -n movie-analytics service/superset-service 8088:8088

# Clean up
./deploy.sh clean
```

See [kubernetes/README.md](kubernetes/README.md) for detailed deployment instructions.

## 📊 Monitoring & Operations

### Key Performance Indicators

| Metric | Target | Description |
|--------|--------|-------------|
| **Batch Job Success Rate** | >99% | Percentage of successful Airflow DAG runs |
| **Batch Processing Time** | <2 hours | Time to complete Bronze → Silver → Gold |
| **Speed Layer Latency** | <5 minutes | End-to-end processing time for streaming |
| **API Response Time (p95)** | <100ms | 95th percentile API latency |
| **Data Quality Score** | >95% | Percentage of rows passing validation |
| **Kafka Consumer Lag** | <1000 msgs | Number of unprocessed Kafka messages |
| **Cache Hit Rate** | >70% | Percentage of requests served from cache |

### Monitoring Dashboards

**Grafana Dashboards** (http://localhost:3000):
1. **System Health**: API latency, MongoDB performance, Redis cache hit rates
2. **Data Freshness**: Batch layer updates, speed layer lag, view staleness
3. **Infrastructure**: Kafka throughput, Cassandra write rates, Spark job duration

**Apache Superset Dashboards** (http://localhost:8088):
1. **Executive Overview**: Total movies, average ratings, revenue trends
2. **Real-time Analytics**: Trending movies, recent sentiment changes
3. **Historical Analysis**: Year-over-year comparisons, genre performance

### Alerting Rules

- **Critical Alerts** (PagerDuty):
  - Batch job failures
  - Streaming job crashes
  - MongoDB/Cassandra node down
  - API p99 latency >500ms

- **Warning Alerts** (Slack):
  - Kafka consumer lag >5000 messages
  - Data quality score <90%
  - Cache hit rate <50%
  - Speed layer lag >10 minutes

### Log Aggregation (Dummy, didn't work yet)

All logs are centralized and searchable:

```bash
# Docker Compose logs
docker-compose logs -f [service_name]

# Kubernetes logs
kubectl logs -n movie-analytics -l app=[app_name] -f

# View specific service logs
kubectl logs -n movie-analytics deployment/movie-api --tail=100
```

## 🧪 Testing (Dummy, didn't work yet)

### Run Tests

```bash
# Run all tests
pytest tests/

# Run specific layer tests
pytest layers/batch_layer/tests/
pytest layers/speed_layer/tests/
pytest layers/serving_layer/tests/

# Run with coverage
pytest --cov=layers --cov-report=html

# Run integration tests only
pytest -m integration
```

### Test Categories

- **Unit Tests**: Individual component functionality
- **Integration Tests**: End-to-end pipeline flows
- **Performance Tests**: Latency and throughput benchmarks
- **Data Quality Tests**: Schema validation and completeness
