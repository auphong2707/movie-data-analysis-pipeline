# Movie Data Analytics Pipeline - Lambda Architecture

A big data analytics pipeline implementing **Lambda Architecture** with Apache Hadoop, Spark, Kafka, and Cassandra.

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

## 🛠️ Technology Stack

| Layer | Technology |
|-------|-----------|
| **Batch** | HDFS (Hadoop 3.x), Apache Spark, Apache Airflow |
| **Speed** | Apache Kafka, Apache Cassandra, Spark Streaming |
| **Serving** | MongoDB, FastAPI, Apache Superset, Grafana |
| **Deployment** | Kubernetes, Docker Compose |

## ✅ Implementation To-Do

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

- **Architecture**: `LAMBDA_ARCHITECTURE_V2.md`
- **Roadmap**: `NEXT_PHASE_ROADMAP.md` (14-week plan)
- **Batch Layer**: `layers/batch_layer/README.md`
- **Speed Layer**: `layers/speed_layer/README.md`
- **Serving Layer**: `layers/serving_layer/README.md`
- **Index**: `DOCUMENTATION_INDEX.md`

## 🎯 Current Status

**Phase 1 Complete** ✅  
Architecture design and documentation ready. Ready to implement Phase 2 (Batch Layer).

---

**License**: MIT | **Support**: [GitHub Issues](https://github.com/auphong2707/movie-data-analysis-pipeline/issues)
