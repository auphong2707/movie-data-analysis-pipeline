# Speed Layer Setup Guide - Movie Data Analytics Pipeline

## 🐳 Fully Containerized Speed Layer

The Speed Layer runs completely in Docker containers using `docker-compose.speed.yml`. No host dependencies required!

### 📋 Services Architecture

**Infrastructure Services:**
| Service | Image | Port | Purpose |
|---------|--------|------|---------|
| `zookeeper` | confluentinc/cp-zookeeper:7.4.0 | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.4.0 | 9092 | Event streaming backbone |
| `schema-registry` | confluentinc/cp-schema-registry:7.4.0 | 8081 | Avro schema management |
| `cassandra` | cassandra:4.1 | 9042 | Speed Layer storage (48h TTL) |
| `mongodb` | mongo:6.0 | 27017 | Batch layer storage |

**Speed Layer Application Services:**
| Service | Purpose |
|---------|---------|
| `tmdb-producer` | Streams TMDB API data to Kafka (reviews, ratings, metadata) |
| `sentiment-stream` | Real-time sentiment analysis using VADER (Spark) |
| `aggregation-stream` | Movie metrics aggregation (Spark) |
| `trending-stream` | Trending movie detection and ranking (Spark) |
| `cassandra-mongo-sync` | Syncs speed layer data to MongoDB (300s interval) |

> **Note**: This pipeline uses **TMDB API data only**. No synthetic or user-generated events are included.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- TMDB API key from https://www.themoviedb.org/settings/api
- 8GB+ RAM available for Docker

### Step 1: Configure Environment
```bash
# Navigate to speed layer directory
cd layers/speed_layer

# Copy example environment file
cp .env.example .env

# Edit and add your TMDB API key
notepad .env  # Windows
nano .env     # Linux/Mac
```

### Step 2: Start Speed Layer
```powershell
# From the speed_layer directory
docker-compose -f docker-compose.speed.yml up -d

# Or if already in the directory with the file
docker-compose up -d
```

The system will start all services in the correct order with health checks.

### Service Startup Order
Docker Compose automatically manages dependencies and health checks:

1. **Infrastructure Layer** (parallel start):
   - `zookeeper` - Kafka coordination
   - `cassandra` - Speed layer storage
   - `mongodb` - Serving layer storage

2. **Messaging Layer** (waits for Zookeeper):
   - `kafka` - Event streaming
   - `schema-registry` - Avro schema management

3. **Schema Initialization** (waits for Cassandra):
   - `cassandra-init` - Creates keyspace and tables (one-time job)

4. **Data Producers** (waits for Kafka + Schema Registry):
   - `tmdb-producer` - Starts streaming TMDB data

5. **Streaming Jobs** (waits for Kafka + Cassandra + TMDB producer):
   - `sentiment-stream` - Sentiment analysis
   - `aggregation-stream` - Movie metrics
   - `trending-stream` - Trending detection

6. **Sync Service** (waits for all streaming jobs healthy):
   - `cassandra-mongo-sync` - Syncs to MongoDB every 5 minutes

All services include health checks to ensure proper initialization before dependent services start.

## 📖 Management Commands

Use standard docker-compose commands:

```powershell
# Start all services
docker-compose -f docker-compose.speed.yml up -d

# Stop all services (containers remain)
docker-compose -f docker-compose.speed.yml stop

# Restart all services
docker-compose -f docker-compose.speed.yml restart

# Check service status
docker-compose -f docker-compose.speed.yml ps

# Follow logs from all services
docker-compose -f docker-compose.speed.yml logs -f

# Stop and remove containers
docker-compose -f docker-compose.speed.yml down

# Complete cleanup including data volumes
docker-compose -f docker-compose.speed.yml down -v
```

## 🔍 Monitoring & Debugging

### View Logs
```powershell
# All services
docker-compose -f docker-compose.speed.yml logs -f

# Specific service
docker-compose -f docker-compose.speed.yml logs -f tmdb-producer
docker-compose -f docker-compose.speed.yml logs -f sentiment-stream
docker-compose -f docker-compose.speed.yml logs -f cassandra-mongo-sync

# Last 100 lines
docker-compose -f docker-compose.speed.yml logs --tail=100
```

### Check Kafka Topics
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic
docker exec -it kafka kafka-topics --describe --topic movie.reviews --bootstrap-server localhost:9092

# Consume messages
docker exec -it kafka kafka-console-consumer --topic movie.reviews --from-beginning --bootstrap-server localhost:9092 --max-messages 10
```

### Query Cassandra
```powershell
# Enter cqlsh
docker exec -it cassandra cqlsh

# Query from host
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.movie_stats;"
docker exec -it cassandra cqlsh -e "SELECT * FROM speed_layer.review_sentiments LIMIT 10;"
docker exec -it cassandra cqlsh -e "SELECT * FROM speed_layer.trending_movies LIMIT 10;"
```

### Check Schema Registry
```powershell
# List subjects
curl http://localhost:8081/subjects

# Get schema for reviews
curl http://localhost:8081/subjects/movie.reviews-value/versions/latest

# Get schema for ratings
curl http://localhost:8081/subjects/movie.ratings-value/versions/latest

# Get schema for metadata
curl http://localhost:8081/subjects/movie.metadata-value/versions/latest
```

### Monitor Container Resources
```powershell
# Resource usage
docker stats

# Specific containers
docker stats kafka cassandra mongodb spark-master
```

### Validate TMDB-Only Architecture
```powershell
# Run validation script to ensure no synthetic data
python validate_tmdb_only.py
```

## 🏗️ Architecture

### Data Flow
```
TMDB API → TMDB Producer → Kafka Topics (Avro) → Spark Streaming Jobs → Cassandra
                              ↓                                            ↓
                      Schema Registry                        Cassandra-MongoDB Sync
                                                                          ↓
                                                                      MongoDB
```

### Kafka Topics
- `movie.reviews` - Movie reviews from TMDB API
- `movie.ratings` - Rating updates extracted from TMDB data
- `movie.metadata` - Movie metadata and updates from TMDB

> **Data Format**: All messages use Confluent Avro serialization with Wire Format (5-byte header)

### Cassandra Tables (48h TTL)
- `review_sentiments` - Real-time sentiment scores from movie reviews
- `movie_stats` - Aggregated movie statistics (vote avg, popularity, velocity)
- `trending_movies` - Hourly ranked trending movies by score

## 🔧 Configuration

### Environment Variables
Edit `.env` in the speed_layer directory:

```bash
# Required
TMDB_API_KEY=your_actual_api_key_here

# Kafka (Docker service names)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Cassandra
CASSANDRA_HOSTS=cassandra
CASSANDRA_KEYSPACE=speed_layer
CASSANDRA_PORT=9042

# MongoDB (for serving layer sync)
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_USERNAME=admin
MONGO_PASSWORD=password
MONGO_DATABASE=moviedb

# Sync Configuration
SYNC_INTERVAL=300  # MongoDB sync interval in seconds (default: 5 minutes)
```

### Spark Configuration
Located in `config/spark_streaming_config.yaml`:
- Checkpoint locations
- Window durations
- Batch sizes
- Cassandra connection settings

### MongoDB Sync Configuration
The `cassandra-mongo-sync` service automatically syncs speed layer data to MongoDB:
- **Sync Interval**: 300 seconds (5 minutes) by default
- **Collections Synced**:
  - `movie_stats` - Aggregated movie statistics
  - `trending_movies` - Hourly trending movie rankings
  - `review_sentiments` - Sentiment analysis results
- **Sync Strategy**: Bulk upsert operations to prevent duplicates
- **Health Checks**: Service only starts after all streaming jobs are healthy

To trigger manual sync:
```powershell
docker exec -it cassandra-mongo-sync python /app/connectors/cassandra_to_mongo.py
```

## 🐛 Troubleshooting

### Services Won't Start
```bash
# Check Docker is running
docker info

# Check available resources (need 8GB+ RAM)
docker system df

# Clean up if needed
docker system prune -a
```

### TMDB Producer Failing
```bash
# Check API key is set
docker-compose logs tmdb-producer

# Verify .env file
cat .env | grep TMDB_API_KEY
```

### Kafka Connection Issues
```bash
# Test Kafka connectivity
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check if topics exist
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Cassandra Not Ready
```bash
# Check Cassandra status
docker exec -it cassandra nodetool status

# Verify schema
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE speed_layer;"
```

### Container Keeps Restarting
```powershell
# Check logs for specific container
docker logs <container-name> --tail=50

# Check exit code
docker inspect <container-name> --format='{{.State.ExitCode}}'

# Common issues:
# - tmdb-producer: Check TMDB_API_KEY in .env
# - streaming jobs: Check Kafka and Cassandra are healthy
# - cassandra-mongo-sync: Verify all streaming jobs started successfully
```

### No Data in MongoDB
```powershell
# Check sync service is running
docker-compose -f docker-compose.speed.yml ps cassandra-mongo-sync

# Check sync logs
docker-compose -f docker-compose.speed.yml logs cassandra-mongo-sync

# Verify Cassandra has data first
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.movie_stats;"

# Manually trigger sync
docker exec -it cassandra-mongo-sync python /app/connectors/cassandra_to_mongo.py
```

## 📝 Development Workflow

### Making Code Changes
```powershell
# 1. Edit code in the speed_layer directory
# 2. Rebuild affected containers
docker-compose -f docker-compose.speed.yml up -d --build

# Or rebuild specific service
docker-compose -f docker-compose.speed.yml up -d --build tmdb-producer
docker-compose -f docker-compose.speed.yml up -d --build sentiment-stream
```

### Testing Changes
```powershell
# Check data flow in Cassandra
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.review_sentiments;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.movie_stats;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.trending_movies;"

# Check MongoDB sync
docker exec -it mongodb mongosh moviedb --eval "db.movie_stats.countDocuments()"
docker exec -it mongodb mongosh moviedb --eval "db.trending_movies.countDocuments()"

# Run validation
python validate_tmdb_only.py
```

## 🚀 Production Deployment

### Kubernetes
The project includes K8s manifests in `/kubernetes/`:
```bash
cd kubernetes
kubectl apply -f namespace.yaml
kubectl apply -f kafka.yaml
kubectl apply -f spark.yaml
# ... etc
```

### Docker Swarm
```bash
cd layers/speed_layer
docker stack deploy -c docker-compose.speed.yml speed-layer
```

## 📊 Performance Tuning

### Kafka Optimization
Edit `docker-compose.speed.yml`:
```yaml
environment:
  KAFKA_LOG_RETENTION_HOURS: 168
  KAFKA_LOG_SEGMENT_BYTES: 1073741824
  KAFKA_MESSAGE_MAX_BYTES: 1000000
```

### Cassandra Tuning
```yaml
environment:
  MAX_HEAP_SIZE: 512M  # Increase for production
  HEAP_NEWSIZE: 100M
```

### Spark Resources
Edit streaming job commands:
```yaml
command: >
  spark-submit
  --executor-memory 2g
  --executor-cores 2
  --driver-memory 1g
  streaming_jobs/review_sentiment_stream.py
```

## 📚 Additional Resources

- TMDB API Documentation: https://developers.themoviedb.org/
- Kafka Documentation: https://kafka.apache.org/documentation/
- Cassandra Documentation: https://cassandra.apache.org/doc/
- PySpark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

## 🆘 Getting Help

If you encounter issues:
1. Check logs: `docker-compose -f docker-compose.speed.yml logs -f`
2. Verify status: `docker-compose -f docker-compose.speed.yml ps`
3. Review configuration in `.env`
4. Check Docker resources (RAM, disk space)
5. Run validation: `python validate_tmdb_only.py`
6. Consult troubleshooting section above

## ✅ Validation

The `validate_tmdb_only.py` script verifies:
- ✅ No synthetic data generators in codebase
- ✅ TMDB-only schemas registered
- ✅ Docker compose has only TMDB producer (no event-producer)
- ✅ Streaming jobs consume only TMDB topics

Run after setup to confirm TMDB-only architecture.