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
| `tmdb-producer` | Streams TMDB API data to Kafka |
| `event-producer` | Generates synthetic user events |
| `sentiment-stream` | Real-time sentiment analysis (Spark) |
| `aggregation-stream` | Movie metrics aggregation (Spark) |
| `trending-stream` | Trending movie detection (Spark) |

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- TMDB API key from https://www.themoviedb.org/settings/api
- 8GB+ RAM available for Docker

### Step 1: Configure Environment
```bash
# Copy example environment file
cp layers/speed_layer/.env.example layers/speed_layer/.env

# Edit and add your TMDB API key
notepad layers/speed_layer/.env  # Windows
nano layers/speed_layer/.env     # Linux/Mac
```

### Step 2: Validate Setup (Optional)
```bash
bash validate-speed-layer.sh
```

### Step 3: Start Speed Layer
```bash
# Start all services (default command)
bash start-speed-layer-docker.sh start

# Or simply
bash start-speed-layer-docker.sh
```

## 📖 Management Commands

The unified script supports multiple operations:

```bash
# Start all services
bash start-speed-layer-docker.sh start

# Stop all services (containers remain)
bash start-speed-layer-docker.sh stop

# Restart all services
bash start-speed-layer-docker.sh restart

# Check service status
bash start-speed-layer-docker.sh status

# Follow logs from all services
bash start-speed-layer-docker.sh logs

# Stop and remove containers
bash start-speed-layer-docker.sh cleanup

# Complete cleanup including data volumes
bash start-speed-layer-docker.sh purge
```

## 🔍 Monitoring & Debugging

### View Logs
```bash
# All services
docker-compose -f docker-compose.speed.yml logs -f

# Specific service
docker-compose -f docker-compose.speed.yml logs -f tmdb-producer
docker-compose -f docker-compose.speed.yml logs -f sentiment-stream
docker-compose -f docker-compose.speed.yml logs -f kafka

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
```bash
# Enter cqlsh
docker exec -it cassandra cqlsh

# Query from host
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.movie_aggregations;"
docker exec -it cassandra cqlsh -e "SELECT * FROM speed_layer.review_sentiments LIMIT 10;"
```

### Check Schema Registry
```bash
# List subjects
curl http://localhost:8081/subjects

# Get schema for a subject
curl http://localhost:8081/subjects/movie.reviews-value/versions/latest
```

### Monitor Container Resources
```bash
# Resource usage
docker stats

# Specific containers
docker stats kafka cassandra mongodb
```

## 🏗️ Architecture

### Data Flow
```
TMDB API → TMDB Producer → Kafka Topics → Streaming Jobs → Cassandra
                              ↓
                      Schema Registry
                              
User Events → Event Producer → Kafka Topics → Streaming Jobs → Cassandra
```

### Kafka Topics
- `movie.reviews` - Movie reviews from TMDB
- `movie.ratings` - User ratings
- `movie.metadata` - Movie information
- `movie.trending` - Trending detection results
- `user.events` - Synthetic user events

### Cassandra Tables (48h TTL)
- `review_sentiments` - Real-time sentiment scores
- `movie_aggregations` - Aggregated metrics
- `trending_movies` - Detected trends
- `real_time_stats` - Live statistics

## 🔧 Configuration

### Environment Variables
Edit `layers/speed_layer/.env`:

```bash
# Required
TMDB_API_KEY=your_actual_api_key_here

# Kafka (Docker service names)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Cassandra
CASSANDRA_HOSTS=cassandra
CASSANDRA_KEYSPACE=speed_layer

# MongoDB
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_USERNAME=admin
MONGO_PASSWORD=password
MONGO_DATABASE=moviedb
```

### Spark Configuration
Located in `config/spark_streaming_config.yaml`:
- Checkpoint locations
- Window durations
- Batch sizes
- Cassandra connection settings

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
docker-compose -f docker-compose.speed.yml logs tmdb-producer

# Verify .env file
cat layers/speed_layer/.env | grep TMDB_API_KEY
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
```bash
# Check logs for specific container
docker logs <container-name> --tail=50

# Check exit code
docker inspect <container-name> --format='{{.State.ExitCode}}'
```

## 📝 Development Workflow

### Making Code Changes
```bash
# 1. Edit code in layers/speed_layer/
# 2. Rebuild affected containers
bash start-speed-layer-docker.sh restart

# Or rebuild specific service
docker-compose -f docker-compose.speed.yml up -d --build tmdb-producer
```

### Testing Changes
```bash
# Run tests (if available)
docker-compose -f docker-compose.speed.yml exec tmdb-producer python -m pytest

# Check data flow
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM speed_layer.review_sentiments;"
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
1. Check logs: `bash start-speed-layer-docker.sh logs`
2. Verify status: `bash start-speed-layer-docker.sh status`
3. Review configuration in `layers/speed_layer/.env`
4. Check Docker resources (RAM, disk space)
5. Consult troubleshooting section above