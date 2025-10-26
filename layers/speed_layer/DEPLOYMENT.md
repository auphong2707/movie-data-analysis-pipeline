# Speed Layer Deployment Guide

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install all required Python packages
pip install confluent-kafka>=2.11.1 kafka-python>=2.2.15 avro-python3>=1.10.2 pyspark>=4.0.1 cassandra-driver>=3.29.2 vaderSentiment>=3.3.2

# Or install from requirements.txt
pip install -r ../../requirements.txt
```

### 2. Run Pre-Flight Check

```bash
# Check if system is ready to run
python speed_layer_checker.py

# For detailed output
python speed_layer_checker.py --verbose
```

### 3. Start External Services

```bash
# Start Kafka and Zookeeper
docker-compose up -d kafka zookeeper schema-registry

# Start Cassandra
docker-compose up -d cassandra

# Start MongoDB (for sync layer)
docker-compose up -d mongodb
```

### 4. Initialize Cassandra Schema

```bash
# Connect to Cassandra and run schema
cqlsh -f cassandra_views/schema.cql
```

### 5. Start Speed Layer Components

```bash
# Terminal 1: Start TMDB stream producer
python kafka_producers/tmdb_stream_producer.py

# Terminal 2: Start sentiment analysis stream
python streaming_jobs/review_sentiment_stream.py

# Terminal 3: Start movie aggregation stream  
python streaming_jobs/movie_aggregation_stream.py

# Terminal 4: Start trending detection stream
python streaming_jobs/trending_detection_stream.py
```

## 🏗️ Architecture Overview

```
TMDB API → Kafka Topics → Spark Streaming → Cassandra → MongoDB
    ↓           ↓              ↓             ↓          ↓
Producer   Schema Reg.   Real-time      Speed Views  Batch Sync
```

## 📊 Components Status

| Component | Status | Description |
|-----------|--------|-------------|
| ✅ Kafka Producers | Ready | TMDB API streaming with rate limiting |
| ✅ Spark Streaming | Ready | 3 streaming jobs with 5-min windows |
| ✅ Cassandra Views | Ready | TTL tables with 48h expiration |
| ✅ Configuration | Ready | YAML configs for all services |
| ✅ Runtime Checker | Ready | Comprehensive validation tool |
| ⏳ MongoDB Sync | Pending | Periodic sync to MongoDB |
| ⏳ Testing Suite | Pending | Unit tests for all components |

## 🔧 Configuration Files

### Spark Streaming Config (`config/spark_streaming_config.yaml`)
- Kafka connection settings
- Spark executor configuration  
- Windowing parameters
- Cassandra connection settings

### Cassandra Config (`config/cassandra_config.yaml`)
- Connection parameters
- TTL settings (48 hours)
- Performance tuning
- Table schema configuration

## 📈 Data Flow

### 1. TMDB Data Ingestion
- **Rate Limited**: 4 requests/second to TMDB API
- **Topics**: reviews, ratings, metadata, trending
- **Format**: Avro serialization via Schema Registry

### 2. Real-Time Processing
- **Review Sentiment**: VADER sentiment analysis on reviews
- **Movie Aggregation**: Popularity velocity and rating distributions
- **Trending Detection**: Hot, breakout, and declining movie detection

### 3. Speed Layer Storage
- **Cassandra Tables**: 6 main tables with 48h TTL
- **Windowing**: 5-minute tumbling windows
- **Optimization**: TimeWindowCompactionStrategy

## 🛠️ Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Check if packages are installed
   python speed_layer_checker.py
   
   # Install missing packages
   pip install <missing-package>
   ```

2. **Service Connection Issues**
   ```bash
   # Check service status
   docker-compose ps
   
   # View service logs
   docker-compose logs kafka
   docker-compose logs cassandra
   ```

3. **Cassandra Schema Issues**
   ```bash
   # Connect to Cassandra
   docker exec -it cassandra cqlsh
   
   # Check keyspace
   DESCRIBE KEYSPACE speed_layer;
   ```

4. **Spark Streaming Issues**
   ```bash
   # Check Spark logs
   ls /tmp/spark-streaming-checkpoints/
   
   # Reset checkpoint (if needed)
   rm -rf /tmp/spark-streaming-checkpoints/
   ```

### Performance Tuning

1. **Increase Kafka Throughput**
   ```yaml
   kafka:
     batch_size: 16384
     linger_ms: 100
     buffer_memory: 67108864
   ```

2. **Optimize Spark Resources**
   ```yaml
   spark:
     executor:
       memory: "4g"
       cores: 4
     driver:
       memory: "2g"
   ```

3. **Tune Cassandra Performance**
   ```yaml
   cassandra:
     batch:
       max_batch_size: 200
     performance:
       write_consistency: "LOCAL_ONE"  # For speed
   ```

## 📝 Monitoring

### Key Metrics to Monitor

1. **Kafka Metrics**
   - Producer throughput
   - Consumer lag
   - Topic partition count

2. **Spark Metrics**
   - Processing time per batch
   - Records per second
   - Memory usage

3. **Cassandra Metrics**
   - Write latency
   - TTL effectiveness
   - Compaction performance

### Health Checks

```bash
# Run comprehensive health check
python speed_layer_checker.py --verbose

# Check individual components
python -c "from kafka_producers.tmdb_stream_producer import TMDBAPIClient; print('Kafka producer OK')"
python -c "from cassandra_views.speed_view_manager import SpeedViewManager; print('Cassandra OK')"
```

## 🔄 Operations

### Starting the Speed Layer

```bash
# 1. Start external services
docker-compose up -d

# 2. Initialize schema
cqlsh -f cassandra_views/schema.cql

# 3. Run pre-flight check
python speed_layer_checker.py

# 4. Start streaming components
./start_speed_layer.sh  # (if created)
```

### Stopping the Speed Layer

```bash
# Stop streaming jobs (Ctrl+C in each terminal)
# Or kill processes
pkill -f "python.*streaming_jobs"
pkill -f "python.*kafka_producers"
```

### Monitoring Data Flow

```bash
# Check Kafka topics
kafka-topics --bootstrap-server localhost:9092 --list

# Monitor consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Check Cassandra data
cqlsh -e "SELECT COUNT(*) FROM speed_layer.review_sentiments;"
```

## 🚨 Alerts and Monitoring

### Set up alerts for:
- High consumer lag (> 1000 messages)
- Low throughput (< 100 records/sec)
- TTL data not expiring
- High error rates in streaming jobs

## 📚 Further Reading

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/)
- [TMDB API Documentation](https://developers.themoviedb.org/3)