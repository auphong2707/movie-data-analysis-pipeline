# =====================================================================
# Speed Layer Dockerfile - Spark Structured Streaming Pipeline
# =====================================================================
# Mô tả: Container cho Speed Layer xử lý streaming data từ Kafka
# Stack: Python 3.10 + Java 11 + PySpark 3.4.4 + Kafka + Cassandra 4.x
# =====================================================================

# ---------------------------------------------------------------------
# BƯỚC 1: Base Image - Python 3.10 trên Debian Bullseye
# ---------------------------------------------------------------------
# Lý do chọn bullseye: Hỗ trợ OpenJDK 11 ổn định từ repository chính thức
FROM python:3.10-slim-bullseye

# ---------------------------------------------------------------------
# BƯỚC 2: Cài đặt OpenJDK 11 và các công cụ cần thiết
# ---------------------------------------------------------------------
# Java 11: Phiên bản ổn định nhất cho Spark 3.4.x (tránh lỗi class version)
# procps: Cung cấp ps, top để monitoring containers
# curl: Download dependencies hoặc health checks
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------
# BƯỚC 3: Thiết lập biến môi trường Java
# ---------------------------------------------------------------------
# JAVA_HOME: Đảm bảo Spark tìm đúng Java runtime
# PATH: Cho phép gọi java, javac từ command line
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ---------------------------------------------------------------------
# BƯỚC 4: Tạo thư mục làm việc
# ---------------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------------
# BƯỚC 5: Cài đặt Python dependencies
# ---------------------------------------------------------------------
# PySpark 3.4.4: Phiên bản stable, tương thích Java 11 và Cassandra 4.x
# kafka-python: Consumer/Producer Kafka messages  
# confluent-kafka: Kafka producers với Avro serialization (for tmdb producer)
# fastavro: Required by confluent-kafka for Avro serialization
# cassandra-driver: Connector cho Apache Cassandra
# --no-cache-dir: Giảm image size
# --prefer-binary: Ưu tiên wheel packages, build nhanh hơn
RUN pip install --no-cache-dir --prefer-binary \
    pyspark==3.4.4 \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    fastavro==1.9.4 \
    cassandra-driver==3.29.1 \
    vaderSentiment==3.3.2 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    avro==1.11.3 \
    python-dotenv==1.0.0

# ---------------------------------------------------------------------
# BƯỚC 6: Copy source code vào container
# ---------------------------------------------------------------------
# Copy thư mục streaming_jobs (chứa logic xử lý streaming)
COPY streaming_jobs/ /app/streaming_jobs/

# Copy các modules hỗ trợ
COPY kafka_producers/ /app/kafka_producers/
COPY cassandra_views/ /app/cassandra_views/

# Copy requirements.txt để reference (optional)
COPY requirements.txt /app/

# Note: config/ folder will be mounted as volume in docker-compose.yml
# so we don't copy it here to avoid conflicts

# ---------------------------------------------------------------------
# BƯỚC 7: Tạo thư mục logs và checkpoints
# ---------------------------------------------------------------------
# logs/: Lưu application logs
# checkpoints/: Spark Structured Streaming checkpoints (fault-tolerance)
RUN mkdir -p /app/logs /app/checkpoints

# ---------------------------------------------------------------------
# BƯỚC 8: Thiết lập PYTHONPATH
# ---------------------------------------------------------------------
# Đảm bảo Python tìm thấy modules từ /app và /app/config
ENV PYTHONPATH=/app:/app/config

# ---------------------------------------------------------------------
# BƯỚC 9: Expose port Spark UI
# ---------------------------------------------------------------------
# Port 4040: Spark Web UI để monitoring streaming jobs
EXPOSE 4040

# ---------------------------------------------------------------------
# BƯỚC 10: Health Check
# ---------------------------------------------------------------------
# Kiểm tra container còn sống không (mỗi 30s)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1

# ---------------------------------------------------------------------
# BƯỚC 11: Default Command
# ---------------------------------------------------------------------
# Default: show help/version (docker-compose will override with specific job)
# Docker Compose services override this with specific streaming job
CMD ["python3", "--version"]

# =====================================================================
# 🚀 Hướng dẫn sử dụng:
# =====================================================================
# Build image:
#   docker build -t speed-layer:latest .
#
# Run standalone:
#   docker run -it --network=host speed-layer:latest
#
# Dùng với Docker Compose:
#   Trong docker-compose.yml, override CMD nếu cần:
#   command: ["python3", "streaming_jobs/movie_aggregation_stream.py"]
#
# Dùng spark-submit với Cassandra connector:
#   docker run -it speed-layer:latest spark-submit \
#     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
#     --conf spark.cassandra.connection.host=cassandra \
#     streaming_jobs/movie_aggregation_stream.py
# =====================================================================
