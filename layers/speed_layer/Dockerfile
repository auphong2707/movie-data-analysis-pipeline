# =====================================================================
# Speed Layer Dockerfile - Spark Structured Streaming Pipeline
# =====================================================================
# Description: Container for Speed Layer processing streaming data from Kafka
# Stack: Python 3.10 + Java 11 + PySpark 3.4.4 + Kafka + Cassandra 4.x
# =====================================================================

# ---------------------------------------------------------------------
# STEP 1: Base Image - Python 3.10 on Debian Bullseye
# ---------------------------------------------------------------------
# Reason for choosing bullseye: Stable support for OpenJDK 11 from official repository
FROM python:3.10-slim-bullseye

# ---------------------------------------------------------------------
# STEP 2: Install OpenJDK 11 and necessary tools
# ---------------------------------------------------------------------
# Java 11: Most stable version for Spark 3.4.x (avoids class version errors)
# procps: Provides ps, top for container monitoring
# curl: Download dependencies or health checks
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------
# STEP 3: Set up Java environment variables
# ---------------------------------------------------------------------
# JAVA_HOME: Ensure Spark finds the correct Java runtime
# PATH: Allow calling java, javac from command line
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ---------------------------------------------------------------------
# STEP 4: Create working directory
# ---------------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------------
# STEP 5: Copy requirements.txt and install Python dependencies
# ---------------------------------------------------------------------
# Copy requirements.txt first to leverage Docker layer caching
# (only rebuild this layer when requirements.txt changes)
COPY requirements.txt /app/

# Install all dependencies from requirements.txt
# --no-cache-dir: Reduce image size
# --prefer-binary: Prefer wheel packages, faster builds
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# ---------------------------------------------------------------------
# STEP 6: Copy source code into container
# ---------------------------------------------------------------------
# Copy streaming_jobs directory (contains streaming processing logic)
COPY streaming_jobs/ /app/streaming_jobs/

# Copy supporting modules
COPY kafka_producers/ /app/kafka_producers/
COPY cassandra_views/ /app/cassandra_views/
COPY connectors/ /app/connectors/
COPY config/ /app/config/

# ---------------------------------------------------------------------
# STEP 7: Create logs and checkpoints directories
# ---------------------------------------------------------------------
# logs/: Store application logs
# checkpoints/: Spark Structured Streaming checkpoints (fault-tolerance)
RUN mkdir -p /app/logs /app/checkpoints

# ---------------------------------------------------------------------
# STEP 8: Set up PYTHONPATH
# ---------------------------------------------------------------------
# Ensure Python finds modules from /app
ENV PYTHONPATH=/app

# ---------------------------------------------------------------------
# STEP 9: Expose Spark UI port
# ---------------------------------------------------------------------
# Port 4040: Spark Web UI for monitoring streaming jobs
EXPOSE 4040

# ---------------------------------------------------------------------
# STEP 10: Health Check
# ---------------------------------------------------------------------
# Check if container is alive (every 30s)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1

# ---------------------------------------------------------------------
# STEP 11: Default Command
# ---------------------------------------------------------------------
# Default: show help/version (docker-compose will override with specific job)
# Docker Compose services override this with specific streaming job
CMD ["python3", "--version"]

# =====================================================================
# 🚀 Usage Guide:
# =====================================================================
# Build image:
#   docker build -t speed-layer:latest .
#
# Run standalone:
#   docker run -it --network=host speed-layer:latest
#
# Use with Docker Compose:
#   In docker-compose.yml, override CMD if needed:
#   command: ["python3", "streaming_jobs/movie_aggregation_stream.py"]
#
# Use spark-submit with Cassandra connector:
#   docker run -it speed-layer:latest spark-submit \
#     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
#     --conf spark.cassandra.connection.host=cassandra \
#     streaming_jobs/movie_aggregation_stream.py
# =====================================================================
