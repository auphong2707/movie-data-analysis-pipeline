# ========================================
# Airflow Dockerfile for Batch Layer
# ========================================
# Base: Apache Airflow 2.8.0 with Python 3.11
# Includes: Project dependencies, DAGs, connections
# ========================================

FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    procps \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Upgrade pip
RUN pip install --upgrade pip setuptools wheel

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies for batch layer
RUN pip install --no-cache-dir \
    apache-airflow==2.8.0 \
    pyspark==3.5.0 \
    requests==2.32.3 \
    pymongo==4.10.1 \
    python-dotenv==1.0.1 \
    vaderSentiment==3.3.2 \
    great-expectations==1.1.0 \
    "pandas>=1.3.0,<2.2" \
    "numpy>=1.21.0,<1.25" \
    psycopg2-binary==2.9.10

# Set Python environment
ENV PYTHONPATH=/app:/opt/airflow:$PYTHONPATH
ENV AIRFLOW_HOME=/opt/airflow

# Create necessary directories
RUN mkdir -p /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    /opt/airflow/config

# Copy project files
COPY --chown=airflow:airflow . /app

# Set permissions
USER root
RUN chmod -R 755 /app
USER airflow

# Expose Airflow ports
EXPOSE 8080

# Default command (overridden by docker-compose)
CMD ["airflow", "webserver"]
