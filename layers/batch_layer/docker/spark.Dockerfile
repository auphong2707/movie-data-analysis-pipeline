# ========================================
# Spark Dockerfile for Batch Layer
# ========================================
# Base: Apache Spark 3.5.0 with Python 3.11
# Includes: PySpark, HDFS client, project dependencies
# ========================================

FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and setuptools
RUN pip install --upgrade pip setuptools wheel

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
    # Install in stages to avoid memory issues
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    requests==2.32.3 \
    pymongo==4.10.1 \
    python-dotenv==1.0.1 \
    vaderSentiment==3.3.2

RUN pip install --no-cache-dir \
    great-expectations==1.1.0 \
    "pandas>=1.3.0,<2.2" \
    "numpy>=1.21.0,<1.25"

RUN pip install --no-cache-dir \
    pytest==8.3.5 \
    pytest-cov==5.0.0

# Set Python environment
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Set Spark configuration
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy project code (or mount at runtime)
COPY . /app

# Create directories for Great Expectations
RUN mkdir -p /app/ge_expectations

# Set permissions
RUN chmod -R 755 /app

# Expose Spark ports
EXPOSE 8080 7077 6066 4040

# Default command (overridden by docker-compose)
CMD ["/bin/bash"]
