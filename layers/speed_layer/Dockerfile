# Speed Layer Dockerfile
# Multi-stage build for Python + PySpark applications

FROM python:3.10-slim

# Install Java for PySpark
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy Speed Layer code
COPY . .

# Create logs and checkpoints directories
RUN mkdir -p logs checkpoints

# Set Python path to include current directory and config
ENV PYTHONPATH=/app:/app/config

# Expose common ports (for debugging/monitoring)
EXPOSE 4040

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command (can be overridden in docker-compose)
CMD ["python", "--version"]
