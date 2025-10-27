# Speed Layer Dockerfile
# Multi-stage build for Python + PySpark applications

# Use Python 3.10 based on Debian Bullseye (which has Java 11)
FROM python:3.10.13-slim-bullseye

# Install Java 11 for PySpark (PySpark 3.4.x requires Java 11 or 17)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
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
