#!/bin/bash
# ========================================
# Airflow Initialization Script
# ========================================
# Sets up Airflow connections for HDFS, MongoDB, and Spark
# ========================================

set -e

echo "=========================================="
echo "Airflow Initialization Script"
echo "=========================================="

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 10

# Create HDFS connection
echo "Creating HDFS connection..."
airflow connections delete hdfs_default 2>/dev/null || true
airflow connections add hdfs_default \
    --conn-type hdfs \
    --conn-host namenode \
    --conn-port 8020 \
    --conn-extra '{"namenode_principal": ""}' \
    2>/dev/null || echo "HDFS connection already exists"

# Create MongoDB connection
echo "Creating MongoDB connection..."
airflow connections delete mongo_default 2>/dev/null || true
airflow connections add mongo_default \
    --conn-type mongo \
    --conn-host mongo \
    --conn-port 27017 \
    --conn-login admin \
    --conn-password password \
    --conn-schema moviedb \
    --conn-extra '{"authSource": "admin"}' \
    2>/dev/null || echo "MongoDB connection already exists"

# Create Spark connection
echo "Creating Spark connection..."
airflow connections delete spark_default 2>/dev/null || true
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-extra '{"queue": "default", "deploy-mode": "client"}' \
    2>/dev/null || echo "Spark connection already exists"

# Create HTTP connection for TMDB API
echo "Creating TMDB API connection..."
airflow connections delete tmdb_api 2>/dev/null || true
airflow connections add tmdb_api \
    --conn-type http \
    --conn-host https://api.themoviedb.org/3 \
    --conn-extra "{\"api_key\": \"${TMDB_API_KEY}\"}" \
    2>/dev/null || echo "TMDB API connection already exists"

# Set variables
echo "Setting Airflow variables..."
airflow variables set hdfs_namenode "hdfs://namenode:8020" 2>/dev/null || true
airflow variables set spark_master "spark://spark-master:7077" 2>/dev/null || true
airflow variables set mongodb_uri "mongodb://admin:password@mongo:27017/moviedb?authSource=admin" 2>/dev/null || true
airflow variables set tmdb_api_key "${TMDB_API_KEY}" 2>/dev/null || true

echo "=========================================="
echo "Airflow initialization completed!"
echo "=========================================="
