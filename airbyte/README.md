# Airbyte Configuration

This directory contains Airbyte configuration files for the movie data pipeline.

## Files

- `docker-compose.airbyte.yml` - Airbyte services configuration
- `sources/` - Source connector configurations
- `destinations/` - Destination connector configurations
- `connections/` - Connection configurations between sources and destinations
- `transformations/` - Data transformation configurations (dbt models)

## Setup

1. Start Airbyte services: `docker-compose -f docker-compose.airbyte.yml up -d`
2. Access Airbyte UI at http://localhost:8000 (user: airbyte, password: password)
3. Configure sources and destinations using the provided configurations
4. Set up connections and sync schedules

## Sources Configured

- **TMDB API**: Custom HTTP API source for TMDB endpoints
- **File System**: For batch processing of movie data files

## Destinations Configured

- **Kafka**: Stream processed data to Kafka topics
- **MongoDB**: Store processed data in MongoDB collections
- **MinIO/S3**: Store raw and processed data files

## Transformations

- Data cleaning and validation
- Schema normalization
- Sentiment analysis preparation
- Aggregations and metrics calculation