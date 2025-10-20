"""
Schema definitions for the Batch Layer data pipeline.

This module defines the strict schemas for Bronze, Silver, and Gold layers
according to the TMDB data processing requirements.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType, DateType, ArrayType
)

# Bronze Layer Schema - Raw TMDB API responses
BRONZE_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("raw_json", StringType(), False),
    StructField("api_endpoint", StringType(), False),
    StructField("extraction_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False),
    StructField("partition_day", IntegerType(), False),
    StructField("partition_hour", IntegerType(), False)
])

# Cast and Crew sub-schemas for Silver layer
CAST_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("character", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("cast_id", IntegerType(), True),
    StructField("credit_id", StringType(), True)
])

CREW_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("job", StringType(), True),
    StructField("department", StringType(), True),
    StructField("credit_id", StringType(), True)
])

PRODUCTION_COMPANY_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("logo_path", StringType(), True),
    StructField("origin_country", StringType(), True)
])

# Silver Layer Schema - Cleaned and enriched data
SILVER_SCHEMA = StructType([
    # Core movie attributes
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("budget", LongType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("overview", StringType(), True),
    
    # Cast and crew information
    StructField("cast", ArrayType(CAST_SCHEMA), True),
    StructField("crew", ArrayType(CREW_SCHEMA), True),
    StructField("production_companies", ArrayType(PRODUCTION_COMPANY_SCHEMA), True),
    StructField("production_countries", ArrayType(StringType()), True),
    StructField("spoken_languages", ArrayType(StringType()), True),
    
    # Derived attributes
    StructField("sentiment_score", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("quality_flag", StringType(), False),
    StructField("processed_timestamp", TimestampType(), False),
    
    # Partition columns
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False),
    StructField("partition_genre", StringType(), False)
])

# Gold Layer Schemas - Aggregated views

# Genre Analytics Schema
GENRE_ANALYTICS_SCHEMA = StructType([
    StructField("genre", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("total_movies", IntegerType(), False),
    StructField("avg_rating", DoubleType(), True),
    StructField("total_revenue", LongType(), True),
    StructField("avg_budget", DoubleType(), True),
    StructField("avg_sentiment", DoubleType(), True),
    StructField("top_movies", ArrayType(StringType()), True),
    StructField("top_directors", ArrayType(StringType()), True),
    StructField("computed_timestamp", TimestampType(), False)
])

# Trending Scores Schema
TRENDING_SCORES_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("window", StringType(), False),  # "7d", "30d", "90d"
    StructField("trend_score", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("popularity_change", DoubleType(), True),
    StructField("rating_momentum", DoubleType(), True),
    StructField("computed_date", DateType(), False),
    StructField("computed_timestamp", TimestampType(), False)
])

# Temporal Analysis Schema (Year-over-Year)
TEMPORAL_ANALYSIS_SCHEMA = StructType([
    StructField("metric_type", StringType(), False),  # "revenue", "popularity", "ratings"
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("current_value", DoubleType(), True),
    StructField("previous_year_value", DoubleType(), True),
    StructField("yoy_change_percent", DoubleType(), True),
    StructField("yoy_change_absolute", DoubleType(), True),
    StructField("trend_direction", StringType(), True),  # "up", "down", "stable"
    StructField("computed_timestamp", TimestampType(), False)
])

# Actor Networks Schema
ACTOR_NETWORKS_SCHEMA = StructType([
    StructField("actor_id", StringType(), False),
    StructField("actor_name", StringType(), True),
    StructField("collaborator_id", StringType(), False),
    StructField("collaborator_name", StringType(), True),
    StructField("collaboration_count", IntegerType(), False),
    StructField("collaboration_score", DoubleType(), False),
    StructField("recent_collaboration_date", DateType(), True),
    StructField("shared_movies", ArrayType(StringType()), True),
    StructField("network_centrality", DoubleType(), True),
    StructField("snapshot_month", StringType(), False),  # "YYYY-MM"
    StructField("computed_timestamp", TimestampType(), False)
])

# MongoDB Batch Views Schema
BATCH_VIEW_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("view_type", StringType(), False),  # "genre_analytics", "trending", "temporal"
    StructField("data", StringType(), False),  # JSON string of the actual data
    StructField("computed_at", TimestampType(), False),
    StructField("batch_run_id", StringType(), False),
    StructField("schema_version", StringType(), False)
])

# Quality flags enumeration
class QualityFlags:
    """Quality flag constants for data validation."""
    OK = "OK"
    WARNING = "WARNING"
    ERROR = "ERROR"

# Sentiment labels enumeration
class SentimentLabels:
    """Sentiment label constants."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"

# View types for MongoDB collections
class ViewTypes:
    """Batch view type constants."""
    GENRE_ANALYTICS = "genre_analytics"
    TRENDING = "trending"
    TEMPORAL = "temporal"
    ACTOR_NETWORKS = "actor_networks"

def validate_bronze_schema(df):
    """
    Validate that a DataFrame conforms to the Bronze schema.
    
    Args:
        df: PySpark DataFrame to validate
        
    Returns:
        bool: True if schema matches, False otherwise
        
    Raises:
        ValueError: If schema validation fails with details
    """
    expected_fields = set(field.name for field in BRONZE_SCHEMA.fields)
    actual_fields = set(df.columns)
    
    if expected_fields != actual_fields:
        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields
        raise ValueError(
            f"Bronze schema validation failed. "
            f"Missing fields: {missing}, Extra fields: {extra}"
        )
    
    return True

def validate_silver_schema(df):
    """
    Validate that a DataFrame conforms to the Silver schema.
    
    Args:
        df: PySpark DataFrame to validate
        
    Returns:
        bool: True if schema matches, False otherwise
        
    Raises:
        ValueError: If schema validation fails with details
    """
    expected_fields = set(field.name for field in SILVER_SCHEMA.fields)
    actual_fields = set(df.columns)
    
    if expected_fields != actual_fields:
        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields
        raise ValueError(
            f"Silver schema validation failed. "
            f"Missing fields: {missing}, Extra fields: {extra}"
        )
    
    return True

def get_schema_by_layer(layer: str) -> StructType:
    """
    Get the schema for a specific data layer.
    
    Args:
        layer: Layer name ("bronze", "silver", or "gold")
        
    Returns:
        StructType: The corresponding schema
        
    Raises:
        ValueError: If layer is not recognized
    """
    schemas = {
        "bronze": BRONZE_SCHEMA,
        "silver": SILVER_SCHEMA,
        "genre_analytics": GENRE_ANALYTICS_SCHEMA,
        "trending_scores": TRENDING_SCORES_SCHEMA,
        "temporal_analysis": TEMPORAL_ANALYSIS_SCHEMA,
        "actor_networks": ACTOR_NETWORKS_SCHEMA,
        "batch_view": BATCH_VIEW_SCHEMA
    }
    
    if layer.lower() not in schemas:
        raise ValueError(f"Unknown layer: {layer}. Available: {list(schemas.keys())}")
    
    return schemas[layer.lower()]