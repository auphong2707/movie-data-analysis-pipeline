"""
Avro schemas for movie data pipeline.
"""

# Movie schema
MOVIE_SCHEMA = """
{
  "type": "record",
  "name": "Movie",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "title", "type": "string"},
    {"name": "original_title", "type": ["null", "string"], "default": null},
    {"name": "overview", "type": ["null", "string"], "default": null},
    {"name": "release_date", "type": ["null", "string"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "vote_average", "type": ["null", "double"], "default": null},
    {"name": "vote_count", "type": ["null", "int"], "default": null},
    {"name": "poster_path", "type": ["null", "string"], "default": null},
    {"name": "backdrop_path", "type": ["null", "string"], "default": null},
    {"name": "original_language", "type": ["null", "string"], "default": null},
    {"name": "budget", "type": ["null", "long"], "default": null},
    {"name": "revenue", "type": ["null", "long"], "default": null},
    {"name": "runtime", "type": ["null", "int"], "default": null},
    {"name": "status", "type": ["null", "string"], "default": null},
    {"name": "tagline", "type": ["null", "string"], "default": null},
    {"name": "homepage", "type": ["null", "string"], "default": null},
    {"name": "imdb_id", "type": ["null", "string"], "default": null},
    {
      "name": "genres",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Genre",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
          ]
        }
      },
      "default": []
    },
    {
      "name": "production_companies",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "ProductionCompany",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "logo_path", "type": ["null", "string"], "default": null},
            {"name": "origin_country", "type": ["null", "string"], "default": null}
          ]
        }
      },
      "default": []
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Person schema
PERSON_SCHEMA = """
{
  "type": "record",
  "name": "Person",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "biography", "type": ["null", "string"], "default": null},
    {"name": "birthday", "type": ["null", "string"], "default": null},
    {"name": "deathday", "type": ["null", "string"], "default": null},
    {"name": "place_of_birth", "type": ["null", "string"], "default": null},
    {"name": "profile_path", "type": ["null", "string"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "gender", "type": ["null", "int"], "default": null},
    {"name": "known_for_department", "type": ["null", "string"], "default": null},
    {"name": "homepage", "type": ["null", "string"], "default": null},
    {"name": "imdb_id", "type": ["null", "string"], "default": null},
    {
      "name": "also_known_as",
      "type": {
        "type": "array",
        "items": "string"
      },
      "default": []
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Credit schema
CREDIT_SCHEMA = """
{
  "type": "record",
  "name": "Credit",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "movie_id", "type": "int"},
    {"name": "person_id", "type": "int"},
    {"name": "credit_type", "type": {"type": "enum", "name": "CreditType", "symbols": ["cast", "crew"]}},
    {"name": "character", "type": ["null", "string"], "default": null},
    {"name": "job", "type": ["null", "string"], "default": null},
    {"name": "department", "type": ["null", "string"], "default": null},
    {"name": "order", "type": ["null", "int"], "default": null},
    {"name": "name", "type": "string"},
    {"name": "profile_path", "type": ["null", "string"], "default": null},
    {"name": "gender", "type": ["null", "int"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "known_for_department", "type": ["null", "string"], "default": null},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Review schema
REVIEW_SCHEMA = """
{
  "type": "record",
  "name": "Review",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "movie_id", "type": "int"},
    {"name": "author", "type": "string"},
    {"name": "content", "type": "string"},
    {"name": "created_at", "type": ["null", "string"], "default": null},
    {"name": "updated_at", "type": ["null", "string"], "default": null},
    {"name": "url", "type": ["null", "string"], "default": null},
    {
      "name": "author_details",
      "type": {
        "type": "record",
        "name": "AuthorDetails",
        "fields": [
          {"name": "name", "type": ["null", "string"], "default": null},
          {"name": "username", "type": ["null", "string"], "default": null},
          {"name": "avatar_path", "type": ["null", "string"], "default": null},
          {"name": "rating", "type": ["null", "double"], "default": null}
        ]
      },
      "default": null
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Rating schema
RATING_SCHEMA = """
{
  "type": "record",
  "name": "Rating",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "movie_id", "type": "int"},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "rating", "type": "double"},
    {"name": "timestamp", "type": "double"},
    {"name": "source", "type": "string", "default": "tmdb"},
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Schema registry mapping
SCHEMAS = {
    'movies': MOVIE_SCHEMA,
    'people': PERSON_SCHEMA,
    'credits': CREDIT_SCHEMA,
    'reviews': REVIEW_SCHEMA,
    'ratings': RATING_SCHEMA
}

# ==================== PARQUET SCHEMAS FOR BATCH LAYER ====================

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, BooleanType, TimestampType, DateType, ArrayType
)

# Bronze Layer Schemas (Raw Data)
BRONZE_MOVIE_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("raw_json", StringType(), False),
    StructField("api_endpoint", StringType(), False),
    StructField("extraction_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False),
    StructField("partition_day", IntegerType(), False),
    StructField("partition_hour", IntegerType(), False)
])

BRONZE_REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("movie_id", IntegerType(), False),
    StructField("raw_json", StringType(), False),
    StructField("api_endpoint", StringType(), False),
    StructField("extraction_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False),
    StructField("partition_day", IntegerType(), False),
    StructField("partition_hour", IntegerType(), False)
])

# Silver Layer Schemas (Cleaned & Enriched)
SILVER_MOVIE_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("adult", BooleanType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("budget", LongType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("production_companies", ArrayType(StringType()), True),
    StructField("cast", ArrayType(StringType()), True),
    StructField("crew", ArrayType(StringType()), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("quality_flag", StringType(), True),
    StructField("processed_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False),
    StructField("partition_genre", StringType(), True)
])

SILVER_REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("movie_id", IntegerType(), False),
    StructField("author", StringType(), True),
    StructField("content", StringType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("rating", DoubleType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("sentiment_compound", DoubleType(), True),
    StructField("sentiment_positive", DoubleType(), True),
    StructField("sentiment_neutral", DoubleType(), True),
    StructField("sentiment_negative", DoubleType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("processed_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False)
])

# Gold Layer Schemas (Aggregated Views)
GOLD_GENRE_ANALYTICS_SCHEMA = StructType([
    StructField("genre", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("total_movies", IntegerType(), False),
    StructField("avg_rating", DoubleType(), True),
    StructField("total_revenue", LongType(), True),
    StructField("avg_budget", LongType(), True),
    StructField("avg_popularity", DoubleType(), True),
    StructField("avg_sentiment", DoubleType(), True),
    StructField("total_reviews", IntegerType(), True),
    StructField("top_movies", ArrayType(StringType()), True),
    StructField("computed_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False)
])

GOLD_TRENDING_SCHEMA = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("window", StringType(), False),  # 7d, 30d, 90d
    StructField("trend_score", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("acceleration", DoubleType(), True),
    StructField("popularity_start", DoubleType(), True),
    StructField("popularity_end", DoubleType(), True),
    StructField("popularity_change_pct", DoubleType(), True),
    StructField("computed_date", DateType(), False),
    StructField("computed_timestamp", TimestampType(), False),
    StructField("partition_window", StringType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), False)
])

GOLD_TEMPORAL_ANALYTICS_SCHEMA = StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), True),
    StructField("total_movies", IntegerType(), False),
    StructField("total_revenue", LongType(), True),
    StructField("avg_rating", DoubleType(), True),
    StructField("avg_budget", LongType(), True),
    StructField("top_genre", StringType(), True),
    StructField("avg_sentiment", DoubleType(), True),
    StructField("yoy_movie_growth_pct", DoubleType(), True),
    StructField("yoy_revenue_growth_pct", DoubleType(), True),
    StructField("computed_timestamp", TimestampType(), False),
    StructField("partition_year", IntegerType(), False),
    StructField("partition_month", IntegerType(), True)
])

# Schema mapping for easy access
PARQUET_SCHEMAS = {
    'bronze': {
        'movies': BRONZE_MOVIE_SCHEMA,
        'reviews': BRONZE_REVIEW_SCHEMA
    },
    'silver': {
        'movies': SILVER_MOVIE_SCHEMA,
        'reviews': SILVER_REVIEW_SCHEMA
    },
    'gold': {
        'genre_analytics': GOLD_GENRE_ANALYTICS_SCHEMA,
        'trending': GOLD_TRENDING_SCHEMA,
        'temporal_analytics': GOLD_TEMPORAL_ANALYTICS_SCHEMA
    }
}