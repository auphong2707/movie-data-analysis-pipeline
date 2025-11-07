"""
Unit tests for Silver layer transformations.
"""

import pytest
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from layers.batch_layer.spark_jobs.utils.transformations import (
    deduplicate_by_key, validate_completeness, add_quality_flags
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("batch_layer_tests") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


def test_deduplication(spark):
    """Test deduplication by key."""
    # Create test data with duplicates
    data = [
        (1, "Movie A", "2025-01-01"),
        (1, "Movie A Updated", "2025-01-02"),  # Duplicate, newer
        (2, "Movie B", "2025-01-01"),
    ]
    
    schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("extraction_timestamp", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Deduplicate
    result = deduplicate_by_key(df, ["movie_id"], "extraction_timestamp", ascending=False)
    
    # Should keep only 2 records (latest for movie_id 1)
    assert result.count() == 2
    
    # Check that we kept the newer record for movie_id 1
    movie_1 = result.filter(result.movie_id == 1).collect()[0]
    assert movie_1.title == "Movie A Updated"


def test_validate_completeness(spark):
    """Test completeness validation."""
    data = [
        (1, "Movie A", 7.5),
        (2, "Movie B", None),  # Missing rating
        (3, None, 8.0),  # Missing title
        (4, "Movie D", 6.5),
    ]
    
    schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("rating", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Validate
    _, stats = validate_completeness(df, ["title", "rating"], threshold=0.75)
    
    # Should pass threshold (2/4 = 50% complete for each field, avg = 50%)
    # But threshold is 75%, so should fail
    assert stats["total_rows"] == 4
    assert not stats["passes_threshold"]


def test_add_quality_flags(spark):
    """Test quality flag addition."""
    data = [
        (1, "Movie A", 7.5, 100.0),
        (2, None, 8.0, 200.0),  # Missing title (ERROR)
        (3, "Movie C", -1.0, 150.0),  # Invalid rating (WARNING)
        (4, "Movie D", 6.5, 180.0),  # OK
    ]
    
    schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("popularity", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Add quality flags
    result = add_quality_flags(df, ["movie_id", "title"], ["rating"])
    
    # Check flags
    flags = {row.movie_id: row.quality_flag for row in result.collect()}
    
    assert flags[1] == "OK"
    assert flags[2] == "ERROR"  # Missing required field
    assert flags[3] == "WARNING"  # Invalid numeric value
    assert flags[4] == "OK"


def test_calculate_window_aggregates(spark):
    """Test rolling window aggregations."""
    from layers.batch_layer.spark_jobs.utils.transformations import calculate_window_aggregates
    
    data = [
        (1, "2025-01-01", 100.0),
        (1, "2025-01-02", 110.0),
        (1, "2025-01-03", 120.0),
        (1, "2025-01-04", 130.0),
        (1, "2025-01-05", 140.0),
        (1, "2025-01-06", 150.0),
        (1, "2025-01-07", 160.0),
        (1, "2025-01-08", 170.0),
    ]
    
    schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("popularity", DoubleType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Calculate 7-day rolling average
    result = calculate_window_aggregates(df, ["movie_id"], "date", "popularity", [7])
    
    # Last record should have 7-day average of (100+110+120+130+140+150+160)/7 = 130
    last_row = result.orderBy("date", ascending=False).first()
    assert abs(last_row.popularity_7d_avg - 130.0) < 0.1


def test_sentiment_analysis():
    """Test sentiment analysis (mocked)."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    
    analyzer = SentimentIntensityAnalyzer()
    
    # Positive text
    positive_text = "This movie is absolutely amazing! I loved every minute of it."
    scores = analyzer.polarity_scores(positive_text)
    assert scores['compound'] > 0.5
    
    # Negative text
    negative_text = "This movie is terrible. Waste of time and money."
    scores = analyzer.polarity_scores(negative_text)
    assert scores['compound'] < -0.5
    
    # Neutral text
    neutral_text = "The movie was okay. It had some good parts and some bad parts."
    scores = analyzer.polarity_scores(neutral_text)
    assert -0.5 < scores['compound'] < 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
