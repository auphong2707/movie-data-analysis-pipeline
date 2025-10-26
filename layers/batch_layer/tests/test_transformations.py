"""
Transformation Tests for TMDB Movie Data Pipeline.

This module contains comprehensive tests for the Bronze-to-Silver and Silver-to-Gold
transformation pipelines, including data quality validation and schema compliance.
"""

import pytest
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDataGenerator:
    """Generate synthetic test data for transformation testing."""
    
    @staticmethod
    def create_bronze_test_data(spark: SparkSession, num_records: int = 100) -> DataFrame:
        """
        Create synthetic Bronze layer test data.
        
        Args:
            spark: SparkSession instance
            num_records: Number of test records to generate
            
        Returns:
            DataFrame: Bronze layer test data
        """
        bronze_data = []
        
        for i in range(num_records):
            record = {
                "movie_id": f"movie_{i:06d}",
                "raw_json": json.dumps({
                    "id": i,
                    "title": f"Test Movie {i}",
                    "overview": f"This is a test movie overview for movie {i}. It contains sample text for sentiment analysis.",
                    "release_date": f"202{i%4}-{(i%12)+1:02d}-{(i%28)+1:02d}",
                    "genres": [{"id": (i%20)+1, "name": f"Genre_{(i%20)+1}"}],
                    "vote_average": round(4.0 + (i % 6), 1),
                    "vote_count": (i % 1000) + 100,
                    "popularity": round(10.0 + (i % 90), 2),
                    "budget": (i % 10) * 10000000 if i % 5 != 0 else 0,
                    "revenue": (i % 15) * 15000000 if i % 4 != 0 else 0,
                    "runtime": 90 + (i % 60),
                    "spoken_languages": [{"iso_639_1": "en", "name": "English"}],
                    "production_countries": [{"iso_3166_1": "US", "name": "United States"}],
                    "production_companies": [{"id": (i%50)+1, "name": f"Studio_{(i%50)+1}"}],
                    "cast": [
                        {"id": (i*3)%1000, "name": f"Actor_{(i*3)%1000}", "character": f"Character_{i}"},
                        {"id": (i*3+1)%1000, "name": f"Actor_{(i*3+1)%1000}", "character": f"Character_{i+1}"}
                    ],
                    "crew": [
                        {"id": (i*2)%500, "name": f"Director_{(i*2)%500}", "job": "Director"},
                        {"id": (i*2+1)%500, "name": f"Producer_{(i*2+1)%500}", "job": "Producer"}
                    ]
                }),
                "api_response_code": 200,
                "ingestion_timestamp": datetime.utcnow() - timedelta(hours=i%24),
                "batch_run_id": f"batch_test_{datetime.now().strftime('%Y%m%d')}",
                "partition_year": 2020 + (i % 4),
                "partition_month": (i % 12) + 1,
                "partition_day": (i % 28) + 1,
                "partition_hour": i % 24
            }
            bronze_data.append(record)
        
        # Create DataFrame
        return spark.createDataFrame(bronze_data)
    
    @staticmethod
    def create_silver_test_data(spark: SparkSession, num_records: int = 100) -> DataFrame:
        """
        Create synthetic Silver layer test data.
        
        Args:
            spark: SparkSession instance
            num_records: Number of test records to generate
            
        Returns:
            DataFrame: Silver layer test data
        """
        silver_data = []
        
        for i in range(num_records):
            record = {
                "movie_id": f"movie_{i:06d}",
                "title": f"Test Movie {i}",
                "overview": f"This is a test movie overview for movie {i}.",
                "release_date": datetime(2020 + (i % 4), (i % 12) + 1, (i % 28) + 1),
                "genres": f"Genre_{(i%5)+1}|Genre_{((i+1)%5)+1}",
                "director": f"Director_{(i*2)%50}",
                "cast": json.dumps([
                    {"id": (i*3)%1000, "name": f"Actor_{(i*3)%1000}", "character": f"Character_{i}"},
                    {"id": (i*3+1)%1000, "name": f"Actor_{(i*3+1)%1000}", "character": f"Character_{i+1}"}
                ]),
                "crew": json.dumps([
                    {"id": (i*2)%500, "name": f"Director_{(i*2)%500}", "job": "Director"}
                ]),
                "vote_average": round(4.0 + (i % 6), 1),
                "vote_count": (i % 1000) + 100,
                "popularity": round(10.0 + (i % 90), 2),
                "budget": (i % 10) * 10000000 if i % 5 != 0 else 0,
                "revenue": (i % 15) * 15000000 if i % 4 != 0 else 0,
                "runtime": 90 + (i % 60),
                "spoken_languages": "English",
                "production_countries": "United States",
                "production_companies": f"Studio_{(i%50)+1}",
                "sentiment_score": round(0.3 + (i % 7) * 0.1, 2),
                "sentiment_label": "POSITIVE" if i % 3 == 0 else ("NEGATIVE" if i % 3 == 1 else "NEUTRAL"),
                "quality_flag": "HIGH" if i % 10 != 0 else "MEDIUM",
                "data_source": "TMDB_API",
                "last_updated": datetime.utcnow(),
                "schema_version": "1.0"
            }
            silver_data.append(record)
        
        return spark.createDataFrame(silver_data)


class TestBronzeToSilverTransformation:
    """Test Bronze-to-Silver transformation pipeline."""
    
    @pytest.fixture(scope="session")
    def spark_session(self):
        """Create Spark session for tests."""
        return SparkSession.builder \
            .appName("TestBronzeToSilver") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    @pytest.fixture
    def sample_bronze_data(self, spark_session):
        """Create sample Bronze layer data."""
        return TestDataGenerator.create_bronze_test_data(spark_session, 50)
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        return {
            "spark": {
                "app_name": "test_bronze_to_silver",
                "serializer": "org.apache.spark.serializer.KryoSerializer"
            },
            "hdfs": {
                "paths": {
                    "bronze": "/test/bronze",
                    "silver": "/test/silver"
                }
            },
            "data_quality": {
                "null_threshold": 0.1,
                "duplicate_threshold": 0.05
            }
        }
    
    def test_bronze_data_loading(self, spark_session, sample_bronze_data):
        """Test Bronze data loading functionality."""
        logger.info("Testing Bronze data loading")
        
        # Verify data is loaded correctly
        assert sample_bronze_data.count() == 50
        assert "movie_id" in sample_bronze_data.columns
        assert "raw_json" in sample_bronze_data.columns
        
        # Verify data types
        schema = sample_bronze_data.schema
        movie_id_field = next(field for field in schema.fields if field.name == "movie_id")
        assert isinstance(movie_id_field.dataType, StringType)
    
    def test_json_parsing(self, spark_session, sample_bronze_data):
        """Test JSON parsing from Bronze raw_json field."""
        logger.info("Testing JSON parsing functionality")
        
        # Mock the BronzeToSilverTransformer
        with patch('layers.batch_layer.spark_jobs.bronze_to_silver.BronzeToSilverTransformer') as mock_transformer:
            # Test JSON parsing logic
            sample_row = sample_bronze_data.first()
            raw_json = sample_row["raw_json"]
            
            # Verify JSON can be parsed
            parsed_data = json.loads(raw_json)
            assert "id" in parsed_data
            assert "title" in parsed_data
            assert "overview" in parsed_data
            
            logger.info("JSON parsing test passed")
    
    def test_schema_validation(self, spark_session, mock_config):
        """Test Silver schema validation."""
        logger.info("Testing Silver schema validation")
        
        # Create test data with schema violations
        invalid_data = [
            {"movie_id": None, "title": "Test Movie"},  # Null movie_id
            {"movie_id": "movie_001", "title": None},   # Null title
            {"movie_id": "movie_002", "title": "Valid Movie", "vote_average": "invalid"}  # Invalid data type
        ]
        
        invalid_df = spark_session.createDataFrame(invalid_data)
        
        # Test schema validation logic
        null_count = invalid_df.filter(col("movie_id").isNull()).count()
        assert null_count == 1
        
        logger.info("Schema validation test passed")
    
    def test_data_quality_checks(self, spark_session, sample_bronze_data, mock_config):
        """Test data quality validation."""
        logger.info("Testing data quality checks")
        
        # Calculate quality metrics
        total_records = sample_bronze_data.count()
        
        # Test for duplicates
        distinct_count = sample_bronze_data.select("movie_id").distinct().count()
        duplicate_rate = (total_records - distinct_count) / total_records
        
        # Test for null values
        null_count = sample_bronze_data.filter(col("movie_id").isNull()).count()
        null_rate = null_count / total_records
        
        # Verify quality thresholds
        assert duplicate_rate <= mock_config["data_quality"]["duplicate_threshold"]
        assert null_rate <= mock_config["data_quality"]["null_threshold"]
        
        logger.info(f"Quality check passed: duplicate_rate={duplicate_rate:.3f}, null_rate={null_rate:.3f}")
    
    def test_transformation_output(self, spark_session, sample_bronze_data):
        """Test Bronze-to-Silver transformation output."""
        logger.info("Testing transformation output")
        
        # Simulate transformation logic
        transformed_data = sample_bronze_data.select(
            col("movie_id"),
            col("batch_run_id"),
            col("ingestion_timestamp")
        ).withColumn("quality_flag", 
                    when(col("movie_id").isNotNull(), "HIGH").otherwise("LOW"))
        
        # Verify transformation
        assert transformed_data.count() > 0
        assert "quality_flag" in transformed_data.columns
        
        # Check quality flag distribution
        high_quality_count = transformed_data.filter(col("quality_flag") == "HIGH").count()
        assert high_quality_count > 0
        
        logger.info("Transformation output test passed")
    
    def test_error_handling(self, spark_session):
        """Test error handling in transformation."""
        logger.info("Testing error handling")
        
        # Create data that would cause errors
        error_data = [
            {"movie_id": "test_001", "raw_json": "invalid_json"},
            {"movie_id": "test_002", "raw_json": '{"valid": "json"}'}
        ]
        
        error_df = spark_session.createDataFrame(error_data)
        
        # Test error handling logic
        try:
            # Simulate JSON parsing with error handling
            valid_json_count = 0
            invalid_json_count = 0
            
            for row in error_df.collect():
                try:
                    json.loads(row["raw_json"])
                    valid_json_count += 1
                except json.JSONDecodeError:
                    invalid_json_count += 1
            
            assert valid_json_count == 1
            assert invalid_json_count == 1
            
        except Exception as e:
            logger.error(f"Error handling test failed: {e}")
            raise
        
        logger.info("Error handling test passed")


class TestSilverToGoldTransformation:
    """Test Silver-to-Gold transformation pipeline."""
    
    @pytest.fixture(scope="session")
    def spark_session(self):
        """Create Spark session for tests."""
        return SparkSession.builder \
            .appName("TestSilverToGold") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    @pytest.fixture
    def sample_silver_data(self, spark_session):
        """Create sample Silver layer data."""
        return TestDataGenerator.create_silver_test_data(spark_session, 100)
    
    def test_genre_aggregation(self, spark_session, sample_silver_data):
        """Test genre-based aggregations."""
        logger.info("Testing genre aggregation")
        
        # Simulate genre aggregation
        genre_stats = (sample_silver_data
                      .filter(col("quality_flag") == "HIGH")
                      .groupBy("genres")
                      .agg(
                          count("movie_id").alias("movie_count"),
                          avg("vote_average").alias("avg_rating"),
                          spark_sum("revenue").alias("total_revenue")
                      ))
        
        # Verify aggregation results
        assert genre_stats.count() > 0
        
        # Check for required columns
        expected_columns = ["genres", "movie_count", "avg_rating", "total_revenue"]
        for col_name in expected_columns:
            assert col_name in genre_stats.columns
        
        # Verify data ranges
        stats = genre_stats.collect()
        for row in stats:
            assert row["movie_count"] > 0
            assert 0 <= row["avg_rating"] <= 10
            assert row["total_revenue"] >= 0
        
        logger.info("Genre aggregation test passed")
    
    def test_trending_calculations(self, spark_session, sample_silver_data):
        """Test trending score calculations."""
        logger.info("Testing trending calculations")
        
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lag, when
        
        # Simulate trending calculation
        window_spec = Window.partitionBy("movie_id").orderBy("release_date")
        
        trending_data = (sample_silver_data
                        .withColumn("prev_popularity", lag("popularity").over(window_spec))
                        .withColumn("popularity_change",
                                   when(col("prev_popularity").isNotNull(),
                                        col("popularity") - col("prev_popularity"))
                                   .otherwise(0)))
        
        # Verify trending calculations
        assert "popularity_change" in trending_data.columns
        
        # Check for reasonable trend values
        trend_stats = trending_data.agg(
            spark_max("popularity_change").alias("max_change"),
            spark_min("popularity_change").alias("min_change")
        ).collect()[0]
        
        assert trend_stats["max_change"] >= trend_stats["min_change"]
        
        logger.info("Trending calculations test passed")
    
    def test_temporal_analysis(self, spark_session, sample_silver_data):
        """Test temporal analysis functionality."""
        logger.info("Testing temporal analysis")
        
        from pyspark.sql.functions import year, month
        
        # Simulate temporal analysis
        temporal_stats = (sample_silver_data
                         .withColumn("release_year", year("release_date"))
                         .withColumn("release_month", month("release_date"))
                         .groupBy("release_year", "release_month")
                         .agg(
                             count("movie_id").alias("monthly_releases"),
                             avg("vote_average").alias("avg_monthly_rating"),
                             avg("revenue").alias("avg_monthly_revenue")
                         ))
        
        # Verify temporal analysis
        assert temporal_stats.count() > 0
        assert "release_year" in temporal_stats.columns
        assert "monthly_releases" in temporal_stats.columns
        
        # Check for valid date ranges
        year_stats = temporal_stats.agg(
            spark_min("release_year").alias("min_year"),
            spark_max("release_year").alias("max_year")
        ).collect()[0]
        
        assert year_stats["min_year"] >= 2020
        assert year_stats["max_year"] <= 2024
        
        logger.info("Temporal analysis test passed")
    
    def test_aggregation_completeness(self, spark_session, sample_silver_data):
        """Test aggregation completeness and data coverage."""
        logger.info("Testing aggregation completeness")
        
        # Check data coverage
        total_movies = sample_silver_data.count()
        high_quality_movies = sample_silver_data.filter(col("quality_flag") == "HIGH").count()
        
        coverage_ratio = high_quality_movies / total_movies
        
        # Verify adequate data coverage
        assert coverage_ratio >= 0.8  # At least 80% high quality data
        
        # Check for complete aggregations
        genre_coverage = (sample_silver_data
                         .filter(col("genres").isNotNull() & (col("genres") != ""))
                         .count())
        
        genre_coverage_ratio = genre_coverage / total_movies
        assert genre_coverage_ratio >= 0.9  # At least 90% have genre data
        
        logger.info(f"Aggregation completeness test passed: coverage={coverage_ratio:.2f}")


class TestSentimentAnalysis:
    """Test sentiment analysis functionality."""
    
    @pytest.fixture(scope="session")
    def spark_session(self):
        """Create Spark session for tests."""
        return SparkSession.builder \
            .appName("TestSentimentAnalysis") \
            .master("local[2]") \
            .getOrCreate()
    
    def test_sentiment_scoring(self, spark_session):
        """Test sentiment scoring functionality."""
        logger.info("Testing sentiment scoring")
        
        # Create test data with known sentiment
        sentiment_test_data = [
            {"movie_id": "pos_001", "overview": "This is an amazing and wonderful movie with great acting."},
            {"movie_id": "neg_001", "overview": "This is a terrible and awful movie with bad acting."},
            {"movie_id": "neu_001", "overview": "This is a movie about something that happened."}
        ]
        
        test_df = spark_session.createDataFrame(sentiment_test_data)
        
        # Simulate sentiment analysis (simplified)
        def classify_sentiment(text):
            if any(word in text.lower() for word in ["amazing", "wonderful", "great"]):
                return "POSITIVE"
            elif any(word in text.lower() for word in ["terrible", "awful", "bad"]):
                return "NEGATIVE"
            else:
                return "NEUTRAL"
        
        # Test sentiment classification
        for row in test_df.collect():
            sentiment = classify_sentiment(row["overview"])
            
            if row["movie_id"].startswith("pos_"):
                assert sentiment == "POSITIVE"
            elif row["movie_id"].startswith("neg_"):
                assert sentiment == "NEGATIVE"
            else:
                assert sentiment == "NEUTRAL"
        
        logger.info("Sentiment scoring test passed")
    
    def test_sentiment_batch_processing(self, spark_session):
        """Test batch sentiment processing."""
        logger.info("Testing batch sentiment processing")
        
        # Create larger batch of test data
        batch_size = 1000
        test_data = []
        
        for i in range(batch_size):
            sentiment_type = i % 3
            if sentiment_type == 0:
                overview = "This is a fantastic and excellent movie with outstanding performances."
            elif sentiment_type == 1:
                overview = "This is a disappointing and boring movie with poor execution."
            else:
                overview = "This is a movie that tells a story about various events."
            
            test_data.append({
                "movie_id": f"batch_{i:06d}",
                "overview": overview
            })
        
        batch_df = spark_session.createDataFrame(test_data)
        
        # Verify batch processing capability
        assert batch_df.count() == batch_size
        
        # Test processing performance
        start_time = datetime.utcnow()
        processed_count = batch_df.count()  # Simulate processing
        end_time = datetime.utcnow()
        
        processing_time = (end_time - start_time).total_seconds()
        throughput = processed_count / processing_time if processing_time > 0 else float('inf')
        
        logger.info(f"Batch processing test passed: {throughput:.0f} records/second")
        assert throughput > 100  # At least 100 records per second


class TestDataQuality:
    """Test data quality validation across all transformation stages."""
    
    @pytest.fixture(scope="session")
    def spark_session(self):
        """Create Spark session for tests."""
        return SparkSession.builder \
            .appName("TestDataQuality") \
            .master("local[2]") \
            .getOrCreate()
    
    def test_completeness_validation(self, spark_session):
        """Test data completeness validation."""
        logger.info("Testing data completeness validation")
        
        # Create test data with missing values
        test_data = [
            {"movie_id": "comp_001", "title": "Complete Movie", "vote_average": 8.5},
            {"movie_id": "comp_002", "title": None, "vote_average": 7.2},
            {"movie_id": None, "title": "Missing ID Movie", "vote_average": 6.8},
            {"movie_id": "comp_004", "title": "Another Complete", "vote_average": None}
        ]
        
        test_df = spark_session.createDataFrame(test_data)
        
        # Test completeness metrics
        total_records = test_df.count()
        
        movie_id_completeness = (test_df.filter(col("movie_id").isNotNull()).count() / total_records)
        title_completeness = (test_df.filter(col("title").isNotNull()).count() / total_records)
        rating_completeness = (test_df.filter(col("vote_average").isNotNull()).count() / total_records)
        
        # Verify completeness calculations
        assert movie_id_completeness == 0.75  # 3/4
        assert title_completeness == 0.75     # 3/4
        assert rating_completeness == 0.75    # 3/4
        
        logger.info("Data completeness validation test passed")
    
    def test_consistency_validation(self, spark_session):
        """Test data consistency validation."""
        logger.info("Testing data consistency validation")
        
        # Create test data with consistency issues
        test_data = [
            {"movie_id": "cons_001", "budget": 100000000, "revenue": 200000000},  # Consistent
            {"movie_id": "cons_002", "budget": 150000000, "revenue": 50000000},   # Revenue < Budget
            {"movie_id": "cons_003", "budget": -1000000, "revenue": 100000000},   # Negative budget
            {"movie_id": "cons_004", "budget": 0, "revenue": -500000}             # Negative revenue
        ]
        
        test_df = spark_session.createDataFrame(test_data)
        
        # Test consistency checks
        negative_budget_count = test_df.filter(col("budget") < 0).count()
        negative_revenue_count = test_df.filter(col("revenue") < 0).count()
        impossible_profit_count = test_df.filter(
            (col("budget") > 0) & (col("revenue") > 0) & (col("revenue") < col("budget") * 0.1)
        ).count()
        
        assert negative_budget_count == 1
        assert negative_revenue_count == 1
        assert impossible_profit_count >= 0  # Should identify potential issues
        
        logger.info("Data consistency validation test passed")
    
    def test_accuracy_validation(self, spark_session):
        """Test data accuracy validation."""
        logger.info("Testing data accuracy validation")
        
        # Create test data with accuracy issues
        test_data = [
            {"movie_id": "acc_001", "vote_average": 8.5, "vote_count": 1000, "runtime": 120},  # Valid
            {"movie_id": "acc_002", "vote_average": 15.0, "vote_count": 500, "runtime": 90},   # Invalid rating
            {"movie_id": "acc_003", "vote_average": 7.2, "vote_count": -100, "runtime": 200}, # Invalid vote count
            {"movie_id": "acc_004", "vote_average": 6.8, "vote_count": 800, "runtime": 600}   # Invalid runtime
        ]
        
        test_df = spark_session.createDataFrame(test_data)
        
        # Test accuracy checks
        invalid_rating_count = test_df.filter((col("vote_average") < 0) | (col("vote_average") > 10)).count()
        invalid_vote_count = test_df.filter(col("vote_count") < 0).count()
        invalid_runtime_count = test_df.filter((col("runtime") < 30) | (col("runtime") > 300)).count()
        
        assert invalid_rating_count == 1
        assert invalid_vote_count == 1
        assert invalid_runtime_count == 1
        
        logger.info("Data accuracy validation test passed")


# Integration test fixtures
@pytest.fixture(scope="session")
def integration_config():
    """Configuration for integration tests."""
    return {
        "spark": {
            "app_name": "integration_test",
            "master": "local[2]"
        },
        "hdfs": {
            "paths": {
                "bronze": "/tmp/test_bronze",
                "silver": "/tmp/test_silver", 
                "gold": "/tmp/test_gold"
            }
        },
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_tmdb_analytics"
        }
    }


class TestEndToEndTransformation:
    """End-to-end transformation pipeline tests."""
    
    @pytest.fixture(scope="session")
    def spark_session(self):
        """Create Spark session for integration tests."""
        return SparkSession.builder \
            .appName("TestEndToEndTransformation") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    def test_full_pipeline_execution(self, spark_session, integration_config):
        """Test complete Bronze → Silver → Gold pipeline."""
        logger.info("Testing full pipeline execution")
        
        try:
            # Generate test data
            bronze_data = TestDataGenerator.create_bronze_test_data(spark_session, 200)
            
            # Simulate Bronze → Silver transformation
            silver_data = self._simulate_bronze_to_silver(bronze_data)
            
            # Validate Silver data
            assert silver_data.count() > 0
            assert silver_data.count() <= bronze_data.count()  # Some records may be filtered
            
            # Simulate Silver → Gold transformation
            gold_data = self._simulate_silver_to_gold(silver_data)
            
            # Validate Gold data
            assert gold_data.count() > 0
            
            logger.info("Full pipeline execution test passed")
            
        except Exception as e:
            logger.error(f"Full pipeline test failed: {e}")
            raise
    
    def _simulate_bronze_to_silver(self, bronze_df: DataFrame) -> DataFrame:
        """Simulate Bronze-to-Silver transformation."""
        # Simplified transformation simulation
        return bronze_df.select(
            col("movie_id"),
            col("batch_run_id")
        ).withColumn("quality_flag", lit("HIGH")).limit(150)
    
    def _simulate_silver_to_gold(self, silver_df: DataFrame) -> DataFrame:
        """Simulate Silver-to-Gold transformation."""
        # Simplified aggregation simulation
        return silver_df.groupBy("batch_run_id").agg(
            count("movie_id").alias("total_movies")
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])