"""
Silver Layer Transformation - Clean, Deduplicate, and Enrich

Reads Bronze layer Parquet, performs cleaning, deduplication, enrichment,
and sentiment analysis, then writes to Silver layer.

Usage:
    spark-submit silver_transform.py --lookback-hours 4
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, ArrayType, DateType
)
import pyspark.sql.functions as F

# Add utils to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_bronze_path, get_silver_path

logger = get_logger(__name__)


class SentimentAnalyzer:
    """
    Simple sentiment analyzer using VADER-like approach.
    
    For production, consider using:
    - VADER (vaderSentiment library)
    - Transformer models (BERT, RoBERTa)
    - Cloud APIs (AWS Comprehend, Google NLP)
    """
    
    # Simple sentiment word lists
    POSITIVE_WORDS = {
        'excellent', 'amazing', 'great', 'wonderful', 'fantastic', 'perfect',
        'love', 'loved', 'best', 'brilliant', 'outstanding', 'superb',
        'masterpiece', 'incredible', 'awesome', 'beautiful', 'good'
    }
    
    NEGATIVE_WORDS = {
        'terrible', 'awful', 'bad', 'worst', 'horrible', 'poor',
        'boring', 'waste', 'disappointing', 'disappointed', 'dull',
        'weak', 'fail', 'failed', 'hate', 'hated', 'trash'
    }
    
    @classmethod
    def analyze_text(cls, text: str) -> Dict[str, float]:
        """
        Analyze sentiment of text.
        
        Returns:
            Dictionary with score (-1 to 1) and label
        """
        if not text:
            return {'score': 0.0, 'label': 'neutral'}
        
        text_lower = text.lower()
        words = text_lower.split()
        
        positive_count = sum(1 for word in words if word in cls.POSITIVE_WORDS)
        negative_count = sum(1 for word in words if word in cls.NEGATIVE_WORDS)
        
        total_sentiment_words = positive_count + negative_count
        
        if total_sentiment_words == 0:
            score = 0.0
            label = 'neutral'
        else:
            score = (positive_count - negative_count) / len(words)
            # Normalize to -1 to 1 range
            score = max(-1.0, min(1.0, score * 10))
            
            if score > 0.2:
                label = 'positive'
            elif score < -0.2:
                label = 'negative'
            else:
                label = 'neutral'
        
        return {'score': score, 'label': label}


# Register sentiment analysis as UDF
def sentiment_score_udf(text):
    """UDF for sentiment score."""
    result = SentimentAnalyzer.analyze_text(text)
    return result['score']

def sentiment_label_udf(text):
    """UDF for sentiment label."""
    result = SentimentAnalyzer.analyze_text(text)
    return result['label']


class SilverTransformationJob:
    """
    Silver Layer transformation job.
    
    Processes:
    1. Read Bronze layer data
    2. Deduplicate by movie_id
    3. Clean and validate data
    4. Enrich with genre names, cast info
    5. Compute sentiment scores
    6. Write to Silver layer
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("silver_transform")
        
        # Register UDFs
        self.spark.udf.register("sentiment_score", sentiment_score_udf, DoubleType())
        self.spark.udf.register("sentiment_label", sentiment_label_udf, StringType())
    
    @log_execution(logger, "silver_transform")
    def run(self, lookback_hours: int = 4):
        """
        Run Silver layer transformation.
        
        Args:
            lookback_hours: Hours of Bronze data to process
        """
        logger.info("Starting Silver layer transformation",
                   extra={"context": {"lookback_hours": lookback_hours}})
        
        # Process movies
        movies_df = self._process_movies(lookback_hours)
        if movies_df:
            self._write_to_silver(movies_df, "movies")
        
        # Process reviews with sentiment
        reviews_df = self._process_reviews(lookback_hours)
        if reviews_df:
            self._write_to_silver(reviews_df, "reviews")
        
        # Process credits
        credits_df = self._process_credits(lookback_hours)
        if credits_df:
            self._write_to_silver(credits_df, "credits")
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Silver layer transformation completed successfully")
    
    def _process_movies(self, lookback_hours: int) -> Optional[DataFrame]:
        """
        Process movies from Bronze to Silver.
        
        Steps:
        1. Read Bronze movies
        2. Deduplicate by movie_id
        3. Extract and clean fields
        4. Enrich with genre names
        5. Validate data quality
        """
        logger.info("Processing movies from Bronze")
        
        try:
            # Read Bronze movies (last N hours)
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=lookback_hours)
            
            bronze_path = get_bronze_path("movies", None).rstrip('/')
            
            df = self.spark.read.parquet(bronze_path)
            
            # Filter by time window
            df = df.filter(
                (F.col("extraction_timestamp") >= start_time.isoformat()) &
                (F.col("extraction_timestamp") <= end_time.isoformat())
            )
            
            bronze_count = df.count()
            logger.info(f"Read {bronze_count} movies from Bronze")
            self.metrics.add_metric("bronze_movies_read", bronze_count)
            
            if bronze_count == 0:
                logger.warning("No movies found in Bronze layer")
                return None
            
            # Deduplicate by movie_id (keep latest)
            window_spec = Window.partitionBy("id").orderBy(F.col("extraction_timestamp").desc())
            df = df.withColumn("row_num", F.row_number().over(window_spec))
            df = df.filter(F.col("row_num") == 1).drop("row_num")
            
            deduplicated_count = df.count()
            logger.info(f"After deduplication: {deduplicated_count} unique movies")
            self.metrics.add_metric("movies_deduplicated", deduplicated_count)
            
            # Extract and clean fields
            df = df.select(
                F.col("id").alias("movie_id"),
                F.col("title"),
                F.col("original_title"),
                F.col("overview"),
                F.to_date(F.col("release_date")).alias("release_date"),
                F.col("adult").cast("boolean"),
                F.col("popularity").cast("double"),
                F.col("vote_average").cast("double"),
                F.col("vote_count").cast("int"),
                F.col("poster_path"),
                F.col("backdrop_path"),
                F.col("original_language"),
                F.col("genre_ids"),  # Will convert to genre names
                F.col("extraction_timestamp").cast("timestamp").alias("processed_timestamp")
            )
            
            # Convert genre IDs to genre names
            genre_map = {
                28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
                80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
                14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
                9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
                10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
            }
            
            # Create genre names array
            def map_genres(genre_ids):
                if not genre_ids:
                    return []
                return [genre_map.get(gid, "Unknown") for gid in genre_ids]
            
            map_genres_udf = F.udf(map_genres, ArrayType(StringType()))
            df = df.withColumn("genres", map_genres_udf(F.col("genre_ids")))
            df = df.drop("genre_ids")
            
            # Add data quality flag
            df = df.withColumn(
                "quality_flag",
                F.when(
                    (F.col("title").isNull()) | (F.col("release_date").isNull()),
                    "WARNING"
                ).otherwise("OK")
            )
            
            # Add partition columns
            df = df.withColumn("partition_year", F.year(F.col("release_date")))
            df = df.withColumn("partition_month", F.month(F.col("release_date")))
            
            # For movies without release_date, use extraction time
            df = df.withColumn(
                "partition_year",
                F.when(F.col("partition_year").isNull(), 
                       F.year(F.col("processed_timestamp"))).otherwise(F.col("partition_year"))
            )
            df = df.withColumn(
                "partition_month",
                F.when(F.col("partition_month").isNull(), 
                       F.month(F.col("processed_timestamp"))).otherwise(F.col("partition_month"))
            )
            
            # Add primary genre for partitioning (first genre in list)
            df = df.withColumn(
                "partition_genre",
                F.when(F.size(F.col("genres")) > 0, F.col("genres")[0]).otherwise("Unknown")
            )
            
            final_count = df.count()
            logger.info(f"Processed {final_count} movies for Silver layer")
            self.metrics.add_metric("movies_processed", final_count)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to process movies: {str(e)}", exc_info=True)
            return None
    
    def _process_reviews(self, lookback_hours: int) -> Optional[DataFrame]:
        """
        Process reviews from Bronze to Silver with sentiment analysis.
        """
        logger.info("Processing reviews from Bronze")
        
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=lookback_hours)
            
            bronze_path = get_bronze_path("reviews", None).rstrip('/')
            
            df = self.spark.read.parquet(bronze_path)
            
            # Filter by time window
            df = df.filter(
                (F.col("extraction_timestamp") >= start_time.isoformat()) &
                (F.col("extraction_timestamp") <= end_time.isoformat())
            )
            
            bronze_count = df.count()
            logger.info(f"Read {bronze_count} reviews from Bronze")
            self.metrics.add_metric("bronze_reviews_read", bronze_count)
            
            if bronze_count == 0:
                logger.warning("No reviews found in Bronze layer")
                return None
            
            # Deduplicate by review ID
            window_spec = Window.partitionBy("id").orderBy(F.col("extraction_timestamp").desc())
            df = df.withColumn("row_num", F.row_number().over(window_spec))
            df = df.filter(F.col("row_num") == 1).drop("row_num")
            
            # Extract fields and compute sentiment
            df = df.select(
                F.col("id").alias("review_id"),
                F.col("movie_id"),
                F.col("author"),
                F.col("content"),
                F.to_date(F.col("created_at")).alias("review_date"),
                F.col("extraction_timestamp").cast("timestamp").alias("processed_timestamp")
            )
            
            # Compute sentiment scores
            df = df.withColumn("sentiment_score", 
                             F.expr("sentiment_score(content)"))
            df = df.withColumn("sentiment_label", 
                             F.expr("sentiment_label(content)"))
            
            # Add partition columns
            df = df.withColumn("partition_year", F.year(F.col("review_date")))
            df = df.withColumn("partition_month", F.month(F.col("review_date")))
            
            # Handle nulls
            df = df.withColumn(
                "partition_year",
                F.when(F.col("partition_year").isNull(), 
                       F.year(F.col("processed_timestamp"))).otherwise(F.col("partition_year"))
            )
            df = df.withColumn(
                "partition_month",
                F.when(F.col("partition_month").isNull(), 
                       F.month(F.col("processed_timestamp"))).otherwise(F.col("partition_month"))
            )
            
            final_count = df.count()
            logger.info(f"Processed {final_count} reviews for Silver layer")
            self.metrics.add_metric("reviews_processed", final_count)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to process reviews: {str(e)}", exc_info=True)
            return None
    
    def _process_credits(self, lookback_hours: int) -> Optional[DataFrame]:
        """Process credits from Bronze to Silver."""
        logger.info("Processing credits from Bronze")
        
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=lookback_hours)
            
            bronze_path = get_bronze_path("credits", None).rstrip('/')
            
            df = self.spark.read.parquet(bronze_path)
            
            # Filter by time window
            df = df.filter(
                (F.col("extraction_timestamp") >= start_time.isoformat()) &
                (F.col("extraction_timestamp") <= end_time.isoformat())
            )
            
            bronze_count = df.count()
            logger.info(f"Read {bronze_count} credits from Bronze")
            self.metrics.add_metric("bronze_credits_read", bronze_count)
            
            if bronze_count == 0:
                logger.warning("No credits found in Bronze layer")
                return None
            
            # Deduplicate by movie_id
            window_spec = Window.partitionBy("movie_id").orderBy(F.col("extraction_timestamp").desc())
            df = df.withColumn("row_num", F.row_number().over(window_spec))
            df = df.filter(F.col("row_num") == 1).drop("row_num")
            
            # Extract fields
            df = df.select(
                F.col("movie_id"),
                F.col("cast"),
                F.col("crew"),
                F.col("extraction_timestamp").cast("timestamp").alias("processed_timestamp")
            )
            
            # Add partition columns (use extraction time)
            df = df.withColumn("partition_year", F.year(F.col("processed_timestamp")))
            df = df.withColumn("partition_month", F.month(F.col("processed_timestamp")))
            
            final_count = df.count()
            logger.info(f"Processed {final_count} credits for Silver layer")
            self.metrics.add_metric("credits_processed", final_count)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to process credits: {str(e)}", exc_info=True)
            return None
    
    def _write_to_silver(self, df: DataFrame, data_type: str):
        """Write DataFrame to Silver layer."""
        try:
            output_path = get_silver_path(data_type, None).rstrip('/')
            
            logger.info(f"Writing {data_type} to Silver layer: {output_path}")
            
            # Partition by year, month, and genre (if applicable)
            if data_type == "movies":
                partition_cols = ["partition_year", "partition_month", "partition_genre"]
            else:
                partition_cols = ["partition_year", "partition_month"]
            
            df.write \
                .mode("append") \
                .partitionBy(*partition_cols) \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"Successfully wrote {count} {data_type} to Silver layer")
            self.metrics.add_metric(f"{data_type}_written_to_silver", count)
            
        except Exception as e:
            logger.error(f"Failed to write {data_type} to Silver: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for Silver transformation job."""
    parser = argparse.ArgumentParser(description="Silver Layer Transformation")
    parser.add_argument("--lookback-hours", type=int, default=4,
                       help="Hours of Bronze data to process (default: 4)")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("silver_transform")
        
        # Run transformation
        job = SilverTransformationJob(spark)
        job.run(lookback_hours=args.lookback_hours)
        
    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
