"""
Silver Layer - Baseline Calculation

Calculates historical baselines from TMDB data for comparison with Reddit real-time data.
Computes genre-level sentiment baselines, vote thresholds, and popularity metrics.

Usage:
    spark-submit silver_transform.py
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


class BaselineCalculationJob:
    """
    Baseline Calculation Job for Silver Layer.
    
    Calculates historical baselines from TMDB data:
    1. Load TMDB movies and reviews from Bronze
    2. Join movies with genres
    3. Calculate sentiment baselines per genre
    4. Calculate viral thresholds (75th percentile vote_count)
    5. Write baselines to Silver layer
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("baseline_calculation")
        
        # Register UDFs
        self.spark.udf.register("sentiment_score", sentiment_score_udf, DoubleType())
        self.spark.udf.register("sentiment_label", sentiment_label_udf, StringType())
    
    @log_execution(logger, "baseline_calculation")
    def run(self):
        """
        Run baseline calculation.
        
        Computes:
        - avg_sentiment per genre (from TMDB reviews)
        - sentiment_stddev per genre
        - viral_threshold per genre (75th percentile vote_count)
        """
        logger.info("Starting baseline calculation from TMDB data")
        
        # Load Bronze data
        movies_df = self._load_bronze_movies()
        reviews_df = self._load_bronze_reviews()
        genres_df = self._load_bronze_genres()
        
        if movies_df is None or genres_df is None:
            logger.error("Cannot calculate baselines without movies and genres")
            return
        
        # Calculate baselines
        baselines_df = self._calculate_genre_baselines(movies_df, reviews_df, genres_df)
        
        if baselines_df:
            self._write_baselines_to_silver(baselines_df)
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Baseline calculation completed successfully")
    
    def _load_bronze_movies(self) -> Optional[DataFrame]:
        """Load movies from Bronze layer."""
        try:
            bronze_path = get_bronze_path("tmdb_movies", None).rstrip('/')
            logger.info(f"Loading movies from Bronze: {bronze_path}")
            
            df = self.spark.read.parquet(bronze_path)
            count = df.count()
            
            logger.info(f"Loaded {count} movies from Bronze")
            self.metrics.add_metric("bronze_movies_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Bronze movies: {str(e)}", exc_info=True)
            return None
    
    def _load_bronze_reviews(self) -> Optional[DataFrame]:
        """Load reviews from Bronze layer."""
        try:
            bronze_path = get_bronze_path("tmdb_reviews", None).rstrip('/')
            logger.info(f"Loading reviews from Bronze: {bronze_path}")
            
            df = self.spark.read.parquet(bronze_path)
            count = df.count()
            
            logger.info(f"Loaded {count} reviews from Bronze")
            self.metrics.add_metric("bronze_reviews_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.warning(f"No reviews found in Bronze (optional for baselines): {str(e)}")
            return None
    
    def _load_bronze_genres(self) -> Optional[DataFrame]:
        """Load genres from Bronze layer."""
        try:
            bronze_path = get_bronze_path("tmdb_genres", None).rstrip('/')
            logger.info(f"Loading genres from Bronze: {bronze_path}")
            
            df = self.spark.read.parquet(bronze_path)
            count = df.count()
            
            logger.info(f"Loaded {count} genres from Bronze")
            self.metrics.add_metric("bronze_genres_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Bronze genres: {str(e)}", exc_info=True)
            return None
    
    def _calculate_genre_baselines(
        self,
        movies_df: DataFrame,
        reviews_df: Optional[DataFrame],
        genres_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        Calculate baseline metrics per genre.
        
        Computes:
        - avg_sentiment: Average sentiment score from TMDB reviews
        - sentiment_stddev: Standard deviation of sentiment
        - viral_threshold: 75th percentile of vote_count (upvote proxy)
        """
        logger.info("Calculating genre baselines")
        
        try:
            # Explode genres array to one row per (movie, genre)
            movies_exploded = movies_df.select(
                F.col("id").alias("movie_id"),
                F.col("title"),
                F.col("vote_average"),
                F.col("vote_count"),
                F.col("popularity"),
                F.explode(F.col("genre_ids")).alias("genre_id")
            )
            
            # Join with genre names
            movies_with_genres = movies_exploded.join(
                genres_df.select(
                    F.col("id").alias("genre_id"),
                    F.col("name").alias("genre_name")
                ),
                on="genre_id",
                how="inner"
            )
            
            # Calculate vote_count-based viral threshold by genre
            genre_stats = movies_with_genres.groupBy("genre_name").agg(
                F.expr("percentile_approx(vote_count, 0.75)").alias("viral_threshold"),
                F.avg("vote_average").alias("avg_rating"),
                F.avg("popularity").alias("avg_popularity"),
                F.count("*").alias("movie_count")
            )
            
            # If reviews exist, calculate sentiment baselines
            if reviews_df is not None:
                logger.info("Calculating sentiment baselines from TMDB reviews")
                
                # Apply sentiment analysis to reviews
                reviews_with_sentiment = reviews_df.select(
                    F.col("movie_id"),
                    F.col("content"),
                    F.expr("sentiment_score(content)").alias("sentiment_score")
                )
                
                # Join reviews with movie genres
                reviews_with_genres = reviews_with_sentiment.join(
                    movies_with_genres.select("movie_id", "genre_name").distinct(),
                    on="movie_id",
                    how="inner"
                )
                
                # Calculate sentiment stats by genre
                sentiment_stats = reviews_with_genres.groupBy("genre_name").agg(
                    F.avg("sentiment_score").alias("avg_sentiment"),
                    F.stddev("sentiment_score").alias("sentiment_stddev"),
                    F.count("*").alias("review_count")
                )
                
                # Join genre stats with sentiment stats
                baselines = genre_stats.join(
                    sentiment_stats,
                    on="genre_name",
                    how="left"
                )
            else:
                logger.warning("No reviews available - using rating as sentiment proxy")
                
                # Use vote_average as sentiment proxy (normalized to -1 to 1 scale)
                # TMDB: 0-10 scale, transform to -1 to 1: (vote_average - 5) / 5
                movies_with_sentiment_proxy = movies_with_genres.withColumn(
                    "sentiment_proxy",
                    (F.col("vote_average") - 5) / 5
                )
                
                sentiment_proxy_stats = movies_with_sentiment_proxy.groupBy("genre_name").agg(
                    F.avg("sentiment_proxy").alias("avg_sentiment"),
                    F.stddev("sentiment_proxy").alias("sentiment_stddev"),
                    F.lit(0).alias("review_count")
                )
                
                baselines = genre_stats.join(
                    sentiment_proxy_stats,
                    on="genre_name",
                    how="left"
                )
            
            # Add metadata
            baselines = baselines.withColumn("type", F.lit("baseline"))
            baselines = baselines.withColumn("updated_at", F.current_timestamp())
            baselines = baselines.withColumn("source", F.lit("tmdb_batch"))
            
            # Rename genre_name to genre for consistency
            baselines = baselines.withColumnRenamed("genre_name", "genre")
            
            # Fill nulls
            baselines = baselines.fillna({
                "avg_sentiment": 0.0,
                "sentiment_stddev": 0.1,
                "review_count": 0
            })
            
            count = baselines.count()
            logger.info(f"Calculated baselines for {count} genres")
            self.metrics.add_metric("baselines_calculated", count)
            
            # Log sample
            logger.info("Sample baselines:")
            baselines.show(5, truncate=False)
            
            return baselines
            
        except Exception as e:
            logger.error(f"Failed to calculate baselines: {str(e)}", exc_info=True)
            return None
    
    def _write_baselines_to_silver(self, baselines_df: DataFrame):
        """Write baselines to Silver layer."""
        try:
            output_path = get_silver_path("baselines", None).rstrip('/')
            
            logger.info(f"Writing baselines to Silver layer: {output_path}")
            
            baselines_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            count = baselines_df.count()
            logger.info(f"Successfully wrote {count} genre baselines to Silver layer")
            self.metrics.add_metric("baselines_written", count)
            
        except Exception as e:
            logger.error(f"Failed to write baselines to Silver: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for baseline calculation job."""
    parser = argparse.ArgumentParser(description="Silver Layer Baseline Calculation")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("baseline_calculation")
        
        # Run baseline calculation
        job = BaselineCalculationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Baseline calculation failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
