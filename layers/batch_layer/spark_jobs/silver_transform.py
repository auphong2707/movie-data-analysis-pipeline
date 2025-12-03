"""
Silver Layer - Multi-Goal Baseline Calculation

Generates three optimized datasets for business goals:
1. Sentiment Baselines: Genre/franchise/director/temporal sentiment patterns
2. Viral Thresholds: Genre/budget-tier/seasonal viral cutoffs  
3. Movie Intelligence: Individual movie data for competitive analysis

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
    Multi-Goal Baseline Calculation Job for Silver Layer.
    
    Generates three optimized datasets:
    1. Sentiment Baselines: Genre/franchise/director/temporal patterns
    2. Viral Thresholds: Genre/budget-tier/seasonal cutoffs
    3. Movie Intelligence: Individual movie data for competitive analysis
    
    Processes:
    1. Load TMDB movies, reviews, genres from Bronze
    2. Enrich with franchise, director, budget tier classification
    3. Calculate three separate datasets optimized per business goal
    4. Write to three separate Parquet files in Silver layer
    """
    
    # Budget tier thresholds (in USD)
    BUDGET_TIERS = {
        'indie': (0, 20_000_000),
        'mid': (20_000_000, 100_000_000),
        'blockbuster': (100_000_000, float('inf'))
    }
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("multi_goal_baseline_calculation")
        
        # Register UDFs
        self.spark.udf.register("sentiment_score", sentiment_score_udf, DoubleType())
        self.spark.udf.register("sentiment_label", sentiment_label_udf, StringType())
    
    @log_execution(logger, "multi_goal_baseline_calculation")
    def run(self):
        """
        Run multi-goal baseline calculation.
        
        Generates three datasets:
        1. sentiment_baselines: Genre/franchise/director/temporal sentiment patterns
        2. viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
        3. movie_intelligence: Individual movie competitive data
        """
        logger.info("Starting multi-goal baseline calculation from TMDB data")
        
        # Load Bronze data
        movies_df = self._load_bronze_movies()
        reviews_df = self._load_bronze_reviews()
        genres_df = self._load_bronze_genres()
        
        if movies_df is None or genres_df is None:
            logger.error("Cannot calculate baselines without movies and genres")
            return
        
        # Enrich movies with derived fields
        enriched_movies_df = self._enrich_movies(movies_df, reviews_df, genres_df)
        
        if enriched_movies_df is None:
            logger.error("Movie enrichment failed")
            return
        
        # Generate three datasets
        sentiment_baselines_df = self._generate_sentiment_baselines(enriched_movies_df)
        viral_thresholds_df = self._generate_viral_thresholds(enriched_movies_df)
        movie_intelligence_df = self._generate_movie_intelligence(enriched_movies_df)
        
        # Write to Silver layer
        if sentiment_baselines_df:
            self._write_to_silver(sentiment_baselines_df, "sentiment_baselines")
        if viral_thresholds_df:
            self._write_to_silver(viral_thresholds_df, "viral_thresholds")
        if movie_intelligence_df:
            self._write_to_silver(movie_intelligence_df, "movie_intelligence")
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Multi-goal baseline calculation completed successfully")
    
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
    
    def _enrich_movies(
        self,
        movies_df: DataFrame,
        reviews_df: Optional[DataFrame],
        genres_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        Enrich movies with derived fields for business goals.
        
        Adds:
        - genre_names: Array of genre names (not just IDs)
        - primary_genre: First genre for categorization
        - franchise: From belongs_to_collection
        - director: From credits (if available)
        - budget_tier: indie/mid/blockbuster classification
        - release_month, release_year: Temporal fields
        - avg_sentiment: Movie-level sentiment from reviews
        """
        logger.info("Enriching movies with derived fields")
        
        try:
            # Join with genres to get genre names
            genre_map = genres_df.select(
                F.col("id").alias("genre_id"),
                F.col("name").alias("genre_name")
            )
            
            # Create genre_names array from genre_ids
            # Note: This requires exploding and collecting, which is expensive
            # For simplicity, we'll use the first genre as primary
            enriched = movies_df.withColumn(
                "primary_genre_id",
                F.when(F.size(F.col("genre_ids")) > 0, F.col("genre_ids")[0])
                .otherwise(F.lit(None))
            )
            
            enriched = enriched.join(
                genre_map.select(
                    F.col("genre_id").alias("primary_genre_id"),
                    F.col("genre_name").alias("primary_genre")
                ),
                on="primary_genre_id",
                how="left"
            )
            
            # Extract franchise from belongs_to_collection (if available)
            # Note: This field may not exist in basic TMDB movie data
            if "belongs_to_collection" in enriched.columns:
                enriched = enriched.withColumn(
                    "franchise",
                    F.when(F.col("belongs_to_collection").isNotNull(),
                           F.col("belongs_to_collection.name"))
                    .otherwise(F.lit(None))
                )
            else:
                logger.warning("belongs_to_collection field not available - franchise will be null")
                enriched = enriched.withColumn("franchise", F.lit(None).cast(StringType()))
            
            # Classify budget tier (handle missing budget field)
            if "budget" in enriched.columns:
                enriched = enriched.withColumn(
                    "budget_tier",
                    F.when(F.col("budget") >= self.BUDGET_TIERS['blockbuster'][0], "blockbuster")
                    .when(F.col("budget") >= self.BUDGET_TIERS['mid'][0], "mid")
                    .when(F.col("budget") > 0, "indie")
                    .otherwise(F.lit("unknown"))
                )
            else:
                logger.warning("budget field not available - budget_tier will be unknown")
                enriched = enriched.withColumn("budget_tier", F.lit("unknown"))
                enriched = enriched.withColumn("budget", F.lit(0))
            
            # Extract temporal fields
            enriched = enriched.withColumn(
                "release_year",
                F.year(F.col("release_date"))
            ).withColumn(
                "release_month",
                F.month(F.col("release_date"))
            ).withColumn(
                "release_month_name",
                F.date_format(F.col("release_date"), "MMMM")
            )
            
            # Map month to season
            enriched = enriched.withColumn(
                "season",
                F.when(F.col("release_month").isin([12, 1, 2]), "winter")
                .when(F.col("release_month").isin([3, 4, 5]), "spring")
                .when(F.col("release_month").isin([6, 7, 8]), "summer")
                .when(F.col("release_month").isin([9, 10, 11]), "fall")
                .otherwise(F.lit("unknown"))
            )
            
            # Extract director from credits if available
            # Note: TMDB API includes crew in separate endpoint - not in basic movie data
            if "director" in enriched.columns:
                # Field exists, use it
                pass
            else:
                logger.warning("director field not available - will be null (requires detailed movie API call)")
                enriched = enriched.withColumn("director", F.lit(None).cast(StringType()))
            
            # Calculate movie-level sentiment from reviews
            if reviews_df is not None:
                logger.info("Calculating movie-level sentiment from reviews")
                
                reviews_with_sentiment = reviews_df.select(
                    F.col("movie_id"),
                    F.expr("sentiment_score(content)").alias("sentiment_score")
                )
                
                movie_sentiments = reviews_with_sentiment.groupBy("movie_id").agg(
                    F.avg("sentiment_score").alias("avg_sentiment"),
                    F.count("*").alias("review_count")
                )
                
                enriched = enriched.join(
                    movie_sentiments,
                    enriched.id == movie_sentiments.movie_id,
                    how="left"
                ).drop("movie_id")
            else:
                # Use vote_average as sentiment proxy
                logger.warning("No reviews - using rating as sentiment proxy")
                enriched = enriched.withColumn(
                    "avg_sentiment",
                    (F.col("vote_average") - 5) / 5  # Normalize to -1 to 1
                ).withColumn(
                    "review_count",
                    F.lit(0)
                )
            
            # Fill nulls
            enriched = enriched.fillna({
                "avg_sentiment": 0.0,
                "review_count": 0,
                "budget": 0,
                "popularity": 0.0
            })
            
            count = enriched.count()
            logger.info(f"Enriched {count} movies with derived fields")
            self.metrics.add_metric("movies_enriched", count)
            
            return enriched
            
        except Exception as e:
            logger.error(f"Failed to enrich movies: {str(e)}", exc_info=True)
            return None
    
    def _generate_sentiment_baselines(self, enriched_movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Generate sentiment baselines for Business Goal #1: PR Crisis Detection.
        
        Produces genre/franchise/director/temporal sentiment patterns.
        """
        logger.info("Generating sentiment baselines for Goal #1")
        
        try:
            # Genre-level baselines
            genre_baselines = enriched_movies_df.groupBy("primary_genre").agg(
                F.avg("avg_sentiment").alias("avg_sentiment"),
                F.stddev("avg_sentiment").alias("sentiment_stddev"),
                F.count("*").alias("movie_count"),
                F.sum("review_count").alias("review_count")
            ).select(
                F.col("primary_genre"),
                F.col("avg_sentiment"),
                F.col("sentiment_stddev"),
                F.col("movie_count"),
                F.col("review_count"),
                F.lit(None).cast(StringType()).alias("franchise"),
                F.lit(None).cast(DoubleType()).alias("franchise_avg_sentiment"),
                F.lit(None).cast(StringType()).alias("director"),
                F.lit(None).cast(DoubleType()).alias("director_avg_sentiment"),
                F.lit(None).cast(IntegerType()).alias("year"),
                F.lit(None).cast(DoubleType()).alias("yearly_sentiment")
            )
            
            # Franchise-level baselines
            franchise_baselines = enriched_movies_df.filter(
                F.col("franchise").isNotNull()
            ).groupBy("primary_genre", "franchise").agg(
                F.avg("avg_sentiment").alias("franchise_avg_sentiment"),
                F.stddev("avg_sentiment").alias("sentiment_stddev"),
                F.count("*").alias("movie_count"),
                F.sum("review_count").alias("review_count")
            ).select(
                F.col("primary_genre"),
                F.col("franchise_avg_sentiment").alias("avg_sentiment"),
                F.col("sentiment_stddev"),
                F.col("movie_count"),
                F.col("review_count"),
                F.col("franchise"),
                F.col("franchise_avg_sentiment"),
                F.lit(None).cast(StringType()).alias("director"),
                F.lit(None).cast(DoubleType()).alias("director_avg_sentiment"),
                F.lit(None).cast(IntegerType()).alias("year"),
                F.lit(None).cast(DoubleType()).alias("yearly_sentiment")
            )
            
            # Year-level baselines
            yearly_baselines = enriched_movies_df.filter(
                F.col("release_year").isNotNull()
            ).groupBy("primary_genre", "release_year").agg(
                F.avg("avg_sentiment").alias("yearly_sentiment"),
                F.stddev("avg_sentiment").alias("sentiment_stddev"),
                F.count("*").alias("movie_count"),
                F.sum("review_count").alias("review_count")
            ).select(
                F.col("primary_genre"),
                F.col("yearly_sentiment").alias("avg_sentiment"),
                F.col("sentiment_stddev"),
                F.col("movie_count"),
                F.col("review_count"),
                F.lit(None).cast(StringType()).alias("franchise"),
                F.lit(None).cast(DoubleType()).alias("franchise_avg_sentiment"),
                F.lit(None).cast(StringType()).alias("director"),
                F.lit(None).cast(DoubleType()).alias("director_avg_sentiment"),
                F.col("release_year").alias("year"),
                F.col("yearly_sentiment")
            )
            
            # Union all baselines
            sentiment_baselines = genre_baselines.union(franchise_baselines).union(yearly_baselines)
            
            # Add metadata
            sentiment_baselines = sentiment_baselines \
                .withColumn("type", F.lit("sentiment_baseline")) \
                .withColumn("updated_at", F.current_timestamp()) \
                .withColumnRenamed("primary_genre", "genre") \
                .fillna({
                    "avg_sentiment": 0.0,
                    "sentiment_stddev": 0.1,
                    "review_count": 0
                })
            
            count = sentiment_baselines.count()
            logger.info(f"Generated {count} sentiment baseline records")
            self.metrics.add_metric("sentiment_baselines_generated", count)
            
            return sentiment_baselines
            
        except Exception as e:
            logger.error(f"Failed to generate sentiment baselines: {str(e)}", exc_info=True)
            return None
    
    def _generate_viral_thresholds(self, enriched_movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Generate viral thresholds for Business Goal #2: Viral Content Identification.
        
        Produces genre/budget-tier/seasonal viral cutoffs.
        """
        logger.info("Generating viral thresholds for Goal #2")
        
        try:
            # Genre-level thresholds (99th percentile for viral, not 75th)
            genre_thresholds = enriched_movies_df.groupBy("primary_genre").agg(
                F.expr("percentile_approx(vote_count, 0.99)").alias("viral_threshold"),
                F.avg("popularity").alias("avg_popularity"),
                F.count("*").alias("movie_count")
            ).select(
                F.col("primary_genre"),
                F.col("viral_threshold"),
                F.col("avg_popularity"),
                F.col("movie_count"),
                F.lit(None).cast(StringType()).alias("budget_tier"),
                F.lit(None).cast(IntegerType()).alias("budget_tier_threshold"),
                F.lit(None).cast(DoubleType()).alias("budget_tier_coefficient"),
                F.lit(None).cast(StringType()).alias("season"),
                F.lit(None).cast(IntegerType()).alias("seasonal_threshold")
            )
            
            # Budget tier thresholds
            budget_thresholds = enriched_movies_df.filter(
                F.col("budget_tier") != "unknown"
            ).groupBy("primary_genre", "budget_tier").agg(
                F.expr("percentile_approx(vote_count, 0.99)").alias("budget_tier_threshold"),
                F.expr("percentile_approx(vote_count, 0.99)").alias("viral_threshold"),
                F.avg("popularity").alias("avg_popularity"),
                F.count("*").alias("movie_count")
            ).select(
                F.col("primary_genre"),
                F.col("viral_threshold"),
                F.col("avg_popularity"),
                F.col("movie_count"),
                F.col("budget_tier"),
                F.col("budget_tier_threshold"),
                F.lit(2.5).alias("budget_tier_coefficient"),  # Hardcoded breakout multiplier
                F.lit(None).cast(StringType()).alias("season"),
                F.lit(None).cast(IntegerType()).alias("seasonal_threshold")
            )
            
            # Seasonal thresholds
            seasonal_thresholds = enriched_movies_df.filter(
                F.col("season") != "unknown"
            ).groupBy("primary_genre", "season").agg(
                F.expr("percentile_approx(vote_count, 0.99)").alias("seasonal_threshold"),
                F.expr("percentile_approx(vote_count, 0.99)").alias("viral_threshold"),
                F.avg("popularity").alias("avg_popularity"),
                F.count("*").alias("movie_count")
            ).select(
                F.col("primary_genre"),
                F.col("viral_threshold"),
                F.col("avg_popularity"),
                F.col("movie_count"),
                F.lit(None).cast(StringType()).alias("budget_tier"),
                F.lit(None).cast(IntegerType()).alias("budget_tier_threshold"),
                F.lit(None).cast(DoubleType()).alias("budget_tier_coefficient"),
                F.col("season"),
                F.col("seasonal_threshold")
            )
            
            # Union all thresholds
            viral_thresholds = genre_thresholds.union(budget_thresholds).union(seasonal_thresholds)
            
            # Add metadata
            viral_thresholds = viral_thresholds \
                .withColumn("type", F.lit("viral_threshold")) \
                .withColumn("updated_at", F.current_timestamp()) \
                .withColumn("viral_case_study", F.lit(None).cast(StringType())) \
                .withColumnRenamed("primary_genre", "genre")
            
            count = viral_thresholds.count()
            logger.info(f"Generated {count} viral threshold records")
            self.metrics.add_metric("viral_thresholds_generated", count)
            
            return viral_thresholds
            
        except Exception as e:
            logger.error(f"Failed to generate viral thresholds: {str(e)}", exc_info=True)
            return None
    
    def _generate_movie_intelligence(self, enriched_movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Generate movie intelligence for Business Goal #3: Competitive Intelligence.
        
        Produces individual movie records with competitive context.
        """
        logger.info("Generating movie intelligence for Goal #3")
        
        try:
            # Select relevant fields for movie intelligence
            # Handle potentially missing fields
            select_fields = [
                F.col("id").alias("movie_id"),
                F.col("title"),
                F.array(F.col("primary_genre")).alias("genre"),  # Array for consistency
                F.col("release_date"),
                F.col("release_month_name").alias("release_month"),
                F.col("release_year"),
                F.col("avg_sentiment"),
                F.col("vote_average"),
                F.col("vote_count"),
                F.col("popularity"),
                F.col("budget"),
                F.col("budget_tier"),
                F.col("franchise"),
                F.col("director")
            ]
            
            # Add runtime if available
            if "runtime" in enriched_movies_df.columns:
                select_fields.append(F.col("runtime"))
            else:
                select_fields.append(F.lit(None).cast(IntegerType()).alias("runtime"))
            
            select_fields.append(F.col("review_count"))
            
            movie_intelligence = enriched_movies_df.select(*select_fields)
            
            # Add competitive context placeholder
            # TODO: Calculate same-month releases, genre rank, year rank
            movie_intelligence = movie_intelligence \
                .withColumn("competitive_context", F.lit(None).cast(StringType()))
            
            # Add metadata
            movie_intelligence = movie_intelligence \
                .withColumn("type", F.lit("movie_intelligence")) \
                .withColumn("updated_at", F.current_timestamp())
            
            count = movie_intelligence.count()
            logger.info(f"Generated {count} movie intelligence records")
            self.metrics.add_metric("movie_intelligence_generated", count)
            
            return movie_intelligence
            
        except Exception as e:
            logger.error(f"Failed to generate movie intelligence: {str(e)}", exc_info=True)
            return None
    
    def _write_to_silver(self, df: DataFrame, dataset_name: str):
        """Write dataset to Silver layer."""
        try:
            output_path = get_silver_path(dataset_name, None).rstrip('/')
            
            logger.info(f"Writing {dataset_name} to Silver layer: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"Successfully wrote {count} records to {dataset_name}")
            self.metrics.add_metric(f"{dataset_name}_written", count)
            
        except Exception as e:
            logger.error(f"Failed to write {dataset_name} to Silver: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for multi-goal baseline calculation job."""
    parser = argparse.ArgumentParser(description="Silver Layer Multi-Goal Baseline Calculation")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("multi_goal_baseline_calculation")
        
        # Run baseline calculation
        job = BaselineCalculationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Multi-goal baseline calculation failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
