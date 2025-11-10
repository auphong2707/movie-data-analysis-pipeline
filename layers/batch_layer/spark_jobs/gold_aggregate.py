"""
Gold Layer Aggregation - Business-ready analytics

Computes aggregations from Silver layer:
- Genre analytics (avg rating, revenue by genre/year)
- Trending scores with rolling windows (7d, 30d, 90d)
- Temporal analysis

Usage:
    spark-submit gold_aggregate.py --lookback-days 30
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
import pyspark.sql.functions as F

# Add utils to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_silver_path, get_gold_path

logger = get_logger(__name__)


class GoldAggregationJob:
    """
    Gold Layer aggregation job.
    
    Creates business-ready aggregations:
    1. Genre analytics (ratings, counts, revenue by genre/year)
    2. Trending scores (7d, 30d, 90d rolling windows)
    3. Temporal analysis (year-over-year trends)
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("gold_aggregate")
    
    @log_execution(logger, "gold_aggregate")
    def run(self, lookback_days: int = 30):
        """
        Run Gold layer aggregation.
        
        Args:
            lookback_days: Days of Silver data to aggregate
        """
        logger.info("Starting Gold layer aggregation",
                   extra={"context": {"lookback_days": lookback_days}})
        
        # Load Silver data
        movies_df = self._load_silver_movies(lookback_days)
        reviews_df = self._load_silver_reviews(lookback_days)
        
        if movies_df is None:
            logger.warning("No movies data found in Silver layer")
            return
        
        # Compute genre analytics
        genre_analytics_df = self._compute_genre_analytics(movies_df, reviews_df)
        if genre_analytics_df:
            self._write_to_gold(genre_analytics_df, "genre_analytics")
        
        # Compute trending scores
        trending_df = self._compute_trending_scores(movies_df)
        if trending_df:
            self._write_to_gold(trending_df, "trending_scores")
        
        # Compute temporal analysis
        temporal_df = self._compute_temporal_analysis(movies_df)
        if temporal_df:
            self._write_to_gold(temporal_df, "temporal_analysis")
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Gold layer aggregation completed successfully")
    
    def _load_silver_movies(self, lookback_days: int) -> Optional[DataFrame]:
        """Load movies from Silver layer."""
        try:
            silver_path = get_silver_path("movies", None).rstrip('/')
            logger.info(f"Loading movies from Silver: {silver_path}")
            
            df = self.spark.read.parquet(silver_path)
            
            # Filter by lookback period
            cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
            df = df.filter(F.col("release_date") >= cutoff_date.date())
            
            count = df.count()
            logger.info(f"Loaded {count} movies from Silver layer")
            self.metrics.add_metric("silver_movies_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Silver movies: {str(e)}", exc_info=True)
            return None
    
    def _load_silver_reviews(self, lookback_days: int) -> Optional[DataFrame]:
        """Load reviews from Silver layer."""
        try:
            silver_path = get_silver_path("reviews", None).rstrip('/')
            logger.info(f"Loading reviews from Silver: {silver_path}")
            
            df = self.spark.read.parquet(silver_path)
            
            # Filter by lookback period
            cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
            df = df.filter(F.col("review_date") >= cutoff_date.date())
            
            count = df.count()
            logger.info(f"Loaded {count} reviews from Silver layer")
            self.metrics.add_metric("silver_reviews_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Silver reviews: {str(e)}", exc_info=True)
            return None
    
    def _compute_genre_analytics(
        self,
        movies_df: DataFrame,
        reviews_df: Optional[DataFrame]
    ) -> Optional[DataFrame]:
        """
        Compute genre-level analytics.
        
        Metrics:
        - Total movies per genre
        - Average rating per genre
        - Total vote count
        - Average sentiment (if reviews available)
        """
        logger.info("Computing genre analytics")
        
        try:
            # Explode genres array so each movie-genre is a row
            df = movies_df.select(
                "movie_id",
                "title",
                "release_date",
                "vote_average",
                "vote_count",
                "popularity",
                F.explode("genres").alias("genre")
            )
            
            # Add year and month
            df = df.withColumn("year", F.year("release_date"))
            df = df.withColumn("month", F.month("release_date"))
            
            # Aggregate by genre, year, month
            genre_agg = df.groupBy("genre", "year", "month").agg(
                F.count("movie_id").alias("total_movies"),
                F.avg("vote_average").alias("avg_rating"),
                F.sum("vote_count").alias("total_votes"),
                F.avg("popularity").alias("avg_popularity"),
                F.max("vote_average").alias("max_rating"),
                F.min("vote_average").alias("min_rating")
            )
            
            # Add sentiment if reviews available
            if reviews_df is not None:
                # Aggregate sentiment by movie
                sentiment_by_movie = reviews_df.groupBy("movie_id").agg(
                    F.avg("sentiment_score").alias("avg_movie_sentiment")
                )
                
                # Join with movies
                df_with_sentiment = df.join(sentiment_by_movie, "movie_id", "left")
                
                # Re-aggregate with sentiment
                genre_agg_with_sentiment = df_with_sentiment.groupBy("genre", "year", "month").agg(
                    F.avg("avg_movie_sentiment").alias("avg_sentiment")
                )
                
                # Join sentiment back to genre_agg
                genre_agg = genre_agg.join(
                    genre_agg_with_sentiment,
                    ["genre", "year", "month"],
                    "left"
                )
            
            # Add computed_at timestamp
            genre_agg = genre_agg.withColumn(
                "computed_at",
                F.lit(datetime.utcnow().isoformat() + "Z")
            )
            
            # Add partition columns
            genre_agg = genre_agg.withColumn("partition_year", F.col("year"))
            genre_agg = genre_agg.withColumn("partition_month", F.col("month"))
            
            count = genre_agg.count()
            logger.info(f"Computed {count} genre analytics records")
            self.metrics.add_metric("genre_analytics_computed", count)
            
            return genre_agg
            
        except Exception as e:
            logger.error(f"Failed to compute genre analytics: {str(e)}", exc_info=True)
            return None
    
    def _compute_trending_scores(self, movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Compute trending scores using rolling windows.
        
        Windows: 7d, 30d, 90d
        Metrics: velocity (rate of popularity change), acceleration
        """
        logger.info("Computing trending scores")
        
        try:
            # Prepare data with date columns
            df = movies_df.select(
                "movie_id",
                "title",
                "release_date",
                "popularity",
                "vote_count",
                "vote_average"
            )
            
            # For trending, we need time-series data
            # Since we have snapshot data, we'll compute based on current values
            # In production, this would use historical snapshots
            
            # Compute popularity percentile as trend score
            window_all = Window.orderBy(F.col("popularity").desc())
            
            df = df.withColumn(
                "popularity_percentile",
                F.percent_rank().over(window_all)
            )
            
            # Compute velocity (change in votes as proxy)
            df = df.withColumn(
                "velocity",
                F.col("vote_count") / F.datediff(F.current_date(), F.col("release_date"))
            )
            
            # Create records for each window (7d, 30d, 90d)
            windows = [
                ("7d", 7),
                ("30d", 30),
                ("90d", 90)
            ]
            
            trending_dfs = []
            
            for window_name, days in windows:
                # Filter movies within window
                cutoff_date = datetime.utcnow().date() - timedelta(days=days)
                window_df = df.filter(F.col("release_date") >= cutoff_date)
                
                # Compute window-specific trend score
                window_spec = Window.orderBy(F.col("popularity").desc())
                window_df = window_df.withColumn(
                    "trend_score",
                    F.percent_rank().over(window_spec) * 100
                )
                
                # Add window identifier
                window_df = window_df.withColumn("window", F.lit(window_name))
                
                # Select final columns
                window_df = window_df.select(
                    "movie_id",
                    "title",
                    "window",
                    "trend_score",
                    "velocity",
                    "popularity",
                    F.lit(datetime.utcnow().isoformat() + "Z").alias("computed_at")
                )
                
                trending_dfs.append(window_df)
            
            # Union all windows
            trending_df = trending_dfs[0]
            for df in trending_dfs[1:]:
                trending_df = trending_df.union(df)
            
            # Add partition columns
            current_date = datetime.utcnow()
            trending_df = trending_df.withColumn("partition_year", F.lit(current_date.year))
            trending_df = trending_df.withColumn("partition_month", F.lit(current_date.month))
            
            count = trending_df.count()
            logger.info(f"Computed {count} trending score records")
            self.metrics.add_metric("trending_scores_computed", count)
            
            return trending_df
            
        except Exception as e:
            logger.error(f"Failed to compute trending scores: {str(e)}", exc_info=True)
            return None
    
    def _compute_temporal_analysis(self, movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Compute temporal analysis (year-over-year trends).
        """
        logger.info("Computing temporal analysis")
        
        try:
            # Add year column
            df = movies_df.withColumn("year", F.year("release_date"))
            
            # Aggregate by year
            temporal_agg = df.groupBy("year").agg(
                F.count("movie_id").alias("total_movies"),
                F.avg("vote_average").alias("avg_rating"),
                F.avg("popularity").alias("avg_popularity"),
                F.sum("vote_count").alias("total_votes")
            )
            
            # Compute year-over-year change
            window_spec = Window.orderBy("year")
            
            temporal_agg = temporal_agg.withColumn(
                "prev_year_movies",
                F.lag("total_movies", 1).over(window_spec)
            )
            
            temporal_agg = temporal_agg.withColumn(
                "yoy_change",
                F.when(
                    F.col("prev_year_movies").isNotNull(),
                    ((F.col("total_movies") - F.col("prev_year_movies")) / F.col("prev_year_movies")) * 100
                ).otherwise(None)
            )
            
            temporal_agg = temporal_agg.drop("prev_year_movies")
            
            # Add computed_at timestamp
            temporal_agg = temporal_agg.withColumn(
                "computed_at",
                F.lit(datetime.utcnow().isoformat() + "Z")
            )
            
            # Add partition columns
            temporal_agg = temporal_agg.withColumn("partition_year", F.col("year"))
            temporal_agg = temporal_agg.withColumn("partition_month", F.lit(datetime.utcnow().month))
            
            count = temporal_agg.count()
            logger.info(f"Computed {count} temporal analysis records")
            self.metrics.add_metric("temporal_analysis_computed", count)
            
            return temporal_agg
            
        except Exception as e:
            logger.error(f"Failed to compute temporal analysis: {str(e)}", exc_info=True)
            return None
    
    def _write_to_gold(self, df: DataFrame, metric_type: str):
        """Write DataFrame to Gold layer."""
        try:
            output_path = get_gold_path(metric_type, None).rstrip('/')
            
            logger.info(f"Writing {metric_type} to Gold layer: {output_path}")
            
            df.write \
                .mode("append") \
                .partitionBy("partition_year", "partition_month") \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"Successfully wrote {count} {metric_type} records to Gold layer")
            self.metrics.add_metric(f"{metric_type}_written_to_gold", count)
            
        except Exception as e:
            logger.error(f"Failed to write {metric_type} to Gold: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for Gold aggregation job."""
    parser = argparse.ArgumentParser(description="Gold Layer Aggregation")
    parser.add_argument("--lookback-days", type=int, default=30,
                       help="Days of Silver data to aggregate (default: 30)")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("gold_aggregate")
        
        # Run aggregation
        job = GoldAggregationJob(spark)
        job.run(lookback_days=args.lookback_days)
        
    except Exception as e:
        logger.error(f"Gold aggregation failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
