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
        
        # Compute movie_details view (required by SCHEMA_REQUIREMENTS)
        movie_details_df = self._compute_movie_details(movies_df)
        if movie_details_df:
            self._write_to_gold(movie_details_df, "movie_details")
        
        # Compute sentiment view (required by SCHEMA_REQUIREMENTS)
        if reviews_df is not None:
            sentiment_df = self._compute_sentiment_view(reviews_df)
            if sentiment_df:
                self._write_to_gold(sentiment_df, "sentiment")
        
        # Compute genre analytics
        genre_analytics_df = self._compute_genre_analytics(movies_df, reviews_df)
        if genre_analytics_df:
            self._write_to_gold(genre_analytics_df, "genre_analytics")
        
        # Compute temporal trends (updated format)
        temporal_trends_df = self._compute_temporal_trends(movies_df, reviews_df)
        if temporal_trends_df:
            self._write_to_gold(temporal_trends_df, "temporal_trends")
        
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
    
    def _compute_movie_details(self, movies_df: DataFrame) -> Optional[DataFrame]:
        """
        Compute movie_details view per SCHEMA_REQUIREMENTS.
        
        Required fields:
        - movie_id, title, release_date, genres, vote_average, 
          vote_count, popularity, runtime, budget, revenue, overview, original_language
        """
        logger.info("Computing movie_details view")
        
        try:
            # Select and transform fields according to schema requirements
            movie_details = movies_df.select(
                F.col("movie_id"),
                F.struct(
                    F.col("title").alias("title"),
                    F.col("release_date").cast("string").alias("release_date"),
                    F.col("genres").alias("genres"),
                    F.col("vote_average").cast("double").alias("vote_average"),
                    F.col("vote_count").cast("int").alias("vote_count"),
                    F.col("popularity").cast("double").alias("popularity"),
                    F.lit(None).cast("int").alias("runtime"),  # May not have in source
                    F.lit(None).cast("bigint").alias("budget"),
                    F.lit(None).cast("bigint").alias("revenue"),
                    F.col("overview").alias("overview"),
                    F.col("original_language").alias("original_language")
                ).alias("data")
            )
            
            # Add computed_at timestamp
            movie_details = movie_details.withColumn(
                "computed_at",
                F.lit(datetime.utcnow().isoformat() + "Z")
            )
            
            # Add partition columns
            current_date = datetime.utcnow()
            movie_details = movie_details.withColumn("partition_year", F.lit(current_date.year))
            movie_details = movie_details.withColumn("partition_month", F.lit(current_date.month))
            
            count = movie_details.count()
            logger.info(f"Computed {count} movie_details records")
            self.metrics.add_metric("movie_details_computed", count)
            
            return movie_details
            
        except Exception as e:
            logger.error(f"Failed to compute movie_details: {str(e)}", exc_info=True)
            return None
    
    def _compute_sentiment_view(self, reviews_df: DataFrame) -> Optional[DataFrame]:
        """
        Compute sentiment view per SCHEMA_REQUIREMENTS.
        
        Required fields:
        - movie_id, avg_sentiment, review_count, positive_count, 
          negative_count, neutral_count
        """
        logger.info("Computing sentiment view")
        
        try:
            # Aggregate sentiment by movie
            sentiment_agg = reviews_df.groupBy("movie_id").agg(
                F.avg("sentiment_score").alias("avg_sentiment"),
                F.count("review_id").alias("review_count"),
                F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
                F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
                F.sum(F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count")
            )
            
            # Structure according to schema: wrap aggregates in 'data' field
            sentiment_view = sentiment_agg.select(
                F.col("movie_id"),
                F.struct(
                    F.col("avg_sentiment").cast("double").alias("avg_sentiment"),
                    F.col("review_count").cast("int").alias("review_count"),
                    F.col("positive_count").cast("int").alias("positive_count"),
                    F.col("negative_count").cast("int").alias("negative_count"),
                    F.col("neutral_count").cast("int").alias("neutral_count")
                ).alias("data")
            )
            
            # Add computed_at timestamp
            sentiment_view = sentiment_view.withColumn(
                "computed_at",
                F.lit(datetime.utcnow().isoformat() + "Z")
            )
            
            # Add partition columns
            current_date = datetime.utcnow()
            sentiment_view = sentiment_view.withColumn("partition_year", F.lit(current_date.year))
            sentiment_view = sentiment_view.withColumn("partition_month", F.lit(current_date.month))
            
            count = sentiment_view.count()
            logger.info(f"Computed {count} sentiment records")
            self.metrics.add_metric("sentiment_computed", count)
            
            return sentiment_view
            
        except Exception as e:
            logger.error(f"Failed to compute sentiment view: {str(e)}", exc_info=True)
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
    
    def _compute_temporal_trends(
        self, 
        movies_df: DataFrame, 
        reviews_df: Optional[DataFrame]
    ) -> Optional[DataFrame]:
        """
        Compute temporal_trends view per SCHEMA_REQUIREMENTS.
        
        Required fields in data:
        - metric (rating/sentiment/popularity)
        - value (float)
        - count (integer) 
        - date (string ISO date)
        
        Optional fields (for filtering):
        - genre (string)
        - movie_id (int)
        
        Creates 3 types of trends:
        1. Overall trends (all movies) - no genre/movie_id
        2. Genre-specific trends - with genre field
        3. Movie-specific trends - with movie_id field
        """
        logger.info("Computing temporal_trends view")
        
        try:
            all_trends_list = []
            
            # ===== 1. OVERALL TRENDS (aggregate across all movies) =====
            df = movies_df.withColumn("date", F.to_date("release_date"))
            
            # Overall rating trends by date
            rating_trends = df.groupBy("date").agg(
                F.avg("vote_average").alias("value"),
                F.count("movie_id").alias("count")
            ).withColumn("metric", F.lit("rating")) \
             .withColumn("genre", F.lit(None).cast(StringType())) \
             .withColumn("movie_id", F.lit(None).cast(IntegerType()))
            
            # Overall popularity trends by date
            popularity_trends = df.groupBy("date").agg(
                F.avg("popularity").alias("value"),
                F.count("movie_id").alias("count")
            ).withColumn("metric", F.lit("popularity")) \
             .withColumn("genre", F.lit(None).cast(StringType())) \
             .withColumn("movie_id", F.lit(None).cast(IntegerType()))
            
            all_trends_list.extend([rating_trends, popularity_trends])
            
            # Overall sentiment trends if reviews available
            if reviews_df is not None:
                sentiment_trends = reviews_df.withColumn("date", F.to_date("review_date")) \
                    .groupBy("date").agg(
                        F.avg("sentiment_score").alias("value"),
                        F.count("review_id").alias("count")
                    ).withColumn("metric", F.lit("sentiment")) \
                     .withColumn("genre", F.lit(None).cast(StringType())) \
                     .withColumn("movie_id", F.lit(None).cast(IntegerType()))
                
                all_trends_list.append(sentiment_trends)
            
            # ===== 2. GENRE-SPECIFIC TRENDS =====
            # Explode genres and compute trends per genre
            df_with_genre = movies_df.select(
                "movie_id",
                "release_date",
                "vote_average",
                "popularity",
                F.explode("genres").alias("genre")
            ).withColumn("date", F.to_date("release_date"))
            
            # Genre rating trends
            genre_rating_trends = df_with_genre.groupBy("date", "genre").agg(
                F.avg("vote_average").alias("value"),
                F.count("movie_id").alias("count")
            ).withColumn("metric", F.lit("rating")) \
             .withColumn("movie_id", F.lit(None).cast(IntegerType()))
            
            # Genre popularity trends
            genre_popularity_trends = df_with_genre.groupBy("date", "genre").agg(
                F.avg("popularity").alias("value"),
                F.count("movie_id").alias("count")
            ).withColumn("metric", F.lit("popularity")) \
             .withColumn("movie_id", F.lit(None).cast(IntegerType()))
            
            all_trends_list.extend([genre_rating_trends, genre_popularity_trends])
            
            # Genre sentiment trends if reviews available
            if reviews_df is not None:
                # Join reviews with movies to get genres
                reviews_with_genre = reviews_df.join(
                    movies_df.select("movie_id", F.explode("genres").alias("genre")),
                    "movie_id",
                    "left"
                ).withColumn("date", F.to_date("review_date"))
                
                genre_sentiment_trends = reviews_with_genre.groupBy("date", "genre").agg(
                    F.avg("sentiment_score").alias("value"),
                    F.count("review_id").alias("count")
                ).withColumn("metric", F.lit("sentiment")) \
                 .withColumn("movie_id", F.lit(None).cast(IntegerType()))
                
                all_trends_list.append(genre_sentiment_trends)
            
            # ===== 3. MOVIE-SPECIFIC TRENDS =====
            # Individual movie rating trends (time series per movie)
            movie_rating_trends = movies_df.select(
                "movie_id",
                F.to_date("release_date").alias("date"),
                F.col("vote_average").alias("value")
            ).withColumn("count", F.lit(1)) \
             .withColumn("metric", F.lit("rating")) \
             .withColumn("genre", F.lit(None).cast(StringType()))
            
            # Individual movie popularity trends
            movie_popularity_trends = movies_df.select(
                "movie_id",
                F.to_date("release_date").alias("date"),
                F.col("popularity").alias("value")
            ).withColumn("count", F.lit(1)) \
             .withColumn("metric", F.lit("popularity")) \
             .withColumn("genre", F.lit(None).cast(StringType()))
            
            all_trends_list.extend([movie_rating_trends, movie_popularity_trends])
            
            # Individual movie sentiment trends if reviews available
            if reviews_df is not None:
                movie_sentiment_trends = reviews_df.groupBy("movie_id", F.to_date("review_date").alias("date")).agg(
                    F.avg("sentiment_score").alias("value"),
                    F.count("review_id").alias("count")
                ).withColumn("metric", F.lit("sentiment")) \
                 .withColumn("genre", F.lit(None).cast(StringType()))
                
                all_trends_list.append(movie_sentiment_trends)
            
            # ===== UNION ALL TRENDS =====
            all_trends = all_trends_list[0]
            for trend_df in all_trends_list[1:]:
                all_trends = all_trends.union(trend_df)
            
            # Structure according to schema: wrap in 'data' field with optional fields
            temporal_trends = all_trends.select(
                F.struct(
                    F.col("metric").alias("metric"),
                    F.col("value").cast("double").alias("value"),
                    F.col("count").cast("int").alias("count"),
                    F.col("date").cast("string").alias("date"),
                    F.col("genre").alias("genre"),  # OPTIONAL - null for overall/movie trends
                    F.col("movie_id").alias("movie_id")  # OPTIONAL - null for overall/genre trends
                ).alias("data")
            )
            
            # Add computed_at timestamp
            temporal_trends = temporal_trends.withColumn(
                "computed_at",
                F.lit(datetime.utcnow().isoformat() + "Z")
            )
            
            # Add partition columns
            current_date = datetime.utcnow()
            temporal_trends = temporal_trends.withColumn("partition_year", F.lit(current_date.year))
            temporal_trends = temporal_trends.withColumn("partition_month", F.lit(current_date.month))
            
            count = temporal_trends.count()
            logger.info(f"Computed {count} temporal_trends records (overall + genre + movie)")
            self.metrics.add_metric("temporal_trends_computed", count)
            
            return temporal_trends
            
        except Exception as e:
            logger.error(f"Failed to compute temporal_trends: {str(e)}", exc_info=True)
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
