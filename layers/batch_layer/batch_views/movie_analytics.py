"""
Movie Analytics Batch Views Module.

This module generates comprehensive movie analytics from the Gold layer
data including ratings, revenue, genre performance, and trending metrics.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    stddev, percentile_approx, collect_list, struct, lit, when,
    desc, asc, row_number, rank, dense_rank, lag, lead,
    year, month, dayofmonth, date_format, date_sub, date_add,
    split, explode, size, array_contains, coalesce,
    round as spark_round, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MovieAnalyticsGenerator:
    """Generates comprehensive movie analytics for batch views."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize movie analytics generator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        
        # Paths
        self.gold_path = self.hdfs_config["paths"]["gold"]
        self.silver_path = self.hdfs_config["paths"]["silver"]
        
        # Analytics configuration
        self.analytics_config = config.get("analytics", {})
        self.min_rating_count = self.analytics_config.get("min_rating_count", 100)
        self.trending_window_days = self.analytics_config.get("trending_window_days", [7, 30, 90])
        self.top_n_limit = self.analytics_config.get("top_n_limit", 50)
    
    def generate_comprehensive_analytics(self, batch_id: str) -> Dict[str, str]:
        """
        Generate all movie analytics views.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Dict[str, str]: Paths to generated analytics
        """
        logger.info(f"Generating comprehensive movie analytics for batch {batch_id}")
        
        analytics_paths = {}
        
        try:
            # Load Silver layer data
            silver_df = self._load_silver_data()
            
            # Generate movie performance analytics
            performance_path = self._generate_movie_performance_analytics(silver_df, batch_id)
            if performance_path:
                analytics_paths["movie_performance"] = performance_path
            
            # Generate genre analytics
            genre_path = self._generate_genre_analytics(silver_df, batch_id)
            if genre_path:
                analytics_paths["genre_analytics"] = genre_path
            
            # Generate director analytics
            director_path = self._generate_director_analytics(silver_df, batch_id)
            if director_path:
                analytics_paths["director_analytics"] = director_path
            
            # Generate cast analytics
            cast_path = self._generate_cast_analytics(silver_df, batch_id)
            if cast_path:
                analytics_paths["cast_analytics"] = cast_path
            
            # Generate revenue analytics
            revenue_path = self._generate_revenue_analytics(silver_df, batch_id)
            if revenue_path:
                analytics_paths["revenue_analytics"] = revenue_path
            
            # Generate rating analytics
            rating_path = self._generate_rating_analytics(silver_df, batch_id)
            if rating_path:
                analytics_paths["rating_analytics"] = rating_path
            
            # Generate popularity analytics
            popularity_path = self._generate_popularity_analytics(silver_df, batch_id)
            if popularity_path:
                analytics_paths["popularity_analytics"] = popularity_path
            
            logger.info(f"Movie analytics generation completed: {list(analytics_paths.keys())}")
            return analytics_paths
            
        except Exception as e:
            logger.error(f"Error generating movie analytics: {e}")
            raise
    
    def _load_silver_data(self) -> DataFrame:
        """Load Silver layer data with necessary transformations."""
        logger.info("Loading Silver layer data for analytics")
        
        try:
            # Load Silver data
            silver_df = self.spark.read.parquet(self.silver_path)
            
            # Add derived fields for analytics
            enriched_df = (silver_df
                          .withColumn("release_year", year(col("release_date")))
                          .withColumn("release_month", month(col("release_date")))
                          .withColumn("profit", col("revenue") - col("budget"))
                          .withColumn("roi", 
                                    when(col("budget") > 0, 
                                         (col("revenue") - col("budget")) / col("budget"))
                                    .otherwise(None))
                          .withColumn("runtime_hours", col("runtime") / 60.0)
                          .withColumn("genres_list", split(col("genres"), "\\|"))
                          .withColumn("countries_list", split(col("production_countries"), "\\|"))
                          .withColumn("companies_list", split(col("production_companies"), "\\|"))
                          .withColumn("languages_list", split(col("spoken_languages"), "\\|")))
            
            # Cache for multiple analytics operations
            enriched_df.cache()
            
            logger.info(f"Loaded {enriched_df.count()} movies from Silver layer")
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error loading Silver data: {e}")
            raise
    
    def _generate_movie_performance_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate individual movie performance analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating movie performance analytics")
        
        try:
            # Calculate performance metrics for each movie
            performance_df = (silver_df
                            .filter(col("quality_flag") == "HIGH")
                            .select(
                                col("movie_id"),
                                col("title"),
                                col("release_date"),
                                col("release_year"),
                                col("genres"),
                                col("runtime"),
                                col("budget"),
                                col("revenue"),
                                col("profit"),
                                col("roi"),
                                col("vote_average").alias("imdb_rating"),
                                col("vote_count").alias("rating_count"),
                                col("popularity"),
                                col("sentiment_score"),
                                col("sentiment_label"),
                                
                                # Performance categories
                                when(col("revenue") >= 1000000000, "Blockbuster")
                                .when(col("revenue") >= 100000000, "Major Hit")
                                .when(col("revenue") >= 10000000, "Moderate Success")
                                .when(col("revenue") >= 1000000, "Limited Success")
                                .otherwise("Low Performer").alias("revenue_category"),
                                
                                when(col("vote_average") >= 8.0, "Excellent")
                                .when(col("vote_average") >= 7.0, "Good")
                                .when(col("vote_average") >= 6.0, "Average")
                                .when(col("vote_average") >= 5.0, "Below Average")
                                .otherwise("Poor").alias("rating_category"),
                                
                                when(col("roi") >= 10.0, "Extremely Profitable")
                                .when(col("roi") >= 5.0, "Highly Profitable")
                                .when(col("roi") >= 2.0, "Profitable")
                                .when(col("roi") >= 0.0, "Break Even")
                                .otherwise("Loss").alias("profitability_category")
                            ))
            
            # Add rankings within release year
            year_window = Window.partitionBy("release_year").orderBy(desc("revenue"))
            rating_window = Window.partitionBy("release_year").orderBy(desc("imdb_rating"))
            
            performance_with_ranks = (performance_df
                                    .withColumn("revenue_rank_in_year", 
                                              row_number().over(year_window))
                                    .withColumn("rating_rank_in_year", 
                                              row_number().over(rating_window))
                                    .withColumn("batch_run_id", lit(batch_id))
                                    .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/movie_performance"
            (performance_with_ranks
             .coalesce(10)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Movie performance analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating movie performance analytics: {e}")
            return None
    
    def _generate_genre_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate genre-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating genre analytics")
        
        try:
            # Explode genres for individual analysis
            genre_exploded = (silver_df
                            .filter(col("quality_flag") == "HIGH")
                            .select(
                                col("movie_id"),
                                col("title"),
                                col("release_year"),
                                col("budget"),
                                col("revenue"),
                                col("profit"),
                                col("roi"),
                                col("vote_average"),
                                col("vote_count"),
                                col("popularity"),
                                col("sentiment_score"),
                                explode(col("genres_list")).alias("genre")
                            )
                            .filter(col("genre").isNotNull() & (col("genre") != "")))
            
            # Calculate genre statistics
            genre_stats = (genre_exploded
                         .groupBy("genre", "release_year")
                         .agg(
                             count("movie_id").alias("movie_count"),
                             avg("vote_average").alias("avg_rating"),
                             avg("popularity").alias("avg_popularity"),
                             avg("sentiment_score").alias("avg_sentiment"),
                             spark_sum("revenue").alias("total_revenue"),
                             avg("revenue").alias("avg_revenue"),
                             spark_sum("budget").alias("total_budget"),
                             avg("budget").alias("avg_budget"),
                             avg("roi").alias("avg_roi"),
                             avg("profit").alias("avg_profit"),
                             spark_max("revenue").alias("max_revenue"),
                             spark_min("revenue").alias("min_revenue"),
                             stddev("vote_average").alias("rating_stddev")
                         ))
            
            # Add genre rankings
            genre_window = Window.partitionBy("release_year").orderBy(desc("total_revenue"))
            
            genre_with_ranks = (genre_stats
                              .withColumn("genre_rank_by_revenue", 
                                        row_number().over(genre_window))
                              .withColumn("batch_run_id", lit(batch_id))
                              .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Calculate top movies per genre
            top_movies_per_genre = (genre_exploded
                                  .withColumn("rank", 
                                            row_number().over(
                                                Window.partitionBy("genre", "release_year")
                                                .orderBy(desc("revenue"))))
                                  .filter(col("rank") <= 5)
                                  .groupBy("genre", "release_year")
                                  .agg(collect_list(struct(
                                      col("movie_id"),
                                      col("title"),
                                      col("revenue"),
                                      col("vote_average")
                                  )).alias("top_movies")))
            
            # Join with main genre stats
            final_genre_analytics = (genre_with_ranks
                                   .join(top_movies_per_genre, 
                                         ["genre", "release_year"], "left"))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/genre_analytics_detailed"
            (final_genre_analytics
             .coalesce(5)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Genre analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating genre analytics: {e}")
            return None
    
    def _generate_director_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate director-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating director analytics")
        
        try:
            # Extract director information
            director_movies = (silver_df
                             .filter(col("quality_flag") == "HIGH")
                             .filter(col("director").isNotNull() & (col("director") != ""))
                             .select(
                                 col("movie_id"),
                                 col("title"),
                                 col("director"),
                                 col("release_year"),
                                 col("budget"),
                                 col("revenue"),
                                 col("profit"),
                                 col("roi"),
                                 col("vote_average"),
                                 col("vote_count"),
                                 col("popularity"),
                                 col("sentiment_score"),
                                 col("genres")
                             ))
            
            # Calculate director statistics
            director_stats = (director_movies
                            .groupBy("director")
                            .agg(
                                count("movie_id").alias("total_movies"),
                                avg("vote_average").alias("avg_rating"),
                                avg("popularity").alias("avg_popularity"),
                                avg("sentiment_score").alias("avg_sentiment"),
                                spark_sum("revenue").alias("total_revenue"),
                                avg("revenue").alias("avg_revenue"),
                                spark_sum("budget").alias("total_budget"),
                                avg("budget").alias("avg_budget"),
                                avg("roi").alias("avg_roi"),
                                spark_max("revenue").alias("highest_grossing_movie_revenue"),
                                spark_max("vote_average").alias("highest_rated_movie_rating"),
                                spark_min("release_year").alias("career_start_year"),
                                spark_max("release_year").alias("most_recent_year"),
                                collect_list(col("genres")).alias("all_genres")
                            ))
            
            # Calculate career span and productivity
            director_career = (director_stats
                             .withColumn("career_span", 
                                       col("most_recent_year") - col("career_start_year") + 1)
                             .withColumn("movies_per_year",
                                       col("total_movies") / col("career_span"))
                             .withColumn("productivity_category",
                                       when(col("movies_per_year") >= 2.0, "Highly Productive")
                                       .when(col("movies_per_year") >= 1.0, "Productive")
                                       .when(col("movies_per_year") >= 0.5, "Moderate")
                                       .otherwise("Low Productivity")))
            
            # Add director rankings
            director_window = Window.orderBy(desc("total_revenue"))
            
            director_with_ranks = (director_career
                                 .withColumn("director_rank_by_revenue", 
                                           row_number().over(director_window))
                                 .filter(col("total_movies") >= 3)  # Filter for established directors
                                 .withColumn("batch_run_id", lit(batch_id))
                                 .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Get top movies for each director
            top_movies_per_director = (director_movies
                                     .withColumn("rank", 
                                               row_number().over(
                                                   Window.partitionBy("director")
                                                   .orderBy(desc("revenue"))))
                                     .filter(col("rank") <= 3)
                                     .groupBy("director")
                                     .agg(collect_list(struct(
                                         col("movie_id"),
                                         col("title"),
                                         col("revenue"),
                                         col("vote_average"),
                                         col("release_year")
                                     )).alias("top_movies")))
            
            # Join with director stats
            final_director_analytics = (director_with_ranks
                                      .join(top_movies_per_director, "director", "left"))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/director_analytics"
            (final_director_analytics
             .coalesce(3)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Director analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating director analytics: {e}")
            return None
    
    def _generate_cast_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate cast-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating cast analytics")
        
        try:
            # Extract cast information (assuming cast is stored as JSON array)
            cast_movies = (silver_df
                         .filter(col("quality_flag") == "HIGH")
                         .filter(col("cast").isNotNull())
                         .select(
                             col("movie_id"),
                             col("title"),
                             col("cast"),
                             col("release_year"),
                             col("budget"),
                             col("revenue"),
                             col("profit"),
                             col("vote_average"),
                             col("popularity"),
                             col("genres")
                         ))
            
            # For simplified analysis, assuming cast contains actor names
            # In production, would parse JSON array of cast members
            cast_stats = (cast_movies
                        .filter(col("cast").contains(","))  # Basic filter for cast data
                        .groupBy("release_year")
                        .agg(
                            count("movie_id").alias("movies_with_cast"),
                            avg("vote_average").alias("avg_rating_with_cast"),
                            avg("revenue").alias("avg_revenue_with_cast"),
                            avg("popularity").alias("avg_popularity_with_cast")
                        )
                        .withColumn("batch_run_id", lit(batch_id))
                        .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/cast_analytics"
            (cast_stats
             .coalesce(1)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Cast analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating cast analytics: {e}")
            return None
    
    def _generate_revenue_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate revenue-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating revenue analytics")
        
        try:
            # Revenue analytics by various dimensions
            revenue_by_year = (silver_df
                             .filter(col("quality_flag") == "HIGH")
                             .filter(col("revenue") > 0)
                             .groupBy("release_year")
                             .agg(
                                 count("movie_id").alias("movie_count"),
                                 spark_sum("revenue").alias("total_revenue"),
                                 avg("revenue").alias("avg_revenue"),
                                 spark_max("revenue").alias("max_revenue"),
                                 spark_min("revenue").alias("min_revenue"),
                                 percentile_approx("revenue", 0.5).alias("median_revenue"),
                                 percentile_approx("revenue", 0.75).alias("q3_revenue"),
                                 percentile_approx("revenue", 0.25).alias("q1_revenue"),
                                 stddev("revenue").alias("revenue_stddev"),
                                 spark_sum("budget").alias("total_budget"),
                                 avg("budget").alias("avg_budget"),
                                 avg("roi").alias("avg_roi")
                             ))
            
            # Add year-over-year growth
            revenue_window = Window.orderBy("release_year")
            
            revenue_with_growth = (revenue_by_year
                                 .withColumn("prev_year_revenue", 
                                           lag("total_revenue").over(revenue_window))
                                 .withColumn("yoy_growth",
                                           when(col("prev_year_revenue").isNotNull(),
                                                ((col("total_revenue") - col("prev_year_revenue")) 
                                                 / col("prev_year_revenue") * 100))
                                           .otherwise(None))
                                 .withColumn("batch_run_id", lit(batch_id))
                                 .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Revenue distribution analysis
            revenue_buckets = (silver_df
                             .filter(col("quality_flag") == "HIGH")
                             .filter(col("revenue") > 0)
                             .withColumn("revenue_bucket",
                                       when(col("revenue") >= 1000000000, "1B+")
                                       .when(col("revenue") >= 500000000, "500M-1B")
                                       .when(col("revenue") >= 100000000, "100M-500M")
                                       .when(col("revenue") >= 50000000, "50M-100M")
                                       .when(col("revenue") >= 10000000, "10M-50M")
                                       .when(col("revenue") >= 1000000, "1M-10M")
                                       .otherwise("<1M"))
                             .groupBy("revenue_bucket", "release_year")
                             .agg(
                                 count("movie_id").alias("movie_count"),
                                 avg("vote_average").alias("avg_rating"),
                                 avg("popularity").alias("avg_popularity")
                             ))
            
            # Combine revenue analytics
            final_revenue_analytics = revenue_with_growth.union(
                revenue_buckets
                .groupBy("release_year")
                .agg(
                    lit("bucket_analysis").alias("analysis_type"),
                    collect_list(struct(
                        col("revenue_bucket"),
                        col("movie_count"),
                        col("avg_rating"),
                        col("avg_popularity")
                    )).alias("bucket_data")
                )
                .withColumn("batch_run_id", lit(batch_id))
                .withColumn("computed_at", lit(datetime.utcnow()))
                .select("release_year", "analysis_type", "bucket_data", 
                       "batch_run_id", "computed_at")
            )
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/revenue_analytics"
            (revenue_with_growth
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Revenue analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating revenue analytics: {e}")
            return None
    
    def _generate_rating_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate rating-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating rating analytics")
        
        try:
            # Rating analytics with vote count thresholds
            rating_analytics = (silver_df
                              .filter(col("quality_flag") == "HIGH")
                              .filter(col("vote_count") >= self.min_rating_count)
                              .groupBy("release_year")
                              .agg(
                                  count("movie_id").alias("rated_movie_count"),
                                  avg("vote_average").alias("avg_rating"),
                                  spark_max("vote_average").alias("max_rating"),
                                  spark_min("vote_average").alias("min_rating"),
                                  percentile_approx("vote_average", 0.5).alias("median_rating"),
                                  stddev("vote_average").alias("rating_stddev"),
                                  avg("vote_count").alias("avg_vote_count"),
                                  spark_sum("vote_count").alias("total_votes"),
                                  
                                  # Rating distribution
                                  spark_sum(when(col("vote_average") >= 8.0, 1).otherwise(0)).alias("excellent_count"),
                                  spark_sum(when((col("vote_average") >= 7.0) & (col("vote_average") < 8.0), 1).otherwise(0)).alias("good_count"),
                                  spark_sum(when((col("vote_average") >= 6.0) & (col("vote_average") < 7.0), 1).otherwise(0)).alias("average_count"),
                                  spark_sum(when(col("vote_average") < 6.0, 1).otherwise(0)).alias("below_average_count")
                              ))
            
            # Calculate rating percentages
            rating_with_percentages = (rating_analytics
                                     .withColumn("excellent_pct", 
                                               spark_round(col("excellent_count") / col("rated_movie_count") * 100, 2))
                                     .withColumn("good_pct",
                                               spark_round(col("good_count") / col("rated_movie_count") * 100, 2))
                                     .withColumn("average_pct",
                                               spark_round(col("average_count") / col("rated_movie_count") * 100, 2))
                                     .withColumn("below_average_pct",
                                               spark_round(col("below_average_count") / col("rated_movie_count") * 100, 2))
                                     .withColumn("batch_run_id", lit(batch_id))
                                     .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/rating_analytics"
            (rating_with_percentages
             .coalesce(1)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Rating analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating rating analytics: {e}")
            return None
    
    def _generate_popularity_analytics(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate popularity-based analytics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analytics
        """
        logger.info("Generating popularity analytics")
        
        try:
            # Popularity trends and correlations
            popularity_analytics = (silver_df
                                  .filter(col("quality_flag") == "HIGH")
                                  .groupBy("release_year", "release_month")
                                  .agg(
                                      count("movie_id").alias("movie_count"),
                                      avg("popularity").alias("avg_popularity"),
                                      spark_max("popularity").alias("max_popularity"),
                                      spark_min("popularity").alias("min_popularity"),
                                      percentile_approx("popularity", 0.95).alias("p95_popularity"),
                                      stddev("popularity").alias("popularity_stddev"),
                                      
                                      # Correlation with other metrics
                                      avg("vote_average").alias("avg_rating"),
                                      avg("revenue").alias("avg_revenue"),
                                      avg("sentiment_score").alias("avg_sentiment")
                                  ))
            
            # Add seasonal analysis
            popularity_with_seasons = (popularity_analytics
                                     .withColumn("season",
                                               when(col("release_month").isin([12, 1, 2]), "Winter")
                                               .when(col("release_month").isin([3, 4, 5]), "Spring")
                                               .when(col("release_month").isin([6, 7, 8]), "Summer")
                                               .otherwise("Fall"))
                                     .withColumn("batch_run_id", lit(batch_id))
                                     .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/popularity_analytics"
            (popularity_with_seasons
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Popularity analytics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating popularity analytics: {e}")
            return None


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Movie Analytics Generator")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Movie_Analytics_Generator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize analytics generator
        analytics_generator = MovieAnalyticsGenerator(spark, config)
        
        # Generate analytics
        analytics_paths = analytics_generator.generate_comprehensive_analytics(args.batch_id)
        
        logger.info(f"Movie analytics generation completed: {analytics_paths}")
        
    finally:
        spark.stop()