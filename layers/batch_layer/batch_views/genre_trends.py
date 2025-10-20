"""
Genre Trends Analysis Module.

This module analyzes genre trends over time, identifying emerging genres,
declining categories, and seasonal patterns in movie production and performance.
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
    round as spark_round, monotonically_increasing_id, regexp_replace
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import yaml
from math import sqrt

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GenreTrendsAnalyzer:
    """Analyzes genre trends and patterns over time."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize genre trends analyzer.
        
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
        
        # Trend analysis configuration
        self.trends_config = config.get("trends", {})
        self.min_movies_per_genre = self.trends_config.get("min_movies_per_genre", 10)
        self.trend_window_years = self.trends_config.get("trend_window_years", 5)
        self.significance_threshold = self.trends_config.get("significance_threshold", 0.1)
    
    def analyze_all_genre_trends(self, batch_id: str) -> Dict[str, str]:
        """
        Analyze all genre trends and patterns.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Dict[str, str]: Paths to generated trend analyses
        """
        logger.info(f"Analyzing genre trends for batch {batch_id}")
        
        trends_paths = {}
        
        try:
            # Load Silver layer data with genre explosion
            genre_df = self._prepare_genre_data()
            
            # Analyze genre popularity trends
            popularity_path = self._analyze_genre_popularity_trends(genre_df, batch_id)
            if popularity_path:
                trends_paths["genre_popularity_trends"] = popularity_path
            
            # Analyze genre performance trends
            performance_path = self._analyze_genre_performance_trends(genre_df, batch_id)
            if performance_path:
                trends_paths["genre_performance_trends"] = performance_path
            
            # Analyze seasonal patterns
            seasonal_path = self._analyze_seasonal_patterns(genre_df, batch_id)
            if seasonal_path:
                trends_paths["seasonal_patterns"] = seasonal_path
            
            # Analyze emerging and declining genres
            emergence_path = self._analyze_genre_emergence_decline(genre_df, batch_id)
            if emergence_path:
                trends_paths["genre_emergence"] = emergence_path
            
            # Analyze genre combinations
            combinations_path = self._analyze_genre_combinations(genre_df, batch_id)
            if combinations_path:
                trends_paths["genre_combinations"] = combinations_path
            
            # Analyze market share evolution
            market_share_path = self._analyze_market_share_evolution(genre_df, batch_id)
            if market_share_path:
                trends_paths["market_share_evolution"] = market_share_path
            
            logger.info(f"Genre trends analysis completed: {list(trends_paths.keys())}")
            return trends_paths
            
        except Exception as e:
            logger.error(f"Error analyzing genre trends: {e}")
            raise
    
    def _prepare_genre_data(self) -> DataFrame:
        """Prepare genre data by exploding genre lists."""
        logger.info("Preparing genre data for trend analysis")
        
        try:
            # Load Silver data
            silver_df = self.spark.read.parquet(self.silver_path)
            
            # Explode genres and enrich with derived fields
            genre_df = (silver_df
                       .filter(col("quality_flag") == "HIGH")
                       .filter(col("genres").isNotNull())
                       .withColumn("release_year", year(col("release_date")))
                       .withColumn("release_month", month(col("release_date")))
                       .withColumn("release_quarter", 
                                 when(col("release_month").isin([1,2,3]), 1)
                                 .when(col("release_month").isin([4,5,6]), 2)
                                 .when(col("release_month").isin([7,8,9]), 3)
                                 .otherwise(4))
                       .withColumn("genres_list", split(col("genres"), "\\|"))
                       .select(
                           col("movie_id"),
                           col("title"),
                           col("release_date"),
                           col("release_year"),
                           col("release_month"),
                           col("release_quarter"),
                           col("budget"),
                           col("revenue"),
                           col("vote_average"),
                           col("vote_count"),
                           col("popularity"),
                           col("sentiment_score"),
                           col("runtime"),
                           explode(col("genres_list")).alias("genre")
                       )
                       .filter(col("genre").isNotNull() & (col("genre") != ""))
                       .withColumn("genre", regexp_replace(col("genre"), "^\\s+|\\s+$", ""))  # Trim whitespace
                       .withColumn("profit", col("revenue") - col("budget"))
                       .withColumn("roi", 
                                 when(col("budget") > 0, 
                                      (col("revenue") - col("budget")) / col("budget"))
                                 .otherwise(None)))
            
            # Cache for multiple operations
            genre_df.cache()
            
            logger.info(f"Prepared genre data with {genre_df.count()} genre-movie combinations")
            return genre_df
            
        except Exception as e:
            logger.error(f"Error preparing genre data: {e}")
            raise
    
    def _analyze_genre_popularity_trends(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze genre popularity trends over time.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing genre popularity trends")
        
        try:
            # Calculate yearly genre statistics
            yearly_genre_stats = (genre_df
                                .groupBy("genre", "release_year")
                                .agg(
                                    count("movie_id").alias("movie_count"),
                                    avg("popularity").alias("avg_popularity"),
                                    avg("vote_average").alias("avg_rating"),
                                    avg("vote_count").alias("avg_vote_count"),
                                    spark_sum("revenue").alias("total_revenue"),
                                    avg("revenue").alias("avg_revenue"),
                                    avg("sentiment_score").alias("avg_sentiment")
                                )
                                .filter(col("movie_count") >= self.min_movies_per_genre))
            
            # Calculate trends using linear regression approximation
            genre_window = Window.partitionBy("genre").orderBy("release_year")
            
            trends_df = (yearly_genre_stats
                        .withColumn("prev_movie_count", lag("movie_count").over(genre_window))
                        .withColumn("prev_avg_popularity", lag("avg_popularity").over(genre_window))
                        .withColumn("prev_avg_revenue", lag("avg_revenue").over(genre_window))
                        
                        # Calculate year-over-year changes
                        .withColumn("movie_count_yoy_change",
                                   when(col("prev_movie_count").isNotNull(),
                                        (col("movie_count") - col("prev_movie_count")) / col("prev_movie_count"))
                                   .otherwise(None))
                        .withColumn("popularity_yoy_change",
                                   when(col("prev_avg_popularity").isNotNull(),
                                        (col("avg_popularity") - col("prev_avg_popularity")) / col("prev_avg_popularity"))
                                   .otherwise(None))
                        .withColumn("revenue_yoy_change",
                                   when(col("prev_avg_revenue").isNotNull() & (col("prev_avg_revenue") > 0),
                                        (col("avg_revenue") - col("prev_avg_revenue")) / col("prev_avg_revenue"))
                                   .otherwise(None)))
            
            # Calculate overall trend metrics per genre
            genre_trends = (trends_df
                           .groupBy("genre")
                           .agg(
                               count("release_year").alias("years_active"),
                               spark_min("release_year").alias("first_year"),
                               spark_max("release_year").alias("last_year"),
                               avg("movie_count").alias("avg_yearly_movies"),
                               avg("avg_popularity").alias("overall_avg_popularity"),
                               avg("avg_rating").alias("overall_avg_rating"),
                               spark_sum("total_revenue").alias("total_genre_revenue"),
                               avg("movie_count_yoy_change").alias("avg_movie_count_growth"),
                               avg("popularity_yoy_change").alias("avg_popularity_growth"),
                               avg("revenue_yoy_change").alias("avg_revenue_growth"),
                               stddev("movie_count").alias("movie_count_volatility"),
                               stddev("avg_popularity").alias("popularity_volatility")
                           ))
            
            # Classify trend patterns
            trend_classified = (genre_trends
                              .withColumn("trend_category",
                                        when(col("avg_movie_count_growth") > 0.1, "Growing")
                                        .when(col("avg_movie_count_growth") < -0.1, "Declining")
                                        .otherwise("Stable"))
                              .withColumn("popularity_trend",
                                        when(col("avg_popularity_growth") > 0.05, "Rising Popularity")
                                        .when(col("avg_popularity_growth") < -0.05, "Declining Popularity")
                                        .otherwise("Stable Popularity"))
                              .withColumn("revenue_trend",
                                        when(col("avg_revenue_growth") > 0.1, "Revenue Growth")
                                        .when(col("avg_revenue_growth") < -0.1, "Revenue Decline")
                                        .otherwise("Revenue Stable"))
                              .withColumn("volatility_category",
                                        when(col("movie_count_volatility") > col("avg_yearly_movies") * 0.5, "High Volatility")
                                        .when(col("movie_count_volatility") > col("avg_yearly_movies") * 0.25, "Medium Volatility")
                                        .otherwise("Low Volatility"))
                              .withColumn("batch_run_id", lit(batch_id))
                              .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/genre_popularity_trends"
            (trend_classified
             .coalesce(3)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Genre popularity trends saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing genre popularity trends: {e}")
            return None
    
    def _analyze_genre_performance_trends(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze genre performance trends including ROI, ratings, and commercial success.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing genre performance trends")
        
        try:
            # Calculate performance metrics by genre and year
            performance_df = (genre_df
                            .filter(col("revenue") > 0)
                            .filter(col("budget") > 0)
                            .groupBy("genre", "release_year")
                            .agg(
                                count("movie_id").alias("movie_count"),
                                avg("roi").alias("avg_roi"),
                                avg("vote_average").alias("avg_rating"),
                                avg("sentiment_score").alias("avg_sentiment"),
                                percentile_approx("roi", 0.5).alias("median_roi"),
                                percentile_approx("vote_average", 0.5).alias("median_rating"),
                                spark_max("roi").alias("max_roi"),
                                spark_max("vote_average").alias("max_rating"),
                                
                                # Success rate metrics
                                spark_sum(when(col("roi") > 1.0, 1).otherwise(0)).alias("profitable_movies"),
                                spark_sum(when(col("vote_average") >= 7.0, 1).otherwise(0)).alias("well_rated_movies"),
                                spark_sum(when(col("revenue") >= 100000000, 1).otherwise(0)).alias("blockbuster_movies")
                            )
                            .withColumn("profitability_rate", col("profitable_movies") / col("movie_count"))
                            .withColumn("quality_rate", col("well_rated_movies") / col("movie_count"))
                            .withColumn("blockbuster_rate", col("blockbuster_movies") / col("movie_count")))
            
            # Calculate performance trends
            perf_window = Window.partitionBy("genre").orderBy("release_year")
            
            performance_trends = (performance_df
                                .withColumn("prev_avg_roi", lag("avg_roi").over(perf_window))
                                .withColumn("prev_avg_rating", lag("avg_rating").over(perf_window))
                                .withColumn("prev_profitability_rate", lag("profitability_rate").over(perf_window))
                                
                                # Calculate performance changes
                                .withColumn("roi_trend",
                                           when(col("prev_avg_roi").isNotNull(),
                                                (col("avg_roi") - col("prev_avg_roi")) / col("prev_avg_roi"))
                                           .otherwise(None))
                                .withColumn("rating_trend",
                                           when(col("prev_avg_rating").isNotNull(),
                                                (col("avg_rating") - col("prev_avg_rating")) / col("prev_avg_rating"))
                                           .otherwise(None))
                                .withColumn("profitability_trend",
                                           when(col("prev_profitability_rate").isNotNull(),
                                                col("profitability_rate") - col("prev_profitability_rate"))
                                           .otherwise(None)))
            
            # Aggregate trend statistics per genre
            genre_performance_summary = (performance_trends
                                       .groupBy("genre")
                                       .agg(
                                           count("release_year").alias("years_tracked"),
                                           avg("avg_roi").alias("overall_avg_roi"),
                                           avg("avg_rating").alias("overall_avg_rating"),
                                           avg("avg_sentiment").alias("overall_avg_sentiment"),
                                           avg("profitability_rate").alias("overall_profitability_rate"),
                                           avg("quality_rate").alias("overall_quality_rate"),
                                           avg("blockbuster_rate").alias("overall_blockbuster_rate"),
                                           
                                           # Trend indicators
                                           avg("roi_trend").alias("avg_roi_trend"),
                                           avg("rating_trend").alias("avg_rating_trend"),
                                           avg("profitability_trend").alias("avg_profitability_trend"),
                                           
                                           # Volatility measures
                                           stddev("avg_roi").alias("roi_volatility"),
                                           stddev("avg_rating").alias("rating_volatility"),
                                           stddev("profitability_rate").alias("profitability_volatility")
                                       ))
            
            # Classify performance categories
            performance_classified = (genre_performance_summary
                                    .withColumn("roi_category",
                                              when(col("overall_avg_roi") >= 3.0, "Highly Profitable")
                                              .when(col("overall_avg_roi") >= 1.5, "Profitable")
                                              .when(col("overall_avg_roi") >= 1.0, "Marginally Profitable")
                                              .otherwise("Unprofitable"))
                                    .withColumn("quality_category",
                                              when(col("overall_avg_rating") >= 7.0, "High Quality")
                                              .when(col("overall_avg_rating") >= 6.5, "Good Quality")
                                              .when(col("overall_avg_rating") >= 6.0, "Average Quality")
                                              .otherwise("Low Quality"))
                                    .withColumn("performance_trend_direction",
                                              when((col("avg_roi_trend") > 0.05) & (col("avg_rating_trend") > 0.02), "Improving")
                                              .when((col("avg_roi_trend") < -0.05) | (col("avg_rating_trend") < -0.02), "Declining")
                                              .otherwise("Stable"))
                                    .withColumn("risk_category",
                                              when(col("roi_volatility") > 2.0, "High Risk")
                                              .when(col("roi_volatility") > 1.0, "Medium Risk")
                                              .otherwise("Low Risk"))
                                    .withColumn("batch_run_id", lit(batch_id))
                                    .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/genre_performance_trends"
            (performance_classified
             .coalesce(3)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Genre performance trends saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing genre performance trends: {e}")
            return None
    
    def _analyze_seasonal_patterns(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze seasonal release patterns for genres.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing seasonal patterns")
        
        try:
            # Seasonal analysis by month and quarter
            seasonal_stats = (genre_df
                            .groupBy("genre", "release_month", "release_quarter")
                            .agg(
                                count("movie_id").alias("movie_count"),
                                avg("revenue").alias("avg_revenue"),
                                avg("vote_average").alias("avg_rating"),
                                avg("popularity").alias("avg_popularity"),
                                spark_sum(when(col("revenue") >= 100000000, 1).otherwise(0)).alias("blockbusters")
                            ))
            
            # Calculate seasonal preferences by genre
            seasonal_preferences = (seasonal_stats
                                  .groupBy("genre")
                                  .agg(
                                      # Quarter preferences
                                      spark_sum(when(col("release_quarter") == 1, col("movie_count")).otherwise(0)).alias("q1_movies"),
                                      spark_sum(when(col("release_quarter") == 2, col("movie_count")).otherwise(0)).alias("q2_movies"),
                                      spark_sum(when(col("release_quarter") == 3, col("movie_count")).otherwise(0)).alias("q3_movies"),
                                      spark_sum(when(col("release_quarter") == 4, col("movie_count")).otherwise(0)).alias("q4_movies"),
                                      
                                      # Revenue by quarter
                                      avg(when(col("release_quarter") == 1, col("avg_revenue"))).alias("q1_avg_revenue"),
                                      avg(when(col("release_quarter") == 2, col("avg_revenue"))).alias("q2_avg_revenue"),
                                      avg(when(col("release_quarter") == 3, col("avg_revenue"))).alias("q3_avg_revenue"),
                                      avg(when(col("release_quarter") == 4, col("avg_revenue"))).alias("q4_avg_revenue"),
                                      
                                      # Best performing season
                                      spark_sum(col("movie_count")).alias("total_movies"),
                                      avg("avg_revenue").alias("overall_avg_revenue")
                                  ))
            
            # Calculate seasonal indices and identify patterns
            seasonal_analyzed = (seasonal_preferences
                               .withColumn("q1_index", col("q1_movies") / col("total_movies") * 4)  # Normalize to 1.0 = average
                               .withColumn("q2_index", col("q2_movies") / col("total_movies") * 4)
                               .withColumn("q3_index", col("q3_movies") / col("total_movies") * 4)
                               .withColumn("q4_index", col("q4_movies") / col("total_movies") * 4)
                               
                               # Identify preferred seasons
                               .withColumn("preferred_quarter",
                                         when((col("q1_index") >= col("q2_index")) & 
                                              (col("q1_index") >= col("q3_index")) & 
                                              (col("q1_index") >= col("q4_index")), "Q1 (Winter/Spring)")
                                         .when((col("q2_index") >= col("q3_index")) & 
                                               (col("q2_index") >= col("q4_index")), "Q2 (Spring/Summer)")
                                         .when(col("q3_index") >= col("q4_index"), "Q3 (Summer/Fall)")
                                         .otherwise("Q4 (Fall/Winter)"))
                               
                               # Calculate seasonality strength
                               .withColumn("seasonality_strength",
                                         spark_max(struct(col("q1_index"), col("q2_index"), 
                                                        col("q3_index"), col("q4_index"))).getItem(0) - 
                                         spark_min(struct(col("q1_index"), col("q2_index"), 
                                                        col("q3_index"), col("q4_index"))).getItem(0))
                               
                               .withColumn("seasonality_category",
                                         when(col("seasonality_strength") > 0.5, "Highly Seasonal")
                                         .when(col("seasonality_strength") > 0.25, "Moderately Seasonal")
                                         .otherwise("Not Seasonal"))
                               
                               .withColumn("batch_run_id", lit(batch_id))
                               .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/seasonal_patterns"
            (seasonal_analyzed
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Seasonal patterns saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing seasonal patterns: {e}")
            return None
    
    def _analyze_genre_emergence_decline(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Identify emerging and declining genres based on production trends.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing genre emergence and decline")
        
        try:
            # Get current year and define time windows
            current_year = datetime.now().year
            recent_years = list(range(current_year - 3, current_year + 1))
            earlier_years = list(range(current_year - 8, current_year - 3))
            
            # Calculate genre statistics for different time periods
            recent_stats = (genre_df
                          .filter(col("release_year").isin(recent_years))
                          .groupBy("genre")
                          .agg(
                              count("movie_id").alias("recent_movie_count"),
                              avg("vote_average").alias("recent_avg_rating"),
                              avg("revenue").alias("recent_avg_revenue"),
                              avg("popularity").alias("recent_avg_popularity")
                          ))
            
            earlier_stats = (genre_df
                           .filter(col("release_year").isin(earlier_years))
                           .groupBy("genre")
                           .agg(
                               count("movie_id").alias("earlier_movie_count"),
                               avg("vote_average").alias("earlier_avg_rating"),
                               avg("revenue").alias("earlier_avg_revenue"),
                               avg("popularity").alias("earlier_avg_popularity")
                           ))
            
            # Join and calculate changes
            emergence_analysis = (recent_stats
                                .join(earlier_stats, "genre", "full_outer")
                                .fillna(0, ["recent_movie_count", "earlier_movie_count"])
                                .fillna(0, ["recent_avg_rating", "earlier_avg_rating"])
                                .fillna(0, ["recent_avg_revenue", "earlier_avg_revenue"])
                                .fillna(0, ["recent_avg_popularity", "earlier_avg_popularity"])
                                
                                # Calculate growth rates
                                .withColumn("movie_count_growth_rate",
                                           when(col("earlier_movie_count") > 0,
                                                (col("recent_movie_count") - col("earlier_movie_count")) / col("earlier_movie_count"))
                                           .otherwise(
                                               when(col("recent_movie_count") > 0, 1.0).otherwise(0.0)))
                                
                                .withColumn("rating_change",
                                           col("recent_avg_rating") - col("earlier_avg_rating"))
                                
                                .withColumn("revenue_growth_rate",
                                           when(col("earlier_avg_revenue") > 0,
                                                (col("recent_avg_revenue") - col("earlier_avg_revenue")) / col("earlier_avg_revenue"))
                                           .otherwise(0.0))
                                
                                .withColumn("popularity_growth_rate",
                                           when(col("earlier_avg_popularity") > 0,
                                                (col("recent_avg_popularity") - col("earlier_avg_popularity")) / col("earlier_avg_popularity"))
                                           .otherwise(0.0)))
            
            # Classify genres based on trends
            classified_genres = (emergence_analysis
                               .withColumn("trend_classification",
                                         # Emerging genres
                                         when((col("movie_count_growth_rate") > 0.5) & 
                                              (col("recent_movie_count") >= 5), "Emerging")
                                         
                                         # Growing genres
                                         .when((col("movie_count_growth_rate") > 0.2) & 
                                               (col("recent_movie_count") >= 10), "Growing")
                                         
                                         # Declining genres
                                         .when((col("movie_count_growth_rate") < -0.3) & 
                                               (col("earlier_movie_count") >= 10), "Declining")
                                         
                                         # Reviving genres
                                         .when((col("movie_count_growth_rate") > 0.3) & 
                                               (col("earlier_movie_count") >= 5) &
                                               (col("recent_movie_count") >= 5), "Reviving")
                                         
                                         .otherwise("Stable"))
                               
                               .withColumn("performance_trend",
                                         when((col("rating_change") > 0.2) & (col("revenue_growth_rate") > 0.2), "Improving Quality & Commercial")
                                         .when(col("rating_change") > 0.2, "Improving Quality")
                                         .when(col("revenue_growth_rate") > 0.2, "Improving Commercial")
                                         .when((col("rating_change") < -0.2) | (col("revenue_growth_rate") < -0.2), "Declining Performance")
                                         .otherwise("Stable Performance"))
                               
                               .withColumn("risk_assessment",
                                         when(col("trend_classification") == "Declining", "High Risk")
                                         .when(col("trend_classification") == "Emerging", "High Opportunity")
                                         .when(col("trend_classification") == "Growing", "Medium Opportunity")
                                         .when(col("trend_classification") == "Reviving", "Medium Opportunity")
                                         .otherwise("Low Risk/Opportunity"))
                               
                               .withColumn("batch_run_id", lit(batch_id))
                               .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/genre_emergence_decline"
            (classified_genres
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Genre emergence/decline analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing genre emergence/decline: {e}")
            return None
    
    def _analyze_genre_combinations(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze popular genre combinations and their success patterns.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing genre combinations")
        
        try:
            # Get original data with multiple genres per movie
            multi_genre_df = (self.spark.read.parquet(self.silver_path)
                            .filter(col("quality_flag") == "HIGH")
                            .filter(col("genres").contains("|"))  # Movies with multiple genres
                            .withColumn("release_year", year(col("release_date")))
                            .withColumn("genres_list", split(col("genres"), "\\|"))
                            .withColumn("genre_count", size(col("genres_list")))
                            .select(
                                col("movie_id"),
                                col("title"),
                                col("release_year"),
                                col("genres"),
                                col("genres_list"),
                                col("genre_count"),
                                col("revenue"),
                                col("vote_average"),
                                col("popularity"),
                                col("sentiment_score")
                            ))
            
            # Analyze genre combination patterns
            combination_stats = (multi_genre_df
                               .groupBy("genres", "genre_count")
                               .agg(
                                   count("movie_id").alias("movie_count"),
                                   avg("revenue").alias("avg_revenue"),
                                   avg("vote_average").alias("avg_rating"),
                                   avg("popularity").alias("avg_popularity"),
                                   avg("sentiment_score").alias("avg_sentiment"),
                                   spark_max("revenue").alias("max_revenue"),
                                   spark_sum(when(col("revenue") >= 100000000, 1).otherwise(0)).alias("blockbuster_count")
                               )
                               .filter(col("movie_count") >= 3)  # Filter for meaningful combinations
                               .withColumn("blockbuster_rate", col("blockbuster_count") / col("movie_count"))
                               .withColumn("success_score", 
                                         (col("avg_rating") / 10.0 * 0.3) +
                                         (col("blockbuster_rate") * 0.4) +
                                         (col("avg_sentiment") * 0.3)))
            
            # Rank combinations by different metrics
            combination_ranked = (combination_stats
                                .withColumn("revenue_rank", 
                                          row_number().over(Window.orderBy(desc("avg_revenue"))))
                                .withColumn("rating_rank",
                                          row_number().over(Window.orderBy(desc("avg_rating"))))
                                .withColumn("success_rank",
                                          row_number().over(Window.orderBy(desc("success_score"))))
                                .withColumn("popularity_rank",
                                          row_number().over(Window.orderBy(desc("avg_popularity"))))
                                .withColumn("batch_run_id", lit(batch_id))
                                .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/genre_combinations"
            (combination_ranked
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Genre combinations analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing genre combinations: {e}")
            return None
    
    def _analyze_market_share_evolution(self, genre_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze how genre market share has evolved over time.
        
        Args:
            genre_df: Genre-exploded DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing market share evolution")
        
        try:
            # Calculate yearly totals for market share calculation
            yearly_totals = (genre_df
                           .groupBy("release_year")
                           .agg(
                               count("movie_id").alias("total_movies"),
                               spark_sum("revenue").alias("total_revenue")
                           ))
            
            # Calculate genre market shares by year
            genre_yearly = (genre_df
                          .groupBy("genre", "release_year")
                          .agg(
                              count("movie_id").alias("genre_movies"),
                              spark_sum("revenue").alias("genre_revenue")
                          ))
            
            market_shares = (genre_yearly
                           .join(yearly_totals, "release_year")
                           .withColumn("movie_share_pct", 
                                     col("genre_movies") / col("total_movies") * 100)
                           .withColumn("revenue_share_pct",
                                     col("genre_revenue") / col("total_revenue") * 100))
            
            # Calculate market share trends
            share_window = Window.partitionBy("genre").orderBy("release_year")
            
            share_trends = (market_shares
                          .withColumn("prev_movie_share", lag("movie_share_pct").over(share_window))
                          .withColumn("prev_revenue_share", lag("revenue_share_pct").over(share_window))
                          .withColumn("movie_share_change",
                                    col("movie_share_pct") - col("prev_movie_share"))
                          .withColumn("revenue_share_change",
                                    col("revenue_share_pct") - col("prev_revenue_share")))
            
            # Summarize market evolution per genre
            market_evolution = (share_trends
                              .groupBy("genre")
                              .agg(
                                  count("release_year").alias("years_tracked"),
                                  avg("movie_share_pct").alias("avg_movie_share"),
                                  avg("revenue_share_pct").alias("avg_revenue_share"),
                                  spark_max("movie_share_pct").alias("peak_movie_share"),
                                  spark_max("revenue_share_pct").alias("peak_revenue_share"),
                                  avg("movie_share_change").alias("avg_movie_share_change"),
                                  avg("revenue_share_change").alias("avg_revenue_share_change"),
                                  stddev("movie_share_pct").alias("movie_share_volatility"),
                                  stddev("revenue_share_pct").alias("revenue_share_volatility")
                              ))
            
            # Classify market position
            market_classified = (market_evolution
                               .withColumn("market_position",
                                         when(col("avg_movie_share") >= 10.0, "Dominant")
                                         .when(col("avg_movie_share") >= 5.0, "Major")
                                         .when(col("avg_movie_share") >= 2.0, "Significant")
                                         .otherwise("Niche"))
                               .withColumn("trend_direction",
                                         when(col("avg_movie_share_change") > 0.1, "Growing Share")
                                         .when(col("avg_movie_share_change") < -0.1, "Losing Share")
                                         .otherwise("Stable Share"))
                               .withColumn("market_stability",
                                         when(col("movie_share_volatility") > 2.0, "Volatile")
                                         .when(col("movie_share_volatility") > 1.0, "Moderate")
                                         .otherwise("Stable"))
                               .withColumn("batch_run_id", lit(batch_id))
                               .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/market_share_evolution"
            (market_classified
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Market share evolution analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing market share evolution: {e}")
            return None


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Genre Trends Analyzer")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Genre_Trends_Analyzer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize trends analyzer
        trends_analyzer = GenreTrendsAnalyzer(spark, config)
        
        # Analyze trends
        trends_paths = trends_analyzer.analyze_all_genre_trends(args.batch_id)
        
        logger.info(f"Genre trends analysis completed: {trends_paths}")
        
    finally:
        spark.stop()