"""
Temporal Analysis Module.

This module performs year-over-year analysis and temporal trend detection
for movie industry metrics including revenue, ratings, and production patterns.
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
    round as spark_round, monotonically_increasing_id, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TemporalAnalyzer:
    """Performs comprehensive temporal analysis on movie industry data."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize temporal analyzer.
        
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
        
        # Temporal analysis configuration
        self.temporal_config = config.get("temporal", {})
        self.min_years_for_trend = self.temporal_config.get("min_years_for_trend", 3)
        self.significance_threshold = self.temporal_config.get("significance_threshold", 0.05)
        self.cyclical_window = self.temporal_config.get("cyclical_window", 5)
    
    def generate_temporal_analysis(self, batch_id: str) -> Dict[str, str]:
        """
        Generate comprehensive temporal analysis.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Dict[str, str]: Paths to generated analyses
        """
        logger.info(f"Generating temporal analysis for batch {batch_id}")
        
        analysis_paths = {}
        
        try:
            # Load and prepare data
            silver_df = self._prepare_temporal_data()
            
            # Year-over-year analysis
            yoy_path = self._analyze_year_over_year_trends(silver_df, batch_id)
            if yoy_path:
                analysis_paths["year_over_year"] = yoy_path
            
            # Monthly and seasonal patterns
            seasonal_path = self._analyze_monthly_seasonal_patterns(silver_df, batch_id)
            if seasonal_path:
                analysis_paths["seasonal_patterns"] = seasonal_path
            
            # Cyclical pattern detection
            cyclical_path = self._detect_cyclical_patterns(silver_df, batch_id)
            if cyclical_path:
                analysis_paths["cyclical_patterns"] = cyclical_path
            
            # Long-term trend analysis
            longterm_path = self._analyze_longterm_trends(silver_df, batch_id)
            if longterm_path:
                analysis_paths["longterm_trends"] = longterm_path
            
            # Economic correlation analysis
            economic_path = self._analyze_economic_correlations(silver_df, batch_id)
            if economic_path:
                analysis_paths["economic_correlations"] = economic_path
            
            # Forecast indicators
            forecast_path = self._generate_forecast_indicators(silver_df, batch_id)
            if forecast_path:
                analysis_paths["forecast_indicators"] = forecast_path
            
            logger.info(f"Temporal analysis completed: {list(analysis_paths.keys())}")
            return analysis_paths
            
        except Exception as e:
            logger.error(f"Error generating temporal analysis: {e}")
            raise
    
    def _prepare_temporal_data(self) -> DataFrame:
        """Prepare Silver layer data for temporal analysis."""
        logger.info("Preparing temporal data for analysis")
        
        try:
            # Load Silver data
            silver_df = self.spark.read.parquet(self.silver_path)
            
            # Add temporal features
            temporal_df = (silver_df
                          .filter(col("quality_flag") == "HIGH")
                          .withColumn("release_year", year(col("release_date")))
                          .withColumn("release_month", month(col("release_date")))
                          .withColumn("release_quarter", 
                                    when(col("release_month").isin([1,2,3]), 1)
                                    .when(col("release_month").isin([4,5,6]), 2)
                                    .when(col("release_month").isin([7,8,9]), 3)
                                    .otherwise(4))
                          .withColumn("decade", (col("release_year") / 10).cast("int") * 10)
                          .withColumn("profit", col("revenue") - col("budget"))
                          .withColumn("roi", 
                                    when(col("budget") > 0, 
                                         (col("revenue") - col("budget")) / col("budget"))
                                    .otherwise(None))
                          .withColumn("is_profitable", col("profit") > 0)
                          .withColumn("is_blockbuster", col("revenue") >= 100000000)
                          .withColumn("is_well_rated", col("vote_average") >= 7.0))
            
            # Cache for multiple operations
            temporal_df.cache()
            
            logger.info(f"Prepared temporal data with {temporal_df.count()} movies")
            return temporal_df
            
        except Exception as e:
            logger.error(f"Error preparing temporal data: {e}")
            raise
    
    def _analyze_year_over_year_trends(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze year-over-year trends for key metrics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing year-over-year trends")
        
        try:
            # Calculate yearly aggregates
            yearly_metrics = (silver_df
                            .groupBy("release_year")
                            .agg(
                                count("movie_id").alias("total_movies"),
                                spark_sum("revenue").alias("total_revenue"),
                                avg("revenue").alias("avg_revenue"),
                                spark_sum("budget").alias("total_budget"),
                                avg("budget").alias("avg_budget"),
                                avg("vote_average").alias("avg_rating"),
                                avg("popularity").alias("avg_popularity"),
                                avg("sentiment_score").alias("avg_sentiment"),
                                avg("runtime").alias("avg_runtime"),
                                
                                # Success metrics
                                spark_sum(when(col("is_profitable"), 1).otherwise(0)).alias("profitable_movies"),
                                spark_sum(when(col("is_blockbuster"), 1).otherwise(0)).alias("blockbuster_movies"),
                                spark_sum(when(col("is_well_rated"), 1).otherwise(0)).alias("well_rated_movies"),
                                
                                # Revenue distribution
                                percentile_approx("revenue", 0.5).alias("median_revenue"),
                                percentile_approx("revenue", 0.75).alias("q3_revenue"),
                                percentile_approx("revenue", 0.25).alias("q1_revenue"),
                                spark_max("revenue").alias("max_revenue"),
                                stddev("revenue").alias("revenue_stddev")
                            ))
            
            # Calculate year-over-year changes
            yoy_window = Window.orderBy("release_year")
            
            yoy_trends = (yearly_metrics
                         .withColumn("prev_total_movies", lag("total_movies").over(yoy_window))
                         .withColumn("prev_total_revenue", lag("total_revenue").over(yoy_window))
                         .withColumn("prev_avg_revenue", lag("avg_revenue").over(yoy_window))
                         .withColumn("prev_avg_rating", lag("avg_rating").over(yoy_window))
                         .withColumn("prev_avg_budget", lag("avg_budget").over(yoy_window))
                         
                         # Calculate percentage changes
                         .withColumn("movies_yoy_change_pct",
                                    when(col("prev_total_movies").isNotNull() & (col("prev_total_movies") > 0),
                                         ((col("total_movies") - col("prev_total_movies")) / col("prev_total_movies")) * 100)
                                    .otherwise(None))
                         .withColumn("revenue_yoy_change_pct",
                                    when(col("prev_total_revenue").isNotNull() & (col("prev_total_revenue") > 0),
                                         ((col("total_revenue") - col("prev_total_revenue")) / col("prev_total_revenue")) * 100)
                                    .otherwise(None))
                         .withColumn("avg_revenue_yoy_change_pct",
                                    when(col("prev_avg_revenue").isNotNull() & (col("prev_avg_revenue") > 0),
                                         ((col("avg_revenue") - col("prev_avg_revenue")) / col("prev_avg_revenue")) * 100)
                                    .otherwise(None))
                         .withColumn("rating_yoy_change_pct",
                                    when(col("prev_avg_rating").isNotNull() & (col("prev_avg_rating") > 0),
                                         ((col("avg_rating") - col("prev_avg_rating")) / col("prev_avg_rating")) * 100)
                                    .otherwise(None))
                         .withColumn("budget_yoy_change_pct",
                                    when(col("prev_avg_budget").isNotNull() & (col("prev_avg_budget") > 0),
                                         ((col("avg_budget") - col("prev_avg_budget")) / col("prev_avg_budget")) * 100)
                                    .otherwise(None)))
            
            # Calculate success rates and their changes
            yoy_with_rates = (yoy_trends
                            .withColumn("profitability_rate", col("profitable_movies") / col("total_movies") * 100)
                            .withColumn("blockbuster_rate", col("blockbuster_movies") / col("total_movies") * 100)
                            .withColumn("quality_rate", col("well_rated_movies") / col("total_movies") * 100)
                            
                            # Previous year rates for comparison
                            .withColumn("prev_profitability_rate", lag("profitability_rate").over(yoy_window))
                            .withColumn("prev_blockbuster_rate", lag("blockbuster_rate").over(yoy_window))
                            .withColumn("prev_quality_rate", lag("quality_rate").over(yoy_window))
                            
                            # Rate changes
                            .withColumn("profitability_rate_change",
                                       col("profitability_rate") - col("prev_profitability_rate"))
                            .withColumn("blockbuster_rate_change",
                                       col("blockbuster_rate") - col("prev_blockbuster_rate"))
                            .withColumn("quality_rate_change",
                                       col("quality_rate") - col("prev_quality_rate")))
            
            # Classify trend directions
            yoy_classified = (yoy_with_rates
                            .withColumn("overall_trend",
                                      when((col("movies_yoy_change_pct") > 5) & (col("revenue_yoy_change_pct") > 10), "Strong Growth")
                                      .when((col("movies_yoy_change_pct") > 0) & (col("revenue_yoy_change_pct") > 0), "Moderate Growth")
                                      .when((col("movies_yoy_change_pct") < -5) | (col("revenue_yoy_change_pct") < -10), "Decline")
                                      .otherwise("Stable"))
                            .withColumn("quality_trend",
                                      when(col("rating_yoy_change_pct") > 2, "Improving Quality")
                                      .when(col("rating_yoy_change_pct") < -2, "Declining Quality")
                                      .otherwise("Stable Quality"))
                            .withColumn("market_health",
                                      when((col("profitability_rate") > 60) & (col("quality_rate") > 30), "Healthy")
                                      .when((col("profitability_rate") > 40) & (col("quality_rate") > 20), "Moderate")
                                      .otherwise("Challenging"))
                            .withColumn("batch_run_id", lit(batch_id))
                            .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/year_over_year_analysis"
            (yoy_classified
             .coalesce(5)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Year-over-year analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing year-over-year trends: {e}")
            return None
    
    def _analyze_monthly_seasonal_patterns(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze monthly and seasonal patterns in movie releases and performance.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing monthly and seasonal patterns")
        
        try:
            # Monthly statistics across all years
            monthly_stats = (silver_df
                           .groupBy("release_month")
                           .agg(
                               count("movie_id").alias("total_releases"),
                               avg("revenue").alias("avg_revenue"),
                               avg("vote_average").alias("avg_rating"),
                               avg("popularity").alias("avg_popularity"),
                               spark_sum("revenue").alias("total_revenue"),
                               spark_sum(when(col("is_blockbuster"), 1).otherwise(0)).alias("blockbuster_count"),
                               avg("budget").alias("avg_budget"),
                               avg("sentiment_score").alias("avg_sentiment")
                           ))
            
            # Calculate monthly indices (normalized to 1.0 = average)
            total_releases = monthly_stats.agg(spark_sum("total_releases")).collect()[0][0]
            monthly_avg_releases = total_releases / 12.0
            
            monthly_indexed = (monthly_stats
                             .withColumn("release_index", col("total_releases") / lit(monthly_avg_releases))
                             .withColumn("revenue_index", 
                                       col("avg_revenue") / 
                                       monthly_stats.agg(avg("avg_revenue")).collect()[0][0])
                             .withColumn("rating_index",
                                       col("avg_rating") / 
                                       monthly_stats.agg(avg("avg_rating")).collect()[0][0])
                             
                             # Season classification
                             .withColumn("season",
                                       when(col("release_month").isin([12, 1, 2]), "Winter")
                                       .when(col("release_month").isin([3, 4, 5]), "Spring")
                                       .when(col("release_month").isin([6, 7, 8]), "Summer")
                                       .otherwise("Fall"))
                             
                             # Month names for readability
                             .withColumn("month_name",
                                       when(col("release_month") == 1, "January")
                                       .when(col("release_month") == 2, "February")
                                       .when(col("release_month") == 3, "March")
                                       .when(col("release_month") == 4, "April")
                                       .when(col("release_month") == 5, "May")
                                       .when(col("release_month") == 6, "June")
                                       .when(col("release_month") == 7, "July")
                                       .when(col("release_month") == 8, "August")
                                       .when(col("release_month") == 9, "September")
                                       .when(col("release_month") == 10, "October")
                                       .when(col("release_month") == 11, "November")
                                       .otherwise("December")))
            
            # Seasonal aggregations
            seasonal_stats = (monthly_indexed
                            .groupBy("season")
                            .agg(
                                spark_sum("total_releases").alias("season_releases"),
                                avg("avg_revenue").alias("season_avg_revenue"),
                                avg("avg_rating").alias("season_avg_rating"),
                                spark_sum("blockbuster_count").alias("season_blockbusters"),
                                avg("release_index").alias("season_release_index"),
                                avg("revenue_index").alias("season_revenue_index")
                            ))
            
            # Identify seasonal preferences
            seasonal_classified = (seasonal_stats
                                 .withColumn("season_type",
                                           when(col("season_release_index") > 1.2, "Peak Season")
                                           .when(col("season_release_index") > 0.8, "Active Season")
                                           .otherwise("Quiet Season"))
                                 .withColumn("commercial_performance",
                                           when(col("season_revenue_index") > 1.1, "High Commercial")
                                           .when(col("season_revenue_index") > 0.9, "Average Commercial")
                                           .otherwise("Low Commercial")))
            
            # Combine monthly and seasonal data
            final_seasonal_analysis = (monthly_indexed
                                     .join(seasonal_classified.select("season", "season_type", "commercial_performance"), 
                                           "season", "left")
                                     .withColumn("batch_run_id", lit(batch_id))
                                     .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/seasonal_patterns_analysis"
            (final_seasonal_analysis
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Seasonal patterns analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing seasonal patterns: {e}")
            return None
    
    def _detect_cyclical_patterns(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Detect cyclical patterns in movie industry metrics.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Detecting cyclical patterns")
        
        try:
            # Calculate moving averages for cycle detection
            yearly_data = (silver_df
                         .groupBy("release_year")
                         .agg(
                             count("movie_id").alias("movie_count"),
                             avg("revenue").alias("avg_revenue"),
                             avg("vote_average").alias("avg_rating"),
                             spark_sum("revenue").alias("total_revenue")
                         )
                         .orderBy("release_year"))
            
            # Calculate moving averages
            cycle_window = Window.orderBy("release_year").rowsBetween(-2, 2)  # 5-year window
            
            cyclical_df = (yearly_data
                         .withColumn("ma_movie_count", avg("movie_count").over(cycle_window))
                         .withColumn("ma_avg_revenue", avg("avg_revenue").over(cycle_window))
                         .withColumn("ma_total_revenue", avg("total_revenue").over(cycle_window))
                         .withColumn("ma_avg_rating", avg("avg_rating").over(cycle_window))
                         
                         # Calculate deviations from moving average
                         .withColumn("movie_count_deviation", 
                                   (col("movie_count") - col("ma_movie_count")) / col("ma_movie_count") * 100)
                         .withColumn("revenue_deviation",
                                   (col("avg_revenue") - col("ma_avg_revenue")) / col("ma_avg_revenue") * 100)
                         .withColumn("rating_deviation",
                                   (col("avg_rating") - col("ma_avg_rating")) / col("ma_avg_rating") * 100))
            
            # Identify cycle phases
            cyclical_classified = (cyclical_df
                                 .withColumn("production_cycle_phase",
                                           when(col("movie_count_deviation") > 10, "Peak Production")
                                           .when(col("movie_count_deviation") > 0, "Expansion")
                                           .when(col("movie_count_deviation") > -10, "Contraction")
                                           .otherwise("Trough"))
                                 .withColumn("revenue_cycle_phase",
                                           when(col("revenue_deviation") > 15, "Revenue Peak")
                                           .when(col("revenue_deviation") > 0, "Revenue Growth")
                                           .when(col("revenue_deviation") > -15, "Revenue Decline")
                                           .otherwise("Revenue Trough"))
                                 .withColumn("quality_cycle_phase",
                                           when(col("rating_deviation") > 2, "Quality Peak")
                                           .when(col("rating_deviation") > 0, "Quality Improvement")
                                           .when(col("rating_deviation") > -2, "Quality Decline")
                                           .otherwise("Quality Trough"))
                                 .withColumn("batch_run_id", lit(batch_id))
                                 .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/cyclical_patterns"
            (cyclical_classified
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Cyclical patterns analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error detecting cyclical patterns: {e}")
            return None
    
    def _analyze_longterm_trends(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze long-term trends spanning decades.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing long-term trends")
        
        try:
            # Decade-level analysis
            decade_trends = (silver_df
                           .groupBy("decade")
                           .agg(
                               count("movie_id").alias("total_movies"),
                               avg("runtime").alias("avg_runtime"),
                               avg("budget").alias("avg_budget"),
                               avg("revenue").alias("avg_revenue"),
                               avg("vote_average").alias("avg_rating"),
                               avg("popularity").alias("avg_popularity"),
                               avg("sentiment_score").alias("avg_sentiment"),
                               
                               # Technology and production evolution
                               percentile_approx("budget", 0.9).alias("top_10_pct_budget"),
                               percentile_approx("revenue", 0.9).alias("top_10_pct_revenue"),
                               spark_max("budget").alias("max_budget"),
                               spark_max("revenue").alias("max_revenue"),
                               
                               # Success metrics evolution
                               (spark_sum(when(col("is_profitable"), 1).otherwise(0)) / count("*") * 100).alias("profitability_rate"),
                               (spark_sum(when(col("is_blockbuster"), 1).otherwise(0)) / count("*") * 100).alias("blockbuster_rate"),
                               (spark_sum(when(col("is_well_rated"), 1).otherwise(0)) / count("*") * 100).alias("quality_rate")
                           ))
            
            # Calculate decade-over-decade changes
            decade_window = Window.orderBy("decade")
            
            longterm_changes = (decade_trends
                              .withColumn("prev_avg_runtime", lag("avg_runtime").over(decade_window))
                              .withColumn("prev_avg_budget", lag("avg_budget").over(decade_window))
                              .withColumn("prev_avg_revenue", lag("avg_revenue").over(decade_window))
                              .withColumn("prev_avg_rating", lag("avg_rating").over(decade_window))
                              
                              # Calculate percentage changes
                              .withColumn("runtime_change_pct",
                                        when(col("prev_avg_runtime").isNotNull() & (col("prev_avg_runtime") > 0),
                                             ((col("avg_runtime") - col("prev_avg_runtime")) / col("prev_avg_runtime")) * 100)
                                        .otherwise(None))
                              .withColumn("budget_change_pct",
                                        when(col("prev_avg_budget").isNotNull() & (col("prev_avg_budget") > 0),
                                             ((col("avg_budget") - col("prev_avg_budget")) / col("prev_avg_budget")) * 100)
                                        .otherwise(None))
                              .withColumn("revenue_change_pct",
                                        when(col("prev_avg_revenue").isNotNull() & (col("prev_avg_revenue") > 0),
                                             ((col("avg_revenue") - col("prev_avg_revenue")) / col("prev_avg_revenue")) * 100)
                                        .otherwise(None))
                              .withColumn("rating_change_pct",
                                        when(col("prev_avg_rating").isNotNull() & (col("prev_avg_rating") > 0),
                                             ((col("avg_rating") - col("prev_avg_rating")) / col("prev_avg_rating")) * 100)
                                        .otherwise(None)))
            
            # Classify evolution patterns
            evolution_classified = (longterm_changes
                                  .withColumn("production_evolution",
                                            when(col("budget_change_pct") > 50, "High Investment Era")
                                            .when(col("budget_change_pct") > 0, "Growing Investment")
                                            .otherwise("Stable Investment"))
                                  .withColumn("commercial_evolution",
                                            when(col("revenue_change_pct") > 100, "Explosive Growth")
                                            .when(col("revenue_change_pct") > 50, "Strong Growth")
                                            .when(col("revenue_change_pct") > 0, "Moderate Growth")
                                            .otherwise("Stagnation"))
                                  .withColumn("content_evolution",
                                            when(col("runtime_change_pct") > 20, "Longer Content")
                                            .when(spark_abs(col("runtime_change_pct")) <= 10, "Stable Content")
                                            .otherwise("Shorter Content"))
                                  .withColumn("decade_label",
                                            when(col("decade") == 1970, "1970s")
                                            .when(col("decade") == 1980, "1980s")
                                            .when(col("decade") == 1990, "1990s")
                                            .when(col("decade") == 2000, "2000s")
                                            .when(col("decade") == 2010, "2010s")
                                            .otherwise(col("decade").cast("string") + "s"))
                                  .withColumn("batch_run_id", lit(batch_id))
                                  .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/longterm_trends"
            (evolution_classified
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Long-term trends analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing long-term trends: {e}")
            return None
    
    def _analyze_economic_correlations(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Analyze correlations between movie metrics and potential economic indicators.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Analyzing economic correlations")
        
        try:
            # Simplified economic correlation analysis
            # In production, this would integrate with external economic data
            
            yearly_economic_proxies = (silver_df
                                     .groupBy("release_year")
                                     .agg(
                                         count("movie_id").alias("production_volume"),
                                         avg("budget").alias("avg_investment"),
                                         spark_sum("budget").alias("total_investment"),
                                         avg("revenue").alias("avg_returns"),
                                         spark_sum("revenue").alias("total_returns"),
                                         (spark_sum("revenue") / spark_sum("budget")).alias("industry_roi"),
                                         
                                         # Market confidence indicators
                                         (spark_sum(when(col("budget") >= 100000000, 1).otherwise(0)) / count("*") * 100).alias("big_budget_rate"),
                                         stddev("revenue").alias("revenue_volatility"),
                                         (percentile_approx("revenue", 0.9) / percentile_approx("revenue", 0.1)).alias("revenue_inequality")
                                     ))
            
            # Calculate correlation proxies
            econ_window = Window.orderBy("release_year")
            
            economic_indicators = (yearly_economic_proxies
                                 .withColumn("prev_production_volume", lag("production_volume").over(econ_window))
                                 .withColumn("prev_total_investment", lag("total_investment").over(econ_window))
                                 .withColumn("prev_industry_roi", lag("industry_roi").over(econ_window))
                                 
                                 # Growth indicators
                                 .withColumn("production_growth",
                                           when(col("prev_production_volume").isNotNull() & (col("prev_production_volume") > 0),
                                                (col("production_volume") - col("prev_production_volume")) / col("prev_production_volume"))
                                           .otherwise(None))
                                 .withColumn("investment_growth",
                                           when(col("prev_total_investment").isNotNull() & (col("prev_total_investment") > 0),
                                                (col("total_investment") - col("prev_total_investment")) / col("prev_total_investment"))
                                           .otherwise(None))
                                 .withColumn("roi_change",
                                           col("industry_roi") - col("prev_industry_roi")))
            
            # Classify economic periods
            economic_classified = (economic_indicators
                                 .withColumn("investment_climate",
                                           when(col("investment_growth") > 0.2, "Expansion")
                                           .when(col("investment_growth") < -0.1, "Contraction")
                                           .otherwise("Stable"))
                                 .withColumn("market_confidence",
                                           when((col("big_budget_rate") > 20) & (col("industry_roi") > 1.5), "High Confidence")
                                           .when(col("big_budget_rate") > 15, "Moderate Confidence")
                                           .otherwise("Low Confidence"))
                                 .withColumn("market_risk_level",
                                           when(col("revenue_volatility") > col("avg_returns"), "High Risk")
                                           .when(col("revenue_volatility") > col("avg_returns") * 0.5, "Medium Risk")
                                           .otherwise("Low Risk"))
                                 .withColumn("batch_run_id", lit(batch_id))
                                 .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/economic_correlations"
            (economic_classified
             .coalesce(2)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Economic correlations analysis saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error analyzing economic correlations: {e}")
            return None
    
    def _generate_forecast_indicators(self, silver_df: DataFrame, batch_id: str) -> Optional[str]:
        """
        Generate indicators that can be used for forecasting future trends.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            Optional[str]: Path to saved analysis
        """
        logger.info("Generating forecast indicators")
        
        try:
            # Calculate trend momentum indicators
            recent_years = [datetime.now().year - i for i in range(1, 4)]  # Last 3 years
            
            recent_trends = (silver_df
                           .filter(col("release_year").isin(recent_years))
                           .groupBy("release_year")
                           .agg(
                               count("movie_id").alias("movie_count"),
                               avg("revenue").alias("avg_revenue"),
                               avg("budget").alias("avg_budget"),
                               avg("vote_average").alias("avg_rating"),
                               (spark_sum(when(col("is_profitable"), 1).otherwise(0)) / count("*") * 100).alias("profitability_rate")
                           )
                           .orderBy("release_year"))
            
            # Calculate trend velocities (simple linear trend approximation)
            trend_window = Window.orderBy("release_year")
            
            forecast_indicators = (recent_trends
                                 .withColumn("prev_movie_count", lag("movie_count").over(trend_window))
                                 .withColumn("prev_avg_revenue", lag("avg_revenue").over(trend_window))
                                 .withColumn("prev_profitability", lag("profitability_rate").over(trend_window))
                                 
                                 # Trend velocities
                                 .withColumn("production_velocity",
                                           when(col("prev_movie_count").isNotNull(),
                                                col("movie_count") - col("prev_movie_count"))
                                           .otherwise(None))
                                 .withColumn("revenue_velocity",
                                           when(col("prev_avg_revenue").isNotNull(),
                                                (col("avg_revenue") - col("prev_avg_revenue")) / col("prev_avg_revenue"))
                                           .otherwise(None))
                                 .withColumn("profitability_velocity",
                                           col("profitability_rate") - col("prev_profitability")))
            
            # Aggregate forecast signals
            forecast_summary = (forecast_indicators
                              .agg(
                                  avg("production_velocity").alias("avg_production_velocity"),
                                  avg("revenue_velocity").alias("avg_revenue_velocity"),
                                  avg("profitability_velocity").alias("avg_profitability_velocity"),
                                  stddev("production_velocity").alias("production_volatility"),
                                  stddev("revenue_velocity").alias("revenue_volatility")
                              )
                              .withColumn("production_trend_signal",
                                        when(col("avg_production_velocity") > 10, "Accelerating")
                                        .when(col("avg_production_velocity") > 0, "Growing")
                                        .when(col("avg_production_velocity") > -10, "Declining")
                                        .otherwise("Contracting"))
                              .withColumn("revenue_trend_signal",
                                        when(col("avg_revenue_velocity") > 0.1, "Strong Growth")
                                        .when(col("avg_revenue_velocity") > 0, "Moderate Growth")
                                        .when(col("avg_revenue_velocity") > -0.1, "Declining")
                                        .otherwise("Steep Decline"))
                              .withColumn("market_stability_signal",
                                        when((col("production_volatility") < 5) & (col("revenue_volatility") < 0.2), "Stable")
                                        .when((col("production_volatility") < 15) & (col("revenue_volatility") < 0.5), "Moderately Volatile")
                                        .otherwise("Highly Volatile"))
                              .withColumn("forecast_confidence",
                                        when(col("market_stability_signal") == "Stable", "High")
                                        .when(col("market_stability_signal") == "Moderately Volatile", "Medium")
                                        .otherwise("Low"))
                              .withColumn("batch_run_id", lit(batch_id))
                              .withColumn("computed_at", lit(datetime.utcnow())))
            
            # Save to Gold layer
            output_path = f"{self.gold_path}/forecast_indicators"
            (forecast_summary
             .coalesce(1)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            logger.info(f"Forecast indicators saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating forecast indicators: {e}")
            return None


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Temporal Analyzer")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Temporal_Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize temporal analyzer
        temporal_analyzer = TemporalAnalyzer(spark, config)
        
        # Generate analysis
        analysis_paths = temporal_analyzer.generate_temporal_analysis(args.batch_id)
        
        logger.info(f"Temporal analysis completed: {analysis_paths}")
        
    finally:
        spark.stop()