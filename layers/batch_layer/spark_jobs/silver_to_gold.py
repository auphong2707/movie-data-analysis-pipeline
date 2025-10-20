"""
Silver to Gold Layer Transformation Job.

This PySpark job creates aggregated views from Silver layer data including
genre analytics, trending scores, temporal analysis, and prepares data for Gold layer.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, stddev, collect_list, collect_set, size, desc, asc,
    lag, lead, row_number, rank, dense_rank, percent_rank, ntile,
    window, date_sub, date_add, datediff, months_between, year, month,
    dayofmonth, weekofyear, quarter, date_format, unix_timestamp,
    from_unixtime, lit, coalesce, concat, concat_ws, split, regexp_extract,
    array_contains, array_distinct, array_union, array_intersect, 
    explode, posexplode, greatest, least, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    LongType, DateType, TimestampType, ArrayType
)
from pyspark.sql.window import Window
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverToGoldTransformer:
    """Transforms Silver layer data into Gold layer aggregated views."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        self.job_config = config.get("jobs", {}).get("silver_to_gold", {})
        
        # Set up paths
        self.silver_path = self.hdfs_config["paths"]["silver"]
        self.gold_path = self.hdfs_config["paths"]["gold"]
        self.errors_path = self.hdfs_config["paths"]["errors"]
        
        # Performance settings
        self.window_partitions = self.job_config.get("window_partitions", 50)
        self.aggregation_push_down = self.job_config.get("aggregation_push_down", True)
        self.cache_intermediate = self.job_config.get("cache_intermediate", True)
        
        # Configure Spark for optimal aggregations
        self._configure_spark_session()
    
    def _configure_spark_session(self):
        """Configure Spark session for optimal aggregation performance."""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.shuffle.partitions", str(self.window_partitions))
    
    def transform_silver_to_gold(
        self, 
        start_time: datetime,
        end_time: datetime,
        batch_id: str
    ) -> Dict[str, str]:
        """
        Main transformation function from Silver to Gold layer.
        
        Args:
            start_time: Start time for data processing window
            end_time: End time for data processing window
            batch_id: Unique identifier for this batch
            
        Returns:
            Dict[str, str]: Paths where Gold data was written
        """
        logger.info(f"Starting Silver to Gold transformation for batch {batch_id}")
        
        try:
            # Load Silver data
            silver_df = self._load_silver_data(start_time, end_time)
            
            if silver_df.count() == 0:
                logger.warning("No Silver data found for the specified time range")
                return {}
            
            # Cache Silver data for multiple aggregations
            if self.cache_intermediate:
                silver_df = silver_df.persist("MEMORY_AND_DISK_SER")
            
            gold_paths = {}
            
            # 1. Generate Genre Analytics
            genre_analytics_path = self._create_genre_analytics(silver_df, batch_id)
            if genre_analytics_path:
                gold_paths["genre_analytics"] = genre_analytics_path
            
            # 2. Generate Trending Scores
            trending_scores_path = self._create_trending_scores(silver_df, batch_id)
            if trending_scores_path:
                gold_paths["trending_scores"] = trending_scores_path
            
            # 3. Generate Temporal Analysis (YoY)
            temporal_analysis_path = self._create_temporal_analysis(silver_df, batch_id)
            if temporal_analysis_path:
                gold_paths["temporal_analysis"] = temporal_analysis_path
            
            # Update metadata
            self._update_gold_metadata(gold_paths, batch_id)
            
            logger.info(f"Successfully created Gold layer views: {list(gold_paths.keys())}")
            return gold_paths
            
        except Exception as e:
            logger.error(f"Error in Silver to Gold transformation: {e}")
            raise
        finally:
            # Unpersist cached data
            if self.cache_intermediate:
                silver_df.unpersist()
    
    def _load_silver_data(self, start_time: datetime, end_time: datetime) -> DataFrame:
        """
        Load Silver layer data for the specified time range.
        
        Args:
            start_time: Start of time window
            end_time: End of time window
            
        Returns:
            DataFrame: Silver layer data
        """
        logger.info(f"Loading Silver data from {start_time} to {end_time}")
        
        try:
            # Load with time-based filtering
            silver_df = (self.spark.read
                        .parquet(self.silver_path)
                        .filter(
                            (col("processed_timestamp") >= start_time) &
                            (col("processed_timestamp") <= end_time) &
                            (col("quality_flag").isin(["OK", "WARNING"]))  # Exclude ERROR quality
                        ))
            
            logger.info(f"Loaded {silver_df.count()} Silver records")
            return silver_df
            
        except Exception as e:
            logger.error(f"Error loading Silver data: {e}")
            raise
    
    def _create_genre_analytics(self, silver_df: DataFrame, batch_id: str) -> str:
        """
        Create genre analytics aggregations.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            str: Path where genre analytics was written
        """
        logger.info("Creating genre analytics aggregations")
        
        try:
            # Explode genres to create one row per genre per movie
            genres_df = (silver_df
                        .filter(col("genres").isNotNull() & (size(col("genres")) > 0))
                        .select(
                            col("movie_id"),
                            col("title"),
                            col("release_date"),
                            col("vote_average"),
                            col("vote_count"),
                            col("popularity"),
                            col("budget"),
                            col("revenue"),
                            col("sentiment_score"),
                            col("cast"),
                            col("crew"),
                            explode(col("genres")).alias("genre")
                        )
                        .filter(col("genre").isNotNull()))
            
            # Calculate genre aggregations by year and month
            genre_analytics = (genres_df
                              .withColumn("year", year(col("release_date")))
                              .withColumn("month", month(col("release_date")))
                              .filter(col("year").isNotNull())
                              .groupBy("genre", "year", "month")
                              .agg(
                                  count("movie_id").alias("total_movies"),
                                  avg("vote_average").alias("avg_rating"),
                                  spark_sum("revenue").alias("total_revenue"),
                                  avg("budget").alias("avg_budget"),
                                  avg("sentiment_score").alias("avg_sentiment"),
                                  collect_list("title").alias("all_movies")
                              ))
            
            # Get top movies per genre (by rating and popularity)
            window_spec = (Window
                          .partitionBy("genre", "year", "month")
                          .orderBy(desc("vote_average"), desc("popularity")))
            
            top_movies_df = (genres_df
                            .withColumn("year", year(col("release_date")))
                            .withColumn("month", month(col("release_date")))
                            .withColumn("rank", row_number().over(window_spec))
                            .filter(col("rank") <= 5)
                            .groupBy("genre", "year", "month")
                            .agg(collect_list("title").alias("top_movies")))
            
            # Get top directors per genre
            directors_df = (genres_df
                           .withColumn("year", year(col("release_date")))
                           .withColumn("month", month(col("release_date")))
                           .withColumn("directors", 
                                     expr("filter(crew, x -> x.job = 'Director')"))
                           .withColumn("director_names",
                                     expr("transform(directors, x -> x.name)"))
                           .filter(size(col("director_names")) > 0)
                           .select("genre", "year", "month", "director_names")
                           .withColumn("director", explode(col("director_names")))
                           .groupBy("genre", "year", "month", "director")
                           .count()
                           .withColumn("rank", row_number().over(
                               Window.partitionBy("genre", "year", "month")
                               .orderBy(desc("count"))))
                           .filter(col("rank") <= 3)
                           .groupBy("genre", "year", "month")
                           .agg(collect_list("director").alias("top_directors")))
            
            # Combine all genre analytics
            final_genre_analytics = (genre_analytics
                                    .join(top_movies_df, ["genre", "year", "month"], "left")
                                    .join(directors_df, ["genre", "year", "month"], "left")
                                    .withColumn("computed_timestamp", lit(datetime.utcnow()))
                                    .drop("all_movies"))
            
            # Write to Gold layer
            genre_analytics_path = f"{self.gold_path}/genre_analytics"
            (final_genre_analytics.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("year", "month")
             .parquet(genre_analytics_path))
            
            logger.info(f"Genre analytics written to: {genre_analytics_path}")
            return genre_analytics_path
            
        except Exception as e:
            logger.error(f"Error creating genre analytics: {e}")
            raise
    
    def _create_trending_scores(self, silver_df: DataFrame, batch_id: str) -> str:
        """
        Create trending scores with rolling windows.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            str: Path where trending scores was written
        """
        logger.info("Creating trending scores with rolling windows")
        
        try:
            # Prepare data for trending analysis
            trending_base = (silver_df
                           .filter(col("release_date").isNotNull())
                           .select(
                               col("movie_id"),
                               col("title"),
                               col("release_date"),
                               col("popularity"),
                               col("vote_average"),
                               col("vote_count")
                           )
                           .withColumn("release_date_unix", unix_timestamp(col("release_date")))
                           .filter(col("release_date_unix").isNotNull()))
            
            # Create windows for different time periods
            windows = {
                "7d": 7,
                "30d": 30, 
                "90d": 90
            }
            
            trending_results = []
            
            for window_name, days in windows.items():
                logger.info(f"Calculating trending scores for {window_name} window")
                
                # Calculate rolling metrics
                window_spec = (Window
                              .partitionBy("movie_id")
                              .orderBy("release_date_unix")
                              .rowsBetween(-days + 1, 0))
                
                windowed_df = (trending_base
                              .withColumn(f"avg_popularity_{window_name}", 
                                        avg("popularity").over(window_spec))
                              .withColumn(f"avg_rating_{window_name}",
                                        avg("vote_average").over(window_spec))
                              .withColumn(f"popularity_change_{window_name}",
                                        col("popularity") - lag("popularity", days).over(
                                            Window.partitionBy("movie_id").orderBy("release_date_unix")))
                              .withColumn(f"rating_momentum_{window_name}",
                                        col("vote_average") - lag("vote_average", days).over(
                                            Window.partitionBy("movie_id").orderBy("release_date_unix"))))
                
                # Calculate trend score (combination of popularity change and rating momentum)
                trend_df = (windowed_df
                           .withColumn("trend_score",
                                     coalesce(col(f"popularity_change_{window_name}"), lit(0.0)) * 0.7 +
                                     coalesce(col(f"rating_momentum_{window_name}"), lit(0.0)) * 0.3)
                           .withColumn("velocity",
                                     coalesce(col(f"popularity_change_{window_name}"), lit(0.0)) / days)
                           .withColumn("window", lit(window_name))
                           .withColumn("computed_date", col("release_date"))
                           .withColumn("computed_timestamp", lit(datetime.utcnow()))
                           .select(
                               col("movie_id"),
                               col("title"),
                               col("window"),
                               col("trend_score"),
                               col("velocity"),
                               col(f"popularity_change_{window_name}").alias("popularity_change"),
                               col(f"rating_momentum_{window_name}").alias("rating_momentum"),
                               col("computed_date"),
                               col("computed_timestamp")
                           ))
                
                trending_results.append(trend_df)
            
            # Union all window results
            final_trending = trending_results[0]
            for trend_df in trending_results[1:]:
                final_trending = final_trending.union(trend_df)
            
            # Write to Gold layer
            trending_path = f"{self.gold_path}/trending_scores"
            (final_trending.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("window", "computed_date")
             .parquet(trending_path))
            
            logger.info(f"Trending scores written to: {trending_path}")
            return trending_path
            
        except Exception as e:
            logger.error(f"Error creating trending scores: {e}")
            raise
    
    def _create_temporal_analysis(self, silver_df: DataFrame, batch_id: str) -> str:
        """
        Create temporal year-over-year analysis.
        
        Args:
            silver_df: Silver layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            str: Path where temporal analysis was written
        """
        logger.info("Creating temporal year-over-year analysis")
        
        try:
            # Prepare base data for temporal analysis
            temporal_base = (silver_df
                           .filter(col("release_date").isNotNull())
                           .withColumn("year", year(col("release_date")))
                           .withColumn("month", month(col("release_date")))
                           .filter(col("year").isNotNull()))
            
            # Calculate metrics by year, month, and genre
            temporal_metrics = []
            
            # Revenue analysis
            revenue_analysis = (temporal_base
                              .filter(col("revenue").isNotNull() & (col("revenue") > 0))
                              .withColumn("primary_genre", col("genres")[0])
                              .groupBy("year", "month", "primary_genre")
                              .agg(
                                  spark_sum("revenue").alias("total_revenue"),
                                  avg("revenue").alias("avg_revenue"),
                                  count("movie_id").alias("movie_count")
                              )
                              .withColumn("metric_type", lit("revenue")))
            
            # Popularity analysis  
            popularity_analysis = (temporal_base
                                 .filter(col("popularity").isNotNull())
                                 .withColumn("primary_genre", col("genres")[0])
                                 .groupBy("year", "month", "primary_genre")
                                 .agg(
                                     avg("popularity").alias("avg_popularity"),
                                     spark_max("popularity").alias("max_popularity"),
                                     count("movie_id").alias("movie_count")
                                 )
                                 .withColumn("metric_type", lit("popularity"))
                                 .withColumn("total_revenue", lit(None).cast("long"))
                                 .withColumn("avg_revenue", lit(None).cast("double"))
                                 .select("year", "month", "primary_genre", "metric_type",
                                       "avg_popularity", "max_popularity", "movie_count",
                                       "total_revenue", "avg_revenue"))
            
            # Ratings analysis
            ratings_analysis = (temporal_base
                              .filter(col("vote_average").isNotNull())
                              .withColumn("primary_genre", col("genres")[0])
                              .groupBy("year", "month", "primary_genre")
                              .agg(
                                  avg("vote_average").alias("avg_rating"),
                                  spark_sum("vote_count").alias("total_votes"),
                                  count("movie_id").alias("movie_count")
                              )
                              .withColumn("metric_type", lit("ratings"))
                              .withColumn("total_revenue", lit(None).cast("long"))
                              .withColumn("avg_revenue", lit(None).cast("double"))
                              .withColumn("avg_popularity", lit(None).cast("double"))
                              .withColumn("max_popularity", lit(None).cast("double"))
                              .select("year", "month", "primary_genre", "metric_type",
                                    "avg_rating", "total_votes", "movie_count",
                                    "total_revenue", "avg_revenue", "avg_popularity", "max_popularity"))
            
            # Calculate year-over-year changes for each metric type
            yoy_analyses = []
            
            for analysis_df, metric_col in [
                (revenue_analysis, "total_revenue"),
                (popularity_analysis, "avg_popularity"), 
                (ratings_analysis, "avg_rating")
            ]:
                # Calculate previous year values
                window_spec = (Window
                              .partitionBy("month", "primary_genre", "metric_type")
                              .orderBy("year"))
                
                yoy_df = (analysis_df
                         .withColumn("previous_year_value",
                                   lag(col(metric_col), 1).over(window_spec))
                         .withColumn("current_value", col(metric_col))
                         .withColumn("yoy_change_absolute",
                                   col("current_value") - col("previous_year_value"))
                         .withColumn("yoy_change_percent",
                                   when(col("previous_year_value") != 0,
                                       (col("yoy_change_absolute") / col("previous_year_value")) * 100)
                                   .otherwise(lit(None)))
                         .withColumn("trend_direction",
                                   when(col("yoy_change_percent") > 5, "up")
                                   .when(col("yoy_change_percent") < -5, "down")
                                   .otherwise("stable"))
                         .withColumn("computed_timestamp", lit(datetime.utcnow()))
                         .select(
                             col("metric_type"),
                             col("year"),
                             col("month"),
                             col("primary_genre").alias("genre"),
                             col("current_value"),
                             col("previous_year_value"),
                             col("yoy_change_percent"),
                             col("yoy_change_absolute"),
                             col("trend_direction"),
                             col("computed_timestamp")
                         ))
                
                yoy_analyses.append(yoy_df)
            
            # Union all temporal analyses
            final_temporal = yoy_analyses[0]
            for yoy_df in yoy_analyses[1:]:
                final_temporal = final_temporal.union(yoy_df)
            
            # Write to Gold layer
            temporal_path = f"{self.gold_path}/temporal_analysis"
            (final_temporal.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("metric_type", "year")
             .parquet(temporal_path))
            
            logger.info(f"Temporal analysis written to: {temporal_path}")
            return temporal_path
            
        except Exception as e:
            logger.error(f"Error creating temporal analysis: {e}")
            raise
    
    def _update_gold_metadata(self, gold_paths: Dict[str, str], batch_id: str):
        """
        Update metadata for Gold layer transformations.
        
        Args:
            gold_paths: Dictionary of Gold layer paths created
            batch_id: Batch identifier
        """
        logger.info("Updating Gold layer metadata")
        
        for view_type, path in gold_paths.items():
            metadata_record = {
                "batch_id": batch_id,
                "view_type": view_type,
                "gold_path": path,
                "transformation": "silver_to_gold",
                "processed_timestamp": datetime.utcnow().isoformat(),
                "status": "completed"
            }
            
            try:
                metadata_df = self.spark.createDataFrame([metadata_record])
                metadata_path = f"{self.hdfs_config['paths']['metadata']}/gold_transformation_log"
                
                (metadata_df.write
                 .mode("append")
                 .option("compression", "snappy")
                 .json(metadata_path))
                
            except Exception as e:
                logger.error(f"Error updating Gold metadata for {view_type}: {e}")


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Silver to Gold Transformation")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--start-time", required=True, help="Start time (ISO format)")
    parser.add_argument("--end-time", required=True, help="End time (ISO format)")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Parse times
    start_time = datetime.fromisoformat(args.start_time)
    end_time = datetime.fromisoformat(args.end_time)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Silver_to_Gold_Transformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize transformer
        transformer = SilverToGoldTransformer(spark, config)
        
        # Run transformation
        gold_paths = transformer.transform_silver_to_gold(
            start_time=start_time,
            end_time=end_time,
            batch_id=args.batch_id
        )
        
        logger.info(f"Gold layer transformation completed: {gold_paths}")
        
    finally:
        spark.stop()