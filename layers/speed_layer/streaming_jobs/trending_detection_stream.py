"""
Trending Detection Stream Processing
Detect trending/hot movies using velocity and acceleration metrics.
"""

import logging
import os
import sys
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, stddev, first, last, 
    lag, lead, coalesce, lit, expr, when, rank, dense_rank, desc, asc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    LongType, TimestampType, ArrayType, BooleanType
)
from pyspark.sql.window import Window

# Add config to path
sys.path.insert(0, '/app/config')
from config_loader import load_config

logger = logging.getLogger(__name__)


class TrendingDetectionStreamProcessor:
    """Detect trending movies using real-time velocity and acceleration analysis."""
    
    def __init__(self, config_path: str = "/app/config/spark_streaming_config.yaml"):
        """Initialize the trending detection stream processor."""
        
        # Load configuration with env var substitution
        self.config = load_config(config_path)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Define schemas
        self.trending_schema = self._get_trending_schema()
        
        # Trending detection parameters
        self.trending_params = self.config.get('trending_detection', {
            'min_popularity_threshold': 10.0,
            'min_rating_count': 5,
            'velocity_weight': 0.4,
            'acceleration_weight': 0.3,
            'volume_weight': 0.3,
            'top_n_trending': 20
        })
        
        logger.info("Trending Detection Stream Processor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with appropriate configuration."""
        spark_config = self.config['spark']
        
        builder = SparkSession.builder \
            .appName(spark_config['app_name'] + "_trending") \
            .master(spark_config['master'])
        
        # Add configuration parameters
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        # Add packages (compatible versions for Spark 3.4+)
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
            "org.apache.kafka:kafka-clients:3.4.0"
        ]
        builder = builder.config("spark.jars.packages", ",".join(packages))
        
        # Cassandra connection
        builder = builder.config("spark.cassandra.connection.host", 
                                os.getenv("CASSANDRA_HOSTS", "cassandra"))
        builder = builder.config("spark.cassandra.connection.port", "9042")
        
        # Checkpoint location
        builder = builder.config("spark.sql.streaming.checkpointLocation", 
                                "/app/checkpoints/trending")
        
        return builder.getOrCreate()
    
    def _get_trending_schema(self) -> StructType:
        """Define schema for trending event messages."""
        return StructType([
            StructField("movie_id", LongType(), False),
            StructField("title", StringType(), False),
            StructField("trend_score", DoubleType(), False),
            StructField("popularity_delta", DoubleType(), False),
            StructField("rating_velocity", DoubleType(), False),
            StructField("review_volume", LongType(), False),
            StructField("time_window", StringType(), False),
            StructField("timestamp", LongType(), False)
        ])
    
    def read_trending_stream(self) -> DataFrame:
        """Read trending events stream from Kafka."""
        kafka_config = self.config['kafka']
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("subscribe", kafka_config['topics']['trending']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.trending_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("timestamp") / 1000).cast("long")))
        
        return timestamped_df
    
    def read_movie_stats_stream(self) -> DataFrame:
        """Read movie statistics from Cassandra for trending analysis."""
        cassandra_config = self.config['cassandra']
        
        # This would typically read from Cassandra table
        # For now, we'll simulate reading from a Kafka topic
        return self.spark.readStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", cassandra_config['keyspace']) \
            .option("table", "movie_stats") \
            .load()
    
    def calculate_trending_velocity(self, trending_df: DataFrame) -> DataFrame:
        """Calculate velocity metrics for trending detection."""
        
        window_duration = self.config['processing']['window_duration']
        watermark_delay = self.config['processing']['watermark_delay']
        
        # Add watermark
        watermarked_df = trending_df \
            .withWatermark("event_time", watermark_delay)
        
        # Current window metrics
        current_metrics = watermarked_df \
            .groupBy(
                window(col("event_time"), window_duration),
                col("movie_id")
            ) \
            .agg(
                first("title").alias("title"),
                avg("trend_score").alias("avg_trend_score"),
                avg("popularity_delta").alias("avg_popularity_delta"),
                avg("rating_velocity").alias("avg_rating_velocity"),
                sum("review_volume").alias("total_review_volume"),
                count("movie_id").alias("event_count"),
                max("trend_score").alias("max_trend_score")
            )
        
        # Calculate velocity using window functions
        movie_window = Window.partitionBy("movie_id").orderBy("window")
        
        velocity_df = current_metrics \
            .withColumn("prev_trend_score",
                       lag("avg_trend_score", 1).over(movie_window)) \
            .withColumn("trend_velocity",
                       when(col("prev_trend_score").isNotNull(),
                            (col("avg_trend_score") - col("prev_trend_score")) / 
                            (col("prev_trend_score") + 1))  # Avoid division by zero
                       .otherwise(lit(0.0))) \
            .withColumn("prev_trend_velocity",
                       lag("trend_velocity", 1).over(movie_window)) \
            .withColumn("trend_acceleration",
                       when(col("prev_trend_velocity").isNotNull(),
                            col("trend_velocity") - col("prev_trend_velocity"))
                       .otherwise(lit(0.0)))
        
        return velocity_df
    
    def detect_hot_movies(self, velocity_df: DataFrame) -> DataFrame:
        """Detect hot movies based on trending criteria."""
        
        params = self.trending_params
        
        # Calculate composite hotness score
        hotness_df = velocity_df \
            .withColumn("velocity_score",
                       col("trend_velocity") * params['velocity_weight']) \
            .withColumn("acceleration_score",
                       col("trend_acceleration") * params['acceleration_weight']) \
            .withColumn("volume_score",
                       (col("total_review_volume") / 100.0) * params['volume_weight']) \
            .withColumn("hotness_score",
                       col("velocity_score") + col("acceleration_score") + col("volume_score"))
        
        # Apply thresholds and filters
        filtered_df = hotness_df \
            .filter(col("avg_trend_score") >= params['min_popularity_threshold']) \
            .filter(col("total_review_volume") >= params['min_rating_count']) \
            .filter(col("hotness_score") > 0)  # Only positive trending
        
        # Rank by hotness score within each window
        window_spec = Window.partitionBy("window").orderBy(desc("hotness_score"))
        
        ranked_df = filtered_df \
            .withColumn("trend_rank", rank().over(window_spec)) \
            .filter(col("trend_rank") <= params['top_n_trending'])
        
        # Select final columns
        result_df = ranked_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("movie_id"),
            col("title"),
            col("trend_rank"),
            col("hotness_score"),
            col("avg_trend_score"),
            col("trend_velocity"),
            col("trend_acceleration"),
            col("total_review_volume"),
            col("event_count"),
            col("velocity_score"),
            col("acceleration_score"),
            col("volume_score")
        )
        
        return result_df
    
    def detect_breakout_movies(self, velocity_df: DataFrame) -> DataFrame:
        """Detect breakout movies with sudden popularity spikes."""
        
        # Look for movies with significant acceleration
        breakout_df = velocity_df \
            .filter(col("trend_acceleration") > 0.5) \
            .filter(col("trend_velocity") > 0.2) \
            .filter(col("total_review_volume") >= 3)
        
        # Calculate breakout score
        breakout_scored = breakout_df \
            .withColumn("breakout_score",
                       col("trend_acceleration") * 0.6 + 
                       col("trend_velocity") * 0.4) \
            .withColumn("surge_multiplier",
                       when(col("prev_trend_score").isNotNull() & (col("prev_trend_score") > 0),
                            col("avg_trend_score") / col("prev_trend_score"))
                       .otherwise(lit(1.0)))
        
        # Rank breakout movies
        window_spec = Window.partitionBy("window").orderBy(desc("breakout_score"))
        
        ranked_breakout = breakout_scored \
            .withColumn("breakout_rank", rank().over(window_spec)) \
            .filter(col("breakout_rank") <= 10)  # Top 10 breakouts
        
        result_df = ranked_breakout.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("movie_id"),
            col("title"),
            col("breakout_rank"),
            col("breakout_score"),
            col("surge_multiplier"),
            col("trend_acceleration"),
            col("trend_velocity"),
            col("avg_trend_score"),
            col("total_review_volume")
        )
        
        return result_df
    
    def detect_declining_movies(self, velocity_df: DataFrame) -> DataFrame:
        """Detect movies losing momentum (negative trending)."""
        
        # Look for movies with declining trends
        declining_df = velocity_df \
            .filter(col("trend_velocity") < -0.1) \
            .filter(col("avg_trend_score") > 5.0) \
            .filter(col("total_review_volume") >= 2)
        
        # Calculate decline score
        decline_scored = declining_df \
            .withColumn("decline_score",
                       abs(col("trend_velocity")) * 0.7 + 
                       abs(col("trend_acceleration")) * 0.3) \
            .withColumn("momentum_loss",
                       when(col("prev_trend_score").isNotNull(),
                            (col("prev_trend_score") - col("avg_trend_score")) / 
                            (col("prev_trend_score") + 1))
                       .otherwise(lit(0.0)))
        
        # Rank declining movies
        window_spec = Window.partitionBy("window").orderBy(desc("decline_score"))
        
        ranked_declining = decline_scored \
            .withColumn("decline_rank", rank().over(window_spec)) \
            .filter(col("decline_rank") <= 10)  # Top 10 declining
        
        result_df = ranked_declining.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("movie_id"),
            col("title"),
            col("decline_rank"),
            col("decline_score"),
            col("momentum_loss"),
            col("trend_velocity"),
            col("trend_acceleration"),
            col("avg_trend_score"),
            col("total_review_volume")
        )
        
        return result_df
    
    def write_to_cassandra(self, df: DataFrame, table_name: str, checkpoint_location: str):
        """Write trending data to Cassandra."""
        cassandra_config = self.config['cassandra']
        
        query = df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", cassandra_config['keyspace']) \
            .option("table", table_name) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("append") \
            .trigger(processingTime=self.config['processing']['trigger_interval'])
        
        return query.start()
    
    def write_to_console(self, df: DataFrame, query_name: str):
        """Write results to console for debugging."""
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .queryName(query_name) \
            .trigger(processingTime=self.config['processing']['trigger_interval'])
        
        return query.start()
    
    def run_streaming_pipeline(self, output_mode: str = "console",
                             checkpoint_base: str = "/tmp/spark-checkpoint-trending"):
        """Run the complete trending detection streaming pipeline."""
        
        logger.info("Starting trending detection streaming pipeline...")
        
        try:
            # Read trending stream
            trending_df = self.read_trending_stream()
            
            # Calculate velocity metrics
            velocity_df = self.calculate_trending_velocity(trending_df)
            
            # Detect different types of trends
            hot_movies = self.detect_hot_movies(velocity_df)
            breakout_movies = self.detect_breakout_movies(velocity_df)
            declining_movies = self.detect_declining_movies(velocity_df)
            
            queries = []
            
            # Start streaming queries
            if output_mode == "cassandra":
                queries.append(self.write_to_cassandra(
                    hot_movies, "trending_movies", f"{checkpoint_base}/hot"))
                queries.append(self.write_to_cassandra(
                    breakout_movies, "breakout_movies", f"{checkpoint_base}/breakout"))
                queries.append(self.write_to_cassandra(
                    declining_movies, "declining_movies", f"{checkpoint_base}/declining"))
            elif output_mode == "console":
                queries.append(self.write_to_console(hot_movies, "hot_movies"))
                queries.append(self.write_to_console(breakout_movies, "breakout_movies"))
                queries.append(self.write_to_console(declining_movies, "declining_movies"))
            else:
                raise ValueError(f"Unsupported output mode: {output_mode}")
            
            logger.info("Trending detection streaming pipeline started successfully")
            return queries
            
        except Exception as e:
            logger.error(f"Failed to start trending detection pipeline: {e}")
            raise
    
    def stop_streaming(self):
        """Stop all streaming queries and close Spark session."""
        logger.info("Stopping trending detection streaming pipeline...")
        
        # Stop all active streams
        for stream in self.spark.streams.active:
            stream.stop()
        
        # Close Spark session
        self.spark.stop()
        logger.info("Trending detection streaming pipeline stopped")


def main():
    """Main entry point for trending detection stream processing."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = None
    
    try:
        # Create and start processor
        processor = TrendingDetectionStreamProcessor()
        
        # Run streaming pipeline
        queries = processor.run_streaming_pipeline(output_mode="console")
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Trending detection pipeline failed: {e}")
        raise
    finally:
        if processor:
            processor.stop_streaming()


if __name__ == "__main__":
    main()