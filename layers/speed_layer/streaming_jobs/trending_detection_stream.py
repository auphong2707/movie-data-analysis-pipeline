"""
Trending Detection Stream Processing
Detect trending/hot movies using velocity and acceleration metrics.
Consumes from movie.ratings topic and ranks top 100 movies.
"""

import logging
import os
import sys
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, stddev, first, last, 
    lag, lead, coalesce, lit, expr, when, rank, dense_rank, desc, asc, abs as spark_abs,
    row_number, current_timestamp
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
    """Detect trending movies using real-time velocity and acceleration analysis from movie.ratings."""
    
    def __init__(self, config_path: str = "/app/config/spark_streaming_config.yaml"):
        """Initialize the trending detection stream processor."""
        
        # Load configuration with env var substitution
        self.config = load_config(config_path)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Define rating schema
        self.rating_schema = self._get_rating_schema()
        
        # Trending detection parameters
        self.trending_params = {
            'min_popularity_threshold': 10.0,
            'min_vote_count': 10,
            'top_n_trending': 100,
            'velocity_weight': 0.5,
            'acceleration_weight': 0.5
        }
        
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
        
        # Add Kafka and Cassandra packages
        # Versions aligned with PySpark 3.4.4 (installed in Dockerfile)
        # spark-sql-kafka: 3.4.4 matches PySpark version
        # spark-cassandra-connector: 3.4.1 is latest stable for Cassandra 4.x
        # kafka-clients: 3.4.0 compatible with Kafka 7.4.x (Confluent)
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4",
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
    
    def _get_rating_schema(self) -> StructType:
        """Define schema for movie rating messages."""
        return StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("vote_average", DoubleType(), False),
            StructField("vote_count", IntegerType(), False),
            StructField("popularity", DoubleType(), False),
            StructField("timestamp", LongType(), False)
        ])
    
    def read_ratings_stream(self) -> DataFrame:
        """Read movie ratings stream from Kafka."""
        kafka_config = self.config['kafka']
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("subscribe", kafka_config['topics']['ratings']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.rating_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("timestamp") / 1000).cast("long")))
        
        return timestamped_df
    
    def calculate_trending_scores(self, ratings_df: DataFrame) -> DataFrame:
        """
        Calculate trending scores with velocity and acceleration.
        Rank top 100 movies by trending score.
        Matches Cassandra trending_movies table schema.
        """
        watermark_delay = self.config['processing']['watermark_delay']
        params = self.trending_params
        
        # Add watermark
        watermarked_df = ratings_df.withWatermark("event_time", watermark_delay)
        
        # Group by hour and movie_id
        hourly_df = watermarked_df \
            .withColumn("hour", expr("date_trunc('hour', event_time)")) \
            .groupBy("hour", "movie_id") \
            .agg(
                last("vote_average").alias("vote_average"),
                last("vote_count").alias("vote_count"),
                last("popularity").alias("popularity"),
                max("event_time").alias("last_updated")
            )
        
        # Calculate velocity and acceleration using window functions
        window_spec = Window.partitionBy("movie_id").orderBy("hour")
        
        velocity_df = hourly_df \
            .withColumn("prev_popularity", lag("popularity", 1).over(window_spec)) \
            .withColumn("velocity",
                       when(col("prev_popularity").isNotNull(),
                            col("popularity") - col("prev_popularity"))
                       .otherwise(lit(0.0))) \
            .withColumn("prev_velocity", lag("velocity", 1).over(window_spec)) \
            .withColumn("acceleration",
                       when(col("prev_velocity").isNotNull(),
                            col("velocity") - col("prev_velocity"))
                       .otherwise(lit(0.0)))
        
        # Calculate trending score
        trending_df = velocity_df \
            .withColumn("trending_score",
                       (spark_abs(col("velocity")) * params['velocity_weight']) +
                       (spark_abs(col("acceleration")) * params['acceleration_weight'])) \
            .filter(col("popularity") >= params['min_popularity_threshold']) \
            .filter(col("vote_count") >= params['min_vote_count'])
        
        # Rank top 100 movies per hour
        rank_window = Window.partitionBy("hour").orderBy(desc("trending_score"))
        
        result_df = trending_df \
            .withColumn("rank", row_number().over(rank_window)) \
            .filter(col("rank") <= params['top_n_trending']) \
            .select(
                col("hour"),
                col("rank"),
                col("movie_id"),
                lit("Unknown").alias("title"),  # Title will be enriched from metadata
                col("trending_score"),
                col("velocity"),
                col("acceleration")
            )
        
        return result_df
        """Read movie statistics from Cassandra for trending analysis."""
        cassandra_config = self.config['spark']['cassandra']
        
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
                spark_sum("review_volume").alias("total_review_volume"),
                count("movie_id").alias("event_count"),
                spark_max("trend_score").alias("max_trend_score")
            )
        
        # Calculate velocity within current window (no cross-window lag in streaming)
        # Use max_trend_score range and average as proxy for velocity
        velocity_df = current_metrics \
            .withColumn("trend_velocity",
                       when(col("event_count") > 1,
                            (col("max_trend_score") - col("avg_trend_score")) / col("event_count"))
                       .otherwise(lit(0.0))) \
            .withColumn("trend_acceleration",
                       # Approximate acceleration using popularity delta and rating velocity
                       (spark_abs(col("avg_popularity_delta")) + spark_abs(col("avg_rating_velocity"))) / 2.0)
        
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
        
        # Add ranking using row_number() which IS supported in streaming
        # Partition by window and order by hotness_score descending
        window_spec = Window.partitionBy("window.start", "window.end").orderBy(col("hotness_score").desc())
        
        ranked_df = filtered_df \
            .withColumn("trend_rank", row_number().over(window_spec))
        
        # Select final columns with trend_rank and created_at timestamp
        result_df = ranked_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("trend_rank"),
            col("movie_id"),
            col("title"),
            col("hotness_score"),
            col("avg_trend_score"),
            col("trend_velocity"),
            col("trend_acceleration"),
            col("total_review_volume"),
            col("event_count"),
            col("velocity_score"),
            col("acceleration_score"),
            col("volume_score"),
            current_timestamp().alias("created_at")
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
                       # Use ratio of max to avg as proxy for surge
                       when(col("avg_trend_score") > 0,
                            col("max_trend_score") / col("avg_trend_score"))
                       .otherwise(lit(1.0)))
        
        # Add ranking using row_number() - partition by window, order by breakout_score
        window_spec = Window.partitionBy("window.start", "window.end").orderBy(col("breakout_score").desc())
        
        ranked_df = breakout_scored \
            .withColumn("breakout_rank", row_number().over(window_spec))
        
        # Select breakout movies with ranking and timestamp
        result_df = ranked_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("breakout_rank"),
            col("movie_id"),
            col("title"),
            col("breakout_score"),
            col("surge_multiplier"),
            col("trend_acceleration"),
            col("trend_velocity"),
            col("avg_trend_score"),
            col("total_review_volume"),
            current_timestamp().alias("created_at")
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
                       spark_abs(col("trend_velocity")) * 0.7 + 
                       spark_abs(col("trend_acceleration")) * 0.3) \
            .withColumn("momentum_loss",
                       # Use negative velocity magnitude as momentum loss indicator
                       spark_abs(col("trend_velocity")))
        
        # Add ranking using row_number() - partition by window, order by decline_score
        window_spec = Window.partitionBy("window.start", "window.end").orderBy(col("decline_score").desc())
        
        ranked_df = decline_scored \
            .withColumn("decline_rank", row_number().over(window_spec))
        
        # Select declining movies with ranking and timestamp
        result_df = ranked_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("decline_rank"),
            col("movie_id"),
            col("title"),
            col("decline_score"),
            col("momentum_loss"),
            col("trend_velocity"),
            col("trend_acceleration"),
            col("avg_trend_score"),
            col("total_review_volume"),
            current_timestamp().alias("created_at")
        )
        
        return result_df
    
    def write_to_cassandra(self, df: DataFrame, table_name: str, checkpoint_location: str):
        """Write trending data to Cassandra."""
        cassandra_config = self.config['spark']['cassandra']
        
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
    
    def run_streaming_pipeline(self, output_mode: str = "cassandra",
                             checkpoint_base: str = "/tmp/checkpoints/trending_detection"):
        """Run the trending detection streaming pipeline with new schema."""
        
        logger.info("Starting trending detection streaming pipeline...")
        
        try:
            # Read ratings stream
            ratings_df = self.read_ratings_stream()
            
            # Calculate trending scores
            trending_df = self.calculate_trending_scores(ratings_df)
            
            queries = []
            
            # Write to Cassandra or console
            if output_mode == "cassandra":
                query = self.write_to_cassandra(
                    trending_df,
                    "trending_movies",
                    f"{checkpoint_base}/trending"
                )
                queries.append(query)
                logger.info("Started writing to Cassandra table: trending_movies")
                
            elif output_mode == "console":
                query = self.write_to_console(trending_df, "trending_movies")
                queries.append(query)
                logger.info("Started writing to console")
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
        
        # Run streaming pipeline with Cassandra output
        queries = processor.run_streaming_pipeline(output_mode="cassandra")
        
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