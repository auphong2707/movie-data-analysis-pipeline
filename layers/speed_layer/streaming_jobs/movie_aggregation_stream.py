"""
Movie Aggregation Stream Processing
Real-time aggregation of movie metrics including ratings, popularity, and velocity calculations.
"""

import logging
import os
import sys
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, stddev, first, last, 
    lag, lead, coalesce, lit, expr, when, least, log10, current_timestamp
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


class MovieAggregationStreamProcessor:
    """Process movie data for real-time aggregations and metrics."""
    
    def __init__(self, config_path: str = "/app/config/spark_streaming_config.yaml"):
        """Initialize the movie aggregation stream processor."""
        
        # Load configuration with env var substitution
        self.config = load_config(config_path)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Define schemas
        self.metadata_schema = self._get_metadata_schema()
        self.rating_schema = self._get_rating_schema()
        
        logger.info("Movie Aggregation Stream Processor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with appropriate configuration."""
        spark_config = self.config['spark']
        
        builder = SparkSession.builder \
            .appName(spark_config['app_name']) \
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
                                "/app/checkpoints/aggregation")
        
        return builder.getOrCreate()
    
    def _get_metadata_schema(self) -> StructType:
        """Define schema for movie metadata messages."""
        return StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("release_date", StringType(), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("runtime", IntegerType(), True),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("timestamp", LongType(), False),
            StructField("event_type", StringType(), False)
        ])
    
    def _get_rating_schema(self) -> StructType:
        """Define schema for movie rating messages."""
        return StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("vote_average", DoubleType(), False),
            StructField("vote_count", IntegerType(), False),
            StructField("popularity", DoubleType(), False),
            StructField("timestamp", LongType(), False)
        ])
    
    def read_metadata_stream(self) -> DataFrame:
        """Read movie metadata stream from Kafka."""
        kafka_config = self.config['kafka']
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("subscribe", kafka_config['topics']['metadata']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.metadata_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("timestamp") / 1000).cast("long")))
        
        return timestamped_df
    
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
        
        # Convert timestamp - use TMDB vote_average directly as the rating
        # No synthetic calculation needed, TMDB provides authoritative ratings
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("timestamp") / 1000).cast("long")))
        
        return timestamped_df
    
    def join_and_aggregate_movie_stats(self, ratings_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
        """
        Join ratings and metadata streams, compute incremental aggregations.
        Matches Cassandra movie_stats table schema.
        """
        # Add watermarks to both streams
        watermark_delay = self.config['processing']['watermark_delay']
        
        ratings_watermarked = ratings_df.withWatermark("event_time", watermark_delay)
        metadata_watermarked = metadata_df.withWatermark("event_time", watermark_delay)
        
        # Join on movie_id with time constraints (within 5 minutes)
        joined_df = ratings_watermarked.alias("r").join(
            metadata_watermarked.alias("m"),
            (col("r.movie_id") == col("m.movie_id")) & 
            (col("r.event_time") >= col("m.event_time") - expr("INTERVAL 5 MINUTES")) &
            (col("r.event_time") <= col("m.event_time") + expr("INTERVAL 5 MINUTES")),
            "inner"
        ).select(
            col("r.movie_id"),
            col("r.vote_average"),
            col("r.vote_count"),
            col("r.popularity"),
            col("r.event_time"),
            col("m.title")
        )
        
        # Aggregate by hour (truncate to hour for partition key)
        aggregated_df = joined_df \
            .withColumn("hour", expr("date_trunc('hour', event_time)")) \
            .groupBy("movie_id", "hour") \
            .agg(
                last("vote_average").alias("vote_average"),
                last("vote_count").alias("vote_count"),
                last("popularity").alias("popularity"),
                last("title").alias("title"),
                spark_max(col("event_time")).alias("last_updated")
            )
        
        # Calculate rating velocity using window functions
        window_spec = Window.partitionBy("movie_id").orderBy("hour")
        
        result_df = aggregated_df \
            .withColumn("prev_vote_average", lag("vote_average", 1).over(window_spec)) \
            .withColumn("rating_velocity",
                       when(col("prev_vote_average").isNotNull(),
                            col("vote_average") - col("prev_vote_average"))
                       .otherwise(lit(0.0))) \
            .select(
                col("movie_id"),
                col("hour"),
                col("vote_average"),
                col("vote_count").cast("int"),
                col("popularity"),
                col("rating_velocity"),
                col("last_updated")
            )
        
        return result_df
        """Calculate velocity metrics for movies (popularity change rate, etc.)."""
        
        window_duration = self.config['processing']['window_duration']
        watermark_delay = self.config['processing']['watermark_delay']
        
        # Add watermark
        watermarked_df = metadata_df \
            .withWatermark("event_time", watermark_delay)
        
        # Window aggregation for current metrics
        current_window = watermarked_df \
            .groupBy(
                window(col("event_time"), window_duration),
                col("movie_id")
            ) \
            .agg(
                first("title").alias("title"),
                avg("popularity").alias("avg_popularity"),
                avg("vote_average").alias("avg_vote_average"),
                avg("vote_count").alias("avg_vote_count"),
                count("movie_id").alias("update_count"),
                spark_min("popularity").alias("min_popularity"),
                spark_max("popularity").alias("max_popularity"),
                stddev("popularity").alias("popularity_stddev")
            )
        
        # For streaming, calculate simple velocity within the window
        # Velocity = (max - min) / avg gives a relative change measure
        result_df = current_window.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("movie_id"),
            col("title"),
            col("avg_popularity"),
            col("avg_vote_average"),
            col("avg_vote_count"),
            col("update_count"),
            col("min_popularity"),
            col("max_popularity"),
            coalesce(col("popularity_stddev"), lit(0.0)).alias("popularity_stddev"),
            # Simple velocity approximation: range / average
            when(col("avg_popularity") > 0,
                 (col("max_popularity") - col("min_popularity")) / col("avg_popularity"))
            .otherwise(lit(0.0)).alias("popularity_velocity"),
            # Acceleration approximated by stddev/avg (volatility measure)
            when(col("avg_popularity") > 0,
                 coalesce(col("popularity_stddev"), lit(0.0)) / col("avg_popularity"))
            .otherwise(lit(0.0)).alias("popularity_acceleration")
        )
        
        return result_df
    
    def calculate_rating_aggregations(self, ratings_df: DataFrame) -> DataFrame:
        """Calculate rating aggregations by time windows."""
        
        window_duration = self.config['processing']['window_duration']
        watermark_delay = self.config['processing']['watermark_delay']
        
        # Add watermark
        watermarked_df = ratings_df \
            .withWatermark("event_time", watermark_delay)
        
        # Window aggregation - use TMDB vote_average field directly
        windowed_df = watermarked_df \
            .groupBy(
                window(col("event_time"), window_duration),
                col("movie_id")
            ) \
            .agg(
                count("vote_average").alias("rating_count"),
                avg("vote_average").alias("avg_rating"),
                spark_min("vote_average").alias("min_rating"),
                spark_max("vote_average").alias("max_rating"),
                stddev("vote_average").alias("rating_stddev"),
                # Calculate rating distribution based on TMDB scale (0-10)
                count(when(col("vote_average") >= 8.0, 1)).alias("high_ratings_count"),
                count(when((col("vote_average") >= 6.0) & (col("vote_average") < 8.0), 1)).alias("medium_ratings_count"),
                count(when(col("vote_average") < 6.0, 1)).alias("low_ratings_count")
            )
        
        # Calculate rating velocity within the window (range-based approximation)
        # Use rating range and stddev as proxy for volatility/velocity
        velocity_df = windowed_df \
            .withColumn("rating_range", col("max_rating") - col("min_rating")) \
            .withColumn("rating_velocity",
                       when(col("rating_count") > 1,
                            col("rating_range") / col("rating_count"))
                       .otherwise(lit(0.0)))
        
        # Format output
        result_df = velocity_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("movie_id"),
            col("rating_count"),
            coalesce(col("avg_rating"), lit(0.0)).alias("avg_rating"),
            coalesce(col("min_rating"), lit(0.0)).alias("min_rating"),
            coalesce(col("max_rating"), lit(0.0)).alias("max_rating"),
            coalesce(col("rating_stddev"), lit(0.0)).alias("rating_stddev"),
            col("high_ratings_count"),
            col("medium_ratings_count"),
            col("low_ratings_count"),
            coalesce(col("rating_velocity"), lit(0.0)).alias("rating_velocity")
        )
        
        return result_df
    
    def detect_trending_movies(self, movie_metrics_df: DataFrame, 
                             rating_metrics_df: DataFrame) -> DataFrame:
        """Detect trending movies by combining popularity and rating metrics."""
        
        # Use inner join - only detect trending for movies with both metrics
        # This is more reliable for streaming than full outer join
        joined_df = movie_metrics_df.alias("m") \
            .join(rating_metrics_df.alias("r"),
                  (col("m.movie_id") == col("r.movie_id")) &
                  (col("m.window_start") == col("r.window_start")),
                  "inner")
        
        # Calculate composite trend score
        # Use select to avoid ambiguous column references
        trend_df = joined_df \
            .select(
                coalesce(col("m.movie_id"), col("r.movie_id")).alias("movie_id"),
                coalesce(col("m.window_start"), col("r.window_start")).alias("window_start"),
                coalesce(col("m.window_end"), col("r.window_end")).alias("window_end"),
                coalesce(col("m.title"), lit("Unknown")).alias("title"),
                coalesce(col("m.popularity_velocity"), lit(0.0)).alias("popularity_velocity"),
                coalesce(col("m.avg_popularity"), lit(0.0)).alias("avg_popularity"),
                coalesce(col("m.update_count"), lit(0.0)).alias("update_count"),
                coalesce(col("r.rating_velocity"), lit(0.0)).alias("rating_velocity"),
                coalesce(col("r.avg_rating"), lit(0.0)).alias("avg_rating"),
                coalesce(col("r.rating_count"), lit(0.0)).alias("rating_count")
            ) \
            .withColumn("popularity_score",
                       col("popularity_velocity") * 0.4 +
                       col("avg_popularity") * 0.3 +
                       col("update_count") * 0.3) \
            .withColumn("rating_score",
                       col("rating_velocity") * 0.5 +
                       col("avg_rating") * 0.3 +
                       col("rating_count") * 0.2) \
            .withColumn("trend_score",
                       col("popularity_score") * 0.6 + col("rating_score") * 0.4)
        
        # Select trending movies (top trend scores)
        trending_df = trend_df \
            .filter(col("trend_score") > 0) \
            .select(
                col("window_start"),
                col("window_end"), 
                col("movie_id"),
                col("title"),
                col("trend_score"),
                col("avg_popularity").alias("popularity"),
                col("avg_rating"),
                col("rating_count"),
                col("popularity_velocity"),
                col("rating_velocity")
            )
        
        return trending_df
    
    def write_to_cassandra(self, df: DataFrame, table_name: str, checkpoint_location: str):
        """Write data to Cassandra."""
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
    
    def write_to_kafka(self, df: DataFrame, topic: str, checkpoint_location: str):
        """Write streaming results to Kafka topic."""
        kafka_config = self.config['kafka']
        
        # Convert DataFrame to JSON string for Kafka value
        kafka_df = df.selectExpr("CAST(movie_id AS STRING) as key", "to_json(struct(*)) AS value")
        
        query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("append") \
            .trigger(processingTime=self.config['processing']['trigger_interval'])
        
        return query.start()
    
    def run_streaming_pipeline(self, output_mode: str = "cassandra",
                             checkpoint_base: str = "/app/checkpoints/movie_aggregation"):
        """Run the movie aggregation streaming pipeline with new schema."""
        
        logger.info("Starting movie aggregation streaming pipeline...")
        
        try:
            # Read streams
            metadata_df = self.read_metadata_stream()
            ratings_df = self.read_ratings_stream()
            
            # Join and aggregate
            movie_stats_df = self.join_and_aggregate_movie_stats(ratings_df, metadata_df)
            
            queries = []
            
            # Write to Cassandra or console
            if output_mode == "cassandra":
                query = self.write_to_cassandra(
                    movie_stats_df, 
                    "movie_stats", 
                    f"{checkpoint_base}/movie_stats"
                )
                queries.append(query)
                logger.info("Started writing to Cassandra table: movie_stats")
                
            elif output_mode == "console":
                query = self.write_to_console(movie_stats_df, "movie_aggregation")
                queries.append(query)
                logger.info("Started writing to console")
            
            else:
                raise ValueError(f"Unsupported output mode: {output_mode}")
            
            logger.info("Movie aggregation streaming pipeline started successfully")
            return queries
            
        except Exception as e:
            logger.error(f"Failed to start aggregation streaming pipeline: {e}")
            raise
    
    def stop_streaming(self):
        """Stop all streaming queries and close Spark session."""
        logger.info("Stopping aggregation streaming pipeline...")
        
        # Stop all active streams
        for stream in self.spark.streams.active:
            stream.stop()
        
        # Close Spark session
        self.spark.stop()
        logger.info("Aggregation streaming pipeline stopped")


def main():
    """Main entry point for movie aggregation stream processing."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = None
    
    try:
        # Create and start processor
        processor = MovieAggregationStreamProcessor()
        
        # Run streaming pipeline with Cassandra output
        queries = processor.run_streaming_pipeline(output_mode="cassandra")
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Aggregation streaming pipeline failed: {e}")
        raise
    finally:
        if processor:
            processor.stop_streaming()


if __name__ == "__main__":
    main()