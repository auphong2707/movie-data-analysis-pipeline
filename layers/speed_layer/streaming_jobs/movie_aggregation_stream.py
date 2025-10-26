"""
Movie Aggregation Stream Processing
Real-time aggregation of movie metrics including ratings, popularity, and velocity calculations.
"""

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, stddev, first, last, 
    lag, lead, coalesce, lit, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    LongType, TimestampType, ArrayType, BooleanType
)
from pyspark.sql.window import Window
import yaml

logger = logging.getLogger(__name__)


class MovieAggregationStreamProcessor:
    """Process movie data for real-time aggregations and metrics."""
    
    def __init__(self, config_path: str = "config/spark_streaming_config.yaml"):
        """Initialize the movie aggregation stream processor."""
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
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
        
        # Add packages
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
        ]
        builder = builder.config("spark.jars.packages", ",".join(packages))
        
        return builder.getOrCreate()
    
    def _get_metadata_schema(self) -> StructType:
        """Define schema for movie metadata messages."""
        return StructType([
            StructField("movie_id", LongType(), False),
            StructField("title", StringType(), False),
            StructField("release_date", StringType(), True),
            StructField("popularity", DoubleType(), False),
            StructField("vote_average", DoubleType(), False),
            StructField("vote_count", LongType(), False),
            StructField("adult", BooleanType(), True),
            StructField("genre_ids", ArrayType(IntegerType()), True),
            StructField("overview", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("backdrop_path", StringType(), True),
            StructField("original_language", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("video", BooleanType(), True),
            StructField("timestamp", LongType(), False)
        ])
    
    def _get_rating_schema(self) -> StructType:
        """Define schema for movie rating messages."""
        return StructType([
            StructField("movie_id", LongType(), False),
            StructField("user_id", StringType(), False),
            StructField("rating", DoubleType(), False),
            StructField("timestamp", LongType(), False),
            StructField("source", StringType(), True)
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
        
        # Convert timestamp
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("timestamp") / 1000).cast("long")))
        
        return timestamped_df
    
    def calculate_movie_velocity_metrics(self, metadata_df: DataFrame) -> DataFrame:
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
        
        # Calculate velocity using window functions
        movie_window = Window.partitionBy("movie_id").orderBy("window")
        
        velocity_df = current_window \
            .withColumn("prev_popularity", 
                       lag("avg_popularity", 1).over(movie_window)) \
            .withColumn("popularity_velocity",
                       when(col("prev_popularity").isNotNull(),
                            (col("avg_popularity") - col("prev_popularity")) / col("avg_popularity"))
                       .otherwise(lit(0.0))) \
            .withColumn("popularity_acceleration",
                       lag("popularity_velocity", 1).over(movie_window) - col("popularity_velocity"))
        
        # Select final columns
        result_df = velocity_df.select(
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
            coalesce(col("popularity_velocity"), lit(0.0)).alias("popularity_velocity"),
            coalesce(col("popularity_acceleration"), lit(0.0)).alias("popularity_acceleration")
        )
        
        return result_df
    
    def calculate_rating_aggregations(self, ratings_df: DataFrame) -> DataFrame:
        """Calculate rating aggregations by time windows."""
        
        window_duration = self.config['processing']['window_duration']
        watermark_delay = self.config['processing']['watermark_delay']
        
        # Add watermark
        watermarked_df = ratings_df \
            .withWatermark("event_time", watermark_delay)
        
        # Window aggregation
        windowed_df = watermarked_df \
            .groupBy(
                window(col("event_time"), window_duration),
                col("movie_id")
            ) \
            .agg(
                count("rating").alias("rating_count"),
                avg("rating").alias("avg_rating"),
                spark_min("rating").alias("min_rating"),
                spark_max("rating").alias("max_rating"),
                stddev("rating").alias("rating_stddev"),
                # Calculate rating distribution
                count(when(col("rating") >= 8.0, 1)).alias("high_ratings_count"),
                count(when((col("rating") >= 6.0) & (col("rating") < 8.0), 1)).alias("medium_ratings_count"),
                count(when(col("rating") < 6.0, 1)).alias("low_ratings_count")
            )
        
        # Calculate rating velocity
        movie_window = Window.partitionBy("movie_id").orderBy("window")
        
        velocity_df = windowed_df \
            .withColumn("prev_avg_rating",
                       lag("avg_rating", 1).over(movie_window)) \
            .withColumn("rating_velocity",
                       when(col("prev_avg_rating").isNotNull(),
                            col("avg_rating") - col("prev_avg_rating"))
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
        
        # Join movie and rating metrics
        joined_df = movie_metrics_df.alias("m") \
            .join(rating_metrics_df.alias("r"),
                  (col("m.movie_id") == col("r.movie_id")) &
                  (col("m.window_start") == col("r.window_start")),
                  "full_outer")
        
        # Calculate composite trend score
        trend_df = joined_df \
            .withColumn("movie_id", coalesce(col("m.movie_id"), col("r.movie_id"))) \
            .withColumn("window_start", coalesce(col("m.window_start"), col("r.window_start"))) \
            .withColumn("window_end", coalesce(col("m.window_end"), col("r.window_end"))) \
            .withColumn("title", coalesce(col("m.title"), lit("Unknown"))) \
            .withColumn("popularity_score",
                       coalesce(col("m.popularity_velocity"), lit(0.0)) * 0.4 +
                       coalesce(col("m.avg_popularity"), lit(0.0)) * 0.3 +
                       coalesce(col("m.update_count"), lit(0.0)) * 0.3) \
            .withColumn("rating_score",
                       coalesce(col("r.rating_velocity"), lit(0.0)) * 0.5 +
                       coalesce(col("r.avg_rating"), lit(0.0)) * 0.3 +
                       coalesce(col("r.rating_count"), lit(0.0)) * 0.2) \
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
                coalesce(col("m.avg_popularity"), lit(0.0)).alias("popularity"),
                coalesce(col("r.avg_rating"), lit(0.0)).alias("avg_rating"),
                coalesce(col("r.rating_count"), lit(0.0)).alias("rating_count"),
                coalesce(col("m.popularity_velocity"), lit(0.0)).alias("popularity_velocity"),
                coalesce(col("r.rating_velocity"), lit(0.0)).alias("rating_velocity")
            )
        
        return trending_df
    
    def write_to_cassandra(self, df: DataFrame, table_name: str, checkpoint_location: str):
        """Write data to Cassandra."""
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
                             checkpoint_base: str = "/tmp/spark-checkpoint-aggregation"):
        """Run the complete movie aggregation streaming pipeline."""
        
        logger.info("Starting movie aggregation streaming pipeline...")
        
        try:
            # Read streams
            metadata_df = self.read_metadata_stream()
            ratings_df = self.read_ratings_stream()
            
            # Process aggregations
            movie_metrics = self.calculate_movie_velocity_metrics(metadata_df)
            rating_metrics = self.calculate_rating_aggregations(ratings_df)
            trending_movies = self.detect_trending_movies(movie_metrics, rating_metrics)
            
            queries = []
            
            # Start streaming queries
            if output_mode == "cassandra":
                queries.append(self.write_to_cassandra(
                    movie_metrics, "movie_stats", f"{checkpoint_base}/movie_stats"))
                queries.append(self.write_to_cassandra(
                    rating_metrics, "movie_ratings_by_window", f"{checkpoint_base}/ratings"))
                queries.append(self.write_to_cassandra(
                    trending_movies, "trending_movies", f"{checkpoint_base}/trending"))
            elif output_mode == "console":
                queries.append(self.write_to_console(movie_metrics, "movie_metrics"))
                queries.append(self.write_to_console(rating_metrics, "rating_metrics"))
                queries.append(self.write_to_console(trending_movies, "trending_movies"))
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
        
        # Run streaming pipeline
        queries = processor.run_streaming_pipeline(output_mode="console")
        
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