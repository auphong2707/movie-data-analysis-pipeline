"""
Review Sentiment Stream Processing
Real-time sentiment analysis using VADER on movie reviews from Kafka.
"""

import logging
import os
import sys
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    udf, struct, when, coalesce, lit, lag, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    LongType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Add config to path
sys.path.insert(0, '/app/config')
from config_loader import load_config

logger = logging.getLogger(__name__)


class ReviewSentimentStreamProcessor:
    """Process movie reviews for real-time sentiment analysis."""
    
    def __init__(self, config_path: str = "/app/config/spark_streaming_config.yaml"):
        """Initialize the sentiment stream processor."""
        
        # Load configuration with env var substitution
        self.config = load_config(config_path)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize VADER sentiment analyzer
        self.analyzer = SentimentIntensityAnalyzer()
        
        # Define schemas
        self.review_schema = self._get_review_schema()
        
        logger.info("Review Sentiment Stream Processor initialized")
    
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
                                "/app/checkpoints/sentiment")
        
        return builder.getOrCreate()
    
    def _get_review_schema(self) -> StructType:
        """Define schema for movie review messages."""
        return StructType([
            StructField("review_id", StringType(), False),
            StructField("movie_id", IntegerType(), False),
            StructField("author", StringType(), False),
            StructField("content", StringType(), False),
            StructField("rating", DoubleType(), True),
            StructField("created_at", LongType(), False),
            StructField("url", StringType(), False)
        ])
    
    def _create_sentiment_udf(self):
        """Create UDF for VADER sentiment analysis."""
        
        def analyze_sentiment(text: str) -> Dict[str, float]:
            """Analyze sentiment using VADER."""
            # Import and initialize analyzer inside UDF to avoid serialization issues
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            analyzer = SentimentIntensityAnalyzer()
            
            if not text or not isinstance(text, str):
                return {
                    'compound': 0.0,
                    'positive': 0.0,
                    'negative': 0.0,
                    'neutral': 0.0
                }
            
            try:
                scores = analyzer.polarity_scores(text)
                return {
                    'compound': float(scores['compound']),
                    'positive': float(scores['pos']),
                    'negative': float(scores['neg']),
                    'neutral': float(scores['neu'])
                }
            except Exception as e:
                # Can't use logger in UDF, just return default
                return {
                    'compound': 0.0,
                    'positive': 0.0,
                    'negative': 0.0,
                    'neutral': 0.0
                }
        
        # Define return type schema
        sentiment_schema = StructType([
            StructField("compound", DoubleType(), False),
            StructField("positive", DoubleType(), False),
            StructField("negative", DoubleType(), False),
            StructField("neutral", DoubleType(), False)
        ])
        
        return udf(analyze_sentiment, sentiment_schema)
    
    def read_review_stream(self) -> DataFrame:
        """Read movie review stream from Kafka."""
        kafka_config = self.config['kafka']
        
        # Read from Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("subscribe", kafka_config['topics']['reviews']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.review_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp to proper format
        timestamped_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp((col("created_at") / 1000).cast("long"))) \
            .withColumn("kafka_time", 
                       to_timestamp(col("kafka_timestamp")))
        
        return timestamped_df
    
    def process_sentiment_analysis(self, reviews_df: DataFrame) -> DataFrame:
        """Process reviews for sentiment analysis."""
        
        # Create sentiment UDF
        sentiment_udf = self._create_sentiment_udf()
        
        # Apply sentiment analysis
        sentiment_df = reviews_df \
            .withColumn("sentiment", sentiment_udf(col("content"))) \
            .select(
                col("review_id"),
                col("movie_id"),
                col("author"),
                col("content"),
                col("rating"),
                col("event_time"),
                col("sentiment.compound").alias("sentiment_compound"),
                col("sentiment.positive").alias("sentiment_positive"),
                col("sentiment.negative").alias("sentiment_negative"),
                col("sentiment.neutral").alias("sentiment_neutral")
            )
        
        # Add sentiment category
        sentiment_categorized = sentiment_df \
            .withColumn("sentiment_category",
                       when(col("sentiment_compound") >= 0.05, "positive")
                       .when(col("sentiment_compound") <= -0.05, "negative")
                       .otherwise("neutral"))
        
        return sentiment_categorized
    
    def aggregate_sentiment_by_window(self, sentiment_df: DataFrame) -> DataFrame:
        """
        Aggregate sentiment scores by 5-minute tumbling window.
        Matches Cassandra review_sentiments table schema.
        """
        window_duration = self.config['processing']['window_duration']
        watermark_delay = self.config['processing']['watermark_delay']
        
        # Add watermark for late data handling
        watermarked_df = sentiment_df \
            .withWatermark("event_time", watermark_delay)
        
        # Aggregate by movie_id and 5-minute window
        aggregated_df = watermarked_df \
            .groupBy(
                window(col("event_time"), window_duration),
                col("movie_id")
            ) \
            .agg(
                avg("sentiment_compound").alias("avg_sentiment"),
                count("review_id").alias("review_count"),
                spark_sum(when(col("sentiment_category") == "positive", 1).otherwise(0)).alias("positive_count"),
                spark_sum(when(col("sentiment_category") == "negative", 1).otherwise(0)).alias("negative_count"),
                spark_sum(when(col("sentiment_category") == "neutral", 1).otherwise(0)).alias("neutral_count")
            )
        
        # Note: sentiment_velocity calculation using lag() is not supported in Spark Structured Streaming
        # Window functions over non-time columns are not allowed
        # Setting sentiment_velocity to 0.0 for now
        
        result_df = aggregated_df \
            .select(
                col("movie_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                # Hour for partition key (truncate to hour)
                expr("date_trunc('hour', window.start)").alias("hour"),
                col("avg_sentiment"),
                col("review_count").cast("int"),
                col("positive_count").cast("int"),
                col("negative_count").cast("int"),
                col("neutral_count").cast("int"),
                lit(0.0).alias("sentiment_velocity")  # Placeholder - lag() not supported in streaming
            )
        
        return result_df
    
    def write_batch_to_cassandra(self, batch_df: DataFrame, batch_id: int):
        """Write a microbatch to Cassandra using foreachBatch.
        
        This approach treats each microbatch as a normal DataFrame, allowing:
        - Standard batch operations (no streaming aggregation restrictions)
        - Proper Cassandra upserts (append overwrites same primary key)
        - TTL-based auto-expiration (48 hours)
        """
        if batch_df.count() > 0:
            cassandra_config = self.config['spark']['cassandra']
            
            logger.info(f"Writing batch {batch_id} with {batch_df.count()} rows to review_sentiments")
            
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .option("keyspace", cassandra_config['keyspace']) \
                .option("table", "review_sentiments") \
                .save()
    
    def write_to_cassandra(self, df: DataFrame, checkpoint_location: str):
        """Write aggregated sentiment data to Cassandra using foreachBatch."""
        
        cassandra_config = self.config['spark']['cassandra']
        
        # Use foreachBatch to process each microbatch as a regular DataFrame
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: self.write_batch_to_cassandra(batch_df, batch_id)) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=self.config['processing']['trigger_interval'])
        
        return query.start()
    
    def write_to_console(self, df: DataFrame, query_name: str = "review_sentiment"):
        """Write results to console for debugging."""
        
        query = df.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .queryName(query_name) \
            .trigger(processingTime=self.config['processing']['trigger_interval'])
        
        return query.start()
    
    def run_streaming_pipeline(self, output_mode: str = "cassandra", 
                             checkpoint_location: str = "/app/checkpoints/review_sentiment"):
        """Run the complete streaming pipeline with new schema."""
        
        logger.info("Starting review sentiment streaming pipeline...")
        
        try:
            # Read review stream
            reviews_df = self.read_review_stream()
            
            # Process sentiment analysis
            sentiment_df = self.process_sentiment_analysis(reviews_df)
            
            # Aggregate by windows
            aggregated_df = self.aggregate_sentiment_by_window(sentiment_df)
            
            # Write output
            if output_mode == "cassandra":
                query = self.write_to_cassandra(aggregated_df, checkpoint_location)
            elif output_mode == "console":
                query = self.write_to_console(aggregated_df)
            else:
                raise ValueError(f"Unsupported output mode: {output_mode}")
            
            logger.info("Review sentiment streaming pipeline started successfully")
            return query
            
        except Exception as e:
            logger.error(f"Failed to start sentiment streaming pipeline: {e}")
            raise
    
    def stop_streaming(self):
        """Stop all streaming queries and close Spark session."""
        logger.info("Stopping sentiment streaming pipeline...")
        
        # Stop all active streams
        for stream in self.spark.streams.active:
            stream.stop()
        
        # Close Spark session
        self.spark.stop()
        logger.info("Sentiment streaming pipeline stopped")


def main():
    """Main entry point for review sentiment stream processing."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = None
    
    try:
        # Create and start processor
        processor = ReviewSentimentStreamProcessor()
        
        # Run streaming pipeline with Cassandra output
        query = processor.run_streaming_pipeline(output_mode="cassandra")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Sentiment streaming pipeline failed: {e}")
        raise
    finally:
        if processor:
            processor.stop_streaming()


if __name__ == "__main__":
    main()