"""
Reddit Sentiment Streaming Job

Consumes Reddit posts and comments from Kafka, performs real-time sentiment analysis
and viral metric calculation, then writes results to Cassandra speed views.

Processing:
- 5-minute tumbling windows
- VADER sentiment analysis on post titles + comments
- Viral metrics: upvote velocity, comment acceleration, award velocity
- Reddit-to-TMDB movie matching

Usage:
    spark-submit reddit_sentiment_stream.py
"""

import os
import sys
import logging
from datetime import datetime, timezone
from typing import List, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, current_timestamp, lit, when, explode, udf, collect_list,
    broadcast, date_trunc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, ArrayType, FloatType, TimestampType
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define Reddit post schema
REDDIT_POST_SCHEMA = StructType([
    StructField("post_id", StringType(), False),
    StructField("subreddit", StringType(), False),
    StructField("title", StringType(), False),
    StructField("selftext", StringType(), True),
    StructField("author", StringType(), False),
    StructField("created_utc", DoubleType(), False),
    StructField("upvotes", IntegerType(), False),
    StructField("upvote_ratio", FloatType(), False),
    StructField("num_comments", IntegerType(), False),
    StructField("awards", IntegerType(), False),
    StructField("url", StringType(), False),
    StructField("is_self", BooleanType(), False),
    StructField("potential_movies", ArrayType(StringType()), False),
    StructField("fetched_at", StringType(), False)
])

# Define Reddit comment schema
REDDIT_COMMENT_SCHEMA = StructType([
    StructField("comment_id", StringType(), False),
    StructField("post_id", StringType(), False),
    StructField("subreddit", StringType(), False),
    StructField("body", StringType(), False),
    StructField("author", StringType(), False),
    StructField("created_utc", DoubleType(), False),
    StructField("upvotes", IntegerType(), False),
    StructField("awards", IntegerType(), False),
    StructField("parent_id", StringType(), False),
    StructField("is_submitter", BooleanType(), False),
    StructField("potential_movies", ArrayType(StringType()), False),
    StructField("fetched_at", StringType(), False)
])


class RedditSentimentStreaming:
    """Real-time sentiment analysis and viral metrics from Reddit stream."""
    
    def __init__(self, kafka_bootstrap_servers: str, cassandra_host: str):
        """
        Initialize Spark streaming job.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            cassandra_host: Cassandra host address
        """
        self.kafka_bootstrap = kafka_bootstrap_servers
        self.cassandra_host = cassandra_host
        
        # Initialize Spark session with Cassandra connector
        logger.info("Initializing Spark session...")
        self.spark = SparkSession.builder \
            .appName("RedditSentimentStream") \
            .config("spark.sql.streaming.checkpointLocation", "/opt/spark/checkpoints/reddit_stream") \
            .config("spark.cassandra.connection.host", cassandra_host) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Initialize VADER sentiment analyzer
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
        logger.info("Spark session initialized")
    
    def create_sentiment_udf(self):
        """Create UDF for VADER sentiment analysis."""
        analyzer = SentimentIntensityAnalyzer()
        
        def analyze_sentiment(text: str) -> float:
            if not text:
                return 0.0
            scores = analyzer.polarity_scores(text)
            return float(scores['compound'])
        
        return udf(analyze_sentiment, FloatType())
    
    def read_reddit_posts_stream(self):
        """Read Reddit posts from Kafka."""
        logger.info("Reading reddit.posts stream from Kafka...")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", "reddit.posts") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON and extract fields
        posts = df.select(
            from_json(col("value").cast("string"), REDDIT_POST_SCHEMA).alias("data")
        ).select("data.*")
        
        # Add event timestamp from created_utc
        posts = posts.withColumn(
            "event_time",
            (col("created_utc").cast("long")).cast("timestamp")
        )
        
        return posts
    
    def read_reddit_comments_stream(self):
        """Read Reddit comments from Kafka."""
        logger.info("Reading reddit.comments stream from Kafka...")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", "reddit.comments") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON and extract fields
        comments = df.select(
            from_json(col("value").cast("string"), REDDIT_COMMENT_SCHEMA).alias("data")
        ).select("data.*")
        
        # Add event timestamp
        comments = comments.withColumn(
            "event_time",
            (col("created_utc").cast("long")).cast("timestamp")
        )
        
        return comments
    
    def process_posts_with_sentiment(self, posts_df):
        """
        Process posts: sentiment analysis and viral metrics.
        No TMDB matching - just analyze by movie title mentions.
        
        Args:
            posts_df: Streaming DataFrame of Reddit posts
            
        Returns:
            Processed DataFrame with sentiment and metrics
        """
        sentiment_udf = self.create_sentiment_udf()
        
        # Explode potential_movies array to get one row per movie mention
        posts_exploded = posts_df.select(
            explode(col("potential_movies")).alias("movie_title"),
            col("*")
        )
        
        # Calculate sentiment on title + selftext
        posts_with_sentiment = posts_exploded.withColumn(
            "combined_text",
            when(col("selftext").isNotNull(), 
                 col("title") + " " + col("selftext"))
            .otherwise(col("title"))
        ).withColumn(
            "sentiment_score",
            sentiment_udf(col("combined_text"))
        )
        
        # Apply 5-minute tumbling windows
        windowed = posts_with_sentiment \
            .withWatermark("event_time", "30 seconds") \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("movie_title")
            ) \
            .agg(
                count("post_id").alias("post_count"),
                spark_sum("upvotes").alias("total_upvotes"),
                avg("upvote_ratio").alias("avg_upvote_ratio"),
                spark_sum("num_comments").alias("total_comments"),
                spark_sum("awards").alias("total_awards"),
                avg("sentiment_score").alias("avg_sentiment"),
                spark_max("upvotes").alias("max_upvotes"),
                collect_list("post_id").alias("post_ids")
            )
        
        # Calculate viral velocity metrics
        windowed = windowed.withColumn(
            "upvote_velocity",
            col("total_upvotes") / 300.0  # upvotes per second (5-min window)
        ).withColumn(
            "comment_velocity",
            col("total_comments") / 300.0
        ).withColumn(
            "award_velocity",
            col("total_awards") / 300.0
        ).withColumn(
            "viral_score",
            (col("upvote_velocity") * 0.5) + 
            (col("comment_velocity") * 0.3) + 
            (col("award_velocity") * 0.2)
        )
        
        # Add metadata
        windowed = windowed.withColumn(
            "window_start", col("window.start")
        ).withColumn(
            "hour", date_trunc("hour", col("window.start"))
        ).withColumn(
            "data_source", lit("reddit")
        ).withColumn(
            "processed_at", current_timestamp()
        )
        
        return windowed
    
    def process_comments_with_sentiment(self, comments_df):
        """
        Process comments: sentiment analysis aggregated by movie.
        No TMDB matching - just analyze by movie title mentions.
        
        Args:
            comments_df: Streaming DataFrame of Reddit comments
            
        Returns:
            Processed DataFrame with sentiment metrics
        """
        sentiment_udf = self.create_sentiment_udf()
        
        # Explode potential_movies
        comments_exploded = comments_df.select(
            explode(col("potential_movies")).alias("movie_title"),
            col("*")
        )
        
        # Calculate sentiment
        comments_with_sentiment = comments_exploded.withColumn(
            "sentiment_score",
            sentiment_udf(col("body"))
        )
        
        # Apply 5-minute tumbling windows
        windowed = comments_with_sentiment \
            .withWatermark("event_time", "30 seconds") \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("movie_title")
            ) \
            .agg(
                count("comment_id").alias("comment_count"),
                spark_sum("upvotes").alias("total_upvotes"),
                spark_sum("awards").alias("total_awards"),
                avg("sentiment_score").alias("avg_sentiment"),
                spark_max("upvotes").alias("max_upvotes")
            )
        
        # Calculate velocities
        windowed = windowed.withColumn(
            "upvote_velocity",
            col("total_upvotes") / 300.0  # upvotes per second (5-min window)
        ).withColumn(
            "award_velocity",
            col("total_awards") / 300.0
        )
        
        # Add metadata
        windowed = windowed.withColumn(
            "window_start", col("window.start")
        ).withColumn(
            "hour", date_trunc("hour", col("window.start"))
        ).withColumn(
            "data_source", lit("reddit")
        ).withColumn(
            "processed_at", current_timestamp()
        )
        
        return windowed
    
    def write_to_cassandra(self, df, table_name: str):
        """
        Write streaming DataFrame to Cassandra.
        
        Args:
            df: Streaming DataFrame
            table_name: Cassandra table name
        """
        logger.info(f"Writing stream to Cassandra table: {table_name}")
        
        query = df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "speed_layer") \
            .option("table", table_name) \
            .option("checkpointLocation", f"/opt/spark/checkpoints/{table_name}") \
            .start()
        
        return query
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info("Starting Reddit sentiment streaming pipeline...")
        
        try:
            # Read streams
            posts = self.read_reddit_posts_stream()
            comments = self.read_reddit_comments_stream()
            
            # Process posts
            processed_posts = self.process_posts_with_sentiment(posts)
            
            # Process comments
            processed_comments = self.process_comments_with_sentiment(comments)
            
            # Write to Cassandra
            posts_query = self.write_to_cassandra(
                processed_posts.drop("window", "post_ids"),
                "reddit_post_metrics"
            )
            
            comments_query = self.write_to_cassandra(
                processed_comments.drop("window"),
                "reddit_comment_metrics"
            )
            
            logger.info("Streaming queries started. Awaiting termination...")
            
            # Wait for termination
            posts_query.awaitTermination()
            comments_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming pipeline failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """Main entry point."""
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    cassandra_host = os.getenv('CASSANDRA_HOST', 'cassandra')
    
    pipeline = RedditSentimentStreaming(
        kafka_bootstrap_servers=kafka_bootstrap,
        cassandra_host=cassandra_host
    )
    
    pipeline.run()


if __name__ == '__main__':
    main()
