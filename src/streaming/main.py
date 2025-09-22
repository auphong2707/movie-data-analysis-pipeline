"""
Main Spark Structured Streaming job for movie data processing.
"""
import logging
import sys
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from .spark_config import create_spark_session, SCHEMAS
from .data_cleaner import DataCleaner
from .sentiment_analyzer import SentimentAnalyzer
from config.config import config

logger = logging.getLogger(__name__)

class MovieStreamingProcessor:
    """Main streaming processor for movie data analytics."""
    
    def __init__(self, checkpoint_location: str = "/tmp/spark-checkpoint"):
        self.spark = None
        self.checkpoint_location = checkpoint_location
        self.data_cleaner = DataCleaner()
        self.sentiment_analyzer = None
        self.running_queries = []
        
    def initialize(self):
        """Initialize Spark session and components."""
        logger.info("Initializing Movie Streaming Processor...")
        
        self.spark = create_spark_session(
            app_name="MovieAnalyticsStreaming",
            master_url=config.spark_master_url
        )
        
        self.sentiment_analyzer = SentimentAnalyzer(self.spark)
        
        logger.info("Streaming processor initialized successfully")
    
    def create_kafka_stream(self, topic: str, schema: StructType) -> DataFrame:
        """Create a streaming DataFrame from Kafka topic."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("json_data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select(
                col("*"),
                from_json(col("json_data"), schema).alias("data")
            ) \
            .select("message_key", "topic", "partition", "offset", "kafka_timestamp", "data.*")
    
    def process_movie_stream(self) -> DataFrame:
        """Process movie data stream."""
        logger.info("Setting up movie stream processing...")
        
        # Create streaming DataFrame
        movie_stream = self.create_kafka_stream(
            config.kafka_topics['movies'], 
            SCHEMAS['movies']
        )
        
        # Apply data cleansing and enrichment
        cleaned_movies = self.data_cleaner.clean_movie_data(movie_stream)
        
        # Write to Bronze layer (raw data with minimal cleaning)
        bronze_query = cleaned_movies.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['bronze']}/movies/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/movies-bronze") \
            .partitionBy("release_year", "language") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.running_queries.append(bronze_query)
        
        # Write to Silver layer (enriched data)
        silver_query = cleaned_movies.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['silver']}/movies/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/movies-silver") \
            .partitionBy("release_year", "popularity_tier") \
            .trigger(processingTime="1 minute") \
            .start()
        
        self.running_queries.append(silver_query)
        
        return cleaned_movies
    
    def process_review_stream(self) -> DataFrame:
        """Process review data stream with sentiment analysis."""
        logger.info("Setting up review stream processing...")
        
        # Create streaming DataFrame
        review_stream = self.create_kafka_stream(
            config.kafka_topics['reviews'],
            SCHEMAS['reviews']
        )
        
        # Apply data cleaning
        cleaned_reviews = self.data_cleaner.clean_review_data(review_stream)
        
        # Apply sentiment analysis
        reviews_with_sentiment = self.sentiment_analyzer.analyze_sentiment_advanced(cleaned_reviews)
        
        # Extract additional sentiment features
        enriched_reviews = self.sentiment_analyzer.extract_sentiment_features(reviews_with_sentiment)
        
        # Write to Bronze layer
        bronze_query = enriched_reviews.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['bronze']}/reviews/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/reviews-bronze") \
            .partitionBy("review_date") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.running_queries.append(bronze_query)
        
        # Write to Silver layer
        silver_query = enriched_reviews.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['silver']}/reviews/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/reviews-silver") \
            .partitionBy("review_date", "sentiment_label") \
            .trigger(processingTime="1 minute") \
            .start()
        
        self.running_queries.append(silver_query)
        
        return enriched_reviews
    
    def process_credit_stream(self) -> DataFrame:
        """Process credits data stream."""
        logger.info("Setting up credits stream processing...")
        
        credit_stream = self.create_kafka_stream(
            config.kafka_topics['credits'],
            SCHEMAS['credits']
        )
        
        cleaned_credits = self.data_cleaner.clean_credit_data(credit_stream)
        
        # Write to Bronze layer
        bronze_query = cleaned_credits.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['bronze']}/credits/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/credits-bronze") \
            .partitionBy("credit_type") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.running_queries.append(bronze_query)
        
        return cleaned_credits
    
    def create_trending_aggregations(self, movie_stream: DataFrame, review_stream: DataFrame):
        """Create trending movie aggregations."""
        logger.info("Setting up trending aggregations...")
        
        # Trending movies by popularity (windowed)
        trending_movies = movie_stream \
            .withWatermark("processing_timestamp", "10 minutes") \
            .groupBy(
                window(col("processing_timestamp"), "1 hour", "15 minutes"),
                col("movie_id"),
                col("title")
            ).agg(
                max("popularity").alias("max_popularity"),
                avg("vote_average").alias("avg_rating"),
                max("vote_count").alias("vote_count"),
                first("release_year").alias("release_year"),
                first("genres").alias("genres")
            ).withColumn(
                "trend_score", 
                col("max_popularity") * col("avg_rating") / 10.0
            ).withColumn(
                "window_start", col("window.start")
            ).withColumn(
                "window_end", col("window.end")
            ).drop("window")
        
        # Write trending movies to Gold layer
        trending_query = trending_movies.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"s3a://{config.minio_buckets['gold']}/trending_movies/") \
            .option("checkpointLocation", f"{self.checkpoint_location}/trending-movies") \
            .trigger(processingTime="15 minutes") \
            .start()
        
        self.running_queries.append(trending_query)
        
        # Sentiment trends by movie
        if review_stream is not None:
            sentiment_trends = review_stream \
                .withWatermark("processing_timestamp", "10 minutes") \
                .groupBy(
                    window(col("processing_timestamp"), "2 hours", "30 minutes"),
                    col("movie_id")
                ).agg(
                    count("*").alias("review_count"),
                    avg("sentiment_score").alias("avg_sentiment"),
                    sum(when(col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
                    sum(when(col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count")
                ).withColumn(
                    "sentiment_ratio", 
                    col("positive_count") / (col("positive_count") + col("negative_count"))
                ).withColumn(
                    "window_start", col("window.start")
                ).withColumn(
                    "window_end", col("window.end")
                ).drop("window")
            
            sentiment_query = sentiment_trends.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"s3a://{config.minio_buckets['gold']}/sentiment_trends/") \
                .option("checkpointLocation", f"{self.checkpoint_location}/sentiment-trends") \
                .trigger(processingTime="30 minutes") \
                .start()
            
            self.running_queries.append(sentiment_query)
    
    def create_mongodb_sink(self, df: DataFrame, collection_name: str):
        """Write streaming data to MongoDB."""
        def write_to_mongodb(batch_df, batch_id):
            if batch_df.count() > 0:
                batch_df.write \
                    .format("mongo") \
                    .option("uri", config.mongodb_connection_string) \
                    .option("database", config.mongodb_database) \
                    .option("collection", collection_name) \
                    .mode("append") \
                    .save()
                
                logger.info(f"Batch {batch_id}: Written {batch_df.count()} records to {collection_name}")
        
        return df.writeStream \
            .foreachBatch(write_to_mongodb) \
            .option("checkpointLocation", f"{self.checkpoint_location}/mongodb-{collection_name}") \
            .trigger(processingTime="2 minutes") \
            .start()
    
    def run_streaming_pipeline(self):
        """Run the complete streaming pipeline."""
        logger.info("Starting movie analytics streaming pipeline...")
        
        try:
            # Process individual streams
            movie_stream = self.process_movie_stream()
            review_stream = self.process_review_stream()
            credit_stream = self.process_credit_stream()
            
            # Create aggregations
            self.create_trending_aggregations(movie_stream, review_stream)
            
            # Setup MongoDB sinks for real-time serving
            # Aggregate movie metrics for serving
            movie_metrics = movie_stream.groupBy("movie_id").agg(
                first("title").alias("title"),
                avg("popularity").alias("avg_popularity"),
                avg("vote_average").alias("avg_rating"),
                first("genres").alias("genres"),
                first("release_year").alias("release_year")
            )
            
            mongodb_movie_query = self.create_mongodb_sink(movie_metrics, "movie_metrics")
            self.running_queries.append(mongodb_movie_query)
            
            # Aggregate review sentiment for serving
            if review_stream is not None:
                review_metrics = review_stream.groupBy("movie_id").agg(
                    count("*").alias("total_reviews"),
                    avg("sentiment_score").alias("avg_sentiment"),
                    countDistinct("author").alias("unique_reviewers")
                )
                
                mongodb_review_query = self.create_mongodb_sink(review_metrics, "review_metrics")
                self.running_queries.append(mongodb_review_query)
            
            logger.info(f"Started {len(self.running_queries)} streaming queries")
            
            # Wait for termination
            for query in self.running_queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {e}")
            self.stop_all_queries()
            raise
    
    def stop_all_queries(self):
        """Stop all running queries."""
        logger.info("Stopping all streaming queries...")
        
        for query in self.running_queries:
            if query.isActive:
                query.stop()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("All queries stopped")
    
    def get_query_status(self) -> Dict[str, Any]:
        """Get status of all running queries."""
        status = {
            "total_queries": len(self.running_queries),
            "active_queries": 0,
            "query_details": []
        }
        
        for i, query in enumerate(self.running_queries):
            query_info = {
                "query_id": i,
                "name": query.name,
                "is_active": query.isActive,
                "exception": str(query.exception()) if query.exception() else None
            }
            
            if query.isActive:
                status["active_queries"] += 1
                try:
                    progress = query.lastProgress
                    query_info.update({
                        "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                        "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                        "batch_duration": progress.get("durationMs", {}).get("batchDuration", 0)
                    })
                except:
                    pass
            
            status["query_details"].append(query_info)
        
        return status

def main():
    """Main entry point."""
    processor = MovieStreamingProcessor()
    
    try:
        processor.initialize()
        processor.run_streaming_pipeline()
    except KeyboardInterrupt:
        logger.info("Streaming pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Streaming pipeline failed: {e}")
        sys.exit(1)
    finally:
        processor.stop_all_queries()

if __name__ == "__main__":
    main()