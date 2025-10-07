"""
Example script demonstrating how to update streaming jobs to use Apache Iceberg.
This shows how to modify existing streaming code to leverage Iceberg table format.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.streaming.spark_config import create_spark_session, SCHEMAS
from src.streaming.data_cleaner import DataCleaner
from src.storage.iceberg_storage_manager import IcebergStorageManager
from config.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MovieStreamingProcessorWithIceberg:
    """
    Example streaming processor using Apache Iceberg table format.
    
    This demonstrates how to modify your existing streaming jobs to use Iceberg
    instead of direct Parquet writes.
    """
    
    def __init__(self, use_iceberg: bool = True, checkpoint_location: str = "/tmp/spark-checkpoint"):
        self.use_iceberg = use_iceberg
        self.checkpoint_location = checkpoint_location
        self.spark = None
        self.storage_manager = None
        self.data_cleaner = DataCleaner()
        self.running_queries = []
        
    def initialize(self):
        """Initialize Spark session with Iceberg support."""
        logger.info("Initializing Movie Streaming Processor with Iceberg...")
        
        # Create Spark session with Iceberg enabled
        self.spark = create_spark_session(
            app_name="MovieAnalyticsStreaming-Iceberg",
            master_url=config.spark_master_url,
            enable_iceberg=self.use_iceberg
        )
        
        if self.use_iceberg:
            # Initialize Iceberg storage manager
            self.storage_manager = IcebergStorageManager(self.spark)
            logger.info("Iceberg storage manager initialized")
        
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
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("processing_timestamp", current_timestamp())
    
    def process_movies_with_iceberg(self):
        """
        Process movie stream and write to Iceberg tables.
        
        This replaces the old Parquet-based approach with Iceberg tables.
        """
        logger.info("Setting up movie stream processing with Iceberg...")
        
        # Create Kafka stream
        movie_stream = self.create_kafka_stream(
            config.kafka_topics['movies'],
            SCHEMAS['movies']
        )
        
        # Clean data
        cleaned_movies = self.data_cleaner.clean_movie_data(movie_stream)
        
        if self.use_iceberg:
            # Option 1: Write to Iceberg using foreachBatch
            def write_to_iceberg_bronze(batch_df: DataFrame, batch_id: int):
                if batch_df.count() > 0:
                    logger.info(f"Writing batch {batch_id} to Iceberg Bronze layer...")
                    self.storage_manager.write_to_bronze(
                        df=batch_df,
                        table_name="movies_raw",
                        partition_cols=["year", "month"],
                        mode="append"
                    )
            
            bronze_query = cleaned_movies.writeStream \
                .outputMode("append") \
                .foreachBatch(write_to_iceberg_bronze) \
                .option("checkpointLocation", f"{self.checkpoint_location}/movies-bronze-iceberg") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            self.running_queries.append(bronze_query)
            
            # For Silver layer, you can process further
            def write_to_iceberg_silver(batch_df: DataFrame, batch_id: int):
                if batch_df.count() > 0:
                    logger.info(f"Writing batch {batch_id} to Iceberg Silver layer...")
                    # Apply additional transformations if needed
                    enriched_df = batch_df  # Add enrichment logic here
                    
                    self.storage_manager.write_to_silver(
                        df=enriched_df,
                        table_name="movies_clean",
                        partition_cols=["year", "month"],
                        mode="append"
                    )
            
            silver_query = cleaned_movies.writeStream \
                .outputMode("append") \
                .foreachBatch(write_to_iceberg_silver) \
                .option("checkpointLocation", f"{self.checkpoint_location}/movies-silver-iceberg") \
                .trigger(processingTime="1 minute") \
                .start()
            
            self.running_queries.append(silver_query)
        
        else:
            # Fallback to Parquet (old approach)
            bronze_query = cleaned_movies.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"s3a://{config.minio_buckets['bronze']}/movies/") \
                .option("checkpointLocation", f"{self.checkpoint_location}/movies-bronze") \
                .partitionBy("year", "month") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            self.running_queries.append(bronze_query)
        
        return cleaned_movies
    
    def process_reviews_with_iceberg(self):
        """Process review stream with sentiment analysis and write to Iceberg."""
        logger.info("Setting up review stream processing with Iceberg...")
        
        # Create Kafka stream
        review_stream = self.create_kafka_stream(
            config.kafka_topics['reviews'],
            SCHEMAS['reviews']
        )
        
        # Clean data
        cleaned_reviews = self.data_cleaner.clean_review_data(review_stream)
        
        if self.use_iceberg:
            def write_reviews_to_iceberg(batch_df: DataFrame, batch_id: int):
                if batch_df.count() > 0:
                    logger.info(f"Processing review batch {batch_id}...")
                    
                    # Write to Bronze first
                    self.storage_manager.write_to_bronze(
                        df=batch_df,
                        table_name="reviews_raw",
                        partition_cols=["year", "month"],
                        mode="append"
                    )
                    
                    # Apply sentiment analysis (for Silver layer)
                    # Note: In production, this might be a separate job
                    # to avoid blocking the streaming pipeline
                    try:
                        from src.streaming.sentiment_analyzer import SentimentAnalyzer
                        sentiment_analyzer = SentimentAnalyzer(self.spark)
                        reviews_with_sentiment = sentiment_analyzer.analyze_reviews(batch_df)
                        
                        self.storage_manager.write_to_silver(
                            df=reviews_with_sentiment,
                            table_name="reviews_with_sentiment",
                            partition_cols=["year", "month"],
                            mode="append"
                        )
                    except Exception as e:
                        logger.error(f"Sentiment analysis failed: {e}")
            
            review_query = cleaned_reviews.writeStream \
                .outputMode("append") \
                .foreachBatch(write_reviews_to_iceberg) \
                .option("checkpointLocation", f"{self.checkpoint_location}/reviews-iceberg") \
                .trigger(processingTime="1 minute") \
                .start()
            
            self.running_queries.append(review_query)
        
        return cleaned_reviews
    
    def create_gold_aggregations_with_iceberg(self, movie_stream: DataFrame):
        """Create Gold layer aggregations using Iceberg."""
        logger.info("Setting up Gold layer aggregations with Iceberg...")
        
        if self.use_iceberg:
            # Create windowed aggregations
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
                    first("release_year").alias("release_year")
                ).withColumn(
                    "trend_score", 
                    col("max_popularity") * col("avg_rating") / 10.0
                ).withColumn(
                    "window_start", col("window.start")
                ).withColumn(
                    "window_end", col("window.end")
                ).drop("window")
            
            def write_trending_to_iceberg(batch_df: DataFrame, batch_id: int):
                if batch_df.count() > 0:
                    logger.info(f"Writing trending aggregations batch {batch_id} to Gold layer...")
                    
                    # Add year partition from window_start
                    batch_with_partition = batch_df.withColumn(
                        "year", year(col("window_start"))
                    )
                    
                    self.storage_manager.write_to_gold(
                        df=batch_with_partition,
                        table_name="trending_movies",
                        partition_cols=["year"],
                        mode="append"
                    )
            
            gold_query = trending_movies.writeStream \
                .outputMode("append") \
                .foreachBatch(write_trending_to_iceberg) \
                .option("checkpointLocation", f"{self.checkpoint_location}/trending-gold-iceberg") \
                .trigger(processingTime="5 minutes") \
                .start()
            
            self.running_queries.append(gold_query)
    
    def run(self):
        """Run all streaming queries."""
        logger.info("Starting streaming queries...")
        
        # Initialize
        self.initialize()
        
        # Process movies
        movie_stream = self.process_movies_with_iceberg()
        
        # Process reviews
        review_stream = self.process_reviews_with_iceberg()
        
        # Create Gold layer aggregations
        self.create_gold_aggregations_with_iceberg(movie_stream)
        
        # Wait for all queries
        logger.info(f"Running {len(self.running_queries)} streaming queries...")
        for query in self.running_queries:
            logger.info(f"Query: {query.name} - Status: {query.status}")
        
        # Block until terminated
        try:
            for query in self.running_queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping streaming queries...")
            self.stop()
    
    def stop(self):
        """Stop all streaming queries."""
        logger.info("Stopping all streaming queries...")
        for query in self.running_queries:
            query.stop()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("All queries stopped")


def main():
    """Main entry point for Iceberg-based streaming."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Movie Analytics Streaming with Apache Iceberg'
    )
    parser.add_argument(
        '--use-iceberg',
        action='store_true',
        default=True,
        help='Use Apache Iceberg table format (default: True)'
    )
    parser.add_argument(
        '--checkpoint-location',
        type=str,
        default='/tmp/spark-checkpoint',
        help='Checkpoint location for streaming queries'
    )
    
    args = parser.parse_args()
    
    # Create and run processor
    processor = MovieStreamingProcessorWithIceberg(
        use_iceberg=args.use_iceberg,
        checkpoint_location=args.checkpoint_location
    )
    
    processor.run()


if __name__ == '__main__':
    main()
