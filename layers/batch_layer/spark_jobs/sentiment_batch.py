"""
Sentiment Analysis Batch Processing Job.

This PySpark job performs sentiment analysis on movie reviews and overviews
in batch mode with configurable models and deterministic results.
"""

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, trim, length, regexp_replace,
    udf, broadcast, lit, coalesce, desc, explode, collect_list
)
from pyspark.sql.types import DoubleType, StringType, IntegerType, StructType, StructField
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Performs sentiment analysis using VADER or configurable models."""
    
    def __init__(self, model_type: str = "vader"):
        """
        Initialize sentiment analyzer.
        
        Args:
            model_type: Type of sentiment model to use ('vader', 'textblob', etc.)
        """
        self.model_type = model_type.lower()
        self._initialize_model()
    
    def _initialize_model(self):
        """Initialize the sentiment analysis model."""
        if self.model_type == "vader":
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                self.analyzer = SentimentIntensityAnalyzer()
                logger.info("VADER sentiment analyzer initialized")
            except ImportError:
                logger.warning("VADER not available, falling back to rule-based sentiment")
                self.analyzer = None
        else:
            logger.warning(f"Model type {self.model_type} not implemented, using rule-based")
            self.analyzer = None
    
    def analyze_sentiment(self, text: str) -> Tuple[float, str]:
        """
        Analyze sentiment of given text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Tuple[float, str]: (sentiment_score, sentiment_label)
        """
        if not text or not isinstance(text, str):
            return 0.0, "neutral"
        
        # Clean text
        cleaned_text = self._clean_text(text)
        
        if self.analyzer and self.model_type == "vader":
            return self._vader_sentiment(cleaned_text)
        else:
            return self._rule_based_sentiment(cleaned_text)
    
    def _clean_text(self, text: str) -> str:
        """Clean and preprocess text for sentiment analysis."""
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove URLs
        text = re.sub(r'http\S+|www.\S+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Limit length for processing efficiency
        return text[:2000]
    
    def _vader_sentiment(self, text: str) -> Tuple[float, str]:
        """Perform VADER sentiment analysis."""
        try:
            scores = self.analyzer.polarity_scores(text)
            compound_score = scores['compound']
            
            # Map compound score to label
            if compound_score >= 0.05:
                label = "positive"
            elif compound_score <= -0.05:
                label = "negative"
            else:
                label = "neutral"
            
            return compound_score, label
            
        except Exception as e:
            logger.error(f"VADER sentiment analysis failed: {e}")
            return self._rule_based_sentiment(text)
    
    def _rule_based_sentiment(self, text: str) -> Tuple[float, str]:
        """Perform rule-based sentiment analysis as fallback."""
        text_lower = text.lower()
        
        # Simple positive/negative word lists
        positive_words = {
            'excellent', 'amazing', 'great', 'fantastic', 'wonderful', 'outstanding',
            'brilliant', 'superb', 'magnificent', 'perfect', 'love', 'best',
            'incredible', 'awesome', 'remarkable', 'exceptional', 'marvelous'
        }
        
        negative_words = {
            'terrible', 'awful', 'horrible', 'bad', 'worst', 'hate', 'disappointing',
            'boring', 'stupid', 'waste', 'poor', 'lame', 'dull', 'pathetic',
            'annoying', 'frustrating', 'ridiculous', 'garbage', 'trash'
        }
        
        # Count positive and negative words
        words = re.findall(r'\b\w+\b', text_lower)
        positive_count = sum(1 for word in words if word in positive_words)
        negative_count = sum(1 for word in words if word in negative_words)
        
        # Calculate sentiment score
        total_words = len(words)
        if total_words == 0:
            return 0.0, "neutral"
        
        sentiment_score = (positive_count - negative_count) / total_words
        
        # Normalize to [-1, 1] range
        sentiment_score = max(-1.0, min(1.0, sentiment_score * 10))
        
        # Determine label
        if sentiment_score > 0.1:
            label = "positive"
        elif sentiment_score < -0.1:
            label = "negative"
        else:
            label = "neutral"
        
        return sentiment_score, label


class SentimentBatchProcessor:
    """Processes sentiment analysis in batch mode using Spark."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize sentiment batch processor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        self.job_config = config.get("jobs", {}).get("sentiment_batch", {})
        
        # Configuration
        self.batch_size = self.job_config.get("batch_size", 1000)
        self.model_broadcast = self.job_config.get("model_broadcast", True)
        self.partition_by_length = self.job_config.get("partition_by_length", True)
        
        # Paths
        self.silver_path = self.hdfs_config["paths"]["silver"]
        self.errors_path = self.hdfs_config["paths"]["errors"]
        
        # Initialize sentiment analyzer
        self.sentiment_analyzer = SentimentAnalyzer()
        
        # Create UDF for sentiment analysis
        self._create_sentiment_udf()
    
    def _create_sentiment_udf(self):
        """Create Spark UDF for sentiment analysis."""
        def sentiment_udf(text):
            """UDF wrapper for sentiment analysis."""
            if text is None:
                return (0.0, "neutral")
            
            analyzer = SentimentAnalyzer()  # Create new instance for each executor
            return analyzer.analyze_sentiment(text)
        
        # Define return schema
        sentiment_schema = StructType([
            StructField("sentiment_score", DoubleType(), False),
            StructField("sentiment_label", StringType(), False)
        ])
        
        self.sentiment_udf = udf(sentiment_udf, sentiment_schema)
    
    def process_sentiment_batch(
        self, 
        start_time: datetime,
        end_time: datetime,
        batch_id: str
    ) -> str:
        """
        Process sentiment analysis for Silver layer data.
        
        Args:
            start_time: Start time for processing window
            end_time: End time for processing window
            batch_id: Batch identifier
            
        Returns:
            str: Path where updated data was written
        """
        logger.info(f"Starting sentiment analysis for batch {batch_id}")
        
        try:
            # Load Silver data that needs sentiment analysis
            silver_df = self._load_silver_data_for_sentiment(start_time, end_time)
            
            if silver_df.count() == 0:
                logger.warning("No Silver data found for sentiment analysis")
                return ""
            
            # Process sentiment analysis
            sentiment_df = self._analyze_sentiment_batch(silver_df, batch_id)
            
            # Update Silver layer with sentiment scores
            updated_path = self._update_silver_with_sentiment(sentiment_df, batch_id)
            
            logger.info(f"Sentiment analysis completed: {updated_path}")
            return updated_path
            
        except Exception as e:
            logger.error(f"Error in sentiment batch processing: {e}")
            raise
    
    def _load_silver_data_for_sentiment(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> DataFrame:
        """
        Load Silver layer data that needs sentiment analysis.
        
        Args:
            start_time: Start time window
            end_time: End time window
            
        Returns:
            DataFrame: Silver data for sentiment processing
        """
        logger.info("Loading Silver data for sentiment analysis")
        
        try:
            # Load Silver data with filter for missing sentiment
            silver_df = (self.spark.read
                        .parquet(self.silver_path)
                        .filter(
                            (col("processed_timestamp") >= start_time) &
                            (col("processed_timestamp") <= end_time) &
                            (col("sentiment_score").isNull() | col("sentiment_label").isNull())
                        ))
            
            # Select relevant columns for sentiment analysis
            sentiment_input_df = silver_df.select(
                col("movie_id"),
                col("title"),
                col("overview"),
                col("tagline"),
                coalesce(col("overview"), col("tagline"), col("title")).alias("text_for_sentiment"),
                col("partition_year"),
                col("partition_month"),
                col("partition_genre")
            ).filter(col("text_for_sentiment").isNotNull())
            
            logger.info(f"Loaded {sentiment_input_df.count()} records for sentiment analysis")
            return sentiment_input_df
            
        except Exception as e:
            logger.error(f"Error loading Silver data for sentiment: {e}")
            raise
    
    def _analyze_sentiment_batch(self, df: DataFrame, batch_id: str) -> DataFrame:
        """
        Perform sentiment analysis on batch of text data.
        
        Args:
            df: DataFrame with text data
            batch_id: Batch identifier
            
        Returns:
            DataFrame: DataFrame with sentiment scores
        """
        logger.info("Performing batch sentiment analysis")
        
        try:
            # Optional: Partition by text length for better performance
            if self.partition_by_length:
                df = df.withColumn(
                    "text_length_bucket",
                    when(length(col("text_for_sentiment")) <= 100, "short")
                    .when(length(col("text_for_sentiment")) <= 500, "medium")
                    .otherwise("long")
                )
            
            # Apply sentiment analysis UDF
            sentiment_df = df.withColumn(
                "sentiment_result",
                self.sentiment_udf(col("text_for_sentiment"))
            )
            
            # Extract sentiment components
            result_df = (sentiment_df
                        .withColumn("sentiment_score", col("sentiment_result.sentiment_score"))
                        .withColumn("sentiment_label", col("sentiment_result.sentiment_label"))
                        .withColumn("sentiment_batch_id", lit(batch_id))
                        .withColumn("sentiment_processed_at", lit(datetime.utcnow()))
                        .drop("sentiment_result", "text_for_sentiment"))
            
            if self.partition_by_length:
                result_df = result_df.drop("text_length_bucket")
            
            logger.info("Sentiment analysis completed")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            raise
    
    def _update_silver_with_sentiment(self, sentiment_df: DataFrame, batch_id: str) -> str:
        """
        Update Silver layer with computed sentiment scores.
        
        Args:
            sentiment_df: DataFrame with sentiment scores
            batch_id: Batch identifier
            
        Returns:
            str: Path where updated data was written
        """
        logger.info("Updating Silver layer with sentiment scores")
        
        try:
            # Load current Silver data
            current_silver_df = self.spark.read.parquet(self.silver_path)
            
            # Create lookup for sentiment scores
            sentiment_lookup = sentiment_df.select(
                col("movie_id"),
                col("sentiment_score"),
                col("sentiment_label"),
                col("sentiment_batch_id"),
                col("sentiment_processed_at")
            )
            
            # Update Silver data with sentiment scores
            updated_df = (current_silver_df
                         .alias("silver")
                         .join(
                             sentiment_lookup.alias("sentiment"),
                             col("silver.movie_id") == col("sentiment.movie_id"),
                             "left"
                         )
                         .select(
                             col("silver.*"),
                             coalesce(col("sentiment.sentiment_score"), col("silver.sentiment_score")).alias("sentiment_score"),
                             coalesce(col("sentiment.sentiment_label"), col("silver.sentiment_label")).alias("sentiment_label")
                         ))
            
            # Write updated Silver data
            (updated_df.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("partition_year", "partition_month", "partition_genre")
             .parquet(self.silver_path))
            
            # Update metadata
            self._update_sentiment_metadata(batch_id, sentiment_df.count())
            
            logger.info(f"Silver layer updated with sentiment scores")
            return self.silver_path
            
        except Exception as e:
            logger.error(f"Error updating Silver with sentiment: {e}")
            raise
    
    def _update_sentiment_metadata(self, batch_id: str, records_processed: int):
        """
        Update metadata for sentiment processing.
        
        Args:
            batch_id: Batch identifier
            records_processed: Number of records processed
        """
        metadata_record = {
            "batch_id": batch_id,
            "job_type": "sentiment_analysis",
            "records_processed": records_processed,
            "processed_timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        }
        
        try:
            metadata_df = self.spark.createDataFrame([metadata_record])
            metadata_path = f"{self.hdfs_config['paths']['metadata']}/sentiment_log"
            
            (metadata_df.write
             .mode("append")
             .option("compression", "snappy")
             .json(metadata_path))
            
        except Exception as e:
            logger.error(f"Error updating sentiment metadata: {e}")
    
    def validate_sentiment_quality(self, sentiment_df: DataFrame) -> Dict:
        """
        Validate quality of sentiment analysis results.
        
        Args:
            sentiment_df: DataFrame with sentiment results
            
        Returns:
            Dict: Quality metrics
        """
        total_records = sentiment_df.count()
        
        if total_records == 0:
            return {"is_valid": False, "error": "No sentiment results"}
        
        # Calculate quality metrics
        valid_scores = sentiment_df.filter(
            col("sentiment_score").isNotNull() &
            (col("sentiment_score") >= -1.0) &
            (col("sentiment_score") <= 1.0)
        ).count()
        
        valid_labels = sentiment_df.filter(
            col("sentiment_label").isin(["positive", "negative", "neutral"])
        ).count()
        
        score_completeness = valid_scores / total_records
        label_completeness = valid_labels / total_records
        
        # Distribution check
        label_distribution = (sentiment_df
                             .groupBy("sentiment_label")
                             .count()
                             .collect())
        
        distribution = {row["sentiment_label"]: row["count"] for row in label_distribution}
        
        quality_threshold = 0.95
        overall_quality = min(score_completeness, label_completeness)
        
        return {
            "is_valid": overall_quality >= quality_threshold,
            "metrics": {
                "total_records": total_records,
                "score_completeness": score_completeness,
                "label_completeness": label_completeness,
                "overall_quality": overall_quality,
                "label_distribution": distribution
            }
        }


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Sentiment Analysis Batch Processing")
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
        .appName("Sentiment_Analysis_Batch") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize processor
        processor = SentimentBatchProcessor(spark, config)
        
        # Run sentiment analysis
        result_path = processor.process_sentiment_batch(
            start_time=start_time,
            end_time=end_time,
            batch_id=args.batch_id
        )
        
        logger.info(f"Sentiment analysis completed successfully: {result_path}")
        
    finally:
        spark.stop()