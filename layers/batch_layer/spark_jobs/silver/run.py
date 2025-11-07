"""
Silver Layer Transformation - Bronze to Silver

Transforms raw Bronze layer data to clean, enriched Silver layer data.

Features:
- Deduplication by movie_id
- Schema validation
- Data enrichment (genres, cast/crew)
- Historical sentiment analysis (VADER)
- Quality flags
- Partitioning by year/month/genre

Usage:
    python run.py --execution-date 2025-01-15
    python run.py --execution-date 2025-01-15 --data-type movies
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
from config.config import config
from config.schemas import SILVER_MOVIE_SCHEMA, SILVER_REVIEW_SCHEMA
from layers.batch_layer.spark_jobs.utils.spark_session import get_spark_session, stop_spark_session
from layers.batch_layer.spark_jobs.utils.logging_utils import get_logger, log_dataframe_info, JobMetrics
from layers.batch_layer.spark_jobs.utils.transformations import (
    deduplicate_by_key, validate_completeness, add_quality_flags
)
from layers.batch_layer.spark_jobs.utils.hdfs_utils import HDFSManager

from pyspark.sql.functions import (
    col, from_json, explode_outer, array, struct, lit, 
    to_date, current_timestamp, when, trim, regexp_replace,
    udf, concat_ws
)
from pyspark.sql.types import DoubleType, StringType, ArrayType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logger = get_logger(__name__)


# Sentiment analysis UDF
def get_sentiment_scores(text: str) -> dict:
    """Calculate sentiment scores using VADER."""
    if not text:
        return {"compound": 0.0, "positive": 0.0, "neutral": 1.0, "negative": 0.0}
    
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    return scores


class SilverTransformation:
    """
    Silver layer transformation for movies and reviews.
    """
    
    def __init__(self, execution_date: datetime):
        """
        Initialize Silver transformation.
        
        Args:
            execution_date: Date for which to run transformation
        """
        self.execution_date = execution_date
        self.spark = get_spark_session("silver_transformation")
        self.hdfs_manager = HDFSManager()
        self.metrics = JobMetrics("silver_transformation", logger)
        
        # Register sentiment UDF
        self.sentiment_udf = udf(get_sentiment_scores, 
                                 ArrayType(DoubleType()))
    
    def transform_movies(self) -> bool:
        """
        Transform Bronze movies to Silver movies.
        
        Returns:
            True if successful
        """
        logger.info("Transforming movies: Bronze → Silver")
        
        # Read Bronze layer
        bronze_path = f"{config.hdfs_namenode}{config.hdfs_paths['bronze']}/movies"
        
        try:
            bronze_df = self.spark.read.parquet(bronze_path)
            log_dataframe_info(logger, bronze_df, "bronze_movies")
            
            # Parse raw JSON
            movies_df = bronze_df.select(
                col("extraction_timestamp"),
                from_json(col("raw_json"), "struct<" +
                    "id:int,title:string,original_title:string," +
                    "overview:string,release_date:string,adult:boolean," +
                    "popularity:double,vote_average:double,vote_count:int," +
                    "budget:long,revenue:long,runtime:int,status:string," +
                    "tagline:string,original_language:string," +
                    "genres:array<struct<id:int,name:string>>," +
                    "production_companies:array<struct<id:int,name:string>>," +
                    "credits:struct<cast:array<struct<name:string,character:string>>," +
                              "crew:array<struct<name:string,job:string>>>" +
                    ">").alias("data")
            ).select("extraction_timestamp", "data.*")
            
            # Deduplicate by movie_id (keep latest)
            movies_dedup = deduplicate_by_key(
                movies_df,
                ["id"],
                "extraction_timestamp",
                ascending=False
            )
            
            self.metrics.add_metric("movies_after_dedup", movies_dedup.count())
            
            # Extract and transform fields
            silver_movies = movies_dedup.select(
                col("id").alias("movie_id"),
                col("title"),
                col("original_title"),
                col("overview"),
                to_date(col("release_date")).alias("release_date"),
                col("adult"),
                col("popularity"),
                col("vote_average"),
                col("vote_count"),
                col("budget"),
                col("revenue"),
                col("runtime"),
                col("status"),
                col("tagline"),
                col("original_language"),
                # Extract genre names
                when(
                    col("genres").isNotNull(),
                    concat_ws(",", col("genres.name"))
                ).alias("genres_raw"),
                # Extract company names
                when(
                    col("production_companies").isNotNull(),
                    concat_ws(",", col("production_companies.name"))
                ).alias("production_companies_raw"),
                # Extract cast names
                when(
                    col("credits.cast").isNotNull(),
                    concat_ws(",", col("credits.cast.name"))
                ).alias("cast_raw"),
                # Extract crew names
                when(
                    col("credits.crew").isNotNull(),
                    concat_ws(",", col("credits.crew.name"))
                ).alias("crew_raw"),
                current_timestamp().alias("processed_timestamp")
            )
            
            # Convert comma-separated to arrays
            silver_movies = silver_movies.withColumn(
                "genres",
                when(col("genres_raw").isNotNull(), 
                     expr("split(genres_raw, ',')")).otherwise(array())
            ).withColumn(
                "production_companies",
                when(col("production_companies_raw").isNotNull(),
                     expr("split(production_companies_raw, ',')")).otherwise(array())
            ).withColumn(
                "cast",
                when(col("cast_raw").isNotNull(),
                     expr("split(cast_raw, ',')")).otherwise(array())
            ).withColumn(
                "crew",
                when(col("crew_raw").isNotNull(),
                     expr("split(crew_raw, ',')")).otherwise(array())
            ).withColumn(
                "keywords",
                array()  # Placeholder
            ).drop("genres_raw", "production_companies_raw", "cast_raw", "crew_raw")
            
            # Add quality flags
            silver_movies = add_quality_flags(
                silver_movies,
                required_columns=["movie_id", "title"],
                numeric_columns=["vote_average", "popularity"]
            )
            
            # Add partition columns
            silver_movies = silver_movies.withColumn(
                "partition_year", 
                expr("year(release_date)")
            ).withColumn(
                "partition_month",
                expr("month(release_date)")
            ).withColumn(
                "partition_genre",
                expr("genres[0]")  # First genre for partitioning
            )
            
            # Validate completeness
            _, validation_stats = validate_completeness(
                silver_movies,
                required_columns=["movie_id", "title", "release_date"],
                threshold=0.95
            )
            
            logger.info("Movies validation complete", extra=validation_stats)
            
            if not validation_stats["passes_threshold"]:
                logger.warning("Movies failed completeness validation!")
            
            # Write to Silver layer
            silver_path = f"{config.hdfs_namenode}{config.hdfs_paths['silver']}/movies"
            
            silver_movies.write \
                .mode("append") \
                .partitionBy("partition_year", "partition_month", "partition_genre") \
                .parquet(silver_path)
            
            final_count = silver_movies.count()
            self.metrics.add_metric("silver_movies_written", final_count)
            
            logger.info(f"Wrote {final_count} movies to Silver layer")
            return True
            
        except Exception as e:
            logger.error(f"Failed to transform movies: {e}", exc_info=True)
            return False
    
    def transform_reviews(self) -> bool:
        """
        Transform Bronze reviews to Silver reviews with sentiment analysis.
        
        Returns:
            True if successful
        """
        logger.info("Transforming reviews: Bronze → Silver")
        
        bronze_path = f"{config.hdfs_namenode}{config.hdfs_paths['bronze']}/reviews"
        
        try:
            bronze_df = self.spark.read.parquet(bronze_path)
            log_dataframe_info(logger, bronze_df, "bronze_reviews")
            
            # Parse raw JSON
            reviews_df = bronze_df.select(
                col("extraction_timestamp"),
                from_json(col("raw_json"), "struct<" +
                    "id:string,movie_id:int,author:string,content:string," +
                    "created_at:string,author_details:struct<rating:double>" +
                    ">").alias("data")
            ).select("extraction_timestamp", "data.*")
            
            # Deduplicate
            reviews_dedup = deduplicate_by_key(
                reviews_df,
                ["id"],
                "extraction_timestamp",
                ascending=False
            )
            
            # Calculate sentiment (using Pandas UDF for better performance)
            @udf(returnType=StringType())
            def calculate_sentiment_label(compound_score):
                if compound_score is None:
                    return "neutral"
                if compound_score >= 0.05:
                    return "positive"
                elif compound_score <= -0.05:
                    return "negative"
                else:
                    return "neutral"
            
            @udf(returnType=DoubleType())
            def calculate_sentiment_compound(text):
                if not text:
                    return 0.0
                analyzer = SentimentIntensityAnalyzer()
                return analyzer.polarity_scores(text)['compound']
            
            @udf(returnType=DoubleType())
            def calculate_sentiment_positive(text):
                if not text:
                    return 0.0
                analyzer = SentimentIntensityAnalyzer()
                return analyzer.polarity_scores(text)['pos']
            
            @udf(returnType=DoubleType())
            def calculate_sentiment_neutral(text):
                if not text:
                    return 1.0
                analyzer = SentimentIntensityAnalyzer()
                return analyzer.polarity_scores(text)['neu']
            
            @udf(returnType=DoubleType())
            def calculate_sentiment_negative(text):
                if not text:
                    return 0.0
                analyzer = SentimentIntensityAnalyzer()
                return analyzer.polarity_scores(text)['neg']
            
            silver_reviews = reviews_dedup.select(
                col("id").alias("review_id"),
                col("movie_id"),
                col("author"),
                col("content"),
                to_date(col("created_at")).alias("created_at"),
                col("author_details.rating").alias("rating"),
                current_timestamp().alias("processed_timestamp")
            )
            
            # Add sentiment scores
            silver_reviews = silver_reviews.withColumn(
                "sentiment_compound",
                calculate_sentiment_compound(col("content"))
            ).withColumn(
                "sentiment_positive",
                calculate_sentiment_positive(col("content"))
            ).withColumn(
                "sentiment_neutral",
                calculate_sentiment_neutral(col("content"))
            ).withColumn(
                "sentiment_negative",
                calculate_sentiment_negative(col("content"))
            ).withColumn(
                "sentiment_score",
                col("sentiment_compound")
            ).withColumn(
                "sentiment_label",
                calculate_sentiment_label(col("sentiment_compound"))
            )
            
            # Add quality flags
            silver_reviews = add_quality_flags(
                silver_reviews,
                required_columns=["review_id", "movie_id", "content"]
            )
            
            # Add partition columns
            silver_reviews = silver_reviews.withColumn(
                "partition_year",
                expr("year(created_at)")
            ).withColumn(
                "partition_month",
                expr("month(created_at)")
            )
            
            # Write to Silver layer
            silver_path = f"{config.hdfs_namenode}{config.hdfs_paths['silver']}/reviews"
            
            silver_reviews.write \
                .mode("append") \
                .partitionBy("partition_year", "partition_month") \
                .parquet(silver_path)
            
            final_count = silver_reviews.count()
            self.metrics.add_metric("silver_reviews_written", final_count)
            
            logger.info(f"Wrote {final_count} reviews to Silver layer")
            return True
            
        except Exception as e:
            logger.error(f"Failed to transform reviews: {e}", exc_info=True)
            return False
    
    def run(self, data_type: Optional[str] = None) -> bool:
        """
        Run Silver transformation.
        
        Args:
            data_type: Specific data type to transform (movies, reviews, or None for all)
            
        Returns:
            True if successful
        """
        logger.info(f"Starting Silver transformation for {data_type or 'all types'}")
        
        success = True
        
        try:
            if data_type is None or data_type == "movies":
                if not self.transform_movies():
                    success = False
            
            if data_type is None or data_type == "reviews":
                if not self.transform_reviews():
                    success = False
            
            self.metrics.finish(success=success)
            return success
            
        except Exception as e:
            logger.error(f"Silver transformation failed: {e}", exc_info=True)
            self.metrics.finish(success=False)
            return False
        finally:
            stop_spark_session(self.spark)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Silver Layer Transformation")
    parser.add_argument(
        "--execution-date",
        type=str,
        required=True,
        help="Execution date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        choices=["movies", "reviews"],
        help="Specific data type to transform"
    )
    
    args = parser.parse_args()
    
    execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    
    logger.info("Starting Silver transformation", extra={
        "execution_date": execution_date.isoformat(),
        "data_type": args.data_type or "all"
    })
    
    transformation = SilverTransformation(execution_date)
    success = transformation.run(args.data_type)
    
    if success:
        print(f"\n✅ Silver transformation completed successfully!")
        sys.exit(0)
    else:
        print(f"\n❌ Silver transformation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
