"""
Bronze to Silver Layer Transformation Job.

This PySpark job transforms raw TMDB data from Bronze layer to cleaned and
enriched Silver layer with deduplication, schema validation, and quality checks.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, trim, upper, lower,
    from_json, to_json, struct, explode, coalesce, lit, desc, row_number,
    unix_timestamp, from_unixtime, date_format, year, month, dayofmonth,
    size, array_contains, split, concat_ws, udf, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    ArrayType, DateType, TimestampType
)
from pyspark.sql.window import Window
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeToSilverTransformer:
    """Transforms Bronze layer data to Silver layer with cleaning and enrichment."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        self.job_config = config.get("jobs", {}).get("bronze_to_silver", {})
        
        # Set up paths
        self.bronze_path = self.hdfs_config["paths"]["bronze"]
        self.silver_path = self.hdfs_config["paths"]["silver"]
        self.errors_path = self.hdfs_config["paths"]["errors"]
        
        # Performance settings
        self.checkpoint_interval = self.job_config.get("checkpoint_interval", 10)
        self.cache_level = self.job_config.get("cache_level", "MEMORY_AND_DISK_SER")
        self.broadcast_threshold = self.job_config.get("broadcast_threshold", "10MB")
        
        # Set Spark configurations
        self._configure_spark_session()
    
    def _configure_spark_session(self):
        """Configure Spark session for optimal performance."""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.broadcastTimeout", "300")
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    def transform_bronze_to_silver(
        self, 
        start_time: datetime,
        end_time: datetime,
        batch_id: str
    ) -> str:
        """
        Main transformation function from Bronze to Silver layer.
        
        Args:
            start_time: Start time for data processing window
            end_time: End time for data processing window  
            batch_id: Unique identifier for this batch
            
        Returns:
            str: Path where Silver data was written
        """
        logger.info(f"Starting Bronze to Silver transformation for batch {batch_id}")
        
        try:
            # Load Bronze data
            bronze_df = self._load_bronze_data(start_time, end_time)
            if bronze_df.count() == 0:
                logger.warning("No Bronze data found for the specified time range")
                return ""
            
            # Apply transformations
            silver_df = self._apply_transformations(bronze_df, batch_id)
            
            # Write to Silver layer
            silver_path = self._write_silver_data(silver_df, batch_id)
            
            # Update metadata
            self._update_transformation_metadata(silver_path, silver_df.count(), batch_id)
            
            logger.info(f"Successfully transformed Bronze to Silver: {silver_path}")
            return silver_path
            
        except Exception as e:
            logger.error(f"Error in Bronze to Silver transformation: {e}")
            raise
    
    def _load_bronze_data(self, start_time: datetime, end_time: datetime) -> DataFrame:
        """
        Load Bronze layer data for the specified time range.
        
        Args:
            start_time: Start of time window
            end_time: End of time window
            
        Returns:
            DataFrame: Bronze layer data
        """
        logger.info(f"Loading Bronze data from {start_time} to {end_time}")
        
        try:
            # Create partition filter for efficient reading
            partition_filter = (
                (col("partition_year") >= start_time.year) &
                (col("partition_year") <= end_time.year) &
                (col("partition_month") >= start_time.month) &
                (col("partition_month") <= end_time.month) &
                (col("extraction_timestamp") >= start_time) &
                (col("extraction_timestamp") <= end_time)
            )
            
            # Load with partition pruning
            bronze_df = (self.spark.read
                        .parquet(self.bronze_path)
                        .filter(partition_filter)
                        .persist(self.cache_level))
            
            logger.info(f"Loaded {bronze_df.count()} Bronze records")
            return bronze_df
            
        except Exception as e:
            logger.error(f"Error loading Bronze data: {e}")
            raise
    
    def _apply_transformations(self, bronze_df: DataFrame, batch_id: str) -> DataFrame:
        """
        Apply all transformations to convert Bronze to Silver.
        
        Args:
            bronze_df: Bronze layer DataFrame
            batch_id: Batch identifier
            
        Returns:
            DataFrame: Transformed Silver layer data
        """
        logger.info("Applying Bronze to Silver transformations")
        
        # 1. Parse raw JSON and extract structured fields
        parsed_df = self._parse_raw_json(bronze_df)
        
        # 2. Deduplicate by movie_id (keep latest)
        deduplicated_df = self._deduplicate_movies(parsed_df)
        
        # 3. Validate and clean data
        cleaned_df = self._clean_and_validate(deduplicated_df)
        
        # 4. Enrich with additional fields
        enriched_df = self._enrich_data(cleaned_df)
        
        # 5. Apply schema transformations
        schema_df = self._apply_schema_transformations(enriched_df)
        
        # 6. Add quality flags and metadata
        final_df = self._add_quality_metadata(schema_df, batch_id)
        
        return final_df
    
    def _parse_raw_json(self, bronze_df: DataFrame) -> DataFrame:
        """
        Parse raw JSON data from Bronze layer into structured format.
        
        Args:
            bronze_df: Bronze layer DataFrame
            
        Returns:
            DataFrame: DataFrame with parsed JSON fields
        """
        logger.info("Parsing raw JSON data")
        
        # Define schema for TMDB movie response
        tmdb_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("budget", IntegerType(), True),
            StructField("revenue", IntegerType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("overview", StringType(), True),
            StructField("genres", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("production_companies", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("logo_path", StringType(), True),
                StructField("origin_country", StringType(), True)
            ])), True),
            StructField("production_countries", ArrayType(StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("spoken_languages", ArrayType(StructType([
                StructField("iso_639_1", StringType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("credits", StructType([
                StructField("cast", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("character", StringType(), True),
                    StructField("order", IntegerType(), True),
                    StructField("cast_id", IntegerType(), True),
                    StructField("credit_id", StringType(), True)
                ])), True),
                StructField("crew", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("job", StringType(), True),
                    StructField("department", StringType(), True),
                    StructField("credit_id", StringType(), True)
                ])), True)
            ]), True)
        ])
        
        try:
            # Parse JSON with schema
            parsed_df = bronze_df.withColumn(
                "parsed_data", 
                from_json(col("raw_json"), tmdb_schema)
            )
            
            # Extract fields from parsed JSON
            result_df = (parsed_df
                        .select(
                            col("movie_id"),
                            col("api_endpoint"),
                            col("extraction_timestamp"),
                            col("partition_year"),
                            col("partition_month"),
                            col("partition_day"),
                            col("partition_hour"),
                            # Core movie fields
                            col("parsed_data.id").alias("tmdb_id"),
                            col("parsed_data.title").alias("title"),
                            col("parsed_data.original_title").alias("original_title"),
                            col("parsed_data.release_date").alias("release_date_str"),
                            col("parsed_data.vote_average").alias("vote_average"),
                            col("parsed_data.vote_count").alias("vote_count"),
                            col("parsed_data.popularity").alias("popularity"),
                            col("parsed_data.budget").alias("budget"),
                            col("parsed_data.revenue").alias("revenue"),
                            col("parsed_data.runtime").alias("runtime"),
                            col("parsed_data.status").alias("status"),
                            col("parsed_data.tagline").alias("tagline"),
                            col("parsed_data.overview").alias("overview"),
                            # Complex fields
                            col("parsed_data.genres").alias("genres_raw"),
                            col("parsed_data.production_companies").alias("production_companies_raw"),
                            col("parsed_data.production_countries").alias("production_countries_raw"),
                            col("parsed_data.spoken_languages").alias("spoken_languages_raw"),
                            col("parsed_data.credits.cast").alias("cast_raw"),
                            col("parsed_data.credits.crew").alias("crew_raw")
                        ))
            
            logger.info("Successfully parsed raw JSON data")
            return result_df
            
        except Exception as e:
            logger.error(f"Error parsing raw JSON: {e}")
            raise
    
    def _deduplicate_movies(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate movies by movie_id, keeping the latest extraction.
        
        Args:
            df: DataFrame with potential duplicates
            
        Returns:
            DataFrame: Deduplicated DataFrame
        """
        logger.info("Deduplicating movies by movie_id")
        
        try:
            # Create window specification to get latest record per movie_id
            window_spec = Window.partitionBy("movie_id").orderBy(desc("extraction_timestamp"))
            
            # Add row number and filter to keep only the first (latest) record
            deduplicated_df = (df
                              .withColumn("rn", row_number().over(window_spec))
                              .filter(col("rn") == 1)
                              .drop("rn"))
            
            original_count = df.count()
            deduplicated_count = deduplicated_df.count()
            duplicates_removed = original_count - deduplicated_count
            
            logger.info(f"Removed {duplicates_removed} duplicate records. "
                       f"Final count: {deduplicated_count}")
            
            return deduplicated_df
            
        except Exception as e:
            logger.error(f"Error in deduplication: {e}")
            raise
    
    def _clean_and_validate(self, df: DataFrame) -> DataFrame:
        """
        Clean and validate data according to business rules.
        
        Args:
            df: DataFrame to clean and validate
            
        Returns:
            DataFrame: Cleaned and validated DataFrame
        """
        logger.info("Cleaning and validating data")
        
        try:
            # Clean text fields
            cleaned_df = (df
                         .withColumn("title", trim(col("title")))
                         .withColumn("original_title", trim(col("original_title")))
                         .withColumn("tagline", trim(col("tagline")))
                         .withColumn("overview", trim(col("overview")))
                         .withColumn("status", upper(trim(col("status")))))
            
            # Convert and validate release_date
            cleaned_df = (cleaned_df
                         .withColumn("release_date", 
                                   when(col("release_date_str").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                                       col("release_date_str").cast(DateType()))
                                   .otherwise(lit(None))))
            
            # Validate numeric fields
            cleaned_df = (cleaned_df
                         .withColumn("vote_average", 
                                   when((col("vote_average") >= 0) & (col("vote_average") <= 10), 
                                       col("vote_average"))
                                   .otherwise(lit(None)))
                         .withColumn("vote_count",
                                   when(col("vote_count") >= 0, col("vote_count"))
                                   .otherwise(lit(None)))
                         .withColumn("popularity",
                                   when(col("popularity") >= 0, col("popularity"))
                                   .otherwise(lit(None)))
                         .withColumn("budget",
                                   when(col("budget") >= 0, col("budget"))
                                   .otherwise(lit(None)))
                         .withColumn("revenue",
                                   when(col("revenue") >= 0, col("revenue"))
                                   .otherwise(lit(None)))
                         .withColumn("runtime",
                                   when((col("runtime") > 0) & (col("runtime") < 1000), 
                                       col("runtime"))
                                   .otherwise(lit(None))))
            
            logger.info("Data cleaning and validation completed")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error in cleaning and validation: {e}")
            raise
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with derived fields and transformations.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            DataFrame: Enriched DataFrame
        """
        logger.info("Enriching data with derived fields")
        
        try:
            # Extract genres as array of strings
            enriched_df = df.withColumn(
                "genres",
                when(col("genres_raw").isNotNull(),
                     expr("transform(genres_raw, x -> x.name)"))
                .otherwise(lit(None).cast(ArrayType(StringType())))
            )
            
            # Extract production companies
            enriched_df = enriched_df.withColumn(
                "production_companies",
                when(col("production_companies_raw").isNotNull(),
                     col("production_companies_raw"))
                .otherwise(lit(None))
            )
            
            # Extract production countries as array of strings
            enriched_df = enriched_df.withColumn(
                "production_countries",
                when(col("production_countries_raw").isNotNull(),
                     expr("transform(production_countries_raw, x -> x.name)"))
                .otherwise(lit(None).cast(ArrayType(StringType())))
            )
            
            # Extract spoken languages as array of strings
            enriched_df = enriched_df.withColumn(
                "spoken_languages",
                when(col("spoken_languages_raw").isNotNull(),
                     expr("transform(spoken_languages_raw, x -> x.name)"))
                .otherwise(lit(None).cast(ArrayType(StringType())))
            )
            
            # Process cast information
            enriched_df = enriched_df.withColumn(
                "cast",
                when(col("cast_raw").isNotNull(),
                     expr("slice(cast_raw, 1, 20)"))  # Keep top 20 cast members
                .otherwise(lit(None))
            )
            
            # Process crew information
            enriched_df = enriched_df.withColumn(
                "crew",
                when(col("crew_raw").isNotNull(),
                     col("crew_raw"))
                .otherwise(lit(None))
            )
            
            # Add primary genre for partitioning
            enriched_df = enriched_df.withColumn(
                "partition_genre",
                when(size(col("genres")) > 0, col("genres")[0])
                .otherwise(lit("unknown"))
            )
            
            # Add derived fields
            enriched_df = (enriched_df
                          .withColumn("processed_timestamp", lit(datetime.utcnow()))
                          .withColumn("partition_year", year(col("release_date")))
                          .withColumn("partition_month", month(col("release_date"))))
            
            # Initialize sentiment fields (will be populated by sentiment job)
            enriched_df = (enriched_df
                          .withColumn("sentiment_score", lit(None).cast(DoubleType()))
                          .withColumn("sentiment_label", lit(None).cast(StringType())))
            
            logger.info("Data enrichment completed")
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error in data enrichment: {e}")
            raise
    
    def _apply_schema_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply final schema transformations to match Silver layer schema.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            DataFrame: DataFrame with Silver layer schema
        """
        logger.info("Applying schema transformations")
        
        try:
            # Select and rename columns to match Silver schema
            schema_df = df.select(
                # Core identifiers
                col("movie_id"),
                col("title"),
                col("original_title"),
                col("release_date"),
                col("genres"),
                col("vote_average"),
                col("vote_count"),
                col("popularity"),
                col("budget").cast("long").alias("budget"),
                col("revenue").cast("long").alias("revenue"),
                col("runtime"),
                col("status"),
                col("tagline"),
                col("overview"),
                
                # Complex types
                col("cast"),
                col("crew"),
                col("production_companies"),
                col("production_countries"),
                col("spoken_languages"),
                
                # Derived fields
                col("sentiment_score"),
                col("sentiment_label"),
                col("processed_timestamp"),
                
                # Partition columns
                coalesce(col("partition_year"), year(col("release_date"))).alias("partition_year"),
                coalesce(col("partition_month"), month(col("release_date"))).alias("partition_month"),
                col("partition_genre")
            )
            
            logger.info("Schema transformations completed")
            return schema_df
            
        except Exception as e:
            logger.error(f"Error in schema transformation: {e}")
            raise
    
    def _add_quality_metadata(self, df: DataFrame, batch_id: str) -> DataFrame:
        """
        Add quality flags and metadata to the DataFrame.
        
        Args:
            df: DataFrame to add metadata to
            batch_id: Batch identifier
            
        Returns:
            DataFrame: DataFrame with quality metadata
        """
        logger.info("Adding quality metadata")
        
        try:
            # Define quality check conditions
            has_required_fields = (
                col("movie_id").isNotNull() &
                col("title").isNotNull() &
                col("title") != ""
            )
            
            has_valid_dates = (
                col("release_date").isNotNull()
            )
            
            has_valid_ratings = (
                col("vote_average").isNotNull() &
                col("vote_count").isNotNull() &
                col("vote_count") > 0
            )
            
            # Assign quality flags
            quality_df = df.withColumn(
                "quality_flag",
                when(has_required_fields & has_valid_dates & has_valid_ratings, "OK")
                .when(has_required_fields & (has_valid_dates | has_valid_ratings), "WARNING")
                .otherwise("ERROR")
            )
            
            # Add batch metadata
            quality_df = quality_df.withColumn("batch_id", lit(batch_id))
            
            logger.info("Quality metadata added")
            return quality_df
            
        except Exception as e:
            logger.error(f"Error adding quality metadata: {e}")
            raise
    
    def _write_silver_data(self, df: DataFrame, batch_id: str) -> str:
        """
        Write Silver layer data to HDFS with proper partitioning.
        
        Args:
            df: DataFrame to write
            batch_id: Batch identifier
            
        Returns:
            str: Path where data was written
        """
        logger.info("Writing Silver layer data")
        
        try:
            # Write with dynamic partitioning
            (df.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("partition_year", "partition_month", "partition_genre")
             .parquet(self.silver_path))
            
            logger.info(f"Successfully wrote Silver data to {self.silver_path}")
            return self.silver_path
            
        except Exception as e:
            logger.error(f"Error writing Silver data: {e}")
            raise
    
    def _update_transformation_metadata(self, silver_path: str, record_count: int, batch_id: str):
        """
        Update metadata for the transformation.
        
        Args:
            silver_path: Path where Silver data was written
            record_count: Number of records processed
            batch_id: Batch identifier
        """
        logger.info("Updating transformation metadata")
        
        metadata_record = {
            "batch_id": batch_id,
            "transformation": "bronze_to_silver",
            "silver_path": silver_path,
            "record_count": record_count,
            "processed_timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        }
        
        try:
            metadata_df = self.spark.createDataFrame([metadata_record])
            metadata_path = f"{self.hdfs_config['paths']['metadata']}/transformation_log"
            
            (metadata_df.write
             .mode("append")
             .option("compression", "snappy")
             .json(metadata_path))
            
            logger.info("Transformation metadata updated")
            
        except Exception as e:
            logger.error(f"Error updating transformation metadata: {e}")


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze to Silver Transformation")
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
        .appName("Bronze_to_Silver_Transformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize transformer
        transformer = BronzeToSilverTransformer(spark, config)
        
        # Run transformation
        result_path = transformer.transform_bronze_to_silver(
            start_time=start_time,
            end_time=end_time,
            batch_id=args.batch_id
        )
        
        logger.info(f"Transformation completed successfully: {result_path}")
        
    finally:
        spark.stop()