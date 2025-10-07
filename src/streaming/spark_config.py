"""
Spark session configuration for the movie analytics pipeline.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
import logging

# Import Iceberg configuration
from config.iceberg_config import iceberg_config

logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "MovieAnalyticsPipeline", 
                        master_url: str = "local[*]",
                        additional_configs: dict = None,
                        enable_iceberg: bool = True) -> SparkSession:
    """Create and configure Spark session for the movie analytics pipeline."""
    
    # Base configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(master_url)
    
    # Kafka configuration
    conf.set("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
    conf.set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    
    # Performance tuning
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Memory configuration
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.executor.cores", "2")
    
    # Streaming configuration
    conf.set("spark.sql.streaming.metricsEnabled", "true")
    conf.set("spark.sql.streaming.ui.enabled", "true")
    
    # Delta Lake configuration (if using)
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Apache Iceberg Configuration
    if enable_iceberg:
        logger.info("Enabling Apache Iceberg table format")
        iceberg_configs = iceberg_config.get_spark_iceberg_configs()
        for key, value in iceberg_configs.items():
            conf.set(key, value)
    
    # Additional configurations
    if additional_configs:
        for key, value in additional_configs.items():
            conf.set(key, value)
    
    # Required JARs for Kafka, Iceberg and other dependencies
    jars = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "org.apache.kafka:kafka-clients:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.4.1",
        "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4",
        # Apache Iceberg dependencies
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    conf.set("spark.jars.packages", ",".join(jars))
    
    # Create Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Spark session created: {app_name}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Master URL: {master_url}")
    
    return spark

def get_movie_schema() -> StructType:
    """Get schema for movie data."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("adult", BooleanType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("budget", LongType(), True),
        StructField("revenue", LongType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("genres", ArrayType(StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False)
        ])), True),
        StructField("production_companies", ArrayType(StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("logo_path", StringType(), True),
            StructField("origin_country", StringType(), True)
        ])), True),
        StructField("ingestion_timestamp", DoubleType(), False),
        StructField("data_source", StringType(), False)
    ])

def get_person_schema() -> StructType:
    """Get schema for person data."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("biography", StringType(), True),
        StructField("birthday", StringType(), True),
        StructField("deathday", StringType(), True),
        StructField("place_of_birth", StringType(), True),
        StructField("profile_path", StringType(), True),
        StructField("adult", BooleanType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("gender", IntegerType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("also_known_as", ArrayType(StringType()), True),
        StructField("ingestion_timestamp", DoubleType(), False),
        StructField("data_source", StringType(), False)
    ])

def get_credit_schema() -> StructType:
    """Get schema for credit data."""
    return StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("person_id", IntegerType(), False),
        StructField("credit_type", StringType(), False),
        StructField("character", StringType(), True),
        StructField("job", StringType(), True),
        StructField("department", StringType(), True),
        StructField("order", IntegerType(), True),
        StructField("name", StringType(), False),
        StructField("profile_path", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("adult", BooleanType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("ingestion_timestamp", DoubleType(), False),
        StructField("data_source", StringType(), False)
    ])

def get_review_schema() -> StructType:
    """Get schema for review data."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("movie_id", IntegerType(), False),
        StructField("author", StringType(), False),
        StructField("content", StringType(), False),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("url", StringType(), True),
        StructField("author_details", StructType([
            StructField("name", StringType(), True),
            StructField("username", StringType(), True),
            StructField("avatar_path", StringType(), True),
            StructField("rating", DoubleType(), True)
        ]), True),
        StructField("ingestion_timestamp", DoubleType(), False),
        StructField("data_source", StringType(), False)
    ])

# Schema mapping
SCHEMAS = {
    'movies': get_movie_schema(),
    'people': get_person_schema(),
    'credits': get_credit_schema(),
    'reviews': get_review_schema()
}