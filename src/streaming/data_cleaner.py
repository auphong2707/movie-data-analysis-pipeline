"""
Data cleansing and enrichment functions for Spark streaming.
"""
import re
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    """Data cleansing and enrichment for movie data."""
    
    def __init__(self):
        self.text_cleaning_regex = re.compile(r'[^\w\s]')
        
    def clean_movie_data(self, df: DataFrame) -> DataFrame:
        """Clean and enrich movie data."""
        logger.info("Cleaning movie data...")
        
        # Remove duplicates based on movie ID
        df_dedup = df.dropDuplicates(['id'])
        
        # Data quality improvements
        df_clean = df_dedup.select(
            col('id').alias('movie_id'),
            trim(col('title')).alias('title'),
            trim(col('original_title')).alias('original_title'),
            # Clean overview text
            regexp_replace(col('overview'), r'\s+', ' ').alias('overview'),
            # Standardize release date format
            to_date(col('release_date'), 'yyyy-MM-dd').alias('release_date'),
            col('adult'),
            # Ensure popularity is positive
            when(col('popularity') < 0, 0.0).otherwise(col('popularity')).alias('popularity'),
            # Clamp vote_average between 0 and 10
            when(col('vote_average') < 0, 0.0)
            .when(col('vote_average') > 10, 10.0)
            .otherwise(col('vote_average')).alias('vote_average'),
            # Ensure vote_count is non-negative
            when(col('vote_count') < 0, 0).otherwise(col('vote_count')).alias('vote_count'),
            col('poster_path'),
            col('backdrop_path'),
            lower(col('original_language')).alias('language'),
            # Ensure budget and revenue are non-negative
            when(col('budget') < 0, 0).otherwise(col('budget')).alias('budget'),
            when(col('revenue') < 0, 0).otherwise(col('revenue')).alias('revenue'),
            # Ensure runtime is positive
            when(col('runtime') <= 0, null()).otherwise(col('runtime')).alias('runtime'),
            col('status'),
            trim(col('tagline')).alias('tagline'),
            col('homepage'),
            col('imdb_id'),
            col('genres'),
            col('production_companies'),
            col('ingestion_timestamp'),
            col('data_source'),
            # Add processing timestamp
            current_timestamp().alias('processing_timestamp'),
            # Add data quality flags
            when(col('title').isNull() | (trim(col('title')) == ''), 1).otherwise(0).alias('missing_title_flag'),
            when(col('release_date').isNull(), 1).otherwise(0).alias('missing_release_date_flag'),
            when(col('overview').isNull() | (trim(col('overview')) == ''), 1).otherwise(0).alias('missing_overview_flag')
        )
        
        # Add enriched fields
        df_enriched = df_clean.withColumn(
            'release_year', year(col('release_date'))
        ).withColumn(
            'release_decade', (floor(year(col('release_date')) / 10) * 10).cast(IntegerType())
        ).withColumn(
            'profit', col('revenue') - col('budget')
        ).withColumn(
            'roi', when(col('budget') > 0, (col('revenue') - col('budget')) / col('budget')).otherwise(0.0)
        ).withColumn(
            'popularity_tier', 
            when(col('popularity') >= 50, 'high')
            .when(col('popularity') >= 20, 'medium')
            .otherwise('low')
        ).withColumn(
            'rating_tier',
            when(col('vote_average') >= 8.0, 'excellent')
            .when(col('vote_average') >= 7.0, 'good')
            .when(col('vote_average') >= 6.0, 'average')
            .otherwise('poor')
        )
        
        return df_enriched
    
    def clean_person_data(self, df: DataFrame) -> DataFrame:
        """Clean and enrich person data."""
        logger.info("Cleaning person data...")
        
        # Remove duplicates
        df_dedup = df.dropDuplicates(['id'])
        
        df_clean = df_dedup.select(
            col('id').alias('person_id'),
            trim(col('name')).alias('name'),
            # Clean biography
            regexp_replace(col('biography'), r'\s+', ' ').alias('biography'),
            to_date(col('birthday'), 'yyyy-MM-dd').alias('birthday'),
            to_date(col('deathday'), 'yyyy-MM-dd').alias('deathday'),
            trim(col('place_of_birth')).alias('place_of_birth'),
            col('profile_path'),
            col('adult'),
            when(col('popularity') < 0, 0.0).otherwise(col('popularity')).alias('popularity'),
            col('gender'),
            col('known_for_department'),
            col('homepage'),
            col('imdb_id'),
            col('also_known_as'),
            col('ingestion_timestamp'),
            col('data_source'),
            current_timestamp().alias('processing_timestamp')
        )
        
        # Add enriched fields
        df_enriched = df_clean.withColumn(
            'age_at_death',
            when(col('deathday').isNotNull() & col('birthday').isNotNull(),
                 datediff(col('deathday'), col('birthday')) / 365.25).otherwise(null())
        ).withColumn(
            'current_age',
            when(col('deathday').isNull() & col('birthday').isNotNull(),
                 datediff(current_date(), col('birthday')) / 365.25).otherwise(null())
        ).withColumn(
            'birth_decade',
            when(col('birthday').isNotNull(),
                 (floor(year(col('birthday')) / 10) * 10).cast(IntegerType())).otherwise(null())
        ).withColumn(
            'popularity_tier',
            when(col('popularity') >= 20, 'high')
            .when(col('popularity') >= 5, 'medium')
            .otherwise('low')
        ).withColumn(
            'is_alive',
            col('deathday').isNull()
        )
        
        return df_enriched
    
    def clean_credit_data(self, df: DataFrame) -> DataFrame:
        """Clean and enrich credit data."""
        logger.info("Cleaning credit data...")
        
        # Remove duplicates
        df_dedup = df.dropDuplicates(['movie_id', 'person_id', 'credit_type', 'character', 'job'])
        
        df_clean = df_dedup.select(
            col('movie_id'),
            col('person_id'),
            col('credit_type'),
            trim(col('character')).alias('character'),
            trim(col('job')).alias('job'),
            trim(col('department')).alias('department'),
            col('order').alias('cast_order'),
            trim(col('name')).alias('person_name'),
            col('profile_path'),
            col('gender'),
            col('adult'),
            col('known_for_department'),
            when(col('popularity') < 0, 0.0).otherwise(col('popularity')).alias('popularity'),
            col('ingestion_timestamp'),
            col('data_source'),
            current_timestamp().alias('processing_timestamp')
        )
        
        # Add enriched fields
        df_enriched = df_clean.withColumn(
            'is_lead_role',
            when((col('credit_type') == 'cast') & (col('cast_order') <= 5), True).otherwise(False)
        ).withColumn(
            'is_director',
            col('job') == 'Director'
        ).withColumn(
            'is_producer',
            col('job').isin(['Producer', 'Executive Producer'])
        ).withColumn(
            'is_writer',
            col('job').isin(['Writer', 'Screenplay', 'Story'])
        )
        
        return df_enriched
    
    def clean_review_data(self, df: DataFrame) -> DataFrame:
        """Clean and enrich review data."""
        logger.info("Cleaning review data...")
        
        # Remove duplicates
        df_dedup = df.dropDuplicates(['id'])
        
        df_clean = df_dedup.select(
            col('id').alias('review_id'),
            col('movie_id'),
            trim(col('author')).alias('author'),
            # Clean review content
            regexp_replace(trim(col('content')), r'\s+', ' ').alias('content'),
            to_timestamp(col('created_at')).alias('created_at'),
            to_timestamp(col('updated_at')).alias('updated_at'),
            col('url'),
            col('author_details'),
            col('ingestion_timestamp'),
            col('data_source'),
            current_timestamp().alias('processing_timestamp')
        )
        
        # Add enriched fields
        df_enriched = df_clean.withColumn(
            'content_length', length(col('content'))
        ).withColumn(
            'word_count', size(split(col('content'), '\\s+'))
        ).withColumn(
            'has_rating', col('author_details.rating').isNotNull()
        ).withColumn(
            'review_date', to_date(col('created_at'))
        ).withColumn(
            'content_type',
            when(col('word_count') < 50, 'short')
            .when(col('word_count') < 200, 'medium')
            .otherwise('long')
        )
        
        return df_enriched

def detect_language(text_col: Column) -> Column:
    """Simple language detection UDF (placeholder for more sophisticated detection)."""
    return when(text_col.rlike(r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]'), 'non-english').\
           otherwise('english')

def extract_sentiment_features(text_col: Column) -> Column:
    """Extract basic sentiment features (placeholder for ML model)."""
    return struct(
        when(text_col.rlike(r'(?i)\b(excellent|amazing|fantastic|wonderful|great|love)\b'), 'positive')
        .when(text_col.rlike(r'(?i)\b(terrible|awful|horrible|hate|worst|bad)\b'), 'negative')
        .otherwise('neutral').alias('basic_sentiment'),
        
        size(split(text_col, r'(?i)\b(excellent|amazing|fantastic|wonderful|great|love)\b')) - 1,
        size(split(text_col, r'(?i)\b(terrible|awful|horrible|hate|worst|bad)\b')) - 1
    )