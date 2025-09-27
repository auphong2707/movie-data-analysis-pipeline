"""
Data partitioning strategies for the movie analytics pipeline.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import null
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class PartitioningStrategy:
    """Implements partitioning strategies for different data types."""
    
    @staticmethod
    def get_movie_partitioning() -> Dict[str, Any]:
        """Get partitioning strategy for movie data."""
        return {
            'bronze': {
                'columns': ['release_year', 'language'],
                'description': 'Partition by release year and language for time-based and regional queries'
            },
            'silver': {
                'columns': ['release_year', 'popularity_tier'], 
                'description': 'Partition by release year and popularity tier for analytics queries'
            },
            'gold': {
                'columns': ['release_decade'],
                'description': 'Partition by decade for high-level trend analysis'
            }
        }
    
    @staticmethod
    def get_review_partitioning() -> Dict[str, Any]:
        """Get partitioning strategy for review data."""
        return {
            'bronze': {
                'columns': ['review_date'],
                'description': 'Partition by review date for temporal queries'
            },
            'silver': {
                'columns': ['review_date', 'sentiment_label'],
                'description': 'Partition by date and sentiment for sentiment analysis'
            },
            'gold': {
                'columns': ['year', 'month'],
                'description': 'Partition by year/month for time-series analysis'
            }
        }
    
    @staticmethod
    def get_credit_partitioning() -> Dict[str, Any]:
        """Get partitioning strategy for credit data."""
        return {
            'bronze': {
                'columns': ['credit_type'],
                'description': 'Partition by credit type (cast/crew)'
            },
            'silver': {
                'columns': ['credit_type', 'department'],
                'description': 'Partition by credit type and department for role-based queries'
            },
            'gold': {
                'columns': ['decade', 'credit_type'],
                'description': 'Partition by decade and credit type for trend analysis'
            }
        }
    
    @staticmethod
    def get_people_partitioning() -> Dict[str, Any]:
        """Get partitioning strategy for people data."""
        return {
            'bronze': {
                'columns': ['known_for_department'],
                'description': 'Partition by department for role-based queries'
            },
            'silver': {
                'columns': ['known_for_department', 'popularity_tier'],
                'description': 'Partition by department and popularity for analytics'
            },
            'gold': {
                'columns': ['birth_decade', 'known_for_department'],
                'description': 'Partition by birth decade and department for demographic analysis'
            }
        }
    
    @staticmethod
    def add_partition_columns(df: DataFrame, data_type: str, layer: str) -> DataFrame:
        """Add partition columns to DataFrame based on data type and layer."""
        
        if data_type == 'movies':
            df = df.withColumn('release_decade', 
                              (floor(year(col('release_date')) / 10) * 10).cast(IntegerType()))
            
            if layer in ['bronze', 'silver']:
                df = df.withColumn('release_year', year(col('release_date')))
            
        elif data_type == 'reviews':
            df = df.withColumn('year', year(col('review_date'))) \
                   .withColumn('month', month(col('review_date')))
        
        elif data_type == 'credits':
            if layer == 'gold':
                # Assuming we have movie data joined to get release dates
                if 'release_date' in df.columns:
                    df = df.withColumn('decade', 
                                      (floor(year(col('release_date')) / 10) * 10).cast(IntegerType()))
        
        elif data_type == 'people':
            if 'birthday' in df.columns:
                df = df.withColumn('birth_decade',
                                  when(col('birthday').isNotNull(),
                                       (floor(year(col('birthday')) / 10) * 10).cast(IntegerType()))
                                  .otherwise(null()))
        
        return df

class DataQualityRules:
    """Data quality rules and validation for each layer."""
    
    @staticmethod
    def get_bronze_quality_rules() -> Dict[str, List[str]]:
        """Quality rules for Bronze layer (minimal validation)."""
        return {
            'movies': [
                'id IS NOT NULL',
                'title IS NOT NULL AND title != ""',
                'ingestion_timestamp IS NOT NULL'
            ],
            'reviews': [
                'id IS NOT NULL',
                'movie_id IS NOT NULL',
                'content IS NOT NULL AND content != ""',
                'author IS NOT NULL'
            ],
            'credits': [
                'movie_id IS NOT NULL',
                'person_id IS NOT NULL',
                'credit_type IN ("cast", "crew")'
            ],
            'people': [
                'id IS NOT NULL',
                'name IS NOT NULL AND name != ""'
            ]
        }
    
    @staticmethod
    def get_silver_quality_rules() -> Dict[str, List[str]]:
        """Quality rules for Silver layer (enhanced validation)."""
        return {
            'movies': [
                'movie_id IS NOT NULL',
                'title IS NOT NULL AND title != ""',
                'release_date IS NOT NULL',
                'vote_average >= 0 AND vote_average <= 10',
                'vote_count >= 0',
                'popularity >= 0',
                'budget >= 0',
                'revenue >= 0'
            ],
            'reviews': [
                'review_id IS NOT NULL',
                'movie_id IS NOT NULL',
                'content IS NOT NULL AND length(content) > 10',
                'word_count > 0',
                'sentiment_score >= -1 AND sentiment_score <= 1',
                'sentiment_label IN ("positive", "negative", "neutral")'
            ],
            'credits': [
                'movie_id IS NOT NULL',
                'person_id IS NOT NULL',
                'person_name IS NOT NULL AND person_name != ""',
                'credit_type IN ("cast", "crew")',
                'popularity >= 0'
            ],
            'people': [
                'person_id IS NOT NULL',
                'name IS NOT NULL AND name != ""',
                'popularity >= 0',
                'gender IN (0, 1, 2, 3) OR gender IS NULL'
            ]
        }
    
    @staticmethod
    def get_gold_quality_rules() -> Dict[str, List[str]]:
        """Quality rules for Gold layer (business logic validation)."""
        return {
            'movie_summary': [
                'movie_count > 0',
                'avg_rating >= 0 AND avg_rating <= 10',
                'avg_popularity >= 0'
            ],
            'genre_summary': [
                'movie_count >= 5',  # Minimum movies per genre
                'avg_rating >= 0 AND avg_rating <= 10',
                'genre_name IS NOT NULL'
            ],
            'trending_movies': [
                'trend_score > 0',
                'window_start IS NOT NULL',
                'window_end IS NOT NULL'
            ],
            'sentiment_trends': [
                'review_count > 0',
                'avg_sentiment >= -1 AND avg_sentiment <= 1',
                'sentiment_ratio >= 0 AND sentiment_ratio <= 1'
            ]
        }
    
    @staticmethod
    def validate_data_quality(df: DataFrame, data_type: str, layer: str) -> Dict[str, Any]:
        """Validate data quality based on rules."""
        if layer == 'bronze':
            rules = DataQualityRules.get_bronze_quality_rules().get(data_type, [])
        elif layer == 'silver':
            rules = DataQualityRules.get_silver_quality_rules().get(data_type, [])
        elif layer == 'gold':
            rules = DataQualityRules.get_gold_quality_rules().get(data_type, [])
        else:
            return {'error': f'Unknown layer: {layer}'}
        
        validation_results = {
            'total_records': df.count(),
            'rules_passed': 0,
            'rules_failed': 0,
            'rule_results': []
        }
        
        for rule in rules:
            try:
                valid_count = df.filter(rule).count()
                invalid_count = validation_results['total_records'] - valid_count
                
                rule_result = {
                    'rule': rule,
                    'valid_records': valid_count,
                    'invalid_records': invalid_count,
                    'pass_rate': valid_count / validation_results['total_records'] if validation_results['total_records'] > 0 else 0,
                    'passed': invalid_count == 0
                }
                
                if rule_result['passed']:
                    validation_results['rules_passed'] += 1
                else:
                    validation_results['rules_failed'] += 1
                
                validation_results['rule_results'].append(rule_result)
                
            except Exception as e:
                validation_results['rule_results'].append({
                    'rule': rule,
                    'error': str(e),
                    'passed': False
                })
                validation_results['rules_failed'] += 1
        
        validation_results['overall_quality_score'] = (
            validation_results['rules_passed'] / 
            (validation_results['rules_passed'] + validation_results['rules_failed'])
            if (validation_results['rules_passed'] + validation_results['rules_failed']) > 0 else 0
        )
        
        return validation_results

class StorageOptimization:
    """Storage optimization techniques for the data lake."""
    
    @staticmethod
    def get_compression_settings() -> Dict[str, str]:
        """Get optimal compression settings for different data types."""
        return {
            'parquet.compression': 'snappy',  # Good balance of compression and speed
            'parquet.block.size': '134217728',  # 128MB block size
            'parquet.page.size': '1048576',  # 1MB page size
            'parquet.dictionary.page.size': '1048576',  # 1MB dictionary page size
            'parquet.enable.dictionary': 'true'
        }
    
    @staticmethod
    def get_file_size_targets() -> Dict[str, int]:
        """Get target file sizes (in MB) for different layers."""
        return {
            'bronze': 64,   # Smaller files for frequent writes
            'silver': 128,  # Medium files for balanced performance
            'gold': 256     # Larger files for analytical queries
        }
    
    @staticmethod
    def calculate_optimal_partitions(df: DataFrame, target_file_size_mb: int = 128) -> int:
        """Calculate optimal number of partitions based on data size."""
        try:
            # Estimate data size (rough calculation)
            row_count = df.count()
            if row_count == 0:
                return 1
            
            # Estimate average row size (in bytes)
            sample_size = min(1000, row_count)
            sample_df = df.limit(sample_size)
            
            # Convert to pandas for size estimation
            sample_pandas = sample_df.toPandas()
            avg_row_size_bytes = sample_pandas.memory_usage(deep=True).sum() / sample_size
            
            # Calculate total size and optimal partitions
            total_size_mb = (row_count * avg_row_size_bytes) / (1024 * 1024)
            optimal_partitions = max(1, int(total_size_mb / target_file_size_mb))
            
            return optimal_partitions
            
        except Exception as e:
            logger.warning(f"Error calculating optimal partitions, using default: {e}")
            return max(1, int(df.rdd.getNumPartitions() / 2))  # Conservative default