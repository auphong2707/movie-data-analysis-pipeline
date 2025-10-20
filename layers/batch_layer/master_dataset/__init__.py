"""
Master Dataset Package for Batch Layer Processing.

This package contains modules for TMDB data ingestion, schema definitions,
and partitioning strategies for the Bronze layer.
"""

from .ingestion import BronzeLayerIngestion, TMDBAPIClient
from .schema import (
    BRONZE_SCHEMA, SILVER_SCHEMA, GENRE_ANALYTICS_SCHEMA,
    TRENDING_SCORES_SCHEMA, TEMPORAL_ANALYSIS_SCHEMA, 
    ACTOR_NETWORKS_SCHEMA, QualityFlags, SentimentLabels, ViewTypes
)
from .partitioning import PartitionManager, RetentionManager

__all__ = [
    # Ingestion components
    'BronzeLayerIngestion',
    'TMDBAPIClient',
    
    # Schema definitions
    'BRONZE_SCHEMA',
    'SILVER_SCHEMA', 
    'GENRE_ANALYTICS_SCHEMA',
    'TRENDING_SCORES_SCHEMA',
    'TEMPORAL_ANALYSIS_SCHEMA',
    'ACTOR_NETWORKS_SCHEMA',
    'QualityFlags',
    'SentimentLabels',
    'ViewTypes',
    
    # Partitioning utilities
    'PartitionManager',
    'RetentionManager'
]

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'