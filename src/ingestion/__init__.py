"""
Movie data ingestion package.
"""

from .tmdb_client import TMDBClient
from .kafka_producer import MovieDataProducer
from .data_extractor import DataExtractor
from .main import IngestionPipeline

__all__ = [
    'TMDBClient',
    'MovieDataProducer', 
    'DataExtractor',
    'IngestionPipeline'
]