"""
Spark Streaming components for movie data processing.
"""

from .spark_config import create_spark_session, SCHEMAS
from .data_cleaner import DataCleaner
from .sentiment_analyzer import SentimentAnalyzer, LanguageDetector
from .main import MovieStreamingProcessor

__all__ = [
    'create_spark_session',
    'SCHEMAS',
    'DataCleaner',
    'SentimentAnalyzer',
    'LanguageDetector',
    'MovieStreamingProcessor'
]