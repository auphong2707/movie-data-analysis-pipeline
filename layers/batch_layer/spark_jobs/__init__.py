"""
Spark Jobs Package for Batch Layer Processing.

This package contains PySpark jobs for transforming data between layers:
- Bronze to Silver transformation with cleaning and enrichment
- Silver to Gold aggregations and analytics
- Sentiment analysis batch processing
- Actor networks analysis
"""

from .bronze_to_silver import BronzeToSilverTransformer
from .silver_to_gold import SilverToGoldTransformer
from .sentiment_batch import SentimentBatchProcessor
from .actor_networks import ActorNetworksAnalyzer

__all__ = [
    'BronzeToSilverTransformer',
    'SilverToGoldTransformer', 
    'SentimentBatchProcessor',
    'ActorNetworksAnalyzer'
]

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'