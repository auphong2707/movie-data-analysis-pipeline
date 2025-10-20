"""
Batch Views Package for Movie Data Analysis Pipeline.

This package contains modules for generating batch views from Gold layer data
and exporting them to serving systems like MongoDB.

Components:
- export_to_mongo: MongoDB export functionality for batch views
- movie_analytics: Comprehensive movie performance analytics
- genre_trends: Genre trend analysis and pattern detection  
- temporal_analysis: Year-over-year and temporal trend analysis
"""

from .export_to_mongo import MongoDBExporter
from .movie_analytics import MovieAnalyticsGenerator
from .genre_trends import GenreTrendsAnalyzer
from .temporal_analysis import TemporalAnalyzer

__all__ = [
    'MongoDBExporter',
    'MovieAnalyticsGenerator', 
    'GenreTrendsAnalyzer',
    'TemporalAnalyzer'
]

__version__ = "1.0.0"
__author__ = "TMDB Analytics Team"