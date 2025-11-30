"""
MongoDB module for serving layer
"""

from .client import get_mongodb_client, close_mongodb_client
from .queries import MovieQueries

__all__ = ['get_mongodb_client', 'close_mongodb_client', 'MovieQueries']
