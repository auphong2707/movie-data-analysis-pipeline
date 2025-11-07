"""
Batch Views Module

Export Gold layer aggregations to MongoDB serving layer.
"""

from .export_to_mongo import MongoDBExporter

__all__ = ['MongoDBExporter']
