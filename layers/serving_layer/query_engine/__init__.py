"""
Query Engine - Data access and similarity utilities

Active components:
- similarity_engine: Content-based similarity calculations
- utils: Utility functions for text processing

Archived components (moved to archive/):
- view_merger: Batch/speed layer merging (replaced by inline logic in API routes)
- query_router: Query routing logic (replaced by direct MongoDB queries)
- recommendation_engine: Recommendation logic (replaced by inline logic in recommendations.py)
- cache_manager: Redis caching (defined but not actively used)
"""

from .similarity_engine import build_feature_vector, calculate_similarity_score

__all__ = ['build_feature_vector', 'calculate_similarity_score']
