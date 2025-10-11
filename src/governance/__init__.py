"""
Governance package for data lineage and metadata management.
"""

from .datahub_lineage import DataHubLineageTracker, get_lineage_tracker

__all__ = [
    'DataHubLineageTracker',
    'get_lineage_tracker'
]