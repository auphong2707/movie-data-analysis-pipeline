"""
Utility functions for query engine operations
"""

import re
from difflib import SequenceMatcher


def normalize_title(title: str) -> str:
    """
    Normalize movie title for matching
    
    Removes special characters, converts to lowercase, normalizes whitespace
    
    Args:
        title: Movie title to normalize
        
    Returns:
        Normalized title string
    """
    if not title:
        return ""
    
    # Remove special characters except spaces and alphanumeric
    normalized = re.sub(r'[^\w\s]', '', title.lower())
    
    # Normalize multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def fuzzy_match_title(title1: str, title2: str, threshold: float = 0.8) -> bool:
    """
    Fuzzy match two movie titles
    
    Uses SequenceMatcher for similarity comparison
    
    Args:
        title1: First title
        title2: Second title
        threshold: Minimum similarity ratio (0-1)
        
    Returns:
        True if titles match above threshold
    """
    if not title1 or not title2:
        return False
    
    # Normalize both titles
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)
    
    # Check exact match first
    if norm1 == norm2:
        return True
    
    # Use SequenceMatcher for fuzzy comparison
    ratio = SequenceMatcher(None, norm1, norm2).ratio()
    
    return ratio >= threshold


def extract_base_title(title: str) -> str:
    """
    Extract base title by removing common patterns
    
    Removes year, "The", special editions, etc.
    
    Args:
        title: Movie title
        
    Returns:
        Base title string
    """
    if not title:
        return ""
    
    # Remove year patterns (1990-2099)
    base = re.sub(r'\b(19|20)\d{2}\b', '', title)
    
    # Remove "The" at start
    base = re.sub(r'^the\s+', '', base, flags=re.IGNORECASE)
    
    # Remove special edition markers
    base = re.sub(r'\(.*?\)', '', base)  # Remove parenthetical content
    base = re.sub(r'\[.*?\]', '', base)  # Remove bracketed content
    
    return normalize_title(base)
