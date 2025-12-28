"""
Similarity Engine - Content-Based Filtering
Goal #3: Content Recommendation Optimization

Implements cosine similarity for movie recommendations based on:
- Genre matching (with related genres support)
- Director matching (with co-directors support)
- Franchise matching (heavily weighted)
- Budget tier similarity
- Release year proximity
"""
import numpy as np
from math import exp
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Budget tier ordering for adjacency calculation
BUDGET_TIERS = ["indie", "mid", "blockbuster", "unknown"]

# Related genres mapping (genres that often go together)
RELATED_GENRES = {
    'Action': ['Adventure', 'Science Fiction', 'Thriller'],
    'Adventure': ['Action', 'Fantasy', 'Science Fiction'],
    'Science Fiction': ['Action', 'Adventure', 'Thriller'],
    'Fantasy': ['Adventure', 'Action', 'Animation'],
    'Animation': ['Family', 'Fantasy', 'Adventure'],
    'Family': ['Animation', 'Adventure', 'Comedy'],
    'Comedy': ['Romance', 'Family', 'Drama'],
    'Romance': ['Comedy', 'Drama'],
    'Drama': ['Romance', 'Crime', 'Mystery'],
    'Crime': ['Thriller', 'Drama', 'Mystery'],
    'Thriller': ['Action', 'Crime', 'Mystery', 'Horror'],
    'Horror': ['Thriller', 'Mystery'],
    'Mystery': ['Thriller', 'Crime', 'Horror'],
    'War': ['Action', 'Drama', 'History'],
    'History': ['Drama', 'War'],
    'Western': ['Action', 'Drama'],
    'Music': ['Drama', 'Documentary'],
    'Documentary': ['History', 'Drama']
}

# Common co-director patterns (e.g., Russo Brothers)
CO_DIRECTOR_PAIRS = [
    {'Anthony Russo', 'Joe Russo'},
    {'Joel Coen', 'Ethan Coen'},
    {'Lana Wachowski', 'Lilly Wachowski'},
    {'Peter Farrelly', 'Bobby Farrelly'},
]


def build_feature_vector(movie: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a 5-dimensional feature vector for content-based similarity.
    
    Based on movie_intelligence schema fields:
    - genre: String (single genre, not array)
    - director: String
    - franchise: String or null
    - budget_tier: String ("indie", "mid", "blockbuster", "unknown")
    - release_year: Integer
    
    Args:
        movie: Movie document from MongoDB
    
    Returns:
        Dict with feature values (not one-hot encoded)
    """
    return {
        'genre': movie.get('genre', ''),           # String
        'director': movie.get('director', ''),     # String
        'franchise': movie.get('franchise', None), # String or None
        'budget_tier': movie.get('budget_tier', 'unknown'),  # String
        'release_year': movie.get('release_year', 0)  # Integer
    }


def calculate_feature_similarity(vec_a: Dict[str, Any], vec_b: Dict[str, Any]) -> np.ndarray:
    """
    Calculate similarity between two feature vectors.

    Returns a 5-element numeric vector with weighted similarity scores.
    Each dimension represents a similarity component.
    
    Feature weights (for cosine similarity):
    - Genre: 1.0 (standard weight - reduced importance)
    - Director: 4.0 (4x weight - very important!)
    - Franchise: 8.0 (8x weight - MOST important!)
    - Budget Tier: 1.0 (standard weight)
    - Year: 1.0 (standard weight)
    
    Total weight = 1 + 4 + 8 + 1 + 1 = 15
    Franchise contribution: 8/15 = ~53% (DOMINANT)
    Director contribution: 4/15 = ~27%
    Genre contribution: 1/15 = ~7% (reduced)
    Budget/Year: 1/15 each = ~7% each
    
    Args:
        vec_a: Feature vector dict
        vec_b: Feature vector dict
    
    Returns:
        numpy array of WEIGHTED similarity scores
    """
    # 1. Genre Match (0, 0.5 for related, 1 for exact)
    genre_match = calculate_genre_similarity(vec_a['genre'], vec_b['genre'])
    
    # 2. Director Match (0, 0.75 for co-directors, 1 for exact)
    director_match = calculate_director_similarity(vec_a['director'], vec_b['director'])
    
    # 3. Franchise Match (binary: 1 if same franchise, 0 otherwise)
    franchise_match = 1.0 if (vec_a['franchise'] and vec_b['franchise'] and 
                              vec_a['franchise'] == vec_b['franchise']) else 0.0
    
    # 4. Budget Tier Similarity (0, 0.5, or 1.0)
    budget_sim = calculate_budget_tier_similarity(
        vec_a['budget_tier'], 
        vec_b['budget_tier']
    )
    
    # 5. Year Proximity (exponential decay: 1.0 for same year, decreasing with distance)
    year_prox = calculate_year_proximity(
        vec_a['release_year'], 
        vec_b['release_year']
    )
    
    # Apply feature weights to increase importance of key features
    # Franchise is now DOMINANT (53%), Director is important (27%), Genre reduced (7%)
    weighted_genre = genre_match * 1.0        # Standard weight (7% - REDUCED)
    weighted_director = director_match * 4.0  # 4x weight (27% - VERY IMPORTANT)
    weighted_franchise = franchise_match * 8.0  # 8x weight (53% - DOMINANT!)
    weighted_budget = budget_sim * 1.0        # Standard weight (7%)
    weighted_year = year_prox * 1.0           # Standard weight (7%)
    
    # Return as numpy array for cosine similarity
    return np.array([weighted_genre, weighted_director, weighted_franchise, 
                     weighted_budget, weighted_year])


def calculate_genre_similarity(genre_a: str, genre_b: str) -> float:
    """
    Calculate similarity between genres, considering related genres.
    
    Args:
        genre_a: First genre
        genre_b: Second genre
    
    Returns:
        1.0 if exact match
        0.5 if related genres
        0.0 if unrelated
    """
    if not genre_a or not genre_b:
        return 0.0
    
    if genre_a == genre_b:
        return 1.0
    
    # Check if genres are related
    if genre_a in RELATED_GENRES:
        if genre_b in RELATED_GENRES[genre_a]:
            return 0.5
    
    return 0.0


def calculate_director_similarity(director_a: str, director_b: str) -> float:
    """
    Calculate similarity between directors, considering co-director teams.
    
    For example, Anthony Russo and Joe Russo (Russo Brothers) should match.
    
    Args:
        director_a: First director
        director_b: Second director
    
    Returns:
        1.0 if exact match
        0.75 if co-directors (e.g., Russo Brothers)
        0.0 if different
    """
    if not director_a or not director_b:
        return 0.0
    
    if director_a == director_b:
        return 1.0
    
    # Check if they are co-directors
    for pair in CO_DIRECTOR_PAIRS:
        if director_a in pair and director_b in pair:
            return 0.75
    
    return 0.0


def calculate_budget_tier_similarity(tier_a: str, tier_b: str) -> float:
    """
    Calculate similarity between budget tiers.
    
    Budget tier ordering:
    - indie: Low-budget independent films
    - mid: Mid-range budget films
    - blockbuster: High-budget major productions
    - unknown: Budget information not available
    
    Args:
        tier_a: First budget tier
        tier_b: Second budget tier
    
    Returns:
        1.0 if same tier
        0.5 if adjacent tiers (indie-mid, mid-blockbuster)
        0.0 if non-adjacent or unknown
    """
    if tier_a == tier_b:
        return 1.0
    
    if tier_a == 'unknown' or tier_b == 'unknown':
        return 0.0
    
    # Check if tiers are adjacent in the ordering
    try:
        idx_a = BUDGET_TIERS.index(tier_a)
        idx_b = BUDGET_TIERS.index(tier_b)
        
        # Adjacent if difference is exactly 1
        if abs(idx_a - idx_b) == 1:
            return 0.5
    except ValueError:
        pass
    
    return 0.0


def calculate_year_proximity(year_a: int, year_b: int) -> float:
    """
    Calculate proximity between release years using exponential decay.
    
    Formula: proximity = exp(-|year_diff| / decay_factor)
    
    Where:
    - decay_factor = 5 years (half-life of similarity)
    - Same year → 1.0
    - 5 years apart → ~0.37 (e^-1)
    - 10 years apart → ~0.14 (e^-2)
    - 15 years apart → ~0.05 (e^-3)
    
    Args:
        year_a: First release year
        year_b: Second release year
    
    Returns:
        Year proximity score [0, 1]
    """
    if not year_a or not year_b:
        return 0.0
    
    year_diff = abs(year_a - year_b)
    decay_factor = 5.0  # Years for similarity to decay to ~37%
    
    return exp(-year_diff / decay_factor)


def cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    """
    Calculate cosine similarity between two numpy arrays.
    
    Formula: cos(θ) = (A · B) / (||A|| * ||B||)
    
    Args:
        vec_a: First vector (numpy array)
        vec_b: Second vector (numpy array)
    
    Returns:
        Cosine similarity [-1, 1] where 1 = identical, 0 = orthogonal, -1 = opposite
    """
    # Calculate dot product
    dot_product = np.dot(vec_a, vec_b)
    
    # Calculate magnitudes
    magnitude_a = np.linalg.norm(vec_a)
    magnitude_b = np.linalg.norm(vec_b)
    
    # Avoid division by zero
    if magnitude_a == 0 or magnitude_b == 0:
        return 0.0
    
    # Cosine similarity
    return float(dot_product / (magnitude_a * magnitude_b))


def get_sentiment_boost(sentiment_a: Optional[float], sentiment_b: Optional[float]) -> float:
    """
    Calculate sentiment-aware boost multiplier.
    
    Sentiment range: -1.0 to 1.0
    - Positive sentiment: > 0.3
    - Neutral: -0.3 to 0.3
    - Negative sentiment: < -0.3
    
    Args:
        sentiment_a: First movie sentiment
        sentiment_b: Second movie sentiment
    
    Returns:
        1.2 if both positive
        1.0 if neutral
        0.8 if either negative
    """
    if sentiment_a is None or sentiment_b is None:
        return 1.0
    
    both_positive = sentiment_a > 0.3 and sentiment_b > 0.3
    either_negative = sentiment_a < -0.3 or sentiment_b < -0.3
    
    if both_positive:
        return 1.2
    elif either_negative:
        return 0.8
    else:
        return 1.0


def calculate_similarity_score(
    target_vecs: List[Dict[str, Any]],
    candidate_vec: Dict[str, Any],
    strategy: str = "average",
    target_sentiments: Optional[List[float]] = None,
    candidate_sentiment: Optional[float] = None
) -> float:
    """
    Calculate similarity score between target movie(s) and candidate.
    
    Args:
        target_vecs: List of feature vectors from input movies
        candidate_vec: Feature vector of candidate movie
        strategy: "average", "union", or "intersection"
        target_sentiments: Optional sentiment scores for target movies
        candidate_sentiment: Optional sentiment score for candidate
    
    Returns:
        Final similarity score with sentiment boost applied
    """
    if strategy == "average":
        # Average approach: Create averaged preference vector
        target_similarity_vecs = [
            calculate_feature_similarity(tv, candidate_vec) 
            for tv in target_vecs
        ]
        # Average the similarity vectors
        avg_similarity_vec = np.mean(target_similarity_vecs, axis=0)
        # Compare against candidate's self-similarity (all 1s)
        candidate_self_vec = calculate_feature_similarity(candidate_vec, candidate_vec)
        sim = cosine_similarity(avg_similarity_vec, candidate_self_vec)
        
    elif strategy == "union":
        # Max similarity to ANY target movie
        sims = []
        for tv in target_vecs:
            similarity_vec = calculate_feature_similarity(tv, candidate_vec)
            self_vec = calculate_feature_similarity(tv, tv)
            sim_score = cosine_similarity(similarity_vec, self_vec)
            sims.append(sim_score)
        sim = max(sims)
        
    elif strategy == "intersection":
        # Min similarity across ALL target movies
        sims = []
        for tv in target_vecs:
            similarity_vec = calculate_feature_similarity(tv, candidate_vec)
            self_vec = calculate_feature_similarity(tv, tv)
            sim_score = cosine_similarity(similarity_vec, self_vec)
            sims.append(sim_score)
        sim = min(sims)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
    
    # Apply sentiment boost if sentiment data available
    if target_sentiments and candidate_sentiment is not None:
        # Average sentiment across all target movies
        avg_target_sentiment = np.mean([s for s in target_sentiments if s is not None])
        sentiment_boost = get_sentiment_boost(avg_target_sentiment, candidate_sentiment)
        sim = sim * sentiment_boost
    
    return sim
