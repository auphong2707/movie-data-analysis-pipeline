"""
Search Endpoints - Movie search with filters
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

from mongodb.client import get_database
from mongodb.queries import MovieQueries
from query_engine.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["search"]
)


def get_movie_queries():
    """Get MovieQueries instance"""
    db = get_database()
    return MovieQueries(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/movies")
async def search_movies(
    q: Optional[str] = Query(None, description="Search query (title, keywords)"),
    genre: Optional[str] = Query(None, description="Genre filter"),
    year_from: Optional[int] = Query(None, ge=1900, le=2100, description="Minimum year"),
    year_to: Optional[int] = Query(None, ge=1900, le=2100, description="Maximum year"),
    rating_min: Optional[float] = Query(None, ge=0, le=10, description="Minimum rating"),
    rating_max: Optional[float] = Query(None, ge=0, le=10, description="Maximum rating"),
    sort_by: str = Query("popularity", description="Sort field"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    queries: MovieQueries = Depends(get_movie_queries),
    cache = Depends(get_cache)
):
    """
    Search movies with multiple filters
    
    Supports filtering by:
    - Text search (title, keywords)
    - Genre
    - Year range
    - Rating range
    
    Sorting options:
    - popularity: Most popular first
    - rating: Highest rated first
    - release_date: Most recent first
    
    Args:
        q: Search query
        genre: Genre filter
        year_from: Minimum year
        year_to: Maximum year
        rating_min: Minimum rating
        rating_max: Maximum rating
        sort_by: Sort field (popularity, rating, release_date)
        limit: Results per page (1-100)
        offset: Pagination offset
    
    Returns:
        Search results with pagination info
    """
    try:
        # Try cache first
        cache_key = f"search:{q}:{genre}:{year_from}:{year_to}:{rating_min}:{rating_max}:{sort_by}:{limit}:{offset}"
        cached = cache.get(cache_key)
        if cached:
            logger.info("Cache hit for search")
            return cached
        
        # Execute search
        results = queries.search_movies(
            query=q,
            genre=genre,
            year_from=year_from,
            year_to=year_to,
            rating_min=rating_min,
            rating_max=rating_max,
            sort_by=sort_by,
            limit=limit,
            offset=offset
        )
        
        # Format results
        formatted_results = {
            'results': [
                {
                    'genre': r.get('genre'),
                    'year': r.get('year'),
                    'month': r.get('month'),
                    'total_movies': r.get('total_movies', 0),
                    'avg_rating': round(r.get('avg_rating', 0), 2),
                    'avg_popularity': round(r.get('avg_popularity', 0), 2)
                }
                for r in results['results']
            ],
            'pagination': {
                'total_results': results['total_results'],
                'page': results['page'],
                'total_pages': results['total_pages'],
                'limit': results['limit'],
                'offset': offset
            },
            'filters': {
                'query': q,
                'genre': genre,
                'year_range': [year_from, year_to],
                'rating_range': [rating_min, rating_max],
                'sort_by': sort_by
            }
        }
        
        # Cache result
        cache.set(cache_key, formatted_results, ttl_seconds=600)  # 10 minutes
        
        return formatted_results
        
    except Exception as e:
        logger.error(f"Error searching movies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/genres")
async def search_genres(
    q: Optional[str] = Query(None, description="Genre search query"),
    queries: MovieQueries = Depends(get_movie_queries),
    cache = Depends(get_cache)
):
    """
    Search and list available genres
    
    Args:
        q: Optional search query to filter genres
    
    Returns:
        List of genres with movie counts
    """
    try:
        # Try cache first
        cache_key = f"search:genres:{q}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        # Get all genre analytics
        all_genres = queries.get_batch_genre_analytics()
        
        # Extract unique genres with counts
        genre_stats = {}
        for g in all_genres:
            genre_name = g.get('genre')
            if genre_name:
                if genre_name not in genre_stats:
                    genre_stats[genre_name] = {
                        'genre': genre_name,
                        'total_movies': 0,
                        'avg_rating': 0
                    }
                genre_stats[genre_name]['total_movies'] += g.get('total_movies', 0)
        
        # Filter by query if provided
        if q:
            q_lower = q.lower()
            genre_stats = {
                k: v for k, v in genre_stats.items() 
                if q_lower in k.lower()
            }
        
        # Sort by movie count
        genres = sorted(
            genre_stats.values(),
            key=lambda x: x['total_movies'],
            reverse=True
        )
        
        response = {
            'genres': genres,
            'total_genres': len(genres)
        }
        
        # Cache result (long TTL - genres don't change often)
        cache.set(cache_key, response, ttl_seconds=3600)  # 1 hour
        
        return response
        
    except Exception as e:
        logger.error(f"Error searching genres: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
