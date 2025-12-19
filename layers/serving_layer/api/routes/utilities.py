"""
Utility Routes - Supporting endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
import logging
import re

from mongodb.client import get_mongodb_client, get_database
from mongodb.queries import MovieQueries
from api.schemas.utilities import GenresResponse, GenreInfo, MovieSearchResponse, MovieSearchResult

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/utilities",
    tags=["utilities"]
)


@router.get("/genres", response_model=GenresResponse)
async def get_genres():
    """
    Get all available genres with movie counts
    
    Returns genres sorted by movie count (descending).
    Used for dashboard variable population and filtering.
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Get all genres
        genres_data = queries.get_all_genres()
        
        # Convert to response model
        genres = [GenreInfo(**genre) for genre in genres_data]
        
        response = GenresResponse(
            genres=genres,
            total=len(genres)
        )
        
        logger.info(f"Returning {len(genres)} genres")
        return response
        
    except Exception as e:
        logger.error(f"Error getting genres: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/movies/{movie_id}")
async def get_movie_details(movie_id: int):
    """Get movie details (batch + speed merge)"""
    pass


@router.get("/search", response_model=MovieSearchResponse)
async def search_movies(
    q: Optional[str] = Query(None, description="Search query (movie title)"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    year_from: Optional[int] = Query(None, ge=1800, le=2100, description="Filter by year from"),
    year_to: Optional[int] = Query(None, ge=1800, le=2100, description="Filter by year to"),
    limit: int = Query(100, ge=1, le=200, description="Maximum number of results")
):
    """
    Search movies by title with optional filters
    
    Used for Grafana query variables to populate movie selections.
    Returns movies matching the search query with their IDs and titles.
    
    Args:
        q: Search query (partial title match, case-insensitive)
        genre: Optional genre filter
        year_from: Optional minimum release year
        year_to: Optional maximum release year
        limit: Maximum results (default 100 for dropdown population)
    
    Returns:
        List of movies with movie_id, title, year, and genre
    """
    try:
        db = get_database()
        collection = db.movie_intelligence
        
        # Build query
        query_filter = {}
        
        # Title search (case-insensitive regex)
        if q:
            # Escape special regex characters
            escaped_query = re.escape(q)
            query_filter['title'] = {'$regex': escaped_query, '$options': 'i'}
        
        # Genre filter
        if genre:
            query_filter['genre'] = genre
        
        # Year range filter
        if year_from or year_to:
            year_filter = {}
            if year_from:
                year_filter['$gte'] = year_from
            if year_to:
                year_filter['$lte'] = year_to
            if year_filter:
                query_filter['release_year'] = year_filter
        
        # Execute query
        cursor = collection.find(
            query_filter,
            {'movie_id': 1, 'title': 1, 'release_year': 1, 'genre': 1, '_id': 0}
        ).sort('popularity', -1).limit(limit)
        
        # Convert to response
        movies = []
        for doc in cursor:
            movies.append(MovieSearchResult(
                movie_id=doc.get('movie_id', 0),
                title=doc.get('title', 'Unknown'),
                year=doc.get('release_year'),
                genre=doc.get('genre')
            ))
        
        logger.info(f"Search query='{q}' genre={genre} year_range=[{year_from},{year_to}] returned {len(movies)} results")
        
        return MovieSearchResponse(
            movies=movies,
            total=len(movies),
            query=q
        )
        
    except Exception as e:
        logger.error(f"Error in movie search: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

