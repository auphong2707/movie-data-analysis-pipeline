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


@router.get("/grafana/genres")
async def get_genres_for_grafana():
    """
    Get genres formatted for Grafana variable dropdowns
    
    Returns genres in Grafana Infinity datasource format with 'value' and 'text' fields.
    Useful for genre filter dropdowns in dashboards.
    
    Returns:
        Array of objects with:
        - value: genre name (string) - used as the variable value
        - text: "Genre Name (count)" - displayed in dropdown
    
    Example response:
    [
        {"value": "Drama", "text": "Drama (1059)"},
        {"value": "Comedy", "text": "Comedy (583)"}
    ]
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Get all genres
        genres_data = queries.get_all_genres()
        
        # Format for Grafana
        genres = []
        for genre in genres_data:
            name = genre.get('name', 'Unknown')
            count = genre.get('movie_count', 0)
            
            genres.append({
                "value": name,
                "text": f"{name} ({count})"
            })
        
        logger.info(f"Returning {len(genres)} genres for Grafana")
        return genres
        
    except Exception as e:
        logger.error(f"Error getting genres for Grafana: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/movies/{movie_id}")
async def get_movie_details(movie_id: int):
    """Get movie details (batch + speed merge)"""
    pass


@router.get("/grafana/movies")
async def get_movies_for_grafana(
    q: Optional[str] = Query(None, description="Search query (movie title)"),
    limit: Optional[int] = Query(None, ge=1, le=5000, description="Optional limit (default: all movies)")
):
    """
    Get movies formatted for Grafana variable dropdowns
    
    Returns movies in Grafana Infinity datasource format with 'value' and 'text' fields.
    This endpoint is specifically designed for Grafana query variables.
    
    Args:
        q: Optional search query (partial title match, case-insensitive)
        limit: Optional limit (default: returns all movies, sorted by popularity)
    
    Returns:
        Array of objects with:
        - value: movie_id (integer) - used as the variable value
        - text: "Movie Title (Year)" - displayed in dropdown
    
    Example response:
    [
        {"value": 299534, "text": "Avengers: Endgame (2019)"},
        {"value": 157336, "text": "Interstellar (2014)"}
    ]
    """
    try:
        db = get_database()
        collection = db.movie_intelligence
        
        # Build query
        query_filter = {}
        
        # Title search (case-insensitive regex)
        if q:
            escaped_query = re.escape(q)
            query_filter['title'] = {'$regex': escaped_query, '$options': 'i'}
        
        # Execute query - sorted by popularity for best results
        cursor = collection.find(
            query_filter,
            {'movie_id': 1, 'title': 1, 'release_year': 1, '_id': 0}
        ).sort('popularity', -1)
        
        # Apply limit only if specified
        if limit:
            cursor = cursor.limit(limit)
        
        # Format for Grafana
        movies = []
        for doc in cursor:
            movie_id = doc.get('movie_id', 0)
            title = doc.get('title', 'Unknown')
            year = doc.get('release_year', '')
            
            # Format: "Movie Title (Year)"
            display_text = f"{title} ({year})" if year else title
            
            movies.append({
                "value": movie_id,
                "text": display_text
            })
        
        logger.info(f"Grafana movie search query='{q}' returned {len(movies)} results")
        
        return movies
        
    except Exception as e:
        logger.error(f"Error in Grafana movie search: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


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

