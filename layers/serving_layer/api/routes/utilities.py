"""
Utility Routes - Supporting endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

from mongodb.client import get_mongodb_client
from mongodb.queries import MovieQueries
from api.schemas.utilities import GenresResponse, GenreInfo

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

@router.get("/search")
async def search_movies(
    q: Optional[str] = Query(None),
    genre: Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Search movies with filters"""
    pass

