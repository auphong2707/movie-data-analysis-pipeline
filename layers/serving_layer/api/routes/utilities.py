"""
Utility Routes - Supporting endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

router = APIRouter(
    prefix="/utilities",
    tags=["utilities"]
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
