"""
Utilities Response Schemas

Pydantic models for utility/support endpoints
"""
from pydantic import BaseModel
from typing import List, Optional


class GenreInfo(BaseModel):
    """Individual genre information"""
    name: str
    movie_count: int


class GenresResponse(BaseModel):
    """Response for GET /utilities/genres"""
    genres: List[GenreInfo]
    total: int


class MovieSearchResult(BaseModel):
    """Individual movie search result"""
    movie_id: int
    title: str
    year: Optional[int] = None
    genre: Optional[str] = None


class MovieSearchResponse(BaseModel):
    """Response for GET /utilities/search"""
    movies: List[MovieSearchResult]
    total: int
    query: Optional[str] = None
