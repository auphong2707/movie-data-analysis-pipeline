"""
Utilities Response Schemas

Pydantic models for utility/support endpoints
"""
from pydantic import BaseModel
from typing import List


class GenreInfo(BaseModel):
    """Individual genre information"""
    name: str
    movie_count: int


class GenresResponse(BaseModel):
    """Response for GET /utilities/genres"""
    genres: List[GenreInfo]
    total: int
