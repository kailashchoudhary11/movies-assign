from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# --- Error ---

class ErrorDetail(BaseModel):
    code: int
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


# --- Upload ---

class UploadResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    filename: str
    status: str
    total_rows: int
    processed_rows: int
    errors: list[str]
    created_at: datetime
    updated_at: datetime


# --- Movies ---

class MovieResponse(BaseModel):
    id: str = Field(alias="_id")
    budget: Optional[int] = None
    homepage: Optional[str] = None
    original_language: Optional[str] = None
    original_title: str
    overview: Optional[str] = None
    release_date: Optional[datetime] = None
    revenue: Optional[int] = None
    runtime: Optional[int] = None
    status: Optional[str] = None
    title: Optional[str] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    production_company_id: Optional[int] = None
    genre_id: Optional[int] = None
    languages: Optional[list[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"populate_by_name": True}


class PaginationResponse(BaseModel):
    page: int
    per_page: int
    total: int
    total_pages: int


class MovieListResponse(BaseModel):
    data: list[MovieResponse]
    pagination: PaginationResponse
