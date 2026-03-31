import os
from datetime import datetime
from typing import Any, TextIO

import pandas as pd
from flask import Flask

from app.models.movie import bulk_upsert_movies
from app.models.job import increment_processed_rows, add_job_error
from app.services.job_service import mark_job_processing, mark_job_completed, mark_job_failed


EXPECTED_HEADERS: set[str] = {
    "budget", "homepage", "original_language", "original_title", "overview",
    "release_date", "revenue", "runtime", "status", "title",
    "vote_average", "vote_count", "production_company_id", "genre_id", "languages",
}


def validate_csv_headers(file_path: str) -> tuple[bool, str | None]:
    df: pd.DataFrame = pd.read_csv(file_path, nrows=0)
    headers: set[str] = set(df.columns.str.strip())
    missing: set[str] = EXPECTED_HEADERS - headers
    if missing:
        return False, f"Missing columns: {', '.join(sorted(missing))}"
    return True, None


def count_csv_rows(file_path: str) -> int:
    count: int = 0
    f: TextIO
    with open(file_path, "r") as f:
        for _ in f:
            count += 1
    return max(count - 1, 0)


def process_csv(file_path: str, job_id: str, app: Flask) -> None:
    with app.app_context():
        try:
            mark_job_processing(job_id)
            chunk_size: int = app.config.get("UPLOAD_CHUNK_SIZE", 1000)

            chunk: pd.DataFrame
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                movie_docs: list[dict[str, Any]] = []
                _: int
                row: pd.Series
                for _, row in chunk.iterrows():
                    try:
                        doc: dict[str, Any] = _build_movie_doc(row)
                        movie_docs.append(doc)
                    except Exception as e:
                        add_job_error(job_id, f"Row error: {str(e)}")

                if movie_docs:
                    bulk_upsert_movies(movie_docs)

                increment_processed_rows(job_id, len(chunk))

            mark_job_completed(job_id)
        except Exception as e:
            add_job_error(job_id, f"Fatal error: {str(e)}")
            mark_job_failed(job_id)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)


def _build_movie_doc(row: pd.Series) -> dict[str, Any]:
    release_date: datetime | None = None
    if pd.notna(row.get("release_date")):
        release_date = pd.to_datetime(row["release_date"])

    languages: list[str] | None = None
    if pd.notna(row.get("languages")):
        raw: str = str(row["languages"])
        # CSV has Python-style lists like "['English', 'Français']"
        cleaned: str = raw.strip("[]").replace("'", "").replace('"', "")
        languages = [lang.strip() for lang in cleaned.split(",") if lang.strip()]

    doc: dict[str, Any] = {
        "budget": _safe_number(row.get("budget")),
        "homepage": str(row.get("homepage", "")) if pd.notna(row.get("homepage")) else None,
        "original_language": str(row.get("original_language", "")).strip() if pd.notna(row.get("original_language")) else None,
        "original_title": str(row["original_title"]).strip(),
        "overview": str(row.get("overview", "")) if pd.notna(row.get("overview")) else None,
        "release_date": release_date,
        "revenue": _safe_number(row.get("revenue")),
        "runtime": _safe_number(row.get("runtime")),
        "status": str(row.get("status", "")) if pd.notna(row.get("status")) else None,
        "title": str(row.get("title", "")).strip() if pd.notna(row.get("title")) else None,
        "vote_average": _safe_float(row.get("vote_average")),
        "vote_count": _safe_number(row.get("vote_count")),
        "production_company_id": _safe_number(row.get("production_company_id")),
        "genre_id": _safe_number(row.get("genre_id")),
        "languages": languages,
    }
    return doc


def _safe_number(value: Any) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
