import math
from datetime import datetime, timezone
from typing import Any, Optional

from pymongo import UpdateOne
from pymongo.database import Database
from pymongo.results import BulkWriteResult

from app import get_db
from app.utils.validators import SortField, SortOrder


def bulk_upsert_movies(movie_docs: list[dict[str, Any]]) -> int:
    db: Database = get_db()
    if not movie_docs:
        return 0

    operations: list[UpdateOne] = []
    now: datetime = datetime.now(timezone.utc)
    for doc in movie_docs:
        doc["updated_at"] = now
        operations.append(
            UpdateOne(
                {
                    "original_title": doc["original_title"],
                    "release_date": doc["release_date"],
                },
                {"$set": doc, "$setOnInsert": {"created_at": now}},
                upsert=True,
            )
        )

    result: BulkWriteResult = db.movies.bulk_write(operations, ordered=False)
    return result.upserted_count + result.modified_count


def get_movies(
    page: int = 1,
    per_page: int = 20,
    year: Optional[int] = None,
    language: Optional[str] = None,
    sort_by: Optional[SortField] = None,
    order: SortOrder = SortOrder.desc,
) -> dict[str, Any]:
    db: Database = get_db()

    query: dict[str, Any] = {}
    if year is not None:
        query["$expr"] = {"$eq": [{"$year": "$release_date"}, year]}
    if language is not None:
        query["original_language"] = language

    sort_field: str = sort_by.value if sort_by else "_id"
    sort_direction: int = 1 if order == "asc" else -1

    skip: int = (page - 1) * per_page
    total: int = db.movies.count_documents(query)
    total_pages: int = math.ceil(total / per_page) if total > 0 else 0

    cursor = (
        db.movies.find(query)
        .sort(sort_field, sort_direction)
        .skip(skip)
        .limit(per_page)
    )

    movies: list[dict[str, Any]] = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        movies.append(doc)

    return {
        "data": movies,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages,
        },
    }
