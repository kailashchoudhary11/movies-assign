from typing import Any

from pymongo.database import Database
from pymongo.errors import CollectionInvalid

db: Database | None = None


def get_db() -> Database:
    return db

MOVIES_SCHEMA: dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["original_title", "release_date"],
        "properties": {
            "budget": {"bsonType": ["int", "null"]},
            "homepage": {"bsonType": ["string", "null"]},
            "original_language": {"bsonType": ["string", "null"]},
            "original_title": {"bsonType": "string"},
            "overview": {"bsonType": ["string", "null"]},
            "release_date": {"bsonType": ["date", "null"]},
            "revenue": {"bsonType": ["int", "null"]},
            "runtime": {"bsonType": ["int", "null"]},
            "status": {"bsonType": ["string", "null"]},
            "title": {"bsonType": ["string", "null"]},
            "vote_average": {"bsonType": ["double", "null"]},
            "vote_count": {"bsonType": ["int", "null"]},
            "production_company_id": {"bsonType": ["int", "null"]},
            "genre_id": {"bsonType": ["int", "null"]},
            "languages": {
                "bsonType": ["array", "null"],
                "items": {"bsonType": "string"},
            },
            "created_at": {"bsonType": "date"},
            "updated_at": {"bsonType": "date"},
        },
    }
}

UPLOAD_JOBS_SCHEMA: dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["filename", "status"],
        "properties": {
            "filename": {"bsonType": "string"},
            "status": {"enum": ["pending", "processing", "completed", "failed"]},
            "total_rows": {"bsonType": "int"},
            "processed_rows": {"bsonType": "int"},
            "errors": {
                "bsonType": "array",
                "items": {"bsonType": "string"},
            },
            "created_at": {"bsonType": "date"},
            "updated_at": {"bsonType": "date"},
        },
    }
}


def init_db(database: Database) -> None:
    _ensure_collections(database)
    _ensure_indexes(database)


def _ensure_collections(database: Database) -> None:
    try:
        database.create_collection("movies", validator=MOVIES_SCHEMA)
    except CollectionInvalid:
        database.command("collMod", "movies", validator=MOVIES_SCHEMA)

    try:
        database.create_collection("upload_jobs", validator=UPLOAD_JOBS_SCHEMA)
    except CollectionInvalid:
        database.command("collMod", "upload_jobs", validator=UPLOAD_JOBS_SCHEMA)


def _ensure_indexes(database: Database) -> None:
    database.movies.create_index(
        [("original_title", 1), ("release_date", 1)], unique=True
    )
    database.movies.create_index([("release_date", 1)])
    database.movies.create_index([("vote_average", 1)])
    database.movies.create_index([("original_language", 1)])
