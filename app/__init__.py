from typing import Any

from flask import Flask
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import CollectionInvalid

from app.config import Config

mongo_client: MongoClient | None = None
db: Database | None = None

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


def create_app(config_class: type = Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)

    global mongo_client, db
    mongo_client = MongoClient(app.config["MONGO_URI"])
    db = mongo_client[app.config["DATABASE_NAME"]]

    _ensure_collections(db)
    _ensure_indexes(db)

    from app.routes.upload import upload_bp
    from app.routes.movies import movies_bp

    app.register_blueprint(upload_bp, url_prefix="/api/v1")
    app.register_blueprint(movies_bp, url_prefix="/api/v1")

    return app


def get_db() -> Database:
    return db


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
