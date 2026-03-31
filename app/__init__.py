from flask import Flask
from pymongo import MongoClient
from pymongo.database import Database

from app.config import Config

mongo_client: MongoClient | None = None
db: Database | None = None


def create_app(config_class: type = Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)

    global mongo_client, db
    mongo_client = MongoClient(app.config["MONGO_URI"])
    db = mongo_client[app.config["DATABASE_NAME"]]

    _ensure_indexes(db)

    from app.routes.upload import upload_bp
    from app.routes.movies import movies_bp

    app.register_blueprint(upload_bp, url_prefix="/api/v1")
    app.register_blueprint(movies_bp, url_prefix="/api/v1")

    return app


def get_db() -> Database:
    return db


def _ensure_indexes(database: Database) -> None:
    database.movies.create_index(
        [("original_title", 1), ("release_date", 1)], unique=True
    )
    database.movies.create_index([("release_date", 1)])
    database.movies.create_index([("vote_average", 1)])
    database.movies.create_index([("original_language", 1)])
