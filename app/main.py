from flask import Flask
from pymongo import MongoClient

from app.config import Config
import app.database as database

mongo_client: MongoClient | None = None


def create_app(config_class: type = Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)

    global mongo_client
    mongo_client = MongoClient(app.config["MONGO_URI"])
    database.db = mongo_client[app.config["DATABASE_NAME"]]

    database.init_db(database.db)

    from app.routes.upload import upload_bp
    from app.routes.movies import movies_bp

    app.register_blueprint(upload_bp, url_prefix="/api/v1")
    app.register_blueprint(movies_bp, url_prefix="/api/v1")

    return app
