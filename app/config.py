import os


class Config:
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    DATABASE_NAME = os.environ.get("DATABASE_NAME", "marrow")
    MAX_CONTENT_LENGTH = 1024 * 1024 * 1024  # 1GB
    UPLOAD_CHUNK_SIZE = 1000


class TestConfig(Config):
    DATABASE_NAME = "marrow_test"
    TESTING = True
