from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from pymongo.database import Database
from pymongo.results import InsertOneResult

from app import get_db


def create_job(filename: str, total_rows: int) -> str:
    db: Database = get_db()
    now: datetime = datetime.now(timezone.utc)
    job: dict[str, Any] = {
        "filename": filename,
        "status": "pending",
        "total_rows": total_rows,
        "processed_rows": 0,
        "errors": [],
        "created_at": now,
        "updated_at": now,
    }
    result: InsertOneResult = db.upload_jobs.insert_one(job)
    return str(result.inserted_id)


def get_job(job_id: str) -> dict[str, Any] | None:
    db: Database = get_db()
    try:
        return db.upload_jobs.find_one({"_id": ObjectId(job_id)})
    except Exception:
        return None


def update_job_status(job_id: str, status: str) -> None:
    db: Database = get_db()
    db.upload_jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": status, "updated_at": datetime.now(timezone.utc)}},
    )


def increment_processed_rows(job_id: str, count: int) -> None:
    db: Database = get_db()
    db.upload_jobs.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$inc": {"processed_rows": count},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
    )


def add_job_error(job_id: str, error_message: str) -> None:
    db: Database = get_db()
    db.upload_jobs.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$push": {"errors": error_message},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
    )
