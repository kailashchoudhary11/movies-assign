from typing import Any

from app.models.job import create_job, get_job, update_job_status


def create_upload_job(filename: str, total_rows: int) -> str:
    return create_job(filename, total_rows)


def get_upload_job(job_id: str) -> dict[str, Any] | None:
    job: dict[str, Any] | None = get_job(job_id)
    if job is None:
        return None
    return {
        "job_id": str(job["_id"]),
        "filename": job["filename"],
        "status": job["status"],
        "total_rows": job["total_rows"],
        "processed_rows": job["processed_rows"],
        "errors": job["errors"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
    }


def mark_job_processing(job_id: str) -> None:
    update_job_status(job_id, "processing")


def mark_job_completed(job_id: str) -> None:
    update_job_status(job_id, "completed")


def mark_job_failed(job_id: str) -> None:
    update_job_status(job_id, "failed")
