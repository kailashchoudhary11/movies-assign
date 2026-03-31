import os
import tempfile
import threading

from flask import Blueprint, request, current_app, Flask
from werkzeug.datastructures import FileStorage

from app.schemas import UploadResponse, JobStatusResponse, ErrorResponse, ErrorDetail
from app.utils.validators import validate_upload_file
from app.services.csv_processor import validate_csv_headers, count_csv_rows, process_csv
from app.services.job_service import create_upload_job, get_upload_job

upload_bp: Blueprint = Blueprint("upload", __name__)


@upload_bp.route("/upload", methods=["POST"])
def upload_csv() -> tuple[dict, int]:
    file: FileStorage | None = request.files.get("file")
    valid: bool
    error: str | None
    valid, error = validate_upload_file(file)
    if not valid:
        return ErrorResponse(error=ErrorDetail(code=400, message=error)).model_dump(mode="json"), 400

    # Save to temp file
    temp_fd: int
    temp_path: str
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    os.close(temp_fd)
    file.save(temp_path)

    # Validate CSV headers
    valid, error = validate_csv_headers(temp_path)
    if not valid:
        os.remove(temp_path)
        return ErrorResponse(error=ErrorDetail(code=400, message=error)).model_dump(mode="json"), 400

    total_rows: int = count_csv_rows(temp_path)
    job_id: str = create_upload_job(file.filename, total_rows)

    # Spawn background thread
    app: Flask = current_app._get_current_object()
    thread: threading.Thread = threading.Thread(target=process_csv, args=(temp_path, job_id, app))
    thread.daemon = True
    thread.start()

    return UploadResponse(job_id=job_id, status="pending").model_dump(mode="json"), 202


@upload_bp.route("/upload/<job_id>/status", methods=["GET"])
def get_job_status(job_id: str) -> tuple[dict, int]:
    job: dict | None = get_upload_job(job_id)
    if job is None:
        return ErrorResponse(error=ErrorDetail(code=404, message="Job not found")).model_dump(mode="json"), 404
    return JobStatusResponse(**job).model_dump(mode="json"), 200
