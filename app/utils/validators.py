from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator
from werkzeug.datastructures import FileStorage, MultiDict


ALLOWED_EXTENSIONS: set[str] = {"csv"}


class SortField(str, Enum):
    release_date = "release_date"
    vote_average = "vote_average"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


class MovieQueryParams(BaseModel):
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=20, ge=1, le=100)
    year: Optional[int] = None
    language: Optional[str] = None
    sort_by: Optional[SortField] = None
    order: SortOrder = SortOrder.desc

    @field_validator("sort_by", mode="before")
    @classmethod
    def parse_sort_by(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        return v


def validate_upload_file(file: FileStorage | None) -> tuple[bool, str | None]:
    if file is None or file.filename == "":
        return False, "No file provided"

    ext: str = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        return False, "Only CSV files are allowed"

    return True, None


def validate_movie_query_params(args: MultiDict) -> tuple[dict[str, Any] | None, list[str] | None]:
    try:
        params: MovieQueryParams = MovieQueryParams(**args.to_dict())
        return params.model_dump(), None
    except Exception as e:
        messages: list[str] = []
        if hasattr(e, "errors"):
            for err in e.errors():
                field: str = err["loc"][-1]
                msg: str = err["msg"]
                messages.append(f"{field}: {msg}")
        else:
            messages.append(str(e))
        return None, messages
