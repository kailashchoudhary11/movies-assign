from typing import Any

from flask import Blueprint, request

from app.models.movie import get_movies
from app.schemas import MovieListResponse, ErrorResponse, ErrorDetail
from app.utils.validators import validate_movie_query_params

movies_bp: Blueprint = Blueprint("movies", __name__)


@movies_bp.route("/movies", methods=["GET"])
def list_movies() -> tuple[dict, int]:
    params: dict[str, Any] | None
    errors: list[str] | None
    params, errors = validate_movie_query_params(request.args)
    if errors:
        return ErrorResponse(error=ErrorDetail(code=400, message="; ".join(errors))).model_dump(mode="json"), 400

    result: dict[str, Any] = get_movies(
        page=params["page"],
        per_page=params["per_page"],
        year=params["year"],
        language=params["language"],
        sort_by=params["sort_by"],
        order=params["order"],
    )
    return MovieListResponse(**result).model_dump(mode="json"), 200
