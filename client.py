import sys
import time
from typing import Any

import requests

BASE_URL: str = "http://localhost:5000/api/v1"


def upload_csv(file_path: str) -> None:
    print(f"Uploading {file_path}...")
    with open(file_path, "rb") as f:
        response: requests.Response = requests.post(
            f"{BASE_URL}/upload",
            files={"file": (file_path.split("/")[-1], f, "text/csv")},
        )

    if response.status_code != 202:
        print(f"Upload failed: {response.json()}")
        return

    job_id: str = response.json()["job_id"]
    print(f"Job created: {job_id}")

    # Poll until complete
    while True:
        status_resp: requests.Response = requests.get(f"{BASE_URL}/upload/{job_id}/status")
        job: dict[str, Any] = status_resp.json()
        status: str = job["status"]
        processed: int = job["processed_rows"]
        total: int = job["total_rows"]

        print(f"  Status: {status} ({processed}/{total} rows)")

        if status == "completed":
            print("Upload completed!")
            break
        elif status == "failed":
            print(f"Upload failed! Errors: {job['errors']}")
            break

        time.sleep(1)


def get_movies(
    page: int = 1,
    per_page: int = 20,
    year: int | None = None,
    language: str | None = None,
    sort_by: str = "release_date",
    order: str = "desc",
) -> None:
    params: dict[str, Any] = {
        "page": page,
        "per_page": per_page,
        "sort_by": sort_by,
        "order": order,
    }
    if year:
        params["year"] = year
    if language:
        params["language"] = language

    response: requests.Response = requests.get(f"{BASE_URL}/movies", params=params)

    if response.status_code != 200:
        print(f"Error: {response.json()}")
        return

    import json
    print(json.dumps(response.json(), indent=2))


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py upload <csv_path>")
        print("  python client.py movies [--page 1] [--per_page 20] [--year 2023] [--language en] [--sort_by vote_average] [--order desc]")
        return

    command: str = sys.argv[1]

    if command == "upload":
        if len(sys.argv) < 3:
            print("Usage: python client.py upload <csv_path>")
            return
        csv_path: str = sys.argv[2]
        upload_csv(csv_path)

    elif command == "movies":
        kwargs: dict[str, Any] = {}
        args: list[str] = sys.argv[2:]
        i: int = 0
        while i < len(args):
            key: str = args[i].lstrip("-")
            value: str = args[i + 1]
            if key in ("page", "per_page", "year"):
                kwargs[key] = int(value)
            else:
                kwargs[key] = value
            i += 2
        get_movies(**kwargs)

    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
