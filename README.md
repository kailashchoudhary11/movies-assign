# Marrow

Movie data upload and review system built with Flask and MongoDB.

## Setup

You need Python 3.11+ and MongoDB running on port 27017.

Easiest way to get MongoDB running:
```bash
docker run -d -p 27017:27017 --name marrow-mongo mongo
```

Then create a virtual environment, install dependencies, and start the server:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python run.py
```

Server runs at `http://localhost:5000`.

## How to use

There's a `client.py` script to interact with the API.

### Upload a CSV

```bash
python client.py upload path/to/movies.csv
```

This uploads the file, then polls until processing is done. You'll see progress printed as it goes.

### Browse movies

```bash
# all movies
python client.py movies

# filter by year and language
python client.py movies --year 1995 --language en

# sort by rating
python client.py movies --sort_by vote_average --order desc

# pagination
python client.py movies --page 2 --per_page 10
```



## How it works

- CSV uploads are processed in the background so the API responds immediately
- Large files (up to 1GB) are read in chunks of 1000 rows to keep memory usage low
- Duplicate movies (same title + release date) get updated instead of duplicated
- Upload progress is tracked and can be checked via the status endpoint
