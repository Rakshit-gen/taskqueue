# Development

## Setup

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Running tests

Tests need a real reachable Redis — there's no mock/fakeredis layer. By
default they use `redis://localhost:6379/15` (db 15, to avoid colliding with
whatever's on db 0); override with `TASKQUEUE_TEST_REDIS_URL`. Each test
flushes that db before and after, so don't point it at a database with data
you care about.

```bash
redis-server &   # if you don't already have one running
pytest -q
ruff check .
```

## Running the admin API locally

```bash
REDIS_URL=redis://localhost:6379/0 uvicorn taskqueue.app:app --reload
```
