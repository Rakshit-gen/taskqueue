# taskqueue

A Redis-backed distributed task queue: retries with exponential backoff and
jitter, a dead-letter queue, cron-style scheduling, priority queues,
idempotency keys, worker heartbeats, and an admin API to inspect and retry
jobs.

## Quickstart

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

redis-server &   # or point REDIS_URL at any reachable Redis
```

Enqueue a job:

```python
from taskqueue.client import TaskQueue

tq = TaskQueue(redis_url="redis://localhost:6379/0")
tq.enqueue("emails", {"to": "a@example.com"}, priority=5, max_attempts=3)
```

Run a worker:

```python
from taskqueue.client import TaskQueue
from taskqueue.worker import Worker

tq = TaskQueue(redis_url="redis://localhost:6379/0")


def handle(job):
    print("processing", job.payload)


Worker(tq, ["emails"], handle).run_forever()
```

Register a recurring job:

```python
from taskqueue.scheduler import Scheduler

Scheduler(tq).register("nightly-digest", queue="emails", payload={"kind": "digest"}, cron="0 2 * * *")
# then run Scheduler(tq).run_forever() in a process alongside your workers
```

Run the admin API:

```bash
REDIS_URL=redis://localhost:6379/0 uvicorn taskqueue.app:app --reload
```

| Endpoint | Purpose |
|---|---|
| `GET /queues` | queue names + pending/in_progress/dlq depths |
| `GET /queues/{queue}/jobs?status=pending\|in_progress\|dlq\|completed` | list jobs by status |
| `GET /jobs/{id}` | job detail |
| `POST /jobs/{id}/retry` | move a dead-lettered job back to pending |
| `POST /jobs/{id}/cancel` | cancel a pending (not yet claimed) job |
| `GET /workers` | live/dead workers by heartbeat |

## Measured vs Claimed

Every number below states what it measures and how. Update this table
instead of writing new numbers straight into prose elsewhere (portfolio site,
resume) — keep one source of truth and link to it.

| Claim | Value | How measured | Date |
|---|---|---|---|
| _e.g. p50 latency_ | _e.g. 42ms_ | _e.g. `wrk -t4 -c100 -d30s`, local, M-series laptop_ | _YYYY-MM-DD_ |

## Architecture

- **Storage** (`storage.py`): one Redis sorted set per queue per state
  (`pending`, `inprogress`, `dlq`), plus a capped recent-history list for
  `completed`. Pending jobs are scored by `scheduled_at`, with priority
  folded in as a sub-second tiebreak, so one data structure covers delayed
  scheduling and priority ordering together.
- **Atomic claim**: `claim()` runs a Lua script that pops the lowest-scored
  due job from `pending` and adds it to `inprogress` with a lease deadline
  in one atomic step, so concurrent workers never double-claim.
- **Retries** (`retry.py`): exponential backoff with full jitter
  (`random(0, min(cap, base * 2^attempt))`), rescheduled through the same
  sorted set as fresh jobs. After `max_attempts` a job moves to the
  per-queue dead-letter set instead of being rescheduled.
- **Lease expiry**: `reap_expired()` finds `inprogress` jobs whose lease
  passed and requeues them through the normal failure path, so a worker
  that crashes mid-job doesn't strand it forever.
- **Idempotency**: an optional `idempotency_key` is deduped via `SET NX EX`
  before a job is stored, so re-enqueueing the same logical job within the
  TTL window is a no-op.
- **Scheduler** (`scheduler.py`): recurring jobs are registered as Redis
  hashes (queue, payload, cron-or-interval, `next_run`) so schedule state
  survives process restarts. `tick()` enqueues anything due and advances
  `next_run` via `croniter` for cron expressions, or `now + interval` for
  plain intervals.
- **Worker** (`worker.py`): `run_forever()` claims across its configured
  queues, invokes the handler, acks or fails based on the outcome, and
  periodically refreshes a TTL heartbeat key and reaps expired leases.
  `worker_status()` reports a worker as live or dead based on whether its
  heartbeat key still exists.
- **Admin API** (`admin_api.py`, `app.py`): a thin FastAPI layer over
  `Store` and `worker_status()` — no business logic of its own.

## Status

Storage, retries, DLQ, idempotency, lease reaping, the cron/interval
scheduler, the worker loop, and the admin API all have passing tests run
against a real Redis (locally and in CI via a `redis:7-alpine` service
container) — this isn't tested against fakeredis or mocks.

Known simplifications, not bugs:

- The priority tiebreak in the pending-queue score only holds within jobs
  scheduled in roughly the same second; two jobs scheduled seconds apart
  are always ordered by time first, priority second (see the comment in
  `storage.py`).
- `completed` history is capped at the last 100 jobs per queue — it's for
  spot-checking in the admin API, not a durable audit log.
- The worker registry (`tq:workers`) never prunes entries, so a long-lived
  deployment that churns through many short-lived worker processes will
  accumulate dead entries in `GET /workers` over time.
- The admin API has no authentication — it's meant to sit behind something
  that already gates access (internal network, reverse-proxy auth), not to
  be exposed directly.
- Single-Redis-instance design throughout; no support for Redis Cluster key
  hashing.
