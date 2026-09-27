from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from taskqueue.client import TaskQueue
from taskqueue.models import Job
from taskqueue.worker import worker_status


class JobOut(BaseModel):
    id: str
    queue: str
    payload: dict
    priority: int
    status: str
    attempts: int
    max_attempts: int
    idempotency_key: str | None
    scheduled_at: float
    created_at: float
    last_error: str | None
    worker_id: str | None

    @classmethod
    def from_job(cls, job: Job) -> JobOut:
        return cls(
            id=job.id,
            queue=job.queue,
            payload=job.payload,
            priority=job.priority,
            status=job.status.value,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            idempotency_key=job.idempotency_key,
            scheduled_at=job.scheduled_at,
            created_at=job.created_at,
            last_error=job.last_error,
            worker_id=job.worker_id,
        )


_STATUS_LISTERS = {
    "pending": "list_pending",
    "in_progress": "list_in_progress",
    "dlq": "list_dlq",
    "completed": "list_completed",
}


def create_app(task_queue: TaskQueue) -> FastAPI:
    """Build the read/inspect/retry/cancel admin API over a TaskQueue's store."""
    app = FastAPI(title="taskqueue admin API")
    store = task_queue.store

    @app.get("/queues")
    def list_queues():
        return [{"name": q, **store.queue_depths(q)} for q in store.queue_names()]

    @app.get("/queues/{queue}/jobs")
    def list_jobs(queue: str, status: str = "pending", limit: int = Query(100, ge=1, le=1000)):
        lister_name = _STATUS_LISTERS.get(status)
        if lister_name is None:
            raise HTTPException(400, f"unknown status '{status}', expected one of {list(_STATUS_LISTERS)}")
        jobs = getattr(store, lister_name)(queue, limit)
        return [JobOut.from_job(j) for j in jobs]

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return JobOut.from_job(job)

    @app.post("/jobs/{job_id}/retry")
    def retry_job(job_id: str):
        job = store.retry_dlq_job(job_id)
        if job is None:
            raise HTTPException(404, "job not found or not in dead-letter queue")
        return JobOut.from_job(job)

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str):
        job = store.cancel(job_id)
        if job is None:
            raise HTTPException(404, "job not found or not pending")
        return JobOut.from_job(job)

    @app.get("/workers")
    def list_workers():
        return worker_status(task_queue)

    return app
