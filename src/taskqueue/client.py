from __future__ import annotations

import time

import redis

from taskqueue.models import Job
from taskqueue.retry import backoff_seconds
from taskqueue.storage import Store


class TaskQueue:
    """Public client: enqueue jobs, and the claim/complete/fail cycle workers use."""

    def __init__(self, redis_client: redis.Redis | None = None, redis_url: str = "redis://localhost:6379/0"):
        self.redis = redis_client or redis.Redis.from_url(redis_url, decode_responses=True)
        self.store = Store(self.redis)

    def enqueue(
        self,
        queue: str,
        payload: dict,
        priority: int = 0,
        max_attempts: int = 5,
        idempotency_key: str | None = None,
        delay_seconds: float = 0,
    ) -> Job | None:
        """Build and store a new job.

        Returns the enqueued Job, or None if idempotency_key was a duplicate
        already enqueued within its TTL window.
        """
        job = Job(
            queue=queue,
            payload=payload,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            scheduled_at=time.time() + delay_seconds,
        )
        return self.store.enqueue(job)

    def claim(self, queue: str, worker_id: str, lease_seconds: int = 60) -> Job | None:
        """Atomically pop the next due job from queue and lease it to worker_id."""
        return self.store.claim(queue, worker_id, lease_seconds)

    def complete(self, job: Job) -> None:
        """Mark job as completed and drop it from the in-progress set."""
        self.store.ack(job)

    def fail(self, job: Job, error: str) -> Job:
        """Record a failed attempt; reschedules with backoff or moves to DLQ."""
        # job.attempts was already incremented by claim(); attempt 0 => first backoff.
        backoff = backoff_seconds(max(job.attempts - 1, 0))
        return self.store.fail(job, error, backoff)

    def reap_expired(self, queue: str) -> list[str]:
        """Requeue any in-progress jobs whose lease has expired."""
        return self.store.reap_expired(queue)
