from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable

from taskqueue.client import TaskQueue
from taskqueue.models import Job

HEARTBEAT_TTL = 30
_WORKER_REGISTRY = "tq:workers"


def _worker_key(worker_id: str) -> str:
    return f"tq:worker:{worker_id}"


class Worker:
    """Pulls jobs from one or more queues and runs them against a handler."""

    def __init__(
        self,
        task_queue: TaskQueue,
        queues: list[str],
        handler: Callable[[Job], None],
        worker_id: str | None = None,
        lease_seconds: int = 60,
        heartbeat_ttl: int = HEARTBEAT_TTL,
    ):
        self.tq = task_queue
        self.queues = queues
        self.handler = handler
        self.worker_id = worker_id or str(uuid.uuid4())
        self.lease_seconds = lease_seconds
        self.heartbeat_ttl = heartbeat_ttl

    def heartbeat(self) -> None:
        """Refresh this worker's liveness key (TTL heartbeat_ttl seconds)."""
        r = self.tq.redis
        r.sadd(_WORKER_REGISTRY, self.worker_id)
        r.set(
            _worker_key(self.worker_id),
            json.dumps({"queues": self.queues, "last_seen": time.time()}),
            ex=self.heartbeat_ttl,
        )

    def run_once(self) -> bool:
        """Claim and process a single job from the first non-empty queue.

        Returns True if a job was processed, False if all queues were empty.
        """
        for queue in self.queues:
            job = self.tq.claim(queue, self.worker_id, self.lease_seconds)
            if job is None:
                continue
            try:
                self.handler(job)
            except Exception as exc:  # noqa: BLE001 - any handler error is a job failure
                self.tq.fail(job, repr(exc))
            else:
                self.tq.complete(job)
            return True
        return False

    def run_forever(self, poll_interval: float = 1.0, reap_interval: float = 30.0) -> None:
        """Loop forever: heartbeat, periodically reap expired leases, process jobs."""
        last_reap = 0.0
        while True:
            self.heartbeat()
            now = time.time()
            if now - last_reap > reap_interval:
                for queue in self.queues:
                    self.tq.reap_expired(queue)
                last_reap = now
            if not self.run_once():
                time.sleep(poll_interval)


def worker_status(task_queue: TaskQueue) -> list[dict]:
    """List all known workers with live/dead status based on heartbeat TTL."""
    r = task_queue.redis
    out = []
    for worker_id in r.smembers(_WORKER_REGISTRY):
        raw = r.get(_worker_key(worker_id))
        if raw is None:
            out.append({"worker_id": worker_id, "status": "dead", "queues": [], "last_seen": None})
        else:
            data = json.loads(raw)
            out.append({"worker_id": worker_id, "status": "live", **data})
    return out
