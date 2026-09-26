from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum


class JobStatus(str, Enum):
    """Lifecycle states a Job moves through from enqueue to terminal state."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"


_OPTIONAL_STR_FIELDS = ("idempotency_key", "last_error", "worker_id")


@dataclass
class Job:
    queue: str
    payload: dict
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: int = 0
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    idempotency_key: str | None = None
    scheduled_at: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    last_error: str | None = None
    worker_id: str | None = None

    def to_redis(self) -> dict:
        """Flatten to a dict of strings/numbers suitable for HSET."""
        d = asdict(self)
        d["payload"] = json.dumps(d["payload"])
        d["status"] = self.status.value
        return {k: ("" if v is None else v) for k, v in d.items()}

    @classmethod
    def from_redis(cls, data: dict) -> Job:
        data = dict(data)
        data["payload"] = json.loads(data["payload"]) if data.get("payload") else {}
        data["priority"] = int(data.get("priority", 0))
        data["attempts"] = int(data.get("attempts", 0))
        data["max_attempts"] = int(data.get("max_attempts", 5))
        data["scheduled_at"] = float(data.get("scheduled_at", 0))
        data["created_at"] = float(data.get("created_at", 0))
        data["status"] = JobStatus(data.get("status", "pending"))
        for f in _OPTIONAL_STR_FIELDS:
            if data.get(f) == "":
                data[f] = None
        return cls(**data)
