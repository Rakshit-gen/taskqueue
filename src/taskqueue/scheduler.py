from __future__ import annotations

import json
import time

import redis
from croniter import croniter

from taskqueue.client import TaskQueue

# Recurring jobs use cron expressions (via croniter) rather than a hand-rolled
# interval scheduler: cron covers "every N seconds" (*/N * * * * with a
# seconds-capable expression isn't standard cron, so plain interval support
# is kept too) as well as real calendar schedules ("2am daily", "every
# Monday"), which developers expect from a job queue's recurring-job feature.

_CRON_SET = "tq:crons"


def _cron_key(name: str) -> str:
    return f"tq:cron:{name}"


class Scheduler:
    """Registers cron/interval recurring jobs and enqueues them when due."""

    def __init__(self, task_queue: TaskQueue):
        self.tq = task_queue
        self.r: redis.Redis = task_queue.redis

    def register(
        self,
        name: str,
        queue: str,
        payload: dict,
        cron: str | None = None,
        interval_seconds: float | None = None,
        priority: int = 0,
        max_attempts: int = 5,
    ) -> None:
        if (cron is None) == (interval_seconds is None):
            raise ValueError("provide exactly one of cron or interval_seconds")
        if cron is not None:
            croniter(cron)  # validates the expression, raises on bad syntax
            next_run = croniter(cron, time.time()).get_next(float)
        else:
            next_run = time.time() + interval_seconds
        self.r.sadd(_CRON_SET, name)
        self.r.hset(
            _cron_key(name),
            mapping={
                "queue": queue,
                "payload": json.dumps(payload),
                "cron": cron or "",
                "interval_seconds": interval_seconds if interval_seconds is not None else "",
                "priority": priority,
                "max_attempts": max_attempts,
                "next_run": next_run,
            },
        )

    def unregister(self, name: str) -> None:
        self.r.srem(_CRON_SET, name)
        self.r.delete(_cron_key(name))

    def list_recurring(self) -> list[dict]:
        return [self._read(name) for name in self.r.smembers(_CRON_SET)]

    def _read(self, name: str) -> dict:
        data = self.r.hgetall(_cron_key(name))
        return {
            "name": name,
            "queue": data["queue"],
            "payload": json.loads(data["payload"]),
            "cron": data["cron"] or None,
            "interval_seconds": float(data["interval_seconds"]) if data["interval_seconds"] else None,
            "priority": int(data["priority"]),
            "max_attempts": int(data["max_attempts"]),
            "next_run": float(data["next_run"]),
        }

    def tick(self) -> list[str]:
        """Enqueue any recurring job whose next_run is due. Returns names enqueued."""
        now = time.time()
        fired = []
        for name in self.r.smembers(_CRON_SET):
            spec = self._read(name)
            if spec["next_run"] > now:
                continue
            self.tq.enqueue(
                spec["queue"],
                spec["payload"],
                priority=spec["priority"],
                max_attempts=spec["max_attempts"],
            )
            if spec["cron"]:
                next_run = croniter(spec["cron"], now).get_next(float)
            else:
                next_run = now + spec["interval_seconds"]
            self.r.hset(_cron_key(name), "next_run", next_run)
            fired.append(name)
        return fired

    def run_forever(self, poll_interval: float = 1.0) -> None:
        while True:
            self.tick()
            time.sleep(poll_interval)
