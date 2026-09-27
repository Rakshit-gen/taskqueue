from __future__ import annotations

import time

import redis

from taskqueue.models import Job, JobStatus

# Score = scheduled_at, with priority as a sub-second tiebreak so higher
# priority jobs due at (roughly) the same time sort first. A single ZSET
# this way carries both delayed scheduling and priority ordering without a
# second data structure.
# ponytail: priority tiebreak only holds within the same scheduled second
# for priority deltas under ~1e6; fine for a small int priority field, would
# need a proper composite key (e.g. two ZSETs) if priorities ever needed to
# reorder jobs scheduled seconds apart.
_PRIORITY_SCALE = 1e-6

_CLAIM_SCRIPT = """
local job_id = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)[1]
if not job_id then
    return nil
end
redis.call('ZREM', KEYS[1], job_id)
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]) + tonumber(ARGV[2]), job_id)
return job_id
"""

_REAP_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #expired > 0 then
    redis.call('ZREM', KEYS[1], unpack(expired))
end
return expired
"""

DEFAULT_LEASE_SECONDS = 60
COMPLETED_HISTORY_LIMIT = 100


class Store:
    """Redis-backed persistence for jobs: per-queue pending/inprogress/dlq
    sorted sets, a capped completed-history list, and a hash per job.
    """

    def __init__(self, client: redis.Redis):
        self.r = client
        self._claim = client.register_script(_CLAIM_SCRIPT)
        self._reap = client.register_script(_REAP_SCRIPT)

    # -- key helpers ---------------------------------------------------
    @staticmethod
    def _pending_key(queue: str) -> str:
        return f"tq:{queue}:pending"

    @staticmethod
    def _inprogress_key(queue: str) -> str:
        return f"tq:{queue}:inprogress"

    @staticmethod
    def _dlq_key(queue: str) -> str:
        return f"tq:{queue}:dlq"

    @staticmethod
    def _completed_key(queue: str) -> str:
        return f"tq:{queue}:completed"

    @staticmethod
    def _job_key(job_id: str) -> str:
        return f"tq:job:{job_id}"

    @staticmethod
    def _idem_key(queue: str, idempotency_key: str) -> str:
        return f"tq:idem:{queue}:{idempotency_key}"

    def _score(self, job: Job) -> float:
        return job.scheduled_at - job.priority * _PRIORITY_SCALE

    # -- enqueue ---------------------------------------------------------
    def enqueue(self, job: Job, idempotency_ttl: int = 86400) -> Job | None:
        """Store a job and add it to its queue's pending set.

        Returns None (without enqueuing) if idempotency_key was already
        used within idempotency_ttl seconds.
        """
        if job.idempotency_key:
            idem_key = self._idem_key(job.queue, job.idempotency_key)
            if not self.r.set(idem_key, job.id, nx=True, ex=idempotency_ttl):
                return None
        pipe = self.r.pipeline()
        pipe.sadd("tq:queues", job.queue)
        pipe.hset(self._job_key(job.id), mapping=job.to_redis())
        pipe.zadd(self._pending_key(job.queue), {job.id: self._score(job)})
        pipe.execute()
        return job

    # -- claim / ack / fail ----------------------------------------------
    def claim(self, queue: str, worker_id: str, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> Job | None:
        now = time.time()
        job_id = self._claim(
            keys=[self._pending_key(queue), self._inprogress_key(queue)],
            args=[now, lease_seconds],
        )
        if job_id is None:
            return None
        job_id = job_id.decode() if isinstance(job_id, bytes) else job_id
        job_key = self._job_key(job_id)
        self.r.hincrby(job_key, "attempts", 1)
        self.r.hset(job_key, mapping={"status": JobStatus.IN_PROGRESS.value, "worker_id": worker_id})
        return self.get_job(job_id)

    def ack(self, job: Job) -> None:
        self.r.zrem(self._inprogress_key(job.queue), job.id)
        self.r.hset(self._job_key(job.id), "status", JobStatus.COMPLETED.value)
        pipe = self.r.pipeline()
        pipe.lpush(self._completed_key(job.queue), job.id)
        pipe.ltrim(self._completed_key(job.queue), 0, COMPLETED_HISTORY_LIMIT - 1)
        pipe.execute()

    def fail(self, job: Job, error: str, backoff_seconds: float) -> Job:
        """Record a failure. Reschedules if attempts remain, else moves to DLQ."""
        self.r.zrem(self._inprogress_key(job.queue), job.id)
        job_key = self._job_key(job.id)
        if job.attempts >= job.max_attempts:
            self.r.hset(job_key, mapping={"status": JobStatus.DEAD.value, "last_error": error})
            self.r.zadd(self._dlq_key(job.queue), {job.id: time.time()})
            job.status = JobStatus.DEAD
        else:
            next_at = time.time() + backoff_seconds
            self.r.hset(
                job_key,
                mapping={"status": JobStatus.FAILED.value, "last_error": error, "scheduled_at": next_at},
            )
            job.scheduled_at = next_at
            self.r.zadd(self._pending_key(job.queue), {job.id: self._score(job)})
            job.status = JobStatus.FAILED
        job.last_error = error
        return job

    # -- reaping stuck jobs ------------------------------------------------
    def reap_expired(self, queue: str) -> list[str]:
        """Find in-progress jobs whose lease expired and requeue them as failures.

        Returns the list of job ids that were reaped.
        """
        raw = self._reap(keys=[self._inprogress_key(queue)], args=[time.time()])
        job_ids = [j.decode() if isinstance(j, bytes) else j for j in raw]
        for job_id in job_ids:
            job = self.get_job(job_id)
            if job is None:
                continue
            self.fail(job, "worker lease expired", backoff_seconds=0)
        return job_ids

    # -- dlq ---------------------------------------------------------------
    def retry_dlq_job(self, job_id: str) -> Job | None:
        job = self.get_job(job_id)
        if job is None or job.status != JobStatus.DEAD:
            return None
        removed = self.r.zrem(self._dlq_key(job.queue), job.id)
        if not removed:
            # Another concurrent retry_dlq_job call already claimed this
            # job between our status check and this zrem (same TOCTOU
            # guard cancel() uses below).
            return None
        job.status = JobStatus.PENDING
        job.attempts = 0
        job.scheduled_at = time.time()
        job.last_error = None
        self.r.hset(self._job_key(job.id), mapping=job.to_redis())
        self.r.zadd(self._pending_key(job.queue), {job.id: self._score(job)})
        return job

    # -- cancel ---------------------------------------------------------------
    def cancel(self, job_id: str) -> Job | None:
        job = self.get_job(job_id)
        if job is None or job.status != JobStatus.PENDING:
            return None
        removed = self.r.zrem(self._pending_key(job.queue), job.id)
        if not removed:
            return None
        job.status = JobStatus.CANCELLED
        self.r.hset(self._job_key(job.id), "status", JobStatus.CANCELLED.value)
        return job

    # -- reads ---------------------------------------------------------------
    def get_job(self, job_id: str) -> Job | None:
        """Fetch a job by id, or None if no hash exists for that id."""
        data = self.r.hgetall(self._job_key(job_id))
        if not data:
            return None
        data.setdefault("id", job_id)
        return Job.from_redis(data)

    def queue_names(self) -> list[str]:
        """All queue names that have ever had a job enqueued."""
        return sorted(self.r.smembers("tq:queues"))

    def queue_depths(self, queue: str) -> dict:
        """Counts of pending/in_progress/dlq jobs for one queue."""
        return {
            "pending": self.r.zcard(self._pending_key(queue)),
            "in_progress": self.r.zcard(self._inprogress_key(queue)),
            "dlq": self.r.zcard(self._dlq_key(queue)),
        }

    def list_pending(self, queue: str, limit: int = 100) -> list[Job]:
        """Pending jobs due soonest first, capped at limit."""
        if limit <= 0:
            return []
        ids = self.r.zrange(self._pending_key(queue), 0, limit - 1)
        return [j for j in (self.get_job(i) for i in ids) if j is not None]

    def list_dlq(self, queue: str, limit: int = 100) -> list[Job]:
        """Dead-lettered jobs, most recently dead first, capped at limit."""
        if limit <= 0:
            return []
        ids = self.r.zrevrange(self._dlq_key(queue), 0, limit - 1)
        return [j for j in (self.get_job(i) for i in ids) if j is not None]

    def list_in_progress(self, queue: str, limit: int = 100) -> list[Job]:
        """Currently-claimed jobs ordered by lease expiry, capped at limit."""
        if limit <= 0:
            return []
        ids = self.r.zrange(self._inprogress_key(queue), 0, limit - 1)
        return [j for j in (self.get_job(i) for i in ids) if j is not None]

    def list_completed(self, queue: str, limit: int = 100) -> list[Job]:
        """Most recently completed jobs, capped at limit and at
        COMPLETED_HISTORY_LIMIT overall (see ack()).
        """
        if limit <= 0:
            return []
        ids = self.r.lrange(self._completed_key(queue), 0, limit - 1)
        return [j for j in (self.get_job(i) for i in ids) if j is not None]
