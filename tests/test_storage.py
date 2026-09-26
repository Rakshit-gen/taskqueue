from __future__ import annotations

import time

from taskqueue.models import Job, JobStatus


def test_priority_orders_claim_before_lower_priority(store):
    low = Job(queue="q", payload={}, priority=1)
    high = Job(queue="q", payload={}, priority=5)
    store.enqueue(low)
    store.enqueue(high)

    claimed = store.claim("q", "w1")
    assert claimed.id == high.id


def test_delayed_job_not_claimable_until_due(store):
    job = Job(queue="q", payload={}, scheduled_at=time.time() + 60)
    store.enqueue(job)
    assert store.claim("q", "w1") is None


def test_idempotency_key_dedupes_within_ttl(store):
    first = Job(queue="q", payload={}, idempotency_key="order-1")
    second = Job(queue="q", payload={}, idempotency_key="order-1")

    assert store.enqueue(first) is not None
    assert store.enqueue(second) is None
    assert store.queue_depths("q")["pending"] == 1


def test_fail_reschedules_until_max_attempts_then_dlq(store):
    job = Job(queue="q", payload={}, max_attempts=2)
    store.enqueue(job)

    claimed = store.claim("q", "w1")
    assert claimed.attempts == 1
    failed = store.fail(claimed, "boom", backoff_seconds=0)
    assert failed.status == JobStatus.FAILED
    assert store.queue_depths("q") == {"pending": 1, "in_progress": 0, "dlq": 0}

    claimed2 = store.claim("q", "w1")
    assert claimed2.attempts == 2
    dead = store.fail(claimed2, "boom again", backoff_seconds=0)
    assert dead.status == JobStatus.DEAD
    assert store.queue_depths("q") == {"pending": 0, "in_progress": 0, "dlq": 1}
    assert [j.id for j in store.list_dlq("q")] == [job.id]


def test_ack_records_completion_and_clears_in_progress(store):
    job = Job(queue="q", payload={"n": 1})
    store.enqueue(job)
    claimed = store.claim("q", "w1")
    store.ack(claimed)

    assert store.queue_depths("q")["in_progress"] == 0
    assert [j.id for j in store.list_completed("q")] == [job.id]
    assert store.get_job(job.id).status == JobStatus.COMPLETED


def test_retry_dlq_job_resets_attempts_and_requeues(store):
    job = Job(queue="q", payload={}, max_attempts=1)
    store.enqueue(job)
    claimed = store.claim("q", "w1")
    store.fail(claimed, "boom", backoff_seconds=0)
    assert store.get_job(job.id).status == JobStatus.DEAD

    retried = store.retry_dlq_job(job.id)
    assert retried.status == JobStatus.PENDING
    assert retried.attempts == 0
    assert store.queue_depths("q") == {"pending": 1, "in_progress": 0, "dlq": 0}


def test_cancel_removes_pending_job(store):
    job = Job(queue="q", payload={})
    store.enqueue(job)
    cancelled = store.cancel(job.id)
    assert cancelled.status == JobStatus.CANCELLED
    assert store.queue_depths("q")["pending"] == 0
    assert store.claim("q", "w1") is None


def test_cancel_fails_for_non_pending_job(store):
    job = Job(queue="q", payload={})
    store.enqueue(job)
    store.claim("q", "w1")
    assert store.cancel(job.id) is None


def test_reap_expired_lease_requeues_as_failure(store):
    job = Job(queue="q", payload={}, max_attempts=3)
    store.enqueue(job)
    store.claim("q", "w1", lease_seconds=0)
    time.sleep(0.05)

    reaped = store.reap_expired("q")
    assert reaped == [job.id]
    requeued = store.get_job(job.id)
    assert requeued.status == JobStatus.FAILED
    assert requeued.last_error == "worker lease expired"
    assert store.queue_depths("q")["pending"] == 1


def test_queue_names_and_depths(store):
    store.enqueue(Job(queue="alpha", payload={}))
    store.enqueue(Job(queue="beta", payload={}))
    assert store.queue_names() == ["alpha", "beta"]


def test_get_job_returns_none_for_unknown_id(store):
    assert store.get_job("does-not-exist") is None


def test_list_pending_respects_limit(store):
    for i in range(5):
        store.enqueue(Job(queue="q", payload={"i": i}))
    assert len(store.list_pending("q", limit=2)) == 2


def test_list_in_progress_reflects_claimed_jobs(store):
    store.enqueue(Job(queue="q", payload={}))
    claimed = store.claim("q", "w1")
    in_progress = store.list_in_progress("q")
    assert [j.id for j in in_progress] == [claimed.id]


def test_list_dlq_returns_dead_lettered_jobs(store):
    job = Job(queue="q", payload={}, max_attempts=1)
    store.enqueue(job)
    claimed = store.claim("q", "w1")  # attempts is now 1, equal to max_attempts
    dead = store.fail(claimed, "boom", backoff_seconds=0)
    assert dead.status == JobStatus.DEAD
    assert [j.id for j in store.list_dlq("q")] == [dead.id]
