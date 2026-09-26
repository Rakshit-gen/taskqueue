from __future__ import annotations

from taskqueue.models import Job, JobStatus


def test_to_redis_serializes_payload_and_status():
    job = Job(queue="emails", payload={"to": "a@example.com"}, priority=3)
    flat = job.to_redis()
    assert flat["payload"] == '{"to": "a@example.com"}'
    assert flat["status"] == "pending"
    assert flat["priority"] == 3


def test_from_redis_round_trips_to_redis_output():
    job = Job(queue="emails", payload={"to": "a@example.com"}, priority=3, max_attempts=2)
    restored = Job.from_redis(job.to_redis())
    assert restored.queue == job.queue
    assert restored.payload == job.payload
    assert restored.priority == job.priority
    assert restored.status == JobStatus.PENDING
    assert restored.max_attempts == 2
