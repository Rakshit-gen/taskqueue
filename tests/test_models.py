from __future__ import annotations

from taskqueue.models import Job, JobStatus


def test_to_redis_serializes_payload_and_status():
    job = Job(queue="emails", payload={"to": "a@example.com"}, priority=3)
    flat = job.to_redis()
    assert flat["payload"] == '{"to": "a@example.com"}'
    assert flat["status"] == "pending"
    assert flat["priority"] == 3
