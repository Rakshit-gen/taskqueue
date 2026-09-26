from __future__ import annotations


def test_enqueue_returns_job_with_given_fields(task_queue):
    job = task_queue.enqueue("emails", {"to": "a@example.com"}, priority=5, max_attempts=2)
    assert job.queue == "emails"
    assert job.payload == {"to": "a@example.com"}
    assert job.priority == 5
    assert job.max_attempts == 2
