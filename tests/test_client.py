from __future__ import annotations

import pytest


def test_enqueue_returns_job_with_given_fields(task_queue):
    job = task_queue.enqueue("emails", {"to": "a@example.com"}, priority=5, max_attempts=2)
    assert job.queue == "emails"
    assert job.payload == {"to": "a@example.com"}
    assert job.priority == 5
    assert job.max_attempts == 2


def test_enqueue_returns_none_on_duplicate_idempotency_key(task_queue):
    first = task_queue.enqueue("emails", {"n": 1}, idempotency_key="welcome-42")
    second = task_queue.enqueue("emails", {"n": 2}, idempotency_key="welcome-42")
    assert first is not None
    assert second is None


def test_fail_backoff_uses_attempts_minus_one_floored_at_zero(task_queue, monkeypatch):
    seen_attempts = []
    monkeypatch.setattr(
        "taskqueue.client.backoff_seconds",
        lambda attempt, **kw: seen_attempts.append(attempt) or 0.0,
    )
    task_queue.enqueue("emails", {})
    claimed = task_queue.claim("emails", "w1")
    task_queue.fail(claimed, "boom")
    assert seen_attempts == [0]


def test_enqueue_rejects_empty_queue_name(task_queue):
    with pytest.raises(ValueError):
        task_queue.enqueue("", {})
