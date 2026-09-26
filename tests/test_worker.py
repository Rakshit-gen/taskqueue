from __future__ import annotations

from taskqueue.models import JobStatus
from taskqueue.worker import Worker, worker_status


def test_run_once_processes_and_completes_job(task_queue):
    task_queue.enqueue("q", {"n": 41})
    seen = []

    def handler(job):
        seen.append(job.payload["n"])

    w = Worker(task_queue, ["q"], handler, worker_id="w1")
    did_work = w.run_once()

    assert did_work is True
    assert seen == [41]
    assert task_queue.store.queue_depths("q") == {"pending": 0, "in_progress": 0, "dlq": 0}


def test_run_once_returns_false_when_empty(task_queue):
    w = Worker(task_queue, ["q"], lambda job: None, worker_id="w1")
    assert w.run_once() is False


def test_handler_exception_marks_job_failed(task_queue):
    task_queue.enqueue("q", {}, max_attempts=3)

    def handler(job):
        raise RuntimeError("boom")

    w = Worker(task_queue, ["q"], handler, worker_id="w1")
    w.run_once()

    depths = task_queue.store.queue_depths("q")
    assert depths["pending"] == 1
    job = task_queue.store.list_pending("q")[0]
    assert job.status == JobStatus.FAILED
    assert "boom" in job.last_error


def test_heartbeat_marks_worker_live_then_dead_after_expiry(task_queue):
    w = Worker(task_queue, ["q"], lambda job: None, worker_id="w1", heartbeat_ttl=1)
    w.heartbeat()

    statuses = {s["worker_id"]: s["status"] for s in worker_status(task_queue)}
    assert statuses["w1"] == "live"

    task_queue.redis.delete("tq:worker:w1")
    statuses = {s["worker_id"]: s["status"] for s in worker_status(task_queue)}
    assert statuses["w1"] == "dead"


def test_run_once_falls_through_to_second_queue_when_first_empty(task_queue):
    task_queue.enqueue("q2", {"n": 7})
    seen = []
    w = Worker(task_queue, ["q1", "q2"], lambda job: seen.append(job.payload["n"]), worker_id="w1")

    did_work = w.run_once()

    assert did_work is True
    assert seen == [7]
