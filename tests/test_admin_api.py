from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from taskqueue.admin_api import create_app


@pytest.fixture
def client(task_queue):
    return TestClient(create_app(task_queue))


def test_list_queues_reports_depths(client, task_queue):
    task_queue.enqueue("q", {})
    resp = client.get("/queues")
    assert resp.status_code == 200
    assert resp.json() == [{"name": "q", "pending": 1, "in_progress": 0, "dlq": 0}]


def test_list_jobs_by_status(client, task_queue):
    task_queue.enqueue("q", {"n": 1})
    resp = client.get("/queues/q/jobs", params={"status": "pending"})
    assert resp.status_code == 200
    jobs = resp.json()
    assert len(jobs) == 1
    assert jobs[0]["payload"] == {"n": 1}


def test_list_jobs_rejects_unknown_status(client):
    resp = client.get("/queues/q/jobs", params={"status": "bogus"})
    assert resp.status_code == 400


@pytest.mark.parametrize("limit", [0, -1, 1001])
def test_list_jobs_rejects_out_of_range_limit(client, limit):
    resp = client.get("/queues/q/jobs", params={"limit": limit})
    assert resp.status_code == 422


def test_get_job_detail(client, task_queue):
    job = task_queue.enqueue("q", {"n": 1})
    resp = client.get(f"/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job.id


def test_get_job_404_for_unknown_id(client):
    resp = client.get("/jobs/does-not-exist")
    assert resp.status_code == 404


def test_retry_dlq_job(client, task_queue):
    job = task_queue.enqueue("q", {}, max_attempts=1)
    claimed = task_queue.claim("q", "w1")
    task_queue.fail(claimed, "boom")

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_retry_non_dlq_job_404s(client, task_queue):
    job = task_queue.enqueue("q", {})
    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 404


def test_cancel_pending_job(client, task_queue):
    job = task_queue.enqueue("q", {})
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


def test_list_workers_empty_initially(client):
    resp = client.get("/workers")
    assert resp.status_code == 200
    assert resp.json() == []


def test_cancel_404s_for_already_completed_job(client, task_queue):
    job = task_queue.enqueue("q", {})
    claimed = task_queue.claim("q", "w1")
    task_queue.complete(claimed)

    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 404
