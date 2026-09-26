from __future__ import annotations

import os

import pytest
import redis

from taskqueue.client import TaskQueue
from taskqueue.storage import Store

REDIS_URL = os.environ.get("TASKQUEUE_TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def redis_client():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError as exc:
        pytest.skip(f"no redis reachable at {REDIS_URL}: {exc}")
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def store(redis_client):
    return Store(redis_client)


@pytest.fixture
def task_queue(redis_client):
    return TaskQueue(redis_client=redis_client)
