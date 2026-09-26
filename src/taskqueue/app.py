"""Uvicorn entrypoint: `uvicorn taskqueue.app:app`. Configure via REDIS_URL env var."""

from __future__ import annotations

import os

from taskqueue.admin_api import create_app
from taskqueue.client import TaskQueue

task_queue = TaskQueue(redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
app = create_app(task_queue)
