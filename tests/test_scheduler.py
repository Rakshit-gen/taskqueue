from __future__ import annotations

import time

import pytest

from taskqueue.scheduler import Scheduler


def test_interval_job_fires_when_due(task_queue):
    sched = Scheduler(task_queue)
    sched.register("ping", queue="q", payload={"kind": "ping"}, interval_seconds=0)
    time.sleep(0.01)

    fired = sched.tick()
    assert fired == ["ping"]
    assert task_queue.store.queue_depths("q")["pending"] == 1


def test_interval_job_not_due_yet_does_not_fire(task_queue):
    sched = Scheduler(task_queue)
    sched.register("ping", queue="q", payload={}, interval_seconds=3600)

    fired = sched.tick()
    assert fired == []
    assert task_queue.store.queue_depths("q")["pending"] == 0


def test_cron_job_computes_next_run_and_fires_when_due(task_queue):
    sched = Scheduler(task_queue)
    # "* * * * *" = every minute, so a job registered with a base time already
    # past the top of the minute won't be due yet, but registering directly
    # at a due next_run in the past should still fire on tick.
    sched.register("hourly", queue="q", payload={}, cron="* * * * *")
    # Force next_run into the past to simulate time passing without sleeping a full minute.
    sched.r.hset("tq:cron:hourly", "next_run", time.time() - 1)

    fired = sched.tick()
    assert fired == ["hourly"]
    assert task_queue.store.queue_depths("q")["pending"] == 1

    spec = sched._read("hourly")
    assert spec["next_run"] > time.time()


def test_register_rejects_both_or_neither_schedule_kind(task_queue):
    sched = Scheduler(task_queue)
    with pytest.raises(ValueError):
        sched.register("bad", queue="q", payload={})
    with pytest.raises(ValueError):
        sched.register("bad", queue="q", payload={}, cron="* * * * *", interval_seconds=5)


def test_unregister_removes_recurring_job(task_queue):
    sched = Scheduler(task_queue)
    sched.register("ping", queue="q", payload={}, interval_seconds=10)
    assert len(sched.list_recurring()) == 1

    sched.unregister("ping")
    assert sched.list_recurring() == []


def test_list_recurring_round_trips_registration_fields(task_queue):
    sched = Scheduler(task_queue)
    sched.register("digest", queue="emails", payload={"kind": "digest"}, interval_seconds=60, priority=3, max_attempts=2)

    [spec] = sched.list_recurring()
    assert spec["name"] == "digest"
    assert spec["queue"] == "emails"
    assert spec["payload"] == {"kind": "digest"}
    assert spec["interval_seconds"] == 60
    assert spec["cron"] is None
    assert spec["priority"] == 3
    assert spec["max_attempts"] == 2
