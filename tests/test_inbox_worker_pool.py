from __future__ import annotations

import threading

from src.inbox.worker_pool import WorkerPool


def test_worker_pool_dedupes_pending_tasks() -> None:
    handled: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def _handler(task) -> None:
        handled.append(task.task_type)
        started.set()
        release.wait(timeout=2.0)

    pool = WorkerPool(_handler, max_workers=1)
    pool.start()
    try:
        assert pool.submit("sync_account", {"account_id": "acc1"}, dedupe_key="sync:acc1")
        assert not pool.submit("sync_account", {"account_id": "acc1"}, dedupe_key="sync:acc1")
        assert started.wait(timeout=2.0)
        release.set()
    finally:
        pool.stop()

    assert handled == ["sync_account"]

