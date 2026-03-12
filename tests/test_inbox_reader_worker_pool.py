from __future__ import annotations

import threading
import time

from workers.inbox_reader_worker import InboxReaderWorkerPool


def test_reader_worker_pool_stop_resets_queue_and_allows_restart() -> None:
    handled: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def _handler(task) -> None:
        handled.append(task.task_type)
        started.set()
        release.wait(timeout=1.0)

    pool = InboxReaderWorkerPool(_handler, max_workers=1)
    pool.start()
    try:
        assert pool.submit("sync_account", {"account_id": "acc-1"}, dedupe_key="sync:acc-1")
        assert pool.submit("read_thread", {"thread_key": "acc-1:thread-1"}, dedupe_key="read:acc-1:thread-1")
        assert started.wait(timeout=1.0)

        stopper = threading.Thread(target=pool.stop, daemon=True)
        stopper.start()
        time.sleep(0.05)
        release.set()
        stopper.join(timeout=2.0)

        assert stopper.is_alive() is False
        assert pool.worker_count() == 0
        assert pool.qsize() == 0
        assert pool.pending_count() == 0

        started.clear()
        release.clear()
        pool.start()
        assert pool.submit("sync_account", {"account_id": "acc-2"}, dedupe_key="sync:acc-2")
        assert started.wait(timeout=1.0)
    finally:
        release.set()
        pool.stop()

    assert handled == ["sync_account", "sync_account"]
