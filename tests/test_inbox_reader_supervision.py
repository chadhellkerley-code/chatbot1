from __future__ import annotations

import threading
import time

from workers.inbox_reader_worker import InboxReaderWorkerPool


def test_reader_worker_pool_replaces_timed_out_worker_and_continues_processing() -> None:
    handled: list[str] = []
    timeout_events: list[str] = []
    slow_started = threading.Event()
    quick_done = threading.Event()
    release_slow = threading.Event()

    def _handler(task) -> None:
        if task.task_type == "slow":
            slow_started.set()
            release_slow.wait(timeout=5.0)
            return
        handled.append(task.task_type)
        quick_done.set()

    pool = InboxReaderWorkerPool(
        _handler,
        max_workers=1,
        task_timeout_resolver=lambda task: 0.2 if task.task_type == "slow" else 1.0,
        on_task_timeout=lambda snapshot: timeout_events.append(snapshot.task_type),
        monitor_interval_seconds=0.05,
    )
    pool.start()
    try:
        assert pool.submit("slow", {}, dedupe_key="slow:1")
        assert slow_started.wait(timeout=1.0)

        deadline = time.time() + 2.0
        while time.time() < deadline and not timeout_events:
            time.sleep(0.05)

        assert timeout_events == ["slow"]
        assert pool.timeout_count() == 1
        assert pool.worker_count() == 1

        assert pool.submit("quick", {}, dedupe_key="quick:1")
        assert quick_done.wait(timeout=1.0)
        assert handled == ["quick"]

        event_types = [event["event_type"] for event in pool.recent_events(limit=10)]
        assert "task_timeout" in event_types
    finally:
        release_slow.set()
        pool.stop()
