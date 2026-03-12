from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(order=True)
class InboxTask:
    priority: int
    sequence: int
    task_type: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
    dedupe_key: str = field(compare=False, default="")


class WorkerPool:
    def __init__(
        self,
        handler: Callable[[InboxTask], None],
        *,
        max_workers: int = 4,
        name: str = "inbox-worker",
    ) -> None:
        self._handler = handler
        self._max_workers = max(1, int(max_workers or 1))
        self._name = str(name or "inbox-worker")
        self._queue: queue.PriorityQueue[InboxTask | None] = queue.PriorityQueue()
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._threads: list[threading.Thread] = []
        self._pending_keys: set[str] = set()
        self._sequence = 0

    def start(self) -> None:
        with self._lock:
            if self._threads:
                return
            self._stop_event.clear()
            for index in range(self._max_workers):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"{self._name}-{index + 1}",
                    daemon=True,
                )
                self._threads.append(worker)
                worker.start()

    def stop(self) -> None:
        with self._lock:
            if not self._threads:
                return
            self._stop_event.set()
            for _ in self._threads:
                self._queue.put(None)
            threads = list(self._threads)
            self._threads.clear()
            self._pending_keys.clear()
        for worker in threads:
            worker.join(timeout=2.0)

    def submit(
        self,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 50,
        dedupe_key: str = "",
    ) -> bool:
        clean_type = str(task_type or "").strip()
        if not clean_type:
            return False
        payload_dict = dict(payload or {})
        with self._lock:
            if dedupe_key and dedupe_key in self._pending_keys:
                return False
            self._sequence += 1
            task = InboxTask(
                priority=max(0, int(priority or 0)),
                sequence=self._sequence,
                task_type=clean_type,
                payload=payload_dict,
                dedupe_key=str(dedupe_key or "").strip(),
            )
            if task.dedupe_key:
                self._pending_keys.add(task.dedupe_key)
            self._queue.put(task)
            return True

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            task = self._queue.get()
            if task is None:
                self._queue.task_done()
                return
            try:
                self._handler(task)
            finally:
                with self._lock:
                    if task.dedupe_key:
                        self._pending_keys.discard(task.dedupe_key)
                self._queue.task_done()

