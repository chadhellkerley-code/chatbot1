from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class MessageQueueTask:
    priority: int
    sequence: int
    task_type: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
    dedupe_key: str = field(compare=False, default="")


class MessageQueue:
    def __init__(self) -> None:
        self._queue: queue.PriorityQueue[MessageQueueTask] = queue.PriorityQueue()
        self._lock = threading.RLock()
        self._pending_keys: set[str] = set()
        self._sequence = 0

    def submit(
        self,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 50,
        dedupe_key: str = "",
    ) -> bool:
        clean_type = str(task_type or "").strip()
        clean_dedupe = str(dedupe_key or "").strip()
        if not clean_type:
            return False
        with self._lock:
            if clean_dedupe and clean_dedupe in self._pending_keys:
                return False
            self._sequence += 1
            task = MessageQueueTask(
                priority=max(0, int(priority or 0)),
                sequence=self._sequence,
                task_type=clean_type,
                payload=dict(payload or {}),
                dedupe_key=clean_dedupe,
            )
            if clean_dedupe:
                self._pending_keys.add(clean_dedupe)
            self._queue.put(task)
            return True

    def get(self, *, timeout: float = 0.5) -> MessageQueueTask | None:
        try:
            return self._queue.get(timeout=max(0.05, float(timeout or 0.5)))
        except queue.Empty:
            return None

    def task_done(self, task: MessageQueueTask | None) -> None:
        try:
            self._queue.task_done()
        finally:
            if task is None:
                return
            clean_dedupe = str(task.dedupe_key or "").strip()
            if not clean_dedupe:
                return
            with self._lock:
                self._pending_keys.discard(clean_dedupe)

    def clear(self) -> None:
        with self._lock:
            self._queue = queue.PriorityQueue()
            self._pending_keys.clear()

    def qsize(self) -> int:
        try:
            return int(self._queue.qsize())
        except Exception:
            return 0

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_keys)
