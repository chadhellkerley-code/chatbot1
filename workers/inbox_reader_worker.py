from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from src.inbox.reader_supervisor import InboxReaderSupervisor, ReaderTaskSnapshot


@dataclass(order=True)
class InboxReaderTask:
    priority: int
    sequence: int
    task_type: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
    dedupe_key: str = field(compare=False, default="")


class InboxReaderWorker(threading.Thread):
    def __init__(
        self,
        *,
        name: str,
        task_queue: "queue.PriorityQueue[InboxReaderTask]",
        handler: Callable[[InboxReaderTask], None],
        stop_event: threading.Event,
        lock: threading.RLock,
        pending_keys: set[str],
        supervisor: InboxReaderSupervisor,
        task_timeout_resolver: Callable[[InboxReaderTask], float],
    ) -> None:
        super().__init__(name=name, daemon=True)
        self._task_queue = task_queue
        self._handler = handler
        self._stop_event = stop_event
        self._lock = lock
        self._pending_keys = pending_keys
        self._supervisor = supervisor
        self._task_timeout_resolver = task_timeout_resolver

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self._task_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._supervisor.begin_task(
                    worker_name=self.name,
                    task_type=task.task_type,
                    priority=task.priority,
                    dedupe_key=task.dedupe_key,
                    payload=task.payload,
                    timeout_seconds=self._task_timeout_resolver(task),
                )
                self._handler(task)
            finally:
                self._supervisor.complete_task(self.name)
                with self._lock:
                    if task.dedupe_key:
                        self._pending_keys.discard(task.dedupe_key)
                self._task_queue.task_done()


class InboxReaderWorkerPool:
    def __init__(
        self,
        handler: Callable[[InboxReaderTask], None],
        *,
        max_workers: int = 4,
        name: str = "inbox-reader",
        task_timeout_resolver: Optional[Callable[[InboxReaderTask], float]] = None,
        on_task_timeout: Optional[Callable[[ReaderTaskSnapshot], None]] = None,
        monitor_interval_seconds: float = 0.5,
    ) -> None:
        self._handler = handler
        self._max_workers = max(1, int(max_workers or 1))
        self._name = str(name or "inbox-reader")
        self._queue: queue.PriorityQueue[InboxReaderTask] = queue.PriorityQueue()
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._threads: dict[str, InboxReaderWorker] = {}
        self._all_threads: list[InboxReaderWorker] = []
        self._pending_keys: set[str] = set()
        self._sequence = 0
        self._worker_sequence = 0
        self._external_timeout_handler = on_task_timeout
        self._task_timeout_resolver = task_timeout_resolver or (lambda _task: 60.0)
        self._supervisor = InboxReaderSupervisor(
            monitor_interval_seconds=monitor_interval_seconds,
            on_task_timeout=self._handle_task_timeout,
        )

    def start(self) -> None:
        with self._lock:
            if self._threads:
                return
            self._stop_event.clear()
            self._supervisor.start()
            for _ in range(self._max_workers):
                self._spawn_worker_locked()

    def stop(self) -> None:
        with self._lock:
            if not self._threads and not self._all_threads:
                return
            self._stop_event.set()
            self._supervisor.stop()
            threads = list(self._all_threads)
            self._threads.clear()
            self._all_threads.clear()
            self._pending_keys.clear()
        for worker in threads:
            worker.join(timeout=2.0)
        with self._lock:
            self._queue = queue.PriorityQueue()

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
            task = InboxReaderTask(
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

    def active_task_count(self) -> int:
        return self._supervisor.active_task_count()

    def timeout_count(self) -> int:
        return self._supervisor.timeout_count()

    def recent_events(self, *, limit: int = 20) -> list[dict[str, Any]]:
        events = self._supervisor.recent_events(limit=limit)
        return [
            {
                "event_type": event.event_type,
                "worker_name": event.worker_name,
                "task_type": event.task_type,
                "dedupe_key": event.dedupe_key,
                "occurred_at": event.occurred_at,
                "detail": event.detail,
            }
            for event in events
        ]

    def qsize(self) -> int:
        try:
            return int(self._queue.qsize())
        except Exception:
            return 0

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_keys)

    def worker_count(self) -> int:
        with self._lock:
            return len(self._threads)

    def _spawn_worker_locked(self) -> InboxReaderWorker:
        self._worker_sequence += 1
        worker_name = f"{self._name}-{self._worker_sequence}"
        worker = InboxReaderWorker(
            name=worker_name,
            task_queue=self._queue,
            handler=self._handler,
            stop_event=self._stop_event,
            lock=self._lock,
            pending_keys=self._pending_keys,
            supervisor=self._supervisor,
            task_timeout_resolver=self._task_timeout_resolver,
        )
        self._threads[worker_name] = worker
        self._all_threads.append(worker)
        worker.start()
        return worker

    def _handle_task_timeout(self, snapshot: ReaderTaskSnapshot) -> None:
        callback = self._external_timeout_handler
        should_spawn = False
        with self._lock:
            retired = self._threads.pop(snapshot.worker_name, None)
            if retired is None:
                return
            if snapshot.dedupe_key:
                self._pending_keys.discard(snapshot.dedupe_key)
            should_spawn = not self._stop_event.is_set() and len(self._threads) < self._max_workers
        if callback is not None:
            callback(snapshot)
        if should_spawn:
            with self._lock:
                if not self._stop_event.is_set() and len(self._threads) < self._max_workers:
                    self._spawn_worker_locked()
