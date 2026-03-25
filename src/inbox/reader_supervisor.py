from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Optional


@dataclass
class ReaderTaskSnapshot:
    worker_name: str
    task_type: str
    priority: int
    dedupe_key: str
    payload: dict[str, Any]
    started_at: float
    heartbeat_at: float
    deadline_at: float
    timed_out_at: float | None = None

    @property
    def account_id(self) -> str:
        return str(self.payload.get("account_id") or "").strip().lstrip("@")

    @property
    def thread_key(self) -> str:
        return str(self.payload.get("thread_key") or "").strip()


@dataclass(frozen=True)
class ReaderSupervisorEvent:
    event_type: str
    worker_name: str
    task_type: str
    dedupe_key: str
    occurred_at: float
    detail: str = ""


class InboxReaderSupervisor:
    def __init__(
        self,
        *,
        monitor_interval_seconds: float = 0.5,
        on_task_timeout: Optional[Callable[[ReaderTaskSnapshot], None]] = None,
        event_history_limit: int = 200,
    ) -> None:
        self._monitor_interval_seconds = max(0.05, float(monitor_interval_seconds or 0.5))
        self._on_task_timeout = on_task_timeout
        self._event_history_limit = max(10, int(event_history_limit or 200))
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active_tasks: dict[str, ReaderTaskSnapshot] = {}
        self._events: Deque[ReaderSupervisorEvent] = deque(maxlen=self._event_history_limit)
        self._timeout_count = 0

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._active_tasks.clear()
            self._events.clear()
            self._timeout_count = 0
            self._thread = threading.Thread(
                target=self._run_loop,
                name="inbox-reader-supervisor",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        worker = None
        with self._lock:
            worker = self._thread
            self._thread = None
            self._active_tasks.clear()
        if worker is not None:
            worker.join(timeout=2.0)

    def begin_task(
        self,
        *,
        worker_name: str,
        task_type: str,
        priority: int,
        dedupe_key: str,
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> ReaderTaskSnapshot:
        now = time.time()
        snapshot = ReaderTaskSnapshot(
            worker_name=str(worker_name or "").strip(),
            task_type=str(task_type or "").strip(),
            priority=max(0, int(priority or 0)),
            dedupe_key=str(dedupe_key or "").strip(),
            payload=dict(payload or {}),
            started_at=now,
            heartbeat_at=now,
            deadline_at=now + max(0.05, float(timeout_seconds or 0.05)),
        )
        with self._lock:
            self._active_tasks[snapshot.worker_name] = snapshot
            self._events.append(
                ReaderSupervisorEvent(
                    event_type="task_started",
                    worker_name=snapshot.worker_name,
                    task_type=snapshot.task_type,
                    dedupe_key=snapshot.dedupe_key,
                    occurred_at=now,
                )
            )
        return snapshot

    def heartbeat(self, worker_name: str) -> None:
        now = time.time()
        with self._lock:
            snapshot = self._active_tasks.get(str(worker_name or "").strip())
            if snapshot is None or snapshot.timed_out_at is not None:
                return
            snapshot.heartbeat_at = now

    def complete_task(self, worker_name: str) -> None:
        now = time.time()
        with self._lock:
            snapshot = self._active_tasks.pop(str(worker_name or "").strip(), None)
            if snapshot is None:
                return
            self._events.append(
                ReaderSupervisorEvent(
                    event_type="task_finished",
                    worker_name=snapshot.worker_name,
                    task_type=snapshot.task_type,
                    dedupe_key=snapshot.dedupe_key,
                    occurred_at=now,
                    detail="after_timeout" if snapshot.timed_out_at is not None else "",
                )
            )

    def active_task_count(self) -> int:
        with self._lock:
            return len(self._active_tasks)

    def timeout_count(self) -> int:
        with self._lock:
            return self._timeout_count

    def active_snapshots(self) -> list[ReaderTaskSnapshot]:
        with self._lock:
            return [self._copy_snapshot(item) for item in self._active_tasks.values()]

    def recent_events(self, *, limit: int = 20) -> list[ReaderSupervisorEvent]:
        max_items = max(1, int(limit or 20))
        with self._lock:
            items = list(self._events)
        return items[-max_items:]

    def _run_loop(self) -> None:
        while not self._stop_event.wait(self._monitor_interval_seconds):
            expired = self._collect_expired_tasks()
            if not expired:
                continue
            callback = self._on_task_timeout
            if callback is None:
                continue
            for snapshot in expired:
                try:
                    callback(self._copy_snapshot(snapshot))
                except Exception:
                    continue

    def _collect_expired_tasks(self) -> list[ReaderTaskSnapshot]:
        now = time.time()
        expired: list[ReaderTaskSnapshot] = []
        with self._lock:
            for snapshot in self._active_tasks.values():
                if snapshot.timed_out_at is not None:
                    continue
                if snapshot.deadline_at > now:
                    continue
                snapshot.timed_out_at = now
                self._timeout_count += 1
                self._events.append(
                    ReaderSupervisorEvent(
                        event_type="task_timeout",
                        worker_name=snapshot.worker_name,
                        task_type=snapshot.task_type,
                        dedupe_key=snapshot.dedupe_key,
                        occurred_at=now,
                    )
                )
                expired.append(self._copy_snapshot(snapshot))
        return expired

    @staticmethod
    def _copy_snapshot(snapshot: ReaderTaskSnapshot) -> ReaderTaskSnapshot:
        return ReaderTaskSnapshot(
            worker_name=snapshot.worker_name,
            task_type=snapshot.task_type,
            priority=snapshot.priority,
            dedupe_key=snapshot.dedupe_key,
            payload=dict(snapshot.payload),
            started_at=snapshot.started_at,
            heartbeat_at=snapshot.heartbeat_at,
            deadline_at=snapshot.deadline_at,
            timed_out_at=snapshot.timed_out_at,
        )
