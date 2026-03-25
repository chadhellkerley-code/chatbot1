from __future__ import annotations

import time
from pathlib import Path

from application.services.base import ServiceContext
from application.services.inbox_runtime import InboxRuntime


class _FakeEngine:
    def __init__(self, diagnostics_payloads: list[dict[str, int]] | None = None) -> None:
        self.start_calls = 0
        self.shutdown_calls = 0
        self.foreground_calls: list[bool] = []
        self.sync_calls: list[bool] = []
        self._diagnostics_payloads = list(diagnostics_payloads or [])

    @property
    def events(self) -> "_FakeEngine":
        return self

    def start(self) -> None:
        self.start_calls += 1

    def shutdown(self) -> None:
        self.shutdown_calls += 1

    def set_foreground_active(self, active: bool) -> None:
        self.foreground_calls.append(bool(active))

    def enqueue_periodic_sync(self, *, force: bool = False) -> None:
        self.sync_calls.append(bool(force))

    def list_threads(self, filter_mode: str = "all") -> list[dict]:
        del filter_mode
        return []

    def get_thread(self, thread_key: str) -> dict | None:
        del thread_key
        return None

    def diagnostics(self) -> dict[str, int]:
        if self._diagnostics_payloads:
            return dict(self._diagnostics_payloads.pop(0))
        return {
            "queued_tasks": 0,
            "dedupe_pending": 0,
            "reader_active_tasks": 0,
            "worker_count": 0,
        }


class _SelectiveCacheEngine(_FakeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.rows = [
            {
                "thread_key": "acc-1:thread-1",
                "thread_id": "thread-1",
                "account_id": "acc-1",
                "display_name": "Uno",
                "last_message_text": "Hola",
                "last_message_timestamp": 100.0,
                "last_message_direction": "inbound",
                "unread_count": 1,
            },
            {
                "thread_key": "acc-2:thread-2",
                "thread_id": "thread-2",
                "account_id": "acc-2",
                "display_name": "Dos",
                "last_message_text": "Seguimos",
                "last_message_timestamp": 90.0,
                "last_message_direction": "outbound",
                "unread_count": 0,
            },
        ]
        self.threads = {
            "acc-1:thread-1": {
                "thread_key": "acc-1:thread-1",
                "thread_id": "thread-1",
                "account_id": "acc-1",
                "messages": [{"message_id": "m1", "text": "Hola", "timestamp": 100.0, "direction": "inbound"}],
            },
            "acc-2:thread-2": {
                "thread_key": "acc-2:thread-2",
                "thread_id": "thread-2",
                "account_id": "acc-2",
                "messages": [{"message_id": "m2", "text": "Seguimos", "timestamp": 90.0, "direction": "outbound"}],
            },
        }
        self.get_thread_calls: dict[str, int] = {}

    def list_threads(self, filter_mode: str = "all") -> list[dict]:
        del filter_mode
        return [dict(row) for row in self.rows]

    def get_thread(self, thread_key: str) -> dict | None:
        clean_key = str(thread_key or "").strip()
        self.get_thread_calls[clean_key] = self.get_thread_calls.get(clean_key, 0) + 1
        thread = self.threads.get(clean_key)
        return dict(thread) if isinstance(thread, dict) else None


class _OpenHydrationEngine(_FakeEngine):
    def __init__(self, *, messages: list[dict] | None) -> None:
        super().__init__()
        self.open_calls: list[str] = []
        self.hydrate_calls: list[str] = []
        self.thread = {
            "thread_key": "acc-1:thread-1",
            "thread_id": "thread-1",
            "account_id": "acc-1",
            "messages": [dict(item) for item in messages or []],
        }

    def list_threads(self, filter_mode: str = "all") -> list[dict]:
        del filter_mode
        return [
            {
                "thread_key": "acc-1:thread-1",
                "thread_id": "thread-1",
                "account_id": "acc-1",
                "display_name": "Uno",
                "last_message_text": "Hola",
                "last_message_timestamp": 100.0,
                "last_message_direction": "inbound",
                "unread_count": 1,
            }
        ]

    def get_thread(self, thread_key: str) -> dict | None:
        if str(thread_key or "").strip() != "acc-1:thread-1":
            return None
        return dict(self.thread)

    def open_thread(self, thread_key: str) -> bool:
        self.open_calls.append(str(thread_key or "").strip())
        return True

    def hydrate_thread(self, thread_key: str) -> bool:
        self.hydrate_calls.append(str(thread_key or "").strip())
        return True


def _wait_until(predicate, *, timeout: float = 0.8, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.05, float(timeout or 0.05))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(max(0.005, float(interval or 0.005)))
    return bool(predicate())


def test_inbox_runtime_stops_backend_after_idle_window(tmp_path: Path) -> None:
    runtime = InboxRuntime(
        ServiceContext(root_dir=tmp_path),
        _FakeEngine(),
        idle_backend_shutdown_seconds=0.05,
        idle_backend_retry_seconds=0.05,
    )
    try:
        runtime.ensure_backend_started()
        runtime.set_ui_active(True)
        runtime.set_ui_active(False)

        assert _wait_until(lambda: runtime.diagnostics()["backend_started"] is False)
        payload = runtime.diagnostics()
        assert payload["backend_started"] is False
        assert payload["ui_active"] is False
    finally:
        runtime.shutdown()


def test_inbox_runtime_retries_idle_shutdown_until_work_drains(tmp_path: Path) -> None:
    engine = _FakeEngine(
        diagnostics_payloads=[
            {"queued_tasks": 1, "dedupe_pending": 0, "reader_active_tasks": 0},
            {"queued_tasks": 0, "dedupe_pending": 0, "reader_active_tasks": 0},
        ]
    )
    runtime = InboxRuntime(
        ServiceContext(root_dir=tmp_path),
        engine,
        idle_backend_shutdown_seconds=0.05,
        idle_backend_retry_seconds=0.05,
    )
    try:
        runtime.ensure_backend_started()
        runtime.set_ui_active(False)

        assert _wait_until(lambda: engine.shutdown_calls == 1)
        payload = runtime.diagnostics()
        assert engine.shutdown_calls == 1
        assert payload["backend_started"] is False
    finally:
        runtime.shutdown()


def test_inbox_runtime_cancels_idle_shutdown_when_reactivated(tmp_path: Path) -> None:
    engine = _FakeEngine()
    runtime = InboxRuntime(
        ServiceContext(root_dir=tmp_path),
        engine,
        idle_backend_shutdown_seconds=0.08,
        idle_backend_retry_seconds=0.05,
    )
    try:
        runtime.ensure_backend_started()
        runtime.set_ui_active(False)
        time.sleep(0.02)
        runtime.set_ui_active(True)

        time.sleep(0.18)
        payload = runtime.diagnostics()
        assert engine.shutdown_calls == 0
        assert payload["backend_started"] is True
        assert payload["ui_active"] is True
    finally:
        runtime.shutdown()


def test_inbox_runtime_forces_sync_when_activating_cold_backend(tmp_path: Path) -> None:
    engine = _FakeEngine()
    runtime = InboxRuntime(ServiceContext(root_dir=tmp_path), engine)
    try:
        runtime.set_ui_active(True)

        assert _wait_until(lambda: runtime.diagnostics()["backend_started"] is True)
        assert engine.start_calls == 1
        assert engine.sync_calls == [True]
    finally:
        runtime.shutdown()


def test_inbox_runtime_reports_worker_crash_in_diagnostics(tmp_path: Path) -> None:
    engine = _FakeEngine()
    runtime = InboxRuntime(ServiceContext(root_dir=tmp_path), engine)
    runtime._builder.build_rows = lambda _rows: (_ for _ in ()).throw(RuntimeError("projection boom"))
    try:
        runtime.ensure_backend_started()

        assert _wait_until(lambda: runtime.diagnostics()["runtime_worker_state"] == "error")
        payload = runtime.diagnostics()

        assert payload["runtime_worker_alive"] is False
        assert payload["runtime_worker_state"] == "error"
        assert "RuntimeError: projection boom" in payload["runtime_worker_last_error"]
        assert payload["backend_started"] is False
        assert payload["projection_ready"] is False
        assert engine.shutdown_calls == 1
    finally:
        runtime.shutdown()


def test_inbox_runtime_keeps_unrelated_thread_cache_on_account_sync(tmp_path: Path) -> None:
    engine = _SelectiveCacheEngine()
    runtime = InboxRuntime(ServiceContext(root_dir=tmp_path), engine)
    try:
        thread = runtime.get_thread("acc-2:thread-2")
        assert thread is not None
        assert engine.get_thread_calls["acc-2:thread-2"] == 1

        runtime._rebuild_projection(
            reason="sync_account",
            full=False,
            account_ids=["acc-1"],
            invalidate_legacy=False,
        )
        cached_again = runtime.get_thread("acc-2:thread-2")
        assert cached_again is not None
        assert engine.get_thread_calls["acc-2:thread-2"] == 1

        runtime._rebuild_projection(
            reason="read_thread",
            full=False,
            thread_keys=["acc-2:thread-2"],
            invalidate_legacy=False,
        )
        refreshed = runtime.get_thread("acc-2:thread-2")
        assert refreshed is not None
        assert engine.get_thread_calls["acc-2:thread-2"] == 2
    finally:
        runtime.shutdown()


def test_inbox_runtime_hydrates_opened_thread_when_preview_lacks_real_timestamps(tmp_path: Path) -> None:
    engine = _OpenHydrationEngine(
        messages=[
            {
                "message_id": "preview-1",
                "text": "Hola",
                "timestamp": None,
                "direction": "inbound",
            }
        ]
    )
    runtime = InboxRuntime(ServiceContext(root_dir=tmp_path), engine)
    try:
        opened = runtime._open_thread_in_worker("acc-1:thread-1")

        assert opened is True
        assert engine.open_calls == ["acc-1:thread-1"]
        assert engine.hydrate_calls == ["acc-1:thread-1"]
    finally:
        runtime.shutdown()


def test_inbox_runtime_skips_hydration_when_opened_thread_already_has_real_timestamps(tmp_path: Path) -> None:
    engine = _OpenHydrationEngine(
        messages=[
            {
                "message_id": "msg-1",
                "text": "Hola",
                "timestamp": 100.0,
                "direction": "inbound",
            }
        ]
    )
    runtime = InboxRuntime(ServiceContext(root_dir=tmp_path), engine)
    try:
        opened = runtime._open_thread_in_worker("acc-1:thread-1")

        assert opened is True
        assert engine.open_calls == ["acc-1:thread-1"]
        assert engine.hydrate_calls == []
    finally:
        runtime.shutdown()
