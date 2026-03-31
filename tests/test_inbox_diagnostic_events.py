from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from core.inbox.account_worker import AccountWorker
from core.inbox.conversation_sender import ConversationSender
from core.inbox.conversation_store import ConversationStore
from src.auth import persistent_login
from src.inbox.inbox_storage import InboxStorage


class ComposerExplosion(RuntimeError):
    pass


class _FakePreparedRuntime:
    def __init__(self, account: dict[str, object]) -> None:
        self.account = dict(account)
        self.shutdown_calls = 0

    def set_diagnostic_context(self, *, thread_key: str = "", job_type: str = "") -> None:
        self.account["_inbox_diagnostic_thread_key"] = thread_key
        self.account["_inbox_diagnostic_job_type"] = job_type

    def shutdown(self) -> None:
        self.shutdown_calls += 1

    def run_async(self, _coro, *, timeout: float | None = None):
        del timeout
        return None

    def open_page(self, _account: dict[str, object], *, timeout: float | None = None):
        del timeout
        return object()

    def close_page(self, _page, *, timeout: float | None = None) -> None:
        del timeout
        return None


class _FakeClient:
    def __init__(self, *_args, thread_id: str, **_kwargs) -> None:
        self.thread_id = thread_id
        self.closed = False

    def ensure_thread_ready_strict(self, _thread_id: str) -> tuple[bool, str]:
        return True, "ok"

    def close(self) -> None:
        self.closed = True

    def _ensure_page(self):
        return object()


class _IdleBrowserPool:
    def shutdown(self) -> None:
        return None


class _ExplodingLoginService:
    instances: list["_ExplodingLoginService"] = []

    def __init__(
        self,
        *,
        headless: bool,
        base_profiles: Path,
        prefer_persistent: bool,
        browser_mode: str,
        subsystem: str,
    ) -> None:
        del headless, base_profiles, prefer_persistent, browser_mode, subsystem
        self.closed = False
        type(self).instances.append(self)

    async def new_context_for_account(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError("context exploded")

    async def close(self) -> None:
        self.closed = True

    async def record_diagnostic_failure(self, **_kwargs) -> None:
        return None

    async def save_storage_state(self, *_args, **_kwargs) -> None:
        return None


def _raise_prepare_failure(_self, _thread_id: str) -> None:
    raise ComposerExplosion("composer exploded")


def test_inbox_storage_creates_diagnostic_events_table(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(inbox_diagnostic_events)").fetchall()
        }
        assert {
            "id",
            "created_at",
            "account_id",
            "alias_id",
            "thread_key",
            "job_type",
            "stage",
            "event_type",
            "outcome",
            "reason_code",
            "reason",
            "file",
            "function",
            "line",
            "exception_type",
            "exception_message",
            "traceback",
            "payload_json",
        } <= columns
    finally:
        storage.shutdown()


def test_account_worker_prepare_persists_file_function_and_line(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("core.inbox.account_worker._PreparedRuntime", _FakePreparedRuntime)
    monkeypatch.setattr("core.inbox.account_worker.TaskDirectClient", _FakeClient)
    monkeypatch.setattr("core.inbox.account_worker.AccountWorker._focus_composer", _raise_prepare_failure)

    store = ConversationStore(tmp_path)
    try:
        worker = AccountWorker({"username": "acc1", "alias": "ventas"}, diagnostics_store=store)
        with pytest.raises(ComposerExplosion, match="composer exploded"):
            worker.prepare(
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "thread_href": "https://www.instagram.com/direct/t/thread-a/",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                }
            )

        event = next(
            item
            for item in store.list_diagnostic_events(thread_key="acc1:thread-a", limit=20)
            if item["event_type"] == "composer_ready_failed"
        )

        assert event["exception_type"] == "ComposerExplosion"
        assert event["exception_message"] == "composer exploded"
        assert event["function"] == "_raise_prepare_failure"
        assert event["line"] > 0
        assert event["file"].endswith("test_inbox_diagnostic_events.py")
        assert "composer exploded" in event["traceback"]
        assert event["reason_code"] == "unexpected_exception"
    finally:
        store.shutdown()


def test_sender_cancellation_persists_reason_code(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "lead_a",
                    "display_name": "Lead A",
                    "last_message_text": "hola",
                    "last_message_timestamp": 100.0,
                    "last_message_direction": "inbound",
                }
            ]
        )
        local = store.append_local_outbound_message("acc1:thread-a", "respuesta", source="auto")
        assert local is not None
        job_id = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "respuesta",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            },
        )
        sender = ConversationSender(store, _IdleBrowserPool(), notifier=lambda **_kwargs: None)
        job = store.get_send_queue_job(job_id)
        assert job is not None
        assert sender.queue_existing_job(job) is True

        cancelled = sender.cancel_pending_thread_jobs("acc1:thread-a", reason="manual_takeover")

        assert cancelled == 1
        event = next(
            item
            for item in store.list_diagnostic_events(thread_key="acc1:thread-a", limit=20)
            if item["event_type"] == "job_cancelled_by_takeover"
        )
        assert event["reason_code"] == "job_cancelled_by_takeover"
        assert event["job_type"] == "auto_reply"
        assert event["payload"]["job_id"] == job_id
    finally:
        store.shutdown()


def test_persistent_login_unexpected_failure_persists_traceback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(persistent_login, "PlaywrightService", _ExplodingLoginService)

    store = ConversationStore(tmp_path)
    profile_root = tmp_path / "profiles"
    account = {
        "username": "tester",
        "alias": "ventas",
        "password": "secret",
        "_inbox_diagnostics_store": store,
        "_inbox_diagnostic_thread_key": "tester:thread-a",
        "_inbox_diagnostic_job_type": "manual_reply",
    }
    try:
        with pytest.raises(RuntimeError, match="context exploded"):
            asyncio.run(
                persistent_login.ensure_logged_in_async(
                    account,
                    headless=True,
                    profile_root=profile_root,
                )
            )

        event = next(
            item
            for item in store.list_diagnostic_events(account_id="tester", limit=20)
            if item["event_type"] == "browser_launch_failed"
        )
        assert event["reason_code"] == "unexpected_exception"
        assert event["exception_type"] == "RuntimeError"
        assert event["thread_key"] == "tester:thread-a"
        assert "context exploded" in event["traceback"]
    finally:
        store.shutdown()


def test_account_worker_prepare_success_still_works_with_diagnostics_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("core.inbox.account_worker._PreparedRuntime", _FakePreparedRuntime)
    monkeypatch.setattr("core.inbox.account_worker.TaskDirectClient", _FakeClient)
    monkeypatch.setattr("core.inbox.account_worker.AccountWorker._focus_composer", lambda self, _thread_id: None)

    store = ConversationStore(tmp_path)
    try:
        worker = AccountWorker({"username": "acc1", "alias": "ventas"}, diagnostics_store=store)

        result = worker.prepare(
            {
                "thread_key": "acc1:thread-a",
                "thread_id": "thread-a",
                "thread_href": "https://www.instagram.com/direct/t/thread-a/",
                "account_id": "acc1",
                "alias_id": "ventas",
            }
        )

        assert result == {"ok": True, "reason": "prepared"}
        failures = [
            item
            for item in store.list_diagnostic_events(thread_key="acc1:thread-a", limit=20)
            if item["outcome"] == "fail"
        ]
        assert failures == []
    finally:
        store.shutdown()
