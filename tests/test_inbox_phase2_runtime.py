from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from application.services import AutomationService, ServiceContext
from application.services.inbox_automation_service import InboxAutomationService
from core.inbox.conversation_sender import ConversationSender
from core.inbox.conversation_store import ConversationStore
from paths import storage_root
from src.inbox.inbox_storage import InboxStorage
from src.runtime.alias_runtime_scheduler import AliasRuntimeScheduler
from src.runtime.inbox_automation_runtime import InboxAutomationRuntime
from src.runtime.ownership_router import OwnershipRouter
from src.runtime.session_connector_registry import SessionConnectorRegistry


def _automation_service_for_storage(storage: InboxStorage) -> InboxAutomationService:
    return InboxAutomationService(
        store=SimpleNamespace(
            get_thread=storage.get_thread,
            update_thread_record=storage.update_thread_record,
            cancel_send_queue_jobs=storage.cancel_send_queue_jobs,
            add_thread_event=storage.add_thread_event,
            list_runtime_alias_states=storage.list_runtime_alias_states,
            get_runtime_alias_state=storage.get_runtime_alias_state,
        ),
        sender=SimpleNamespace(),
        ensure_backend_started=lambda: None,
    )


def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.05, float(timeout or 0.05))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(max(0.005, float(interval or 0.005)))
    return bool(predicate())


def test_inbox_storage_persists_operational_thread_fields_and_runtime_state(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "open",
                    "stage_id": "stage_2",
                    "followup_level": 2,
                    "manual_lock": True,
                    "manual_assignee": "operator-1",
                    "last_action_type": "manual_reply",
                    "last_action_at": 120.0,
                    "last_pack_sent": "pack-1",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "outbound",
                    "unread_count": 0,
                }
            ]
        )
        storage.upsert_runtime_alias_state(
            "ventas",
            {
                "is_running": True,
                "current_account_id": "acc1",
                "next_account_id": "acc2",
                "current_turn_count": 1,
                "max_turns_per_account": 3,
                "delay_min_ms": 1000,
                "delay_max_ms": 2000,
                "mode": "both",
                "stats": {"queued_jobs": 4},
            },
        )

        thread = storage.get_thread("acc1:thread-a")
        runtime_state = storage.get_runtime_alias_state("ventas")

        assert thread is not None
        assert thread["owner"] == "manual"
        assert thread["bucket"] == "qualified"
        assert thread["status"] == "open"
        assert thread["stage_id"] == "stage_2"
        assert thread["manual_lock"] is True
        assert thread["manual_assignee"] == "operator-1"
        assert thread["last_pack_sent"] == "pack-1"
        assert runtime_state["is_running"] is True
        assert runtime_state["current_account_id"] == "acc1"
        assert runtime_state["stats"]["queued_jobs"] == 4
        assert runtime_state["worker_state"] == "stopped"
        assert runtime_state["last_heartbeat_at"] is None
    finally:
        storage.shutdown()

    reopened = InboxStorage(tmp_path)
    try:
        thread = reopened.get_thread("acc1:thread-a")
        runtime_state = reopened.get_runtime_alias_state("ventas")

        assert thread is not None
        assert thread["owner"] == "manual"
        assert thread["bucket"] == "qualified"
        assert thread["stage_id"] == "stage_2"
        assert runtime_state["is_running"] is True
        assert runtime_state["current_account_id"] == "acc1"
        assert runtime_state["worker_state"] == "stopped"
    finally:
        reopened.shutdown()


def test_automation_service_wrapper_does_not_start_or_stop_inbox_runtime_bridge(tmp_path: Path) -> None:
    class _FakeBridge:
        def __init__(self) -> None:
            self.started: list[tuple[str, dict[str, object]]] = []
            self.stopped: list[str] = []
            self._state = {
                "ventas": {
                    "alias_id": "ventas",
                    "is_running": True,
                    "current_account_id": "acc1",
                    "next_account_id": "acc2",
                    "current_turn_count": 1,
                    "max_turns_per_account": 2,
                    "delay_min_ms": 1000,
                    "delay_max_ms": 2000,
                    "mode": "both",
                    "stats": {"queued_jobs": 2, "errors": 0},
                }
            }

        def start_alias(self, alias_id: str, config: dict[str, object]) -> dict[str, object]:
            self.started.append((alias_id, dict(config)))
            return dict(self._state[alias_id])

        def stop_alias(self, alias_id: str) -> dict[str, object]:
            self.stopped.append(alias_id)
            state = dict(self._state[alias_id])
            state["is_running"] = False
            self._state[alias_id] = state
            return state

        def status(self, alias_id: str) -> dict[str, object]:
            return dict(self._state.get(alias_id, {}))

        def alias_accounts(self, alias_id: str) -> list[dict[str, object]]:
            assert alias_id == "ventas"
            return [{"username": "acc1", "assigned_proxy_id": "proxy-a"}]

    bridge = _FakeBridge()
    service = AutomationService(ServiceContext.default(root_dir=tmp_path), inbox_service=SimpleNamespace(_automation=bridge))

    snapshot = service.start_autoresponder({"alias": "ventas", "delay_min": 1, "delay_max": 2, "threads": 3})

    assert bridge.started == []
    assert snapshot["status"] == "Running"
    assert snapshot["task_active"] is True
    assert snapshot["current_account"] == "acc1"

    service.stop_autoresponder("stop desde test")
    stopped_snapshot = service.autoresponder_snapshot("ventas")

    assert bridge.stopped == []
    assert stopped_snapshot["status"] == "Running"
    assert stopped_snapshot["task_active"] is True
    assert stopped_snapshot["current_account"] == "acc1"


def test_inbox_runtime_status_invalidates_stale_alias_without_active_accounts(tmp_path: Path, monkeypatch) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.upsert_runtime_alias_state(
            "ventas",
            {
                "is_running": True,
                "current_account_id": "ghost-account",
                "next_account_id": "ghost-next",
                "current_turn_count": 2,
                "mode": "both",
            },
        )
        monkeypatch.setattr("src.runtime.inbox_automation_runtime.accounts_module.list_all", lambda: [])
        service = InboxAutomationService(
            store=store,
            sender=SimpleNamespace(),
            ensure_backend_started=lambda: None,
        )

        status = service.status("ventas")
        persisted = store.get_runtime_alias_state("ventas")
        snapshot = AutomationService(
            ServiceContext.default(root_dir=tmp_path),
            inbox_service=SimpleNamespace(_automation=service),
        ).autoresponder_snapshot("ventas")

        assert status["account_rows"] == []
        assert status["is_running"] is False
        assert status["current_account_id"] == ""
        assert status["next_account_id"] == ""
        assert status["current_turn_count"] == 0
        assert status["last_error"] == "no_active_accounts"
        assert persisted["is_running"] is False
        assert persisted["last_error"] == "no_active_accounts"
        assert snapshot["status"] == "Stopped"
        assert snapshot["task_active"] is False
        assert snapshot["account_rows"] == []
    finally:
        store.shutdown()


def test_inbox_runtime_list_alias_accounts_excludes_usage_deactivated_accounts(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.runtime.inbox_automation_runtime.accounts_module.list_all",
        lambda: [
            {"username": "acc1", "alias": "ventas", "active": True, "usage_state": "active"},
            {"username": "acc2", "alias": "ventas", "active": True, "usage_state": "deactivated"},
            {"username": "acc3", "alias": "soporte", "active": True, "usage_state": "active"},
        ],
    )

    runtime = InboxAutomationRuntime(
        store=SimpleNamespace(),
        sender=SimpleNamespace(),
        ensure_backend_started=lambda: None,
    )

    rows = runtime.list_alias_accounts("ventas")

    assert [str(row.get("username") or "") for row in rows] == ["acc1"]


def test_manual_takeover_cancels_queued_auto_jobs(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "stage_id": "initial",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        storage.update_thread_state(
            "acc1:thread-a",
            {
                "pending_reply": True,
                "pending_inbound_id": "in-queued-1",
            },
        )
        local = storage.append_local_outbound_message("acc1:thread-a", "respuesta automatica", source="auto")
        assert local is not None
        job_id = storage.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "respuesta automatica",
                "local_message_id": str(local["message_id"]),
                "post_send_state_updates": {
                    "pending_reply": False,
                    "pending_inbound_id": None,
                    "last_inbound_id_seen": "in-queued-1",
                },
            },
            dedupe_key="auto:acc1:thread-a:1",
        )
        service = InboxAutomationService(
            store=SimpleNamespace(
                get_thread=storage.get_thread,
                update_thread_record=storage.update_thread_record,
                cancel_send_queue_jobs=storage.cancel_send_queue_jobs,
                add_thread_event=storage.add_thread_event,
                list_runtime_alias_states=storage.list_runtime_alias_states,
                get_runtime_alias_state=storage.get_runtime_alias_state,
            ),
            sender=SimpleNamespace(),
            ensure_backend_started=lambda: None,
        )

        updated = service.manual_takeover("acc1:thread-a", operator_id="operator-1")
        jobs = storage.list_send_queue_jobs(states=["cancelled"], limit=10)
        events = storage.list_thread_events("acc1:thread-a", limit=10)

        assert updated is not None
        assert updated["owner"] == "manual"
        assert updated["bucket"] == "qualified"
        assert updated["manual_assignee"] == "operator-1"
        assert [job["id"] for job in jobs] == [job_id]
        assert any(event["event_type"] == "manual_taken" for event in events)
        thread = storage.get_thread("acc1:thread-a")
        assert thread is not None
        assert thread["messages"][-1]["sent_status"] == "failed"
        assert thread["pending_reply"] is False
        assert thread.get("pending_inbound_id") in {None, ""}
    finally:
        storage.shutdown()


def test_manual_takeover_cancels_processing_auto_job_before_send(tmp_path: Path, monkeypatch) -> None:
    class _FakeBrowserPool:
        def __init__(self) -> None:
            self.send_text_calls = 0

        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            self.send_text_calls += 1
            return {"ok": True, "item_id": "msg-1"}

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "stage_id": "initial",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
        store.update_thread_state(
            "acc1:thread-a",
            {
                "pending_reply": True,
                "pending_inbound_id": "in-processing-1",
            },
        )
        local = store.append_local_outbound_message("acc1:thread-a", "respuesta automatica", source="auto")
        assert local is not None
        job_id = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "respuesta automatica",
                "local_message_id": str(local["message_id"]),
                "post_send_state_updates": {
                    "pending_reply": False,
                    "pending_inbound_id": None,
                    "last_inbound_id_seen": "in-processing-1",
                },
            },
            dedupe_key="auto:acc1:thread-a:processing",
        )
        browser_pool = _FakeBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        service = InboxAutomationService(
            store=store,
            sender=sender,
            ensure_backend_started=lambda: None,
        )
        original_validate = sender._validate_job_sendability
        processing_gate = threading.Event()
        continue_gate = threading.Event()
        calls = {"count": 0}

        def _wrapped_validate(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                processing_gate.set()
                assert continue_gate.wait(timeout=2.0)
            return original_validate(*args, **kwargs)

        monkeypatch.setattr(sender, "_validate_job_sendability", _wrapped_validate)
        worker = threading.Thread(
            target=sender._handle_send_message,
            args=(
                {
                    "job_id": job_id,
                    "thread_key": "acc1:thread-a",
                    "text": "respuesta automatica",
                    "local_message_id": str(local["message_id"]),
                    "job_type": "auto_reply",
                },
            ),
            daemon=True,
        )

        worker.start()
        assert processing_gate.wait(timeout=2.0)

        updated = service.manual_takeover("acc1:thread-a", operator_id="operator-1")

        continue_gate.set()
        worker.join(timeout=2.0)
        jobs = store.list_send_queue_jobs(states=["cancelled"], limit=10)
        thread = store.get_thread("acc1:thread-a")

        assert worker.is_alive() is False
        assert updated is not None
        assert updated["owner"] == "manual"
        assert updated["manual_assignee"] == "operator-1"
        assert calls["count"] >= 2
        assert browser_pool.send_text_calls == 0
        assert [job["id"] for job in jobs] == [job_id]
        assert thread is not None
        assert thread["messages"][-1]["sent_status"] == "failed"
        assert thread["pending_reply"] is False
        assert thread.get("pending_inbound_id") in {None, ""}
    finally:
        store.shutdown()


def test_sender_persists_last_send_attempt_into_runtime_alias_state(tmp_path: Path) -> None:
    class _FakeBrowserPool:
        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            return {"ok": True, "item_id": "msg-1"}

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "stage_id": "initial",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
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
            },
            dedupe_key="auto:acc1:thread-a:last_attempt",
        )
        sender = ConversationSender(store, _FakeBrowserPool(), notifier=lambda **_kwargs: None)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "respuesta",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            }
        )

        state = store.get_runtime_alias_state("ventas")
        assert state.get("last_send_attempt_account_id") == "acc1"
        assert state.get("last_send_attempt_thread_key") == "acc1:thread-a"
        assert int(state.get("last_send_attempt_job_id") or 0) == job_id
        assert state.get("last_send_attempt_job_type") == "auto_reply"
        assert state.get("last_send_attempt_outcome") == "success"
        assert state.get("last_send_attempt_reason_code") == "success"
        assert isinstance(state.get("last_send_attempt_at"), (int, float))
        assert float(state.get("last_send_attempt_at") or 0) > 0

        assert state.get("last_send_outcome") == "sent"
        assert state.get("last_send_reason_code") == "success"
        assert state.get("last_send_reason") == "success"
        assert state.get("last_send_account_id") == "acc1"
        assert state.get("last_send_thread_key") == "acc1:thread-a"
        assert int(state.get("last_send_job_id") or 0) == job_id
        assert state.get("last_send_job_type") == "auto_reply"
        assert isinstance(state.get("last_send_at"), (int, float))
        assert float(state.get("last_send_at") or 0) > 0
    finally:
        store.shutdown()


def test_sender_persists_last_send_failed_outcome_into_runtime_alias_state(tmp_path: Path) -> None:
    class _FakeBrowserPool:
        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            return {"ok": False, "reason": "composer_not_found"}

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "stage_id": "initial",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
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
            },
            dedupe_key="auto:acc1:thread-a:last_attempt",
        )
        sender = ConversationSender(store, _FakeBrowserPool(), notifier=lambda **_kwargs: None)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "respuesta",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            }
        )

        state = store.get_runtime_alias_state("ventas")
        assert state.get("last_send_outcome") == "failed"
        assert state.get("last_send_reason_code") == "composer_not_found"
        assert state.get("last_send_reason") == "composer_not_found"
        assert state.get("last_send_account_id") == "acc1"
        assert state.get("last_send_thread_key") == "acc1:thread-a"
        assert int(state.get("last_send_job_id") or 0) == job_id
        assert state.get("last_send_job_type") == "auto_reply"
        assert isinstance(state.get("last_send_at"), (int, float))
        assert float(state.get("last_send_at") or 0) > 0
    finally:
        store.shutdown()


def test_sender_persists_last_send_cancelled_outcome_into_runtime_alias_state(tmp_path: Path) -> None:
    class _FakeBrowserPool:
        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            raise AssertionError("send_text should not be called for cancelled jobs")

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "stage_id": "initial",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": False})
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
            },
            dedupe_key="auto:acc1:thread-a:last_attempt",
        )
        sender = ConversationSender(store, _FakeBrowserPool(), notifier=lambda **_kwargs: None)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "respuesta",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            }
        )

        state = store.get_runtime_alias_state("ventas")
        assert state.get("last_send_outcome") == "cancelled"
        assert state.get("last_send_reason_code") == "job_cancelled_by_runtime_stop"
        assert state.get("last_send_reason") == "runtime_inactive"
        assert state.get("last_send_account_id") == "acc1"
        assert state.get("last_send_thread_key") == "acc1:thread-a"
        assert int(state.get("last_send_job_id") or 0) == job_id
        assert state.get("last_send_job_type") == "auto_reply"
        assert isinstance(state.get("last_send_at"), (int, float))
        assert float(state.get("last_send_at") or 0) > 0
    finally:
        store.shutdown()


def test_manual_takeover_keeps_manual_thread_and_cancels_auto_jobs(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "open",
                    "manual_lock": True,
                    "manual_assignee": "operator-1",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        local = storage.append_local_outbound_message("acc1:thread-a", "respuesta automatica", source="auto")
        assert local is not None
        job_id = storage.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "respuesta automatica",
                "local_message_id": str(local["message_id"]),
            },
            dedupe_key="auto:acc1:thread-a:manual",
        )
        service = InboxAutomationService(
            store=SimpleNamespace(
                get_thread=storage.get_thread,
                update_thread_record=storage.update_thread_record,
                cancel_send_queue_jobs=storage.cancel_send_queue_jobs,
                add_thread_event=storage.add_thread_event,
                list_runtime_alias_states=storage.list_runtime_alias_states,
                get_runtime_alias_state=storage.get_runtime_alias_state,
            ),
            sender=SimpleNamespace(),
            ensure_backend_started=lambda: None,
        )

        updated = service.manual_takeover("acc1:thread-a", operator_id="operator-2")
        jobs = storage.list_send_queue_jobs(states=["cancelled"], limit=10)
        thread = storage.get_thread("acc1:thread-a")

        assert updated is not None
        assert updated["owner"] == "manual"
        assert updated["bucket"] == "qualified"
        assert updated["manual_assignee"] == "operator-2"
        assert [job["id"] for job in jobs] == [job_id]
        assert thread is not None
        assert thread["manual_lock"] is True
        assert thread["messages"][-1]["sent_status"] == "failed"
    finally:
        storage.shutdown()


def test_manual_release_restores_previous_context_after_takeover_from_all(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "pending",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        service = _automation_service_for_storage(storage)

        taken = service.manual_takeover("acc1:thread-a", operator_id="operator-1")
        released = service.manual_release("acc1:thread-a")

        assert taken is not None
        assert taken["owner"] == "manual"
        assert taken["bucket"] == "qualified"
        assert taken["status"] == "open"
        assert taken["previous_bucket"] == "all"
        assert taken["previous_status"] == "pending"
        assert taken["previous_owner"] == "auto"

        assert released is not None
        assert released["owner"] == "auto"
        assert released["bucket"] == "all"
        assert released["status"] == "pending"
        assert released["manual_lock"] is False
        assert released["manual_assignee"] == ""
        assert "previous_bucket" not in released
        assert "previous_status" not in released
        assert "previous_owner" not in released
    finally:
        storage.shutdown()


def test_manual_release_keeps_qualified_bucket_after_takeover_from_qualified(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "qualified",
                    "status": "replied",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "outbound",
                    "unread_count": 0,
                }
            ]
        )
        service = _automation_service_for_storage(storage)

        taken = service.manual_takeover("acc1:thread-a", operator_id="operator-1")
        released = service.manual_release("acc1:thread-a")

        assert taken is not None
        assert taken["previous_bucket"] == "qualified"
        assert taken["previous_status"] == "replied"
        assert taken["previous_owner"] == "auto"

        assert released is not None
        assert released["owner"] == "auto"
        assert released["bucket"] == "qualified"
        assert released["status"] == "replied"
        assert OwnershipRouter().can_followup_touch(released) is False
    finally:
        storage.shutdown()


def test_manual_release_keeps_current_context_for_legacy_manual_thread_without_snapshot(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "replied",
                    "manual_lock": True,
                    "manual_assignee": "operator-legacy",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "outbound",
                    "unread_count": 0,
                }
            ]
        )
        service = _automation_service_for_storage(storage)

        released = service.manual_release("acc1:thread-a")

        assert released is not None
        assert released["owner"] == "auto"
        assert released["bucket"] == "qualified"
        assert released["status"] == "replied"
        assert OwnershipRouter().can_followup_touch(released) is False
    finally:
        storage.shutdown()


def test_followup_touchability_is_conservative_for_qualified_manual_and_closed_threads() -> None:
    router = OwnershipRouter()

    assert router.can_followup_touch({"owner": "auto", "bucket": "all", "status": "open"}) is True
    assert router.can_followup_touch({"owner": "auto", "bucket": "qualified", "status": "replied"}) is False
    assert router.can_followup_touch({"owner": "manual", "bucket": "qualified", "status": "open"}) is False
    assert router.can_followup_touch({"owner": "none", "bucket": "disqualified", "status": "closed"}) is False
    assert router.can_followup_touch({"owner": "auto", "bucket": "all", "status": "closed"}) is False


def test_manual_takeover_preserves_existing_release_snapshot_for_manual_thread(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "account_id": "acc1",
                    "alias_id": "ventas",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "open",
                    "manual_lock": True,
                    "manual_assignee": "operator-1",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        storage.update_thread_state(
            "acc1:thread-a",
            {
                "previous_bucket": "all",
                "previous_status": "pending",
                "previous_owner": "auto",
            },
        )
        service = _automation_service_for_storage(storage)

        taken = service.manual_takeover("acc1:thread-a", operator_id="operator-2")
        released = service.manual_release("acc1:thread-a")

        assert taken is not None
        assert taken["manual_assignee"] == "operator-2"
        assert taken["previous_bucket"] == "all"
        assert taken["previous_status"] == "pending"
        assert taken["previous_owner"] == "auto"

        assert released is not None
        assert released["owner"] == "auto"
        assert released["bucket"] == "all"
        assert released["status"] == "pending"
    finally:
        storage.shutdown()


def test_sender_cancels_auto_job_when_thread_is_manual(tmp_path: Path) -> None:
    class _FakeBrowserPool:
        def __init__(self) -> None:
            self.send_text_calls = 0

        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            self.send_text_calls += 1
            return {"ok": True, "item_id": "msg-1"}

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "open",
                    "manual_lock": True,
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        local = store.append_local_outbound_message("acc1:thread-a", "auto reply", source="auto")
        assert local is not None
        job_id = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "auto reply",
                "local_message_id": str(local["message_id"]),
            },
        )
        browser_pool = _FakeBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "auto reply",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            }
        )

        jobs = store.list_send_queue_jobs(states=["cancelled"], limit=10)
        thread = store.get_thread("acc1:thread-a")

        assert browser_pool.send_text_calls == 0
        assert [job["id"] for job in jobs] == [job_id]
        assert thread is not None
        assert thread["messages"][-1]["sent_status"] == "failed"
    finally:
        store.shutdown()


def test_stop_alias_cancels_only_auto_and_followup_jobs_for_alias(tmp_path: Path) -> None:
    class _IdleRuntime:
        def list_alias_accounts(self, _alias_id: str) -> list[dict[str, object]]:
            return []

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                },
                {
                    "thread_key": "acc2:thread-b",
                    "thread_id": "thread-b",
                    "account_id": "acc2",
                    "alias_id": "soporte",
                    "account_alias": "soporte",
                    "recipient_username": "cliente_b",
                    "display_name": "Cliente B",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                },
            ]
        )
        auto_local = store.append_local_outbound_message("acc1:thread-a", "auto", source="auto")
        time.sleep(0.002)
        followup_local = store.append_local_outbound_message("acc1:thread-a", "followup", source="followup")
        time.sleep(0.002)
        manual_local = store.append_local_outbound_message("acc1:thread-a", "manual", source="manual")
        time.sleep(0.002)
        other_alias_local = store.append_local_outbound_message("acc2:thread-b", "auto otro alias", source="auto")
        assert auto_local is not None
        assert followup_local is not None
        assert manual_local is not None
        assert other_alias_local is not None

        auto_job = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={"thread_key": "acc1:thread-a", "text": "auto", "local_message_id": str(auto_local["message_id"])},
        )
        followup_job = store.create_send_queue_job(
            "followup",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={"thread_key": "acc1:thread-a", "text": "followup", "local_message_id": str(followup_local["message_id"])},
        )
        manual_job = store.create_send_queue_job(
            "manual_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={"thread_key": "acc1:thread-a", "text": "manual", "local_message_id": str(manual_local["message_id"])},
        )
        other_alias_job = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc2:thread-b",
            account_id="acc2",
            payload={
                "thread_key": "acc2:thread-b",
                "text": "auto otro alias",
                "local_message_id": str(other_alias_local["message_id"]),
            },
        )
        scheduler = AliasRuntimeScheduler(runtime=_IdleRuntime(), store=store)
        scheduler.start_alias("ventas", {})

        stopped = scheduler.stop_alias("ventas")
        cancelled_jobs = {job["id"]: job for job in store.list_send_queue_jobs(states=["cancelled"], limit=20)}
        queued_jobs = {job["id"]: job for job in store.list_send_queue_jobs(states=["queued"], limit=20)}
        ventas_thread = store.get_thread("acc1:thread-a")

        assert stopped["is_running"] is False
        assert set(cancelled_jobs) == {auto_job, followup_job}
        assert set(queued_jobs) == {manual_job, other_alias_job}
        assert ventas_thread is not None
        failed_rows = [
            message
            for message in ventas_thread["messages"]
            if str(message.get("sent_status") or "") == "failed"
        ]
        assert any(str(message.get("text") or "") == "auto" for message in failed_rows)
        assert any(str(message.get("text") or "") == "followup" for message in failed_rows)
        assert not any(
            str(message.get("text") or "") == "manual" and str(message.get("sent_status") or "") == "failed"
            for message in ventas_thread["messages"]
        )
    finally:
        store.shutdown()


def test_sender_rechecks_runtime_before_auto_send(tmp_path: Path, monkeypatch) -> None:
    class _FakeBrowserPool:
        def __init__(self) -> None:
            self.send_text_calls = 0

        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            self.send_text_calls += 1
            return {"ok": True, "item_id": "msg-1"}

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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": True})
        local = store.append_local_outbound_message("acc1:thread-a", "auto reply", source="auto")
        assert local is not None
        job_id = store.create_send_queue_job(
            "auto_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "auto reply",
                "local_message_id": str(local["message_id"]),
            },
        )
        browser_pool = _FakeBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        original_validate = sender._validate_job_sendability
        calls = {"count": 0}

        def _wrapped_validate(*args, **kwargs):
            result = original_validate(*args, **kwargs)
            calls["count"] += 1
            if calls["count"] == 1:
                store.upsert_runtime_alias_state("ventas", {"is_running": False})
            return result

        monkeypatch.setattr(sender, "_validate_job_sendability", _wrapped_validate)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "auto reply",
                "local_message_id": str(local["message_id"]),
                "job_type": "auto_reply",
            }
        )

        jobs = store.list_send_queue_jobs(states=["cancelled"], limit=10)
        thread = store.get_thread("acc1:thread-a")

        assert calls["count"] >= 2
        assert browser_pool.send_text_calls == 0
        assert [job["id"] for job in jobs] == [job_id]
        assert thread is not None
        assert thread["messages"][-1]["sent_status"] == "failed"
    finally:
        store.shutdown()


def test_alias_runtime_crash_clears_running_and_persists_last_error(tmp_path: Path) -> None:
    class _CrashingRuntime:
        def list_alias_accounts(self, _alias_id: str) -> list[dict[str, object]]:
            return [{"username": "acc1"}]

        def process_account_turn(self, _account: dict[str, object], *, mode: str) -> dict[str, object]:
            assert mode == "both"
            raise RuntimeError("boom")

    store = ConversationStore(tmp_path)
    try:
        scheduler = AliasRuntimeScheduler(runtime=_CrashingRuntime(), store=store)
        started = scheduler.start_alias("ventas", {"mode": "both"})

        assert started["is_running"] is True
        assert _wait_until(lambda: scheduler.status("ventas").get("is_running") is False)

        state = scheduler.status("ventas")
        persisted = store.get_runtime_alias_state("ventas")

        assert state["is_running"] is False
        assert state["worker_state"] == "error"
        assert "RuntimeError: boom" in state["last_error"]
        assert state["current_account_id"] == ""
        assert state["current_turn_count"] == 0
        assert state["last_heartbeat_at"] is not None
        assert persisted["is_running"] is False
        assert persisted["worker_state"] == "error"
        assert persisted["current_account_id"] == ""
        assert persisted["current_turn_count"] == 0
    finally:
        store.shutdown()


def test_alias_runtime_status_degrades_when_heartbeat_is_stale(tmp_path: Path) -> None:
    class _IdleRuntime:
        def list_alias_accounts(self, _alias_id: str) -> list[dict[str, object]]:
            return [{"username": "acc1"}]

    def _sleep_forever() -> None:
        while True:
            time.sleep(0.1)

    store = ConversationStore(tmp_path)
    sleeper = threading.Thread(target=_sleep_forever, daemon=True)
    sleeper.start()
    try:
        scheduler = AliasRuntimeScheduler(runtime=_IdleRuntime(), store=store)
        scheduler._threads["ventas"] = sleeper
        scheduler._stops["ventas"] = threading.Event()
        store.upsert_runtime_alias_state(
            "ventas",
            {
                "is_running": True,
                "worker_state": "running",
                "current_account_id": "acc1",
                "current_turn_count": 1,
                "last_heartbeat_at": time.time() - 120.0,
            },
        )

        state = scheduler.status("ventas")

        assert state["is_running"] is False
        assert state["worker_state"] == "degraded"
        assert state["current_account_id"] == ""
        assert state["current_turn_count"] == 0
        assert state["last_error"] == "worker_heartbeat_stale"
    finally:
        store.shutdown()


def test_session_connector_ready_state_degrades_when_heartbeat_goes_stale(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        registry = SessionConnectorRegistry(
            account_resolver=lambda account_id: {"username": account_id, "alias": "ventas", "active": True},
            store=store,
        )
        store.upsert_session_connector_state(
            "acc1",
            {
                "alias_id": "ventas",
                "state": "ready",
                "last_heartbeat_at": time.time() - 120.0,
            },
        )

        ready = registry.is_ready("acc1")
        state = store.get_session_connector_state("acc1")

        assert ready is False
        assert state["state"] == "degraded"
        assert state["last_error"] == "heartbeat_stale"
        assert state["last_heartbeat_at"] is not None
    finally:
        store.shutdown()


def test_boot_sweep_cleans_runtime_alias_state_and_deletes_missing_alias(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.upsert_runtime_alias_state(
            "ventas",
            {
                "is_running": True,
                "worker_state": "running",
                "current_account_id": "ghost-account",
                "next_account_id": "ghost-next",
                "current_turn_count": 3,
                "last_error": "",
            },
        )
        store.upsert_runtime_alias_state(
            "legacy",
            {
                "is_running": True,
                "worker_state": "running",
                "current_account_id": "ghost-account",
                "current_turn_count": 1,
            },
        )

        summary = AliasRuntimeScheduler.sweep_boot_persisted_states(
            store=store,
            existing_aliases={"ventas"},
            active_alias_accounts={"ventas": {"acc1"}},
            now=500.0,
        )
        ventas = store.get_runtime_alias_state("ventas")

        assert summary["checked"] == 2
        assert summary["cleaned"] == 1
        assert summary["deleted"] == 1
        assert ventas["is_running"] is False
        assert ventas["worker_state"] == "stopped"
        assert ventas["current_account_id"] == ""
        assert ventas["next_account_id"] == ""
        assert ventas["current_turn_count"] == 0
        assert ventas["last_error"] == "boot_stale_runtime_cleaned"
        assert store.get_runtime_alias_state("legacy") == {}
    finally:
        store.shutdown()


def test_boot_sweep_cleans_session_connector_state_and_deletes_missing_account(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.upsert_session_connector_state(
            "acc1",
            {
                "alias_id": "otro",
                "state": "ready",
                "proxy_key": "proxy-old",
                "last_error": "",
            },
        )
        store.upsert_session_connector_state(
            "ghost",
            {
                "alias_id": "ventas",
                "state": "ready",
                "proxy_key": "proxy-old",
            },
        )

        summary = SessionConnectorRegistry.sweep_boot_persisted_states(
            store=store,
            accounts_by_id={
                "acc1": {
                    "username": "acc1",
                    "alias": "ventas",
                    "assigned_proxy_id": "proxy-a",
                    "active": True,
                }
            },
            now=900.0,
        )
        acc1 = store.get_session_connector_state("acc1")

        assert summary["checked"] == 2
        assert summary["cleaned"] == 1
        assert summary["deleted"] == 1
        assert acc1["state"] == "offline"
        assert acc1["alias_id"] == "ventas"
        assert acc1["proxy_key"] == "proxy-a"
        assert acc1["last_error"] == "boot_stale_connector_cleaned"
        assert store.get_session_connector_state("ghost") == {}
    finally:
        store.shutdown()


def test_sender_manual_reply_still_sends_when_runtime_inactive(tmp_path: Path, monkeypatch) -> None:
    class _FakeBrowserPool:
        def __init__(self) -> None:
            self.send_text_calls = 0

        def shutdown(self) -> None:
            return None

        def send_text(self, _thread, _text):
            self.send_text_calls += 1
            return {"ok": True, "item_id": "msg-1"}

    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
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
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "owner": "manual",
                    "bucket": "qualified",
                    "status": "open",
                    "manual_lock": True,
                    "manual_assignee": "operator-1",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 120.0,
                    "last_message_direction": "inbound",
                    "unread_count": 1,
                }
            ]
        )
        store.upsert_runtime_alias_state("ventas", {"is_running": False})
        local = store.append_local_outbound_message("acc1:thread-a", "manual reply", source="manual")
        assert local is not None
        job_id = store.create_send_queue_job(
            "manual_reply",
            thread_key="acc1:thread-a",
            account_id="acc1",
            payload={
                "thread_key": "acc1:thread-a",
                "text": "manual reply",
                "local_message_id": str(local["message_id"]),
            },
        )
        browser_pool = _FakeBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)

        sender._handle_send_message(
            {
                "job_id": job_id,
                "thread_key": "acc1:thread-a",
                "text": "manual reply",
                "local_message_id": str(local["message_id"]),
                "job_type": "manual_reply",
            }
        )

        jobs = store.list_send_queue_jobs(states=["confirmed"], limit=10)
        thread = store.get_thread("acc1:thread-a")

        assert browser_pool.send_text_calls == 1
        assert [job["id"] for job in jobs] == [job_id]
        assert thread is not None
        assert thread["messages"][-1]["delivery_status"] == "sent"
    finally:
        store.shutdown()


def test_sender_persists_confirmed_browser_timestamp_into_thread_history(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool(
            send_text_result={
                "ok": True,
                "item_id": "msg-ts-1",
                "timestamp": 321.5,
                "reason": "endpoint_confirmed",
            }
        )
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_text", "job_type": "auto_reply", "text": "respuesta automatica"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_message(_queued_job_payload(queued_job))
        thread = store.get_thread("acc1:thread-a")

        assert thread is not None
        assert thread["messages"][-1]["message_id"] == "msg-ts-1"
        assert thread["messages"][-1]["timestamp"] == 321.5
        assert thread["last_message_timestamp"] == 321.5
        assert thread["last_outbound_at"] == 321.5
    finally:
        store.shutdown()


class _FakeDeliveryBrowserPool:
    def __init__(self, *, send_text_result=None, send_pack_result=None) -> None:
        self._send_text_result = dict(send_text_result or {"ok": True, "item_id": "msg-1", "reason": "thread_read_confirmed"})
        self._send_pack_result = dict(send_pack_result or {"ok": True, "item_id": "pack-msg-1", "reason": "thread_read_confirmed"})
        self.send_text_calls = 0
        self.send_pack_calls = 0

    def shutdown(self) -> None:
        return None

    def send_text(self, _thread, _text):
        self.send_text_calls += 1
        return dict(self._send_text_result)

    def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None):
        del conversation_text, flow_config
        self.send_pack_calls += 1
        return dict(self._send_pack_result)


def _seed_runtime_thread(store: ConversationStore) -> None:
    store.upsert_threads(
        [
            {
                "thread_key": "acc1:thread-a",
                "thread_id": "thread-a",
                "account_id": "acc1",
                "alias_id": "ventas",
                "account_alias": "ventas",
                "recipient_username": "cliente_a",
                "display_name": "Cliente A",
                "owner": "auto",
                "bucket": "all",
                "status": "open",
                "stage_id": "initial",
                "last_message_text": "Hola",
                "last_message_timestamp": 120.0,
                "last_message_direction": "inbound",
                "unread_count": 1,
            }
        ]
    )
    store.upsert_runtime_alias_state("ventas", {"is_running": True})


def _queued_job_payload(job: dict[str, object]) -> dict[str, object]:
    payload = dict(job.get("payload") or {})
    return {
        "job_id": int(job.get("id") or 0),
        "thread_key": str(job.get("thread_key") or payload.get("thread_key") or "").strip(),
        "job_type": str(job.get("job_type") or job.get("task_type") or "").strip(),
        **payload,
    }


def test_thread_events_record_queued_then_sent_auto_reply_without_legacy_duplicates(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool()
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        result = runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_text", "job_type": "auto_reply", "text": "respuesta automatica"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_message(_queued_job_payload(queued_job))
        events = store.list_thread_events("acc1:thread-a", limit=10)
        event_types = [event["event_type"] for event in events]
        sent_event = next(event for event in events if event["event_type"] == "sent_auto_reply")

        assert result["queued_jobs"] == 1
        assert browser_pool.send_text_calls == 1
        assert event_types.count("queued_auto_reply") == 1
        assert event_types.count("sent_auto_reply") == 1
        assert "message_sent" not in event_types
        assert "send_failed" not in event_types
        assert sent_event["payload"]["job_type"] == "auto_reply"
        assert sent_event["payload"]["content_kind"] == "text"
        assert sent_event["payload"]["message_id"] == "msg-1"
    finally:
        store.shutdown()


def test_runtime_persists_evaluation_and_enqueue_trace_per_thread(tmp_path: Path, monkeypatch) -> None:
    class _FakeQueueSender:
        def __init__(self) -> None:
            self.enqueue_message_calls: list[dict[str, object]] = []

        def enqueue_message_job(self, thread_key, text, *, job_type, dedupe_key, metadata):
            self.enqueue_message_calls.append(
                {
                    "thread_key": thread_key,
                    "text": text,
                    "job_type": job_type,
                    "dedupe_key": dedupe_key,
                    "metadata": dict(metadata or {}),
                }
            )
            return {
                "ok": True,
                "job_id": 41,
                "created": True,
                "reused": False,
                "dedupe_key": str(dedupe_key or "").strip(),
                "state": "queued",
                "local_message_id": "queued-local-1",
            }

    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        sender = _FakeQueueSender()
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)
        account = {"username": "acc1", "alias": "ventas"}

        monkeypatch.setattr(
            "src.runtime.inbox_automation_runtime.sync_account_threads_from_storage",
            lambda *_args, **_kwargs: [],
        )
        runtime._store.prepare_account_session = lambda *_args, **_kwargs: None
        runtime._store.apply_endpoint_threads = lambda *_args, **_kwargs: ["acc1:thread-a"]
        runtime._connector.start = lambda *_args, **_kwargs: None
        runtime._connector.is_ready = lambda *_args, **_kwargs: True
        runtime._connector.heartbeat = lambda *_args, **_kwargs: None
        monkeypatch.setattr(
            runtime._engine,
            "evaluate_thread",
            lambda *, account, thread, mode: {
                "actions": [{"type": "send_text", "job_type": "auto_reply", "text": "respuesta automatica"}],
                "thread_updates": {},
                "state_updates": {},
                "decision": {"decision": "reply", "reason": "keyword_match"},
            },
        )

        result = runtime.process_account_turn(account, mode="auto")
        events = store.list_thread_events("acc1:thread-a", limit=10)
        started_event = next(event for event in events if event["event_type"] == "automation_evaluate_started")
        completed_event = next(event for event in events if event["event_type"] == "automation_evaluate_completed")
        enqueue_attempt = next(event for event in events if event["event_type"] == "automation_enqueue_attempt")
        enqueue_result = next(event for event in events if event["event_type"] == "automation_enqueue_result")

        assert result["queued_jobs"] == 1
        assert sender.enqueue_message_calls[0]["job_type"] == "auto_reply"
        assert sender.enqueue_message_calls[0]["dedupe_key"].startswith("auto_reply:acc1:thread-a:")
        assert started_event["payload"] == {
            "thread_id": "thread-a",
            "stage_id": "initial",
            "owner": "auto",
            "bucket": "all",
        }
        assert completed_event["payload"] == {
            "decision": "reply",
            "reason": "keyword_match",
            "actions_count": 1,
            "action_types": ["send_text"],
        }
        assert enqueue_attempt["payload"]["action_type"] == "send_text"
        assert enqueue_result["payload"]["attempted"] is True
        assert enqueue_result["payload"]["success"] is True
        assert enqueue_result["payload"]["job_id"] == 41
        assert enqueue_result["payload"]["created"] is True
        assert enqueue_result["payload"]["reused"] is False
    finally:
        store.shutdown()


def test_runtime_does_not_persist_pending_reply_when_enqueue_returns_no_job(tmp_path: Path, monkeypatch) -> None:
    class _RejectingQueueSender:
        def enqueue_message_job(self, thread_key, text, *, job_type, dedupe_key, metadata):
            return {
                "ok": False,
                "job_id": 0,
                "created": False,
                "reused": False,
                "dedupe_key": str(dedupe_key or "").strip(),
                "state": "",
            }

    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        runtime = InboxAutomationRuntime(store=store, sender=_RejectingQueueSender(), ensure_backend_started=lambda: None)
        account = {"username": "acc1", "alias": "ventas"}

        monkeypatch.setattr(
            "src.runtime.inbox_automation_runtime.sync_account_threads_from_storage",
            lambda *_args, **_kwargs: [],
        )
        runtime._store.prepare_account_session = lambda *_args, **_kwargs: None
        runtime._store.apply_endpoint_threads = lambda *_args, **_kwargs: ["acc1:thread-a"]
        runtime._connector.start = lambda *_args, **_kwargs: None
        runtime._connector.is_ready = lambda *_args, **_kwargs: True
        runtime._connector.heartbeat = lambda *_args, **_kwargs: None

        result = runtime.process_account_turn(account, mode="auto")
        thread = store.get_thread("acc1:thread-a") or {}

        assert result["queued_jobs"] == 0
        assert bool(thread.get("pending_reply")) is False
        assert thread.get("pending_inbound_id") in {None, ""}
        assert store.list_send_queue_jobs(states=["queued", "processing"], limit=10) == []
    finally:
        store.shutdown()


def test_runtime_defers_pack_enqueue_when_remaining_quota_cannot_cover_pack(tmp_path: Path, monkeypatch) -> None:
    class _UnexpectedPackQueueSender:
        def enqueue_pack_job(self, *_args, **_kwargs):
            raise AssertionError("pack should not be enqueued when remaining quota cannot cover the full pack")

    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        runtime = InboxAutomationRuntime(store=store, sender=_UnexpectedPackQueueSender(), ensure_backend_started=lambda: None)
        monkeypatch.setattr(
            "src.runtime.inbox_automation_runtime.can_send_message_for_account",
            lambda **_kwargs: (True, 5, 6),
        )
        monkeypatch.setattr(
            "src.runtime.inbox_automation_runtime.datetime",
            type(
                "_FrozenDateTime",
                (),
                {
                    "now": staticmethod(lambda tz=None: datetime(2026, 3, 27, 22, 15, 0, tzinfo=tz)),
                },
            ),
        )

        result = runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas", "messages_per_account": 6},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[
                {
                    "type": "send_pack",
                    "job_type": "auto_reply",
                    "pack_id": "pack-1",
                    "pack_sendable_actions": 3,
                    "latest_inbound_id": "in-1",
                }
            ],
        )

        thread = store.get_thread("acc1:thread-a") or {}
        deferral = dict(thread.get("pack_quota_deferral") or {})
        enqueue_result = next(
            event
            for event in store.list_thread_events("acc1:thread-a", limit=10)
            if event["event_type"] == "automation_enqueue_result"
        )

        assert result["queued_jobs"] == 0
        assert store.list_send_queue_jobs(states=["queued", "processing"], limit=10) == []
        assert bool(thread.get("pending_reply")) is False
        assert thread.get("pending_inbound_id") in {None, ""}
        assert deferral["reason"] == "pack_quota_insufficient:5/6:need=3"
        assert deferral["pack_id"] == "pack-1"
        assert deferral["job_type"] == "auto_reply"
        assert deferral["inbound_id"] == "in-1"
        assert deferral["sendable_actions"] == 3
        assert deferral["sent_today"] == 5
        assert deferral["limit"] == 6
        assert deferral["remaining"] == 1
        assert deferral["retry_after_ts"] > deferral["deferred_at"]
        assert enqueue_result["payload"]["attempted"] is False
        assert enqueue_result["payload"]["success"] is False
        assert enqueue_result["payload"]["reason"] == "pack_quota_insufficient:5/6:need=3"
    finally:
        store.shutdown()


def test_thread_events_record_queued_then_failed_auto_reply_without_legacy_duplicates(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool(send_text_result={"ok": False, "reason": "not_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_text", "job_type": "auto_reply", "text": "respuesta automatica"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_message(_queued_job_payload(queued_job))
        events = store.list_thread_events("acc1:thread-a", limit=10)
        event_types = [event["event_type"] for event in events]
        failed_event = next(event for event in events if event["event_type"] == "failed_auto_reply")

        assert browser_pool.send_text_calls == 1
        assert event_types.count("queued_auto_reply") == 1
        assert event_types.count("failed_auto_reply") == 1
        assert "message_sent" not in event_types
        assert "send_failed" not in event_types
        assert failed_event["payload"]["job_type"] == "auto_reply"
        assert failed_event["payload"]["content_kind"] == "text"
        assert failed_event["payload"]["reason"] == "not_confirmed"
    finally:
        store.shutdown()


def test_thread_events_record_followup_with_explicit_queue_and_send_names(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool(send_text_result={"ok": True, "item_id": "msg-followup", "reason": "thread_read_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_text", "job_type": "followup", "text": "seguimiento"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_message(_queued_job_payload(queued_job))
        events = store.list_thread_events("acc1:thread-a", limit=10)
        event_types = [event["event_type"] for event in events]
        thread = store.get_thread("acc1:thread-a")

        assert browser_pool.send_text_calls == 1
        assert event_types.count("queued_followup") == 1
        assert event_types.count("sent_followup") == 1
        assert "followup_sent" not in event_types
        assert thread is not None
        assert thread["status"] == "followup_sent"
    finally:
        store.shutdown()


def test_runtime_does_not_enqueue_followup_for_qualified_manual_or_closed_threads(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        sender = ConversationSender(store, _FakeDeliveryBrowserPool(), notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)
        thread_key = "acc1:thread-a"
        blocked_cases = [
            {"owner": "auto", "bucket": "qualified", "status": "replied", "manual_lock": False, "manual_assignee": ""},
            {"owner": "manual", "bucket": "qualified", "status": "open", "manual_lock": True, "manual_assignee": "operator-1"},
            {"owner": "none", "bucket": "disqualified", "status": "closed", "manual_lock": False, "manual_assignee": ""},
            {"owner": "auto", "bucket": "all", "status": "closed", "manual_lock": False, "manual_assignee": ""},
        ]

        for updates in blocked_cases:
            store.update_thread_record(
                thread_key,
                {
                    "owner": "auto",
                    "bucket": "all",
                    "status": "open",
                    "manual_lock": False,
                    "manual_assignee": "",
                    **updates,
                },
            )
            result = runtime._apply_actions(
                account={"username": "acc1", "alias": "ventas"},
                thread=store.get_thread(thread_key) or {},
                actions=[{"type": "send_text", "job_type": "followup", "text": "seguimiento"}],
            )

            assert result["queued_jobs"] == 0
            assert store.list_send_queue_jobs(states=["queued", "processing", "confirmed", "cancelled"], limit=10) == []
    finally:
        store.shutdown()


def test_runtime_records_enqueue_trace_when_send_action_is_not_attempted(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        runtime = InboxAutomationRuntime(store=store, sender=SimpleNamespace(), ensure_backend_started=lambda: None)
        thread_key = "acc1:thread-a"
        store.update_thread_record(
            thread_key,
            {
                "owner": "manual",
                "bucket": "qualified",
                "status": "open",
                "manual_lock": True,
                "manual_assignee": "operator-1",
            },
        )

        result = runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            actions=[{"type": "send_text", "job_type": "followup", "text": "seguimiento"}],
        )

        events = store.list_thread_events(thread_key, limit=10)
        enqueue_result = next(event for event in events if event["event_type"] == "automation_enqueue_result")

        assert result["queued_jobs"] == 0
        assert not any(event["event_type"] == "automation_enqueue_attempt" for event in events)
        assert enqueue_result["payload"]["action_type"] == "send_text"
        assert enqueue_result["payload"]["attempted"] is False
        assert enqueue_result["payload"]["success"] is False
        assert enqueue_result["payload"]["reason"] == "followup_not_allowed"
    finally:
        store.shutdown()


def test_sender_cancels_followup_when_thread_becomes_qualified_before_send(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._record_message_sent", lambda *_args, **_kwargs: None)
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool(send_text_result={"ok": True, "item_id": "msg-followup", "reason": "thread_read_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)
        thread_key = "acc1:thread-a"

        result = runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread(thread_key) or {},
            actions=[{"type": "send_text", "job_type": "followup", "text": "seguimiento"}],
        )
        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]

        store.update_thread_record(thread_key, {"bucket": "qualified", "status": "replied"})
        sender._handle_send_message(_queued_job_payload(queued_job))

        cancelled_job = store.list_send_queue_jobs(states=["cancelled"], limit=10)[0]
        events = store.list_thread_events(thread_key, limit=10)
        failed_event = next(event for event in events if event["event_type"] == "failed_followup")

        assert result["queued_jobs"] == 1
        assert browser_pool.send_text_calls == 0
        assert cancelled_job["id"] == queued_job["id"]
        assert failed_event["payload"]["reason"] == "followup_not_allowed"
        assert failed_event["payload"]["cancelled"] is True
    finally:
        store.shutdown()


def test_sender_recovery_requeues_followup_pack_with_full_durable_payload(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        thread_key = "acc1:thread-a"
        dedupe_key = "followup:acc1:thread-a:pack:initial:level:0"
        store.create_send_queue_job(
            "followup",
            thread_key=thread_key,
            account_id="acc1",
            payload={
                "thread_key": thread_key,
                "pack_id": "pack-1",
                "job_type": "followup",
                "post_send_thread_updates": {"stage_id": "initial", "followup_level": 1},
                "post_send_state_updates": {"last_inbound_id_seen": "in-1"},
            },
            dedupe_key=dedupe_key,
        )
        sender = ConversationSender(store, _FakeDeliveryBrowserPool(), notifier=lambda **_kwargs: None)

        sender._recover_jobs()
        task = sender._queue.get_nowait()
        thread = store.get_thread(thread_key)

        assert task.task_type == "followup"
        assert task.payload["pack_id"] == "pack-1"
        assert task.payload["dedupe_key"] == dedupe_key
        assert task.payload["post_send_thread_updates"] == {"stage_id": "initial", "followup_level": 1}
        assert task.payload["post_send_state_updates"] == {"last_inbound_id_seen": "in-1"}
        assert thread is not None
        assert thread["sender_status"] == "queued"
        assert thread["pack_status"] == "queued"
        sender._queue.task_done()
    finally:
        store.shutdown()


def test_cancel_send_queue_jobs_clears_stale_pack_queue_state_when_last_job_is_cancelled(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._list_packs", lambda: [{"id": "pack-1", "name": "Pack Uno"}])
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        thread_key = "acc1:thread-a"
        sender = ConversationSender(store, _FakeDeliveryBrowserPool(), notifier=lambda **_kwargs: None)

        result = sender.enqueue_pack_job(
            thread_key,
            "pack-1",
            job_type="followup",
            dedupe_key="followup:acc1:thread-a:pack:initial:level:0",
            metadata={"post_send_thread_updates": {"stage_id": "initial", "followup_level": 1}},
        )
        queued_before_cancel = store.get_thread(thread_key)
        cancelled = store.cancel_send_queue_jobs(
            thread_key=thread_key,
            job_types=["followup"],
            states=["queued"],
            reason="manual_takeover",
        )
        thread = store.get_thread(thread_key)

        assert result["ok"] is True
        assert queued_before_cancel is not None
        assert queued_before_cancel["sender_status"] == "queued"
        assert queued_before_cancel["pack_status"] == "queued"
        assert cancelled == 1
        assert thread is not None
        assert thread["sender_status"] == "ready"
        assert thread.get("pack_status") in {None, ""}
        assert thread.get("pack_error") in {None, ""}
    finally:
        store.shutdown()


def test_thread_events_record_pack_with_explicit_queue_and_send_names(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("core.responder._list_packs", lambda: [{"id": "pack-1", "name": "Pack Uno"}])
    monkeypatch.setattr("core.responder._flow_config_for_account", lambda _account_id: {})
    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _FakeDeliveryBrowserPool(send_pack_result={"ok": True, "item_id": "pack-msg-1", "reason": "thread_read_confirmed"})
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_pack", "job_type": "auto_reply", "pack_id": "pack-1"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_pack(_queued_job_payload(queued_job))
        events = store.list_thread_events("acc1:thread-a", limit=10)
        event_types = [event["event_type"] for event in events]
        sent_event = next(event for event in events if event["event_type"] == "sent_pack")
        thread = store.get_thread("acc1:thread-a")

        assert browser_pool.send_pack_calls == 1
        assert event_types.count("queued_pack") == 1
        assert event_types.count("sent_pack") == 1
        assert "pack_sent" not in event_types
        assert sent_event["payload"]["job_type"] == "auto_reply"
        assert sent_event["payload"]["content_kind"] == "pack"
        assert sent_event["payload"]["pack_id"] == "pack-1"
        assert thread is not None
        assert thread["status"] == "pack_sent"
    finally:
        store.shutdown()


def test_send_pack_materializes_confirmed_transcript_immediately_from_legacy_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("core.responder._list_packs", lambda: [{"id": "pack-1", "name": "Pack Uno"}])
    monkeypatch.setattr("core.responder._flow_config_for_account", lambda _account_id: {})

    class _LegacyPackBrowserPool(_FakeDeliveryBrowserPool):
        def __init__(self, root_dir: Path) -> None:
            super().__init__(
                send_pack_result={
                    "ok": True,
                    "item_id": "thread-read-confirmed-1",
                    "reason": "thread_read_confirmed",
                    "timestamp": 201.0,
                }
            )
            self._root_dir = root_dir

        def send_pack(self, _thread, _pack, *, conversation_text="", flow_config=None):
            result = super().send_pack(_thread, _pack, conversation_text=conversation_text, flow_config=flow_config)
            storage_dir = storage_root(self._root_dir)
            storage_dir.mkdir(parents=True, exist_ok=True)
            (storage_dir / "message_log.jsonl").write_text(
                "".join(
                    [
                        json.dumps(
                            {
                                "ts": 200.0,
                                "action": "message_sent",
                                "account": "acc1",
                                "thread_id": "thread-a",
                                "lead": "cliente_a",
                                "message_id": "pack-real-1",
                                "message_text": "Primer bloque real",
                            }
                        )
                        + "\n",
                        json.dumps(
                            {
                                "ts": 201.0,
                                "action": "message_sent",
                                "account": "acc1",
                                "thread_id": "thread-a",
                                "lead": "cliente_a",
                                "message_id": "pack-real-2",
                                "message_text": "Segundo bloque real",
                            }
                        )
                        + "\n",
                    ]
                ),
                encoding="utf-8",
            )
            return result

    store = ConversationStore(tmp_path)
    try:
        _seed_runtime_thread(store)
        browser_pool = _LegacyPackBrowserPool(tmp_path)
        sender = ConversationSender(store, browser_pool, notifier=lambda **_kwargs: None)
        runtime = InboxAutomationRuntime(store=store, sender=sender, ensure_backend_started=lambda: None)

        runtime._apply_actions(
            account={"username": "acc1", "alias": "ventas"},
            thread=store.get_thread("acc1:thread-a") or {},
            actions=[{"type": "send_pack", "job_type": "auto_reply", "pack_id": "pack-1"}],
        )

        queued_job = store.list_send_queue_jobs(states=["queued"], limit=10)[0]
        sender._handle_send_pack(_queued_job_payload(queued_job))
        thread = store.get_thread("acc1:thread-a")

        assert thread is not None
        assert browser_pool.send_pack_calls == 1
        assert [row["message_id"] for row in thread["messages"]] == ["pack-real-1", "pack-real-2"]
        assert [row["text"] for row in thread["messages"]] == ["Primer bloque real", "Segundo bloque real"]
        assert thread["last_message_text"] == "Segundo bloque real"
        assert thread["last_message_id"] == "pack-real-2"
        assert thread["last_message_timestamp"] == 201.0
        assert thread["last_outbound_at"] == 201.0
        assert thread["status"] == "pack_sent"
    finally:
        store.shutdown()
