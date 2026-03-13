from __future__ import annotations

import sqlite3
from pathlib import Path

from core.inbox.inbox_manager import InboxManager
from src.inbox.endpoint_reader import InboxEndpointError


def _seed_thread(manager: InboxManager, *, account_id: str = "acc-1", thread_id: str = "thread-1") -> str:
    thread_key = f"{account_id}:{thread_id}"
    manager.prime_thread_snapshot(
        {
            "thread_key": thread_key,
            "thread_id": thread_id,
            "account_id": account_id,
            "thread_href": f"/direct/t/{thread_id}/",
            "recipient_username": "cliente",
            "display_name": "Cliente",
        },
        messages=[],
    )
    return thread_key


def test_inbox_manager_send_failure_marks_local_message_error(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    thread_key = _seed_thread(manager)
    local = manager._storage.append_local_outbound_message(thread_key, "hola")
    emitted: list[dict[str, object]] = []

    monkeypatch.setattr(manager, "_get_account", lambda account_id: {"username": str(account_id or "")})
    monkeypatch.setattr("core.inbox.inbox_manager.send_manual_message", lambda *_args, **_kwargs: {"ok": False, "reason": "composer_send_failed"})
    monkeypatch.setattr(manager, "_emit_cache_updated", lambda **payload: emitted.append(dict(payload)))

    manager._task_send_message(
        {
            "job_id": 0,
            "thread_key": thread_key,
            "local_message_id": str((local or {}).get("message_id") or ""),
            "text": "hola",
        }
    )

    thread = manager.get_thread(thread_key)
    assert thread is not None
    assert thread["messages"][-1]["delivery_status"] == "error"
    assert thread["thread_error"] == "composer_send_failed"
    assert emitted[-1]["reason"] == "send_message_failed"


def test_inbox_manager_sync_account_updates_runtime_health_from_error(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    emitted: list[dict[str, object]] = []

    monkeypatch.setattr(manager, "_get_account", lambda account_id: {"username": str(account_id or ""), "connected": True})
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(
        "core.inbox.inbox_manager.sync_account_threads_from_storage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(InboxEndpointError("checkpoint", "checkpoint_required")),
    )
    monkeypatch.setattr(manager, "_emit_cache_updated", lambda **payload: emitted.append(dict(payload)))

    manager._task_sync_account({"account_id": "acc-1"})

    health = manager._storage.get_account_health("acc-1")
    assert health["state"] == "checkpoint"
    assert emitted[-1]["reason"] == "sync_account_error"


def test_inbox_manager_mark_follow_up_persists_tag_and_action_memory(tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    thread_key = _seed_thread(manager, thread_id="thread-77")

    assert manager.mark_follow_up(thread_key) is True

    thread = manager.get_thread(thread_key)
    assert thread is not None
    assert "seguimiento" in thread["tags"]

    conn = sqlite3.connect(manager._storage._database_path)
    try:
        row = conn.execute(
            """
            SELECT action_type, source
            FROM thread_action_memory
            WHERE thread_id = ? AND account_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("thread-77", "acc-1"),
        ).fetchone()
    finally:
        conn.close()

    assert row == ("follow_up_tag_added", "inbox_rm")


def test_inbox_manager_sync_account_seeds_preview_messages_for_fast_open(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)

    monkeypatch.setattr(manager, "_get_account", lambda account_id: {"username": str(account_id or ""), "connected": False})
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(
        "core.inbox.inbox_manager.sync_account_threads_from_storage",
        lambda *_args, **_kwargs: [
            {
                "thread_key": "acc-1:thread-1",
                "thread_id": "thread-1",
                "thread_href": "https://www.instagram.com/direct/t/thread-1/",
                "account_id": "acc-1",
                "recipient_username": "cliente",
                "display_name": "Cliente",
                "participants": ["cliente"],
                "last_message_text": "Hola",
                "last_message_timestamp": 100.0,
                "last_message_direction": "inbound",
                "unread_count": 1,
                "preview_messages": [
                    {
                        "message_id": "msg-1",
                        "text": "Hola",
                        "timestamp": 100.0,
                        "direction": "inbound",
                    },
                    {
                        "message_id": "msg-2",
                        "text": "Te respondo",
                        "timestamp": 110.0,
                        "direction": "outbound",
                    },
                ],
            }
        ],
    )

    manager._task_sync_account({"account_id": "acc-1"})

    thread = manager.get_thread("acc-1:thread-1")
    assert thread is not None
    assert [row["message_id"] for row in thread["messages"]] == ["msg-1", "msg-2"]
    assert thread["last_message_id"] == "msg-2"
    assert thread["account_health"] == "healthy"


def test_inbox_manager_read_thread_uses_endpoint_reader_and_marks_ready(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    thread_key = _seed_thread(manager)
    emitted: list[dict[str, object]] = []

    monkeypatch.setattr(manager, "_get_account", lambda account_id: {"username": str(account_id or ""), "connected": True})
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(
        "core.inbox.inbox_manager.read_thread_from_storage",
        lambda *_args, **_kwargs: {
            "messages": [
                {
                    "message_id": "msg-1",
                    "text": "Hola",
                    "timestamp": 100.0,
                    "direction": "inbound",
                },
                {
                    "message_id": "msg-2",
                    "text": "Te respondo",
                    "timestamp": 110.0,
                    "direction": "outbound",
                },
            ],
            "participants": ["cliente"],
            "seen_text": "",
            "seen_at": None,
        },
    )
    monkeypatch.setattr(manager, "_emit_cache_updated", lambda **payload: emitted.append(dict(payload)))

    manager._task_read_thread({"thread_key": thread_key})

    thread = manager.get_thread(thread_key)
    assert thread is not None
    assert [row["message_id"] for row in thread["messages"]] == ["msg-1", "msg-2"]
    assert thread["thread_status"] == "ready"
    assert emitted[-1]["reason"] == "read_thread"


def test_inbox_manager_treats_profile_ready_accounts_without_error_badge_as_healthy(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)

    monkeypatch.setattr("core.inbox.inbox_manager.health_store.get_badge", lambda _account_id: ("", False))
    monkeypatch.setattr(manager, "_account_profile_ready", lambda _account_id: True)

    state, reason = manager._map_badge_to_health({"username": "acc-1", "connected": False})

    assert state == "healthy"
    assert reason == ""


def test_inbox_manager_requeues_pending_send_message_job_after_restart(monkeypatch, tmp_path: Path) -> None:
    first = InboxManager(tmp_path)
    thread_key = _seed_thread(first)
    local = first._storage.append_local_outbound_message(thread_key, "hola")
    job_id = first._storage.create_send_queue_job(
        "send_message",
        thread_key=thread_key,
        account_id="acc-1",
        payload={
            "thread_key": thread_key,
            "text": "hola",
            "local_message_id": str((local or {}).get("message_id") or ""),
        },
    )
    first.shutdown()

    recovered = InboxManager(tmp_path)
    queued: list[dict[str, object]] = []
    monkeypatch.setattr(
        recovered,
        "_enqueue_outbound",
        lambda task_type, payload, priority, dedupe_key="": queued.append(
            {
                "task_type": str(task_type),
                "payload": dict(payload),
                "priority": int(priority),
                "dedupe_key": str(dedupe_key or ""),
            }
        )
        or True,
    )

    recovered._recover_persisted_outbound_jobs()

    jobs = recovered._storage.list_send_queue_jobs(states=["pending", "sending"], limit=20)
    thread = recovered.get_thread(thread_key)

    assert len(queued) == 1
    assert queued[0]["task_type"] == "send_message"
    assert queued[0]["payload"]["job_id"] == job_id
    assert jobs[0]["state"] == "pending"
    assert thread is not None
    assert thread["messages"][-1]["delivery_status"] == "pending"


def test_inbox_manager_marks_stale_sending_message_as_sent_when_reconciled(monkeypatch, tmp_path: Path) -> None:
    first = InboxManager(tmp_path)
    thread_key = _seed_thread(first)
    local = first._storage.append_local_outbound_message(thread_key, "hola")
    local_id = str((local or {}).get("message_id") or "")
    job_id = first._storage.create_send_queue_job(
        "send_message",
        thread_key=thread_key,
        account_id="acc-1",
        payload={
            "thread_key": thread_key,
            "text": "hola",
            "local_message_id": local_id,
        },
    )
    first._storage.set_local_outbound_status(thread_key, local_id, status="sending")
    first._storage.update_send_queue_job(job_id, state="sending")
    first.shutdown()

    recovered = InboxManager(tmp_path)
    emitted: list[dict[str, object]] = []
    queued: list[dict[str, object]] = []
    monkeypatch.setattr(recovered, "_get_account", lambda account_id: {"username": str(account_id or "")})
    monkeypatch.setattr(
        "src.inbox.message_sender.reconcile_manual_message",
        lambda *_args, **_kwargs: {
            "ok": True,
            "message_id": "msg-real",
            "timestamp": 150.0,
            "reason": "thread_read_confirmed",
        },
    )
    monkeypatch.setattr(recovered, "_emit_cache_updated", lambda **payload: emitted.append(dict(payload)))
    monkeypatch.setattr(
        recovered,
        "_enqueue_outbound",
        lambda task_type, payload, priority, dedupe_key="": queued.append(dict(payload)) or True,
    )

    recovered._recover_persisted_outbound_jobs()

    jobs = recovered._storage.list_send_queue_jobs(states=["sent"], limit=20)
    thread = recovered.get_thread(thread_key)

    assert queued == []
    assert jobs and jobs[0]["state"] == "sent"
    assert thread is not None
    assert thread["messages"][-1]["message_id"] == "msg-real"
    assert thread["messages"][-1]["delivery_status"] == "sent"
    assert emitted[-1]["reason"] == "send_message_recovered"


def test_inbox_manager_requeues_unconfirmed_stale_sending_message(monkeypatch, tmp_path: Path) -> None:
    first = InboxManager(tmp_path)
    thread_key = _seed_thread(first)
    local = first._storage.append_local_outbound_message(thread_key, "hola")
    local_id = str((local or {}).get("message_id") or "")
    job_id = first._storage.create_send_queue_job(
        "send_message",
        thread_key=thread_key,
        account_id="acc-1",
        payload={
            "thread_key": thread_key,
            "text": "hola",
            "local_message_id": local_id,
        },
    )
    first._storage.set_local_outbound_status(thread_key, local_id, status="sending")
    first._storage.update_send_queue_job(job_id, state="sending")
    first.shutdown()

    recovered = InboxManager(tmp_path)
    emitted: list[dict[str, object]] = []
    queued: list[dict[str, object]] = []
    monkeypatch.setattr(recovered, "_get_account", lambda account_id: {"username": str(account_id or "")})
    monkeypatch.setattr(
        "src.inbox.message_sender.reconcile_manual_message",
        lambda *_args, **_kwargs: {"ok": False, "reason": "thread_read_unconfirmed"},
    )
    monkeypatch.setattr(recovered, "_emit_cache_updated", lambda **payload: emitted.append(dict(payload)))
    monkeypatch.setattr(
        recovered,
        "_enqueue_outbound",
        lambda task_type, payload, priority, dedupe_key="": queued.append(
            {
                "task_type": str(task_type),
                "payload": dict(payload),
            }
        )
        or True,
    )

    recovered._recover_persisted_outbound_jobs()

    jobs = recovered._storage.list_send_queue_jobs(states=["pending"], limit=20)
    thread = recovered.get_thread(thread_key)

    assert len(queued) == 1
    assert queued[0]["task_type"] == "send_message"
    assert queued[0]["payload"]["job_id"] == job_id
    assert jobs and jobs[0]["state"] == "pending"
    assert thread is not None
    assert thread["messages"][-1]["delivery_status"] == "pending"
    assert emitted[-1]["reason"] == "send_message_requeued"
