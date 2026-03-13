from __future__ import annotations

from pathlib import Path

from core.inbox.inbox_manager import InboxManager


def _accounts() -> list[dict[str, object]]:
    return [
        {"username": "acc-1", "active": True, "connected": True},
        {"username": "acc-2", "active": True, "connected": True},
        {"username": "acc-3", "active": True, "connected": True},
    ]


def _rows() -> list[dict[str, object]]:
    return [
        {
            "thread_key": "acc-1:thread-1",
            "account_id": "acc-1",
            "last_message_timestamp": 300.0,
            "unread_count": 1,
            "needs_reply": True,
        },
        {
            "thread_key": "acc-2:thread-2",
            "account_id": "acc-2",
            "last_message_timestamp": 250.0,
            "unread_count": 0,
            "needs_reply": False,
        },
    ]


def test_inbox_manager_prioritizes_thread_refresh_before_account_sync(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    queued: list[tuple[str, str, int]] = []

    monkeypatch.setattr(manager, "_active_accounts", _accounts)
    monkeypatch.setattr(manager, "_sync_external_health", lambda accounts: None)
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(manager._storage, "prune_accounts", lambda account_ids: None)
    monkeypatch.setattr(manager._storage, "get_threads", lambda _mode="all": _rows())
    monkeypatch.setattr(
        manager,
        "_enqueue_refresh",
        lambda task_type, payload, priority, dedupe_key="": queued.append(
            (str(task_type), str(payload.get("thread_key") or payload.get("account_id") or ""), int(priority))
        )
        or True,
    )

    manager._schedule_refresh_tasks()

    assert queued[0] == ("read_thread", "acc-1:thread-1", 10)
    assert ("sync_account", "acc-1", 40) in queued


def test_inbox_manager_force_sync_queues_all_healthy_accounts(monkeypatch, tmp_path: Path) -> None:
    manager = InboxManager(tmp_path)
    queued: list[tuple[str, str, int]] = []

    monkeypatch.setattr(manager, "_active_accounts", _accounts)
    monkeypatch.setattr(manager, "_sync_external_health", lambda accounts: None)
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(manager._storage, "prune_accounts", lambda account_ids: None)
    monkeypatch.setattr(manager._storage, "get_threads", lambda _mode="all": [])
    monkeypatch.setattr(
        manager,
        "_enqueue_refresh",
        lambda task_type, payload, priority, dedupe_key="": queued.append(
            (str(task_type), str(payload.get("account_id") or payload.get("thread_key") or ""), int(priority))
        )
        or True,
    )

    manager.enqueue_periodic_sync(force=True)
    manager._schedule_refresh_tasks()

    assert [item[1] for item in queued if item[0] == "sync_account"] == ["acc-1", "acc-2", "acc-3"]
