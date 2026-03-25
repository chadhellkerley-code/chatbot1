from __future__ import annotations

from pathlib import Path

from core.inbox.inbox_manager import InboxManager


def test_inbox_manager_read_thread_keeps_historical_messages_visible(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manager = InboxManager(tmp_path)
    thread_key = "acc-1:thread-1"
    manager._store.prepare_account_session("acc-1", session_marker="session-1", started_at=150.0)
    manager.prime_thread_snapshot(
        {
            "thread_key": thread_key,
            "thread_id": "thread-1",
            "thread_href": "https://www.instagram.com/direct/t/thread-1/",
            "account_id": "acc-1",
            "recipient_username": "cliente",
            "display_name": "Cliente",
        },
        messages=[],
    )

    monkeypatch.setattr(manager, "_get_account", lambda account_id: {"username": str(account_id or ""), "connected": True})
    monkeypatch.setattr(manager, "_account_can_refresh", lambda _account_id: True)
    monkeypatch.setattr(
        "core.inbox.inbox_manager.read_thread_from_storage",
        lambda *_args, **_kwargs: {
            "messages": [
                {
                    "message_id": "msg-1",
                    "text": "historial previo",
                    "timestamp": 100.0,
                    "direction": "inbound",
                },
                {
                    "message_id": "msg-2",
                    "text": "mensaje nuevo",
                    "timestamp": 160.0,
                    "direction": "inbound",
                },
            ],
            "participants": ["cliente"],
            "seen_text": "",
            "seen_at": None,
        },
    )

    manager._task_read_thread({"thread_key": thread_key})

    thread = manager.get_thread(thread_key)
    assert thread is not None
    assert [row["message_id"] for row in thread["messages"]] == ["msg-2"]
    assert thread["messages"][0]["text"] == "historial previo\nmensaje nuevo"
    assert thread["thread_status"] == "ready"
