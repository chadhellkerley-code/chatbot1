from __future__ import annotations

import json
from pathlib import Path

from core.inbox.conversation_store import ConversationStore


def _endpoint_row(*, timestamp: float) -> dict[str, object]:
    return {
        "thread_key": "acc1:thread-a",
        "thread_id": "thread-a",
        "thread_href": "https://www.instagram.com/direct/t/thread-a/",
        "account_id": "acc1",
        "account_alias": "ventas",
        "recipient_username": "cliente_a",
        "display_name": "Cliente A",
        "last_message_text": "Hola",
        "last_message_timestamp": timestamp,
        "last_message_direction": "inbound",
        "unread_count": 1,
        "participants": ["cliente_a"],
        "preview_messages": [
            {
                "message_id": f"msg-{int(timestamp)}",
                "text": "Hola",
                "timestamp": timestamp,
                "direction": "inbound",
            }
        ],
    }


def _outbound_row(*, thread_id: str, timestamp: float) -> dict[str, object]:
    return {
        "thread_key": f"acc1:{thread_id}",
        "thread_id": thread_id,
        "thread_href": f"https://www.instagram.com/direct/t/{thread_id}/",
        "account_id": "acc1",
        "account_alias": "ventas",
        "recipient_username": f"{thread_id}_user",
        "display_name": thread_id.title(),
        "last_message_text": "Pack enviado",
        "last_message_timestamp": timestamp,
        "last_message_direction": "outbound",
        "unread_count": 0,
        "participants": [f"{thread_id}_user"],
        "preview_messages": [
            {
                "message_id": f"msg-{thread_id}-{int(timestamp)}",
                "text": "Pack enviado",
                "timestamp": timestamp,
                "direction": "outbound",
            }
        ],
    }


def _write_conversation_engine(tmp_path: Path, conversations: dict[str, object]) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    (storage_dir / "conversation_engine.json").write_text(
        json.dumps({"conversations": conversations}),
        encoding="utf-8",
    )


def test_conversation_store_does_not_recreate_deleted_thread_without_newer_activity(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=150.0)])
        assert store.delete_conversation("acc1:thread-a") is True

        touched = store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=150.0)])
        assert touched == []
        assert store.get_thread("acc1:thread-a") is None

        touched = store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=151.0)])
        thread = store.get_thread("acc1:thread-a")

        assert touched == ["acc1:thread-a"]
        assert thread is not None
        assert thread["last_message_timestamp"] == 151.0
    finally:
        store.shutdown()


def test_conversation_store_lists_only_crm_relevant_threads(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads(
            {"username": "acc1"},
            [
                _endpoint_row(timestamp=150.0),
                _outbound_row(thread_id="thread-b", timestamp=160.0),
            ],
        )

        rows = store.list_threads("all")

        assert touched == ["acc1:thread-a"]
        assert [row["thread_key"] for row in rows] == ["acc1:thread-a"]
    finally:
        store.shutdown()


def test_conversation_store_keeps_thread_visible_after_reply_is_detected(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=150.0)])
        assert touched == ["acc1:thread-a"]

        touched = store.apply_endpoint_threads(
            {"username": "acc1"},
            [_outbound_row(thread_id="thread-a", timestamp=180.0)],
        )
        thread = store.get_thread("acc1:thread-a")
        rows = store.list_threads("all")

        assert touched == ["acc1:thread-a"]
        assert thread is not None
        assert thread["last_message_timestamp"] == 180.0
        assert thread["last_message_direction"] == "outbound"
        assert [row["thread_key"] for row in rows] == ["acc1:thread-a"]
    finally:
        store.shutdown()


def test_conversation_store_keeps_pack_threads_visible_without_inbound_reply(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)
        store.record_action_memory("thread-pack", "acc1", "manual_pack_sent", pack_id="pack-1")

        touched = store.apply_endpoint_threads(
            {"username": "acc1"},
            [_outbound_row(thread_id="thread-pack", timestamp=170.0)],
        )

        rows = store.list_threads("all")

        assert touched == ["acc1:thread-pack"]
        assert [row["thread_key"] for row in rows] == ["acc1:thread-pack"]
    finally:
        store.shutdown()


def test_conversation_store_surfaces_recent_legacy_reply_without_endpoint_row(tmp_path: Path) -> None:
    _write_conversation_engine(
        tmp_path,
        {
            "acc1|thread-legacy": {
                "account": "acc1",
                "thread_id": "thread-legacy",
                "thread_id_real": "thread-legacy",
                "thread_href": "https://www.instagram.com/direct/t/thread-legacy/",
                "recipient_username": "legacy_reply_user",
                "title": "Legacy Reply User",
                "snippet": "Te respondieron recien",
                "last_message_sent_at": 140.0,
                "last_message_received_at": 155.0,
                "last_message_sender": "lead",
            }
        },
    )
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads({"username": "acc1", "alias": "ventas"}, [])
        rows = store.list_threads("all")
        thread = store.get_thread("acc1:thread-legacy")

        assert touched == ["acc1:thread-legacy"]
        assert [row["thread_key"] for row in rows] == ["acc1:thread-legacy"]
        assert thread is not None
        assert thread["reply_detected_at"] == 155.0
        assert thread["last_message_timestamp"] == 155.0
        assert thread["last_message_direction"] == "inbound"
    finally:
        store.shutdown()


def test_conversation_store_surfaces_recent_legacy_pack_when_endpoint_is_outdated(tmp_path: Path) -> None:
    _write_conversation_engine(
        tmp_path,
        {
            "acc1|thread-pack": {
                "account": "acc1",
                "thread_id": "thread-pack",
                "thread_id_real": "thread-pack",
                "thread_href": "https://www.instagram.com/direct/t/thread-pack/",
                "recipient_username": "thread-pack_user",
                "title": "Thread Pack",
                "last_message_sent_at": 185.0,
                "last_message_received_at": 120.0,
                "last_message_sender": "bot",
                "pending_pack_run": {
                    "pack_id": "pack-xyz",
                    "pack_name": "Pack Legacy",
                },
                "flow_state": {
                    "outbox": {
                        "thread-pack:pack-xyz:0": {
                            "status": "sent",
                            "sent_at": 185.0,
                        }
                    }
                },
            }
        },
    )
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads(
            {"username": "acc1", "alias": "ventas"},
            [_outbound_row(thread_id="thread-pack", timestamp=110.0)],
        )
        rows = store.list_threads("all")
        thread = store.get_thread("acc1:thread-pack")

        assert touched == ["acc1:thread-pack"]
        assert [row["thread_key"] for row in rows] == ["acc1:thread-pack"]
        assert thread is not None
        assert thread["pack_sent_at"] == 185.0
        assert thread["pack_name"] == "Pack Legacy"
        assert thread["last_message_timestamp"] == 185.0
        assert thread["status"] == "pack_sent"
    finally:
        store.shutdown()


def test_conversation_store_ignores_legacy_activity_before_session_start(tmp_path: Path) -> None:
    _write_conversation_engine(
        tmp_path,
        {
            "acc1|thread-old": {
                "account": "acc1",
                "thread_id": "thread-old",
                "thread_id_real": "thread-old",
                "recipient_username": "old_user",
                "title": "Old User",
                "last_message_received_at": 150.0,
                "last_message_sender": "lead",
            }
        },
    )
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=200.0)

        touched = store.apply_endpoint_threads({"username": "acc1", "alias": "ventas"}, [])

        assert touched == []
        assert store.list_threads("all") == []
    finally:
        store.shutdown()


def test_conversation_store_prunes_stale_threads_when_account_snapshot_has_no_relevant_activity(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=150.0)])
        assert touched == ["acc1:thread-a"]
        assert [row["thread_key"] for row in store.list_threads("all")] == ["acc1:thread-a"]

        touched = store.apply_endpoint_threads({"username": "acc1"}, [])

        assert touched == []
        assert store.list_threads("all") == []
        assert store.get_thread("acc1:thread-a") is None
    finally:
        store.shutdown()
