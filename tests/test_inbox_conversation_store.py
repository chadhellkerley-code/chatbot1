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


def _preview_inbound_outbound_row(*, thread_id: str, timestamp: float, last_activity_at: float | None = None) -> dict[str, object]:
    row = {
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
        "latest_customer_message_at": None,
        "preview_messages": [
            {
                "message_id": f"msg-{thread_id}-inbound",
                "text": "Hola, vi el pack",
                "timestamp": None,
                "direction": "inbound",
            },
            {
                "message_id": f"msg-{thread_id}-outbound",
                "text": "Pack enviado",
                "timestamp": timestamp,
                "direction": "outbound",
            },
        ],
    }
    if last_activity_at is not None:
        row["last_activity_at"] = last_activity_at
    return row


def _write_conversation_engine(tmp_path: Path, conversations: dict[str, object]) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    (storage_dir / "conversation_engine.json").write_text(
        json.dumps({"conversations": conversations}),
        encoding="utf-8",
    )


def _write_message_log(tmp_path: Path, rows: list[dict[str, object]]) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(row) for row in rows)
    if payload:
        payload += "\n"
    (storage_dir / "message_log.jsonl").write_text(payload, encoding="utf-8")


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


def test_conversation_store_treats_preview_inbound_as_crm_relevant(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        relevant = store._is_crm_relevant_row(
            _preview_inbound_outbound_row(thread_id="thread-preview", timestamp=180.0),
            account_id="acc1",
            legacy_messages=[],
            session_started_at=None,
        )

        assert relevant is True
    finally:
        store.shutdown()


def test_conversation_store_accepts_preview_inbound_thread_without_customer_timestamp(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        store.prepare_account_session("acc1", session_marker="session-1", started_at=100.0)

        touched = store.apply_endpoint_threads(
            {"username": "acc1"},
            [_preview_inbound_outbound_row(thread_id="thread-preview", timestamp=180.0, last_activity_at=175.0)],
        )
        thread = store.get_thread("acc1:thread-preview")
        rows = store.list_threads("all")

        assert touched == ["acc1:thread-preview"]
        assert [row["thread_key"] for row in rows] == ["acc1:thread-preview"]
        assert thread is not None
        assert thread["latest_customer_message_at"] == 175.0
    finally:
        store.shutdown()


def test_conversation_store_does_not_invent_preview_outbound_timestamp_from_thread_snapshot(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        touched = store.apply_endpoint_threads(
            {"username": "acc1"},
            [
                {
                    "thread_key": "acc1:thread-preview-out",
                    "thread_id": "thread-preview-out",
                    "thread_href": "https://www.instagram.com/direct/t/thread-preview-out/",
                    "account_id": "acc1",
                    "account_alias": "ventas",
                    "recipient_username": "preview_user",
                    "display_name": "Preview User",
                    "last_message_text": "Pack enviado",
                    "last_message_timestamp": 180.0,
                    "last_message_direction": "outbound",
                    "unread_count": 0,
                    "pack_sent_at": 180.0,
                    "participants": ["preview_user"],
                    "preview_messages": [
                        {
                            "message_id": "preview-outbound",
                            "text": "Pack enviado",
                            "timestamp": None,
                            "direction": "outbound",
                        }
                    ],
                }
            ],
        )
        thread = store.get_thread("acc1:thread-preview-out")

        assert touched == ["acc1:thread-preview-out"]
        assert thread is not None
        assert thread["messages"][-1]["timestamp"] is None
        assert thread["last_message_timestamp"] == 180.0
    finally:
        store.shutdown()


def test_conversation_store_separates_operational_status_from_ui_status(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        touched = store.apply_endpoint_threads({"username": "acc1"}, [_endpoint_row(timestamp=150.0)])
        thread = store.get_thread("acc1:thread-a")

        assert touched == ["acc1:thread-a"]
        assert thread is not None
        assert thread["status"] == "open"
        assert thread["operational_status"] == "open"
        assert thread["ui_status"] == "needs_reply"
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
        assert thread["status"] == "open"
        assert thread["operational_status"] == "open"
        assert thread["ui_status"] == "pack_sent"
    finally:
        store.shutdown()


def test_conversation_store_pack_placeholder_does_not_override_real_message_timestamp(tmp_path: Path) -> None:
    store = ConversationStore(tmp_path)
    try:
        base_row = _outbound_row(thread_id="thread-pack-placeholder", timestamp=170.0)
        store.upsert_threads([base_row])

        thread_key = store.ensure_conversation_from_pack(
            account={"username": "acc1"},
            thread_row=base_row,
            pack_name="Pack Placeholder",
        )
        thread = store.get_thread(thread_key)

        assert thread_key == "acc1:thread-pack-placeholder"
        assert thread is not None
        assert thread["last_message_timestamp"] == 170.0
        assert thread["last_message_text"] == "Pack enviado"
        assert thread.get("pack_name") == "Pack Placeholder"
    finally:
        store.shutdown()


def test_conversation_store_refresh_thread_from_legacy_materializes_confirmed_pack_messages_without_duplicates(
    tmp_path: Path,
) -> None:
    _write_message_log(
        tmp_path,
        [
            {
                "ts": 180.0,
                "action": "message_sent",
                "account": "acc1",
                "thread_id": "thread-pack-live",
                "lead": "thread-pack-live_user",
                "message_id": "pack-msg-1",
                "message_text": "Primer bloque real",
            },
            {
                "ts": 181.0,
                "action": "message_sent",
                "account": "acc1",
                "thread_id": "thread-pack-live",
                "lead": "thread-pack-live_user",
                "message_id": "pack-msg-2",
                "message_text": "Segundo bloque real",
            },
        ],
    )
    store = ConversationStore(tmp_path)
    try:
        store.upsert_threads([_outbound_row(thread_id="thread-pack-live", timestamp=170.0)])

        refreshed = store.refresh_thread_from_legacy("acc1:thread-pack-live")
        thread = store.get_thread("acc1:thread-pack-live")

        assert refreshed is True
        assert thread is not None
        assert [row["message_id"] for row in thread["messages"]] == ["pack-msg-1", "pack-msg-2"]
        assert [row["text"] for row in thread["messages"]] == ["Primer bloque real", "Segundo bloque real"]
        assert thread["last_message_text"] == "Segundo bloque real"
        assert thread["last_message_id"] == "pack-msg-2"
        assert thread["last_message_timestamp"] == 181.0

        store.replace_messages(
            "acc1:thread-pack-live",
            [
                {
                    "message_id": "pack-msg-1",
                    "text": "Primer bloque real",
                    "timestamp": 180.0,
                    "direction": "outbound",
                    "delivery_status": "sent",
                },
                {
                    "message_id": "pack-msg-2",
                    "text": "Segundo bloque real",
                    "timestamp": 181.0,
                    "direction": "outbound",
                    "delivery_status": "sent",
                },
            ],
        )
        resynced = store.get_thread("acc1:thread-pack-live")

        assert resynced is not None
        assert [row["message_id"] for row in resynced["messages"]] == ["pack-msg-1", "pack-msg-2"]
        assert [row["text"] for row in resynced["messages"]] == ["Primer bloque real", "Segundo bloque real"]
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
