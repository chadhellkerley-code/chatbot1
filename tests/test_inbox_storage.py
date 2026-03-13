from __future__ import annotations
import contextlib
from pathlib import Path

from core.storage_atomic import atomic_write_json
from src.inbox.inbox_storage import InboxStorage


def _thread_row(
    account_id: str,
    thread_id: str,
    *,
    timestamp: float,
    direction: str = "inbound",
    unread_count: int = 1,
) -> dict[str, object]:
    return {
        "thread_key": f"{account_id}:{thread_id}",
        "thread_id": thread_id,
        "thread_href": f"https://www.instagram.com/direct/t/{thread_id}/",
        "account_id": account_id,
        "account_alias": "ventas",
        "recipient_username": f"{thread_id}_user",
        "display_name": thread_id.title(),
        "last_message_text": f"Mensaje {thread_id}",
        "last_message_timestamp": timestamp,
        "last_message_direction": direction,
        "unread_count": unread_count,
    }


def test_inbox_storage_filters_and_local_echo(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    "thread_key": "acc1:thread-a",
                    "thread_id": "thread-a",
                    "thread_href": "https://www.instagram.com/direct/t/thread-a/",
                    "account_id": "acc1",
                    "account_alias": "ventas",
                    "recipient_username": "cliente_a",
                    "display_name": "Cliente A",
                    "last_message_text": "Hola",
                    "last_message_timestamp": 100.0,
                    "last_message_direction": "inbound",
                    "unread_count": 2,
                },
                {
                    "thread_key": "acc2:thread-b",
                    "thread_id": "thread-b",
                    "thread_href": "https://www.instagram.com/direct/t/thread-b/",
                    "account_id": "acc2",
                    "account_alias": "soporte",
                    "recipient_username": "cliente_b",
                    "display_name": "Cliente B",
                    "last_message_text": "Todo bien",
                    "last_message_timestamp": 50.0,
                    "last_message_direction": "outbound",
                    "unread_count": 0,
                },
            ]
        )

        assert [row["thread_key"] for row in storage.get_threads("all")] == [
            "acc1:thread-a",
            "acc2:thread-b",
        ]
        assert [row["thread_key"] for row in storage.get_threads("unread")] == ["acc1:thread-a"]
        assert [row["thread_key"] for row in storage.get_threads("pending")] == ["acc1:thread-a"]

        local = storage.append_local_outbound_message("acc1:thread-a", "Te respondo ahora")
        assert local is not None
        thread = storage.get_thread("acc1:thread-a")
        assert thread is not None
        assert thread["last_message_direction"] == "outbound"
        assert thread["messages"][-1]["delivery_status"] == "pending"

        storage.resolve_local_outbound(
            "acc1:thread-a",
            str(local["message_id"]),
            final_message_id="real-msg-1",
            sent_timestamp=120.0,
        )
        resolved = storage.get_thread("acc1:thread-a")
        assert resolved is not None
        assert resolved["messages"][-1]["message_id"] == "real-msg-1"
        assert resolved["messages"][-1]["delivery_status"] == "sent"
    finally:
        storage.shutdown()


def test_inbox_storage_keeps_synced_activity_even_if_it_predates_session_start(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.prepare_account_session("acc1", session_marker="monitor-1", started_at=100.0)

        storage.upsert_threads(
            [
                _thread_row("acc1", "thread-old", timestamp=90.0),
                _thread_row("acc1", "thread-new", timestamp=110.0),
            ]
        )

        assert [row["thread_key"] for row in storage.get_threads("all")] == [
            "acc1:thread-new",
            "acc1:thread-old",
        ]

        storage.replace_messages(
            "acc1:thread-new",
            [
                {
                    "message_id": "msg-1",
                    "text": "antes de conectar",
                    "timestamp": 95.0,
                    "direction": "inbound",
                },
                {
                    "message_id": "msg-2",
                    "text": "nuevo inbound",
                    "timestamp": 111.0,
                    "direction": "inbound",
                },
                {
                    "message_id": "msg-3",
                    "text": "respuesta",
                    "timestamp": 112.0,
                    "direction": "outbound",
                },
            ],
            mark_read=True,
        )

        thread = storage.get_thread("acc1:thread-new")
        assert thread is not None
        assert [row["message_id"] for row in thread["messages"]] == ["msg-2", "msg-3"]
        assert thread["messages"][0]["text"] == "antes de conectar\nnuevo inbound"
        assert thread["unread_count"] == 0

        storage.prepare_account_session("acc1", session_marker="monitor-2", started_at=200.0)

        assert storage.get_threads("all") == []
        assert storage.get_thread("acc1:thread-new") is None
    finally:
        storage.shutdown()


def test_inbox_storage_backdates_existing_session_start_when_requested(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        first_started_at = storage.prepare_account_session("acc1", session_marker="monitor-1", started_at=200.0)
        second_started_at = storage.prepare_account_session("acc1", session_marker="monitor-1", started_at=150.0)

        assert first_started_at == 200.0
        assert second_started_at == 150.0
        assert storage.account_session_started_at("acc1") == 150.0
    finally:
        storage.shutdown()


def test_inbox_storage_recovers_partial_thread_identity_from_disk(tmp_path: Path) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        storage_dir / InboxStorage.THREADS_FILE,
        {
            "threads": {
                "matidiazlife:1982656739802039": {
                    "last_message_text": "hola",
                    "last_message_timestamp": 100.0,
                    "last_message_direction": "outbound",
                    "last_message_id": "msg-1",
                    "participants": ["nicosaenzcore"],
                }
            }
        },
    )

    storage = InboxStorage(tmp_path)
    thread = storage.get_thread("matidiazlife:1982656739802039")

    assert thread is not None
    assert thread["account_id"] == "matidiazlife"
    assert thread["thread_id"] == "1982656739802039"
    assert thread["recipient_username"] == "nicosaenzcore"
    assert thread["display_name"] == "nicosaenzcore"


def test_inbox_storage_caps_visible_threads_per_account(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.prepare_account_session("acc1", session_marker="monitor-1", started_at=1.0)
        total_threads = storage._MAX_THREADS_PER_ACCOUNT + 5

        storage.upsert_threads(
            [
                _thread_row("acc1", f"thread-{index:03d}", timestamp=float(index))
                for index in range(1, total_threads + 1)
            ]
        )

        rows = storage.get_threads("all")
        assert len(rows) == storage._MAX_THREADS_PER_ACCOUNT
        assert rows[0]["thread_key"] == f"acc1:thread-{total_threads:03d}"
        assert rows[-1]["thread_key"] == "acc1:thread-006"
        assert not any(row["thread_key"] == "acc1:thread-005" for row in rows)
    finally:
        storage.shutdown()


def test_inbox_storage_persists_sqlite_state_and_action_memory(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    reopened = None
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        local = storage.append_local_outbound_message("acc1:thread-a", "hola")
        assert local is not None
        storage.set_local_outbound_status("acc1:thread-a", str(local["message_id"]), status="sending")
        storage.resolve_local_outbound("acc1:thread-a", str(local["message_id"]), final_message_id="msg-1", sent_timestamp=101.0)
        storage.update_thread_state("acc1:thread-a", {"suggestion_status": "ready"})
        storage.set_account_health("acc1", "checkpoint", reason="challenge")
        storage.record_action_memory("thread-a", "acc1", "manual_reply_sent", source="inbox_rm")
        storage.flush()
        storage.shutdown()

        reopened = InboxStorage(tmp_path)
        thread = reopened.get_thread("acc1:thread-a")
        assert thread is not None
        assert thread["messages"][-1]["message_id"] == "msg-1"
        assert thread["account_health"] == "checkpoint"
        assert thread["account_health_reason"] == "challenge"
        snapshot = reopened.snapshot()
        assert snapshot["state"]["accounts"]["acc1"]["health_state"] == "checkpoint"
    finally:
        with contextlib.suppress(Exception):
            if reopened is not None:
                reopened.shutdown()


def test_inbox_storage_orders_threads_by_last_activity_timestamp(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                _thread_row("acc1", "thread-a", timestamp=100.0),
                _thread_row("acc1", "thread-b", timestamp=200.0),
            ]
        )
        storage.update_thread_state("acc1:thread-a", {"last_activity_timestamp": 999.0})

        rows = storage.get_threads("all")

        assert [row["thread_key"] for row in rows] == [
            "acc1:thread-a",
            "acc1:thread-b",
        ]
    finally:
        storage.shutdown()


def test_inbox_storage_deleted_thread_requires_newer_activity_to_recreate(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.update_thread_state("acc1:thread-a", {"last_activity_timestamp": 150.0})

        assert storage.delete_thread("acc1:thread-a") is True
        assert storage.allow_deleted_thread_recreate("acc1:thread-a", last_activity_timestamp=150.0) is False
        assert storage.allow_deleted_thread_recreate("acc1:thread-a", last_activity_timestamp=149.0) is False
        assert storage.allow_deleted_thread_recreate("acc1:thread-a", last_activity_timestamp=151.0) is True
        assert storage.allow_deleted_thread_recreate("acc1:thread-a", last_activity_timestamp=151.0) is True
    finally:
        storage.shutdown()


def test_inbox_storage_seeds_preview_messages_without_dropping_cached_detail(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.replace_messages(
            "acc1:thread-a",
            [
                {
                    "message_id": "msg-1",
                    "text": "hola",
                    "timestamp": 100.0,
                    "direction": "inbound",
                },
                {
                    "message_id": "msg-2",
                    "text": "respuesta inicial",
                    "timestamp": 110.0,
                    "direction": "outbound",
                },
            ],
            participants=["cliente_a"],
            mark_read=True,
        )

        storage.seed_messages(
            "acc1:thread-a",
            [
                {
                    "message_id": "msg-2",
                    "text": "respuesta inicial",
                    "timestamp": 110.0,
                    "direction": "outbound",
                },
                {
                    "message_id": "msg-3",
                    "text": "nuevo inbound",
                    "timestamp": 120.0,
                    "direction": "inbound",
                },
            ],
            participants=["cliente_a"],
        )

        thread = storage.get_thread("acc1:thread-a")
        assert thread is not None
        assert [row["message_id"] for row in thread["messages"]] == ["msg-1", "msg-2", "msg-3"]
        assert thread["last_message_id"] == "msg-3"
        assert thread["needs_reply"] is True
    finally:
        storage.shutdown()
