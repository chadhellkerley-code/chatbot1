from __future__ import annotations
import contextlib
import json
import sqlite3
from pathlib import Path

from core.storage_atomic import atomic_write_json
from paths import storage_root
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


def test_inbox_storage_resolve_local_outbound_without_sent_timestamp_keeps_original_message_time(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0, direction="outbound", unread_count=0)])

        local = storage.append_local_outbound_message("acc1:thread-a", "Te respondo prolijo")

        assert local is not None
        created_at = float(local["created_at"])
        local_timestamp = float(local["timestamp"])

        storage.resolve_local_outbound(
            "acc1:thread-a",
            str(local["message_id"]),
            final_message_id="real-msg-no-ts",
        )

        thread = storage.get_thread("acc1:thread-a")

        assert thread is not None
        latest = thread["messages"][-1]
        assert latest["message_id"] == "real-msg-no-ts"
        assert latest["timestamp"] == local_timestamp
        assert latest["confirmed_at"] is None
        assert latest["message_ts_canonical"] == local_timestamp
        assert latest["message_ts_source"] == "timestamp"
        assert latest["created_at"] == created_at
        assert thread["last_message_timestamp"] == local_timestamp
    finally:
        storage.shutdown()


<<<<<<< HEAD
def test_inbox_storage_trim_global_threads_does_not_drop_threads_with_pending_work(tmp_path: Path) -> None:
    original_limit = InboxStorage._MAX_ACTIVE_THREADS
    InboxStorage._MAX_ACTIVE_THREADS = 3
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                _thread_row("acc1", "thread-old", timestamp=800.0, direction="inbound", unread_count=1),
                _thread_row("acc1", "thread-pending", timestamp=10.0, direction="inbound", unread_count=1),
                _thread_row("acc1", "thread-very-old", timestamp=1.0, direction="inbound", unread_count=1),
            ]
        )
        pending_key = "acc1:thread-pending"
        storage.update_thread_state(pending_key, {"sender_status": "sending", "sender_error": ""})

        storage.upsert_threads(
            [
                _thread_row("acc1", "thread-new-1", timestamp=1000.0, direction="inbound", unread_count=1),
                _thread_row("acc1", "thread-new-2", timestamp=900.0, direction="inbound", unread_count=1),
            ]
        )

        thread_keys = {row["thread_key"] for row in storage.get_threads("all")}
        assert thread_keys == {
            "acc1:thread-new-1",
            "acc1:thread-new-2",
            "acc1:thread-pending",
        }
        assert storage.get_thread(pending_key) is not None
        storage.update_thread_state(pending_key, {"sender_status": "failed", "sender_error": "quota"})
        refreshed = storage.get_thread(pending_key)
        assert refreshed is not None
        assert refreshed["sender_status"] == "failed"

        fk_issues = storage._conn.execute("PRAGMA foreign_key_check").fetchall()
        assert fk_issues == []
    finally:
        storage.shutdown()
        InboxStorage._MAX_ACTIVE_THREADS = original_limit


=======
>>>>>>> origin/main
def test_inbox_storage_persists_outbound_source_by_job_type(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])

        auto = storage.append_local_outbound_message("acc1:thread-a", "Auto", source="auto")
        followup = storage.append_local_outbound_message("acc1:thread-a", "Follow", source="followup")

        thread = storage.get_thread("acc1:thread-a")

        assert auto is not None
        assert followup is not None
        assert thread is not None
        assert sorted(row["source"] for row in thread["messages"][-2:]) == ["auto", "followup"]
        assert thread["last_action_type"] == "followup"
    finally:
        storage.shutdown()


def test_inbox_storage_reconciles_synthetic_confirmation_with_remote_message_without_duplicates(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    reopened = None
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0, direction="outbound", unread_count=0)])

        local = storage.append_local_outbound_message("acc1:thread-a", "hola serio", source="manual")
        assert local is not None

        storage.resolve_local_outbound(
            "acc1:thread-a",
            str(local["message_id"]),
            final_message_id="dom-confirmed-1",
            sent_timestamp=130.0,
        )
        storage.seed_messages(
            "acc1:thread-a",
            [
                {
                    "message_id": "ig-real-1",
                    "text": "hola serio",
                    "timestamp": 131.0,
                    "direction": "outbound",
                    "delivery_status": "sent",
                }
            ],
            participants=["cliente_a"],
        )

        thread = storage.get_thread("acc1:thread-a")

        assert thread is not None
        assert len(thread["messages"]) == 1
        assert thread["messages"][0]["message_id"] == "ig-real-1"
        assert thread["messages"][0]["delivery_status"] == "sent"
        assert thread["last_message_id"] == "ig-real-1"
        assert thread["last_message_timestamp"] == 131.0

        storage.shutdown()
        reopened = InboxStorage(tmp_path)
        reopened_thread = reopened.get_thread("acc1:thread-a")

        assert reopened_thread is not None
        assert len(reopened_thread["messages"]) == 1
        assert reopened_thread["messages"][0]["message_id"] == "ig-real-1"
    finally:
        with contextlib.suppress(Exception):
            if reopened is not None:
                reopened.shutdown()
            else:
                storage.shutdown()


<<<<<<< HEAD
def test_enqueue_send_queue_job_reuses_dedupe_without_losing_metadata_or_local_message_id(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])

        first = storage.enqueue_send_queue_job(
            "auto_reply",
            thread_key=thread_key,
            account_id="acc1",
            payload={
                "thread_key": thread_key,
                "text": "hola",
                "local_message_id": "local-1",
                "post_send_thread_updates": {"stage_id": "stage_2"},
            },
            dedupe_key="auto_reply:acc1:thread-a:text:in-1",
        )
        reused = storage.enqueue_send_queue_job(
            "auto_reply",
            thread_key=thread_key,
            account_id="acc1",
            payload={
                "thread_key": thread_key,
                "text": "hola",
                "local_message_id": "local-2",
                "post_send_state_updates": {"last_inbound_id_seen": "in-1"},
            },
            dedupe_key="auto_reply:acc1:thread-a:text:in-1",
        )
        job = storage.get_send_queue_job(int(first.get("job_id") or 0))

        assert first["created"] is True
        assert reused["reused"] is True
        assert job is not None
        assert job["dedupe_key"] == "auto_reply:acc1:thread-a:text:in-1"
        assert job["payload"]["local_message_id"] == "local-1"
        assert job["payload"]["post_send_thread_updates"] == {"stage_id": "stage_2"}
        assert job["payload"]["post_send_state_updates"] == {"last_inbound_id_seen": "in-1"}
        assert job["payload"]["dedupe_key"] == "auto_reply:acc1:thread-a:text:in-1"
        assert job["payload"]["job_type"] == "auto_reply"
    finally:
        storage.shutdown()


=======
>>>>>>> origin/main
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
        assert [row["message_id"] for row in thread["messages"]] == ["msg-1", "msg-2", "msg-3"]
        assert thread["messages"][0]["text"] == "antes de conectar"
        assert thread["unread_count"] == 0

        storage.prepare_account_session("acc1", session_marker="monitor-2", started_at=200.0)

        assert storage.get_threads("all") == []
        assert storage.get_thread("acc1:thread-new") is None
    finally:
        storage.shutdown()


def test_inbox_storage_mark_read_creates_state_for_new_thread_without_fk_error(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.replace_messages(
            "acc1:thread-brand-new",
            [
                {
                    "message_id": "msg-1",
                    "text": "hola",
                    "timestamp": 101.0,
                    "direction": "inbound",
                }
            ],
            mark_read=True,
        )

        thread = storage.get_thread("acc1:thread-brand-new")
        assert thread is not None
        assert thread["unread_count"] == 0

        state = storage.snapshot()["state"]["threads"].get("acc1:thread-brand-new")
        assert isinstance(state, dict)
        assert isinstance(state.get("last_opened_at"), float)
    finally:
        storage.shutdown()


def test_inbox_storage_update_thread_state_tolerates_parent_row_gap_between_connections(tmp_path: Path) -> None:
    primary = InboxStorage(tmp_path)
    secondary = InboxStorage(tmp_path)
    thread_key = "acc1:thread-race"
    try:
        primary.upsert_threads([_thread_row("acc1", "thread-race", timestamp=100.0)])

        for idx in range(30):
            with primary._lock:
                primary._conn.execute("DELETE FROM inbox_threads WHERE thread_key = ?", (thread_key,))
                primary._conn.commit()

            # Must not raise IntegrityError even if the parent row is temporarily absent.
            secondary.update_thread_state(thread_key, {"tick": idx})

            with primary._lock:
                primary._upsert_thread_record(primary._thread_shell(thread_key))
                primary._conn.commit()

        secondary.update_thread_state(thread_key, {"tick": 999})
        state = secondary.snapshot()["state"]["threads"].get(thread_key)
        assert isinstance(state, dict)
        assert state.get("tick") == 999
    finally:
        secondary.shutdown()
        primary.shutdown()


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


def test_inbox_storage_separates_operational_status_from_ui_status(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    **_thread_row("acc1", "thread-a", timestamp=100.0),
                    "status": "open",
                }
            ]
        )

        storage.update_thread_state("acc1:thread-a", {"status": "needs_reply", "thread_status": "ready"})
        updated = storage.update_thread_record("acc1:thread-a", {"operational_status": "closed"})
        thread = storage.get_thread("acc1:thread-a")
        snapshot = storage.snapshot()

        assert updated is not None
        assert updated["status"] == "closed"
        assert updated["operational_status"] == "closed"
        assert updated["ui_status"] == "needs_reply"
        assert thread is not None
        assert thread["status"] == "closed"
        assert thread["operational_status"] == "closed"
        assert thread["ui_status"] == "needs_reply"
        assert snapshot["state"]["threads"]["acc1:thread-a"]["ui_status"] == "needs_reply"
        assert "status" not in snapshot["state"]["threads"]["acc1:thread-a"]
    finally:
        storage.shutdown()


def test_inbox_storage_partial_upsert_drops_unconfirmed_promoted_stage_and_followup(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.update_thread_record(
            thread_key,
            {
                "stage_id": "stage_2",
                "followup_level": 2,
                "last_message_text": "hola",
                "last_message_timestamp": 130.0,
                "last_message_direction": "inbound",
                "last_message_id": "in-2",
                "last_outbound_at": None,
            },
        )
        storage._conn.execute(
            """
            INSERT INTO inbox_thread_state(thread_key, state_json)
            VALUES(?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET state_json = excluded.state_json
            """,
            (
                thread_key,
                json.dumps(
                    {
                        "stage_id": "stage_2",
                        "followup_level": 2,
                        "flow_state": {
                            "stage_id": "stage_2",
                            "followup_level": 2,
                            "followup_anchor_ts": 180.0,
                            "last_outbound_ts": 180.0,
                            "objection_step": 0,
                        },
                    }
                ),
            ),
        )
        storage._conn.commit()

        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=131.0)])

        thread = storage.get_thread(thread_key)
        snapshot = storage.snapshot()

        assert thread is not None
        assert thread["stage_id"] == "initial"
        assert thread["followup_level"] == 0
        assert thread["flow_state"]["stage_id"] == "initial"
        assert thread["flow_state"]["followup_level"] == 0
        assert thread["flow_state"]["followup_anchor_ts"] is None
        assert thread["flow_state"]["last_outbound_ts"] is None
        assert "stage_id" not in snapshot["state"]["threads"][thread_key]
        assert "followup_level" not in snapshot["state"]["threads"][thread_key]
    finally:
        storage.shutdown()


def test_inbox_storage_partial_upsert_keeps_confirmed_advanced_stage_and_followup(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.update_thread_record(
            thread_key,
            {
                "stage_id": "stage_2",
                "followup_level": 1,
                "status": "replied",
                "last_message_text": "me interesa",
                "last_message_timestamp": 240.0,
                "last_message_direction": "inbound",
                "last_message_id": "in-2",
                "last_inbound_at": 240.0,
                "last_outbound_at": 180.0,
            },
        )
        storage._conn.execute(
            """
            INSERT INTO inbox_thread_state(thread_key, state_json)
            VALUES(?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET state_json = excluded.state_json
            """,
            (
                thread_key,
                json.dumps(
                    {
                        "flow_state": {
                            "stage_id": "stage_2",
                            "followup_level": 1,
                            "followup_anchor_ts": 180.0,
                            "last_outbound_ts": 180.0,
                            "objection_step": 0,
                        }
                    }
                ),
            ),
        )
        storage._conn.commit()

        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=241.0)])

        thread = storage.get_thread(thread_key)

        assert thread is not None
        assert thread["stage_id"] == "stage_2"
        assert thread["followup_level"] == 1
        assert thread["flow_state"]["stage_id"] == "stage_2"
        assert thread["flow_state"]["followup_level"] == 1
        assert thread["flow_state"]["followup_anchor_ts"] == 180.0
        assert thread["flow_state"]["last_outbound_ts"] == 180.0
    finally:
        storage.shutdown()


def test_inbox_storage_partial_upsert_does_not_keep_followup_level_ahead_without_evidence(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.update_thread_record(
            thread_key,
            {
                "stage_id": "initial",
                "followup_level": 3,
                "last_message_text": "hola",
                "last_message_timestamp": 150.0,
                "last_message_direction": "inbound",
                "last_message_id": "in-3",
                "last_outbound_at": None,
            },
        )
        storage._conn.execute(
            """
            INSERT INTO inbox_thread_state(thread_key, state_json)
            VALUES(?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET state_json = excluded.state_json
            """,
            (
                thread_key,
                json.dumps(
                    {
                        "followup_level": 3,
                        "flow_state": {
                            "stage_id": "initial",
                            "followup_level": 3,
                            "objection_step": 0,
                        },
                    }
                ),
            ),
        )
        storage._conn.commit()

        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=151.0)])

        thread = storage.get_thread(thread_key)

        assert thread is not None
        assert thread["stage_id"] == "initial"
        assert thread["followup_level"] == 0
        assert thread["flow_state"]["stage_id"] == "initial"
        assert thread["flow_state"]["followup_level"] == 0
    finally:
        storage.shutdown()


def test_inbox_storage_partial_upsert_keeps_canonical_inicial_with_legacy_initial_flow_state(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])
        storage.update_thread_record(
            thread_key,
            {
                "stage_id": "inicial",
                "followup_level": 1,
                "status": "pending",
                "last_message_text": "primer outreach",
                "last_message_timestamp": 180.0,
                "last_message_direction": "outbound",
                "last_message_id": "out-1",
                "last_outbound_at": 180.0,
            },
        )
        storage._conn.execute(
            """
            INSERT INTO inbox_thread_state(thread_key, state_json)
            VALUES(?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET state_json = excluded.state_json
            """,
            (
                thread_key,
                json.dumps(
                    {
                        "flow_state": {
                            "stage_id": "initial",
                            "followup_level": 1,
                            "followup_anchor_ts": 180.0,
                            "last_outbound_ts": 180.0,
                            "objection_step": 0,
                        }
                    }
                ),
            ),
        )
        storage._conn.commit()

        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=181.0)])

        thread = storage.get_thread(thread_key)

        assert thread is not None
        assert thread["stage_id"] == "inicial"
        assert thread["followup_level"] == 1
        assert thread["flow_state"]["stage_id"] == "inicial"
        assert thread["flow_state"]["followup_level"] == 1
    finally:
        storage.shutdown()


def test_inbox_storage_rewrites_latest_outbound_message_stage_when_thread_stage_is_confirmed(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        thread_key = "acc1:thread-a"
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0, direction="outbound", unread_count=0)])

        local = storage.append_local_outbound_message(thread_key, "respuesta automatica", source="auto")
        assert local is not None

        storage.resolve_local_outbound(
            thread_key,
            str(local["message_id"]),
            final_message_id="real-msg-1",
            sent_timestamp=120.0,
        )
        storage.update_thread_record(
            thread_key,
            {
                "stage_id": "etapa_1",
                "last_outbound_at": 120.0,
                "last_action_type": "auto_reply_sent",
                "last_action_at": 120.0,
                "last_message_direction": "outbound",
                "last_message_id": "real-msg-1",
            },
        )

        thread = storage.get_thread(thread_key)

        assert thread is not None
        assert thread["stage_id"] == "etapa_1"
        assert thread["messages"][-1]["message_id"] == "real-msg-1"
        assert thread["messages"][-1]["stage_id"] == "etapa_1"
    finally:
        storage.shutdown()


def test_inbox_storage_preserves_separate_messages_inside_previous_group_window(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0, direction="outbound", unread_count=0)])

        storage.replace_messages(
            "acc1:thread-a",
            [
                {
                    "message_id": "msg-1",
                    "text": "Dale gracias",
                    "timestamp": 100.0,
                    "direction": "outbound",
                },
                {
                    "message_id": "msg-2",
                    "text": "Bueno dale",
                    "timestamp": 130.0,
                    "direction": "outbound",
                },
            ],
            mark_read=True,
        )

        thread = storage.get_thread("acc1:thread-a")

        assert thread is not None
        assert [row["message_id"] for row in thread["messages"]] == ["msg-1", "msg-2"]
        assert [row["text"] for row in thread["messages"]] == ["Dale gracias", "Bueno dale"]
    finally:
        storage.shutdown()


def test_inbox_storage_update_thread_record_persists_and_clears_thread_state_metadata(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=100.0)])

        taken = storage.update_thread_record(
            "acc1:thread-a",
            {
                "owner": "manual",
                "bucket": "qualified",
                "status": "open",
                "previous_bucket": "all",
                "previous_status": "pending",
                "previous_owner": "auto",
            },
        )
        released = storage.update_thread_record(
            "acc1:thread-a",
            {
                "owner": "auto",
                "bucket": "all",
                "status": "pending",
                "previous_bucket": None,
                "previous_status": None,
                "previous_owner": None,
            },
        )
        snapshot = storage.snapshot()

        assert taken is not None
        assert taken["previous_bucket"] == "all"
        assert taken["previous_status"] == "pending"
        assert taken["previous_owner"] == "auto"

        assert released is not None
        assert "previous_bucket" not in released
        assert "previous_status" not in released
        assert "previous_owner" not in released
        assert "previous_bucket" not in snapshot["state"]["threads"]["acc1:thread-a"]
        assert "previous_status" not in snapshot["state"]["threads"]["acc1:thread-a"]
        assert "previous_owner" not in snapshot["state"]["threads"]["acc1:thread-a"]
    finally:
        storage.shutdown()


def test_inbox_storage_orders_threads_by_last_real_message_timestamp(tmp_path: Path) -> None:
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
            "acc1:thread-b",
            "acc1:thread-a",
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


def test_inbox_storage_does_not_invent_message_timestamp_when_preview_message_lacks_timestamp(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads(
            [
                {
                    **_thread_row("acc1", "thread-a", timestamp=180.0, direction="inbound", unread_count=1),
                    "last_message_text": "Preview inbound",
                }
            ]
        )

        storage.seed_messages(
            "acc1:thread-a",
            [
                {
                    "message_id": "preview-inbound",
                    "text": "Preview inbound",
                    "timestamp": None,
                    "direction": "inbound",
                }
            ],
            participants=["cliente_a"],
        )

        thread = storage.get_thread("acc1:thread-a")

        assert thread is not None
        assert thread["messages"][-1]["timestamp"] is None
        assert thread["last_message_text"] == "Preview inbound"
        assert thread["last_message_direction"] == "inbound"
        assert thread["last_message_timestamp"] == 180.0
    finally:
        storage.shutdown()


def test_inbox_storage_delete_message_local_hides_message_without_removing_it_from_instagram_state(tmp_path: Path) -> None:
    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_threads([_thread_row("acc1", "thread-a", timestamp=120.0, direction="outbound", unread_count=0)])
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
                    "text": "respuesta",
                    "timestamp": 120.0,
                    "direction": "outbound",
                },
            ],
            participants=["cliente_a"],
            mark_read=True,
        )

        deleted = storage.delete_message_local("acc1:thread-a", {"message_id": "msg-2"})
        thread = storage.get_thread("acc1:thread-a")

        assert deleted is True
        assert thread is not None
        assert [row["message_id"] for row in thread["messages"]] == ["msg-1"]
        assert thread["last_message_id"] == "msg-1"
        assert thread["last_message_text"] == "hola"
        assert thread["last_message_direction"] == "inbound"

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
                    "text": "respuesta",
                    "timestamp": 120.0,
                    "direction": "outbound",
                },
            ],
            participants=["cliente_a"],
            mark_read=False,
        )
        refreshed = storage.get_thread("acc1:thread-a")

        assert refreshed is not None
        assert [row["message_id"] for row in refreshed["messages"]] == ["msg-1"]
    finally:
        storage.shutdown()


def test_inbox_storage_migrates_legacy_sqlite_schema_before_alias_dependent_indexes(tmp_path: Path) -> None:
    storage_dir = storage_root(tmp_path)
    db_path = storage_dir / InboxStorage.DATABASE_FILE
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE inbox_threads (
                thread_key TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                thread_href TEXT NOT NULL DEFAULT '',
                account_id TEXT NOT NULL,
                account_alias TEXT NOT NULL DEFAULT '',
                recipient_username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                created_at REAL,
                updated_at REAL,
                last_message_text TEXT NOT NULL DEFAULT '',
                last_message_timestamp REAL,
                last_message_direction TEXT NOT NULL DEFAULT 'unknown',
                last_message_id TEXT NOT NULL DEFAULT '',
                unread_count INTEGER NOT NULL DEFAULT 0,
                tags_json TEXT NOT NULL DEFAULT '[]',
                participants_json TEXT NOT NULL DEFAULT '[]',
                last_synced_at REAL
            );

            CREATE TABLE inbox_messages (
                thread_key TEXT NOT NULL,
                block_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                message_id TEXT NOT NULL,
                text TEXT NOT NULL DEFAULT '',
                timestamp REAL,
                direction TEXT NOT NULL DEFAULT 'unknown',
                PRIMARY KEY(thread_key, block_id, ordinal)
            );

            CREATE TABLE inbox_send_queue_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued',
                created_at REAL,
                updated_at REAL
            );

            CREATE TABLE inbox_thread_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_key TEXT NOT NULL,
                account_id TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL
            );

            CREATE TABLE session_connector_state (
                account_id TEXT PRIMARY KEY,
                state TEXT NOT NULL DEFAULT 'offline',
                proxy_key TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO inbox_threads(
                thread_key, thread_id, thread_href, account_id, account_alias,
                recipient_username, display_name, created_at, updated_at,
                last_message_text, last_message_timestamp, last_message_direction,
                last_message_id, unread_count, tags_json, participants_json, last_synced_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "acc1:thread-a",
                "thread-a",
                "https://www.instagram.com/direct/t/thread-a/",
                "acc1",
                "ventas",
                "cliente_a",
                "Cliente A",
                100.0,
                101.0,
                "Hola legacy",
                101.0,
                "inbound",
                "msg-legacy",
                2,
                "[]",
                '["cliente_a"]',
                101.0,
            ),
        )
        conn.execute(
            """
            INSERT INTO inbox_messages(thread_key, block_id, ordinal, message_id, text, timestamp, direction)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            ("acc1:thread-a", "legacy", 0, "msg-legacy", "Hola legacy", 101.0, "inbound"),
        )
        conn.execute(
            """
            INSERT INTO inbox_thread_events(thread_key, account_id, event_type, payload_json, created_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            ("acc1:thread-a", "acc1", "legacy_sync", "{}", 101.0),
        )
        conn.execute(
            """
            INSERT INTO session_connector_state(account_id, state, proxy_key, last_error, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            ("acc1", "offline", "", "", 101.0),
        )
        conn.commit()
    finally:
        conn.close()

    storage = InboxStorage(tmp_path)
    reopened = None
    try:
        thread = storage.get_thread("acc1:thread-a")
        assert thread is not None
        assert thread["last_message_text"] == "Hola legacy"
        assert thread["account_alias"] == "ventas"
        assert thread["alias_id"] == "ventas"

        thread_columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(inbox_threads)").fetchall()
        }
        assert {"alias_id", "needs_reply", "last_seen_text", "last_seen_at", "latest_customer_message_at"} <= thread_columns

        message_columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(inbox_messages)").fetchall()
        }
        assert {"account_id", "source", "user_id", "delivery_status", "sent_status", "local_echo", "error_message"} <= message_columns

        queue_columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(inbox_send_queue_jobs)").fetchall()
        }
        assert {"job_type", "dedupe_key", "priority", "attempt_count", "failure_reason"} <= queue_columns

        event_columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(inbox_thread_events)").fetchall()
        }
        assert "alias_id" in event_columns

        connector_columns = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("PRAGMA table_info(session_connector_state)").fetchall()
        }
        assert "alias_id" in connector_columns

        index_names = {
            str(row["name"] or "").strip()
            for row in storage._conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
        }
        assert "inbox_threads_alias_bucket_idx" in index_names
        assert "inbox_messages_thread_ts_idx" in index_names
        assert "inbox_thread_events_thread_idx" in index_names

        storage.add_thread_event("acc1:thread-a", "opened", account_id="acc1", alias_id="ventas")
        events = storage.list_thread_events("acc1:thread-a")
        assert events[0]["alias_id"] == "ventas"

        connector = storage.upsert_session_connector_state("acc1", {"alias_id": "ventas", "state": "online"})
        assert connector["alias_id"] == "ventas"
        assert connector["state"] == "online"

        storage.shutdown()

        reopened = InboxStorage(tmp_path)
        reopened_thread = reopened.get_thread("acc1:thread-a")
        assert reopened_thread is not None
        assert reopened_thread["alias_id"] == "ventas"
        assert reopened.list_thread_events("acc1:thread-a")[0]["alias_id"] == "ventas"
        assert reopened.get_session_connector_state("acc1")["alias_id"] == "ventas"
    finally:
        with contextlib.suppress(Exception):
            if reopened is not None:
                reopened.shutdown()
            else:
                storage.shutdown()
