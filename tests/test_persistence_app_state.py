from __future__ import annotations

import json
import sqlite3

from paths import accounts_root, storage_root
from src.persistence import get_app_state_store, sync_foundation_state


def test_sync_foundation_state_creates_sqlite_shadow_store(tmp_path) -> None:
    accounts_dir = accounts_root(tmp_path)
    storage_dir = storage_root(tmp_path)

    (accounts_dir / "accounts.json").write_text(
        json.dumps(
            [
                {
                    "username": "demo.account",
                    "alias": "ventas",
                    "active": True,
                    "connected": False,
                    "first_seen": "2026-03-10T00:00:00+00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    (storage_dir / "lead_status.json").write_text(
        json.dumps(
            {
                "version": 2,
                "aliases": {
                    "ventas": {
                        "leads": {
                            "lead.one": {
                                "status": "pending",
                                "updated_at": 123,
                                "last_alias": "ventas",
                            }
                        }
                    }
                },
                "legacy_global_leads": {},
            }
        ),
        encoding="utf-8",
    )
    (storage_dir / "conversation_engine.json").write_text(
        json.dumps(
            {
                "conversations": {
                    "ventas|thread-1": {
                        "account": "ventas",
                        "thread_id": "thread-1",
                        "stage": "initial",
                        "updated_at": 456.0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = sync_foundation_state(tmp_path)

    assert result == {"accounts": 1, "lead_status": 1, "conversation_engine": 1}

    connection = sqlite3.connect(tmp_path / "data" / "app_state.db")
    try:
        accounts_count = connection.execute("select count(*) from accounts").fetchone()[0]
        account_alias = connection.execute("select alias from accounts where username = ?", ("demo.account",)).fetchone()[0]
        lead_count = connection.execute("select count(*) from lead_status").fetchone()[0]
        conversation_count = connection.execute("select count(*) from conversation_engine_state").fetchone()[0]
    finally:
        connection.close()

    assert accounts_count == 1
    assert account_alias == "ventas"
    assert lead_count == 1
    assert conversation_count == 1


def test_sync_campaign_state_upserts_runtime_snapshot_into_sqlite(tmp_path) -> None:
    store = get_app_state_store(tmp_path)
    store.sync_campaign_state(
        {
            "run_id": "campaign-20260310",
            "alias": "ventas",
            "leads_alias": "ventas",
            "status": "running",
            "started_at": "2026-03-10T00:00:00+00:00",
            "finished_at": "",
            "sent": 12,
        }
    )

    connection = sqlite3.connect(tmp_path / "data" / "app_state.db")
    try:
        row = connection.execute(
            "select alias, leads_alias, status from campaign_state where run_id = ?",
            ("campaign-20260310",),
        ).fetchone()
    finally:
        connection.close()

    assert row == ("ventas", "ventas", "running")


def test_get_campaign_state_returns_latest_payload_with_heartbeat(tmp_path) -> None:
    store = get_app_state_store(tmp_path)
    store.sync_campaign_state(
        {
            "run_id": "run-1",
            "alias": "ventas",
            "leads_alias": "lista-1",
            "status": "Completed",
            "task_active": False,
        }
    )
    store.sync_campaign_state(
        {
            "run_id": "run-2",
            "alias": "ventas",
            "leads_alias": "lista-2",
            "status": "Running",
            "task_active": True,
        }
    )

    payload = store.get_campaign_state()

    assert payload["run_id"] == "run-2"
    assert payload["leads_alias"] == "lista-2"
    assert payload["status"] == "Running"
    assert payload["heartbeat_at"]


def test_recover_interrupted_campaign_state_marks_running_rows(tmp_path) -> None:
    store = get_app_state_store(tmp_path)
    store.sync_campaign_state(
        {
            "run_id": "run-active",
            "alias": "ventas",
            "leads_alias": "lista-1",
            "status": "Running",
            "task_active": True,
            "workers_active": 2,
            "message": "Procesando campaña.",
        }
    )

    recovered = store.recover_interrupted_campaign_states()
    payload = store.get_campaign_state(run_id="run-active")

    assert len(recovered) == 1
    assert payload["status"] == "Interrupted"
    assert payload["task_active"] is False
    assert payload["workers_active"] == 0
    assert payload["finished_at"]
    assert "reabrir la aplicacion" in payload["message"]


def test_append_and_list_campaign_events(tmp_path) -> None:
    store = get_app_state_store(tmp_path)
    first = store.append_campaign_event(
        {
            "event_id": "evt-1",
            "run_id": "run-1",
            "event_type": "worker_stalled",
            "severity": "warning",
            "failure_kind": "retryable",
            "message": "Worker worker-1 detectado como stalled.",
            "worker_id": "worker-1",
        }
    )
    second = store.append_campaign_event(
        {
            "event_id": "evt-2",
            "run_id": "run-1",
            "event_type": "worker_restarted",
            "severity": "warning",
            "failure_kind": "retryable",
            "message": "Worker worker-1 relanzado.",
            "worker_id": "worker-1",
        }
    )

    events = store.list_campaign_events(run_id="run-1")

    assert first["event_id"] == "evt-1"
    assert second["event_id"] == "evt-2"
    assert [event["event_type"] for event in events] == ["worker_stalled", "worker_restarted"]
    assert events[0]["failure_kind"] == "retryable"
