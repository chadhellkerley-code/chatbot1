from __future__ import annotations

import sqlite3
from pathlib import Path

from application.services.base import ServiceContext
from application.services.warmup_service import WarmupService


def _service(tmp_path: Path) -> WarmupService:
    context = ServiceContext.default(root_dir=tmp_path)
    return WarmupService(context)


def test_warmup_service_creates_sqlite_schema_and_cascades_flow_storage(tmp_path: Path) -> None:
    service = _service(tmp_path)
    flow = service.create_flow(alias="default", usernames=["uno", "dos"], name="Flujo A")

    updated = service.save_stage(
        flow["id"],
        title="Dia 1",
        settings={"base_delay_minutes": 15},
        actions=[
            {"action_type": "watch_reels", "target": "", "text": "", "quantity": 9},
            {"action_type": "send_message", "target": "demo_user", "text": "Hola", "quantity": 1},
        ],
    )
    service.append_log(flow["id"], "Warm Up iniciado")
    service.record_account_state(
        flow["id"],
        "uno",
        stage_order=1,
        action_order=2,
        last_action_type="send_message",
        status="paused",
        payload={"last_ok": True},
    )

    assert updated["account_count"] == 2
    assert len(updated["stages"]) == 1
    assert service.db_path.exists()

    with sqlite3.connect(service.db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type = 'table'"
            ).fetchall()
        }
        assert {
            "warmup_flows",
            "warmup_stages",
            "warmup_actions",
            "warmup_flow_accounts",
            "warmup_account_state",
            "warmup_logs",
        } <= tables

    assert service.delete_flow(flow["id"]) is True

    with sqlite3.connect(service.db_path) as connection:
        remaining = {
            "stages": connection.execute("select count(*) from warmup_stages").fetchone()[0],
            "actions": connection.execute("select count(*) from warmup_actions").fetchone()[0],
            "flow_accounts": connection.execute("select count(*) from warmup_flow_accounts").fetchone()[0],
            "account_state": connection.execute("select count(*) from warmup_account_state").fetchone()[0],
            "logs": connection.execute("select count(*) from warmup_logs").fetchone()[0],
        }
    assert remaining == {
        "stages": 0,
        "actions": 0,
        "flow_accounts": 0,
        "account_state": 0,
        "logs": 0,
    }


def test_warmup_service_recovers_running_flows_as_paused_and_keeps_resume_cursor(tmp_path: Path) -> None:
    service = _service(tmp_path)
    flow = service.create_flow(alias="matias", usernames=["matias_a"], name="Flujo Matias")
    flow = service.save_stage(
        flow["id"],
        title="Dia 1",
        settings={"base_delay_minutes": 20},
        actions=[
            {"action_type": "watch_reels", "target": "", "text": "", "quantity": 5},
            {"action_type": "like_posts", "target": "", "text": "", "quantity": 2},
        ],
    )
    flow = service.save_stage(
        flow["id"],
        title="Dia 2",
        settings={"base_delay_minutes": 30},
        actions=[
            {"action_type": "follow_accounts", "target": "demo", "text": "", "quantity": 1},
            {"action_type": "send_message", "target": "demo", "text": "Hola", "quantity": 1},
        ],
    )

    service.record_account_state(
        flow["id"],
        "matias_a",
        stage_order=2,
        action_order=2,
        last_action_type="send_message",
        status="running",
        payload={"last_ok": True},
    )

    recovered = _service(tmp_path)
    flow_after_restart = recovered.get_flow(flow["id"])

    assert flow_after_restart["status"] == "paused"
    assert flow_after_restart["has_started"] is True
    assert flow_after_restart["resume"]["status"] == "paused"
    assert flow_after_restart["resume"]["current_stage_order"] == 2
    assert flow_after_restart["resume"]["current_action_order"] == 2
    assert flow_after_restart["resume"]["last_account"] == "matias_a"
    assert flow_after_restart["resume"]["last_action_type"] == "send_message"
    assert flow_after_restart["selected_usernames"] == ["matias_a"]
    assert any("reabrir la aplicacion" in row["message"] for row in recovered.list_logs(flow["id"]))


def test_warmup_service_auto_numbers_duplicate_names_and_streams_logs(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.create_flow(alias="default", usernames=["uno"], name="Flujo Warm Up")
    second = service.create_flow(alias="default", usernames=["dos"], name="Flujo Warm Up")

    service.append_log(second["id"], "Linea 1")
    service.append_log(second["id"], "Linea 2", level="warning")
    next_log_id, rows = service.read_logs_after(second["id"], 0)

    assert first["name"] == "Flujo Warm Up"
    assert second["name"] == "Flujo Warm Up 2"
    assert next_log_id == rows[-1]["id"]
    assert [row["message"] for row in rows[-2:]] == ["Linea 1", "Linea 2"]
