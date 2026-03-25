import json
from types import SimpleNamespace

import update_system
from application.services.base import ServiceContext
from application.services.system_service import SystemService
from build.helpers import assemble_client_layout
from gui.snapshot_queries import build_system_update_check_snapshot


def test_assemble_client_layout_marks_full_package_update_mode(tmp_path):
    built_executable = tmp_path / "InstaCRM.exe"
    built_executable.write_bytes(b"stub-exe")

    target_dir = tmp_path / "client_dist"
    assemble_client_layout(
        built_executable,
        target_dir=target_dir,
        version="9.9.9",
        bundle_playwright=False,
    )

    app_version = json.loads((target_dir / "app" / "app_version.json").read_text(encoding="utf-8"))

    assert (target_dir / "InstaCRM.exe").exists()
    assert (target_dir / "app").is_dir()
    assert app_version["layout"] == "instacrm.v1"
    assert app_version["update_mode"] == "full-package"


def test_validate_staged_release_payload_requires_full_package(tmp_path):
    payload_root = tmp_path / "payload"
    payload_root.mkdir()
    manifest = {"executable_name": "InstaCRM.exe"}

    ok, message = update_system._validate_staged_release_payload(payload_root, manifest)
    assert ok is False
    assert "ejecutable requerido" in message

    (payload_root / "InstaCRM.exe").write_bytes(b"exe")
    ok, message = update_system._validate_staged_release_payload(payload_root, manifest)
    assert ok is False
    assert "carpeta app/" in message

    (payload_root / "app").mkdir()
    ok, message = update_system._validate_staged_release_payload(payload_root, manifest)
    assert ok is True
    assert message == "payload_full_package_ok"


def test_save_update_config_removes_legacy_exe_asset_name(tmp_path, monkeypatch):
    config_path = tmp_path / "update_config.json"
    monkeypatch.setattr(update_system, "_UPDATE_CONFIG_FILE", config_path)

    update_system._save_update_config(
        {
            "auto_check_enabled": True,
            "check_interval_seconds": 60,
            "last_check_ts": 0,
            "current_version": "1.2.3",
            "exe_asset_name": "InstaCRM.exe",
        }
    )

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["current_version"] == "1.2.3"
    assert "exe_asset_name" not in saved


def test_check_for_updates_returns_structured_update_available_payload(monkeypatch):
    saved_configs: list[dict[str, object]] = []
    release_info = {
        "version": "v1.2.0",
        "description": "Nueva build",
        "release_url": "https://example.test/release",
    }

    monkeypatch.setattr(
        update_system,
        "_load_update_config",
        lambda: {
            "current_version": "1.1.0",
            "last_check_ts": 0,
            "check_interval_seconds": 3600,
        },
    )
    monkeypatch.setattr(update_system, "_save_update_config", lambda payload: saved_configs.append(dict(payload)))
    monkeypatch.setattr(update_system, "_get_latest_release_from_github", lambda repo: dict(release_info))

    result = update_system.check_for_updates(github_repo="demo/repo", force=True)

    assert result["status"] == "update_available"
    assert result["update_available"] is True
    assert result["checked"] is True
    assert result["current_version"] == "1.1.0"
    assert result["latest_version"] == "v1.2.0"
    assert result["update_info"] == release_info
    assert result["github_repo"] == "demo/repo"
    assert "Actualización disponible" in result["message"]
    assert saved_configs


def test_check_for_updates_returns_structured_up_to_date_payload(monkeypatch):
    saved_configs: list[dict[str, object]] = []

    monkeypatch.setattr(
        update_system,
        "_load_update_config",
        lambda: {
            "current_version": "1.2.0",
            "last_check_ts": 0,
            "check_interval_seconds": 3600,
        },
    )
    monkeypatch.setattr(update_system, "_save_update_config", lambda payload: saved_configs.append(dict(payload)))
    monkeypatch.setattr(
        update_system,
        "_get_latest_release_from_github",
        lambda repo: {"version": "v1.2.0", "release_url": "https://example.test/release"},
    )

    result = update_system.check_for_updates(force=True)

    assert result["status"] == "up_to_date"
    assert result["update_available"] is False
    assert result["checked"] is True
    assert result["current_version"] == "1.2.0"
    assert result["latest_version"] == "v1.2.0"
    assert "más reciente" in result["message"]
    assert isinstance(result["update_info"], dict)
    assert saved_configs


def test_check_for_updates_returns_structured_error_payload(monkeypatch):
    monkeypatch.setattr(
        update_system,
        "_load_update_config",
        lambda: {
            "current_version": "1.2.0",
            "last_check_ts": 0,
            "check_interval_seconds": 3600,
        },
    )
    monkeypatch.setattr(update_system, "_get_latest_release_from_github", lambda repo: None)

    result = update_system.check_for_updates(force=True)

    assert result["status"] == "error"
    assert result["update_available"] is False
    assert result["checked"] is True
    assert result["current_version"] == "1.2.0"
    assert result["latest_version"] is None
    assert result["update_info"] is None
    assert "No se pudo conectar" in result["message"]


def test_system_service_check_updates_returns_backend_contract(monkeypatch):
    expected = {
        "status": "up_to_date",
        "checked": True,
        "update_available": False,
        "message": "Sin novedades",
        "current_version": "1.0.0",
        "latest_version": "1.0.0",
        "update_info": {"version": "1.0.0"},
        "github_repo": "demo/repo",
    }
    calls: list[dict[str, object]] = []

    def _fake_check_for_updates(*, force: bool = False, github_repo=None):  # noqa: ANN001
        calls.append({"force": force, "github_repo": github_repo})
        return dict(expected)

    monkeypatch.setenv("INSTACRM_BOOTSTRAP_MODE", "owner")
    monkeypatch.setattr(update_system, "check_for_updates", _fake_check_for_updates)

    service = SystemService(ServiceContext.default())
    result = service.check_updates()

    assert result == expected
    assert calls == [{"force": True, "github_repo": None}]


def test_build_system_update_check_snapshot_preserves_service_contract():
    expected = {
        "status": "error",
        "checked": True,
        "update_available": False,
        "message": "fallo",
        "current_version": "1.0.0",
        "latest_version": None,
        "update_info": None,
        "github_repo": "demo/repo",
    }
    services = SimpleNamespace(system=SimpleNamespace(check_updates=lambda: dict(expected)))

    assert build_system_update_check_snapshot(services) == expected


def test_evaluate_update_runtime_preflight_blocks_campaign_and_inbox_activity():
    ok_to_update, blockers, details = update_system._evaluate_update_runtime_preflight(
        campaign_state={
            "run_id": "campaign-123",
            "status": "Running",
            "task_active": True,
            "_synced_at_ts": update_system.time.time(),
        },
        runtime_alias_states=[
            {
                "alias_id": "ventas",
                "is_running": True,
                "worker_state": "running",
            }
        ],
        pending_inbox_jobs=[
            {
                "job_type": "followup",
                "state": "queued",
            }
        ],
        session_connectors=[
            {
                "account_id": "agente_1",
                "state": "ready",
            }
        ],
    )

    assert ok_to_update is False
    assert len(blockers) == 4
    assert "campaign-123" in blockers[0]
    assert "ventas" in blockers[1]
    assert "followup" in blockers[2]
    assert "agente_1" in blockers[3]
    assert details["campaign_state"]["run_id"] == "campaign-123"


def test_evaluate_update_runtime_preflight_ignores_stale_runtime_markers():
    stale_ts = update_system.time.time() - 3600

    ok_to_update, blockers, _details = update_system._evaluate_update_runtime_preflight(
        campaign_state={
            "run_id": "campaign-stale",
            "status": "Running",
            "task_active": True,
            "_synced_at_ts": stale_ts,
        },
        runtime_alias_states=[],
        pending_inbox_jobs=[],
        session_connectors=[],
    )

    assert ok_to_update is True
    assert blockers == []


def test_runtime_alias_state_blocks_update_only_when_heartbeat_is_fresh():
    fresh_state = {
        "alias_id": "ventas",
        "is_running": True,
        "worker_state": "running",
        "delay_max_ms": 0,
        "last_heartbeat_at": update_system.time.time(),
        "updated_at": update_system.time.time(),
    }
    stale_state = {
        **fresh_state,
        "last_heartbeat_at": update_system.time.time() - 600,
        "updated_at": update_system.time.time() - 600,
    }

    assert update_system._runtime_alias_state_blocks_update(fresh_state) is True
    assert update_system._runtime_alias_state_blocks_update(stale_state) is False


def test_session_connector_state_blocks_update_only_when_heartbeat_is_fresh():
    fresh_state = {
        "account_id": "agente_1",
        "state": "ready",
        "last_heartbeat_at": update_system.time.time(),
        "updated_at": update_system.time.time(),
    }
    stale_state = {
        **fresh_state,
        "last_heartbeat_at": update_system.time.time() - 600,
        "updated_at": update_system.time.time() - 600,
    }

    assert update_system._session_connector_state_blocks_update(fresh_state) is True
    assert update_system._session_connector_state_blocks_update(stale_state) is False


def test_apply_update_blocks_when_runtime_preflight_fails(tmp_path, monkeypatch):
    update_file = tmp_path / "update.zip"
    update_file.write_bytes(b"stub")

    monkeypatch.setattr(
        update_system,
        "_check_update_runtime_preflight",
        lambda: (False, "blocked by runtime", {"blockers": ["runtime"]}),
    )

    success, message = update_system.apply_update(update_file, backup=True)

    assert success is False
    assert message == "blocked by runtime"


def test_apply_update_continues_when_runtime_preflight_is_clean(tmp_path, monkeypatch):
    update_file = tmp_path / "update.zip"
    update_file.write_bytes(b"stub")
    stage_root = tmp_path / "stage"
    payload_root = stage_root / "payload"
    payload_root.mkdir(parents=True)
    (payload_root / "InstaCRM.exe").write_bytes(b"exe")
    (payload_root / "app").mkdir()

    monkeypatch.setattr(
        update_system,
        "_check_update_runtime_preflight",
        lambda: (True, "ok", {"blockers": []}),
    )
    monkeypatch.setattr(
        update_system,
        "_stage_update_archive",
        lambda _path: (
            True,
            {
                "payload_root": str(payload_root),
                "stage_root": str(stage_root),
                "manifest": {"version": "9.9.9", "executable_name": "InstaCRM.exe"},
                "version": "9.9.9",
            },
            "stage_ok",
        ),
    )
    monkeypatch.setattr(
        update_system,
        "_schedule_staged_update_windows",
        lambda _stage_info, backup=True: (True, "scheduled"),
    )

    success, message = update_system.apply_update(update_file, backup=True)

    assert success is True
    assert "scheduled" in message
