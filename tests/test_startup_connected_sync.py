from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

import health_store
import runtime.runtime_parity as runtime_parity
import src.playwright_service as playwright_service
from bootstrap import lifecycle
from core import accounts as accounts_module
from src.inbox.inbox_storage import InboxStorage


def _configure_health_store_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "accounts-runtime"
    monkeypatch.setattr(health_store, "DATA_DIR", data_dir)
    monkeypatch.setattr(health_store, "HEALTH_FILE", data_dir / "account_health.json")
    monkeypatch.setattr(health_store, "DB_FILE", data_dir / "account_runtime_state.sqlite3")
    monkeypatch.setattr(
        health_store,
        "LEGACY_HEALTH_BACKUP_FILE",
        data_dir / "account_health.legacy.json",
    )


def _configure_accounts_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    accounts_dir = tmp_path / "storage" / "accounts"
    accounts_dir.mkdir(parents=True, exist_ok=True)
    accounts_file = accounts_dir / "accounts.json"
    monkeypatch.setattr(accounts_module, "DATA", accounts_dir)
    monkeypatch.setattr(accounts_module, "FILE", accounts_file)
    monkeypatch.setattr(accounts_module, "_PASSWORD_FILE", accounts_dir / "passwords.json")
    monkeypatch.setattr(accounts_module, "_PASSWORD_CACHE", {})
    return accounts_file


def test_mark_connected_default_does_not_invalidate_health_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_health_store_paths(monkeypatch, tmp_path)
    health_store._ensure_schema()
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "acc-1",
                    "active": True,
                    "connected": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    health_store.mark_alive("acc-1", reason="seed_viva")
    badge_before, expired_before = health_store.get_badge("acc-1")
    assert badge_before == "VIVA"
    assert expired_before is False

    accounts_module.mark_connected("acc-1", False)

    badge_after, expired_after = health_store.get_badge("acc-1")
    assert badge_after == "VIVA"
    assert expired_after is False
    assert health_store.get_connected("acc-1")[0] is False
    assert bool(accounts_module.get_account("acc-1")["connected"]) is False


def test_update_account_metadata_does_not_invalidate_health_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_health_store_paths(monkeypatch, tmp_path)
    health_store._ensure_schema()
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "acc-1",
                    "alias": "ventas",
                    "active": True,
                    "connected": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    health_store.mark_alive("acc-1", reason="seed_viva")

    updated = accounts_module.update_account(
        "acc-1",
        {"alias": "soporte", "messages_per_account": 7},
    )

    badge_after, expired_after = health_store.get_badge("acc-1")

    assert updated is True
    assert accounts_module.get_account("acc-1")["alias"] == "soporte"
    assert badge_after == "VIVA"
    assert expired_after is False


def test_rename_account_transfers_canonical_runtime_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_health_store_paths(monkeypatch, tmp_path)
    health_store._ensure_schema()
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "acc-1",
                    "alias": "ventas",
                    "active": True,
                    "connected": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    health_store.mark_alive("acc-1", reason="seed_viva")
    health_store.set_connected("acc-1", True, source="seed")
    health_store.set_login_progress(
        "acc-1",
        health_store.LOGIN_PROGRESS_QUEUED,
        run_id="run-1",
        message="En cola",
    )

    renamed = accounts_module._rename_account_record("acc-1", "acc-2")

    old_badge, old_expired = health_store.get_badge("acc-1")
    new_badge, new_expired = health_store.get_badge("acc-2")
    connected, source, reason = health_store.get_connected("acc-2")
    progress = health_store.get_login_progress("acc-2")

    assert renamed == "acc-2"
    assert accounts_module.get_account("acc-1") is None
    assert accounts_module.get_account("acc-2")["username"] == "acc-2"
    assert old_badge is None
    assert old_expired is True
    assert new_badge == "VIVA"
    assert new_expired is False
    assert connected is True
    assert source == "seed"
    assert reason == ""
    assert progress["active"] is True
    assert progress["state"] == health_store.LOGIN_PROGRESS_QUEUED
    assert progress["run_id"] == "run-1"


def test_runtime_preflight_syncs_connected_without_invalidating_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    profiles_dir = tmp_path / "runtime" / "browser_profiles"
    storage_dir = tmp_path / "storage"
    browser_path = tmp_path / "runtime" / "browsers" / "chrome.exe"
    browser_path.parent.mkdir(parents=True, exist_ok=True)
    browser_path.write_text("browser", encoding="utf-8")

    calls: list[tuple[str, bool, bool]] = []
    fake_accounts = ModuleType("core.accounts")
    fake_accounts.list_all = lambda: [{"username": "acc-1", "connected": True}]
    fake_accounts.mark_connected = (
        lambda username, connected, *, invalidate_health=False: calls.append(
            (username, connected, invalidate_health)
        )
    )

    monkeypatch.setitem(sys.modules, "core.accounts", fake_accounts)
    monkeypatch.setattr(runtime_parity, "bootstrap_runtime_env", lambda _mode: {"app_data_root": str(tmp_path)})
    monkeypatch.setattr(runtime_parity, "resolve_profiles_dir", lambda _app_root: profiles_dir)
    monkeypatch.setattr(runtime_parity, "storage_root", lambda _app_root: storage_dir)
    monkeypatch.setattr(runtime_parity, "_is_file_writable", lambda _path: True)
    monkeypatch.setattr(runtime_parity, "_is_valid_executable", lambda _path: True)
    monkeypatch.setattr(runtime_parity, "_resolve_browser_from_roots", lambda _app_root: (browser_path, tmp_path))
    monkeypatch.setattr(playwright_service, "resolve_playwright_executable", lambda **_kwargs: browser_path)

    report = runtime_parity.run_runtime_preflight("owner", sync_connected=True)

    assert report["connected_without_storage_state"] == ["acc-1"]
    assert report["disconnected_by_preflight"] == ["acc-1"]
    assert calls == [("acc-1", False, False)]


def test_bootstrap_application_sweeps_stale_runtime_states_at_boot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    accounts_file = _configure_accounts_storage(monkeypatch, tmp_path)
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "username": "acc-1",
                    "alias": "ventas",
                    "active": True,
                    "proxy_url": "http://proxy-a",
                }
            ]
        ),
        encoding="utf-8",
    )

    storage = InboxStorage(tmp_path)
    try:
        storage.upsert_runtime_alias_state(
            "ventas",
            {
                "is_running": True,
                "worker_state": "running",
                "current_account_id": "ghost-account",
                "next_account_id": "ghost-next",
                "current_turn_count": 2,
                "last_error": "",
            },
        )
        storage.upsert_session_connector_state(
            "acc-1",
            {
                "alias_id": "otro",
                "state": "ready",
                "proxy_key": "proxy-old",
                "last_error": "",
            },
        )
    finally:
        storage.shutdown()

    for env_name in (
        "INSTACRM_INSTALL_ROOT",
        "INSTACRM_APP_ROOT",
        "INSTACRM_DATA_ROOT",
        "INSTACRM_RUNTIME_ROOT",
        "INSTACRM_LOGS_ROOT",
        "INSTACRM_UPDATES_ROOT",
        "APP_DATA_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)

    monkeypatch.setattr(lifecycle, "_BOOTSTRAP_CACHE", {})
    monkeypatch.setattr(lifecycle, "_run_cleanup", lambda _ctx: {"runtime_removed": [], "update_removed": []})
    monkeypatch.setattr(lifecycle, "_recover_update_state", lambda _ctx: {"issues": [], "safe_mode_candidate": False})
    monkeypatch.setattr(lifecycle, "_validate_state", lambda _ctx: {"issues": [], "critical_count": 0, "warning_count": 0})
    monkeypatch.setattr(lifecycle, "emit_disk_warnings", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(lifecycle, "record_system_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lifecycle, "record_critical_error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lifecycle, "write_startup_diagnostic", lambda *_args, **_kwargs: tmp_path / "startup.json")
    monkeypatch.setattr(lifecycle, "build_support_diagnostic_bundle", lambda *_args, **_kwargs: tmp_path / "bundle.zip")
    monkeypatch.setattr(lifecycle, "update_local_heartbeat", lambda *_args, **_kwargs: None)

    fake_runtime_parity = ModuleType("runtime.runtime_parity")
    fake_runtime_parity.bootstrap_runtime_env = lambda *_args, **_kwargs: {}
    fake_runtime_parity.run_runtime_preflight = lambda *_args, **_kwargs: {
        "critical_count": 0,
        "warning_count": 0,
        "issues": [],
    }
    monkeypatch.setitem(sys.modules, "runtime.runtime_parity", fake_runtime_parity)

    ctx = lifecycle.bootstrap_application("owner", install_root_hint=tmp_path, force=True)

    reopened = InboxStorage(tmp_path)
    try:
        runtime_state = reopened.get_runtime_alias_state("ventas")
        connector_state = reopened.get_session_connector_state("acc-1")
    finally:
        reopened.shutdown()

    sweep_summary = ctx.cleanup_summary["boot_state_sweep"]
    assert sweep_summary["ran"] is True
    assert sweep_summary["runtime_alias_state"]["cleaned"] == 1
    assert sweep_summary["session_connector_state"]["cleaned"] == 1
    assert runtime_state["is_running"] is False
    assert runtime_state["worker_state"] == "stopped"
    assert runtime_state["current_account_id"] == ""
    assert runtime_state["next_account_id"] == ""
    assert runtime_state["current_turn_count"] == 0
    assert runtime_state["last_error"] == "boot_stale_runtime_cleaned"
    assert connector_state["state"] == "offline"
    assert connector_state["alias_id"] == "ventas"
    assert connector_state["proxy_key"] == "http://proxy-a"
    assert connector_state["last_error"] == "boot_stale_connector_cleaned"
