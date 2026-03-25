from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from bootstrap import lifecycle
from gui import gui_app


def _clear_bootstrap_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in (
        "INSTACRM_INSTALL_ROOT",
        "INSTACRM_APP_ROOT",
        "INSTACRM_DATA_ROOT",
        "INSTACRM_RUNTIME_ROOT",
        "INSTACRM_LOGS_ROOT",
        "INSTACRM_UPDATES_ROOT",
        "INSTACRM_BOOTSTRAPPED",
        "INSTACRM_BOOTSTRAP_MODE",
        "INSTACRM_STARTUP_ID",
        "APP_DATA_ROOT",
        "INSTACRM_FORCE_CLIENT_LAYOUT",
    ):
        monkeypatch.delenv(env_name, raising=False)


def _install_fake_runtime_parity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    calls: list[str] | None = None,
) -> None:
    fake_runtime_parity = ModuleType("runtime.runtime_parity")
    fake_runtime_parity.bootstrap_runtime_env = lambda *_args, **_kwargs: {}
    fake_runtime_parity.run_runtime_preflight_minimal = lambda *_args, **_kwargs: (
        calls.append("preflight_minimal") if calls is not None else None
    ) or {
        "phase": "pre_show_minimal",
        "critical_count": 0,
        "warning_count": 0,
        "issues": [],
        "report_path": "",
    }
    fake_runtime_parity.run_runtime_preflight = lambda *_args, **_kwargs: (
        calls.append("preflight_full") if calls is not None else None
    ) or {
        "phase": "post_show_full",
        "critical_count": 0,
        "warning_count": 0,
        "issues": [],
        "report_path": str(Path.cwd() / "runtime_preflight_report.json"),
    }
    monkeypatch.setitem(sys.modules, "runtime.runtime_parity", fake_runtime_parity)


def test_bootstrap_can_defer_cleanup_and_disk_warnings_until_post_show(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_bootstrap_env(monkeypatch)
    calls: list[str] = []
    _install_fake_runtime_parity(monkeypatch, calls=calls)

    monkeypatch.setattr(lifecycle, "_BOOTSTRAP_CACHE", {})
    monkeypatch.setattr(lifecycle, "_POST_SHOW_HOUSEKEEPING_STATUS", {})

    diagnostics: list[dict[str, object]] = []
    events: list[tuple[str, dict[str, object] | None]] = []

    monkeypatch.setattr(
        lifecycle,
        "_run_cleanup",
        lambda _ctx: calls.append("cleanup") or {"retention": {"ran": True}, "runtime_removed": ["tmp"], "update_removed": ["staged"]},
    )
    monkeypatch.setattr(
        lifecycle,
        "emit_disk_warnings",
        lambda *_args, **_kwargs: calls.append("disk") or ["Low disk space: 1.00 GB free"],
    )
    monkeypatch.setattr(lifecycle, "_recover_update_state", lambda _ctx: {"issues": [], "safe_mode_candidate": False})
    monkeypatch.setattr(
        lifecycle,
        "_run_boot_state_sweep",
        lambda _ctx: calls.append("sweep") or {
            "ran": True,
            "runtime_alias_state": {"checked": 0, "cleaned": 0, "deleted": 0, "details": []},
            "session_connector_state": {"checked": 0, "cleaned": 0, "deleted": 0, "details": []},
        },
    )
    monkeypatch.setattr(lifecycle, "_validate_state", lambda _ctx: {"issues": [], "critical_count": 0, "warning_count": 0})
    monkeypatch.setattr(
        lifecycle,
        "record_system_event",
        lambda root, event_type, **kwargs: events.append((event_type, kwargs.get("payload"))),
    )
    monkeypatch.setattr(lifecycle, "record_critical_error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "write_startup_diagnostic",
        lambda _root, payload: diagnostics.append(dict(payload)) or (tmp_path / "startup.json"),
    )
    monkeypatch.setattr(
        lifecycle,
        "build_support_diagnostic_bundle",
        lambda *_args, **_kwargs: tmp_path / "bundle.json",
    )
    monkeypatch.setattr(lifecycle, "update_local_heartbeat", lambda *_args, **_kwargs: tmp_path / "heartbeat.json")

    ctx = lifecycle.bootstrap_application(
        "owner",
        install_root_hint=tmp_path,
        force=True,
        defer_housekeeping=True,
    )

    assert calls == ["preflight_minimal"]
    assert ctx.cleanup_summary["deferred"] is True
    assert ctx.cleanup_summary["completed"] is False
    assert ctx.cleanup_summary["boot_state_sweep"]["status"] == "pending"
    assert ctx.preflight_summary["phase"] == "pre_show_minimal"
    assert ctx.preflight_summary["report_path"] == ""
    assert diagnostics == []

    result = lifecycle.run_post_show_bootstrap_tasks(ctx, time_to_window_show_seconds=1.25)

    assert calls == ["preflight_minimal", "cleanup", "disk", "sweep", "preflight_full"]
    assert result["status"] == "completed"
    assert result["disk_warnings"] == ["Low disk space: 1.00 GB free"]
    assert ctx.cleanup_summary["runtime_removed"] == ["tmp"]
    assert ctx.cleanup_summary["update_removed"] == ["staged"]
    assert ctx.cleanup_summary["disk_warnings"] == ["Low disk space: 1.00 GB free"]
    assert ctx.cleanup_summary["completed"] is True
    assert ctx.cleanup_summary["boot_state_sweep"]["ran"] is True
    assert ctx.preflight_summary["phase"] == "post_show_full"
    assert diagnostics[-1]["time_to_window_show_seconds"] == 1.25
    assert diagnostics[-1]["housekeeping_deferred"] is True
    assert diagnostics[-1]["housekeeping_completed"] is True
    assert any(event_type == "bootstrap_completed" for event_type, _payload in events)

    second = lifecycle.run_post_show_bootstrap_tasks(ctx)
    assert second["status"] == "completed"
    assert calls == ["preflight_minimal", "cleanup", "disk", "sweep", "preflight_full"]


@pytest.mark.parametrize("mode", ["owner", "client"])
def test_launch_gui_app_schedules_post_show_bootstrap_after_window_show(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: str,
) -> None:
    app = QApplication.instance() or QApplication([])
    order: list[str] = []
    ensure_calls: list[tuple[str | None, bool]] = []
    events: list[tuple[str, dict[str, object] | None]] = []

    fake_ctx = SimpleNamespace(
        install_root=tmp_path,
        startup_id=f"{mode}-startup",
        mode=mode,
    )

    class _FakeInbox:
        def __init__(self) -> None:
            self.ensure_started_calls = 0

        def ensure_started(self) -> None:
            self.ensure_started_calls += 1

    class _FakeServices:
        def __init__(self) -> None:
            self.inbox = _FakeInbox()

    services = _FakeServices()

    class _FakeLogs:
        def append(self, _message: str) -> None:
            return None

    created_windows: list[object] = []

    class _FakeWindow:
        def __init__(self, *, mode: str, services: object) -> None:
            self.mode = mode
            self.services = services
            self.logs = _FakeLogs()
            self.backend_exit_code = None
            self.show_called = False
            created_windows.append(self)

        def show(self) -> None:
            self.show_called = True
            order.append("show")

        def start_startup_housekeeping(self) -> None:
            order.append("window_housekeeping")
            app.quit()

    monkeypatch.setattr(
        gui_app,
        "ensure_bootstrapped",
        lambda requested_mode, defer_housekeeping=False: ensure_calls.append((requested_mode, defer_housekeeping)) or fake_ctx,
    )
    monkeypatch.setattr(gui_app, "bootstrap_runtime_env", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gui_app, "configure_logging", lambda _root: str(tmp_path / "app.log"))
    monkeypatch.setattr(gui_app, "install_exception_hooks", lambda **_kwargs: None)
    monkeypatch.setattr(gui_app, "MainWindow", _FakeWindow)
    monkeypatch.setattr(
        gui_app,
        "record_system_event",
        lambda _root, event_type, **kwargs: events.append((event_type, kwargs.get("payload"))),
    )
    monkeypatch.setattr(
        gui_app,
        "start_post_show_bootstrap_tasks",
        lambda ctx, time_to_window_show_seconds=None: order.append("post_show_bootstrap")
        or created_windows[0].show_called
        or True,
    )

    import application.services as services_module

    monkeypatch.setattr(services_module, "build_application_services", lambda _root: services)

    exit_code = gui_app.launch_gui_app(mode=mode)

    assert exit_code == 0
    assert ensure_calls == [(mode, True)]
    assert services.inbox.ensure_started_calls == 0
    assert "show" in order
    assert "post_show_bootstrap" in order
    assert order.index("show") < order.index("post_show_bootstrap") < order.index("window_housekeeping")
    shown_events = [payload for event_type, payload in events if event_type == "gui_window_shown"]
    assert len(shown_events) == 1
    assert int(shown_events[0]["time_to_window_show_ms"]) >= 0
