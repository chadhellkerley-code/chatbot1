from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from application.services import build_application_services
from gui.main_window import MainWindow
from gui.modules.leads.leads_page import LeadsHomePage
from gui.page_base import GuiState, PageContext
from gui.pages_automation_flow import AutomationFlowPage
from gui.pages_system import SystemConfigPage, SystemDiagnosticsPage
from gui.query_runner import QueryManager
from gui.task_runner import LogStore


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


def _wait_until(predicate, *, timeout: float = 1.5, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.1, float(timeout or 0.1))
    while time.time() < deadline:
        if predicate():
            return True
        _pump_events(2)
        time.sleep(max(0.005, float(interval or 0.005)))
    _pump_events(4)
    return bool(predicate())


class _StaticTasks:
    def running_tasks(self) -> list[str]:
        return ["diag-task"]

    def is_running(self, name: str) -> bool:
        return False


class _FakeLeadsService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []

    def list_templates(self) -> list[dict[str, str]]:
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.15)
        return [{"name": "tmpl-1", "text": "hola"}]

    def list_lists(self) -> list[str]:
        return ["seed-list", "second-list"]


class _FakeSystemService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []

    def dashboard_snapshot(self) -> dict[str, Any]:
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.15)
        return {"metrics": {"connected_accounts": 5}}


class _FakeInboxDiagnosticsService:
    def diagnostics(self) -> dict[str, Any]:
        return {"worker_count": 2, "queued_tasks": 3, "dedupe_pending": 1}


class _FakeAutomationAccountsService:
    def list_aliases(self) -> list[str]:
        return ["default", "sales"]


class _FakeAutomationService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []

    def list_packs(self) -> list[dict[str, Any]]:
        return [{"id": "pack_1", "name": "Pack 1"}]

    def get_flow_config(self, alias: str) -> dict[str, Any]:
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.15)
        return {
            "version": 1,
            "entry_stage_id": "stage_1",
            "stages": [
                {
                    "id": "stage_1",
                    "action_type": "mensaje",
                    "transitions": {
                        "positive": "stage_1",
                        "negative": "stage_1",
                        "doubt": "stage_1",
                        "neutral": "stage_1",
                    },
                    "followups": [],
                    "post_objection": {"enabled": False},
                }
            ],
            "allow_empty": False,
            "layout": {
                "nodes": {"stage_1": {"x": 120.0, "y": 120.0}},
                "viewport": {"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0},
            },
            "alias": alias,
        }


def _build_ctx(*, services: Any, tasks: Any | None = None) -> tuple[PageContext, QueryManager]:
    queries = QueryManager()
    ctx = PageContext(
        services=services,
        tasks=tasks or _StaticTasks(),
        logs=LogStore(),
        queries=queries,
        state=GuiState(),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries


def test_leads_home_navigation_requests_background_snapshot_without_blocking() -> None:
    _app()
    services = SimpleNamespace(leads=_FakeLeadsService())
    ctx, queries = _build_ctx(services=services)
    page = LeadsHomePage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._cards["templates"]._value.text() == "1")
        assert services.leads.thread_ids
        assert all(thread_id != threading.get_ident() for thread_id in services.leads.thread_ids)
    finally:
        queries.shutdown()


def test_system_diagnostics_refresh_runs_off_main_thread(monkeypatch, tmp_path: Path) -> None:
    _app()
    monkeypatch.setattr("gui.snapshot_queries.list_saved_sessions", lambda: ["a.session", "b.session"])
    monkeypatch.setattr("gui.snapshot_queries.resolve_playwright_executable", lambda headless=True: "pw.exe")

    services = SimpleNamespace(
        system=_FakeSystemService(),
        inbox=_FakeInboxDiagnosticsService(),
        context=SimpleNamespace(root_dir=tmp_path),
    )
    ctx, queries = _build_ctx(services=services)
    page = SystemDiagnosticsPage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._table.rowCount() == 5)
        assert '"accounts_active": 5' in page._raw.toPlainText()
        assert services.system.thread_ids
        assert all(thread_id != threading.get_ident() for thread_id in services.system.thread_ids)
    finally:
        page.on_navigate_from()
        queries.shutdown()


def test_system_config_page_renders_update_check_contract_without_nested_result() -> None:
    _app()
    services = SimpleNamespace(system=SimpleNamespace())
    ctx, queries = _build_ctx(services=services)
    page = SystemConfigPage(ctx)
    try:
        page._updates_request_id = 7
        page._updates_loading = True

        payload = {
            "status": "up_to_date",
            "checked": True,
            "update_available": False,
            "message": "Ya tienes la versión más reciente (1.0.0).",
            "current_version": "1.0.0",
            "latest_version": "1.0.0",
            "update_info": {"version": "1.0.0"},
            "github_repo": "demo/repo",
        }

        page._on_updates_loaded(7, payload)

        rendered = page._updates_box.toPlainText()
        assert '"status": "up_to_date"' in rendered
        assert '"result"' not in rendered
        assert page._status_label.text() == "Ya tienes la versión más reciente (1.0.0)."
    finally:
        queries.shutdown()


def test_automation_flow_loads_snapshot_off_main_thread() -> None:
    _app()
    services = SimpleNamespace(
        automation=_FakeAutomationService(),
        accounts=_FakeAutomationAccountsService(),
    )
    ctx, queries = _build_ctx(services=services)
    page = AutomationFlowPage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._table.rowCount() == 1)
        assert page._pack_options == ["pack_1"]
        assert services.automation.thread_ids
        assert all(thread_id != threading.get_ident() for thread_id in services.automation.thread_ids)
    finally:
        queries.shutdown()


def test_main_window_creates_pages_lazily() -> None:
    _app()
    services = build_application_services(ROOT)
    window = MainWindow(mode="owner", services=services)
    try:
        assert set(window._created_pages) == {"dashboard"}
        assert "accounts_home" not in window._created_pages

        window.open_route("accounts_home")
        created_page = window._created_pages["accounts_home"]

        window.open_route("dashboard")
        window.open_route("accounts_home")

        assert window._created_pages["accounts_home"] is created_page
    finally:
        window.close()
