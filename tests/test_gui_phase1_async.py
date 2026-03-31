from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from application.services.base import ServiceContext
from application.services.inbox_service import InboxService
from gui.page_base import GuiState, PageContext
from gui.pages_accounts import AccountsPage
from gui.pages_campaigns import CampaignCreatePage
from gui.pages_campaigns import CampaignMonitorPage
from gui.pages_dashboard import DashboardPage
from gui.query_runner import QueryManager
from gui.task_runner import LogStore, TaskManager


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


class _SimpleTasks:
    def is_running(self, name: str) -> bool:
        return str(name or "").strip() == "campaign"


class _FakeDashboardSystem:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []

    def dashboard_snapshot(self) -> dict[str, Any]:
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.15)
        return {
            "metrics": {
                "total_accounts": 3,
                "connected_accounts": 2,
                "messages_sent_today": 11,
                "messages_error_today": 1,
                "messages_replied_today": 4,
            },
            "conversion": {"rate": 55.0},
            "timezone_label": "UTC",
            "last_reset_display": "08:00",
        }


class _FakeDashboardServices:
    def __init__(self) -> None:
        self.system = _FakeDashboardSystem()


class _FakeAccountsService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []
        self.login_calls: list[dict[str, Any]] = []
        self.relogin_calls: list[dict[str, Any]] = []
        self._rows = [
            {"username": "uno", "assigned_proxy_id": "px-1", "messages_per_account": 20},
            {"username": "dos", "proxy_url": "http://proxy:8080", "messages_per_account": 15},
        ]

    def list_aliases(self) -> list[str]:
        return ["default"]

    def list_accounts(self, alias: str | None = None) -> list[dict[str, Any]]:
        del alias
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.15)
        return [dict(row) for row in self._rows]

    def connected_status(self, record: dict[str, Any]) -> bool:
        return str(record.get("username") or "") == "uno"

    def health_badge(self, record: dict[str, Any]) -> str:
        return "ok" if self.connected_status(record) else "review"

    def import_accounts_csv(
        self,
        alias: str,
        path: str,
        *,
        login_after_import: bool = False,
        concurrency: int = 1,
    ) -> dict[str, Any]:
        del path, login_after_import, concurrency
        self._rows.append({"username": "tres", "assigned_proxy_id": "", "messages_per_account": 10, "alias": alias})
        return {
            "alias": alias,
            "added": 1,
            "skipped": 0,
            "imported_usernames": ["tres"],
            "login_usernames": ["tres"],
            "login_results": [],
        }

    def login(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        self.login_calls.append(
            {
                "alias": alias,
                "usernames": list(usernames or []),
                "concurrency": concurrency,
            }
        )
        return [{"ok": True}]

    def relogin(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        self.relogin_calls.append(
            {
                "alias": alias,
                "usernames": list(usernames or []),
                "concurrency": concurrency,
            }
        )
        return [{"ok": True}]

    def remove_accounts(self, usernames: list[str]) -> int:
        selected = {item.lower() for item in usernames}
        before = len(self._rows)
        self._rows = [row for row in self._rows if str(row.get("username") or "").lower() not in selected]
        return before - len(self._rows)

    def set_message_limit(self, usernames: list[str], limit: int) -> int:
        selected = {item.lower() for item in usernames}
        updated = 0
        for row in self._rows:
            if str(row.get("username") or "").lower() in selected:
                row["messages_per_account"] = limit
                updated += 1
        return updated


class _FakeAccountsServices:
    def __init__(self) -> None:
        self.accounts = _FakeAccountsService()


class _FakeCampaignAccounts:
    def __init__(self) -> None:
        self.list_aliases_thread_ids: list[int] = []

    def list_aliases(self) -> list[str]:
        self.list_aliases_thread_ids.append(threading.get_ident())
        time.sleep(0.1)
        return ["default"]


class _FakeCampaignLeads:
    def __init__(self) -> None:
        self.list_calls: list[int] = []
        self.summary_calls: list[int] = []
        self.load_calls: list[tuple[int, str]] = []
<<<<<<< HEAD
        self.summary_count = 5
=======
>>>>>>> origin/main

    def list_lists(self) -> list[str]:
        self.list_calls.append(threading.get_ident())
        time.sleep(0.1)
        return ["lead-list"]

    def list_list_summaries(self) -> list[dict[str, Any]]:
        self.summary_calls.append(threading.get_ident())
        time.sleep(0.1)
<<<<<<< HEAD
        return [{"name": "lead-list", "count": self.summary_count}]
=======
        return [{"name": "lead-list", "count": 5}]
>>>>>>> origin/main

    def load_list(self, alias: str) -> list[dict[str, Any]]:
        self.load_calls.append((threading.get_ident(), str(alias or "")))
        time.sleep(0.1)
<<<<<<< HEAD
        return [{"username": "lead-1"} for _ in range(self.summary_count)]
=======
        return [{"username": "lead-1"} for _ in range(5)]
>>>>>>> origin/main


class _FakeCampaignService:
    def __init__(self) -> None:
        self.template_thread_ids: list[int] = []
        self.capacity_thread_ids: list[int] = []
        self.launch_calls: list[dict[str, Any]] = []
        self.stop_reasons: list[str] = []
        self.launch_error: Exception | None = None
        self.hold_task_open = False
        self.task_started = threading.Event()
        self.task_release = threading.Event()
<<<<<<< HEAD
        self.workers_capacity = 3
        self.remaining_slots_total = 5
        self.planned_eligible_leads = 5
        self.planned_runnable_leads = 5
=======
>>>>>>> origin/main
        self._current_run = {
            "run_id": "run-1",
            "alias": "default",
            "leads_alias": "lead-list",
            "sent": 4,
            "failed": 1,
            "skipped": 0,
            "retried": 0,
            "remaining": 6,
            "total_leads": 10,
            "workers_active": 1,
            "workers_requested": 2,
            "workers_capacity": 3,
            "workers_effective": 2,
            "active_accounts": 2,
            "worker_rows": [
                {
                    "worker_id": "worker-1",
                    "proxy_label": "proxy-1",
                    "execution_state": "running",
                    "execution_stage": "sending",
                    "current_account": "uno",
                    "current_lead": "lead-1",
                    "restarts": 0,
                }
            ],
            "started_at": "2026-03-08T10:00:00",
            "finished_at": "",
            "message": "Procesando campaña.",
            "status": "Running",
            "task_active": True,
        }

    def list_templates(self) -> list[dict[str, str]]:
        self.template_thread_ids.append(threading.get_ident())
        time.sleep(0.1)
        return [{"name": "Hola", "text": "Hola {{username}}", "id": "tpl-1"}]

<<<<<<< HEAD
    def get_capacity(
        self,
        alias: str,
        *,
        leads_alias: str = "",
        workers_requested: int = 0,
    ) -> dict[str, Any]:
=======
    def get_capacity(self, alias: str) -> dict[str, Any]:
>>>>>>> origin/main
        self.capacity_thread_ids.append(threading.get_ident())
        time.sleep(0.1)
        return {
            "alias": str(alias or ""),
<<<<<<< HEAD
            "leads_alias": str(leads_alias or ""),
            "workers_capacity": self.workers_capacity,
            "workers_requested": max(0, int(workers_requested or 0)),
            "workers_effective": (
                min(self.workers_capacity, max(0, int(workers_requested or 0)))
                if workers_requested
                else self.workers_capacity
            ),
            "proxies": [],
            "has_none_accounts": True,
            "remaining_slots_total": self.remaining_slots_total,
            "planned_eligible_leads": self.planned_eligible_leads if leads_alias else 0,
            "planned_runnable_leads": self.planned_runnable_leads if leads_alias else 0,
            "account_remaining": [
                {
                    "username": "acct-1",
                    "remaining": self.remaining_slots_total,
                    "sent_today": 0,
                    "limit": self.remaining_slots_total,
                },
            ],
=======
            "workers_capacity": 3,
            "proxies": [],
            "has_none_accounts": True,
>>>>>>> origin/main
        }

    def build_template_entries(
        self,
        *,
        use_saved_template: str = "",
        manual_message: str = "",
    ) -> list[dict[str, str]]:
        if use_saved_template:
            return [{"name": use_saved_template, "text": "Hola {{username}}", "id": "tpl-1"}]
        clean_manual = str(manual_message or "").strip()
        return [{"name": "", "text": clean_manual, "id": "manual"}] if clean_manual else []

    def _launch_payload(self, request: Any) -> dict[str, Any]:
        if hasattr(request, "to_payload"):
            return dict(request.to_payload())
        return dict(request or {})

    def launch_campaign(self, request: Any, *, task_runner: Any) -> dict[str, Any]:
        config = self._launch_payload(request)
        self.launch_calls.append(dict(config))
        if self.launch_error is not None:
            raise self.launch_error
<<<<<<< HEAD
        workers_requested = int(config.get("workers_requested") or 1)
        workers_capacity = int(
            self.get_capacity(
                str(config.get("alias") or ""),
                leads_alias=str(config.get("leads_alias") or ""),
                workers_requested=workers_requested,
            ).get("workers_capacity")
            or 0
        )
=======
        workers_capacity = int(self.get_capacity(str(config.get("alias") or "")).get("workers_capacity") or 0)
        workers_requested = int(config.get("workers_requested") or 1)
>>>>>>> origin/main
        workers_effective = min(workers_requested, workers_capacity) if workers_capacity > 0 else 0
        started_at = str(config.get("started_at") or "2026-03-08T10:00:01")
        run_id = str(config.get("run_id") or f"run-{len(self.launch_calls)}")
        self._current_run = {
            "run_id": run_id,
            "alias": str(config.get("alias") or ""),
            "leads_alias": str(config.get("leads_alias") or ""),
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "retried": 0,
            "remaining": int(config.get("total_leads") or 0),
            "total_leads": int(config.get("total_leads") or 0),
            "workers_active": 0,
            "workers_requested": workers_requested,
            "workers_capacity": workers_capacity,
            "workers_effective": workers_effective,
            "worker_rows": [],
            "started_at": started_at,
            "finished_at": "",
            "message": "Campaña iniciada.",
            "status": "Starting",
            "task_active": True,
        }
        start_task = getattr(task_runner, "start_task", None)
        if callable(start_task):
            task_started = self.task_started
            task_release = self.task_release
            hold_task_open = self.hold_task_open

            def _task() -> None:
                task_started.set()
                if hold_task_open:
                    task_release.wait(1.0)

            start_task(
                "campaign",
                _task,
                metadata={
                    "alias": str(config.get("alias") or ""),
                    "run_id": run_id,
                },
            )
        return dict(self._current_run)

    def current_run_snapshot(self, *, run_id: str = "") -> dict[str, Any]:
        if run_id and run_id != str(self._current_run.get("run_id") or ""):
            return {}
        return dict(self._current_run)

    def stop_campaign(self, reason: str) -> None:
        self.stop_reasons.append(str(reason or ""))


class _FakeCampaignServices:
    def __init__(self) -> None:
        self.accounts = _FakeCampaignAccounts()
        self.leads = _FakeCampaignLeads()
        self.campaigns = _FakeCampaignService()


class _CountingLogStore(LogStore):
    def __init__(self) -> None:
        super().__init__()
        self.text_calls = 0

    def text(self) -> str:
        self.text_calls += 1
        return super().text()


class _FakeCampaignTasks(QObject):
    taskFinished = Signal(str, bool, str)

    def __init__(self) -> None:
        super().__init__()
        self.stop_reasons: list[str] = []

    def is_running(self, name: str) -> bool:
        return str(name or "").strip() == "campaign"

    def request_stop(self, reason: str) -> None:
        self.stop_reasons.append(str(reason or ""))


class _FakeInboxEngine:
    def __init__(self, _root_dir: Path) -> None:
        self.start_calls = 0
        self.shutdown_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def shutdown(self) -> None:
        self.shutdown_calls += 1


def _build_ctx(
    *,
    services: Any,
    tasks: Any,
    logs: LogStore | None = None,
    open_route=None,
) -> tuple[PageContext, QueryManager]:
    queries = QueryManager()
    ctx = PageContext(
        services=services,
        tasks=tasks,
        logs=logs or LogStore(),
        queries=queries,
        state=GuiState(),
        open_route=open_route or (lambda route, payload=None: None),
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries


def test_query_manager_executes_snapshot_off_main_thread() -> None:
    _app()
    manager = QueryManager()
    try:
        results: list[int] = []
        main_thread = threading.get_ident()
        manager.submit(lambda: threading.get_ident(), on_success=lambda _request_id, payload: results.append(int(payload)))
        assert _wait_until(lambda: bool(results))
        assert results[0] != main_thread
    finally:
        manager.shutdown()


def test_dashboard_navigation_requests_background_snapshot_without_blocking() -> None:
    _app()
    services = _FakeDashboardServices()
    ctx, queries = _build_ctx(services=services, tasks=_SimpleTasks())
    page = DashboardPage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._cards["total_accounts"]._value.text() == "3")
        assert services.system.thread_ids
        assert all(thread_id != threading.get_ident() for thread_id in services.system.thread_ids)
    finally:
        page.on_navigate_from()
        queries.shutdown()


def test_accounts_page_refreshes_async_and_keeps_search_local() -> None:
    _app()
    services = _FakeAccountsServices()
    logs = LogStore()
    tasks = TaskManager(logs)
    ctx, queries = _build_ctx(services=services, tasks=tasks, logs=logs)
    page = AccountsPage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._table.rowCount() == 2)
        assert services.accounts.thread_ids
        assert all(thread_id != threading.get_ident() for thread_id in services.accounts.thread_ids)

        page._search_input.setText("dos")
        _pump_events(4)
        visible_rows = sum(1 for row in range(page._table.rowCount()) if not page._table.isRowHidden(row))
        assert visible_rows == 1
    finally:
        queries.shutdown()


def test_accounts_import_refreshes_table_before_optional_login() -> None:
    _app()
    services = _FakeAccountsServices()
    logs = LogStore()
    tasks = TaskManager(logs)
    ctx, queries = _build_ctx(services=services, tasks=tasks, logs=logs)
    page = AccountsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._table.rowCount() == 2)
        assert page._login_queue_mode.isReadOnly() is True
        assert page._login_queue_mode.text() == "Secuencial"

        page._pending_import_request = {
            "alias": "default",
            "login_after_import": True,
            "concurrency": 3,
        }
        services.accounts.import_accounts_csv("default", "accounts.csv")
        page._on_task_completed(
            "accounts_import",
            True,
            "",
            {
                "alias": "default",
                "added": 1,
                "skipped": 0,
                "imported_usernames": ["tres"],
                "login_usernames": ["tres"],
                "login_results": [],
            },
        )

        assert _wait_until(lambda: page._table.rowCount() == 3)
        assert _wait_until(lambda: bool(services.accounts.login_calls))
        assert services.accounts.login_calls[0] == {
            "alias": "default",
            "usernames": ["tres"],
            "concurrency": 1,
        }
        assert any(page._table.item(row, 0).text() == "@tres" for row in range(page._table.rowCount()))
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_accounts_page_uses_relogin_contract_when_forced() -> None:
    _app()
    services = _FakeAccountsServices()
    logs = LogStore()
    tasks = TaskManager(logs)
    ctx, queries = _build_ctx(services=services, tasks=tasks, logs=logs)
    page = AccountsPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._table.rowCount() == 2)

        page._force_relogin.setChecked(True)
        page._run_login(usernames=["uno"], alias="default")

        assert _wait_until(lambda: bool(services.accounts.relogin_calls))
        assert services.accounts.relogin_calls[0]["usernames"] == ["uno"]
        assert services.accounts.login_calls == []
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_campaign_monitor_appends_logs_incrementally() -> None:
    _app()
    logs = _CountingLogStore()
    tasks = _FakeCampaignTasks()
    ctx, queries = _build_ctx(services=_FakeCampaignServices(), tasks=tasks, logs=logs)
    ctx.state.campaign_monitor = {
        "run_id": "run-1",
        "alias": "default",
        "leads_alias": "lead-list",
        "total_leads": 10,
        "log_cursor_start": 0,
    }
    page = CampaignMonitorPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._sent_value.text() == "4")
        assert logs.text_calls == 0

        logs.append("uno\n")
        _pump_events(4)
        assert page._logs.toPlainText() == "uno\n"

        page.refresh_monitor()
        _pump_events(4)
        logs.append("dos\n")
        _pump_events(4)

        assert page._logs.toPlainText().endswith("uno\ndos\n")
        assert logs.text_calls == 0
    finally:
        page.on_navigate_from()
        queries.shutdown()


def test_campaign_create_page_loads_form_async_and_starts_without_sync_reads() -> None:
    _app()
    services = _FakeCampaignServices()
    logs = LogStore()
    tasks = TaskManager(logs)
    route_calls: list[tuple[str, Any]] = []
    ctx, queries = _build_ctx(
        services=services,
        tasks=tasks,
        logs=logs,
        open_route=lambda route, payload=None: route_calls.append((route, payload)),
    )
    page = CampaignCreatePage(ctx)
    try:
        started = time.perf_counter()
        page.on_navigate_to()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.08
        assert _wait_until(lambda: page._alias_combo.count() == 1)
        assert _wait_until(lambda: bool(services.campaigns.capacity_thread_ids))
        assert all(thread_id != threading.get_ident() for thread_id in services.accounts.list_aliases_thread_ids)
        assert all(thread_id != threading.get_ident() for thread_id in services.leads.summary_calls)
        assert all(thread_id != threading.get_ident() for thread_id in services.campaigns.template_thread_ids)
        assert all(thread_id != threading.get_ident() for thread_id in services.campaigns.capacity_thread_ids)
        assert services.leads.load_calls == []
<<<<<<< HEAD
        assert _wait_until(lambda: "Cupo restante hoy: 5" in page._capacity_label.text())
        assert "Leads ejecutables: 5" in page._capacity_label.text()
=======
>>>>>>> origin/main

        load_calls_before = len(services.leads.load_calls)
        page._template_combo.setCurrentIndex(1)
        assert page._template_combo.currentData() == "tpl-1"
        assert "Hola {{username}}" in page._template_preview.toPlainText()
        page._start_campaign()

        assert route_calls
        assert route_calls[0][0] == "campaign_monitor_page"
        assert len(services.leads.load_calls) == load_calls_before
        assert services.campaigns.launch_calls[0]["total_leads"] == 5
        assert services.campaigns.launch_calls[0]["workers_capacity"] == 0
        assert services.campaigns.launch_calls[0]["templates"][0]["id"] == "tpl-1"
        assert route_calls[0][1]["workers_requested"] == 1
        assert route_calls[0][1]["workers_capacity"] == 3
        assert route_calls[0][1]["workers_effective"] == 1
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


<<<<<<< HEAD
def test_campaign_create_page_uses_planned_queue_total_in_summary_and_launch() -> None:
    _app()
    services = _FakeCampaignServices()
    services.leads.summary_count = 45
    services.campaigns.remaining_slots_total = 16
    services.campaigns.planned_eligible_leads = 18
    services.campaigns.planned_runnable_leads = 16
    logs = LogStore()
    tasks = TaskManager(logs)
    route_calls: list[tuple[str, Any]] = []
    ctx, queries = _build_ctx(
        services=services,
        tasks=tasks,
        logs=logs,
        open_route=lambda route, payload=None: route_calls.append((route, payload)),
    )
    page = CampaignCreatePage(ctx)
    try:
        page.on_navigate_to()

        assert _wait_until(lambda: page._alias_combo.count() == 1)
        assert _wait_until(lambda: page._summary_values["count"].text() == "16")
        assert "Leads elegibles: 18" in page._capacity_label.text()
        assert "Leads ejecutables: 16" in page._capacity_label.text()

        page._template_combo.setCurrentIndex(1)
        page._start_campaign()

        assert route_calls
        assert services.campaigns.launch_calls[0]["total_leads"] == 16
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


=======
>>>>>>> origin/main
def test_campaign_create_page_does_not_open_monitor_when_launch_fails() -> None:
    _app()
    services = _FakeCampaignServices()
    services.campaigns.launch_error = RuntimeError("No se pudo iniciar la campana.")
    logs = LogStore()
    tasks = TaskManager(logs)
    route_calls: list[tuple[str, Any]] = []
    ctx, queries = _build_ctx(
        services=services,
        tasks=tasks,
        logs=logs,
        open_route=lambda route, payload=None: route_calls.append((route, payload)),
    )
    page = CampaignCreatePage(ctx)
    errors: list[str] = []
    try:
        page.show_error = lambda text: errors.append(str(text))
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 1)
        page._template_combo.setCurrentIndex(1)

        page._start_campaign()

        assert errors == ["No se pudo iniciar la campana."]
        assert route_calls == []
        assert ctx.state.campaign_monitor == {}
        assert page._start_button.isEnabled() is True
        assert page._start_button.text() == "INICIAR CAMPAÑA"
    finally:
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_campaign_create_page_blocks_double_start_while_task_is_running() -> None:
    _app()
    services = _FakeCampaignServices()
    services.campaigns.hold_task_open = True
    logs = LogStore()
    tasks = TaskManager(logs)
    ctx, queries = _build_ctx(
        services=services,
        tasks=tasks,
        logs=logs,
    )
    page = CampaignCreatePage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 1)
        page._template_combo.setCurrentIndex(1)

        page._start_campaign()

        assert services.campaigns.task_started.wait(1.0)
        assert _wait_until(lambda: tasks.is_running("campaign"))
        assert page._start_button.isEnabled() is False
        assert page._start_button.text() == "CAMPAÑA EN CURSO"

        page._start_campaign()

        assert len(services.campaigns.launch_calls) == 1
    finally:
        services.campaigns.task_release.set()
        queries.shutdown()
        tasks.shutdown("test cleanup")


def test_campaign_monitor_keeps_page_open_when_task_finishes() -> None:
    _app()
    tasks = _FakeCampaignTasks()
    route_calls: list[tuple[str, Any]] = []
    ctx, queries = _build_ctx(
        services=_FakeCampaignServices(),
        tasks=tasks,
        logs=LogStore(),
        open_route=lambda route, payload=None: route_calls.append((route, payload)),
    )
    ctx.state.campaign_monitor = {"run_id": "run-1", "log_cursor_start": 0}
    page = CampaignMonitorPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._status_value.text() == "Running")

        tasks.taskFinished.emit("campaign", True, "")
        _pump_events(4)

        assert route_calls == []
    finally:
        page.on_navigate_from()
        queries.shutdown()


def test_inbox_service_initialization_defers_projection_bootstrap(tmp_path: Path, monkeypatch) -> None:
    calls: list[int] = []

    def _mark_build_rows(self, live_rows):  # noqa: ANN001
        del self, live_rows
        calls.append(threading.get_ident())
        return []

    monkeypatch.setattr("application.services.inbox_service.InboxEngine", _FakeInboxEngine)
    monkeypatch.setattr("application.services.inbox_runtime.InboxProjectionBuilder.build_rows", _mark_build_rows)

    started = time.perf_counter()
    service = InboxService(ServiceContext(root_dir=tmp_path))
    elapsed = time.perf_counter() - started
    try:
        assert elapsed < 0.08
        assert calls == []
        assert service.projection_ready() is False
        assert service._engine.start_calls == 0
    finally:
        service.shutdown()
