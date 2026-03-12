from __future__ import annotations

import os
import threading
import time
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog

from gui.page_base import GuiState, PageContext
from gui.pages_automation import AutomationAutoresponderPage, AutomationConfigPage
from gui.query_runner import QueryManager
from gui.task_runner import LogStore, TaskManager, _SignalWriter


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.1, float(timeout))
    while time.time() < deadline:
        if predicate():
            return True
        _pump_events(2)
        time.sleep(interval)
    _pump_events(4)
    return bool(predicate())


class _FakeAccountsService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []

    def list_aliases(self) -> list[str]:
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.05)
        return ["default", "ventas"]

    def list_accounts(self, alias: str | None = None) -> list[dict[str, Any]]:
        self.thread_ids.append(threading.get_ident())
        rows = [
            {"username": "cuenta_1", "alias": "default", "active": True},
            {"username": "cuenta_2", "alias": "default", "active": True},
            {"username": "cuenta_3", "alias": "ventas", "active": True},
        ]
        if alias is None:
            return rows
        return [row for row in rows if row["alias"] == alias]


class _FakeAutomationService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []
        self.saved_followup: list[dict[str, Any]] = []
        self.saved_prompts: list[tuple[str, str]] = []
        self.start_calls: list[dict[str, Any]] = []
        self.stop_reasons: list[str] = []
        self.task_active = False
        self.current_alias = "default"

    def load_openai_api_key(self) -> str:
        self.thread_ids.append(threading.get_ident())
        return "sk-test"

    def save_openai_api_key(self, value: str) -> str:
        return value

    def list_objection_prompts(self) -> list[dict[str, str]]:
        self.thread_ids.append(threading.get_ident())
        return [{"name": "ventas", "content": "Prompt comercial"}]

    def save_objection_prompt(self, name: str, content: str) -> dict[str, str]:
        self.saved_prompts.append((name, content))
        return {"name": name, "content": content}

    def delete_objection_prompt(self, name: str) -> int:
        return 1

    def get_prompt_entry(self, alias: str) -> dict[str, Any]:
        return {"alias": alias, "objection_strategy_name": "ventas", "objection_prompt": "Prompt comercial"}

    def save_prompt_entry(self, alias: str, updates: dict[str, Any]) -> dict[str, Any]:
        return {"alias": alias, **updates}

    def get_followup_entry(self, alias: str) -> dict[str, Any]:
        return {"alias": alias, "enabled": True, "accounts": ["cuenta_1"]}

    def save_followup_entry(self, alias: str, updates: dict[str, Any]) -> dict[str, Any]:
        return {"alias": alias, **updates}

    def get_followup_account_selection(self, alias: str) -> dict[str, Any]:
        return {
            "mode": "individual",
            "selected_aliases": [],
            "selected_accounts": ["cuenta_1"],
            "effective_accounts": ["cuenta_1"],
            "enabled": True,
        }

    def save_followup_account_selection(
        self,
        alias: str,
        *,
        mode: str,
        selected_aliases: list[str] | None = None,
        selected_accounts: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "alias": alias,
            "mode": mode,
            "selected_aliases": list(selected_aliases or []),
            "selected_accounts": list(selected_accounts or []),
        }
        self.saved_followup.append(payload)
        return payload

    def alias_account_rows(self, alias: str) -> list[dict[str, Any]]:
        self.thread_ids.append(threading.get_ident())
        rows = {
            "default": [
                {"username": "cuenta_1", "proxy": "proxy-a", "connected": True},
                {"username": "cuenta_2", "proxy": "proxy-b", "connected": True},
            ],
            "ventas": [
                {"username": "cuenta_3", "proxy": "", "connected": False},
            ],
        }
        return list(rows.get(alias, []))

    def max_alias_concurrency(self, alias: str) -> int:
        return 2 if alias == "default" else 1

    def autoresponder_snapshot(self, alias: str) -> dict[str, Any]:
        self.thread_ids.append(threading.get_ident())
        self.current_alias = alias
        time.sleep(0.05)
        return {
            "run_id": "auto-1",
            "alias": alias,
            "status": "Running" if self.task_active else "Idle",
            "message": "Monitor activo.",
            "started_at": "2026-03-08T11:00:00",
            "finished_at": "2026-03-08T11:05:00" if not self.task_active else "",
            "delay_min": 30,
            "delay_max": 60,
            "concurrency": 2,
            "threads": 12,
            "followup_only": True,
            "followup_schedule_label": "4, 24",
            "accounts_total": 2 if alias == "default" else 1,
            "task_active": self.task_active,
            "message_success": 7,
            "message_failed": 1,
            "followup_success": 3,
            "followup_failed": 1,
            "agendas_generated": 2,
            "account_rows": [
                {"account": "cuenta_1", "proxy": "proxy-a", "blocked": False},
                {"account": "cuenta_2", "proxy": "proxy-b", "blocked": False},
            ]
            if alias == "default"
            else [{"account": "cuenta_3", "proxy": "", "blocked": False}],
        }

    def start_autoresponder(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.start_calls.append(dict(payload))
        self.task_active = True
        time.sleep(0.05)
        self.task_active = False
        return self.autoresponder_snapshot(str(payload.get("alias") or "default"))

    def stop_autoresponder(self, reason: str) -> None:
        self.stop_reasons.append(reason)


def _build_ctx() -> tuple[PageContext, QueryManager]:
    logs = LogStore()
    tasks = TaskManager(logs)
    queries = QueryManager()
    services = SimpleNamespace(
        accounts=_FakeAccountsService(),
        automation=_FakeAutomationService(),
    )
    ctx = PageContext(
        services=services,
        tasks=tasks,
        logs=logs,
        queries=queries,
        state=GuiState(active_alias="default"),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries


def test_automation_config_page_uses_async_snapshot_and_structured_followup_selection(monkeypatch):
    _app()
    ctx, queries = _build_ctx()
    page = AutomationConfigPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 2)
        assert page._prompt_selector.count() >= 2
        assert page._follow_mode.count() == 3
        assert page._api_summary.text() == "API Key configurada."
        main_thread = threading.get_ident()
        assert main_thread not in ctx.services.accounts.thread_ids
        page._follow_mode.setCurrentIndex(page._follow_mode.findData("alias"))
        page._follow_aliases.item(0).setCheckState(Qt.Checked)
        page._save_followup_selection()
        assert ctx.services.automation.saved_followup[-1]["mode"] == "alias"
    finally:
        page.close()
        queries.shutdown()


def test_automation_config_page_imports_prompt_txt(monkeypatch, tmp_path):
    from gui import automation_pages_base as automation_base_module

    _app()
    prompt_path = tmp_path / "objecion_ventas.txt"
    prompt_path.write_text("Prompt importado desde TXT", encoding="utf-8")
    monkeypatch.setattr(
        automation_base_module,
        "open_automation_file_dialog",
        lambda *args, **kwargs: str(prompt_path),
    )

    ctx, queries = _build_ctx()
    page = AutomationConfigPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 2)
        page._new_prompt()
        page._import_prompt_txt()
        assert page._prompt_name.text() == "objecion_ventas"
        assert page._prompt_content.toPlainText() == "Prompt importado desde TXT"
    finally:
        page.close()
        queries.shutdown()


def test_autoresponder_page_switches_to_monitor_and_accepts_log_updates(monkeypatch):
    from gui import pages_automation_autoresponder as autoresponder_module

    _app()
    monkeypatch.setattr(autoresponder_module.AutomationMessageDialog, "exec", lambda self: QDialog.Accepted)
    ctx, queries = _build_ctx()
    page = AutomationAutoresponderPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 2)
        assert page._concurrency.maximum() == 2
        page._start()
        assert page._stack.currentWidget() is page._monitor_view
        ctx.logs.append("[INFO] Account loaded\n")
        ctx.logs.append(
            'AR_EVENT {"event":"PROGRESS","account":"cuenta_1","outcome":"respondio","reason":"ok"}\n'
        )
        _pump_events(4)
        assert "[INFO] Account loaded" in page._log.toPlainText()
        assert "@cuenta_1 respondio (ok)" in page._log.toPlainText()
        assert "AR_EVENT" not in page._log.toPlainText()
        assert _wait_until(lambda: not ctx.tasks.is_running("autoresponder"), timeout=3.0)
        page._stop()
        assert ctx.services.automation.stop_reasons[-1] == "stop solicitado desde GUI"
    finally:
        page.close()
        queries.shutdown()


def test_autoresponder_page_keeps_monitor_open_while_start_is_pending(monkeypatch):
    from gui import pages_automation_autoresponder as autoresponder_module

    _app()
    monkeypatch.setattr(autoresponder_module.AutomationMessageDialog, "exec", lambda self: QDialog.Accepted)
    ctx, queries = _build_ctx()

    def _slow_start(payload: dict[str, Any]) -> dict[str, Any]:
        ctx.services.automation.start_calls.append(dict(payload))
        ctx.services.automation.task_active = True
        time.sleep(0.2)
        ctx.services.automation.task_active = False
        return ctx.services.automation.autoresponder_snapshot(str(payload.get("alias") or "default"))

    ctx.services.automation.start_autoresponder = _slow_start  # type: ignore[method-assign]
    page = AutomationAutoresponderPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 2)
        page._start()
        assert page._stack.currentWidget() is page._monitor_view
        idle_snapshot = ctx.services.automation.autoresponder_snapshot("default")
        idle_snapshot["task_active"] = False
        idle_snapshot["status"] = "Idle"
        page._apply_snapshot(
            {
                "aliases": ["default", "ventas"],
                "selected_alias": "default",
                "snapshot": idle_snapshot,
                "task_active": False,
                "alias_accounts": ctx.services.automation.alias_account_rows("default"),
                "max_concurrency": ctx.services.automation.max_alias_concurrency("default"),
            }
        )
        assert page._stack.currentWidget() is page._monitor_view
        assert _wait_until(lambda: not ctx.tasks.is_running("autoresponder"), timeout=3.0)
    finally:
        page.close()
        queries.shutdown()


def test_autoresponder_page_surfaces_account_safety_reason_before_start(monkeypatch):
    _app()
    ctx, queries = _build_ctx()
    ctx.services.automation.alias_account_rows = lambda alias: [  # type: ignore[method-assign]
        {
            "username": "cuenta_1",
            "proxy": "proxy-a",
            "connected": False,
            "blocked": True,
            "blocked_reason": "Re-login requerido",
            "safety_state": "needs_login",
            "safety_message": "Re-login requerido",
        }
    ]
    ctx.services.automation.max_alias_concurrency = lambda alias: 1  # type: ignore[method-assign]
    ctx.services.automation.autoresponder_snapshot = lambda alias: {  # type: ignore[method-assign]
        "run_id": "auto-1",
        "alias": alias,
        "status": "Idle",
        "message": "Monitor activo.",
        "started_at": "",
        "finished_at": "",
        "delay_min": 30,
        "delay_max": 60,
        "concurrency": 1,
        "threads": 12,
        "followup_only": False,
        "followup_schedule_label": "4, 24",
        "accounts_total": 1,
        "accounts_active": 0,
        "accounts_blocked": 1,
        "task_active": False,
        "message_success": 0,
        "message_failed": 0,
        "followup_success": 0,
        "followup_failed": 0,
        "agendas_generated": 0,
        "account_rows": [
            {
                "account": "cuenta_1",
                "proxy": "proxy-a",
                "blocked": True,
                "blocked_reason": "Re-login requerido",
                "safety_state": "needs_login",
                "safety_message": "Re-login requerido",
            }
        ],
    }

    page = AutomationAutoresponderPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._alias_combo.count() == 2)
        assert _wait_until(lambda: page._accounts_preview.item(0, 3) is not None)
        assert page._accounts_preview.item(0, 2).text() == "No"
        assert page._accounts_preview.item(0, 3).text() == "Re-login requerido"
        assert page._active_accounts.item(0, 2).text() == "Re-login requerido"
        assert "1 cuenta bloqueada por seguridad" in page._capacity_hint.text()
        assert page._start_button is not None
        assert page._start_button.isEnabled() is False
    finally:
        page.close()
        queries.shutdown()


def test_autoresponder_log_buffer_keeps_one_entry_per_line():
    _app()
    ctx, queries = _build_ctx()
    page = AutomationAutoresponderPage(ctx)
    try:
        page._capturing_logs = True
        page._on_log_added("[INFO] Account loaded")
        page._on_log_added("\n[INFO] Checking inbox\n[INFO] Message detected")
        page._on_log_added("\n[INFO] Sending response\n")
        assert page._log.toPlainText().splitlines() == [
            "[INFO] Account loaded",
            "[INFO] Checking inbox",
            "[INFO] Message detected",
            "[INFO] Sending response",
        ]
    finally:
        page.close()
        queries.shutdown()


def test_signal_writer_waits_for_newline_before_emitting():
    chunks: list[str] = []
    writer = _SignalWriter(chunks.append)

    assert writer.write("Preparando autoresponder para alias matias") > 0
    assert chunks == []

    writer.write("\n")
    assert chunks == ["Preparando autoresponder para alias matias\n"]


def test_autoresponder_page_consumes_runtime_event_with_logging_prefix():
    _app()
    ctx, queries = _build_ctx()
    page = AutomationAutoresponderPage(ctx)
    try:
        page._capturing_logs = True
        page._on_log_added(
            '[INFO] core.responder: AR_EVENT {"event":"PROGRESS","account":"cuenta_1","outcome":"respondio","reason":"ok"}\n'
        )
        assert page._log.toPlainText().splitlines() == ["@cuenta_1 respondio (ok)"]
    finally:
        page.close()
        queries.shutdown()


def test_autoresponder_page_filters_console_noise_and_formats_structured_runtime_events():
    _app()
    ctx, queries = _build_ctx()
    page = AutomationAutoresponderPage(ctx)
    try:
        page._capturing_logs = True
        page._on_log_added("raw console noise\n")
        page._on_log_added(
            '[INFO] core.responder: AR_EVENT {"event":"THREADS_DISCOVERED","account":"cuenta_1","source":"initial","discovered":5}\n'
        )
        page._on_log_added(
            '[INFO] core.responder: AR_EVENT {"event":"PACK_SELECTED","account":"cuenta_1","pack_type":"PACK_A","recipient":"lead_1"}\n'
        )
        page._on_log_added(
            '[INFO] core.responder: AR_EVENT {"event":"MESSAGE_SENT","account":"cuenta_1","recipient":"lead_1"}\n'
        )
        page._on_log_added(
            '[INFO] core.responder: AR_EVENT {"event":"FOLLOWUP_SENT","account":"cuenta_1","recipient":"lead_1"}\n'
        )

        assert page._log.toPlainText().splitlines() == [
            "@cuenta_1 threads discovered=5 (initial)",
            "@cuenta_1 pack selected PACK_A -> @lead_1",
            "@cuenta_1 message sent -> @lead_1",
            "@cuenta_1 follow-up sent -> @lead_1",
        ]
    finally:
        page.close()
        queries.shutdown()
