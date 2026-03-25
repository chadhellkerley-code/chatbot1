from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from gui.modules.leads.import_panel import LeadsImportPanel
from gui.page_base import GuiState, PageContext
from gui.query_runner import QueryManager
from gui.task_runner import LogStore


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


class _FakeLeadsService:
    def __init__(self) -> None:
        self.preview_calls: list[tuple[str, str]] = []
        self.import_calls: list[tuple[str, str, str]] = []
        self.rollback_calls: list[str] = []

    def validate_list_name(self, name: str) -> str:
        clean_name = str(name or "").strip()
        if "*" in clean_name:
            raise ValueError("El nombre de lista contiene caracteres no permitidos.")
        return clean_name

    def preview_csv(self, path: str, name: str) -> dict[str, object]:
        self.preview_calls.append((path, name))
        return {
            "kind": "csv",
            "valid_count": 3,
            "new_count": 2,
            "already_present_count": 1,
            "duplicate_in_file_count": 1,
            "blank_or_invalid_count": 0,
            "encoding": "utf-8-sig",
            "delimiter": ",",
            "header_detected": True,
            "username_column": "username",
            "same_file_import_count": 0,
            "sanity_state": "warning",
            "sanity_messages": ["El archivo trae usernames duplicados."],
        }

    def preview_txt(self, path: str, name: str) -> dict[str, object]:
        self.preview_calls.append((path, name))
        return {
            "kind": "txt",
            "valid_count": 2,
            "new_count": 2,
            "already_present_count": 0,
            "duplicate_in_file_count": 0,
            "blank_or_invalid_count": 0,
            "encoding": "utf-8",
            "delimiter": "",
            "header_detected": False,
            "username_column": "",
            "same_file_import_count": 0,
        }

    def import_csv(self, path: str, name: str) -> dict[str, object]:
        self.import_calls.append(("csv", path, name))
        return {
            "kind": "csv",
            "list_name": name,
            "new_count": 2,
            "already_present_count": 1,
            "resulting_count": 5,
            "sanity_messages": ["El archivo trae usernames duplicados."],
        }

    def import_txt(self, path: str, name: str) -> dict[str, object]:
        self.import_calls.append(("txt", path, name))
        return {"kind": "txt", "list_name": name, "new_count": 2, "already_present_count": 0, "resulting_count": 2}

    def rollback_last_import(self, name: str) -> dict[str, object]:
        self.rollback_calls.append(name)
        return {"list_name": name, "restored_count": 3, "previous_count": 5}

    def list_lists(self) -> list[str]:
        return ["demo"]

    def import_status_snapshot(self) -> dict[str, object]:
        return {
            "summary": "Ultimo import: demo\n7d: ok 2  |  fallidos 0  |  rollbacks 1",
            "latest_event": None,
        }


def _build_ctx() -> tuple[PageContext, QueryManager, _FakeLeadsService]:
    queries = QueryManager()
    leads_service = _FakeLeadsService()
    ctx = PageContext(
        services=SimpleNamespace(leads=leads_service),
        tasks=SimpleNamespace(),
        logs=LogStore(),
        queries=queries,
        state=GuiState(),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries, leads_service


def test_import_panel_validate_rejects_wrong_extension(tmp_path: Path, monkeypatch) -> None:
    _app()
    errors: list[str] = []
    monkeypatch.setattr("gui.modules.leads.import_panel.show_panel_error", lambda _widget, text: errors.append(str(text)))
    ctx, queries, _leads = _build_ctx()
    panel = LeadsImportPanel(ctx)
    try:
        file_path = tmp_path / "seed.txt"
        file_path.write_text("uno\n", encoding="utf-8")
        panel._path_input.setText(str(file_path))
        panel._list_combo.setEditText("demo")

        assert panel._validate_import_target(expected_suffix=".csv") is None
        assert errors == ["Selecciona un archivo .csv."]
    finally:
        queries.shutdown()
        panel.deleteLater()


def test_import_panel_validate_rejects_invalid_list_name(tmp_path: Path, monkeypatch) -> None:
    _app()
    errors: list[str] = []
    monkeypatch.setattr("gui.modules.leads.import_panel.show_panel_error", lambda _widget, text: errors.append(str(text)))
    ctx, queries, _leads = _build_ctx()
    panel = LeadsImportPanel(ctx)
    try:
        file_path = tmp_path / "seed.csv"
        file_path.write_text("username\nuno\n", encoding="utf-8")
        panel._path_input.setText(str(file_path))
        panel._list_combo.setEditText("demo*")

        assert panel._validate_import_target(expected_suffix=".csv") is None
        assert errors == ["El nombre de lista contiene caracteres no permitidos."]
    finally:
        queries.shutdown()
        panel.deleteLater()


def test_import_panel_preview_updates_summary(tmp_path: Path) -> None:
    _app()
    ctx, queries, leads = _build_ctx()
    panel = LeadsImportPanel(ctx)
    try:
        file_path = tmp_path / "seed.csv"
        file_path.write_text("username\nuno\ndos\n", encoding="utf-8")
        panel._path_input.setText(str(file_path))
        panel._list_combo.setEditText("demo")

        panel._preview_selected_file()

        assert leads.preview_calls == [(str(file_path), "demo")]
        assert "Analisis CSV" in panel._summary.text()
        assert "Nuevos: 2" in panel._summary.text()
        assert "Advertencias" in panel._summary.text()
    finally:
        queries.shutdown()
        panel.deleteLater()


def test_import_panel_rollback_calls_service_and_sets_status(monkeypatch) -> None:
    _app()
    statuses: list[str] = []
    monkeypatch.setattr("gui.modules.leads.import_panel.set_panel_status", lambda _widget, text: statuses.append(str(text)))
    ctx, queries, leads = _build_ctx()
    panel = LeadsImportPanel(ctx)
    try:
        panel._list_combo.setEditText("demo")

        panel._rollback_last_import()

        assert leads.rollback_calls == ["demo"]
        assert statuses
        assert "Rollback aplicado" in statuses[-1]
        assert "restaurados 3 leads" in statuses[-1]
    finally:
        queries.shutdown()
        panel.deleteLater()


def test_import_panel_refresh_page_loads_snapshot_summary_and_lists() -> None:
    _app()
    ctx, queries, _leads = _build_ctx()
    panel = LeadsImportPanel(ctx)
    try:
        panel.refresh_page()

        assert _wait_until(lambda: "Ultimo import: demo" in panel._summary.text())
        assert "7d: ok 2" in panel._summary.text()
        assert panel._list_combo.count() == 1
        assert panel._list_combo.itemText(0) == "demo"
    finally:
        queries.shutdown()
        panel.deleteLater()
