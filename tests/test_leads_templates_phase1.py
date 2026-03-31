from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from application.services.base import ServiceContext, ServiceError
from application.services.leads_service import LeadsService
from core.templates_store import load_templates
from gui.modules.leads.templates_panel import LeadsTemplatesPanel
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


class _StaticTasks:
    def is_running(self, name: str) -> bool:
        del name
        return False


def _build_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LeadsService:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    return LeadsService(ServiceContext.default(root_dir=tmp_path))


def _build_panel_ctx(service: LeadsService) -> tuple[PageContext, QueryManager]:
    queries = QueryManager()
    ctx = PageContext(
        services=SimpleNamespace(leads=service),
        tasks=_StaticTasks(),
        logs=LogStore(),
        queries=queries,
        state=GuiState(),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries


def test_templates_store_migrates_legacy_records_to_canonical_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    storage_path = tmp_path / "storage" / "templates.json"
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_text(
        json.dumps(
            [
                {"name": "Alpha", "text": "hola"},
                {"name": "Beta", "text": "chau"},
            ]
        ),
        encoding="utf-8",
    )

    templates = load_templates()

    assert [item["name"] for item in templates] == ["Alpha", "Beta"]
    assert all(str(item.get("id") or "").startswith("tpl_") for item in templates)
    assert all(str(item.get("created_at") or "").strip() for item in templates)
    assert all(str(item.get("updated_at") or "").strip() for item in templates)
    assert all(item.get("schema_version") == 1 for item in templates)

    persisted = json.loads(storage_path.read_text(encoding="utf-8"))
    assert persisted == templates


def test_leads_service_upsert_preserves_id_and_blocks_rename_conflicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _build_service(tmp_path, monkeypatch)

    alpha = service.upsert_template("Alpha", "hola")
    renamed = service.upsert_template("Alpha VIP", "hola 2", template_id=str(alpha.get("id") or ""))
    beta = service.upsert_template("Beta", "chau")

    assert renamed["id"] == alpha["id"]
    assert renamed["name"] == "Alpha VIP"

    with pytest.raises(ServiceError, match="Ya existe una plantilla con ese nombre"):
        service.upsert_template("Beta", "mensaje", template_id=str(alpha.get("id") or ""))

    names = sorted(str(item.get("name") or "") for item in service.list_templates())
    assert names == ["Alpha VIP", "Beta"]
    assert beta["id"] != alpha["id"]


def test_leads_service_delete_template_uses_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _build_service(tmp_path, monkeypatch)

    alpha = service.upsert_template("Alpha", "hola")
    beta = service.upsert_template("Beta", "chau")

    assert service.delete_template(str(beta.get("id") or "")) == 1

    templates = service.list_templates()
    loaded = service.load_template(str(alpha.get("id") or ""))
    assert loaded is not None
    assert templates[0]["id"] == alpha["id"]
    assert templates[0]["name"] == "Alpha"
    assert loaded["id"] == alpha["id"]
    assert loaded["variant_count"] == 1


def test_leads_templates_panel_renames_and_deletes_selected_template_by_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _app()
    service = _build_service(tmp_path, monkeypatch)
    ctx, queries = _build_panel_ctx(service)
    panel = LeadsTemplatesPanel(ctx)
    try:
        panel.refresh_page()
        assert _wait_until(lambda: panel._table.rowCount() == 0)

        panel._name_input.setText("Alpha")
        panel._messages_input.setPlainText("hola")
        panel._save_template()
        assert _wait_until(lambda: panel._table.rowCount() == 1)

        created = service.list_templates()[0]
        panel._table.selectRow(0)
        _pump_events(4)
        assert panel._selected_template_id == created["id"]

        panel._name_input.setText("Alpha VIP")
        panel._messages_input.setPlainText("hola 2")
        panel._save_template()
        assert _wait_until(lambda: service.list_templates()[0]["name"] == "Alpha VIP")

        renamed = service.list_templates()[0]
        assert renamed["id"] == created["id"]

        panel._new_template()
        panel._name_input.setText("Beta")
        panel._messages_input.setPlainText("chau")
        panel._save_template()
        assert _wait_until(lambda: len(service.list_templates()) == 2)
        panel.refresh_page()
        assert _wait_until(lambda: panel._table.rowCount() == 2)

        for row in range(panel._table.rowCount()):
            item = panel._table.item(row, 0)
            if item is not None and item.text() == "Beta":
                panel._table.selectRow(row)
                break
        _pump_events(4)
        beta_id = panel._selected_template_id

        panel._delete_template()
        assert _wait_until(lambda: len(service.list_templates()) == 1)
        panel.refresh_page()
        assert _wait_until(lambda: panel._table.rowCount() == 1)

        remaining = service.list_templates()
        assert [item["id"] for item in remaining] == [renamed["id"]]
        assert beta_id != renamed["id"]
    finally:
        queries.shutdown()
