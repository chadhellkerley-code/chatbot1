from __future__ import annotations

import os
import threading
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QDialog

from gui.page_base import GuiState, PageContext
from gui.pages_automation_flow import AutomationFlowPage
from gui.query_runner import QueryManager
from gui.task_runner import LogStore


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + max(0.1, timeout)
    while time.time() < deadline:
        if predicate():
            return True
        _pump_events(2)
        time.sleep(interval)
    _pump_events(4)
    return bool(predicate())


class _FakeAccountsService:
    def list_aliases(self) -> list[str]:
        return ["default"]


class _FakeAutomationService:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []
        self.saved_payloads: list[dict] = []

    def list_packs(self) -> list[dict[str, str]]:
        return [{"id": "pack_bienvenida", "name": "Pack bienvenida"}]

    def get_flow_config(self, alias: str) -> dict:
        del alias
        self.thread_ids.append(threading.get_ident())
        time.sleep(0.05)
        return {
            "version": 1,
            "entry_stage_id": "etapa_1",
            "stages": [
                {
                    "id": "etapa_1",
                    "action_type": "mensaje",
                    "transitions": {
                        "positive": "etapa_1",
                        "negative": "etapa_1",
                        "doubt": "etapa_1",
                        "neutral": "etapa_1",
                    },
                    "followups": [{"delay_hours": 4.0, "action_type": "followup"}],
                    "post_objection": {"enabled": False, "action_type": "mensaje", "max_steps": 2},
                }
            ],
            "layout": {
                "nodes": {"etapa_1": {"x": 120.0, "y": 120.0}},
                "viewport": {"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0},
            },
        }

    def save_flow_config(self, alias: str, flow_config: dict) -> dict:
        self.saved_payloads.append({"alias": alias, "flow": flow_config})
        return flow_config


def test_flow_page_separates_modes_and_persists_structured_stage_updates(monkeypatch):
    from gui import pages_automation_flow as flow_module

    _app()
    monkeypatch.setattr(flow_module.StageEditorDialog, "exec", lambda self: QDialog.Accepted)
    monkeypatch.setattr(
        flow_module.StageEditorDialog,
        "payload",
        lambda self: {
            "id": "etapa_cierre",
            "action_type": "pack_bienvenida",
            "transitions": {
                "positive": "etapa_1",
                "negative": "etapa_cierre",
                "neutral": "etapa_cierre",
                "doubt": "etapa_cierre",
            },
            "followups": [{"delay_hours": 6.0, "action_type": "followup"}],
            "post_objection": {
                "enabled": True,
                "action_type": "mensaje",
                "max_steps": 2,
                "resolved_transition": "positive",
                "unresolved_transition": "negative",
            },
        },
    )
    queries = QueryManager()
    ctx = PageContext(
        services=SimpleNamespace(accounts=_FakeAccountsService(), automation=_FakeAutomationService()),
        tasks=SimpleNamespace(is_running=lambda name: False),
        logs=LogStore(),
        queries=queries,
        state=GuiState(active_alias="default"),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    page = AutomationFlowPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._advanced_table.rowCount() == 1)
        assert page._stack.currentIndex() == 0
        assert page._simple_mode.isChecked()
        page._set_mode("advanced")
        assert page._stack.currentIndex() == 1
        assert page._advanced_mode.isChecked()
        page._set_mode("simple")
        page._add_stage(open_editor=True)
        assert len(page._stage_ids()) == 2
        assert "etapa_cierre" in page._stage_ids()
        refresh_calls: list[str] = []
        original_refresh = page._refresh_canvas

        def _tracking_refresh() -> None:
            refresh_calls.append("refresh")
            original_refresh()

        page._refresh_canvas = _tracking_refresh  # type: ignore[method-assign]
        page._canvas.stage_selected.emit("etapa_1")
        _pump_events(2)
        assert refresh_calls == []
        page._toggle_canvas_maximize()
        assert page._controls_widget.isHidden()
        assert page.page_header_widget() is not None and page.page_header_widget().isHidden()
        assert page.section_nav_widget().isHidden()
        assert page.content_margins() == (0, 0, 0, 0)
        page._toggle_canvas_maximize()
        assert not page._controls_widget.isHidden()
        assert page.page_header_widget() is not None and not page.page_header_widget().isHidden()
        assert not page.section_nav_widget().isHidden()
        assert page.content_margins() == page.default_content_margins()
        page._apply_canvas_stage_payload(
            "etapa_cierre",
            {
                "id": "etapa_cierre",
                "action_type": "pack_bienvenida",
                "transitions": {
                    "positive": "etapa_1",
                    "negative": "etapa_cierre",
                    "neutral": "etapa_cierre",
                    "doubt": "etapa_cierre",
                },
                "followups": [{"delay_hours": 8.0, "action_type": "followup"}],
                "post_objection": {
                    "enabled": True,
                    "action_type": "mensaje",
                    "max_steps": 2,
                    "resolved_transition": "positive",
                    "unresolved_transition": "negative",
                },
            },
        )
        page._canvas._scene._nodes["etapa_cierre"].setPos(320.0, 440.0)
        page._canvas._view.set_zoom_value(1.15)
        page._canvas._view.horizontalScrollBar().setValue(60)
        page._canvas._view.verticalScrollBar().setValue(90)
        page._canvas._emit_viewport_changed()
        page.save_flow()
        assert ctx.services.automation.saved_payloads
        saved = ctx.services.automation.saved_payloads[-1]["flow"]
        assert any(str(stage.get("id")) == "etapa_cierre" for stage in saved["stages"])
        assert any(str(stage.get("action_type")) == "pack_bienvenida" for stage in saved["stages"])
        assert saved["layout"]["nodes"]["etapa_cierre"] == {"x": 320.0, "y": 440.0}
        assert saved["layout"]["viewport"]["zoom"] == 1.15
    finally:
        page.close()
        queries.shutdown()
