from __future__ import annotations

import os
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QDialog

from gui.page_base import GuiState, PageContext
from gui.pages_automation import AutomationPacksPage, AutomationWhatsAppPage, PackEditorDialog
from gui.query_runner import QueryManager
from gui.task_runner import LogStore, TaskManager


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

    def list_accounts(self, alias: str | None = None):
        del alias
        return []


class _FakeAutomationService:
    def __init__(self) -> None:
        self.saved_templates: list[tuple[str, str, str]] = []
        self.saved_lists: list[tuple[str, list[dict[str, str]]]] = []
        self.saved_whatsapp_autoresponder: list[dict[str, object]] = []

    def list_objection_prompts(self) -> list[dict[str, str]]:
        return []

    def list_packs(self) -> list[dict[str, object]]:
        return [{"id": "pack_1", "name": "Pack 1", "type": "bienvenida", "actions": [{"type": "text_fixed", "content": "Hola"}]}]

    def upsert_pack(self, payload: dict[str, object]) -> dict[str, object]:
        return payload

    def delete_pack(self, pack_id: str) -> int:
        del pack_id
        return 1

    def whatsapp_snapshot(self) -> dict[str, object]:
        return {
            "numbers": [{"id": "num_1", "alias": "Wa 1", "phone": "+59800000000", "connected": True}],
            "lists": [{"alias": "lista_a", "contacts": [{"name": "Juan", "number": "+5981"}]}],
            "templates": [{"id": "tpl_1", "name": "Plantilla A", "content": "Hola {nombre}"}],
            "runs": [{"id": "run_1", "list_alias": "lista_a", "status": "running", "events": [{"message": "Hola"}]}],
            "autoresponder": {"mode": "ia", "prompt": "Responde corto", "fixed_message": "", "enabled": True},
        }

    def connect_whatsapp_number(self, *, alias: str, phone: str) -> dict[str, str]:
        return {"alias": alias, "phone": phone}

    def save_whatsapp_contact_list(self, alias: str, contacts: list[dict[str, str]]) -> dict[str, object]:
        self.saved_lists.append((alias, contacts))
        return {"alias": alias}

    def delete_whatsapp_contact_list(self, alias: str) -> int:
        del alias
        return 1

    def save_whatsapp_template(self, template_id: str, name: str, content: str) -> dict[str, str]:
        self.saved_templates.append((template_id, name, content))
        return {"id": template_id or "tpl_1", "name": name, "content": content}

    def delete_whatsapp_template(self, template_id: str) -> int:
        del template_id
        return 1

    def schedule_whatsapp_message_run(self, **kwargs):
        return kwargs

    def save_whatsapp_autoresponder_config(self, **kwargs):
        self.saved_whatsapp_autoresponder.append(kwargs)
        return kwargs


def _build_ctx() -> tuple[PageContext, QueryManager]:
    logs = LogStore()
    tasks = TaskManager(logs)
    queries = QueryManager()
    ctx = PageContext(
        services=SimpleNamespace(accounts=_FakeAccountsService(), automation=_FakeAutomationService()),
        tasks=tasks,
        logs=logs,
        queries=queries,
        state=GuiState(active_alias="default"),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: False,
    )
    return ctx, queries


def test_packs_page_loads_structured_rows():
    _app()
    ctx, queries = _build_ctx()
    page = AutomationPacksPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._table.rowCount() == 1)
        assert page._table.item(0, 1).text() == "Pack 1"
    finally:
        page.close()
        queries.shutdown()


def test_pack_editor_supports_reordering_structured_actions():
    _app()
    dialog = PackEditorDialog(
        {
            "id": "pack_1",
            "name": "Pack 1",
            "type": "bienvenida",
            "actions": [{"type": "text_fixed", "content": "Hola"}],
        }
    )
    try:
        dialog._add_action({"type": "text_fixed", "content": "Segundo mensaje"})
        dialog._add_action({"type": "text_adaptive", "instruction": "Prompt IA"})
        dialog._move_action_down(dialog._action_editors[0])
        payload = dialog.payload()
        assert len(payload["actions"]) == 3
        assert payload["actions"][0]["content"] == "Segundo mensaje"
        assert payload["actions"][1]["content"] == "Hola"
        assert payload["actions"][2]["instruction"] == "Prompt IA"
        assert not dialog._add_button.isEnabled()
        assert dialog._actions_scroll.widget() is dialog._actions_container
        assert all(editor._content.minimumHeight() == 96 for editor in dialog._action_editors)
    finally:
        dialog.close()


def test_whatsapp_page_exposes_five_panels_and_saves_structured_forms():
    _app()
    ctx, queries = _build_ctx()
    page = AutomationWhatsAppPage(ctx)
    try:
        page.on_navigate_to()
        assert _wait_until(lambda: page._templates_table.rowCount() == 1)
        assert len(page._panel_buttons) == 5
        page._list_name.setText("lista_b")
        page._list_contacts.setPlainText("Ana|+5982")
        page._save_contact_list()
        assert ctx.services.automation.saved_lists[-1][0] == "lista_b"
        page._template_name.setText("Plantilla B")
        page._template_content.setPlainText("Hola")
        page._save_template()
        assert ctx.services.automation.saved_templates[-1][1] == "Plantilla B"
        page._wa_mode.setCurrentIndex(page._wa_mode.findData("fijo"))
        page._wa_fixed.setPlainText("Mensaje fijo")
        page._save_whatsapp_autoresponder()
        assert ctx.services.automation.saved_whatsapp_autoresponder[-1]["mode"] == "fijo"
    finally:
        page.close()
        queries.shutdown()
