from __future__ import annotations

import os
import time
from typing import Any
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from gui.inbox.actions_panel import ActionsPanel
from gui.inbox.chat_view import ChatView
from gui.inbox.chat_view import _message_meta, _normalize_message_rows
from gui.inbox.conversation_list import ConversationListModel
from gui.inbox.inbox_controller import InboxController
from gui.inbox.inbox_view import InboxView
from gui.page_base import GuiState, PageContext


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


class _FakeInboxEvents(QObject):
    cache_updated = Signal(object)


class _FakeInboxService:
    def __init__(self) -> None:
        self.events = _FakeInboxEvents()
        self.ensure_started_calls = 0
        self.refresh_calls = 0
        self.cached_get_thread_calls = 0
        self.cached_list_thread_calls = 0
        self.ui_active_calls: list[bool] = []
        self.open_thread_calls: list[str] = []
        self.send_message_calls: list[tuple[str, str]] = []
        self.send_pack_calls: list[tuple[str, str]] = []
        self.request_ai_calls: list[str] = []
        self.take_thread_manual_calls: list[str] = []
        self.release_thread_manual_calls: list[str] = []
        self.mark_thread_qualified_calls: list[str] = []
        self.mark_thread_disqualified_calls: list[str] = []
        self.clear_thread_classification_calls: list[str] = []
        self.delete_local_message_calls: list[tuple[str, dict[str, Any]]] = []
        self.start_runtime_calls: list[tuple[str, dict[str, Any]]] = []
        self.stop_runtime_calls: list[str] = []
        self.runtime_aliases = ["ventas", "soporte"]
        self.runtime_states: dict[str, dict[str, Any]] = {
            "ventas": {"alias_id": "ventas", "is_running": True, "mode": "both"},
            "soporte": {"alias_id": "soporte", "is_running": False, "mode": "both"},
        }
        self.rows = [
            {
                "thread_key": "acc-1:thread-1",
                "account_id": "acc-1",
                "account_alias": "soporte",
                "display_name": "Cliente Uno",
                "recipient_username": "cliente1",
                "unread_count": 1,
                "last_message_text": "Hola",
                "last_message_direction": "inbound",
                "last_message_timestamp": 100.0,
                "owner": "auto",
                "bucket": "qualified",
                "account_health": "healthy",
            },
            {
                "thread_key": "acc-2:thread-2",
                "account_id": "acc-2",
                "account_alias": "ventas",
                "display_name": "Cliente Dos",
                "recipient_username": "cliente2",
                "unread_count": 0,
                "last_message_text": "Seguimos",
                "last_message_direction": "outbound",
                "last_message_timestamp": 90.0,
                "owner": "auto",
                "bucket": "qualified",
                "account_health": "healthy",
            },
        ]
        self.threads: dict[str, dict[str, Any]] = {
            "acc-1:thread-1": {
                "thread_key": "acc-1:thread-1",
                "account_id": "acc-1",
                "account_alias": "soporte",
                "recipient_username": "cliente1",
                "owner": "auto",
                "bucket": "qualified",
                "account_health": "healthy",
                "messages": [
                    {
                        "message_id": "m-1",
                        "direction": "inbound",
                        "text": "Hola",
                        "timestamp": 100.0,
                    }
                ],
            },
            "acc-2:thread-2": {
                "thread_key": "acc-2:thread-2",
                "account_id": "acc-2",
                "account_alias": "ventas",
                "recipient_username": "cliente2",
                "owner": "auto",
                "bucket": "qualified",
                "account_health": "healthy",
                "messages": [],
            },
        }

    def ensure_started(self) -> None:
        self.ensure_started_calls += 1

    def refresh(self) -> None:
        self.refresh_calls += 1

    def set_ui_active(self, active: bool) -> None:
        self.ui_active_calls.append(bool(active))

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows]

    def list_threads_cached(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        self.cached_list_thread_calls += 1
        return self.list_threads(filter_mode)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        thread = self.threads.get(str(thread_key or "").strip())
        return dict(thread) if isinstance(thread, dict) else None

    def get_thread_cached(self, thread_key: str) -> dict[str, Any] | None:
        self.cached_get_thread_calls += 1
        return self.get_thread(thread_key)

    def projection_ready(self) -> bool:
        return True

    def request_open_thread(self, thread_key: str) -> bool:
        return self.open_thread(thread_key)

    def open_thread(self, thread_key: str) -> bool:
        self.open_thread_calls.append(str(thread_key or "").strip())
        return True

    def send_message(self, thread_key: str, text: str) -> str:
        self.send_message_calls.append((str(thread_key or "").strip(), str(text or "").strip()))
        return "local-1"

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        self.send_pack_calls.append((str(thread_key or "").strip(), str(pack_id or "").strip()))
        return True

    def request_ai_suggestion(self, thread_key: str) -> bool:
        self.request_ai_calls.append(str(thread_key or "").strip())
        return True

    def take_thread_manual(self, thread_key: str) -> bool:
        self.take_thread_manual_calls.append(str(thread_key or "").strip())
        return True

    def release_thread_manual(self, thread_key: str) -> bool:
        self.release_thread_manual_calls.append(str(thread_key or "").strip())
        return True

    def mark_thread_qualified(self, thread_key: str) -> bool:
        self.mark_thread_qualified_calls.append(str(thread_key or "").strip())
        return True

    def mark_thread_disqualified(self, thread_key: str) -> bool:
        self.mark_thread_disqualified_calls.append(str(thread_key or "").strip())
        return True

    def clear_thread_classification(self, thread_key: str) -> bool:
        self.clear_thread_classification_calls.append(str(thread_key or "").strip())
        return True

    def delete_local_message(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        self.delete_local_message_calls.append((str(thread_key or "").strip(), dict(message_ref or {})))
        return True

    def list_packs(self) -> list[dict[str, Any]]:
        return [{"id": "pack-1", "name": "Pack Bienvenida"}]

    def list_runtime_aliases(self) -> list[str]:
        return list(self.runtime_aliases)

    def alias_runtime_status(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return {}
        return dict(self.runtime_states.get(clean_alias) or {"alias_id": clean_alias, "is_running": False, "mode": "both"})


def test_chat_view_normalizes_optimistic_duplicate_and_uses_fallback_timestamp() -> None:
    rows = _normalize_message_rows(
        [
            {
                "message_id": "local-1",
                "external_message_id": "dom-confirmed-1",
                "direction": "outbound",
                "text": "Hola prolijo",
                "created_at": 100.0,
                "delivery_status": "sent",
                "local_echo": False,
            },
            {
                "message_id": "ig-real-1",
                "direction": "outbound",
                "text": "Hola prolijo",
                "timestamp": 101.0,
                "delivery_status": "sent",
                "local_echo": False,
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0]["message_id"] == "ig-real-1"
    assert rows[0]["message_ts_canonical"] == 101.0
    assert rows[0]["message_ts_source"] == "timestamp"
    assert _message_meta(
        {
            "message_id": "local-1",
            "created_at": 100.0,
            "direction": "outbound",
            "delivery_status": "pending",
            "local_echo": True,
        }
    ) != ""
    assert _message_meta({"message_id": "msg-1", "created_at": 100.0, "delivery_status": "sent"}) != ""

    def start_alias_runtime(self, alias_id: str, config: dict[str, Any]) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        payload = {"alias_id": clean_alias, "is_running": True, **dict(config or {})}
        self.runtime_states[clean_alias] = payload
        self.start_runtime_calls.append((clean_alias, dict(config or {})))
        return payload

    def stop_alias_runtime(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        payload = dict(self.runtime_states.get(clean_alias) or {"alias_id": clean_alias})
        payload["alias_id"] = clean_alias
        payload["is_running"] = False
        self.runtime_states[clean_alias] = payload
        self.stop_runtime_calls.append(clean_alias)
        return payload


def _thread_permissions(
    *,
    runtime_active: bool,
    owner: str,
    can_manual_send: bool,
) -> dict[str, Any]:
    return {
        "has_thread": True,
        "runtime_active": runtime_active,
        "owner": owner,
        "health": "healthy",
        "can_manual_send": can_manual_send,
        "can_send_pack": can_manual_send,
        "can_request_ai": can_manual_send,
        "can_takeover_manual": runtime_active and owner != "manual",
        "can_release_manual": owner == "manual",
        "can_mark_follow_up": owner != "manual",
        "can_add_tag": True,
        "can_mark_qualified": owner != "manual",
        "can_mark_disqualified": True,
        "can_clear_classification": True,
        "composer_mode": "editable" if can_manual_send else ("readonly" if runtime_active and owner != "manual" else "disabled"),
        "manual_send_reason": "" if can_manual_send else ("runtime_auto_owner" if runtime_active and owner != "manual" else "manual_blocked"),
    }


def _thread_payload(*, owner: str = "auto") -> dict[str, Any]:
    return {
        "thread_key": "acc-1:thread-1",
        "display_name": "Cliente Uno",
        "account_id": "acc-1",
        "account_alias": "ventas",
        "recipient_username": "cliente1",
        "owner": owner,
        "bucket": "qualified",
        "account_health": "healthy",
    }


class _ViewController(QObject):
    snapshot_changed = Signal(object)

    def activate(self, *, initial_thread_key: str = "") -> None:
        del initial_thread_key

    def deactivate(self) -> None:
        return None

    def force_refresh(self) -> None:
        return None

    def set_filter(self, filter_mode: str) -> None:
        del filter_mode

    def select_thread(self, thread_key: str) -> None:
        del thread_key

    def send_message(self, text: str) -> None:
        del text

    def add_tag(self, tag: str = "Etiqueta manual") -> None:
        del tag

    def mark_follow_up(self) -> None:
        return None

    def delete_message(self, message: Any) -> None:
        del message

    def take_thread_manual(self) -> None:
        return None

    def release_thread_manual(self) -> None:
        return None

    def mark_thread_qualified(self) -> None:
        return None

    def mark_thread_disqualified(self) -> None:
        return None

    def clear_thread_classification(self) -> None:
        return None

    def request_ai_suggestion(self) -> None:
        return None

    def set_runtime_alias(self, alias_id: str) -> None:
        del alias_id

    def start_runtime(self, config: dict[str, Any]) -> None:
        del config

    def stop_runtime(self) -> None:
        return None

    def send_pack(self, pack_id: str) -> None:
        del pack_id


def _page_context() -> PageContext:
    return PageContext(
        services=SimpleNamespace(),
        tasks=SimpleNamespace(),
        logs=SimpleNamespace(),
        queries=SimpleNamespace(),
        state=GuiState(),
        open_route=lambda route, payload=None: None,
        go_back=lambda: None,
        can_go_back=lambda: True,
        toggle_sidebar=lambda: None,
        is_sidebar_visible=lambda: True,
    )


def test_inbox_controller_activates_from_cache_without_polling_loop() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate()
    _pump_events()

    assert service.ensure_started_calls == 1
    assert service.refresh_calls == 0
    assert service.cached_list_thread_calls >= 1
    assert service.cached_get_thread_calls >= 1
    assert service.ui_active_calls == [True]
    assert service.open_thread_calls == []
    assert snapshots
    assert snapshots[-1]["current_thread_key"] == "acc-1:thread-1"
    assert snapshots[-1]["metrics"] == {"threads": 2, "unread": 1, "pending": 1}

    time.sleep(0.05)
    _pump_events()
    assert service.refresh_calls == 0

    service.events.cache_updated.emit({"updated_at": time.time()})
    _pump_events()
    assert len(snapshots) >= 2
    assert str(snapshots[-1]["sync_label"]).startswith("Actualizado ")

    controller.deactivate()
    assert service.ui_active_calls == [True, False]


def test_inbox_controller_selects_empty_thread_and_queues_send_actions() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate(initial_thread_key="acc-2:thread-2")
    _pump_events()

    assert service.open_thread_calls == ["acc-2:thread-2"]
    assert service.cached_get_thread_calls >= 1
    assert snapshots[-1]["loading"] is True
    assert snapshots[-1]["thread"]["display_name"] == "Cliente Dos"
    assert snapshots[-1]["thread"]["account_id"] == "acc-2"
    assert snapshots[-1]["thread"]["recipient_username"] == "cliente2"

    controller.send_message("  Hola desde cache  ")
    _pump_events()
    assert service.send_message_calls == [("acc-2:thread-2", "Hola desde cache")]
    assert snapshots[-1]["force_scroll_to_bottom"] is True

    controller.send_pack("pack-1")
    controller.request_ai_suggestion()
    _pump_events()
    assert service.send_pack_calls == [("acc-2:thread-2", "pack-1")]
    assert service.request_ai_calls == ["acc-2:thread-2"]

    controller.deactivate()


def test_inbox_controller_updates_only_when_service_emits_events() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate(initial_thread_key="acc-2:thread-2")
    _pump_events()

    assert snapshots[-1]["loading"] is True
    assert service.refresh_calls == 0

    service.rows[1]["last_message_text"] = "Mensaje nuevo"
    service.rows[1]["last_message_timestamp"] = 190.0
    service.threads["acc-2:thread-2"] = {
        "thread_key": "acc-2:thread-2",
        "account_id": "acc-2",
        "recipient_username": "cliente2",
        "messages": [
            {
                "message_id": "m-2",
                "direction": "outbound",
                "text": "Mensaje nuevo",
                "timestamp": 190.0,
            }
        ],
    }

    time.sleep(0.30)
    _pump_events(8)

    assert service.refresh_calls == 0
    assert snapshots[-1]["loading"] is True

    service.events.cache_updated.emit({"updated_at": time.time(), "thread_keys": ["acc-2:thread-2"]})
    _pump_events(8)

    assert snapshots[-1]["loading"] is False
    assert [row["message_id"] for row in snapshots[-1]["messages"]] == ["m-2"]
    assert snapshots[-1]["thread"]["last_message_text"] == "Mensaje nuevo"

    controller.deactivate()


def test_inbox_controller_preserves_operational_and_ui_status_fields() -> None:
    _app()
    service = _FakeInboxService()
    service.rows[0]["status"] = "replied"
    service.rows[0]["operational_status"] = "replied"
    service.rows[0]["ui_status"] = "error"
    service.threads["acc-1:thread-1"].update(
        {
            "status": "replied",
            "operational_status": "replied",
            "ui_status": "error",
        }
    )
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate(initial_thread_key="acc-1:thread-1")
    _pump_events()

    assert snapshots
    assert snapshots[-1]["thread"]["status"] == "replied"
    assert snapshots[-1]["thread"]["operational_status"] == "replied"
    assert snapshots[-1]["thread"]["ui_status"] == "error"

    controller.deactivate()


def test_inbox_controller_exposes_manual_takeover_and_release_actions() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)

    controller.activate(initial_thread_key="acc-2:thread-2")
    _pump_events()

    controller.take_thread_manual()
    controller.release_thread_manual()
    _pump_events()

    assert service.take_thread_manual_calls == ["acc-2:thread-2"]
    assert service.release_thread_manual_calls == ["acc-2:thread-2"]

    controller.deactivate()


def test_inbox_controller_exposes_thread_classification_actions() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)

    controller.activate(initial_thread_key="acc-2:thread-2")
    _pump_events()

    controller.mark_thread_qualified()
    controller.mark_thread_disqualified()
    controller.clear_thread_classification()
    _pump_events()

    assert service.mark_thread_qualified_calls == ["acc-2:thread-2"]
    assert service.mark_thread_disqualified_calls == ["acc-2:thread-2"]
    assert service.clear_thread_classification_calls == ["acc-2:thread-2"]

    controller.deactivate()


def test_inbox_controller_deletes_message_locally() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)

    controller.activate(initial_thread_key="acc-1:thread-1")
    _pump_events()

    controller.delete_message({"block_id": "m-1", "message_id": "m-1", "text": "Hola"})
    _pump_events()

    assert service.delete_local_message_calls == [
        ("acc-1:thread-1", {"block_id": "m-1", "message_id": "m-1", "text": "Hola"})
    ]

    controller.deactivate()


def test_inbox_controller_uses_thread_alias_runtime_for_permissions() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate()
    _pump_events()

    assert snapshots
    assert snapshots[-1]["runtime_status"]["alias_id"] == "ventas"
    assert snapshots[-1]["runtime_status"]["is_running"] is True
    assert snapshots[-1]["thread_runtime_status"]["alias_id"] == "soporte"
    assert snapshots[-1]["thread_runtime_status"]["is_running"] is False
    assert snapshots[-1]["thread_permissions"]["runtime_active"] is False
    assert snapshots[-1]["thread_permissions"]["can_manual_send"] is True

    controller.deactivate()


def test_inbox_controller_auto_selects_without_opening_thread_until_user_clicks() -> None:
    _app()
    service = _FakeInboxService()
    service.threads["acc-1:thread-1"] = {
        "thread_key": "acc-1:thread-1",
        "account_id": "acc-1",
        "recipient_username": "cliente1",
        "messages": [],
    }
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate()
    _pump_events()

    assert snapshots[-1]["current_thread_key"] == "acc-1:thread-1"
    assert service.open_thread_calls == []
    assert snapshots[-1]["loading"] is False

    controller.select_thread("acc-1:thread-1")
    _pump_events()

    assert service.open_thread_calls == ["acc-1:thread-1"]
    assert snapshots[-1]["loading"] is True

    controller.deactivate()


def test_inbox_controller_stops_loading_and_exposes_error_when_thread_read_fails() -> None:
    _app()
    service = _FakeInboxService()
    controller = InboxController(service)
    snapshots: list[dict[str, Any]] = []
    controller.snapshot_changed.connect(lambda payload: snapshots.append(dict(payload)))

    controller.activate(initial_thread_key="acc-2:thread-2")
    _pump_events()

    assert snapshots[-1]["loading"] is True

    service.threads["acc-2:thread-2"] = {
        "thread_key": "acc-2:thread-2",
        "account_id": "acc-2",
        "recipient_username": "cliente2",
        "thread_status": "failed",
        "thread_error": "reader_task_timeout",
        "messages": [],
    }
    service.events.cache_updated.emit(
        {
            "updated_at": time.time(),
            "thread_keys": ["acc-2:thread-2"],
        }
    )
    _pump_events(8)

    assert snapshots[-1]["loading"] is False
    assert snapshots[-1]["thread_status"] == "failed"
    assert snapshots[-1]["thread_error"] == "reader_task_timeout"

    controller.deactivate()


def test_conversation_list_model_paginates_visible_rows() -> None:
    _app()
    model = ConversationListModel()
    rows = [
        {
            "thread_key": f"acc-1:thread-{index}",
            "display_name": f"Cliente {index}",
        }
        for index in range(120)
    ]

    model.set_threads(rows)
    assert model.rowCount() == 50

    visible_row = model.ensure_visible("acc-1:thread-88")
    assert visible_row == 88
    assert model.rowCount() >= 89

    model.set_threads(rows, current_thread_key="acc-1:thread-79")
    assert model.rowCount() == 80


def test_chat_view_makes_composer_read_only_when_runtime_active_and_owner_is_auto() -> None:
    _app()
    view = ChatView()
    view.set_thread(
        _thread_payload(owner="auto"),
        permissions=_thread_permissions(runtime_active=True, owner="auto", can_manual_send=False),
    )

    assert view._input.isEnabled() is True
    assert view._input.isReadOnly() is True
    assert view._send_button.isEnabled() is False
    assert view._pack_action.isEnabled() is False
    assert view._takeover_action.isEnabled() is True
    assert "solo lectura" in view._composer_hint.text().lower()


def test_chat_view_keeps_manual_composer_enabled_when_runtime_is_active() -> None:
    _app()
    view = ChatView()
    view.set_thread(
        _thread_payload(owner="manual"),
        permissions=_thread_permissions(runtime_active=True, owner="manual", can_manual_send=True),
    )

    assert view._input.isEnabled() is True
    assert view._input.isReadOnly() is False
    assert view._send_button.isEnabled() is True
    assert view._pack_action.isEnabled() is True
    assert view._release_action.isEnabled() is True
    assert view._takeover_action.isEnabled() is False


def test_chat_view_copy_and_delete_message_actions(monkeypatch) -> None:
    _app()
    monkeypatch.setattr("gui.inbox.chat_view.confirm_automation_action", lambda *args, **kwargs: True)
    view = ChatView()
    emitted: list[dict[str, Any]] = []
    view.messageDeleteRequested.connect(lambda payload: emitted.append(dict(payload)))
    view.set_thread(
        _thread_payload(owner="manual"),
        permissions=_thread_permissions(runtime_active=False, owner="manual", can_manual_send=True),
    )
    message = {
        "block_id": "msg-1",
        "message_id": "msg-1",
        "direction": "inbound",
        "text": "Copiame",
        "timestamp": 100.0,
    }

    view._copy_message(message)
    view._confirm_delete_message(message)

    assert QApplication.clipboard().text() == "Copiame"
    assert emitted == [message]


def test_actions_panel_blocks_ai_and_packs_when_runtime_is_active_and_owner_is_auto() -> None:
    _app()
    panel = ActionsPanel()
    panel.set_packs([{"id": "pack-1", "name": "Pack Bienvenida"}])
    panel.set_thread(
        _thread_payload(owner="auto"),
        permissions=_thread_permissions(runtime_active=True, owner="auto", can_manual_send=False),
    )

    assert panel._ai_button.isEnabled() is False
    assert panel._packs.isEnabled() is False


def test_actions_panel_enables_ai_and_packs_when_runtime_is_stopped() -> None:
    _app()
    panel = ActionsPanel()
    panel.set_packs([{"id": "pack-1", "name": "Pack Bienvenida"}])
    panel.set_thread(
        _thread_payload(owner="auto"),
        permissions=_thread_permissions(runtime_active=False, owner="auto", can_manual_send=True),
    )

    assert panel._ai_button.isEnabled() is True
    assert panel._packs.isEnabled() is True


def test_actions_panel_shows_scrollable_suggestion_preview_and_insert_action() -> None:
    _app()
    panel = ActionsPanel()
    emitted: list[str] = []
    panel.suggestionInsertRequested.connect(emitted.append)
    thread = _thread_payload(owner="auto")
    thread["suggestion_status"] = "ready"
    thread["suggested_reply"] = "Texto sugerido para pegar."
    thread["suggested_reply_at"] = 100.0
    panel.set_thread(
        thread,
        permissions=_thread_permissions(runtime_active=False, owner="auto", can_manual_send=True),
    )

    assert panel._suggestion_preview.toPlainText() == "Texto sugerido para pegar."
    assert panel._insert_suggestion_button.isEnabled() is True
    panel._insert_suggestion_button.click()
    assert emitted == ["Texto sugerido para pegar."]


def test_inbox_view_runtime_buttons_reflect_selected_alias_state() -> None:
    _app()
    view = InboxView(_page_context(), _ViewController())
    payload = {
        "metrics": {"threads": 0, "unread": 0, "pending": 0},
        "sync_label": "Cache local",
        "runtime_aliases": ["ventas"],
        "runtime_status": {"alias_id": "ventas", "is_running": True, "mode": "both"},
        "rows": [],
        "total_count": 0,
        "current_thread_key": "",
        "thread": None,
        "thread_permissions": {},
        "messages": [],
        "packs": [],
        "actions_status": "Selecciona una conversacion para habilitar IA y packs.",
        "loading": False,
        "force_scroll_to_bottom": False,
    }

    view._apply_snapshot(payload)

    assert view._start_runtime.isEnabled() is False
    assert view._stop_runtime.isEnabled() is True
    assert view._mode_combo.isEnabled() is False
    assert view._delay_min.isEnabled() is False

    payload["runtime_status"] = {"alias_id": "ventas", "is_running": False, "mode": "both"}
    view._apply_snapshot(payload)

    assert view._start_runtime.isEnabled() is True
    assert view._stop_runtime.isEnabled() is False
    assert view._mode_combo.isEnabled() is True
    assert view._delay_min.isEnabled() is True


def test_inbox_view_refreshes_actions_panel_when_suggestion_projection_changes() -> None:
    _app()
    view = InboxView(_page_context(), _ViewController())
    payload = {
        "metrics": {"threads": 1, "unread": 0, "pending": 0},
        "sync_label": "Cache local",
        "runtime_aliases": ["ventas"],
        "runtime_status": {"alias_id": "ventas", "is_running": False, "mode": "both"},
        "rows": [_thread_payload()],
        "total_count": 1,
        "current_thread_key": "acc-1:thread-1",
        "thread": _thread_payload(),
        "thread_permissions": _thread_permissions(runtime_active=False, owner="auto", can_manual_send=True),
        "messages": [],
        "packs": [],
        "actions_status": "Lista para accionar",
        "loading": False,
        "force_scroll_to_bottom": False,
    }

    view._apply_snapshot(payload)
    assert view._actions_panel._suggestion_preview.toPlainText() == ""

    updated_thread = dict(payload["thread"])
    updated_thread["suggestion_status"] = "ready"
    updated_thread["suggested_reply"] = "Nueva sugerencia visible."
    updated_thread["suggested_reply_at"] = 150.0
    payload["thread"] = updated_thread
    view._apply_snapshot(payload)

    assert view._actions_panel._suggestion_preview.toPlainText() == "Nueva sugerencia visible."
