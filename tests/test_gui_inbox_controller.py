from __future__ import annotations

import os
import time
from typing import Any

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from gui.inbox.conversation_list import ConversationListModel
from gui.inbox.inbox_controller import InboxController


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
        self.rows = [
            {
                "thread_key": "acc-1:thread-1",
                "account_id": "acc-1",
                "display_name": "Cliente Uno",
                "recipient_username": "cliente1",
                "unread_count": 1,
                "last_message_text": "Hola",
                "last_message_direction": "inbound",
                "last_message_timestamp": 100.0,
            },
            {
                "thread_key": "acc-2:thread-2",
                "account_id": "acc-2",
                "display_name": "Cliente Dos",
                "recipient_username": "cliente2",
                "unread_count": 0,
                "last_message_text": "Seguimos",
                "last_message_direction": "outbound",
                "last_message_timestamp": 90.0,
            },
        ]
        self.threads: dict[str, dict[str, Any]] = {
            "acc-1:thread-1": {
                "thread_key": "acc-1:thread-1",
                "account_id": "acc-1",
                "recipient_username": "cliente1",
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

    def list_packs(self) -> list[dict[str, Any]]:
        return [{"id": "pack-1", "name": "Pack Bienvenida"}]


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
