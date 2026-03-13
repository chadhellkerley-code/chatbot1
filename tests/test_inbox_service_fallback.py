from __future__ import annotations

import json
from pathlib import Path

from application.services.base import ServiceContext
from application.services.inbox_service import InboxService


class FakeInboxEngine:
    def __init__(self, _root_dir: Path) -> None:
        self._threads: dict[str, dict] = {}
        self.opened: list[str] = []
        self.start_calls = 0
        self.shutdown_calls = 0
        self.diagnostics_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def shutdown(self) -> None:
        self.shutdown_calls += 1

    def list_threads(self, filter_mode: str = "all") -> list[dict]:
        del filter_mode
        rows: list[dict] = []
        for row in self._threads.values():
            payload = dict(row)
            payload.pop("messages", None)
            rows.append(payload)
        return rows

    def get_thread(self, thread_key: str) -> dict | None:
        row = self._threads.get(thread_key)
        return dict(row) if isinstance(row, dict) else None

    def prime_thread_snapshot(
        self,
        thread_row: dict,
        *,
        messages: list[dict] | None = None,
    ) -> bool:
        payload = dict(thread_row)
        payload["messages"] = [dict(item) for item in messages or []]
        self._threads[str(payload.get("thread_key") or "")] = payload
        return True

    def open_thread(self, thread_key: str) -> bool:
        self.opened.append(thread_key)
        return thread_key in self._threads

    def send_message(self, thread_key: str, text: str) -> str:
        return f"{thread_key}:{text}"

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        return bool(thread_key and pack_id)

    def request_ai_suggestion(self, thread_key: str) -> bool:
        return bool(thread_key)

    def list_packs(self) -> list[dict]:
        return []

    def enqueue_periodic_sync(self) -> None:
        return None

    def diagnostics(self) -> dict:
        self.diagnostics_calls += 1
        return {
            "thread_count": len(self._threads),
            "message_groups": len(self._threads),
            "queued_tasks": 0,
            "worker_count": 0,
            "dedupe_pending": 0,
        }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_service(tmp_path: Path, monkeypatch) -> InboxService:
    monkeypatch.setattr("application.services.inbox_service.InboxEngine", FakeInboxEngine)
    _write_json(
        tmp_path / "storage" / "accounts" / "accounts.json",
        [
            {
                "username": "matidiazlife",
                "active": True,
                "connected": True,
            }
        ],
    )
    _write_json(
        tmp_path / "storage" / "conversation_engine.json",
        {
            "conversations": {
                "matidiazlife|thread-1": {
                    "account": "matidiazlife",
                    "thread_id": "thread-1",
                    "recipient_username": "lead_user",
                    "messages_sent": [
                        {
                            "text": "Hola desde bot",
                            "first_sent_at": 100.0,
                            "last_sent_at": 100.0,
                            "message_id": "msg-out-1",
                        }
                    ],
                    "last_message_sent_at": 100.0,
                    "last_message_received_at": 200.0,
                    "last_message_sender": "lead",
                    "last_inbound_id_seen": "msg-in-1",
                    "stage": "active",
                },
                "oldaccount|thread-2": {
                    "account": "oldaccount",
                    "thread_id": "thread-2",
                    "recipient_username": "legacy_user",
                    "last_message_received_at": 300.0,
                    "last_message_sender": "lead",
                },
            }
        },
    )
    _write_json(
        tmp_path / "storage" / "conversation_state.json",
        {
            "conversations": {
                "matidiazlife|thread-1": {
                    "status": "open",
                }
            }
        },
    )
    return InboxService(ServiceContext(root_dir=tmp_path))


def test_list_threads_ignores_legacy_json_when_sqlite_is_empty(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path, monkeypatch)

    rows = service.list_threads()

    assert rows == []
    service.shutdown()


def test_open_thread_does_not_seed_legacy_messages_into_sqlite(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path, monkeypatch)

    opened = service.open_thread("matidiazlife:thread-1")
    thread = service.get_thread("matidiazlife:thread-1")

    assert opened is False
    assert thread is None
    service.shutdown()


def test_diagnostics_does_not_start_backend_when_inbox_is_idle(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path, monkeypatch)

    payload = service.diagnostics()

    assert service._engine.start_calls == 0
    assert service._engine.diagnostics_calls == 0
    assert payload["backend_started"] is False
    assert payload["projection_threads"] == 0
    service.shutdown()
