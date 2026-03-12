from __future__ import annotations

import os
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import core.storage_atomic as storage_atomic
from PySide6.QtWidgets import QApplication

from gui.inbox.chat_view import ChatView
from gui.snapshot_queries import build_system_logs_snapshot
from gui.task_runner import LogStore


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _pump_events(iterations: int = 4) -> None:
    app = _app()
    for _ in range(max(1, iterations)):
        app.processEvents()


def _wait_until(predicate, *, timeout: float = 1.5) -> bool:  # noqa: ANN001
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        _pump_events(2)
        if predicate():
            return True
        time.sleep(0.01)
    _pump_events(2)
    return bool(predicate())


def _thread_payload() -> dict[str, object]:
    return {
        "thread_key": "acc-1:thread-1",
        "display_name": "Cliente Uno",
        "account_id": "acc-1",
        "recipient_username": "cliente1",
    }


def _message_row(index: int) -> dict[str, object]:
    return {
        "message_id": f"msg-{index}",
        "direction": "outbound" if index % 2 else "inbound",
        "text": f"Mensaje {index}",
        "timestamp": float(100 + index),
        "delivery_status": "sent",
    }


def test_build_system_logs_snapshot_streams_memory_and_file_deltas(tmp_path: Path) -> None:
    logs = LogStore()
    log_path = tmp_path / "app.log"
    logs.append("uno\n")
    log_path.write_text("disk-1\n", encoding="utf-8")

    first = build_system_logs_snapshot(logs, log_path=log_path, log_cursor=0, file_cursor=0)
    assert first["log_text"] == "uno\n"
    assert first["file_text"] == "disk-1\n"
    assert bool(first["file_reset"]) is True

    logs.append("dos\n")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("disk-2\n")

    second = build_system_logs_snapshot(
        logs,
        log_path=log_path,
        log_cursor=int(first["log_cursor"]),
        file_cursor=int(first["file_cursor"]),
    )
    assert second["log_text"] == "dos\n"
    assert str(second["file_text"]).replace("\r\n", "\n") == "disk-2\n"
    assert bool(second["log_reset"]) is False
    assert bool(second["file_reset"]) is False


def test_load_json_file_uses_cache_until_file_changes(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "payload.json"
    storage_atomic.atomic_write_json(path, {"value": 1})

    calls = 0
    real_json_load = storage_atomic.json.load

    def _counting_load(handle):  # noqa: ANN001
        nonlocal calls
        calls += 1
        return real_json_load(handle)

    monkeypatch.setattr(storage_atomic.json, "load", _counting_load)

    first = storage_atomic.load_json_file(path, {})
    first["value"] = 99
    second = storage_atomic.load_json_file(path, {})

    assert calls == 1
    assert second["value"] == 1

    time.sleep(0.02)
    storage_atomic.atomic_write_json(path, {"value": 2})
    third = storage_atomic.load_json_file(path, {})

    assert calls == 2
    assert third["value"] == 2


def test_load_jsonl_entries_uses_cache_until_append(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "payload.jsonl"
    path.write_text('{"value": 1}\n{"value": 2}\n', encoding="utf-8")

    calls = 0
    real_json_loads = storage_atomic.json.loads

    def _counting_loads(raw: str, *args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal calls
        calls += 1
        return real_json_loads(raw, *args, **kwargs)

    monkeypatch.setattr(storage_atomic.json, "loads", _counting_loads)

    assert [row["value"] for row in storage_atomic.load_jsonl_entries(path)] == [1, 2]
    assert [row["value"] for row in storage_atomic.load_jsonl_entries(path)] == [1, 2]
    assert calls == 2

    time.sleep(0.02)
    storage_atomic.atomic_append_jsonl(path, {"value": 3})
    assert [row["value"] for row in storage_atomic.load_jsonl_entries(path)] == [1, 2, 3]
    assert calls == 5


def test_chat_view_renders_large_threads_in_batches_without_blocking() -> None:
    _app()
    view = ChatView()
    view.set_thread(_thread_payload())
    rows = [_message_row(index) for index in range(180)]

    started = time.perf_counter()
    view.set_messages(rows, force_scroll_to_bottom=True)
    elapsed = time.perf_counter() - started

    assert elapsed < 0.08
    assert _wait_until(lambda: len(view._message_widgets) == 180)
    assert view._rendering is False


def test_chat_view_appends_new_message_without_rebuilding_existing_widgets() -> None:
    _app()
    view = ChatView()
    view.set_thread(_thread_payload())
    initial_rows = [_message_row(index) for index in range(3)]
    view.set_messages(initial_rows, force_scroll_to_bottom=True)
    assert _wait_until(lambda: len(view._message_widgets) == 3)

    existing_widgets = list(view._message_widgets)
    updated_rows = initial_rows + [_message_row(3)]
    view.set_messages(updated_rows, force_scroll_to_bottom=True)

    assert _wait_until(lambda: len(view._message_widgets) == 4)
    assert view._message_widgets[0] is existing_widgets[0]
    assert view._message_widgets[1] is existing_widgets[1]
    assert view._message_widgets[2] is existing_widgets[2]
    assert view._message_widgets[3] not in existing_widgets
