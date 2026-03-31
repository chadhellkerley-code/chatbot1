from __future__ import annotations

import threading
import time

from gui.task_runner import LogStore, TaskManager
from runtime.runtime import STOP_EVENT, reset_stop_event


def test_task_manager_can_stop_isolated_task_without_touching_global_event() -> None:
    reset_stop_event()
    logs = LogStore()
    manager = TaskManager(logs)
    ready = threading.Event()
    finished = threading.Event()
    global_states: list[bool] = []

    def _target() -> None:
        ready.set()
        deadline = time.time() + 5.0
        while time.time() < deadline and not STOP_EVENT.is_set():
            time.sleep(0.01)
        global_states.append(STOP_EVENT.global_is_set())
        finished.set()

    try:
        manager.start_task("isolated", _target, isolated_stop=True)
        assert ready.wait(1.0)

        manager.request_task_stop("isolated", "isolated stop requested by test")

        assert finished.wait(2.0)
        assert STOP_EVENT.global_is_set() is False
        assert global_states == [False]
    finally:
        manager.shutdown("test cleanup")
        reset_stop_event()


def test_task_manager_preserves_global_stop_for_non_isolated_tasks() -> None:
    reset_stop_event()
    logs = LogStore()
    manager = TaskManager(logs)
    ready = threading.Event()
    finished = threading.Event()

    def _target() -> None:
        ready.set()
        deadline = time.time() + 5.0
        while time.time() < deadline and not STOP_EVENT.is_set():
            time.sleep(0.01)
        finished.set()

    try:
        manager.start_task("shared", _target)
        assert ready.wait(1.0)

        manager.request_task_stop("shared", "shared stop requested by test")

        assert finished.wait(2.0)
        assert STOP_EVENT.global_is_set() is True
    finally:
        manager.shutdown("test cleanup")
        reset_stop_event()


def test_task_manager_request_task_stop_ignores_missing_named_task() -> None:
    reset_stop_event()
    logs = LogStore()
    manager = TaskManager(logs)

    try:
        manager.request_task_stop("missing-task", "missing stop requested by test")
        assert STOP_EVENT.global_is_set() is False
    finally:
        manager.shutdown("test cleanup")
        reset_stop_event()
