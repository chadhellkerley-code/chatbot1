from __future__ import annotations

from pathlib import Path

from application.services.base import ServiceContext
from application.services.leads_service import LeadsService
from runtime.runtime import STOP_EVENT, reset_stop_event


class _FakeTaskRunner:
    def __init__(self) -> None:
        self.stop_calls: list[tuple[str, str]] = []

    def request_task_stop(self, name: str, reason: str) -> None:
        self.stop_calls.append((str(name or ""), str(reason or "")))


def test_leads_service_stop_filtering_uses_task_scoped_stop_when_available(tmp_path: Path) -> None:
    reset_stop_event()
    service = LeadsService(ServiceContext(root_dir=tmp_path))
    task_runner = _FakeTaskRunner()

    try:
        service.stop_filtering("isolated stop requested", task_runner=task_runner)

        assert task_runner.stop_calls == [("leads_filter", "isolated stop requested")]
        assert STOP_EVENT.global_is_set() is False
    finally:
        reset_stop_event()


def test_leads_service_stop_filtering_falls_back_to_global_stop_without_task_runner(tmp_path: Path) -> None:
    reset_stop_event()
    service = LeadsService(ServiceContext(root_dir=tmp_path))

    try:
        service.stop_filtering("global stop requested")

        assert STOP_EVENT.global_is_set() is True
    finally:
        reset_stop_event()
