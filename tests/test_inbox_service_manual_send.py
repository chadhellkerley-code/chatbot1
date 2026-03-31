from __future__ import annotations

from typing import Any

from application.services.inbox_service import InboxService


class _FakeRuntime:
    def __init__(self) -> None:
        self.backend_started = 0
        self.rebuilds: list[tuple[str, list[str]]] = []

    def ensure_backend_started(self) -> None:
        self.backend_started += 1

    def ensure_thread_seeded(self, _thread_key: str) -> bool:
        return True

    def request_rebuild(self, *, reason: str, thread_keys: list[str]) -> None:
        self.rebuilds.append((reason, list(thread_keys)))


class _FakeSender:
    def __init__(self) -> None:
        self.message_calls: list[tuple[str, str, str]] = []
        self.pack_calls: list[tuple[str, str, str]] = []

    def queue_message(self, thread_key: str, text: str, *, job_type: str = "manual_reply") -> str:
        self.message_calls.append((thread_key, text, job_type))
        return "local-msg-1"

    def queue_pack(self, thread_key: str, pack_id: str, *, job_type: str = "manual_pack") -> bool:
        self.pack_calls.append((thread_key, pack_id, job_type))
        return True


class _FakeEngine:
    def __init__(self, thread: dict[str, Any]) -> None:
        self._thread = dict(thread)
        self._sender = _FakeSender()

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        if thread_key != str(self._thread.get("thread_key") or "").strip():
            return None
        return dict(self._thread)


class _FakeAutomation:
    def __init__(self, *, allow_takeover: bool = True) -> None:
        self.allow_takeover = allow_takeover
        self.takeover_calls: list[tuple[str, str]] = []
        self.manual_send_allowed_calls: list[dict[str, Any]] = []
        self.manual_takeover_allowed_calls: list[dict[str, Any]] = []

    def manual_send_allowed(self, thread: dict[str, Any] | None) -> bool:
        row = dict(thread or {})
        self.manual_send_allowed_calls.append(row)
        return (
            str(row.get("owner") or "").strip().lower() == "manual"
            and str(row.get("bucket") or "").strip().lower() == "qualified"
            and str(row.get("status") or "").strip().lower() == "open"
        )

    def manual_takeover_allowed(self, thread: dict[str, Any] | None) -> bool:
        row = dict(thread or {})
        self.manual_takeover_allowed_calls.append(row)
        return self.allow_takeover

    def manual_takeover(self, thread_key: str, *, operator_id: str) -> dict[str, Any] | None:
        self.takeover_calls.append((thread_key, operator_id))
        if not self.allow_takeover:
            return None
        return {
            "thread_key": thread_key,
            "owner": "manual",
            "bucket": "qualified",
            "status": "open",
            "manual_lock": True,
            "manual_assignee": operator_id,
        }


def _build_service(thread: dict[str, Any], automation: _FakeAutomation) -> InboxService:
    service = InboxService.__new__(InboxService)
    service.context = None
    service._engine = _FakeEngine(thread)
    service._runtime = _FakeRuntime()
    service._automation = automation
    return service


def test_send_message_takes_over_auto_thread_before_manual_send_gate() -> None:
    automation = _FakeAutomation()
    service = _build_service(
        {
            "thread_key": "acc1:thread-a",
            "owner": "auto",
            "bucket": "all",
            "status": "open",
        },
        automation,
    )

    local_id = service.send_message("acc1:thread-a", "Hola manual")

    assert local_id == "local-msg-1"
    assert automation.takeover_calls == [("acc1:thread-a", "inbox_ui")]
    assert automation.manual_send_allowed_calls == [
        {
            "thread_key": "acc1:thread-a",
            "owner": "manual",
            "bucket": "qualified",
            "status": "open",
            "manual_lock": True,
            "manual_assignee": "inbox_ui",
        }
    ]
    assert service._engine._sender.message_calls == [("acc1:thread-a", "Hola manual", "manual_reply")]
    assert service._runtime.rebuilds == [("send_message", ["acc1:thread-a"])]


def test_send_pack_takes_over_auto_thread_before_manual_send_gate() -> None:
    automation = _FakeAutomation()
    service = _build_service(
        {
            "thread_key": "acc1:thread-a",
            "owner": "auto",
            "bucket": "all",
            "status": "open",
        },
        automation,
    )

    queued = service.send_pack("acc1:thread-a", "pack-1")

    assert queued is True
    assert automation.takeover_calls == [("acc1:thread-a", "inbox_ui")]
    assert automation.manual_send_allowed_calls == [
        {
            "thread_key": "acc1:thread-a",
            "owner": "manual",
            "bucket": "qualified",
            "status": "open",
            "manual_lock": True,
            "manual_assignee": "inbox_ui",
        }
    ]
    assert service._engine._sender.pack_calls == [("acc1:thread-a", "pack-1", "manual_pack")]
    assert service._runtime.rebuilds == [("send_pack", ["acc1:thread-a"])]


def test_send_message_keeps_disqualified_thread_blocked_when_takeover_is_not_allowed() -> None:
    automation = _FakeAutomation(allow_takeover=False)
    service = _build_service(
        {
            "thread_key": "acc1:thread-a",
            "owner": "auto",
            "bucket": "disqualified",
            "status": "closed",
        },
        automation,
    )

    local_id = service.send_message("acc1:thread-a", "No deberia salir")

    assert local_id == ""
    assert automation.manual_takeover_allowed_calls == [
        {
            "thread_key": "acc1:thread-a",
            "owner": "auto",
            "bucket": "disqualified",
            "status": "closed",
        }
    ]
    assert automation.takeover_calls == []
    assert automation.manual_send_allowed_calls == []
    assert service._engine._sender.message_calls == []
    assert service._runtime.rebuilds == []
