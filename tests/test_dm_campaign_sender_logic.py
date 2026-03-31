from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from runtime.runtime import reset_stop_event
from src.auth.persistent_login import ChallengeRequired
from src.dm_campaign.contracts import CampaignSendResult, CampaignSendStatus
from src.dm_campaign.proxy_workers_runner import (
    ProxyWorker,
    TemplateRotator,
    _campaign_failure_reason,
    _parse_send_result,
)
from src.runtime.playwright_runtime import PersistentProfileOwnershipError
from src.transport.human_instagram_sender import HumanInstagramSender


@pytest.fixture(autouse=True)
def _reset_stop_event_between_tests():
    reset_stop_event()
    yield
    reset_stop_event()


def test_parse_send_result_keeps_send_unverified_blocked_as_failure() -> None:
    ok, detail, payload = _parse_send_result(
        (
            False,
            "send_unverified_blocked",
            {
                "reason_code": "SENT_UNVERIFIED",
                "verified": False,
            },
        )
    )

    assert ok is False
    assert detail == "send_unverified_blocked"
    assert payload["reason_code"] == "SENT_UNVERIFIED"


def test_parse_send_result_accepts_explicit_sent_unverified_flag() -> None:
    ok, detail, payload = _parse_send_result(
        (
            False,
            "",
            {
                "reason_code": "SENT_UNVERIFIED",
                "sent_unverified": True,
            },
        )
    )

    assert ok is True
    assert detail == "sent_unverified"
    assert payload["sent_unverified"] is True


def test_text_contains_exact_username_accepts_full_row_text() -> None:
    sender = HumanInstagramSender()

    ok, candidate = sender._text_contains_exact_username(
        "Cesar Alejandro Morales\ncesarmoraless08\n~Haciendo amigos por el mundo",
        "cesarmoraless08",
    )

    assert ok is True
    assert candidate == "cesarmoraless08"


def test_filter_recent_message_texts_removes_sidebar_and_timestamp_noise() -> None:
    sender = HumanInstagramSender()

    filtered = sender._filter_recent_message_texts(
        [
            "ryneth.ink",
            "Instagram",
            "11:20",
            "1 d",
            "Tu: como va todo?",
            "Visto",
        ],
        limit=10,
    )

    assert filtered == ["ryneth.ink", "Tu: como va todo?", "Visto"]


def test_sender_uses_sidebar_thread_resolution_for_outbound_flow(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    class _FakeComposer:
        async def press(self, _key: str) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    fake_composer = _FakeComposer()
    opener_calls: list[tuple[str, object]] = []
    stage_events: list[str] = []

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        opener_calls.append(("ensure_inbox_surface", deadline > 0))
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        opener_calls.append(("open_thread_from_sidebar", (username, deadline > 0)))
        fake_page.url = "https://www.instagram.com/direct/t/thread-123/"
        return SimpleNamespace(opened=True, reason="ok", method="sidebar_search", thread_id="thread-123")

    async def _return_fake_composer(*_args, **_kwargs):
        return fake_composer

    async def _return_empty_text(*_args, **_kwargs):
        return ""

    async def _return_false(*_args, **_kwargs):
        return False

    async def _build_snapshot(*_args, **_kwargs):
        return SimpleNamespace(
            snippet="Hola",
            snippet_norm="hola",
            before_hits=0,
            before_tail=[],
        )

    async def _wait_network(*_args, **_kwargs):
        return True, {"matched_responses": 1}

    async def _wait_dom(*_args, **_kwargs):
        return True, {"mode": "dom"}

    async def _wait_bubble(*_args, **_kwargs):
        return True, {"visible": True}

    async def _capture_success(*_args, **_kwargs):
        return None

    async def _ensure_surface_ready(*_args, **_kwargs):
        return fake_composer, {"ok": True, "reason_code": "", "normalized": False, "diagnostic_reason_codes": []}

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "ensure_visible_chat_surface_ready", _ensure_surface_ready)
    monkeypatch.setattr(sender._message_composer, "type_message", _noop)
    monkeypatch.setattr(sender._message_composer, "composer_text", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "click_send_button", _return_false)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(sender._delivery_verifier, "wait_send_network_ok", _wait_network)
    monkeypatch.setattr(sender._delivery_verifier, "wait_dom_send_confirmation", _wait_dom)
    monkeypatch.setattr(sender._delivery_verifier, "verify_message_visible_after_send", _wait_bubble)
    monkeypatch.setattr(
        sender._delivery_verifier,
        "decide_confirmation",
        lambda **_kwargs: SimpleNamespace(
            ok=True,
            verified=True,
            sent_unverified=False,
            detail="sent",
            verify_source="network",
            reason_code="",
            stage="",
        ),
    )
    monkeypatch.setattr(sender, "_capture_success", _capture_success)

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
            stage_callback=lambda stage, _payload: stage_events.append(stage),
        )
    )

    assert ok is True
    assert detail == "sent"
    assert opener_calls == [
        ("ensure_inbox_surface", True),
        ("open_thread_from_sidebar", ("lead1", True)),
    ]
    non_flow_stage_events = [stage for stage in stage_events if stage != "flow_stage"]
    assert non_flow_stage_events == ["opening_session", "opening_dm", "sending", "sending"]
    assert "flow_stage" in stage_events
    assert payload["method"] == "outbound_compose"
    assert payload["thread_open_method"] == "sidebar_search"
    assert payload["thread_id"] == "thread-123"


def test_sender_returns_inbox_not_ready_as_retryable_failure(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/inbox/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    fake_session = SimpleNamespace(page=_FakePage())

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return False

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "INBOX_NOT_READY"
    assert payload["reason_code"] == "INBOX_NOT_READY"
    assert "skip_reason" not in payload


def test_sender_does_not_start_send_when_sidebar_is_unavailable(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/inbox/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    fake_session = SimpleNamespace(page=_FakePage())

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        return SimpleNamespace(opened=False, reason="sidebar_unavailable", method="sidebar_search", thread_id="")

    async def _type_message(*_args, **_kwargs):
        raise AssertionError("message send must not start when the sidebar is unavailable")

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "type_message", _type_message)
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "UI_NOT_FOUND"
    assert payload["reason_code"] == "SIDEBAR_UNAVAILABLE"


def test_sender_preserves_challenge_reason_during_session_open(monkeypatch) -> None:
    sender = HumanInstagramSender()

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )
    monkeypatch.setattr(
        sender._session_manager,
        "open_session",
        lambda **_kwargs: (_ for _ in ()).throw(ChallengeRequired("challenge_required")),
    )
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "session_open_failed"
    assert payload["reason_code"] == "CHALLENGE_REQUIRED"


def test_sender_normalizes_profile_conflict_during_session_open(monkeypatch) -> None:
    sender = HumanInstagramSender()

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    def _raise_profile_conflict(**_kwargs):
        raise PersistentProfileOwnershipError(
            profile_dir="runtime/browser_profiles/cuenta1",
            requested_mode="headful",
            active_mode="headless",
            runtime_id="runtime-b",
            active_runtime_id="runtime-a",
            owner_module="tests",
        )

    monkeypatch.setattr(sender._session_manager, "open_session", _raise_profile_conflict)
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "session_open_failed"
    assert payload["reason_code"] == "PROFILE_MODE_CONFLICT"


def test_sender_uses_usable_composer_and_types_before_snapshot(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    class _FakeComposer:
        async def press(self, _key: str) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    fake_composer = _FakeComposer()
    call_order: list[str] = []

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        fake_page.url = "https://www.instagram.com/direct/t/thread-123/"
        return SimpleNamespace(opened=True, reason="ok", method="sidebar_search", thread_id="thread-123")

    async def _wait_for_usable_composer(*_args, **_kwargs):
        call_order.append("composer_ready")
        return fake_composer

    async def _ensure_surface_ready(*_args, **_kwargs):
        call_order.append("surface_ready")
        return fake_composer, {"ok": True, "reason_code": "", "normalized": True, "diagnostic_reason_codes": []}

    async def _return_empty_text(*_args, **_kwargs):
        return ""

    async def _return_false(*_args, **_kwargs):
        return False

    async def _build_snapshot(*_args, **_kwargs):
        call_order.append("snapshot")
        return SimpleNamespace(
            snippet="Hola",
            snippet_norm="hola",
            before_hits=0,
            before_tail=[],
        )

    async def _wait_network(*_args, **_kwargs):
        return True, {"matched_responses": 1}

    async def _wait_dom(*_args, **_kwargs):
        return True, {"mode": "dom"}

    async def _wait_bubble(*_args, **_kwargs):
        return True, {"visible": True}

    async def _capture_success(*_args, **_kwargs):
        return None

    async def _type_message(*_args, **_kwargs):
        call_order.append("type_message")
        return None

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "ensure_visible_chat_surface_ready", _ensure_surface_ready)
    monkeypatch.setattr(sender._message_composer, "type_message", _type_message)
    monkeypatch.setattr(sender._message_composer, "wait_for_text_change", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "composer_text", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "click_send_button", _return_false)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(sender._delivery_verifier, "wait_send_network_ok", _wait_network)
    monkeypatch.setattr(sender._delivery_verifier, "wait_dom_send_confirmation", _wait_dom)
    monkeypatch.setattr(sender._delivery_verifier, "verify_message_visible_after_send", _wait_bubble)
    monkeypatch.setattr(
        sender._delivery_verifier,
        "decide_confirmation",
        lambda **_kwargs: SimpleNamespace(
            ok=True,
            verified=True,
            sent_unverified=False,
            detail="sent",
            verify_source="network",
            reason_code="",
            stage="",
        ),
    )
    monkeypatch.setattr(sender, "_capture_success", _capture_success)

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is True
    assert detail == "sent"
    assert payload["thread_id"] == "thread-123"
    assert call_order == ["surface_ready", "type_message", "snapshot"]


def test_sender_fails_when_no_composer_is_returned(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    events: list[str] = []

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        fake_page.url = "https://www.instagram.com/direct/t/thread-123/"
        return SimpleNamespace(opened=True, reason="ok", method="sidebar_search", thread_id="thread-123")

    async def _wait_for_usable_composer(*_args, **_kwargs):
        events.append("composer_wait")
        return None

    async def _ensure_surface_ready(*_args, **_kwargs):
        events.append("surface_wait")
        return None, {
            "ok": False,
            "reason_code": "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION",
            "normalized": True,
            "diagnostic_reason_codes": [],
        }

    async def _type_message(*_args, **_kwargs):
        raise AssertionError("typing must not start when no usable composer appears")

    async def _build_snapshot(*_args, **_kwargs):
        raise AssertionError("snapshot must stay out of the hot path when composer readiness fails")

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "ensure_visible_chat_surface_ready", _ensure_surface_ready)
    monkeypatch.setattr(sender._message_composer, "type_message", _type_message)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "THREAD_OPEN_FAILED"
    assert payload["reason_code"] == "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION"
    assert payload["composer_reason_code"] == "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION"
    assert events == ["surface_wait"]


def test_sender_uses_visible_composer_when_surface_audit_is_imperfect(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    fake_composer = object()
    call_order: list[str] = []

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        fake_page.url = "https://www.instagram.com/direct/t/thread-123/"
        return SimpleNamespace(opened=True, reason="ok", method="sidebar_search", thread_id="thread-123")

    async def _ensure_surface_ready(*_args, **_kwargs):
        call_order.append("surface_ready")
        return fake_composer, {
            "ok": False,
            "reason_code": "COMPOSER_OVERLAPPED",
            "normalized": True,
            "failed_checks": ["composer_overlapped"],
            "diagnostic_reason_codes": ["HEADER_PARTIAL_HYDRATION"],
        }

    async def _type_message(*_args, **_kwargs):
        call_order.append("type_message")
        return None

    async def _return_empty_text(*_args, **_kwargs):
        return ""

    async def _return_false(*_args, **_kwargs):
        return False

    async def _build_snapshot(*_args, **_kwargs):
        call_order.append("snapshot")
        return SimpleNamespace(
            snippet="Hola",
            snippet_norm="hola",
            before_hits=0,
            before_tail=[],
        )

    async def _wait_network(*_args, **_kwargs):
        call_order.append("network_verify")
        return True, {"matched_responses": 1}

    async def _wait_dom(*_args, **_kwargs):
        call_order.append("dom_verify")
        return True, {"mode": "dom"}

    async def _wait_bubble(*_args, **_kwargs):
        call_order.append("bubble_verify")
        return True, {"visible": True}

    async def _capture_success(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "ensure_visible_chat_surface_ready", _ensure_surface_ready)
    monkeypatch.setattr(sender._message_composer, "type_message", _type_message)
    monkeypatch.setattr(sender._message_composer, "wait_for_text_change", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "composer_text", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "click_send_button", _return_false)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(sender._delivery_verifier, "wait_send_network_ok", _wait_network)
    monkeypatch.setattr(sender._delivery_verifier, "wait_dom_send_confirmation", _wait_dom)
    monkeypatch.setattr(sender._delivery_verifier, "verify_message_visible_after_send", _wait_bubble)
    monkeypatch.setattr(
        sender._delivery_verifier,
        "decide_confirmation",
        lambda **_kwargs: SimpleNamespace(
            ok=True,
            verified=True,
            sent_unverified=False,
            detail="sent",
            verify_source="network",
            reason_code="",
            stage="",
        ),
    )
    monkeypatch.setattr(sender, "_capture_success", _capture_success)

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is True
    assert detail == "sent"
    assert payload["post_open_surface"]["ok"] is False
    assert payload["post_open_surface"]["reason_code"] == "COMPOSER_OVERLAPPED"
    assert payload["post_open_surface_diagnostic_codes"] == ["HEADER_PARTIAL_HYDRATION"]
    assert "composer_reason_code" not in payload
    assert call_order[:3] == ["surface_ready", "type_message", "snapshot"]
    assert sorted(call_order[3:]) == ["bubble_verify", "dom_verify", "network_verify"]


def test_sender_reuses_matching_current_thread_before_opening_compose(monkeypatch) -> None:
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakeHeaderLink:
        async def get_attribute(self, name: str) -> str | None:
            return "/lead1/" if name == "href" else None

    class _FakeHeaderLinks:
        async def count(self) -> int:
            return 1

        def nth(self, _index: int) -> _FakeHeaderLink:
            return _FakeHeaderLink()

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

        def locator(self, _selector: str) -> _FakeHeaderLinks:
            return _FakeHeaderLinks()

        async def evaluate(self, _script: str, payload=None) -> bool:
            return False

    class _FakeComposer:
        async def press(self, _key: str) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    fake_composer = _FakeComposer()

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(*_args, **_kwargs):
        raise AssertionError("compose flow should be skipped when the target thread is already open")

    async def _wait_for_usable_composer(*_args, **_kwargs):
        return fake_composer

    async def _ensure_surface_ready(*_args, **_kwargs):
        return fake_composer, {
            "ok": True,
            "reason_code": "",
            "normalized": False,
            "diagnostic_reason_codes": ["HEADER_PARTIAL_HYDRATION"],
        }

    async def _return_empty_text(*_args, **_kwargs):
        return ""

    async def _return_false(*_args, **_kwargs):
        return False

    async def _build_snapshot(*_args, **_kwargs):
        return SimpleNamespace(
            snippet="Hola",
            snippet_norm="hola",
            before_hits=0,
            before_tail=[],
        )

    async def _wait_network(*_args, **_kwargs):
        return True, {"matched_responses": 1}

    async def _wait_dom(*_args, **_kwargs):
        return True, {"mode": "dom"}

    async def _wait_bubble(*_args, **_kwargs):
        return True, {"visible": True}

    async def _capture_success(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "ensure_visible_chat_surface_ready", _ensure_surface_ready)
    monkeypatch.setattr(sender._message_composer, "type_message", _noop)
    monkeypatch.setattr(sender._message_composer, "wait_for_text_change", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "composer_text", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "click_send_button", _return_false)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(sender._delivery_verifier, "wait_send_network_ok", _wait_network)
    monkeypatch.setattr(sender._delivery_verifier, "wait_dom_send_confirmation", _wait_dom)
    monkeypatch.setattr(sender._delivery_verifier, "verify_message_visible_after_send", _wait_bubble)
    monkeypatch.setattr(
        sender._delivery_verifier,
        "decide_confirmation",
        lambda **_kwargs: SimpleNamespace(
            ok=True,
            verified=True,
            sent_unverified=False,
            detail="sent",
            verify_source="network",
            reason_code="",
            stage="",
        ),
    )
    monkeypatch.setattr(sender, "_capture_success", _capture_success)

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is True
    assert detail == "sent"
    assert payload["thread_id"] == "thread-123"
    assert payload["thread_open_method"] == "current_thread"
    assert payload["post_open_surface_diagnostic_codes"] == ["HEADER_PARTIAL_HYDRATION"]


def test_sender_uses_cached_account_quota_before_opening_session(monkeypatch) -> None:
    sender = HumanInstagramSender()

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("quota fallback should not run")),
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={
                "username": "cuenta1",
                "messages_per_account": 2,
                "sent_today": 2,
            },
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "account_quota_reached"
    assert payload["reason_code"] == "ACCOUNT_QUOTA_REACHED"
    assert payload["quota"] == {"sent_today": 2, "limit": 2}


def test_sender_campaign_mode_reconciles_cached_quota_with_live_sent_log(monkeypatch) -> None:
    sender = HumanInstagramSender(reconcile_live_quota=True)

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (False, 3, 2),
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={
                "username": "cuenta1",
                "messages_per_account": 2,
                "sent_today": 0,
            },
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "account_quota_reached"
    assert payload["reason_code"] == "ACCOUNT_QUOTA_REACHED"
    assert payload["quota"] == {"sent_today": 3, "limit": 2}


def test_sender_can_skip_quota_gate_when_campaign_already_preselected(monkeypatch) -> None:
    sender = HumanInstagramSender(enforce_account_quota=False)

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (False, 99, 2),
    )
    monkeypatch.setattr(
        sender._session_manager,
        "open_session",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("session_open_attempted")),
    )
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1", "messages_per_account": 2, "sent_today": 2},
            target_username="lead1",
            text="Hola",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "session_open_failed"
    assert payload["error"] == "RuntimeError('session_open_attempted')"


def _build_sender_for_unverified_confirmation_test(monkeypatch, *, refresh_confirmed: bool):
    sender = HumanInstagramSender()

    class _FakeKeyboard:
        async def press(self, _key: str) -> None:
            return None

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.instagram.com/direct/t/thread-123/"
            self.keyboard = _FakeKeyboard()
            self.goto_calls: list[str] = []

        async def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

        async def goto(self, url: str, *, wait_until: str, timeout: int) -> None:
            self.goto_calls.append(str(url))
            self.url = str(url)

    class _FakeComposer:
        async def press(self, _key: str) -> None:
            return None

    fake_page = _FakePage()
    fake_session = SimpleNamespace(page=fake_page)
    fake_composer = _FakeComposer()

    monkeypatch.setattr(
        "src.transport.human_instagram_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 0),
    )

    async def _open_session(**_kwargs):
        return fake_session

    async def _noop(*_args, **_kwargs):
        return None

    async def _ensure_inbox_surface(_page, *, deadline: float):
        return True

    async def _open_thread_from_sidebar(_page, username: str, *, deadline: float):
        fake_page.url = "https://www.instagram.com/direct/t/thread-123/"
        return SimpleNamespace(opened=True, reason="ok", method="sidebar_search", thread_id="thread-123")

    async def _return_fake_composer(*_args, **_kwargs):
        return fake_composer

    async def _return_empty_text(*_args, **_kwargs):
        return ""

    async def _return_false(*_args, **_kwargs):
        return False

    async def _build_snapshot(*_args, **_kwargs):
        return SimpleNamespace(
            snippet="Hola desde campana",
            snippet_norm="hola desde campana",
            before_hits=0,
            before_tail=[],
        )

    async def _wait_network(*_args, **_kwargs):
        return False, {"matched_responses": 0}

    dom_attempts = 0
    bubble_attempts = 0

    async def _wait_dom(*_args, **_kwargs):
        nonlocal dom_attempts
        dom_attempts += 1
        return False, {"mode": "dom_timeout", "attempt": dom_attempts}

    async def _wait_bubble(*_args, **_kwargs):
        nonlocal bubble_attempts
        bubble_attempts += 1
        ok = bool(refresh_confirmed and bubble_attempts >= 2)
        return ok, {
            "mode": "bubble_tail_match" if ok else "bubble_timeout",
            "attempt": bubble_attempts,
        }

    async def _capture_success(*_args, **_kwargs):
        return None

    async def _capture_failure_artifacts(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(
        sender._message_composer,
        "ensure_visible_chat_surface_ready",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=(fake_composer, {"ok": True, "reason_code": "", "normalized": False, "diagnostic_reason_codes": []}),
        ),
    )
    monkeypatch.setattr(sender._message_composer, "wait_composer_visible", _return_fake_composer)
    monkeypatch.setattr(sender._message_composer, "type_message", _noop)
    monkeypatch.setattr(sender._message_composer, "composer_text", _return_empty_text)
    monkeypatch.setattr(sender._message_composer, "click_send_button", _return_false)
    monkeypatch.setattr(sender._delivery_verifier, "build_snapshot", _build_snapshot)
    monkeypatch.setattr(sender._delivery_verifier, "wait_send_network_ok", _wait_network)
    monkeypatch.setattr(sender._delivery_verifier, "wait_dom_send_confirmation", _wait_dom)
    monkeypatch.setattr(sender._delivery_verifier, "verify_message_visible_after_send", _wait_bubble)
    monkeypatch.setattr(sender, "_capture_success", _capture_success)
    monkeypatch.setattr(
        "src.transport.human_instagram_sender._capture_failure_artifacts",
        _capture_failure_artifacts,
    )
    return sender, fake_page


def test_sender_reconciles_unverified_send_via_thread_refresh(monkeypatch) -> None:
    sender, fake_page = _build_sender_for_unverified_confirmation_test(
        monkeypatch,
        refresh_confirmed=True,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola desde campana",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is True
    assert detail == "sent_verified"
    assert payload["verified"] is True
    assert payload["verify_source"] == "thread_refresh_bubble"
    assert payload["thread_refresh_verify"]["mode"] == "thread_refresh_bubble_confirmed"
    assert fake_page.goto_calls == ["https://www.instagram.com/direct/t/thread-123/"]


def test_sender_keeps_unverified_blocked_when_thread_refresh_cannot_confirm(monkeypatch) -> None:
    sender, fake_page = _build_sender_for_unverified_confirmation_test(
        monkeypatch,
        refresh_confirmed=False,
    )

    ok, detail, payload = asyncio.run(
        sender.send_message_like_human(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="Hola desde campana",
            return_detail=True,
            return_payload=True,
        )
    )

    assert ok is False
    assert detail == "send_unverified_blocked"
    assert payload["reason_code"] == "SENT_UNVERIFIED"
    assert payload["thread_refresh_verify"]["mode"] == "thread_refresh_no_match"
    assert fake_page.goto_calls == ["https://www.instagram.com/direct/t/thread-123/"]


def test_sender_sync_cancel_does_not_close_all_sessions_for_persistent_campaign(monkeypatch) -> None:
    sender = HumanInstagramSender(keep_browser_open_per_account=True)
    close_calls: list[float] = []
    run_payload: dict[str, object] = {}

    def _fake_close_all_sessions_sync(*, timeout: float = 5.0) -> None:
        close_calls.append(float(timeout))

    def _fake_run_coroutine_sync(coro, *, timeout, cancel_reason="", on_cancel=None, **kwargs):
        del timeout, cancel_reason, kwargs
        run_payload["on_cancel"] = on_cancel
        try:
            coro.close()
        except Exception:
            pass
        raise Exception("stop")

    monkeypatch.setattr(sender, "close_all_sessions_sync", _fake_close_all_sessions_sync)
    monkeypatch.setattr("src.transport.human_instagram_sender.run_coroutine_sync", _fake_run_coroutine_sync)

    try:
        sender.send_message_like_human_sync(
            account={"username": "cuenta1"},
            target_username="lead1",
            text="hola",
        )
    except Exception as exc:
        assert str(exc) == "stop"

    assert run_payload["on_cancel"] is None
    assert close_calls == []


def test_campaign_failure_reason_keeps_skipped_detail_over_generic_reason_code() -> None:
    parsed = CampaignSendResult(
        ok=False,
        detail="SKIPPED_UI_NOT_FOUND",
        payload={"reason_code": "UI_NOT_FOUND"},
        status=CampaignSendStatus.SKIPPED,
        reason_code="UI_NOT_FOUND",
        verified=False,
    )

    assert _campaign_failure_reason(parsed) == "SKIPPED_UI_NOT_FOUND"


def test_proxy_worker_request_stop_closes_sender_sessions(monkeypatch) -> None:
    close_calls: list[float] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            close_calls.append(float(timeout))

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)

    class _FakeHealthMonitor:
        def proxy_status(self, _proxy_id: str) -> str:
            return "healthy"

    class _FakeScheduler:
        def update_worker_activity(self, *args, **kwargs) -> None:
            return None

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id="proxy-1",
        accounts=[],
        all_proxy_ids=["proxy-1"],
        scheduler=_FakeScheduler(),
        health_monitor=_FakeHealthMonitor(),
        stats={},
        stats_lock=threading.Lock(),
        delay_min=1,
        delay_max=2,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="matias",
        leads_alias="matidiazlife",
        campaign_run_id="run-1",
        headless=True,
        send_flow_timeout_seconds=15.0,
    )

    worker.request_stop("test-stop")

    assert close_calls == [10.0]
