from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from runtime.runtime import reset_stop_event
from src.dm_campaign.contracts import CampaignSendResult, CampaignSendStatus
from src.dm_campaign.proxy_workers_runner import (
    ProxyWorker,
    TemplateRotator,
    _campaign_failure_reason,
    _parse_send_result,
)
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

    monkeypatch.setattr(sender._session_manager, "open_session", _open_session)
    monkeypatch.setattr(sender._session_manager, "save_storage_state", _noop)
    monkeypatch.setattr(sender._session_manager, "discard_if_unhealthy", _noop)
    monkeypatch.setattr(sender._session_manager, "finalize_session", _noop)
    monkeypatch.setattr(sender._inbox_navigator, "ensure_inbox_surface", _ensure_inbox_surface)
    monkeypatch.setattr(sender._thread_resolver, "open_thread_from_sidebar", _open_thread_from_sidebar)
    monkeypatch.setattr(sender._message_composer, "wait_composer_visible", _return_fake_composer)
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
    assert stage_events == ["opening_session", "opening_dm", "sending", "sending"]
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


def test_sender_uses_visible_composer_without_waiting_for_chat_load(monkeypatch) -> None:
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

    async def _thread_composer(_page):
        return fake_composer

    async def _wait_composer_visible(*_args, **_kwargs):
        raise AssertionError("chat load wait should be skipped when the composer is already visible")

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
    monkeypatch.setattr(sender._message_composer, "thread_composer", _thread_composer)
    monkeypatch.setattr(sender._message_composer, "wait_composer_visible", _wait_composer_visible)
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

    assert close_calls == [2.0]
