from __future__ import annotations

import asyncio
from pathlib import Path

from core import accounts
import src.auth.onboarding as onboarding


class _FakeSvc:
    def __init__(self) -> None:
        self.saved: list[tuple[object, str]] = []

    async def save_storage_state(self, ctx: object, profile_path: str) -> None:
        self.saved.append((ctx, profile_path))


def test_build_playwright_login_payload_does_not_force_relogin(monkeypatch) -> None:
    monkeypatch.setattr(
        accounts,
        "_playwright_account_payload",
        lambda username, password, proxy_settings: {
            "username": username,
            "password": password,
            "proxy_settings": proxy_settings,
        },
    )

    payload = accounts._build_playwright_login_payload(
        "tester",
        "secret",
        {"username": "tester"},
        alias="ventas",
        totp_secret="totp-secret",
        row_number=7,
    )

    assert payload["alias"] == "ventas"
    assert payload["totp_secret"] == "totp-secret"
    assert payload["row_number"] == 7
    assert payload["disable_safe_browser_recovery"] is True
    assert "strict_login" not in payload
    assert "force_login" not in payload


def test_login_account_playwright_async_defaults_to_session_reuse(
    monkeypatch,
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}
    steps: list[str] = []
    fake_svc = _FakeSvc()
    fake_ctx = object()
    fake_page = object()

    async def _fake_ensure_logged_in_async(account, *, headless, profile_root, proxy):
        observed["account"] = dict(account)
        observed["headless"] = headless
        observed["profile_root"] = profile_root
        observed["proxy"] = proxy
        return fake_svc, fake_ctx, fake_page

    async def _fake_confirm_inbox_logged_in(page, *, trace=None):
        steps.append("inbox")
        observed["trace_is_callable"] = callable(trace)
        assert page is fake_page
        return True, "instagram_ui_ready"

    async def _fake_confirm_feed_logged_in(page, *, trace=None):
        steps.append("feed")
        assert callable(trace)
        assert page is fake_page
        return True, "feed_ready"

    async def _fake_shutdown(svc, ctx) -> None:
        observed["shutdown"] = (svc, ctx)

    monkeypatch.setattr(onboarding, "ensure_logged_in_async", _fake_ensure_logged_in_async)
    monkeypatch.setattr(onboarding, "confirm_feed_logged_in", _fake_confirm_feed_logged_in)
    monkeypatch.setattr(onboarding, "confirm_inbox_logged_in", _fake_confirm_inbox_logged_in)
    monkeypatch.setattr(onboarding, "shutdown", _fake_shutdown)
    monkeypatch.setattr(onboarding, "_profile_path_for", lambda username, root: tmp_path / username)

    result = asyncio.run(
        onboarding.login_account_playwright_async(
            {"username": "tester", "password": "secret"},
            "ventas",
            headful=True,
        )
    )

    assert result == {
        "username": "tester",
        "status": "ok",
        "message": "feed_ready -> instagram_ui_ready",
        "profile_path": str(tmp_path / "tester"),
        "row_number": None,
    }
    assert steps == ["feed", "inbox"]
    assert observed["headless"] is False
    assert observed["trace_is_callable"] is True
    assert observed["shutdown"] == (fake_svc, fake_ctx)
    assert observed["account"] == {
        "username": "tester",
        "password": "secret",
        "alias": "ventas",
        "trace": observed["account"]["trace"],
        "strict_login": False,
        "force_login": False,
        "disable_safe_browser_recovery": True,
    }
    assert callable(observed["account"]["trace"])
    assert fake_svc.saved == [(fake_ctx, str(tmp_path / "tester"))]


def test_login_account_playwright_async_preserves_explicit_relogin_flags(
    monkeypatch,
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}
    steps: list[str] = []
    fake_svc = _FakeSvc()
    fake_ctx = object()
    fake_page = object()

    async def _fake_ensure_logged_in_async(account, *, headless, profile_root, proxy):
        observed["account"] = dict(account)
        observed["headless"] = headless
        observed["profile_root"] = profile_root
        observed["proxy"] = proxy
        return fake_svc, fake_ctx, fake_page

    async def _fake_confirm_inbox_logged_in(page, *, trace=None):
        steps.append("inbox")
        assert callable(trace)
        assert page is fake_page
        return True, "instagram_ui_ready"

    async def _fake_confirm_feed_logged_in(page, *, trace=None):
        steps.append("feed")
        assert callable(trace)
        assert page is fake_page
        return True, "feed_ready"

    async def _fake_shutdown(_svc, _ctx) -> None:
        return None

    monkeypatch.setattr(onboarding, "ensure_logged_in_async", _fake_ensure_logged_in_async)
    monkeypatch.setattr(onboarding, "confirm_feed_logged_in", _fake_confirm_feed_logged_in)
    monkeypatch.setattr(onboarding, "confirm_inbox_logged_in", _fake_confirm_inbox_logged_in)
    monkeypatch.setattr(onboarding, "shutdown", _fake_shutdown)
    monkeypatch.setattr(onboarding, "_profile_path_for", lambda username, root: tmp_path / username)

    result = asyncio.run(
        onboarding.login_account_playwright_async(
            {
                "username": "tester",
                "password": "secret",
                "strict_login": True,
                "force_login": True,
                "disable_safe_browser_recovery": False,
            },
            "ventas",
            headful=False,
        )
    )

    assert result["status"] == "ok"
    assert result["message"] == "feed_ready -> instagram_ui_ready"
    assert steps == ["feed", "inbox"]
    assert observed["headless"] is True
    assert observed["account"]["strict_login"] is True
    assert observed["account"]["force_login"] is True
    assert observed["account"]["disable_safe_browser_recovery"] is False
