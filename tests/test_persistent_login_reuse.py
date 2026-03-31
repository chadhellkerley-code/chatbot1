from __future__ import annotations

import asyncio
import importlib
import json
import sys
from pathlib import Path

import pytest

from src.runtime.playwright_runtime import (
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
    PLAYWRIGHT_BROWSER_MODE_MANAGED,
)
from src.campaign_timezone_policy import CampaignBrowserTimezoneResolution


class _FakePage:
    def __init__(self) -> None:
        self.url = "about:blank"
        self.default_timeout = None
        self.default_navigation_timeout = None

    def set_default_timeout(self, value: int) -> None:
        self.default_timeout = value

    def set_default_navigation_timeout(self, value: int) -> None:
        self.default_navigation_timeout = value


class _FakeContext:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeService:
    instances: list["_FakeService"] = []

    def __init__(
        self,
        *,
        headless: bool,
        base_profiles: Path,
        prefer_persistent: bool,
        browser_mode: str,
        subsystem: str = "auth",
    ) -> None:
        del subsystem
        self.headless = headless
        self.base_profiles = Path(base_profiles)
        self.prefer_persistent = prefer_persistent
        self.browser_mode = browser_mode
        self.calls: list[dict[str, object]] = []
        self.closed = False
        _FakeService.instances.append(self)

    async def new_context_for_account(
        self,
        profile_dir,
        storage_state=None,
        proxy=None,
        *,
        timezone_id=None,
        campaign_desktop_layout=None,
        visible_browser_layout=None,
        safe_mode: bool = False,
        **_kwargs,
    ):
        payload = {
            "profile_dir": Path(profile_dir),
            "storage_state": storage_state,
            "proxy": proxy,
            "safe_mode": safe_mode,
        }
        if timezone_id is not None:
            payload["timezone_id"] = timezone_id
        if campaign_desktop_layout is not None:
            payload["campaign_desktop_layout"] = campaign_desktop_layout
        if visible_browser_layout is not None:
            payload["visible_browser_layout"] = visible_browser_layout
        self.calls.append(payload)
        return _FakeContext()

    async def close(self) -> None:
        self.closed = True

    async def record_diagnostic_failure(self, **_kwargs) -> None:
        return None

    async def save_storage_state(self, *_args, **_kwargs) -> None:
        return None


def _profile_root(tmp_path: Path) -> Path:
    return tmp_path / "runtime" / "browser_profiles"


def _seed_profile(profile_root: Path, username: str) -> Path:
    profile_dir = profile_root / username
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "Local State").write_text("{}", encoding="utf-8")
    default_dir = profile_dir / "Default"
    default_dir.mkdir(parents=True, exist_ok=True)
    (default_dir / "Preferences").write_text("{}", encoding="utf-8")
    return profile_dir


def _reload_proxy_pool(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    sys.modules.pop("src.proxy_pool", None)
    import src.proxy_pool as proxy_pool  # type: ignore

    return importlib.reload(proxy_pool)


def _install_fake_browser(monkeypatch, persistent_login, calls: dict[str, int]) -> None:
    _FakeService.instances.clear()
    monkeypatch.setattr(persistent_login, "PlaywrightService", _FakeService)

    async def _fake_get_page(_ctx):
        return _FakePage()

    async def _fail_load_home(*_args, **_kwargs):
        calls["load_home"] += 1
        raise AssertionError("_load_home should not run for reuse-only action sessions")

    async def _fail_check_logged_in(*_args, **_kwargs):
        calls["check_logged_in"] += 1
        raise AssertionError("check_logged_in should not run for reuse-only action sessions")

    async def _fail_human_login(*_args, **_kwargs):
        calls["human_login"] += 1
        raise AssertionError("human_login should not run for reuse-only action sessions")

    monkeypatch.setattr(persistent_login, "get_page", _fake_get_page)
    monkeypatch.setattr(persistent_login, "_load_home", _fail_load_home)
    monkeypatch.setattr(persistent_login, "check_logged_in", _fail_check_logged_in)
    monkeypatch.setattr(persistent_login, "human_login", _fail_human_login)


def test_reuse_session_only_opens_existing_profile_without_validation(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    profile_root = _profile_root(tmp_path)
    profile_dir = _seed_profile(profile_root, "tester")

    svc, ctx, page = asyncio.run(
        persistent_login.ensure_logged_in_async(
            {"username": "tester", "reuse_session_only": True},
            headless=True,
            profile_root=profile_root,
        )
    )

    assert svc is _FakeService.instances[0]
    assert isinstance(ctx, _FakeContext)
    assert isinstance(page, _FakePage)
    assert _FakeService.instances[0].calls == [
        {
            "profile_dir": profile_dir,
            "storage_state": None,
            "proxy": None,
            "safe_mode": False,
        }
    ]
    assert _FakeService.instances[0].browser_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


def test_reuse_session_only_applies_assigned_proxy_at_launch(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    _reload_proxy_pool(monkeypatch, tmp_path)
    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    proxies_path = tmp_path / "storage" / "accounts" / "proxies.json"
    proxies_path.parent.mkdir(parents=True, exist_ok=True)
    proxies_path.write_text(
        json.dumps(
            {
                "proxies": [
                    {
                        "id": "proxy-1",
                        "server": "http://127.0.0.1:9000",
                        "user": "alice",
                        "pass": "secret",
                        "active": True,
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    profile_root = _profile_root(tmp_path)
    _seed_profile(profile_root, "tester")

    asyncio.run(
        persistent_login.ensure_logged_in_async(
            {
                "username": "tester",
                "assigned_proxy_id": "proxy-1",
                "reuse_session_only": True,
            },
            headless=True,
            profile_root=profile_root,
        )
    )

    assert _FakeService.instances[0].calls[0]["proxy"] == {
        "server": "http://127.0.0.1:9000",
        "username": "alice",
        "password": "secret",
    }
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


def test_campaign_visible_reuse_session_uses_managed_browser_mode(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    profile_root = _profile_root(tmp_path)
    _seed_profile(profile_root, "tester")

    asyncio.run(
        persistent_login.ensure_logged_in_async(
            {
                "username": "tester",
                "reuse_session_only": True,
                "manual_visible_browser": True,
                "playwright_browser_mode": PLAYWRIGHT_BROWSER_MODE_MANAGED,
            },
            headless=False,
            profile_root=profile_root,
        )
    )

    assert _FakeService.instances[0].browser_mode == PLAYWRIGHT_BROWSER_MODE_MANAGED
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


def test_campaign_reuse_session_logs_and_applies_resolved_timezone(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    logged: list[dict[str, object]] = []
    monkeypatch.setattr(
        persistent_login,
        "resolve_campaign_browser_timezone",
        lambda _account: CampaignBrowserTimezoneResolution(
            timezone_id="America/Montevideo",
            browser_timezone_source="system",
            business_timezone_id="America/Argentina/Cordoba",
            has_proxy=False,
        ),
    )
    monkeypatch.setattr(persistent_login, "log_browser_stage", lambda **kwargs: logged.append(dict(kwargs)))

    profile_root = _profile_root(tmp_path)
    profile_dir = _seed_profile(profile_root, "tester")

    asyncio.run(
        persistent_login.ensure_logged_in_async(
            {
                "username": "tester",
                "reuse_session_only": True,
                "_playwright_subsystem": "campaign",
            },
            headless=True,
            profile_root=profile_root,
        )
    )

    assert _FakeService.instances[0].calls == [
        {
            "profile_dir": profile_dir,
            "storage_state": None,
            "proxy": None,
            "safe_mode": False,
            "timezone_id": "America/Montevideo",
        }
    ]
    assert logged == [
        {
            "component": "campaign_timezone_policy",
            "stage": "timezone_resolved",
            "status": "ok",
            "account": "tester",
            "has_proxy": False,
            "proxy_id": "",
            "proxy_label": "",
            "browser_timezone_source": "system",
            "browser_timezone_id": "America/Montevideo",
            "business_timezone_id": "America/Argentina/Cordoba",
        }
    ]
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


def test_reuse_session_only_requires_existing_profile(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    profile_root = _profile_root(tmp_path)
    missing_profile = profile_root / "tester"

    try:
        asyncio.run(
            persistent_login.ensure_logged_in_async(
                {"username": "tester", "reuse_session_only": True},
                headless=True,
                profile_root=profile_root,
            )
        )
    except RuntimeError as exc:
        assert str(exc) == "persistent_profile_missing:tester"
    else:
        raise AssertionError("reuse-only flow should fail when the persistent profile is missing")

    assert not missing_profile.exists()
    assert _FakeService.instances[0].calls == []
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


def test_reuse_session_only_rejects_profile_without_chrome_state(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    calls = {"load_home": 0, "check_logged_in": 0, "human_login": 0}
    _install_fake_browser(monkeypatch, persistent_login, calls)

    profile_root = _profile_root(tmp_path)
    profile_dir = profile_root / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "storage_state.json").write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="persistent_profile_invalid:tester"):
        asyncio.run(
            persistent_login.ensure_logged_in_async(
                {"username": "tester", "reuse_session_only": True},
                headless=True,
                profile_root=profile_root,
            )
        )

    assert _FakeService.instances[0].calls == []
    assert calls == {"load_home": 0, "check_logged_in": 0, "human_login": 0}


class _DriverCrashService(_FakeService):
    async def new_context_for_account(
        self,
        profile_dir,
        storage_state=None,
        proxy=None,
        *,
        timezone_id=None,
        safe_mode: bool = False,
        **_kwargs,
    ):
        payload = {
            "profile_dir": Path(profile_dir),
            "storage_state": storage_state,
            "proxy": proxy,
            "safe_mode": safe_mode,
        }
        if timezone_id is not None:
            payload["timezone_id"] = timezone_id
        self.calls.append(payload)
        raise RuntimeError("target page, context or browser has been closed")


class _TrackingService(_FakeService):
    created_contexts: list[_FakeContext] = []

    async def new_context_for_account(
        self,
        profile_dir,
        storage_state=None,
        proxy=None,
        *,
        timezone_id=None,
        safe_mode: bool = False,
        **_kwargs,
    ):
        payload = {
            "profile_dir": Path(profile_dir),
            "storage_state": storage_state,
            "proxy": proxy,
            "safe_mode": safe_mode,
        }
        if timezone_id is not None:
            payload["timezone_id"] = timezone_id
        self.calls.append(payload)
        ctx = _FakeContext()
        self.created_contexts.append(ctx)
        return ctx


def test_reuse_session_rejects_chrome_profile_picker(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    _TrackingService.instances.clear()
    _TrackingService.created_contexts.clear()
    monkeypatch.setattr(persistent_login, "PlaywrightService", _TrackingService)

    async def _fake_get_page(_ctx):
        page = _FakePage()
        page.url = "chrome://profile-picker/"
        return page

    monkeypatch.setattr(persistent_login, "get_page", _fake_get_page)

    profile_root = _profile_root(tmp_path)
    profile_dir = _seed_profile(profile_root, "tester")

    with pytest.raises(RuntimeError, match="chrome_profile_picker:tester"):
        asyncio.run(
            persistent_login.ensure_logged_in_async(
                {"username": "tester", "reuse_session_only": True},
                headless=True,
                profile_root=profile_root,
            )
        )

    assert _TrackingService.instances[0].calls == [
        {
            "profile_dir": profile_dir,
            "storage_state": None,
            "proxy": None,
            "safe_mode": False,
        }
    ]
    assert _TrackingService.created_contexts[0].closed is True


def test_validate_reused_session_rejects_invalid_profile_without_login(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    _TrackingService.instances.clear()
    _TrackingService.created_contexts.clear()
    monkeypatch.setattr(persistent_login, "PlaywrightService", _TrackingService)

    fake_page = _FakePage()

    async def _fake_get_page(_ctx):
        return fake_page

    async def _fake_load_home(page, *, timeout_ms=None):
        del timeout_ms
        page.url = "https://www.instagram.com/accounts/login/"

    async def _fake_check_logged_in(_page):
        return False, "url_login_or_challenge"

    async def _fail_human_login(*_args, **_kwargs):
        raise AssertionError("human_login should not run when reused session validation fails")

    monkeypatch.setattr(persistent_login, "get_page", _fake_get_page)
    monkeypatch.setattr(persistent_login, "_load_home", _fake_load_home)
    monkeypatch.setattr(persistent_login, "check_logged_in", _fake_check_logged_in)
    monkeypatch.setattr(persistent_login, "human_login", _fail_human_login)

    profile_root = _profile_root(tmp_path)
    profile_dir = _seed_profile(profile_root, "tester")

    with pytest.raises(RuntimeError, match="session_invalid:tester:url_login_or_challenge"):
        asyncio.run(
            persistent_login.ensure_logged_in_async(
                {
                    "username": "tester",
                    "reuse_session_only": True,
                    "validate_reused_session": True,
                },
                headless=True,
                profile_root=profile_root,
            )
        )

    assert _TrackingService.instances[0].calls == [
        {
            "profile_dir": profile_dir,
            "storage_state": None,
            "proxy": None,
            "safe_mode": False,
        }
    ]
    assert _TrackingService.created_contexts[0].closed is True
    assert _TrackingService.instances[0].closed is True


def test_manual_visible_browser_does_not_retry_headless_safe_mode(monkeypatch, tmp_path: Path) -> None:
    import src.auth.persistent_login as persistent_login

    _FakeService.instances.clear()
    monkeypatch.setattr(persistent_login, "PlaywrightService", _DriverCrashService)

    profile_root = _profile_root(tmp_path)
    profile_dir = _seed_profile(profile_root, "tester")

    with pytest.raises(RuntimeError, match="target page, context or browser has been closed"):
        asyncio.run(
            persistent_login.ensure_logged_in_async(
                {
                    "username": "tester",
                    "reuse_session_only": True,
                    "manual_visible_browser": True,
                },
                headless=False,
                profile_root=profile_root,
            )
        )

    assert len(_FakeService.instances) == 1
    assert _FakeService.instances[0].headless is False
    assert _FakeService.instances[0].browser_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
    assert _FakeService.instances[0].calls == [
        {
            "profile_dir": profile_dir,
            "storage_state": None,
            "proxy": None,
            "safe_mode": False,
        }
    ]
