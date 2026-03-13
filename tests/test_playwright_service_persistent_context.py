from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

import src.playwright_service as playwright_service


class _FakePage:
    def __init__(self) -> None:
        self.default_timeout: int | None = None
        self.default_navigation_timeout: int | None = None

    def set_default_timeout(self, value: int) -> None:
        self.default_timeout = value

    def set_default_navigation_timeout(self, value: int) -> None:
        self.default_navigation_timeout = value


class _FakeContext:
    def __init__(self) -> None:
        self.pages: list[_FakePage] = []
        self.default_timeout: int | None = None
        self.routes: list[str] = []
        self.closed = False

    def set_default_timeout(self, value: int) -> None:
        self.default_timeout = value

    async def route(self, pattern: str, _handler: Any) -> None:
        self.routes.append(pattern)

    async def new_page(self) -> _FakePage:
        page = _FakePage()
        self.pages.append(page)
        return page

    async def close(self) -> None:
        self.closed = True


class _FakeRuntime:
    def __init__(self, ctx: _FakeContext) -> None:
        self.ctx = ctx
        self.calls: list[dict[str, Any]] = []
        self.playwright = object()
        self.browser = None

    async def get_context(self, **kwargs: Any) -> _FakeContext:
        self.calls.append(dict(kwargs))
        return self.ctx

    async def stop(self) -> None:
        return None


def test_build_launch_args_does_not_force_locale_when_empty(monkeypatch) -> None:
    monkeypatch.setattr(playwright_service, "BASE_FLAGS", ["--disable-gpu", "--lang=es-ES"])
    monkeypatch.setattr(playwright_service, "HEADFUL_ADAPTIVE_VIEWPORT", False)

    args = playwright_service.build_launch_args(headless=False, locale="")

    assert args == ["--disable-gpu"]


def _write_cookie_db(profile_dir: Path, cookie_names: list[str]) -> Path:
    cookies_db = profile_dir / "Default" / "Network" / "Cookies"
    cookies_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cookies_db)
    try:
        conn.execute("CREATE TABLE cookies (host_key TEXT, name TEXT)")
        conn.executemany(
            "INSERT INTO cookies(host_key, name) VALUES (?, ?)",
            [(".instagram.com", name) for name in cookie_names],
        )
        conn.commit()
    finally:
        conn.close()
    return cookies_db


@pytest.mark.parametrize(
    "proxy_payload",
    [
        None,
        {"server": "http://127.0.0.1:9000", "username": "alice", "password": "secret"},
    ],
)
def test_new_context_for_account_applies_storage_state_in_persistent_mode(
    monkeypatch,
    tmp_path: Path,
    proxy_payload: dict[str, str] | None,
) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state = profile_dir / "storage_state.json"
    storage_state.write_text('{"cookies":[],"origins":[]}', encoding="utf-8")

    ctx = _FakeContext()
    runtime = _FakeRuntime(ctx)
    service = playwright_service.PlaywrightService(headless=False, base_profiles=tmp_path)
    service._runtime = runtime

    applied_storage: list[str] = []

    async def _fake_apply_storage_state_compat(target_ctx: Any, target_storage: str | Path | None) -> None:
        assert target_ctx is ctx
        applied_storage.append(str(target_storage))

    monkeypatch.setattr(playwright_service, "_apply_storage_state_compat", _fake_apply_storage_state_compat)

    returned_ctx = asyncio.run(
        service.new_context_for_account(
            profile_dir=profile_dir,
            storage_state=storage_state,
            proxy=proxy_payload,
        )
    )

    assert returned_ctx is ctx
    assert runtime.calls == [
        {
            "account": "tester",
            "profile_dir": profile_dir,
            "storage_state": str(storage_state),
            "proxy": proxy_payload,
            "mode": "persistent",
            "executable_path": playwright_service.resolve_playwright_executable(headless=False),
            "launch_args": playwright_service.build_launch_args(
                headless=False,
                locale=playwright_service.DEFAULT_LOCALE,
            ),
            "user_agent": playwright_service.DEFAULT_USER_AGENT,
            "locale": playwright_service.DEFAULT_LOCALE,
            "timezone_id": playwright_service.DEFAULT_TIMEZONE,
            "viewport_kwargs": playwright_service.context_viewport_kwargs(headless=False),
            "permissions": [],
            "launch_proxy": None,
            "force_headless": False,
            "safe_mode": False,
            "browser_mode": playwright_service.PLAYWRIGHT_BROWSER_MODE_DEFAULT,
        }
    ]
    assert applied_storage == [str(storage_state)]
    assert ctx.default_timeout == 30_000
    assert len(ctx.routes) == len(playwright_service._LOGIN_SYNC_BLOCK_PATTERNS)
    assert len(ctx.pages) == 1
    assert ctx.pages[0].default_timeout == 30_000
    assert ctx.pages[0].default_navigation_timeout == 30_000


def test_migrate_legacy_profile_dir_repairs_profile_missing_session_cookie(monkeypatch, tmp_path: Path) -> None:
    legacy_root = tmp_path / "legacy"
    current_root = tmp_path / "current"
    legacy_profile = legacy_root / "tester"
    current_profile = current_root / "tester"
    legacy_profile.mkdir(parents=True, exist_ok=True)
    current_profile.mkdir(parents=True, exist_ok=True)

    _write_cookie_db(legacy_profile, ["sessionid", "csrftoken"])
    _write_cookie_db(current_profile, ["csrftoken"])

    monkeypatch.setattr(playwright_service, "_LEGACY_BASE_PROFILES", legacy_root)
    monkeypatch.setattr(playwright_service, "BASE_PROFILES", current_root)

    assert playwright_service._profile_has_instagram_session_cookie(current_profile) is False
    assert playwright_service._profile_has_instagram_session_cookie(legacy_profile) is True

    playwright_service._migrate_legacy_profile_dir(current_profile)

    assert playwright_service._profile_has_instagram_session_cookie(current_profile) is True


def test_new_context_for_account_migrates_legacy_profile_before_launch(monkeypatch, tmp_path: Path) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)

    ctx = _FakeContext()
    runtime = _FakeRuntime(ctx)
    service = playwright_service.PlaywrightService(headless=False, base_profiles=tmp_path)
    service._runtime = runtime

    migrated: list[Path] = []

    def _fake_migrate(target: Path) -> None:
        migrated.append(Path(target))

    monkeypatch.setattr(playwright_service, "_migrate_legacy_profile_dir", _fake_migrate)

    asyncio.run(service.new_context_for_account(profile_dir=profile_dir, storage_state=None, proxy=None))

    assert migrated == [profile_dir]
