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
    fingerprint = playwright_service.get_account_fingerprint("tester")

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
                "executable_path": playwright_service.resolve_google_chrome_executable(),
            "launch_args": playwright_service.build_launch_args(
                headless=False,
                locale=str(fingerprint["locale"]),
            ),
            "user_agent": str(fingerprint["user_agent"]),
            "locale": str(fingerprint["locale"]),
            "timezone_id": str(fingerprint["timezone_id"]),
            "viewport_kwargs": playwright_service.context_viewport_kwargs(headless=False),
            "permissions": [],
            "launch_proxy": None,
            "force_headless": False,
            "safe_mode": False,
            "browser_mode": playwright_service.PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
            "subsystem": "default",
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


def test_new_context_for_account_keeps_fingerprint_locale_but_uses_explicit_timezone_override(
    monkeypatch,
    tmp_path: Path,
) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)

    ctx = _FakeContext()
    runtime = _FakeRuntime(ctx)
    service = playwright_service.PlaywrightService(headless=False, base_profiles=tmp_path)
    service._runtime = runtime

    fingerprint = {
        "locale": "es-MX",
        "timezone_id": "America/Mexico_City",
        "user_agent": "agent",
        "viewport": {"width": 1365, "height": 911},
        "device_scale_factor": 2,
    }
    monkeypatch.setattr(playwright_service, "get_account_fingerprint", lambda _username: dict(fingerprint))

    asyncio.run(
        service.new_context_for_account(
            profile_dir=profile_dir,
            storage_state=None,
            proxy=None,
            timezone_id="America/Montevideo",
        )
    )

    assert runtime.calls[0]["locale"] == "es-MX"
    assert runtime.calls[0]["timezone_id"] == "America/Montevideo"
    assert runtime.calls[0]["launch_args"] == playwright_service.build_launch_args(
        headless=False,
        locale="es-MX",
    )


def test_compute_visible_window_rects_tiles_two_windows_side_by_side() -> None:
    area = playwright_service._WorkAreaRect(left=0, top=0, width=1440, height=900)

    rects = playwright_service._compute_visible_window_rects(2, work_area=area)

    assert len(rects) == 2
    assert rects[0].top == rects[1].top
    assert rects[0].left < rects[1].left
    assert rects[0].width < area.width
    assert rects[0].height <= area.height


def test_compute_visible_window_rects_keeps_single_window_compact() -> None:
    area = playwright_service._WorkAreaRect(left=0, top=0, width=1440, height=900)

    rects = playwright_service._compute_visible_window_rects(1, work_area=area)

    assert len(rects) == 1
    assert rects[0].width <= 960
    assert rects[0].height <= 820
    assert rects[0].width >= 800
    assert rects[0].height >= 700
    assert rects[0].left > 0
    assert rects[0].top > 0


def test_visible_campaign_layout_manager_returns_initial_rect_for_single_window() -> None:
    manager = playwright_service._VisibleCampaignLayoutManager()

    config = asyncio.run(
        manager.before_context_launch(
            {
                "scope": "campaign:single-window",
                "target_count": 1,
                "layout_policy": "compact",
                "stagger_min_ms": 300,
                "stagger_max_ms": 800,
            }
        )
    )

    assert config is not None
    assert config["layout_policy"] == "compact"
    assert isinstance(config["initial_rect"], playwright_service._WindowRect)
    assert config["initial_rect"].width <= 960
    assert config["initial_rect"].height <= 820


def test_visible_campaign_layout_manager_preserves_explicit_desktop_size() -> None:
    manager = playwright_service._VisibleCampaignLayoutManager()

    config = asyncio.run(
        manager.before_context_launch(
            {
                "scope": "campaign:desktop-window",
                "target_count": 2,
                "layout_policy": "compact",
                "width": 1366,
                "height": 900,
                "stagger_min_ms": 300,
                "stagger_max_ms": 800,
            }
        )
    )

    assert config is not None
    assert config["initial_rect"].width == 1366
    assert config["initial_rect"].height == 900


def test_compute_visible_window_rects_uses_compact_cascade_for_dense_layouts() -> None:
    area = playwright_service._WorkAreaRect(left=0, top=0, width=1440, height=900)

    rects = playwright_service._compute_visible_window_rects(10, work_area=area)

    assert len(rects) == 10
    assert len({rect.width for rect in rects}) == 1
    assert len({rect.height for rect in rects}) == 1
    assert rects[0].left < rects[4].left < rects[8].left
    assert rects[0].top < rects[4].top < rects[8].top


def test_new_context_for_account_uses_visible_campaign_layout_manager(monkeypatch, tmp_path: Path) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)

    ctx = _FakeContext()
    runtime = _FakeRuntime(ctx)
    service = playwright_service.PlaywrightService(headless=False, base_profiles=tmp_path)
    service._runtime = runtime

    observed: dict[str, Any] = {}

    class _FakeLayoutManager:
        async def before_context_launch(self, config):
            observed["before"] = dict(config)
            return {"scope": str(config["scope"]), "target_count": int(config["target_count"])}

        async def attach_context(self, config, *, ctx, page) -> None:
            observed["attach"] = {
                "config": dict(config),
                "ctx": ctx,
                "page": page,
            }

        async def release_context(self, config, *, ctx) -> None:
            observed["release"] = {"config": dict(config), "ctx": ctx}

    monkeypatch.setattr(playwright_service, "_VISIBLE_CAMPAIGN_LAYOUT_MANAGER", _FakeLayoutManager())

    async def _fake_focus_visible_page(page) -> None:
        observed["focus"] = page

    monkeypatch.setattr(playwright_service, "_focus_visible_page", _fake_focus_visible_page)

    returned_ctx = asyncio.run(
        service.new_context_for_account(
            profile_dir=profile_dir,
            storage_state=None,
            proxy=None,
            visible_browser_layout={
                "scope": "campaign:run-123",
                "target_count": 4,
                "stagger_min_ms": 300,
                "stagger_max_ms": 800,
            },
        )
    )

    assert returned_ctx is ctx
    assert observed["before"]["scope"] == "campaign:run-123"
    assert observed["before"]["target_count"] == 4
    assert observed["attach"]["config"] == {
        "scope": "campaign:run-123",
        "target_count": 4,
    }
    assert observed["attach"]["ctx"] is ctx
    assert observed["attach"]["page"] is ctx.pages[0]
    assert observed["focus"] is ctx.pages[0]


def test_new_context_for_account_launches_campaign_visible_windows_compact(monkeypatch, tmp_path: Path) -> None:
    profile_dir = tmp_path / "tester"
    profile_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = playwright_service.get_account_fingerprint("tester")

    ctx = _FakeContext()
    runtime = _FakeRuntime(ctx)
    service = playwright_service.PlaywrightService(headless=False, base_profiles=tmp_path)
    service._runtime = runtime

    class _FakeLayoutManager:
        async def before_context_launch(self, config):
            assert config["layout_policy"] == "compact"
            return {
                "scope": str(config["scope"]),
                "target_count": int(config["target_count"]),
                "layout_policy": "compact",
                "initial_rect": {
                    "left": 40,
                    "top": 60,
                    "width": 640,
                    "height": 480,
                },
            }

        async def attach_context(self, config, *, ctx, page) -> None:
            return None

        async def release_context(self, config, *, ctx) -> None:
            return None

    monkeypatch.setattr(playwright_service, "_VISIBLE_CAMPAIGN_LAYOUT_MANAGER", _FakeLayoutManager())

    asyncio.run(
        service.new_context_for_account(
            profile_dir=profile_dir,
            storage_state=None,
            proxy=None,
            visible_browser_layout={
                "scope": "campaign:run-123",
                "target_count": 4,
                "layout_policy": "compact",
                "stagger_min_ms": 300,
                "stagger_max_ms": 800,
            },
        )
    )

    assert runtime.calls[0]["launch_args"] == playwright_service.build_launch_args(
        headless=False,
        locale=str(fingerprint["locale"]),
        initial_window_rect=playwright_service._WindowRect(left=40, top=60, width=640, height=480),
    )
    assert "--start-maximized" not in runtime.calls[0]["launch_args"]
    assert runtime.calls[0]["viewport_kwargs"] == {"no_viewport": True}


def test_visible_campaign_layout_manager_preserves_explicit_window_size_when_attaching(monkeypatch) -> None:
    manager = playwright_service._VisibleCampaignLayoutManager()
    captured_rects: list[playwright_service._WindowRect] = []

    async def _fake_apply_window_rect(_page, rect) -> None:
        captured_rects.append(rect)

    monkeypatch.setattr(
        playwright_service,
        "_read_primary_work_area",
        lambda: playwright_service._WorkAreaRect(left=0, top=0, width=1600, height=1000),
    )
    monkeypatch.setattr(
        playwright_service._VisibleCampaignLayoutManager,
        "_apply_window_rect",
        staticmethod(_fake_apply_window_rect),
    )

    config = asyncio.run(
        manager.before_context_launch(
            {
                "scope": "campaign:desktop-window",
                "target_count": 4,
                "layout_policy": "compact",
                "width": 1366,
                "height": 900,
                "stagger_min_ms": 300,
                "stagger_max_ms": 800,
            }
        )
    )

    assert config is not None

    asyncio.run(manager.attach_context(config, ctx=object(), page=object()))

    assert captured_rects
    assert captured_rects[-1].width == 1366
    assert captured_rects[-1].height == 900


def test_context_viewport_kwargs_uses_campaign_desktop_override() -> None:
    viewport = playwright_service.context_viewport_kwargs(
        headless=True,
        viewport_override={"width": 1366, "height": 900},
        fingerprint={"viewport": {"width": 1920, "height": 1080}, "device_scale_factor": 2},
    )

    assert viewport == {
        "viewport": {"width": 1366, "height": 900},
        "device_scale_factor": 1,
    }


def test_apply_window_rect_logs_when_cdp_is_unavailable(caplog: pytest.LogCaptureFixture) -> None:
    class _NoCdpPage:
        context = object()

    with caplog.at_level("WARNING"):
        asyncio.run(
            playwright_service._VisibleCampaignLayoutManager._apply_window_rect(
                _NoCdpPage(),
                playwright_service._WindowRect(left=10, top=20, width=640, height=480),
            )
        )

    assert "CDP is unavailable" in caplog.text
