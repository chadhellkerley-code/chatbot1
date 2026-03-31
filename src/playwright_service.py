from __future__ import annotations

import asyncio
import contextlib
import ctypes
<<<<<<< HEAD
import hashlib
=======
>>>>>>> origin/main
import json
import logging
import math
import os
import sqlite3
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from paths import browser_profiles_root, runtime_base
from typing import Any, Mapping, Optional, Tuple, Union

from playwright.async_api import Browser, BrowserContext, Page, Playwright
<<<<<<< HEAD
from src.browser_profile_lifecycle import emit_profile_lifecycle_diagnostic
from src.browser_profile_paths import browser_profile_dir
=======
>>>>>>> origin/main
from src.runtime.playwright_resolver import (
    ensure_local_playwright_browsers_env,
    resolve_google_chrome_executable,
    resolve_playwright_chromium_executable,
)
from src.runtime.playwright_runtime import (
    PLAYWRIGHT_BASE_FLAGS,
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
    PLAYWRIGHT_BROWSER_MODE_DEFAULT,
    PLAYWRIGHT_BROWSER_MODE_MANAGED,
    PlaywrightRuntime,
    is_driver_crash_error,
<<<<<<< HEAD
    normalize_browser_mode,
=======
>>>>>>> origin/main
)
from runtime.runtime_parity import resolve_profiles_dir

_BASE_ROOT = runtime_base(Path(__file__).resolve().parent.parent)
ensure_local_playwright_browsers_env()
_LOGGER = logging.getLogger(__name__)


def _resolved_profiles_root() -> Path:
    return resolve_profiles_dir(_BASE_ROOT)


class _ProfilesRootProxy(os.PathLike[str]):
    def __fspath__(self) -> str:
        return str(self.resolve())

    def __str__(self) -> str:
        return str(self.resolve())

    def __repr__(self) -> str:
        return repr(self.resolve())

    def __truediv__(self, other: object) -> Path:
        return self.resolve() / other

    def __getattr__(self, name: str) -> Any:
        return getattr(self.resolve(), name)

    def resolve(self) -> Path:
        return _resolved_profiles_root()


def _base_profiles_path() -> Path:
    base_profiles = BASE_PROFILES
    if isinstance(base_profiles, _ProfilesRootProxy):
        return base_profiles.resolve()
    return Path(base_profiles)


_LEGACY_BASE_PROFILES = browser_profiles_root(_BASE_ROOT)
BASE_PROFILES = _ProfilesRootProxy()


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on", "si"}


def _migrate_legacy_profile_dir(profile_path: Path) -> None:
    legacy_root = _LEGACY_BASE_PROFILES
    current_root = _base_profiles_path()
    if legacy_root == current_root:
        return
    legacy_path = legacy_root / profile_path.name
    if not legacy_path.exists():
        return
    current_has_session = _profile_has_instagram_session_cookie(profile_path)
    legacy_has_session = _profile_has_instagram_session_cookie(legacy_path)
    if profile_path.exists() and current_has_session:
        return
    if profile_path.exists() and not legacy_has_session:
        return
    try:
        shutil.copytree(legacy_path, profile_path, dirs_exist_ok=True)
    except Exception:
        # Mejor esfuerzo: si falla la copia completa, al menos migramos storage_state.
        try:
            profile_path.mkdir(parents=True, exist_ok=True)
            legacy_storage = legacy_path / "storage_state.json"
            if legacy_storage.exists():
                shutil.copy2(legacy_storage, profile_path / "storage_state.json")
        except Exception:
            return


def _profile_cookies_db_path(profile_path: Path) -> Path:
    return profile_path / "Default" / "Network" / "Cookies"


def _profile_has_instagram_session_cookie(profile_path: Path) -> bool:
    cookies_db = _profile_cookies_db_path(profile_path)
    if not cookies_db.exists():
        return False
    try:
        conn = sqlite3.connect(f"file:{cookies_db}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT 1 FROM cookies WHERE host_key LIKE ? AND name = ? LIMIT 1",
                ("%instagram.com%", "sessionid"),
            ).fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except Exception:
        return default


<<<<<<< HEAD
_WORK_AREA_FALLBACK_VIEWPORT = {
=======
DEFAULT_VIEWPORT = {
>>>>>>> origin/main
    "width": _env_int("PLAYWRIGHT_VIEWPORT_WIDTH", 1920, minimum=800),
    "height": _env_int("PLAYWRIGHT_VIEWPORT_HEIGHT", 1080, minimum=600),
}
HEADFUL_ADAPTIVE_VIEWPORT = _env_flag("PLAYWRIGHT_HEADFUL_ADAPTIVE_VIEWPORT", True)
<<<<<<< HEAD
DEFAULT_VIEWPORT = dict(_WORK_AREA_FALLBACK_VIEWPORT)
=======
>>>>>>> origin/main
DEFAULT_USER_AGENT = (os.getenv("HUMAN_USER_AGENT") or "").strip()
DEFAULT_LOCALE = (os.getenv("HUMAN_LOCALE") or "").strip()
DEFAULT_TIMEZONE = (os.getenv("HUMAN_TZ") or "").strip()
BASE_FLAGS = list(PLAYWRIGHT_BASE_FLAGS)
_LOGIN_SYNC_BLOCK_PATTERNS = (
    "**://www.facebook.com/instagram/login_sync/**",
    "**://www.facebook.com/instagram/login_sync/*",
    "**://m.facebook.com/instagram/login_sync/**",
    "**://m.facebook.com/instagram/login_sync/*",
    "**://*.facebook.com/instagram/login_sync/**",
    "**://*.facebook.com/instagram/login_sync/*",
)

<<<<<<< HEAD
_ACCOUNT_FINGERPRINT_LOCALES = (
    "es-AR",
    "es-MX",
    "es-ES",
    "en-US",
)
_ACCOUNT_FINGERPRINT_TIMEZONES = {
    "es-AR": "America/Argentina/Buenos_Aires",
    "es-MX": "America/Mexico_City",
    "es-ES": "Europe/Madrid",
    "en-US": "America/New_York",
}
_ACCOUNT_FINGERPRINT_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.7049.84 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7_4) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.7049.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Arm Mac OS X 14_4_0) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.7103.49 Safari/537.36",
)
_ACCOUNT_VIEWPORT_WIDTH_RANGE = (1280, 1920)
_ACCOUNT_VIEWPORT_HEIGHT_RANGE = (768, 1080)


def _timezone_for_locale(locale: str, *, fallback: str = "America/New_York") -> str:
    normalized_locale = str(locale or "").strip()
    return _ACCOUNT_FINGERPRINT_TIMEZONES.get(normalized_locale, fallback)


def _hash_range_value(seed: bytes, start: int, length: int, *, minimum: int, maximum: int) -> int:
    span = max(1, int(maximum) - int(minimum) + 1)
    value = int.from_bytes(seed[start : start + length], "big")
    return int(minimum) + (value % span)


def get_account_fingerprint(username: str) -> dict[str, Any]:
    normalized_username = str(username or "").strip() or "account"
    digest = hashlib.sha256(normalized_username.encode("utf-8")).digest()
    locale = _ACCOUNT_FINGERPRINT_LOCALES[digest[0] % len(_ACCOUNT_FINGERPRINT_LOCALES)]
    return {
        "viewport": {
            "width": _hash_range_value(
                digest,
                1,
                4,
                minimum=_ACCOUNT_VIEWPORT_WIDTH_RANGE[0],
                maximum=_ACCOUNT_VIEWPORT_WIDTH_RANGE[1],
            ),
            "height": _hash_range_value(
                digest,
                5,
                4,
                minimum=_ACCOUNT_VIEWPORT_HEIGHT_RANGE[0],
                maximum=_ACCOUNT_VIEWPORT_HEIGHT_RANGE[1],
            ),
        },
        "device_scale_factor": 1 if digest[9] % 2 == 0 else 2,
        "locale": locale,
        "timezone_id": _timezone_for_locale(locale),
        "user_agent": _ACCOUNT_FINGERPRINT_USER_AGENTS[digest[10] % len(_ACCOUNT_FINGERPRINT_USER_AGENTS)],
    }

=======
>>>>>>> origin/main

@dataclass(frozen=True)
class _WorkAreaRect:
    left: int
    top: int
    width: int
    height: int


@dataclass(frozen=True)
class _WindowRect:
    left: int
    top: int
    width: int
    height: int


@dataclass
class _VisibleCampaignWindow:
    key: str
    page: Page
    target_count: int
    sequence: int
<<<<<<< HEAD
    window_width: int = 0
    window_height: int = 0
=======
>>>>>>> origin/main


@dataclass
class _VisibleCampaignScopeState:
    launch_sequence: int = 0
    last_launch_deadline: float = 0.0
    windows: dict[str, _VisibleCampaignWindow] = field(default_factory=dict)


def _read_primary_work_area() -> _WorkAreaRect:
    if os.name == "nt":
        class _WinRect(ctypes.Structure):
            _fields_ = [
                ("left", ctypes.c_long),
                ("top", ctypes.c_long),
                ("right", ctypes.c_long),
                ("bottom", ctypes.c_long),
            ]

        rect = _WinRect()
        spi_get_work_area = 0x0030
        if ctypes.windll.user32.SystemParametersInfoW(spi_get_work_area, 0, ctypes.byref(rect), 0):
            width = max(1, int(rect.right - rect.left))
            height = max(1, int(rect.bottom - rect.top))
            if width > 0 and height > 0:
                return _WorkAreaRect(
                    left=int(rect.left),
                    top=int(rect.top),
                    width=width,
                    height=height,
                )
    with contextlib.suppress(Exception):
        import tkinter  # built-in

        root = tkinter.Tk()
        root.withdraw()
        try:
            width = max(1, int(root.winfo_screenwidth()))
            height = max(1, int(root.winfo_screenheight()))
        finally:
            root.destroy()
        return _WorkAreaRect(left=0, top=0, width=width, height=height)
<<<<<<< HEAD
    return _WorkAreaRect(
        left=0,
        top=0,
        width=_WORK_AREA_FALLBACK_VIEWPORT["width"],
        height=_WORK_AREA_FALLBACK_VIEWPORT["height"],
    )
=======
    return _WorkAreaRect(left=0, top=0, width=DEFAULT_VIEWPORT["width"], height=DEFAULT_VIEWPORT["height"])
>>>>>>> origin/main


def _visible_grid(window_count: int) -> tuple[int, int]:
    total = max(1, int(window_count or 1))
    if total == 1:
        return 1, 1
    cols = max(1, math.ceil(math.sqrt(total)))
    rows = max(1, math.ceil(total / cols))
    return rows, cols


def _clamp(value: int, minimum: int, maximum: int) -> int:
    if minimum > maximum:
        minimum, maximum = maximum, minimum
    return max(minimum, min(maximum, int(value)))


def _compute_compact_grid_window_rects(
    total: int,
    *,
    area: _WorkAreaRect,
    outer_margin: int,
    gap: int,
    usable_width: int,
    usable_height: int,
) -> list[_WindowRect]:
    rows, cols = _visible_grid(total)
<<<<<<< HEAD
    cell_width = max(380, (usable_width - (gap * (cols - 1))) // cols)
    cell_height = max(280, (usable_height - (gap * (rows - 1))) // rows)
    max_width = min(
        usable_width,
        _clamp(int(area.width * (0.56 if total == 1 else 0.58)), 620, 960),
    )
    max_height = min(
        usable_height,
        _clamp(int(area.height * (0.80 if total <= 2 else 0.84)), 500, 820),
=======
    cell_width = max(320, (usable_width - (gap * (cols - 1))) // cols)
    cell_height = max(240, (usable_height - (gap * (rows - 1))) // rows)
    max_width = min(
        usable_width,
        _clamp(int(area.width * (0.50 if total == 1 else 0.52)), 540, 860),
    )
    max_height = min(
        usable_height,
        _clamp(int(area.height * (0.76 if total <= 2 else 0.82)), 420, 760),
>>>>>>> origin/main
    )
    tile_width = min(cell_width, max_width)
    tile_height = min(cell_height, max_height)
    cluster_width = (tile_width * cols) + (gap * (cols - 1))
    cluster_height = (tile_height * rows) + (gap * (rows - 1))
    origin_left = area.left + outer_margin + max(0, (usable_width - cluster_width) // 2)
    origin_top = area.top + outer_margin + max(0, (usable_height - cluster_height) // 2)

    rects: list[_WindowRect] = []
    for index in range(total):
        row = index // cols
        col = index % cols
        left = origin_left + (col * (tile_width + gap))
        top = origin_top + (row * (tile_height + gap))
        rects.append(
            _WindowRect(
                left=int(left),
                top=int(top),
                width=int(tile_width),
                height=int(tile_height),
            )
        )
    return rects


def _compute_compact_cascade_window_rects(
    total: int,
    *,
    area: _WorkAreaRect,
    outer_margin: int,
    usable_width: int,
    usable_height: int,
) -> list[_WindowRect]:
<<<<<<< HEAD
    base_width = min(usable_width, _clamp(int(area.width * 0.36), 420, 620))
    base_height = min(usable_height, _clamp(int(area.height * 0.60), 320, 540))
=======
    base_width = min(usable_width, _clamp(int(area.width * 0.32), 360, 520))
    base_height = min(usable_height, _clamp(int(area.height * 0.56), 280, 460))
>>>>>>> origin/main
    offset_x = _clamp(int(base_width * 0.14), 26, 48)
    offset_y = _clamp(int(base_height * 0.12), 20, 40)
    cascade_span_x = max(0, usable_width - base_width)
    cascade_span_y = max(0, usable_height - base_height)
    max_depth = min(
        max(1, (cascade_span_x // max(1, offset_x)) + 1),
        max(1, (cascade_span_y // max(1, offset_y)) + 1),
    )
    visible_depth = max(5, min(total, max_depth, 10))
    cycle_shift_x = max(10, offset_x // 2)
    cycle_shift_y = max(8, offset_y // 2)

    rects: list[_WindowRect] = []
    for index in range(total):
        tier = index % visible_depth
        cycle = index // visible_depth
        left = area.left + outer_margin + min(cascade_span_x, (tier * offset_x) + (cycle * cycle_shift_x))
        top = area.top + outer_margin + min(cascade_span_y, (tier * offset_y) + (cycle * cycle_shift_y))
        rects.append(
            _WindowRect(
                left=int(left),
                top=int(top),
                width=int(base_width),
                height=int(base_height),
            )
        )
    return rects


def _compute_visible_window_rects(window_count: int, work_area: Optional[_WorkAreaRect] = None) -> list[_WindowRect]:
    total = max(1, int(window_count or 1))
    area = work_area or _read_primary_work_area()
    short_side = max(1, min(area.width, area.height))
    outer_margin = max(8, min(18, short_side // 90))
    gap = max(6, min(14, short_side // 130))
    usable_width = max(320, area.width - (outer_margin * 2))
    usable_height = max(240, area.height - (outer_margin * 2))
    if total > 8:
        return _compute_compact_cascade_window_rects(
            total,
            area=area,
            outer_margin=outer_margin,
            usable_width=usable_width,
            usable_height=usable_height,
        )
    return _compute_compact_grid_window_rects(
        total,
        area=area,
        outer_margin=outer_margin,
        gap=gap,
        usable_width=usable_width,
        usable_height=usable_height,
    )


<<<<<<< HEAD
def _normalize_campaign_desktop_layout(config: Mapping[str, Any] | None) -> dict[str, int] | None:
    if not isinstance(config, Mapping):
        return None
    has_width = (
        ("width" in config and config.get("width") not in (None, ""))
        or ("window_width" in config and config.get("window_width") not in (None, ""))
    )
    has_height = (
        ("height" in config and config.get("height") not in (None, ""))
        or ("window_height" in config and config.get("window_height") not in (None, ""))
    )
    if not has_width and not has_height:
        return None
    try:
        width = max(1280, int(config.get("width") or config.get("window_width") or 1366))
        height = max(800, int(config.get("height") or config.get("window_height") or 900))
    except Exception:
        return None
    if width <= 0 or height <= 0:
        return None
    return {"width": width, "height": height}


def _fixed_window_rect(
    rect: _WindowRect,
    *,
    width: int,
    height: int,
    work_area: Optional[_WorkAreaRect] = None,
) -> _WindowRect:
    area = work_area or _read_primary_work_area()
    max_left = area.left + max(0, area.width - int(width))
    max_top = area.top + max(0, area.height - int(height))
    return _WindowRect(
        left=_clamp(int(rect.left), area.left, max_left),
        top=_clamp(int(rect.top), area.top, max_top),
        width=int(width),
        height=int(height),
    )


=======
>>>>>>> origin/main
class _VisibleCampaignLayoutManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._scopes: dict[str, _VisibleCampaignScopeState] = {}

    @staticmethod
    def _normalize(config: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(config, Mapping):
            return None
        scope = str(config.get("scope") or "").strip()
        if not scope:
            return None
<<<<<<< HEAD
        desktop_layout = _normalize_campaign_desktop_layout(config)
=======
>>>>>>> origin/main
        target_count = max(1, int(config.get("target_count") or 1))
        stagger_min_ms = max(300, int(config.get("stagger_min_ms") or 300))
        stagger_max_ms = max(stagger_min_ms, int(config.get("stagger_max_ms") or 800))
        stagger_step_ms = max(1, int(config.get("stagger_step_ms") or 100))
        return {
            "scope": scope,
            "target_count": target_count,
            "layout_policy": "compact",
            "stagger_min_ms": stagger_min_ms,
            "stagger_max_ms": stagger_max_ms,
            "stagger_step_ms": stagger_step_ms,
<<<<<<< HEAD
            "window_width": int((desktop_layout or {}).get("width") or 0),
            "window_height": int((desktop_layout or {}).get("height") or 0),
=======
>>>>>>> origin/main
        }

    async def before_context_launch(self, config: Mapping[str, Any] | None) -> dict[str, Any] | None:
        normalized = self._normalize(config)
        if normalized is None:
            return None
        wait_ms = 0
        launch_index = 0
        scope = normalized["scope"]
        with self._lock:
            state = self._scopes.setdefault(scope, _VisibleCampaignScopeState())
            launch_index = state.launch_sequence
            state.launch_sequence += 1
            if normalized["target_count"] > 1 and launch_index > 0:
                cycle_span = max(1, ((normalized["stagger_max_ms"] - normalized["stagger_min_ms"]) // normalized["stagger_step_ms"]) + 1)
                interval_ms = min(
                    normalized["stagger_max_ms"],
                    normalized["stagger_min_ms"] + (((launch_index - 1) % cycle_span) * normalized["stagger_step_ms"]),
                )
                now = time.monotonic()
                scheduled_at = max(now, state.last_launch_deadline) + (interval_ms / 1000.0)
                wait_ms = max(0, int(round((scheduled_at - now) * 1000.0)))
                state.last_launch_deadline = scheduled_at
            else:
                state.last_launch_deadline = time.monotonic()
<<<<<<< HEAD
        normalized["launch_index"] = launch_index
        work_area = _read_primary_work_area()
        launch_rects = _compute_visible_window_rects(normalized["target_count"], work_area=work_area)
        initial_rect = launch_rects[min(launch_index, len(launch_rects) - 1)]
        if normalized["window_width"] > 0 and normalized["window_height"] > 0:
            initial_rect = _fixed_window_rect(
                initial_rect,
                width=int(normalized["window_width"]),
                height=int(normalized["window_height"]),
                work_area=work_area,
            )
        normalized["initial_rect"] = initial_rect
=======
        launch_rects = _compute_visible_window_rects(normalized["target_count"])
        normalized["launch_index"] = launch_index
        normalized["initial_rect"] = launch_rects[min(launch_index, len(launch_rects) - 1)]
>>>>>>> origin/main
        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000.0)
        return normalized

    async def attach_context(self, config: Mapping[str, Any] | None, *, ctx: BrowserContext, page: Page) -> None:
        normalized = self._normalize(config)
        if normalized is None:
            return
        scope = normalized["scope"]
        key = f"{scope}:{id(ctx)}"
        with self._lock:
            state = self._scopes.setdefault(scope, _VisibleCampaignScopeState())
            existing = state.windows.get(key)
            if existing is None:
                existing = _VisibleCampaignWindow(
                    key=key,
                    page=page,
                    target_count=int(normalized["target_count"]),
                    sequence=len(state.windows),
<<<<<<< HEAD
                    window_width=int(normalized["window_width"] or 0),
                    window_height=int(normalized["window_height"] or 0),
=======
>>>>>>> origin/main
                )
                state.windows[key] = existing
            else:
                existing.page = page
                existing.target_count = int(normalized["target_count"])
<<<<<<< HEAD
                existing.window_width = int(normalized["window_width"] or 0)
                existing.window_height = int(normalized["window_height"] or 0)
=======
>>>>>>> origin/main
        await self._retile_scope(scope)

    async def release_context(self, config: Mapping[str, Any] | None, *, ctx: BrowserContext) -> None:
        normalized = self._normalize(config)
        if normalized is None:
            return
        scope = normalized["scope"]
        should_retile = False
        with self._lock:
            state = self._scopes.get(scope)
            if state is None:
                return
            state.windows.pop(f"{scope}:{id(ctx)}", None)
            if state.windows:
                should_retile = True
            else:
                self._scopes.pop(scope, None)
        if should_retile:
            await self._retile_scope(scope)

    async def _retile_scope(self, scope: str) -> None:
        with self._lock:
            state = self._scopes.get(scope)
            if state is None:
                return
            stale_keys = [
                key
                for key, handle in state.windows.items()
                if self._page_closed(handle.page)
            ]
            for key in stale_keys:
                state.windows.pop(key, None)
            handles = sorted(state.windows.values(), key=lambda item: item.sequence)
            if not handles:
                self._scopes.pop(scope, None)
                return
            planned_count = max(max(handle.target_count for handle in handles), len(handles))
<<<<<<< HEAD
        work_area = _read_primary_work_area()
        rects = _compute_visible_window_rects(planned_count, work_area=work_area)
        for handle, rect in zip(handles, rects):
            if handle.window_width > 0 and handle.window_height > 0:
                rect = _fixed_window_rect(
                    rect,
                    width=handle.window_width,
                    height=handle.window_height,
                    work_area=work_area,
                )
=======
        rects = _compute_visible_window_rects(planned_count)
        for handle, rect in zip(handles, rects):
>>>>>>> origin/main
            await self._apply_window_rect(handle.page, rect)

    @staticmethod
    def _page_closed(page: Page | None) -> bool:
        if page is None:
            return True
        checker = getattr(page, "is_closed", None)
        if callable(checker):
            with contextlib.suppress(Exception):
                return bool(checker())
        return False

    @staticmethod
    async def _apply_window_rect(page: Page, rect: _WindowRect) -> None:
        if _VisibleCampaignLayoutManager._page_closed(page):
            return
        context = getattr(page, "context", None)
        new_cdp_session = getattr(context, "new_cdp_session", None)
        if not callable(new_cdp_session):
            _LOGGER.warning(
                "Visible campaign layout skipped because CDP is unavailable for rect=%s",
                rect,
            )
            return
        session = None
        try:
            session = await new_cdp_session(page)
            target_window = await session.send("Browser.getWindowForTarget")
            window_id = int(target_window.get("windowId") or 0)
            if window_id <= 0:
                _LOGGER.warning(
                    "Visible campaign layout skipped because Browser.getWindowForTarget returned no window id for rect=%s",
                    rect,
                )
                return
            with contextlib.suppress(Exception):
                await session.send(
                    "Browser.setWindowBounds",
                    {
                        "windowId": window_id,
                        "bounds": {"windowState": "normal"},
                    },
                )
            await session.send(
                "Browser.setWindowBounds",
                {
                    "windowId": window_id,
                    "bounds": {
                        "left": int(rect.left),
                        "top": int(rect.top),
                        "width": int(rect.width),
                        "height": int(rect.height),
                    },
                },
            )
        except Exception as exc:
            _LOGGER.warning(
                "Visible campaign layout apply failed for rect=%s: %s",
                rect,
                exc,
            )
        finally:
            if session is not None:
                detach = getattr(session, "detach", None)
                if callable(detach):
                    with contextlib.suppress(Exception):
                        await detach()


_VISIBLE_CAMPAIGN_LAYOUT_MANAGER = _VisibleCampaignLayoutManager()


async def _focus_visible_page(page: Page) -> None:
    if _VisibleCampaignLayoutManager._page_closed(page):
        return
    with contextlib.suppress(Exception):
        await page.bring_to_front()
    with contextlib.suppress(Exception):
        await page.evaluate(
            """() => {
                try {
                    window.focus();
                } catch (_err) {
                }
            }"""
        )
    context = getattr(page, "context", None)
    new_cdp_session = getattr(context, "new_cdp_session", None)
    if not callable(new_cdp_session):
        return
    session = None
    try:
        session = await new_cdp_session(page)
        with contextlib.suppress(Exception):
            await session.send("Page.bringToFront")
    finally:
        if session is not None:
            detach = getattr(session, "detach", None)
            if callable(detach):
                with contextlib.suppress(Exception):
                    await detach()


def build_launch_args(
    *,
    headless: bool,
    locale: Optional[str] = None,
    initial_window_rect: Optional[_WindowRect] = None,
) -> list[str]:
    lang_value = (locale or "").strip()
    args = [
        arg
        for arg in BASE_FLAGS
        if (
            not arg.startswith("--lang=")
            and (initial_window_rect is None or arg not in {"--start-maximized", "--start-fullscreen", "--kiosk"})
            and (initial_window_rect is None or not arg.startswith("--window-size="))
            and (initial_window_rect is None or not arg.startswith("--window-position="))
        )
    ]
    if lang_value:
        args.append(f"--lang={lang_value}")
    if not headless and initial_window_rect is not None:
        args.extend(
            [
                f"--window-size={int(initial_window_rect.width)},{int(initial_window_rect.height)}",
                f"--window-position={int(initial_window_rect.left)},{int(initial_window_rect.top)}",
            ]
        )
    elif not headless and HEADFUL_ADAPTIVE_VIEWPORT and "--start-maximized" not in args:
        args.append("--start-maximized")
    return args


<<<<<<< HEAD
def context_viewport_kwargs(
    *,
    headless: bool,
    initial_window_rect: Optional[_WindowRect] = None,
    fingerprint: Optional[Mapping[str, Any]] = None,
    viewport_override: Optional[Mapping[str, Any]] = None,
) -> dict:
    if not headless and (HEADFUL_ADAPTIVE_VIEWPORT or initial_window_rect is not None):
        return {"no_viewport": True}
    viewport_payload = dict(_WORK_AREA_FALLBACK_VIEWPORT)
    override = _normalize_campaign_desktop_layout(viewport_override)
    if override is not None:
        viewport_payload = {
            "width": int(override["width"]),
            "height": int(override["height"]),
        }
    device_scale_factor = 1
    if override is None and isinstance(fingerprint, Mapping):
        raw_viewport = fingerprint.get("viewport")
        if isinstance(raw_viewport, Mapping):
            try:
                viewport_payload = {
                    "width": int(raw_viewport.get("width") or viewport_payload["width"]),
                    "height": int(raw_viewport.get("height") or viewport_payload["height"]),
                }
            except Exception:
                viewport_payload = dict(_WORK_AREA_FALLBACK_VIEWPORT)
        try:
            device_scale_factor = 2 if int(fingerprint.get("device_scale_factor") or 1) == 2 else 1
        except Exception:
            device_scale_factor = 1
    return {
        "viewport": viewport_payload,
        "device_scale_factor": device_scale_factor,
    }


async def ensure_page_campaign_desktop_layout(
    page: Page,
    layout: Mapping[str, Any] | None,
) -> bool:
    desktop_layout = _normalize_campaign_desktop_layout(layout)
    if desktop_layout is None:
        return False
    rect = _fixed_window_rect(
        _WindowRect(
            left=0,
            top=0,
            width=desktop_layout["width"],
            height=desktop_layout["height"],
        ),
        width=desktop_layout["width"],
        height=desktop_layout["height"],
    )
    await _VisibleCampaignLayoutManager._apply_window_rect(page, rect)
    return True
=======
def context_viewport_kwargs(*, headless: bool, initial_window_rect: Optional[_WindowRect] = None) -> dict:
    if not headless and (HEADFUL_ADAPTIVE_VIEWPORT or initial_window_rect is not None):
        return {"no_viewport": True}
    return {"viewport": dict(DEFAULT_VIEWPORT)}
>>>>>>> origin/main


def _load_storage_state_payload(storage_state: Optional[Union[str, Path]]) -> dict[str, Any]:
    if not storage_state:
        return {}
    try:
        path = Path(storage_state)
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalized_state_cookies(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cookies_raw = payload.get("cookies")
    if not isinstance(cookies_raw, list):
        return []
    cookies: list[dict[str, Any]] = []
    for raw in cookies_raw:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        value = str(raw.get("value") or "")
        if not name:
            continue
        cookie: dict[str, Any] = {"name": name, "value": value}
        for key in ("url", "domain", "path", "expires", "httpOnly", "secure", "sameSite"):
            if key in raw:
                cookie[key] = raw.get(key)
        # Need at least url or domain to be accepted by add_cookies.
        if not cookie.get("url") and not cookie.get("domain"):
            continue
        cookies.append(cookie)
    return cookies


def _normalized_state_origins(payload: dict[str, Any]) -> list[dict[str, Any]]:
    origins_raw = payload.get("origins")
    if not isinstance(origins_raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for raw in origins_raw:
        if not isinstance(raw, dict):
            continue
        origin = str(raw.get("origin") or "").strip()
        local_items = raw.get("localStorage")
        if not origin or not isinstance(local_items, list):
            continue
        rows: list[dict[str, str]] = []
        for item in local_items:
            if not isinstance(item, dict):
                continue
            key = str(item.get("name") or "").strip()
            if not key:
                continue
            rows.append({"name": key, "value": str(item.get("value") or "")})
        if rows:
            normalized.append({"origin": origin, "localStorage": rows})
    return normalized


async def _apply_storage_state_compat(
    ctx: BrowserContext,
    storage_state: Optional[Union[str, Path]],
) -> None:
    payload = _load_storage_state_payload(storage_state)
    if not payload:
        return

    cookies = _normalized_state_cookies(payload)
    if cookies:
        try:
            await ctx.add_cookies(cookies)
        except Exception:
            pass

    origins = _normalized_state_origins(payload)
    if not origins:
        return
    temp_page: Optional[Page] = None
    try:
        if ctx.pages:
            page = ctx.pages[0]
        else:
            temp_page = await ctx.new_page()
            page = temp_page
        for origin_row in origins:
            origin = origin_row["origin"]
            local_rows = origin_row["localStorage"]
            try:
                await page.goto(origin, wait_until="domcontentloaded", timeout=15_000)
                await page.evaluate(
                    """(items) => {
                        for (const row of items) {
                            try {
                                localStorage.setItem(row.name, row.value ?? "");
                            } catch (_err) {}
                        }
                    }""",
                    local_rows,
                )
            except Exception:
                continue
    finally:
        if temp_page is not None:
            try:
                await temp_page.close()
            except Exception:
                pass


def resolve_playwright_executable(headless: bool) -> Optional[Path]:
    return resolve_playwright_chromium_executable(headless=headless)


class AsyncBrowserHandle:
    """Wrapper que ofrece close() sobre el runtime async de Playwright."""

    def __init__(self, runtime):
        self._runtime = runtime

    async def close(self) -> None:
        await self._runtime.stop()


class PlaywrightService:
    """
    Servicio para administrar un navegador Chromium compartido y
    crear contextos aislados por cuenta con storage_state persistente.
    """

    def __init__(
        self,
        headless: bool = False,
        base_profiles: Optional[Path] = None,
        prefer_persistent: bool = False,
        browser_mode: str = PLAYWRIGHT_BROWSER_MODE_DEFAULT,
<<<<<<< HEAD
        subsystem: str = "default",
=======
>>>>>>> origin/main
    ) -> None:
        self._headless = headless
        self._base_profiles = Path(base_profiles) if base_profiles is not None else _base_profiles_path()
        self._prefer_persistent = bool(prefer_persistent)
<<<<<<< HEAD
        self._subsystem = str(subsystem or "").strip().lower() or "default"
        normalized_browser_mode = normalize_browser_mode(browser_mode)
=======
        normalized_browser_mode = str(browser_mode or PLAYWRIGHT_BROWSER_MODE_DEFAULT).strip().lower()
        if normalized_browser_mode not in {
            PLAYWRIGHT_BROWSER_MODE_DEFAULT,
            PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
            PLAYWRIGHT_BROWSER_MODE_MANAGED,
        }:
            normalized_browser_mode = PLAYWRIGHT_BROWSER_MODE_DEFAULT
>>>>>>> origin/main
        self._browser_mode = normalized_browser_mode
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._launch_proxy: Optional[dict] = None
        self._safe_mode = False
        self._runtime = PlaywrightRuntime(headless=self._headless, owner_module=__name__)

    def _use_persistent_profile(self, *, safe_mode: bool) -> bool:
        # Visible/manual flows and auth/session flows flagged as persistent
        # must run with a real persistent profile dir per account.
        return (not safe_mode) and (self._prefer_persistent or (not self._headless))

    def _resolve_launch_executable(self) -> Optional[Path]:
        if self._browser_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
            return resolve_google_chrome_executable()
        return resolve_playwright_executable(headless=self._headless)

    @property
    def playwright(self) -> Optional[Playwright]:
        return self._playwright

    @staticmethod
    def _launch_proxy_payload(proxy: Optional[dict]) -> Optional[dict]:
        if not proxy or not isinstance(proxy, dict):
            return None
        server = (
            str(proxy.get("server") or proxy.get("url") or proxy.get("proxy") or "")
            .strip()
        )
        if not server:
            return None
        payload: dict = {"server": server}
        username = str(proxy.get("username") or "").strip()
        password = str(proxy.get("password") or "").strip()
        if username:
            payload["username"] = username
        if password:
            payload["password"] = password
        return payload

    async def start(
        self,
        launch_proxy: Optional[dict] = None,
        *,
        safe_mode: bool = False,
        launch_browser: Optional[bool] = None,
    ) -> "PlaywrightService":
        use_persistent_profile = self._use_persistent_profile(safe_mode=safe_mode)
        should_launch_browser = (
            bool(launch_browser)
            if launch_browser is not None
            else (not use_persistent_profile)
        )

        if self._playwright is not None and (self._browser is not None or not should_launch_browser):
            return self

        self._base_profiles.mkdir(parents=True, exist_ok=True)
        proxy_payload = None if safe_mode else self._launch_proxy_payload(launch_proxy)
        self._launch_proxy = proxy_payload
        self._safe_mode = bool(safe_mode)
        executable = self._resolve_launch_executable()
        await self._runtime.start(
            launch_proxy=proxy_payload,
            executable_path=executable,
<<<<<<< HEAD
            launch_args=build_launch_args(headless=self._headless),
=======
            launch_args=build_launch_args(headless=self._headless, locale=DEFAULT_LOCALE),
>>>>>>> origin/main
            safe_mode=safe_mode,
            launch_browser=should_launch_browser,
            force_headless=True if safe_mode else self._headless,
            browser_mode=self._browser_mode,
        )
        self._playwright = self._runtime.playwright
        self._browser = self._runtime.browser if should_launch_browser else None
        return self

    async def new_context_for_account(
        self,
        profile_dir: Union[str, Path],
        storage_state: Optional[Union[str, Path]] = None,
        proxy: Optional[dict] = None,
        *,
<<<<<<< HEAD
        timezone_id: Optional[str] = None,
        safe_mode: bool = False,
        visible_browser_layout: Mapping[str, Any] | None = None,
        campaign_desktop_layout: Mapping[str, Any] | None = None,
=======
        safe_mode: bool = False,
        visible_browser_layout: Mapping[str, Any] | None = None,
>>>>>>> origin/main
    ) -> BrowserContext:
        use_persistent_profile = self._use_persistent_profile(safe_mode=safe_mode)

        profile_path = Path(profile_dir)
        _migrate_legacy_profile_dir(profile_path)
        profile_path.mkdir(parents=True, exist_ok=True)
<<<<<<< HEAD
        account_name = str(profile_path.name or "account")
        fingerprint = get_account_fingerprint(account_name)
        resolved_timezone_id = str(timezone_id or "").strip() or str(fingerprint["timezone_id"])
        desktop_layout = _normalize_campaign_desktop_layout(campaign_desktop_layout)
=======
>>>>>>> origin/main
        layout_config: dict[str, Any] | None = None
        initial_window_rect: _WindowRect | None = None
        if use_persistent_profile and not self._headless and not safe_mode:
            layout_config = await _VISIBLE_CAMPAIGN_LAYOUT_MANAGER.before_context_launch(visible_browser_layout)
            raw_initial_rect = layout_config.get("initial_rect") if isinstance(layout_config, Mapping) else None
            if isinstance(raw_initial_rect, _WindowRect):
                initial_window_rect = raw_initial_rect
            elif isinstance(raw_initial_rect, Mapping):
                try:
                    initial_window_rect = _WindowRect(
                        left=int(raw_initial_rect.get("left") or 0),
                        top=int(raw_initial_rect.get("top") or 0),
                        width=int(raw_initial_rect.get("width") or 0),
                        height=int(raw_initial_rect.get("height") or 0),
                    )
                except Exception:
                    initial_window_rect = None
<<<<<<< HEAD
            if initial_window_rect is None and desktop_layout is not None:
                initial_window_rect = _fixed_window_rect(
                    _WindowRect(left=0, top=0, width=desktop_layout["width"], height=desktop_layout["height"]),
                    width=desktop_layout["width"],
                    height=desktop_layout["height"],
                )

        launch_args = build_launch_args(
            headless=self._headless,
            locale=str(fingerprint["locale"]),
=======

        launch_args = build_launch_args(
            headless=self._headless,
            locale=DEFAULT_LOCALE,
>>>>>>> origin/main
            initial_window_rect=initial_window_rect,
        )
        viewport_kwargs = context_viewport_kwargs(
            headless=self._headless,
            initial_window_rect=initial_window_rect,
<<<<<<< HEAD
            fingerprint=fingerprint,
            viewport_override=desktop_layout,
=======
>>>>>>> origin/main
        )

        storage_state_path: Optional[str] = None
        if storage_state:
            storage_state_path = str(storage_state)

        # If browser already has a global proxy (shared mode), do not override at context level.
        # Persistent mode applies proxy at launch_persistent_context level.
        context_proxy = None if (self._launch_proxy or safe_mode) else self._launch_proxy_payload(proxy)
        if use_persistent_profile:
            context_proxy = self._launch_proxy_payload(proxy)

        context_kwargs = {
<<<<<<< HEAD
            "account": account_name,
=======
            "account": str(profile_path.name or "account"),
>>>>>>> origin/main
            "profile_dir": profile_path,
            "storage_state": storage_state_path,
            "proxy": context_proxy,
            "mode": "persistent" if use_persistent_profile else "shared",
            "executable_path": self._resolve_launch_executable(),
            "launch_args": launch_args,
<<<<<<< HEAD
            "user_agent": str(fingerprint["user_agent"]),
            "locale": str(fingerprint["locale"]),
            "timezone_id": resolved_timezone_id,
=======
            "user_agent": DEFAULT_USER_AGENT,
            "locale": DEFAULT_LOCALE,
            "timezone_id": DEFAULT_TIMEZONE,
>>>>>>> origin/main
            "viewport_kwargs": viewport_kwargs,
            "permissions": [],
            "launch_proxy": None if safe_mode else (None if use_persistent_profile else self._launch_proxy),
            "force_headless": True if safe_mode else self._headless,
            "safe_mode": safe_mode,
            "browser_mode": self._browser_mode,
<<<<<<< HEAD
            "subsystem": self._subsystem,
=======
>>>>>>> origin/main
        }
        ctx = await self._runtime.get_context(**context_kwargs)
        # Runtime start is handled inside get_context; mirror live handles here.
        self._playwright = self._runtime.playwright
        self._browser = None if use_persistent_profile else self._runtime.browser
        await self._install_login_sync_guard(ctx)
        if use_persistent_profile and storage_state_path:
            # Manual/account actions run in persistent mode but the current
            # session system still persists cookies/localStorage in
            # storage_state.json. Seed the persistent profile before any
            # navigation so the visible browser opens already authenticated.
            await _apply_storage_state_compat(ctx, storage_state_path)
        if use_persistent_profile:
            print(
                f"[Browser Layer] Persistent profile dir -> {profile_path}",
                flush=True,
            )
        ctx.set_default_timeout(30_000)
        try:
            if not ctx.pages:
                page = await ctx.new_page()
                try:
                    page.set_default_timeout(30_000)
                    page.set_default_navigation_timeout(30_000)
                except Exception:
                    pass
            else:
                page = ctx.pages[0]
<<<<<<< HEAD
            if desktop_layout is not None:
                with contextlib.suppress(Exception):
                    setattr(ctx, "_campaign_desktop_layout", dict(desktop_layout))
                with contextlib.suppress(Exception):
                    setattr(page, "_campaign_desktop_layout", dict(desktop_layout))
=======
>>>>>>> origin/main
            if layout_config is not None:
                await _VISIBLE_CAMPAIGN_LAYOUT_MANAGER.attach_context(layout_config, ctx=ctx, page=page)
                self._bind_visible_browser_layout_cleanup(ctx, layout_config)
                await _focus_visible_page(page)
            return ctx
        except Exception as page_exc:
            with contextlib.suppress(Exception):
                await ctx.close()
            if not is_driver_crash_error(page_exc):
                raise
            if self._browser_mode == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY:
                await self.record_diagnostic_failure(
                    code="driver_crash_new_page_no_fallback",
                    error=page_exc,
                    extra={"account": str(profile_path.name or "account"), "stage": "new_context_for_account"},
                )
                raise RuntimeError(f"PW-CONTEXT-PAGE-FAILED: {page_exc}") from page_exc
            await self.record_diagnostic_failure(
                code="driver_crash_new_page_retry",
                error=page_exc,
                extra={"account": str(profile_path.name or "account"), "stage": "new_context_for_account"},
            )
            restarted = await self._runtime.restart(reason="new_page_driver_crash")
            if not restarted:
                raise RuntimeError(
                    f"PW-RESTART-BLOCKED: runtime_id={self._runtime.runtime_id} "
                    f"active_contexts={self._runtime.active_contexts}"
                ) from page_exc
            safe_kwargs = dict(context_kwargs)
            safe_kwargs["proxy"] = None
            safe_kwargs["launch_proxy"] = None
            safe_kwargs["force_headless"] = True
            safe_kwargs["safe_mode"] = True
            try:
                ctx = await self._runtime.get_context(**safe_kwargs)
                ctx.set_default_timeout(30_000)
                if not ctx.pages:
                    _ = await ctx.new_page()
                return ctx
            except Exception as safe_exc:
                await self.record_diagnostic_failure(
                    code="driver_crash_new_page_safe_failed",
                    error=safe_exc,
                    extra={"account": str(profile_path.name or "account"), "stage": "new_context_for_account_safe"},
                )
                raise RuntimeError(f"PW-CONTEXT-PAGE-FAILED: {safe_exc}") from safe_exc

    async def _install_login_sync_guard(self, ctx: BrowserContext) -> None:
        async def _abort_login_sync(route: Any) -> None:
            with contextlib.suppress(Exception):
                await route.abort()

        for pattern in _LOGIN_SYNC_BLOCK_PATTERNS:
            with contextlib.suppress(Exception):
                await ctx.route(pattern, _abort_login_sync)

    @staticmethod
    def _bind_visible_browser_layout_cleanup(
        ctx: BrowserContext,
        layout_config: Mapping[str, Any],
    ) -> None:
        event_emitter = getattr(ctx, "on", None)
        if not callable(event_emitter):
            return

        def _on_close(*_args: Any) -> None:
            with contextlib.suppress(RuntimeError):
                loop = asyncio.get_running_loop()
                loop.create_task(
                    _VISIBLE_CAMPAIGN_LAYOUT_MANAGER.release_context(layout_config, ctx=ctx)
                )

        with contextlib.suppress(Exception):
            event_emitter("close", _on_close)

    async def record_diagnostic_failure(
        self,
        *,
        code: str,
        error: BaseException,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        try:
            await self._runtime.record_failure(
                code=code,
                error=error,
                executable_path=self._resolve_launch_executable(),
                extra=extra,
            )
        except Exception:
            pass

    async def save_storage_state(
        self,
        ctx: BrowserContext,
        destination: Union[str, Path],
    ) -> Path:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
<<<<<<< HEAD
        mode = "headless" if (self._headless or self._safe_mode) else "headful"
        try:
            await ctx.storage_state(path=str(dest))
        except Exception as exc:
            emit_profile_lifecycle_diagnostic(
                event_type="storage_state_save_failed",
                profile_dir=dest.parent,
                account=dest.parent.name,
                subsystem=self._subsystem,
                mode=mode,
                reason_code="storage_state_save_failed",
                pid=os.getpid(),
                payload={
                    "error": str(exc) or type(exc).__name__,
                    "error_type": type(exc).__name__,
                    "storage_state_path": str(dest),
                },
                callsite_skip=2,
            )
            raise
        emit_profile_lifecycle_diagnostic(
            event_type="storage_state_saved",
            profile_dir=dest.parent,
            account=dest.parent.name,
            subsystem=self._subsystem,
            mode=mode,
            reason_code="storage_state_saved",
            pid=os.getpid(),
            payload={"storage_state_path": str(dest)},
            callsite_skip=2,
        )
=======
        await ctx.storage_state(path=str(dest))
>>>>>>> origin/main
        return dest

    async def close(self) -> None:
        self._browser = None
        self._playwright = None
        await self._runtime.stop()


async def launch_persistent(
    account_id: str,
    proxy: Optional[dict] = None,
    headful: Optional[bool] = None,
    storage_state: Optional[Union[str, Path]] = None,
) -> Tuple[PlaywrightRuntime, BrowserContext]:
    """
    Lanza un contexto de navegador PERSISTENTE por cuenta (API legado).
    - account_id: normalmente el username de IG.
    - proxy: dict opcional: {"server": "http://ip:port", "username": "...", "password": "..."}
    - headful: si None, usa env HUMAN_HEADFUL (default true).
    """
    base_profiles = _base_profiles_path()
    base_profiles.mkdir(exist_ok=True)
<<<<<<< HEAD
    user_data_dir = browser_profile_dir(account_id, profiles_root=base_profiles)
=======
    user_data_dir = base_profiles / account_id
>>>>>>> origin/main
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if headful is None:
        headful = os.getenv("HUMAN_HEADFUL", "true").lower() == "true"

    runtime = PlaywrightRuntime(headless=not headful, owner_module=__name__)
    executable = resolve_playwright_executable(headless=not headful)
<<<<<<< HEAD
    fingerprint = get_account_fingerprint(account_id)
=======
>>>>>>> origin/main
    storage_state_path: Optional[str] = None
    if storage_state:
        candidate = Path(storage_state)
        if candidate.exists():
            storage_state_path = str(candidate)
    default_storage_state = user_data_dir / "storage_state.json"
    if not storage_state_path and default_storage_state.exists():
        storage_state_path = str(default_storage_state)
    proxy_payload = proxy or None
<<<<<<< HEAD
    recovery_dir = browser_profile_dir(f"{account_id}__recovery", profiles_root=base_profiles)
=======
    recovery_dir = base_profiles / f"{account_id}__recovery"
>>>>>>> origin/main

    attempts: list[tuple[str, Path, Optional[dict]]] = [
        ("primary", user_data_dir, proxy_payload),
    ]
    if proxy_payload is not None:
        attempts.append(("primary_no_proxy", user_data_dir, None))
    attempts.append(("recovery", recovery_dir, proxy_payload))
    if proxy_payload is not None:
        attempts.append(("recovery_no_proxy", recovery_dir, None))

    errors: list[str] = []
    for label, target_dir, target_proxy in attempts:
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            ctx = await runtime.get_context(
                account=account_id,
                profile_dir=target_dir,
                storage_state=None,
                proxy=target_proxy,
                mode="persistent",
                executable_path=executable,
<<<<<<< HEAD
                launch_args=build_launch_args(headless=not headful, locale=str(fingerprint["locale"])),
                user_agent=str(fingerprint["user_agent"]),
                locale=str(fingerprint["locale"]),
                timezone_id=str(fingerprint["timezone_id"]),
                viewport_kwargs=context_viewport_kwargs(headless=not headful, fingerprint=fingerprint),
=======
                launch_args=build_launch_args(headless=not headful, locale=DEFAULT_LOCALE),
                user_agent=DEFAULT_USER_AGENT,
                locale=DEFAULT_LOCALE,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
>>>>>>> origin/main
                permissions=[],
                launch_proxy=target_proxy,
                force_headless=not headful,
            )
            if storage_state_path:
                await _apply_storage_state_compat(ctx, storage_state_path)
            try:
                probe_page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                await probe_page.goto("about:blank", wait_until="domcontentloaded", timeout=15_000)
            except Exception as probe_exc:
                try:
                    await runtime.record_failure(
                        code="persistent_probe_failed",
                        error=probe_exc,
                        executable_path=executable,
                        extra={"account": account_id, "attempt": label},
                    )
                except Exception:
                    pass
                with contextlib.suppress(Exception):
                    await ctx.close()
                if is_driver_crash_error(probe_exc):
                    try:
                        print("mode=persistent failed -> fallback to shared", flush=True)
                    except Exception:
                        pass
                    restarted = False
                    with contextlib.suppress(Exception):
                        restarted = await runtime.restart(reason=f"{label}_probe_driver_crash")
                    if not restarted:
                        errors.append(f"{label}_restart_blocked")
                        continue
                    try:
                        shared_ctx = await runtime.get_context(
                            account=account_id,
                            profile_dir=target_dir,
                            storage_state=storage_state_path,
                            proxy=target_proxy,
                            mode="shared",
                            executable_path=executable,
<<<<<<< HEAD
                            launch_args=build_launch_args(headless=not headful, locale=str(fingerprint["locale"])),
                            user_agent=str(fingerprint["user_agent"]),
                            locale=str(fingerprint["locale"]),
                            timezone_id=str(fingerprint["timezone_id"]),
                            viewport_kwargs=context_viewport_kwargs(headless=not headful, fingerprint=fingerprint),
=======
                            launch_args=build_launch_args(headless=not headful, locale=DEFAULT_LOCALE),
                            user_agent=DEFAULT_USER_AGENT,
                            locale=DEFAULT_LOCALE,
                            timezone_id=DEFAULT_TIMEZONE,
                            viewport_kwargs=context_viewport_kwargs(headless=not headful),
>>>>>>> origin/main
                            permissions=[],
                            launch_proxy=target_proxy,
                            force_headless=not headful,
                        )
                        shared_probe = shared_ctx.pages[0] if shared_ctx.pages else await shared_ctx.new_page()
                        await shared_probe.goto("about:blank", wait_until="domcontentloaded", timeout=15_000)
                        return runtime, shared_ctx
                    except Exception as shared_exc:
                        with contextlib.suppress(Exception):
                            await runtime.record_failure(
                                code="persistent_probe_shared_fallback_failed",
                                error=shared_exc,
                                executable_path=executable,
                                extra={"account": account_id, "attempt": label},
                            )
                        errors.append(f"{label}_shared_fallback: {shared_exc}")
                    errors.append(f"{label}: {probe_exc}")
                    continue
                raise
            return runtime, ctx
        except Exception as exc:
            if is_driver_crash_error(exc):
                restarted = False
                with contextlib.suppress(Exception):
                    restarted = await runtime.restart(reason=f"{label}_driver_crash")
                if not restarted:
                    errors.append(f"{label}_restart_blocked")
            errors.append(f"{label}: {exc}")
            continue
    try:
        await runtime.stop()
    except Exception:
        pass
    details_parts = errors or ["unknown error"]
    details = " | ".join(details_parts[-6:])
    raise RuntimeError(f"PW-PERSISTENT-FAILED: launch_persistent failed after fallbacks ({details})")


async def get_page(ctx: BrowserContext) -> Page:
    """Devuelve la primera página abierta o crea una nueva."""
    return ctx.pages[0] if ctx.pages else await ctx.new_page()


async def ensure_context(
    *,
    account: str,
    headful: bool = True,
    lang: Optional[str] = None,
    proxy: Optional[dict] = None,
    mode: str = "shared",
) -> Tuple[AsyncBrowserHandle, BrowserContext, Page]:
    """
    Crea contexto Playwright para la cuenta y devuelve browser/context/page async.
    - mode="shared" (default): browser compartido + storage_state por cuenta.
    - mode="persistent": contexto persistente por cuenta (uso interactivo).
    """
    runtime = PlaywrightRuntime(headless=not headful, owner_module=__name__)
    base_profiles = _base_profiles_path()
<<<<<<< HEAD
    profile_dir = browser_profile_dir(account, profiles_root=base_profiles)
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = profile_dir / "storage_state.json"

    fingerprint = get_account_fingerprint(account)
    locale = (lang or str(fingerprint["locale"]) or "").strip()
    timezone_id = _timezone_for_locale(locale, fallback=str(fingerprint["timezone_id"]))
=======
    profile_dir = base_profiles / account
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = profile_dir / "storage_state.json"

    locale = (lang or DEFAULT_LOCALE or "").strip()
>>>>>>> origin/main
    args = build_launch_args(headless=not headful, locale=locale)

    executable = resolve_playwright_executable(headless=not headful)
    normalized_mode = str(mode or "shared").strip().lower()
    if normalized_mode not in {"shared", "persistent"}:
        normalized_mode = "shared"

    selected_storage_state: Optional[str] = None
    if normalized_mode == "shared":
        if storage_state_path.exists():
            selected_storage_state = str(storage_state_path)
        else:
            # Bootstrap interactivo mínimo para permitir generar storage_state
            # y luego continuar por el modo compartido.
            bootstrap_ctx = await runtime.get_context(
                account=account,
                profile_dir=profile_dir,
                storage_state=None,
                proxy=proxy,
                mode="persistent",
                executable_path=executable,
                launch_args=args,
<<<<<<< HEAD
                user_agent=str(fingerprint["user_agent"]),
                locale=locale,
                timezone_id=timezone_id,
                viewport_kwargs=context_viewport_kwargs(headless=not headful, fingerprint=fingerprint),
=======
                user_agent=DEFAULT_USER_AGENT,
                locale=locale,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
>>>>>>> origin/main
                permissions=[],
                launch_proxy=proxy,
                force_headless=not headful,
            )
            try:
                with contextlib.suppress(Exception):
                    await bootstrap_ctx.storage_state(path=str(storage_state_path))
            finally:
                with contextlib.suppress(Exception):
                    await bootstrap_ctx.close()
            if storage_state_path.exists():
                selected_storage_state = str(storage_state_path)

    context: BrowserContext = await runtime.get_context(
        account=account,
        profile_dir=profile_dir,
        storage_state=selected_storage_state if normalized_mode == "shared" else None,
        proxy=proxy,
        mode=normalized_mode,
        executable_path=executable,
        launch_args=args,
<<<<<<< HEAD
        user_agent=str(fingerprint["user_agent"]),
        locale=locale,
        timezone_id=timezone_id,
        viewport_kwargs=context_viewport_kwargs(headless=not headful, fingerprint=fingerprint),
=======
        user_agent=DEFAULT_USER_AGENT,
        locale=locale,
        timezone_id=DEFAULT_TIMEZONE,
        viewport_kwargs=context_viewport_kwargs(headless=not headful),
>>>>>>> origin/main
        permissions=[],
        launch_proxy=proxy,
        force_headless=not headful,
    )
    context.set_default_timeout(30_000)
    try:
        page: Page = context.pages[0] if context.pages else await context.new_page()
    except Exception as page_exc:
        with contextlib.suppress(Exception):
            await context.close()
        if normalized_mode == "persistent" and is_driver_crash_error(page_exc):
            try:
                print("mode=persistent failed -> fallback to shared", flush=True)
            except Exception:
                pass
            restarted = False
            with contextlib.suppress(Exception):
                restarted = await runtime.restart(reason="ensure_context_page_driver_crash")
            if not restarted:
                raise RuntimeError(
                    f"PW-RESTART-BLOCKED: runtime_id={runtime.runtime_id} "
                    f"active_contexts={runtime.active_contexts}"
                ) from page_exc
            shared_storage = selected_storage_state
            if not shared_storage and storage_state_path.exists():
                shared_storage = str(storage_state_path)
            context = await runtime.get_context(
                account=account,
                profile_dir=profile_dir,
                storage_state=shared_storage,
                proxy=proxy,
                mode="shared",
                executable_path=executable,
                launch_args=args,
<<<<<<< HEAD
                user_agent=str(fingerprint["user_agent"]),
                locale=locale,
                timezone_id=timezone_id,
                viewport_kwargs=context_viewport_kwargs(headless=not headful, fingerprint=fingerprint),
=======
                user_agent=DEFAULT_USER_AGENT,
                locale=locale,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
>>>>>>> origin/main
                permissions=[],
                launch_proxy=proxy,
                force_headless=not headful,
            )
            context.set_default_timeout(30_000)
            page = context.pages[0] if context.pages else await context.new_page()
        else:
            raise
    return AsyncBrowserHandle(runtime), context, page


async def shutdown(
    pw_or_service: Union[Playwright, PlaywrightService, PlaywrightRuntime],
    ctx: Optional[BrowserContext],
):
    """
    Cierra el contexto y el runtime de Playwright con seguridad.
    Acepta tanto la API vieja (Playwright) como la nueva (PlaywrightService).
    """
    if ctx is not None:
        try:
            await ctx.close()
        except Exception:
            pass

    if isinstance(pw_or_service, PlaywrightService):
        await pw_or_service.close()
    else:
        await pw_or_service.stop()
