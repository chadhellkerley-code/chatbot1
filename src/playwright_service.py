from __future__ import annotations

import os
import json
import contextlib
import sqlite3
import shutil
from pathlib import Path
from paths import browser_profiles_root, runtime_base
from typing import Optional, Tuple, Union, Any

from playwright.async_api import Browser, BrowserContext, Page, Playwright
from src.runtime.playwright_resolver import (
    ensure_local_playwright_browsers_env,
    resolve_google_chrome_executable,
    resolve_playwright_chromium_executable,
)
from src.runtime.playwright_runtime import (
    PLAYWRIGHT_BASE_FLAGS,
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
    PLAYWRIGHT_BROWSER_MODE_DEFAULT,
    PlaywrightRuntime,
    is_driver_crash_error,
)

# Carpeta donde se guardan las sesiones persistentes (cookies, localStorage, etc.)
# IMPORTANTE: Chrome real maneja mejor perfiles persistentes bajo LocalAppData
# que dentro del proyecto en Desktop/Downloads. Si el usuario no define
# PROFILES_DIR, usamos LocalAppData y migramos perfiles legacy bajo demanda.
_BASE_ROOT = runtime_base(Path(__file__).resolve().parent.parent)
ensure_local_playwright_browsers_env()


def _default_local_profiles_root() -> Path | None:
    local_app_data = (os.environ.get("LOCALAPPDATA") or "").strip()
    if not local_app_data:
        return None
    try:
        root = Path(local_app_data).expanduser() / "InstaCRM" / "runtime" / "browser_profiles"
        root.mkdir(parents=True, exist_ok=True)
        return root
    except Exception:
        return None


def _resolve_profiles_root(base_root: Path) -> Path:
    profiles_env = (os.environ.get("PROFILES_DIR") or "").strip()
    if profiles_env:
        profiles_path = Path(profiles_env).expanduser()
        resolved = profiles_path if profiles_path.is_absolute() else (base_root / profiles_path)
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved
    local_profiles = _default_local_profiles_root()
    if local_profiles is not None:
        return local_profiles
    return browser_profiles_root(base_root)


_LEGACY_BASE_PROFILES = browser_profiles_root(_BASE_ROOT)
BASE_PROFILES = _resolve_profiles_root(_BASE_ROOT)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on", "si"}


def _migrate_legacy_profile_dir(profile_path: Path) -> None:
    legacy_root = _LEGACY_BASE_PROFILES
    if legacy_root == BASE_PROFILES:
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


DEFAULT_VIEWPORT = {
    "width": _env_int("PLAYWRIGHT_VIEWPORT_WIDTH", 1920, minimum=800),
    "height": _env_int("PLAYWRIGHT_VIEWPORT_HEIGHT", 1080, minimum=600),
}
HEADFUL_ADAPTIVE_VIEWPORT = _env_flag("PLAYWRIGHT_HEADFUL_ADAPTIVE_VIEWPORT", True)
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


def build_launch_args(*, headless: bool, locale: Optional[str] = None) -> list[str]:
    lang_value = (locale or "").strip()
    args = [arg for arg in BASE_FLAGS if not arg.startswith("--lang=")]
    if lang_value:
        args.append(f"--lang={lang_value}")
    if not headless and HEADFUL_ADAPTIVE_VIEWPORT and "--start-maximized" not in args:
        args.append("--start-maximized")
    return args


def context_viewport_kwargs(*, headless: bool) -> dict:
    if not headless and HEADFUL_ADAPTIVE_VIEWPORT:
        return {"no_viewport": True}
    return {"viewport": dict(DEFAULT_VIEWPORT)}


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
    ) -> None:
        self._headless = headless
        self._base_profiles = Path(base_profiles or BASE_PROFILES)
        self._prefer_persistent = bool(prefer_persistent)
        normalized_browser_mode = str(browser_mode or PLAYWRIGHT_BROWSER_MODE_DEFAULT).strip().lower()
        if normalized_browser_mode not in {
            PLAYWRIGHT_BROWSER_MODE_DEFAULT,
            PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        }:
            normalized_browser_mode = PLAYWRIGHT_BROWSER_MODE_DEFAULT
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
            launch_args=build_launch_args(headless=self._headless, locale=DEFAULT_LOCALE),
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
        safe_mode: bool = False,
    ) -> BrowserContext:
        use_persistent_profile = self._use_persistent_profile(safe_mode=safe_mode)

        profile_path = Path(profile_dir)
        _migrate_legacy_profile_dir(profile_path)
        profile_path.mkdir(parents=True, exist_ok=True)

        storage_state_path: Optional[str] = None
        if storage_state:
            storage_state_path = str(storage_state)

        # If browser already has a global proxy (shared mode), do not override at context level.
        # Persistent mode applies proxy at launch_persistent_context level.
        context_proxy = None if (self._launch_proxy or safe_mode) else self._launch_proxy_payload(proxy)
        if use_persistent_profile:
            context_proxy = self._launch_proxy_payload(proxy)

        context_kwargs = {
            "account": str(profile_path.name or "account"),
            "profile_dir": profile_path,
            "storage_state": storage_state_path,
            "proxy": context_proxy,
            "mode": "persistent" if use_persistent_profile else "shared",
            "executable_path": self._resolve_launch_executable(),
            "launch_args": build_launch_args(headless=self._headless, locale=DEFAULT_LOCALE),
            "user_agent": DEFAULT_USER_AGENT,
            "locale": DEFAULT_LOCALE,
            "timezone_id": DEFAULT_TIMEZONE,
            "viewport_kwargs": context_viewport_kwargs(headless=self._headless),
            "permissions": [],
            "launch_proxy": None if safe_mode else (None if use_persistent_profile else self._launch_proxy),
            "force_headless": True if safe_mode else self._headless,
            "safe_mode": safe_mode,
            "browser_mode": self._browser_mode,
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
        await ctx.storage_state(path=str(dest))
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
    BASE_PROFILES.mkdir(exist_ok=True)
    user_data_dir = BASE_PROFILES / account_id
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if headful is None:
        headful = os.getenv("HUMAN_HEADFUL", "true").lower() == "true"

    runtime = PlaywrightRuntime(headless=not headful, owner_module=__name__)
    executable = resolve_playwright_executable(headless=not headful)
    storage_state_path: Optional[str] = None
    if storage_state:
        candidate = Path(storage_state)
        if candidate.exists():
            storage_state_path = str(candidate)
    default_storage_state = user_data_dir / "storage_state.json"
    if not storage_state_path and default_storage_state.exists():
        storage_state_path = str(default_storage_state)
    proxy_payload = proxy or None
    recovery_dir = BASE_PROFILES / f"{account_id}__recovery"

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
                launch_args=build_launch_args(headless=not headful, locale=DEFAULT_LOCALE),
                user_agent=DEFAULT_USER_AGENT,
                locale=DEFAULT_LOCALE,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
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
                            launch_args=build_launch_args(headless=not headful, locale=DEFAULT_LOCALE),
                            user_agent=DEFAULT_USER_AGENT,
                            locale=DEFAULT_LOCALE,
                            timezone_id=DEFAULT_TIMEZONE,
                            viewport_kwargs=context_viewport_kwargs(headless=not headful),
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
    profile_dir = BASE_PROFILES / account
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = profile_dir / "storage_state.json"

    locale = (lang or DEFAULT_LOCALE or "").strip()
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
                user_agent=DEFAULT_USER_AGENT,
                locale=locale,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
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
        user_agent=DEFAULT_USER_AGENT,
        locale=locale,
        timezone_id=DEFAULT_TIMEZONE,
        viewport_kwargs=context_viewport_kwargs(headless=not headful),
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
                user_agent=DEFAULT_USER_AGENT,
                locale=locale,
                timezone_id=DEFAULT_TIMEZONE,
                viewport_kwargs=context_viewport_kwargs(headless=not headful),
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
