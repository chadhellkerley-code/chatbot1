from __future__ import annotations

import asyncio
<<<<<<< HEAD
import contextlib
=======
>>>>>>> origin/main
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

from playwright.async_api import BrowserContext, Page
from paths import logs_root
<<<<<<< HEAD
from src.browser_telemetry import log_browser_stage
from src.inbox_diagnostics import record_inbox_diagnostic
=======
>>>>>>> origin/main
from src.instagram_adapter import (
    BASE_URL,
    check_logged_in,
    get_login_errors,
    human_login,
    is_logged_in,
    _ensure_login_view,
)
<<<<<<< HEAD
from src.campaign_timezone_policy import (
    CampaignBrowserTimezoneResolution,
    CampaignTimezoneResolutionError,
    resolve_campaign_browser_timezone,
)
=======
>>>>>>> origin/main
from src.playwright_service import BASE_PROFILES, PlaywrightService, get_page
from src.proxy_payload import normalize_playwright_proxy, proxy_from_account
from src.runtime.playwright_runtime import (
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
    PLAYWRIGHT_BROWSER_MODE_MANAGED,
<<<<<<< HEAD
    PersistentProfileOwnershipError,
    is_driver_crash_error,
    run_coroutine_sync,
)
from src.browser_profile_paths import browser_profile_dir, browser_storage_state_path
=======
    is_driver_crash_error,
    run_coroutine_sync,
)
from src.browser_profile_paths import browser_storage_state_path
>>>>>>> origin/main

logger = logging.getLogger(__name__)

LOGIN_FAILED_DIRNAME = "login_failed_screenshots"
STORAGE_FILENAME = "storage_state.json"
ACCOUNTS_LOGIN_NAV_TIMEOUT_ENV = "ACCOUNTS_LOGIN_NAV_TIMEOUT_MS"
ACCOUNTS_LOGIN_INIT_TIMEOUT_ENV = "ACCOUNTS_LOGIN_INIT_TIMEOUT_SECONDS"
LEGACY_LEADS_LOGIN_NAV_TIMEOUT_ENV = "LEADS_INIT_NAV_TIMEOUT_MS"
LEGACY_LEADS_LOGIN_INIT_TIMEOUT_ENV = "LEADS_INIT_ACCOUNT_TIMEOUT_SECONDS"

_EMAIL_CHALLENGE_URL_PARTS = (
    "challenge",
    "checkpoint",
    "accounts/confirm_email",
)
_CHALLENGE_URL_PARTS = _EMAIL_CHALLENGE_URL_PARTS + ("two_factor", "accounts/suspended")
_EMAIL_CHALLENGE_TEXT_PATTERNS = [
    re.compile(r"we can send you an email", re.I),
    re.compile(r"confirm (it'?s|its) you", re.I),
    re.compile(r"send security code", re.I),
    re.compile(r"enter security code", re.I),
    re.compile(r"check your email", re.I),
    re.compile(r"we sent you an email", re.I),
    re.compile(r"send email", re.I),
    re.compile(r"codigo de seguridad", re.I),
    re.compile(r"enviar codigo", re.I),
    re.compile(r"revisa tu correo", re.I),
    re.compile(r"confirmar tu identidad", re.I),
]


class ChallengeRequired(RuntimeError):
    pass


def _run_sync(coro):
    return run_coroutine_sync(coro)


def _has_chrome_error_url(page: Optional[Page]) -> bool:
    if page is None:
        return False
    try:
        current_url = str(page.url or "").strip().lower()
    except Exception:
        return False
    return current_url.startswith("chrome-error://")


def _bootstrap_nav_timeout_ms() -> int:
    raw = os.getenv(ACCOUNTS_LOGIN_NAV_TIMEOUT_ENV)
    if raw is None:
        raw = os.getenv(LEGACY_LEADS_LOGIN_NAV_TIMEOUT_ENV)
    try:
        value = int(raw) if raw is not None else 10_000
    except Exception:
        value = 10_000
    return max(4_000, min(30_000, value))


def _accounts_login_init_timeout_seconds(*, headless: bool) -> float:
    default_init_timeout = 180.0 if not headless else 120.0
    raw = os.getenv(ACCOUNTS_LOGIN_INIT_TIMEOUT_ENV)
    if raw is None:
        raw = os.getenv(LEGACY_LEADS_LOGIN_INIT_TIMEOUT_ENV)
    try:
        value = float(raw) if raw is not None else default_init_timeout
    except Exception:
        value = default_init_timeout
    return max(10.0, value)


async def _safe_nav_candidates(
    page: Page,
    urls: tuple[str, ...],
    *,
    timeout_ms: int,
) -> tuple[bool, str]:
    candidates = [str(url or "").strip() for url in urls if str(url or "").strip()]
    if not candidates:
        return False, "no_url_candidates"
    wait_modes = ("domcontentloaded", "commit")
    last_error = ""
    for url in candidates:
        for wait_mode in wait_modes:
            try:
                await page.goto(url, wait_until=wait_mode, timeout=timeout_ms)
                return True, ""
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if "ERR_HTTP_RESPONSE_CODE_FAILURE" in str(exc):
                    continue
    return False, last_error or "navigation_failed"


def _overnight_enabled() -> bool:
    return os.getenv("IG_OVERNIGHT", "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_stat_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def _session_log_root(profile_root: Optional[Union[str, Path]]) -> Path:
    override = os.environ.get("APP_DATA_ROOT")
    if override:
        return Path(override)
    base = Path(profile_root or BASE_PROFILES)
    if base.name == "profiles":
        return base.parent
    return base


def _session_log_path(profile_root: Optional[Union[str, Path]]) -> Path:
    root = _session_log_root(profile_root)
    return logs_root(root) / "session_debug.log"


def _session_log(profile_root: Optional[Union[str, Path]], message: str) -> None:
    try:
        path = _session_log_path(profile_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with path.open("a", encoding="utf-8", errors="ignore") as handle:
            handle.write(f"{timestamp} {message}\n")
    except Exception:
        return


def _owner_debug_enabled() -> bool:
    return os.getenv("OWNER_DEBUG", "").strip() == "1"


def _debug_log(message: str, *args: object) -> None:
    if _owner_debug_enabled():
        logger.info(message, *args)


def _storage_state_path(username: str, profile_root: Optional[Union[str, Path]] = None) -> Path:
    return browser_storage_state_path(
        username,
        profiles_root=profile_root or BASE_PROFILES,
        filename=STORAGE_FILENAME,
    )


def _login_playwright_service(
    *,
    headless: bool,
    profile_root: Path,
    browser_mode: str = PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
<<<<<<< HEAD
    subsystem: str = "auth",
=======
>>>>>>> origin/main
) -> PlaywrightService:
    return PlaywrightService(
        headless=headless,
        base_profiles=profile_root,
        prefer_persistent=True,
        browser_mode=browser_mode,
<<<<<<< HEAD
        subsystem=subsystem,
=======
>>>>>>> origin/main
    )


def _has_persistent_profile_state(profile_dir: Path) -> bool:
    try:
        if not profile_dir.exists() or not profile_dir.is_dir():
            return False
    except Exception:
        return False

    storage_state = profile_dir / STORAGE_FILENAME
    try:
<<<<<<< HEAD
        has_storage_state = bool(storage_state.exists())
    except Exception:
        return False

    if _profile_has_chrome_state(profile_dir):
        return True
    if not has_storage_state:
        return False

    # Storage_state without a Chrome profile root is not sufficient for
    # a persistent-profile reuse.
    return False


def _profile_has_chrome_state(profile_dir: Path) -> bool:
    try:
        if not profile_dir.exists() or not profile_dir.is_dir():
            return False
    except Exception:
        return False

    default_dir = profile_dir / "Default"
    if default_dir.exists():
        return True
    if (profile_dir / "Local State").exists():
        return True
    if (profile_dir / "Preferences").exists():
        return True
    if (default_dir / "Preferences").exists():
        return True
    if (default_dir / "Network" / "Cookies").exists():
        return True
    return False


def _is_chrome_profile_picker_url(url: str) -> bool:
    normalized = (url or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("chrome://profile-picker") or "profile-picker" in normalized


async def _is_chrome_profile_picker(page: Page, ctx: Optional[BrowserContext] = None) -> bool:
    pages: list[Page] = []
    if ctx is not None:
        try:
            pages.extend(list(ctx.pages))
        except Exception:
            pages = []
    if page not in pages:
        pages.insert(0, page)

    for candidate in pages:
        try:
            if _is_chrome_profile_picker_url(candidate.url):
                return True
        except Exception:
            continue
    return False
=======
        if storage_state.exists():
            return True
    except Exception:
        return False

    try:
        next(profile_dir.iterdir())
    except StopIteration:
        return False
    except Exception:
        return False
    return True
>>>>>>> origin/main


def _is_challenge_url(url: str) -> bool:
    normalized = (url or "").lower()
    return any(part in normalized for part in _CHALLENGE_URL_PARTS)


def _is_email_challenge_url(url: str) -> bool:
    normalized = (url or "").lower()
    return any(part in normalized for part in _EMAIL_CHALLENGE_URL_PARTS)


async def _has_email_challenge_text(page: Page) -> bool:
    for pattern in _EMAIL_CHALLENGE_TEXT_PATTERNS:
        try:
            locator = page.get_by_text(pattern, exact=False)
            if await locator.count():
                _debug_log("Email challenge text matched: %s", pattern.pattern)
                return True
        except Exception:
            continue
    return False


async def _is_email_challenge(page: Page) -> bool:
    current_url = page.url or ""
    if _is_email_challenge_url(current_url):
        _debug_log("Email challenge URL detected: %s", current_url)
        return True
    return await _has_email_challenge_text(page)


async def _await_manual_email_challenge(page: Page, username: str, *, headless: bool = False) -> bool:
    if headless:
        logger.warning(
            "Headless activo: se omite prompt manual de verificacion por email para @%s.",
            username,
        )
        return False
    if _overnight_enabled():
        logger.warning("Overnight activo: se omite prompt manual para @%s.", username)
        return False
    prompt = (
        "Instagram requiere verificación por email.\n"
        "1) Elegí \"Send email\" en el navegador\n"
        "2) Revisá tu correo y pegá el código en Instagram\n"
        "3) NO cierres el navegador\n"
        "Presioná ENTER cuando termines"
    )
    first = True
    while True:
        if first:
            print(prompt)
            first = False
        else:
            print("Todavía falta completar la verificación por email.")
            print("Presioná ENTER para reintentar o escribí 'q' para abortar.")
        try:
            answer = input().strip().lower()
        except (EOFError, KeyboardInterrupt):
            return False
        if answer in {"q", "quit", "salir", "abort"}:
            return False
        try:
            await page.wait_for_timeout(1000)
        except Exception:
            pass
        if await is_logged_in(page):
            return True
        if not await _is_email_challenge(page):
            return False


async def ensure_logged_in_async(
    account: dict,
    headless: bool = False,
    profile_root: Optional[Union[str, Path]] = None,
    proxy: Optional[dict] = None,
) -> Tuple[PlaywrightService, BrowserContext, Page]:
    """
    Asegura una sesión humana persistente por cuenta. Retorna
    (PlaywrightService, BrowserContext, Page) listos para usar.
    """
    username = account.get("username")
    if not username:
        raise ValueError("account debe incluir 'username'.")
    password = account.get("password")
    logger.info("Engine=playwright_async login account=@%s", username)

    trace = account.get("trace")

    def _trace_msg(message: str) -> None:
        if callable(trace):
            try:
                trace(message)
            except Exception:
                pass

    derived_profile_root = profile_root
    derived_proxy = proxy

    # Compatibilidad con la firma anterior: ensure_logged_in(account, headless, proxy)
    if derived_proxy is None and isinstance(derived_profile_root, dict):
        derived_proxy = derived_profile_root  # type: ignore[assignment]
        derived_profile_root = None

    profile_root_path = Path(derived_profile_root or BASE_PROFILES)
<<<<<<< HEAD
    account_profile = browser_profile_dir(username, profiles_root=profile_root_path)
    storage_state = _storage_state_path(username, profile_root_path)
=======
    storage_state = _storage_state_path(username, profile_root_path)
    account_profile = storage_state.parent
>>>>>>> origin/main
    if derived_proxy is not None:
        proxy_payload = normalize_playwright_proxy(
            derived_proxy,
            proxy_user=account.get("proxy_user"),
            proxy_pass=account.get("proxy_pass"),
        )
    else:
        proxy_payload = proxy_from_account(account)
    force_login = bool(
        account.get("force_login")
        or account.get("force_relogin")
        or account.get("relogin")
    )
    reuse_session_only = bool(
        account.get("reuse_session_only")
        or account.get("reuse_existing_session")
        or account.get("skip_session_validation")
    )
    validate_reused_session = bool(
        account.get("validate_reused_session")
        or account.get("require_valid_session")
        or account.get("require_logged_in_session")
    )
    strict_visible_browser = bool(
        account.get("manual_visible_browser")
        or account.get("disable_safe_browser_recovery")
    )
<<<<<<< HEAD
    require_persistent_profile = bool(
        account.get("require_persistent_profile")
        or str(account.get("_playwright_subsystem") or "").strip().lower() == "campaign"
    )
    if require_persistent_profile:
        strict_visible_browser = True
=======
>>>>>>> origin/main
    strict_login = bool(account.get("strict_login"))
    browser_mode = str(account.get("playwright_browser_mode") or "").strip().lower()
    if browser_mode not in {
        PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        PLAYWRIGHT_BROWSER_MODE_MANAGED,
    }:
        browser_mode = PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
    if strict_login:
        # Flujos de login/relogin explícito no deben gastar tiempo en probe de
        # sesión previa; se fuerza una corrida limpia.
        force_login = True
        reuse_session_only = False
    init_timeout_seconds = _accounts_login_init_timeout_seconds(headless=headless)
    loop = asyncio.get_running_loop()
    init_started_mono = loop.time()

    def _remaining_timeout() -> float:
        elapsed = loop.time() - init_started_mono
        remaining = init_timeout_seconds - elapsed
        if remaining <= 0:
            raise asyncio.TimeoutError("init_timeout_global")
        return max(0.1, remaining)

    _session_log(profile_root_path, f"login_start username={username} headless={headless}")

    _trace_msg(f"Launch browser ({'headful' if not headless else 'headless'})")
<<<<<<< HEAD
    service_subsystem = str(account.get("_playwright_subsystem") or account.get("playwright_subsystem") or "auth").strip()
=======
>>>>>>> origin/main
    svc = _login_playwright_service(
        headless=headless,
        profile_root=profile_root_path,
        browser_mode=browser_mode,
<<<<<<< HEAD
        subsystem=service_subsystem,
    )
    campaign_timezone_resolution: CampaignBrowserTimezoneResolution | None = None
    if service_subsystem.lower() == "campaign":
        try:
            campaign_timezone_resolution = resolve_campaign_browser_timezone(account)
        except CampaignTimezoneResolutionError as exc:
            log_browser_stage(
                component="campaign_timezone_policy",
                stage="timezone_resolved",
                status="failed",
                account=username,
                has_proxy=exc.has_proxy,
                proxy_id=exc.proxy_id,
                proxy_label=exc.proxy_label,
                browser_timezone_source=exc.browser_timezone_source,
                browser_timezone_id="",
                business_timezone_id=exc.business_timezone_id,
                reason_code=exc.reason_code,
            )
            raise
        log_browser_stage(
            component="campaign_timezone_policy",
            stage="timezone_resolved",
            status="ok",
            account=username,
            has_proxy=campaign_timezone_resolution.has_proxy,
            proxy_id=campaign_timezone_resolution.proxy_id,
            proxy_label=campaign_timezone_resolution.proxy_label,
            browser_timezone_source=campaign_timezone_resolution.browser_timezone_source,
            browser_timezone_id=campaign_timezone_resolution.timezone_id,
            business_timezone_id=campaign_timezone_resolution.business_timezone_id,
        )

    def _record_login_diagnostic(
        *,
        event_type: str,
        stage: str,
        outcome: str,
        reason: str = "",
        reason_code: str = "",
        exception: BaseException | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        record_inbox_diagnostic(
            account,
            event_type=event_type,
            stage=stage,
            outcome=outcome,
            account_id=str(username or "").strip().lstrip("@").lower(),
            alias_id=str(account.get("alias") or "").strip(),
            thread_key=str(account.get("_inbox_diagnostic_thread_key") or "").strip(),
            job_type=str(account.get("_inbox_diagnostic_job_type") or "").strip().lower(),
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload={
                "headless": bool(headless),
                "browser_mode": browser_mode,
                "proxy_present": bool(proxy_payload),
                "storage_state_present": storage_state.exists(),
                "storage_state_path": str(storage_state),
                "profile_dir": str(account_profile),
                **dict(payload or {}),
            },
            callsite_skip=2,
        )
=======
    )
>>>>>>> origin/main

    async def _new_context(use_storage: bool, *, safe_mode: bool = False) -> tuple[BrowserContext, Page]:
        context_kwargs = {
            "profile_dir": account_profile,
            "storage_state": str(storage_state) if use_storage and storage_state.exists() else None,
            "proxy": None if safe_mode else proxy_payload,
            "safe_mode": safe_mode,
        }
<<<<<<< HEAD
        if campaign_timezone_resolution is not None:
            context_kwargs["timezone_id"] = campaign_timezone_resolution.timezone_id
        campaign_desktop_layout = account.get("campaign_desktop_layout")
        if campaign_desktop_layout is not None:
            context_kwargs["campaign_desktop_layout"] = campaign_desktop_layout
=======
>>>>>>> origin/main
        visible_browser_layout = account.get("visible_browser_layout")
        if visible_browser_layout is not None:
            context_kwargs["visible_browser_layout"] = visible_browser_layout
        ctx = await svc.new_context_for_account(**context_kwargs)
        try:
            page = await get_page(ctx)
        except Exception:
            try:
                await ctx.close()
            except Exception:
                pass
            raise
<<<<<<< HEAD
        if await _is_chrome_profile_picker(page, ctx):
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="browser_launch",
                outcome="fail",
                reason="chrome_profile_picker",
                reason_code="chrome_profile_picker",
                payload={"page_url": str(page.url or "")},
            )
            try:
                await ctx.close()
            except Exception:
                pass
            raise RuntimeError(f"chrome_profile_picker:{username}")
=======
>>>>>>> origin/main
        nav_timeout = _bootstrap_nav_timeout_ms()
        try:
            page.set_default_timeout(nav_timeout)
            page.set_default_navigation_timeout(nav_timeout)
        except Exception:
            pass
        return ctx, page

    async def _new_context_with_recovery(use_storage: bool) -> tuple[BrowserContext, Page]:
        nonlocal svc
<<<<<<< HEAD
        _record_login_diagnostic(
            event_type="browser_launch_started",
            stage="browser_launch",
            outcome="attempt",
            reason="browser_launch_started",
            reason_code="browser_launch_started",
            payload={"use_storage": bool(use_storage), "safe_mode": False},
        )
=======
>>>>>>> origin/main
        try:
            context_timeout = _remaining_timeout()
            return await asyncio.wait_for(
                _new_context(use_storage, safe_mode=False),
                timeout=context_timeout,
            )
<<<<<<< HEAD
        except PersistentProfileOwnershipError as exc:
            _record_login_diagnostic(
                event_type="browser_launch_failed",
                stage="browser_launch",
                outcome="fail",
                reason=exc.conflict_code,
                reason_code=exc.reason_code,
                exception=exc,
                payload=exc.to_payload(),
            )
            raise
        except Exception as exc:
            _record_login_diagnostic(
                event_type="browser_launch_failed",
                stage="browser_launch",
                outcome="fail",
                exception=exc,
                payload={"use_storage": bool(use_storage), "safe_mode": False},
            )
            if (strict_visible_browser or require_persistent_profile) and is_driver_crash_error(exc):
=======
        except Exception as exc:
            if strict_visible_browser and is_driver_crash_error(exc):
>>>>>>> origin/main
                try:
                    await svc.record_diagnostic_failure(
                        code="driver_crash_no_safe_recovery",
                        error=exc,
                        extra={"username": username, "stage": "new_context"},
                    )
                except Exception:
                    pass
                raise
<<<<<<< HEAD
            if require_persistent_profile and not is_driver_crash_error(exc):
                raise
=======
>>>>>>> origin/main
            if not is_driver_crash_error(exc):
                raise
            try:
                await svc.record_diagnostic_failure(
                    code="driver_crash_new_page_retry",
                    error=exc,
                    extra={"username": username, "stage": "new_context"},
                )
            except Exception:
                pass
            try:
                await svc.close()
            except Exception:
                pass
            svc = _login_playwright_service(
                headless=True,
                profile_root=profile_root_path,
                browser_mode=browser_mode,
<<<<<<< HEAD
                subsystem=service_subsystem,
            )
            _record_login_diagnostic(
                event_type="browser_launch_started",
                stage="browser_launch",
                outcome="retry",
                reason="browser_launch_retry_safe_mode",
                reason_code="browser_launch_retry_safe_mode",
                payload={"use_storage": bool(use_storage), "safe_mode": True},
=======
>>>>>>> origin/main
            )
            try:
                safe_context_timeout = _remaining_timeout()
                return await asyncio.wait_for(
                    _new_context(use_storage, safe_mode=True),
                    timeout=safe_context_timeout,
                )
            except Exception as safe_exc:
<<<<<<< HEAD
                _record_login_diagnostic(
                    event_type="browser_launch_failed",
                    stage="browser_launch",
                    outcome="fail",
                    exception=safe_exc,
                    payload={"use_storage": bool(use_storage), "safe_mode": True},
                )
=======
>>>>>>> origin/main
                if is_driver_crash_error(safe_exc):
                    try:
                        await svc.record_diagnostic_failure(
                            code="driver_crash_safe_retry_failed",
                            error=safe_exc,
                            extra={"username": username, "stage": "new_context_safe"},
                        )
                    except Exception:
                        pass
                    raise RuntimeError(f"PW-CTX-FAILED: {safe_exc}") from safe_exc
                raise

    ctx: Optional[BrowserContext] = None
    page: Optional[Page] = None

    async def _update_account_health_best_effort() -> None:
        # Health is Playwright-only; never use API checks here. Best-effort only.
        if page is None:
            return
        try:
            from src.health_playwright import detect_account_health_async

            import health_store
        except Exception:
            return
        try:
            status, reason = await detect_account_health_async(page)
            health_store.update_from_playwright_status(username, status, reason=reason)
        except Exception:
            return

    try:
        if reuse_session_only:
<<<<<<< HEAD
            profile_exists = False
            try:
                profile_exists = account_profile.exists() and account_profile.is_dir()
            except Exception:
                profile_exists = False
            if not _has_persistent_profile_state(account_profile):
                reason_code = "persistent_profile_missing" if not profile_exists else "persistent_profile_invalid"
                diagnostic_code = "storage_state_missing" if not profile_exists else "profile_invalid"
                _record_login_diagnostic(
                    event_type="persistent_login_failed",
                    stage="proxy_session_bootstrap",
                    outcome="fail",
                    reason=f"{reason_code}:{username}",
                    reason_code=diagnostic_code,
                )
                raise RuntimeError(f"{reason_code}:{username}")
=======
            if not _has_persistent_profile_state(account_profile):
                raise RuntimeError(f"persistent_profile_missing:{username}")
>>>>>>> origin/main
            ctx, page = await _new_context_with_recovery(use_storage=storage_state.exists())
            _session_log(
                profile_root_path,
                (
                    f"session_reuse_only username={username} "
                    f"profile={account_profile} proxy={bool(proxy_payload)}"
                ),
            )
            if validate_reused_session:
                remaining_timeout_ms = int(
                    max(1_000, min(_bootstrap_nav_timeout_ms(), _remaining_timeout() * 1000.0))
                )
                await _load_home(page, timeout_ms=remaining_timeout_ms)
                if _has_chrome_error_url(page):
<<<<<<< HEAD
                    _record_login_diagnostic(
                        event_type="persistent_login_failed",
                        stage="session_validation",
                        outcome="fail",
                        reason="session_invalid:chrome_error_page",
                        reason_code="storage_state_invalid",
                    )
=======
>>>>>>> origin/main
                    await _update_account_health_best_effort()
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                    ctx = None
                    page = None
                    try:
                        await svc.close()
                    except Exception:
                        pass
                    raise RuntimeError(f"session_invalid:{username}:chrome_error_page")
                session_check_timeout = _remaining_timeout()
                ok, reason = await asyncio.wait_for(check_logged_in(page), timeout=session_check_timeout)
                if not ok:
<<<<<<< HEAD
                    _record_login_diagnostic(
                        event_type="persistent_login_failed",
                        stage="session_validation",
                        outcome="fail",
                        reason=f"session_invalid:{reason}",
                        reason_code="storage_state_invalid",
                        payload={"page_url": str(page.url or "")},
                    )
=======
>>>>>>> origin/main
                    _session_log(
                        profile_root_path,
                        f"session_reuse_validation_fail username={username} reason={reason} url={page.url}",
                    )
                    await _update_account_health_best_effort()
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                    ctx = None
                    page = None
                    try:
                        await svc.close()
                    except Exception:
                        pass
                    raise RuntimeError(f"session_invalid:{username}:{reason}")
                _session_log(
                    profile_root_path,
                    f"session_reuse_validation_ok username={username} reason={reason} url={page.url}",
                )
                logger.info("Sesion persistente validada y reutilizada para @%s", username)
                await _update_account_health_best_effort()
                return svc, ctx, page
            logger.info("Sesion persistente reusada para @%s sin verificacion previa.", username)
            return svc, ctx, page

        if storage_state.exists() and not force_login:
            _session_log(
                profile_root_path,
                f"session_loaded path={storage_state} size={_safe_stat_size(storage_state)}",
            )
            ctx, page = await _new_context_with_recovery(use_storage=True)
            remaining_timeout_ms = int(max(1_000, min(_bootstrap_nav_timeout_ms(), _remaining_timeout() * 1000.0)))
            await _load_home(page, timeout_ms=remaining_timeout_ms)
            if _has_chrome_error_url(page):
<<<<<<< HEAD
                _record_login_diagnostic(
                    event_type="persistent_login_failed",
                    stage="session_validation",
                    outcome="fail",
                    reason="instagram_navigation_failed:chrome_error_page",
                    reason_code="browser_launch_failed",
                )
=======
>>>>>>> origin/main
                raise RuntimeError("instagram_navigation_failed:chrome_error_page")
            session_check_timeout = _remaining_timeout()
            ok, reason = await asyncio.wait_for(check_logged_in(page), timeout=session_check_timeout)
            if ok:
                _session_log(profile_root_path, f"session_check_ok reason={reason}")
                logger.info("Sesi?n existente reutilizada para @%s", username)
                await _update_account_health_best_effort()
                return svc, ctx, page
            _session_log(
                profile_root_path,
                f"session_check_fail stage=load reason={reason} url={page.url}",
            )
<<<<<<< HEAD
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="session_validation",
                outcome="fail",
                reason=f"session_invalid:{reason}",
                reason_code="storage_state_invalid",
                payload={"page_url": str(page.url or "")},
            )
=======
>>>>>>> origin/main
            await _update_account_health_best_effort()
            logger.info("storage_state inv?lido para @%s. Se intentar? nuevo login.", username)
            try:
                await ctx.close()
            except Exception:
                pass
            ctx = None
            page = None
    
        if not password:
<<<<<<< HEAD
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="login",
                outcome="fail",
                reason="password_missing",
                reason_code="password_missing",
            )
=======
>>>>>>> origin/main
            await svc.close()
            raise RuntimeError("Se requiere password para iniciar sesión por primera vez")
    
        ctx, page = await _new_context_with_recovery(use_storage=False)
        # Para login forzado/no-storage, ir directo al formulario reduce latencia
        # y evita agotar el timeout global en probes redundantes.
        remaining_timeout_ms = int(max(1_000, min(_bootstrap_nav_timeout_ms(), _remaining_timeout() * 1000.0)))
        nav_ok, nav_err = await _safe_nav_candidates(
            page,
            ("https://www.instagram.com/accounts/login/", "https://www.instagram.com/"),
            timeout_ms=remaining_timeout_ms,
        )
        if not nav_ok:
<<<<<<< HEAD
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="login",
                outcome="fail",
                reason=f"instagram_navigation_failed:{nav_err}",
                reason_code="browser_launch_failed",
            )
            raise RuntimeError(f"instagram_navigation_failed:{nav_err}")
        if _has_chrome_error_url(page):
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="login",
                outcome="fail",
                reason="instagram_navigation_failed:chrome_error_page",
                reason_code="browser_launch_failed",
            )
=======
            raise RuntimeError(f"instagram_navigation_failed:{nav_err}")
        if _has_chrome_error_url(page):
>>>>>>> origin/main
            raise RuntimeError("instagram_navigation_failed:chrome_error_page")
        # En algunos perfiles Instagram redirige directo al home/feed aunque se pida
        # /accounts/login. Si ya estamos autenticados, tratamos como exito.
        pre_login_ok, pre_login_reason = await check_logged_in(page)
        if pre_login_ok:
            _session_log(profile_root_path, f"session_check_ok reason={pre_login_reason}")
            _session_log(profile_root_path, f"login_success_condition_met condition={pre_login_reason}")
            await svc.save_storage_state(ctx, str(storage_state))
            _session_log(
                profile_root_path,
                f"storage_state_saved path={storage_state} size={_safe_stat_size(storage_state)}",
            )
            logger.info("Sesion ya activa para @%s. Se reutiliza login existente.", username)
            await _update_account_health_best_effort()
            return svc, ctx, page
        ensure_view_timeout = _remaining_timeout()
        await asyncio.wait_for(_ensure_login_view(page), timeout=ensure_view_timeout)
    
        logger.info("No se encontró sesión activa para @%s. Iniciando login humano.", username)
        try:
            code_provider = (
                account.get("challenge_code_provider")
                or account.get("challenge_code_callback")
                or account.get("code_provider")
            )
            login_timeout = _remaining_timeout()
            login_ok = await asyncio.wait_for(
                human_login(
                    page,
                    username,
                    password,
                    totp_secret=account.get("totp_secret"),
                    totp_provider=account.get("totp_callback"),
                    code_provider=code_provider,
                    trace=trace if callable(trace) else None,
                    retry_on_still_login=not strict_login,
                ),
                timeout=login_timeout,
            )
        except Exception as exc:
<<<<<<< HEAD
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="login",
                outcome="fail",
                exception=exc,
            )
=======
>>>>>>> origin/main
            # Si el intento de login lanza error pero ya estamos autenticados,
            # evitamos falso negativo y persistimos storage_state.
            try:
                fallback_ok, fallback_reason = await check_logged_in(page)
            except Exception:
                fallback_ok, fallback_reason = False, "exception"
            if fallback_ok:
                _session_log(profile_root_path, f"session_check_ok reason={fallback_reason}")
                _session_log(profile_root_path, f"login_success_condition_met condition={fallback_reason}")
                await svc.save_storage_state(ctx, str(storage_state))
                _session_log(
                    profile_root_path,
                    f"storage_state_saved path={storage_state} size={_safe_stat_size(storage_state)}",
                )
                logger.info(
                    "Sesion confirmada tras error de login para @%s. Se marca exito (reason=%s).",
                    username,
                    fallback_reason,
                )
                await _update_account_health_best_effort()
                return svc, ctx, page
            await _update_account_health_best_effort()
            raise await _raise_login_failure(page, username, profile_root_path, exc) from exc
    
        if login_ok:
            ok, reason = await check_logged_in(page)
            if ok:
                _session_log(profile_root_path, f"session_check_ok reason={reason}")
                _session_log(profile_root_path, f"login_success_condition_met condition={reason}")
                await svc.save_storage_state(ctx, str(storage_state))
                _session_log(
                    profile_root_path,
                    f"storage_state_saved path={storage_state} size={_safe_stat_size(storage_state)}",
                )
                logger.info("Login exitoso para @%s. storage_state guardado en %s", username, storage_state)
                await _update_account_health_best_effort()
                return svc, ctx, page
            _session_log(
                profile_root_path,
                f"session_check_fail stage=post_login reason={reason} url={page.url}",
            )
            await _update_account_health_best_effort()

        if await _is_email_challenge(page):
            resolved = await _await_manual_email_challenge(page, username, headless=headless)
            if resolved and await is_logged_in(page):
                await svc.save_storage_state(ctx, str(storage_state))
                _session_log(
                    profile_root_path,
                    f"storage_state_saved path={storage_state} size={_safe_stat_size(storage_state)}",
                )
                logger.info("Login confirmado tras verificacion manual para @%s", username)
                await _update_account_health_best_effort()
                return svc, ctx, page
            logger.warning(
                "Verificacion por email pendiente para @%s. Dejando navegador abierto.",
                username,
            )
            await _update_account_health_best_effort()
<<<<<<< HEAD
            _record_login_diagnostic(
                event_type="persistent_login_failed",
                stage="login",
                outcome="fail",
                reason="challenge_required",
                reason_code="challenge_required",
                payload={"page_url": str(page.url or "")},
            )
=======
>>>>>>> origin/main
            raise ChallengeRequired("challenge_required")

        # Si quedó en captcha/suspensión o en two_factor, dejar el navegador abierto
        # para intervención manual en lugar de cerrarlo.
        current_url = page.url or ""
        if _is_challenge_url(current_url):
            logger.warning(
                "Login incompleto para @%s (URL: %s). Dejando navegador abierto para resolver manualmente.",
                username,
                current_url,
            )
            await _update_account_health_best_effort()
            return svc, ctx, page

        await _update_account_health_best_effort()

        raise await _raise_login_failure(page, username, profile_root_path)
<<<<<<< HEAD
    except Exception as exc:
        _record_login_diagnostic(
            event_type="persistent_login_failed",
            stage="persistent_login",
            outcome="fail",
            exception=exc,
        )
        if ctx is not None:
            try:
                await ctx.close()
            except Exception:
                pass
        try:
            await svc.close()
        except Exception:
            pass
        raise
=======
>>>>>>> origin/main
    except BaseException:
        if ctx is not None:
            try:
                await ctx.close()
            except Exception:
                pass
        try:
            await svc.close()
        except Exception:
            pass
        raise


def ensure_logged_in(
    account: dict,
    headless: bool = False,
    profile_root: Optional[Union[str, Path]] = None,
    proxy: Optional[dict] = None,
) -> Tuple[PlaywrightService, BrowserContext, Page]:
    """
    Wrapper sync para ensure_logged_in_async. Evita usar Playwright sync API.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_sync(
            ensure_logged_in_async(
                account,
                headless=headless,
                profile_root=profile_root,
                proxy=proxy,
            )
        )
    return ensure_logged_in_async(
        account,
        headless=headless,
        profile_root=profile_root,
        proxy=proxy,
    )


async def check_session_async(
    username: str,
    *,
    profile_root: Optional[Union[str, Path]] = None,
    proxy: Optional[dict] = None,
    headless: bool = True,
) -> tuple[bool, str]:
    if not username:
        raise ValueError("username requerido para verificar sesion.")

    profile_root_path = Path(profile_root or BASE_PROFILES)
    storage_state = _storage_state_path(username, profile_root_path)
    if not storage_state.exists():
        _session_log(
            profile_root_path,
            f"session_check_fail stage=missing username={username} path={storage_state}",
        )
        with contextlib.suppress(Exception):
            import health_store

            health_store.mark_session_expired(username, reason="storage_state_missing")
        return False, "storage_state_missing"

    _session_log(profile_root_path, f"session_check_start username={username} headless={headless}")

<<<<<<< HEAD
    svc = _login_playwright_service(headless=headless, profile_root=profile_root_path, subsystem="auth")
=======
    svc = _login_playwright_service(headless=headless, profile_root=profile_root_path)
>>>>>>> origin/main
    svc_proxy = normalize_playwright_proxy(proxy)
    ctx: Optional[BrowserContext] = None
    page: Optional[Page] = None
    try:
        try:
            ctx = await svc.new_context_for_account(
                profile_dir=storage_state.parent,
                storage_state=str(storage_state),
                proxy=svc_proxy,
            )
            page = await get_page(ctx)
        except Exception as exc:
            if not is_driver_crash_error(exc):
                raise
            try:
                await svc.record_diagnostic_failure(
                    code="driver_crash_check_session_no_fallback",
                    error=exc,
                    extra={"username": username, "stage": "check_session"},
                )
            except Exception:
                pass
            raise RuntimeError(f"PW-CHECK-SESSION-FAILED: {exc}") from exc
        try:
            page.set_default_timeout(10_000)
            page.set_default_navigation_timeout(20_000)
        except Exception:
            pass
        await _load_home(page)
        if _has_chrome_error_url(page):
            _session_log(
                profile_root_path,
                f"session_check_fail stage=load username={username} reason=chrome_error_page",
            )
            return False, "chrome_error_page"
        try:
            from src.health_playwright import detect_account_health_async

            import health_store

            state, reason = await detect_account_health_async(page)
            health_store.update_from_playwright_status(username, state, reason=reason)
            ok = state == health_store.HEALTH_STATE_ALIVE
            _session_log(
                profile_root_path,
                (
                    f"session_check_{'ok' if ok else 'fail'} "
                    f"state={state} reason={reason} url={page.url}"
                ),
            )
            return ok, reason
        except Exception as probe_exc:
            _session_log(
                profile_root_path,
                f"session_check_probe_fallback username={username} error={probe_exc}",
            )
        ok, reason = await check_logged_in(page)
        with contextlib.suppress(Exception):
            import health_store

            if ok:
                health_store.mark_alive(username, reason=reason)
            else:
                health_store.mark_session_expired(username, reason=reason)
        _session_log(
            profile_root_path,
            f"session_check_{'ok' if ok else 'fail'} reason={reason} url={page.url}",
        )
        return ok, reason
    except Exception as exc:
        _session_log(
            profile_root_path,
            f"session_check_fail stage=exception username={username} error={exc}",
        )
        return False, f"exception:{exc}"
    finally:
        if ctx is not None:
            try:
                await ctx.close()
            except Exception:
                pass
        await svc.close()


def check_session(
    username: str,
    *,
    profile_root: Optional[Union[str, Path]] = None,
    proxy: Optional[dict] = None,
    headless: bool = True,
) -> tuple[bool, str]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_sync(
            check_session_async(
                username,
                profile_root=profile_root,
                proxy=proxy,
                headless=headless,
            )
        )
    return check_session_async(
        username,
        profile_root=profile_root,
        proxy=proxy,
        headless=headless,
    )


async def _load_home(page: Page, *, timeout_ms: Optional[int] = None) -> None:
    nav_timeout_ms = max(5_000, int(timeout_ms or _bootstrap_nav_timeout_ms()))
    ok, _err = await _safe_nav_candidates(
        page,
        (
            "https://www.instagram.com/",
            "https://www.instagram.com/accounts/login/",
            "https://www.instagram.com/direct/inbox/",
        ),
        timeout_ms=nav_timeout_ms,
    )
    if not ok:
        return
    
    try:
        await page.wait_for_selector(
            "a[href='/direct/inbox/'], nav[role='navigation'], textarea, input[name='username']",
            timeout=min(12_000, nav_timeout_ms),
        )
    except Exception:
        pass


async def _raise_login_failure(
    page: Page,
    username: str,
    profile_root_path: Path,
    original_exc: Optional[Exception] = None,
) -> RuntimeError:
    screenshot_path = await _capture_failure_screenshot(page, profile_root_path, username)
    errors = await get_login_errors(page)
    error_details = ", ".join(errors) if errors else "sin mensajes visibles"

    base_msg = f"Falló el login para @{username}. URL actual: {page.url}. Errores: {error_details}."
    if _has_chrome_error_url(page):
        base_msg += " Diagnostico: navegacion bloqueada (proxy/red o respuesta HTTP invalida)."
    if screenshot_path:
        base_msg += f" Screenshot: {screenshot_path}"
    if original_exc:
        base_msg += f" (Detalle: {original_exc})"
    return RuntimeError(base_msg)


async def _capture_failure_screenshot(page: Page, profile_root_path: Path, username: str) -> Optional[Path]:
    failed_dir = profile_root_path / LOGIN_FAILED_DIRNAME
    failed_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = failed_dir / f"{username}_failed.png"
    try:
        await page.screenshot(path=str(screenshot_path), full_page=True)
        return screenshot_path
    except Exception as exc:  # pragma: no cover - mejor esfuerzo
        logger.warning("No se pudo guardar screenshot de login fallido (%s): %s", username, exc)
        return None
