from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

from playwright.async_api import BrowserContext, Page
from src.instagram_adapter import (
    BASE_URL,
    check_logged_in,
    get_login_errors,
    human_login,
    is_logged_in,
    _ensure_login_view,
)
from src.playwright_service import BASE_PROFILES, PlaywrightService, get_page
from src.proxy_payload import normalize_playwright_proxy, proxy_from_account

logger = logging.getLogger(__name__)

LOGIN_FAILED_DIRNAME = "login_failed_screenshots"
STORAGE_FILENAME = "storage_state.json"

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
    return root / "storage" / "session_debug.log"


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
    base = Path(profile_root or BASE_PROFILES)
    return base / username / STORAGE_FILENAME


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
    storage_state = _storage_state_path(username, profile_root_path)
    account_profile = storage_state.parent
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

    _session_log(profile_root_path, f"login_start username={username} headless={headless}")

    _trace_msg(f"Launch browser ({'headful' if not headless else 'headless'})")
    svc = PlaywrightService(headless=headless, base_profiles=profile_root_path)
    await svc.start(launch_proxy=proxy_payload)

    async def _new_context(use_storage: bool) -> tuple[BrowserContext, Page]:
        ctx = await svc.new_context_for_account(
            profile_dir=account_profile,
            storage_state=str(storage_state) if use_storage and storage_state.exists() else None,
            proxy=proxy_payload,
        )
        page = await get_page(ctx)
        try:
            page.set_default_timeout(20_000)
            page.set_default_navigation_timeout(45_000)
        except Exception:
            pass
        return ctx, page

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
        if storage_state.exists() and not force_login:
            _session_log(
                profile_root_path,
                f"session_loaded path={storage_state} size={_safe_stat_size(storage_state)}",
            )
            ctx, page = await _new_context(use_storage=True)
            await _load_home(page)
            ok, reason = await check_logged_in(page)
            if ok:
                _session_log(profile_root_path, f"session_check_ok reason={reason}")
                logger.info("Sesi?n existente reutilizada para @%s", username)
                await _update_account_health_best_effort()
                return svc, ctx, page
            _session_log(
                profile_root_path,
                f"session_check_fail stage=load reason={reason} url={page.url}",
            )
            await _update_account_health_best_effort()
            logger.info("storage_state inv?lido para @%s. Se intentar? nuevo login.", username)
            try:
                await ctx.close()
            except Exception:
                pass
            ctx = None
            page = None
    
        if not password:
            await svc.close()
            raise RuntimeError("Se requiere password para iniciar sesión por primera vez")
    
        ctx, page = await _new_context(use_storage=False)
        await _load_home(page)
        await _ensure_login_view(page)
    
        logger.info("No se encontró sesión activa para @%s. Iniciando login humano.", username)
        try:
            code_provider = (
                account.get("challenge_code_provider")
                or account.get("challenge_code_callback")
                or account.get("code_provider")
            )
            login_ok = await human_login(
                page,
                username,
                password,
                totp_secret=account.get("totp_secret"),
                totp_provider=account.get("totp_callback"),
                code_provider=code_provider,
                trace=trace if callable(trace) else None,
                retry_on_still_login=not bool(account.get("strict_login")),
            )
        except Exception as exc:
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
    except Exception:
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
        return asyncio.run(
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
        return False, "storage_state_missing"

    _session_log(profile_root_path, f"session_check_start username={username} headless={headless}")

    svc = PlaywrightService(headless=headless, base_profiles=profile_root_path)
    svc_proxy = normalize_playwright_proxy(proxy)
    await svc.start(launch_proxy=svc_proxy)
    ctx: Optional[BrowserContext] = None
    page: Optional[Page] = None
    try:
        ctx = await svc.new_context_for_account(
            profile_dir=storage_state.parent,
            storage_state=str(storage_state),
            proxy=proxy,
        )
        page = await get_page(ctx)
        try:
            page.set_default_timeout(10_000)
            page.set_default_navigation_timeout(20_000)
        except Exception:
            pass
        await _load_home(page)
        ok, reason = await check_logged_in(page)
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
        return asyncio.run(
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


async def _load_home(page: Page) -> None:
    # MODIFICADO: Vamos directo al INBOX, es más seguro y rápido para verificar sesión
    inbox_url = "https://www.instagram.com/direct/inbox/"
    try:
        await page.goto(inbox_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception:
        try:
            # Segundo intento sin propagar el fallo de navegación del proxy.
            await page.goto(inbox_url, wait_until="domcontentloaded", timeout=60_000)
        except Exception:
            pass
    
    try:
        # Esperamos elementos clave del inbox
        await page.wait_for_selector("a[href='/direct/inbox/'], nav[role='navigation'], textarea", timeout=10_000)
    except Exception:
        pass  # is_logged_in hara la verificacion real


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
