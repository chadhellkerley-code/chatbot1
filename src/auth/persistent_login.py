from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Optional, Tuple, Union

from playwright.async_api import BrowserContext, Page
from src.instagram_adapter import (
    BASE_URL,
    get_login_errors,
    human_login,
    is_logged_in,
    _ensure_login_view,
)
from src.playwright_service import BASE_PROFILES, PlaywrightService, get_page

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


async def _await_manual_email_challenge(page: Page, username: str) -> bool:
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

    derived_profile_root = profile_root
    derived_proxy = proxy

    # Compatibilidad con la firma anterior: ensure_logged_in(account, headless, proxy)
    if derived_proxy is None and isinstance(derived_profile_root, dict):
        derived_proxy = derived_profile_root  # type: ignore[assignment]
        derived_profile_root = None

    profile_root_path = Path(derived_profile_root or BASE_PROFILES)
    storage_state = _storage_state_path(username, profile_root_path)
    account_profile = storage_state.parent
    proxy_payload = derived_proxy or account.get("proxy")

    svc = PlaywrightService(headless=headless, base_profiles=profile_root_path)
    await svc.start()

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

    if storage_state.exists():
        ctx, page = await _new_context(use_storage=True)
        await _load_home(page)
        if await is_logged_in(page):
            logger.info("Sesión existente reutilizada para @%s", username)
            return svc, ctx, page
        logger.info("storage_state inválido para @%s. Se intentará nuevo login.", username)
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
        )
    except Exception as exc:
        raise await _raise_login_failure(page, username, profile_root_path, exc) from exc

    if login_ok and await is_logged_in(page):
        await svc.save_storage_state(ctx, str(storage_state))
        logger.info("Login exitoso para @%s. storage_state guardado en %s", username, storage_state)
        return svc, ctx, page

    if await _is_email_challenge(page):
        resolved = await _await_manual_email_challenge(page, username)
        if resolved and await is_logged_in(page):
            await svc.save_storage_state(ctx, str(storage_state))
            logger.info("Login confirmado tras verificacion manual para @%s", username)
            return svc, ctx, page
        logger.warning(
            "Verificacion por email pendiente para @%s. Dejando navegador abierto.",
            username,
        )
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
        return svc, ctx, page

    raise await _raise_login_failure(page, username, profile_root_path)


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


async def _load_home(page: Page) -> None:
    # MODIFICADO: Vamos directo al INBOX, es más seguro y rápido para verificar sesión
    inbox_url = "https://www.instagram.com/direct/inbox/"
    try:
        await page.goto(inbox_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception:
        await page.goto(inbox_url)
    
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
