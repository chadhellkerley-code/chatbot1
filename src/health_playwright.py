"""
Playwright-only Instagram account health detection.

The detector uses real browser interaction and returns one of the three
canonical account states:
- VIVA
- NO ACTIVA
- MUERTA
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from typing import Tuple

from health_store import (
    HEALTH_STATE_ALIVE,
    HEALTH_STATE_DEAD,
    HEALTH_STATE_INACTIVE,
)

_URL_RULES: tuple[tuple[str, str, str], ...] = (
    ("/accounts/login", HEALTH_STATE_INACTIVE, "redirected_to_login"),
    ("/accounts/onetap", HEALTH_STATE_INACTIVE, "login_redirect"),
    ("/challenge/", HEALTH_STATE_DEAD, "challenge"),
    ("/checkpoint/", HEALTH_STATE_DEAD, "checkpoint"),
    ("two_factor", HEALTH_STATE_DEAD, "two_factor"),
    ("/accounts/confirm_email", HEALTH_STATE_DEAD, "confirm_email"),
    ("/accounts/suspended", HEALTH_STATE_DEAD, "suspended"),
    ("/accounts/disabled", HEALTH_STATE_DEAD, "disabled"),
)

_LOGIN_FORM_SELECTORS = (
    "input[name='username']",
    "input[name='password']",
    "form[action*='login']",
)

_ALIVE_SELECTORS = (
    "a[href='/direct/inbox/']",
    "a[href*='/direct/inbox/']",
    "a[href*='/direct/t/']",
    "div[role='navigation'] a[href*='/direct/']",
    "div[role='navigation'] svg[aria-label='Home']",
    "div[role='navigation'] svg[aria-label='Inicio']",
    "svg[aria-label='Direct']",
    "svg[aria-label='Mensajes']",
    "svg[aria-label='Home']",
    "svg[aria-label='Inicio']",
    "input[placeholder='Search']",
    "input[placeholder='Buscar']",
    "input[name='queryBox']",
)

_CAPTCHA_SELECTORS = (
    "iframe[src*='captcha']",
    "iframe[title*='captcha' i]",
    "[id*='captcha' i]",
    "[class*='captcha' i]",
)

_DEAD_TEXT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"captcha", re.I), "captcha_text"),
    (re.compile(r"temporarily blocked|bloquead[ao] temporalmente", re.I), "temporarily_blocked"),
    (re.compile(r"your account has been disabled|cuenta.*desactiv", re.I), "disabled_text"),
    (re.compile(r"suspended|cuenta suspendida", re.I), "suspended_text"),
    (re.compile(r"checkpoint|challenge required", re.I), "checkpoint_text"),
    (
        re.compile(
            r"verification required|verificaci[oó]n requerida|confirm (it'?s|its) you|security code",
            re.I,
        ),
        "verification_required",
    ),
)

_POPUP_DISMISS_SELECTORS = (
    'button:has-text("Not now")',
    'button:has-text("Not Now")',
    'button:has-text("Ahora no")',
    'button:has-text("Cancelar")',
    'button:has-text("No gracias")',
    'button:has-text("Mas tarde")',
    'button:has-text("MAs tarde")',
    'button:has-text("Remind me later")',
)


def _union(selectors: tuple[str, ...]) -> str:
    return ", ".join(selectors)


async def _dismiss_common_popups_async(page) -> None:
    for selector in _POPUP_DISMISS_SELECTORS:
        try:
            button = page.locator(selector).first
            if await button.count() > 0:
                await button.click()
                await asyncio.sleep(0.25)
        except Exception:
            continue


def _classify_url(url: str) -> Tuple[str, str] | None:
    lowered = str(url or "").strip().lower()
    for token, state, reason in _URL_RULES:
        if token in lowered:
            return state, reason
    return None


async def _read_body_text_async(page) -> str:
    with contextlib.suppress(Exception):
        body = page.locator("body").first
        if await body.count() > 0:
            return (await body.inner_text() or "").strip()
    return ""


def _read_body_text_sync(page) -> str:
    with contextlib.suppress(Exception):
        body = page.locator("body").first
        if body.count() > 0:
            return (body.inner_text() or "").strip()
    return ""


async def _has_auth_cookies_async(page) -> bool:
    context = getattr(page, "context", None)
    cookies_method = getattr(context, "cookies", None)
    if not callable(cookies_method):
        return False
    try:
        cookies = await cookies_method(["https://www.instagram.com/"])
    except TypeError:
        cookies = await cookies_method("https://www.instagram.com/")
    except Exception:
        return False
    names = {str(cookie.get("name") or ""): str(cookie.get("value") or "") for cookie in cookies or []}
    return bool(names.get("sessionid") and names.get("ds_user_id"))


def _has_auth_cookies_sync(page) -> bool:
    context = getattr(page, "context", None)
    cookies_method = getattr(context, "cookies", None)
    if not callable(cookies_method):
        return False
    try:
        cookies = cookies_method(["https://www.instagram.com/"])
    except TypeError:
        cookies = cookies_method("https://www.instagram.com/")
    except Exception:
        return False
    names = {str(cookie.get("name") or ""): str(cookie.get("value") or "") for cookie in cookies or []}
    return bool(names.get("sessionid") and names.get("ds_user_id"))


def _classify_text(text: str) -> Tuple[str, str] | None:
    lowered = str(text or "").strip()
    if not lowered:
        return None
    for pattern, reason in _DEAD_TEXT_PATTERNS:
        if pattern.search(lowered):
            return HEALTH_STATE_DEAD, reason
    return None


async def detect_account_health_async(page, *, timeout_ms: int = 6_000) -> Tuple[str, str]:
    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    await _dismiss_common_popups_async(page)

    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    for selector in _LOGIN_FORM_SELECTORS:
        with contextlib.suppress(Exception):
            if await page.locator(selector).count() > 0:
                return HEALTH_STATE_INACTIVE, "login_form"

    for selector in _CAPTCHA_SELECTORS:
        with contextlib.suppress(Exception):
            if await page.locator(selector).count() > 0:
                return HEALTH_STATE_DEAD, "captcha"

    try:
        await page.wait_for_selector(_union(_ALIVE_SELECTORS), timeout=timeout_ms)
        return HEALTH_STATE_ALIVE, "instagram_ui_ready"
    except Exception:
        pass

    text = await _read_body_text_async(page)
    classification = _classify_text(text)
    if classification is not None:
        return classification

    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    if await _has_auth_cookies_async(page):
        return HEALTH_STATE_ALIVE, "auth_cookies_without_ui"

    if text and "instagram" in text.casefold():
        return HEALTH_STATE_ALIVE, "instagram_ui_text_ready"

    return HEALTH_STATE_INACTIVE, "login_state_unconfirmed"


def detect_account_health_sync(page, *, timeout_ms: int = 6_000) -> Tuple[str, str]:
    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    for selector in _POPUP_DISMISS_SELECTORS:
        with contextlib.suppress(Exception):
            button = page.locator(selector).first
            if button.count() > 0:
                button.click()

    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    for selector in _LOGIN_FORM_SELECTORS:
        with contextlib.suppress(Exception):
            if page.locator(selector).count() > 0:
                return HEALTH_STATE_INACTIVE, "login_form"

    for selector in _CAPTCHA_SELECTORS:
        with contextlib.suppress(Exception):
            if page.locator(selector).count() > 0:
                return HEALTH_STATE_DEAD, "captcha"

    try:
        page.wait_for_selector(_union(_ALIVE_SELECTORS), timeout=timeout_ms)
        return HEALTH_STATE_ALIVE, "instagram_ui_ready"
    except Exception:
        pass

    text = _read_body_text_sync(page)
    classification = _classify_text(text)
    if classification is not None:
        return classification

    classification = _classify_url(getattr(page, "url", ""))
    if classification is not None:
        return classification

    if _has_auth_cookies_sync(page):
        return HEALTH_STATE_ALIVE, "auth_cookies_without_ui"

    if text and "instagram" in text.casefold():
        return HEALTH_STATE_ALIVE, "instagram_ui_text_ready"

    return HEALTH_STATE_INACTIVE, "login_state_unconfirmed"
