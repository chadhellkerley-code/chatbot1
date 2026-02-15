# -*- coding: utf-8 -*-
"""
Playwright-only account health detection.

The goal is to determine whether an account can access Instagram Inbox UI
without relying on any API calls (instagrapi).

This module is used by real Playwright operations (login, inbox, responder,
cold DM sending, filter mode) to persist health status implicitly.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from typing import Tuple


_BLOCKED_URL_RULES: tuple[tuple[str, str, str], ...] = (
    ("/accounts/login", "session_expired", "redirected_to_login"),
    ("/challenge/", "checkpoint", "challenge"),
    ("/checkpoint/", "checkpoint", "checkpoint"),
    ("two_factor", "checkpoint", "two_factor"),
    ("/accounts/confirm_email", "checkpoint", "confirm_email"),
    ("/accounts/suspended", "suspended", "suspended"),
    ("/accounts/disabled", "blocked", "disabled"),
)

_LOGIN_FORM_SELECTORS = (
    "input[name='username']",
    "input[name='password']",
    "form[action*='login']",
)

_INBOX_READY_SELECTORS = (
    "a[href='/direct/inbox/']",
    "a[href*='/direct/inbox/']",
    "a[href*='/direct/t/']",
    "div[role='navigation'] a[href*='/direct/']",
    "input[placeholder='Search']",
    "input[placeholder='Buscar']",
    "input[name='queryBox']",
    "svg[aria-label='Direct']",
    "svg[aria-label='Mensajes']",
)

_CAPTCHA_SELECTORS = (
    "iframe[src*='captcha']",
    "iframe[title*='captcha' i]",
    "[id*='captcha' i]",
    "[class*='captcha' i]",
)

_BLOCKED_TEXT_PATTERNS: tuple[tuple[re.Pattern, str, str], ...] = (
    (re.compile(r"captcha", re.I), "blocked", "captcha_text"),
    (re.compile(r"temporarily blocked|bloquead[ao] temporalmente", re.I), "blocked", "temporarily_blocked"),
    (re.compile(r"your account has been disabled|cuenta.*desactiv", re.I), "blocked", "disabled_text"),
    (re.compile(r"suspended|cuenta suspendida", re.I), "suspended", "suspended_text"),
    (re.compile(r"checkpoint|challenge required", re.I), "checkpoint", "checkpoint_text"),
)

_POPUP_DISMISS_SELECTORS = (
    'button:has-text("Not now")',
    'button:has-text("Not Now")',
    'button:has-text("Ahora no")',
    'button:has-text("Cancelar")',
    'button:has-text("No gracias")',
    'button:has-text("Más tarde")',
    'button:has-text("Remind me later")',
)


def _union(selectors: tuple[str, ...]) -> str:
    return ", ".join(selectors)


async def _dismiss_common_popups_async(page) -> None:
    for sel in _POPUP_DISMISS_SELECTORS:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                await asyncio.sleep(0.25)
        except Exception:
            continue


async def detect_account_health_async(page, *, timeout_ms: int = 6_000) -> Tuple[str, str]:
    """
    Returns (status, reason)
    status in: alive | session_expired | checkpoint | blocked | suspended | unknown
    """

    url = ""
    with contextlib.suppress(Exception):
        url = (page.url or "").lower()

    for token, status, reason in _BLOCKED_URL_RULES:
        if token in url:
            return status, reason

    # Best-effort dismiss non-blocking popups before checking UI access.
    await _dismiss_common_popups_async(page)

    # Login form visible -> session expired / not logged in.
    for sel in _LOGIN_FORM_SELECTORS:
        with contextlib.suppress(Exception):
            if await page.locator(sel).count() > 0:
                return "session_expired", "login_form"

    # Captcha indicators.
    for sel in _CAPTCHA_SELECTORS:
        with contextlib.suppress(Exception):
            if await page.locator(sel).count() > 0:
                return "blocked", "captcha"

    # Inbox ready (UI access).
    try:
        await page.wait_for_selector(_union(_INBOX_READY_SELECTORS), timeout=timeout_ms)
        return "alive", "inbox_accessible"
    except Exception:
        pass

    # Read a bit of page text to classify common blocks.
    text = ""
    with contextlib.suppress(Exception):
        body = page.locator("body").first
        if await body.count() > 0:
            text = (await body.inner_text() or "").strip()
    for pattern, status, reason in _BLOCKED_TEXT_PATTERNS:
        if pattern.search(text or ""):
            return status, reason

    # If we cannot confirm inbox, consider it blocked for health purposes.
    return "blocked", "inbox_unavailable"


def detect_account_health_sync(page, *, timeout_ms: int = 6_000) -> Tuple[str, str]:
    """
    Sync version for playwright.sync_api.Page.
    Returns (status, reason)
    """

    url = ""
    with contextlib.suppress(Exception):
        url = (page.url or "").lower()

    for token, status, reason in _BLOCKED_URL_RULES:
        if token in url:
            return status, reason

    # Dismiss popups (best-effort).
    for sel in _POPUP_DISMISS_SELECTORS:
        with contextlib.suppress(Exception):
            btn = page.locator(sel).first
            if btn.count() > 0:
                btn.click()

    for sel in _LOGIN_FORM_SELECTORS:
        with contextlib.suppress(Exception):
            if page.locator(sel).count() > 0:
                return "session_expired", "login_form"

    for sel in _CAPTCHA_SELECTORS:
        with contextlib.suppress(Exception):
            if page.locator(sel).count() > 0:
                return "blocked", "captcha"

    try:
        page.wait_for_selector(_union(_INBOX_READY_SELECTORS), timeout=timeout_ms)
        return "alive", "inbox_accessible"
    except Exception:
        pass

    text = ""
    with contextlib.suppress(Exception):
        body = page.locator("body").first
        if body.count() > 0:
            text = (body.inner_text() or "").strip()
    for pattern, status, reason in _BLOCKED_TEXT_PATTERNS:
        if pattern.search(text or ""):
            return status, reason

    return "blocked", "inbox_unavailable"

