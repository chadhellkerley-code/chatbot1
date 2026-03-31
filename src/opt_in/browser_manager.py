"""Utilities to bootstrap an isolated Playwright browser session for opt-in flows.

This module intentionally lives under :mod:`src.opt_in` so that the existing code base
remains untouched as requested in the user specification.  The helper below keeps the
function relatively small but feature rich enough so that the higher level flows can
plug in without having to worry about environment management.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:  # pragma: no cover - import guard for optional dependency
    from playwright.async_api import Browser, BrowserContext, Page
except ImportError:  # pragma: no cover
    Browser = BrowserContext = Page = object  # type: ignore

from . import session_store
from paths import sessions_root
<<<<<<< HEAD
from src.playwright_service import get_account_fingerprint, resolve_playwright_executable
from src.runtime.playwright_runtime import PlaywrightRuntime, launch_async_browser
from src.stealth.stealth_core import patch_context
=======
from src.playwright_service import resolve_playwright_executable
from src.runtime.playwright_runtime import PlaywrightRuntime, launch_async_browser
>>>>>>> origin/main

_BASE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STORAGE_DIR = sessions_root(_BASE_ROOT) / "optin"


async def _ensure_storage_dir() -> None:
    DEFAULT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


async def launch_browser(
    account: str,
    headless: Optional[bool] = None,
    proxy_url: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> Tuple[Browser, BrowserContext, Page]:
    """Launch a Playwright browser suitable for the opt-in Instagram automation flows.

    Parameters
    ----------
    account:
        Alias of the account.  It is used to resolve the persisted storage state and
        to namespace runtime logs/contexts.
    headless:
        Whether Playwright should run in headless mode.  If ``None`` the value of the
        ``OPTIN_HEADLESS`` environment variable is inspected (defaults to ``False``).
    proxy_url:
        Optional proxy URL that should be used for the browser context.  When this is
        ``None`` the function attempts to use ``OPTIN_PROXY_URL`` as a fallback.
    user_agent:
        Optional user agent string.  When omitted Playwright uses its default.

    Returns
    -------
    Tuple[Browser, BrowserContext, Page]
        The created browser, context and the first page ready to be used.
    """

    if not account:
        raise ValueError("account alias is required to bootstrap the opt-in browser")

    if headless is None:
        headless = os.getenv("OPTIN_HEADLESS", "false").lower() == "true"

    resolved_proxy = proxy_url or os.getenv("OPTIN_PROXY_URL") or None

    await _ensure_storage_dir()

    runtime = PlaywrightRuntime(headless=headless)
    await runtime.start(launch_browser=False, force_headless=headless)
    playwright_cm = runtime.playwright
    if playwright_cm is None:
        raise RuntimeError("No se pudo inicializar Playwright runtime para opt-in.")
    try:
        executable = resolve_playwright_executable(headless=headless)
        browser = await launch_async_browser(
            playwright_cm,
            headless=headless,
            executable_path=executable,
            visible_reason=f"opt_in:{account}",
        )
    except Exception:
        raise

    storage_state = await session_store.load_storage_state_dict(account)
    context_kwargs: Dict[str, Any] = {}
    if storage_state:
        context_kwargs["storage_state"] = storage_state
    if resolved_proxy:
        context_kwargs["proxy"] = {"server": resolved_proxy}
    if user_agent:
        context_kwargs["user_agent"] = user_agent
<<<<<<< HEAD
    context_kwargs["extra_http_headers"] = {
        "Accept-Language": str(get_account_fingerprint(account)["locale"]),
    }

    context = await browser.new_context(**context_kwargs)
    await patch_context(context, account)
=======

    context = await browser.new_context(**context_kwargs)
>>>>>>> origin/main
    page = await context.new_page()

    async def _close_all() -> None:
        with contextlib.suppress(Exception):
            await context.close()
        with contextlib.suppress(Exception):
            await browser.close()

    # Attach helper attribute so callers can gracefully shutdown without leaking.
    setattr(page, "_optin_close", _close_all)
    return browser, context, page


async def close_browser_tuple(browser: Browser, context: BrowserContext, page: Page) -> None:
    """Gracefully close objects returned by :func:`launch_browser`.

    The helper is intentionally tolerant; failures while closing the browser should
    not surface at the call site because the browser is already deemed unusable at
    that point.
    """

    with contextlib.suppress(Exception):
        await page.close()
    with contextlib.suppress(Exception):
        await context.close()
    with contextlib.suppress(Exception):
        await browser.close()
    with contextlib.suppress(Exception):
        await asyncio.sleep(0)
