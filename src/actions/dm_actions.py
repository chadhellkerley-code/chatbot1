from __future__ import annotations

import logging
import os
import random
import sys
from typing import Iterable, Optional

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

PWTimeoutError = PlaywrightTimeoutError

# ensure imports resolve when executing module directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.actions.direct_helpers import (
    SELECTORS_JOINED,
    ensure_inbox,
    ensure_new_message_dialog,
    search_and_select,
    wait_thread_open,
    focus_composer,
    wait_own_bubble,
    last_error_toast,
    _snap,
)
from src.auth.persistent_login import ensure_logged_in_async
from src.humanizer import human_type, human_wait, type_text, random_wait
from src.playwright_service import shutdown

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Async DM flow                                                               #
# --------------------------------------------------------------------------- #

def pick_random_message(messages: Iterable[str], username: str) -> str:
    pool = list(messages)
    if not pool:
        raise ValueError("messages pool vacío.")
    template = random.choice(pool)
    safe_username = username.strip().lstrip("@")
    return template.format(username=safe_username)


async def send_dm_to_user(
    page: Page,
    username: str,
    message: str,
    humanizer_cfg: Optional[dict] = None,
) -> dict:
    """
    Envía un mensaje directo simulando interacción humana usando Playwright async.
    """
    cfg = humanizer_cfg or {}
    target = username.strip().lstrip("@")
    result = {"ok": False, "username": target}
    if not target:
        result["error"] = "invalid_username"
        return result
    safe_user = target or "unknown"
    safe_user = safe_user.replace("/", "_")

    async def snap(reason: str) -> Optional[str]:
        return await _snap(page, f"{reason}_{safe_user}")

    try:
        inbox_ok = await ensure_inbox(page)
        if not inbox_ok:
            result["error"] = "inbox_unavailable"
            result["screenshot"] = await snap("inbox_unavailable")
            return result

        if not await ensure_new_message_dialog(page):
            result["error"] = "no_dialog"
            result["screenshot"] = await snap("no_dialog")
            return result

        picked, reason = await search_and_select(page, target, exact=True)
        if not picked:
            error_label = reason or "user_not_found"
            result["error"] = error_label
            result["screenshot"] = await snap(error_label)
            return result

        if not await wait_thread_open(page):
            result["error"] = "open_failed"
            result["screenshot"] = await snap("open_failed")
            return result

        if not await focus_composer(page):
            result["error"] = "composer_not_found"
            result["screenshot"] = await snap("composer_not_found")
            return result

        composer = page.locator(SELECTORS_JOINED["composer"]).first
        typing_defaults = {"min_delay": 0.04, "max_delay": 0.18, "occasional_pause": 0.12}
        typing_cfg = {**typing_defaults, **(cfg.get("typing", {}) or {})}
        typed_message = message.format(username=target)
        await type_text(
            composer,
            typed_message,
            min_delay=typing_cfg.get("min_delay", typing_defaults["min_delay"]),
            max_delay=typing_cfg.get("max_delay", typing_defaults["max_delay"]),
            occasional_pause=typing_cfg.get("occasional_pause", typing_defaults["occasional_pause"]),
        )
        await random_wait(200, 500)
        await page.keyboard.press("Enter")

        sent = await wait_own_bubble(page, timeout_ms=9_000)
        if not sent:
            toast = await last_error_toast(page)
            result["error"] = f"send_failed:{toast}" if toast else "send_failed"
            result["screenshot"] = await snap("send_failed")
            return result

        toast = await last_error_toast(page)
        if toast:
            result["error"] = f"send_failed:{toast}"
            result["screenshot"] = await snap("send_failed_toast")
            return result

        result.update(ok=True, sent_text=typed_message)
        logger.info("[DM] Sent to %s: %s", target, typed_message)
        return result
    except PlaywrightTimeoutError as exc:
        result["error"] = "timeout"
        result["detail"] = str(exc)
        result["screenshot"] = await snap("timeout")
        return result
    except Exception as exc:
        result["error"] = f"exception:{type(exc).__name__}:{exc}"
        result["screenshot"] = await snap("exception")
        return result


# --------------------------------------------------------------------------- #
# Compatibility helper (async)                                                #
# --------------------------------------------------------------------------- #

async def _find_message_box(page):
    composer_selector = SELECTORS_JOINED["composer"]
    locator = page.locator(composer_selector)
    if await locator.count():
        return locator
    if await page.locator("textarea").count():
        return page.locator("textarea")
    return page.locator("div[contenteditable='true']")


async def _first_present(page, selectors: list[str], timeout_each=5000):
    """Devuelve el primer locator que aparece de la lista."""
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout_each)
            return page.locator(sel)
        except PlaywrightTimeoutError:
            continue
    raise PWTimeoutError(f"Ninguno de los selectores apareció: {selectors}")


async def send_message(account: dict, to_username: str, message: str, headful: Optional[bool] = None) -> None:
    """Compat async helper still used by adapter flows."""
    headless = False if headful is None else not headful
    pw = ctx = page = None
    try:
        pw, ctx, page = await ensure_logged_in_async(account, headless=headless)
        await page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
        new_btn_candidates = [
            "a[href='/direct/new/']",
            "div[role='button']:has-text('Nuevo mensaje')",
            "div[role='button']:has-text('Mensaje nuevo')",
            "div[role='button']:has-text('Enviar mensaje')",
            "button:has-text('Nuevo mensaje')",
            "button:has-text('Mensaje nuevo')",
            "button:has-text('Enviar mensaje')",
            "svg[aria-label*='New message']",
            "svg[aria-label*='Mensaje nuevo']",
        ]
        dialog_open = await page.locator("div[role='dialog']").count() > 0
        if not dialog_open:
            try:
                new_btn = await _first_present(page, new_btn_candidates, timeout_each=4000)
                try:
                    await new_btn.click()
                except Exception:
                    await new_btn.locator("xpath=ancestor-or-self::*[self::button or self::div or self::a]").first.click()
            except PlaywrightTimeoutError:
                await page.goto("https://www.instagram.com/direct/new/", wait_until="domcontentloaded")

        await page.wait_for_selector("div[role='dialog']", timeout=15000)
        search_input_candidates = [
            "div[role='dialog'] input[name='queryBox']",
            "div[role='dialog'] input[placeholder*='Search']",
            "div[role='dialog'] input[placeholder*='Buscar']",
            "div[role='dialog'] input[aria-label*='Search']",
            "div[role='dialog'] input[aria-label*='Buscar']",
            "div[role='dialog'] input[type='text']",
        ]
        search_box = await _first_present(page, search_input_candidates, timeout_each=5000)
        await human_type(search_box, to_username)
        await human_wait(0.5, 1.2)
        await page.keyboard.press("Enter")
        await human_wait(0.4, 1.0)

        next_btn_candidates = [
            "div[role='dialog'] button:has-text('Siguiente')",
            "div[role='dialog'] button:has-text('Next')",
        ]
        next_btn = await _first_present(page, next_btn_candidates, timeout_each=6000)
        await next_btn.click()

        await page.wait_for_selector("textarea, div[contenteditable='true']", timeout=15000)
        box = await _find_message_box(page)
        await human_type(box, message)
        send_btn_candidates = [
            "button[type='submit']",
            "button:has-text('Enviar')",
            "button[aria-label*='Send']",
        ]
        try:
            await (await _first_present(page, send_btn_candidates, timeout_each=4000)).click()
        except PlaywrightTimeoutError:
            await page.keyboard.press("Enter")

        await human_wait(0.8, 1.6)
        sent = await wait_own_bubble(page, timeout_ms=9_000)
        if not sent:
            toast = await last_error_toast(page)
            raise RuntimeError(f"send_failed:{toast}" if toast else "send_failed")
        toast = await last_error_toast(page)
        if toast:
            raise RuntimeError(f"send_failed:{toast}")

    except Exception:
        try:
            os.makedirs("screenshots", exist_ok=True)
            await page.screenshot(path="screenshots/dm_debug.png", full_page=True)
        except Exception:
            pass
        raise
    finally:
        stay_open = False
        try:
            current_url = page.url if page else ""
            if current_url:
                stay_open = any(
                    token in current_url
                    for token in (
                        "accounts/suspended",
                        "two_factor",
                        "challenge",
                        "checkpoint",
                        "accounts/confirm_email",
                    )
                )
        except Exception:
            stay_open = False
        if not stay_open:
            await shutdown(pw, ctx)
