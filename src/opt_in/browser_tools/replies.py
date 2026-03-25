from __future__ import annotations

from typing import Optional

from playwright.sync_api import Locator, Page

from . import audit
from .browser_manager import BrowserManager
from .config import get_settings
from .utils import random_human_delay, sample_delay, wait_first_selector


def _threads_with_unread(page: Page) -> Optional[Locator]:
    candidates = [
        page.locator("//a[contains(@aria-label, 'unread')]"),
        page.locator("//div[@role='row' and .//span[contains(@aria-label, 'unread')]]"),
        page.locator("//div[contains(@aria-label, 'unread message')]"),
    ]
    for locator in candidates:
        if locator.count() > 0:
            return locator
    return None


def _type_reply(page: Page, text: str) -> None:
    box = wait_first_selector(
        page,
        [
            "textarea[placeholder='Message...']",
            "textarea[aria-label='Message']",
        ],
    )
    box.click()
    box.fill("")
    settings = get_settings()
    for char in text:
        delay_ms = max(int(sample_delay(settings.keyboard_delay) * 1000), 20)
        box.type(char, delay=delay_ms)
    random_human_delay(settings.action_delay)
    page.keyboard.press("Enter")


def reply_unread(account: str, text: str, limit: int = 5) -> int:
    audit.log_event("replies.start", account=account, details={"limit": limit})
    handled = 0
    with BrowserManager(account_alias=account, persist_session=True) as manager:
        page = manager.ensure_page()
        page.goto("https://www.instagram.com/direct/inbox/", wait_until="networkidle")
        random_human_delay(get_settings().action_delay)

        while handled < limit:
            threads = _threads_with_unread(page)
            if not threads or threads.count() == 0:
                break
            thread = threads.nth(0)
            alias = thread.get_attribute("aria-label") or ""
            thread.click()
            random_human_delay(get_settings().action_delay)
            _type_reply(page, text)
            handled += 1
            audit.log_event("replies.sent", account=account, details={"thread": alias.strip()})

    return handled


def cli_reply_unread(account: str, text: str) -> None:
    total = reply_unread(account=account, text=text)
    print(f"[OPT-IN] Respuestas enviadas: {total}")
