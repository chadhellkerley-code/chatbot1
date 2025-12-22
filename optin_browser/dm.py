from __future__ import annotations

from typing import Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_random

from . import audit
from .browser_manager import BrowserManager
from .config import get_settings
from .utils import click_first, random_human_delay, sample_delay, wait_first_selector


def _open_conversation(page: Page, username: str) -> None:
    page.goto(f"https://www.instagram.com/{username}/", wait_until="networkidle")
    random_human_delay(get_settings().action_delay)
    click_first(page, ["button:has-text('Message')", "button:has-text('Enviar mensaje')"])
    wait_first_selector(page, ["textarea[placeholder='Message...']", "textarea[aria-label='Message']"])


def _type_message(page: Page, text: str) -> None:
    message_box = wait_first_selector(
        page,
        [
            "textarea[placeholder='Message...']",
            "textarea[aria-label='Message']",
        ],
    )
    message_box.click()
    message_box.fill("")
    settings = get_settings()
    for char in text:
        delay_ms = max(int(sample_delay(settings.keyboard_delay) * 1000), 20)
        message_box.type(char, delay=delay_ms)
    random_human_delay(get_settings().action_delay)
    page.keyboard.press("Enter")


@retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=2))
def _ensure_bubble(page: Page, text: str) -> None:
    selectors = [
        f"//div[contains(@class, 'x1n2onr6') and .//span[text()='{text}']]",
        f"//div[contains(@class, 'x14ctfv') and contains(., '{text[:30]}')]",
    ]
    try:
        wait_first_selector(page, selectors, state="visible", timeout=5000)
    except PlaywrightTimeout as error:
        raise RuntimeError("No se observó el mensaje enviado") from error


def send_dm(account: str, to_username: str, message: str) -> None:
    audit.log_event("dm.start", account=account, details={"to": to_username})
    with BrowserManager(account_alias=account, persist_session=True) as manager:
        page = manager.ensure_page()
        _open_conversation(page, to_username)
        _type_message(page, message)
        _ensure_bubble(page, message.strip())
        manager.wait_idle(1.0)
    audit.log_event("dm.sent", account=account, details={"to": to_username})


def cli_send_dm(account: str, to_username: str, message: str) -> None:
    send_dm(account, to_username, message)
