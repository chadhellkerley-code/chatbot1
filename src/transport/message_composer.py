from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Callable, Optional

from playwright.async_api import Locator, Page

if TYPE_CHECKING:
    from src.transport.human_instagram_sender import HumanInstagramSender


class MessageComposer:
    def __init__(
        self,
        sender: "HumanInstagramSender",
        *,
        thread_composers: tuple[str, ...],
        send_buttons: tuple[str, ...],
        composer_visible_timeout_ms: int,
        type_delay_min_ms: int,
        type_delay_max_ms: int,
        log_event: Callable[..., None],
    ) -> None:
        self._sender = sender
        self._thread_composers = tuple(thread_composers)
        self._send_buttons = tuple(send_buttons)
        self._composer_visible_timeout_ms = int(composer_visible_timeout_ms)
        self._type_delay_min_ms = int(type_delay_min_ms)
        self._type_delay_max_ms = int(type_delay_max_ms)
        self._log_event = log_event

    async def thread_composer(self, page: Page) -> Optional[Locator]:
        for sel in self._thread_composers:
            loc = page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                continue
            for idx in range(min(count, 6)):
                candidate = loc.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    in_overlay = await candidate.evaluate(
                        "el => !!el && !!el.closest('[role=\"dialog\"], [aria-modal=\"true\"]')"
                    )
                except Exception:
                    in_overlay = False
                if not in_overlay:
                    return candidate
        return None

    async def wait_composer_visible(self, page: Page, *, deadline: float) -> Optional[Locator]:
        timeout_ms = self._sender._remaining_ms(deadline, self._composer_visible_timeout_ms)
        if timeout_ms <= 0:
            return None
        timeout_at = time.time() + (timeout_ms / 1000.0)
        while time.time() < timeout_at:
            composer = await self.thread_composer(page)
            if composer is not None:
                self._log_event("COMPOSER_VISIBLE", url=page.url if page else "")
                return composer
            try:
                await page.wait_for_timeout(120)
            except Exception:
                break
        return None

    async def focus_and_clear_composer(self, page: Page, composer: Locator) -> None:
        await composer.click()
        try:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Delete")
            return
        except Exception:
            pass
        try:
            await composer.fill("")
        except Exception:
            pass

    async def type_message(self, page: Page, composer: Locator, text: str) -> None:
        payload = (text or "").replace("\r\n", "\n")
        if not payload.strip():
            raise ValueError("empty_message")

        await self.focus_and_clear_composer(page, composer)
        lines = payload.split("\n")
        for idx, part in enumerate(lines):
            if idx > 0:
                try:
                    await page.keyboard.press("Shift+Enter")
                except Exception:
                    await composer.press("Shift+Enter")
                await self._sender._sleep(0.05, 0.14)
            if not part:
                continue
            try:
                await composer.type(part, delay=random.randint(self._type_delay_min_ms, self._type_delay_max_ms))
            except Exception:
                await page.keyboard.type(part, delay=random.randint(self._type_delay_min_ms, self._type_delay_max_ms))
            await self._sender._sleep(0.03, 0.12)

    async def composer_text(self, composer: Locator) -> str:
        try:
            value = await composer.input_value()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        try:
            value = await composer.inner_text()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        try:
            value = await composer.text_content()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        return ""

    async def wait_for_text_change(
        self,
        composer: Locator,
        *,
        previous_text: str,
        timeout_ms: int,
    ) -> str:
        deadline = time.time() + (max(80, int(timeout_ms or 0)) / 1000.0)
        baseline = str(previous_text or "").strip()
        last_value = baseline
        while time.time() < deadline:
            current = await self.composer_text(composer)
            last_value = current
            if current.strip() != baseline:
                return current
            await asyncio.sleep(0.08)
        return last_value

    async def click_send_button(self, page: Page) -> bool:
        for sel in self._send_buttons:
            btn = page.locator(sel)
            try:
                count = await btn.count()
            except Exception:
                continue
            for idx in range(min(count, 3)):
                candidate = btn.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    await candidate.click()
                    self._log_event("SEND_FALLBACK_CLICK", selector=sel, index=idx, url=page.url if page else "")
                    return True
                except Exception:
                    continue
        return False
