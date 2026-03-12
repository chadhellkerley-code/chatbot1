from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable

from playwright.async_api import Page, TimeoutError as PwTimeoutError

if TYPE_CHECKING:
    from src.transport.human_instagram_sender import HumanInstagramSender


class InboxNavigator:
    def __init__(
        self,
        sender: "HumanInstagramSender",
        *,
        direct_inbox: str,
        inbox_ready_selectors: tuple[str, ...],
        inbox_ready_timeout_ms: int,
        log_event: Callable[..., None],
    ) -> None:
        self._sender = sender
        self._direct_inbox = str(direct_inbox)
        self._inbox_ready_selectors = tuple(inbox_ready_selectors)
        self._inbox_ready_timeout_ms = int(inbox_ready_timeout_ms)
        self._log_event = log_event

    async def wait_inbox_ready(self, page: Page, timeout_ms: int) -> bool:
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        while time.time() < deadline:
            url_ok = "/direct" in (page.url or "").lower()
            panel = await self._sender._first_visible(page, self._inbox_ready_selectors, max_scan_per_selector=2)
            if url_ok and panel is not None:
                return True
            try:
                await page.wait_for_timeout(140)
            except Exception:
                break
        url_ok = "/direct" in (page.url or "").lower()
        panel = await self._sender._first_visible(page, self._inbox_ready_selectors, max_scan_per_selector=2)
        return bool(url_ok and panel is not None)

    async def ensure_inbox_surface(self, page: Page, *, deadline: float) -> bool:
        flow_hook = getattr(self._sender, "_active_flow_hook", None)
        if callable(flow_hook):
            flow_hook("open inbox", True)
        if self._sender._is_chrome_error_url(page):
            recovered = await self._sender._recover_inbox_after_chrome_error(page, deadline=deadline)
            if recovered:
                if callable(flow_hook):
                    flow_hook("inbox loaded", False)
                return True

        current_url = (page.url or "").lower()
        if "/direct/inbox" in current_url:
            quick_timeout = self._sender._remaining_ms(deadline, 3_500)
            if callable(flow_hook):
                flow_hook("waiting inbox load", True)
            if quick_timeout > 0 and await self.wait_inbox_ready(page, quick_timeout):
                self._log_event("INBOX_REUSE", url=page.url if page else "")
                if callable(flow_hook):
                    flow_hook("inbox loaded", False)
                return True

        nav_timeout = self._sender._remaining_ms(deadline, 45_000)
        if nav_timeout <= 0:
            return False
        try:
            await page.goto(self._direct_inbox, wait_until="domcontentloaded", timeout=nav_timeout)
        except PwTimeoutError:
            self._log_event("INBOX_GOTO_TIMEOUT", timeout_ms=nav_timeout)
        except Exception as exc:
            self._log_event("INBOX_GOTO_FAIL", error=repr(exc))

        if self._sender._is_chrome_error_url(page):
            recovered = await self._sender._recover_inbox_after_chrome_error(page, deadline=deadline)
            if recovered:
                if callable(flow_hook):
                    flow_hook("inbox loaded", False)
                return True

        if callable(flow_hook):
            flow_hook("waiting inbox load", True)
        inbox_timeout = self._sender._remaining_ms(deadline, self._inbox_ready_timeout_ms)
        inbox_ready = inbox_timeout > 0 and await self.wait_inbox_ready(page, inbox_timeout)
        self._log_event(
            "INBOX_READY",
            ok=inbox_ready,
            url=page.url if page else "",
        )
        if inbox_ready and callable(flow_hook):
            flow_hook("inbox loaded", False)
        return inbox_ready
