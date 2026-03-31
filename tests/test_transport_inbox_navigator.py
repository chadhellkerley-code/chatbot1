from __future__ import annotations

import asyncio
import time

from src.transport.inbox_navigator import InboxNavigator


class _PageStub:
    def __init__(self, url: str) -> None:
        self.url = url
        self.goto_calls: list[tuple[str, str, int]] = []

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        return None

    async def goto(self, url: str, *, wait_until: str, timeout: int) -> None:
        self.goto_calls.append((url, wait_until, timeout))
        self.url = url


class _SenderStub:
    def __init__(self, visible_sequence: list[object | None]) -> None:
        self._visible_sequence = list(visible_sequence)
        self._first_visible_calls = 0
<<<<<<< HEAD
        self.layout_calls = 0
=======
>>>>>>> origin/main

    async def _first_visible(self, page, selectors, *, max_scan_per_selector: int = 4):
        if self._first_visible_calls < len(self._visible_sequence):
            result = self._visible_sequence[self._first_visible_calls]
        else:
            result = self._visible_sequence[-1] if self._visible_sequence else None
        self._first_visible_calls += 1
        return result

    def _is_chrome_error_url(self, page) -> bool:
        return False

    async def _recover_inbox_after_chrome_error(self, page, *, deadline: float) -> bool:
        return False

    def _remaining_ms(self, deadline: float, cap_ms: int) -> int:
        return cap_ms

<<<<<<< HEAD
    async def _ensure_campaign_desktop_layout(self, page) -> bool:
        self.layout_calls += 1
        return True

=======
>>>>>>> origin/main

def test_wait_inbox_ready_retries_until_panel_is_visible() -> None:
    sender = _SenderStub([None, object()])
    navigator = InboxNavigator(
        sender,
        direct_inbox="https://www.instagram.com/direct/inbox/",
        inbox_ready_selectors=("input",),
        inbox_ready_timeout_ms=1000,
        log_event=lambda *args, **kwargs: None,
    )
    page = _PageStub("https://www.instagram.com/direct/inbox/")

    ready = asyncio.run(navigator.wait_inbox_ready(page, timeout_ms=400))

    assert ready is True
    assert sender._first_visible_calls >= 2


def test_ensure_inbox_surface_reuses_existing_inbox_when_ready() -> None:
    events: list[tuple[str, dict]] = []
    sender = _SenderStub([object()])
    navigator = InboxNavigator(
        sender,
        direct_inbox="https://www.instagram.com/direct/inbox/",
        inbox_ready_selectors=("input",),
        inbox_ready_timeout_ms=1000,
        log_event=lambda event, **kwargs: events.append((event, kwargs)),
    )
    page = _PageStub("https://www.instagram.com/direct/inbox/")

    ready = asyncio.run(navigator.ensure_inbox_surface(page, deadline=time.time() + 5))

    assert ready is True
    assert page.goto_calls == []
    assert events == [("INBOX_REUSE", {"url": "https://www.instagram.com/direct/inbox/"})]


def test_ensure_inbox_surface_navigates_away_from_thread_page_before_search() -> None:
    events: list[tuple[str, dict]] = []
    sender = _SenderStub([object()])
    navigator = InboxNavigator(
        sender,
        direct_inbox="https://www.instagram.com/direct/inbox/",
        inbox_ready_selectors=("input",),
        inbox_ready_timeout_ms=1000,
        log_event=lambda event, **kwargs: events.append((event, kwargs)),
    )
    page = _PageStub("https://www.instagram.com/direct/t/thread-123/")

    ready = asyncio.run(navigator.ensure_inbox_surface(page, deadline=time.time() + 5))

    assert ready is True
    assert page.goto_calls == [("https://www.instagram.com/direct/inbox/", "domcontentloaded", 45000)]
    assert events == [("INBOX_READY", {"ok": True, "url": "https://www.instagram.com/direct/inbox/"})]
<<<<<<< HEAD
    assert sender.layout_calls == 3
=======
>>>>>>> origin/main
