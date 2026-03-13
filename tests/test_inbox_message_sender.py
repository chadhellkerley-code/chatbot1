from __future__ import annotations

import asyncio

from src.inbox.conversation_sync import _wait_for_any_selector
from src.inbox.message_sender import TaskDirectClient, _wait_for_visible_locator_async


class _FakeLocatorItem:
    def __init__(self, page: "_FakePage", selector: str, index: int) -> None:
        self._page = page
        self._selector = selector
        self._index = index

    async def is_visible(self) -> bool:
        sequences = self._page.sequences.get(self._selector, [])
        if self._index >= len(sequences):
            return False
        timeline = sequences[self._index]
        if not timeline:
            return False
        tick = min(self._page.tick, len(timeline) - 1)
        return bool(timeline[tick])


class _FakeLocator:
    def __init__(self, page: "_FakePage", selector: str) -> None:
        self._page = page
        self._selector = selector

    async def count(self) -> int:
        return len(self._page.sequences.get(self._selector, []))

    def nth(self, index: int) -> _FakeLocatorItem:
        return _FakeLocatorItem(self._page, self._selector, index)


class _FakeKeyboard:
    async def press(self, _key: str) -> None:
        return None


class _FakeComposer:
    def __init__(self) -> None:
        self.filled: list[str] = []
        self.pressed: list[str] = []

    async def click(self) -> None:
        return None

    async def fill(self, text: str) -> None:
        self.filled.append(str(text))

    async def press(self, key: str) -> None:
        self.pressed.append(str(key))


class _FakePage:
    def __init__(self, sequences: dict[str, list[list[bool]]], *, url: str = "https://www.instagram.com/direct/t/123/") -> None:
        self.sequences = sequences
        self.tick = 0
        self.url = url
        self.keyboard = _FakeKeyboard()

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self, selector)

    async def wait_for_timeout(self, _ms: int) -> None:
        self.tick += 1


class _FakeRuntime:
    def __init__(self, page: _FakePage) -> None:
        self._page = page

    def run_async(self, coro):
        return asyncio.run(coro)

    def open_page(self, _account):
        return self._page

    def close_page(self, _page) -> None:
        return None


def test_task_direct_client_send_text_with_ack_respects_account_quota(monkeypatch) -> None:
    page = _FakePage({})
    runtime = _FakeRuntime(page)
    client = TaskDirectClient(runtime, {"username": "acct-1", "messages_per_account": 2}, thread_id="123")
    monkeypatch.setattr(
        "src.inbox.message_sender.can_send_message_for_account",
        lambda **_kwargs: (False, 2, 2),
    )

    result = client.send_text_with_ack("123", "hola")

    assert result == {
        "ok": False,
        "item_id": None,
        "reason": "account_quota_reached:2/2",
    }


def test_wait_for_any_selector_ignores_hidden_matches() -> None:
    page = _FakePage(
        {
            "hidden": [[False, False, False]],
            "visible-late": [[False, False, True]],
        }
    )

    asyncio.run(_wait_for_any_selector(page, ("hidden", "visible-late"), timeout_ms=1_000))

    assert page.tick >= 2


def test_wait_for_visible_locator_async_polls_until_locator_is_visible() -> None:
    selector = "div[role='main'] div[role='textbox'][contenteditable='true']"
    page = _FakePage({selector: [[False, False, True]]})

    locator = asyncio.run(_wait_for_visible_locator_async(page, (selector,), timeout_ms=1_000))

    assert locator is not None
    assert page.tick >= 2


def test_task_direct_client_waits_for_composer_before_thread_ready() -> None:
    selector = "div[role='main'] div[role='textbox'][contenteditable='true']"
    page = _FakePage({selector: [[False, False, True]]})
    runtime = _FakeRuntime(page)
    client = TaskDirectClient(
        runtime,
        {"username": "matidiazlife"},
        thread_id="123",
        thread_href="https://www.instagram.com/direct/t/123/",
    )
    client._page = page
    client.open_thread_by_href = lambda *_args, **_kwargs: True  # type: ignore[method-assign]

    ready_ok, reason = client.ensure_thread_ready_strict("123")

    assert ready_ok is True
    assert reason == "ok"
    assert page.tick >= 2


def test_task_direct_client_reconciles_send_via_thread_read_after_refresh(monkeypatch) -> None:
    page = _FakePage({})
    runtime = _FakeRuntime(page)
    client = TaskDirectClient(
        runtime,
        {"username": "matidiazlife", "messages_per_account": 20},
        thread_id="123",
        thread_href="https://www.instagram.com/direct/t/123/",
    )
    composer = _FakeComposer()
    call_order: list[str] = []

    monkeypatch.setattr(
        "src.inbox.message_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 0, 20),
    )
    monkeypatch.setattr(client, "ensure_thread_ready_strict", lambda *_args, **_kwargs: (True, "ok"))
    monkeypatch.setattr(
        client,
        "get_outbound_baseline",
        lambda *_args, **_kwargs: {"ok": True, "item_id": "", "timestamp": None, "reason": "baseline_empty"},
    )

    async def _fake_wait_for_visible(*_args, **_kwargs):
        return composer

    monkeypatch.setattr("src.inbox.message_sender._wait_for_visible_locator_async", _fake_wait_for_visible)

    def _fake_confirm(*_args, **_kwargs):
        call_order.append("confirm")
        return {"ok": False, "item_id": None, "reason": "not_confirmed"}

    monkeypatch.setattr(client, "confirm_new_outbound_after_baseline", _fake_confirm)
    monkeypatch.setattr(
        client,
        "refresh_thread_for_confirmation",
        lambda *_args, **_kwargs: call_order.append("refresh") or True,
    )
    monkeypatch.setattr(
        client,
        "confirm_outbound_via_thread_read",
        lambda *_args, **_kwargs: call_order.append("thread_read")
        or {"ok": True, "item_id": "msg-1", "reason": "thread_read_confirmed"},
    )

    result = client.send_text_with_ack("123", "hola")

    assert result == {"ok": True, "item_id": "msg-1", "reason": "thread_read_confirmed"}
    assert composer.filled == ["hola"]
    assert composer.pressed == ["Enter"]
    assert call_order == ["confirm", "refresh", "confirm", "thread_read"]
