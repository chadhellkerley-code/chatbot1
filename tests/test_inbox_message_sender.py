from __future__ import annotations

import asyncio

from src.inbox.conversation_sync import _wait_for_any_selector
from src.inbox.message_sender import (
    TaskDirectClient,
    _wait_for_visible_locator_async,
    send_pack_messages,
)


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
    def __init__(self) -> None:
        self.pressed: list[str] = []
        self.inserted: list[str] = []

    async def press(self, _key: str) -> None:
        self.pressed.append(str(_key))
        return None

    async def insert_text(self, text: str) -> None:
        self.inserted.append(str(text))
        return None

    async def type(self, text: str) -> None:
        self.inserted.append(str(text))
        return None


class _FakeComposer:
    def __init__(self) -> None:
        self.filled: list[str] = []
        self.pressed: list[str] = []
        self.clicked = 0
        self.focused = 0
        self.fail_click = False
        self.fail_fill = False
        self.fail_press = False

    async def click(self, *args, **kwargs) -> None:
        del args, kwargs
        self.clicked += 1
        if self.fail_click:
            raise RuntimeError("click blocked")
        return None

    async def fill(self, text: str, *args, **kwargs) -> None:
        del args, kwargs
        if self.fail_fill:
            raise RuntimeError("fill blocked")
        self.filled.append(str(text))

    async def press(self, key: str, *args, **kwargs) -> None:
        del args, kwargs
        if self.fail_press:
            raise RuntimeError("press blocked")
        self.pressed.append(str(key))

    async def focus(self, *args, **kwargs) -> None:
        del args, kwargs
        self.focused += 1

    async def scroll_into_view_if_needed(self, *args, **kwargs) -> None:
        del args, kwargs
        return None


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

    def run_async(self, coro, *, timeout=None):
        del timeout
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


def test_task_direct_client_send_text_with_ack_bypasses_quota_for_manual_inbox(monkeypatch) -> None:
    page = _FakePage({})
    runtime = _FakeRuntime(page)
    client = TaskDirectClient(
        runtime,
        {"username": "acct-1", "messages_per_account": 2},
        thread_id="123",
        thread_href="https://www.instagram.com/direct/t/123/",
        bypass_account_quota=True,
    )
    composer = _FakeComposer()
    quota_calls = {"count": 0}

    def _fake_quota(**_kwargs):
        quota_calls["count"] += 1
        return (False, 2, 2)

    monkeypatch.setattr("src.inbox.message_sender.can_send_message_for_account", _fake_quota)
    monkeypatch.setattr(client, "ensure_thread_ready_strict", lambda *_args, **_kwargs: (True, "ok"))
    monkeypatch.setattr(
        client,
        "get_outbound_baseline",
        lambda *_args, **_kwargs: {"ok": True, "item_id": "", "timestamp": None, "reason": "baseline_empty"},
    )

    async def _fake_wait_for_visible(*_args, **_kwargs):
        return composer

    monkeypatch.setattr("src.inbox.message_sender._wait_for_visible_locator_async", _fake_wait_for_visible)
    monkeypatch.setattr(
        client,
        "confirm_new_outbound_after_baseline",
        lambda *_args, **_kwargs: {"ok": True, "item_id": "msg-manual-1", "reason": "dom_confirmed"},
    )

    result = client.send_text_with_ack("123", "hola")

    assert result == {"ok": True, "item_id": "msg-manual-1", "reason": "dom_confirmed"}
    assert composer.filled == ["hola"]
    assert composer.pressed == ["Enter"]
    assert quota_calls["count"] == 0


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


def test_task_direct_client_send_text_with_ack_uses_keyboard_and_button_fallbacks(monkeypatch) -> None:
    page = _FakePage({})
    runtime = _FakeRuntime(page)
    client = TaskDirectClient(
        runtime,
        {"username": "matidiazlife", "messages_per_account": 20},
        thread_id="123",
        thread_href="https://www.instagram.com/direct/t/123/",
    )
    composer = _FakeComposer()
    composer.fail_fill = True
    composer.fail_press = True

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

    async def _fake_find_button(*_args, **_kwargs):
        return composer

    monkeypatch.setattr("src.inbox.message_sender._wait_for_visible_locator_async", _fake_wait_for_visible)
    monkeypatch.setattr("src.inbox.message_sender._find_visible_locator_async", _fake_find_button)
    monkeypatch.setattr(
        client,
        "confirm_new_outbound_after_baseline",
        lambda *_args, **_kwargs: {"ok": True, "item_id": "msg-1", "reason": "dom_confirmed"},
    )

    result = client.send_text_with_ack("123", "hola")

    assert result == {"ok": True, "item_id": "msg-1", "reason": "dom_confirmed"}
    assert page.keyboard.inserted == ["hola"]
    assert composer.focused >= 1


def test_send_pack_messages_aborts_before_opening_client_when_quota_cannot_cover_pack(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.inbox.message_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 5, 6),
    )

    def _unexpected_client(*_args, **_kwargs):
        raise AssertionError("TaskDirectClient should not be created when quota is insufficient for the full pack")

    monkeypatch.setattr("src.inbox.message_sender.TaskDirectClient", _unexpected_client)

    result = send_pack_messages(
        object(),
        {"username": "acct-1", "messages_per_account": 6},
        {
            "thread_id": "123",
            "thread_href": "https://www.instagram.com/direct/t/123/",
            "recipient_username": "lead",
        },
        {
            "id": "pack-1",
            "actions": [
                {"type": "text_fixed", "content": "uno"},
                {"type": "text_fixed", "content": "dos"},
                {"type": "text_fixed", "content": "tres"},
            ],
        },
        conversation_text="",
        flow_config={},
    )

    assert result == {
        "ok": False,
        "completed": False,
        "sent_count": 0,
        "reason": "pack_quota_insufficient:5/6:need=3",
        "error": "pack_quota_insufficient:5/6:need=3",
    }


def test_send_pack_messages_executes_pack_when_quota_covers_full_sequence(monkeypatch) -> None:
    class _FakePackClient:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    fake_client = _FakePackClient()
    execute_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "src.inbox.message_sender.can_send_message_for_account",
        lambda **_kwargs: (True, 3, 6),
    )
    monkeypatch.setattr("src.inbox.message_sender.TaskDirectClient", lambda *_args, **_kwargs: fake_client)
    monkeypatch.setattr("src.inbox.message_sender.responder_module._get_account_memory", lambda _account_id: {})
    monkeypatch.setattr("src.inbox.message_sender.responder_module._resolve_ai_api_key", lambda: "api-key")

    def _fake_execute_pack(pack, account_id, memory, **kwargs):
        execute_calls.append(
            {
                "pack": dict(pack),
                "account_id": account_id,
                "memory": dict(memory),
                "thread_id": kwargs.get("thread_id"),
                "client": kwargs.get("client"),
            }
        )
        return {"completed": True, "sent_count": 3}

    monkeypatch.setattr("src.inbox.message_sender.responder_module.execute_pack", _fake_execute_pack)

    result = send_pack_messages(
        object(),
        {"username": "acct-1", "messages_per_account": 6},
        {
            "thread_id": "123",
            "thread_href": "https://www.instagram.com/direct/t/123/",
            "recipient_username": "lead",
        },
        {
            "id": "pack-1",
            "type": "PEACH_A",
            "actions": [
                {"type": "text_fixed", "content": "uno"},
                {"type": "text_fixed", "content": "dos"},
                {"type": "text_fixed", "content": "tres"},
            ],
        },
        conversation_text="historial",
        flow_config={},
    )

    assert result["ok"] is True
    assert result["sent_count"] == 3
    assert execute_calls == [
        {
            "pack": {
                "id": "pack-1",
                "type": "PEACH_A",
                "actions": [
                    {"type": "text_fixed", "content": "uno"},
                    {"type": "text_fixed", "content": "dos"},
                    {"type": "text_fixed", "content": "tres"},
                ],
            },
            "account_id": "acct-1",
            "memory": {},
            "thread_id": "123",
            "client": fake_client,
        }
    ]
    assert fake_client.closed is True
