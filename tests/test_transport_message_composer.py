from __future__ import annotations

import asyncio

import pytest

from src.transport.message_composer import MessageComposer


class _SenderStub:
    def _remaining_ms(self, deadline: float, cap_ms: int) -> int:
        return cap_ms

    async def _sleep(self, minimum: float, maximum: float) -> None:
        return None


class _ComposerValueStub:
    def __init__(self, *, input_value=None, inner_text=None, text_content=None) -> None:
        self._input_value = input_value
        self._inner_text = inner_text
        self._text_content = text_content

    async def input_value(self):
        if isinstance(self._input_value, Exception):
            raise self._input_value
        return self._input_value

    async def inner_text(self):
        if isinstance(self._inner_text, Exception):
            raise self._inner_text
        return self._inner_text

    async def text_content(self):
        if isinstance(self._text_content, Exception):
            raise self._text_content
        return self._text_content


def _build_composer() -> MessageComposer:
    return MessageComposer(
        _SenderStub(),
        thread_composers=("a",),
        send_buttons=("b",),
        composer_visible_timeout_ms=1000,
        type_delay_min_ms=1,
        type_delay_max_ms=1,
        log_event=lambda *args, **kwargs: None,
    )


def test_composer_text_uses_first_available_source() -> None:
    composer = _build_composer()
    locator = _ComposerValueStub(
        input_value=RuntimeError("no input"),
        inner_text="  hola mundo  ",
        text_content="fallback",
    )

    value = asyncio.run(composer.composer_text(locator))

    assert value == "hola mundo"


def test_type_message_rejects_empty_payload() -> None:
    composer = _build_composer()

    with pytest.raises(ValueError, match="empty_message"):
        asyncio.run(composer.type_message(page=None, composer=None, text="   \n  "))
