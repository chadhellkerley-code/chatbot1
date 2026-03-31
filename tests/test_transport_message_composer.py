from __future__ import annotations

import asyncio
<<<<<<< HEAD
import time
from types import SimpleNamespace

import pytest
from playwright.async_api import async_playwright
=======

import pytest
>>>>>>> origin/main

from src.transport.message_composer import MessageComposer


class _SenderStub:
    def _remaining_ms(self, deadline: float, cap_ms: int) -> int:
        return cap_ms

    async def _sleep(self, minimum: float, maximum: float) -> None:
        return None


<<<<<<< HEAD
class _ComposerCandidateStub:
    def __init__(
        self,
        *,
        visible: bool = True,
        in_overlay: bool = False,
        typeable: bool = True,
        focus_ok: bool = True,
    ) -> None:
        self.visible = visible
        self.in_overlay = in_overlay
        self.typeable = typeable
        self.focus_ok = focus_ok
        self.focused = False

    async def is_visible(self) -> bool:
        return self.visible

    async def evaluate(self, script: str):
        text = str(script)
        if "closest('[role=\"dialog\"], [aria-modal=\"true\"]')" in text:
            return self.in_overlay
        if "const typeable =" in text:
            return self.typeable and not self.in_overlay
        if "typeof el.focus !== \"function\"" in text:
            if not self.focus_ok:
                return False
            self.focused = True
            return True
        if "document.activeElement === el" in text:
            return self.focused
        raise AssertionError(f"unexpected evaluate call: {text}")

    async def focus(self, timeout: int = 0) -> None:
        del timeout
        if not self.focus_ok:
            raise RuntimeError("focus_failed")
        self.focused = True


class _LocatorCollectionStub:
    def __init__(self, candidates) -> None:
        self._candidates = list(candidates)

    async def count(self) -> int:
        return len(self._candidates)

    def nth(self, index: int):
        return self._candidates[index]


class _PageStub:
    def __init__(self, candidates) -> None:
        self.url = "https://www.instagram.com/direct/t/thread-123/"
        self._candidates = list(candidates)
        self.wait_calls: list[int] = []

    def locator(self, _selector: str):
        return _LocatorCollectionStub(self._candidates)

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_calls.append(int(timeout_ms))
        await asyncio.sleep(0)


=======
>>>>>>> origin/main
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
<<<<<<< HEAD
        usable_composer_timeout_ms=1000,
=======
>>>>>>> origin/main
        type_delay_min_ms=1,
        type_delay_max_ms=1,
        log_event=lambda *args, **kwargs: None,
    )


<<<<<<< HEAD
async def _audit_surface_from_html(html: str) -> dict:
    composer = _build_composer()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 900, "height": 700})
        page = await context.new_page()
        await page.set_content(html)
        locator = page.locator("[role='main'] [role='textbox']").first
        meta = await composer.audit_post_open_surface(page, composer=locator)
        await context.close()
        await browser.close()
        return meta


=======
>>>>>>> origin/main
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
<<<<<<< HEAD


def test_thread_composer_require_usable_rejects_modal_candidate() -> None:
    composer = _build_composer()
    page = _PageStub([_ComposerCandidateStub(in_overlay=True)])

    result = asyncio.run(composer.thread_composer(page, require_usable=True))

    assert result is None


def test_thread_composer_require_usable_rejects_non_typeable_candidate() -> None:
    composer = _build_composer()
    page = _PageStub([_ComposerCandidateStub(typeable=False)])

    result = asyncio.run(composer.thread_composer(page, require_usable=True))

    assert result is None


def test_ensure_visible_chat_surface_ready_normalizes_below_fold(monkeypatch) -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")
    fake_locator = object()
    audits = [
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": False,
            "composer_partially_visible": True,
            "composer_below_fold": True,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": False,
            "diagnostic_reason_codes": [],
        },
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": True,
            "diagnostic_reason_codes": [],
        },
    ]
    normalize_calls: list[str] = []

    async def _wait_visible(_page, *, deadline: float):
        assert deadline > time.time()
        return fake_locator

    async def _wait_usable(_page, *, deadline: float):
        assert deadline > time.time()
        return fake_locator

    async def _audit(_page, *, composer=None):
        assert composer is fake_locator
        return dict(audits.pop(0))

    async def _normalize(_page, _composer):
        normalize_calls.append("normalize")
        return {"actions": ["composer_scroll_into_view", "scroll_chat_container_to_bottom"]}

    monkeypatch.setattr(composer, "wait_composer_visible", _wait_visible)
    monkeypatch.setattr(composer, "wait_for_usable_composer", _wait_usable)
    monkeypatch.setattr(composer, "audit_post_open_surface", _audit)
    monkeypatch.setattr(composer, "_normalize_post_open_surface", _normalize)

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is fake_locator
    assert meta["ok"] is True
    assert meta["normalized"] is True
    assert meta["normalization"]["actions"] == ["composer_scroll_into_view", "scroll_chat_container_to_bottom"]
    assert normalize_calls == ["normalize"]


def test_ensure_visible_chat_surface_ready_scrolls_chat_container_to_bottom(monkeypatch) -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")
    fake_locator = object()
    audits = [
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": False,
            "diagnostic_reason_codes": [],
        },
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": True,
            "diagnostic_reason_codes": [],
        },
    ]

    async def _wait_visible(_page, *, deadline: float):
        return fake_locator

    async def _wait_usable(_page, *, deadline: float):
        return fake_locator

    async def _audit(_page, *, composer=None):
        return dict(audits.pop(0))

    async def _normalize(_page, _composer):
        return {"actions": ["scroll_chat_container_to_bottom"]}

    monkeypatch.setattr(composer, "wait_composer_visible", _wait_visible)
    monkeypatch.setattr(composer, "wait_for_usable_composer", _wait_usable)
    monkeypatch.setattr(composer, "audit_post_open_surface", _audit)
    monkeypatch.setattr(composer, "_normalize_post_open_surface", _normalize)

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is fake_locator
    assert meta["ok"] is True
    assert meta["normalization"]["actions"] == ["scroll_chat_container_to_bottom"]


def test_ensure_visible_chat_surface_ready_keeps_visible_composer_when_overlap_persists(monkeypatch) -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")
    fake_locator = object()
    audits = [
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": True,
            "scrollable_ancestor_found": False,
            "scroll_near_bottom": True,
            "diagnostic_reason_codes": [],
        },
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": True,
            "scrollable_ancestor_found": False,
            "scroll_near_bottom": True,
            "diagnostic_reason_codes": [],
        },
    ]
    normalize_calls: list[str] = []

    async def _wait_visible(_page, *, deadline: float):
        return fake_locator

    async def _wait_usable(_page, *, deadline: float):
        return fake_locator

    async def _audit(_page, *, composer=None):
        return dict(audits.pop(0))

    async def _normalize(_page, _composer):
        normalize_calls.append("normalize")
        return {"actions": ["composer_scroll_into_view"]}

    monkeypatch.setattr(composer, "wait_composer_visible", _wait_visible)
    monkeypatch.setattr(composer, "wait_for_usable_composer", _wait_usable)
    monkeypatch.setattr(composer, "audit_post_open_surface", _audit)
    monkeypatch.setattr(composer, "_normalize_post_open_surface", _normalize)

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is fake_locator
    assert meta["ok"] is False
    assert meta["reason_code"] == "COMPOSER_OVERLAPPED"
    assert meta["failed_checks"] == ["composer_overlapped"]
    assert normalize_calls == ["normalize"]


def test_ensure_visible_chat_surface_ready_keeps_header_partial_hydration_diagnostic_only(monkeypatch) -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")
    fake_locator = object()
    audit = {
        "supported": True,
        "composer_found": True,
        "composer_fully_inside_viewport": True,
        "composer_partially_visible": True,
        "composer_below_fold": False,
        "composer_overlapped": False,
        "scrollable_ancestor_found": True,
        "scroll_near_bottom": True,
        "diagnostic_reason_codes": ["HEADER_PARTIAL_HYDRATION"],
    }

    async def _wait_visible(_page, *, deadline: float):
        return fake_locator

    async def _wait_usable(_page, *, deadline: float):
        return fake_locator

    async def _audit(_page, *, composer=None):
        return dict(audit)

    async def _normalize(_page, _composer):
        raise AssertionError("normalization should not run when the surface is already usable")

    monkeypatch.setattr(composer, "wait_composer_visible", _wait_visible)
    monkeypatch.setattr(composer, "wait_for_usable_composer", _wait_usable)
    monkeypatch.setattr(composer, "audit_post_open_surface", _audit)
    monkeypatch.setattr(composer, "_normalize_post_open_surface", _normalize)

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is fake_locator
    assert meta["ok"] is True
    assert meta["diagnostic_reason_codes"] == ["HEADER_PARTIAL_HYDRATION"]
    assert meta["failed_checks"] == []


def test_audit_post_open_surface_ignores_same_surface_placeholder_overlap() -> None:
    html = """
    <main role="main">
      <div id="footer" style="position: relative; margin-top: 120px; width: 420px; height: 36px;">
        <div
          id="composer"
          role="textbox"
          aria-label="Mensaje"
          aria-placeholder="Envía un mensaje..."
          contenteditable="true"
          tabindex="0"
          style="position:absolute; inset:0; height:18px; user-select:text; white-space:pre-wrap;"
        ><p><br></p></div>
        <div
          id="placeholder-shell"
          aria-hidden="true"
          style="position:absolute; inset:0; height:18px; z-index:2;"
        ><div>Envía un mensaje...</div></div>
      </div>
    </main>
    """

    meta = asyncio.run(_audit_surface_from_html(html))

    assert meta["composer_found"] is True
    assert meta["composer_overlap_ignored"] is True
    assert meta["composer_overlap_reason"] == "same_surface_placeholder_shell"
    assert meta["composer_overlapped"] is False
    assert meta["surface_failed_checks"] == []


def test_audit_post_open_surface_rejects_unrelated_overlay_overlap() -> None:
    html = """
    <main role="main">
      <div id="footer" style="position: relative; margin-top: 120px; width: 420px; height: 36px;">
        <div
          id="composer"
          role="textbox"
          aria-label="Mensaje"
          aria-placeholder="Envía un mensaje..."
          contenteditable="true"
          tabindex="0"
          style="position:absolute; inset:0; height:18px; user-select:text; white-space:pre-wrap;"
        ><p><br></p></div>
        <div
          id="blocking-overlay"
          role="status"
          style="position:absolute; inset:0; height:18px; z-index:2;"
        >Bloqueando</div>
      </div>
    </main>
    """

    meta = asyncio.run(_audit_surface_from_html(html))

    assert meta["composer_found"] is True
    assert meta["composer_overlap_ignored"] is False
    assert meta["composer_overlap_reason"] == "blocking_element"
    assert meta["composer_overlapped"] is True
    assert meta["surface_failed_checks"] == ["composer_overlapped"]


def test_ensure_visible_chat_surface_ready_allows_non_bottom_scroll_when_composer_is_usable(monkeypatch) -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")
    fake_locator = object()
    audits = [
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": False,
            "diagnostic_reason_codes": ["HEADER_PARTIAL_HYDRATION"],
        },
        {
            "supported": True,
            "composer_found": True,
            "composer_fully_inside_viewport": True,
            "composer_partially_visible": True,
            "composer_below_fold": False,
            "composer_overlapped": False,
            "scrollable_ancestor_found": True,
            "scroll_near_bottom": False,
            "diagnostic_reason_codes": ["HEADER_PARTIAL_HYDRATION"],
        },
    ]

    async def _wait_visible(_page, *, deadline: float):
        return fake_locator

    async def _wait_usable(_page, *, deadline: float):
        return fake_locator

    async def _audit(_page, *, composer=None):
        return dict(audits.pop(0))

    async def _normalize(_page, _composer):
        return {"actions": ["scroll_chat_container_to_bottom"]}

    monkeypatch.setattr(composer, "wait_composer_visible", _wait_visible)
    monkeypatch.setattr(composer, "wait_for_usable_composer", _wait_usable)
    monkeypatch.setattr(composer, "audit_post_open_surface", _audit)
    monkeypatch.setattr(composer, "_normalize_post_open_surface", _normalize)

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is fake_locator
    assert meta["ok"] is True
    assert meta["reason_code"] == ""
    assert meta["diagnostic_reason_codes"] == ["HEADER_PARTIAL_HYDRATION"]
    assert meta["failed_checks"] == []
    assert meta["after"]["surface_failed_checks"] == []


def test_ensure_visible_chat_surface_ready_fails_when_composer_is_missing() -> None:
    composer = _build_composer()
    fake_page = SimpleNamespace(url="https://www.instagram.com/direct/t/thread-123/")

    async def _wait_visible(_page, *, deadline: float):
        return None

    composer.wait_composer_visible = _wait_visible  # type: ignore[method-assign]

    result, meta = asyncio.run(composer.ensure_visible_chat_surface_ready(fake_page, deadline=time.time() + 3))

    assert result is None
    assert meta["ok"] is False
    assert meta["reason_code"] == "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION"
    assert meta["failed_checks"] == ["composer_not_found"]
=======
>>>>>>> origin/main
