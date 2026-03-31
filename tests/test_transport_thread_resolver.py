from __future__ import annotations

import asyncio
import time

from src.transport.thread_resolver import SidebarThreadResolver, ThreadOpenResult


class _SearchInputStub:
    def __init__(self, value: str = "") -> None:
        self.value = value

    async def input_value(self) -> str:
        return self.value


class _PageStub:
    def __init__(self) -> None:
        self.url = "https://www.instagram.com/direct/inbox/"
        self.wait_calls: list[int] = []
<<<<<<< HEAD
        self.evaluate_calls: list[str] = []
        self.keyboard = type("_KeyboardStub", (), {"press": self._press})()
        self.key_presses: list[str] = []
=======
>>>>>>> origin/main

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_calls.append(timeout_ms)

<<<<<<< HEAD
    async def evaluate(self, script: str, *_args, **_kwargs):
        self.evaluate_calls.append(str(script))
        return False

    async def _press(self, key: str) -> None:
        self.key_presses.append(str(key))

=======
>>>>>>> origin/main

class _SenderStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
<<<<<<< HEAD
        self._message_composer = type(
            "_ComposerStub",
            (),
            {"thread_composer": staticmethod(lambda _page: asyncio.sleep(0, result=None))},
        )()
        self._inbox_navigator = type(
            "_InboxNavigatorStub",
            (),
            {"ensure_inbox_surface": staticmethod(lambda _page, *, deadline: asyncio.sleep(0, result=True))},
        )()
=======
>>>>>>> origin/main

    async def _focus_input_best_effort(self, _search_input) -> None:
        self.calls.append(("focus", ""))

    async def _clear_input_best_effort(self, _page, search_input) -> None:
        self.calls.append(("clear", ""))
        search_input.value = ""

    async def _sleep(self, _minimum: float, _maximum: float) -> None:
        return None

    async def _type_input_like_human(self, _page, search_input, value: str) -> None:
        self.calls.append(("type_like_human", value))
        search_input.value = value

    async def _set_input_value_best_effort(self, _page, search_input, value: str) -> None:
        self.calls.append(("set_input_value", value))
        search_input.value = value

    async def _first_visible(self, _page, _selectors, *, max_scan_per_selector: int = 2):
        return _SearchInputStub()

    def _is_chrome_error_url(self, _page) -> bool:
        return False

    def _remaining_ms(self, deadline: float, cap_ms: int) -> int:
        remaining_ms = int((deadline - time.time()) * 1000)
        return max(0, min(cap_ms, remaining_ms))

    def _normalize_username(self, username: str) -> str:
        return str(username or "").strip().lstrip("@")


def _build_resolver(sender: _SenderStub) -> SidebarThreadResolver:
    return SidebarThreadResolver(
        sender,
<<<<<<< HEAD
        compose_triggers=("button",),
        search_inputs=("input",),
        result_rows=("li",),
        confirm_buttons=("button",),
=======
        search_inputs=("input",),
        result_rows=("li",),
>>>>>>> origin/main
        thread_open_timeout_ms=2000,
        sidebar_row_timeout_ms=1200,
        log_event=lambda *args, **kwargs: None,
    )


def test_clear_and_type_search_uses_human_typing() -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    search_input = _SearchInputStub()

    asyncio.run(resolver.clear_and_type_search(page, search_input, "lead_01"))

    assert sender.calls == [
        ("focus", ""),
        ("clear", ""),
        ("type_like_human", "lead_01"),
    ]
    assert search_input.value == "lead_01"


def test_clear_and_type_search_repairs_corrupted_value_with_exact_set() -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    search_input = _SearchInputStub()

    async def _corrupting_type(_page, _search_input, value: str) -> None:
        sender.calls.append(("type_like_human", value))
        _search_input.value = value + value

    sender._type_input_like_human = _corrupting_type  # type: ignore[method-assign]

    asyncio.run(resolver.clear_and_type_search(page, search_input, "lead_01"))

    assert sender.calls == [
        ("focus", ""),
        ("clear", ""),
        ("type_like_human", "lead_01"),
        ("set_input_value", "lead_01"),
    ]
    assert search_input.value == "lead_01"


<<<<<<< HEAD
=======
def test_open_thread_from_sidebar_does_not_use_fixed_sleep_after_typing(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()

    async def _clear_and_type_search(_page, _search_input, _username: str) -> None:
        return None

    async def _find_exact_sidebar_row(_page, _username: str, *, timeout_ms: int):
        return None

    async def _sidebar_js_click_exact_match(_page, _username: str):
        return False, {}

    monkeypatch.setattr(resolver, "clear_and_type_search", _clear_and_type_search)
    monkeypatch.setattr(resolver, "find_exact_sidebar_row", _find_exact_sidebar_row)
    monkeypatch.setattr(resolver, "sidebar_js_click_exact_match", _sidebar_js_click_exact_match)

    result = asyncio.run(
        resolver.open_thread_from_sidebar(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result.opened is False
    assert result.reason == "username_not_found"
    assert 1000 not in page.wait_calls


>>>>>>> origin/main
def test_wait_sidebar_results_ready_waits_for_surface_change(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    probes = iter(
        [
            {"row_count": 2, "query_value": "lead_01", "signature": "baseline"},
            {"row_count": 2, "query_value": "lead_01", "signature": "baseline"},
            {"row_count": 1, "query_value": "lead_01", "signature": "changed"},
        ]
    )

    async def _probe_sidebar_results(_page, _username="", *, click_match: bool = False):
        assert click_match is False
        return next(probes)

    monkeypatch.setattr(resolver, "_probe_sidebar_results", _probe_sidebar_results)

    result = asyncio.run(
        resolver.wait_sidebar_results_ready(
            page,
            "lead_01",
            timeout_ms=900,
            baseline_signature="baseline",
        )
    )

    assert result["surface_changed"] is True
    assert result["signature"] == "changed"
    assert len(page.wait_calls) == 2


<<<<<<< HEAD
def test_try_open_existing_sidebar_thread_uses_locator_row_not_js_exact_match(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    search_input = _SearchInputStub()
    row = object()

    async def _wait_sidebar_search_input(_page, *, timeout_ms: int):
        assert timeout_ms > 0
        return search_input
=======
def test_open_thread_from_sidebar_uses_js_match_when_probe_detects_exact_result(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    sentinel = ThreadOpenResult(True, "ok", thread_id="thread-123")
>>>>>>> origin/main

    async def _clear_and_type_search(_page, _search_input, _username: str) -> None:
        return None

<<<<<<< HEAD
    async def _find_exact_sidebar_row(_page, _username: str, *, timeout_ms: int):
        assert timeout_ms > 0
        return row

    async def _wait_thread_open(_page, _row, _username: str, *, timeout_ms: int, flow_hook=None):
        assert _row is row
        assert timeout_ms > 0
        assert flow_hook is None
        page.url = "https://www.instagram.com/direct/t/thread-123/"
        return True

    async def _unexpected_js(*_args, **_kwargs):
        raise AssertionError("JS exact-match path should not be reachable")

    monkeypatch.setattr(resolver, "_wait_sidebar_search_input", _wait_sidebar_search_input)
    monkeypatch.setattr(resolver, "clear_and_type_search", _clear_and_type_search)
    monkeypatch.setattr(resolver, "find_exact_sidebar_row", _find_exact_sidebar_row)
    monkeypatch.setattr(resolver, "wait_thread_open", _wait_thread_open)
    monkeypatch.setattr(resolver, "_open_exact_match_via_js", _unexpected_js)

    result = asyncio.run(
        resolver._try_open_existing_sidebar_thread(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result == ThreadOpenResult(True, "ok", method="sidebar_search", thread_id="thread-123")


def test_try_open_existing_sidebar_thread_waits_for_sidebar_results_before_lookup(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    search_input = _SearchInputStub()
    row = object()
    call_order: list[str] = []

    async def _wait_sidebar_search_input(_page, *, timeout_ms: int):
        assert timeout_ms > 0
        call_order.append("search_input")
        return search_input

    async def _probe_sidebar_results(_page, _username="", *, click_match: bool = False):
        assert click_match is False
        return {"signature": "baseline"}

    async def _clear_and_type_search(_page, _search_input, _username: str) -> None:
        call_order.append("type")
=======
    async def _probe_sidebar_results(_page, _username="", *, click_match: bool = False):
        return {"signature": "baseline"} if not _username else {"exact_match": True, "row_count": 1, "query_value": "lead_01"}
>>>>>>> origin/main

    async def _wait_sidebar_results_ready(_page, _username: str, *, timeout_ms: int, baseline_signature: str):
        assert timeout_ms > 0
        assert baseline_signature == "baseline"
<<<<<<< HEAD
        call_order.append("results_ready")
        return {"query_value": "lead_01", "signature": "changed", "surface_changed": True}

    async def _find_exact_sidebar_row(_page, _username: str, *, timeout_ms: int):
        assert timeout_ms > 0
        call_order.append("find_row")
        return row

    async def _wait_thread_open(_page, _row, _username: str, *, timeout_ms: int, flow_hook=None):
        assert _row is row
        assert timeout_ms > 0
        assert flow_hook is None
        call_order.append("thread_open")
        page.url = "https://www.instagram.com/direct/t/thread-123/"
        return True

    monkeypatch.setattr(resolver, "_wait_sidebar_search_input", _wait_sidebar_search_input)
    monkeypatch.setattr(resolver, "_probe_sidebar_results", _probe_sidebar_results)
    monkeypatch.setattr(resolver, "clear_and_type_search", _clear_and_type_search)
    monkeypatch.setattr(resolver, "wait_sidebar_results_ready", _wait_sidebar_results_ready)
    monkeypatch.setattr(resolver, "find_exact_sidebar_row", _find_exact_sidebar_row)
    monkeypatch.setattr(resolver, "wait_thread_open", _wait_thread_open)

    result = asyncio.run(
        resolver._try_open_existing_sidebar_thread(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result == ThreadOpenResult(True, "ok", method="sidebar_search", thread_id="thread-123")
    assert call_order == ["search_input", "type", "results_ready", "find_row", "thread_open"]


def test_try_open_existing_sidebar_thread_fails_closed_when_sidebar_is_unavailable(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()

    async def _wait_sidebar_search_input(_page, *, timeout_ms: int):
        assert timeout_ms > 0
        return None

    monkeypatch.setattr(resolver, "_wait_sidebar_search_input", _wait_sidebar_search_input)

    result = asyncio.run(
        resolver._try_open_existing_sidebar_thread(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result == ThreadOpenResult(False, "sidebar_unavailable", method="sidebar_search")


def test_open_thread_from_sidebar_uses_sidebar_resolution_only(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    sentinel = ThreadOpenResult(True, "ok", method="sidebar_search", thread_id="thread-123")

    async def _cleanup(_page, *, deadline: float):
        assert deadline > time.time()
        return True

    async def _sidebar_only(_page, _username: str, *, deadline: float, flow_hook=None):
=======
        return {"exact_match": True, "row_count": 1, "query_value": "lead_01", "surface_changed": True}

    async def _open_exact_match_via_js(_page, _username: str, *, deadline: float, flow_hook=None):
>>>>>>> origin/main
        assert deadline > time.time()
        assert flow_hook is None
        return sentinel

<<<<<<< HEAD
    async def _unexpected_compose(*_args, **_kwargs):
        raise AssertionError("compose/new-message flow should not be reachable")

    monkeypatch.setattr(resolver, "cleanup_stale_compose_state", _cleanup)
    monkeypatch.setattr(resolver, "_try_open_existing_sidebar_thread", _sidebar_only)
    monkeypatch.setattr(resolver, "_open_compose_surface", _unexpected_compose)
    monkeypatch.setattr(resolver, "_open_exact_match_via_js", _unexpected_compose)
=======
    async def _find_exact_sidebar_row(_page, _username: str, *, timeout_ms: int):
        raise AssertionError("locator search should be skipped when the exact result is already visible")

    monkeypatch.setattr(resolver, "clear_and_type_search", _clear_and_type_search)
    monkeypatch.setattr(resolver, "_probe_sidebar_results", _probe_sidebar_results)
    monkeypatch.setattr(resolver, "wait_sidebar_results_ready", _wait_sidebar_results_ready)
    monkeypatch.setattr(resolver, "_open_exact_match_via_js", _open_exact_match_via_js)
    monkeypatch.setattr(resolver, "find_exact_sidebar_row", _find_exact_sidebar_row)
>>>>>>> origin/main

    result = asyncio.run(
        resolver.open_thread_from_sidebar(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result == sentinel
<<<<<<< HEAD


def test_wait_thread_navigation_accepts_thread_url_during_partial_header_hydration(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    page.url = "https://www.instagram.com/direct/t/thread-123/"

    async def _header_match(_page, _username: str) -> bool:
        return False

    monkeypatch.setattr(resolver, "_chat_header_matches_target", _header_match)

    opened = asyncio.run(
        resolver.wait_thread_navigation(
            page,
            "lead_01",
            timeout_ms=900,
            previous_url=page.url,
            previous_thread="thread-old",
        )
    )

    assert opened is True


def test_wait_thread_navigation_accepts_thread_only_with_url_and_header(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    page.url = "https://www.instagram.com/direct/t/thread-123/"

    async def _compose_surface_active(_page) -> bool:
        return True

    async def _header_match(_page, _username: str) -> bool:
        return True

    monkeypatch.setattr(resolver, "_compose_surface_active", _compose_surface_active)
    monkeypatch.setattr(resolver, "_chat_header_matches_target", _header_match)

    opened = asyncio.run(
        resolver.wait_thread_navigation(
            page,
            "lead_01",
            timeout_ms=900,
            previous_url="https://www.instagram.com/direct/inbox/",
            previous_thread="",
            candidate_username="lead_01",
        )
    )

    assert opened is True


def test_cleanup_stale_compose_state_uses_escape_and_inbox_reset(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    state = {"compose": True, "dialog": True, "reset_calls": 0}

    async def _compose_surface_active(_page) -> bool:
        return bool(state["compose"])

    async def _dialog_surface_visible(_page) -> bool:
        return bool(state["dialog"])

    async def _ensure_inbox_surface(_page, *, deadline: float):
        assert deadline > time.time()
        state["reset_calls"] += 1
        state["compose"] = False
        state["dialog"] = False
        return True

    async def _first_visible(_page, selectors, *, max_scan_per_selector: int = 2):
        del max_scan_per_selector
        if selectors == resolver._DIALOG_CLOSE_BUTTONS:
            return None
        return _SearchInputStub()

    sender._inbox_navigator = type(
        "_InboxNavigatorStub",
        (),
        {"ensure_inbox_surface": staticmethod(_ensure_inbox_surface)},
    )()
    sender._first_visible = _first_visible  # type: ignore[method-assign]
    monkeypatch.setattr(resolver, "_compose_surface_active", _compose_surface_active)
    monkeypatch.setattr(resolver, "_dialog_surface_visible", _dialog_surface_visible)

    cleaned = asyncio.run(
        resolver.cleanup_stale_compose_state(
            page,
            deadline=time.time() + 3,
        )
    )

    assert cleaned is True
    assert state["reset_calls"] == 1
    assert "Escape" in page.key_presses


def test_open_thread_from_sidebar_cleans_stale_modal_before_sidebar_search(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    call_order: list[str] = []

    async def _cleanup(_page, *, deadline: float):
        assert deadline > time.time()
        call_order.append("cleanup")
        return True

    async def _sidebar_open(_page, _username: str, *, deadline: float, flow_hook=None):
        assert deadline > time.time()
        assert flow_hook is None
        call_order.append("sidebar")
        return ThreadOpenResult(False, "username_not_found", method="sidebar_search")

    monkeypatch.setattr(resolver, "cleanup_stale_compose_state", _cleanup)
    monkeypatch.setattr(resolver, "_try_open_existing_sidebar_thread", _sidebar_open)

    result = asyncio.run(
        resolver.open_thread_from_sidebar(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result.opened is False
    assert result.reason == "username_not_found"
    assert call_order == ["cleanup", "sidebar"]
=======
>>>>>>> origin/main
