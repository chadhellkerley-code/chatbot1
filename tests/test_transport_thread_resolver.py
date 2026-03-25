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

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_calls.append(timeout_ms)


class _SenderStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

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
        search_inputs=("input",),
        result_rows=("li",),
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


def test_open_thread_from_sidebar_uses_js_match_when_probe_detects_exact_result(monkeypatch) -> None:
    sender = _SenderStub()
    resolver = _build_resolver(sender)
    page = _PageStub()
    sentinel = ThreadOpenResult(True, "ok", thread_id="thread-123")

    async def _clear_and_type_search(_page, _search_input, _username: str) -> None:
        return None

    async def _probe_sidebar_results(_page, _username="", *, click_match: bool = False):
        return {"signature": "baseline"} if not _username else {"exact_match": True, "row_count": 1, "query_value": "lead_01"}

    async def _wait_sidebar_results_ready(_page, _username: str, *, timeout_ms: int, baseline_signature: str):
        assert timeout_ms > 0
        assert baseline_signature == "baseline"
        return {"exact_match": True, "row_count": 1, "query_value": "lead_01", "surface_changed": True}

    async def _open_exact_match_via_js(_page, _username: str, *, deadline: float, flow_hook=None):
        assert deadline > time.time()
        assert flow_hook is None
        return sentinel

    async def _find_exact_sidebar_row(_page, _username: str, *, timeout_ms: int):
        raise AssertionError("locator search should be skipped when the exact result is already visible")

    monkeypatch.setattr(resolver, "clear_and_type_search", _clear_and_type_search)
    monkeypatch.setattr(resolver, "_probe_sidebar_results", _probe_sidebar_results)
    monkeypatch.setattr(resolver, "wait_sidebar_results_ready", _wait_sidebar_results_ready)
    monkeypatch.setattr(resolver, "_open_exact_match_via_js", _open_exact_match_via_js)
    monkeypatch.setattr(resolver, "find_exact_sidebar_row", _find_exact_sidebar_row)

    result = asyncio.run(
        resolver.open_thread_from_sidebar(
            page,
            "lead_01",
            deadline=time.time() + 3,
        )
    )

    assert result == sentinel
