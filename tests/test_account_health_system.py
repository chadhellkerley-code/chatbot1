from __future__ import annotations

import asyncio
from collections import deque
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import health_store
from core import accounts
from core.inbox.inbox_manager import InboxManager
from src import leads_filter_pipeline
from src.health_playwright import detect_account_health_async, detect_account_health_sync


class _FakeContext:
    def __init__(self, *, cookies: list[dict[str, str]] | None = None) -> None:
        self._cookies = list(cookies or [])

    def cookies(self, *_args, **_kwargs) -> list[dict[str, str]]:
        return list(self._cookies)


class _FakeLocator:
    def __init__(self, page: "_FakePage", selector: str) -> None:
        self._page = page
        self._selector = selector

    @property
    def first(self) -> "_FakeLocator":
        return self

    def count(self) -> int:
        return 1 if self._page.has_selector(self._selector) else 0

    def click(self) -> None:
        self._page.clicked.append(self._selector)

    def inner_text(self) -> str:
        return self._page.body_text


class _FakePage:
    def __init__(
        self,
        *,
        url: str,
        selectors: set[str] | None = None,
        body_text: str = "",
        context: _FakeContext | None = None,
    ) -> None:
        self.url = url
        self._selectors = set(selectors or set())
        self.body_text = body_text
        self.clicked: list[str] = []
        self.context = context or _FakeContext()

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self, selector)

    def has_selector(self, selector: str) -> bool:
        if selector == "body":
            return bool(self.body_text)
        return selector in self._selectors

    def wait_for_selector(self, selector: str, timeout: int = 0) -> bool:
        del timeout
        selectors = [item.strip() for item in selector.split(",")]
        if any(self.has_selector(item) for item in selectors):
            return True
        raise RuntimeError("selector_not_found")


class _AsyncFakeLocator:
    def __init__(self, page: "_AsyncFakePage", selector: str) -> None:
        self._page = page
        self._selector = selector

    @property
    def first(self) -> "_AsyncFakeLocator":
        return self

    async def count(self) -> int:
        return 1 if self._page.has_selector(self._selector) else 0

    async def click(self) -> None:
        self._page.clicked.append(self._selector)

    async def inner_text(self) -> str:
        return self._page.body_text


class _AsyncFakeContext:
    def __init__(self, *, cookies: list[dict[str, str]] | None = None) -> None:
        self._cookies = list(cookies or [])

    async def cookies(self, *_args, **_kwargs) -> list[dict[str, str]]:
        return list(self._cookies)


class _AsyncFakePage:
    def __init__(
        self,
        *,
        url: str,
        selectors: set[str] | None = None,
        body_text: str = "",
        context: _AsyncFakeContext | None = None,
    ) -> None:
        self.url = url
        self._selectors = set(selectors or set())
        self.body_text = body_text
        self.clicked: list[str] = []
        self.context = context or _AsyncFakeContext()

    def locator(self, selector: str) -> _AsyncFakeLocator:
        return _AsyncFakeLocator(self, selector)

    def has_selector(self, selector: str) -> bool:
        if selector == "body":
            return bool(self.body_text)
        return selector in self._selectors

    async def wait_for_selector(self, selector: str, timeout: int = 0) -> bool:
        del timeout
        selectors = [item.strip() for item in selector.split(",")]
        if any(self.has_selector(item) for item in selectors):
            return True
        raise RuntimeError("selector_not_found")


def _configure_health_store_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    data_dir = tmp_path / "accounts-runtime"
    monkeypatch.setattr(health_store, "DATA_DIR", data_dir)
    monkeypatch.setattr(health_store, "HEALTH_FILE", data_dir / "account_health.json")
    monkeypatch.setattr(health_store, "DB_FILE", data_dir / "account_runtime_state.sqlite3")
    monkeypatch.setattr(
        health_store,
        "LEGACY_HEALTH_BACKUP_FILE",
        data_dir / "account_health.legacy.json",
    )
    return data_dir


def test_detect_account_health_sync_classifies_viva() -> None:
    page = _FakePage(
        url="https://www.instagram.com/",
        selectors={"svg[aria-label='Home']"},
    )

    state, reason = detect_account_health_sync(page)

    assert state == "VIVA"
    assert reason == "instagram_ui_ready"


def test_detect_account_health_sync_classifies_no_activa() -> None:
    page = _FakePage(
        url="https://www.instagram.com/accounts/login/",
        selectors={"input[name='username']"},
    )

    state, reason = detect_account_health_sync(page)

    assert state == "NO ACTIVA"
    assert reason == "redirected_to_login"


def test_detect_account_health_sync_classifies_muerta() -> None:
    page = _FakePage(
        url="https://www.instagram.com/challenge/",
        body_text="Challenge required",
    )

    state, reason = detect_account_health_sync(page)

    assert state == "MUERTA"
    assert reason == "challenge"


def test_detect_account_health_sync_uses_auth_cookies_when_ui_is_not_ready() -> None:
    page = _FakePage(
        url="https://www.instagram.com/",
        context=_FakeContext(
            cookies=[
                {"name": "sessionid", "value": "session-value"},
                {"name": "ds_user_id", "value": "42"},
            ]
        ),
    )

    state, reason = detect_account_health_sync(page)

    assert state == "VIVA"
    assert reason == "auth_cookies_without_ui"


def test_detect_account_health_async_uses_auth_cookies_when_ui_is_not_ready() -> None:
    page = _AsyncFakePage(
        url="https://www.instagram.com/",
        context=_AsyncFakeContext(
            cookies=[
                {"name": "sessionid", "value": "session-value"},
                {"name": "ds_user_id", "value": "42"},
            ]
        ),
    )

    state, reason = asyncio.run(detect_account_health_async(page))

    assert state == "VIVA"
    assert reason == "auth_cookies_without_ui"


def test_health_store_migrates_legacy_json_to_sqlite(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = _configure_health_store_paths(monkeypatch, tmp_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    health_store.HEALTH_FILE.write_text(
        json.dumps(
            {
                "acc-1": {
                    "state": "NO ACTIVA",
                    "reason": "redirected_to_login",
                    "timestamp": health_store._now_iso(),
                }
            }
        ),
        encoding="utf-8",
    )

    health_store._ensure_schema()

    badge, expired = health_store.get_badge("acc-1")
    reason, reason_expired = health_store.get_reason("acc-1")

    assert health_store.DB_FILE.exists() is True
    assert badge == "NO ACTIVA"
    assert expired is False
    assert reason == "redirected_to_login"
    assert reason_expired is False
    assert health_store.HEALTH_FILE.exists() is False
    assert health_store.LEGACY_HEALTH_BACKUP_FILE.exists() is True


def test_connected_status_fast_uses_sqlite_runtime_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = {"username": "acc-1", "connected": True}
    mark_calls: list[tuple[str, bool, bool]] = []

    monkeypatch.setattr(accounts, "has_session", lambda _username: False)
    monkeypatch.setattr(accounts, "_has_playwright_session", lambda _username: True)
    monkeypatch.setattr(
        accounts.health_store,
        "get_connected",
        lambda _username: (False, "sqlite", "login_failed"),
    )
    monkeypatch.setattr(
        accounts,
        "mark_connected",
        lambda username, connected, *, invalidate_health=True: mark_calls.append(
            (username, connected, invalidate_health)
        ),
    )

    connected = accounts.connected_status(account, fast=True, persist=False)

    assert connected is False
    assert account["connected"] is False
    assert mark_calls == []


def test_sync_playwright_login_result_marks_success_as_viva(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_health_store_paths(monkeypatch, tmp_path)
    health_store._ensure_schema()
    health_store.mark_blocked("acc-1", reason="checkpoint")
    health_store.set_login_progress("acc-1", health_store.LOGIN_PROGRESS_CONFIRMING_INBOX)

    monkeypatch.setattr(
        accounts,
        "mark_connected",
        lambda username, connected, *, invalidate_health=True: health_store.set_connected(
            username,
            connected,
            source="test.mark_connected",
        ),
    )

    accounts._sync_playwright_login_result(
        {"username": "acc-1", "status": "ok"},
        clear_stale_session_on_failure=True,
    )

    badge, expired = health_store.get_badge("acc-1")
    connected, source, reason = health_store.get_connected("acc-1")
    progress = health_store.get_login_progress("acc-1")

    assert badge == "VIVA"
    assert expired is False
    assert connected is True
    assert source == "test.mark_connected"
    assert reason == ""
    assert progress["active"] is False


def test_badge_for_display_without_session_never_returns_sin_chequeo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(accounts, "_has_playwright_session", lambda _username: False)
    monkeypatch.setattr(
        accounts.health_store,
        "mark_session_expired",
        lambda _username, *, reason="": "NO ACTIVA",
    )

    badge, expired = accounts._badge_for_display({"username": "acc-1"})

    assert badge == "NO ACTIVA"
    assert expired is False


def test_badge_for_display_uses_connected_flag_without_runtime_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(accounts, "_health_cached", lambda _username: (None, True))
    monkeypatch.setattr(accounts, "_has_playwright_session", lambda _username: True)
    monkeypatch.setattr(
        accounts,
        "_check_playwright_session",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no runtime probe expected")),
    )
    monkeypatch.setattr(
        accounts.health_store,
        "mark_alive",
        lambda _username, *, reason="": "VIVA",
    )

    badge, expired = accounts._badge_for_display({"username": "acc-1", "connected": True})

    assert badge == "VIVA"
    assert expired is False


@pytest.mark.parametrize("state", ["NO ACTIVA", "MUERTA"])
def test_execute_filter_list_async_skips_non_usable_accounts(
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    warnings: list[str] = []
    list_data = {"items": [{"status": "PENDING", "username": "lead-1"}]}
    run_cfg = SimpleNamespace(
        accounts=["acc-1"],
        concurrency=1,
        delay_min=0,
        delay_max=0,
        max_runtime_seconds=None,
        max_items=0,
    )

    monkeypatch.setattr(health_store, "get_badge", lambda _username: (state, False))

    async def _update_item(*_args, **_kwargs) -> None:
        raise AssertionError("update_item should not run when all accounts are filtered out by health")

    result = asyncio.run(
        leads_filter_pipeline.execute_filter_list_async(
            list_data,
            SimpleNamespace(),
            run_cfg,
            resolve_accounts=lambda _requested: [{"username": "acc-1"}],
            refresh_list_stats=lambda _payload: None,
            save_filter_list=lambda _payload: None,
            reset_runtime_stop_event=lambda: None,
            should_stop=lambda _event: False,
            warn=warnings.append,
            log_filter_result=lambda *_args, **_kwargs: None,
            update_item=_update_item,
        )
    )

    assert result is False
    assert any("cuenta excluida por salud cacheada @acc-1" in message for message in warnings)
    assert any("No hay cuentas saludables" in message for message in warnings)


@pytest.mark.parametrize(
    ("badge", "expected_state"),
    [
        ("MUERTA", "banned"),
        ("NO ACTIVA", "login_required"),
    ],
)
def test_inbox_manager_blocks_canonical_unusable_states(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    badge: str,
    expected_state: str,
) -> None:
    manager = InboxManager(tmp_path)

    monkeypatch.setattr("core.inbox.inbox_manager.health_store.get_badge", lambda _account_id: (badge, False))
    monkeypatch.setattr(manager, "_account_profile_ready", lambda _account_id: True)

    state, reason = manager._map_badge_to_health({"username": "acc-1"})

    assert state == expected_state
    assert reason == badge


@pytest.mark.parametrize(
    ("reason", "status_code", "expected_state"),
    [
        ("http_403_login_required", 403, "NO ACTIVA"),
        ("http_403_checkpoint_required", 403, "MUERTA"),
        ("http_401", 401, "NO ACTIVA"),
    ],
)
def test_runtime_health_isolation_maps_to_canonical_account_states(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    status_code: int,
    expected_state: str,
) -> None:
    updates: list[tuple[str, str, str]] = []
    warnings: list[str] = []
    runtime = leads_filter_pipeline.AccountRuntime(
        account={"username": "acc-1"},
        username="acc-1",
        svc=None,
        ctx=None,
        page=object(),
        profile_gate=leads_filter_pipeline.SlotGate(1),
        image_gate=leads_filter_pipeline.SlotGate(1),
    )

    monkeypatch.setattr(
        health_store,
        "mark_session_expired",
        lambda username, *, reason="": updates.append((username, "NO ACTIVA", reason)) or "NO ACTIVA",
    )
    monkeypatch.setattr(
        health_store,
        "mark_blocked",
        lambda username, *, reason="": updates.append((username, "MUERTA", reason)) or "MUERTA",
    )

    isolated = leads_filter_pipeline._isolate_runtime_for_account_health(
        runtime,
        status_code=status_code,
        reason=reason,
        warn=warnings.append,
    )

    assert isolated is True
    assert runtime.disabled_reason == reason
    assert updates == [("acc-1", expected_state, reason)]
    assert any("cuenta aislada del run @acc-1" in message for message in warnings)


def test_pick_profile_runtime_skips_isolated_accounts() -> None:
    isolated_runtime = leads_filter_pipeline.AccountRuntime(
        account={"username": "acc-dead"},
        username="acc-dead",
        svc=None,
        ctx=None,
        page=object(),
        profile_gate=leads_filter_pipeline.SlotGate(1),
        image_gate=leads_filter_pipeline.SlotGate(1),
        profile_limiter=leads_filter_pipeline.ProfileLimiter(
            "acc-dead",
            daily_budget=0,
            delay_min_seconds=0.1,
            delay_max_seconds=0.1,
        ),
        disabled_reason="http_403_checkpoint_required",
    )
    healthy_runtime = leads_filter_pipeline.AccountRuntime(
        account={"username": "acc-ok"},
        username="acc-ok",
        svc=None,
        ctx=None,
        page=object(),
        profile_gate=leads_filter_pipeline.SlotGate(1),
        image_gate=leads_filter_pipeline.SlotGate(1),
        profile_limiter=leads_filter_pipeline.ProfileLimiter(
            "acc-ok",
            daily_budget=0,
            delay_min_seconds=0.1,
            delay_max_seconds=0.1,
        ),
    )

    runtime, wait_seconds = asyncio.run(
        leads_filter_pipeline._v2_pick_profile_runtime(
            deque([isolated_runtime, healthy_runtime]),
        )
    )

    assert runtime is healthy_runtime
    assert wait_seconds == 0.0


def test_execute_filter_list_async_stops_when_last_runtime_becomes_unusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warnings: list[str] = []
    health_updates: list[tuple[str, str, str]] = []
    fetch_calls: list[str] = []
    list_data = {
        "items": [
            {"status": "PENDING", "username": "lead-1"},
            {"status": "PENDING", "username": "lead-2"},
        ]
    }
    filter_cfg = SimpleNamespace(
        classic=None,
        text=SimpleNamespace(enabled=False, criteria="", state="disabled"),
        image=SimpleNamespace(enabled=False, prompt="", state="disabled"),
    )
    run_cfg = SimpleNamespace(
        accounts=["acc-1"],
        concurrency=1,
        delay_min=0,
        delay_max=0,
        max_runtime_seconds=None,
        max_items=0,
    )
    runtime = leads_filter_pipeline.AccountRuntime(
        account={"username": "acc-1"},
        username="acc-1",
        svc=None,
        ctx=None,
        page=object(),
        profile_gate=leads_filter_pipeline.SlotGate(1),
        image_gate=leads_filter_pipeline.SlotGate(1),
        profile_limiter=leads_filter_pipeline.ProfileLimiter(
            "acc-1",
            daily_budget=0,
            delay_min_seconds=0.1,
            delay_max_seconds=0.1,
        ),
    )

    monkeypatch.setenv("LEADS_SESSION_PREFLIGHT", "0")
    monkeypatch.setattr(health_store, "get_badge", lambda _username: ("VIVA", False))
    monkeypatch.setattr(
        health_store,
        "mark_blocked",
        lambda username, *, reason="": health_updates.append((username, "MUERTA", reason)) or "MUERTA",
    )
    monkeypatch.setattr(
        leads_filter_pipeline,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )

    async def _fake_init_runtime(*_args, **_kwargs):
        return runtime

    async def _fake_fetch(runtime_arg, username):
        fetch_calls.append(username)
        assert runtime_arg is runtime
        if len(fetch_calls) > 1:
            raise AssertionError("runtime aislado no debe reutilizarse")
        return None, 403, "http_403_checkpoint_required"

    async def _update_item(list_payload, idx, account, evaluation, lock):
        async with lock:
            item = list_payload["items"][idx]
            item["status"] = "QUALIFIED" if evaluation.passed else "DISCARDED"
            item["result"] = item["status"]
            item["reason"] = evaluation.primary_reason
            item["account"] = account
            item["updated_at"] = "2026-03-12T00:00:00"

    def _refresh_list_stats(payload):
        items = payload.get("items") or []
        qualified = sum(1 for item in items if item.get("status") == "QUALIFIED")
        discarded = sum(1 for item in items if item.get("status") == "DISCARDED")
        processed = qualified + discarded
        payload["total"] = len(items)
        payload["processed"] = processed
        payload["qualified"] = qualified
        payload["discarded"] = discarded
        payload["status"] = "done" if processed >= len(items) else "pending"

    monkeypatch.setattr(leads_filter_pipeline, "_v2_init_runtime", _fake_init_runtime)
    monkeypatch.setattr(leads_filter_pipeline, "fetch_profile_json_with_meta", _fake_fetch)

    stopped = asyncio.run(
        leads_filter_pipeline.execute_filter_list_async(
            list_data,
            filter_cfg,
            run_cfg,
            resolve_accounts=lambda _requested: [{"username": "acc-1"}],
            refresh_list_stats=_refresh_list_stats,
            save_filter_list=lambda _payload: None,
            reset_runtime_stop_event=lambda: None,
            should_stop=lambda event: event.is_set(),
            warn=warnings.append,
            log_filter_result=lambda *_args, **_kwargs: None,
            update_item=_update_item,
        )
    )

    assert stopped is True
    assert fetch_calls == ["lead-1"]
    assert runtime.disabled_reason == "http_403_checkpoint_required"
    assert health_updates == [("acc-1", "MUERTA", "http_403_checkpoint_required")]
    assert list_data["items"][0]["status"] == "DISCARDED"
    assert list_data["items"][0]["reason"] == "http_403_checkpoint_required"
    assert list_data["items"][1]["status"] == "PENDING"
    assert any("cuenta aislada del run @acc-1" in message for message in warnings)
    assert any("sin cuentas seguras/utilizables" in message for message in warnings)
