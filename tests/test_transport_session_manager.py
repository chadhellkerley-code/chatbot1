from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path

import pytest

from core.proxy_registry import ProxyResolutionError
from src.runtime.playwright_runtime import PersistentProfileOwnershipError
from src.transport.session_manager import (
    ManagedSession,
    NavigationLockedError,
    NavigationOwnershipError,
    SessionManager,
    SyncSessionRuntime,
)


class _PageStub:
    def __init__(self, url: str = "https://www.instagram.com/") -> None:
        self.url = url
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed

    def set_default_timeout(self, _value: int) -> None:
        return None

    def set_default_navigation_timeout(self, _value: int) -> None:
        return None


class _ContextStub:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _ServiceStub:
    def __init__(self) -> None:
        self.closed = False
        self.saved_paths: list[Path] = []

    async def close(self) -> None:
        self.closed = True

    async def save_storage_state(self, _ctx, path: Path) -> None:
        self.saved_paths.append(Path(path))


def _account(username: str = "tester") -> dict:
    return {"username": username}


def _manager(
    *,
    persistent: bool,
    events: list[tuple[str, dict]] | None = None,
    headless: bool = True,
    subsystem: str = "default",
) -> SessionManager:
    return SessionManager(
        headless=headless,
        keep_browser_open_per_account=persistent,
        profiles_root="runtime/browser_profiles",
        normalize_username=lambda value: str(value or "").strip().lstrip("@"),
        log_event=(lambda event, **kwargs: events.append((event, kwargs))) if events is not None else (lambda *_args, **_kwargs: None),
        subsystem=subsystem,
    )


@pytest.fixture(autouse=True)
def _reset_session_manager_globals() -> None:
    SessionManager._SHARED_SESSIONS.clear()
    SessionManager._OPENING.clear()


def test_open_session_reuses_cached_persistent_session() -> None:
    events: list[tuple[str, dict]] = []
    login_calls = {"count": 0}
    received_accounts: list[dict] = []

    async def login_func(account, *, headless, proxy):
        login_calls["count"] += 1
        received_accounts.append(dict(account))
        assert account["reuse_session_only"] is True
        assert account["validate_reused_session"] is True
        assert headless is True
        assert proxy is None
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, events=events)
    first = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert isinstance(first, ManagedSession)
    assert first.reused is False
    assert second.reused is True
    assert login_calls["count"] == 1
    assert received_accounts == [
        {
            "username": "tester",
            "reuse_session_only": True,
            "validate_reused_session": True,
            "_playwright_subsystem": "default",
        }
    ]
    assert [item[0] for item in events] == ["SESSION_OPEN", "SESSION_REUSE"]


def test_campaign_open_session_skips_reused_session_visual_validation() -> None:
    login_calls = {"count": 0}
    received_accounts: list[dict] = []

    async def login_func(account, *, headless, proxy):
        del headless, proxy
        login_calls["count"] += 1
        received_accounts.append(dict(account))
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, subsystem="campaign")
    session = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert session.reused is False
    assert login_calls["count"] == 1
    assert received_accounts == [
        {
            "username": "tester",
            "reuse_session_only": True,
            "validate_reused_session": False,
            "_playwright_subsystem": "campaign",
        }
    ]


def test_non_persistent_manager_reuses_shared_active_session() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    persistent_manager = _manager(persistent=True, subsystem="inbox")
    ephemeral_manager = _manager(persistent=False, subsystem="inbox")

    sticky = asyncio.run(persistent_manager.open_session(account=_account(), proxy=None, login_func=login_func))
    borrowed = asyncio.run(ephemeral_manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert sticky.reused is False
    assert borrowed.reused is True
    assert borrowed.persistent is True
    assert login_calls["count"] == 1


def test_open_session_does_not_reuse_across_subsystems_for_same_account() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    inbox_manager = _manager(persistent=True, subsystem="inbox")
    campaign_manager = _manager(persistent=True, subsystem="campaign")

    inbox_session = asyncio.run(inbox_manager.open_session(account=_account(), proxy=None, login_func=login_func))
    campaign_session = asyncio.run(campaign_manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert inbox_session.reused is False
    assert campaign_session.reused is False
    assert inbox_session.page is not campaign_session.page
    assert inbox_session.pool_key == "headless:inbox:tester"
    assert campaign_session.pool_key == "headless:campaign:tester"
    assert login_calls["count"] == 2


def test_headful_manual_session_uses_distinct_live_bucket() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, proxy
        login_calls["count"] += 1
        page_url = "https://www.instagram.com/accounts/edit/" if not headless else "https://www.instagram.com/direct/inbox/"
        return _ServiceStub(), _ContextStub(), _PageStub(page_url)

    campaign_manager = _manager(persistent=True, subsystem="campaign", headless=True)
    manual_manager = _manager(persistent=True, subsystem="manual", headless=False)

    campaign_session = asyncio.run(campaign_manager.open_session(account=_account(), proxy=None, login_func=login_func))
    manual_session = asyncio.run(manual_manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert campaign_session.pool_key == "headless:campaign:tester"
    assert manual_session.pool_key == "headful:manual:tester"
    assert campaign_session.page is not manual_session.page
    assert login_calls["count"] == 2


def test_finalize_session_closes_ephemeral_session_after_release() -> None:
    manager = _manager(persistent=False)
    captured: dict[str, object] = {}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        svc = _ServiceStub()
        ctx = _ContextStub()
        page = _PageStub("https://www.instagram.com/direct/t/123/")
        captured.update({"svc": svc, "ctx": ctx, "page": page})
        return svc, ctx, page

    session = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    asyncio.run(manager.finalize_session(session, current_url=session.page.url))

    assert captured["ctx"].closed is True
    assert captured["svc"].closed is True


def test_discard_if_unhealthy_drops_persistent_session() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True)
    session = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    asyncio.run(manager.discard_if_unhealthy(session, RuntimeError("fatal"), is_fatal_error=lambda exc: True))
    reopened = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert reopened.reused is False
    assert login_calls["count"] == 2


def test_open_session_reopens_when_proxy_changes() -> None:
    events: list[tuple[str, dict]] = []
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, events=events)
    first_proxy = {"server": "http://proxy-a:8000", "username": "user-a", "password": "pass-a"}
    second_proxy = {"server": "http://proxy-b:8000", "username": "user-b", "password": "pass-b"}

    first = asyncio.run(manager.open_session(account=_account(), proxy=first_proxy, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=second_proxy, login_func=login_func))

    assert first.reused is False
    assert second.reused is False
    assert login_calls["count"] == 2
    assert [item[0] for item in events] == ["SESSION_OPEN", "SESSION_PROXY_REFRESH", "SESSION_OPEN"]


def test_close_all_sessions_sync_closes_only_manager_owned_session() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    owner = _manager(persistent=True)
    borrower = _manager(persistent=False)
    sticky = asyncio.run(owner.open_session(account=_account(), proxy=None, login_func=login_func))
    borrowed = asyncio.run(borrower.open_session(account=_account(), proxy=None, login_func=login_func))

    owner.close_all_sessions_sync(timeout=1.0)

    assert sticky.ctx.closed is False
    assert sticky.svc.closed is False
    asyncio.run(borrower.finalize_session(borrowed, current_url=borrowed.page.url))
    owner.close_all_sessions_sync(timeout=1.0)
    assert sticky.ctx.closed is True
    assert sticky.svc.closed is True
    assert login_calls["count"] == 1


def test_save_storage_state_uses_profiles_root() -> None:
    manager = _manager(persistent=False)
    svc = _ServiceStub()
    session = ManagedSession(
        key="tester",
        svc=svc,
        ctx=_ContextStub(),
        page=_PageStub(),
        persistent=False,
        reused=False,
        lease_id="lease-1",
        pool_key="headless:default:tester",
    )

    asyncio.run(manager.save_storage_state(session, "tester"))

    assert svc.saved_paths == [Path("runtime/browser_profiles") / "tester" / "storage_state.json"]


def test_sync_session_runtime_reuses_same_shared_session() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True)
    runtime = SyncSessionRuntime(
        account=_account(),
        session_manager=manager,
        login_func=login_func,
        proxy_resolver=lambda _account: None,
        open_timeout_seconds=5.0,
    )

    page_a = runtime.open_page(_account())
    page_b = runtime.open_page(_account())

    assert page_a is page_b
    assert login_calls["count"] == 1
    runtime.shutdown(timeout=1.0)


def test_sync_session_runtime_navigation_scope_reuses_outer_owner() -> None:
    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, subsystem="inbox")
    runtime = SyncSessionRuntime(
        account=_account(),
        session_manager=manager,
        login_func=login_func,
        proxy_resolver=lambda _account: None,
        open_timeout_seconds=5.0,
    )

    with runtime.navigation_scope("inbox:send_text:123", timeout=1.0) as outer_page:
        assert runtime._session is not None
        assert runtime._session.navigation_metadata()["current_owner"] == "inbox:send_text:123"
        with runtime.navigation_scope("inbox:baseline:123", timeout=1.0) as inner_page:
            assert inner_page is outer_page
            assert runtime._session is not None
            assert runtime._session.navigation_metadata()["current_owner"] == "inbox:send_text:123"

    assert runtime._session is not None
    assert runtime._session.navigation_metadata()["current_owner"] == ""
    runtime.shutdown(timeout=1.0)


def test_sync_session_runtime_blocks_proxy_preflight_before_login(monkeypatch) -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True)
    runtime = SyncSessionRuntime(
        account={"username": "tester", "assigned_proxy_id": "proxy-a"},
        session_manager=manager,
        login_func=login_func,
        proxy_resolver=lambda _account: None,
        open_timeout_seconds=5.0,
    )
    monkeypatch.setattr(
        "src.transport.session_manager.account_proxy_preflight",
        lambda account: {
            "username": str(account.get("username") or "").strip(),
            "status": "quarantined",
            "message": "proxy quarantined",
            "blocking": True,
        },
    )

    with pytest.raises(RuntimeError, match="proxy quarantined"):
        runtime.open_page({"username": "tester"})

    assert login_calls["count"] == 0


def test_sync_session_runtime_does_not_swallow_proxy_resolution_error() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True)
    runtime = SyncSessionRuntime(
        account={"username": "tester"},
        session_manager=manager,
        login_func=login_func,
        proxy_resolver=lambda _account: (_ for _ in ()).throw(
            ProxyResolutionError("assigned_proxy_quarantined", "proxy-a", "proxy quarantined")
        ),
        open_timeout_seconds=5.0,
    )

    with pytest.raises(ProxyResolutionError, match="proxy quarantined"):
        runtime.open_page({"username": "tester"})

    assert login_calls["count"] == 0


def test_finalize_session_persists_storage_state_for_persistent_session() -> None:
    manager = _manager(persistent=True)
    captured: dict[str, object] = {}

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        svc = _ServiceStub()
        ctx = _ContextStub()
        page = _PageStub("https://www.instagram.com/")
        captured.update({"svc": svc, "ctx": ctx, "page": page})
        return svc, ctx, page

    session = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    asyncio.run(manager.finalize_session(session, current_url=session.page.url))

    svc = captured["svc"]
    ctx = captured["ctx"]
    assert isinstance(svc, _ServiceStub)
    assert isinstance(ctx, _ContextStub)
    assert svc.saved_paths == [Path("runtime/browser_profiles") / "tester" / "storage_state.json"]
    assert ctx.closed is False


def test_open_session_propagates_profile_mode_conflict_reason() -> None:
    events: list[tuple[str, dict]] = []
    profile_dir = Path("runtime/browser_profiles") / "tester"

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        raise PersistentProfileOwnershipError(
            profile_dir=profile_dir,
            requested_mode="headful",
            active_mode="headless",
            runtime_id="runtime-b",
            active_runtime_id="runtime-a",
            owner_module="tests.runtime",
        )

    manager = _manager(persistent=True, events=events, headless=False)

    with pytest.raises(PersistentProfileOwnershipError, match="profile_mode_conflict"):
        asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert len(events) == 1
    assert events[0][0] == "SESSION_OPEN_FAILED"
    payload = events[0][1]
    assert payload["key"] == "tester"
    assert payload["error_type"] == "PersistentProfileOwnershipError"
    assert payload["reason"] == "profile_in_use_by_headless"
    assert payload["conflict_code"] == "profile_mode_conflict"
    assert payload["handoff_code"] == "profile_handoff_required"
    assert payload["requested_mode"] == "headful"
    assert payload["active_mode"] == "headless"
    assert str(payload["profile_dir"]).endswith(str(profile_dir))
    assert "profile_mode_conflict" in str(payload["error"])


def test_proxy_signature_behavior_remains_intact() -> None:
    assert SessionManager.proxy_signature(None) == ""
    assert SessionManager.proxy_signature({"server": "HTTP://Proxy:8080", "username": "Alice", "password": "secret"}) == (
        "http://proxy:8080|alice|secret"
    )
    assert SessionManager.proxy_signature({"url": "proxy.local:9000", "user": "bob", "pass": "pw"}) == (
        "proxy.local:9000|bob|pw"
    )


def test_navigation_lock_blocks_concurrent_owner_until_release() -> None:
    events: list[tuple[str, dict]] = []

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, events=events, subsystem="inbox")
    first = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    first.acquire_navigation("inbox:open_thread:123", 1.0)

    acquired = threading.Event()
    finished = threading.Event()
    wait_elapsed: list[float] = []

    def _worker() -> None:
        started = time.monotonic()
        meta = second.acquire_navigation("campaign:send_message:lead", 2.0)
        wait_elapsed.append(time.monotonic() - started)
        assert meta["current_owner"] == "campaign:send_message:lead"
        acquired.set()
        second.release_navigation("campaign:send_message:lead")
        finished.set()

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()
    time.sleep(0.2)
    assert acquired.is_set() is False

    first.release_navigation("inbox:open_thread:123")
    worker.join(timeout=2.0)

    assert finished.is_set() is True
    assert wait_elapsed and wait_elapsed[0] >= 0.15
    event_names = [event for event, _payload in events]
    assert "navigation_conflict" in event_names
    assert event_names.count("navigation_acquired") >= 2
    assert event_names.count("navigation_released") >= 2


def test_navigation_lock_allows_sequential_owners() -> None:
    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, subsystem="campaign")
    session = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))

    first_meta = session.acquire_navigation("campaign:open_profile:lead-a", 1.0)
    session.release_navigation("campaign:open_profile:lead-a")
    second_meta = session.acquire_navigation("manual:navigate:lead-b", 1.0)
    session.release_navigation("manual:navigate:lead-b")

    assert first_meta["current_owner"] == "campaign:open_profile:lead-a"
    assert second_meta["current_owner"] == "manual:navigate:lead-b"
    assert session.navigation_metadata()["current_owner"] == ""


def test_navigation_lock_times_out_with_structured_error() -> None:
    events: list[tuple[str, dict]] = []

    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, events=events, subsystem="warmup")
    first = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    first.acquire_navigation("warmup:watch_reels", 1.0)

    with pytest.raises(NavigationLockedError) as excinfo:
        second.acquire_navigation("inbox:open_thread:123", 0.1)

    payload = excinfo.value.to_payload()
    assert payload["navigation_locked"] is True
    assert payload["navigation_in_use_by"] == "warmup:watch_reels"
    assert payload["navigation_timeout"] == pytest.approx(0.1, abs=0.05)
    assert [event for event, _payload in events].count("navigation_timeout") == 1

    first.release_navigation("warmup:watch_reels")


def test_navigation_release_rejects_owner_mismatch() -> None:
    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, subsystem="inbox")
    first = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    first.acquire_navigation("inbox:send_text:123", 1.0)

    with pytest.raises(NavigationOwnershipError):
        second.release_navigation("campaign:send_message:lead")

    first.release_navigation("inbox:send_text:123")


def test_navigation_waiter_is_released_when_session_is_dropped() -> None:
    async def login_func(account, *, headless, proxy):
        del account, headless, proxy
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    manager = _manager(persistent=True, subsystem="campaign")
    first = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    second = asyncio.run(manager.open_session(account=_account(), proxy=None, login_func=login_func))
    first.acquire_navigation("campaign:send_message:lead-a", 1.0)

    result: dict[str, str] = {}
    waiter_started = threading.Event()

    def _worker() -> None:
        waiter_started.set()
        try:
            second.acquire_navigation("campaign:send_message:lead-b", 2.0)
            result["status"] = "acquired"
        except RuntimeError as exc:
            result["status"] = str(exc)

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()
    waiter_started.wait(timeout=1.0)
    time.sleep(0.2)

    asyncio.run(manager.drop_cached_session(first.key))
    worker.join(timeout=2.0)

    assert result.get("status") in {"session_dropped", "navigation_session_retired", "navigation_session_missing"}
