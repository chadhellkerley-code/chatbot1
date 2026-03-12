from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from core.proxy_registry import ProxyResolutionError
from src.transport.session_manager import ManagedSession, SessionManager, SyncSessionRuntime


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


def _manager(*, persistent: bool, events: list[tuple[str, dict]] | None = None) -> SessionManager:
    return SessionManager(
        headless=True,
        keep_browser_open_per_account=persistent,
        profiles_root="runtime/browser_profiles",
        normalize_username=lambda value: str(value or "").strip().lstrip("@"),
        log_event=(lambda event, **kwargs: events.append((event, kwargs))) if events is not None else (lambda *_args, **_kwargs: None),
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
    assert received_accounts == [{"username": "tester", "reuse_session_only": True, "validate_reused_session": True}]
    assert [item[0] for item in events] == ["SESSION_OPEN", "SESSION_REUSE"]


def test_non_persistent_manager_reuses_shared_active_session() -> None:
    login_calls = {"count": 0}

    async def login_func(account, *, headless, proxy):
        login_calls["count"] += 1
        return _ServiceStub(), _ContextStub(), _PageStub("https://www.instagram.com/direct/inbox/")

    persistent_manager = _manager(persistent=True)
    ephemeral_manager = _manager(persistent=False)

    sticky = asyncio.run(persistent_manager.open_session(account=_account(), proxy=None, login_func=login_func))
    borrowed = asyncio.run(ephemeral_manager.open_session(account=_account(), proxy=None, login_func=login_func))

    assert sticky.reused is False
    assert borrowed.reused is True
    assert borrowed.persistent is True
    assert login_calls["count"] == 1


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
        pool_key="headless:tester",
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
