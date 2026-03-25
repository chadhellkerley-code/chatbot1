from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import core.accounts as accounts_module
from runtime.runtime import request_stop, reset_stop_event


def test_open_playwright_manual_session_ignores_stale_global_stop(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class _FakeLifecycle:
        async def open_manual_session(self, **kwargs):
            calls.append(dict(kwargs))
            return {"opened": True, "username": "tester"}

    monkeypatch.setattr(accounts_module, "_manual_lifecycle", lambda: _FakeLifecycle())

    request_stop("stale stop from previous automation")
    try:
        result = accounts_module._open_playwright_manual_session(
            {"username": "tester"},
            start_url="https://www.instagram.com/",
            action_label="Navegar cuenta",
        )
    finally:
        reset_stop_event()

    assert result == {"opened": True, "username": "tester"}
    assert calls == [
        {
            "account": {"username": "tester"},
            "start_url": "https://www.instagram.com/",
            "action_label": "Navegar cuenta",
            "max_seconds": None,
            "restore_page_if_closed": True,
        }
    ]


def test_manual_lifecycle_marks_visible_manual_browser_sessions(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeSessionManager:
        def __init__(self, **_kwargs) -> None:
            return None

        async def open_session(self, *, account, proxy, login_func):
            captured["account"] = dict(account)
            captured["proxy"] = proxy
            captured["login_func"] = login_func
            return SimpleNamespace(ctx=SimpleNamespace(), page=SimpleNamespace())

    monkeypatch.setattr(accounts_module, "SessionManager", _FakeSessionManager)

    lifecycle = accounts_module._ManualPlaywrightLifecycle()
    ctx = asyncio.run(
        lifecycle._ensure_context(
            account={"username": "tester"},
            username="tester",
            proxy_payload=None,
            storage_state=None,
        )
    )

    assert ctx is lifecycle._contexts["tester"]
    assert captured["account"] == {
        "username": "tester",
        "manual_visible_browser": True,
    }


def test_manual_lifecycle_closes_session_when_last_page_is_gone(monkeypatch) -> None:
    closed: list[str] = []
    ctx_holder: dict[str, object] = {}

    class _FakeSessionManager:
        def __init__(self, **_kwargs) -> None:
            return None

        async def drop_cached_session(self, key: str) -> None:
            closed.append(key)
            ctx = ctx_holder.get("ctx")
            if ctx is not None:
                await ctx.close()

        def close_all_sessions_sync(self, timeout: float = 0.0) -> None:
            del timeout

    class _FakeContext:
        def __init__(self) -> None:
            self.pages: list[object] = []
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(accounts_module, "SessionManager", _FakeSessionManager)

    lifecycle = accounts_module._ManualPlaywrightLifecycle()
    lifecycle._session_manager = _FakeSessionManager()
    lifecycle._persist_storage_state = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]

    ctx = _FakeContext()
    ctx_holder["ctx"] = ctx
    lifecycle._contexts["tester"] = ctx
    lifecycle._pages["tester"] = SimpleNamespace()
    lifecycle._sessions["tester"] = SimpleNamespace()

    asyncio.run(
        lifecycle._wait_until_manual_end(
            username="tester",
            ctx=ctx,
            page=SimpleNamespace(),
            start_url="https://www.instagram.com/accounts/edit/",
            max_seconds=None,
            restore_page_if_closed=False,
        )
    )

    assert ctx.closed is True
    assert closed == ["tester"]


def test_launch_inbox_filters_proxy_preflight_blocked_accounts_from_menu(monkeypatch, capsys) -> None:
    warnings: list[str] = []

    monkeypatch.setattr(
        accounts_module,
        "_load",
        lambda: [
            {"username": "ready", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-ready"},
            {"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"},
        ],
    )
    monkeypatch.setattr(
        accounts_module,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts if item.get("username") == "ready"],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "quarantined",
                    "message": "proxy quarantined",
                }
            ],
        },
    )
    monkeypatch.setattr(accounts_module, "banner", lambda: None)
    monkeypatch.setattr(accounts_module, "title", lambda _text: None)
    monkeypatch.setattr(accounts_module, "_session_label", lambda _username: "[pw]")
    monkeypatch.setattr(accounts_module, "ask", lambda _prompt="": "")
    monkeypatch.setattr(accounts_module, "warn", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(accounts_module, "press_enter", lambda _msg="": None)

    accounts_module._launch_inbox("alias-a")

    captured = capsys.readouterr().out
    assert "@ready" in captured
    assert "@blocked" not in captured
    assert any("@blocked" in message and "proxy quarantined" in message for message in warnings)


def test_resolve_accounts_for_modifications_skips_proxy_preflight_blocked_accounts(monkeypatch) -> None:
    warnings: list[str] = []

    monkeypatch.setattr(
        accounts_module,
        "_load",
        lambda: [
            {"username": "ready", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-ready"},
            {"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"},
        ],
    )
    monkeypatch.setattr(
        accounts_module,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts if item.get("username") == "ready"],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "inactive",
                    "message": "proxy inactive",
                }
            ],
        },
    )
    monkeypatch.setattr(accounts_module, "warn", lambda message: warnings.append(str(message)))

    resolved = accounts_module._resolve_accounts_for_modifications("alias-a", ["ready", "blocked", "missing"])

    assert resolved[0]["username"] == "ready"
    assert resolved[1] is None
    assert resolved[2] is None
    assert any("@missing" in message for message in warnings)
    assert any("@blocked" in message and "proxy inactive" in message for message in warnings)


def test_open_playwright_manual_session_blocks_proxy_preflight_before_open(monkeypatch) -> None:
    monkeypatch.setattr(
        accounts_module,
        "account_proxy_preflight",
        lambda account: {
            "username": str(account.get("username") or ""),
            "status": "quarantined",
            "blocking": True,
            "message": "proxy quarantined",
        },
    )
    monkeypatch.setattr(
        accounts_module,
        "_manual_lifecycle",
        lambda: (_ for _ in ()).throw(AssertionError("manual lifecycle should not be reached")),
    )

    with pytest.raises(RuntimeError, match="proxy quarantined"):
        accounts_module._open_playwright_manual_session(
            {"username": "tester", "assigned_proxy_id": "proxy-1"},
            start_url="https://www.instagram.com/direct/inbox/",
            action_label="Entrar al inbox",
        )
