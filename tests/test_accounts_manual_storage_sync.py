from __future__ import annotations

import asyncio
from types import SimpleNamespace

import core.accounts as accounts_module


def test_manual_lifecycle_disables_background_storage_sync_for_visible_sessions(monkeypatch) -> None:
    lifecycle = accounts_module._ManualPlaywrightLifecycle()
    page = SimpleNamespace(
        url="https://www.instagram.com/accounts/edit/",
        goto=lambda *args, **kwargs: None,
    )
    ctx = SimpleNamespace()
    persisted: list[tuple[str, bool]] = []
    waited: list[dict[str, object]] = []

    async def _ensure_context(**kwargs):
        return ctx

    async def _ensure_page(**kwargs):
        return page

    async def _classify_manual_surface(**kwargs):
        return ("unknown", "surface_not_classified")

    async def _update_health_from_surface(**kwargs):
        return None

    async def _persist_storage_state(username: str, *, force: bool = False):
        persisted.append((username, force))

    async def _wait_until_manual_end(**kwargs):
        waited.append(dict(kwargs))

    async def _goto(url: str, **kwargs):
        page.url = url

    page.goto = _goto

    monkeypatch.setattr(lifecycle, "_ensure_context", _ensure_context)
    monkeypatch.setattr(lifecycle, "_ensure_page", _ensure_page)
    monkeypatch.setattr(lifecycle, "_classify_manual_surface", _classify_manual_surface)
    monkeypatch.setattr(lifecycle, "_update_health_from_surface", _update_health_from_surface)
    monkeypatch.setattr(lifecycle, "_persist_storage_state", _persist_storage_state)
    monkeypatch.setattr(lifecycle, "_wait_until_manual_end", _wait_until_manual_end)
    monkeypatch.setattr(
        accounts_module.asyncio,
        "create_task",
        lambda coro: (_ for _ in ()).throw(AssertionError("background sync should stay disabled")),
    )

    result = asyncio.run(
        lifecycle.open_manual_session(
            account={"username": "tester"},
            start_url="https://www.instagram.com/accounts/edit/",
            action_label="Cambiar username",
            max_seconds=None,
            restore_page_if_closed=False,
        )
    )

    assert result["opened"] is True
    assert persisted == [("tester", True)]
    assert waited and waited[0]["restore_page_if_closed"] is False
