from __future__ import annotations

import asyncio

from runtime.runtime import request_stop, reset_stop_event
from src.auth import onboarding


async def _fast_result() -> str:
    await asyncio.sleep(0)
    return "ok"


def test_onboarding_run_async_ignores_stale_stop_event() -> None:
    request_stop("stale stop event for accounts login")
    try:
        assert onboarding._run_async(_fast_result()) == "ok"
    finally:
        reset_stop_event()


def test_onboarding_run_async_uses_runtime_sync_runner(monkeypatch) -> None:
    called: dict[str, object] = {}

    def _fake_runner(coro, **kwargs):
        called["coro"] = coro
        called["kwargs"] = dict(kwargs)
        try:
            coro.close()
        except Exception:
            pass
        return "runner-ok"

    monkeypatch.setattr(onboarding, "run_coroutine_sync", _fake_runner)

    async def _sample() -> str:
        return "never-direct"

    assert onboarding._run_async(_sample()) == "runner-ok"
    assert called["coro"] is not None
    assert called["kwargs"] == {"ignore_stop": True}
