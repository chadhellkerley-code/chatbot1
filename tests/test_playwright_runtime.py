from __future__ import annotations

import asyncio

from runtime.runtime import EngineCancellationToken, bind_stop_token, request_stop, restore_stop_token
from src.runtime.playwright_runtime import (
    PlaywrightRuntimeCancelledError,
    PlaywrightRuntimeTimeoutError,
    run_coroutine_sync,
)


def test_run_coroutine_sync_respects_timeout() -> None:
    async def _slow() -> str:
        await asyncio.sleep(0.20)
        return "done"

    try:
        run_coroutine_sync(_slow(), timeout=0.01, ignore_stop=True)
    except PlaywrightRuntimeTimeoutError as exc:
        assert str(exc) == "playwright_operation_timeout"
    else:
        raise AssertionError("run_coroutine_sync should time out")


def test_run_coroutine_sync_respects_bound_stop_token() -> None:
    async def _slow() -> str:
        await asyncio.sleep(0.20)
        return "done"

    token = EngineCancellationToken("test-playwright-stop")
    previous = bind_stop_token(token)
    try:
        request_stop("stop requested by test", token=token)
        try:
            run_coroutine_sync(_slow(), timeout=0.20)
        except PlaywrightRuntimeCancelledError as exc:
            assert str(exc) == "playwright_operation_cancelled"
        else:
            raise AssertionError("run_coroutine_sync should stop when the bound token is cancelled")
    finally:
        restore_stop_token(previous)
