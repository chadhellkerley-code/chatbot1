from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.runtime import playwright_runtime


class _FakePersistentContext:
    def __init__(self) -> None:
        self.default_timeout: int | None = None
        self._close_handlers: list[Any] = []
        self.closed = False

    def on(self, event: str, handler: Any) -> None:
        if event == "close":
            self._close_handlers.append(handler)

    def set_default_timeout(self, value: int) -> None:
        self.default_timeout = value

    async def close(self) -> None:
        self.closed = True
        for handler in list(self._close_handlers):
            handler()


@pytest.fixture(autouse=True)
def _reset_profile_ownership() -> None:
    playwright_runtime._PERSISTENT_PROFILE_OWNERS.clear()
    yield
    playwright_runtime._PERSISTENT_PROFILE_OWNERS.clear()


def _install_persistent_launch_stubs(monkeypatch: pytest.MonkeyPatch, calls: list[dict[str, Any]]) -> None:
    async def _fake_start(self, **_kwargs: Any) -> None:
        self._playwright = object()

    async def _fake_launch_persistent_context(_playwright: Any, **kwargs: Any) -> _FakePersistentContext:
        calls.append(dict(kwargs))
        return _FakePersistentContext()

    monkeypatch.setattr(playwright_runtime.PlaywrightRuntime, "start", _fake_start)
    monkeypatch.setattr(playwright_runtime, "_launch_persistent_context", _fake_launch_persistent_context)


def test_same_account_headless_then_headful_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _install_persistent_launch_stubs(monkeypatch, calls)
    profile_dir = tmp_path / "tester"

    headless_runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    headful_runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)

    context = asyncio.run(
        headless_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
        )
    )

    with pytest.raises(playwright_runtime.PersistentProfileOwnershipError) as exc_info:
        asyncio.run(
            headful_runtime.get_context(
                account="tester",
                profile_dir=profile_dir,
                mode="persistent",
                force_headless=False,
            )
        )

    assert exc_info.value.reason_code == "profile_in_use_by_headless"
    assert exc_info.value.conflict_code == "profile_mode_conflict"
    assert exc_info.value.handoff_code == "profile_handoff_required"
    assert len(calls) == 1
    asyncio.run(context.close())


def test_same_account_headful_then_headless_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _install_persistent_launch_stubs(monkeypatch, calls)
    profile_dir = tmp_path / "tester"

    headful_runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)
    headless_runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)

    context = asyncio.run(
        headful_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=False,
        )
    )

    with pytest.raises(playwright_runtime.PersistentProfileOwnershipError) as exc_info:
        asyncio.run(
            headless_runtime.get_context(
                account="tester",
                profile_dir=profile_dir,
                mode="persistent",
                force_headless=True,
            )
        )

    assert exc_info.value.reason_code == "profile_in_use_by_headful"
    assert exc_info.value.conflict_code == "profile_mode_conflict"
    assert exc_info.value.handoff_code == "profile_handoff_required"
    assert len(calls) == 1
    asyncio.run(context.close())


def test_persistent_profile_can_reopen_in_opposite_mode_after_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _install_persistent_launch_stubs(monkeypatch, calls)
    profile_dir = tmp_path / "tester"

    headless_runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    headful_runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)

    first_context = asyncio.run(
        headless_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
        )
    )
    asyncio.run(first_context.close())

    second_context = asyncio.run(
        headful_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=False,
        )
    )

    assert len(calls) == 2
    assert calls[0]["user_data_dir"] == str(profile_dir)
    assert calls[1]["user_data_dir"] == str(profile_dir)
    assert calls[0]["headless"] is True
    assert calls[1]["headless"] is False
    asyncio.run(second_context.close())


def test_same_mode_persistent_launches_for_same_account_raise_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _install_persistent_launch_stubs(monkeypatch, calls)
    profile_dir = tmp_path / "tester"

    runtime_a = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    runtime_b = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)

    context_a = asyncio.run(
        runtime_a.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
        )
    )

    with pytest.raises(playwright_runtime.PersistentProfileOwnershipError) as exc_info:
        asyncio.run(
            runtime_b.get_context(
                account="tester",
                profile_dir=profile_dir,
                mode="persistent",
                force_headless=True,
            )
        )

    assert exc_info.type is playwright_runtime.PersistentProfileOwnershipError
    assert exc_info.value.reason_code == "profile_in_use_by_headless"
    assert exc_info.value.conflict_code == "profile_mode_conflict"
    assert exc_info.value.handoff_code == "profile_handoff_required"
    assert exc_info.value.requested_mode == "headless"
    assert exc_info.value.active_mode == "headless"
    assert len(calls) == 1
    assert calls[0]["headless"] is True
    asyncio.run(context_a.close())


def test_different_accounts_keep_independent_profile_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _install_persistent_launch_stubs(monkeypatch, calls)

    runtime_a = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    runtime_b = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)

    context_a = asyncio.run(
        runtime_a.get_context(
            account="tester_a",
            profile_dir=tmp_path / "tester_a",
            mode="persistent",
            force_headless=True,
        )
    )
    context_b = asyncio.run(
        runtime_b.get_context(
            account="tester_b",
            profile_dir=tmp_path / "tester_b",
            mode="persistent",
            force_headless=False,
        )
    )

    assert len(calls) == 2
    assert calls[0]["user_data_dir"] != calls[1]["user_data_dir"]
    asyncio.run(context_a.close())
    asyncio.run(context_b.close())
