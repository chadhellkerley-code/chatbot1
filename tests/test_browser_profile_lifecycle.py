from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

import src.playwright_service as playwright_service
from src.browser_profile_lifecycle import load_profile_lifecycle
from src.browser_profile_paths import browser_profile_lifecycle_diagnostics_path
from src.runtime import playwright_runtime


class _FakePersistentContext:
    def __init__(self) -> None:
        self.default_timeout: int | None = None
        self.closed = False
        self._close_handlers: list[Any] = []

    def on(self, event: str, handler: Any) -> None:
        if event == "close":
            self._close_handlers.append(handler)

    def set_default_timeout(self, value: int) -> None:
        self.default_timeout = value

    async def close(self) -> None:
        self.closed = True
        for handler in list(self._close_handlers):
            handler()


class _FailingStorageStateContext:
    async def storage_state(self, *, path: str) -> None:
        raise RuntimeError(f"disk_full:{path}")


def _load_diagnostic_events(profile_dir: Path) -> list[dict[str, Any]]:
    path = browser_profile_lifecycle_diagnostics_path(profile_dir)
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@pytest.fixture(autouse=True)
def _reset_profile_lifecycle_state() -> None:
    playwright_runtime._PERSISTENT_PROFILE_OWNERS.clear()
    yield
    playwright_runtime._PERSISTENT_PROFILE_OWNERS.clear()


def _install_persistent_launch_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_start(self, **_kwargs: Any) -> None:
        self._playwright = object()

    async def _fake_launch_persistent_context(_playwright: Any, **_kwargs: Any) -> _FakePersistentContext:
        return _FakePersistentContext()

    monkeypatch.setattr(playwright_runtime.PlaywrightRuntime, "start", _fake_start)
    monkeypatch.setattr(playwright_runtime, "_launch_persistent_context", _fake_launch_persistent_context)


def test_clean_open_close_persists_clean_lifecycle_and_clears_markers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_persistent_launch_stubs(monkeypatch)
    profile_dir = tmp_path / "tester"
    runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)

    context = asyncio.run(
        runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
            subsystem="inbox",
        )
    )

    open_metadata = load_profile_lifecycle(profile_dir)
    assert open_metadata["lifecycle_state"] == "open"
    assert open_metadata["open_count"] == 1
    assert open_metadata["subsystem"] == "inbox"
    assert open_metadata["mode"] == "headless"

    asyncio.run(context.close())

    closed_metadata = load_profile_lifecycle(profile_dir)
    assert closed_metadata["lifecycle_state"] == "clean"
    assert closed_metadata["open_count"] == 0
    assert closed_metadata["owners"] == []
    assert closed_metadata["last_clean_shutdown"]
    assert playwright_runtime._PERSISTENT_PROFILE_OWNERS == {}


def test_next_launch_detects_previous_unclean_profile_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_persistent_launch_stubs(monkeypatch)
    profile_dir = tmp_path / "tester"

    first_runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    _ = asyncio.run(
        first_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
            subsystem="inbox",
        )
    )

    # Simulate a process crash/restart that lost in-memory ownership but left
    # the persisted lifecycle state as open.
    playwright_runtime._PERSISTENT_PROFILE_OWNERS.clear()

    second_runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)
    recovered_context = asyncio.run(
        second_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=True,
            subsystem="campaign",
        )
    )

    metadata = load_profile_lifecycle(profile_dir)
    events = _load_diagnostic_events(profile_dir)

    assert any(event["event_type"] == "profile_unclean_shutdown_detected" for event in events)
    assert any(event["event_type"] == "lifecycle_state_recovered" for event in events)
    assert metadata["lifecycle_state"] == "open"
    assert metadata["subsystem"] == "campaign"
    assert metadata["last_unclean_shutdown_reason"] == "previous_state_open"

    asyncio.run(recovered_context.close())


def test_storage_state_save_failure_is_logged(tmp_path: Path) -> None:
    service = playwright_service.PlaywrightService(headless=True, base_profiles=tmp_path, subsystem="campaign")
    profile_dir = tmp_path / "tester"

    with pytest.raises(RuntimeError, match="disk_full"):
        asyncio.run(service.save_storage_state(_FailingStorageStateContext(), profile_dir / "storage_state.json"))

    events = _load_diagnostic_events(profile_dir)
    assert events[-1]["event_type"] == "storage_state_save_failed"
    assert events[-1]["subsystem"] == "campaign"
    assert events[-1]["reason_code"] == "storage_state_save_failed"


def test_different_accounts_keep_isolated_lifecycle_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_persistent_launch_stubs(monkeypatch)
    runtime = playwright_runtime.PlaywrightRuntime(headless=True, owner_module=__name__)

    first_context = asyncio.run(
        runtime.get_context(
            account="tester_a",
            profile_dir=tmp_path / "tester_a",
            mode="persistent",
            force_headless=True,
            subsystem="inbox",
        )
    )
    second_context = asyncio.run(
        runtime.get_context(
            account="tester_b",
            profile_dir=tmp_path / "tester_b",
            mode="persistent",
            force_headless=True,
            subsystem="warmup",
        )
    )

    asyncio.run(first_context.close())

    first_metadata = load_profile_lifecycle(tmp_path / "tester_a")
    second_metadata = load_profile_lifecycle(tmp_path / "tester_b")

    assert first_metadata["lifecycle_state"] == "clean"
    assert second_metadata["lifecycle_state"] == "open"
    assert first_metadata["account_username"] == "tester_a"
    assert second_metadata["account_username"] == "tester_b"

    asyncio.run(second_context.close())


def test_same_account_across_subsystems_preserves_lifecycle_consistency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_persistent_launch_stubs(monkeypatch)
    profile_dir = tmp_path / "tester"

    inbox_runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)
    campaign_runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)

    inbox_context = asyncio.run(
        inbox_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=False,
            subsystem="inbox",
        )
    )
    asyncio.run(inbox_context.close())

    campaign_context = asyncio.run(
        campaign_runtime.get_context(
            account="tester",
            profile_dir=profile_dir,
            mode="persistent",
            force_headless=False,
            subsystem="campaign",
        )
    )

    metadata = load_profile_lifecycle(profile_dir)
    events = _load_diagnostic_events(profile_dir)

    assert metadata["lifecycle_state"] == "open"
    assert metadata["subsystem"] == "campaign"
    assert metadata["mode"] == "headful"
    assert metadata["last_clean_shutdown"]
    assert not any(event["event_type"] == "profile_unclean_shutdown_detected" for event in events)

    asyncio.run(campaign_context.close())
