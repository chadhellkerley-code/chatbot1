from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

import runtime.runtime_parity as runtime_parity
import src.playwright_service as playwright_service


def _reset_runtime_bootstrap(monkeypatch: pytest.MonkeyPatch, app_root: Path) -> None:
    runtime_parity._BOOTSTRAP_CACHE.clear()
    monkeypatch.setenv("APP_DATA_ROOT", str(app_root))
    monkeypatch.delenv("PROFILES_DIR", raising=False)


def _install_fake_accounts(
    monkeypatch: pytest.MonkeyPatch,
    *,
    accounts: list[dict[str, object]],
) -> list[tuple[str, bool, bool]]:
    calls: list[tuple[str, bool, bool]] = []
    module = types.ModuleType("core.accounts")
    module.list_all = lambda: list(accounts)

    def _mark_connected(username: str, connected: bool, *, invalidate_health: bool = False) -> None:
        calls.append((username, connected, invalidate_health))

    module.mark_connected = _mark_connected
    monkeypatch.setitem(sys.modules, "core.accounts", module)
    return calls


def _configure_preflight_runtime(monkeypatch: pytest.MonkeyPatch, app_root: Path) -> Path:
    _reset_runtime_bootstrap(monkeypatch, app_root)
    fake_browser = app_root / "runtime" / "browsers" / "chrome.exe"
    fake_browser.parent.mkdir(parents=True, exist_ok=True)
    fake_browser.write_text("browser", encoding="utf-8")
    monkeypatch.setenv("PLAYWRIGHT_CHROME_EXECUTABLE", str(fake_browser))
    monkeypatch.setattr(runtime_parity, "_is_file_writable", lambda _path: True)
    monkeypatch.setattr(runtime_parity, "_is_valid_executable", lambda _path: True)
    monkeypatch.setattr(playwright_service, "resolve_playwright_executable", lambda **_kwargs: fake_browser)
    bootstrap = runtime_parity.bootstrap_runtime_env("owner", force=True)
    return Path(str(bootstrap["profiles_dir"]))


def test_playwright_service_and_runtime_parity_share_profiles_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _reset_runtime_bootstrap(monkeypatch, tmp_path)

    bootstrap = runtime_parity.bootstrap_runtime_env("owner", force=True)
    expected = Path(str(bootstrap["profiles_dir"]))

    assert runtime_parity.resolve_profiles_dir(tmp_path) == expected
    assert Path(playwright_service.BASE_PROFILES) == expected
    assert playwright_service.PlaywrightService(headless=True)._base_profiles == expected


def test_playwright_profiles_root_updates_after_bootstrap_even_if_module_was_imported_early(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime_parity._BOOTSTRAP_CACHE.clear()
    monkeypatch.delenv("APP_DATA_ROOT", raising=False)
    monkeypatch.delenv("PROFILES_DIR", raising=False)

    imported_before_bootstrap = Path(playwright_service.BASE_PROFILES)

    bootstrap = runtime_parity.bootstrap_runtime_env("owner", app_root_hint=tmp_path, force=True)
    expected = Path(str(bootstrap["profiles_dir"]))

    assert imported_before_bootstrap != expected
    assert Path(playwright_service.BASE_PROFILES) == expected


def test_run_runtime_preflight_keeps_connected_account_when_storage_state_exists_in_unified_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    profiles_dir = _configure_preflight_runtime(monkeypatch, tmp_path)
    calls = _install_fake_accounts(
        monkeypatch,
        accounts=[{"username": "mrcook9958", "connected": True}],
    )
    storage_state = profiles_dir / "mrcook9958" / "storage_state.json"
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    storage_state.write_text('{"cookies":[],"origins":[]}', encoding="utf-8")

    report = runtime_parity.run_runtime_preflight("owner", sync_connected=True)

    assert report["connected_without_storage_state"] == []
    assert report["disconnected_by_preflight"] == []
    assert calls == []


def test_run_runtime_preflight_degrades_connected_account_when_storage_state_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_preflight_runtime(monkeypatch, tmp_path)
    calls = _install_fake_accounts(
        monkeypatch,
        accounts=[{"username": "mrcook9958", "connected": True}],
    )

    report = runtime_parity.run_runtime_preflight("owner", sync_connected=True)

    assert report["connected_without_storage_state"] == ["mrcook9958"]
    assert report["disconnected_by_preflight"] == ["mrcook9958"]
    assert calls == [("mrcook9958", False, False)]
