from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.runtime import playwright_runtime
from src.runtime import playwright_resolver
from src.runtime.playwright_runtime import (
    PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
    PLAYWRIGHT_BROWSER_MODE_MANAGED,
)


class _FakeChromium:
    def __init__(self) -> None:
        self.launch_calls: list[dict] = []
        self.persistent_calls: list[dict] = []

    async def launch(self, **kwargs):
        self.launch_calls.append(dict(kwargs))
        if len(self.launch_calls) == 1:
            raise RuntimeError("chrome channel unavailable")
        return {"browser": "ok"}

    async def launch_persistent_context(self, **kwargs):
        self.persistent_calls.append(dict(kwargs))
        if len(self.persistent_calls) == 1:
            raise RuntimeError("chrome channel unavailable")
        return {"context": "ok"}


class _FakePlaywright:
    def __init__(self) -> None:
        self.chromium = _FakeChromium()


class _StrictChromium:
    def __init__(self, *, fail: bool = False, results: list[object] | None = None) -> None:
        self.fail = fail
        self.results = list(results or [])
        self.launch_calls: list[dict] = []
        self.persistent_calls: list[dict] = []

    async def launch(self, **kwargs):
        self.launch_calls.append(dict(kwargs))
        if self.results:
            result = self.results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        if self.fail:
            raise RuntimeError("strict chrome launch failed")
        return {"browser": "ok"}

    async def launch_persistent_context(self, **kwargs):
        self.persistent_calls.append(dict(kwargs))
        if self.results:
            result = self.results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        if self.fail:
            raise RuntimeError("strict chrome launch failed")
        return {"context": "ok"}


class _StrictPlaywright:
    def __init__(self, *, fail: bool = False, results: list[object] | None = None) -> None:
        self.chromium = _StrictChromium(fail=fail, results=results)


def test_launch_browser_default_mode_uses_chrome_only_candidate(monkeypatch, tmp_path: Path) -> None:
    fake_playwright = _StrictPlaywright()
    chrome = tmp_path / "Google" / "Chrome" / "Application" / "chrome.exe"
    chrome.parent.mkdir(parents=True, exist_ok=True)
    chrome.write_bytes(b"fake-google-chrome")
    monkeypatch.setattr(
        playwright_runtime,
        "_chrome_only_launch_candidates",
        lambda executable_path, headless=False: [("Google Chrome", str(chrome))],
    )
    monkeypatch.setattr(
        playwright_runtime,
        "resolve_playwright_chromium_executable",
        lambda headless=False: tmp_path / "chromium.exe",
    )

    browser = asyncio.run(playwright_runtime._launch_browser(fake_playwright, headless=False))

    assert browser == {"browser": "ok"}
    assert fake_playwright.chromium.launch_calls == [
        {
            "headless": False,
            "slow_mo": 0,
            "args": playwright_runtime.PLAYWRIGHT_BASE_FLAGS,
            "executable_path": str(chrome),
        }
    ]


def test_launch_persistent_context_default_mode_uses_chrome_only_candidate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fake_playwright = _StrictPlaywright()
    chrome = tmp_path / "Google" / "Chrome" / "Application" / "chrome.exe"
    chrome.parent.mkdir(parents=True, exist_ok=True)
    chrome.write_bytes(b"fake-google-chrome")
    monkeypatch.setattr(
        playwright_runtime,
        "_chrome_only_launch_candidates",
        lambda executable_path, headless=False: [("Google Chrome", str(chrome))],
    )
    monkeypatch.setattr(
        playwright_runtime,
        "resolve_playwright_chromium_executable",
        lambda headless=False: tmp_path / "chromium.exe",
    )

    context = asyncio.run(
        playwright_runtime._launch_persistent_context(
            fake_playwright,
            user_data_dir=Path("C:/profiles/worker_one"),
            headless=False,
        )
    )

    assert context == {"context": "ok"}
    assert fake_playwright.chromium.persistent_calls == [
        {
            "user_data_dir": str(Path("C:/profiles/worker_one")),
            "headless": False,
            "args": playwright_runtime.PLAYWRIGHT_BASE_FLAGS,
            "executable_path": str(chrome),
        }
    ]


def test_launch_browser_managed_uses_playwright_executable_without_chrome_channel(monkeypatch, tmp_path: Path) -> None:
    fake_playwright = _StrictPlaywright()
    managed = tmp_path / "managed.exe"
    managed.write_bytes(b"managed")

    monkeypatch.setattr(
        playwright_runtime,
        "resolve_playwright_chromium_executable",
        lambda headless=False: managed,
    )

    browser = asyncio.run(
        playwright_runtime._launch_browser(
            fake_playwright,
            headless=False,
            browser_mode=PLAYWRIGHT_BROWSER_MODE_MANAGED,
        )
    )

    assert browser == {"browser": "ok"}
    assert fake_playwright.chromium.launch_calls == [
        {
            "headless": False,
            "slow_mo": 0,
            "args": playwright_runtime.PLAYWRIGHT_BASE_FLAGS,
            "executable_path": str(managed),
        }
    ]


def test_launch_persistent_context_managed_uses_playwright_executable_without_chrome_channel(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fake_playwright = _StrictPlaywright()
    managed = tmp_path / "managed.exe"
    managed.write_bytes(b"managed")

    monkeypatch.setattr(
        playwright_runtime,
        "resolve_playwright_chromium_executable",
        lambda headless=False: managed,
    )

    context = asyncio.run(
        playwright_runtime._launch_persistent_context(
            fake_playwright,
            user_data_dir=Path("C:/profiles/managed_account"),
            headless=False,
            browser_mode=PLAYWRIGHT_BROWSER_MODE_MANAGED,
        )
    )

    assert context == {"context": "ok"}
    assert fake_playwright.chromium.persistent_calls == [
        {
            "user_data_dir": str(Path("C:/profiles/managed_account")),
            "headless": False,
            "args": playwright_runtime.PLAYWRIGHT_BASE_FLAGS,
            "executable_path": str(managed),
        }
    ]


def test_get_context_managed_does_not_fallback_to_shared(tmp_path: Path, monkeypatch) -> None:
    async def _run() -> None:
        runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)
        managed = tmp_path / "managed.exe"
        managed.write_bytes(b"managed")
        launch_calls: list[dict] = []

        async def _fake_start(*args, **kwargs) -> None:
            runtime._playwright = object()

        async def _fake_launch_persistent_context(*args, **kwargs):
            launch_calls.append(dict(kwargs))
            raise RuntimeError("browser has been closed")

        async def _fake_write_diag(*args, **kwargs) -> None:
            return None

        monkeypatch.setattr(runtime, "start", _fake_start)
        monkeypatch.setattr(playwright_runtime, "_launch_persistent_context", _fake_launch_persistent_context)
        monkeypatch.setattr(runtime, "_write_diagnostic_bundle", _fake_write_diag)

        with pytest.raises(RuntimeError, match="browser has been closed"):
            await runtime.get_context(
                account="managed-account",
                profile_dir=tmp_path / "profile",
                mode="persistent",
                executable_path=managed,
                browser_mode=PLAYWRIGHT_BROWSER_MODE_MANAGED,
            )

        assert len(launch_calls) == 1

    asyncio.run(_run())


def test_launch_persistent_context_chrome_only_uses_explicit_google_chrome_without_fallback(tmp_path: Path) -> None:
    fake_playwright = _StrictPlaywright()
    chrome = tmp_path / "Google" / "Chrome" / "Application" / "chrome.exe"
    chrome.parent.mkdir(parents=True, exist_ok=True)
    chrome.write_bytes(b"fake-google-chrome")

    context = asyncio.run(
        playwright_runtime._launch_persistent_context(
            fake_playwright,
            user_data_dir=Path("C:/profiles/manual_account"),
            headless=False,
            executable_path=chrome,
            browser_mode=PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        )
    )

    assert context == {"context": "ok"}
    assert fake_playwright.chromium.persistent_calls == [
        {
            "user_data_dir": str(Path("C:/profiles/manual_account")),
            "headless": False,
            "args": playwright_runtime.PLAYWRIGHT_BASE_FLAGS,
            "executable_path": str(chrome),
        }
    ]


def test_launch_persistent_context_chrome_only_falls_back_to_bundled_chrome_then_chromium(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fake_playwright = _StrictPlaywright(
        results=[
            RuntimeError("local chrome failed"),
            RuntimeError("bundled chrome failed"),
            {"context": "ok"},
        ]
    )
    chrome = tmp_path / "Google" / "Chrome" / "Application" / "chrome.exe"
    chrome.parent.mkdir(parents=True, exist_ok=True)
    chrome.write_bytes(b"fake-google-chrome")
    bundled_chrome = tmp_path / "runtime" / "browsers" / "chrome-win64" / "chrome.exe"
    bundled_chrome.parent.mkdir(parents=True, exist_ok=True)
    bundled_chrome.write_bytes(b"fake-bundled-chrome")
    chromium = tmp_path / "runtime" / "playwright" / "chromium-1155" / "chrome-win" / "chrome.exe"
    chromium.parent.mkdir(parents=True, exist_ok=True)
    chromium.write_bytes(b"fake-chromium")

    monkeypatch.setattr(playwright_runtime, "resolve_google_chrome_executable", lambda: chrome)
    monkeypatch.setattr(playwright_runtime, "resolve_bundled_google_chrome_executable", lambda: bundled_chrome)
    monkeypatch.setattr(playwright_runtime, "resolve_playwright_chromium_executable", lambda headless=False: chromium)

    context = asyncio.run(
        playwright_runtime._launch_persistent_context(
            fake_playwright,
            user_data_dir=Path("C:/profiles/manual_account"),
            headless=False,
            executable_path=chrome,
            browser_mode=PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        )
    )

    assert context == {"context": "ok"}
    assert [call["executable_path"] for call in fake_playwright.chromium.persistent_calls] == [
        str(chrome),
        str(bundled_chrome),
        str(chromium),
    ]
    assert all("channel" not in call for call in fake_playwright.chromium.persistent_calls)


def test_resolve_google_chrome_accepts_standard_install_path_without_version_output(monkeypatch) -> None:
    candidate = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")
    monkeypatch.setattr(playwright_resolver, "_system_chrome_candidates", lambda: [candidate])
    monkeypatch.setattr(playwright_resolver, "_is_valid_executable", lambda path: path == candidate)
    monkeypatch.setattr(playwright_resolver, "_browser_version_text", lambda _path: "")
    playwright_resolver.resolve_google_chrome_executable.cache_clear()
    try:
        assert playwright_resolver.resolve_google_chrome_executable() == candidate
    finally:
        playwright_resolver.resolve_google_chrome_executable.cache_clear()


def test_resolve_google_chrome_rejects_embedded_playwright_chromium(monkeypatch) -> None:
    candidate = Path(r"C:\app\runtime\playwright\chromium-1155\chrome-win\chrome.exe")
    monkeypatch.setattr(playwright_resolver, "_system_chrome_candidates", lambda: [candidate])
    monkeypatch.setattr(playwright_resolver, "_is_valid_executable", lambda path: path == candidate)
    monkeypatch.setattr(playwright_resolver, "_browser_version_text", lambda _path: "")
    playwright_resolver.resolve_google_chrome_executable.cache_clear()
    try:
        assert playwright_resolver.resolve_google_chrome_executable() is None
    finally:
        playwright_resolver.resolve_google_chrome_executable.cache_clear()


def test_resolve_bundled_google_chrome_accepts_runtime_browsers_layout(monkeypatch) -> None:
    candidate = Path(r"C:\app\runtime\browsers\chrome-win64\chrome.exe")
    monkeypatch.setattr(playwright_resolver, "_bundled_chrome_roots", lambda: [Path(r"C:\app\runtime\browsers")])
    monkeypatch.setattr(playwright_resolver, "_is_valid_executable", lambda path: path == candidate)
    playwright_resolver.resolve_bundled_google_chrome_executable.cache_clear()
    try:
        assert playwright_resolver.resolve_bundled_google_chrome_executable() == candidate
    finally:
        playwright_resolver.resolve_bundled_google_chrome_executable.cache_clear()
