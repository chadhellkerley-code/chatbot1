from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.runtime import playwright_runtime


class _FakeChromium:
    def __init__(
        self,
        *,
        launch_error: Exception | None = None,
        launch_results: list[object] | None = None,
    ) -> None:
        self.launch_calls: list[dict] = []
        self.persistent_calls: list[dict] = []
        self._launch_error = launch_error
        self._launch_results = list(launch_results or [])

    def _next_result(self) -> object:
        if self._launch_results:
            return self._launch_results.pop(0)
        if self._launch_error is not None:
            return self._launch_error
        return {"ok": True}

    async def launch(self, **kwargs):
        self.launch_calls.append(dict(kwargs))
        result = self._next_result()
        if isinstance(result, Exception):
            raise result
        return {"browser": "ok"}

    async def launch_persistent_context(self, **kwargs):
        self.persistent_calls.append(dict(kwargs))
        result = self._next_result()
        if isinstance(result, Exception):
            raise result
        return {"context": "ok"}


class _FakePlaywright:
    def __init__(
        self,
        *,
        launch_error: Exception | None = None,
        launch_results: list[object] | None = None,
    ) -> None:
        self.chromium = _FakeChromium(
            launch_error=launch_error,
            launch_results=launch_results,
        )


def test_launch_persistent_context_chrome_only_uses_google_chrome_executable(tmp_path: Path) -> None:
    async def _run() -> None:
        fake_playwright = _FakePlaywright()
        chrome = tmp_path / "chrome.exe"
        chrome.write_bytes(b"0")

        context = await playwright_runtime._launch_persistent_context(
            fake_playwright,
            user_data_dir=tmp_path / "profile",
            headless=False,
            executable_path=chrome,
            browser_mode=playwright_runtime.PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
        )

        assert context == {"context": "ok"}
        assert len(fake_playwright.chromium.persistent_calls) == 1
        launch_kwargs = fake_playwright.chromium.persistent_calls[0]
        assert launch_kwargs["executable_path"] == str(chrome)
        assert "channel" not in launch_kwargs

    asyncio.run(_run())


def test_launch_persistent_context_chrome_only_tries_local_bundled_and_chromium_in_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        fake_playwright = _FakePlaywright(
            launch_results=[
                RuntimeError("local chrome failed"),
                RuntimeError("bundled chrome failed"),
                RuntimeError("chromium failed"),
            ]
        )
        chrome = tmp_path / "local-chrome.exe"
        chrome.write_bytes(b"0")
        bundled_chrome = tmp_path / "runtime" / "browsers" / "chrome-win64" / "chrome.exe"
        bundled_chrome.parent.mkdir(parents=True, exist_ok=True)
        bundled_chrome.write_bytes(b"0")
        chromium = tmp_path / "runtime" / "playwright" / "chromium-1155" / "chrome-win" / "chrome.exe"
        chromium.parent.mkdir(parents=True, exist_ok=True)
        chromium.write_bytes(b"0")

        monkeypatch.setattr(playwright_runtime, "resolve_google_chrome_executable", lambda: chrome)
        monkeypatch.setattr(playwright_runtime, "resolve_bundled_google_chrome_executable", lambda: bundled_chrome)
        monkeypatch.setattr(playwright_runtime, "resolve_playwright_chromium_executable", lambda headless=False: chromium)

        with pytest.raises(RuntimeError, match="PW-PERSISTENT-CHROME-ONLY-FAILED"):
            await playwright_runtime._launch_persistent_context(
                fake_playwright,
                user_data_dir=tmp_path / "profile",
                headless=False,
                executable_path=chrome,
                browser_mode=playwright_runtime.PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
            )

        assert [call["executable_path"] for call in fake_playwright.chromium.persistent_calls] == [
            str(chrome),
            str(bundled_chrome),
            str(chromium),
        ]
        assert all("--no-sandbox" not in list(call.get("args") or []) for call in fake_playwright.chromium.persistent_calls)

    asyncio.run(_run())


def test_get_context_chrome_only_does_not_fallback_to_shared(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        runtime = playwright_runtime.PlaywrightRuntime(headless=False, owner_module=__name__)
        chrome = tmp_path / "chrome.exe"
        chrome.write_bytes(b"0")
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
                account="test-account",
                profile_dir=tmp_path / "profile",
                mode="persistent",
                executable_path=chrome,
                browser_mode=playwright_runtime.PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY,
            )

        assert len(launch_calls) == 1

    asyncio.run(_run())
