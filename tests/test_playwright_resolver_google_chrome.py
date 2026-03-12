from __future__ import annotations

from pathlib import Path

import pytest

from src.runtime import playwright_resolver


def _write_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"0" * (1024 * 1024 + 1))
    return path


def test_resolve_google_chrome_executable_prefers_google_chrome(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    chromium = _write_executable(tmp_path / "chromium.exe")
    chrome = _write_executable(tmp_path / "chrome.exe")

    monkeypatch.setattr(playwright_resolver, "_system_chrome_candidates", lambda: [chromium, chrome])
    monkeypatch.setattr(
        playwright_resolver,
        "_browser_version_text",
        lambda path: "Chromium 123.0" if path == chromium else "Google Chrome 123.0",
    )
    playwright_resolver.resolve_google_chrome_executable.cache_clear()

    assert playwright_resolver.resolve_google_chrome_executable() == chrome


def test_require_google_chrome_executable_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(playwright_resolver, "_system_chrome_candidates", lambda: [])
    monkeypatch.setattr(playwright_resolver, "_browser_version_text", lambda _path: "")
    playwright_resolver.resolve_google_chrome_executable.cache_clear()

    with pytest.raises(RuntimeError, match="Google Chrome no esta disponible en el sistema"):
        playwright_resolver.require_google_chrome_executable()
