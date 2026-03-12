from __future__ import annotations

import os
import shutil
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

from paths import browser_binaries_root, playwright_browsers_root, runtime_base

_BASE_ROOT = runtime_base(Path(__file__).resolve().parents[2])
_RUNTIME_BROWSERS_ROOT = browser_binaries_root(_BASE_ROOT)
_PLAYWRIGHT_ROOT = playwright_browsers_root(_BASE_ROOT)
_PLAYWRIGHT_CHROMIUM_PREFIX = "chromium-"
_PLAYWRIGHT_EXECUTABLE_ENV_KEYS = (
    "PLAYWRIGHT_CHROME_EXECUTABLE",
    "PLAYWRIGHT_EXECUTABLE_PATH",
    "CHROME_EXECUTABLE",
)
_SYSTEM_CHROME_ENV_KEYS = (
    "GOOGLE_CHROME_EXECUTABLE",
    "GOOGLE_CHROME_PATH",
    "GOOGLE_CHROME_BIN",
)
_MIN_EXECUTABLE_BYTES = 1 * 1024 * 1024


def _is_valid_executable(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > _MIN_EXECUTABLE_BYTES
    except Exception:
        return False


def _chromium_exe_candidates(browser_dir: Path) -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            browser_dir / "chrome-win64" / "chrome.exe",
            browser_dir / "chrome-win" / "chrome.exe",
        ]
    if sys.platform == "darwin":
        return [
            browser_dir / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium"
        ]
    return [browser_dir / "chrome-linux" / "chrome"]


def _standalone_chrome_candidates(root: Path) -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            root / "chrome-win64" / "chrome.exe",
            root / "chrome-win" / "chrome.exe",
            root / "browsers" / "chrome-win64" / "chrome.exe",
            root / "browsers" / "chrome-win" / "chrome.exe",
        ]
    if sys.platform == "darwin":
        return [
            root / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium",
            root / "browsers" / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium",
        ]
    return [
        root / "chrome-linux" / "chrome",
        root / "browsers" / "chrome-linux" / "chrome",
    ]


def _parse_revision(name: str, prefix: str) -> int:
    if not name.startswith(prefix):
        return -1
    suffix = name[len(prefix) :]
    digits = "".join(ch for ch in suffix if ch.isdigit())
    return int(digits) if digits else -1


def _candidate_roots() -> list[Path]:
    candidates: list[Path] = []
    env_root = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if env_root:
        env_path = Path(env_root).expanduser()
        candidates.extend([env_path, env_path / "ms-playwright"])

    candidates.extend(
        [
            _PLAYWRIGHT_ROOT,
            _RUNTIME_BROWSERS_ROOT,
            _BASE_ROOT / "playwright_browsers",
            _BASE_ROOT / "ms-playwright",
            _BASE_ROOT / "_internal" / "playwright_browsers",
            _BASE_ROOT / "_internal" / "ms-playwright",
        ]
    )

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        base = Path(meipass)
        candidates.extend(
            [
                base / "playwright_browsers",
                base / "ms-playwright",
                base / "_internal" / "playwright_browsers",
                base / "_internal" / "ms-playwright",
            ]
        )

    exe_path = getattr(sys, "executable", "") or ""
    if exe_path:
        try:
            exe_dir = Path(exe_path).resolve().parent
            candidates.extend(
                [
                    exe_dir / "playwright_browsers",
                    exe_dir / "ms-playwright",
                    exe_dir / "_internal" / "playwright_browsers",
                    exe_dir / "_internal" / "ms-playwright",
                ]
            )
        except Exception:
            pass

    roots: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        roots.append(candidate)
    return roots


def ensure_local_playwright_browsers_env() -> Optional[Path]:
    if (os.environ.get("PLAYWRIGHT_CHROME_EXECUTABLE") or "").strip():
        return Path(os.environ["PLAYWRIGHT_CHROME_EXECUTABLE"]).expanduser()
    current = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if current:
        return Path(current).expanduser()
    for candidate in _candidate_roots():
        if candidate.exists():
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(candidate)
            return candidate
    return None


def _resolve_executable_from_env() -> Optional[Path]:
    for key in _PLAYWRIGHT_EXECUTABLE_ENV_KEYS:
        value = (os.environ.get(key) or "").strip()
        if not value:
            continue
        candidate = Path(value).expanduser()
        if _is_valid_executable(candidate):
            return candidate
    return None


def _resolve_from_browser_roots() -> Optional[Path]:
    browser_dirs: list[tuple[int, Path]] = []
    for root in _candidate_roots():
        if not root.exists():
            continue
        try:
            for item in root.iterdir():
                if item.is_dir() and item.name.startswith(_PLAYWRIGHT_CHROMIUM_PREFIX):
                    browser_dirs.append((_parse_revision(item.name, _PLAYWRIGHT_CHROMIUM_PREFIX), item))
        except Exception:
            continue
    browser_dirs.sort(key=lambda item: item[0], reverse=True)
    for _revision, browser_dir in browser_dirs:
        for candidate in _chromium_exe_candidates(browser_dir):
            if _is_valid_executable(candidate):
                return candidate
    return None


def _browser_version_text(path: Path) -> str:
    try:
        run_kwargs: dict[str, object] = {
            "capture_output": True,
            "text": True,
            "encoding": "utf-8",
            "errors": "ignore",
            "timeout": 10,
        }
        if os.name == "nt":
            create_no_window = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0))
            startupinfo.wShowWindow = 0
            run_kwargs["creationflags"] = create_no_window
            run_kwargs["startupinfo"] = startupinfo
        completed = subprocess.run([str(path), "--version"], **run_kwargs)
    except Exception:
        return ""
    output = f"{completed.stdout or ''}\n{completed.stderr or ''}".strip()
    return output


def _is_google_chrome_binary(path: Path) -> bool:
    if not _is_valid_executable(path):
        return False
    normalized = str(path).replace("/", "\\").lower()
    trusted_windows_install = "\\google\\chrome\\application\\chrome.exe"
    trusted_macos_install = "\\google chrome.app\\contents\\macos\\google chrome"
    if trusted_windows_install in normalized or trusted_macos_install in normalized:
        return True
    version_text = _browser_version_text(path).lower()
    return "google chrome" in version_text


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in paths:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _bundled_chrome_roots() -> list[Path]:
    candidates: list[Path] = [
        _RUNTIME_BROWSERS_ROOT,
        _BASE_ROOT / "browsers",
        _BASE_ROOT / "_internal" / "browsers",
    ]

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        base = Path(meipass)
        candidates.extend(
            [
                base / "browsers",
                base / "runtime" / "browsers",
                base / "_internal" / "browsers",
            ]
        )

    exe_path = getattr(sys, "executable", "") or ""
    if exe_path:
        try:
            exe_dir = Path(exe_path).resolve().parent
            candidates.extend(
                [
                    exe_dir / "browsers",
                    exe_dir / "runtime" / "browsers",
                    exe_dir / "_internal" / "browsers",
                ]
            )
        except Exception:
            pass

    return _dedupe_paths(candidates)


def _system_chrome_candidates() -> list[Path]:
    candidates: list[Path] = []
    for key in _SYSTEM_CHROME_ENV_KEYS:
        value = (os.environ.get(key) or "").strip()
        if value:
            candidates.append(Path(value).expanduser())

    if sys.platform.startswith("win"):
        program_files = os.environ.get("PROGRAMFILES", r"C:\Program Files")
        program_files_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
        local_app_data = os.environ.get("LOCALAPPDATA", "")
        candidates.extend(
            [
                Path(program_files) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(program_files_x86) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(local_app_data) / "Google" / "Chrome" / "Application" / "chrome.exe",
            ]
        )
        chrome_on_path = shutil.which("chrome")
        if chrome_on_path:
            candidates.append(Path(chrome_on_path))
    elif sys.platform == "darwin":
        candidates.extend(
            [
                Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
                Path.home() / "Applications" / "Google Chrome.app" / "Contents" / "MacOS" / "Google Chrome",
            ]
        )
        chrome_on_path = shutil.which("google-chrome")
        if chrome_on_path:
            candidates.append(Path(chrome_on_path))
    else:
        for command_name in ("google-chrome", "google-chrome-stable", "chrome", "chrome-browser"):
            resolved = shutil.which(command_name)
            if resolved:
                candidates.append(Path(resolved))
        candidates.extend(
            [
                Path("/usr/bin/google-chrome"),
                Path("/usr/bin/google-chrome-stable"),
            ]
        )
    return _dedupe_paths(candidates)


@lru_cache(maxsize=1)
def resolve_google_chrome_executable() -> Optional[Path]:
    for candidate in _system_chrome_candidates():
        if _is_google_chrome_binary(candidate):
            return candidate
    return None


def require_google_chrome_executable() -> Path:
    candidate = resolve_google_chrome_executable()
    if candidate is not None:
        return candidate
    searched = ", ".join(str(path) for path in _system_chrome_candidates()) or "no candidates"
    raise RuntimeError(
        "Google Chrome no esta disponible en el sistema. "
        f"Rutas evaluadas: {searched}"
    )


@lru_cache(maxsize=1)
def resolve_bundled_google_chrome_executable() -> Optional[Path]:
    for root in _bundled_chrome_roots():
        for candidate in _standalone_chrome_candidates(root):
            if _is_valid_executable(candidate):
                return candidate
    return None


@lru_cache(maxsize=1)
def _resolve_from_playwright_api() -> Optional[Path]:
    ensure_local_playwright_browsers_env()
    try:
        with sync_playwright() as playwright:
            candidate = Path(playwright.chromium.executable_path).expanduser()
            if _is_valid_executable(candidate):
                return candidate
    except Exception:
        return None
    return None


def resolve_playwright_chromium_executable(*, headless: bool = False) -> Optional[Path]:
    _ = headless
    ensure_local_playwright_browsers_env()
    explicit = _resolve_executable_from_env()
    if explicit:
        return explicit
    official = _resolve_from_playwright_api()
    if official and _is_valid_executable(official):
        return official
    return _resolve_from_browser_roots()
