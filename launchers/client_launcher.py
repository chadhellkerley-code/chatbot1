# client_launcher.py
# -*- coding: utf-8 -*-
"""Punto de entrada para ejecutables generados."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bootstrap import bootstrap_application
from runtime.runtime_parity import bootstrap_runtime_env

_BOOTSTRAP_CONTEXT = bootstrap_application("client", defer_housekeeping=True)


def _bootstrap_playwright_env() -> None:
    frozen = getattr(sys, "frozen", False)
    if frozen:
        exe_dir = Path(sys.executable).resolve().parent
    else:
        exe_dir = Path(__file__).resolve().parent.parent
    app_dir_raw = (os.environ.get("INSTACRM_APP_ROOT") or "").strip()
    app_dir = Path(app_dir_raw).expanduser() if app_dir_raw else None
    candidates: List[Path] = []
    if app_dir is not None:
        candidates.extend(
            [
                app_dir / "playwright_browsers",
                app_dir / "runtime" / "playwright",
                app_dir / "runtime" / "browsers",
                app_dir / "playwright",
                app_dir / "browsers",
            ]
        )
    candidates.extend(
        [
            exe_dir / "playwright_browsers",
            exe_dir / "runtime" / "playwright",
            exe_dir / "runtime" / "browsers",
            exe_dir / "playwright",
            exe_dir / "browsers",
        ]
    )
    pw_root = candidates[0]
    if not pw_root.exists():
        for candidate in candidates[1:]:
            if candidate.exists():
                pw_root = candidate
                break
    os.environ["PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD"] = "1"
    os.environ.setdefault("PLAYWRIGHT_DOWNLOAD_HOST", "")
    if pw_root.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(pw_root)
    elif (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip() == str(pw_root):
        os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
    exists = pw_root.exists()
    current = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    print(
        "Playwright bootstrap: "
        f"exe_dir={exe_dir} pw_root={pw_root} exists={'true' if exists else 'false'} "
        f"PLAYWRIGHT_BROWSERS_PATH={current or '-'}"
    )


_bootstrap_playwright_env()
bootstrap_runtime_env("client", app_root_hint=_BOOTSTRAP_CONTEXT.install_root, force=True)


_PLAYWRIGHT_BROWSER_PREFIXES = (
    "chromium-",
    "chromium_headless_shell-",
    "firefox-",
    "webkit-",
    "ffmpeg-",
)
_PLAYWRIGHT_CHROMIUM_PREFIX = "chromium-"
_PLAYWRIGHT_HEADLESS_PREFIX = "chromium_headless_shell-"
_MIN_EXECUTABLE_BYTES = 1 * 1024 * 1024
_PAK_FILES = (
    "resources.pak",
    "chrome_100_percent.pak",
    "chrome_200_percent.pak",
    "headless_lib_data.pak",
    "headless_lib_strings.pak",
    "headless_command_resources.pak",
)
_PLAYWRIGHT_SELECTION_LOGGED = False


def _safe_stat_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def _parse_revision(name: str, prefix: str) -> int:
    if not name.startswith(prefix):
        return -1
    suffix = name[len(prefix) :]
    digits = "".join(ch for ch in suffix if ch.isdigit())
    return int(digits) if digits else -1


def _pick_latest_dir(root: Path, prefix: str) -> Optional[Path]:
    try:
        candidates: List[Tuple[int, Path]] = []
        for item in root.iterdir():
            if item.is_dir() and item.name.startswith(prefix):
                candidates.append((_parse_revision(item.name, prefix), item))
    except Exception:
        return None
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _key_files_ok(folder: Path) -> bool:
    if _safe_stat_size(folder / "icudtl.dat") <= 0:
        return False
    for name in _PAK_FILES:
        if _safe_stat_size(folder / name) > 0:
            return True
    return False


def _chromium_exe_candidates(browser_dir: Path) -> List[Path]:
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


def _headless_exe_candidates(browser_dir: Path) -> List[Path]:
    if sys.platform.startswith("win"):
        return [
            browser_dir / "chrome-headless-shell-win64" / "chrome-headless-shell.exe",
            browser_dir / "chrome-headless-shell-win32" / "chrome-headless-shell.exe",
            browser_dir / "chrome-headless-shell" / "chrome-headless-shell.exe",
            browser_dir / "headless_shell" / "headless_shell.exe",
        ]
    if sys.platform == "darwin":
        return [
            browser_dir
            / "chrome-headless-shell"
            / "Chromium.app"
            / "Contents"
            / "MacOS"
            / "Chromium"
        ]
    return [browser_dir / "chrome-headless-shell" / "chrome-headless-shell"]


def _standalone_chrome_candidates(root: Path) -> List[Path]:
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


def _validate_executable(exe_path: Path) -> Tuple[bool, int]:
    size = _safe_stat_size(exe_path)
    if size <= _MIN_EXECUTABLE_BYTES:
        return False, size
    return True, size


def _select_standalone_chrome(
    root: Path,
) -> Optional[Tuple[Path, int, str, Path, str, str]]:
    for exe_path in _standalone_chrome_candidates(root):
        ok, size = _validate_executable(exe_path)
        if not ok:
            continue
        browser_dir = exe_path.parent
        return (
            exe_path,
            size,
            "standalone_chrome",
            browser_dir,
            "standalone_chrome",
            browser_dir.name,
        )
    return None


def _select_executable(
    browser_dir: Path,
    candidates: List[Path],
    reason: str,
    browser_type: str,
    prefix: str,
) -> Optional[Tuple[Path, int, str, Path, str, str]]:
    for exe_path in candidates:
        ok, size = _validate_executable(exe_path)
        if ok:
            version = browser_dir.name[len(prefix) :] if browser_dir.name.startswith(prefix) else ""
            return exe_path, size, reason, browser_dir, browser_type, version
    return None


def _select_playwright_root(
    candidate: Path,
) -> Optional[Tuple[Path, Path, int, str, Path, str, str]]:
    if not candidate.exists():
        return None

    standalone = _select_standalone_chrome(candidate)
    if standalone:
        exe_path, size, reason, browser_dir, browser_type, version = standalone
        return candidate, exe_path, size, reason, browser_dir, browser_type, version

    chromium_dir = _pick_latest_dir(candidate, _PLAYWRIGHT_CHROMIUM_PREFIX)
    if chromium_dir:
        selection = _select_executable(
            chromium_dir,
            _chromium_exe_candidates(chromium_dir),
            "accepted_standalone",
            "chromium",
            _PLAYWRIGHT_CHROMIUM_PREFIX,
        )
        if selection:
            exe_path, size, reason, browser_dir, browser_type, version = selection
            return candidate, exe_path, size, reason, browser_dir, browser_type, version

    headless_dir = _pick_latest_dir(candidate, _PLAYWRIGHT_HEADLESS_PREFIX)
    if headless_dir:
        selection = _select_executable(
            headless_dir,
            _headless_exe_candidates(headless_dir),
            "accepted_standalone",
            "headless",
            _PLAYWRIGHT_HEADLESS_PREFIX,
        )
        if selection:
            exe_path, size, reason, browser_dir, browser_type, version = selection
            return candidate, exe_path, size, reason, browser_dir, browser_type, version

    nested = candidate / "ms-playwright"
    if nested.exists():
        return _select_playwright_root(nested)
    return None


def _log_playwright_selection(
    root: Path,
    browser_dir: Path,
    exe_path: Path,
    size: int,
    reason: str,
    browser_type: str,
    version: str,
) -> None:
    global _PLAYWRIGHT_SELECTION_LOGGED
    if _PLAYWRIGHT_SELECTION_LOGGED:
        return
    _PLAYWRIGHT_SELECTION_LOGGED = True
    print(
        "Selected browser: "
        f"type={browser_type} version={version or '-'} "
        f"exe={exe_path.name} size={size} reason={reason}"
    )


def _resolve_playwright_browsers_path() -> Optional[
    Tuple[Path, Path, int, str, Path, str, str]
]:
    candidates: List[Path] = []
    exe_parent = None
    if getattr(sys, "frozen", False):
        exe = getattr(sys, "executable", "") or ""
        if exe:
            try:
                exe_parent = Path(exe).resolve().parent
            except Exception:
                exe_parent = None
    if exe_parent:
        candidates.extend(
            [
                exe_parent / "app" / "playwright_browsers",
                exe_parent / "app" / "runtime" / "playwright",
                exe_parent / "app" / "runtime" / "browsers",
                exe_parent / "app" / "playwright",
                exe_parent / "app" / "browsers",
                exe_parent / "playwright_browsers",
                exe_parent / "runtime" / "playwright",
                exe_parent / "runtime" / "browsers",
                exe_parent / "playwright",
                exe_parent / "browsers",
                exe_parent,
            ]
        )

    env_app_root = (os.environ.get("INSTACRM_APP_ROOT") or "").strip()
    if env_app_root:
        app_root = Path(env_app_root).expanduser()
        candidates.extend(
            [
                app_root / "playwright_browsers",
                app_root / "runtime" / "playwright",
                app_root / "runtime" / "browsers",
                app_root / "playwright",
                app_root / "browsers",
                app_root,
            ]
        )

    repo_root = Path(__file__).resolve().parent.parent
    candidates.extend(
        [
            repo_root / "playwright_browsers",
            repo_root / "runtime" / "playwright",
            repo_root / "runtime" / "browsers",
            repo_root / "playwright",
            repo_root / "browsers",
            repo_root,
        ]
    )

    base = getattr(sys, "_MEIPASS", None)
    if base:
        candidates.extend(
            [
                Path(base) / "playwright_browsers",
                Path(base) / "runtime" / "playwright",
                Path(base) / "runtime" / "browsers",
                Path(base) / "playwright",
                Path(base) / "browsers",
                Path(base),
            ]
        )

    for candidate in candidates:
        selection = _select_playwright_root(candidate)
        if selection:
            return selection
    return None


def _configure_playwright_browsers() -> None:
    current = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if current:
        selection = _select_playwright_root(Path(current).expanduser())
        if selection:
            root, exe_path, size, reason, browser_dir, browser_type, version = selection
            os.environ["PLAYWRIGHT_CHROME_EXECUTABLE"] = str(exe_path)
            if browser_type == "standalone_chrome":
                os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
            elif str(root) != current:
                os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(root)
            _log_playwright_selection(
                root, browser_dir, exe_path, size, reason, browser_type, version
            )
            return
        print(
            "Warning: PLAYWRIGHT_BROWSERS_PATH no contiene un browser valido. "
            f"Ruta ignorada: {current}"
        )
        os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
    selection = _resolve_playwright_browsers_path()
    if selection:
        root, exe_path, size, reason, browser_dir, browser_type, version = selection
        os.environ["PLAYWRIGHT_CHROME_EXECUTABLE"] = str(exe_path)
        if browser_type == "standalone_chrome":
            os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
        else:
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(root)
        _log_playwright_selection(
            root, browser_dir, exe_path, size, reason, browser_type, version
        )
        return
    print("Warning: No se detecto ningun browser embebido valido.")


_configure_playwright_browsers()


def _configure_app_version() -> None:
    if os.environ.get("APP_VERSION") or os.environ.get("CLIENT_VERSION"):
        return
    candidates: List[Path] = []
    base = getattr(sys, "_MEIPASS", None)
    if base:
        candidates.extend(
            [
                Path(base) / "app_version.json",
                Path(base) / "storage" / "app_version.json",
            ]
        )
    exe_path = getattr(sys, "executable", "") or ""
    if exe_path:
        try:
            exe_parent = Path(exe_path).resolve().parent
        except Exception:
            exe_parent = None
        if exe_parent:
            candidates.extend(
                [
                    exe_parent / "app" / "app_version.json",
                    exe_parent / "app_version.json",
                    exe_parent / "storage" / "app_version.json",
                ]
            )
    env_app_root = (os.environ.get("INSTACRM_APP_ROOT") or "").strip()
    if env_app_root:
        candidates.append(Path(env_app_root).expanduser() / "app_version.json")
    candidates.extend(
        [
            Path(__file__).resolve().parent.parent / "app_version.json",
            Path(__file__).resolve().parent.parent / "storage" / "app_version.json",
        ]
    )

    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            version = str(payload.get("version") or payload.get("app_version") or "").strip()
        except Exception:
            version = ""
        if version:
            os.environ["APP_VERSION"] = version
            return


_configure_app_version()

from license_client import launch_with_license


def _show_startup_error(title: str, message: str) -> None:
    text = f"{message}\n\nLa aplicación se cerrará."
    if os.name == "nt":
        try:
            import ctypes

            ctypes.windll.user32.MessageBoxW(None, text, title, 0x10)
            return
        except Exception:
            pass
    try:
        print(f"{title}: {text}", file=sys.stderr, flush=True)
    except Exception:
        pass


def _launch_entrypoint() -> int:
    try:
        from gui.gui_app import launch_gui_app
    except Exception as exc:
        _show_startup_error(
            "InstaCRM",
            f"No se pudo cargar la interfaz gráfica.\nDetalle: {exc}",
        )
        return 1

    try:
        return int(
            launch_gui_app(
                backend_entrypoint=launch_with_license,
                mode="client",
            )
        )
    except Exception as exc:
        _show_startup_error(
            "InstaCRM",
            f"La interfaz gráfica falló al iniciar.\nDetalle: {exc}",
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(_launch_entrypoint())
