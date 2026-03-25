# tools/build_owner_macos_universal.py
# -*- coding: utf-8 -*-
"""Builder aislado para generar el ejecutable owner universal de macOS."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Optional, TextIO, Tuple

_MIN_EXECUTABLE_BYTES = 1 * 1024 * 1024
_PLAYWRIGHT_BUNDLE_DIR = "playwright_browsers"
_PAK_FILES = (
    "resources.pak",
    "chrome_100_percent.pak",
    "chrome_200_percent.pak",
    "headless_lib_data.pak",
    "headless_lib_strings.pak",
    "headless_command_resources.pak",
)
_HIDDEN_IMPORTS = [
    "accounts",
    "actions.content_publisher",
    "actions.interactions",
    "actions.interactions_adapters",
    "app",
    "backend_license_client",
    "config",
    "gui_app",
    "ig",
    "io_adapter",
    "leads",
    "licensekit",
    "main_window",
    "media_norm",
    "proxy_manager",
    "responder",
    "runtime",
    "sdk_sanitize",
    "session_store",
    "state_view",
    "src.analytics.stats_engine",
    "storage",
    "totp_store",
    "ui",
    "update_system",
    "utils",
    "whatsapp",
    "jaraco",
    "jaraco.text",
    "jaraco.classes",
    "jaraco.functools",
    "pkg_resources",
    "setuptools",
]
_COLLECT_ALL_MODULES = [
    "openpyxl",
    "PySide6",
]
_DEFAULT_EXCLUDES = [
    "tkinter",
    "_tkinter",
]
_HEAVY_EXCLUDES = [
    "torch",
    "transformers",
    "tensorflow",
    "tf_keras",
    "keras",
    "mediapipe",
    "deepface",
    "retinaface",
    "clip",
    "kivy",
    "kivymd",
    "pandas",
    "scipy",
    "matplotlib",
    "cv2",
    "h5py",
]


def _log(message: str) -> None:
    print(message, flush=True)


def _safe_stat_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def _tail_stream(stream: TextIO, lines: int = 40) -> str:
    tail = deque(maxlen=lines)
    try:
        stream.flush()
        stream.seek(0)
        for line in stream:
            tail.append(line.rstrip())
    except OSError:
        return ""
    return "\n".join(tail)


def _tail_log(path: Path, lines: int = 40) -> str:
    if not path.exists():
        return ""
    tail = deque(maxlen=lines)
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                tail.append(line.rstrip())
    except OSError:
        return ""
    return "\n".join(tail)


def _macos_major_version() -> int:
    try:
        out = subprocess.check_output(
            ["sw_vers", "-productVersion"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return 0
    if not out:
        return 0
    try:
        major_text = out.split(".", 1)[0]
        return int(major_text)
    except Exception:
        return 0


def _parse_revision(name: str, prefix: str) -> int:
    if not name.startswith(prefix):
        return -1
    digits = "".join(ch for ch in name[len(prefix) :] if ch.isdigit())
    return int(digits) if digits else -1


def _is_valid_chromium_executable(exe_path: Path) -> bool:
    size = _safe_stat_size(exe_path)
    if size <= _MIN_EXECUTABLE_BYTES:
        return False
    parent = exe_path.parent
    if _safe_stat_size(parent / "icudtl.dat") <= 0:
        return False
    for pak in _PAK_FILES:
        if _safe_stat_size(parent / pak) > 0:
            return True
    return False


def _resolve_playwright_browsers_root(project_root: Path) -> Optional[Path]:
    env_path = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if env_path:
        candidate = Path(env_path).expanduser()
        if candidate.exists():
            return candidate
    for candidate in (project_root / "ms-playwright", project_root / "browsers"):
        if candidate.exists():
            return candidate
    cache_candidate = Path.home() / "Library" / "Caches" / "ms-playwright"
    if cache_candidate.exists():
        return cache_candidate
    return None


def _select_playwright_dirs(source: Path) -> list[Path]:
    try:
        chromium_dirs = [
            item
            for item in source.iterdir()
            if item.is_dir() and item.name.startswith("chromium-")
        ]
    except Exception:
        return []
    if not chromium_dirs:
        return []
    chromium_dirs.sort(
        key=lambda item: _parse_revision(item.name, "chromium-"),
        reverse=True,
    )
    selected: list[Path] = []
    chosen_revision = ""
    for chromium_dir in chromium_dirs:
        exe = chromium_dir / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium"
        if _is_valid_chromium_executable(exe):
            selected.append(chromium_dir)
            chosen_revision = chromium_dir.name[len("chromium-") :]
            break
    if not selected:
        return []
    if chosen_revision:
        headless_dir = source / f"chromium_headless_shell-{chosen_revision}"
        if headless_dir.exists():
            selected.append(headless_dir)
    for item in source.iterdir():
        if item.is_dir() and item.name.startswith("ffmpeg-"):
            selected.append(item)
            break
    return selected


def _copy_playwright_browsers(dest_root: Path, source: Path) -> Tuple[bool, str]:
    target = dest_root / _PLAYWRIGHT_BUNDLE_DIR
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True, exist_ok=True)

    selected_dirs = _select_playwright_dirs(source)
    if not selected_dirs:
        return False, "No se encontro Chromium valido de Playwright para macOS."

    selected_names = {item.name for item in selected_dirs}
    for item in source.iterdir():
        if item.is_file():
            shutil.copy2(item, target / item.name)
            continue
        if not item.is_dir():
            continue
        if item.name in selected_names or item.name.startswith("ffmpeg-"):
            shutil.copytree(item, target / item.name)
    return True, ", ".join(sorted(selected_names))


def _guess_output(dist_dir: Path, name: str, onefile: bool) -> Optional[Path]:
    app_candidate = dist_dir / f"{name}.app"
    if app_candidate.exists():
        return app_candidate
    if onefile:
        candidate = dist_dir / name
        if candidate.exists():
            return candidate
    dir_candidate = dist_dir / name
    if dir_candidate.exists():
        return dir_candidate
    return None


def build_owner_macos_universal(
    *,
    name: str = "insta_owner_universal_macos",
    onefile: bool = True,
    bundle_playwright_browsers: bool = False,
    timeout_seconds: int = 7200,
) -> Tuple[bool, Optional[Path], str]:
    """Genera un ejecutable owner universal2 para macOS Big Sur o superior."""

    if sys.platform != "darwin":
        return False, None, "Este builder solo puede ejecutarse en macOS."
    if _macos_major_version() < 11:
        return False, None, "Se requiere macOS Big Sur (11) o superior."

    root = Path(__file__).resolve().parents[1]
    entrypoint = root / "owner_gui_launcher.py"
    if not entrypoint.exists():
        return False, None, "No se encontro owner_gui_launcher.py para el build owner."
    dist_dir = root / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    styles_path = root / "styles.qss"

    sanitized_name = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in name).strip("_")
    exe_name = sanitized_name or "insta_owner_universal_macos"
    log_path = dist_dir / f"{exe_name}_pyinstaller.log"

    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--target-architecture",
        "universal2",
        "--name",
        exe_name,
        "--onefile" if onefile else "--onedir",
        "--windowed",
        str(entrypoint.name),
    ]
    if styles_path.exists():
        command.extend(["--add-data", f"{styles_path}{os.pathsep}."])

    command.extend(["--collect-all", "playwright"])
    for module in _COLLECT_ALL_MODULES:
        command.extend(["--collect-all", module])
    for module in _HIDDEN_IMPORTS:
        command.extend(["--hidden-import", module])
    for module in _DEFAULT_EXCLUDES + _HEAVY_EXCLUDES:
        command.extend(["--exclude-module", module])

    pyinstaller_tail = ""
    try:
        _log("Iniciando build owner macOS universal2...")
        _log(f"Proyecto: {root}")
        _log(f"Salida: {dist_dir}")
        _log(f"Modo: {'onefile' if onefile else 'onedir'}")
        _log(f"Bundle Playwright browsers: {'on' if bundle_playwright_browsers else 'off'}")
        _log(f"Log: {log_path}")

        start = time.monotonic()
        next_heartbeat = start + 30
        with log_path.open("w+", encoding="utf-8", errors="ignore") as log_handle:
            proc = subprocess.Popen(
                command,
                cwd=root,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
            while True:
                retcode = proc.poll()
                if retcode is not None:
                    if retcode != 0:
                        pyinstaller_tail = _tail_stream(log_handle)
                        raise subprocess.CalledProcessError(retcode, command)
                    break
                now = time.monotonic()
                if now - start >= timeout_seconds:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    pyinstaller_tail = _tail_stream(log_handle)
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                if now >= next_heartbeat:
                    _log(f"Compilando... {int(now - start)}s")
                    next_heartbeat = now + 30
                time.sleep(1)
        _log("PyInstaller finalizo correctamente.")

        output = _guess_output(dist_dir, exe_name, onefile=onefile)
        if output is None:
            return False, None, "PyInstaller no genero el artefacto esperado."

        if bundle_playwright_browsers:
            source = _resolve_playwright_browsers_root(root)
            if source is None:
                return (
                    False,
                    None,
                    "No se encontro cache de Playwright. Ejecuta 'python -m playwright install chromium'.",
                )
            if output.suffix.lower() == ".app":
                bundle_root = output / "Contents" / "MacOS"
            else:
                bundle_root = output if output.is_dir() else output.parent
            ok_copy, detail = _copy_playwright_browsers(bundle_root, source)
            if not ok_copy:
                return False, None, detail
            _log(f"Playwright browsers copiados: {detail}")

        return True, output, f"Build owner macOS universal generado en {output}"
    except subprocess.CalledProcessError as exc:
        tail = pyinstaller_tail or _tail_log(log_path)
        detail = f"Error al ejecutar PyInstaller: {exc}"
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
    except subprocess.TimeoutExpired:
        tail = pyinstaller_tail or _tail_log(log_path)
        detail = (
            "PyInstaller supero el tiempo limite y fue detenido. "
            "Aumenta timeout_seconds o reduce modulos."
        )
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
