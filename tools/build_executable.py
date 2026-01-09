# tools/build_executable.py
# -*- coding: utf-8 -*-
"""Utilidad para generar ejecutables por licencia."""

from __future__ import annotations

import errno
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from collections import deque
from typing import Dict, Optional, Tuple


def _slugify(value: str) -> str:
    cleaned = [c if c.isalnum() or c in {"_", "-"} else "_" for c in value.lower()]
    slug = "".join(cleaned).strip("_")
    return slug or "cliente"


def _guess_output(dist_dir: Path, name: str) -> Path:
    if sys.platform.startswith("win"):
        candidate = dist_dir / f"{name}.exe"
        if candidate.exists():
            return candidate
    elif sys.platform == "darwin":
        candidate = dist_dir / f"{name}.app"
        if candidate.exists():
            return candidate
    candidate = dist_dir / name
    if candidate.exists():
        return candidate
    # fallback al .exe por si PyInstaller usa sufijo aun en otros SO
    candidate_exe = dist_dir / f"{name}.exe"
    if candidate_exe.exists():
        return candidate_exe
    return candidate


def _resolve_build_root() -> Path:
    override = os.environ.get("LICENSE_BUILD_ROOT") or os.environ.get("BUILD_TEMP_DIR")
    if override:
        candidate = Path(override).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate
    return Path(tempfile.gettempdir())


def _copy_project(src: Path, dest: Path) -> None:
    ignore = shutil.ignore_patterns(
        "venv*",
        ".venv*",
        "dist",
        "build",
        "__pycache__",
        "*.pyc",
        "*.pyo",
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        "*.log",
        ".sessions",
        "browser_sessions",
        "profiles",
        "whatsapp_exports",
        "storage",
        "data",
        "text",
        "accounts",
        "_archive",
        "tests",
        "tests_optin",
    )
    shutil.copytree(src, dest, ignore=ignore)


def _sanitize_tree(root: Path) -> None:
    for name in (".env", ".env.local"):
        target = root / name
        if target.exists():
            target.unlink()

    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    for item in data_dir.glob("*"):
        if item.is_file():
            item.unlink()

    leads_dir = root / "text" / "leads"
    leads_dir.mkdir(parents=True, exist_ok=True)
    for item in leads_dir.glob("*"):
        if item.is_file():
            item.unlink()

    storage_dir = root / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    for item in storage_dir.glob("*.json*"):
        item.unlink()
    logs_dir = storage_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for item in logs_dir.glob("*"):
        if item.is_file():
            item.unlink()


def _write_client_env(root: Path) -> None:
    env_path = root / ".env"
    lines = ["CLIENT_DISTRIBUTION=1"]
    remote_only = os.environ.get("LICENSE_REMOTE_ONLY")
    if remote_only:
        lines.append(f"LICENSE_REMOTE_ONLY={remote_only}")
    lines.append("# Configure tus propias claves aqui")
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _log_step(message: str) -> None:
    print(message, flush=True)


_HIDDEN_IMPORTS = [
    "accounts",
    "actions.hashtag_mode",
    "actions.content_publisher",
    "actions.interactions",
    "actions.interactions_adapters",
    "app",
    "backend_license_client",
    "config",
    "ig",
    "leads",
    "licensekit",
    "media_norm",
    "proxy_manager",
    "responder",
    "runtime",
    "sdk_sanitize",
    "session_store",
    "state_view",
    "storage",
    "totp_store",
    "ui",
    "utils",
]

_DEFAULT_EXCLUDES = [
    "tkinter",
    "_tkinter",
]

_PLAYWRIGHT_BUNDLE_DIR = "playwright_browsers"
_PLAYWRIGHT_CHROMIUM_PREFIXES = (
    "chromium-",
    "chromium_headless_shell-",
    "ffmpeg-",
    "winldd-",
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


def _key_files_ok(folder: Path) -> bool:
    if _safe_stat_size(folder / "icudtl.dat") <= 0:
        return False
    for name in _PAK_FILES:
        if _safe_stat_size(folder / name) > 0:
            return True
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


def _headless_exe_candidates(browser_dir: Path) -> list[Path]:
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


def _validate_executable(exe_path: Path) -> bool:
    size = _safe_stat_size(exe_path)
    if size <= _MIN_EXECUTABLE_BYTES:
        return False
    return _key_files_ok(exe_path.parent)


def _dir_has_valid_executable(folder: Path, candidates_fn) -> bool:
    for exe_path in candidates_fn(folder):
        if _validate_executable(exe_path):
            return True
    return False


def _pick_latest_valid_dir(root: Path, prefix: str, candidates_fn) -> Optional[Path]:
    try:
        candidates: list[tuple[int, Path]] = []
        for item in root.iterdir():
            if item.is_dir() and item.name.startswith(prefix):
                candidates.append((_parse_revision(item.name, prefix), item))
    except Exception:
        return None
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    for _, folder in candidates:
        for exe_path in candidates_fn(folder):
            if _validate_executable(exe_path):
                return folder
    return None


def _select_playwright_browser_dirs(source: Path) -> tuple[list[Path], str]:
    chromium_dir = _pick_latest_valid_dir(
        source, _PLAYWRIGHT_CHROMIUM_PREFIX, _chromium_exe_candidates
    )
    if chromium_dir:
        revision = chromium_dir.name[len(_PLAYWRIGHT_CHROMIUM_PREFIX) :]
        headless_dir = source / f"{_PLAYWRIGHT_HEADLESS_PREFIX}{revision}"
        if headless_dir.exists() and _dir_has_valid_executable(
            headless_dir, _headless_exe_candidates
        ):
            return [chromium_dir, headless_dir], "chromium_ok"
        if headless_dir.exists():
            return [chromium_dir], "chromium_ok_headless_invalid"
        return [chromium_dir], "chromium_ok_headless_missing"

    headless_dir = _pick_latest_valid_dir(
        source, _PLAYWRIGHT_HEADLESS_PREFIX, _headless_exe_candidates
    )
    if headless_dir:
        return [headless_dir], "headless_only"
    return [], "no_valid_browser"


def _parse_excludes() -> list[str]:
    extra = os.environ.get("PYINSTALLER_EXCLUDE_MODULES", "")
    extra_items = [item.strip() for item in extra.split(",") if item.strip()]
    seen = set()
    combined: list[str] = []
    for item in _DEFAULT_EXCLUDES + extra_items:
        if item in seen:
            continue
        seen.add(item)
        combined.append(item)
    return combined


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


def _ensure_playwright_available() -> tuple[bool, str]:
    try:
        import playwright  # noqa: F401
    except Exception as exc:
        return (
            False,
            "Playwright no disponible en el entorno de build. "
            f"Detalle: {exc}",
        )
    return True, ""


def _resolve_playwright_browsers_path() -> Optional[Path]:
    env_override = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env_override:
        candidate = Path(env_override).expanduser()
        if candidate.exists():
            return candidate

    if sys.platform.startswith("win"):
        local = os.environ.get("LOCALAPPDATA")
        if local:
            candidate = Path(local) / "ms-playwright"
            if candidate.exists():
                return candidate
        candidate = Path.home() / "AppData" / "Local" / "ms-playwright"
        if candidate.exists():
            return candidate
        return None

    if sys.platform == "darwin":
        candidate = Path.home() / "Library" / "Caches" / "ms-playwright"
    else:
        candidate = Path.home() / ".cache" / "ms-playwright"
    if candidate.exists():
        return candidate
    return None


def _playwright_bundle_mode() -> str:
    return os.environ.get("PLAYWRIGHT_BUNDLE", "all").strip().lower()


def _should_bundle_playwright_browsers() -> bool:
    return _playwright_bundle_mode() not in {"none", "external", "skip", "no"}


def _is_no_space_error(exc: BaseException) -> bool:
    if isinstance(exc, OSError):
        if exc.errno == errno.ENOSPC:
            return True
        if getattr(exc, "winerror", None) == 112:
            return True
    if isinstance(exc, shutil.Error):
        errors = exc.args[0] if exc.args else []
        if isinstance(errors, (list, tuple)):
            for entry in errors:
                detail = ""
                if isinstance(entry, tuple) and len(entry) >= 3:
                    detail = str(entry[2])
                else:
                    detail = str(entry)
                lower = detail.lower()
                if (
                    "no space left on device" in lower
                    or "espacio en disco insuficiente" in lower
                    or "winerror 112" in lower
                ):
                    return True
    return False


def _copy_playwright_subset(
    source: Path, target: Path, prefixes: tuple[str, ...]
) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        if item.is_file():
            shutil.copy2(item, target / item.name)
            continue
        if not any(item.name.startswith(prefix) for prefix in prefixes):
            continue
        dest = target / item.name
        shutil.copytree(item, dest)


def _copy_playwright_selected(source: Path, target: Path, selected_dirs: list[Path]) -> None:
    target.mkdir(parents=True, exist_ok=True)
    selected_names = {item.name for item in selected_dirs}
    for item in source.iterdir():
        if item.is_file():
            shutil.copy2(item, target / item.name)
            continue
        if not item.is_dir():
            continue
        if item.name in selected_names or item.name.startswith(("ffmpeg-", "winldd-")):
            shutil.copytree(item, target / item.name)


def _copy_playwright_browsers(
    dest_root: Path, source: Path, *, mode: str | None = None
) -> None:
    target = dest_root / _PLAYWRIGHT_BUNDLE_DIR
    if target.exists():
        shutil.rmtree(target)
    bundle_mode = (mode or _playwright_bundle_mode()).strip().lower()
    if bundle_mode in {"none", "external", "skip", "no"}:
        _log_step("Playwright browsers omitidos (modo external)")
        return
    selected_dirs, reason = _select_playwright_browser_dirs(source)
    if not selected_dirs:
        _log_step("No se encontro un Chromium sano para copiar.")
        return
    selected_names = ", ".join(item.name for item in selected_dirs)
    _log_step(f"Playwright seleccionado: {selected_names} ({reason})")
    _copy_playwright_selected(source, target, selected_dirs)


def build_for_license(
    record: Dict[str, str], *, name: str | None = None
) -> Tuple[bool, Path | None, str]:
    """Genera un ejecutable para la licencia suministrada."""

    root = Path(__file__).resolve().parents[1]
    launcher = root / "client_launcher.py"
    if not launcher.exists():
        return False, None, "No se encontró client_launcher.py"

    dist_dir = root / "dist"
    dist_dir.mkdir(exist_ok=True)

    exe_name = name or f"insta_cli_{_slugify(record.get('client_name') or 'cliente')}"

    build_root = _resolve_build_root()
    temp_base = Path(tempfile.mkdtemp(prefix="license_build_", dir=build_root))
    workspace = temp_base / "workspace"
    include_playwright = os.environ.get("INCLUDE_PLAYWRIGHT", "1") != "0"
    bundle_playwright = _should_bundle_playwright_browsers()
    onefile = os.environ.get("PYINSTALLER_ONEFILE", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    timeout_env = os.environ.get("PYINSTALLER_TIMEOUT")
    if timeout_env:
        build_timeout = int(timeout_env)
    else:
        build_timeout = 7200 if include_playwright else 1800
    log_path = dist_dir / f"{exe_name}_pyinstaller.log"

    try:
        playwright_src: Optional[Path] = None
        if include_playwright:
            ok_playwright, message = _ensure_playwright_available()
            if not ok_playwright:
                return False, None, message
            if bundle_playwright:
                playwright_src = _resolve_playwright_browsers_path()
                if not playwright_src:
                    return (
                        False,
                        None,
                        "No se encontraron los navegadores de Playwright. "
                        "Ejecuta: python -m playwright install",
                    )

        _log_step("Preparando workspace temporal...")
        _log_step(f"Directorio temporal: {build_root}")
        _log_step(f"Timeout PyInstaller: {build_timeout}s")
        _log_step(f"Modo PyInstaller: {'onefile' if onefile else 'onedir'}")
        start = time.perf_counter()
        try:
            _copy_project(root, workspace)
        except (OSError, shutil.Error) as exc:
            if _is_no_space_error(exc):
                return (
                    False,
                    None,
                    "Espacio insuficiente para preparar el workspace temporal. "
                    "Liberá espacio o definí LICENSE_BUILD_ROOT en un disco con espacio libre.",
                )
            raise
        _log_step(f"Workspace listo en {time.perf_counter() - start:.1f}s")

        _log_step("Sanitizando archivos de cliente...")
        _sanitize_tree(workspace)
        _log_step("Sanitizado listo")

        _log_step("Escribiendo .env de cliente...")
        _write_client_env(workspace)

        payload_path = workspace / "storage" / "license_payload.json"
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "license_key": record.get("license_key"),
            "client_name": record.get("client_name"),
            "expires_at": record.get("expires_at"),
            "status": record.get("status", "active"),
            "edition": "client",
        }
        payload_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        _log_step("Generando bundle limpio (zip) del workspace...")
        bundle_base = dist_dir / f"{exe_name}_source"
        if bundle_base.with_suffix(".zip").exists():
            bundle_base.with_suffix(".zip").unlink()
        archive_path = Path(
            shutil.make_archive(str(bundle_base), "zip", root_dir=workspace)
        )
        _log_step(f"Bundle limpio generado: {archive_path}")

        command = [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onefile" if onefile else "--onedir",
            "--name",
            exe_name,
            "--add-data",
            f"{payload_path}{os.pathsep}storage",
            "client_launcher.py",
        ]

        if include_playwright:
            command.extend(["--collect-all", "playwright"])
            if playwright_src:
                _log_step(f"Playwright browsers listos para copiar: {playwright_src}")
            elif not bundle_playwright:
                _log_step("Playwright browsers externos: no se copian al bundle")

        for module in _HIDDEN_IMPORTS:
            command.extend(["--hidden-import", module])

        for module in _parse_excludes():
            command.extend(["--exclude-module", module])

        _log_step(f"Log de PyInstaller: {log_path}")
        _log_step("Ejecutando PyInstaller (puede tardar varios minutos)...")
        start = time.monotonic()
        next_heartbeat = start + 30
        with log_path.open("w", encoding="utf-8", errors="ignore") as log_handle:
            proc = subprocess.Popen(
                command,
                cwd=workspace,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
        while True:
            retcode = proc.poll()
            if retcode is not None:
                if retcode != 0:
                    raise subprocess.CalledProcessError(retcode, command)
                break
            now = time.monotonic()
            if now - start >= build_timeout:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                raise subprocess.TimeoutExpired(command, build_timeout)
            if now >= next_heartbeat:
                elapsed = int(now - start)
                _log_step(f"Compilando... {elapsed}s")
                next_heartbeat = now + 30
            time.sleep(1)
        _log_step("PyInstaller finalizo correctamente")

        output = _guess_output(workspace / "dist", exe_name)
        if not output.exists():
            return False, None, "PyInstaller no generó el archivo esperado."

        final_output = dist_dir / output.name
        if final_output.exists():
            if final_output.is_dir():
                shutil.rmtree(final_output)
            else:
                final_output.unlink()
        shutil.move(str(output), final_output)
        if include_playwright and bundle_playwright and playwright_src:
            dest_root = final_output if final_output.is_dir() else final_output.parent
            _log_step("Copiando navegadores Playwright al bundle...")
            _copy_playwright_browsers(
                dest_root, playwright_src, mode=_playwright_bundle_mode()
            )
            _log_step("Navegadores Playwright copiados")
        message = (
            f"Ejecutable generado en {final_output} (bundle limpio: {archive_path})"
        )
        return True, final_output, message
    except subprocess.CalledProcessError as exc:
        tail = _tail_log(log_path)
        detail = f"Error al ejecutar PyInstaller: {exc}"
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
    except subprocess.TimeoutExpired:
        tail = _tail_log(log_path)
        detail = (
            "PyInstaller tardo demasiado y fue detenido. "
            "Proba de nuevo o aumenta PYINSTALLER_TIMEOUT."
        )
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
    finally:
        shutil.rmtree(temp_base, ignore_errors=True)
