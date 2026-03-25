# tools/build_executable.py
# -*- coding: utf-8 -*-
"""Utilidad para generar ejecutables por licencia."""

from __future__ import annotations

import errno
import fnmatch
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from collections import deque
from typing import Dict, Optional, TextIO, Tuple


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
    base_patterns = (
        "venv*",
        ".venv*",
        ".venv_models",
        ".codex_*",
        ".tmp*",
        ".vscode",
        ".idea",
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
        "sessions",
        "browser_sessions",
        "browsers",
        "ms-playwright",
        "playwright_browsers",
        "profiles",
        "legacy",
        "whatsapp_exports",
        "storage",
        "data",
        "text",
        "accounts",
        "models",
        "_archive",
        "tests",
        "docs",
        "cloudflare",
        "node_modules",
        "logs",
        "updates",
    )

    def ignore(current_dir: str, names: list[str]) -> set[str]:
        current_path = Path(current_dir)
        ignored = {
            name for name in names if any(fnmatch.fnmatch(name, pattern) for pattern in base_patterns)
        }
        try:
            relative_parts = current_path.relative_to(src).parts
        except Exception:
            relative_parts = ()

        if relative_parts and relative_parts[0] == "runtime":
            ignored.update(name for name in names if name in _RUNTIME_TRANSIENT_DIRS)
        if relative_parts == ("tools",) and "build_artifacts" in names:
            ignored.add("build_artifacts")
        return ignored

    shutil.copytree(src, dest, ignore=ignore)


_RUNTIME_TRANSIENT_DIRS = (
    "artifacts",
    "browser_profiles",
    "browsers",
    "logs",
    "models",
    "playwright",
    "screenshots",
    "sessions",
    "traces",
    "__pycache__",
)


def _prune_runtime_tree(root: Path) -> None:
    runtime_dir = root / "runtime"
    if not runtime_dir.exists():
        return

    for name in _RUNTIME_TRANSIENT_DIRS:
        target = runtime_dir / name
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink()

    for pattern in ("*.pyc", "*.pyo"):
        for item in runtime_dir.glob(pattern):
            item.unlink(missing_ok=True)


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

    _prune_runtime_tree(root)


def _write_client_env(root: Path) -> None:
    env_path = root / ".env"
    overweight_threshold = (
        os.environ.get("LEADS_IMAGE_OVERWEIGHT_THRESHOLD", "0.56").strip() or "0.56"
    )
    lines = [
        "CLIENT_DISTRIBUTION=1",
        "HUMAN_DM_ALLOW_UNVERIFIED=1",
        f"LEADS_IMAGE_OVERWEIGHT_THRESHOLD={overweight_threshold}",
    ]
    remote_only = os.environ.get("LICENSE_REMOTE_ONLY")
    if remote_only:
        lines.append(f"LICENSE_REMOTE_ONLY={remote_only}")
    lines.append("# Configure tus propias claves aqui")
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _log_step(message: str) -> None:
    print(message, flush=True)


_HIDDEN_IMPORTS = [
    "core.accounts",
    "automation.actions.content_publisher",
    "automation.actions.interactions",
    "automation.actions.interactions_adapters",
    "app",
    "backend_license_client",
    "config",
    "gui.gui_app",
    "core.ig",
    "core.leads",
    "licensekit",
    "gui.main_window",
    "media_norm",
    "proxy_manager",
    "core.responder",
    "runtime.runtime",
    "runtime.runtime_parity",
    "sdk_sanitize",
    "core.session_store",
    "state_view",
    "src.analytics.stats_engine",
    "core.storage",
    "core.totp_store",
    "ui",
    "update_system",
    "utils",
    "automation.whatsapp",
]

_AI_HIDDEN_IMPORTS = [
    "src.image_attribute_filter",
    "src.image_prompt_parser",
    "src.image_rule_evaluator",
    "src.vision.face_detector_scrfd",
    "src.vision.fairface_analyzer",
    "src.vision.gender_age_analyzer",
]

# Dependencias requeridas por pkg_resources/pyi_rth_pkgres
_EXTRA_HIDDEN_IMPORTS = [
    "jaraco",
    "jaraco.text",
    "jaraco.classes",
    "jaraco.functools",
    "pkg_resources",
    "setuptools",
]

_AI_EXTRA_HIDDEN_IMPORTS = [
    "onnxruntime",
]

_COLLECT_ALL_BASE = [
    "openpyxl",
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
_EMBEDDED_CHROMIUM_REVISION = "1200"


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


def _extract_chromium_revision_from_browsers_json(path: Path) -> Optional[str]:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    browsers = payload.get("browsers")
    if not isinstance(browsers, list):
        return None
    for row in browsers:
        if not isinstance(row, dict):
            continue
        if str(row.get("name") or "").strip().lower() != "chromium":
            continue
        revision = str(row.get("revision") or "").strip()
        if revision:
            return revision
    return None


def _expected_chromium_revision() -> Optional[str]:
    override = (
        os.environ.get("PLAYWRIGHT_BUNDLE_REVISION")
        or os.environ.get("PLAYWRIGHT_CHROMIUM_REVISION")
        or ""
    ).strip()
    if override:
        return override

    # Default to the revision expected by the installed Playwright package, so
    # the bundled browsers match the runtime dependencies.
    try:
        import playwright  # type: ignore

        browsers_json = (
            Path(playwright.__file__).resolve().parent / "driver" / "package" / "browsers.json"
        )
        revision = _extract_chromium_revision_from_browsers_json(browsers_json)
        if revision:
            return revision
    except Exception:
        pass

    return _EMBEDDED_CHROMIUM_REVISION


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


def _select_standalone_executable(root: Path) -> Optional[Path]:
    for exe_path in _standalone_chrome_candidates(root):
        if _validate_executable(exe_path):
            return exe_path
    return None


def _select_playwright_browser_dirs(source: Path) -> tuple[list[Path], str]:
    target_revision = _expected_chromium_revision()
    if not target_revision:
        return [], "chromium_revision_missing"
    chromium_dir = source / f"{_PLAYWRIGHT_CHROMIUM_PREFIX}{target_revision}"
    if not (
        chromium_dir.exists()
        and chromium_dir.is_dir()
        and _dir_has_valid_executable(chromium_dir, _chromium_exe_candidates)
    ):
        return [], "chromium_missing_or_invalid"

    selected = [chromium_dir]
    headless_dir = source / f"{_PLAYWRIGHT_HEADLESS_PREFIX}{target_revision}"
    if headless_dir.exists() and headless_dir.is_dir():
        if _dir_has_valid_executable(headless_dir, _headless_exe_candidates):
            selected.append(headless_dir)
            return selected, "chromium_with_headless"
        return selected, "chromium_headless_invalid"
    return selected, "chromium_headless_missing"


def _parse_excludes() -> list[str]:
    heavy_flag = os.environ.get("PYINSTALLER_EXCLUDE_HEAVY", "1").strip().lower()
    use_heavy = heavy_flag not in {"0", "false", "no", "off", "skip"}
    extra = os.environ.get("PYINSTALLER_EXCLUDE_MODULES", "")
    extra_items = [item.strip() for item in extra.split(",") if item.strip()]
    seen = set()
    combined: list[str] = []
    base = _DEFAULT_EXCLUDES + (_HEAVY_EXCLUDES if use_heavy else [])
    for item in base + extra_items:
        if item in seen:
            continue
        seen.add(item)
        combined.append(item)
    return combined


def _include_ai_modules() -> bool:
    return os.environ.get("PYINSTALLER_INCLUDE_AI", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def _parse_hidden_imports() -> list[str]:
    combined = list(_HIDDEN_IMPORTS) + list(_EXTRA_HIDDEN_IMPORTS)
    if _include_ai_modules():
        combined.extend(_AI_HIDDEN_IMPORTS)
        combined.extend(_AI_EXTRA_HIDDEN_IMPORTS)
    return combined


def _parse_collect_all_modules() -> list[str]:
    override = os.environ.get("PYINSTALLER_COLLECT_ALL", "").strip()
    if override:
        if override.lower() in {"none", "0", "false", "no", "skip"}:
            return []
        items = [item.strip() for item in override.split(",") if item.strip()]
        return items
    modules = list(_COLLECT_ALL_BASE)
    include_ai = _include_ai_modules()
    if include_ai:
        modules.extend(_COLLECT_ALL_AI)
    extra = os.environ.get("PYINSTALLER_COLLECT_ALL_EXTRA", "").strip()
    if extra:
        for item in extra.split(","):
            item = item.strip()
            if item and item not in modules:
                modules.append(item)
    return modules


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

    project_root = Path(__file__).resolve().parents[1]

    def _has_versioned_playwright_layout(path: Path) -> bool:
        selected_dirs, _ = _select_playwright_browser_dirs(path)
        return bool(selected_dirs)

    preferred_versioned: list[Path] = []

    if sys.platform.startswith("win"):
        local = os.environ.get("LOCALAPPDATA")
        if local:
            preferred_versioned.append(Path(local) / "ms-playwright")
        preferred_versioned.append(Path.home() / "AppData" / "Local" / "ms-playwright")
    elif sys.platform == "darwin":
        preferred_versioned.append(Path.home() / "Library" / "Caches" / "ms-playwright")
    else:
        preferred_versioned.append(Path.home() / ".cache" / "ms-playwright")

    preferred_versioned.extend(
        [
            project_root / "runtime" / "playwright",
            project_root / "runtime" / "browsers",
            project_root / "ms-playwright",
            project_root / _PLAYWRIGHT_BUNDLE_DIR,
            project_root / "playwright",
            project_root,
        ]
    )

    seen_versioned: set[str] = set()
    for candidate in preferred_versioned:
        key = str(candidate)
        if key in seen_versioned:
            continue
        seen_versioned.add(key)
        if candidate.exists() and _has_versioned_playwright_layout(candidate):
            return candidate

    return None


def _playwright_bundle_mode() -> str:
    return os.environ.get("PLAYWRIGHT_BUNDLE", "all").strip().lower()


def _build_mode() -> str:
    return os.environ.get("BUILD_MODE", "full").strip().lower()


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


def _copy_standalone_browser(target: Path, executable: Path) -> None:
    browser_dir = executable.parent
    destination = target / "browsers" / browser_dir.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(browser_dir, destination)


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
    if selected_dirs:
        selected_names = ", ".join(item.name for item in selected_dirs)
        _log_step(f"Playwright seleccionado: {selected_names} ({reason})")
        _copy_playwright_selected(source, target, selected_dirs)
        return
    _log_step("No se encontro un Chromium de Playwright valido para copiar.")


def build_for_license(
    record: Dict[str, str], *, name: str | None = None
) -> Tuple[bool, Path | None, str]:
    """Genera un ejecutable para la licencia suministrada."""

    root = Path(__file__).resolve().parents[1]
    launcher = root / "launchers" / "client_launcher.py"
    if not launcher.exists():
        return False, None, "No se encontró launchers/client_launcher.py"

    dist_dir = root / "dist"
    dist_dir.mkdir(exist_ok=True)

    exe_name = name or f"insta_cli_{_slugify(record.get('client_name') or 'cliente')}"

    build_root = _resolve_build_root()
    temp_base = Path(tempfile.mkdtemp(prefix="license_build_", dir=build_root))
    workspace = temp_base / "workspace"
    build_mode = _build_mode()
    minimal_mode = build_mode == "minimal"
    include_playwright = os.environ.get("INCLUDE_PLAYWRIGHT", "1") != "0"
    bundle_playwright = _should_bundle_playwright_browsers()
    onefile = os.environ.get("PYINSTALLER_ONEFILE", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    windowed = os.environ.get("PYINSTALLER_WINDOWED", "1").strip().lower() in {
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
    log_path: Optional[Path] = None
    pyinstaller_tail = ""

    try:
        stale_source_bundle = dist_dir / f"{exe_name}_source.zip"
        if stale_source_bundle.exists() and stale_source_bundle.is_file():
            stale_source_bundle.unlink()
        if minimal_mode:
            stale_log = dist_dir / f"{exe_name}_pyinstaller.log"
            if stale_log.exists() and stale_log.is_file():
                stale_log.unlink()

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
                        "Asegura playwright_browsers/chromium-* (o ms-playwright/chromium-*) "
                        "y ejecuta: python -m playwright install chromium",
                    )

        _log_step("Preparando workspace temporal...")
        _log_step(f"Directorio temporal: {build_root}")
        _log_step(f"Timeout PyInstaller: {build_timeout}s")
        _log_step(f"Modo PyInstaller: {'onefile' if onefile else 'onedir'}")
        _log_step(f"Modo ventana (sin consola): {'on' if windowed else 'off'}")
        _log_step(f"Modo build: {'minimal' if minimal_mode else 'full'}")
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

        license_key_path = workspace / "license.key"
        license_key = str(record.get("license_key") or "").strip()
        if license_key:
            license_key_path.write_text(license_key + "\n", encoding="utf-8")

        if minimal_mode:
            _log_step("Bundle limpio omitido (modo minimal)")
        else:
            _log_step("Bundle limpio omitido (solo carpeta distribuible)")

        styles_path = workspace / "styles.qss"

        command = [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onefile" if onefile else "--onedir",
            "--name",
            exe_name,
            "launchers/client_launcher.py",
        ]
        if license_key:
            command.extend(
                [
                    "--add-data",
                    f"{license_key_path}{os.pathsep}.",
                ]
            )
        if windowed:
            command.append("--windowed")
        if styles_path.exists():
            command.extend(
                [
                    "--add-data",
                    f"{styles_path}{os.pathsep}.",
                ]
            )
            _log_step(f"Incluyendo stylesheet: {styles_path}")
        else:
            _log_step("styles.qss no encontrado en workspace; build seguira sin tema visual.")
        collect_all_modules = _parse_collect_all_modules()
        if collect_all_modules:
            _log_step(f"PyInstaller collect-all: {', '.join(collect_all_modules)}")
        else:
            _log_step("PyInstaller collect-all: (ninguno)")

        if include_playwright:
            command.extend(["--collect-all", "playwright"])
            if playwright_src:
                _log_step(f"Playwright browsers listos para copiar: {playwright_src}")
            elif not bundle_playwright:
                _log_step("Playwright browsers externos: no se copian al bundle")

        for module in collect_all_modules:
            command.extend(["--collect-all", module])

        hidden_imports = _parse_hidden_imports()
        if _include_ai_modules():
            _log_step("PyInstaller AI stack: habilitado")
        else:
            _log_step("PyInstaller AI stack: omitido (usa PYINSTALLER_INCLUDE_AI=1 para incluirlo)")
        for module in hidden_imports:
            command.extend(["--hidden-import", module])

        for module in _parse_excludes():
            command.extend(["--exclude-module", module])

        if minimal_mode:
            _log_step("Log de PyInstaller omitido (modo minimal)")
            log_handle_cm = tempfile.SpooledTemporaryFile(
                max_size=10 * 1024 * 1024,
                mode="w+",
                encoding="utf-8",
                errors="ignore",
            )
        else:
            log_path = dist_dir / f"{exe_name}_pyinstaller.log"
            _log_step(f"Log de PyInstaller: {log_path}")
            log_handle_cm = log_path.open("w+", encoding="utf-8", errors="ignore")
        _log_step("Ejecutando PyInstaller (puede tardar varios minutos)...")
        start = time.monotonic()
        next_heartbeat = start + 30
        with log_handle_cm as log_handle:
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
                        pyinstaller_tail = _tail_stream(log_handle)
                        raise subprocess.CalledProcessError(retcode, command)
                    break
                now = time.monotonic()
                if now - start >= build_timeout:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    pyinstaller_tail = _tail_stream(log_handle)
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
        return True, final_output, f"Ejecutable generado en {final_output}"
    except subprocess.CalledProcessError as exc:
        tail = pyinstaller_tail or (_tail_log(log_path) if log_path else "")
        detail = f"Error al ejecutar PyInstaller: {exc}"
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
    except subprocess.TimeoutExpired:
        tail = pyinstaller_tail or (_tail_log(log_path) if log_path else "")
        detail = (
            "PyInstaller tardo demasiado y fue detenido. "
            "Proba de nuevo o aumenta PYINSTALLER_TIMEOUT."
        )
        if tail:
            detail = f"{detail}\nUltimas lineas:\n{tail}"
        return False, None, detail
    finally:
        shutil.rmtree(temp_base, ignore_errors=True)
