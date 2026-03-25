from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import errno
from pathlib import Path
from typing import Optional

from tools.build_executable import (
    _copy_playwright_browsers,
    _copy_project,
    _ensure_playwright_available,
    _parse_collect_all_modules,
    _parse_excludes,
    _parse_hidden_imports,
    _prune_runtime_tree,
    _resolve_playwright_browsers_path,
)

PRODUCT_NAME = "InstaCRM"
CLIENT_EXE_STEM = "InstaCRM"
OWNER_EXE_STEM = "InstaCRMOwner"


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def dist_root() -> Path:
    path = project_root() / "dist"
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_temp_root() -> Path:
    override = (os.environ.get("BUILD_TEMP_DIR") or "").strip()
    if override:
        path = Path(override).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path
    return Path(tempfile.gettempdir())


def current_version() -> str:
    root = project_root()
    for candidate in (
        root / "app_version.json",
        root / "storage" / "app_version.json",
        root / "update_manifest.json",
        root / "storage" / "update_manifest.json",
        root / "VERSION",
    ):
        if not candidate.exists():
            continue
        if candidate.suffix.lower() == ".json":
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except Exception:
                continue
            version = str(payload.get("version") or payload.get("app_version") or "").strip()
            if version:
                return version
            continue
        try:
            value = candidate.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if value:
            return value
    return os.environ.get("BUILD_VERSION", "").strip() or "dev"


def prepare_workspace(prefix: str) -> tuple[Path, Path]:
    temp_root = Path(tempfile.mkdtemp(prefix=f"{prefix}_", dir=build_temp_root()))
    workspace = temp_root / "workspace"
    try:
        _copy_project(project_root(), workspace)
        for name in (".env", ".env.local", "dist", "build", "storage", "logs", "updates", "data"):
            target = workspace / name
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            elif target.exists():
                target.unlink()
        _prune_runtime_tree(workspace)
        return temp_root, workspace
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def _artifact_path(workspace: Path, exe_name: str) -> Path:
    if sys.platform.startswith("win"):
        return workspace / "dist" / f"{exe_name}.exe"
    return workspace / "dist" / exe_name


def build_onefile_executable(
    workspace: Path,
    *,
    entrypoint: str,
    exe_name: str,
    include_playwright: bool = True,
    windowed: bool = True,
) -> Path:
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--name",
        exe_name,
    ]
    if windowed:
        command.append("--windowed")

    styles_path = workspace / "styles.qss"
    if styles_path.exists():
        command.extend(["--add-data", f"{styles_path}{os.pathsep}."])

    if include_playwright:
        ok, message = _ensure_playwright_available()
        if not ok:
            raise RuntimeError(message)
        command.extend(["--collect-all", "playwright"])

    for module in _parse_collect_all_modules():
        command.extend(["--collect-all", module])
    for module in _parse_hidden_imports():
        command.extend(["--hidden-import", module])
    for module in _parse_excludes():
        command.extend(["--exclude-module", module])

    command.append(entrypoint)
    subprocess.run(command, cwd=workspace, check=True)

    artifact = _artifact_path(workspace, exe_name)
    if not artifact.exists():
        raise FileNotFoundError(f"PyInstaller no generó {artifact}")

    target = dist_root() / artifact.name
    return _move_or_copy(artifact, target)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _copy_optional(src: Optional[Path], dest: Path) -> None:
    if src is None or not src.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def _move_or_copy(src: Path, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        if dest.is_dir():
            shutil.rmtree(dest, ignore_errors=True)
        else:
            dest.unlink()
    try:
        return src.replace(dest)
    except OSError as exc:
        if exc.errno not in {errno.EXDEV, errno.EACCES, errno.EPERM}:
            raise
    shutil.copy2(src, dest)
    return dest


def _default_client_env() -> str:
    overweight_threshold = (
        os.environ.get("LEADS_IMAGE_OVERWEIGHT_THRESHOLD", "0.56").strip() or "0.56"
    )
    lines = [
        "CLIENT_DISTRIBUTION=1",
        "HUMAN_DM_ALLOW_UNVERIFIED=1",
        f"LEADS_IMAGE_OVERWEIGHT_THRESHOLD={overweight_threshold}",
    ]
    remote_only = (os.environ.get("LICENSE_REMOTE_ONLY") or "").strip()
    if remote_only:
        lines.append(f"LICENSE_REMOTE_ONLY={remote_only}")
    return "\n".join(lines) + "\n"


def _find_license_payload(explicit_path: Optional[Path]) -> Optional[Path]:
    candidates = []
    if explicit_path is not None:
        candidates.append(explicit_path)
    root = project_root()
    candidates.extend(
        [
            root / "license.key",
            root / "license_payload.json",
            root / "license.json",
            root / "storage" / "license.key",
            root / "storage" / "license_payload.json",
            root / "storage" / "license.json",
        ]
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _extract_license_key(path: Path) -> str:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
    if not raw:
        return ""
    if path.suffix.lower() == ".json" or raw.startswith("{"):
        try:
            payload = json.loads(raw)
        except Exception:
            return ""
        if isinstance(payload, dict):
            for key in ("license_key", "key"):
                value = str(payload.get(key) or "").strip()
                if value:
                    return value
    for line in raw.splitlines():
        value = str(line or "").strip()
        if value:
            return value
    return ""


def _resolve_license_key(
    *,
    explicit_license_key: str = "",
    license_payload: Optional[Path] = None,
) -> str:
    key = str(explicit_license_key or "").strip()
    if key:
        return key
    payload_path = _find_license_payload(license_payload)
    if payload_path is None:
        return ""
    return _extract_license_key(payload_path)


def _copy_release_metadata(
    app_dir: Path,
    *,
    channel: str,
    version: str,
    executable_name: str,
) -> None:
    root = project_root()
    _copy_optional(root / "styles.qss", app_dir / "styles.qss")
    _copy_optional(root / "update_manifest.json", app_dir / "update_manifest.json")
    _copy_optional(root / "storage" / "update_manifest.json", app_dir / "update_manifest.json")
    _write_json(
        app_dir / "app_version.json",
        {
            "product_name": PRODUCT_NAME,
            "version": version,
            "channel": channel,
            "layout": "instacrm.v1",
            "update_mode": "full-package",
            "executable_name": executable_name,
        },
    )


def _create_layout_dirs(target_dir: Path) -> None:
    directories = (
        target_dir / "app",
        target_dir / "data",
        target_dir / "data" / "accounts",
        target_dir / "data" / "lead_filters",
        target_dir / "data" / "leads",
        target_dir / "data" / "campaigns",
        target_dir / "data" / "stations",
        target_dir / "data" / "totp",
        target_dir / "runtime",
        target_dir / "runtime" / "browser_profiles",
        target_dir / "runtime" / "sessions",
        target_dir / "runtime" / "screenshots",
        target_dir / "runtime" / "traces",
        target_dir / "runtime" / "artifacts",
        target_dir / "logs",
        target_dir / "updates",
        target_dir / "updates" / "backups",
        target_dir / "updates" / "cache",
        target_dir / "updates" / "staging",
        target_dir / "updates" / "state",
        target_dir / "updates" / "quarantine",
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def assemble_client_layout(
    built_executable: Path,
    *,
    target_dir: Path,
    version: str,
    license_payload: Optional[Path] = None,
    license_key: str = "",
    bundle_playwright: bool = True,
) -> Path:
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    _create_layout_dirs(target_dir)

    executable_name = f"{CLIENT_EXE_STEM}.exe" if sys.platform.startswith("win") else CLIENT_EXE_STEM
    _move_or_copy(built_executable, target_dir / executable_name)

    app_dir = target_dir / "app"
    _copy_release_metadata(
        app_dir,
        channel="client",
        version=version,
        executable_name=executable_name,
    )
    (app_dir / ".env").write_text(_default_client_env(), encoding="utf-8")

    resolved_license_key = _resolve_license_key(
        explicit_license_key=license_key,
        license_payload=license_payload,
    )
    if resolved_license_key:
        for destination in (target_dir / "license.key", app_dir / "license.key"):
            destination.write_text(resolved_license_key + "\n", encoding="utf-8")

    if bundle_playwright:
        playwright_root = _resolve_playwright_browsers_path()
        if playwright_root is not None:
            _copy_playwright_browsers(app_dir, playwright_root)

    return target_dir


def assemble_owner_layout(
    built_executable: Path,
    *,
    target_dir: Path,
    version: str,
    bundle_playwright: bool = True,
) -> Path:
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    _create_layout_dirs(target_dir)

    executable_name = f"{OWNER_EXE_STEM}.exe" if sys.platform.startswith("win") else OWNER_EXE_STEM
    _move_or_copy(built_executable, target_dir / executable_name)

    app_dir = target_dir / "app"
    _copy_release_metadata(
        app_dir,
        channel="owner",
        version=version,
        executable_name=executable_name,
    )

    if bundle_playwright:
        playwright_root = _resolve_playwright_browsers_path()
        if playwright_root is not None:
            _copy_playwright_browsers(app_dir, playwright_root)

    return target_dir


def build_client_distribution(
    *,
    target_dir: Path,
    version: str = "",
    license_payload: Optional[Path] = None,
    license_key: str = "",
    bundle_playwright: bool = True,
) -> Path:
    resolved_version = str(version or current_version()).strip() or "dev"
    temp_root, workspace = prepare_workspace("instacrm_client_build")
    built: Optional[Path] = None
    try:
        built = build_onefile_executable(
            workspace,
            entrypoint="launchers/client_launcher.py",
            exe_name=CLIENT_EXE_STEM,
            include_playwright=True,
            windowed=True,
        )
        return assemble_client_layout(
            built,
            target_dir=target_dir,
            version=resolved_version,
            license_payload=license_payload,
            license_key=license_key,
            bundle_playwright=bundle_playwright,
        )
    finally:
        if built is not None and built.exists():
            if built.is_dir():
                shutil.rmtree(built, ignore_errors=True)
            else:
                built.unlink()
        shutil.rmtree(temp_root, ignore_errors=True)


def package_directory(folder: Path, zip_path: Path) -> Path:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    archive_base = zip_path.with_suffix("")
    generated = Path(
        shutil.make_archive(str(archive_base), "zip", root_dir=folder.parent, base_dir=folder.name)
    )
    if generated != zip_path:
        if zip_path.exists():
            zip_path.unlink()
        generated.replace(zip_path)
    return zip_path
