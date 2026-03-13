from __future__ import annotations

import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from core.disk_monitor import emit_disk_warnings
from core.log_rotation import run_retention_maintenance
from paths import (
    accounts_root,
    app_root,
    artifacts_root,
    browser_binaries_root,
    browser_profiles_root,
    leads_root,
    logs_root,
    runtime_base,
    runtime_root,
    screenshots_root,
    sessions_root,
    storage_root,
    traces_root,
    updates_root,
)

from .observability import (
    build_support_diagnostic_bundle,
    record_critical_error,
    record_system_event,
    update_local_heartbeat,
    write_startup_diagnostic,
)

_BOOTSTRAP_CACHE: dict[str, "BootstrapContext"] = {}
_STALE_FILE_PATTERNS = ("*.tmp", "*.partial", "*.part", "*.staged", "*.download")
_STALE_RUNTIME_SECONDS = 24 * 3600
_STALE_UPDATE_SECONDS = 6 * 3600


@dataclass(frozen=True)
class BootstrapContext:
    mode: str
    install_root: Path
    app_root: Path
    data_root: Path
    runtime_root: Path
    logs_root: Path
    updates_root: Path
    layout: str
    frozen: bool
    startup_id: str
    cleanup_summary: dict[str, Any]
    recovery_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    preflight_summary: dict[str, Any]


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _install_root(mode: str, install_root_hint: Path | None = None) -> Path:
    for env_name in ("INSTACRM_INSTALL_ROOT", "APP_DATA_ROOT"):
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            return Path(raw).expanduser()
    if install_root_hint is not None:
        return Path(install_root_hint).expanduser()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _project_root()


def _detect_client_layout(install_root: Path) -> bool:
    if _truthy(os.environ.get("INSTACRM_FORCE_CLIENT_LAYOUT")):
        return True
    markers = (
        install_root / "app",
        install_root / "data",
        install_root / "updates",
    )
    return any(marker.exists() for marker in markers)


def _resolve_app_root(install_root: Path, app_root_hint: Path | None = None) -> Path:
    raw = (os.environ.get("INSTACRM_APP_ROOT") or "").strip()
    if raw:
        return Path(raw).expanduser()
    if app_root_hint is not None:
        return Path(app_root_hint).expanduser()
    candidate = install_root / "app"
    if candidate.exists():
        return candidate
    return _project_root()


def _new_context(mode: str, *, app_root_hint: Path | None = None, install_root_hint: Path | None = None) -> BootstrapContext:
    normalized_mode = "client" if str(mode or "").strip().lower() == "client" else "owner"
    install = _install_root(normalized_mode, install_root_hint=install_root_hint).resolve()
    client_layout = _detect_client_layout(install)
    app_dir = _resolve_app_root(install, app_root_hint=app_root_hint).resolve()
    if client_layout:
        data_dir = (install / "data").resolve()
        runtime_dir = (install / "runtime").resolve()
        logs_dir = (install / "logs").resolve()
        updates_dir = (install / "updates").resolve()
        layout = "client-layout"
    else:
        data_dir = storage_root(install).resolve()
        runtime_dir = runtime_root(install).resolve()
        logs_dir = logs_root(install).resolve()
        updates_dir = updates_root(install).resolve()
        layout = "legacy-layout"
    startup_id = uuid.uuid4().hex[:12]
    return BootstrapContext(
        mode=normalized_mode,
        install_root=install,
        app_root=app_dir,
        data_root=data_dir,
        runtime_root=runtime_dir,
        logs_root=logs_dir,
        updates_root=updates_dir,
        layout=layout,
        frozen=bool(getattr(sys, "frozen", False)),
        startup_id=startup_id,
        cleanup_summary={},
        recovery_summary={},
        validation_summary={},
        preflight_summary={},
    )


def _export_env(ctx: BootstrapContext) -> None:
    os.environ["INSTACRM_INSTALL_ROOT"] = str(ctx.install_root)
    os.environ["INSTACRM_APP_ROOT"] = str(ctx.app_root)
    os.environ["INSTACRM_DATA_ROOT"] = str(ctx.data_root)
    os.environ["INSTACRM_RUNTIME_ROOT"] = str(ctx.runtime_root)
    os.environ["INSTACRM_LOGS_ROOT"] = str(ctx.logs_root)
    os.environ["INSTACRM_UPDATES_ROOT"] = str(ctx.updates_root)
    os.environ["INSTACRM_BOOTSTRAPPED"] = "1"
    os.environ["INSTACRM_BOOTSTRAP_MODE"] = ctx.mode
    os.environ["INSTACRM_STARTUP_ID"] = ctx.startup_id
    os.environ["APP_DATA_ROOT"] = str(ctx.install_root)
    if ctx.layout == "client-layout":
        os.environ["INSTACRM_FORCE_CLIENT_LAYOUT"] = "1"


def _ensure_directories(ctx: BootstrapContext) -> None:
    required = (
        ctx.install_root,
        ctx.app_root,
        ctx.data_root,
        ctx.runtime_root,
        ctx.logs_root,
        ctx.updates_root,
        accounts_root(ctx.install_root),
        leads_root(ctx.install_root),
        browser_profiles_root(ctx.install_root),
        browser_binaries_root(ctx.install_root),
        sessions_root(ctx.install_root),
        screenshots_root(ctx.install_root),
        traces_root(ctx.install_root),
        artifacts_root(ctx.install_root),
        ctx.updates_root / "staging",
        ctx.updates_root / "state",
        ctx.updates_root / "backups",
        ctx.updates_root / "quarantine",
    )
    for directory in required:
        directory.mkdir(parents=True, exist_ok=True)


def _cleanup_stale_files(root: Path, *, older_than_seconds: int) -> list[str]:
    removed: list[str] = []
    now = time.time()
    if not root.exists():
        return removed
    for pattern in _STALE_FILE_PATTERNS:
        for candidate in root.rglob(pattern):
            if not candidate.is_file():
                continue
            try:
                age = now - candidate.stat().st_mtime
            except Exception:
                continue
            if age < older_than_seconds:
                continue
            try:
                candidate.unlink()
                removed.append(str(candidate))
            except Exception:
                continue
    return removed


def _quarantine_path(ctx: BootstrapContext, name: str) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    target = ctx.updates_root / "quarantine" / f"{stamp}_{name}"
    counter = 1
    while target.exists():
        target = ctx.updates_root / "quarantine" / f"{stamp}_{counter}_{name}"
        counter += 1
    return target


def _recover_update_state(ctx: BootstrapContext) -> dict[str, Any]:
    state_dir = ctx.updates_root / "state"
    staging_dir = ctx.updates_root / "staging"
    pending_path = state_dir / "pending_update.json"
    applying_path = state_dir / "apply_in_progress.json"
    recovered: list[str] = []
    issues: list[str] = []

    def _quarantine_candidate(path: Path, label: str) -> None:
        if not path.exists():
            return
        target = _quarantine_path(ctx, label)
        try:
            shutil.move(str(path), str(target))
            recovered.append(f"{path.name} -> {target.name}")
        except Exception as exc:
            issues.append(f"quarantine_failed:{path.name}:{exc}")

    if applying_path.exists():
        issues.append("interrupted_update_detected")
        _quarantine_candidate(staging_dir, "staging")
        _quarantine_candidate(applying_path, "apply_in_progress.json")
        if pending_path.exists():
            _quarantine_candidate(pending_path, "pending_update.json")

    elif pending_path.exists():
        if not staging_dir.exists() or not any(staging_dir.iterdir()):
            issues.append("pending_update_without_staging")
            _quarantine_candidate(pending_path, "pending_update.json")

    for artifact in (
        ctx.install_root / "app.__new__",
        ctx.install_root / "app.__old__",
    ):
        if artifact.exists():
            issues.append(f"stale_update_artifact:{artifact.name}")
            _quarantine_candidate(artifact, artifact.name)

    for artifact in ctx.install_root.glob("*.new"):
        if artifact.is_file():
            issues.append(f"stale_update_artifact:{artifact.name}")
            _quarantine_candidate(artifact, artifact.name)

    return {
        "issues": issues,
        "recovered": recovered,
        "safe_mode_candidate": bool(issues),
    }


def _validate_state(ctx: BootstrapContext) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    for label, directory in (
        ("install_root", ctx.install_root),
        ("app_root", ctx.app_root),
        ("data_root", ctx.data_root),
        ("runtime_root", ctx.runtime_root),
        ("logs_root", ctx.logs_root),
        ("updates_root", ctx.updates_root),
    ):
        if directory.exists():
            continue
        issues.append(
            {
                "level": "critical",
                "code": "missing_directory",
                "message": f"{label} missing: {directory}",
            }
        )
    app_root_sparse = True
    if ctx.app_root.exists():
        try:
            app_root_sparse = not any(ctx.app_root.iterdir())
        except Exception:
            app_root_sparse = True
    if ctx.layout == "client-layout" and app_root_sparse:
        issues.append(
            {
                "level": "warning",
                "code": "app_root_sparse",
                "message": f"App root looks sparse: {ctx.app_root}",
            }
        )
    return {
        "issues": issues,
        "critical_count": sum(1 for issue in issues if issue.get("level") == "critical"),
        "warning_count": sum(1 for issue in issues if issue.get("level") == "warning"),
    }


def _run_cleanup(ctx: BootstrapContext) -> dict[str, Any]:
    retention = run_retention_maintenance(ctx.install_root)
    runtime_removed = _cleanup_stale_files(ctx.runtime_root, older_than_seconds=_STALE_RUNTIME_SECONDS)
    update_removed = _cleanup_stale_files(ctx.updates_root, older_than_seconds=_STALE_UPDATE_SECONDS)
    return {
        "retention": retention,
        "runtime_removed": runtime_removed,
        "update_removed": update_removed,
    }


def bootstrap_application(
    mode: str | None,
    *,
    app_root_hint: Path | None = None,
    install_root_hint: Path | None = None,
    force: bool = False,
) -> BootstrapContext:
    ctx = _new_context(str(mode or ""), app_root_hint=app_root_hint, install_root_hint=install_root_hint)
    cache_key = f"{ctx.mode}|{ctx.install_root}"
    if not force and cache_key in _BOOTSTRAP_CACHE:
        return _BOOTSTRAP_CACHE[cache_key]

    _export_env(ctx)
    _ensure_directories(ctx)
    record_system_event(
        ctx.install_root,
        "bootstrap_started",
        payload={
            "mode": ctx.mode,
            "layout": ctx.layout,
            "startup_id": ctx.startup_id,
            "frozen": ctx.frozen,
        },
    )

    cleanup_summary = _run_cleanup(ctx)
    recovery_summary = _recover_update_state(ctx)
    validation_summary = _validate_state(ctx)

    preflight_summary: dict[str, Any] = {}
    try:
        from runtime.runtime_parity import bootstrap_runtime_env, run_runtime_preflight

        bootstrap_runtime_env(ctx.mode, app_root_hint=ctx.install_root, force=True)
        preflight_summary = run_runtime_preflight(ctx.mode, strict=False, sync_connected=True)
    except Exception as exc:
        preflight_summary = {
            "critical_count": 1,
            "warning_count": 0,
            "issues": [
                {
                    "level": "critical",
                    "code": "bootstrap_preflight_failed",
                    "message": str(exc),
                }
            ],
        }
        record_critical_error(
            ctx.install_root,
            "bootstrap_preflight_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )

    warnings = emit_disk_warnings(ctx.install_root)
    diagnostic = {
        "timestamp": time.time(),
        "startup_id": ctx.startup_id,
        "mode": ctx.mode,
        "layout": ctx.layout,
        "install_root": str(ctx.install_root),
        "app_root": str(ctx.app_root),
        "data_root": str(ctx.data_root),
        "runtime_root": str(ctx.runtime_root),
        "logs_root": str(ctx.logs_root),
        "updates_root": str(ctx.updates_root),
        "cleanup": cleanup_summary,
        "recovery": recovery_summary,
        "validation": validation_summary,
        "preflight": preflight_summary,
        "disk_warnings": warnings,
    }
    write_startup_diagnostic(ctx.install_root, diagnostic)
    build_support_diagnostic_bundle(ctx.install_root, extra=diagnostic)
    update_local_heartbeat(
        ctx.install_root,
        component="bootstrap",
        state="degraded" if recovery_summary.get("safe_mode_candidate") else "ok",
        payload={
            "startup_id": ctx.startup_id,
            "preflight_critical_count": int(preflight_summary.get("critical_count", 0)),
        },
    )
    record_system_event(
        ctx.install_root,
        "bootstrap_completed",
        payload={
            "startup_id": ctx.startup_id,
            "cleanup_removed": len(cleanup_summary.get("runtime_removed") or [])
            + len(cleanup_summary.get("update_removed") or []),
            "recovery_issues": len(recovery_summary.get("issues") or []),
            "preflight_critical_count": int(preflight_summary.get("critical_count", 0)),
        },
    )

    bootstrapped = replace(
        ctx,
        cleanup_summary=cleanup_summary,
        recovery_summary=recovery_summary,
        validation_summary=validation_summary,
        preflight_summary=preflight_summary,
    )
    _BOOTSTRAP_CACHE[cache_key] = bootstrapped
    return bootstrapped


def ensure_bootstrapped(
    mode: str | None,
    *,
    app_root_hint: Path | None = None,
    install_root_hint: Path | None = None,
) -> BootstrapContext:
    normalized_mode = "client" if str(mode or "").strip().lower() == "client" else "owner"
    install = _install_root(normalized_mode, install_root_hint=install_root_hint).resolve()
    cache_key = f"{normalized_mode}|{install}"
    if cache_key in _BOOTSTRAP_CACHE and os.environ.get("INSTACRM_BOOTSTRAPPED") == "1":
        return _BOOTSTRAP_CACHE[cache_key]
    return bootstrap_application(
        normalized_mode,
        app_root_hint=app_root_hint,
        install_root_hint=install_root_hint,
        force=False,
    )
