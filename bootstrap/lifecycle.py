from __future__ import annotations

import os
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from core.disk_monitor import emit_disk_warnings
from core.log_rotation import run_retention_maintenance
from license_identity import apply_client_identity_env, set_client_isolation_enabled
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
_POST_SHOW_HOUSEKEEPING_STATUS: dict[str, str] = {}
_POST_SHOW_HOUSEKEEPING_LOCK = threading.RLock()
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
        runtime_dir = runtime_root(install).resolve()
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


def _cache_key_for_context(ctx: BootstrapContext) -> str:
    return f"{ctx.mode}|{ctx.install_root}"


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


def _run_boot_state_sweep(ctx: BootstrapContext) -> dict[str, Any]:
    database_path = ctx.data_root / "inbox_rm.sqlite3"
    if not database_path.exists():
        return {
            "ran": False,
            "reason": "missing_database",
            "runtime_alias_state": {"checked": 0, "cleaned": 0, "deleted": 0, "details": []},
            "session_connector_state": {"checked": 0, "cleaned": 0, "deleted": 0, "details": []},
        }
    from core import accounts as accounts_module
    from src.inbox.inbox_storage import InboxStorage
    from src.runtime.alias_runtime_scheduler import AliasRuntimeScheduler
    from src.runtime.session_connector_registry import SessionConnectorRegistry

    def _account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()

    account_rows = [dict(row) for row in accounts_module.list_all() if isinstance(row, dict)]
    accounts_by_id: dict[str, dict[str, Any]] = {}
    existing_aliases: set[str] = set()
    active_alias_accounts: dict[str, set[str]] = {}
    for account in account_rows:
        account_id = _account_id(account.get("username"))
        alias_id = str(account.get("alias") or "").strip().lower()
        if alias_id:
            existing_aliases.add(alias_id)
        if not account_id:
            continue
        accounts_by_id[account_id] = dict(account)
        if alias_id and bool(account.get("active", True)):
            active_alias_accounts.setdefault(alias_id, set()).add(account_id)

    storage = InboxStorage(ctx.install_root)
    try:
        runtime_summary = AliasRuntimeScheduler.sweep_boot_persisted_states(
            store=storage,
            existing_aliases=existing_aliases,
            active_alias_accounts=active_alias_accounts,
        )
        connector_summary = SessionConnectorRegistry.sweep_boot_persisted_states(
            store=storage,
            accounts_by_id=accounts_by_id,
        )
    finally:
        storage.shutdown()
    return {
        "ran": True,
        "database_path": str(database_path),
        "runtime_alias_state": runtime_summary,
        "session_connector_state": connector_summary,
    }


def _build_startup_diagnostic(
    ctx: BootstrapContext,
    *,
    cleanup_summary: dict[str, Any],
    recovery_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    preflight_summary: dict[str, Any],
    disk_warnings: list[str],
    housekeeping_deferred: bool,
    housekeeping_completed: bool,
    time_to_window_show_seconds: float | None = None,
) -> dict[str, Any]:
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
        "disk_warnings": list(disk_warnings or []),
        "housekeeping_deferred": bool(housekeeping_deferred),
        "housekeeping_completed": bool(housekeeping_completed),
    }
    if time_to_window_show_seconds is not None:
        diagnostic["time_to_window_show_seconds"] = float(time_to_window_show_seconds)
    return diagnostic


def _finalize_bootstrap_observability(
    ctx: BootstrapContext,
    *,
    cleanup_summary: dict[str, Any],
    recovery_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    preflight_summary: dict[str, Any],
    disk_warnings: list[str],
    housekeeping_deferred: bool,
    housekeeping_completed: bool,
    time_to_window_show_seconds: float | None = None,
) -> dict[str, Any]:
    diagnostic = _build_startup_diagnostic(
        ctx,
        cleanup_summary=cleanup_summary,
        recovery_summary=recovery_summary,
        validation_summary=validation_summary,
        preflight_summary=preflight_summary,
        disk_warnings=disk_warnings,
        housekeeping_deferred=housekeeping_deferred,
        housekeeping_completed=housekeeping_completed,
        time_to_window_show_seconds=time_to_window_show_seconds,
    )
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
            "housekeeping_deferred": bool(housekeeping_deferred),
            "housekeeping_completed": bool(housekeeping_completed),
        },
    )
    return diagnostic


def _run_post_show_bootstrap_tasks_impl(
    ctx: BootstrapContext,
    *,
    time_to_window_show_seconds: float | None = None,
) -> dict[str, Any]:
    cleanup_summary = ctx.cleanup_summary
    preflight_summary = ctx.preflight_summary
    housekeeping_was_deferred = bool(cleanup_summary.get("deferred"))
    cleanup_summary["was_deferred"] = housekeeping_was_deferred
    cleanup_summary.setdefault(
        "boot_state_sweep",
        {"ran": False, "status": "pending"},
    )
    cleanup_summary["completed"] = False
    cleanup_summary["started_at"] = time.time()

    try:
        cleanup_result = _run_cleanup(ctx)
    except Exception as exc:
        cleanup_result = {
            "retention": {},
            "runtime_removed": [],
            "update_removed": [],
            "error": str(exc),
        }
        record_critical_error(
            ctx.install_root,
            "bootstrap_cleanup_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )
    cleanup_summary.update(cleanup_result)

    disk_warnings: list[str] = []
    try:
        disk_warnings = emit_disk_warnings(ctx.install_root)
    except Exception as exc:
        cleanup_summary["disk_warnings_error"] = str(exc)
        record_critical_error(
            ctx.install_root,
            "bootstrap_disk_warning_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )

    try:
        cleanup_summary["boot_state_sweep"] = _run_boot_state_sweep(ctx)
        cleanup_summary["boot_state_sweep"]["status"] = "completed"
    except Exception as exc:
        cleanup_summary["boot_state_sweep"] = {
            "ran": False,
            "status": "failed",
            "error": str(exc),
        }
        record_critical_error(
            ctx.install_root,
            "bootstrap_state_sweep_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )

    try:
        from runtime.runtime_parity import run_runtime_preflight

        full_preflight_summary = run_runtime_preflight(
            ctx.mode,
            strict=False,
            sync_connected=True,
        )
    except Exception as exc:
        full_preflight_summary = {
            "phase": "post_show_full",
            "critical_count": 1,
            "warning_count": 0,
            "issues": [
                {
                    "level": "critical",
                    "code": "bootstrap_preflight_failed",
                    "message": str(exc),
                }
            ],
            "report_path": "",
        }
        record_critical_error(
            ctx.install_root,
            "bootstrap_preflight_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )
    preflight_summary.clear()
    preflight_summary.update(full_preflight_summary)

    cleanup_summary["completed"] = True
    cleanup_summary["completed_at"] = time.time()
    cleanup_summary["disk_warnings"] = list(disk_warnings or [])
    cleanup_summary["deferred"] = False

    diagnostic = _finalize_bootstrap_observability(
        ctx,
        cleanup_summary=cleanup_summary,
        recovery_summary=ctx.recovery_summary,
        validation_summary=ctx.validation_summary,
        preflight_summary=ctx.preflight_summary,
        disk_warnings=disk_warnings,
        housekeeping_deferred=housekeeping_was_deferred,
        housekeeping_completed=True,
        time_to_window_show_seconds=time_to_window_show_seconds,
    )
    return {
        "cleanup": cleanup_summary,
        "disk_warnings": disk_warnings,
        "diagnostic": diagnostic,
    }


def run_post_show_bootstrap_tasks(
    ctx: BootstrapContext,
    *,
    time_to_window_show_seconds: float | None = None,
) -> dict[str, Any]:
    cache_key = _cache_key_for_context(ctx)
    with _POST_SHOW_HOUSEKEEPING_LOCK:
        status = _POST_SHOW_HOUSEKEEPING_STATUS.get(cache_key)
        if status == "completed":
            return {
                "cleanup": ctx.cleanup_summary,
                "disk_warnings": list(ctx.cleanup_summary.get("disk_warnings") or []),
                "status": status,
            }
        if status == "running":
            return {
                "cleanup": ctx.cleanup_summary,
                "disk_warnings": [],
                "status": status,
            }
        _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "running"

    try:
        result = _run_post_show_bootstrap_tasks_impl(
            ctx,
            time_to_window_show_seconds=time_to_window_show_seconds,
        )
    finally:
        with _POST_SHOW_HOUSEKEEPING_LOCK:
            _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "completed"
    result["status"] = "completed"
    return result


def start_post_show_bootstrap_tasks(
    ctx: BootstrapContext,
    *,
    time_to_window_show_seconds: float | None = None,
) -> bool:
    cache_key = _cache_key_for_context(ctx)
    with _POST_SHOW_HOUSEKEEPING_LOCK:
        status = _POST_SHOW_HOUSEKEEPING_STATUS.get(cache_key)
        if status in {"running", "completed"}:
            return False
        _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "running"

    def _runner() -> None:
        try:
            _run_post_show_bootstrap_tasks_impl(
                ctx,
                time_to_window_show_seconds=time_to_window_show_seconds,
            )
        finally:
            with _POST_SHOW_HOUSEKEEPING_LOCK:
                _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "completed"

    thread = threading.Thread(
        target=_runner,
        name=f"bootstrap-housekeeping-{ctx.startup_id}",
        daemon=True,
    )
    thread.start()
    return True


def run_post_show_housekeeping(
    ctx: BootstrapContext,
    *,
    time_to_window_show_seconds: float | None = None,
) -> dict[str, Any]:
    return run_post_show_bootstrap_tasks(
        ctx,
        time_to_window_show_seconds=time_to_window_show_seconds,
    )


def start_post_show_housekeeping(
    ctx: BootstrapContext,
    *,
    time_to_window_show_seconds: float | None = None,
) -> bool:
    return start_post_show_bootstrap_tasks(
        ctx,
        time_to_window_show_seconds=time_to_window_show_seconds,
    )


def bootstrap_application(
    mode: str | None,
    *,
    app_root_hint: Path | None = None,
    install_root_hint: Path | None = None,
    force: bool = False,
    defer_housekeeping: bool = False,
) -> BootstrapContext:
    normalized_mode = "client" if str(mode or "").strip().lower() == "client" else "owner"
    set_client_isolation_enabled(normalized_mode == "client")
    if normalized_mode == "client":
        apply_client_identity_env()
    ctx = _new_context(str(mode or ""), app_root_hint=app_root_hint, install_root_hint=install_root_hint)
    cache_key = _cache_key_for_context(ctx)
    if not force and cache_key in _BOOTSTRAP_CACHE:
        cached = _BOOTSTRAP_CACHE[cache_key]
        if not defer_housekeeping:
            run_post_show_bootstrap_tasks(cached)
        return cached

    with _POST_SHOW_HOUSEKEEPING_LOCK:
        _POST_SHOW_HOUSEKEEPING_STATUS.pop(cache_key, None)

    _export_env(ctx)
    _ensure_directories(ctx)
    try:
        from core.totp_store import migrate_legacy_store

        migrate_legacy_store()
    except Exception:
        pass
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

    cleanup_summary: dict[str, Any] = {
        "deferred": bool(defer_housekeeping),
        "completed": False,
        "boot_state_sweep": {
            "ran": False,
            "status": "pending" if defer_housekeeping else "scheduled_post_bootstrap",
        },
    }
    recovery_summary = _recover_update_state(ctx)
    validation_summary = _validate_state(ctx)

    preflight_summary: dict[str, Any] = {}
    try:
        from runtime.runtime_parity import bootstrap_runtime_env, run_runtime_preflight_minimal

        bootstrap_runtime_env(ctx.mode, app_root_hint=ctx.install_root, force=True)
        preflight_summary = run_runtime_preflight_minimal(ctx.mode, strict=False)
    except Exception as exc:
        preflight_summary = {
            "phase": "pre_show_minimal",
            "critical_count": 1,
            "warning_count": 0,
            "issues": [
                {
                    "level": "critical",
                    "code": "bootstrap_preflight_failed",
                    "message": str(exc),
                }
            ],
            "report_path": "",
        }
        record_critical_error(
            ctx.install_root,
            "bootstrap_preflight_failed",
            error=exc,
            payload={"mode": ctx.mode, "startup_id": ctx.startup_id},
        )

    bootstrapped = replace(
        ctx,
        cleanup_summary=cleanup_summary,
        recovery_summary=recovery_summary,
        validation_summary=validation_summary,
        preflight_summary=preflight_summary,
    )
    if defer_housekeeping:
        with _POST_SHOW_HOUSEKEEPING_LOCK:
            _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "pending"
    else:
        _run_post_show_bootstrap_tasks_impl(bootstrapped)
        with _POST_SHOW_HOUSEKEEPING_LOCK:
            _POST_SHOW_HOUSEKEEPING_STATUS[cache_key] = "completed"
    _BOOTSTRAP_CACHE[cache_key] = bootstrapped
    return bootstrapped


def ensure_bootstrapped(
    mode: str | None,
    *,
    app_root_hint: Path | None = None,
    install_root_hint: Path | None = None,
    defer_housekeeping: bool = False,
) -> BootstrapContext:
    normalized_mode = "client" if str(mode or "").strip().lower() == "client" else "owner"
    install = _install_root(normalized_mode, install_root_hint=install_root_hint).resolve()
    cache_key = f"{normalized_mode}|{install}"
    if cache_key in _BOOTSTRAP_CACHE and os.environ.get("INSTACRM_BOOTSTRAPPED") == "1":
        if not defer_housekeeping:
            run_post_show_bootstrap_tasks(_BOOTSTRAP_CACHE[cache_key])
        return _BOOTSTRAP_CACHE[cache_key]
    return bootstrap_application(
        normalized_mode,
        app_root_hint=app_root_hint,
        install_root_hint=install_root_hint,
        force=False,
        defer_housekeeping=defer_housekeeping,
    )
