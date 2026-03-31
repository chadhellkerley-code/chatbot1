from __future__ import annotations

import os
import platform
import socket
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.storage_atomic import atomic_append_jsonl, atomic_write_json, load_json_file
from paths import app_root, logs_root, storage_root

_EVENT_LOG_NAME = "system_events.jsonl"
_HEARTBEAT_NAME = "heartbeat_state.json"
_STARTUP_DIAG_NAME = "startup_diagnostic.json"
_SUPPORT_BUNDLE_NAME = "support_diagnostic_bundle.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _base_root(default_root: Path) -> Path:
    return Path(default_root).resolve()


def event_log_path(default_root: Path) -> Path:
    return logs_root(_base_root(default_root)) / _EVENT_LOG_NAME


def heartbeat_path(default_root: Path) -> Path:
    return storage_root(_base_root(default_root)) / _HEARTBEAT_NAME


def startup_diagnostic_path(default_root: Path) -> Path:
    return logs_root(_base_root(default_root)) / _STARTUP_DIAG_NAME


def support_bundle_path(default_root: Path) -> Path:
    return logs_root(_base_root(default_root)) / _SUPPORT_BUNDLE_NAME


def app_version_path(default_root: Path) -> Path:
    return app_root(_base_root(default_root)) / "app_version.json"


def load_app_version(default_root: Path) -> dict[str, Any]:
    base = _base_root(default_root)
    candidates = [
        app_version_path(base),
        app_root(base) / "update_manifest.json",
        storage_root(base) / "update_manifest.json",
        app_root(base) / "VERSION",
        base / "VERSION",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        if candidate.suffix.lower() == ".json":
            payload = load_json_file(candidate, {}, label=f"bootstrap.version:{candidate.name}")
            if isinstance(payload, dict) and payload:
                return dict(payload)
            continue
        try:
            value = candidate.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if value:
            return {"version": value, "source": str(candidate)}
    return {"version": "unknown"}


def _event_base(default_root: Path) -> dict[str, Any]:
    version = load_app_version(default_root)
    return {
        "timestamp": _utc_now(),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "version": str(version.get("version") or "unknown"),
    }


def record_system_event(
    default_root: Path,
    event_type: str,
    *,
    level: str = "info",
    payload: dict[str, Any] | None = None,
) -> Path:
    entry = _event_base(default_root)
    entry.update(
        {
            "event_type": str(event_type or "").strip() or "system_event",
            "level": str(level or "info").strip().lower(),
            "payload": dict(payload or {}),
        }
    )
    return atomic_append_jsonl(event_log_path(default_root), entry)


def record_critical_error(
    default_root: Path,
    event_type: str,
    *,
    error: BaseException | None = None,
    message: str = "",
    payload: dict[str, Any] | None = None,
) -> Path:
    merged = dict(payload or {})
    if message:
        merged["message"] = str(message)
    if error is not None:
        merged["error_type"] = error.__class__.__name__
        merged["error_message"] = str(error)
        merged["stack"] = traceback.format_exc()
    return record_system_event(default_root, event_type, level="error", payload=merged)


def update_local_heartbeat(
    default_root: Path,
    *,
    component: str,
    state: str = "ok",
    payload: dict[str, Any] | None = None,
) -> Path:
    entry = {
        "timestamp": _utc_now(),
        "component": str(component or "").strip() or "unknown",
        "state": str(state or "ok").strip().lower(),
        "payload": dict(payload or {}),
    }
    return atomic_write_json(heartbeat_path(default_root), entry)


def write_startup_diagnostic(default_root: Path, payload: dict[str, Any]) -> Path:
    return atomic_write_json(startup_diagnostic_path(default_root), dict(payload or {}))


def build_support_diagnostic_bundle(
    default_root: Path,
    *,
    extra: dict[str, Any] | None = None,
) -> Path:
    base = _base_root(default_root)
    bundle = {
        "generated_at": _utc_now(),
        "install_root": str(base),
        "app_root": str(app_root(base)),
        "data_root": str(storage_root(base)),
        "logs_root": str(logs_root(base)),
        "event_log": str(event_log_path(base)),
        "heartbeat": str(heartbeat_path(base)),
        "version": load_app_version(base),
    }
    if extra:
        bundle["extra"] = dict(extra)
    return atomic_write_json(support_bundle_path(base), bundle)
