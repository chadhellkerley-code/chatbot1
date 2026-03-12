from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any

from paths import browser_profiles_root, logs_root, storage_root

logger = logging.getLogger(__name__)

_WARN_LOCK = threading.RLock()
_LAST_WARNINGS: dict[str, float] = {}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    try:
        for item in path.rglob("*"):
            if item.is_file():
                total += int(item.stat().st_size)
    except Exception:
        logger.exception("No se pudo calcular tamaño de directorio: %s", path)
    return total


def snapshot_disk_usage(root: str | Path) -> dict[str, Any]:
    base = Path(root)
    storage_dir = storage_root(base)
    logs_dir = logs_root(base)
    profiles_dir = browser_profiles_root(base)
    disk_root = storage_dir if storage_dir.exists() else base
    usage = shutil.disk_usage(disk_root)
    return {
        "disk_root": str(disk_root),
        "free_bytes": int(usage.free),
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "logs_bytes": _directory_size_bytes(logs_dir) + _directory_size_bytes(storage_dir / "debug_screenshots"),
        "profiles_bytes": _directory_size_bytes(profiles_dir),
    }


def emit_disk_warnings(root: str | Path, *, throttle_seconds: float = 300.0) -> list[str]:
    snapshot = snapshot_disk_usage(root)
    free_gb = float(snapshot["free_bytes"]) / (1024 ** 3)
    logs_gb = float(snapshot["logs_bytes"]) / (1024 ** 3)
    profiles_gb = float(snapshot["profiles_bytes"]) / (1024 ** 3)

    min_free_gb = max(0.25, _env_float("DISK_MONITOR_MIN_FREE_GB", 2.0))
    max_logs_gb = max(0.1, _env_float("DISK_MONITOR_MAX_LOGS_GB", 2.0))
    max_profiles_gb = max(0.25, _env_float("DISK_MONITOR_MAX_PROFILES_GB", 5.0))

    warnings: list[str] = []
    if free_gb <= min_free_gb:
        warnings.append(f"Low disk space: {free_gb:.2f} GB free")
    if logs_gb >= max_logs_gb:
        warnings.append(f"Log storage is large: {logs_gb:.2f} GB")
    if profiles_gb >= max_profiles_gb:
        warnings.append(f"Profiles storage is large: {profiles_gb:.2f} GB")

    if not warnings:
        return []

    now = time.time()
    emitted: list[str] = []
    with _WARN_LOCK:
        for message in warnings:
            last_seen = _LAST_WARNINGS.get(message, 0.0)
            if now - last_seen < throttle_seconds:
                continue
            _LAST_WARNINGS[message] = now
            emitted.append(message)

    for message in emitted:
        logger.warning("DISK_PRESSURE %s", message)
    return emitted
