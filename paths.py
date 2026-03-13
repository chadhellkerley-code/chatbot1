# paths.py
# -*- coding: utf-8 -*-
"""Utilities to resolve runtime-dependent storage directories."""

from __future__ import annotations

import os
from pathlib import Path


def _env_path(name: str) -> Path | None:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        path = Path(raw).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        return None


def runtime_base(default: Path) -> Path:
    """Return the directory that should be used for writable assets.

    Client builds run from temporary locations, so we allow overriding the
    default module directory via the ``APP_DATA_ROOT`` environment variable.
    When the override is present we ensure the directory exists and fall back
    to ``default`` if anything fails.
    """

    for env_name in ("APP_DATA_ROOT", "INSTACRM_INSTALL_ROOT"):
        path = _env_path(env_name)
        if path is not None:
            return path
    return default


def app_root(default: Path) -> Path:
    path = _env_path("INSTACRM_APP_ROOT")
    if path is not None:
        return path
    return default


def data_root(default: Path) -> Path:
    path = _env_path("INSTACRM_DATA_ROOT")
    if path is not None:
        return path
    return runtime_base(default) / "storage"


def storage_root(default: Path) -> Path:
    path = data_root(default)
    path.mkdir(parents=True, exist_ok=True)
    return path


def runtime_root(default: Path) -> Path:
    path = runtime_base(default) / "runtime"
    path.mkdir(parents=True, exist_ok=True)
    return path


def accounts_root(default: Path) -> Path:
    path = storage_root(default) / "accounts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def leads_root(default: Path) -> Path:
    path = storage_root(default) / "leads"
    path.mkdir(parents=True, exist_ok=True)
    return path


def campaigns_root(default: Path) -> Path:
    path = storage_root(default) / "campaigns"
    path.mkdir(parents=True, exist_ok=True)
    return path


def analytics_root(default: Path) -> Path:
    path = storage_root(default) / "analytics"
    path.mkdir(parents=True, exist_ok=True)
    return path


def stations_root(default: Path) -> Path:
    path = storage_root(default) / "stations"
    path.mkdir(parents=True, exist_ok=True)
    return path


def exports_root(default: Path) -> Path:
    path = storage_root(default) / "exports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def browser_profiles_root(default: Path) -> Path:
    path = runtime_root(default) / "browser_profiles"
    path.mkdir(parents=True, exist_ok=True)
    return path


def browser_binaries_root(default: Path) -> Path:
    path = runtime_root(default) / "browsers"
    path.mkdir(parents=True, exist_ok=True)
    return path


def playwright_browsers_root(default: Path) -> Path:
    path = runtime_root(default) / "playwright"
    path.mkdir(parents=True, exist_ok=True)
    return path


def sessions_root(default: Path) -> Path:
    path = runtime_root(default) / "sessions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def logs_root(default: Path) -> Path:
    path = _env_path("INSTACRM_LOGS_ROOT")
    if path is None:
        path = runtime_root(default) / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def updates_root(default: Path) -> Path:
    path = _env_path("INSTACRM_UPDATES_ROOT")
    if path is None:
        path = runtime_base(default) / "updates"
    path.mkdir(parents=True, exist_ok=True)
    return path


def screenshots_root(default: Path) -> Path:
    path = runtime_root(default) / "screenshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def traces_root(default: Path) -> Path:
    path = runtime_root(default) / "traces"
    path.mkdir(parents=True, exist_ok=True)
    return path


def artifacts_root(default: Path) -> Path:
    path = runtime_root(default) / "artifacts"
    path.mkdir(parents=True, exist_ok=True)
    return path
