from __future__ import annotations

from pathlib import Path
from typing import Any

from src.playwright_service import BASE_PROFILES

STORAGE_STATE_FILENAME = "storage_state.json"


def normalize_browser_profile_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def browser_profiles_root(profiles_root: str | Path | None = None) -> Path:
    return Path(BASE_PROFILES if profiles_root is None else profiles_root)


def browser_profile_dir(username: Any, *, profiles_root: str | Path | None = None) -> Path:
    return browser_profiles_root(profiles_root) / normalize_browser_profile_username(username)


def browser_storage_state_path(
    username: Any,
    *,
    profiles_root: str | Path | None = None,
    filename: str = STORAGE_STATE_FILENAME,
) -> Path:
    return browser_profile_dir(username, profiles_root=profiles_root) / (filename or STORAGE_STATE_FILENAME)
