from __future__ import annotations

<<<<<<< HEAD
import os
from pathlib import Path
from typing import Any

STORAGE_STATE_FILENAME = "storage_state.json"
PROFILE_LIFECYCLE_FILENAME = "profile_lifecycle.json"
PROFILE_LIFECYCLE_DIAGNOSTICS_FILENAME = "profile_lifecycle_diagnostics.jsonl"


def normalize_browser_profile_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def browser_profiles_root(profiles_root: str | Path | None = None) -> Path:
    if profiles_root is None:
        from src.playwright_service import BASE_PROFILES

        profiles_root = BASE_PROFILES
    return Path(profiles_root)
=======
from pathlib import Path
from typing import Any

from src.playwright_service import BASE_PROFILES

STORAGE_STATE_FILENAME = "storage_state.json"


def normalize_browser_profile_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def browser_profiles_root(profiles_root: str | Path | None = None) -> Path:
    return Path(BASE_PROFILES if profiles_root is None else profiles_root)
>>>>>>> origin/main


def browser_profile_dir(username: Any, *, profiles_root: str | Path | None = None) -> Path:
    return browser_profiles_root(profiles_root) / normalize_browser_profile_username(username)


<<<<<<< HEAD
def canonical_browser_profile_path(profile_dir: str | Path) -> Path:
    path = Path(profile_dir).expanduser()
    try:
        return path.resolve(strict=False)
    except TypeError:
        return path.resolve()


def browser_profile_owner_key(profile_dir: str | Path) -> str:
    return os.path.normcase(str(canonical_browser_profile_path(profile_dir)))


def browser_profile_lifecycle_path(profile_dir: str | Path) -> Path:
    return canonical_browser_profile_path(profile_dir) / PROFILE_LIFECYCLE_FILENAME


def browser_profile_lifecycle_diagnostics_path(profile_dir: str | Path) -> Path:
    return canonical_browser_profile_path(profile_dir) / PROFILE_LIFECYCLE_DIAGNOSTICS_FILENAME


=======
>>>>>>> origin/main
def browser_storage_state_path(
    username: Any,
    *,
    profiles_root: str | Path | None = None,
    filename: str = STORAGE_STATE_FILENAME,
) -> Path:
    return browser_profile_dir(username, profiles_root=profiles_root) / (filename or STORAGE_STATE_FILENAME)
