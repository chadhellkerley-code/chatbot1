# storage_migration.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Iterable

from paths import sessions_root

logger = logging.getLogger(__name__)

# Old dev layout (relative to where the previous app was executed from).
OLD_STORAGE_PATH = Path("./data")

# New client runtime layout (default, unless INSTACRM_DATA_ROOT is set).
NEW_STORAGE_PATH = Path.home() / "InstaCRM" / "data"


def _is_client_mode() -> bool:
    mode = (os.environ.get("INSTACRM_APP_MODE") or "").strip().lower()
    if mode:
        return mode == "client"
    argv0 = Path(sys.argv[0]).stem.lower() if sys.argv else ""
    return argv0 == "client_launcher"


def _resolve_new_root() -> Path:
    raw = (os.environ.get("INSTACRM_DATA_ROOT") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return NEW_STORAGE_PATH


def _iter_candidate_old_roots() -> Iterable[Path]:
    yield OLD_STORAGE_PATH
    with _suppress_exceptions():
        yield Path.cwd() / "data"
    raw_install = (os.environ.get("INSTACRM_INSTALL_ROOT") or "").strip()
    if raw_install:
        yield Path(raw_install).expanduser() / "data"
    if getattr(sys, "frozen", False):
        with _suppress_exceptions():
            yield Path(sys.executable).resolve().parent / "data"


def _json_has_real_data(path: Path) -> bool:
    """Return True when a JSON file exists and contains non-empty data.

    This intentionally treats empty placeholders like ``[]`` / ``{}`` as "no data"
    so storage migration can still run when the build created empty files.
    """

    try:
        if not path.is_file():
            return False
        try:
            if path.stat().st_size <= 0:
                return False
        except Exception:
            return False

        raw = path.read_text(encoding="utf-8", errors="ignore").strip()
        if raw in ("", "[]", "{}", "null"):
            return False

        try:
            payload = json.loads(raw)
        except Exception:
            # Non-empty but invalid JSON: consider it "real" to avoid repeated
            # migrations and potential user confusion.
            return True

        if isinstance(payload, list):
            return len(payload) > 0

        if isinstance(payload, dict):
            # aliases.json is commonly a dict wrapper: {"schema_version": X, "aliases": []}
            if path.name == "aliases.json":
                aliases = payload.get("aliases")
                if isinstance(aliases, list):
                    return len(aliases) > 0

            # Generic heuristic: any non-empty nested structure or meaningful scalar.
            for key, value in payload.items():
                if key in {"schema_version", "version"}:
                    continue
                if isinstance(value, (list, dict)) and len(value) > 0:
                    return True
                if isinstance(value, str) and value.strip():
                    return True
                if isinstance(value, (int, float)) and value != 0:
                    return True
                if isinstance(value, bool) and value:
                    return True
            return False

        # Primitive JSON values (str/number/bool) that aren't blank/null count as data.
        return True
    except Exception:
        return False


def _sqlite_has_real_data(path: Path) -> bool:
    """Return True when a SQLite-like DB file exists and isn't an empty placeholder."""

    try:
        if not path.is_file():
            return False
        try:
            size = int(path.stat().st_size)
        except Exception:
            return False
        if size <= 0:
            return False
        # Basic SQLite header check.
        with path.open("rb") as f:
            header = f.read(16)
        if header != b"SQLite format 3\x00":
            # Unknown DB format; treat as real to avoid repeatedly migrating.
            return True
        # A valid SQLite database is at least 100 bytes (database header size).
        return size >= 100
    except Exception:
        return False


def has_real_data(root: Path) -> bool:
    """True if a storage root contains real account-related data.

    Do NOT use only `root.exists()` because builds may create the directory (or empty
    placeholder files) before the first run.
    """

    # Prefer checking the same filenames we migrate.
    candidates = (
        root / "accounts.json",
        root / "accounts.db",
        root / "aliases.json",
        root / "proxies.json",
        root / "accounts" / "accounts.json",
        root / "accounts" / "accounts.db",
        root / "accounts" / "aliases.json",
        root / "accounts" / "proxies.json",
    )

    for path in candidates:
        if path.suffix == ".json":
            if _json_has_real_data(path):
                return True
        elif path.suffix == ".db":
            if _sqlite_has_real_data(path):
                return True
        else:
            try:
                if path.is_file() and path.stat().st_size > 0:
                    return True
            except Exception:
                continue
    return False


def _copy_file_no_overwrite(src: Path, dst: Path) -> None:
    if not src.is_file():
        return
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree_no_overwrite(src_dir: Path, dst_dir: Path) -> None:
    if not src_dir.is_dir():
        return
    for src in src_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(src_dir)
        dst = dst_dir / rel
        _copy_file_no_overwrite(src, dst)


class _suppress_exceptions:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return True


def migrate_old_storage_if_needed() -> bool:
    """Migrate accounts/proxies/aliases from old dev storage into new runtime storage.

    Rules:
    - If NEW storage already has account data, do nothing.
    - Never overwrite existing files.
    - Ensure NEW storage folder exists.
    """

    if not _is_client_mode():
        return False

    new_root = _resolve_new_root()
    try:
        new_root.mkdir(parents=True, exist_ok=True)
    except Exception:
        return False

    # Only skip migration when NEW storage contains real data (not just an existing folder).
    if has_real_data(new_root):
        return False

    selected_old: Path | None = None
    for candidate in _iter_candidate_old_roots():
        try:
            if candidate.resolve() == new_root.resolve():
                continue
        except Exception:
            pass
        if has_real_data(candidate):
            selected_old = candidate
            break

    if selected_old is None:
        return False

    # Allowlist: keep this tight to avoid moving unrelated subsystems.
    src_accounts_dir = selected_old / "accounts"
    dst_accounts_dir = new_root / "accounts"
    _copy_tree_no_overwrite(src_accounts_dir, dst_accounts_dir)

    for filename in (
        "accounts.json",
        "accounts.db",
        "aliases.json",
        "proxies.json",
        "accounts_status.json",
        "app_state.db",
        "proxy_registry.sqlite3",
        "proxy_registry.sqlite3-wal",
        "proxy_registry.sqlite3-shm",
    ):
        _copy_file_no_overwrite(selected_old / filename, new_root / filename)

    # Legacy session folder (if present in old storage).
    _copy_tree_no_overwrite(
        sessions_root(selected_old, scoped=False, honor_env=False),
        sessions_root(new_root, scoped=False, honor_env=False),
    )

    logger.info("Storage migrated from old path to new path")
    logger.info("Storage migration details: old=%s new=%s", str(selected_old), str(new_root))
    return True
