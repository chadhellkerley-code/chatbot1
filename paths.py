# paths.py
# -*- coding: utf-8 -*-
"""Utilities to resolve runtime-dependent storage directories."""

from __future__ import annotations

import os
import json
import shutil
from pathlib import Path


_CLIENT_ISOLATION_STATE = ".client_isolation_state.json"


def _env_path(name: str, *, create: bool = True) -> Path | None:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        path = Path(raw).expanduser()
        if create:
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


def _base_root(default: Path) -> Path:
    return runtime_base(default)


def _storage_base(default: Path) -> Path:
    path = _env_path("INSTACRM_DATA_ROOT")
    if path is not None:
        return path
    return _base_root(default) / "storage"


def _runtime_base_dir(default: Path) -> Path:
    path = _env_path("INSTACRM_RUNTIME_ROOT")
    if path is not None:
        return path
    return _base_root(default) / "runtime"


def _sessions_base_dir(default: Path) -> Path:
    return _base_root(default) / "sessions"


def _scoped_path(base: Path, client_id: str) -> Path:
    if not client_id or base.name == client_id:
        return base
    return base / client_id


def _is_client_bucket(path: Path) -> bool:
    name = path.name.strip().lower()
    return len(name) == 40 and all(ch in "0123456789abcdef" for ch in name)


def _state_path(default: Path) -> Path:
    return _base_root(default) / _CLIENT_ISOLATION_STATE


def _load_state(default: Path) -> dict[str, str]:
    path = _state_path(default)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    state: dict[str, str] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, str):
            state[key] = value
    return state


def _save_state(default: Path, state: dict[str, str]) -> None:
    path = _state_path(default)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(payload + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _legacy_claim(default: Path, scope: str) -> str:
    return _load_state(default).get(scope, "")


def _set_legacy_claim(default: Path, scope: str, client_id: str) -> None:
    if not client_id:
        return
    state = _load_state(default)
    state[scope] = client_id
    _save_state(default, state)


def _has_entries(path: Path) -> bool:
    try:
        next(path.iterdir())
    except (StopIteration, FileNotFoundError, NotADirectoryError):
        return False
    except Exception:
        return False
    return True


def _copy_child(source: Path, target: Path) -> None:
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _migrate_legacy_tree(
    default: Path,
    *,
    scope: str,
    legacy_root: Path,
    target_root: Path,
    client_id: str,
) -> None:
    if not client_id:
        return
    try:
        if legacy_root.resolve() == target_root.resolve():
            return
    except Exception:
        pass
    target_root.mkdir(parents=True, exist_ok=True)
    if _has_entries(target_root):
        return
    claimed = _legacy_claim(default, scope)
    if claimed and claimed != client_id:
        return
    try:
        children = [
            child
            for child in legacy_root.iterdir()
            if child.name != target_root.name and not _is_client_bucket(child)
        ]
    except Exception:
        return
    if not children:
        return
    copied_any = False
    for child in children:
        try:
            _copy_child(child, target_root / child.name)
            copied_any = True
        except Exception:
            continue
    if copied_any:
        try:
            _set_legacy_claim(default, scope, client_id)
        except Exception:
            pass


def _client_id() -> str:
    from license_identity import resolve_client_id

    return resolve_client_id()


def data_root(default: Path, *, scoped: bool = True, honor_env: bool = True) -> Path:
    base = _storage_base(default) if honor_env else Path(default) / "storage"
    client_id = _client_id() if scoped else ""
    path = _scoped_path(base, client_id)
    if client_id:
        _migrate_legacy_tree(
            default,
            scope="storage",
            legacy_root=base,
            target_root=path,
            client_id=client_id,
        )
    return path


def storage_root(default: Path, *, scoped: bool = True, honor_env: bool = True) -> Path:
    path = data_root(default, scoped=scoped, honor_env=honor_env)
    path.mkdir(parents=True, exist_ok=True)
    return path


def runtime_root(default: Path, *, scoped: bool = True, honor_env: bool = True) -> Path:
    base = _runtime_base_dir(default) if honor_env else Path(default) / "runtime"
    client_id = _client_id() if scoped else ""
    path = _scoped_path(base, client_id)
    if client_id:
        _migrate_legacy_tree(
            default,
            scope="runtime",
            legacy_root=base,
            target_root=path,
            client_id=client_id,
        )
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


def sessions_root(default: Path, *, scoped: bool = True, honor_env: bool = True) -> Path:
    base = _sessions_base_dir(default) if honor_env else Path(default) / "sessions"
    client_id = _client_id() if scoped else ""
    path = _scoped_path(base, client_id)
    if client_id:
        legacy_roots = [base, _runtime_base_dir(default) / "sessions"]
        for legacy_root in legacy_roots:
            _migrate_legacy_tree(
                default,
                scope="sessions",
                legacy_root=legacy_root,
                target_root=path,
                client_id=client_id,
            )
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
