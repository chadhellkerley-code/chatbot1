from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any
from paths import runtime_root


_CLIENT_ID_ENV = "INSTACRM_CLIENT_ID"
_ISOLATION_ENV = "INSTACRM_ENABLE_CLIENT_ISOLATION"
_LICENSE_FILE_ENV_NAMES = ("LICENSE_FILE", "INSTACRM_LICENSE_FILE")
_LICENSE_FILE_NAMES = ("license.key", "license.json", "license_payload.json")
_RUNTIME_CACHE_NAME = "license.json"


def client_isolation_enabled() -> bool:
    value = str(os.environ.get(_ISOLATION_ENV) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def set_client_isolation_enabled(enabled: bool) -> None:
    if enabled:
        os.environ[_ISOLATION_ENV] = "1"
        return
    os.environ.pop(_ISOLATION_ENV, None)


def client_id_from_license_key(license_key: str) -> str:
    clean = str(license_key or "").strip()
    if not clean:
        return ""
    return hashlib.sha1(clean.encode("utf-8")).hexdigest()


def _candidate_roots() -> list[Path]:
    roots: list[Path] = []
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            roots.append(Path(meipass).resolve())
        except Exception:
            pass
    executable = str(getattr(sys, "executable", "") or "").strip()
    if executable:
        try:
            roots.append(Path(executable).resolve().parent)
        except Exception:
            pass
    for env_name in (
        "INSTACRM_INSTALL_ROOT",
        "INSTACRM_APP_ROOT",
        "INSTACRM_DATA_ROOT",
        "APP_DATA_ROOT",
    ):
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            roots.append(Path(raw).expanduser())
    roots.append(Path(__file__).resolve().parent)
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _license_file_candidates() -> list[Path]:
    candidates: list[Path] = []
    for env_name in _LICENSE_FILE_ENV_NAMES:
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            candidates.append(Path(raw).expanduser())
    for root in _candidate_roots():
        for filename in _LICENSE_FILE_NAMES:
            candidates.append(root / filename)
        candidates.append(runtime_root(root, scoped=False) / _RUNTIME_CACHE_NAME)
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _extract_license_key(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("license_key", "key"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    nested = payload.get("license")
    if isinstance(nested, dict):
        return _extract_license_key(nested)
    return ""


def _parse_license_key_blob(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            return _extract_license_key(json.loads(text))
        except Exception:
            return ""
    for line in text.splitlines():
        license_key = str(line or "").strip()
        if license_key:
            return license_key
    return ""


def resolve_license_key() -> str:
    for candidate in _license_file_candidates():
        if not candidate.is_file():
            continue
        try:
            raw = candidate.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        license_key = _parse_license_key_blob(raw)
        if license_key:
            return license_key
    return ""


def resolve_client_id(*, license_key: str = "", require_isolation: bool = True) -> str:
    env_client_id = str(os.environ.get(_CLIENT_ID_ENV) or "").strip().lower()
    if env_client_id:
        return env_client_id
    if require_isolation and not client_isolation_enabled():
        return ""
    return client_id_from_license_key(license_key or resolve_license_key())


def apply_client_identity_env(license_key: str = "") -> str:
    client_id = resolve_client_id(license_key=license_key, require_isolation=False)
    if client_id:
        os.environ[_CLIENT_ID_ENV] = client_id
    return client_id


def clear_client_identity_env() -> None:
    os.environ.pop(_CLIENT_ID_ENV, None)
