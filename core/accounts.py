# accounts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import builtins
import contextlib
import csv
import getpass
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from threading import Lock, RLock
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import logging
from urllib.parse import urlparse

from config import SETTINGS
from core.alias_identity import (
    AliasValidationError,
    DEFAULT_ALIAS_DISPLAY_NAME,
    DEFAULT_ALIAS_ID,
    alias_record_from_input,
    normalize_alias_display,
    normalize_alias_id,
)
from core.accounts_helpers.csv_utils import (
    _extract_totp_entries_from_csv,
)
from core.accounts_helpers.password_cache import (
    _clear_login_failure,
    _load_password_cache,
    _login_backoff_remaining,
    _password_key,
    _record_login_failure,
    _save_password_cache,
    configure as _configure_password_cache,
)
from core.storage_atomic import (
    atomic_write_json,
    atomic_write_text,
    load_json_file,
    load_jsonl_entries,
    path_lock,
)
from core.proxy_preflight import account_proxy_preflight
from core.proxy_registry import (
    ProxyResolutionError,
    get_proxy_by_id,
    load_proxy_audit_entries,
    proxy_audit_path,
    sync_account_proxy_links,
    upsert_proxy_record,
)
from src.auth.persistent_login import ensure_logged_in_async
from src.browser_profile_paths import normalize_browser_profile_username
from src.browser_telemetry import log_browser_stage
from src.playwright_service import BASE_PROFILES
from src.transport.session_manager import ManagedSession, SessionManager
from src.transport.session_manager_factory import get_session_manager


def _noop_log_event(*_args: Any, **_kwargs: Any) -> None:
    return


def _ask_secret(prompt: str) -> str:
    """
    Read secret values without blocking when UI mode monkeypatches input().
    """
    input_fn = getattr(builtins, "input", None)
    if getattr(input_fn, "__module__", "") == "gui.io_adapter":
        return ask(prompt)
    stdin = getattr(sys, "stdin", None)
    stdout = getattr(sys, "stdout", None)
    try:
        has_tty = bool(stdin and stdin.isatty() and stdout and stdout.isatty())
    except Exception:
        has_tty = False
    if not has_tty:
        return ask(prompt)
    try:
        return getpass.getpass(prompt)
    except (EOFError, KeyboardInterrupt):
        raise
    except Exception:
        return ask(prompt)


try:
    from src.auth.onboarding import (
        build_proxy as build_playwright_proxy,
        login_and_persist,
        onboard_accounts_from_csv,
    )
    _ONBOARDING_BACKEND_ERROR: Optional[Exception] = None
    _ONBOARDING_AVAILABLE = True
except Exception as exc:  # pragma: no cover - depende de entorno
    build_playwright_proxy = None
    login_and_persist = None
    onboard_accounts_from_csv = None
    _ONBOARDING_BACKEND_ERROR = exc
    _ONBOARDING_AVAILABLE = False
try:
    from src.auth.persistent_login import check_session as _check_playwright_session
except Exception:
    _check_playwright_session = None
try:
    from src.proxy_payload import (
        proxy_fields_from_account as _proxy_fields_from_account,
        proxy_from_account as _proxy_from_account,
    )
except Exception:
    _proxy_fields_from_account = None
    _proxy_from_account = None
from proxy_manager import (
    ProxyConfig,
    clear_proxy,
    config_from_account,
    default_proxy_settings,
    record_proxy_failure,
    should_retry_proxy,
    test_proxy_connection,
)
from core.session_store import has_session, remove as remove_session
from core.totp_store import generate_code as generate_totp_code
from core.totp_store import get_secret as get_totp_secret
from core.totp_store import has_secret as has_totp_secret
from core.totp_store import normalize_username as normalize_totp_username
from core.totp_store import remove_secret as remove_totp_secret
from core.totp_store import rename_secret as rename_totp_secret
from core.totp_store import save_secret as save_totp_secret
from utils import ask, ask_int, ok, press_enter, warn
from paths import accounts_root, runtime_base, storage_root
from src.browser_profile_paths import browser_storage_state_path
from src.playwright_service import BASE_PROFILES

BASE = runtime_base(Path(__file__).resolve().parent.parent)
BASE.mkdir(parents=True, exist_ok=True)
DATA = accounts_root(BASE)
FILE = DATA / "accounts.json"
_PASSWORD_FILE = DATA / "passwords.json"
_TRASH_ACCOUNTS_FILE = DATA / "trash_accounts.json"
_TRASH_RETENTION = timedelta(days=15)
_ACCOUNT_STORE_LOCK = RLock()
_ACCOUNT_PROXY_LINK_SYNC_LOCK = Lock()
_MANAGED_ACCOUNT_PROXY_PREFIX = "acct:"
_PROXY_ASSIGN_AUDIT_LOOKBACK = 5000

_LOGIN_FAILURE_BACKOFF = timedelta(minutes=5)
_configure_password_cache(_PASSWORD_FILE, login_failure_backoff=_LOGIN_FAILURE_BACKOFF)


_PASSWORD_CACHE: Dict[str, str] = _load_password_cache()
_TOTP_EXPORT_CACHE: Dict[str, str] = {}
_TOTP_EXPORT_CACHE_TIMESTAMP = 0.0

logger = logging.getLogger(__name__)

# Account runtime state is persisted in SQLite and updated ONLY by Playwright
# flows (no API-based verification).
import health_store


_SENT_LOG = storage_root(BASE) / "sent_log.jsonl"
_ACTIVITY_CACHE_TTL = timedelta(minutes=5)
_ACTIVITY_CACHE: Optional[Tuple[int, datetime, Dict[str, int]]] = None

ACCOUNT_USAGE_STATE_ACTIVE = "active"
ACCOUNT_USAGE_STATE_DEACTIVATED = "deactivated"
_ACCOUNT_USAGE_STATE_VALUES = {
    ACCOUNT_USAGE_STATE_ACTIVE,
    ACCOUNT_USAGE_STATE_DEACTIVATED,
}


def _configure_password_backend() -> None:
    _configure_password_cache(_PASSWORD_FILE, login_failure_backoff=_LOGIN_FAILURE_BACKOFF)


def _cached_password(username: str | None) -> str:
    key = _password_key(username)
    if not key:
        return ""
    cached = _PASSWORD_CACHE.get(key)
    if cached:
        return cached
    _configure_password_backend()
    loaded = _load_password_cache()
    if loaded:
        _PASSWORD_CACHE.update(loaded)
    return _PASSWORD_CACHE.get(key, "")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_alias(value: str | None) -> str:
    if not value:
        return "default"
    normalized = str(value).strip().lower()
    return normalized if normalized else "default"


def normalize_account_usage_state(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _ACCOUNT_USAGE_STATE_VALUES:
        return normalized
    if normalized in {"inactive", "disabled", "off", "paused"}:
        return ACCOUNT_USAGE_STATE_DEACTIVATED
    return ACCOUNT_USAGE_STATE_ACTIVE


def account_usage_state(record: Dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return ACCOUNT_USAGE_STATE_ACTIVE
    return normalize_account_usage_state(record.get("usage_state"))


def is_account_usage_active(record: Dict[str, Any] | None) -> bool:
    return account_usage_state(record) == ACCOUNT_USAGE_STATE_ACTIVE


def is_account_enabled_for_operation(record: Dict[str, Any] | None) -> bool:
    if not isinstance(record, dict):
        return False
    return bool(record.get("active", True)) and is_account_usage_active(record)


def _parse_datetime(value: object) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_trash_accounts_file() -> None:
    try:
        if _TRASH_ACCOUNTS_FILE.exists():
            return
    except Exception:
        return
    try:
        atomic_write_json(_TRASH_ACCOUNTS_FILE, [])
    except Exception:
        return


def _append_deleted_account_to_trash(username: str) -> None:
    _ensure_trash_accounts_file()
    clean_username = str(username or "").strip()
    if not clean_username:
        return
    entry = {"username": clean_username, "deleted_at": _isoformat_utc(_now_utc())}
    try:
        with path_lock(_TRASH_ACCOUNTS_FILE):
            existing = load_json_file(_TRASH_ACCOUNTS_FILE, [], label="accounts.trash")
            if not isinstance(existing, list):
                existing = []
            deduped = [
                item
                for item in existing
                if not (
                    isinstance(item, dict)
                    and str(item.get("username") or "").strip().lower() == clean_username.lower()
                )
            ]
            deduped.append(entry)
            atomic_write_json(_TRASH_ACCOUNTS_FILE, deduped)
    except Exception:
        return


def cleanup_deleted_accounts() -> None:
    try:
        if not _TRASH_ACCOUNTS_FILE.exists():
            return
    except Exception:
        return
    try:
        now = _now_utc()
        with path_lock(_TRASH_ACCOUNTS_FILE):
            payload = load_json_file(_TRASH_ACCOUNTS_FILE, [], label="accounts.trash")
            if not isinstance(payload, list):
                payload = []
            kept: list[dict[str, Any]] = []
            changed = False
            for item in payload:
                if not isinstance(item, dict):
                    changed = True
                    continue
                deleted_at = _parse_datetime(item.get("deleted_at"))
                if deleted_at is None:
                    kept.append(item)
                    continue
                if now - deleted_at > _TRASH_RETENTION:
                    changed = True
                    continue
                kept.append(item)
            if changed:
                atomic_write_json(_TRASH_ACCOUNTS_FILE, kept)
    except Exception:
        return


def _settings_value(name: str, default: int) -> int:
    try:
        value = getattr(SETTINGS, name)
    except AttributeError:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _ensure_timestamp(record: Dict, key: str) -> Tuple[Optional[datetime], bool]:
    original = record.get(key)
    parsed = _parse_datetime(original)
    if parsed is None:
        record.pop(key, None)
        return None, bool(original)
    formatted = _isoformat_utc(parsed)
    if formatted != original:
        record[key] = formatted
        return parsed, True
    return parsed, False


def _ensure_first_seen(record: Dict) -> bool:
    first_seen, changed = _ensure_timestamp(record, "first_seen")
    if first_seen is not None:
        return changed
    now_iso = _isoformat_utc(_now_utc())
    record["first_seen"] = now_iso
    return True


def _normalize_profile_edit_metadata(record: Dict) -> None:
    try:
        count = int(record.get("profile_edit_count", 0))
    except Exception:
        count = 0
    record["profile_edit_count"] = max(0, count)

    types_raw = record.get("profile_edit_types")
    if isinstance(types_raw, list):
        normalized = sorted({str(item).strip() for item in types_raw if str(item).strip()})
    else:
        normalized = []
    record["profile_edit_types"] = normalized

    _ensure_timestamp(record, "last_profile_edit")


def _recent_activity_counts() -> Dict[str, int]:
    global _ACTIVITY_CACHE
    window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
    now = _now_utc()
    if _ACTIVITY_CACHE is not None:
        cached_window, timestamp, cached_counts = _ACTIVITY_CACHE
        if cached_window == window_hours and now - timestamp < _ACTIVITY_CACHE_TTL:
            return dict(cached_counts)

    counts: Dict[str, int] = defaultdict(int)
    cutoff = now - timedelta(hours=window_hours)
    if _SENT_LOG.exists():
        try:
            for entry in load_jsonl_entries(_SENT_LOG, label="accounts.sent_log"):
                ts_raw = entry.get("ts")
                if ts_raw is None:
                    continue
                try:
                    ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                except Exception:
                    continue
                if ts < cutoff:
                    continue
                username = str(entry.get("account") or "").strip().lstrip("@").lower()
                if not username:
                    continue
                counts[username] += 1
        except Exception:
            counts = defaultdict(int)

    frozen = dict(counts)
    _ACTIVITY_CACHE = (window_hours, now, frozen)
    return frozen


def _auto_low_profile(record: Dict) -> Tuple[bool, str, int]:
    username = (record.get("username") or "").strip().lstrip("@").lower()
    if not username:
        return False, "", 0

    _ensure_first_seen(record)
    first_seen = _parse_datetime(record.get("first_seen"))
    age_days = 0.0
    if first_seen is not None:
        age_days = (_now_utc() - first_seen).total_seconds() / 86400

    recent_activity_map = _recent_activity_counts()
    recent_activity = int(recent_activity_map.get(username, 0))
    record["recent_activity_count"] = recent_activity

    age_limit = max(1, _settings_value("low_profile_age_days", 14))
    edits_threshold = max(1, _settings_value("low_profile_profile_edit_threshold", 3))
    activity_threshold = max(0, _settings_value("low_profile_activity_threshold", 30))

    is_new = age_days < age_limit
    edit_count = int(record.get("profile_edit_count", 0) or 0)
    has_many_edits = edit_count >= edits_threshold
    has_high_activity = activity_threshold > 0 and recent_activity >= activity_threshold

    reasons: list[str] = []
    if is_new:
        reasons.append(f"cuenta nueva ({int(age_days)}d)")
    if has_many_edits:
        reasons.append(f"{edit_count} cambios de perfil")
    if has_high_activity:
        window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
        reasons.append(f"{recent_activity} envÃƒÂ­os/{window_hours}h")

    should_flag = is_new and (has_many_edits or has_high_activity)
    reason_text = "; ".join(reasons) if should_flag else ""
    return should_flag, reason_text, recent_activity


def _record_profile_edit(username: str, kind: str) -> None:
    normalized = username.strip().lstrip("@").lower()
    if not normalized:
        return

    items = _load()
    updated = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored != normalized:
            continue
        record = dict(item)
        try:
            count = int(record.get("profile_edit_count", 0))
        except Exception:
            count = 0
        record["profile_edit_count"] = max(0, count) + 1

        types_raw = record.get("profile_edit_types")
        types: list[str]
        if isinstance(types_raw, list):
            types = [str(entry).strip() for entry in types_raw if str(entry).strip()]
        else:
            types = []
        kind_clean = kind.strip()
        if kind_clean and kind_clean not in types:
            types.append(kind_clean)
        record["profile_edit_types"] = sorted(set(types))
        record["last_profile_edit"] = _isoformat_utc(_now_utc())
        items[idx] = record
        updated = True
        break

    if updated:
        _save(items)


def _account_alias_fields(record: Dict[str, Any]) -> tuple[str, str]:
    raw_alias_id = normalize_alias_id(record.get("alias_id"), default="")
    raw_alias_display_name = normalize_alias_display(record.get("alias_display_name"), default="")
    raw_alias = normalize_alias_display(record.get("alias"), default="")
    candidate_display_name = raw_alias_display_name or raw_alias
    if raw_alias and raw_alias_display_name and raw_alias.casefold() != raw_alias_display_name.casefold():
        candidate_display_name = raw_alias
    if raw_alias_id == DEFAULT_ALIAS_ID or candidate_display_name.casefold() == DEFAULT_ALIAS_ID:
        return DEFAULT_ALIAS_ID, DEFAULT_ALIAS_DISPLAY_NAME
    if raw_alias_id:
        if candidate_display_name and normalize_alias_id(candidate_display_name, default="") == raw_alias_id:
            return raw_alias_id, candidate_display_name
        if raw_alias and normalize_alias_id(raw_alias, default=DEFAULT_ALIAS_ID) != raw_alias_id:
            return normalize_alias_id(raw_alias, default=DEFAULT_ALIAS_ID), raw_alias
        return raw_alias_id, raw_alias_id
    if candidate_display_name:
        return (
            normalize_alias_id(candidate_display_name, default=DEFAULT_ALIAS_ID),
            candidate_display_name,
        )
    return DEFAULT_ALIAS_ID, DEFAULT_ALIAS_DISPLAY_NAME


def _account_alias_id(record: Dict[str, Any]) -> str:
    alias_id, _display_name = _account_alias_fields(record)
    return alias_id


def _validate_user_alias_display(value: Any, *, allow_default: bool = False) -> str:
    display_name = normalize_alias_display(value)
    if not display_name and allow_default:
        return DEFAULT_ALIAS_DISPLAY_NAME
    if allow_default and normalize_alias_id(display_name, default="") == DEFAULT_ALIAS_ID:
        return DEFAULT_ALIAS_DISPLAY_NAME
    try:
        return alias_record_from_input(display_name).display_name
    except AliasValidationError as exc:
        raise ValueError(str(exc)) from exc


def _normalize_account(record: Dict) -> Dict:
    result = dict(record)
    alias_id, alias_display_name = _account_alias_fields(result)
    result["alias_id"] = alias_id
    result["alias_display_name"] = alias_display_name
    if not result.get("alias"):
        result["alias"] = alias_display_name
    result.setdefault("active", True)
    result["usage_state"] = normalize_account_usage_state(result.get("usage_state"))
    result.setdefault("connected", False)
    result.setdefault("password", "")
    result.setdefault("assigned_proxy_id", None)
    result.setdefault("proxy_url", "")
    result.setdefault("proxy_user", "")
    result.setdefault("proxy_pass", "")
    sticky_default = SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_value = int(result.get("proxy_sticky_minutes", sticky_default))
    except Exception:
        sticky_value = sticky_default
    result["proxy_sticky_minutes"] = max(1, sticky_value)

    _normalize_profile_edit_metadata(result)
    _ensure_first_seen(result)

    username = result.get("username")
    if username:
        key = _password_key(username)
        if not result.get("password") and key:
            cached = _cached_password(username)
            if cached:
                result["password"] = cached
        result["has_totp"] = has_totp_secret(username)
    else:
        result.setdefault("has_totp", False)

    manual_override = bool(result.get("low_profile_manual"))
    auto_flag, auto_reason, recent_activity = _auto_low_profile(result)
    result["recent_activity_count"] = recent_activity
    result["low_profile_auto"] = auto_flag

    if manual_override:
        manual_value = bool(result.get("low_profile"))
        result["low_profile"] = manual_value
        existing_reason = str(result.get("low_profile_reason") or "")
        if manual_value and not existing_reason:
            result["low_profile_reason"] = "Marcado manualmente"
        elif not manual_value:
            result["low_profile_reason"] = existing_reason
        result["low_profile_source"] = "manual" if manual_value else ""
    else:
        result["low_profile"] = auto_flag
        result["low_profile_reason"] = auto_reason if auto_flag else ""
        result["low_profile_source"] = "auto" if auto_flag else ""
        result.setdefault("low_profile_manual", False)

    return result


def _prepare_for_save(record: Dict) -> Dict:
    stored = dict(record)
    alias_id, alias_display_name = _account_alias_fields(stored)
    stored["alias_id"] = alias_id
    stored["alias_display_name"] = alias_display_name
    stored["usage_state"] = normalize_account_usage_state(stored.get("usage_state"))
    stored.pop("alias", None)
    stored.pop("password", None)
    assigned_proxy_id = str(stored.get("assigned_proxy_id") or "").strip()
    if assigned_proxy_id:
        stored["assigned_proxy_id"] = assigned_proxy_id
    else:
        stored.pop("assigned_proxy_id", None)

    stored.pop("proxy_url", None)
    stored.pop("proxy_user", None)
    stored.pop("proxy_pass", None)

    if assigned_proxy_id:
        sticky_default = SETTINGS.proxy_sticky_minutes or 10
        try:
            sticky_value = int(stored.get("proxy_sticky_minutes", sticky_default))
        except Exception:
            sticky_value = sticky_default
        stored["proxy_sticky_minutes"] = max(1, sticky_value)
    else:
        stored.pop("proxy_sticky_minutes", None)
    stored.pop("has_totp", None)
    stored.pop("recent_activity_count", None)
    stored.pop("low_profile_auto", None)
    stored.pop("low_profile_source", None)

    manual_override = bool(stored.get("low_profile_manual"))
    if manual_override:
        stored["low_profile"] = bool(stored.get("low_profile"))
        reason = str(stored.get("low_profile_reason") or "")
        if reason:
            stored["low_profile_reason"] = reason
        else:
            stored.pop("low_profile_reason", None)
    else:
        stored.pop("low_profile", None)
        stored.pop("low_profile_reason", None)
        stored.pop("low_profile_manual", None)

    return stored


def _proxy_registry_store_path() -> Path:
    return Path(FILE).with_name("proxies.json")


def _latest_proxy_assignments_from_audit() -> Dict[str, str]:
    assignments: Dict[str, str] = {}
    try:
        audit_rows = load_proxy_audit_entries(
            limit=_PROXY_ASSIGN_AUDIT_LOOKBACK,
            path=proxy_audit_path(BASE),
        )
    except Exception:
        logger.exception("No se pudo leer la auditoria de proxies para reconstruir asignaciones.")
        return assignments

    for row in audit_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("event") or "").strip().lower() != "proxy_assign":
            continue
        if str(row.get("status") or "").strip().lower() != "ok":
            continue
        proxy_id = str(row.get("proxy_id") or "").strip()
        if not proxy_id:
            continue
        meta = row.get("meta")
        if not isinstance(meta, dict):
            continue
        accounts = meta.get("accounts")
        if not isinstance(accounts, list):
            continue
        for account in accounts:
            username = str(account or "").strip().lstrip("@").lower()
            if username:
                assignments[username] = proxy_id
    return assignments


def _validate_assigned_proxy_reference(proxy_id: Any) -> None:
    clean_id = str(proxy_id or "").strip()
    if not clean_id:
        return
    if get_proxy_by_id(clean_id, active_only=False, path=_proxy_registry_store_path()) is None:
        raise ValueError(f"El proxy asignado {clean_id} no existe.")


def _managed_proxy_id_for_username(username: Any) -> str:
    clean_username = str(username or "").strip().lstrip("@").lower()
    slug = re.sub(r"[^a-z0-9_.-]+", "-", clean_username).strip("-.")
    if not slug:
        slug = "cuenta"
    return f"{_MANAGED_ACCOUNT_PROXY_PREFIX}{slug}"


def _ensure_managed_account_proxy(record: Dict[str, Any], *, strict: bool) -> tuple[Dict[str, Any], bool]:
    payload = dict(record)
    username = str(payload.get("username") or "").strip().lstrip("@")
    proxy_url = str(payload.get("proxy_url") or "").strip()
    if not username or not proxy_url:
        return payload, False
    managed_proxy_id = _managed_proxy_id_for_username(username)
    current_proxy_id = str(payload.get("assigned_proxy_id") or "").strip()
    if current_proxy_id and current_proxy_id != managed_proxy_id:
        return payload, False
    proxy_record = {
        "id": managed_proxy_id,
        "server": proxy_url,
        "user": str(payload.get("proxy_user") or "").strip(),
        "pass": str(payload.get("proxy_pass") or "").strip(),
        "active": True,
    }
    try:
        upsert_proxy_record(proxy_record, _proxy_registry_store_path())
    except Exception:
        if strict:
            raise
        logger.exception("No se pudo materializar proxy administrado para %s", username)
        return payload, False
    changed = current_proxy_id != managed_proxy_id
    payload["assigned_proxy_id"] = managed_proxy_id
    return payload, changed


def _recover_proxy_assignment_from_audit(
    record: Dict[str, Any],
    *,
    audit_assignments: Dict[str, str] | None = None,
) -> tuple[Dict[str, Any], bool]:
    payload = dict(record)
    username = str(payload.get("username") or "").strip().lstrip("@")
    if not username:
        return payload, False
    if str(payload.get("assigned_proxy_id") or "").strip():
        return payload, False
    if str(payload.get("proxy_url") or "").strip():
        return payload, False

    assignment_map = audit_assignments or {}
    proxy_id = str(assignment_map.get(username.lower()) or "").strip()
    if not proxy_id:
        return payload, False
    if get_proxy_by_id(proxy_id, active_only=False, path=_proxy_registry_store_path()) is None:
        return payload, False

    payload["assigned_proxy_id"] = proxy_id
    logger.warning(
        "Se recupero proxy asignado desde auditoria para @%s -> %s.",
        username,
        proxy_id,
    )
    return payload, True


def _account_store_token() -> tuple[int, int] | None:
    target = Path(FILE)
    if not target.exists():
        return None
    try:
        stat = target.stat()
    except Exception:
        return None
    return int(stat.st_mtime_ns), int(stat.st_size)


def _sync_account_proxy_links(records: List[Dict]) -> None:
    token = _account_store_token()
    if token is None:
        return
    with _ACCOUNT_PROXY_LINK_SYNC_LOCK:
        try:
            sync_account_proxy_links(
                [dict(item) for item in records if isinstance(item, dict)],
                path=_proxy_registry_store_path(),
            )
        except Exception:
            logger.exception("No se pudo sincronizar la integridad cuenta-proxy.")
            raise


def _load() -> List[Dict]:
    with _ACCOUNT_STORE_LOCK:
        cleanup_deleted_accounts()
        if not FILE.exists():
            return []
        try:
            data = load_json_file(FILE, [], label="accounts.registry")
        except Exception:
            return []
        normalized: List[Dict] = []
        migrated_passwords: Dict[str, str] = {}
        audit_assignments = _latest_proxy_assignments_from_audit()
        changed = False
        for item in data:
            if not isinstance(item, dict):
                continue
            raw_alias = item.get("alias")
            normalized_alias = normalize_alias_display(raw_alias, default="")
            if not normalized_alias:
                normalized_alias = normalize_alias_display(
                    item.get("alias_display_name") or item.get("alias_id") or DEFAULT_ALIAS_DISPLAY_NAME,
                    default=DEFAULT_ALIAS_DISPLAY_NAME,
                )
            if normalized_alias != str(raw_alias or ""):
                item = dict(item)
                item["alias"] = normalized_alias
                changed = True
            normalized_item = _normalize_account(item)
            normalized_item, managed_changed = _ensure_managed_account_proxy(normalized_item, strict=False)
            normalized_item, recovered_from_audit = _recover_proxy_assignment_from_audit(
                normalized_item,
                audit_assignments=audit_assignments,
            )
            assigned_proxy_id = str(normalized_item.get("assigned_proxy_id") or "").strip()
            if assigned_proxy_id:
                if get_proxy_by_id(
                    assigned_proxy_id,
                    active_only=False,
                    path=_proxy_registry_store_path(),
                ) is None:
                    username = str(normalized_item.get("username") or "").strip().lstrip("@")
                    logger.warning(
                        "Proxy asignado inexistente para @%s: %s. Se limpiara assigned_proxy_id.",
                        username or "-",
                        assigned_proxy_id,
                    )
                    normalized_item = dict(normalized_item)
                    normalized_item["assigned_proxy_id"] = None
                    changed = True
            normalized.append(normalized_item)
            if managed_changed:
                changed = True
            if recovered_from_audit:
                changed = True
            if normalize_alias_id(item.get("alias_id"), default="") != normalized_item.get("alias_id"):
                changed = True
            original_alias_display_name = normalize_alias_display(
                item.get("alias_display_name") or item.get("alias"),
                default="",
            )
            if original_alias_display_name != normalized_item.get("alias_display_name"):
                changed = True
            if item.get("first_seen") != normalized_item.get("first_seen"):
                changed = True
            if int(item.get("profile_edit_count", 0) or 0) != normalized_item.get(
                "profile_edit_count", 0
            ):
                changed = True
            original_types = item.get("profile_edit_types") if isinstance(item, dict) else []
            if isinstance(original_types, list):
                original_sorted = sorted({str(v).strip() for v in original_types if str(v).strip()})
            else:
                original_sorted = []
            if original_sorted != normalized_item.get("profile_edit_types", []):
                changed = True
            legacy_password = str(item.get("password") or "").strip()
            if legacy_password:
                key = _password_key(normalized_item.get("username"))
                if key and _PASSWORD_CACHE.get(key) != legacy_password:
                    migrated_passwords[key] = legacy_password
                changed = True
        if migrated_passwords:
            _configure_password_backend()
            _PASSWORD_CACHE.update(migrated_passwords)
            _save_password_cache(_PASSWORD_CACHE)
        if changed:
            try:
                cleaned = [_prepare_for_save(_normalize_account(it)) for it in normalized]
                atomic_write_json(FILE, cleaned)
            except Exception:
                pass
        _sync_account_proxy_links(normalized)
        return normalized


def _save(items: List[Dict]) -> None:
    with _ACCOUNT_STORE_LOCK:
        normalized: List[Dict] = []
        for item in items:
            normalized_item = _normalize_account(item)
            normalized_item, _changed = _ensure_managed_account_proxy(normalized_item, strict=False)
            normalized.append(normalized_item)
        cleaned = [_prepare_for_save(item) for item in normalized]
        atomic_write_json(FILE, cleaned)
        _sync_account_proxy_links(normalized)


def list_all() -> List[Dict]:
    return _load()


def get_accounts() -> List[Any]:
    from types import SimpleNamespace

    def _is_active(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"0", "false", "no", "off", "inactive", "disabled"}:
            return False
        if text in {"1", "true", "yes", "on", "active", "enabled"}:
            return True
        return bool(value)

    accounts: List[Any] = []
    for item in list_all():
        if not isinstance(item, dict):
            continue
        username = str(item.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        accounts.append(
            SimpleNamespace(
                username=username,
                active=_is_active(item.get("active", True)),
                usage_state=account_usage_state(item),
                data=dict(item),
            )
        )
    return accounts


def _find(items: List[Dict], username: str) -> Optional[Dict]:
    username = username.lower()
    for it in items:
        if it.get("username", "").lower() == username:
            return it
    return None


def get_account(username: str) -> Optional[Dict]:
    items = _load()
    account = _find(items, username)
    return _normalize_account(account) if account else None


def update_account(username: str, updates: Dict, *, invalidate_health: bool = False) -> bool:
    with _ACCOUNT_STORE_LOCK:
        items = _load()
        username_norm = username.lower()
        for idx, item in enumerate(items):
            if item.get("username", "").lower() == username_norm:
                updated = dict(item)
                updated.update(updates)
                if "assigned_proxy_id" in updates:
                    _validate_assigned_proxy_reference(updated.get("assigned_proxy_id"))
                normalized_updated = _normalize_account(updated)
                normalized_updated, _changed = _ensure_managed_account_proxy(normalized_updated, strict=True)
                items[idx] = normalized_updated
                _save(items)
                if invalidate_health:
                    _invalidate_health(username)
                return True
        return False


def _prompt_totp(username: str) -> bool:
    while True:
        raw = ask("TOTP Secret / otpauth URI (opcional): ").strip()
        if not raw:
            return False
        try:
            save_totp_secret(username, raw)
            ok("Se guardÃƒÂ³ el TOTP cifrado para esta cuenta.")
            # Muestra el cÃƒÂ³digo actual para facilitar el primer login manual.
            current = generate_totp_code(username)
            if current:
                print(f"CÃƒÂ³digo TOTP actual (cambia cada 30s): {current}")
            return True
        except ValueError as exc:
            warn(f"No se pudo guardar el TOTP: {exc}")
            retry = ask("Ã‚Â¿Reintentar ingreso de TOTP? (s/N): ").strip().lower()
            if retry != "s":
                return False


def _onboarding_backend_ready() -> bool:
    return bool(_ONBOARDING_AVAILABLE and login_and_persist and onboard_accounts_from_csv)


def _print_onboarding_backend_help() -> None:
    if _ONBOARDING_BACKEND_ERROR:
        print(f"[ERROR] Backend de onboarding no disponible: {_ONBOARDING_BACKEND_ERROR}")
    else:
        print("[ERROR] Backend de onboarding no cargado.")
    print("InstalÃƒÂ¡ dependencias: pip install playwright pyotp y luego playwright install")
    print("Verifica que existan los archivos src/__init__.py y src/auth/__init__.py")


def _playwright_proxy_payload(settings: Optional[Dict]) -> Optional[Dict[str, str]]:
    if not settings:
        return None

    if _proxy_from_account:
        try:
            payload = _proxy_from_account(settings)
        except ProxyResolutionError:
            raise
        except Exception:
            payload = None
        if payload:
            return payload

    if build_playwright_proxy and settings.get("proxy"):
        with contextlib.suppress(Exception):
            payload = build_playwright_proxy(settings.get("proxy"))
            if payload:
                return payload

    payload = {
        "url": settings.get("proxy_url"),
        "username": settings.get("proxy_user"),
        "password": settings.get("proxy_pass"),
    }
    if not build_playwright_proxy:
        return None
    return build_playwright_proxy(payload)


def _account_has_proxy(payload: Optional[Dict[str, Any]]) -> bool:
    if not payload:
        return False
    if payload.get("proxy"):
        return True
    proxy_url = str(payload.get("proxy_url") or "").strip()
    if proxy_url:
        return True
    assigned_proxy_id = str(payload.get("assigned_proxy_id") or "").strip()
    if assigned_proxy_id:
        return True
    return bool(_playwright_proxy_payload(payload))

def _refresh_totp_export_cache(force: bool = False) -> None:
    """Legacy helper kept for manual TOTP repair/migration outside login runtime."""
    global _TOTP_EXPORT_CACHE
    global _TOTP_EXPORT_CACHE_TIMESTAMP
    now = time.time()
    if not force and _TOTP_EXPORT_CACHE and (now - _TOTP_EXPORT_CACHE_TIMESTAMP) < 60:
        return

    candidates: List[Path] = []
    for directory in (storage_root(BASE), Path.home() / "Desktop" / "archivos CSV"):
        try:
            if directory.exists():
                candidates.extend(path for path in directory.glob("*.csv") if path.is_file())
        except Exception:
            continue

    # Priorizamos los mÃ¡s recientes para evitar secretos viejos.
    candidates.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0.0, reverse=True)

    cache: Dict[str, str] = {}
    for path in candidates[:80]:
        parsed = _extract_totp_entries_from_csv(path)
        for key, secret in parsed.items():
            if key and secret and key not in cache:
                cache[key] = secret

    _TOTP_EXPORT_CACHE = cache
    _TOTP_EXPORT_CACHE_TIMESTAMP = now


def _ensure_totp_for_playwright(username: str, *, force_refresh: bool = False) -> bool:
    del force_refresh
    if not has_totp_secret(username):
        return False
    return bool(get_totp_secret(username))


def _playwright_account_payload(
    username: str,
    password: str,
    proxy_settings: Optional[Dict],
    *,
    force_totp_refresh: bool = False,
) -> Dict[str, Any]:
    del force_totp_refresh
    payload: Dict[str, Any] = {
        "username": username,
        "password": password,
    }
    proxy_payload = _playwright_proxy_payload(proxy_settings)
    if proxy_payload:
        payload["proxy"] = proxy_payload
    if _ensure_totp_for_playwright(username):
        secret = get_totp_secret(username)
        if secret:
            payload["totp_secret"] = secret
            payload["totp_callback"] = lambda _ignored, target=username: generate_totp_code(target)
    return payload


def _playwright_onboarding(username: str, password: str, proxy_settings: Optional[Dict]) -> Dict[str, str]:
    if not _onboarding_backend_ready():
        raise RuntimeError("Backend de onboarding no disponible.")
    payload = _playwright_account_payload(username, password, proxy_settings)
    return login_and_persist(payload, headless=False)


def _build_playwright_login_payload(
    username: str,
    password: str,
    proxy_settings: Optional[Dict],
    *,
    alias: str,
    totp_secret: Optional[str] = None,
    row_number: Optional[int] = None,
) -> Dict[str, Any]:
    del totp_secret
    payload = _playwright_account_payload(username, password, proxy_settings)
    payload["alias"] = alias
    payload["disable_safe_browser_recovery"] = True
    if row_number is not None:
        payload["row_number"] = row_number
    return payload


def playwright_login_queue_concurrency() -> int:
    # Las sesiones persistentes visibles de Chrome se procesan en cola: una cuenta por vez.
    return 1


def login_accounts_with_playwright(
    alias: str,
    accounts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    try:
        from src.auth.onboarding import login_account_playwright, write_onboarding_results
    except Exception as exc:
        warn(f"No se pudo iniciar login con Playwright: {exc}")
        return []

    _queue_login_progress(
        [
            str(account.get("username") or "").strip().lstrip("@")
            for account in accounts
            if str(account.get("username") or "").strip()
        ]
    )
    results: List[Dict[str, Any]] = []
    for account in accounts:
        username = str(account.get("username") or "").strip().lstrip("@")
        _set_login_progress(
            username,
            health_store.LOGIN_PROGRESS_OPENING_BROWSER,
            message="Abriendo navegador",
        )
        account = dict(account)
        account["login_progress_callback"] = _login_progress_callback(username)
        try:
            results.append(login_account_playwright(account, alias, headful=True))
        except Exception as exc:
            results.append(
                {
                    "username": (account.get("username") or "").strip(),
                    "status": "failed",
                    "message": str(exc),
                    "profile_path": "",
                    "row_number": account.get("row_number"),
                }
            )

    # Normaliza orden si viene desde CSV (usa row_number si estÃ¡).
    if results and any(item.get("row_number") is not None for item in results):
        results.sort(key=lambda item: item.get("row_number") or 0)

    # El account health canonico lo escribe el flujo Playwright real.
    # Aqui solo sincronizamos el estado persistido sin invalidar ese resultado.
    for result in results:
        _sync_playwright_login_result(
            result,
            clear_stale_session_on_failure=False,
        )

    try:
        write_onboarding_results(results)
    except Exception:
        pass
    return results


def relogin_accounts_with_playwright(
    alias: str,
    accounts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Fuerza un relogin visible con Playwright/Chrome real para las cuentas indicadas y
    persiste storage_state.json en runtime/browser_profiles/<username>/storage_state.json.

    Nota: si no hay password guardada para una cuenta se solicitarÃ¡ por consola.
    """

    try:
        from src.auth.onboarding import login_account_playwright, write_onboarding_results
    except Exception as exc:
        warn(f"No se pudo iniciar relogin con Playwright: {exc}")
        return []

    def _password_for(account: Dict[str, Any]) -> str:
        username = (account.get("username") or "").strip().lstrip("@")
        password = _account_password(account).strip()
        if not password and username:
            # Reconsulta estado persistido por si el dict recibido estaba desactualizado.
            refreshed = get_account(username)
            if refreshed:
                password = _account_password(refreshed).strip()
        if password:
            return password
        entered = _ask_secret(f"Password @{username} (Enter = omitir): ")
        entered = (entered or "").strip()
        if entered:
            _store_account_password(username, entered)
        return entered

    payloads: List[Dict[str, Any]] = []
    for acct in accounts:
        username = (acct.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        password = _password_for(acct)
        if not password:
            warn(f"@{username}: sin password, se omite relogin.")
            continue
        payload = _playwright_account_payload(
            username,
            password,
            acct,
        )
        payload["alias"] = alias
        payload["strict_login"] = True
        payload["force_login"] = True
        payload["disable_safe_browser_recovery"] = True
        if not payload.get("totp_secret") and not callable(payload.get("totp_callback")):
            logger.warning(
                "Relogin @%s sin TOTP disponible en store canónico.",
                username,
            )
        payloads.append(payload)

    if not payloads:
        return []

    _queue_login_progress(
        [
            str(payload.get("username") or "").strip().lstrip("@")
            for payload in payloads
            if str(payload.get("username") or "").strip()
        ]
    )
    results: List[Dict[str, Any]] = []
    for payload in payloads:
        username = str(payload.get("username") or "").strip().lstrip("@")
        _set_login_progress(
            username,
            health_store.LOGIN_PROGRESS_OPENING_BROWSER,
            message="Abriendo navegador",
        )
        payload = dict(payload)
        payload["login_progress_callback"] = _login_progress_callback(username)
        try:
            results.append(login_account_playwright(payload, alias, headful=True))
        except Exception as exc:
            results.append(
                {
                    "username": (payload.get("username") or "").strip(),
                    "status": "failed",
                    "message": str(exc),
                    "profile_path": "",
                    "row_number": payload.get("row_number"),
                }
            )

    for result in results:
        _sync_playwright_login_result(
            result,
            clear_stale_session_on_failure=True,
        )

    try:
        write_onboarding_results(results)
    except Exception:
        pass
    return results


def add_account(username: str, alias: str, proxy: Optional[Dict] = None) -> bool:
    items = _load()
    if _find(items, username):
        warn("Ya existe.")
        return False
    record = {
        "username": username.strip().lstrip("@"),
        "alias": alias,
        "active": True,
        "usage_state": ACCOUNT_USAGE_STATE_ACTIVE,
        "connected": False,
    }
    if proxy:
        _validate_assigned_proxy_reference(proxy.get("assigned_proxy_id"))
        record.update(proxy)
    normalized_record = _normalize_account(record)
    normalized_record, _changed = _ensure_managed_account_proxy(normalized_record, strict=True)
    items.append(normalized_record)
    _save(items)
    health_store.mark_session_expired(username, reason="storage_state_missing")
    ok("Agregada.")
    return True


def sync_alias_metadata(alias: str, *, alias_id: str | None = None, display_name: str | None = None) -> int:
    source_alias_id = normalize_alias_id(alias, default=DEFAULT_ALIAS_ID)
    target_alias_id = normalize_alias_id(alias_id or display_name or source_alias_id, default=DEFAULT_ALIAS_ID)
    target_display_name = normalize_alias_display(
        display_name or alias_id or source_alias_id,
        default=DEFAULT_ALIAS_DISPLAY_NAME if target_alias_id == DEFAULT_ALIAS_ID else target_alias_id,
    )
    if target_alias_id == DEFAULT_ALIAS_ID:
        target_display_name = DEFAULT_ALIAS_DISPLAY_NAME

    items = _load()
    updated_count = 0
    for index, item in enumerate(items):
        if _account_alias_id(item) != source_alias_id:
            continue
        updated = dict(item)
        updated["alias_id"] = target_alias_id
        updated["alias_display_name"] = target_display_name
        updated["alias"] = target_display_name
        items[index] = _normalize_account(updated)
        updated_count += 1

    if updated_count:
        _save(items)
    return updated_count


def remove_account(username: str) -> None:
    items = _load()
    removed_username: str | None = None
    new_items: list[dict] = []
    username_norm = username.lower()
    for item in items:
        if item.get("username", "").lower() == username_norm:
            if removed_username is None:
                removed_username = str(item.get("username") or "").strip() or None
            continue
        new_items.append(item)
    _ensure_trash_accounts_file()
    if removed_username:
        _append_deleted_account_to_trash(removed_username)
    _save(new_items)
    remove_session(username)
    remove_totp_secret(username)
    clear_proxy(username)
    _invalidate_health(username)
    key = _password_key(username)
    if key and key in _PASSWORD_CACHE:
        _PASSWORD_CACHE.pop(key, None)
        _configure_password_backend()
        _save_password_cache(_PASSWORD_CACHE)
    ok("Eliminada (si existÃƒÂ­a).")


def set_active(username: str, is_active: bool = True) -> None:
    if update_account(username, {"active": is_active}):
        ok("Actualizada.")
    else:
        warn("No existe.")


def set_usage_state(username: str, usage_state: str = ACCOUNT_USAGE_STATE_ACTIVE) -> None:
    normalized = normalize_account_usage_state(usage_state)
    if update_account(username, {"usage_state": normalized}):
        ok("Actualizada.")
    else:
        warn("No existe.")


def mark_connected(username: str, connected: bool, *, invalidate_health: bool = False) -> None:
    updated = update_account(username, {"connected": connected}, invalidate_health=invalidate_health)
    health_store.set_connected(
        username,
        connected,
        source="accounts.mark_connected",
    )
    if invalidate_health and not updated:
        _invalidate_health(username)


def _queue_login_progress(usernames: list[str]) -> None:
    for username in usernames:
        clean_username = str(username or "").strip().lstrip("@")
        if not clean_username:
            continue
        health_store.set_login_progress(
            clean_username,
            health_store.LOGIN_PROGRESS_QUEUED,
            message="En cola",
        )


def _set_login_progress(username: str, state: str, *, message: str = "") -> None:
    clean_username = str(username or "").strip().lstrip("@")
    if not clean_username:
        return
    health_store.set_login_progress(
        clean_username,
        state,
        message=message,
    )


def _clear_login_progress(username: str) -> None:
    clean_username = str(username or "").strip().lstrip("@")
    if not clean_username:
        return
    health_store.clear_login_progress(clean_username)


def _login_progress_callback(username: str):
    clean_username = str(username or "").strip().lstrip("@")

    def _callback(state: str, message: str = "") -> None:
        _set_login_progress(clean_username, state, message=message)

    return _callback


def _proxy_config_from_inputs(data: Dict) -> ProxyConfig:
    return ProxyConfig(
        url=data.get("proxy_url", ""),
        user=data.get("proxy_user") or None,
        password=data.get("proxy_pass") or None,
        sticky_minutes=int(data.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)),
    )


def _prompt_proxy_settings(existing: Optional[Dict] = None) -> Dict:
    defaults = default_proxy_settings()
    current = existing or {}
    print("\nConfiguraciÃƒÂ³n de proxy (opcional)")
    base_default = current.get("proxy_url") or defaults["url"]
    prompt_default = base_default or "sin proxy"
    raw_url = ask(f"Proxy URL [{prompt_default}]: ").strip()
    if raw_url.lower() in {"-", "none", "sin", "no"}:
        url = ""
    elif not raw_url and base_default:
        url = base_default
    else:
        url = raw_url

    user_default = current.get("proxy_user") or defaults["user"]
    user_prompt = user_default or "(sin definir)"
    proxy_user = ask(f"Usuario (opcional) [{user_prompt}]: ").strip() or user_default

    pass_default = current.get("proxy_pass") or defaults["password"]
    pass_prompt = "***" if pass_default else "(sin definir)"
    proxy_pass = ask(f"Password (opcional) [{pass_prompt}]: ").strip() or pass_default

    sticky_default = current.get("proxy_sticky_minutes") or defaults["sticky"]
    sticky_input = ask(f"Sticky minutes [{sticky_default}]: ").strip()
    try:
        sticky = int(sticky_input) if sticky_input else int(sticky_default)
    except Exception:
        sticky = int(defaults["sticky"] or 10)
    sticky = max(1, sticky)

    proxy_url = url.strip()
    data = {
        "proxy_url": proxy_url,
        "proxy_user": (proxy_user or "").strip(),
        "proxy_pass": (proxy_pass or "").strip(),
        "proxy_sticky_minutes": sticky,
    }

    if not proxy_url:
        return {"proxy_url": "", "proxy_user": "", "proxy_pass": "", "proxy_sticky_minutes": sticky}

    if ask("Ã‚Â¿Probar proxy ahora? (s/N): ").strip().lower() == "s":
        try:
            result = test_proxy_connection(_proxy_config_from_inputs(data))
            ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
        except Exception as exc:
            warn(f"Proxy fallÃƒÂ³: {exc}")
            retry = ask("Ã‚Â¿Reintentar configuraciÃƒÂ³n? (s/N): ").strip().lower()
            if retry == "s":
                return _prompt_proxy_settings(existing)
    return data


def _test_existing_proxy(account: Dict) -> None:
    config = None
    with contextlib.suppress(Exception):
        config = config_from_account(account)
    if not config:
        warn("La cuenta no tiene proxy configurado.")
        return
    try:
        result = test_proxy_connection(config)
        ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
    except Exception as exc:
        warn(f"Error probando proxy: {exc}")

def _launch_content_publisher(alias: str) -> None:
    try:
        from automation.actions import content_publisher
    except Exception as exc:  # pragma: no cover - mÃƒÂ³dulo opcional
        warn(f"No se pudo iniciar el mÃƒÂ³dulo de publicaciones: {exc}")
        press_enter()
        return

    content_publisher.run_from_menu(alias)


def _launch_interactions(alias: str) -> None:
    try:
        from automation.actions import interactions
    except Exception as exc:  # pragma: no cover - mÃƒÂ³dulo opcional
        warn(f"No se pudo iniciar el mÃƒÂ³dulo de interacciones: {exc}")
        press_enter()
        return

    interactions.run_from_menu(alias)


def _login_and_save_session(
    account: Dict, password: str, *, respect_backoff: bool = True
) -> bool:
    """Login con Playwright y persistencia de profile/storage_state."""

    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        return False
    if respect_backoff:
        remaining = _login_backoff_remaining(username)
        if remaining > 0:
            logger.debug(
                "Omitiendo login automÃƒÂ¡tico para @%s (reintentar en %.0fs)",
                username,
                remaining,
            )
            return False

    try:
        if not _onboarding_backend_ready():
            raise RuntimeError("Backend de onboarding no disponible.")
        proxy_settings = config_from_account(account)
        payload = _playwright_account_payload(username, password, proxy_settings)
        result = login_and_persist(payload, headless=False)
    except Exception as exc:
        if _account_has_proxy(account) and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False, invalidate_health=False)
        _record_login_failure(username)
        warn(f"No se pudo iniciar sesiÃƒÂ³n para @{username}: {exc}")
        return False

    status = str((result or {}).get("status") or "").strip().lower()
    message = str((result or {}).get("message") or "").strip()
    if status == "ok":
        mark_connected(username, True, invalidate_health=False)
        _clear_login_failure(username)
        account["has_totp"] = has_totp_secret(username)
        ok(f"SesiÃƒÂ³n Playwright guardada para {username}.")
        return True

    mark_connected(username, False, invalidate_health=False)
    _record_login_failure(username)
    warn(message or f"No se pudo iniciar sesiÃƒÂ³n para @{username}.")
    return False


def _session_active(
    username: str,
    *,
    account: Optional[Dict] = None,
    reason: str = "session-check",
    strict: bool = False,
) -> bool:
    if not username:
        return False

    account = account or get_account(username)
    connected = _playwright_session_active(
        username,
        account=account,
        strict=strict,
    )
    mark_connected(username, connected, invalidate_health=False)
    if not connected:
        logger.debug(
            "La sesiÃƒÂ³n Playwright para @%s no estÃƒÂ¡ activa (%s).",
            username,
            reason,
        )
    return connected


def auto_login_with_saved_password(
    username: str, *, account: Optional[Dict] = None
) -> bool:
    """Intenta iniciar sesiÃƒÂ³n reutilizando la contraseÃƒÂ±a almacenada."""

    account = account or get_account(username)
    if not account:
        return False

    if _session_active(username, account=account, reason="auto-login-check"):
        return True

    stored_password = _account_password(account).strip()
    if not stored_password:
        return False

    return _login_and_save_session(account, stored_password)


def prompt_login(username: str, *, interactive: bool = True) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False

    if _session_active(username, account=account, reason="prompt-login"):
        return True
    stored_password = _account_password(account).strip()
    original_stored = stored_password
    attempted_auto = False

    if stored_password:
        attempted_auto = True
        if auto_login_with_saved_password(username, account=account):
            return True

    while True:
        if attempted_auto and stored_password:
            changed = (
                ask("Ã‚Â¿Cambiaste la contraseÃƒÂ±a de esta cuenta? (s/N): ")
                .strip()
                .lower()
            )
            if changed != "s":
                warn(
                    "Instagram rechazÃƒÂ³ la sesiÃƒÂ³n guardada. Posiblemente haya un challenge o chequeo de seguridad pendiente."
                )
                return False
            password = _ask_secret(
                f"Nueva password @{account['username']}: "
            )
        else:
            password = _ask_secret(
                f"Password @{account['username']}: "
            )

        if not password:
            warn("Se cancelÃƒÂ³ el inicio de sesiÃƒÂ³n.")
            return False

        success = _login_and_save_session(
            account, password, respect_backoff=False
        )
        if success:
            if password != original_stored:
                _store_account_password(username, password)
            return True

        attempted_auto = False
        stored_password = ""
        if interactive and (
            ask("Ã‚Â¿Intentar ingresar nuevamente? (s/N): ")
            .strip()
            .lower()
            == "s"
        ):
            continue
        return False


def _low_profile_indicator(account: Dict) -> str:
    return " [LP] bajo perfil" if account.get("low_profile") else ""


def _proxy_indicator(account: Dict) -> str:
    return " [PROXY]" if _account_has_proxy(account) else ""

def _totp_indicator(account: Dict) -> str:
    return " [2FA]" if account.get("has_totp") else ""


def _has_playwright_session(username: str) -> bool:
    if not username:
        return False
    try:
        path = _playwright_storage_state_path(username)
        return path.exists() and path.stat().st_size > 0
    except Exception:
        return False


def has_playwright_storage_state(username: str) -> bool:
    return _has_playwright_session(username)


def playwright_storage_state_path(username: str) -> Path:
    return _playwright_storage_state_path(username)


def _playwright_storage_state_path(username: str) -> Path:
    return browser_storage_state_path(username, profiles_root=BASE_PROFILES)


def _clear_playwright_storage_state(username: str) -> None:
    path = _playwright_storage_state_path(username)
    with contextlib.suppress(Exception):
        with path_lock(path):
            path.unlink(missing_ok=True)


def _sync_playwright_login_result(
    result: Dict[str, Any],
    *,
    clear_stale_session_on_failure: bool,
) -> None:
    username = str(result.get("username") or "").strip().lstrip("@")
    if not username:
        return

    success = str(result.get("status") or "").strip().lower() == "ok"
    mark_connected(username, success, invalidate_health=False)

    if success:
        health_store.mark_alive(username, reason="login_success")
        _clear_login_progress(username)
        return

    if clear_stale_session_on_failure:
        _clear_playwright_storage_state(username)

    _clear_login_progress(username)
    cached_state, _expired = health_store.get_badge(username)
    if not cached_state:
        health_store.mark_session_expired(username, reason="login_failed")


def _playwright_cookie_session_active(username: str) -> bool:
    if not username:
        return False
    try:
        path = _playwright_storage_state_path(username)
        if not path.exists() or path.stat().st_size <= 0:
            return False
        payload = load_json_file(
            path,
            {"cookies": [], "origins": []},
            label=f"accounts.storage_state.{username}",
        )
    except Exception:
        return False

    cookies = payload.get("cookies")
    if not isinstance(cookies, list):
        return False

    now = time.time()
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name") or "").strip().lower()
        if name != "sessionid":
            continue
        value = str(cookie.get("value") or "").strip()
        if not value:
            continue
        expires = cookie.get("expires")
        if expires in (None, "", -1, 0):
            return True
        try:
            if float(expires) > now:
                return True
        except Exception:
            return True
    return False


def _playwright_session_active(
    username: str,
    *,
    account: Optional[Dict] = None,
    strict: bool = False,
) -> bool:
    if not _has_playwright_session(username):
        return False

    if strict and _check_playwright_session:
        proxy_payload = None
        with contextlib.suppress(Exception):
            proxy_payload = _playwright_proxy_payload(account)
        try:
            ok, _reason = _check_playwright_session(
                username,
                proxy=proxy_payload,
                headless=True,
            )
            return bool(ok)
        except Exception as exc:
            logger.debug(
                "No se pudo validar la sesion de Playwright para @%s: %s",
                username,
                exc,
            )

    return _playwright_cookie_session_active(username)


def _session_label(username: str) -> str:
    if has_session(username) or _has_playwright_session(username):
        return "[sesiÃƒÂ³n]"
    return "[sin sesiÃƒÂ³n]"


def connected_status(
    account: Dict,
    *,
    strict: bool = False,
    reason: str = "connection-status",
    fast: bool = False,
    persist: bool = True,
) -> bool:
    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        return False

    has_api_session = has_session(username)
    has_playwright_file = _has_playwright_session(username)

    connected = False
    if fast and not strict:
        # Ruta ultra-rÃƒÂ¡pida para menÃƒÂºs: sÃƒÂ³lo seÃƒÂ±ales locales de sesiÃƒÂ³n.
        stored_connected, _source, _stored_reason = health_store.get_connected(username)
        if stored_connected is not None:
            connected = bool(stored_connected)
        else:
            connected = bool(account.get("connected", False))
    else:
        if has_api_session:
            connected = _session_active(username, account=account, reason=reason, strict=strict)
            account["connected"] = connected
        if not connected and has_playwright_file:
            connected = _playwright_session_active(
                username,
                account=account,
                strict=strict,
            )

    current = bool(account.get("connected"))
    if current != connected:
        if persist:
            mark_connected(username, connected, invalidate_health=False)
        account["connected"] = connected

    return connected


def _invalidate_health(username: str) -> None:
    health_store.invalidate(username)


def _health_cached(username: str) -> tuple[str | None, bool]:
    return health_store.get_badge(username)


ACCOUNT_UI_STATE_UNVERIFIED = "NO VERIFICADA"


def _badge_for_display(account: Dict) -> tuple[str, bool]:
    username = str(account.get("username") or "").strip().lstrip("@")
    cached_state, expired = _health_cached(username)
    if cached_state:
        return cached_state, expired

    legacy_badge = str(account.get("health_badge") or "").strip()
    if legacy_badge:
        # Read-only: legacy badge parsing for display only.
        # Never persist derived health here.
        with contextlib.suppress(Exception):
            normalized = getattr(health_store, "_coerce_state", None)
            if callable(normalized):
                coerced = normalized(legacy_badge)
                if coerced:
                    return str(coerced), True

    has_api_session = has_session(username)
    has_playwright_session = _has_playwright_session(username)
    if not has_api_session and not has_playwright_session:
        return health_store.HEALTH_STATE_INACTIVE, False

    if bool(account.get("connected")):
        return health_store.HEALTH_STATE_ALIVE, True

    return health_store.HEALTH_STATE_INACTIVE, True


def _life_status_badge(account: Dict, badge: str) -> str:
    normalized = str(badge or "").strip().upper()
    if normalized == health_store.HEALTH_STATE_ALIVE:
        return "[VIVA]"
    if normalized == health_store.HEALTH_STATE_INACTIVE:
        return "[NO ACTIVA]"
    if normalized == health_store.HEALTH_STATE_DEAD:
        return "[MUERTA]"
    if not account.get("active"):
        return "[NO ACTIVA]"
    return "[NO ACTIVA]"


def _account_status_from_badge(account: Dict, badge: str) -> str:
    if not account.get("active"):
        return "no activa"
    normalized = str(badge or "").strip().upper()
    if normalized == health_store.HEALTH_STATE_ALIVE:
        return "viva"
    if normalized == health_store.HEALTH_STATE_DEAD:
        return "muerta"
    return "no activa"


def _proxy_status_from_badge(badge: str) -> str:
    lowered = (badge or "").lower()
    if "proxy" in lowered and any(term in lowered for term in ("caÃƒÂ­do", "caido", "bloqueado")):
        return "bloqueado"
    return "activo"


def _current_totp_code(username: str) -> str:
    if not username:
        return ""
    try:
        code = generate_totp_code(username)
    except Exception:
        return ""
    return code or ""


def _proxy_components(account: Dict) -> tuple[str, str, str, str]:
    raw_url = ""
    proxy_user = ""
    proxy_pass = ""
    if _proxy_fields_from_account:
        try:
            fields = _proxy_fields_from_account(account)
        except ProxyResolutionError:
            raise
        except Exception:
            fields = {}
        raw_url = str(fields.get("proxy_url") or "").strip()
        proxy_user = str(fields.get("proxy_user") or "").strip()
        proxy_pass = str(fields.get("proxy_pass") or "").strip()
    else:
        payload = _playwright_proxy_payload(account)
        if payload:
            raw_url = str(payload.get("server") or payload.get("url") or payload.get("proxy") or "").strip()
            proxy_user = str(payload.get("username") or payload.get("user") or "").strip()
            proxy_pass = str(payload.get("password") or payload.get("pass") or "").strip()
        else:
            raw_url = str(account.get("proxy_url") or "").strip()
            proxy_user = str(account.get("proxy_user") or "").strip()
            proxy_pass = str(account.get("proxy_pass") or "").strip()

    ip = ""
    port = ""
    if raw_url:
        parsed = urlparse(raw_url if "://" in raw_url else f"http://{raw_url}")
        ip = (parsed.hostname or "").strip()
        port = str(parsed.port) if parsed.port else ""
    return ip, port, proxy_user, proxy_pass

def _alias_slug(alias: str) -> str:
    candidate = re.sub(r"[^0-9A-Za-z_-]", "_", alias.strip())
    candidate = candidate.strip("_")
    return candidate or "default"


def _export_paths(alias: str) -> tuple[Path, Path]:
    base_dir = Path.home() / "Desktop" / "archivos CSV"
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _alias_slug(alias)
    csv_path = base_dir / f"{slug}_accounts_{timestamp}.csv"
    totp_backup_path = base_dir / f"{slug}_totp_backup_{timestamp}.zip"
    return csv_path, totp_backup_path


def _totp_store_dir() -> Path:
    # Reusa el mismo BASE/runtime_base que totp_store.py para apuntar al mismo storage.
    return storage_root(BASE) / "totp"


def _totp_record_path(username: str) -> Path:
    safe = normalize_totp_username(username)
    return _totp_store_dir() / f"{safe}.json"


def _export_totp_backup_zip(usernames: List[str], destination: Path) -> int:
    """
    Exporta un backup cifrado de los secretos TOTP (NO en texto plano).
    Se copian los archivos JSON de storage/totp para los usernames indicados.
    Nota: no incluye `.master_key` por seguridad.
    """

    store_dir = _totp_store_dir()
    if not store_dir.exists():
        return 0

    normalized = []
    seen = set()
    for u in usernames:
        cleaned = (u or "").strip().lstrip("@")
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)

    if not normalized:
        return 0

    written = 0
    try:
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for username in normalized:
                path = _totp_record_path(username)
                if not path.exists():
                    continue
                arcname_root = storage_root(BASE, scoped=False, honor_env=False).name
                arcname = str(Path(arcname_root) / "totp" / path.name)
                zf.write(path, arcname=arcname)
                written += 1
    except Exception as exc:
        warn(f"No se pudo generar backup TOTP: {exc}")
        try:
            if destination.exists():
                destination.unlink()
        except Exception:
            pass
        return 0

    if written == 0:
        try:
            if destination.exists():
                destination.unlink()
        except Exception:
            pass

    return written


def _account_password(account: Dict) -> str:
    value = account.get("password")
    if isinstance(value, str) and value:
        return value
    return _cached_password(account.get("username"))


def _store_account_password(username: str, password: str) -> None:
    if not password:
        return
    key = _password_key(username)
    if not key:
        return
    if _PASSWORD_CACHE.get(key) == password:
        return
    _PASSWORD_CACHE[key] = password
    _configure_password_backend()
    _save_password_cache(_PASSWORD_CACHE)


def _export_accounts_csv(alias: str) -> None:
    accounts = [acct for acct in _load() if acct.get("alias") == alias]
    destination, totp_backup_path = _export_paths(alias)
    include_totp_secret = (
        ask("Â¿Incluir TOTP secret (texto plano) en el CSV? (s/N): ").strip().lower() == "s"
    )
    if include_totp_secret:
        warn(
            "ATENCIÃ“N: el TOTP secret permite generar cÃ³digos 2FA. TratÃ¡ este CSV como altamente sensible."
        )
    headers = [
        "Username",
        "ContraseÃƒÂ±a",
        "CÃƒÂ³digo 2FA",
        "Proxy IP",
        "Proxy Puerto",
        "Proxy Usuario",
        "Proxy ContraseÃƒÂ±a",
        "Estado de la cuenta",
        "Estado del proxy",
    ]
    if include_totp_secret:
        headers.append("TOTP Secret")

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for account in accounts:
        username = (account.get("username") or "").strip()
        badge, _ = _badge_for_display(account)
        account_status = _account_status_from_badge(account, badge)
        proxy_status = _proxy_status_from_badge(badge)
        proxy_ip, proxy_port, proxy_user, proxy_pass = _proxy_components(account)
        row = [
            username,
            _account_password(account),
            _current_totp_code(username),
            proxy_ip,
            proxy_port,
            proxy_user,
            proxy_pass,
            account_status,
            proxy_status,
        ]
        if include_totp_secret:
            try:
                row.append(get_totp_secret(username) or "")
            except Exception:
                row.append("")
        writer.writerow(row)
    atomic_write_text(destination, buffer.getvalue())

    ok(f"Archivo CSV generado en: {destination}")
    totp_written = _export_totp_backup_zip(
        [(acct.get("username") or "").strip() for acct in accounts],
        totp_backup_path,
    )
    if totp_written:
        ok(
            f"Backup TOTP cifrado generado en: {totp_backup_path} "
            f"(registros incluidos: {totp_written})."
        )
        print("Nota: el backup NO incluye la llave `.master_key` por seguridad.")
    press_enter()


def _prompt_destination_alias(current_alias: str) -> Optional[str]:
    items = _load()
    aliases = sorted({(it.get("alias") or "default") for it in items} | {"default"})
    alias_lookup = {alias.lower(): alias for alias in aliases}
    normalized_current = current_alias.lower()
    if normalized_current not in alias_lookup:
        alias_lookup[normalized_current] = current_alias
        aliases.append(current_alias)

    if aliases:
        print("\nAlias disponibles para mover: " + ", ".join(sorted(set(aliases))))

    while True:
        destination = ask("Alias destino (Enter para cancelar): ").strip()
        if not destination:
            return None
        try:
            destination = _validate_user_alias_display(destination, allow_default=True)
        except ValueError as exc:
            warn(str(exc))
            continue

        normalized = destination.lower()
        if normalized == normalized_current:
            warn("El alias destino es el mismo que el origen. SeleccionÃƒÂ¡ otro alias.")
            continue

        if normalized in alias_lookup:
            return alias_lookup[normalized]

        create = (
            ask(
                f"El alias '{destination}' no existe. Ã‚Â¿Crear automÃƒÂ¡ticamente y continuar? (s/N): "
            )
            .strip()
            .lower()
        )
        if create == "s":
            ok(f"Alias '{destination}' creado.")
            return destination


def _import_accounts_from_csv(alias: str) -> None:
    path_input = ask("Ruta del archivo CSV: ").strip()
    if not path_input:
        warn("No se indicÃƒÆ’Ã‚Â³ la ruta del archivo.")
        press_enter()
        return

    try:
        from src.auth.onboarding import parse_accounts_csv
    except Exception as exc:
        warn(f"No se pudo iniciar el parser de CSV: {exc}")
        press_enter()
        return

    try:
        parsed_rows = parse_accounts_csv(path_input)
    except FileNotFoundError:
        warn("El archivo CSV indicado no existe o no es un archivo vÃƒÆ’Ã‚Â¡lido.")
        press_enter()
        return
    except Exception as exc:
        warn(f"No se pudo procesar el CSV: {exc}")
        press_enter()
        return

    if not parsed_rows:
        warn("El archivo CSV no contiene registros vÃƒÆ’Ã‚Â¡lidos.")
        press_enter()
        return

    proxy_rows = [row for row in parsed_rows if (row.get("proxy_url") or "").strip()]
    no_proxy_rows = [row for row in parsed_rows if not (row.get("proxy_url") or "").strip()]
    print(
        "\nDetectadas: {with_proxy} con proxy | {without_proxy} sin proxy".format(
            with_proxy=len(proxy_rows),
            without_proxy=len(no_proxy_rows),
        )
    )

    default_concurrency = max(1, min(int(SETTINGS.max_concurrency or 1), max(1, len(proxy_rows) or 1)))
    requested_concurrency = ask_int(
        f"Concurrencia para login con proxy? [{default_concurrency}]: ",
        min_value=1,
        default=default_concurrency,
    )
    if not proxy_rows and requested_concurrency > 1:
        warn("El CSV no trae proxys: el login se harÃƒÂ¡ 1 a 1 (concurrencia ignorada).")
    elif proxy_rows and requested_concurrency >= 2:
        ok(
            f"Logins con proxy en segundo plano (headless). Concurrencia={requested_concurrency}."
        )
    elif proxy_rows:
        ok("Logins con proxy en modo visible (concurrencia=1).")

    added = 0
    errors: List[tuple[Optional[int], str]] = []
    status_counter: Dict[str, int] = defaultdict(int)
    accounts_to_login: List[Dict[str, Any]] = []

    for row in parsed_rows:
        row_number = row.get("row_number")
        username = (row.get("username") or "").strip().lstrip("@")
        password = (row.get("password") or "").strip()
        totp_value = (row.get("totp_secret") or "").strip()

        if not username or not password:
            errors.append((row_number, "Datos incompletos: username/password."))
            continue

        sticky_value = row.get("proxy_sticky_minutes") or SETTINGS.proxy_sticky_minutes or 10
        try:
            sticky_minutes = max(1, int(sticky_value))
        except Exception:
            sticky_minutes = SETTINGS.proxy_sticky_minutes or 10

        proxy_data = {
            "proxy_url": row.get("proxy_url") or "",
            "proxy_user": row.get("proxy_user") or "",
            "proxy_pass": row.get("proxy_pass") or "",
            "proxy_sticky_minutes": sticky_minutes,
        }

        if not add_account(username, alias, proxy_data):
            errors.append((row_number, "No se pudo agregar la cuenta (posible duplicado)."))
            continue

        if totp_value:
            try:
                save_totp_secret(username, totp_value)
            except ValueError as exc:
                remove_account(username)
                errors.append((row_number, f"2FA invÃƒÆ’Ã‚Â¡lido: {exc}"))
                continue

        _store_account_password(username, password)
        payload = _build_playwright_login_payload(
            username,
            password,
            proxy_data,
            alias=alias,
            row_number=row_number,
        )
        accounts_to_login.append(payload)
        added += 1

    if accounts_to_login:
        results = login_accounts_with_playwright(
            alias,
            accounts_to_login,
        )
        for result in results:
            status = (result.get("status") or "failed").lower()
            status_counter[status] += 1

    total = len(parsed_rows)
    print("\nResumen de importaciÃƒÆ’Ã‚Â³n:")
    print(f"Total de filas procesadas: {total}")
    print(f"Cuentas agregadas al alias: {added}")
    print("Resultados de login guardados en storage/accounts/onboarding_results.csv")
    print(
        "Estados de login -> ok: {ok} | need_code: {need} | failed: {failed}".format(
            ok=status_counter.get("ok", 0),
            need=status_counter.get("need_code", 0),
            failed=status_counter.get("failed", 0),
        )
    )
    if errors:
        warn("Detalle de errores durante el alta:")
        for row_number, message in errors:
            label = f"Fila {row_number}" if row_number else "Fila desconocida"
            print(f" - {label}: {message}")
    press_enter()


def _ask_delay_seconds(default: float = 5.0) -> float:
    prompt = ask(f"Delay entre cuentas en segundos [{default:.0f}]: ").strip()
    if not prompt:
        return max(1.0, default)
    try:
        value = float(prompt.replace(",", "."))
    except ValueError:
        warn("Valor invÃƒÂ¡lido, se utilizarÃƒÂ¡ el delay por defecto.")
        return max(1.0, default)
    return max(1.0, value)


def _rename_password_cache(old_username: str, new_username: str) -> None:
    old_key = _password_key(old_username)
    new_key = _password_key(new_username)
    if not old_key or not new_key or old_key == new_key:
        return
    password = _PASSWORD_CACHE.pop(old_key, None)
    if not password:
        return
    if new_key not in _PASSWORD_CACHE:
        _PASSWORD_CACHE[new_key] = password
    _configure_password_backend()
    _save_password_cache(_PASSWORD_CACHE)


def _rename_playwright_profile(old_username: str, new_username: str) -> None:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not old_clean or not new_clean or old_clean.lower() == new_clean.lower():
        return

    try:
        base = Path(BASE_PROFILES)
    except Exception:
        return

    old_dir = base / old_clean
    new_dir = base / new_clean

    try:
        if not old_dir.exists() or not old_dir.is_dir():
            return
    except Exception:
        return

    try:
        if new_dir.exists():
            # Mejor esfuerzo: si existe el destino, al menos mover storage_state.json si falta.
            old_state = old_dir / "storage_state.json"
            new_state = new_dir / "storage_state.json"
            if old_state.exists():
                should_move = False
                if not new_state.exists():
                    should_move = True
                else:
                    with contextlib.suppress(Exception):
                        should_move = new_state.stat().st_size <= 0
                if should_move:
                    new_dir.mkdir(parents=True, exist_ok=True)
                    old_state.replace(new_state)
            return
    except Exception:
        return

    try:
        old_dir.replace(new_dir)
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo renombrar el perfil de Playwright %s -> %s: %s",
            old_dir,
            new_dir,
            exc,
        )


def _rename_account_record(old_username: str, new_username: str) -> str:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not new_clean:
        return old_clean

    # Evita pisar una cuenta distinta con el mismo username.
    existing = get_account(new_clean)
    if existing:
        existing_username = (existing.get("username") or "").strip().lstrip("@")
        if existing_username and existing_username.lower() != old_clean.lower():
            warn(f"Ya existe una cuenta con username @{existing_username}.")
            return old_clean

    items = _load()
    old_norm = old_clean.lower()
    changed = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored == old_norm:
            updated = dict(item)
            updated["username"] = new_clean
            items[idx] = _normalize_account(updated)
            changed = True
            break

    if changed:
        _save(items)

    if changed and old_clean and new_clean and old_clean.lower() != new_clean.lower():
        try:
            health_store.rename_account(old_clean, new_clean)
        except Exception as exc:
            logger.warning(
                "No se pudo trasladar runtime state de @%s a @%s: %s",
                old_clean,
                new_clean,
                exc,
            )

    try:
        rename_totp_secret(old_clean, new_clean)
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo trasladar el TOTP de @%s a @%s: %s", old_clean, new_clean, exc
        )

    try:
        _rename_password_cache(old_clean, new_clean)
    except Exception:
        pass
    try:
        _rename_playwright_profile(old_clean, new_clean)
    except Exception:
        pass

    return new_clean


def _fallback_playwright_proxy(account: Optional[Dict]) -> Optional[Dict[str, str]]:
    if not account:
        return None
    if _proxy_from_account:
        try:
            payload = _proxy_from_account(account)
        except ProxyResolutionError:
            raise
        except Exception:
            payload = None
        if payload:
            return payload
    raw = str(account.get("proxy_url") or "").strip()
    if not raw:
        return None
    server = raw if "://" in raw else f"http://{raw}"
    payload: Dict[str, str] = {"server": server}
    user = str(account.get("proxy_user") or "").strip()
    password = str(account.get("proxy_pass") or "").strip()
    if user:
        payload["username"] = user
    if password:
        payload["password"] = password
    return payload

def _run_async(coro, *, ignore_stop: bool = False):
    from src.runtime.playwright_runtime import run_coroutine_sync

    return run_coroutine_sync(coro, ignore_stop=ignore_stop)


def _proxy_signature(payload: Optional[Dict[str, str]]) -> str:
    if not payload:
        return ""
    server = str(payload.get("server") or payload.get("url") or "").strip().lower()
    user = str(payload.get("username") or "").strip().lower()
    password = str(payload.get("password") or "").strip()
    return "|".join((server, user, password))


class _ManualPlaywrightLifecycle:
    """Browser lifecycle for manual visible flows.

    - One shared Playwright service for manual UI work.
    - One persistent context per account, reused across sections.
    - Direct navigation to requested URL.
    """

    def __init__(self) -> None:
        self._contexts: Dict[str, Any] = {}
        self._pages: Dict[str, Any] = {}
        self._sessions: Dict[str, ManagedSession] = {}
        self._proxy_keys: Dict[str, str] = {}
        self._inbox_guard_enabled: Dict[str, bool] = {}
        self._last_storage_sync: Dict[str, float] = {}
        self._session_manager = SessionManager(
            headless=False,
            keep_browser_open_per_account=True,
            profiles_root=str(BASE_PROFILES),
            normalize_username=normalize_browser_profile_username,
            log_event=_noop_log_event,
            subsystem="manual",
        )

    _INBOX_READY_SELECTORS: tuple[str, ...] = (
        "a[href='/direct/inbox/']",
        "a[href*='/direct/inbox/']",
        "a[href*='/direct/t/']",
        "div[role='navigation'] a[href*='/direct/']",
        "input[placeholder='Search']",
        "input[placeholder='Buscar']",
        "input[name='queryBox']",
        "svg[aria-label='Direct']",
        "svg[aria-label='Mensajes']",
    )
    _LOGIN_FORM_SELECTORS: tuple[str, ...] = (
        "input[name='username']",
        "input[name='email']",
        "input[autocomplete='username']",
        "input[name='password']",
        "input[type='password']",
        "form[action*='login']",
    )
    # Visible manual sessions already persist state on open/close and run with
    # a persistent profile. Avoid periodic snapshots here because Chromium can
    # briefly surface transient tabs during ctx.storage_state() in this mode.
    _BACKGROUND_STORAGE_SYNC_ENABLED = False
    _STORAGE_SYNC_INTERVAL_SECONDS = 2.0

    @staticmethod
    def _context_alive(ctx: Any) -> bool:
        if ctx is None:
            return False
        browser = getattr(ctx, "browser", None)
        if browser is not None:
            connected = getattr(browser, "is_connected", None)
            if callable(connected):
                with contextlib.suppress(Exception):
                    if not bool(connected()):
                        return False
        checker = getattr(ctx, "is_closed", None)
        if callable(checker):
            with contextlib.suppress(Exception):
                return not bool(checker())
        with contextlib.suppress(Exception):
            _ = list(ctx.pages)
            return True
        return False

    @staticmethod
    def _page_alive(page: Any) -> bool:
        if page is None:
            return False
        checker = getattr(page, "is_closed", None)
        if callable(checker):
            with contextlib.suppress(Exception):
                return not bool(checker())
        return True

    @staticmethod
    def _is_closed_target_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        markers = (
            "target page, context or browser has been closed",
            "browser has been closed",
            "context has been closed",
            "page has been closed",
        )
        return any(marker in text for marker in markers)

    async def _close_context(self, username: str) -> None:
        key = username.lower()
        with contextlib.suppress(Exception):
            await self._persist_storage_state(username, force=True)
        self._contexts.pop(key, None)
        self._pages.pop(key, None)
        self._sessions.pop(key, None)
        self._proxy_keys.pop(key, None)
        self._inbox_guard_enabled.pop(key, None)
        self._last_storage_sync.pop(key, None)
        with contextlib.suppress(Exception):
            await self._session_manager.drop_cached_session(key)

    @staticmethod
    def _storage_state_path(username: str) -> Path:
        return playwright_storage_state_path(username)

    async def _persist_storage_state(self, username: str, *, force: bool = False) -> None:
        key = username.lower()
        if not key:
            return
        now = time.monotonic()
        last_sync = float(self._last_storage_sync.get(key, 0.0) or 0.0)
        if not force and (now - last_sync) < self._STORAGE_SYNC_INTERVAL_SECONDS:
            return
        session = self._sessions.get(key)
        storage_state = self._storage_state_path(username)
        storage_state.parent.mkdir(parents=True, exist_ok=True)
        saved = False
        if session is not None and self._context_alive(getattr(session, "ctx", None)):
            with contextlib.suppress(Exception):
                await self._session_manager.save_storage_state(session, username)
                saved = True
        if saved:
            self._last_storage_sync[key] = time.monotonic()

    async def _storage_state_sync_loop(self, *, username: str, stop_event: asyncio.Event) -> None:
        key = username.lower()
        while not stop_event.is_set():
            await asyncio.sleep(self._STORAGE_SYNC_INTERVAL_SECONDS)
            if stop_event.is_set():
                break
            if not self._context_alive(self._contexts.get(key)):
                break
            await self._persist_storage_state(username)

    async def _ensure_context(
        self,
        *,
        account: Dict[str, Any],
        username: str,
        proxy_payload: Optional[Dict[str, str]],
    ) -> Any:
        key = username.lower()
        desired_proxy_key = _proxy_signature(proxy_payload)
        current_proxy_key = self._proxy_keys.get(key, "")
        ctx = self._contexts.get(key)
        if ctx is not None and (not self._context_alive(ctx) or current_proxy_key != desired_proxy_key):
            await self._close_context(username)
            ctx = None

        if ctx is None:
            manual_account = dict(account or {})
            manual_account["manual_visible_browser"] = True
            session = await self._session_manager.open_session(
                account=manual_account,
                proxy=proxy_payload,
                login_func=ensure_logged_in_async,
            )
            ctx = session.ctx
            self._sessions[key] = session
            self._contexts[key] = ctx
            self._pages[key] = session.page
            self._proxy_keys[key] = desired_proxy_key
            self._inbox_guard_enabled[key] = False
            print(f"[Manual Browser] context created @{username}", flush=True)
        else:
            print(f"[Manual Browser] context reused @{username}", flush=True)
        return ctx

    async def _ensure_page(self, *, username: str, ctx: Any) -> Any:
        key = username.lower()
        page = self._pages.get(key)
        if page is not None:
            same_ctx = False
            with contextlib.suppress(Exception):
                same_ctx = page.context == ctx
            if not self._page_alive(page) or not same_ctx:
                page = None

        if page is None:
            pages: list[Any] = []
            with contextlib.suppress(Exception):
                pages = list(ctx.pages)
            if pages:
                page = pages[-1]
            else:
                page = await ctx.new_page()
            self._pages[key] = page
            print(f"[Manual Browser] page created @{username}", flush=True)
        else:
            print(f"[Manual Browser] page reused @{username}", flush=True)
        return page

    async def _first_visible_selector(
        self,
        page: Any,
        selectors: tuple[str, ...],
    ) -> Optional[str]:
        for selector in selectors:
            with contextlib.suppress(Exception):
                if await page.locator(selector).count():
                    return selector
        return None

    async def _ensure_inbox_login_sync_guard(self, *, username: str, ctx: Any) -> None:
        key = username.lower()
        if self._inbox_guard_enabled.get(key):
            return

        async def _block_login_sync(route: Any) -> None:
            request = None
            with contextlib.suppress(Exception):
                request = route.request
            url = ""
            with contextlib.suppress(Exception):
                url = str(request.url or "")
            print(
                f"[Manual Browser] blocked external redirect @{username} -> {url or 'facebook_login_sync'}",
                flush=True,
            )
            with contextlib.suppress(Exception):
                await route.abort()

        patterns = (
            "**://www.facebook.com/instagram/login_sync/**",
            "**://www.facebook.com/instagram/login_sync/*",
            "**://m.facebook.com/instagram/login_sync/**",
            "**://m.facebook.com/instagram/login_sync/*",
            "**://*.facebook.com/instagram/login_sync/**",
            "**://*.facebook.com/instagram/login_sync/*",
        )
        for pattern in patterns:
            with contextlib.suppress(Exception):
                await ctx.route(pattern, _block_login_sync)
        self._inbox_guard_enabled[key] = True
        print(f"[Manual Browser] login_sync guard enabled @{username}", flush=True)

    async def _classify_manual_surface(
        self,
        *,
        page: Any,
        start_url: str,
    ) -> tuple[str, str]:
        try:
            from src.health_playwright import detect_account_health_async

            status, reason = await detect_account_health_async(page, timeout_ms=8_000)
            normalized_status = str(status or "").strip().upper()
            normalized_reason = str(reason or "").strip() or "health_probe"
            if normalized_status == health_store.HEALTH_STATE_ALIVE:
                return "inbox_ready", normalized_reason
            if normalized_status == health_store.HEALTH_STATE_INACTIVE:
                return "login_required", normalized_reason
            if normalized_status == health_store.HEALTH_STATE_DEAD:
                return "blocked", normalized_reason
            return "unknown", normalized_reason
        except Exception:
            pass

        url = (page.url or "").lower()
        inbox_selector = await self._first_visible_selector(page, self._INBOX_READY_SELECTORS)
        if "/direct/inbox" in url or inbox_selector:
            return "inbox_ready", inbox_selector or "inbox_url"
        login_selector = await self._first_visible_selector(page, self._LOGIN_FORM_SELECTORS)
        if "/accounts/login" in url or "/accounts/onetap" in url or login_selector:
            return "login_required", login_selector or "login_url"
        if "/direct/inbox" in (start_url or "").lower():
            raise RuntimeError("inbox_surface_unknown: no inbox DOM y no login form")
        return "unknown", "surface_not_classified"

    async def _update_health_from_surface(
        self,
        *,
        username: str,
        surface: str,
        reason: str,
        start_url: str,
    ) -> None:
        if "/direct/" not in (start_url or "").lower():
            return
        with contextlib.suppress(Exception):
            if surface == "inbox_ready":
                health_store.update_from_playwright_status(
                    username,
                    health_store.HEALTH_STATE_ALIVE,
                    reason=reason,
                )
            elif surface == "login_required":
                health_store.update_from_playwright_status(
                    username,
                    health_store.HEALTH_STATE_INACTIVE,
                    reason=reason,
                )
                with contextlib.suppress(Exception):
                    from src.telemetry import report_session_expired

                    report_session_expired(
                        f"Sesion expirada para @{username}",
                        payload={"username": username, "reason": reason},
                    )
            elif surface == "blocked":
                health_store.update_from_playwright_status(
                    username,
                    health_store.HEALTH_STATE_DEAD,
                    reason=reason,
                )

    async def _wait_until_manual_end(
        self,
        *,
        username: str,
        ctx: Any,
        start_url: str,
        max_seconds: Optional[int],
        restore_page_if_closed: bool,
    ) -> None:
        key = username.lower()
        started = time.monotonic()
        timeout_seconds = float(max_seconds) if (max_seconds and max_seconds > 0) else None

        while True:
            if _manual_close_requested(username):
                print(f"[Manual Browser] close requested @{username}", flush=True)
                await self._close_context(username)
                _clear_manual_close_request(username)
                return
            if not self._context_alive(ctx):
                print(f"[Manual Browser] context closed @{username}", flush=True)
                await self._close_context(username)
                return

            pages: list[Any] = []
            with contextlib.suppress(Exception):
                pages = list(ctx.pages)
            live_pages = [item for item in pages if self._page_alive(item)]
            if not live_pages:
                if not restore_page_if_closed:
                    print(f"[Manual Browser] no live pages @{username}; closing session", flush=True)
                    await self._close_context(username)
                    return
                # Some surfaces (e.g. login_sync) can close the active tab.
                # Restore one tab and keep waiting for the user to close the browser.
                with contextlib.suppress(Exception):
                    restored = await ctx.new_page()
                    self._pages[key] = restored
                    if start_url:
                        await restored.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
                    print(f"[Manual Browser] active page restored @{username}", flush=True)
                    live_pages = [restored]
            if not live_pages:
                await self._close_context(username)
                return

            active_page = live_pages[-1]
            self._pages[key] = active_page

            remaining: Optional[float]
            if timeout_seconds is None:
                remaining = None
            else:
                elapsed = time.monotonic() - started
                remaining = max(0.0, timeout_seconds - elapsed)
                if remaining <= 0.0:
                    print("\n[PLAYWRIGHT] Tiempo cumplido. Cerrando navegador...", flush=True)
                    await self._close_context(username)
                    return

            page_closed_task = asyncio.create_task(active_page.wait_for_event("close", timeout=0))
            ctx_closed_task = asyncio.create_task(ctx.wait_for_event("close", timeout=0))
            done: set[asyncio.Task[Any]]
            pending: set[asyncio.Task[Any]]
            done, pending = await asyncio.wait(
                {page_closed_task, ctx_closed_task},
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*pending, return_exceptions=True)

            if not done:
                print("\n[PLAYWRIGHT] Tiempo cumplido. Cerrando navegador...", flush=True)
                await self._close_context(username)
                return

            ctx_done_ok = False
            page_done_ok = False
            if ctx_closed_task in done:
                with contextlib.suppress(Exception):
                    _ = ctx_closed_task.result()
                    ctx_done_ok = True
            if page_closed_task in done:
                with contextlib.suppress(Exception):
                    _ = page_closed_task.result()
                    page_done_ok = True

            if ctx_done_ok:
                open_urls: list[str] = []
                with contextlib.suppress(Exception):
                    open_urls = [str(getattr(item, "url", "") or "") for item in list(ctx.pages)]
                if open_urls:
                    print(
                        f"[Manual Browser] browser/context closed @{username} open_urls={open_urls}",
                        flush=True,
                    )
                else:
                    print(f"[Manual Browser] browser/context closed @{username}", flush=True)
                await self._close_context(username)
                return

            if not page_done_ok and not ctx_done_ok:
                # Event wait can fail for transport reasons; keep waiting while context is alive.
                print(f"[Manual Browser] close-event wait interrupted @{username}", flush=True)
                continue

            # Active page closed, but browser can still be open.
            # Keep session alive until context/browser is explicitly closed.
            print(f"[Manual Browser] active page closed @{username} (waiting browser close)", flush=True)
            self._pages.pop(key, None)

    async def open_manual_session(
        self,
        *,
        account: Dict,
        start_url: str,
        action_label: str,
        max_seconds: Optional[int],
        restore_page_if_closed: bool = True,
    ) -> Dict[str, Any]:
        username = str(account.get("username") or "").strip().lstrip("@")
        if not username:
            raise RuntimeError("Cuenta invalida (sin username).")

        proxy_payload = None
        with contextlib.suppress(Exception):
            proxy_payload = _playwright_proxy_payload(account)
        if not proxy_payload:
            proxy_payload = _fallback_playwright_proxy(account)

        storage_state_dir = browser_storage_state_path(username, profiles_root=BASE_PROFILES).parent
        storage_state_dir.mkdir(parents=True, exist_ok=True)

        log_browser_stage(
            component="manual_account_action",
            stage="session_open_start",
            status="started",
            account=username,
            action=action_label,
            start_url=start_url,
        )

        try:
            ctx = await self._ensure_context(
                account=account,
                username=username,
                proxy_payload=proxy_payload,
            )
            try:
                page = await self._ensure_page(username=username, ctx=ctx)
            except Exception as exc:
                if not self._is_closed_target_error(exc):
                    raise
                await self._close_context(username)
                ctx = await self._ensure_context(
                    account=account,
                    username=username,
                    proxy_payload=proxy_payload,
                )
                page = await self._ensure_page(username=username, ctx=ctx)
        except Exception as exc:
            log_browser_stage(
                component="manual_account_action",
                stage="session_open_end",
                status="failed",
                account=username,
                action=action_label,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
            )
            raise RuntimeError(f"Manual browser launch failed for @{username}: {exc}") from exc

        log_browser_stage(
            component="manual_account_action",
            stage="session_open_end",
            status="ok",
            account=username,
            action=action_label,
        )
        log_browser_stage(
            component="manual_account_action",
            stage="browser_open",
            status="ok",
            account=username,
            action=action_label,
        )
        if _manual_close_requested(username):
            print(f"[Manual Browser] close requested before navigation @{username}", flush=True)
            await self._close_context(username)
            _clear_manual_close_request(username)
            return {
                "opened": True,
                "username": username,
                "action": action_label,
                "start_url": start_url,
                "current_url": "",
            }
        storage_sync_stop: asyncio.Event | None = None
        storage_sync_task: asyncio.Task[Any] | None = None
        if self._BACKGROUND_STORAGE_SYNC_ENABLED:
            storage_sync_stop = asyncio.Event()
            storage_sync_task = asyncio.create_task(
                self._storage_state_sync_loop(username=username, stop_event=storage_sync_stop)
            )
        try:
            if start_url:
                if "/direct/inbox" in (start_url or "").lower():
                    await self._ensure_inbox_login_sync_guard(username=username, ctx=ctx)
                await page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
                print(f"[Manual Browser] direct navigation @{username} -> {start_url}", flush=True)

            surface, reason = await self._classify_manual_surface(page=page, start_url=start_url)
            await self._update_health_from_surface(
                username=username,
                surface=surface,
                reason=reason,
                start_url=start_url,
            )
            log_browser_stage(
                component="manual_account_action",
                stage="workspace_ready",
                status="ok",
                account=username,
                action=action_label,
                surface=surface,
                reason=reason,
                url=str(getattr(page, "url", "") or ""),
            )
            if surface == "login_required":
                print(
                    f"[Manual Browser] @{username} requiere re-login manual (surface={reason})",
                    flush=True,
                )
            elif surface == "blocked":
                print(
                    f"[Manual Browser] @{username} marcada como BLOQUEADA (surface={reason})",
                    flush=True,
                )

            await self._persist_storage_state(username, force=True)

            print(f"\n[PLAYWRIGHT] @{username} -> {action_label}", flush=True)
            if max_seconds and max_seconds > 0:
                mins = max(1, int((max_seconds + 59) // 60))
                print(
                    f"Se cerrarÃ¡ automÃ¡ticamente en ~{mins} min (o cerrÃ¡ el navegador antes para continuar).",
                    flush=True,
                )
            else:
                print("UsÃ¡ el navegador manualmente y cerralo para continuar.", flush=True)

            await self._wait_until_manual_end(
                username=username,
                ctx=ctx,
                start_url=start_url,
                max_seconds=max_seconds,
                restore_page_if_closed=restore_page_if_closed,
            )
        finally:
            if storage_sync_stop is not None:
                storage_sync_stop.set()
            if storage_sync_task is not None:
                with contextlib.suppress(Exception):
                    await storage_sync_task

        return {
            "opened": True,
            "username": username,
            "action": action_label,
            "start_url": start_url,
            "current_url": str(getattr(page, "url", "") or ""),
        }

    async def close_manual_session(self, username: str) -> bool:
        key = str(username or "").strip().lstrip("@").lower()
        if not key:
            return False
        should_close = any(key in registry for registry in (self._contexts, self._pages, self._sessions))
        if should_close:
            await self._close_context(username)
        return should_close

    async def shutdown(self) -> None:
        for username in list(self._contexts.keys()):
            await self._close_context(username)
        self._session_manager.close_all_sessions_sync(timeout=10.0)
        self._contexts.clear()
        self._pages.clear()
        self._sessions.clear()
        self._proxy_keys.clear()


_MANUAL_PLAYWRIGHT_LIFECYCLE: Optional[_ManualPlaywrightLifecycle] = None
_MANUAL_PLAYWRIGHT_LOCK = Lock()
_MANUAL_PLAYWRIGHT_PENDING_CLOSE: set[str] = set()


def _manual_close_request_key(username: str) -> str:
    return str(username or "").strip().lstrip("@").lower()


def _manual_close_requested(username: str) -> bool:
    key = _manual_close_request_key(username)
    if not key:
        return False
    with _MANUAL_PLAYWRIGHT_LOCK:
        return key in _MANUAL_PLAYWRIGHT_PENDING_CLOSE


def _request_manual_close(username: str) -> None:
    key = _manual_close_request_key(username)
    if not key:
        return
    with _MANUAL_PLAYWRIGHT_LOCK:
        _MANUAL_PLAYWRIGHT_PENDING_CLOSE.add(key)


def _clear_manual_close_request(username: str) -> None:
    key = _manual_close_request_key(username)
    if not key:
        return
    with _MANUAL_PLAYWRIGHT_LOCK:
        _MANUAL_PLAYWRIGHT_PENDING_CLOSE.discard(key)


def _manual_lifecycle() -> _ManualPlaywrightLifecycle:
    global _MANUAL_PLAYWRIGHT_LIFECYCLE
    with _MANUAL_PLAYWRIGHT_LOCK:
        if _MANUAL_PLAYWRIGHT_LIFECYCLE is None:
            _MANUAL_PLAYWRIGHT_LIFECYCLE = _ManualPlaywrightLifecycle()
        return _MANUAL_PLAYWRIGHT_LIFECYCLE


def shutdown_manual_playwright_sessions() -> None:
    global _MANUAL_PLAYWRIGHT_LIFECYCLE
    lifecycle = _MANUAL_PLAYWRIGHT_LIFECYCLE
    if lifecycle is None:
        with _MANUAL_PLAYWRIGHT_LOCK:
            _MANUAL_PLAYWRIGHT_PENDING_CLOSE.clear()
        return
    with _MANUAL_PLAYWRIGHT_LOCK:
        _MANUAL_PLAYWRIGHT_LIFECYCLE = None
        _MANUAL_PLAYWRIGHT_PENDING_CLOSE.clear()
    with contextlib.suppress(Exception):
        _run_async(lifecycle.shutdown())


def clear_manual_playwright_session_close_request(username: str) -> None:
    _clear_manual_close_request(username)


def close_manual_playwright_session(username: str) -> bool:
    clean_username = str(username or "").strip().lstrip("@")
    if not clean_username:
        return False
    _request_manual_close(clean_username)
    with _MANUAL_PLAYWRIGHT_LOCK:
        lifecycle = _MANUAL_PLAYWRIGHT_LIFECYCLE
    if lifecycle is None:
        return False
    with contextlib.suppress(Exception):
        closed = bool(_run_async(lifecycle.close_manual_session(clean_username), ignore_stop=True))
        if closed:
            _clear_manual_close_request(clean_username)
        return closed
    return False


def _open_playwright_manual_session(
    account: Dict,
    *,
    start_url: str,
    action_label: str,
    max_seconds: Optional[int] = None,
    restore_page_if_closed: bool = True,
) -> Dict[str, Any]:
    """
    Abre un navegador Playwright headful para que el usuario haga cambios MANUALES.
    Reutiliza storage_state.json si existe y lo re-guardara al cerrar la ventana.

    No hay timeout por defecto: el flujo vuelve recien cuando el usuario cierra el navegador.
    Si max_seconds > 0, se cerrarÃ¡ automÃ¡ticamente al cumplirse ese tiempo.
    """

    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        raise RuntimeError("Cuenta invalida (sin username).")
    preflight = account_proxy_preflight(account)
    if bool(preflight.get("blocking")):
        detail = str(preflight.get("message") or "Proxy no disponible para esta cuenta.").strip()
        raise RuntimeError(detail or "Proxy no disponible para esta cuenta.")

    log_browser_stage(
        component="manual_account_action",
        stage="spawn",
        status="started",
        account=username,
        action=action_label,
        start_url=start_url,
    )

    async def _runner() -> Dict[str, Any]:
        lifecycle = _manual_lifecycle()
        return await lifecycle.open_manual_session(
            account=account,
            start_url=start_url,
            action_label=action_label,
            max_seconds=max_seconds,
            restore_page_if_closed=restore_page_if_closed,
        )

    try:
        return _run_async(_runner(), ignore_stop=True)
    except Exception as exc:
        raise RuntimeError(f"Failed to open manual browser for @{username}: {exc}") from exc


# Mantener compatibilidad con importaciÃƒÂ³n dinÃƒÂ¡mica
mark_connected.__doc__ = "Actualiza el flag de conexiÃƒÂ³n en almacenamiento"

