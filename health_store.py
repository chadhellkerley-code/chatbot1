"""
Canonical account runtime state persisted in SQLite.

This module keeps backward-compatible health APIs while also storing:
- connection state
- login queue/progress state

Legacy `storage/accounts/account_health.json` data is migrated once into SQLite.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Iterable, Optional, Tuple

from core.storage_atomic import load_json_file
from paths import accounts_root, runtime_base

logger = logging.getLogger(__name__)

BASE = runtime_base(Path(__file__).resolve().parent)
DATA_DIR = accounts_root(BASE)
HEALTH_FILE = DATA_DIR / "account_health.json"
DB_FILE = DATA_DIR / "account_runtime_state.sqlite3"
LEGACY_HEALTH_BACKUP_FILE = DATA_DIR / "account_health.legacy.json"

HEALTH_STATE_ALIVE = "VIVA"
HEALTH_STATE_INACTIVE = "NO ACTIVA"
HEALTH_STATE_DEAD = "MUERTA"
VALID_HEALTH_STATES = {
    HEALTH_STATE_ALIVE,
    HEALTH_STATE_INACTIVE,
    HEALTH_STATE_DEAD,
}

LOGIN_PROGRESS_QUEUED = "queued"
LOGIN_PROGRESS_OPENING_BROWSER = "opening_browser"
LOGIN_PROGRESS_RUNNING_LOGIN = "running_login"
LOGIN_PROGRESS_CONFIRMING_FEED = "confirming_feed"
LOGIN_PROGRESS_CONFIRMING_INBOX = "confirming_inbox"
VALID_LOGIN_PROGRESS_STATES = {
    LOGIN_PROGRESS_QUEUED,
    LOGIN_PROGRESS_OPENING_BROWSER,
    LOGIN_PROGRESS_RUNNING_LOGIN,
    LOGIN_PROGRESS_CONFIRMING_FEED,
    LOGIN_PROGRESS_CONFIRMING_INBOX,
}

_TTL = timedelta(minutes=15)
_LOCK = RLock()


@dataclass(frozen=True)
class AccountHealthRecord:
    state: str
    reason: str
    timestamp: datetime


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _key(username: str) -> str:
    return (username or "").strip().lstrip("@").lower()


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return _as_utc(datetime.fromisoformat(normalized))
    except Exception:
        return None


def _now_iso() -> str:
    return _utcnow().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_FILE, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("pragma journal_mode = wal")
    connection.execute("pragma synchronous = normal")
    return connection


def _meta_value_locked(connection: sqlite3.Connection, key: str) -> str:
    row = connection.execute(
        "select value from account_runtime_meta where key = ?",
        (str(key or "").strip(),),
    ).fetchone()
    if row is None:
        return ""
    return str(row["value"] or "")


def _set_meta_locked(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        """
        insert into account_runtime_meta(key, value)
        values(?, ?)
        on conflict(key) do update set value = excluded.value
        """,
        (str(key or "").strip(), str(value or "").strip()),
    )


def _state_from_reason(value: str) -> str | None:
    normalized = str(value or "").strip().casefold()
    if not normalized:
        return None
    if normalized in {"alive", "healthy", "inbox_accessible", "workspace_ready", "viva"}:
        return HEALTH_STATE_ALIVE
    if normalized in {
        "session_expired",
        "login_required",
        "redirected_to_login",
        "login_form",
        "storage_state_missing",
        "storage_state_invalid",
        "inactive",
        "no_activa",
        "no activa",
    }:
        return HEALTH_STATE_INACTIVE
    if normalized in {
        "checkpoint",
        "challenge",
        "captcha",
        "blocked",
        "suspended",
        "disabled",
        "verification_required",
        "confirm_email",
        "two_factor",
        "dead",
        "muerta",
    }:
        return HEALTH_STATE_DEAD
    return None


def _state_from_legacy_badge(badge: str) -> tuple[str | None, str]:
    text = str(badge or "").strip()
    lowered = text.casefold()
    if not lowered:
        return None, ""
    if any(token in lowered for token in ("sin chequeo", "sin datos", "desconoc", "unknown")):
        return None, ""
    if "viva" in lowered or "ok" in lowered:
        return HEALTH_STATE_ALIVE, "legacy_badge"
    if any(
        token in lowered
        for token in (
            "no activa",
            "sin sesion",
            "sin sesion",
            "sesion expirada",
            "sesion expirada",
            "login",
        )
    ):
        return HEALTH_STATE_INACTIVE, "legacy_badge"
    if any(
        token in lowered
        for token in (
            "muerta",
            "bloqueada",
            "blocked",
            "checkpoint",
            "challenge",
            "captcha",
            "suspend",
            "disabled",
            "verificacion",
            "verificacion",
        )
    ):
        return HEALTH_STATE_DEAD, "legacy_badge"
    return None, ""


def _coerce_state(value: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    upper = text.upper()
    if upper in VALID_HEALTH_STATES:
        return upper
    mapped = _state_from_reason(text)
    if mapped:
        return mapped
    legacy_state, _legacy_reason = _state_from_legacy_badge(text)
    return legacy_state


def _normalize_progress_state(value: str) -> str:
    text = str(value or "").strip().lower()
    if text not in VALID_LOGIN_PROGRESS_STATES:
        raise ValueError(f"Unsupported login progress state: {value!r}")
    return text


def _migrate_legacy_health_file_locked(connection: sqlite3.Connection) -> None:
    if _meta_value_locked(connection, "legacy_health_json_migrated") == "1":
        return
    if connection.execute("select 1 from account_health_state limit 1").fetchone() is not None:
        _set_meta_locked(connection, "legacy_health_json_migrated", "1")
        return
    if not HEALTH_FILE.exists():
        _set_meta_locked(connection, "legacy_health_json_migrated", "1")
        return
    try:
        raw = load_json_file(HEALTH_FILE, {}, label="health_store")
    except Exception:
        _set_meta_locked(connection, "legacy_health_json_migrated", "1")
        return
    if not isinstance(raw, dict):
        _set_meta_locked(connection, "legacy_health_json_migrated", "1")
        return

    for username, entry in raw.items():
        if not isinstance(username, str) or not isinstance(entry, dict):
            continue
        timestamp = _parse_timestamp(entry.get("timestamp"))
        if timestamp is None:
            continue
        state = _coerce_state(str(entry.get("state") or ""))
        reason = str(entry.get("reason") or "").strip()
        if state is None:
            state, legacy_reason = _state_from_legacy_badge(str(entry.get("badge") or ""))
            if not reason and legacy_reason:
                reason = legacy_reason
        if state is None:
            continue
        connection.execute(
            """
            insert into account_health_state(username, state, reason, updated_at)
            values(?, ?, ?, ?)
            on conflict(username) do update set
                state = excluded.state,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (_key(username), state, reason, timestamp.isoformat()),
        )

    _set_meta_locked(connection, "legacy_health_json_migrated", "1")
    with contextlib.suppress(Exception):
        if HEALTH_FILE.exists():
            if LEGACY_HEALTH_BACKUP_FILE.exists():
                HEALTH_FILE.unlink()
            else:
                HEALTH_FILE.replace(LEGACY_HEALTH_BACKUP_FILE)


def _ensure_schema() -> None:
    with _LOCK, _connect() as connection:
        connection.executescript(
            """
            create table if not exists account_runtime_meta (
                key text primary key,
                value text not null
            );

            create table if not exists account_health_state (
                username text primary key,
                state text not null,
                reason text not null default '',
                updated_at text not null
            );

            create table if not exists account_session_state (
                username text primary key,
                connected integer not null,
                source text not null default '',
                reason text not null default '',
                updated_at text not null
            );

            create table if not exists account_login_progress (
                username text primary key,
                run_id text not null default '',
                state text not null,
                message text not null default '',
                updated_at text not null
            );
            """
        )
        _migrate_legacy_health_file_locked(connection)


_ensure_schema()


def get_record(username: str) -> Tuple[Optional[AccountHealthRecord], bool]:
    key = _key(username)
    if not key:
        return None, True
    with _LOCK, _connect() as connection:
        row = connection.execute(
            """
            select state, reason, updated_at
            from account_health_state
            where username = ?
            """,
            (key,),
        ).fetchone()
    if row is None:
        return None, True
    timestamp = _parse_timestamp(row["updated_at"])
    if timestamp is None:
        return None, True
    record = AccountHealthRecord(
        state=str(row["state"] or "").strip(),
        reason=str(row["reason"] or "").strip(),
        timestamp=timestamp,
    )
    expired = (_utcnow() - record.timestamp) >= _TTL
    return record, expired


def get_state(username: str) -> Tuple[Optional[str], bool]:
    record, expired = get_record(username)
    return (record.state if record is not None else None), expired


def get_reason(username: str) -> Tuple[str, bool]:
    record, expired = get_record(username)
    return (record.reason if record is not None else ""), expired


def get_badge(username: str) -> Tuple[Optional[str], bool]:
    return get_state(username)


def set_state(
    username: str,
    state: str,
    *,
    reason: str = "",
    source: str = "",
) -> str:
    normalized = _coerce_state(state)
    if normalized is None:
        raise ValueError(f"Unsupported account health state: {state!r}")
    key = _key(username)
    if not key:
        return normalized
    record_reason = str(reason or source or "").strip()
    with _LOCK, _connect() as connection:
        connection.execute(
            """
            insert into account_health_state(username, state, reason, updated_at)
            values(?, ?, ?, ?)
            on conflict(username) do update set
                state = excluded.state,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (key, normalized, record_reason, _now_iso()),
        )
    _log_health(key, normalized, record_reason)
    return normalized


def set_badge(username: str, badge: str) -> str:
    normalized = _coerce_state(badge)
    if normalized is None:
        raise ValueError(f"Unsupported legacy account health badge: {badge!r}")
    return set_state(username, normalized, reason="legacy_set_badge")


def invalidate(username: str) -> None:
    key = _key(username)
    if not key:
        return
    with _LOCK, _connect() as connection:
        connection.execute(
            "delete from account_health_state where username = ?",
            (key,),
        )


def _log_health(username: str, state: str, reason: str = "") -> None:
    account = (username or "").strip().lstrip("@")
    if not account:
        return
    if reason:
        logger.info("[ACCOUNT HEALTH] account=@%s state=%s reason=%s", account, state, reason)
        return
    logger.info("[ACCOUNT HEALTH] account=@%s state=%s", account, state)


def mark_alive(username: str, *, reason: str = "") -> str:
    return set_state(username, HEALTH_STATE_ALIVE, reason=reason)


def mark_session_expired(username: str, *, reason: str = "session_expired") -> str:
    return set_state(username, HEALTH_STATE_INACTIVE, reason=reason)


def mark_blocked(username: str, *, reason: str) -> str:
    return set_state(username, HEALTH_STATE_DEAD, reason=reason)


def update_from_playwright_status(username: str, status: str, *, reason: str = "") -> str:
    normalized = _coerce_state(status)
    if normalized is None:
        raise ValueError(f"Unsupported Playwright account health result: {status!r}")
    return set_state(username, normalized, reason=reason or status)


def is_usable_state(state: str) -> bool:
    return _coerce_state(state) == HEALTH_STATE_ALIVE


def is_dead_state(state: str) -> bool:
    return _coerce_state(state) == HEALTH_STATE_DEAD


def blocks_automation(state: str) -> bool:
    normalized = _coerce_state(state)
    return normalized in {HEALTH_STATE_INACTIVE, HEALTH_STATE_DEAD}


def set_connected(
    username: str,
    connected: bool,
    *,
    source: str = "",
    reason: str = "",
) -> bool:
    key = _key(username)
    if not key:
        return bool(connected)
    with _LOCK, _connect() as connection:
        connection.execute(
            """
            insert into account_session_state(username, connected, source, reason, updated_at)
            values(?, ?, ?, ?, ?)
            on conflict(username) do update set
                connected = excluded.connected,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (
                key,
                1 if bool(connected) else 0,
                str(source or "").strip(),
                str(reason or "").strip(),
                _now_iso(),
            ),
        )
    return bool(connected)


def get_connected(username: str) -> tuple[Optional[bool], str, str]:
    key = _key(username)
    if not key:
        return None, "", ""
    with _LOCK, _connect() as connection:
        row = connection.execute(
            """
            select connected, source, reason
            from account_session_state
            where username = ?
            """,
            (key,),
        ).fetchone()
    if row is None:
        return None, "", ""
    return bool(int(row["connected"] or 0)), str(row["source"] or ""), str(row["reason"] or "")


def set_login_progress(
    username: str,
    state: str,
    *,
    run_id: str = "",
    message: str = "",
) -> str:
    key = _key(username)
    normalized = _normalize_progress_state(state)
    if not key:
        return normalized
    with _LOCK, _connect() as connection:
        connection.execute(
            """
            insert into account_login_progress(username, run_id, state, message, updated_at)
            values(?, ?, ?, ?, ?)
            on conflict(username) do update set
                run_id = excluded.run_id,
                state = excluded.state,
                message = excluded.message,
                updated_at = excluded.updated_at
            """,
            (
                key,
                str(run_id or "").strip(),
                normalized,
                str(message or "").strip(),
                _now_iso(),
            ),
        )
    return normalized


def get_login_progress(username: str) -> dict[str, str | bool]:
    key = _key(username)
    if not key:
        return {"active": False, "state": "", "message": "", "run_id": "", "updated_at": ""}
    with _LOCK, _connect() as connection:
        row = connection.execute(
            """
            select run_id, state, message, updated_at
            from account_login_progress
            where username = ?
            """,
            (key,),
        ).fetchone()
    if row is None:
        return {"active": False, "state": "", "message": "", "run_id": "", "updated_at": ""}
    return {
        "active": True,
        "state": str(row["state"] or ""),
        "message": str(row["message"] or ""),
        "run_id": str(row["run_id"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def list_login_progress(usernames: Iterable[str] | None = None) -> dict[str, dict[str, str | bool]]:
    keys = [_key(username) for username in (usernames or []) if _key(username)]
    with _LOCK, _connect() as connection:
        if keys:
            placeholders = ", ".join("?" for _ in keys)
            rows = connection.execute(
                f"""
                select username, run_id, state, message, updated_at
                from account_login_progress
                where username in ({placeholders})
                """,
                tuple(keys),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                select username, run_id, state, message, updated_at
                from account_login_progress
                """
            ).fetchall()
    payload: dict[str, dict[str, str | bool]] = {}
    for row in rows:
        username = str(row["username"] or "").strip()
        if not username:
            continue
        payload[username] = {
            "active": True,
            "state": str(row["state"] or ""),
            "message": str(row["message"] or ""),
            "run_id": str(row["run_id"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
    return payload


def clear_login_progress(username: str) -> None:
    key = _key(username)
    if not key:
        return
    with _LOCK, _connect() as connection:
        connection.execute(
            "delete from account_login_progress where username = ?",
            (key,),
        )


def clear_login_progress_many(usernames: Iterable[str]) -> None:
    keys = [_key(username) for username in usernames if _key(username)]
    if not keys:
        return
    placeholders = ", ".join("?" for _ in keys)
    with _LOCK, _connect() as connection:
        connection.execute(
            f"delete from account_login_progress where username in ({placeholders})",
            tuple(keys),
        )
