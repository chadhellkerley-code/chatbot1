from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - zoneinfo is stdlib on supported runtimes
    ZoneInfo = None  # type: ignore[assignment]

from cryptography.fernet import Fernet, InvalidToken

from core.storage_atomic import (
    atomic_append_jsonl,
    atomic_write_json,
    atomic_write_text,
    load_json_file,
    load_jsonl_entries,
)
from paths import accounts_root, logs_root, runtime_base


logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 2
_DB_SCHEMA_VERSION = 2
_DEFAULT_PAYLOAD = {"schema_version": _SCHEMA_VERSION, "proxies": []}
_STORE_CACHE_GUARD = threading.RLock()
_STORE_CACHE: dict[str, "ProxyRegistryStore"] = {}
_FERNET_LOCK = threading.RLock()
_FERNET_CACHE: dict[str, Fernet] = {}
_SECRET_PREFIX = "enc:v1:"
_AUDIT_MAX_SIZE_MB = 10.0
_SENSITIVE_MARKERS = {"password", "pass", "secret", "token", "otp", "code"}
_PROXY_QUARANTINE_THRESHOLD = max(1, int(os.getenv("PROXY_QUARANTINE_THRESHOLD", "3") or "3"))
_PROXY_QUARANTINE_SECONDS = max(60, int(os.getenv("PROXY_QUARANTINE_SECONDS", "900") or "900"))
_PROXY_MAX_QUARANTINE_SECONDS = max(
    _PROXY_QUARANTINE_SECONDS,
    int(os.getenv("PROXY_MAX_QUARANTINE_SECONDS", "7200") or "7200"),
)


class ProxyValidationError(ValueError):
    pass


class ProxyResolutionError(RuntimeError):
    def __init__(self, code: str, proxy_id: str = "", message: str = "") -> None:
        self.code = str(code or "").strip() or "proxy_resolution_error"
        self.proxy_id = str(proxy_id or "").strip()
        detail = str(message or "").strip()
        if not detail:
            detail = self.code
            if self.proxy_id:
                detail = f"{detail}:{self.proxy_id}"
        super().__init__(detail)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _now_ts() -> float:
    return time.time()


def _normalize_timestamp(value: Any) -> str:
    return _clean(value)


def _normalize_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_int(value: Any, *, default: int = 0, minimum: int = 0) -> int:
    try:
        return max(minimum, int(value))
    except Exception:
        return max(minimum, int(default))


def _normalize_bool(value: Any, *, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _clean(value).lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "si", "on", "active", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "inactive", "disabled"}:
        return False
    return bool(default)


def _normalize_timezone_id(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if ZoneInfo is None:
        return text
    try:
        ZoneInfo(text)
    except Exception as exc:
        raise ProxyValidationError(f"Timezone de proxy invalida: {text}") from exc
    return text


def _sanitize_meta(value: Any) -> Any:
    if isinstance(value, dict):
        payload: dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key or "").lower()
            if any(marker in lowered for marker in _SENSITIVE_MARKERS):
                payload[str(key)] = "***"
            else:
                payload[str(key)] = _sanitize_meta(item)
        return payload
    if isinstance(value, list):
        return [_sanitize_meta(item) for item in value]
    return value


def _resolved_base_root(base_root: Path | None = None) -> Path:
    candidate = Path(base_root) if base_root is not None else Path(__file__).resolve().parent.parent
    return runtime_base(candidate)


def _base_root_from_path(path: Path | None) -> Path:
    if path is None:
        return _resolved_base_root()
    try:
        return path.resolve().parents[2]
    except Exception:
        return _resolved_base_root()


def proxy_store_path(base_root: Path | None = None) -> Path:
    return accounts_root(_resolved_base_root(base_root)) / "proxies.json"


def proxy_db_path(base_root: Path | None = None) -> Path:
    root_dir = _resolved_base_root(base_root)
    path = root_dir / "data" / "proxy_registry.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def proxy_audit_path(base_root: Path | None = None) -> Path:
    return logs_root(_resolved_base_root(base_root)) / "proxy_audit.jsonl"


def _audit_path_from_store_path(path: Path | None) -> Path:
    return proxy_audit_path(_base_root_from_path(path))


def _session_key_path(base_root: Path | None = None) -> Path:
    return _resolved_base_root(base_root) / ".session_key"


def _fernet(base_root: Path | None = None) -> Fernet:
    key_path = _session_key_path(base_root)
    cache_key = str(key_path.resolve())
    with _FERNET_LOCK:
        current = _FERNET_CACHE.get(cache_key)
        if current is not None:
            return current
        key_path.parent.mkdir(parents=True, exist_ok=True)
        if key_path.exists():
            raw_key = key_path.read_text(encoding="utf-8").strip().encode("utf-8")
        else:
            raw_key = Fernet.generate_key()
            atomic_write_text(key_path, raw_key.decode("ascii"))
        current = Fernet(raw_key)
        _FERNET_CACHE[cache_key] = current
        return current


def _encrypt_secret(value: Any, *, base_root: Path | None = None) -> str:
    text = _clean(value)
    if not text:
        return ""
    token = _fernet(base_root).encrypt(text.encode("utf-8")).decode("ascii")
    return f"{_SECRET_PREFIX}{token}"


def _decrypt_secret(value: Any, *, base_root: Path | None = None) -> str:
    text = _clean(value)
    if not text:
        return ""
    if not text.startswith(_SECRET_PREFIX):
        return text
    token = text[len(_SECRET_PREFIX) :]
    try:
        return _fernet(base_root).decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        logger.error("No se pudo descifrar un secreto de proxy; se devolvera vacio.")
        return ""


def _split_server_and_auth(raw_server: Any) -> tuple[str, str, str]:
    text = _clean(raw_server)
    if not text:
        raise ProxyValidationError("Servidor de proxy vacio.")
    candidate = text if "://" in text else f"http://{text}"
    try:
        parsed = urlparse(candidate)
        hostname = parsed.hostname or ""
        port = parsed.port
    except Exception as exc:
        raise ProxyValidationError("Servidor de proxy invalido.") from exc
    if parsed.scheme not in {"http", "https"} or not hostname:
        raise ProxyValidationError("El proxy debe usar http:// o https://")
    server = f"{parsed.scheme}://{hostname}"
    if port:
        server += f":{port}"
    username = unquote(parsed.username) if parsed.username else ""
    password = unquote(parsed.password) if parsed.password else ""
    return server, username, password


def _raw_secret(record: dict[str, Any], primary: str, encrypted: str, *, base_root: Path | None = None) -> str:
    if _clean(record.get(encrypted)):
        return _decrypt_secret(record.get(encrypted), base_root=base_root)
    return _clean(record.get(primary))


def _quarantine_seconds(consecutive_failures: int) -> int:
    over = max(0, int(consecutive_failures) - int(_PROXY_QUARANTINE_THRESHOLD))
    seconds = int(_PROXY_QUARANTINE_SECONDS) * int(2**over)
    return max(_PROXY_QUARANTINE_SECONDS, min(_PROXY_MAX_QUARANTINE_SECONDS, seconds))


def _quarantine_active_until(value: Any) -> float:
    current = _normalize_float(value) or 0.0
    if current <= 0:
        return 0.0
    if current <= _now_ts():
        return 0.0
    return current


def normalize_proxy_record(
    raw: dict[str, Any],
    *,
    base_root: Path | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ProxyValidationError("Registro de proxy invalido.")
    proxy_id = _clean(raw.get("id") or raw.get("proxy_id"))
    if not proxy_id:
        raise ProxyValidationError("Proxy invalido.")
    server, server_user, server_pass = _split_server_and_auth(
        raw.get("server") or raw.get("proxy_url") or raw.get("url") or raw.get("proxy")
    )
    user = (
        _raw_secret(raw, "user", "user_enc", base_root=base_root)
        or _raw_secret(raw, "proxy_user", "proxy_user_enc", base_root=base_root)
        or _clean(raw.get("username"))
        or _clean(raw.get("proxy_username"))
        or server_user
    )
    password = (
        _raw_secret(raw, "pass", "pass_enc", base_root=base_root)
        or _raw_secret(raw, "proxy_pass", "proxy_pass_enc", base_root=base_root)
        or _clean(raw.get("password"))
        or _clean(raw.get("proxy_password"))
        or server_pass
    )
    active = _normalize_bool(raw.get("active"), default=True)
    disabled_reason = _clean(raw.get("disabled_reason"))
    if active:
        disabled_reason = ""
    elif not disabled_reason:
        disabled_reason = "manual"
    latency_ms = _normalize_float(raw.get("last_latency_ms"))
    quarantine_until = _quarantine_active_until(raw.get("quarantine_until"))
    quarantine_reason = _clean(raw.get("quarantine_reason"))
    if quarantine_until <= 0:
        quarantine_reason = ""
    return {
        "id": proxy_id,
        "server": server,
        "user": user,
        "pass": password,
        "timezone_id": _normalize_timezone_id(
            raw.get("timezone_id")
            or raw.get("proxy_timezone_id")
            or raw.get("timezone")
            or raw.get("tz")
        ),
        "active": active,
        "disabled_reason": disabled_reason,
        "last_test_at": _normalize_timestamp(raw.get("last_test_at")),
        "last_success_at": _normalize_timestamp(raw.get("last_success_at")),
        "last_failure_at": _normalize_timestamp(raw.get("last_failure_at")),
        "last_public_ip": _clean(raw.get("last_public_ip")),
        "last_latency_ms": latency_ms,
        "last_error": _clean(raw.get("last_error")),
        "failure_count": _normalize_int(raw.get("failure_count"), default=0, minimum=0),
        "success_count": _normalize_int(raw.get("success_count"), default=0, minimum=0),
        "consecutive_failures": _normalize_int(raw.get("consecutive_failures"), default=0, minimum=0),
        "quarantine_until": quarantine_until,
        "quarantine_reason": quarantine_reason,
        "last_event_at": _normalize_timestamp(raw.get("last_event_at")),
    }


def _serialize_proxy_record(
    record: dict[str, Any],
    *,
    existing_raw: dict[str, Any] | None = None,
    base_root: Path | None = None,
) -> dict[str, Any]:
    normalized = normalize_proxy_record(record, base_root=base_root)
    current_user = normalized.get("user") or ""
    current_pass = normalized.get("pass") or ""
    existing_user_enc = _clean((existing_raw or {}).get("user_enc"))
    existing_pass_enc = _clean((existing_raw or {}).get("pass_enc"))
    if existing_user_enc and _decrypt_secret(existing_user_enc, base_root=base_root) == current_user:
        user_enc = existing_user_enc
    else:
        user_enc = _encrypt_secret(current_user, base_root=base_root)
    if existing_pass_enc and _decrypt_secret(existing_pass_enc, base_root=base_root) == current_pass:
        pass_enc = existing_pass_enc
    else:
        pass_enc = _encrypt_secret(current_pass, base_root=base_root)
    return {
        "id": normalized["id"],
        "server": normalized["server"],
        "user_enc": user_enc,
        "pass_enc": pass_enc,
        "timezone_id": normalized["timezone_id"],
        "active": normalized["active"],
        "disabled_reason": normalized["disabled_reason"],
        "last_test_at": normalized["last_test_at"],
        "last_success_at": normalized["last_success_at"],
        "last_failure_at": normalized["last_failure_at"],
        "last_public_ip": normalized["last_public_ip"],
        "last_latency_ms": normalized["last_latency_ms"],
        "last_error": normalized["last_error"],
        "failure_count": normalized["failure_count"],
        "success_count": normalized["success_count"],
        "consecutive_failures": normalized["consecutive_failures"],
        "quarantine_until": normalized["quarantine_until"],
        "quarantine_reason": normalized["quarantine_reason"],
        "last_event_at": normalized["last_event_at"],
    }


def _normalize_payload(
    payload: Any,
    *,
    base_root: Path | None = None,
) -> tuple[dict[str, Any], bool]:
    changed = False
    if isinstance(payload, list):
        raw_records = payload
        changed = True
    elif isinstance(payload, dict):
        raw_records = payload.get("proxies")
        if payload.get("schema_version") != _SCHEMA_VERSION:
            changed = True
    else:
        raw_records = []
        changed = True
    if not isinstance(raw_records, list):
        raw_records = []
        changed = True
    normalized_records: list[dict[str, Any]] = []
    existing_by_id: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for raw in raw_records:
        if not isinstance(raw, dict):
            changed = True
            continue
        try:
            normalized = normalize_proxy_record(raw, base_root=base_root)
        except ProxyValidationError:
            logger.warning("Se descarto un registro de proxy invalido durante normalizacion.")
            changed = True
            continue
        key = str(normalized.get("id") or "").strip().lower()
        if not key or key in seen:
            changed = True
            continue
        seen.add(key)
        existing_by_id[key] = raw
        normalized_records.append(normalized)
    serialized_records = [
        _serialize_proxy_record(
            record,
            existing_raw=existing_by_id.get(str(record["id"]).lower()),
            base_root=base_root,
        )
        for record in normalized_records
    ]
    normalized_payload = {
        "schema_version": _SCHEMA_VERSION,
        "proxies": serialized_records,
    }
    if payload != normalized_payload:
        changed = True
    return normalized_payload, changed


def _store_for_path(path: Path | None = None) -> "ProxyRegistryStore":
    base_root = _base_root_from_path(path)
    cache_key = str(proxy_db_path(base_root).resolve())
    with _STORE_CACHE_GUARD:
        current = _STORE_CACHE.get(cache_key)
        if current is None:
            current = ProxyRegistryStore(base_root)
            _STORE_CACHE[cache_key] = current
        return current


class ProxyRegistryStore:
    def __init__(self, base_root: Path) -> None:
        self.base_root = _resolved_base_root(base_root)
        self.db_path = proxy_db_path(self.base_root)
        self.shadow_path = proxy_store_path(self.base_root)
        self.shadow_audit_path = proxy_audit_path(self.base_root)
        self.accounts_path = accounts_root(self.base_root) / "accounts.json"
        self._lock = threading.RLock()
        self._ensure_schema()
        self._migrate_legacy_state()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("pragma foreign_keys = on")
        connection.execute("pragma journal_mode = wal")
        connection.execute("pragma busy_timeout = 5000")
        return connection

    def _ensure_schema(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                f"""
                create table if not exists registry_meta (
                    meta_key text primary key,
                    meta_value text not null,
                    updated_at text not null
                );

                create table if not exists proxies (
                    id text primary key collate nocase,
                    server text not null,
                    user_enc text not null default '',
                    pass_enc text not null default '',
                    timezone_id text not null default '',
                    active integer not null default 1,
                    disabled_reason text not null default '',
                    last_test_at text not null default '',
                    last_success_at text not null default '',
                    last_failure_at text not null default '',
                    last_public_ip text not null default '',
                    last_latency_ms real,
                    last_error text not null default '',
                    failure_count integer not null default 0,
                    success_count integer not null default 0,
                    consecutive_failures integer not null default 0,
                    quarantine_until real not null default 0,
                    quarantine_reason text not null default '',
                    last_event_at text not null default '',
                    created_at text not null,
                    updated_at text not null
                );

                create table if not exists proxy_audit (
                    id integer primary key autoincrement,
                    ts text not null,
                    proxy_id text not null collate nocase,
                    event text not null,
                    status text not null,
                    message text not null default '',
                    meta_json text not null default '{{}}'
                );

                create index if not exists idx_proxy_registry_active
                    on proxies(active);
                create index if not exists idx_proxy_registry_quarantine
                    on proxies(quarantine_until);
                create index if not exists idx_proxy_registry_audit_proxy_id
                    on proxy_audit(proxy_id, id desc);

                create table if not exists account_proxy_links (
                    username text primary key collate nocase,
                    alias text not null default '',
                    assigned_proxy_id text references proxies(id) on delete restrict on update cascade,
                    has_legacy_proxy integer not null default 0,
                    updated_at text not null
                );

                create index if not exists idx_account_proxy_links_proxy
                    on account_proxy_links(assigned_proxy_id);
                """
            )
            proxy_columns = {
                _clean(row["name"]).lower()
                for row in connection.execute("pragma table_info(proxies)").fetchall()
            }
            if "timezone_id" not in proxy_columns:
                connection.execute(
                    "alter table proxies add column timezone_id text not null default ''"
                )
            self._set_meta_locked(
                connection,
                "db_schema_version",
                str(_DB_SCHEMA_VERSION),
            )

    def _meta_value_locked(self, connection: sqlite3.Connection, key: str) -> str:
        row = connection.execute(
            "select meta_value from registry_meta where meta_key = ?",
            (_clean(key),),
        ).fetchone()
        return _clean(row["meta_value"]) if row is not None else ""

    def _set_meta_locked(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        stamp = _now_iso()
        connection.execute(
            """
            insert into registry_meta (meta_key, meta_value, updated_at)
            values (?, ?, ?)
            on conflict(meta_key) do update set
                meta_value = excluded.meta_value,
                updated_at = excluded.updated_at
            """,
            (_clean(key), _clean(value), stamp),
        )

    def _has_proxy_rows_locked(self, connection: sqlite3.Connection) -> bool:
        row = connection.execute("select 1 from proxies limit 1").fetchone()
        return row is not None

    def _has_audit_rows_locked(self, connection: sqlite3.Connection) -> bool:
        row = connection.execute("select 1 from proxy_audit limit 1").fetchone()
        return row is not None

    def _migrate_legacy_state(self) -> None:
        with self._lock, self._connect() as connection:
            with connection:
                if self._meta_value_locked(connection, "legacy_proxy_payload_migrated_v1") != "1":
                    self._migrate_legacy_proxy_payload_locked(connection)
                    self._set_meta_locked(connection, "legacy_proxy_payload_migrated_v1", "1")
                if self._meta_value_locked(connection, "legacy_proxy_audit_migrated_v1") != "1":
                    self._migrate_legacy_proxy_audit_locked(connection)
                    self._set_meta_locked(connection, "legacy_proxy_audit_migrated_v1", "1")
                if self._meta_value_locked(connection, "legacy_account_proxy_links_migrated_v1") != "1":
                    self._migrate_account_proxy_links_locked(connection)
                    self._set_meta_locked(connection, "legacy_account_proxy_links_migrated_v1", "1")
            self._write_shadow_payload_locked(connection)

    def _migrate_legacy_proxy_payload_locked(self, connection: sqlite3.Connection) -> None:
        payload = load_json_file(self.shadow_path, _DEFAULT_PAYLOAD, label="proxy.registry")
        normalized_payload, changed = _normalize_payload(payload, base_root=self.base_root)
        if changed:
            self.shadow_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.shadow_path, normalized_payload)
        if self._has_proxy_rows_locked(connection):
            return
        for raw in normalized_payload.get("proxies") or []:
            if not isinstance(raw, dict):
                continue
            self._upsert_proxy_row_locked(connection, raw)

    def _migrate_legacy_proxy_audit_locked(self, connection: sqlite3.Connection) -> None:
        if self._has_audit_rows_locked(connection):
            return
        for row in load_jsonl_entries(self.shadow_audit_path, label="proxy.audit"):
            if not isinstance(row, dict):
                continue
            entry = {
                "ts": _clean(row.get("ts")) or _now_iso(),
                "proxy_id": _clean(row.get("proxy_id")) or "bulk",
                "event": _clean(row.get("event")) or "proxy_event",
                "status": _clean(row.get("status")) or "unknown",
                "message": _clean(row.get("message")),
                "meta": row.get("meta") if isinstance(row.get("meta"), dict) else {},
            }
            self._insert_audit_locked(connection, entry)

    def _migrate_account_proxy_links_locked(self, connection: sqlite3.Connection) -> None:
        payload = load_json_file(self.accounts_path, [], label="accounts.registry")
        if not isinstance(payload, list):
            payload = []
        self._replace_account_proxy_links_locked(connection, payload)

    def _raw_proxy_from_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return {
            "id": _clean(row["id"]),
            "server": _clean(row["server"]),
            "user_enc": _clean(row["user_enc"]),
            "pass_enc": _clean(row["pass_enc"]),
            "timezone_id": _clean(row["timezone_id"]),
            "active": bool(int(row["active"])) if row["active"] not in (None, "") else True,
            "disabled_reason": _clean(row["disabled_reason"]),
            "last_test_at": _clean(row["last_test_at"]),
            "last_success_at": _clean(row["last_success_at"]),
            "last_failure_at": _clean(row["last_failure_at"]),
            "last_public_ip": _clean(row["last_public_ip"]),
            "last_latency_ms": _normalize_float(row["last_latency_ms"]),
            "last_error": _clean(row["last_error"]),
            "failure_count": _normalize_int(row["failure_count"], default=0, minimum=0),
            "success_count": _normalize_int(row["success_count"], default=0, minimum=0),
            "consecutive_failures": _normalize_int(row["consecutive_failures"], default=0, minimum=0),
            "quarantine_until": _normalize_float(row["quarantine_until"]) or 0.0,
            "quarantine_reason": _clean(row["quarantine_reason"]),
            "last_event_at": _clean(row["last_event_at"]),
        }

    def _proxy_record_from_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return normalize_proxy_record(self._raw_proxy_from_row(row), base_root=self.base_root)

    def _select_proxy_row_locked(self, connection: sqlite3.Connection, proxy_id: str) -> sqlite3.Row | None:
        return connection.execute(
            """
            select *
            from proxies
            where id = ? collate nocase
            limit 1
            """,
            (_clean(proxy_id),),
        ).fetchone()

    def _select_proxy_rows_locked(self, connection: sqlite3.Connection) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                select *
                from proxies
                order by lower(id) asc, id asc
                """
            ).fetchall()
        )

    def _existing_raw_map_locked(self, connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        for row in self._select_proxy_rows_locked(connection):
            raw = self._raw_proxy_from_row(row)
            payload[_clean(raw.get("id")).lower()] = raw
        return payload

    def _upsert_proxy_row_locked(self, connection: sqlite3.Connection, raw: dict[str, Any]) -> None:
        normalized = normalize_proxy_record(raw, base_root=self.base_root)
        existing_raw = self._existing_raw_map_locked(connection).get(normalized["id"].lower())
        serialized = _serialize_proxy_record(
            normalized,
            existing_raw=existing_raw,
            base_root=self.base_root,
        )
        current_row = self._select_proxy_row_locked(connection, serialized["id"])
        created_at = _clean(current_row["created_at"]) if current_row is not None else _now_iso()
        updated_at = _now_iso()
        connection.execute(
            """
            insert into proxies (
                id,
                server,
                user_enc,
                pass_enc,
                timezone_id,
                active,
                disabled_reason,
                last_test_at,
                last_success_at,
                last_failure_at,
                last_public_ip,
                last_latency_ms,
                last_error,
                failure_count,
                success_count,
                consecutive_failures,
                quarantine_until,
                quarantine_reason,
                last_event_at,
                created_at,
                updated_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(id) do update set
                server = excluded.server,
                user_enc = excluded.user_enc,
                pass_enc = excluded.pass_enc,
                timezone_id = excluded.timezone_id,
                active = excluded.active,
                disabled_reason = excluded.disabled_reason,
                last_test_at = excluded.last_test_at,
                last_success_at = excluded.last_success_at,
                last_failure_at = excluded.last_failure_at,
                last_public_ip = excluded.last_public_ip,
                last_latency_ms = excluded.last_latency_ms,
                last_error = excluded.last_error,
                failure_count = excluded.failure_count,
                success_count = excluded.success_count,
                consecutive_failures = excluded.consecutive_failures,
                quarantine_until = excluded.quarantine_until,
                quarantine_reason = excluded.quarantine_reason,
                last_event_at = excluded.last_event_at,
                updated_at = excluded.updated_at
            """,
            (
                serialized["id"],
                serialized["server"],
                serialized["user_enc"],
                serialized["pass_enc"],
                serialized["timezone_id"],
                1 if serialized["active"] else 0,
                serialized["disabled_reason"],
                serialized["last_test_at"],
                serialized["last_success_at"],
                serialized["last_failure_at"],
                serialized["last_public_ip"],
                serialized["last_latency_ms"],
                serialized["last_error"],
                serialized["failure_count"],
                serialized["success_count"],
                serialized["consecutive_failures"],
                serialized["quarantine_until"],
                serialized["quarantine_reason"],
                serialized["last_event_at"],
                created_at,
                updated_at,
            ),
        )

    def _write_shadow_payload_locked(self, connection: sqlite3.Connection) -> dict[str, Any]:
        rows = [self._raw_proxy_from_row(row) for row in self._select_proxy_rows_locked(connection)]
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "proxies": rows,
        }
        self.shadow_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.shadow_path, payload)
        return payload

    def _insert_audit_locked(self, connection: sqlite3.Connection, entry: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "ts": _clean(entry.get("ts")) or _now_iso(),
            "proxy_id": _clean(entry.get("proxy_id")) or "bulk",
            "event": _clean(entry.get("event")) or "proxy_event",
            "status": _clean(entry.get("status")) or "unknown",
            "message": _clean(entry.get("message")),
            "meta": _sanitize_meta(entry.get("meta") if isinstance(entry.get("meta"), dict) else {}),
        }
        connection.execute(
            """
            insert into proxy_audit (
                ts,
                proxy_id,
                event,
                status,
                message,
                meta_json
            )
            values (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["ts"],
                payload["proxy_id"],
                payload["event"],
                payload["status"],
                payload["message"],
                json.dumps(payload["meta"], ensure_ascii=False, sort_keys=True),
            ),
        )
        return payload

    def _mirror_audit_entries(self, entries: list[dict[str, Any]], target: Path | None = None) -> None:
        if not entries:
            return
        audit_target = Path(target) if target is not None else self.shadow_audit_path
        audit_target.parent.mkdir(parents=True, exist_ok=True)
        for entry in entries:
            atomic_append_jsonl(audit_target, entry, max_size_mb=_AUDIT_MAX_SIZE_MB)

    def _account_proxy_rows(self, accounts: list[dict[str, Any]]) -> list[tuple[str, str, str | None, int, str]]:
        rows: list[tuple[str, str, str | None, int, str]] = []
        seen: set[str] = set()
        stamp = _now_iso()
        for account in accounts:
            if not isinstance(account, dict):
                continue
            username = _clean(account.get("username")).lstrip("@")
            if not username:
                continue
            key = username.lower()
            if key in seen:
                continue
            seen.add(key)
            alias = _clean(account.get("alias_id") or account.get("alias") or account.get("alias_display_name"))
            assigned_proxy_id = _clean(account.get("assigned_proxy_id")) or None
            has_legacy_proxy = 1 if _clean(account.get("proxy_url")) else 0
            rows.append((username, alias, assigned_proxy_id, has_legacy_proxy, stamp))
        return rows

    def _replace_account_proxy_links_locked(
        self,
        connection: sqlite3.Connection,
        accounts: list[dict[str, Any]],
    ) -> int:
        rows = self._account_proxy_rows(accounts)
        connection.execute("delete from account_proxy_links")
        inserted = 0
        for username, alias, assigned_proxy_id, has_legacy_proxy, updated_at in rows:
            resolved_proxy_id: str | None = None
            if assigned_proxy_id:
                proxy_row = self._select_proxy_row_locked(connection, assigned_proxy_id)
                if proxy_row is not None:
                    resolved_proxy_id = _clean(proxy_row["id"])
                else:
                    logger.warning(
                        "Se omitio asignacion a proxy inexistente durante sync de cuenta %s -> %s",
                        username,
                        assigned_proxy_id,
                    )
            connection.execute(
                """
                insert into account_proxy_links (
                    username,
                    alias,
                    assigned_proxy_id,
                    has_legacy_proxy,
                    updated_at
                )
                values (?, ?, ?, ?, ?)
                """,
                (username, alias, resolved_proxy_id, has_legacy_proxy, updated_at),
            )
            inserted += 1
        return inserted

    def shadow_payload(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            return self._write_shadow_payload_locked(connection)

    def list_proxies(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            return [self._proxy_record_from_row(row) for row in self._select_proxy_rows_locked(connection)]

    def get_proxy_by_id(self, proxy_id: Any, *, active_only: bool = False) -> dict[str, Any] | None:
        clean_id = _clean(proxy_id)
        if not clean_id:
            return None
        with self._lock, self._connect() as connection:
            row = self._select_proxy_row_locked(connection, clean_id)
            if row is None:
                return None
            record = self._proxy_record_from_row(row)
            if active_only and not is_active_proxy(record):
                return None
            return record

    def save_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in records:
            normalized_record = normalize_proxy_record(raw, base_root=self.base_root)
            key = normalized_record["id"].lower()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(normalized_record)
        with self._lock, self._connect() as connection:
            existing_by_id = self._existing_raw_map_locked(connection)
            with connection:
                keep_keys = {record["id"].lower() for record in normalized}
                for existing_key, existing_raw in existing_by_id.items():
                    if existing_key in keep_keys:
                        continue
                    connection.execute(
                        "delete from proxies where id = ? collate nocase",
                        (_clean(existing_raw.get("id")),),
                    )
                for record in normalized:
                    self._upsert_proxy_row_locked(
                        connection,
                        _serialize_proxy_record(
                            record,
                            existing_raw=existing_by_id.get(record["id"].lower()),
                            base_root=self.base_root,
                        ),
                    )
            self._write_shadow_payload_locked(connection)
            return [self._proxy_record_from_row(row) for row in self._select_proxy_rows_locked(connection)]

    def upsert_record(self, record: dict[str, Any]) -> dict[str, Any]:
        proxy_id = _clean(record.get("id") or record.get("proxy_id"))
        if not proxy_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            existing_row = self._select_proxy_row_locked(connection, proxy_id)
            candidate = self._proxy_record_from_row(existing_row) if existing_row is not None else {}
            candidate.update(record)
            normalized = normalize_proxy_record(candidate, base_root=self.base_root)
            existing_raw = self._raw_proxy_from_row(existing_row) if existing_row is not None else None
            with connection:
                self._upsert_proxy_row_locked(
                    connection,
                    _serialize_proxy_record(
                        normalized,
                        existing_raw=existing_raw,
                        base_root=self.base_root,
                    ),
                )
            self._write_shadow_payload_locked(connection)
            stored_row = self._select_proxy_row_locked(connection, normalized["id"])
            if stored_row is None:
                raise ProxyValidationError(f"No se encontro el proxy {normalized['id']}.")
            return self._proxy_record_from_row(stored_row)

    def upsert_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in records:
            proxy_id = _clean(raw.get("id") or raw.get("proxy_id"))
            if not proxy_id or proxy_id.lower() in seen:
                continue
            seen.add(proxy_id.lower())
            prepared.append(dict(raw))
        if not prepared:
            return []
        with self._lock, self._connect() as connection:
            existing_by_id = self._existing_raw_map_locked(connection)
            normalized_ids: list[str] = []
            with connection:
                for raw in prepared:
                    proxy_id = _clean(raw.get("id") or raw.get("proxy_id"))
                    existing_raw = existing_by_id.get(proxy_id.lower())
                    candidate = (
                        normalize_proxy_record(existing_raw, base_root=self.base_root)
                        if existing_raw is not None
                        else {}
                    )
                    candidate.update(raw)
                    normalized = normalize_proxy_record(candidate, base_root=self.base_root)
                    self._upsert_proxy_row_locked(
                        connection,
                        _serialize_proxy_record(
                            normalized,
                            existing_raw=existing_raw,
                            base_root=self.base_root,
                        ),
                    )
                    normalized_ids.append(normalized["id"])
            self._write_shadow_payload_locked(connection)
            output: list[dict[str, Any]] = []
            for proxy_id in normalized_ids:
                row = self._select_proxy_row_locked(connection, proxy_id)
                if row is not None:
                    output.append(self._proxy_record_from_row(row))
            return output

    def set_active(self, proxy_id: Any, *, active: bool) -> dict[str, Any]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            row = self._select_proxy_row_locked(connection, clean_id)
            if row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            current = self._proxy_record_from_row(row)
            current["active"] = bool(active)
            current["disabled_reason"] = "" if active else "manual"
            with connection:
                self._upsert_proxy_row_locked(
                    connection,
                    _serialize_proxy_record(
                        current,
                        existing_raw=self._raw_proxy_from_row(row),
                        base_root=self.base_root,
                    ),
                )
            self._write_shadow_payload_locked(connection)
            updated_row = self._select_proxy_row_locked(connection, clean_id)
            if updated_row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            return self._proxy_record_from_row(updated_row)

    def delete_proxy(self, proxy_id: Any) -> int:
        clean_id = _clean(proxy_id)
        if not clean_id:
            return 0
        with self._lock, self._connect() as connection:
            assigned_count = int(
                connection.execute(
                    """
                    select count(*)
                    from account_proxy_links
                    where assigned_proxy_id = ? collate nocase
                    """,
                    (clean_id,),
                ).fetchone()[0]
            )
            if assigned_count:
                raise ProxyValidationError(
                    f"No se puede eliminar {clean_id}: hay {assigned_count} cuenta(s) asignadas."
                )
            with connection:
                deleted = connection.execute(
                    "delete from proxies where id = ? collate nocase",
                    (clean_id,),
                ).rowcount
            if deleted:
                self._write_shadow_payload_locked(connection)
            return int(deleted or 0)

    def sync_account_proxy_links(self, accounts: list[dict[str, Any]]) -> int:
        with self._lock, self._connect() as connection:
            with connection:
                inserted = self._replace_account_proxy_links_locked(connection, accounts)
            return inserted

    def assigned_accounts(self, proxy_id: Any) -> list[str]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            return []
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                select username
                from account_proxy_links
                where assigned_proxy_id = ? collate nocase
                order by lower(username) asc, username asc
                """,
                (clean_id,),
            ).fetchall()
        return [_clean(row["username"]).lstrip("@") for row in rows if _clean(row["username"])]

    def record_audit_event(
        self,
        proxy_id: Any,
        *,
        event: str,
        status: str,
        message: str = "",
        meta: dict[str, Any] | None = None,
        audit_target: Path | None = None,
    ) -> dict[str, Any]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            with connection:
                entry = self._insert_audit_locked(
                    connection,
                    {
                        "ts": _now_iso(),
                        "proxy_id": clean_id,
                        "event": _clean(event) or "proxy_event",
                        "status": _clean(status) or "unknown",
                        "message": _clean(message),
                        "meta": meta or {},
                    },
                )
            self._mirror_audit_entries([entry], audit_target)
            return entry

    def load_audit_entries(
        self,
        *,
        proxy_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clean_proxy_id = _clean(proxy_id)
        max_rows = max(1, int(limit or 1))
        sql = """
            select ts, proxy_id, event, status, message, meta_json
            from proxy_audit
        """
        params: list[Any] = []
        if clean_proxy_id:
            sql += " where proxy_id = ? collate nocase"
            params.append(clean_proxy_id)
        sql += " order by id desc limit ?"
        params.append(max_rows)
        with self._lock, self._connect() as connection:
            rows = connection.execute(sql, tuple(params)).fetchall()
        payload: list[dict[str, Any]] = []
        for row in reversed(rows):
            try:
                meta = json.loads(_clean(row["meta_json"]) or "{}")
            except Exception:
                meta = {}
            item = {
                "ts": _clean(row["ts"]),
                "proxy_id": _clean(row["proxy_id"]),
                "event": _clean(row["event"]),
                "status": _clean(row["status"]),
            }
            if _clean(row["message"]):
                item["message"] = _clean(row["message"])
            if isinstance(meta, dict) and meta:
                item["meta"] = meta
            payload.append(item)
        return payload

    def record_success(
        self,
        proxy_id: Any,
        *,
        event: str,
        public_ip: str = "",
        latency_ms: float | None = None,
        audit_target: Path | None = None,
        message: str = "",
    ) -> dict[str, Any]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            row = self._select_proxy_row_locked(connection, clean_id)
            if row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            current = self._proxy_record_from_row(row)
            stamp = _now_iso()
            current["last_test_at"] = stamp
            current["last_success_at"] = stamp
            current["last_event_at"] = stamp
            current["last_public_ip"] = _clean(public_ip) or _clean(current.get("last_public_ip"))
            current["last_latency_ms"] = (
                round(float(latency_ms), 2) if latency_ms is not None else current.get("last_latency_ms")
            )
            current["last_error"] = ""
            current["success_count"] = _normalize_int(current.get("success_count")) + 1
            current["consecutive_failures"] = 0
            current["quarantine_until"] = 0.0
            current["quarantine_reason"] = ""
            audit_entries: list[dict[str, Any]] = []
            with connection:
                self._upsert_proxy_row_locked(
                    connection,
                    _serialize_proxy_record(
                        current,
                        existing_raw=self._raw_proxy_from_row(row),
                        base_root=self.base_root,
                    ),
                )
                audit_entries.append(
                    self._insert_audit_locked(
                        connection,
                        {
                            "ts": stamp,
                            "proxy_id": clean_id,
                            "event": event,
                            "status": "ok",
                            "message": message or f"Proxy {clean_id} operativo.",
                            "meta": {
                                "public_ip": _clean(public_ip),
                                "latency_ms": current.get("last_latency_ms"),
                            },
                        },
                    )
                )
            updated_row = self._select_proxy_row_locked(connection, clean_id)
            self._write_shadow_payload_locked(connection)
            self._mirror_audit_entries(audit_entries, audit_target)
            if updated_row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            return self._proxy_record_from_row(updated_row)

    def record_failure(
        self,
        proxy_id: Any,
        *,
        event: str,
        error: str,
        audit_target: Path | None = None,
    ) -> dict[str, Any]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            row = self._select_proxy_row_locked(connection, clean_id)
            if row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            current = self._proxy_record_from_row(row)
            stamp = _now_iso()
            consecutive_failures = _normalize_int(current.get("consecutive_failures")) + 1
            current["last_test_at"] = stamp
            current["last_failure_at"] = stamp
            current["last_event_at"] = stamp
            current["last_error"] = _clean(error) or "unknown_error"
            current["failure_count"] = _normalize_int(current.get("failure_count")) + 1
            current["consecutive_failures"] = consecutive_failures
            quarantine_opened = False
            if consecutive_failures >= _PROXY_QUARANTINE_THRESHOLD:
                quarantine_opened = True
                current["quarantine_until"] = max(
                    float(current.get("quarantine_until") or 0.0),
                    _now_ts() + float(_quarantine_seconds(consecutive_failures)),
                )
                current["quarantine_reason"] = current["last_error"]
            audit_entries: list[dict[str, Any]] = []
            with connection:
                self._upsert_proxy_row_locked(
                    connection,
                    _serialize_proxy_record(
                        current,
                        existing_raw=self._raw_proxy_from_row(row),
                        base_root=self.base_root,
                    ),
                )
                audit_entries.append(
                    self._insert_audit_locked(
                        connection,
                        {
                            "ts": stamp,
                            "proxy_id": clean_id,
                            "event": event,
                            "status": "failed",
                            "message": current["last_error"],
                            "meta": {
                                "consecutive_failures": current.get("consecutive_failures"),
                                "failure_count": current.get("failure_count"),
                                "quarantine_until": current.get("quarantine_until"),
                            },
                        },
                    )
                )
                if quarantine_opened:
                    audit_entries.append(
                        self._insert_audit_locked(
                            connection,
                            {
                                "ts": stamp,
                                "proxy_id": clean_id,
                                "event": "proxy_quarantine_opened",
                                "status": "quarantined",
                                "message": f"Proxy {clean_id} en cuarentena automatica.",
                                "meta": {
                                    "quarantine_until": current.get("quarantine_until"),
                                    "reason": current.get("quarantine_reason"),
                                },
                            },
                        )
                    )
            updated_row = self._select_proxy_row_locked(connection, clean_id)
            self._write_shadow_payload_locked(connection)
            self._mirror_audit_entries(audit_entries, audit_target)
            if updated_row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            return self._proxy_record_from_row(updated_row)

    def clear_quarantine(
        self,
        proxy_id: Any,
        *,
        event: str = "proxy_quarantine_cleared",
        audit_target: Path | None = None,
        message: str = "",
    ) -> dict[str, Any]:
        clean_id = _clean(proxy_id)
        if not clean_id:
            raise ProxyValidationError("Proxy invalido.")
        with self._lock, self._connect() as connection:
            row = self._select_proxy_row_locked(connection, clean_id)
            if row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            current = self._proxy_record_from_row(row)
            stamp = _now_iso()
            current["quarantine_until"] = 0.0
            current["quarantine_reason"] = ""
            current["consecutive_failures"] = 0
            current["last_event_at"] = stamp
            audit_entries: list[dict[str, Any]] = []
            with connection:
                self._upsert_proxy_row_locked(
                    connection,
                    _serialize_proxy_record(
                        current,
                        existing_raw=self._raw_proxy_from_row(row),
                        base_root=self.base_root,
                    ),
                )
                audit_entries.append(
                    self._insert_audit_locked(
                        connection,
                        {
                            "ts": stamp,
                            "proxy_id": clean_id,
                            "event": event,
                            "status": "ok",
                            "message": message or f"Cuarentena liberada para {clean_id}.",
                            "meta": {},
                        },
                    )
                )
            updated_row = self._select_proxy_row_locked(connection, clean_id)
            self._write_shadow_payload_locked(connection)
            self._mirror_audit_entries(audit_entries, audit_target)
            if updated_row is None:
                raise ProxyValidationError(f"No se encontro el proxy {clean_id}.")
            return self._proxy_record_from_row(updated_row)


def load_proxy_payload(path: Path | None = None) -> dict[str, Any]:
    return _store_for_path(Path(path) if path is not None else None).shadow_payload()


def load_proxies(path: Path | None = None) -> list[dict[str, Any]]:
    return _store_for_path(Path(path) if path is not None else None).list_proxies()


def save_proxy_records(records: list[dict[str, Any]], path: Path | None = None) -> list[dict[str, Any]]:
    return _store_for_path(Path(path) if path is not None else None).save_records(records)


def upsert_proxy_record(record: dict[str, Any], path: Path | None = None) -> dict[str, Any]:
    return _store_for_path(Path(path) if path is not None else None).upsert_record(record)


def upsert_proxy_records(records: list[dict[str, Any]], path: Path | None = None) -> list[dict[str, Any]]:
    return _store_for_path(Path(path) if path is not None else None).upsert_records(records)


def set_proxy_active(proxy_id: Any, *, active: bool, path: Path | None = None) -> dict[str, Any]:
    return _store_for_path(Path(path) if path is not None else None).set_active(proxy_id, active=active)


def delete_proxy_record(proxy_id: Any, path: Path | None = None) -> int:
    return _store_for_path(Path(path) if path is not None else None).delete_proxy(proxy_id)


def sync_account_proxy_links(accounts: list[dict[str, Any]], path: Path | None = None) -> int:
    return _store_for_path(Path(path) if path is not None else None).sync_account_proxy_links(accounts)


def assigned_accounts_for_proxy(proxy_id: Any, path: Path | None = None) -> list[str]:
    return _store_for_path(Path(path) if path is not None else None).assigned_accounts(proxy_id)


def record_proxy_audit_event(
    proxy_id: str,
    *,
    event: str,
    status: str,
    message: str = "",
    meta: dict[str, Any] | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else None
    return _store_for_path(target).record_audit_event(
        proxy_id,
        event=event,
        status=status,
        message=message,
        meta=meta,
        audit_target=target,
    )


def load_proxy_audit_entries(
    *,
    proxy_id: str | None = None,
    limit: int = 50,
    path: Path | None = None,
) -> list[dict[str, Any]]:
    return _store_for_path(Path(path) if path is not None else None).load_audit_entries(
        proxy_id=proxy_id,
        limit=limit,
    )


def is_active_proxy(record: dict[str, Any]) -> bool:
    try:
        normalized = normalize_proxy_record(record)
    except ProxyValidationError:
        return False
    return bool(normalized.get("active", True)) and not bool(normalized.get("quarantine_until"))


def get_proxy_by_id(
    proxy_id: Any,
    *,
    active_only: bool = False,
    path: Path | None = None,
) -> Optional[Dict[str, Any]]:
    return _store_for_path(Path(path) if path is not None else None).get_proxy_by_id(
        proxy_id,
        active_only=active_only,
    )


def list_active_proxies(path: Path | None = None) -> list[Dict[str, Any]]:
    return [dict(proxy) for proxy in load_proxies(path) if is_active_proxy(proxy)]


def proxy_reference_status(proxy_id: Any, *, path: Path | None = None) -> dict[str, Any]:
    normalized_id = _clean(proxy_id)
    if not normalized_id:
        return {"status": "none", "proxy_id": "", "record": None, "message": ""}
    record = get_proxy_by_id(normalized_id, active_only=False, path=path)
    if record is None:
        return {
            "status": "missing",
            "proxy_id": normalized_id,
            "record": None,
            "message": f"El proxy asignado {normalized_id} no existe.",
        }
    if not bool(record.get("active", True)):
        return {
            "status": "inactive",
            "proxy_id": normalized_id,
            "record": record,
            "message": f"El proxy asignado {normalized_id} esta inactivo.",
        }
    quarantine_until = float(record.get("quarantine_until") or 0.0)
    if quarantine_until > _now_ts():
        remaining = max(0, int(quarantine_until - _now_ts()))
        return {
            "status": "quarantined",
            "proxy_id": normalized_id,
            "record": record,
            "message": f"El proxy asignado {normalized_id} esta en cuarentena ({remaining}s).",
        }
    return {
        "status": "ok",
        "proxy_id": normalized_id,
        "record": record,
        "message": "",
    }


def record_proxy_success(
    proxy_id: Any,
    *,
    event: str,
    public_ip: str = "",
    latency_ms: float | None = None,
    path: Path | None = None,
    audit_path: Path | None = None,
    message: str = "",
) -> dict[str, Any]:
    target_path = Path(path) if path is not None else None
    target_audit_path = Path(audit_path) if audit_path is not None else None
    return _store_for_path(target_path).record_success(
        proxy_id,
        event=event,
        public_ip=public_ip,
        latency_ms=latency_ms,
        audit_target=target_audit_path,
        message=message,
    )


def record_proxy_failure(
    proxy_id: Any,
    *,
    event: str,
    error: str,
    path: Path | None = None,
    audit_path: Path | None = None,
) -> dict[str, Any]:
    target_path = Path(path) if path is not None else None
    target_audit_path = Path(audit_path) if audit_path is not None else None
    return _store_for_path(target_path).record_failure(
        proxy_id,
        event=event,
        error=error,
        audit_target=target_audit_path,
    )


def clear_proxy_quarantine(
    proxy_id: Any,
    *,
    event: str = "proxy_quarantine_cleared",
    path: Path | None = None,
    audit_path: Path | None = None,
    message: str = "",
) -> dict[str, Any]:
    target_path = Path(path) if path is not None else None
    target_audit_path = Path(audit_path) if audit_path is not None else None
    return _store_for_path(target_path).clear_quarantine(
        proxy_id,
        event=event,
        audit_target=target_audit_path,
        message=message,
    )


def record_proxy_test_success(
    proxy_id: Any,
    *,
    public_ip: str,
    latency_ms: float,
    path: Path | None = None,
    audit_path: Path | None = None,
) -> dict[str, Any]:
    return record_proxy_success(
        proxy_id,
        event="proxy_test",
        public_ip=public_ip,
        latency_ms=latency_ms,
        path=path,
        audit_path=audit_path,
        message=f"Test de proxy OK para {proxy_id}.",
    )


def record_proxy_test_failure(
    proxy_id: Any,
    *,
    error: str,
    path: Path | None = None,
    audit_path: Path | None = None,
) -> dict[str, Any]:
    return record_proxy_failure(
        proxy_id,
        event="proxy_test",
        error=error,
        path=path,
        audit_path=audit_path,
    )


def proxy_health_label(record: dict[str, Any]) -> str:
    normalized = normalize_proxy_record(record)
    if not bool(normalized.get("active", True)):
        return "Inactivo"
    quarantine_until = float(normalized.get("quarantine_until") or 0.0)
    if quarantine_until > _now_ts():
        remaining = max(0, int(quarantine_until - _now_ts()))
        return f"Cuarentena {remaining}s"
    success = _clean(normalized.get("last_success_at"))
    failure = _clean(normalized.get("last_failure_at"))
    latency_ms = _normalize_float(normalized.get("last_latency_ms"))
    if success and (not failure or success >= failure):
        if latency_ms is not None:
            return f"OK {latency_ms:.0f} ms"
        return "OK"
    last_error = _clean(normalized.get("last_error"))
    if last_error:
        return "Error"
    return "Sin test"
