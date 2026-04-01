from __future__ import annotations

import copy
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from core import responder as responder_module
from core.storage_atomic import load_json_file
from paths import storage_root
from src.inbox_diagnostics import normalize_reason_code
from src.inbox.message_timestamps import (
    annotate_message_timestamps,
    message_canonical_timestamp,
    message_sort_key,
)


class InboxStorage:
    THREADS_FILE = "inbox_threads.json"
    MESSAGES_FILE = "inbox_messages.json"
    STATE_FILE = "inbox_state.json"
    DATABASE_FILE = "inbox_rm.sqlite3"
    _MAX_MESSAGES_PER_THREAD = 80
    _MAX_ACTIVE_THREADS = 500
    _MAX_THREADS_PER_ACCOUNT = _MAX_ACTIVE_THREADS
    _LOCAL_TAG_FOLLOW_UP = "seguimiento"
    _RESPONSE_BLOCK_WINDOW_SECONDS = 180.0
    _MESSAGE_RECONCILE_WINDOW_SECONDS = 180.0
    _SYNTHETIC_MESSAGE_ID_PREFIXES = (
        "local-",
        "dom-confirmed-",
        "confirmed-",
        "thread-read-confirmed-",
    )
    _LOCAL_ONLY_MESSAGE_ID_PREFIXES = (
        "local-",
        "synthetic-",
    )
    _THREAD_RECORD_FIELDS = {
        "thread_key",
        "thread_id",
        "thread_href",
        "account_id",
        "alias_id",
        "account_alias",
        "recipient_username",
        "display_name",
        "owner",
        "bucket",
        "status",
        "stage_id",
        "followup_level",
        "last_inbound_at",
        "last_outbound_at",
        "last_action_type",
        "last_action_at",
        "last_pack_sent",
        "manual_lock",
        "manual_assignee",
        "is_deleted_from_view",
        "trash_at",
        "created_at",
        "updated_at",
        "last_message_text",
        "last_message_timestamp",
        "last_message_direction",
        "last_message_id",
        "unread_count",
        "needs_reply",
        "tags",
        "participants",
        "last_synced_at",
        "last_seen_text",
        "last_seen_at",
        "latest_customer_message_at",
    }

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = Path(root_dir)
        self._storage_dir = storage_root(self._root_dir)
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._threads_path = self._storage_dir / self.THREADS_FILE
        self._messages_path = self._storage_dir / self.MESSAGES_FILE
        self._state_path = self._storage_dir / self.STATE_FILE
        self._database_path = self._storage_dir / self.DATABASE_FILE
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._database_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure_connection()
        self._create_schema()
        self._ensure_schema_compat()
        self._create_indexes()
        self._migrate_legacy_json_if_needed()

    def _configure_connection(self) -> None:
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS inbox_threads (
                    thread_key TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    thread_href TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL,
                    alias_id TEXT NOT NULL DEFAULT '',
                    account_alias TEXT NOT NULL DEFAULT '',
                    recipient_username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    owner TEXT NOT NULL DEFAULT 'none',
                    bucket TEXT NOT NULL DEFAULT 'all',
                    status TEXT NOT NULL DEFAULT 'open',
                    stage_id TEXT NOT NULL DEFAULT 'initial',
                    followup_level INTEGER NOT NULL DEFAULT 0,
                    last_inbound_at REAL,
                    last_outbound_at REAL,
                    last_action_type TEXT NOT NULL DEFAULT '',
                    last_action_at REAL,
                    last_pack_sent TEXT NOT NULL DEFAULT '',
                    manual_lock INTEGER NOT NULL DEFAULT 0,
                    manual_assignee TEXT NOT NULL DEFAULT '',
                    is_deleted_from_view INTEGER NOT NULL DEFAULT 0,
                    trash_at REAL,
                    created_at REAL,
                    updated_at REAL,
                    last_message_text TEXT NOT NULL DEFAULT '',
                    last_message_timestamp REAL,
                    last_message_direction TEXT NOT NULL DEFAULT 'unknown',
                    last_message_id TEXT NOT NULL DEFAULT '',
                    unread_count INTEGER NOT NULL DEFAULT 0,
                    needs_reply INTEGER NOT NULL DEFAULT 0,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    participants_json TEXT NOT NULL DEFAULT '[]',
                    last_synced_at REAL,
                    last_seen_text TEXT NOT NULL DEFAULT '',
                    last_seen_at REAL,
                    latest_customer_message_at REAL
                );

                CREATE TABLE IF NOT EXISTS inbox_messages (
                    thread_key TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    account_id TEXT NOT NULL DEFAULT '',
                    message_id TEXT NOT NULL,
                    external_message_id TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    timestamp REAL,
                    direction TEXT NOT NULL DEFAULT 'unknown',
                    source TEXT NOT NULL DEFAULT 'manual',
                    pack_id TEXT NOT NULL DEFAULT '',
                    stage_id TEXT NOT NULL DEFAULT '',
                    created_at REAL,
                    confirmed_at REAL,
                    user_id TEXT NOT NULL DEFAULT '',
                    delivery_status TEXT NOT NULL DEFAULT 'sent',
                    sent_status TEXT NOT NULL DEFAULT 'sent',
                    local_echo INTEGER NOT NULL DEFAULT 0,
                    hidden_at REAL,
                    error_message TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(thread_key, block_id, ordinal),
                    FOREIGN KEY(thread_key) REFERENCES inbox_threads(thread_key) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS inbox_thread_state (
                    thread_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(thread_key) REFERENCES inbox_threads(thread_key) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS inbox_account_state (
                    account_id TEXT PRIMARY KEY,
                    session_marker TEXT NOT NULL DEFAULT '',
                    session_started_at REAL,
                    last_sync_at REAL,
                    last_error TEXT NOT NULL DEFAULT '',
                    thread_count INTEGER NOT NULL DEFAULT 0,
                    health_state TEXT NOT NULL DEFAULT 'healthy',
                    health_reason TEXT NOT NULL DEFAULT '',
                    health_updated_at REAL
                );

                CREATE TABLE IF NOT EXISTS thread_action_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    pack_id TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS inbox_send_queue_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    job_type TEXT NOT NULL DEFAULT '',
                    dedupe_key TEXT NOT NULL DEFAULT '',
                    thread_key TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    priority INTEGER NOT NULL DEFAULT 50,
                    state TEXT NOT NULL DEFAULT 'queued',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    scheduled_at REAL,
                    started_at REAL,
                    finished_at REAL,
                    error_message TEXT NOT NULL DEFAULT '',
                    failure_reason TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS inbox_deleted_threads (
                    thread_key TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL DEFAULT '',
                    deleted_at REAL NOT NULL,
                    last_activity_timestamp REAL
                );

                CREATE TABLE IF NOT EXISTS inbox_thread_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_key TEXT NOT NULL,
                    account_id TEXT NOT NULL DEFAULT '',
                    alias_id TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_alias_state (
                    alias_id TEXT PRIMARY KEY,
                    is_running INTEGER NOT NULL DEFAULT 0,
                    worker_state TEXT NOT NULL DEFAULT 'stopped',
                    current_account_id TEXT NOT NULL DEFAULT '',
                    current_turn_count INTEGER NOT NULL DEFAULT 0,
                    max_turns_per_account INTEGER NOT NULL DEFAULT 1,
                    delay_min_ms INTEGER NOT NULL DEFAULT 0,
                    delay_max_ms INTEGER NOT NULL DEFAULT 0,
                    mode TEXT NOT NULL DEFAULT 'both',
                    next_account_id TEXT NOT NULL DEFAULT '',
                    last_send_attempt_account_id TEXT NOT NULL DEFAULT '',
                    last_send_attempt_thread_key TEXT NOT NULL DEFAULT '',
                    last_send_attempt_job_id INTEGER NOT NULL DEFAULT 0,
                    last_send_attempt_job_type TEXT NOT NULL DEFAULT '',
                    last_send_attempt_at REAL,
                    last_send_attempt_outcome TEXT NOT NULL DEFAULT '',
                    last_send_attempt_reason_code TEXT NOT NULL DEFAULT '',
                    last_send_outcome TEXT NOT NULL DEFAULT '',
                    last_send_reason_code TEXT NOT NULL DEFAULT '',
                    last_send_reason TEXT NOT NULL DEFAULT '',
                    last_send_account_id TEXT NOT NULL DEFAULT '',
                    last_send_thread_key TEXT NOT NULL DEFAULT '',
                    last_send_job_id INTEGER NOT NULL DEFAULT 0,
                    last_send_job_type TEXT NOT NULL DEFAULT '',
                    last_send_at REAL,
                    last_send_exception_type TEXT NOT NULL DEFAULT '',
                    last_send_exception_message TEXT NOT NULL DEFAULT '',
                    last_heartbeat_at REAL,
                    last_error TEXT NOT NULL DEFAULT '',
                    stats_json TEXT NOT NULL DEFAULT '{}',
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS session_connector_state (
                    account_id TEXT PRIMARY KEY,
                    alias_id TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL DEFAULT 'offline',
                    proxy_key TEXT NOT NULL DEFAULT '',
                    last_heartbeat_at REAL,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS inbox_diagnostic_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    account_id TEXT NOT NULL DEFAULT '',
                    alias_id TEXT NOT NULL DEFAULT '',
                    thread_key TEXT NOT NULL DEFAULT '',
                    job_type TEXT NOT NULL DEFAULT '',
                    stage TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    outcome TEXT NOT NULL DEFAULT '',
                    reason_code TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT '',
                    file TEXT NOT NULL DEFAULT '',
                    function TEXT NOT NULL DEFAULT '',
                    line INTEGER NOT NULL DEFAULT 0,
                    exception_type TEXT NOT NULL DEFAULT '',
                    exception_message TEXT NOT NULL DEFAULT '',
                    traceback TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            self._conn.commit()

    def _ensure_schema_compat(self) -> None:
        with self._lock:
            self._ensure_table_columns(
                "inbox_threads",
                {
                    "thread_id": "TEXT NOT NULL DEFAULT ''",
                    "thread_href": "TEXT NOT NULL DEFAULT ''",
                    "account_id": "TEXT NOT NULL DEFAULT ''",
                    "alias_id": "TEXT NOT NULL DEFAULT ''",
                    "account_alias": "TEXT NOT NULL DEFAULT ''",
                    "recipient_username": "TEXT NOT NULL DEFAULT ''",
                    "display_name": "TEXT NOT NULL DEFAULT ''",
                    "owner": "TEXT NOT NULL DEFAULT 'none'",
                    "bucket": "TEXT NOT NULL DEFAULT 'all'",
                    "status": "TEXT NOT NULL DEFAULT 'open'",
                    "stage_id": "TEXT NOT NULL DEFAULT 'initial'",
                    "followup_level": "INTEGER NOT NULL DEFAULT 0",
                    "last_inbound_at": "REAL",
                    "last_outbound_at": "REAL",
                    "last_action_type": "TEXT NOT NULL DEFAULT ''",
                    "last_action_at": "REAL",
                    "last_pack_sent": "TEXT NOT NULL DEFAULT ''",
                    "manual_lock": "INTEGER NOT NULL DEFAULT 0",
                    "manual_assignee": "TEXT NOT NULL DEFAULT ''",
                    "is_deleted_from_view": "INTEGER NOT NULL DEFAULT 0",
                    "trash_at": "REAL",
                    "created_at": "REAL",
                    "updated_at": "REAL",
                    "last_message_text": "TEXT NOT NULL DEFAULT ''",
                    "last_message_timestamp": "REAL",
                    "last_message_direction": "TEXT NOT NULL DEFAULT 'unknown'",
                    "last_message_id": "TEXT NOT NULL DEFAULT ''",
                    "unread_count": "INTEGER NOT NULL DEFAULT 0",
                    "needs_reply": "INTEGER NOT NULL DEFAULT 0",
                    "tags_json": "TEXT NOT NULL DEFAULT '[]'",
                    "participants_json": "TEXT NOT NULL DEFAULT '[]'",
                    "last_synced_at": "REAL",
                    "last_seen_text": "TEXT NOT NULL DEFAULT ''",
                    "last_seen_at": "REAL",
                    "latest_customer_message_at": "REAL",
                },
            )
            self._ensure_table_columns(
                "inbox_messages",
                {
                    "account_id": "TEXT NOT NULL DEFAULT ''",
                    "message_id": "TEXT NOT NULL DEFAULT ''",
                    "external_message_id": "TEXT NOT NULL DEFAULT ''",
                    "text": "TEXT NOT NULL DEFAULT ''",
                    "timestamp": "REAL",
                    "direction": "TEXT NOT NULL DEFAULT 'unknown'",
                    "source": "TEXT NOT NULL DEFAULT 'manual'",
                    "pack_id": "TEXT NOT NULL DEFAULT ''",
                    "stage_id": "TEXT NOT NULL DEFAULT ''",
                    "created_at": "REAL",
                    "confirmed_at": "REAL",
                    "user_id": "TEXT NOT NULL DEFAULT ''",
                    "delivery_status": "TEXT NOT NULL DEFAULT 'sent'",
                    "sent_status": "TEXT NOT NULL DEFAULT 'sent'",
                    "local_echo": "INTEGER NOT NULL DEFAULT 0",
                    "hidden_at": "REAL",
                    "error_message": "TEXT NOT NULL DEFAULT ''",
                },
            )
            self._ensure_table_columns(
                "inbox_send_queue_jobs",
                {
                    "task_type": "TEXT NOT NULL DEFAULT ''",
                    "job_type": "TEXT NOT NULL DEFAULT ''",
                    "dedupe_key": "TEXT NOT NULL DEFAULT ''",
                    "thread_key": "TEXT NOT NULL DEFAULT ''",
                    "account_id": "TEXT NOT NULL DEFAULT ''",
                    "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                    "priority": "INTEGER NOT NULL DEFAULT 50",
                    "state": "TEXT NOT NULL DEFAULT 'queued'",
                    "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                    "scheduled_at": "REAL",
                    "started_at": "REAL",
                    "finished_at": "REAL",
                    "error_message": "TEXT NOT NULL DEFAULT ''",
                    "failure_reason": "TEXT NOT NULL DEFAULT ''",
                    "created_at": "REAL",
                    "updated_at": "REAL",
                },
            )
            self._ensure_table_columns(
                "inbox_thread_events",
                {
                    "account_id": "TEXT NOT NULL DEFAULT ''",
                    "alias_id": "TEXT NOT NULL DEFAULT ''",
                    "event_type": "TEXT NOT NULL DEFAULT ''",
                    "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                    "created_at": "REAL",
                },
            )
            self._ensure_table_columns(
                "runtime_alias_state",
                {
                    "is_running": "INTEGER NOT NULL DEFAULT 0",
                    "worker_state": "TEXT NOT NULL DEFAULT 'stopped'",
                    "current_account_id": "TEXT NOT NULL DEFAULT ''",
                    "current_turn_count": "INTEGER NOT NULL DEFAULT 0",
                    "max_turns_per_account": "INTEGER NOT NULL DEFAULT 1",
                    "delay_min_ms": "INTEGER NOT NULL DEFAULT 0",
                    "delay_max_ms": "INTEGER NOT NULL DEFAULT 0",
                    "mode": "TEXT NOT NULL DEFAULT 'both'",
                    "next_account_id": "TEXT NOT NULL DEFAULT ''",
                    "last_send_attempt_account_id": "TEXT NOT NULL DEFAULT ''",
                    "last_send_attempt_thread_key": "TEXT NOT NULL DEFAULT ''",
                    "last_send_attempt_job_id": "INTEGER NOT NULL DEFAULT 0",
                    "last_send_attempt_job_type": "TEXT NOT NULL DEFAULT ''",
                    "last_send_attempt_at": "REAL",
                    "last_send_attempt_outcome": "TEXT NOT NULL DEFAULT ''",
                    "last_send_attempt_reason_code": "TEXT NOT NULL DEFAULT ''",
                    "last_send_outcome": "TEXT NOT NULL DEFAULT ''",
                    "last_send_reason_code": "TEXT NOT NULL DEFAULT ''",
                    "last_send_reason": "TEXT NOT NULL DEFAULT ''",
                    "last_send_account_id": "TEXT NOT NULL DEFAULT ''",
                    "last_send_thread_key": "TEXT NOT NULL DEFAULT ''",
                    "last_send_job_id": "INTEGER NOT NULL DEFAULT 0",
                    "last_send_job_type": "TEXT NOT NULL DEFAULT ''",
                    "last_send_at": "REAL",
                    "last_send_exception_type": "TEXT NOT NULL DEFAULT ''",
                    "last_send_exception_message": "TEXT NOT NULL DEFAULT ''",
                    "last_heartbeat_at": "REAL",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "stats_json": "TEXT NOT NULL DEFAULT '{}'",
                    "updated_at": "REAL",
                },
            )
            self._ensure_table_columns(
                "session_connector_state",
                {
                    "alias_id": "TEXT NOT NULL DEFAULT ''",
                    "state": "TEXT NOT NULL DEFAULT 'offline'",
                    "proxy_key": "TEXT NOT NULL DEFAULT ''",
                    "last_heartbeat_at": "REAL",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "updated_at": "REAL",
                },
            )
            self._ensure_table_columns(
                "inbox_diagnostic_events",
                {
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "account_id": "TEXT NOT NULL DEFAULT ''",
                    "alias_id": "TEXT NOT NULL DEFAULT ''",
                    "thread_key": "TEXT NOT NULL DEFAULT ''",
                    "job_type": "TEXT NOT NULL DEFAULT ''",
                    "stage": "TEXT NOT NULL DEFAULT ''",
                    "event_type": "TEXT NOT NULL DEFAULT ''",
                    "outcome": "TEXT NOT NULL DEFAULT ''",
                    "reason_code": "TEXT NOT NULL DEFAULT ''",
                    "reason": "TEXT NOT NULL DEFAULT ''",
                    "file": "TEXT NOT NULL DEFAULT ''",
                    "function": "TEXT NOT NULL DEFAULT ''",
                    "line": "INTEGER NOT NULL DEFAULT 0",
                    "exception_type": "TEXT NOT NULL DEFAULT ''",
                    "exception_message": "TEXT NOT NULL DEFAULT ''",
                    "traceback": "TEXT NOT NULL DEFAULT ''",
                    "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                },
            )
            self._backfill_alias_ids()
            self._conn.commit()

    def _ensure_table_columns(self, table_name: str, columns: dict[str, str]) -> None:
        existing = self._table_columns(table_name)
        if not existing:
            return
        for column_name, definition in columns.items():
            if column_name.lower() in existing:
                continue
            self._conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
            existing.add(column_name.lower())

    def _create_indexes(self) -> None:
        with self._lock:
            self._create_index_if_possible(
                "inbox_threads_account_ts_idx",
                "inbox_threads",
                "account_id, last_message_timestamp DESC, thread_key",
                ("account_id", "last_message_timestamp", "thread_key"),
            )
            self._create_index_if_possible(
                "inbox_threads_alias_bucket_idx",
                "inbox_threads",
                "alias_id, bucket, owner, status, thread_key",
                ("alias_id", "bucket", "owner", "status", "thread_key"),
            )
            self._create_index_if_possible(
                "inbox_messages_thread_ts_idx",
                "inbox_messages",
                "thread_key, timestamp, ordinal",
                ("thread_key", "timestamp", "ordinal"),
            )
            self._create_index_if_possible(
                "thread_action_memory_lookup_idx",
                "thread_action_memory",
                "account_id, thread_id, created_at DESC",
                ("account_id", "thread_id", "created_at"),
            )
            self._create_index_if_possible(
                "inbox_send_queue_state_idx",
                "inbox_send_queue_jobs",
                "state, priority DESC, created_at",
                ("state", "priority", "created_at"),
            )
            self._create_index_if_possible(
                "inbox_send_queue_account_idx",
                "inbox_send_queue_jobs",
                "account_id, state, priority DESC, created_at",
                ("account_id", "state", "priority", "created_at"),
            )
            self._create_index_if_possible(
                "inbox_deleted_threads_account_idx",
                "inbox_deleted_threads",
                "account_id, deleted_at DESC",
                ("account_id", "deleted_at"),
            )
            self._create_index_if_possible(
                "inbox_thread_events_thread_idx",
                "inbox_thread_events",
                "thread_key, created_at DESC",
                ("thread_key", "created_at"),
            )
            self._create_index_if_possible(
                "inbox_diagnostic_events_created_idx",
                "inbox_diagnostic_events",
                "created_at DESC, id DESC",
                ("created_at",),
            )
            self._create_index_if_possible(
                "inbox_diagnostic_events_thread_idx",
                "inbox_diagnostic_events",
                "thread_key, created_at DESC",
                ("thread_key", "created_at"),
            )
            self._create_index_if_possible(
                "inbox_diagnostic_events_account_idx",
                "inbox_diagnostic_events",
                "account_id, created_at DESC",
                ("account_id", "created_at"),
            )
            self._conn.commit()

    def _create_index_if_possible(
        self,
        index_name: str,
        table_name: str,
        columns_sql: str,
        required_columns: tuple[str, ...],
    ) -> None:
        if not self._table_has_columns(table_name, *required_columns):
            return
        self._conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns_sql})")

    def _table_exists(self, table_name: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, table_name: str) -> set[str]:
        if not self._table_exists(table_name):
            return set()
        return {
            str(row["name"] or "").strip().lower()
            for row in self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }

    def _table_has_columns(self, table_name: str, *required_columns: str) -> bool:
        existing = self._table_columns(table_name)
        return all(str(column or "").strip().lower() in existing for column in required_columns)

    def _backfill_alias_ids(self) -> None:
        if not self._table_has_columns("inbox_threads", "alias_id", "account_alias"):
            return
        self._conn.execute(
            """
            UPDATE inbox_threads
            SET alias_id = TRIM(COALESCE(account_alias, ''))
            WHERE TRIM(COALESCE(alias_id, '')) = ''
              AND TRIM(COALESCE(account_alias, '')) <> ''
            """
        )

    def _migrate_legacy_json_if_needed(self) -> None:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS total FROM inbox_threads").fetchone()
            if int(self._row_value(row, "total", 0) or 0):
                return
        if not (self._threads_path.exists() or self._messages_path.exists() or self._state_path.exists()):
            return

        threads_payload = load_json_file(self._threads_path, {"threads": {}}, label="inbox.legacy.threads")
        messages_payload = load_json_file(self._messages_path, {"messages": {}}, label="inbox.legacy.messages")
        state_payload = load_json_file(self._state_path, {"accounts": {}, "threads": {}}, label="inbox.legacy.state")

        raw_threads = threads_payload.get("threads") if isinstance(threads_payload, dict) else {}
        raw_messages = messages_payload.get("messages") if isinstance(messages_payload, dict) else {}
        raw_state_accounts = state_payload.get("accounts") if isinstance(state_payload, dict) else {}
        raw_state_threads = state_payload.get("threads") if isinstance(state_payload, dict) else {}

        if isinstance(raw_threads, dict):
            migrated_threads: list[dict[str, Any]] = []
            for fallback_key, item in raw_threads.items():
                if not isinstance(item, dict):
                    continue
                payload = dict(item)
                payload.setdefault("thread_key", str(fallback_key or "").strip())
                migrated_threads.append(payload)
            self.upsert_threads(migrated_threads)
        if isinstance(raw_messages, dict):
            for thread_key, rows in raw_messages.items():
                if isinstance(rows, list):
                    self.replace_messages(str(thread_key or "").strip(), rows)
        if isinstance(raw_state_threads, dict):
            for thread_key, state in raw_state_threads.items():
                if isinstance(state, dict):
                    self.update_thread_state(str(thread_key or "").strip(), dict(state))
        if isinstance(raw_state_accounts, dict):
            for account_id, state in raw_state_accounts.items():
                if not isinstance(state, dict):
                    continue
                clean_account = self._clean_account_id(account_id)
                if not clean_account:
                    continue
                self.register_account_sync(
                    clean_account,
                    last_error=str(state.get("last_error") or "").strip(),
                    thread_count=state.get("thread_count"),
                )
                self.prepare_account_session(
                    clean_account,
                    session_marker=str(state.get("session_marker") or f"legacy:{clean_account}").strip(),
                    started_at=self._coerce_timestamp(state.get("session_started_at")),
                )
                self.set_account_health(
                    clean_account,
                    str(state.get("health_state") or "healthy"),
                    reason=str(state.get("health_reason") or "").strip(),
                    updated_at=self._coerce_timestamp(state.get("health_updated_at")),
                )

    @staticmethod
    def _clean_account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()

    @staticmethod
    def _coerce_timestamp(value: Any) -> float | None:
        try:
            stamp = float(value)
        except Exception:
            return None
        return stamp if stamp > 0 else None

    @staticmethod
    def _coerce_non_negative_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except Exception:
            return 0

    @staticmethod
    def _normalize_stage_id(value: Any, *, default: str = "initial") -> str:
        return str(value or default).strip() or default

    @staticmethod
    def _split_thread_key(thread_key: str) -> tuple[str, str]:
        clean_key = str(thread_key or "").strip()
        if ":" not in clean_key:
            return "", ""
        raw_account, raw_thread = clean_key.split(":", 1)
        return InboxStorage._clean_account_id(raw_account), str(raw_thread or "").strip()

    @staticmethod
    def _normalize_participants(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        rows: list[str] = []
        seen: set[str] = set()
        for raw in value:
            text = str(raw or "").strip().lstrip("@")
            key = text.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            rows.append(text)
        return rows

    @staticmethod
    def _normalize_tags(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        rows: list[str] = []
        seen: set[str] = set()
        for raw in value:
            text = str(raw or "").strip()
            key = text.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            rows.append(text)
        return rows

    @staticmethod
    def _encode_json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _decode_json_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return list(value)
        text = str(value or "").strip()
        if not text:
            return []
        try:
            payload = json.loads(text)
        except Exception:
            return []
        return list(payload) if isinstance(payload, list) else []

    @staticmethod
    def _decode_json_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        text = str(value or "").strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    @staticmethod
    def _normalize_delivery_status(value: Any) -> str:
        status = str(value or "").strip().lower() or "sent"
        if status == "failed":
            status = "error"
        if status not in {"pending", "sending", "sent", "error"}:
            return "sent"
        return status

    @classmethod
    def _is_synthetic_message_id(cls, value: Any) -> bool:
        message_id = str(value or "").strip().lower()
        if not message_id:
            return False
        return any(message_id.startswith(prefix) for prefix in cls._SYNTHETIC_MESSAGE_ID_PREFIXES)

    @classmethod
    def _looks_like_local_only_message_id(cls, value: Any) -> bool:
        message_id = str(value or "").strip().lower()
        if not message_id:
            return False
        return any(message_id.startswith(prefix) for prefix in cls._LOCAL_ONLY_MESSAGE_ID_PREFIXES)

    @classmethod
    def _flow_state_anchor_timestamp(cls, flow_state: dict[str, Any] | None) -> float | None:
        if not isinstance(flow_state, dict):
            return None
        return cls._coerce_timestamp(flow_state.get("followup_anchor_ts")) or cls._coerce_timestamp(
            flow_state.get("last_outbound_ts")
        )

    @classmethod
    def _confirmed_outbound_message_for_stage(cls, message: dict[str, Any], *, stage_id: str) -> bool:
        if str(message.get("direction") or "").strip().lower() != "outbound":
            return False
        if not responder_module._flow_stage_ids_match(message.get("stage_id"), stage_id):
            return False
        confirmation_ts = cls._coerce_timestamp(message.get("confirmed_at")) or cls._coerce_timestamp(message.get("timestamp"))
        if confirmation_ts is None:
            return False
        delivery_status = str(message.get("delivery_status") or "").strip().lower()
        sent_status = str(message.get("sent_status") or "").strip().lower()
        return delivery_status == "sent" or sent_status in {"sent", "confirmed"}

    @classmethod
    def _message_anchor_timestamp(cls, message: dict[str, Any]) -> float | None:
        del cls
        return message_canonical_timestamp(message)

    @classmethod
    def _message_display_timestamp(cls, message: dict[str, Any]) -> float | None:
        del cls
        return message_canonical_timestamp(message)

    @classmethod
    def _message_sort_key(cls, message: dict[str, Any]) -> tuple[float, float, str, int]:
        del cls
        return message_sort_key(message)

    @classmethod
    def _message_has_real_identity(cls, message: dict[str, Any]) -> bool:
        for value in (
            message.get("external_message_id"),
            message.get("message_id"),
        ):
            clean_value = str(value or "").strip()
            if clean_value and not cls._is_synthetic_message_id(clean_value):
                return True
        return False

    @classmethod
    def _message_identity_tokens(cls, message: dict[str, Any]) -> set[str]:
        tokens: set[str] = set()
        message_id = str(message.get("message_id") or "").strip()
        external_message_id = str(message.get("external_message_id") or "").strip()
        if message_id:
            tokens.add(f"message:{message_id}")
        if external_message_id:
            tokens.add(f"external:{external_message_id}")
        return tokens

    @classmethod
    def _message_status_rank(cls, message: dict[str, Any]) -> int:
        return {
            "error": 0,
            "pending": 1,
            "sending": 2,
            "sent": 3,
        }.get(cls._normalize_delivery_status(message.get("delivery_status")), 0)

    @classmethod
    def _message_priority(cls, message: dict[str, Any]) -> tuple[int, int, int, int, float]:
        return (
            cls._message_has_real_identity(message),
            cls._message_status_rank(message),
            1 if cls._coerce_timestamp(message.get("timestamp")) is not None else 0,
            0 if bool(message.get("local_echo")) else 1,
            cls._message_anchor_timestamp(message) or 0.0,
        )

    @classmethod
    def _message_text_key(cls, message: dict[str, Any]) -> str:
        return str(message.get("text") or "").strip()

    @classmethod
    def _messages_match_for_reconciliation(
        cls,
        left: dict[str, Any],
        right: dict[str, Any],
    ) -> bool:
        left_direction = str(left.get("direction") or "").strip().lower()
        right_direction = str(right.get("direction") or "").strip().lower()
        if left_direction != right_direction:
            return False
        if cls._message_identity_tokens(left) & cls._message_identity_tokens(right):
            return True
        if left_direction != "outbound":
            return False
        left_text = cls._message_text_key(left)
        right_text = cls._message_text_key(right)
        if not left_text or left_text != right_text:
            return False
        left_anchor = cls._message_anchor_timestamp(left)
        right_anchor = cls._message_anchor_timestamp(right)
        if left_anchor is None or right_anchor is None:
            return False
        if abs(left_anchor - right_anchor) > cls._MESSAGE_RECONCILE_WINDOW_SECONDS:
            return False
        left_needs_reconcile = bool(left.get("local_echo")) or not cls._message_has_real_identity(left)
        right_needs_reconcile = bool(right.get("local_echo")) or not cls._message_has_real_identity(right)
        return left_needs_reconcile or right_needs_reconcile

    @classmethod
    def _pick_message_value(cls, preferred: dict[str, Any], fallback: dict[str, Any], key: str) -> Any:
        preferred_value = preferred.get(key)
        if preferred_value not in (None, "", [], ()):
            return preferred_value
        return fallback.get(key)

    @classmethod
    def _merge_reconciled_messages(
        cls,
        left: dict[str, Any],
        right: dict[str, Any],
    ) -> dict[str, Any]:
        preferred = left if cls._message_priority(left) >= cls._message_priority(right) else right
        fallback = right if preferred is left else left
        merged = dict(preferred)

        preferred_anchor = cls._message_anchor_timestamp(preferred)
        fallback_anchor = cls._message_anchor_timestamp(fallback)
        preferred_has_real_identity = cls._message_has_real_identity(preferred)
        fallback_has_real_identity = cls._message_has_real_identity(fallback)

        for key in ("account_id", "source", "pack_id", "stage_id", "user_id", "text"):
            merged[key] = cls._pick_message_value(preferred, fallback, key)

        if fallback_has_real_identity and not preferred_has_real_identity:
            merged["message_id"] = str(
                fallback.get("message_id") or fallback.get("external_message_id") or preferred.get("message_id") or ""
            ).strip()
            merged["external_message_id"] = str(
                fallback.get("external_message_id") or fallback.get("message_id") or preferred.get("external_message_id") or ""
            ).strip()
        else:
            merged["message_id"] = str(
                preferred.get("message_id") or preferred.get("external_message_id") or fallback.get("message_id") or ""
            ).strip()
            merged["external_message_id"] = str(
                preferred.get("external_message_id") or preferred.get("message_id") or fallback.get("external_message_id") or ""
            ).strip()

        resolved_timestamp = cls._coerce_timestamp(preferred.get("timestamp"))
        if fallback_has_real_identity and (
            resolved_timestamp is None
            or (not preferred_has_real_identity and cls._coerce_timestamp(fallback.get("timestamp")) is not None)
        ):
            resolved_timestamp = cls._coerce_timestamp(fallback.get("timestamp"))
        if resolved_timestamp is None:
            resolved_timestamp = max(
                (stamp for stamp in (preferred_anchor, fallback_anchor) if stamp is not None),
                default=None,
            )
        merged["timestamp"] = resolved_timestamp

        created_candidates = [
            stamp
            for stamp in (
                cls._coerce_timestamp(preferred.get("created_at")),
                cls._coerce_timestamp(fallback.get("created_at")),
            )
            if stamp is not None
        ]
        merged["created_at"] = min(created_candidates) if created_candidates else resolved_timestamp

        confirmed_candidates = [
            stamp
            for stamp in (
                cls._coerce_timestamp(preferred.get("confirmed_at")),
                cls._coerce_timestamp(fallback.get("confirmed_at")),
                resolved_timestamp if cls._message_status_rank(preferred) >= 3 or cls._message_status_rank(fallback) >= 3 else None,
            )
            if stamp is not None
        ]
        merged["confirmed_at"] = max(confirmed_candidates) if confirmed_candidates else None

        if cls._message_status_rank(preferred) >= cls._message_status_rank(fallback):
            merged["delivery_status"] = cls._normalize_delivery_status(preferred.get("delivery_status"))
        else:
            merged["delivery_status"] = cls._normalize_delivery_status(fallback.get("delivery_status"))
        merged["sent_status"] = cls._normalize_job_state(
            preferred.get("sent_status") or fallback.get("sent_status") or merged.get("delivery_status")
        )
        if cls._normalize_delivery_status(merged.get("delivery_status")) == "sent":
            merged["local_echo"] = False
            merged["error_message"] = ""
        else:
            merged["local_echo"] = bool(preferred.get("local_echo")) and bool(fallback.get("local_echo"))
            merged["error_message"] = str(
                preferred.get("error_message") or fallback.get("error_message") or ""
            ).strip()
        merged["hidden_at"] = max(
            (
                stamp
                for stamp in (
                    cls._coerce_timestamp(preferred.get("hidden_at")),
                    cls._coerce_timestamp(fallback.get("hidden_at")),
                )
                if stamp is not None
            ),
            default=None,
        )
        merged["block_id"] = str(merged.get("message_id") or merged.get("block_id") or "").strip()
        return merged

    @classmethod
    def _visible_blocks(cls, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            annotate_message_timestamps(block)
            for block in blocks
            if cls._coerce_timestamp(block.get("hidden_at")) is None
        ]

    @classmethod
    def _canonicalize_blocks(cls, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = [dict(block) for block in blocks if isinstance(block, dict)]
        ordered.sort(key=cls._message_sort_key)
        reconciled: list[dict[str, Any]] = []
        for candidate in ordered:
            merged = False
            for index, existing in enumerate(reconciled):
                if not cls._messages_match_for_reconciliation(existing, candidate):
                    continue
                reconciled[index] = cls._merge_reconciled_messages(existing, candidate)
                merged = True
                break
            if not merged:
                reconciled.append(dict(candidate))
        reconciled.sort(key=cls._message_sort_key)
        return [annotate_message_timestamps(block) for block in reconciled[-cls._MAX_MESSAGES_PER_THREAD :]]

    @staticmethod
    def _normalize_thread_owner(value: Any) -> str:
        owner = str(value or "").strip().lower() or "none"
        if owner not in {"auto", "manual", "none"}:
            return "none"
        return owner

    @staticmethod
    def _normalize_thread_bucket(value: Any) -> str:
        bucket = str(value or "").strip().lower() or "all"
        if bucket in {"schedule", "scheduled"}:
            bucket = "qualified"
        if bucket not in {"all", "qualified", "disqualified"}:
            return "all"
        return bucket

    @staticmethod
    def _normalize_thread_status(value: Any) -> str:
        status = str(value or "").strip().lower() or "open"
        if status not in {
            "open",
            "pending",
            "replied",
            "followup_sent",
            "paused",
            "closed",
            "failed",
            "pack_sent",
            "active",
            "error",
        }:
            return "open"
        return status

    @staticmethod
    def _normalize_ui_status(value: Any) -> str:
        status = str(value or "").strip().lower()
        if not status:
            return ""
        if status not in {
            "open",
            "pending",
            "replied",
            "followup_sent",
            "paused",
            "closed",
            "failed",
            "pack_sent",
            "active",
            "error",
            "needs_reply",
        }:
            return ""
        return status

    @classmethod
    def _normalize_thread_state_payload(cls, raw: Any) -> dict[str, Any]:
        payload = cls._decode_json_dict(raw)
        if not payload:
            return {}
        normalized = dict(payload)
        normalized.pop("stage_id", None)
        normalized.pop("followup_level", None)
        legacy_ui_status = normalized.get("ui_status")
        if legacy_ui_status in (None, ""):
            legacy_ui_status = normalized.get("status")
        normalized.pop("status", None)
        ui_status = cls._normalize_ui_status(legacy_ui_status)
        if ui_status:
            normalized["ui_status"] = ui_status
        else:
            normalized.pop("ui_status", None)
        flow_state = cls._normalize_flow_state_payload(normalized.get("flow_state"))
        if flow_state:
            normalized["flow_state"] = flow_state
        else:
            normalized.pop("flow_state", None)
        return normalized

    @classmethod
    def _normalize_flow_state_payload(cls, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            return {}
        normalized = dict(raw)
        stage_id = str(normalized.get("stage_id") or "").strip()
        if stage_id:
            normalized["stage_id"] = stage_id
        else:
            normalized.pop("stage_id", None)
        normalized["followup_level"] = cls._coerce_non_negative_int(normalized.get("followup_level"))
        return normalized

    @staticmethod
    def _normalize_message_source(value: Any) -> str:
        source = str(value or "").strip().lower() or "manual"
        if source == "manual_pack":
            return "manual"
        if source not in {"auto", "manual", "followup", "campaign", "inbound"}:
            return "manual"
        return source

    @staticmethod
    def _normalize_job_state(value: Any) -> str:
        state = str(value or "").strip().lower() or "queued"
        remapped = {
            "pending": "queued",
            "sending": "processing",
            "error": "failed",
            "confirmed": "confirmed",
        }
        state = remapped.get(state, state)
        if state not in {"queued", "processing", "sent", "confirmed", "failed", "cancelled"}:
            return "queued"
        return state

    @staticmethod
    def _normalize_job_type(value: Any) -> str:
        job_type = str(value or "").strip().lower()
        legacy_map = {
            "send_message": "manual_reply",
            "send_pack": "manual_pack",
        }
        job_type = legacy_map.get(job_type, job_type)
        if job_type not in {"manual_reply", "manual_pack", "auto_reply", "followup"}:
            return "manual_reply"
        return job_type

    @staticmethod
    def _priority_for_job_type(job_type: str) -> int:
        mapping = {
            "manual_reply": 100,
            "manual_pack": 80,
            "auto_reply": 60,
            "followup": 40,
        }
        return int(mapping.get(str(job_type or "").strip().lower(), 50))

    @staticmethod
    def _normalize_runtime_mode(value: Any) -> str:
        mode = str(value or "").strip().lower() or "both"
        aliases = {
            "autoresponder": "auto",
            "reply": "auto",
            "follow-up": "followup",
        }
        mode = aliases.get(mode, mode)
        if mode not in {"auto", "followup", "both"}:
            return "both"
        return mode

    @staticmethod
    def _normalize_health_state(value: Any) -> str:
        state = str(value or "").strip().lower() or "unknown"
        if state not in {
            "healthy",
            "login_required",
            "checkpoint",
            "suspended",
            "banned",
            "proxy_error",
            "unknown",
        }:
            return "unknown"
        return state

    @staticmethod
    def _row_value(row: sqlite3.Row | dict[str, Any] | None, key: str, default: Any = None) -> Any:
        if row is None:
            return default
        if isinstance(row, dict):
            return row.get(key, default)
        try:
            return row[key]
        except Exception:
            return default

    def _normalize_thread_record(self, raw: Any, *, fallback_key: str = "") -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        thread_key = str(raw.get("thread_key") or fallback_key or "").strip()
        thread_id = str(raw.get("thread_id") or "").strip()
        account_id = self._clean_account_id(raw.get("account_id") or raw.get("account"))
        if thread_key and (not thread_id or not account_id):
            fallback_account, fallback_thread = self._split_thread_key(thread_key)
            account_id = account_id or fallback_account
            thread_id = thread_id or fallback_thread
        if not thread_key and account_id and thread_id:
            thread_key = f"{account_id}:{thread_id}"
        if not thread_key or not thread_id or not account_id:
            return None
        participants = self._normalize_participants(raw.get("participants"))
        recipient_username = str(raw.get("recipient_username") or "").strip().lstrip("@")
        if not recipient_username and participants:
            recipient_username = participants[0]
        tags = self._normalize_tags(raw.get("tags"))
        last_direction = str(raw.get("last_message_direction") or "unknown").strip().lower() or "unknown"
        try:
            unread_count = max(0, int(raw.get("unread_count") or 0))
        except Exception:
            unread_count = 0
        needs_reply_raw = raw.get("needs_reply")
        needs_reply = bool(needs_reply_raw) if needs_reply_raw is not None else last_direction == "inbound"
        return {
            "thread_key": thread_key,
            "thread_id": thread_id,
            "thread_href": str(raw.get("thread_href") or "").strip()
            or f"https://www.instagram.com/direct/t/{thread_id}/",
            "account_id": account_id,
            "alias_id": str(raw.get("alias_id") or raw.get("account_alias") or "").strip(),
            "account_alias": str(raw.get("account_alias") or "").strip(),
            "recipient_username": recipient_username,
            "display_name": str(raw.get("display_name") or raw.get("title") or recipient_username or thread_id).strip(),
            "owner": self._normalize_thread_owner(raw.get("owner")),
            "bucket": self._normalize_thread_bucket(raw.get("bucket")),
            "status": self._normalize_thread_status(raw.get("status") or raw.get("operational_status")),
            "stage_id": str(raw.get("stage_id") or raw.get("stage") or "initial").strip() or "initial",
            "followup_level": max(0, int(raw.get("followup_level") or 0)),
            "last_inbound_at": self._coerce_timestamp(raw.get("last_inbound_at")),
            "last_outbound_at": self._coerce_timestamp(raw.get("last_outbound_at")),
            "last_action_type": str(raw.get("last_action_type") or "").strip(),
            "last_action_at": self._coerce_timestamp(raw.get("last_action_at")),
            "last_pack_sent": str(raw.get("last_pack_sent") or "").strip(),
            "manual_lock": bool(raw.get("manual_lock", False)),
            "manual_assignee": str(raw.get("manual_assignee") or "").strip(),
            "is_deleted_from_view": bool(raw.get("is_deleted_from_view", False)),
            "trash_at": self._coerce_timestamp(raw.get("trash_at")),
            "created_at": self._coerce_timestamp(raw.get("created_at")) or self._coerce_timestamp(raw.get("confirmed_at")) or self._coerce_timestamp(raw.get("timestamp")),
            "updated_at": self._coerce_timestamp(raw.get("updated_at")) or time.time(),
            "last_message_text": str(raw.get("last_message_text") or "").strip(),
            "last_message_timestamp": self._coerce_timestamp(raw.get("last_message_timestamp")),
            "last_message_direction": last_direction,
            "last_message_id": str(raw.get("last_message_id") or "").strip(),
            "unread_count": unread_count,
            "needs_reply": 1 if needs_reply else 0,
            "tags": tags,
            "participants": participants,
            "last_synced_at": self._coerce_timestamp(raw.get("last_synced_at")) or time.time(),
            "last_seen_text": str(raw.get("last_seen_text") or "").strip(),
            "last_seen_at": self._coerce_timestamp(raw.get("last_seen_at")),
            "latest_customer_message_at": self._coerce_timestamp(raw.get("latest_customer_message_at")),
        }

    def _normalize_message_record(self, raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        message_id = str(raw.get("message_id") or raw.get("id") or "").strip()
        if not message_id:
            return None
        direction = str(raw.get("direction") or "").strip().lower() or "unknown"
        if direction not in {"inbound", "outbound", "unknown"}:
            direction = "unknown"
        return {
            "message_id": message_id,
            "external_message_id": str(raw.get("external_message_id") or message_id).strip(),
            "text": str(raw.get("text") or "").strip(),
            "timestamp": self._coerce_timestamp(raw.get("timestamp")),
            "direction": direction,
            "account_id": self._clean_account_id(raw.get("account_id") or raw.get("account")),
            "source": self._normalize_message_source(
                raw.get("source") or ("inbound" if direction == "inbound" else "manual")
            ),
            "pack_id": str(raw.get("pack_id") or "").strip(),
            "stage_id": str(raw.get("stage_id") or "").strip(),
            "created_at": self._coerce_timestamp(raw.get("created_at")) or self._coerce_timestamp(raw.get("confirmed_at")) or self._coerce_timestamp(raw.get("timestamp")),
            "confirmed_at": self._coerce_timestamp(raw.get("confirmed_at")),
            "user_id": str(raw.get("user_id") or "").strip(),
            "delivery_status": self._normalize_delivery_status(raw.get("delivery_status")),
            "sent_status": self._normalize_job_state(raw.get("sent_status") or raw.get("delivery_status")),
            "local_echo": bool(raw.get("local_echo", False)),
            "hidden_at": self._coerce_timestamp(raw.get("hidden_at")),
            "error_message": str(raw.get("error_message") or "").strip(),
        }

    def _load_thread_state(self, thread_key: str) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT state_json FROM inbox_thread_state WHERE thread_key = ?",
            (str(thread_key or "").strip(),),
        ).fetchone()
        if row is None:
            return {}
        return self._normalize_thread_state_payload(row["state_json"])

    def _save_thread_state(self, thread_key: str, state: dict[str, Any]) -> None:
        normalized = self._normalize_thread_state_payload(state)
        self._conn.execute(
            """
            INSERT INTO inbox_thread_state(thread_key, state_json)
            SELECT ?, ?
            WHERE EXISTS(SELECT 1 FROM inbox_threads WHERE thread_key = ?)
            ON CONFLICT(thread_key) DO UPDATE SET state_json = excluded.state_json
            """,
            (thread_key, self._encode_json(normalized), thread_key),
        )

    def _thread_stage_has_operational_evidence_locked(
        self,
        thread_key: str,
        *,
        stage_id: str,
        thread: dict[str, Any],
        state: dict[str, Any] | None = None,
    ) -> bool:
        clean_stage = str(stage_id or "").strip()
        if not clean_stage:
            return False
        flow_state = self._normalize_flow_state_payload(
            (state or {}).get("flow_state") if isinstance(state, dict) else thread.get("flow_state")
        )
        flow_anchor_ts = self._flow_state_anchor_timestamp(flow_state)
        last_outbound_at = self._coerce_timestamp(thread.get("last_outbound_at"))
        if (
            responder_module._is_initial_flow_stage_id(clean_stage)
            and responder_module._flow_stage_ids_match(flow_state.get("stage_id"), clean_stage)
            and flow_anchor_ts is not None
        ):
            return True
        if (
            responder_module._flow_stage_ids_match(flow_state.get("stage_id"), clean_stage)
            and flow_anchor_ts is not None
            and last_outbound_at is not None
        ):
            return True
        last_direction = str(thread.get("last_message_direction") or "").strip().lower()
        last_message_id = str(thread.get("last_message_id") or "").strip()
        thread_anchor_ts = last_outbound_at or self._coerce_timestamp(thread.get("last_message_timestamp"))
        if (
            self._normalize_stage_id(thread.get("stage_id")) == clean_stage
            and last_direction == "outbound"
            and last_message_id
            and thread_anchor_ts is not None
            and not self._looks_like_local_only_message_id(last_message_id)
        ):
            return True
        for block in reversed(self._load_blocks(thread_key)):
            if self._confirmed_outbound_message_for_stage(block, stage_id=clean_stage):
                return True
        return False

    def _reconciled_followup_level_locked(
        self,
        thread_key: str,
        *,
        stage_id: str,
        thread: dict[str, Any],
        state: dict[str, Any] | None = None,
    ) -> int:
        if not self._thread_stage_has_operational_evidence_locked(
            thread_key,
            stage_id=stage_id,
            thread=thread,
            state=state,
        ):
            return 0
        flow_state = self._normalize_flow_state_payload(
            (state or {}).get("flow_state") if isinstance(state, dict) else thread.get("flow_state")
        )
        if not responder_module._flow_stage_ids_match(flow_state.get("stage_id"), stage_id):
            return 0
        if self._flow_state_anchor_timestamp(flow_state) is None:
            return 0
        return self._coerce_non_negative_int(flow_state.get("followup_level"))

    def _reconcile_thread_state_locked(
        self,
        thread_key: str,
        *,
        thread: dict[str, Any],
        state: dict[str, Any] | None,
        stage_evidenced: bool | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_thread_state_payload(state)
        flow_state = self._normalize_flow_state_payload(normalized.get("flow_state"))
        if not flow_state:
            normalized.pop("flow_state", None)
            return normalized
        canonical_stage_id = self._normalize_stage_id(thread.get("stage_id"))
        canonical_followup_level = self._coerce_non_negative_int(thread.get("followup_level"))
        if stage_evidenced is None:
            stage_evidenced = self._thread_stage_has_operational_evidence_locked(
                thread_key,
                stage_id=canonical_stage_id,
                thread=thread,
                state=normalized,
            )
        flow_state["stage_id"] = canonical_stage_id
        flow_state["followup_level"] = canonical_followup_level if stage_evidenced else 0
        if not stage_evidenced:
            flow_state["last_outbound_ts"] = None
            flow_state["followup_anchor_ts"] = None
        normalized["flow_state"] = flow_state
        return normalized

    def _account_overlay(self, account_id: str) -> dict[str, Any]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return {"account_health": "unknown", "account_health_reason": ""}
        row = self._conn.execute(
            """
            SELECT last_sync_at, last_error, thread_count, health_state, health_reason, health_updated_at
            FROM inbox_account_state
            WHERE account_id = ?
            """,
            (clean_account,),
        ).fetchone()
        if row is None:
            return {"account_health": "healthy", "account_health_reason": ""}
        return {
            "account_health": self._normalize_health_state(row["health_state"]),
            "account_health_reason": str(row["health_reason"] or row["last_error"] or "").strip(),
            "account_last_sync_at": self._coerce_timestamp(row["last_sync_at"]),
            "account_last_error": str(row["last_error"] or "").strip(),
            "account_thread_count": max(0, int(row["thread_count"] or 0)),
        }

    @classmethod
    def _thread_activity_timestamp(cls, thread: dict[str, Any] | None) -> float | None:
        if not isinstance(thread, dict):
            return None
        for key in (
            "last_activity_timestamp",
            "last_action_at",
            "last_message_timestamp",
            "latest_customer_message_at",
            "last_seen_at",
        ):
            stamp = cls._coerce_timestamp(thread.get(key))
            if stamp is not None:
                return stamp
        return None

    @classmethod
    def _thread_sort_key(cls, thread: dict[str, Any]) -> tuple[float, float, str, str]:
        message_stamp = cls._coerce_timestamp(thread.get("last_message_timestamp")) or 0.0
        activity_stamp = cls._thread_activity_timestamp(thread) or 0.0
        return (
            -message_stamp,
            -activity_stamp,
            str(thread.get("display_name") or "").strip().lower(),
            str(thread.get("thread_key") or "").strip(),
        )

    def _load_deleted_thread_row(self, thread_key: str) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT thread_key, account_id, deleted_at, last_activity_timestamp
            FROM inbox_deleted_threads
            WHERE thread_key = ?
            """,
            (str(thread_key or "").strip(),),
        ).fetchone()

    def _remember_deleted_thread_locked(self, thread: dict[str, Any]) -> None:
        clean_key = str(thread.get("thread_key") or "").strip()
        if not clean_key:
            return
        self._conn.execute(
            """
            INSERT INTO inbox_deleted_threads(thread_key, account_id, deleted_at, last_activity_timestamp)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET
                account_id = excluded.account_id,
                deleted_at = excluded.deleted_at,
                last_activity_timestamp = excluded.last_activity_timestamp
            """,
            (
                clean_key,
                self._clean_account_id(thread.get("account_id")),
                time.time(),
                self._thread_activity_timestamp(thread),
            ),
        )

    def _clear_deleted_thread_locked(self, thread_key: str) -> None:
        self._conn.execute(
            "DELETE FROM inbox_deleted_threads WHERE thread_key = ?",
            (str(thread_key or "").strip(),),
        )

    def _row_to_thread_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        thread_key = str(row["thread_key"] or "").strip()
        operational_status = self._normalize_thread_status(self._row_value(row, "status", "open"))
        payload = {
            "thread_key": thread_key,
            "thread_id": str(row["thread_id"] or "").strip(),
            "thread_href": str(row["thread_href"] or "").strip(),
            "account_id": self._clean_account_id(row["account_id"]),
            "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
            "account_alias": str(row["account_alias"] or "").strip(),
            "recipient_username": str(row["recipient_username"] or "").strip(),
            "display_name": str(row["display_name"] or "").strip(),
            "owner": self._normalize_thread_owner(self._row_value(row, "owner", "none")),
            "bucket": self._normalize_thread_bucket(self._row_value(row, "bucket", "all")),
            "status": operational_status,
            "operational_status": operational_status,
            "stage_id": str(self._row_value(row, "stage_id", "initial") or "initial").strip() or "initial",
            "followup_level": max(0, int(self._row_value(row, "followup_level", 0) or 0)),
            "last_inbound_at": self._coerce_timestamp(self._row_value(row, "last_inbound_at")),
            "last_outbound_at": self._coerce_timestamp(self._row_value(row, "last_outbound_at")),
            "last_action_type": str(self._row_value(row, "last_action_type", "") or "").strip(),
            "last_action_at": self._coerce_timestamp(self._row_value(row, "last_action_at")),
            "last_pack_sent": str(self._row_value(row, "last_pack_sent", "") or "").strip(),
            "manual_lock": bool(self._row_value(row, "manual_lock", 0)),
            "manual_assignee": str(self._row_value(row, "manual_assignee", "") or "").strip(),
            "is_deleted_from_view": bool(self._row_value(row, "is_deleted_from_view", 0)),
            "trash_at": self._coerce_timestamp(self._row_value(row, "trash_at")),
            "created_at": self._coerce_timestamp(self._row_value(row, "created_at")),
            "updated_at": self._coerce_timestamp(self._row_value(row, "updated_at")),
            "last_message_text": str(row["last_message_text"] or "").strip(),
            "last_message_timestamp": self._coerce_timestamp(row["last_message_timestamp"]),
            "last_message_direction": str(row["last_message_direction"] or "").strip() or "unknown",
            "last_message_id": str(row["last_message_id"] or "").strip(),
            "unread_count": max(0, int(row["unread_count"] or 0)),
            "needs_reply": bool(row["needs_reply"]),
            "tags": self._normalize_tags(self._decode_json_list(row["tags_json"])),
            "participants": self._normalize_participants(self._decode_json_list(row["participants_json"])),
            "last_synced_at": self._coerce_timestamp(row["last_synced_at"]),
            "last_seen_text": str(row["last_seen_text"] or "").strip(),
            "last_seen_at": self._coerce_timestamp(row["last_seen_at"]),
            "latest_customer_message_at": self._coerce_timestamp(row["latest_customer_message_at"]),
        }
        payload.update(
            self._reconcile_thread_state_locked(
                thread_key,
                thread=payload,
                state=self._load_thread_state(thread_key),
            )
        )
        payload["status"] = operational_status
        payload["operational_status"] = operational_status
        payload["ui_status"] = self._normalize_ui_status(payload.get("ui_status"))
        payload.update(self._account_overlay(payload["account_id"]))
        return payload

    def _load_thread_record(self, thread_key: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM inbox_threads WHERE thread_key = ?",
            (str(thread_key or "").strip(),),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_thread_payload(row)

    def _thread_shell(
        self,
        thread_key: str,
        *,
        current: dict[str, Any] | None = None,
        participants: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        current_payload = dict(current or {})
        clean_key = str(thread_key or "").strip()
        account_id, thread_id = self._split_thread_key(clean_key)
        current_participants = self._normalize_participants(current_payload.get("participants"))
        merged_participants = list(current_participants)
        for item in self._normalize_participants(participants):
            if item not in merged_participants:
                merged_participants.append(item)
        current_tags = self._normalize_tags(current_payload.get("tags"))
        merged_tags = list(current_tags)
        for item in self._normalize_tags(tags):
            if item not in merged_tags:
                merged_tags.append(item)
        recipient_username = (
            str(current_payload.get("recipient_username") or "").strip().lstrip("@")
            or (merged_participants[0] if merged_participants else "")
        )
        display_name = (
            str(current_payload.get("display_name") or "").strip()
            or recipient_username
            or thread_id
            or "Conversacion"
        )
        try:
            unread_count = max(0, int(current_payload.get("unread_count") or 0))
        except Exception:
            unread_count = 0
        return {
            "thread_key": clean_key,
            "thread_id": str(current_payload.get("thread_id") or thread_id).strip(),
            "thread_href": str(current_payload.get("thread_href") or "").strip()
            or (f"https://www.instagram.com/direct/t/{thread_id}/" if thread_id else ""),
            "account_id": self._clean_account_id(current_payload.get("account_id") or account_id),
            "alias_id": str(current_payload.get("alias_id") or current_payload.get("account_alias") or "").strip(),
            "account_alias": str(current_payload.get("account_alias") or "").strip(),
            "recipient_username": recipient_username,
            "display_name": display_name,
            "owner": self._normalize_thread_owner(current_payload.get("owner")),
            "bucket": self._normalize_thread_bucket(current_payload.get("bucket")),
            "status": self._normalize_thread_status(
                current_payload.get("status") or current_payload.get("operational_status")
            ),
            "stage_id": str(current_payload.get("stage_id") or "initial").strip() or "initial",
            "followup_level": max(0, int(current_payload.get("followup_level") or 0)),
            "last_inbound_at": self._coerce_timestamp(current_payload.get("last_inbound_at")),
            "last_outbound_at": self._coerce_timestamp(current_payload.get("last_outbound_at")),
            "last_action_type": str(current_payload.get("last_action_type") or "").strip(),
            "last_action_at": self._coerce_timestamp(current_payload.get("last_action_at")),
            "last_pack_sent": str(current_payload.get("last_pack_sent") or "").strip(),
            "manual_lock": bool(current_payload.get("manual_lock", False)),
            "manual_assignee": str(current_payload.get("manual_assignee") or "").strip(),
            "is_deleted_from_view": bool(current_payload.get("is_deleted_from_view", False)),
            "trash_at": self._coerce_timestamp(current_payload.get("trash_at")),
            "created_at": self._coerce_timestamp(current_payload.get("created_at")) or time.time(),
            "updated_at": self._coerce_timestamp(current_payload.get("updated_at")) or time.time(),
            "last_message_text": str(current_payload.get("last_message_text") or "").strip(),
            "last_message_timestamp": self._coerce_timestamp(current_payload.get("last_message_timestamp")),
            "last_message_direction": str(current_payload.get("last_message_direction") or "unknown").strip() or "unknown",
            "last_message_id": str(current_payload.get("last_message_id") or "").strip(),
            "unread_count": unread_count,
            "needs_reply": 1 if bool(current_payload.get("needs_reply")) else 0,
            "tags": merged_tags,
            "participants": merged_participants,
            "last_synced_at": self._coerce_timestamp(current_payload.get("last_synced_at")) or time.time(),
            "last_seen_text": str(current_payload.get("last_seen_text") or "").strip(),
            "last_seen_at": self._coerce_timestamp(current_payload.get("last_seen_at")),
            "latest_customer_message_at": self._coerce_timestamp(current_payload.get("latest_customer_message_at")),
        }

    def _account_session_started_at_locked(self, account_id: str) -> float | None:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return None
        row = self._conn.execute(
            "SELECT session_started_at FROM inbox_account_state WHERE account_id = ?",
            (clean_account,),
        ).fetchone()
        return self._coerce_timestamp(self._row_value(row, "session_started_at"))

    def _upsert_thread_record(self, thread: dict[str, Any]) -> None:
        thread = dict(thread or {})
        thread.setdefault("created_at", self._coerce_timestamp(thread.get("created_at")) or time.time())
        thread["updated_at"] = self._coerce_timestamp(thread.get("updated_at")) or time.time()
        self._conn.execute(
            """
            INSERT INTO inbox_threads(
                thread_key, thread_id, thread_href, account_id, alias_id, account_alias,
                recipient_username, display_name, owner, bucket, status, stage_id,
                followup_level, last_inbound_at, last_outbound_at, last_action_type,
                last_action_at, last_pack_sent, manual_lock, manual_assignee,
                is_deleted_from_view, trash_at, created_at, updated_at,
                last_message_text, last_message_timestamp, last_message_direction,
                last_message_id, unread_count, needs_reply, tags_json, participants_json,
                last_synced_at, last_seen_text, last_seen_at, latest_customer_message_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET
                thread_id = excluded.thread_id,
                thread_href = excluded.thread_href,
                account_id = excluded.account_id,
                alias_id = excluded.alias_id,
                account_alias = excluded.account_alias,
                recipient_username = excluded.recipient_username,
                display_name = excluded.display_name,
                owner = excluded.owner,
                bucket = excluded.bucket,
                status = excluded.status,
                stage_id = excluded.stage_id,
                followup_level = excluded.followup_level,
                last_inbound_at = excluded.last_inbound_at,
                last_outbound_at = excluded.last_outbound_at,
                last_action_type = excluded.last_action_type,
                last_action_at = excluded.last_action_at,
                last_pack_sent = excluded.last_pack_sent,
                manual_lock = excluded.manual_lock,
                manual_assignee = excluded.manual_assignee,
                is_deleted_from_view = excluded.is_deleted_from_view,
                trash_at = excluded.trash_at,
                created_at = COALESCE(inbox_threads.created_at, excluded.created_at),
                updated_at = excluded.updated_at,
                last_message_text = excluded.last_message_text,
                last_message_timestamp = excluded.last_message_timestamp,
                last_message_direction = excluded.last_message_direction,
                last_message_id = excluded.last_message_id,
                unread_count = excluded.unread_count,
                needs_reply = excluded.needs_reply,
                tags_json = excluded.tags_json,
                participants_json = excluded.participants_json,
                last_synced_at = excluded.last_synced_at,
                last_seen_text = excluded.last_seen_text,
                last_seen_at = excluded.last_seen_at,
                latest_customer_message_at = excluded.latest_customer_message_at
            """,
            (
                thread["thread_key"],
                thread["thread_id"],
                thread["thread_href"],
                thread["account_id"],
                str(thread.get("alias_id") or "").strip(),
                thread["account_alias"],
                thread["recipient_username"],
                thread["display_name"],
                self._normalize_thread_owner(thread.get("owner")),
                self._normalize_thread_bucket(thread.get("bucket")),
                self._normalize_thread_status(thread.get("status")),
                str(thread.get("stage_id") or "initial").strip() or "initial",
                max(0, int(thread.get("followup_level") or 0)),
                self._coerce_timestamp(thread.get("last_inbound_at")),
                self._coerce_timestamp(thread.get("last_outbound_at")),
                str(thread.get("last_action_type") or "").strip(),
                self._coerce_timestamp(thread.get("last_action_at")),
                str(thread.get("last_pack_sent") or "").strip(),
                1 if bool(thread.get("manual_lock")) else 0,
                str(thread.get("manual_assignee") or "").strip(),
                1 if bool(thread.get("is_deleted_from_view")) else 0,
                self._coerce_timestamp(thread.get("trash_at")),
                self._coerce_timestamp(thread.get("created_at")),
                self._coerce_timestamp(thread.get("updated_at")),
                thread["last_message_text"],
                thread["last_message_timestamp"],
                thread["last_message_direction"],
                thread["last_message_id"],
                max(0, int(thread.get("unread_count") or 0)),
                1 if bool(thread.get("needs_reply")) else 0,
                self._encode_json(self._normalize_tags(thread.get("tags"))),
                self._encode_json(self._normalize_participants(thread.get("participants"))),
                self._coerce_timestamp(thread.get("last_synced_at")),
                str(thread.get("last_seen_text") or "").strip(),
                self._coerce_timestamp(thread.get("last_seen_at")),
                self._coerce_timestamp(thread.get("latest_customer_message_at")),
            ),
        )

    def _load_blocks(self, thread_key: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT block_id, ordinal, account_id, message_id, external_message_id, text,
                   timestamp, direction, source, pack_id, stage_id, created_at,
                   confirmed_at, user_id, delivery_status, sent_status, local_echo,
                   hidden_at,
                   error_message
            FROM inbox_messages
            WHERE thread_key = ?
            ORDER BY COALESCE(timestamp, 0), ordinal
            """,
            (str(thread_key or "").strip(),),
        ).fetchall()
        return [
            {
                "block_id": str(row["block_id"] or "").strip(),
                "account_id": self._clean_account_id(self._row_value(row, "account_id", "")),
                "message_id": str(row["message_id"] or "").strip(),
                "external_message_id": str(self._row_value(row, "external_message_id", "") or "").strip(),
                "text": str(row["text"] or "").strip(),
                "timestamp": self._coerce_timestamp(row["timestamp"]),
                "direction": str(row["direction"] or "").strip() or "unknown",
                "source": self._normalize_message_source(self._row_value(row, "source", "")),
                "pack_id": str(self._row_value(row, "pack_id", "") or "").strip(),
                "stage_id": str(self._row_value(row, "stage_id", "") or "").strip(),
                "created_at": self._coerce_timestamp(self._row_value(row, "created_at")),
                "confirmed_at": self._coerce_timestamp(self._row_value(row, "confirmed_at")),
                "user_id": str(row["user_id"] or "").strip(),
                "delivery_status": self._normalize_delivery_status(row["delivery_status"]),
                "sent_status": self._normalize_job_state(self._row_value(row, "sent_status", row["delivery_status"])),
                "local_echo": bool(row["local_echo"]),
                "hidden_at": self._coerce_timestamp(self._row_value(row, "hidden_at")),
                "error_message": str(row["error_message"] or "").strip(),
            }
            for row in rows
        ]

    def _save_blocks(self, thread_key: str, blocks: list[dict[str, Any]]) -> None:
        self._conn.execute("DELETE FROM inbox_messages WHERE thread_key = ?", (thread_key,))
        for ordinal, block in enumerate(blocks):
            self._conn.execute(
                """
                INSERT INTO inbox_messages(
                    thread_key, block_id, ordinal, account_id, message_id, external_message_id,
                    text, timestamp, direction, source, pack_id, stage_id, created_at,
                    confirmed_at, user_id, delivery_status, sent_status, local_echo,
                    hidden_at,
                    error_message
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_key,
                    str(block.get("block_id") or block.get("message_id") or f"block:{ordinal}").strip(),
                    ordinal,
                    self._clean_account_id(block.get("account_id")),
                    str(block.get("message_id") or "").strip(),
                    str(block.get("external_message_id") or block.get("message_id") or "").strip(),
                    str(block.get("text") or "").strip(),
                    self._coerce_timestamp(block.get("timestamp")),
                    str(block.get("direction") or "unknown").strip(),
                    self._normalize_message_source(block.get("source")),
                    str(block.get("pack_id") or "").strip(),
                    str(block.get("stage_id") or "").strip(),
                    self._coerce_timestamp(block.get("created_at")) or self._coerce_timestamp(block.get("timestamp")),
                    self._coerce_timestamp(block.get("confirmed_at")),
                    str(block.get("user_id") or "").strip(),
                    self._normalize_delivery_status(block.get("delivery_status")),
                    self._normalize_job_state(block.get("sent_status") or block.get("delivery_status")),
                    1 if bool(block.get("local_echo")) else 0,
                    self._coerce_timestamp(block.get("hidden_at")),
                    str(block.get("error_message") or "").strip(),
                ),
            )

    def _compress_blocks(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = [self._normalize_message_record(item) for item in messages]
        clean_rows = [item for item in normalized if isinstance(item, dict)]
        clean_rows.sort(key=self._message_sort_key)
        if not clean_rows:
            return []

        blocks: list[dict[str, Any]] = []
        for index, message in enumerate(clean_rows):
            stamp = self._message_anchor_timestamp(message)
            message_id = str(message.get("message_id") or "").strip()
            external_message_id = str(message.get("external_message_id") or message_id).strip()
            block_id = str(message_id or external_message_id or f"block:{index}").strip()
            blocks.append(
                {
                    "block_id": block_id,
                    "account_id": self._clean_account_id(message.get("account_id")),
                    "message_id": message_id,
                    "external_message_id": external_message_id,
                    "text": str(message.get("text") or "").strip(),
                    "timestamp": self._coerce_timestamp(message.get("timestamp")),
                    "direction": str(message.get("direction") or "unknown").strip().lower() or "unknown",
                    "source": self._normalize_message_source(message.get("source")),
                    "pack_id": str(message.get("pack_id") or "").strip(),
                    "stage_id": str(message.get("stage_id") or "").strip(),
                    "created_at": self._coerce_timestamp(message.get("created_at")) or self._coerce_timestamp(message.get("confirmed_at")) or stamp,
                    "confirmed_at": self._coerce_timestamp(message.get("confirmed_at")),
                    "user_id": str(message.get("user_id") or "").strip(),
                    "delivery_status": self._normalize_delivery_status(message.get("delivery_status")),
                    "sent_status": self._normalize_job_state(message.get("sent_status") or message.get("delivery_status")),
                    "local_echo": bool(message.get("local_echo")),
                    "hidden_at": self._coerce_timestamp(message.get("hidden_at")),
                    "error_message": str(message.get("error_message") or "").strip(),
                }
            )
        return self._canonicalize_blocks(blocks)

    def _apply_message_timestamp_fallbacks(
        self,
        blocks: list[dict[str, Any]],
        *,
        current: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        del current
        return [dict(block) for block in blocks if isinstance(block, dict)]

    def _merge_remote_and_local_blocks(self, thread_key: str, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        remote_ids = {str(item.get("message_id") or "").strip() for item in blocks if str(item.get("message_id") or "").strip()}
        carried_blocks = [
            item
            for item in self._load_blocks(thread_key)
            if self._normalize_delivery_status(item.get("delivery_status")) in {"pending", "sending", "error"}
            or self._coerce_timestamp(item.get("hidden_at")) is not None
        ]
        merged = list(blocks)
        for block in carried_blocks:
            block_id = str(block.get("message_id") or "").strip()
            if block_id and block_id in remote_ids and self._coerce_timestamp(block.get("hidden_at")) is None:
                continue
            merged.append(dict(block))
        merged = self._canonicalize_blocks(merged)
        deduped: dict[str, dict[str, Any]] = {}
        for block in merged:
            key = str(block.get("message_id") or block.get("block_id") or "").strip()
            if not key:
                key = f"anon:{len(deduped)}"
            deduped[key] = dict(block)
        rows = list(deduped.values())[-self._MAX_MESSAGES_PER_THREAD :]
        rows.sort(key=self._message_sort_key)
        return rows

    def _merge_cached_blocks(
        self,
        existing_blocks: list[dict[str, Any]],
        incoming_blocks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for source in (existing_blocks, incoming_blocks):
            for block in source:
                key = self._block_identity(block)
                if not key:
                    continue
                if key not in merged:
                    order.append(key)
                merged[key] = dict(block)
        rows = [merged[key] for key in order]
        rows = self._canonicalize_blocks(rows)
        rows.sort(key=self._message_sort_key)
        return rows[-self._MAX_MESSAGES_PER_THREAD :]

    @staticmethod
    def _block_identity(block: dict[str, Any]) -> str:
        message_id = str(block.get("message_id") or block.get("block_id") or "").strip()
        if message_id:
            return f"id:{message_id}"
        return "|".join(
            (
                str(block.get("direction") or "").strip().lower(),
                str(block.get("timestamp") or ""),
                str(block.get("text") or "").strip(),
            )
        )

    @staticmethod
    def _block_matches_message_ref(block: dict[str, Any], message_ref: dict[str, Any]) -> bool:
        for key in ("block_id", "message_id", "external_message_id"):
            ref_value = str(message_ref.get(key) or "").strip()
            if not ref_value:
                continue
            block_value = str(block.get(key) or "").strip()
            if block_value and block_value == ref_value:
                return True
        return False

    def _derive_thread_metrics(
        self,
        blocks: list[dict[str, Any]],
        *,
        current_unread_count: int,
        mark_read: bool,
    ) -> dict[str, Any]:
        visible_blocks = self._visible_blocks(blocks)
        latest_message = visible_blocks[-1] if visible_blocks else None
        latest_inbound_at: float | None = None
        latest_outbound_sent_at: float | None = None
        for block in visible_blocks:
            real_timestamp = self._coerce_timestamp(block.get("timestamp"))
            display_timestamp = self._message_display_timestamp(block)
            direction = str(block.get("direction") or "").strip().lower()
            status = self._normalize_delivery_status(block.get("delivery_status"))
            if direction == "inbound" and real_timestamp is not None:
                latest_inbound_at = real_timestamp if latest_inbound_at is None else max(latest_inbound_at, real_timestamp)
            if direction == "outbound" and display_timestamp is not None and status == "sent":
                latest_outbound_sent_at = (
                    display_timestamp
                    if latest_outbound_sent_at is None
                    else max(latest_outbound_sent_at, display_timestamp)
                )
        needs_reply = bool(
            latest_inbound_at is not None
            and (latest_outbound_sent_at is None or latest_outbound_sent_at < latest_inbound_at)
        )
        unread_count = 0
        if not mark_read and latest_message is not None and str(latest_message.get("direction") or "").strip().lower() == "inbound":
            unread_count = max(1, int(current_unread_count or 0))
        return {
            "latest_message": latest_message,
            "latest_customer_message_at": latest_inbound_at,
            "needs_reply": needs_reply,
            "unread_count": unread_count,
        }

    def _drop_thread_locked(self, thread_key: str) -> None:
        self._conn.execute("DELETE FROM inbox_messages WHERE thread_key = ?", (thread_key,))
        self._conn.execute("DELETE FROM inbox_thread_state WHERE thread_key = ?", (thread_key,))
        self._conn.execute("DELETE FROM inbox_threads WHERE thread_key = ?", (thread_key,))

    def _thread_has_pending_work_locked(self, thread_key: str, state: dict[str, Any] | None = None) -> bool:
        payload = dict(state or {})
        sender_status = str(payload.get("sender_status") or "").strip().lower()
        pack_status = str(payload.get("pack_status") or "").strip().lower()
        if sender_status in {"queued", "preparing", "sending"}:
            return True
        if pack_status in {"queued", "running"}:
            return True
        for block in self._load_blocks(thread_key):
            status = self._normalize_delivery_status(block.get("delivery_status"))
            if status in {"pending", "sending"}:
                return True
        return False

    def _trim_global_threads_locked(self) -> None:
        rows = [
            self._row_to_thread_payload(row)
            for row in self._conn.execute("SELECT * FROM inbox_threads").fetchall()
        ]
        rows.sort(key=self._thread_sort_key)
        if len(rows) <= self._MAX_ACTIVE_THREADS:
            return
        max_threads = max(1, int(self._MAX_ACTIVE_THREADS or 0))
        excess = len(rows) - max_threads
        if excess <= 0:
            return
        for row in reversed(rows):
            if excess <= 0:
                return
            thread_key = str(row.get("thread_key") or "").strip()
            if not thread_key:
                continue
            if self._thread_has_pending_work_locked(thread_key, row):
                continue
            self._drop_thread_locked(thread_key)
            excess -= 1

    @staticmethod
    def _include_thread(thread: dict[str, Any], filter_mode: str) -> bool:
        mode = str(filter_mode or "all").strip().lower()
        if bool(thread.get("is_deleted_from_view")):
            return False
        if mode == "qualified":
            return str(thread.get("bucket") or "").strip().lower() == "qualified"
        if mode == "disqualified":
            return str(thread.get("bucket") or "").strip().lower() == "disqualified"
        if mode == "unread":
            try:
                return int(thread.get("unread_count") or 0) > 0
            except Exception:
                return False
        if mode == "pending":
            if "needs_reply" in thread:
                return bool(thread.get("needs_reply"))
            return str(thread.get("last_message_direction") or "").strip().lower() == "inbound"
        return True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            threads = {
                row["thread_key"]: self.get_thread(row["thread_key"])
                for row in self._conn.execute("SELECT thread_key FROM inbox_threads").fetchall()
            }
            state = {
                "accounts": {
                    row["account_id"]: {
                        "session_marker": str(row["session_marker"] or "").strip(),
                        "session_started_at": self._coerce_timestamp(row["session_started_at"]),
                        "last_sync_at": self._coerce_timestamp(row["last_sync_at"]),
                        "last_error": str(row["last_error"] or "").strip(),
                        "thread_count": max(0, int(row["thread_count"] or 0)),
                        "health_state": self._normalize_health_state(row["health_state"]),
                        "health_reason": str(row["health_reason"] or "").strip(),
                        "health_updated_at": self._coerce_timestamp(row["health_updated_at"]),
                    }
                    for row in self._conn.execute("SELECT * FROM inbox_account_state").fetchall()
                },
                "threads": {
                    row["thread_key"]: self._load_thread_state(row["thread_key"])
                    for row in self._conn.execute("SELECT thread_key FROM inbox_threads").fetchall()
                },
            }
        return {
            "threads": copy.deepcopy(threads),
            "messages": {
                thread_key: thread.get("messages", [])
                for thread_key, thread in threads.items()
                if isinstance(thread, dict)
            },
            "state": state,
        }

    def stats(self) -> dict[str, int]:
        with self._lock:
            thread_total = self._conn.execute("SELECT COUNT(*) AS total FROM inbox_threads").fetchone()
            message_total = self._conn.execute("SELECT COUNT(DISTINCT thread_key) AS total FROM inbox_messages").fetchone()
        return {
            "thread_count": int(self._row_value(thread_total, "total", 0) or 0),
            "message_groups": int(self._row_value(message_total, "total", 0) or 0),
        }

    def get_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM inbox_threads").fetchall()
            payload = [self._row_to_thread_payload(row) for row in rows]
            payload.sort(key=self._thread_sort_key)
        return [dict(row) for row in payload if self._include_thread(row, filter_mode)]

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return None
        with self._lock:
            thread = self._load_thread_record(clean_key)
            if not isinstance(thread, dict):
                return None
            if bool(thread.get("is_deleted_from_view")):
                return None
            blocks = self._load_blocks(clean_key)
            canonical = self._canonicalize_blocks(blocks)
            if canonical != blocks:
                self._save_blocks(clean_key, canonical)
                self._conn.commit()
            thread["messages"] = self._visible_blocks(canonical)
            return copy.deepcopy(thread)

    def get_messages(self, thread_key: str) -> list[dict[str, Any]]:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return []
        with self._lock:
            blocks = self._load_blocks(clean_key)
            canonical = self._canonicalize_blocks(blocks)
            if canonical != blocks:
                self._save_blocks(clean_key, canonical)
                self._conn.commit()
            return copy.deepcopy(self._visible_blocks(canonical))

    def upsert_threads(self, thread_rows: list[dict[str, Any]]) -> None:
        now = time.time()
        with self._lock:
            for raw in thread_rows:
                raw_stage_present = any(
                    key in raw and str(raw.get(key) or "").strip()
                    for key in ("stage_id", "stage")
                ) if isinstance(raw, dict) else False
                raw_followup_present = (
                    isinstance(raw, dict)
                    and "followup_level" in raw
                    and raw.get("followup_level") not in {None, ""}
                )
                thread = self._normalize_thread_record(raw)
                if not thread:
                    continue
                current = self._load_thread_record(thread["thread_key"]) or {}
                if not thread.get("last_message_text"):
                    thread["last_message_text"] = str(current.get("last_message_text") or "").strip()
                if not thread.get("last_message_timestamp"):
                    thread["last_message_timestamp"] = self._coerce_timestamp(current.get("last_message_timestamp"))
                if not thread.get("last_message_id"):
                    thread["last_message_id"] = str(current.get("last_message_id") or "").strip()
                if not thread.get("latest_customer_message_at"):
                    thread["latest_customer_message_at"] = self._coerce_timestamp(current.get("latest_customer_message_at"))
                if not thread.get("last_seen_text"):
                    thread["last_seen_text"] = str(current.get("last_seen_text") or "").strip()
                    thread["last_seen_at"] = self._coerce_timestamp(current.get("last_seen_at"))
                thread["alias_id"] = str(thread.get("alias_id") or current.get("alias_id") or thread.get("account_alias") or "").strip()
                thread["owner"] = self._normalize_thread_owner(thread.get("owner") or current.get("owner") or "none")
                thread["bucket"] = self._normalize_thread_bucket(thread.get("bucket") or current.get("bucket") or "all")
                thread["status"] = self._normalize_thread_status(thread.get("status") or current.get("status") or "open")
                current_stage_id = self._normalize_stage_id(current.get("stage_id"))
                current_state = self._load_thread_state(thread["thread_key"])
                current_stage_evidenced = self._thread_stage_has_operational_evidence_locked(
                    thread["thread_key"],
                    stage_id=current_stage_id,
                    thread=current,
                    state=current_state,
                ) if current else False
                if raw_stage_present:
                    thread["stage_id"] = self._normalize_stage_id(thread.get("stage_id"))
                    final_stage_evidenced = True
                elif responder_module._is_initial_flow_stage_id(current_stage_id) or current_stage_evidenced:
                    thread["stage_id"] = current_stage_id
                    final_stage_evidenced = current_stage_evidenced
                else:
                    thread["stage_id"] = "initial"
                    final_stage_evidenced = False
                if raw_followup_present and (raw_stage_present or thread["stage_id"] == current_stage_id):
                    thread["followup_level"] = self._coerce_non_negative_int(thread.get("followup_level"))
                elif thread["stage_id"] == current_stage_id and current:
                    thread["followup_level"] = self._reconciled_followup_level_locked(
                        thread["thread_key"],
                        stage_id=thread["stage_id"],
                        thread=current,
                        state=current_state,
                    )
                else:
                    thread["followup_level"] = 0
                thread["last_inbound_at"] = self._coerce_timestamp(thread.get("last_inbound_at") or current.get("last_inbound_at"))
                thread["last_outbound_at"] = self._coerce_timestamp(thread.get("last_outbound_at") or current.get("last_outbound_at"))
                thread["last_action_type"] = str(thread.get("last_action_type") or current.get("last_action_type") or "").strip()
                thread["last_action_at"] = self._coerce_timestamp(thread.get("last_action_at") or current.get("last_action_at"))
                thread["last_pack_sent"] = str(thread.get("last_pack_sent") or current.get("last_pack_sent") or "").strip()
                thread["manual_lock"] = bool(thread.get("manual_lock", current.get("manual_lock", False)))
                thread["manual_assignee"] = str(thread.get("manual_assignee") or current.get("manual_assignee") or "").strip()
                thread["is_deleted_from_view"] = bool(thread.get("is_deleted_from_view", current.get("is_deleted_from_view", False)))
                thread["trash_at"] = self._coerce_timestamp(thread.get("trash_at") or current.get("trash_at"))
                thread["created_at"] = self._coerce_timestamp(thread.get("created_at") or current.get("created_at")) or now
                thread["updated_at"] = now
                thread["participants"] = self._normalize_participants(
                    list(current.get("participants") or []) + list(thread.get("participants") or [])
                )
                thread["tags"] = self._normalize_tags(
                    list(current.get("tags") or []) + list(thread.get("tags") or [])
                )
                thread["last_synced_at"] = now
                self._upsert_thread_record(thread)
                state = self._load_thread_state(thread["thread_key"])
                reconciled_state = self._reconcile_thread_state_locked(
                    thread["thread_key"],
                    thread=thread,
                    state=state,
                    stage_evidenced=final_stage_evidenced,
                )
                if reconciled_state != state:
                    self._save_thread_state(thread["thread_key"], reconciled_state)
            self._trim_global_threads_locked()
            self._conn.commit()

    def replace_messages(
        self,
        thread_key: str,
        messages: list[dict[str, Any]],
        *,
        seen_text: str = "",
        seen_at: float | None = None,
        participants: list[str] | None = None,
        mark_read: bool = False,
    ) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._lock:
            current = self._load_thread_record(clean_key) or self._thread_shell(clean_key, participants=participants)
            compressed = self._apply_message_timestamp_fallbacks(
                self._compress_blocks(messages),
                current=current,
            )
            blocks = self._merge_remote_and_local_blocks(clean_key, compressed)
            metrics = self._derive_thread_metrics(
                blocks,
                current_unread_count=max(0, int(current.get("unread_count") or 0)),
                mark_read=mark_read,
            )
            latest = metrics["latest_message"]
            thread = self._thread_shell(clean_key, current=current, participants=participants)
            latest_timestamp = self._message_display_timestamp(latest) if latest is not None else None
            if latest is not None:
                thread["last_message_text"] = str(latest.get("text") or "").strip()
                thread["last_message_direction"] = str(latest.get("direction") or "unknown").strip()
                thread["last_message_id"] = str(latest.get("message_id") or "").strip()
                if latest_timestamp is not None:
                    thread["last_message_timestamp"] = latest_timestamp
            else:
                thread["last_message_text"] = ""
                thread["last_message_direction"] = "unknown"
                thread["last_message_id"] = ""
                thread["last_message_timestamp"] = None
            thread["last_inbound_at"] = metrics["latest_customer_message_at"] or self._coerce_timestamp(
                current.get("last_inbound_at")
            )
            if latest is not None and str(latest.get("direction") or "").strip().lower() == "outbound":
                if latest_timestamp is not None:
                    thread["last_outbound_at"] = latest_timestamp
            thread["latest_customer_message_at"] = metrics["latest_customer_message_at"] or self._coerce_timestamp(
                current.get("latest_customer_message_at")
            )
            thread["needs_reply"] = 1 if metrics["needs_reply"] else 0
            thread["unread_count"] = int(metrics["unread_count"] or 0)
            thread["last_synced_at"] = time.time()
            thread["updated_at"] = time.time()
            if seen_text:
                thread["last_seen_text"] = str(seen_text or "").strip()
                thread["last_seen_at"] = self._coerce_timestamp(seen_at) or time.time()
            self._upsert_thread_record(thread)
            if mark_read:
                state = self._load_thread_state(clean_key)
                state["last_opened_at"] = time.time()
                self._save_thread_state(clean_key, state)
            self._save_blocks(clean_key, blocks)
            self._conn.commit()

    def seed_messages(
        self,
        thread_key: str,
        messages: list[dict[str, Any]],
        *,
        participants: list[str] | None = None,
    ) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._lock:
            current = self._load_thread_record(clean_key) or self._thread_shell(clean_key, participants=participants)
            preview_blocks = self._apply_message_timestamp_fallbacks(
                self._compress_blocks(messages),
                current=current,
            )
            if not preview_blocks:
                return
            existing_blocks = self._load_blocks(clean_key)
            blocks = self._merge_cached_blocks(existing_blocks, preview_blocks)
            metrics = self._derive_thread_metrics(
                blocks,
                current_unread_count=max(0, int(current.get("unread_count") or 0)),
                mark_read=False,
            )
            latest = metrics["latest_message"]
            thread = self._thread_shell(clean_key, current=current, participants=participants)
            latest_timestamp = self._message_display_timestamp(latest) if latest is not None else None
            if latest is not None:
                thread["last_message_text"] = str(latest.get("text") or "").strip()
                thread["last_message_direction"] = str(latest.get("direction") or "unknown").strip()
                thread["last_message_id"] = str(latest.get("message_id") or "").strip()
                if latest_timestamp is not None:
                    thread["last_message_timestamp"] = latest_timestamp
            else:
                thread["last_message_text"] = ""
                thread["last_message_direction"] = "unknown"
                thread["last_message_id"] = ""
                thread["last_message_timestamp"] = None
            thread["last_inbound_at"] = metrics["latest_customer_message_at"] or self._coerce_timestamp(
                current.get("last_inbound_at")
            )
            if latest is not None and str(latest.get("direction") or "").strip().lower() == "outbound":
                if latest_timestamp is not None:
                    thread["last_outbound_at"] = latest_timestamp
            thread["latest_customer_message_at"] = metrics["latest_customer_message_at"] or self._coerce_timestamp(
                current.get("latest_customer_message_at")
            )
            thread["needs_reply"] = 1 if metrics["needs_reply"] else 0
            thread["unread_count"] = int(metrics["unread_count"] or 0)
            thread["last_synced_at"] = time.time()
            thread["updated_at"] = time.time()
            self._upsert_thread_record(thread)
            self._save_blocks(clean_key, blocks)
            self._conn.commit()

    def append_local_outbound_message(
        self,
        thread_key: str,
        text: str,
        *,
        source: str = "manual",
        pack_id: str = "",
        local_message_id: str = "",
    ) -> dict[str, Any] | None:
        clean_key = str(thread_key or "").strip()
        content = str(text or "").strip()
        if not clean_key or not content:
            return None
        local_id = str(local_message_id or "").strip() or f"local-{uuid.uuid4().hex}"
        now = time.time()
        normalized_source = self._normalize_message_source(source)
        action_by_source = {
            "auto": "auto_reply",
            "followup": "followup",
            "campaign": "campaign",
            "manual": "manual_reply",
        }
        block = {
            "block_id": local_id,
            "message_id": local_id,
            "external_message_id": local_id,
            "text": content,
            "timestamp": now,
            "direction": "outbound",
            "source": normalized_source,
            "pack_id": str(pack_id or "").strip(),
            "created_at": now,
            "user_id": "",
            "delivery_status": "pending",
            "sent_status": "queued",
            "local_echo": True,
            "hidden_at": None,
            "error_message": "",
        }
        with self._lock:
            current = self._load_thread_record(clean_key) or self._thread_shell(clean_key)
            block["account_id"] = str(current.get("account_id") or "").strip()
            block["stage_id"] = str(current.get("stage_id") or "").strip()
            blocks = self._load_blocks(clean_key)
            blocks.append(block)
            blocks = self._canonicalize_blocks(blocks)
            metrics = self._derive_thread_metrics(
                blocks,
                current_unread_count=max(0, int(current.get("unread_count") or 0)),
                mark_read=True,
            )
            thread = self._thread_shell(clean_key, current=current)
            local_timestamp = self._message_display_timestamp(block)
            thread["last_message_text"] = content
            thread["last_message_timestamp"] = local_timestamp
            thread["last_message_direction"] = "outbound"
            thread["last_message_id"] = local_id
            thread["last_outbound_at"] = local_timestamp
            thread["last_action_type"] = str(action_by_source.get(normalized_source, "manual_reply"))
            thread["last_action_at"] = local_timestamp
            thread["status"] = "pending"
            if normalized_source == "manual":
                thread["owner"] = self._normalize_thread_owner(thread.get("owner") or "manual")
                thread["manual_lock"] = bool(thread.get("manual_lock")) or thread.get("owner") == "manual"
            thread["unread_count"] = 0
            thread["needs_reply"] = 1 if metrics["needs_reply"] else 0
            thread["last_synced_at"] = now
            thread["updated_at"] = now
            self._upsert_thread_record(thread)
            self._save_blocks(clean_key, blocks)
            self._conn.commit()
        return dict(block)

    def set_local_outbound_status(
        self,
        thread_key: str,
        local_message_id: str,
        *,
        status: str,
        error_message: str = "",
    ) -> None:
        clean_key = str(thread_key or "").strip()
        local_id = str(local_message_id or "").strip()
        next_status = self._normalize_delivery_status(status)
        if not clean_key or not local_id:
            return
        with self._lock:
            blocks = self._load_blocks(clean_key)
            changed = False
            for block in blocks:
                if str(block.get("message_id") or "").strip() != local_id:
                    continue
                block["delivery_status"] = next_status
                block["sent_status"] = self._normalize_job_state(status)
                block["error_message"] = str(error_message or "").strip()
                changed = True
                break
            if not changed:
                return
            blocks = self._canonicalize_blocks(blocks)
            self._save_blocks(clean_key, blocks)
            self._conn.commit()

    def resolve_local_outbound(
        self,
        thread_key: str,
        local_message_id: str,
        *,
        final_message_id: str = "",
        sent_timestamp: float | None = None,
        error_message: str = "",
    ) -> None:
        clean_key = str(thread_key or "").strip()
        local_id = str(local_message_id or "").strip()
        if not clean_key or not local_id:
            return
        with self._lock:
            current = self._load_thread_record(clean_key) or self._thread_shell(clean_key)
            blocks = self._load_blocks(clean_key)
            changed = False
            resolved_created_at: float | None = None
            for block in blocks:
                if str(block.get("message_id") or "").strip() != local_id:
                    continue
                resolved_created_at = self._coerce_timestamp(block.get("created_at"))
                if error_message:
                    block["delivery_status"] = "error"
                    block["sent_status"] = "failed"
                    block["error_message"] = str(error_message).strip()
                else:
                    block["delivery_status"] = "sent"
                    block["sent_status"] = "confirmed"
                    block["error_message"] = ""
                    block["local_echo"] = False
                    resolved_message_id = str(final_message_id or "").strip()
                    if resolved_message_id and not self._is_synthetic_message_id(resolved_message_id):
                        block["message_id"] = str(final_message_id).strip()
                        block["block_id"] = str(final_message_id).strip()
                        block["external_message_id"] = str(final_message_id).strip()
                    elif resolved_message_id:
                        block["external_message_id"] = resolved_message_id
                    resolved_timestamp = self._coerce_timestamp(sent_timestamp)
                    if resolved_timestamp is not None:
                        block["timestamp"] = resolved_timestamp
                        block["confirmed_at"] = resolved_timestamp
                    else:
                        block["confirmed_at"] = self._coerce_timestamp(block.get("confirmed_at"))
                changed = True
                break
            if not changed:
                return
            blocks = self._canonicalize_blocks(blocks)
            metrics = self._derive_thread_metrics(
                blocks,
                current_unread_count=max(0, int(current.get("unread_count") or 0)),
                mark_read=False,
            )
            latest = metrics["latest_message"]
            thread = self._thread_shell(clean_key, current=current)
            latest_timestamp = self._message_display_timestamp(latest) if latest is not None else None
            if latest is not None:
                thread["last_message_text"] = str(latest.get("text") or "").strip()
                thread["last_message_direction"] = str(latest.get("direction") or "unknown").strip()
                thread["last_message_id"] = str(latest.get("message_id") or "").strip()
                if latest_timestamp is not None:
                    thread["last_message_timestamp"] = latest_timestamp
            else:
                thread["last_message_text"] = ""
                thread["last_message_direction"] = "unknown"
                thread["last_message_id"] = ""
                thread["last_message_timestamp"] = None
            thread["needs_reply"] = 1 if metrics["needs_reply"] else 0
            thread["latest_customer_message_at"] = metrics["latest_customer_message_at"] or self._coerce_timestamp(
                current.get("latest_customer_message_at")
            )
            resolved_ts = (
                self._coerce_timestamp(sent_timestamp)
                or latest_timestamp
                or resolved_created_at
                or time.time()
            )
            thread["last_synced_at"] = time.time()
            thread["last_outbound_at"] = resolved_ts
            thread["last_action_at"] = resolved_ts
            thread["status"] = "failed" if error_message else "replied"
            thread["updated_at"] = time.time()
            self._upsert_thread_record(thread)
            self._save_blocks(clean_key, blocks[-self._MAX_MESSAGES_PER_THREAD :])
            self._conn.commit()

    def delete_message_local(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        clean_key = str(thread_key or "").strip()
        payload = dict(message_ref or {})
        if not clean_key or not payload:
            return False
        with self._lock:
            current = self._load_thread_record(clean_key) or self._thread_shell(clean_key)
            blocks = self._load_blocks(clean_key)
            changed = False
            for block in blocks:
                if not self._block_matches_message_ref(block, payload):
                    continue
                if self._coerce_timestamp(block.get("hidden_at")) is not None:
                    return False
                block["hidden_at"] = time.time()
                changed = True
                break
            if not changed:
                return False
            blocks = self._canonicalize_blocks(blocks)
            metrics = self._derive_thread_metrics(
                blocks,
                current_unread_count=max(0, int(current.get("unread_count") or 0)),
                mark_read=False,
            )
            latest = metrics["latest_message"]
            thread = self._thread_shell(clean_key, current=current)
            latest_timestamp = self._message_display_timestamp(latest) if latest is not None else None
            if latest is not None:
                thread["last_message_text"] = str(latest.get("text") or "").strip()
                thread["last_message_direction"] = str(latest.get("direction") or "unknown").strip()
                thread["last_message_id"] = str(latest.get("message_id") or "").strip()
                thread["last_message_timestamp"] = latest_timestamp
            else:
                thread["last_message_text"] = ""
                thread["last_message_direction"] = "unknown"
                thread["last_message_id"] = ""
                thread["last_message_timestamp"] = None
            thread["last_inbound_at"] = metrics["latest_customer_message_at"]
            if latest is not None and str(latest.get("direction") or "").strip().lower() == "outbound":
                thread["last_outbound_at"] = latest_timestamp
            thread["latest_customer_message_at"] = metrics["latest_customer_message_at"]
            thread["needs_reply"] = 1 if metrics["needs_reply"] else 0
            thread["unread_count"] = int(metrics["unread_count"] or 0)
            thread["last_synced_at"] = time.time()
            thread["updated_at"] = time.time()
            self._upsert_thread_record(thread)
            self._save_blocks(clean_key, blocks[-self._MAX_MESSAGES_PER_THREAD :])
            self._conn.commit()
            return True

    def update_thread_state(self, thread_key: str, updates: dict[str, Any]) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key or not isinstance(updates, dict):
            return
        with self._lock:
            state = self._load_thread_state(clean_key)
            incoming = copy.deepcopy(updates)
            if "status" in incoming and "ui_status" not in incoming:
                incoming["ui_status"] = incoming.get("status")
            incoming.pop("status", None)
            self._merge_thread_state_updates(state, incoming)
            current = self._load_thread_record(clean_key)
            reconciled = (
                self._reconcile_thread_state_locked(clean_key, thread=current, state=state)
                if isinstance(current, dict)
                else self._normalize_thread_state_payload(state)
            )
            self._save_thread_state(clean_key, reconciled)
            self._conn.commit()

    def update_thread_record(self, thread_key: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        clean_key = str(thread_key or "").strip()
        if not clean_key or not isinstance(updates, dict):
            return None
        with self._lock:
            current = self._load_thread_record(clean_key)
            if not isinstance(current, dict):
                return None
            incoming = copy.deepcopy(updates)
            if "operational_status" in incoming and "status" not in incoming:
                incoming["status"] = incoming.get("operational_status")
            incoming.pop("operational_status", None)
            state_updates = {
                key: incoming.pop(key)
                for key in list(incoming.keys())
                if key not in self._THREAD_RECORD_FIELDS
            }
            current.update(incoming)
            current["updated_at"] = time.time()
            self._upsert_thread_record(current)
            self._sync_latest_outbound_block_stage_locked(
                clean_key,
                stage_id=incoming.get("stage_id"),
                thread_updates=incoming,
            )
            state = self._load_thread_state(clean_key)
            if state_updates:
                self._merge_thread_state_updates(state, state_updates)
            reconciled_state = self._reconcile_thread_state_locked(
                clean_key,
                thread=current,
                state=state,
                stage_evidenced=True if "stage_id" in incoming or "followup_level" in incoming else None,
            )
            if state_updates or state or reconciled_state or "stage_id" in incoming or "followup_level" in incoming:
                self._save_thread_state(clean_key, reconciled_state)
            self._conn.commit()
            refreshed = self._load_thread_record(clean_key)
            return copy.deepcopy(refreshed) if isinstance(refreshed, dict) else None

    def _sync_latest_outbound_block_stage_locked(
        self,
        thread_key: str,
        *,
        stage_id: Any,
        thread_updates: dict[str, Any],
    ) -> None:
        clean_stage_id = str(stage_id or "").strip()
        if not clean_stage_id:
            return
        has_outbound_context = bool(
            "last_outbound_at" in thread_updates
            or "last_action_type" in thread_updates
            or str(thread_updates.get("last_message_direction") or "").strip().lower() == "outbound"
        )
        if not has_outbound_context:
            return
        target_message_id = str(thread_updates.get("last_message_id") or "").strip()
        blocks = self._load_blocks(thread_key)
        changed = False
        for block in reversed(blocks):
            if str(block.get("direction") or "").strip().lower() != "outbound":
                continue
            block_message_id = str(block.get("message_id") or "").strip()
            block_external_id = str(block.get("external_message_id") or "").strip()
            if target_message_id and target_message_id not in {block_message_id, block_external_id}:
                continue
            if str(block.get("stage_id") or "").strip() == clean_stage_id:
                return
            block["stage_id"] = clean_stage_id
            changed = True
            break
        if not changed:
            return
        self._save_blocks(thread_key, self._canonicalize_blocks(blocks))

    @staticmethod
    def _merge_thread_state_updates(state: dict[str, Any], updates: dict[str, Any]) -> None:
        for key, value in dict(updates or {}).items():
            if value is None:
                state.pop(str(key), None)
                continue
            state[str(key)] = value

    def _cleanup_auto_reply_pending_state_locked(
        self,
        thread_key: str,
        *,
        job_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        clean_key = str(thread_key or "").strip()
        clean_job_type = self._normalize_job_type(job_type)
        if not clean_key or clean_job_type != "auto_reply":
            return
        state = self._load_thread_state(clean_key)
        pending_reply = bool(state.get("pending_reply"))
        pending_inbound_id = str(state.get("pending_inbound_id") or "").strip()
        if not pending_reply and not pending_inbound_id:
            return
        clean_payload = dict(payload or {})
        post_send_state_updates = dict(clean_payload.get("post_send_state_updates") or {})
        inbound_id_hint = str(post_send_state_updates.get("last_inbound_id_seen") or clean_payload.get("latest_inbound_id") or "").strip()
        if inbound_id_hint and pending_inbound_id and pending_inbound_id != inbound_id_hint:
            return
        self._merge_thread_state_updates(
            state,
            {
                "pending_reply": False,
                "pending_inbound_id": None,
            },
        )
        current = self._load_thread_record(clean_key)
        reconciled = (
            self._reconcile_thread_state_locked(clean_key, thread=current, state=state)
            if isinstance(current, dict)
            else self._normalize_thread_state_payload(state)
        )
        self._save_thread_state(clean_key, reconciled)

    def mark_thread_opened(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._lock:
            thread = self._load_thread_record(clean_key)
            if not isinstance(thread, dict):
                return
            thread["unread_count"] = 0
            self._upsert_thread_record(thread)
            state = self._load_thread_state(clean_key)
            state["last_opened_at"] = time.time()
            self._save_thread_state(clean_key, state)
            self._conn.commit()

    def register_account_sync(
        self,
        account_id: str,
        *,
        last_error: str = "",
        thread_count: int | None = None,
    ) -> None:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT session_marker, session_started_at, health_state, health_reason, health_updated_at
                FROM inbox_account_state WHERE account_id = ?
                """,
                (clean_account,),
            ).fetchone()
            session_marker = str(self._row_value(existing, "session_marker", "") or "").strip()
            session_started_at = self._coerce_timestamp(self._row_value(existing, "session_started_at"))
            health_state = self._normalize_health_state(self._row_value(existing, "health_state", "healthy"))
            health_reason = str(self._row_value(existing, "health_reason", "") or "").strip()
            health_updated_at = self._coerce_timestamp(self._row_value(existing, "health_updated_at"))
            next_thread_count = max(0, int(thread_count or 0)) if thread_count is not None else 0
            self._conn.execute(
                """
                INSERT INTO inbox_account_state(
                    account_id, session_marker, session_started_at, last_sync_at, last_error, thread_count,
                    health_state, health_reason, health_updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    session_marker = excluded.session_marker,
                    session_started_at = excluded.session_started_at,
                    last_sync_at = excluded.last_sync_at,
                    last_error = excluded.last_error,
                    thread_count = excluded.thread_count,
                    health_state = excluded.health_state,
                    health_reason = excluded.health_reason,
                    health_updated_at = excluded.health_updated_at
                """,
                (
                    clean_account,
                    session_marker,
                    session_started_at,
                    time.time(),
                    str(last_error or "").strip(),
                    next_thread_count,
                    health_state,
                    health_reason,
                    health_updated_at,
                ),
            )
            self._conn.commit()

    def prepare_account_session(
        self,
        account_id: str,
        *,
        session_marker: str,
        started_at: float | None = None,
    ) -> float | None:
        clean_account = self._clean_account_id(account_id)
        clean_marker = str(session_marker or "").strip()
        if not clean_account or not clean_marker:
            return None
        requested_started_at = self._coerce_timestamp(started_at)
        session_started_at = requested_started_at or time.time()
        with self._lock:
            current = self._conn.execute(
                """
                SELECT session_marker, session_started_at, health_state, health_reason, health_updated_at
                FROM inbox_account_state WHERE account_id = ?
                """,
                (clean_account,),
            ).fetchone()
            current_marker = str(self._row_value(current, "session_marker", "") or "").strip()
            current_started_at = self._coerce_timestamp(self._row_value(current, "session_started_at"))
            if current_marker == clean_marker and current_started_at is not None:
                if requested_started_at is not None and requested_started_at + 0.000001 < current_started_at:
                    self._conn.execute(
                        "UPDATE inbox_account_state SET session_started_at = ? WHERE account_id = ?",
                        (requested_started_at, clean_account),
                    )
                    self._conn.commit()
                    return requested_started_at
                return current_started_at
            thread_rows = self._conn.execute(
                "SELECT thread_key FROM inbox_threads WHERE account_id = ?",
                (clean_account,),
            ).fetchall()
            for row in thread_rows:
                self._drop_thread_locked(str(row["thread_key"] or "").strip())
            self._conn.execute("DELETE FROM inbox_deleted_threads WHERE account_id = ?", (clean_account,))
            self._conn.execute("DELETE FROM inbox_send_queue_jobs WHERE account_id = ?", (clean_account,))
            self._conn.execute("DELETE FROM thread_action_memory WHERE account_id = ?", (clean_account,))
            health_state = self._normalize_health_state(self._row_value(current, "health_state", "healthy"))
            health_reason = str(self._row_value(current, "health_reason", "") or "").strip()
            health_updated_at = self._coerce_timestamp(self._row_value(current, "health_updated_at"))
            self._conn.execute(
                """
                INSERT INTO inbox_account_state(
                    account_id, session_marker, session_started_at, last_sync_at, last_error, thread_count,
                    health_state, health_reason, health_updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    session_marker = excluded.session_marker,
                    session_started_at = excluded.session_started_at,
                    last_sync_at = excluded.last_sync_at,
                    last_error = excluded.last_error,
                    thread_count = excluded.thread_count,
                    health_state = excluded.health_state,
                    health_reason = excluded.health_reason,
                    health_updated_at = excluded.health_updated_at
                """,
                (
                    clean_account,
                    clean_marker,
                    session_started_at,
                    None,
                    "",
                    0,
                    health_state,
                    health_reason,
                    health_updated_at,
                ),
            )
            self._conn.commit()
        return session_started_at

    def account_session_started_at(self, account_id: str) -> float | None:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return None
        with self._lock:
            return self._account_session_started_at_locked(clean_account)

    def prune_accounts(self, account_ids: set[str]) -> None:
        allowed = {self._clean_account_id(item) for item in account_ids if self._clean_account_id(item)}
        with self._lock:
            account_rows = self._conn.execute("SELECT account_id FROM inbox_account_state").fetchall()
            for row in account_rows:
                account_id = self._clean_account_id(row["account_id"])
                if account_id in allowed:
                    continue
                self._conn.execute("DELETE FROM inbox_account_state WHERE account_id = ?", (account_id,))
                self._conn.execute("DELETE FROM inbox_send_queue_jobs WHERE account_id = ?", (account_id,))
                self._conn.execute("DELETE FROM thread_action_memory WHERE account_id = ?", (account_id,))
            deleted_rows = self._conn.execute("SELECT thread_key, account_id FROM inbox_deleted_threads").fetchall()
            for row in deleted_rows:
                account_id = self._clean_account_id(row["account_id"])
                if account_id in allowed:
                    continue
                self._clear_deleted_thread_locked(str(row["thread_key"] or "").strip())
            thread_rows = self._conn.execute("SELECT thread_key, account_id FROM inbox_threads").fetchall()
            for row in thread_rows:
                account_id = self._clean_account_id(row["account_id"])
                if account_id in allowed:
                    continue
                self._drop_thread_locked(str(row["thread_key"] or "").strip())
            self._conn.commit()

    def prune_account_threads(self, account_id: str, *, keep_thread_keys: set[str]) -> list[str]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return []
        keep = {str(item or "").strip() for item in keep_thread_keys if str(item or "").strip()}
        removed: list[str] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT thread_key FROM inbox_threads WHERE account_id = ?",
                (clean_account,),
            ).fetchall()
            for row in rows:
                thread_key = str(row["thread_key"] or "").strip()
                if not thread_key or thread_key in keep:
                    continue
                state = self._load_thread_state(thread_key)
                if self._thread_has_pending_work_locked(thread_key, state):
                    continue
                self._drop_thread_locked(thread_key)
                removed.append(thread_key)
            if removed:
                self._conn.commit()
        return removed

    def set_account_health(
        self,
        account_id: str,
        state: str,
        *,
        reason: str = "",
        updated_at: float | None = None,
    ) -> None:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return
        normalized_state = self._normalize_health_state(state)
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT session_marker, session_started_at, last_sync_at, last_error, thread_count
                FROM inbox_account_state WHERE account_id = ?
                """,
                (clean_account,),
            ).fetchone()
            self._conn.execute(
                """
                INSERT INTO inbox_account_state(
                    account_id, session_marker, session_started_at, last_sync_at, last_error, thread_count,
                    health_state, health_reason, health_updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    session_marker = excluded.session_marker,
                    session_started_at = excluded.session_started_at,
                    last_sync_at = excluded.last_sync_at,
                    last_error = excluded.last_error,
                    thread_count = excluded.thread_count,
                    health_state = excluded.health_state,
                    health_reason = excluded.health_reason,
                    health_updated_at = excluded.health_updated_at
                """,
                (
                    clean_account,
                    str(self._row_value(existing, "session_marker", "") or "").strip(),
                    self._coerce_timestamp(self._row_value(existing, "session_started_at")),
                    self._coerce_timestamp(self._row_value(existing, "last_sync_at")),
                    str(self._row_value(existing, "last_error", "") or "").strip(),
                    max(0, int(self._row_value(existing, "thread_count", 0) or 0)),
                    normalized_state,
                    str(reason or "").strip(),
                    self._coerce_timestamp(updated_at) or time.time(),
                ),
            )
            self._conn.commit()

    def get_account_health(self, account_id: str) -> dict[str, Any]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return {"state": "unknown", "reason": ""}
        with self._lock:
            row = self._conn.execute(
                "SELECT health_state, health_reason, health_updated_at FROM inbox_account_state WHERE account_id = ?",
                (clean_account,),
            ).fetchone()
        if row is None:
            return {"state": "healthy", "reason": ""}
        return {
            "state": self._normalize_health_state(row["health_state"]),
            "reason": str(row["health_reason"] or "").strip(),
            "updated_at": self._coerce_timestamp(row["health_updated_at"]),
        }

    def thread_has_action_since(
        self,
        account_id: str,
        thread_id: str,
        *,
        action_types: list[str] | tuple[str, ...],
        started_at: float | None = None,
    ) -> bool:
        clean_account = self._clean_account_id(account_id)
        clean_thread = str(thread_id or "").strip()
        clean_actions = [str(item or "").strip() for item in action_types if str(item or "").strip()]
        if not clean_account or not clean_thread or not clean_actions:
            return False
        placeholders = ",".join("?" for _ in clean_actions)
        query = (
            "SELECT 1 FROM thread_action_memory "
            f"WHERE account_id = ? AND thread_id = ? AND action_type IN ({placeholders})"
        )
        params: list[Any] = [clean_account, clean_thread, *clean_actions]
        started_value = self._coerce_timestamp(started_at)
        if started_value is not None:
            query += " AND created_at >= ?"
            params.append(started_value)
        query += " ORDER BY created_at DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(query, tuple(params)).fetchone()
        return row is not None

    def record_action_memory(
        self,
        thread_id: str,
        account_id: str,
        action_type: str,
        *,
        pack_id: str = "",
        source: str = "inbox_rm",
        created_at: float | None = None,
    ) -> None:
        clean_thread = str(thread_id or "").strip()
        clean_account = self._clean_account_id(account_id)
        clean_action = str(action_type or "").strip()
        if not clean_thread or not clean_account or not clean_action:
            return
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO thread_action_memory(
                    thread_id, account_id, action_type, pack_id, source, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    clean_thread,
                    clean_account,
                    clean_action,
                    str(pack_id or "").strip(),
                    str(source or "").strip(),
                    self._coerce_timestamp(created_at) or time.time(),
                ),
            )
            self._conn.commit()

    def create_send_queue_job(
        self,
        task_type: str,
        *,
        thread_key: str,
        account_id: str,
        payload: dict[str, Any],
        dedupe_key: str = "",
        priority: int | None = None,
        state: str = "queued",
        scheduled_at: float | None = None,
    ) -> int:
        result = self.enqueue_send_queue_job(
            task_type,
            thread_key=thread_key,
            account_id=account_id,
            payload=payload,
            dedupe_key=dedupe_key,
            priority=priority,
            state=state,
            scheduled_at=scheduled_at,
        )
        return int(result.get("job_id") or 0)

    @staticmethod
    def _send_queue_payload_content_kind(payload: dict[str, Any] | None) -> str:
        clean_payload = dict(payload or {})
        if str(clean_payload.get("pack_id") or "").strip():
            return "pack"
        if (
            str(clean_payload.get("text") or "").strip()
            or str(clean_payload.get("local_message_id") or "").strip()
        ):
            return "text"
        return ""

    @classmethod
    def _merge_reused_send_queue_payload(
        cls,
        existing_payload: dict[str, Any] | None,
        incoming_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged = copy.deepcopy(dict(existing_payload or {}))
        for key, value in dict(incoming_payload or {}).items():
            if key not in merged or merged.get(key) in (None, "", [], (), {}):
                merged[key] = copy.deepcopy(value)
        for stable_key in ("thread_key", "local_message_id", "text", "pack_id", "job_type", "dedupe_key"):
            stable_value = (existing_payload or {}).get(stable_key)
            if stable_value not in (None, "", [], (), {}):
                merged[stable_key] = copy.deepcopy(stable_value)
        return merged

    def enqueue_send_queue_job(
        self,
        task_type: str,
        *,
        thread_key: str,
        account_id: str,
        payload: dict[str, Any],
        dedupe_key: str = "",
        priority: int | None = None,
        state: str = "queued",
        scheduled_at: float | None = None,
    ) -> dict[str, Any]:
        clean_job_type = self._normalize_job_type(task_type)
        if not clean_job_type:
            return {
                "ok": False,
                "job_id": 0,
                "created": False,
                "reused": False,
                "dedupe_key": str(dedupe_key or "").strip(),
                "state": self._normalize_job_state(state),
                "payload": {},
            }
        now = time.time()
        job_priority = self._priority_for_job_type(clean_job_type) if priority is None else int(priority)
        scheduled = self._coerce_timestamp(scheduled_at) or now
        clean_thread_key = str(thread_key or "").strip()
        clean_account_id = self._clean_account_id(account_id)
        clean_payload = dict(payload or {})
        clean_dedupe = str(dedupe_key or "").strip()
        if clean_thread_key and not str(clean_payload.get("thread_key") or "").strip():
            clean_payload["thread_key"] = clean_thread_key
        if clean_job_type and not str(clean_payload.get("job_type") or "").strip():
            clean_payload["job_type"] = clean_job_type
        if clean_dedupe and not str(clean_payload.get("dedupe_key") or "").strip():
            clean_payload["dedupe_key"] = clean_dedupe
        with self._lock:
            if clean_dedupe:
                existing = self._conn.execute(
                    """
                    SELECT id, task_type, job_type, dedupe_key, thread_key, account_id, payload_json,
                           priority, state, attempt_count, scheduled_at, started_at, finished_at,
                           error_message, failure_reason, created_at, updated_at
                    FROM inbox_send_queue_jobs
                    WHERE dedupe_key = ?
                      AND state IN ('queued', 'processing')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (clean_dedupe,),
                ).fetchone()
                if existing is not None:
                    existing_payload = self._decode_json_dict(self._row_value(existing, "payload_json", "{}"))
                    merged_payload = self._merge_reused_send_queue_payload(existing_payload, clean_payload)
                    if merged_payload != existing_payload:
                        self._conn.execute(
                            """
                            UPDATE inbox_send_queue_jobs
                            SET payload_json = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (
                                self._encode_json(merged_payload),
                                now,
                                int(self._row_value(existing, "id", 0) or 0),
                            ),
                        )
                        self._conn.commit()
                    return {
                        "ok": True,
                        "job_id": int(self._row_value(existing, "id", 0) or 0),
                        "created": False,
                        "reused": True,
                        "dedupe_key": clean_dedupe,
                        "state": self._normalize_job_state(self._row_value(existing, "state", "queued")),
                        "payload": merged_payload,
                    }
            cursor = self._conn.execute(
                """
                INSERT INTO inbox_send_queue_jobs(
                    task_type, job_type, dedupe_key, thread_key, account_id, payload_json,
                    priority, state, attempt_count, scheduled_at, started_at, finished_at,
                    error_message, failure_reason, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, '', '', ?, ?)
                """,
                (
                    clean_job_type,
                    clean_job_type,
                    clean_dedupe,
                    clean_thread_key,
                    clean_account_id,
                    self._encode_json(clean_payload),
                    int(job_priority),
                    self._normalize_job_state(state),
                    scheduled,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            return {
                "ok": True,
                "job_id": int(cursor.lastrowid or 0),
                "created": True,
                "reused": False,
                "dedupe_key": clean_dedupe,
                "state": self._normalize_job_state(state),
                "payload": clean_payload,
            }

    def _reconcile_send_queue_thread_state_locked(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        current = self._load_thread_record(clean_key)
        if not isinstance(current, dict):
            return
        rows = self._conn.execute(
            """
            SELECT payload_json, state
            FROM inbox_send_queue_jobs
            WHERE thread_key = ?
              AND state IN ('queued', 'processing')
            ORDER BY id ASC
            """,
            (clean_key,),
        ).fetchall()
        has_active_jobs = bool(rows)
        has_active_pack = False
        has_processing_pack = False
        has_processing_job = False
        for row in rows:
            payload = self._decode_json_dict(self._row_value(row, "payload_json", "{}"))
            content_kind = self._send_queue_payload_content_kind(payload)
            job_state = self._normalize_job_state(self._row_value(row, "state", "queued"))
            if job_state == "processing":
                has_processing_job = True
            if content_kind == "pack":
                has_active_pack = True
                if job_state == "processing":
                    has_processing_pack = True
        state = self._load_thread_state(clean_key)
        sender_status = str(state.get("sender_status") or "").strip().lower()
        pack_status = str(state.get("pack_status") or "").strip().lower()
        updates: dict[str, Any] = {}
        if has_active_jobs:
            updates["sender_status"] = "sending" if has_processing_job else "queued"
            updates["sender_error"] = ""
            updates["thread_error"] = ""
        elif sender_status in {"queued", "sending"}:
            updates["sender_status"] = "ready"
            updates["sender_error"] = ""
            updates["thread_error"] = ""
        if has_active_pack:
            updates["pack_status"] = "running" if has_processing_pack else "queued"
            updates["pack_error"] = ""
        elif pack_status in {"queued", "running"}:
            updates["pack_status"] = None
            updates["pack_error"] = None
        if not updates:
            return
        self._merge_thread_state_updates(state, updates)
        reconciled = self._reconcile_thread_state_locked(clean_key, thread=current, state=state)
        self._save_thread_state(clean_key, reconciled)

    def reconcile_send_queue_thread_state(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._lock:
            self._reconcile_send_queue_thread_state_locked(clean_key)
            self._conn.commit()

    def update_send_queue_job(
        self,
        job_id: int,
        *,
        state: str,
        error_message: str = "",
        failure_reason: str = "",
        started_at: float | None = None,
        finished_at: float | None = None,
        increment_attempt: bool = False,
    ) -> None:
        if int(job_id or 0) <= 0:
            return
        next_state = self._normalize_job_state(state)
        with self._lock:
            updates = [
                "state = ?",
                "error_message = ?",
                "failure_reason = ?",
                "started_at = COALESCE(?, started_at)",
                "finished_at = COALESCE(?, finished_at)",
                "updated_at = ?",
                "job_type = COALESCE(NULLIF(job_type, ''), task_type)",
            ]
            if increment_attempt:
                updates.append("attempt_count = attempt_count + 1")
            query = (
                "UPDATE inbox_send_queue_jobs SET "
                + ", ".join(updates)
                + " WHERE id = ?"
            )
            self._conn.execute(
                query,
                (
                    next_state,
                    str(error_message or "").strip(),
                    str(failure_reason or error_message or "").strip(),
                    self._coerce_timestamp(started_at),
                    self._coerce_timestamp(finished_at),
                    time.time(),
                    int(job_id),
                ),
            )
            self._conn.commit()

    def get_send_queue_job(self, job_id: int) -> dict[str, Any] | None:
        if int(job_id or 0) <= 0:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, task_type, job_type, dedupe_key, thread_key, account_id, payload_json,
                       priority, state, attempt_count, scheduled_at, started_at, finished_at,
                       error_message, failure_reason, created_at, updated_at
                FROM inbox_send_queue_jobs
                WHERE id = ?
                LIMIT 1
                """,
                (int(job_id),),
            ).fetchone()
        if row is None:
            return None
        raw_payload = str(self._row_value(row, "payload_json", "") or "").strip()
        try:
            decoded_payload = json.loads(raw_payload) if raw_payload else {}
        except Exception:
            decoded_payload = {}
        return {
            "id": int(self._row_value(row, "id", 0) or 0),
            "task_type": str(self._row_value(row, "task_type", "") or "").strip(),
            "job_type": self._normalize_job_type(self._row_value(row, "job_type", self._row_value(row, "task_type", ""))),
            "dedupe_key": str(self._row_value(row, "dedupe_key", "") or "").strip(),
            "thread_key": str(self._row_value(row, "thread_key", "") or "").strip(),
            "account_id": self._clean_account_id(self._row_value(row, "account_id", "")),
            "payload": decoded_payload if isinstance(decoded_payload, dict) else {},
            "priority": int(self._row_value(row, "priority", 0) or 0),
            "state": self._normalize_job_state(self._row_value(row, "state", "queued")),
            "attempt_count": int(self._row_value(row, "attempt_count", 0) or 0),
            "scheduled_at": self._coerce_timestamp(self._row_value(row, "scheduled_at")),
            "started_at": self._coerce_timestamp(self._row_value(row, "started_at")),
            "finished_at": self._coerce_timestamp(self._row_value(row, "finished_at")),
            "error_message": str(self._row_value(row, "error_message", "") or "").strip(),
            "failure_reason": str(self._row_value(row, "failure_reason", "") or "").strip(),
            "created_at": self._coerce_timestamp(self._row_value(row, "created_at")),
            "updated_at": self._coerce_timestamp(self._row_value(row, "updated_at")),
        }

    def list_send_queue_jobs(
        self,
        *,
        states: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit or 100))
        clean_states = [
            self._normalize_job_state(item)
            for item in (states or [])
            if self._normalize_job_state(item)
        ]
        query = """
            SELECT id, task_type, job_type, dedupe_key, thread_key, account_id, payload_json,
                   priority, state, attempt_count, scheduled_at, started_at, finished_at,
                   error_message, failure_reason, created_at, updated_at
            FROM inbox_send_queue_jobs
        """
        params: list[Any] = []
        if clean_states:
            placeholders = ",".join("?" for _ in clean_states)
            query += f" WHERE state IN ({placeholders})"
            params.extend(clean_states)
        query += " ORDER BY priority DESC, scheduled_at ASC, created_at ASC, id ASC LIMIT ?"
        params.append(safe_limit)
        with self._lock:
            rows = self._conn.execute(query, tuple(params)).fetchall()
        payload: list[dict[str, Any]] = []
        for row in rows:
            raw_payload = str(row["payload_json"] or "").strip()
            try:
                decoded_payload = json.loads(raw_payload) if raw_payload else {}
            except Exception:
                decoded_payload = {}
            payload.append(
                {
                    "id": int(row["id"] or 0),
                    "task_type": str(row["task_type"] or "").strip(),
                    "job_type": self._normalize_job_type(self._row_value(row, "job_type", row["task_type"])),
                    "dedupe_key": str(row["dedupe_key"] or "").strip(),
                    "thread_key": str(row["thread_key"] or "").strip(),
                    "account_id": self._clean_account_id(row["account_id"]),
                    "payload": decoded_payload if isinstance(decoded_payload, dict) else {},
                    "priority": int(self._row_value(row, "priority", 0) or 0),
                    "state": self._normalize_job_state(row["state"]),
                    "attempt_count": int(self._row_value(row, "attempt_count", 0) or 0),
                    "scheduled_at": self._coerce_timestamp(self._row_value(row, "scheduled_at")),
                    "started_at": self._coerce_timestamp(self._row_value(row, "started_at")),
                    "finished_at": self._coerce_timestamp(self._row_value(row, "finished_at")),
                    "error_message": str(row["error_message"] or "").strip(),
                    "failure_reason": str(self._row_value(row, "failure_reason", "") or "").strip(),
                    "created_at": self._coerce_timestamp(row["created_at"]),
                    "updated_at": self._coerce_timestamp(row["updated_at"]),
                }
            )
        return payload

    def claim_next_send_queue_job(
        self,
        account_id: str,
        *,
        allowed_states: list[str] | None = None,
    ) -> dict[str, Any] | None:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return None
        candidate_states = allowed_states or ["queued"]
        normalized_states = [self._normalize_job_state(item) for item in candidate_states if self._normalize_job_state(item)]
        if not normalized_states:
            normalized_states = ["queued"]
        placeholders = ",".join("?" for _ in normalized_states)
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT id
                FROM inbox_send_queue_jobs
                WHERE account_id = ?
                  AND state IN ({placeholders})
                  AND COALESCE(scheduled_at, created_at) <= ?
                ORDER BY priority DESC, scheduled_at ASC, created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_account, *normalized_states, now),
            ).fetchone()
            if row is None:
                return None
            job_id = int(self._row_value(row, "id", 0) or 0)
            self.update_send_queue_job(
                job_id,
                state="processing",
                started_at=now,
                increment_attempt=True,
            )
        jobs = self.list_send_queue_jobs(states=["processing"], limit=200)
        for job in jobs:
            if int(job.get("id") or 0) == job_id:
                return job
        return None

    def cancel_send_queue_jobs(
        self,
        *,
        thread_key: str = "",
        account_id: str = "",
        alias_id: str = "",
        job_types: list[str] | None = None,
        states: list[str] | None = None,
        reason: str = "cancelled",
    ) -> int:
        clean_thread = str(thread_key or "").strip()
        clean_account = self._clean_account_id(account_id)
        clean_alias = str(alias_id or "").strip()
        normalized_job_types = [
            self._normalize_job_type(item)
            for item in (job_types or [])
            if self._normalize_job_type(item)
        ]
        normalized_states = [
            self._normalize_job_state(item)
            for item in (states or ["queued", "processing"])
            if self._normalize_job_state(item)
        ]
        if not clean_thread and not clean_account and not clean_alias:
            return 0
        if not normalized_states:
            normalized_states = ["queued", "processing"]
        filters: list[str] = []
        params: list[Any] = []
        if clean_thread:
            filters.append("jobs.thread_key = ?")
            params.append(clean_thread)
        if clean_account:
            filters.append("jobs.account_id = ?")
            params.append(clean_account)
        if clean_alias:
            filters.append("LOWER(COALESCE(NULLIF(threads.alias_id, ''), NULLIF(threads.account_alias, ''))) = ?")
            params.append(clean_alias.lower())
        if normalized_job_types:
            placeholders = ",".join("?" for _ in normalized_job_types)
            filters.append(f"jobs.job_type IN ({placeholders})")
            params.extend(normalized_job_types)
        state_placeholders = ",".join("?" for _ in normalized_states)
        filters.append(f"jobs.state IN ({state_placeholders})")
        params.extend(normalized_states)
        query = (
            "SELECT jobs.id, jobs.thread_key, jobs.job_type, jobs.account_id, jobs.payload_json, "
            "       COALESCE(NULLIF(threads.alias_id, ''), NULLIF(threads.account_alias, '')) AS alias_id "
            "FROM inbox_send_queue_jobs AS jobs "
            "LEFT JOIN inbox_threads AS threads ON threads.thread_key = jobs.thread_key "
            "WHERE "
            + " AND ".join(filters)
            + " ORDER BY jobs.id ASC"
        )
        with self._lock:
            rows = self._conn.execute(query, tuple(params)).fetchall()
            if not rows:
                return 0
            now = time.time()
            cancelled = 0
            touched_threads: set[str] = set()
            for row in rows:
                payload = self._decode_json_dict(self._row_value(row, "payload_json", "{}"))
                local_message_id = str(payload.get("local_message_id") or "").strip()
                job_thread_key = str(self._row_value(row, "thread_key", "") or "").strip()
                job_type = self._normalize_job_type(self._row_value(row, "job_type", ""))
                self._conn.execute(
                    """
                    UPDATE inbox_send_queue_jobs
                    SET state = 'cancelled',
                        error_message = ?,
                        failure_reason = ?,
                        finished_at = COALESCE(finished_at, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(reason or "cancelled").strip(),
                        str(reason or "cancelled").strip(),
                        now,
                        now,
                        int(self._row_value(row, "id", 0) or 0),
                    ),
                )
                if job_thread_key and local_message_id:
                    self.set_local_outbound_status(
                        job_thread_key,
                        local_message_id,
                        status="failed",
                        error_message=str(reason or "cancelled").strip(),
                    )
                if job_thread_key:
                    touched_threads.add(job_thread_key)
                    self._cleanup_auto_reply_pending_state_locked(
                        job_thread_key,
                        job_type=job_type,
                        payload=payload,
                    )
                    reason_code = normalize_reason_code(str(reason or "cancelled").strip())
                    event_type = "job_cancelled"
                    if reason_code == "job_cancelled_by_takeover":
                        event_type = "job_cancelled_by_takeover"
                    elif reason_code == "job_cancelled_by_runtime_stop":
                        event_type = "job_cancelled_by_runtime_stop"
                    self.record_diagnostic_event(
                        account_id=self._clean_account_id(self._row_value(row, "account_id", "")),
                        alias_id=str(self._row_value(row, "alias_id", "") or "").strip(),
                        thread_key=job_thread_key,
                        job_type=job_type,
                        stage="job_cancel",
                        event_type=event_type,
                        outcome="cancel",
                        reason_code=reason_code,
                        reason=str(reason or "cancelled").strip(),
                        file=str(Path(__file__)),
                        function="cancel_send_queue_jobs",
                        line=0,
                        payload={
                            "job_id": int(self._row_value(row, "id", 0) or 0),
                            "local_message_id": local_message_id,
                            "cancelled_by": "InboxStorage.cancel_send_queue_jobs",
                        },
                        created_at=now,
                    )
                cancelled += 1
            for item in touched_threads:
                self._reconcile_send_queue_thread_state_locked(item)
            self._conn.commit()
        return cancelled

    def add_thread_event(
        self,
        thread_key: str,
        event_type: str,
        *,
        account_id: str = "",
        alias_id: str = "",
        payload: dict[str, Any] | None = None,
        created_at: float | None = None,
    ) -> None:
        clean_key = str(thread_key or "").strip()
        clean_event = str(event_type or "").strip().lower()
        if not clean_key or not clean_event:
            return
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO inbox_thread_events(thread_key, account_id, alias_id, event_type, payload_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    clean_key,
                    self._clean_account_id(account_id),
                    str(alias_id or "").strip(),
                    clean_event,
                    self._encode_json(dict(payload or {})),
                    self._coerce_timestamp(created_at) or time.time(),
                ),
            )
            self._conn.commit()

    def record_diagnostic_event(
        self,
        *,
        account_id: str = "",
        alias_id: str = "",
        thread_key: str = "",
        job_type: str = "",
        stage: str,
        event_type: str,
        outcome: str,
        reason_code: str = "",
        reason: str = "",
        file: str = "",
        function: str = "",
        line: int = 0,
        exception_type: str = "",
        exception_message: str = "",
        traceback: str = "",
        payload: dict[str, Any] | None = None,
        created_at: float | None = None,
    ) -> int:
        clean_event = str(event_type or "").strip().lower()
        clean_stage = str(stage or "").strip().lower()
        if not clean_event or not clean_stage:
            return 0
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO inbox_diagnostic_events(
                    created_at, account_id, alias_id, thread_key, job_type, stage, event_type,
                    outcome, reason_code, reason, file, function, line, exception_type,
                    exception_message, traceback, payload_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._coerce_timestamp(created_at) or time.time(),
                    self._clean_account_id(account_id),
                    str(alias_id or "").strip(),
                    str(thread_key or "").strip(),
                    self._normalize_job_type(job_type),
                    clean_stage,
                    clean_event,
                    str(outcome or "").strip().lower(),
                    str(reason_code or "").strip().lower(),
                    str(reason or "").strip(),
                    str(file or "").strip(),
                    str(function or "").strip(),
                    max(0, int(line or 0)),
                    str(exception_type or "").strip(),
                    str(exception_message or "").strip(),
                    str(traceback or ""),
                    self._encode_json(dict(payload or {})),
                ),
            )
            self._conn.commit()
            return int(cursor.lastrowid or 0)

    def list_diagnostic_events(
        self,
        *,
        limit: int = 100,
        account_id: str = "",
        alias_id: str = "",
        thread_key: str = "",
        event_type: str = "",
        stage: str = "",
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit or 100))
        filters: list[str] = []
        params: list[Any] = []
        clean_account = self._clean_account_id(account_id)
        clean_alias = str(alias_id or "").strip()
        clean_thread = str(thread_key or "").strip()
        clean_event = str(event_type or "").strip().lower()
        clean_stage = str(stage or "").strip().lower()
        if clean_account:
            filters.append("account_id = ?")
            params.append(clean_account)
        if clean_alias:
            filters.append("alias_id = ?")
            params.append(clean_alias)
        if clean_thread:
            filters.append("thread_key = ?")
            params.append(clean_thread)
        if clean_event:
            filters.append("event_type = ?")
            params.append(clean_event)
        if clean_stage:
            filters.append("stage = ?")
            params.append(clean_stage)
        query = """
            SELECT id, created_at, account_id, alias_id, thread_key, job_type, stage, event_type,
                   outcome, reason_code, reason, file, function, line, exception_type,
                   exception_message, traceback, payload_json
            FROM inbox_diagnostic_events
        """
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(safe_limit)
        with self._lock:
            rows = self._conn.execute(query, tuple(params)).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            events.append(
                {
                    "id": int(self._row_value(row, "id", 0) or 0),
                    "created_at": self._coerce_timestamp(self._row_value(row, "created_at")),
                    "account_id": self._clean_account_id(self._row_value(row, "account_id", "")),
                    "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
                    "thread_key": str(self._row_value(row, "thread_key", "") or "").strip(),
                    "job_type": self._normalize_job_type(self._row_value(row, "job_type", "")),
                    "stage": str(self._row_value(row, "stage", "") or "").strip(),
                    "event_type": str(self._row_value(row, "event_type", "") or "").strip(),
                    "outcome": str(self._row_value(row, "outcome", "") or "").strip(),
                    "reason_code": str(self._row_value(row, "reason_code", "") or "").strip(),
                    "reason": str(self._row_value(row, "reason", "") or "").strip(),
                    "file": str(self._row_value(row, "file", "") or "").strip(),
                    "function": str(self._row_value(row, "function", "") or "").strip(),
                    "line": int(self._row_value(row, "line", 0) or 0),
                    "exception_type": str(self._row_value(row, "exception_type", "") or "").strip(),
                    "exception_message": str(self._row_value(row, "exception_message", "") or "").strip(),
                    "traceback": str(self._row_value(row, "traceback", "") or ""),
                    "payload": self._decode_json_dict(self._row_value(row, "payload_json", "{}")),
                }
            )
        return events

    def list_thread_events(self, thread_key: str, *, limit: int = 50) -> list[dict[str, Any]]:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return []
        safe_limit = max(1, int(limit or 50))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT thread_key, account_id, alias_id, event_type, payload_json, created_at
                FROM inbox_thread_events
                WHERE thread_key = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (clean_key, safe_limit),
            ).fetchall()
        payload: list[dict[str, Any]] = []
        for row in rows:
            payload.append(
                {
                    "thread_key": str(self._row_value(row, "thread_key", "") or "").strip(),
                    "account_id": self._clean_account_id(self._row_value(row, "account_id", "")),
                    "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
                    "event_type": str(self._row_value(row, "event_type", "") or "").strip(),
                    "payload": self._decode_json_dict(self._row_value(row, "payload_json", "{}")),
                    "created_at": self._coerce_timestamp(self._row_value(row, "created_at")),
                }
            )
        return payload

    def get_runtime_alias_state(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return {}
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runtime_alias_state WHERE alias_id = ?",
                (clean_alias,),
            ).fetchone()
        if row is None:
            return {}
        return self._runtime_alias_row_to_payload(row)

    def upsert_runtime_alias_state(self, alias_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias or not isinstance(updates, dict):
            return {}
        current = self.get_runtime_alias_state(clean_alias)

        def _string_field(name: str) -> str:
            if name in updates:
                return str(updates.get(name) or "").strip()
            return str(current.get(name) or "").strip()

        def _int_field(name: str) -> int:
            if name not in updates:
                try:
                    return max(0, int(current.get(name) or 0))
                except Exception:
                    return 0
            try:
                return max(0, int(updates.get(name) or 0))
            except Exception:
                return 0

        def _timestamp_field(name: str) -> float | None:
            if name in updates:
                return self._coerce_timestamp(updates.get(name))
            return self._coerce_timestamp(current.get(name))

        merged = {
            "alias_id": clean_alias,
            "is_running": bool(updates.get("is_running", current.get("is_running", False))),
            "worker_state": str(updates.get("worker_state") or current.get("worker_state") or "stopped").strip() or "stopped",
            "current_account_id": _string_field("current_account_id"),
            "current_turn_count": max(0, int(updates.get("current_turn_count", current.get("current_turn_count", 0)) or 0)),
            "max_turns_per_account": max(1, int(updates.get("max_turns_per_account", current.get("max_turns_per_account", 1)) or 1)),
            "delay_min_ms": max(0, int(updates.get("delay_min_ms", current.get("delay_min_ms", 0)) or 0)),
            "delay_max_ms": max(0, int(updates.get("delay_max_ms", current.get("delay_max_ms", 0)) or 0)),
            "mode": self._normalize_runtime_mode(updates.get("mode") or current.get("mode") or "both"),
            "next_account_id": _string_field("next_account_id"),
            "last_send_attempt_account_id": self._clean_account_id(_string_field("last_send_attempt_account_id")),
            "last_send_attempt_thread_key": _string_field("last_send_attempt_thread_key"),
            "last_send_attempt_job_id": _int_field("last_send_attempt_job_id"),
            "last_send_attempt_job_type": _string_field("last_send_attempt_job_type"),
            "last_send_attempt_at": _timestamp_field("last_send_attempt_at"),
            "last_send_attempt_outcome": _string_field("last_send_attempt_outcome"),
            "last_send_attempt_reason_code": _string_field("last_send_attempt_reason_code"),
            "last_send_outcome": _string_field("last_send_outcome"),
            "last_send_reason_code": _string_field("last_send_reason_code"),
            "last_send_reason": _string_field("last_send_reason"),
            "last_send_account_id": self._clean_account_id(_string_field("last_send_account_id")),
            "last_send_thread_key": _string_field("last_send_thread_key"),
            "last_send_job_id": _int_field("last_send_job_id"),
            "last_send_job_type": _string_field("last_send_job_type"),
            "last_send_at": _timestamp_field("last_send_at"),
            "last_send_exception_type": _string_field("last_send_exception_type"),
            "last_send_exception_message": _string_field("last_send_exception_message"),
            "last_heartbeat_at": self._coerce_timestamp(updates.get("last_heartbeat_at") or current.get("last_heartbeat_at")),
            "last_error": _string_field("last_error"),
            "stats": dict(updates.get("stats") or current.get("stats") or {}),
            "updated_at": self._coerce_timestamp(updates.get("updated_at")) or time.time(),
        }
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO runtime_alias_state(
                    alias_id, is_running, worker_state, current_account_id, current_turn_count,
                    max_turns_per_account, delay_min_ms, delay_max_ms, mode,
                    next_account_id,
                    last_send_attempt_account_id, last_send_attempt_thread_key, last_send_attempt_job_id,
                    last_send_attempt_job_type, last_send_attempt_at, last_send_attempt_outcome, last_send_attempt_reason_code,
                    last_send_outcome, last_send_reason_code, last_send_reason,
                    last_send_account_id, last_send_thread_key, last_send_job_id, last_send_job_type, last_send_at,
                    last_send_exception_type, last_send_exception_message,
                    last_heartbeat_at, last_error, stats_json, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(alias_id) DO UPDATE SET
                    is_running = excluded.is_running,
                    worker_state = excluded.worker_state,
                    current_account_id = excluded.current_account_id,
                    current_turn_count = excluded.current_turn_count,
                    max_turns_per_account = excluded.max_turns_per_account,
                    delay_min_ms = excluded.delay_min_ms,
                    delay_max_ms = excluded.delay_max_ms,
                    mode = excluded.mode,
                    next_account_id = excluded.next_account_id,
                    last_send_attempt_account_id = excluded.last_send_attempt_account_id,
                    last_send_attempt_thread_key = excluded.last_send_attempt_thread_key,
                    last_send_attempt_job_id = excluded.last_send_attempt_job_id,
                    last_send_attempt_job_type = excluded.last_send_attempt_job_type,
                    last_send_attempt_at = excluded.last_send_attempt_at,
                    last_send_attempt_outcome = excluded.last_send_attempt_outcome,
                    last_send_attempt_reason_code = excluded.last_send_attempt_reason_code,
                    last_send_outcome = excluded.last_send_outcome,
                    last_send_reason_code = excluded.last_send_reason_code,
                    last_send_reason = excluded.last_send_reason,
                    last_send_account_id = excluded.last_send_account_id,
                    last_send_thread_key = excluded.last_send_thread_key,
                    last_send_job_id = excluded.last_send_job_id,
                    last_send_job_type = excluded.last_send_job_type,
                    last_send_at = excluded.last_send_at,
                    last_send_exception_type = excluded.last_send_exception_type,
                    last_send_exception_message = excluded.last_send_exception_message,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    last_error = excluded.last_error,
                    stats_json = excluded.stats_json,
                    updated_at = excluded.updated_at
                """,
                (
                    clean_alias,
                    1 if merged["is_running"] else 0,
                    merged["worker_state"],
                    merged["current_account_id"],
                    merged["current_turn_count"],
                    merged["max_turns_per_account"],
                    merged["delay_min_ms"],
                    merged["delay_max_ms"],
                    merged["mode"],
                    merged["next_account_id"],
                    merged["last_send_attempt_account_id"],
                    merged["last_send_attempt_thread_key"],
                    merged["last_send_attempt_job_id"],
                    merged["last_send_attempt_job_type"],
                    merged["last_send_attempt_at"],
                    merged["last_send_attempt_outcome"],
                    merged["last_send_attempt_reason_code"],
                    merged["last_send_outcome"],
                    merged["last_send_reason_code"],
                    merged["last_send_reason"],
                    merged["last_send_account_id"],
                    merged["last_send_thread_key"],
                    merged["last_send_job_id"],
                    merged["last_send_job_type"],
                    merged["last_send_at"],
                    merged["last_send_exception_type"],
                    merged["last_send_exception_message"],
                    merged["last_heartbeat_at"],
                    merged["last_error"],
                    self._encode_json(merged["stats"]),
                    merged["updated_at"],
                ),
            )
            self._conn.commit()
        return merged

    def list_runtime_alias_states(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM runtime_alias_state ORDER BY alias_id ASC").fetchall()
        return [self._runtime_alias_row_to_payload(row) for row in rows]

    def delete_runtime_alias_state(self, alias_id: str) -> bool:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return False
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM runtime_alias_state WHERE alias_id = ?",
                (clean_alias,),
            )
            self._conn.commit()
        return int(cursor.rowcount or 0) > 0

    def _runtime_alias_row_to_payload(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return {
            "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
            "is_running": bool(self._row_value(row, "is_running", 0)),
            "worker_state": str(self._row_value(row, "worker_state", "stopped") or "stopped").strip() or "stopped",
            "current_account_id": self._clean_account_id(self._row_value(row, "current_account_id", "")),
            "current_turn_count": max(0, int(self._row_value(row, "current_turn_count", 0) or 0)),
            "max_turns_per_account": max(1, int(self._row_value(row, "max_turns_per_account", 1) or 1)),
            "delay_min_ms": max(0, int(self._row_value(row, "delay_min_ms", 0) or 0)),
            "delay_max_ms": max(0, int(self._row_value(row, "delay_max_ms", 0) or 0)),
            "mode": self._normalize_runtime_mode(self._row_value(row, "mode", "both")),
            "next_account_id": self._clean_account_id(self._row_value(row, "next_account_id", "")),
            "last_send_attempt_account_id": self._clean_account_id(self._row_value(row, "last_send_attempt_account_id", "")),
            "last_send_attempt_thread_key": str(self._row_value(row, "last_send_attempt_thread_key", "") or "").strip(),
            "last_send_attempt_job_id": max(0, int(self._row_value(row, "last_send_attempt_job_id", 0) or 0)),
            "last_send_attempt_job_type": str(self._row_value(row, "last_send_attempt_job_type", "") or "").strip(),
            "last_send_attempt_at": self._coerce_timestamp(self._row_value(row, "last_send_attempt_at")),
            "last_send_attempt_outcome": str(self._row_value(row, "last_send_attempt_outcome", "") or "").strip(),
            "last_send_attempt_reason_code": str(self._row_value(row, "last_send_attempt_reason_code", "") or "").strip(),
            "last_send_outcome": str(self._row_value(row, "last_send_outcome", "") or "").strip(),
            "last_send_reason_code": str(self._row_value(row, "last_send_reason_code", "") or "").strip(),
            "last_send_reason": str(self._row_value(row, "last_send_reason", "") or "").strip(),
            "last_send_account_id": self._clean_account_id(self._row_value(row, "last_send_account_id", "")),
            "last_send_thread_key": str(self._row_value(row, "last_send_thread_key", "") or "").strip(),
            "last_send_job_id": max(0, int(self._row_value(row, "last_send_job_id", 0) or 0)),
            "last_send_job_type": str(self._row_value(row, "last_send_job_type", "") or "").strip(),
            "last_send_at": self._coerce_timestamp(self._row_value(row, "last_send_at")),
            "last_send_exception_type": str(self._row_value(row, "last_send_exception_type", "") or "").strip(),
            "last_send_exception_message": str(self._row_value(row, "last_send_exception_message", "") or "").strip(),
            "last_heartbeat_at": self._coerce_timestamp(self._row_value(row, "last_heartbeat_at")),
            "last_error": str(self._row_value(row, "last_error", "") or "").strip(),
            "stats": self._decode_json_dict(self._row_value(row, "stats_json", "{}")),
            "updated_at": self._coerce_timestamp(self._row_value(row, "updated_at")),
        }

    def get_session_connector_state(self, account_id: str) -> dict[str, Any]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return {}
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM session_connector_state WHERE account_id = ?",
                (clean_account,),
            ).fetchone()
        if row is None:
            return {}
        return {
            "account_id": clean_account,
            "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
            "state": str(self._row_value(row, "state", "offline") or "offline").strip(),
            "proxy_key": str(self._row_value(row, "proxy_key", "") or "").strip(),
            "last_heartbeat_at": self._coerce_timestamp(self._row_value(row, "last_heartbeat_at")),
            "last_error": str(self._row_value(row, "last_error", "") or "").strip(),
            "updated_at": self._coerce_timestamp(self._row_value(row, "updated_at")),
        }

    def list_session_connector_states(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM session_connector_state ORDER BY alias_id ASC, account_id ASC"
            ).fetchall()
        return [
            {
                "account_id": self._clean_account_id(self._row_value(row, "account_id", "")),
                "alias_id": str(self._row_value(row, "alias_id", "") or "").strip(),
                "state": str(self._row_value(row, "state", "offline") or "offline").strip(),
                "proxy_key": str(self._row_value(row, "proxy_key", "") or "").strip(),
                "last_heartbeat_at": self._coerce_timestamp(self._row_value(row, "last_heartbeat_at")),
                "last_error": str(self._row_value(row, "last_error", "") or "").strip(),
                "updated_at": self._coerce_timestamp(self._row_value(row, "updated_at")),
            }
            for row in rows
        ]

    def upsert_session_connector_state(self, account_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account or not isinstance(updates, dict):
            return {}
        current = self.get_session_connector_state(clean_account)
        merged = {
            "account_id": clean_account,
            "alias_id": str(updates.get("alias_id") or current.get("alias_id") or "").strip(),
            "state": str(updates.get("state") or current.get("state") or "offline").strip() or "offline",
            "proxy_key": str(updates.get("proxy_key") or current.get("proxy_key") or "").strip(),
            "last_heartbeat_at": self._coerce_timestamp(updates.get("last_heartbeat_at") or current.get("last_heartbeat_at")),
            "last_error": str(updates.get("last_error") or current.get("last_error") or "").strip(),
            "updated_at": self._coerce_timestamp(updates.get("updated_at")) or time.time(),
        }
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO session_connector_state(account_id, alias_id, state, proxy_key, last_heartbeat_at, last_error, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    alias_id = excluded.alias_id,
                    state = excluded.state,
                    proxy_key = excluded.proxy_key,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    clean_account,
                    merged["alias_id"],
                    merged["state"],
                    merged["proxy_key"],
                    merged["last_heartbeat_at"],
                    merged["last_error"],
                    merged["updated_at"],
                ),
            )
            self._conn.commit()
        return merged

    def delete_session_connector_state(self, account_id: str) -> bool:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return False
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM session_connector_state WHERE account_id = ?",
                (clean_account,),
            )
            self._conn.commit()
        return int(cursor.rowcount or 0) > 0

    def append_thread_tag(self, thread_key: str, tag: str) -> list[str]:
        clean_key = str(thread_key or "").strip()
        clean_tag = str(tag or "").strip()
        if not clean_key or not clean_tag:
            return []
        with self._lock:
            thread = self._load_thread_record(clean_key) or self._thread_shell(clean_key)
            tags = self._normalize_tags(list(thread.get("tags") or []) + [clean_tag])
            thread["tags"] = tags
            self._upsert_thread_record(thread)
            self._conn.commit()
        return tags

    def mark_follow_up(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        with self._lock:
            thread = self._load_thread_record(clean_key)
            if not isinstance(thread, dict):
                return False
            tags = self._normalize_tags(list(thread.get("tags") or []) + [self._LOCAL_TAG_FOLLOW_UP])
            thread["tags"] = tags
            thread["status"] = "pending"
            thread["last_action_type"] = "followup_marked"
            thread["last_action_at"] = time.time()
            thread["updated_at"] = time.time()
            self._upsert_thread_record(thread)
            state = self._load_thread_state(clean_key)
            state["follow_up_marked_at"] = time.time()
            self._save_thread_state(clean_key, state)
            self.record_action_memory(
                str(thread.get("thread_id") or "").strip(),
                str(thread.get("account_id") or "").strip(),
                "follow_up_tag_added",
                source="inbox_rm",
            )
            self._conn.commit()
            return True

    def delete_thread(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        with self._lock:
            thread = self._load_thread_record(clean_key)
            if not isinstance(thread, dict):
                return False
            self._remember_deleted_thread_locked(thread)
            thread["is_deleted_from_view"] = True
            thread["trash_at"] = time.time()
            thread["updated_at"] = time.time()
            self._upsert_thread_record(thread)
            self._conn.commit()
            return True

    def clear_deleted_thread(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._lock:
            self._clear_deleted_thread_locked(clean_key)
            thread = self._load_thread_record(clean_key)
            if isinstance(thread, dict):
                thread["is_deleted_from_view"] = False
                thread["trash_at"] = None
                thread["updated_at"] = time.time()
                self._upsert_thread_record(thread)
            self._conn.commit()

    def allow_deleted_thread_recreate(
        self,
        thread_key: str,
        *,
        last_activity_timestamp: float | None = None,
    ) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        incoming_activity = self._coerce_timestamp(last_activity_timestamp)
        with self._lock:
            row = self._load_deleted_thread_row(clean_key)
            if row is None:
                return True
            deleted_activity = self._coerce_timestamp(self._row_value(row, "last_activity_timestamp"))
            if incoming_activity is not None and (
                deleted_activity is None or incoming_activity > (deleted_activity + 0.000001)
            ):
                self._clear_deleted_thread_locked(clean_key)
                thread = self._load_thread_record(clean_key)
                if isinstance(thread, dict):
                    thread["is_deleted_from_view"] = False
                    thread["trash_at"] = None
                    thread["updated_at"] = time.time()
                    self._upsert_thread_record(thread)
                self._conn.commit()
                return True
        return False

    def flush(self) -> None:
        with self._lock:
            self._conn.commit()

    def shutdown(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
            finally:
                self._conn.close()
