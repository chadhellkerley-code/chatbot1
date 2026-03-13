from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.alias_identity import DEFAULT_ALIAS_ID, normalize_alias_id
from core.storage_atomic import load_json_file
from paths import accounts_root, storage_root


_STORE_CACHE: dict[str, "AppStateStore"] = {}
_STORE_CACHE_LOCK = threading.RLock()
_CAMPAIGN_INTERRUPTED_RECOVERY_STATUSES = ("starting", "running", "stopping")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _root_dir(root_dir: Path | str | None = None) -> Path:
    if root_dir is not None:
        return Path(root_dir).resolve()
    for env_name in ("INSTACRM_INSTALL_ROOT", "APP_DATA_ROOT"):
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            return Path(raw).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


class AppStateStore:
    def __init__(self, root_dir: Path | str | None = None) -> None:
        self.root_dir = _root_dir(root_dir)
        self.data_dir = self.root_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "app_state.db"
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_schema(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                create table if not exists sync_runs (
                    sync_key text primary key,
                    source_path text not null,
                    source_mtime real not null default 0,
                    synced_at text not null,
                    row_count integer not null default 0
                );

                create table if not exists state_snapshots (
                    snapshot_key text primary key,
                    source_path text not null,
                    payload_json text not null,
                    synced_at text not null
                );

                create table if not exists app_settings (
                    setting_key text primary key,
                    setting_value text not null,
                    updated_at text not null
                );

                create table if not exists accounts (
                    username text primary key,
                    alias text not null,
                    active integer not null default 1,
                    connected integer not null default 0,
                    first_seen text,
                    payload_json text not null,
                    synced_at text not null
                );

                create table if not exists campaign_state (
                    run_id text primary key,
                    alias text,
                    leads_alias text,
                    status text,
                    started_at text,
                    finished_at text,
                    payload_json text not null,
                    synced_at text not null
                );

                create table if not exists campaign_events (
                    event_id text primary key,
                    run_id text not null,
                    event_type text not null,
                    severity text not null,
                    message text not null,
                    payload_json text not null,
                    created_at text not null
                );

                create index if not exists idx_campaign_events_run_id_created_at
                    on campaign_events (run_id, created_at, event_id);

                create table if not exists lead_status (
                    alias text not null,
                    lead_key text not null,
                    status text,
                    updated_at integer,
                    payload_json text not null,
                    synced_at text not null,
                    primary key (alias, lead_key)
                );

                create table if not exists conversation_engine_state (
                    conversation_key text primary key,
                    account text,
                    thread_id text,
                    stage text,
                    updated_at real,
                    payload_json text not null,
                    synced_at text not null
                );
                """
            )

    def sync_foundation(self) -> dict[str, int]:
        return {
            "accounts": self.sync_accounts(),
            "lead_status": self.sync_lead_status(),
            "conversation_engine": self.sync_conversation_engine(),
        }

    def sync_accounts(self) -> int:
        path = accounts_root(self.root_dir) / "accounts.json"
        payload = self._load_json(path, [])
        if not isinstance(payload, list):
            payload = []
        rows: list[tuple[Any, ...]] = []
        synced_at = _utc_now_iso()
        for item in payload:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            alias_id = normalize_alias_id(
                item.get("alias_id") or item.get("alias"),
                default=DEFAULT_ALIAS_ID,
            )
            rows.append(
                (
                    username,
                    alias_id,
                    1 if bool(item.get("active", True)) else 0,
                    1 if bool(item.get("connected", False)) else 0,
                    str(item.get("first_seen") or "").strip(),
                    json.dumps(item, ensure_ascii=False, sort_keys=True),
                    synced_at,
                )
            )
        with self._lock, self._connect() as connection:
            connection.execute("delete from accounts")
            connection.executemany(
                """
                insert into accounts (
                    username,
                    alias,
                    active,
                    connected,
                    first_seen,
                    payload_json,
                    synced_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._record_sync(
                connection,
                sync_key="accounts",
                source_path=path,
                payload=payload,
                row_count=len(rows),
            )
        return len(rows)

    def sync_lead_status(self) -> int:
        path = storage_root(self.root_dir) / "lead_status.json"
        payload = self._load_json(path, {})
        aliases = payload.get("aliases") if isinstance(payload, dict) else {}
        legacy = payload.get("legacy_global_leads") if isinstance(payload, dict) else {}
        rows: list[tuple[Any, ...]] = []
        synced_at = _utc_now_iso()
        if isinstance(aliases, dict):
            for alias_key, bucket in aliases.items():
                normalized_alias = str(alias_key or "").strip().lower()
                if not normalized_alias or not isinstance(bucket, dict):
                    continue
                leads = bucket.get("leads")
                if not isinstance(leads, dict):
                    continue
                for lead_key, entry in leads.items():
                    if not isinstance(entry, dict):
                        continue
                    normalized_lead = str(lead_key or "").strip().lstrip("@").lower()
                    if not normalized_lead:
                        continue
                    rows.append(
                        (
                            normalized_alias,
                            normalized_lead,
                            str(entry.get("status") or "").strip().lower(),
                            int(entry.get("updated_at") or 0),
                            json.dumps(entry, ensure_ascii=False, sort_keys=True),
                            synced_at,
                        )
                    )
        if isinstance(legacy, dict):
            for lead_key, entry in legacy.items():
                if not isinstance(entry, dict):
                    continue
                normalized_lead = str(lead_key or "").strip().lstrip("@").lower()
                if not normalized_lead:
                    continue
                rows.append(
                    (
                        "__legacy__",
                        normalized_lead,
                        str(entry.get("status") or "").strip().lower(),
                        int(entry.get("updated_at") or 0),
                        json.dumps(entry, ensure_ascii=False, sort_keys=True),
                        synced_at,
                    )
                )
        with self._lock, self._connect() as connection:
            connection.execute("delete from lead_status")
            connection.executemany(
                """
                insert into lead_status (
                    alias,
                    lead_key,
                    status,
                    updated_at,
                    payload_json,
                    synced_at
                )
                values (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._record_sync(
                connection,
                sync_key="lead_status",
                source_path=path,
                payload=payload,
                row_count=len(rows),
            )
        return len(rows)

    def sync_conversation_engine(self) -> int:
        path = storage_root(self.root_dir) / "conversation_engine.json"
        payload = self._load_json(path, {})
        conversations = payload.get("conversations") if isinstance(payload, dict) else {}
        rows: list[tuple[Any, ...]] = []
        synced_at = _utc_now_iso()
        if isinstance(conversations, dict):
            for conversation_key, entry in conversations.items():
                if not isinstance(entry, dict):
                    continue
                normalized_key = str(conversation_key or "").strip()
                if not normalized_key:
                    continue
                rows.append(
                    (
                        normalized_key,
                        str(entry.get("account") or "").strip(),
                        str(entry.get("thread_id") or "").strip(),
                        str(entry.get("stage") or "").strip(),
                        float(entry.get("updated_at") or 0.0),
                        json.dumps(entry, ensure_ascii=False, sort_keys=True),
                        synced_at,
                    )
                )
        with self._lock, self._connect() as connection:
            connection.execute("delete from conversation_engine_state")
            connection.executemany(
                """
                insert into conversation_engine_state (
                    conversation_key,
                    account,
                    thread_id,
                    stage,
                    updated_at,
                    payload_json,
                    synced_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._record_sync(
                connection,
                sync_key="conversation_engine",
                source_path=path,
                payload=payload,
                row_count=len(rows),
            )
        return len(rows)

    def sync_campaign_state(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            return
        synced_at = _utc_now_iso()
        with self._lock, self._connect() as connection:
            self._sync_campaign_state_locked(connection, payload, synced_at=synced_at)

    def get_campaign_state(self, *, run_id: str = "") -> dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        with self._lock, self._connect() as connection:
            if clean_run_id:
                row = connection.execute(
                    """
                    select *
                    from campaign_state
                    where run_id = ?
                    limit 1
                    """,
                    (clean_run_id,),
                ).fetchone()
            else:
                latest_row = connection.execute(
                    """
                    select setting_value
                    from app_settings
                    where setting_key = 'last_campaign_run_id'
                    limit 1
                    """
                ).fetchone()
                latest_run_id = str(latest_row["setting_value"] or "").strip() if latest_row is not None else ""
                row = None
                if latest_run_id:
                    row = connection.execute(
                        """
                        select *
                        from campaign_state
                        where run_id = ?
                        limit 1
                        """,
                        (latest_run_id,),
                    ).fetchone()
                if row is None:
                    row = connection.execute(
                        """
                        select *
                        from campaign_state
                        order by synced_at desc, started_at desc, run_id desc
                        limit 1
                        """
                    ).fetchone()
        return self._campaign_state_from_row(row)

    def recover_interrupted_campaign_states(self) -> list[dict[str, Any]]:
        recovered: list[dict[str, Any]] = []
        placeholders = ", ".join("?" for _ in _CAMPAIGN_INTERRUPTED_RECOVERY_STATUSES)
        now = _utc_now_iso()
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                f"""
                select *
                from campaign_state
                where lower(status) in ({placeholders})
                order by synced_at asc, started_at asc, run_id asc
                """,
                _CAMPAIGN_INTERRUPTED_RECOVERY_STATUSES,
            ).fetchall()
            for row in rows:
                payload = self._campaign_state_from_row(row)
                if not payload:
                    continue
                message = str(payload.get("message") or "").strip()
                interrupted_detail = "Campana interrumpida al reabrir la aplicacion."
                if interrupted_detail.lower() not in message.lower():
                    message = f"{message} {interrupted_detail}".strip() if message else interrupted_detail
                payload.update(
                    {
                        "status": "Interrupted",
                        "task_active": False,
                        "finished_at": str(payload.get("finished_at") or "").strip() or now,
                        "workers_active": 0,
                        "message": message,
                    }
                )
                self._sync_campaign_state_locked(connection, payload, synced_at=now)
                recovered.append(payload)
        return recovered

    def append_campaign_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        run_id = str(payload.get("run_id") or "").strip()
        event_type = str(payload.get("event_type") or "").strip()
        if not run_id or not event_type:
            return {}
        created_at = str(payload.get("created_at") or "").strip() or _utc_now_iso()
        severity = str(payload.get("severity") or "info").strip().lower() or "info"
        message = str(payload.get("message") or "").strip()
        event_payload = {
            **payload,
            "run_id": run_id,
            "event_type": event_type,
            "severity": severity,
            "message": message,
            "created_at": created_at,
        }
        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            payload_hash = hashlib.sha1(
                json.dumps(event_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()[:12]
            event_id = f"{run_id}:{event_type}:{payload_hash}"
        event_payload["event_id"] = event_id
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                insert into campaign_events (
                    event_id,
                    run_id,
                    event_type,
                    severity,
                    message,
                    payload_json,
                    created_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict(event_id) do nothing
                """,
                (
                    event_id,
                    run_id,
                    event_type,
                    severity,
                    message,
                    json.dumps(event_payload, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
        return event_payload

    def list_campaign_events(self, *, run_id: str = "", limit: int = 100) -> list[dict[str, Any]]:
        clean_run_id = str(run_id or "").strip()
        clean_limit = max(1, int(limit or 1))
        with self._lock, self._connect() as connection:
            if clean_run_id:
                rows = connection.execute(
                    """
                    select *
                    from campaign_events
                    where run_id = ?
                    order by created_at asc, event_id asc
                    limit ?
                    """,
                    (clean_run_id, clean_limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    select *
                    from campaign_events
                    order by created_at desc, event_id desc
                    limit ?
                    """,
                    (clean_limit,),
                ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            payload = self._json_loads(row["payload_json"], {})
            if not isinstance(payload, dict):
                payload = {}
            events.append(
                {
                    **payload,
                    "event_id": str(payload.get("event_id") or row["event_id"] or "").strip(),
                    "run_id": str(payload.get("run_id") or row["run_id"] or "").strip(),
                    "event_type": str(payload.get("event_type") or row["event_type"] or "").strip(),
                    "severity": str(payload.get("severity") or row["severity"] or "").strip(),
                    "message": str(payload.get("message") or row["message"] or "").strip(),
                    "created_at": str(payload.get("created_at") or row["created_at"] or "").strip(),
                }
            )
        if clean_run_id:
            return events
        events.reverse()
        return events

    def get_setting(self, setting_key: str, default: str = "") -> str:
        clean_key = str(setting_key or "").strip()
        if not clean_key:
            return str(default or "")
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "select setting_value from app_settings where setting_key = ?",
                (clean_key,),
            ).fetchone()
        if row is None:
            return str(default or "")
        return str(row["setting_value"] or "").strip() or str(default or "")

    def set_setting(self, setting_key: str, value: Any) -> str:
        clean_key = str(setting_key or "").strip()
        if not clean_key:
            return ""
        clean_value = str(value or "").strip()
        updated_at = _utc_now_iso()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                insert into app_settings (setting_key, setting_value, updated_at)
                values (?, ?, ?)
                on conflict(setting_key) do update set
                    setting_value=excluded.setting_value,
                    updated_at=excluded.updated_at
                """,
                (clean_key, clean_value, updated_at),
            )
            self._record_snapshot(
                connection,
                snapshot_key=f"setting:{clean_key}",
                source_path=self.db_path,
                payload={"key": clean_key, "value": clean_value},
            )
        return clean_value

    def get_active_alias(self) -> str:
        return self.get_setting("active_alias", "default") or "default"

    def set_active_alias(self, alias: str) -> str:
        clean_alias = str(alias or "").strip() or "default"
        return self.set_setting("active_alias", clean_alias) or "default"

    def _load_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return load_json_file(path, default, label=f"app_state:{path.name}")
        except Exception:
            return default

    def _json_loads(self, value: Any, default: Any) -> Any:
        try:
            payload = json.loads(str(value or ""))
        except Exception:
            return default
        return payload if payload is not None else default

    def _campaign_state_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {}
        payload = self._json_loads(row["payload_json"], {})
        if not isinstance(payload, dict):
            payload = {}
        return {
            **payload,
            "run_id": str(payload.get("run_id") or row["run_id"] or "").strip(),
            "alias": str(payload.get("alias") or row["alias"] or "").strip(),
            "leads_alias": str(payload.get("leads_alias") or row["leads_alias"] or "").strip(),
            "status": str(payload.get("status") or row["status"] or "").strip(),
            "started_at": str(payload.get("started_at") or row["started_at"] or "").strip(),
            "finished_at": str(payload.get("finished_at") or row["finished_at"] or "").strip(),
            "heartbeat_at": str(row["synced_at"] or "").strip(),
        }

    def _sync_campaign_state_locked(
        self,
        connection: sqlite3.Connection,
        payload: dict[str, Any],
        *,
        synced_at: str,
    ) -> None:
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            return
        connection.execute(
            """
            insert into campaign_state (
                run_id,
                alias,
                leads_alias,
                status,
                started_at,
                finished_at,
                payload_json,
                synced_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(run_id) do update set
                alias=excluded.alias,
                leads_alias=excluded.leads_alias,
                status=excluded.status,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                payload_json=excluded.payload_json,
                synced_at=excluded.synced_at
            """,
            (
                run_id,
                str(payload.get("alias") or "").strip(),
                str(payload.get("leads_alias") or "").strip(),
                str(payload.get("status") or "").strip(),
                str(payload.get("started_at") or "").strip(),
                str(payload.get("finished_at") or "").strip(),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                synced_at,
            ),
        )
        connection.execute(
            """
            insert into app_settings (setting_key, setting_value, updated_at)
            values ('last_campaign_run_id', ?, ?)
            on conflict(setting_key) do update set
                setting_value=excluded.setting_value,
                updated_at=excluded.updated_at
            """,
            (run_id, synced_at),
        )
        self._record_snapshot(
            connection,
            snapshot_key=f"campaign_state:{run_id}",
            source_path=self.db_path,
            payload=payload,
        )

    def _record_sync(
        self,
        connection: sqlite3.Connection,
        *,
        sync_key: str,
        source_path: Path,
        payload: Any,
        row_count: int,
    ) -> None:
        synced_at = _utc_now_iso()
        try:
            source_mtime = float(source_path.stat().st_mtime)
        except Exception:
            source_mtime = 0.0
        connection.execute(
            """
            insert into sync_runs (sync_key, source_path, source_mtime, synced_at, row_count)
            values (?, ?, ?, ?, ?)
            on conflict(sync_key) do update set
                source_path=excluded.source_path,
                source_mtime=excluded.source_mtime,
                synced_at=excluded.synced_at,
                row_count=excluded.row_count
            """,
            (sync_key, str(source_path), source_mtime, synced_at, int(row_count)),
        )
        self._record_snapshot(
            connection,
            snapshot_key=sync_key,
            source_path=source_path,
            payload=payload,
        )

    def _record_snapshot(
        self,
        connection: sqlite3.Connection,
        *,
        snapshot_key: str,
        source_path: Path,
        payload: Any,
    ) -> None:
        connection.execute(
            """
            insert into state_snapshots (snapshot_key, source_path, payload_json, synced_at)
            values (?, ?, ?, ?)
            on conflict(snapshot_key) do update set
                source_path=excluded.source_path,
                payload_json=excluded.payload_json,
                synced_at=excluded.synced_at
            """,
            (
                snapshot_key,
                str(source_path),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                _utc_now_iso(),
            ),
        )


def get_app_state_store(root_dir: Path | str | None = None) -> AppStateStore:
    root = _root_dir(root_dir)
    cache_key = str(root)
    with _STORE_CACHE_LOCK:
        store = _STORE_CACHE.get(cache_key)
        if store is None:
            store = AppStateStore(root)
            _STORE_CACHE[cache_key] = store
        return store


def sync_foundation_state(root_dir: Path | str | None = None) -> dict[str, int]:
    return get_app_state_store(root_dir).sync_foundation()
