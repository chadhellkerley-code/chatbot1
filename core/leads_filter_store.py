from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from uuid import uuid4

from core.storage_atomic import atomic_write_json, load_json_file

_SAFE_FILTER_LIST_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class LeadFilterStoreError(ValueError):
    pass


def build_filter_list_id() -> str:
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return f"{stamp}_{uuid4().hex[:8]}"


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, *, default: int = 0, minimum: int = 0) -> int:
    try:
        return max(minimum, int(value))
    except Exception:
        return max(minimum, int(default))


def _json_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_load_dict(raw: Any) -> dict[str, Any]:
    text = _clean_text(raw)
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _json_load_item(raw: Any) -> dict[str, Any]:
    text = _clean_text(raw)
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _stats_from_items(items: Any) -> tuple[int, int, int, int, int]:
    if not isinstance(items, list):
        return 0, 0, 0, 0, 0
    total = len(items)
    qualified = 0
    discarded = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        status = _clean_text(item.get("status")).upper()
        if status == "QUALIFIED":
            qualified += 1
        elif status == "DISCARDED":
            discarded += 1
    processed = qualified + discarded
    pending = max(0, total - processed)
    return total, processed, qualified, discarded, pending


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class LeadFilterStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lists_dir = self.root / "lists"
        self.lists_dir.mkdir(parents=True, exist_ok=True)
        self.deleted_lists_dir = self.root / "_deleted_lists"
        self.deleted_lists_dir.mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "filters_config.json"
        self.index_path = self.root / "filters_index.sqlite3"
        self._index_lock = threading.RLock()
        self._ensure_index_schema()

    def _connect_index(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _table_columns(self, connection: sqlite3.Connection, table_name: str) -> set[str]:
        rows = connection.execute(f"pragma table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        ddl: str,
    ) -> None:
        if column_name in self._table_columns(connection, table_name):
            return
        connection.execute(f"alter table {table_name} add column {column_name} {ddl}")

    def _ensure_index_schema(self) -> None:
        with self._index_lock, self._connect_index() as connection:
            connection.executescript(
                """
                create table if not exists filter_lists_index (
                    list_id text primary key,
                    payload_path text not null,
                    payload_mtime_ns integer not null default 0,
                    payload_size integer not null default 0,
                    created_at text not null default '',
                    updated_at text not null default '',
                    source_list text not null default '',
                    export_alias text not null default '',
                    status text not null default '',
                    total integer not null default 0,
                    processed integer not null default 0,
                    qualified integer not null default 0,
                    discarded integer not null default 0,
                    pending integer not null default 0,
                    errors integer not null default 0,
                    run_payload_json text not null default '{}',
                    filters_payload_json text not null default '{}',
                    pipeline_state_json text not null default '{}'
                );

                create index if not exists idx_filter_lists_index_status
                on filter_lists_index (status, list_id);

                create index if not exists idx_filter_lists_index_pending
                on filter_lists_index (pending, list_id);

                create table if not exists filter_list_items (
                    list_id text not null,
                    item_index integer not null,
                    username text not null default '',
                    status text not null default '',
                    result text not null default '',
                    reason text not null default '',
                    account text not null default '',
                    updated_at text not null default '',
                    payload_json text not null default '{}',
                    primary key (list_id, item_index)
                );

                create index if not exists idx_filter_list_items_list
                on filter_list_items (list_id, item_index);

                create index if not exists idx_filter_list_items_status
                on filter_list_items (list_id, status, item_index);
                """
            )
            self._ensure_column(
                connection,
                "filter_lists_index",
                "pipeline_state_json",
                "text not null default '{}'",
            )

    def _normalize_list_id(self, value: object) -> str:
        clean_value = str(value or "").strip()
        if not clean_value:
            return build_filter_list_id()
        if not _SAFE_FILTER_LIST_ID.fullmatch(clean_value):
            raise LeadFilterStoreError("ID de lista de filtrado invalido.")
        return clean_value

    def list_path(self, list_id: object) -> Path:
        clean_id = self._normalize_list_id(list_id)
        return self.lists_dir / f"{clean_id}.json"

    def _build_summary_payload(
        self,
        payload: dict[str, Any],
        *,
        payload_mtime_ns: int | None = None,
        payload_size: int | None = None,
    ) -> dict[str, Any]:
        list_id = self._normalize_list_id(payload.get("id"))
        total, processed, qualified, discarded, pending = _stats_from_items(payload.get("items"))
        if not total and not isinstance(payload.get("items"), list):
            total = _safe_int(payload.get("total"), minimum=0)
            processed = _safe_int(payload.get("processed"), minimum=0)
            qualified = _safe_int(payload.get("qualified"), minimum=0)
            discarded = _safe_int(payload.get("discarded"), minimum=0)
            pending = _safe_int(payload.get("pending"), default=max(0, total - processed), minimum=0)
        if payload_mtime_ns is None:
            payload_mtime_ns = time.time_ns()
        if payload_size is None:
            payload_size = max(0, len(payload.get("items") or []))
        status = "done" if processed >= total else "pending"
        return {
            "list_id": list_id,
            "payload_path": str(self.list_path(list_id)),
            "payload_mtime_ns": int(payload_mtime_ns),
            "payload_size": int(payload_size),
            "created_at": _clean_text(payload.get("created_at")),
            "updated_at": _clean_text(payload.get("updated_at")) or _utc_now_iso(),
            "source_list": _clean_text(payload.get("source_list") or payload.get("list_name")),
            "export_alias": _clean_text(payload.get("export_alias")),
            "status": status,
            "total": total,
            "processed": processed,
            "qualified": qualified,
            "discarded": discarded,
            "pending": pending,
            "errors": _safe_int(payload.get("errors"), minimum=0),
            "run_payload_json": _json_text(_json_dict(payload.get("run"))),
            "filters_payload_json": _json_text(_json_dict(payload.get("filters"))),
            "pipeline_state_json": _json_text(_json_dict(payload.get("_pipeline_state"))),
        }

    def _upsert_summary_locked(self, connection: sqlite3.Connection, summary: dict[str, Any]) -> None:
        connection.execute(
            """
            insert into filter_lists_index (
                list_id,
                payload_path,
                payload_mtime_ns,
                payload_size,
                created_at,
                updated_at,
                source_list,
                export_alias,
                status,
                total,
                processed,
                qualified,
                discarded,
                pending,
                errors,
                run_payload_json,
                filters_payload_json,
                pipeline_state_json
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(list_id) do update set
                payload_path = excluded.payload_path,
                payload_mtime_ns = excluded.payload_mtime_ns,
                payload_size = excluded.payload_size,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                source_list = excluded.source_list,
                export_alias = excluded.export_alias,
                status = excluded.status,
                total = excluded.total,
                processed = excluded.processed,
                qualified = excluded.qualified,
                discarded = excluded.discarded,
                pending = excluded.pending,
                errors = excluded.errors,
                run_payload_json = excluded.run_payload_json,
                filters_payload_json = excluded.filters_payload_json,
                pipeline_state_json = excluded.pipeline_state_json
            """,
            (
                summary["list_id"],
                summary["payload_path"],
                int(summary["payload_mtime_ns"]),
                int(summary["payload_size"]),
                summary["created_at"],
                summary["updated_at"],
                summary["source_list"],
                summary["export_alias"],
                summary["status"],
                int(summary["total"]),
                int(summary["processed"]),
                int(summary["qualified"]),
                int(summary["discarded"]),
                int(summary["pending"]),
                int(summary["errors"]),
                summary["run_payload_json"],
                summary["filters_payload_json"],
                summary["pipeline_state_json"],
            ),
        )

    def _serialize_item_record(self, list_id: str, item_index: int, item: dict[str, Any]) -> tuple[Any, ...]:
        payload = dict(item or {})
        return (
            list_id,
            int(item_index),
            _clean_text(payload.get("username")),
            _clean_text(payload.get("status")),
            _clean_text(payload.get("result")),
            _clean_text(payload.get("reason")),
            _clean_text(payload.get("account")),
            _clean_text(payload.get("updated_at")),
            _json_text(payload),
        )

    def _replace_items_locked(
        self,
        connection: sqlite3.Connection,
        list_id: str,
        items: list[dict[str, Any]],
    ) -> None:
        connection.execute("delete from filter_list_items where list_id = ?", (list_id,))
        if not items:
            return
        connection.executemany(
            """
            insert into filter_list_items (
                list_id,
                item_index,
                username,
                status,
                result,
                reason,
                account,
                updated_at,
                payload_json
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self._serialize_item_record(list_id, item_index, item)
                for item_index, item in enumerate(items)
                if isinstance(item, dict)
            ],
        )

    def _normalize_item_indexes(self, item_indexes: Iterable[int] | None, total_items: int) -> list[int] | None:
        if item_indexes is None:
            return None
        normalized: set[int] = set()
        for raw_index in item_indexes:
            try:
                index = int(raw_index)
            except Exception:
                continue
            if 0 <= index < total_items:
                normalized.add(index)
        return sorted(normalized)

    def _upsert_selected_items_locked(
        self,
        connection: sqlite3.Connection,
        list_id: str,
        items: list[dict[str, Any]],
        item_indexes: list[int],
    ) -> None:
        if not item_indexes:
            return
        connection.executemany(
            """
            insert into filter_list_items (
                list_id,
                item_index,
                username,
                status,
                result,
                reason,
                account,
                updated_at,
                payload_json
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(list_id, item_index) do update set
                username = excluded.username,
                status = excluded.status,
                result = excluded.result,
                reason = excluded.reason,
                account = excluded.account,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            """,
            [
                self._serialize_item_record(list_id, item_index, items[item_index])
                for item_index in item_indexes
                if 0 <= item_index < len(items) and isinstance(items[item_index], dict)
            ],
        )

    def _select_summary_locked(self, connection: sqlite3.Connection, list_id: str) -> sqlite3.Row | None:
        return connection.execute(
            """
            select
                list_id,
                payload_path,
                payload_mtime_ns,
                payload_size,
                created_at,
                updated_at,
                source_list,
                export_alias,
                status,
                total,
                processed,
                qualified,
                discarded,
                pending,
                errors,
                run_payload_json,
                filters_payload_json,
                pipeline_state_json
            from filter_lists_index
            where list_id = ?
            """,
            (list_id,),
        ).fetchone()

    def _select_summary_rows_locked(
        self,
        connection: sqlite3.Connection,
        *,
        status: str | None = None,
    ) -> list[sqlite3.Row]:
        clean_status = _clean_text(status).lower()
        query = """
            select
                list_id,
                payload_path,
                payload_mtime_ns,
                payload_size,
                created_at,
                updated_at,
                source_list,
                export_alias,
                status,
                total,
                processed,
                qualified,
                discarded,
                pending,
                errors,
                run_payload_json,
                filters_payload_json,
                pipeline_state_json
            from filter_lists_index
        """
        params: list[Any] = []
        if clean_status == "completed":
            query += " where pending = 0"
        elif clean_status == "incomplete":
            query += " where pending > 0"
        query += " order by list_id asc"
        return connection.execute(query, params).fetchall()

    def _select_items_locked(self, connection: sqlite3.Connection, list_id: str) -> list[sqlite3.Row]:
        return connection.execute(
            """
            select item_index, payload_json
            from filter_list_items
            where list_id = ?
            order by item_index asc
            """,
            (list_id,),
        ).fetchall()

    def _summary_from_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        list_id = _clean_text(data.get("list_id"))
        return {
            "id": list_id,
            "_path": _clean_text(data.get("payload_path")) or str(self.list_path(list_id)),
            "created_at": _clean_text(data.get("created_at")),
            "updated_at": _clean_text(data.get("updated_at")),
            "source_list": _clean_text(data.get("source_list")),
            "export_alias": _clean_text(data.get("export_alias")),
            "status": _clean_text(data.get("status")),
            "total": _safe_int(data.get("total"), minimum=0),
            "processed": _safe_int(data.get("processed"), minimum=0),
            "qualified": _safe_int(data.get("qualified"), minimum=0),
            "discarded": _safe_int(data.get("discarded"), minimum=0),
            "pending": _safe_int(data.get("pending"), minimum=0),
            "errors": _safe_int(data.get("errors"), minimum=0),
            "run": _json_load_dict(data.get("run_payload_json")),
            "filters": _json_load_dict(data.get("filters_payload_json")),
        }

    def _assemble_list_payload(
        self,
        summary_row: sqlite3.Row | dict[str, Any],
        item_rows: list[sqlite3.Row],
    ) -> dict[str, Any]:
        data = dict(summary_row)
        list_id = _clean_text(data.get("list_id"))
        payload = self._summary_from_row(data)
        payload["items"] = [_json_load_item(row["payload_json"]) for row in item_rows]
        payload["_pipeline_state"] = _json_load_dict(data.get("pipeline_state_json"))
        payload.setdefault("id", list_id)
        payload.setdefault("export_alias", "")
        return payload

    def _load_legacy_payload(
        self,
        path: Path,
        *,
        migrate: Callable[[dict[str, Any]], tuple[dict[str, Any], bool]] | None = None,
    ) -> dict[str, Any] | None:
        if not path.exists():
            return None
        payload = load_json_file(
            path,
            {},
            label=f"leads.filter_list:{path.name}",
        )
        if not isinstance(payload, dict) or not payload:
            return None
        row = dict(payload)
        if callable(migrate):
            row, _changed = migrate(row)
            row = dict(row or {})
        if not isinstance(row, dict) or not row:
            return None
        row.setdefault("id", path.stem)
        return row

    def _migrate_missing_legacy_payloads(
        self,
        *,
        migrate: Callable[[dict[str, Any]], tuple[dict[str, Any], bool]] | None = None,
    ) -> None:
        legacy_paths = sorted(self.lists_dir.glob("*.json"))
        if not legacy_paths:
            return
        with self._index_lock, self._connect_index() as connection:
            existing_ids = {
                _clean_text(row["list_id"])
                for row in connection.execute("select list_id from filter_lists_index").fetchall()
                if _clean_text(row["list_id"])
            }
        for path in legacy_paths:
            if path.stem in existing_ids:
                continue
            row = self._load_legacy_payload(path, migrate=migrate)
            if not row:
                continue
            self.save_list(row)

    def _write_deleted_backup(self, list_id: str, payload: dict[str, Any]) -> Path:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target = self.deleted_lists_dir / f"{self.list_path(list_id).name}.deleted.{stamp}.bak"
        counter = 1
        while target.exists():
            target = self.deleted_lists_dir / f"{self.list_path(list_id).name}.deleted.{stamp}.{counter}.bak"
            counter += 1
        atomic_write_json(target, dict(payload or {}))
        return target

    def save_config(self, payload: dict[str, Any]) -> Path:
        return atomic_write_json(self.config_path, dict(payload or {}))

    def load_config(self) -> dict[str, Any]:
        payload = load_json_file(
            self.config_path,
            {},
            label="leads.filter_config",
        )
        if isinstance(payload, dict):
            return dict(payload)
        return {}

    def save_list(self, payload: dict[str, Any]) -> Path:
        data = dict(payload or {})
        list_id = self._normalize_list_id(data.get("id"))
        data["id"] = list_id
        summary = self._build_summary_payload(data)
        items = list(data.get("items") or [])
        with self._index_lock, self._connect_index() as connection:
            self._upsert_summary_locked(connection, summary)
            self._replace_items_locked(connection, list_id, items)
        return atomic_write_json(self.list_path(list_id), data)

    def save_runtime_state(
        self,
        payload: dict[str, Any],
        *,
        item_indexes: Iterable[int] | None = None,
    ) -> None:
        data = dict(payload or {})
        list_id = self._normalize_list_id(data.get("id"))
        data["id"] = list_id
        items = list(data.get("items") or [])
        normalized_indexes = self._normalize_item_indexes(item_indexes, len(items))
        summary = self._build_summary_payload(data)
        with self._index_lock, self._connect_index() as connection:
            self._upsert_summary_locked(connection, summary)
            if normalized_indexes is None:
                self._replace_items_locked(connection, list_id, items)
            else:
                self._upsert_selected_items_locked(connection, list_id, items, normalized_indexes)

    def load_list(
        self,
        list_id: object,
        *,
        migrate: Callable[[dict[str, Any]], tuple[dict[str, Any], bool]] | None = None,
    ) -> dict[str, Any] | None:
        clean_id = self._normalize_list_id(list_id)
        with self._index_lock, self._connect_index() as connection:
            summary_row = self._select_summary_locked(connection, clean_id)
            if summary_row is not None:
                item_rows = self._select_items_locked(connection, clean_id)
                if item_rows or _safe_int(summary_row["total"], minimum=0) == 0:
                    return self._assemble_list_payload(summary_row, item_rows)

        legacy_row = self._load_legacy_payload(self.list_path(clean_id), migrate=migrate)
        if legacy_row is not None:
            self.save_list(legacy_row)
            with self._index_lock, self._connect_index() as connection:
                summary_row = self._select_summary_locked(connection, clean_id)
                if summary_row is None:
                    return None
                item_rows = self._select_items_locked(connection, clean_id)
                return self._assemble_list_payload(summary_row, item_rows)
        return None

    def load_lists(
        self,
        *,
        migrate: Callable[[dict[str, Any]], tuple[dict[str, Any], bool]] | None = None,
    ) -> list[dict[str, Any]]:
        self._migrate_missing_legacy_payloads(migrate=migrate)
        with self._index_lock, self._connect_index() as connection:
            summary_rows = self._select_summary_rows_locked(connection)
        rows: list[dict[str, Any]] = []
        for summary_row in summary_rows:
            row = self.load_list(summary_row["list_id"], migrate=migrate)
            if row is not None:
                rows.append(row)
        return rows

    def list_summaries(
        self,
        *,
        migrate: Callable[[dict[str, Any]], tuple[dict[str, Any], bool]] | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        self._migrate_missing_legacy_payloads(migrate=migrate)
        with self._index_lock, self._connect_index() as connection:
            rows = self._select_summary_rows_locked(connection, status=status)
        return [self._summary_from_row(row) for row in rows]

    def delete_list(self, payload: dict[str, Any]) -> bool:
        clean_id = self._normalize_list_id(payload.get("id"))
        current_payload = self.load_list(clean_id)
        if current_payload is None:
            shadow_path = self.list_path(clean_id)
            if shadow_path.exists():
                fallback_payload = load_json_file(
                    shadow_path,
                    {},
                    label=f"leads.filter_list:{shadow_path.name}",
                )
                if isinstance(fallback_payload, dict) and fallback_payload:
                    current_payload = dict(fallback_payload)
        if current_payload is not None:
            self._write_deleted_backup(clean_id, current_payload)

        shadow_path = self.list_path(clean_id)
        if shadow_path.exists():
            shadow_path.unlink(missing_ok=True)

        with self._index_lock, self._connect_index() as connection:
            deleted_items = connection.execute(
                "delete from filter_list_items where list_id = ?",
                (clean_id,),
            ).rowcount
            deleted_summary = connection.execute(
                "delete from filter_lists_index where list_id = ?",
                (clean_id,),
            ).rowcount
        return bool(current_payload) or bool(deleted_items) or bool(deleted_summary)
