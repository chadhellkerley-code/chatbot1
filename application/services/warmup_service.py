from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from .base import ServiceContext, ServiceError, dedupe_usernames, normalize_alias


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_status(value: Any, *, default: str = "paused") -> str:
    status = str(value or "").strip().lower()
    return status or default


def _normalize_log_level(value: Any) -> str:
    level = str(value or "").strip().lower()
    return level or "info"


_DEFAULT_STAGE_BLUEPRINTS: tuple[dict[str, Any], ...] = (
    {
        "title": "Dia 1",
        "settings": {"base_delay_minutes": 20},
        "actions": (
            {"action_type": "watch_reels", "target": "", "text": "", "quantity": 8},
            {"action_type": "like_posts", "target": "", "text": "", "quantity": 2},
        ),
    },
    {
        "title": "Dia 2",
        "settings": {"base_delay_minutes": 30},
        "actions": (
            {"action_type": "watch_reels", "target": "", "text": "", "quantity": 10},
            {"action_type": "follow_accounts", "target": "", "text": "", "quantity": 1},
        ),
    },
    {
        "title": "Dia 3",
        "settings": {"base_delay_minutes": 45},
        "actions": (
            {"action_type": "watch_reels", "target": "", "text": "", "quantity": 12},
            {"action_type": "send_message", "target": "", "text": "Hola, como estas?", "quantity": 1},
        ),
    },
)


class WarmupService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        self.db_path = self.context.root_dir / "data" / "warmup.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ensure_schema()
        self._recover_running_flows()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("pragma foreign_keys = on")
        return connection

    def _ensure_schema(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                create table if not exists warmup_flows (
                    id integer primary key autoincrement,
                    alias text not null,
                    name text not null,
                    status text not null default 'paused',
                    has_started integer not null default 0,
                    current_stage_order integer not null default 1,
                    current_action_order integer not null default 1,
                    last_account text not null default '',
                    created_at text not null,
                    updated_at text not null
                );

                create table if not exists warmup_stages (
                    id integer primary key autoincrement,
                    flow_id integer not null references warmup_flows(id) on delete cascade,
                    stage_order integer not null,
                    title text not null,
                    enabled integer not null default 1,
                    settings_json text not null default '{}',
                    created_at text not null,
                    updated_at text not null
                );

                create table if not exists warmup_actions (
                    id integer primary key autoincrement,
                    stage_id integer not null references warmup_stages(id) on delete cascade,
                    action_order integer not null,
                    action_type text not null,
                    payload_json text not null default '{}',
                    created_at text not null,
                    updated_at text not null
                );

                create table if not exists warmup_flow_accounts (
                    flow_id integer not null references warmup_flows(id) on delete cascade,
                    account_order integer not null default 1,
                    username text not null,
                    created_at text not null,
                    primary key (flow_id, username)
                );

                create table if not exists warmup_account_state (
                    flow_id integer not null references warmup_flows(id) on delete cascade,
                    username text not null,
                    status text not null default 'paused',
                    current_stage_order integer not null default 1,
                    current_action_order integer not null default 1,
                    last_action_type text not null default '',
                    updated_at text not null,
                    payload_json text not null default '{}',
                    primary key (flow_id, username)
                );

                create table if not exists warmup_logs (
                    id integer primary key autoincrement,
                    flow_id integer not null references warmup_flows(id) on delete cascade,
                    level text not null default 'info',
                    message text not null,
                    created_at text not null
                );

                create unique index if not exists idx_warmup_flow_alias_name
                    on warmup_flows(alias, name);
                create unique index if not exists idx_warmup_stage_flow_order
                    on warmup_stages(flow_id, stage_order);
                create unique index if not exists idx_warmup_action_stage_order
                    on warmup_actions(stage_id, action_order);
                create unique index if not exists idx_warmup_flow_account_order
                    on warmup_flow_accounts(flow_id, account_order, username);
                create index if not exists idx_warmup_account_state_status
                    on warmup_account_state(status);
                create index if not exists idx_warmup_logs_flow_id
                    on warmup_logs(flow_id, id);
                """
            )
            self._ensure_column(connection, "warmup_flows", "has_started", "integer not null default 0")

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        definition: str,
    ) -> None:
        existing = {
            str(row["name"] or "").strip().lower()
            for row in connection.execute(f"pragma table_info({table_name})").fetchall()
        }
        if column_name.lower() in existing:
            return
        connection.execute(f"alter table {table_name} add column {column_name} {definition}")

    def _recover_running_flows(self) -> None:
        with self._lock, self._connect() as connection:
            now = _utc_now_iso()
            running_ids = [
                int(row["id"])
                for row in connection.execute(
                    "select id from warmup_flows where lower(status) = 'running'"
                ).fetchall()
            ]
            connection.execute(
                """
                update warmup_flows
                set status='paused',
                    has_started = case when has_started > 0 then has_started else 1 end,
                    updated_at=?
                where lower(status) = 'running'
                """,
                (now,),
            )
            connection.execute(
                """
                update warmup_account_state
                set status='paused', updated_at=?
                where lower(status) = 'running'
                """,
                (now,),
            )
            for flow_id in running_ids:
                self._append_log_locked(
                    connection,
                    flow_id,
                    "Flujo pausado automaticamente al reabrir la aplicacion.",
                    level="warning",
                    created_at=now,
                )

    def ensure_default_flow(self, alias: str | None) -> dict[str, Any]:
        clean_alias = normalize_alias(alias, default="default")
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                select id
                from warmup_flows
                where lower(alias) = lower(?)
                  and lower(name) = lower('Warm Up')
                order by id asc
                limit 1
                """,
                (clean_alias,),
            ).fetchone()
            if row is None:
                flow_id = self._create_flow_locked(
                    connection,
                    clean_alias,
                    "Warm Up",
                    usernames=[],
                    stage_blueprints=_DEFAULT_STAGE_BLUEPRINTS,
                )
            else:
                flow_id = int(row["id"])
        return self.get_flow(flow_id)

    def create_flow(
        self,
        *,
        alias: str | None,
        usernames: list[str] | tuple[str, ...],
        name: str | None = None,
    ) -> dict[str, Any]:
        clean_alias = normalize_alias(alias, default="default")
        selected_usernames = dedupe_usernames(usernames)
        if not selected_usernames:
            raise RuntimeError("Selecciona al menos una cuenta para el flujo.")
        desired_name = str(name or "").strip() or "Flujo Warm Up"
        with self._lock, self._connect() as connection:
            flow_id = self._create_flow_locked(
                connection,
                clean_alias,
                self._next_available_flow_name_locked(connection, clean_alias, desired_name),
                usernames=selected_usernames,
                stage_blueprints=(),
            )
            self._append_log_locked(
                connection,
                flow_id,
                f"Flujo creado para alias {clean_alias} con {len(selected_usernames)} cuenta(s).",
            )
        return self.get_flow(flow_id)

    def delete_flow(self, flow_id: int) -> bool:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            return False
        with self._lock, self._connect() as connection:
            deleted = connection.execute(
                "delete from warmup_flows where id = ?",
                (clean_flow_id,),
            ).rowcount
        return bool(deleted)

    def alias_state_paths(self) -> list[str]:
        return [str(self.db_path)]

    def rename_alias_state(self, source_alias: str, target_alias: str) -> dict[str, Any]:
        source = normalize_alias(source_alias, default="default")
        target = normalize_alias(target_alias, default="default")
        if not source or not target:
            raise ServiceError("Alias invalido para warmup.")
        renamed_flows = 0
        renamed_titles = 0
        now = _utc_now_iso()
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                select id, name
                from warmup_flows
                where lower(alias) = lower(?)
                order by id asc
                """,
                (source,),
            ).fetchall()
            for row in rows:
                flow_id = int(row["id"])
                current_name = str(row["name"] or "").strip() or "Flujo Warm Up"
                next_name = current_name
                if source.lower() != target.lower():
                    next_name = self._next_available_flow_name_locked(connection, target, current_name)
                    if next_name != current_name:
                        renamed_titles += 1
                connection.execute(
                    """
                    update warmup_flows
                    set alias = ?, name = ?, updated_at = ?
                    where id = ?
                    """,
                    (target, next_name, now, flow_id),
                )
                self._append_log_locked(
                    connection,
                    flow_id,
                    f"Alias actualizado de {source} a {target}.",
                    created_at=now,
                )
                renamed_flows += 1
        return {
            "flows_updated": renamed_flows,
            "flow_names_renamed": renamed_titles,
        }

    def delete_alias_state(self, alias: str) -> dict[str, Any]:
        clean_alias = normalize_alias(alias, default="default")
        if not clean_alias:
            raise ServiceError("Alias invalido para warmup.")
        with self._lock, self._connect() as connection:
            deleted = connection.execute(
                "delete from warmup_flows where lower(alias) = lower(?)",
                (clean_alias,),
            ).rowcount
        return {"flows_deleted": int(deleted or 0)}

    def list_flows(self, alias: str | None = None) -> list[dict[str, Any]]:
        clean_alias = str(alias or "").strip()
        params: list[Any] = []
        where = ""
        if clean_alias:
            where = "where lower(flows.alias) = lower(?)"
            params.append(clean_alias)
        query = f"""
            select
                flows.*,
                count(distinct stages.id) as stages_count,
                count(distinct flow_accounts.username) as account_count,
                count(distinct logs.id) as log_count
            from warmup_flows as flows
            left join warmup_stages as stages on stages.flow_id = flows.id
            left join warmup_flow_accounts as flow_accounts on flow_accounts.flow_id = flows.id
            left join warmup_logs as logs on logs.flow_id = flows.id
            {where}
            group by flows.id
            order by lower(flows.alias) asc, lower(flows.name) asc, flows.id asc
        """
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._serialize_flow_summary(row) for row in rows]

    def get_flow(self, flow_id: int) -> dict[str, Any]:
        clean_id = int(flow_id or 0)
        if clean_id <= 0:
            return {}
        with self._lock, self._connect() as connection:
            flow_row = connection.execute(
                "select * from warmup_flows where id = ? limit 1",
                (clean_id,),
            ).fetchone()
            if flow_row is None:
                return {}
            stage_rows = connection.execute(
                """
                select *
                from warmup_stages
                where flow_id = ?
                order by stage_order asc, id asc
                """,
                (clean_id,),
            ).fetchall()
            action_rows = connection.execute(
                """
                select actions.*, stages.stage_order
                from warmup_actions as actions
                join warmup_stages as stages on stages.id = actions.stage_id
                where stages.flow_id = ?
                order by stages.stage_order asc, actions.action_order asc, actions.id asc
                """,
                (clean_id,),
            ).fetchall()
            selected_account_rows = connection.execute(
                """
                select *
                from warmup_flow_accounts
                where flow_id = ?
                order by account_order asc, username asc
                """,
                (clean_id,),
            ).fetchall()
            account_rows = connection.execute(
                """
                select *
                from warmup_account_state
                where flow_id = ?
                order by updated_at desc, username asc
                """,
                (clean_id,),
            ).fetchall()
            latest_log_id = int(
                connection.execute(
                    "select coalesce(max(id), 0) from warmup_logs where flow_id = ?",
                    (clean_id,),
                ).fetchone()[0]
                or 0
            )
        actions_by_stage: dict[int, list[dict[str, Any]]] = {}
        for row in action_rows:
            stage_id = int(row["stage_id"])
            actions_by_stage.setdefault(stage_id, []).append(self._serialize_action(row))
        stages = []
        for row in stage_rows:
            stage_id = int(row["id"])
            stages.append(
                {
                    "id": stage_id,
                    "flow_id": int(row["flow_id"]),
                    "stage_order": int(row["stage_order"]),
                    "title": str(row["title"] or "").strip() or f"Dia {int(row['stage_order'])}",
                    "enabled": bool(row["enabled"]),
                    "settings": self._json_loads(row["settings_json"], {}),
                    "actions": actions_by_stage.get(stage_id, []),
                    "created_at": str(row["created_at"] or "").strip(),
                    "updated_at": str(row["updated_at"] or "").strip(),
                }
            )
        selected_accounts = [self._serialize_selected_account(row) for row in selected_account_rows]
        accounts = [self._serialize_account_state(row) for row in account_rows]
        resume = self._build_resume_snapshot(
            flow_row,
            accounts,
        )
        return {
            "id": int(flow_row["id"]),
            "alias": str(flow_row["alias"] or "").strip(),
            "name": str(flow_row["name"] or "").strip(),
            "status": _normalize_status(flow_row["status"]),
            "has_started": bool(flow_row["has_started"]),
            "current_stage_order": int(flow_row["current_stage_order"] or 1),
            "current_action_order": int(flow_row["current_action_order"] or 1),
            "last_account": str(flow_row["last_account"] or "").strip(),
            "created_at": str(flow_row["created_at"] or "").strip(),
            "updated_at": str(flow_row["updated_at"] or "").strip(),
            "stages": stages,
            "selected_accounts": selected_accounts,
            "selected_usernames": [item["username"] for item in selected_accounts],
            "account_count": len(selected_accounts),
            "account_states": accounts,
            "accounts": accounts,
            "resume": resume,
            "latest_log_id": latest_log_id,
        }

    def create_stage(self, flow_id: int, *, title: str | None = None) -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            raise RuntimeError("Flow ID invalido.")
        with self._lock, self._connect() as connection:
            next_order = (
                connection.execute(
                    "select coalesce(max(stage_order), 0) + 1 from warmup_stages where flow_id = ?",
                    (clean_flow_id,),
                ).fetchone()[0]
                or 1
            )
            now = _utc_now_iso()
            cursor = connection.execute(
                """
                insert into warmup_stages (
                    flow_id,
                    stage_order,
                    title,
                    enabled,
                    settings_json,
                    created_at,
                    updated_at
                )
                values (?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    clean_flow_id,
                    int(next_order),
                    str(title or "").strip() or f"Dia {int(next_order)}",
                    json.dumps({"base_delay_minutes": 20}, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            stage_id = int(cursor.lastrowid)
        return self.get_flow(clean_flow_id)

    def save_stage(
        self,
        flow_id: int,
        *,
        stage_id: int | None = None,
        title: str,
        settings: dict[str, Any] | None = None,
        actions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            raise RuntimeError("Flow ID invalido.")
        clean_title = str(title or "").strip()
        if not clean_title:
            raise RuntimeError("El titulo de la etapa es obligatorio.")
        normalized_settings = dict(settings or {})
        normalized_actions = [self._normalize_action_payload(item) for item in actions or []]
        if not normalized_actions:
            normalized_actions = [self._normalize_action_payload({"action_type": "watch_reels", "quantity": 5})]
        with self._lock, self._connect() as connection:
            now = _utc_now_iso()
            clean_stage_id = int(stage_id or 0)
            if clean_stage_id > 0:
                existing = connection.execute(
                    "select id from warmup_stages where id = ? and flow_id = ? limit 1",
                    (clean_stage_id, clean_flow_id),
                ).fetchone()
                if existing is None:
                    raise RuntimeError("La etapa ya no existe.")
                connection.execute(
                    """
                    update warmup_stages
                    set title = ?, settings_json = ?, updated_at = ?
                    where id = ? and flow_id = ?
                    """,
                    (
                        clean_title,
                        json.dumps(normalized_settings, ensure_ascii=False, sort_keys=True),
                        now,
                        clean_stage_id,
                        clean_flow_id,
                    ),
                )
            else:
                next_order = (
                    connection.execute(
                        "select coalesce(max(stage_order), 0) + 1 from warmup_stages where flow_id = ?",
                        (clean_flow_id,),
                    ).fetchone()[0]
                    or 1
                )
                stage_cursor = connection.execute(
                    """
                    insert into warmup_stages (
                        flow_id,
                        stage_order,
                        title,
                        enabled,
                        settings_json,
                        created_at,
                        updated_at
                    )
                    values (?, ?, ?, 1, ?, ?, ?)
                    """,
                    (
                        clean_flow_id,
                        int(next_order),
                        clean_title,
                        json.dumps(normalized_settings, ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
                clean_stage_id = int(stage_cursor.lastrowid)
            connection.execute("delete from warmup_actions where stage_id = ?", (clean_stage_id,))
            for index, action in enumerate(normalized_actions, start=1):
                connection.execute(
                    """
                    insert into warmup_actions (
                        stage_id,
                        action_order,
                        action_type,
                        payload_json,
                        created_at,
                        updated_at
                    )
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        clean_stage_id,
                        index,
                        action["action_type"],
                        json.dumps(action["payload"], ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
            connection.execute(
                """
                update warmup_flows
                set updated_at = ?, status = case when lower(status) = 'running' then 'running' else 'paused' end
                where id = ?
                """,
                (now, clean_flow_id),
            )
        return self.get_flow(clean_flow_id)

    def delete_stage(self, stage_id: int) -> dict[str, Any]:
        clean_stage_id = int(stage_id or 0)
        if clean_stage_id <= 0:
            return {}
        flow_id = 0
        with self._lock, self._connect() as connection:
            stage_row = connection.execute(
                "select flow_id from warmup_stages where id = ? limit 1",
                (clean_stage_id,),
            ).fetchone()
            if stage_row is None:
                return {}
            flow_id = int(stage_row["flow_id"])
            connection.execute("delete from warmup_stages where id = ?", (clean_stage_id,))
            self._resequence_stages_locked(connection, flow_id)
            connection.execute(
                "update warmup_flows set updated_at = ?, status = 'paused' where id = ?",
                (_utc_now_iso(), flow_id),
            )
        return self.get_flow(flow_id)

    def append_log(self, flow_id: int, message: str, *, level: str = "info") -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        clean_message = str(message or "").strip()
        if clean_flow_id <= 0 or not clean_message:
            return {}
        with self._lock, self._connect() as connection:
            return self._append_log_locked(connection, clean_flow_id, clean_message, level=level)

    def list_logs(self, flow_id: int) -> list[dict[str, Any]]:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            return []
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                select *
                from warmup_logs
                where flow_id = ?
                order by id asc
                """,
                (clean_flow_id,),
            ).fetchall()
        return [self._serialize_log(row) for row in rows]

    def read_logs_after(self, flow_id: int, last_log_id: int = 0) -> tuple[int, list[dict[str, Any]]]:
        clean_flow_id = int(flow_id or 0)
        clean_last_id = max(0, int(last_log_id or 0))
        if clean_flow_id <= 0:
            return 0, []
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                select *
                from warmup_logs
                where flow_id = ? and id > ?
                order by id asc
                """,
                (clean_flow_id, clean_last_id),
            ).fetchall()
        logs = [self._serialize_log(row) for row in rows]
        next_log_id = clean_last_id
        if logs:
            next_log_id = int(logs[-1]["id"] or clean_last_id)
        return next_log_id, logs

    def mark_flow_running(
        self,
        flow_id: int,
        *,
        stage_order: int | None = None,
        action_order: int | None = None,
        last_account: str = "",
    ) -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            return {}
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                select current_stage_order, current_action_order, last_account
                from warmup_flows
                where id = ?
                limit 1
                """,
                (clean_flow_id,),
            ).fetchone()
            if row is None:
                return {}
            now = _utc_now_iso()
            connection.execute(
                """
                update warmup_flows
                set status = 'running',
                    has_started = 1,
                    current_stage_order = ?,
                    current_action_order = ?,
                    last_account = ?,
                    updated_at = ?
                where id = ?
                """,
                (
                    max(1, int(stage_order or row["current_stage_order"] or 1)),
                    max(1, int(action_order or row["current_action_order"] or 1)),
                    str(last_account or row["last_account"] or "").strip(),
                    now,
                    clean_flow_id,
                ),
            )
        return self.get_flow(clean_flow_id)

    def record_account_state(
        self,
        flow_id: int,
        username: str,
        *,
        stage_order: int,
        action_order: int,
        last_action_type: str = "",
        status: str = "paused",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        clean_username = str(username or "").strip().lstrip("@")
        if clean_flow_id <= 0 or not clean_username:
            return {}
        now = _utc_now_iso()
        normalized_status = _normalize_status(status)
        normalized_payload = dict(payload or {})
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                insert into warmup_account_state (
                    flow_id,
                    username,
                    status,
                    current_stage_order,
                    current_action_order,
                    last_action_type,
                    updated_at,
                    payload_json
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(flow_id, username) do update set
                    status=excluded.status,
                    current_stage_order=excluded.current_stage_order,
                    current_action_order=excluded.current_action_order,
                    last_action_type=excluded.last_action_type,
                    updated_at=excluded.updated_at,
                    payload_json=excluded.payload_json
                """,
                (
                    clean_flow_id,
                    clean_username,
                    normalized_status,
                    max(1, int(stage_order or 1)),
                    max(1, int(action_order or 1)),
                    str(last_action_type or "").strip(),
                    now,
                    json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            connection.execute(
                """
                update warmup_flows
                set status = ?,
                    has_started = 1,
                    current_stage_order = ?,
                    current_action_order = ?,
                    last_account = ?,
                    updated_at = ?
                where id = ?
                """,
                (
                    normalized_status,
                    max(1, int(stage_order or 1)),
                    max(1, int(action_order or 1)),
                    clean_username,
                    now,
                    clean_flow_id,
                ),
            )
        return self.get_flow(clean_flow_id)

    def pause_flow(self, flow_id: int, *, reason: str = "paused") -> dict[str, Any]:
        clean_flow_id = int(flow_id or 0)
        if clean_flow_id <= 0:
            return {}
        now = _utc_now_iso()
        detail = str(reason or "").strip() or "paused"
        with self._lock, self._connect() as connection:
            account_rows = connection.execute(
                """
                select username, payload_json
                from warmup_account_state
                where flow_id = ?
                """,
                (clean_flow_id,),
            ).fetchall()
            connection.execute(
                """
                update warmup_flows
                set status = 'paused',
                    has_started = case when has_started > 0 then has_started else 1 end,
                    updated_at = ?
                where id = ?
                """,
                (now, clean_flow_id),
            )
            for row in account_rows:
                payload = self._json_loads(row["payload_json"], {})
                payload["pause_reason"] = detail
                connection.execute(
                    """
                    update warmup_account_state
                    set status = 'paused', updated_at = ?, payload_json = ?
                    where flow_id = ? and username = ?
                    """,
                    (
                        now,
                        json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        clean_flow_id,
                        str(row["username"] or "").strip(),
                    ),
                )
            self._append_log_locked(
                connection,
                clean_flow_id,
                f"Flujo pausado: {detail}.",
                level="warning",
                created_at=now,
            )
        return self.get_flow(clean_flow_id)

    def pause_active_flows(self, reason: str = "application closing") -> int:
        now = _utc_now_iso()
        detail = str(reason or "").strip() or "application closing"
        with self._lock, self._connect() as connection:
            flow_ids = [
                int(row["id"])
                for row in connection.execute(
                    "select id from warmup_flows where lower(status) = 'running'"
                ).fetchall()
            ]
            account_rows = connection.execute(
                """
                select flow_id, username, payload_json
                from warmup_account_state
                where lower(status) = 'running'
                """
            ).fetchall()
            connection.execute(
                """
                update warmup_flows
                set status = 'paused',
                    has_started = case when has_started > 0 then has_started else 1 end,
                    updated_at = ?
                where lower(status) = 'running'
                """,
                (now,),
            )
            for row in account_rows:
                payload = self._json_loads(row["payload_json"], {})
                payload["pause_reason"] = detail
                connection.execute(
                    """
                    update warmup_account_state
                    set status = 'paused',
                        updated_at = ?,
                        payload_json = ?
                    where flow_id = ? and username = ?
                    """,
                    (
                        now,
                        json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        int(row["flow_id"]),
                        str(row["username"] or "").strip(),
                    ),
                )
            for flow_id in flow_ids:
                self._append_log_locked(
                    connection,
                    flow_id,
                    f"Flujo pausado: {detail}.",
                    level="warning",
                    created_at=now,
                )
        return len(flow_ids)

    def _create_flow_locked(
        self,
        connection: sqlite3.Connection,
        alias: str,
        name: str,
        *,
        usernames: list[str],
        stage_blueprints: tuple[dict[str, Any], ...] | tuple[()] = (),
    ) -> int:
        now = _utc_now_iso()
        cursor = connection.execute(
            """
            insert into warmup_flows (
                alias,
                name,
                status,
                has_started,
                current_stage_order,
                current_action_order,
                last_account,
                created_at,
                updated_at
            )
            values (?, ?, 'paused', 0, 1, 1, '', ?, ?)
            """,
            (alias, name, now, now),
        )
        flow_id = int(cursor.lastrowid)
        self._replace_flow_accounts_locked(connection, flow_id, usernames, created_at=now)
        for stage_order, blueprint in enumerate(stage_blueprints or (), start=1):
            stage_cursor = connection.execute(
                """
                insert into warmup_stages (
                    flow_id,
                    stage_order,
                    title,
                    enabled,
                    settings_json,
                    created_at,
                    updated_at
                )
                values (?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    flow_id,
                    stage_order,
                    str(blueprint.get("title") or f"Dia {stage_order}").strip(),
                    json.dumps(dict(blueprint.get("settings") or {}), ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            stage_id = int(stage_cursor.lastrowid)
            for action_order, action in enumerate(blueprint.get("actions") or [], start=1):
                normalized_action = self._normalize_action_payload(dict(action))
                connection.execute(
                    """
                    insert into warmup_actions (
                        stage_id,
                        action_order,
                        action_type,
                        payload_json,
                        created_at,
                        updated_at
                    )
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stage_id,
                        action_order,
                        normalized_action["action_type"],
                        json.dumps(normalized_action["payload"], ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
        return flow_id

    def _replace_flow_accounts_locked(
        self,
        connection: sqlite3.Connection,
        flow_id: int,
        usernames: list[str],
        *,
        created_at: str,
    ) -> None:
        connection.execute("delete from warmup_flow_accounts where flow_id = ?", (flow_id,))
        for account_order, username in enumerate(dedupe_usernames(usernames), start=1):
            connection.execute(
                """
                insert into warmup_flow_accounts (
                    flow_id,
                    account_order,
                    username,
                    created_at
                )
                values (?, ?, ?, ?)
                """,
                (flow_id, account_order, username, created_at),
            )

    def _next_available_flow_name_locked(
        self,
        connection: sqlite3.Connection,
        alias: str,
        desired_name: str,
    ) -> str:
        base_name = str(desired_name or "").strip() or "Flujo Warm Up"
        existing = {
            str(row["name"] or "").strip().lower()
            for row in connection.execute(
                "select name from warmup_flows where lower(alias) = lower(?)",
                (alias,),
            ).fetchall()
        }
        if base_name.lower() not in existing:
            return base_name
        index = 2
        while True:
            candidate = f"{base_name} {index}"
            if candidate.lower() not in existing:
                return candidate
            index += 1

    def _append_log_locked(
        self,
        connection: sqlite3.Connection,
        flow_id: int,
        message: str,
        *,
        level: str = "info",
        created_at: str | None = None,
    ) -> dict[str, Any]:
        now = created_at or _utc_now_iso()
        cursor = connection.execute(
            """
            insert into warmup_logs (
                flow_id,
                level,
                message,
                created_at
            )
            values (?, ?, ?, ?)
            """,
            (flow_id, _normalize_log_level(level), str(message or "").strip(), now),
        )
        return {
            "id": int(cursor.lastrowid or 0),
            "flow_id": int(flow_id),
            "level": _normalize_log_level(level),
            "message": str(message or "").strip(),
            "created_at": now,
        }

    def _normalize_action_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_type = str(payload.get("action_type") or "").strip().lower() or "watch_reels"
        quantity = max(1, int(payload.get("quantity") or 1))
        normalized_payload = {
            "target": str(payload.get("target") or "").strip(),
            "text": str(payload.get("text") or "").strip(),
            "quantity": quantity,
        }
        extra_payload = payload.get("payload")
        if isinstance(extra_payload, dict):
            normalized_payload.update(extra_payload)
            normalized_payload["quantity"] = max(1, int(normalized_payload.get("quantity") or quantity))
            normalized_payload["target"] = str(normalized_payload.get("target") or "").strip()
            normalized_payload["text"] = str(normalized_payload.get("text") or "").strip()
        return {"action_type": action_type, "payload": normalized_payload}

    def _serialize_action(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._json_loads(row["payload_json"], {})
        return {
            "id": int(row["id"]),
            "stage_id": int(row["stage_id"]),
            "action_order": int(row["action_order"]),
            "action_type": str(row["action_type"] or "").strip().lower(),
            "payload": payload,
            "target": str(payload.get("target") or "").strip(),
            "text": str(payload.get("text") or "").strip(),
            "quantity": max(1, int(payload.get("quantity") or 1)),
            "created_at": str(row["created_at"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
        }

    def _serialize_flow_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "alias": str(row["alias"] or "").strip(),
            "name": str(row["name"] or "").strip(),
            "status": _normalize_status(row["status"]),
            "has_started": bool(row["has_started"]),
            "current_stage_order": int(row["current_stage_order"] or 1),
            "current_action_order": int(row["current_action_order"] or 1),
            "last_account": str(row["last_account"] or "").strip(),
            "stages_count": int(row["stages_count"] or 0),
            "account_count": int(row["account_count"] or 0),
            "log_count": int(row["log_count"] or 0),
            "created_at": str(row["created_at"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
        }

    def _serialize_selected_account(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "flow_id": int(row["flow_id"]),
            "account_order": int(row["account_order"] or 1),
            "username": str(row["username"] or "").strip().lstrip("@"),
            "created_at": str(row["created_at"] or "").strip(),
        }

    def _serialize_account_state(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._json_loads(row["payload_json"], {})
        return {
            "flow_id": int(row["flow_id"]),
            "username": str(row["username"] or "").strip().lstrip("@"),
            "status": _normalize_status(row["status"]),
            "current_stage_order": int(row["current_stage_order"] or 1),
            "current_action_order": int(row["current_action_order"] or 1),
            "last_action_type": str(row["last_action_type"] or "").strip().lower(),
            "updated_at": str(row["updated_at"] or "").strip(),
            "payload": payload,
        }

    def _serialize_log(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "flow_id": int(row["flow_id"]),
            "level": _normalize_log_level(row["level"]),
            "message": str(row["message"] or "").strip(),
            "created_at": str(row["created_at"] or "").strip(),
        }

    def _build_resume_snapshot(
        self,
        flow_state: dict[str, Any] | sqlite3.Row,
        accounts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        last_account = str(flow_state["last_account"] if isinstance(flow_state, sqlite3.Row) else flow_state.get("last_account") or "").strip().lstrip("@")
        latest = next(
            (
                item
                for item in accounts
                if str(item.get("username") or "").strip().lower() == last_account.lower()
            ),
            accounts[0] if accounts else {},
        )
        return {
            "status": _normalize_status(
                (latest or {}).get("status")
                or (flow_state["status"] if isinstance(flow_state, sqlite3.Row) else flow_state.get("status"))
                or "paused"
            ),
            "has_started": bool(
                (flow_state["has_started"] if isinstance(flow_state, sqlite3.Row) else flow_state.get("has_started"))
                or bool(latest)
            ),
            "current_stage_order": int(
                (latest or {}).get("current_stage_order")
                or (flow_state["current_stage_order"] if isinstance(flow_state, sqlite3.Row) else flow_state.get("current_stage_order"))
                or 1
            ),
            "current_action_order": int(
                (latest or {}).get("current_action_order")
                or (flow_state["current_action_order"] if isinstance(flow_state, sqlite3.Row) else flow_state.get("current_action_order"))
                or 1
            ),
            "last_account": str((latest or {}).get("username") or last_account).strip(),
            "last_action_type": str((latest or {}).get("last_action_type") or "").strip(),
            "accounts": accounts,
        }

    def _resequence_stages_locked(self, connection: sqlite3.Connection, flow_id: int) -> None:
        stage_rows = connection.execute(
            """
            select id
            from warmup_stages
            where flow_id = ?
            order by stage_order asc, id asc
            """,
            (flow_id,),
        ).fetchall()
        for index, row in enumerate(stage_rows, start=1):
            connection.execute(
                "update warmup_stages set stage_order = ?, updated_at = ? where id = ?",
                (index, _utc_now_iso(), int(row["id"])),
            )

    @staticmethod
    def _json_loads(raw: Any, default: Any) -> Any:
        try:
            payload = json.loads(str(raw or "").strip() or "null")
        except Exception:
            return default
        return payload if payload is not None else default
