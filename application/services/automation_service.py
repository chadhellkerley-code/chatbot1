from __future__ import annotations

import logging
import textwrap
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from automation import whatsapp as whatsapp_module
from automation.whatsapp import WhatsAppDataStore
from config import read_app_config, read_env_local, update_app_config, update_env_local
from core import accounts as accounts_module
from core import responder as responder_module
from paths import storage_root

from .base import ServiceContext, ServiceError


_OBJECTION_PROMPTS_KEY = "automation_objection_prompts"
logger = logging.getLogger(__name__)


class AutomationService:
    def __init__(self, context: ServiceContext, inbox_service: Any | None = None) -> None:
        self.context = context
        self._inbox_service = inbox_service
        self._autoresponder_lock = threading.RLock()
        self._autoresponder_state: dict[str, Any] = {}
        self._sync_legacy_storage()

    def _inbox_runtime_bridge(self) -> Any | None:
        inbox_service = self._inbox_service
        if inbox_service is None:
            return None
        return getattr(inbox_service, "_automation", None)

    def _runtime_bridge_snapshot(self, alias: str) -> dict[str, Any]:
        bridge = self._inbox_runtime_bridge()
        if bridge is None:
            return {}
        status = bridge.status(alias)
        account_rows = [
            {
                "account": str(row.get("username") or "").strip(),
                "active": True,
                "blocked": False,
                "blocked_reason": "",
                "safety_state": "usable",
                "safety_reason": "",
                "safety_message": "Lista",
                "proxy": str(
                    row.get("proxy_url")
                    or row.get("proxy")
                    or row.get("assigned_proxy_id")
                    or ""
                ).strip(),
            }
            for row in bridge.alias_accounts(alias)
            if isinstance(row, dict)
        ]
        stats = dict(status.get("stats") or {})
        return {
            "run_id": f"inbox-runtime:{alias}",
            "alias": str(alias or "").strip(),
            "status": "Running" if bool(status.get("is_running")) else "Stopped",
            "message": str(status.get("last_error") or "").strip() or (
                "Runtime operativo desde Inbox." if bool(status.get("is_running")) else "Runtime detenido."
            ),
            "task_active": bool(status.get("is_running")),
            "started_at": "",
            "finished_at": "",
            "delay_min": max(0, int(status.get("delay_min_ms") or 0)) // 1000,
            "delay_max": max(0, int(status.get("delay_max_ms") or 0)) // 1000,
            "concurrency": 1,
            "threads": 0,
            "followup_only": str(status.get("mode") or "").strip().lower() == "followup",
            "followup_schedule_label": "",
            "accounts_total": len(account_rows),
            "accounts_active": len(account_rows),
            "accounts_blocked": 0,
            "account_rows": account_rows,
            "current_account": str(status.get("current_account_id") or "").strip(),
            "next_account": str(status.get("next_account_id") or "").strip(),
            "mode": str(status.get("mode") or "both").strip(),
            "turns_per_account": int(status.get("max_turns_per_account") or 1),
            "message_success": int(stats.get("queued_jobs") or 0),
            "message_failed": int(stats.get("errors") or 0),
            "followup_success": 0,
            "followup_failed": 0,
            "agendas_generated": 0,
        }

    def _legacy_storage_root(self) -> Path:
        return storage_root(Path(self.context.root_dir), scoped=False, honor_env=False)

    def _sync_legacy_json_file(
        self,
        *,
        primary: Path,
        legacy: Path,
        default: dict[str, Any],
        has_data: Callable[[dict[str, Any]], bool],
    ) -> bool:
        try:
            if primary.resolve() == legacy.resolve():
                return False
        except Exception:
            pass
        if not legacy.exists():
            return False
        if primary.exists():
            primary_payload = self.context.read_json(primary, default)
            if has_data(primary_payload):
                return False
        legacy_payload = self.context.read_json(legacy, default)
        if not has_data(legacy_payload):
            return False
        try:
            self.context.write_json(primary, legacy_payload)
            return True
        except Exception:
            logger.exception("No se pudo migrar storage legacy %s -> %s", legacy, primary)
            return False

    def _sync_legacy_storage(self) -> None:
        legacy_root = self._legacy_storage_root()
        copied = False
        copied = self._sync_legacy_json_file(
            primary=Path(
                getattr(
                    responder_module,
                    "_PROMPTS_FILE",
                    self.context.storage_path("autoresponder_prompts.json"),
                )
            ),
            legacy=legacy_root / "autoresponder_prompts.json",
            default={"aliases": {}, "accounts": {}},
            has_data=lambda payload: bool(dict(payload.get("aliases") or {}) or dict(payload.get("accounts") or {})),
        ) or copied
        copied = self._sync_legacy_json_file(
            primary=Path(
                getattr(
                    responder_module,
                    "_PACKS_FILE",
                    self.context.storage_path("conversational_packs.json"),
                )
            ),
            legacy=legacy_root / "conversational_packs.json",
            default={"packs": []},
            has_data=lambda payload: bool(list(payload.get("packs") or [])),
        ) or copied
        copied = self._sync_legacy_json_file(
            primary=Path(
                getattr(
                    responder_module,
                    "_FOLLOWUP_FILE",
                    self.context.storage_path("followups.json"),
                )
            ),
            legacy=legacy_root / "followups.json",
            default={"aliases": {}, "accounts": {}},
            has_data=lambda payload: bool(dict(payload.get("aliases") or {}) or dict(payload.get("accounts") or {})),
        ) or copied
        copied = self._sync_legacy_json_file(
            primary=Path(
                getattr(
                    responder_module,
                    "_ACCOUNT_MEMORY_FILE",
                    self.context.storage_path("autoresponder_account_memory.json"),
                )
            ),
            legacy=legacy_root / "autoresponder_account_memory.json",
            default={"accounts": {}},
            has_data=lambda payload: bool(dict(payload.get("accounts") or {})),
        ) or copied
        if not copied:
            return
        refresh_caches = getattr(responder_module, "_refresh_autoresponder_storage_caches", None)
        if callable(refresh_caches):
            refresh_caches()
        refresh_account_memory = getattr(responder_module, "_read_account_memory_state", None)
        if callable(refresh_account_memory):
            refresh_account_memory(refresh=True)

    def _copy_autoresponder_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        snapshot = dict(payload)
        account_rows = snapshot.get("account_rows")
        if isinstance(account_rows, list):
            snapshot["account_rows"] = [
                dict(row) if isinstance(row, dict) else row
                for row in account_rows
            ]
        return snapshot

    def _update_autoresponder_state(self, payload: dict[str, Any], *, replace: bool = False) -> None:
        if not isinstance(payload, dict):
            return
        with self._autoresponder_lock:
            if replace:
                self._autoresponder_state = self._copy_autoresponder_state(payload)
                return
            current = self._copy_autoresponder_state(self._autoresponder_state)
            current.update(self._copy_autoresponder_state(payload))
            self._autoresponder_state = current

    def current_autoresponder_snapshot(self, *, alias: str = "") -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        with self._autoresponder_lock:
            if not self._autoresponder_state:
                return {}
            current = self._copy_autoresponder_state(self._autoresponder_state)
        if clean_alias and clean_alias != str(current.get("alias") or "").strip():
            return {}
        return current

    @staticmethod
    def _autoresponder_inbox_only_message() -> str:
        return "El runtime de autoresponder/follow-up ahora se administra solo desde Inbox."

    def _autoresponder_wrapper_snapshot(self, alias: str) -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            return {}
        bridge = self._inbox_runtime_bridge()
        if bridge is not None:
            snapshot = self._runtime_bridge_snapshot(clean_alias)
            if snapshot and bool(snapshot.get("task_active")):
                return snapshot
            if snapshot:
                wrapped = dict(snapshot)
                wrapped.update(
                    {
                        "status": "Idle",
                        "message": self._autoresponder_inbox_only_message(),
                        "task_active": False,
                    }
                )
                return wrapped
        account_rows = []
        accounts_blocked = 0
        for row in self.alias_account_rows(clean_alias):
            if not isinstance(row, dict):
                continue
            blocked = bool(row.get("blocked"))
            if blocked:
                accounts_blocked += 1
            account_rows.append(
                {
                    "account": str(row.get("username") or "").strip(),
                    "proxy": str(row.get("proxy") or ""),
                    "blocked": blocked,
                    "blocked_reason": str(row.get("blocked_reason") or ""),
                    "blocked_remaining_seconds": float(row.get("blocked_remaining_seconds") or 0.0),
                    "safety_state": str(row.get("safety_state") or ("blocked" if blocked else "usable")).strip() or "usable",
                    "safety_reason": str(row.get("safety_reason") or row.get("blocked_reason") or "").strip(),
                    "safety_message": str(
                        row.get("safety_message")
                        or row.get("blocked_reason")
                        or ("Lista" if not blocked else "Cuenta bloqueada")
                    ).strip(),
                }
            )
        return {
            "run_id": "",
            "alias": clean_alias,
            "status": "Idle",
            "message": self._autoresponder_inbox_only_message(),
            "started_at": "",
            "finished_at": "",
            "delay_min": 45,
            "delay_max": 76,
            "concurrency": 1,
            "threads": 0,
            "followup_only": False,
            "followup_schedule_label": self.resolve_followup_schedule_label(clean_alias),
            "accounts_total": len(account_rows),
            "accounts_active": max(0, len(account_rows) - accounts_blocked),
            "accounts_blocked": accounts_blocked,
            "message_success": 0,
            "message_failed": 0,
            "followup_success": 0,
            "followup_failed": 0,
            "agendas_generated": 0,
            "account_rows": account_rows,
            "task_active": False,
        }

    def _all_account_records(self) -> list[dict[str, Any]]:
        return [dict(item) for item in accounts_module.list_all() if isinstance(item, dict)]

    def _account_username(self, value: Any) -> str:
        return str(value or "").strip().lstrip("@")

    def _account_alias(self, value: Any) -> str:
        return str(value or "").strip()

    def _active_alias_records(self, alias: str) -> list[dict[str, Any]]:
        clean_alias = str(alias or "").strip().lower()
        rows: list[dict[str, Any]] = []
        for record in self._all_account_records():
            username = self._account_username(record.get("username"))
            if not username:
                continue
            if self._account_alias(record.get("alias")).lower() != clean_alias:
                continue
            if not bool(record.get("active", True)):
                continue
            rows.append(record)
        return rows

    def _proxy_key(self, record: dict[str, Any]) -> str:
        return str(
            record.get("proxy_url")
            or record.get("proxy")
            or record.get("proxy_host")
            or record.get("proxy_name")
            or ""
        ).strip()

    def _active_alias_autoresponder_safety(
        self,
        alias: str,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        records = self._active_alias_records(alias)
        inspection = self._autoresponder_startability(
            [
                username
                for record in records
                if (username := self._account_username(record.get("username")))
            ]
        )
        indexed_status: dict[str, dict[str, Any]] = {}
        for item in inspection.get("account_statuses") or []:
            if not isinstance(item, dict):
                continue
            username = self._account_username(item.get("username"))
            if not username:
                continue
            indexed_status[username.lower()] = dict(item)
        if indexed_status:
            return records, indexed_status
        startable = {
            self._account_username(item).lower()
            for item in (inspection.get("startable_accounts") or [])
            if self._account_username(item)
        }
        for record in records:
            username = self._account_username(record.get("username"))
            if not username:
                continue
            blocked = not startable or username.lower() not in startable
            indexed_status[username.lower()] = {
                "username": username,
                "blocked": blocked,
                "safety_state": "blocked" if blocked else "usable",
                "reason": "blocked" if blocked else "usable",
                "message": "Cuenta bloqueada" if blocked else "Lista",
                "remaining_seconds": 0.0,
            }
        return records, indexed_status

    def max_alias_concurrency(self, alias: str) -> int:
        rows = self.alias_account_rows(alias)
        usable_rows = [row for row in rows if isinstance(row, dict) and not bool(row.get("blocked"))]
        if not usable_rows:
            return 1
        proxy_keys = {
            str(row.get("proxy") or "").strip()
            for row in usable_rows
            if str(row.get("proxy") or "").strip()
        }
        if proxy_keys:
            return max(1, len(proxy_keys))
        return 1

    def alias_account_rows(self, alias: str) -> list[dict[str, Any]]:
        records, safety_by_username = self._active_alias_autoresponder_safety(alias)
        rows: list[dict[str, Any]] = []
        for record in records:
            username = self._account_username(record.get("username"))
            if not username:
                continue
            safety = dict(safety_by_username.get(username.lower()) or {})
            safety_state = str(safety.get("safety_state") or ("blocked" if bool(safety.get("blocked")) else "usable")).strip()
            safety_message = str(safety.get("message") or "").strip() or ("Lista" if safety_state == "usable" else "Cuenta bloqueada")
            blocked = bool(safety.get("blocked"))
            connected = bool(record.get("connected") or record.get("playwright_ok"))
            if safety_state == "needs_login":
                connected = False
            rows.append(
                {
                    "username": username,
                    "alias": self._account_alias(record.get("alias")),
                    "proxy": self._proxy_key(record),
                    "connected": connected,
                    "blocked": blocked,
                    "blocked_reason": safety_message if blocked else "",
                    "blocked_remaining_seconds": float(safety.get("remaining_seconds") or 0.0),
                    "safety_state": safety_state,
                    "safety_reason": str(safety.get("reason") or "").strip(),
                    "safety_message": safety_message,
                }
            )
        return rows

    def _autoresponder_targets(self, alias: str) -> list[str]:
        chooser = getattr(responder_module, "_choose_targets", None)
        if callable(chooser):
            try:
                values = chooser(str(alias or "").strip())
                return [
                    str(item or "").strip().lstrip("@")
                    for item in values or []
                    if str(item or "").strip()
                ]
            except Exception:
                return []
        clean_alias = str(alias or "").strip().lstrip("@")
        return [clean_alias] if clean_alias else []

    def _autoresponder_startability(self, targets: list[str]) -> dict[str, Any]:
        inspector = getattr(responder_module, "_inspect_startable_accounts", None)
        if not callable(inspector):
            return {"startable_accounts": list(targets), "skipped_accounts": []}
        try:
            payload = inspector(list(targets), log_skipped=False)
        except Exception:
            return {"startable_accounts": list(targets), "skipped_accounts": []}
        if not isinstance(payload, dict):
            return {"startable_accounts": list(targets), "skipped_accounts": []}
        return payload

    def _autoresponder_runtime_safety(self, reason: Any, *, remaining_seconds: float = 0.0) -> dict[str, Any]:
        descriptor = getattr(responder_module, "_autoresponder_safety_descriptor", None)
        if callable(descriptor):
            try:
                payload = descriptor(
                    source="runtime",
                    reason=str(reason or "").strip(),
                    remaining_seconds=float(remaining_seconds or 0.0),
                )
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                return dict(payload)
        blocked_reason = str(reason or "").strip() or "Cuenta en pausa"
        return {
            "blocked": True,
            "safety_state": "cooldown",
            "reason": blocked_reason,
            "message": blocked_reason,
            "remaining_seconds": max(0.0, float(remaining_seconds or 0.0)),
        }

    def _autoresponder_row_safety(self, row: dict[str, Any]) -> dict[str, Any]:
        blocked = bool(row.get("blocked"))
        safety_state = str(row.get("safety_state") or ("blocked" if blocked else "usable")).strip() or (
            "blocked" if blocked else "usable"
        )
        safety_message = str(
            row.get("safety_message") or row.get("blocked_reason") or ("Lista" if safety_state == "usable" else "Cuenta bloqueada")
        ).strip()
        return {
            "blocked": blocked,
            "safety_state": safety_state,
            "safety_reason": str(row.get("safety_reason") or row.get("reason") or "").strip(),
            "safety_message": safety_message,
            "blocked_reason": safety_message if blocked else "",
            "blocked_remaining_seconds": float(
                row.get("blocked_remaining_seconds") or row.get("remaining_seconds") or 0.0
            ),
        }

    def _autoresponder_state_account_rows(
        self,
        alias: str,
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        state = self.current_autoresponder_snapshot(alias=alias)
        rows_raw = state.get("account_rows")
        rows = list(rows_raw) if isinstance(rows_raw, list) else []
        ordered_accounts: list[str] = []
        indexed_rows: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            username = self._account_username(row.get("account") or row.get("username"))
            if not username:
                continue
            ordered_accounts.append(username)
            indexed_rows[username.lower()] = dict(row)
        return ordered_accounts, indexed_rows

    def resolve_followup_schedule_hours(self, alias: str) -> list[float]:
        flow = self.get_flow_config(alias)
        values: list[float] = []
        for stage in flow.get("stages") or []:
            if not isinstance(stage, dict):
                continue
            for followup in stage.get("followups") or []:
                if not isinstance(followup, dict):
                    continue
                try:
                    delay_hours = float(followup.get("delay_hours") or 0.0)
                except Exception:
                    continue
                if delay_hours > 0:
                    values.append(delay_hours)
        if values:
            return sorted(dict.fromkeys(values))
        fallback = getattr(responder_module, "_flow_followup_hours", None)
        if callable(fallback):
            try:
                return [float(item) for item in fallback(None)]
            except Exception:
                pass
        return [4.0, 24.0, 48.0]

    def resolve_followup_schedule_label(self, alias: str) -> str:
        values = self.resolve_followup_schedule_hours(alias)
        parts: list[str] = []
        for value in values:
            if float(value).is_integer():
                parts.append(str(int(value)))
            else:
                parts.append(str(round(float(value), 2)))
        return ", ".join(parts)

    def load_openai_api_key(self) -> str:
        env_values = read_env_local()
        return str(env_values.get("OPENAI_API_KEY") or "").strip()

    def save_openai_api_key(self, api_key: str) -> str:
        clean_value = str(api_key or "").strip()
        update_env_local({"OPENAI_API_KEY": clean_value})
        return clean_value

    def list_objection_prompts(self) -> list[dict[str, str]]:
        config = read_app_config()
        raw = config.get(_OBJECTION_PROMPTS_KEY)
        items = dict(raw) if isinstance(raw, dict) else {}
        rows: list[dict[str, str]] = []
        for name, content in items.items():
            clean_name = str(name or "").strip()
            if not clean_name:
                continue
            rows.append({"name": clean_name, "content": str(content or "").strip()})
        rows.sort(key=lambda item: str(item.get("name") or "").lower())
        return rows

    def save_objection_prompt(self, name: str, content: str) -> dict[str, str]:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ServiceError("El nombre del prompt es obligatorio.")
        prompts = {item["name"]: item["content"] for item in self.list_objection_prompts()}
        prompts[clean_name] = str(content or "").strip()
        update_app_config({_OBJECTION_PROMPTS_KEY: prompts})
        return {"name": clean_name, "content": prompts[clean_name]}

    def delete_objection_prompt(self, name: str) -> int:
        clean_name = str(name or "").strip()
        if not clean_name:
            return 0
        prompts = {item["name"]: item["content"] for item in self.list_objection_prompts()}
        if clean_name not in prompts:
            return 0
        prompts.pop(clean_name, None)
        update_app_config({_OBJECTION_PROMPTS_KEY: prompts})
        return 1

    def get_prompt_entry(self, alias: str) -> dict[str, Any]:
        return dict(responder_module._get_prompt_entry(str(alias or "").strip()))

    def save_prompt_entry(self, alias: str, updates: dict[str, Any]) -> dict[str, Any]:
        responder_module._set_prompt_entry(str(alias or "").strip(), dict(updates or {}))
        return self.get_prompt_entry(alias)

    def get_followup_entry(self, alias: str) -> dict[str, Any]:
        return dict(responder_module._get_followup_entry(str(alias or "").strip()))

    def save_followup_entry(self, alias: str, updates: dict[str, Any]) -> dict[str, Any]:
        responder_module._set_followup_entry(str(alias or "").strip(), dict(updates or {}))
        return self.get_followup_entry(alias)

    def _resolve_followup_accounts(
        self,
        *,
        mode: str,
        selected_aliases: list[str],
        selected_accounts: list[str],
    ) -> list[str]:
        clean_mode = str(mode or "").strip().lower()
        if clean_mode == "all":
            return [
                self._account_username(record.get("username"))
                for record in self._all_account_records()
                if self._account_username(record.get("username")) and bool(record.get("active", True))
            ]
        if clean_mode == "alias":
            selected_aliases_set = {
                str(item or "").strip().lower()
                for item in selected_aliases
                if str(item or "").strip()
            }
            return [
                self._account_username(record.get("username"))
                for record in self._all_account_records()
                if self._account_username(record.get("username"))
                and bool(record.get("active", True))
                and self._account_alias(record.get("alias")).lower() in selected_aliases_set
            ]
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in selected_accounts:
            username = self._account_username(raw)
            key = username.lower()
            if not username or key in seen:
                continue
            seen.add(key)
            normalized.append(username)
        return normalized

    def get_followup_account_selection(self, alias: str) -> dict[str, Any]:
        entry = self.get_followup_entry(alias)
        raw_mode = str(entry.get("selection_mode") or "").strip().lower()
        selected_aliases = [
            str(item or "").strip()
            for item in entry.get("selected_aliases") or []
            if str(item or "").strip()
        ]
        selected_accounts = [
            self._account_username(item)
            for item in entry.get("selected_accounts") or entry.get("accounts") or []
            if self._account_username(item)
        ]
        mode = raw_mode if raw_mode in {"all", "alias", "individual"} else "all"
        effective_accounts = self._resolve_followup_accounts(
            mode=mode,
            selected_aliases=selected_aliases,
            selected_accounts=selected_accounts,
        )
        return {
            "mode": mode,
            "selected_aliases": selected_aliases,
            "selected_accounts": selected_accounts,
            "effective_accounts": effective_accounts,
            "enabled": bool(entry.get("enabled", True)),
        }

    def save_followup_account_selection(
        self,
        alias: str,
        *,
        mode: str,
        selected_aliases: list[str] | None = None,
        selected_accounts: list[str] | None = None,
    ) -> dict[str, Any]:
        clean_mode = str(mode or "").strip().lower()
        if clean_mode not in {"all", "alias", "individual"}:
            raise ServiceError("Modo de cuentas para follow-up invalido.")
        aliases = [
            str(item or "").strip()
            for item in (selected_aliases or [])
            if str(item or "").strip()
        ]
        accounts = [
            self._account_username(item)
            for item in (selected_accounts or [])
            if self._account_username(item)
        ]
        effective_accounts = self._resolve_followup_accounts(
            mode=clean_mode,
            selected_aliases=aliases,
            selected_accounts=accounts,
        )
        saved = self.save_followup_entry(
            alias,
            {
                "enabled": True,
                "selection_mode": clean_mode,
                "selected_aliases": aliases,
                "selected_accounts": accounts,
                "accounts": effective_accounts,
            },
        )
        return {
            **saved,
            "selection_mode": clean_mode,
            "selected_aliases": aliases,
            "selected_accounts": accounts,
            "effective_accounts": effective_accounts,
        }

    def list_packs(self) -> list[dict[str, Any]]:
        return [dict(item) for item in responder_module._list_packs()]

    def upsert_pack(self, pack: dict[str, Any]) -> dict[str, Any]:
        payload = dict(pack or {})
        actions = payload.get("actions")
        if isinstance(actions, list):
            payload["actions"] = list(actions[:3])
        return dict(responder_module._upsert_pack(payload))

    def delete_pack(self, pack_id: str) -> int:
        clean_id = str(pack_id or "").strip()
        if not clean_id:
            return 0
        packs = [
            item
            for item in responder_module._list_packs()
            if str(item.get("id") or "").strip() != clean_id
        ]
        responder_module._save_packs(packs)
        return 1

    def alias_state_paths(self) -> list[Path]:
        prompt_file = getattr(responder_module, "_PROMPTS_FILE", self.context.storage_path("autoresponder_prompts.json"))
        followup_file = getattr(responder_module, "_FOLLOWUP_FILE", self.context.storage_path("followups.json"))
        return [Path(prompt_file), Path(followup_file)]

    def refresh_alias_state_cache(self) -> None:
        prompt_reader = getattr(responder_module, "_read_prompts_state", None)
        if callable(prompt_reader):
            prompt_reader(refresh=True)
        followup_reader = getattr(responder_module, "_read_followup_state", None)
        if callable(followup_reader):
            followup_reader(refresh=True)

    def alias_reference_snapshot(self) -> dict[str, Any]:
        prompt_reader = getattr(responder_module, "_read_prompts_state", None)
        followup_reader = getattr(responder_module, "_read_followup_state", None)
        prompt_state = dict(prompt_reader(refresh=True) or {}) if callable(prompt_reader) else {}
        followup_state = dict(followup_reader(refresh=True) or {}) if callable(followup_reader) else {}
        prompt_aliases = [
            str((entry.get("alias") if isinstance(entry, dict) else "") or key).strip()
            for key, entry in dict(prompt_state.get("aliases") or {}).items()
            if str((entry.get("alias") if isinstance(entry, dict) else "") or key).strip()
        ]
        followup_aliases = [
            str((entry.get("alias") if isinstance(entry, dict) else "") or key).strip()
            for key, entry in dict(followup_state.get("aliases") or {}).items()
            if str((entry.get("alias") if isinstance(entry, dict) else "") or key).strip()
        ]
        followup_selected_aliases: list[dict[str, Any]] = []
        for scope_name in ("aliases", "accounts"):
            container = dict(followup_state.get(scope_name) or {})
            for key, raw_entry in container.items():
                if not isinstance(raw_entry, dict):
                    continue
                selected = [
                    str(item or "").strip()
                    for item in raw_entry.get("selected_aliases") or []
                    if str(item or "").strip()
                ]
                if not selected:
                    continue
                owner_label = str(raw_entry.get("alias") or key).strip() or str(key or "").strip()
                followup_selected_aliases.append(
                    {
                        "owner_scope": scope_name,
                        "owner_key": str(key or "").strip(),
                        "owner_label": owner_label,
                        "selected_aliases": selected,
                    }
                )
        return {
            "prompt_aliases": prompt_aliases,
            "followup_aliases": followup_aliases,
            "followup_selected_aliases": followup_selected_aliases,
        }

    def _alias_key(self, alias: str | None) -> str:
        resolver = getattr(responder_module, "_normalize_alias_key", None)
        if callable(resolver):
            return str(resolver(str(alias or "")) or "").strip()
        return str(alias or "").strip().lower()

    def _rewrite_alias_labels(
        self,
        values: list[Any] | tuple[Any, ...] | set[Any],
        *,
        source_alias: str,
        target_alias: str | None = None,
    ) -> list[str]:
        source_key = self._alias_key(source_alias)
        target_value = str(target_alias or "").strip()
        rewritten: list[str] = []
        seen: set[str] = set()
        for raw in values:
            clean_value = str(raw or "").strip()
            if not clean_value:
                continue
            if self._alias_key(clean_value) == source_key:
                clean_value = target_value
            if not clean_value:
                continue
            clean_key = self._alias_key(clean_value)
            if not clean_key or clean_key in seen:
                continue
            seen.add(clean_key)
            rewritten.append(clean_value)
        return rewritten

    def _rewrite_prompt_alias_state(self, source_alias: str, target_alias: str | None = None) -> dict[str, int]:
        reader = getattr(responder_module, "_read_prompts_state", None)
        writer = getattr(responder_module, "_write_prompts_state", None)
        if not callable(reader) or not callable(writer):
            return {"prompt_entries_updated": 0, "prompt_entries_deleted": 0}

        source = str(source_alias or "").strip()
        target = str(target_alias or "").strip()
        source_key = self._alias_key(source)
        target_key = self._alias_key(target) if target else ""
        state = dict(reader(refresh=True) or {})
        aliases_raw = state.get("aliases")
        aliases = dict(aliases_raw) if isinstance(aliases_raw, dict) else {}
        source_entry = aliases.get(source_key)
        if not isinstance(source_entry, dict):
            return {"prompt_entries_updated": 0, "prompt_entries_deleted": 0}
        if target and source_key != target_key and isinstance(aliases.get(target_key), dict):
            raise ServiceError("El alias destino ya tiene configuracion de prompt.")

        updated_entry = dict(source_entry)
        if target:
            updated_entry["alias"] = target
            if source_key != target_key:
                aliases.pop(source_key, None)
            aliases[target_key or source_key] = updated_entry
            state["aliases"] = aliases
            writer(state)
            return {"prompt_entries_updated": 1, "prompt_entries_deleted": 0}

        aliases.pop(source_key, None)
        state["aliases"] = aliases
        writer(state)
        return {"prompt_entries_updated": 0, "prompt_entries_deleted": 1}

    def _rewrite_followup_alias_state(self, source_alias: str, target_alias: str | None = None) -> dict[str, int]:
        reader = getattr(responder_module, "_read_followup_state", None)
        writer = getattr(responder_module, "_write_followup_state", None)
        if not callable(reader) or not callable(writer):
            return {
                "followup_entries_updated": 0,
                "followup_entries_deleted": 0,
                "followup_alias_refs_updated": 0,
            }

        source = str(source_alias or "").strip()
        target = str(target_alias or "").strip()
        source_key = self._alias_key(source)
        target_key = self._alias_key(target) if target else ""
        state = dict(reader(refresh=True) or {})
        aliases_raw = state.get("aliases")
        aliases = dict(aliases_raw) if isinstance(aliases_raw, dict) else {}
        source_entry = aliases.get(source_key)
        target_entry = aliases.get(target_key) if target else None
        if target and source_key != target_key and isinstance(source_entry, dict) and isinstance(target_entry, dict):
            raise ServiceError("El alias destino ya tiene configuracion de follow-up.")

        references_updated = 0
        for container_name in ("aliases", "accounts"):
            container_raw = state.get(container_name)
            container = dict(container_raw) if isinstance(container_raw, dict) else {}
            for key, raw_entry in list(container.items()):
                if not isinstance(raw_entry, dict):
                    continue
                entry = dict(raw_entry)
                selected_aliases_raw = entry.get("selected_aliases") or []
                if isinstance(selected_aliases_raw, (list, tuple, set)):
                    rewritten = self._rewrite_alias_labels(
                        list(selected_aliases_raw),
                        source_alias=source,
                        target_alias=target or None,
                    )
                    original = [str(item or "").strip() for item in selected_aliases_raw if str(item or "").strip()]
                    if rewritten != original:
                        entry["selected_aliases"] = rewritten
                        container[key] = entry
                        references_updated += 1
            state[container_name] = container
        aliases = dict(state.get("aliases") or {})

        if isinstance(source_entry, dict):
            updated_entry = dict(aliases.get(source_key) or source_entry)
            updated_entry["alias"] = target if target else str(updated_entry.get("alias") or source).strip() or source
            selected_aliases_raw = updated_entry.get("selected_aliases") or []
            if isinstance(selected_aliases_raw, (list, tuple, set)):
                updated_entry["selected_aliases"] = self._rewrite_alias_labels(
                    list(selected_aliases_raw),
                    source_alias=source,
                    target_alias=target or None,
                )
            if target:
                if source_key != target_key:
                    aliases.pop(source_key, None)
                aliases[target_key or source_key] = updated_entry
                state["aliases"] = aliases
                writer(state)
                return {
                    "followup_entries_updated": 1,
                    "followup_entries_deleted": 0,
                    "followup_alias_refs_updated": references_updated,
                }
            aliases.pop(source_key, None)
            state["aliases"] = aliases
            writer(state)
            return {
                "followup_entries_updated": 0,
                "followup_entries_deleted": 1,
                "followup_alias_refs_updated": references_updated,
            }

        state["aliases"] = aliases
        writer(state)
        return {
            "followup_entries_updated": 0,
            "followup_entries_deleted": 0,
            "followup_alias_refs_updated": references_updated,
        }

    def _rewrite_autoresponder_runtime_alias(self, source_alias: str, target_alias: str | None = None) -> dict[str, int]:
        source_key = self._alias_key(source_alias)
        target = str(target_alias or "").strip()
        with self._autoresponder_lock:
            if not self._autoresponder_state:
                return {"runtime_alias_updated": 0}
            snapshot = self._copy_autoresponder_state(self._autoresponder_state)
            changed = 0
            current_alias = str(snapshot.get("alias") or "").strip()
            if current_alias and self._alias_key(current_alias) == source_key:
                snapshot["alias"] = target
                changed = 1
            account_rows = snapshot.get("account_rows")
            if isinstance(account_rows, list):
                rewritten_rows = []
                for row in account_rows:
                    if not isinstance(row, dict):
                        rewritten_rows.append(row)
                        continue
                    payload = dict(row)
                    row_alias = str(payload.get("alias") or "").strip()
                    if row_alias and self._alias_key(row_alias) == source_key:
                        payload["alias"] = target
                        changed = 1
                    rewritten_rows.append(payload)
                snapshot["account_rows"] = rewritten_rows
            if changed:
                self._autoresponder_state = snapshot
            return {"runtime_alias_updated": changed}

    def rename_alias_state(self, source_alias: str, target_alias: str) -> dict[str, int]:
        source = str(source_alias or "").strip()
        target = str(target_alias or "").strip()
        if not source or not target:
            raise ServiceError("Alias invalido para automation.")
        prompt_stats = self._rewrite_prompt_alias_state(source, target)
        followup_stats = self._rewrite_followup_alias_state(source, target)
        runtime_stats = self._rewrite_autoresponder_runtime_alias(source, target)
        self.refresh_alias_state_cache()
        return {
            **prompt_stats,
            **followup_stats,
            **runtime_stats,
        }

    def delete_alias_state(self, alias: str, *, move_to: str | None = None) -> dict[str, int]:
        source = str(alias or "").strip()
        target = str(move_to or "").strip()
        if not source:
            raise ServiceError("Alias invalido para automation.")
        prompt_stats = self._rewrite_prompt_alias_state(source, target or None)
        followup_stats = self._rewrite_followup_alias_state(source, target or None)
        runtime_stats = self._rewrite_autoresponder_runtime_alias(source, target or None)
        self.refresh_alias_state_cache()
        return {
            **prompt_stats,
            **followup_stats,
            **runtime_stats,
        }

    def get_flow_config(self, alias: str) -> dict[str, Any]:
        entry = self.get_prompt_entry(alias)
        raw_flow = entry.get("flow_config") or {}
        return dict(responder_module._normalize_flow_config(raw_flow))

    def save_flow_config(self, alias: str, flow_config: dict[str, Any]) -> dict[str, Any]:
        normalized = responder_module._normalize_flow_config(dict(flow_config or {}))
        responder_module._set_prompt_entry(
            str(alias or "").strip(),
            {"flow_config": normalized},
        )
        return normalized

    def start_autoresponder(self, config: dict[str, Any]) -> dict[str, Any]:
        alias = str(config.get("alias") or "").strip()
        if not alias:
            raise ServiceError("Alias invalido para autoresponder.")
        snapshot = self._autoresponder_wrapper_snapshot(alias)
        self._update_autoresponder_state(snapshot, replace=True)
        return snapshot

    def stop_autoresponder(self, reason: str = "autoresponder stopped from GUI") -> None:
        current = self.current_autoresponder_snapshot()
        alias = str((current or {}).get("alias") or "").strip()
        if not alias:
            return
        snapshot = self._autoresponder_wrapper_snapshot(alias)
        if snapshot and not bool(snapshot.get("task_active")):
            snapshot = dict(snapshot)
            snapshot["message"] = self._autoresponder_inbox_only_message()
        self._update_autoresponder_state(snapshot, replace=True)

    def _runtime_metric(self, payload: dict[str, Any], *keys: str) -> int:
        for key in keys:
            try:
                value = int(payload.get(key) or 0)
            except Exception:
                value = 0
            if value:
                return value
        return 0

    def autoresponder_snapshot(self, alias: str) -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            return {}
        bridge = self._inbox_runtime_bridge()
        if bridge is not None:
            snapshot = self._runtime_bridge_snapshot(clean_alias)
            state = self.current_autoresponder_snapshot(alias=clean_alias)
            if state:
                merged = dict(state)
                merged.update(snapshot)
                return merged
            return snapshot
        getter = getattr(responder_module, "_get_autoresponder_runtime_controller", None)
        runtime = None
        if callable(getter):
            try:
                runtime = getter()
            except Exception:
                runtime = None
        account_rows: list[dict[str, Any]] = []
        requests_last_minute = 0.0
        pending_hydration = 0.0
        risk_score_total = 0.0
        hydration_attempts = 0.0
        hydration_success = 0.0
        hydration_complete = 0.0
        rate_signals = 0.0
        accounts_blocked = 0
        message_success = 0
        message_failed = 0
        followup_success = 0
        followup_failed = 0
        agendas_generated = 0
        state = self.current_autoresponder_snapshot(alias=clean_alias)
        targets, state_rows_by_username = self._autoresponder_state_account_rows(clean_alias)
        if not targets:
            targets = self._autoresponder_targets(clean_alias)
        else:
            seen_targets = {account.lower() for account in targets}
            for account in self._autoresponder_targets(clean_alias):
                normalized = account.lower()
                if normalized in seen_targets:
                    continue
                seen_targets.add(normalized)
                targets.append(account)
        alias_rows_by_username = {
            row["username"].lower(): row
            for row in self.alias_account_rows(clean_alias)
            if str(row.get("username") or "").strip()
        }
        for account in targets:
            payload = {}
            if runtime is not None:
                try:
                    payload = runtime.snapshot(account)
                except Exception:
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            requests_value = float(payload.get("requests_last_minute") or 0.0)
            pending_value = float(payload.get("pending_hydration") or 0.0)
            risk_value = float(payload.get("risk_score") or 0.0)
            attempts_value = float(payload.get("hydration_attempts") or 0.0)
            success_value = float(payload.get("hydration_success") or 0.0)
            complete_value = float(payload.get("hydration_complete") or 0.0)
            rate_value = float(payload.get("rate_signals") or 0.0)
            requests_last_minute += requests_value
            pending_hydration += pending_value
            risk_score_total += risk_value
            hydration_attempts += attempts_value
            hydration_success += success_value
            hydration_complete += complete_value
            rate_signals += rate_value
            message_success += self._runtime_metric(payload, "responses_success", "message_success", "messages_responded")
            message_failed += self._runtime_metric(payload, "responses_failed", "message_failed", "messages_failed")
            followup_success += self._runtime_metric(payload, "followups_success", "followup_success")
            followup_failed += self._runtime_metric(payload, "followups_failed", "followup_failed")
            agendas_generated += self._runtime_metric(payload, "agendas_generated", "agenda_success")
            state_row = state_rows_by_username.get(account.lower(), {})
            account_row = alias_rows_by_username.get(account.lower(), {})
            blocked = bool(payload.get("account_blocked"))
            blocked_reason = str(payload.get("account_blocked_reason") or "").strip()
            blocked_remaining_seconds = float(payload.get("account_blocked_remaining_seconds") or 0.0)
            state_safety = self._autoresponder_row_safety(state_row) if isinstance(state_row, dict) else {}
            account_safety = self._autoresponder_row_safety(account_row) if isinstance(account_row, dict) else {}
            runtime_safety = (
                self._autoresponder_runtime_safety(
                    blocked_reason,
                    remaining_seconds=blocked_remaining_seconds,
                )
                if blocked
                else {}
            )
            if blocked:
                blocked_reason = str(runtime_safety.get("message") or blocked_reason).strip()
            if not blocked and bool(state_safety.get("blocked")):
                blocked = True
                blocked_reason = str(state_safety.get("blocked_reason") or "").strip()
                blocked_remaining_seconds = float(state_safety.get("blocked_remaining_seconds") or 0.0)
                runtime_safety = dict(state_safety)
            if not blocked and bool(account_safety.get("blocked")):
                blocked = True
                blocked_reason = str(account_safety.get("blocked_reason") or "").strip()
                blocked_remaining_seconds = float(account_safety.get("blocked_remaining_seconds") or 0.0)
                runtime_safety = dict(account_safety)
            if blocked:
                accounts_blocked += 1
            merged_safety = runtime_safety if blocked else (state_safety or account_safety)
            safety_state = str(merged_safety.get("safety_state") or ("blocked" if blocked else "usable")).strip() or (
                "blocked" if blocked else "usable"
            )
            safety_message = str(
                merged_safety.get("safety_message")
                or merged_safety.get("message")
                or ("Activa" if safety_state == "usable" else "Cuenta bloqueada")
            ).strip()
            account_rows.append(
                {
                    "account": account,
                    "proxy": str(account_row.get("proxy") or state_row.get("proxy") or ""),
                    "requests_last_minute": requests_value,
                    "pending_hydration": pending_value,
                    "risk_score": risk_value,
                    "blocked": blocked,
                    "blocked_reason": blocked_reason or (safety_message if blocked else ""),
                    "blocked_remaining_seconds": blocked_remaining_seconds,
                    "safety_state": safety_state,
                    "safety_reason": str(
                        merged_safety.get("safety_reason") or merged_safety.get("reason") or blocked_reason
                    ).strip(),
                    "safety_message": safety_message,
                }
            )

        accounts_total = len(targets)
        risk_score_avg = (risk_score_total / accounts_total) if accounts_total else 0.0
        followup_label = str(state.get("followup_schedule_label") or "").strip() or self.resolve_followup_schedule_label(clean_alias)
        return {
            "run_id": str(state.get("run_id") or ""),
            "alias": clean_alias,
            "status": str(state.get("status") or ("Running" if bool(state.get("task_active")) else "Idle")),
            "message": str(state.get("message") or ""),
            "started_at": str(state.get("started_at") or ""),
            "finished_at": str(state.get("finished_at") or ""),
            "delay_min": int(state.get("delay_min") or 45),
            "delay_max": int(state.get("delay_max") or 76),
            "concurrency": int(state.get("concurrency") or 1),
            "threads": int(state.get("threads") or 20),
            "followup_only": bool(state.get("followup_only")),
            "followup_schedule_label": followup_label,
            "accounts_total": accounts_total,
            "accounts_active": max(0, accounts_total - accounts_blocked),
            "accounts_blocked": accounts_blocked,
            "requests_last_minute": requests_last_minute,
            "pending_hydration": pending_hydration,
            "risk_score_avg": risk_score_avg,
            "hydration_attempts": hydration_attempts,
            "hydration_success": hydration_success,
            "hydration_complete": hydration_complete,
            "rate_signals": rate_signals,
            "message_success": max(message_success, int(state.get("message_success") or 0)),
            "message_failed": max(message_failed, int(state.get("message_failed") or 0)),
            "followup_success": max(followup_success, int(state.get("followup_success") or 0)),
            "followup_failed": max(followup_failed, int(state.get("followup_failed") or 0)),
            "agendas_generated": max(agendas_generated, int(state.get("agendas_generated") or 0)),
            "account_rows": account_rows,
            "task_active": bool(state.get("task_active")),
        }

    def whatsapp_store(self) -> WhatsAppDataStore:
        return WhatsAppDataStore()

    def list_whatsapp_templates(self) -> list[dict[str, Any]]:
        store = self.whatsapp_store()
        raw = store.state.get("templates")
        items = dict(raw) if isinstance(raw, dict) else {}
        rows: list[dict[str, Any]] = []
        for template_id, value in items.items():
            payload = dict(value) if isinstance(value, dict) else {}
            clean_id = str(payload.get("id") or template_id or "").strip()
            if not clean_id:
                continue
            rows.append(
                {
                    "id": clean_id,
                    "name": str(payload.get("name") or clean_id).strip(),
                    "content": str(payload.get("content") or "").strip(),
                }
            )
        rows.sort(key=lambda item: str(item.get("name") or "").lower())
        return rows

    def save_whatsapp_template(self, template_id: str, name: str, content: str) -> dict[str, Any]:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ServiceError("El nombre de la plantilla es obligatorio.")
        store = self.whatsapp_store()
        clean_id = str(template_id or "").strip() or clean_name.lower().replace(" ", "_")
        store.state.setdefault("templates", {})[clean_id] = {
            "id": clean_id,
            "name": clean_name,
            "content": str(content or "").strip(),
        }
        store.save()
        return dict(store.state["templates"][clean_id])

    def delete_whatsapp_template(self, template_id: str) -> int:
        clean_id = str(template_id or "").strip()
        if not clean_id:
            return 0
        store = self.whatsapp_store()
        templates = store.state.setdefault("templates", {})
        if clean_id not in templates:
            return 0
        templates.pop(clean_id, None)
        store.save()
        return 1

    def save_whatsapp_contact_list(
        self,
        alias: str,
        contacts: list[dict[str, str]],
        *,
        notes: str = "",
    ) -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            raise ServiceError("El nombre de la lista es obligatorio.")
        normalized_contacts: list[dict[str, str]] = []
        for item in contacts:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("nombre") or "").strip()
            number = str(item.get("number") or item.get("telefono") or item.get("phone") or "").strip()
            if not number:
                continue
            normalized_contacts.append({"name": name or number, "number": number})
        store = self.whatsapp_store()
        store.state.setdefault("contact_lists", {})[clean_alias] = {
            "alias": clean_alias,
            "notes": str(notes or "").strip(),
            "contacts": normalized_contacts,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        store.save()
        return {"alias": clean_alias, "contacts": normalized_contacts}

    def delete_whatsapp_contact_list(self, alias: str) -> int:
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            return 0
        store = self.whatsapp_store()
        lists = store.state.setdefault("contact_lists", {})
        if clean_alias not in lists:
            return 0
        lists.pop(clean_alias, None)
        store.save()
        return 1

    def connect_whatsapp_number(
        self,
        *,
        alias: str,
        phone: str,
        keep_alive: bool = True,
        background_mode: bool = True,
        backend: str = "playwright",
    ) -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        clean_phone = str(phone or "").strip()
        if not clean_alias:
            raise ServiceError("El alias del numero es obligatorio.")
        number_id = clean_alias.lower().replace(" ", "_") or str(uuid.uuid4())
        session_dir = whatsapp_module._playwright_session_dir_for_number(number_id)
        success, snapshot, details = whatsapp_module._initiate_whatsapp_web_login(session_dir, backend)
        if not success:
            raise ServiceError(str(details or "No se pudo vincular el numero de WhatsApp."))
        store = self.whatsapp_store()
        store.state.setdefault("numbers", {})[number_id] = {
            "id": number_id,
            "alias": clean_alias,
            "phone": clean_phone,
            "connected": True,
            "last_connected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "session_notes": [],
            "keep_alive": bool(keep_alive),
            "session_path": str(session_dir),
            "qr_snapshot": str(snapshot) if snapshot else "",
            "last_qr_capture_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "connection_state": "conectado",
            "connection_method": "playwright",
            "background_mode": bool(background_mode),
        }
        store.save()
        return dict(store.state["numbers"][number_id])

    def schedule_whatsapp_message_run(
        self,
        *,
        list_alias: str,
        template_id: str,
        number_id: str,
        delay_min: float,
        delay_max: float,
    ) -> dict[str, Any]:
        clean_alias = str(list_alias or "").strip()
        clean_template_id = str(template_id or "").strip()
        if not clean_alias or not clean_template_id:
            raise ServiceError("Selecciona una lista y una plantilla.")
        store = self.whatsapp_store()
        contact_list = store.find_list(clean_alias) or {}
        contacts = list(contact_list.get("contacts") or [])
        if not contacts:
            raise ServiceError("La lista seleccionada no tiene contactos.")
        template = None
        for item in self.list_whatsapp_templates():
            if str(item.get("id") or "").strip() == clean_template_id:
                template = item
                break
        if not template:
            raise ServiceError("La plantilla seleccionada no existe.")
        min_value = max(1.0, float(delay_min or 1.0))
        max_value = max(min_value, float(delay_max or min_value))
        run_id = datetime.utcnow().strftime("wa-run-%Y%m%d%H%M%S%f")
        created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        events: list[dict[str, Any]] = []
        for contact in contacts:
            rendered = whatsapp_module._render_message(str(template.get("content") or ""), dict(contact))
            events.append(
                {
                    "contact": str(contact.get("number") or ""),
                    "name": str(contact.get("name") or ""),
                    "message": rendered,
                    "scheduled_at": created_at,
                    "status": "pendiente",
                    "delivered_at": None,
                    "notes": "",
                    "confirmation": "no_enviado",
                    "validation_status": str(((contact.get("validation") or {}) if isinstance(contact, dict) else {}).get("status") or ""),
                    "error_code": "",
                }
            )
        run = {
            "id": run_id,
            "list_alias": clean_alias,
            "number_id": str(number_id or "").strip(),
            "status": "running",
            "created_at": created_at,
            "updated_at": created_at,
            "next_run_at": created_at,
            "delay": {"min": min_value, "max": max_value},
            "message_template": str(template.get("content") or ""),
            "message_preview": textwrap.shorten(str(template.get("content") or ""), width=90, placeholder="..."),
            "events": events,
            "max_contacts": len(events),
            "total_contacts": len(events),
            "sent_contacts": 0,
            "failed_contacts": 0,
            "cancelled_contacts": 0,
            "completion_notified": False,
            "last_session_at": None,
            "log": [
                {
                    "timestamp": created_at,
                    "message": f"Se programo el envio para {len(events)} contactos.",
                }
            ],
        }
        store.state.setdefault("message_runs", []).append(run)
        store.save()
        return run

    def get_whatsapp_autoresponder_config(self) -> dict[str, Any]:
        store = self.whatsapp_store()
        configs = store.state.get("ai_automations")
        payload = dict(configs.get("default")) if isinstance(configs, dict) and isinstance(configs.get("default"), dict) else {}
        mode = str(payload.get("mode") or "ia").strip().lower()
        return {
            "mode": mode if mode in {"ia", "fijo"} else "ia",
            "prompt": str(payload.get("prompt") or "").strip(),
            "fixed_message": str(payload.get("fixed_message") or "").strip(),
            "enabled": bool(payload.get("enabled")),
        }

    def save_whatsapp_autoresponder_config(
        self,
        *,
        mode: str,
        prompt: str,
        fixed_message: str,
        enabled: bool,
    ) -> dict[str, Any]:
        clean_mode = str(mode or "").strip().lower()
        if clean_mode not in {"ia", "fijo"}:
            raise ServiceError("Modo de autoresponder de WhatsApp invalido.")
        store = self.whatsapp_store()
        store.state.setdefault("ai_automations", {})["default"] = {
            "mode": clean_mode,
            "prompt": str(prompt or "").strip(),
            "fixed_message": str(fixed_message or "").strip(),
            "enabled": bool(enabled),
        }
        store.save()
        return self.get_whatsapp_autoresponder_config()

    def whatsapp_snapshot(self) -> dict[str, Any]:
        store = self.whatsapp_store()
        numbers = list(store.iter_numbers())
        lists = list(store.iter_lists())
        runs = list(store.state.get("message_runs", []))
        templates = self.list_whatsapp_templates()
        connected = sum(1 for item in numbers if bool(item.get("connected")))
        active_runs = sum(
            1
            for run in runs
            if str(run.get("status") or "").strip().lower()
            in {"running", "active", "paused"}
        )
        return {
            "numbers_total": len(numbers),
            "numbers_connected": connected,
            "lists_total": len(lists),
            "templates_total": len(templates),
            "runs_total": len(runs),
            "runs_active": active_runs,
            "numbers": numbers,
            "lists": [{"alias": alias, **payload} for alias, payload in lists],
            "templates": templates,
            "runs": [item for item in runs if isinstance(item, dict)],
            "autoresponder": self.get_whatsapp_autoresponder_config(),
        }
