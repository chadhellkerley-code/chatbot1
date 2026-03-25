from __future__ import annotations

import csv
import importlib
import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import health_store
from core.alias_identity import (
    DEFAULT_ALIAS_ID,
    AliasRecord,
    AliasValidationError,
    alias_record_from_input,
    alias_record_from_payload,
    default_alias_record,
    normalize_alias_display,
    normalize_alias_id,
)
from core import accounts as accounts_module
from core.accounts_helpers.csv_utils import (
    _compose_proxy_url,
    _extract_totp_entries_from_csv,
    _parse_accounts_csv,
    extract_totp_secret_from_row,
)
from core.proxy_preflight import account_proxy_preflight, preflight_accounts_for_proxy_runtime
from core.proxy_registry import (
    ProxyValidationError,
    clear_proxy_quarantine,
    delete_proxy_record,
    get_proxy_by_id,
    load_proxies,
    load_proxy_audit_entries,
    proxy_audit_path,
    proxy_health_label as build_proxy_health_label,
    proxy_reference_status,
    record_proxy_audit_event,
    record_proxy_failure,
    record_proxy_success,
    record_proxy_test_failure,
    record_proxy_test_success,
    save_proxy_records as save_registry_proxy_records,
    set_proxy_active,
    upsert_proxy_record,
    upsert_proxy_records,
)
from core.totp_store import save_secret as save_totp_secret
from proxy_manager import ProxyConfig, test_proxy_connection

from paths import runtime_base

from core.accounts import normalize_alias as normalize_alias_key

from .base import ServiceContext, ServiceError, dedupe_usernames, normalize_alias


logger = logging.getLogger(__name__)
_ALIAS_REGISTRY_SCHEMA_VERSION = 2
_IG_EDIT_PROFILE_URL = "https://www.instagram.com/accounts/edit/"


def assign_proxy_to_account(username: str, proxy_id: str | None) -> bool:
    from core.accounts import update_account

    return bool(
        update_account(
            username,
            {
                "assigned_proxy_id": proxy_id,
                "proxy_url": None,
                "proxy_user": None,
                "proxy_pass": None,
            },
        )
    )


def _health_refresh_proxy(record: dict[str, Any]) -> dict[str, str] | None:
    try:
        from src.proxy_payload import proxy_from_account
    except Exception:
        return None
    try:
        return proxy_from_account(record)
    except Exception:
        logger.debug(
            "No se pudo resolver el proxy de health refresh para @%s.",
            str(record.get("username") or "").strip().lstrip("@"),
            exc_info=True,
        )
        return None


def _check_connected_account_health(record: dict[str, Any]) -> tuple[bool, str]:
    from src.auth.persistent_login import check_session

    username = str(record.get("username") or "").strip().lstrip("@")
    if not username:
        raise ValueError("username requerido para refrescar health.")
    return check_session(
        username,
        proxy=_health_refresh_proxy(record),
        headless=True,
    )


class AccountService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    def is_ready(self) -> bool:
        try:
            expected = runtime_base(Path(accounts_module.__file__).resolve().parent.parent).resolve()
            current = Path(getattr(accounts_module, "BASE")).resolve()
        except Exception:
            return True
        return current == expected

    def ensure_ready_from_env(self) -> bool:
        if self.is_ready():
            return True
        try:
            importlib.reload(accounts_module)
        except Exception:
            logger.exception("No se pudo recargar core.accounts luego del bootstrap.")
            return False
        return self.is_ready()

    def login_queue_concurrency(self) -> int:
        provider = getattr(accounts_module, "playwright_login_queue_concurrency", None)
        if callable(provider):
            try:
                return max(1, int(provider()))
            except Exception:
                return 1
        return 1

    @staticmethod
    def _login_progress_label(state: str, message: str = "") -> str:
        normalized = str(state or "").strip().lower()
        if normalized == health_store.LOGIN_PROGRESS_QUEUED:
            return "En cola"
        if normalized == health_store.LOGIN_PROGRESS_OPENING_BROWSER:
            return "Abriendo navegador"
        if normalized == health_store.LOGIN_PROGRESS_RUNNING_LOGIN:
            return "Ejecutando login"
        if normalized == health_store.LOGIN_PROGRESS_CONFIRMING_FEED:
            return "Confirmando feed"
        if normalized == health_store.LOGIN_PROGRESS_CONFIRMING_INBOX:
            return "Confirmando inbox"
        return str(message or "").strip()

    @staticmethod
    def _invalid_login_result(username: str, message: str) -> dict[str, Any]:
        return {
            "username": str(username or "").strip().lstrip("@"),
            "status": "failed",
            "message": str(message or "").strip() or "login_input_invalid",
            "profile_path": "",
            "row_number": None,
        }

    @staticmethod
    def _account_password_value(record: dict[str, Any]) -> str:
        account_password = getattr(accounts_module, "_account_password", None)
        if callable(account_password):
            return str(account_password(record) or "").strip()
        return str(record.get("password") or "").strip()

    def _report_login_failures(
        self,
        *,
        alias: str,
        results: list[dict[str, Any]],
        source: str,
    ) -> None:
        failed_rows = [
            dict(row)
            for row in results
            if isinstance(row, dict) and str(row.get("status") or "").strip().lower() not in {"", "ok", "success"}
        ]
        if not failed_rows:
            return
        try:
            from src.telemetry import report_login_failed
        except Exception:
            return
        for row in failed_rows:
            username = str(row.get("username") or "").strip().lstrip("@")
            message = str(row.get("message") or "login_failed").strip()
            report_login_failed(
                f"Login fallido para @{username or '-'}: {message}",
                payload={
                    "alias": alias,
                    "source": source,
                    "username": username,
                    "status": row.get("status"),
                    "message": message,
                },
            )

    def _alias_registry_path(self) -> Path:
        return self.context.accounts_path("aliases.json")

    def _proxy_path(self) -> Path:
        return self.context.accounts_path("proxies.json")

    def alias_state_paths(self) -> list[Path]:
        accounts_path = getattr(accounts_module, "FILE", self.context.accounts_path("accounts.json"))
        return [Path(accounts_path), self._alias_registry_path()]

    def _raw_account_records(self) -> list[dict[str, Any]]:
        return [dict(item) for item in accounts_module.list_all() if isinstance(item, dict)]

    def _alias_registry(self, *, raw_accounts: list[dict[str, Any]] | None = None) -> dict[str, AliasRecord]:
        payload = self.context.read_json(
            self._alias_registry_path(),
            {"schema_version": _ALIAS_REGISTRY_SCHEMA_VERSION, "aliases": []},
        )
        raw_aliases: list[Any] = []
        if isinstance(payload, dict):
            raw_aliases = payload.get("aliases") if isinstance(payload.get("aliases"), list) else []
        elif isinstance(payload, list):
            raw_aliases = payload

        records: dict[str, AliasRecord] = {}
        for raw_entry in raw_aliases:
            try:
                record = alias_record_from_payload(raw_entry)
            except AliasValidationError:
                continue
            records.setdefault(record.alias_id, record)

        for item in raw_accounts or self._raw_account_records():
            raw_alias = normalize_alias_display(
                item.get("alias_display_name") or item.get("alias"),
                default=DEFAULT_ALIAS_ID,
            )
            try:
                record = alias_record_from_input(raw_alias, system=True)
            except AliasValidationError:
                continue
            records.setdefault(record.alias_id, record)

        records.setdefault(DEFAULT_ALIAS_ID, default_alias_record())
        return records

    def _decorate_account_record(
        self,
        record: dict[str, Any],
        *,
        alias_records: dict[str, AliasRecord],
    ) -> dict[str, Any]:
        row = dict(record)
        alias_id = normalize_alias_id(row.get("alias_id") or row.get("alias"), default=DEFAULT_ALIAS_ID)
        alias_entry = alias_records.get(alias_id)
        fallback_display_name = normalize_alias_display(
            row.get("alias_display_name") or row.get("alias"),
            default=DEFAULT_ALIAS_ID if alias_id == DEFAULT_ALIAS_ID else alias_id,
        )
        display_name = alias_entry.display_name if alias_entry is not None else fallback_display_name
        row["alias_id"] = alias_id
        row["alias_display_name"] = display_name
        row["alias"] = "Sin alias" if alias_id == DEFAULT_ALIAS_ID else display_name
        return row

    def _account_records(self) -> list[dict[str, Any]]:
        raw_records = self._raw_account_records()
        alias_records = self._alias_registry(raw_accounts=raw_records)
        return [
            self._decorate_account_record(item, alias_records=alias_records)
            for item in raw_records
        ]

    def _sorted_alias_records(self, records: dict[str, AliasRecord] | None = None) -> list[AliasRecord]:
        source = records or self._alias_registry()
        return sorted(
            source.values(),
            key=lambda record: (
                record.alias_id != DEFAULT_ALIAS_ID,
                record.display_name.casefold(),
                record.alias_id,
            ),
        )

    def _save_alias_registry(self, records: list[AliasRecord] | dict[str, AliasRecord]) -> None:
        if isinstance(records, dict):
            record_map = dict(records)
        else:
            record_map = {record.alias_id: record for record in records}

        deduped_by_display: dict[str, AliasRecord] = {}
        for record in record_map.values():
            if record.alias_id == DEFAULT_ALIAS_ID:
                continue
            display_key = normalize_alias_key(record.display_name)
            if not display_key:
                continue
            existing = deduped_by_display.get(display_key)
            if existing is None:
                deduped_by_display[display_key] = record
                continue
            existing_rank = (bool(existing.system), str(existing.created_at or ""), str(existing.alias_id or ""))
            candidate_rank = (bool(record.system), str(record.created_at or ""), str(record.alias_id or ""))
            if candidate_rank < existing_rank:
                deduped_by_display[display_key] = record

        if deduped_by_display:
            cleaned: dict[str, AliasRecord] = {}
            for record in record_map.values():
                if record.alias_id == DEFAULT_ALIAS_ID:
                    continue
                display_key = normalize_alias_key(record.display_name)
                if display_key and deduped_by_display.get(display_key) is not record:
                    continue
                cleaned[record.alias_id] = record
            record_map = cleaned

        record_map.setdefault(DEFAULT_ALIAS_ID, default_alias_record())
        payload = {
            "schema_version": _ALIAS_REGISTRY_SCHEMA_VERSION,
            "aliases": [record.to_payload() for record in self._sorted_alias_records(record_map)],
        }
        self.context.write_json(self._alias_registry_path(), payload)

    def list_alias_records(self) -> list[dict[str, Any]]:
        return [record.to_payload() for record in self._sorted_alias_records()]

    def rebuild_alias_registry(self) -> list[dict[str, Any]]:
        records = self._alias_registry()
        self._save_alias_registry(records)
        return [record.to_payload() for record in self._sorted_alias_records(records)]

    def resolve_alias_id(self, alias: str | None, *, default: str = DEFAULT_ALIAS_ID) -> str:
        clean_alias = normalize_alias_display(alias)
        if not clean_alias:
            return default
        alias_id = normalize_alias_id(clean_alias, default=default)
        records = self._alias_registry()
        if alias_id in records:
            return records[alias_id].alias_id
        return alias_id or default

    def resolve_alias_display_name(self, alias: str | None, *, default: str = DEFAULT_ALIAS_ID) -> str:
        alias_id = self.resolve_alias_id(alias, default=default)
        if alias_id == DEFAULT_ALIAS_ID:
            return "Sin alias"
        record = self._alias_registry().get(alias_id)
        if record is not None:
            return record.display_name
        return normalize_alias(alias, default=default)

    def get_alias_record(self, alias: str | None) -> dict[str, Any]:
        alias_id = self.resolve_alias_id(alias)
        record = self._alias_registry().get(alias_id)
        if record is None:
            fallback_name = normalize_alias(alias, default=alias_id or DEFAULT_ALIAS_ID)
            record = AliasRecord(
                alias_id=alias_id or DEFAULT_ALIAS_ID,
                display_name=fallback_name,
                created_at="",
                updated_at="",
                system=alias_id == DEFAULT_ALIAS_ID,
            )
        return record.to_payload()

    def update_alias_display_name(self, alias: str, display_name: str) -> dict[str, Any]:
        current = self.get_alias_record(alias)
        current_alias_id = str(current.get("alias_id") or DEFAULT_ALIAS_ID).strip() or DEFAULT_ALIAS_ID
        if current_alias_id == DEFAULT_ALIAS_ID:
            raise ServiceError("No se puede renombrar el alias default.")
        try:
            requested = alias_record_from_input(display_name)
        except AliasValidationError as exc:
            raise ServiceError(str(exc)) from exc
        next_alias_id = requested.alias_id
        next_display_name = requested.display_name
        if next_alias_id != current_alias_id:
            raise ServiceError("El nuevo nombre cambia la identidad del alias y requiere lifecycle completo.")
        records = self._alias_registry()
        next_key = normalize_alias_key(next_display_name)
        if next_key:
            for record in records.values():
                if record.alias_id == current_alias_id:
                    continue
                if normalize_alias_key(record.display_name) == next_key:
                    raise ServiceError("Ya existe un alias con ese nombre (ignorando mayusculas/espacios).")
        existing = records.get(current_alias_id)
        if existing is None:
            raise ServiceError("No se encontro el alias a renombrar.")
        updated = AliasRecord(
            alias_id=existing.alias_id,
            display_name=next_display_name,
            created_at=existing.created_at,
            updated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            system=existing.system,
        )
        records[current_alias_id] = updated
        try:
            self._save_alias_registry(records)
        except Exception as exc:
            raise ServiceError("No se pudo guardar el alias renombrado.") from exc
        sync_alias_metadata = getattr(accounts_module, "sync_alias_metadata", None)
        if callable(sync_alias_metadata):
            try:
                sync_alias_metadata(
                    current_alias_id,
                    alias_id=updated.alias_id,
                    display_name=updated.display_name,
                )
            except Exception as exc:
                records[current_alias_id] = existing
                try:
                    self._save_alias_registry(records)
                except Exception:
                    logger.exception("No se pudo revertir aliases.json luego de un error sincronizando cuentas.")
                raise ServiceError("No se pudo actualizar el alias en las cuentas.") from exc
        return updated.to_payload()

    def _record_alias_id(self, record: dict[str, Any]) -> str:
        return normalize_alias_id(record.get("alias_id") or record.get("alias"), default=DEFAULT_ALIAS_ID)

    def list_accounts(self, alias: str | None = None) -> list[dict[str, Any]]:
        records = self._account_records()
        if alias is None:
            return records
        alias_id = self.resolve_alias_id(alias)
        return [
            item
            for item in records
            if self._record_alias_id(item) == alias_id
        ]

    def list_aliases(self) -> list[str]:
        return [record.display_name for record in self._sorted_alias_records()]

    def create_alias(self, alias: str) -> str:
        try:
            requested = alias_record_from_input(alias)
        except AliasValidationError as exc:
            raise ServiceError(str(exc)) from exc
        records = self._alias_registry()
        requested_key = normalize_alias_key(requested.display_name)
        if requested_key:
            for record in records.values():
                if normalize_alias_key(record.display_name) == requested_key:
                    return record.display_name
        existing = records.get(requested.alias_id)
        if existing is not None:
            return existing.display_name
        original_records = dict(records)
        records[requested.alias_id] = requested
        try:
            self._save_alias_registry(records)
        except Exception as exc:
            raise ServiceError("No se pudo guardar el alias.") from exc
        sync_alias_metadata = getattr(accounts_module, "sync_alias_metadata", None)
        if callable(sync_alias_metadata):
            try:
                sync_alias_metadata(
                    requested.alias_id,
                    alias_id=requested.alias_id,
                    display_name=requested.display_name,
                )
            except Exception as exc:
                try:
                    self._save_alias_registry(original_records)
                except Exception:
                    logger.exception("No se pudo revertir aliases.json luego de un error sincronizando cuentas.")
                raise ServiceError("No se pudo sincronizar el alias en las cuentas.") from exc
        return requested.display_name

    def delete_alias(self, alias: str, *, move_accounts_to: str | None = None) -> None:
        clean_alias_id = self.resolve_alias_id(alias)
        if clean_alias_id == DEFAULT_ALIAS_ID:
            raise ServiceError("No se puede eliminar el alias default.")
        accounts = self.list_accounts(alias)
        if accounts and not move_accounts_to:
            raise ServiceError("El alias tiene cuentas. Define un alias destino para moverlas.")
        if accounts and move_accounts_to:
            self.move_accounts(
                [str(item.get("username") or "") for item in accounts],
                move_accounts_to,
            )
        records = self._alias_registry()
        records.pop(clean_alias_id, None)
        self._save_alias_registry(records)

    def connected_status(self, record: dict[str, Any]) -> bool:
        resolver = getattr(accounts_module, "connected_status", None)
        if callable(resolver):
            try:
                return bool(
                    resolver(
                        record,
                        strict=False,
                        reason="application-service",
                        fast=True,
                        persist=False,
                    )
                )
            except Exception:
                pass
        return bool(record.get("connected", False))

    def login_progress_for_account(self, record: dict[str, Any]) -> dict[str, Any]:
        username = str(record.get("username") or "").strip().lstrip("@")
        progress = health_store.get_login_progress(username)
        state = str(progress.get("state") or "").strip().lower()
        message = str(progress.get("message") or "").strip()
        active = bool(progress.get("active")) and bool(state)
        return {
            "active": active,
            "state": state,
            "message": message,
            "label": self._login_progress_label(state, message),
            "run_id": str(progress.get("run_id") or "").strip(),
            "updated_at": str(progress.get("updated_at") or "").strip(),
        }

    def queue_login_progress(
        self,
        usernames: list[str],
        *,
        run_id: str = "",
    ) -> None:
        for username in dedupe_usernames(usernames):
            health_store.set_login_progress(
                username,
                health_store.LOGIN_PROGRESS_QUEUED,
                run_id=run_id,
                message="En cola",
            )

    def clear_login_progress(self, usernames: list[str]) -> None:
        health_store.clear_login_progress_many(dedupe_usernames(usernames))

    def refresh_connected_health(
        self,
        alias: str,
        usernames: list[str] | None = None,
    ) -> dict[str, Any]:
        clean_alias, records = self._resolve_selected_records(alias, usernames)
        connected_records = [record for record in records if self.connected_status(record)]
        results: list[dict[str, Any]] = []
        state_counts = {
            health_store.HEALTH_STATE_ALIVE: 0,
            health_store.HEALTH_STATE_INACTIVE: 0,
            health_store.HEALTH_STATE_DEAD: 0,
        }
        error_count = 0

        for record in connected_records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            probe_ok = False
            probe_reason = ""
            error_message = ""
            try:
                probe_ok, probe_reason = _check_connected_account_health(record)
            except Exception as exc:
                error_count += 1
                error_message = str(exc) or exc.__class__.__name__
                probe_reason = f"exception:{error_message}"
                logger.exception("No se pudo refrescar health para @%s.", username)
            state, stale = health_store.get_badge(username)
            normalized_state = str(state or "").strip().upper()
            if normalized_state in state_counts:
                state_counts[normalized_state] += 1
            results.append(
                {
                    "username": username,
                    "ok": bool(probe_ok),
                    "reason": probe_reason,
                    "health": str(state or "").strip(),
                    "stale": bool(stale),
                    "error": error_message,
                }
            )

        return {
            "alias": clean_alias,
            "requested": len(records),
            "eligible": len(connected_records),
            "refreshed": len(results),
            "skipped": max(0, len(records) - len(connected_records)),
            "alive": state_counts[health_store.HEALTH_STATE_ALIVE],
            "inactive": state_counts[health_store.HEALTH_STATE_INACTIVE],
            "dead": state_counts[health_store.HEALTH_STATE_DEAD],
            "errors": error_count,
            "results": results,
        }

    def health_badge(self, record: dict[str, Any]) -> str:
        badge_resolver = getattr(accounts_module, "_badge_for_display", None)
        if callable(badge_resolver):
            try:
                badge, stale = badge_resolver(record)
                if stale:
                    return str(
                        getattr(
                            accounts_module,
                            "ACCOUNT_UI_STATE_UNVERIFIED",
                            "NO VERIFICADA",
                        )
                    ).strip()
                return str(badge or "").strip()
            except Exception:
                pass
        return str(record.get("health_badge") or "").strip()

    def manual_action_eligibility(self, record: dict[str, Any]) -> dict[str, Any]:
        active = bool(record.get("active", True))
        connected = bool(record.get("connected")) if "connected" in record else self.connected_status(record)
        badge = str(record.get("health_badge") or self.health_badge(record) or "").strip().upper()
        allowed = active and connected and badge == health_store.HEALTH_STATE_ALIVE
        message = "" if allowed else "Necesitas re-login en esta cuenta"
        return {
            "allowed": allowed,
            "connected": connected,
            "badge": badge,
            "message": message,
        }

    def get_alias_snapshot(self, alias: str) -> dict[str, Any]:
        rows = self.list_accounts(alias)
        connected = 0
        blocked = 0
        for row in rows:
            if self.connected_status(row):
                connected += 1
            badge = self.health_badge(row)
            if badge.strip().upper() == "MUERTA":
                blocked += 1
        proxies = self.list_proxy_records()
        proxy_ids = {
            str(item.get("assigned_proxy_id") or "").strip()
            for item in rows
            if str(item.get("assigned_proxy_id") or "").strip()
        }
        return {
            "alias": self.resolve_alias_display_name(alias),
            "accounts_total": len(rows),
            "accounts_connected": connected,
            "accounts_blocked": blocked,
            "proxies_total": len(proxies),
            "proxies_assigned": len(proxy_ids),
        }

    def add_account(
        self,
        username: str,
        alias: str,
        *,
        password: str = "",
        proxy: dict[str, Any] | None = None,
        totp_secret: str = "",
    ) -> bool:
        clean_username = str(username or "").strip().lstrip("@")
        if not clean_username:
            raise ServiceError("Username invalido.")
        clean_password = str(password or "").strip()
        if not clean_password:
            raise ServiceError("Password invalida.")
        clean_totp_secret = str(totp_secret or "").strip()
        clean_alias = self.create_alias(alias)
        added = accounts_module.add_account(clean_username, clean_alias, proxy)
        if not added:
            return False
        try:
            store_password = getattr(accounts_module, "_store_account_password", None)
            if callable(store_password):
                store_password(clean_username, clean_password)
            else:
                accounts_module.update_account(clean_username, {"password": clean_password})
        except Exception as exc:
            accounts_module.remove_account(clean_username)
            raise ServiceError(f"No se pudo persistir la cuenta @{clean_username}: {exc}") from exc
        if clean_totp_secret:
            try:
                save_totp_secret(clean_username, clean_totp_secret)
            except Exception as exc:
                accounts_module.remove_account(clean_username)
                raise ServiceError(f"No se pudo persistir TOTP para @{clean_username}: {exc}") from exc
        return True

    def remove_accounts(self, usernames: list[str]) -> int:
        removed = 0
        for username in dedupe_usernames(usernames):
            if accounts_module.get_account(username):
                accounts_module.remove_account(username)
                removed += 1
        return removed

    def move_accounts(self, usernames: list[str], target_alias: str) -> int:
        target = self.create_alias(target_alias)
        moved = 0
        for username in dedupe_usernames(usernames):
            if accounts_module.update_account(username, {"alias": target}):
                moved += 1
        return moved

    def import_accounts_csv(
        self,
        alias: str,
        path: str | Path,
        *,
        login_after_import: bool = False,
        concurrency: int = 1,
    ) -> dict[str, Any]:
        file_path = Path(path)
        if not file_path.is_file():
            raise ServiceError(f"No existe el archivo CSV: {file_path}")
        clean_alias = self.create_alias(alias)
        rows = _parse_accounts_csv(file_path)
        totp_entries = _extract_totp_entries_from_csv(file_path)
        added = 0
        skipped = 0
        imported_usernames: list[str] = []
        login_usernames: list[str] = []
        login_payloads: list[dict[str, Any]] = []
        build_login_payload = getattr(accounts_module, "_build_playwright_login_payload", None)

        for row_index, row in enumerate(rows, start=1):
            username = str(row.get("username") or "").strip().lstrip("@")
            password = str(row.get("password") or "").strip()
            if not username or not password:
                skipped += 1
                continue
            totp_secret = totp_entries.get(username.lower(), "")
            if not totp_secret:
                totp_secret = extract_totp_secret_from_row(row)
            proxy_url = _compose_proxy_url(
                str(row.get("proxy id") or ""),
                str(row.get("proxy port") or ""),
            )
            proxy = {
                "proxy_url": proxy_url,
                "proxy_user": str(row.get("proxy username") or "").strip(),
                "proxy_pass": str(row.get("proxy password") or "").strip(),
                "proxy_sticky_minutes": 10,
            }
            if not proxy_url:
                proxy = None
            try:
                created = self.add_account(
                    username,
                    clean_alias,
                    password=password,
                    proxy=proxy,
                    totp_secret=totp_secret,
                )
            except ServiceError as exc:
                raise ServiceError(
                    f"Fila {row_index}: no se pudo importar @{username}: {exc}"
                ) from exc
            if created:
                added += 1
                imported_usernames.append(username)
            else:
                skipped += 1
            if created:
                login_usernames.append(username)
            if (
                created
                and login_after_import
                and callable(build_login_payload)
            ):
                payload = build_login_payload(
                    username,
                    password,
                    proxy or {},
                    alias=clean_alias,
                    totp_secret=totp_secret,
                )
                login_payloads.append(payload)

        login_results: list[dict[str, Any]] = []
        if login_payloads:
            login_results = accounts_module.login_accounts_with_playwright(
                clean_alias,
                login_payloads,
            )
            self._report_login_failures(
                alias=clean_alias,
                results=login_results,
                source="import_accounts_csv",
            )

        return {
            "alias": clean_alias,
            "added": added,
            "skipped": skipped,
            "imported_usernames": imported_usernames,
            "login_usernames": login_usernames,
            "login_results": login_results,
        }

    def _resolve_selected_records(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        active_only: bool = False,
    ) -> tuple[str, list[dict[str, Any]]]:
        clean_alias = normalize_alias(alias)
        records = self.list_accounts(clean_alias)
        if usernames:
            selected = {item.lower() for item in dedupe_usernames(usernames)}
            records = [
                item
                for item in records
                if str(item.get("username") or "").strip().lstrip("@").lower() in selected
            ]
        if active_only:
            records = [item for item in records if bool(item.get("active", True))]
        return clean_alias, records

    def open_manual_sessions(
        self,
        alias: str,
        usernames: list[str],
        *,
        start_url: str,
        action_label: str,
        max_minutes: int = 0,
        restore_page_if_closed: bool = False,
    ) -> dict[str, Any]:
        clean_alias, records = self._resolve_selected_records(
            alias,
            usernames,
            active_only=True,
        )
        if not records:
            raise ServiceError("No hay cuentas activas seleccionadas para esta accion.")

        eligible_records = [
            record
            for record in records
            if self.manual_action_eligibility(record).get("allowed")
        ]
        if not eligible_records:
            raise ServiceError("Las cuentas seleccionadas requieren re-login antes de usar esta accion.")
        records = eligible_records

        opener = getattr(accounts_module, "_open_playwright_manual_session", None)
        if not callable(opener):
            raise ServiceError("Las sesiones manuales de Playwright no estan disponibles.")

        clear_close_request = getattr(accounts_module, "clear_manual_playwright_session_close_request", None)
        opened: list[str] = []
        max_seconds = max(0, int(max_minutes or 0)) * 60 or None
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            if callable(clear_close_request):
                clear_close_request(username)
            logger.info(
                "Opening manual session for @%s (%s, %s)",
                username,
                clean_alias,
                action_label,
            )
            try:
                launch_result = opener(
                    record,
                    start_url=start_url,
                    action_label=action_label,
                    max_seconds=max_seconds,
                    restore_page_if_closed=restore_page_if_closed,
                )
            except Exception as exc:
                logger.exception(
                    "Manual session failed for @%s (%s, %s)",
                    username,
                    clean_alias,
                    action_label,
                )
                raise ServiceError(f"Failed to open manual browser for @{username}: {exc}") from exc
            if not bool(dict(launch_result or {}).get("opened")):
                raise ServiceError(
                    f"Failed to open manual browser for @{username}: launcher did not confirm browser open."
                )
            opened.append(username)

        if not opened:
            raise ServiceError("No se pudo abrir ninguna cuenta seleccionada.")
        return {
            "alias": clean_alias,
            "action": action_label,
            "opened": opened,
            "count": len(opened),
        }

    def clear_manual_session_close_request(self, username: str) -> None:
        clearer = getattr(accounts_module, "clear_manual_playwright_session_close_request", None)
        if callable(clearer):
            clearer(username)

    def close_manual_session(self, username: str) -> bool:
        closer = getattr(accounts_module, "close_manual_playwright_session", None)
        if not callable(closer):
            return False
        return bool(closer(username))

    def shutdown_manual_sessions(self) -> None:
        shutdown = getattr(accounts_module, "shutdown_manual_playwright_sessions", None)
        if callable(shutdown):
            shutdown()

    def browse_accounts(
        self,
        alias: str,
        usernames: list[str],
        *,
        max_minutes: int = 0,
    ) -> dict[str, Any]:
        return self.open_manual_sessions(
            alias,
            usernames,
            start_url="https://www.instagram.com/",
            action_label="Navegar cuenta",
            max_minutes=max_minutes,
        )

    def edit_profiles(
        self,
        alias: str,
        usernames: list[str],
        *,
        max_minutes: int = 0,
    ) -> dict[str, Any]:
        return self.open_manual_sessions(
            alias,
            usernames,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label="Editar perfil",
            max_minutes=max_minutes,
        )

    def open_profile_sessions(
        self,
        alias: str,
        usernames: list[str],
        *,
        action_label: str = "Otros cambios",
        max_minutes: int = 0,
    ) -> dict[str, Any]:
        return self.open_manual_sessions(
            alias,
            usernames,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label=action_label,
            max_minutes=max_minutes,
        )

    def rename_account_username(self, old_username: str, new_username: str) -> str:
        old_clean = str(old_username or "").strip().lstrip("@")
        new_clean = str(new_username or "").strip().lstrip("@")
        if not old_clean or not new_clean:
            raise ServiceError("Debes indicar el username actual y el nuevo username.")
        if old_clean == new_clean:
            return old_clean
        if not accounts_module.get_account(old_clean):
            raise ServiceError(f"No se encontro la cuenta @{old_clean}.")

        renamer = getattr(accounts_module, "_rename_account_record", None)
        if callable(renamer):
            updated = str(renamer(old_clean, new_clean) or "").strip().lstrip("@")
        else:
            existing = accounts_module.get_account(new_clean)
            if existing and str(existing.get("username") or "").strip().lstrip("@").lower() != old_clean.lower():
                raise ServiceError(f"Ya existe una cuenta con @{new_clean}.")
            if not accounts_module.update_account(old_clean, {"username": new_clean}):
                raise ServiceError(f"No se pudo actualizar @{old_clean}.")
            updated = new_clean

        if not updated:
            raise ServiceError(f"No se pudo actualizar @{old_clean}.")
        if updated.lower() != new_clean.lower():
            raise ServiceError(f"No se pudo actualizar @{old_clean} a @{new_clean}.")

        recorder = getattr(accounts_module, "_record_profile_edit", None)
        if callable(recorder):
            try:
                recorder(updated, "username")
            except Exception:
                pass
        return updated

    def run_reels_playwright(
        self,
        alias: str,
        usernames: list[str],
        *,
        minutes: int = 10,
        likes_target: int = 0,
    ) -> list[dict[str, Any]]:
        clean_alias, records = self._resolve_selected_records(
            alias,
            usernames,
            active_only=True,
        )
        if not records:
            raise ServiceError("No hay cuentas activas seleccionadas para reels.")

        try:
            from automation.actions import interactions as interactions_module
        except Exception as exc:  # pragma: no cover - depende del entorno runtime
            raise ServiceError(f"No se pudo cargar el modulo de interacciones: {exc}") from exc

        minutes = max(1, int(minutes or 1))
        likes_target = max(0, int(likes_target or 0))

        interactions_module.ensure_logging(
            quiet=interactions_module.SETTINGS.quiet,
            log_dir=interactions_module.SETTINGS.log_dir,
            log_file=interactions_module.SETTINGS.log_file,
        )
        interactions_module.reset_stop_event()
        token = interactions_module.EngineCancellationToken("gui-interactions-reels-playwright")
        binding = interactions_module.bind_stop_token(token)
        listener = interactions_module.start_q_listener(
            "Presiona Q y Enter para detener la accion.",
            interactions_module.logger,
            token=token,
        )

        async def _runner():
            try:
                from src.auth.onboarding import build_proxy
                from src.proxy_payload import build_proxy_input_from_account
            except Exception as exc:
                raise RuntimeError(
                    "Playwright no esta disponible. Instala dependencias y navegadores."
                ) from exc

            base_profiles = interactions_module._profiles_root()
            session_manager = interactions_module._reels_session_manager(True)
            summaries: list[Any] = []
            for record in records:
                if interactions_module.STOP_EVENT.is_set():
                    break

                username = str(record.get("username") or "").strip().lstrip("@")
                summary = interactions_module.ReelsPlaywrightSummary(username=username)
                summaries.append(summary)

                storage_state = base_profiles / username / "storage_state.json"
                if not storage_state.exists():
                    summary.errors += 1
                    summary.messages.append("Falta runtime/browser_profiles/<username>/storage_state.json.")
                    continue

                proxy_payload = None
                try:
                    proxy_input = build_proxy_input_from_account(record)
                    proxy_payload = build_proxy(proxy_input) if proxy_input else None
                except Exception:
                    proxy_payload = None

                session = None
                try:
                    interactions_module.log_browser_stage(
                        component="automation_reels_playwright",
                        stage="spawn",
                        status="started",
                        account=username,
                    )
                    session = await session_manager.open_session(
                        account=record,
                        proxy=proxy_payload,
                        login_func=interactions_module.ensure_logged_in_async,
                    )
                    page = session.page
                    try:
                        await page.goto(
                            "https://www.instagram.com/reels/?hl=en",
                            wait_until="domcontentloaded",
                            timeout=60_000,
                        )
                    except Exception:
                        await page.goto("https://www.instagram.com/reels/")
                    current_url = ""
                    try:
                        current_url = (page.url or "").lower()
                    except Exception:
                        current_url = ""
                    if any(token_value in current_url for token_value in ("accounts/login", "/challenge/", "/checkpoint/")):
                        raise RuntimeError("La sesion Playwright expiro o requiere verificacion.")
                    interactions_module.log_browser_stage(
                        component="automation_reels_playwright",
                        stage="workspace_ready",
                        status="ok",
                        account=username,
                        url=str(getattr(page, "url", "") or ""),
                    )
                    await interactions_module._run_reels_for_account(
                        page=page,
                        summary=summary,
                        duration_s=minutes * 60,
                        likes_target=likes_target,
                    )
                except Exception as exc:
                    await session_manager.discard_if_unhealthy(
                        session,
                        exc,
                        is_fatal_error=lambda error: "closed" in str(error or "").lower(),
                    )
                    summary.errors += 1
                    summary.messages.append(interactions_module._short_message(exc, limit=160))
                finally:
                    if session is not None:
                        current_url = ""
                        try:
                            current_url = str(getattr(session.page, "url", "") or "")
                        except Exception:
                            current_url = ""
                        try:
                            await session_manager.finalize_session(session, current_url=current_url)
                        except Exception:
                            pass

            return summaries

        try:
            summaries = interactions_module._run_async(_runner()) or []
        except Exception as exc:
            raise ServiceError(str(exc) or "No se pudo ejecutar reels.") from exc
        finally:
            interactions_module.request_stop("reels finalizados")
            listener.join(timeout=0.2)
            interactions_module.restore_stop_token(binding)

        payloads: list[dict[str, Any]] = []
        for summary in summaries:
            payloads.append(
                {
                    "username": str(getattr(summary, "username", "") or ""),
                    "viewed": int(getattr(summary, "viewed", 0) or 0),
                    "liked": int(getattr(summary, "liked", 0) or 0),
                    "errors": int(getattr(summary, "errors", 0) or 0),
                    "messages": list(getattr(summary, "messages", []) or []),
                }
            )
        return payloads

    def export_accounts_csv(
        self,
        alias: str,
        *,
        destination: str | Path | None = None,
        include_totp_secret: bool = False,
    ) -> dict[str, Any]:
        clean_alias = normalize_alias(alias)
        records = self.list_accounts(clean_alias)
        if not records:
            raise ServiceError("No hay cuentas para exportar.")

        export_paths = getattr(accounts_module, "_export_paths", None)
        if destination is None and callable(export_paths):
            csv_path, totp_backup_path = export_paths(clean_alias)
        else:
            target = Path(destination) if destination else self.context.storage_path(
                "exports", f"{clean_alias}_accounts.csv"
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            csv_path = target
            totp_backup_path = target.with_name(f"{target.stem}_totp_backup.zip")

        account_password = getattr(accounts_module, "_account_password", None)
        current_totp_code = getattr(accounts_module, "_current_totp_code", None)
        proxy_components = getattr(accounts_module, "_proxy_components", None)
        badge_for_display = getattr(accounts_module, "_badge_for_display", None)
        account_status_from_badge = getattr(accounts_module, "_account_status_from_badge", None)
        proxy_status_from_badge = getattr(accounts_module, "_proxy_status_from_badge", None)
        export_totp_backup = getattr(accounts_module, "_export_totp_backup_zip", None)
        get_totp_secret = getattr(accounts_module, "get_totp_secret", None)

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        headers = [
            "Username",
            "Password",
            "Codigo 2FA",
            "Proxy IP",
            "Proxy Puerto",
            "Proxy Usuario",
            "Proxy Password",
            "Estado cuenta",
            "Estado proxy",
        ]
        if include_totp_secret:
            headers.append("TOTP Secret")
        writer.writerow(headers)

        for account in records:
            username = str(account.get("username") or "").strip()
            badge = ""
            if callable(badge_for_display):
                try:
                    badge, _cached = badge_for_display(account)
                except Exception:
                    badge = ""
            account_status = (
                account_status_from_badge(account, badge)
                if callable(account_status_from_badge)
                else ("conectada" if self.connected_status(account) else "desconocida")
            )
            proxy_status = (
                proxy_status_from_badge(account, badge)
                if callable(proxy_status_from_badge)
                else (
                    "activa"
                    if str(account.get("proxy_url") or "").strip()
                    else "sin proxy"
                )
            )
            proxy_ip = proxy_port = proxy_user = proxy_pass = ""
            if callable(proxy_components):
                try:
                    proxy_ip, proxy_port, proxy_user, proxy_pass = proxy_components(account)
                except Exception:
                    proxy_ip = proxy_port = proxy_user = proxy_pass = ""
            row = [
                username,
                account_password(account)
                if callable(account_password)
                else str(account.get("password") or ""),
                current_totp_code(username) if callable(current_totp_code) else "",
                proxy_ip,
                proxy_port,
                proxy_user,
                proxy_pass,
                account_status,
                proxy_status,
            ]
            if include_totp_secret:
                if callable(get_totp_secret):
                    try:
                        row.append(get_totp_secret(username) or "")
                    except Exception:
                        row.append("")
                else:
                    row.append("")
            writer.writerow(row)

        csv_path.write_text(buffer.getvalue(), encoding="utf-8")
        totp_written = 0
        if callable(export_totp_backup):
            totp_written = export_totp_backup(
                [str(item.get("username") or "") for item in records],
                totp_backup_path,
            )
        return {
            "csv_path": csv_path,
            "totp_backup_path": totp_backup_path if totp_written else None,
            "totp_backup_records": totp_written,
        }

    def relogin(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        clean_alias = normalize_alias(alias)
        records = self.list_accounts(clean_alias)
        if usernames:
            selected = {item.lower() for item in dedupe_usernames(usernames)}
            records = [
                item
                for item in records
                if str(item.get("username") or "").strip().lstrip("@").lower()
                in selected
            ]
        if not records:
            raise ServiceError("No hay cuentas para relogin.")

        valid_records: list[dict[str, Any]] = []
        ordered_results: list[dict[str, Any] | None] = []
        valid_indexes: list[int] = []
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            proxy_state = self.proxy_state_for_account(record)
            if str(proxy_state.get("status") or "").strip() in {"inactive", "missing", "quarantined"}:
                health_store.clear_login_progress(username)
                ordered_results.append(
                    self._invalid_login_result(
                        username,
                        str(proxy_state.get("status") or "proxy_invalid"),
                    )
                )
                continue
            password = self._account_password_value(record)
            if not password:
                health_store.clear_login_progress(username)
                ordered_results.append(
                    self._invalid_login_result(username, "missing_password")
                )
                continue
            valid_indexes.append(len(ordered_results))
            ordered_results.append(None)
            valid_records.append(dict(record))

        if not valid_records and ordered_results:
            invalid_results = [item for item in ordered_results if isinstance(item, dict)]
            self._report_login_failures(
                alias=clean_alias,
                results=invalid_results,
                source="relogin",
            )
            return invalid_results

        if not valid_records:
            raise ServiceError("No hay cuentas validas para relogin.")

        results = accounts_module.relogin_accounts_with_playwright(
            clean_alias,
            valid_records,
        )
        for result_index, result in enumerate(results):
            if result_index >= len(valid_indexes):
                break
            ordered_results[valid_indexes[result_index]] = dict(result)
        merged_results = [item for item in ordered_results if isinstance(item, dict)]
        self._report_login_failures(
            alias=clean_alias,
            results=merged_results,
            source="relogin",
        )
        return merged_results

    def login(
        self,
        alias: str,
        usernames: list[str] | None = None,
        *,
        concurrency: int = 1,
    ) -> list[dict[str, Any]]:
        clean_alias = normalize_alias(alias)
        records = self.list_accounts(clean_alias)
        if usernames:
            selected = {item.lower() for item in dedupe_usernames(usernames)}
            records = [
                item
                for item in records
                if str(item.get("username") or "").strip().lstrip("@").lower()
                in selected
            ]
        if not records:
            raise ServiceError("No hay cuentas para login.")

        build_login_payload = getattr(accounts_module, "_build_playwright_login_payload", None)
        account_password = getattr(accounts_module, "_account_password", None)
        if not callable(build_login_payload):
            raise ServiceError("Login Playwright no disponible en este entorno.")

        payloads: list[dict[str, Any]] = []
        ordered_results: list[dict[str, Any] | None] = []
        valid_indexes: list[int] = []
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            proxy_state = self.proxy_state_for_account(record)
            if str(proxy_state.get("status") or "").strip() in {"inactive", "missing", "quarantined"}:
                health_store.clear_login_progress(username)
                ordered_results.append(
                    self._invalid_login_result(
                        username,
                        str(proxy_state.get("status") or "proxy_invalid"),
                    )
                )
                continue
            if callable(account_password):
                password = str(account_password(record) or "").strip()
            else:
                password = str(record.get("password") or "").strip()
            if not password:
                health_store.clear_login_progress(username)
                ordered_results.append(
                    self._invalid_login_result(username, "missing_password")
                )
                continue
            valid_indexes.append(len(ordered_results))
            ordered_results.append(None)
            payloads.append(
                build_login_payload(
                    username,
                    password,
                    record,
                    alias=clean_alias,
                )
            )

        if not payloads and ordered_results:
            invalid_results = [item for item in ordered_results if isinstance(item, dict)]
            self._report_login_failures(
                alias=clean_alias,
                results=invalid_results,
                source="login",
            )
            return invalid_results
        if not payloads:
            raise ServiceError("No hay cuentas validas para login.")

        results = accounts_module.login_accounts_with_playwright(
            clean_alias,
            payloads,
        )
        for result_index, result in enumerate(results):
            if result_index >= len(valid_indexes):
                break
            ordered_results[valid_indexes[result_index]] = dict(result)
        merged_results = [item for item in ordered_results if isinstance(item, dict)]
        self._report_login_failures(
            alias=clean_alias,
            results=merged_results,
            source="login",
        )
        return merged_results

    def set_message_limit(self, usernames: list[str], limit: int) -> int:
        clean_limit = max(1, int(limit or 1))
        updated = 0
        for username in dedupe_usernames(usernames):
            if accounts_module.update_account(
                username,
                {
                    "messages_per_account": clean_limit,
                    "max_messages": clean_limit,
                },
            ):
                updated += 1
        return updated

    def _proxy_status(self, proxy_id: str) -> dict[str, Any]:
        return proxy_reference_status(proxy_id, path=self._proxy_path())

    def proxy_state_for_account(self, account: dict[str, Any]) -> dict[str, Any]:
        assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
        if assigned_proxy_id:
            return self._proxy_status(assigned_proxy_id)
        proxy_url = str(account.get("proxy_url") or "").strip()
        if proxy_url:
            return {
                "status": "legacy",
                "proxy_id": "",
                "record": None,
                "message": "La cuenta usa proxy directo legacy.",
            }
        return {
            "status": "none",
            "proxy_id": "",
            "record": None,
            "message": "",
        }

    def proxy_display_for_account(self, account: dict[str, Any]) -> dict[str, str]:
        state = self.proxy_state_for_account(account)
        status = str(state.get("status") or "none").strip()
        assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
        proxy_url = str(account.get("proxy_url") or "").strip()
        if status == "ok" and assigned_proxy_id:
            return {"status": status, "label": assigned_proxy_id}
        if status == "inactive" and assigned_proxy_id:
            return {"status": status, "label": f"{assigned_proxy_id} (INACTIVO)"}
        if status == "quarantined" and assigned_proxy_id:
            return {"status": status, "label": f"{assigned_proxy_id} (CUARENTENA)"}
        if status == "missing" and assigned_proxy_id:
            return {"status": status, "label": f"{assigned_proxy_id} (FALTANTE)"}
        if status == "legacy" and proxy_url:
            return {"status": status, "label": proxy_url}
        return {"status": status, "label": "-"}

    def proxy_preflight_for_account(
        self,
        account: dict[str, Any],
        *,
        allow_proxyless: bool = True,
        allow_legacy: bool = True,
    ) -> dict[str, Any]:
        return account_proxy_preflight(
            account,
            path=self._proxy_path(),
            allow_proxyless=allow_proxyless,
            allow_legacy=allow_legacy,
        )

    def proxy_preflight_for_accounts(
        self,
        accounts: list[dict[str, Any]],
        *,
        allow_proxyless: bool = True,
        allow_legacy: bool = True,
    ) -> dict[str, Any]:
        return preflight_accounts_for_proxy_runtime(
            accounts,
            path=self._proxy_path(),
            allow_proxyless=allow_proxyless,
            allow_legacy=allow_legacy,
        )

    def proxy_health_label(self, record: dict[str, Any]) -> str:
        return build_proxy_health_label(record)

    def proxy_integrity_summary(self) -> dict[str, Any]:
        proxies = self.list_proxy_records()
        accounts = self.list_accounts(None)
        assigned_accounts = 0
        invalid_assignments = 0
        inactive_assignments = 0
        missing_assignments = 0
        quarantined_assignments = 0
        for account in accounts:
            assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
            if not assigned_proxy_id:
                continue
            assigned_accounts += 1
            state = self.proxy_state_for_account(account)
            status = str(state.get("status") or "").strip()
            if status == "inactive":
                invalid_assignments += 1
                inactive_assignments += 1
            elif status == "quarantined":
                invalid_assignments += 1
                quarantined_assignments += 1
            elif status == "missing":
                invalid_assignments += 1
                missing_assignments += 1
        return {
            "total": len(proxies),
            "active": sum(1 for item in proxies if bool(item.get("active", True))),
            "quarantined": sum(1 for item in proxies if float(item.get("quarantine_until") or 0.0) > 0.0),
            "assigned_accounts": assigned_accounts,
            "invalid_assignments": invalid_assignments,
            "inactive_assignments": inactive_assignments,
            "quarantined_assignments": quarantined_assignments,
            "missing_assignments": missing_assignments,
        }

    def recent_proxy_audit(self, *, proxy_id: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
        return load_proxy_audit_entries(
            proxy_id=proxy_id,
            limit=limit,
            path=proxy_audit_path(self.context.root_dir),
        )

    def list_proxy_records(self) -> list[dict[str, Any]]:
        return load_proxies(self._proxy_path())

    def save_proxy_records(self, proxies: list[dict[str, Any]]) -> None:
        save_registry_proxy_records(proxies, self._proxy_path())

    def upsert_proxy(self, record: dict[str, Any]) -> dict[str, Any]:
        try:
            normalized = upsert_proxy_record(record, self._proxy_path())
        except ProxyValidationError as exc:
            raise ServiceError(str(exc) or "Proxy invalido.") from exc
        record_proxy_audit_event(
            normalized["id"],
            event="proxy_upsert",
            status="ok",
            message=f"Proxy {normalized['id']} guardado.",
            meta={"active": normalized.get("active", True)},
            path=proxy_audit_path(self.context.root_dir),
        )
        return normalized

    def toggle_proxy_active(self, proxy_id: str, *, active: bool) -> dict[str, Any]:
        clean_id = str(proxy_id or "").strip()
        if not clean_id:
            raise ServiceError("Proxy invalido.")
        try:
            updated = set_proxy_active(clean_id, active=bool(active), path=self._proxy_path())
            if active:
                clear_proxy_quarantine(
                    clean_id,
                    path=self._proxy_path(),
                    audit_path=proxy_audit_path(self.context.root_dir),
                )
        except ProxyValidationError as exc:
            raise ServiceError(str(exc) or "Proxy invalido.") from exc
        record_proxy_audit_event(
            clean_id,
            event="proxy_toggle_active",
            status="ok",
            message=f"Proxy {clean_id} {'activado' if active else 'desactivado'}.",
            meta={"active": bool(active)},
            path=proxy_audit_path(self.context.root_dir),
        )
        return updated

    def import_proxies_csv(self, path: str | Path) -> dict[str, Any]:
        file_path = Path(path)
        if not file_path.is_file():
            raise ServiceError(f"No existe el archivo CSV: {file_path}")

        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ServiceError("El CSV de proxies no tiene cabeceras.")
            proxy_payloads: list[dict[str, Any]] = []
            imported = 0
            for row in reader:
                proxy_id = str(
                    row.get("id")
                    or row.get("proxy_id")
                    or row.get("name")
                    or row.get("alias")
                    or ""
                ).strip()
                server = str(
                    row.get("server")
                    or row.get("proxy_url")
                    or row.get("url")
                    or row.get("proxy")
                    or ""
                ).strip()
                if not proxy_id or not server:
                    continue
                active_raw = str(row.get("active") or "true").strip().lower()
                payload = {
                    "id": proxy_id,
                    "server": server,
                    "user": str(row.get("user") or row.get("username") or "").strip(),
                    "pass": str(row.get("pass") or row.get("password") or "").strip(),
                    "active": active_raw not in {"0", "false", "no", "off"},
                }
                proxy_payloads.append(payload)
                imported += 1
        try:
            upsert_proxy_records(proxy_payloads, self._proxy_path())
        except ProxyValidationError as exc:
            raise ServiceError(str(exc) or "Proxy invalido.") from exc
        record_proxy_audit_event(
            "bulk",
            event="proxy_import_csv",
            status="ok",
            message=f"Importados {imported} proxies desde CSV.",
            meta={"path": str(file_path), "imported": imported},
            path=proxy_audit_path(self.context.root_dir),
        )
        return {"imported": imported}

    @staticmethod
    def _parse_proxy_timestamp(value: Any) -> datetime | None:
        cleaned = str(value or "").strip()
        if not cleaned:
            return None
        normalized = cleaned.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _proxy_config_from_record(record: dict[str, Any]) -> ProxyConfig:
        return ProxyConfig(
            url=str(record.get("server") or "").strip(),
            user=str(record.get("user") or "").strip() or None,
            password=str(record.get("pass") or "").strip() or None,
            sticky_minutes=10,
        )

    def _record_proxy_probe_success(
        self,
        proxy_id: str,
        *,
        public_ip: str,
        latency_ms: float,
        event: str,
    ) -> dict[str, Any]:
        if event == "proxy_test":
            return record_proxy_test_success(
                proxy_id,
                public_ip=public_ip,
                latency_ms=latency_ms,
                path=self._proxy_path(),
                audit_path=proxy_audit_path(self.context.root_dir),
            )
        return record_proxy_success(
            proxy_id,
            event=event,
            public_ip=public_ip,
            latency_ms=latency_ms,
            path=self._proxy_path(),
            audit_path=proxy_audit_path(self.context.root_dir),
            message=f"Chequeo OK para {proxy_id}.",
        )

    def _record_proxy_probe_failure(self, proxy_id: str, *, error: str, event: str) -> dict[str, Any]:
        if event == "proxy_test":
            return record_proxy_test_failure(
                proxy_id,
                error=error,
                path=self._proxy_path(),
                audit_path=proxy_audit_path(self.context.root_dir),
            )
        return record_proxy_failure(
            proxy_id,
            event=event,
            error=error,
            path=self._proxy_path(),
            audit_path=proxy_audit_path(self.context.root_dir),
        )

    def _probe_proxy_record(
        self,
        record: dict[str, Any],
        *,
        event: str,
    ) -> tuple[dict[str, Any], Any]:
        clean_id = str(record.get("id") or "").strip()
        if not clean_id:
            raise ServiceError("Proxy invalido.")
        try:
            binding = test_proxy_connection(self._proxy_config_from_record(record))
        except Exception as exc:
            self._record_proxy_probe_failure(
                clean_id,
                error=str(exc) or type(exc).__name__,
                event=event,
            )
            raise ServiceError(str(exc) or "No se pudo probar el proxy.") from exc
        updated = self._record_proxy_probe_success(
            clean_id,
            public_ip=str(binding.public_ip or "").strip(),
            latency_ms=float(binding.latency or 0.0) * 1000.0,
            event=event,
        )
        return updated, binding

    def test_proxy(self, proxy_id: str) -> dict[str, Any]:
        clean_id = str(proxy_id or "").strip()
        if not clean_id:
            raise ServiceError("Proxy invalido.")
        record = get_proxy_by_id(clean_id, path=self._proxy_path())
        if not record:
            raise ServiceError(f"No se encontro el proxy {clean_id}.")
        updated, binding = self._probe_proxy_record(record, event="proxy_test")
        return {
            "proxy_id": clean_id,
            "public_ip": binding.public_ip,
            "masked_ip": binding.masked_ip,
            "latency": binding.latency,
            "health_label": self.proxy_health_label(updated),
            "last_test_at": str(updated.get("last_test_at") or ""),
        }

    def sweep_proxy_health(
        self,
        *,
        proxy_ids: list[str] | None = None,
        active_only: bool = True,
        only_assigned: bool = True,
        limit: int = 0,
        stale_after_seconds: float = 0.0,
        source: str = "manual",
    ) -> dict[str, Any]:
        requested_ids = {
            str(item or "").strip().lower()
            for item in (proxy_ids or [])
            if str(item or "").strip()
        }
        assigned_ids = {
            str(item.get("assigned_proxy_id") or "").strip().lower()
            for item in self.list_accounts(None)
            if str(item.get("assigned_proxy_id") or "").strip()
        }
        candidates: list[dict[str, Any]] = []
        skipped_filtered = 0
        skipped_not_due = 0
        now = datetime.now(timezone.utc)
        for record in self.list_proxy_records():
            proxy_id = str(record.get("id") or "").strip()
            if not proxy_id:
                skipped_filtered += 1
                continue
            proxy_key = proxy_id.lower()
            if requested_ids and proxy_key not in requested_ids:
                skipped_filtered += 1
                continue
            if active_only and not bool(record.get("active", True)):
                skipped_filtered += 1
                continue
            if only_assigned and proxy_key not in assigned_ids:
                skipped_filtered += 1
                continue
            last_test_at = self._parse_proxy_timestamp(record.get("last_test_at"))
            if stale_after_seconds > 0 and last_test_at is not None:
                age_seconds = max(0.0, (now - last_test_at).total_seconds())
                if age_seconds < float(stale_after_seconds):
                    skipped_not_due += 1
                    continue
            candidates.append(record)
        candidates.sort(key=lambda item: (str(item.get("last_test_at") or ""), str(item.get("id") or "")))
        if int(limit or 0) > 0:
            candidates = candidates[: max(0, int(limit or 0))]

        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        results: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0
        for record in candidates:
            proxy_id = str(record.get("id") or "").strip()
            try:
                updated, binding = self._probe_proxy_record(record, event="proxy_sweep")
                results.append(
                    {
                        "proxy_id": proxy_id,
                        "ok": True,
                        "public_ip": str(binding.public_ip or "").strip(),
                        "latency": float(binding.latency or 0.0),
                        "health_label": self.proxy_health_label(updated),
                        "last_test_at": str(updated.get("last_test_at") or ""),
                    }
                )
                succeeded += 1
            except ServiceError as exc:
                updated = get_proxy_by_id(proxy_id, path=self._proxy_path()) or record
                results.append(
                    {
                        "proxy_id": proxy_id,
                        "ok": False,
                        "error": str(exc) or "proxy_check_failed",
                        "health_label": self.proxy_health_label(updated),
                        "last_test_at": str(updated.get("last_test_at") or ""),
                    }
                )
                failed += 1

        finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        checked = len(results)
        if checked > 0:
            if failed == 0:
                status = "ok"
            elif succeeded > 0:
                status = "partial"
            else:
                status = "failed"
            record_proxy_audit_event(
                "bulk",
                event="proxy_sweep",
                status=status,
                message=(
                    f"Barrido de proxies ({source}) completado: "
                    f"{succeeded} OK, {failed} con error, {checked} chequeados."
                ),
                meta={
                    "source": source,
                    "checked": checked,
                    "succeeded": succeeded,
                    "failed": failed,
                    "requested": sorted(requested_ids),
                    "only_assigned": bool(only_assigned),
                    "active_only": bool(active_only),
                    "stale_after_seconds": float(stale_after_seconds or 0.0),
                },
                path=proxy_audit_path(self.context.root_dir),
            )
        return {
            "source": source,
            "started_at": started_at,
            "finished_at": finished_at,
            "checked": checked,
            "succeeded": succeeded,
            "failed": failed,
            "skipped_filtered": skipped_filtered,
            "skipped_not_due": skipped_not_due,
            "results": results,
        }

    def delete_proxy(self, proxy_id: str) -> int:
        clean_id = str(proxy_id or "").strip()
        if not clean_id:
            return 0
        assigned = [
            str(item.get("username") or "").strip().lstrip("@")
            for item in self.list_accounts(None)
            if str(item.get("assigned_proxy_id") or "").strip().lower() == clean_id.lower()
        ]
        if assigned:
            raise ServiceError(
                f"No se puede eliminar {clean_id}: hay {len(assigned)} cuenta(s) asignadas."
            )
        try:
            deleted = delete_proxy_record(clean_id, self._proxy_path())
        except ProxyValidationError as exc:
            raise ServiceError(str(exc) or "Proxy invalido.") from exc
        if not deleted:
            return 0
        record_proxy_audit_event(
            clean_id,
            event="proxy_delete",
            status="ok",
            message=f"Proxy {clean_id} eliminado.",
            path=proxy_audit_path(self.context.root_dir),
        )
        return int(deleted)

    def assign_proxy(self, usernames: list[str], proxy_id: str) -> int:
        clean_proxy_id = str(proxy_id or "").strip()
        if not clean_proxy_id:
            raise ServiceError("Proxy invalido.")
        status = self._proxy_status(clean_proxy_id)
        if str(status.get("status") or "").strip() != "ok":
            raise ServiceError(
                str(status.get("message") or f"El proxy {clean_proxy_id} no esta disponible.")
            )
        updated = 0
        for username in dedupe_usernames(usernames):
            if assign_proxy_to_account(username, clean_proxy_id):
                updated += 1
        if updated:
            record_proxy_audit_event(
                clean_proxy_id,
                event="proxy_assign",
                status="ok",
                message=f"Proxy {clean_proxy_id} asignado a {updated} cuenta(s).",
                meta={"accounts": dedupe_usernames(usernames)},
                path=proxy_audit_path(self.context.root_dir),
            )
        return updated

    def session_rows(self, alias: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for account in self.list_accounts(alias):
            username = str(account.get("username") or "").strip()
            if not username:
                continue
            rows.append(
                {
                    "username": username,
                    "alias": normalize_alias(account.get("alias")),
                    "connected": self.connected_status(account),
                    "health_badge": self.health_badge(account),
                    "active": bool(account.get("active", True)),
                    "last_updated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                }
            )
        return rows
