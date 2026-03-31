from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from core.alias_identity import (
    DEFAULT_ALIAS_DISPLAY_NAME,
    DEFAULT_ALIAS_ID,
    AliasValidationError,
    RESERVED_ALIAS_IDS,
    alias_record_from_input,
    normalize_alias_id,
)
from src.persistence import get_app_state_store

from .account_service import AccountService
from .automation_service import AutomationService
from .base import ServiceContext, ServiceError
from .warmup_service import WarmupService


class AliasLifecycleService:
    def __init__(
        self,
        context: ServiceContext,
        *,
        accounts: AccountService | None = None,
        automation: AutomationService | None = None,
        warmup: WarmupService | None = None,
        automation_provider: Callable[[], AutomationService] | None = None,
        warmup_provider: Callable[[], WarmupService] | None = None,
    ) -> None:
        self.context = context
        self.accounts = accounts or AccountService(context)
        self._automation = automation
        self._warmup = warmup
        self._automation_provider = automation_provider
        self._warmup_provider = warmup_provider
        self.state_store = get_app_state_store(context.root_dir)

    @property
    def automation(self) -> AutomationService:
        if self._automation is None:
            provider = self._automation_provider
            self._automation = provider() if callable(provider) else AutomationService(self.context)
        return self._automation

    @property
    def warmup(self) -> WarmupService:
        if self._warmup is None:
            provider = self._warmup_provider
            self._warmup = provider() if callable(provider) else WarmupService(self.context)
        return self._warmup

    def list_alias_records(self) -> list[dict[str, Any]]:
        return self.accounts.list_alias_records()

    def diagnose_integrity(self) -> dict[str, Any]:
        alias_records = self.accounts.rebuild_alias_registry()
        registered_alias_ids = {
            str(item.get("alias_id") or "").strip()
            for item in alias_records
            if isinstance(item, dict) and str(item.get("alias_id") or "").strip()
        }
        issues: list[dict[str, Any]] = []

        active_alias = str(self.state_store.get_active_alias() or "").strip()
        if active_alias:
            active_alias_id = normalize_alias_id(active_alias, default="")
            if active_alias_id and active_alias_id != DEFAULT_ALIAS_ID and active_alias_id not in registered_alias_ids:
                issues.append(
                    {
                        "type": "invalid_active_alias",
                        "source": "app_state",
                        "alias": active_alias,
                        "repairable": True,
                        "action": "reset_to_default",
                    }
                )

        for record in self.accounts.list_accounts(None):
            alias_label = str(record.get("alias") or "").strip()
            alias_id = normalize_alias_id(record.get("alias_id") or alias_label, default="")
            if alias_id in RESERVED_ALIAS_IDS:
                issues.append(
                    {
                        "type": "reserved_alias_reference",
                        "source": "accounts",
                        "alias": alias_label,
                        "account": str(record.get("username") or "").strip(),
                        "repairable": False,
                    }
                )

        for flow in self.warmup.list_flows():
            alias_label = str(flow.get("alias") or "").strip()
            alias_id = normalize_alias_id(alias_label, default="")
            if not alias_id:
                continue
            if alias_id in RESERVED_ALIAS_IDS:
                issues.append(
                    {
                        "type": "reserved_alias_reference",
                        "source": "warmup",
                        "alias": alias_label,
                        "flow_id": int(flow.get("id") or 0),
                        "repairable": False,
                    }
                )
                continue
            if alias_id not in registered_alias_ids:
                issues.append(
                    {
                        "type": "missing_alias_record",
                        "source": "warmup",
                        "alias": alias_label,
                        "flow_id": int(flow.get("id") or 0),
                        "repairable": True,
                    }
                )

        automation_refs = self.automation.alias_reference_snapshot()
        for source_name, values in (
            ("automation_prompt", automation_refs.get("prompt_aliases") or []),
            ("automation_followup", automation_refs.get("followup_aliases") or []),
        ):
            for alias_label in values:
                alias_id = normalize_alias_id(alias_label, default="")
                if not alias_id or alias_id in {DEFAULT_ALIAS_ID, *RESERVED_ALIAS_IDS}:
                    continue
                if alias_id not in registered_alias_ids:
                    issues.append(
                        {
                            "type": "missing_alias_record",
                            "source": source_name,
                            "alias": alias_label,
                            "repairable": True,
                        }
                    )
        for entry in automation_refs.get("followup_selected_aliases") or []:
            if not isinstance(entry, dict):
                continue
            owner_label = str(entry.get("owner_label") or "").strip()
            for alias_label in entry.get("selected_aliases") or []:
                alias_id = normalize_alias_id(alias_label, default="")
                if not alias_id:
                    continue
                if alias_id in RESERVED_ALIAS_IDS:
                    issues.append(
                        {
                            "type": "reserved_alias_reference",
                            "source": "automation_followup_selection",
                            "alias": alias_label,
                            "owner": owner_label,
                            "repairable": False,
                        }
                    )
                    continue
                if alias_id == DEFAULT_ALIAS_ID:
                    continue
                if alias_id not in registered_alias_ids:
                    issues.append(
                        {
                            "type": "missing_alias_record",
                            "source": "automation_followup_selection",
                            "alias": alias_label,
                            "owner": owner_label,
                            "repairable": True,
                        }
                    )

        issues.sort(
            key=lambda item: (
                not bool(item.get("repairable")),
                str(item.get("source") or ""),
                str(item.get("alias") or ""),
            )
        )
        return {
            "registered_aliases": alias_records,
            "issues": issues,
            "repairable_count": sum(1 for item in issues if bool(item.get("repairable"))),
            "unrepairable_count": sum(1 for item in issues if not bool(item.get("repairable"))),
        }

    def repair_integrity(self) -> dict[str, Any]:
        before = self.diagnose_integrity()
        repaired_aliases: list[str] = []
        for issue in before.get("issues") or []:
            if not isinstance(issue, dict) or not bool(issue.get("repairable")):
                continue
            if str(issue.get("type") or "") != "missing_alias_record":
                continue
            alias_label = str(issue.get("alias") or "").strip()
            alias_id = normalize_alias_id(alias_label, default="")
            if not alias_label or not alias_id or alias_id in RESERVED_ALIAS_IDS:
                continue
            self.accounts.create_alias(alias_label)
            repaired_aliases.append(alias_label)

        active_alias = str(self.state_store.get_active_alias() or "").strip()
        active_reset = False
        if active_alias:
            active_alias_id = normalize_alias_id(active_alias, default="")
            valid_alias_ids = {
                str(item.get("alias_id") or "").strip()
                for item in self.accounts.rebuild_alias_registry()
                if isinstance(item, dict) and str(item.get("alias_id") or "").strip()
            }
            valid_alias_ids.add(DEFAULT_ALIAS_ID)
            if active_alias_id and active_alias_id not in valid_alias_ids:
                self.set_active_alias(DEFAULT_ALIAS_DISPLAY_NAME)
                active_reset = True

        after = self.diagnose_integrity()
        return {
            "before": before,
            "after": after,
            "created_aliases": sorted(dict.fromkeys(repaired_aliases)),
            "active_alias_reset": active_reset,
        }

    def get_active_alias(self) -> str:
        return self._coerce_existing_alias(self.state_store.get_active_alias())

    def set_active_alias(self, alias: str | None) -> str:
        active_alias = self._coerce_existing_alias(alias)
        self.state_store.set_active_alias(active_alias)
        return active_alias

    def create_alias(self, alias: str, *, activate: bool = False) -> dict[str, Any]:
        created = self.accounts.create_alias(alias)
        active_alias = self.set_active_alias(created) if activate else self.get_active_alias()
        return {
            "alias": self.accounts.get_alias_record(created),
            "active_alias": active_alias,
        }

    def rename_alias(
        self,
        current_alias: str,
        new_alias: str,
        *,
        activate_target: bool = True,
        running_tasks: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        current = self._require_existing_alias(current_alias)
        current_alias_id = str(current.get("alias_id") or DEFAULT_ALIAS_ID).strip() or DEFAULT_ALIAS_ID
        if current_alias_id == DEFAULT_ALIAS_ID:
            raise ServiceError("No se puede renombrar el alias default.")
        try:
            requested = alias_record_from_input(new_alias)
        except AliasValidationError as exc:
            raise ServiceError(str(exc)) from exc
        self._assert_aliases_not_in_use(
            [str(current.get("display_name") or ""), requested.display_name],
            running_tasks=running_tasks,
        )
        previous_active_id = self.accounts.resolve_alias_id(self.get_active_alias())
        source_display_name = str(current.get("display_name") or "").strip()
        source_usernames = [
            str(item.get("username") or "").strip()
            for item in self.accounts.list_accounts(source_display_name)
            if str(item.get("username") or "").strip()
        ]
        snapshots = self._capture_state_files()
        try:
            if requested.alias_id == current_alias_id:
                self.warmup.rename_alias_state(source_display_name, requested.display_name)
                self.automation.rename_alias_state(source_display_name, requested.display_name)
                updated = self.accounts.update_alias_display_name(source_display_name, requested.display_name)
                active_alias = self.get_active_alias()
                if activate_target and previous_active_id == current_alias_id:
                    active_alias = self.set_active_alias(str(updated.get("display_name") or DEFAULT_ALIAS_DISPLAY_NAME))
                return {
                    "mode": "display_name_update",
                    "alias": updated,
                    "moved_accounts": 0,
                    "active_alias": active_alias,
                }

            target_display_name = self.accounts.create_alias(requested.display_name)
            self.warmup.rename_alias_state(source_display_name, target_display_name)
            self.automation.rename_alias_state(source_display_name, target_display_name)
            self.accounts.delete_alias(source_display_name, move_accounts_to=target_display_name)
            active_alias = self.get_active_alias()
            if activate_target and previous_active_id == current_alias_id:
                active_alias = self.set_active_alias(target_display_name)
            return {
                "mode": "rekey",
                "source_alias": current,
                "target_alias": self.accounts.get_alias_record(target_display_name),
                "moved_accounts": len(source_usernames),
                "active_alias": active_alias,
            }
        except Exception:
            self._restore_state_files(snapshots)
            raise

    def delete_alias(
        self,
        alias: str,
        *,
        move_accounts_to: str | None = None,
        activate_fallback: bool = True,
        running_tasks: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        current = self._require_existing_alias(alias)
        current_alias_id = str(current.get("alias_id") or DEFAULT_ALIAS_ID).strip() or DEFAULT_ALIAS_ID
        if current_alias_id == DEFAULT_ALIAS_ID:
            raise ServiceError("No se puede eliminar el alias default.")
        previous_active_id = self.accounts.resolve_alias_id(self.get_active_alias())
        requested_target_display_name = ""
        if move_accounts_to:
            try:
                target = alias_record_from_input(move_accounts_to)
            except AliasValidationError as exc:
                raise ServiceError(str(exc)) from exc
            if target.alias_id == current_alias_id:
                raise ServiceError("El alias destino debe ser distinto al alias origen.")
            requested_target_display_name = target.display_name
        self._assert_aliases_not_in_use(
            [str(current.get("display_name") or ""), requested_target_display_name],
            running_tasks=running_tasks,
        )
        target_display_name = (
            self.accounts.create_alias(requested_target_display_name)
            if requested_target_display_name
            else ""
        )

        usernames = [
            str(item.get("username") or "").strip()
            for item in self.accounts.list_accounts(str(current.get("display_name") or ""))
            if str(item.get("username") or "").strip()
        ]
        snapshots = self._capture_state_files()
        try:
            self.warmup.delete_alias_state(str(current.get("display_name") or "")) if not target_display_name else self.warmup.rename_alias_state(str(current.get("display_name") or ""), target_display_name)
            self.automation.delete_alias_state(str(current.get("display_name") or ""), move_to=target_display_name or None)
            self.accounts.delete_alias(str(current.get("display_name") or ""), move_accounts_to=target_display_name or None)

            active_alias = self.get_active_alias()
            if activate_fallback and previous_active_id == current_alias_id:
                active_alias = self.set_active_alias(target_display_name or DEFAULT_ALIAS_DISPLAY_NAME)

            return {
                "deleted_alias": current,
                "target_alias": self.accounts.get_alias_record(target_display_name) if target_display_name else None,
                "moved_accounts": len(usernames),
                "active_alias": active_alias,
            }
        except Exception:
            self._restore_state_files(snapshots)
            raise

    def merge_aliases(
        self,
        source_alias: str,
        target_alias: str,
        *,
        activate_target: bool = True,
        running_tasks: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        source = self._require_existing_alias(source_alias)
        try:
            target = alias_record_from_input(target_alias)
        except AliasValidationError as exc:
            raise ServiceError(str(exc)) from exc
        if source.get("alias_id") == target.alias_id:
            raise ServiceError("El alias origen y destino son equivalentes.")
        self._assert_aliases_not_in_use(
            [str(source.get("display_name") or ""), target.display_name],
            running_tasks=running_tasks,
        )
        target_display_name = self.accounts.create_alias(target.display_name)
        previous_active_id = self.accounts.resolve_alias_id(self.get_active_alias())
        usernames = [
            str(item.get("username") or "").strip()
            for item in self.accounts.list_accounts(str(source.get("display_name") or ""))
            if str(item.get("username") or "").strip()
        ]
        snapshots = self._capture_state_files()
        try:
            self.warmup.rename_alias_state(str(source.get("display_name") or ""), target_display_name)
            self.automation.rename_alias_state(str(source.get("display_name") or ""), target_display_name)
            self.accounts.delete_alias(str(source.get("display_name") or ""), move_accounts_to=target_display_name)
            active_alias = self.get_active_alias()
            if activate_target and previous_active_id == str(source.get("alias_id") or ""):
                active_alias = self.set_active_alias(target_display_name)
            return {
                "source_alias": source,
                "target_alias": self.accounts.get_alias_record(target_display_name),
                "moved_accounts": len(usernames),
                "active_alias": active_alias,
            }
        except Exception:
            self._restore_state_files(snapshots)
            raise

    def _alias_records_by_id(self) -> dict[str, dict[str, Any]]:
        return {
            str(item.get("alias_id") or "").strip(): dict(item)
            for item in self.accounts.list_alias_records()
            if isinstance(item, dict) and str(item.get("alias_id") or "").strip()
        }

    def _coerce_existing_alias(self, alias: str | None) -> str:
        alias_id = self.accounts.resolve_alias_id(alias, default=DEFAULT_ALIAS_ID)
        record = self._alias_records_by_id().get(alias_id)
        if record is None:
            return DEFAULT_ALIAS_DISPLAY_NAME
        return str(record.get("display_name") or DEFAULT_ALIAS_DISPLAY_NAME).strip() or DEFAULT_ALIAS_DISPLAY_NAME

    def _require_existing_alias(self, alias: str | None) -> dict[str, Any]:
        alias_id = self.accounts.resolve_alias_id(alias, default="")
        record = self._alias_records_by_id().get(alias_id)
        if record is None:
            raise ServiceError("El alias indicado no existe.")
        return record

    def _state_files(self) -> list[Path]:
        paths: list[Path] = []
        for candidate in self.accounts.alias_state_paths():
            paths.append(Path(candidate))
        paths.append(Path(self.state_store.db_path))
        for candidate in self.warmup.alias_state_paths():
            paths.append(Path(candidate))
        for candidate in self.automation.alias_state_paths():
            paths.append(Path(candidate))
        unique: list[Path] = []
        seen: set[str] = set()
        for path in paths:
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    def _capture_state_files(self) -> dict[Path, bytes | None]:
        snapshots: dict[Path, bytes | None] = {}
        for path in self._state_files():
            resolved = path.resolve()
            snapshots[resolved] = resolved.read_bytes() if resolved.exists() else None
        return snapshots

    def _restore_state_files(self, snapshots: dict[Path, bytes | None]) -> None:
        for path, payload in snapshots.items():
            if payload is None:
                if path.exists():
                    path.unlink()
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        self.automation.refresh_alias_state_cache()

    def _assert_aliases_not_in_use(
        self,
        aliases: list[str],
        *,
        running_tasks: list[dict[str, Any]] | None = None,
    ) -> None:
        blocked_alias_ids = {
            normalize_alias_id(alias, default="")
            for alias in aliases
            if str(alias or "").strip()
        }
        blocked_alias_ids.discard("")
        if not blocked_alias_ids:
            return
        active_tasks: list[str] = []
        for task in running_tasks or []:
            if not isinstance(task, dict):
                continue
            task_name = str(task.get("name") or "").strip() or "task"
            alias_candidates: list[str] = []
            for key in ("alias", "active_alias", "account_alias"):
                value = str(task.get(key) or "").strip()
                if value:
                    alias_candidates.append(value)
            aliases_value = task.get("aliases")
            if isinstance(aliases_value, (list, tuple, set)):
                alias_candidates.extend(str(item or "").strip() for item in aliases_value if str(item or "").strip())
            matched_alias = next(
                (
                    candidate
                    for candidate in alias_candidates
                    if normalize_alias_id(candidate, default="") in blocked_alias_ids
                ),
                "",
            )
            if matched_alias:
                active_tasks.append(f"{task_name} ({matched_alias})")
        if active_tasks:
            raise ServiceError(
                "No se puede modificar el alias mientras hay tareas activas usandolo: "
                + ", ".join(sorted(dict.fromkeys(active_tasks)))
            )
