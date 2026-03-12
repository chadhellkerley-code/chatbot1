from __future__ import annotations

import hashlib
import logging
import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from core import leads as leads_module
from core.leads_import import (
    LeadImportError,
    LeadImportPreview,
    preview_usernames_from_csv,
    preview_usernames_from_txt,
)
from core.leads_store import (
    LeadListStore,
    LeadListStoreError,
)
from core.storage_atomic import atomic_append_jsonl, path_lock
from core.templates_store import (
    TemplateStore,
    load_template_state_file,
    load_templates_file,
    save_template_state_file,
    save_templates_file,
)
from runtime.runtime import request_stop

from .base import ServiceContext, ServiceError, dedupe_usernames, normalize_alias

logger = logging.getLogger(__name__)


class LeadsService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        leads_module.refresh_runtime_paths(self.context.root_dir)
        self._list_store = LeadListStore(self.context.leads_path())
        self._template_store = TemplateStore(self.context.root_dir)
        self._import_operation_lock = threading.RLock()
        self._active_import_lists: set[str] = set()
        self._sync_legacy_storage()

    def _require_list_name(self, name: str) -> str:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ServiceError("Nombre de lista invalido.")
        try:
            return self._list_store.validate_name(clean_name)
        except LeadListStoreError as exc:
            raise ServiceError(str(exc)) from exc
        except OSError as exc:
            raise ServiceError("No se pudo validar el nombre de lista.") from exc

    @staticmethod
    def _storage_error(message: str, _exc: OSError) -> ServiceError:
        return ServiceError(message)

    def validate_list_name(self, name: str) -> str:
        return self._require_list_name(name)

    @staticmethod
    def _operation_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _lead_import_root(self) -> Path:
        root = self.context.storage_path("lead_imports")
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _lead_import_audit_path(self) -> Path:
        return self._lead_import_root() / "audit.jsonl"

    def _lead_import_snapshot_path(self, import_id: str) -> Path:
        snapshots_dir = self._lead_import_root() / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        return snapshots_dir / f"{import_id}.json"

    @staticmethod
    def _import_preview_reader(kind: str):
        if kind == "csv":
            return preview_usernames_from_csv
        if kind == "txt":
            return preview_usernames_from_txt
        raise ServiceError("Tipo de importacion no soportado.")

    def _require_import_file(self, path: str | Path, *, kind: str) -> Path:
        file_path = Path(path)
        if not file_path.is_file():
            raise ServiceError(f"No existe el archivo {kind.upper()}: {file_path}")
        expected_suffix = f".{kind.lower()}"
        if file_path.suffix.lower() != expected_suffix:
            raise ServiceError(f"Selecciona un archivo {expected_suffix}.")
        return file_path

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _import_audit_entries(self) -> list[dict[str, Any]]:
        return self.context.read_jsonl(self._lead_import_audit_path())

    def _append_import_audit(self, payload: dict[str, Any]) -> None:
        entry = {"created_at": self._operation_timestamp(), **payload}
        try:
            atomic_append_jsonl(self._lead_import_audit_path(), entry)
        except Exception:
            logger.exception("No se pudo guardar la auditoria de importacion de leads")

    def _same_file_import_count(self, list_name: str, file_hash: str) -> int:
        count = 0
        for entry in self._import_audit_entries():
            if str(entry.get("event") or "") != "import_success":
                continue
            if str(entry.get("list_name") or "") != list_name:
                continue
            if str(entry.get("file_hash") or "") != file_hash:
                continue
            count += 1
        return count

    @staticmethod
    def _rolled_back_import_ids(entries: list[dict[str, Any]], *, list_name: str) -> set[str]:
        rolled_back: set[str] = set()
        for entry in entries:
            if str(entry.get("event") or "") != "import_rollback":
                continue
            if str(entry.get("list_name") or "") != list_name:
                continue
            import_id = str(entry.get("rolled_back_import_id") or "").strip()
            if import_id:
                rolled_back.add(import_id)
        return rolled_back

    def _latest_restorable_import(self, list_name: str) -> dict[str, Any] | None:
        entries = self._import_audit_entries()
        rolled_back = self._rolled_back_import_ids(entries, list_name=list_name)
        for entry in reversed(entries):
            if str(entry.get("event") or "") != "import_success":
                continue
            if str(entry.get("list_name") or "") != list_name:
                continue
            import_id = str(entry.get("import_id") or "").strip()
            if import_id and import_id not in rolled_back:
                return dict(entry)
        return None

    @staticmethod
    def _numeric_only_ratio(usernames: list[str]) -> float:
        if not usernames:
            return 0.0
        numeric_only = sum(1 for username in usernames if username.isdigit())
        return float(numeric_only) / float(len(usernames))

    @classmethod
    def _apply_import_sanity(cls, payload: dict[str, Any], preview: LeadImportPreview) -> dict[str, Any]:
        warnings: list[str] = []
        blocking_reasons: list[str] = []
        if int(payload.get("duplicate_in_file_count") or 0) > 0:
            warnings.append("El archivo trae usernames duplicados.")
        if int(payload.get("blank_or_invalid_count") or 0) > 0:
            warnings.append("Hay filas vacias o invalidas que se omitiran.")
        if bool(payload.get("is_repeat_file")):
            warnings.append("Este archivo ya fue importado antes en esta lista.")
        if preview.kind == "csv" and preview.used_first_column_fallback and preview.max_columns > 1:
            warnings.append("No se detecto una columna de username; se usara la primera columna del CSV.")
            if cls._numeric_only_ratio(preview.usernames) >= 0.8 and len(preview.usernames) >= 2:
                blocking_reasons.append(
                    "El CSV parece traer IDs en la primera columna en lugar de usernames."
                )
        if int(payload.get("valid_count") or 0) <= 0:
            blocking_reasons.append("No se detectaron usernames validos en el archivo.")
        return {
            **payload,
            "sanity_state": "blocked" if blocking_reasons else "warning" if warnings else "ok",
            "sanity_messages": [*blocking_reasons, *warnings],
            "blocking_reasons": blocking_reasons,
        }

    @classmethod
    def _import_plan_payload(cls, plan: dict[str, Any]) -> dict[str, Any]:
        preview: LeadImportPreview = plan["preview"]
        current_usernames = list(plan["current_usernames"])
        new_usernames = list(plan["new_usernames"])
        valid_count = len(preview.usernames)
        payload = {
            "kind": str(plan["kind"]),
            "list_name": str(plan["list_name"]),
            "file_name": str(plan["file_path"].name),
            "file_path": str(plan["file_path"]),
            "file_hash": str(plan["file_hash"]),
            "encoding": str(preview.encoding),
            "delimiter": str(preview.delimiter),
            "header_detected": bool(preview.header_detected),
            "username_column": str(preview.username_column),
            "selected_column_index": int(preview.selected_column_index),
            "row_count": int(preview.row_count),
            "max_columns": int(preview.max_columns),
            "used_first_column_fallback": bool(preview.used_first_column_fallback),
            "current_count": len(current_usernames),
            "valid_count": valid_count,
            "new_count": len(new_usernames),
            "already_present_count": max(0, valid_count - len(new_usernames)),
            "duplicate_in_file_count": int(preview.duplicate_count),
            "blank_or_invalid_count": int(preview.blank_or_invalid_count),
            "same_file_import_count": int(plan["same_file_import_count"]),
            "is_repeat_file": bool(plan["same_file_import_count"]),
        }
        return cls._apply_import_sanity(payload, preview)

    def _build_import_plan(self, *, list_name: str, path: str | Path, kind: str) -> dict[str, Any]:
        file_path = self._require_import_file(path, kind=kind)
        preview_reader = self._import_preview_reader(kind)
        preview = preview_reader(file_path)
        current_usernames = list(self._list_store.load(list_name))
        current_keys = {username.lower() for username in current_usernames}
        new_usernames = [username for username in preview.usernames if username.lower() not in current_keys]
        file_hash = self._file_sha256(file_path)
        return {
            "kind": kind,
            "list_name": list_name,
            "file_path": file_path,
            "file_hash": file_hash,
            "preview": preview,
            "current_usernames": current_usernames,
            "new_usernames": new_usernames,
            "same_file_import_count": self._same_file_import_count(list_name, file_hash),
        }

    def _write_import_snapshot(
        self,
        *,
        import_id: str,
        list_name: str,
        list_existed: bool,
        usernames: list[str],
    ) -> Path:
        snapshot_path = self._lead_import_snapshot_path(import_id)
        self.context.write_json(
            snapshot_path,
            {
                "import_id": import_id,
                "list_name": list_name,
                "list_existed": bool(list_existed),
                "usernames": list(usernames),
                "created_at": self._operation_timestamp(),
            },
        )
        return snapshot_path

    def _enter_import_operation(self, list_name: str) -> None:
        with self._import_operation_lock:
            if list_name in self._active_import_lists:
                raise ServiceError("Ya hay una importacion en curso para esta lista.")
            self._active_import_lists.add(list_name)

    def _leave_import_operation(self, list_name: str) -> None:
        with self._import_operation_lock:
            self._active_import_lists.discard(list_name)

    def _record_import_failure(
        self,
        *,
        kind: str,
        list_name: str,
        path: str | Path,
        message: str,
    ) -> None:
        file_path = Path(path)
        self._append_import_audit(
            {
                "event": "import_failed",
                "kind": kind,
                "list_name": list_name,
                "file_name": file_path.name,
                "file_path": str(file_path),
                "error": str(message or "").strip(),
            }
        )

    @staticmethod
    def _event_in_last_days(entry: dict[str, Any], *, days: int) -> bool:
        raw_timestamp = str(entry.get("created_at") or "").strip()
        if not raw_timestamp:
            return False
        try:
            timestamp = datetime.fromisoformat(raw_timestamp)
        except ValueError:
            return False
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp >= datetime.now(timezone.utc) - timedelta(days=days)

    @classmethod
    def _import_metrics(cls, entries: list[dict[str, Any]]) -> dict[str, int]:
        def _count(event_name: str, *, recent: bool) -> int:
            return sum(
                1
                for entry in entries
                if str(entry.get("event") or "") == event_name
                and (not recent or cls._event_in_last_days(entry, days=7))
            )

        return {
            "success_total": _count("import_success", recent=False),
            "failed_total": _count("import_failed", recent=False),
            "rollback_total": _count("import_rollback", recent=False),
            "success_last_7d": _count("import_success", recent=True),
            "failed_last_7d": _count("import_failed", recent=True),
            "rollback_last_7d": _count("import_rollback", recent=True),
        }

    @staticmethod
    def _format_import_event_summary(event: dict[str, Any] | None) -> str:
        if not isinstance(event, dict):
            return (
                "Importa archivos CSV o TXT en una lista existente o crea una nueva "
                "escribiendo su nombre en el destino."
            )
        event_type = str(event.get("event") or "").strip()
        list_name = str(event.get("list_name") or "-").strip() or "-"
        if event_type == "import_success":
            return (
                f"Ultimo import: {list_name}  |  "
                f"Nuevos: {int(event.get('new_count') or 0)}  |  "
                f"Ya estaban: {int(event.get('already_present_count') or 0)}  |  "
                f"Duplicados en archivo: {int(event.get('duplicate_in_file_count') or 0)}  |  "
                f"Invalidos/vacios: {int(event.get('blank_or_invalid_count') or 0)}"
            )
        if event_type == "import_rollback":
            return (
                f"Ultimo movimiento: rollback en {list_name}  |  "
                f"Restaurados: {int(event.get('restored_count') or 0)}"
            )
        if event_type == "import_failed":
            message = str(event.get("error") or "Error desconocido").strip()
            return f"Ultimo intento fallido: {list_name}  |  {message}"
        return (
            "Importa archivos CSV o TXT en una lista existente o crea una nueva "
            "escribiendo su nombre en el destino."
        )

    def import_status_snapshot(self) -> dict[str, Any]:
        entries = self._import_audit_entries()
        latest_event = dict(entries[-1]) if entries else None
        metrics = self._import_metrics(entries)
        summary = self._format_import_event_summary(latest_event)
        if entries:
            summary += (
                "\n"
                f"7d: ok {metrics['success_last_7d']}  |  "
                f"fallidos {metrics['failed_last_7d']}  |  "
                f"rollbacks {metrics['rollback_last_7d']}"
            )
        return {
            "latest_event": latest_event,
            "metrics": metrics,
            "summary": summary,
        }

    @staticmethod
    def _template_variants(text: str) -> list[str]:
        return [line.strip() for line in str(text or "").splitlines() if line.strip()]

    @staticmethod
    def _template_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _legacy_leads_path(self) -> Path:
        return Path(self.context.root_dir) / "storage" / "leads"

    def _legacy_filter_path(self) -> Path:
        return Path(self.context.root_dir) / "storage" / "lead_filters"

    def _template_storage_roots(self) -> list[Path]:
        candidates = [
            self.context.storage_path(),
            Path(self.context.root_dir) / "storage",
            Path(self.context.root_dir) / "data",
        ]
        roots: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            try:
                key = str(candidate.resolve())
            except Exception:
                key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            roots.append(candidate)
        return roots

    @staticmethod
    def _template_record_timestamp(item: dict[str, Any]) -> datetime:
        raw = str(item.get("updated_at") or item.get("created_at") or "").strip()
        if not raw:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _merge_template_records(
        cls,
        primary: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
    ) -> bool:
        changed = False
        by_id: dict[str, int] = {}
        by_name: dict[str, int] = {}
        for index, item in enumerate(primary):
            template_id = str(item.get("id") or "").strip()
            name_key = str(item.get("name") or "").strip().lower()
            if template_id:
                by_id[template_id] = index
            if name_key:
                by_name[name_key] = index

        for item in incoming:
            incoming_id = str(item.get("id") or "").strip()
            incoming_name = str(item.get("name") or "").strip()
            name_key = incoming_name.lower()
            target_index = by_id.get(incoming_id) if incoming_id else None
            if target_index is None and name_key:
                target_index = by_name.get(name_key)
            if target_index is None:
                primary.append(dict(item))
                new_index = len(primary) - 1
                if incoming_id:
                    by_id[incoming_id] = new_index
                if name_key:
                    by_name[name_key] = new_index
                changed = True
                continue

            current = primary[target_index]
            if cls._template_record_timestamp(item) <= cls._template_record_timestamp(current):
                continue

            current_id = str(current.get("id") or "").strip()
            current_name_key = str(current.get("name") or "").strip().lower()
            primary[target_index] = dict(item)
            if current_id and current_id != incoming_id and by_id.get(current_id) == target_index:
                by_id.pop(current_id, None)
            if current_name_key and current_name_key != name_key and by_name.get(current_name_key) == target_index:
                by_name.pop(current_name_key, None)
            if incoming_id:
                by_id[incoming_id] = target_index
            if name_key:
                by_name[name_key] = target_index
            changed = True
        return changed

    @staticmethod
    def _merge_template_state(primary: dict[str, int], incoming: dict[str, int]) -> bool:
        changed = False
        for key, value in incoming.items():
            clean_key = str(key).strip()
            if not clean_key:
                continue
            clean_value = int(value)
            current_value = primary.get(clean_key)
            if current_value is None or clean_value > int(current_value):
                primary[clean_key] = clean_value
                changed = True
        return changed

    def _sync_legacy_storage(self) -> None:
        self._sync_legacy_lead_lists()
        self._sync_legacy_filter_storage()
        self._sync_legacy_template_storage()
        leads_module.refresh_runtime_paths(self.context.root_dir)
        self._list_store = LeadListStore(self.context.leads_path())

    def _sync_legacy_lead_lists(self) -> None:
        primary = self.context.leads_path()
        legacy = self._legacy_leads_path()
        try:
            if primary.resolve() == legacy.resolve():
                return
        except Exception:
            pass
        if not legacy.exists():
            return

        primary_store = LeadListStore(primary)
        legacy_store = LeadListStore(legacy)
        primary_names = set(primary_store.list_names())
        legacy_names = legacy_store.list_names()
        if not legacy_names:
            return

        if not primary_names:
            for name in legacy_names:
                primary_store.save(name, legacy_store.load(name))
            return

        for name in legacy_names:
            if name in primary_names:
                continue
            primary_store.save(name, legacy_store.load(name))

    def _sync_legacy_filter_storage(self) -> None:
        primary_root = self.context.storage_path("lead_filters")
        legacy_root = self._legacy_filter_path()
        try:
            if primary_root.resolve() == legacy_root.resolve():
                return
        except Exception:
            pass
        if not legacy_root.exists():
            return

        primary_root.mkdir(parents=True, exist_ok=True)
        primary_lists_dir = primary_root / "lists"
        primary_lists_dir.mkdir(parents=True, exist_ok=True)
        legacy_lists_dir = legacy_root / "lists"

        primary_has_lists = any(primary_lists_dir.glob("*.json"))
        if not primary_has_lists and legacy_lists_dir.exists():
            for candidate in legacy_lists_dir.glob("*.json"):
                target = primary_lists_dir / candidate.name
                if target.exists():
                    continue
                shutil.copy2(candidate, target)

        for relative_name in ("filters_config.json", "account_http_meta.json"):
            legacy_file = legacy_root / relative_name
            primary_file = primary_root / relative_name
            if primary_file.exists() or not legacy_file.exists():
                continue
            shutil.copy2(legacy_file, primary_file)

    def _sync_legacy_template_storage(self) -> None:
        roots = self._template_storage_roots()
        if not roots:
            return

        primary_root = roots[0]
        primary_templates_path = primary_root / "templates.json"
        primary_state_path = primary_root / "templates_state.json"
        merged_templates = load_templates_file(primary_templates_path)
        merged_state = load_template_state_file(primary_state_path)
        templates_changed = False
        state_changed = False

        for candidate_root in roots[1:]:
            candidate_templates_path = candidate_root / "templates.json"
            if candidate_templates_path.exists():
                candidate_templates = load_templates_file(candidate_templates_path)
                if self._merge_template_records(merged_templates, candidate_templates):
                    templates_changed = True

            candidate_state_path = candidate_root / "templates_state.json"
            if candidate_state_path.exists():
                candidate_state = load_template_state_file(candidate_state_path)
                if self._merge_template_state(merged_state, candidate_state):
                    state_changed = True

        if templates_changed:
            save_templates_file(primary_templates_path, merged_templates)
        if state_changed:
            save_template_state_file(primary_state_path, merged_state)

    def default_filter_config(self) -> dict[str, Any]:
        text_defaults = getattr(leads_module, "_default_text_engine_thresholds_payload", None)
        image_defaults = getattr(leads_module, "_default_image_engine_thresholds_payload", None)
        return {
            "classic": {
                "min_followers": 0,
                "min_posts": 0,
                "privacy": "any",
                "link_in_bio": "any",
                "include_keywords": [],
                "exclude_keywords": [],
                "language": "any",
                "min_followers_state": leads_module.FILTER_STATE_DISABLED,
                "min_posts_state": leads_module.FILTER_STATE_DISABLED,
                "privacy_state": leads_module.FILTER_STATE_DISABLED,
                "link_in_bio_state": leads_module.FILTER_STATE_DISABLED,
                "include_keywords_state": leads_module.FILTER_STATE_DISABLED,
                "exclude_keywords_state": leads_module.FILTER_STATE_DISABLED,
                "language_state": leads_module.FILTER_STATE_DISABLED,
            },
            "text": {
                "enabled": False,
                "criteria": "",
                "model_path": "",
                "state": leads_module.FILTER_STATE_DISABLED,
                "engine_thresholds": text_defaults() if callable(text_defaults) else {},
            },
            "image": {
                "enabled": False,
                "prompt": "",
                "state": leads_module.FILTER_STATE_DISABLED,
                "engine_thresholds": image_defaults() if callable(image_defaults) else {},
            },
        }

    def list_lists(self) -> list[str]:
        return list(self._list_store.list_names())

    def list_list_summaries(self) -> list[dict[str, Any]]:
        try:
            rows = self._list_store.list_summaries()
        except OSError as exc:
            raise self._storage_error("No se pudieron leer los metadatos de listas de leads.", exc) from exc
        summaries: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            summaries.append(
                {
                    "name": name,
                    "count": max(0, int(row.get("count") or 0)),
                }
            )
        return summaries

    def get_list_summary(self, name: str) -> dict[str, Any]:
        clean_name = self._require_list_name(name)
        try:
            payload = self._list_store.summary(clean_name)
        except OSError as exc:
            raise self._storage_error("No se pudo leer el resumen de la lista de leads.", exc) from exc
        return {
            "name": clean_name,
            "count": max(0, int(payload.get("count") or 0)) if isinstance(payload, dict) else 0,
        }

    def load_list(self, name: str) -> list[str]:
        clean_name = self._require_list_name(name)
        try:
            return list(self._list_store.load(clean_name))
        except OSError as exc:
            raise self._storage_error("No se pudo leer la lista de leads.", exc) from exc

    def save_list(self, name: str, usernames: list[str]) -> None:
        clean_name = self._require_list_name(name)
        try:
            self._list_store.save(clean_name, dedupe_usernames(usernames))
        except OSError as exc:
            raise self._storage_error("No se pudo guardar la lista de leads.", exc) from exc

    def delete_list(self, name: str) -> None:
        clean_name = self._require_list_name(name)
        try:
            self._list_store.delete(clean_name)
        except OSError as exc:
            raise self._storage_error("No se pudo eliminar la lista de leads.", exc) from exc

    def preview_csv(self, path: str | Path, name: str) -> dict[str, Any]:
        clean_name = self._require_list_name(str(name))
        try:
            plan = self._build_import_plan(list_name=clean_name, path=path, kind="csv")
        except LeadImportError as exc:
            raise ServiceError(str(exc)) from exc
        except OSError as exc:
            raise self._storage_error("No se pudo analizar el CSV de leads.", exc) from exc
        return self._import_plan_payload(plan)

    def preview_txt(self, path: str | Path, name: str) -> dict[str, Any]:
        clean_name = self._require_list_name(str(name))
        try:
            plan = self._build_import_plan(list_name=clean_name, path=path, kind="txt")
        except LeadImportError as exc:
            raise ServiceError(str(exc)) from exc
        except OSError as exc:
            raise self._storage_error("No se pudo analizar el TXT de leads.", exc) from exc
        return self._import_plan_payload(plan)

    def _import_file(self, path: str | Path, name: str, *, kind: str) -> dict[str, Any]:
        clean_name = self._require_list_name(str(name))
        self._enter_import_operation(clean_name)
        try:
            try:
                plan = self._build_import_plan(list_name=clean_name, path=path, kind=kind)
                list_path = self._list_store.path_for(clean_name)
                with path_lock(list_path):
                    current_usernames = list(self._list_store.load(clean_name))
                    current_keys = {username.lower() for username in current_usernames}
                    new_usernames = [
                        username
                        for username in plan["preview"].usernames
                        if username.lower() not in current_keys
                    ]
                    same_file_import_count = self._same_file_import_count(clean_name, str(plan["file_hash"]))
                    payload = self._import_plan_payload(
                        {
                            **plan,
                            "current_usernames": current_usernames,
                            "new_usernames": new_usernames,
                            "same_file_import_count": same_file_import_count,
                        }
                    )
                    if str(payload.get("sanity_state") or "") == "blocked":
                        messages = payload.get("blocking_reasons") or payload.get("sanity_messages") or []
                        reason = str(messages[0] if messages else "El archivo no es seguro para importar.").strip()
                        raise ServiceError(reason)
                    if int(payload["new_count"]) <= 0:
                        if bool(payload["is_repeat_file"]):
                            raise ServiceError(
                                "Este archivo ya fue importado en esta lista y no aporta nuevos leads."
                            )
                        raise ServiceError("El archivo no aporta nuevos leads para esta lista.")

                    snapshot_path = self._write_import_snapshot(
                        import_id=str(uuid4().hex),
                        list_name=clean_name,
                        list_existed=list_path.exists(),
                        usernames=current_usernames,
                    )
                    self._list_store.append(clean_name, plan["preview"].usernames)

                result = {
                    **payload,
                    "snapshot_path": str(snapshot_path),
                    "resulting_count": int(payload["current_count"]) + int(payload["new_count"]),
                }
                self._append_import_audit(
                    {
                        "event": "import_success",
                        "import_id": snapshot_path.stem,
                        **result,
                    }
                )
                return result
            except LeadImportError as exc:
                self._record_import_failure(kind=kind, list_name=clean_name, path=path, message=str(exc))
                raise ServiceError(str(exc)) from exc
            except ServiceError as exc:
                self._record_import_failure(kind=kind, list_name=clean_name, path=path, message=str(exc))
                raise
            except OSError as exc:
                self._record_import_failure(kind=kind, list_name=clean_name, path=path, message=str(exc))
                raise self._storage_error(
                    f"No se pudo importar el {kind.upper()} en la lista de leads.",
                    exc,
                ) from exc
        finally:
            self._leave_import_operation(clean_name)

    def import_csv(self, path: str | Path, name: str) -> dict[str, Any]:
        return self._import_file(path, name, kind="csv")

    def import_txt(self, path: str | Path, name: str) -> dict[str, Any]:
        return self._import_file(path, name, kind="txt")

    def rollback_last_import(self, name: str) -> dict[str, Any]:
        clean_name = self._require_list_name(name)
        self._enter_import_operation(clean_name)
        try:
            target = self._latest_restorable_import(clean_name)
            if target is None:
                raise ServiceError("No hay una importacion reciente para deshacer en esta lista.")

            import_id = str(target.get("import_id") or "").strip()
            snapshot_path = self._lead_import_snapshot_path(import_id)
            if not snapshot_path.is_file():
                raise ServiceError("No se encontro el snapshot del ultimo import.")

            snapshot = self.context.read_json(snapshot_path, default={})
            if not isinstance(snapshot, dict):
                raise ServiceError("No se pudo leer el snapshot del ultimo import.")

            usernames = snapshot.get("usernames")
            if not isinstance(usernames, list):
                raise ServiceError("El snapshot del ultimo import es invalido.")

            list_path = self._list_store.path_for(clean_name)
            with path_lock(list_path):
                current_usernames = list(self._list_store.load(clean_name))
                list_existed = bool(snapshot.get("list_existed"))
                if not list_existed and not usernames:
                    self._list_store.delete(clean_name)
                else:
                    self._list_store.save(clean_name, usernames)

            result = {
                "list_name": clean_name,
                "rolled_back_import_id": import_id,
                "previous_count": len(current_usernames),
                "restored_count": len(usernames),
            }
            self._append_import_audit({"event": "import_rollback", **result})
            return result
        except OSError as exc:
            raise self._storage_error("No se pudo deshacer el ultimo import de leads.", exc) from exc
        finally:
            self._leave_import_operation(clean_name)

    def add_manual(self, name: str, usernames: list[str]) -> None:
        clean_name = self._require_list_name(name)
        try:
            self._list_store.append(clean_name, dedupe_usernames(usernames))
        except OSError as exc:
            raise self._storage_error("No se pudieron agregar usernames a la lista de leads.", exc) from exc

    def list_templates(self) -> list[dict[str, Any]]:
        return self._template_store.load_templates()

    def list_template_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in self.list_templates():
            template_id = str(item.get("id") or "").strip()
            name = str(item.get("name") or "").strip()
            text = str(item.get("text") or "").strip()
            if not template_id or not name or not text:
                continue
            variants = self._template_variants(text)
            preview = variants[0] if variants else ""
            rows.append(
                {
                    "id": template_id,
                    "name": name,
                    "text": text,
                    "variants": variants,
                    "variant_count": len(variants),
                    "preview": preview,
                    "created_at": str(item.get("created_at") or ""),
                    "updated_at": str(item.get("updated_at") or ""),
                    "schema_version": item.get("schema_version"),
                }
            )
        rows.sort(key=lambda item: str(item.get("name") or "").lower())
        return rows

    def load_template(self, template_id: str) -> dict[str, Any] | None:
        clean_id = str(template_id or "").strip()
        if not clean_id:
            return None
        for row in self.list_template_rows():
            if str(row.get("id") or "").strip() == clean_id:
                return dict(row)
        return None

    def upsert_template(
        self,
        name: str,
        text: str,
        *,
        template_id: str = "",
    ) -> dict[str, Any]:
        clean_name = str(name or "").strip()
        clean_text = str(text or "").strip()
        clean_id = str(template_id or "").strip()
        if not clean_name or not clean_text or not self._template_variants(clean_text):
            raise ServiceError("La plantilla requiere nombre y texto.")
        templates = self._template_store.load_templates()
        target_index = -1
        current_record: dict[str, Any] | None = None

        for index, item in enumerate(templates):
            current_id = str(item.get("id") or "").strip()
            current_name = str(item.get("name") or "").strip().lower()
            if clean_id and current_id == clean_id:
                target_index = index
                current_record = dict(item)
                continue
            if current_name == clean_name.lower():
                raise ServiceError("Ya existe una plantilla con ese nombre.")

        if clean_id and current_record is None:
            raise ServiceError("No se encontro la plantilla seleccionada.")

        if current_record is None:
            templates.append(
                {
                    "name": clean_name,
                    "text": clean_text,
                    "created_at": self._template_timestamp(),
                    "updated_at": self._template_timestamp(),
                }
            )
        else:
            templates[target_index] = {
                "id": str(current_record.get("id") or clean_id),
                "name": clean_name,
                "text": clean_text,
                "created_at": str(current_record.get("created_at") or self._template_timestamp()),
                "updated_at": self._template_timestamp(),
                "schema_version": current_record.get("schema_version", 1),
            }
        self._template_store.save_templates(templates)
        saved = templates[target_index] if current_record is not None else templates[-1]
        saved_id = str(saved.get("id") or "").strip()
        if not saved_id:
            persisted = [
                item
                for item in self._template_store.load_templates()
                if str(item.get("name") or "").strip().lower() == clean_name.lower()
            ]
            if persisted:
                return dict(persisted[-1])
        return dict(self.load_template(saved_id) or saved)

    def delete_template(self, template_id: str) -> int:
        clean_id = str(template_id or "").strip()
        if not clean_id:
            return 0
        templates = self._template_store.load_templates()
        kept = [item for item in templates if str(item.get("id") or "").strip() != clean_id]
        if len(kept) == len(templates):
            return 0
        self._template_store.save_templates(kept)
        return 1

    def load_filter_config(self) -> dict[str, Any]:
        cfg = leads_module._load_filter_config()
        if cfg is None:
            return {}
        return leads_module._filter_config_to_dict(cfg)

    def effective_filter_config(self) -> dict[str, Any]:
        payload = self.load_filter_config()
        if payload:
            return payload
        return self.default_filter_config()

    def save_filter_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = leads_module._filter_config_from_dict(dict(payload or {}))
        if cfg is None:
            raise ServiceError("Configuracion de filtrado invalida.")
        leads_module._save_filter_config(cfg)
        return leads_module._filter_config_to_dict(cfg)

    def delete_filter_config(self) -> bool:
        path = leads_module.FILTER_CONFIG_PATH
        if not path.exists():
            return False
        path.unlink()
        return True

    def list_filter_lists(self, *, status: str | None = None) -> list[dict[str, Any]]:
        rows = leads_module._load_filter_lists()
        for row in rows:
            leads_module._refresh_list_stats(row)
            row["pending"] = int(leads_module._pending_count(row))
            row["source_list"] = str(row.get("source_list") or row.get("list_name") or "")
        if not status:
            return rows
        status_value = str(status or "").strip().lower()
        if status_value == "completed":
            return [item for item in rows if leads_module._pending_count(item) == 0]
        if status_value == "incomplete":
            return [item for item in rows if leads_module._pending_count(item) > 0]
        return rows

    def list_filter_list_summaries(self, *, status: str | None = None) -> list[dict[str, Any]]:
        rows = leads_module._load_filter_list_summaries(status=status)
        for row in rows:
            row["pending"] = int(row.get("pending") or 0)
            row["source_list"] = str(row.get("source_list") or row.get("list_name") or "")
            row["processed"] = int(row.get("processed") or 0)
            row["qualified"] = int(row.get("qualified") or 0)
            row["discarded"] = int(row.get("discarded") or 0)
            row["total"] = int(row.get("total") or 0)
            row["errors"] = int(row.get("errors") or 0)
        return rows

    def create_filter_list(
        self,
        usernames: list[str],
        *,
        export_alias: str,
        filters: dict[str, Any],
        run: dict[str, Any],
        source_list: str = "",
    ) -> dict[str, Any]:
        clean_usernames = dedupe_usernames(usernames)
        if not clean_usernames:
            raise ServiceError("No hay usernames para filtrar.")
        filter_cfg = leads_module._filter_config_from_dict(dict(filters or {}))
        if filter_cfg is None:
            raise ServiceError("Configuracion de filtros invalida.")
        run_cfg = leads_module._run_config_from_dict(dict(run or {}))
        if run_cfg is None:
            raise ServiceError("Configuracion de ejecucion invalida.")
        data = leads_module._create_filter_list(clean_usernames)
        data["export_alias"] = normalize_alias(export_alias, default="leads_filtrados")
        data["filters"] = leads_module._filter_config_to_dict(filter_cfg)
        data["run"] = leads_module._run_config_to_dict(run_cfg)
        data["source_list"] = str(source_list or "").strip()
        leads_module._save_filter_list(data)
        return data

    def create_filter_list_from_source(
        self,
        source_list: str,
        *,
        export_alias: str,
        filters: dict[str, Any],
        run: dict[str, Any],
    ) -> dict[str, Any]:
        clean_source = str(source_list or "").strip()
        usernames = self.load_list(clean_source)
        if not usernames:
            raise ServiceError("La lista origen no tiene usernames para filtrar.")
        return self.create_filter_list(
            usernames,
            export_alias=export_alias,
            filters=filters,
            run=run,
            source_list=clean_source,
        )

    def find_filter_list(self, list_id: str) -> dict[str, Any]:
        clean_id = str(list_id or "").strip()
        if not clean_id:
            raise ServiceError("ID de lista invalido.")
        item = leads_module._load_filter_list_by_id(clean_id)
        if item:
            leads_module._refresh_list_stats(item)
            item["pending"] = int(leads_module._pending_count(item))
            item["source_list"] = str(item.get("source_list") or item.get("list_name") or "")
            return item
        raise ServiceError(f"No se encontro la lista de filtrado {clean_id}.")

    def delete_filter_list(self, list_id: str) -> None:
        item = self.find_filter_list(list_id)
        leads_module._delete_filter_list(item)

    def filter_list_result_rows(self, list_id: str) -> list[dict[str, Any]]:
        list_data = self.find_filter_list(list_id)
        rows = [dict(item) for item in list_data.get("items") or [] if isinstance(item, dict)]
        rows.sort(
            key=lambda item: (
                str(item.get("updated_at") or ""),
                str(item.get("username") or ""),
            ),
            reverse=True,
        )
        return rows

    def update_filter_list_settings(
        self,
        list_id: str,
        *,
        filters: dict[str, Any],
        run: dict[str, Any],
        export_alias: str = "",
    ) -> dict[str, Any]:
        list_data = self.find_filter_list(list_id)
        filter_cfg = leads_module._filter_config_from_dict(dict(filters or {}))
        if filter_cfg is None:
            raise ServiceError("Configuracion de filtros invalida.")
        run_cfg = leads_module._run_config_from_dict(dict(run or {}))
        if run_cfg is None:
            raise ServiceError("Configuracion de ejecucion invalida.")
        list_data["filters"] = leads_module._filter_config_to_dict(filter_cfg)
        list_data["run"] = leads_module._run_config_to_dict(run_cfg)
        if export_alias:
            list_data["export_alias"] = normalize_alias(export_alias, default="leads_filtrados")
        leads_module._save_filter_list(list_data)
        return list_data

    def restart_filter_list(
        self,
        list_id: str,
        *,
        filters: dict[str, Any],
        run: dict[str, Any],
        export_alias: str = "",
    ) -> dict[str, Any]:
        list_data = self.find_filter_list(list_id)
        usernames = [
            str(item.get("username") or "").strip()
            for item in list_data.get("items") or []
            if isinstance(item, dict) and str(item.get("username") or "").strip()
        ]
        return self.create_filter_list(
            usernames,
            export_alias=export_alias or str(list_data.get("export_alias") or ""),
            filters=filters,
            run=run,
            source_list=str(list_data.get("source_list") or ""),
        )

    def finalize_stopped_filter_list(
        self,
        list_id: str,
        *,
        action: str,
        export_alias: str = "",
    ) -> dict[str, Any]:
        list_data = self.find_filter_list(list_id)
        leads_module._refresh_list_stats(list_data)
        action_value = str(action or "keep").strip().lower() or "keep"
        alias = normalize_alias(
            export_alias or str(list_data.get("export_alias") or ""),
            default="leads_filtrados",
        )
        qualified = leads_module._collect_qualified_usernames(list_data)

        if action_value == "export":
            list_data["export_alias"] = alias
            leads_module._save_filter_list(list_data)
            if qualified:
                leads_module._export_to_alias(alias, qualified)
            return {
                "action": action_value,
                "alias": alias,
                "exported": len(qualified),
                "processed": int(list_data.get("processed") or 0),
                "qualified": int(list_data.get("qualified") or 0),
                "discarded": int(list_data.get("discarded") or 0),
                "pending": int(leads_module._pending_count(list_data)),
            }

        if action_value == "keep":
            leads_module._save_filter_list(list_data)
            return {
                "action": action_value,
                "alias": alias,
                "exported": 0,
                "processed": int(list_data.get("processed") or 0),
                "qualified": int(list_data.get("qualified") or 0),
                "discarded": int(list_data.get("discarded") or 0),
                "pending": int(leads_module._pending_count(list_data)),
            }

        if action_value == "delete":
            leads_module._delete_filter_list(list_data)
            return {
                "action": action_value,
                "alias": alias,
                "exported": 0,
                "processed": int(list_data.get("processed") or 0),
                "qualified": int(list_data.get("qualified") or 0),
                "discarded": int(list_data.get("discarded") or 0),
                "pending": int(leads_module._pending_count(list_data)),
            }

        raise ServiceError("Accion final de filtrado invalida.")

    def stop_filtering(
        self,
        reason: str = "stop requested from leads GUI",
        *,
        task_runner: Any | None = None,
    ) -> None:
        clean_reason = str(reason or "").strip() or "stop requested from leads GUI"
        request_task_stop = getattr(task_runner, "request_task_stop", None)
        if callable(request_task_stop):
            request_task_stop("leads_filter", clean_reason)
            return
        request_stop(clean_reason)

    def execute_filter_list(self, list_id: str) -> dict[str, Any]:
        list_data = self.find_filter_list(list_id)
        filter_cfg = leads_module._filter_config_from_dict(list_data.get("filters") or {})
        if filter_cfg is None:
            filter_cfg = leads_module._load_filter_config()
        if filter_cfg is None:
            raise ServiceError("No hay filtros configurados para ejecutar.")
        run_cfg = leads_module._run_config_from_dict(list_data.get("run") or {})
        if run_cfg is None:
            raise ServiceError("No hay configuracion de ejecucion para la lista.")

        leads_module._verify_dependencies_for_run(filter_cfg)
        if not list_data.get("export_alias"):
            list_data["export_alias"] = normalize_alias(
                "leads_filtrados",
                default="leads_filtrados",
            )
            leads_module._save_filter_list(list_data)

        print("Ejecutando filtrado... (presiona Q para detener)")
        print("Inicializando cuentas y scheduler...")
        stopped = leads_module._run_async(
            leads_module._execute_filter_list_async(list_data, filter_cfg, run_cfg)
        )
        leads_module._save_filter_list(list_data)
        if not stopped:
            leads_module._auto_export_on_complete(list_data)
        return {
            "list_id": str(list_data.get("id") or ""),
            "source_list": str(list_data.get("source_list") or ""),
            "export_alias": str(list_data.get("export_alias") or ""),
            "stopped": bool(stopped),
            "processed": int(list_data.get("processed") or 0),
            "qualified": int(list_data.get("qualified") or 0),
            "discarded": int(list_data.get("discarded") or 0),
            "pending": int(leads_module._pending_count(list_data)),
        }
