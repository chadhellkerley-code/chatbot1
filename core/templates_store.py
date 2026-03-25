from __future__ import annotations

import logging
import shutil
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.storage_atomic import atomic_write_json, load_json_file
from paths import runtime_base, storage_root

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_TEMPLATES_FILENAME = "templates.json"
_STATE_FILENAME = "templates_state.json"


def _default_root_dir() -> Path:
    return runtime_base(Path(__file__).resolve().parent.parent)


def _resolve_root_dir(root_dir: str | Path | None) -> Path:
    if root_dir is None:
        return _default_root_dir()
    return Path(root_dir)


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clone_default(value: Any) -> Any:
    return deepcopy(value)


def _backup_invalid_file(path: Path, *, reason: str) -> Path | None:
    if not path.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    backup_path = path.with_name(f"{path.name}.{reason}.{stamp}.bak")
    counter = 1
    while backup_path.exists():
        backup_path = path.with_name(f"{path.name}.{reason}.{stamp}.{counter}.bak")
        counter += 1
    try:
        shutil.copy2(path, backup_path)
        return backup_path
    except Exception:
        logger.exception("No se pudo respaldar archivo invalido: %s", path)
        return None


def _repair_invalid_structure(
    path: Path,
    default: Any,
    *,
    label: str,
    detail: str,
) -> Any:
    backup_path = _backup_invalid_file(path, reason="json_schema_error")
    logger.error(
        "Estructura JSON invalida en %s%s: %s",
        path,
        f" ({label})" if label else "",
        detail,
    )
    if backup_path is not None:
        logger.error("Respaldo de integridad creado en %s", backup_path)
    repaired = _clone_default(default)
    try:
        atomic_write_json(path, repaired)
    except Exception:
        logger.exception("No se pudo re-crear archivo limpio para %s", path)
    return repaired


def _new_template_id(seen_ids: set[str]) -> str:
    while True:
        candidate = f"tpl_{uuid.uuid4().hex[:12]}"
        if candidate not in seen_ids:
            return candidate


def _normalize_template_item(
    item: dict[str, Any],
    *,
    seen_ids: set[str],
) -> tuple[dict[str, Any] | None, bool]:
    changed = False
    raw_name = item.get("name")
    raw_text = item.get("text")
    name = str(raw_name or "").strip()
    text = str(raw_text or "").strip()
    if not name or not text:
        return None, True
    if str(raw_name or "") != name:
        changed = True
    if str(raw_text or "") != text:
        changed = True

    template_id = str(item.get("id") or "").strip()
    if not template_id or template_id in seen_ids:
        template_id = _new_template_id(seen_ids)
        changed = True
    seen_ids.add(template_id)

    created_at = str(item.get("created_at") or "").strip()
    if not created_at:
        created_at = _timestamp()
        changed = True

    updated_at = str(item.get("updated_at") or "").strip()
    if not updated_at:
        updated_at = created_at
        changed = True

    schema_version = item.get("schema_version")
    if schema_version != _SCHEMA_VERSION:
        changed = True

    normalized = {
        "id": template_id,
        "name": name,
        "text": text,
        "created_at": created_at,
        "updated_at": updated_at,
        "schema_version": _SCHEMA_VERSION,
    }
    return normalized, changed


def _normalize_template_items(items: list[Any]) -> tuple[list[dict[str, Any]], bool]:
    normalized: list[dict[str, Any]] = []
    changed = False
    seen_ids: set[str] = set()
    for raw in items:
        if not isinstance(raw, dict):
            changed = True
            continue
        item, item_changed = _normalize_template_item(raw, seen_ids=seen_ids)
        if item is None:
            changed = True
            continue
        normalized.append(item)
        changed = changed or item_changed
    return normalized, changed


def _normalize_template_state(payload: Any) -> tuple[dict[str, int], bool]:
    clean: dict[str, int] = {}
    changed = not isinstance(payload, dict)
    if not isinstance(payload, dict):
        return clean, changed
    for key, value in payload.items():
        clean_key = str(key).strip()
        if not clean_key:
            changed = True
            continue
        try:
            clean_value = int(value)
        except Exception:
            changed = True
            continue
        clean[clean_key] = clean_value
        if key != clean_key or value != clean_value:
            changed = True
    return clean, changed


def _type_label(expected_type: type[Any]) -> str:
    return getattr(expected_type, "__name__", str(expected_type))


def _load_typed_payload(path: Path, default: Any, *, label: str, expected_type: type[Any]) -> Any:
    payload = load_json_file(path, default, label=label)
    if isinstance(payload, expected_type):
        return payload
    detail = (
        f"se esperaba {_type_label(expected_type)} "
        f"y se recibio {type(payload).__name__}"
    )
    return _repair_invalid_structure(path, default, label=label, detail=detail)


def load_templates_file(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    payload = _load_typed_payload(
        target,
        [],
        label=f"templates.store:{target.name}",
        expected_type=list,
    )
    items, changed = _normalize_template_items(payload)
    if changed:
        atomic_write_json(target, items)
    return items


def save_templates_file(path: str | Path, items: list[dict[str, Any]]) -> None:
    normalized, _changed = _normalize_template_items(list(items or []))
    atomic_write_json(Path(path), normalized)


def load_template_state_file(path: str | Path) -> dict[str, int]:
    target = Path(path)
    payload = _load_typed_payload(
        target,
        {},
        label=f"templates.state:{target.name}",
        expected_type=dict,
    )
    clean, changed = _normalize_template_state(payload)
    if changed:
        atomic_write_json(target, clean)
    return clean


def save_template_state_file(path: str | Path, state: dict[str, int]) -> None:
    clean, _changed = _normalize_template_state(state)
    atomic_write_json(Path(path), clean)


class TemplateStore:
    def __init__(self, root_dir: str | Path | None = None) -> None:
        self.root_dir = _resolve_root_dir(root_dir)
        self.storage_root = storage_root(self.root_dir)
        self.templates_path = self.storage_root / _TEMPLATES_FILENAME
        self.state_path = self.storage_root / _STATE_FILENAME

    def _load_payload(self, path: Path, default: Any, *, label: str, expected_type: type[Any]) -> Any:
        return _load_typed_payload(path, default, label=label, expected_type=expected_type)

    def load_templates(self) -> list[dict[str, Any]]:
        return load_templates_file(self.templates_path)

    def save_templates(self, items: list[dict[str, Any]]) -> None:
        save_templates_file(self.templates_path, items)

    def load_template_state(self) -> dict[str, int]:
        return load_template_state_file(self.state_path)

    def save_template_state(self, state: dict[str, int]) -> None:
        save_template_state_file(self.state_path, state)

    def next_round_robin(
        self,
        account: str,
        template_id: str,
        candidates: list[str],
    ) -> tuple[str, int]:
        if not candidates:
            return "", -1
        state = self.load_template_state()
        key = f"{account}:{template_id}"
        idx = int(state.get(key, -1)) + 1
        if idx >= len(candidates):
            idx = 0
        state[key] = idx
        self.save_template_state(state)
        return candidates[idx], idx


def template_store(root_dir: str | Path | None = None) -> TemplateStore:
    return TemplateStore(root_dir=root_dir)


def load_templates(*, root_dir: str | Path | None = None) -> list[dict[str, Any]]:
    return template_store(root_dir).load_templates()


def save_templates(items: list[dict[str, Any]], *, root_dir: str | Path | None = None) -> None:
    template_store(root_dir).save_templates(items)


def load_template_state(*, root_dir: str | Path | None = None) -> dict[str, int]:
    return template_store(root_dir).load_template_state()


def save_template_state(state: dict[str, int], *, root_dir: str | Path | None = None) -> None:
    template_store(root_dir).save_template_state(state)


def next_round_robin(
    account: str,
    template_id: str,
    candidates: list[str],
    *,
    root_dir: str | Path | None = None,
) -> tuple[str, int]:
    return template_store(root_dir).next_round_robin(account, template_id, candidates)


def render_template(text: str, variables: dict[str, str]) -> str:
    result = text or ""
    for key, value in variables.items():
        token = "{" + key + "}"
        result = result.replace(token, value or "")
    return result
