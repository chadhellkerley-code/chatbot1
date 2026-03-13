from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

DEFAULT_ALIAS_ID = "default"
DEFAULT_ALIAS_DISPLAY_NAME = "default"
RESERVED_ALIAS_IDS = frozenset({"all"})
SYSTEM_ALIAS_IDS = frozenset({DEFAULT_ALIAS_ID})
_MAX_ALIAS_LENGTH = 64
_SEPARATOR_PATTERN = re.compile(r"[\s\-_.]+", re.UNICODE)


class AliasValidationError(ValueError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_alias_display(value: Any, *, default: str = "") -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text or str(default or "")


def normalize_alias_id(value: Any, *, default: str = DEFAULT_ALIAS_ID) -> str:
    display_name = normalize_alias_display(value)
    if not display_name:
        return str(default or "")
    alias_id = display_name.casefold()
    alias_id = re.sub(r"[^\w\s\-_.]+", "-", alias_id, flags=re.UNICODE)
    alias_id = re.sub(r"\s+", "-", alias_id, flags=re.UNICODE)
    alias_id = _SEPARATOR_PATTERN.sub("-", alias_id)
    alias_id = alias_id.strip("-")
    return alias_id or str(default or "")


def validate_alias_display_name(value: Any, *, allow_system_alias: bool = False) -> tuple[str, str]:
    display_name = normalize_alias_display(value)
    if not display_name:
        raise AliasValidationError("Ingresa un alias valido.")
    if len(display_name) > _MAX_ALIAS_LENGTH:
        raise AliasValidationError(f"El alias no puede superar {_MAX_ALIAS_LENGTH} caracteres.")
    alias_id = normalize_alias_id(display_name, default="")
    if not alias_id:
        raise AliasValidationError("El alias debe contener letras o numeros.")
    if len(alias_id) > _MAX_ALIAS_LENGTH:
        raise AliasValidationError(f"El alias no puede superar {_MAX_ALIAS_LENGTH} caracteres.")
    if alias_id in SYSTEM_ALIAS_IDS and not allow_system_alias:
        raise AliasValidationError(f"El alias '{display_name}' esta reservado por el sistema.")
    if alias_id in RESERVED_ALIAS_IDS:
        raise AliasValidationError(f"El alias '{display_name}' esta reservado por el sistema.")
    return alias_id, display_name


@dataclass(frozen=True)
class AliasRecord:
    alias_id: str
    display_name: str
    created_at: str
    updated_at: str
    system: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "alias_id": self.alias_id,
            "display_name": self.display_name,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "system": self.system,
        }


def default_alias_record() -> AliasRecord:
    now_iso = _utc_now_iso()
    return AliasRecord(
        alias_id=DEFAULT_ALIAS_ID,
        display_name=DEFAULT_ALIAS_DISPLAY_NAME,
        created_at=now_iso,
        updated_at=now_iso,
        system=True,
    )


def alias_record_from_input(value: Any, *, system: bool = False, now_iso: str | None = None) -> AliasRecord:
    display_name = normalize_alias_display(value)
    if not display_name:
        if system:
            return default_alias_record()
        raise AliasValidationError("Ingresa un alias valido.")
    if system and normalize_alias_id(display_name, default="") == DEFAULT_ALIAS_ID:
        return default_alias_record()
    alias_id, display_name = validate_alias_display_name(display_name, allow_system_alias=system)
    timestamp = str(now_iso or _utc_now_iso())
    return AliasRecord(
        alias_id=alias_id,
        display_name=display_name,
        created_at=timestamp,
        updated_at=timestamp,
        system=bool(system),
    )


def alias_record_from_payload(payload: Any, *, now_iso: str | None = None) -> AliasRecord:
    if isinstance(payload, str):
        return alias_record_from_input(payload, now_iso=now_iso)
    if not isinstance(payload, dict):
        raise AliasValidationError("Alias payload invalido.")
    raw_alias_id = normalize_alias_id(payload.get("alias_id"), default="")
    raw_display_name = normalize_alias_display(
        payload.get("display_name") or payload.get("alias") or payload.get("name")
    )
    if raw_alias_id == DEFAULT_ALIAS_ID or raw_display_name.casefold() == DEFAULT_ALIAS_ID:
        return default_alias_record()
    source_value = raw_display_name or raw_alias_id
    alias_id, display_name = validate_alias_display_name(source_value, allow_system_alias=False)
    if raw_alias_id and raw_alias_id != alias_id:
        raise AliasValidationError("Alias payload con alias_id inconsistente.")
    timestamp = str(now_iso or _utc_now_iso())
    created_at = str(payload.get("created_at") or payload.get("created") or "").strip() or timestamp
    updated_at = str(payload.get("updated_at") or payload.get("updated") or "").strip() or created_at
    return AliasRecord(
        alias_id=alias_id,
        display_name=display_name,
        created_at=created_at,
        updated_at=updated_at,
        system=bool(payload.get("system")),
    )
