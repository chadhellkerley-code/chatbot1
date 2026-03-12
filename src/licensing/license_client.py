from __future__ import annotations

import json
import logging
import os
import secrets
import string
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from config import read_app_config, read_env_local

from .device_id import DeviceIdentity, collect_device_identity


logger = logging.getLogger(__name__)

INVALID_LICENSE_MESSAGE = (
    "Esta licencia no es válida o fue desactivada.\n"
    "Contacte al soporte para continuar."
)
DEVICE_LIMIT_MESSAGE = (
    "Esta licencia ya alcanzó el límite máximo de dispositivos permitidos.\n"
    "El acceso fue bloqueado automáticamente por el sistema de licencias."
)
LICENSE_EXPIRED_MESSAGE = (
    "Esta licencia ha expirado.\n"
    "Por favor contacte al soporte para renovarla."
)
LICENSE_VALIDATION_UNAVAILABLE_MESSAGE = (
    "No se pudo validar la licencia con el servidor.\n"
    "Verifique la conexion e intente nuevamente."
)
LICENSE_FILE_MISSING_MESSAGE = (
    "No se encontró un archivo de licencia local en la carpeta de la aplicación."
)
SUPABASE_NOT_CONFIGURED_MESSAGE = (
    "Falta la configuración de Supabase.\n"
    "Abra InstaCRM Owner y use Sistema > Configuración > Configurar Supabase."
)

_DEFAULT_MAX_DEVICES = 2
_LICENSE_TABLE = "licenses"
_ACTIVATION_TABLE = "license_activations"
_CLIENT_KEY_NAMES = ("SUPABASE_KEY", "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_ROLE_KEY")
_ADMIN_KEY_NAMES = ("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_KEY", "SUPABASE_ANON_KEY")
_LICENSE_FILE_ENV_NAMES = ("LICENSE_FILE", "INSTACRM_LICENSE_FILE")
_LICENSE_FILE_NAMES = ("license.key", "license.json", "license_payload.json")
_RUNTIME_CONTEXT_LOCK = threading.RLock()
_RUNTIME_CONTEXT: "LicenseRuntimeContext | None" = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _candidate_roots() -> list[Path]:
    roots: list[Path] = []
    meipass = str(getattr(sys, "_MEIPASS", "") or "").strip()
    if meipass:
        try:
            roots.append(Path(meipass).resolve())
        except Exception:
            pass
    executable = str(getattr(sys, "executable", "") or "").strip()
    if executable:
        try:
            roots.append(Path(executable).resolve().parent)
        except Exception:
            pass
    for env_name in (
        "INSTACRM_INSTALL_ROOT",
        "INSTACRM_APP_ROOT",
        "INSTACRM_DATA_ROOT",
        "APP_DATA_ROOT",
    ):
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            roots.append(Path(raw).expanduser())
    roots.append(Path(__file__).resolve().parents[2])
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _license_file_candidates() -> list[Path]:
    candidates: list[Path] = []
    for env_name in _LICENSE_FILE_ENV_NAMES:
        raw = (os.environ.get(env_name) or "").strip()
        if raw:
            candidates.append(Path(raw).expanduser())
    for root in _candidate_roots():
        for filename in _LICENSE_FILE_NAMES:
            candidates.append(root / filename)
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _extract_license_key(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("license_key", "key"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    nested = payload.get("license")
    if isinstance(nested, dict):
        return _extract_license_key(nested)
    return ""


def _parse_license_key_blob(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            return _extract_license_key(json.loads(text))
        except Exception:
            return ""
    for line in text.splitlines():
        license_key = str(line or "").strip()
        if license_key:
            return license_key
    return ""


def load_local_license_key() -> str:
    for candidate in _license_file_candidates():
        if not candidate.is_file():
            continue
        try:
            raw = candidate.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        license_key = _parse_license_key_blob(raw)
        if license_key:
            return license_key
    return ""


def generate_license_key() -> str:
    alphabet = string.ascii_uppercase + string.digits
    groups = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(4)]
    return "-".join(groups)


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str

    @classmethod
    def from_env(cls, *, admin: bool = False) -> "SupabaseConfig":
        env_local = read_env_local()
        app_config = read_app_config()
        merged = {**app_config, **env_local, **os.environ}
        url = str(
            merged.get("SUPABASE_URL")
            or merged.get("supabase_url")
            or ""
        ).strip()
        key_names = _ADMIN_KEY_NAMES if admin else _CLIENT_KEY_NAMES
        key = ""
        for name in key_names:
            candidate = str(
                merged.get(name)
                or (merged.get("supabase_key") if name == "SUPABASE_KEY" else "")
                or ""
            ).strip()
            if candidate:
                key = candidate
                break
        if not url or not key:
            raise LicenseStartupError(
                code="supabase_not_configured",
                user_message=SUPABASE_NOT_CONFIGURED_MESSAGE,
                detail="Supabase credentials are not configured.",
            )
        return cls(url=url.rstrip("/"), key=key)


class LicenseStartupError(RuntimeError):
    def __init__(self, *, code: str, user_message: str, detail: str = "") -> None:
        super().__init__(detail or user_message)
        self.code = str(code or "license_error").strip() or "license_error"
        self.user_message = str(user_message or INVALID_LICENSE_MESSAGE)
        self.detail = str(detail or user_message or "")


class SupabaseRestError(RuntimeError):
    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        payload: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code or 0)
        self.message = str(message or "")
        self.payload = payload


@dataclass(frozen=True)
class LicenseRuntimeContext:
    license_key: str
    license_id: str
    device_id: str
    machine_name: str
    os_user: str
    mac_address: str
    status: str
    expires_at: str
    max_devices: int
    plan_name: str
    client_name: str
    notes: str
    app_version: str
    activation_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "license_key": self.license_key,
            "license_id": self.license_id,
            "device_id": self.device_id,
            "machine_name": self.machine_name,
            "os_user": self.os_user,
            "mac_address": self.mac_address,
            "status": self.status,
            "expires_at": self.expires_at,
            "max_devices": self.max_devices,
            "plan_name": self.plan_name,
            "client_name": self.client_name,
            "notes": self.notes,
            "app_version": self.app_version,
            "activation_id": self.activation_id,
        }


def get_runtime_context() -> LicenseRuntimeContext | None:
    with _RUNTIME_CONTEXT_LOCK:
        return _RUNTIME_CONTEXT


def set_runtime_context(context: LicenseRuntimeContext | None) -> None:
    global _RUNTIME_CONTEXT
    with _RUNTIME_CONTEXT_LOCK:
        _RUNTIME_CONTEXT = context


def clear_runtime_context() -> None:
    set_runtime_context(None)


class SupabaseRestClient:
    def __init__(self, config: SupabaseConfig, *, timeout_seconds: int = 15) -> None:
        self.config = config
        self.timeout_seconds = max(5, int(timeout_seconds or 15))
        self._session = requests.Session()

    def _headers(self, *, prefer: str | None = None) -> dict[str, str]:
        headers = {
            "apikey": self.config.key,
            "Authorization": f"Bearer {self.config.key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        return headers

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_payload: Any = None,
        prefer: str | None = None,
    ) -> Any:
        url = f"{self.config.url}/rest/v1/{endpoint.lstrip('/')}"
        try:
            response = self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_payload,
                headers=self._headers(prefer=prefer),
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise LicenseStartupError(
                code="supabase_request_failed",
                user_message=LICENSE_VALIDATION_UNAVAILABLE_MESSAGE,
                detail=str(exc),
            ) from exc
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            raise SupabaseRestError(
                status_code=response.status_code,
                message=f"{response.status_code}: {payload}",
                payload=payload,
            )
        if not response.text:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    def select(
        self,
        table: str,
        *,
        filters: dict[str, Any] | None = None,
        columns: str = "*",
        order: str = "",
        limit: int | None = None,
        single: bool = False,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        params: dict[str, Any] = {"select": columns}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = int(limit)
        payload = self.request("get", table, params=params)
        if single:
            if isinstance(payload, list):
                return payload[0] if payload else None
            if isinstance(payload, dict):
                return payload
            return None
        if not isinstance(payload, list):
            return []
        return [row for row in payload if isinstance(row, dict)]

    def insert(
        self,
        table: str,
        payload: dict[str, Any] | list[dict[str, Any]],
        *,
        returning: str = "representation",
    ) -> Any:
        return self.request(
            "post",
            table,
            json_payload=payload,
            prefer=f"return={returning}",
        )

    def update(
        self,
        table: str,
        payload: dict[str, Any],
        *,
        filters: dict[str, Any],
        returning: str = "representation",
    ) -> Any:
        return self.request(
            "patch",
            table,
            params=filters,
            json_payload=payload,
            prefer=f"return={returning}",
        )

    def delete(
        self,
        table: str,
        *,
        filters: dict[str, Any],
        returning: str = "representation",
    ) -> Any:
        return self.request(
            "delete",
            table,
            params=filters,
            prefer=f"return={returning}",
        )


def _error_mentions_column(error: SupabaseRestError, column_name: str) -> bool:
    text = f"{error.message} {error.payload}"
    return str(column_name or "") in text


def _activation_device_value(row: dict[str, Any]) -> str:
    for key in ("device_id", "client_fingerprint", "machine_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _activation_last_seen_value(row: dict[str, Any]) -> str:
    for key in ("last_seen", "last_seen_at", "activated_at", "created_at"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _activation_machine_name_value(row: dict[str, Any]) -> str:
    for key in ("machine_name", "hostname", "machine", "user_agent"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _activation_status_value(row: dict[str, Any]) -> str:
    if "status" in row:
        return str(row.get("status") or "").strip().lower()
    if "is_active" in row:
        return "active" if bool(row.get("is_active")) else "inactive"
    if row.get("deactivated_at"):
        return "inactive"
    return "active"


def _activation_is_active(row: dict[str, Any]) -> bool:
    return _activation_status_value(row) in {"", "active", "enabled"}


def _normalize_license_row(row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or "").strip().lower()
    if not status:
        status = "active" if bool(row.get("is_active", True)) else "inactive"
    return {
        "id": str(row.get("id") or "").strip(),
        "license_key": str(row.get("license_key") or "").strip(),
        "status": status,
        "expires_at": str(row.get("expires_at") or "").strip(),
        "max_devices": max(1, _coerce_int(row.get("max_devices"), _DEFAULT_MAX_DEVICES)),
        "client_name": str(
            row.get("client_name")
            or row.get("name")
            or row.get("customer_name")
            or ""
        ).strip(),
        "plan_name": str(row.get("plan_name") or row.get("plan") or "").strip(),
        "notes": str(row.get("notes") or "").strip(),
        "created_at": str(row.get("created_at") or "").strip(),
        "last_seen_at": str(row.get("last_seen_at") or row.get("last_seen") or "").strip(),
    }


def _normalize_activation_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or "").strip(),
        "license_key": str(row.get("license_key") or "").strip(),
        "license_id": str(row.get("license_id") or "").strip(),
        "device_id": _activation_device_value(row),
        "machine_name": _activation_machine_name_value(row),
        "os_user": str(row.get("os_user") or "").strip(),
        "status": _activation_status_value(row),
        "activated_at": str(row.get("activated_at") or row.get("created_at") or "").strip(),
        "last_seen": _activation_last_seen_value(row),
        "payload": dict(row),
    }


class SupabaseLicenseClient:
    def __init__(
        self,
        *,
        admin: bool = False,
        rest_client: SupabaseRestClient | None = None,
    ) -> None:
        self.config = rest_client.config if rest_client is not None else SupabaseConfig.from_env(admin=admin)
        self.rest = rest_client or SupabaseRestClient(self.config)
        self.admin = bool(admin)

    def fetch_license(self, license_key: str) -> dict[str, Any] | None:
        clean_key = str(license_key or "").strip()
        if not clean_key:
            return None
        payload = self.rest.select(
            _LICENSE_TABLE,
            filters={"license_key": f"eq.{clean_key}"},
            order="created_at.desc",
            limit=1,
            single=True,
        )
        if not isinstance(payload, dict):
            return None
        row = _normalize_license_row(payload)
        return row if row.get("license_key") else None

    def list_licenses(self) -> list[dict[str, Any]]:
        rows = self.rest.select(_LICENSE_TABLE, order="created_at.desc")
        return [_normalize_license_row(row) for row in rows if isinstance(row, dict)]

    def create_license(
        self,
        *,
        client_name: str,
        plan_name: str,
        max_devices: int = _DEFAULT_MAX_DEVICES,
        expires_at: str | datetime,
        notes: str = "",
    ) -> dict[str, Any]:
        clean_client_name = str(client_name or "").strip()
        if not clean_client_name:
            raise ValueError("client_name is required")
        payload = {
            "client_name": clean_client_name,
            "license_key": generate_license_key(),
            "plan_name": str(plan_name or "").strip(),
            "max_devices": max(1, int(max_devices or _DEFAULT_MAX_DEVICES)),
            "expires_at": self._normalize_expiration(expires_at),
            "notes": str(notes or "").strip(),
            "status": "active",
            "created_at": _utc_now_iso(),
        }
        created = self.rest.insert(_LICENSE_TABLE, payload)
        if isinstance(created, list) and created:
            return _normalize_license_row(dict(created[0]))
        if isinstance(created, dict):
            return _normalize_license_row(created)
        return _normalize_license_row(payload)

    def deactivate_license(self, license_key: str) -> dict[str, Any] | None:
        record = self.fetch_license(license_key)
        if not record:
            return None
        updated = self.rest.update(
            _LICENSE_TABLE,
            {"status": "inactive", "last_seen_at": _utc_now_iso()},
            filters={"license_key": f"eq.{record['license_key']}"},
        )
        if isinstance(updated, list) and updated:
            return _normalize_license_row(dict(updated[0]))
        if isinstance(updated, dict):
            return _normalize_license_row(updated)
        record["status"] = "inactive"
        return record

    def extend_license(self, license_key: str, *, days: int) -> dict[str, Any] | None:
        record = self.fetch_license(license_key)
        if not record:
            return None
        base_dt = _parse_iso(record.get("expires_at")) or _utc_now()
        next_dt = base_dt + timedelta(days=max(1, int(days or 1)))
        updated = self.rest.update(
            _LICENSE_TABLE,
            {"expires_at": next_dt.isoformat(), "status": "active"},
            filters={"license_key": f"eq.{record['license_key']}"},
        )
        if isinstance(updated, list) and updated:
            return _normalize_license_row(dict(updated[0]))
        if isinstance(updated, dict):
            return _normalize_license_row(updated)
        record["expires_at"] = next_dt.isoformat()
        record["status"] = "active"
        return record

    def list_license_activations(
        self,
        license_key: str,
        *,
        license_id: str = "",
    ) -> list[dict[str, Any]]:
        filters_to_try: list[dict[str, Any]] = []
        if license_key:
            filters_to_try.append({"license_key": f"eq.{license_key}"})
        if license_id:
            filters_to_try.append({"license_id": f"eq.{license_id}"})
        filters_to_try.append({})
        last_error: SupabaseRestError | None = None
        for filters in filters_to_try:
            for order in ("last_seen.desc", "last_seen_at.desc", "activated_at.desc", "created_at.desc", ""):
                try:
                    rows = self.rest.select(
                        _ACTIVATION_TABLE,
                        filters=filters or None,
                        order=order,
                    )
                except SupabaseRestError as exc:
                    last_error = exc
                    if filters and any(
                        _error_mentions_column(exc, column_name)
                        for column_name in filters.keys()
                    ):
                        break
                    if order and _error_mentions_column(exc, order.split(".", 1)[0]):
                        continue
                    raise
                normalized = [_normalize_activation_row(row) for row in rows if isinstance(row, dict)]
                if not filters:
                    filtered: list[dict[str, Any]] = []
                    for row in normalized:
                        if license_key and str(row.get("license_key") or "").strip() == license_key:
                            filtered.append(row)
                        elif license_id and str(row.get("license_id") or "").strip() == license_id:
                            filtered.append(row)
                    normalized = filtered
                return normalized
        if last_error is not None:
            raise last_error
        return []

    def reset_device_activations(self, license_key: str) -> int:
        record = self.fetch_license(license_key)
        if not record:
            return 0
        filters_to_try = [{"license_key": f"eq.{record['license_key']}"}]
        if record.get("id"):
            filters_to_try.append({"license_id": f"eq.{record['id']}"})
        last_error: SupabaseRestError | None = None
        for filters in filters_to_try:
            try:
                deleted = self.rest.delete(_ACTIVATION_TABLE, filters=filters)
            except SupabaseRestError as exc:
                last_error = exc
                if any(_error_mentions_column(exc, key) for key in filters.keys()):
                    continue
                raise
            return len(deleted) if isinstance(deleted, list) else 0
        if last_error is not None:
            raise last_error
        return 0

    def validate_license_key(self, license_key: str) -> dict[str, Any]:
        record = self.fetch_license(license_key)
        if not record:
            raise LicenseStartupError(
                code="license_invalid",
                user_message=INVALID_LICENSE_MESSAGE,
                detail="License key not found.",
            )
        status = str(record.get("status") or "").strip().lower()
        if status != "active":
            raise LicenseStartupError(
                code="license_inactive",
                user_message=INVALID_LICENSE_MESSAGE,
                detail=f"License status is {status or 'unknown'}.",
            )
        expires_at = _parse_iso(record.get("expires_at"))
        if expires_at is not None and expires_at < _utc_now():
            raise LicenseStartupError(
                code="license_expired",
                user_message=LICENSE_EXPIRED_MESSAGE,
                detail=f"License expired at {record.get('expires_at')}.",
            )
        return record

    def validate_and_activate(
        self,
        license_key: str,
        *,
        device: DeviceIdentity | None = None,
        app_version: str = "",
    ) -> LicenseRuntimeContext:
        record = self.validate_license_key(license_key)
        device = device or collect_device_identity()
        activation = self._activate_device(record, device=device, app_version=app_version)
        self._touch_license(record["license_key"])
        context = LicenseRuntimeContext(
            license_key=record["license_key"],
            license_id=record.get("id", ""),
            device_id=device.device_id,
            machine_name=device.hostname,
            os_user=device.os_user,
            mac_address=device.mac_address,
            status=str(record.get("status") or "active"),
            expires_at=str(record.get("expires_at") or ""),
            max_devices=max(1, _coerce_int(record.get("max_devices"), _DEFAULT_MAX_DEVICES)),
            plan_name=str(record.get("plan_name") or ""),
            client_name=str(record.get("client_name") or ""),
            notes=str(record.get("notes") or ""),
            app_version=str(app_version or os.environ.get("APP_VERSION") or "").strip(),
            activation_id=str(activation.get("id") or ""),
        )
        set_runtime_context(context)
        return context

    def _normalize_expiration(self, value: str | datetime) -> str:
        parsed = value if isinstance(value, datetime) else _parse_iso(value)
        if parsed is None:
            raise ValueError("expires_at is invalid")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    def _touch_license(self, license_key: str) -> None:
        try:
            self.rest.update(
                _LICENSE_TABLE,
                {"last_seen_at": _utc_now_iso()},
                filters={"license_key": f"eq.{license_key}"},
                returning="minimal",
            )
        except Exception:
            logger.debug("Could not update license last_seen_at", exc_info=True)

    def _activate_device(
        self,
        record: dict[str, Any],
        *,
        device: DeviceIdentity,
        app_version: str = "",
    ) -> dict[str, Any]:
        license_key = str(record.get("license_key") or "").strip()
        license_id = str(record.get("id") or "").strip()
        activations = self.list_license_activations(license_key, license_id=license_id)
        by_device: dict[str, dict[str, Any]] = {}
        for row in activations:
            device_key = str(row.get("device_id") or "").strip()
            if not device_key:
                continue
            current = by_device.get(device_key)
            if current is None:
                by_device[device_key] = row
                continue
            current_seen = (
                _parse_iso(current.get("last_seen"))
                or _parse_iso(current.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            row_seen = (
                _parse_iso(row.get("last_seen"))
                or _parse_iso(row.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            if row_seen >= current_seen:
                by_device[device_key] = row

        existing = by_device.get(device.device_id)
        max_devices = max(1, _coerce_int(record.get("max_devices"), _DEFAULT_MAX_DEVICES))
        active_devices = [row for row in by_device.values() if _activation_is_active(row)]

        if existing is not None:
            updated = self._update_activation(
                existing,
                license_key=license_key,
                license_id=license_id,
                device=device,
                app_version=app_version,
            )
            return updated or existing

        if len(active_devices) >= max_devices:
            raise LicenseStartupError(
                code="device_limit_reached",
                user_message=DEVICE_LIMIT_MESSAGE,
                detail=(
                    f"License {license_key} reached max devices. "
                    f"Active devices: {len(active_devices)} / {max_devices}."
                ),
            )

        return self._insert_activation(
            license_key=license_key,
            license_id=license_id,
            device=device,
            app_version=app_version,
        )

    def _update_activation(
        self,
        current: dict[str, Any],
        *,
        license_key: str,
        license_id: str,
        device: DeviceIdentity,
        app_version: str,
    ) -> dict[str, Any] | None:
        del license_key, license_id, app_version
        activation_id = str(current.get("id") or "").strip()
        if not activation_id:
            return None
        payloads = [
            {
                "last_seen": _utc_now_iso(),
                "machine_name": device.hostname,
                "os_user": device.os_user,
                "status": "active",
            },
            {
                "last_seen_at": _utc_now_iso(),
                "machine_name": device.hostname,
                "os_user": device.os_user,
                "status": "active",
            },
            {"last_seen_at": _utc_now_iso()},
        ]
        for payload in payloads:
            try:
                updated = self.rest.update(
                    _ACTIVATION_TABLE,
                    payload,
                    filters={"id": f"eq.{activation_id}"},
                )
            except SupabaseRestError as exc:
                if any(_error_mentions_column(exc, key) for key in payload):
                    continue
                raise
            if isinstance(updated, list) and updated:
                return _normalize_activation_row(dict(updated[0]))
            if isinstance(updated, dict):
                return _normalize_activation_row(updated)
            break
        return {
            **current,
            "last_seen": _utc_now_iso(),
            "machine_name": device.hostname,
            "os_user": device.os_user,
            "status": "active",
        }

    def _insert_activation(
        self,
        *,
        license_key: str,
        license_id: str,
        device: DeviceIdentity,
        app_version: str,
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        payloads: list[dict[str, Any]] = [
            {
                "license_key": license_key,
                "license_id": license_id or None,
                "device_id": device.device_id,
                "machine_name": device.hostname,
                "os_user": device.os_user,
                "mac_address": device.mac_address,
                "activated_at": now_iso,
                "last_seen": now_iso,
                "status": "active",
            },
            {
                "license_key": license_key,
                "license_id": license_id or None,
                "device_id": device.device_id,
                "machine_name": device.hostname,
                "os_user": device.os_user,
                "mac_address": device.mac_address,
                "activated_at": now_iso,
                "last_seen_at": now_iso,
                "status": "active",
            },
        ]
        if license_id:
            payloads.append(
                {
                    "license_id": license_id,
                    "client_fingerprint": device.device_id,
                    "activated_at": now_iso,
                    "last_seen_at": now_iso,
                    "user_agent": (
                        f"InstaCRM/{app_version or os.environ.get('APP_VERSION') or 'unknown'}"
                    ),
                }
            )
        last_error: SupabaseRestError | None = None
        for payload in payloads:
            compact_payload = {
                key: value
                for key, value in payload.items()
                if value not in (None, "")
            }
            try:
                created = self.rest.insert(_ACTIVATION_TABLE, compact_payload)
            except SupabaseRestError as exc:
                last_error = exc
                if any(_error_mentions_column(exc, key) for key in compact_payload):
                    continue
                raise
            if isinstance(created, list) and created:
                return _normalize_activation_row(dict(created[0]))
            if isinstance(created, dict):
                return _normalize_activation_row(created)
            return _normalize_activation_row(compact_payload)
        if last_error is not None:
            raise last_error
        return _normalize_activation_row(payloads[0])


def launch_with_license() -> LicenseRuntimeContext:
    current = get_runtime_context()
    if current is not None:
        return current
    license_key = load_local_license_key()
    if not license_key:
        raise LicenseStartupError(
            code="license_missing",
            user_message=LICENSE_FILE_MISSING_MESSAGE,
            detail="No local license key was found.",
        )
    client = SupabaseLicenseClient(admin=False)
    device = collect_device_identity()
    app_version = str(
        os.environ.get("APP_VERSION") or os.environ.get("CLIENT_VERSION") or ""
    ).strip()
    try:
        return client.validate_and_activate(
            license_key,
            device=device,
            app_version=app_version,
        )
    except SupabaseRestError as exc:
        raise LicenseStartupError(
            code="supabase_request_failed",
            user_message=LICENSE_VALIDATION_UNAVAILABLE_MESSAGE,
            detail=exc.message,
        ) from exc
