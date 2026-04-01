from __future__ import annotations

import base64
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

from license_identity import apply_client_identity_env, clear_client_identity_env
from paths import runtime_base, runtime_root

from .device_id import DeviceIdentity, collect_device_identity


logger = logging.getLogger(__name__)

INVALID_LICENSE_MESSAGE = (
    "Licencia inválida o desactivada.\n"
    "Contacte al soporte para continuar."
)
DEVICE_LIMIT_MESSAGE = (
    "Máximo de dispositivos alcanzado."
)
LICENSE_EXPIRED_MESSAGE = (
    "Licencia expirada."
)
LICENSE_VALIDATION_UNAVAILABLE_MESSAGE = (
    "Sin conexión. Reintentar."
)
SUPABASE_AUTH_FAILED_MESSAGE = (
    "No se pudo autenticar con Supabase.\n"
    "Contacte al soporte para continuar."
)
LICENSE_FILE_MISSING_MESSAGE = (
    "Licencia requerida."
)
SUPABASE_NOT_CONFIGURED_MESSAGE = (
    "Falta la configuración de Supabase.\n"
    "Abra InstaCRM Owner y use Sistema > Configuración > Configurar Supabase."
)

_DEFAULT_MAX_DEVICES = 2
_LICENSE_TABLE = "licenses"
_ACTIVATION_TABLE = "license_activations"
_LICENSE_FILE_ENV_NAMES = ("LICENSE_FILE", "INSTACRM_LICENSE_FILE")
_LICENSE_FILE_NAMES = ("license.key", "license.json", "license_payload.json")
_RUNTIME_CONTEXT_LOCK = threading.RLock()
_RUNTIME_CONTEXT: "LicenseRuntimeContext | None" = None
_LICENSE_CACHE_FILENAME = "license.json"
_LICENSE_CACHE_BYPASS_AGE = timedelta(hours=24)
_LICENSE_CACHE_OFFLINE_GRACE = timedelta(days=3)


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


def _license_cache_path() -> Path:
    default_root = Path(__file__).resolve().parents[2]
    return runtime_root(default_root) / _LICENSE_CACHE_FILENAME


def _local_license_key_path() -> Path:
    default_root = Path(__file__).resolve().parents[2]
    return runtime_base(default_root) / "license.key"


def save_local_license_key(license_key: str) -> Path | None:
    clean_key = str(license_key or "").strip()
    if not clean_key:
        return None
    path = _local_license_key_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(clean_key + "\n", encoding="utf-8")
        tmp_path.replace(path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        return None
    return path


def load_local_license_cache() -> dict[str, Any] | None:
    path = _license_cache_path()
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return dict(payload)


def save_local_license_cache(payload: dict[str, Any]) -> Path | None:
    path = _license_cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None
    compact = {key: value for key, value in dict(payload or {}).items() if value is not None}
    try:
        blob = json.dumps(compact, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(blob + "\n", encoding="utf-8")
        tmp_path.replace(path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        return None
    return path


def _load_license_cache() -> dict[str, Any] | None:
    return load_local_license_cache()


def _save_license_cache(data: dict[str, Any]) -> Path | None:
    return save_local_license_cache(data)


def _is_cache_valid(cache: dict[str, Any]) -> bool:
    now = _utc_now()
    expires_at = _parse_iso(cache.get("expires_at"))
    if expires_at is not None and expires_at < now:
        return False
    validated_at = _parse_iso(cache.get("validated_at"))
    if validated_at is None:
        return False
    return now - validated_at < _LICENSE_CACHE_BYPASS_AGE


def _cache_payload_allows_use(
    payload: dict[str, Any] | None,
    *,
    license_key: str,
    device_id: str,
    now: datetime,
    max_age: timedelta,
) -> bool:
    if not isinstance(payload, dict):
        return False
    clean_key = _extract_license_key(payload)
    if not clean_key or clean_key != str(license_key or "").strip():
        return False
    clean_device = str(payload.get("device_id") or "").strip()
    if not clean_device or clean_device != str(device_id or "").strip():
        return False
    validated_at = _parse_iso(payload.get("validated_at"))
    if validated_at is None:
        return False
    if now - validated_at > max_age:
        return False
    expires_at = _parse_iso(payload.get("expires_at"))
    if expires_at is not None and expires_at < now:
        return False
    return True


def _cache_payload_expired(payload: dict[str, Any] | None, *, now: datetime) -> bool:
    if not isinstance(payload, dict):
        return False
    expires_at = _parse_iso(payload.get("expires_at"))
    return bool(expires_at is not None and expires_at < now)


def _context_from_cache(
    payload: dict[str, Any],
    *,
    device: DeviceIdentity,
    app_version: str,
) -> "LicenseRuntimeContext":
    license_key = _extract_license_key(payload)
    expires_at = str(payload.get("expires_at") or "").strip()
    return LicenseRuntimeContext(
        license_key=license_key,
        license_id="",
        device_id=device.device_id,
        machine_name=device.hostname,
        os_user=device.os_user,
        mac_address=device.mac_address,
        status="active",
        expires_at=expires_at,
        max_devices=_DEFAULT_MAX_DEVICES,
        plan_name="",
        client_name="",
        notes="",
        app_version=str(app_version or os.environ.get("APP_VERSION") or "").strip(),
        activation_id="",
    )


def license_failure_reason(code: str) -> str:
    clean = str(code or "").strip().lower()
    if clean in {"license_expired"}:
        return "expired"
    if clean in {"device_limit_reached"}:
        return "max devices"
    if clean in {"license_invalid", "license_inactive"}:
        return "invalid key"
    if clean in {"offline_cache_expired", "offline_cache_missing"}:
        return "offline expired"
    return clean or "license_error"


def format_license_user_message(exc: "LicenseStartupError") -> str:
    reason = license_failure_reason(exc.code)
    base = str(exc.user_message or "").strip() or INVALID_LICENSE_MESSAGE
    return f"{base}\n\nMotivo: {reason}"


def activate_and_cache_license(
    license_key: str,
    *,
    device: DeviceIdentity | None = None,
    app_version: str = "",
) -> "LicenseRuntimeContext":
    clean_key = str(license_key or "").strip()
    if not clean_key:
        raise LicenseStartupError(
            code="license_missing",
            user_message=LICENSE_FILE_MISSING_MESSAGE,
            detail="No license key was provided.",
        )
    device = device or collect_device_identity()
    clean_version = str(
        app_version or os.environ.get("APP_VERSION") or os.environ.get("CLIENT_VERSION") or ""
    ).strip()
    client = SupabaseLicenseClient(admin=False)
    context = client.validate_and_activate(clean_key, device=device, app_version=clean_version)
    apply_client_identity_env(context.license_key)
    save_local_license_key(context.license_key)
    save_local_license_cache(
        {
            "license_key": context.license_key,
            "device_id": context.device_id,
            "validated_at": _utc_now_iso(),
            "expires_at": context.expires_at,
        }
    )
    return context


def _prompt_license_ui() -> str:
    try:
        from PySide6.QtWidgets import QApplication
    except Exception as exc:  # pragma: no cover
        raise LicenseStartupError(
            code="license_missing",
            user_message=LICENSE_FILE_MISSING_MESSAGE,
            detail="License activation UI is unavailable (PySide6 not installed).",
        ) from exc

    app = QApplication.instance()
    if app is None:  # pragma: no cover
        app = QApplication([])

    try:
        from gui.license_activation_dialog import LicenseDialog
    except Exception as exc:  # pragma: no cover
        raise LicenseStartupError(
            code="license_missing",
            user_message=LICENSE_FILE_MISSING_MESSAGE,
            detail="License activation UI module is unavailable.",
        ) from exc

    dialog = LicenseDialog()
    if dialog.exec():
        license_key = str(dialog.get_key() or "").strip()
        if license_key:
            return license_key

    raise LicenseStartupError(
        code="license_missing",
        user_message=LICENSE_FILE_MISSING_MESSAGE,
        detail="License activation was cancelled or no key was provided.",
    )


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
    processed: set[str] = set()
    for env_name in _LICENSE_FILE_ENV_NAMES:
        raw = (os.environ.get(env_name) or "").strip()
        if not raw:
            continue
        candidate = Path(raw).expanduser()
        processed.add(str(candidate))
        if not candidate.is_file():
            continue
        try:
            raw_text = candidate.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        license_key = _parse_license_key_blob(raw_text)
        if license_key:
            return license_key

    cached = load_local_license_cache()
    if cached:
        cached_key = _extract_license_key(cached)
        if cached_key:
            return cached_key
    for candidate in _license_file_candidates():
        if str(candidate) in processed:
            continue
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


def _get_embedded_supabase_config() -> tuple[str, str]:
    url = "https://sizacwrksmozgtjtonuu.supabase.co"

    PART1 = "c2Jfc2VjcmV0X3dnRWJP"
    PART2 = "XzRuVDMtZkdacUFPZFFl"
    PART3 = "TndfSGxUbTR6c2U="

    encoded = PART1 + PART2 + PART3
    key = base64.b64decode(encoded).decode().strip()
    clean_url = url.rstrip("/")

    if not clean_url or not key:
        raise LicenseStartupError(
            code="supabase_not_configured",
            user_message=SUPABASE_NOT_CONFIGURED_MESSAGE,
            detail="Embedded Supabase credentials are incomplete.",
        )

    return clean_url, key


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str


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
    if context is None:
        clear_client_identity_env()
        return
    apply_client_identity_env(context.license_key)


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
            if response.status_code in {401, 403}:
                raise LicenseStartupError(
                    code="supabase_auth_failed",
                    user_message=SUPABASE_AUTH_FAILED_MESSAGE,
                    detail=f"{response.status_code}: {payload}",
                )
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


def _activation_is_active(row: dict[str, Any]) -> bool:
    return str(row["status"]).strip().lower() == "active"


def _normalize_license_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]).strip(),
        "license_key": str(row["license_key"]).strip(),
        "status": str(row["status"]).strip().lower(),
        "expires_at": str(row["expires_at"]).strip(),
        "max_devices": max(1, _coerce_int(row["max_devices"], _DEFAULT_MAX_DEVICES)),
        "client_name": str(row["client_name"]).strip(),
        "plan_name": str(row["plan_name"]).strip(),
        "notes": "",
        "created_at": str(row["created_at"]).strip(),
        "last_seen_at": str(row["last_seen_at"]).strip(),
    }


def _normalize_activation_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]).strip(),
        "license_id": str(row["license_id"]).strip(),
        "device_id": str(row["device_id"]).strip(),
        "machine_name": str(row["machine_name"]).strip(),
        "os_user": str(row["os_user"]).strip(),
        "mac_address": str(row["mac_address"]).strip(),
        "status": str(row["status"]).strip().lower(),
        "activated_at": str(row["activated_at"]).strip(),
        "last_seen_at": str(row["last_seen_at"]).strip(),
    }


class SupabaseLicenseClient:
    def __init__(
        self,
        *,
        admin: bool = False,
        rest_client: SupabaseRestClient | None = None,
    ) -> None:
        if rest_client is not None:
            self.config = rest_client.config
        else:
            url, key = _get_embedded_supabase_config()
            self.config = SupabaseConfig(url=url, key=key)
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
        del notes
        clean_client_name = str(client_name or "").strip()
        if not clean_client_name:
            raise ValueError("client_name is required")
        payload = {
            "client_name": clean_client_name,
            "license_key": generate_license_key(),
            "plan_name": str(plan_name or "").strip(),
            "max_devices": max(1, int(max_devices or _DEFAULT_MAX_DEVICES)),
            "expires_at": self._normalize_expiration(expires_at),
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

    def list_license_activations(self, license_id: str) -> list[dict[str, Any]]:
        clean_id = str(license_id or "").strip()
        if not clean_id:
            return []
        rows = self.rest.select(
            _ACTIVATION_TABLE,
            filters={"license_id": f"eq.{clean_id}"},
            order="last_seen_at.desc",
        )
        return [_normalize_activation_row(row) for row in rows if isinstance(row, dict)]

    def reset_device_activations(self, license_key: str) -> int:
        record = self.fetch_license(license_key)
        license_id = str((record or {}).get("id") or "").strip()
        if not license_id:
            return 0
        deleted = self.rest.delete(
            _ACTIVATION_TABLE,
            filters={"license_id": f"eq.{license_id}"},
        )
        return len(deleted) if isinstance(deleted, list) else 0

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
        activation = self._activate_device(record, device=device)
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
        self.rest.update(
            _LICENSE_TABLE,
            {"last_seen_at": _utc_now_iso()},
            filters={"license_key": f"eq.{license_key}"},
            returning="minimal",
        )

    def _activate_device(
        self,
        record: dict[str, Any],
        *,
        device: DeviceIdentity,
    ) -> dict[str, Any]:
        license_key = str(record.get("license_key") or "").strip()
        license_id = str(record.get("id") or "").strip()
        if not license_id:
            raise LicenseStartupError(
                code="license_invalid",
                user_message=INVALID_LICENSE_MESSAGE,
                detail=f"License {license_key} is missing id.",
            )
        activations = self.list_license_activations(license_id)
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
                _parse_iso(current.get("last_seen_at"))
                or _parse_iso(current.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            row_seen = (
                _parse_iso(row.get("last_seen_at"))
                or _parse_iso(row.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            if row_seen >= current_seen:
                by_device[device_key] = row

        by_mac: dict[str, dict[str, Any]] = {}
        for row in activations:
            mac_key = str(row.get("mac_address") or "").strip().lower()
            if not mac_key:
                continue
            current = by_mac.get(mac_key)
            if current is None:
                by_mac[mac_key] = row
                continue
            current_seen = (
                _parse_iso(current.get("last_seen_at"))
                or _parse_iso(current.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            row_seen = (
                _parse_iso(row.get("last_seen_at"))
                or _parse_iso(row.get("activated_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            )
            if row_seen >= current_seen:
                by_mac[mac_key] = row

        existing = by_device.get(device.device_id)
        if existing is None:
            mac_key = str(device.mac_address or "").strip().lower()
            if mac_key:
                existing = by_mac.get(mac_key)
        max_devices = max(1, _coerce_int(record.get("max_devices"), _DEFAULT_MAX_DEVICES))
        active_devices = [row for row in by_device.values() if _activation_is_active(row)]

        if existing is not None:
            updated = self._update_activation(
                existing,
                device=device,
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
            license_id=license_id,
            device=device,
        )

    def _update_activation(
        self,
        current: dict[str, Any],
        *,
        device: DeviceIdentity,
    ) -> dict[str, Any] | None:
        activation_id = str(current.get("id") or "").strip()
        if not activation_id:
            return None
        payload = {
            "device_id": device.device_id,
            "machine_name": device.hostname,
            "os_user": device.os_user,
            "mac_address": device.mac_address,
            "last_seen_at": _utc_now_iso(),
            "status": "active",
        }
        updated = self.rest.update(
            _ACTIVATION_TABLE,
            payload,
            filters={"id": f"eq.{activation_id}"},
        )
        if isinstance(updated, list) and updated:
            return _normalize_activation_row(dict(updated[0]))
        if isinstance(updated, dict):
            return _normalize_activation_row(updated)
        return _normalize_activation_row({**current, **payload})

    def _insert_activation(
        self,
        *,
        license_id: str,
        device: DeviceIdentity,
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        payload = {
            "license_id": license_id,
            "device_id": device.device_id,
            "machine_name": device.hostname,
            "os_user": device.os_user,
            "mac_address": device.mac_address,
            "activated_at": now_iso,
            "last_seen_at": now_iso,
            "status": "active",
        }
        created = self.rest.insert(_ACTIVATION_TABLE, payload)
        if isinstance(created, list) and created:
            return _normalize_activation_row(dict(created[0]))
        if isinstance(created, dict):
            return _normalize_activation_row(created)
        return _normalize_activation_row(payload)


def launch_with_license() -> LicenseRuntimeContext:
    current = get_runtime_context()
    if current is not None:
        return current
    device = collect_device_identity()
    license_key = load_local_license_key()
    app_version = str(
        os.environ.get("APP_VERSION") or os.environ.get("CLIENT_VERSION") or ""
    ).strip()
    now = _utc_now()
    cached_payload = load_local_license_cache()
    if license_key and _cache_payload_allows_use(
        cached_payload,
        license_key=license_key,
        device_id=device.device_id,
        now=now,
        max_age=_LICENSE_CACHE_BYPASS_AGE,
    ):
        context = _context_from_cache(
            cached_payload or {},
            device=device,
            app_version=app_version,
        )
        set_runtime_context(context)
        logger.info("License startup: using cached validation (fresh).")
        return context

    if not license_key:
        license_key = _prompt_license_ui()
        context = activate_and_cache_license(
            license_key,
            device=device,
            app_version=app_version,
        )
        set_runtime_context(context)
        return context

    client = SupabaseLicenseClient(admin=False)
    try:
        context = client.validate_and_activate(
            license_key,
            device=device,
            app_version=app_version,
        )
    except LicenseStartupError as exc:
        if exc.code == "supabase_request_failed":
            if _cache_payload_allows_use(
                cached_payload,
                license_key=license_key,
                device_id=device.device_id,
                now=now,
                max_age=_LICENSE_CACHE_OFFLINE_GRACE,
            ):
                context = _context_from_cache(
                    cached_payload or {},
                    device=device,
                    app_version=app_version,
                )
                set_runtime_context(context)
                logger.warning(
                    "License startup: Supabase unavailable, using cached validation (offline)."
                )
                return context
            if _cache_payload_expired(cached_payload, now=now):
                raise LicenseStartupError(
                    code="license_expired",
                    user_message=LICENSE_EXPIRED_MESSAGE,
                    detail="Cached license indicates expiration and server validation failed.",
                ) from exc
            raise LicenseStartupError(
                code="offline_cache_expired",
                user_message=LICENSE_VALIDATION_UNAVAILABLE_MESSAGE,
                detail="Server validation failed and offline cache is missing/too old.",
            ) from exc
        raise
    except SupabaseRestError as exc:
        if _cache_payload_allows_use(
            cached_payload,
            license_key=license_key,
            device_id=device.device_id,
            now=now,
            max_age=_LICENSE_CACHE_OFFLINE_GRACE,
        ):
            context = _context_from_cache(
                cached_payload or {},
                device=device,
                app_version=app_version,
            )
            set_runtime_context(context)
            logger.warning("License startup: Supabase error, using cached validation (offline).")
            return context
        if _cache_payload_expired(cached_payload, now=now):
            raise LicenseStartupError(
                code="license_expired",
                user_message=LICENSE_EXPIRED_MESSAGE,
                detail="Cached license indicates expiration and server validation failed.",
            ) from exc
        raise LicenseStartupError(
            code="offline_cache_expired",
            user_message=LICENSE_VALIDATION_UNAVAILABLE_MESSAGE,
            detail=exc.message,
        ) from exc

    save_local_license_cache(
        {
            "license_key": context.license_key,
            "device_id": context.device_id,
            "validated_at": _utc_now_iso(),
            "expires_at": context.expires_at,
        }
    )
    save_local_license_key(context.license_key)
    return context
