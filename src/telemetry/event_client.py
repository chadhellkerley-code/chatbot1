from __future__ import annotations

import logging
import threading
import time
from typing import Any

from src.licensing.license_client import SupabaseLicenseClient, get_runtime_context


logger = logging.getLogger(__name__)

_ALLOWED_EVENT_TYPES = {
    "PLAYWRIGHT_CRASH",
    "SESSION_EXPIRED",
    "UPDATE_FAILED",
    "LOGIN_FAILED",
    "RUNTIME_ERROR",
}
_ALLOWED_SEVERITIES = {"info", "warning", "error", "critical"}
_DEDUP_TTL_SECONDS = 300
_HEALTH_LOCK = threading.RLock()
_LAST_ERROR_CODE = ""
_LAST_ERROR_MESSAGE = ""
_LAST_ERROR_AT = 0.0
_RECENT_EVENTS: dict[str, float] = {}


def _normalize_severity(value: str) -> str:
    severity = str(value or "error").strip().lower()
    return severity if severity in _ALLOWED_SEVERITIES else "error"


def _normalize_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return dict(payload)
    if isinstance(payload, list):
        return {"items": list(payload)}
    if payload in (None, ""):
        return {}
    return {"value": payload}


def _dedupe_key(event_type: str, severity: str, message: str, device_id: str) -> str:
    return "|".join(
        [
            str(event_type or "").strip().upper(),
            str(severity or "").strip().lower(),
            str(device_id or "").strip(),
            str(message or "").strip(),
        ]
    )


def _should_skip_duplicate(event_type: str, severity: str, message: str, device_id: str) -> bool:
    key = _dedupe_key(event_type, severity, message, device_id)
    now = time.time()
    last_seen = _RECENT_EVENTS.get(key, 0.0)
    _RECENT_EVENTS[key] = now
    stale = [
        cache_key
        for cache_key, seen_at in _RECENT_EVENTS.items()
        if now - seen_at > _DEDUP_TTL_SECONDS
    ]
    for cache_key in stale:
        _RECENT_EVENTS.pop(cache_key, None)
    return last_seen > 0 and (now - last_seen) < _DEDUP_TTL_SECONDS


def _update_runtime_error(code: str, message: str) -> None:
    global _LAST_ERROR_CODE
    global _LAST_ERROR_MESSAGE
    global _LAST_ERROR_AT
    with _HEALTH_LOCK:
        _LAST_ERROR_CODE = str(code or "").strip()
        _LAST_ERROR_MESSAGE = str(message or "").strip()
        _LAST_ERROR_AT = time.time() if (_LAST_ERROR_CODE or _LAST_ERROR_MESSAGE) else 0.0


def clear_runtime_error() -> None:
    _update_runtime_error("", "")


def runtime_health_snapshot() -> dict[str, Any]:
    with _HEALTH_LOCK:
        return {
            "last_error_code": _LAST_ERROR_CODE,
            "last_error_message": _LAST_ERROR_MESSAGE,
            "last_error_at": _LAST_ERROR_AT,
            "runtime_ok": not bool(_LAST_ERROR_CODE),
        }


def report_client_event(
    event_type: str,
    *,
    severity: str = "error",
    message: str = "",
    payload: Any = None,
) -> bool:
    normalized_type = str(event_type or "").strip().upper()
    if normalized_type not in _ALLOWED_EVENT_TYPES:
        raise ValueError(f"Unsupported event type: {normalized_type}")
    normalized_severity = _normalize_severity(severity)
    context = get_runtime_context()
    if context is None:
        return False
    if _should_skip_duplicate(normalized_type, normalized_severity, message, context.device_id):
        return False
    if normalized_severity in {"error", "critical"}:
        _update_runtime_error(normalized_type, message)

    client = SupabaseLicenseClient(admin=False)
    try:
        client.rest.insert(
            "client_events",
            {
                "license_key": context.license_key,
                "device_id": context.device_id,
                "event_type": normalized_type,
                "severity": normalized_severity,
                "message": str(message or "").strip(),
                "payload_json": _normalize_payload(payload),
            },
            returning="minimal",
        )
        return True
    except Exception:
        logger.exception("Could not persist client event %s", normalized_type)
        return False


def report_playwright_crash(message: str, *, payload: Any = None) -> bool:
    return report_client_event(
        "PLAYWRIGHT_CRASH",
        severity="critical",
        message=message,
        payload=payload,
    )


def report_session_expired(message: str, *, payload: Any = None) -> bool:
    return report_client_event(
        "SESSION_EXPIRED",
        severity="warning",
        message=message,
        payload=payload,
    )


def report_update_failed(message: str, *, payload: Any = None) -> bool:
    return report_client_event(
        "UPDATE_FAILED",
        severity="error",
        message=message,
        payload=payload,
    )


def report_login_failed(message: str, *, payload: Any = None) -> bool:
    return report_client_event(
        "LOGIN_FAILED",
        severity="warning",
        message=message,
        payload=payload,
    )


def report_runtime_error(message: str, *, payload: Any = None) -> bool:
    return report_client_event(
        "RUNTIME_ERROR",
        severity="error",
        message=message,
        payload=payload,
    )
