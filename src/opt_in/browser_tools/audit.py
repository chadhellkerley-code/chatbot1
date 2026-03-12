import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .config import get_settings


_LOCK = threading.Lock()


SENSITIVE_KEYS = {"password", "pass", "secret", "token", "otp", "code"}


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_value(key, item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


def _sanitize_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(marker in lowered for marker in SENSITIVE_KEYS):
        return "***"
    return _sanitize(value)


def log_event(event: str, account: Optional[str] = None, details: Optional[Dict[str, Any]] = None) -> None:
    settings = get_settings()
    payload = {
        "ts": time.time(),
        "event": event,
        "account": account,
        "details": _sanitize(details or {}),
    }

    line = json.dumps(payload, ensure_ascii=True)
    path: Path = settings.audit_log_path

    with _LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def log_error(event: str, account: Optional[str], error: Exception) -> None:
    log_event(
        event=event,
        account=account,
        details={
            "type": error.__class__.__name__,
            "message": str(error),
        },
    )
