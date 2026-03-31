from __future__ import annotations

import inspect
import re
import traceback as traceback_module
from pathlib import Path
from typing import Any


_DIRECT_REASON_CODES = {
    "account_not_found": "account_not_found",
    "automation_not_allowed": "thread_not_touchable",
    "browser_launch_failed": "browser_launch_failed",
    "browser_launch_timeout": "browser_launch_timeout",
    "chrome_error_page": "browser_launch_failed",
    "composer_not_found": "composer_not_found",
    "dedupe_pending": "dedupe_pending",
    "followup_not_allowed": "followup_not_allowed",
    "invalid_thread": "invalid_thread",
    "job_cancelled": "job_cancelled",
    "job_cancelled_by_runtime_stop": "job_cancelled_by_runtime_stop",
    "job_cancelled_by_takeover": "job_cancelled_by_takeover",
    "manual_send_not_allowed": "manual_send_not_allowed",
    "manual_takeover": "job_cancelled_by_takeover",
    "manual_takeover_pending": "job_cancelled_by_takeover",
    "network_identity_mismatch": "network_identity_mismatch",
    "persistent_profile_missing": "storage_state_missing",
    "proxy_unreachable": "proxy_unreachable",
    "runtime_inactive": "job_cancelled_by_runtime_stop",
    "runtime_stopped": "job_cancelled_by_runtime_stop",
    "runtime_stopping": "job_cancelled_by_runtime_stop",
    "session_invalid": "storage_state_invalid",
    "storage_state_invalid": "storage_state_invalid",
    "storage_state_missing": "storage_state_missing",
    "thread_locked_for_manual": "thread_not_touchable",
    "thread_not_opened": "thread_not_opened",
    "thread_not_touchable": "thread_not_touchable",
    "worker_missing": "worker_missing",
}


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return repr(value)


def _capture_callsite(*, skip: int) -> dict[str, Any]:
    frame = inspect.currentframe()
    try:
        current = frame
        for _ in range(max(0, int(skip)) + 1):
            if current is None:
                break
            current = current.f_back
        if current is None:
            return {"file": "", "function": "", "line": 0}
        info = inspect.getframeinfo(current)
        return {
            "file": str(Path(info.filename)),
            "function": str(info.function or "").strip(),
            "line": int(info.lineno or 0),
        }
    finally:
        del frame


def exception_details(exception: BaseException) -> dict[str, Any]:
    frames = traceback_module.extract_tb(exception.__traceback__) if exception.__traceback__ is not None else []
    frame = frames[-1] if frames else None
    return {
        "file": str(Path(frame.filename)) if frame is not None else "",
        "function": str(frame.name or "").strip() if frame is not None else "",
        "line": int(frame.lineno or 0) if frame is not None else 0,
        "exception_type": type(exception).__name__,
        "exception_message": str(exception or "").strip(),
        "traceback": "".join(
            traceback_module.format_exception(type(exception), exception, exception.__traceback__)
        ),
    }


def normalize_reason_code(
    reason: str = "",
    *,
    exception: BaseException | None = None,
    default: str = "unexpected_exception",
) -> str:
    values = [str(reason or "").strip().lower()]
    if exception is not None:
        values.append(str(exception or "").strip().lower())
    for value in values:
        if not value:
            continue
        if value in _DIRECT_REASON_CODES:
            return _DIRECT_REASON_CODES[value]
        if "persistent_profile_missing" in value:
            return "storage_state_missing"
        if "storage_state_missing" in value:
            return "storage_state_missing"
        if "storage_state_invalid" in value or value.startswith("session_invalid"):
            return "storage_state_invalid"
        if "network_identity_mismatch" in value:
            return "network_identity_mismatch"
        if "proxy" in value and any(token in value for token in ("blocked", "unreachable", "timeout", "refused", "error")):
            return "proxy_unreachable"
        if "composer_not_found" in value:
            return "composer_not_found"
        if "thread_locked_for_manual" in value or "not_touchable" in value:
            return "thread_not_touchable"
        if "manual_takeover" in value:
            return "job_cancelled_by_takeover"
        if "runtime_stop" in value or value == "runtime_inactive":
            return "job_cancelled_by_runtime_stop"
        if "thread_not_open" in value:
            return "thread_not_opened"
        if "browser" in value and "timeout" in value:
            return "browser_launch_timeout"
        if any(token in value for token in ("pw-ctx-failed", "driver_crash", "chrome_error", "instagram_navigation_failed")):
            return "browser_launch_failed"
        if re.fullmatch(r"[a-z0-9_]+", value):
            return value
    if exception is not None:
        return default
    normalized = re.sub(r"[^a-z0-9]+", "_", str(reason or "").strip().lower()).strip("_")
    return normalized or default


def record_inbox_diagnostic(
    target: Any,
    *,
    event_type: str,
    stage: str,
    outcome: str,
    account_id: str = "",
    alias_id: str = "",
    thread_key: str = "",
    job_type: str = "",
    reason: str = "",
    reason_code: str = "",
    exception: BaseException | None = None,
    payload: dict[str, Any] | None = None,
    created_at: float | None = None,
    callsite_skip: int = 1,
) -> int:
    writer = getattr(target, "record_diagnostic_event", None)
    if not callable(writer) and isinstance(target, dict):
        writer = getattr(target.get("_inbox_diagnostics_store"), "record_diagnostic_event", None)
    if not callable(writer):
        return 0
    details = exception_details(exception) if exception is not None else {
        **_capture_callsite(skip=callsite_skip + 1),
        "exception_type": "",
        "exception_message": "",
        "traceback": "",
    }
    clean_reason = str(reason or details.get("exception_message") or "").strip()
    clean_reason_code = normalize_reason_code(
        reason_code or clean_reason,
        exception=exception,
    )
    return int(
        writer(
            account_id=str(account_id or "").strip().lstrip("@").lower(),
            alias_id=str(alias_id or "").strip(),
            thread_key=str(thread_key or "").strip(),
            job_type=str(job_type or "").strip().lower(),
            stage=str(stage or "").strip().lower(),
            event_type=str(event_type or "").strip().lower(),
            outcome=str(outcome or "").strip().lower(),
            reason_code=clean_reason_code,
            reason=clean_reason,
            file=str(details.get("file") or "").strip(),
            function=str(details.get("function") or "").strip(),
            line=int(details.get("line") or 0),
            exception_type=str(details.get("exception_type") or "").strip(),
            exception_message=str(details.get("exception_message") or "").strip(),
            traceback=str(details.get("traceback") or ""),
            payload=_json_safe(dict(payload or {})),
            created_at=created_at,
        )
        or 0
    )
