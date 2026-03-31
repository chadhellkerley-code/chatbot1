from __future__ import annotations

import json
import logging
import re
from typing import Any


logger = logging.getLogger("src.browser_telemetry")

_ALLOWED_STAGES = {
    "spawn",
    "session_open_start",
    "session_open_end",
    "browser_open",
    "sender_job_received",
    "page_attached",
    "workspace_ready",
    "inbox_ready",
    "thread_open_start",
    "thread_open",
    "thread_open_ok",
    "composer_wait_start",
    "composer_ready",
    "type_start",
    "typing_started",
    "send_triggered",
    "send_attempt",
    "confirm_start",
    "confirm_ok",
    "timezone_resolved",
    "send_success",
    "send_fail",
    "exact_fail_reason",
    "session_recovery",
}

_ALLOWED_STATUSES = {
    "started",
    "ok",
    "failed",
    "reused",
}


def _clean_token(value: Any, *, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized or fallback


def _serialize(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) > 240:
        text = text[:237] + "..."
    return json.dumps(text, ensure_ascii=True)


def log_browser_stage(
    *,
    component: str,
    stage: str,
    status: str,
    account: str = "",
    lead: str = "",
    **fields: Any,
) -> None:
    clean_stage = _clean_token(stage, fallback="unknown")
    if clean_stage not in _ALLOWED_STAGES:
        raise ValueError(f"Unsupported browser telemetry stage: {stage}")

    clean_status = _clean_token(status, fallback="failed")
    if clean_status not in _ALLOWED_STATUSES:
        raise ValueError(f"Unsupported browser telemetry status: {status}")

    clean_component = _clean_token(component, fallback="browser")
    payload: list[str] = [
        f"component={clean_component}",
        f"stage={clean_stage}",
        f"status={clean_status}",
    ]

    if str(account or "").strip():
        payload.append(f"account={_serialize(str(account).strip().lstrip('@'))}")
    if str(lead or "").strip():
        payload.append(f"lead={_serialize(str(lead).strip().lstrip('@'))}")

    for key in sorted(fields.keys()):
        value = fields[key]
        if value is None:
            continue
        clean_key = _clean_token(key, fallback="")
        if not clean_key:
            continue
        payload.append(f"{clean_key}={_serialize(value)}")

    logger.info("browser_exec %s", " ".join(payload))
