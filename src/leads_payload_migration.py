from __future__ import annotations

import copy
from typing import Any, Dict, Tuple

DEFAULT_DELAY_MIN_SECONDS = 20.0
DEFAULT_DELAY_MAX_SECONDS = 40.0
DEFAULT_MAX_RUNTIME_SECONDS = 3600.0


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_run_section(run: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    normalized = dict(run or {})
    changed = False

    delay_min = _safe_float(normalized.get("delay_min"), DEFAULT_DELAY_MIN_SECONDS)
    if delay_min < 0:
        delay_min = 0.0
        changed = True

    delay_max = _safe_float(normalized.get("delay_max"), max(delay_min, DEFAULT_DELAY_MAX_SECONDS))
    if delay_max < delay_min:
        delay_max = delay_min
        changed = True

    max_runtime_raw = normalized.get("max_runtime_seconds")
    if max_runtime_raw is None:
        max_runtime_seconds = DEFAULT_MAX_RUNTIME_SECONDS
        changed = True
    else:
        max_runtime_seconds = _safe_float(max_runtime_raw, DEFAULT_MAX_RUNTIME_SECONDS)

    if 0.0 < max_runtime_seconds < 30.0:
        max_runtime_seconds = DEFAULT_MAX_RUNTIME_SECONDS
        changed = True

    if normalized.get("delay_min") != delay_min:
        normalized["delay_min"] = delay_min
        changed = True
    if normalized.get("delay_max") != delay_max:
        normalized["delay_max"] = delay_max
        changed = True
    if normalized.get("max_runtime_seconds") != max_runtime_seconds:
        normalized["max_runtime_seconds"] = max_runtime_seconds
        changed = True

    return normalized, changed


def _normalize_pending_retry_state(state_payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    normalized_state = dict(state_payload or {})
    pending_retry_state = dict(normalized_state.get("pending_retry_state") or {})
    changed = False
    migrated: Dict[str, Dict[str, Any]] = {}

    for idx, payload in pending_retry_state.items():
        retry_payload = dict(payload or {})
        profile_retry_count = int(
            retry_payload.get("profile_retry_count")
            or retry_payload.get("retry_count")
            or 0
        )
        profile_next_attempt_at = str(
            retry_payload.get("profile_next_attempt_at")
            or retry_payload.get("next_attempt_at")
            or ""
        )
        image_retry_count = int(retry_payload.get("image_retry_count") or 0)
        image_next_attempt_at = str(retry_payload.get("image_next_attempt_at") or "")
        migrated[str(idx)] = {
            "profile_retry_count": max(0, profile_retry_count),
            "profile_next_attempt_at": profile_next_attempt_at,
            "image_retry_count": max(0, image_retry_count),
            "image_next_attempt_at": image_next_attempt_at,
        }
        if (
            retry_payload.get("profile_retry_count") is None
            or retry_payload.get("profile_next_attempt_at") is None
            or "retry_count" in retry_payload
            or "next_attempt_at" in retry_payload
        ):
            changed = True

    if changed or normalized_state.get("pending_retry_state") != migrated:
        normalized_state["pending_retry_state"] = migrated
        normalized_state["schema"] = int(normalized_state.get("schema") or 1)
        changed = True

    return normalized_state, changed


def normalize_filter_list_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    normalized = copy.deepcopy(payload or {})
    changed = False

    run_section = dict(normalized.get("run") or {})
    normalized_run, run_changed = _normalize_run_section(run_section)
    if run_changed or normalized.get("run") != normalized_run:
        normalized["run"] = normalized_run
        changed = True

    pipeline_state = normalized.get("_pipeline_state")
    if isinstance(pipeline_state, dict):
        normalized_state, state_changed = _normalize_pending_retry_state(pipeline_state)
        if state_changed:
            normalized["_pipeline_state"] = normalized_state
            changed = True

    items = list(normalized.get("items") or [])
    for item in items:
        if not isinstance(item, dict):
            continue
        profile_retry_count = int(item.get("profile_retry_count") or item.get("retry_count") or 0)
        profile_next_attempt_at = str(item.get("profile_next_attempt_at") or item.get("next_attempt_at") or "")

        if int(item.get("profile_retry_count") or 0) != profile_retry_count:
            item["profile_retry_count"] = profile_retry_count
            changed = True
        if str(item.get("profile_next_attempt_at") or "") != profile_next_attempt_at:
            if profile_next_attempt_at:
                item["profile_next_attempt_at"] = profile_next_attempt_at
            else:
                item.pop("profile_next_attempt_at", None)
            changed = True

    return normalized, changed
