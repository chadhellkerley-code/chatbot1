from __future__ import annotations

import inspect
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.storage_atomic import atomic_write_json, load_json_file, path_lock
from src.browser_profile_paths import (
    browser_profile_lifecycle_diagnostics_path,
    browser_profile_lifecycle_path,
    canonical_browser_profile_path,
)

logger = logging.getLogger("src.browser_profile_lifecycle")

LIFECYCLE_STATE_CLEAN = "clean"
LIFECYCLE_STATE_OPEN = "open"
LIFECYCLE_STATE_CLOSING = "closing"
LIFECYCLE_STATE_UNCLEAN = "unclean_shutdown_detected"

_KNOWN_STATES = {
    LIFECYCLE_STATE_CLEAN,
    LIFECYCLE_STATE_OPEN,
    LIFECYCLE_STATE_CLOSING,
    LIFECYCLE_STATE_UNCLEAN,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_account(value: Any, *, fallback: str) -> str:
    return str(value or "").strip().lstrip("@") or fallback


def _normalize_mode(value: Any) -> str:
    clean = str(value or "").strip().lower()
    if clean in {"headless", "headful"}:
        return clean
    return "unknown"


def _normalize_subsystem(value: Any) -> str:
    clean = re.sub(r"[^a-z0-9_-]+", "_", str(value or "").strip().lower()).strip("_")
    return clean or "default"


def _normalize_reason(value: Any) -> str:
    clean = re.sub(r"[^a-z0-9_.:-]+", "_", str(value or "").strip().lower()).strip("_")
    return clean or "unspecified"


def _caller_payload(*, skip: int = 2) -> dict[str, Any]:
    frame = inspect.currentframe()
    try:
        current = frame
        for _ in range(max(0, int(skip))):
            current = current.f_back if current is not None else None
        if current is None:
            return {}
        code = current.f_code
        return {
            "file": str(code.co_filename or ""),
            "function": str(code.co_name or ""),
            "line": int(current.f_lineno or 0),
        }
    finally:
        del frame


def _default_metadata(profile_path: Path) -> dict[str, Any]:
    normalized = canonical_browser_profile_path(profile_path)
    return {
        "account_username": normalized.name,
        "profile_path": str(normalized),
        "mode": "",
        "subsystem": "",
        "pid": None,
        "opened_at": "",
        "closed_at": "",
        "last_clean_shutdown": "",
        "last_unclean_shutdown_reason": "",
        "lifecycle_state": LIFECYCLE_STATE_CLEAN,
        "open_count": 0,
        "owners": [],
    }


def load_profile_lifecycle(profile_dir: str | Path) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    path = browser_profile_lifecycle_path(profile_path)
    return dict(load_json_file(path, _default_metadata(profile_path), label="profile_lifecycle"))


def _write_diagnostic_line(path: Path, payload: dict[str, Any]) -> None:
    lock = path_lock(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    with lock:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")


def emit_profile_lifecycle_diagnostic(
    *,
    event_type: str,
    profile_dir: str | Path,
    account: Any = "",
    subsystem: Any = "",
    mode: Any = "",
    reason_code: Any = "",
    pid: int | None = None,
    owner_token: str = "",
    payload: dict[str, Any] | None = None,
    callsite_skip: int = 1,
) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    diagnostic = {
        "event_type": str(event_type or "").strip() or "profile_lifecycle_event",
        "timestamp": _utc_now_iso(),
        "account": _normalize_account(account, fallback=profile_path.name),
        "subsystem": _normalize_subsystem(subsystem),
        "mode": _normalize_mode(mode),
        "profile_path": str(profile_path),
        "reason_code": _normalize_reason(reason_code) if reason_code else "",
        "pid": int(pid) if pid is not None else None,
        "owner_token": str(owner_token or "").strip(),
    }
    diagnostic.update(_caller_payload(skip=max(1, int(callsite_skip)) + 1))
    if payload:
        diagnostic["payload"] = dict(payload)
    _write_diagnostic_line(browser_profile_lifecycle_diagnostics_path(profile_path), diagnostic)
    logger.info("profile_lifecycle %s", json.dumps(diagnostic, ensure_ascii=False, sort_keys=True))
    return diagnostic


def _normalized_owner_list(value: Any) -> list[dict[str, Any]]:
    owners: list[dict[str, Any]] = []
    for raw in list(value or []):
        if not isinstance(raw, dict):
            continue
        token = str(raw.get("owner_token") or "").strip()
        if not token:
            continue
        owners.append(
            {
                "owner_token": token,
                "subsystem": _normalize_subsystem(raw.get("subsystem")),
                "mode": _normalize_mode(raw.get("mode")),
                "pid": int(raw.get("pid")) if raw.get("pid") not in {None, ""} else None,
                "opened_at": str(raw.get("opened_at") or "").strip(),
            }
        )
    return owners


def _update_lifecycle_metadata(*, profile_path: Path, mutate: Any) -> dict[str, Any]:
    path = browser_profile_lifecycle_path(profile_path)
    lock = path_lock(path)
    with lock:
        metadata = dict(load_json_file(path, _default_metadata(profile_path), label="profile_lifecycle"))
        metadata = dict(mutate(metadata))
        metadata["profile_path"] = str(profile_path)
        state = str(metadata.get("lifecycle_state") or "").strip().lower()
        metadata["lifecycle_state"] = state if state in _KNOWN_STATES else LIFECYCLE_STATE_CLEAN
        metadata["owners"] = _normalized_owner_list(metadata.get("owners"))
        metadata["open_count"] = max(0, int(metadata.get("open_count") or len(metadata["owners"])))
        atomic_write_json(path, metadata)
        return metadata


def mark_profile_open(
    *,
    account: Any,
    profile_dir: str | Path,
    subsystem: Any,
    mode: Any,
    pid: int | None = None,
    owner_token: str,
    owner_hold_count: int | None = None,
) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    account_value = _normalize_account(account, fallback=profile_path.name)
    subsystem_value = _normalize_subsystem(subsystem)
    mode_value = _normalize_mode(mode)
    pid_value = int(pid) if pid is not None else os.getpid()
    owner_value = str(owner_token or "").strip()
    timestamp = _utc_now_iso()
    recovery_events: list[tuple[str, str, dict[str, Any]]] = []

    def _mutate(metadata: dict[str, Any]) -> dict[str, Any]:
        previous_state = str(metadata.get("lifecycle_state") or "").strip().lower()
        previous_owners = _normalized_owner_list(metadata.get("owners"))
        previous_open_count = max(0, int(metadata.get("open_count") or len(previous_owners)))
        current_hold_count = max(0, int(owner_hold_count or 0))
        live_expansion = current_hold_count > previous_open_count
        if (
            not live_expansion
            and (previous_state in {LIFECYCLE_STATE_OPEN, LIFECYCLE_STATE_CLOSING} or previous_open_count > 0 or previous_owners)
        ):
            reason = f"previous_state_{previous_state or LIFECYCLE_STATE_OPEN}"
            recovery_events.append(
                (
                    "profile_unclean_shutdown_detected",
                    reason,
                    {
                        "previous_state": previous_state or "",
                        "previous_open_count": previous_open_count,
                    },
                )
            )
            recovery_events.append(
                (
                    "lifecycle_state_recovered",
                    "recovered_from_previous_open_state",
                    {
                        "previous_state": previous_state or "",
                        "previous_open_count": previous_open_count,
                    },
                )
            )
            previous_owners = []
            metadata["closed_at"] = timestamp
            metadata["last_unclean_shutdown_reason"] = reason
        elif previous_state == LIFECYCLE_STATE_UNCLEAN:
            recovery_events.append(
                (
                    "lifecycle_state_recovered",
                    str(metadata.get("last_unclean_shutdown_reason") or "recovered_from_unclean_shutdown"),
                    {"previous_state": previous_state},
                )
            )
            previous_owners = []

        owners = [owner for owner in previous_owners if owner.get("owner_token") != owner_value]
        owners.append(
            {
                "owner_token": owner_value,
                "subsystem": subsystem_value,
                "mode": mode_value,
                "pid": pid_value,
                "opened_at": timestamp,
            }
        )
        metadata.update(
            {
                "account_username": account_value,
                "profile_path": str(profile_path),
                "mode": mode_value,
                "subsystem": subsystem_value,
                "pid": pid_value,
                "opened_at": timestamp,
                "lifecycle_state": LIFECYCLE_STATE_OPEN,
                "open_count": max(len(owners), current_hold_count or len(owners)),
                "owners": owners,
            }
        )
        return metadata

    metadata = _update_lifecycle_metadata(profile_path=profile_path, mutate=_mutate)
    for event_type, reason_code, payload in recovery_events:
        emit_profile_lifecycle_diagnostic(
            event_type=event_type,
            profile_dir=profile_path,
            account=account_value,
            subsystem=subsystem_value,
            mode=mode_value,
            reason_code=reason_code,
            pid=pid_value,
            owner_token=owner_value,
            payload=payload,
            callsite_skip=2,
        )
    emit_profile_lifecycle_diagnostic(
        event_type="profile_opened",
        profile_dir=profile_path,
        account=account_value,
        subsystem=subsystem_value,
        mode=mode_value,
        reason_code="profile_opened",
        pid=pid_value,
        owner_token=owner_value,
        payload={"open_count": int(metadata.get("open_count") or 0)},
        callsite_skip=2,
    )
    return metadata


def mark_profile_closing(
    *,
    account: Any,
    profile_dir: str | Path,
    subsystem: Any,
    mode: Any,
    pid: int | None = None,
    owner_token: str,
) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    account_value = _normalize_account(account, fallback=profile_path.name)
    subsystem_value = _normalize_subsystem(subsystem)
    mode_value = _normalize_mode(mode)
    pid_value = int(pid) if pid is not None else os.getpid()
    owner_value = str(owner_token or "").strip()

    def _mutate(metadata: dict[str, Any]) -> dict[str, Any]:
        owners = _normalized_owner_list(metadata.get("owners"))
        if len(owners) <= 1:
            metadata["lifecycle_state"] = LIFECYCLE_STATE_CLOSING
        metadata.update(
            {
                "account_username": account_value,
                "profile_path": str(profile_path),
                "mode": mode_value,
                "subsystem": subsystem_value,
                "pid": pid_value,
            }
        )
        return metadata

    metadata = _update_lifecycle_metadata(profile_path=profile_path, mutate=_mutate)
    emit_profile_lifecycle_diagnostic(
        event_type="profile_closing",
        profile_dir=profile_path,
        account=account_value,
        subsystem=subsystem_value,
        mode=mode_value,
        reason_code="profile_closing",
        pid=pid_value,
        owner_token=owner_value,
        payload={"open_count": int(metadata.get("open_count") or 0)},
        callsite_skip=2,
    )
    return metadata


def mark_profile_closed_cleanly(
    *,
    account: Any,
    profile_dir: str | Path,
    subsystem: Any,
    mode: Any,
    pid: int | None = None,
    owner_token: str,
) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    account_value = _normalize_account(account, fallback=profile_path.name)
    subsystem_value = _normalize_subsystem(subsystem)
    mode_value = _normalize_mode(mode)
    pid_value = int(pid) if pid is not None else os.getpid()
    owner_value = str(owner_token or "").strip()
    timestamp = _utc_now_iso()
    recovered_owner_marker = {"value": False}

    def _mutate(metadata: dict[str, Any]) -> dict[str, Any]:
        owners = _normalized_owner_list(metadata.get("owners"))
        filtered = [owner for owner in owners if owner.get("owner_token") != owner_value]
        if len(filtered) == len(owners) and len(owners) <= 1:
            recovered_owner_marker["value"] = True
            filtered = []
        metadata.update(
            {
                "account_username": account_value,
                "profile_path": str(profile_path),
                "mode": mode_value,
                "subsystem": subsystem_value,
                "pid": pid_value,
                "closed_at": timestamp,
                "owners": filtered,
                "open_count": len(filtered),
            }
        )
        if filtered:
            metadata["lifecycle_state"] = LIFECYCLE_STATE_OPEN
        else:
            metadata["lifecycle_state"] = LIFECYCLE_STATE_CLEAN
            metadata["last_clean_shutdown"] = timestamp
        return metadata

    metadata = _update_lifecycle_metadata(profile_path=profile_path, mutate=_mutate)
    if recovered_owner_marker["value"]:
        emit_profile_lifecycle_diagnostic(
            event_type="lifecycle_state_recovered",
            profile_dir=profile_path,
            account=account_value,
            subsystem=subsystem_value,
            mode=mode_value,
            reason_code="owner_marker_recovered_on_close",
            pid=pid_value,
            owner_token=owner_value,
            callsite_skip=2,
        )
    emit_profile_lifecycle_diagnostic(
        event_type="profile_closed_cleanly",
        profile_dir=profile_path,
        account=account_value,
        subsystem=subsystem_value,
        mode=mode_value,
        reason_code="profile_closed_cleanly",
        pid=pid_value,
        owner_token=owner_value,
        payload={"open_count": int(metadata.get("open_count") or 0)},
        callsite_skip=2,
    )
    return metadata


def mark_profile_unclean_shutdown(
    *,
    account: Any,
    profile_dir: str | Path,
    subsystem: Any,
    mode: Any,
    reason_code: Any,
    pid: int | None = None,
    owner_token: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile_path = canonical_browser_profile_path(profile_dir)
    account_value = _normalize_account(account, fallback=profile_path.name)
    subsystem_value = _normalize_subsystem(subsystem)
    mode_value = _normalize_mode(mode)
    pid_value = int(pid) if pid is not None else os.getpid()
    owner_value = str(owner_token or "").strip()
    reason_value = _normalize_reason(reason_code)
    timestamp = _utc_now_iso()

    def _mutate(metadata: dict[str, Any]) -> dict[str, Any]:
        metadata.update(
            {
                "account_username": account_value,
                "profile_path": str(profile_path),
                "mode": mode_value,
                "subsystem": subsystem_value,
                "pid": pid_value,
                "closed_at": timestamp,
                "last_unclean_shutdown_reason": reason_value,
                "lifecycle_state": LIFECYCLE_STATE_UNCLEAN,
                "owners": [],
                "open_count": 0,
            }
        )
        return metadata

    metadata = _update_lifecycle_metadata(profile_path=profile_path, mutate=_mutate)
    emit_profile_lifecycle_diagnostic(
        event_type="profile_unclean_shutdown_detected",
        profile_dir=profile_path,
        account=account_value,
        subsystem=subsystem_value,
        mode=mode_value,
        reason_code=reason_value,
        pid=pid_value,
        owner_token=owner_value,
        payload=payload,
        callsite_skip=2,
    )
    return metadata
