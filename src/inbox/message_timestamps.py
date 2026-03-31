from __future__ import annotations

from typing import Any


_RAW_CANONICAL_KEYS = ("timestamp", "confirmed_at", "created_at")


def coerce_message_timestamp_value(value: Any) -> float | None:
    try:
        stamp = float(value)
    except Exception:
        return None
    return stamp if stamp > 0 else None


def message_timestamp_info(message: dict[str, Any] | None) -> tuple[float | None, str]:
    payload = message if isinstance(message, dict) else {}
    explicit_canonical = coerce_message_timestamp_value(payload.get("message_ts_canonical"))
    if explicit_canonical is not None:
        explicit_source = str(payload.get("message_ts_source") or "").strip() or "message_ts_canonical"
        return explicit_canonical, explicit_source
    for key in _RAW_CANONICAL_KEYS:
        stamp = coerce_message_timestamp_value(payload.get(key))
        if stamp is not None:
            return stamp, key
    return None, ""


def message_canonical_timestamp(message: dict[str, Any] | None) -> float | None:
    stamp, _source = message_timestamp_info(message)
    return stamp


def message_sort_key(message: dict[str, Any], *, position: int = 0) -> tuple[float, float, str, int]:
    canonical = message_canonical_timestamp(message) or 0.0
    created_at = coerce_message_timestamp_value((message or {}).get("created_at")) or canonical
    identity = (
        str((message or {}).get("block_id") or "").strip()
        or str((message or {}).get("external_message_id") or "").strip()
        or str((message or {}).get("message_id") or "").strip()
        or str((message or {}).get("text") or "").strip()
    )
    return (canonical, created_at, identity, max(0, int(position or 0)))


def annotate_message_timestamps(message: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(message or {})
    stamp, source = message_timestamp_info(payload)
    if stamp is None:
        payload.pop("message_ts_canonical", None)
        payload.pop("message_ts_source", None)
        return payload
    payload["message_ts_canonical"] = stamp
    payload["message_ts_source"] = source
    return payload
