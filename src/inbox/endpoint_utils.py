from __future__ import annotations

import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.dm_playwright_client import (
    THREAD_URL_TEMPLATE,
    _build_inbox_endpoint_candidates,
    _extract_api_messages_from_payload,
)


def _append_cache_bust_query(url: str, *, nonce: int | None = None) -> str:
    clean_url = str(url or "").strip()
    if not clean_url:
        return ""
    parts = urlsplit(clean_url)
    query_pairs = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "_cb"]
    query_pairs.append(("_cb", str(int(nonce if nonce is not None else (time.time() * 1000.0)))))
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query_pairs, doseq=True),
            parts.fragment,
        )
    )


def _build_inbox_candidate_urls(*, cursor: str, limit: int, message_limit: int, nonce: int | None = None) -> list[str]:
    safe_limit = max(1, min(200, int(limit or 20)))
    safe_message_limit = max(1, min(80, int(message_limit or 12)))
    request_nonce = int(nonce if nonce is not None else (time.time() * 1000.0))
    urls: list[str] = []
    for candidate in _build_inbox_endpoint_candidates(
        cursor=str(cursor or "").strip(),
        limit=safe_limit,
        message_limit=safe_message_limit,
    ):
        cache_busted = _append_cache_bust_query(candidate, nonce=request_nonce)
        if cache_busted not in urls:
            urls.append(cache_busted)
    return urls


def _snapshot_to_thread_row(
    snapshot: dict[str, Any],
    *,
    account_id: str,
    account_alias: str,
) -> dict[str, Any] | None:
    thread_id = str(snapshot.get("thread_id") or "").strip()
    if not thread_id:
        return None
    messages = snapshot.get("messages")
    preview_messages = _normalize_preview_messages(messages)
    latest = preview_messages[-1] if preview_messages else None
    latest_customer_message_at: float | None = None
    for message in preview_messages:
        if str(message.get("direction") or "").strip().lower() == "inbound":
            latest_customer_message_at = message.get("timestamp")
    try:
        unread_count = max(0, int(snapshot.get("unread_count") or 0))
    except Exception:
        unread_count = 0
    fallback_activity_at = None
    try:
        fallback_activity_at = (
            float(snapshot.get("last_activity_at")) if snapshot.get("last_activity_at") is not None else None
        )
    except Exception:
        fallback_activity_at = None
    latest_direction = str((latest or {}).get("direction") or "").strip().lower()
    if latest_direction not in {"inbound", "outbound", "unknown"}:
        latest_direction = "unknown"
    if latest_customer_message_at is None and unread_count > 0 and fallback_activity_at is not None:
        latest_customer_message_at = fallback_activity_at
    if latest_direction == "unknown" and unread_count > 0:
        latest_direction = "inbound"
    display_name = (
        str(snapshot.get("title") or "").strip()
        or str(snapshot.get("recipient_username") or "").strip()
        or thread_id
    )
    return {
        "thread_key": f"{account_id}:{thread_id}",
        "thread_id": thread_id,
        "thread_href": str(snapshot.get("thread_href") or "").strip() or THREAD_URL_TEMPLATE.format(thread_id=thread_id),
        "account_id": account_id,
        "account_alias": account_alias,
        "recipient_username": str(snapshot.get("recipient_username") or "").strip(),
        "display_name": display_name,
        "last_message_text": str((latest or {}).get("text") or snapshot.get("snippet") or "").strip(),
        "last_message_timestamp": (latest or {}).get("timestamp"),
        "last_message_direction": latest_direction or "unknown",
        "last_message_id": str((latest or {}).get("message_id") or "").strip(),
        "unread_count": unread_count,
        "participants": [str(snapshot.get("recipient_username") or "").strip() or display_name],
        "last_activity_timestamp": (latest or {}).get("timestamp") or fallback_activity_at,
        "latest_customer_message_at": latest_customer_message_at,
        "preview_messages": preview_messages,
    }


def _coerce_preview_timestamp(raw: dict[str, Any]) -> float | None:
    for key in ("timestamp", "timestamp_epoch"):
        try:
            stamp = float(raw.get(key)) if raw.get(key) is not None else None
        except Exception:
            stamp = None
        if stamp is not None:
            return stamp
    return None


def _normalize_preview_messages(raw_messages: Any) -> list[dict[str, Any]]:
    rows = raw_messages if isinstance(raw_messages, list) else []
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        message_id = str(raw.get("message_id") or raw.get("id") or "").strip()
        if not message_id:
            continue
        direction = str(raw.get("direction") or "").strip().lower() or "unknown"
        if direction not in {"inbound", "outbound", "unknown"}:
            direction = "unknown"
        timestamp = _coerce_preview_timestamp(raw)
        normalized.append(
            {
                "message_id": message_id,
                "text": str(raw.get("text") or "").strip(),
                "timestamp": timestamp,
                "direction": direction,
            }
        )
    normalized.sort(key=lambda item: item.get("timestamp") or 0.0)
    return normalized


def _payload_to_messages(payload: Any, *, self_user_id: str) -> list[dict[str, Any]]:
    parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self_user_id)
    rows: list[dict[str, Any]] = []
    for message in parsed:
        message_id = str(getattr(message, "item_id", "") or "").strip()
        if not message_id:
            continue
        try:
            timestamp = float(getattr(message, "timestamp", 0.0) or 0.0)
        except Exception:
            timestamp = None
        direction = str(getattr(message, "direction", "") or "").strip().lower() or "unknown"
        if direction not in {"inbound", "outbound", "unknown"}:
            direction = "unknown"
        rows.append(
            {
                "message_id": message_id,
                "text": str(getattr(message, "text", "") or "").strip(),
                "timestamp": timestamp,
                "direction": direction,
                "user_id": str(getattr(message, "sender_id", "") or "").strip(),
                "delivery_status": "sent",
                "local_echo": False,
            }
        )
    rows.sort(key=lambda item: item.get("timestamp") or 0.0)
    return rows
