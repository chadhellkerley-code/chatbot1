# -*- coding: utf-8 -*-
"""
Account health storage (Playwright-driven).

This module is intentionally dependency-light so it can be imported from both
CLI menus (accounts.py) and Playwright flows (src/*) without causing circular
imports or pulling in API clients.

Storage format is kept compatible with the previous account health cache:
data/account_health.json -> { "<username>": {"timestamp": "<iso>", "badge": "<text>"} }.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

from paths import runtime_base

logger = logging.getLogger(__name__)

BASE = runtime_base(Path(__file__).resolve().parent)
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
HEALTH_FILE = DATA_DIR / "account_health.json"

_TTL = timedelta(minutes=15)
_LOCK = Lock()
_CACHE: dict[str, tuple[datetime, str]] = {}


def _key(username: str) -> str:
    return (username or "").strip().lstrip("@").lower()


def _load_from_disk() -> None:
    if not HEALTH_FILE.exists():
        return
    try:
        raw = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(raw, dict):
        return
    loaded: dict[str, tuple[datetime, str]] = {}
    for k, entry in raw.items():
        if not isinstance(k, str) or not isinstance(entry, dict):
            continue
        ts_raw = entry.get("timestamp")
        badge = entry.get("badge")
        if not ts_raw or not badge:
            continue
        try:
            ts = datetime.fromisoformat(str(ts_raw))
        except Exception:
            continue
        loaded[k] = (ts, str(badge))
    if not loaded:
        return
    with _LOCK:
        _CACHE.update(loaded)


def _persist_to_disk() -> None:
    try:
        with _LOCK:
            serializable = {
                k: {"timestamp": ts.isoformat(), "badge": badge}
                for k, (ts, badge) in _CACHE.items()
            }
        HEALTH_FILE.write_text(json.dumps(serializable, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return


_load_from_disk()


def get_badge(username: str) -> Tuple[Optional[str], bool]:
    """Return (badge, expired)."""

    k = _key(username)
    if not k:
        return None, True
    with _LOCK:
        cached = _CACHE.get(k)
    if not cached:
        return None, True
    ts, badge = cached
    expired = (datetime.utcnow() - ts) >= _TTL
    return badge, expired


def set_badge(username: str, badge: str) -> str:
    """Persist a badge for account health (best-effort)."""

    k = _key(username)
    if not k:
        return badge
    with _LOCK:
        _CACHE[k] = (datetime.utcnow(), str(badge))
    _persist_to_disk()
    return badge


def invalidate(username: str) -> None:
    k = _key(username)
    if not k:
        return
    with _LOCK:
        if k in _CACHE:
            _CACHE.pop(k, None)
    _persist_to_disk()


def _log_health(username: str, state: str, reason: str = "") -> None:
    user = (username or "").strip().lstrip("@")
    if not user:
        return
    normalized = (state or "").strip().upper()
    if normalized == "ALIVE":
        # Keep message prefix stable for log filtering.
        logger.info("[HEALTH CHECK] Account ALIVE account=@%s", user)
        return
    if reason:
        logger.info("[HEALTH CHECK] Account BLOCKED - reason: %s account=@%s", reason, user)
    else:
        logger.info("[HEALTH CHECK] Account BLOCKED account=@%s", user)


def mark_alive(username: str, *, reason: str = "") -> str:
    badge = "[✅ OK]"
    set_badge(username, badge)
    _log_health(username, "ALIVE", reason)
    return badge


def mark_blocked(username: str, *, reason: str) -> str:
    # Keep "bloqueada" keyword for downstream badge consumers (UI/export).
    badge = f"[🔴 Bloqueada] {reason}".strip()
    set_badge(username, badge)
    _log_health(username, "BLOCKED", reason)
    return badge


def mark_session_expired(username: str, *, reason: str = "session_expired") -> str:
    badge = "[⚠️ Sesión expirada]"
    set_badge(username, badge)
    _log_health(username, "BLOCKED", reason)
    return badge


def mark_unknown(username: str, *, reason: str = "unknown") -> str:
    badge = f"[🟡 En riesgo: unknown] {reason}".strip()
    set_badge(username, badge)
    _log_health(username, "BLOCKED", reason)
    return badge


def update_from_playwright_status(username: str, status: str, *, reason: str = "") -> str:
    """
    Map Playwright-detected status into a stored badge.
    Expected statuses: alive, session_expired, checkpoint, blocked, suspended, unknown.
    """

    normalized = (status or "").strip().lower() or "unknown"
    if normalized == "alive":
        return mark_alive(username, reason=reason or "inbox_accessible")
    if normalized in {"session_expired"}:
        return mark_session_expired(username, reason=reason or normalized)
    if normalized in {"checkpoint", "blocked", "suspended"}:
        return mark_blocked(username, reason=reason or normalized)
    return mark_unknown(username, reason=reason or normalized)
