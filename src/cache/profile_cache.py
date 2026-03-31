from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


def _now() -> float:
    return time.time()


def _default_cache_path() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "data" / "instagram_profile_cache.sqlite3"


@dataclass
class CacheEntry:
    value: Dict[str, Any]
    expires_at: float


class ProfileCache:
    def __init__(self, *, path: Path | None = None, ttl_seconds: float = 24.0 * 3600.0) -> None:
        self._path = Path(os.getenv("IG_PUBLIC_PROFILE_CACHE_PATH") or (path or _default_cache_path()))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._ttl = float(ttl_seconds)
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at REAL NOT NULL)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at)")
            conn.commit()

    def _key(self, username: str) -> str:
        normalized = str(username or "").strip().lstrip("@").lower()
        return f"profile:{normalized}"

    async def get(self, username: str) -> Optional[Dict[str, Any]]:
        key = self._key(username)
        now = _now()
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, key, now)

    def _get_sync(self, key: str, now: float) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self._path) as conn:
            row = conn.execute("SELECT value, expires_at FROM cache WHERE key = ?", (key,)).fetchone()
            if not row:
                return None
            value_raw, expires_at = row
            if float(expires_at or 0.0) <= now:
                conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
                return None
            try:
                parsed = json.loads(value_raw)
            except Exception:
                conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
                return None
            return parsed if isinstance(parsed, dict) else None

    async def set(self, username: str, value: Dict[str, Any]) -> None:
        key = self._key(username)
        expires_at = _now() + self._ttl
        payload = json.dumps(dict(value or {}), ensure_ascii=False, separators=(",", ":"))
        async with self._lock:
            await asyncio.to_thread(self._set_sync, key, payload, expires_at)

    def _set_sync(self, key: str, payload: str, expires_at: float) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache(key, value, expires_at) VALUES (?, ?, ?)",
                (key, payload, float(expires_at)),
            )
            conn.commit()

