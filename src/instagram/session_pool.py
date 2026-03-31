from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from src.network.http_client import random_accept_language, random_user_agent
from src.network.proxy_pool import ProxyPool, ProxyState


def _now() -> float:
    return time.time()


def _normalize_proxy_url(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    return candidate or None


def _proxy_pool() -> ProxyPool:
    env_list = os.getenv("IG_PUBLIC_PROXY_URLS") or os.getenv("IG_PROXY_URLS") or ""
    proxies = [line.strip() for line in env_list.replace(",", "\n").splitlines() if line.strip()]
    if proxies:
        return ProxyPool(proxies)
    return ProxyPool.from_proxy_registry()


@dataclass
class SessionContext:
    session_id: str
    proxy_url: str | None
    headers: Dict[str, str]
    cookies: Dict[str, str]
    user_agent: str
    device_id: str
    last_used_timestamp: float = 0.0
    request_count: int = 0

    proxy_state: ProxyState | None = None
    assigned_proxy_url: str | None = None
    proxy_override_url: str | None = None

    in_use: bool = False
    cooldown_until: float = 0.0
    consecutive_failures: int = 0


def _default_session_headers(*, user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": str(user_agent or "").strip() or random_user_agent(),
        "Accept": "*/*",
        "Accept-Language": random_accept_language(),
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
    }


class SessionPool:
    def __init__(
        self,
        *,
        pool_size: int = 6,
        max_requests_per_session: int = 30,
        rate_limit_cooldown_seconds: float = 120.0,
        failure_cooldown_seconds: float = 30.0,
    ) -> None:
        self._pool_size = max(1, int(pool_size))
        self._max_requests = max(1, int(max_requests_per_session))
        self._cooldown_429 = max(1.0, float(rate_limit_cooldown_seconds))
        self._cooldown_fail = max(1.0, float(failure_cooldown_seconds))

        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)
        self._sessions: list[SessionContext] = []
        self._rr_index = 0

        self._proxy_pool: ProxyPool | None = None

    async def _ensure_initialized(self) -> None:
        async with self._lock:
            if self._sessions:
                return
            for _ in range(self._pool_size):
                session = self._new_session()
                self._sessions.append(session)

    def _get_proxy_pool(self) -> ProxyPool:
        if self._proxy_pool is None:
            self._proxy_pool = _proxy_pool()
        return self._proxy_pool

    def _new_session(self) -> SessionContext:
        device_id = str(uuid.uuid4())
        user_agent = random_user_agent()
        session_id = f"sp_{uuid.uuid4().hex[:12]}"
        cookies = {
            "ig_did": device_id,
            "ig_nrcb": "1",
        }
        headers = _default_session_headers(user_agent=user_agent)
        return SessionContext(
            session_id=session_id,
            proxy_url=None,
            headers=headers,
            cookies=cookies,
            user_agent=user_agent,
            device_id=device_id,
            last_used_timestamp=0.0,
            request_count=0,
            proxy_state=None,
            assigned_proxy_url=None,
            proxy_override_url=None,
            in_use=False,
            cooldown_until=0.0,
            consecutive_failures=0,
        )

    async def _maybe_recycle_session(self, session: SessionContext) -> None:
        if session.request_count < self._max_requests:
            return
        recycled = self._new_session()
        session.session_id = recycled.session_id
        session.headers = recycled.headers
        session.cookies = recycled.cookies
        session.user_agent = recycled.user_agent
        session.device_id = recycled.device_id
        session.last_used_timestamp = 0.0
        session.request_count = 0
        session.proxy_state = None
        session.assigned_proxy_url = None
        session.proxy_override_url = None
        session.cooldown_until = 0.0
        session.consecutive_failures = 0
        session.proxy_url = None

    async def _reserve_available_session(self) -> SessionContext:
        while True:
            now = _now()
            next_ready_at: float | None = None
            for offset in range(len(self._sessions)):
                idx = (self._rr_index + offset) % len(self._sessions)
                candidate = self._sessions[idx]
                if candidate.in_use:
                    continue
                cooldown_until = float(candidate.cooldown_until or 0.0)
                if cooldown_until > now:
                    if next_ready_at is None or cooldown_until < next_ready_at:
                        next_ready_at = cooldown_until
                    continue
                await self._maybe_recycle_session(candidate)
                candidate.in_use = True
                self._rr_index = (idx + 1) % len(self._sessions)
                print(
                    f"[SESSION_POOL_SELECT] session_id={candidate.session_id} "
                    f"proxy={candidate.proxy_url or '-'} "
                    f"requests={candidate.request_count}"
                )
                return candidate

            delay = 0.5
            if next_ready_at is not None:
                delay = max(0.05, next_ready_at - now)
            try:
                await asyncio.wait_for(self._cond.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue

    async def acquire_session(self) -> SessionContext:
        await self._ensure_initialized()
        async with self._lock:
            return await self._reserve_available_session()

    async def ensure_assigned_proxy(self, session: SessionContext) -> None:
        async with self._lock:
            if session.proxy_override_url:
                session.proxy_url = _normalize_proxy_url(session.proxy_override_url)
                return
            if session.proxy_state is not None and session.assigned_proxy_url:
                session.proxy_url = _normalize_proxy_url(session.assigned_proxy_url)
                return

        proxy_state = await self._get_proxy_pool().acquire()
        async with self._lock:
            if session.proxy_override_url:
                self._get_proxy_pool().report_success(proxy_state)
                session.proxy_url = _normalize_proxy_url(session.proxy_override_url)
                return
            session.proxy_state = proxy_state
            session.assigned_proxy_url = proxy_state.proxy_url
            session.proxy_url = proxy_state.proxy_url

    def apply_proxy_override(self, session: SessionContext, proxy_url: str | None) -> None:
        normalized = _normalize_proxy_url(proxy_url)
        session.proxy_override_url = normalized
        session.proxy_url = normalized

    async def report_success(self, session: SessionContext) -> None:
        proxy_state: ProxyState | None = None
        used_proxy = session.proxy_url

        async with self._lock:
            session.last_used_timestamp = _now()
            session.request_count += 1
            session.consecutive_failures = 0

            if session.proxy_override_url is None:
                proxy_state = session.proxy_state

            if session.proxy_override_url is not None:
                session.proxy_override_url = None
                session.proxy_url = _normalize_proxy_url(session.assigned_proxy_url)

            session.in_use = False
            self._cond.notify(1)

        if proxy_state is not None:
            self._get_proxy_pool().report_success(proxy_state)
        print(f"[SESSION_SUCCESS] session_id={session.session_id} proxy={used_proxy or '-'}")

    async def report_failure(self, session: SessionContext, reason: str) -> None:
        normalized_reason = str(reason or "unknown").strip() or "unknown"
        proxy_state: ProxyState | None = None
        used_proxy = session.proxy_url

        async with self._lock:
            session.last_used_timestamp = _now()
            session.request_count += 1
            session.consecutive_failures += 1

            if "429" in normalized_reason or "rate_limit" in normalized_reason:
                session.cooldown_until = max(float(session.cooldown_until or 0.0), _now() + self._cooldown_429)
            else:
                session.cooldown_until = max(float(session.cooldown_until or 0.0), _now() + self._cooldown_fail)

            if session.proxy_override_url is None:
                proxy_state = session.proxy_state
                if proxy_state is not None and ("429" in normalized_reason or "rate_limit" in normalized_reason):
                    session.proxy_state = None
                    session.assigned_proxy_url = None
                    session.proxy_url = None

            if session.proxy_override_url is not None:
                session.proxy_override_url = None
                session.proxy_url = _normalize_proxy_url(session.assigned_proxy_url)

            session.in_use = False
            self._cond.notify(1)

        if proxy_state is not None:
            self._get_proxy_pool().report_failure(proxy_state, normalized_reason)
        print(
            f"[SESSION_FAILURE] session_id={session.session_id} proxy={used_proxy or '-'} "
            f"reason={normalized_reason}"
        )


_SESSION_POOL: SessionPool | None = None


def get_session_pool() -> SessionPool:
    global _SESSION_POOL
    if _SESSION_POOL is None:
        pool_size = int(os.getenv("IG_PUBLIC_SESSION_POOL_SIZE") or 6)
        max_requests = int(os.getenv("IG_PUBLIC_SESSION_MAX_REQUESTS") or 30)
        _SESSION_POOL = SessionPool(pool_size=pool_size, max_requests_per_session=max_requests)
    return _SESSION_POOL
