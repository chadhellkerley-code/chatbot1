from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse


@dataclass
class ProxyState:
    proxy_url: str
    proxy_key: str
    last_request: float = 0.0
    cooldown_until: float = 0.0
    error_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    reputation_score: float = 1.0
    total_requests: int = 0
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    next_request_not_before: float = 0.0

    def is_available(self) -> bool:
        now = time.time()
        if self.cooldown_until > now:
            return False
        return True


def _now() -> float:
    return time.time()


def _normalize_proxy_url(url: str) -> str:
    return str(url or "").strip()


def _proxy_url_from_registry_record(record: Dict[str, Any]) -> str:
    server = str(record.get("server") or "").strip()
    if not server:
        server = str(record.get("proxy_url") or record.get("url") or record.get("proxy") or "").strip()
    if not server:
        return ""
    parsed = urlparse(server if "://" in server else f"http://{server}")
    user = str(record.get("user") or record.get("username") or "").strip()
    password = str(record.get("pass") or record.get("password") or "").strip()
    if user or password:
        netloc = parsed.hostname or ""
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        auth = quote(user, safe="") if user else ""
        if password:
            auth = f"{auth}:{quote(password, safe='')}"
        netloc = f"{auth}@{netloc}" if auth else netloc
        parsed = parsed._replace(netloc=netloc)
    return urlunparse(parsed)


class ProxyPool:
    def __init__(
        self,
        proxies: Iterable[str],
        *,
        min_interval_seconds: float = 1.5,
        max_interval_seconds: float = 3.5,
        error_cooldown_min_seconds: float = 60.0,
        error_cooldown_max_seconds: float = 120.0,
        consecutive_errors_for_cooldown: int = 3,
    ) -> None:
        normalized = [_normalize_proxy_url(p) for p in (proxies or [])]
        normalized = [p for p in normalized if p]
        if not normalized:
            raise ValueError("ProxyPool requires at least one proxy URL.")

        self._min_interval = float(min_interval_seconds)
        self._max_interval = float(max_interval_seconds)
        self._cooldown_min = float(error_cooldown_min_seconds)
        self._cooldown_max = float(error_cooldown_max_seconds)
        self._cooldown_errors = max(1, int(consecutive_errors_for_cooldown))

        self._lock = asyncio.Lock()
        self._states: List[ProxyState] = [
            ProxyState(proxy_url=url, proxy_key=f"proxy:{idx}") for idx, url in enumerate(normalized)
        ]

    @classmethod
    def from_proxy_registry(
        cls,
        *,
        min_interval_seconds: float = 1.5,
        max_interval_seconds: float = 3.5,
        error_cooldown_min_seconds: float = 60.0,
        error_cooldown_max_seconds: float = 120.0,
        consecutive_errors_for_cooldown: int = 3,
    ) -> "ProxyPool":
        from src.proxy_pool import list_active_proxies

        records = list_active_proxies()
        urls: List[str] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            url = _proxy_url_from_registry_record(record)
            if url:
                urls.append(url)
        return cls(
            urls,
            min_interval_seconds=min_interval_seconds,
            max_interval_seconds=max_interval_seconds,
            error_cooldown_min_seconds=error_cooldown_min_seconds,
            error_cooldown_max_seconds=error_cooldown_max_seconds,
            consecutive_errors_for_cooldown=consecutive_errors_for_cooldown,
        )

    def stats(self) -> List[Dict[str, Any]]:
        return [
            {
                "proxy_key": state.proxy_key,
                "last_request": state.last_request,
                "cooldown_until": state.cooldown_until,
                "error_count": state.error_count,
                "success_count": state.success_count,
                "failure_count": state.failure_count,
            }
            for state in self._states
        ]

    async def acquire(self) -> ProxyState:
        while True:
            delay = 0.0
            chosen: Optional[ProxyState] = None
            now = _now()
            async with self._lock:
                best_ready_at = float("inf")
                ready = [state for state in self._states if state.is_available()]
                ready.sort(
                    key=lambda p: p.reputation_score,
                    reverse=True
                )
                pool_states = ready if ready else self._states
                candidates: List[Tuple[float, ProxyState]] = []
                for state in pool_states:
                    ready_at = max(float(state.cooldown_until or 0.0), float(state.next_request_not_before or 0.0))
                    candidates.append((ready_at, state))
                    if ready_at < best_ready_at:
                        best_ready_at = ready_at

                ready_states = [s for (ready_at, s) in candidates if ready_at <= now]
                if ready_states:
                    ready_states.sort(
                        key=lambda p: p.reputation_score,
                        reverse=True,
                    )
                    top_count = max(3, int(len(ready_states) * 0.50))
                    top_count = min(top_count, len(ready_states))
                    top_group = ready_states[:top_count]
                    chosen = random.choice(top_group)
                else:
                    soonest = min(candidates, key=lambda pair: pair[0])[0]
                    delay = max(0.05, float(soonest) - now)

                if chosen is not None:
                    chosen.last_request = now
                    chosen.next_request_not_before = now + random.uniform(self._min_interval, self._max_interval)
                    print(
                        f"[LEADS][PROXY_SELECT] proxy={chosen.proxy_url} "
                        f"group={top_count}/{len(ready_states)} "
                        f"score={chosen.reputation_score:.2f}"
                    )
                    return chosen

            await asyncio.sleep(delay)

    def report_success(self, proxy_state: ProxyState) -> None:
        proxy_state.success_count += 1
        proxy_state.consecutive_failures = 0

        proxy_state.total_requests += 1
        success_ratio = proxy_state.success_count / max(proxy_state.total_requests, 1)
        proxy_state.reputation_score = success_ratio

        print(
            f"[LEADS][PROXY_OK] proxy={proxy_state.proxy_url}"
        )
        print(
            f"[LEADS][PROXY_SCORE] proxy={proxy_state.proxy_url} "
            f"score={proxy_state.reputation_score:.2f}"
        )

    def report_failure(self, proxy_state: ProxyState, error_type: str) -> None:
        proxy_state.failure_count += 1
        proxy_state.consecutive_failures += 1
        proxy_state.last_error = error_type

        proxy_state.total_requests += 1
        success_ratio = proxy_state.success_count / max(proxy_state.total_requests, 1)
        proxy_state.reputation_score = success_ratio

        print(
            f"[LEADS][PROXY_SCORE_FAIL] proxy={proxy_state.proxy_url} "
            f"score={proxy_state.reputation_score:.2f}"
        )

        if proxy_state.consecutive_failures >= 3:
            cooldown = random.randint(60, 180)
            proxy_state.cooldown_until = time.time() + cooldown

            print(
                f"[LEADS][PROXY_COOLDOWN] "
                f"proxy={proxy_state.proxy_url} "
                f"cooldown={cooldown}s"
            )
