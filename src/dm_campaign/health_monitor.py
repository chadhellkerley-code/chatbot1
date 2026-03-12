from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional


@dataclass
class ProxyHealthState:
    status: str = "healthy"
    fail_count: int = 0
    success_count: int = 0
    consecutive_errors: int = 0
    last_error: str = ""
    login_errors: int = 0
    send_errors: int = 0
    blocked_until: float = 0.0
    response_times: Deque[float] = field(default_factory=lambda: deque(maxlen=200))


@dataclass
class AccountHealthState:
    fail_count: int = 0
    success_count: int = 0
    login_errors: int = 0
    send_errors: int = 0
    cooldown_until: float = 0.0
    last_error: str = ""
    response_times: Deque[float] = field(default_factory=lambda: deque(maxlen=200))


class HealthMonitor:
    def __init__(
        self,
        *,
        proxy_degraded_threshold: int = 5,
        proxy_blocked_threshold: int = 10,
        proxy_block_seconds: int = 600,
        account_cooldown_threshold: int = 3,
        account_cooldown_seconds: int = 600,
    ) -> None:
        self._lock = threading.RLock()
        self._proxy_state: Dict[str, ProxyHealthState] = {}
        self._account_state: Dict[str, AccountHealthState] = {}
        self._proxy_degraded_threshold = max(1, int(proxy_degraded_threshold))
        self._proxy_blocked_threshold = max(
            self._proxy_degraded_threshold + 1,
            int(proxy_blocked_threshold),
        )
        self._proxy_block_seconds = max(1, int(proxy_block_seconds))
        self._account_cooldown_threshold = max(1, int(account_cooldown_threshold))
        self._account_cooldown_seconds = max(1, int(account_cooldown_seconds))

    def record_send_success(self, proxy_id: str, account_id: str, response_time: float) -> None:
        now = time.time()
        proxy_key = _norm(proxy_id)
        account_key = _norm(account_id)
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            account = self._get_account(account_key)
            proxy.success_count += 1
            proxy.consecutive_errors = 0
            if response_time >= 0:
                proxy.response_times.append(float(response_time))
            if proxy.status != "blocked":
                proxy.status = "healthy"

            account.success_count += 1
            account.fail_count = 0
            if response_time >= 0:
                account.response_times.append(float(response_time))
            if account.cooldown_until and account.cooldown_until <= now:
                account.cooldown_until = 0.0

    def record_account_success(self, account_id: str, response_time: float) -> None:
        now = time.time()
        account_key = _norm(account_id)
        with self._lock:
            account = self._get_account(account_key)
            account.success_count += 1
            account.fail_count = 0
            if response_time >= 0:
                account.response_times.append(float(response_time))
            if account.cooldown_until and account.cooldown_until <= now:
                account.cooldown_until = 0.0

    def record_send_error(
        self,
        proxy_id: str,
        account_id: str,
        error: str,
        *,
        response_time: Optional[float] = None,
    ) -> None:
        self._record_error(
            proxy_id,
            account_id,
            error,
            is_login_error=False,
            response_time=response_time,
        )

    def record_login_error(
        self,
        proxy_id: str,
        account_id: str,
        error: str,
        *,
        response_time: Optional[float] = None,
    ) -> None:
        self._record_error(
            proxy_id,
            account_id,
            error,
            is_login_error=True,
            response_time=response_time,
        )

    def record_account_error(
        self,
        account_id: str,
        error: str,
        *,
        is_login_error: bool,
        response_time: Optional[float] = None,
    ) -> None:
        now = time.time()
        account_key = _norm(account_id)
        message = str(error or "unknown_error")
        with self._lock:
            account = self._get_account(account_key)
            account.fail_count += 1
            account.last_error = message
            if response_time is not None and response_time >= 0:
                account.response_times.append(float(response_time))
            if is_login_error:
                account.login_errors += 1
            else:
                account.send_errors += 1

            if account.fail_count >= self._account_cooldown_threshold:
                account.cooldown_until = now + self._account_cooldown_seconds
                account.fail_count = 0

    def is_account_available(self, account_id: str, now: Optional[float] = None) -> bool:
        account_key = _norm(account_id)
        ts = time.time() if now is None else float(now)
        with self._lock:
            account = self._get_account(account_key)
            return account.cooldown_until <= ts

    def account_cooldown_remaining(self, account_id: str, now: Optional[float] = None) -> float:
        account_key = _norm(account_id)
        ts = time.time() if now is None else float(now)
        with self._lock:
            account = self._get_account(account_key)
            return max(0.0, float(account.cooldown_until - ts))

    def set_account_cooldown(self, account_id: str, *, reason: str = "") -> float:
        account_key = _norm(account_id)
        with self._lock:
            account = self._get_account(account_key)
            account.cooldown_until = time.time() + self._account_cooldown_seconds
            if reason:
                account.last_error = reason
            return account.cooldown_until

    def is_proxy_available(self, proxy_id: str, now: Optional[float] = None) -> bool:
        proxy_key = _norm(proxy_id)
        ts = time.time() if now is None else float(now)
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            self._refresh_proxy_status(proxy, ts)
            return proxy.status != "blocked"

    def proxy_status(self, proxy_id: str, now: Optional[float] = None) -> str:
        proxy_key = _norm(proxy_id)
        ts = time.time() if now is None else float(now)
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            self._refresh_proxy_status(proxy, ts)
            return proxy.status

    def proxy_fail_rate(self, proxy_id: str) -> float:
        proxy_key = _norm(proxy_id)
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            total = proxy.success_count + proxy.fail_count
            if total <= 0:
                return 0.0
            return float(proxy.fail_count) / float(total)

    def account_fail_rate(self, account_id: str) -> float:
        account_key = _norm(account_id)
        with self._lock:
            account = self._get_account(account_key)
            total = account.success_count + account.fail_count
            if total <= 0:
                return 0.0
            return float(account.fail_count) / float(total)

    def proxy_response_time(self, proxy_id: str) -> float:
        proxy_key = _norm(proxy_id)
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            if not proxy.response_times:
                return 0.0
            return float(sum(proxy.response_times) / len(proxy.response_times))

    def account_response_time(self, account_id: str) -> float:
        account_key = _norm(account_id)
        with self._lock:
            account = self._get_account(account_key)
            if not account.response_times:
                return 0.0
            return float(sum(account.response_times) / len(account.response_times))

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.time()
        with self._lock:
            payload: Dict[str, Dict[str, object]] = {}
            for proxy_id, state in self._proxy_state.items():
                self._refresh_proxy_status(state, now)
                payload[proxy_id] = {
                    "status": state.status,
                    "fail_count": state.fail_count,
                    "success_count": state.success_count,
                    "consecutive_errors": state.consecutive_errors,
                    "proxy_fail_rate": self.proxy_fail_rate(proxy_id),
                    "response_time": self.proxy_response_time(proxy_id),
                    "login_errors": state.login_errors,
                    "send_errors": state.send_errors,
                    "last_error": state.last_error,
                    "blocked_until": state.blocked_until,
                }
            return payload

    def accounts_snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.time()
        with self._lock:
            payload: Dict[str, Dict[str, object]] = {}
            for account_id, state in self._account_state.items():
                payload[account_id] = {
                    "fail_count": state.fail_count,
                    "success_count": state.success_count,
                    "account_fail_rate": self.account_fail_rate(account_id),
                    "response_time": self.account_response_time(account_id),
                    "cooldown_until": state.cooldown_until,
                    "cooldown_active": state.cooldown_until > now,
                    "login_errors": state.login_errors,
                    "send_errors": state.send_errors,
                    "last_error": state.last_error,
                }
            return payload

    def _record_error(
        self,
        proxy_id: str,
        account_id: str,
        error: str,
        *,
        is_login_error: bool,
        response_time: Optional[float],
    ) -> None:
        now = time.time()
        proxy_key = _norm(proxy_id)
        account_key = _norm(account_id)
        message = str(error or "unknown_error")
        with self._lock:
            proxy = self._get_proxy(proxy_key)
            account = self._get_account(account_key)

            proxy.fail_count += 1
            proxy.consecutive_errors += 1
            proxy.last_error = message
            if response_time is not None and response_time >= 0:
                proxy.response_times.append(float(response_time))
            if is_login_error:
                proxy.login_errors += 1
            else:
                proxy.send_errors += 1
            self._refresh_proxy_status(proxy, now)

            account.fail_count += 1
            account.last_error = message
            if response_time is not None and response_time >= 0:
                account.response_times.append(float(response_time))
            if is_login_error:
                account.login_errors += 1
            else:
                account.send_errors += 1

            if account.fail_count >= self._account_cooldown_threshold:
                account.cooldown_until = now + self._account_cooldown_seconds
                account.fail_count = 0

    def _refresh_proxy_status(self, proxy: ProxyHealthState, now: float) -> None:
        if proxy.status == "blocked":
            if now < proxy.blocked_until:
                return
            proxy.status = "healthy"
            proxy.consecutive_errors = 0
            proxy.blocked_until = 0.0

        if proxy.consecutive_errors > self._proxy_blocked_threshold:
            proxy.status = "blocked"
            proxy.blocked_until = now + self._proxy_block_seconds
            return
        if proxy.consecutive_errors > self._proxy_degraded_threshold:
            proxy.status = "degraded"
            return
        proxy.status = "healthy"

    def _get_proxy(self, proxy_id: str) -> ProxyHealthState:
        key = proxy_id or "__no_proxy__"
        existing = self._proxy_state.get(key)
        if existing is None:
            existing = ProxyHealthState()
            self._proxy_state[key] = existing
        return existing

    def _get_account(self, account_id: str) -> AccountHealthState:
        key = account_id or "__unknown__"
        existing = self._account_state.get(key)
        if existing is None:
            existing = AccountHealthState()
            self._account_state[key] = existing
        return existing


def _norm(value: str) -> str:
    return str(value or "").strip().lower()
