from __future__ import annotations

import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return max(minimum, int(default))
    try:
        value = int(float(str(raw).strip()))
    except Exception:
        value = int(default)
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return max(minimum, float(default))
    try:
        value = float(str(raw).strip())
    except Exception:
        value = float(default)
    return max(minimum, value)


@dataclass(frozen=True)
class AutoresponderRuntimeLimits:
    requests_per_minute: int = 45
    hydrations_per_cycle: int = 6
    hydrations_per_thread_per_hour: int = 4
    thread_cooldown_seconds: int = 8 * 60
    pending_queue_limit: int = 250
    backoff_base_seconds: int = 45
    backoff_max_seconds: int = 20 * 60
    temporary_disable_seconds: int = 6 * 60
    jitter_min_seconds: float = 0.10
    jitter_max_seconds: float = 0.45

    @classmethod
    def from_env(cls) -> "AutoresponderRuntimeLimits":
        jitter_min = _env_float("AUTORESPONDER_HYDRATION_JITTER_MIN_S", 0.10, minimum=0.0)
        jitter_max = _env_float("AUTORESPONDER_HYDRATION_JITTER_MAX_S", 0.45, minimum=0.0)
        if jitter_max < jitter_min:
            jitter_max = jitter_min
        return cls(
            requests_per_minute=_env_int("AUTORESPONDER_REQUESTS_PER_MIN", 45, minimum=5),
            hydrations_per_cycle=_env_int("AUTORESPONDER_HYDRATIONS_PER_CYCLE", 6, minimum=0),
            hydrations_per_thread_per_hour=_env_int(
                "AUTORESPONDER_HYDRATIONS_PER_THREAD_PER_HOUR",
                4,
                minimum=1,
            ),
            thread_cooldown_seconds=_env_int(
                "AUTORESPONDER_HYDRATION_COOLDOWN_SECONDS",
                8 * 60,
                minimum=10,
            ),
            pending_queue_limit=_env_int(
                "AUTORESPONDER_HYDRATION_PENDING_LIMIT",
                250,
                minimum=10,
            ),
            backoff_base_seconds=_env_int(
                "AUTORESPONDER_HYDRATION_BACKOFF_BASE_SECONDS",
                45,
                minimum=10,
            ),
            backoff_max_seconds=_env_int(
                "AUTORESPONDER_HYDRATION_BACKOFF_MAX_SECONDS",
                20 * 60,
                minimum=60,
            ),
            temporary_disable_seconds=_env_int(
                "AUTORESPONDER_HYDRATION_TEMP_DISABLE_SECONDS",
                6 * 60,
                minimum=30,
            ),
            jitter_min_seconds=jitter_min,
            jitter_max_seconds=jitter_max,
        )


@dataclass
class PendingHydration:
    thread_id: str
    reason: str
    queued_at: float
    priority: int = 0


@dataclass
class _ThreadState:
    last_hydrated_at: float = 0.0
    last_activity_at: float = 0.0
    attempts_last_hour: Deque[float] = field(default_factory=deque)


@dataclass
class _AccountState:
    cycle_started_at: float = 0.0
    cycle_hydrations: int = 0
    request_timestamps: Deque[float] = field(default_factory=deque)
    pending_hydration: Deque[PendingHydration] = field(default_factory=deque)
    thread_state: Dict[str, _ThreadState] = field(default_factory=dict)
    risk_score: int = 0
    paused_until: float = 0.0
    paused_reason: str = ""
    backoff_until: float = 0.0
    hydration_disabled_until: float = 0.0
    metrics: Dict[str, int] = field(
        default_factory=lambda: {
            "hydration_attempts": 0,
            "hydration_success": 0,
            "hydration_complete": 0,
            "rate_signals": 0,
            "pending_enqueued": 0,
            "pending_dequeued": 0,
            "responses_success": 0,
            "responses_failed": 0,
            "followups_success": 0,
            "followups_failed": 0,
            "agendas_generated": 0,
        }
    )


class AutoresponderRuntimeController:
    def __init__(
        self,
        *,
        limits: Optional[AutoresponderRuntimeLimits] = None,
        logger_name: str = "autoresponder.runtime",
    ) -> None:
        self.limits = limits or AutoresponderRuntimeLimits.from_env()
        self._accounts: Dict[str, _AccountState] = {}
        self._logger = logging.getLogger(logger_name)

    @classmethod
    def from_env(cls) -> "AutoresponderRuntimeController":
        return cls(limits=AutoresponderRuntimeLimits.from_env())

    def _state_for(self, account: str) -> _AccountState:
        key = str(account or "").strip().lower()
        if key not in self._accounts:
            self._accounts[key] = _AccountState()
        return self._accounts[key]

    def _thread_for(self, account_state: _AccountState, thread_id: str) -> _ThreadState:
        key = str(thread_id or "").strip()
        if key not in account_state.thread_state:
            account_state.thread_state[key] = _ThreadState()
        return account_state.thread_state[key]

    @staticmethod
    def _trim_window(values: Deque[float], *, now_ts: float, window_seconds: float) -> None:
        while values and (now_ts - float(values[0])) > window_seconds:
            values.popleft()

    def begin_cycle(self, account: str, *, now_ts: Optional[float] = None) -> None:
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        if state.cycle_started_at <= 0.0 or (now_value - state.cycle_started_at) > 120.0:
            state.cycle_started_at = now_value
            state.cycle_hydrations = 0
        self._trim_window(state.request_timestamps, now_ts=now_value, window_seconds=60.0)

    def remaining_hydrations_for_cycle(self, account: str) -> int:
        state = self._state_for(account)
        return max(0, int(self.limits.hydrations_per_cycle) - int(state.cycle_hydrations))

    def is_account_blocked(self, account: str, *, now_ts: Optional[float] = None) -> Tuple[bool, float, str]:
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        if state.paused_until > now_value:
            pause_reason = str(state.paused_reason or "").strip() or "paused"
            return True, max(0.0, state.paused_until - now_value), f"pause:{pause_reason}"
        if state.paused_until > 0.0:
            state.paused_until = 0.0
            state.paused_reason = ""
        if state.backoff_until > now_value:
            return True, max(0.0, state.backoff_until - now_value), "backoff"
        if state.hydration_disabled_until > now_value:
            return True, max(0.0, state.hydration_disabled_until - now_value), "hydration_disabled"
        return False, 0.0, ""

    def should_hydrate(
        self,
        account: str,
        thread_id: str,
        *,
        last_activity_at: Optional[float],
        critical: bool = False,
        now_ts: Optional[float] = None,
    ) -> Tuple[bool, str]:
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        blocked, _remaining, blocked_reason = self.is_account_blocked(account, now_ts=now_value)
        if blocked:
            return False, f"account_{blocked_reason}"

        self._trim_window(state.request_timestamps, now_ts=now_value, window_seconds=60.0)
        if len(state.request_timestamps) >= int(self.limits.requests_per_minute):
            return False, "rpm_limit"

        if int(state.cycle_hydrations) >= int(self.limits.hydrations_per_cycle):
            return False, "cycle_limit"

        thread = self._thread_for(state, thread_id)
        self._trim_window(thread.attempts_last_hour, now_ts=now_value, window_seconds=3600.0)
        if len(thread.attempts_last_hour) >= int(self.limits.hydrations_per_thread_per_hour):
            return False, "thread_hour_limit"

        if not critical:
            if thread.last_hydrated_at > 0.0 and (now_value - thread.last_hydrated_at) < float(
                self.limits.thread_cooldown_seconds
            ):
                return False, "thread_cooldown"
            activity_value = float(last_activity_at or 0.0)
            if (
                activity_value > 0.0
                and thread.last_activity_at > 0.0
                and activity_value <= thread.last_activity_at
                and (now_value - thread.last_hydrated_at) < float(self.limits.thread_cooldown_seconds * 2)
            ):
                return False, "thread_activity_unchanged"

        return True, "ok"

    def record_hydration_attempt(
        self,
        account: str,
        thread_id: str,
        *,
        success: bool,
        complete: bool,
        last_activity_at: Optional[float] = None,
        now_ts: Optional[float] = None,
    ) -> None:
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        thread = self._thread_for(state, thread_id)

        state.metrics["hydration_attempts"] += 1
        state.cycle_hydrations = int(state.cycle_hydrations) + 1
        state.request_timestamps.append(now_value)
        self._trim_window(state.request_timestamps, now_ts=now_value, window_seconds=60.0)

        thread.last_hydrated_at = now_value
        thread.attempts_last_hour.append(now_value)
        self._trim_window(thread.attempts_last_hour, now_ts=now_value, window_seconds=3600.0)

        activity_value = float(last_activity_at or 0.0)
        if activity_value > 0.0:
            thread.last_activity_at = max(thread.last_activity_at, activity_value)

        if success:
            state.metrics["hydration_success"] += 1
            if complete:
                state.metrics["hydration_complete"] += 1
            if state.risk_score > 0:
                state.risk_score -= 1

    def mark_rate_signal(self, account: str, *, reason: str, now_ts: Optional[float] = None) -> None:
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        state.metrics["rate_signals"] += 1
        state.risk_score += 1
        backoff = float(self.limits.backoff_base_seconds) * float(2 ** max(0, state.risk_score - 1))
        backoff = min(backoff, float(self.limits.backoff_max_seconds))
        state.backoff_until = max(state.backoff_until, now_value + backoff)
        disable_for = min(float(self.limits.temporary_disable_seconds), backoff)
        state.hydration_disabled_until = max(state.hydration_disabled_until, now_value + disable_for)
        self._logger.warning(
            "Runtime rate-signal account=@%s reason=%s risk=%s backoff_s=%s disable_s=%s",
            account,
            reason,
            state.risk_score,
            int(max(0.0, state.backoff_until - now_value)),
            int(max(0.0, state.hydration_disabled_until - now_value)),
        )

    def pause_account(
        self,
        account: str,
        *,
        reason: str,
        duration_seconds: float,
        now_ts: Optional[float] = None,
    ) -> None:
        now_value = float(now_ts if now_ts is not None else time.time())
        pause_for = max(0.0, float(duration_seconds))
        if pause_for <= 0.0:
            return
        state = self._state_for(account)
        pause_reason = str(reason or "").strip() or "paused"
        state.paused_until = max(state.paused_until, now_value + pause_for)
        if pause_reason:
            state.paused_reason = pause_reason
        self._logger.warning(
            "Runtime pause account=@%s reason=%s pause_s=%s",
            account,
            pause_reason,
            int(max(0.0, state.paused_until - now_value)),
        )

    def record_reply_success(self, account: str) -> None:
        state = self._state_for(account)
        state.metrics["responses_success"] += 1

    def record_reply_failure(self, account: str) -> None:
        state = self._state_for(account)
        state.metrics["responses_failed"] += 1

    def record_followup_success(self, account: str) -> None:
        state = self._state_for(account)
        state.metrics["followups_success"] += 1

    def record_followup_failure(self, account: str) -> None:
        state = self._state_for(account)
        state.metrics["followups_failed"] += 1

    def record_agenda_generated(self, account: str) -> None:
        state = self._state_for(account)
        state.metrics["agendas_generated"] += 1

    def enqueue_pending(
        self,
        account: str,
        thread_id: str,
        *,
        reason: str,
        priority: int = 0,
        now_ts: Optional[float] = None,
    ) -> None:
        key = str(thread_id or "").strip()
        if not key:
            return
        now_value = float(now_ts if now_ts is not None else time.time())
        state = self._state_for(account)
        existing: List[PendingHydration] = list(state.pending_hydration)
        existing = [item for item in existing if item.thread_id != key]
        existing.append(
            PendingHydration(
                thread_id=key,
                reason=str(reason or "").strip() or "pending_hydration",
                queued_at=now_value,
                priority=int(priority),
            )
        )
        existing.sort(key=lambda item: (-int(item.priority), float(item.queued_at)))
        while len(existing) > int(self.limits.pending_queue_limit):
            existing.pop()
        state.pending_hydration = deque(existing)
        state.metrics["pending_enqueued"] += 1

    def dequeue_pending(self, account: str, *, limit: int) -> List[PendingHydration]:
        take = max(0, int(limit))
        if take <= 0:
            return []
        state = self._state_for(account)
        picked: List[PendingHydration] = []
        while state.pending_hydration and len(picked) < take:
            picked.append(state.pending_hydration.popleft())
        if picked:
            state.metrics["pending_dequeued"] += len(picked)
        return picked

    def next_jitter_seconds(self) -> float:
        low = float(self.limits.jitter_min_seconds)
        high = float(self.limits.jitter_max_seconds)
        if high <= low:
            return max(0.0, low)
        return max(0.0, random.uniform(low, high))

    def snapshot(self, account: str) -> Dict[str, float | str]:
        now_value = time.time()
        state = self._state_for(account)
        self._trim_window(state.request_timestamps, now_ts=now_value, window_seconds=60.0)
        blocked, remaining, reason = self.is_account_blocked(account, now_ts=now_value)
        return {
            "requests_last_minute": float(len(state.request_timestamps)),
            "cycle_hydrations": float(state.cycle_hydrations),
            "pending_hydration": float(len(state.pending_hydration)),
            "risk_score": float(state.risk_score),
            "account_blocked": 1.0 if blocked else 0.0,
            "account_blocked_remaining_seconds": float(max(0.0, remaining)),
            "account_blocked_reason": reason,
            "hydration_attempts": float(state.metrics.get("hydration_attempts", 0)),
            "hydration_success": float(state.metrics.get("hydration_success", 0)),
            "hydration_complete": float(state.metrics.get("hydration_complete", 0)),
            "rate_signals": float(state.metrics.get("rate_signals", 0)),
            "responses_success": float(state.metrics.get("responses_success", 0)),
            "responses_failed": float(state.metrics.get("responses_failed", 0)),
            "followups_success": float(state.metrics.get("followups_success", 0)),
            "followups_failed": float(state.metrics.get("followups_failed", 0)),
            "agendas_generated": float(state.metrics.get("agendas_generated", 0)),
        }
