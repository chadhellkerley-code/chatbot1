from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, Optional

from src.dm_campaign.contracts import WorkerExecutionStage, WorkerExecutionState
from src.dm_campaign.health_monitor import HealthMonitor


@dataclass
class LeadTask:
    lead: str
    attempt: int = 1
    preferred_proxy_id: Optional[str] = None
    excluded_accounts: tuple[str, ...] = field(default_factory=tuple)
    history: tuple[str, ...] = field(default_factory=tuple)


@dataclass
class WorkerRuntimeState:
    proxy_id: str
    last_send_at: float
    last_activity_at: float
    execution_state: WorkerExecutionState = WorkerExecutionState.IDLE
    execution_stage: WorkerExecutionStage = WorkerExecutionStage.IDLE
    state_entered_at: float = 0.0
    current_lead: str = ""
    current_account: str = ""
    state_reason: str = ""
    restarts: int = 0


class AdaptiveScheduler:
    def __init__(
        self,
        *,
        lead_queue: Deque[LeadTask],
        lead_queue_lock: threading.Lock,
        health_monitor: HealthMonitor,
        idle_seconds: int = 30,
        max_attempts_per_lead: int = 3,
    ) -> None:
        self._lead_queue_lock = lead_queue_lock
        self._health = health_monitor
        self._idle_seconds = max(1, int(idle_seconds))
        self._max_attempts_per_lead = max(1, int(max_attempts_per_lead))

        self._generic_queue: Deque[LeadTask] = deque()
        self._queue_by_proxy: Dict[str, Deque[LeadTask]] = {}

        self._worker_lock = threading.RLock()
        self._worker_state: Dict[str, WorkerRuntimeState] = {}

        for task in list(lead_queue or deque()):
            self._push_task_unlocked(task)

    @staticmethod
    def build_initial_queue(leads: Iterable[str]) -> Deque[LeadTask]:
        queue: Deque[LeadTask] = deque()
        for lead in leads:
            clean = str(lead or "").strip().lstrip("@")
            if not clean:
                continue
            queue.append(LeadTask(lead=clean, attempt=1))
        return queue

    def register_proxy_queue(self, proxy_id: str) -> None:
        key = _norm_proxy(proxy_id)
        with self._lead_queue_lock:
            self._queue_by_proxy.setdefault(key, deque())

    def register_proxy_queues(self, proxy_ids: list[str]) -> None:
        with self._lead_queue_lock:
            for proxy_id in proxy_ids:
                key = _norm_proxy(proxy_id)
                self._queue_by_proxy.setdefault(key, deque())

    def distribute_unpinned_round_robin(self, proxy_ids: list[str]) -> None:
        ordered = [_norm_proxy(item) for item in proxy_ids if str(item or "").strip()]
        if not ordered:
            return
        with self._lead_queue_lock:
            for key in ordered:
                self._queue_by_proxy.setdefault(key, deque())
            index = 0
            while self._generic_queue:
                task = self._generic_queue.popleft()
                proxy_key = ordered[index % len(ordered)]
                self._queue_by_proxy[proxy_key].append(task)
                index += 1

    def pop_task_for_proxy(self, proxy_id: str) -> Optional[LeadTask]:
        proxy_key = _norm_proxy(proxy_id)
        with self._lead_queue_lock:
            queue = self._queue_by_proxy.get(proxy_key)
            if queue and queue:
                return queue.popleft()
            if self._generic_queue:
                return self._generic_queue.popleft()
            return None

    def push_task(self, task: LeadTask) -> None:
        with self._lead_queue_lock:
            self._push_task_unlocked(task)

    def _push_task_unlocked(self, task: LeadTask) -> None:
        if not isinstance(task, LeadTask):
            return
        if task.preferred_proxy_id:
            key = _norm_proxy(task.preferred_proxy_id)
            queue = self._queue_by_proxy.setdefault(key, deque())
            queue.append(task)
            return
        self._generic_queue.append(task)

    def queue_size(self) -> int:
        with self._lead_queue_lock:
            total = len(self._generic_queue)
            for queue in self._queue_by_proxy.values():
                total += len(queue)
            return total

    def is_empty(self) -> bool:
        return self.queue_size() <= 0

    def drain_all(self) -> list[LeadTask]:
        with self._lead_queue_lock:
            items: list[LeadTask] = []
            items.extend(list(self._generic_queue))
            self._generic_queue.clear()
            for queue in self._queue_by_proxy.values():
                items.extend(list(queue))
                queue.clear()
            return items

    def build_retry_task(
        self,
        task: LeadTask,
        *,
        failed_proxy_id: str,
        failed_account_id: str,
        same_proxy_account_ids: list[str],
        all_proxy_ids: list[str],
    ) -> Optional[LeadTask]:
        if task.attempt >= self._max_attempts_per_lead:
            return None

        failed_proxy_key = _norm_proxy(failed_proxy_id)
        failed_account_key = _norm_account(failed_account_id)
        next_attempt = task.attempt + 1

        if task.attempt == 1:
            alternatives = [
                _norm_account(account_id)
                for account_id in same_proxy_account_ids
                if _norm_account(account_id) != failed_account_key
            ]
            if alternatives:
                return LeadTask(
                    lead=task.lead,
                    attempt=next_attempt,
                    preferred_proxy_id=failed_proxy_key,
                    excluded_accounts=(failed_account_key,),
                    history=task.history + (f"{failed_proxy_key}:{failed_account_key}",),
                )

        if task.attempt <= 2:
            target_proxy = self._choose_other_proxy(
                all_proxy_ids=all_proxy_ids,
                exclude_proxy_id=failed_proxy_key,
            )
            if target_proxy:
                return LeadTask(
                    lead=task.lead,
                    attempt=next_attempt,
                    preferred_proxy_id=target_proxy,
                    excluded_accounts=tuple(),
                    history=task.history + (f"{failed_proxy_key}:{failed_account_key}",),
                )
        return None

    def register_worker(self, worker_id: str, proxy_id: str) -> None:
        now = time.time()
        with self._worker_lock:
            self._worker_state[worker_id] = WorkerRuntimeState(
                proxy_id=_norm_proxy(proxy_id),
                last_send_at=now,
                last_activity_at=now,
                execution_state=WorkerExecutionState.IDLE,
                execution_stage=WorkerExecutionStage.IDLE,
                state_entered_at=now,
                restarts=0,
            )

    def update_worker_activity(
        self,
        worker_id: str,
        *,
        sent: bool = False,
        proxy_id: Optional[str] = None,
        execution_state: Optional[WorkerExecutionState] = None,
        execution_stage: Optional[WorkerExecutionStage] = None,
        lead: Optional[str] = None,
        account: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        now = time.time()
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                state = WorkerRuntimeState(
                    proxy_id=_norm_proxy(proxy_id or ""),
                    last_send_at=now,
                    last_activity_at=now,
                    execution_state=execution_state or WorkerExecutionState.IDLE,
                    execution_stage=execution_stage or WorkerExecutionStage.IDLE,
                    state_entered_at=now,
                    current_lead=str(lead or "").strip().lstrip("@"),
                    current_account=str(account or "").strip().lstrip("@"),
                    state_reason=str(reason or "").strip(),
                    restarts=0,
                )
                self._worker_state[worker_id] = state
            if proxy_id:
                state.proxy_id = _norm_proxy(proxy_id)
            next_state = execution_state or state.execution_state
            next_stage = execution_stage or state.execution_stage
            next_lead = str(lead or "").strip().lstrip("@") if lead is not None else state.current_lead
            next_account = str(account or "").strip().lstrip("@") if account is not None else state.current_account
            next_reason = str(reason or "").strip() if reason is not None else state.state_reason
            if (
                next_state != state.execution_state
                or next_stage != state.execution_stage
                or next_lead != state.current_lead
                or next_account != state.current_account
                or next_reason != state.state_reason
            ):
                state.execution_state = next_state
                state.execution_stage = next_stage
                state.current_lead = next_lead
                state.current_account = next_account
                state.state_reason = next_reason
                state.state_entered_at = now
            state.last_activity_at = now
            if sent:
                state.last_send_at = now

    def worker_activity_age(self, worker_id: str, now: Optional[float] = None) -> float:
        ts = time.time() if now is None else float(now)
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                return 0.0
            return max(0.0, ts - state.last_activity_at)

    def worker_stage_age(self, worker_id: str, now: Optional[float] = None) -> float:
        ts = time.time() if now is None else float(now)
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                return 0.0
            return max(0.0, ts - state.state_entered_at)

    def worker_is_stalled(self, worker_id: str, now: Optional[float] = None) -> bool:
        ts = time.time() if now is None else float(now)
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                return False
            activity_age = max(0.0, ts - state.last_activity_at)
            stage_age = max(0.0, ts - state.state_entered_at)
            if state.execution_state == WorkerExecutionState.STOPPING:
                return False
            if state.execution_state == WorkerExecutionState.PROCESSING:
                return activity_age > self._idle_seconds and stage_age > self._idle_seconds
            return activity_age > self._idle_seconds

    def worker_proxy(self, worker_id: str) -> str:
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                return ""
            return state.proxy_id

    def worker_snapshot(self, worker_id: str) -> Optional[WorkerRuntimeState]:
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                return None
            return WorkerRuntimeState(
                proxy_id=state.proxy_id,
                last_send_at=state.last_send_at,
                last_activity_at=state.last_activity_at,
                execution_state=state.execution_state,
                execution_stage=state.execution_stage,
                state_entered_at=state.state_entered_at,
                current_lead=state.current_lead,
                current_account=state.current_account,
                state_reason=state.state_reason,
                restarts=state.restarts,
            )

    def reassign_worker_proxy(
        self,
        worker_id: str,
        *,
        current_proxy: str,
        all_proxy_ids: list[str],
    ) -> str:
        replacement = self._choose_other_proxy(
            all_proxy_ids=all_proxy_ids,
            exclude_proxy_id=current_proxy,
        )
        if not replacement:
            replacement = _norm_proxy(current_proxy)
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                now = time.time()
                self._worker_state[worker_id] = WorkerRuntimeState(
                    proxy_id=replacement,
                    last_send_at=now,
                    last_activity_at=now,
                    execution_state=WorkerExecutionState.IDLE,
                    execution_stage=WorkerExecutionStage.IDLE,
                    state_entered_at=now,
                    restarts=0,
                )
            else:
                now = time.time()
                state.proxy_id = replacement
                state.last_activity_at = now
                state.execution_state = WorkerExecutionState.IDLE
                state.execution_stage = WorkerExecutionStage.IDLE
                state.state_entered_at = now
                state.current_lead = ""
                state.current_account = ""
                state.state_reason = "proxy_reassigned"
        return replacement

    def record_worker_restart(self, worker_id: str) -> int:
        with self._worker_lock:
            state = self._worker_state.get(worker_id)
            if state is None:
                now = time.time()
                state = WorkerRuntimeState(
                    proxy_id="",
                    last_send_at=now,
                    last_activity_at=now,
                    execution_state=WorkerExecutionState.IDLE,
                    execution_stage=WorkerExecutionStage.IDLE,
                    state_entered_at=now,
                    restarts=0,
                )
                self._worker_state[worker_id] = state
            state.restarts += 1
            now = time.time()
            state.last_activity_at = now
            state.execution_state = WorkerExecutionState.IDLE
            state.execution_stage = WorkerExecutionStage.IDLE
            state.state_entered_at = now
            state.current_lead = ""
            state.current_account = ""
            state.state_reason = "worker_restarted"
            return state.restarts

    def _choose_other_proxy(self, *, all_proxy_ids: list[str], exclude_proxy_id: str) -> str:
        exclude_key = _norm_proxy(exclude_proxy_id)
        healthy: list[str] = []
        degraded: list[str] = []
        for proxy_id in all_proxy_ids:
            key = _norm_proxy(proxy_id)
            if not key or key == exclude_key:
                continue
            status = self._health.proxy_status(key)
            if status == "healthy" and self._health.is_proxy_available(key):
                healthy.append(key)
                continue
            if status == "degraded" and self._health.is_proxy_available(key):
                degraded.append(key)
        if healthy:
            return healthy[0]
        if degraded:
            return degraded[0]
        return ""


def _norm_proxy(value: str) -> str:
    clean = str(value or "").strip().lower()
    return clean or "__no_proxy__"


def _norm_account(value: str) -> str:
    return str(value or "").strip().lstrip("@").lower()
