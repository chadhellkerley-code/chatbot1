from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

from src.dm_campaign.contracts import WorkerExecutionStage, WorkerExecutionState


_PROCESSING_STAGES = {
    WorkerExecutionStage.WAITING_ACCOUNT,
    WorkerExecutionStage.OPENING_SESSION,
    WorkerExecutionStage.OPENING_DM,
    WorkerExecutionStage.SENDING,
}


def _state_from_stage(stage: WorkerExecutionStage) -> WorkerExecutionState:
    if stage == WorkerExecutionStage.IDLE:
        return WorkerExecutionState.IDLE
    if stage == WorkerExecutionStage.STOPPING:
        return WorkerExecutionState.STOPPING
    if stage in _PROCESSING_STAGES:
        return WorkerExecutionState.PROCESSING
    return WorkerExecutionState.WAITING


@dataclass(frozen=True)
class WorkerStateSnapshot:
    state: WorkerExecutionState
    stage: WorkerExecutionStage
    entered_at: float
    lead: str = ""
    account: str = ""
    reason: str = ""


class CampaignWorkerStateMachine:
    def __init__(self, *, max_busy_seconds: float = 120.0) -> None:
        self._lock = threading.RLock()
        self._max_busy_seconds = max(1.0, float(max_busy_seconds))
        now = time.time()
        self._snapshot = WorkerStateSnapshot(
            state=WorkerExecutionState.IDLE,
            stage=WorkerExecutionStage.IDLE,
            entered_at=now,
        )

    def snapshot(self) -> WorkerStateSnapshot:
        with self._lock:
            return self._snapshot

    def execution_state(self) -> WorkerExecutionState:
        return self.snapshot().state

    def execution_stage(self) -> WorkerExecutionStage:
        return self.snapshot().stage

    def busy_age(self, now: Optional[float] = None) -> float:
        snap = self.snapshot()
        if snap.stage not in _PROCESSING_STAGES:
            return 0.0
        ts = time.time() if now is None else float(now)
        return max(0.0, ts - snap.entered_at)

    def is_busy(self, now: Optional[float] = None) -> bool:
        age = self.busy_age(now=now)
        return 0.0 < age <= self._max_busy_seconds

    def set_idle(self, *, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.IDLE, reason=reason)

    def set_waiting_queue(self, *, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.WAITING_QUEUE, reason=reason)

    def set_blocked_proxy(self, *, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.BLOCKED_PROXY, reason=reason)

    def set_waiting_account(self, *, lead: str, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.WAITING_ACCOUNT, lead=lead, reason=reason)

    def set_cooldown(self, *, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.COOLDOWN, reason=reason)

    def set_opening_session(self, *, lead: str, account: str, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(
            WorkerExecutionStage.OPENING_SESSION,
            lead=lead,
            account=account,
            reason=reason,
        )

    def set_opening_dm(self, *, lead: str, account: str, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(
            WorkerExecutionStage.OPENING_DM,
            lead=lead,
            account=account,
            reason=reason,
        )

    def set_sending(self, *, lead: str, account: str, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(
            WorkerExecutionStage.SENDING,
            lead=lead,
            account=account,
            reason=reason,
        )

    def set_stopping(self, *, reason: str = "") -> WorkerStateSnapshot:
        return self._transition(WorkerExecutionStage.STOPPING, reason=reason)

    def _transition(
        self,
        stage: WorkerExecutionStage,
        *,
        lead: str = "",
        account: str = "",
        reason: str = "",
    ) -> WorkerStateSnapshot:
        clean_lead = str(lead or "").strip().lstrip("@")
        clean_account = str(account or "").strip().lstrip("@")
        clean_reason = str(reason or "").strip()
        snapshot = WorkerStateSnapshot(
            state=_state_from_stage(stage),
            stage=stage,
            entered_at=time.time(),
            lead=clean_lead,
            account=clean_account,
            reason=clean_reason,
        )
        with self._lock:
            self._snapshot = snapshot
            return snapshot
