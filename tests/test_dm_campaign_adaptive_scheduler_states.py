from __future__ import annotations

from collections import deque
from threading import Lock

from src.dm_campaign.adaptive_scheduler import AdaptiveScheduler
from src.dm_campaign.contracts import WorkerExecutionStage, WorkerExecutionState
from src.dm_campaign.health_monitor import HealthMonitor


def _build_scheduler(*, idle_seconds: int = 30) -> AdaptiveScheduler:
    return AdaptiveScheduler(
        lead_queue=deque(),
        lead_queue_lock=Lock(),
        health_monitor=HealthMonitor(),
        idle_seconds=idle_seconds,
    )


def test_scheduler_preserves_stage_entered_at_on_heartbeat_only() -> None:
    scheduler = _build_scheduler()
    scheduler.register_worker("worker-1", "__no_proxy__")
    scheduler.update_worker_activity(
        "worker-1",
        proxy_id="__no_proxy__",
        execution_state=WorkerExecutionState.PROCESSING,
        execution_stage=WorkerExecutionStage.WAITING_ACCOUNT,
        lead="lead-a",
        account="account-1",
        reason="select_account",
    )
    first = scheduler.worker_snapshot("worker-1")
    assert first is not None

    scheduler.update_worker_activity(
        "worker-1",
        proxy_id="__no_proxy__",
        execution_state=WorkerExecutionState.PROCESSING,
        execution_stage=WorkerExecutionStage.WAITING_ACCOUNT,
        lead="lead-a",
        account="account-1",
        reason="select_account",
    )
    second = scheduler.worker_snapshot("worker-1")
    assert second is not None

    assert second.state_entered_at == first.state_entered_at
    assert second.current_lead == "lead-a"
    assert second.current_account == "account-1"


def test_scheduler_marks_processing_worker_as_stalled_only_after_activity_timeout() -> None:
    scheduler = _build_scheduler(idle_seconds=10)
    scheduler.register_worker("worker-1", "__no_proxy__")
    scheduler.update_worker_activity(
        "worker-1",
        proxy_id="__no_proxy__",
        execution_state=WorkerExecutionState.PROCESSING,
        execution_stage=WorkerExecutionStage.OPENING_DM,
        lead="lead-a",
        account="account-1",
        reason="open_outbound_dm",
    )

    state = scheduler._worker_state["worker-1"]
    state.state_entered_at = 100.0
    state.last_activity_at = 115.0

    assert scheduler.worker_is_stalled("worker-1", now=120.0) is False

    state.last_activity_at = 100.0
    assert scheduler.worker_is_stalled("worker-1", now=120.5) is True


def test_scheduler_reassign_worker_proxy_resets_runtime_stage_to_idle() -> None:
    scheduler = _build_scheduler()
    scheduler.register_worker("worker-1", "proxy-a")
    scheduler.update_worker_activity(
        "worker-1",
        proxy_id="proxy-a",
        execution_state=WorkerExecutionState.PROCESSING,
        execution_stage=WorkerExecutionStage.SENDING,
        lead="lead-a",
        account="account-1",
        reason="send_dm",
    )

    replacement = scheduler.reassign_worker_proxy(
        "worker-1",
        current_proxy="proxy-a",
        all_proxy_ids=["proxy-a", "proxy-b"],
    )
    snapshot = scheduler.worker_snapshot("worker-1")
    assert snapshot is not None

    assert replacement == "proxy-b"
    assert snapshot.proxy_id == "proxy-b"
    assert snapshot.execution_state == WorkerExecutionState.IDLE
    assert snapshot.execution_stage == WorkerExecutionStage.IDLE
    assert snapshot.state_reason == "proxy_reassigned"
    assert snapshot.current_lead == ""
