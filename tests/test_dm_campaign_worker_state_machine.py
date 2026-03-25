from __future__ import annotations

from src.dm_campaign.contracts import WorkerExecutionStage, WorkerExecutionState
from src.dm_campaign.worker_state_machine import CampaignWorkerStateMachine


def test_worker_state_machine_maps_runtime_stages_to_execution_states() -> None:
    machine = CampaignWorkerStateMachine()

    waiting_queue = machine.set_waiting_queue(reason="queue_poll")
    assert waiting_queue.stage == WorkerExecutionStage.WAITING_QUEUE
    assert waiting_queue.state == WorkerExecutionState.WAITING

    waiting_account = machine.set_waiting_account(lead="lead-a", reason="select_account")
    assert waiting_account.stage == WorkerExecutionStage.WAITING_ACCOUNT
    assert waiting_account.state == WorkerExecutionState.PROCESSING
    assert waiting_account.lead == "lead-a"

    cooldown = machine.set_cooldown(reason="account_cooldown")
    assert cooldown.stage == WorkerExecutionStage.COOLDOWN
    assert cooldown.state == WorkerExecutionState.WAITING

    opening_dm = machine.set_opening_dm(
        lead="lead-b",
        account="account-1",
        reason="open_outbound_dm",
    )
    assert opening_dm.stage == WorkerExecutionStage.OPENING_DM
    assert opening_dm.state == WorkerExecutionState.PROCESSING
    assert opening_dm.account == "account-1"

    stopping = machine.set_stopping(reason="stop_requested")
    assert stopping.stage == WorkerExecutionStage.STOPPING
    assert stopping.state == WorkerExecutionState.STOPPING


def test_worker_state_machine_busy_age_only_counts_processing_stages() -> None:
    machine = CampaignWorkerStateMachine(max_busy_seconds=60.0)

    machine.set_cooldown(reason="account_cooldown")
    assert machine.busy_age(now=1_050.0) == 0.0

    machine.set_opening_session(lead="lead-a", account="account-1", reason="ensure_session")
    entered_at = machine.snapshot().entered_at
    assert machine.busy_age(now=entered_at + 12.0) == 12.0


def test_worker_state_machine_busy_window_expires_after_max_busy_seconds() -> None:
    machine = CampaignWorkerStateMachine(max_busy_seconds=30.0)
    machine.set_sending(lead="lead-a", account="account-1", reason="send_dm")
    entered_at = machine.snapshot().entered_at

    assert machine.is_busy(now=entered_at + 10.0) is True
    assert machine.is_busy(now=entered_at + 45.0) is False
