from __future__ import annotations

from src.dm_campaign.contracts import (
    CampaignCapacity,
    CampaignLaunchRequest,
    CampaignRunSnapshot,
    CampaignRunStatus,
    CampaignSendResult,
    CampaignSendStatus,
)


def test_campaign_send_result_marks_verified_success_as_sent() -> None:
    result = CampaignSendResult.from_sender_result(
        (
            True,
            "sent_verified",
            {
                "reason_code": "SENT_OK",
                "verified": True,
            },
        )
    )

    assert result.ok is True
    assert result.status == CampaignSendStatus.SENT
    assert result.verified is True
    assert result.should_retry is False


def test_campaign_send_result_marks_skipped_reason_as_terminal() -> None:
    result = CampaignSendResult.from_sender_result(
        (
            False,
            "SKIPPED_USERNAME_NOT_FOUND",
            {
                "reason_code": "SKIPPED_USERNAME_NOT_FOUND",
            },
        )
    )

    assert result.ok is False
    assert result.status == CampaignSendStatus.SKIPPED
    assert result.reason_code == "SKIPPED_USERNAME_NOT_FOUND"
    assert result.should_retry is False


def test_campaign_send_result_marks_unverified_blocked_as_ambiguous() -> None:
    result = CampaignSendResult.from_sender_result(
        (
            False,
            "send_unverified_blocked",
            {
                "reason_code": "SENT_UNVERIFIED",
                "verified": False,
            },
        )
    )

    assert result.ok is False
    assert result.status == CampaignSendStatus.AMBIGUOUS
    assert result.should_retry is False


def test_campaign_send_result_marks_generic_failure_as_retryable() -> None:
    result = CampaignSendResult.from_sender_result(
        (
            False,
            "network_timeout",
            {
                "reason_code": "NETWORK_TIMEOUT",
            },
        )
    )

    assert result.ok is False
    assert result.status == CampaignSendStatus.FAILED
    assert result.should_retry is True


def test_campaign_send_result_promotes_explicit_sent_unverified_flag() -> None:
    result = CampaignSendResult.from_sender_result(
        (
            False,
            "",
            {
                "reason_code": "SENT_UNVERIFIED",
                "sent_unverified": True,
            },
        )
    )

    assert result.ok is True
    assert result.detail == "sent_unverified"
    assert result.status == CampaignSendStatus.AMBIGUOUS
    assert result.should_retry is False


def test_campaign_capacity_normalizes_to_canonical_payload() -> None:
    payload = CampaignCapacity.from_payload(
        {
            "alias": "demo",
            "workers_capacity": 3,
            "proxies": ["p1", "p2"],
            "has_none_accounts": True,
        }
    ).to_payload()

    assert payload == {
        "alias": "demo",
        "workers_capacity": 3,
        "leads_alias": "",
        "proxies": ["p1", "p2"],
        "has_none_accounts": True,
        "workers_requested": 0,
        "workers_effective": 0,
        "selected_leads_total": 0,
        "planned_eligible_leads": 0,
        "planned_runnable_leads": 0,
        "remaining_slots_total": 0,
        "account_remaining": [],
    }


def test_campaign_launch_request_builds_runner_payload_with_canonical_keys() -> None:
    request = CampaignLaunchRequest.from_payload(
        {
            "alias": "demo",
            "leads_alias": "lista",
            "run_id": "run-1",
            "templates": [{"id": "tpl-1", "text": "hola"}],
            "delay_min": 10,
            "delay_max": 20,
            "workers_requested": 2,
            "workers_capacity": 4,
            "headless": True,
            "total_leads": 30,
            "started_at": "2026-03-12T10:00:00",
        }
    )

    assert request.to_runner_payload() == {
        "alias": "demo",
        "leads_alias": "lista",
        "run_id": "run-1",
        "templates": [{"id": "tpl-1", "text": "hola"}],
        "delay_min": 10,
        "delay_max": 20,
        "workers_requested": 2,
        "workers_capacity": 4,
        "headless": True,
        "total_leads": 30,
    }


def test_campaign_run_snapshot_reads_canonical_worker_fields() -> None:
    snapshot = CampaignRunSnapshot.from_payload(
        {
            "run_id": "run-1",
            "alias": "demo",
            "leads_alias": "lista",
            "status": "Running",
            "workers_requested": 2,
            "workers_capacity": 4,
            "workers_effective": 2,
        }
    )

    assert snapshot.status == CampaignRunStatus.RUNNING
    assert snapshot.workers_requested == 2
    assert snapshot.workers_capacity == 4
    assert snapshot.workers_effective == 2


def test_campaign_run_status_interrupted_is_terminal() -> None:
    status = CampaignRunStatus.parse("Interrupted")

    assert status == CampaignRunStatus.INTERRUPTED
    assert status.is_terminal is True
