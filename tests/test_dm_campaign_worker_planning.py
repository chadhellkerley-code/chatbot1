from __future__ import annotations

import threading

import pytest

from runtime.runtime import request_stop, reset_stop_event
from src.dm_campaign.health_monitor import HealthMonitor
from src.dm_campaign.proxy_workers_runner import (
    LOCAL_WORKER_PROXY_ID,
    LeadTask,
    ProxyWorker,
    TemplateRotator,
    calculate_workers,
    load_accounts,
    run_dynamic_campaign,
)


class _FakeScheduler:
    def update_worker_activity(self, *args, **kwargs) -> None:
        return None

    def build_retry_task(self, *args, **kwargs):
        return None

    def push_task(self, *args, **kwargs) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_stop_event_between_tests():
    reset_stop_event()
    yield
    reset_stop_event()


def test_calculate_workers_uses_explicit_proxy_groups_and_local_group(monkeypatch) -> None:
    accounts = [
        {"username": "proxied-a", "assigned_proxy_id": "proxy-a", "max_messages": 10, "sent_today": 0},
        {"username": "proxied-b", "assigned_proxy_id": "proxy-b", "max_messages": 5, "sent_today": 1},
        {"username": "local-a", "max_messages": 3, "sent_today": 0},
    ]

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._account_has_storage_state",
        lambda _account: False,
    )

    payload = calculate_workers(accounts)

    assert payload["workers_capacity"] == 3
    assert set(payload["ordered_worker_ids"]) == {"proxy-a", "proxy-b", LOCAL_WORKER_PROXY_ID}
    assert set(payload["proxies"]) == {"proxy-a", "proxy-b"}
    assert payload["has_none_accounts"] is True
    assert payload["group_capacities"]["proxy-a"] == 10
    assert payload["group_capacities"]["proxy-b"] == 4
    assert payload["group_capacities"][LOCAL_WORKER_PROXY_ID] == 3


def test_calculate_workers_skips_accounts_without_remaining_capacity(monkeypatch) -> None:
    accounts = [
        {"username": "spent-proxy", "assigned_proxy_id": "proxy-a", "max_messages": 2, "sent_today": 2},
        {"username": "local-ready", "max_messages": 5, "sent_today": 1},
    ]

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._account_has_storage_state",
        lambda _account: False,
    )

    payload = calculate_workers(accounts)

    assert payload["workers_capacity"] == 1
    assert payload["ordered_worker_ids"] == [LOCAL_WORKER_PROXY_ID]
    assert payload["group_capacities"][LOCAL_WORKER_PROXY_ID] == 4


def test_proxy_worker_rotation_does_not_starve_non_session_ready_accounts(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._account_has_storage_state",
        lambda account: str(account.get("username") or "") == "acct-ready",
    )

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[
            {"username": "acct-ready", "max_messages": 10, "sent_today": 0},
            {"username": "acct-pending", "max_messages": 10, "sent_today": 0},
        ],
        all_proxy_ids=[LOCAL_WORKER_PROXY_ID],
        scheduler=_FakeScheduler(),
        health_monitor=HealthMonitor(),
        stats={},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="alias",
        leads_alias="leads",
        campaign_run_id="run-1",
        headless=True,
        send_flow_timeout_seconds=15.0,
    )

    first = worker._next_ready_account(None)
    second = worker._next_ready_account(None)

    assert first is not None
    assert second is not None
    assert first.account["username"] == "acct-ready"
    assert second.account["username"] == "acct-pending"


def test_run_dynamic_campaign_caps_queue_by_capacity_uses_full_worker_plan_and_keeps_headless(monkeypatch) -> None:
    reset_stop_event()
    created_headless: list[bool] = []

    class _FakeSender:
        def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False) -> None:
            created_headless.append(bool(headless))

        def send_message_like_human_sync(
            self,
            account,
            target_username: str,
            text: str,
            *,
            stage_callback=None,
            **kwargs,
        ):
            if callable(stage_callback):
                stage_callback("opening_dm", {"account": account.get("username"), "lead": target_username})
                stage_callback("sending", {"account": account.get("username"), "lead": target_username})
            return True, "sent_verified", {"verified": True}

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias, *, run_id="": [
            {
                "username": "proxy-a-acct",
                "alias": alias,
                "active": True,
                "connected": True,
                "assigned_proxy_id": "proxy-a",
                "max_messages": 2,
                "messages_per_account": 2,
                "sent_today": 0,
            },
            {
                "username": "proxy-b-acct",
                "alias": alias,
                "active": True,
                "connected": True,
                "assigned_proxy_id": "proxy-b",
                "max_messages": 1,
                "messages_per_account": 1,
                "sent_today": 0,
            },
            {
                "username": "local-acct",
                "alias": alias,
                "active": True,
                "connected": True,
                "max_messages": 1,
                "messages_per_account": 1,
                "sent_today": 0,
            },
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.load_leads",
        lambda _leads_alias: [f"lead-{index}" for index in range(10)],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._filter_pending_leads_for_campaign",
        lambda leads, **_kwargs: (list(leads), {"skipped_already_sent": 0}),
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.preflight_accounts_for_proxy_runtime",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.get_account", lambda _username: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.log_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_failed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_skipped", lambda *_args, **_kwargs: None)

    result = run_dynamic_campaign(
        {
            "alias": "alias-a",
            "leads_alias": "lead-list",
            "workers_requested": 1,
            "headless": True,
            "templates": [{"text": "Hola"}],
            "total_leads": 10,
            "delay_min": 0,
            "delay_max": 0,
        }
    )

    assert result["sent"] == 2
    assert result["failed"] == 0
    assert result["remaining"] == 0
    assert result["workers_requested"] == 1
    assert result["workers_effective"] == 1
    assert result["workers_capacity"] == 3
    assert result["skipped_preblocked"] == 8
    assert created_headless == [True]


def test_local_worker_failures_do_not_block_the_local_lane(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)

    health_monitor = HealthMonitor(proxy_blocked_threshold=2, proxy_degraded_threshold=1)
    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "local-acct", "max_messages": 10, "sent_today": 0}],
        all_proxy_ids=[LOCAL_WORKER_PROXY_ID],
        scheduler=_FakeScheduler(),
        health_monitor=health_monitor,
        stats={},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="alias",
        leads_alias="leads",
        campaign_run_id="run-1",
        headless=True,
        send_flow_timeout_seconds=15.0,
    )

    for _ in range(5):
        worker._record_health_failure(
            "local-acct",
            "FLOW_TIMEOUT",
            is_login_error=False,
            response_time=1.0,
        )

    assert health_monitor.proxy_status(LOCAL_WORKER_PROXY_ID) == "healthy"
    assert health_monitor.accounts_snapshot()["local-acct"]["send_errors"] == 5


def test_ui_not_found_and_inbox_not_ready_remain_retryable_failures() -> None:
    assert ProxyWorker._is_non_retryable_lead_failure("UI_NOT_FOUND") is False
    assert ProxyWorker._is_non_retryable_lead_failure("SKIPPED_UI_NOT_FOUND") is False
    assert ProxyWorker._is_non_retryable_lead_failure("INBOX_NOT_READY") is False


def test_proxy_worker_emits_runtime_event_when_sent_log_write_fails(monkeypatch) -> None:
    events: list[dict[str, object]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.log_sent",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "local-acct", "max_messages": 10, "sent_today": 0}],
        all_proxy_ids=[LOCAL_WORKER_PROXY_ID],
        scheduler=_FakeScheduler(),
        health_monitor=HealthMonitor(),
        stats={"sent": 0},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="alias",
        leads_alias="leads",
        campaign_run_id="run-1",
        runtime_event_callback=lambda payload: events.append(dict(payload)),
        headless=True,
        send_flow_timeout_seconds=15.0,
    )

    worker._handle_success(
        LeadTask(lead="lead-1"),
        worker._states[0],
        detail="sent_verified",
        response_time=0.8,
    )

    assert any(
        event.get("event_type") == "sent_log_write_failed" and event.get("failure_kind") == "system"
        for event in events
    )


def test_proxy_worker_emits_runtime_event_when_proxy_degrades(monkeypatch) -> None:
    events: list[dict[str, object]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id="proxy-a",
        accounts=[{"username": "acct-a", "max_messages": 10, "sent_today": 0}],
        all_proxy_ids=["proxy-a"],
        scheduler=_FakeScheduler(),
        health_monitor=HealthMonitor(),
        stats={},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="alias",
        leads_alias="leads",
        campaign_run_id="run-1",
        runtime_event_callback=lambda payload: events.append(dict(payload)),
        headless=True,
        send_flow_timeout_seconds=15.0,
    )

    worker._log_proxy_status_change("degraded")

    assert events[0]["event_type"] == "proxy_degraded"
    assert events[0]["failure_kind"] == "retryable"


def test_load_accounts_excludes_accounts_blocked_by_proxy_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {"username": "ready", "alias": alias, "active": True, "connected": True},
            {"username": "blocked", "alias": alias, "active": True, "connected": True, "assigned_proxy_id": "proxy-a"},
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(accounts[0])],
            "blocked_accounts": [{"username": "blocked", "status": "quarantined", "message": "proxy quarantined"}],
        },
    )

    rows = load_accounts("alias-a", sent_today_counts={})

    assert [str(item.get("username") or "") for item in rows] == ["ready"]


def test_run_dynamic_campaign_stops_when_proxy_preflight_removes_every_account(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {"username": "blocked", "alias": alias, "active": True, "connected": True, "assigned_proxy_id": "proxy-a"},
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [],
            "blocked_accounts": [{"username": "blocked", "status": "quarantined", "message": "proxy quarantined"}],
        },
    )

    progress: list[dict[str, object]] = []
    result = run_dynamic_campaign(
        {
            "alias": "alias-a",
            "leads_alias": "lead-list",
            "workers_requested": 1,
            "headless": True,
            "templates": [{"text": "Hola"}],
            "total_leads": 10,
            "delay_min": 0,
            "delay_max": 0,
        },
        progress_callback=lambda payload: progress.append(dict(payload)),
    )

    assert result["sent"] == 0
    assert result["workers_capacity"] == 0
    assert progress[-1]["message"] == "No hay cuentas con proxy operativo para iniciar la campana."


def test_run_dynamic_campaign_keeps_pending_leads_when_user_stops(monkeypatch) -> None:
    failed_marks: list[tuple[str, str, int, str]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            self._calls = 0

        def send_message_like_human_sync(
            self,
            account,
            target_username: str,
            text: str,
            *args,
            stage_callback=None,
            **kwargs,
        ):
            self._calls += 1
            if callable(stage_callback):
                stage_callback("opening_dm", {"account": account.get("username"), "lead": target_username})
                stage_callback("sending", {"account": account.get("username"), "lead": target_username})
            if self._calls == 1:
                request_stop("test stop")
            return True, "sent_verified", {"verified": True}

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias, *, run_id="": [
            {
                "username": "local-acct",
                "alias": alias,
                "active": True,
                "connected": True,
                "max_messages": 3,
                "messages_per_account": 3,
                "sent_today": 0,
            }
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.load_leads",
        lambda _leads_alias: [f"lead-{index}" for index in range(3)],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._filter_pending_leads_for_campaign",
        lambda leads, **_kwargs: (list(leads), {"skipped_already_sent": 0}),
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.get_account", lambda _username: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.log_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_skipped", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.mark_lead_failed",
        lambda lead, *, reason="", attempts=0, alias="": failed_marks.append((lead, reason, attempts, alias)),
    )

    result = run_dynamic_campaign(
        {
            "alias": "alias-a",
            "leads_alias": "lead-list",
            "workers_requested": 1,
            "headless": True,
            "templates": [{"text": "Hola"}],
            "total_leads": 3,
            "delay_min": 0,
            "delay_max": 0,
        }
    )

    assert result["sent"] == 1
    assert result["failed"] == 0
    assert result["remaining"] == 2
    assert failed_marks == []
