from __future__ import annotations

<<<<<<< HEAD
import json
import threading
from datetime import datetime
from pathlib import Path
=======
import threading
>>>>>>> origin/main

import pytest

from core.proxy_preflight import DIRECT_NETWORK_KEY
<<<<<<< HEAD
from runtime.runtime import reset_stop_event
=======
from runtime.runtime import request_stop, reset_stop_event
>>>>>>> origin/main
from src.dm_campaign.contracts import CampaignSendResult, CampaignSendStatus
from src.dm_campaign.health_monitor import HealthMonitor
from src.dm_campaign.proxy_workers_runner import (
    LOCAL_WORKER_PROXY_ID,
    LeadTask,
    ProxyWorker,
    TemplateRotator,
<<<<<<< HEAD
    calculate_workers_for_alias,
=======
>>>>>>> origin/main
    calculate_workers,
    load_accounts,
    run_dynamic_campaign,
)
<<<<<<< HEAD
from src.runtime.playwright_runtime import PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
=======
from src.runtime.playwright_runtime import PLAYWRIGHT_BROWSER_MODE_MANAGED
>>>>>>> origin/main


class _FakeScheduler:
    def update_worker_activity(self, *args, **kwargs) -> None:
        return None

    def build_retry_task(self, *args, **kwargs):
        return None

    def push_task(self, *args, **kwargs) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_stop_event_between_tests(monkeypatch):
    reset_stop_event()
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.connected_status",
        lambda account, **_kwargs: bool(account.get("connected")),
    )
    yield
    reset_stop_event()


def _fake_account_proxy_preflight(account, **_kwargs):
    assigned = str((account or {}).get("assigned_proxy_id") or "").strip()
    if assigned:
        return {
            "status": "ok",
            "network_mode": "proxy",
            "effective_network_key": f"proxy:{assigned}",
            "proxy_id": assigned,
            "proxy_label": assigned,
            "message": "",
            "blocking": False,
        }
    if str((account or {}).get("proxy_url") or "").strip():
        return {
            "status": "legacy",
            "network_mode": "legacy",
            "effective_network_key": "",
            "proxy_id": "",
            "proxy_label": "legacy",
            "message": "legacy",
            "blocking": True,
        }
    return {
        "status": "none",
        "network_mode": "direct",
        "effective_network_key": DIRECT_NETWORK_KEY,
        "proxy_id": "",
        "proxy_label": DIRECT_NETWORK_KEY,
        "message": "",
        "blocking": False,
    }


<<<<<<< HEAD
def _configure_campaign_root(monkeypatch, tmp_path: Path) -> Path:
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("INSTACRM_INSTALL_ROOT", str(tmp_path))
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTACRM_DATA_ROOT", str(data_root))
    return data_root


=======
>>>>>>> origin/main
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
    assert set(payload["ordered_worker_ids"]) == {"proxy:proxy-a", "proxy:proxy-b", DIRECT_NETWORK_KEY}
    assert set(payload["proxies"]) == {"proxy-a", "proxy-b"}
    assert payload["has_none_accounts"] is True
    assert payload["group_capacities"]["proxy:proxy-a"] == 10
    assert payload["group_capacities"]["proxy:proxy-b"] == 4
    assert payload["group_capacities"][DIRECT_NETWORK_KEY] == 3


<<<<<<< HEAD
def test_calculate_workers_for_alias_uses_live_root_for_same_day_blocking(monkeypatch, tmp_path: Path) -> None:
    data_root = _configure_campaign_root(monkeypatch, tmp_path)
    leads_root = data_root / "leads"
    leads_root.mkdir(parents=True, exist_ok=True)
    (leads_root / "lista.txt").write_text("lead-sent\nlead-fresh\n", encoding="utf-8")

    same_day_ts = int(datetime(2026, 3, 30, 16, 56, 55).timestamp())
    (data_root / "sent_log.jsonl").write_text(
        json.dumps(
            {
                "ts": same_day_ts,
                "account": "acct-1",
                "to": "lead-sent",
                "ok": True,
                "detail": "sent_verified",
                "source_engine": "campaign",
                "campaign_alias": "nuevas",
                "leads_alias": "lista",
                "run_id": "run-old",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (data_root / "lead_status.json").write_text(
        json.dumps(
            {
                "version": 3,
                "aliases": {
                    "nuevas": {
                        "leads": {
                            "lead-sent": {
                                "status": "sent",
                                "sent_timestamp": same_day_ts,
                                "sent_by": "acct-1",
                            }
                        }
                    }
                },
                "legacy_global_leads": {},
                "global_contacted_leads": {
                    "lead-sent": {
                        "last_contacted_at": same_day_ts,
                        "last_status": "sent",
                        "last_alias": "nuevas",
                        "last_account": "acct-1",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    storage_root = tmp_path / "storage"
    storage_root.mkdir(parents=True, exist_ok=True)
    (storage_root / "lead_status.json").write_text(
        json.dumps(
            {
                "version": 3,
                "aliases": {
                    "nuevas": {
                        "leads": {
                            "lead-sent": {
                                "status": "pending",
                                "updated_at": same_day_ts + 5,
                                "pending_run_id": "run-bad",
                            }
                        }
                    }
                },
                "legacy_global_leads": {},
                "global_contacted_leads": {},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias, *, run_id="": [
            {
                "username": "acct-1",
                "alias": alias,
                "active": True,
                "connected": True,
                "max_messages": 10,
                "messages_per_account": 10,
                "sent_today": 0,
            }
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )

    plan = calculate_workers_for_alias(
        "nuevas",
        leads_alias="lista",
        workers_requested=1,
        run_id="run-new",
        root_dir=tmp_path,
    )

    assert plan["selected_leads_total"] == 2
    assert plan["planned_eligible_leads"] == 1
    assert plan["planned_queue"] == ["lead-fresh"]


=======
>>>>>>> origin/main
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
    assert payload["ordered_worker_ids"] == [DIRECT_NETWORK_KEY]
    assert payload["group_capacities"][DIRECT_NETWORK_KEY] == 4


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


<<<<<<< HEAD
def test_proxy_worker_limits_active_accounts_and_rotates_next_batch(monkeypatch) -> None:
    closed_accounts: list[str] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_account_session_sync(self, username: str, *, timeout: float = 5.0) -> None:
            del timeout
            closed_accounts.append(str(username))

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[
            {"username": f"acct-{index}", "max_messages": 1, "sent_today": 0}
            for index in range(8)
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
        active_account_limit=6,
    )

    active_before = [state.account["username"] for state in worker._states if state.active_in_worker]
    assert active_before == [f"acct-{index}" for index in range(6)]

    worker._retire_account(worker._states[0], reason="account_quota_reached")

    active_after = [state.account["username"] for state in worker._states if state.active_in_worker]
    assert active_after == [f"acct-{index}" for index in range(1, 7)]
    assert closed_accounts == ["acct-0"]


def test_proxy_worker_account_unavailable_discards_account_for_run_and_requeues_lead(monkeypatch) -> None:
    pushed: list[str] = []
    closed_accounts: list[str] = []

    class _Scheduler(_FakeScheduler):
        def push_task(self, task, *args, **kwargs) -> None:
            del args, kwargs
            pushed.append(task.lead)

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_account_session_sync(self, username: str, *, timeout: float = 5.0) -> None:
            del timeout
            closed_accounts.append(str(username))

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[
            {"username": "acct-bad", "max_messages": 10, "sent_today": 0},
            {"username": "acct-good", "max_messages": 10, "sent_today": 0},
        ],
        all_proxy_ids=[LOCAL_WORKER_PROXY_ID],
        scheduler=_Scheduler(),
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
        active_account_limit=1,
    )

    task = LeadTask(lead="lead-1")
    worker._handle_account_unavailable(
        task=task,
        account_state=worker._states[0],
        reason="challenge_required",
    )

    assert worker._states[0].disabled_for_campaign is True
    assert worker._states[0].active_in_worker is False
    assert worker._states[1].active_in_worker is True
    assert pushed == ["lead-1"]
    assert closed_accounts == ["acct-bad"]


def test_run_dynamic_campaign_uses_planned_queue_without_reloading_raw_leads(monkeypatch, tmp_path: Path) -> None:
    _configure_campaign_root(monkeypatch, tmp_path)
    captured_pending: list[str] = []

    class _FakeSender:
        def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False) -> None:
            del headless, keep_browser_open_per_account

        def send_message_like_human_sync(self, account, target_username: str, text: str, *_, stage_callback=None, **__):
            del account, text
            if callable(stage_callback):
                stage_callback("sending", {"lead": target_username})
            return True, "sent_verified", {"verified": True}

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            del timeout
            return None

    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.load_leads",
        lambda _leads_alias: (_ for _ in ()).throw(AssertionError("raw leads should not reload when planned_queue is provided")),
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias, *, run_id="": [
            {
                "username": "acct-1",
                "alias": alias,
                "active": True,
                "connected": True,
                "max_messages": 5,
                "messages_per_account": 5,
                "sent_today": 0,
            }
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.campaign_start_snapshot",
        lambda accounts, *, campaign_alias="": {
            "daily_counts": {account: 0 for account in accounts},
            "campaign_registry": set(),
            "shared_registry": set(),
        },
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._refresh_accounts_sent_today_from_log", lambda accounts: accounts)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.mark_leads_pending",
        lambda leads, *, alias="", run_id="": (
            lambda normalized: captured_pending.extend(normalized) or len(normalized)
        )(list(leads)),
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.get_account", lambda _username: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.log_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_failed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_skipped", lambda *_args, **_kwargs: None)

    result = run_dynamic_campaign(
        {
            "root_dir": str(tmp_path),
            "alias": "nuevas",
            "leads_alias": "lista",
            "workers_requested": 1,
            "headless": True,
            "templates": [{"text": "Hola"}],
            "total_leads": 1,
            "selected_leads_total": 2,
            "planned_eligible_leads": 1,
            "planned_queue": ["lead-fresh"],
            "delay_min": 0,
            "delay_max": 0,
        }
    )

    assert captured_pending == ["lead-fresh"]
    assert result["selected_leads_total"] == 2
    assert result["planned_eligible_leads"] == 1


def test_run_dynamic_campaign_progress_preserves_planning_provenance(monkeypatch, tmp_path: Path) -> None:
    _configure_campaign_root(monkeypatch, tmp_path)
    progress_payloads: list[dict[str, object]] = []
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._load_selected_accounts", lambda alias, *, run_id="": [])

    run_dynamic_campaign(
        {
            "root_dir": str(tmp_path),
            "alias": "nuevas",
            "leads_alias": "lista",
            "workers_requested": 1,
            "headless": True,
            "templates": [{"text": "Hola"}],
            "total_leads": 5,
            "selected_leads_total": 97,
            "planned_eligible_leads": 52,
            "planned_queue": ["lead-fresh"],
        },
        progress_callback=lambda payload: progress_payloads.append(dict(payload)),
    )

    assert progress_payloads
    assert progress_payloads[-1]["selected_leads_total"] == 97
    assert progress_payloads[-1]["planned_eligible_leads"] == 52


=======
>>>>>>> origin/main
def test_proxy_worker_injects_visible_browser_layout_for_headful_campaigns(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    worker = ProxyWorker(
        worker_id="worker-2",
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
        campaign_run_id="run-55",
        headless=False,
        send_flow_timeout_seconds=15.0,
        visible_browser_layout={
            "scope": "campaign:run-55",
            "target_count": 4,
            "layout_policy": "compact",
            "stagger_min_ms": 300,
            "stagger_max_ms": 800,
            "stagger_step_ms": 100,
        },
    )

    assert worker._states[0].account["visible_browser_layout"] == {
        "scope": "campaign:run-55",
        "target_count": 4,
        "layout_policy": "compact",
<<<<<<< HEAD
        "width": 1366,
        "height": 900,
=======
>>>>>>> origin/main
        "stagger_min_ms": 300,
        "stagger_max_ms": 800,
        "stagger_step_ms": 100,
        "worker_id": "worker-2",
        "proxy_id": "proxy-a",
        "network_key": "proxy:proxy-a",
    }
<<<<<<< HEAD
    assert worker._states[0].account["campaign_desktop_layout"] == {
        "width": 1366,
        "height": 900,
    }
    assert worker._states[0].account["manual_visible_browser"] is True
    assert worker._states[0].account["playwright_browser_mode"] == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY


def test_proxy_worker_keeps_campaign_sender_headless_and_sets_desktop_layout(monkeypatch) -> None:
    created_headless: list[bool] = []

    class _FakeSender:
        def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False, **_kwargs) -> None:
            created_headless.append(bool(headless))

=======
    assert worker._states[0].account["manual_visible_browser"] is True
    assert worker._states[0].account["playwright_browser_mode"] == PLAYWRIGHT_BROWSER_MODE_MANAGED


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

>>>>>>> origin/main
        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
<<<<<<< HEAD
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "acct-a", "max_messages": 3, "sent_today": 0}],
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

    assert created_headless == [True]
    assert worker._states[0].account["campaign_desktop_layout"] == {"width": 1366, "height": 900}
    assert "visible_browser_layout" not in worker._states[0].account
    assert worker._states[0].account.get("manual_visible_browser") is None


def test_proxy_worker_applies_same_desktop_visible_layout_to_all_headful_accounts(monkeypatch) -> None:
    created_headless: list[bool] = []

    class _FakeSender:
        def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False, **_kwargs) -> None:
            created_headless.append(bool(headless))

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    worker = ProxyWorker(
        worker_id="worker-2",
        proxy_id="proxy-a",
        accounts=[
            {"username": "acct-a", "max_messages": 1, "sent_today": 0, "assigned_proxy_id": "proxy-a"},
            {"username": "acct-b", "max_messages": 1, "sent_today": 0, "assigned_proxy_id": "proxy-a"},
        ],
        all_proxy_ids=["proxy:proxy-a"],
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
        campaign_run_id="run-visible",
        headless=False,
        send_flow_timeout_seconds=15.0,
        visible_browser_layout={
            "scope": "campaign:run-visible",
            "target_count": 2,
            "layout_policy": "compact",
            "stagger_min_ms": 300,
            "stagger_max_ms": 800,
            "stagger_step_ms": 100,
        },
    )

    assert created_headless == [False]
    assert len(worker._states) == 2
    received_layouts = [dict(state.account.get("visible_browser_layout") or {}) for state in worker._states]
    assert {layout["scope"] for layout in received_layouts} == {"campaign:run-visible"}
    assert {layout["target_count"] for layout in received_layouts} == {2}
    assert {layout["layout_policy"] for layout in received_layouts} == {"compact"}
    assert {layout["width"] for layout in received_layouts} == {1366}
    assert {layout["height"] for layout in received_layouts} == {900}
    assert {layout["worker_id"] for layout in received_layouts} == {"worker-2"}
    assert {layout["network_key"] for layout in received_layouts} == {"proxy:proxy-a"}
    assert all(state.account["manual_visible_browser"] is True for state in worker._states)
    assert all(
        state.account["playwright_browser_mode"] == PLAYWRIGHT_BROWSER_MODE_CHROME_ONLY
        for state in worker._states
    )
=======
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
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
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


def test_run_dynamic_campaign_passes_visible_browser_layout_when_headful(monkeypatch) -> None:
    reset_stop_event()
    received_layouts: list[dict[str, object]] = []
    received_visible_flags: list[tuple[bool, str]] = []

    class _FakeSender:
        def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False) -> None:
            assert headless is False

        def send_message_like_human_sync(
            self,
            account,
            target_username: str,
            text: str,
            *,
            stage_callback=None,
            **kwargs,
        ):
            del target_username, text, kwargs
            received_layouts.append(dict(account.get("visible_browser_layout") or {}))
            received_visible_flags.append(
                (
                    bool(account.get("manual_visible_browser")),
                    str(account.get("playwright_browser_mode") or ""),
                )
            )
            if callable(stage_callback):
                stage_callback("opening_dm", {"account": account.get("username"), "lead": "lead"})
                stage_callback("sending", {"account": account.get("username"), "lead": "lead"})
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
                "max_messages": 1,
                "messages_per_account": 1,
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
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.load_leads",
        lambda _leads_alias: ["lead-1", "lead-2"],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._filter_pending_leads_for_campaign",
        lambda leads, **_kwargs: (list(leads), {"skipped_already_sent": 0}),
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {"ready_accounts": [dict(item) for item in accounts], "blocked_accounts": []},
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.get_account", lambda _username: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.log_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_failed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_skipped", lambda *_args, **_kwargs: None)

    result = run_dynamic_campaign(
        {
            "alias": "alias-visible",
            "leads_alias": "lead-list",
            "workers_requested": 2,
            "headless": False,
            "templates": [{"text": "Hola"}],
            "total_leads": 2,
            "delay_min": 0,
            "delay_max": 0,
        }
    )

    assert result["sent"] == 2
    assert result["workers_effective"] == 2
    assert len(received_layouts) == 2
    assert len({layout["scope"] for layout in received_layouts}) == 1
    assert next(iter({layout["scope"] for layout in received_layouts})).startswith("campaign:campaign-")
    assert {layout["target_count"] for layout in received_layouts} == {2}
    assert {layout["layout_policy"] for layout in received_layouts} == {"compact"}
    assert {layout["stagger_min_ms"] for layout in received_layouts} == {300}
    assert {layout["stagger_max_ms"] for layout in received_layouts} == {800}
    assert received_visible_flags == [
        (True, PLAYWRIGHT_BROWSER_MODE_MANAGED),
        (True, PLAYWRIGHT_BROWSER_MODE_MANAGED),
    ]
>>>>>>> origin/main


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
        result=CampaignSendResult(
            ok=True,
            detail="sent_verified",
            payload={"verified": True},
            status=CampaignSendStatus.SENT,
            verified=True,
        ),
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

<<<<<<< HEAD
    proxy_events = [event for event in events if event.get("event_type") == "proxy_degraded"]
    assert proxy_events[0]["failure_kind"] == "retryable"
=======
    assert events[0]["event_type"] == "proxy_degraded"
    assert events[0]["failure_kind"] == "retryable"
>>>>>>> origin/main


def test_proxy_worker_success_syncs_connected_without_invalidating_health(monkeypatch) -> None:
    mark_calls: list[tuple[str, bool, bool]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.mark_connected",
        lambda username, connected, *, invalidate_health=False: mark_calls.append(
            (username, connected, invalidate_health)
        ),
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.log_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_lead_sent", lambda *_args, **_kwargs: None)

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "acct-a", "max_messages": 10, "sent_today": 0}],
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

    worker._handle_success(
        LeadTask(lead="lead-1"),
        worker._states[0],
        detail="sent_verified",
        response_time=0.8,
        result=CampaignSendResult(
            ok=True,
            detail="sent_verified",
            payload={"verified": True},
            status=CampaignSendStatus.SENT,
            verified=True,
        ),
    )

    assert mark_calls == [("acct-a", True, False)]


<<<<<<< HEAD
def test_proxy_worker_marks_global_contact_for_sent_unverified(monkeypatch) -> None:
=======
def test_proxy_worker_does_not_mark_global_contact_for_sent_unverified(monkeypatch) -> None:
>>>>>>> origin/main
    mark_sent_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    log_calls: list[dict[str, object]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.mark_connected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.mark_lead_sent",
        lambda *args, **kwargs: mark_sent_calls.append((args, dict(kwargs))),
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.log_sent",
        lambda *args, **kwargs: log_calls.append({"args": args, "kwargs": dict(kwargs)}),
    )

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "acct-a", "max_messages": 10, "sent_today": 0}],
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

    worker._handle_success(
        LeadTask(lead="lead-1"),
        worker._states[0],
        detail="sent_unverified",
        response_time=0.8,
        result=CampaignSendResult(
            ok=True,
            detail="sent_unverified",
            payload={"sent_unverified": True},
            status=CampaignSendStatus.AMBIGUOUS,
            verified=False,
        ),
    )

<<<<<<< HEAD
    assert mark_sent_calls == [
        (("lead-1",), {"sent_by": "acct-a", "alias": "alias"})
    ]
=======
    assert mark_sent_calls == []
>>>>>>> origin/main
    assert log_calls[0]["kwargs"]["sent_unverified"] is True


def test_load_accounts_excludes_accounts_blocked_by_proxy_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {"username": "ready", "alias": alias, "active": True, "connected": True},
            {"username": "blocked", "alias": alias, "active": True, "connected": True, "assigned_proxy_id": "proxy-a"},
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
        lambda accounts: {
            "ready_accounts": [dict(accounts[0])],
            "blocked_accounts": [{"username": "blocked", "status": "quarantined", "message": "proxy quarantined"}],
        },
    )

    rows = load_accounts("alias-a", sent_today_counts={})

    assert [str(item.get("username") or "") for item in rows] == ["ready"]


<<<<<<< HEAD
def test_load_accounts_excludes_usage_deactivated_accounts(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {"username": "ready", "alias": alias, "active": True, "usage_state": "active", "connected": True},
            {"username": "paused", "alias": alias, "active": True, "usage_state": "deactivated", "connected": True},
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._account_has_storage_state",
        lambda _account: True,
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.connected_status",
        lambda record, **_kwargs: bool(record.get("connected")),
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.health_store.get_record",
        lambda _username: (None, False),
    )

    rows = load_accounts("alias-a", sent_today_counts={})

    assert [str(item.get("username") or "") for item in rows] == ["ready"]


=======
>>>>>>> origin/main
def test_run_dynamic_campaign_stops_when_proxy_preflight_removes_every_account(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {"username": "blocked", "alias": alias, "active": True, "connected": True, "assigned_proxy_id": "proxy-a"},
        ],
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._campaign_accounts_preflight",
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
    assert result["preflight_blocked"][0]["username"] == "blocked"
    assert progress[-1]["message"] == "No hay cuentas operables para iniciar la campaña."


<<<<<<< HEAD
def test_proxy_worker_headless_accounts_do_not_receive_visible_browser_payload(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None
=======
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
>>>>>>> origin/main

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
<<<<<<< HEAD
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    worker = ProxyWorker(
        worker_id="worker-3",
        proxy_id=LOCAL_WORKER_PROXY_ID,
        accounts=[{"username": "local-acct", "max_messages": 3, "sent_today": 0}],
        all_proxy_ids=[LOCAL_WORKER_PROXY_ID],
        scheduler=_FakeScheduler(),
        health_monitor=HealthMonitor(),
        stats={},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=3,
        campaign_alias="alias-a",
        leads_alias="lead-list",
        campaign_run_id="run-headless",
        headless=True,
        send_flow_timeout_seconds=15.0,
        visible_browser_layout={
            "scope": "campaign:run-headless",
            "target_count": 1,
            "layout_policy": "compact",
        },
    )

    assert worker._states[0].account["campaign_desktop_layout"] == {"width": 1366, "height": 900}
    assert "visible_browser_layout" not in worker._states[0].account
    assert worker._states[0].account.get("manual_visible_browser") is None
=======
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
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
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
>>>>>>> origin/main


def test_load_accounts_blocks_legacy_proxy_accounts_from_direct_worker(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner._load_selected_accounts",
        lambda alias: [
            {
                "username": "direct-acct",
                "alias": alias,
                "active": True,
                "connected": True,
            },
            {
                "username": "legacy-acct",
                "alias": alias,
                "active": True,
                "connected": True,
                "proxy_url": "http://legacy-proxy:9000",
            },
        ],
    )
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)

    rows = load_accounts("alias-a", sent_today_counts={})

    assert [str(item.get("username") or "") for item in rows] == ["direct-acct"]
    assert rows[0]["effective_network_key"] == DIRECT_NETWORK_KEY


def test_proxy_worker_ensure_session_rejects_network_identity_mismatch(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.get_account",
        lambda _username: {
            "username": "acct-a",
            "active": True,
            "connected": True,
            "assigned_proxy_id": "proxy-b",
            "max_messages": 10,
            "sent_today": 0,
        },
    )

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id="proxy-a",
        accounts=[{"username": "acct-a", "active": True, "connected": True, "assigned_proxy_id": "proxy-a"}],
        all_proxy_ids=["proxy:proxy-a", "proxy:proxy-b"],
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

    assert worker._ensure_session(worker._states[0]) is False
    assert worker._states[0].disabled_for_campaign is True
    assert worker._states[0].preflight_failure_reason == "network_identity_mismatch"


def test_proxy_worker_ensure_session_syncs_connected_without_invalidating_health(monkeypatch) -> None:
    mark_calls: list[tuple[str, bool, bool]] = []

    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.account_proxy_preflight", _fake_account_proxy_preflight)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.get_account",
        lambda _username: {
            "username": "acct-a",
            "active": True,
            "connected": True,
            "assigned_proxy_id": "proxy-a",
            "max_messages": 10,
            "sent_today": 0,
        },
    )
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.mark_connected",
        lambda username, connected, *, invalidate_health=False: mark_calls.append(
            (username, connected, invalidate_health)
        ),
    )

    worker = ProxyWorker(
        worker_id="worker-1",
        proxy_id="proxy-a",
        accounts=[{"username": "acct-a", "active": True, "connected": True, "assigned_proxy_id": "proxy-a"}],
        all_proxy_ids=["proxy:proxy-a"],
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

    assert worker._ensure_session(worker._states[0]) is True
    assert mark_calls == [("acct-a", True, False)]
<<<<<<< HEAD


def test_proxy_worker_account_limit_does_not_requery_live_quota_mid_run(monkeypatch) -> None:
    class _FakeSender:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
            return None

    class _DummyScheduler:
        pass

    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner.HumanInstagramSender", _FakeSender)
    monkeypatch.setattr("src.dm_campaign.proxy_workers_runner._account_has_storage_state", lambda _account: True)
    monkeypatch.setattr(
        "src.dm_campaign.proxy_workers_runner.can_send_message_for_account",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live quota should stay frozen after preselection")),
    )

    worker = ProxyWorker(
        worker_id="worker-1",
        network_key=DIRECT_NETWORK_KEY,
        proxy_id=DIRECT_NETWORK_KEY,
        accounts=[{"username": "acct-a", "max_messages": 3, "sent_today": 2}],
        all_proxy_ids=[DIRECT_NETWORK_KEY],
        scheduler=_DummyScheduler(),
        health_monitor=HealthMonitor(),
        stats={"sent": 0, "failed": 0},
        stats_lock=threading.Lock(),
        delay_min=0,
        delay_max=0,
        template_rotator=TemplateRotator(["Hola"]),
        cooldown_fail_threshold=2,
        campaign_alias="alias-a",
        leads_alias="lista-a",
        campaign_run_id="run-1",
        runtime_event_callback=None,
        headless=True,
        send_flow_timeout_seconds=30.0,
    )

    assert worker._account_reached_limit(worker._states[0]) is False
    worker._states[0].sent_count = 3
    worker._states[0].account["sent_today"] = 3
    assert worker._account_reached_limit(worker._states[0]) is True
=======
>>>>>>> origin/main
