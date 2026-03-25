from __future__ import annotations

from types import SimpleNamespace

from gui.snapshot_queries import build_campaign_monitor_snapshot


class _FakeCampaignService:
    def __init__(self, snapshot: dict[str, object]) -> None:
        self._snapshot = dict(snapshot)

    def current_run_snapshot(self, *, run_id: str = "") -> dict[str, object]:
        if run_id and run_id != str(self._snapshot.get("run_id") or ""):
            return {}
        return dict(self._snapshot)


class _FakeTasks:
    def is_running(self, name: str) -> bool:
        return str(name or "").strip() == "campaign"


class _IdleTasks:
    def is_running(self, name: str) -> bool:
        del name
        return False


def test_campaign_monitor_progress_uses_terminal_outcomes_not_queue_drain() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaignService(
            {
                "run_id": "run-1",
                "alias": "alias-1",
                "leads_alias": "leads-1",
                "sent": 0,
                "failed": 0,
                "skipped": 0,
                "retried": 0,
                "remaining": 1,
                "total_leads": 100,
                "workers_active": 2,
                "workers_requested": 2,
                "workers_capacity": 2,
                "workers_effective": 2,
                "worker_rows": [],
                "task_active": True,
                "status": "Running",
            }
        )
    )

    payload = build_campaign_monitor_snapshot(
        services,
        _FakeTasks(),
        monitor_state={"run_id": "run-1", "total_leads": 100},
    )

    assert payload["progress"] == 0
    assert payload["remaining"] == 1


def test_campaign_monitor_progress_advances_from_terminal_results() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaignService(
            {
                "run_id": "run-2",
                "alias": "alias-1",
                "leads_alias": "leads-1",
                "sent": 4,
                "failed": 2,
                "skipped": 1,
                "retried": 0,
                "remaining": 0,
                "total_leads": 10,
                "workers_active": 1,
                "workers_requested": 2,
                "workers_capacity": 2,
                "workers_effective": 2,
                "worker_rows": [],
                "task_active": True,
                "status": "Running",
            }
        )
    )

    payload = build_campaign_monitor_snapshot(
        services,
        _FakeTasks(),
        monitor_state={"run_id": "run-2", "total_leads": 10},
    )

    assert payload["progress"] == 70
    assert payload["remaining"] == 0


def test_campaign_monitor_counts_preblocked_leads_in_completed_progress() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaignService(
            {
                "run_id": "run-3",
                "alias": "alias-1",
                "leads_alias": "leads-1",
                "sent": 0,
                "failed": 0,
                "skipped": 0,
                "skipped_preblocked": 10,
                "retried": 0,
                "remaining": 0,
                "total_leads": 10,
                "workers_active": 0,
                "workers_requested": 0,
                "workers_capacity": 0,
                "workers_effective": 0,
                "worker_rows": [],
                "task_active": False,
                "status": "Completed",
            }
        )
    )

    payload = build_campaign_monitor_snapshot(
        services,
        _FakeTasks(),
        monitor_state={"run_id": "run-3", "total_leads": 10},
    )

    assert payload["skipped_preblocked"] == 10
    assert payload["progress"] == 100
    assert payload["remaining"] == 0


def test_campaign_monitor_preserves_interrupted_status_as_terminal() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaignService(
            {
                "run_id": "run-4",
                "alias": "alias-1",
                "leads_alias": "leads-1",
                "sent": 3,
                "failed": 1,
                "skipped": 0,
                "retried": 0,
                "remaining": 6,
                "total_leads": 10,
                "workers_active": 0,
                "workers_requested": 2,
                "workers_capacity": 2,
                "workers_effective": 2,
                "worker_rows": [],
                "task_active": False,
                "status": "Interrupted",
                "message": "Campana interrumpida al reabrir la aplicacion.",
            }
        )
    )

    payload = build_campaign_monitor_snapshot(
        services,
        _FakeTasks(),
        monitor_state={"run_id": "run-4", "total_leads": 10},
    )

    assert payload["status"] == "Interrupted"
    assert payload["task_active"] is False
    assert payload["progress"] == 40


def test_campaign_monitor_rebuilds_last_run_without_visual_state() -> None:
    services = SimpleNamespace(
        campaigns=_FakeCampaignService(
            {
                "run_id": "run-recovered",
                "alias": "alias-1",
                "leads_alias": "leads-1",
                "sent": 3,
                "failed": 2,
                "skipped": 0,
                "retried": 0,
                "remaining": 5,
                "total_leads": 10,
                "workers_active": 0,
                "workers_requested": 2,
                "workers_capacity": 2,
                "workers_effective": 2,
                "worker_rows": [],
                "task_active": False,
                "status": "Interrupted",
                "message": "Campana recuperada desde SQLite.",
            }
        )
    )

    payload = build_campaign_monitor_snapshot(
        services,
        _IdleTasks(),
        monitor_state={},
    )

    assert payload["run_id"] == "run-recovered"
    assert payload["status"] == "Interrupted"
    assert payload["task_active"] is False
    assert payload["progress"] == 50
    assert payload["message"] == "Campana recuperada desde SQLite."
