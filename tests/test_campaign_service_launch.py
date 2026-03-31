from __future__ import annotations

import threading

import pytest

import application.services.campaign_service as campaign_service_module
from application.services.base import ServiceContext, ServiceError
from application.services.campaign_service import CampaignService
from runtime.runtime import reset_stop_event


class _IdleTaskRunner:
    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []

    def is_running(self, name: str) -> bool:
        return False

    def start_task(self, name: str, target, *, metadata=None) -> None:  # noqa: ANN001
        self.start_calls.append(
            {
                "name": str(name or ""),
                "target": target,
                "metadata": dict(metadata or {}),
            }
        )


class _BusyTaskRunner:
    def is_running(self, name: str) -> bool:
        return str(name or "").strip() == "campaign"


class _FailingTaskRunner(_IdleTaskRunner):
    def start_task(self, name: str, target, *, metadata=None) -> None:  # noqa: ANN001
        del name, target, metadata
        raise RuntimeError("spawn failed")


def _launch_payload() -> dict[str, object]:
    return {
        "alias": "ventas",
        "leads_alias": "lista-a",
        "templates": [{"id": "tpl-1", "text": "hola"}],
        "delay_min": 5,
        "delay_max": 10,
        "workers_requested": 2,
        "total_leads": 8,
        "headless": True,
    }


def _prime_launch_context(service: CampaignService) -> None:
    service._lead_store.save("lista-a", ["uno", "dos", "dos", "tres"])


def _stub_plan(
    monkeypatch,
    *,
    workers_capacity: int,
    workers_requested: int = 0,
    selected_leads_total: int = 3,
    planned_eligible_leads: int = 3,
    planned_runnable_leads: int = 3,
    remaining_slots_total: int | None = None,
) -> None:
    def _fake_plan(
        alias: str,
        *,
        leads_alias: str = "",
        workers_requested: int = 0,
        run_id: str = "",
        root_dir=None,
    ) -> dict[str, object]:
        del run_id, root_dir
        return {
            "alias": str(alias or ""),
            "leads_alias": str(leads_alias or ""),
            "workers_capacity": int(workers_capacity),
            "workers_requested": int(workers_requested),
            "workers_effective": min(int(workers_requested or 0), int(workers_capacity)) if workers_requested else int(workers_capacity),
            "selected_leads_total": int(selected_leads_total),
            "planned_eligible_leads": int(planned_eligible_leads),
            "planned_runnable_leads": int(planned_runnable_leads),
            "remaining_slots_total": int(remaining_slots_total if remaining_slots_total is not None else planned_runnable_leads),
            "proxies": [],
            "has_none_accounts": False,
            "account_remaining": [],
            "blocked_accounts": [],
            "blocked_reason_counts": {},
            "network_mode_counts": {},
            "raw_leads": ["uno", "dos", "tres"][: int(selected_leads_total)],
            "lead_filter_stats": {"skipped_already_sent": max(0, int(selected_leads_total) - int(planned_eligible_leads))},
            "planned_queue": ["uno", "dos", "tres"][: int(planned_runnable_leads)],
            "skipped_for_quota": max(0, int(planned_eligible_leads) - int(planned_runnable_leads)),
            "skipped_preblocked": max(0, int(selected_leads_total) - int(planned_eligible_leads)) + max(0, int(planned_eligible_leads) - int(planned_runnable_leads)),
        }

    monkeypatch.setattr(campaign_service_module, "calculate_workers_for_alias", _fake_plan)


def test_launch_campaign_returns_start_snapshot_and_caps_effective_workers(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    task_runner = _IdleTaskRunner()

    snapshot = service.launch_campaign(_launch_payload(), task_runner=task_runner)

    assert task_runner.start_calls[0]["name"] == "campaign"
    assert task_runner.start_calls[0]["metadata"] == {
        "alias": "ventas",
        "run_id": snapshot["run_id"],
    }
    assert snapshot["status"] == "Starting"
    assert snapshot["task_active"] is True
    assert snapshot["workers_requested"] == 2
    assert snapshot["workers_capacity"] == 3
    assert snapshot["workers_effective"] == 2
    assert snapshot["total_leads"] == 3
    assert snapshot["remaining"] == 3
    assert service.current_run_snapshot(run_id=snapshot["run_id"]) == snapshot


def test_launch_campaign_blocks_duplicate_start_from_task_runner(tmp_path) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))

    with pytest.raises(ServiceError, match="Ya hay una campana en ejecucion."):
        service.launch_campaign(_launch_payload(), task_runner=_BusyTaskRunner())


def test_launch_campaign_reconciles_persisted_active_snapshot_when_task_runner_is_idle(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    service._update_current_run(
        {
            "run_id": "run-active",
            "alias": "ventas",
            "leads_alias": "lista-a",
            "status": "Running",
            "task_active": True,
            "workers_requested": 1,
            "workers_capacity": 1,
            "workers_effective": 1,
        },
        replace=True,
    )

    snapshot = service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    assert snapshot["status"] == "Starting"
    assert snapshot["run_id"] != "run-active"


def test_launch_campaign_recovers_stale_active_snapshot_when_task_runner_is_idle(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    service._update_current_run(
        {
            "run_id": "run-stale",
            "alias": "ventas",
            "leads_alias": "lista-a",
            "status": "Running",
            "task_active": True,
            "workers_requested": 1,
            "workers_capacity": 1,
            "workers_effective": 1,
        },
        replace=True,
    )

    snapshot = service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    current = service.current_run_snapshot(run_id=snapshot["run_id"])
    assert snapshot["status"] == "Starting"
    assert current["run_id"] == snapshot["run_id"]


def test_launch_campaign_restores_previous_snapshot_when_spawn_fails(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    previous_snapshot = {
        "run_id": "run-prev",
        "alias": "ventas",
        "leads_alias": "lista-previa",
        "status": "Completed",
        "task_active": False,
        "workers_requested": 1,
        "workers_capacity": 1,
        "workers_effective": 1,
    }
    service._update_current_run(previous_snapshot, replace=True)

    with pytest.raises(ServiceError, match="spawn failed"):
        service.launch_campaign(_launch_payload(), task_runner=_FailingTaskRunner())

    current = service.current_run_snapshot()
    assert current["run_id"] == "run-prev"
    assert current["alias"] == "ventas"
    assert current["leads_alias"] == "lista-previa"
    assert current["status"] == "Completed"
    assert current["task_active"] is False


def test_launch_campaign_recomputes_total_leads_from_storage_and_not_from_gui(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=4)

    snapshot = service.launch_campaign(
        {
            **_launch_payload(),
            "total_leads": 999,
        },
        task_runner=_IdleTaskRunner(),
    )

    assert snapshot["total_leads"] == 3
    assert snapshot["remaining"] == 3


def test_launch_campaign_uses_same_day_remaining_quota_in_start_snapshot(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(
        monkeypatch,
        workers_capacity=1,
        selected_leads_total=45,
        planned_eligible_leads=45,
        planned_runnable_leads=16,
        remaining_slots_total=16,
    )

    snapshot = service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    assert snapshot["total_leads"] == 16
    assert snapshot["remaining"] == 16
    assert snapshot["selected_leads_total"] == 45
    assert snapshot["planned_eligible_leads"] == 45


def test_launch_campaign_truncates_raw_lead_list_before_persisting_start_snapshot(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(
        monkeypatch,
        workers_capacity=2,
        selected_leads_total=80,
        planned_eligible_leads=80,
        planned_runnable_leads=16,
        remaining_slots_total=16,
    )

    snapshot = service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    assert snapshot["total_leads"] == 16
    assert snapshot["remaining"] == 16
    assert snapshot["selected_leads_total"] == 80
    assert snapshot["planned_eligible_leads"] == 80


def test_launch_campaign_persists_prefiltered_totals_before_runtime_starts(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(
        monkeypatch,
        workers_capacity=2,
        selected_leads_total=50,
        planned_eligible_leads=18,
        planned_runnable_leads=16,
        remaining_slots_total=16,
    )

    snapshot = service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    assert snapshot["total_leads"] == 16
    assert snapshot["remaining"] == 16
    assert snapshot["selected_leads_total"] == 50
    assert snapshot["planned_eligible_leads"] == 18
    assert snapshot["skipped_preblocked"] == 34


def test_launch_campaign_blocks_when_runtime_disk_budget_is_low(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    monkeypatch.setattr(
        "application.services.campaign_service.snapshot_disk_usage",
        lambda _root: {"free_bytes": 1},
    )

    with pytest.raises(ServiceError, match="espacio libre suficiente"):
        service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())


def test_launch_campaign_blocks_when_sqlite_is_unavailable(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)
    service.current_run_snapshot = lambda *, run_id="": {}  # type: ignore[method-assign]

    def _broken_connect(*_args, **_kwargs):
        raise RuntimeError("db locked")

    monkeypatch.setattr("application.services.campaign_service.sqlite3.connect", _broken_connect)

    with pytest.raises(ServiceError, match="SQLite no esta disponible"):
        service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())


def test_launch_campaign_fails_cleanly_when_start_snapshot_cannot_be_persisted(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=3)

    def _broken_sync(_payload):  # noqa: ANN001
        raise RuntimeError("sqlite write failed")

    service._state_store.sync_campaign_state = _broken_sync  # type: ignore[method-assign]

    with pytest.raises(ServiceError, match="No se pudo persistir el inicio de la campana."):
        service.launch_campaign(_launch_payload(), task_runner=_IdleTaskRunner())

    assert service.current_run_snapshot() == {}


def test_campaign_service_recovers_running_snapshot_as_interrupted_on_restart(tmp_path) -> None:
    first = CampaignService(ServiceContext(root_dir=tmp_path))
    first._state_store.sync_campaign_state(
        {
            "run_id": "run-active",
            "alias": "ventas",
            "leads_alias": "lista-a",
            "status": "Running",
            "task_active": True,
            "workers_active": 1,
            "message": "Procesando campaña.",
        }
    )

    recovered = CampaignService(ServiceContext(root_dir=tmp_path))
    snapshot = recovered.current_run_snapshot()

    assert snapshot["run_id"] == "run-active"
    assert snapshot["status"] == "Interrupted"
    assert snapshot["task_active"] is False
    assert snapshot["workers_active"] == 0
    assert "reabrir la aplicacion" in snapshot["message"]


def test_campaign_service_persists_runtime_events_from_runner_progress(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=1)

    def _fake_run_campaign(config, *, progress_callback=None):  # noqa: ANN001
        if callable(progress_callback):
            progress_callback(
                {
                    "run_id": str(config.get("run_id") or ""),
                    "status": "Running",
                    "message": "Procesando cola activa de campana.",
                    "runtime_events": [
                        {
                            "event_id": "evt-proxy-degraded",
                            "event_type": "proxy_degraded",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": "Proxy degradado detectado por health monitor.",
                            "proxy_id": "proxy-a",
                        },
                        {
                            "event_id": "evt-worker-restarted",
                            "event_type": "worker_restarted",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": "Worker worker-1 relanzado en proxy-a.",
                            "worker_id": "worker-1",
                            "proxy_id": "proxy-a",
                        },
                    ],
                }
            )
        return {
            "sent": 1,
            "failed": 0,
            "skipped": 0,
            "retried": 1,
            "remaining": 0,
            "workers_requested": 1,
            "workers_capacity": 1,
            "workers_effective": 1,
        }

    monkeypatch.setattr("application.services.campaign_service.run_campaign", _fake_run_campaign)

    service.start_campaign(_launch_payload())
    snapshot = service.current_run_snapshot()
    events = service._state_store.list_campaign_events(run_id=snapshot["run_id"])
    event_types = [event["event_type"] for event in events]

    assert snapshot["status"] == "Completed"
    assert "launch_started" in event_types
    assert "proxy_degraded" in event_types
    assert "worker_restarted" in event_types
    assert "run_completed" in event_types


def test_campaign_service_persists_sent_log_failure_event_from_runner_progress(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=1)

    def _fake_run_campaign(config, *, progress_callback=None):  # noqa: ANN001
        if callable(progress_callback):
            progress_callback(
                {
                    "run_id": str(config.get("run_id") or ""),
                    "status": "Running",
                    "message": "Persistiendo sent_log.",
                    "runtime_events": [
                        {
                            "event_id": "evt-sent-log-failed",
                            "event_type": "sent_log_write_failed",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": "No se pudo escribir sent_log.",
                            "worker_id": "worker-1",
                            "lead": "lead-1",
                        }
                    ],
                }
            )
        return {
            "sent": 0,
            "failed": 1,
            "skipped": 0,
            "retried": 0,
            "remaining": 2,
            "workers_requested": 1,
            "workers_capacity": 1,
            "workers_effective": 1,
        }

    monkeypatch.setattr("application.services.campaign_service.run_campaign", _fake_run_campaign)

    service.start_campaign(_launch_payload())
    snapshot = service.current_run_snapshot()
    events = service._state_store.list_campaign_events(run_id=snapshot["run_id"])

    assert any(event["event_type"] == "sent_log_write_failed" for event in events)
    assert snapshot["status"] == "Stopped"
    assert snapshot["failed"] == 1


def test_campaign_service_marks_run_stopped_when_stop_requested_during_send(tmp_path, monkeypatch) -> None:
    service = CampaignService(ServiceContext(root_dir=tmp_path))
    _prime_launch_context(service)
    _stub_plan(monkeypatch, workers_capacity=1)
    progress_started = threading.Event()
    allow_finish = threading.Event()
    result_holder: dict[str, object] = {}
    error_holder: list[Exception] = []

    def _fake_run_campaign(config, *, progress_callback=None):  # noqa: ANN001
        if callable(progress_callback):
            progress_callback(
                {
                    "run_id": str(config.get("run_id") or ""),
                    "status": "Running",
                    "message": "Enviando DM activo.",
                    "remaining": 2,
                    "total_leads": 3,
                    "workers_active": 1,
                    "workers_requested": 1,
                    "workers_capacity": 1,
                    "workers_effective": 1,
                    "worker_rows": [
                        {
                            "worker_id": "worker-1",
                            "execution_state": "processing",
                            "execution_stage": "sending",
                            "current_account": "acct-1",
                            "current_lead": "lead-1",
                            "restarts": 0,
                        }
                    ],
                }
            )
        progress_started.set()
        assert allow_finish.wait(1.0)
        return {
            "sent": 1,
            "failed": 0,
            "skipped": 0,
            "retried": 0,
            "remaining": 2,
            "workers_requested": 1,
            "workers_capacity": 1,
            "workers_effective": 1,
        }

    def _run_start() -> None:
        try:
            result_holder["result"] = service.start_campaign(_launch_payload())
        except Exception as exc:  # pragma: no cover - defensive capture for assertion below
            error_holder.append(exc)

    monkeypatch.setattr("application.services.campaign_service.run_campaign", _fake_run_campaign)

    worker = threading.Thread(target=_run_start, daemon=True)
    worker.start()
    try:
        assert progress_started.wait(1.0)
        service.stop_campaign("stop from test")
        stopping = service.current_run_snapshot()

        assert stopping["status"] == "Stopping"
        assert stopping["task_active"] is True

        allow_finish.set()
        worker.join(timeout=1.0)
        assert not error_holder
        assert worker.is_alive() is False

        snapshot = service.current_run_snapshot()
        events = service._state_store.list_campaign_events(run_id=snapshot["run_id"])
        event_types = [event["event_type"] for event in events]

        assert snapshot["status"] == "Stopped"
        assert snapshot["task_active"] is False
        assert snapshot["remaining"] == 2
        assert snapshot["sent"] == 1
        assert "stop_requested" in event_types
        assert "run_stopped" in event_types
    finally:
        allow_finish.set()
        worker.join(timeout=1.0)
        reset_stop_event()
