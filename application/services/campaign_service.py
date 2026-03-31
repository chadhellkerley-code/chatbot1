from __future__ import annotations

import logging
import sqlite3
import tempfile
import threading
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from core.disk_monitor import snapshot_disk_usage
from core import ig as ig_module
from core.leads_store import LeadListStore, LeadListStoreError
from core.templates_store import TemplateStore
from runtime.runtime import request_stop, reset_stop_event
from src.dm_campaign.campaign_runner import start_campaign as run_campaign
from src.dm_campaign.contracts import CampaignCapacity, CampaignLaunchRequest, CampaignRunSnapshot, CampaignRunStatus
from src.dm_campaign.proxy_workers_runner import calculate_workers_for_alias, refresh_campaign_runtime_paths
from src.persistence import get_app_state_store

from .base import ServiceContext, ServiceError, normalize_alias

logger = logging.getLogger(__name__)
_CAMPAIGN_HEARTBEAT_INTERVAL_SECONDS = 1.0
_CAMPAIGN_PREFLIGHT_MIN_FREE_BYTES = 256 * 1024 * 1024


class CampaignService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        self._refresh_campaign_runtime_paths()
        self._template_store = TemplateStore(self.context.root_dir)
        self._lead_store = LeadListStore(self.context.leads_path())
        self._state_store = get_app_state_store(context.root_dir)
        self._launch_lock = threading.RLock()
        self._run_lock = threading.RLock()
        self._current_run: dict[str, Any] = {}
        self._restore_persisted_run_state()

    def _copy_run_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        snapshot = dict(payload)
        worker_rows = snapshot.get("worker_rows")
        if isinstance(worker_rows, list):
            snapshot["worker_rows"] = [
                dict(row) if isinstance(row, dict) else row
                for row in worker_rows
            ]
        return snapshot

    def _update_current_run(self, payload: dict[str, Any], *, replace: bool = False) -> None:
        if not isinstance(payload, dict):
            return
        if replace:
            snapshot = CampaignRunSnapshot.from_payload(payload).to_payload()
        else:
            run_id = str(payload.get("run_id") or "").strip()
            current = self._copy_run_snapshot(self.current_run_snapshot(run_id=run_id))
            current.update(self._copy_run_snapshot(payload))
            snapshot = CampaignRunSnapshot.from_payload(current).to_payload()
        self._state_store.sync_campaign_state(snapshot)
        with self._run_lock:
            self._current_run = snapshot

    def current_run_snapshot(self, *, run_id: str = "") -> dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        persisted = self._load_persisted_run_snapshot(run_id=clean_run_id)
        if persisted:
            return persisted
        with self._run_lock:
            if not self._current_run:
                return {}
            current = CampaignRunSnapshot.from_payload(self._current_run).to_payload()
        if clean_run_id and clean_run_id != str(current.get("run_id") or "").strip():
            return {}
        return current

    def _load_persisted_run_snapshot(self, *, run_id: str = "") -> dict[str, Any]:
        payload = self._state_store.get_campaign_state(run_id=run_id)
        if not payload:
            return {}
        snapshot = CampaignRunSnapshot.from_payload(payload).to_payload()
        with self._run_lock:
            self._current_run = snapshot
        return snapshot

    def _restore_persisted_run_state(self) -> None:
        try:
            recovered = self._state_store.recover_interrupted_campaign_states()
            if recovered:
                for payload in recovered:
                    run_id = str(payload.get("run_id") or "").strip()
                    if not run_id:
                        continue
                    self._emit_service_event(
                        run_id=run_id,
                        event_type="run_interrupted_recovered",
                        severity="warning",
                        failure_kind="system",
                        message="Campana recuperada como Interrupted al reabrir la aplicacion.",
                    )
                snapshot = CampaignRunSnapshot.from_payload(recovered[-1]).to_payload()
                with self._run_lock:
                    self._current_run = snapshot
                return
            self._load_persisted_run_snapshot()
        except Exception:
            logger.exception("No se pudo restaurar campaign_state desde SQLite")

    def _refresh_campaign_runtime_paths(self) -> dict[str, Path]:
        payload = refresh_campaign_runtime_paths(self.context.root_dir)
        return {
            key: Path(value)
            for key, value in payload.items()
            if isinstance(value, Path)
        }

    def _log_campaign_paths(self, request: CampaignLaunchRequest) -> None:
        try:
            leads_path = self._lead_store.path_for(request.leads_alias).resolve()
        except Exception:
            leads_path = self.context.leads_path(request.leads_alias).resolve()
        sent_log_path = self.context.storage_path("sent_log.jsonl").resolve()
        lead_status_path = self.context.storage_path("lead_status.json").resolve()
        self._log_campaign(
            "info",
            request.run_id,
            "campaign_paths leads=%s sent_log=%s lead_status=%s",
            leads_path,
            sent_log_path,
            lead_status_path,
        )

    def _start_heartbeat(self, run_id: str) -> tuple[threading.Event, threading.Thread]:
        stop_event = threading.Event()

        def _beat() -> None:
            while not stop_event.wait(_CAMPAIGN_HEARTBEAT_INTERVAL_SECONDS):
                snapshot = self.current_run_snapshot(run_id=run_id)
                if not snapshot or not bool(snapshot.get("task_active")):
                    return
                try:
                    self._update_current_run({"run_id": run_id})
                except Exception:
                    logger.exception("No se pudo persistir heartbeat de campaign_state")
                    return

        thread = threading.Thread(
            target=_beat,
            name=f"campaign-heartbeat-{run_id}",
            daemon=True,
        )
        thread.start()
        return stop_event, thread

    def list_templates(self) -> list[dict[str, Any]]:
        return self._template_store.load_templates()

    def get_capacity(
        self,
        alias: str,
        *,
        leads_alias: str = "",
        workers_requested: int = 0,
        run_id: str = "",
    ) -> dict[str, Any]:
        clean_alias = normalize_alias(alias)
        self._refresh_campaign_runtime_paths()
        try:
            payload = calculate_workers_for_alias(
                clean_alias,
                leads_alias=str(leads_alias or "").strip(),
                workers_requested=max(0, int(workers_requested or 0)),
                run_id=str(run_id or "").strip(),
                root_dir=self.context.root_dir,
            )
        except Exception:
            payload = {}
        return CampaignCapacity.from_payload(
            {
                "alias": clean_alias,
                "workers_capacity": payload.get("workers_capacity"),
                "leads_alias": payload.get("leads_alias"),
                "proxies": payload.get("proxies"),
                "has_none_accounts": payload.get("has_none_accounts"),
                "workers_requested": payload.get("workers_requested"),
                "workers_effective": payload.get("workers_effective"),
                "selected_leads_total": payload.get("selected_leads_total"),
                "planned_eligible_leads": payload.get("planned_eligible_leads"),
                "planned_runnable_leads": payload.get("planned_runnable_leads"),
                "remaining_slots_total": payload.get("remaining_slots_total"),
                "account_remaining": payload.get("account_remaining"),
            }
        ).to_payload()

    def _build_launch_request(self, config: CampaignLaunchRequest | Mapping[str, Any]) -> CampaignLaunchRequest:
        if isinstance(config, CampaignLaunchRequest):
            raw_payload = config.to_payload()
        elif isinstance(config, Mapping):
            raw_payload = dict(config)
        else:
            raw_payload = {}
        alias = normalize_alias(raw_payload.get("alias"), default="")
        leads_alias = normalize_alias(raw_payload.get("leads_alias"), default="")
        return CampaignLaunchRequest.from_payload(
            {
                **raw_payload,
                "alias": alias,
                "leads_alias": leads_alias,
                "root_dir": str(self.context.root_dir),
                "run_id": str(raw_payload.get("run_id") or "").strip()
                or datetime.now().strftime("campaign-%Y%m%d%H%M%S%f"),
                "started_at": str(raw_payload.get("started_at") or "").strip()
                or datetime.now().isoformat(timespec="seconds"),
            }
        )

    @staticmethod
    def _replace_launch_request(
        request: CampaignLaunchRequest,
        **updates: Any,
    ) -> CampaignLaunchRequest:
        return CampaignLaunchRequest.from_payload(
            {
                **request.to_payload(),
                **updates,
            }
        )

    @staticmethod
    def _log_campaign(level: str, run_id: str, message: str, *args: Any, exc_info: bool = False) -> None:
        log_method = getattr(logger, level)
        clean_run_id = str(run_id or "-").strip() or "-"
        if level == "exception" and not exc_info:
            exc_info = True
        log_method(f"[run_id=%s] {message}", clean_run_id, *args, exc_info=exc_info)

    def _record_campaign_event(self, payload: Mapping[str, Any] | None) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {}
        raw_event = dict(payload)
        run_id = str(raw_event.get("run_id") or "").strip()
        if not run_id:
            return {}
        try:
            event = self._state_store.append_campaign_event(raw_event)
        except Exception:
            self._log_campaign("exception", run_id, "No se pudo persistir campaign_event.")
            return {}
        if not event:
            return {}
        severity = str(event.get("severity") or "info").strip().lower()
        failure_kind = str(event.get("failure_kind") or "").strip().lower()
        details = []
        if failure_kind:
            details.append(failure_kind)
        for key in ("worker_id", "proxy_id", "account", "lead"):
            value = str(event.get(key) or "").strip()
            if value:
                details.append(f"{key}={value}")
        details_suffix = f" ({', '.join(details)})" if details else ""
        level = "error" if severity == "error" else "warning" if severity == "warning" else "info"
        self._log_campaign(
            level,
            run_id,
            "event=%s%s | %s",
            str(event.get("event_type") or "-").strip(),
            details_suffix,
            str(event.get("message") or "-").strip() or "-",
        )
        return event

    def _emit_service_event(
        self,
        *,
        run_id: str,
        event_type: str,
        message: str,
        severity: str = "info",
        failure_kind: str = "",
        **payload: Any,
    ) -> dict[str, Any]:
        return self._record_campaign_event(
            {
                "run_id": str(run_id or "").strip(),
                "event_type": str(event_type or "").strip(),
                "severity": str(severity or "info").strip().lower() or "info",
                "failure_kind": str(failure_kind or "").strip().lower(),
                "message": str(message or "").strip(),
                **payload,
            }
        )

    def _record_runtime_events(self, payload: dict[str, Any], *, run_id: str) -> None:
        raw_events = payload.pop("runtime_events", None)
        if not isinstance(raw_events, list):
            return
        for item in raw_events:
            if not isinstance(item, dict):
                continue
            self._record_campaign_event(
                {
                    "run_id": str(item.get("run_id") or run_id or "").strip(),
                    **item,
                }
            )

    def _campaign_task_running(self, task_runner: Any | None = None) -> bool:
        is_running = getattr(task_runner, "is_running", None)
        if not callable(is_running):
            return False
        try:
            return bool(is_running("campaign"))
        except Exception:
            return False

    def _validate_launch_request(self, request: CampaignLaunchRequest, *, task_runner: Any | None = None) -> None:
        active_run = CampaignRunSnapshot.from_payload(self.current_run_snapshot())
        task_running = self._campaign_task_running(task_runner)
        if active_run.task_active and not task_running:
            reconciled = active_run.to_payload()
            reconciled.update(
                {
                    "status": (
                        CampaignRunStatus.STOPPED.value
                        if CampaignRunStatus.parse(active_run.status) == CampaignRunStatus.STOPPING
                        else CampaignRunStatus.INTERRUPTED.value
                    ),
                    "message": "Campana recuperada tras cierre inconsistente del runtime anterior.",
                    "task_active": False,
                    "workers_active": 0,
                    "worker_rows": [],
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            self._update_current_run(reconciled, replace=True)
            active_run = CampaignRunSnapshot.from_payload(reconciled)
        if task_running:
            raise ServiceError("Ya hay una campana en ejecucion.")
        if active_run.task_active:
            raise ServiceError("Ya hay una campana en ejecucion.")
        if not request.alias:
            raise ServiceError("Debes seleccionar un alias.")
        if not request.leads_alias:
            raise ServiceError("Debes seleccionar una lista de leads.")
        if request.delay_max < request.delay_min:
            raise ServiceError("El delay maximo no puede ser menor al minimo.")

    def _resolve_request_templates(self, request: CampaignLaunchRequest) -> CampaignLaunchRequest:
        stored_by_id: dict[str, dict[str, Any]] = {}
        stored_by_name: dict[str, dict[str, Any]] = {}
        for item in self.list_templates():
            if not isinstance(item, dict):
                continue
            template_id = str(item.get("id") or "").strip()
            template_name = str(item.get("name") or "").strip()
            template_text = str(item.get("text") or "").strip()
            if not template_text:
                continue
            payload = {
                "id": template_id,
                "name": template_name,
                "text": template_text,
            }
            if template_id:
                stored_by_id.setdefault(template_id, payload)
            if template_name:
                stored_by_name.setdefault(template_name.casefold(), payload)

        resolved_templates: list[dict[str, Any]] = []
        for item in request.templates:
            raw = dict(item or {})
            template_id = str(raw.get("id") or "").strip()
            template_name = str(raw.get("name") or "").strip()
            template_text = str(raw.get("text") or "").strip()
            stored = stored_by_id.get(template_id) or stored_by_name.get(template_name.casefold())
            if stored is not None:
                resolved_templates.append(dict(stored))
                continue
            if template_text:
                resolved_templates.append(
                    {
                        "id": template_id,
                        "name": template_name,
                        "text": template_text,
                    }
                )

        if not resolved_templates:
            raise ServiceError("Debes configurar al menos una plantilla valida.")
        return self._replace_launch_request(request, templates=resolved_templates)

    def _resolve_request_total_leads(self, request: CampaignLaunchRequest) -> CampaignLaunchRequest:
        try:
            leads_path = self._lead_store.path_for(request.leads_alias)
        except LeadListStoreError as exc:
            raise ServiceError(str(exc)) from exc
        except OSError as exc:
            raise ServiceError("No se pudo acceder a la lista seleccionada.") from exc
        if not leads_path.exists():
            raise ServiceError("La lista seleccionada no existe.")
        try:
            summary = self._lead_store.summary(request.leads_alias)
        except LeadListStoreError as exc:
            raise ServiceError(str(exc)) from exc
        except OSError as exc:
            raise ServiceError("No se pudo leer la lista seleccionada.") from exc
        total_leads = max(0, int(summary.get("count") or 0))
        if total_leads <= 0:
            raise ServiceError("La lista seleccionada no tiene leads disponibles.")
        return self._replace_launch_request(request, total_leads=total_leads)

    def _resolve_request_capacity(self, request: CampaignLaunchRequest) -> CampaignLaunchRequest:
        capacity = CampaignCapacity.from_payload(
            self.get_capacity(
                request.alias,
                leads_alias=request.leads_alias,
                workers_requested=request.workers_requested,
                run_id=request.run_id,
            )
        )
        return request.with_capacity(capacity.workers_capacity)

    def _resolve_request_plan(self, request: CampaignLaunchRequest) -> tuple[CampaignLaunchRequest, dict[str, Any]]:
        try:
            self._refresh_campaign_runtime_paths()
            plan = dict(
                calculate_workers_for_alias(
                    request.alias,
                    leads_alias=request.leads_alias,
                    workers_requested=request.workers_requested,
                    run_id=request.run_id,
                    root_dir=self.context.root_dir,
                )
                or {}
            )
        except Exception as exc:
            raise ServiceError("No se pudo calcular el plan real de la campaña.") from exc
        planned_total = max(0, int(plan.get("planned_runnable_leads") or 0))
        workers_capacity = max(0, int(plan.get("workers_capacity") or 0))
        return self._replace_launch_request(
            request,
            total_leads=planned_total,
            workers_capacity=workers_capacity,
            selected_leads_total=max(0, int(plan.get("selected_leads_total") or 0)),
            planned_eligible_leads=max(0, int(plan.get("planned_eligible_leads") or 0)),
            planned_queue=list(plan.get("planned_queue") or []),
        ), plan

    def _validate_runtime_db_available(self) -> None:
        raw_db_path = getattr(self._state_store, "db_path", "")
        if not str(raw_db_path or "").strip():
            raise ServiceError("No se encontro la base de estado de la aplicacion.")
        db_path = Path(raw_db_path)
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(db_path, timeout=1.0) as connection:
                connection.execute("pragma busy_timeout = 1000")
                connection.execute("select 1").fetchone()
                connection.execute("begin immediate")
                connection.rollback()
        except Exception as exc:
            raise ServiceError("SQLite no esta disponible para iniciar la campana.") from exc

    @staticmethod
    def _probe_parent_writable(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        probe_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent,
                prefix=f".{path.stem}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                probe_path = Path(handle.name)
        finally:
            if probe_path is not None:
                probe_path.unlink(missing_ok=True)

    def _validate_runtime_path_writable(self, path: Path, *, label: str) -> None:
        try:
            if path.exists():
                if not path.is_file():
                    raise ServiceError(f"La ruta critica {label} no es un archivo.")
                with path.open("ab"):
                    pass
                return
            self._probe_parent_writable(path)
        except ServiceError:
            raise
        except Exception as exc:
            raise ServiceError(f"No se puede escribir en {label}.") from exc

    def _validate_runtime_outputs_writable(self) -> None:
        self._validate_runtime_path_writable(
            self.context.storage_path("sent_log.jsonl"),
            label="storage/sent_log.jsonl",
        )
        self._validate_runtime_path_writable(
            self.context.storage_path("lead_status.json"),
            label="storage/lead_status.json",
        )

    def _validate_runtime_disk_budget(self) -> None:
        try:
            snapshot = snapshot_disk_usage(self.context.root_dir)
        except Exception as exc:
            raise ServiceError("No se pudo verificar el espacio libre del disco.") from exc
        free_bytes = max(0, int(snapshot.get("free_bytes") or 0))
        if free_bytes < _CAMPAIGN_PREFLIGHT_MIN_FREE_BYTES:
            raise ServiceError("No hay espacio libre suficiente para iniciar la campana.")

    def _validate_runtime_environment(self) -> None:
        self._validate_runtime_db_available()
        self._validate_runtime_outputs_writable()
        self._validate_runtime_disk_budget()

    def _preflight_launch_request(
        self,
        request: CampaignLaunchRequest,
        *,
        task_runner: Any | None = None,
    ) -> tuple[CampaignLaunchRequest, dict[str, Any]]:
        self._refresh_campaign_runtime_paths()
        self._validate_launch_request(request, task_runner=task_runner)
        request = self._resolve_request_templates(request)
        request = self._resolve_request_total_leads(request)
        request, plan = self._resolve_request_plan(request)
        if request.workers_effective <= 0:
            raise ServiceError("No hay workers disponibles para el alias seleccionado.")
        self._validate_runtime_environment()
        return request, plan

    def _prepare_launch_request(
        self,
        config: CampaignLaunchRequest | Mapping[str, Any],
        *,
        task_runner: Any | None = None,
    ) -> tuple[CampaignLaunchRequest, dict[str, Any]]:
        request = self._build_launch_request(config)
        return self._preflight_launch_request(request, task_runner=task_runner)

    def _starting_snapshot(self, request: CampaignLaunchRequest, *, plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
        planning = dict(plan or {})
        snapshot = CampaignRunSnapshot.starting(request).to_payload()
        snapshot["selected_leads_total"] = max(0, int(planning.get("selected_leads_total") or 0))
        snapshot["planned_eligible_leads"] = max(0, int(planning.get("planned_eligible_leads") or request.total_leads))
        snapshot["skipped_preblocked"] = max(0, int(planning.get("skipped_preblocked") or 0))
        if request.workers_effective < request.workers_requested:
            snapshot["message"] = (
                f"Preparando campana y workers... "
                f"Aplicando {request.workers_effective} de {request.workers_requested} solicitados."
            )
        return snapshot

    def _persist_launch_started(self, request: CampaignLaunchRequest, *, plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
        snapshot = self._starting_snapshot(request, plan=plan)
        try:
            self._update_current_run(snapshot, replace=True)
        except Exception as exc:
            raise ServiceError("No se pudo persistir el inicio de la campana.") from exc
        self._emit_service_event(
            run_id=request.run_id,
            event_type="launch_started",
            message="Launch validado y snapshot Starting persistido.",
            severity="info",
            alias=request.alias,
            leads_alias=request.leads_alias,
            workers_requested=request.workers_requested,
            workers_capacity=request.workers_capacity,
            workers_effective=request.workers_effective,
        )
        return snapshot

    def _launch_failed_snapshot(
        self,
        request: CampaignLaunchRequest,
        *,
        message: str,
        plan: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = self._starting_snapshot(request, plan=plan)
        snapshot.update(
            {
                "status": CampaignRunStatus.FAILED.value,
                "message": str(message or "").strip() or "No se pudo iniciar la campana.",
                "task_active": False,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "workers_active": 0,
                "worker_rows": [],
            }
        )
        return snapshot

    def launch_campaign(self, config: CampaignLaunchRequest | Mapping[str, Any], *, task_runner: Any) -> dict[str, Any]:
        with self._launch_lock:
            previous_snapshot = self.current_run_snapshot()
            request, plan = self._prepare_launch_request(config, task_runner=task_runner)
            starting_snapshot = self._persist_launch_started(request, plan=plan)
            self._log_campaign_paths(request)
            try:
                task_runner.start_task(
                    "campaign",
                    lambda request=request: self._run_campaign(request),
                    metadata={
                        "alias": request.alias,
                        "run_id": request.run_id,
                    },
                )
            except Exception as exc:
                if previous_snapshot:
                    self._update_current_run(previous_snapshot, replace=True)
                else:
                    self._update_current_run(
                        self._launch_failed_snapshot(request, message=str(exc) or exc.__class__.__name__, plan=plan),
                        replace=True,
                    )
                self._emit_service_event(
                    run_id=request.run_id,
                    event_type="launch_spawn_failed",
                    severity="error",
                    failure_kind="system",
                    message=str(exc) or exc.__class__.__name__,
                )
                raise ServiceError(str(exc) or exc.__class__.__name__) from exc
            return self.current_run_snapshot(run_id=request.run_id) or starting_snapshot

    def _run_campaign(self, request: CampaignLaunchRequest) -> dict[str, Any]:
        self._refresh_campaign_runtime_paths()
        def _progress_update(progress: dict[str, Any]) -> None:
            patch = dict(progress or {})
            self._record_runtime_events(patch, run_id=request.run_id)
            status = CampaignRunStatus.parse(patch.get("status"))
            if status.is_terminal:
                patch["task_active"] = False
                patch.setdefault("finished_at", datetime.now().isoformat(timespec="seconds"))
            else:
                patch["task_active"] = True
            self._update_current_run(patch)

        heartbeat_stop, heartbeat_thread = self._start_heartbeat(request.run_id)
        try:
            reset_stop_event()
            result = run_campaign(request.to_runner_payload(), progress_callback=_progress_update)
        except Exception as exc:
            self._emit_service_event(
                run_id=request.run_id,
                event_type="runner_crashed",
                severity="error",
                failure_kind="system",
                message=str(exc) or exc.__class__.__name__,
            )
            self._update_current_run(
                {
                    "status": CampaignRunStatus.FAILED.value,
                    "message": str(exc) or exc.__class__.__name__,
                    "task_active": False,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "worker_rows": [],
                    "workers_active": 0,
                }
            )
            raise
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.5)

        current = self.current_run_snapshot(run_id=request.run_id)
        status = CampaignRunStatus.parse(current.get("status"))
        if not status.is_terminal:
            status = (
                CampaignRunStatus.COMPLETED
                if int(result.get("remaining") or 0) <= 0
                else CampaignRunStatus.STOPPED
            )
        if status == CampaignRunStatus.COMPLETED:
            message = "Campana finalizada."
        elif status == CampaignRunStatus.STOPPED:
            message = "Campana detenida."
        else:
            message = str(current.get("message") or "").strip()

        self._update_current_run(
            {
                "sent": int(result.get("sent") or 0),
                "failed": int(result.get("failed") or 0),
                "skipped": int(result.get("skipped") or 0),
                "skipped_preblocked": int(result.get("skipped_preblocked") or 0),
                "retried": int(result.get("retried") or 0),
                "remaining": int(result.get("remaining") or 0),
                "workers_active": 0,
                "workers_requested": int(result.get("workers_requested") or request.workers_requested),
                "workers_capacity": int(result.get("workers_capacity") or request.workers_capacity),
                "workers_effective": int(result.get("workers_effective") or request.workers_effective),
                "status": status.value,
                "message": message,
                "task_active": False,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "worker_rows": [],
            }
        )
        final_failure_kind = "terminal" if status == CampaignRunStatus.FAILED else ""
        final_severity = "error" if status == CampaignRunStatus.FAILED else "info"
        self._emit_service_event(
            run_id=request.run_id,
            event_type=f"run_{status.value.lower()}",
            severity=final_severity,
            failure_kind=final_failure_kind,
            message=message or status.value,
            sent=int(result.get("sent") or 0),
            failed=int(result.get("failed") or 0),
            skipped=int(result.get("skipped") or 0),
            retried=int(result.get("retried") or 0),
            remaining=int(result.get("remaining") or 0),
        )
        return result

    def start_campaign(self, config: CampaignLaunchRequest | Mapping[str, Any]) -> dict[str, Any]:
        with self._launch_lock:
            request, plan = self._prepare_launch_request(config)
            self._persist_launch_started(request, plan=plan)
        return self._run_campaign(request)

    def stop_campaign(self, reason: str = "campaign stopped from GUI") -> None:
        current = self.current_run_snapshot()
        if current:
            self._update_current_run(
                {
                    "status": CampaignRunStatus.STOPPING.value,
                    "message": "Solicitando stop seguro...",
                    "task_active": True,
                }
            )
            self._emit_service_event(
                run_id=str(current.get("run_id") or "").strip(),
                event_type="stop_requested",
                severity="warning",
                failure_kind="retryable",
                message="Solicitando stop seguro desde GUI/servicio.",
                reason=str(reason or "").strip() or "campaign stopped from GUI",
            )
        request_stop(str(reason or "").strip() or "campaign stopped from GUI")

    def build_template_entries(
        self,
        *,
        use_saved_template: str = "",
        manual_message: str = "",
    ) -> list[dict[str, Any]]:
        build_template_entry = getattr(ig_module, "_build_template_entry", None)
        clean_saved_template = str(use_saved_template or "").strip()
        if clean_saved_template:
            for item in self.list_templates():
                template_id = str(item.get("id") or "").strip()
                name = str(item.get("name") or "").strip()
                if clean_saved_template in {template_id, name}:
                    return [dict(item)]
        clean_manual = str(manual_message or "").strip()
        if clean_manual and callable(build_template_entry):
            return [build_template_entry("", clean_manual)]
        if clean_manual:
            return [{"name": "", "text": clean_manual}]
        return []
