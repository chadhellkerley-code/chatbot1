from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, MutableMapping, Optional

from core.accounts import (
    connected_status,
    get_account,
    has_playwright_storage_state,
    list_all,
    mark_connected,
    playwright_storage_state_path,
)
from core.leads import load_list
from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from runtime.runtime import (
    EngineCancellationToken,
    STOP_EVENT,
    bind_stop_token,
    bind_stop_token_callable,
    restore_stop_token,
)
from src.dm_campaign.adaptive_scheduler import AdaptiveScheduler, LeadTask
from src.dm_campaign.contracts import (
    CampaignRunSnapshot,
    CampaignRunStatus,
    CampaignSendResult,
    CampaignSendStatus,
    WorkerExecutionStage,
    WorkerExecutionState,
)
from src.dm_campaign.health_monitor import HealthMonitor
from src.dm_campaign.lead_status_store import (
    apply_terminal_status_updates,
    get_prefilter_snapshot,
    mark_lead_failed,
    mark_lead_sent,
    mark_lead_skipped,
)
from src.dm_campaign.worker_state_machine import CampaignWorkerStateMachine, WorkerStateSnapshot
from src.proxy_payload import proxy_from_account
from src.runtime.playwright_runtime import PlaywrightRuntimeCancelledError, PlaywrightRuntimeTimeoutError
from src.transport.human_instagram_sender import HumanInstagramSender
from core.storage import (
    campaign_start_snapshot,
    log_sent,
    normalize_contact_username,
)
from core.templates_store import render_template


logger = logging.getLogger(__name__)
LOCAL_WORKER_PROXY_ID = "__no_proxy__"


def _is_local_proxy_id(proxy_id: str) -> bool:
    normalized = str(proxy_id or "").strip().lower()
    return normalized in {"", LOCAL_WORKER_PROXY_ID}


def _account_storage_state_path(username: str) -> Path:
    return playwright_storage_state_path(username)


def _account_has_storage_state(account: Dict[str, Any]) -> bool:
    if not isinstance(account, dict):
        return False
    username = str(account.get("username") or "").strip()
    if not username:
        return False
    return has_playwright_storage_state(username)


def _order_accounts_for_worker_start(accounts: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    ready_accounts: list[Dict[str, Any]] = []
    pending_accounts: list[Dict[str, Any]] = []
    for account in accounts:
        if _account_has_storage_state(account):
            ready_accounts.append(account)
            continue
        pending_accounts.append(account)
    return ready_accounts + pending_accounts


@dataclass
class AccountRuntimeState:
    account: Dict[str, Any]
    max_messages: int
    next_send_time: float = 0.0
    sent_count: int = 0
    fail_count: int = 0
    cooldown_until: float = 0.0
    disabled_for_campaign: bool = False
    session_ready: bool = False


@dataclass(frozen=True)
class AccountWaitDecision:
    seconds: float
    stage: WorkerExecutionStage
    reason: str


class TemplateRotator:
    """Thread-safe round-robin selector for template variants."""

    def __init__(self, variants: list[str]) -> None:
        cleaned = [str(item or "").strip() for item in variants if str(item or "").strip()]
        self._variants: list[str] = cleaned or ["hola!"]
        self._cursor = 0
        self._lock = threading.Lock()

    @property
    def total_variants(self) -> int:
        return len(self._variants)

    def next_variant(self) -> tuple[str, int]:
        with self._lock:
            index = self._cursor % len(self._variants)
            self._cursor += 1
            return self._variants[index], index


class ProxyWorker:
    def __init__(
        self,
        *,
        worker_id: str,
        proxy_id: str,
        accounts: list[Dict[str, Any]],
        all_proxy_ids: list[str],
        scheduler: AdaptiveScheduler,
        health_monitor: HealthMonitor,
        stats: Dict[str, int],
        stats_lock: threading.Lock,
        delay_min: int,
        delay_max: int,
        template_rotator: TemplateRotator,
        cooldown_fail_threshold: int,
        campaign_alias: str,
        leads_alias: str,
        campaign_run_id: str,
        runtime_event_callback: Callable[[dict[str, Any]], None] | None = None,
        headless: bool,
        send_flow_timeout_seconds: float,
    ) -> None:
        self.worker_id = worker_id
        self.proxy_id = str(proxy_id or LOCAL_WORKER_PROXY_ID).strip() or LOCAL_WORKER_PROXY_ID
        self._is_local_worker = _is_local_proxy_id(self.proxy_id)
        self.accounts = accounts
        self.browser = None
        self.context = None
        self.lead_queue = scheduler
        self.delay_min = max(0, int(delay_min))
        self.delay_max = max(self.delay_min, int(delay_max))
        self._template_rotator = template_rotator
        self._scheduler = scheduler
        self._health = health_monitor
        self._stats = stats
        self._stats_lock = stats_lock
        self._campaign_alias = str(campaign_alias or "").strip().lower()
        self._leads_alias = str(leads_alias or "").strip().lower()
        self._campaign_run_id = str(campaign_run_id or "").strip()
        self._all_proxy_ids = [str(item or "").strip() for item in all_proxy_ids if str(item or "").strip()]
        self._cooldown_fail_threshold = max(1, int(cooldown_fail_threshold))
        self._send_flow_timeout_seconds = max(10.0, float(send_flow_timeout_seconds or 10.0))
        self._runtime_event_callback = runtime_event_callback
        self._sender = HumanInstagramSender(
            headless=headless,
            keep_browser_open_per_account=True,
        )
        self._sender_close_lock = threading.Lock()
        self._sender_closed = False
        self._worker_state = CampaignWorkerStateMachine(
            max_busy_seconds=max(20.0, self._send_flow_timeout_seconds + 10.0)
        )
        self._states: list[AccountRuntimeState] = []
        for account in _order_accounts_for_worker_start(accounts):
            if not isinstance(account, dict):
                continue
            username = str(account.get("username") or "").strip()
            if not username:
                continue
            limit = _resolve_account_message_limit(account)
            sent_today = _resolve_account_sent_today(account)
            state = AccountRuntimeState(
                account=dict(account),
                max_messages=limit,
                sent_count=sent_today,
                disabled_for_campaign=sent_today >= limit,
                session_ready=_account_has_storage_state(account),
            )
            state.account["sent_today"] = sent_today
            self._states.append(state)
        self._rotation_cursor = 0
        self._stop_event = threading.Event()
        self._proxy_status_cache = "healthy" if self._is_local_worker else self._health.proxy_status(self.proxy_id)
        self._last_selected_account = ""

    def _log(self, level: str, message: str, *args: Any, exc_info: bool = False) -> None:
        log_method = getattr(logger, level)
        if level == "exception" and not exc_info:
            exc_info = True
        log_method(
            f"[run_id=%s worker=%s proxy=%s] {message}",
            self._campaign_run_id or "-",
            self.worker_id,
            self.proxy_id,
            *args,
            exc_info=exc_info,
        )

    def _emit_runtime_event(
        self,
        event_type: str,
        *,
        severity: str = "info",
        failure_kind: str = "",
        message: str,
        **payload: Any,
    ) -> None:
        if not callable(self._runtime_event_callback):
            return
        self._runtime_event_callback(
            {
                "run_id": self._campaign_run_id,
                "event_type": str(event_type or "").strip(),
                "severity": str(severity or "info").strip().lower() or "info",
                "failure_kind": str(failure_kind or "").strip().lower(),
                "message": str(message or "").strip(),
                "worker_id": self.worker_id,
                "proxy_id": self.proxy_id,
                **payload,
            }
        )

    def _report_storage_failure(
        self,
        *,
        event_type: str,
        message: str,
        exc: Exception,
        failure_kind: str = "system",
        **payload: Any,
    ) -> None:
        self._log("exception", "%s", message, exc_info=True)
        self._emit_runtime_event(
            event_type,
            severity="error",
            failure_kind=failure_kind,
            message=message,
            error=str(exc) or exc.__class__.__name__,
            **payload,
        )

    def request_stop(self, reason: str = "") -> None:
        if reason:
            self._log("info", "stop solicitado (%s).", reason)
        self._stop_event.set()
        self._transition_state(self._worker_state.set_stopping(reason=reason or "stop_requested"))
        self._close_sender_sessions()

    def _stop_requested(self) -> bool:
        return STOP_EVENT.is_set() or self._stop_event.is_set()

    def _wait_briefly(self, seconds: float) -> None:
        remaining = max(0.0, float(seconds))
        while remaining > 0:
            if self._stop_requested():
                return
            step = min(0.10, remaining)
            time.sleep(step)
            remaining = max(0.0, remaining - step)

    def _requeue_task_for_stop(self, task: LeadTask, *, reason: str) -> None:
        self._scheduler.push_task(task)
        self._transition_state(self._worker_state.set_stopping(reason=reason or "stop_requested"))

    def _sender_stage_callback(self, stage: str, payload: Dict[str, Any]) -> None:
        stage_name = str(stage or "").strip().lower()
        lead = str(payload.get("lead") or "").strip()
        account = str(payload.get("account") or "").strip()
        reason = str(payload.get("reason") or stage_name or "").strip()
        if stage_name == "opening_dm":
            self._transition_state(
                self._worker_state.set_opening_dm(
                    lead=lead,
                    account=account,
                    reason=reason or "open_outbound_dm",
                )
            )
            return
        if stage_name == "sending":
            self._transition_state(
                self._worker_state.set_sending(
                    lead=lead,
                    account=account,
                    reason=reason or "send_message",
                )
            )
            return
        self._heartbeat(sent=False)

    def _transition_state(self, snapshot: WorkerStateSnapshot) -> WorkerStateSnapshot:
        self._scheduler.update_worker_activity(
            self.worker_id,
            sent=False,
            proxy_id=self.proxy_id,
            execution_state=snapshot.state,
            execution_stage=snapshot.stage,
            lead=snapshot.lead,
            account=snapshot.account,
            reason=snapshot.reason,
        )
        return snapshot

    def _heartbeat(self, *, sent: bool = False) -> None:
        snapshot = self._worker_state.snapshot()
        self._scheduler.update_worker_activity(
            self.worker_id,
            sent=sent,
            proxy_id=self.proxy_id,
            execution_state=snapshot.state,
            execution_stage=snapshot.stage,
            lead=snapshot.lead,
            account=snapshot.account,
            reason=snapshot.reason,
        )

    def _proxy_status(self, *, now: Optional[float] = None) -> str:
        if self._is_local_worker:
            return "healthy"
        return self._health.proxy_status(self.proxy_id, now=now)

    def _record_health_success(self, username: str, response_time: float) -> None:
        if self._is_local_worker:
            self._health.record_account_success(username, response_time)
            return
        self._health.record_send_success(self.proxy_id, username, response_time)

    def _record_health_failure(
        self,
        username: str,
        reason: str,
        *,
        is_login_error: bool,
        response_time: float,
    ) -> None:
        if self._is_local_worker:
            self._health.record_account_error(
                username,
                reason,
                is_login_error=is_login_error,
                response_time=response_time,
            )
            return
        if is_login_error:
            self._health.record_login_error(
                self.proxy_id,
                username,
                reason,
                response_time=response_time,
            )
            return
        self._health.record_send_error(
            self.proxy_id,
            username,
            reason,
            response_time=response_time,
        )

    def busy_age(self, now: Optional[float] = None) -> float:
        return self._worker_state.busy_age(now=now)

    def is_busy(self, now: Optional[float] = None) -> bool:
        return self._worker_state.is_busy(now=now)

    def execution_state(self) -> WorkerExecutionState:
        return self._worker_state.execution_state()

    def execution_stage(self) -> WorkerExecutionStage:
        return self._worker_state.execution_stage()

    def has_schedulable_accounts(self, now: Optional[float] = None) -> bool:
        # "Schedulable" means at least one account can still run in this worker.
        # It intentionally ignores next_send_time to avoid false idle/restart loops
        # while accounts are waiting their configured delay window.
        ts = time.time() if now is None else float(now)
        for state in self._states:
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                state.disabled_for_campaign = True
                continue
            username = str(state.account.get("username") or "").strip()
            if state.cooldown_until > ts:
                continue
            if not self._health.is_account_available(username, now=ts):
                remaining = self._health.account_cooldown_remaining(username, now=ts)
                state.cooldown_until = ts + remaining
                continue
            return True
        return False

    def run(self) -> None:
        self._log("info", "iniciado")
        self._log("info", "usando %d cuentas", len(self._states))
        self._transition_state(self._worker_state.set_idle(reason="worker_start"))
        try:
            while not self._stop_requested():
                proxy_status = self._proxy_status()
                self._log_proxy_status_change(proxy_status)
                if proxy_status == "blocked":
                    task = self._scheduler.pop_task_for_proxy(self.proxy_id)
                    if task is not None:
                        self._transition_state(self._worker_state.set_blocked_proxy(reason="proxy_blocked"))
                        self._handle_blocked_proxy_task(task)
                        self._heartbeat(sent=False)
                        continue
                    if self._scheduler.is_empty():
                        break
                    self._transition_state(self._worker_state.set_blocked_proxy(reason="proxy_blocked"))
                    self._heartbeat(sent=False)
                    self._wait_briefly(0.35)
                    continue

                task = self._scheduler.pop_task_for_proxy(self.proxy_id)
                if task is None:
                    if self._scheduler.is_empty():
                        break
                    self._transition_state(self._worker_state.set_waiting_queue(reason="queue_poll"))
                    self._heartbeat(sent=False)
                    self._wait_briefly(0.20)
                    continue

                sent = self._process_task(task)
                self._heartbeat(sent=sent)
        except Exception:
            self._log("exception", "worker crash", exc_info=True)
            self._emit_runtime_event(
                "worker_crashed",
                severity="error",
                failure_kind="system",
                message="Worker crasheo durante la ejecucion.",
            )
            raise
        finally:
            self._close_sender_sessions()

    def _process_task(self, task: LeadTask) -> bool:
        self._transition_state(self._worker_state.set_waiting_account(lead=task.lead, reason="select_account"))
        try:
            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_before_account_selection")
                return False

            account_state = self._next_ready_account(task)
            if account_state is None:
                wait_decision = self._next_account_wait_decision(task)
                if wait_decision is not None:
                    self._scheduler.push_task(task)
                    sleep_for = min(30.0, max(0.15, float(wait_decision.seconds)))
                    self._sleep_with_heartbeat(
                        sleep_for,
                        stage=wait_decision.stage,
                        reason=wait_decision.reason,
                    )
                    return False
                self._handle_no_account_available(task)
                return False

            account = account_state.account
            username = str(account.get("username") or "").strip()
            if not username:
                account_state.disabled_for_campaign = True
                self._mark_task_failed(task, reason="account_missing_username")
                return False

            self._log("info", "LeadQueue: %d", self._scheduler.queue_size())
            self._log("info", "[QUEUE] worker picked lead: %s", task.lead)
            self._log("info", "Lead tomado @%s con @%s", task.lead, username)
            if self._last_selected_account and self._last_selected_account != username:
                _print_info_block(
                    "RotaciÃ³n de cuenta",
                    [f"Siguiente cuenta seleccionada: {username}"],
                )
            self._last_selected_account = username

            self._transition_state(
                self._worker_state.set_opening_session(
                    lead=task.lead,
                    account=username,
                    reason="ensure_session",
                )
            )
            if not self._ensure_session(account_state):
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_session_open")
                    return False
                self._handle_failure(
                    task=task,
                    account_state=account_state,
                    reason="login_failed",
                    is_login_error=True,
                    response_time=0.0,
                )
                return False

            message = self._render_message_for_lead(account_state.account, task.lead)
            if not message:
                self._mark_task_failed(task, reason="template_empty")
                return False

            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_before_send")
                return False

            self._log("info", "Abriendo DM a @%s con @%s", task.lead, username)
            self._transition_state(
                self._worker_state.set_opening_dm(
                    lead=task.lead,
                    account=username,
                    reason="open_outbound_dm",
                )
            )
            self._log("info", "Enviando primer mensaje a @%s con @%s", task.lead, username)
            started = time.time()
            try:
                send_result = self._sender.send_message_like_human_sync(
                    account=account_state.account,
                    target_username=task.lead,
                    text=message,
                    base_delay_seconds=0.0,
                    jitter_seconds=0.0,
                    return_detail=True,
                    return_payload=True,
                    flow_timeout_seconds=self._send_flow_timeout_seconds,
                    stage_callback=self._sender_stage_callback,
                )
                parsed_result = CampaignSendResult.from_sender_result(send_result)
            except PlaywrightRuntimeCancelledError:
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_send")
                    return False
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail="send_cancelled",
                    payload={"reason_code": "SEND_CANCELLED"},
                )
            except PlaywrightRuntimeTimeoutError:
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail="send_deadline_exceeded",
                    payload={"reason_code": "FLOW_TIMEOUT"},
                )
            except Exception as exc:
                if self._stop_requested():
                    self._requeue_task_for_stop(task, reason="stop_during_send")
                    return False
                parsed_result = CampaignSendResult(
                    ok=False,
                    detail=str(exc),
                    payload={},
                )
            elapsed = max(0.0, time.time() - started)

            if parsed_result.ok:
                self._handle_success(
                    task,
                    account_state,
                    detail=parsed_result.detail or "ok",
                    response_time=elapsed,
                )
                return True

            if self._stop_requested():
                self._requeue_task_for_stop(task, reason="stop_after_send_attempt")
                return False

            reason = _campaign_failure_reason(parsed_result)
            self._handle_failure(
                task=task,
                account_state=account_state,
                reason=reason,
                is_login_error=False,
                response_time=elapsed,
            )
            return False
        finally:
            if self._stop_requested():
                self._transition_state(self._worker_state.set_stopping(reason="stop_requested"))
            else:
                self._transition_state(self._worker_state.set_idle(reason="task_complete"))

    def _handle_success(
        self,
        task: LeadTask,
        account_state: AccountRuntimeState,
        *,
        detail: str,
        response_time: float,
    ) -> None:
        username = str(account_state.account.get("username") or "").strip()
        account_state.sent_count += 1
        account_state.account["sent_today"] = account_state.sent_count
        account_state.fail_count = 0
        account_state.cooldown_until = 0.0
        account_state.next_send_time = time.time() + random.uniform(self.delay_min, self.delay_max)
        if account_state.sent_count >= account_state.max_messages:
            account_state.disabled_for_campaign = True

        self._record_health_success(username, response_time)
        self._log_proxy_status_change(self._proxy_status())
        mark_connected(username, True)

        with self._stats_lock:
            self._stats["sent"] = int(self._stats.get("sent", 0)) + 1

        try:
            log_sent(
                username,
                task.lead,
                True,
                detail,
                verified=True,
                duration_ms=int(max(0.0, response_time) * 1000),
                source_engine="campaign",
                campaign_alias=self._campaign_alias,
                leads_alias=self._leads_alias,
                run_id=self._campaign_run_id,
            )
        except Exception as exc:
            self._report_storage_failure(
                event_type="sent_log_write_failed",
                message="No se pudo persistir sent_log para un envio confirmado.",
                exc=exc,
                account=username,
                lead=task.lead,
            )
        try:
            mark_lead_sent(task.lead, sent_by=username, alias=self._campaign_alias)
        except Exception as exc:
            self._report_storage_failure(
                event_type="lead_status_write_failed",
                message="No se pudo persistir lead_status sent para un envio confirmado.",
                exc=exc,
                account=username,
                lead=task.lead,
            )

        delay_left = max(0.0, account_state.next_send_time - time.time())
        delay_applied_seconds = max(0, int(round(delay_left)))
        self._log("info", "Enviado @%s -> @%s (%s)", username, task.lead, detail)
        _print_send_block(
            account=username,
            lead=task.lead,
            delay_seconds=delay_applied_seconds,
            proxy_id=self.proxy_id,
        )

    def _handle_failure(
        self,
        *,
        task: LeadTask,
        account_state: AccountRuntimeState,
        reason: str,
        is_login_error: bool,
        response_time: float,
    ) -> None:
        username = str(account_state.account.get("username") or "").strip()
        reason_text = str(reason or "send_failed").strip() or "send_failed"
        reason_upper = self._normalize_failure_reason(reason_text)
        if reason_upper == "ACCOUNT_QUOTA_REACHED":
            account_state.sent_count = max(account_state.sent_count, account_state.max_messages)
            account_state.account["sent_today"] = account_state.sent_count
            account_state.disabled_for_campaign = True
            self._handle_no_account_available(task)
            return
        if self._try_transient_same_proxy_retry(
            task=task,
            account_state=account_state,
            reason_upper=reason_upper,
            reason_text=reason_text,
        ):
            return

        if self._is_non_retryable_lead_failure(reason_upper):
            account_state.fail_count = 0
            account_state.cooldown_until = 0.0
            account_state.next_send_time = time.time() + 0.25
            self._log("info", "Lead @%s descartado sin retry (%s) usando @%s.", task.lead, reason_text, username)
            self._mark_task_failed(task, reason=reason_text, account_username=username)
            return

        account_state.fail_count += 1

        if is_login_error:
            mark_connected(username, False)
        elif username:
            # Un error de envio no implica sesion rota; evitar desconectar cuentas sanas.
            mark_connected(username, True)

        if is_login_error:
            self._record_health_failure(
                username,
                reason_text,
                is_login_error=True,
                response_time=response_time,
            )
        else:
            self._record_health_failure(
                username,
                reason_text,
                is_login_error=False,
                response_time=response_time,
            )

        if account_state.fail_count >= self._cooldown_fail_threshold:
            cooldown_until = self._health.set_account_cooldown(username, reason=reason_text)
            account_state.cooldown_until = cooldown_until
            account_state.fail_count = 0
            cooldown_seconds = max(0, int(cooldown_until - time.time()))
            self._log("warning", "Account cooldown: @%s en cooldown por %ss.", username, cooldown_seconds)
            _print_info_block(
                "Cuenta en cooldown",
                [
                    f"Cuenta: {username}",
                    f"Cooldown restante: {cooldown_seconds}s",
                ],
            )

        proxy_status = self._proxy_status()
        self._log_proxy_status_change(proxy_status)

        same_proxy_accounts = [
            str(state.account.get("username") or "")
            for state in self._states
            if isinstance(state.account, dict)
        ]
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.proxy_id,
            failed_account_id=username,
            same_proxy_account_ids=same_proxy_accounts,
            all_proxy_ids=self._all_proxy_ids,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    f"Motivo: {self._humanize_reason(reason_text)}",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return

        self._mark_task_failed(task, reason=reason_text)

    def _try_transient_same_proxy_retry(
        self,
        *,
        task: LeadTask,
        account_state: AccountRuntimeState,
        reason_upper: str,
        reason_text: str,
    ) -> bool:
        if not self._is_transient_same_proxy_retry_reason(reason_upper):
            return False
        # Retry once on the same proxy/account to absorb startup race conditions.
        if task.attempt >= 2:
            return False

        retry_task = LeadTask(
            lead=task.lead,
            attempt=task.attempt + 1,
            preferred_proxy_id=self.proxy_id,
            excluded_accounts=tuple(),
            history=task.history + (f"{self.proxy_id}:{reason_upper}",),
        )
        account_state.fail_count = 0
        account_state.cooldown_until = 0.0
        account_state.next_send_time = time.time() + 2.0
        self._scheduler.push_task(retry_task)
        with self._stats_lock:
            self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
        self._log(
            "info",
            "Retry transient: lead=@%s intento=%d reason=%s",
            retry_task.lead,
            retry_task.attempt,
            reason_text,
        )
        _print_info_block(
            "Lead reencolado",
            [
                f"Lead: {retry_task.lead}",
                f"Motivo: {self._humanize_reason(reason_text)}",
                f"Intento: {retry_task.attempt}",
            ],
        )
        return True

    def _sleep_with_heartbeat(
        self,
        seconds: float,
        *,
        stage: WorkerExecutionStage,
        reason: str,
    ) -> None:
        remaining = max(0.0, float(seconds))
        if self.execution_state() != WorkerExecutionState.STOPPING:
            if stage == WorkerExecutionStage.COOLDOWN:
                self._transition_state(self._worker_state.set_cooldown(reason=reason))
            else:
                self._transition_state(self._worker_state.set_waiting_queue(reason=reason))
        while remaining > 0:
            if self._stop_requested():
                return
            step = min(0.5, remaining)
            time.sleep(step)
            remaining = max(0.0, remaining - step)
            self._heartbeat(sent=False)

    def _close_sender_sessions(self) -> None:
        with self._sender_close_lock:
            if self._sender_closed:
                return
            self._sender_closed = True
        try:
            self._sender.close_all_sessions_sync(timeout=2.0)
        except Exception as exc:
            self._report_storage_failure(
                event_type="sender_close_failed",
                message="No se pudieron cerrar las sesiones del sender.",
                exc=exc,
            )

    @staticmethod
    def _normalize_failure_reason(reason: str) -> str:
        return str(reason or "").strip().upper()

    @staticmethod
    def _is_non_retryable_lead_failure(reason: str) -> bool:
        reason_upper = str(reason or "").strip().upper()
        if not reason_upper:
            return False
        if reason_upper.startswith("SKIPPED_") and reason_upper not in {"SKIPPED_UI_NOT_FOUND"}:
            return True
        terminal = {
            "SKIPPED_USERNAME_NOT_FOUND",
            "SKIPPED_NO_DM_OR_THREAD_BLOCKED",
            "SEND_UNVERIFIED_BLOCKED",
            "SENT_UNVERIFIED",
            "THREAD_OPEN_FAILED",
            "USERNAME_NOT_FOUND",
        }
        if reason_upper in terminal:
            return True
        return any(
            token in reason_upper
            for token in (
                "USERNAME_NOT_FOUND",
                "NO_RESULTS_FOUND",
            )
        )

    @staticmethod
    def _is_transient_same_proxy_retry_reason(reason: str) -> bool:
        reason_upper = str(reason or "").strip().upper()
        if not reason_upper:
            return False
        return "INBOX_NOT_READY" in reason_upper or "UI_NOT_FOUND" in reason_upper

    @staticmethod
    def _humanize_reason(reason: str) -> str:
        key = str(reason or "").strip()
        normalized = key.upper()
        mapping = {
            "INBOX_NOT_READY": "inbox no disponible todavÃ­a",
            "LOGIN_FAILED": "fallÃ³ la sesiÃ³n de la cuenta",
            "NO_ACCOUNT_AVAILABLE": "no habÃ­a cuentas disponibles",
            "PROXY_BLOCKED": "proxy bloqueado",
            "THREAD_OPEN_FAILED": "no se pudo abrir la conversaciÃ³n",
            "USERNAME_NOT_FOUND": "usuario no encontrado",
            "UI_NOT_FOUND": "la interfaz no devolviÃ³ resultados",
            "SKIPPED_NO_DM_OR_THREAD_BLOCKED": "conversaciÃ³n existente o no admite DM",
            "SKIPPED_USERNAME_NOT_FOUND": "usuario no encontrado",
            "SKIPPED_UI_NOT_FOUND": "no se encontraron resultados en bÃºsqueda",
            "SKIPPED_CAMPAIGN_QUOTA_REACHED": "se alcanzÃ³ el cupo de mensajes de la cuenta",
            "SEND_UNVERIFIED_BLOCKED": "mensaje no confirmado por la plataforma",
            "SENT_UNVERIFIED": "mensaje no confirmado por la plataforma",
        }
        return mapping.get(normalized, key.replace("_", " ").strip().lower() or "error de envÃ­o")

    def _mark_task_failed(
        self,
        task: LeadTask,
        *,
        reason: str,
        account_username: str = "",
        force_skip: bool = False,
    ) -> None:
        reason_text = str(reason or "send_failed").strip() or "send_failed"
        reason_upper = self._normalize_failure_reason(reason_text)
        skip_lead = bool(force_skip) or self._is_non_retryable_lead_failure(reason_upper)

        with self._stats_lock:
            if skip_lead:
                self._stats["skipped"] = int(self._stats.get("skipped", 0)) + 1
            else:
                self._stats["failed"] = int(self._stats.get("failed", 0)) + 1

        self._log("warning", "Lead @%s marcado como fallido (%s).", task.lead, reason_text)

        try:
            if skip_lead:
                log_sent(
                    account_username or "-",
                    task.lead,
                    False,
                    reason_text,
                    skip=True,
                    skip_reason=reason_text,
                    source_engine="campaign",
                    campaign_alias=self._campaign_alias,
                    leads_alias=self._leads_alias,
                    run_id=self._campaign_run_id,
                )
                mark_lead_skipped(task.lead, reason=reason_text, alias=self._campaign_alias)
            else:
                log_sent(
                    account_username or "-",
                    task.lead,
                    False,
                    reason_text,
                    source_engine="campaign",
                    campaign_alias=self._campaign_alias,
                    leads_alias=self._leads_alias,
                    run_id=self._campaign_run_id,
                )
                mark_lead_failed(
                    task.lead,
                    reason=reason_text,
                    attempts=task.attempt,
                    alias=self._campaign_alias,
                )
        except Exception as exc:
            self._report_storage_failure(
                event_type="lead_failure_persist_failed",
                message="No se pudo persistir el resultado fallido del lead.",
                exc=exc,
                account=account_username or "-",
                lead=task.lead,
                failure_kind="terminal" if skip_lead else "retryable",
                reason=reason_text,
            )

        account_display = account_username or "-"
        if skip_lead:
            _print_skip_block(
                account=account_display,
                lead=task.lead,
                reason=self._humanize_reason(reason_text),
                proxy_id=self.proxy_id,
            )
            return
        _print_error_block(
            account=account_display,
            lead=task.lead,
            reason=self._humanize_reason(reason_text),
            proxy_id=self.proxy_id,
        )

    def _handle_no_account_available(self, task: LeadTask) -> None:
        if self._all_accounts_reached_limit():
            self._mark_task_failed(
                task,
                reason="SKIPPED_CAMPAIGN_QUOTA_REACHED",
                force_skip=True,
            )
            return
        same_proxy_accounts = [
            str(state.account.get("username") or "")
            for state in self._states
            if isinstance(state.account, dict)
        ]
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.proxy_id,
            failed_account_id="",
            same_proxy_account_ids=same_proxy_accounts,
            all_proxy_ids=self._all_proxy_ids,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s (sin cuenta disponible)",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    "Motivo: no habÃ­a cuentas disponibles",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return
        self._mark_task_failed(task, reason="no_account_available")

    def _all_accounts_reached_limit(self) -> bool:
        if not self._states:
            return False
        for state in self._states:
            if not self._account_reached_limit(state):
                return False
            state.disabled_for_campaign = True
        return True

    def _handle_blocked_proxy_task(self, task: LeadTask) -> None:
        retry_task = self._scheduler.build_retry_task(
            task,
            failed_proxy_id=self.proxy_id,
            failed_account_id="",
            same_proxy_account_ids=[],
            all_proxy_ids=self._all_proxy_ids,
        )
        if retry_task is not None:
            self._scheduler.push_task(retry_task)
            with self._stats_lock:
                self._stats["retried"] = int(self._stats.get("retried", 0)) + 1
            self._log(
                "info",
                "Retry attempt: lead=@%s intento=%d proxy=%s (proxy bloqueado)",
                retry_task.lead,
                retry_task.attempt,
                retry_task.preferred_proxy_id or "any",
            )
            _print_info_block(
                "Lead reencolado",
                [
                    f"Lead: {retry_task.lead}",
                    "Motivo: proxy bloqueado",
                    f"Intento: {retry_task.attempt}",
                ],
            )
            return
        self._mark_task_failed(task, reason="proxy_blocked")

    def _ensure_session(self, state: AccountRuntimeState) -> bool:
        account = state.account
        username = str(account.get("username") or "").strip()
        if not username:
            return False

        refreshed = get_account(username) or account
        state.account = dict(refreshed)
        refreshed_sent_today = _resolve_account_sent_today(state.account)
        state.sent_count = max(state.sent_count, refreshed_sent_today)
        state.account["sent_today"] = state.sent_count
        state.max_messages = _resolve_account_message_limit(state.account)
        if state.sent_count >= state.max_messages:
            state.disabled_for_campaign = True

        state.session_ready = has_playwright_storage_state(username)
        if state.session_ready:
            mark_connected(username, True)
        return True

    def _account_schedulable(
        self,
        state: AccountRuntimeState,
        *,
        ts: float,
        excluded: set[str],
        require_session_ready: bool,
        mutate: bool,
    ) -> bool:
        username = str(state.account.get("username") or "").strip()
        username_norm = _norm_account(username)
        if state.disabled_for_campaign:
            return False
        if self._account_reached_limit(state):
            if mutate:
                state.disabled_for_campaign = True
            return False
        if username_norm in excluded:
            return False
        if require_session_ready and not state.session_ready:
            return False
        if state.cooldown_until > ts:
            return False
        if not self._health.is_account_available(username, now=ts):
            remaining = self._health.account_cooldown_remaining(username, now=ts)
            if mutate:
                state.cooldown_until = ts + remaining
            return False
        if state.next_send_time > ts:
            return False
        return True

    def _next_ready_account(
        self,
        task: Optional[LeadTask],
        *,
        now: Optional[float] = None,
    ) -> Optional[AccountRuntimeState]:
        total = len(self._states)
        if total <= 0:
            return None

        ts = time.time() if now is None else float(now)
        excluded = set(task.excluded_accounts if task else ())
        for _ in range(total):
            index = self._rotation_cursor % total
            self._rotation_cursor += 1
            state = self._states[index]
            if self._account_schedulable(
                state,
                ts=ts,
                excluded=excluded,
                require_session_ready=False,
                mutate=True,
            ):
                return state
        return None

    def _next_account_wait_decision(self, task: LeadTask) -> Optional[AccountWaitDecision]:
        ts = time.time()
        excluded = set(task.excluded_accounts if task else ())
        decision: Optional[AccountWaitDecision] = None

        for state in self._states:
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                continue

            username = str(state.account.get("username") or "").strip()
            username_norm = _norm_account(username)
            if username_norm in excluded:
                continue

            if state.cooldown_until > ts:
                wait_seconds = max(0.0, state.cooldown_until - ts)
                decision = self._pick_wait_decision(
                    decision,
                    wait_seconds=wait_seconds,
                    stage=WorkerExecutionStage.COOLDOWN,
                    reason="account_cooldown",
                )
                continue

            if not self._health.is_account_available(username, now=ts):
                remaining = self._health.account_cooldown_remaining(username, now=ts)
                if remaining > 0:
                    decision = self._pick_wait_decision(
                        decision,
                        wait_seconds=float(remaining),
                        stage=WorkerExecutionStage.COOLDOWN,
                        reason="account_cooldown",
                    )
                continue

            if state.next_send_time > ts:
                wait_seconds = max(0.0, state.next_send_time - ts)
                decision = self._pick_wait_decision(
                    decision,
                    wait_seconds=wait_seconds,
                    stage=WorkerExecutionStage.COOLDOWN,
                    reason="account_rate_window",
                )
                continue

            return AccountWaitDecision(
                seconds=0.0,
                stage=WorkerExecutionStage.WAITING_QUEUE,
                reason="account_rotation",
            )

        return decision

    @staticmethod
    def _pick_wait_decision(
        current: Optional[AccountWaitDecision],
        *,
        wait_seconds: float,
        stage: WorkerExecutionStage,
        reason: str,
    ) -> AccountWaitDecision:
        candidate = AccountWaitDecision(
            seconds=max(0.0, float(wait_seconds)),
            stage=stage,
            reason=str(reason or "").strip() or stage.value,
        )
        if current is None or candidate.seconds < current.seconds:
            return candidate
        return current

    def _has_candidate_account_for_task(self, task: LeadTask) -> bool:
        ts = time.time()
        excluded = set(task.excluded_accounts if task else ())
        for state in self._states:
            if state.disabled_for_campaign:
                continue
            if self._account_reached_limit(state):
                continue
            username = str(state.account.get("username") or "").strip()
            username_norm = _norm_account(username)
            if username_norm in excluded:
                continue
            if state.cooldown_until > ts:
                continue
            if not self._health.is_account_available(username, now=ts):
                continue
            return True
        return False

    def _render_message_for_lead(self, account: Dict[str, Any], lead: str) -> str:
        selected, _ = self._template_rotator.next_variant()
        variables = {
            "nombre": lead,
            "username": lead,
            "usuario": lead,
            "lead": lead,
            "cuenta": str(account.get("username") or ""),
            "account": str(account.get("username") or ""),
        }
        rendered = render_template(selected, variables)
        # Campaign DM templates are 1 message per line.
        # Enforce single-line payload even if source text contains newlines.
        for line in str(rendered or "").splitlines():
            candidate = line.strip()
            if candidate:
                return candidate
        return ""

    def _account_reached_limit(self, state: AccountRuntimeState) -> bool:
        sent_today = _resolve_account_sent_today(state.account)
        if sent_today > state.sent_count:
            state.sent_count = sent_today
        state.account["sent_today"] = state.sent_count
        return state.sent_count >= state.max_messages

    def _log_proxy_status_change(self, new_status: str) -> None:
        current = str(new_status or "healthy").strip().lower()
        if not current:
            current = "healthy"
        if current == self._proxy_status_cache:
            return
        self._proxy_status_cache = current
        if current == "degraded":
            self._log("warning", "Proxy degraded")
            self._emit_runtime_event(
                "proxy_degraded",
                severity="warning",
                failure_kind="retryable",
                message="Proxy degradado detectado por health monitor.",
            )
            return
        if current == "blocked":
            self._log("error", "Proxy blocked")
            self._emit_runtime_event(
                "proxy_blocked",
                severity="error",
                failure_kind="terminal",
                message="Proxy bloqueado detectado por health monitor.",
            )
            return
        self._log("info", "Proxy healthy")


def _account_usernames(accounts: list[Dict[str, Any]]) -> set[str]:
    return {
        str(account.get("username") or "").strip().lstrip("@").lower()
        for account in accounts
        if isinstance(account, dict) and str(account.get("username") or "").strip()
    }


def _load_selected_accounts(alias: str) -> list[Dict[str, Any]]:
    alias_norm = str(alias or "default").strip().lower()
    selected: list[Dict[str, Any]] = []
    for account in list_all():
        if not isinstance(account, dict):
            continue
        username = str(account.get("username") or "").strip()
        if not username:
            continue
        account_alias = str(account.get("alias") or "default").strip().lower()
        if account_alias != alias_norm:
            continue
        if not bool(account.get("active")):
            continue
        try:
            is_connected = bool(
                connected_status(
                    account,
                    fast=True,
                    persist=False,
                    reason="campaign-load",
                )
            )
        except Exception:
            is_connected = bool(account.get("connected"))
        if not is_connected:
            continue
        selected.append(dict(account))
    return selected


def _apply_sent_today_counts(
    accounts: list[Dict[str, Any]],
    *,
    sent_today_counts: Dict[str, int] | None,
) -> list[Dict[str, Any]]:
    counts_today = {
        str(username or "").strip().lstrip("@").lower(): max(0, int(value or 0))
        for username, value in dict(sent_today_counts or {}).items()
        if str(username or "").strip()
    }
    for account in accounts:
        username = str(account.get("username") or "").strip().lower()
        if not username:
            continue
        account["sent_today"] = int(counts_today.get(username, 0))
    return accounts


def load_accounts(
    alias: str,
    *,
    run_id: str = "",
    sent_today_counts: Dict[str, int] | None = None,
) -> list[Dict[str, Any]]:
    _ = run_id
    alias_norm = str(alias or "default").strip().lower()
    selected = preflight_accounts_for_proxy_runtime(_load_selected_accounts(alias_norm)).get("ready_accounts") or []
    counts_today = sent_today_counts
    if counts_today is None:
        counts_today = dict(
            campaign_start_snapshot(
                _account_usernames(selected),
                campaign_alias=alias_norm,
            ).get("daily_counts")
            or {}
        )
    return _apply_sent_today_counts(selected, sent_today_counts=counts_today)


def load_leads(leads_alias: str) -> list[str]:
    raw = load_list(str(leads_alias or "").strip())
    leads: list[str] = []
    seen: set[str] = set()
    for item in raw:
        lead = normalize_contact_username(item)
        if not lead:
            continue
        if lead in seen:
            continue
        seen.add(lead)
        leads.append(lead)
    return leads


def _normalize_campaign_alias(value: Any) -> str:
    return str(value or "").strip().lower()


def _campaign_account_usernames(alias: str) -> set[str]:
    alias_norm = _normalize_campaign_alias(alias)
    usernames: set[str] = set()
    if not alias_norm:
        return usernames
    for account in list_all():
        if not isinstance(account, dict):
            continue
        account_alias = _normalize_campaign_alias(account.get("alias"))
        if account_alias != alias_norm:
            continue
        username = _norm_account(str(account.get("username") or ""))
        if username:
            usernames.add(username)
    return usernames


def _legacy_terminal_status_matches_alias(
    *,
    alias: str,
    alias_accounts: set[str],
    entry: Dict[str, Any] | None,
) -> bool:
    if not isinstance(entry, dict):
        return False
    status = str(entry.get("status") or "").strip().lower()
    if status not in {"sent", "skipped"}:
        return False
    reason = str(entry.get("last_error") or "").strip().lower()
    if status == "skipped" and reason == "already_contacted":
        return False
    entry_alias = _normalize_campaign_alias(entry.get("last_alias"))
    if entry_alias:
        return entry_alias == _normalize_campaign_alias(alias)
    if status != "sent":
        return False
    sent_by = _norm_account(str(entry.get("sent_by") or ""))
    return bool(sent_by and sent_by in alias_accounts)


def _collect_legacy_terminal_status_update(
    lead: str,
    *,
    entry: Dict[str, Any],
    sent_updates: list[tuple[str, str]],
    skipped_updates: list[tuple[str, str]],
) -> None:
    status = str(entry.get("status") or "").strip().lower()
    if status == "sent":
        sent_updates.append((lead, str(entry.get("sent_by") or "").strip()))
        return
    if status != "skipped":
        return
    reason = str(entry.get("last_error") or "").strip()
    if reason.lower() == "already_contacted":
        return
    skipped_updates.append((lead, reason))


def _filter_pending_leads_for_campaign(
    leads: list[str],
    *,
    alias: str,
    alias_accounts: set[str] | None = None,
    campaign_registry: set[str] | None = None,
    shared_registry: set[str] | None = None,
    alias_terminal_leads: set[str] | None = None,
    legacy_status_map: Dict[str, Dict[str, Any]] | None = None,
) -> tuple[list[str], Dict[str, int]]:
    alias_norm = _normalize_campaign_alias(alias)
    resolved_alias_accounts = set(alias_accounts or _campaign_account_usernames(alias_norm))
    storage_snapshot = None
    if campaign_registry is None or shared_registry is None:
        storage_snapshot = campaign_start_snapshot(
            resolved_alias_accounts,
            campaign_alias=alias_norm,
        )
    blocked_by_campaign_registry_set = set(
        campaign_registry
        if campaign_registry is not None
        else (storage_snapshot.get("campaign_registry") if storage_snapshot is not None else set())
    )
    shared_registry_set = set(
        shared_registry
        if shared_registry is not None
        else (storage_snapshot.get("shared_registry") if storage_snapshot is not None else set())
    )
    if alias_terminal_leads is None or legacy_status_map is None:
        status_terminal_leads, status_legacy_map = get_prefilter_snapshot(alias_norm)
        if alias_terminal_leads is None:
            alias_terminal_leads = status_terminal_leads
        if legacy_status_map is None:
            legacy_status_map = status_legacy_map
    scoped_terminal_leads = set(alias_terminal_leads or ())
    resolved_legacy_status_map = dict(legacy_status_map or {})
    pending: list[str] = []
    skipped_duplicates = 0
    skipped_already_sent = 0
    blocked_by_alias_status = 0
    blocked_by_campaign_registry = 0
    blocked_by_legacy_campaign_status = 0
    advisory_shared_registry_hits = 0
    advisory_legacy_status_ignored = 0
    legacy_sent_updates: list[tuple[str, str]] = []
    legacy_skipped_updates: list[tuple[str, str]] = []
    seen: set[str] = set()

    for lead in leads:
        normalized = normalize_contact_username(lead)
        if not normalized:
            continue
        if normalized in seen:
            skipped_duplicates += 1
            continue
        seen.add(normalized)

        blocked_by_scoped_status = normalized in scoped_terminal_leads
        blocked_by_status = blocked_by_scoped_status
        blocked_by_registry = normalized in blocked_by_campaign_registry_set
        legacy_entry = resolved_legacy_status_map.get(normalized)
        blocked_by_legacy = _legacy_terminal_status_matches_alias(
            alias=alias_norm,
            alias_accounts=resolved_alias_accounts,
            entry=legacy_entry,
        )

        if blocked_by_legacy and not blocked_by_status and isinstance(legacy_entry, dict):
            _collect_legacy_terminal_status_update(
                normalized,
                entry=legacy_entry,
                sent_updates=legacy_sent_updates,
                skipped_updates=legacy_skipped_updates,
            )
            scoped_terminal_leads.add(normalized)
            blocked_by_status = True

        if blocked_by_status or blocked_by_registry or blocked_by_legacy:
            skipped_already_sent += 1
            if blocked_by_scoped_status:
                blocked_by_alias_status += 1
            if blocked_by_registry:
                blocked_by_campaign_registry += 1
            if blocked_by_legacy:
                blocked_by_legacy_campaign_status += 1
            continue

        if normalized in shared_registry_set:
            advisory_shared_registry_hits += 1
        if isinstance(legacy_entry, dict):
            advisory_legacy_status_ignored += 1

        pending.append(normalized)

    if legacy_sent_updates or legacy_skipped_updates:
        try:
            apply_terminal_status_updates(
                alias=alias_norm,
                sent_updates=legacy_sent_updates,
                skipped_updates=legacy_skipped_updates,
            )
        except Exception:
            logger.exception(
                "[campaign_alias=%s] No se pudieron migrar estados terminales legacy a lead_status.",
                alias_norm or "-",
            )

    return pending, {
        "skipped_duplicates": skipped_duplicates,
        "skipped_already_sent": skipped_already_sent,
        "pending": len(pending),
        "blocked_total": skipped_already_sent,
        "valid_total": len(pending),
        "blocked_by_alias_status": blocked_by_alias_status,
        "blocked_by_campaign_registry": blocked_by_campaign_registry,
        "blocked_by_legacy_campaign_status": blocked_by_legacy_campaign_status,
        "advisory_shared_registry_hits": advisory_shared_registry_hits,
        "advisory_legacy_status_ignored": advisory_legacy_status_ignored,
        "blocked_by_campaign_history": 0,
    }


def _log_campaign_diagnostics(
    *,
    alias: str,
    leads_alias: str,
    total_leads_loaded: int,
    lead_filter_stats: Dict[str, int],
    log_callback: Callable[..., None] | None = None,
) -> None:
    blocked_total = max(0, int(lead_filter_stats.get("blocked_total", lead_filter_stats.get("skipped_already_sent", 0))))
    valid_total = max(0, int(lead_filter_stats.get("valid_total", lead_filter_stats.get("pending", 0))))
    duplicates = max(0, int(lead_filter_stats.get("skipped_duplicates", 0)))
    blocked_alias_status = max(0, int(lead_filter_stats.get("blocked_by_alias_status", 0)))
    blocked_campaign_registry = max(0, int(lead_filter_stats.get("blocked_by_campaign_registry", 0)))
    blocked_legacy_campaign_status = max(0, int(lead_filter_stats.get("blocked_by_legacy_campaign_status", 0)))
    advisory_shared_registry_hits = max(0, int(lead_filter_stats.get("advisory_shared_registry_hits", 0)))
    advisory_legacy_status_ignored = max(0, int(lead_filter_stats.get("advisory_legacy_status_ignored", 0)))
    blocked_history = max(0, int(lead_filter_stats.get("blocked_by_campaign_history", 0)))

    if callable(log_callback):
        log_callback(
            "info",
            "Campaign diagnostics | alias=%s leads_alias=%s total=%d blocked=%d valid=%d duplicates=%d "
            "alias_status=%d campaign_registry=%d legacy_campaign_status=%d shared_registry_ignored=%d "
            "legacy_status_ignored=%d campaign_history=%d",
            alias,
            leads_alias,
            total_leads_loaded,
            blocked_total,
            valid_total,
            duplicates,
            blocked_alias_status,
            blocked_campaign_registry,
            blocked_legacy_campaign_status,
            advisory_shared_registry_hits,
            advisory_legacy_status_ignored,
            blocked_history,
        )
    else:
        logger.info(
            "Campaign diagnostics | alias=%s leads_alias=%s total=%d blocked=%d valid=%d duplicates=%d "
            "alias_status=%d campaign_registry=%d legacy_campaign_status=%d shared_registry_ignored=%d "
            "legacy_status_ignored=%d campaign_history=%d",
            alias,
            leads_alias,
            total_leads_loaded,
            blocked_total,
            valid_total,
            duplicates,
            blocked_alias_status,
            blocked_campaign_registry,
            blocked_legacy_campaign_status,
            advisory_shared_registry_hits,
            advisory_legacy_status_ignored,
            blocked_history,
        )
    _print_info_block(
        "Campaign diagnostics",
        [
            f"alias: {alias}",
            f"leads alias: {leads_alias}",
            f"total leads loaded: {max(0, int(total_leads_loaded))}",
            f"duplicates ignored: {duplicates}",
            f"blocked leads: {blocked_total}",
            f"valid leads: {valid_total}",
            "source of block:",
            "note: source counts may overlap",
            f"campaign alias lead status: {blocked_alias_status}",
            f"campaign sent registry: {blocked_campaign_registry}",
            f"legacy campaign status migrated: {blocked_legacy_campaign_status}",
            f"campaign history: {blocked_history}",
            f"shared sent_log advisory only: {advisory_shared_registry_hits}",
            f"legacy global lead status ignored: {advisory_legacy_status_ignored}",
        ],
    )


def _explicit_worker_proxy_id(account: Dict[str, Any]) -> str:
    assigned_proxy_id = str(account.get("assigned_proxy_id") or "").strip()
    if assigned_proxy_id:
        return assigned_proxy_id
    proxy_payload = proxy_from_account(account) or {}
    proxy_server = str(proxy_payload.get("server") or "").strip()
    return proxy_server


def _account_remaining_capacity(account: Dict[str, Any]) -> int:
    limit = _resolve_account_message_limit(account)
    sent_today = _resolve_account_sent_today(account)
    return max(0, int(limit) - int(sent_today))


def _group_remaining_capacity(accounts: list[Dict[str, Any]]) -> int:
    total = 0
    for account in accounts:
        if not isinstance(account, dict):
            continue
        total += _account_remaining_capacity(account)
    return max(0, total)


def _total_remaining_capacity_for_groups(
    group_capacities: Dict[str, int],
    worker_ids: list[str],
) -> int:
    total = 0
    for worker_id in worker_ids:
        total += max(0, int(group_capacities.get(worker_id, 0)))
    return max(0, total)


def _limit_leads_to_worker_capacity(
    leads: list[str],
    *,
    group_capacities: Dict[str, int],
    worker_ids: list[str],
) -> tuple[list[str], int]:
    total_capacity = _total_remaining_capacity_for_groups(group_capacities, worker_ids)
    if total_capacity <= 0:
        return [], len(leads)
    limited = list(leads[:total_capacity])
    return limited, max(0, len(leads) - len(limited))


def _build_initial_worker_tasks(
    leads: list[str],
    *,
    worker_ids: list[str],
    group_capacities: Dict[str, int],
) -> list[LeadTask]:
    remaining_slots = {
        worker_id: max(0, int(group_capacities.get(worker_id, 0)))
        for worker_id in worker_ids
    }
    active_workers = [
        worker_id
        for worker_id in worker_ids
        if remaining_slots.get(worker_id, 0) > 0
    ]
    tasks: list[LeadTask] = []
    cursor = 0

    for lead in leads:
        if not active_workers:
            break
        index = cursor % len(active_workers)
        worker_id = active_workers[index]
        tasks.append(
            LeadTask(
                lead=str(lead or "").strip().lstrip("@"),
                attempt=1,
                preferred_proxy_id=worker_id,
            )
        )
        remaining_slots[worker_id] = max(0, remaining_slots[worker_id] - 1)
        if remaining_slots[worker_id] <= 0:
            active_workers.pop(index)
            if active_workers:
                cursor = index % len(active_workers)
            continue
        cursor = index + 1

    return tasks


def _validate_initial_worker_tasks(
    tasks: list[LeadTask],
    *,
    log_callback: Callable[..., None] | None = None,
) -> list[LeadTask]:
    validated: list[LeadTask] = []
    invalid = 0
    for task in tasks:
        if not isinstance(task, LeadTask):
            invalid += 1
            continue
        normalized = normalize_contact_username(task.lead)
        if not normalized:
            invalid += 1
            continue
        validated.append(
            LeadTask(
                lead=normalized,
                attempt=max(1, int(task.attempt or 1)),
                preferred_proxy_id=task.preferred_proxy_id,
                excluded_accounts=tuple(str(item or "").strip() for item in task.excluded_accounts if str(item or "").strip()),
                history=tuple(str(item or "").strip() for item in task.history if str(item or "").strip()),
            )
        )
    if invalid > 0:
        if callable(log_callback):
            log_callback("warning", "[QUEUE] invalid leads dropped before worker start: %d", invalid)
        else:
            logger.warning("[QUEUE] invalid leads dropped before worker start: %d", invalid)
    if callable(log_callback):
        log_callback("info", "[QUEUE] total leads enqueued: %d", len(validated))
    else:
        logger.info("[QUEUE] total leads enqueued: %d", len(validated))
    return validated


def calculate_workers(accounts: list[Dict[str, Any]]) -> Dict[str, Any]:
    proxy_groups = {
        proxy_id: _order_accounts_for_worker_start(grouped_accounts)
        for proxy_id, grouped_accounts in _group_accounts_by_proxy(accounts).items()
        if grouped_accounts
    }
    group_capacities = {
        proxy_id: _group_remaining_capacity(grouped_accounts)
        for proxy_id, grouped_accounts in proxy_groups.items()
    }
    ranked = sorted(
        [
            (proxy_id, items)
            for proxy_id, items in proxy_groups.items()
            if items and int(group_capacities.get(proxy_id, 0)) > 0
        ],
        key=lambda item: (
            -sum(1 for account in item[1] if _account_has_storage_state(account)),
            -int(group_capacities.get(item[0], 0)),
            -len(item[1]),
            str(item[0] or ""),
        ),
    )
    ordered_worker_ids = [proxy_id for proxy_id, _items in ranked]
    proxy_ids = [proxy_id for proxy_id in ordered_worker_ids if not _is_local_proxy_id(proxy_id)]
    has_none_accounts = any(_is_local_proxy_id(proxy_id) for proxy_id in ordered_worker_ids)

    return {
        "proxy_groups": proxy_groups,
        "group_capacities": group_capacities,
        "proxies": proxy_ids,
        "has_none_accounts": has_none_accounts,
        "workers_capacity": len(ordered_worker_ids),
        "ordered_worker_ids": ordered_worker_ids,
    }


def calculate_workers_for_alias(alias: str) -> Dict[str, Any]:
    accounts = load_accounts(alias)
    capacity = calculate_workers(accounts)
    capacity["accounts"] = accounts
    return capacity


def run_dynamic_campaign(
    config: MutableMapping[str, Any],
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    alias = str(config.get("alias") or "default").strip() or "default"
    leads_alias = str(config.get("leads_alias") or alias).strip() or alias
    run_id = str(config.get("run_id") or "").strip() or datetime.now().strftime("campaign-%Y%m%d%H%M%S%f")
    delay_min = _as_int(config.get("delay_min", 10), default=10, minimum=0)
    delay_max = _as_int(config.get("delay_max", max(delay_min, 20)), default=max(delay_min, 20), minimum=delay_min)
    workers_requested = _as_int(config.get("workers_requested", 1), default=1, minimum=1)
    headless = bool(config.get("headless", True))
    max_attempts_per_lead = _as_int(config.get("max_attempts_per_lead", 3), default=3, minimum=1)
    worker_idle_seconds = _as_int(config.get("worker_idle_seconds", 30), default=30, minimum=1)
    worker_restart_limit = _as_int(config.get("worker_restart_limit", 20), default=20, minimum=1)
    monitor_interval = _as_float(config.get("worker_monitor_interval", 0.5), default=0.5, minimum=0.1)
    send_flow_timeout_seconds = _as_float(
        config.get("send_flow_timeout_seconds", 75.0),
        default=75.0,
        minimum=10.0,
    )
    worker_shutdown_timeout_seconds = _as_float(
        config.get("worker_shutdown_timeout_seconds", 8.0),
        default=8.0,
        minimum=1.0,
    )
    cooldown_fail_threshold = _as_int(config.get("cooldown_fail_threshold", 3), default=3, minimum=1)
    cooldown_seconds = _as_int(config.get("account_cooldown_seconds", 600), default=600, minimum=1)
    proxy_degraded_threshold = _as_int(config.get("proxy_degraded_threshold", 5), default=5, minimum=1)
    proxy_blocked_threshold = _as_int(config.get("proxy_blocked_threshold", 10), default=10, minimum=2)
    proxy_block_seconds = _as_int(config.get("proxy_block_seconds", 600), default=600, minimum=1)
    template_variants = _normalize_templates(config.get("templates"))
    template_rotator = TemplateRotator(template_variants)
    total_leads_hint = max(0, int(config.get("total_leads") or 0))
    preflight_started_at = time.perf_counter()
    preflight_timings_ms: dict[str, float] = {}
    last_progress_message = ""

    def _run_log(level: str, message: str, *args: Any, exc_info: bool = False) -> None:
        log_method = getattr(logger, level)
        if level == "exception" and not exc_info:
            exc_info = True
        log_method(f"[run_id=%s] {message}", run_id, *args, exc_info=exc_info)

    def _measure_preflight(label: str, factory: Callable[[], Any]) -> Any:
        started_at = time.perf_counter()
        result = factory()
        preflight_timings_ms[label] = (time.perf_counter() - started_at) * 1000.0
        return result

    def _log_preflight_timings() -> None:
        _run_log(
            "info",
            "Campaign preflight timings | alias=%s leads_alias=%s total_ms=%.2f "
            "load_accounts_ms=%.2f proxy_preflight_ms=%.2f start_snapshot_ms=%.2f load_leads_ms=%.2f "
            "prefilter_snapshot_ms=%.2f filter_pending_ms=%.2f capacity_ms=%.2f",
            alias,
            leads_alias,
            (time.perf_counter() - preflight_started_at) * 1000.0,
            preflight_timings_ms.get("load_accounts", 0.0),
            preflight_timings_ms.get("proxy_preflight", 0.0),
            preflight_timings_ms.get("start_snapshot", 0.0),
            preflight_timings_ms.get("load_leads", 0.0),
            preflight_timings_ms.get("prefilter_snapshot", 0.0),
            preflight_timings_ms.get("filter_pending", 0.0),
            preflight_timings_ms.get("capacity", 0.0),
        )

    def _worker_rows_snapshot(
        scheduler: AdaptiveScheduler | None,
        worker_slots: Dict[str, Dict[str, Any]] | None,
        health_monitor: HealthMonitor | None,
    ) -> list[dict[str, Any]]:
        if scheduler is None or not worker_slots:
            return []
        rows: list[dict[str, Any]] = []
        now = time.time()
        for worker_id, slot in worker_slots.items():
            snapshot = scheduler.worker_snapshot(worker_id)
            proxy_id = str(slot.get("proxy_id") or "")
            rows.append(
                {
                    "worker_id": worker_id,
                    "proxy_id": proxy_id,
                    "proxy_label": _proxy_label(proxy_id),
                    "proxy_status": health_monitor.proxy_status(proxy_id, now=now) if health_monitor is not None else "",
                    "execution_state": (
                        snapshot.execution_state.value if snapshot is not None else WorkerExecutionState.IDLE.value
                    ),
                    "execution_stage": (
                        snapshot.execution_stage.value if snapshot is not None else WorkerExecutionStage.IDLE.value
                    ),
                    "current_lead": snapshot.current_lead if snapshot is not None else "",
                    "current_account": snapshot.current_account if snapshot is not None else "",
                    "state_reason": snapshot.state_reason if snapshot is not None else "",
                    "restarts": int(snapshot.restarts or 0) if snapshot is not None else 0,
                }
            )
        rows.sort(key=lambda item: str(item.get("worker_id") or ""))
        return rows

    def _emit_progress(
        status: str,
        *,
        message: str = "",
        stats_snapshot: Dict[str, int] | None = None,
        total_leads: int | None = None,
        remaining: int | None = None,
        workers_active: int = 0,
        workers_capacity: int = 0,
        workers_effective: int = 0,
        worker_slots: Dict[str, Dict[str, Any]] | None = None,
        scheduler: AdaptiveScheduler | None = None,
        health_monitor: HealthMonitor | None = None,
        runtime_events: list[dict[str, Any]] | None = None,
    ) -> None:
        nonlocal last_progress_message
        if not callable(progress_callback):
            return
        payload_message = str(message or "").strip()
        if payload_message:
            last_progress_message = payload_message
        else:
            payload_message = last_progress_message
        counters = dict(stats_snapshot or {})
        payload = CampaignRunSnapshot.from_payload(
            {
                "run_id": run_id,
                "alias": alias,
                "leads_alias": leads_alias,
                "status": str(status or "").strip() or CampaignRunStatus.IDLE.value,
                "message": payload_message,
                "sent": int(counters.get("sent", 0)),
                "failed": int(counters.get("failed", 0)),
                "skipped": int(counters.get("skipped", 0)),
                "skipped_preblocked": int(counters.get("skipped_preblocked", 0)),
                "retried": int(counters.get("retried", 0)),
                "remaining": max(0, int(remaining if remaining is not None else 0)),
                "total_leads": max(0, int(total_leads if total_leads is not None else total_leads_hint)),
                "workers_active": max(0, int(workers_active or 0)),
                "workers_requested": workers_requested,
                "workers_capacity": max(0, int(workers_capacity or 0)),
                "workers_effective": max(0, int(workers_effective or 0)),
                "worker_rows": _worker_rows_snapshot(scheduler, worker_slots, health_monitor),
                "task_active": not CampaignRunStatus.parse(status).is_terminal,
            }
        ).to_payload()
        if runtime_events:
            payload["runtime_events"] = [dict(item) for item in runtime_events if isinstance(item, dict)]
        progress_callback(payload)

    def _build_result(
        *,
        sent: int = 0,
        failed: int = 0,
        skipped: int = 0,
        retried: int = 0,
        remaining: int = 0,
        workers_capacity: int = 0,
        workers_effective: int = 0,
        proxies: int = 0,
        worker_restarts: int = 0,
        skipped_preblocked: int = 0,
        health_state: dict[str, Any] | None = None,
        account_health: dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {
            "sent": max(0, int(sent or 0)),
            "failed": max(0, int(failed or 0)),
            "skipped": max(0, int(skipped or 0)),
            "retried": max(0, int(retried or 0)),
            "remaining": max(0, int(remaining or 0)),
            "workers_requested": workers_requested,
            "workers_capacity": max(0, int(workers_capacity or 0)),
            "workers_effective": max(0, int(workers_effective or 0)),
            "proxies": max(0, int(proxies or 0)),
            "worker_restarts": max(0, int(worker_restarts or 0)),
            "skipped_preblocked": max(0, int(skipped_preblocked or 0)),
            "health_state": dict(health_state or {}),
            "account_health": dict(account_health or {}),
        }

    accounts = _measure_preflight("load_accounts", lambda: _load_selected_accounts(alias))
    if not accounts:
        _run_log("warning", "No hay cuentas activas+conectadas en alias '%s'.", alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay cuentas activas o conectadas para iniciar la campaña.",
            total_leads=total_leads_hint,
        )
        return _build_result()

    proxy_preflight = _measure_preflight(
        "proxy_preflight",
        lambda: preflight_accounts_for_proxy_runtime(accounts),
    )
    for blocked in proxy_preflight.get("blocked_accounts") or []:
        if not isinstance(blocked, dict):
            continue
        username = str(blocked.get("username") or "").strip().lstrip("@") or "-"
        message = str(blocked.get("message") or blocked.get("status") or "proxy_blocked").strip()
        _run_log("warning", "Cuenta excluida por proxy invalido/quarantined @%s: %s", username, message)
    accounts = [
        dict(account)
        for account in (proxy_preflight.get("ready_accounts") or [])
        if isinstance(account, dict)
    ]
    if not accounts:
        _run_log("warning", "No hay cuentas utilizables tras preflight de proxy en alias '%s'.", alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay cuentas con proxy operativo para iniciar la campana.",
            total_leads=total_leads_hint,
        )
        return _build_result()

    alias_accounts = _account_usernames(accounts)
    start_snapshot = _measure_preflight(
        "start_snapshot",
        lambda: campaign_start_snapshot(alias_accounts, campaign_alias=alias),
    )
    accounts = _apply_sent_today_counts(
        accounts,
        sent_today_counts=dict(start_snapshot.get("daily_counts") or {}),
    )

    raw_leads = _measure_preflight("load_leads", lambda: load_leads(leads_alias))
    total_leads_hint = max(total_leads_hint, len(raw_leads))
    if not raw_leads:
        _run_log("warning", "No hay leads en alias '%s'.", leads_alias)
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay leads cargados para la campaña.",
            total_leads=total_leads_hint,
        )
        return _build_result()

    alias_terminal_leads, legacy_status_map = _measure_preflight(
        "prefilter_snapshot",
        lambda: get_prefilter_snapshot(alias),
    )
    skipped_for_quota = 0
    leads, lead_filter_stats = _measure_preflight(
        "filter_pending",
        lambda: _filter_pending_leads_for_campaign(
            raw_leads,
            alias=alias,
            alias_accounts=alias_accounts,
            campaign_registry=set(start_snapshot.get("campaign_registry") or set()),
            shared_registry=set(start_snapshot.get("shared_registry") or set()),
            alias_terminal_leads=alias_terminal_leads,
            legacy_status_map=legacy_status_map,
        ),
    )
    _log_campaign_diagnostics(
        alias=alias,
        leads_alias=leads_alias,
        total_leads_loaded=len(raw_leads),
        lead_filter_stats=lead_filter_stats,
        log_callback=_run_log,
    )
    if not leads:
        _run_log(
            "info",
            "Proxy Worker Runner: no quedaron leads elegibles tras el prefilter de campaña (alias=%s leads=%s).",
            alias,
            leads_alias,
        )
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="Todos los leads quedaron excluidos antes de iniciar workers.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
        )

    capacity = _measure_preflight("capacity", lambda: calculate_workers(accounts))
    proxy_groups = capacity.get("proxy_groups") or {}
    group_capacities = {
        str(proxy_id): max(0, int(value or 0))
        for proxy_id, value in (capacity.get("group_capacities") or {}).items()
    }
    ordered_worker_ids = list(capacity.get("ordered_worker_ids") or [])
    workers_capacity = int(capacity.get("workers_capacity") or 0)

    if workers_capacity <= 0:
        skipped_for_quota = len(leads)
        _run_log(
            "info",
            "Proxy Worker Runner: sin capacidad disponible en cuentas del alias '%s'.",
            alias,
        )
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay workers disponibles para ejecutar la campaña.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
        )

    workers_effective = min(workers_requested, workers_capacity)
    selected_proxy_ids = ordered_worker_ids[:workers_effective]
    leads, skipped_for_quota = _limit_leads_to_worker_capacity(
        leads,
        group_capacities=group_capacities,
        worker_ids=selected_proxy_ids,
    )
    if skipped_for_quota > 0:
        _run_log(
            "info",
            "Campaign quota cap applied: alias=%s queued=%d deferred=%d capacity=%d",
            alias,
            len(leads),
            skipped_for_quota,
            _total_remaining_capacity_for_groups(group_capacities, selected_proxy_ids),
        )
        _print_info_block(
            "Capacidad de campana",
            [
                f"Leads encolados para este run: {len(leads)}",
                f"Leads diferidos por limite de cuentas: {skipped_for_quota}",
            ],
        )
    if not leads:
        _log_preflight_timings()
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="No hay capacidad disponible en las cuentas para nuevos envios.",
            total_leads=total_leads_hint,
            remaining=0,
        )
        return _build_result(
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            skipped_preblocked=int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
        )
    proxy_worker_count = sum(1 for proxy_id in selected_proxy_ids if not _is_local_proxy_id(proxy_id))
    _log_preflight_timings()
    _print_info_block("Inicializando workers")

    health_monitor = HealthMonitor(
        proxy_degraded_threshold=proxy_degraded_threshold,
        proxy_blocked_threshold=proxy_blocked_threshold,
        proxy_block_seconds=proxy_block_seconds,
        account_cooldown_threshold=cooldown_fail_threshold,
        account_cooldown_seconds=cooldown_seconds,
    )
    initial_tasks = _build_initial_worker_tasks(
        leads,
        worker_ids=selected_proxy_ids,
        group_capacities=group_capacities,
    )
    initial_tasks = _validate_initial_worker_tasks(initial_tasks, log_callback=_run_log)
    lead_queue_lock = threading.Lock()
    scheduler = AdaptiveScheduler(
        lead_queue=initial_tasks,
        lead_queue_lock=lead_queue_lock,
        health_monitor=health_monitor,
        idle_seconds=worker_idle_seconds,
        max_attempts_per_lead=max_attempts_per_lead,
    )
    scheduler.register_proxy_queues(selected_proxy_ids)

    stats: Dict[str, int] = {
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "retried": 0,
        "worker_restarts": 0,
        "skipped_preblocked": int(lead_filter_stats.get("skipped_already_sent", 0)) + skipped_for_quota,
    }
    stats_lock = threading.Lock()
    runtime_event_counter = 0
    campaign_started_at = time.time()
    last_progress_at = 0.0
    progress_interval_seconds = 20.0

    def _stats_snapshot() -> Dict[str, int]:
        with stats_lock:
            return {
                "sent": int(stats.get("sent", 0)),
                "failed": int(stats.get("failed", 0)),
                "skipped": int(stats.get("skipped", 0)),
                "retried": int(stats.get("retried", 0)),
                "worker_restarts": int(stats.get("worker_restarts", 0)),
                "skipped_preblocked": int(stats.get("skipped_preblocked", 0)),
            }

    def _record_runtime_event(raw_event: dict[str, Any]) -> None:
        nonlocal runtime_event_counter
        if not isinstance(raw_event, dict):
            return
        event_type = str(raw_event.get("event_type") or "").strip()
        if not event_type:
            return
        runtime_event_counter += 1
        event = {
            **raw_event,
            "run_id": str(raw_event.get("run_id") or run_id).strip(),
            "event_type": event_type,
            "severity": str(raw_event.get("severity") or "info").strip().lower() or "info",
            "message": str(raw_event.get("message") or last_progress_message or "").strip(),
            "created_at": str(raw_event.get("created_at") or datetime.utcnow().isoformat()).strip(),
            "event_id": str(raw_event.get("event_id") or f"{run_id}:{runtime_event_counter:05d}:{event_type}").strip(),
        }
        _emit_progress(
            str(raw_event.get("status") or CampaignRunStatus.RUNNING.value).strip() or CampaignRunStatus.RUNNING.value,
            message=str(event.get("message") or last_progress_message or "").strip(),
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=scheduler.queue_size() if scheduler is not None else total_leads_hint,
            workers_active=sum(1 for slot in worker_slots.values() if not slot["future"].done()) if worker_slots else 0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots=worker_slots,
            scheduler=scheduler,
            health_monitor=health_monitor,
            runtime_events=[event],
        )

    def _emit_live_progress(status: str, message: str) -> None:
        _emit_progress(
            status,
            message=message,
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=scheduler.queue_size(),
            workers_active=sum(1 for slot in worker_slots.values() if not slot["future"].done()),
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots=worker_slots,
            scheduler=scheduler,
            health_monitor=health_monitor,
        )

    _run_log(
        "info",
        "alias=%s leads=%d proxies=%d workers=%d",
        alias,
        len(leads),
        proxy_worker_count,
        workers_effective,
    )

    campaign_token = EngineCancellationToken(f"proxy-campaign:{alias}")
    token_binding = bind_stop_token(campaign_token)
    worker_slots: Dict[str, Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=workers_effective, thread_name_prefix="proxy-worker") as executor:

        def _spawn_worker(worker_id: str, proxy_id: str) -> None:
            retry_proxy_ids = list(selected_proxy_ids)
            if proxy_id not in retry_proxy_ids:
                retry_proxy_ids.append(proxy_id)
            worker = ProxyWorker(
                worker_id=worker_id,
                proxy_id=proxy_id,
                accounts=proxy_groups.get(proxy_id, []),
                all_proxy_ids=retry_proxy_ids,
                scheduler=scheduler,
                health_monitor=health_monitor,
                stats=stats,
                stats_lock=stats_lock,
                delay_min=delay_min,
                delay_max=delay_max,
                template_rotator=template_rotator,
                cooldown_fail_threshold=cooldown_fail_threshold,
                campaign_alias=alias,
                leads_alias=leads_alias,
                campaign_run_id=run_id,
                runtime_event_callback=_record_runtime_event,
                headless=headless,
                send_flow_timeout_seconds=send_flow_timeout_seconds,
            )
            scheduler.register_worker(worker_id, proxy_id)
            future = executor.submit(bind_stop_token_callable(campaign_token, worker.run))
            worker_suffix = str(worker_id).split("-")[-1] or worker_id
            print("")
            print(f"Worker #{worker_suffix} iniciado")
            print(f"Proxy: {_proxy_label(proxy_id)} ({proxy_id})")
            print(f"Cuentas asignadas: {len(proxy_groups.get(proxy_id, []))}")
            worker_slots[worker_id] = {
                "worker": worker,
                "future": future,
                "proxy_id": proxy_id,
            }

        for index, proxy_id in enumerate(selected_proxy_ids, start=1):
            _spawn_worker(f"worker-{index}", proxy_id)

        _emit_live_progress(
            "Starting",
            "Workers inicializados. Preparando cuentas y cola de leads.",
        )

        _print_info_block("Cuentas listas para envÃ­o")
        reported_accounts: set[str] = set()
        for proxy_id in selected_proxy_ids:
            for account in proxy_groups.get(proxy_id, []):
                username = str(account.get("username") or "").strip()
                if not username or username in reported_accounts:
                    continue
                reported_accounts.add(username)
                session_label = (
                    "session_ready âœ“"
                    if has_playwright_storage_state(username)
                    else "session_pending"
                )
                print("")
                print(f"Cuenta: {username}")
                print(f"Estado: {session_label}")

        _emit_live_progress(
            "Running",
            "Campaña iniciada. Workers activos procesando la cola.",
        )

        while worker_slots and not STOP_EVENT.is_set():
            queue_size = scheduler.queue_size()
            now = time.time()
            if now - last_progress_at >= progress_interval_seconds:
                _print_progress_block(
                    sent=int(stats.get("sent", 0)),
                    failed=int(stats.get("failed", 0)),
                    skipped=int(stats.get("skipped", 0)),
                    remaining=queue_size,
                    started_at=campaign_started_at,
                )
                last_progress_at = now

            _emit_live_progress(
                "Running",
                "Procesando cola activa de campaña.",
            )

            if queue_size > 0:
                for worker_id, slot in list(worker_slots.items()):
                    worker: ProxyWorker = slot["worker"]
                    snapshot = scheduler.worker_snapshot(worker_id)
                    if snapshot is None:
                        continue
                    if worker.is_busy(now=now):
                        continue
                    if not scheduler.worker_is_stalled(worker_id, now=now):
                        continue
                    current_proxy = str(slot["proxy_id"] or "")
                    proxy_status = health_monitor.proxy_status(current_proxy, now=now)
                    activity_age = max(0.0, now - snapshot.last_activity_at)
                    stage_age = max(0.0, now - snapshot.state_entered_at)
                    _run_log(
                        "warning",
                        "Worker stalled detectado: %s proxy=%s status=%s exec_state=%s exec_stage=%s lead=%s account=%s activity_age=%.1fs stage_age=%.1fs queue=%d",
                        worker_id,
                        current_proxy,
                        proxy_status,
                        snapshot.execution_state.value,
                        snapshot.execution_stage.value,
                        snapshot.current_lead or "-",
                        snapshot.current_account or "-",
                        activity_age,
                        stage_age,
                        queue_size,
                    )
                    _record_runtime_event(
                        {
                            "event_type": "worker_stalled",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": f"Worker {worker_id} detectado como stalled.",
                            "worker_id": worker_id,
                            "proxy_id": current_proxy,
                            "lead": snapshot.current_lead or "",
                            "account": snapshot.current_account or "",
                            "queue_size": queue_size,
                            "activity_age_seconds": round(activity_age, 1),
                            "stage_age_seconds": round(stage_age, 1),
                            "proxy_status": proxy_status,
                        }
                    )
                    if proxy_status == "blocked":
                        new_proxy = scheduler.reassign_worker_proxy(
                            worker_id,
                            current_proxy=current_proxy,
                            all_proxy_ids=selected_proxy_ids,
                        )
                        slot["next_proxy_id"] = new_proxy
                        worker.request_stop("idle_reassignment")

            for worker_id in list(worker_slots.keys()):
                slot = worker_slots[worker_id]
                future: Future = slot["future"]
                if not future.done():
                    continue

                exc = future.exception()
                queue_pending = scheduler.queue_size() > 0
                should_restart = queue_pending and not STOP_EVENT.is_set()
                reason = "completed"
                if exc is not None:
                    reason = f"exception:{exc}"
                    _run_log("error", "Worker %s termino con excepcion: %s", worker_id, exc)
                    _record_runtime_event(
                        {
                            "event_type": "worker_future_exception",
                            "severity": "error",
                            "failure_kind": "system",
                            "message": f"Worker {worker_id} termino con excepcion.",
                            "worker_id": worker_id,
                            "proxy_id": str(slot.get("proxy_id") or ""),
                            "error": str(exc) or exc.__class__.__name__,
                        }
                    )
                elif should_restart:
                    reason = "queue_pending"

                if should_restart:
                    restart_count = scheduler.record_worker_restart(worker_id)
                    with stats_lock:
                        stats["worker_restarts"] = int(stats.get("worker_restarts", 0)) + 1
                    if restart_count > worker_restart_limit:
                        logger.error(
                            "Worker %s alcanzÃ³ lÃ­mite de reinicios (%d).",
                            worker_id,
                            worker_restart_limit,
                        )
                        _run_log("error", "Worker %s alcanzo limite de reinicios (%d).", worker_id, worker_restart_limit)
                        _record_runtime_event(
                            {
                                "event_type": "worker_restart_limit_reached",
                                "severity": "error",
                                "failure_kind": "terminal",
                                "message": f"Worker {worker_id} alcanzo el limite de reinicios.",
                                "worker_id": worker_id,
                                "proxy_id": str(slot.get("proxy_id") or ""),
                                "restart_count": restart_count,
                                "restart_limit": worker_restart_limit,
                            }
                        )
                        worker_slots.pop(worker_id, None)
                        continue

                    restart_proxy = str(slot.get("next_proxy_id") or slot["proxy_id"] or "")
                    if not health_monitor.is_proxy_available(restart_proxy):
                        restart_proxy = scheduler.reassign_worker_proxy(
                            worker_id,
                            current_proxy=restart_proxy,
                            all_proxy_ids=selected_proxy_ids,
                        )
                    if restart_proxy not in proxy_groups:
                        restart_proxy = str(slot["proxy_id"])

                    _run_log(
                        "warning",
                        "Worker restarted: %s proxy=%s reason=%s restart=%d",
                        worker_id,
                        restart_proxy,
                        reason,
                        restart_count,
                    )
                    _record_runtime_event(
                        {
                            "event_type": "worker_restarted",
                            "severity": "warning",
                            "failure_kind": "retryable",
                            "message": f"Worker {worker_id} relanzado en {_proxy_label(restart_proxy)}.",
                            "worker_id": worker_id,
                            "proxy_id": restart_proxy,
                            "reason": reason,
                            "restart_count": restart_count,
                        }
                    )
                    _spawn_worker(worker_id, restart_proxy)
                    _run_log("info", "Worker %s relanzado en proxy %s.", worker_id, restart_proxy)
                    _emit_live_progress(
                        "Running",
                        f"Worker {worker_id} relanzado en {_proxy_label(restart_proxy)}.",
                    )
                    continue

                worker_slots.pop(worker_id, None)

            if scheduler.is_empty():
                if all(slot["future"].done() for slot in worker_slots.values()):
                    break
            time.sleep(monitor_interval)

        for worker_id, slot in list(worker_slots.items()):
            worker: ProxyWorker = slot["worker"]
            worker.request_stop("campaign_shutdown")
            future: Future = slot["future"]
            try:
                future.result(timeout=worker_shutdown_timeout_seconds)
            except FutureTimeoutError:
                _run_log("warning", "Worker %s no se detuvo dentro de %.1fs durante shutdown.", worker_id, worker_shutdown_timeout_seconds)
                _record_runtime_event(
                    {
                        "event_type": "worker_shutdown_timeout",
                        "severity": "warning",
                        "failure_kind": "system",
                        "message": f"Worker {worker_id} no se detuvo dentro del timeout de shutdown.",
                        "worker_id": worker_id,
                        "proxy_id": str(slot.get("proxy_id") or ""),
                        "timeout_seconds": worker_shutdown_timeout_seconds,
                    }
                )
            except Exception as exc:
                _run_log("exception", "Worker %s fallo durante shutdown.", worker_id, exc_info=True)
                _record_runtime_event(
                    {
                        "event_type": "worker_shutdown_failed",
                        "severity": "error",
                        "failure_kind": "system",
                        "message": f"Worker {worker_id} fallo durante shutdown.",
                        "worker_id": worker_id,
                        "proxy_id": str(slot.get("proxy_id") or ""),
                        "error": str(exc) or exc.__class__.__name__,
                    }
                )
            worker_suffix = str(worker_id).split("-")[-1] or worker_id
            _print_info_block(
                "Worker detenido",
                [f"Worker #{worker_suffix} finalizado"],
            )

    stop_requested = STOP_EVENT.is_set()
    residual_tasks = scheduler.drain_all()
    residual_count = len(residual_tasks)
    if residual_tasks and not stop_requested:
        with stats_lock:
            stats["failed"] = int(stats.get("failed", 0)) + residual_count
        _run_log(
            "warning",
            "Proxy Worker Runner: %d leads marcados como fallidos por falta de workers activos.",
            residual_count,
        )
        for task in residual_tasks:
            try:
                mark_lead_failed(
                    task.lead,
                    reason="worker_exhausted",
                    attempts=task.attempt,
                    alias=alias,
                )
            except Exception as exc:
                _run_log("exception", "No se pudo persistir worker_exhausted para @%s.", task.lead, exc_info=True)
                _record_runtime_event(
                    {
                        "event_type": "worker_exhausted_persist_failed",
                        "severity": "error",
                        "failure_kind": "system",
                        "message": "No se pudo persistir worker_exhausted en lead_status.",
                        "lead": task.lead,
                        "error": str(exc) or exc.__class__.__name__,
                    }
                )
    elif residual_tasks:
        _run_log(
            "info",
            "Proxy Worker Runner: stop solicitado; %d leads quedan pendientes para un proximo run.",
            residual_count,
        )

    result = _build_result(
        sent=int(stats.get("sent", 0)),
        failed=int(stats.get("failed", 0)),
        skipped=int(stats.get("skipped", 0)),
        retried=int(stats.get("retried", 0)),
        remaining=residual_count if stop_requested else scheduler.queue_size(),
        workers_capacity=workers_capacity,
        workers_effective=workers_effective,
        proxies=proxy_worker_count,
        worker_restarts=int(stats.get("worker_restarts", 0)),
        skipped_preblocked=int(stats.get("skipped_preblocked", 0)),
        health_state=health_monitor.snapshot(),
        account_health=health_monitor.accounts_snapshot(),
    )
    _run_log(
        "info",
        "Proxy Worker Runner finalizado: sent=%d failed=%d retried=%d remaining=%d workers=%d proxies=%d restarts=%d",
        result["sent"],
        result["failed"],
        result["retried"],
        result["remaining"],
        result["workers_effective"],
        result["proxies"],
        result["worker_restarts"],
    )
    finished_at = time.time()
    if stop_requested:
        _print_campaign_end_block(
            completed=False,
            reason="detenida por usuario",
            sent=result["sent"],
            failed=result["failed"],
            skipped=result["skipped"],
            remaining=result["remaining"],
            started_at=campaign_started_at,
            finished_at=finished_at,
        )
        _emit_progress(
            CampaignRunStatus.STOPPED.value,
            message="Campaña detenida por usuario.",
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=result["remaining"],
            workers_active=0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots={},
            scheduler=scheduler,
            health_monitor=health_monitor,
        )
    else:
        _print_campaign_end_block(
            completed=True,
            reason="todos los leads procesados",
            sent=result["sent"],
            failed=result["failed"],
            skipped=result["skipped"],
            remaining=result["remaining"],
            started_at=campaign_started_at,
            finished_at=finished_at,
        )
        _emit_progress(
            CampaignRunStatus.COMPLETED.value,
            message="Campaña finalizada. Todos los leads del run actual fueron procesados.",
            stats_snapshot=_stats_snapshot(),
            total_leads=total_leads_hint,
            remaining=result["remaining"],
            workers_active=0,
            workers_capacity=workers_capacity,
            workers_effective=workers_effective,
            worker_slots={},
            scheduler=scheduler,
            health_monitor=health_monitor,
        )
    restore_stop_token(token_binding)
    return result


def _resolve_account_message_limit(account: Dict[str, Any]) -> int:
    for key in ("messages_per_account", "max_messages"):
        current = account.get(key)
        try:
            parsed = int(current)
            if parsed > 0:
                return parsed
        except Exception:
            continue
    return 25


def _resolve_account_sent_today(account: Dict[str, Any]) -> int:
    current = account.get("sent_today")
    try:
        parsed = int(current)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return 0


def _normalize_templates(raw_templates: Any) -> list[str]:
    if raw_templates is None:
        return []
    items = raw_templates if isinstance(raw_templates, list) else [raw_templates]
    templates: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _extract_template_text(item)
        if not text:
            continue
        for variant in _expand_template_variants(text):
            key = variant.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            templates.append(variant)
    return templates


def _expand_template_variants(text: str) -> list[str]:
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    variants = [line.strip() for line in normalized.splitlines() if line.strip()]
    if variants:
        return variants
    single = normalized.strip()
    return [single] if single else []


def _extract_template_text(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("text", "content", "message", "body", "template", "value"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        return ""
    return str(item or "").strip()


def _group_accounts_by_proxy(accounts: list[Dict[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        if _account_remaining_capacity(account) <= 0:
            continue
        proxy_id = _explicit_worker_proxy_id(account) or LOCAL_WORKER_PROXY_ID
        grouped.setdefault(proxy_id, []).append(account)
    return grouped


def _norm_account(value: str) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _parse_send_result(send_result: Any) -> tuple[bool, str, Dict[str, Any]]:
    parsed = CampaignSendResult.from_sender_result(send_result)
    return parsed.ok, parsed.detail, dict(parsed.payload)


def _campaign_failure_reason(parsed: CampaignSendResult) -> str:
    detail = str(parsed.detail or "").strip()
    reason_code = str(parsed.reason_code or "").strip()
    detail_upper = detail.upper()
    if detail and (
        detail_upper.startswith("SKIPPED_")
        or parsed.status in {CampaignSendStatus.SKIPPED, CampaignSendStatus.AMBIGUOUS}
    ):
        return detail
    return reason_code or detail or "send_failed"


def _as_int(value: Any, *, default: int, minimum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, parsed)


def _as_float(value: Any, *, default: float, minimum: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(float(minimum), parsed)


def _now_hms() -> str:
    return time.strftime("%H:%M:%S")


def _proxy_label(proxy_id: str) -> str:
    normalized = str(proxy_id or "").strip()
    if not normalized or normalized == "__no_proxy__":
        return "local"
    return normalized


def _print_info_block(title: str, lines: list[str] | None = None) -> None:
    print(f"[INFO] {str(title or '').strip()}")
    if lines:
        print("")
        for line in lines:
            if str(line or "").strip():
                print(str(line))


def _print_send_block(*, account: str, lead: str, delay_seconds: int, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print("Estado: enviado âœ“")
    print(f"Delay aplicado: {max(0, int(delay_seconds))}s")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _print_error_block(*, account: str, lead: str, reason: str, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print(f"ERROR: {str(reason or '').strip() or 'error de envÃ­o'}")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _print_skip_block(*, account: str, lead: str, reason: str, proxy_id: str) -> None:
    print(f"{_now_hms()}  {account} â†’ {lead}")
    print("Estado: omitido")
    print(f"Motivo: {str(reason or '').strip() or 'omitido por reglas de campaÃ±a'}")
    print(f"Proxy: {_proxy_label(proxy_id)}")
    print("")


def _format_human_duration(seconds: float) -> str:
    total = max(0, int(round(float(seconds))))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _print_progress_block(
    *,
    sent: int,
    failed: int,
    skipped: int,
    remaining: int,
    started_at: float,
) -> None:
    elapsed = max(1.0, time.time() - float(started_at or time.time()))
    speed_h = max(0.0, (float(sent) / elapsed) * 3600.0)
    if speed_h <= 0.01:
        eta_text = "-"
    else:
        eta_seconds = max(0.0, (float(remaining) / speed_h) * 3600.0)
        eta_text = _format_human_duration(eta_seconds)
    print("[PROGRESS] Estado de campaÃ±a")
    print("")
    print(f"Leads enviados: {max(0, int(sent))}")
    print(f"Errores: {max(0, int(failed))}")
    print(f"Omitidos: {max(0, int(skipped))}")
    print(f"Leads restantes: {max(0, int(remaining))}")
    print(f"Velocidad actual: {int(round(speed_h))} mensajes/hora")
    print(f"Tiempo estimado restante: {eta_text}")


def _print_campaign_end_block(
    *,
    completed: bool,
    reason: str,
    sent: int,
    failed: int,
    skipped: int,
    remaining: int,
    started_at: float,
    finished_at: float,
) -> None:
    if completed:
        print("[INFO] CampaÃ±a completada")
    else:
        print("[INFO] CampaÃ±a finalizada")
    print("")
    print(f"Motivo: {str(reason or '').strip()}")
    print("")
    print(f"Leads enviados: {max(0, int(sent))}")
    print(f"Errores: {max(0, int(failed))}")
    print(f"Omitidos: {max(0, int(skipped))}")
    print(f"Leads restantes: {max(0, int(remaining))}")
    print("")
    print(f"Tiempo total ejecutado: {_format_human_duration(max(0.0, finished_at - started_at))}")
    print(f"Hora de finalizaciÃ³n: {time.strftime('%H:%M:%S', time.localtime(max(0.0, finished_at)))}")




