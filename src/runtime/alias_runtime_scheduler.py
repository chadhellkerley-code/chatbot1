from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any

from src.inbox_diagnostics import record_inbox_diagnostic


logger = logging.getLogger(__name__)


class AliasRuntimeScheduler:
    _HEARTBEAT_INTERVAL_SECONDS = 1.0
    _HEARTBEAT_STALE_SECONDS = 30.0
    _BOOT_SWEEP_ACTIVE_STATES = {"starting", "running", "stopping", "degraded"}

    def __init__(self, *, runtime: Any, store: Any, cancel_pending_jobs=None) -> None:
        self._runtime = runtime
        self._store = store
        self._cancel_pending_jobs = cancel_pending_jobs
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._stops: dict[str, threading.Event] = {}

    def _record_diagnostic(
        self,
        *,
        alias_id: str,
        account_id: str = "",
        event_type: str,
        stage: str,
        outcome: str,
        reason: str = "",
        reason_code: str = "",
        exception: BaseException | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_inbox_diagnostic(
            self._store,
            event_type=event_type,
            stage=stage,
            outcome=outcome,
            account_id=account_id,
            alias_id=alias_id,
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload=payload,
            callsite_skip=2,
        )

    def start_alias(self, alias_id: str, config: dict[str, Any]) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return {}
        self._record_diagnostic(
            alias_id=clean_alias,
            event_type="alias_runtime_start_requested",
            stage="start",
            outcome="attempt",
            reason="alias_runtime_start_requested",
            reason_code="alias_runtime_start_requested",
            payload={"config": dict(config or {})},
        )
        started_at = time.time()
        with self._lock:
            worker = self._threads.get(clean_alias)
            if worker is not None and worker.is_alive():
                return self.status(clean_alias)
            self._threads.pop(clean_alias, None)
            self._stops.pop(clean_alias, None)
            stop_event = threading.Event()
            worker = threading.Thread(
                target=self._run_alias,
                args=(clean_alias, dict(config or {}), stop_event),
                name=f"inbox-automation-{clean_alias}",
                daemon=True,
            )
            self._stops[clean_alias] = stop_event
            self._threads[clean_alias] = worker
            worker.start()
        return self._store.upsert_runtime_alias_state(
            clean_alias,
            {
                "is_running": True,
                "worker_state": "starting",
                "current_account_id": "",
                "next_account_id": "",
                "current_turn_count": 0,
                "last_error": "",
                "last_heartbeat_at": started_at,
                **dict(config or {}),
            },
        )

    def stop_alias(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return {}
        self._record_diagnostic(
            alias_id=clean_alias,
            event_type="alias_runtime_stop_requested",
            stage="stop",
            outcome="attempt",
            reason="alias_runtime_stop_requested",
            reason_code="alias_runtime_stop_requested",
        )
        with self._lock:
            stop_event = self._stops.get(clean_alias)
            worker = self._threads.get(clean_alias)
        if stop_event is not None:
            stop_event.set()
        self._drain_pending_jobs(clean_alias, reason="runtime_stopping")
        if worker is not None:
            worker.join(timeout=2.0)
        is_alive = bool(worker is not None and worker.is_alive())
        self._drain_pending_jobs(clean_alias, reason="runtime_stopping")
        with self._lock:
            if not is_alive:
                self._stops.pop(clean_alias, None)
                self._threads.pop(clean_alias, None)
        if is_alive:
            return self._store.upsert_runtime_alias_state(
                clean_alias,
                {
                    "is_running": True,
                    "worker_state": "stopping",
                    "last_heartbeat_at": time.time(),
                },
            )
        return self._mark_alias_stopped(clean_alias, worker_state="stopped", last_error="")

    def _mark_alias_stopped(self, alias_id: str, *, worker_state: str, last_error: str) -> dict[str, Any]:
        return self._store.upsert_runtime_alias_state(
            alias_id,
            {
                "is_running": False,
                "worker_state": str(worker_state or "stopped").strip() or "stopped",
                "current_account_id": "",
                "next_account_id": "",
                "current_turn_count": 0,
                "last_heartbeat_at": time.time(),
                "last_error": str(last_error or "").strip(),
            },
        )

    @classmethod
    def sweep_boot_persisted_states(
        cls,
        *,
        store: Any,
        existing_aliases: set[str] | None = None,
        active_alias_accounts: dict[str, set[str]] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        list_states = getattr(store, "list_runtime_alias_states", None)
        update_state = getattr(store, "upsert_runtime_alias_state", None)
        delete_state = getattr(store, "delete_runtime_alias_state", None)
        if not callable(list_states) or not callable(update_state):
            return {"checked": 0, "cleaned": 0, "deleted": 0, "details": []}

        known_aliases = {str(item or "").strip().lower() for item in (existing_aliases or set()) if str(item or "").strip()}
        allowed_accounts = {
            str(alias or "").strip().lower(): {
                cls._normalize_account_id(account_id)
                for account_id in (accounts or set())
                if cls._normalize_account_id(account_id)
            }
            for alias, accounts in dict(active_alias_accounts or {}).items()
            if str(alias or "").strip()
        }
        timestamp = float(now or time.time())
        summary = {"checked": 0, "cleaned": 0, "deleted": 0, "details": []}
        for raw_state in list_states():
            if not isinstance(raw_state, dict):
                continue
            alias_id = str(raw_state.get("alias_id") or "").strip()
            alias_key = alias_id.lower()
            if not alias_id:
                continue
            summary["checked"] += 1
            if known_aliases and alias_key not in known_aliases:
                if callable(delete_state) and bool(delete_state(alias_id)):
                    summary["deleted"] += 1
                    summary["details"].append({"alias_id": alias_id, "action": "deleted_missing_alias"})
                continue

            current_account_id = cls._normalize_account_id(raw_state.get("current_account_id"))
            next_account_id = cls._normalize_account_id(raw_state.get("next_account_id"))
            worker_state = str(raw_state.get("worker_state") or "").strip().lower() or "stopped"
            is_running = bool(raw_state.get("is_running"))
            active_accounts = allowed_accounts.get(alias_key, set())
            current_turn_count = max(0, int(raw_state.get("current_turn_count") or 0))
            reasons: list[str] = []
            if is_running:
                reasons.append("running_without_boot_worker")
            if worker_state in cls._BOOT_SWEEP_ACTIVE_STATES:
                reasons.append(f"worker_state_{worker_state}")
            if current_account_id and current_account_id not in active_accounts:
                reasons.append("current_account_zombie")
            if next_account_id and next_account_id not in active_accounts:
                reasons.append("next_account_zombie")
            if current_turn_count > 0 and (not current_account_id or not is_running):
                reasons.append("current_turn_zombie")
            if current_turn_count > 0 and current_account_id and current_account_id not in active_accounts:
                reasons.append("current_turn_account_zombie")
            if not reasons:
                continue

            last_error = str(raw_state.get("last_error") or "").strip()
            if not last_error:
                last_error = "no_active_accounts" if not active_accounts else "boot_stale_runtime_cleaned"
            update_state(
                alias_id,
                {
                    "is_running": False,
                    "worker_state": "stopped",
                    "current_account_id": "",
                    "next_account_id": "",
                    "current_turn_count": 0,
                    "last_heartbeat_at": timestamp,
                    "last_error": last_error,
                    "updated_at": timestamp,
                },
            )
            summary["cleaned"] += 1
            summary["details"].append({"alias_id": alias_id, "action": "cleaned", "reasons": reasons})
        return summary

    def _drain_pending_jobs(self, alias_id: str, *, reason: str = "runtime_stopped") -> None:
        callback = self._cancel_pending_jobs
        if callable(callback):
            callback(alias_id, reason=reason)
            return
        cancel_jobs = getattr(self._store, "cancel_send_queue_jobs", None)
        if not callable(cancel_jobs):
            return
        try:
            cancel_jobs(
                alias_id=alias_id,
                job_types=["auto_reply", "followup"],
                states=["queued", "processing"],
                reason=reason,
            )
            return
        except TypeError:
            pass
        for account in list(self._runtime.list_alias_accounts(alias_id) or []):
            account_id = str(account.get("username") or "").strip().lstrip("@").lower()
            if not account_id:
                continue
            cancel_jobs(
                account_id=account_id,
                job_types=["auto_reply", "followup"],
                states=["queued", "processing"],
                reason=reason,
            )

    def status(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        if not clean_alias:
            return {}
        state = self._store.get_runtime_alias_state(clean_alias)
        if not state:
            return {}
        with self._lock:
            worker = self._threads.get(clean_alias)
            stop_event = self._stops.get(clean_alias)
        if bool(state.get("is_running")) and (worker is None or not worker.is_alive()):
            requested_stop = bool(stop_event is not None and stop_event.is_set()) or str(state.get("worker_state") or "").strip().lower() in {
                "stopping",
                "stopped",
            }
            worker_state = "stopped" if requested_stop else "error"
            last_error = "" if worker_state == "stopped" else str(state.get("last_error") or "").strip() or "worker_stopped_unexpectedly"
            return self._mark_alias_stopped(clean_alias, worker_state=worker_state, last_error=last_error)
        if bool(state.get("is_running")) and self._heartbeat_is_stale(state):
            return self._mark_alias_stopped(
                clean_alias,
                worker_state="degraded",
                last_error=str(state.get("last_error") or "").strip() or "worker_heartbeat_stale",
            )
        return state

    def _run_alias(self, alias_id: str, config: dict[str, Any], stop_event: threading.Event) -> None:
        mode = str(config.get("mode") or "both").strip().lower() or "both"
        turns_per_account = max(1, int(config.get("max_turns_per_account") or config.get("turns_per_account") or 1))
        delay_min_ms = max(0, int(config.get("delay_min_ms") or config.get("delay_min") or 0))
        delay_max_ms = max(delay_min_ms, int(config.get("delay_max_ms") or config.get("delay_max") or delay_min_ms))
        stats = {"accounts_processed": 0, "turns": 0, "queued_jobs": 0, "errors": 0}
        exit_state = "stopped"
        last_error = ""
        drain_reason = "runtime_stopped"
        try:
            self._heartbeat_alias(
                alias_id,
                {
                    "is_running": True,
                    "worker_state": "running",
                    "current_account_id": "",
                    "next_account_id": "",
                    "current_turn_count": 0,
                    "max_turns_per_account": turns_per_account,
                    "delay_min_ms": delay_min_ms,
                    "delay_max_ms": delay_max_ms,
                    "mode": mode,
                    "last_error": "",
                    "stats": stats,
                },
            )
            while not stop_event.is_set():
                self._heartbeat_alias(
                    alias_id,
                    {
                        "is_running": True,
                        "worker_state": "running",
                        "max_turns_per_account": turns_per_account,
                        "delay_min_ms": delay_min_ms,
                        "delay_max_ms": delay_max_ms,
                        "mode": mode,
                        "stats": stats,
                    },
                )
                accounts = self._runtime.list_alias_accounts(alias_id)
                if stop_event.is_set():
                    break
                if not accounts:
                    last_error = "no_active_accounts"
                    self._record_diagnostic(
                        alias_id=alias_id,
                        event_type="process_account_turn_skipped",
                        stage="process_account_turn",
                        outcome="skip",
                        reason=last_error,
                        reason_code=last_error,
                    )
                    drain_reason = last_error
                    self._drain_pending_jobs(alias_id, reason=drain_reason)
                    break
                for index, account in enumerate(accounts):
                    if stop_event.is_set():
                        break
                    account_id = str(account.get("username") or "").strip().lstrip("@").lower()
                    next_account_id = str(accounts[(index + 1) % len(accounts)].get("username") or "").strip().lstrip("@").lower()
                    for turn in range(1, turns_per_account + 1):
                        if stop_event.is_set():
                            break
                        self._heartbeat_alias(
                            alias_id,
                            {
                                "is_running": True,
                                "worker_state": "running",
                                "current_account_id": account_id,
                                "next_account_id": next_account_id,
                                "current_turn_count": turn,
                                "max_turns_per_account": turns_per_account,
                                "delay_min_ms": delay_min_ms,
                                "delay_max_ms": delay_max_ms,
                                "mode": mode,
                                "last_error": "",
                                "stats": stats,
                            },
                        )
                        try:
                            result = self._runtime.process_account_turn(account, mode=mode)
                        except Exception as exc:
                            self._record_diagnostic(
                                alias_id=alias_id,
                                account_id=account_id,
                                event_type="process_account_turn_failed",
                                stage="process_account_turn",
                                outcome="fail",
                                exception=exc,
                                payload={"mode": mode, "turn": turn},
                            )
                            raise
                        stats["accounts_processed"] += 1
                        stats["turns"] += 1
                        stats["queued_jobs"] += int(result.get("queued_jobs") or 0)
                        stats["errors"] += int(result.get("errors") or 0)
                        self._heartbeat_alias(
                            alias_id,
                            {
                                "is_running": True,
                                "worker_state": "running",
                                "current_account_id": account_id,
                                "next_account_id": next_account_id,
                                "current_turn_count": turn,
                                "max_turns_per_account": turns_per_account,
                                "delay_min_ms": delay_min_ms,
                                "delay_max_ms": delay_max_ms,
                                "mode": mode,
                                "last_error": "",
                                "stats": stats,
                            },
                        )
                        pause_ms = random.randint(delay_min_ms, delay_max_ms) if delay_max_ms > delay_min_ms else delay_min_ms
                        if pause_ms > 0:
                            self._sleep_with_heartbeat(
                                alias_id,
                                stop_event,
                                pause_ms / 1000.0,
                                {
                                    "is_running": True,
                                    "worker_state": "running",
                                    "current_account_id": account_id,
                                    "next_account_id": next_account_id,
                                    "current_turn_count": turn,
                                    "max_turns_per_account": turns_per_account,
                                    "delay_min_ms": delay_min_ms,
                                    "delay_max_ms": delay_max_ms,
                                    "mode": mode,
                                    "stats": stats,
                                },
                            )
                if not stop_event.is_set():
                    self._sleep_with_heartbeat(
                        alias_id,
                        stop_event,
                        0.1,
                        {
                            "is_running": True,
                            "worker_state": "running",
                            "current_account_id": "",
                            "current_turn_count": 0,
                            "max_turns_per_account": turns_per_account,
                            "delay_min_ms": delay_min_ms,
                            "delay_max_ms": delay_max_ms,
                            "mode": mode,
                            "stats": stats,
                        },
                    )
        except Exception as exc:
            exit_state = "error"
            drain_reason = "runtime_crashed"
            last_error = f"{type(exc).__name__}: {exc}"
            self._record_diagnostic(
                alias_id=alias_id,
                event_type="alias_runtime_loop_failed",
                stage="loop",
                outcome="fail",
                exception=exc,
                payload={"mode": mode, "stats": stats},
            )
            logger.exception("Alias runtime worker crashed for %s", alias_id)
            self._drain_pending_jobs(alias_id, reason=drain_reason)
        finally:
            if stop_event.is_set() and exit_state != "error":
                exit_state = "stopped"
                last_error = ""
            self._store.upsert_runtime_alias_state(
                alias_id,
                {
                    "is_running": False,
                    "worker_state": exit_state,
                    "current_account_id": "",
                    "next_account_id": "",
                    "current_turn_count": 0,
                    "max_turns_per_account": turns_per_account,
                    "delay_min_ms": delay_min_ms,
                    "delay_max_ms": delay_max_ms,
                    "mode": mode,
                    "last_heartbeat_at": time.time(),
                    "last_error": str(last_error or "").strip(),
                    "stats": stats,
                },
            )
            current_thread = threading.current_thread()
            with self._lock:
                if self._threads.get(alias_id) is current_thread:
                    self._threads.pop(alias_id, None)
                if self._stops.get(alias_id) is stop_event:
                    self._stops.pop(alias_id, None)

    def _heartbeat_alias(self, alias_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        payload = dict(updates or {})
        payload["last_heartbeat_at"] = time.time()
        return self._store.upsert_runtime_alias_state(alias_id, payload)

    @staticmethod
    def _normalize_account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()

    def _sleep_with_heartbeat(
        self,
        alias_id: str,
        stop_event: threading.Event,
        sleep_seconds: float,
        updates: dict[str, Any],
    ) -> None:
        remaining = max(0.0, float(sleep_seconds or 0.0))
        if remaining <= 0.0:
            return
        while remaining > 0.0 and not stop_event.is_set():
            wait_for = min(self._HEARTBEAT_INTERVAL_SECONDS, remaining)
            stop_event.wait(wait_for)
            remaining -= wait_for
            if not stop_event.is_set():
                self._heartbeat_alias(alias_id, updates)

    def _heartbeat_is_stale(self, state: dict[str, Any]) -> bool:
        try:
            heartbeat = float(state.get("last_heartbeat_at") or 0.0)
        except Exception:
            heartbeat = 0.0
        if heartbeat <= 0.0:
            return True
        stale_window = max(
            self._HEARTBEAT_STALE_SECONDS,
            (max(0, int(state.get("delay_max_ms") or 0)) / 1000.0) + 20.0,
        )
        return (time.time() - heartbeat) > stale_window
