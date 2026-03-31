from __future__ import annotations

from typing import Any

from core import accounts as accounts_module
from src.runtime.alias_runtime_scheduler import AliasRuntimeScheduler
from src.runtime.inbox_automation_runtime import InboxAutomationRuntime
from src.runtime.ownership_router import OwnershipRouter
from src.runtime.runtime_events import MANUAL_TAKEN


class InboxAutomationService:
    def __init__(self, *, store: Any, sender: Any, ensure_backend_started) -> None:
        self._store = store
        self._sender = sender
        self._ensure_backend_started = ensure_backend_started
        self._runtime = InboxAutomationRuntime(
            store=store,
            sender=sender,
            ensure_backend_started=ensure_backend_started,
        )
        self._scheduler = AliasRuntimeScheduler(
            runtime=self._runtime,
            store=store,
            cancel_pending_jobs=self._cancel_pending_runtime_jobs,
        )
        self._router = OwnershipRouter()

    def list_aliases(self) -> list[str]:
        aliases: list[str] = []
        seen: set[str] = set()
        for raw in accounts_module.list_all():
            if not isinstance(raw, dict):
                continue
            alias = str(raw.get("alias") or "").strip()
            if not alias:
                continue
            key = alias.lower()
            if key in seen:
                continue
            seen.add(key)
            aliases.append(alias)
        aliases.sort(key=str.lower)
        return aliases

    def alias_accounts(self, alias_id: str) -> list[dict[str, Any]]:
        return self._runtime.list_alias_accounts(alias_id)

    def start_alias(self, alias_id: str, config: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "mode": str(config.get("mode") or "both").strip().lower() or "both",
            "max_turns_per_account": max(1, int(config.get("turns_per_account") or config.get("max_turns_per_account") or 1)),
            "delay_min_ms": max(0, int(config.get("delay_min_ms") or config.get("delay_min") or 0)),
            "delay_max_ms": max(
                max(0, int(config.get("delay_min_ms") or config.get("delay_min") or 0)),
                int(config.get("delay_max_ms") or config.get("delay_max") or config.get("delay_min_ms") or config.get("delay_min") or 0),
            ),
        }
        return self._scheduler.start_alias(alias_id, payload)

    def stop_alias(self, alias_id: str) -> dict[str, Any]:
        return self._scheduler.stop_alias(alias_id)

    def _cancel_pending_runtime_jobs(self, alias_id: str, *, reason: str = "runtime_stopped") -> None:
        cancel_jobs = getattr(self._store, "cancel_send_queue_jobs", None)
        if callable(cancel_jobs):
            try:
                cancel_jobs(
                    alias_id=alias_id,
                    job_types=["auto_reply", "followup"],
                    states=["queued", "processing"],
                    reason=reason,
                )
            except TypeError:
                for account in self.alias_accounts(alias_id):
                    account_id = str(account.get("username") or "").strip().lstrip("@").lower()
                    if not account_id:
                        continue
                    cancel_jobs(
                        account_id=account_id,
                        job_types=["auto_reply", "followup"],
                        states=["queued", "processing"],
                        reason=reason,
                    )
        drain_jobs = getattr(self._sender, "cancel_pending_runtime_jobs", None)
        if callable(drain_jobs):
            drain_jobs(
                alias_id,
                job_types=["auto_reply", "followup"],
                reason=reason,
            )

    def status(self, alias_id: str) -> dict[str, Any]:
        clean_alias = str(alias_id or "").strip()
        account_rows = self.alias_accounts(clean_alias)
        state = self._scheduler.status(clean_alias)
        if state and not account_rows:
            worker_state = str(state.get("worker_state") or "").strip().lower()
            updates = {
                "is_running": False,
                "worker_state": "error" if worker_state == "error" else "stopped",
                "current_account_id": "",
                "next_account_id": "",
                "current_turn_count": 0,
                "last_error": str(state.get("last_error") or "").strip() or "no_active_accounts",
            }
            persist_state = getattr(self._store, "upsert_runtime_alias_state", None)
            if callable(persist_state):
                state = persist_state(clean_alias, updates)
            else:
                state = {**dict(state), "alias_id": clean_alias, **updates}
        if not state:
            return {
                "alias_id": clean_alias,
                "is_running": False,
                "worker_state": "stopped",
                "current_account_id": "",
                "next_account_id": "",
                "current_turn_count": 0,
                "max_turns_per_account": 1,
                "delay_min_ms": 0,
                "delay_max_ms": 0,
                "mode": "both",
                "last_heartbeat_at": None,
                "stats": {},
                "account_rows": account_rows,
            }
        state = dict(state)
        state["account_rows"] = account_rows
        return state

    def list_statuses(self) -> list[dict[str, Any]]:
        return self._store.list_runtime_alias_states()

    def manual_send_allowed(self, thread: dict[str, Any] | None) -> bool:
        row = dict(thread or {})
        alias_id = str(row.get("alias_id") or row.get("account_alias") or "").strip()
        runtime_active = bool(self.status(alias_id).get("is_running")) if alias_id else False
        return self._router.can_manual_send(row, runtime_active=runtime_active)

    def manual_takeover_allowed(self, thread: dict[str, Any] | None) -> bool:
        row = dict(thread or {})
        alias_id = str(row.get("alias_id") or row.get("account_alias") or "").strip()
        runtime_active = bool(self.status(alias_id).get("is_running")) if alias_id else False
        return self._router.can_manual_takeover(row, runtime_active=runtime_active)

    def manual_release_allowed(self, thread: dict[str, Any] | None) -> bool:
        return self._router.can_manual_release(dict(thread or {}))

    def manual_takeover(self, thread_key: str, *, operator_id: str) -> dict[str, Any] | None:
        begin_takeover = getattr(self._sender, "begin_manual_takeover", None)
        finish_takeover = getattr(self._sender, "finish_manual_takeover", None)
        cancel_thread_jobs = getattr(self._sender, "cancel_pending_thread_jobs", None)
        if callable(begin_takeover):
            begin_takeover(thread_key)
        thread = self._store.get_thread(thread_key)
        try:
            if not isinstance(thread, dict):
                return None
            updated = self._store.update_thread_record(thread_key, self._router.manual_takeover(thread, operator_id))
            if not isinstance(updated, dict):
                return None
            self._store.cancel_send_queue_jobs(
                thread_key=thread_key,
                job_types=["auto_reply", "followup"],
                states=["queued", "processing"],
                reason="manual_takeover",
            )
            if callable(cancel_thread_jobs):
                cancel_thread_jobs(
                    thread_key,
                    job_types=["auto_reply", "followup"],
                    reason="manual_takeover",
                )
            self._store.add_thread_event(
                thread_key,
                MANUAL_TAKEN,
                account_id=str(updated.get("account_id") or "").strip(),
                alias_id=str(updated.get("alias_id") or updated.get("account_alias") or "").strip(),
                payload={"operator_id": str(operator_id or "").strip()},
            )
            return self._store.get_thread(thread_key)
        finally:
            if callable(finish_takeover):
                finish_takeover(thread_key)

    def manual_release(self, thread_key: str) -> dict[str, Any] | None:
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict):
            return None
        updated = self._store.update_thread_record(thread_key, self._router.manual_release(thread))
        return self._store.get_thread(thread_key) if isinstance(updated, dict) else None

    def mark_thread_qualified(self, thread_key: str, *, operator_id: str = "inbox_ui") -> dict[str, Any] | None:
        return self.manual_takeover(thread_key, operator_id=operator_id)

    def mark_thread_disqualified(self, thread_key: str) -> dict[str, Any] | None:
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict):
            return None
        updated = self._store.update_thread_record(thread_key, self._router.mark_disqualified(thread))
        return self._store.get_thread(thread_key) if isinstance(updated, dict) else None

    def clear_thread_classification(self, thread_key: str) -> dict[str, Any] | None:
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict):
            return None
        current = self._router.initialize_thread(thread)
        updated = self._store.update_thread_record(
            thread_key,
            {
                "owner": "auto",
                "bucket": "all",
                "status": "open",
                "manual_lock": False,
                "manual_assignee": "",
                "previous_bucket": None,
                "previous_status": None,
                "previous_owner": None,
                "stage_id": str(current.get("stage_id") or "initial").strip() or "initial",
            },
        )
        return self._store.get_thread(thread_key) if isinstance(updated, dict) else None
