from __future__ import annotations

<<<<<<< HEAD
from datetime import datetime, timedelta
from typing import Any

from core import accounts as accounts_module
from core import responder as responder_module
from core.inbox.conversation_reader import ConversationReader
from core.storage import TZ as STORAGE_TZ
from core.account_limits import can_send_message_for_account
from src.inbox_diagnostics import record_inbox_diagnostic
=======
from typing import Any

from core import accounts as accounts_module
from core.inbox.conversation_reader import ConversationReader
>>>>>>> origin/main
from src.inbox.endpoint_reader import sync_account_threads_from_storage
from src.runtime.automation_engine_adapter import AutomationEngineAdapter
from src.runtime.ownership_router import OwnershipRouter
from src.runtime.runtime_events import (
    DISQUALIFIED,
    INBOUND_RECEIVED,
    QUALIFIED,
    STAGE_CHANGED,
    THREAD_UPDATED,
    queued_thread_event,
)
from src.runtime.session_connector_registry import SessionConnectorRegistry


class RuntimeSendQueue:
    def __init__(self, *, store: Any, sender: Any) -> None:
        self._store = store
        self._sender = sender

<<<<<<< HEAD
    def _record_enqueue_diagnostic(
        self,
        *,
        thread_key: str,
        job_type: str,
        event_type: str,
        outcome: str,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        thread = self._store.get_thread(thread_key) if callable(getattr(self._store, "get_thread", None)) else {}
        row = dict(thread or {})
        record_inbox_diagnostic(
            self._store,
            event_type=event_type,
            stage="enqueue",
            outcome=outcome,
            account_id=str(row.get("account_id") or "").strip(),
            alias_id=str(row.get("alias_id") or row.get("account_alias") or "").strip(),
            thread_key=thread_key,
            job_type=job_type,
            reason=reason,
            payload=payload,
            callsite_skip=2,
        )

=======
>>>>>>> origin/main
    def enqueue_text(
        self,
        thread_key: str,
        text: str,
        *,
        job_type: str,
        dedupe_key: str,
        metadata: dict[str, Any] | None = None,
<<<<<<< HEAD
    ) -> dict[str, Any]:
        enqueue_fn = getattr(self._sender, "enqueue_message_job", None)
        result: dict[str, Any]
        if callable(enqueue_fn):
            raw_result = enqueue_fn(
                thread_key,
                text,
                job_type=job_type,
                dedupe_key=dedupe_key,
                metadata=dict(metadata or {}),
            )
            result = dict(raw_result or {}) if isinstance(raw_result, dict) else {}
        else:
            local_id = self._sender.queue_message(
                thread_key,
                text,
                job_type=job_type,
                metadata={
                    "dedupe_key": dedupe_key,
                    **dict(metadata or {}),
                },
            )
            result = {
                "ok": bool(local_id),
                "job_id": 0,
                "created": bool(local_id),
                "reused": False,
                "dedupe_key": dedupe_key,
                "state": "queued" if local_id else "",
                "local_message_id": str(local_id or "").strip(),
            }
        if not bool(result.get("ok")) or int(result.get("job_id") or 0) <= 0:
            self._record_enqueue_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="enqueue_failed",
                outcome="fail",
                reason="enqueue_rejected",
                payload={"dedupe_key": dedupe_key, "content_kind": "text"},
            )
            return {
                "ok": False,
                "job_id": int(result.get("job_id") or 0),
                "created": bool(result.get("created")),
                "reused": bool(result.get("reused")),
                "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
                "state": str(result.get("state") or "").strip(),
            }
        if bool(result.get("reused")):
            self._record_enqueue_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="enqueue_reused",
                outcome="success",
                reason="dedupe_reused",
                payload={
                    "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
                    "content_kind": "text",
                    "job_id": int(result.get("job_id") or 0),
                },
            )
        return {
            "ok": True,
            "job_id": int(result.get("job_id") or 0),
            "created": bool(result.get("created")),
            "reused": bool(result.get("reused")),
            "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
            "state": str(result.get("state") or "queued").strip() or "queued",
        }
=======
    ) -> bool:
        if self._has_pending_dedupe(dedupe_key):
            return False
        local_id = self._sender.queue_message(
            thread_key,
            text,
            job_type=job_type,
            metadata={
                "dedupe_key": dedupe_key,
                **dict(metadata or {}),
            },
        )
        return bool(local_id)
>>>>>>> origin/main

    def enqueue_pack(
        self,
        thread_key: str,
        pack_id: str,
        *,
        job_type: str,
        dedupe_key: str,
        metadata: dict[str, Any] | None = None,
<<<<<<< HEAD
    ) -> dict[str, Any]:
        enqueue_fn = getattr(self._sender, "enqueue_pack_job", None)
        result: dict[str, Any]
        if callable(enqueue_fn):
            raw_result = enqueue_fn(
                thread_key,
                pack_id,
                job_type=job_type,
                dedupe_key=dedupe_key,
                metadata=dict(metadata or {}),
            )
            result = dict(raw_result or {}) if isinstance(raw_result, dict) else {}
        else:
            enqueue_ok = bool(
                self._sender.queue_pack(
                    thread_key,
                    pack_id,
                    job_type=job_type,
                    metadata={"dedupe_key": dedupe_key, **dict(metadata or {})},
                )
            )
            result = {
                "ok": enqueue_ok,
                "job_id": 0,
                "created": enqueue_ok,
                "reused": False,
                "dedupe_key": dedupe_key,
                "state": "queued" if enqueue_ok else "",
            }
        if not bool(result.get("ok")) or int(result.get("job_id") or 0) <= 0:
            self._record_enqueue_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="enqueue_failed",
                outcome="fail",
                reason="enqueue_rejected",
                payload={"dedupe_key": dedupe_key, "content_kind": "pack", "pack_id": pack_id},
            )
            return {
                "ok": False,
                "job_id": int(result.get("job_id") or 0),
                "created": bool(result.get("created")),
                "reused": bool(result.get("reused")),
                "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
                "state": str(result.get("state") or "").strip(),
            }
        if bool(result.get("reused")):
            self._record_enqueue_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="enqueue_reused",
                outcome="success",
                reason="dedupe_reused",
                payload={
                    "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
                    "content_kind": "pack",
                    "pack_id": pack_id,
                    "job_id": int(result.get("job_id") or 0),
                },
            )
        return {
            "ok": True,
            "job_id": int(result.get("job_id") or 0),
            "created": bool(result.get("created")),
            "reused": bool(result.get("reused")),
            "dedupe_key": str(result.get("dedupe_key") or dedupe_key).strip(),
            "state": str(result.get("state") or "queued").strip() or "queued",
        }
=======
    ) -> bool:
        if self._has_pending_dedupe(dedupe_key):
            return False
        return bool(
            self._sender.queue_pack(
                thread_key,
                pack_id,
                job_type=job_type,
                metadata={"dedupe_key": dedupe_key, **dict(metadata or {})},
            )
        )
>>>>>>> origin/main

    def _has_pending_dedupe(self, dedupe_key: str) -> bool:
        clean_dedupe = str(dedupe_key or "").strip()
        if not clean_dedupe:
            return False
        for job in self._store.list_send_queue_jobs(states=["queued", "processing"], limit=500):
            if str(job.get("dedupe_key") or "").strip() == clean_dedupe:
                return True
        return False


class InboxAutomationRuntime:
<<<<<<< HEAD
    _EVALUATE_STARTED_EVENT = "automation_evaluate_started"
    _EVALUATE_COMPLETED_EVENT = "automation_evaluate_completed"
    _ENQUEUE_ATTEMPT_EVENT = "automation_enqueue_attempt"
    _ENQUEUE_RESULT_EVENT = "automation_enqueue_result"

=======
>>>>>>> origin/main
    def __init__(
        self,
        *,
        store: Any,
        sender: Any,
        ensure_backend_started,
    ) -> None:
        self._store = store
        self._sender = sender
        self._ensure_backend_started = ensure_backend_started
        self._router = OwnershipRouter()
        self._engine = AutomationEngineAdapter()
        self._send_queue = RuntimeSendQueue(store=store, sender=sender)
        self._connector = SessionConnectorRegistry(
            account_resolver=self._get_account,
            store=store,
        )

<<<<<<< HEAD
    def _record_diagnostic(
        self,
        *,
        account_id: str = "",
        alias_id: str = "",
        thread_key: str = "",
        job_type: str = "",
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
            thread_key=thread_key,
            job_type=job_type,
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload=payload,
            callsite_skip=2,
        )

=======
>>>>>>> origin/main
    def list_alias_accounts(self, alias_id: str) -> list[dict[str, Any]]:
        clean_alias = str(alias_id or "").strip().lower()
        rows: list[dict[str, Any]] = []
        for raw in accounts_module.list_all():
            if not isinstance(raw, dict):
                continue
<<<<<<< HEAD
            enabled_for_operation = getattr(accounts_module, "is_account_enabled_for_operation", None)
            if callable(enabled_for_operation):
                if not bool(enabled_for_operation(raw)):
                    continue
            elif not bool(raw.get("active", True)):
=======
            if not bool(raw.get("active", True)):
>>>>>>> origin/main
                continue
            alias = str(raw.get("alias") or "").strip().lower()
            username = str(raw.get("username") or "").strip().lstrip("@")
            if not clean_alias or alias != clean_alias or not username:
                continue
            rows.append(dict(raw))
        return rows

    def process_account_turn(self, account: dict[str, Any], *, mode: str) -> dict[str, Any]:
        account_id = str(account.get("username") or "").strip().lstrip("@").lower()
<<<<<<< HEAD
        alias_id = str(account.get("alias") or "").strip()
        if not account_id:
            self._record_diagnostic(
                event_type="process_account_turn_skipped",
                stage="process_account_turn",
                outcome="skip",
                reason="missing_account_id",
                payload={"mode": mode},
            )
=======
        if not account_id:
>>>>>>> origin/main
            return {"account_id": "", "touched_threads": 0, "queued_jobs": 0, "errors": 1}
        self._ensure_backend_started()
        self._connector.start(account_id)
        if not self._connector.is_ready(account_id):
            self._connector.mark_degraded(account_id, "storage_state_missing")
<<<<<<< HEAD
            self._record_diagnostic(
                account_id=account_id,
                alias_id=alias_id,
                event_type="process_account_turn_skipped",
                stage="process_account_turn",
                outcome="skip",
                reason="storage_state_missing",
                reason_code="storage_state_missing",
                payload={"mode": mode},
            )
=======
>>>>>>> origin/main
            return {"account_id": account_id, "touched_threads": 0, "queued_jobs": 0, "errors": 1}
        try:
            started_at = ConversationReader._account_started_at(account)
            self._store.prepare_account_session(
                account_id,
                session_marker=ConversationReader._account_session_marker(account_id),
                started_at=started_at,
            )
            rows = sync_account_threads_from_storage(
                account,
                thread_limit=40,
                message_limit=20,
                max_pages=1,
                timeout_seconds=12.0,
            )
            touched = self._store.apply_endpoint_threads(account, list(rows or []))
            queued_jobs = 0
            errors = 0
<<<<<<< HEAD
=======
            alias_id = str(account.get("alias") or "").strip()
>>>>>>> origin/main
            for thread_row in self._threads_for_account(account_id):
                thread_key = str(thread_row.get("thread_key") or "").strip()
                if not thread_key:
                    continue
                thread = self._store.get_thread(thread_key) or thread_row
<<<<<<< HEAD
                try:
                    base_updates = self._router.initialize_thread(thread)
                    base_updates["alias_id"] = alias_id
                    self._store.update_thread_record(thread_key, base_updates)
                    refreshed = self._store.get_thread(thread_key) or thread
                    if not self._router.can_automation_touch(refreshed):
                        self._record_diagnostic(
                            account_id=account_id,
                            alias_id=alias_id,
                            thread_key=thread_key,
                            event_type="evaluate_skipped",
                            stage="evaluate",
                            outcome="skip",
                            reason="automation_not_allowed",
                            payload={"mode": mode},
                        )
                        continue
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        event_type="evaluate_started",
                        stage="evaluate",
                        outcome="attempt",
                        reason="evaluate_started",
                        reason_code="evaluate_started",
                        payload=self._build_evaluate_context(refreshed),
                    )
                    self._store.add_thread_event(
                        thread_key,
                        self._EVALUATE_STARTED_EVENT,
                        account_id=account_id,
                        alias_id=alias_id,
                        payload=self._build_evaluate_context(refreshed),
                    )
                    evaluation = self._engine.evaluate_thread(account=account, thread=refreshed, mode=mode)
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        event_type="evaluate_completed",
                        stage="evaluate",
                        outcome="success",
                        reason="evaluate_completed",
                        reason_code="evaluate_completed",
                        payload=self._engine.describe_evaluation(evaluation),
                    )
                    self._store.add_thread_event(
                        thread_key,
                        self._EVALUATE_COMPLETED_EVENT,
                        account_id=account_id,
                        alias_id=alias_id,
                        payload=self._engine.describe_evaluation(evaluation),
                    )
                    if evaluation.get("thread_updates"):
                        self._store.update_thread_record(thread_key, dict(evaluation.get("thread_updates") or {}))
                    if evaluation.get("state_updates"):
                        self._store.update_thread_state(thread_key, dict(evaluation.get("state_updates") or {}))
                    result = self._apply_actions(
                        account=account,
                        thread=self._store.get_thread(thread_key) or refreshed,
                        actions=list(evaluation.get("actions") or []),
                    )
                    queued_jobs += int(result.get("queued_jobs") or 0)
                    errors += int(result.get("errors") or 0)
                except Exception as exc:
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        event_type="evaluate_failed",
                        stage="evaluate",
                        outcome="fail",
                        exception=exc,
                        payload={"mode": mode},
                    )
                    raise
        except Exception as exc:
            self._record_diagnostic(
                account_id=account_id,
                alias_id=alias_id,
                event_type="process_account_turn_failed",
                stage="process_account_turn",
                outcome="fail",
                exception=exc,
                payload={"mode": mode},
            )
=======
                base_updates = self._router.initialize_thread(thread)
                base_updates["alias_id"] = alias_id
                self._store.update_thread_record(thread_key, base_updates)
                refreshed = self._store.get_thread(thread_key) or thread
                if not self._router.can_automation_touch(refreshed):
                    continue
                evaluation = self._engine.evaluate_thread(account=account, thread=refreshed, mode=mode)
                if evaluation.get("thread_updates"):
                    self._store.update_thread_record(thread_key, dict(evaluation.get("thread_updates") or {}))
                if evaluation.get("state_updates"):
                    self._store.update_thread_state(thread_key, dict(evaluation.get("state_updates") or {}))
                result = self._apply_actions(
                    account=account,
                    thread=self._store.get_thread(thread_key) or refreshed,
                    actions=list(evaluation.get("actions") or []),
                )
                queued_jobs += int(result.get("queued_jobs") or 0)
                errors += int(result.get("errors") or 0)
        except Exception as exc:
>>>>>>> origin/main
            self._connector.mark_degraded(account_id, f"{type(exc).__name__}: {exc}")
            raise
        self._connector.heartbeat(account_id, state="ready", last_error="")
        return {
            "account_id": account_id,
            "touched_threads": len(touched),
            "queued_jobs": queued_jobs,
            "errors": errors,
        }

    def _apply_actions(self, *, account: dict[str, Any], thread: dict[str, Any], actions: list[dict[str, Any]]) -> dict[str, Any]:
        account_id = str(account.get("username") or "").strip().lstrip("@").lower()
        alias_id = str(account.get("alias") or thread.get("alias_id") or "").strip()
        thread_key = str(thread.get("thread_key") or "").strip()
        queued_jobs = 0
        errors = 0
        for action in actions:
            action_type = str(action.get("type") or "").strip().lower()
            if action_type == "mark_qualified":
                self._store.update_thread_record(thread_key, self._router.mark_qualified(thread))
                self._store.add_thread_event(thread_key, QUALIFIED, account_id=account_id, alias_id=alias_id, payload={})
                thread = self._store.get_thread(thread_key) or thread
            elif action_type == "mark_disqualified":
                self._store.update_thread_record(thread_key, self._router.mark_disqualified(thread))
                self._store.add_thread_event(thread_key, DISQUALIFIED, account_id=account_id, alias_id=alias_id, payload={})
                thread = self._store.get_thread(thread_key) or thread
            elif action_type == "move_stage":
                stage_id = str(action.get("stage_id") or "").strip()
                if stage_id:
                    self._store.update_thread_record(thread_key, {"stage_id": stage_id})
                    self._store.add_thread_event(
                        thread_key,
                        STAGE_CHANGED,
                        account_id=account_id,
                        alias_id=alias_id,
                        payload={"stage_id": stage_id},
                    )
                    thread = self._store.get_thread(thread_key) or thread
            elif action_type == "schedule_followup":
                if self._router.can_followup_touch(thread):
                    self._store.update_thread_record(thread_key, {"status": "pending"})
            elif action_type == "send_text":
                job_type = str(action.get("job_type") or "auto_reply")
                if job_type == "followup" and not self._router.can_followup_touch(thread):
<<<<<<< HEAD
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_skipped",
                        stage="enqueue",
                        outcome="skip",
                        reason="followup_not_allowed",
                        payload={"action_type": action_type, "content_kind": "text"},
                    )
                    self._record_enqueue_result(
                        thread_key=thread_key,
                        account_id=account_id,
                        alias_id=alias_id,
                        action_type=action_type,
                        job_type=job_type,
                        content_kind="text",
                        success=False,
                        attempted=False,
                        reason="followup_not_allowed",
                    )
                    continue
                if job_type != "followup" and not self._router.can_automation_touch(thread):
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_skipped",
                        stage="enqueue",
                        outcome="skip",
                        reason="automation_not_allowed",
                        payload={"action_type": action_type, "content_kind": "text"},
                    )
                    self._record_enqueue_result(
                        thread_key=thread_key,
                        account_id=account_id,
                        alias_id=alias_id,
                        action_type=action_type,
                        job_type=job_type,
                        content_kind="text",
                        success=False,
                        attempted=False,
                        reason="automation_not_allowed",
                    )
                    continue
                dedupe_key = self._dedupe_key_for_action(
                    thread,
                    action=action,
                    job_type=job_type,
                    content_kind="text",
                )
                self._record_enqueue_attempt(
                    thread_key=thread_key,
                    account_id=account_id,
                    alias_id=alias_id,
                    action_type=action_type,
                    job_type=job_type,
                    content_kind="text",
                )
                try:
                    enqueue_result = self._send_queue.enqueue_text(
                        thread_key,
                        str(action.get("text") or "").strip(),
                        job_type=job_type,
                        dedupe_key=dedupe_key,
                        metadata={
                            "post_send_thread_updates": dict(action.get("post_send_thread_updates") or {}),
                            "post_send_state_updates": dict(action.get("post_send_state_updates") or {}),
                        },
                    )
                except Exception as exc:
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_failed",
                        stage="enqueue",
                        outcome="fail",
                        exception=exc,
                        payload={"action_type": action_type, "content_kind": "text"},
                    )
                    raise
                enqueue_ok = bool(enqueue_result.get("ok")) and int(enqueue_result.get("job_id") or 0) > 0
                if not enqueue_ok:
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_failed",
                        stage="enqueue",
                        outcome="fail",
                        reason="enqueue_rejected",
                        payload={"action_type": action_type, "content_kind": "text"},
                    )
                elif action.get("enqueue_state_updates"):
                    self._store.update_thread_state(thread_key, dict(action.get("enqueue_state_updates") or {}))
                self._record_enqueue_result(
                    thread_key=thread_key,
                    account_id=account_id,
                    alias_id=alias_id,
                    action_type=action_type,
                    job_type=job_type,
                    content_kind="text",
                    success=enqueue_ok,
                    attempted=True,
                    job_id=int(enqueue_result.get("job_id") or 0),
                    created=bool(enqueue_result.get("created")),
                    reused=bool(enqueue_result.get("reused")),
                    dedupe_key=str(enqueue_result.get("dedupe_key") or dedupe_key).strip(),
                )
                if enqueue_ok:
                    if bool(enqueue_result.get("created")):
                        queued_jobs += 1
                        self._store.add_thread_event(
                            thread_key,
                            queued_thread_event(job_type),
                            account_id=account_id,
                            alias_id=alias_id,
                            payload={
                                "job_type": job_type,
                                "content_kind": "text",
                                "job_id": int(enqueue_result.get("job_id") or 0),
                            },
                        )
            elif action_type == "send_pack":
                job_type = str(action.get("job_type") or "auto_reply")
                if job_type == "followup" and not self._router.can_followup_touch(thread):
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_skipped",
                        stage="enqueue",
                        outcome="skip",
                        reason="followup_not_allowed",
                        payload={"action_type": action_type, "content_kind": "pack"},
                    )
                    self._record_enqueue_result(
                        thread_key=thread_key,
                        account_id=account_id,
                        alias_id=alias_id,
                        action_type=action_type,
                        job_type=job_type,
                        content_kind="pack",
                        success=False,
                        attempted=False,
                        reason="followup_not_allowed",
                    )
                    continue
                if job_type != "followup" and not self._router.can_automation_touch(thread):
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_skipped",
                        stage="enqueue",
                        outcome="skip",
                        reason="automation_not_allowed",
                        payload={"action_type": action_type, "content_kind": "pack"},
                    )
                    self._record_enqueue_result(
                        thread_key=thread_key,
                        account_id=account_id,
                        alias_id=alias_id,
                        action_type=action_type,
                        job_type=job_type,
                        content_kind="pack",
                        success=False,
                        attempted=False,
                        reason="automation_not_allowed",
                    )
                    continue
                pack_id = str(action.get("pack_id") or "").strip()
                dedupe_key = self._dedupe_key_for_action(
                    thread,
                    action=action,
                    job_type=job_type,
                    content_kind="pack",
                )
                self._record_enqueue_attempt(
                    thread_key=thread_key,
                    account_id=account_id,
                    alias_id=alias_id,
                    action_type=action_type,
                    job_type=job_type,
                    content_kind="pack",
                    pack_id=pack_id,
                )
                quota_reason, quota_deferral = self._pack_quota_deferral_for_action(
                    account=account,
                    thread=thread,
                    action=action,
                    pack_id=pack_id,
                    job_type=job_type,
                )
                if quota_reason:
                    if quota_deferral:
                        self._store.update_thread_state(
                            thread_key,
                            {
                                AutomationEngineAdapter.PACK_QUOTA_DEFERRAL_STATE_KEY: quota_deferral,
                            },
                        )
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_skipped",
                        stage="enqueue",
                        outcome="skip",
                        reason=quota_reason,
                        payload={
                            "action_type": action_type,
                            "content_kind": "pack",
                            "pack_id": pack_id,
                            "retry_after_ts": self._ts((quota_deferral or {}).get("retry_after_ts")),
                        },
                    )
                    self._record_enqueue_result(
                        thread_key=thread_key,
                        account_id=account_id,
                        alias_id=alias_id,
                        action_type=action_type,
                        job_type=job_type,
                        content_kind="pack",
                        success=False,
                        attempted=False,
                        reason=quota_reason,
                        pack_id=pack_id,
                    )
                    continue
                try:
                    enqueue_result = self._send_queue.enqueue_pack(
                        thread_key,
                        pack_id,
                        job_type=job_type,
                        dedupe_key=dedupe_key,
                        metadata={
                            "post_send_thread_updates": dict(action.get("post_send_thread_updates") or {}),
                            "post_send_state_updates": dict(action.get("post_send_state_updates") or {}),
                        },
                    )
                except Exception as exc:
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_failed",
                        stage="enqueue",
                        outcome="fail",
                        exception=exc,
                        payload={"action_type": action_type, "content_kind": "pack", "pack_id": pack_id},
                    )
                    raise
                enqueue_ok = bool(enqueue_result.get("ok")) and int(enqueue_result.get("job_id") or 0) > 0
                if not enqueue_ok:
                    self._record_diagnostic(
                        account_id=account_id,
                        alias_id=alias_id,
                        thread_key=thread_key,
                        job_type=job_type,
                        event_type="enqueue_failed",
                        stage="enqueue",
                        outcome="fail",
                        reason="enqueue_rejected",
                        payload={"action_type": action_type, "content_kind": "pack", "pack_id": pack_id},
                    )
                elif action.get("enqueue_state_updates"):
                    self._store.update_thread_state(thread_key, dict(action.get("enqueue_state_updates") or {}))
                self._record_enqueue_result(
                    thread_key=thread_key,
                    account_id=account_id,
                    alias_id=alias_id,
                    action_type=action_type,
                    job_type=job_type,
                    content_kind="pack",
                    success=enqueue_ok,
                    attempted=True,
                    pack_id=pack_id,
                    job_id=int(enqueue_result.get("job_id") or 0),
                    created=bool(enqueue_result.get("created")),
                    reused=bool(enqueue_result.get("reused")),
                    dedupe_key=str(enqueue_result.get("dedupe_key") or dedupe_key).strip(),
                )
                if enqueue_ok:
                    if bool(enqueue_result.get("created")):
                        queued_jobs += 1
                        self._store.add_thread_event(
                            thread_key,
                            queued_thread_event(job_type, is_pack=True),
                            account_id=account_id,
                            alias_id=alias_id,
                            payload={
                                "job_type": job_type,
                                "content_kind": "pack",
                                "pack_id": pack_id,
                                "job_id": int(enqueue_result.get("job_id") or 0),
                            },
                        )
=======
                    continue
                if job_type != "followup" and not self._router.can_automation_touch(thread):
                    continue
                latest_inbound_id = str(action.get("latest_inbound_id") or thread.get("last_inbound_id_seen") or "").strip()
                dedupe_key = f"{job_type}:{thread_key}:{latest_inbound_id or action.get('text')}"
                if self._send_queue.enqueue_text(
                    thread_key,
                    str(action.get("text") or "").strip(),
                    job_type=job_type,
                    dedupe_key=dedupe_key,
                    metadata={
                        "post_send_thread_updates": dict(action.get("post_send_thread_updates") or {}),
                        "post_send_state_updates": dict(action.get("post_send_state_updates") or {}),
                    },
                ):
                    queued_jobs += 1
                    self._store.add_thread_event(
                        thread_key,
                        queued_thread_event(job_type),
                        account_id=account_id,
                        alias_id=alias_id,
                        payload={"job_type": job_type, "content_kind": "text"},
                    )
            elif action_type == "send_pack":
                job_type = str(action.get("job_type") or "auto_reply")
                if job_type == "followup" and not self._router.can_followup_touch(thread):
                    continue
                if job_type != "followup" and not self._router.can_automation_touch(thread):
                    continue
                pack_id = str(action.get("pack_id") or "").strip()
                dedupe_key = f"{job_type}:{thread_key}:pack:{pack_id}:{action.get('latest_inbound_id') or thread.get('followup_level')}"
                if self._send_queue.enqueue_pack(
                    thread_key,
                    pack_id,
                    job_type=job_type,
                    dedupe_key=dedupe_key,
                    metadata={
                        "post_send_thread_updates": dict(action.get("post_send_thread_updates") or {}),
                        "post_send_state_updates": dict(action.get("post_send_state_updates") or {}),
                    },
                ):
                    queued_jobs += 1
                    self._store.add_thread_event(
                        thread_key,
                        queued_thread_event(job_type, is_pack=True),
                        account_id=account_id,
                        alias_id=alias_id,
                        payload={
                            "job_type": job_type,
                            "content_kind": "pack",
                            "pack_id": pack_id,
                        },
                    )
>>>>>>> origin/main
            else:
                errors += 1
        self._store.add_thread_event(
            thread_key,
            INBOUND_RECEIVED if thread.get("last_message_direction") == "inbound" else THREAD_UPDATED,
            account_id=account_id,
            alias_id=alias_id,
            payload={"queued_jobs": queued_jobs},
        )
        return {"queued_jobs": queued_jobs, "errors": errors}

<<<<<<< HEAD
    @staticmethod
    def _build_evaluate_context(thread: dict[str, Any]) -> dict[str, Any]:
        return {
            "thread_id": str(thread.get("thread_id") or "").strip(),
            "stage_id": str(thread.get("stage_id") or "").strip(),
            "owner": str(thread.get("owner") or "").strip(),
            "bucket": str(thread.get("bucket") or "").strip(),
        }

    @staticmethod
    def _dedupe_identity_for_reply(thread: dict[str, Any], *, action: dict[str, Any]) -> str:
        latest_inbound_id = str(action.get("latest_inbound_id") or "").strip()
        if latest_inbound_id:
            return latest_inbound_id
        last_direction = str(thread.get("last_message_direction") or "").strip().lower()
        last_message_id = str(thread.get("last_message_id") or "").strip()
        if last_direction == "inbound" and last_message_id:
            return last_message_id
        last_inbound_seen = str(thread.get("last_inbound_id_seen") or "").strip()
        if last_inbound_seen:
            return last_inbound_seen
        last_inbound_at = thread.get("last_inbound_at")
        return f"ts:{last_inbound_at}" if last_inbound_at not in {None, ""} else "reply"

    @staticmethod
    def _dedupe_identity_for_followup(thread: dict[str, Any]) -> str:
        stage_id = str(thread.get("stage_id") or "initial").strip() or "initial"
        followup_level = max(0, int(thread.get("followup_level") or 0))
        return f"{stage_id}:level:{followup_level}"

    @classmethod
    def _dedupe_key_for_action(
        cls,
        thread: dict[str, Any],
        *,
        action: dict[str, Any],
        job_type: str,
        content_kind: str,
    ) -> str:
        clean_job_type = str(job_type or "").strip().lower() or "manual_reply"
        clean_thread_key = str(thread.get("thread_key") or "").strip()
        clean_content_kind = str(content_kind or "").strip().lower() or "text"
        if clean_job_type == "followup":
            identity = cls._dedupe_identity_for_followup(thread)
        else:
            identity = cls._dedupe_identity_for_reply(thread, action=action)
        return f"{clean_job_type}:{clean_thread_key}:{clean_content_kind}:{identity}"

    def _record_enqueue_attempt(
        self,
        *,
        thread_key: str,
        account_id: str,
        alias_id: str,
        action_type: str,
        job_type: str,
        content_kind: str,
        pack_id: str = "",
    ) -> None:
        payload = {
            "action_type": str(action_type or "").strip().lower(),
            "job_type": str(job_type or "").strip().lower(),
            "content_kind": str(content_kind or "").strip().lower(),
        }
        clean_pack_id = str(pack_id or "").strip()
        if clean_pack_id:
            payload["pack_id"] = clean_pack_id
        self._store.add_thread_event(
            thread_key,
            self._ENQUEUE_ATTEMPT_EVENT,
            account_id=account_id,
            alias_id=alias_id,
            payload=payload,
        )

    def _record_enqueue_result(
        self,
        *,
        thread_key: str,
        account_id: str,
        alias_id: str,
        action_type: str,
        job_type: str,
        content_kind: str,
        success: bool,
        attempted: bool,
        reason: str = "",
        pack_id: str = "",
        job_id: int = 0,
        created: bool = False,
        reused: bool = False,
        dedupe_key: str = "",
    ) -> None:
        payload = {
            "action_type": str(action_type or "").strip().lower(),
            "job_type": str(job_type or "").strip().lower(),
            "content_kind": str(content_kind or "").strip().lower(),
            "attempted": bool(attempted),
            "success": bool(success),
            "created": bool(created),
            "reused": bool(reused),
        }
        clean_reason = str(reason or "").strip()
        clean_pack_id = str(pack_id or "").strip()
        clean_dedupe_key = str(dedupe_key or "").strip()
        if int(job_id or 0) > 0:
            payload["job_id"] = int(job_id or 0)
        if clean_reason:
            payload["reason"] = clean_reason
        if clean_pack_id:
            payload["pack_id"] = clean_pack_id
        if clean_dedupe_key:
            payload["dedupe_key"] = clean_dedupe_key
        self._store.add_thread_event(
            thread_key,
            self._ENQUEUE_RESULT_EVENT,
            account_id=account_id,
            alias_id=alias_id,
            payload=payload,
        )

=======
>>>>>>> origin/main
    def _threads_for_account(self, account_id: str) -> list[dict[str, Any]]:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        return [
            row
            for row in self._store.list_threads("all")
            if str(row.get("account_id") or "").strip().lower() == clean_account
        ]

    @staticmethod
    def _get_account(account_id: str) -> dict[str, Any] | None:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        for raw in accounts_module.list_all():
            if not isinstance(raw, dict):
                continue
            username = str(raw.get("username") or "").strip().lstrip("@").lower()
            if username == clean_account:
                return dict(raw)
        return None
<<<<<<< HEAD

    @staticmethod
    def _pack_sendable_action_count(action: dict[str, Any], pack_id: str) -> int:
        try:
            inline_count_hint = int(action.get("pack_sendable_actions") or 0)
        except Exception:
            inline_count_hint = 0
        if inline_count_hint > 0:
            return inline_count_hint
        inline_count = responder_module._pack_sendable_action_count(
            action.get("pack_actions") if isinstance(action.get("pack_actions"), list) else action.get("actions")
        )
        if inline_count > 0:
            return inline_count
        try:
            packs = responder_module._list_packs()
        except Exception:
            packs = []
        clean_pack_id = str(pack_id or "").strip()
        for pack in packs:
            if not isinstance(pack, dict):
                continue
            if str(pack.get("id") or "").strip() != clean_pack_id:
                continue
            return responder_module._pack_sendable_action_count(pack.get("actions"))
        return 0

    @staticmethod
    def _next_quota_retry_after_ts() -> float:
        now = datetime.now(tz=STORAGE_TZ)
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return float(next_midnight.timestamp())

    def _pack_quota_deferral_for_action(
        self,
        *,
        account: dict[str, Any],
        thread: dict[str, Any],
        action: dict[str, Any],
        pack_id: str,
        job_type: str,
    ) -> tuple[str, dict[str, Any] | None]:
        sendable_actions = self._pack_sendable_action_count(action, pack_id)
        if sendable_actions <= 0:
            return "", None
        account_id = str(account.get("username") or thread.get("account_id") or "").strip().lstrip("@")
        can_send, sent_today, limit = can_send_message_for_account(
            account=account,
            username=account_id,
            default=None,
        )
        if limit is None:
            return "", None
        remaining = max(0, int(limit or 0) - int(sent_today or 0))
        if can_send and remaining >= sendable_actions:
            return "", None
        reason = f"pack_quota_insufficient:{sent_today}/{limit}:need={sendable_actions}"
        deferred_at = self._ts(datetime.now(tz=STORAGE_TZ).timestamp()) or 0.0
        inbound_id = str(action.get("latest_inbound_id") or thread.get("pending_inbound_id") or "").strip()
        return reason, {
            "reason": reason,
            "pack_id": str(pack_id or "").strip(),
            "job_type": str(job_type or "").strip().lower() or "auto_reply",
            "inbound_id": inbound_id,
            "sendable_actions": int(sendable_actions),
            "sent_today": int(sent_today or 0),
            "limit": int(limit or 0),
            "remaining": int(remaining),
            "deferred_at": deferred_at,
            "retry_after_ts": self._next_quota_retry_after_ts(),
        }

    @staticmethod
    def _ts(value: Any) -> float | None:
        try:
            if value in {None, ""}:
                return None
            return float(value)
        except Exception:
            return None
=======
>>>>>>> origin/main
