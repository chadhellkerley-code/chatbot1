from __future__ import annotations

from typing import Any

from core import accounts as accounts_module
from core.inbox.conversation_reader import ConversationReader
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

    def enqueue_text(
        self,
        thread_key: str,
        text: str,
        *,
        job_type: str,
        dedupe_key: str,
        metadata: dict[str, Any] | None = None,
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

    def enqueue_pack(
        self,
        thread_key: str,
        pack_id: str,
        *,
        job_type: str,
        dedupe_key: str,
        metadata: dict[str, Any] | None = None,
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

    def _has_pending_dedupe(self, dedupe_key: str) -> bool:
        clean_dedupe = str(dedupe_key or "").strip()
        if not clean_dedupe:
            return False
        for job in self._store.list_send_queue_jobs(states=["queued", "processing"], limit=500):
            if str(job.get("dedupe_key") or "").strip() == clean_dedupe:
                return True
        return False


class InboxAutomationRuntime:
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

    def list_alias_accounts(self, alias_id: str) -> list[dict[str, Any]]:
        clean_alias = str(alias_id or "").strip().lower()
        rows: list[dict[str, Any]] = []
        for raw in accounts_module.list_all():
            if not isinstance(raw, dict):
                continue
            if not bool(raw.get("active", True)):
                continue
            alias = str(raw.get("alias") or "").strip().lower()
            username = str(raw.get("username") or "").strip().lstrip("@")
            if not clean_alias or alias != clean_alias or not username:
                continue
            rows.append(dict(raw))
        return rows

    def process_account_turn(self, account: dict[str, Any], *, mode: str) -> dict[str, Any]:
        account_id = str(account.get("username") or "").strip().lstrip("@").lower()
        if not account_id:
            return {"account_id": "", "touched_threads": 0, "queued_jobs": 0, "errors": 1}
        self._ensure_backend_started()
        self._connector.start(account_id)
        if not self._connector.is_ready(account_id):
            self._connector.mark_degraded(account_id, "storage_state_missing")
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
            alias_id = str(account.get("alias") or "").strip()
            for thread_row in self._threads_for_account(account_id):
                thread_key = str(thread_row.get("thread_key") or "").strip()
                if not thread_key:
                    continue
                thread = self._store.get_thread(thread_key) or thread_row
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
