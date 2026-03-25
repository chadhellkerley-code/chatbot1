from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import health_store
from PySide6.QtCore import QObject, Signal
from core.accounts import has_playwright_storage_state

import src.inbox.message_sender as message_sender_module
from src.inbox.endpoint_reader import read_thread_from_storage, sync_account_threads_from_storage

from .conversation_service import ConversationService


send_manual_message = message_sender_module.send_manual_message


class InboxManager(QObject):
    cache_updated = Signal(object)

    def __init__(self, root_dir: Path, *_, **__) -> None:
        super().__init__()
        self._root_dir = Path(root_dir)
        self._service = ConversationService(self._root_dir, notifier=self._emit_cache_updated)
        self._store = self._service._store
        # Keep the legacy storage handle available for compatibility tests and helpers.
        self._storage = self._store._storage
        self._reader = self._service._reader
        self._sender = self._service._sender
        self._started = False
        self._force_refresh_requested = False

    @property
    def events(self) -> "InboxManager":
        return self

    def start(self) -> None:
        if self._started:
            return
        self._service.start()
        self._started = True

    def shutdown(self) -> None:
        if not self._started:
            self._service.shutdown()
            return
        self._started = False
        self._service.shutdown()

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        return self._service.list_threads(filter_mode)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        return self._service.get_thread(thread_key)

    def snapshot(self) -> dict[str, Any]:
        rows = self.list_threads("all")
        return {
            "threads": rows,
            "messages": {
                thread_key: list((self.get_thread(thread_key) or {}).get("messages") or [])
                for row in rows
                for thread_key in [str(row.get("thread_key") or "").strip()]
                if thread_key
            },
            "state": {
                "started": self._started,
            },
        }

    def set_foreground_active(self, active: bool) -> None:
        self._service.set_foreground_active(active)

    def prime_thread_snapshot(
        self,
        thread_row: dict[str, Any],
        *,
        messages: list[dict[str, Any]] | None = None,
    ) -> bool:
        row = dict(thread_row or {})
        thread_key = str(row.get("thread_key") or "").strip()
        if not thread_key:
            return False
        self._store.upsert_threads([row])
        if messages is not None:
            self._store.replace_messages(
                thread_key,
                [dict(item) for item in messages if isinstance(item, dict)],
                participants=list(row.get("participants") or []),
                mark_read=False,
            )
        self._emit_cache_updated(
            reason="prime_thread_snapshot",
            thread_keys=[thread_key],
            account_ids=[row.get("account_id")],
        )
        return True

    def list_packs(self) -> list[dict[str, Any]]:
        self.start()
        return self._service.list_packs()

    def open_thread(self, thread_key: str) -> bool:
        self.start()
        return self._service.open_thread(thread_key)

    def hydrate_thread(self, thread_key: str) -> bool:
        self.start()
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        self._task_read_thread({"thread_key": clean_key})
        return True

    def send_message(self, thread_key: str, text: str) -> str:
        self.start()
        return self._service.send_message(thread_key, text)

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        self.start()
        return self._service.send_pack(thread_key, pack_id)

    def request_ai_suggestion(self, thread_key: str) -> bool:
        self.start()
        return self._service.request_ai_suggestion(thread_key)

    def add_tag(self, thread_key: str, tag: str) -> bool:
        return self._service.add_tag(thread_key, tag)

    def mark_follow_up(self, thread_key: str) -> bool:
        return self._service.mark_follow_up(thread_key)

    def delete_conversation(self, thread_key: str) -> bool:
        return self._service.delete_conversation(thread_key)

    def delete_message_local(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        return self._service.delete_message_local(thread_key, message_ref)

    def enqueue_periodic_sync(self, *, force: bool = False) -> None:
        self._force_refresh_requested = self._force_refresh_requested or bool(force)
        self.start()
        self._service.enqueue_periodic_sync(force=force)

    def diagnostics(self) -> dict[str, Any]:
        payload = dict(self._service.diagnostics() or {})
        payload.setdefault("thread_count", int(payload.get("thread_count") or 0))
        payload.setdefault("message_groups", int(payload.get("message_groups") or 0))
        payload["queued_tasks"] = int(payload.get("sender_queue_size") or 0)
        payload["worker_count"] = 2 + int(bool(payload.get("worker_ready")))
        payload["dedupe_pending"] = 0
        payload["reader_active_tasks"] = 1 if bool(payload.get("running")) else 0
        payload["sender_active_tasks"] = 1 if str(payload.get("sender_active_task") or "").strip() else 0
        return payload

    def _active_accounts(self) -> list[dict[str, Any]]:
        return self._service._active_accounts()

    def _get_account(self, account_id: str) -> dict[str, Any] | None:
        return self._service._get_account(account_id)

    def _account_profile_ready(self, account_id: str) -> bool:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return False
        return has_playwright_storage_state(clean_account)

    def _map_badge_to_health(self, account: dict[str, Any]) -> tuple[str, str]:
        account_id = str(account.get("username") or "").strip().lstrip("@")
        badge, expired = health_store.get_badge(account_id)
        badge_text = str(badge or "").strip()
        normalized = badge_text.upper()
        profile_ready = self._account_profile_ready(account_id)
        if normalized == health_store.HEALTH_STATE_DEAD:
            return "banned", badge_text
        if normalized == health_store.HEALTH_STATE_INACTIVE:
            return "login_required", badge_text
        if not profile_ready:
            return "login_required", "storage_state_missing"
        if normalized == health_store.HEALTH_STATE_ALIVE and not expired:
            return "healthy", badge_text
        return "healthy", ""

    def _account_can_refresh(self, account_id: str) -> bool:
        health = self._storage.get_account_health(account_id)
        state = str((health or {}).get("state") or "").strip().lower()
        if state in {"banned", "suspended"}:
            return False
        account = self._get_account(account_id)
        if isinstance(account, dict):
            return True
        return self._account_profile_ready(account_id)

    def _sync_external_health(self, accounts: list[dict[str, Any]]) -> None:
        for account in accounts:
            account_id = str(account.get("username") or "").strip().lstrip("@").lower()
            if not account_id:
                continue
            state, reason = self._map_badge_to_health(account)
            self._storage.set_account_health(account_id, state, reason=reason)

    def _task_send_message(self, payload: dict[str, Any]) -> None:
        thread_key = str(payload.get("thread_key") or "").strip()
        local_message_id = str(payload.get("local_message_id") or "").strip()
        text = str(payload.get("text") or "").strip()
        job_id = int(payload.get("job_id") or 0)
        thread = self.get_thread(thread_key)
        if not isinstance(thread, dict) or not local_message_id or not text:
            return
        account_id = str(thread.get("account_id") or "").strip()
        account = self._get_account(account_id) or {"username": account_id}
        result = send_manual_message(None, account, thread, text)
        if bool(result.get("ok", False)):
            sent_timestamp = float(result.get("timestamp") or time.time())
            self._storage.resolve_local_outbound(
                thread_key,
                local_message_id,
                final_message_id=str(result.get("message_id") or ""),
                sent_timestamp=sent_timestamp,
            )
            if job_id > 0:
                self._storage.update_send_queue_job(job_id, state="sent")
            self._storage.update_thread_state(
                thread_key,
                {
                    "sender_status": "ready",
                    "sender_error": "",
                    "thread_status": "ready",
                    "thread_error": "",
                    "ui_status": "active",
                    "last_message": text,
                    "last_activity_timestamp": sent_timestamp,
                },
            )
            self._storage.set_account_health(account_id, "healthy", reason="")
            self._emit_cache_updated(reason="send_message_success", thread_keys=[thread_key], account_ids=[account_id])
            return
        reason = str(result.get("reason") or "send_failed").strip()
        state, health_reason = self._sender._classify_health_from_error(reason)
        if state != "unknown":
            self._storage.set_account_health(account_id, state, reason=health_reason)
        self._storage.resolve_local_outbound(thread_key, local_message_id, error_message=reason)
        if job_id > 0:
            self._storage.update_send_queue_job(job_id, state="error", error_message=reason)
        self._storage.update_thread_state(
            thread_key,
            {
                "sender_status": "failed",
                "sender_error": reason,
                "thread_error": reason,
                "ui_status": "error",
            },
        )
        self._emit_cache_updated(reason="send_message_failed", thread_keys=[thread_key], account_ids=[account_id])

    def _task_sync_account(self, payload: dict[str, Any]) -> None:
        account_id = str(payload.get("account_id") or "").strip().lstrip("@").lower()
        if not account_id or not self._account_can_refresh(account_id):
            return
        account = self._get_account(account_id) or {"username": account_id}
        try:
            started_at = self._reader._account_started_at(account)
            if started_at is not None:
                self._store.prepare_account_session(
                    account_id,
                    session_marker=self._reader._account_session_marker(account_id),
                    started_at=started_at,
                )
            rows = sync_account_threads_from_storage(
                account,
                thread_limit=50,
                message_limit=12,
                max_pages=1,
                timeout_seconds=12.0,
            )
        except Exception as exc:
            state, reason = self._reader._classify_reader_error(exc)
            if state != "unknown":
                self._storage.set_account_health(account_id, state, reason=reason)
            self._storage.register_account_sync(account_id, last_error=reason, thread_count=0)
            self._emit_cache_updated(reason="sync_account_error", thread_keys=[], account_ids=[account_id])
            return
        touched_keys = self._store.apply_endpoint_threads(account, list(rows or []))
        self._emit_cache_updated(reason="sync_account", thread_keys=touched_keys, account_ids=[account_id])

    def _task_read_thread(self, payload: dict[str, Any]) -> None:
        thread_key = str(payload.get("thread_key") or "").strip()
        thread = self.get_thread(thread_key)
        if not isinstance(thread, dict):
            return
        account_id = str(thread.get("account_id") or "").strip().lstrip("@").lower()
        if not account_id or not self._account_can_refresh(account_id):
            return
        account = self._get_account(account_id) or {"username": account_id}
        try:
            result = read_thread_from_storage(
                account,
                thread_id=str(thread.get("thread_id") or ""),
                thread_href=str(thread.get("thread_href") or ""),
                timeout_seconds=12.0,
            )
        except Exception as exc:
            state, reason = self._reader._classify_reader_error(exc)
            if state != "unknown":
                self._storage.set_account_health(account_id, state, reason=reason)
            self._storage.update_thread_state(
                thread_key,
                {
                    "thread_status": "failed",
                    "thread_error": reason,
                },
            )
            self._emit_cache_updated(reason="read_thread_error", thread_keys=[thread_key], account_ids=[account_id])
            return
        self._store.replace_messages(
            thread_key,
            [dict(item) for item in result.get("messages") or [] if isinstance(item, dict)],
            seen_text=str(result.get("seen_text") or ""),
            seen_at=result.get("seen_at"),
            participants=list(result.get("participants") or []),
            mark_read=False,
        )
        self._storage.update_thread_state(
            thread_key,
            {
                "thread_status": "ready",
                "thread_error": "",
                "sender_status": "ready",
                "sender_error": "",
            },
        )
        self._storage.set_account_health(account_id, "healthy", reason="")
        self._emit_cache_updated(reason="read_thread", thread_keys=[thread_key], account_ids=[account_id])

    def _enqueue_refresh(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int,
        dedupe_key: str = "",
    ) -> bool:
        del priority, dedupe_key
        if task_type == "sync_account":
            self._task_sync_account(payload)
            return True
        if task_type == "read_thread":
            self._task_read_thread(payload)
            return True
        return False

    def _enqueue_outbound(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int,
        dedupe_key: str = "",
    ) -> bool:
        del dedupe_key
        if not hasattr(self._sender, "_enqueue"):
            return False
        self._sender._enqueue(task_type, dict(payload or {}), priority=priority)
        return True

    def _schedule_refresh_tasks(self) -> None:
        accounts = [dict(item) for item in self._active_accounts() if isinstance(item, dict)]
        self._sync_external_health(accounts)
        active_ids = {
            str(account.get("username") or "").strip().lstrip("@").lower()
            for account in accounts
            if str(account.get("username") or "").strip()
        }
        self._storage.prune_accounts(active_ids)
        rows = [dict(item) for item in self._storage.get_threads("all") if isinstance(item, dict)]
        for row in rows:
            thread_key = str(row.get("thread_key") or "").strip()
            if not thread_key:
                continue
            unread_count = int(row.get("unread_count") or 0)
            needs_reply = bool(row.get("needs_reply"))
            if unread_count > 0 or needs_reply:
                self._enqueue_refresh(
                    "read_thread",
                    {"thread_key": thread_key},
                    priority=10,
                    dedupe_key=f"read:{thread_key}",
                )
        should_queue_accounts = self._force_refresh_requested or bool(rows)
        for account in accounts:
            account_id = str(account.get("username") or "").strip().lstrip("@").lower()
            if not account_id or not should_queue_accounts or not self._account_can_refresh(account_id):
                continue
            self._enqueue_refresh(
                "sync_account",
                {"account_id": account_id},
                priority=40,
                dedupe_key=f"sync:{account_id}",
            )
        self._force_refresh_requested = False

    def _recover_persisted_outbound_jobs(self) -> None:
        jobs = self._storage.list_send_queue_jobs(states=["pending", "sending"], limit=200)
        for job in jobs:
            task_type = str(job.get("task_type") or "").strip().lower()
            payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
            job_id = int(job.get("id") or 0)
            if task_type in {"send_message", "manual_reply", "auto_reply", "followup"}:
                self._recover_send_message_job(job, payload, job_id)
                continue
            if task_type in {"send_pack", "manual_pack"}:
                self._storage.update_send_queue_job(job_id, state="pending")
                self._enqueue_outbound(
                    "send_pack",
                    {
                        "job_id": job_id,
                        "thread_key": str(job.get("thread_key") or payload.get("thread_key") or ""),
                        "pack_id": str(payload.get("pack_id") or ""),
                    },
                    priority=10,
                    dedupe_key=f"pack:{str(job.get('thread_key') or payload.get('thread_key') or '')}",
                )

    def _recover_send_message_job(self, job: dict[str, Any], payload: dict[str, Any], job_id: int) -> None:
        thread_key = str(job.get("thread_key") or payload.get("thread_key") or "").strip()
        local_message_id = str(payload.get("local_message_id") or "").strip()
        text = str(payload.get("text") or "").strip()
        if not thread_key or not local_message_id or not text:
            return
        thread = self.get_thread(thread_key)
        if not isinstance(thread, dict):
            return
        account_id = str(thread.get("account_id") or job.get("account_id") or "").strip().lstrip("@").lower()
        state = str(job.get("state") or "").strip().lower()
        if state in {"sending", "processing"}:
            account = self._get_account(account_id)
            if isinstance(account, dict):
                local_row = next(
                    (
                        dict(item)
                        for item in thread.get("messages") or []
                        if isinstance(item, dict) and str(item.get("message_id") or "").strip() == local_message_id
                    ),
                    {},
                )
                reconcile = message_sender_module.reconcile_manual_message(
                    None,
                    account,
                    thread,
                    text,
                    sent_after_ts=local_row.get("timestamp"),
                )
                if bool(reconcile.get("ok", False)):
                    sent_timestamp = float(reconcile.get("timestamp") or time.time())
                    self._storage.resolve_local_outbound(
                        thread_key,
                        local_message_id,
                        final_message_id=str(reconcile.get("message_id") or ""),
                        sent_timestamp=sent_timestamp,
                    )
                    self._storage.update_send_queue_job(job_id, state="sent")
                    self._storage.update_thread_state(
                        thread_key,
                        {
                            "sender_status": "ready",
                            "sender_error": "",
                            "thread_error": "",
                            "ui_status": "active",
                            "last_activity_timestamp": sent_timestamp,
                        },
                    )
                    self._emit_cache_updated(
                        reason="send_message_recovered",
                        thread_keys=[thread_key],
                        account_ids=[account_id],
                    )
                    return
        self._storage.set_local_outbound_status(thread_key, local_message_id, status="pending")
        self._storage.update_send_queue_job(job_id, state="pending")
        self._storage.update_thread_state(
            thread_key,
            {
                "sender_status": "queued",
                "sender_error": "",
                "thread_error": "",
            },
        )
        self._enqueue_outbound(
            "send_message",
            {
                "job_id": job_id,
                "thread_key": thread_key,
                "text": text,
                "local_message_id": local_message_id,
            },
            priority=0,
            dedupe_key=f"msg:{thread_key}:{local_message_id}",
        )
        self._emit_cache_updated(reason="send_message_requeued", thread_keys=[thread_key], account_ids=[account_id])

    def _emit_cache_updated(
        self,
        *,
        reason: str,
        thread_keys: list[str] | None = None,
        account_ids: list[str] | None = None,
    ) -> None:
        self.cache_updated.emit(
            {
                "reason": str(reason or "").strip(),
                "thread_keys": [str(item or "").strip() for item in thread_keys or [] if str(item or "").strip()],
                "account_ids": [str(item or "").strip() for item in account_ids or [] if str(item or "").strip()],
                "updated_at": time.time(),
            }
        )
