from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from core import accounts as accounts_module
from core import responder as responder_module
from src.browser_telemetry import log_browser_stage
from src.inbox_diagnostics import normalize_reason_code, record_inbox_diagnostic
from src.inbox.message_sender import build_conversation_text
from src.runtime.ownership_router import OwnershipRouter
from src.runtime.runtime_events import failed_thread_event, queued_thread_event, sent_thread_event

from .browser_pool import BrowserPool
from .conversation_store import ConversationStore


@dataclass(order=True)
class _SenderTask:
    priority: int
    sequence: int
    task_type: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)


class ConversationSender:
    def __init__(
        self,
        store: ConversationStore,
        browser_pool: BrowserPool,
        *,
        notifier,
    ) -> None:
        self._store = store
        self._browser_pool = browser_pool
        self._notifier = notifier
        self._stop_event = threading.Event()
        self._queue: queue.PriorityQueue[_SenderTask] = queue.PriorityQueue()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._takeover_condition = threading.Condition(self._lock)
        self._sequence = 0
        self._prepare_generation = 0
        self._active_task = ""
        self._takeover_blocked_threads: set[str] = set()
        self._irreversible_send_threads: set[str] = set()
        self._router = OwnershipRouter()

    def _record_diagnostic(
        self,
        *,
        thread: dict[str, Any] | None = None,
        thread_key: str = "",
        job_type: str = "",
        event_type: str,
        stage: str,
        outcome: str,
        reason: str = "",
        reason_code: str = "",
        exception: BaseException | None = None,
        payload: dict[str, Any] | None = None,
        created_at: float | None = None,
    ) -> None:
        row = dict(thread or {})
        clean_thread_key = str(thread_key or row.get("thread_key") or "").strip()
        if not row and clean_thread_key:
            row = self._store.get_thread(clean_thread_key) or {}
        record_inbox_diagnostic(
            self._store,
            event_type=event_type,
            stage=stage,
            outcome=outcome,
            account_id=str(row.get("account_id") or "").strip(),
            alias_id=str(row.get("alias_id") or row.get("account_alias") or "").strip(),
            thread_key=clean_thread_key,
            job_type=job_type,
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload=payload,
            created_at=created_at,
            callsite_skip=2,
        )

    def _persist_last_send_attempt(
        self,
        *,
        thread: dict[str, Any],
        thread_key: str,
        account_id: str,
        job_id: int,
        job_type: str,
        attempted_at: float | None = None,
        outcome: str = "",
        reason_code: str = "",
    ) -> None:
        alias_id = str(thread.get("alias_id") or thread.get("account_alias") or "").strip()
        if not alias_id:
            return
        updates: dict[str, Any] = {
            "last_send_attempt_account_id": str(account_id or "").strip().lstrip("@").lower(),
            "last_send_attempt_thread_key": str(thread_key or "").strip(),
            "last_send_attempt_job_id": max(0, int(job_id or 0)),
            "last_send_attempt_job_type": str(job_type or "").strip().lower(),
        }
        if attempted_at is not None:
            updates["last_send_attempt_at"] = float(attempted_at)
        if outcome:
            updates["last_send_attempt_outcome"] = str(outcome or "").strip().lower()
        if reason_code:
            updates["last_send_attempt_reason_code"] = str(reason_code or "").strip().lower()
        try:
            self._store.upsert_runtime_alias_state(alias_id, updates)
        except Exception:
            return

    def _persist_last_send_outcome(
        self,
        *,
        thread: dict[str, Any],
        thread_key: str,
        account_id: str,
        job_id: int,
        job_type: str,
        at: float | None = None,
        outcome: str,
        reason: str = "",
        reason_code: str = "",
        exception: BaseException | None = None,
    ) -> None:
        alias_id = str(thread.get("alias_id") or thread.get("account_alias") or "").strip()
        if not alias_id:
            return
        clean_outcome = str(outcome or "").strip().lower()
        if clean_outcome not in {"sent", "failed", "cancelled"}:
            return
        clean_reason = str(reason or "").strip()
        clean_exception_type = type(exception).__name__ if exception is not None else ""
        clean_exception_message = str(exception or "").strip() if exception is not None else ""
        if len(clean_reason) > 500:
            clean_reason = clean_reason[:500].rstrip() + "…"
        if len(clean_exception_message) > 500:
            clean_exception_message = clean_exception_message[:500].rstrip() + "…"
        normalized_reason_code = normalize_reason_code(
            str(reason_code or clean_reason or "").strip(),
            exception=exception,
        )
        updates: dict[str, Any] = {
            "last_send_outcome": clean_outcome,
            "last_send_reason_code": str(normalized_reason_code or "").strip(),
            "last_send_reason": clean_reason,
            "last_send_account_id": str(account_id or "").strip().lstrip("@").lower(),
            "last_send_thread_key": str(thread_key or "").strip(),
            "last_send_job_id": max(0, int(job_id or 0)),
            "last_send_job_type": str(job_type or "").strip().lower(),
            "last_send_exception_type": clean_exception_type,
            "last_send_exception_message": clean_exception_message,
        }
        if at is not None:
            updates["last_send_at"] = float(at)
        try:
            self._store.upsert_runtime_alias_state(alias_id, updates)
        except Exception:
            return

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="inbox-conversation-sender",
            daemon=True,
        )
        self._thread.start()
        self._recover_jobs()

    def stop(self) -> None:
        self._stop_event.set()
        self._enqueue("stop", {}, priority=99)
        worker = self._thread
        self._thread = None
        if worker is not None:
            worker.join(timeout=3.0)
        self._browser_pool.shutdown()

    def diagnostics(self) -> dict[str, Any]:
        return {
            "sender_queue_size": int(self._queue.qsize()),
            "sender_active_task": self._active_task,
        }

    def cancel_pending_runtime_jobs(
        self,
        alias_id: str,
        *,
        job_types: list[str] | None = None,
        reason: str = "runtime_stopped",
    ) -> int:
        clean_alias = str(alias_id or "").strip().lower()
        normalized_job_types = {
            str(item or "").strip().lower()
            for item in (job_types or ["auto_reply", "followup"])
            if str(item or "").strip()
        }
        if not clean_alias or not normalized_job_types:
            return 0
        retained: list[_SenderTask] = []
        cancelled_tasks: list[_SenderTask] = []
        with self._lock:
            while True:
                try:
                    task = self._queue.get_nowait()
                except queue.Empty:
                    break
                if self._matches_runtime_task(task, alias_id=clean_alias, job_types=normalized_job_types):
                    cancelled_tasks.append(task)
                else:
                    retained.append(task)
                self._queue.task_done()
            for task in retained:
                self._queue.put(task)
        for task in cancelled_tasks:
            task_payload = dict(task.payload or {})
            task_thread_key = str(task_payload.get("thread_key") or "").strip()
            task_job_type = str(task_payload.get("job_type") or task.task_type or "").strip().lower()
            self._cancel_send_job(
                int(task_payload.get("job_id") or 0),
                self._store.get_thread(task_thread_key) or {"thread_key": task_thread_key},
                task_job_type,
                reason=reason,
                local_message_id=str(task_payload.get("local_message_id") or "").strip(),
                pack_id=str(task_payload.get("pack_id") or "").strip(),
            )
        return len(cancelled_tasks)

    def begin_manual_takeover(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._takeover_condition:
            self._takeover_blocked_threads.add(clean_key)
            while clean_key in self._irreversible_send_threads:
                self._takeover_condition.wait(timeout=0.05)

    def finish_manual_takeover(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        with self._takeover_condition:
            self._takeover_blocked_threads.discard(clean_key)
            self._takeover_condition.notify_all()

    def cancel_pending_thread_jobs(
        self,
        thread_key: str,
        *,
        job_types: list[str] | None = None,
        reason: str = "manual_takeover",
    ) -> int:
        clean_key = str(thread_key or "").strip()
        normalized_job_types = {
            str(item or "").strip().lower()
            for item in (job_types or ["auto_reply", "followup"])
            if str(item or "").strip()
        }
        if not clean_key or not normalized_job_types:
            return 0
        retained: list[_SenderTask] = []
        cancelled_tasks: list[_SenderTask] = []
        with self._lock:
            while True:
                try:
                    task = self._queue.get_nowait()
                except queue.Empty:
                    break
                if self._matches_thread_task(task, thread_key=clean_key, job_types=normalized_job_types):
                    cancelled_tasks.append(task)
                else:
                    retained.append(task)
                self._queue.task_done()
            for task in retained:
                self._queue.put(task)
        for task in cancelled_tasks:
            task_payload = dict(task.payload or {})
            task_job_type = str(task_payload.get("job_type") or task.task_type or "").strip().lower()
            self._cancel_send_job(
                int(task_payload.get("job_id") or 0),
                self._store.get_thread(clean_key) or {"thread_key": clean_key},
                task_job_type,
                reason=reason,
                local_message_id=str(task_payload.get("local_message_id") or "").strip(),
                pack_id=str(task_payload.get("pack_id") or "").strip(),
            )
        return len(cancelled_tasks)

    def prepare_thread(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        with self._lock:
            self._prepare_generation += 1
            generation = self._prepare_generation
        self._enqueue(
            "prepare",
            {"thread_key": clean_key, "generation": generation},
            priority=5,
        )
        return True

    def queue_message(
        self,
        thread_key: str,
        text: str,
        *,
        job_type: str = "manual_reply",
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        result = self.enqueue_message_job(
            thread_key,
            text,
            job_type=job_type,
            priority=priority,
            dedupe_key=str(dict(metadata or {}).get("dedupe_key") or "").strip(),
            metadata=metadata,
        )
        return str(result.get("local_message_id") or "").strip() if bool(result.get("ok")) else ""

    def enqueue_message_job(
        self,
        thread_key: str,
        text: str,
        *,
        job_type: str = "manual_reply",
        priority: int | None = None,
        dedupe_key: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_key = str(thread_key or "").strip()
        content = str(text or "").strip()
        if not clean_key or not content:
            return {"ok": False, "job_id": 0, "created": False, "reused": False, "dedupe_key": str(dedupe_key or "").strip()}
        thread = self._store.get_thread(clean_key)
        if not isinstance(thread, dict):
            return {"ok": False, "job_id": 0, "created": False, "reused": False, "dedupe_key": str(dedupe_key or "").strip()}
        source = "manual"
        if str(job_type or "").strip().lower() == "auto_reply":
            source = "auto"
        elif str(job_type or "").strip().lower() == "followup":
            source = "followup"
        clean_metadata = dict(metadata or {})
        clean_dedupe = str(dedupe_key or clean_metadata.get("dedupe_key") or "").strip()
        local_id = f"local-{time.time_ns()}"
        enqueue_result = self._store.enqueue_send_queue_job(
            job_type,
            thread_key=clean_key,
            account_id=str(thread.get("account_id") or "").strip(),
            payload={
                "thread_key": clean_key,
                "text": content,
                "local_message_id": local_id,
                **clean_metadata,
            },
            dedupe_key=clean_dedupe,
            priority=priority,
        )
        job_id = int(enqueue_result.get("job_id") or 0)
        if job_id <= 0:
            return {
                "ok": False,
                "job_id": 0,
                "created": False,
                "reused": False,
                "dedupe_key": clean_dedupe,
            }
        if bool(enqueue_result.get("reused")):
            existing_payload = dict(enqueue_result.get("payload") or {})
            return {
                "ok": True,
                "job_id": job_id,
                "created": False,
                "reused": True,
                "dedupe_key": clean_dedupe,
                "state": str(enqueue_result.get("state") or "queued").strip(),
                "local_message_id": str(existing_payload.get("local_message_id") or "").strip(),
            }
        local_message = self._store.append_local_outbound_message(
            clean_key,
            content,
            source=source,
            local_message_id=local_id,
        )
        if not isinstance(local_message, dict):
            failure_reason = "local_echo_not_created"
            self._store.update_send_queue_job(
                job_id,
                state="cancelled",
                error_message=failure_reason,
                failure_reason=failure_reason,
                finished_at=time.time(),
            )
            return {
                "ok": False,
                "job_id": job_id,
                "created": True,
                "reused": False,
                "dedupe_key": clean_dedupe,
                "state": "cancelled",
                "local_message_id": local_id,
            }
        queued_at = local_message.get("timestamp")
        self._store.update_thread_state(
            clean_key,
            {
                "sender_status": "queued",
                "sender_error": "",
                "thread_error": "",
                "last_message": content,
                "last_activity_timestamp": queued_at or time.time(),
                "ui_status": "active",
            },
        )
        self._enqueue(
            str(job_type or "manual_reply").strip(),
            {
                "job_id": job_id,
                "thread_key": clean_key,
                "text": content,
                "local_message_id": local_id,
                "job_type": str(job_type or "manual_reply").strip(),
                "dedupe_key": clean_dedupe,
                **clean_metadata,
            },
            priority=self._job_priority(job_type, override=priority),
        )
        clean_job_type = str(job_type or "manual_reply").strip().lower() or "manual_reply"
        if clean_job_type == "manual_reply":
            self._store.add_thread_event(
                clean_key,
                queued_thread_event(clean_job_type),
                account_id=str(thread.get("account_id") or "").strip(),
                alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
                payload={"job_type": clean_job_type, "content_kind": "text", "local_message_id": local_id},
                created_at=queued_at if isinstance(queued_at, (int, float)) else None,
            )
        self._notifier(
            reason="send_message_queued",
            thread_keys=[clean_key],
            account_ids=[str(thread.get("account_id") or "").strip()],
        )
        return {
            "ok": True,
            "job_id": job_id,
            "created": True,
            "reused": False,
            "dedupe_key": clean_dedupe,
            "state": "queued",
            "local_message_id": local_id,
        }

    def queue_pack(
        self,
        thread_key: str,
        pack_id: str,
        *,
        job_type: str = "manual_pack",
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        result = self.enqueue_pack_job(
            thread_key,
            pack_id,
            job_type=job_type,
            priority=priority,
            dedupe_key=str(dict(metadata or {}).get("dedupe_key") or "").strip(),
            metadata=metadata,
        )
        return bool(result.get("ok"))

    def enqueue_pack_job(
        self,
        thread_key: str,
        pack_id: str,
        *,
        job_type: str = "manual_pack",
        priority: int | None = None,
        dedupe_key: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_key = str(thread_key or "").strip()
        clean_pack = str(pack_id or "").strip()
        if not clean_key or not clean_pack:
            return {"ok": False, "job_id": 0, "created": False, "reused": False, "dedupe_key": str(dedupe_key or "").strip()}
        thread = self._store.get_thread(clean_key)
        if not isinstance(thread, dict):
            return {"ok": False, "job_id": 0, "created": False, "reused": False, "dedupe_key": str(dedupe_key or "").strip()}
        pack = self._pack_by_id(clean_pack)
        if not isinstance(pack, dict):
            return {"ok": False, "job_id": 0, "created": False, "reused": False, "dedupe_key": str(dedupe_key or "").strip()}
        clean_metadata = dict(metadata or {})
        clean_dedupe = str(dedupe_key or clean_metadata.get("dedupe_key") or "").strip()
        enqueue_result = self._store.enqueue_send_queue_job(
            job_type,
            thread_key=clean_key,
            account_id=str(thread.get("account_id") or "").strip(),
            payload={"thread_key": clean_key, "pack_id": clean_pack, **clean_metadata},
            dedupe_key=clean_dedupe,
            priority=priority,
        )
        job_id = int(enqueue_result.get("job_id") or 0)
        if job_id <= 0:
            return {
                "ok": False,
                "job_id": 0,
                "created": False,
                "reused": False,
                "dedupe_key": clean_dedupe,
            }
        if bool(enqueue_result.get("reused")):
            return {
                "ok": True,
                "job_id": job_id,
                "created": False,
                "reused": True,
                "dedupe_key": clean_dedupe,
                "state": str(enqueue_result.get("state") or "queued").strip(),
            }
        self._store.ensure_conversation_from_pack(
            account={"username": str(thread.get("account_id") or "").strip()},
            thread_row=thread,
            pack_name=str(pack.get("name") or clean_pack).strip(),
        )
        self._store.update_thread_state(
            clean_key,
            {
                "sender_status": "queued",
                "sender_error": "",
                "thread_error": "",
                "pack_status": "queued",
                "pack_error": "",
                "pack_id": clean_pack,
                "pack_name": str(pack.get("name") or clean_pack).strip(),
                "ui_status": "pack_sent",
                "last_activity_timestamp": time.time(),
            },
        )
        self._enqueue(
            str(job_type or "manual_pack").strip(),
            {
                "job_id": job_id,
                "thread_key": clean_key,
                "pack_id": clean_pack,
                "job_type": str(job_type or "manual_pack").strip(),
                "dedupe_key": clean_dedupe,
                **clean_metadata,
            },
            priority=self._job_priority(job_type, override=priority),
        )
        clean_job_type = str(job_type or "manual_pack").strip().lower() or "manual_pack"
        if clean_job_type == "manual_pack":
            self._store.add_thread_event(
                clean_key,
                queued_thread_event(clean_job_type, is_pack=True),
                account_id=str(thread.get("account_id") or "").strip(),
                alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
                payload={"job_type": clean_job_type, "content_kind": "pack", "pack_id": clean_pack},
            )
        self._notifier(
            reason="send_pack_queued",
            thread_keys=[clean_key],
            account_ids=[str(thread.get("account_id") or "").strip()],
        )
        return {
            "ok": True,
            "job_id": job_id,
            "created": True,
            "reused": False,
            "dedupe_key": clean_dedupe,
            "state": "queued",
        }

    @staticmethod
    def _job_payload_content_kind(payload: dict[str, Any] | None) -> str:
        clean_payload = dict(payload or {})
        if str(clean_payload.get("pack_id") or "").strip():
            return "pack"
        if (
            str(clean_payload.get("text") or "").strip()
            or str(clean_payload.get("local_message_id") or "").strip()
        ):
            return "text"
        return ""

    @staticmethod
    def _recovery_payload(job: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job.get("payload") or {})
        clean_thread_key = str(job.get("thread_key") or payload.get("thread_key") or "").strip()
        clean_job_type = str(job.get("job_type") or job.get("task_type") or payload.get("job_type") or "").strip()
        clean_dedupe = str(job.get("dedupe_key") or payload.get("dedupe_key") or "").strip()
        recovered = {
            **payload,
            "job_id": int(job.get("id") or payload.get("job_id") or 0),
            "thread_key": clean_thread_key,
            "job_type": clean_job_type,
        }
        if clean_dedupe:
            recovered["dedupe_key"] = clean_dedupe
        return recovered

    def queue_existing_job(self, job: dict[str, Any]) -> bool:
        if not isinstance(job, dict):
            return False
        job_type = str(job.get("job_type") or job.get("task_type") or "").strip()
        payload = self._recovery_payload(job)
        thread_key = str(payload.get("thread_key") or "").strip()
        content_kind = self._job_payload_content_kind(payload)
        if content_kind == "text" and job_type in {"manual_reply", "auto_reply", "followup"}:
            text = str(payload.get("text") or "").strip()
            local_message_id = str(payload.get("local_message_id") or "").strip()
            if not thread_key or not text or not local_message_id:
                return False
            self._enqueue(
                job_type,
                payload,
                priority=self._job_priority(job_type, override=job.get("priority")),
            )
            return True
        if content_kind == "pack" and job_type in {"manual_pack", "auto_reply", "followup"}:
            pack_id = str(payload.get("pack_id") or "").strip()
            if not thread_key or not pack_id:
                return False
            self._enqueue(
                job_type,
                payload,
                priority=self._job_priority(job_type, override=job.get("priority")),
            )
            return True
        return False

    def _recover_jobs(self) -> None:
        jobs = self._store.list_send_queue_jobs(states=["queued", "processing"], limit=200)
        for job in jobs:
            job_id = int(job.get("id") or 0)
            job_type = str(job.get("job_type") or job.get("task_type") or "").strip().lower()
            payload = self._recovery_payload(job)
            thread_key = str(payload.get("thread_key") or "").strip()
            local_message_id = str(payload.get("local_message_id") or "").strip()
            content_kind = self._job_payload_content_kind(payload)
            if content_kind == "text" and thread_key and local_message_id:
                self._store.set_local_outbound_status(thread_key, local_message_id, status="pending")
                self._store.update_thread_state(
                    thread_key,
                    {
                        "sender_status": "queued",
                        "sender_error": "",
                        "thread_error": "",
                    },
                )
            elif content_kind == "pack" and thread_key:
                self._store.update_thread_state(
                    thread_key,
                    {
                        "sender_status": "queued",
                        "sender_error": "",
                        "thread_error": "",
                        "pack_status": "queued",
                        "pack_error": "",
                    },
                )
            self._store.update_send_queue_job(job_id, state="queued")
            if self.queue_existing_job(job) and thread_key:
                thread = self._store.get_thread(thread_key) or {}
                self._notifier(
                    reason="send_pack_requeued" if content_kind == "pack" else "send_message_requeued",
                    thread_keys=[thread_key],
                    account_ids=[str(thread.get("account_id") or "").strip()],
                )
                continue
            failure_reason = "job_recovery_invalid_payload"
            self._store.update_send_queue_job(
                job_id,
                state="failed",
                error_message=failure_reason,
                failure_reason=failure_reason,
                finished_at=time.time(),
            )
            if thread_key:
                self._store.reconcile_send_queue_thread_state(thread_key)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if task.task_type == "stop":
                self._queue.task_done()
                return
            self._active_task = str(task.task_type or "").strip()
            try:
                if task.task_type == "prepare":
                    self._handle_prepare(task.payload)
                elif task.task_type in {"manual_reply", "auto_reply", "followup"} and str(task.payload.get("pack_id") or "").strip():
                    self._handle_send_pack(task.payload)
                elif task.task_type in {"manual_reply", "auto_reply", "followup"}:
                    self._handle_send_message(task.payload)
                elif task.task_type in {"manual_pack"}:
                    self._handle_send_pack(task.payload)
            except Exception as exc:
                payload = dict(task.payload or {})
                self._record_diagnostic(
                    thread_key=str(payload.get("thread_key") or "").strip(),
                    job_type=str(payload.get("job_type") or task.task_type or "").strip().lower(),
                    event_type="sender_loop_task_failed",
                    stage="sender_loop",
                    outcome="fail",
                    exception=exc,
                    payload={
                        "task_type": str(task.task_type or "").strip(),
                        "job_id": int(payload.get("job_id") or 0),
                    },
                )
                raise
            finally:
                self._active_task = ""
                self._queue.task_done()

    def _handle_prepare(self, payload: dict[str, Any]) -> None:
        thread_key = str(payload.get("thread_key") or "").strip()
        generation = int(payload.get("generation") or 0)
        if not thread_key or self._is_stale_prepare(generation):
            self._record_diagnostic(
                thread_key=thread_key,
                event_type="thread_open_skipped",
                stage="prepare",
                outcome="skip",
                reason="stale_prepare_generation" if thread_key else "invalid_thread",
                payload={"generation": generation},
            )
            return
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict):
            self._record_diagnostic(
                thread_key=thread_key,
                event_type="thread_open_skipped",
                stage="prepare",
                outcome="skip",
                reason="thread_missing",
                payload={"generation": generation},
            )
            return
        account_id = str(thread.get("account_id") or "").strip()
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            event_type="thread_open_started",
            stage="prepare",
            outcome="attempt",
            reason="thread_open_started",
            reason_code="thread_open_started",
            payload={"generation": generation},
        )
        self._store.update_thread_state(
            thread_key,
            {
                "thread_status": "opening",
                "thread_error": "",
                "sender_status": "preparing",
                "sender_error": "",
            },
        )
        self._notifier(reason="prepare_thread_started", thread_keys=[thread_key], account_ids=[account_id])
        try:
            result = self._browser_pool.prepare(thread)
        except Exception as exc:
            self._record_diagnostic(
                thread=thread,
                thread_key=thread_key,
                event_type="thread_open_failed",
                stage="prepare",
                outcome="fail",
                exception=exc,
                payload={"generation": generation},
            )
            raise
        if self._is_stale_prepare(generation):
            self._browser_pool.cancel()
            self._record_diagnostic(
                thread=thread,
                thread_key=thread_key,
                event_type="thread_open_skipped",
                stage="prepare",
                outcome="skip",
                reason="stale_prepare_generation",
                payload={"generation": generation, "cancelled_browser_pool": True},
            )
            return
        if bool(result.get("ok", False)):
            self._store.update_thread_state(
                thread_key,
                {
                    "thread_status": "ready",
                    "thread_error": "",
                    "sender_status": "ready",
                    "sender_error": "",
                    "sender_prepared_at": time.time(),
                },
            )
            self._notifier(reason="prepare_thread_ready", thread_keys=[thread_key], account_ids=[account_id])
            return
        reason = str(result.get("reason") or "prepare_failed").strip()
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            event_type="thread_open_failed",
            stage="prepare",
            outcome="fail",
            reason=reason,
            payload={"generation": generation},
        )
        self._store.update_thread_state(
            thread_key,
            {
                "thread_status": "failed",
                "thread_error": reason,
                "sender_status": "failed",
                "sender_error": reason,
            },
        )
        self._notifier(reason="prepare_thread_failed", thread_keys=[thread_key], account_ids=[account_id])

    def _handle_send_message(self, payload: dict[str, Any]) -> None:
        thread_key = str(payload.get("thread_key") or "").strip()
        local_message_id = str(payload.get("local_message_id") or "").strip()
        job_id = int(payload.get("job_id") or 0)
        text = str(payload.get("text") or "").strip()
        job_type = str(payload.get("job_type") or "manual_reply").strip().lower() or "manual_reply"
        post_send_thread_updates = dict(payload.get("post_send_thread_updates") or {})
        post_send_state_updates = dict(payload.get("post_send_state_updates") or {})
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict) or not text or not local_message_id:
            failure_reason = "invalid_send_payload"
            finished_at = time.time()
            if thread_key and local_message_id:
                self._store.resolve_local_outbound(thread_key, local_message_id, error_message=failure_reason)
            if int(job_id or 0) > 0:
                self._store.update_send_queue_job(
                    job_id,
                    state="failed",
                    error_message=failure_reason,
                    failure_reason=failure_reason,
                    finished_at=finished_at,
                )
            if thread_key:
                self._cleanup_auto_reply_pending_state(
                    thread_key,
                    job_type=job_type,
                    inbound_id_hint=self._pending_inbound_id_hint(payload),
                )
                self._store.reconcile_send_queue_thread_state(thread_key)
            self._record_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="send_skipped",
                stage="send",
                outcome="skip",
                reason=failure_reason,
                payload={"job_id": job_id, "local_message_id": local_message_id},
            )
            if thread_key:
                self._notifier(reason="send_message_failed", thread_keys=[thread_key], account_ids=[])
            return
        account_id = str(thread.get("account_id") or "").strip()
        log_browser_stage(
            component="inbox_message_sender",
            stage="sender_job_received",
            status="started",
            account=account_id,
            thread_id=str(thread.get("thread_id") or "").strip(),
            thread_key=thread_key,
            job_id=job_id,
            job_type=job_type,
        )
        can_send, _cancel_reason = self._validate_job_sendability(
            job_id,
            thread,
            job_type,
            local_message_id=local_message_id,
        )
        if not can_send:
            self._notifier(reason="send_message_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        self._store.set_local_outbound_status(thread_key, local_message_id, status="sending")
        self._store.update_send_queue_job(job_id, state="processing", started_at=time.time(), increment_attempt=True)
        self._store.update_thread_state(
            thread_key,
            {
                "sender_status": "sending",
                "sender_error": "",
            },
        )
        self._notifier(reason="send_message_sending", thread_keys=[thread_key], account_ids=[account_id])
        thread = self._store.get_thread(thread_key) or thread
        can_send, cancel_reason = self._validate_job_sendability(
            job_id,
            thread,
            job_type,
            local_message_id=local_message_id,
        )
        if not can_send:
            self._store.update_thread_state(
                thread_key,
                {
                    "sender_status": "ready",
                    "sender_error": cancel_reason,
                    "thread_error": "",
                },
            )
            self._notifier(reason="send_message_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        entered_irreversible_window = self._enter_irreversible_send_window(thread_key, job_type)
        if not entered_irreversible_window:
            cancel_reason = self._cancel_send_job(
                job_id,
                thread,
                job_type,
                reason="manual_takeover_pending",
                local_message_id=local_message_id,
            )
            self._store.update_thread_state(
                thread_key,
                {
                    "sender_status": "ready",
                    "sender_error": cancel_reason,
                    "thread_error": "",
                },
            )
            self._notifier(reason="send_message_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        attempted_at = time.time()
        self._persist_last_send_attempt(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            attempted_at=attempted_at,
            outcome="attempt",
            reason_code="send_attempt",
        )
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            job_type=job_type,
            event_type="send_attempt",
            stage="send",
            outcome="attempt",
            reason="send_attempt",
            reason_code="send_attempt",
            payload={"job_id": job_id, "local_message_id": local_message_id},
        )
        send_exception: BaseException | None = None
        try:
            try:
                try:
                    result = self._browser_pool.send_text(thread, text, job_type=job_type)
                except TypeError:
                    result = self._browser_pool.send_text(thread, text)
            except Exception as exc:
                send_exception = exc
                self._record_diagnostic(
                    thread=thread,
                    thread_key=thread_key,
                    job_type=job_type,
                    event_type="send_failed",
                    stage="send",
                    outcome="fail",
                    exception=exc,
                    payload={"job_id": job_id, "local_message_id": local_message_id},
                )
                result = {"ok": False, "reason": str(exc or "send_failed").strip() or "send_failed"}
        finally:
            self._leave_irreversible_send_window(thread_key, job_type)
        if bool(result.get("ok", False)):
            self._persist_last_send_attempt(
                thread=thread,
                thread_key=thread_key,
                account_id=account_id,
                job_id=job_id,
                job_type=job_type,
                outcome="success",
                reason_code=normalize_reason_code(str(result.get("reason") or "success").strip()),
            )
            message_id = str(result.get("item_id") or "").strip()
            sent_timestamp = float(result.get("timestamp") or time.time())
            self._store.resolve_local_outbound(
                thread_key,
                local_message_id,
                final_message_id=message_id,
                sent_timestamp=sent_timestamp,
            )
            self._store.update_send_queue_job(job_id, state="confirmed", finished_at=sent_timestamp)
            self._persist_last_send_outcome(
                thread=thread,
                thread_key=thread_key,
                account_id=account_id,
                job_id=job_id,
                job_type=job_type,
                at=sent_timestamp,
                outcome="sent",
                reason=str(result.get("reason") or "success").strip() or "success",
            )
            action_type = {
                "manual_reply": "manual_reply_sent",
                "auto_reply": "auto_reply_sent",
                "followup": "followup_sent",
            }.get(job_type, "manual_reply_sent")
            self._store.update_thread_state(
                thread_key,
                {
                    "sender_status": "ready",
                    "sender_error": "",
                    "thread_error": "",
                    "ui_status": "active",
                    "last_message": text,
                    "last_activity_timestamp": sent_timestamp,
                },
            )
            self._store.update_thread_record(
                thread_key,
                {
                    "last_outbound_at": sent_timestamp,
                    "last_action_type": action_type,
                    "last_action_at": sent_timestamp,
                    "status": "followup_sent" if job_type == "followup" else "replied",
                    **post_send_thread_updates,
                },
            )
            if post_send_state_updates:
                self._store.update_thread_state(thread_key, post_send_state_updates)
            self._store.record_action_memory(
                str(thread.get("thread_id") or "").strip(),
                account_id,
                action_type,
                source="inbox_crm",
            )
            responder_module._record_message_sent(
                account_id,
                str(thread.get("thread_id") or "").strip(),
                text,
                message_id=message_id,
                recipient_username=str(thread.get("recipient_username") or "").strip(),
                is_followup=job_type == "followup",
            )
            self._store.add_thread_event(
                thread_key,
                sent_thread_event(job_type),
                account_id=account_id,
                alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
                payload={
                    "job_type": job_type,
                    "content_kind": "text",
                    "message_id": message_id,
                    "text": text,
                    "confirmation_reason": str(result.get("reason") or "").strip(),
                },
                created_at=sent_timestamp,
            )
            self._store.set_account_health(account_id, "healthy", reason="")
            self._notifier(reason="send_message_success", thread_keys=[thread_key], account_ids=[account_id])
            return
        reason = str(result.get("reason") or "send_failed").strip()
        self._persist_last_send_attempt(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            outcome="fail",
            reason_code=normalize_reason_code(reason),
        )
        finished_at = time.time()
        self._persist_last_send_outcome(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            at=finished_at,
            outcome="failed",
            reason=reason,
            exception=send_exception,
        )
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            job_type=job_type,
            event_type="send_failed",
            stage="send",
            outcome="fail",
            reason=reason,
            payload={"job_id": job_id, "local_message_id": local_message_id},
        )
        health_state, health_reason = self._classify_health_from_error(reason)
        if health_state != "unknown":
            self._store.set_account_health(account_id, health_state, reason=health_reason)
        self._store.resolve_local_outbound(thread_key, local_message_id, error_message=reason)
        self._store.update_send_queue_job(job_id, state="failed", error_message=reason, failure_reason=reason, finished_at=finished_at)
        self._store.update_thread_state(
            thread_key,
            {
                "sender_status": "failed",
                "sender_error": reason,
                "thread_error": reason,
                "ui_status": "error",
            },
        )
        self._store.update_thread_record(
            thread_key,
            {
                "last_action_type": "send_failed",
                "last_action_at": time.time(),
                "status": "failed",
            },
        )
        self._cleanup_auto_reply_pending_state(
            thread_key,
            job_type=job_type,
            inbound_id_hint=self._pending_inbound_id_hint({"post_send_state_updates": post_send_state_updates}),
        )
        self._store.add_thread_event(
            thread_key,
            failed_thread_event(job_type),
            account_id=account_id,
            alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
            payload={"job_type": job_type, "content_kind": "text", "reason": reason},
            created_at=time.time(),
        )
        self._notifier(reason="send_message_failed", thread_keys=[thread_key], account_ids=[account_id])

    def _handle_send_pack(self, payload: dict[str, Any]) -> None:
        thread_key = str(payload.get("thread_key") or "").strip()
        pack_id = str(payload.get("pack_id") or "").strip()
        job_id = int(payload.get("job_id") or 0)
        job_type = str(payload.get("job_type") or "manual_pack").strip().lower() or "manual_pack"
        post_send_thread_updates = dict(payload.get("post_send_thread_updates") or {})
        post_send_state_updates = dict(payload.get("post_send_state_updates") or {})
        thread = self._store.get_thread(thread_key)
        pack = self._pack_by_id(pack_id)
        if not isinstance(thread, dict) or not isinstance(pack, dict):
            failure_reason = "invalid_pack_payload"
            finished_at = time.time()
            if int(job_id or 0) > 0:
                self._store.update_send_queue_job(
                    job_id,
                    state="failed",
                    error_message=failure_reason,
                    failure_reason=failure_reason,
                    finished_at=finished_at,
                )
            if thread_key:
                self._cleanup_auto_reply_pending_state(
                    thread_key,
                    job_type=job_type,
                    inbound_id_hint=self._pending_inbound_id_hint(payload),
                )
                self._store.reconcile_send_queue_thread_state(thread_key)
            self._record_diagnostic(
                thread_key=thread_key,
                job_type=job_type,
                event_type="send_skipped",
                stage="send",
                outcome="skip",
                reason=failure_reason,
                payload={"job_id": job_id, "pack_id": pack_id},
            )
            if thread_key:
                self._notifier(reason="send_pack_failed", thread_keys=[thread_key], account_ids=[])
            return
        account_id = str(thread.get("account_id") or "").strip()
        log_browser_stage(
            component="inbox_message_sender",
            stage="sender_job_received",
            status="started",
            account=account_id,
            thread_id=str(thread.get("thread_id") or "").strip(),
            thread_key=thread_key,
            job_id=job_id,
            job_type=job_type,
            content_kind="pack",
        )
        can_send, _cancel_reason = self._validate_job_sendability(job_id, thread, job_type, pack_id=pack_id)
        if not can_send:
            self._notifier(reason="send_pack_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        self._store.update_send_queue_job(job_id, state="processing", started_at=time.time(), increment_attempt=True)
        self._store.update_thread_state(
            thread_key,
            {
                "pack_status": "running",
                "pack_error": "",
                "sender_status": "sending",
                "sender_error": "",
            },
        )
        self._notifier(reason="send_pack_running", thread_keys=[thread_key], account_ids=[account_id])
        thread = self._store.get_thread(thread_key) or thread
        can_send, cancel_reason = self._validate_job_sendability(job_id, thread, job_type, pack_id=pack_id)
        if not can_send:
            self._store.update_thread_state(
                thread_key,
                {
                    "pack_status": "failed",
                    "pack_error": cancel_reason,
                    "sender_status": "ready",
                    "sender_error": cancel_reason,
                    "thread_error": "",
                },
            )
            self._notifier(reason="send_pack_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        entered_irreversible_window = self._enter_irreversible_send_window(thread_key, job_type)
        if not entered_irreversible_window:
            cancel_reason = self._cancel_send_job(
                job_id,
                thread,
                job_type,
                reason="manual_takeover_pending",
                pack_id=pack_id,
            )
            self._store.update_thread_state(
                thread_key,
                {
                    "pack_status": "failed",
                    "pack_error": cancel_reason,
                    "sender_status": "ready",
                    "sender_error": cancel_reason,
                    "thread_error": "",
                },
            )
            self._notifier(reason="send_pack_cancelled", thread_keys=[thread_key], account_ids=[account_id])
            return
        attempted_at = time.time()
        self._persist_last_send_attempt(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            attempted_at=attempted_at,
            outcome="attempt",
            reason_code="send_attempt",
        )
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            job_type=job_type,
            event_type="send_attempt",
            stage="send",
            outcome="attempt",
            reason="send_attempt",
            reason_code="send_attempt",
            payload={"job_id": job_id, "pack_id": pack_id, "content_kind": "pack"},
        )
        send_exception: BaseException | None = None
        try:
            try:
                try:
                    result = self._browser_pool.send_pack(
                        thread,
                        pack,
                        conversation_text=build_conversation_text(list(thread.get("messages") or []), limit=12),
                        flow_config=responder_module._flow_config_for_account(account_id),
                        job_type=job_type,
                    )
                except TypeError:
                    result = self._browser_pool.send_pack(
                        thread,
                        pack,
                        conversation_text=build_conversation_text(list(thread.get("messages") or []), limit=12),
                        flow_config=responder_module._flow_config_for_account(account_id),
                    )
            except Exception as exc:
                send_exception = exc
                self._record_diagnostic(
                    thread=thread,
                    thread_key=thread_key,
                    job_type=job_type,
                    event_type="send_failed",
                    stage="send",
                    outcome="fail",
                    exception=exc,
                    payload={"job_id": job_id, "pack_id": pack_id, "content_kind": "pack"},
                )
                result = {"ok": False, "reason": str(exc or "pack_failed").strip() or "pack_failed"}
        finally:
            self._leave_irreversible_send_window(thread_key, job_type)
        if bool(result.get("ok", False)):
            self._persist_last_send_attempt(
                thread=thread,
                thread_key=thread_key,
                account_id=account_id,
                job_id=job_id,
                job_type=job_type,
                outcome="success",
                reason_code=normalize_reason_code(str(result.get("reason") or "success").strip()),
            )
            sent_at = float(result.get("timestamp") or time.time())
            confirmed_message_id = str(result.get("item_id") or "").strip() or f"thread-read-confirmed-{time.time_ns()}"
            pack_preview_text = str(pack.get("name") or pack_id).strip()
            self._store.update_send_queue_job(job_id, state="confirmed", finished_at=sent_at)
            self._persist_last_send_outcome(
                thread=thread,
                thread_key=thread_key,
                account_id=account_id,
                job_id=job_id,
                job_type=job_type,
                at=sent_at,
                outcome="sent",
                reason=str(result.get("reason") or "success").strip() or "success",
            )
            action_type = "followup_sent" if job_type == "followup" else "manual_pack_sent"
            self._store.update_thread_state(
                thread_key,
                {
                    "pack_status": "done",
                    "pack_error": "",
                    "sender_status": "ready",
                    "sender_error": "",
                    "ui_status": "pack_sent",
                    "last_activity_timestamp": sent_at,
                    "pack_sent_at": sent_at,
                    "crm_relevant": True,
                },
            )
            self._store.record_action_memory(
                str(thread.get("thread_id") or "").strip(),
                account_id,
                action_type,
                pack_id=pack_id,
                source="inbox_crm",
            )
            self._store.add_thread_event(
                thread_key,
                sent_thread_event(job_type, is_pack=True),
                account_id=account_id,
                alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
                payload={
                    "job_type": job_type,
                    "content_kind": "pack",
                    "pack_id": pack_id,
                    "confirmation_reason": str(result.get("reason") or "").strip(),
                },
                created_at=sent_at,
            )
            refreshed_from_legacy = self._store.refresh_thread_from_legacy(thread_key)
            thread_record_updates = {
                "last_pack_sent": pack_id,
                "last_outbound_at": sent_at,
                "last_action_type": action_type,
                "last_action_at": sent_at,
                "unread_count": 0,
                "needs_reply": False,
                "status": "followup_sent" if job_type == "followup" else "pack_sent",
                **post_send_thread_updates,
            }
            if not refreshed_from_legacy:
                thread_record_updates.update(
                    {
                        "last_message_text": pack_preview_text,
                        "last_message_timestamp": sent_at,
                        "last_message_direction": "outbound",
                        "last_message_id": confirmed_message_id,
                    }
                )
            self._store.update_thread_record(thread_key, thread_record_updates)
            if post_send_state_updates:
                self._store.update_thread_state(thread_key, post_send_state_updates)
            self._notifier(reason="send_pack_success", thread_keys=[thread_key], account_ids=[account_id])
            return
        reason = str(result.get("reason") or result.get("error") or "pack_failed").strip()
        self._persist_last_send_attempt(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            outcome="fail",
            reason_code=normalize_reason_code(reason),
        )
        finished_at = time.time()
        self._persist_last_send_outcome(
            thread=thread,
            thread_key=thread_key,
            account_id=account_id,
            job_id=job_id,
            job_type=job_type,
            at=finished_at,
            outcome="failed",
            reason=reason,
            exception=send_exception,
        )
        self._record_diagnostic(
            thread=thread,
            thread_key=thread_key,
            job_type=job_type,
            event_type="send_failed",
            stage="send",
            outcome="fail",
            reason=reason,
            payload={"job_id": job_id, "pack_id": pack_id, "content_kind": "pack"},
        )
        health_state, health_reason = self._classify_health_from_error(reason)
        if health_state != "unknown":
            self._store.set_account_health(account_id, health_state, reason=health_reason)
        self._store.update_send_queue_job(job_id, state="failed", error_message=reason, failure_reason=reason, finished_at=finished_at)
        self._store.update_thread_state(
            thread_key,
            {
                "pack_status": "failed",
                "pack_error": reason,
                "sender_status": "failed",
                "sender_error": reason,
                "ui_status": "error",
            },
        )
        self._store.update_thread_record(
            thread_key,
            {
                "last_action_type": "send_failed",
                "last_action_at": time.time(),
                "status": "failed",
            },
        )
        self._cleanup_auto_reply_pending_state(
            thread_key,
            job_type=job_type,
            inbound_id_hint=self._pending_inbound_id_hint({"post_send_state_updates": post_send_state_updates}),
        )
        self._store.add_thread_event(
            thread_key,
            failed_thread_event(job_type, is_pack=True),
            account_id=account_id,
            alias_id=str(thread.get("alias_id") or thread.get("account_alias") or "").strip(),
            payload={"job_type": job_type, "content_kind": "pack", "pack_id": pack_id, "reason": reason},
            created_at=time.time(),
        )
        self._notifier(reason="send_pack_failed", thread_keys=[thread_key], account_ids=[account_id])

    def _enqueue(self, task_type: str, payload: dict[str, Any], *, priority: int) -> None:
        with self._lock:
            self._sequence += 1
            task = _SenderTask(
                priority=max(0, int(priority or 0)),
                sequence=self._sequence,
                task_type=str(task_type or "").strip(),
                payload=dict(payload or {}),
            )
            self._queue.put(task)

    def _validate_job_sendability(
        self,
        job_id: int,
        thread: dict[str, Any],
        job_type: str,
        *,
        local_message_id: str = "",
        pack_id: str = "",
    ) -> tuple[bool, str]:
        clean_job_type = str(job_type or "").strip().lower() or "manual_reply"
        thread_key = str(thread.get("thread_key") or "").strip()
        alias_id = str(thread.get("alias_id") or thread.get("account_alias") or "").strip()
        job = self._store.get_send_queue_job(job_id) if int(job_id or 0) > 0 else None
        job_state = str((job or {}).get("state") or "").strip().lower()
        if job and job_state not in {"queued", "processing"}:
            return False, self._cancel_send_job(
                job_id,
                thread,
                clean_job_type,
                reason="job_cancelled",
                local_message_id=local_message_id,
                pack_id=pack_id,
            )
        runtime_state = self._store.get_runtime_alias_state(alias_id) if alias_id else {}
        runtime_active = bool(runtime_state.get("is_running"))
        reason = self._sendability_block_reason(
            thread,
            clean_job_type,
            runtime_active=runtime_active,
        )
        if not reason:
            return True, ""
        return False, self._cancel_send_job(
            job_id,
            thread,
            clean_job_type,
            reason=reason,
            local_message_id=local_message_id,
            pack_id=pack_id,
        )

    def _sendability_block_reason(
        self,
        thread: dict[str, Any],
        job_type: str,
        *,
        runtime_active: bool,
    ) -> str:
        clean_job_type = str(job_type or "").strip().lower() or "manual_reply"
        if clean_job_type in {"auto_reply", "followup"}:
            account_id = str(thread.get("account_id") or "").strip().lstrip("@")
            if account_id:
                get_account = getattr(accounts_module, "get_account", None)
                operational_resolver = getattr(accounts_module, "is_account_enabled_for_operation", None)
                if callable(get_account) and callable(operational_resolver):
                    account = get_account(account_id)
                    if isinstance(account, dict) and not bool(operational_resolver(account)):
                        return "account_not_operational"
            if not runtime_active:
                return "runtime_inactive"
            if not self._router.can_automation_touch(thread):
                return "thread_locked_for_manual"
            if clean_job_type == "followup" and not self._router.can_followup_touch(thread):
                return "followup_not_allowed"
            return ""
        if clean_job_type in {"manual_reply", "manual_pack"} and runtime_active and not self._router.can_manual_send(
            thread,
            runtime_active=True,
        ):
            return "manual_send_not_allowed"
        return ""

    def _matches_runtime_task(
        self,
        task: _SenderTask,
        *,
        alias_id: str,
        job_types: set[str],
    ) -> bool:
        payload = dict(task.payload or {})
        clean_job_type = str(payload.get("job_type") or task.task_type or "").strip().lower()
        if clean_job_type not in job_types:
            return False
        thread_key = str(payload.get("thread_key") or "").strip()
        if not thread_key:
            return False
        thread = self._store.get_thread(thread_key) or {}
        task_alias = str(thread.get("alias_id") or thread.get("account_alias") or "").strip().lower()
        return bool(task_alias) and task_alias == str(alias_id or "").strip().lower()

    @staticmethod
    def _matches_thread_task(
        task: _SenderTask,
        *,
        thread_key: str,
        job_types: set[str],
    ) -> bool:
        payload = dict(task.payload or {})
        clean_job_type = str(payload.get("job_type") or task.task_type or "").strip().lower()
        if clean_job_type not in job_types:
            return False
        task_thread_key = str(payload.get("thread_key") or "").strip()
        return bool(task_thread_key) and task_thread_key == str(thread_key or "").strip()

    def _cancel_send_job(
        self,
        job_id: int,
        thread: dict[str, Any],
        job_type: str,
        *,
        reason: str,
        local_message_id: str = "",
        pack_id: str = "",
    ) -> str:
        clean_reason = str(reason or "cancelled").strip() or "cancelled"
        clean_job_type = str(job_type or "").strip().lower() or "manual_reply"
        thread_key = str(thread.get("thread_key") or "").strip()
        alias_id = str(thread.get("alias_id") or thread.get("account_alias") or "").strip()
        job = self._store.get_send_queue_job(job_id) if int(job_id or 0) > 0 else None
        job_payload = dict((job or {}).get("payload") or {})
        finished_at = time.time()
        self._store.update_send_queue_job(
            job_id,
            state="cancelled",
            error_message=clean_reason,
            failure_reason=clean_reason,
            finished_at=finished_at,
        )
        self._persist_last_send_outcome(
            thread=thread,
            thread_key=thread_key,
            account_id=str(thread.get("account_id") or "").strip(),
            job_id=job_id,
            job_type=clean_job_type,
            at=finished_at,
            outcome="cancelled",
            reason=clean_reason,
        )
        if thread_key and local_message_id:
            self._store.resolve_local_outbound(thread_key, local_message_id, error_message=clean_reason)
        self._cleanup_auto_reply_pending_state(
            thread_key,
            job_type=clean_job_type,
            inbound_id_hint=self._pending_inbound_id_hint(job_payload),
        )
        if thread_key:
            self._store.reconcile_send_queue_thread_state(thread_key)
        if thread_key:
            event_type = "job_cancelled"
            if normalize_reason_code(clean_reason) == "job_cancelled_by_takeover":
                event_type = "job_cancelled_by_takeover"
            elif normalize_reason_code(clean_reason) == "job_cancelled_by_runtime_stop":
                event_type = "job_cancelled_by_runtime_stop"
            self._record_diagnostic(
                thread=thread,
                thread_key=thread_key,
                job_type=clean_job_type,
                event_type=event_type,
                stage="job_cancel",
                outcome="cancel",
                reason=clean_reason,
                payload={
                    "job_id": int(job_id or 0),
                    "local_message_id": local_message_id,
                    "pack_id": str(pack_id or "").strip(),
                    "cancelled_by": "conversation_sender._cancel_send_job",
                },
                created_at=finished_at,
            )
            self._store.add_thread_event(
                thread_key,
                failed_thread_event(clean_job_type, is_pack=bool(str(pack_id or "").strip())),
                account_id=str(thread.get("account_id") or "").strip(),
                alias_id=alias_id,
                payload={
                    "job_type": clean_job_type,
                    "content_kind": "pack" if str(pack_id or "").strip() else "text",
                    "pack_id": str(pack_id or "").strip(),
                    "reason": clean_reason,
                    "cancelled": True,
                },
                created_at=finished_at,
            )
        return clean_reason

    @staticmethod
    def _pending_inbound_id_hint(payload: dict[str, Any] | None) -> str:
        clean_payload = dict(payload or {})
        post_send_state_updates = dict(clean_payload.get("post_send_state_updates") or {})
        return str(post_send_state_updates.get("last_inbound_id_seen") or clean_payload.get("latest_inbound_id") or "").strip()

    def _cleanup_auto_reply_pending_state(
        self,
        thread_key: str,
        *,
        job_type: str,
        inbound_id_hint: str = "",
    ) -> None:
        clean_key = str(thread_key or "").strip()
        clean_job_type = str(job_type or "").strip().lower() or "manual_reply"
        if not clean_key or clean_job_type != "auto_reply":
            return
        thread = self._store.get_thread(clean_key) or {}
        pending_reply = bool(thread.get("pending_reply"))
        pending_inbound_id = str(thread.get("pending_inbound_id") or "").strip()
        if not pending_reply and not pending_inbound_id:
            return
        clean_inbound_hint = str(inbound_id_hint or "").strip()
        if clean_inbound_hint and pending_inbound_id and pending_inbound_id != clean_inbound_hint:
            return
        self._store.update_thread_state(
            clean_key,
            {
                "pending_reply": False,
                "pending_inbound_id": None,
            },
        )

    def _enter_irreversible_send_window(self, thread_key: str, job_type: str) -> bool:
        clean_key = str(thread_key or "").strip()
        clean_job_type = str(job_type or "").strip().lower()
        if not clean_key or clean_job_type not in {"auto_reply", "followup"}:
            return True
        with self._takeover_condition:
            if clean_key in self._takeover_blocked_threads:
                return False
            self._irreversible_send_threads.add(clean_key)
            return True

    def _leave_irreversible_send_window(self, thread_key: str, job_type: str) -> None:
        clean_key = str(thread_key or "").strip()
        clean_job_type = str(job_type or "").strip().lower()
        if not clean_key or clean_job_type not in {"auto_reply", "followup"}:
            return
        with self._takeover_condition:
            self._irreversible_send_threads.discard(clean_key)
            self._takeover_condition.notify_all()

    @staticmethod
    def _job_priority(job_type: Any, *, override: Any = None) -> int:
        if override is not None:
            try:
                return int(override)
            except Exception:
                pass
        mapping = {
            "manual_reply": 0,
            "manual_pack": 10,
            "auto_reply": 20,
            "followup": 30,
        }
        return int(mapping.get(str(job_type or "").strip().lower(), 10))

    def _is_stale_prepare(self, generation: int) -> bool:
        with self._lock:
            return generation != self._prepare_generation

    @staticmethod
    def _classify_health_from_error(error: Any) -> tuple[str, str]:
        text = str(error or "").strip()
        lowered = text.lower()
        if "proxy" in lowered:
            return "proxy_error", text or "proxy_error"
        if "checkpoint" in lowered or "challenge" in lowered:
            return "checkpoint", text or "checkpoint"
        if "suspend" in lowered:
            return "suspended", text or "suspended"
        if "banned" in lowered or "disabled" in lowered or "blocked" in lowered:
            return "banned", text or "banned"
        if "login" in lowered or "session" in lowered or "storage_state" in lowered:
            return "login_required", text or "login_required"
        return "unknown", text or "unknown"

    @staticmethod
    def _pack_by_id(pack_id: str) -> dict[str, Any] | None:
        clean_pack = str(pack_id or "").strip()
        if not clean_pack:
            return None
        for row in responder_module._list_packs():
            if isinstance(row, dict) and str(row.get("id") or "").strip() == clean_pack:
                return dict(row)
        return None
