from __future__ import annotations

from typing import Any

from core.inbox.inbox_manager import InboxManager

from .base import ServiceContext
from .inbox_automation_service import InboxAutomationService
from .inbox_runtime import InboxRuntime


InboxEngine = InboxManager


class InboxService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        self._engine = InboxEngine(context.root_dir)
        self._runtime = InboxRuntime(context, self._engine)
        engine_store = getattr(self._engine, "_store", None)
        engine_sender = getattr(self._engine, "_sender", None)
        self._automation = (
            InboxAutomationService(
                store=engine_store,
                sender=engine_sender,
                ensure_backend_started=self.ensure_started,
            )
            if engine_store is not None and engine_sender is not None
            else None
        )

    def ensure_started(self) -> None:
        self._runtime.ensure_backend_started()

    def shutdown(self) -> None:
        self._runtime.shutdown()

    @property
    def events(self) -> Any:
        return self._runtime.events

    def set_ui_active(self, active: bool) -> None:
        self._runtime.set_ui_active(active)

    def list_threads_cached(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        return self._runtime.list_threads(filter_mode)

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        self._runtime.ensure_projection_ready()
        return self._runtime.list_threads(filter_mode)

    def get_thread_cached(self, thread_key: str) -> dict[str, Any] | None:
        return self._runtime.get_thread_cached(thread_key)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        self._runtime.ensure_projection_ready()
        return self._runtime.get_thread(thread_key)

    def projection_ready(self) -> bool:
        return self._runtime.is_projection_ready()

    def request_open_thread(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        self._runtime.request_thread_open(clean_key)
        return True

    def open_thread(self, thread_key: str) -> bool:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        self._runtime.ensure_thread_seeded(clean_key)
        opened = bool(self._engine.open_thread(clean_key))
        self._runtime.request_rebuild(reason="open_thread", thread_keys=[clean_key])
        return opened

    def send_message(self, thread_key: str, text: str) -> str:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        thread = self._engine.get_thread(clean_key)
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(thread, dict):
            return ""
        thread = self._engine.get_thread(clean_key)
        if not isinstance(thread, dict):
            return ""
        if self._automation is None:
            local_id = str(self._engine.send_message(clean_key, text) or "").strip()
            self._runtime.request_rebuild(reason="send_message", thread_keys=[clean_key])
            return local_id
        thread, takeover_applied = self._prepare_thread_for_manual_send(clean_key, thread)
        if not isinstance(thread, dict):
            if takeover_applied:
                self._runtime.request_rebuild(reason="send_message", thread_keys=[clean_key])
            return ""
        local_id = str(self._engine._sender.queue_message(clean_key, text, job_type="manual_reply") or "").strip()
        self._runtime.request_rebuild(reason="send_message", thread_keys=[clean_key])
        return local_id

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        thread = self._engine.get_thread(clean_key)
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(thread, dict):
            return False
        thread = self._engine.get_thread(clean_key)
        if not isinstance(thread, dict):
            return False
        if self._automation is None:
            queued = bool(self._engine.send_pack(clean_key, pack_id))
            if queued:
                self._runtime.request_rebuild(reason="send_pack", thread_keys=[clean_key])
            return queued
        thread, takeover_applied = self._prepare_thread_for_manual_send(clean_key, thread)
        if not isinstance(thread, dict):
            if takeover_applied:
                self._runtime.request_rebuild(reason="send_pack", thread_keys=[clean_key])
            return False
        queued = bool(self._engine._sender.queue_pack(clean_key, pack_id, job_type="manual_pack"))
        if queued:
            self._runtime.request_rebuild(reason="send_pack", thread_keys=[clean_key])
        return queued

    def take_thread_manual(self, thread_key: str, *, operator_id: str = "inbox_ui") -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or self._automation is None:
            return False
        thread = self.get_thread(clean_key)
        if not isinstance(thread, dict) or not self._automation.manual_takeover_allowed(thread):
            return False
        updated = self._automation.manual_takeover(clean_key, operator_id=operator_id)
        if not isinstance(updated, dict):
            return False
        self._runtime.request_rebuild(reason="manual_takeover", thread_keys=[clean_key])
        return True

    def release_thread_manual(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or self._automation is None:
            return False
        thread = self.get_thread(clean_key)
        if not isinstance(thread, dict) or not self._automation.manual_release_allowed(thread):
            return False
        updated = self._automation.manual_release(clean_key)
        if not isinstance(updated, dict):
            return False
        self._runtime.request_rebuild(reason="manual_release", thread_keys=[clean_key])
        return True

    def request_ai_suggestion(self, thread_key: str) -> bool:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        queued = bool(self._engine.request_ai_suggestion(clean_key))
        if queued:
            self._runtime.request_rebuild(reason="request_ai_suggestion", thread_keys=[clean_key])
        return queued

    def mark_thread_qualified(self, thread_key: str, *, operator_id: str = "inbox_ui") -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or self._automation is None:
            return False
        updated = self._automation.mark_thread_qualified(clean_key, operator_id=operator_id)
        if not isinstance(updated, dict):
            return False
        self._runtime.request_rebuild(reason="thread_marked_qualified", thread_keys=[clean_key])
        return True

    def mark_thread_disqualified(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or self._automation is None:
            return False
        updated = self._automation.mark_thread_disqualified(clean_key)
        if not isinstance(updated, dict):
            return False
        self._runtime.request_rebuild(reason="thread_marked_disqualified", thread_keys=[clean_key])
        return True

    def clear_thread_classification(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or self._automation is None:
            return False
        updated = self._automation.clear_thread_classification(clean_key)
        if not isinstance(updated, dict):
            return False
        self._runtime.request_rebuild(reason="thread_classification_cleared", thread_keys=[clean_key])
        return True

    def list_packs(self) -> list[dict[str, Any]]:
        self.ensure_started()
        return self._engine.list_packs()

    def diagnostics(self) -> dict[str, Any]:
        self._runtime.ensure_projection_ready()
        runtime_payload = self._runtime.diagnostics()
        payload = {}
        if runtime_payload.get("backend_started") and hasattr(self._engine, "diagnostics"):
            payload = dict(self._engine.diagnostics() or {})
        payload["runtime_alias_states"] = self._automation.list_statuses() if self._automation is not None else []
        payload.update(runtime_payload)
        payload["effective_thread_count"] = max(
            int(payload.get("thread_count") or 0),
            int(payload.get("projection_threads") or 0),
        )
        payload["thread_count"] = int(payload.get("thread_count") or 0)
        payload["message_groups"] = int(payload.get("message_groups") or 0)
        payload["queued_tasks"] = int(payload.get("queued_tasks") or 0)
        payload["worker_count"] = int(payload.get("worker_count") or 0)
        payload["dedupe_pending"] = int(payload.get("dedupe_pending") or 0)
        return payload

    def add_tag(self, thread_key: str, tag: str) -> bool:
        clean_key = str(thread_key or "").strip()
        clean_tag = str(tag or "").strip()
        if not clean_key or not clean_tag:
            return False
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        changed = bool(getattr(self._engine, "add_tag", lambda *_args, **_kwargs: False)(clean_key, clean_tag))
        if changed:
            self._runtime.request_rebuild(reason="thread_tag_added", thread_keys=[clean_key])
        return changed

    def mark_follow_up(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        changed = bool(getattr(self._engine, "mark_follow_up", lambda *_args, **_kwargs: False)(clean_key))
        if changed:
            self._runtime.request_rebuild(reason="follow_up_marked", thread_keys=[clean_key])
        return changed

    def delete_local_message(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or not isinstance(message_ref, dict):
            return False
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        deleted = bool(getattr(self._engine, "delete_message_local", lambda *_args, **_kwargs: False)(clean_key, message_ref))
        if deleted:
            self._runtime.request_rebuild(reason="delete_local_message", thread_keys=[clean_key])
        return deleted

    def refresh(self) -> None:
        self.ensure_started()
        self._runtime.request_sync(force=True)

    def list_runtime_aliases(self) -> list[str]:
        return self._automation.list_aliases() if self._automation is not None else []

    def alias_runtime_status(self, alias_id: str) -> dict[str, Any]:
        if self._automation is None:
            return {}
        scheduler_state = self._automation.status(alias_id)
        if not scheduler_state:
            return {}
        payload: dict[str, Any] = dict(scheduler_state or {})

        # Scheduler state: keep existing ambiguous names for compatibility, but also expose explicit keys.
        payload.setdefault("scheduler_current_account_id", str(payload.get("current_account_id") or "").strip())
        payload.setdefault("scheduler_next_account_id", str(payload.get("next_account_id") or "").strip())

        # Sender/browser state: derive from the real browser pool attachment (global, not alias-scoped).
        engine_diag: dict[str, Any] = {}
        try:
            if hasattr(self._engine, "diagnostics"):
                engine_diag = dict(self._engine.diagnostics() or {})
        except Exception:
            engine_diag = {}

        payload.setdefault("sender_attached_account_id", str(engine_diag.get("active_account_id") or "").strip())
        payload.setdefault("sender_attached_thread_key", str(engine_diag.get("active_thread_key") or "").strip())

        payload.setdefault("last_send_attempt_account_id", str(payload.get("last_send_attempt_account_id") or "").strip())
        payload.setdefault("last_send_attempt_thread_key", str(payload.get("last_send_attempt_thread_key") or "").strip())
        payload.setdefault("last_send_attempt_job_id", int(payload.get("last_send_attempt_job_id") or 0))
        payload.setdefault("last_send_attempt_job_type", str(payload.get("last_send_attempt_job_type") or "").strip())
        payload.setdefault("last_send_attempt_at", payload.get("last_send_attempt_at"))
        payload.setdefault("last_send_attempt_outcome", str(payload.get("last_send_attempt_outcome") or "").strip())
        payload.setdefault("last_send_attempt_reason_code", str(payload.get("last_send_attempt_reason_code") or "").strip())

        payload.setdefault("last_send_outcome", str(payload.get("last_send_outcome") or "").strip())
        payload.setdefault("last_send_reason_code", str(payload.get("last_send_reason_code") or "").strip())
        payload.setdefault("last_send_reason", str(payload.get("last_send_reason") or "").strip())
        payload.setdefault("last_send_account_id", str(payload.get("last_send_account_id") or "").strip())
        payload.setdefault("last_send_thread_key", str(payload.get("last_send_thread_key") or "").strip())
        payload.setdefault("last_send_job_id", int(payload.get("last_send_job_id") or 0))
        payload.setdefault("last_send_job_type", str(payload.get("last_send_job_type") or "").strip())
        payload.setdefault("last_send_at", payload.get("last_send_at"))
        payload.setdefault("last_send_exception_type", str(payload.get("last_send_exception_type") or "").strip())
        payload.setdefault("last_send_exception_message", str(payload.get("last_send_exception_message") or "").strip())
        return payload

    def start_alias_runtime(self, alias_id: str, config: dict[str, Any]) -> dict[str, Any]:
        self.ensure_started()
        return self._automation.start_alias(alias_id, config) if self._automation is not None else {}

    def stop_alias_runtime(self, alias_id: str) -> dict[str, Any]:
        return self._automation.stop_alias(alias_id) if self._automation is not None else {}
    def _prepare_thread_for_manual_send(
        self,
        thread_key: str,
        thread: dict[str, Any] | None,
        *,
        operator_id: str = "inbox_ui",
    ) -> tuple[dict[str, Any] | None, bool]:
        if not isinstance(thread, dict) or self._automation is None:
            return None, False
        current = dict(thread)
        takeover_applied = False
        if str(current.get("owner") or "").strip().lower() != "manual":
            if not self._automation.manual_takeover_allowed(current):
                return None, False
            updated = self._automation.manual_takeover(thread_key, operator_id=operator_id)
            if not isinstance(updated, dict):
                return None, False
            current = updated
            takeover_applied = True
        if not self._automation.manual_send_allowed(current):
            return None, takeover_applied
        return current, takeover_applied
