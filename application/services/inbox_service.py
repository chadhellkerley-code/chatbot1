from __future__ import annotations

from typing import Any

from core.inbox.inbox_manager import InboxManager

from .base import ServiceContext
from .inbox_runtime import InboxRuntime


InboxEngine = InboxManager


class InboxService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        self._engine = InboxEngine(context.root_dir)
        self._runtime = InboxRuntime(context, self._engine)

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
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return ""
        local_id = str(self._engine.send_message(clean_key, text) or "").strip()
        self._runtime.request_rebuild(reason="send_message", thread_keys=[clean_key])
        return local_id

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        queued = bool(self._engine.send_pack(clean_key, pack_id))
        if queued:
            self._runtime.request_rebuild(reason="send_pack", thread_keys=[clean_key])
        return queued

    def request_ai_suggestion(self, thread_key: str) -> bool:
        self.ensure_started()
        clean_key = str(thread_key or "").strip()
        if not self._runtime.ensure_thread_seeded(clean_key) and not isinstance(self._engine.get_thread(clean_key), dict):
            return False
        queued = bool(self._engine.request_ai_suggestion(clean_key))
        if queued:
            self._runtime.request_rebuild(reason="request_ai_suggestion", thread_keys=[clean_key])
        return queued

    def list_packs(self) -> list[dict[str, Any]]:
        self.ensure_started()
        return self._engine.list_packs()

    def diagnostics(self) -> dict[str, Any]:
        self._runtime.ensure_projection_ready()
        runtime_payload = self._runtime.diagnostics()
        payload = {}
        if runtime_payload.get("backend_started") and hasattr(self._engine, "diagnostics"):
            payload = dict(self._engine.diagnostics() or {})
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

    def refresh(self) -> None:
        self.ensure_started()
        self._runtime.request_sync(force=True)
