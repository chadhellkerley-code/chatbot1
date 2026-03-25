from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from core import accounts as accounts_module
from core import responder as responder_module
from core.autoresponder.openai_client import (
    _build_openai_client,
    _openai_generate_text,
    _resolve_ai_model,
)
from src.inbox.message_sender import build_conversation_text

from .browser_pool import BrowserPool
from .conversation_reader import ConversationReader
from .conversation_sender import ConversationSender
from .conversation_store import ConversationStore


class ConversationService:
    def __init__(self, root_dir: Path, *, notifier) -> None:
        self._store = ConversationStore(root_dir)
        self._notifier = notifier
        self._browser_pool = BrowserPool(self._get_account)
        self._reader = ConversationReader(
            self._store,
            accounts_provider=self._active_accounts,
            notifier=self._notifier,
        )
        self._sender = ConversationSender(
            self._store,
            self._browser_pool,
            notifier=self._notifier,
        )
        self._started = False
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._browser_pool.start()
            self._sender.start()
            self._reader.start()
            self._started = True

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                self._store.shutdown()
                return
            self._started = False
        self._reader.stop()
        self._sender.stop()
        self._store.shutdown()

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        return self._store.list_threads(filter_mode)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        return self._store.get_thread(thread_key)

    def open_thread(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        thread = self._store.get_thread(clean_key)
        if not isinstance(thread, dict):
            return False
        self._store.mark_thread_opened(clean_key)
        self._store.update_thread_state(
            clean_key,
            {
                "thread_status": "opening",
                "thread_error": "",
                "sender_status": "queued",
                "sender_error": "",
            },
        )
        self._notifier(
            reason="prepare_thread_requested",
            thread_keys=[clean_key],
            account_ids=[str(thread.get("account_id") or "").strip()],
        )
        return self._sender.prepare_thread(clean_key)

    def send_message(self, thread_key: str, text: str) -> str:
        return self._sender.queue_message(thread_key, text, job_type="manual_reply")

    def send_pack(self, thread_key: str, pack_id: str) -> bool:
        return self._sender.queue_pack(thread_key, pack_id, job_type="manual_pack")

    def request_ai_suggestion(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        thread = self._store.get_thread(clean_key)
        if not isinstance(thread, dict):
            return False
        self._store.update_thread_state(
            clean_key,
            {
                "suggestion_status": "queued",
                "suggestion_error": "",
                "suggested_reply": "",
                "suggested_reply_at": None,
            },
        )
        self._notifier(reason="suggest_reply_queued", thread_keys=[clean_key], account_ids=[])
        worker = threading.Thread(
            target=self._build_suggestion,
            args=(clean_key,),
            name=f"inbox-ai-suggestion-{clean_key}",
            daemon=True,
        )
        worker.start()
        return True

    def add_tag(self, thread_key: str, tag: str) -> bool:
        clean_key = str(thread_key or "").strip()
        clean_tag = str(tag or "").strip()
        if not clean_key or not clean_tag:
            return False
        tags = self._store.append_thread_tag(clean_key, clean_tag)
        if not tags:
            return False
        self._notifier(reason="thread_tag_added", thread_keys=[clean_key], account_ids=[])
        return True

    def mark_follow_up(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        marked = self._store.mark_follow_up(clean_key)
        if marked:
            self._notifier(reason="follow_up_marked", thread_keys=[clean_key], account_ids=[])
        return marked

    def delete_conversation(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        thread = self._store.get_thread(clean_key) or {}
        deleted = self._store.delete_conversation(clean_key)
        if deleted:
            self._notifier(
                reason="conversation_deleted",
                thread_keys=[clean_key],
                account_ids=[str(thread.get("account_id") or "").strip()],
            )
        return deleted

    def delete_message_local(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key or not isinstance(message_ref, dict):
            return False
        thread = self._store.get_thread(clean_key) or {}
        deleted = self._store.delete_message_local(clean_key, message_ref)
        if deleted:
            self._notifier(
                reason="message_deleted_local",
                thread_keys=[clean_key],
                account_ids=[str(thread.get("account_id") or "").strip()],
            )
        return deleted

    def list_packs(self) -> list[dict[str, Any]]:
        return self._store.list_packs()

    def enqueue_periodic_sync(self, *, force: bool = False) -> None:
        self._reader.request_sync(force=force)

    def set_foreground_active(self, active: bool) -> None:
        del active

    def diagnostics(self) -> dict[str, Any]:
        payload = {}
        payload.update(self._store.diagnostics())
        payload.update(self._reader.diagnostics())
        payload.update(self._sender.diagnostics())
        payload.update(self._browser_pool.diagnostics())
        return payload

    def _build_suggestion(self, thread_key: str) -> None:
        thread = self._store.get_thread(thread_key)
        if not isinstance(thread, dict):
            return
        account = self._get_account(str(thread.get("account_id") or ""))
        if not isinstance(account, dict):
            return
        try:
            suggestion = self._generate_ai_suggestion(account, thread)
        except Exception as exc:
            self._store.update_thread_state(
                thread_key,
                {
                    "suggestion_status": "failed",
                    "suggestion_error": str(exc),
                    "suggested_reply": "",
                    "suggested_reply_at": None,
                },
            )
            self._notifier(reason="suggest_reply_failed", thread_keys=[thread_key], account_ids=[])
            return
        self._store.update_thread_state(
            thread_key,
            {
                "suggestion_status": "ready" if suggestion else "failed",
                "suggestion_error": "" if suggestion else "empty_suggestion",
                "suggested_reply": suggestion,
                "suggested_reply_at": time.time() if suggestion else None,
            },
        )
        self._notifier(reason="suggest_reply_ready", thread_keys=[thread_key], account_ids=[])

    def _generate_ai_suggestion(self, account: dict[str, Any], thread: dict[str, Any]) -> str:
        account_id = str(account.get("username") or "").strip()
        alias = str(account.get("alias") or "").strip()
        messages = list(thread.get("messages") or [])
        packs = self.list_packs()
        prompt_entry = responder_module._resolve_prompt_entry_for_user(
            account_id,
            active_alias=alias or None,
            fallback_entry=responder_module._get_prompt_entry(alias or account_id),
        )
        flow_config = responder_module._flow_config_for_account(account_id)
        objection_prompt = str(prompt_entry.get("objection_prompt") or "").strip()
        recent_text = build_conversation_text(messages, limit=12)
        api_key = responder_module._resolve_ai_api_key()
        if not api_key:
            return self._fallback_suggestion(thread, packs)
        client = _build_openai_client(api_key)
        model = _resolve_ai_model(api_key)
        raw = _openai_generate_text(
            client,
            system_prompt=(
                "Sos un asistente de CRM para mensajes directos de Instagram. "
                "Escribi una sola respuesta breve, natural y lista para pegar."
            ),
            user_content=(
                f"cuenta: @{account_id}\n"
                f"alias: {alias or '-'}\n\n"
                f"mensajes_recientes:\n{recent_text or '(sin historial)'}\n\n"
                f"packs_disponibles:\n{self._packs_summary(packs)}\n\n"
                f"objeciones:\n{objection_prompt or '-'}\n\n"
                f"flow:\n{self._flow_summary(flow_config)}\n"
            ),
            model=model,
            temperature=0.35,
            max_output_tokens=160,
        )
        candidate = str(raw or "").strip()
        sanitizer = getattr(responder_module, "_sanitize_generated_message", None)
        if callable(sanitizer):
            candidate = str(sanitizer(candidate) or "").strip()
        return candidate or self._fallback_suggestion(thread, packs)

    @staticmethod
    def _packs_summary(packs: list[dict[str, Any]]) -> str:
        rows: list[str] = []
        for pack in packs:
            if not isinstance(pack, dict) or not bool(pack.get("active", True)):
                continue
            preview = ""
            for action in pack.get("actions") or []:
                if not isinstance(action, dict):
                    continue
                if str(action.get("type") or "").strip().lower() == "text_fixed":
                    preview = str(action.get("content") or "").strip()
                    if preview:
                        break
            rows.append(f"- {str(pack.get('name') or 'Pack').strip()}: {preview[:80] or 'sin preview'}")
        return "\n".join(rows[:12]) or "- sin packs activos"

    @staticmethod
    def _flow_summary(flow_config: dict[str, Any]) -> str:
        stages = flow_config.get("stages") if isinstance(flow_config, dict) else []
        if not isinstance(stages, list) or not stages:
            return "- sin flow"
        rows: list[str] = []
        for stage in stages[:6]:
            if not isinstance(stage, dict):
                continue
            followups = stage.get("followups")
            rows.append(
                f"- {str(stage.get('id') or 'stage').strip()}: accion={str(stage.get('action_type') or '-').strip()} "
                f"followups={len(followups) if isinstance(followups, list) else 0}"
            )
        return "\n".join(rows) or "- sin flow"

    @staticmethod
    def _fallback_suggestion(thread: dict[str, Any], packs: list[dict[str, Any]]) -> str:
        last_inbound = ""
        for row in reversed(list(thread.get("messages") or [])):
            if not isinstance(row, dict):
                continue
            if str(row.get("direction") or "").strip().lower() == "inbound":
                last_inbound = str(row.get("text") or "").strip().lower()
                break
        for pack in packs:
            if not isinstance(pack, dict) or not bool(pack.get("active", True)):
                continue
            for action in pack.get("actions") or []:
                if isinstance(action, dict) and str(action.get("type") or "").strip().lower() == "text_fixed":
                    preview = str(action.get("content") or "").strip()
                    if preview:
                        return preview
        if any(token in last_inbound for token in ("precio", "sale", "cuesta", "valor")):
            return "Te paso la info y el valor en un segundo."
        if any(token in last_inbound for token in ("hola", "buenas", "hey")):
            return "Hola, gracias por escribirnos. Contame y te ayudo."
        return "Gracias por escribirnos. Decime y te respondo por aca."

    @staticmethod
    def _active_accounts() -> list[dict[str, Any]]:
        rows = []
        seen: set[str] = set()
        for raw in accounts_module.list_all():
            if not isinstance(raw, dict):
                continue
            username = str(raw.get("username") or "").strip().lstrip("@").lower()
            if not username or username in seen:
                continue
            if not bool(raw.get("active", True)):
                continue
            if not bool(raw.get("connected", False)):
                profile_ready = accounts_module.has_playwright_storage_state(username)
                if not profile_ready:
                    continue
            if str(raw.get("status") or "").strip().lower() == "disabled":
                continue
            seen.add(username)
            rows.append(dict(raw))
        return rows

    @staticmethod
    def _get_account(account_id: str) -> dict[str, Any] | None:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return None
        account = accounts_module.get_account(clean_account)
        return dict(account) if isinstance(account, dict) else None
