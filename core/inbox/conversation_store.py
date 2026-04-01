from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.inbox.inbox_storage import InboxStorage
from src.inbox.message_timestamps import message_canonical_timestamp, message_sort_key

from .legacy_projection import LegacyConversationProjection


class ConversationStore:
    _SESSION_MARKER = "connected"
    _LEGACY_INBOUND_PLACEHOLDER = "Nuevo mensaje recibido"
    _CRM_RELEVANT_ACTION_TYPES = ("manual_reply_sent", "manual_pack_sent", "follow_up_tag_added")

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = Path(root_dir)
        self._storage = InboxStorage(self._root_dir)
        self._legacy_projection = LegacyConversationProjection(self._root_dir)

    @staticmethod
    def thread_key(account_id: str, thread_id: str) -> str:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        clean_thread = str(thread_id or "").strip()
        if not clean_account or not clean_thread:
            return ""
        return f"{clean_account}:{clean_thread}"

    def shutdown(self) -> None:
        self._storage.shutdown()

    def diagnostics(self) -> dict[str, int]:
        return self._storage.stats()

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        rows = self._storage.get_threads("all")
        mode = str(filter_mode or "all").strip().lower()
        if mode == "all":
            return rows
        return [row for row in rows if self._matches_filter(row, mode)]

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        return self._storage.get_thread(thread_key)

    def prepare_account_session(
        self,
        account_id: str,
        *,
        session_marker: str | None = None,
        started_at: float | None = None,
    ) -> float | None:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return None
        return self._storage.prepare_account_session(
            clean_account,
            session_marker=str(session_marker or self._SESSION_MARKER).strip() or self._SESSION_MARKER,
            started_at=started_at,
        )

    def list_packs(self) -> list[dict[str, Any]]:
        from core import responder as responder_module

        return [dict(item) for item in responder_module._list_packs()]

    def build_thread_row(
        self,
        *,
        account_id: str,
        thread_id: str,
        thread_href: str = "",
        recipient_username: str = "",
        display_name: str = "",
        account_alias: str = "",
        last_message_text: str = "",
        last_message_timestamp: float | None = None,
        last_message_direction: str = "unknown",
        last_message_id: str = "",
        unread_count: int = 0,
        participants: list[str] | None = None,
        latest_customer_message_at: float | None = None,
    ) -> dict[str, Any]:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        clean_thread = str(thread_id or "").strip()
        clean_recipient = str(recipient_username or "").strip().lstrip("@")
        clean_display = str(display_name or clean_recipient or clean_thread or "Conversacion").strip()
        clean_direction = str(last_message_direction or "unknown").strip().lower() or "unknown"
        if clean_direction not in {"inbound", "outbound", "unknown"}:
            clean_direction = "unknown"
        key = self.thread_key(clean_account, clean_thread)
        return {
            "thread_key": key,
            "thread_id": clean_thread,
            "thread_href": str(thread_href or "").strip(),
            "account_id": clean_account,
            "account_alias": str(account_alias or "").strip(),
            "recipient_username": clean_recipient,
            "display_name": clean_display,
            "last_message_text": str(last_message_text or "").strip(),
            "last_message_timestamp": last_message_timestamp,
            "last_message_direction": clean_direction,
            "last_message_id": str(last_message_id or "").strip(),
            "unread_count": max(0, int(unread_count or 0)),
            "needs_reply": clean_direction == "inbound",
            "participants": list(participants or ([clean_recipient] if clean_recipient else [])),
            "latest_customer_message_at": latest_customer_message_at,
        }

    def upsert_threads(self, rows: list[dict[str, Any]]) -> None:
        self._storage.upsert_threads(rows)

    def update_thread_record(self, thread_key: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        return self._storage.update_thread_record(thread_key, updates)

    def seed_messages(
        self,
        thread_key: str,
        messages: list[dict[str, Any]],
        *,
        participants: list[str] | None = None,
    ) -> None:
        self._storage.seed_messages(thread_key, messages, participants=participants)

    def replace_messages(
        self,
        thread_key: str,
        messages: list[dict[str, Any]],
        *,
        seen_text: str = "",
        seen_at: float | None = None,
        participants: list[str] | None = None,
        mark_read: bool = False,
    ) -> None:
        self._storage.replace_messages(
            thread_key,
            messages,
            seen_text=seen_text,
            seen_at=seen_at,
            participants=participants,
            mark_read=mark_read,
        )

    def apply_endpoint_threads(
        self,
        account: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> list[str]:
        clean_rows = [dict(row) for row in rows if isinstance(row, dict)]
        account_id = str(account.get("username") or "").strip().lstrip("@").lower()
        if not account_id:
            return []
        endpoint_thread_keys = {
            str(row.get("thread_key") or "").strip()
            for row in clean_rows
            if str(row.get("thread_key") or "").strip()
        }
        session_started_at = self._storage.account_session_started_at(account_id)
        legacy_rows, legacy_messages = self._legacy_projection.load_account(
            account_id,
            account_alias=str(account.get("alias") or "").strip(),
        )
        started_at = self._coerce_timestamp(session_started_at)
        if started_at is not None and legacy_rows:
            filtered_rows: dict[str, dict[str, Any]] = {}
            filtered_messages: dict[str, list[dict[str, Any]]] = {}
            for thread_key, legacy_row in legacy_rows.items():
                if thread_key in endpoint_thread_keys:
                    filtered_rows[thread_key] = legacy_row
                    filtered_messages[thread_key] = legacy_messages.get(thread_key, [])
                    continue
                legacy_activity = max(
                    (
                        stamp
                        for stamp in (
                            self._coerce_timestamp(legacy_row.get("last_message_timestamp")),
                            self._coerce_timestamp(legacy_row.get("latest_customer_message_at")),
                            self._coerce_timestamp(legacy_row.get("pack_sent_at")),
                            self._coerce_timestamp(legacy_row.get("_legacy_pack_sent_at")),
                        )
                        if stamp is not None
                    ),
                    default=None,
                )
                if legacy_activity is None or legacy_activity + 0.000001 < started_at:
                    continue
                filtered_rows[thread_key] = legacy_row
                filtered_messages[thread_key] = legacy_messages.get(thread_key, [])
            legacy_rows = filtered_rows
            legacy_messages = filtered_messages
        merged_rows = [
            self._apply_preview_customer_activity_fallback(row)
            for row in self._merge_endpoint_and_legacy_rows(
                account,
                clean_rows,
                legacy_rows=legacy_rows,
            )
        ]
        accepted_rows: list[dict[str, Any]] = []
        for row in merged_rows:
            thread_key = str(row.get("thread_key") or "").strip()
            if not thread_key:
                continue
            legacy_thread_messages = legacy_messages.get(thread_key, [])
            has_activity = self._thread_has_activity(
                row,
                legacy_messages=legacy_thread_messages,
            )
            if not has_activity:
                continue
            current = self._storage.get_thread(thread_key)
            if isinstance(current, dict):
                accepted_rows.append(row)
                continue
            if not self._is_crm_relevant_row(
                row,
                account_id=account_id,
                legacy_messages=legacy_thread_messages,
                session_started_at=session_started_at,
            ):
                continue
            activity_stamp = row.get("last_message_timestamp") or row.get("latest_customer_message_at")
            if self._storage.allow_deleted_thread_recreate(
                thread_key,
                last_activity_timestamp=activity_stamp,
            ):
                accepted_rows.append(row)
        if not accepted_rows:
            self._storage.prune_account_threads(account_id, keep_thread_keys=set())
            self._storage.register_account_sync(
                str(account.get("username") or "").strip(),
                last_error="",
                thread_count=0,
            )
            self._storage.set_account_health(str(account.get("username") or "").strip(), "healthy", reason="")
            return []
        self._storage.upsert_threads(accepted_rows)
        touched: list[str] = []
        keep_thread_keys: set[str] = set()
        for row in accepted_rows:
            thread_key = str(row.get("thread_key") or "").strip()
            if not thread_key:
                continue
            keep_thread_keys.add(thread_key)
            combined_messages = self._merge_message_rows(
                row.get("preview_messages"),
                legacy_messages.get(thread_key, []),
            )
            if combined_messages:
                self._storage.seed_messages(
                    thread_key,
                    combined_messages,
                    participants=list(row.get("participants") or []),
                )
            current = self._storage.get_thread(thread_key) or {}
            self._storage.update_thread_state(thread_key, self._thread_state_payload(thread_key, row, current))
            touched.append(thread_key)
        self._storage.prune_account_threads(account_id, keep_thread_keys=keep_thread_keys)
        self._storage.register_account_sync(
            str(account.get("username") or "").strip(),
            last_error="",
            thread_count=len(accepted_rows),
        )
        self._storage.set_account_health(str(account.get("username") or "").strip(), "healthy", reason="")
        return touched

    def ensure_conversation_from_pack(
        self,
        *,
        account: dict[str, Any],
        thread_row: dict[str, Any],
        pack_name: str,
    ) -> str:
        account_id = str(thread_row.get("account_id") or account.get("username") or "").strip()
        thread_id = str(thread_row.get("thread_id") or "").strip()
        thread_key = self.thread_key(account_id, thread_id)
        if not thread_key:
            return ""
        self._storage.clear_deleted_thread(thread_key)
        row = self.build_thread_row(
            account_id=account_id,
            thread_id=thread_id,
            thread_href=str(thread_row.get("thread_href") or "").strip(),
            recipient_username=str(thread_row.get("recipient_username") or "").strip(),
            display_name=str(thread_row.get("display_name") or "").strip(),
            account_alias=str(thread_row.get("account_alias") or account.get("alias") or "").strip(),
            last_message_text=str(thread_row.get("last_message_text") or "").strip(),
            last_message_timestamp=thread_row.get("last_message_timestamp"),
            last_message_direction=str(thread_row.get("last_message_direction") or "outbound").strip(),
            last_message_id=str(thread_row.get("last_message_id") or "").strip(),
            unread_count=int(thread_row.get("unread_count") or 0),
            participants=list(thread_row.get("participants") or []),
            latest_customer_message_at=thread_row.get("latest_customer_message_at"),
        )
        self._storage.upsert_threads([row])
        current = self._storage.get_thread(thread_key) or {}
        sent_at = time.time()
        self._storage.update_thread_state(
            thread_key,
            {
                "conversation_id": str(current.get("conversation_id") or thread_key).strip(),
                "username": str(
                    current.get("recipient_username")
                    or current.get("username")
                    or row.get("recipient_username")
                    or ""
                ).strip(),
                "pack_name": str(pack_name or "").strip(),
                "ui_status": "pack_sent",
                "last_message": str(pack_name or current.get("last_message") or "").strip(),
                "latest_customer_message_at": self._coerce_timestamp(
                    current.get("latest_customer_message_at") or row.get("latest_customer_message_at")
                ),
                "last_activity_timestamp": sent_at,
                "thread_status": "ready",
                "thread_error": "",
                "reply_detected_at": self._coerce_timestamp(current.get("reply_detected_at")),
            },
        )
        return thread_key

    def refresh_thread_from_legacy(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        current = self._storage.get_thread(clean_key) or {}
        account_id, _thread_id = self._thread_identity(clean_key)
        if not account_id:
            return False
        legacy_rows, legacy_messages = self._legacy_projection.load_account(
            account_id,
            account_alias=str(current.get("account_alias") or "").strip(),
        )
        legacy_row = legacy_rows.get(clean_key)
        messages = legacy_messages.get(clean_key, [])
        if not isinstance(legacy_row, dict) and not messages:
            return False
        merged_row = self._merge_legacy_row(dict(current), legacy_row or {})
        if isinstance(legacy_row, dict):
            self._storage.upsert_threads([merged_row])
        if messages:
            self._storage.seed_messages(
                clean_key,
                messages,
                participants=list(merged_row.get("participants") or current.get("participants") or []),
            )
        refreshed = self._storage.get_thread(clean_key) or merged_row
        self._storage.update_thread_state(
            clean_key,
            self._thread_state_payload(clean_key, merged_row, refreshed),
        )
        return True

    def append_local_outbound_message(
        self,
        thread_key: str,
        text: str,
        *,
        source: str = "manual",
        pack_id: str = "",
        local_message_id: str = "",
    ) -> dict[str, Any] | None:
        return self._storage.append_local_outbound_message(
            thread_key,
            text,
            source=source,
            pack_id=pack_id,
            local_message_id=local_message_id,
        )

    def set_local_outbound_status(self, thread_key: str, local_message_id: str, *, status: str) -> None:
        self._storage.set_local_outbound_status(thread_key, local_message_id, status=status)

    def resolve_local_outbound(
        self,
        thread_key: str,
        local_message_id: str,
        *,
        final_message_id: str = "",
        sent_timestamp: float | None = None,
        error_message: str = "",
    ) -> None:
        self._storage.resolve_local_outbound(
            thread_key,
            local_message_id,
            final_message_id=final_message_id,
            sent_timestamp=sent_timestamp,
            error_message=error_message,
        )

    def delete_message_local(self, thread_key: str, message_ref: dict[str, Any]) -> bool:
        return self._storage.delete_message_local(thread_key, message_ref)

    def create_send_queue_job(
        self,
        task_type: str,
        *,
        thread_key: str,
        account_id: str,
        payload: dict[str, Any],
        dedupe_key: str = "",
        priority: int | None = None,
        state: str = "queued",
        scheduled_at: float | None = None,
    ) -> int:
        return self._storage.create_send_queue_job(
            task_type,
            thread_key=thread_key,
            account_id=account_id,
            payload=payload,
            dedupe_key=dedupe_key,
            priority=priority,
            state=state,
            scheduled_at=scheduled_at,
        )

    def enqueue_send_queue_job(
        self,
        task_type: str,
        *,
        thread_key: str,
        account_id: str,
        payload: dict[str, Any],
        dedupe_key: str = "",
        priority: int | None = None,
        state: str = "queued",
        scheduled_at: float | None = None,
    ) -> dict[str, Any]:
        return self._storage.enqueue_send_queue_job(
            task_type,
            thread_key=thread_key,
            account_id=account_id,
            payload=payload,
            dedupe_key=dedupe_key,
            priority=priority,
            state=state,
            scheduled_at=scheduled_at,
        )

    def update_send_queue_job(
        self,
        job_id: int,
        *,
        state: str,
        error_message: str = "",
        failure_reason: str = "",
        started_at: float | None = None,
        finished_at: float | None = None,
        increment_attempt: bool = False,
    ) -> None:
        self._storage.update_send_queue_job(
            job_id,
            state=state,
            error_message=error_message,
            failure_reason=failure_reason,
            started_at=started_at,
            finished_at=finished_at,
            increment_attempt=increment_attempt,
        )

    def list_send_queue_jobs(
        self,
        *,
        states: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._storage.list_send_queue_jobs(states=states, limit=limit)

    def get_send_queue_job(self, job_id: int) -> dict[str, Any] | None:
        return self._storage.get_send_queue_job(job_id)

    def claim_next_send_queue_job(self, account_id: str, *, allowed_states: list[str] | None = None) -> dict[str, Any] | None:
        return self._storage.claim_next_send_queue_job(account_id, allowed_states=allowed_states)

    def cancel_send_queue_jobs(
        self,
        *,
        thread_key: str = "",
        account_id: str = "",
        alias_id: str = "",
        job_types: list[str] | None = None,
        states: list[str] | None = None,
        reason: str = "cancelled",
    ) -> int:
        return self._storage.cancel_send_queue_jobs(
            thread_key=thread_key,
            account_id=account_id,
            alias_id=alias_id,
            job_types=job_types,
            states=states,
            reason=reason,
        )

    def reconcile_send_queue_thread_state(self, thread_key: str) -> None:
        self._storage.reconcile_send_queue_thread_state(thread_key)

    def update_thread_state(self, thread_key: str, updates: dict[str, Any]) -> None:
        self._storage.update_thread_state(thread_key, updates)

    def add_thread_event(
        self,
        thread_key: str,
        event_type: str,
        *,
        account_id: str = "",
        alias_id: str = "",
        payload: dict[str, Any] | None = None,
        created_at: float | None = None,
    ) -> None:
        self._storage.add_thread_event(
            thread_key,
            event_type,
            account_id=account_id,
            alias_id=alias_id,
            payload=payload,
            created_at=created_at,
        )

    def list_thread_events(self, thread_key: str, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._storage.list_thread_events(thread_key, limit=limit)

    def record_diagnostic_event(
        self,
        *,
        account_id: str = "",
        alias_id: str = "",
        thread_key: str = "",
        job_type: str = "",
        stage: str,
        event_type: str,
        outcome: str,
        reason_code: str = "",
        reason: str = "",
        file: str = "",
        function: str = "",
        line: int = 0,
        exception_type: str = "",
        exception_message: str = "",
        traceback: str = "",
        payload: dict[str, Any] | None = None,
        created_at: float | None = None,
    ) -> int:
        return self._storage.record_diagnostic_event(
            account_id=account_id,
            alias_id=alias_id,
            thread_key=thread_key,
            job_type=job_type,
            stage=stage,
            event_type=event_type,
            outcome=outcome,
            reason_code=reason_code,
            reason=reason,
            file=file,
            function=function,
            line=line,
            exception_type=exception_type,
            exception_message=exception_message,
            traceback=traceback,
            payload=payload,
            created_at=created_at,
        )

    def list_diagnostic_events(
        self,
        *,
        limit: int = 100,
        account_id: str = "",
        alias_id: str = "",
        thread_key: str = "",
        event_type: str = "",
        stage: str = "",
    ) -> list[dict[str, Any]]:
        return self._storage.list_diagnostic_events(
            limit=limit,
            account_id=account_id,
            alias_id=alias_id,
            thread_key=thread_key,
            event_type=event_type,
            stage=stage,
        )

    def get_runtime_alias_state(self, alias_id: str) -> dict[str, Any]:
        return self._storage.get_runtime_alias_state(alias_id)

    def upsert_runtime_alias_state(self, alias_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        return self._storage.upsert_runtime_alias_state(alias_id, updates)

    def list_runtime_alias_states(self) -> list[dict[str, Any]]:
        return self._storage.list_runtime_alias_states()

    def delete_runtime_alias_state(self, alias_id: str) -> bool:
        return self._storage.delete_runtime_alias_state(alias_id)

    def get_session_connector_state(self, account_id: str) -> dict[str, Any]:
        return self._storage.get_session_connector_state(account_id)

    def list_session_connector_states(self) -> list[dict[str, Any]]:
        return self._storage.list_session_connector_states()

    def upsert_session_connector_state(self, account_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        return self._storage.upsert_session_connector_state(account_id, updates)

    def delete_session_connector_state(self, account_id: str) -> bool:
        return self._storage.delete_session_connector_state(account_id)

    def mark_thread_opened(self, thread_key: str) -> None:
        self._storage.mark_thread_opened(thread_key)

    def register_account_sync(self, account_id: str, *, last_error: str = "", thread_count: int | None = None) -> None:
        self._storage.register_account_sync(account_id, last_error=last_error, thread_count=thread_count)

    def set_account_health(self, account_id: str, state: str, *, reason: str = "") -> None:
        self._storage.set_account_health(account_id, state, reason=reason)

    def get_account_health(self, account_id: str) -> dict[str, Any]:
        return self._storage.get_account_health(account_id)

    def record_action_memory(
        self,
        thread_id: str,
        account_id: str,
        action_type: str,
        *,
        pack_id: str = "",
        source: str = "inbox_crm",
    ) -> None:
        self._storage.record_action_memory(
            thread_id,
            account_id,
            action_type,
            pack_id=pack_id,
            source=source,
        )

    def append_thread_tag(self, thread_key: str, tag: str) -> list[str]:
        return self._storage.append_thread_tag(thread_key, tag)

    def mark_follow_up(self, thread_key: str) -> bool:
        return self._storage.mark_follow_up(thread_key)

    def prune_accounts(self, account_ids: set[str]) -> None:
        self._storage.prune_accounts(account_ids)

    def delete_conversation(self, thread_key: str) -> bool:
        return self._storage.delete_thread(thread_key)

    @staticmethod
    def _coerce_timestamp(value: Any) -> float | None:
        try:
            stamp = float(value)
        except Exception:
            return None
        return stamp if stamp > 0 else None

    @staticmethod
    def _thread_identity(thread_key: str) -> tuple[str, str]:
        clean_key = str(thread_key or "").strip()
        if ":" not in clean_key:
            return "", ""
        raw_account, raw_thread = clean_key.split(":", 1)
        return str(raw_account or "").strip().lstrip("@").lower(), str(raw_thread or "").strip()

    @classmethod
    def _merge_message_rows(
        cls,
        primary_rows: Any,
        secondary_rows: Any,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for source_rows in (secondary_rows, primary_rows):
            if not isinstance(source_rows, list):
                continue
            for raw in source_rows:
                if not isinstance(raw, dict):
                    continue
                message = dict(raw)
                message_id = str(message.get("message_id") or "").strip()
                key = message_id or "|".join(
                    (
                        str(message.get("direction") or "").strip().lower(),
                        str(message.get("timestamp") or ""),
                        str(message.get("text") or "").strip(),
                    )
                )
                if not key:
                    continue
                previous = merged.get(key)
                message_ts = message_canonical_timestamp(message) or 0.0
                previous_ts = message_canonical_timestamp(previous) or 0.0
                if previous is None or message_ts >= previous_ts:
                    merged[key] = message
        rows = list(merged.values())
        rows.sort(key=lambda item: message_sort_key(item))
        return rows

    @classmethod
    def _preview_has_inbound_message(cls, row: dict[str, Any]) -> bool:
        for raw in row.get("preview_messages") or []:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("direction") or "").strip().lower() == "inbound":
                return True
        return False

    @classmethod
    def _preview_inbound_activity_at(cls, row: dict[str, Any]) -> float | None:
        latest_inbound_at = max(
            (
                stamp
                for stamp in (
                    cls._coerce_timestamp(raw.get("timestamp"))
                    for raw in row.get("preview_messages") or []
                    if isinstance(raw, dict)
                    and str(raw.get("direction") or "").strip().lower() == "inbound"
                )
                if stamp is not None
            ),
            default=None,
        )
        if latest_inbound_at is not None:
            return latest_inbound_at
        if not cls._preview_has_inbound_message(row):
            return None
        activity_stamp = (
            cls._coerce_timestamp(row.get("last_activity_at"))
            or cls._coerce_timestamp(row.get("last_activity_timestamp"))
        )
        if activity_stamp is not None:
            return activity_stamp
        if str(row.get("last_message_direction") or "").strip().lower() == "inbound":
            return cls._coerce_timestamp(row.get("last_message_timestamp"))
        return None

    @classmethod
    def _apply_preview_customer_activity_fallback(cls, row: dict[str, Any]) -> dict[str, Any]:
        merged = dict(row)
        if cls._coerce_timestamp(merged.get("latest_customer_message_at")) is not None:
            return merged
        preview_inbound_at = cls._preview_inbound_activity_at(merged)
        if preview_inbound_at is not None:
            merged["latest_customer_message_at"] = preview_inbound_at
        return merged

    @classmethod
    def _thread_has_activity(
        cls,
        row: dict[str, Any],
        *,
        legacy_messages: list[dict[str, Any]],
    ) -> bool:
        if legacy_messages:
            return True
        if cls._coerce_timestamp(row.get("last_message_timestamp")) is not None:
            return True
        if cls._coerce_timestamp(row.get("latest_customer_message_at")) is not None:
            return True
        if cls._coerce_timestamp(row.get("pack_sent_at") or row.get("_legacy_pack_sent_at")) is not None:
            return True
        return bool(str(row.get("last_message_text") or "").strip())

    def _is_crm_relevant_row(
        self,
        row: dict[str, Any],
        *,
        account_id: str,
        legacy_messages: list[dict[str, Any]],
        session_started_at: float | None,
    ) -> bool:
        # Inbound activity is always relevant; outbound-only threads are only
        # relevant if we have local CRM memory (pack/reply/follow-up) or legacy activity.
        direction = str(row.get("last_message_direction") or "").strip().lower()
        try:
            unread = max(0, int(row.get("unread_count") or 0))
        except Exception:
            unread = 0
        if direction == "inbound" or unread > 0 or self._preview_has_inbound_message(row):
            return True
        if legacy_messages:
            return True
        if self._coerce_timestamp(row.get("latest_customer_message_at")) is not None:
            return True
        if self._coerce_timestamp(row.get("pack_sent_at") or row.get("_legacy_pack_sent_at")) is not None:
            return True
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            _, thread_id = self._thread_identity(str(row.get("thread_key") or "").strip())
        if not thread_id:
            return False
        return self._storage.thread_has_action_since(
            account_id,
            thread_id,
            action_types=self._CRM_RELEVANT_ACTION_TYPES,
            started_at=session_started_at,
        )

    @classmethod
    def _merge_legacy_row(cls, current: dict[str, Any], legacy_row: dict[str, Any]) -> dict[str, Any]:
        merged = dict(current)
        for key in ("thread_href", "recipient_username", "display_name", "account_alias"):
            if not str(merged.get(key) or "").strip() and str(legacy_row.get(key) or "").strip():
                merged[key] = legacy_row.get(key)
        participants = [str(item or "").strip() for item in merged.get("participants") or [] if str(item or "").strip()]
        for raw in legacy_row.get("participants") or []:
            candidate = str(raw or "").strip()
            if candidate and candidate not in participants:
                participants.append(candidate)
        if participants:
            merged["participants"] = participants
        current_ts = cls._coerce_timestamp(merged.get("last_message_timestamp"))
        legacy_ts = cls._coerce_timestamp(legacy_row.get("last_message_timestamp"))
        if legacy_ts is not None and (current_ts is None or legacy_ts + 0.000001 >= current_ts):
            for key in ("last_message_text", "last_message_timestamp", "last_message_direction", "last_message_id"):
                value = legacy_row.get(key)
                if value not in (None, "", [], ()):
                    merged[key] = value
        current_reply_at = cls._coerce_timestamp(merged.get("latest_customer_message_at"))
        legacy_reply_at = cls._coerce_timestamp(legacy_row.get("latest_customer_message_at"))
        if legacy_reply_at is not None and (current_reply_at is None or legacy_reply_at > current_reply_at + 0.000001):
            merged["latest_customer_message_at"] = legacy_reply_at
            merged["unread_count"] = max(1, int(merged.get("unread_count") or 0))
        for key in ("pack_sent_at", "pack_name", "_legacy_pack_sent_at", "_legacy_pack_id", "_legacy_pack_name"):
            value = legacy_row.get(key)
            if value not in (None, "", [], ()):
                merged[key] = value
        return merged

    def _merge_endpoint_and_legacy_rows(
        self,
        account: dict[str, Any],
        rows: list[dict[str, Any]],
        *,
        legacy_rows: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged_by_key: dict[str, dict[str, Any]] = {}
        account_alias = str(account.get("alias") or "").strip()
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            thread_key = str(raw.get("thread_key") or "").strip()
            if not thread_key:
                continue
            row = dict(raw)
            if account_alias and not str(row.get("account_alias") or "").strip():
                row["account_alias"] = account_alias
            merged_by_key[thread_key] = row
        for thread_key, legacy_row in legacy_rows.items():
            current = merged_by_key.get(thread_key)
            if isinstance(current, dict):
                merged_by_key[thread_key] = self._merge_legacy_row(current, legacy_row)
                continue
            merged_by_key[thread_key] = dict(legacy_row)
        return list(merged_by_key.values())

    @classmethod
    def _thread_state_payload(
        cls,
        thread_key: str,
        row: dict[str, Any],
        current: dict[str, Any],
    ) -> dict[str, Any]:
        eps = 0.000001

        def normalize_direction(value: Any) -> str:
            direction = str(value or "").strip().lower() or "unknown"
            return direction if direction in {"inbound", "outbound", "unknown"} else "unknown"

        row_ts = cls._coerce_timestamp(row.get("last_message_timestamp"))
        cur_ts = cls._coerce_timestamp(current.get("last_message_timestamp"))
        row_dir = normalize_direction(row.get("last_message_direction"))
        cur_dir = normalize_direction(current.get("last_message_direction"))
        row_text = str(row.get("last_message_text") or "").strip()
        cur_text = str(current.get("last_message_text") or current.get("last_message") or "").strip()
        row_id = str(row.get("last_message_id") or "").strip()
        cur_id = str(current.get("last_message_id") or "").strip()

        if row_ts is not None and (cur_ts is None or row_ts > cur_ts + eps):
            effective_last_message_timestamp = row_ts
            effective_last_message_direction = row_dir
            effective_last_message_text = row_text or cur_text
            effective_last_message_id = row_id or cur_id
        elif cur_ts is not None and (row_ts is None or cur_ts > row_ts + eps):
            effective_last_message_timestamp = cur_ts
            effective_last_message_direction = cur_dir
            effective_last_message_text = cur_text or row_text
            effective_last_message_id = cur_id or row_id
        else:
            effective_last_message_timestamp = row_ts if row_ts is not None else cur_ts
            effective_last_message_direction = row_dir if row_dir != "unknown" else cur_dir
            effective_last_message_text = row_text or cur_text
            effective_last_message_id = row_id or cur_id
        latest_customer_message_at = max(
            (
                stamp
                for stamp in (
                    cls._coerce_timestamp(current.get("latest_customer_message_at")),
                    cls._coerce_timestamp(row.get("latest_customer_message_at")),
                    cls._coerce_timestamp(current.get("reply_detected_at")),
                )
                if stamp is not None
            ),
            default=None,
        )
        pack_sent_at = max(
            (
                stamp
                for stamp in (
                    cls._coerce_timestamp(current.get("pack_sent_at")),
                    cls._coerce_timestamp(row.get("pack_sent_at")),
                    cls._coerce_timestamp(row.get("_legacy_pack_sent_at")),
                )
                if stamp is not None
            ),
            default=None,
        )
        pack_name = str(
            current.get("pack_name")
            or row.get("pack_name")
            or row.get("_legacy_pack_name")
            or ""
        ).strip()
        if pack_sent_at is not None and (
            effective_last_message_timestamp is None or pack_sent_at > effective_last_message_timestamp + eps
        ):
            effective_last_message_timestamp = pack_sent_at
            effective_last_message_direction = "outbound"
            if not effective_last_message_text:
                effective_last_message_text = pack_name
        if effective_last_message_direction == "inbound" and not effective_last_message_text:
            effective_last_message_text = cls._LEGACY_INBOUND_PLACEHOLDER
        activity_candidates = [
            effective_last_message_timestamp,
            latest_customer_message_at,
            cls._coerce_timestamp(row.get("last_activity_timestamp")),
            cls._coerce_timestamp(current.get("last_activity_timestamp")),
            pack_sent_at,
        ]
        last_activity_timestamp = max((stamp for stamp in activity_candidates if stamp is not None), default=None)
        ui_status = (
            "pack_sent"
            if pack_sent_at is not None and effective_last_message_direction != "inbound"
            else cls._ui_status_from_direction(
                effective_last_message_direction,
                fallback=str(current.get("ui_status") or "").strip(),
            )
        )
        return {
            "conversation_id": str(current.get("conversation_id") or thread_key).strip(),
            "username": str(
                current.get("recipient_username")
                or current.get("username")
                or row.get("recipient_username")
                    or ""
                ).strip(),
            "pack_name": pack_name,
            "ui_status": ui_status,
            "last_message": effective_last_message_text,
            "last_message_text": effective_last_message_text,
            "last_message_timestamp": effective_last_message_timestamp,
            "last_message_direction": effective_last_message_direction,
            "last_message_id": effective_last_message_id,
            "latest_customer_message_at": latest_customer_message_at,
            "last_activity_timestamp": last_activity_timestamp,
            "thread_status": "ready",
            "thread_error": "",
            "thread_loaded_at": time.time(),
            "reply_detected_at": latest_customer_message_at,
            "pack_sent_at": pack_sent_at,
        }

    @staticmethod
    def _matches_filter(row: dict[str, Any], mode: str) -> bool:
        filter_mode = str(mode or "all").strip().lower()
        if filter_mode == "unread":
            try:
                return int(row.get("unread_count") or 0) > 0
            except Exception:
                return False
        if filter_mode == "pending":
            if "needs_reply" in row:
                return bool(row.get("needs_reply"))
            return str(row.get("last_message_direction") or "").strip().lower() == "inbound"
        return True

    @staticmethod
    def _ui_status_from_direction(direction: str, *, fallback: str = "") -> str:
        clean_direction = str(direction or "").strip().lower()
        if clean_direction == "inbound":
            return "needs_reply"
        if clean_direction == "outbound":
            return "active"
        return str(fallback or "active").strip() or "active"
