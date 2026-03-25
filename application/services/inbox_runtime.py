from __future__ import annotations

import copy
import logging
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from PySide6.QtCore import QCoreApplication, QFileSystemWatcher, QObject, Signal

from src.inbox.message_timestamps import message_canonical_timestamp, message_sort_key

from .base import ServiceContext


_DEFAULT_IDLE_BACKEND_SHUTDOWN_SECONDS = 20.0
_DEFAULT_IDLE_BACKEND_RETRY_SECONDS = 5.0

logger = logging.getLogger(__name__)


def _include_thread(thread: dict[str, Any], filter_mode: str) -> bool:
    mode = str(filter_mode or "all").strip().lower()
    if mode == "unread":
        try:
            return int(thread.get("unread_count") or 0) > 0
        except Exception:
            return False
    if mode == "pending":
        if "needs_reply" in thread:
            return bool(thread.get("needs_reply"))
        return str(thread.get("last_message_direction") or "").strip().lower() == "inbound"
    return True


class InboxProjectionBuilder:
    _INBOUND_PLACEHOLDER = "Nuevo mensaje recibido"

    def __init__(self, context: ServiceContext) -> None:
        self.context = context
        self._lock = threading.RLock()
        self._conversation_cache_signature: tuple[tuple[int, int, int], tuple[int, int, int]] | None = None
        self._conversation_cache: tuple[dict[str, Any], dict[str, Any]] = ({}, {})
        self._legacy_cache_signature: (
            tuple[tuple[int, int, int], tuple[int, int, int], tuple[int, int, int], tuple[int, int, int]] | None
        ) = None
        self._legacy_cache: tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]] = ({}, {})

    def invalidate(self) -> None:
        with self._lock:
            self._conversation_cache_signature = None
            self._conversation_cache = ({}, {})
            self._legacy_cache_signature = None
            self._legacy_cache = ({}, {})

    def watch_files(self) -> list[Path]:
        return []

    def watch_directories(self) -> list[Path]:
        return []

    @staticmethod
    def _path_signature(path: Path) -> tuple[int, int, int]:
        try:
            stat = path.stat()
        except OSError:
            return (0, 0, 0)
        return (1, int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _clean_account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@")

    @staticmethod
    def _coerce_timestamp(value: Any) -> float | None:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except Exception:
            return None

    @classmethod
    def _thread_key_for(cls, account_id: Any, thread_id: Any) -> str:
        clean_account = cls._clean_account_id(account_id)
        clean_thread = str(thread_id or "").strip()
        if clean_account and clean_thread:
            return f"{clean_account}:{clean_thread}"
        return ""

    @classmethod
    def _message_key(cls, message: dict[str, Any]) -> str:
        message_id = str(message.get("message_id") or "").strip()
        if message_id:
            return message_id
        return "|".join(
            [
                str(message.get("direction") or "").strip().lower(),
                str(message.get("timestamp") or ""),
                str(message.get("text") or "").strip(),
            ]
        )

    @staticmethod
    def _message_from_log(action: str) -> str:
        lowered = str(action or "").strip().lower()
        if lowered == "message_received":
            return "inbound"
        if lowered in {"message_sent", "followup_sent"}:
            return "outbound"
        return ""

    @staticmethod
    def _direction_from_conversation(
        last_sender: str,
        last_sent_at: float | None,
        last_received_at: float | None,
    ) -> str:
        lowered = str(last_sender or "").strip().lower()
        if lowered == "lead":
            return "inbound"
        if lowered == "bot":
            return "outbound"
        if last_received_at and (last_sent_at is None or last_received_at >= last_sent_at):
            return "inbound"
        if last_sent_at:
            return "outbound"
        return "unknown"

    @staticmethod
    def _latest_outbound_text(conversation: dict[str, Any]) -> str:
        rows = conversation.get("messages_sent")
        if not isinstance(rows, list):
            return ""
        latest_text = ""
        latest_ts = -1.0
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            text = str(raw.get("text") or "").strip()
            if not text:
                continue
            try:
                stamp = float(raw.get("last_sent_at") or raw.get("first_sent_at") or 0.0)
            except Exception:
                stamp = 0.0
            if stamp >= latest_ts:
                latest_ts = stamp
                latest_text = text
        return latest_text

    def _active_account_ids_locked(self) -> set[str]:
        payload = self.context.read_json(self.context.accounts_path("accounts.json"), [])
        if not isinstance(payload, list):
            return set()
        active_accounts: set[str] = set()
        for raw in payload:
            if not isinstance(raw, dict):
                continue
            if not bool(raw.get("active", True)):
                continue
            username = self._clean_account_id(raw.get("username"))
            if username:
                active_accounts.add(username.lower())
        return active_accounts

    @classmethod
    def _conversation_identity(
        cls,
        conversation_key: str,
        payload: dict[str, Any],
    ) -> tuple[str, str]:
        account_id = cls._clean_account_id(payload.get("account") or payload.get("account_id"))
        thread_id = str(payload.get("thread_id") or "").strip()
        if (not account_id or not thread_id) and "|" in str(conversation_key or ""):
            raw_account, raw_thread = str(conversation_key or "").split("|", 1)
            account_id = account_id or cls._clean_account_id(raw_account)
            thread_id = thread_id or str(raw_thread or "").strip()
        return account_id, thread_id

    @classmethod
    def _conversation_key(cls, row: dict[str, Any]) -> str:
        account_id = cls._clean_account_id(row.get("account_id") or row.get("account"))
        thread_id = str(row.get("thread_id") or "").strip()
        if account_id and thread_id:
            return f"{account_id}|{thread_id}"
        return str(row.get("thread_key") or thread_id or "").strip()

    @classmethod
    def _merge_messages(
        cls,
        primary_rows: list[dict[str, Any]] | None,
        secondary_rows: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        for source_rows in (secondary_rows or [], primary_rows or []):
            for raw in source_rows:
                if not isinstance(raw, dict):
                    continue
                candidate = dict(raw)
                key = cls._message_key(candidate)
                if not key:
                    continue
                previous = deduped.get(key)
                candidate_ts = message_canonical_timestamp(candidate) or 0.0
                previous_ts = message_canonical_timestamp(previous) or 0.0
                if previous is None or candidate_ts >= previous_ts:
                    deduped[key] = candidate
        rows = list(deduped.values())
        rows.sort(
            key=lambda item: (
                message_canonical_timestamp(item) or 0.0,
                str(item.get("message_id") or "").strip(),
            )
        )
        return rows

    @classmethod
    def _apply_activity(
        cls,
        row: dict[str, Any],
        *,
        timestamp: float | None,
        direction: str,
        text: str = "",
        message_id: str = "",
        recipient_username: str = "",
    ) -> None:
        if recipient_username and not str(row.get("recipient_username") or "").strip():
            row["recipient_username"] = recipient_username
            row["display_name"] = recipient_username
            row["participants"] = [recipient_username]
        current_ts = cls._coerce_timestamp(row.get("last_message_timestamp"))
        if timestamp is None:
            return
        if current_ts is not None and timestamp < current_ts:
            return
        clean_text = str(text or "").strip()
        row["last_message_timestamp"] = timestamp
        row["last_message_direction"] = direction or str(row.get("last_message_direction") or "unknown")
        if clean_text:
            row["last_message_text"] = clean_text
        elif direction == "inbound" and not str(row.get("last_message_text") or "").strip():
            row["last_message_text"] = cls._INBOUND_PLACEHOLDER
        if message_id:
            row["last_message_id"] = message_id

    @classmethod
    def _append_message(
        cls,
        bucket: defaultdict[str, list[dict[str, Any]]],
        *,
        thread_key: str,
        direction: str,
        timestamp: float | None,
        message_id: str,
        text: str,
        user_id: str,
    ) -> None:
        if not thread_key or timestamp is None:
            return
        clean_text = str(text or "").strip()
        if direction == "inbound" and not clean_text:
            clean_text = cls._INBOUND_PLACEHOLDER
        if not clean_text and direction != "outbound":
            return
        bucket[thread_key].append(
            {
                "message_id": message_id,
                "text": clean_text,
                "timestamp": timestamp,
                "direction": direction,
                "user_id": user_id,
                "delivery_status": "sent",
                "local_echo": False,
            }
        )

    def _conversation_payloads_locked(self) -> tuple[dict[str, Any], dict[str, Any]]:
        engine_path = self.context.storage_path("conversation_engine.json")
        state_path = self.context.storage_path("conversation_state.json")
        signature = (
            self._path_signature(engine_path),
            self._path_signature(state_path),
        )
        if signature == self._conversation_cache_signature:
            return self._conversation_cache

        engine_payload = self.context.read_json(engine_path, {"conversations": {}})
        state_payload = self.context.read_json(state_path, {"conversations": {}})
        engine_conversations = dict(engine_payload.get("conversations") or {}) if isinstance(engine_payload, dict) else {}
        state_conversations = dict(state_payload.get("conversations") or {}) if isinstance(state_payload, dict) else {}
        self._conversation_cache_signature = signature
        self._conversation_cache = (engine_conversations, state_conversations)
        return self._conversation_cache

    @classmethod
    def _build_legacy_payloads(
        cls,
        *,
        engine_conversations: dict[str, Any],
        state_conversations: dict[str, Any],
        message_entries: list[dict[str, Any]],
        active_accounts: set[str],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        legacy_threads: dict[str, dict[str, Any]] = {}
        legacy_messages: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

        def _is_allowed(account_id: str) -> bool:
            if not active_accounts:
                return True
            return account_id.lower() in active_accounts

        def _ensure_thread(account_id: str, thread_id: str) -> dict[str, Any]:
            thread_key = cls._thread_key_for(account_id, thread_id)
            row = legacy_threads.get(thread_key)
            if isinstance(row, dict):
                return row
            row = {
                "thread_key": thread_key,
                "thread_id": thread_id,
                "thread_href": "",
                "account_id": account_id,
                "account_alias": "",
                "recipient_username": "",
                "display_name": thread_id,
                "last_message_text": "",
                "last_message_timestamp": None,
                "last_message_direction": "unknown",
                "last_message_id": "",
                "unread_count": 0,
                "participants": [],
                "last_synced_at": None,
                "last_seen_text": "",
                "last_seen_at": None,
                "latest_customer_message_at": None,
            }
            legacy_threads[thread_key] = row
            return row

        for entry in message_entries:
            if not isinstance(entry, dict):
                continue
            direction = cls._message_from_log(entry.get("action"))
            if not direction:
                continue
            account_id = cls._clean_account_id(entry.get("account"))
            thread_id = str(entry.get("thread_id") or "").strip()
            if not account_id or not thread_id or not _is_allowed(account_id):
                continue
            row = _ensure_thread(account_id, thread_id)
            recipient_username = str(entry.get("lead") or "").strip()
            timestamp = cls._coerce_timestamp(entry.get("ts"))
            message_id = str(entry.get("message_id") or "").strip()
            if not message_id:
                message_id = f"log:{direction}:{thread_id}:{int(timestamp or 0)}:{len(legacy_messages[row['thread_key']])}"
            text = str(entry.get("message_text") or "").strip()
            cls._append_message(
                legacy_messages,
                thread_key=str(row.get("thread_key") or ""),
                direction=direction,
                timestamp=timestamp,
                message_id=message_id,
                text=text,
                user_id=recipient_username if direction == "inbound" else account_id,
            )
            cls._apply_activity(
                row,
                timestamp=timestamp,
                direction=direction,
                text=text,
                message_id=message_id,
                recipient_username=recipient_username,
            )
            if direction == "inbound" and timestamp is not None:
                previous_inbound = cls._coerce_timestamp(row.get("latest_customer_message_at")) or 0.0
                row["latest_customer_message_at"] = max(previous_inbound, timestamp)

        for conversation_key, conversation in list(state_conversations.items()) + list(engine_conversations.items()):
            if not isinstance(conversation, dict):
                continue
            account_id, thread_id = cls._conversation_identity(conversation_key, conversation)
            if not account_id or not thread_id or not _is_allowed(account_id):
                continue
            row = _ensure_thread(account_id, thread_id)
            recipient_username = str(conversation.get("recipient_username") or "").strip()
            if recipient_username and not str(row.get("recipient_username") or "").strip():
                row["recipient_username"] = recipient_username
                row["display_name"] = recipient_username
                row["participants"] = [recipient_username]

            messages_sent = conversation.get("messages_sent")
            if isinstance(messages_sent, list):
                for idx, sent_row in enumerate(messages_sent):
                    if not isinstance(sent_row, dict):
                        continue
                    text = str(sent_row.get("text") or "").strip()
                    if not text:
                        continue
                    timestamp = cls._coerce_timestamp(sent_row.get("last_sent_at") or sent_row.get("first_sent_at"))
                    message_id = str(sent_row.get("last_message_id") or sent_row.get("message_id") or "").strip()
                    if not message_id:
                        message_id = f"legacy-out:{thread_id}:{idx}:{int(timestamp or 0)}"
                    cls._append_message(
                        legacy_messages,
                        thread_key=str(row.get("thread_key") or ""),
                        direction="outbound",
                        timestamp=timestamp,
                        message_id=message_id,
                        text=text,
                        user_id=account_id,
                    )
                    cls._apply_activity(
                        row,
                        timestamp=timestamp,
                        direction="outbound",
                        text=text,
                        message_id=message_id,
                        recipient_username=recipient_username,
                    )

            last_sent_at = cls._coerce_timestamp(conversation.get("last_message_sent_at"))
            last_received_at = cls._coerce_timestamp(conversation.get("last_message_received_at"))
            last_direction = cls._direction_from_conversation(
                str(conversation.get("last_message_sender") or ""),
                last_sent_at,
                last_received_at,
            )

            if last_received_at is not None:
                inbound_id = str(
                    conversation.get("last_inbound_id_seen") or conversation.get("last_message_id_seen") or ""
                ).strip()
                if not inbound_id:
                    inbound_id = f"legacy-in:{thread_id}:{int(last_received_at)}"
                cls._append_message(
                    legacy_messages,
                    thread_key=str(row.get("thread_key") or ""),
                    direction="inbound",
                    timestamp=last_received_at,
                    message_id=inbound_id,
                    text="",
                    user_id=recipient_username,
                )
                previous_inbound = cls._coerce_timestamp(row.get("latest_customer_message_at")) or 0.0
                row["latest_customer_message_at"] = max(previous_inbound, last_received_at)

            latest_timestamp = None
            if last_sent_at is not None and last_received_at is not None:
                latest_timestamp = max(last_sent_at, last_received_at)
            else:
                latest_timestamp = last_received_at if last_received_at is not None else last_sent_at
            latest_text = ""
            if last_direction == "outbound":
                latest_text = cls._latest_outbound_text(conversation)
            cls._apply_activity(
                row,
                timestamp=latest_timestamp,
                direction=last_direction,
                text=latest_text,
                message_id=str(conversation.get("last_message_id_seen") or "").strip(),
                recipient_username=recipient_username,
            )

        finalized_messages: dict[str, list[dict[str, Any]]] = {}
        for thread_key, messages in legacy_messages.items():
            merged = cls._merge_messages(messages, None)
            if merged:
                finalized_messages[thread_key] = merged

        for thread_key, row in legacy_threads.items():
            messages = finalized_messages.get(thread_key, [])
            if not messages:
                fallback_timestamp = cls._coerce_timestamp(row.get("last_message_timestamp"))
                fallback_direction = str(row.get("last_message_direction") or "").strip().lower() or "unknown"
                fallback_text = str(row.get("last_message_text") or "").strip()
                if fallback_direction == "inbound" and not fallback_text:
                    fallback_text = cls._INBOUND_PLACEHOLDER
                if fallback_timestamp is not None and fallback_text:
                    messages = [
                        {
                            "message_id": str(row.get("last_message_id") or "").strip()
                            or f"snapshot:{thread_key}:{int(fallback_timestamp)}",
                            "text": fallback_text,
                            "timestamp": fallback_timestamp,
                            "direction": fallback_direction,
                            "user_id": str(row.get("recipient_username") or "").strip()
                            if fallback_direction == "inbound"
                            else str(row.get("account_id") or "").strip(),
                            "delivery_status": "sent",
                            "local_echo": False,
                        }
                    ]
                    finalized_messages[thread_key] = messages
            if messages:
                latest = messages[-1]
                cls._apply_activity(
                    row,
                    timestamp=cls._coerce_timestamp(latest.get("timestamp")),
                    direction=str(latest.get("direction") or "").strip().lower() or "unknown",
                    text=str(latest.get("text") or "").strip(),
                    message_id=str(latest.get("message_id") or "").strip(),
                    recipient_username=str(row.get("recipient_username") or "").strip(),
                )
            if not str(row.get("display_name") or "").strip():
                row["display_name"] = str(row.get("recipient_username") or row.get("thread_id") or "Thread").strip()
            row["unread_count"] = 1 if str(row.get("last_message_direction") or "").strip().lower() == "inbound" else 0

        return legacy_threads, finalized_messages

    def _legacy_payloads_locked(self) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        engine_path = self.context.storage_path("conversation_engine.json")
        state_path = self.context.storage_path("conversation_state.json")
        message_log_path = self.context.storage_path("message_log.jsonl")
        accounts_path = self.context.accounts_path("accounts.json")
        signature = (
            self._path_signature(engine_path),
            self._path_signature(state_path),
            self._path_signature(message_log_path),
            self._path_signature(accounts_path),
        )
        if signature == self._legacy_cache_signature:
            return self._legacy_cache

        engine_conversations, state_conversations = self._conversation_payloads_locked()
        active_accounts = self._active_account_ids_locked()
        message_entries = self.context.read_jsonl(message_log_path)
        self._legacy_cache = self._build_legacy_payloads(
            engine_conversations=engine_conversations,
            state_conversations=state_conversations,
            message_entries=message_entries,
            active_accounts=active_accounts,
        )
        self._legacy_cache_signature = signature
        return self._legacy_cache

    def _decorate_thread_locked(
        self,
        row: dict[str, Any],
        *,
        engine_conversations: dict[str, Any],
        state_conversations: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row)
        conversation_key = self._conversation_key(payload)
        engine_row = engine_conversations.get(conversation_key)
        state_row = state_conversations.get(conversation_key)
        if not isinstance(engine_row, dict):
            engine_row = {}
        if not isinstance(state_row, dict):
            state_row = {}

        tags_raw = engine_row.get("tags") or state_row.get("tags") or []
        tags = [str(item).strip() for item in tags_raw if str(item or "").strip()] if isinstance(tags_raw, list) else []

        payload["conversation_id"] = conversation_key or str(payload.get("thread_id") or "").strip()
        payload["username"] = str(payload.get("recipient_username") or engine_row.get("recipient_username") or "").strip()
        payload["account"] = str(payload.get("account_id") or "").strip()
        payload["last_message"] = str(payload.get("last_message_text") or "").strip()
        payload["last_direction"] = str(payload.get("last_message_direction") or "").strip()
        payload["last_timestamp"] = payload.get("last_message_timestamp")
        payload["campaign_id"] = str(engine_row.get("campaign_id") or state_row.get("campaign_id") or "").strip()
        payload["lead_source"] = str(engine_row.get("source") or payload.get("account_alias") or "").strip()
        payload["operational_status"] = (
            str(payload.get("operational_status") or payload.get("status") or "").strip() or "open"
        )
        payload["status"] = payload["operational_status"]
        payload["ui_status"] = (
            str(payload.get("ui_status") or "").strip()
            or ("closed" if bool(state_row.get("cerrado")) else str(state_row.get("status") or "").strip())
        )
        payload["tags"] = tags
        payload["stage"] = str(
            payload.get("stage_id")
            or engine_row.get("stage")
            or state_row.get("stage")
            or state_row.get("seguimiento_actual")
            or ""
        ).strip()
        payload["owner"] = str(payload.get("owner") or "none").strip()
        payload["bucket"] = str(payload.get("bucket") or "all").strip()
        payload["last_action_type"] = str(payload.get("last_action_type") or "").strip()
        payload["last_pack_sent"] = str(payload.get("last_pack_sent") or "").strip()
        return payload

    def build_rows(self, live_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(row) for row in live_rows or [] if isinstance(row, dict)]
        rows.sort(
            key=lambda item: (
                -(self._coerce_timestamp(item.get("last_message_timestamp")) or 0.0),
                str(item.get("display_name") or "").lower(),
                str(item.get("thread_key") or "").strip(),
            )
        )
        return rows

    @classmethod
    def _message_anchor_timestamp(cls, message: dict[str, Any]) -> float | None:
        del cls
        return message_canonical_timestamp(message)

    @classmethod
    def _normalize_thread_messages(cls, rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        ordered = [dict(row) for row in rows or [] if isinstance(row, dict)]
        enumerated = list(enumerate(ordered))
        enumerated.sort(key=lambda pair: message_sort_key(pair[1], position=pair[0]))
        return [row for _, row in enumerated]

    def build_thread(self, thread_key: str, live_thread: dict[str, Any] | None) -> dict[str, Any] | None:
        del thread_key
        if not isinstance(live_thread, dict):
            return None
        payload = copy.deepcopy(live_thread)
        payload["messages"] = self._normalize_thread_messages(payload.get("messages"))
        return payload

    def legacy_seed(self, thread_key: str) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
        del thread_key
        return None


class InboxRuntime(QObject):
    cache_updated = Signal(object)
    snapshot_updated = Signal(object)
    thread_updated = Signal(object)

    def __init__(
        self,
        context: ServiceContext,
        engine: Any,
        parent: QObject | None = None,
        *,
        idle_backend_shutdown_seconds: float = _DEFAULT_IDLE_BACKEND_SHUTDOWN_SECONDS,
        idle_backend_retry_seconds: float = _DEFAULT_IDLE_BACKEND_RETRY_SECONDS,
    ) -> None:
        super().__init__(parent)
        self._context = context
        self._engine = engine
        self._builder = InboxProjectionBuilder(context)
        self._state_lock = threading.RLock()
        self._worker_lock = threading.Condition(threading.RLock())
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._watcher: QFileSystemWatcher | None = None
        self._engine_events_connected = False
        self._started = False
        self._backend_started = False
        self._ui_active = False
        self._worker_state = "stopped"
        self._worker_last_heartbeat_at: float | None = None
        self._worker_last_error = ""
        self._idle_backend_shutdown_seconds = max(0.1, float(idle_backend_shutdown_seconds or 0.1))
        self._idle_backend_retry_seconds = max(0.1, float(idle_backend_retry_seconds or 0.1))
        self._idle_shutdown_timer: threading.Timer | None = None
        self._idle_shutdown_token = 0
        self._snapshots: dict[str, list[dict[str, Any]]] = {"all": [], "unread": [], "pending": []}
        self._rows_by_key: dict[str, dict[str, Any]] = {}
        self._thread_cache: dict[str, dict[str, Any]] = {}
        self._metrics = {"threads": 0, "unread": 0, "pending": 0}
        self._pending_rebuild = True
        self._pending_full_rebuild = True
        self._pending_invalidate_legacy = False
        self._pending_reason = "bootstrap"
        self._pending_thread_keys: set[str] = set()
        self._pending_account_ids: set[str] = set()
        self._pending_open_thread_keys: set[str] = set()
        self._projection_ready = threading.Event()
        self._connect_engine_events()

    @property
    def events(self) -> "InboxRuntime":
        return self

    def start(self) -> None:
        with self._worker_lock:
            worker = self._worker
            if self._started and worker is not None and worker.is_alive():
                self._ensure_watcher()
                return
            if worker is not None and not worker.is_alive():
                self._worker = None
                self._started = False
            self._stop_event.clear()
            self._mark_worker_state("starting", clear_error=True)
            self._worker = threading.Thread(
                target=self._run_loop,
                name="inbox-runtime",
                daemon=True,
            )
            self._worker.start()
            self._started = True
        self._ensure_watcher()

    def shutdown(self) -> None:
        self._cancel_idle_backend_shutdown()
        with self._worker_lock:
            self._stop_event.set()
            self._worker_lock.notify_all()
            worker = self._worker
            self._worker = None
            self._started = False
        if worker is not None:
            worker.join(timeout=2.0)
        self._watcher = None
        with self._state_lock:
            self._backend_started = False
            self._ui_active = False
        self._mark_worker_state("stopped", clear_error=False)
        engine_shutdown = getattr(self._engine, "shutdown", None)
        if callable(engine_shutdown):
            engine_shutdown()

    def ensure_backend_started(self) -> None:
        self.start()
        self._cancel_idle_backend_shutdown()
        with self._state_lock:
            already_started = self._backend_started
            active = self._ui_active
        if not already_started:
            starter = getattr(self._engine, "start", None)
            if callable(starter):
                starter()
            with self._state_lock:
                self._backend_started = True
        self._set_engine_foreground(active)

    def set_ui_active(self, active: bool) -> None:
        with self._state_lock:
            self._ui_active = bool(active)
            backend_started = self._backend_started
        self.start()
        if active:
            self._cancel_idle_backend_shutdown()
        if backend_started:
            self._set_engine_foreground(active)
        elif active:
            self._cancel_idle_backend_shutdown()
        if active:
            self.request_sync(force=not backend_started)
            return
        if backend_started:
            self._schedule_idle_backend_shutdown()

    def request_sync(self, *, force: bool = False) -> None:
        self.ensure_backend_started()
        refresh = getattr(self._engine, "enqueue_periodic_sync", None)
        if not callable(refresh):
            return
        try:
            refresh(force=bool(force))
        except TypeError:
            refresh()

    def ensure_projection_ready(self, *, timeout: float = 2.0) -> bool:
        if self._projection_ready.is_set():
            return True
        self.start()
        with self._worker_lock:
            if not self._pending_rebuild:
                self._pending_rebuild = True
                self._pending_full_rebuild = True
                self._pending_reason = "bootstrap"
                self._worker_lock.notify_all()
        return self._projection_ready.wait(max(0.05, float(timeout or 0.05)))

    def is_projection_ready(self) -> bool:
        return self._projection_ready.is_set()

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        mode = str(filter_mode or "all").strip().lower() or "all"
        if mode not in self._snapshots:
            mode = "all"
        with self._state_lock:
            return [dict(row) for row in self._snapshots.get(mode, [])]

    def get_thread_cached(self, thread_key: str) -> dict[str, Any] | None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return None
        with self._state_lock:
            cached = self._thread_cache.get(clean_key)
            if isinstance(cached, dict):
                return copy.deepcopy(cached)
        live_thread = self._safe_engine_get_thread(clean_key)
        detail = self._builder.build_thread(clean_key, live_thread)
        if not isinstance(detail, dict):
            return None
        with self._state_lock:
            self._thread_cache[clean_key] = copy.deepcopy(detail)
            return copy.deepcopy(detail)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return None
        with self._state_lock:
            cached = self._thread_cache.get(clean_key)
            if isinstance(cached, dict):
                return copy.deepcopy(cached)
        live_thread = self._safe_engine_get_thread(clean_key)
        detail = self._builder.build_thread(clean_key, live_thread)
        if not isinstance(detail, dict):
            return None
        with self._state_lock:
            self._thread_cache[clean_key] = copy.deepcopy(detail)
            return copy.deepcopy(detail)

    def metrics(self) -> dict[str, int]:
        with self._state_lock:
            return dict(self._metrics)

    def ensure_thread_seeded(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        return isinstance(self._safe_engine_get_thread(clean_key), dict)

    def request_thread_open(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        self.start()
        with self._worker_lock:
            self._pending_rebuild = True
            self._pending_reason = "open_thread"
            self._pending_thread_keys.add(clean_key)
            self._pending_open_thread_keys.add(clean_key)
            self._worker_lock.notify_all()

    def request_rebuild(
        self,
        *,
        reason: str,
        thread_keys: list[str] | None = None,
        account_ids: list[str] | None = None,
        full: bool = False,
        invalidate_legacy: bool = False,
    ) -> None:
        self.start()
        clean_keys = {
            str(item or "").strip()
            for item in thread_keys or []
            if str(item or "").strip()
        }
        clean_accounts = {
            self._builder._clean_account_id(item)
            for item in account_ids or []
            if self._builder._clean_account_id(item)
        }
        with self._worker_lock:
            self._pending_rebuild = True
            self._pending_reason = str(reason or "").strip() or self._pending_reason or "refresh"
            self._pending_full_rebuild = self._pending_full_rebuild or bool(full)
            self._pending_invalidate_legacy = self._pending_invalidate_legacy or bool(invalidate_legacy)
            self._pending_thread_keys.update(clean_keys)
            self._pending_account_ids.update(clean_accounts)
            self._worker_lock.notify_all()

    def diagnostics(self) -> dict[str, Any]:
        with self._worker_lock:
            worker = self._worker
            worker_alive = bool(worker is not None and worker.is_alive())
        with self._state_lock:
            return {
                "projection_threads": len(self._rows_by_key),
                "projection_unread": int(self._metrics.get("unread") or 0),
                "projection_pending": int(self._metrics.get("pending") or 0),
                "cached_thread_details": len(self._thread_cache),
                "backend_started": bool(self._backend_started and worker_alive),
                "ui_active": bool(self._ui_active),
                "projection_ready": bool(self._projection_ready.is_set() and worker_alive),
                "runtime_worker_alive": worker_alive,
                "runtime_worker_state": str(self._worker_state or "stopped").strip() or "stopped",
                "runtime_worker_last_heartbeat_at": self._worker_last_heartbeat_at,
                "runtime_worker_last_error": str(self._worker_last_error or "").strip(),
            }

    def _cancel_idle_backend_shutdown(self) -> None:
        timer: threading.Timer | None = None
        with self._state_lock:
            self._idle_shutdown_token += 1
            timer = self._idle_shutdown_timer
            self._idle_shutdown_timer = None
        if timer is not None:
            timer.cancel()

    def _schedule_idle_backend_shutdown(self, *, delay_seconds: float | None = None) -> None:
        self._cancel_idle_backend_shutdown()
        delay = max(0.1, float(delay_seconds or self._idle_backend_shutdown_seconds))
        with self._state_lock:
            if self._ui_active or not self._backend_started:
                return
            self._idle_shutdown_token += 1
            token = self._idle_shutdown_token
        timer = threading.Timer(delay, lambda: self._attempt_idle_backend_shutdown(token))
        timer.daemon = True
        with self._state_lock:
            if token != self._idle_shutdown_token or self._ui_active or not self._backend_started:
                return
            self._idle_shutdown_timer = timer
        timer.start()

    def _attempt_idle_backend_shutdown(self, token: int) -> None:
        with self._state_lock:
            if token != self._idle_shutdown_token:
                return
            self._idle_shutdown_timer = None
            if self._ui_active or not self._backend_started:
                return
        if self._engine_has_pending_work():
            self._schedule_idle_backend_shutdown(delay_seconds=self._idle_backend_retry_seconds)
            return
        engine_shutdown = getattr(self._engine, "shutdown", None)
        if callable(engine_shutdown):
            engine_shutdown()
        with self._state_lock:
            if token == self._idle_shutdown_token and not self._ui_active:
                self._backend_started = False

    def _engine_has_pending_work(self) -> bool:
        diagnostics = getattr(self._engine, "diagnostics", None)
        if not callable(diagnostics):
            return False
        try:
            payload = diagnostics() or {}
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        counters = (
            payload.get("queued_tasks"),
            payload.get("dedupe_pending"),
            payload.get("reader_active_tasks"),
        )
        for raw in counters:
            try:
                if int(raw or 0) > 0:
                    return True
            except Exception:
                continue
        return False

    def _connect_engine_events(self) -> None:
        if self._engine_events_connected:
            return
        events = getattr(self._engine, "events", self._engine)
        cache_signal = getattr(events, "cache_updated", None)
        if cache_signal is None or not hasattr(cache_signal, "connect"):
            return
        cache_signal.connect(self._on_engine_cache_updated)
        self._engine_events_connected = True

    def _ensure_watcher(self) -> None:
        if self._watcher is not None:
            self._sync_watcher_paths()
            return
        if QCoreApplication.instance() is None:
            return
        watcher = QFileSystemWatcher(self)
        watcher.fileChanged.connect(self._on_legacy_path_changed)
        watcher.directoryChanged.connect(self._on_legacy_path_changed)
        self._watcher = watcher
        self._sync_watcher_paths()

    def _sync_watcher_paths(self) -> None:
        watcher = self._watcher
        if watcher is None:
            return
        expected = {
            str(path)
            for path in [*self._builder.watch_files(), *self._builder.watch_directories()]
            if path.exists()
        }
        current = set(watcher.files()) | set(watcher.directories())
        to_remove = [path for path in current if path not in expected]
        if to_remove:
            watcher.removePaths(to_remove)
        to_add = [path for path in expected if path not in current]
        if to_add:
            watcher.addPaths(to_add)

    def _set_engine_foreground(self, active: bool) -> None:
        setter = getattr(self._engine, "set_foreground_active", None)
        if callable(setter):
            setter(bool(active))

    def _run_loop(self) -> None:
        current_thread = threading.current_thread()
        self._mark_worker_state("running", clear_error=True)
        try:
            while True:
                with self._worker_lock:
                    while not self._stop_event.is_set() and not self._pending_rebuild:
                        self._mark_worker_state("idle", clear_error=False)
                        self._worker_lock.wait(timeout=0.5)
                    if self._stop_event.is_set():
                        return
                    self._mark_worker_state("running", clear_error=False)
                    reason = self._pending_reason or "refresh"
                    full = self._pending_full_rebuild
                    invalidate_legacy = self._pending_invalidate_legacy
                    thread_keys = list(self._pending_thread_keys)
                    account_ids = list(self._pending_account_ids)
                    open_thread_keys = list(self._pending_open_thread_keys)
                    self._pending_rebuild = False
                    self._pending_full_rebuild = False
                    self._pending_invalidate_legacy = False
                    self._pending_thread_keys.clear()
                    self._pending_account_ids.clear()
                    self._pending_open_thread_keys.clear()
                for thread_key in open_thread_keys:
                    if thread_key:
                        self._mark_worker_state("running", clear_error=False)
                        self._open_thread_in_worker(thread_key)
                if open_thread_keys:
                    for thread_key in open_thread_keys:
                        if thread_key:
                            thread_keys.append(thread_key)
                self._mark_worker_state("running", clear_error=False)
                self._rebuild_projection(
                    reason=reason,
                    full=full,
                    thread_keys=thread_keys,
                    account_ids=account_ids,
                    invalidate_legacy=invalidate_legacy,
                )
        except Exception as exc:
            logger.exception("Inbox runtime worker crashed")
            self._projection_ready.clear()
            self._mark_worker_state("error", error=f"{type(exc).__name__}: {exc}", clear_error=False)
            engine_shutdown = getattr(self._engine, "shutdown", None)
            if callable(engine_shutdown):
                try:
                    engine_shutdown()
                except Exception:
                    logger.exception("Inbox runtime worker could not shutdown engine cleanly after crash")
            with self._state_lock:
                self._backend_started = False
        finally:
            with self._worker_lock:
                if self._worker is current_thread:
                    self._worker = None
                self._started = False
            if self._stop_event.is_set():
                self._mark_worker_state("stopped", clear_error=False)

    def _mark_worker_state(
        self,
        state: str,
        *,
        error: str | None = None,
        clear_error: bool,
    ) -> None:
        with self._state_lock:
            self._worker_state = str(state or "stopped").strip() or "stopped"
            self._worker_last_heartbeat_at = time.time()
            if clear_error:
                self._worker_last_error = ""
            elif error is not None:
                self._worker_last_error = str(error or "").strip()

    def _safe_engine_list_threads(self) -> list[dict[str, Any]]:
        try:
            rows = self._engine.list_threads("all")
        except Exception:
            return []
        return [dict(row) for row in rows or [] if isinstance(row, dict)]

    def _open_thread_in_worker(self, thread_key: str) -> bool:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return False
        self.ensure_backend_started()
        live_thread = self._safe_engine_get_thread(clean_key)
        if not isinstance(live_thread, dict):
            return False
        opener = getattr(self._engine, "open_thread", None)
        if not callable(opener):
            return True
        try:
            opened = bool(opener(clean_key))
        except Exception:
            return False
        if opened:
            self._hydrate_thread_after_open(clean_key, live_thread)
        return opened

    def _hydrate_thread_after_open(self, thread_key: str, live_thread: dict[str, Any]) -> bool:
        if not self._thread_needs_full_hydration(live_thread):
            return False
        hydrator = getattr(self._engine, "hydrate_thread", None)
        if not callable(hydrator):
            return False
        try:
            return bool(hydrator(thread_key))
        except Exception:
            return False

    @staticmethod
    def _thread_needs_full_hydration(thread: dict[str, Any] | None) -> bool:
        if not isinstance(thread, dict):
            return False
        messages = [dict(item) for item in thread.get("messages") or [] if isinstance(item, dict)]
        if not messages:
            return True
        return any(message_canonical_timestamp(message) is None for message in messages)

    def _safe_engine_get_thread(self, thread_key: str) -> dict[str, Any] | None:
        getter = getattr(self._engine, "get_thread", None)
        if not callable(getter):
            return None
        try:
            payload = getter(thread_key)
        except Exception:
            return None
        return dict(payload) if isinstance(payload, dict) else None

    def _rebuild_projection(
        self,
        *,
        reason: str,
        full: bool,
        thread_keys: list[str] | None = None,
        account_ids: list[str] | None = None,
        invalidate_legacy: bool = False,
    ) -> None:
        del full
        if invalidate_legacy:
            self._builder.invalidate()
        rows = self._builder.build_rows(self._safe_engine_list_threads())
        rows_by_key = {
            str(row.get("thread_key") or "").strip(): dict(row)
            for row in rows
            if str(row.get("thread_key") or "").strip()
        }
        unread_rows = [dict(row) for row in rows if _include_thread(row, "unread")]
        pending_rows = [dict(row) for row in rows if _include_thread(row, "pending")]
        invalidated_keys = {str(item or "").strip() for item in thread_keys or [] if str(item or "").strip()}
        detail_cache: dict[str, dict[str, Any]] = {}
        for thread_key in sorted(invalidated_keys):
            detail = self._builder.build_thread(thread_key, self._safe_engine_get_thread(thread_key))
            if isinstance(detail, dict):
                detail_cache[thread_key] = detail
        invalidated_accounts = {
            self._builder._clean_account_id(item)
            for item in account_ids or []
            if self._builder._clean_account_id(item)
        }
        payload = {
            "reason": str(reason or "").strip() or "refresh",
            "thread_keys": sorted(invalidated_keys),
            "account_ids": sorted(invalidated_accounts),
            "updated_at": time.time(),
        }
        with self._state_lock:
            previous_rows_by_key = dict(self._rows_by_key)
            existing_thread_keys = set(previous_rows_by_key)
            known_thread_keys = set(rows_by_key)
            removed_keys = existing_thread_keys - known_thread_keys
            self._rows_by_key = rows_by_key
            self._snapshots = {
                "all": [dict(row) for row in rows],
                "unread": unread_rows,
                "pending": pending_rows,
            }
            self._metrics = {
                "threads": len(rows),
                "unread": len(unread_rows),
                "pending": len(pending_rows),
            }
            if not invalidated_keys and not invalidated_accounts:
                self._thread_cache = {}
            else:
                affected_keys = set(invalidated_keys) | set(removed_keys)
                if invalidated_accounts:
                    for source in (previous_rows_by_key, rows_by_key):
                        for key, row in source.items():
                            if self._builder._clean_account_id((row or {}).get("account_id")) in invalidated_accounts:
                                affected_keys.add(key)
                for key in affected_keys:
                    self._thread_cache.pop(key, None)
            for key, detail in detail_cache.items():
                self._thread_cache[key] = copy.deepcopy(detail)
        self._projection_ready.set()
        self.snapshot_updated.emit(dict(payload))
        self.cache_updated.emit(dict(payload))
        for thread_key in sorted(invalidated_keys):
            self.thread_updated.emit({"thread_key": thread_key, "updated_at": payload["updated_at"]})

    def _on_engine_cache_updated(self, payload: Any) -> None:
        thread_keys: list[str] = []
        account_ids: list[str] = []
        reason = "engine_cache_updated"
        if isinstance(payload, dict):
            thread_keys = [
                str(item or "").strip()
                for item in payload.get("thread_keys") or []
                if str(item or "").strip()
            ]
            account_ids = [
                str(item or "").strip()
                for item in payload.get("account_ids") or []
                if str(item or "").strip()
            ]
            reason = str(payload.get("reason") or "").strip() or reason
        self.request_rebuild(reason=reason, thread_keys=thread_keys, account_ids=account_ids)

    def _on_legacy_path_changed(self, _path: str) -> None:
        self._sync_watcher_paths()
        self.request_rebuild(reason="legacy_changed", full=True, invalidate_legacy=True)
