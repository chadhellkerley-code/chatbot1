from __future__ import annotations

import threading
from collections import defaultdict
from pathlib import Path
from typing import Any

from core.storage_atomic import load_json_file, load_jsonl_entries
from paths import storage_root


class LegacyConversationProjection:
    _INBOUND_PLACEHOLDER = "Nuevo mensaje recibido"

    def __init__(self, root_dir: Path) -> None:
        storage_dir = storage_root(Path(root_dir))
        self._engine_path = storage_dir / "conversation_engine.json"
        self._state_path = storage_dir / "conversation_state.json"
        self._message_log_path = storage_dir / "message_log.jsonl"
        self._lock = threading.RLock()
        self._signature: tuple[tuple[int, int, int], tuple[int, int, int], tuple[int, int, int]] | None = None
        self._threads_by_account: dict[str, dict[str, dict[str, Any]]] = {}
        self._messages_by_thread: dict[str, list[dict[str, Any]]] = {}

    def invalidate(self) -> None:
        with self._lock:
            self._signature = None
            self._threads_by_account = {}
            self._messages_by_thread = {}

    def load_account(
        self,
        account_id: str,
        *,
        account_alias: str = "",
    ) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        clean_account = self._clean_account_id(account_id)
        if not clean_account:
            return {}, {}
        self._ensure_cache_loaded()
        with self._lock:
            rows = {
                thread_key: dict(row)
                for thread_key, row in self._threads_by_account.get(clean_account, {}).items()
            }
            messages = {
                thread_key: [dict(item) for item in self._messages_by_thread.get(thread_key, [])]
                for thread_key in rows
            }
        clean_alias = str(account_alias or "").strip()
        if clean_alias:
            for row in rows.values():
                if not str(row.get("account_alias") or "").strip():
                    row["account_alias"] = clean_alias
        return rows, messages

    def _ensure_cache_loaded(self) -> None:
        signature = (
            self._path_signature(self._engine_path),
            self._path_signature(self._state_path),
            self._path_signature(self._message_log_path),
        )
        with self._lock:
            if signature == self._signature:
                return
        engine_payload = load_json_file(self._engine_path, {"conversations": {}}, label="inbox.legacy.engine")
        state_payload = load_json_file(self._state_path, {"conversations": {}}, label="inbox.legacy.state")
        message_entries = load_jsonl_entries(self._message_log_path, label="inbox.legacy.message_log")
        threads_by_account, messages_by_thread = self._build_projection(
            engine_conversations=engine_payload.get("conversations") if isinstance(engine_payload, dict) else {},
            state_conversations=state_payload.get("conversations") if isinstance(state_payload, dict) else {},
            message_entries=message_entries,
        )
        with self._lock:
            self._signature = signature
            self._threads_by_account = threads_by_account
            self._messages_by_thread = messages_by_thread

    @classmethod
    def _build_projection(
        cls,
        *,
        engine_conversations: Any,
        state_conversations: Any,
        message_entries: Any,
    ) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
        threads_by_account: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        messages_bucket: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

        def ensure_thread(account_id: str, thread_id: str) -> dict[str, Any]:
            thread_key = cls._thread_key(account_id, thread_id)
            row = threads_by_account[account_id].get(thread_key)
            if isinstance(row, dict):
                return row
            row = {
                "thread_key": thread_key,
                "thread_id": thread_id,
                "thread_href": f"https://www.instagram.com/direct/t/{thread_id}/",
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
                "latest_customer_message_at": None,
                "pack_sent_at": None,
                "pack_name": "",
            }
            threads_by_account[account_id][thread_key] = row
            return row

        for entry in message_entries if isinstance(message_entries, list) else []:
            if not isinstance(entry, dict):
                continue
            direction = cls._message_direction_from_action(entry.get("action"))
            if not direction:
                continue
            account_id = cls._clean_account_id(entry.get("account"))
            thread_id = str(entry.get("thread_id") or "").strip()
            if not account_id or not thread_id:
                continue
            row = ensure_thread(account_id, thread_id)
            recipient_username = str(entry.get("lead") or "").strip().lstrip("@")
            message_id = str(entry.get("message_id") or "").strip()
            timestamp = cls._coerce_timestamp(entry.get("ts"))
            text = str(entry.get("message_text") or "").strip()
            if not message_id:
                message_id = f"log:{direction}:{thread_id}:{int(timestamp or 0)}:{len(messages_bucket[row['thread_key']])}"
            cls._append_message(
                messages_bucket,
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
                previous = cls._coerce_timestamp(row.get("latest_customer_message_at")) or 0.0
                row["latest_customer_message_at"] = max(previous, timestamp)

        combined_conversations: list[tuple[str, dict[str, Any]]] = []
        for source in (state_conversations, engine_conversations):
            if not isinstance(source, dict):
                continue
            for conversation_key, raw in source.items():
                if isinstance(raw, dict):
                    combined_conversations.append((str(conversation_key or ""), dict(raw)))

        for conversation_key, conversation in combined_conversations:
            account_id, thread_id = cls._conversation_identity(conversation_key, conversation)
            if not account_id or not thread_id:
                continue
            row = ensure_thread(account_id, thread_id)
            recipient_username = str(conversation.get("recipient_username") or "").strip().lstrip("@")
            display_name = str(
                conversation.get("title")
                or conversation.get("display_name")
                or recipient_username
                or thread_id
            ).strip()
            thread_href = str(conversation.get("thread_href") or "").strip()
            if thread_href:
                row["thread_href"] = thread_href
            if recipient_username:
                row["recipient_username"] = recipient_username
                row["participants"] = [recipient_username]
            if display_name:
                row["display_name"] = display_name

            for index, sent_row in enumerate(conversation.get("messages_sent") or []):
                if not isinstance(sent_row, dict):
                    continue
                timestamp = cls._coerce_timestamp(sent_row.get("last_sent_at") or sent_row.get("first_sent_at"))
                text = str(sent_row.get("text") or "").strip()
                if not text:
                    continue
                message_id = str(
                    sent_row.get("last_message_id")
                    or sent_row.get("message_id")
                    or sent_row.get("id")
                    or ""
                ).strip()
                if not message_id:
                    message_id = f"legacy-out:{thread_id}:{index}:{int(timestamp or 0)}"
                cls._append_message(
                    messages_bucket,
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
            pack_at, pack_id, pack_name = cls._pack_signal(conversation)
            if pack_at is not None:
                row["pack_sent_at"] = pack_at
            if pack_name:
                row["pack_name"] = pack_name
            if last_received_at is not None:
                inbound_id = str(
                    conversation.get("last_inbound_id_seen")
                    or conversation.get("last_message_id_seen")
                    or ""
                ).strip()
                if not inbound_id:
                    inbound_id = f"legacy-in:{thread_id}:{int(last_received_at)}"
                cls._append_message(
                    messages_bucket,
                    thread_key=str(row.get("thread_key") or ""),
                    direction="inbound",
                    timestamp=last_received_at,
                    message_id=inbound_id,
                    text="",
                    user_id=recipient_username,
                )
                previous = cls._coerce_timestamp(row.get("latest_customer_message_at")) or 0.0
                row["latest_customer_message_at"] = max(previous, last_received_at)

            latest_text = ""
            latest_timestamp = max(
                (stamp for stamp in (last_sent_at, last_received_at, pack_at) if stamp is not None),
                default=None,
            )
            if latest_timestamp is not None:
                if latest_timestamp == last_received_at:
                    latest_direction = "inbound"
                else:
                    latest_direction = "outbound"
                    latest_text = cls._latest_outbound_text(conversation) or pack_name
                cls._apply_activity(
                    row,
                    timestamp=latest_timestamp,
                    direction=latest_direction,
                    text=latest_text,
                    message_id=str(conversation.get("last_message_id_seen") or "").strip(),
                    recipient_username=recipient_username,
                )
                if latest_direction == "outbound":
                    thread_key = str(row.get("thread_key") or "").strip()
                    # When the legacy engine indicates a newer outbound activity (pack/followup) but we don't have the
                    # corresponding message row, ensure the message list can reflect the true latest timestamp.
                    if thread_key:
                        has_outbound_snapshot = False
                        for existing in messages_bucket.get(thread_key, []):
                            if not isinstance(existing, dict):
                                continue
                            if str(existing.get("direction") or "").strip().lower() != "outbound":
                                continue
                            existing_ts = cls._coerce_timestamp(existing.get("timestamp"))
                            if existing_ts is not None and abs(existing_ts - latest_timestamp) <= 0.000001:
                                has_outbound_snapshot = True
                                break
                        if not has_outbound_snapshot:
                            cls._append_message(
                                messages_bucket,
                                thread_key=thread_key,
                                direction="outbound",
                                timestamp=latest_timestamp,
                                message_id=f"legacy-snapshot:out:{thread_id}:{int(latest_timestamp)}",
                                text=latest_text,
                                user_id=account_id,
                            )
            if pack_at is not None:
                row["_legacy_pack_sent_at"] = pack_at
            if pack_id:
                row["_legacy_pack_id"] = pack_id
            if pack_name:
                row["_legacy_pack_name"] = pack_name

        finalized_messages: dict[str, list[dict[str, Any]]] = {}
        for account_threads in threads_by_account.values():
            for thread_key, row in account_threads.items():
                messages = cls._merge_messages(messages_bucket.get(thread_key, []))
                if not messages:
                    fallback_timestamp = cls._coerce_timestamp(row.get("last_message_timestamp"))
                    fallback_direction = str(row.get("last_message_direction") or "").strip().lower() or "unknown"
                    fallback_text = str(row.get("last_message_text") or "").strip()
                    if fallback_direction == "inbound" and not fallback_text:
                        fallback_text = cls._INBOUND_PLACEHOLDER
                    if fallback_timestamp is not None and fallback_text:
                        fallback_message_id = str(row.get("last_message_id") or "").strip()
                        messages = [
                            {
                                "message_id": fallback_message_id or f"snapshot:{thread_key}:{int(fallback_timestamp)}",
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
                if messages:
                    finalized_messages[thread_key] = messages
                    latest = messages[-1]
                    cls._apply_activity(
                        row,
                        timestamp=cls._coerce_timestamp(latest.get("timestamp")),
                        direction=str(latest.get("direction") or "").strip().lower() or "unknown",
                        text=str(latest.get("text") or "").strip(),
                        message_id=str(latest.get("message_id") or "").strip(),
                        recipient_username=str(row.get("recipient_username") or "").strip(),
                    )
                row["unread_count"] = 1 if str(row.get("last_message_direction") or "").strip().lower() == "inbound" else 0
        return dict(threads_by_account), finalized_messages

    @staticmethod
    def _path_signature(path: Path) -> tuple[int, int, int]:
        try:
            stat = path.stat()
        except OSError:
            return (0, 0, 0)
        return (1, int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _clean_account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()

    @staticmethod
    def _coerce_timestamp(value: Any) -> float | None:
        try:
            stamp = float(value)
        except Exception:
            return None
        return stamp if stamp > 0 else None

    @classmethod
    def _thread_key(cls, account_id: str, thread_id: str) -> str:
        clean_account = cls._clean_account_id(account_id)
        clean_thread = str(thread_id or "").strip()
        if not clean_account or not clean_thread:
            return ""
        return f"{clean_account}:{clean_thread}"

    @staticmethod
    def _message_direction_from_action(action: Any) -> str:
        lowered = str(action or "").strip().lower()
        if lowered == "message_received":
            return "inbound"
        if lowered in {"message_sent", "followup_sent"}:
            return "outbound"
        return ""

    @classmethod
    def _conversation_identity(cls, conversation_key: str, payload: dict[str, Any]) -> tuple[str, str]:
        account_id = cls._clean_account_id(payload.get("account") or payload.get("account_id"))
        thread_id = str(
            payload.get("thread_id_real")
            or payload.get("thread_id")
            or payload.get("thread_id_api")
            or ""
        ).strip()
        if (not account_id or not thread_id) and "|" in str(conversation_key or ""):
            raw_account, raw_thread = str(conversation_key or "").split("|", 1)
            account_id = account_id or cls._clean_account_id(raw_account)
            thread_id = thread_id or str(raw_thread or "").strip()
        return account_id, thread_id

    @classmethod
    def _pack_signal(cls, conversation: dict[str, Any]) -> tuple[float | None, str, str]:
        pending_pack = conversation.get("pending_pack_run")
        pending = pending_pack if isinstance(pending_pack, dict) else {}
        pack_id = str((pending or {}).get("pack_id") or "").strip()
        pack_name = str(
            (pending or {}).get("pack_name")
            or (pending or {}).get("strategy_name")
            or ""
        ).strip()
        flow_state = conversation.get("flow_state") if isinstance(conversation.get("flow_state"), dict) else {}
        outbox = flow_state.get("outbox") if isinstance(flow_state, dict) else {}
        sent_stamps: list[float] = []
        if isinstance(outbox, dict):
            for raw in outbox.values():
                if not isinstance(raw, dict):
                    continue
                status = str(raw.get("status") or "").strip().lower()
                if status != "sent":
                    continue
                stamp = cls._coerce_timestamp(raw.get("sent_at") or raw.get("started_at"))
                if stamp is not None:
                    sent_stamps.append(stamp)
        candidates = [
            max(sent_stamps) if sent_stamps else None,
            cls._coerce_timestamp(conversation.get("last_followup_sent_at")),
        ]
        if pending:
            candidates.append(cls._coerce_timestamp(conversation.get("last_message_sent_at")))
        pack_at = max((stamp for stamp in candidates if stamp is not None), default=None)
        return pack_at, pack_id, pack_name

    @staticmethod
    def _latest_outbound_text(conversation: dict[str, Any]) -> str:
        rows = conversation.get("messages_sent")
        if not isinstance(rows, list):
            return ""
        latest_text = ""
        latest_timestamp = -1.0
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
            if stamp >= latest_timestamp:
                latest_timestamp = stamp
                latest_text = text
        return latest_text

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
                "message_id": str(message_id or "").strip(),
                "text": clean_text,
                "timestamp": timestamp,
                "direction": direction,
                "user_id": str(user_id or "").strip(),
                "delivery_status": "sent",
                "local_echo": False,
            }
        )

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
            row["last_message_id"] = str(message_id).strip()

    @classmethod
    def _message_key(cls, message: dict[str, Any]) -> str:
        message_id = str(message.get("message_id") or "").strip()
        if message_id:
            return f"id:{message_id}"
        return "|".join(
            [
                str(message.get("direction") or "").strip().lower(),
                str(message.get("timestamp") or ""),
                str(message.get("text") or "").strip(),
            ]
        )

    @classmethod
    def _merge_messages(cls, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            message = dict(raw)
            key = cls._message_key(message)
            if not key:
                continue
            previous = deduped.get(key)
            message_ts = cls._coerce_timestamp(message.get("timestamp")) or 0.0
            previous_ts = cls._coerce_timestamp((previous or {}).get("timestamp")) or 0.0
            if previous is None or message_ts >= previous_ts:
                deduped[key] = message
        merged = list(deduped.values())
        merged.sort(
            key=lambda item: (
                cls._coerce_timestamp(item.get("timestamp")) or 0.0,
                str(item.get("message_id") or "").strip(),
            )
        )
        return merged
