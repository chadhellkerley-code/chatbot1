from __future__ import annotations

from pathlib import Path
from typing import Any

from src.inbox.inbox_storage import InboxStorage


class InboxCache:
    """Thread-safe in-memory inbox cache backed by the existing storage layer."""

    def __init__(self, root_dir: Path) -> None:
        self._storage = InboxStorage(root_dir)

    def snapshot(self) -> dict[str, Any]:
        return self._storage.snapshot()

    def flush(self) -> None:
        self._storage.flush()

    def stats(self) -> dict[str, int]:
        return self._storage.stats()

    def list_threads(self, filter_mode: str = "all") -> list[dict[str, Any]]:
        return self._storage.get_threads(filter_mode)

    def get_thread(self, thread_key: str) -> dict[str, Any] | None:
        return self._storage.get_thread(thread_key)

    def get_messages(self, thread_key: str) -> list[dict[str, Any]]:
        return self._storage.get_messages(thread_key)

    def has_thread(self, thread_key: str) -> bool:
        return isinstance(self.get_thread(thread_key), dict)

    def upsert_threads(self, thread_rows: list[dict[str, Any]]) -> None:
        self._storage.upsert_threads(thread_rows)

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

    def append_local_outbound_message(self, thread_key: str, text: str) -> dict[str, Any] | None:
        return self._storage.append_local_outbound_message(thread_key, text)

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

    def update_thread_state(self, thread_key: str, updates: dict[str, Any]) -> None:
        self._storage.update_thread_state(thread_key, updates)

    def register_account_sync(
        self,
        account_id: str,
        *,
        last_error: str = "",
        thread_count: int | None = None,
    ) -> None:
        self._storage.register_account_sync(
            account_id,
            last_error=last_error,
            thread_count=thread_count,
        )

    def prepare_account_session(
        self,
        account_id: str,
        *,
        session_marker: str,
        started_at: float | None = None,
    ) -> float | None:
        return self._storage.prepare_account_session(
            account_id,
            session_marker=session_marker,
            started_at=started_at,
        )

    def account_session_started_at(self, account_id: str) -> float | None:
        return self._storage.account_session_started_at(account_id)

    def prune_accounts(self, account_ids: set[str]) -> None:
        self._storage.prune_accounts(account_ids)

    def prime_thread_snapshot(
        self,
        thread_row: dict[str, Any],
        *,
        messages: list[dict[str, Any]] | None = None,
    ) -> bool:
        clean_row = dict(thread_row or {})
        thread_key = str(clean_row.get("thread_key") or "").strip()
        if not thread_key:
            return False
        if not self.has_thread(thread_key):
            self.upsert_threads([clean_row])
        if messages and not self.get_messages(thread_key):
            self.replace_messages(
                thread_key,
                list(messages),
                participants=list(clean_row.get("participants") or []),
                mark_read=False,
            )
        return True
