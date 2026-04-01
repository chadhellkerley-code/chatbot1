from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import health_store
from core.accounts import has_playwright_storage_state, playwright_storage_state_path

from src.inbox.endpoint_reader import InboxEndpointError, sync_account_threads_from_storage

from .conversation_store import ConversationStore


class ConversationReader:
    def __init__(
        self,
        store: ConversationStore,
        *,
        accounts_provider: Callable[[], list[dict[str, Any]]],
        notifier: Callable[..., None],
        interval_seconds: float = 5.0,  # PERF: tighter poll interval reduces inbound receive latency
        parallelism: int = 8,
        thread_limit: int = 50,
        message_limit: int = 12,
        timeout_seconds: float = 8.0,  # PERF: shorter per-account timeout frees worker slots sooner
        batch_interval_seconds: float = 0.0,
    ) -> None:
        self._store = store
        self._accounts_provider = accounts_provider
        self._notifier = notifier
        self._interval_seconds = max(2.0, float(interval_seconds or 5.0))  # PERF: allow faster configured polling
        self._parallelism = max(1, int(parallelism or 1))
        self._thread_limit = max(1, int(thread_limit or 40))
        self._message_limit = max(1, int(message_limit or 12))
        self._timeout_seconds = max(2.0, float(timeout_seconds or 8.0))  # PERF: keep floor while lowering default timeout
        self._batch_interval_seconds = max(0.0, float(batch_interval_seconds or 0.0))
        self._stop_event = threading.Event()
        self._wakeup = threading.Event()
        self._force_sync = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._wakeup.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="inbox-conversation-reader",
            daemon=True,
        )
        self._thread.start()
        self.request_sync(force=True)

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup.set()
        worker = self._thread
        self._thread = None
        if worker is not None:
            worker.join(timeout=3.0)

    def request_sync(self, *, force: bool = False) -> None:
        self._force_sync = self._force_sync or bool(force)
        self._wakeup.set()

    def diagnostics(self) -> dict[str, Any]:
        return {
            "poll_interval_seconds": self._interval_seconds,
            "parallelism": self._parallelism,
            "batch_interval_seconds": self._batch_interval_seconds,
            "running": bool(self._thread and self._thread.is_alive()),
        }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._force_sync:
                self._force_sync = False
                self._wakeup.clear()
            else:
                self._wakeup.wait(self._interval_seconds)
                self._wakeup.clear()
            if self._stop_event.is_set():
                return
            try:
                self._sync_all_accounts()
            except Exception as exc:
                logging.getLogger(__name__).error("inbox reader outer loop error — cycle skipped", exc_info=exc)

    def _sync_all_accounts(self) -> None:
        accounts = [dict(item) for item in self._accounts_provider() if isinstance(item, dict)]
        active_ids = {
            str(account.get("username") or "").strip().lstrip("@").lower()
            for account in accounts
            if str(account.get("username") or "").strip()
        }
        self._store.prune_accounts(active_ids)
        candidates: list[dict[str, Any]] = []
        for account in accounts:
            health_state, reason = self._map_badge_to_health(account)
            account_id = str(account.get("username") or "").strip().lstrip("@").lower()
            if not account_id:
                continue
            self._store.set_account_health(account_id, health_state, reason=reason)
            if health_state == "healthy":
                candidates.append(account)
        if not candidates:
            return
        with ThreadPoolExecutor(
            max_workers=min(len(candidates), max(self._parallelism, 12)),  # PERF: scale pool to active account count
            thread_name_prefix="inbox-poll",
        ) as pool:
            futures = {pool.submit(self._sync_single_account, account): account for account in candidates}
            for future in as_completed(futures):
                account = futures[future]
                account_id = str(account.get("username") or "").strip().lstrip("@").lower()
                try:
                    touched_keys = list(future.result() or [])
                except Exception as exc:
                    logging.getLogger(__name__).exception(
                        "Inbox reader sync failed for @%s",
                        account_id or "?",
                    )
                    health_state, reason = self._classify_reader_error(exc)
                    self._store.set_account_health(account_id, health_state, reason=reason)
                    self._store.register_account_sync(account_id, last_error=reason, thread_count=0)
                    self._notifier(reason="conversation_poll_error", account_ids=[account_id], thread_keys=[])
                    continue
                self._notifier(
                    reason="conversation_poll",
                    account_ids=[account_id],
                    thread_keys=touched_keys,
                )

    def _sync_single_account(self, account: dict[str, Any]) -> list[str]:
        account_id = str(account.get("username") or "").strip().lstrip("@").lower()
        if not account_id:
            return []
        started_at = self._account_started_at(account) or time.time()
        self._store.prepare_account_session(
            account_id,
            session_marker=self._account_session_marker(account_id),
            started_at=started_at,
        )
        rows = sync_account_threads_from_storage(
            account,
            thread_limit=self._thread_limit,
            message_limit=self._message_limit,
            max_pages=1,
            timeout_seconds=self._timeout_seconds,
        )
        return self._store.apply_endpoint_threads(account, list(rows or []))

    @staticmethod
    def _account_profile_ready(account_id: str) -> bool:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return False
        return has_playwright_storage_state(clean_account)

    @staticmethod
    def _account_started_at(account: dict[str, Any]) -> float | None:
        if not isinstance(account, dict):
            return None
        for key in ("last_connected_at", "connected_at", "first_seen"):
            value = account.get(key)
            if value is None or value == "":
                continue
            try:
                return float(value)
            except Exception:
                pass
            text = str(value or "").strip()
            if not text:
                continue
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except Exception:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return float(parsed.timestamp())
        return None

    @staticmethod
    def _account_session_marker(account_id: str) -> str:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return "missing"
        storage_state_path = playwright_storage_state_path(clean_account)
        try:
            stat = storage_state_path.stat()
        except OSError:
            return "missing"
        return f"{int(stat.st_mtime_ns)}:{int(stat.st_ctime_ns)}:{int(stat.st_size)}"

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

    @staticmethod
    def _classify_reader_error(error: Any) -> tuple[str, str]:
        if isinstance(error, InboxEndpointError):
            detail = str(error.detail or error.kind or "endpoint_error").strip()
            if error.kind == "proxy_error":
                return "proxy_error", detail
            if error.kind == "login_required":
                return "login_required", detail
            if error.kind == "checkpoint":
                return "checkpoint", detail
            if error.kind == "suspended":
                return "suspended", detail
            if error.kind == "banned":
                return "banned", detail
            return "unknown", detail
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
