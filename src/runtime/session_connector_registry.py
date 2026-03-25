from __future__ import annotations

import threading
import time
from typing import Any, Callable

from core.accounts import has_playwright_storage_state
from core.inbox.conversation_reader import ConversationReader


class SessionConnectorRegistry:
    _HEARTBEAT_TTL_SECONDS = 45.0
    _BOOT_SWEEP_LIVE_STATES = {"ready", "degraded", "running"}

    def __init__(
        self,
        *,
        account_resolver: Callable[[str], dict[str, Any] | None],
        store: Any,
    ) -> None:
        self._account_resolver = account_resolver
        self._store = store
        self._lock = threading.RLock()
        self._transports: dict[str, dict[str, Any]] = {}

    def start(self, account_id: str) -> dict[str, Any]:
        account = self._account_resolver(str(account_id or "").strip())
        clean_account = str((account or {}).get("username") or account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return {}
        state = "ready" if self._is_account_ready(account) else "degraded"
        transport = dict(account or {})
        transport["account_id"] = clean_account
        transport["connector_state"] = state
        transport["started_at"] = time.time()
        with self._lock:
            self._transports[clean_account] = transport
        payload = self._persist_state(
            clean_account,
            account=account,
            state=state,
            last_error="" if state == "ready" else "storage_state_missing",
        )
        started_at = ConversationReader._account_started_at(account or {})
        if started_at is not None:
            self._store.prepare_account_session(
                clean_account,
                session_marker=ConversationReader._account_session_marker(clean_account),
                started_at=started_at,
            )
        return payload

    def stop(self, account_id: str, *, last_error: str = "") -> dict[str, Any]:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return {}
        with self._lock:
            self._transports.pop(clean_account, None)
        return self._persist_state(clean_account, account=self._account_resolver(clean_account), state="offline", last_error=last_error)

    def heartbeat(
        self,
        account_id: str,
        *,
        state: str | None = None,
        last_error: str | None = None,
    ) -> dict[str, Any]:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return {}
        current = self._store.get_session_connector_state(clean_account)
        if not current:
            return self.start(clean_account)
        next_state = str(state or current.get("state") or "ready").strip().lower() or "ready"
        if next_state == "ready":
            next_error = "" if last_error is None else str(last_error or "").strip()
        else:
            next_error = str(last_error if last_error is not None else current.get("last_error") or "").strip()
        return self._persist_state(
            clean_account,
            account=self._account_resolver(clean_account),
            state=next_state,
            last_error=next_error,
        )

    def mark_degraded(self, account_id: str, last_error: str) -> dict[str, Any]:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return {}
        return self._persist_state(
            clean_account,
            account=self._account_resolver(clean_account),
            state="degraded",
            last_error=str(last_error or "").strip() or "connector_degraded",
        )

    def is_ready(self, account_id: str) -> bool:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return False
        state = self._store.get_session_connector_state(clean_account)
        if state:
            if self._is_state_stale(state):
                state = self.mark_degraded(clean_account, "heartbeat_stale")
            return str(state.get("state") or "").strip().lower() == "ready"
        account = self._account_resolver(clean_account)
        return self._is_account_ready(account)

    def get_transport(self, account_id: str) -> dict[str, Any] | None:
        clean_account = str(account_id or "").strip().lstrip("@").lower()
        if not clean_account:
            return None
        with self._lock:
            transport = self._transports.get(clean_account)
        if isinstance(transport, dict):
            return dict(transport)
        account = self._account_resolver(clean_account)
        return dict(account) if isinstance(account, dict) else None

    @staticmethod
    def _is_account_ready(account: dict[str, Any] | None) -> bool:
        username = str((account or {}).get("username") or "").strip().lstrip("@").lower()
        if not username:
            return False
        if not bool((account or {}).get("active", True)):
            return False
        return has_playwright_storage_state(username)

    def _persist_state(
        self,
        account_id: str,
        *,
        account: dict[str, Any] | None,
        state: str,
        last_error: str,
    ) -> dict[str, Any]:
        clean_state = str(state or "offline").strip().lower() or "offline"
        return self._store.upsert_session_connector_state(
            account_id,
            {
                "alias_id": str((account or {}).get("alias") or "").strip(),
                "state": clean_state,
                "proxy_key": str(
                    (account or {}).get("proxy_url")
                    or (account or {}).get("proxy")
                    or (account or {}).get("assigned_proxy_id")
                    or ""
                ).strip(),
                "last_heartbeat_at": time.time(),
                "last_error": str(last_error or "").strip(),
            },
        )

    @classmethod
    def sweep_boot_persisted_states(
        cls,
        *,
        store: Any,
        accounts_by_id: dict[str, dict[str, Any]] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        list_states = getattr(store, "list_session_connector_states", None)
        update_state = getattr(store, "upsert_session_connector_state", None)
        delete_state = getattr(store, "delete_session_connector_state", None)
        if not callable(list_states) or not callable(update_state):
            return {"checked": 0, "cleaned": 0, "deleted": 0, "details": []}

        accounts = {
            cls._normalize_account_id(account_id): dict(payload or {})
            for account_id, payload in dict(accounts_by_id or {}).items()
            if cls._normalize_account_id(account_id)
        }
        timestamp = float(now or time.time())
        summary = {"checked": 0, "cleaned": 0, "deleted": 0, "details": []}
        for raw_state in list_states():
            if not isinstance(raw_state, dict):
                continue
            account_id = cls._normalize_account_id(raw_state.get("account_id"))
            if not account_id:
                continue
            summary["checked"] += 1
            account = accounts.get(account_id)
            if account is None:
                if callable(delete_state) and bool(delete_state(account_id)):
                    summary["deleted"] += 1
                    summary["details"].append({"account_id": account_id, "action": "deleted_missing_account"})
                continue

            current_state = str(raw_state.get("state") or "").strip().lower() or "offline"
            expected_alias = str(account.get("alias") or "").strip()
            expected_proxy = str(
                account.get("proxy_url")
                or account.get("proxy")
                or account.get("assigned_proxy_id")
                or ""
            ).strip()
            reasons: list[str] = []
            if current_state in cls._BOOT_SWEEP_LIVE_STATES:
                reasons.append(f"connector_state_{current_state}")
            if str(raw_state.get("alias_id") or "").strip() != expected_alias:
                reasons.append("alias_mismatch")
            if str(raw_state.get("proxy_key") or "").strip() != expected_proxy:
                reasons.append("proxy_mismatch")
            if not reasons:
                continue

            last_error = str(raw_state.get("last_error") or "").strip()
            if current_state in cls._BOOT_SWEEP_LIVE_STATES and not last_error:
                last_error = "boot_stale_connector_cleaned"
            update_state(
                account_id,
                {
                    "alias_id": expected_alias,
                    "proxy_key": expected_proxy,
                    "state": "offline",
                    "last_heartbeat_at": timestamp,
                    "last_error": last_error,
                    "updated_at": timestamp,
                },
            )
            summary["cleaned"] += 1
            summary["details"].append({"account_id": account_id, "action": "cleaned", "reasons": reasons})
        return summary

    def _is_state_stale(self, state: dict[str, Any] | None) -> bool:
        if not isinstance(state, dict):
            return False
        current_state = str(state.get("state") or "").strip().lower()
        if current_state not in {"ready", "degraded"}:
            return False
        try:
            heartbeat = float(state.get("last_heartbeat_at") or 0.0)
        except Exception:
            heartbeat = 0.0
        if heartbeat <= 0.0:
            return True
        return (time.time() - heartbeat) > self._HEARTBEAT_TTL_SECONDS

    @staticmethod
    def _normalize_account_id(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()
