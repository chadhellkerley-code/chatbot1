from __future__ import annotations

import threading
from typing import Any

from .account_worker import AccountWorker


class BrowserPool:
    def __init__(self, accounts_provider) -> None:
        self._accounts_provider = accounts_provider
        self._lock = threading.RLock()
        self._worker: AccountWorker | None = None
        self._active_thread_key = ""

    def start(self) -> None:
        return None

    def shutdown(self) -> None:
        with self._lock:
            worker = self._worker
            self._worker = None
            self._active_thread_key = ""
        if worker is not None:
            worker.shutdown()

    def cancel(self) -> None:
        self.shutdown()

    def prepare(self, thread_row: dict[str, Any]) -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        account_id = str(thread_row.get("account_id") or "").strip().lstrip("@").lower()
        if not thread_key or not account_id:
            return {"ok": False, "reason": "invalid_thread"}
        with self._lock:
            current = self._worker
            active_thread_key = self._active_thread_key
            if (
                current is not None
                and current.account_id == account_id
                and active_thread_key == thread_key
            ):
                return current.prepare(thread_row)
        account = self._accounts_provider(account_id)
        if not isinstance(account, dict):
            return {"ok": False, "reason": "account_not_found"}
        next_worker = AccountWorker(account)
        result = next_worker.prepare(thread_row)
        if not bool(result.get("ok", False)):
            next_worker.shutdown()
            return result
        with self._lock:
            previous = self._worker
            self._worker = next_worker
            self._active_thread_key = thread_key
        if previous is not None:
            previous.shutdown()
        return result

    def send_text(self, thread_row: dict[str, Any], text: str) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
        if not bool(prepared.get("ok", False)):
            return {"ok": False, "reason": str(prepared.get("reason") or "prepare_failed")}
        with self._lock:
            worker = self._worker
        if worker is None:
            return {"ok": False, "reason": "worker_missing"}
        return worker.send_text(thread_row, text)

    def send_pack(
        self,
        thread_row: dict[str, Any],
        pack: dict[str, Any],
        *,
        conversation_text: str,
        flow_config: dict[str, Any],
    ) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
        if not bool(prepared.get("ok", False)):
            return {"ok": False, "reason": str(prepared.get("reason") or "prepare_failed")}
        with self._lock:
            worker = self._worker
        if worker is None:
            return {"ok": False, "reason": "worker_missing"}
        return worker.send_pack(
            thread_row,
            pack,
            conversation_text=conversation_text,
            flow_config=flow_config,
        )

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            worker = self._worker
            return {
                "active_account_id": worker.account_id if worker is not None else "",
                "active_thread_key": self._active_thread_key,
                "worker_ready": bool(worker is not None and self._active_thread_key),
            }
