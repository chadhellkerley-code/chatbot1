from __future__ import annotations

import threading
<<<<<<< HEAD
from pathlib import Path
from typing import Any

from src.auth.persistent_login import STORAGE_FILENAME
from src.browser_profile_paths import browser_storage_state_path
from src.inbox_diagnostics import record_inbox_diagnostic

=======
from typing import Any

>>>>>>> origin/main
from .account_worker import AccountWorker


class BrowserPool:
<<<<<<< HEAD
    def __init__(self, accounts_provider, diagnostics_store: Any | None = None) -> None:
        self._accounts_provider = accounts_provider
        self._diagnostics_store = diagnostics_store
=======
    def __init__(self, accounts_provider) -> None:
        self._accounts_provider = accounts_provider
>>>>>>> origin/main
        self._lock = threading.RLock()
        self._worker: AccountWorker | None = None
        self._active_thread_key = ""

<<<<<<< HEAD
    def _record_diagnostic(
        self,
        *,
        thread_row: dict[str, Any],
        event_type: str,
        stage: str,
        outcome: str,
        reason: str = "",
        reason_code: str = "",
        exception: BaseException | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_inbox_diagnostic(
            self._diagnostics_store,
            event_type=event_type,
            stage=stage,
            outcome=outcome,
            account_id=str(thread_row.get("account_id") or "").strip(),
            alias_id=str(thread_row.get("alias_id") or thread_row.get("account_alias") or "").strip(),
            thread_key=str(thread_row.get("thread_key") or "").strip(),
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload={
                "active_thread_key": self._active_thread_key,
                **dict(payload or {}),
            },
            callsite_skip=2,
        )

=======
>>>>>>> origin/main
    def start(self) -> None:
        return None

    def shutdown(self) -> None:
        with self._lock:
            worker = self._worker
<<<<<<< HEAD
            released_thread_key = self._active_thread_key
=======
>>>>>>> origin/main
            self._worker = None
            self._active_thread_key = ""
        if worker is not None:
            worker.shutdown()
<<<<<<< HEAD
            record_inbox_diagnostic(
                self._diagnostics_store,
                event_type="browser_worker_released",
                stage="worker_release",
                outcome="success",
                account_id=worker.account_id,
                thread_key=released_thread_key,
                reason="browser_worker_released",
                reason_code="browser_worker_released",
                payload={"worker_ready": False},
                callsite_skip=1,
            )
=======
>>>>>>> origin/main

    def cancel(self) -> None:
        self.shutdown()

<<<<<<< HEAD
    def prepare(self, thread_row: dict[str, Any], *, job_type: str = "manual_reply") -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        account_id = str(thread_row.get("account_id") or "").strip().lstrip("@").lower()
        if not thread_key or not account_id:
            self._record_diagnostic(
                thread_row=thread_row,
                event_type="browser_worker_acquire_failed",
                stage="worker_acquire",
                outcome="fail",
                reason="invalid_thread",
                reason_code="invalid_thread",
            )
=======
    def prepare(self, thread_row: dict[str, Any]) -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        account_id = str(thread_row.get("account_id") or "").strip().lstrip("@").lower()
        if not thread_key or not account_id:
>>>>>>> origin/main
            return {"ok": False, "reason": "invalid_thread"}
        with self._lock:
            current = self._worker
            active_thread_key = self._active_thread_key
            if (
                current is not None
                and current.account_id == account_id
                and active_thread_key == thread_key
            ):
<<<<<<< HEAD
                self._record_diagnostic(
                    thread_row=thread_row,
                    event_type="browser_worker_reuse_started",
                    stage="worker_reuse",
                    outcome="attempt",
                    reason="browser_worker_reuse_started",
                    reason_code="browser_worker_reuse_started",
                )
                try:
                    try:
                        result = current.prepare(thread_row, job_type=job_type)
                    except TypeError:
                        result = current.prepare(thread_row)
                except Exception as exc:
                    self._record_diagnostic(
                        thread_row=thread_row,
                        event_type="browser_worker_reuse_failed",
                        stage="worker_reuse",
                        outcome="fail",
                        exception=exc,
                    )
                    raise
                if not bool(result.get("ok", False)):
                    self._record_diagnostic(
                        thread_row=thread_row,
                        event_type="browser_worker_reuse_failed",
                        stage="worker_reuse",
                        outcome="fail",
                        reason=str(result.get("reason") or "worker_reuse_failed").strip(),
                    )
                return result
        account = self._accounts_provider(account_id)
        if not isinstance(account, dict):
            self._record_diagnostic(
                thread_row=thread_row,
                event_type="browser_worker_acquire_failed",
                stage="worker_acquire",
                outcome="fail",
                reason="account_not_found",
                reason_code="account_not_found",
            )
            return {"ok": False, "reason": "account_not_found"}
        self._record_diagnostic(
            thread_row=thread_row,
            event_type="browser_worker_acquire_started",
            stage="worker_acquire",
            outcome="attempt",
            reason="browser_worker_acquire_started",
            reason_code="browser_worker_acquire_started",
        )
        next_worker = AccountWorker(account, diagnostics_store=self._diagnostics_store)
        try:
            try:
                result = next_worker.prepare(thread_row, job_type=job_type)
            except TypeError:
                result = next_worker.prepare(thread_row)
        except Exception as exc:
            self._record_diagnostic(
                thread_row=thread_row,
                event_type="browser_worker_acquire_failed",
                stage="worker_acquire",
                outcome="fail",
                exception=exc,
            )
            next_worker.shutdown()
            raise
        if not bool(result.get("ok", False)):
            self._record_diagnostic(
                thread_row=thread_row,
                event_type="browser_worker_acquire_failed",
                stage="worker_acquire",
                outcome="fail",
                reason=str(result.get("reason") or "worker_acquire_failed").strip(),
            )
=======
                return current.prepare(thread_row)
        account = self._accounts_provider(account_id)
        if not isinstance(account, dict):
            return {"ok": False, "reason": "account_not_found"}
        next_worker = AccountWorker(account)
        result = next_worker.prepare(thread_row)
        if not bool(result.get("ok", False)):
>>>>>>> origin/main
            next_worker.shutdown()
            return result
        with self._lock:
            previous = self._worker
            self._worker = next_worker
            self._active_thread_key = thread_key
<<<<<<< HEAD
        self._record_diagnostic(
            thread_row=thread_row,
            event_type="browser_worker_swapped",
            stage="worker_swap",
            outcome="success",
            reason="browser_worker_swapped",
            reason_code="browser_worker_swapped",
            payload={"had_previous_worker": bool(previous is not None)},
        )
=======
>>>>>>> origin/main
        if previous is not None:
            previous.shutdown()
        return result

<<<<<<< HEAD
    def send_text(self, thread_row: dict[str, Any], text: str, *, job_type: str = "manual_reply") -> dict[str, Any]:
        prepared = self.prepare(thread_row, job_type=job_type)
=======
    def send_text(self, thread_row: dict[str, Any], text: str) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
>>>>>>> origin/main
        if not bool(prepared.get("ok", False)):
            return {"ok": False, "reason": str(prepared.get("reason") or "prepare_failed")}
        with self._lock:
            worker = self._worker
        if worker is None:
            return {"ok": False, "reason": "worker_missing"}
<<<<<<< HEAD
        return worker.send_text(thread_row, text, job_type=job_type)
=======
        return worker.send_text(thread_row, text)
>>>>>>> origin/main

    def send_pack(
        self,
        thread_row: dict[str, Any],
        pack: dict[str, Any],
        *,
        conversation_text: str,
        flow_config: dict[str, Any],
<<<<<<< HEAD
        job_type: str = "auto_reply",
    ) -> dict[str, Any]:
        prepared = self.prepare(thread_row, job_type=job_type)
=======
    ) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
>>>>>>> origin/main
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
<<<<<<< HEAD
            job_type=job_type,
=======
>>>>>>> origin/main
        )

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            worker = self._worker
<<<<<<< HEAD
            active_account_id = worker.account_id if worker is not None else ""
            storage_state: Path | None = None
            if active_account_id:
                storage_state = browser_storage_state_path(active_account_id, filename=STORAGE_FILENAME)
            return {
                "active_account_id": active_account_id,
                "active_thread_key": self._active_thread_key,
                "worker_ready": bool(worker is not None and self._active_thread_key),
                # Expose the "real" attached browser profile location for debugging UI/runtime desync.
                "active_storage_state_path": str(storage_state) if storage_state is not None else "",
                "active_storage_state_present": bool(storage_state is not None and storage_state.exists()),
=======
            return {
                "active_account_id": worker.account_id if worker is not None else "",
                "active_thread_key": self._active_thread_key,
                "worker_ready": bool(worker is not None and self._active_thread_key),
>>>>>>> origin/main
            }
