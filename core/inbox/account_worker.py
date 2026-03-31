from __future__ import annotations

import contextlib
from typing import Any

<<<<<<< HEAD
from src.auth.persistent_login import STORAGE_FILENAME, ensure_logged_in_async
from src.browser_profile_paths import browser_storage_state_path
from src.inbox_diagnostics import record_inbox_diagnostic
from src.inbox.message_sender import TaskDirectClient, send_pack_messages
=======
from src.auth.persistent_login import ensure_logged_in_async
from src.inbox.message_sender import (
    TaskDirectClient,
    _COMPOSER_SELECTORS,
    _wait_for_visible_locator_async,
    send_pack_messages,
)
>>>>>>> origin/main
from src.playwright_service import BASE_PROFILES
from src.proxy_payload import proxy_from_account
from src.transport.session_manager import SessionManager, SyncSessionRuntime


class _PreparedRuntime:
    def __init__(self, account: dict[str, Any]) -> None:
        self._account = dict(account or {})
        self._session_manager = SessionManager(
            headless=True,
            keep_browser_open_per_account=True,
            profiles_root=str(BASE_PROFILES),
            normalize_username=lambda value: str(value or "").strip().lstrip("@"),
            log_event=lambda *_args, **_kwargs: None,
<<<<<<< HEAD
            subsystem="inbox",
=======
>>>>>>> origin/main
        )
        self._runtime = SyncSessionRuntime(
            account=self._account,
            session_manager=self._session_manager,
            login_func=ensure_logged_in_async,
            proxy_resolver=proxy_from_account,
            open_timeout_seconds=120.0,
        )

<<<<<<< HEAD
    def set_diagnostic_context(self, *, thread_key: str = "", job_type: str = "") -> None:
        clean_thread_key = str(thread_key or "").strip()
        clean_job_type = str(job_type or "").strip().lower()
        self._account["_inbox_diagnostic_thread_key"] = clean_thread_key
        self._account["_inbox_diagnostic_job_type"] = clean_job_type
        runtime_account = getattr(self._runtime, "_account", None)
        if isinstance(runtime_account, dict):
            runtime_account["_inbox_diagnostic_thread_key"] = clean_thread_key
            runtime_account["_inbox_diagnostic_job_type"] = clean_job_type

=======
>>>>>>> origin/main
    def run_async(self, coro: Any, *, timeout: float | None = None) -> Any:
        return self._runtime.run_async(coro, timeout=timeout)

    def open_page(self, account: dict[str, Any], *, timeout: float | None = None) -> Any:
        return self._runtime.open_page(account, timeout=timeout)

    def close_page(self, page: Any, *, timeout: float | None = None) -> None:
        self._runtime.close_page(page, timeout=timeout)

    def shutdown(self) -> None:
        with contextlib.suppress(Exception):
            self._runtime.shutdown(timeout=10.0)


class AccountWorker:
<<<<<<< HEAD
    def __init__(self, account: dict[str, Any], *, diagnostics_store: Any | None = None) -> None:
        self._account = dict(account or {})
        if diagnostics_store is not None:
            self._account["_inbox_diagnostics_store"] = diagnostics_store
        self._runtime = _PreparedRuntime(self._account)
        self._diagnostics_store = diagnostics_store
=======
    def __init__(self, account: dict[str, Any]) -> None:
        self._account = dict(account or {})
        self._runtime = _PreparedRuntime(self._account)
>>>>>>> origin/main
        self._client: TaskDirectClient | None = None
        self._thread_key = ""
        self._thread_id = ""

    @property
    def account_id(self) -> str:
        return str(self._account.get("username") or "").strip().lstrip("@").lower()

    @property
    def thread_key(self) -> str:
        return self._thread_key

    def shutdown(self) -> None:
        client = self._client
        self._client = None
        self._thread_key = ""
        self._thread_id = ""
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()
        self._runtime.shutdown()

<<<<<<< HEAD
    def _record_prepare_event(
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
        account_id = self.account_id
        alias_id = str(
            thread_row.get("alias_id")
            or thread_row.get("account_alias")
            or self._account.get("alias")
            or ""
        ).strip()
        thread_key = str(thread_row.get("thread_key") or self._thread_key or "").strip()
        storage_state = browser_storage_state_path(
            account_id or str(self._account.get("username") or "").strip(),
            profiles_root=BASE_PROFILES,
            filename=STORAGE_FILENAME,
        )
        proxy_payload = proxy_from_account(self._account)
        diagnostic_payload = {
            "prepare_stage": str(stage or "").strip(),
            "proxy_present": bool(proxy_payload),
            "storage_state_present": storage_state.exists(),
            "storage_state_path": str(storage_state),
            "session_cached_thread_key": self._thread_key,
            "session_cached_thread_id": self._thread_id,
            "thread_id": str(thread_row.get("thread_id") or self._thread_id or "").strip(),
            "thread_href": str(thread_row.get("thread_href") or "").strip(),
        }
        diagnostic_payload.update(dict(payload or {}))
        record_inbox_diagnostic(
            self._diagnostics_store or self._account,
            event_type=event_type,
            stage=stage,
            outcome=outcome,
            account_id=account_id,
            alias_id=alias_id,
            thread_key=thread_key,
            reason=reason,
            reason_code=reason_code,
            exception=exception,
            payload=diagnostic_payload,
            callsite_skip=2,
        )

    def prepare(self, thread_row: dict[str, Any], *, job_type: str = "manual_reply") -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        thread_id = str(thread_row.get("thread_id") or "").strip()
        set_diagnostic_context = getattr(self._runtime, "set_diagnostic_context", None)
        if callable(set_diagnostic_context):
            set_diagnostic_context(thread_key=thread_key, job_type=str(job_type or "").strip().lower())
        self._record_prepare_event(
            thread_row=thread_row,
            event_type="account_prepare_started",
            stage="prepare",
            outcome="attempt",
            reason="account_prepare_started",
            reason_code="account_prepare_started",
        )
        if not thread_key or not thread_id:
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="account_prepare_failed",
                stage="prepare",
                outcome="fail",
                reason="invalid_thread",
                reason_code="invalid_thread",
            )
            return {"ok": False, "reason": "invalid_thread"}
        if thread_key == self._thread_key and self._client is not None:
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="thread_open_started",
                stage="thread_revalidation",
                outcome="attempt",
                reason="thread_revalidation_started",
                reason_code="thread_revalidation_started",
            )
            try:
                ready_ok, _ready_reason = self._client.ensure_thread_ready_strict(thread_id)
            except Exception as exc:
                self._record_prepare_event(
                    thread_row=thread_row,
                    event_type="thread_open_failed",
                    stage="thread_revalidation",
                    outcome="fail",
                    exception=exc,
                    payload={"cached_client_reuse": True},
                )
                self._record_prepare_event(
                    thread_row=thread_row,
                    event_type="account_prepare_failed",
                    stage="thread_revalidation",
                    outcome="fail",
                    exception=exc,
                    payload={"cached_client_reuse": True},
                )
                raise
            if ready_ok:
                try:
                    self._focus_composer(thread_id)
                except Exception as exc:
                    self._record_prepare_event(
                        thread_row=thread_row,
                        event_type="composer_ready_failed",
                        stage="composer_ready",
                        outcome="fail",
                        exception=exc,
                        payload={"cached_client_reuse": True},
                    )
                    self._record_prepare_event(
                        thread_row=thread_row,
                        event_type="account_prepare_failed",
                        stage="composer_ready",
                        outcome="fail",
                        exception=exc,
                        payload={"cached_client_reuse": True},
                    )
                    raise
                return {"ok": True, "reason": "already_prepared"}
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="thread_open_failed",
                stage="thread_revalidation",
                outcome="fail",
                reason=_ready_reason,
                payload={"cached_client_reuse": True},
            )
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="account_prepare_failed",
                stage="thread_revalidation",
                outcome="fail",
                reason=_ready_reason,
                payload={"cached_client_reuse": True},
            )
            self.shutdown()
        self.shutdown()
        stage = "browser_launch"
        self._record_prepare_event(
            thread_row=thread_row,
            event_type="browser_launch_started",
            stage=stage,
            outcome="attempt",
            reason="browser_launch_started",
            reason_code="browser_launch_started",
        )
        try:
            self._client = TaskDirectClient(
                self._runtime,
                self._account,
                thread_id=thread_id,
                thread_href=str(thread_row.get("thread_href") or "").strip(),
                bypass_account_quota=True,
            )
            stage = "thread_open"
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="thread_open_started",
                stage=stage,
                outcome="attempt",
                reason="thread_open_started",
                reason_code="thread_open_started",
            )
            ready_ok, ready_reason = self._client.ensure_thread_ready_strict(thread_id)
            if not ready_ok:
                self.shutdown()
                self._record_prepare_event(
                    thread_row=thread_row,
                    event_type="thread_open_failed",
                    stage=stage,
                    outcome="fail",
                    reason=ready_reason,
                )
                self._record_prepare_event(
                    thread_row=thread_row,
                    event_type="account_prepare_failed",
                    stage=stage,
                    outcome="fail",
                    reason=ready_reason,
                )
                return {"ok": False, "reason": ready_reason}
            stage = "composer_ready"
            self._focus_composer(thread_id)
        except Exception as exc:
            self.shutdown()
            failure_event = {
                "browser_launch": "browser_launch_failed",
                "thread_open": "thread_open_failed",
                "composer_ready": "composer_ready_failed",
            }.get(stage, "account_prepare_failed")
            self._record_prepare_event(
                thread_row=thread_row,
                event_type=failure_event,
                stage=stage,
                outcome="fail",
                exception=exc,
            )
            self._record_prepare_event(
                thread_row=thread_row,
                event_type="account_prepare_failed",
                stage=stage,
                outcome="fail",
                exception=exc,
            )
            raise
=======
    def prepare(self, thread_row: dict[str, Any]) -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        thread_id = str(thread_row.get("thread_id") or "").strip()
        if not thread_key or not thread_id:
            return {"ok": False, "reason": "invalid_thread"}
        if thread_key == self._thread_key and self._client is not None:
            ready_ok, _ready_reason = self._client.ensure_thread_ready_strict(thread_id)
            if ready_ok:
                self._focus_composer()
                return {"ok": True, "reason": "already_prepared"}
            self.shutdown()
        self.shutdown()
        self._client = TaskDirectClient(
            self._runtime,
            self._account,
            thread_id=thread_id,
            thread_href=str(thread_row.get("thread_href") or "").strip(),
            bypass_account_quota=True,
        )
        ready_ok, ready_reason = self._client.ensure_thread_ready_strict(thread_id)
        if not ready_ok:
            self.shutdown()
            return {"ok": False, "reason": ready_reason}
        self._focus_composer()
>>>>>>> origin/main
        self._thread_key = thread_key
        self._thread_id = thread_id
        return {"ok": True, "reason": "prepared"}

<<<<<<< HEAD
    def send_text(self, thread_row: dict[str, Any], text: str, *, job_type: str = "manual_reply") -> dict[str, Any]:
        prepared = self.prepare(thread_row, job_type=job_type)
=======
    def send_text(self, thread_row: dict[str, Any], text: str) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
>>>>>>> origin/main
        if not bool(prepared.get("ok", False)):
            return {"ok": False, "reason": str(prepared.get("reason") or "prepare_failed")}
        client = self._client
        if client is None:
            return {"ok": False, "reason": "client_missing"}
        result = client.send_text_with_ack(self._thread_id, str(text or "").strip(), timeout=4.0)
        return dict(result or {})

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
        result = send_pack_messages(
            self._runtime,
            self._account,
            thread_row,
            pack,
            conversation_text=conversation_text,
            flow_config=flow_config,
        )
        # Pack sending can leave the previous page in an unknown state.
        self.shutdown()
        return dict(result or {})

<<<<<<< HEAD
    def _focus_composer(self, thread_id: str) -> None:
        client = self._client
        if client is None:
            return
        ok, reason = client.focus_composer(thread_id, timeout_ms=8_000)
        # Sending performs its own composer check again, so a degraded focus step
        # should not abort the whole job unless the composer truly vanished.
        if ok:
            return
        if str(reason or "").strip() == "composer_not_found":
            raise RuntimeError("composer_not_found")
=======
    def _focus_composer(self) -> None:
        client = self._client
        if client is None:
            return
        page = client._ensure_page()  # type: ignore[attr-defined]

        async def _focus() -> None:
            composer = await _wait_for_visible_locator_async(
                page,
                _COMPOSER_SELECTORS,
                timeout_ms=8_000,
            )
            if composer is None:
                raise RuntimeError("composer_not_found")
            await page.wait_for_timeout(650)
            await composer.click()

        self._runtime.run_async(_focus(), timeout=10.0)
>>>>>>> origin/main
