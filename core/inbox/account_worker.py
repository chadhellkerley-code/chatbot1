from __future__ import annotations

import contextlib
from typing import Any

from src.auth.persistent_login import ensure_logged_in_async
from src.inbox.message_sender import (
    TaskDirectClient,
    _COMPOSER_SELECTORS,
    _wait_for_visible_locator_async,
    send_pack_messages,
)
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
        )
        self._runtime = SyncSessionRuntime(
            account=self._account,
            session_manager=self._session_manager,
            login_func=ensure_logged_in_async,
            proxy_resolver=proxy_from_account,
            open_timeout_seconds=120.0,
        )

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
    def __init__(self, account: dict[str, Any]) -> None:
        self._account = dict(account or {})
        self._runtime = _PreparedRuntime(self._account)
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

    def prepare(self, thread_row: dict[str, Any]) -> dict[str, Any]:
        thread_key = str(thread_row.get("thread_key") or "").strip()
        thread_id = str(thread_row.get("thread_id") or "").strip()
        if not thread_key or not thread_id:
            return {"ok": False, "reason": "invalid_thread"}
        if thread_key == self._thread_key and self._client is not None:
            return {"ok": True, "reason": "already_prepared"}
        self.shutdown()
        self._client = TaskDirectClient(
            self._runtime,
            self._account,
            thread_id=thread_id,
            thread_href=str(thread_row.get("thread_href") or "").strip(),
        )
        ready_ok, ready_reason = self._client.ensure_thread_ready_strict(thread_id)
        if not ready_ok:
            self.shutdown()
            return {"ok": False, "reason": ready_reason}
        self._focus_composer()
        self._thread_key = thread_key
        self._thread_id = thread_id
        return {"ok": True, "reason": "prepared"}

    def send_text(self, thread_row: dict[str, Any], text: str) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
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
    ) -> dict[str, Any]:
        prepared = self.prepare(thread_row)
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
