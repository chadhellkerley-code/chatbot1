from __future__ import annotations

import time
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

from core.account_limits import can_send_message_for_account
from core import responder as responder_module
from src.browser_telemetry import log_browser_stage
from src.dm_playwright_client import (
    INBOX_URL,
    THREAD_URL_TEMPLATE,
    MessageLike,
    ThreadLike,
    UserLike,
    _COMPOSER_SELECTORS,
    _SEND_BUTTON_SELECTORS,
    _extract_api_messages_from_payload,
    _extract_thread_id,
    _message_text_matches,
    _normalize_message_text,
)
from src.inbox.conversation_sync import (
    ensure_inbox_page,
    ensure_thread_page,
    fetch_thread_payload_async,
    read_conversation_async,
)
from src.transport.session_manager import NavigationLockedError


class TaskDirectClient:
    def __init__(
        self,
        runtime: Any,
        account: dict[str, Any],
        *,
        thread_id: str,
        thread_href: str = "",
        telemetry_component: str = "inbox_message_sender",
        emit_spawn: bool = True,
        emit_session_telemetry: bool = True,
        bypass_account_quota: bool = False,
    ) -> None:
        self._runtime = runtime
        self.account = dict(account or {})
        self.username = str(self.account.get("username") or "").strip().lstrip("@")
        self.user_id = self.username
        self.headless = True
        self._thread_id = str(thread_id or "").strip()
        self._thread_href = str(thread_href or "").strip()
        self._page = None
        self._last_open_thread_diag: dict[str, Any] = {}
        self._telemetry_component = str(telemetry_component or "inbox_message_sender").strip()
        self._emit_session_telemetry = bool(emit_session_telemetry)
        self._bypass_account_quota = bool(bypass_account_quota)
        self._navigation_timeout_seconds = 20.0
        if emit_spawn:
            self._telemetry("spawn", "started", thread_id=self._thread_id)

    def _telemetry(self, stage: str, status: str, **fields: Any) -> None:
        log_browser_stage(
            component=self._telemetry_component,
            stage=stage,
            status=status,
            account=self.username,
            **fields,
        )

    def close(self) -> None:
        if self._page is None:
            return
        self._runtime.close_page(self._page)
        self._page = None

    def _ensure_page(self):
        if self._page is None:
            if self._emit_session_telemetry:
                self._telemetry(
                    "session_open_start",
                    "started",
                    thread_id=self._thread_id,
                    headless=self.headless,
                )
            try:
                self._page = self._runtime.open_page(self.account)
            except Exception as exc:
                if self._emit_session_telemetry:
                    self._telemetry(
                        "session_open_end",
                        "failed",
                        thread_id=self._thread_id,
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                raise
            if self._emit_session_telemetry:
                self._telemetry("session_open_end", "ok", thread_id=self._thread_id)
                self._telemetry(
                    "browser_open",
                    "ok",
                    thread_id=self._thread_id,
                    url=str(getattr(self._page, "url", "") or ""),
                )
            self._telemetry(
                "page_attached",
                "ok",
                thread_id=self._thread_id,
                page_id=id(self._page),
                context_id=id(_page_context(self._page)),
                reused=False,
                url=str(getattr(self._page, "url", "") or ""),
            )
        else:
            self._telemetry(
                "page_attached",
                "ok",
                thread_id=self._thread_id,
                page_id=id(self._page),
                context_id=id(_page_context(self._page)),
                reused=True,
                url=str(getattr(self._page, "url", "") or ""),
            )
        return self._page

    def _find_composer(self, page) -> object | None:
        async def _probe() -> bool:
            return await _wait_for_visible_locator_async(page, _COMPOSER_SELECTORS, timeout_ms=8_000) is not None

        return object() if self._runtime.run_async(_probe()) else None

    def _navigation_owner(self, action: str) -> str:
        thread_key = str(self._thread_id or "session").strip() or "session"
        clean_action = str(action or "operation").strip().lower().replace(" ", "_")
        return f"inbox:{clean_action}:{thread_key}"

    def _navigation_payload(self, exc: NavigationLockedError) -> dict[str, Any]:
        payload = exc.to_payload()
        self._last_open_thread_diag = {
            "failed_condition": "navigation_locked",
            "navigation": dict(payload),
            "post_url": str(getattr(self._page, "url", "") or ""),
        }
        return payload

    @contextmanager
    def _navigation_scope(self, action: str, *, timeout: float | None = None):
        scope = getattr(self._runtime, "navigation_scope", None)
        if not callable(scope):
            yield
            return
        with scope(self._navigation_owner(action), timeout=timeout or self._navigation_timeout_seconds):
            yield

    def _open_inbox(self, force_reload: bool = False) -> bool:
        self._telemetry(
            "inbox_ready",
            "started",
            thread_id=self._thread_id,
            force_reload=force_reload,
        )
        try:
            with self._navigation_scope("open_inbox"):
                page = self._ensure_page()

                async def _open() -> bool:
                    current_url = str(getattr(page, "url", "") or "")
                    if force_reload or "/direct/" not in current_url:
                        await page.goto(INBOX_URL, wait_until="domcontentloaded", timeout=45_000)
                    await ensure_inbox_page(page)
                    return True

                ok = bool(self._runtime.run_async(_open()))
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            self._telemetry(
                "inbox_ready",
                "failed",
                thread_id=self._thread_id,
                reason="navigation_locked",
                **payload,
            )
            return False
        except Exception as exc:
            self._telemetry(
                "inbox_ready",
                "failed",
                thread_id=self._thread_id,
                reason="open_inbox_error",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return False
        if not ok:
            self._telemetry(
                "inbox_ready",
                "failed",
                thread_id=self._thread_id,
                reason="inbox_not_ready",
            )
            return False
        self._telemetry(
            "inbox_ready",
            "ok",
            thread_id=self._thread_id,
            url=str(getattr(page, "url", "") or ""),
        )
        self._telemetry(
            "workspace_ready",
            "ok",
            thread_id=self._thread_id,
            url=str(getattr(page, "url", "") or ""),
        )
        return True

    def _ensure_inbox_workspace_fast(self) -> None:
        if not self._open_inbox(force_reload=False):
            raise RuntimeError(f"Inbox not ready for @{self.username}.")

    def _open_thread(self, thread: object, **_kwargs: Any) -> bool:
        href = str(getattr(thread, "link", "") or "").strip() or self._thread_href
        return self.open_thread_by_href(href)

    def open_thread_by_href(self, href: str, **_kwargs: Any) -> bool:
        target_href = str(href or "").strip()
        target_thread_id = _extract_thread_id(target_href) or self._thread_id
        if not target_href and target_thread_id:
            target_href = THREAD_URL_TEMPLATE.format(thread_id=target_thread_id)
        page = self._ensure_page()
        self._telemetry(
            "thread_open_start",
            "started",
            thread_id=target_thread_id,
            href=target_href,
            page_id=id(page),
            url=str(getattr(page, "url", "") or ""),
        )
        if not target_href or not target_thread_id:
            self._last_open_thread_diag = {
                "failed_condition": "invalid_thread_target",
                "post_url": str(getattr(page, "url", "") or ""),
            }
            self._telemetry(
                "thread_open",
                "failed",
                thread_id=target_thread_id,
                href=target_href,
                reason="invalid_thread_target",
            )
            return False

        try:
            with self._navigation_scope("open_thread"):
                page = self._ensure_page()

                async def _open() -> bool:
                    return await ensure_thread_page(
                        page,
                        thread_id=target_thread_id,
                        thread_href=target_href,
                        timeout_ms=12_000,
                    )

                ok = bool(self._runtime.run_async(_open()))
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            self._telemetry(
                "thread_open",
                "failed",
                thread_id=target_thread_id,
                href=target_href,
                reason="navigation_locked",
                **payload,
            )
            return False
        except Exception as exc:
            self._last_open_thread_diag = {
                "failed_condition": f"open_thread_error:{exc}",
                "post_url": str(getattr(page, "url", "") or ""),
            }
            self._telemetry(
                "thread_open",
                "failed",
                thread_id=target_thread_id,
                href=target_href,
                reason="open_thread_error",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return False
        self._thread_id = target_thread_id
        self._thread_href = target_href
        self._last_open_thread_diag = {
            "failed_condition": "" if ok else "open_thread_failed",
            "post_url": str(getattr(page, "url", "") or ""),
        }
        if ok:
            self._telemetry(
                "thread_open_ok",
                "ok",
                thread_id=target_thread_id,
                href=target_href,
                page_id=id(page),
                url=str(getattr(page, "url", "") or ""),
            )
        self._telemetry(
            "thread_open",
            "ok" if ok else "failed",
            thread_id=target_thread_id,
            href=target_href,
            url=str(getattr(page, "url", "") or ""),
            reason="" if ok else "open_thread_failed",
        )
        return ok

    def _get_last_open_thread_diag(self) -> dict[str, Any]:
        return dict(self._last_open_thread_diag)

    def ensure_thread_ready_strict(self, thread_id: str) -> tuple[bool, str]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id
        if not target_thread_id:
            self._telemetry("composer_ready", "failed", thread_id=target_thread_id, reason="invalid_thread_id")
            return False, "invalid_thread_id"
        try:
            with self._navigation_scope("ensure_thread_ready"):
                thread_href = self._thread_href or THREAD_URL_TEMPLATE.format(thread_id=target_thread_id)
                ok = self.open_thread_by_href(thread_href)
                if not ok:
                    self._telemetry("composer_ready", "failed", thread_id=target_thread_id, reason="open_thread_failed")
                    return False, "open_thread_failed"
                page = self._ensure_page()
                if self._find_composer(page) is None:
                    self._last_open_thread_diag = {
                        "failed_condition": "composer_not_found",
                        "post_url": str(getattr(page, "url", "") or ""),
                    }
                    self._telemetry(
                        "composer_ready",
                        "failed",
                        thread_id=target_thread_id,
                        reason="composer_not_found",
                        url=str(getattr(page, "url", "") or ""),
                    )
                    return False, "composer_not_found"
                self._telemetry(
                    "composer_ready",
                    "ok",
                    thread_id=target_thread_id,
                    url=str(getattr(page, "url", "") or ""),
                )
                return True, "ok"
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            self._telemetry(
                "composer_ready",
                "failed",
                thread_id=target_thread_id,
                reason="navigation_locked",
                **payload,
            )
            return False, "navigation_locked"

    def _log_exact_fail_reason(self, *, thread_id: str, reason: str, **fields: Any) -> None:
        self._telemetry(
            "exact_fail_reason",
            "failed",
            thread_id=thread_id,
            reason=str(reason or "").strip(),
            **fields,
        )

    def focus_composer(self, thread_id: str, *, timeout_ms: int = 10_000) -> tuple[bool, str]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id
        page = self._ensure_page()
        self._telemetry(
            "composer_wait_start",
            "started",
            thread_id=target_thread_id,
            timeout_ms=int(timeout_ms or 0),
            page_id=id(page),
            url=str(getattr(page, "url", "") or ""),
        )

        async def _focus() -> tuple[bool, str]:
            composer = await _wait_for_visible_locator_async(
                page,
                _COMPOSER_SELECTORS,
                timeout_ms=max(1_000, int(timeout_ms or 0)),
            )
            if composer is None:
                return False, "composer_not_found"
            return await _activate_composer_async(page, composer)

        ok, reason = tuple(
            self._runtime.run_async(
                _focus(),
                timeout=max(3.0, float(max(1_000, int(timeout_ms or 0))) / 1000.0 + 2.0),
            )
        )
        if ok:
            self._telemetry(
                "composer_ready",
                "ok",
                thread_id=target_thread_id,
                page_id=id(page),
                url=str(getattr(page, "url", "") or ""),
            )
            return True, "ok"
        self._log_exact_fail_reason(
            thread_id=target_thread_id,
            reason=reason,
            page_id=id(page),
            url=str(getattr(page, "url", "") or ""),
        )
        return False, str(reason or "composer_focus_failed")

    def get_messages(self, thread: object, amount: int = 20, *, log: bool = True) -> list[MessageLike]:
        del log
        thread_id = str(getattr(thread, "id", "") or "").strip() or self._thread_id
        thread_href = str(getattr(thread, "link", "") or "").strip() or self._thread_href

        try:
            with self._navigation_scope("read_messages"):
                page = self._ensure_page()

                async def _read() -> list[MessageLike]:
                    payload = await read_conversation_async(
                        page,
                        account=self.account,
                        thread_id=thread_id,
                        thread_href=thread_href,
                        message_limit=max(20, min(80, int(amount or 20))),
                    )
                    rows: list[MessageLike] = []
                    for raw in payload.get("messages", []):
                        if not isinstance(raw, dict):
                            continue
                        rows.append(
                            MessageLike(
                                id=str(raw.get("message_id") or "").strip(),
                                user_id=str(raw.get("user_id") or "").strip(),
                                text=str(raw.get("text") or "").strip(),
                                timestamp=raw.get("timestamp"),
                                direction=str(raw.get("direction") or "unknown"),
                            )
                        )
                    return rows

                return list(self._runtime.run_async(_read()))
        except NavigationLockedError:
            return []

    def get_outbound_baseline(self, thread_id: str, *, expected_text: str = "") -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id

        try:
            with self._navigation_scope("baseline"):
                page = self._ensure_page()

                async def _baseline() -> dict[str, Any]:
                    payload = await fetch_thread_payload_async(page, thread_id=target_thread_id, limit=30)
                    latest = _latest_outbound_from_payload(
                        payload,
                        self_user_id=self.user_id,
                        thread_id=target_thread_id,
                        expected_text=expected_text,
                    )
                    if latest is None:
                        return {"ok": True, "item_id": "", "timestamp": None, "reason": "baseline_empty"}
                    return {
                        "ok": True,
                        "item_id": str(latest.get("item_id") or "").strip(),
                        "timestamp": latest.get("timestamp"),
                        "reason": "baseline_ok",
                    }

                return dict(self._runtime.run_async(_baseline()))
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            return {"ok": False, "item_id": "", "timestamp": None, "reason": "navigation_locked", **payload}

    def refresh_thread_for_confirmation(self, thread_id: str) -> bool:
        ready_ok, _reason = self.ensure_thread_ready_strict(thread_id)
        return bool(ready_ok)

    def send_text_with_ack(self, thread_id: str, text: str, timeout: float = 4.0) -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id
        content = str(text or "").strip()
        if not target_thread_id:
            self._telemetry("send_fail", "failed", thread_id=target_thread_id, reason="invalid_thread_id")
            return {"ok": False, "item_id": None, "reason": "invalid_thread_id"}
        if not content:
            self._telemetry("send_fail", "failed", thread_id=target_thread_id, reason="empty_text")
            return {"ok": False, "item_id": None, "reason": "empty_text"}
        if not self._bypass_account_quota:
            can_send, sent_today, limit = can_send_message_for_account(
                account=self.account,
                username=self.username,
                default=None,
            )
            if not can_send:
                self._telemetry(
                    "send_fail",
                    "failed",
                    thread_id=target_thread_id,
                    reason="account_quota_reached",
                    sent_today=sent_today,
                    limit=limit,
                )
                return {
                    "ok": False,
                    "item_id": None,
                    "reason": f"account_quota_reached:{sent_today}/{limit}",
                }
        try:
            with self._navigation_scope("send_text", timeout=max(self._navigation_timeout_seconds, float(timeout or 0.0) + 10.0)):
                ready_ok, ready_reason = self.ensure_thread_ready_strict(target_thread_id)
                if not ready_ok:
                    self._telemetry(
                        "send_fail",
                        "failed",
                        thread_id=target_thread_id,
                        reason=f"thread_not_ready:{ready_reason}",
                    )
                    return {"ok": False, "item_id": None, "reason": f"thread_not_ready:{ready_reason}"}

                baseline = self.get_outbound_baseline(target_thread_id, expected_text=content)
                baseline_item_id = str(baseline.get("item_id") or "").strip()
                baseline_timestamp = baseline.get("timestamp")
                page = self._ensure_page()
                send_timeout_ms = max(2_500, int(max(1.0, float(timeout or 4.0)) * 1000.0))
                sent_after_ts = time.time()
                confirm_attempts = max(3, int(max(1.0, float(timeout or 4.0)) * 2.5))

                async def _send() -> bool:
                    self._telemetry(
                        "composer_wait_start",
                        "started",
                        thread_id=target_thread_id,
                        timeout_ms=send_timeout_ms,
                        page_id=id(page),
                        url=str(getattr(page, "url", "") or ""),
                    )
                    composer = await _wait_for_visible_locator_async(
                        page,
                        _COMPOSER_SELECTORS,
                        timeout_ms=send_timeout_ms,
                    )
                    if composer is None:
                        self._log_exact_fail_reason(
                            thread_id=target_thread_id,
                            reason="composer_not_found",
                            page_id=id(page),
                            url=str(getattr(page, "url", "") or ""),
                        )
                        return False
                    self._telemetry(
                        "composer_ready",
                        "ok",
                        thread_id=target_thread_id,
                        page_id=id(page),
                        url=str(getattr(page, "url", "") or ""),
                    )
                    activated, activate_reason = await _activate_composer_async(page, composer)
                    if not activated:
                        self._log_exact_fail_reason(
                            thread_id=target_thread_id,
                            reason=activate_reason,
                            page_id=id(page),
                            url=str(getattr(page, "url", "") or ""),
                        )
                        return False
                    self._telemetry(
                        "type_start",
                        "started",
                        thread_id=target_thread_id,
                        message_length=len(content),
                    )
                    typed, type_reason = await _fill_composer_async(page, composer, content)
                    if not typed:
                        self._log_exact_fail_reason(
                            thread_id=target_thread_id,
                            reason=type_reason,
                            page_id=id(page),
                            url=str(getattr(page, "url", "") or ""),
                        )
                        return False
                    triggered, trigger_reason = await _trigger_send_async(page, composer)
                    if not triggered:
                        self._log_exact_fail_reason(
                            thread_id=target_thread_id,
                            reason=trigger_reason,
                            page_id=id(page),
                            url=str(getattr(page, "url", "") or ""),
                        )
                        return False
                    self._telemetry(
                        "send_triggered",
                        "ok",
                        thread_id=target_thread_id,
                        page_id=id(page),
                        url=str(getattr(page, "url", "") or ""),
                    )
                    return True

                self._telemetry(
                    "send_attempt",
                    "started",
                    thread_id=target_thread_id,
                    message_length=len(content),
                )
                sent_ok = bool(self._runtime.run_async(_send()))
                if not sent_ok:
                    self._telemetry(
                        "send_fail",
                        "failed",
                        thread_id=target_thread_id,
                        reason="composer_send_failed",
                    )
                    return {"ok": False, "item_id": None, "reason": "composer_send_failed"}

                self._telemetry(
                    "confirm_start",
                    "started",
                    thread_id=target_thread_id,
                    baseline_item_id=baseline_item_id,
                    baseline_timestamp=baseline_timestamp if baseline_timestamp is not None else "",
                )
                confirm = self.confirm_new_outbound_after_baseline(
                    target_thread_id,
                    baseline_item_id=baseline_item_id,
                    baseline_timestamp=baseline_timestamp,
                    sent_after_ts=sent_after_ts,
                    expected_text=content,
                    attempts=confirm_attempts,
                    poll_interval_seconds=0.7,
                    allow_dom=True,
                )
                if bool(confirm.get("ok", False)):
                    self._telemetry(
                        "confirm_ok",
                        "ok",
                        thread_id=target_thread_id,
                        item_id=str(confirm.get("item_id") or ""),
                        reason=str(confirm.get("reason") or ""),
                    )
                    self._telemetry(
                        "send_success",
                        "ok",
                        thread_id=target_thread_id,
                        item_id=str(confirm.get("item_id") or ""),
                        reason=str(confirm.get("reason") or ""),
                    )
                    return confirm
                if self.refresh_thread_for_confirmation(target_thread_id):
                    confirm = self.confirm_new_outbound_after_baseline(
                        target_thread_id,
                        baseline_item_id=baseline_item_id,
                        baseline_timestamp=baseline_timestamp,
                        sent_after_ts=sent_after_ts,
                        expected_text=content,
                        attempts=max(2, confirm_attempts // 2),
                        poll_interval_seconds=0.6,
                        allow_dom=True,
                    )
                    if bool(confirm.get("ok", False)):
                        self._telemetry(
                            "confirm_ok",
                            "ok",
                            thread_id=target_thread_id,
                            item_id=str(confirm.get("item_id") or ""),
                            reason=str(confirm.get("reason") or ""),
                        )
                        self._telemetry(
                            "send_success",
                            "ok",
                            thread_id=target_thread_id,
                            item_id=str(confirm.get("item_id") or ""),
                            reason=str(confirm.get("reason") or ""),
                        )
                        return confirm
                reconciled = self.confirm_outbound_via_thread_read(
                    target_thread_id,
                    baseline_item_id=baseline_item_id,
                    baseline_timestamp=baseline_timestamp,
                    sent_after_ts=sent_after_ts,
                    expected_text=content,
                )
                if bool(reconciled.get("ok", False)):
                    self._telemetry(
                        "confirm_ok",
                        "ok",
                        thread_id=target_thread_id,
                        item_id=str(reconciled.get("item_id") or ""),
                        reason=str(reconciled.get("reason") or ""),
                    )
                    self._telemetry(
                        "send_success",
                        "ok",
                        thread_id=target_thread_id,
                        item_id=str(reconciled.get("item_id") or ""),
                        reason=str(reconciled.get("reason") or ""),
                    )
                    return reconciled
                final_reason = str(reconciled.get("reason") or confirm.get("reason") or "send_unconfirmed")
                self._telemetry(
                    "send_fail",
                    "failed",
                    thread_id=target_thread_id,
                    reason=final_reason,
                )
                self._log_exact_fail_reason(thread_id=target_thread_id, reason=final_reason)
                return {
                    "ok": False,
                    "item_id": None,
                    "reason": final_reason,
                }
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            self._telemetry(
                "send_fail",
                "failed",
                thread_id=target_thread_id,
                reason="navigation_locked",
                **payload,
            )
            self._log_exact_fail_reason(thread_id=target_thread_id, reason="navigation_locked", **payload)
            return {"ok": False, "item_id": None, "reason": "navigation_locked", **payload}
        except Exception as exc:
            self._telemetry(
                "send_fail",
                "failed",
                thread_id=target_thread_id,
                reason="send_error",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            self._log_exact_fail_reason(
                thread_id=target_thread_id,
                reason=f"send_error:{exc}",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return {"ok": False, "item_id": None, "reason": f"send_error:{exc}"}

    def confirm_new_outbound_after_baseline(
        self,
        thread_id: str,
        *,
        baseline_item_id: str = "",
        baseline_timestamp: float | None = None,
        sent_after_ts: float | None = None,
        expected_text: str = "",
        attempts: int = 6,
        poll_interval_seconds: float = 0.8,
        allow_dom: bool = True,
    ) -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id
        normalized_expected = _normalize_message_text(expected_text)
        baseline_item = str(baseline_item_id or "").strip()
        baseline_ts = float(baseline_timestamp) if baseline_timestamp else None
        sent_anchor = float(sent_after_ts) if sent_after_ts else None

        try:
            with self._navigation_scope("confirm_outbound"):
                page = self._ensure_page()

                async def _confirm() -> dict[str, Any]:
                    for _ in range(max(1, int(attempts or 1))):
                        payload = await fetch_thread_payload_async(page, thread_id=target_thread_id, limit=30)
                        latest = _latest_outbound_from_payload(
                            payload,
                            self_user_id=self.user_id,
                            thread_id=target_thread_id,
                            expected_text=expected_text,
                        )
                        if latest and _is_new_outbound_record(
                            latest,
                            baseline_item_id=baseline_item,
                            baseline_timestamp=baseline_ts,
                            sent_after_ts=sent_anchor,
                        ):
                            return {
                                "ok": True,
                                "item_id": str(latest.get("item_id") or "").strip()
                                or f"confirmed-{int(time.time() * 1000)}",
                                "timestamp": latest.get("timestamp"),
                                "reason": "endpoint_confirmed",
                            }
                        if allow_dom and normalized_expected and await _dom_has_recent_outbound_text(page, normalized_expected):
                            return {
                                "ok": True,
                                "item_id": f"dom-confirmed-{int(time.time() * 1000)}",
                                "timestamp": None,
                                "reason": "dom_confirmed",
                            }
                        await page.wait_for_timeout(max(150, int(float(poll_interval_seconds or 0.8) * 1000.0)))
                    return {"ok": False, "item_id": None, "timestamp": None, "reason": "not_confirmed"}

                return dict(self._runtime.run_async(_confirm()))
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            return {"ok": False, "item_id": None, "timestamp": None, "reason": "navigation_locked", **payload}

    def confirm_outbound_via_thread_read(
        self,
        thread_id: str,
        *,
        baseline_item_id: str = "",
        baseline_timestamp: float | None = None,
        sent_after_ts: float | None = None,
        expected_text: str = "",
    ) -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip() or self._thread_id
        baseline_item = str(baseline_item_id or "").strip()
        baseline_ts = float(baseline_timestamp) if baseline_timestamp else None
        sent_anchor = float(sent_after_ts) if sent_after_ts else None
        normalized_expected = _normalize_message_text(expected_text)

        try:
            with self._navigation_scope("confirm_thread_read"):
                page = self._ensure_page()

                async def _confirm_from_thread() -> dict[str, Any]:
                    payload = await read_conversation_async(
                        page,
                        account=self.account,
                        thread_id=target_thread_id,
                        thread_href=self._thread_href,
                        message_limit=40,
                    )
                    latest = _latest_outbound_from_messages(payload.get("messages") or [], expected_text=expected_text)
                    if latest and _is_new_outbound_record(
                        latest,
                        baseline_item_id=baseline_item,
                        baseline_timestamp=baseline_ts,
                        sent_after_ts=sent_anchor,
                    ):
                        return {
                            "ok": True,
                            "item_id": str(latest.get("item_id") or "").strip()
                            or f"thread-read-confirmed-{int(time.time() * 1000)}",
                            "timestamp": latest.get("timestamp"),
                            "reason": "thread_read_confirmed",
                        }
                    if normalized_expected and await _dom_has_recent_outbound_text(page, normalized_expected):
                        return {
                            "ok": True,
                            "item_id": f"dom-confirmed-{int(time.time() * 1000)}",
                            "timestamp": None,
                            "reason": "dom_confirmed",
                        }
                    return {"ok": False, "item_id": None, "timestamp": None, "reason": "thread_read_unconfirmed"}

                return dict(self._runtime.run_async(_confirm_from_thread()))
        except NavigationLockedError as exc:
            payload = self._navigation_payload(exc)
            return {"ok": False, "item_id": None, "timestamp": None, "reason": "navigation_locked", **payload}


def send_manual_message(
    runtime: Any,
    account: dict[str, Any],
    thread_row: dict[str, Any],
    text: str,
) -> dict[str, Any]:
    content = str(text or "").strip()
    if not content:
        return {"ok": False, "reason": "empty_text"}
    thread = build_thread_like(thread_row)
    client = TaskDirectClient(
        runtime,
        account,
        thread_id=thread.id,
        thread_href=thread.link,
        bypass_account_quota=True,
    )
    try:
        result = client.send_text_with_ack(thread.id, content, timeout=4.0)
        if bool(result.get("ok", False)):
            message_id = str(result.get("item_id") or "").strip()
            responder_module._record_message_sent(
                str(account.get("username") or "").strip(),
                thread.id,
                content,
                message_id=message_id,
                recipient_username=str(thread_row.get("recipient_username") or thread.title or "").strip(),
            )
            return {
                "ok": True,
                "message_id": message_id,
                "timestamp": result.get("timestamp"),
                "reason": str(result.get("reason") or "ok"),
            }
        return {
            "ok": False,
            "reason": str(result.get("reason") or "send_failed"),
        }
    finally:
        client.close()


def reconcile_manual_message(
    runtime: Any,
    account: dict[str, Any],
    thread_row: dict[str, Any],
    text: str,
    *,
    sent_after_ts: float | None = None,
) -> dict[str, Any]:
    content = str(text or "").strip()
    if not content:
        return {"ok": False, "reason": "empty_text"}
    thread = build_thread_like(thread_row)
    client = TaskDirectClient(
        runtime,
        account,
        thread_id=thread.id,
        thread_href=thread.link,
    )
    try:
        result = client.confirm_outbound_via_thread_read(
            thread.id,
            sent_after_ts=sent_after_ts,
            expected_text=content,
        )
        if bool(result.get("ok", False)):
            return {
                "ok": True,
                "message_id": str(result.get("item_id") or "").strip(),
                "timestamp": result.get("timestamp"),
                "reason": str(result.get("reason") or "thread_read_confirmed"),
            }
        return {"ok": False, "reason": str(result.get("reason") or "thread_read_unconfirmed")}
    finally:
        client.close()


def send_pack_messages(
    runtime: Any,
    account: dict[str, Any],
    thread_row: dict[str, Any],
    pack: dict[str, Any],
    *,
    conversation_text: str,
    flow_config: dict[str, Any],
) -> dict[str, Any]:
    sendable_actions = _count_pack_send_actions(pack)
    account_id = str(account.get("username") or "").strip()
    if sendable_actions <= 0:
        return {
            "ok": False,
            "completed": False,
            "sent_count": 0,
            "reason": "pack_without_send_actions",
            "error": "pack_without_send_actions",
        }
    can_send, sent_today, limit = can_send_message_for_account(
        account=account,
        username=account_id,
        default=None,
    )
    if limit is not None:
        remaining = max(0, int(limit or 0) - int(sent_today or 0))
        if (not can_send) or remaining < sendable_actions:
            reason = f"pack_quota_insufficient:{sent_today}/{limit}:need={sendable_actions}"
            return {
                "ok": False,
                "completed": False,
                "sent_count": 0,
                "reason": reason,
                "error": reason,
            }

    thread = build_thread_like(thread_row)
    client = TaskDirectClient(
        runtime,
        account,
        thread_id=thread.id,
        thread_href=thread.link,
    )
    memory = responder_module._get_account_memory(str(account.get("username") or "").strip())
    api_key = responder_module._resolve_ai_api_key()
    try:
        result = responder_module.execute_pack(
            dict(pack or {}),
            str(account.get("username") or "").strip(),
            dict(memory or {}),
            client=client,
            thread=thread,
            thread_id=thread.id,
            recipient_username=str(thread_row.get("recipient_username") or thread.title or "").strip(),
            api_key=api_key,
            conversation_text=str(conversation_text or "").strip(),
            strategy_name=str(pack.get("type") or pack.get("name") or "inbox_pack"),
            persist_pending=False,
            flow_config=dict(flow_config or {}),
        )
        response = dict(result or {})
        response["ok"] = bool(response.get("completed", False))
        response.setdefault("reason", str(response.get("error") or "ok"))
        return response
    finally:
        client.close()


def _count_pack_send_actions(pack: dict[str, Any]) -> int:
    actions = pack.get("actions")
    if not isinstance(actions, list):
        return 0
    total = 0
    for raw_action in actions:
        if not isinstance(raw_action, dict):
            continue
        action_type = str(raw_action.get("type") or "").strip().lower()
        if action_type == "text_fixed" and str(raw_action.get("content") or "").strip():
            total += 1
        elif action_type == "text_adaptive" and str(raw_action.get("instruction") or "").strip():
            total += 1
    return total


def build_thread_like(thread_row: dict[str, Any]) -> ThreadLike:
    thread_id = str(thread_row.get("thread_id") or "").strip()
    recipient_username = str(thread_row.get("recipient_username") or "").strip()
    display_name = str(thread_row.get("display_name") or recipient_username or thread_id).strip()
    users = [
        UserLike(
            pk=recipient_username or display_name,
            id=recipient_username or display_name,
            username=recipient_username or display_name,
        )
    ]
    return ThreadLike(
        id=thread_id,
        pk=thread_id,
        users=users,
        unread_count=max(0, int(thread_row.get("unread_count") or 0)),
        link=str(thread_row.get("thread_href") or "").strip() or THREAD_URL_TEMPLATE.format(thread_id=thread_id),
        title=display_name,
        snippet=str(thread_row.get("last_message_text") or "").strip(),
        source_index=-1,
    )


def _latest_outbound_from_payload(
    payload: Any,
    *,
    self_user_id: str,
    thread_id: str,
    expected_text: str = "",
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self_user_id)
    expected_id = str(thread_id or "").strip()
    normalized_expected = _normalize_message_text(expected_text)
    candidates: list[dict[str, Any]] = []
    for message in parsed:
        if str(getattr(message, "direction", "") or "").strip().lower() != "outbound":
            continue
        msg_thread_id = str(getattr(message, "thread_id", "") or "").strip()
        if expected_id and msg_thread_id and msg_thread_id != expected_id:
            continue
        if normalized_expected and not _message_text_matches(normalized_expected, str(getattr(message, "text", "") or "")):
            continue
        candidates.append(
            {
                "item_id": str(getattr(message, "item_id", "") or "").strip(),
                "timestamp": float(getattr(message, "timestamp", 0.0) or 0.0) or None,
                "text": str(getattr(message, "text", "") or "").strip(),
            }
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: ((item.get("timestamp") or 0.0), str(item.get("item_id") or "")), reverse=True)
    return candidates[0]


def _latest_outbound_from_messages(
    messages: list[dict[str, Any]] | None,
    *,
    expected_text: str = "",
) -> dict[str, Any] | None:
    normalized_expected = _normalize_message_text(expected_text)
    candidates: list[dict[str, Any]] = []
    for raw in messages or []:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("direction") or "").strip().lower() != "outbound":
            continue
        text = str(raw.get("text") or "").strip()
        if normalized_expected and not _message_text_matches(normalized_expected, text):
            continue
        candidates.append(
            {
                "item_id": str(raw.get("message_id") or "").strip(),
                "timestamp": raw.get("timestamp"),
                "text": text,
            }
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: ((item.get("timestamp") or 0.0), str(item.get("item_id") or "")), reverse=True)
    return candidates[0]


def _is_new_outbound_record(
    record: dict[str, Any],
    *,
    baseline_item_id: str,
    baseline_timestamp: float | None,
    sent_after_ts: float | None,
) -> bool:
    item_id = str(record.get("item_id") or "").strip()
    try:
        timestamp = float(record.get("timestamp")) if record.get("timestamp") is not None else None
    except Exception:
        timestamp = None
    if baseline_item_id and item_id and item_id != baseline_item_id:
        return True
    if baseline_timestamp is not None and timestamp is not None and timestamp > (baseline_timestamp + 0.001):
        return True
    if sent_after_ts is not None and timestamp is not None and timestamp >= (sent_after_ts - 1.5):
        return True
    return False


async def _find_visible_locator_async(page, selectors: tuple[str, ...]):
    for selector in selectors:
        try:
            locator = page.locator(selector)
            total = await locator.count()
        except Exception:
            continue
        for index in range(min(total, 4)):
            try:
                candidate = locator.nth(index)
                if await candidate.is_visible():
                    return candidate
            except Exception:
                continue
    return None


async def _wait_for_visible_locator_async(
    page,
    selectors: tuple[str, ...],
    *,
    timeout_ms: int,
    poll_interval_ms: int = 160,
):
    started = time.time()
    remaining = max(250, int(timeout_ms or 0))
    while remaining > 0:
        candidate = await _find_visible_locator_async(page, selectors)
        if candidate is not None:
            return candidate
        wait_ms = min(max(80, int(poll_interval_ms or 160)), remaining)
        try:
            await page.wait_for_timeout(wait_ms)
        except Exception:
            break
        elapsed = int((time.time() - started) * 1000.0)
        remaining = max(0, int(timeout_ms) - elapsed)
    return await _find_visible_locator_async(page, selectors)


def _page_context(page) -> Any:
    context_attr = getattr(page, "context", None)
    if callable(context_attr):
        try:
            return context_attr()
        except Exception:
            return None
    return context_attr


async def _activate_composer_async(
    page,
    composer,
    *,
    short_timeout_ms: int = 1_500,
) -> tuple[bool, str]:
    scroll = getattr(composer, "scroll_into_view_if_needed", None)
    if callable(scroll):
        try:
            await scroll(timeout=short_timeout_ms)
        except Exception:
            pass
    focus = getattr(composer, "focus", None)
    if callable(focus):
        try:
            await focus(timeout=short_timeout_ms)
            return True, "focus_ok"
        except TypeError:
            try:
                await focus()
                return True, "focus_ok"
            except Exception:
                pass
        except Exception:
            pass
    try:
        await composer.click(timeout=short_timeout_ms)
        return True, "click_ok"
    except TypeError:
        try:
            await composer.click()
            return True, "click_ok"
        except Exception:
            pass
    except Exception:
        pass
    try:
        await composer.click(timeout=short_timeout_ms, force=True)
        return True, "force_click_ok"
    except TypeError:
        pass
    except Exception:
        pass
    try:
        dom_focused = await page.evaluate(
            """() => {
                const selectors = [
                    "div[role='main'] div[role='textbox'][contenteditable='true']",
                    "div[role='main'] div[contenteditable='true'][role='textbox']",
                    "div[role='main'] textarea",
                    "div[role='textbox'][contenteditable='true']",
                    "div[contenteditable='true']",
                ];
                for (const selector of selectors) {
                    const node = document.querySelector(selector);
                    if (!node) continue;
                    if (typeof node.scrollIntoView === "function") {
                        node.scrollIntoView({ block: "center", inline: "nearest" });
                    }
                    if (typeof node.focus === "function") {
                        node.focus();
                    }
                    return true;
                }
                return false;
            }"""
        )
    except Exception:
        dom_focused = False
    return (True, "dom_focus_ok") if dom_focused else (False, "composer_focus_failed")


async def _fill_composer_async(
    page,
    composer,
    content: str,
    *,
    short_timeout_ms: int = 1_800,
) -> tuple[bool, str]:
    try:
        await composer.fill(content, timeout=short_timeout_ms)
        return True, "fill_ok"
    except TypeError:
        try:
            await composer.fill(content)
            return True, "fill_ok"
        except Exception:
            pass
    except Exception:
        pass
    try:
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
    except Exception:
        pass
    insert_text = getattr(page.keyboard, "insert_text", None)
    if callable(insert_text):
        try:
            await insert_text(content)
            return True, "keyboard_insert_ok"
        except Exception:
            pass
    try:
        await page.keyboard.type(content)
        return True, "keyboard_type_ok"
    except Exception:
        pass
    type_method = getattr(composer, "type", None)
    if callable(type_method):
        try:
            await type_method(content)
            return True, "composer_type_ok"
        except Exception:
            pass
    return False, "composer_fill_failed"


async def _trigger_send_async(
    page,
    composer,
    *,
    short_timeout_ms: int = 1_500,
) -> tuple[bool, str]:
    press = getattr(composer, "press", None)
    if callable(press):
        try:
            await press("Enter", timeout=short_timeout_ms)
            return True, "enter_ok"
        except TypeError:
            try:
                await press("Enter")
                return True, "enter_ok"
            except Exception:
                pass
        except Exception:
            pass
    try:
        await page.keyboard.press("Enter")
        return True, "keyboard_enter_ok"
    except Exception:
        pass
    button = await _find_visible_locator_async(page, _SEND_BUTTON_SELECTORS)
    if button is None:
        return False, "send_button_not_found"
    try:
        await button.click(timeout=short_timeout_ms)
        return True, "send_button_click_ok"
    except TypeError:
        try:
            await button.click()
            return True, "send_button_click_ok"
        except Exception:
            return False, "send_button_click_failed"
    except Exception:
        return False, "send_button_click_failed"


async def _dom_has_recent_outbound_text(page, normalized_expected: str) -> bool:
    try:
        values = await page.evaluate(
            """() => {
                const root = document.querySelector("main");
                if (!root) return [];
                const texts = [];
                const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
                while (walker.nextNode()) {
                    const text = String(walker.currentNode?.textContent || "").replace(/\\s+/g, " ").trim();
                    if (text) texts.push(text);
                }
                return texts.slice(-20);
            }"""
        )
    except Exception:
        return False
    rows = values if isinstance(values, list) else []
    for raw in rows:
        candidate = _normalize_message_text(str(raw or ""))
        if candidate and _message_text_matches(normalized_expected, candidate):
            return True
    return False


def build_conversation_text(messages: list[dict[str, Any]], *, limit: int = 12) -> str:
    recent = list(messages or [])[-max(1, int(limit or 12)) :]
    lines: list[str] = []
    for row in recent:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "unknown").strip().lower()
        speaker = "cliente" if direction == "inbound" else "equipo"
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)
