from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from playwright.async_api import Locator, Page

from core.account_limits import account_message_limit, can_send_message_for_account
from paths import storage_root
from runtime.runtime import STOP_EVENT
from src.auth.persistent_login import ensure_logged_in_async
from src.browser_telemetry import log_browser_stage
from src.playwright_service import BASE_PROFILES
from src.proxy_payload import normalize_playwright_proxy, proxy_from_account
from src.runtime.playwright_runtime import (
    PlaywrightRuntimeCancelledError,
    PlaywrightRuntimeTimeoutError,
    run_coroutine_sync,
)
from src.transport.delivery_verifier import DeliveryVerifier
from src.transport.inbox_navigator import InboxNavigator
from src.transport.message_composer import MessageComposer
from src.transport.session_manager import ManagedSession, SessionManager
from src.transport.thread_resolver import SidebarThreadResolver, ThreadOpenResult

logger = logging.getLogger(__name__)

INSTAGRAM = "https://www.instagram.com"
DIRECT_INBOX = f"{INSTAGRAM}/direct/inbox/"

MAX_FLOW_SECONDS = max(20.0, float(os.getenv("HUMAN_DM_MAX_FLOW_SECONDS", "75")))
INBOX_READY_TIMEOUT_MS = min(22_000, int(MAX_FLOW_SECONDS * 1000))
THREAD_OPEN_TIMEOUT_MS = max(6_000, int(os.getenv("HUMAN_DM_THREAD_OPEN_TIMEOUT_MS", "12000")))
COMPOSER_VISIBLE_TIMEOUT_MS = max(4_000, int(os.getenv("HUMAN_DM_COMPOSER_VISIBLE_TIMEOUT_MS", "8000")))
SEND_NETWORK_TIMEOUT_MS = max(4_000, int(os.getenv("HUMAN_DM_SEND_NETWORK_TIMEOUT_MS", "8000")))
TYPE_DELAY_MIN_MS = max(8, int(os.getenv("HUMAN_DM_TYPE_DELAY_MIN_MS", "18")))
TYPE_DELAY_MAX_MS = max(TYPE_DELAY_MIN_MS, int(os.getenv("HUMAN_DM_TYPE_DELAY_MAX_MS", "45")))
POST_SEND_DOM_VERIFY_MS = max(800, int(os.getenv("HUMAN_DM_POST_SEND_DOM_VERIFY_MS", "2400")))

ALLOW_UNVERIFIED = os.getenv("HUMAN_DM_ALLOW_UNVERIFIED", "0").strip().lower() in {
    "1", "true", "yes", "y",
}

INBOX_READY_SELECTORS = (
    "div[role='navigation'] input[name='searchInput']",
    "div[role='navigation'] input[placeholder*='Search']",
    "div[role='navigation'] input[placeholder*='Buscar']",
    "div[role='navigation'] a[href*='/direct/t/']",
)
SIDEBAR_SEARCH_INPUTS = (
    "div[role='navigation'] input[name='searchInput']",
    "div[role='navigation'] input[placeholder*='Search']",
    "div[role='navigation'] input[placeholder*='Buscar']",
    "div[role='navigation'] [role='searchbox']",
)
SIDEBAR_RESULT_ROWS = (
    "div[role='navigation'] ul li",
    "div[role='navigation'] [role='listitem']",
    "div[role='navigation'] div[role='button'][tabindex='0']",
    "div[role='navigation'] div[role='button']",
    "div[role='navigation'] a[href*='/direct/t/']",
)

THREAD_COMPOSERS = (
    "[role='main'] div[role='textbox'][aria-label='Message']",
    "[role='main'] div[role='textbox'][aria-label='Mensaje']",
    "[role='main'] div[role='textbox'][contenteditable='true']",
    "[role='main'] div[contenteditable='true'][role='textbox']",
    "[role='main'] div[contenteditable='true']",
    "[role='main'] textarea[placeholder*='message']",
    "[role='main'] textarea[placeholder*='Mensaje']",
    "[role='main'] textarea[aria-label*='Message']",
    "[role='main'] textarea[aria-label*='Mensaje']",
    "[role='main'] textarea",
)

SEND_BUTTONS = (
    "[role='main'] button[aria-label*='Send']",
    "[role='main'] button[aria-label*='Enviar']",
    "[role='main'] button:has-text('Send')",
    "[role='main'] button:has-text('Enviar')",
    "[role='main'] div[role='button'][aria-label*='Send']",
    "[role='main'] div[role='button'][aria-label*='Enviar']",
    "[role='main'] div[role='button']:has-text('Send')",
    "[role='main'] div[role='button']:has-text('Enviar')",
    "[role='main'] [data-testid='send']",
    "[role='main'] [data-testid*='send']",
    "[role='main'] button[type='submit']",
)

_DEBUG_ENV = "HUMAN_DM_DEBUG"
_STORAGE_ROOT = storage_root(Path(__file__).resolve().parents[2])
_DEBUG_SCREENSHOT_DIR = _STORAGE_ROOT / "debug_screenshots"
_FAILURE_CAPTURE_ENV = "HUMAN_DM_CAPTURE_FAILURE_ARTIFACTS"
_FAILURE_ARTIFACT_DIR = _STORAGE_ROOT / "dm_failures"
_FAILURE_HTML_MAX_BYTES = max(120_000, int(os.getenv("HUMAN_DM_FAILURE_HTML_MAX_BYTES", "260000")))
_DM_CTX: ContextVar[dict[str, Any]] = ContextVar("human_dm_ctx", default={})
_STAGE_CALLBACK = Callable[[str, Dict[str, Any]], None]


class CampaignSendCancelled(RuntimeError):
    def __init__(self, stage: str, reason: str = "campaign_stop_requested") -> None:
        self.stage = str(stage or "").strip() or "unknown"
        self.reason = str(reason or "").strip() or "campaign_stop_requested"
        super().__init__(self.reason)


class CampaignSendDeadlineExceeded(TimeoutError):
    def __init__(self, stage: str, reason: str = "send_deadline_exceeded") -> None:
        self.stage = str(stage or "").strip() or "unknown"
        self.reason = str(reason or "").strip() or "send_deadline_exceeded"
        super().__init__(self.reason)

def _debug_enabled() -> bool:
    return os.getenv(_DEBUG_ENV, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _failure_capture_enabled() -> bool:
    raw = os.getenv(_FAILURE_CAPTURE_ENV, "1").strip().lower()
    return raw not in {"0", "false", "no", "n", "off"}


def _safe_slug(value: str, fallback: str = "x") -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return cleaned or fallback


def _compact_text(value: str, limit: int = 180) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


def _dm_ctx() -> dict[str, Any]:
    try:
        ctx = _DM_CTX.get()
        return ctx if isinstance(ctx, dict) else {}
    except Exception:
        return {}


def _dm_log(stage: str, **fields: Any) -> None:
    if not _debug_enabled():
        return
    ctx = _dm_ctx()
    account = ctx.get("account") or "-"
    lead = ctx.get("lead") or "-"
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    kv = " ".join(f"{k}={_compact_text(str(v), 320)}" for k, v in fields.items() if v is not None)
    line = f"[DM][account=@{account}][lead=@{lead}][stage={stage}] ts={ts}"
    if kv:
        line += " " + kv
    try:
        log_path = ctx.get("log_path")
        if log_path:
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            with Path(log_path).open("a", encoding="utf-8", errors="ignore") as handle:
                handle.write(line + "\n")
    except Exception:
        pass
    try:
        logger.info(line)
    except Exception:
        pass


def _flow_log(message: str, *, level: int = logging.INFO) -> None:
    line = str(message or "").strip()
    if not line:
        return
    try:
        logger.log(level, line)
    except Exception:
        pass
    if _debug_enabled():
        _dm_log("FLOW_TRACE", message=line)


def _debug_artifact_path(stage: str, ext: str, *, tag: Optional[str] = None) -> Path:
    ctx = _dm_ctx()
    debug_id = str(ctx.get("debug_id") or int(time.time() * 1000))
    account = _safe_slug(str(ctx.get("account") or "account"), "account")
    lead = _safe_slug(str(ctx.get("lead") or "lead"), "lead")
    stage_slug = _safe_slug(stage, "stage")
    tag_slug = _safe_slug(tag, "") if tag else ""
    suffix = f"_{tag_slug}" if tag_slug else ""
    ext_clean = ext.lstrip(".") or "txt"
    return _DEBUG_SCREENSHOT_DIR / f"{debug_id}_{account}_{lead}_{stage_slug}{suffix}.{ext_clean}"


def _failure_artifact_path(stage: str, ext: str, *, tag: Optional[str] = None) -> Path:
    ctx = _dm_ctx()
    stamp = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    account = _safe_slug(str(ctx.get("account") or "account"), "account")
    lead = _safe_slug(str(ctx.get("lead") or "lead"), "lead")
    stage_slug = _safe_slug(stage, "stage")
    tag_slug = _safe_slug(tag, "") if tag else ""
    suffix = f"_{tag_slug}" if tag_slug else ""
    ext_clean = ext.lstrip(".") or "txt"
    return _FAILURE_ARTIFACT_DIR / f"{stamp}_{account}_{lead}_{stage_slug}{suffix}.{ext_clean}"


async def _capture_failure_artifacts(
    page: Optional[Page],
    stage: str,
    *,
    detail: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not _failure_capture_enabled():
        return {}

    artifacts: Dict[str, Any] = {}
    ctx = _dm_ctx()
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        _FAILURE_ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return {}

    current_url = ""
    if page is not None:
        try:
            current_url = page.url or ""
        except Exception:
            current_url = ""
    if current_url:
        artifacts["url"] = current_url

    if page is not None:
        try:
            screenshot_path = _failure_artifact_path(stage, "png")
            await page.screenshot(path=str(screenshot_path), full_page=True)
            artifacts["screenshot"] = str(screenshot_path)
        except Exception:
            pass

        try:
            html_value = ""
            main = page.locator("main")
            if await main.count() > 0:
                html_value = str(await main.first.evaluate("el => el ? el.outerHTML : ''") or "")
            if not html_value:
                html_value = str(await page.content() or "")
            if html_value:
                if len(html_value) > _FAILURE_HTML_MAX_BYTES:
                    html_value = html_value[:_FAILURE_HTML_MAX_BYTES] + "\n<!-- truncated -->\n"
                html_path = _failure_artifact_path(stage, "html", tag="main")
                html_path.write_text(html_value, encoding="utf-8", errors="ignore")
                artifacts["main_html"] = str(html_path)
        except Exception:
            pass

    meta = {
        "ts": now_iso,
        "stage": stage,
        "detail": detail,
        "account": ctx.get("account") or "",
        "lead": ctx.get("lead") or "",
        "url": current_url,
        "payload": payload or {},
    }
    try:
        meta_path = _failure_artifact_path(stage, "json", tag="meta")
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        artifacts["meta"] = str(meta_path)
    except Exception:
        pass

    return artifacts


async def _debug_screenshot(page: Optional[Page], stage: str, *, tag: Optional[str] = None) -> Optional[str]:
    if not _debug_enabled() or page is None:
        return None
    try:
        _DEBUG_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = _debug_artifact_path(stage, "png", tag=tag)
        await page.screenshot(path=str(path), full_page=True)
        return str(path)
    except Exception as exc:
        _dm_log("DEBUG_SCREENSHOT_FAIL", failed_stage=stage, error=repr(exc))
        return None


async def _debug_dump_outer_html(locator: Optional[Locator], stage: str, *, tag: str) -> Optional[str]:
    if not _debug_enabled() or locator is None:
        return None
    try:
        if await locator.count() <= 0:
            return None
        html = await locator.first.evaluate("el => el ? el.outerHTML : ''")
        if not html:
            return None
        text = str(html)
        if len(text) > 220_000:
            text = text[:220_000] + "\n<!-- truncated -->\n"
        path = _debug_artifact_path(stage, "html", tag=tag)
        path.write_text(text, encoding="utf-8", errors="ignore")
        return str(path)
    except Exception:
        return None

class HumanInstagramSender:
    def __init__(self, headless: bool = True, *, keep_browser_open_per_account: bool = False) -> None:
        self.headless = headless
        self.keep_browser_open_per_account = bool(keep_browser_open_per_account)
        self._active_flow_hook: Optional[Callable[[str, bool], None]] = None
        self._session_manager = SessionManager(
            headless=self.headless,
            keep_browser_open_per_account=self.keep_browser_open_per_account,
            profiles_root=BASE_PROFILES,
            normalize_username=self._normalize_username,
            log_event=_dm_log,
        )
        self._delivery_verifier = DeliveryVerifier(self)
        self._inbox_navigator = InboxNavigator(
            self,
            direct_inbox=DIRECT_INBOX,
            inbox_ready_selectors=INBOX_READY_SELECTORS,
            inbox_ready_timeout_ms=INBOX_READY_TIMEOUT_MS,
            log_event=_dm_log,
        )
        self._message_composer = MessageComposer(
            self,
            thread_composers=THREAD_COMPOSERS,
            send_buttons=SEND_BUTTONS,
            composer_visible_timeout_ms=COMPOSER_VISIBLE_TIMEOUT_MS,
            type_delay_min_ms=TYPE_DELAY_MIN_MS,
            type_delay_max_ms=TYPE_DELAY_MAX_MS,
            log_event=_dm_log,
        )
        self._thread_resolver = SidebarThreadResolver(
            self,
            search_inputs=SIDEBAR_SEARCH_INPUTS,
            result_rows=SIDEBAR_RESULT_ROWS,
            thread_open_timeout_ms=THREAD_OPEN_TIMEOUT_MS,
            sidebar_row_timeout_ms=min(8_000, THREAD_OPEN_TIMEOUT_MS),
            log_event=_dm_log,
        )

    def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
        self._session_manager.close_all_sessions_sync(timeout=timeout)

    async def _sleep(self, low: float, high: float) -> None:
        await asyncio.sleep(random.uniform(low, high))

    def _normalize_username(self, username: str) -> str:
        return username.strip().lstrip("@").split("?", 1)[0]

    def _message_snippet(self, text: str, limit: int = 48) -> str:
        for line in (text or "").splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:limit]
        return (text or "").strip()[:limit]

    def _normalize_text_match(self, value: str) -> str:
        return re.sub(r"\s+", " ", (value or "").strip()).lower()

    def _normalize_message_text_match(self, value: str) -> str:
        normalized = self._normalize_text_match(value)
        return re.sub(r"^(tú|tu|you):\s*", "", normalized)

    def _extract_username_tokens(self, value: str) -> list[str]:
        seen: set[str] = set()
        tokens: list[str] = []
        for raw in re.findall(r"@?[a-z0-9._]{1,30}", str(value or "").lower()):
            candidate = self._strict_username_token(raw)
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            tokens.append(candidate)
        return tokens

    def _text_contains_exact_username(self, value: str, username: str) -> tuple[bool, str]:
        target = self._normalize_username(username).lower()
        if not target:
            return False, ""
        for token in self._extract_username_tokens(value):
            if token == target:
                return True, token
        return False, ""

    def _is_probable_message_noise(self, value: str) -> bool:
        text = self._normalize_text_match(value)
        if not text:
            return True
        if text in {
            "instagram",
            "search",
            "buscar",
            "send",
            "enviar",
            "new message",
            "nuevo mensaje",
        }:
            return True
        if re.fullmatch(r"\d{1,2}:\d{2}", text):
            return True
        if re.fullmatch(r"\d+\s*(s|min|m|h|d|sem|w)", text):
            return True
        if re.fullmatch(r"hace\s+.+", text):
            return True
        return False

    def _filter_recent_message_texts(self, values: list[str], *, limit: int) -> list[str]:
        safe_limit = max(6, min(60, int(limit or 28)))
        filtered: list[str] = []
        for value in values:
            cleaned = re.sub(r"\s+", " ", str(value or "").strip())
            if not cleaned or self._is_probable_message_noise(cleaned):
                continue
            filtered.append(cleaned)
        return filtered[-safe_limit:]

    def _result(
        self,
        ok: bool,
        detail: str,
        payload: Dict[str, Any],
        *,
        return_detail: bool,
        return_payload: bool,
    ) -> Union[bool, Tuple[bool, Optional[str]], Tuple[bool, Optional[str], Dict[str, Any]]]:
        if return_payload:
            return ok, detail, payload
        if return_detail:
            return ok, detail
        return ok

    def _remaining_ms(self, deadline: float, cap_ms: int) -> int:
        remaining = int((deadline - time.time()) * 1000)
        if remaining <= 0:
            return 0
        return min(cap_ms, remaining)

    def _is_chrome_error_url(self, page: Optional[Page]) -> bool:
        if page is None:
            return False
        try:
            current = str(page.url or "").strip().lower()
        except Exception:
            return False
        return current.startswith("chrome-error://")

    def _is_fatal_session_error(self, error: BaseException) -> bool:
        text = str(error or "").strip().lower()
        if not text:
            return False
        return any(
            token in text
            for token in (
                "target page, context or browser has been closed",
                "connection closed",
                "browser has been closed",
                "target closed",
                "protocol error",
                "playwright",
            )
        )

    async def _recover_inbox_after_chrome_error(self, page: Page, *, deadline: float) -> bool:
        if not self._is_chrome_error_url(page):
            return False
        _dm_log("CHROME_ERROR_DETECTED", url=page.url if page else "")
        for attempt in range(1, 3):
            home_timeout = self._remaining_ms(deadline, 12_000)
            if home_timeout <= 0:
                break
            try:
                await page.goto(INSTAGRAM, wait_until="domcontentloaded", timeout=home_timeout)
            except Exception as exc:
                _dm_log("CHROME_ERROR_RECOVER_HOME_FAIL", attempt=attempt, error=repr(exc))

            inbox_timeout = self._remaining_ms(deadline, 12_000)
            if inbox_timeout <= 0:
                break
            try:
                await page.goto(DIRECT_INBOX, wait_until="domcontentloaded", timeout=inbox_timeout)
            except Exception as exc:
                _dm_log("CHROME_ERROR_RECOVER_INBOX_FAIL", attempt=attempt, error=repr(exc))

            ready_timeout = self._remaining_ms(deadline, INBOX_READY_TIMEOUT_MS)
            if ready_timeout > 0 and await self._inbox_navigator.wait_inbox_ready(page, ready_timeout):
                _dm_log("CHROME_ERROR_RECOVERED", attempt=attempt, url=page.url if page else "")
                print("[BrowserSession] chrome-error recovered and inbox restored")
                return True
            try:
                await page.wait_for_timeout(220)
            except Exception:
                break

        _dm_log("CHROME_ERROR_RECOVERY_FAILED", url=page.url if page else "")
        return False

    async def _first_visible(self, root: Any, selectors: tuple[str, ...], *, max_scan_per_selector: int = 4) -> Optional[Locator]:
        for sel in selectors:
            try:
                loc = root.locator(sel)
                count = await loc.count()
            except Exception:
                continue
            for idx in range(min(count, max_scan_per_selector)):
                candidate = loc.nth(idx)
                try:
                    if await candidate.is_visible():
                        return candidate
                except Exception:
                    continue
        return None

    def _strict_username_token(self, value: str) -> str:
        candidate = str(value or "").strip().lstrip("@").lower()
        if not re.fullmatch(r"[a-z0-9._]{1,30}", candidate):
            return ""
        return candidate

    async def _row_contains_exact_span_username(self, row: Locator, username: str) -> tuple[bool, str]:
        target = self._normalize_username(username).lower()
        if not target:
            return False, ""
        spans = row.locator("span[dir='auto']")
        try:
            count = await spans.count()
        except Exception:
            return False, ""
        for idx in range(min(count, 14)):
            span = spans.nth(idx)
            try:
                raw_text = (await span.inner_text() or "").strip()
            except Exception:
                try:
                    raw_text = (await span.text_content() or "").strip()
                except Exception:
                    continue
            match_ok, candidate = self._text_contains_exact_username(raw_text, target)
            if match_ok:
                return True, candidate
        try:
            row_text = (await row.inner_text() or "").strip()
        except Exception:
            try:
                row_text = (await row.text_content() or "").strip()
            except Exception:
                row_text = ""
        return self._text_contains_exact_username(row_text, target)

    async def _focus_input_best_effort(self, search_input: Locator) -> None:
        try:
            await search_input.scroll_into_view_if_needed(timeout=1_500)
        except Exception:
            pass
        try:
            await search_input.click(timeout=1_500)
            return
        except Exception:
            pass
        try:
            await search_input.focus()
            return
        except Exception:
            pass
        try:
            await search_input.evaluate(
                """(el) => {
                    try {
                        el.focus({ preventScroll: true });
                    } catch (_error) {
                        el.focus();
                    }
                }"""
            )
        except Exception:
            pass

    async def _set_input_value_best_effort(self, page: Page, search_input: Locator, value: str) -> None:
        try:
            await search_input.fill(value)
            return
        except Exception:
            pass
        try:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Delete")
        except Exception:
            pass
        try:
            await search_input.type(value, delay=random.randint(TYPE_DELAY_MIN_MS, TYPE_DELAY_MAX_MS))
            return
        except Exception:
            pass
        await search_input.evaluate(
            """(el, nextValue) => {
                el.value = String(nextValue || "");
                el.dispatchEvent(new Event("input", { bubbles: true }));
                el.dispatchEvent(new Event("change", { bubbles: true }));
            }""",
            value,
        )

    async def _clear_input_best_effort(self, page: Page, search_input: Locator) -> None:
        await self._set_input_value_best_effort(page, search_input, "")

    async def _type_input_like_human(self, page: Page, search_input: Locator, value: str) -> None:
        text = str(value or "")
        if not text:
            return
        char_count = max(1, len(text))
        target_total_ms = max(950, min(1_500, 85 * char_count))
        per_char_delay = max(TYPE_DELAY_MIN_MS, min(140, int(round(target_total_ms / char_count))))
        try:
            await search_input.type(text, delay=per_char_delay)
            return
        except Exception:
            pass
        try:
            await page.keyboard.type(text, delay=per_char_delay)
            return
        except Exception:
            pass
        await self._set_input_value_best_effort(page, search_input, text)

    def _resolve_account_quota(self, account: Dict[str, Any], username: str) -> tuple[bool, int, int | None]:
        limit = account_message_limit(account=account, username=username, default=None)
        try:
            sent_today = max(0, int(account.get("sent_today")))
        except Exception:
            sent_today = -1
        if sent_today >= 0:
            if limit is None:
                return True, sent_today, None
            return sent_today < limit, sent_today, limit
        return can_send_message_for_account(
            account=account,
            username=str(username),
            default=None,
        )


    async def _capture_success(self, page: Optional[Page], username: str, target: str) -> Optional[str]:
        if page is None:
            return None
        try:
            folder = Path(BASE_PROFILES) / username / "dm_success"
            folder.mkdir(parents=True, exist_ok=True)
            safe_target = re.sub(r"[^a-z0-9]+", "_", target.lower()).strip("_") or "target"
            screenshot_path = folder / f"{safe_target}_{int(time.time())}.png"
            await page.screenshot(path=str(screenshot_path))
            return str(screenshot_path)
        except Exception:
            return None

    @staticmethod
    def _notify_stage(
        callback: Optional[_STAGE_CALLBACK],
        stage: str,
        **fields: Any,
    ) -> None:
        if not callable(callback):
            return
        payload = {"stage": str(stage or "").strip()}
        for key, value in fields.items():
            if value is not None:
                payload[str(key)] = value
        try:
            callback(payload["stage"], payload)
        except Exception:
            pass

    @staticmethod
    def _stop_requested() -> bool:
        try:
            return STOP_EVENT.is_set()
        except Exception:
            return False

    def _checkpoint(self, *, deadline: float, stage: str) -> None:
        if self._stop_requested():
            raise CampaignSendCancelled(stage)
        if time.time() >= float(deadline):
            raise CampaignSendDeadlineExceeded(stage)

    async def _cooperative_sleep(
        self,
        seconds: float,
        *,
        deadline: float,
        stage: str,
    ) -> None:
        remaining = max(0.0, float(seconds))
        while remaining > 0:
            self._checkpoint(deadline=deadline, stage=stage)
            deadline_remaining = max(0.0, float(deadline) - time.time())
            if deadline_remaining <= 0:
                raise CampaignSendDeadlineExceeded(stage)
            step = min(0.20, remaining, deadline_remaining)
            await asyncio.sleep(max(0.01, step))
            remaining = max(0.0, remaining - step)

    async def _await_with_deadline(
        self,
        awaitable: Any,
        *,
        deadline: float,
        stage: str,
        timeout_ms: int | None = None,
    ) -> Any:
        self._checkpoint(deadline=deadline, stage=stage)
        if timeout_ms is None:
            timeout_seconds = max(0.0, float(deadline) - time.time())
        else:
            timeout_seconds = max(0.0, self._remaining_ms(deadline, timeout_ms) / 1000.0)
        if timeout_seconds <= 0:
            raise CampaignSendDeadlineExceeded(stage)
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise CampaignSendDeadlineExceeded(stage) from exc
        except TimeoutError as exc:
            raise CampaignSendDeadlineExceeded(stage) from exc

    async def _open_thread_via_sidebar_flow(
        self,
        page: Page,
        username: str,
        *,
        deadline: float,
    ) -> ThreadOpenResult:
        target = self._normalize_username(username).lower()
        if not target:
            return ThreadOpenResult(False, "invalid_username")

        self._checkpoint(deadline=deadline, stage="opening_dm")
        _dm_log("OPEN_DM_SIDEBAR_START", target_username=target, url=page.url if page else "")

        inbox_ready = await self._await_with_deadline(
            self._inbox_navigator.ensure_inbox_surface(page, deadline=deadline),
            deadline=deadline,
            stage="opening_dm",
        )
        if not inbox_ready:
            log_browser_stage(
                component="campaign_dm_sender",
                stage="inbox_ready",
                status="failed",
                account=str(_dm_ctx().get("account") or ""),
                lead=target,
                reason="inbox_not_ready",
                url=page.url if page else "",
            )
            _dm_log("OPEN_DM_SIDEBAR_INBOX_FAIL", target_username=target, url=page.url if page else "")
            return ThreadOpenResult(False, "inbox_not_ready")
        log_browser_stage(
            component="campaign_dm_sender",
            stage="inbox_ready",
            status="ok",
            account=str(_dm_ctx().get("account") or ""),
            lead=target,
            url=page.url if page else "",
        )

        self._checkpoint(deadline=deadline, stage="opening_dm")
        open_result = await self._await_with_deadline(
            self._thread_resolver.open_thread_from_sidebar(
                page,
                target,
                deadline=deadline,
            ),
            deadline=deadline,
            stage="opening_dm",
        )
        _dm_log(
            "OPEN_DM_SIDEBAR_RESULT",
            target_username=target,
            opened=open_result.opened,
            reason=open_result.reason,
            thread_id=open_result.thread_id or "-",
            url=page.url if page else "",
        )
        log_browser_stage(
            component="campaign_dm_sender",
            stage="thread_open",
            status="ok" if open_result.opened else "failed",
            account=str(_dm_ctx().get("account") or ""),
            lead=target,
            thread_id=open_result.thread_id or "",
            reason=open_result.reason or "",
            method=open_result.method or "",
            url=page.url if page else "",
        )
        return open_result

    async def _reconcile_unverified_via_thread_refresh(
        self,
        page: Optional[Page],
        *,
        thread_id: str,
        snippet_norm: str,
        before_hits: int,
        before_tail: list[str],
        deadline: float,
    ) -> tuple[bool, str, Dict[str, Any]]:
        clean_thread_id = str(thread_id or "").strip()
        if page is None:
            return False, "", {"mode": "thread_refresh_unavailable", "reason": "page_missing"}
        if not clean_thread_id:
            return False, "", {"mode": "thread_refresh_unavailable", "reason": "thread_id_missing"}
        current_url = str(page.url or "").strip()
        if not current_url:
            return False, "", {"mode": "thread_refresh_unavailable", "reason": "url_missing"}

        timeout_ms = self._remaining_ms(deadline, 8_000)
        if timeout_ms <= 0:
            return False, "", {"mode": "thread_refresh_timeout", "reason": "deadline_exhausted"}

        try:
            await page.goto(
                current_url,
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
        except Exception as exc:
            return False, "", {"mode": "thread_refresh_error", "error": repr(exc)}

        composer = await self._message_composer.wait_composer_visible(page, deadline=deadline)
        if composer is None:
            return False, "", {
                "mode": "thread_refresh_unavailable",
                "reason": "composer_not_found_after_refresh",
                "thread_id": clean_thread_id,
            }

        dom_ok, dom_meta = await self._delivery_verifier.wait_dom_send_confirmation(
            page,
            snippet_norm=snippet_norm,
            before_hits=before_hits,
            before_tail=list(before_tail),
            timeout_ms=max(1_800, int(POST_SEND_DOM_VERIFY_MS)),
        )
        if dom_ok:
            meta = {
                "mode": "thread_refresh_dom_confirmed",
                "thread_id": clean_thread_id,
                "dom_verify": dom_meta,
            }
            return True, "thread_refresh_dom", meta

        bubble_ok, bubble_meta = await self._delivery_verifier.verify_message_visible_after_send(
            page,
            snippet_norm=snippet_norm,
            before_hits=before_hits,
            before_tail=list(before_tail),
            timeout_ms=max(2_200, int(POST_SEND_DOM_VERIFY_MS) * 2),
        )
        meta = {
            "mode": "thread_refresh_bubble_confirmed" if bubble_ok else "thread_refresh_no_match",
            "thread_id": clean_thread_id,
            "dom_verify": dom_meta,
            "bubble_verify": bubble_meta,
        }
        if bubble_ok:
            return True, "thread_refresh_bubble", meta
        return False, "", meta

    def send_message_like_human_sync(
        self,
        account: Dict,
        target_username: str,
        text: str,
        *,
        base_delay_seconds: float = 0,
        jitter_seconds: float = 0,
        proxy: Optional[Dict] = None,
        return_detail: bool = False,
        return_payload: bool = False,
        flow_timeout_seconds: float | None = None,
        stage_callback: Optional[_STAGE_CALLBACK] = None,
    ) -> Union[bool, Tuple[bool, Optional[str]], Tuple[bool, Optional[str], Dict[str, Any]]]:
        coro = self.send_message_like_human(
            account,
            target_username,
            text,
            base_delay_seconds=base_delay_seconds,
            jitter_seconds=jitter_seconds,
            proxy=proxy,
            return_detail=return_detail,
            return_payload=return_payload,
            flow_timeout_seconds=flow_timeout_seconds,
            stage_callback=stage_callback,
        )
        flow_timeout = max(10.0, float(flow_timeout_seconds or MAX_FLOW_SECONDS))
        try:
            return run_coroutine_sync(
                coro,
                timeout=flow_timeout + 5.0,
                cancel_reason="campaign_send_cancelled",
                on_cancel=lambda: self.close_all_sessions_sync(timeout=2.0),
            )
        except PlaywrightRuntimeCancelledError:
            return self._result(
                False,
                "send_cancelled",
                {
                    "method": "outbound_compose",
                    "verified": False,
                    "reason_code": "STOP_REQUESTED",
                },
                return_detail=return_detail,
                return_payload=return_payload,
            )
        except PlaywrightRuntimeTimeoutError:
            return self._result(
                False,
                "send_deadline_exceeded",
                {
                    "method": "outbound_compose",
                    "verified": False,
                    "reason_code": "FLOW_TIMEOUT",
                },
                return_detail=return_detail,
                return_payload=return_payload,
            )

    async def send_message_like_human(
        self,
        account: Dict,
        target_username: str,
        text: str,
        *,
        base_delay_seconds: float = 0,
        jitter_seconds: float = 0,
        proxy: Optional[Dict] = None,
        return_detail: bool = False,
        return_payload: bool = False,
        flow_timeout_seconds: float | None = None,
        stage_callback: Optional[_STAGE_CALLBACK] = None,
    ) -> Union[bool, Tuple[bool, Optional[str]], Tuple[bool, Optional[str], Dict[str, Any]]]:
        payload: Dict[str, Any] = {"method": "outbound_compose"}
        username = account.get("username") or ""
        if not username:
            return self._result(False, "send_not_confirmed", payload, return_detail=return_detail, return_payload=return_payload)

        normalized_target = self._normalize_username(target_username)
        if not normalized_target:
            return self._result(False, "send_not_confirmed", payload, return_detail=return_detail, return_payload=return_payload)

        can_send, sent_today, limit = self._resolve_account_quota(account, str(username))
        if not can_send:
            payload["reason_code"] = "ACCOUNT_QUOTA_REACHED"
            payload["quota"] = {
                "sent_today": int(sent_today),
                "limit": int(limit or 0),
            }
            return self._result(
                False,
                "account_quota_reached",
                payload,
                return_detail=return_detail,
                return_payload=return_payload,
            )

        debug_token = None
        debug_id = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        log_path = _DEBUG_SCREENSHOT_DIR / f"{debug_id}_{_safe_slug(username)}_{_safe_slug(normalized_target)}.log"
        if _debug_enabled():
            try:
                _DEBUG_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
        debug_token = _DM_CTX.set(
            {
                "account": username,
                "lead": normalized_target,
                "debug_id": debug_id,
                "log_path": str(log_path),
            }
        )

        session: Optional[ManagedSession] = None
        page: Optional[Page] = None
        proxy_payload = normalize_playwright_proxy(proxy) if proxy else proxy_from_account(account)
        flow_started_at = time.time()
        flow_timeout = max(10.0, float(flow_timeout_seconds or MAX_FLOW_SECONDS))
        deadline = flow_started_at + flow_timeout
        network_waiter: Optional[asyncio.Task[Any]] = None
        flow_state: Dict[str, str] = {"current": "opening session"}
        send_started = False

        def _stage(stage: str, status: str, **fields: Any) -> None:
            log_browser_stage(
                component="campaign_dm_sender",
                stage=stage,
                status=status,
                account=str(username),
                lead=normalized_target,
                **fields,
            )

        def _flow_event(message: str, track_current: bool = False) -> None:
            text_value = str(message or "").strip()
            if not text_value:
                return
            if track_current:
                flow_state["current"] = text_value
            _flow_log(f"[FLOW] {text_value}")

        async def _fail(
            detail: str,
            *,
            stage: Optional[str] = None,
            skip_reason: Optional[str] = None,
            reason_code: Optional[str] = None,
        ) -> Union[bool, Tuple[bool, Optional[str]], Tuple[bool, Optional[str], Dict[str, Any]]]:
            payload["verified"] = False
            payload["flow_stage_detail"] = flow_state.get("current") or ""
            if skip_reason:
                payload["skip_reason"] = skip_reason
            if reason_code:
                payload["reason_code"] = reason_code
            if send_started:
                _stage(
                    "send_fail",
                    "failed",
                    detail=detail,
                    stage_hint=stage or "",
                    reason_code=reason_code or "",
                    skip_reason=skip_reason or "",
                )

            artifacts = await _capture_failure_artifacts(
                page,
                stage or detail,
                detail=detail,
                payload=payload,
            )
            if artifacts:
                payload["failure_artifacts"] = artifacts
                payload["diagnostic_ref"] = artifacts.get("meta") or artifacts.get("screenshot")

            return self._result(False, detail, payload, return_detail=return_detail, return_payload=return_payload)

        async def _success(detail: str, *, verify_source: str) -> Union[bool, Tuple[bool, Optional[str]], Tuple[bool, Optional[str], Dict[str, Any]]]:
            payload["verified"] = True
            payload["verify_source"] = verify_source
            _stage(
                "send_success",
                "ok",
                verify_source=verify_source,
                url=page.url if page else "",
            )
            _dm_log(
                "SEND_OK",
                verify_source=verify_source,
                url=page.url if page else "",
            )
            try:
                if session is not None:
                    await self._session_manager.save_storage_state(session, username)
            except Exception:
                pass
            screenshot = await self._capture_success(page, username, normalized_target)
            if screenshot:
                payload["screenshot"] = screenshot
            return self._result(True, detail, payload, return_detail=return_detail, return_payload=return_payload)

        try:
            self._active_flow_hook = _flow_event
            _stage("spawn", "started", flow_timeout_seconds=flow_timeout)
            delay_total = 0.0
            if base_delay_seconds or jitter_seconds:
                delay_total = max(0.0, base_delay_seconds) + random.uniform(0.0, max(0.0, jitter_seconds))
                if delay_total > 0:
                    _dm_log("DELAY", seconds=f"{delay_total:.2f}")
                    await self._cooperative_sleep(delay_total, deadline=deadline, stage="delay")

            self._notify_stage(stage_callback, "opening_session", account=username, lead=normalized_target)
            session = await self._await_with_deadline(
                self._session_manager.open_session(
                    account=account,
                    proxy=proxy_payload,
                    login_func=ensure_logged_in_async,
                    deadline=deadline,
                ),
                deadline=deadline,
                stage="opening_session",
            )
            page = session.page
            _dm_log("LOGIN_OK", url=page.url if page else "")

            payload["normalized_username"] = normalized_target
            self._notify_stage(
                stage_callback,
                "opening_dm",
                account=username,
                lead=normalized_target,
            )
            open_result = await self._await_with_deadline(
                self._open_thread_via_sidebar_flow(
                    page,
                    normalized_target,
                    deadline=deadline,
                ),
                deadline=deadline,
                stage="opening_dm",
            )
            payload["thread_open_method"] = open_result.method
            if not open_result.opened:
                if open_result.reason == "username_not_found":
                    detail = "SKIPPED_USERNAME_NOT_FOUND"
                    skip_reason = detail
                elif open_result.reason == "inbox_not_ready":
                    detail = "INBOX_NOT_READY"
                    skip_reason = None
                elif open_result.reason == "ui_not_found":
                    detail = "UI_NOT_FOUND"
                    skip_reason = None
                elif open_result.reason == "chrome_error":
                    detail = "SKIPPED_CHROME_ERROR"
                    skip_reason = detail
                else:
                    detail = "THREAD_OPEN_FAILED"
                    skip_reason = None
                return await _fail(
                    detail,
                    stage="OPEN_DM_FAILED",
                    skip_reason=skip_reason,
                    reason_code=open_result.reason.upper() if open_result.reason else None,
                )
            payload["thread_id"] = open_result.thread_id

            _flow_event("waiting chat load", True)
            try:
                composer = await self._message_composer.thread_composer(page)
            except Exception:
                composer = None
            if composer is None:
                composer = await self._await_with_deadline(
                    self._message_composer.wait_composer_visible(page, deadline=deadline),
                    deadline=deadline,
                    stage="opening_dm",
                )
            if composer is None:
                _stage(
                    "composer_ready",
                    "failed",
                    thread_id=str(open_result.thread_id or ""),
                    reason="composer_not_found",
                )
                return await _fail(
                    "THREAD_OPEN_FAILED",
                    stage="THREAD_OPEN_FAILED",
                    reason_code="THREAD_OPEN_FAILED",
                )
            _stage(
                "composer_ready",
                "ok",
                thread_id=str(open_result.thread_id or ""),
                url=page.url if page else "",
            )
            _flow_event("chat loaded")

            self._notify_stage(
                stage_callback,
                "sending",
                account=username,
                lead=normalized_target,
                reason="compose_message",
            )
            _flow_event("sending message", True)
            send_started = True
            _stage(
                "send_attempt",
                "started",
                thread_id=str(open_result.thread_id or ""),
                message_length=len(text),
            )
            delivery_snapshot = await self._await_with_deadline(
                self._delivery_verifier.build_snapshot(page, text, limit=30),
                deadline=deadline,
                stage="sending",
            )

            await self._await_with_deadline(
                self._message_composer.type_message(page, composer, text),
                deadline=deadline,
                stage="sending",
            )

            network_timeout = self._remaining_ms(deadline, SEND_NETWORK_TIMEOUT_MS)
            if network_timeout <= 0:
                return await _fail("send_not_confirmed", stage="SEND_NETWORK_TIMEOUT")

            network_waiter = asyncio.create_task(
                self._delivery_verifier.wait_send_network_ok(
                    page,
                    delivery_snapshot.snippet,
                    timeout_ms=network_timeout,
                )
            )
            await asyncio.sleep(0)
            self._checkpoint(deadline=deadline, stage="sending")

            enter_ok = False
            try:
                await composer.press("Enter")
                enter_ok = True
            except Exception:
                try:
                    await page.keyboard.press("Enter")
                    enter_ok = True
                except Exception:
                    enter_ok = False

            fallback_clicked = False
            if not network_waiter.done():
                text_change_timeout = self._remaining_ms(deadline, 1_200)
                current_text = await self._message_composer.wait_for_text_change(
                    composer,
                    previous_text=text,
                    timeout_ms=text_change_timeout if text_change_timeout > 0 else 80,
                )
                current_text = current_text.strip().lower()
                if current_text:
                    fallback_clicked = await self._message_composer.click_send_button(page)
            _dm_log("SEND_TRIGGER", enter_ok=enter_ok, fallback_clicked=fallback_clicked, url=page.url if page else "")
            self._notify_stage(
                stage_callback,
                "sending",
                account=username,
                lead=normalized_target,
                reason="verify_send",
            )
            dom_ok, dom_meta = await self._await_with_deadline(
                self._delivery_verifier.wait_dom_send_confirmation(
                    page,
                    snippet_norm=delivery_snapshot.snippet_norm,
                    before_hits=delivery_snapshot.before_hits,
                    before_tail=list(delivery_snapshot.before_tail),
                    timeout_ms=POST_SEND_DOM_VERIFY_MS,
                ),
                deadline=deadline,
                stage="sending",
            )
            payload["dom_verify"] = dom_meta
            _dm_log(
                "SEND_DOM_VERIFY",
                ok=dom_ok,
                mode=dom_meta.get("mode"),
                before_hits=dom_meta.get("before_hits"),
                after_hits=dom_meta.get("after_hits"),
                tail_changed=dom_meta.get("tail_changed"),
            )

            net_ok = False
            net_meta: Dict[str, Any] = {}
            if network_waiter.done():
                net_ok, net_meta = await network_waiter
            elif not dom_ok:
                net_ok, net_meta = await self._await_with_deadline(
                    network_waiter,
                    deadline=deadline,
                    stage="sending",
                )
            else:
                network_waiter.cancel()
                try:
                    await network_waiter
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            payload["network"] = net_meta
            _dm_log(
                "SEND_NETWORK_RESULT",
                ok=net_ok,
                matched_responses=net_meta.get("matched_responses"),
                last_status=net_meta.get("last_status"),
                last_json_status=net_meta.get("last_json_status"),
                last_url=net_meta.get("last_url"),
            )

            bubble_ok, bubble_meta = await self._await_with_deadline(
                self._delivery_verifier.verify_message_visible_after_send(
                    page,
                    snippet_norm=delivery_snapshot.snippet_norm,
                    before_hits=delivery_snapshot.before_hits,
                    before_tail=list(delivery_snapshot.before_tail),
                    timeout_ms=max(1400, int(POST_SEND_DOM_VERIFY_MS)),
                ),
                deadline=deadline,
                stage="sending",
            )
            payload["bubble_verify"] = bubble_meta
            composer_after = (await self._message_composer.composer_text(composer)).strip()
            composer_cleared = composer_after == ""
            payload["composer_cleared"] = composer_cleared

            _dm_log(
                "SEND_CONFIRM_STEPS",
                network_ok=net_ok,
                dom_ok=dom_ok,
                bubble_ok=bubble_ok,
                composer_cleared=composer_cleared,
            )

            decision = self._delivery_verifier.decide_confirmation(
                net_ok=net_ok,
                dom_ok=dom_ok,
                bubble_ok=bubble_ok,
                composer_cleared=composer_cleared,
                allow_unverified=ALLOW_UNVERIFIED,
            )

            if decision.ok and decision.verified:
                return await _success(decision.detail, verify_source=decision.verify_source)

            if composer_cleared:
                refresh_ok, refresh_source, refresh_meta = await self._reconcile_unverified_via_thread_refresh(
                    page,
                    thread_id=str(open_result.thread_id or ""),
                    snippet_norm=delivery_snapshot.snippet_norm,
                    before_hits=delivery_snapshot.before_hits,
                    before_tail=list(delivery_snapshot.before_tail),
                    deadline=deadline,
                )
                payload["thread_refresh_verify"] = refresh_meta
                _dm_log(
                    "SEND_THREAD_REFRESH_VERIFY",
                    ok=refresh_ok,
                    mode=refresh_meta.get("mode"),
                    thread_id=refresh_meta.get("thread_id"),
                )
                if refresh_ok:
                    return await _success("sent_verified", verify_source=refresh_source)

            if decision.ok and decision.sent_unverified:
                payload["verified"] = False
                payload["reason_code"] = decision.reason_code
                payload["sent_unverified"] = True
                return self._result(
                    True,
                    decision.detail,
                    payload,
                    return_detail=return_detail,
                    return_payload=return_payload,
                )

            return await _fail(
                decision.detail,
                stage=decision.stage,
                reason_code=decision.reason_code or None,
            )

        except CampaignSendCancelled:
            raise
        except CampaignSendDeadlineExceeded as exc:
            payload["flow_stage"] = exc.stage
            payload["flow_stage_detail"] = flow_state.get("current") or ""
            _flow_log(
                f"[FLOW ERROR] timeout at stage: {payload['flow_stage_detail'] or exc.stage}",
                level=logging.WARNING,
            )
            if send_started:
                _stage(
                    "send_fail",
                    "failed",
                    detail="send_deadline_exceeded",
                    stage_hint=exc.stage,
                    reason_code="FLOW_TIMEOUT",
                )
            return await _fail(
                "send_deadline_exceeded",
                stage=exc.stage,
                reason_code="FLOW_TIMEOUT",
            )
        except Exception as exc:
            payload["verified"] = False
            payload["method"] = "outbound_compose"
            payload["error"] = repr(exc)
            if session is None:
                payload["reason_code"] = "SESSION_OPEN_FAILED"
                payload["flow_stage"] = "opening_session"
            elif page is not None and not payload.get("thread_id"):
                payload["reason_code"] = "THREAD_OPEN_FAILED"
                payload["flow_stage"] = "opening_dm"
            elif send_started:
                payload["reason_code"] = "SEND_FAILED"
                payload["flow_stage"] = "sending"
            await self._session_manager.discard_if_unhealthy(
                session,
                exc,
                is_fatal_error=self._is_fatal_session_error,
            )
            snap = await _debug_screenshot(page, "EXCEPTION", tag=type(exc).__name__)
            main_locator = None
            if page is not None and hasattr(page, "locator"):
                try:
                    main_locator = page.locator("main")
                except Exception:
                    main_locator = None
            html = await _debug_dump_outer_html(main_locator, "EXCEPTION", tag="main")
            if snap:
                payload["debug_screenshot"] = snap
            if html:
                payload["debug_main_html"] = html
            artifacts = await _capture_failure_artifacts(page, "EXCEPTION", detail="send_not_confirmed", payload=payload)
            if artifacts:
                payload["failure_artifacts"] = artifacts
                payload["diagnostic_ref"] = artifacts.get("meta") or artifacts.get("screenshot")
            _dm_log("EXCEPTION", url=page.url if page else "", error=repr(exc), screenshot=snap, main_html=html)
            detail = "send_not_confirmed"
            if payload.get("reason_code") == "SESSION_OPEN_FAILED":
                detail = "session_open_failed"
            elif payload.get("reason_code") == "THREAD_OPEN_FAILED":
                detail = "thread_open_failed"
            elif payload.get("reason_code") == "SEND_FAILED":
                detail = "send_failed"
                _stage(
                    "send_fail",
                    "failed",
                    detail=detail,
                    stage_hint=str(payload.get("flow_stage") or ""),
                    error=str(exc) or type(exc).__name__,
                )
            return self._result(False, detail, payload, return_detail=return_detail, return_payload=return_payload)

        finally:
            if network_waiter is not None and not network_waiter.done():
                network_waiter.cancel()
                try:
                    await network_waiter
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            current_url = ""
            try:
                current_url = page.url if page else ""
            except Exception:
                current_url = ""

            await self._session_manager.finalize_session(session, current_url=current_url)
            self._active_flow_hook = None

            if debug_token is not None:
                try:
                    _DM_CTX.reset(debug_token)
                except Exception:
                    pass










