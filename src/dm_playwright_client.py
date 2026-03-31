from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, List, Optional
from urllib.parse import urlencode

from ui import Fore, style_text

try:  # pragma: no cover - optional dependency guard
    from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover
    Page = object  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore

from src.playwright_service import (
    BASE_PROFILES,
    DEFAULT_LOCALE,
    DEFAULT_TIMEZONE,
    DEFAULT_USER_AGENT,
    build_launch_args,
    context_viewport_kwargs,
    resolve_playwright_executable,
)
from src.auth.persistent_login import ensure_logged_in_async
from src.browser_profile_paths import browser_storage_state_path
from src.browser_telemetry import log_browser_stage
from core.account_limits import can_send_message_for_account
from core.storage_atomic import atomic_write_json, atomic_write_text, load_json_file
from paths import logs_root, storage_root
from src.runtime.playwright_runtime import (
    launch_sync_browser,
    mark_sync_runtime_context_closed,
    mark_sync_runtime_context_open,
    register_sync_runtime_owner,
    safe_runtime_stop,
    start_sync_playwright,
)
from src.transport.session_manager import SessionManager, SyncSessionRuntime

logger = logging.getLogger(__name__)

INBOX_URL = "https://www.instagram.com/direct/inbox/"
THREAD_URL_TEMPLATE = "https://www.instagram.com/direct/t/{thread_id}/"
DM_DEBUG_DIRNAME = "dm_debug"

VERIFY_TIMEOUT_S = float(os.getenv("HUMAN_DM_VERIFY_TIMEOUT", "10.0"))


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "si", "on"}


_DM_VERBOSE_PROBES = _env_enabled("AUTORESPONDER_DM_VERBOSE_PROBES", False)

# Selectores mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimos necesarios (eliminadas constantes legacy de anchors)
ROW_SELECTOR = "div[role='button'][tabindex='0']"
_MESSAGE_CONTAINER_SELECTORS = (
    "main",
    "div[role='main']",
)
_MESSAGE_NODE_SELECTORS = (
    "[data-testid='message-bubble']",
    "div[role='row']",
    "div[role='none']",
    "div[dir='auto']",
)
THREAD_ROW_SELECTOR = "div[role='main'] a[href*='/direct/t/']"
_COMPOSER_SELECTORS = (
    "div[role='main'] div[role='textbox'][contenteditable='true']",
    "div[role='main'] div[contenteditable='true'][role='textbox']",
    "div[role='main'] textarea",
    "div[role='textbox'][contenteditable='true']",
    "div[contenteditable='true']",
)
_SEND_BUTTON_SELECTORS = (
    "div[role='main'] button:has-text('Enviar')",
    "div[role='main'] div[role='button']:has-text('Enviar')",
    "button:has-text('Enviar')",
    "div[role='button']:has-text('Enviar')",
    "button[aria-label='Enviar']",
    "button[aria-label='Send']",
    "div[role='button'][aria-label='Enviar']",
    "div[role='button'][aria-label='Send']",
)

_UNREAD_HINTS = ("unread", "sin leer", "no leido", "no leido")
_NOTE_PHRASES = (
    "tu nota",
    "your note",
    "share a note",
    "compartir una nota",
    "agrega una nota",
    "agregar una nota",
    "create note",
    "create a note",
    "primera nota",
)


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 10_000) -> int:
    raw = os.getenv(name)
    if raw is None:
        return max(min_value, min(max_value, int(default)))
    try:
        value = int(float(str(raw).strip()))
    except Exception:
        value = int(default)
    return max(min_value, min(max_value, value))


_DM_SCROLL_WAIT_MS = _env_int("AUTORESPONDER_DM_SCROLL_WAIT_MS", 90, min_value=50, max_value=2_500)
_DM_SCROLL_ATTEMPTS = _env_int("AUTORESPONDER_DM_SCROLL_ATTEMPTS", 2, min_value=1, max_value=12)
_DM_MESSAGE_HYDRATION_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_MESSAGE_HYDRATION_TIMEOUT_MS",
    500,
    min_value=200,
    max_value=5_000,
)
_DM_RETURN_INBOX_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_RETURN_INBOX_TIMEOUT_MS",
    650,
    min_value=300,
    max_value=8_000,
)
_DM_STAGNANT_BASE_LIMIT = _env_int(
    "AUTORESPONDER_DM_STAGNANT_BASE_LIMIT",
    12,
    min_value=8,
    max_value=200,
)
_DM_STAGNANT_MAX_LIMIT = _env_int(
    "AUTORESPONDER_DM_STAGNANT_MAX_LIMIT",
    40,
    min_value=_DM_STAGNANT_BASE_LIMIT,
    max_value=500,
)
_DM_API_WAIT_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_API_WAIT_TIMEOUT_MS",
    700,
    min_value=250,
    max_value=8_000,
)
_DM_THREAD_VISUAL_SYNC_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_THREAD_VISUAL_SYNC_TIMEOUT_MS",
    6_000,
    min_value=1_000,
    max_value=20_000,
)
_DM_THREAD_NETWORK_SYNC_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_THREAD_NETWORK_SYNC_TIMEOUT_MS",
    4_500,
    min_value=1_000,
    max_value=20_000,
)
_DM_INBOX_FAST_GOTO_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_INBOX_FAST_GOTO_TIMEOUT_MS",
    9_000,
    min_value=3_000,
    max_value=20_000,
)
_DM_API_CACHE_MAX_PER_THREAD = _env_int(
    "AUTORESPONDER_DM_API_CACHE_MAX_PER_THREAD",
    120,
    min_value=20,
    max_value=2_000,
)
_DM_API_CACHE_MAX_THREADS = _env_int(
    "AUTORESPONDER_DM_API_CACHE_MAX_THREADS",
    1_500,
    min_value=100,
    max_value=20_000,
)
_DM_AUDIO_VERIFY_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_DM_AUDIO_VERIFY_TIMEOUT_MS",
    12_000,
    min_value=2_000,
    max_value=90_000,
)
_DM_AUDIO_VERIFY_POLL_MS = _env_int(
    "AUTORESPONDER_DM_AUDIO_VERIFY_POLL_MS",
    140,
    min_value=60,
    max_value=1_500,
)


def _status_check_timeout_ms() -> int:
    raw = os.getenv("ACCOUNT_STATUS_CHECK_TIMEOUT_MS", "1500")
    try:
        return max(250, int(float(raw)))
    except Exception:
        return 1500


def _status_log_path() -> Path:
    explicit = (os.getenv("ACCOUNT_STATUS_FILE") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    app_root = (os.getenv("APP_DATA_ROOT") or "").strip()
    if app_root:
        return storage_root(Path(app_root).expanduser()) / "accounts_status.json"
    return storage_root(Path(__file__).resolve().parents[1]) / "accounts_status.json"


def log_account_status(username: str, status: str) -> None:
    user = str(username or "").strip().lstrip("@")
    current = str(status or "NO ACTIVA").strip().upper() or "NO ACTIVA"
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    logger.info("PlaywrightDM account_status account=@%s status=%s", user, current)
    if not user:
        return
    try:
        path = _status_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, dict[str, str]] = {}
        if path.exists():
            try:
                loaded = load_json_file(path, {}, label="dm_playwright.account_status")
                if isinstance(loaded, dict):
                    payload = loaded
            except Exception:
                payload = {}
        payload[user] = {
            "username": user,
            "status": current,
            "last_checked": now_utc,
        }
        atomic_write_json(path, payload)
    except Exception:
        return


def _detect_account_status_impl(page: Page) -> str:
    try:
        from src.health_playwright import detect_account_health_sync

        state, _reason = detect_account_health_sync(
            page,
            timeout_ms=_status_check_timeout_ms(),
        )
        return str(state or "").strip().upper() or "NO ACTIVA"
    except Exception:
        return "NO ACTIVA"


async def detect_account_status(page: Page) -> str:
    return _detect_account_status_impl(page)


def detect_account_status_sync(page: Page) -> str:
    return _detect_account_status_impl(page)


@dataclass
class UserLike:
    pk: str
    id: str
    username: str


@dataclass
class ThreadLike:
    id: str
    pk: str
    users: List[UserLike]
    unread_count: int = 0
    link: str = ""
    title: str = ""
    snippet: str = ""
    source_index: int = -1


@dataclass
class MessageLike:
    id: str
    user_id: str
    text: str
    timestamp: Optional[float]
    direction: str = "inbound"  # "inbound", "outbound" or "unknown"


@dataclass(frozen=True)
class _APIMessageRecord:
    thread_id: str
    sender_id: str
    timestamp: float
    item_id: str
    direction: str
    text: str = ""


class PlaywrightDMClient:
    def __init__(
        self,
        *,
        account: Optional[dict] = None,
        headless: bool = False,
        slow_mo_ms: int = 1000,
        dump_on_error: bool = True,
    ) -> None:
        if Page is object:
            raise RuntimeError(
                "Playwright no esta instalado. Ejecuta 'pip install playwright' y luego 'playwright install'."
            )
        self.account = dict(account or {})
        self.username = (self.account.get("username") or "").strip().lstrip("@")
        if not self.username:
            raise RuntimeError("Cuenta invalida: falta username para PlaywrightDMClient.")

        self.user_id = self.username
        self.headless = bool(headless)
        self.slow_mo_ms = max(0, int(slow_mo_ms or 0))
        self.dump_on_error = bool(dump_on_error)
        self._session_manager = SessionManager(
            headless=self.headless,
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
            account=self.account,
            session_manager=self._session_manager,
            login_func=ensure_logged_in_async,
            proxy_resolver=_proxy_from_account,
            open_timeout_seconds=max(60.0, float(os.getenv("AUTORESPONDER_DM_SESSION_OPEN_TIMEOUT_SECONDS", "150"))),
        )

        self._playwright = None
        self._browser = None
        self._context = None
        self._page: Optional[Page] = None
        self._sync_runtime_id = ""
        self._current_thread_id: Optional[str] = None
        self._thread_cache: dict[str, ThreadLike] = {}
        self._thread_cache_meta: dict[str, dict] = {}
        self._api_messages_by_thread: dict[str, dict[str, _APIMessageRecord]] = {}
        self._api_thread_last_seen: dict[str, float] = {}
        self._thread_id_aliases: dict[str, set[str]] = {}
        self._response_listener_registered = False
        self._account_status_checked = False
        log_browser_stage(
            component="playwright_dm_client",
            stage="spawn",
            status="started",
            account=self.username,
            headless=self.headless,
            slow_mo_ms=self.slow_mo_ms,
        )
        self._last_thread_discovery_reason = "other"
        self._last_thread_discovery_detail = "-"
        self._last_thread_discovery_count = 0
        self._last_thread_discovery_target = 0
        self._last_open_thread_diag: dict[str, object] = {}

    @staticmethod
    def storage_state_path(username: str) -> Path:
        return browser_storage_state_path(username, profiles_root=BASE_PROFILES)

    def close(self) -> None:
        try:
            with contextlib.suppress(Exception):
                self._runtime.shutdown(timeout=10.0)
        finally:
            self._playwright = None
            self._browser = None
            self._context = None
            self._page = None
            self._sync_runtime_id = ""
            self._current_thread_id = None
            self._api_messages_by_thread = {}
            self._api_thread_last_seen = {}
            self._thread_id_aliases = {}
            self._response_listener_registered = False
            self._account_status_checked = False
            self._last_thread_discovery_reason = "other"
            self._last_thread_discovery_detail = "closed"
            self._last_thread_discovery_count = 0
            self._last_thread_discovery_target = 0
            self._last_open_thread_diag = {}

    def get_my_username(self) -> str:
        return self.username

    def user_info(self, user_id: object) -> object:
        class _Info:
            def __init__(self, username: str) -> None:
                self.username = username

        if isinstance(user_id, str):
            return _Info(user_id)
        return _Info(str(user_id))

    def ensure_ready(self) -> None:
        self._ensure_page()
        self._ensure_inbox_workspace_fast()

    def fetch_inbox_threads_page(
        self,
        *,
        cursor: str = "",
        limit: int = 20,
        message_limit: int = 20,
        request_timeout_ms: int = 4500,
        total_timeout_ms: int = 9000,
        include_visible_href_resolution: bool = True,
        ingest_payload_cache: bool = False,
    ) -> dict[str, Any]:
        del cursor
        del limit
        del message_limit
        del request_timeout_ms
        del total_timeout_ms
        del include_visible_href_resolution
        del ingest_payload_cache
        raise RuntimeError(
            "Inbox thread discovery moved to src.inbox.endpoint_reader.fetch_account_threads_page_from_storage; "
            "PlaywrightDMClient is browser-only."
        )
        self._ensure_inbox_workspace_fast()
        page = self._ensure_page()
        self._register_response_listener()

        safe_limit = max(1, min(200, int(limit or 20)))
        safe_message_limit = max(1, min(80, int(message_limit or 20)))
        safe_request_timeout_ms = max(1000, min(15_000, int(request_timeout_ms or 4500)))
        safe_total_timeout_ms = max(
            safe_request_timeout_ms,
            min(30_000, int(total_timeout_ms or 9000)),
        )
        urls = _build_inbox_endpoint_candidates(
            cursor=str(cursor or "").strip(),
            limit=safe_limit,
            message_limit=safe_message_limit,
        )
        if not urls:
            raise RuntimeError("No hay endpoints disponibles para discovery del inbox.")

        fetch_result = page.evaluate(
            """async ({ urls, requestTimeoutMs, totalTimeoutMs }) => {
                const responseSummary = {
                    ok: false,
                    status: 0,
                    url: "",
                    error: "",
                    payload: null,
                };
                const startedAt = Date.now();
                const isOverTotalBudget = () => {
                    return Number(Date.now() - startedAt) >= Number(totalTimeoutMs || 0);
                };
                const cookieText = String(document.cookie || "");
                const csrfMatch = cookieText.match(/(?:^|;\\s*)csrftoken=([^;]+)/i);
                const csrfToken = csrfMatch ? decodeURIComponent(csrfMatch[1] || "") : "";
                const headers = {
                    "accept": "application/json, text/plain, */*",
                    "x-requested-with": "XMLHttpRequest",
                    "x-ig-app-id": "936619743392459",
                };
                if (csrfToken) {
                    headers["x-csrftoken"] = csrfToken;
                }
                for (const endpoint of (Array.isArray(urls) ? urls : [])) {
                    if (isOverTotalBudget()) {
                        responseSummary.error = responseSummary.error || `timeout_total_${Number(totalTimeoutMs || 0)}ms`;
                        break;
                    }
                    let timeoutHandle = null;
                    try {
                        const controller = new AbortController();
                        timeoutHandle = setTimeout(() => controller.abort(), Number(requestTimeoutMs || 0));
                        const res = await fetch(endpoint, {
                            method: "GET",
                            credentials: "include",
                            headers,
                            signal: controller.signal,
                        });
                        const status = Number(res.status || 0);
                        const bodyText = await res.text();
                        let parsed = null;
                        try {
                            parsed = JSON.parse(bodyText);
                        } catch (_) {
                            parsed = null;
                        }
                        if (parsed && typeof parsed === "object") {
                            const hasThreads =
                                Array.isArray(parsed?.inbox?.threads)
                                || Array.isArray(parsed?.threads)
                                || Array.isArray(parsed?.data?.inbox?.threads)
                                || String(bodyText || "").indexOf("thread_id") >= 0;
                            if (res.ok || hasThreads) {
                                return {
                                    ok: true,
                                    status,
                                    url: String(endpoint || ""),
                                    error: "",
                                    payload: parsed,
                                };
                            }
                        }
                        if (!responseSummary.error) {
                            responseSummary.error = `status=${status}`;
                        }
                        responseSummary.status = status;
                        responseSummary.url = String(endpoint || "");
                    } catch (err) {
                        const errName = String((err && err.name) || "");
                        if (errName.toLowerCase() === "aborterror") {
                            responseSummary.error = `timeout_fetch_${Number(requestTimeoutMs || 0)}ms`;
                        } else {
                            responseSummary.error = String(err || "fetch_error");
                        }
                        responseSummary.url = String(endpoint || "");
                    } finally {
                        if (timeoutHandle) {
                            clearTimeout(timeoutHandle);
                        }
                    }
                }
                return responseSummary;
            }""",
            {
                "urls": urls,
                "requestTimeoutMs": safe_request_timeout_ms,
                "totalTimeoutMs": safe_total_timeout_ms,
            },
        )
        if not isinstance(fetch_result, dict):
            raise RuntimeError("Respuesta invalida del endpoint de inbox (tipo inesperado).")
        if not bool(fetch_result.get("ok")):
            raise RuntimeError(
                f"Endpoint inbox sin respuesta valida para @{self.username}: "
                f"{fetch_result.get('error') or 'unknown_error'}"
            )

        payload = fetch_result.get("payload")
        source_url = str(fetch_result.get("url") or "")
        status_code = int(fetch_result.get("status") or 0)
        if not isinstance(payload, (dict, list)):
            raise RuntimeError(
                f"Payload de inbox invalido para @{self.username} (status={status_code}, url={source_url})."
            )

        if ingest_payload_cache:
            try:
                self._ingest_api_payload(payload, source_url=source_url)
            except Exception:
                pass

        snapshots = _extract_inbox_threads_from_payload(
            payload,
            self_user_id=str(self.user_id or ""),
            self_username=self.username,
            message_limit=safe_message_limit,
            thread_limit=safe_limit,
        )
        visible_hrefs = {}
        should_resolve_hrefs = bool(include_visible_href_resolution)
        if should_resolve_hrefs:
            should_resolve_hrefs = any(
                (not _normalize_direct_link(str(snap.get("thread_href") or "").strip()))
                or (not _is_probably_web_thread_id(str(snap.get("thread_id_real") or "").strip()))
                for snap in snapshots
                if isinstance(snap, dict)
            )
        if should_resolve_hrefs:
            try:
                visible_hrefs = self._collect_visible_inbox_thread_hrefs(
                    limit=max(10, min(40, safe_limit * 2))
                )
            except Exception:
                visible_hrefs = {}
        for snapshot in snapshots:
            recipient_username = str(snapshot.get("recipient_username") or "").strip()
            title = str(snapshot.get("title") or "").strip()
            thread_href = _normalize_direct_link(str(snapshot.get("thread_href") or "").strip())
            thread_id_real = str(snapshot.get("thread_id_real") or "").strip()
            api_thread_id = str(snapshot.get("thread_id_api") or snapshot.get("thread_id") or "").strip()
            if (not thread_id_real or not _is_probably_web_thread_id(thread_id_real)) and thread_href:
                extracted_href_id = _extract_thread_id(thread_href)
                if _is_probably_web_thread_id(extracted_href_id):
                    thread_id_real = extracted_href_id
            if not thread_href:
                for lookup_key in (recipient_username, title):
                    norm_key = _normalize_key_source(str(lookup_key or ""))
                    if not norm_key:
                        continue
                    match = visible_hrefs.get(norm_key) or {}
                    href_candidate = _normalize_direct_link(str(match.get("thread_href") or ""))
                    href_id = _extract_thread_id(href_candidate)
                    if href_candidate and _is_probably_web_thread_id(href_id):
                        thread_href = href_candidate
                        if not thread_id_real or not _is_probably_web_thread_id(thread_id_real):
                            thread_id_real = href_id
                        break
            if (not thread_id_real or not _is_probably_web_thread_id(thread_id_real)) and _is_probably_web_thread_id(api_thread_id):
                thread_id_real = api_thread_id
            if not thread_href and thread_id_real and _is_probably_web_thread_id(thread_id_real):
                thread_href = THREAD_URL_TEMPLATE.format(thread_id=thread_id_real)

            thread_id = str(thread_id_real or api_thread_id).strip()
            if not thread_id:
                continue
            snapshot["thread_id"] = thread_id
            snapshot["thread_id_real"] = thread_id_real or thread_id
            snapshot["thread_href"] = thread_href
            snapshot["thread_id_api"] = api_thread_id
            recipient_id = str(snapshot.get("recipient_id") or "").strip() or recipient_username or thread_id
            title = title or recipient_username or "unknown"
            snippet = str(snapshot.get("snippet") or "").strip()
            unread_count = snapshot.get("unread_count", 0)
            try:
                unread_int = max(0, int(unread_count))
            except Exception:
                unread_int = 0
            link = thread_href or THREAD_URL_TEMPLATE.format(thread_id=thread_id)
            logger.info(
                "PlaywrightDM discovery_thread_identity account=@%s thread_id_real=%s href=%s username=%s api_thread_id=%s",
                self.username,
                snapshot.get("thread_id_real") or "",
                snapshot.get("thread_href") or "",
                recipient_username or title or "unknown",
                api_thread_id,
            )
            thread = ThreadLike(
                id=thread_id,
                pk=thread_id,
                users=[UserLike(pk=recipient_id, id=recipient_id, username=recipient_username or recipient_id)],
                unread_count=unread_int,
                link=link,
                title=title,
                snippet=snippet,
                source_index=-1,
            )
            self._thread_cache[thread_id] = thread
            self._thread_cache_meta[thread_id] = {
                "title": title,
                "snippet": snippet,
                "link": link,
                "idx": -1,
                "selector": "endpoint_api",
                "key_source": "endpoint_api",
            }

        next_cursor, has_more = _extract_inbox_cursor(payload)
        return {
            "threads": snapshots,
            "cursor": next_cursor,
            "has_more": bool(has_more),
            "source_url": source_url,
            "status": status_code,
        }

    def list_threads(self, amount: int = 20, filter_unread: bool = False) -> List[ThreadLike]:
        for _attempt in range(3):
            threads = list(self.iter_threads(amount=amount, filter_unread=filter_unread))
            if threads:
                return threads
            page = self._ensure_page()
            self._open_inbox()
            try:
                page.wait_for_timeout(300)
            except Exception:
                pass
        return []

    def collect_threads(
        self,
        amount: int = 20,
        filter_unread: bool = False,
    ) -> tuple[List[ThreadLike], str, str]:
        target = max(1, int(amount or 1))
        max_attempts = 3
        aggregated: List[ThreadLike] = []
        seen_keys: set[str] = set()
        reason = "other"
        detail = "-"

        def _dedup_key(thread: ThreadLike) -> str:
            thread_id = str(getattr(thread, "id", "") or "").strip()
            if thread_id and not thread_id.startswith("stable_"):
                return f"id:{thread_id}"
            link_thread_id = _extract_thread_id(str(getattr(thread, "link", "") or ""))
            if link_thread_id:
                return f"id:{link_thread_id}"
            title_key = _normalize_key_source(str(getattr(thread, "title", "") or ""))
            snippet_key = _normalize_key_source(str(getattr(thread, "snippet", "") or ""))
            if title_key or snippet_key:
                return f"stable:{title_key}|{snippet_key}"
            if thread_id:
                return f"id:{thread_id}"
            return ""

        for attempt in range(1, max_attempts + 1):
            try:
                discovered = list(self.iter_threads(amount=target, filter_unread=filter_unread))
                reason = str(getattr(self, "_last_thread_discovery_reason", "") or "").strip() or "other"
                detail = str(getattr(self, "_last_thread_discovery_detail", "") or "").strip() or "-"
            except Exception as exc:
                reason = "exception"
                detail = type(exc).__name__
                logger.warning(
                    "PlaywrightDM collect_threads_error account=@%s target=%s attempt=%s error=%s",
                    self.username,
                    target,
                    attempt,
                    exc,
                )
                break

            for thread in discovered:
                key = _dedup_key(thread)
                if key and key in seen_keys:
                    continue
                if key:
                    seen_keys.add(key)
                aggregated.append(thread)
                if len(aggregated) >= target:
                    break

            if len(aggregated) >= target:
                reason = "target_reached"
                detail = f"attempt={attempt}"
                break

            retryable_reason = reason in {"rows_none", "stagnant_layout", "timeout", "other"}
            if not retryable_reason or attempt >= max_attempts:
                break
            try:
                self._open_inbox()
                page = self._ensure_page()
                page.wait_for_timeout(250)
            except Exception:
                pass

        if reason == "other":
            reason = "target_reached" if len(aggregated) >= target else "no_more_threads"
        self._last_thread_discovery_reason = reason
        self._last_thread_discovery_detail = detail
        self._last_thread_discovery_count = len(aggregated)
        self._last_thread_discovery_target = target
        return aggregated[:target], reason, detail

    def iter_threads(self, amount: int = 20, filter_unread: bool = False) -> Iterator[ThreadLike]:
        """
        Discovery incremental de inbox.
        Produce threads a medida que se descubren para evitar bloqueos largos
        antes de arrancar el procesamiento del primer hilo.
        """
        page = self._ensure_page()
        self._open_inbox()

        target = max(1, int(amount or 1))
        selector_candidates = self._row_selector_candidates()
        seen_thread_keys: set[str] = set()
        yielded = 0
        discovery_reason = "other"
        discovery_detail = "-"

        def _set_discovery_exit(reason: str, detail: str = "-") -> None:
            nonlocal discovery_reason, discovery_detail
            discovery_reason = str(reason or "other")
            discovery_detail = str(detail or "-")

        rows = None
        selected_selector = ""
        inbox_panel = page

        def _resolve_rows() -> tuple[object | None, str]:
            for selector in selector_candidates:
                try:
                    candidate = page.locator(selector)
                    total = candidate.count()
                    if _DM_VERBOSE_PROBES:
                        print(style_text(f"[Probe] Selector '{selector}' -> count={total}", color=Fore.WHITE))
                    if total > 0:
                        return candidate, selector
                except Exception:
                    continue
            return None, ""

        def _rows_count(current_rows) -> int:
            if current_rows is None:
                return 0
            try:
                return max(0, int(current_rows.count()))
            except Exception:
                return 0

        def _log_scroll_check(prev_height: float, new_height: float, rows_count: int) -> None:
            if not _DM_VERBOSE_PROBES:
                return
            print(
                style_text(
                    f"SCROLL_CHECK prev_height={int(prev_height)} new_height={int(new_height)} rows={int(rows_count)}",
                    color=Fore.WHITE,
                )
            )

        def _apply_scroll_pattern(current_panel, current_rows, pattern: str) -> tuple[object, bool, bool]:
            prev_metrics = self._panel_scroll_metrics(current_panel)
            prev_top = float(prev_metrics.get("top", 0))
            prev_height = float(prev_metrics.get("height", 0))
            prev_rows = _rows_count(current_rows)

            try:
                if pattern == "element_end":
                    if prev_rows > 0 and current_rows is not None:
                        last_row = current_rows.nth(prev_rows - 1)
                        try:
                            last_row.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        try:
                            last_row.evaluate(
                                """(el) => {
                                    if (!el || !el.scrollIntoView) return;
                                    try { el.scrollIntoView({ block: "end", inline: "nearest" }); } catch (_) {}
                                }"""
                            )
                        except Exception:
                            pass
                    else:
                        current_panel.evaluate(
                            """(el) => {
                                const node = el || document.scrollingElement || document.documentElement || document.body || null;
                                if (!node) return;
                                const step = Math.max(300, Math.floor(Number(node.clientHeight || 600) * 0.9));
                                try { node.scrollTop = Number(node.scrollTop || 0) + step; } catch (_) {}
                            }"""
                        )
                elif pattern == "bottom_abs":
                    current_panel.evaluate(
                        """(el) => {
                            const node = el || document.scrollingElement || document.documentElement || document.body || null;
                            if (!node) return;
                            try { node.scrollTop = Number(node.scrollHeight || 0); } catch (_) {}
                        }"""
                    )
                elif pattern == "bounce":
                    current_panel.evaluate(
                        """(el) => {
                            const node = el || document.scrollingElement || document.documentElement || document.body || null;
                            if (!node) return;
                            const height = Number(node.scrollHeight || 0);
                            const client = Number(node.clientHeight || 0);
                            const max = Math.max(0, height - client);
                            const current = Number(node.scrollTop || 0);
                            const up = Math.max(0, current - 200);
                            try { node.scrollTop = up; } catch (_) {}
                            try { node.scrollTop = max; } catch (_) {}
                        }"""
                    )
                elif pattern == "wheel":
                    dispatched = False
                    try:
                        dispatched = bool(
                            current_panel.evaluate(
                                """(el) => {
                                    const node = el || document.scrollingElement || document.documentElement || document.body || null;
                                    if (!node) return false;
                                    try {
                                        const evt = new WheelEvent("wheel", { deltaY: 1500, bubbles: true, cancelable: true });
                                        return !!node.dispatchEvent(evt);
                                    } catch (_) {
                                        return false;
                                    }
                                }"""
                            )
                        )
                    except Exception:
                        dispatched = False
                    if not dispatched:
                        try:
                            box = current_panel.bounding_box()
                        except Exception:
                            box = None
                        if box:
                            x = float(box.get("x") or 0.0) + max(16.0, min(float(box.get("width") or 0.0) - 16.0, 56.0))
                            y = float(box.get("y") or 0.0) + max(16.0, min(float(box.get("height") or 0.0) - 16.0, 56.0))
                            try:
                                page.mouse.move(x, y)
                            except Exception:
                                pass
                        try:
                            page.mouse.wheel(0, 1600)
                        except Exception:
                            pass
            except Exception:
                pass

            self._wait_for_scroll_settle(page, extra_ms=80)
            try:
                page.wait_for_timeout(140)
            except Exception:
                pass

            new_metrics = self._panel_scroll_metrics(current_panel)
            new_top = float(new_metrics.get("top", 0))
            new_height = float(new_metrics.get("height", 0))
            new_rows = _rows_count(current_rows)
            _log_scroll_check(prev_height, new_height, new_rows)

            grew = (new_height > prev_height + 1) or (new_rows > prev_rows)
            moved = new_top > prev_top + 1
            return current_panel, grew, moved

        try:
            rows, selected_selector = _resolve_rows()
            if rows is None:
                deadline = time.time() + 8.0
                while rows is None and time.time() < deadline:
                    try:
                        page.wait_for_timeout(200)
                    except Exception:
                        break
                    rows, selected_selector = _resolve_rows()
                if rows is None:
                    _set_discovery_exit("rows_none", "initial_rows_not_found")
                    return

            inbox_panel, _method, _selector, _panel_meta = self._get_inbox_panel(page, rows=rows)
            self._scroll_panel_to_top(inbox_panel)

            if target >= 300:
                max_scroll_passes = max(60, min(1600, target * 4))
            else:
                max_scroll_passes = max(18, min(80, target + 20))
            stagnant_limit = self._stagnation_limit(target)
            if target >= 250:
                stagnant_limit = max(stagnant_limit, min(90, 24 + (target // 12)))
            stagnant_passes = 0
            confirmed_exhausted_passes = 0
            confirmed_exhausted_limit = 2

            for _pass in range(max_scroll_passes):
                if yielded >= target:
                    _set_discovery_exit("target_reached", "target_hit")
                    break

                current_url = page.url or ""
                if "/direct/inbox/" not in current_url:
                    if "/direct/t/" in current_url and self._has_thread_rows_visible(page):
                        pass
                    else:
                        self.return_to_inbox()
                        current_url = page.url or ""
                        if "/direct/inbox/" not in current_url:
                            self._open_inbox()
                if not self._has_thread_rows_visible(page):
                    self._open_inbox()
                    rows, selected_selector = _resolve_rows()
                    if rows is None:
                        _set_discovery_exit("rows_none", "rows_missing_after_reopen")
                        break
                    inbox_panel, _method, _selector, _panel_meta = self._get_inbox_panel(page, rows=rows)

                if rows is None:
                    rows, selected_selector = _resolve_rows()
                    if rows is None:
                        _set_discovery_exit("rows_none", "rows_missing_during_scan")
                        break
                    inbox_panel, _method, _selector, _panel_meta = self._get_inbox_panel(page, rows=rows)

                before_count = yielded
                try:
                    total = rows.count()
                except Exception:
                    rows, selected_selector = _resolve_rows()
                    if rows is None:
                        _set_discovery_exit("rows_none", "rows_missing_after_count_error")
                        break
                    try:
                        total = rows.count()
                    except Exception:
                        total = 0

                for idx in range(total):
                    if yielded >= target:
                        _set_discovery_exit("target_reached", "target_hit")
                        break

                    row = rows.nth(idx)
                    try:
                        row_valid = self._row_is_valid(row, selector=selected_selector, fast=True)
                    except TypeError:
                        row_valid = self._row_is_valid(row, selector=selected_selector)
                    if not row_valid:
                        continue

                    if filter_unread and _thread_unread_count(row) <= 0:
                        continue

                    lines = self._row_lines(row)
                    title = (lines[0] if lines else "unknown").strip()
                    if not title:
                        continue
                    snippet_parts = [(line or "").strip() for line in lines[1:4] if (line or "").strip()]
                    snippet = " | ".join(snippet_parts)
                    thread_key, key_source, _direct_link = self._resolve_thread_key(
                        page,
                        row,
                        title=title,
                        peer_username=title,
                        snippet=snippet,
                    )
                    if not thread_key:
                        fallback_seed = f"{self.username}|{title}|{snippet}|{idx}"
                        fallback_hash = hashlib.sha1(
                            fallback_seed.encode("utf-8", errors="ignore")
                        ).hexdigest()[:16]
                        thread_key = f"stable_{fallback_hash}"
                        key_source = "scan_fallback"
                    if thread_key in seen_thread_keys:
                        continue
                    seen_thread_keys.add(thread_key)

                    thread = ThreadLike(
                        id=thread_key,
                        pk=thread_key,
                        users=[UserLike(pk=title, id=title, username=title)],
                        link=_direct_link,
                        title=title,
                        snippet=snippet,
                        # El indice deja de ser confiable cuando hay scroll/virtualizacion;
                        # forzamos apertura por cache/titulo para evitar clicks en fila incorrecta.
                        source_index=idx,
                    )
                    self._thread_cache[thread_key] = thread
                    self._thread_cache_meta[thread_key] = {
                        "title": title,
                        "snippet": snippet,
                        "link": _direct_link,
                        "idx": idx,
                        "selector": selected_selector,
                        "key_source": key_source,
                    }
                    yielded += 1
                    yield thread

                if yielded >= target:
                    _set_discovery_exit("target_reached", "target_hit")
                    break

                added = yielded - before_count
                before_scroll = self._panel_scroll_metrics(inbox_panel)
                base_height = float(before_scroll.get("height", 0))
                base_rows = _rows_count(rows)

                grew_content = False
                moved_position = False
                scroll_patterns = ["element_end"]
                if added <= 0:
                    scroll_patterns.extend(["bottom_abs", "bounce", "wheel"])
                for pattern in scroll_patterns:
                    inbox_panel, grew_now, moved_now = _apply_scroll_pattern(inbox_panel, rows, pattern)
                    grew_content = grew_content or grew_now
                    moved_position = moved_position or moved_now
                    if grew_content:
                        break

                after_scroll = self._panel_scroll_metrics(inbox_panel)
                final_height = float(after_scroll.get("height", 0))
                final_rows = _rows_count(rows)
                no_height_growth = final_height <= base_height + 1
                no_rows_growth = final_rows <= base_rows
                exhausted_this_pass = no_height_growth and no_rows_growth

                if exhausted_this_pass and added <= 0:
                    confirmed_exhausted_passes += 1
                else:
                    confirmed_exhausted_passes = 0
                if confirmed_exhausted_passes >= confirmed_exhausted_limit:
                    _set_discovery_exit("no_more_threads", "confirmed_scroll_exhausted")
                    break

                if added > 0:
                    stagnant_passes = 0
                else:
                    stagnant_passes += 1
                if stagnant_passes >= stagnant_limit:
                    _set_discovery_exit("stagnant_layout", f"stagnant_passes={stagnant_passes}")
                    break

            if discovery_reason == "other":
                if yielded >= target:
                    _set_discovery_exit("target_reached", "target_hit")
                else:
                    _set_discovery_exit("timeout", f"max_scroll_passes={max_scroll_passes}")
        except Exception as exc:
            _set_discovery_exit("exception", type(exc).__name__)
            raise
        finally:
            self._last_thread_discovery_reason = discovery_reason
            self._last_thread_discovery_detail = discovery_detail
            self._last_thread_discovery_count = yielded
            self._last_thread_discovery_target = target

        if _DM_VERBOSE_PROBES:
            print(
                style_text(
                    f"[Probe] iter_threads target={target} discovered={yielded} reason={self._last_thread_discovery_reason} detail={self._last_thread_discovery_detail}",
                    color=Fore.WHITE,
                )
            )

    def open_thread(self, thread_id: str) -> bool:
        key = str(thread_id or "").strip()
        if not key:
            logger.error("PlaywrightDM open_thread called with empty thread_id")
            return False
        thread = self._thread_cache.get(key)
        if thread is None:
            logger.error("PlaywrightDM open_thread cache_miss thread_id=%s", key)
            return False
        return self._open_thread(thread)

    def open_thread_by_href(
        self,
        href: str,
        *,
        visual_timeout_ms: Optional[int] = None,
    ) -> bool:
        href_value = _normalize_direct_link(str(href or "").strip())
        thread_id = _extract_thread_id(href_value)
        if not href_value or not thread_id:
            self._set_last_open_thread_diag(
                failed_condition="href_invalid",
                row_stale=False,
            )
            log_browser_stage(
                component="playwright_dm_client",
                stage="thread_open",
                status="failed",
                account=self.username,
                reason="href_invalid",
                href=href,
            )
            logger.error(
                "PlaywrightDM open_thread_by_href_invalid account=@%s href=%s",
                self.username,
                href,
            )
            return False

        from src.inbox.conversation_sync import ensure_thread_page

        page = self._ensure_page()
        timeout_ms = _DM_THREAD_VISUAL_SYNC_TIMEOUT_MS if visual_timeout_ms is None else max(1_000, int(visual_timeout_ms))
        try:
            opened = bool(
                self._runtime.run_async(
                    ensure_thread_page(page, thread_id=thread_id, thread_href=href_value, timeout_ms=timeout_ms),
                    timeout=max(15.0, float(timeout_ms) / 1000.0 + 5.0),
                )
            )
        except Exception as exc:
            self._set_last_open_thread_diag(
                failed_condition="thread_open_error",
                post_url=str(getattr(page, "url", "") or ""),
            )
            log_browser_stage(
                component="playwright_dm_client",
                stage="thread_open",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                href=href_value,
                reason="thread_open_error",
                error=str(exc) or type(exc).__name__,
            )
            return False
        self._last_open_thread_diag = {
            "thread_id": thread_id,
            "post_url": str(getattr(page, "url", "") or ""),
            "failed_condition": "" if opened else "thread_not_opened",
        }
        if not opened:
            log_browser_stage(
                component="playwright_dm_client",
                stage="thread_open",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                href=href_value,
                reason="thread_not_opened",
            )
            return False
        self._current_thread_id = thread_id
        meta = self._thread_cache_meta.get(thread_id, {})
        meta["link"] = href_value
        self._thread_cache_meta[thread_id] = meta
        log_browser_stage(
            component="playwright_dm_client",
            stage="thread_open",
            status="ok",
            account=self.username,
            thread_id=thread_id,
            href=href_value,
            url=str(getattr(page, "url", "") or ""),
        )
        return True

        page = self._ensure_page()
        if not self._is_in_direct_workspace(page):
            self._ensure_inbox_workspace_fast()
            page = self._ensure_page()
        pre_url = page.url or ""
        visual_timeout_value = (
            _DM_THREAD_VISUAL_SYNC_TIMEOUT_MS
            if visual_timeout_ms is None
            else max(1_000, int(visual_timeout_ms))
        )
        try:
            page.goto(href_value, wait_until="domcontentloaded", timeout=45_000)
        except PlaywrightTimeoutError:
            pass
        except Exception as exc:
            self._set_last_open_thread_diag(
                pre_url=pre_url,
                post_url=str(page.url or pre_url),
                was_in_thread=bool("/direct/t/" in str(pre_url or "")),
                visual_wait_ms=0,
                network_wait_ms=0,
                failed_condition="href_goto",
                row_stale=False,
                visual_timeout_ms=visual_timeout_value,
                network_timeout_ms=0,
            )
            log_browser_stage(
                component="playwright_dm_client",
                stage="thread_open",
                status="failed",
                account=self.username,
                reason="href_goto",
                href=href_value,
                error=str(exc) or type(exc).__name__,
            )
            logger.error(
                "PlaywrightDM open_thread_by_href_goto_error account=@%s href=%s error=%s",
                self.username,
                href_value,
                exc,
            )
            return False

        self._dismiss_overlays(page)
        visual_started = time.perf_counter()
        visual_ok, state = self._wait_for_visual_thread_sync(
            page,
            timeout_ms=visual_timeout_value,
            require_both=False,
            stable_hits_required=1,
        )
        visual_wait_ms = int((time.perf_counter() - visual_started) * 1000.0)
        post_url = str(state.get("post_url") or (page.url or ""))
        post_thread_id = str(state.get("post_thread_id") or _extract_thread_id(post_url)).strip()
        resolved_thread_id = post_thread_id or thread_id
        if not visual_ok:
            post_url_now = str(page.url or post_url)
            post_thread_id_now = str(_extract_thread_id(post_url_now) or post_thread_id).strip()
            expected_ids = set(self._expand_thread_ids([thread_id]))
            opened_ids = set(self._expand_thread_ids([post_thread_id_now]))
            composer = self._find_composer(page)
            composer_visible = False
            if composer is not None:
                try:
                    composer_visible = bool(composer.is_visible())
                except Exception:
                    composer_visible = False
            message_panel_visible = self._message_panel_visible(page)
            if expected_ids.intersection(opened_ids) and (composer_visible or message_panel_visible):
                visual_ok = True
                post_url = post_url_now
                post_thread_id = post_thread_id_now
                resolved_thread_id = post_thread_id or thread_id
                logger.warning(
                    "PlaywrightDM open_thread_by_href_soft_accept account=@%s href=%s post_url=%s composer_visible=%s message_panel_visible=%s",
                    self.username,
                    href_value,
                    post_url,
                    composer_visible,
                    message_panel_visible,
                )
            else:
                logger.error(
                    "PlaywrightDM open_thread_by_href_visual_sync_error account=@%s href=%s pre_url=%s post_url=%s",
                    self.username,
                    href_value,
                    pre_url,
                    post_url,
                )
                self._set_last_open_thread_diag(
                    pre_url=pre_url,
                    post_url=post_url,
                    was_in_thread=bool("/direct/t/" in str(pre_url or "")),
                    visual_wait_ms=visual_wait_ms,
                    network_wait_ms=0,
                    failed_condition="href_visual_sync",
                    row_stale=False,
                    visual_timeout_ms=visual_timeout_value,
                    network_timeout_ms=0,
                )
                log_browser_stage(
                    component="playwright_dm_client",
                    stage="thread_open",
                    status="failed",
                    account=self.username,
                    reason="href_visual_sync",
                    href=href_value,
                    post_url=post_url,
                )
                return False

        self._register_thread_aliases(thread_id, resolved_thread_id)
        self._current_thread_id = resolved_thread_id

        existing = self._thread_cache.get(resolved_thread_id) or self._thread_cache.get(thread_id)
        if existing is None:
            existing = ThreadLike(
                id=resolved_thread_id,
                pk=resolved_thread_id,
                users=[],
                unread_count=0,
                link=href_value,
                title="",
                snippet="",
                source_index=-1,
            )
        else:
            existing.link = href_value
            self._sync_thread_id(existing, resolved_thread_id)
        self._thread_cache[resolved_thread_id] = existing
        meta = dict(self._thread_cache_meta.get(resolved_thread_id, {}))
        meta.update(
            {
                "link": href_value,
                "idx": -1,
                "selector": "href_direct",
                "key_source": "href_direct",
            }
        )
        self._thread_cache_meta[resolved_thread_id] = meta
        try:
            self._assert_logged_in(page)
            self._refresh_thread_participants(page, existing)
        except Exception:
            pass
        logger.info(
            "PlaywrightDM open_thread_by_href_ok account=@%s href=%s thread_id=%s",
            self.username,
            href_value,
            resolved_thread_id,
        )
        self._set_last_open_thread_diag(
            pre_url=pre_url,
            post_url=post_url,
            was_in_thread=bool("/direct/t/" in str(pre_url or "")),
            visual_wait_ms=visual_wait_ms,
            network_wait_ms=0,
            failed_condition="ok",
            row_stale=False,
            visual_timeout_ms=visual_timeout_value,
            network_timeout_ms=0,
        )
        log_browser_stage(
            component="playwright_dm_client",
            stage="thread_open",
            status="ok",
            account=self.username,
            thread_id=resolved_thread_id,
            href=href_value,
            post_url=post_url,
        )
        return True

    def _collect_visible_inbox_thread_hrefs(self, *, limit: int = 80) -> dict[str, dict[str, str]]:
        self._ensure_inbox_workspace_fast()
        page = self._ensure_page()
        rows = None
        selected_selector = ""
        for selector in self._row_selector_candidates():
            try:
                candidate = page.locator(selector)
                if candidate.count() > 0:
                    rows = candidate
                    selected_selector = selector
                    break
            except Exception:
                continue
        if rows is None:
            return {}

        try:
            total = rows.count()
        except Exception:
            total = 0
        max_rows = max(0, min(int(limit), int(total)))
        resolved: dict[str, dict[str, str]] = {}
        for idx in range(max_rows):
            row = rows.nth(idx)
            try:
                row_valid = self._row_is_valid(row, selector=selected_selector, fast=True)
            except TypeError:
                row_valid = self._row_is_valid(row, selector=selected_selector)
            except Exception:
                row_valid = True
            if not row_valid:
                continue

            href = ""
            try:
                href = (row.get_attribute("href") or "").strip()
            except Exception:
                href = ""
            if "/direct/t/" not in href:
                try:
                    href = (
                        row.locator("a[href*='/direct/t/']").first.get_attribute("href")
                        or ""
                    ).strip()
                except Exception:
                    href = ""
            href_value = _normalize_direct_link(href)
            thread_id = _extract_thread_id(href_value)
            if not href_value or not thread_id or not _is_probably_web_thread_id(thread_id):
                continue

            row_lines = self._row_lines(row)
            names = []
            if row_lines:
                names.append(str(row_lines[0] or "").strip())
            preview = self._row_preview(row)
            if preview:
                names.append(str(preview).strip())
            names = [name for name in names if name]
            if not names:
                continue
            for name in names:
                key = _normalize_key_source(name)
                if not key or key in resolved:
                    continue
                resolved[key] = {
                    "thread_id_real": thread_id,
                    "thread_href": href_value,
                    "username": name,
                }
        return resolved

    # LEGACY: FunciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n deshabilitada - ya no se usa (reemplazada por click-first scan)
    # def _list_threads_from_anchors(...)

    def _row_lines(self, row) -> List[str]:
        try:
            raw_text = row.inner_text() or ""
        except Exception:
            raw_text = ""
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        if lines:
            return lines
        # Fallback: aria-label del avatar (suele contener username o "Tu nota")
        try:
            aria = (row.get_attribute("aria-label") or "").strip()
        except Exception:
            aria = ""
        if aria:
            return [aria]
        # Fallback: intentar tomar texto del contenedor superior clickeable
        try:
            parent = row.locator("xpath=ancestor::div[@role='button'][1]").first
            parent_text = parent.inner_text() or ""
        except Exception:
            parent_text = ""
        return [line.strip() for line in parent_text.splitlines() if line.strip()]

    def _row_selector_candidates(self) -> List[str]:
        # Prioritize lightweight selectors first; heavy :has(...) selectors go last.
        return [
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']",
            "div[role='navigation'][aria-label='List of conversations'] div[role='button'][tabindex='0']",
            "div[role='navigation'] div[role='button'][tabindex='0']",
            THREAD_ROW_SELECTOR,
            "a[href*='/direct/t/']",
            "div[role='main'] div[role='list'] a[href*='/direct/t/']",
            "div[aria-label='Chats'] a[href*='/direct/t/']",
            "div[aria-label='Mensajes'] a[href*='/direct/t/']",
            "div[aria-label='Messages'] a[href*='/direct/t/']",
            "div[role='main'] div[role='list'] div[role='listitem']",
            "div[role='main'] div[role='list'] div[role='row']",
            "div[role='main'] div[role='list'] div[role='button'][tabindex='0']",
            "div[role='main'] div[role='listitem']",
            "div[role='main'] div[role='row']",
            "div[role='main'] div[role='button'][tabindex='0']",
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='List of conversations'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'][aria-label='List of conversations'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'] div[role='button'][tabindex='0']:has(abbr)",
        ]

    def _get_inbox_panel(self, page: Page, rows=None):
        """
        [Probe/Fix] Intenta encontrar el panel lateral de mensajes.
        Retorna (locator, metodo, selector, meta).
        """
        if rows is not None:
            try:
                if rows.count() > 0:
                    first_row = rows.nth(0)
                    # Prefer locator-based panels anchored by the first row to avoid stale ElementHandle scroll targets.
                    for selector in (
                        "div[role='navigation'][aria-label='Lista de conversaciones']",
                        "div[role='navigation'][aria-label='Conversation list']",
                        "div[role='navigation'][aria-label='List of conversations']",
                        "div[role='navigation']",
                        "main",
                        "div[role='main']",
                    ):
                        try:
                            scoped = page.locator(selector, has=first_row)
                            if scoped.count() <= 0:
                                continue
                            panel_loc = scoped.first
                            try:
                                meta = panel_loc.evaluate(
                                    """(el) => ({
                                        tag: String((el && el.tagName) || ""),
                                        role: String((el && el.getAttribute && el.getAttribute("role")) || ""),
                                        ariaLabel: String((el && el.getAttribute && el.getAttribute("aria-label")) || ""),
                                        clientHeight: Number((el && el.clientHeight) || 0),
                                        scrollHeight: Number((el && el.scrollHeight) || 0)
                                    })"""
                                )
                            except Exception:
                                meta = {}
                            if _DM_VERBOSE_PROBES:
                                print(
                                    style_text(
                                        f"[Probe] _get_inbox_panel resolved by has-row selector '{selector}': {meta}",
                                        color=Fore.WHITE,
                                    )
                                )
                            return panel_loc, "selector_has_row", selector, meta or {}
                        except Exception:
                            continue
                    handle = first_row.evaluate_handle(
                        """(el) => {
                            if (!el) return null;
                            const isScrollable = (node) => {
                                if (!node) return false;
                                const style = window.getComputedStyle(node);
                                const overflowY = String((style && style.overflowY) || "").toLowerCase();
                                const allowsScroll = overflowY.includes("auto") || overflowY.includes("scroll") || overflowY.includes("overlay");
                                const hasScrollableContent = Number(node.scrollHeight || 0) - Number(node.clientHeight || 0) > 4;
                                return allowsScroll && hasScrollableContent;
                            };

                            let node = el;
                            for (let i = 0; node && i < 14; i += 1) {
                                if (isScrollable(node)) return node;
                                node = node.parentElement;
                            }

                            node = el;
                            for (let i = 0; node && i < 14; i += 1) {
                                const hasScrollableContent = Number(node.scrollHeight || 0) - Number(node.clientHeight || 0) > 4;
                                if (hasScrollableContent) return node;
                                node = node.parentElement;
                            }

                            return document.scrollingElement || document.documentElement || document.body || null;
                        }"""
                    )
                    resolved = handle.as_element() if handle is not None else None
                    if resolved is not None:
                        try:
                            meta = resolved.evaluate(
                                """(el) => ({
                                    tag: String((el && el.tagName) || ""),
                                    role: String((el && el.getAttribute && el.getAttribute("role")) || ""),
                                    ariaLabel: String((el && el.getAttribute && el.getAttribute("aria-label")) || ""),
                                    clientHeight: Number((el && el.clientHeight) || 0),
                                    scrollHeight: Number((el && el.scrollHeight) || 0)
                                })"""
                            )
                        except Exception:
                            meta = {}
                        if _DM_VERBOSE_PROBES:
                            print(
                                style_text(
                                    f"[Probe] _get_inbox_panel resolved by row: {meta}",
                                    color=Fore.WHITE,
                                )
                            )
                        return resolved, "row_ancestor", "row_scrollable_ancestor", meta or {}
            except Exception:
                pass

        for selector in (
            "div[role='navigation'][aria-label='Lista de conversaciones']",
            "div[role='navigation'][aria-label='Conversation list']",
            "div[role='navigation']",
            "div[role='main']",
            "main",
        ):
            try:
                loc = page.locator(selector)
                if loc.count() > 0:
                    if _DM_VERBOSE_PROBES:
                        print(style_text(f"[Probe] _get_inbox_panel encontrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ '{selector}'", color=Fore.WHITE))
                    return loc.first, "selector", selector, {"count": loc.count()}
            except Exception:
                continue
        if _DM_VERBOSE_PROBES:
            print(style_text("[Probe] _get_inbox_panel no encontrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ nada, usando page", color=Fore.YELLOW))
        return page, "page", "", {"count": 1}

    def _stagnation_limit(self, target: int) -> int:
        target_value = max(1, int(target or 1))
        scaled = _DM_STAGNANT_BASE_LIMIT + int(max(0, target_value - 25) / 60)
        return max(_DM_STAGNANT_BASE_LIMIT, min(_DM_STAGNANT_MAX_LIMIT, scaled))

    def _wait_for_scroll_settle(self, page: Page, *, extra_ms: int = 0) -> None:
        wait_ms = max(50, int(_DM_SCROLL_WAIT_MS + max(0, int(extra_ms))))
        try:
            page.wait_for_timeout(wait_ms)
        except Exception:
            pass

    def _panel_scroll_metrics(self, panel) -> dict[str, float]:
        try:
            result = panel.evaluate(
                """(el) => {
                    const node = el || document.scrollingElement || document.documentElement || document.body || null;
                    if (!node) return { top: 0, height: 0, client: 0, max: 0 };
                    const top = Number(node.scrollTop || 0);
                    const height = Number(node.scrollHeight || 0);
                    const client = Number(node.clientHeight || 0);
                    const max = Math.max(0, height - client);
                    return { top, height, client, max };
                }"""
            )
        except Exception:
            try:
                page = self._ensure_page()
                result = page.evaluate(
                    """() => {
                        const node = document.scrollingElement || document.documentElement || document.body || null;
                        if (!node) return { top: 0, height: 0, client: 0, max: 0 };
                        const top = Number(node.scrollTop || 0);
                        const height = Number(node.scrollHeight || 0);
                        const client = Number(node.clientHeight || 0);
                        const max = Math.max(0, height - client);
                        return { top, height, client, max };
                    }"""
                )
            except Exception:
                result = {}
        top = float((result or {}).get("top", 0))
        height = float((result or {}).get("height", 0))
        client = float((result or {}).get("client", 0))
        max_scroll = float((result or {}).get("max", max(0.0, height - client)))
        return {"top": top, "height": height, "client": client, "max": max_scroll}

    def _scroll_panel_to_top(self, panel) -> None:
        try:
            panel.evaluate(
                """(el) => {
                    const node = el || document.scrollingElement || document.documentElement || document.body || null;
                    if (!node) return;
                    try { node.scrollTop = 0; } catch (_) {}
                }"""
            )
        except Exception:
            try:
                page = self._ensure_page()
                fallback_panel, _method, _selector, _meta = self._get_inbox_panel(page)
                fallback_panel.evaluate(
                    """(el) => {
                        const node = el || document.scrollingElement || document.documentElement || document.body || null;
                        if (!node) return;
                        try { node.scrollTop = 0; } catch (_) {}
                    }"""
                )
            except Exception:
                return
        self._wait_for_scroll_settle(self._ensure_page(), extra_ms=20)

    def _scroll_panel_down(self, panel, *, max_attempts: Optional[int] = None) -> bool:
        page = self._ensure_page()
        attempts_value = _DM_SCROLL_ATTEMPTS if max_attempts is None else max_attempts
        attempts = max(1, int(attempts_value))

        for attempt in range(attempts):
            before = self._panel_scroll_metrics(panel)
            before_top = float(before.get("top", 0))
            before_height = float(before.get("height", 0))
            before_max = float(before.get("max", 0))

            try:
                factor = 0.90 + min(0.28, attempt * 0.07)
                scroll_script = """(el) => {
                    const factor = __FACTOR__;
                    const node = el || document.scrollingElement || document.documentElement || document.body || null;
                    if (!node) return { before: 0, after: 0, max: 0 };
                    const before = Number(node.scrollTop || 0);
                    const max = Math.max(0, Number((node.scrollHeight || 0) - (node.clientHeight || 0)));
                    const step = Math.max(350, Math.floor(Number(node.clientHeight || 600) * Number(factor || 1)));
                    const next = Math.min(max, before + step);
                    try { node.scrollTop = next; } catch (_) {}
                    const after = Number(node.scrollTop || 0);
                    return { before, after, max };
                }"""
                panel.evaluate(
                    scroll_script.replace("__FACTOR__", f"{factor:.6f}"),
                )
            except Exception:
                try:
                    fallback_panel, _method, _selector, _meta = self._get_inbox_panel(page)
                    panel = fallback_panel
                    panel.evaluate(
                        scroll_script.replace("__FACTOR__", f"{factor:.6f}"),
                    )
                except Exception:
                    pass

            if attempt > 0:
                try:
                    box = panel.bounding_box()
                except Exception:
                    box = None
                if box:
                    x = float(box.get("x") or 0.0) + max(12.0, min(float(box.get("width") or 0.0) - 12.0, 44.0))
                    y = float(box.get("y") or 0.0) + max(12.0, min(float(box.get("height") or 0.0) - 12.0, 44.0))
                    try:
                        page.mouse.move(x, y)
                    except Exception:
                        pass
                try:
                    page.mouse.wheel(0, 1200 + (attempt * 350))
                except Exception:
                    pass
                if attempt >= 2:
                    try:
                        panel.focus()
                    except Exception:
                        pass
                    try:
                        page.keyboard.press("PageDown")
                    except Exception:
                        pass
                    if attempt == attempts - 1:
                        try:
                            page.keyboard.press("End")
                        except Exception:
                            pass

            self._wait_for_scroll_settle(page, extra_ms=30 if attempt > 0 else 0)

            after = self._panel_scroll_metrics(panel)
            after_top = float(after.get("top", 0))
            after_height = float(after.get("height", 0))
            after_max = float(after.get("max", 0))

            top_moved = after_top > before_top + 1
            height_grew = after_height > before_height + 2
            if top_moved or height_grew:
                return True

            max_ref = max(after_max, before_max)
            if after_top + 1 < max_ref:
                continue

        final = self._panel_scroll_metrics(panel)
        return float(final.get("top", 0)) + 1 < float(final.get("max", 0))

    def _row_is_valid(self, row, *, selector: str | None = None, fast: bool = False) -> bool:
        """
        ValidaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nima pre-click.
        El filtrado real ocurre POST-CLICK en _open_thread.
        """
        try:
            lines = self._row_lines(row)
            if not lines:
                return False

            # Filtros de exclusiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n de UI bÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡sica (incluye Notas)
            first_line = lines[0].lower()
            if first_line in {
                "primary",
                "general",
                "request",
                "buscar",
                "search",
                "enviar mensaje",
                "nuevo mensaje",
                "new message",
                "solicitudes",
                "principal",
                "mensajes",
                "chats",
                "direct",
            }:
                return False
            if self._is_non_thread_control_row(lines):
                return False

            if fast:
                # Fast-path para escaneo: evita consultas DOM pesadas por fila.
                compact = " ".join((line or "").strip().lower() for line in lines[:6] if line)
                if compact:
                    if compact in {"notes", "notas", "nota"}:
                        return False
                    for token in _NOTE_PHRASES:
                        if token in compact:
                            return False
                    # Menos estricto: cualquier fila no-nota/no-control con texto real
                    # se procesa y la validacion definitiva ocurre al abrir el thread.
                    first_line = (lines[0] or "").strip().lower() if lines else ""
                    has_chat_like_shape = len(lines) >= 2 or len(first_line) >= 3
                    if has_chat_like_shape:
                        return True
                return False

            note_reason = self._note_reason(row)
            if note_reason:
                logger.info(
                    "PlaywrightDM row_discard reason=note selector=%s token=%s first_line=%s",
                    selector or "-",
                    note_reason,
                    self._row_preview(row),
                )
                return False

            # Descartar botones internos (avatar/nota) por tamaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±o.
            if not self._row_has_thread_dimensions(row):
                logger.info(
                    "PlaywrightDM row_discard reason=small_button selector=%s first_line=%s",
                    selector or "-",
                    self._row_preview(row),
                )
                return False

            # Exigir marcador de thread real para evitar abrir burbujas de nota.
            if not self._row_has_thread_marker(row):
                logger.info(
                    "PlaywrightDM row_discard reason=no_thread_marker selector=%s first_line=%s",
                    selector or "-",
                    self._row_preview(row),
                )
                return False

            return True
        except Exception:
            return False

    def _is_non_thread_control_row(self, lines: List[str]) -> bool:
        if not lines:
            return True
        text = " ".join((line or "").strip().lower() for line in lines if line)
        if not text:
            return True
        control_tokens = (
            "nuevo mensaje",
            "new message",
            "icono de comilla angular",
            "desde el corazÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n",
            "solicitudes",
            "requests",
        )
        return any(token in text for token in control_tokens)

    def _note_reason(self, row) -> str:
        candidates: List[str] = []
        try:
            lines = self._row_lines(row)
        except Exception:
            lines = []
        if lines:
            for line in lines[:6]:
                candidates.append((line or "").lower())
        for attr_name in ("aria-label", "data-testid"):
            try:
                value = (row.get_attribute(attr_name) or "").strip().lower()
            except Exception:
                value = ""
            if value:
                candidates.append(value)
        try:
            parent = row.locator("xpath=ancestor-or-self::*[@aria-label][1]").first
            parent_aria = (parent.get_attribute("aria-label") or "").strip().lower()
        except Exception:
            parent_aria = ""
        if parent_aria:
            candidates.append(parent_aria)

        for text in candidates:
            clean = re.sub(r"\s+", " ", text)
            if clean in {"notes", "notas", "nota"}:
                return clean
            for token in _NOTE_PHRASES:
                if token in clean:
                    return token

        try:
            if row.locator(
                "xpath=ancestor-or-self::*[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'note') or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'nota')]"
            ).count() > 0:
                return "aria_ancestor_note"
        except Exception:
            pass
        return ""

    def _row_preview(self, row) -> str:
        try:
            lines = self._row_lines(row)
        except Exception:
            lines = []
        if lines:
            return lines[0][:120]
        return ""

    def _row_has_thread_dimensions(self, row) -> bool:
        try:
            box = row.bounding_box()
        except Exception:
            box = None
        if not box:
            return False
        width = float(box.get("width") or 0.0)
        height = float(box.get("height") or 0.0)
        return width >= 220.0 and height >= 44.0

    def _row_has_thread_marker(self, row) -> bool:
        """
        Heur?stica para distinguir threads de notas.
        """
        try:
            href = row.get_attribute("href") or ""
        except Exception:
            href = ""
        if "/direct/t/" in href:
            return True

        try:
            if row.locator("a[href*='/direct/t/']").count() > 0:
                return True
        except Exception:
            pass

        # Los threads suelen mostrar tiempo (p.ej. <time>)
        try:
            if row.locator("time").count() > 0:
                return True
        except Exception:
            pass
        try:
            if row.locator("abbr[aria-label]").count() > 0:
                return True
        except Exception:
            pass
        try:
            if row.locator("abbr").count() > 0:
                return True
        except Exception:
            pass

        # Unread badge tambi?n indica thread real
        try:
            if _thread_unread_count(row) > 0:
                return True
        except Exception:
            pass

        # Modern inbox rows may not expose anchors/time tags; detect timestamp-like text.
        try:
            full = " ".join(self._row_lines(row)).lower()
        except Exception:
            full = ""
        if full:
            if re.search(r"\b\d+\s*(s|min|h|d|sem|sec|hr|hrs)\b", full):
                return True
            if any(token in full for token in (" ahora", " now", " ayer", " yesterday")):
                return True
            if ("tu:" in full or "you:" in full) and any(tok in full for tok in ("min", " h", " hr", " ayer", " now", "unread", "sin leer")):
                return True

        return False


    def _count_rows_valid(self, rows) -> int:
        try:
            total = rows.count()
        except Exception:
            total = 0
        valid = 0
        for idx in range(total):
            try:
                if self._row_is_valid(rows.nth(idx)):
                    valid += 1
            except Exception:
                continue
        return valid

    # LEGACY: FunciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n deshabilitada - ya no se usa (reemplazada por click-first scan)
    # def _list_threads_from_rows(...) -> List[ThreadLike]


    def get_messages(self, thread: ThreadLike, amount: int = 20, *, log: bool = True) -> List[MessageLike]:
        page = self._ensure_page()
        del log
        from src.inbox.conversation_sync import read_conversation_async

        payload = self._runtime.run_async(
            read_conversation_async(
                page,
                account=self.account,
                thread_id=str(getattr(thread, "id", "") or "").strip(),
                thread_href=str(getattr(thread, "link", "") or "").strip(),
                message_limit=max(20, min(80, int(amount or 20))),
            ),
            timeout=30.0,
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

        # [CLICK-FIRST] Abrir y verificar thread post-click
        try:
            is_valid = self._open_thread(thread)
            if not is_valid:
                if log:
                    logger.info(
                        "[TRACE_MSG_DIAG] thread=%s open_thread_validated=%s final_collected_len=%d return_pairs_ordered=%s",
                        thread.id,
                        is_valid,
                        0,
                        [],
                    )
                return []
        except Exception as exc:
            if log:
                logger.warning("PlaywrightDM error al abrir thread %s: %s", thread.id, e)
                logger.info(
                    "[TRACE_MSG_DIAG] thread=%s open_thread_exception=%r final_collected_len=%d return_pairs_ordered=%s",
                    thread.id,
                    e,
                    0,
                    [],
                )
            return []

        # Esperar a que los mensajes se hidraten
        try:
            msg_selector = _MESSAGE_NODE_SELECTORS[0]
            page.wait_for_selector(
                f"main {msg_selector}, div[role='main'] {msg_selector}",
                timeout=_DM_MESSAGE_HYDRATION_TIMEOUT_MS,
            )
        except Exception:
            pass

        collected: List[MessageLike] = []
        api_messages = self._get_api_messages_for_thread(
            thread,
            page,
            timeout_ms=_DM_API_WAIT_TIMEOUT_MS,
        )
        if not api_messages:
            if log:
                logger.info(
                    "PlaywrightDM mensajes vacios thread=%s peer=%s account=@%s",
                    thread.id,
                    _thread_peer_id(thread, self.user_id),
                    self.username,
                )
                logger.info(
                    "[TRACE_MSG_DIAG] thread=%s final_collected_len=%d return_pairs_ordered=%s",
                    thread.id,
                    0,
                    [],
                )
            return []

        for idx, api_msg in enumerate(api_messages[: max(1, amount)]):
            timestamp = api_msg.timestamp
            if timestamp is None:
                logger.warning(
                    "timestamp_missing_from_api thread_id=%s item_id=%s sender_id=%s account=@%s source=get_messages",
                    api_msg.thread_id,
                    api_msg.item_id,
                    api_msg.sender_id,
                    self.username,
                )
                continue
            if api_msg.direction == "outbound":
                direction = "outbound"
            elif api_msg.direction == "inbound":
                direction = "inbound"
            else:
                direction = "unknown"
            if direction == "outbound":
                user_id = self.user_id
            else:
                user_id = str(api_msg.sender_id or "").strip() or _thread_peer_id(thread, self.user_id)
            msg_id = str(api_msg.item_id or "").strip()
            if not msg_id:
                continue
            text = str(api_msg.text or "")
            if log:
                preview = text.replace("\n", " ").replace("\r", " ")[:50]
                logger.info(
                    "[TRACE_MSG_DIAG] thread=%s parsed_idx=%d text50=%r timestamp=%s user_id=%s direction=%s",
                    thread.id,
                    idx,
                    preview,
                    timestamp,
                    user_id,
                    direction,
                )
            collected.append(
                MessageLike(
                    id=msg_id,
                    user_id=user_id,
                    text=text,
                    timestamp=timestamp,
                    direction=direction,
                )
            )

        if log:
            pre_sort_pairs = [(m.user_id, m.timestamp) for m in collected]
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s before_sort_count=%d pairs=%s",
                thread.id,
                len(collected),
                pre_sort_pairs,
            )
        collected.sort(key=lambda m: (m.timestamp is not None, m.timestamp or 0), reverse=True)
        if log:
            post_sort_pairs = [(m.user_id, m.timestamp) for m in collected]
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s after_sort_count=%d pairs=%s",
                thread.id,
                len(collected),
                post_sort_pairs,
            )

        last_outbound = next((m for m in collected if m.user_id == self.user_id), None)
        last_inbound = next((m for m in collected if m.user_id != self.user_id), None)
        if log:
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s final_collected_len=%d",
                thread.id,
                len(collected),
            )
            logger.info(
                "PlaywrightDM mensajes_leidos thread=%s peer=%s count=%d last_in_ts=%s last_out_ts=%s",
                thread.id,
                _thread_peer_id(thread, self.user_id),
                len(collected),
                _fmt_ts(last_inbound.timestamp if last_inbound else None),
                _fmt_ts(last_outbound.timestamp if last_outbound else None),
            )
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s return_pairs_ordered=%s",
                thread.id,
                [(m.user_id, m.timestamp) for m in collected],
            )
        return collected

    def _thread_from_id_strict(self, thread_id: str) -> Optional[ThreadLike]:
        thread_id_clean = str(thread_id or "").strip()
        if not thread_id_clean:
            return None
        existing = self._thread_cache.get(thread_id_clean)
        if isinstance(existing, ThreadLike):
            return existing
        thread_link = ""
        if _is_probably_web_thread_id(thread_id_clean):
            thread_link = THREAD_URL_TEMPLATE.format(thread_id=thread_id_clean)
        synthetic = ThreadLike(
            id=thread_id_clean,
            pk=thread_id_clean,
            users=[UserLike(pk=thread_id_clean, id=thread_id_clean, username=thread_id_clean)],
            unread_count=0,
            link=thread_link,
            title=thread_id_clean,
            snippet="",
            source_index=-1,
        )
        self._thread_cache[thread_id_clean] = synthetic
        self._thread_cache_meta.setdefault(
            thread_id_clean,
            {
                "title": thread_id_clean,
                "snippet": "",
                "link": thread_link,
                "idx": -1,
                "selector": "strict_thread_id",
                "key_source": "strict_thread_id",
            },
        )
        return synthetic

    def _is_broadcast_ack_response(self, response) -> bool:
        try:
            status = int(getattr(response, "status", 0) or 0)
        except Exception:
            status = 0
        if status != 200:
            return False
        try:
            url = str(getattr(response, "url", "") or "").strip().lower()
        except Exception:
            url = ""
        if not url:
            return False
        if "/direct_v2/threads/broadcast/" in url:
            return True
        if "/api/graphql" not in url and "/graphql/query" not in url:
            return False
        post_data = ""
        try:
            request = getattr(response, "request", None)
            post_data_attr = getattr(request, "post_data", None)
            if callable(post_data_attr):
                post_data = str(post_data_attr() or "")
            else:
                post_data = str(post_data_attr or "")
        except Exception:
            post_data = ""
        lowered_post = post_data.lower()
        if "broadcast" not in lowered_post:
            return False
        return ("mutation" in lowered_post) or ("direct_v2" in lowered_post) or ("thread" in lowered_post)

    def _extract_client_context_from_payload(self, payload: Any, *, target_item_id: str = "") -> str:
        wanted_item_id = str(target_item_id or "").strip()
        first_context = ""
        stack: list[Any] = [payload]
        while stack:
            current = stack.pop()
            if isinstance(current, list):
                for item in current:
                    if isinstance(item, (dict, list)):
                        stack.append(item)
                continue
            if not isinstance(current, dict):
                continue
            current_context = _coerce_str(current.get("client_context"))
            if current_context and not first_context:
                first_context = current_context
            if wanted_item_id:
                current_item_id = _coerce_str(
                    current.get("item_id")
                    or current.get("itemid")
                    or current.get("message_id")
                    or current.get("messageid")
                    or current.get("id")
                    or current.get("pk")
                )
                if current_item_id and current_item_id == wanted_item_id and current_context:
                    return current_context
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        return first_context

    def _extract_ack_item_metadata(
        self,
        payload: Any,
        *,
        expected_thread_ids: set[str],
        expected_text: str,
    ) -> tuple[str, str]:
        normalized_text = _normalize_message_text(expected_text)
        parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self.user_id)
        for require_text in (bool(normalized_text), False):
            for msg in parsed:
                if str(msg.direction or "").strip().lower() != "outbound":
                    continue
                msg_item_id = str(msg.item_id or "").strip()
                if not msg_item_id:
                    continue
                msg_thread_id = str(msg.thread_id or "").strip()
                if expected_thread_ids and msg_thread_id and msg_thread_id not in expected_thread_ids:
                    continue
                if require_text and not _message_text_matches(normalized_text, msg.text):
                    continue
                client_context = self._extract_client_context_from_payload(
                    payload,
                    target_item_id=msg_item_id,
                )
                return msg_item_id, client_context

        for node, _context_thread_id in _iter_payload_nodes(payload):
            candidate_item_id = _coerce_str(
                node.get("item_id")
                or node.get("itemid")
                or node.get("message_id")
                or node.get("messageid")
                or node.get("pk")
                or node.get("id")
            )
            if not candidate_item_id:
                continue
            node_thread_id = _extract_thread_id_from_node(node)
            if expected_thread_ids and node_thread_id and node_thread_id not in expected_thread_ids:
                continue
            node_text = _extract_message_text_from_api_node(node)
            if normalized_text and node_text and not _message_text_matches(normalized_text, node_text):
                continue
            client_context = self._extract_client_context_from_payload(
                payload,
                target_item_id=candidate_item_id,
            )
            return candidate_item_id, client_context

        return "", self._extract_client_context_from_payload(payload, target_item_id="")

    def _payload_contains_item_id(
        self,
        payload: Any,
        *,
        thread_id: str,
        item_id: str,
    ) -> bool:
        wanted_item_id = str(item_id or "").strip()
        if not wanted_item_id:
            return False
        expected_thread_ids = set(self._expand_thread_ids([str(thread_id or "").strip()]))
        parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self.user_id)
        for msg in parsed:
            msg_item_id = str(msg.item_id or "").strip()
            if msg_item_id != wanted_item_id:
                continue
            msg_thread_id = str(msg.thread_id or "").strip()
            if expected_thread_ids and msg_thread_id and msg_thread_id not in expected_thread_ids:
                continue
            return True
        for node, _context_thread_id in _iter_payload_nodes(payload):
            node_item_id = _coerce_str(
                node.get("item_id")
                or node.get("itemid")
                or node.get("message_id")
                or node.get("messageid")
                or node.get("pk")
                or node.get("id")
            )
            if node_item_id != wanted_item_id:
                continue
            node_thread_id = _extract_thread_id_from_node(node)
            if expected_thread_ids and node_thread_id and node_thread_id not in expected_thread_ids:
                continue
            return True
        return False

    def _fetch_thread_payload_from_endpoint(
        self,
        thread_id: str,
        *,
        limit: int = 20,
        request_timeout_ms: int = 3_500,
        total_timeout_ms: int = 6_000,
    ) -> Optional[dict[str, Any]]:
        safe_thread_id = str(thread_id or "").strip()
        if not safe_thread_id:
            return None
        safe_limit = max(1, min(80, int(limit or 20)))
        safe_request_timeout_ms = max(1_000, min(15_000, int(request_timeout_ms or 3_500)))
        safe_total_timeout_ms = max(
            safe_request_timeout_ms,
            min(20_000, int(total_timeout_ms or 6_000)),
        )
        query_variants: list[dict[str, str]] = [
            {"limit": str(safe_limit)},
            {
                "limit": str(safe_limit),
                "visual_message_return_type": "unseen",
                "persistentBadging": "true",
            },
        ]
        urls: list[str] = []
        for params in query_variants:
            query = urlencode(params, doseq=True)
            for base in (
                f"/api/v1/direct_v2/threads/{safe_thread_id}/",
                f"/api/v1/direct_v2/threads/{safe_thread_id}",
            ):
                candidate = f"{base}?{query}" if query else base
                if candidate not in urls:
                    urls.append(candidate)
        if not urls:
            return None
        self._ensure_inbox_workspace_fast()
        page = self._ensure_page()
        fetch_result = page.evaluate(
            """async ({ urls, requestTimeoutMs, totalTimeoutMs }) => {
                const responseSummary = {
                    ok: false,
                    status: 0,
                    url: "",
                    error: "",
                    payload: null,
                };
                const startedAt = Date.now();
                const isOverTotalBudget = () => {
                    return Number(Date.now() - startedAt) >= Number(totalTimeoutMs || 0);
                };
                const cookieText = String(document.cookie || "");
                const csrfMatch = cookieText.match(/(?:^|;\\s*)csrftoken=([^;]+)/i);
                const csrfToken = csrfMatch ? decodeURIComponent(csrfMatch[1] || "") : "";
                const headers = {
                    "accept": "application/json, text/plain, */*",
                    "x-requested-with": "XMLHttpRequest",
                    "x-ig-app-id": "936619743392459",
                };
                if (csrfToken) {
                    headers["x-csrftoken"] = csrfToken;
                }
                for (const endpoint of (Array.isArray(urls) ? urls : [])) {
                    if (isOverTotalBudget()) {
                        responseSummary.error = responseSummary.error || `timeout_total_${Number(totalTimeoutMs || 0)}ms`;
                        break;
                    }
                    let timeoutHandle = null;
                    try {
                        const controller = new AbortController();
                        timeoutHandle = setTimeout(() => controller.abort(), Number(requestTimeoutMs || 0));
                        const res = await fetch(endpoint, {
                            method: "GET",
                            credentials: "include",
                            headers,
                            signal: controller.signal,
                        });
                        const status = Number(res.status || 0);
                        const bodyText = await res.text();
                        let parsed = null;
                        try {
                            parsed = JSON.parse(bodyText);
                        } catch (_) {
                            parsed = null;
                        }
                        if (parsed && typeof parsed === "object") {
                            const hasThreadItems =
                                Array.isArray(parsed?.thread?.items)
                                || Array.isArray(parsed?.items)
                                || Array.isArray(parsed?.data?.thread?.items)
                                || String(bodyText || "").indexOf("item_id") >= 0;
                            if (res.ok || hasThreadItems) {
                                return {
                                    ok: true,
                                    status,
                                    url: String(endpoint || ""),
                                    error: "",
                                    payload: parsed,
                                };
                            }
                        }
                        if (!responseSummary.error) {
                            responseSummary.error = `status=${status}`;
                        }
                        responseSummary.status = status;
                        responseSummary.url = String(endpoint || "");
                    } catch (err) {
                        const errName = String((err && err.name) || "");
                        if (errName.toLowerCase() === "aborterror") {
                            responseSummary.error = `timeout_fetch_${Number(requestTimeoutMs || 0)}ms`;
                        } else {
                            responseSummary.error = String(err || "fetch_error");
                        }
                        responseSummary.url = String(endpoint || "");
                    } finally {
                        if (timeoutHandle) {
                            clearTimeout(timeoutHandle);
                        }
                    }
                }
                return responseSummary;
            }""",
            {
                "urls": urls,
                "requestTimeoutMs": safe_request_timeout_ms,
                "totalTimeoutMs": safe_total_timeout_ms,
            },
        )
        if not isinstance(fetch_result, dict):
            return None
        if not bool(fetch_result.get("ok")):
            return None
        payload = fetch_result.get("payload")
        if not isinstance(payload, (dict, list)):
            return None
        source_url = str(fetch_result.get("url") or "").strip()
        try:
            self._ingest_api_payload(payload, source_url=source_url)
        except Exception:
            pass
        return {
            "payload": payload,
            "url": source_url,
            "status": int(fetch_result.get("status") or 0),
        }

    def ensure_thread_ready_strict(self, thread_id: str) -> tuple[bool, str]:
        target_thread_id = str(thread_id or "").strip()
        if not target_thread_id:
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                reason="invalid_thread_id",
            )
            return False, "invalid_thread_id"
        thread = self._thread_from_id_strict(target_thread_id)
        if thread is None:
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                thread_id=target_thread_id,
                reason="thread_not_resolved",
            )
            return False, "thread_not_resolved"
        from src.inbox.message_sender import _wait_for_visible_locator_async

        thread_href = _normalize_direct_link(str(getattr(thread, "link", "") or ""))
        if not thread_href:
            meta = self._thread_cache_meta.get(str(getattr(thread, "id", "") or ""), {})
            thread_href = _normalize_direct_link(str(meta.get("link") or ""))
        if not thread_href:
            thread_href = THREAD_URL_TEMPLATE.format(thread_id=target_thread_id)
        if not self.open_thread_by_href(thread_href):
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                thread_id=target_thread_id,
                reason="open_thread_failed",
            )
            return False, "open_thread_failed"
        page = self._ensure_page()
        try:
            composer = self._runtime.run_async(
                _wait_for_visible_locator_async(page, _COMPOSER_SELECTORS, timeout_ms=12_000),
                timeout=15.0,
            )
        except Exception as exc:
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                thread_id=target_thread_id,
                reason="composer_probe_failed",
                error=str(exc) or type(exc).__name__,
            )
            return False, "composer_probe_failed"
        if composer is None:
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                thread_id=target_thread_id,
                reason="composer_not_found",
                url=str(getattr(page, "url", "") or ""),
            )
            return False, "composer_not_found"
        log_browser_stage(
            component="playwright_dm_client",
            stage="composer_ready",
            status="ok",
            account=self.username,
            thread_id=target_thread_id,
            url=str(getattr(page, "url", "") or ""),
        )
        return True, "ok"
        page = self._ensure_page()
        current_url = page.url or ""
        current_thread_id = _extract_thread_id(current_url)
        expected_ids = set(self._expected_thread_ids(thread, post_thread_id=current_thread_id, click_href=""))
        already_on_target = bool(current_thread_id and (not expected_ids or current_thread_id in expected_ids))
        if not already_on_target:
            thread_href = _normalize_direct_link(str(getattr(thread, "link", "") or ""))
            if not thread_href:
                meta = self._thread_cache_meta.get(str(getattr(thread, "id", "") or ""), {})
                thread_href = _normalize_direct_link(str(meta.get("link") or ""))
            opened = False
            if thread_href:
                try:
                    opened = bool(self.open_thread_by_href(thread_href))
                except Exception:
                    opened = False
            if not opened:
                opened = self._open_thread(thread)
            if not opened:
                log_browser_stage(
                    component="playwright_dm_client",
                    stage="composer_ready",
                    status="failed",
                    account=self.username,
                    thread_id=target_thread_id,
                    reason="open_thread_failed",
                )
                return False, "open_thread_failed"
            page = self._ensure_page()
            current_url = page.url or ""
            current_thread_id = _extract_thread_id(current_url)
            expected_after = set(
                self._expected_thread_ids(
                    thread,
                    post_thread_id=current_thread_id,
                    click_href=thread_href,
                )
            )
            if expected_after and (not current_thread_id or current_thread_id not in expected_after):
                log_browser_stage(
                    component="playwright_dm_client",
                    stage="composer_ready",
                    status="failed",
                    account=self.username,
                    thread_id=target_thread_id,
                    reason="opened_unexpected_thread",
                )
                return False, "opened_unexpected_thread"
        composer = self._find_composer(page)
        if composer is None:
            log_browser_stage(
                component="playwright_dm_client",
                stage="composer_ready",
                status="failed",
                account=self.username,
                thread_id=target_thread_id,
                reason="composer_not_found",
                url=current_url,
            )
            return False, "composer_not_found"
        log_browser_stage(
            component="playwright_dm_client",
            stage="composer_ready",
            status="ok",
            account=self.username,
            thread_id=target_thread_id,
            url=current_url,
        )
        return True, "ok"

    def _latest_outbound_record_from_payload(
        self,
        payload: Any,
        *,
        thread_id: str,
        expected_text: str = "",
    ) -> Optional[_APIMessageRecord]:
        expected_thread_ids = set(self._expand_thread_ids([str(thread_id or "").strip()]))
        normalized_text = _normalize_message_text(expected_text)
        parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self.user_id)
        if not parsed:
            return None
        candidates: list[_APIMessageRecord] = []
        for msg in parsed:
            if str(msg.direction or "").strip().lower() != "outbound":
                continue
            msg_item_id = str(msg.item_id or "").strip()
            if not msg_item_id:
                continue
            msg_thread_id = str(msg.thread_id or "").strip()
            if expected_thread_ids and msg_thread_id and msg_thread_id not in expected_thread_ids:
                continue
            if normalized_text and not _message_text_matches(normalized_text, msg.text):
                continue
            candidates.append(msg)
        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.timestamp or 0.0), str(item.item_id or "")), reverse=True)
        return candidates[0]

    def _latest_outbound_record_from_cache(
        self,
        *,
        thread_id: str,
        expected_text: str = "",
    ) -> Optional[_APIMessageRecord]:
        expected_thread_ids = set(self._expand_thread_ids([str(thread_id or "").strip()]))
        normalized_text = _normalize_message_text(expected_text)
        candidates: list[_APIMessageRecord] = []
        for key in expected_thread_ids:
            bucket = self._api_messages_by_thread.get(str(key or "").strip())
            if not isinstance(bucket, dict):
                continue
            for msg in bucket.values():
                if not isinstance(msg, _APIMessageRecord):
                    continue
                if str(msg.direction or "").strip().lower() != "outbound":
                    continue
                if normalized_text and not _message_text_matches(normalized_text, msg.text):
                    continue
                candidates.append(msg)
        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.timestamp or 0.0), str(item.item_id or "")), reverse=True)
        return candidates[0]

    def get_outbound_baseline(self, thread_id: str, *, expected_text: str = "") -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip()
        if not target_thread_id:
            return {"ok": False, "item_id": "", "timestamp": None, "reason": "invalid_thread_id"}
        fetched = self._fetch_thread_payload_from_endpoint(
            target_thread_id,
            limit=30,
            request_timeout_ms=1_800,
            total_timeout_ms=2_600,
        )
        if not fetched:
            return {"ok": False, "item_id": "", "timestamp": None, "reason": "baseline_fetch_failed"}
        payload = fetched.get("payload")
        if not isinstance(payload, (dict, list)):
            return {"ok": False, "item_id": "", "timestamp": None, "reason": "baseline_payload_invalid"}
        latest = self._latest_outbound_record_from_payload(
            payload,
            thread_id=target_thread_id,
            expected_text=expected_text,
        )
        if latest is None:
            return {"ok": True, "item_id": "", "timestamp": None, "reason": "baseline_empty"}
        return {
            "ok": True,
            "item_id": str(latest.item_id or "").strip(),
            "timestamp": float(latest.timestamp or 0.0) or None,
            "reason": "baseline_ok",
        }

    def _dom_has_recent_outbound_text(self, text: str) -> bool:
        target = _normalize_message_text(text)
        if not target:
            return False
        page = self._ensure_page()
        try:
            nodes = self._collect_message_nodes(page)
            total = nodes.count()
        except Exception:
            return False
        if total <= 0:
            return False
        start = max(0, total - 14)
        for idx in range(start, total):
            try:
                node = nodes.nth(idx)
                if not self._is_outbound(node):
                    continue
                node_text = _normalize_message_text(_extract_message_text(node))
                if node_text and _message_text_matches(target, node_text):
                    return True
            except Exception:
                continue
        return False

    def refresh_thread_for_confirmation(self, thread_id: str) -> bool:
        target_thread_id = str(thread_id or "").strip()
        if not target_thread_id:
            return False
        thread = self._thread_from_id_strict(target_thread_id)
        if thread is None:
            return False
        thread_href = _normalize_direct_link(str(getattr(thread, "link", "") or ""))
        if not thread_href:
            meta = self._thread_cache_meta.get(str(getattr(thread, "id", "") or ""), {})
            thread_href = _normalize_direct_link(str(meta.get("link") or ""))
        opened = False
        if thread_href:
            try:
                opened = bool(self.open_thread_by_href(thread_href))
            except Exception:
                opened = False
        if not opened:
            try:
                opened = bool(self._open_thread(thread))
            except Exception:
                opened = False
        if not opened:
            return False
        ready_ok, _ready_reason = self.ensure_thread_ready_strict(target_thread_id)
        return bool(ready_ok)

    def confirm_new_outbound_after_baseline(
        self,
        thread_id: str,
        *,
        baseline_item_id: str = "",
        baseline_timestamp: Optional[float] = None,
        sent_after_ts: Optional[float] = None,
        expected_text: str = "",
        attempts: int = 6,
        poll_interval_seconds: float = 0.8,
        allow_dom: bool = True,
    ) -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip()
        if not target_thread_id:
            return {"ok": False, "item_id": None, "reason": "invalid_thread_id"}
        baseline_item = str(baseline_item_id or "").strip()
        baseline_ts = float(baseline_timestamp) if baseline_timestamp else None
        sent_anchor_ts = float(sent_after_ts) if sent_after_ts else None
        max_attempts = max(1, int(attempts or 1))
        poll_seconds = max(0.12, float(poll_interval_seconds or 0.8))
        fallback_dom_item_id = f"dom-confirmed-{int(time.time() * 1000)}"
        fallback_cached_item_id = f"cached-confirmed-{int(time.time() * 1000)}"

        def _record_is_new(candidate: Optional[_APIMessageRecord]) -> tuple[bool, str]:
            if candidate is None:
                return False, ""
            latest_item_id = str(candidate.item_id or "").strip()
            latest_ts = float(candidate.timestamp or 0.0) if candidate.timestamp else None
            is_new = False
            if baseline_item:
                is_new = bool(latest_item_id and latest_item_id != baseline_item)
            if not is_new and baseline_ts is not None and latest_ts is not None:
                is_new = latest_ts > (baseline_ts + 0.001)
            if not is_new and sent_anchor_ts is not None and latest_ts is not None:
                is_new = latest_ts >= (sent_anchor_ts - 1.5)
            if not is_new:
                return False, ""
            return True, latest_item_id or ""

        for attempt in range(1, max_attempts + 1):
            cached_latest = self._latest_outbound_record_from_cache(
                thread_id=target_thread_id,
                expected_text=expected_text,
            )
            cached_ok, cached_item_id = _record_is_new(cached_latest)
            if cached_ok:
                return {
                    "ok": True,
                    "item_id": cached_item_id or fallback_cached_item_id,
                    "reason": "cached_new_outbound",
                }
            if allow_dom and self._dom_has_recent_outbound_text(expected_text):
                return {
                    "ok": True,
                    "item_id": fallback_dom_item_id,
                    "reason": "dom_outbound_visible",
                }
            should_fetch_endpoint = (attempt == 1) or (attempt % 2 == 0)
            if should_fetch_endpoint:
                fetched = self._fetch_thread_payload_from_endpoint(
                    target_thread_id,
                    limit=30,
                    request_timeout_ms=1_800,
                    total_timeout_ms=2_600,
                )
                if fetched:
                    payload = fetched.get("payload")
                    if isinstance(payload, (dict, list)):
                        latest = self._latest_outbound_record_from_payload(
                            payload,
                            thread_id=target_thread_id,
                            expected_text=expected_text,
                        )
                        endpoint_ok, endpoint_item_id = _record_is_new(latest)
                        if endpoint_ok:
                            return {
                                "ok": True,
                                "item_id": endpoint_item_id or fallback_dom_item_id,
                                "reason": "endpoint_new_outbound",
                            }
            if attempt >= max_attempts:
                break
            wait_ms = max(120, int(poll_seconds * 1000.0))
            try:
                self._ensure_page().wait_for_timeout(wait_ms)
            except Exception:
                time.sleep(min(1.2, max(0.12, float(wait_ms) / 1000.0)))
        return {"ok": False, "item_id": None, "reason": "not_confirmed"}

    def send_text_with_ack(self, thread_id: str, text: str, timeout: float = 4.0) -> dict[str, Any]:
        target_thread_id = str(thread_id or "").strip()
        content = str(text or "")
        from src.inbox.message_sender import TaskDirectClient

        client = TaskDirectClient(
            self._runtime,
            self.account,
            thread_id=target_thread_id,
            thread_href=THREAD_URL_TEMPLATE.format(thread_id=target_thread_id) if target_thread_id else "",
            telemetry_component="playwright_dm_client",
            emit_spawn=False,
            emit_session_telemetry=False,
        )
        return dict(client.send_text_with_ack(target_thread_id, content, timeout=timeout) or {})
        if not target_thread_id:
            return {"ok": False, "item_id": None, "reason": "invalid_thread_id"}
        if not content.strip():
            return {"ok": False, "item_id": None, "reason": "empty_text"}
        can_send, sent_today, limit = can_send_message_for_account(
            account=self.account,
            username=self.username,
            default=None,
        )
        if not can_send:
            return {
                "ok": False,
                "item_id": None,
                "reason": f"account_quota_reached:{sent_today}/{limit}",
            }
        ready_ok, ready_reason = self.ensure_thread_ready_strict(target_thread_id)
        if not ready_ok:
            return {"ok": False, "item_id": None, "reason": f"thread_not_ready:{ready_reason}"}

        page = self._ensure_page()
        composer = self._find_composer(page)
        if composer is None:
            return {"ok": False, "item_id": None, "reason": "composer_not_found"}

        timeout_ms = max(600, int(max(0.4, float(timeout or 4.0)) * 1000.0))
        response_obj = None
        try:
            with page.expect_response(
                lambda response: self._is_broadcast_ack_response(response),
                timeout=timeout_ms,
            ) as response_info:
                composer.click()
                composer.fill(content)
                composer.press("Enter")
            response_obj = response_info.value
        except PlaywrightTimeoutError:
            return {"ok": False, "item_id": None, "reason": "ack_timeout"}
        except Exception as exc:
            return {"ok": False, "item_id": None, "reason": f"ack_wait_error:{exc}"}

        if response_obj is None:
            return {"ok": False, "item_id": None, "reason": "ack_missing"}
        try:
            ack_payload = response_obj.json()
        except Exception:
            return {"ok": False, "item_id": None, "reason": "ack_invalid_json"}

        try:
            ack_url = str(getattr(response_obj, "url", "") or "")
        except Exception:
            ack_url = ""
        try:
            self._ingest_api_payload(ack_payload, source_url=ack_url)
        except Exception:
            pass

        expected_thread_ids = set(self._expand_thread_ids([target_thread_id]))
        item_id, client_context = self._extract_ack_item_metadata(
            ack_payload,
            expected_thread_ids=expected_thread_ids,
            expected_text=content,
        )
        if not item_id:
            result = {
                "ok": False,
                "item_id": None,
                "reason": "ack_item_id_missing",
                "client_context": client_context or None,
            }
            logger.info(
                "PlaywrightDM send_text_with_ack_missing_item thread=%s reason=%s",
                target_thread_id,
                result["reason"],
            )
            return result
        logger.info(
            "PlaywrightDM send_text_with_ack_ok thread=%s item_id=%s client_context=%s",
            target_thread_id,
            item_id,
            client_context or "-",
        )
        return {
            "ok": True,
            "item_id": item_id,
            "reason": "ok",
            "client_context": client_context or None,
        }

    def verify_item_id_in_thread(self, thread_id: str, item_id: str, timeout: float = 10.0) -> bool:
        target_thread_id = str(thread_id or "").strip()
        target_item_id = str(item_id or "").strip()
        if not target_thread_id or not target_item_id:
            return False
        timeout_sec = max(0.5, float(timeout or 10.0))
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            fetched = self._fetch_thread_payload_from_endpoint(
                target_thread_id,
                limit=30,
                request_timeout_ms=3_500,
                total_timeout_ms=6_000,
            )
            if fetched:
                payload = fetched.get("payload")
                if isinstance(payload, (dict, list)):
                    if self._payload_contains_item_id(
                        payload,
                        thread_id=target_thread_id,
                        item_id=target_item_id,
                    ):
                        logger.info(
                            "PlaywrightDM verify_item_id_in_thread_ok thread=%s item_id=%s",
                            target_thread_id,
                            target_item_id,
                        )
                        return True
            remaining_ms = int(max(0.0, (deadline - time.time()) * 1000.0))
            if remaining_ms <= 0:
                break
            wait_ms = max(120, min(800, remaining_ms))
            try:
                self._ensure_page().wait_for_timeout(wait_ms)
            except Exception:
                time.sleep(min(0.8, max(0.12, float(wait_ms) / 1000.0)))
        logger.warning(
            "PlaywrightDM verify_item_id_in_thread_timeout thread=%s item_id=%s",
            target_thread_id,
            target_item_id,
        )
        return False

    def send_message(self, thread: ThreadLike, text: str) -> Optional[str]:
        content = str(text or "").strip()
        thread_id = str(getattr(thread, "id", "") or "")
        result = self.send_text_with_ack(thread_id, content, timeout=4.0)
        if bool(result.get("ok")):
            return str(result.get("item_id") or "").strip() or None
        return None
        if not content:
            log_browser_stage(
                component="playwright_dm_client",
                stage="send_fail",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                reason="empty_text",
            )
            return None
        can_send, _sent_today, _limit = can_send_message_for_account(
            account=self.account,
            username=self.username,
            default=None,
        )
        if not can_send:
            logger.warning("PlaywrightDM quota_reached @%s", self.username)
            log_browser_stage(
                component="playwright_dm_client",
                stage="send_fail",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                reason="account_quota_reached",
            )
            return None
        page = self._ensure_page()
        current_url = page.url or ""
        current_thread_id = _extract_thread_id(current_url)
        expected_ids = set(self._expected_thread_ids(thread, post_thread_id=current_thread_id, click_href=""))
        already_on_target = bool(current_thread_id and (not expected_ids or current_thread_id in expected_ids))
        if not already_on_target:
            opened = self._open_thread(thread)
            if not opened:
                logger.warning(
                    "PlaywrightDM send_message_open_failed thread=%s @%s url=%s expected=%s",
                    thread.id,
                    self.username,
                    current_url,
                    sorted(expected_ids),
                )
                log_browser_stage(
                    component="playwright_dm_client",
                    stage="send_fail",
                    status="failed",
                    account=self.username,
                    thread_id=thread_id,
                    reason="open_thread_failed",
                    url=current_url,
                )
                return None
            page = self._ensure_page()

        composer = self._find_composer(page)
        if composer is None:
            logger.warning("PlaywrightDM no composer thread=%s @%s", thread.id, self.username)
            log_browser_stage(
                component="playwright_dm_client",
                stage="send_fail",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                reason="composer_not_found",
                url=page.url or "",
            )
            return None

        log_browser_stage(
            component="playwright_dm_client",
            stage="send_attempt",
            status="started",
            account=self.username,
            thread_id=thread_id,
            message_length=len(content),
        )
        try:
            composer.click()
            composer.fill(content)
            composer.press("Enter")
        except Exception as exc:
            logger.warning("PlaywrightDM no pudo completar acciones de envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o thread=%s @%s", thread.id, self.username)
            log_browser_stage(
                component="playwright_dm_client",
                stage="send_fail",
                status="failed",
                account=self.username,
                thread_id=thread_id,
                reason="send_action_failed",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return None

        message_id = self._verify_sent(thread, content)
        if not message_id and self._click_send_button(page):
            message_id = self._verify_sent(thread, content)
        if message_id:
            logger.info("PlaywrightDM envio_ok thread=%s msg_id=%s", thread.id, message_id)
            log_browser_stage(
                component="playwright_dm_client",
                stage="send_success",
                status="ok",
                account=self.username,
                thread_id=thread_id,
                item_id=str(message_id),
            )
            return message_id
        logger.warning("PlaywrightDM envio_no_verificado thread=%s @%s", thread.id, self.username)
        log_browser_stage(
            component="playwright_dm_client",
            stage="send_fail",
            status="failed",
            account=self.username,
            thread_id=thread_id,
            reason="send_unconfirmed",
        )
        return None

    def send_audio_file(self, thread: ThreadLike, audio_path: str) -> Optional[str]:
        page = self._ensure_page()
        current_url = page.url or ""
        current_thread_id = _extract_thread_id(current_url)
        expected_ids = set(self._expected_thread_ids(thread, post_thread_id=current_thread_id, click_href=""))
        already_on_target = bool(current_thread_id and (not expected_ids or current_thread_id in expected_ids))
        if not already_on_target:
            opened = self._open_thread(thread)
            if not opened:
                logger.warning(
                    "PlaywrightDM send_audio_open_failed thread=%s @%s url=%s expected=%s",
                    thread.id,
                    self.username,
                    current_url,
                    sorted(expected_ids),
                )
                return None
            page = self._ensure_page()

        audio_file = Path(str(audio_path or "").strip()).expanduser()
        if not audio_file.exists():
            logger.warning("PlaywrightDM audio_file_not_found thread=%s path=%s", thread.id, audio_file)
            return None

        try:
            file_input = None
            for selector in (
                "input[type='file'][accept*='audio']",
                "input[type='file'][accept*='m4a']",
                "input[type='file']",
            ):
                locator = page.locator(selector)
                if locator.count() > 0:
                    file_input = locator.first
                    break
            if file_input is None:
                logger.warning(
                    "PlaywrightDM send_audio_no_file_input thread=%s @%s",
                    thread.id,
                    self.username,
                )
                return None

            before_snapshot = self._latest_outbound_snapshot(page)
            before_signature = str(before_snapshot.get("signature") or "")
            file_input.set_input_files(str(audio_file))
            clicked = self._click_send_button(page)
            media_id = self._verify_audio_sent(
                thread,
                page,
                previous_signature=before_signature,
            )
            if not media_id:
                logger.warning(
                    "PlaywrightDM send_audio_not_confirmed thread=%s @%s clicked=%s",
                    thread.id,
                    self.username,
                    clicked,
                )
                return None
            logger.info("PlaywrightDM send_audio_ok thread=%s media_id=%s path=%s", thread.id, media_id, audio_file)
            return media_id
        except Exception as exc:
            logger.warning(
                "PlaywrightDM send_audio_error thread=%s @%s error=%s",
                thread.id,
                self.username,
                exc,
            )
            return None

    def _ensure_page(self) -> Page:
        if self._page is not None:
            try:
                if not bool(self._page.is_closed()):
                    return self._page
            except Exception:
                pass
            self._page = None

        if _DM_VERBOSE_PROBES:
            print(style_text(f"[PlaywrightDM] Iniciando navegador para @{self.username}...", color=Fore.WHITE))

        storage_state = self.storage_state_path(self.username)
        if not storage_state.exists():
            raise RuntimeError(f"No hay sesion Playwright guardada para @{self.username}.")
        log_browser_stage(
            component="playwright_dm_client",
            stage="session_open_start",
            status="started",
            account=self.username,
            headless=self.headless,
        )
        try:
            page = self._runtime.open_page(self.account, timeout=90.0)
            self._page = page
            self._context = getattr(page, "context", None)
        except Exception as exc:
            log_browser_stage(
                component="playwright_dm_client",
                stage="session_open_end",
                status="failed",
                account=self.username,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
            )
            raise
        log_browser_stage(
            component="playwright_dm_client",
            stage="session_open_end",
            status="ok",
            account=self.username,
        )
        log_browser_stage(
            component="playwright_dm_client",
            stage="browser_open",
            status="ok",
            account=self.username,
            url=str(getattr(self._page, "url", "") or ""),
        )
        return self._page

    def _register_response_listener(self) -> None:
        page = self._page
        if page is None or self._response_listener_registered:
            return
        try:
            page.on("response", self._handle_response)
            self._response_listener_registered = True
        except Exception as exc:
            logger.warning(
                "PlaywrightDM response_listener_error account=@%s error=%s",
                self.username,
                exc,
            )

    def _handle_response(self, response) -> None:
        try:
            url = str(getattr(response, "url", "") or "")
        except Exception:
            return
        if not _is_message_api_url(url):
            return
        try:
            request = getattr(response, "request", None)
            resource_type = str(getattr(request, "resource_type", "") or "").lower()
        except Exception:
            resource_type = ""
        if resource_type and resource_type not in {"xhr", "fetch"}:
            return
        try:
            payload = response.json()
        except Exception:
            return
        self._ingest_api_payload(payload, source_url=url)

    def _ingest_api_payload(self, payload: Any, *, source_url: str = "") -> tuple[int, int]:
        self._register_payload_thread_aliases(payload)
        parsed, missing_timestamp = _extract_api_messages_from_payload(
            payload,
            self_user_id=self.user_id,
        )
        added = 0
        for item in parsed:
            if self._store_api_message(item):
                added += 1
        for missing in missing_timestamp:
            logger.warning(
                "timestamp_missing_from_api thread_id=%s item_id=%s sender_id=%s account=@%s source=%s",
                missing.get("thread_id") or "",
                missing.get("item_id") or "",
                missing.get("sender_id") or "",
                self.username,
                source_url,
            )
        return added, len(missing_timestamp)

    def _store_api_message(self, message: _APIMessageRecord) -> bool:
        thread_id = str(message.thread_id or "").strip()
        item_id = str(message.item_id or "").strip()
        if not thread_id or not item_id:
            return False

        bucket = self._api_messages_by_thread.setdefault(thread_id, {})
        existing = bucket.get(item_id)
        seen_at = time.time()
        if existing is not None:
            # Conservar el mejor contenido si la misma key llega mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºltiples veces.
            merged = _APIMessageRecord(
                thread_id=thread_id,
                sender_id=message.sender_id or existing.sender_id,
                timestamp=max(existing.timestamp, message.timestamp),
                item_id=item_id,
                direction=message.direction or existing.direction,
                text=message.text or existing.text,
            )
            if merged == existing:
                self._api_thread_last_seen[thread_id] = seen_at
                return False
            bucket[item_id] = merged
            self._api_thread_last_seen[thread_id] = seen_at
            self._trim_api_cache(thread_id=thread_id)
            return True

        bucket[item_id] = message
        self._api_thread_last_seen[thread_id] = seen_at
        self._trim_api_cache(thread_id=thread_id)
        return True

    def _trim_api_cache(self, *, thread_id: Optional[str] = None) -> None:
        if thread_id is not None:
            bucket = self._api_messages_by_thread.get(thread_id)
            if bucket:
                overflow = len(bucket) - _DM_API_CACHE_MAX_PER_THREAD
                if overflow > 0:
                    ordered = sorted(
                        bucket.values(),
                        key=lambda item: (item.timestamp, item.item_id),
                    )
                    for old in ordered[:overflow]:
                        bucket.pop(old.item_id, None)
        else:
            for current_thread_id, bucket in list(self._api_messages_by_thread.items()):
                overflow = len(bucket) - _DM_API_CACHE_MAX_PER_THREAD
                if overflow <= 0:
                    continue
                ordered = sorted(
                    bucket.values(),
                    key=lambda item: (item.timestamp, item.item_id),
                )
                for old in ordered[:overflow]:
                    bucket.pop(old.item_id, None)
        overflow_threads = len(self._api_messages_by_thread) - _DM_API_CACHE_MAX_THREADS
        if overflow_threads <= 0:
            return
        stale_threads = sorted(
            self._api_thread_last_seen.items(),
            key=lambda kv: kv[1],
        )
        for thread_id, _ in stale_threads[:overflow_threads]:
            self._api_messages_by_thread.pop(thread_id, None)
            self._api_thread_last_seen.pop(thread_id, None)

    def _register_thread_aliases(self, *thread_ids: str) -> None:
        normalized: list[str] = []
        for candidate in thread_ids:
            value = str(candidate or "").strip()
            if value and value not in normalized:
                normalized.append(value)
        if len(normalized) < 2:
            return
        for source in normalized:
            bucket = self._thread_id_aliases.setdefault(source, set())
            for target in normalized:
                if target != source:
                    bucket.add(target)

    def _register_payload_thread_aliases(self, payload: Any) -> None:
        for alias_pair in _extract_payload_thread_alias_pairs(payload):
            self._register_thread_aliases(*alias_pair)

    def _expand_thread_ids(self, thread_ids: list[str]) -> list[str]:
        queue: list[str] = []
        seen: set[str] = set()
        for candidate in thread_ids:
            value = str(candidate or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            queue.append(value)
        idx = 0
        while idx < len(queue):
            source = queue[idx]
            idx += 1
            for alias in self._thread_id_aliases.get(source, set()):
                value = str(alias or "").strip()
                if not value or value in seen:
                    continue
                seen.add(value)
                queue.append(value)
        return queue

    def _thread_cache_keys(self, thread: ThreadLike, page: Page) -> list[str]:
        base_keys: list[str] = []
        for candidate in (
            getattr(thread, "id", None),
            getattr(thread, "pk", None),
            getattr(self, "_current_thread_id", None),
            _extract_thread_id(getattr(thread, "link", "") or ""),
            _extract_thread_id(getattr(page, "url", "") or ""),
        ):
            value = str(candidate or "").strip()
            if value and value not in base_keys:
                base_keys.append(value)
        return self._expand_thread_ids(base_keys)

    def _expected_thread_ids(
        self,
        thread: ThreadLike,
        *,
        post_thread_id: str = "",
        click_href: str = "",
    ) -> list[str]:
        ids: list[str] = []
        for candidate in (
            getattr(thread, "id", None),
            getattr(thread, "pk", None),
            _extract_thread_id(getattr(thread, "link", "") or ""),
            _extract_thread_id(click_href),
        ):
            value = str(candidate or "").strip()
            if value and value not in ids:
                ids.append(value)
        post_value = str(post_thread_id or "").strip()
        has_real_hint = any(value and not value.startswith("stable_") for value in ids)
        if post_value and (not ids or not has_real_hint):
            ids.append(post_value)
        return ids

    def _snapshot_api_counts(self, thread_ids: list[str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for thread_id in self._expand_thread_ids(thread_ids):
            key = str(thread_id or "").strip()
            if not key:
                continue
            counts[key] = len(self._api_messages_by_thread.get(key, {}))
        return counts

    def _thread_has_new_api_messages(
        self,
        thread_ids: list[str],
        *,
        baseline_counts: dict[str, int],
    ) -> tuple[bool, str]:
        for thread_id in self._expand_thread_ids(thread_ids):
            key = str(thread_id or "").strip()
            if not key:
                continue
            if key not in baseline_counts:
                continue
            current = len(self._api_messages_by_thread.get(key, {}))
            baseline = int(baseline_counts.get(key, 0))
            if current > baseline and current > 0:
                return True, key
        return False, ""

    def _thread_has_cached_api_messages(self, thread_ids: list[str]) -> tuple[bool, str]:
        for thread_id in self._expand_thread_ids(thread_ids):
            key = str(thread_id or "").strip()
            if not key:
                continue
            if len(self._api_messages_by_thread.get(key, {})) > 0:
                return True, key
        return False, ""

    def _message_panel_visible(self, page: Page) -> bool:
        try:
            nodes = self._collect_message_nodes(page)
            total = nodes.count()
        except Exception:
            return False
        if total <= 0:
            return False
        scan_limit = min(total, 20)
        for idx in range(scan_limit):
            try:
                node = nodes.nth(idx)
                if node.locator("xpath=ancestor::*[@role='navigation']").count() > 0:
                    continue
                return True
            except Exception:
                continue
        return False

    def _wait_for_visual_thread_sync(
        self,
        page: Page,
        *,
        timeout_ms: int,
        require_both: bool = True,
        stable_hits_required: int = 2,
    ) -> tuple[bool, dict[str, object]]:
        deadline = time.time() + max(0.0, float(timeout_ms) / 1000.0)
        required_hits = max(1, int(stable_hits_required or 1))
        stable_hits = 0
        last_state: dict[str, object] = {
            "post_url": page.url or "",
            "post_thread_id": _extract_thread_id(page.url or ""),
            "composer_visible": False,
            "message_panel_visible": False,
        }
        while time.time() < deadline:
            post_url = page.url or ""
            post_thread_id = _extract_thread_id(post_url)
            composer = self._find_composer(page)
            composer_visible = False
            if composer is not None:
                try:
                    composer_visible = bool(composer.is_visible())
                except Exception:
                    composer_visible = False

            message_panel_visible = self._message_panel_visible(page)
            last_state = {
                "post_url": post_url,
                "post_thread_id": post_thread_id,
                "composer_visible": composer_visible,
                "message_panel_visible": message_panel_visible,
            }
            visual_ready = False
            if require_both:
                visual_ready = composer_visible and message_panel_visible
            else:
                visual_ready = bool(post_thread_id) and (composer_visible or message_panel_visible)
            if visual_ready:
                stable_hits += 1
                if stable_hits >= required_hits:
                    return True, last_state
            else:
                stable_hits = 0
            try:
                page.wait_for_timeout(120)
            except Exception:
                break
        return False, last_state

    def _wait_for_thread_network_sync(
        self,
        page: Page,
        *,
        expected_thread_ids: list[str],
        baseline_counts: dict[str, int],
        timeout_ms: int,
    ) -> tuple[bool, str]:
        expected = self._expand_thread_ids(expected_thread_ids)
        if not expected:
            return False, ""

        has_cached, cached_thread_id = self._thread_has_new_api_messages(
            expected,
            baseline_counts=baseline_counts,
        )
        if has_cached:
            return True, cached_thread_id

        matched_payload: Any = None
        matched_url = ""
        matched_thread_id = ""
        timeout_value = max(200, int(timeout_ms))

        def _response_predicate(response) -> bool:
            nonlocal matched_payload, matched_url, matched_thread_id
            try:
                url = str(getattr(response, "url", "") or "")
            except Exception:
                return False
            if not _is_message_api_url(url):
                return False
            try:
                request = getattr(response, "request", None)
                resource_type = str(getattr(request, "resource_type", "") or "").lower()
            except Exception:
                resource_type = ""
            if resource_type and resource_type not in {"xhr", "fetch"}:
                return False
            try:
                payload = response.json()
            except Exception:
                return False
            try:
                self._ingest_api_payload(payload, source_url=url)
            except Exception:
                return False
            payload_thread_ids = self._expand_thread_ids(
                list(_payload_thread_ids(payload, self_user_id=self.user_id))
            )
            if not payload_thread_ids:
                return False
            dynamic_expected = set(self._expand_thread_ids(expected_thread_ids))
            intersection = dynamic_expected.intersection(payload_thread_ids)
            if intersection:
                matched_payload = payload
                matched_url = url
                matched_thread_id = sorted(intersection, key=len, reverse=True)[0]
                return True
            return False

        try:
            with page.expect_response(_response_predicate, timeout=timeout_value):
                pass
        except Exception:
            pass

        if matched_payload is not None:
            expected = self._expand_thread_ids(expected_thread_ids)
            has_cached, cached_thread_id = self._thread_has_new_api_messages(
                expected,
                baseline_counts=baseline_counts,
            )
            if has_cached:
                return True, cached_thread_id or matched_thread_id
            return True, matched_thread_id

        deadline = time.time() + max(0.0, float(timeout_value) / 1000.0)
        while time.time() < deadline:
            expected = self._expand_thread_ids(expected_thread_ids)
            has_cached, cached_thread_id = self._thread_has_new_api_messages(
                expected,
                baseline_counts=baseline_counts,
            )
            if has_cached:
                return True, cached_thread_id
            try:
                page.wait_for_timeout(80)
            except Exception:
                break
        return False, matched_thread_id

    def _get_api_messages_for_thread(
        self,
        thread: ThreadLike,
        page: Page,
        *,
        timeout_ms: int,
    ) -> list[_APIMessageRecord]:
        deadline = time.time() + max(0.0, float(timeout_ms) / 1000.0)
        merged: dict[str, _APIMessageRecord] = {}

        while time.time() < deadline:
            keys = self._thread_cache_keys(thread, page)
            merged = {}
            for key in keys:
                for item_id, msg in self._api_messages_by_thread.get(key, {}).items():
                    prev = merged.get(item_id)
                    if prev is None or msg.timestamp >= prev.timestamp:
                        merged[item_id] = msg
            if merged:
                break
            try:
                page.wait_for_timeout(80)
            except Exception:
                break

        if not merged:
            keys = self._thread_cache_keys(thread, page)
            for key in keys:
                for item_id, msg in self._api_messages_by_thread.get(key, {}).items():
                    prev = merged.get(item_id)
                    if prev is None or msg.timestamp >= prev.timestamp:
                        merged[item_id] = msg

        ordered = sorted(
            merged.values(),
            key=lambda item: (item.timestamp, item.item_id),
            reverse=True,
        )
        return ordered

    def _ensure_inbox_workspace_fast(self) -> None:
        page = self._ensure_page()
        from src.inbox.conversation_sync import ensure_inbox_page

        try:
            self._runtime.run_async(ensure_inbox_page(page, timeout_ms=_DM_INBOX_FAST_GOTO_TIMEOUT_MS), timeout=30.0)
        except Exception as exc:
            log_browser_stage(
                component="playwright_dm_client",
                stage="inbox_ready",
                status="failed",
                account=self.username,
                reason="inbox_not_ready",
                error=str(exc) or type(exc).__name__,
            )
            raise
        log_browser_stage(
            component="playwright_dm_client",
            stage="inbox_ready",
            status="ok",
            account=self.username,
            url=str(getattr(page, "url", "") or ""),
        )
        log_browser_stage(
            component="playwright_dm_client",
            stage="workspace_ready",
            status="ok",
            account=self.username,
            url=str(getattr(page, "url", "") or ""),
        )
        return
        current_url = str(page.url or "")
        is_direct_workspace = ("/direct/inbox/" in current_url) or ("/direct/t/" in current_url)
        if not is_direct_workspace:
            try:
                page.goto(
                    INBOX_URL,
                    wait_until="domcontentloaded",
                    timeout=_DM_INBOX_FAST_GOTO_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError:
                logger.warning(
                    "PlaywrightDM inbox_fast_goto_timeout account=@%s timeout_ms=%s",
                    self.username,
                    _DM_INBOX_FAST_GOTO_TIMEOUT_MS,
                )
            except Exception:
                pass
        self._dismiss_overlays(page)
        if not self._account_status_checked:
            status = detect_account_status_sync(page)
            self._account_status_checked = True
            log_account_status(self.username, status)
            try:
                import health_store

                health_store.update_from_playwright_status(self.username, status, reason=status)
            except Exception:
                pass
            if status != "VIVA":
                logger.warning(
                    "PlaywrightDM account_status_stop account=@%s status=%s url=%s",
                    self.username,
                    status,
                    page.url,
                )
                self.close()
                raise RuntimeError(
                    f"Cuenta @{self.username} no disponible para operar. Estado detectado: {status}."
                )
        self._assert_logged_in(page)

    def _open_inbox(self, force_reload: bool = False) -> None:
        del force_reload
        self._ensure_inbox_workspace_fast()
        return
        page = self._ensure_page()
        if not force_reload and INBOX_URL in (page.url or ""):
            # Ya estamos en el inbox, no recargar
            return

        try:
            page.goto(INBOX_URL, wait_until="domcontentloaded", timeout=45_000)
        except PlaywrightTimeoutError:
            pass
        self._dismiss_overlays(page)
        if not self._account_status_checked:
            status = detect_account_status_sync(page)
            self._account_status_checked = True
            log_account_status(self.username, status)
            try:
                import health_store

                health_store.update_from_playwright_status(self.username, status, reason=status)
            except Exception:
                pass
            if status != "VIVA":
                logger.warning(
                    "PlaywrightDM account_status_stop account=@%s status=%s url=%s",
                    self.username,
                    status,
                    page.url,
                )
                self.close()
                raise RuntimeError(
                    f"Cuenta @{self.username} no disponible para operar. Estado detectado: {status}."
                )
        self._assert_logged_in(page)

        row_selectors = tuple(self._row_selector_candidates())
        chosen = ""
        for selector in row_selectors:
            try:
                if page.locator(selector).count():
                    chosen = selector
                    break
            except Exception:
                continue
        rows_ready = bool(chosen)

        found_container = None
        # Lista ampliada de selectores de contenedor para mayor robustez
        container_candidates = (
            "div[role='main']",
            "main",
            "section",
            "div[aria-label='Direct']",
            "div[aria-label='Mensajes']",
            "div[aria-label='Chats']",
            "div[role='navigation']",
            "a[href^='/direct/t/']"
        )

        for selector in container_candidates:
            try:
                if page.locator(selector).count() > 0:
                    found_container = selector
                    break
            except Exception:
                continue

        if not found_container and not rows_ready:
            # Si ninguno es visible de inmediato, esperar brevemente al mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡s probable
            try:
                page.wait_for_selector("div[role='main'], main, div[role='navigation']", timeout=10_000)
                # Re-chequear
                for selector in container_candidates:
                    if page.locator(selector).count() > 0:
                        found_container = selector
                        break
            except Exception:
                pass

        if _DM_VERBOSE_PROBES:
            print(style_text(f"[Probe] Inbox container: {found_container}", color=Fore.WHITE))
        if not rows_ready:
            for search_selector in ("input[placeholder='Buscar']", "input[placeholder='Search']", "input[name='queryBox']"):
                try:
                    page.wait_for_selector(search_selector, timeout=15_000)
                    break
                except Exception:
                    continue
        if not rows_ready:
            deadline = time.time() + 12.0
            while time.time() < deadline and not rows_ready:
                for selector in row_selectors:
                    try:
                        if page.locator(selector).count():
                            chosen = selector
                            rows_ready = True
                            break
                    except Exception:
                        continue
                if not rows_ready:
                    try:
                        page.wait_for_timeout(500)
                    except Exception:
                        break
        if not rows_ready:
            logger.warning("PlaywrightDM inbox rows not ready for @%s", self.username)
            for selector in row_selectors:
                try:
                    count = page.locator(selector).count()
                except Exception:
                    count = 0
                logger.info("PlaywrightDM row_selector_count selector=%s count=%s", selector, count)
            log_browser_stage(
                component="playwright_dm_client",
                stage="inbox_ready",
                status="failed",
                account=self.username,
                reason="rows_not_ready",
                url=page.url,
            )
            raise RuntimeError(f"Inbox not ready for @{self.username}.")
        logger.info("PlaywrightDM inbox_abierto account=@%s", self.username)
        log_browser_stage(
            component="playwright_dm_client",
            stage="inbox_ready",
            status="ok",
            account=self.username,
            url=page.url,
        )
        time.sleep(1)

    def _dismiss_overlays(self, page: Page) -> None:
        """
        Cierra overlays de forma robusta:
        1. Detecta div[role='dialog'] y busca botones de cierre dentro del dialog
        2. Si hay inputs de login y no se puede cerrar ? RuntimeError
        3. Intenta Escape + botones de cierre
        4. Re-chequea login inputs al final (no depende del chequeo inicial)
        """
        # Intentar detectar y cerrar dialogs primero
        try:
            dialogs = page.locator("div[role='dialog']")
            dialog_count = dialogs.count()
            if dialog_count > 0:
                logger.info("PlaywrightDM overlay_detected count=%d", dialog_count)
                for idx in range(dialog_count):
                    try:
                        dialog = dialogs.nth(idx)
                        # Buscar botÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n de cierre dentro del dialog
                        close_selectors = [
                            "button[aria-label='Cerrar']",
                            "button[aria-label='Close']",
                            "svg[aria-label='Close']",
                        ]
                        closed = False
                        for sel in close_selectors:
                            try:
                                close_btn = dialog.locator(sel)
                                if close_btn.count() > 0:
                                    if sel.startswith("svg"):
                                        parent = close_btn.first.locator("xpath=ancestor::button[1]")
                                        if parent.count():
                                            parent.first.click()
                                            closed = True
                                            break
                                    else:
                                        close_btn.first.click()
                                        closed = True
                                        break
                            except Exception:
                                continue
                        if closed:
                            logger.info("PlaywrightDM overlay_closed idx=%d", idx)
                    except Exception:
                        continue
        except Exception:
            pass
        
        # Escape general
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        
        # Buscar botones de cierre globales
        for selector in (
            "button[aria-label='Cerrar']",
            "button[aria-label='Close']",
            "svg[aria-label='Close']",
        ):
            try:
                loc = page.locator(selector)
                if loc.count():
                    if selector.startswith("svg"):
                        try:
                            parent = loc.first.locator("xpath=ancestor::button[1]")
                            if parent.count():
                                parent.first.click()
                                logger.info("PlaywrightDM overlay_closed method=global_svg")
                                break
                        except Exception:
                            pass
                    else:
                        loc.first.click()
                        logger.info("PlaywrightDM overlay_closed method=global_button")
                        break
            except Exception:
                continue
        
        # "Ahora no" / "Not now"
        for label in ("Ahora no", "Not now"):
            try:
                btn = page.locator(f"button:has-text('{label}')")
                if btn.count():
                    btn.first.click()
                    logger.info("PlaywrightDM overlay_closed method=not_now label=%s", label)
                    break
            except Exception:
                continue
        
        # CRÃTICO: Siempre re-chequear login inputs al final (no depender del chequeo inicial)
        # porque pueden aparecer despuÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©s de cerrar otros overlays
        try:
            has_login_after_close = page.locator("input[name='username'], input[name='password']").count() > 0
            if has_login_after_close:
                raise RuntimeError(
                    f"Overlay de login detectado y no se pudo cerrar para @{self.username}. "
                    "SesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n posiblemente invÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡lida."
                )
        except RuntimeError:
            raise
        except Exception:
            pass

    def return_to_inbox(self) -> None:
        """
        Vuelve a la vista del inbox sin recargar la pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡gina si es posible.
        """
        page = self._ensure_page()
        current_url = page.url or ""
        if INBOX_URL in current_url and not re.search(r"/direct/t/", current_url):
            return

        # Camino rapido: si estamos en /direct/t/, intentar link al inbox.
        if "/direct/t/" in current_url:
            try:
                inbox_link = page.locator("a[href='/direct/inbox/'], a[href*='/direct/inbox/']").first
                if inbox_link.count() > 0:
                    inbox_link.click(timeout=max(400, _DM_RETURN_INBOX_TIMEOUT_MS // 2))
                    try:
                        page.wait_for_url(re.compile(r".*/direct/inbox/.*"), timeout=_DM_RETURN_INBOX_TIMEOUT_MS)
                        return
                    except Exception:
                        pass
            except Exception:
                pass

        try:
            # Fallback rapido sin bloquear demasiado.
            page.go_back(wait_until="commit", timeout=_DM_RETURN_INBOX_TIMEOUT_MS)
        except Exception:
            self._open_inbox()

    def _open_thread(
        self,
        thread: ThreadLike,
        *,
        visual_timeout_ms: Optional[int] = None,
        network_timeout_ms: Optional[int] = None,
        force_workspace: bool = False,
        prefer_cache: bool = False,
    ) -> bool:
        """
        [CLICK-FIRST] Intenta abrir un thread y valida post-click.
        Retorna True si el thread es un DM real (/direct/t/ + composer visible).
        """
        del network_timeout_ms, force_workspace, prefer_cache
        return self.open_thread_by_href(
            str(getattr(thread, "link", "") or "") or THREAD_URL_TEMPLATE.format(thread_id=str(getattr(thread, "id", "") or "").strip()),
            visual_timeout_ms=visual_timeout_ms,
        )
        page = self._ensure_page()
        visual_timeout_value = (
            _DM_THREAD_VISUAL_SYNC_TIMEOUT_MS
            if visual_timeout_ms is None
            else max(1_000, int(visual_timeout_ms))
        )
        network_timeout_value = (
            _DM_THREAD_NETWORK_SYNC_TIMEOUT_MS
            if network_timeout_ms is None
            else max(1_000, int(network_timeout_ms))
        )
        current_url = page.url or ""
        row_stale_detected = False
        self._set_last_open_thread_diag(
            pre_url=current_url,
            post_url=current_url,
            was_in_thread=bool("/direct/t/" in current_url),
            visual_wait_ms=0,
            network_wait_ms=0,
            failed_condition="not_started",
            row_stale=False,
            visual_timeout_ms=visual_timeout_value,
            network_timeout_ms=network_timeout_value,
        )

        # 1. Asegurar workspace Direct sin forzar "back" en cada thread.
        if force_workspace:
            try:
                self._ensure_inbox_workspace_fast()
            except Exception:
                pass
            try:
                self._open_inbox(force_reload=False)
            except Exception:
                pass
        else:
            if not self._is_in_direct_workspace(page):
                self._open_inbox()
            elif "/direct/t/" in current_url:
                self.return_to_inbox()

        # 2. Intentar clickear por indice usando el selector cacheado del scan
        # source_index path
        if not prefer_cache and thread.source_index != -1:
            meta = self._thread_cache_meta.get(thread.id, {})
            row_selector = meta.get("selector") or THREAD_ROW_SELECTOR
            thread_link_id = _extract_thread_id(str(getattr(thread, "link", "") or ""))
            meta_link_id = _extract_thread_id(str(meta.get("link") or ""))
            has_real_identity = (
                not str(getattr(thread, "id", "") or "").startswith("stable_")
                or bool(thread_link_id)
                or bool(meta_link_id)
            )
            if not has_real_identity:
                logger.info(
                    "PlaywrightDM open_thread_skip_index_unstable_identity account=@%s thread_id=%s selector=%s idx=%s title=%s",
                    self.username,
                    thread.id,
                    row_selector,
                    thread.source_index,
                    meta.get("title") or getattr(thread, "title", ""),
                )
            else:
                try:
                    rows = page.locator(row_selector)
                    row_count = rows.count()
                    if row_count > thread.source_index:
                        row = rows.nth(thread.source_index)
                        row_preview = self._row_preview(row)
                        try:
                            row_valid = self._row_is_valid(row, selector=row_selector, fast=True)
                        except TypeError:
                            row_valid = self._row_is_valid(row, selector=row_selector)
                        if not row_valid:
                            logger.error(
                                "PlaywrightDM open_thread_invalid_row account=@%s thread_id=%s selector=%s idx=%s row=%s",
                                self.username,
                                thread.id,
                                row_selector,
                                thread.source_index,
                                row_preview,
                            )
                        else:
                            row_lines = self._row_lines(row)
                            row_title = (row_lines[0] if row_lines else "").strip()
                            row_snippet = " | ".join(
                                (line or "").strip() for line in row_lines[1:4] if (line or "").strip()
                            )
                            expected_for_click = list(self._expected_thread_ids(thread, click_href=""))
                            meta_link_id = _extract_thread_id(str(meta.get("link") or ""))
                            if meta_link_id and meta_link_id not in expected_for_click:
                                expected_for_click.append(meta_link_id)
                            row_thread_key, _row_key_source, row_link = self._resolve_thread_key(
                                page,
                                row,
                                title=row_title,
                                peer_username=row_title,
                                snippet=row_snippet,
                            )
                            meta_title_norm = _normalize_key_source(
                                str(meta.get("title") or getattr(thread, "title", "") or "")
                            )
                            row_title_norm = _normalize_key_source(row_title)
                            title_matches = (
                                not meta_title_norm
                                or (
                                    bool(row_title_norm)
                                    and (
                                        row_title_norm == meta_title_norm
                                        or row_title_norm in meta_title_norm
                                        or meta_title_norm in row_title_norm
                                    )
                                )
                            )
                            expected_ids = set(self._expand_thread_ids(expected_for_click))
                            row_ids = set(
                                self._expand_thread_ids(
                                    [row_thread_key, _extract_thread_id(row_link)]
                                )
                            )
                            id_matches = (not expected_ids) or (not row_ids) or bool(
                                expected_ids.intersection(row_ids)
                            )
                            if not title_matches or not id_matches:
                                row_stale_detected = True
                                logger.info(
                                    "PlaywrightDM open_thread_skip_stale_index account=@%s thread_id=%s selector=%s idx=%s expected_ids=%s row_ids=%s expected_title=%s row_title=%s row=%s",
                                    self.username,
                                    thread.id,
                                    row_selector,
                                    thread.source_index,
                                    sorted(expected_ids),
                                    sorted(row_ids),
                                    meta.get("title") or "",
                                    row_title,
                                    row_preview,
                                )
                            else:
                                pre_url = page.url or ""
                                baseline_counts = self._snapshot_api_counts(
                                    expected_for_click
                                )
                                clicked, click_href = self._click_row_target(
                                    row,
                                    selector=row_selector,
                                    idx=thread.source_index,
                                )
                                if clicked and self._validate_open_state(
                                    thread,
                                    pre_url=pre_url,
                                    selector=row_selector,
                                    idx=thread.source_index,
                                    row_preview=row_preview,
                                    click_href=click_href,
                                    baseline_counts=baseline_counts,
                                    visual_timeout_ms=visual_timeout_value,
                                    network_timeout_ms=network_timeout_value,
                                    row_stale=row_stale_detected,
                                ):
                                    return True
                                if not self._is_in_direct_workspace(page):
                                    self.return_to_inbox()
                    else:
                        self._set_last_open_thread_diag(
                            failed_condition="missing_row",
                            row_stale=row_stale_detected,
                        )
                        logger.error(
                            "PlaywrightDM open_thread_missing_row account=@%s thread_id=%s selector=%s idx=%s row_count=%s",
                            self.username,
                            thread.id,
                            row_selector,
                            thread.source_index,
                            row_count,
                        )
                except Exception as exc:
                    self._set_last_open_thread_diag(
                        failed_condition="click_error",
                        row_stale=row_stale_detected,
                    )
                    logger.error(
                        "PlaywrightDM open_thread_click_error account=@%s thread_id=%s selector=%s idx=%s error=%s",
                        self.username,
                        thread.id,
                        row_selector,
                        thread.source_index,
                        exc,
                    )
            
        if self._open_thread_by_cache(
            thread,
            visual_timeout_ms=visual_timeout_value,
            network_timeout_ms=network_timeout_value,
            row_stale=row_stale_detected,
        ):
            return True

        final_url = ""
        try:
            final_url = str(self._ensure_page().url or "")
        except Exception:
            final_url = ""
        self._set_last_open_thread_diag(
            post_url=final_url,
            failed_condition="open_thread_failed",
            row_stale=row_stale_detected,
        )
        logger.error(
            "PlaywrightDM open_thread_failed account=@%s thread_id=%s title=%s",
            self.username,
            thread.id,
            thread.title,
        )
        return False

    def _click_row_target(self, row, *, selector: str, idx: int) -> tuple[bool, str]:
        click_target = row
        click_href = ""
        used_anchor_target = False
        page = self._ensure_page()
        self._dismiss_transient_overlay(page)
        try:
            href = (row.get_attribute("href") or "").strip()
        except Exception:
            href = ""
        if "/direct/t/" in href:
            click_href = _normalize_direct_link(href)
        else:
            try:
                anchors = row.locator("a[href*='/direct/t/']")
                anchor_count = anchors.count()
                if anchor_count == 1:
                    anchor = anchors.first
                    click_target = anchor
                    used_anchor_target = True
                    href = (anchor.get_attribute("href") or "").strip()
                    if "/direct/t/" in href:
                        click_href = _normalize_direct_link(href)
            except Exception:
                pass

        try:
            click_target.scroll_into_view_if_needed()
        except Exception:
            pass

        try:
            box = None
            try:
                box = click_target.bounding_box()
            except Exception:
                box = None
            if box and used_anchor_target:
                width = float(box.get("width") or 0.0)
                height = float(box.get("height") or 0.0)
                if width > 0 and height > 0:
                    # Prefer a left/center click to avoid hitting per-row action buttons.
                    safe_x = max(8.0, min(width - 8.0, width * 0.35))
                    safe_y = max(8.0, min(height - 8.0, height * 0.5))
                    click_target.click(timeout=3000, position={"x": safe_x, "y": safe_y})
                    return True, click_href
            click_target.click(timeout=3000)
            return True, click_href
        except Exception:
            try:
                click_target.click(timeout=3000, force=True)
                return True, click_href
            except Exception as exc:
                logger.error(
                    "PlaywrightDM click_failed selector=%s idx=%s error=%s",
                    selector,
                    idx,
                    exc,
                )
                return False, click_href

    def _dismiss_transient_overlay(self, page: Page) -> None:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        close_selectors = (
            "button[aria-label='Cerrar']",
            "button[aria-label='Close']",
            "svg[aria-label='Close']",
        )
        for selector in close_selectors:
            try:
                loc = page.locator(selector)
                if loc.count() <= 0:
                    continue
                if selector.startswith("svg"):
                    btn = loc.first.locator("xpath=ancestor::button[1]")
                    if btn.count() > 0:
                        btn.first.click(timeout=500)
                        return
                else:
                    loc.first.click(timeout=500)
                    return
            except Exception:
                continue

    def _is_in_direct_workspace(self, page: Page) -> bool:
        url = page.url or ""
        return ("/direct/inbox/" in url) or ("/direct/t/" in url)

    def _has_thread_rows_visible(self, page: Page) -> bool:
        for selector in self._row_selector_candidates():
            try:
                if page.locator(selector).count() > 0:
                    return True
            except Exception:
                continue
        return False

    def _set_last_open_thread_diag(self, **payload: object) -> None:
        previous = (
            dict(self._last_open_thread_diag)
            if isinstance(self._last_open_thread_diag, dict)
            else {}
        )
        previous.update(payload)
        self._last_open_thread_diag = previous

    def _get_last_open_thread_diag(self) -> dict[str, object]:
        if isinstance(self._last_open_thread_diag, dict):
            return dict(self._last_open_thread_diag)
        return {}

    def _validate_open_state(
        self,
        thread: ThreadLike,
        *,
        pre_url: str,
        selector: str,
        idx: int,
        row_preview: str,
        click_href: str,
        baseline_counts: Optional[dict[str, int]] = None,
        visual_timeout_ms: Optional[int] = None,
        network_timeout_ms: Optional[int] = None,
        row_stale: bool = False,
    ) -> bool:
        page = self._ensure_page()
        pre_thread_id = _extract_thread_id(pre_url)
        visual_timeout_value = (
            _DM_THREAD_VISUAL_SYNC_TIMEOUT_MS
            if visual_timeout_ms is None
            else max(1_000, int(visual_timeout_ms))
        )
        network_timeout_value = (
            _DM_THREAD_NETWORK_SYNC_TIMEOUT_MS
            if network_timeout_ms is None
            else max(1_000, int(network_timeout_ms))
        )

        visual_started = time.perf_counter()
        visual_ok, visual_state = self._wait_for_visual_thread_sync(
            page,
            timeout_ms=visual_timeout_value,
        )
        visual_wait_ms = int((time.perf_counter() - visual_started) * 1000.0)
        post_url = str(visual_state.get("post_url") or (page.url or ""))
        post_thread_id = str(visual_state.get("post_thread_id") or _extract_thread_id(post_url))
        composer_visible = bool(visual_state.get("composer_visible"))
        message_panel_visible = bool(visual_state.get("message_panel_visible"))
        url_is_thread = bool(post_thread_id)
        thread_id_changed = bool(post_thread_id) and post_thread_id != pre_thread_id

        if not visual_ok:
            logger.error(
                "PlaywrightDM open_thread_visual_sync_error account=@%s target_thread=%s selector=%s idx=%s row=%s pre_url=%s post_url=%s click_href=%s url_is_thread=%s composer_visible=%s thread_id_changed=%s message_panel_visible=%s",
                self.username,
                thread.id,
                selector,
                idx,
                row_preview,
                pre_url,
                post_url,
                click_href,
                url_is_thread,
                composer_visible,
                thread_id_changed,
                message_panel_visible,
            )
            self._dismiss_transient_overlay(page)
            self._set_last_open_thread_diag(
                pre_url=pre_url,
                post_url=post_url,
                was_in_thread=bool("/direct/t/" in str(pre_url or "")),
                visual_wait_ms=visual_wait_ms,
                network_wait_ms=0,
                failed_condition="visual_sync",
                row_stale=bool(row_stale),
                visual_timeout_ms=visual_timeout_value,
                network_timeout_ms=network_timeout_value,
            )
            return False

        expected_thread_ids = self._expected_thread_ids(
            thread,
            post_thread_id=post_thread_id,
            click_href=click_href,
        )
        baseline = dict(baseline_counts or {})
        for thread_id in expected_thread_ids:
            baseline.setdefault(str(thread_id), 0)
        network_started = time.perf_counter()
        network_ok, network_thread_id = self._wait_for_thread_network_sync(
            page,
            expected_thread_ids=expected_thread_ids,
            baseline_counts=baseline,
            timeout_ms=network_timeout_value,
        )
        network_wait_ms = int((time.perf_counter() - network_started) * 1000.0)
        if not network_ok and visual_ok:
            quick_started = time.perf_counter()
            quick_timeout_ms = max(250, min(1_200, int(network_timeout_value * 0.35)))
            try:
                self._get_api_messages_for_thread(
                    thread,
                    page,
                    timeout_ms=quick_timeout_ms,
                )
            except Exception:
                pass
            quick_ok, quick_thread_id = self._thread_has_new_api_messages(
                expected_thread_ids,
                baseline_counts=baseline,
            )
            if not quick_ok:
                cached_ok, cached_thread_id = self._thread_has_cached_api_messages(
                    expected_thread_ids,
                )
                if cached_ok:
                    quick_ok = True
                    quick_thread_id = cached_thread_id
            network_wait_ms += int((time.perf_counter() - quick_started) * 1000.0)
            if quick_ok:
                network_ok = True
                network_thread_id = quick_thread_id
        if not network_ok:
            logger.error(
                "PlaywrightDM open_thread_network_sync_error account=@%s target_thread=%s selector=%s idx=%s row=%s expected_thread_ids=%s pre_url=%s post_url=%s click_href=%s",
                self.username,
                thread.id,
                selector,
                idx,
                row_preview,
                expected_thread_ids,
                pre_url,
                post_url,
                click_href,
            )
            self._set_last_open_thread_diag(
                pre_url=pre_url,
                post_url=post_url,
                was_in_thread=bool("/direct/t/" in str(pre_url or "")),
                visual_wait_ms=visual_wait_ms,
                network_wait_ms=network_wait_ms,
                failed_condition="network_sync",
                row_stale=bool(row_stale),
                visual_timeout_ms=visual_timeout_value,
                network_timeout_ms=network_timeout_value,
            )
            return False

        network_thread_id = str(network_thread_id or "").strip()
        if post_thread_id and network_thread_id and post_thread_id != network_thread_id:
            self._register_thread_aliases(post_thread_id, network_thread_id)
        resolved_thread_id = network_thread_id or post_thread_id
        print(
            style_text(
                f"[TRACE_ID SYNC BEFORE] pre_url={pre_url} post_url={post_url} post_thread_id={post_thread_id} id={thread.id} pk={thread.pk} flags=url_is_thread:{url_is_thread},composer_visible:{composer_visible},message_panel_visible:{message_panel_visible},thread_id_changed:{thread_id_changed}",
                color=Fore.WHITE,
            )
        )
        self._sync_thread_id(thread, resolved_thread_id)
        print(style_text(f"[TRACE_ID SYNC AFTER] id={thread.id} pk={thread.pk}", color=Fore.WHITE))
        self._current_thread_id = thread.id
        self._assert_logged_in(page)
        self._refresh_thread_participants(page, thread)
        logger.info(
            "PlaywrightDM open_thread_ok account=@%s thread_id=%s selector=%s idx=%s row=%s url=%s",
            self.username,
            thread.id,
            selector,
            idx,
            row_preview,
            post_url,
        )
        self._set_last_open_thread_diag(
            pre_url=pre_url,
            post_url=post_url,
            was_in_thread=bool("/direct/t/" in str(pre_url or "")),
            visual_wait_ms=visual_wait_ms,
            network_wait_ms=network_wait_ms,
            failed_condition="ok",
            row_stale=bool(row_stale),
            visual_timeout_ms=visual_timeout_value,
            network_timeout_ms=network_timeout_value,
        )
        return True

    def _sync_thread_id(self, thread: ThreadLike, real_id: str) -> None:
        if not real_id:
            return
        if real_id == thread.id:
            return
        old_id = thread.id
        self._register_thread_aliases(old_id, real_id)
        link_thread_id = _extract_thread_id(getattr(thread, "link", "") or "")
        if link_thread_id:
            self._register_thread_aliases(link_thread_id, real_id)
        thread.id = real_id
        thread.pk = real_id
        if old_id in self._thread_cache:
            self._thread_cache[real_id] = self._thread_cache.pop(old_id)
        if old_id in self._thread_cache_meta:
            self._thread_cache_meta[real_id] = self._thread_cache_meta.pop(old_id)

    def _collect_message_nodes(self, page: Page):
        container = None
        for selector in _MESSAGE_CONTAINER_SELECTORS:
            try:
                if page.locator(selector).count():
                    container = page.locator(selector).first
                    break
            except Exception:
                continue
        if container is None:
            container = page

        for selector in _MESSAGE_NODE_SELECTORS:
            try:
                nodes = container.locator(selector)
                if nodes.count():
                    return nodes
            except Exception:
                continue
        return container.locator(_MESSAGE_NODE_SELECTORS[0])

    # LEGACY: FunciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n deshabilitada - ya no se usa (usaba _THREAD_ANCHOR_SELECTORS eliminadas)
    # def _select_thread_anchor(...)

    def _locator_first_attribute(self, locator, attr_name: str, *, timeout_ms: int = 300) -> str:
        try:
            if locator.count() <= 0:
                return ""
        except Exception:
            return ""
        try:
            return (locator.first.get_attribute(attr_name, timeout=timeout_ms) or "").strip()
        except TypeError:
            try:
                return (locator.first.get_attribute(attr_name) or "").strip()
            except Exception:
                return ""
        except Exception:
            return ""

    def _resolve_thread_key(
        self,
        page: Page,
        row,
        *,
        title: str,
        peer_username: str,
        snippet: str,
    ) -> tuple[str, str, str]:
        # IMPORTANT:
        # Resolver la identidad del thread desde la fila (row), no desde la URL actual.
        # Si se usa page.url mientras estamos en /direct/t/, todas las filas pueden quedar
        # con el mismo id y el barrido se corta antes de tiempo.
        href = self._locator_first_attribute(row, "href")
        if "/direct/t/" not in href:
            href = self._locator_first_attribute(row.locator("a[href*='/direct/t/']"), "href")
        thread_id = _extract_thread_id(href)
        if thread_id:
            return thread_id, "row_href", _normalize_direct_link(href)

        base = _normalize_key_source(title)
        peer_norm = _normalize_key_source(peer_username)
        snippet_norm = _normalize_key_source(snippet)
        if base and peer_norm:
            base = f"{base}|{peer_norm}"
        elif not base:
            base = peer_norm or snippet_norm
        if not base:
            return "", "none", ""
        stable_id = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return f"stable_{stable_id}", "stable_id", ""

    def _log_navigation_state(self, label: str) -> None:
        """
        Probe de diagnÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³stico para saber exactamente donde estamos y quÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â© vemos.
        """
        try:
            if self._page is None:
                return
            page = self._page
            url = page.url or ""
            in_inbox = "/direct/inbox/" in url
            in_thread = "/direct/t/" in url

            composer_results = {}
            for selector in _COMPOSER_SELECTORS:
                try:
                    # check if it exists in DOM at all
                    count = page.locator(selector).count()
                    visible = page.locator(selector).first.is_visible() if count > 0 else False
                    composer_results[selector] = {"count": count, "visible": visible}
                except Exception as e:
                    composer_results[selector] = {"error": str(e)}

            message_results = {}
            for selector in _MESSAGE_NODE_SELECTORS:
                try:
                    count = page.locator(selector).count()
                    visible = page.locator(selector).first.is_visible() if count > 0 else False
                    message_results[selector] = {"count": count, "visible": visible}
                except Exception as e:
                    message_results[selector] = {"error": str(e)}

            logger.info(
                "PlaywrightDM diagnostic_probe label=%s url=%s in_inbox=%s in_thread=%s has_composer=%s composer_details=%s message_details=%s",
                label, url, in_inbox, in_thread,
                any(d.get("visible") for d in composer_results.values() if isinstance(d, dict)),
                composer_results, message_results
            )
        except Exception as e:
            logger.error("Error in PlaywrightDM diagnostic_probe: %s", e)

    def _wait_thread_open(self, page: Page, timeout_ms: int = 6000) -> bool:
        """
        Espera sincronizacion visual completa del thread:
        composer visible + panel de mensajes visible.
        """
        visual_ok, state = self._wait_for_visual_thread_sync(page, timeout_ms=timeout_ms)
        found_composer = bool(state.get("composer_visible"))
        message_panel_visible = bool(state.get("message_panel_visible"))
        current_url = page.url or ""
        is_in_thread = bool(re.search(r"/direct/t/([^/]+)", current_url))

        if _DM_VERBOSE_PROBES:
            print(style_text(f"[Probe] URL = {current_url}", color=Fore.WHITE))
            print(style_text(f"[Probe] thread_abierto = {is_in_thread and found_composer and message_panel_visible}", color=Fore.WHITE))
            print(style_text(f"[Probe] existe_composer = {found_composer}", color=Fore.WHITE))
            print(style_text(f"[Probe] message_panel_visible = {message_panel_visible}", color=Fore.WHITE))

        return bool(visual_ok and is_in_thread and found_composer and message_panel_visible)

    def _open_thread_by_cache(
        self,
        thread: ThreadLike,
        *,
        visual_timeout_ms: Optional[int] = None,
        network_timeout_ms: Optional[int] = None,
        row_stale: bool = False,
    ) -> bool:
        page = self._ensure_page()
        meta = self._thread_cache_meta.get(thread.id)
        if not meta:
            logger.error(
                "PlaywrightDM open_thread_cache_missing account=@%s thread_id=%s",
                self.username,
                thread.id,
            )
            return False

        title = (meta.get("title") or "").strip()
        peer = (meta.get("peer_username") or "").strip()
        thread_title = str(getattr(thread, "title", "") or "").strip()
        candidates = [title, peer, thread_title]
        candidates = [c for c in candidates if c]
        if not candidates:
            logger.error(
                "PlaywrightDM open_thread_cache_candidates_empty account=@%s thread_id=%s",
                self.username,
                thread.id,
            )
            return False
        candidate_norms = {_normalize_key_source(item) for item in candidates}
        candidate_norms.discard("")
        expected_for_click = list(self._expected_thread_ids(thread, click_href=""))
        meta_link_id = _extract_thread_id(str(meta.get("link") or ""))
        if meta_link_id and meta_link_id not in expected_for_click:
            expected_for_click.append(meta_link_id)
        expected_thread_ids = set(self._expand_thread_ids(expected_for_click))

        current_url = page.url or ""
        if not self._is_in_direct_workspace(page):
            self._open_inbox()
        elif "/direct/t/" in current_url and not self._has_thread_rows_visible(page):
            self.return_to_inbox()

        selector_candidates = [
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']",
            "div[role='navigation'] div[role='button'][tabindex='0']",
            THREAD_ROW_SELECTOR,
            "div[role='main'] div[role='list'] a[href*='/direct/t/']",
            "div[role='main'] div[role='listitem']",
            "div[role='main'] div[role='row']",
            "div[role='main'] div[role='button'][tabindex='0']",
        ]

        rows = None
        selected_selector = ""
        for selector in selector_candidates:
            try:
                candidate = page.locator(selector)
                if candidate.count() > 0:
                    rows = candidate
                    selected_selector = selector
                    break
            except Exception:
                continue
        if rows is None:
            logger.error(
                "PlaywrightDM open_thread_cache_no_rows account=@%s thread_id=%s",
                self.username,
                thread.id,
            )
            return False

        inbox_panel, _method, _selector, _panel_counts = self._get_inbox_panel(page, rows=rows)
        self._scroll_panel_to_top(inbox_panel)
        target_scan = max(25, len(self._thread_cache_meta) + 10)
        max_scroll_passes = max(25, min(2000, target_scan * 4))
        stagnant_limit = self._stagnation_limit(target_scan)

        scanned = 0
        stagnant_passes = 0
        for _pass in range(max_scroll_passes):
            try:
                total = rows.count()
            except Exception:
                total = 0
            matched_in_pass = False
            for idx in range(total):
                row = rows.nth(idx)
                try:
                    row_valid = self._row_is_valid(row, selector=selected_selector, fast=True)
                except TypeError:
                    row_valid = self._row_is_valid(row, selector=selected_selector)
                if not row_valid:
                    continue
                try:
                    row_lines = self._row_lines(row)
                except Exception:
                    continue
                row_title = (row_lines[0] if row_lines else "").strip()
                row_snippet = " | ".join(
                    (line or "").strip() for line in row_lines[1:4] if (line or "").strip()
                )
                row_title_norm = _normalize_key_source(row_title)
                text_norm = _normalize_key_source(" ".join(line for line in row_lines if line))
                row_thread_key, _row_key_source, row_link = self._resolve_thread_key(
                    page,
                    row,
                    title=row_title,
                    peer_username=row_title,
                    snippet=row_snippet,
                )
                row_ids = set(
                    self._expand_thread_ids([row_thread_key, _extract_thread_id(row_link)])
                )
                scanned += 1
                id_match = bool(expected_thread_ids and row_ids and expected_thread_ids.intersection(row_ids))
                title_match = (
                    bool(row_title_norm)
                    and bool(candidate_norms)
                    and any(
                        row_title_norm == candidate
                        or row_title_norm in candidate
                        or candidate in row_title_norm
                        for candidate in candidate_norms
                    )
                )
                text_match = (
                    bool(text_norm)
                    and bool(candidate_norms)
                    and any(
                        len(candidate) >= 3 and candidate in text_norm
                        for candidate in candidate_norms
                    )
                )
                if not id_match and not title_match and not text_match:
                    continue

                matched_in_pass = True
                row_preview = self._row_preview(row)
                pre_url = page.url or ""
                baseline_counts = self._snapshot_api_counts(
                    expected_for_click
                )
                clicked, click_href = self._click_row_target(
                    row,
                    selector=selected_selector,
                    idx=idx,
                )
                if not clicked:
                    continue
                if self._validate_open_state(
                    thread,
                    pre_url=pre_url,
                    selector=selected_selector,
                    idx=idx,
                    row_preview=row_preview,
                    click_href=click_href,
                    baseline_counts=baseline_counts,
                    visual_timeout_ms=visual_timeout_ms,
                    network_timeout_ms=network_timeout_ms,
                    row_stale=row_stale,
                ):
                    return True
                if not self._is_in_direct_workspace(page):
                    self.return_to_inbox()

            moved = self._scroll_panel_down(inbox_panel)
            if not moved:
                break
            if matched_in_pass:
                stagnant_passes = 0
            else:
                stagnant_passes += 1
            if stagnant_passes >= stagnant_limit:
                break
            self._wait_for_scroll_settle(page, extra_ms=20)

        logger.error(
            "PlaywrightDM open_thread_cache_failed account=@%s thread_id=%s scanned=%s",
            self.username,
            thread.id,
            scanned,
        )
        return False


    def _find_composer(self, page: Page):
        for selector in _COMPOSER_SELECTORS:
            try:
                loc = page.locator(selector)
                total = loc.count()
            except Exception:
                continue
            if total <= 0:
                continue
            for idx in range(min(total, 5)):
                candidate = loc.nth(idx)
                try:
                    if not candidate.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    is_editable = bool(
                        candidate.evaluate(
                            "el => !!el && ("
                            "String((el && el.tagName) || '').toLowerCase() === 'textarea' || "
                            "!!el.isContentEditable || "
                            "String((el.getAttribute && el.getAttribute('contenteditable')) || '').toLowerCase() === 'true'"
                            ")"
                        )
                    )
                except Exception:
                    is_editable = True
                if not is_editable:
                    continue
                return candidate
        return None

    def _click_send_button(self, page: Page) -> bool:
        for selector in _SEND_BUTTON_SELECTORS:
            try:
                loc = page.locator(selector)
                total = loc.count()
                if total <= 0:
                    continue
                for idx in range(min(total, 3)):
                    btn = loc.nth(idx)
                    try:
                        if not btn.is_visible():
                            continue
                    except Exception:
                        continue
                    try:
                        if btn.is_disabled():
                            continue
                    except Exception:
                        pass
                    try:
                        btn.click(timeout=1_500)
                        return True
                    except Exception:
                        btn.click(timeout=1_500, force=True)
                        return True
            except Exception:
                continue
        return False

    def _refresh_thread_participants(self, page: Page, thread: ThreadLike) -> None:
        username = _extract_header_username(page, self.username)
        if not username:
            return
        if any(u.username == username for u in thread.users):
            return
        thread.users = [UserLike(pk=username, id=username, username=username)]
        self._thread_cache[thread.id] = thread

    def _assert_logged_in(self, page: Page) -> None:
        url = (page.url or "").lower()
        if any(token in url for token in ("/accounts/login", "/challenge/", "/checkpoint/", "two_factor")):
            raise RuntimeError(f"Login requerido o checkpoint detectado para @{self.username}. URL={page.url}")
        try:
            if page.locator("input[name='username'], input[name='password']").count():
                raise RuntimeError(f"Login requerido para @{self.username}.")
        except RuntimeError:
            raise
        except Exception:
            return

    def _is_outbound(self, node) -> bool:
        try:
            if node.locator("[data-testid='own']").count():
                return True
        except Exception:
            pass

        try:
            box = node.bounding_box()
            if not box:
                return False
            viewport = self._page.viewport_size if self._page is not None else None
            if viewport and viewport.get("width"):
                return (box["x"] + box["width"]) > (viewport["width"] * 0.6)
        except Exception:
            pass
        return False

    def _node_has_audio_payload(self, node) -> bool:
        for selector in (
            "audio",
            "source[type*='audio']",
            "[data-testid*='audio']",
            "[aria-label*='audio']",
            "[aria-label*='Audio']",
            "a[href*='.mp3']",
            "a[href*='.m4a']",
            "a[href*='.wav']",
            "a[href*='.ogg']",
        ):
            try:
                if node.locator(selector).count() > 0:
                    return True
            except Exception:
                continue
        text_value = ""
        try:
            text_value = (node.inner_text() or "").strip().lower()
        except Exception:
            text_value = ""
        if not text_value:
            return False
        return any(
            token in text_value
            for token in (
                "mensaje de voz",
                "nota de voz",
                "voice message",
                "audio message",
            )
        )

    def _node_has_upload_in_progress(self, node) -> bool:
        for selector in (
            "[role='progressbar']",
            "progress",
            "[aria-busy='true']",
            "[data-testid*='progress']",
            "[data-testid*='upload']",
            "[aria-label*='Uploading']",
            "[aria-label*='Subiendo']",
            "[aria-label*='Procesando']",
            "[aria-label*='Sending']",
        ):
            try:
                if node.locator(selector).count() > 0:
                    return True
            except Exception:
                continue
        text_value = ""
        try:
            text_value = (node.inner_text() or "").strip().lower()
        except Exception:
            text_value = ""
        if not text_value:
            return False
        return any(
            token in text_value
            for token in (
                "upload",
                "subiendo",
                "enviando",
                "processing",
                "procesando",
            )
        )

    def _latest_outbound_snapshot(self, page: Page) -> dict[str, object]:
        snapshot: dict[str, object] = {
            "found": False,
            "signature": "",
            "message_id": "",
            "is_audio": False,
            "uploading": False,
        }
        try:
            nodes = self._collect_message_nodes(page)
            total_nodes = nodes.count()
        except Exception:
            return snapshot
        if total_nodes <= 0:
            return snapshot
        for idx in range(total_nodes - 1, -1, -1):
            node = nodes.nth(idx)
            try:
                if not self._is_outbound(node):
                    continue
            except Exception:
                continue
            message_id = _extract_message_id(node)
            text_value = _normalize_message_text(_extract_message_text(node))
            is_audio = self._node_has_audio_payload(node)
            uploading = self._node_has_upload_in_progress(node)
            signature = f"{idx}|{message_id}|{text_value[:80]}|audio={int(is_audio)}|up={int(uploading)}"
            snapshot = {
                "found": True,
                "signature": signature,
                "message_id": message_id,
                "is_audio": is_audio,
                "uploading": uploading,
            }
            return snapshot
        return snapshot

    def _verify_audio_sent(
        self,
        thread: ThreadLike,
        page: Page,
        *,
        previous_signature: str,
    ) -> Optional[str]:
        timeout_seconds = max(0.1, float(_DM_AUDIO_VERIFY_TIMEOUT_MS) / 1000.0)
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            snapshot = self._latest_outbound_snapshot(page)
            if bool(snapshot.get("found", False)):
                signature = str(snapshot.get("signature") or "")
                is_new_outbound = bool(signature) and signature != previous_signature
                if (is_new_outbound or not previous_signature) and bool(snapshot.get("is_audio", False)):
                    if not bool(snapshot.get("uploading", False)):
                        message_id = str(snapshot.get("message_id") or "").strip()
                        if message_id:
                            return message_id
                        return _hash_message_id(thread.id, self.user_id, "__audio__", time.time())
            try:
                page.wait_for_timeout(_DM_AUDIO_VERIFY_POLL_MS)
            except Exception:
                break
        return None

    def _verify_sent(self, thread: ThreadLike, text: str) -> Optional[str]:
        deadline = time.time() + VERIFY_TIMEOUT_S
        target_text = _normalize_message_text(text)
        page = self._ensure_page()
        poll_count = 0
        while time.time() < deadline:
            poll_count += 1
            # Camino rapido: revisar solo los ultimos nodos del DOM sin reabrir el thread.
            try:
                nodes = self._collect_message_nodes(page)
                total_nodes = nodes.count()
                for idx in range(max(0, total_nodes - 12), total_nodes):
                    node = nodes.nth(idx)
                    node_text = _extract_message_text(node)
                    if not _message_text_matches(target_text, node_text):
                        continue
                    try:
                        if self._is_outbound(node):
                            node_id = _extract_message_id(node)
                            if node_id:
                                return node_id
                            return _hash_message_id(thread.id, self.user_id, node_text, time.time())
                    except Exception:
                        # Si no se puede determinar direccion, aceptar match exacto como exito.
                        if _normalize_message_text(node_text) == target_text:
                            node_id = _extract_message_id(node)
                            if node_id:
                                return node_id
                            return _hash_message_id(thread.id, self.user_id, node_text, time.time())
            except Exception:
                pass
            # Camino robusto cada cierto numero de polls: parseo normal de mensajes.
            if poll_count % 3 == 0:
                try:
                    messages = self.get_messages(thread, amount=20, log=False)
                    for msg in messages:
                        if _message_text_matches(target_text, msg.text):
                            return msg.id
                except Exception:
                    pass
            time.sleep(0.25)
        try:
            messages = self.get_messages(thread, amount=20, log=False)
            last_out = next((m for m in messages if m.user_id == self.user_id), None)
            last_in = next((m for m in messages if m.user_id != self.user_id), None)
            logger.warning(
                "PlaywrightDM verify_fail thread=%s peer=%s last_in_ts=%s last_out_ts=%s",
                thread.id,
                _thread_peer_id(thread, self.user_id),
                _fmt_ts(last_in.timestamp if last_in else None),
                _fmt_ts(last_out.timestamp if last_out else None),
            )
        except Exception:
            pass
        return None

    def debug_dump_inbox(self, reason: str) -> str:
        page = self._ensure_page()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_dir = logs_root(Path(__file__).resolve().parents[1])
        log_dir.mkdir(parents=True, exist_ok=True)
        base = log_dir / f"{DM_DEBUG_DIRNAME}_{self.username}_{timestamp}"
        try:
            logger.info("PlaywrightDM debug_dump reason=%s url=%s", reason, page.url)
        except Exception:
            pass
        # Selectores mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimos para debugging
        debug_selectors = (
            "div[role='main'] div[role='listitem']",
            "div[role='main'] div[role='row']",
            "div[role='button'][tabindex='0']",
        )
        for selector in debug_selectors:
            try:
                count = page.locator(selector).count()
            except Exception:
                count = 0
            logger.info("PlaywrightDM selector_count selector=%s count=%s", selector, count)
        try:
            page.screenshot(path=str(base) + ".png", full_page=True)
        except Exception:
            pass
        try:
            html = page.content()
            atomic_write_text(Path(str(base) + ".html"), html)
        except Exception:
            pass
        try:
            main_text = page.locator("div[role='main']").inner_text()
            atomic_write_text(Path(str(base) + ".txt"), main_text)
        except Exception:
            pass
        return str(base)


_DM_RESPONSE_URL_HINTS = (
    "/api/graphql",
    "/api/graphql/",
    "/graphql/query",
    "/api/v1/direct_v2/",
    "/api/v1/direct/",
    "/direct_v2/",
)
_DM_THREAD_ID_KEYS = (
    "thread_id",
    "thread_v2_id",
    "thread_pk",
    "threadid",
    "conversation_id",
    "conversationid",
    "thread_key",
)
_DM_SENDER_ID_KEYS = (
    "sender_id",
    "senderid",
    "user_id",
    "userid",
    "actor_id",
    "profile_id",
)
_DM_ITEM_ID_KEYS = (
    "item_id",
    "itemid",
    "message_id",
    "messageid",
    "pk",
    "id",
)
_DM_TIMESTAMP_KEYS = (
    "timestamp_ms",
    "timestamp",
    "created_at_ms",
    "created_at",
    "client_timestamp",
    "server_timestamp_ms",
    "server_timestamp",
)


def _is_message_api_url(url: str) -> bool:
    lowered = str(url or "").strip().lower()
    if not lowered:
        return False
    if "/api/graphql" in lowered:
        return True
    if "/graphql/query" in lowered:
        return True
    return any(hint in lowered for hint in _DM_RESPONSE_URL_HINTS if hint not in {"/api/graphql/", "/api/graphql"})


def _build_inbox_endpoint_candidates(
    *,
    cursor: str,
    limit: int,
    message_limit: int,
) -> list[str]:
    safe_limit = max(1, min(200, int(limit or 20)))
    safe_message_limit = max(1, min(80, int(message_limit or 20)))
    cursor_value = str(cursor or "").strip()

    variants: list[dict[str, str]] = [
        {
            "limit": str(safe_limit),
            "thread_message_limit": str(safe_message_limit),
        },
        {
            "limit": str(safe_limit),
            "thread_message_limit": str(safe_message_limit),
            "persistentBadging": "true",
            "visual_message_return_type": "unseen",
        },
        {
            "limit": str(safe_limit),
            "thread_message_limit": str(safe_message_limit),
            "folder": "",
        },
    ]
    if cursor_value:
        for params in variants:
            params["cursor"] = cursor_value

    endpoints: list[str] = []
    endpoint_bases = (
        "/api/v1/direct_v2/inbox/",
        "/api/v1/direct_v2/inbox",
        "/api/v1/direct_v2/threads/",
    )
    for params in variants:
        query = urlencode(params, doseq=True)
        for base in endpoint_bases:
            url = f"{base}?{query}" if query else base
            if url not in endpoints:
                endpoints.append(url)
    return endpoints


def _extract_inbox_cursor(payload: Any) -> tuple[str, bool]:
    cursor_value = ""
    has_more_value: Optional[bool] = None
    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
            continue
        if not isinstance(current, dict):
            continue

        for key in ("oldest_cursor", "next_cursor", "end_cursor", "cursor", "max_id"):
            if key not in current:
                continue
            value = _coerce_str(current.get(key))
            if value:
                cursor_value = value
                break

        for key in (
            "has_older",
            "has_older_threads",
            "has_next_page",
            "has_next",
            "has_more",
            "more_available",
        ):
            if key not in current:
                continue
            raw = current.get(key)
            if isinstance(raw, bool):
                has_more_value = raw if has_more_value is None else (has_more_value or raw)
            else:
                lowered = _coerce_str(raw).lower()
                if lowered in {"1", "true", "yes", "si", "on"}:
                    has_more_value = True if has_more_value is None else has_more_value or True
                elif lowered in {"0", "false", "no", "off"} and has_more_value is None:
                    has_more_value = False

        for value in current.values():
            if isinstance(value, (dict, list)):
                stack.append(value)

    if has_more_value is None:
        has_more_value = bool(cursor_value)
    return cursor_value, bool(has_more_value)


def _extract_messages_from_thread_node(
    thread_node: dict[str, Any],
    *,
    thread_id: str,
    self_user_id: str,
    message_limit: int,
) -> list[dict[str, Any]]:
    safe_message_limit = max(1, min(80, int(message_limit or 20)))
    self_user_ids = {str(self_user_id or "").strip()}
    self_user_ids.discard("")
    candidates: list[dict[str, Any]] = []

    for key in ("items", "thread_items", "messages", "entries"):
        raw = thread_node.get(key)
        if not isinstance(raw, list):
            continue
        for item in raw[: safe_message_limit * 3]:
            if isinstance(item, dict):
                candidates.append(item)

    for key in ("last_permanent_item", "last_item", "last_message"):
        raw = thread_node.get(key)
        if isinstance(raw, dict):
            candidates.append(raw)

    dedup: dict[str, dict[str, Any]] = {}
    for node in candidates:
        parsed, _missing = _extract_api_message_from_node(
            node,
            context_thread_id=thread_id,
            self_user_id=self_user_id,
            self_user_ids=self_user_ids,
        )
        if parsed is None or parsed.timestamp is None:
            continue
        msg_id = str(parsed.item_id or "").strip()
        if not msg_id:
            continue
        direction_value = str(parsed.direction or "").strip().lower()
        if direction_value == "outbound":
            direction = "outbound"
        elif direction_value == "inbound":
            direction = "inbound"
        else:
            direction = "unknown"
        normalized = {
            "message_id": msg_id,
            "sender_id": str(parsed.sender_id or "").strip(),
            "direction": direction,
            "text": str(parsed.text or ""),
            "timestamp_epoch": float(parsed.timestamp),
        }
        previous = dedup.get(msg_id)
        if previous is None:
            dedup[msg_id] = normalized
            continue
        prev_ts = float(previous.get("timestamp_epoch") or 0.0)
        if float(normalized.get("timestamp_epoch") or 0.0) >= prev_ts:
            dedup[msg_id] = normalized

    messages = list(dedup.values())
    messages.sort(
        key=lambda msg: (
            float(msg.get("timestamp_epoch") or 0.0),
            str(msg.get("message_id") or ""),
        ),
        reverse=True,
    )
    return messages[:safe_message_limit]


def _extract_inbox_threads_from_payload(
    payload: Any,
    *,
    self_user_id: str,
    self_username: str,
    message_limit: int = 20,
    thread_limit: int = 0,
) -> list[dict[str, Any]]:
    safe_message_limit = max(1, min(80, int(message_limit or 20)))
    safe_thread_limit = max(0, int(thread_limit or 0))

    thread_nodes: dict[str, dict[str, Any]] = {}
    thread_scores: dict[str, int] = {}
    for node, _context_thread_id in _iter_payload_nodes(payload):
        thread_id = _extract_thread_id_from_node(node)
        if not thread_id:
            continue
        score = 0
        for key in (
            "users",
            "participants",
            "items",
            "thread_title",
            "title",
            "thread_name",
            "snippet",
            "unread_count",
            "viewer_unseen_count",
            "last_activity_at",
            "last_activity_at_ms",
            "last_permanent_item",
        ):
            if key in node:
                score += 1
        if score <= 0:
            continue
        previous_score = thread_scores.get(thread_id, -1)
        if score >= previous_score:
            thread_scores[thread_id] = score
            thread_nodes[thread_id] = node

    self_id_value = str(self_user_id or "").strip()
    self_username_norm = str(self_username or "").strip().lower()

    snapshots: list[dict[str, Any]] = []
    seen_snapshot_ids: set[str] = set()
    thread_ids = list(thread_nodes.keys())
    for thread_id in thread_ids:
        if not thread_id or thread_id in seen_snapshot_ids:
            continue
        seen_snapshot_ids.add(thread_id)
        node = thread_nodes.get(thread_id, {})

        participants_raw = node.get("users")
        if not isinstance(participants_raw, list):
            participants_raw = node.get("participants")
        participants: list[dict[str, str]] = []
        if isinstance(participants_raw, list):
            for participant in participants_raw:
                if not isinstance(participant, dict):
                    continue
                participant_id = _coerce_str(
                    participant.get("pk")
                    or participant.get("id")
                    or participant.get("user_id")
                    or participant.get("interop_messaging_user_fbid")
                )
                participant_username = _coerce_str(
                    participant.get("username")
                    or participant.get("full_name")
                    or participant.get("name")
                    or participant_id
                )
                if not participant_id and participant_username:
                    participant_id = participant_username
                if not participant_username and participant_id:
                    participant_username = participant_id
                if participant_id or participant_username:
                    participants.append(
                        {
                            "id": participant_id,
                            "username": participant_username,
                        }
                    )

        recipient_id = ""
        recipient_username = ""
        for participant in participants:
            participant_id = _coerce_str(participant.get("id"))
            participant_username = _coerce_str(participant.get("username"))
            participant_username_norm = participant_username.lower()
            if participant_id and self_id_value and participant_id == self_id_value:
                continue
            if participant_username_norm and self_username_norm and participant_username_norm == self_username_norm:
                continue
            recipient_id = participant_id or participant_username
            recipient_username = participant_username or participant_id
            if recipient_id or recipient_username:
                break

        title = _coerce_str(
            node.get("thread_title")
            or node.get("title")
            or node.get("thread_name")
            or node.get("thread_label")
        )
        if not recipient_username:
            recipient_username = title
        if not recipient_id:
            recipient_id = recipient_username
        if not title:
            title = recipient_username or thread_id

        unread_int = 0
        for key in ("unread_count", "viewer_unseen_count", "pending_count", "unseen_count"):
            if key not in node:
                continue
            value = node.get(key)
            try:
                unread_int = max(unread_int, int(value))
            except Exception:
                continue

        snippet = _coerce_str(
            node.get("snippet")
            or node.get("thread_preview")
            or node.get("thread_snippet")
            or node.get("preview")
        )
        if not snippet:
            last_item = node.get("last_permanent_item")
            if isinstance(last_item, dict):
                snippet = _extract_message_text_from_api_node(last_item)

        messages = _extract_messages_from_thread_node(
            node,
            thread_id=thread_id,
            self_user_id=self_user_id,
            message_limit=safe_message_limit,
        )
        if not snippet and messages:
            snippet = _coerce_str(messages[0].get("text"))

        thread_href = _extract_thread_href_from_node(node)
        thread_id_from_href = _extract_thread_id(thread_href)
        if not _is_probably_web_thread_id(thread_id_from_href):
            thread_id_from_href = ""
            thread_href = ""
        thread_id_real = thread_id_from_href if thread_id_from_href else (thread_id if _is_probably_web_thread_id(thread_id) else "")
        canonical_thread_id = thread_id_real or thread_id

        activity_ts_values: list[float] = []
        for key in (
            "last_activity_at_ms",
            "last_activity_at",
            "last_activity_timestamp",
            "updated_at",
            "latest_activity",
        ):
            value = node.get(key)
            if isinstance(value, dict):
                for nested in value.values():
                    ts_value = _coerce_timestamp_seconds(nested)
                    if ts_value is not None:
                        activity_ts_values.append(ts_value)
            else:
                ts_value = _coerce_timestamp_seconds(value)
                if ts_value is not None:
                    activity_ts_values.append(ts_value)
        for msg in messages[:safe_message_limit]:
            ts_value = _coerce_timestamp_seconds(msg.get("timestamp_epoch"))
            if ts_value is not None:
                activity_ts_values.append(ts_value)
        last_activity_ts = max(activity_ts_values) if activity_ts_values else None

        snapshots.append(
            {
                "thread_id": canonical_thread_id,
                "thread_id_api": thread_id,
                "thread_id_real": thread_id_real,
                "thread_href": thread_href,
                "recipient_id": recipient_id or recipient_username or thread_id,
                "recipient_username": recipient_username or title or "unknown",
                "title": title or recipient_username or "unknown",
                "snippet": snippet or "",
                "unread_count": unread_int,
                "last_activity_at": last_activity_ts,
                "messages": messages[:safe_message_limit],
            }
        )

    snapshots.sort(
        key=lambda snap: (
            float(snap.get("last_activity_at") or 0.0),
            str(snap.get("thread_id") or ""),
        ),
        reverse=True,
    )
    if safe_thread_limit > 0:
        snapshots = snapshots[:safe_thread_limit]
    return snapshots


def _extract_api_messages_from_payload(
    payload: Any,
    *,
    self_user_id: str,
) -> tuple[list[_APIMessageRecord], list[dict[str, str]]]:
    messages: list[_APIMessageRecord] = []
    missing_timestamp: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    if isinstance(payload, dict):
        direct_thread_id = _extract_thread_id_from_node(payload)
        direct_items = payload.get("items")
        if direct_thread_id and isinstance(direct_items, list):
            self_user_ids = {str(self_user_id or "").strip()}
            viewer = payload.get("viewer")
            if isinstance(viewer, dict):
                for key in (
                    "interop_messaging_user_fbid",
                    "id",
                    "pk",
                    "user_id",
                    "viewer_id",
                ):
                    value = _coerce_str(viewer.get(key))
                    if value:
                        self_user_ids.add(value)
            for key in ("viewer_id", "viewer_fbid", "viewer_pk", "viewer_user_id"):
                value = _coerce_str(payload.get(key))
                if value:
                    self_user_ids.add(value)
            self_user_ids.discard("")
            for item in direct_items:
                if not isinstance(item, dict):
                    continue
                parsed, missing = _extract_api_message_from_node(
                    item,
                    context_thread_id=direct_thread_id,
                    self_user_id=self_user_id,
                    self_user_ids=self_user_ids,
                )
                if parsed is not None:
                    dedup_key = (parsed.thread_id, parsed.item_id)
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)
                    messages.append(parsed)
                elif missing is not None:
                    missing_timestamp.append(missing)
            return messages, missing_timestamp

    self_user_ids = {str(self_user_id or "").strip()}
    self_user_ids.update(_extract_payload_self_user_ids(payload))
    self_user_ids.discard("")

    for node, context_thread_id in _iter_payload_nodes(payload):
        parsed, missing = _extract_api_message_from_node(
            node,
            context_thread_id=context_thread_id,
            self_user_id=self_user_id,
            self_user_ids=self_user_ids,
        )
        if parsed is not None:
            dedup_key = (parsed.thread_id, parsed.item_id)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            messages.append(parsed)
        elif missing is not None:
            missing_timestamp.append(missing)
    return messages, missing_timestamp


def _payload_thread_ids(payload: Any, *, self_user_id: str) -> set[str]:
    parsed, missing = _extract_api_messages_from_payload(payload, self_user_id=self_user_id)
    ids: set[str] = set()
    for item in parsed:
        thread_id = str(item.thread_id or "").strip()
        if thread_id:
            ids.add(thread_id)
    for item in missing:
        thread_id = str(item.get("thread_id") or "").strip()
        if thread_id:
            ids.add(thread_id)
    for alias_pair in _extract_payload_thread_alias_pairs(payload):
        for alias in alias_pair:
            value = str(alias or "").strip()
            if value:
                ids.add(value)
    return ids


def _extract_payload_thread_alias_pairs(payload: Any) -> set[tuple[str, str]]:
    alias_pairs: set[tuple[str, str]] = set()
    for node, _context_thread_id in _iter_payload_nodes(payload):
        thread_id = _coerce_str(node.get("thread_id") or node.get("thread_v2_id") or node.get("thread_pk"))
        thread_key = _coerce_str(node.get("thread_key"))
        if thread_id and thread_key and thread_id != thread_key:
            alias_pairs.add((thread_id, thread_key))

        nested_thread = node.get("thread")
        if isinstance(nested_thread, dict):
            nested_thread_id = _coerce_str(
                nested_thread.get("thread_id")
                or nested_thread.get("thread_v2_id")
                or nested_thread.get("thread_pk")
                or nested_thread.get("id")
            )
            nested_thread_key = _coerce_str(nested_thread.get("thread_key"))
            if nested_thread_id and nested_thread_key and nested_thread_id != nested_thread_key:
                alias_pairs.add((nested_thread_id, nested_thread_key))
    return alias_pairs


def _iter_payload_nodes(payload: Any) -> Iterator[tuple[dict[str, Any], str]]:
    stack: list[tuple[Any, str]] = [(payload, "")]
    while stack:
        current, thread_context = stack.pop()
        if isinstance(current, list):
            for item in reversed(current):
                if isinstance(item, (dict, list)):
                    stack.append((item, thread_context))
            continue
        if not isinstance(current, dict):
            continue
        mapped = {str(key): value for key, value in current.items()}
        next_thread = _extract_thread_id_from_node(mapped) or thread_context
        yield mapped, next_thread
        for value in mapped.values():
            if isinstance(value, (dict, list)):
                stack.append((value, next_thread))


def _extract_api_message_from_node(
    node: dict[str, Any],
    *,
    context_thread_id: str,
    self_user_id: str,
    self_user_ids: set[str],
) -> tuple[Optional[_APIMessageRecord], Optional[dict[str, str]]]:
    thread_id = _extract_thread_id_from_node(node) or str(context_thread_id or "").strip()
    sender_id = _extract_sender_id_from_node(node)
    explicit_item_id = _extract_explicit_item_id_from_node(node)
    item_id = explicit_item_id or _extract_item_id_from_node(node)
    timestamp = _extract_timestamp_from_node(node)
    text = _extract_message_text_from_api_node(node)
    has_sender_hint = bool(sender_id or any(key in node for key in _DM_SENDER_ID_KEYS + ("sender", "user", "actor", "from")))
    raw_direction = _coerce_str(
        node.get("direction")
        or node.get("message_direction")
        or node.get("folder")
        or node.get("type")
    ).lower()
    has_direction_hint = raw_direction in {
        "outbound",
        "outgoing",
        "sent",
        "viewer",
        "inbound",
        "incoming",
        "received",
    } or any(
        isinstance(node.get(key), bool)
        for key in ("is_sent_by_viewer", "sent_by_viewer", "is_outgoing", "outgoing", "from_viewer")
    )
    raw_message_kind = _coerce_str(node.get("item_type") or node.get("message_type")).lower()
    has_message_kind = bool(raw_message_kind and raw_message_kind not in {
        "thread",
        "inbox",
        "conversation",
        "container",
        "list",
        "node",
    })
    has_client_context = bool(_coerce_str(node.get("client_context")))
    has_text = bool(str(text or "").strip())

    message_identity = bool(
        has_text
        or bool(explicit_item_id)
        or has_message_kind
        or has_client_context
    )
    if not message_identity:
        return None, None
    if not has_sender_hint and not has_direction_hint:
        return None, None
    if not thread_id:
        return None, None
    if timestamp is None:
        has_timestamp_key = any(key in node for key in _DM_TIMESTAMP_KEYS)
        if has_timestamp_key or has_sender_hint:
            return None, {
                "thread_id": thread_id,
                "item_id": item_id,
                "sender_id": sender_id,
            }
        return None, None

    direction = _resolve_direction_from_node(
        node,
        sender_id=sender_id,
        self_user_id=self_user_id,
        self_user_ids=self_user_ids,
    )
    normalized_sender_id = sender_id
    if direction == "outbound":
        normalized_sender_id = str(self_user_id or "").strip() or normalized_sender_id
    elif direction == "inbound" and not normalized_sender_id:
        normalized_sender_id = "peer"
    normalized_item_id = item_id
    if not normalized_item_id:
        seed = f"{thread_id}|{normalized_sender_id}|{timestamp}|{text}".encode(
            "utf-8",
            errors="ignore",
        )
        normalized_item_id = hashlib.sha1(seed).hexdigest()[:20]
    return (
        _APIMessageRecord(
            thread_id=thread_id,
            sender_id=normalized_sender_id,
            timestamp=timestamp,
            item_id=normalized_item_id,
            direction=direction,
            text=text,
        ),
        None,
    )


def _is_probably_web_thread_id(value: str) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    if not token.isdigit():
        return False
    return 6 <= len(token) <= 20


def _prefer_thread_id_candidate(candidates: list[str]) -> str:
    cleaned = [str(item or "").strip() for item in candidates if str(item or "").strip()]
    if not cleaned:
        return ""
    for item in cleaned:
        if _is_probably_web_thread_id(item):
            return item
    return cleaned[0]


def _extract_thread_href_from_node(node: dict[str, Any]) -> str:
    keys = (
        "thread_url",
        "thread_href",
        "thread_link",
        "canonical_url",
        "permalink",
        "url",
        "path",
        "link",
    )
    for key in keys:
        value = _coerce_str(node.get(key))
        if "/direct/t/" in value:
            return _normalize_direct_link(value)

    for nested_key in ("thread", "conversation", "entity"):
        nested = node.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for key in keys:
            value = _coerce_str(nested.get(key))
            if "/direct/t/" in value:
                return _normalize_direct_link(value)
    return ""


def _extract_thread_id_from_node(node: dict[str, Any]) -> str:
    candidates: list[str] = []
    for key in _DM_THREAD_ID_KEYS:
        value = _coerce_str(node.get(key))
        if value:
            candidates.append(value)
    for nested_key in ("thread", "conversation"):
        nested = node.get(nested_key)
        if isinstance(nested, dict):
            for key in _DM_THREAD_ID_KEYS + ("id",):
                value = _coerce_str(nested.get(key))
                if value:
                    candidates.append(value)
    href_value = _extract_thread_href_from_node(node)
    href_thread_id = _extract_thread_id(href_value)
    if href_thread_id:
        candidates.insert(0, href_thread_id)
    return _prefer_thread_id_candidate(candidates)


def _extract_sender_id_from_node(node: dict[str, Any]) -> str:
    for key in _DM_SENDER_ID_KEYS:
        value = _coerce_str(node.get(key))
        if value:
            return value
    for nested_key in ("sender", "user", "actor", "from"):
        nested = node.get(nested_key)
        if not isinstance(nested, dict):
            continue
        value = _coerce_str(nested.get("id") or nested.get("pk") or nested.get("user_id"))
        if value:
            return value
    return ""


def _extract_item_id_from_node(node: dict[str, Any]) -> str:
    for key in _DM_ITEM_ID_KEYS:
        value = _coerce_str(node.get(key))
        if value:
            return value
    return ""


def _extract_explicit_item_id_from_node(node: dict[str, Any]) -> str:
    for key in ("item_id", "itemid", "message_id", "messageid"):
        value = _coerce_str(node.get(key))
        if value:
            return value
    return ""


def _extract_timestamp_from_node(node: dict[str, Any]) -> Optional[float]:
    for key in _DM_TIMESTAMP_KEYS:
        if key not in node:
            continue
        value = node.get(key)
        timestamp = _coerce_timestamp_seconds(value)
        if timestamp is not None:
            return timestamp
    return None


def _coerce_timestamp_seconds(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    numeric: Optional[float] = None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            numeric = float(raw)
        except Exception:
            iso = raw
            if raw.endswith("Z"):
                iso = raw[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(iso)
            except Exception:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            numeric = float(dt.timestamp())
    if numeric is None or numeric <= 0:
        return None
    if numeric >= 1e14:
        numeric /= 1_000_000.0
    elif numeric >= 1e11:
        numeric /= 1_000.0
    return numeric if numeric > 0 else None


def _extract_message_text_from_api_node(node: dict[str, Any]) -> str:
    for key in ("text", "message", "content", "caption", "body"):
        value = node.get(key)
        extracted = _extract_text_value(value)
        if extracted:
            return extracted
    return ""


def _extract_text_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "body", "content", "message"):
            nested = value.get(key)
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    return ""


def _resolve_direction_from_node(
    node: dict[str, Any],
    *,
    sender_id: str,
    self_user_id: str,
    self_user_ids: Optional[set[str]] = None,
) -> str:
    raw_direction = _coerce_str(
        node.get("direction")
        or node.get("message_direction")
        or node.get("folder")
        or node.get("type")
    ).lower()
    if raw_direction in {"outbound", "outgoing", "sent", "viewer"}:
        return "outbound"
    if raw_direction in {"inbound", "incoming", "received"}:
        return "inbound"

    for key in ("is_sent_by_viewer", "sent_by_viewer", "is_outgoing", "outgoing", "from_viewer"):
        value = node.get(key)
        if isinstance(value, bool):
            return "outbound" if value else "inbound"

    aliases = {str(self_user_id or "").strip()}
    if self_user_ids:
        aliases.update(str(value or "").strip() for value in self_user_ids)
    aliases.discard("")
    if sender_id and str(sender_id).strip() in aliases:
        return "outbound"
    return "unknown"


def _extract_payload_self_user_ids(payload: Any) -> set[str]:
    ids: set[str] = set()
    for node, _context_thread_id in _iter_payload_nodes(payload):
        viewer = node.get("viewer")
        if isinstance(viewer, dict):
            for key in ("interop_messaging_user_fbid", "id", "pk", "user_id", "viewer_id"):
                value = _coerce_str(viewer.get(key))
                if value:
                    ids.add(value)
        for key in ("viewer_id", "viewer_fbid", "viewer_pk", "viewer_user_id"):
            value = _coerce_str(node.get(key))
            if value:
                ids.add(value)
    return ids


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    text = str(value).strip()
    return text


def _proxy_from_account(account: dict) -> Optional[dict]:
    try:
        from src.proxy_payload import proxy_from_account
    except Exception:
        return None
    return proxy_from_account(account)


def _extract_thread_id(href: str) -> str:
    if not href:
        return ""
    match = re.search(r"/direct/t/([^/]+)", href)
    if match:
        return match.group(1)
    return ""


def _normalize_key_source(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    raw = re.sub(r"\b(hace\\s+\\d+\\s*[hmd]|ayer|hoy)\\b", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _extract_row_snippet(row) -> str:
    try:
        texts = row.locator("div[dir='auto'], span[dir='auto']").all_inner_texts()
    except Exception:
        texts = []
    cleaned = [text.strip() for text in texts if isinstance(text, str) and text.strip()]
    if not cleaned:
        return ""
    return " ".join(cleaned[:2]).strip()


def _normalize_direct_link(href: str) -> str:
    value = (href or "").strip()
    if not value:
        return ""
    if value.startswith("http"):
        return value
    if value.startswith("/"):
        return f"https://www.instagram.com{value}"
    return f"https://www.instagram.com/{value.lstrip('/')}"


def _fallback_thread_id(account: str, seed: str) -> str:
    raw = f"{account}:{seed}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def _extract_thread_title(node) -> str:
    try:
        text = node.inner_text() or ""
    except Exception:
        text = ""
    text = text.strip()
    if text:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
        if parts:
            return parts[0]
    try:
        aria = node.get_attribute("aria-label") or ""
    except Exception:
        aria = ""
    lowered = aria.lower()
    for token in ("from ", "de "):
        if token in lowered:
            idx = lowered.find(token)
            if idx >= 0:
                return aria[idx + len(token) :].strip()
    return ""


def _thread_unread_count(node) -> int:
    try:
        aria = (node.get_attribute("aria-label") or "").lower()
    except Exception:
        aria = ""

    if any(token in aria for token in _UNREAD_HINTS):
        return 1

    # BÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºsqueda de badge por aria-label
    try:
        badge = node.locator("span[aria-label*='unread'], span[aria-label*='sin leer'], span[aria-label*='no leido']")
        if badge.count():
            return 1
    except Exception:
        pass

    # BÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºsqueda por "punto azul" (visual) - Instagram suele usar un div/span con fondo azul
    # El color rgb(0, 149, 246) es el azul caracterÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­stico de Instagram
    try:
        blue_dot = node.locator("div[style*='background-color: rgb(0, 149, 246)'], span[style*='background-color: rgb(0, 149, 246)']")
        if blue_dot.count() > 0:
            return 1
    except Exception:
        pass

    return 0


def _extract_header_username(page: Page, self_username: str) -> str:
    try:
        links = page.locator("header a[href^='/']")
        total = links.count()
    except Exception:
        total = 0
        links = None
    for idx in range(total):
        try:
            href = links.nth(idx).get_attribute("href") or ""
        except Exception:
            continue
        if not href.endswith("/"):
            continue
        candidate = href.strip("/")
        if "/" in candidate:
            continue
        lowered = candidate.lower()
        if lowered in {"direct", "accounts"}:
            continue
        if lowered == self_username.lower():
            continue
        if re.fullmatch(r"[a-z0-9._]{1,30}", lowered) is None:
            continue
        return candidate
    return ""


def _extract_header_title(page: Page) -> str:
    try:
        header = page.locator("header").first
    except Exception:
        return ""
    for selector in ("span[title]", "h1", "h2", "div[dir='auto']", "span[dir='auto']"):
        try:
            loc = header.locator(selector)
            if loc.count():
                text = (loc.first.inner_text() or "").strip()
                if text:
                    return text
        except Exception:
            continue
    return ""


def _extract_message_text(node) -> str:
    texts: List[str] = []
    for selector in ("div[dir='auto']", "span[dir='auto']"):
        try:
            parts = node.locator(selector).all_inner_texts()
        except Exception:
            parts = []
        for part in parts:
            cleaned = (part or "").strip()
            if cleaned:
                texts.append(cleaned)
    if texts:
        return "\n".join(texts).strip()
    try:
        return (node.inner_text() or "").strip()
    except Exception:
        return ""


def _extract_message_id(node) -> str:
    for attr in ("data-message-id", "data-id", "data-item-id"):
        try:
            value = (node.get_attribute(attr) or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    return ""


def _normalize_message_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _message_text_matches(expected: str, candidate: str) -> bool:
    expected_norm = _normalize_message_text(expected)
    candidate_norm = _normalize_message_text(candidate)
    if not expected_norm or not candidate_norm:
        return False
    if expected_norm == candidate_norm:
        return True
    if len(expected_norm) >= 20 and (
        expected_norm in candidate_norm or candidate_norm in expected_norm
    ):
        return True
    expected_prefix = expected_norm[:48]
    candidate_prefix = candidate_norm[:48]
    if len(expected_prefix) >= 16 and expected_prefix == candidate_prefix:
        return True
    return False


def _hash_message_id(thread_id: str, user_id: str, text: str, timestamp: Optional[float]) -> str:
    seed = f"{thread_id}|{user_id}|{timestamp or ''}|{text}".encode("utf-8", errors="ignore")
    return hashlib.sha1(seed).hexdigest()


def _thread_peer_id(thread: ThreadLike, self_id: str) -> str:
    """
    Retorna el ID del peer (otro usuario) en un thread.
    Usa user.id (no pk) para consistencia.
    """
    for user in thread.users:
        if user.id != self_id:
            return user.id
    if thread.users:
        return thread.users[0].id
    return "peer"


def _fmt_ts(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(value).isoformat()
    except Exception:
        return str(value)
