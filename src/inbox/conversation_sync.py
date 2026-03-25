from __future__ import annotations

import threading
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.dm_playwright_client import (
    INBOX_URL,
    THREAD_URL_TEMPLATE,
    _build_inbox_endpoint_candidates,
    _extract_api_messages_from_payload,
    _extract_inbox_cursor,
    _extract_inbox_threads_from_payload,
    _extract_thread_id,
)

try:
    from playwright.async_api import Page
except Exception:  # pragma: no cover - optional runtime dependency
    Page = object  # type: ignore


_INBOX_READY_SELECTORS = (
    "div[role='navigation'] input[name='searchInput']",
    "div[role='navigation'] input[placeholder*='Search']",
    "div[role='navigation'] input[placeholder*='Buscar']",
    "div[role='navigation'] [aria-label='New message']",
    "div[role='navigation'] [aria-label='Nuevo mensaje']",
    "div[role='navigation'] [aria-label='Enviar mensaje']",
    "div[role='navigation'] button:has-text('New message')",
    "div[role='navigation'] button:has-text('Nuevo mensaje')",
    "div[role='navigation'] button:has-text('Enviar mensaje')",
    "div[role='navigation'] a[href*='/direct/t/']",
    "main a[href*='/direct/t/']",
)

_THREAD_READY_SELECTORS = (
    "div[role='main'] div[role='textbox'][contenteditable='true']",
    "div[role='main'] div[contenteditable='true'][role='textbox']",
    "div[role='main'] textarea",
    "header a[href^='/'] span",
    "header h2",
    "header h1",
    "main header a[href^='/'] span",
)


class ConversationSync:
    def __init__(self, engine: Any, *, interval_seconds: float = 8.0) -> None:
        self._engine = engine
        self._interval_seconds = max(5.0, float(interval_seconds or 8.0))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="inbox-conversation-sync",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        worker = self._thread
        self._thread = None
        if worker is not None:
            worker.join(timeout=2.0)

    def _run_loop(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            self._engine.enqueue_periodic_sync()


async def ensure_inbox_page(page: Page, *, timeout_ms: int = 12_000) -> None:
    current_url = str(getattr(page, "url", "") or "")
    if "/direct/" not in current_url:
        await page.goto(INBOX_URL, wait_until="domcontentloaded", timeout=45_000)
    await _wait_for_any_selector(page, _INBOX_READY_SELECTORS, timeout_ms=timeout_ms)


async def ensure_thread_page(
    page: Page,
    *,
    thread_id: str,
    thread_href: str = "",
    timeout_ms: int = 10_000,
) -> bool:
    target_id = str(thread_id or "").strip()
    href = str(thread_href or "").strip()
    if not href and target_id:
        href = THREAD_URL_TEMPLATE.format(thread_id=target_id)
    if not href:
        return False
    current_url = str(getattr(page, "url", "") or "")
    current_thread_id = _extract_thread_id(current_url)
    if current_thread_id != target_id:
        await page.goto(href, wait_until="domcontentloaded", timeout=45_000)
    await _wait_for_any_selector(page, _THREAD_READY_SELECTORS, timeout_ms=timeout_ms)
    return True


async def sync_account_threads_async(
    page: Page,
    *,
    account: dict[str, Any],
    thread_limit: int = 120,
    message_limit: int = 12,
    max_pages: int = 2,
) -> list[dict[str, Any]]:
    account_id = str(account.get("username") or "").strip().lstrip("@")
    account_alias = str(account.get("alias") or "").strip()
    if not account_id:
        return []
    await ensure_inbox_page(page)
    target_total = max(1, min(120, int(thread_limit or 120)))
    per_page = max(10, min(200, target_total))
    pages = max(1, min(4, int(max_pages or 2)))
    cursor = ""
    collected: list[dict[str, Any]] = []
    seen_thread_ids: set[str] = set()
    for _ in range(pages):
        payload = await _fetch_inbox_payload_async(
            page,
            cursor=cursor,
            limit=min(per_page, max(1, target_total - len(collected))),
            message_limit=max(1, min(20, int(message_limit or 12))),
        )
        if not isinstance(payload, dict):
            break
        snapshots = _extract_inbox_threads_from_payload(
            payload,
            self_user_id=account_id,
            self_username=account_id,
            message_limit=max(1, min(20, int(message_limit or 12))),
            thread_limit=min(per_page, max(1, target_total - len(collected))),
        )
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            thread_id = str(snapshot.get("thread_id") or "").strip()
            if not thread_id or thread_id in seen_thread_ids:
                continue
            seen_thread_ids.add(thread_id)
            thread_row = _snapshot_to_thread_row(
                snapshot,
                account_id=account_id,
                account_alias=account_alias,
            )
            if thread_row:
                collected.append(thread_row)
            if len(collected) >= target_total:
                break
        if len(collected) >= target_total:
            break
        cursor, has_older = _extract_inbox_cursor(payload)
        if not cursor or not has_older:
            break
    return collected


async def read_conversation_async(
    page: Page,
    *,
    account: dict[str, Any],
    thread_id: str,
    thread_href: str = "",
    message_limit: int = 80,
) -> dict[str, Any]:
    account_id = str(account.get("username") or "").strip().lstrip("@")
    target_thread_id = str(thread_id or "").strip()
    if not account_id or not target_thread_id:
        return {"messages": [], "participants": [], "seen_text": "", "seen_at": None}
    opened = await ensure_thread_page(
        page,
        thread_id=target_thread_id,
        thread_href=thread_href,
        timeout_ms=12_000,
    )
    if not opened:
        return {"messages": [], "participants": [], "seen_text": "", "seen_at": None}
    payload = await fetch_thread_payload_async(
        page,
        thread_id=target_thread_id,
        limit=max(1, min(80, int(message_limit or 80))),
    )
    messages = _payload_to_messages(payload, self_user_id=account_id)
    participant = await extract_header_username_async(page)
    seen_text = await read_seen_receipt_async(page)
    seen_at = time.time() if seen_text else None
    return {
        "messages": messages,
        "participants": [participant] if participant else [],
        "seen_text": seen_text,
        "seen_at": seen_at,
    }


async def fetch_thread_payload_async(
    page: Page,
    *,
    thread_id: str,
    limit: int = 40,
    request_timeout_ms: int = 3_000,
    total_timeout_ms: int = 6_500,
) -> dict[str, Any] | None:
    clean_thread_id = str(thread_id or "").strip()
    if not clean_thread_id:
        return None
    safe_limit = max(1, min(80, int(limit or 40)))
    request_nonce = int(time.time() * 1000.0)
    params_list: list[dict[str, str]] = [
        {"limit": str(safe_limit)},
        {
            "limit": str(safe_limit),
            "visual_message_return_type": "unseen",
            "persistentBadging": "true",
        },
    ]
    urls: list[str] = []
    for params in params_list:
        query = urlencode(params, doseq=True)
        for base in (
            f"/api/v1/direct_v2/threads/{clean_thread_id}/",
            f"/api/v1/direct_v2/threads/{clean_thread_id}",
        ):
            candidate = _append_cache_bust_query(f"{base}?{query}" if query else base, nonce=request_nonce)
            if candidate not in urls:
                urls.append(candidate)
    result = await _fetch_json_candidates_async(
        page,
        urls=urls,
        request_timeout_ms=request_timeout_ms,
        total_timeout_ms=total_timeout_ms,
        has_payload_expression="Array.isArray(parsed?.thread?.items) || Array.isArray(parsed?.items) || Array.isArray(parsed?.data?.thread?.items)",
    )
    payload = result.get("payload") if isinstance(result, dict) else None
    return payload if isinstance(payload, dict) else None


async def extract_header_username_async(page: Page) -> str:
    try:
        value = await page.evaluate(
            """() => {
                const selectors = [
                    "header a[href^='/'] span",
                    "header h2",
                    "header h1",
                    "main header a[href^='/'] span",
                ];
                for (const selector of selectors) {
                    const node = document.querySelector(selector);
                    const text = String(node?.textContent || "").trim();
                    if (text) return text;
                }
                return "";
            }"""
        )
    except Exception:
        return ""
    return str(value or "").strip()


async def read_seen_receipt_async(page: Page) -> str:
    try:
        raw = await page.evaluate(
            """() => {
                const root = document.querySelector("main");
                if (!root) return "";
                const texts = [];
                const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
                while (walker.nextNode()) {
                    const text = String(walker.currentNode?.textContent || "").replace(/\\s+/g, " ").trim();
                    if (!text) continue;
                    if (/^(seen|visto)(\\s|$)/i.test(text)) {
                        texts.push(text);
                    }
                }
                return texts.length ? texts[texts.length - 1] : "";
            }"""
        )
    except Exception:
        return ""
    return str(raw or "").strip()


def _snapshot_to_thread_row(
    snapshot: dict[str, Any],
    *,
    account_id: str,
    account_alias: str,
) -> dict[str, Any] | None:
    thread_id = str(snapshot.get("thread_id") or "").strip()
    if not thread_id:
        return None
    messages = snapshot.get("messages")
    preview_messages = _normalize_preview_messages(messages)
    latest = preview_messages[-1] if preview_messages else None
    latest_customer_message_at: float | None = None
    for message in preview_messages:
        if str(message.get("direction") or "").strip().lower() == "inbound":
            latest_customer_message_at = message.get("timestamp")
    try:
        unread_count = max(0, int(snapshot.get("unread_count") or 0))
    except Exception:
        unread_count = 0
    fallback_activity_at = None
    try:
        fallback_activity_at = float(snapshot.get("last_activity_at")) if snapshot.get("last_activity_at") is not None else None
    except Exception:
        fallback_activity_at = None
    latest_direction = str((latest or {}).get("direction") or "").strip().lower()
    if latest_direction not in {"inbound", "outbound", "unknown"}:
        latest_direction = "unknown"
    if latest_customer_message_at is None and unread_count > 0 and fallback_activity_at is not None:
        latest_customer_message_at = fallback_activity_at
    if latest_direction == "unknown" and unread_count > 0:
        latest_direction = "inbound"
    display_name = (
        str(snapshot.get("title") or "").strip()
        or str(snapshot.get("recipient_username") or "").strip()
        or thread_id
    )
    return {
        "thread_key": f"{account_id}:{thread_id}",
        "thread_id": thread_id,
        "thread_href": str(snapshot.get("thread_href") or "").strip()
        or THREAD_URL_TEMPLATE.format(thread_id=thread_id),
        "account_id": account_id,
        "account_alias": account_alias,
        "recipient_username": str(snapshot.get("recipient_username") or "").strip(),
        "display_name": display_name,
        "last_message_text": str((latest or {}).get("text") or snapshot.get("snippet") or "").strip(),
        "last_message_timestamp": (latest or {}).get("timestamp"),
        "last_message_direction": latest_direction or "unknown",
        "last_message_id": str((latest or {}).get("message_id") or "").strip(),
        "unread_count": unread_count,
        "participants": [
            str(snapshot.get("recipient_username") or "").strip()
            or display_name
        ],
        "last_activity_timestamp": (latest or {}).get("timestamp") or fallback_activity_at,
        "latest_customer_message_at": latest_customer_message_at,
        "preview_messages": preview_messages,
    }


def _coerce_preview_timestamp(raw: dict[str, Any]) -> float | None:
    for key in ("timestamp", "timestamp_epoch"):
        try:
            stamp = float(raw.get(key)) if raw.get(key) is not None else None
        except Exception:
            stamp = None
        if stamp is not None:
            return stamp
    return None


def _normalize_preview_messages(raw_messages: Any) -> list[dict[str, Any]]:
    rows = raw_messages if isinstance(raw_messages, list) else []
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        message_id = str(raw.get("message_id") or raw.get("id") or "").strip()
        if not message_id:
            continue
        direction = str(raw.get("direction") or "").strip().lower() or "unknown"
        if direction not in {"inbound", "outbound", "unknown"}:
            direction = "unknown"
        timestamp = _coerce_preview_timestamp(raw)
        normalized.append(
            {
                "message_id": message_id,
                "text": str(raw.get("text") or "").strip(),
                "timestamp": timestamp,
                "direction": direction,
            }
        )
    normalized.sort(key=lambda item: item.get("timestamp") or 0.0)
    return normalized


def _payload_to_messages(payload: Any, *, self_user_id: str) -> list[dict[str, Any]]:
    parsed, _missing = _extract_api_messages_from_payload(payload, self_user_id=self_user_id)
    rows: list[dict[str, Any]] = []
    for message in parsed:
        message_id = str(getattr(message, "item_id", "") or "").strip()
        if not message_id:
            continue
        try:
            timestamp = float(getattr(message, "timestamp", 0.0) or 0.0)
        except Exception:
            timestamp = None
        direction = str(getattr(message, "direction", "") or "").strip().lower() or "unknown"
        if direction not in {"inbound", "outbound", "unknown"}:
            direction = "unknown"
        rows.append(
            {
                "message_id": message_id,
                "text": str(getattr(message, "text", "") or "").strip(),
                "timestamp": timestamp,
                "direction": direction,
                "user_id": str(getattr(message, "sender_id", "") or "").strip(),
                "delivery_status": "sent",
                "local_echo": False,
            }
        )
    rows.sort(key=lambda item: item.get("timestamp") or 0.0)
    return rows


async def _wait_for_any_selector(
    page: Page,
    selectors: tuple[str, ...],
    *,
    timeout_ms: int,
) -> None:
    started = time.time()
    remaining = max(600, int(timeout_ms or 0))
    while remaining > 0:
        if await _has_visible_selector(page, selectors):
            return
        slice_ms = min(450, remaining)
        await page.wait_for_timeout(slice_ms)
        elapsed = int((time.time() - started) * 1000.0)
        remaining = max(0, int(timeout_ms) - elapsed)


async def _has_visible_selector(page: Page, selectors: tuple[str, ...]) -> bool:
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
                    return True
            except Exception:
                continue
    return False


async def _fetch_inbox_payload_async(
    page: Page,
    *,
    cursor: str = "",
    limit: int = 20,
    message_limit: int = 12,
) -> dict[str, Any] | None:
    urls = _build_inbox_candidate_urls(
        cursor=str(cursor or "").strip(),
        limit=max(1, min(40, int(limit or 20))),
        message_limit=max(1, min(20, int(message_limit or 12))),
    )
    result = await _fetch_json_candidates_async(
        page,
        urls=urls,
        request_timeout_ms=4_000,
        total_timeout_ms=8_000,
        has_payload_expression="Array.isArray(parsed?.inbox?.threads) || Array.isArray(parsed?.threads) || Array.isArray(parsed?.data?.inbox?.threads) || String(bodyText || '').indexOf('thread_id') >= 0",
    )
    payload = result.get("payload") if isinstance(result, dict) else None
    return payload if isinstance(payload, dict) else None


def _build_inbox_candidate_urls(*, cursor: str, limit: int, message_limit: int, nonce: int | None = None) -> list[str]:
    safe_limit = max(1, min(200, int(limit or 20)))
    safe_message_limit = max(1, min(80, int(message_limit or 12)))
    request_nonce = int(nonce if nonce is not None else (time.time() * 1000.0))
    urls: list[str] = []
    for candidate in _build_inbox_endpoint_candidates(
        cursor=str(cursor or "").strip(),
        limit=safe_limit,
        message_limit=safe_message_limit,
    ):
        cache_busted = _append_cache_bust_query(candidate, nonce=request_nonce)
        if cache_busted not in urls:
            urls.append(cache_busted)
    return urls


async def _fetch_json_candidates_async(
    page: Page,
    *,
    urls: list[str],
    request_timeout_ms: int,
    total_timeout_ms: int,
    has_payload_expression: str,
) -> dict[str, Any] | None:
    if not urls:
        return None
    try:
        result = await page.evaluate(
            f"""async ({{ urls, requestTimeoutMs, totalTimeoutMs }}) => {{
                const responseSummary = {{
                    ok: false,
                    status: 0,
                    url: "",
                    error: "",
                    payload: null,
                }};
                const startedAt = Date.now();
                const isOverTotalBudget = () => {{
                    return Number(Date.now() - startedAt) >= Number(totalTimeoutMs || 0);
                }};
                const cookieText = String(document.cookie || "");
                const csrfMatch = cookieText.match(/(?:^|;\\s*)csrftoken=([^;]+)/i);
                const csrfToken = csrfMatch ? decodeURIComponent(csrfMatch[1] || "") : "";
                const headers = {{
                    "accept": "application/json, text/plain, */*",
                    "cache-control": "no-cache, no-store, max-age=0",
                    "pragma": "no-cache",
                    "x-requested-with": "XMLHttpRequest",
                    "x-ig-app-id": "936619743392459",
                }};
                if (csrfToken) {{
                    headers["x-csrftoken"] = csrfToken;
                }}
                for (const endpoint of (Array.isArray(urls) ? urls : [])) {{
                    if (isOverTotalBudget()) {{
                        responseSummary.error = responseSummary.error || `timeout_total_${{Number(totalTimeoutMs || 0)}}ms`;
                        break;
                    }}
                    let timeoutHandle = null;
                    try {{
                        const controller = new AbortController();
                        timeoutHandle = setTimeout(() => controller.abort(), Number(requestTimeoutMs || 0));
                        const res = await fetch(endpoint, {{
                            method: "GET",
                            cache: "no-store",
                            credentials: "include",
                            headers,
                            signal: controller.signal,
                        }});
                        const status = Number(res.status || 0);
                        const bodyText = await res.text();
                        let parsed = null;
                        try {{
                            parsed = JSON.parse(bodyText);
                        }} catch (_err) {{
                            parsed = null;
                        }}
                        if (parsed && typeof parsed === "object") {{
                            const hasPayload = {has_payload_expression};
                            if (res.ok || hasPayload) {{
                                return {{
                                    ok: true,
                                    status,
                                    url: String(endpoint || ""),
                                    error: "",
                                    payload: parsed,
                                }};
                            }}
                        }}
                        responseSummary.status = status;
                        responseSummary.url = String(endpoint || "");
                        if (!responseSummary.error) {{
                            responseSummary.error = `status=${{status}}`;
                        }}
                    }} catch (err) {{
                        const errName = String((err && err.name) || "");
                        responseSummary.url = String(endpoint || "");
                        responseSummary.error = errName.toLowerCase() === "aborterror"
                            ? `timeout_fetch_${{Number(requestTimeoutMs || 0)}}ms`
                            : String(err || "fetch_error");
                    }} finally {{
                        if (timeoutHandle) {{
                            clearTimeout(timeoutHandle);
                        }}
                    }}
                }}
                return responseSummary;
            }}""",
            {
                "urls": urls,
                "requestTimeoutMs": max(1_000, int(request_timeout_ms or 1_000)),
                "totalTimeoutMs": max(1_500, int(total_timeout_ms or 1_500)),
            },
        )
    except Exception:
        return None
    return result if isinstance(result, dict) else None


def _append_cache_bust_query(url: str, *, nonce: int | None = None) -> str:
    clean_url = str(url or "").strip()
    if not clean_url:
        return ""
    parts = urlsplit(clean_url)
    query_pairs = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "_cb"]
    query_pairs.append(("_cb", str(int(nonce if nonce is not None else (time.time() * 1000.0)))))
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query_pairs, doseq=True),
            parts.fragment,
        )
    )
