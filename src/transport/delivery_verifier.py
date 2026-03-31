from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict

from playwright.async_api import Page

if TYPE_CHECKING:
    from src.transport.human_instagram_sender import HumanInstagramSender


@dataclass(frozen=True)
class DeliverySnapshot:
    snippet: str
    snippet_norm: str
    before_hits: int
    before_tail: tuple[str, ...]


@dataclass(frozen=True)
class DeliveryDecision:
    ok: bool
    detail: str
    stage: str
    verified: bool
    verify_source: str = ""
    reason_code: str = ""
    sent_unverified: bool = False


class DeliveryVerifier:
    def __init__(self, sender: "HumanInstagramSender") -> None:
        self._sender = sender

    async def build_snapshot(self, page: Page, text: str, *, limit: int = 30) -> DeliverySnapshot:
        snippet = self._sender._message_snippet(text).strip()
        snippet_norm = self._sender._normalize_message_text_match(snippet)
        before_values = await self.collect_recent_message_texts(page, limit=limit)
        before_norm = [self._sender._normalize_message_text_match(value) for value in before_values]
        before_hits = sum(1 for item in before_norm if snippet_norm and snippet_norm in item)
        before_tail = tuple(before_norm[-6:])
        return DeliverySnapshot(
            snippet=snippet,
            snippet_norm=snippet_norm,
            before_hits=before_hits,
            before_tail=before_tail,
        )

    async def verify_message_visible_after_send(
        self,
        page: Page,
        *,
        snippet_norm: str,
        before_hits: int,
        before_tail: list[str],
        timeout_ms: int = 2800,
    ) -> tuple[bool, Dict[str, Any]]:
        deadline = time.time() + (max(500, timeout_ms) / 1000.0)
        last_hits = before_hits
        last_tail: list[str] = before_tail

        while time.time() < deadline:
            values = await self.collect_recent_message_texts(page, limit=34)
            normalized = [
                self._sender._normalize_message_text_match(value)
                for value in values
                if self._sender._normalize_message_text_match(value)
            ]
            hits = sum(1 for item in normalized if snippet_norm and snippet_norm in item)
            tail = normalized[-6:]
            tail_changed = bool(tail) and tail != before_tail
            snippet_in_tail = bool(snippet_norm and any(snippet_norm in item for item in tail[-4:]))
            last_hits = hits
            last_tail = tail

            if snippet_norm and hits > before_hits and snippet_in_tail:
                return True, {
                    "mode": "bubble_hits_growth",
                    "before_hits": before_hits,
                    "after_hits": hits,
                    "tail_changed": tail_changed,
                }
            if snippet_norm and tail_changed and snippet_in_tail and hits >= before_hits:
                return True, {
                    "mode": "bubble_tail_match",
                    "before_hits": before_hits,
                    "after_hits": hits,
                    "tail_changed": True,
                }

            try:
                await page.wait_for_timeout(170)
            except Exception:
                break

        return False, {
            "mode": "bubble_timeout",
            "before_hits": before_hits,
            "after_hits": last_hits,
            "tail_changed": bool(last_tail and last_tail != before_tail),
        }

    async def collect_recent_message_texts(self, page: Page, *, limit: int = 28) -> list[str]:
        script = """
            (limit) => {
                const root = document.querySelector("[role='main']") || document.querySelector("main");
                if (!root) return [];
                const selectors = ["[dir='auto']", "span[dir='auto']", "div[dir='auto']"];
                const safeLimit = Math.max(6, Math.min(60, Number(limit) || 28));
                const normalize = (value) => String(value || "").replace(/\\s+/g, " ").trim();
                const shouldSkip = (node, text) => {
                    if (!text) return true;
                    if (node.closest("[role='navigation'], header, [role='banner'], footer")) return true;
                    if (node.closest("[aria-label='Lista de conversaciones']")) return true;
                    return false;
                };
                const out = [];
                const seen = new Set();
                for (const sel of selectors) {
                    const nodes = root.querySelectorAll(sel);
                    for (const node of nodes) {
                        const text = normalize(node.innerText || node.textContent || "");
                        if (shouldSkip(node, text)) continue;
                        const key = `${node.tagName || ""}:${text}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        out.push(text);
                    }
                    if (out.length >= safeLimit * 4) break;
                }
                return out.slice(-safeLimit * 2);
            }
        """
        try:
            raw = await page.evaluate(script, int(limit))
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        cleaned = [str(item or "").strip() for item in raw if str(item or "").strip()]
        return self._sender._filter_recent_message_texts(cleaned, limit=limit)

    async def wait_dom_send_confirmation(
        self,
        page: Page,
        *,
        snippet_norm: str,
        before_hits: int,
        before_tail: list[str],
        timeout_ms: int,
    ) -> tuple[bool, Dict[str, Any]]:
        deadline = time.time() + (max(500, timeout_ms) / 1000.0)
        last_hits = before_hits
        last_tail: list[str] = before_tail
        last_values: list[str] = []

        while time.time() < deadline:
            values = await self.collect_recent_message_texts(page, limit=30)
            normalized: list[str] = []
            for value in values:
                normalized_value = self._sender._normalize_message_text_match(value)
                if normalized_value:
                    normalized.append(normalized_value)
            hits = sum(1 for item in normalized if snippet_norm and snippet_norm in item)
            tail = normalized[-6:]
            tail_changed = bool(tail) and tail != before_tail
            snippet_in_tail = bool(snippet_norm and any(snippet_norm in item for item in tail[-3:]))
            last_hits = hits
            last_tail = tail
            last_values = values[-6:]

            if snippet_norm and hits > before_hits:
                return True, {
                    "mode": "dom_count_growth",
                    "before_hits": before_hits,
                    "after_hits": hits,
                    "tail_changed": tail_changed,
                }
            if snippet_norm and tail_changed and snippet_in_tail:
                return True, {
                    "mode": "dom_tail_match",
                    "before_hits": before_hits,
                    "after_hits": hits,
                    "tail_changed": True,
                }
            try:
                await page.wait_for_timeout(180)
            except Exception:
                break

        return False, {
            "mode": "dom_timeout",
            "before_hits": before_hits,
            "after_hits": last_hits,
            "tail_changed": bool(last_tail and last_tail != before_tail),
            "tail_sample": last_values,
        }

    async def wait_send_network_ok(
        self,
        page: Page,
        snippet: str,
        timeout_ms: int,
    ) -> tuple[bool, Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        done: asyncio.Future[Dict[str, Any]] = loop.create_future()
        tasks: list[asyncio.Task[Any]] = []
        meta: Dict[str, Any] = {
            "snippet": self._sender._message_snippet(snippet),
            "matched_responses": 0,
            "last_url": None,
            "last_status": None,
            "last_json_status": None,
        }

        def _is_candidate(url: str, method: str) -> bool:
            u = (url or "").lower()
            m = (method or "").upper()
            return m == "POST" and "/direct_v2/threads/" in u and (
                "broadcast" in u or "/items" in u or "send" in u
            )

        async def _handle_response(response: Any) -> None:
            status = None
            url = None
            json_status = None
            try:
                status = int(response.status)
            except Exception:
                status = None
            try:
                url = response.url
            except Exception:
                url = None
            try:
                body = await response.json()
                if isinstance(body, dict) and body.get("status") is not None:
                    json_status = str(body.get("status")).lower()
            except Exception:
                json_status = None

            meta["last_url"] = url
            meta["last_status"] = status
            meta["last_json_status"] = json_status

            ok_http = status in {200, 201}
            ok_json = json_status in {None, "ok", "success"}
            if ok_http and ok_json and not done.done():
                done.set_result({"url": url, "status": status, "json_status": json_status})

        def _on_response(response: Any) -> None:
            try:
                method = response.request.method
                url = response.url
            except Exception:
                return
            if not _is_candidate(url, method):
                return
            meta["matched_responses"] = int(meta["matched_responses"] or 0) + 1
            tasks.append(asyncio.create_task(_handle_response(response)))

        page.on("response", _on_response)
        try:
            try:
                response_meta = await asyncio.wait_for(done, timeout=max(0.3, timeout_ms / 1000.0))
                out = dict(meta)
                out.update(response_meta)
                return True, out
            except asyncio.TimeoutError:
                return False, dict(meta)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def decide_confirmation(
        *,
        net_ok: bool,
        dom_ok: bool,
        bubble_ok: bool,
        composer_cleared: bool,
        allow_unverified: bool,
    ) -> DeliveryDecision:
        if net_ok:
            return DeliveryDecision(
                ok=True,
                detail="sent_verified",
                stage="SEND_OK",
                verified=True,
                verify_source="network",
            )

        if dom_ok and bubble_ok and composer_cleared:
            return DeliveryDecision(
                ok=True,
                detail="sent_verified",
                stage="SEND_OK",
                verified=True,
                verify_source="dom_bubble_composer",
            )

        if bubble_ok and composer_cleared:
            return DeliveryDecision(
                ok=True,
                detail="sent_verified",
                stage="SEND_OK",
                verified=True,
                verify_source="bubble_composer",
            )

        if composer_cleared and allow_unverified:
            return DeliveryDecision(
                ok=True,
                detail="sent_unverified",
                stage="SEND_UNVERIFIED_ALLOWED",
                verified=False,
                reason_code="SENT_UNVERIFIED",
                sent_unverified=True,
            )

        if composer_cleared:
            return DeliveryDecision(
                ok=False,
                detail="send_unverified_blocked",
                stage="SEND_UNVERIFIED_BLOCKED",
                verified=False,
                reason_code="SENT_UNVERIFIED",
            )

        return DeliveryDecision(
            ok=False,
            detail="send_not_confirmed",
            stage="SEND_NOT_CONFIRMED",
            verified=False,
        )
