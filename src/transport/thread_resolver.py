from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from playwright.async_api import Locator, Page

if TYPE_CHECKING:
    from src.transport.human_instagram_sender import HumanInstagramSender


@dataclass(frozen=True)
class ThreadOpenResult:
    opened: bool
    reason: str
    method: str = "sidebar_search"
    thread_id: str = ""


class SidebarThreadResolver:
    def __init__(
        self,
        sender: "HumanInstagramSender",
        *,
        search_inputs: tuple[str, ...],
        result_rows: tuple[str, ...],
        thread_open_timeout_ms: int,
        sidebar_row_timeout_ms: int,
        log_event: Callable[..., None],
    ) -> None:
        self._sender = sender
        self._search_inputs = tuple(search_inputs)
        self._result_rows = tuple(result_rows)
        self._thread_open_timeout_ms = int(thread_open_timeout_ms)
        self._sidebar_row_timeout_ms = int(sidebar_row_timeout_ms)
        self._log_event = log_event

    async def _probe_sidebar_results(
        self,
        page: Page,
        username: str = "",
        *,
        click_match: bool = False,
    ) -> Dict[str, Any]:
        target = self._sender._normalize_username(username).lower()
        try:
            result = await page.evaluate(
                """({ target, clickMatch }) => {
                    const normalize = (value) => String(value || "").toLowerCase().replace(/\\s+/g, " ").trim();
                    const extractTokens = (value) => {
                        const seen = new Set();
                        const tokens = [];
                        const matches = normalize(value).match(/@?[a-z0-9._]{1,30}/g) || [];
                        for (const raw of matches) {
                            const candidate = raw.replace(/^@/, "");
                            if (!/^[a-z0-9._]{1,30}$/.test(candidate) || seen.has(candidate)) continue;
                            seen.add(candidate);
                            tokens.push(candidate);
                        }
                        return tokens;
                    };
                    const isVisible = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        const style = window.getComputedStyle(node);
                        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                            return false;
                        }
                        const rect = node.getBoundingClientRect();
                        if (rect.width < 48 || rect.height < 18) return false;
                        if (rect.bottom <= 0 || rect.right <= 0) return false;
                        if (rect.top >= window.innerHeight || rect.left >= window.innerWidth) return false;
                        return true;
                    };
                    const isSidebarCandidate = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        if (!isVisible(node)) return false;
                        const rect = node.getBoundingClientRect();
                        return rect.left < window.innerWidth * 0.62;
                    };
                    const clickableTarget = (row) => {
                        if (!(row instanceof HTMLElement)) return null;
                        if (row.matches("[role='button'], a[href], button, [tabindex='0']")) return row;
                        return row.querySelector("[role='button'][tabindex='0'], a[href*='/direct/t/'], [role='button'], button, [tabindex='0']");
                    };
                    const searchInput = document.querySelector(
                        "div[role='navigation'] input[name='searchInput'], " +
                        "div[role='navigation'] input[placeholder*='Search'], " +
                        "div[role='navigation'] input[placeholder*='Buscar'], " +
                        "div[role='navigation'] [role='searchbox'], " +
                        "input[name='searchInput'], [role='searchbox']"
                    );
                    const queryValue = normalize(
                        searchInput && "value" in searchInput ? searchInput.value : (searchInput?.textContent || "")
                    );
                    const rows = Array.from(
                        document.querySelectorAll(
                            "ul li, [role='listitem'], div[role='button'][tabindex='0'], div[role='button'], a[href*='/direct/t/']"
                        )
                    );
                    const signatureParts = [];
                    let visibleCount = 0;
                    for (let index = 0; index < rows.length; index += 1) {
                        const row = rows[index];
                        if (!isSidebarCandidate(row)) continue;
                        const text = normalize(row.innerText || row.textContent || "");
                        if (!text) continue;
                        visibleCount += 1;
                        if (signatureParts.length < 6) {
                            signatureParts.push(text.slice(0, 80));
                        }
                        if (!target) continue;
                        if (!extractTokens(text).includes(target)) continue;
                        const clickable = clickableTarget(row);
                        const payload = {
                            exact_match: true,
                            idx: index,
                            row_count: visibleCount,
                            query_value: queryValue,
                            signature: signatureParts.join(" | "),
                            candidate_username: target,
                            text: text.slice(0, 240),
                        };
                        if (!clickMatch || !clickable) {
                            return payload;
                        }
                        try {
                            clickable.scrollIntoView({ block: "center", inline: "nearest" });
                        } catch (_error) {}
                        clickable.click();
                        return { ...payload, clicked: true };
                    }
                    return {
                        exact_match: false,
                        row_count: visibleCount,
                        query_value: queryValue,
                        signature: signatureParts.join(" | "),
                    };
                }""",
                {"target": target, "clickMatch": bool(click_match)},
            )
        except Exception as exc:
            self._log_event("SIDEBAR_PROBE_FAIL", error=repr(exc))
            return {}
        return dict(result) if isinstance(result, dict) else {}

    async def wait_sidebar_results_ready(
        self,
        page: Page,
        username: str,
        *,
        timeout_ms: int,
        baseline_signature: str,
    ) -> Dict[str, Any]:
        target = self._sender._normalize_username(username).lower()
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        baseline = str(baseline_signature or "").strip()
        last_probe: Dict[str, Any] = {}
        while time.time() < deadline:
            probe = await self._probe_sidebar_results(page, username)
            last_probe = dict(probe)
            query_value = self._sender._normalize_username(str(probe.get("query_value") or "")).lower()
            signature = str(probe.get("signature") or "").strip()
            has_rows = max(0, int(probe.get("row_count") or 0)) > 0
            surface_changed = signature != baseline
            if bool(probe.get("exact_match")):
                last_probe["surface_changed"] = surface_changed
                return last_probe
            if query_value == target and (
                surface_changed
                or (not baseline and has_rows)
                or (bool(baseline) and not signature)
            ):
                last_probe["surface_changed"] = surface_changed
                return last_probe
            try:
                await page.wait_for_timeout(120)
            except Exception:
                break
        signature = str(last_probe.get("signature") or "").strip()
        last_probe["surface_changed"] = signature != baseline
        return last_probe

    async def sidebar_js_click_exact_match(self, page: Page, username: str) -> tuple[bool, Dict[str, Any]]:
        if not self._sender._normalize_username(username).lower():
            return False, {}
        result = await self._probe_sidebar_results(page, username, click_match=True)
        if not result:
            return False, {}
        if not bool(result.get("clicked")):
            result.setdefault("reason", "target_missing")
        return bool(result.get("clicked")), result

    async def click_sidebar_row(self, row: Locator) -> bool:
        targets: list[Locator] = [row]
        for selector in (
            "[role='button'][tabindex='0']",
            "a[href*='/direct/t/']",
            "[role='button']",
            "[tabindex='0']",
        ):
            targets.append(row.locator(selector).first)
        for candidate in targets:
            try:
                await candidate.scroll_into_view_if_needed(timeout=1_500)
            except Exception:
                pass
            try:
                await candidate.click(timeout=2_200)
                return True
            except Exception:
                try:
                    await candidate.click(timeout=1_200, force=True)
                    return True
                except Exception:
                    continue
        return False

    async def wait_thread_navigation(
        self,
        page: Page,
        username: str,
        *,
        timeout_ms: int,
        previous_url: str,
        previous_thread: str,
        candidate_username: str = "",
    ) -> bool:
        target = self._sender._normalize_username(username).lower()
        deadline = time.time() + (max(300, timeout_ms) / 1000.0)
        while time.time() < deadline:
            current_url = (page.url or "").lower()
            current_thread = self.extract_thread_id_from_direct_url(current_url)
            in_thread_url = "/direct/t/" in current_url and bool(current_thread)
            changed_from_before = not previous_thread or current_thread != previous_thread or current_url != previous_url
            if in_thread_url and changed_from_before:
                self._log_event(
                    "THREAD_OPEN_OK",
                    row_username=candidate_username or "-",
                    target_username=target,
                    current_thread=current_thread,
                    previous_thread=previous_thread or "-",
                    url=current_url,
                )
                return True
            try:
                await page.wait_for_timeout(120)
            except Exception:
                break
        current_url = (page.url or "").lower()
        current_thread = self.extract_thread_id_from_direct_url(current_url)
        self._log_event(
            "THREAD_OPEN_FAILED",
            target_username=target,
            current_thread=current_thread or "-",
            previous_thread=previous_thread or "-",
            row_username=candidate_username or "-",
            url=current_url,
        )
        return False

    def extract_thread_id_from_direct_url(self, url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        match = re.search(r"/direct/t/([^/?#]+)", raw, re.IGNORECASE)
        if not match:
            return ""
        return str(match.group(1) or "").strip()

    async def clear_and_type_search(self, page: Page, search_input: Locator, username: str) -> None:
        target = self._sender._normalize_username(username)
        await self._sender._focus_input_best_effort(search_input)
        await self._sender._clear_input_best_effort(page, search_input)
        await self._wait_for_search_value(page, search_input, "", timeout_ms=700)
        await self._sender._type_input_like_human(page, search_input, target)
        current_value = await self._wait_for_search_value(
            page,
            search_input,
            target,
            timeout_ms=max(900, min(2_000, len(target) * 110)),
        )
        if self._sender._normalize_username(current_value).lower() != target.lower():
            await self._sender._set_input_value_best_effort(page, search_input, target)
            await self._wait_for_search_value(page, search_input, target, timeout_ms=900)

    async def _wait_for_search_value(
        self,
        page: Page,
        search_input: Locator,
        expected: str,
        *,
        timeout_ms: int,
    ) -> str:
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        expected_norm = self._sender._normalize_username(expected).lower()
        last_value = ""
        while time.time() < deadline:
            try:
                last_value = (await search_input.input_value() or "").strip()
            except Exception:
                last_value = ""
            if self._sender._normalize_username(last_value).lower() == expected_norm:
                return last_value
            try:
                await page.wait_for_timeout(70)
            except Exception:
                break
        return last_value

    async def find_exact_sidebar_row(self, page: Page, username: str, *, timeout_ms: int) -> Optional[Locator]:
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        selector = ", ".join(self._result_rows)
        while time.time() < deadline:
            rows = page.locator(selector)
            try:
                count = await rows.count()
            except Exception:
                count = 0
            for idx in range(min(count, 60)):
                row = rows.nth(idx)
                try:
                    if not await row.is_visible():
                        continue
                except Exception:
                    continue
                match_ok, candidate = await self._sender._row_contains_exact_span_username(row, username)
                if match_ok:
                    self._log_event(
                        "SIDEBAR_MATCH_OK",
                        idx=idx,
                        candidate_username=candidate,
                        target_username=self._sender._normalize_username(username).lower(),
                    )
                    return row
            try:
                await page.wait_for_timeout(130)
            except Exception:
                break
        return None

    async def wait_thread_open(
        self,
        page: Page,
        row: Locator,
        username: str,
        *,
        timeout_ms: int,
        flow_hook: Optional[Callable[[str, bool], None]] = None,
    ) -> bool:
        target = self._sender._normalize_username(username).lower()
        if not target:
            return False

        current_url_before = (page.url or "").lower()
        thread_before = self.extract_thread_id_from_direct_url(current_url_before)
        row_identity_confirmed, candidate_username = await self._sender._row_contains_exact_span_username(row, target)
        if not row_identity_confirmed:
            return False
        try:
            clicked = await self.click_sidebar_row(row)
            if not clicked:
                return False
            if callable(flow_hook):
                flow_hook("waiting chat load", True)
            return await self.wait_thread_navigation(
                page,
                username,
                timeout_ms=timeout_ms,
                previous_url=current_url_before,
                previous_thread=thread_before,
                candidate_username=candidate_username,
            )
        except Exception:
            return False

    async def _open_exact_match_via_js(
        self,
        page: Page,
        username: str,
        *,
        deadline: float,
        flow_hook: Optional[Callable[[str, bool], None]] = None,
    ) -> Optional[ThreadOpenResult]:
        current_url_before = (page.url or "").lower()
        thread_before = self.extract_thread_id_from_direct_url(current_url_before)
        fallback_clicked, fallback_meta = await self.sidebar_js_click_exact_match(page, username)
        if not fallback_clicked:
            return None
        if callable(flow_hook):
            flow_hook("results detected", False)
            flow_hook("clicking result", True)
            flow_hook("waiting chat load", True)
        self._log_event(
            "SIDEBAR_MATCH_OK",
            idx=fallback_meta.get("idx"),
            candidate_username=fallback_meta.get("candidate_username"),
            target_username=self._sender._normalize_username(username).lower(),
            strategy="js_exact_match",
        )
        open_timeout = self._sender._remaining_ms(deadline, self._thread_open_timeout_ms)
        if open_timeout <= 0:
            return ThreadOpenResult(False, "thread_open_failed")
        opened = await self.wait_thread_navigation(
            page,
            username,
            timeout_ms=open_timeout,
            previous_url=current_url_before,
            previous_thread=thread_before,
            candidate_username=str(fallback_meta.get("candidate_username") or ""),
        )
        current_thread = self.extract_thread_id_from_direct_url(page.url or "")
        return ThreadOpenResult(
            opened,
            "ok" if opened else "thread_open_failed",
            thread_id=current_thread,
        )

    async def open_thread_from_sidebar(
        self,
        page: Page,
        username: str,
        *,
        deadline: float,
    ) -> ThreadOpenResult:
        flow_hook = getattr(self._sender, "_active_flow_hook", None)
        if self._sender._is_chrome_error_url(page):
            self._log_event("CHROME_ERROR_IN_OPEN_THREAD", url=page.url if page else "")
            return ThreadOpenResult(False, "chrome_error")

        if callable(flow_hook):
            flow_hook("open search", False)
            flow_hook("waiting search input visible", True)
        search_input = await self._sender._first_visible(page, self._search_inputs, max_scan_per_selector=2)
        if search_input is None:
            self._log_event("SIDEBAR_SEARCH_INPUT_MISSING", url=page.url if page else "")
            if self._sender._is_chrome_error_url(page):
                return ThreadOpenResult(False, "chrome_error")
            return ThreadOpenResult(False, "ui_not_found")
        if callable(flow_hook):
            flow_hook("search input visible", False)

        baseline_probe = await self._probe_sidebar_results(page)
        try:
            if callable(flow_hook):
                flow_hook(f"typing username: {self._sender._normalize_username(username)}", True)
            await self.clear_and_type_search(page, search_input, username)
        except Exception as exc:
            self._log_event("SIDEBAR_SEARCH_TYPE_FAIL", error=repr(exc))
            if self._sender._is_chrome_error_url(page):
                return ThreadOpenResult(False, "chrome_error")
            return ThreadOpenResult(False, "ui_not_found")

        if self._sender._is_chrome_error_url(page):
            return ThreadOpenResult(False, "chrome_error")

        row_timeout = self._sender._remaining_ms(deadline, self._sidebar_row_timeout_ms)
        if row_timeout <= 0:
            return ThreadOpenResult(False, "thread_open_failed")
        if callable(flow_hook):
            flow_hook("waiting search results", True)
        results_wait_started_at = time.perf_counter()
        ready_probe = await self.wait_sidebar_results_ready(
            page,
            username,
            timeout_ms=row_timeout,
            baseline_signature=str(baseline_probe.get("signature") or ""),
        )
        self._log_event(
            "SIDEBAR_RESULTS_READY",
            target_username=self._sender._normalize_username(username).lower(),
            wait_ms=int((time.perf_counter() - results_wait_started_at) * 1000),
            row_count=max(0, int(ready_probe.get("row_count") or 0)),
            exact_match=bool(ready_probe.get("exact_match")),
            surface_changed=bool(ready_probe.get("surface_changed")),
            query_value=str(ready_probe.get("query_value") or ""),
        )
        if bool(ready_probe.get("exact_match")):
            js_open_result = await self._open_exact_match_via_js(
                page,
                username,
                deadline=deadline,
                flow_hook=flow_hook,
            )
            if js_open_result is not None:
                return js_open_result

        row_timeout = self._sender._remaining_ms(deadline, self._sidebar_row_timeout_ms)
        if row_timeout <= 0:
            return ThreadOpenResult(False, "thread_open_failed")
        row = await self.find_exact_sidebar_row(page, username, timeout_ms=row_timeout)
        if row is None:
            js_open_result = await self._open_exact_match_via_js(
                page,
                username,
                deadline=deadline,
                flow_hook=flow_hook,
            )
            if js_open_result is not None:
                return js_open_result
            self._log_event("SIDEBAR_EXACT_MATCH_MISSING", username=username)
            if self._sender._is_chrome_error_url(page):
                return ThreadOpenResult(False, "chrome_error")
            return ThreadOpenResult(False, "username_not_found")

        open_timeout = self._sender._remaining_ms(deadline, self._thread_open_timeout_ms)
        if open_timeout <= 0:
            return ThreadOpenResult(False, "thread_open_failed")
        if callable(flow_hook):
            flow_hook("results detected", False)
            flow_hook("clicking result", True)
        opened = await self.wait_thread_open(
            page,
            row,
            username,
            timeout_ms=open_timeout,
            flow_hook=flow_hook,
        )
        current_thread = self.extract_thread_id_from_direct_url(page.url or "")
        return ThreadOpenResult(
            opened,
            "ok" if opened else "thread_open_failed",
            thread_id=current_thread,
        )
