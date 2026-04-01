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
    method: str = "compose_dialog"
    thread_id: str = ""


class SidebarThreadResolver:
    _SIDEBAR_MIN_WIDTH_PX = 220
    _SIDEBAR_SEARCH_INPUTS = (
        "div[role='navigation'] input[name='searchInput']",
        "div[role='navigation'] input[placeholder*='Search']",
        "div[role='navigation'] input[placeholder*='Buscar']",
        "div[role='navigation'] [role='searchbox']",
        "input[name='searchInput']",
        "[role='searchbox']",
    )
    _DIALOG_SURFACES = (
        "div[role='dialog']",
        "[aria-modal='true']",
    )
    _DIALOG_CLOSE_BUTTONS = (
        "div[role='dialog'] [aria-label='Close']",
        "div[role='dialog'] [aria-label='Cerrar']",
        "div[role='dialog'] button:has-text('Close')",
        "div[role='dialog'] button:has-text('Cerrar')",
        "div[role='dialog'] div[role='button']:has-text('Close')",
        "div[role='dialog'] div[role='button']:has-text('Cerrar')",
    )
    def __init__(
        self,
        sender: "HumanInstagramSender",
        *,
        compose_triggers: tuple[str, ...],
        search_inputs: tuple[str, ...],
        result_rows: tuple[str, ...],
        confirm_buttons: tuple[str, ...],
        thread_open_timeout_ms: int,
        sidebar_row_timeout_ms: int,
        log_event: Callable[..., None],
    ) -> None:
        self._sender = sender
        self._compose_triggers = tuple(compose_triggers)
        self._search_inputs = tuple(search_inputs)
        self._result_rows = tuple(result_rows)
        self._confirm_buttons = tuple(confirm_buttons)
        self._thread_open_timeout_ms = int(thread_open_timeout_ms)
        self._sidebar_row_timeout_ms = int(sidebar_row_timeout_ms)
        self._log_event = log_event

    async def _click_locator_best_effort(self, page: Page, locator: Locator) -> bool:
        try:
            await locator.scroll_into_view_if_needed(timeout=1_200)
        except Exception:
            pass
        try:
            await locator.click(timeout=1_500)
            return True
        except Exception:
            pass
        try:
            await locator.click(timeout=1_500, force=True)
            return True
        except Exception:
            pass
        try:
            await locator.evaluate(
                """(node) => {
                    if (!(node instanceof HTMLElement)) return false;
                    try {
                        node.scrollIntoView({ block: "center", inline: "nearest" });
                    } catch (_error) {}
                    node.click();
                    return true;
                }"""
            )
            return True
        except Exception:
            return False

    async def _wait_compose_search_input(self, page: Page, *, timeout_ms: int) -> Optional[Locator]:
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        while time.time() < deadline:
            search_input = await self._sender._first_visible(page, self._search_inputs, max_scan_per_selector=3)
            if search_input is not None:
                return search_input
            try:
                await page.wait_for_timeout(140)
            except Exception:
                break
        return await self._sender._first_visible(page, self._search_inputs, max_scan_per_selector=3)

    async def _wait_sidebar_search_input(self, page: Page, *, timeout_ms: int) -> Optional[Locator]:
        deadline = time.time() + (max(250, timeout_ms) / 1000.0)
        while time.time() < deadline:
            search_input = await self._sender._first_visible(
                page,
                self._SIDEBAR_SEARCH_INPUTS,
                max_scan_per_selector=3,
            )
            if search_input is not None:
                surface = await self._sidebar_surface_state(page)
                if not surface or bool(surface.get("usable")):
                    return search_input
                self._log_event(
                    "SIDEBAR_SURFACE_UNAVAILABLE",
                    reason=str(surface.get("reason") or "collapsed"),
                    nav_width=int(surface.get("nav_width") or 0),
                    viewport_width=int(surface.get("viewport_width") or 0),
                    viewport_height=int(surface.get("viewport_height") or 0),
                    url=page.url if page else "",
                )
                try:
                    await page.wait_for_timeout(140)
                except Exception:
                    break
                continue
            try:
                await page.wait_for_timeout(140)
            except Exception:
                break
        search_input = await self._sender._first_visible(page, self._SIDEBAR_SEARCH_INPUTS, max_scan_per_selector=3)
        if search_input is None:
            return None
        surface = await self._sidebar_surface_state(page)
        if surface and not bool(surface.get("usable")):
            return None
        return search_input

    async def _sidebar_surface_state(self, page: Page) -> Dict[str, Any]:
        try:
            result = await page.evaluate(
                """({ minWidth }) => {
                    const isVisible = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        const style = window.getComputedStyle(node);
                        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                            return false;
                        }
                        const rect = node.getBoundingClientRect();
                        if (rect.width < 12 || rect.height < 10) return false;
                        if (rect.bottom <= 0 || rect.right <= 0) return false;
                        if (rect.top >= window.innerHeight || rect.left >= window.innerWidth) return false;
                        return true;
                    };
                    const nav =
                        document.querySelector("div[role='navigation']") ||
                        document.querySelector("nav[role='navigation']") ||
                        document.querySelector("aside");
                    const search =
                        document.querySelector("div[role='navigation'] input[name='searchInput']") ||
                        document.querySelector("div[role='navigation'] input[placeholder*='Search']") ||
                        document.querySelector("div[role='navigation'] input[placeholder*='Buscar']") ||
                        document.querySelector("div[role='navigation'] [role='searchbox']");
                    const navVisible = isVisible(nav);
                    const searchVisible = isVisible(search);
                    const navRect = navVisible ? nav.getBoundingClientRect() : null;
                    const searchRect = searchVisible ? search.getBoundingClientRect() : null;
                    const navWidth = navRect ? Math.round(navRect.width) : 0;
                    const searchInSidebar = !!(searchRect && searchRect.left < (window.innerWidth * 0.5));
                    const usable = navVisible && searchVisible && searchInSidebar && navWidth >= minWidth;
                    let reason = "ok";
                    if (!navVisible) {
                        reason = "nav_missing";
                    } else if (!searchVisible) {
                        reason = "search_missing";
                    } else if (!searchInSidebar) {
                        reason = "search_outside_sidebar";
                    } else if (navWidth < minWidth) {
                        reason = "sidebar_collapsed";
                    }
                    return {
                        usable,
                        reason,
                        nav_visible: navVisible,
                        search_visible: searchVisible,
                        nav_width: navWidth,
                        viewport_width: Math.round(window.innerWidth || 0),
                        viewport_height: Math.round(window.innerHeight || 0),
                    };
                }""",
                {"minWidth": int(self._SIDEBAR_MIN_WIDTH_PX)},
            )
        except Exception:
            return {}
        return dict(result) if isinstance(result, dict) else {}

    async def _open_compose_surface(self, page: Page, *, deadline: float) -> Optional[Locator]:
        ready_search_input = await self._wait_compose_search_input(
            page,
            timeout_ms=min(1_200, self._sender._remaining_ms(deadline, 1_200)),
        )
        if ready_search_input is not None:
            return ready_search_input
        trigger = await self._sender._first_visible(page, self._compose_triggers, max_scan_per_selector=3)
        if trigger is None:
            self._log_event("COMPOSE_TRIGGER_MISSING", url=page.url if page else "")
            return None
        if not await self._click_locator_best_effort(page, trigger):
            self._log_event("COMPOSE_TRIGGER_CLICK_FAILED", url=page.url if page else "")
            return None
        wait_ms = self._sender._remaining_ms(deadline, max(2_500, self._sidebar_row_timeout_ms))
        search_input = await self._wait_compose_search_input(page, timeout_ms=wait_ms)
        if search_input is None:
            self._log_event("COMPOSE_SEARCH_INPUT_MISSING", url=page.url if page else "")
        return search_input

    async def _click_compose_confirm_if_needed(self, page: Page) -> bool:
        button = await self._sender._first_visible(page, self._confirm_buttons, max_scan_per_selector=3)
        if button is None:
            return False
        clicked = await self._click_locator_best_effort(page, button)
        if clicked:
            self._log_event("COMPOSE_CONFIRM_CLICKED", url=page.url if page else "")
        return clicked

    async def _compose_surface_active(self, page: Page) -> bool:
        try:
            search_input = await self._sender._first_visible(page, self._search_inputs, max_scan_per_selector=2)
        except Exception:
            search_input = None
        if search_input is not None:
            return True
        try:
            confirm_button = await self._sender._first_visible(page, self._confirm_buttons, max_scan_per_selector=2)
        except Exception:
            confirm_button = None
        return confirm_button is not None

    async def _dialog_surface_visible(self, page: Page) -> bool:
        try:
            dialog = await self._sender._first_visible(page, self._DIALOG_SURFACES, max_scan_per_selector=2)
        except Exception:
            dialog = None
        return dialog is not None

    async def cleanup_stale_compose_state(self, page: Page, *, deadline: float) -> bool:
        attempts = 0
        while attempts < 3:
            compose_active = await self._compose_surface_active(page)
            dialog_visible = await self._dialog_surface_visible(page)
            if not compose_active and not dialog_visible:
                return True
            close_button = await self._sender._first_visible(page, self._DIALOG_CLOSE_BUTTONS, max_scan_per_selector=2)
            if close_button is not None:
                await self._click_locator_best_effort(page, close_button)
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            try:
                await page.wait_for_timeout(180)
            except Exception:
                break
            attempts += 1
        compose_active = await self._compose_surface_active(page)
        dialog_visible = await self._dialog_surface_visible(page)
        if compose_active or dialog_visible:
            try:
                await self._sender._inbox_navigator.ensure_inbox_surface(page, deadline=deadline)
            except Exception:
                return False
        return not (await self._compose_surface_active(page) or await self._dialog_surface_visible(page))

    async def _main_chat_surface_visible(self, page: Page) -> bool:
        try:
            result = await page.evaluate(
                """() => {
                    const isVisible = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        const style = window.getComputedStyle(node);
                        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                            return false;
                        }
                        const rect = node.getBoundingClientRect();
                        return rect.width > 24 && rect.height > 18;
                    };
                    const main = document.querySelector("[role='main'], main");
                    return isVisible(main);
                }"""
            )
        except Exception:
            return True
        return bool(result)

    async def _right_chat_surface_ready(self, page: Page) -> bool:
        if not await self._main_chat_surface_visible(page):
            return False
        try:
            composer = await self._sender._message_composer.thread_composer(page)
        except Exception:
            composer = None
        if composer is not None:
            return True
        try:
            result = await page.evaluate(
                """() => {
                    const isVisible = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        const style = window.getComputedStyle(node);
                        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                            return false;
                        }
                        const rect = node.getBoundingClientRect();
                        return rect.width > 24 && rect.height > 18;
                    };
                    const main = document.querySelector("[role='main'], main");
                    if (!(main instanceof HTMLElement) || !isVisible(main)) return false;
                    const headerSelectors = [
                        "[role='main'] header",
                        "main header",
                    ];
                    for (const selector of headerSelectors) {
                        const node = document.querySelector(selector);
                        if (isVisible(node)) {
                            return true;
                        }
                    }
                    const historySelectors = [
                        "[role='main'] [role='row']",
                        "[role='main'] [role='grid']",
                        "[role='main'] ul",
                        "[role='main'] section",
                        "[role='main'] article",
                    ];
                    for (const selector of historySelectors) {
                        const node = document.querySelector(selector);
                        if (isVisible(node)) {
                            return true;
                        }
                    }
                    return false;
                }"""
            )
        except Exception:
            return True
        return bool(result)

    async def _chat_header_matches_target(self, page: Page, username: str) -> bool:
        target = self._sender._normalize_username(username).lower()
        if not target:
            return False
        try:
            header_links = page.locator("header a[href^='/'], main header a[href^='/']")
            total_links = await header_links.count()
        except Exception:
            total_links = 0
            header_links = None
        for idx in range(min(total_links, 8)):
            try:
                href = str(await header_links.nth(idx).get_attribute("href") or "").strip()
            except Exception:
                continue
            candidate = href.strip("/")
            if not candidate or "/" in candidate:
                continue
            lowered = candidate.lower()
            if lowered in {"direct", "accounts"}:
                continue
            if self._sender._normalize_username(candidate).lower() == target:
                return True
        try:
            matched = await page.evaluate(
                r"""({ target }) => {
                    const normalize = (value) => String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
                    const extractTokens = (value) => {
                        const seen = new Set();
                        const tokens = [];
                        const matches = normalize(value).match(/@?[a-z0-9._]{1,30}/g) || [];
                        for (const raw of matches) {
                            const candidate = raw.replace(/^@/, "");
                            if (!candidate || seen.has(candidate)) continue;
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
                        if (rect.width < 12 || rect.height < 10) return false;
                        if (rect.bottom <= 0 || rect.right <= 0) return false;
                        if (rect.top >= window.innerHeight || rect.left >= window.innerWidth) return false;
                        return true;
                    };
                    const topBoundary = Math.max(260, window.innerHeight * 0.45);
                    const leftBoundary = window.innerWidth * 0.35;
                    const candidates = Array.from(document.querySelectorAll("header *, main *"));
                    for (const node of candidates) {
                        if (!isVisible(node)) continue;
                        const rect = node.getBoundingClientRect();
                        if (rect.top > topBoundary) continue;
                        if (rect.left < leftBoundary) continue;
                        const samples = [
                            node.getAttribute("aria-label"),
                            node.getAttribute("title"),
                            node.textContent,
                        ];
                        for (const sample of samples) {
                            const tokens = extractTokens(sample);
                            if (tokens.includes(target)) {
                                return true;
                            }
                        }
                    }
                    return false;
                }""",
                {"target": target},
            )
        except Exception:
            matched = False
        return bool(matched)
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
            header_match = await self._chat_header_matches_target(page, username)
            candidate_match = self._sender._normalize_username(candidate_username).lower() == target if candidate_username else False
            if in_thread_url and changed_from_before:
                self._log_event(
                    "THREAD_OPEN_OK",
                    row_username=candidate_username or "-",
                    target_username=target,
                    current_thread=current_thread,
                    previous_thread=previous_thread or "-",
                    url=current_url,
                    matched_via="sidebar_thread_header" if header_match else "sidebar_thread_url_only",
                    header_match=header_match,
                )
                return True
            if in_thread_url and candidate_match:
                self._log_event(
                    "THREAD_OPEN_OK",
                    row_username=candidate_username or "-",
                    target_username=target,
                    current_thread=current_thread or "-",
                    previous_thread=previous_thread or "-",
                    url=current_url,
                    matched_via="sidebar_target_header" if header_match else "sidebar_target_url_only",
                    header_match=header_match,
                )
                return True
            if in_thread_url and not changed_from_before:
                header_confirmed = header_match
                header_deadline = time.time() + 2.0
                while not header_confirmed and time.time() < header_deadline:
                    try:
                        await page.wait_for_timeout(120)
                    except Exception:
                        break
                    header_confirmed = await self._chat_header_matches_target(page, username)
                if header_confirmed:
                    self._log_event(
                        "THREAD_ALREADY_OPEN_HEADER_CONFIRMED",
                        row_username=candidate_username or "-",
                        target_username=target,
                        current_thread=current_thread or "-",
                        previous_thread=previous_thread or "-",
                        url=current_url,
                        matched_via="sidebar_existing_thread_header",
                        header_match=header_confirmed,
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

    async def _clear_search_input_best_effort(self, page: Page, search_input: Locator) -> None:
        try:
            await self._sender._focus_input_best_effort(search_input)
            await self._sender._clear_input_best_effort(page, search_input)
            await self._wait_for_search_value(page, search_input, "", timeout_ms=700)
        except Exception:
            return None

    async def _try_open_existing_sidebar_thread(
        self,
        page: Page,
        username: str,
        *,
        deadline: float,
        flow_hook: Optional[Callable[[str, bool], None]] = None,
    ) -> Optional[ThreadOpenResult]:
        target = self._sender._normalize_username(username).lower()
        if not target:
            return ThreadOpenResult(False, "invalid_username", method="sidebar_search")
        search_timeout = min(1_200, self._sender._remaining_ms(deadline, 1_200))
        if search_timeout <= 0:
            return None
        search_input = await self._wait_sidebar_search_input(page, timeout_ms=search_timeout)
        if search_input is None:
            return ThreadOpenResult(False, "sidebar_unavailable", method="sidebar_search")
        try:
            baseline_probe = await self._probe_sidebar_results(page)
            baseline_signature = str(baseline_probe.get("signature") or "").strip()
            if callable(flow_hook):
                flow_hook("typing existing thread search", True)
            await self.clear_and_type_search(page, search_input, username)
            if callable(flow_hook):
                flow_hook("waiting existing thread results", True)
            wait_timeout = self._sender._remaining_ms(deadline, self._sidebar_row_timeout_ms)
            if wait_timeout <= 0:
                return ThreadOpenResult(False, "thread_open_failed")
            probe = await self.wait_sidebar_results_ready(
                page,
                username,
                timeout_ms=wait_timeout,
                baseline_signature=baseline_signature,
            )
            row = await self.find_exact_sidebar_row(page, username, timeout_ms=wait_timeout)
            if self._sender._is_chrome_error_url(page):
                return ThreadOpenResult(False, "chrome_error")
            surface = await self._sidebar_surface_state(page)
            if surface and not bool(surface.get("usable")):
                return ThreadOpenResult(False, "sidebar_unavailable", method="sidebar_search")
            query_value = self._sender._normalize_username(str(probe.get("query_value") or "")).lower()
            if query_value and query_value != target:
                return ThreadOpenResult(False, "sidebar_unavailable", method="sidebar_search")
            if row is None:
                return ThreadOpenResult(False, "username_not_found", method="sidebar_search")
            open_timeout = self._sender._remaining_ms(deadline, self._thread_open_timeout_ms)
            if open_timeout <= 0:
                return ThreadOpenResult(False, "thread_open_failed", method="sidebar_search")
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
                method="sidebar_search",
                thread_id=current_thread,
            )
        finally:
            await self._clear_search_input_best_effort(page, search_input)
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
        cleaned = await self.cleanup_stale_compose_state(page, deadline=deadline)
        if not cleaned:
            return ThreadOpenResult(False, "ui_not_found")
        result = await self._try_open_existing_sidebar_thread(
            page,
            username,
            deadline=deadline,
            flow_hook=flow_hook,
        )
        if result is None:
            return ThreadOpenResult(False, "username_not_found", method="sidebar_search")
        return result
