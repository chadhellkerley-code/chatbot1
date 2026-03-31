from __future__ import annotations

import asyncio
import random
import time
<<<<<<< HEAD
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional
=======
from typing import TYPE_CHECKING, Callable, Optional
>>>>>>> origin/main

from playwright.async_api import Locator, Page

if TYPE_CHECKING:
    from src.transport.human_instagram_sender import HumanInstagramSender


class MessageComposer:
    def __init__(
        self,
        sender: "HumanInstagramSender",
        *,
        thread_composers: tuple[str, ...],
        send_buttons: tuple[str, ...],
        composer_visible_timeout_ms: int,
<<<<<<< HEAD
        usable_composer_timeout_ms: int,
=======
>>>>>>> origin/main
        type_delay_min_ms: int,
        type_delay_max_ms: int,
        log_event: Callable[..., None],
    ) -> None:
        self._sender = sender
        self._thread_composers = tuple(thread_composers)
        self._send_buttons = tuple(send_buttons)
        self._composer_visible_timeout_ms = int(composer_visible_timeout_ms)
<<<<<<< HEAD
        self._usable_composer_timeout_ms = int(usable_composer_timeout_ms)
=======
>>>>>>> origin/main
        self._type_delay_min_ms = int(type_delay_min_ms)
        self._type_delay_max_ms = int(type_delay_max_ms)
        self._log_event = log_event

<<<<<<< HEAD
    @staticmethod
    def _surface_failed_checks(
        meta: Dict[str, Any],
        *,
        usable_composer_confirmed: bool = False,
    ) -> list[str]:
        failures: list[str] = []
        if not bool(meta.get("composer_found")):
            failures.append("composer_not_found")
            return failures
        if not bool(meta.get("composer_fully_inside_viewport")):
            failures.append("composer_not_fully_inside_viewport")
        if bool(meta.get("composer_below_fold")):
            failures.append("composer_below_fold")
        if bool(meta.get("composer_overlapped")):
            failures.append("composer_overlapped")
        if (
            bool(meta.get("scrollable_ancestor_found"))
            and not bool(meta.get("scroll_near_bottom"))
            and not usable_composer_confirmed
        ):
            failures.append("scroll_near_bottom")
        return failures

    @classmethod
    def _surface_failure_reason(
        cls,
        meta: Dict[str, Any],
        *,
        usable_composer_confirmed: bool = False,
    ) -> str:
        failures = cls._surface_failed_checks(meta, usable_composer_confirmed=usable_composer_confirmed)
        if "composer_overlapped" in failures:
            return "COMPOSER_OVERLAPPED"
        if "composer_below_fold" in failures or "composer_not_fully_inside_viewport" in failures:
            return "COMPOSER_OUT_OF_VIEW"
        if "scroll_near_bottom" in failures:
            return "CHAT_SCROLL_NOT_READY"
        if "composer_not_found" in failures:
            return "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION"
        return "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION"

    @classmethod
    def _surface_ready(
        cls,
        meta: Dict[str, Any],
        *,
        usable_composer_confirmed: bool = False,
    ) -> bool:
        return not cls._surface_failed_checks(meta, usable_composer_confirmed=usable_composer_confirmed)

    async def _focus_composer_best_effort(self, composer: Locator) -> bool:
        try:
            focused = bool(
                await composer.evaluate(
                    "el => document.activeElement === el || !!(document.activeElement && el.contains(document.activeElement))"
                )
            )
        except Exception:
            focused = False
        if focused:
            return True
        try:
            await composer.focus(timeout=400)
            return True
        except Exception:
            pass
        try:
            await composer.click(timeout=700)
            return True
        except Exception:
            pass
        try:
            await composer.click(timeout=700, force=True)
            return True
        except Exception:
            pass
        try:
            return bool(
                await composer.evaluate(
                    """el => {
                        if (!(el instanceof HTMLElement)) return false;
                        if (typeof el.focus === "function") el.focus();
                        return document.activeElement === el || !!(document.activeElement && el.contains(document.activeElement));
                    }"""
                )
            )
        except Exception:
            return False

    async def _composer_outside_overlay(self, candidate: Locator) -> bool:
        try:
            in_overlay = await candidate.evaluate(
                "el => !!el && !!el.closest('[role=\"dialog\"], [aria-modal=\"true\"]')"
            )
        except Exception:
            in_overlay = False
        return not bool(in_overlay)

    async def _composer_typeable(self, candidate: Locator) -> bool:
        try:
            usable = await candidate.evaluate(
                """el => {
                    if (!(el instanceof HTMLElement)) return false;
                    if (el.closest('[role="dialog"], [aria-modal="true"]')) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                        return false;
                    }
                    const rect = el.getBoundingClientRect();
                    if (rect.width < 24 || rect.height < 18) return false;
                    const disabled = "disabled" in el ? Boolean(el.disabled) : false;
                    const readOnly = "readOnly" in el ? Boolean(el.readOnly) : false;
                    const role = String(el.getAttribute("role") || "").toLowerCase();
                    const contentEditable = String(el.getAttribute("contenteditable") || "").toLowerCase();
                    const tag = String(el.tagName || "").toLowerCase();
                    const typeable =
                        tag === "textarea" ||
                        tag === "input" ||
                        role === "textbox" ||
                        contentEditable === "true" ||
                        el.isContentEditable;
                    return typeable && !disabled && !readOnly && typeof el.focus === "function";
                }"""
            )
        except Exception:
            return False
        if not bool(usable):
            return False
        try:
            await candidate.focus(timeout=400)
        except Exception:
            try:
                return bool(
                    await candidate.evaluate(
                        """el => {
                            if (!(el instanceof HTMLElement) || typeof el.focus !== "function") return false;
                            el.focus();
                            return document.activeElement === el || !!(document.activeElement && el.contains(document.activeElement));
                        }"""
                    )
                )
            except Exception:
                return False
        try:
            return bool(
                await candidate.evaluate(
                    "el => document.activeElement === el || !!(document.activeElement && el.contains(document.activeElement))"
                )
            )
        except Exception:
            return True

    async def thread_composer(self, page: Page, *, require_usable: bool = False) -> Optional[Locator]:
=======
    async def thread_composer(self, page: Page) -> Optional[Locator]:
>>>>>>> origin/main
        for sel in self._thread_composers:
            loc = page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                continue
            for idx in range(min(count, 6)):
                candidate = loc.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue
<<<<<<< HEAD
                if not await self._composer_outside_overlay(candidate):
                    continue
                if require_usable and not await self._composer_typeable(candidate):
                    continue
                return candidate
=======
                try:
                    in_overlay = await candidate.evaluate(
                        "el => !!el && !!el.closest('[role=\"dialog\"], [aria-modal=\"true\"]')"
                    )
                except Exception:
                    in_overlay = False
                if not in_overlay:
                    return candidate
>>>>>>> origin/main
        return None

    async def wait_composer_visible(self, page: Page, *, deadline: float) -> Optional[Locator]:
        timeout_ms = self._sender._remaining_ms(deadline, self._composer_visible_timeout_ms)
        if timeout_ms <= 0:
            return None
        timeout_at = time.time() + (timeout_ms / 1000.0)
        while time.time() < timeout_at:
            composer = await self.thread_composer(page)
            if composer is not None:
                self._log_event("COMPOSER_VISIBLE", url=page.url if page else "")
                return composer
            try:
                await page.wait_for_timeout(120)
            except Exception:
                break
        return None

<<<<<<< HEAD
    async def wait_for_usable_composer(self, page: Page, *, deadline: float) -> Optional[Locator]:
        timeout_ms = self._sender._remaining_ms(deadline, self._usable_composer_timeout_ms)
        if timeout_ms <= 0:
            return None
        timeout_at = time.time() + (timeout_ms / 1000.0)
        while time.time() < timeout_at:
            composer = await self.thread_composer(page, require_usable=True)
            if composer is not None:
                self._log_event("COMPOSER_USABLE", url=page.url if page else "")
                return composer
            try:
                await page.wait_for_timeout(120)
            except Exception:
                break
        return None

    async def audit_post_open_surface(
        self,
        page: Page,
        *,
        composer: Optional[Locator],
    ) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "supported": False,
            "url": page.url if page else "",
            "composer_found": composer is not None,
            "diagnostic_reason_codes": [],
        }
        if composer is None:
            return meta
        if not callable(getattr(page, "evaluate", None)) or not callable(getattr(composer, "evaluate", None)):
            return meta

        try:
            page_meta = await page.evaluate(
                """() => {
                    const isVisible = (node) => {
                        if (!(node instanceof HTMLElement)) return false;
                        const style = window.getComputedStyle(node);
                        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
                            return false;
                        }
                        const rect = node.getBoundingClientRect();
                        if (rect.width < 6 || rect.height < 6) return false;
                        if (rect.bottom <= 0 || rect.right <= 0) return false;
                        if (rect.top >= window.innerHeight || rect.left >= window.innerWidth) return false;
                        return true;
                    };
                    const normalize = (value) => String(value || "").replace(/\\s+/g, " ").trim();
                    const header =
                        document.querySelector("[role='main'] header") ||
                        document.querySelector("main header") ||
                        document.querySelector("header");
                    const descendants = [];
                    if (header instanceof HTMLElement) {
                        const nodes = [header, ...Array.from(header.querySelectorAll("*"))];
                        for (const node of nodes) {
                            if (!(node instanceof HTMLElement) || !isVisible(node)) continue;
                            const text = normalize(node.innerText || node.textContent || "");
                            const href = normalize(node.getAttribute("href"));
                            const ariaLabel = normalize(node.getAttribute("aria-label"));
                            const title = normalize(node.getAttribute("title"));
                            if (!text && !href && !ariaLabel && !title) continue;
                            const rect = node.getBoundingClientRect();
                            descendants.push({
                                tag: String(node.tagName || "").toLowerCase(),
                                role: normalize(node.getAttribute("role")),
                                href,
                                aria_label: ariaLabel,
                                title_attr: title,
                                text,
                                rect: {
                                    top: rect.top,
                                    bottom: rect.bottom,
                                    left: rect.left,
                                    right: rect.right,
                                    width: rect.width,
                                    height: rect.height,
                                },
                            });
                            if (descendants.length >= 24) break;
                        }
                    }
                    const titleText =
                        descendants.find((item) => item.title_attr)?.title_attr ||
                        descendants.find((item) => item.tag === "h1" || item.tag === "h2")?.text ||
                        descendants.find((item) => item.href && item.text)?.text ||
                        descendants.find((item) => item.text)?.text ||
                        "";
                    const subtitleTexts = descendants
                        .map((item) => item.text)
                        .filter((text) => text && text !== titleText)
                        .slice(0, 8);
                    const partialHydration =
                        /^(usuario de instagram|instagram user)$/i.test(String(titleText || "").trim()) &&
                        subtitleTexts.some((text) => /(activo\\/?a hace|active .* ago)/i.test(text));
                    return {
                        viewport: {
                            inner_width: Number(window.innerWidth || 0),
                            inner_height: Number(window.innerHeight || 0),
                            client_width: Number(document.documentElement?.clientWidth || 0),
                            client_height: Number(document.documentElement?.clientHeight || 0),
                            device_pixel_ratio: Number(window.devicePixelRatio || 1),
                            visual_viewport: window.visualViewport
                                ? {
                                    width: Number(window.visualViewport.width || 0),
                                    height: Number(window.visualViewport.height || 0),
                                    offset_top: Number(window.visualViewport.offsetTop || 0),
                                    offset_left: Number(window.visualViewport.offsetLeft || 0),
                                    page_top: Number(window.visualViewport.pageTop || 0),
                                    page_left: Number(window.visualViewport.pageLeft || 0),
                                  }
                                : null,
                            scroll_x: Number(window.scrollX || 0),
                            scroll_y: Number(window.scrollY || 0),
                        },
                        header_title: titleText,
                        header_subtitles: subtitleTexts,
                        header_descendants: descendants,
                        partial_hydration: partialHydration,
                    };
                }"""
            )
        except Exception:
            return meta

        try:
            composer_meta = await composer.evaluate(
                """(el) => {
                    const normalize = (value) => String(value || "").replace(/\\s+/g, " ").trim();
                    if (!(el instanceof HTMLElement)) {
                        return { composer_found: false };
                    }
                    const vv = window.visualViewport;
                    const usableBottom = Math.min(
                        Number(window.innerHeight || 0),
                        vv ? Number(vv.height || 0) || Number(window.innerHeight || 0) : Number(window.innerHeight || 0)
                    );
                    const usableRight = Math.min(
                        Number(window.innerWidth || 0),
                        vv ? Number(vv.width || 0) || Number(window.innerWidth || 0) : Number(window.innerWidth || 0)
                    );
                    const rect = el.getBoundingClientRect();
                    const centerX = rect.left + (rect.width / 2);
                    const sampleX = Math.min(Math.max(centerX, 1), Math.max(1, usableRight - 1));
                    const sampleY = Math.min(Math.max(rect.bottom - 1, 1), Math.max(1, usableBottom - 1));
                    const topNode = document.elementFromPoint(sampleX, sampleY);
                    const topElement = topNode instanceof HTMLElement ? topNode : topNode?.parentElement || null;
                    const fullyInsideViewport =
                        rect.top >= 0 &&
                        rect.left >= 0 &&
                        rect.bottom <= usableBottom &&
                        rect.right <= usableRight;
                    const partiallyVisible =
                        rect.bottom > 0 &&
                        rect.right > 0 &&
                        rect.top < usableBottom &&
                        rect.left < usableRight;
                    const belowFold = rect.top >= usableBottom || rect.bottom > usableBottom;
                    const placeholderText = normalize(el.getAttribute("aria-placeholder") || el.getAttribute("placeholder"));
                    const topHiddenAncestor =
                        topElement instanceof HTMLElement ? topElement.closest("[aria-hidden='true']") : null;
                    const harmlessOverlapReason = (() => {
                        if (!(topElement instanceof HTMLElement) || topElement === el || el.contains(topElement)) {
                            return "";
                        }
                        const sharedContainer = el.parentElement instanceof HTMLElement ? el.parentElement : null;
                        if (!(sharedContainer instanceof HTMLElement)) return "";
                        const sameSurfaceContainer =
                            topElement.parentElement === sharedContainer ||
                            (topHiddenAncestor instanceof HTMLElement && topHiddenAncestor.parentElement === sharedContainer);
                        if (!sameSurfaceContainer) return "";
                        const topRole = normalize(topElement.getAttribute("role")).toLowerCase();
                        const topText = normalize(topElement.textContent || "").slice(0, 160);
                        const samePlaceholderText =
                            !!placeholderText &&
                            !!topText &&
                            (topText === placeholderText ||
                                topText.startsWith(placeholderText) ||
                                placeholderText.startsWith(topText));
                        if (samePlaceholderText && topHiddenAncestor instanceof HTMLElement) {
                            return "same_surface_placeholder_shell";
                        }
                        if (
                            topHiddenAncestor instanceof HTMLElement &&
                            (topRole === "" || topRole === "presentation" || topRole === "none")
                        ) {
                            return "same_surface_hidden_helper";
                        }
                        return "";
                    })();
                    const overlapped =
                        !!topElement &&
                        topElement !== el &&
                        !el.contains(topElement) &&
                        !harmlessOverlapReason;
                    const mainRoot = el.closest("[role='main'], main");
                    const findScrollable = (start) => {
                        let node = start instanceof HTMLElement ? start.parentElement : null;
                        while (node && node instanceof HTMLElement) {
                            if (mainRoot instanceof HTMLElement && !mainRoot.contains(node)) break;
                            const style = window.getComputedStyle(node);
                            const overflowY = String(style.overflowY || "").toLowerCase();
                            const allowsScroll =
                                overflowY.includes("auto") ||
                                overflowY.includes("scroll") ||
                                overflowY.includes("overlay");
                            const scrollHeight = Number(node.scrollHeight || 0);
                            const clientHeight = Number(node.clientHeight || 0);
                            if (allowsScroll && scrollHeight - clientHeight > 4) {
                                return node;
                            }
                            if (node === mainRoot) break;
                            node = node.parentElement;
                        }
                        if (mainRoot instanceof HTMLElement) {
                            const style = window.getComputedStyle(mainRoot);
                            const overflowY = String(style.overflowY || "").toLowerCase();
                            const allowsScroll =
                                overflowY.includes("auto") ||
                                overflowY.includes("scroll") ||
                                overflowY.includes("overlay");
                            const scrollHeight = Number(mainRoot.scrollHeight || 0);
                            const clientHeight = Number(mainRoot.clientHeight || 0);
                            if (allowsScroll && scrollHeight - clientHeight > 4) {
                                return mainRoot;
                            }
                        }
                        return null;
                    };
                    const scrollNode = findScrollable(el);
                    const scrollTop = Number(scrollNode?.scrollTop || 0);
                    const scrollHeight = Number(scrollNode?.scrollHeight || 0);
                    const clientHeight = Number(scrollNode?.clientHeight || 0);
                    const maxScrollTop = Math.max(0, scrollHeight - clientHeight);
                    const nearBottom = !scrollNode || maxScrollTop <= 4 || scrollTop >= (maxScrollTop - 4);
                    return {
                        composer_found: true,
                        composer_rect: {
                            top: rect.top,
                            bottom: rect.bottom,
                            left: rect.left,
                            right: rect.right,
                            width: rect.width,
                            height: rect.height,
                        },
                        composer_fully_inside_viewport: fullyInsideViewport,
                        composer_partially_visible: partiallyVisible,
                        composer_below_fold: belowFold,
                        composer_overlapped: overlapped,
                        composer_overlap_ignored: !!harmlessOverlapReason,
                        composer_overlap_reason: harmlessOverlapReason || (overlapped ? "blocking_element" : ""),
                        overlap_sample: {
                            x: sampleX,
                            y: sampleY,
                            top_tag: normalize(topElement?.tagName).toLowerCase(),
                            top_role: normalize(topElement?.getAttribute?.("role")),
                            top_text: normalize(topElement?.textContent).slice(0, 120),
                            top_aria_hidden: normalize(topHiddenAncestor?.getAttribute?.("aria-hidden")),
                        },
                        scrollable_ancestor_found: !!scrollNode,
                        scrollable_ancestor: scrollNode
                            ? {
                                tag: normalize(scrollNode.tagName).toLowerCase(),
                                role: normalize(scrollNode.getAttribute?.("role")),
                                aria_label: normalize(scrollNode.getAttribute?.("aria-label")),
                            }
                            : null,
                        scroll_top: scrollTop,
                        scroll_height: scrollHeight,
                        client_height: clientHeight,
                        max_scroll_top: maxScrollTop,
                        scroll_near_bottom: nearBottom,
                    };
                }"""
            )
        except Exception:
            return meta

        meta.update(dict(page_meta) if isinstance(page_meta, dict) else {})
        meta.update(dict(composer_meta) if isinstance(composer_meta, dict) else {})
        meta["supported"] = True
        diagnostic_codes = list(meta.get("diagnostic_reason_codes") or [])
        if bool(meta.get("partial_hydration")) and "HEADER_PARTIAL_HYDRATION" not in diagnostic_codes:
            diagnostic_codes.append("HEADER_PARTIAL_HYDRATION")
        meta["diagnostic_reason_codes"] = diagnostic_codes
        meta["surface_failed_checks"] = self._surface_failed_checks(meta)
        return meta

    async def _normalize_post_open_surface(self, page: Page, composer: Locator) -> Dict[str, Any]:
        actions: list[str] = []
        try:
            await composer.scroll_into_view_if_needed(timeout=1_200)
            actions.append("scroll_into_view_if_needed")
        except Exception:
            pass
        try:
            moved = bool(
                await composer.evaluate(
                    """el => {
                        if (!(el instanceof HTMLElement) || typeof el.scrollIntoView !== "function") return false;
                        el.scrollIntoView({ block: "center", inline: "nearest" });
                        return true;
                    }"""
                )
            )
            if moved:
                actions.append("composer_scroll_into_view")
        except Exception:
            pass
        try:
            scroll_meta = await composer.evaluate(
                """(el) => {
                    if (!(el instanceof HTMLElement)) return { found: false };
                    const mainRoot = el.closest("[role='main'], main");
                    const findScrollable = (start) => {
                        let node = start instanceof HTMLElement ? start.parentElement : null;
                        while (node && node instanceof HTMLElement) {
                            if (mainRoot instanceof HTMLElement && !mainRoot.contains(node)) break;
                            const style = window.getComputedStyle(node);
                            const overflowY = String(style.overflowY || "").toLowerCase();
                            const allowsScroll =
                                overflowY.includes("auto") ||
                                overflowY.includes("scroll") ||
                                overflowY.includes("overlay");
                            const scrollHeight = Number(node.scrollHeight || 0);
                            const clientHeight = Number(node.clientHeight || 0);
                            if (allowsScroll && scrollHeight - clientHeight > 4) {
                                return node;
                            }
                            if (node === mainRoot) break;
                            node = node.parentElement;
                        }
                        return null;
                    };
                    const node = findScrollable(el);
                    if (!(node instanceof HTMLElement)) return { found: false };
                    const maxScrollTop = Math.max(0, Number(node.scrollHeight || 0) - Number(node.clientHeight || 0));
                    try {
                        node.scrollTop = maxScrollTop;
                    } catch (_error) {
                    }
                    return {
                        found: true,
                        scroll_top: Number(node.scrollTop || 0),
                        max_scroll_top: maxScrollTop,
                    };
                }"""
            )
            if isinstance(scroll_meta, dict) and bool(scroll_meta.get("found")):
                actions.append("scroll_chat_container_to_bottom")
        except Exception:
            pass
        try:
            await page.wait_for_timeout(120)
        except Exception:
            pass
        focused = await self._focus_composer_best_effort(composer)
        if focused:
            actions.append("focus_composer")
        return {"actions": actions}

    async def ensure_visible_chat_surface_ready(
        self,
        page: Page,
        *,
        deadline: float,
    ) -> tuple[Optional[Locator], Dict[str, Any]]:
        visible_composer = await self.wait_composer_visible(page, deadline=deadline)
        if visible_composer is None:
            meta = {
                "ok": False,
                "reason_code": "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION",
                "supported": True,
                "diagnostic_reason_codes": [],
                "normalized": False,
                "normalization": {"actions": []},
                "before": {"composer_found": False, "supported": True},
                "after": {"composer_found": False, "supported": True},
                "failed_checks": ["composer_not_found"],
            }
            self._log_event(
                "POST_OPEN_SURFACE_FAIL",
                reason_code=meta["reason_code"],
                failed_checks="|".join(meta["failed_checks"]) or "-",
                url=page.url if page else "",
            )
            return None, meta

        before = await self.audit_post_open_surface(page, composer=visible_composer)
        before["surface_failed_checks"] = self._surface_failed_checks(before)
        supported = bool(before.get("supported", False))
        if not supported:
            composer = await self.wait_for_usable_composer(page, deadline=deadline)
            meta = {
                "ok": composer is not None,
                "reason_code": "" if composer is not None else "COMPOSER_NOT_USABLE_AFTER_NORMALIZATION",
                "supported": False,
                "diagnostic_reason_codes": list(before.get("diagnostic_reason_codes") or []),
                "normalized": False,
                "normalization": {"actions": []},
                "before": before,
                "after": before,
                "failed_checks": [] if composer is not None else self._surface_failed_checks(before),
            }
            return (composer or visible_composer), meta

        normalization = {"actions": []}
        normalized = False
        if not self._surface_ready(before):
            normalization = await self._normalize_post_open_surface(page, visible_composer)
            normalized = bool(normalization.get("actions"))

        composer = await self.wait_for_usable_composer(page, deadline=deadline)
        after = await self.audit_post_open_surface(page, composer=composer or visible_composer)
        after["surface_failed_checks"] = self._surface_failed_checks(
            after,
            usable_composer_confirmed=composer is not None,
        )
        diagnostic_codes = list(before.get("diagnostic_reason_codes") or [])
        for code in list(after.get("diagnostic_reason_codes") or []):
            if code not in diagnostic_codes:
                diagnostic_codes.append(code)
        reason_code = ""
        failed_checks = list(after.get("surface_failed_checks") or [])
        ok = composer is not None and self._surface_ready(after, usable_composer_confirmed=True)
        if not ok:
            reason_code = self._surface_failure_reason(after or before, usable_composer_confirmed=composer is not None)
        meta = {
            "ok": ok,
            "reason_code": reason_code,
            "supported": True,
            "diagnostic_reason_codes": diagnostic_codes,
            "normalized": normalized,
            "normalization": normalization,
            "before": before,
            "after": after,
            "failed_checks": [] if ok else failed_checks,
        }
        if ok:
            self._log_event(
                "POST_OPEN_SURFACE_READY",
                normalized=normalized,
                diagnostic_reason_codes="|".join(diagnostic_codes) or "-",
                failed_checks="|".join(failed_checks) or "-",
                url=page.url if page else "",
            )
        else:
            self._log_event(
                "POST_OPEN_SURFACE_FAIL",
                reason_code=reason_code,
                normalized=normalized,
                diagnostic_reason_codes="|".join(diagnostic_codes) or "-",
                failed_checks="|".join(failed_checks) or "-",
                url=page.url if page else "",
            )
        return (composer or visible_composer), meta

    async def focus_and_clear_composer(self, page: Page, composer: Locator) -> None:
        await self._focus_composer_best_effort(composer)
=======
    async def focus_and_clear_composer(self, page: Page, composer: Locator) -> None:
        await composer.click()
>>>>>>> origin/main
        try:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Delete")
            return
        except Exception:
            pass
        try:
            await composer.fill("")
        except Exception:
            pass

    async def type_message(self, page: Page, composer: Locator, text: str) -> None:
        payload = (text or "").replace("\r\n", "\n")
        if not payload.strip():
            raise ValueError("empty_message")

        await self.focus_and_clear_composer(page, composer)
        lines = payload.split("\n")
        for idx, part in enumerate(lines):
            if idx > 0:
                try:
                    await page.keyboard.press("Shift+Enter")
                except Exception:
                    await composer.press("Shift+Enter")
                await self._sender._sleep(0.05, 0.14)
            if not part:
                continue
            try:
                await composer.type(part, delay=random.randint(self._type_delay_min_ms, self._type_delay_max_ms))
            except Exception:
                await page.keyboard.type(part, delay=random.randint(self._type_delay_min_ms, self._type_delay_max_ms))
            await self._sender._sleep(0.03, 0.12)

    async def composer_text(self, composer: Locator) -> str:
        try:
            value = await composer.input_value()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        try:
            value = await composer.inner_text()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        try:
            value = await composer.text_content()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        return ""

    async def wait_for_text_change(
        self,
        composer: Locator,
        *,
        previous_text: str,
        timeout_ms: int,
    ) -> str:
        deadline = time.time() + (max(80, int(timeout_ms or 0)) / 1000.0)
        baseline = str(previous_text or "").strip()
        last_value = baseline
        while time.time() < deadline:
            current = await self.composer_text(composer)
            last_value = current
            if current.strip() != baseline:
                return current
            await asyncio.sleep(0.08)
        return last_value

    async def click_send_button(self, page: Page) -> bool:
        for sel in self._send_buttons:
            btn = page.locator(sel)
            try:
                count = await btn.count()
            except Exception:
                continue
            for idx in range(min(count, 3)):
                candidate = btn.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    await candidate.click()
                    self._log_event("SEND_FALLBACK_CLICK", selector=sel, index=idx, url=page.url if page else "")
                    return True
                except Exception:
                    continue
        return False
