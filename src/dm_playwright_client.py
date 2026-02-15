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
from typing import List, Optional

from ui import Fore, style_text

try:  # pragma: no cover - optional dependency guard
    from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
except Exception:  # pragma: no cover
    Page = object  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore
    sync_playwright = None  # type: ignore

from src.playwright_service import (
    BASE_PROFILES,
    DEFAULT_ARGS,
    DEFAULT_LOCALE,
    DEFAULT_TIMEZONE,
    DEFAULT_USER_AGENT,
    DEFAULT_VIEWPORT,
    resolve_playwright_executable,
)

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

# Selectores mínimos necesarios (eliminadas constantes legacy de anchors)
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
        return Path(app_root).expanduser() / "storage" / "accounts_status.json"
    return Path(__file__).resolve().parents[1] / "storage" / "accounts_status.json"


def log_account_status(username: str, status: str) -> None:
    user = str(username or "").strip().lstrip("@")
    current = str(status or "unknown").strip().lower() or "unknown"
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
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    payload = loaded
            except Exception:
                payload = {}
        payload[user] = {
            "username": user,
            "status": current,
            "last_checked": now_utc,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _safe_page_text(page: Page) -> str:
    try:
        body = page.locator("body")
        if body.count() > 0:
            return (body.first.inner_text() or "").strip().lower()
    except Exception:
        pass
    try:
        html = page.content() or ""
    except Exception:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", html)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def _detect_account_status_impl(page: Page) -> str:
    try:
        url = (getattr(page, "url", "") or "").lower()
    except Exception:
        url = ""

    try:
        if "/accounts/login/" in url or "/accounts/login" in url:
            return "session_expired"
        if "/challenge/" in url or "/checkpoint/" in url or "two_factor" in url:
            return "checkpoint"
        if "/accounts/suspended/" in url:
            return "suspended"
        if "/accounts/disabled/" in url:
            return "blocked"

        try:
            if page.locator("input[name='username'], input[name='password']").count() > 0:
                return "session_expired"
        except Exception:
            pass

        text = _safe_page_text(page)
        if "temporarily blocked" in text or "bloqueada temporalmente" in text:
            return "blocked"
        if "disabled your account" in text or "your account has been disabled" in text:
            return "blocked"
        if "suspended" in text or "cuenta suspendida" in text:
            return "suspended"
        if "checkpoint" in text or "challenge required" in text:
            return "checkpoint"

        alive_selector = ",".join(
            (
                "a[href='/direct/inbox/']",
                "a[href*='/direct/inbox/']",
                "a[href*='/direct/t/']",
                "a[aria-label='Direct']",
                "a[aria-label='Mensajes']",
                "input[placeholder='Buscar']",
                "input[placeholder='Search']",
                "input[name='queryBox']",
                "div[role='textbox'][contenteditable='true']",
                "textarea[placeholder*='Message']",
                "textarea[placeholder*='Mensaje']",
                "svg[aria-label='Home']",
                "svg[aria-label='Inicio']",
            )
        )
        try:
            if page.locator(alive_selector).count() > 0:
                return "alive"
        except Exception:
            pass

        try:
            page.wait_for_selector(alive_selector, timeout=_status_check_timeout_ms())
            return "alive"
        except Exception:
            pass

        if "/direct/inbox/" in url or "/direct/t/" in url:
            return "alive"

        return "unknown"
    except Exception:
        return "unknown"


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
    source_index: int = -1


@dataclass
class MessageLike:
    id: str
    user_id: str
    text: str
    timestamp: Optional[float]
    direction: str = "inbound"  # "inbound" or "outbound"


class PlaywrightDMClient:
    def __init__(
        self,
        *,
        account: Optional[dict] = None,
        headless: bool = False,
        slow_mo_ms: int = 1000,
        dump_on_error: bool = True,
    ) -> None:
        if sync_playwright is None:
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

        self._playwright = None
        self._browser = None
        self._context = None
        self._page: Optional[Page] = None
        self._current_thread_id: Optional[str] = None
        self._thread_cache: dict[str, ThreadLike] = {}
        self._thread_cache_meta: dict[str, dict] = {}
        self._account_status_checked = False

    @staticmethod
    def storage_state_path(username: str) -> Path:
        safe = (username or "").strip().lstrip("@")
        return Path(BASE_PROFILES) / safe / "storage_state.json"

    def close(self) -> None:
        try:
            if self._page is not None:
                try:
                    if not self._page.is_closed():
                        self._page.close()
                except Exception:
                    pass
            if self._context is not None:
                try:
                    self._context.close()
                except Exception:
                    pass
            if self._browser is not None:
                try:
                    self._browser.close()
                except Exception:
                    pass
        finally:
            if self._playwright is not None:
                try:
                    self._playwright.stop()
                except Exception:
                    pass
            self._playwright = None
            self._browser = None
            self._context = None
            self._page = None
            self._current_thread_id = None
            self._account_status_checked = False

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
        self._open_inbox()

    def list_threads(self, amount: int = 20, filter_unread: bool = False) -> List[ThreadLike]:
        """
        Discovery de inbox: carga progresiva de threads con scroll del panel lateral.
        Soporta cantidades altas (p.ej. 100/500) sin quedarse solo en los visibles iniciales.
        """
        page = self._ensure_page()
        self._open_inbox()

        selector_candidates = self._row_selector_candidates()

        threads: List[ThreadLike] = []
        seen_titles = set()
        rows = None
        selected_selector = ""
        for selector in selector_candidates:
            try:
                candidate = page.locator(selector)
                total = candidate.count()
                if _DM_VERBOSE_PROBES:
                    print(style_text(f"[Probe] Selector '{selector}' -> count={total}", color=Fore.WHITE))
                if total > 0:
                    rows = candidate
                    selected_selector = selector
                    break
            except Exception:
                continue

        if rows is None:
            return threads

        inbox_panel, _method, _selector, _panel_meta = self._get_inbox_panel(page, rows=rows)
        self._scroll_panel_to_top(inbox_panel)

        target = max(1, int(amount or 1))
        max_scroll_passes = max(25, min(2000, target * 6))
        stagnant_passes = 0

        for _pass in range(max_scroll_passes):
            before_count = len(threads)
            try:
                total = rows.count()
            except Exception:
                total = 0

            for idx in range(total):
                if len(threads) >= target:
                    break

                row = rows.nth(idx)
                if not self._row_is_valid(row, selector=selected_selector):
                    continue

                if filter_unread and _thread_unread_count(row) <= 0:
                    continue

                lines = self._row_lines(row)
                title = (lines[0] if lines else "unknown").strip()
                if not title:
                    continue
                title_key = title.lower()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                # ID estable por cuenta+título (se resuelve a id real al abrir thread).
                stable_id = f"{self.username}:{title}"

                thread = ThreadLike(
                    id=stable_id,
                    pk=stable_id,
                    users=[UserLike(pk=title, id=title, username=title)],
                    title=title,
                    # El índice deja de ser confiable cuando hay scroll/virtualización;
                    # forzamos apertura por cache/título para evitar clicks en fila incorrecta.
                    source_index=idx,
                )
                self._thread_cache[stable_id] = thread
                self._thread_cache_meta[stable_id] = {
                    "title": title,
                    "idx": idx,
                    "selector": selected_selector,
                }
                threads.append(thread)

            if len(threads) >= target:
                break

            try:
                before_scroll = inbox_panel.evaluate(
                    """(el) => ({
                        top: Number((el && el.scrollTop) || 0),
                        height: Number((el && el.scrollHeight) || 0)
                    })"""
                )
            except Exception:
                before_scroll = {"top": 0, "height": 0}

            moved = self._scroll_panel_down(inbox_panel)
            if not moved:
                break

            added = len(threads) - before_count
            try:
                total_after_scroll = rows.count()
            except Exception:
                total_after_scroll = total
            try:
                after_scroll = inbox_panel.evaluate(
                    """(el) => ({
                        top: Number((el && el.scrollTop) || 0),
                        height: Number((el && el.scrollHeight) || 0)
                    })"""
                )
            except Exception:
                after_scroll = {"top": 0, "height": 0}
            scroll_top_unchanged = float((after_scroll or {}).get("top", 0)) <= float((before_scroll or {}).get("top", 0)) + 1
            scroll_height_not_increased = float((after_scroll or {}).get("height", 0)) <= float((before_scroll or {}).get("height", 0))
            no_new_rows_detected = int(total_after_scroll) <= int(total)
            if added <= 0 and scroll_top_unchanged and scroll_height_not_increased and no_new_rows_detected:
                stagnant_passes += 1
            else:
                stagnant_passes = 0
            if stagnant_passes >= 8:
                break

            try:
                page.wait_for_timeout(150)
            except Exception:
                pass

        if _DM_VERBOSE_PROBES:
            print(
                style_text(
                    f"[Probe] list_threads target={target} discovered={len(threads)}",
                    color=Fore.WHITE,
                )
            )

        return threads

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

    # LEGACY: Función deshabilitada - ya no se usa (reemplazada por click-first scan)
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
        # Prioritize selectors scoped to actual inbox thread rows.
        return [
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='List of conversations'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'] div[role='button'][tabindex='0']:has(abbr[aria-label])",
            "div[role='navigation'][aria-label='Lista de conversaciones'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'][aria-label='Conversation list'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'][aria-label='List of conversations'] div[role='button'][tabindex='0']:has(abbr)",
            "div[role='navigation'] div[role='button'][tabindex='0']:has(abbr)",
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
                        print(style_text(f"[Probe] _get_inbox_panel encontró '{selector}'", color=Fore.WHITE))
                    return loc.first, "selector", selector, {"count": loc.count()}
            except Exception:
                continue
        if _DM_VERBOSE_PROBES:
            print(style_text("[Probe] _get_inbox_panel no encontró nada, usando page", color=Fore.YELLOW))
        return page, "page", "", {"count": 1}

    def _scroll_panel_to_top(self, panel) -> None:
        try:
            panel.evaluate(
                """(el) => {
                    if (!el) return;
                    try { el.scrollTop = 0; } catch (_) {}
                }"""
            )
            self._ensure_page().wait_for_timeout(120)
        except Exception:
            return

    def _scroll_panel_down(self, panel) -> bool:
        page = self._ensure_page()
        try:
            result = panel.evaluate(
                """(el) => {
                    if (!el) return { before: 0, after: 0, max: 0 };
                    const before = Number(el.scrollTop || 0);
                    const max = Math.max(0, Number((el.scrollHeight || 0) - (el.clientHeight || 0)));
                    const step = Math.max(350, Math.floor(Number(el.clientHeight || 600) * 0.9));
                    const next = Math.min(max, before + step);
                    try { el.scrollTop = next; } catch (_) {}
                    const after = Number(el.scrollTop || 0);
                    return { before, after, max };
                }"""
            )
            before = float((result or {}).get("before", 0))
            after = float((result or {}).get("after", 0))
            max_scroll = float((result or {}).get("max", 0))
            if after > before + 1:
                return True
            if before + 1 >= max_scroll:
                return False
        except Exception:
            pass

        # Fallback de rueda para UIs que no exponen scrollTop.
        try:
            try:
                box = panel.bounding_box()
            except Exception:
                box = None
            if box:
                x = float(box.get("x") or 0.0) + max(8.0, min(float(box.get("width") or 0.0) - 8.0, 40.0))
                y = float(box.get("y") or 0.0) + max(8.0, min(float(box.get("height") or 0.0) - 8.0, 40.0))
                try:
                    page.mouse.move(x, y)
                except Exception:
                    pass
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(120)
            return True
        except Exception:
            return False

    def _row_is_valid(self, row, *, selector: str | None = None) -> bool:
        """
        Validación mínima pre-click.
        El filtrado real ocurre POST-CLICK en _open_thread.
        """
        try:
            lines = self._row_lines(row)
            if not lines:
                return False

            # Filtros de exclusión de UI básica (incluye Notas)
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
            note_reason = self._note_reason(row)
            if note_reason:
                logger.info(
                    "PlaywrightDM row_discard reason=note selector=%s token=%s first_line=%s",
                    selector or "-",
                    note_reason,
                    self._row_preview(row),
                )
                return False

            if self._is_non_thread_control_row(lines):
                return False

            # Descartar botones internos (avatar/nota) por tamaño.
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
            "desde el corazón",
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
            if " · " in full and ("tú:" in full or "tu:" in full or "you:" in full):
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

    # LEGACY: Función deshabilitada - ya no se usa (reemplazada por click-first scan)
    # def _list_threads_from_rows(...) -> List[ThreadLike]


    def get_messages(self, thread: ThreadLike, amount: int = 20, *, log: bool = True) -> List[MessageLike]:
        page = self._ensure_page()

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
        except Exception as e:
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
            page.wait_for_selector(f"main {msg_selector}, div[role='main'] {msg_selector}", timeout=3000)
        except Exception:
            pass

        nodes = self._collect_message_nodes(page)
        total = nodes.count()
        scroll_up_triggered = False
        scroll_iterations = 0
        new_nodes_after_scroll = False
        dedup_total = total
        if log:
            dedup_keys: set[str] = set()
            for raw_idx in range(total):
                try:
                    raw_node = nodes.nth(raw_idx)
                    raw_msg_id = _extract_message_id(raw_node)
                    if raw_msg_id:
                        dedup_keys.add(f"id:{raw_msg_id}")
                    else:
                        raw_text = _extract_message_text(raw_node)
                        raw_ts = _extract_message_timestamp(raw_node)
                        raw_key = hashlib.sha1(
                            f"{raw_text}|{raw_ts}".encode("utf-8", errors="ignore")
                        ).hexdigest()[:16]
                        dedup_keys.add(f"fallback:{raw_key}")
                except Exception:
                    continue
            dedup_total = len(dedup_keys)
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s total_dom_nodes_before_parse=%d total_nodes_after_dedup=%d",
                thread.id,
                total,
                dedup_total,
            )
            logger.info(
                "[TRACE_MSG_DIAG] thread=%s scroll_up_triggered=%s scroll_iterations=%d new_nodes_after_scrolling=%s",
                thread.id,
                scroll_up_triggered,
                scroll_iterations,
                new_nodes_after_scroll,
            )
        if total <= 0:
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

        start_idx = max(0, total - max(1, amount))
        collected: List[MessageLike] = []
        used_fallback_ts = False
        for idx in range(start_idx, total):
            try:
                node = nodes.nth(idx)
                text = _extract_message_text(node)
                timestamp = _extract_message_timestamp(node)
                if timestamp is None:
                    timestamp = time.time()
                    used_fallback_ts = True
                outbound = self._is_outbound(node)
                direction = "outbound" if outbound else "inbound"
                user_id = self.user_id if outbound else _thread_peer_id(thread, self.user_id)
                msg_id = _extract_message_id(node)
                if not msg_id:
                    # Message ID estable recomendado
                    msg_id = hashlib.sha1(f"{text}|{timestamp}|{direction}".encode()).hexdigest()[:12]
                if log:
                    preview = (text or "").replace("\n", " ").replace("\r", " ")[:50]
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
            except Exception:
                continue

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
            if used_fallback_ts:
                logger.warning(
                    "PlaywrightDM timestamps ausentes en thread=%s @%s; usando now() como fallback",
                    thread.id,
                    self.username,
                )
        return collected

    def send_message(self, thread: ThreadLike, text: str) -> Optional[str]:
        page = self._ensure_page()
        self._open_thread(thread)

        composer = self._find_composer(page)
        if composer is None:
            logger.warning("PlaywrightDM sin composer thread=%s @%s", thread.id, self.username)
            return None

        try:
            composer.click()
            composer.fill(text)
            composer.press("Enter")
        except Exception as e:
            logger.warning("PlaywrightDM no pudo completar acciones de envío thread=%s @%s", thread.id, self.username)
            return None

        message_id = self._verify_sent(thread, text)
        if message_id:
            logger.info("PlaywrightDM envio_ok thread=%s msg_id=%s", thread.id, message_id)
        else:
            logger.warning("PlaywrightDM envio_no_verificado thread=%s @%s", thread.id, self.username)
        return message_id

    def _ensure_page(self) -> Page:
        if self._page is not None:
            return self._page

        if _DM_VERBOSE_PROBES:
            print(style_text(f"[PlaywrightDM] Iniciando navegador para @{self.username}...", color=Fore.WHITE))

        storage_state = self.storage_state_path(self.username)
        if not storage_state.exists():
            raise RuntimeError(f"No hay sesion Playwright guardada para @{self.username}.")

        self._playwright = sync_playwright().start()
        launch_kwargs = {
            "headless": self.headless,
            "args": list(DEFAULT_ARGS),
        }
        if self.slow_mo_ms > 0:
            launch_kwargs["slow_mo"] = self.slow_mo_ms
        executable = resolve_playwright_executable(headless=self.headless)
        if executable:
            launch_kwargs["executable_path"] = str(executable)
        self._browser = self._playwright.chromium.launch(**launch_kwargs)
        proxy_payload = _proxy_from_account(self.account)
        self._context = self._browser.new_context(
            storage_state=str(storage_state),
            proxy=proxy_payload or None,
            viewport=DEFAULT_VIEWPORT,
            user_agent=DEFAULT_USER_AGENT,
            locale=DEFAULT_LOCALE,
            timezone_id=DEFAULT_TIMEZONE,
            permissions=[],
            accept_downloads=False,
        )
        self._context.set_default_timeout(30_000)
        self._page = self._context.new_page()
        try:
            self._page.set_default_navigation_timeout(45_000)
        except Exception:
            pass
        return self._page

    def _open_inbox(self, force_reload: bool = False) -> None:
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
            if status != "alive":
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
            # Si ninguno es visible de inmediato, esperar brevemente al más probable
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
        logger.info("PlaywrightDM inbox_abierto account=@%s", self.username)
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
                        # Buscar botón de cierre dentro del dialog
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
        
        # CRÍTICO: Siempre re-chequear login inputs al final (no depender del chequeo inicial)
        # porque pueden aparecer después de cerrar otros overlays
        try:
            has_login_after_close = page.locator("input[name='username'], input[name='password']").count() > 0
            if has_login_after_close:
                raise RuntimeError(
                    f"Overlay de login detectado y no se pudo cerrar para @{self.username}. "
                    "Sesión posiblemente inválida."
                )
        except RuntimeError:
            raise
        except Exception:
            pass

    def return_to_inbox(self) -> None:
        """
        Vuelve a la vista del inbox sin recargar la página si es posible.
        """
        page = self._ensure_page()
        if INBOX_URL in (page.url or "") and not re.search(r"/direct/t/", page.url):
            return

        try:
            # Intentar click en panel lateral o go_back
            page.go_back(wait_until="domcontentloaded", timeout=5000)
        except Exception:
            self._open_inbox()

    def _open_thread(self, thread: ThreadLike) -> bool:
        """
        [CLICK-FIRST] Intenta abrir un thread y valida post-click.
        Retorna True si el thread es un DM real (/direct/t/ + composer visible).
        """
        page = self._ensure_page()

        # 1. ¿Ya estamos en el thread correcto?
        if "/direct/t/" in (page.url or "") and thread.id in page.url:
            if self._wait_thread_open(page, timeout_ms=3000):
                self._current_thread_id = thread.id
                return True

        # 2. Asegurar que estamos en vista Inbox (sin recargar si es posible)
        if INBOX_URL not in (page.url or "") or re.search(r"/direct/t/", page.url):
            self.return_to_inbox()

        # 3. Intentar clickear por índice usando el selector cacheado del scan
        # source_index path
        if thread.source_index != -1:
            meta = self._thread_cache_meta.get(thread.id, {})
            row_selector = meta.get("selector") or THREAD_ROW_SELECTOR
            try:
                rows = page.locator(row_selector)
                row_count = rows.count()
                if row_count > thread.source_index:
                    row = rows.nth(thread.source_index)
                    row_preview = self._row_preview(row)
                    if not self._row_is_valid(row, selector=row_selector):
                        logger.error(
                            "PlaywrightDM open_thread_invalid_row account=@%s thread_id=%s selector=%s idx=%s row=%s",
                            self.username,
                            thread.id,
                            row_selector,
                            thread.source_index,
                            row_preview,
                        )
                    else:
                        pre_url = page.url or ""
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
                        ):
                            return True
                        self.return_to_inbox()
                else:
                    logger.error(
                        "PlaywrightDM open_thread_missing_row account=@%s thread_id=%s selector=%s idx=%s row_count=%s",
                        self.username,
                        thread.id,
                        row_selector,
                        thread.source_index,
                        row_count,
                    )
            except Exception as exc:
                logger.error(
                    "PlaywrightDM open_thread_click_error account=@%s thread_id=%s selector=%s idx=%s error=%s",
                    self.username,
                    thread.id,
                    row_selector,
                    thread.source_index,
                    exc,
                )
        if self._open_thread_by_cache(thread):
            return True

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
                anchor = row.locator("a[href*='/direct/t/']").first
                if anchor.count() > 0:
                    click_target = anchor
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
            if box:
                width = float(box.get("width") or 0.0)
                height = float(box.get("height") or 0.0)
                if width > 0 and height > 0:
                    safe_x = max(8.0, min(width - 8.0, width * 0.82))
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

    def _validate_open_state(
        self,
        thread: ThreadLike,
        *,
        pre_url: str,
        selector: str,
        idx: int,
        row_preview: str,
        click_href: str,
    ) -> bool:
        page = self._ensure_page()
        pre_thread_id = _extract_thread_id(pre_url)
        deadline = time.time() + 8.0
        post_url = page.url or ""
        post_thread_id = _extract_thread_id(post_url)
        composer_visible = False
        url_is_thread = False
        thread_id_changed = False
        message_panel_visible = False

        while time.time() < deadline:
            post_url = page.url or ""
            post_thread_id = _extract_thread_id(post_url)
            url_is_thread = bool(post_thread_id)
            thread_id_changed = bool(post_thread_id) and post_thread_id != pre_thread_id

            composer = self._find_composer(page)
            composer_visible = False
            if composer is not None:
                try:
                    composer_visible = bool(composer.is_visible())
                except Exception:
                    composer_visible = False

            try:
                message_panel_visible = page.locator(_MESSAGE_NODE_SELECTORS[0]).count() > 0
            except Exception:
                message_panel_visible = False

            if composer_visible and (url_is_thread or message_panel_visible):
                break
            try:
                page.wait_for_timeout(200)
            except Exception:
                time.sleep(0.2)

        if not (composer_visible and (url_is_thread or message_panel_visible)):
            logger.error(
                "PlaywrightDM open_thread_validation_error account=@%s target_thread=%s selector=%s idx=%s row=%s pre_url=%s post_url=%s click_href=%s url_is_thread=%s composer_visible=%s thread_id_changed=%s message_panel_visible=%s",
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
            return False

        print(
            style_text(
                f"[TRACE_ID SYNC BEFORE] pre_url={pre_url} post_url={post_url} post_thread_id={post_thread_id} id={thread.id} pk={thread.pk} flags=url_is_thread:{url_is_thread},composer_visible:{composer_visible},message_panel_visible:{message_panel_visible},thread_id_changed:{thread_id_changed}",
                color=Fore.WHITE,
            )
        )
        self._sync_thread_id(thread, post_thread_id)
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
        return True

    def _sync_thread_id(self, thread: ThreadLike, real_id: str) -> None:
        if not real_id:
            return
        if real_id == thread.id:
            return
        old_id = thread.id
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

    # LEGACY: Función deshabilitada - ya no se usa (usaba _THREAD_ANCHOR_SELECTORS eliminadas)
    # def _select_thread_anchor(...)

    def _resolve_thread_key(
        self,
        page: Page,
        row,
        *,
        title: str,
        peer_username: str,
        snippet: str,
    ) -> tuple[str, str, str]:
        url = page.url or ""
        if "/direct/t/" in url:
            thread_id = _extract_thread_id(url)
            if thread_id:
                return thread_id, "real_url", _normalize_direct_link(url)
        href = ""
        try:
            href = row.locator("a[href*='/direct/t/']").first.get_attribute("href") or ""
        except Exception:
            href = ""
        thread_id = _extract_thread_id(href)
        if thread_id:
            return thread_id, "row_href", _normalize_direct_link(href)
        try:
            header_href = page.locator("header a[href*='/direct/t/']").first.get_attribute("href") or ""
        except Exception:
            header_href = ""
        thread_id = _extract_thread_id(header_href)
        if thread_id:
            return thread_id, "header_href", _normalize_direct_link(header_href)
        try:
            any_href = page.locator("a[href*='/direct/t/']").first.get_attribute("href") or ""
        except Exception:
            any_href = ""
        thread_id = _extract_thread_id(any_href)
        if thread_id:
            return thread_id, "dom_fallback", _normalize_direct_link(any_href)

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
        Probe de diagnóstico para saber exactamente donde estamos y qué vemos.
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
        Espera a que el composer esté visible y confirma que estamos en un thread.
        """
        # 1. Esperar a que la URL cambie al patrón de thread
        try:
            page.wait_for_url(re.compile(r".*/direct/t/.*"), timeout=timeout_ms // 2)
        except Exception:
            pass

        found_composer = False
        # Preferir role=textbox si est? visible
        try:
            if page.get_by_role("textbox").first.is_visible():
                found_composer = True
        except Exception:
            pass
        if not found_composer:
            for selector in _COMPOSER_SELECTORS:
                try:
                    # Intentar esperar al selector
                    page.wait_for_selector(selector, timeout=timeout_ms // len(_COMPOSER_SELECTORS))
                    found_composer = True
                    break
                except Exception:
                    continue

        if found_composer:
            composer = self._find_composer(page)
            if composer is None:
                found_composer = False
            else:
                try:
                    found_composer = bool(composer.is_visible())
                except Exception:
                    found_composer = False

        # Re-obtener URL después de la espera
        current_url = page.url or ""
        is_in_thread = bool(re.search(r"/direct/t/([^/]+)", current_url))

        # Probes de estado EXACTOS requeridos por el usuario
        if _DM_VERBOSE_PROBES:
            print(style_text(f"[Probe] URL = {current_url}", color=Fore.WHITE))
            print(style_text(f"[Probe] thread_abierto = {is_in_thread and found_composer}", color=Fore.WHITE))
            print(style_text(f"[Probe] existe_composer = {found_composer}", color=Fore.WHITE))

        return is_in_thread and found_composer

    def _open_thread_by_cache(self, thread: ThreadLike) -> bool:
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
        candidates = [title, peer]
        candidates = [c for c in candidates if c]
        if not candidates:
            logger.error(
                "PlaywrightDM open_thread_cache_candidates_empty account=@%s thread_id=%s",
                self.username,
                thread.id,
            )
            return False

        if INBOX_URL not in (page.url or "") or re.search(r"/direct/t/", page.url):
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
                if not self._row_is_valid(row, selector=selected_selector):
                    continue
                try:
                    text_value = (row.inner_text() or "").lower()
                except Exception:
                    continue
                scanned += 1
                if not any(c.lower() in text_value for c in candidates):
                    continue

                matched_in_pass = True
                row_preview = self._row_preview(row)
                pre_url = page.url or ""
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
                ):
                    return True
                self.return_to_inbox()

            moved = self._scroll_panel_down(inbox_panel)
            if not moved:
                break
            if matched_in_pass:
                stagnant_passes = 0
            else:
                stagnant_passes += 1
            if stagnant_passes >= 8:
                break
            try:
                page.wait_for_timeout(120)
            except Exception:
                pass

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
                if loc.count():
                    return loc.first
            except Exception:
                continue
        return None

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

    def _verify_sent(self, thread: ThreadLike, text: str) -> Optional[str]:
        deadline = time.time() + VERIFY_TIMEOUT_S
        target_text = _normalize_message_text(text)
        while time.time() < deadline:
            try:
                messages = self.get_messages(thread, amount=20, log=False)
                for msg in messages:
                    if msg.user_id == self.user_id and _normalize_message_text(msg.text) == target_text:
                        return msg.id
            except Exception:
                pass
            time.sleep(0.5)
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
        log_dir = Path("storage") / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        base = log_dir / f"{DM_DEBUG_DIRNAME}_{self.username}_{timestamp}"
        try:
            logger.info("PlaywrightDM debug_dump reason=%s url=%s", reason, page.url)
        except Exception:
            pass
        # Selectores mínimos para debugging
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
            Path(str(base) + ".html").write_text(html, encoding="utf-8", errors="ignore")
        except Exception:
            pass
        try:
            main_text = page.locator("div[role='main']").inner_text()
            Path(str(base) + ".txt").write_text(main_text, encoding="utf-8", errors="ignore")
        except Exception:
            pass
        return str(base)


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

    # Búsqueda de badge por aria-label
    try:
        badge = node.locator("span[aria-label*='unread'], span[aria-label*='sin leer'], span[aria-label*='no leido']")
        if badge.count():
            return 1
    except Exception:
        pass

    # Búsqueda por "punto azul" (visual) - Instagram suele usar un div/span con fondo azul
    # El color rgb(0, 149, 246) es el azul característico de Instagram
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


def _extract_message_timestamp(node) -> Optional[float]:
    time_value = ""
    try:
        time_node = node.locator("time").first
        if time_node.count():
            time_value = (time_node.get_attribute("datetime") or time_node.get_attribute("title") or "").strip()
    except Exception:
        time_value = ""
    if not time_value:
        return None
    return _parse_iso_ts(time_value)


def _parse_iso_ts(value: str) -> Optional[float]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


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
    return re.sub(r"\\s+", " ", (text or "").strip())


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
