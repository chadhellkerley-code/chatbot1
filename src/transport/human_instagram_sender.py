from __future__ import annotations

import asyncio
import os
import logging
import random
import re
import time
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from playwright.async_api import Locator, Page, TimeoutError as PwTimeoutError

from src.actions.direct_helpers import (
    DmAvailability,
    detect_dm_availability,
    find_profile_dm_button,
)
from src.auth.persistent_login import ensure_logged_in_async
from src.humanizer import random_wait
from src.playwright_service import BASE_PROFILES, PlaywrightService
from src.proxy_payload import normalize_playwright_proxy, proxy_from_account

logger = logging.getLogger(__name__)

INSTAGRAM = "https://www.instagram.com/"
DIRECT_INBOX = f"{INSTAGRAM}direct/inbox/"
DIRECT_NEW = f"{INSTAGRAM}direct/new/"

DIALOG_SELECTOR = "div[role='dialog']"
SEARCH_INPUTS = (
    "div[role='dialog'] input[placeholder*='Search']",
    "div[role='dialog'] input[placeholder*='Buscar']",
    "div[role='dialog'] input[name='queryBox']",
    "input[placeholder='Search...']",
    "input[placeholder='Search']"
)
NEW_MESSAGE_BUTTONS = (
    "div[role='button']:has-text('Send message')",
    "div[role='button']:has-text('Enviar mensaje')",
    "a[href='/direct/new/']",
    "[aria-label='New message']",
    "[aria-label='Nuevo mensaje']",
    "button:has-text('Enviar mensaje')"
)
NEXT_BTNS = (
    "div[role='dialog'] button:has-text('Next')",
    "div[role='dialog'] button:has-text('Siguiente')",
    "button:has-text('Next')",
    "button:has-text('Siguiente')",
    "button:has-text('Chat')",
    "div[role='button']:has-text('Chat')",
    "div[role='button']:has-text('Next')",
    "div[role='button']:has-text('Siguiente')",
)
COMPOSERS = (
    "div[role='textbox'][aria-label='Mensaje']",
    "div[role='textbox'][aria-label='Message']",
    "div[contenteditable='true']",
    "div[role='textbox']",
    "textarea[placeholder*='message']",
    "textarea[placeholder*='Mensaje']",
    "textarea[aria-label*='Message']",
    "textarea[data-testid='message-input']",
    "div[data-testid='message-input']",
    "textarea"
)
SEND_BUTTONS = (
    "div[role='button']:has-text('Send'), "
    "div[role='button']:has-text('Enviar'), "
    "button:has-text('Send'), "
    "button:has-text('Enviar'), "
    "button[aria-label*='Send'], "
    "button[aria-label*='Enviar'], "
    "div[role='button'][aria-label*='Send'], "
    "div[role='button'][aria-label*='Enviar'], "
    "[data-testid='send'], "
    "[data-testid*='send'], "
    "button[type='submit'], "
    "form button[type='submit']"
)
VERIFY_TIMEOUT_S = float(os.getenv("HUMAN_DM_VERIFY_TIMEOUT", "10.0"))
ALLOW_UNVERIFIED = os.getenv("HUMAN_DM_ALLOW_UNVERIFIED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
)
SKIPPED_NO_DM_REASON = "SKIPPED_NO_DM"
LEGACY_NO_DM_REASON = "NO_DM_BUTTON"
NO_DM_SKIP_REASON = SKIPPED_NO_DM_REASON
NO_DM_SKIP_DETAIL = "Perfil sin botón de mensaje / no permite DM"
NO_DM_SKIP_LOG = f"skip | no_dm | {NO_DM_SKIP_DETAIL}"
NO_DM_SEND_METHOD = "skip_no_dm"
NO_DM_PERMISSION_DETECTED_LOG = "NO_DM_PERMISSION_DETECTED"
_UNVERIFIED_REASONS = {"message_not_present_after_send", "composer_not_cleared"}
_TOAST_FAILURE_RE = re.compile(
    r"("
    r"not sent|couldn'?t send|failed to send|"
    r"no se pudo enviar|mensaje no enviado|"
    r"something went wrong|ha ocurrido un error|ocurri[oó] un error|"
    r"try again later|please wait a few minutes|"
    r"prueba(?:lo)? m[aá]s tarde|int[eé]ntalo de nuevo m[aá]s tarde|"
    r"we restrict certain activity"
    r")",
    re.IGNORECASE,
)
_UNSENT_INDICATOR_SELECTORS = (
    "[aria-label*='Not sent']",
    "[aria-label*=\"Couldn't send\"]",
    "[aria-label*='Couldn’t send']",
    "[aria-label*='No se pudo enviar']",
    "[aria-label*='No enviado']",
    "[aria-label*='No enviado.']",
    "[aria-label*='Error sending']",
    "button:has-text('Retry')",
    "button:has-text('Try again')",
    "button:has-text('Reintentar')",
    "button:has-text('Intentar de nuevo')",
)

_DEBUG_ENV = "HUMAN_DM_DEBUG"
_DEBUG_SCREENSHOT_DIR = Path("storage") / "debug_screenshots"
_DM_CTX: ContextVar[dict[str, Any]] = ContextVar("human_dm_ctx", default={})


def _debug_enabled() -> bool:
    return os.getenv(_DEBUG_ENV, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_slug(value: str, fallback: str = "x") -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return cleaned or fallback


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


def _debug_artifact_path(stage: str, ext: str, *, tag: Optional[str] = None) -> Path:
    ctx = _dm_ctx()
    debug_id = str(ctx.get("debug_id") or int(time.time() * 1000))
    account = _safe_slug(str(ctx.get("account") or "account"), "account")
    lead = _safe_slug(str(ctx.get("lead") or "lead"), "lead")
    stage_slug = _safe_slug(stage, "stage")
    tag_slug = _safe_slug(tag, "") if tag else ""
    suffix = f"_{tag_slug}" if tag_slug else ""
    ext_clean = ext.lstrip(".") or "txt"
    filename = f"{debug_id}_{account}_{lead}_{stage_slug}{suffix}.{ext_clean}"
    return _DEBUG_SCREENSHOT_DIR / filename


async def _debug_screenshot(page: Optional[Page], stage: str, *, tag: Optional[str] = None) -> Optional[str]:
    if not _debug_enabled() or page is None:
        return None
    try:
        _DEBUG_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        path = _debug_artifact_path(stage, "png", tag=tag)
        await page.screenshot(path=str(path), full_page=True)
        return str(path)
    except Exception as exc:
        _dm_log("DEBUG_SCREENSHOT_FAIL", stage=stage, error=repr(exc))
        return None


async def _debug_dump_outer_html(locator: Optional[Locator], stage: str, *, tag: str) -> Optional[str]:
    if not _debug_enabled() or locator is None:
        return None
    try:
        _DEBUG_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        # Best-effort: only dump if it exists.
        try:
            if await locator.count() <= 0:
                return None
        except Exception:
            return None
        html = await locator.first.evaluate("el => el ? el.outerHTML : ''")
        if not html:
            return None
        # Keep dumps bounded.
        html = str(html)
        if len(html) > 220_000:
            html = html[:220_000] + "\n<!-- truncated -->\n"
        path = _debug_artifact_path(stage, "html", tag=tag)
        path.write_text(html, encoding="utf-8", errors="ignore")
        return str(path)
    except Exception as exc:
        _dm_log("DEBUG_HTML_DUMP_FAIL", stage=stage, tag=tag, error=repr(exc))
        return None


async def _probe_locator(locator: Optional[Locator]) -> dict[str, Any]:
    info: dict[str, Any] = {"count": 0, "visible": False, "enabled": False}
    if locator is None:
        return info
    try:
        info["count"] = await locator.count()
    except Exception as exc:
        info["count"] = None
        info["count_error"] = repr(exc)
        return info
    if not info.get("count"):
        return info
    first = locator.first
    try:
        info["visible"] = await first.is_visible()
    except Exception as exc:
        info["visible"] = None
        info["visible_error"] = repr(exc)
    try:
        info["enabled"] = await first.is_enabled()
    except Exception as exc:
        info["enabled"] = None
        info["enabled_error"] = repr(exc)
    return info



def _compact_text(value: str, limit: int = 180) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


class HumanInstagramSender:
    def __init__(self, headless: bool = True) -> None:
        self.headless = headless

    async def _sleep(self, low: float, high: float) -> None:
        await asyncio.sleep(random.uniform(low, high))

    def _normalize_username(self, username: str) -> str:
        return username.strip().lstrip("@").split("?", 1)[0]

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
    ) -> Union[
        bool,
        Tuple[bool, Optional[str]],
        Tuple[bool, Optional[str], Dict[str, Any]],
    ]:
        coro = self.send_message_like_human(
            account,
            target_username,
            text,
            base_delay_seconds=base_delay_seconds,
            jitter_seconds=jitter_seconds,
            proxy=proxy,
            return_detail=return_detail,
            return_payload=return_payload,
        )
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        raise RuntimeError("send_message_like_human_sync requiere contexto sync.")


    async def _goto_inbox(self, page: Page) -> None:
        await page.goto(DIRECT_INBOX, wait_until="domcontentloaded", timeout=45_000)
        try:
            await page.wait_for_selector("nav[role='navigation'], [role='dialog'], a[href='/direct/new/']", timeout=15_000)
        except Exception:
            pass

    async def _dialog_ready(self, page: Page) -> bool:
        return await page.locator(", ".join(SEARCH_INPUTS)).count() > 0

    async def _open_new_message_dialog(self, page: Page) -> bool:
        _dm_log("DIRECT_NEW_NAV_START", target=DIRECT_NEW, url=page.url if page else "")
        t0 = time.time()
        try:
            await page.goto(DIRECT_NEW, wait_until="domcontentloaded", timeout=45_000)
        except PwTimeoutError:
            pass
        _dm_log("DIRECT_NEW_NAV_DONE", url=page.url if page else "", elapsed_ms=int((time.time() - t0) * 1000))
        ready = await self._dialog_ready(page)
        _dm_log("DIRECT_NEW_DIALOG_READY", ready=ready, url=page.url if page else "")
        if ready:
            return True

        await self._goto_inbox(page)
        _dm_log("DIRECT_INBOX_READY", url=page.url if page else "")
        buttons = page.locator(", ".join(NEW_MESSAGE_BUTTONS))
        try:
            btn_count = await buttons.count()
        except Exception:
            btn_count = 0
        if _debug_enabled():
            probe_btn = await _probe_locator(buttons)
            _dm_log("DIRECT_NEW_BUTTONS", url=page.url if page else "", **probe_btn)
        if btn_count > 0:
            target = buttons.first
            try:
                await target.scroll_into_view_if_needed(timeout=2_000)
            except Exception:
                pass
            await target.click()
            _dm_log("DIRECT_NEW_BUTTON_CLICKED", url=page.url if page else "")
            try:
                await page.wait_for_selector(f"{DIALOG_SELECTOR}, {SEARCH_INPUTS}", timeout=15_000)
            except Exception:
                pass
        ready = await self._dialog_ready(page)
        _dm_log("DIRECT_NEW_DIALOG_READY", ready=ready, url=page.url if page else "")
        return ready

    async def _read_search_value(self, field: Locator) -> str:
        try:
            return await field.input_value() or ""
        except Exception:
            try:
                return await field.evaluate("el => el.value || el.textContent || ''") or ""
            except Exception:
                return ""

    async def _type_search_handle(self, field: Locator, handle: str) -> None:
        try:
            await field.click()
        except Exception:
            pass
        try:
            await field.fill("")
        except Exception:
            try:
                await field.press("Control+A")
                await field.press("Delete")
            except Exception:
                pass
        try:
            await field.type(handle, delay=random.randint(55, 85))
        except Exception:
            try:
                await field.fill(handle)
            except Exception:
                pass
        current = (await self._read_search_value(field)).strip().lower()
        if handle and handle not in current:
            try:
                await field.fill(handle)
            except Exception:
                pass

    async def _search_and_select(self, page: Page, handle: str) -> bool:
        cleaned = handle.strip().lstrip("@")
        if not cleaned:
            return False
        search_locator = page.locator(", ".join(SEARCH_INPUTS))
        try:
            search_count = await search_locator.count()
        except Exception:
            search_count = 0
        if _debug_enabled():
            probe_search = await _probe_locator(search_locator)
            _dm_log("DIRECT_NEW_SEARCH_INPUTS", url=page.url if page else "", **probe_search)
        if search_count == 0:
            _dm_log("DIRECT_NEW_SEARCH_INPUT_MISSING", url=page.url if page else "")
            return False
        field = search_locator.first
        normalized = cleaned.lower()
        _dm_log("DIRECT_NEW_SEARCH_TYPE", handle=normalized, url=page.url if page else "")
        await self._type_search_handle(field, normalized)
        typed_value = (await self._read_search_value(field)).strip().lower()
        if normalized not in typed_value:
            logger.info("Busqueda incompleta ('%s' -> '%s'), reintentando.", normalized, typed_value)
            await self._type_search_handle(field, normalized)
        _dm_log("DIRECT_NEW_SEARCH_TYPED", handle=normalized, typed_value=typed_value, url=page.url if page else "")
        try:
            # Aumentamos espera para que IG cargue resultados 
            await page.wait_for_timeout(4000)
        except Exception:
            pass
        _dm_log("DIRECT_NEW_SEARCH_WAITED", ms=4000, url=page.url if page else "")

        # Estrategia 1: Buscar por selectores estándar (globales)
        buttons = page.locator("[role='button']")
        candidate = buttons.filter(has_text=re.compile(rf"^{re.escape(normalized)}$", re.IGNORECASE))
        
        selection: Optional[Locator] = None
        strategy_used: str | None = None
        if await candidate.count() > 0:
            selection = candidate.first
            strategy_used = "role_button_exact_text"
        else:
            # Estrategia 2: Buscar contenedor directo que tenga el texto
            direct_text = page.locator(f"div[role='dialog'] div").filter(has_text=normalized)
            if await direct_text.count() > 0:
                selection = direct_text.last # Usamos last porque suele ser el nodo texto mas profundo
                strategy_used = "dialog_div_has_text"
             
        if selection is None:
              # Estrategia 4: Buscar inputs de tipo radio/checkbox GLOBALMENTE
            radio_inputs = page.locator("input[type='radio'], input[type='checkbox']")
            if await radio_inputs.count() > 0:
                  logger.info("Encontrados inputs de selección (radio/checkbox). Clickeando el primero.")
                  selection = radio_inputs.first
                  strategy_used = "radio_or_checkbox_first"
                  # A veces hay que clickear el padre o label
                  try:
                      await selection.click(force=True)
                      _dm_log("DIRECT_NEW_SELECT", ok=True, strategy=strategy_used, url=page.url if page else "")
                      return True
                  except: 
                      pass

              # Estrategia 5: Fallback a lista genérica GLOBAL
            items = page.locator("[role='button'], li")
            visible_count = await items.count()
            _dm_log("DIRECT_NEW_RESULTS", count=visible_count, url=page.url if page else "")
            limit = min(visible_count, 5)
            for idx in range(limit):
                try:
                    text_value = (await items.nth(idx).inner_text() or "").strip().lower()
                except Exception:
                    continue
                if normalized in text_value:
                    selection = items.nth(idx)
                    strategy_used = "first_item_contains_handle"
                    break
            if selection is None and visible_count > 0:
                selection = items.first
                strategy_used = "items_first_fallback"
        
        if selection is None:
            # Ultimo intento desesperado: Buscar el texto literal y clickearlo
            logger.info(f"Búsqueda estándar falló. Intentando clic por texto: {normalized}")
            try:
                # Buscamos en todo el dialogo
                fallback = page.locator("div[role='dialog']").get_by_text(normalized, exact=False).first
                await fallback.click()
                _dm_log("DIRECT_NEW_SELECT", ok=True, strategy="dialog_get_by_text", url=page.url if page else "")
                return True
            except:
                _dm_log("DIRECT_NEW_SELECT", ok=False, strategy="dialog_get_by_text", url=page.url if page else "")
                return False

        try:
            await selection.scroll_into_view_if_needed(timeout=2_000)
        except Exception:
            pass
        try:
            await selection.click()
            if _debug_enabled():
                probe_sel = await _probe_locator(selection)
                _dm_log("DIRECT_NEW_SELECT", ok=True, strategy=strategy_used, url=page.url if page else "", **probe_sel)
            else:
                _dm_log("DIRECT_NEW_SELECT", ok=True, strategy=strategy_used, url=page.url if page else "")
        except Exception as exc:
            _dm_log("DIRECT_NEW_SELECT", ok=False, strategy=strategy_used, url=page.url if page else "", error=repr(exc))
            return False
        return True

    async def _confirm_next(self, page: Page) -> bool:
        _dm_log("DIRECT_NEW_CONFIRM_START", url=page.url if page else "")
        # 1. Verificar si ya estamos en el chat (si al seleccionar usuario nos llevó directo)
        try:
            if await self._composer(page):
                logger.info("Composer ya visible inmediatamente. Asumiendo éxito.")
                _dm_log("DIRECT_NEW_CONFIRM", ok=True, method="composer_already_visible", url=page.url if page else "")
                return True
        except: pass

        # 2. Buscar botones de confirmación
        potential_texts = ["Next", "Siguiente", "Chat", "Conversar"]
        
        # Intentar clic en botones estandar
        try:
            nxt = page.locator(", ".join(NEXT_BTNS))
            if await nxt.count() > 0:
                # Filtrar visibilidad
                for i in range(await nxt.count()):
                    if await nxt.nth(i).is_visible():
                        await nxt.nth(i).click()
                        await self._sleep(1, 2)
                        _dm_log("DIRECT_NEW_CONFIRM", ok=True, method="next_btns_css", index=i, url=page.url if page else "")
                        return True
        except Exception as e:
            logger.debug(f"Fallo click selector estandar: {e}")
            _dm_log("DIRECT_NEW_CONFIRM", ok=False, method="next_btns_css", url=page.url if page else "", error=repr(e))

        # 3. Fuerza bruta: Buscar texto 'Chat'/'Next' arriba a la derecha (común en web)
        logger.info("Buscando botón Next/Chat por texto...")
        for text in potential_texts:
            try:
                # Buscamos botones o divs con ese texto exacto
                el = page.get_by_role("button", name=text).first
                if await el.is_visible():
                    await el.click()
                    await self._sleep(1, 2)
                    _dm_log("DIRECT_NEW_CONFIRM", ok=True, method="get_by_role_button", name=text, url=page.url if page else "")
                    return True
                
                # O simplemente texto clickeable
                el_text = page.get_by_text(text, exact=True).first
                if await el_text.is_visible():
                    await el_text.click()
                    await self._sleep(1, 2)
                    _dm_log("DIRECT_NEW_CONFIRM", ok=True, method="get_by_text_exact", name=text, url=page.url if page else "")
                    return True
            except:
                continue

        # 4. Esperar un poco y verificar composer de nuevo
        try:
            await page.wait_for_selector(", ".join(COMPOSERS), timeout=5000)
            _dm_log("DIRECT_NEW_CONFIRM", ok=True, method="wait_for_composer", url=page.url if page else "")
            return True
        except:
            pass

        _dm_log("DIRECT_NEW_CONFIRM", ok=False, method="all_failed", url=page.url if page else "")
        return False

    async def _composer(self, page: Page) -> Optional[Locator]:
        locator = page.locator(", ".join(COMPOSERS))
        try:
            count = await locator.count()
        except Exception:
            count = 0
        _dm_log("COMPOSER_SCAN", url=page.url if page else "", count=count)
        if count <= 0:
            return None
        for idx in range(count):
            candidate = locator.nth(idx)
            try:
                if not await candidate.is_visible():
                    continue
            except Exception:
                continue
            if _debug_enabled():
                matched_selector: Optional[str] = None
                for sel in COMPOSERS:
                    try:
                        if await candidate.evaluate("(el, s) => !!el && el.matches(s)", sel):
                            matched_selector = sel
                            break
                    except Exception:
                        continue
                probe = await _probe_locator(candidate)
                _dm_log(
                    "COMPOSER_FOUND",
                    url=page.url if page else "",
                    index=idx,
                    selector=matched_selector,
                    **probe,
                )
            return candidate
        return None

    async def _focus_composer(self, page: Page, composer: Locator) -> Locator:
        # IG re-renderiza el contenteditable al hacer focus; por eso re-resolvemos
        # el locator y verificamos que el foco realmente quedó dentro.
        last = composer
        for _ in range(4):
            try:
                await last.scroll_into_view_if_needed(timeout=2_000)
            except Exception:
                pass
            try:
                await last.click(timeout=5_000)
            except Exception:
                pass
            try:
                await page.wait_for_timeout(random.randint(80, 180))
            except Exception:
                pass
            refreshed = await self._composer(page)
            if refreshed is not None:
                last = refreshed
            try:
                focused = await last.evaluate(
                    "el => !!el && !!document.activeElement && (el === document.activeElement || el.contains(document.activeElement))"
                )
            except Exception:
                focused = False
            if focused:
                return last
        return last

    async def _clear_composer(self, page: Page, composer: Locator) -> None:
        composer = await self._focus_composer(page, composer)
        # Preferimos limpiar con teclado: es más consistente en contenteditable.
        try:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Delete")
            await page.wait_for_timeout(random.randint(60, 140))
            return
        except Exception:
            pass
        try:
            await composer.fill("")
            return
        except Exception:
            pass
        try:
            await composer.evaluate(
                "(el) => {"
                "  if (!el) return;"
                "  if (typeof el.value === 'string') el.value = '';"
                "  if (el.isContentEditable) el.textContent = '';"
                "}"
            )
        except Exception:
            return

    async def _type_text(self, page: Page, composer: Locator, text: str) -> None:
        payload = (text or "").replace("\r\n", "\n")
        if not payload.strip():
            raise ValueError("El mensaje está vacío.")

        # Verificación ligera: si no aparece el snippet en el composer, reintenta.
        snippet = self._message_snippet(payload)
        prefix_len = min(18, len(snippet))
        prefix = snippet[:prefix_len].lower()
        _dm_log(
            "TYPE_START",
            url=page.url if page else "",
            chars=len(payload),
            lines=payload.count("\n") + 1,
        )

        async def _typed_ok() -> bool:
            try:
                current = await self._composer(page)
                if current is not None:
                    current_text = await self._composer_text(current)
                else:
                    current_text = await self._composer_text(composer)
            except Exception:
                return False
            lowered = (current_text or "").strip().lower()
            if not snippet:
                return True
            return (snippet.lower() in lowered) or (prefix and prefix in lowered)

        for attempt in range(3):
            _dm_log("TYPE_ATTEMPT", attempt=attempt + 1, url=page.url if page else "")
            current = await self._composer(page)
            if current is not None:
                composer = current

            composer = await self._focus_composer(page, composer)
            await self._clear_composer(page, composer)
            composer = await self._focus_composer(page, composer)

            parts = payload.split("\n")
            for idx, part in enumerate(parts):
                if idx > 0:
                    try:
                        await page.keyboard.press("Shift+Enter")
                    except Exception:
                        try:
                            await composer.press("Shift+Enter")
                        except Exception:
                            pass
                    await self._sleep(0.08, 0.25)
                if not part:
                    continue
                for ch in part:
                    try:
                        await page.keyboard.type(ch, delay=random.randint(30, 120))
                    except Exception:
                        # Si el foco se perdió por re-render, lo recuperamos y seguimos.
                        composer = await self._focus_composer(page, composer)
                        await page.keyboard.type(ch, delay=random.randint(30, 120))
                await self._sleep(0.05, 0.2)

            if await _typed_ok():
                _dm_log("TYPE_OK", attempt=attempt + 1, url=page.url if page else "")
                return
            # Si no se reflejó lo tipeado, reintento controlado.
            try:
                await page.wait_for_timeout(200 + attempt * 150)
            except Exception:
                pass

        raise RuntimeError("No se pudo tipear el mensaje (composer inestable / re-render continuo).")

    async def _type_and_send(self, page: Page, text: str) -> str:
        _dm_log(
            "TYPE_AND_SEND_START",
            url=page.url if page else "",
            snippet=self._message_snippet(text),
        )
        try:
            await page.wait_for_selector(", ".join(COMPOSERS), timeout=20_000)
        except Exception:
            pass
        composer = await self._composer(page)
        if composer is None:
            snap = await _debug_screenshot(page, "COMPOSER_NOT_FOUND", tag="type_and_send")
            main_html = await _debug_dump_outer_html(page.locator("main"), "COMPOSER_NOT_FOUND", tag="main")
            _dm_log("COMPOSER_NOT_FOUND", url=page.url if page else "", screenshot=snap, main_html=main_html)
            raise RuntimeError("Composer no encontrado.")
        await self._type_text(page, composer, text)
        _dm_log("TYPE_AND_SEND_TYPED", url=page.url if page else "")
        await self._sleep(0.25, 0.9)
        send_candidates = [
            SEND_BUTTONS,
            "button[aria-label*='Send']",
            "button[aria-label*='Enviar']",
            "div[role='button'][aria-label*='Send']",
            "div[role='button'][aria-label*='Enviar']",
        ]
        clicked = False
        clicked_sel: Optional[str] = None
        clicked_idx: Optional[int] = None
        for sel in send_candidates:
            btn = page.locator(sel)
            try:
                count = await btn.count()
            except Exception:
                count = 0
            if count <= 0:
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
                    clicked = True
                    clicked_sel = sel
                    clicked_idx = idx
                    break
                except Exception:
                    continue
            if clicked:
                break
        if not clicked:
            _dm_log("SEND_FALLBACK", method="enter", url=page.url if page else "")
            await composer.press("Enter")
            await self._sleep(0.25, 0.6)
            try:
                composer_text = await self._composer_text(composer)
            except Exception:
                composer_text = ""
            if composer_text:
                try:
                    await composer.press("Control+Enter")
                except Exception:
                    pass
                await self._sleep(0.25, 0.6)
                for sel in send_candidates:
                    btn = page.locator(sel)
                    try:
                        count = await btn.count()
                    except Exception:
                        count = 0
                    if count <= 0:
                        continue
                    try:
                        await btn.first.click()
                        break
                    except Exception:
                        continue
                _dm_log("SEND_METHOD", method="enter_ctrl_fallback", url=page.url if page else "")
                return "enter_ctrl_fallback"
            _dm_log("SEND_METHOD", method="enter_fallback", url=page.url if page else "")
            return "enter_fallback"
        _dm_log("SEND_CLICK", ok=True, selector=clicked_sel, index=clicked_idx, url=page.url if page else "")
        await self._sleep(0.3, 1.0)
        _dm_log("SEND_METHOD", method="click", url=page.url if page else "")
        return "click"

    async def _composer_text(self, composer: Locator) -> str:
        try:
            value = await composer.input_value()
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        try:
            text = await composer.inner_text()
            if isinstance(text, str):
                return text.strip()
        except Exception:
            pass
        try:
            text = await composer.text_content()
            if isinstance(text, str):
                return text.strip()
        except Exception:
            pass
        return ""

    def _message_snippet(self, text: str, limit: int = 48) -> str:
        for line in (text or "").splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:limit]
        return (text or "").strip()[:limit]

    async def _confirm_message_sent(
        self,
        page: Page,
        text: str,
        composer: Optional[Locator] = None,
    ) -> tuple[bool, str]:
        snippet = self._message_snippet(text)
        if not snippet:
            return False, "snippet_empty"

        current_url = page.url or ""
        if "accounts/login" in current_url:
            return False, "login_lost"
        if any(token in current_url for token in ("challenge", "checkpoint", "accounts/confirm_email", "two_factor")):
            return False, "challenge_detected"

        toast_selectors = [
            "[role='alert']",
            "[data-testid='toast']",
            "div[role='status']",
            "[aria-live='assertive']",
            "[aria-live='polite']",
        ]
        toast_success = re.compile(r"(sent|enviado|mensaje enviado|message sent)", re.IGNORECASE)
        toast_negative = _TOAST_FAILURE_RE

        message_selectors = [
            "div[role='list'] div[role='listitem']",
            "div[role='list'] div[role='row']",
            "div[role='log'] div[role='listitem']",
            "div[role='log'] div[role='row']",
            "div[role='main'] div[role='listitem']",
            "div[role='main'] div[role='row']",
            "div[role='row']",
            "[data-testid='message-bubble']",
        ]

        def _contains_snippet(value: str) -> bool:
            return snippet.lower() in (value or "").lower()

        prefix_len = min(18, len(snippet))
        prefix = snippet[:prefix_len].lower()

        def _contains_prefix(value: str) -> bool:
            if not prefix:
                return False
            return prefix in (value or "").lower()

        deadline = time.time() + VERIFY_TIMEOUT_S
        while time.time() < deadline:
            try:
                await page.wait_for_timeout(500)
            except Exception:
                pass
            for selector in toast_selectors:
                try:
                    toasts = page.locator(selector)
                    count = await toasts.count()
                    if count > 0:
                        toast = toasts.nth(max(0, count - 1))
                        toast_text = _compact_text((await toast.inner_text() or ""))
                        if toast_text and toast_negative.search(toast_text):
                            return False, f"send_failed_toast: {toast_text}"
                        if toast_text and toast_success.search(toast_text) and not toast_negative.search(toast_text):
                            return True, "toast_sent"
                except Exception:
                    continue
            if composer is not None:
                try:
                    composer_text = await self._composer_text(composer)
                    if composer_text == "":
                        return True, "composer_cleared"
                    if _contains_snippet(composer_text):
                        continue
                except Exception:
                    pass
            for sel in message_selectors:
                try:
                    items = page.locator(sel)
                    count = await items.count()
                    if count > 0:
                        start = max(0, count - 3)
                        for idx in range(start, count):
                            item = items.nth(idx)
                            try:
                                text_value = await item.inner_text()
                            except Exception:
                                text_value = await item.text_content() or ""
                            if _contains_snippet(text_value) or _contains_prefix(text_value):
                                indicator_text = ""
                                for marker_selector in _UNSENT_INDICATOR_SELECTORS:
                                    try:
                                        markers = item.locator(marker_selector)
                                        marker_count = await markers.count()
                                        if marker_count <= 0:
                                            continue
                                        marker = markers.nth(max(0, marker_count - 1))
                                        for attr in ("aria-label", "title", "data-tooltip-content"):
                                            value = await marker.get_attribute(attr)
                                            if value:
                                                indicator_text = value
                                                break
                                        if not indicator_text:
                                            indicator_text = (await marker.inner_text() or "").strip()
                                        break
                                    except Exception:
                                        continue
                                if indicator_text:
                                    return False, f"send_failed_indicator: {_compact_text(indicator_text)}"
                                return True, "message_present"
                        matches = items.filter(has_text=snippet)
                        if await matches.count() > 0:
                            matched_item = matches.nth(max(0, (await matches.count()) - 1))
                            indicator_text = ""
                            for marker_selector in _UNSENT_INDICATOR_SELECTORS:
                                try:
                                    markers = matched_item.locator(marker_selector)
                                    marker_count = await markers.count()
                                    if marker_count <= 0:
                                        continue
                                    marker = markers.nth(max(0, marker_count - 1))
                                    for attr in ("aria-label", "title", "data-tooltip-content"):
                                        value = await marker.get_attribute(attr)
                                        if value:
                                            indicator_text = value
                                            break
                                    if not indicator_text:
                                        indicator_text = (await marker.inner_text() or "").strip()
                                    break
                                except Exception:
                                    continue
                            if indicator_text:
                                return False, f"send_failed_indicator: {_compact_text(indicator_text)}"
                            return True, "message_present"
                except Exception:
                    continue

        if composer is not None:
            try:
                if _contains_snippet(await self._composer_text(composer)):
                    return False, "composer_not_cleared"
            except Exception:
                pass
        return False, "message_not_present_after_send"

    async def _capture_success(self, page: Optional[Page], username: str, target: str) -> Optional[str]:
        if page is None:
            return None
        try:
            folder = Path(BASE_PROFILES) / username / "dm_success"
            folder.mkdir(parents=True, exist_ok=True)
            safe_target = re.sub(r"[^a-z0-9]+", "_", target.lower()).strip("_") or "target"
            ts = int(time.time())
            screenshot_path = folder / f"{safe_target}_{ts}.png"
            await page.screenshot(path=str(screenshot_path))
            return str(screenshot_path)
        except Exception:
            return None

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
    ) -> Union[
        bool,
        Tuple[bool, Optional[str]],
        Tuple[bool, Optional[str], Dict[str, Any]],
    ]:
        detail: Optional[str] = None
        payload: Dict[str, Any] = {}
        username = account.get("username") or ""
        if not username:
            detail = "Cuenta sin username configurado."
            if return_payload:
                return False, detail, payload
            return (False, detail) if return_detail else False
        logger.info("Engine=playwright_async sender=human account=@%s", username)
        svc: Optional[PlaywrightService] = None
        ctx = None
        page: Optional[Page] = None
        normalized_target = self._normalize_username(target_username)
        if not normalized_target:
            detail = "Lead sin username."
            if return_payload:
                return False, detail, payload
            return (False, detail) if return_detail else False
        debug_token = None
        if _debug_enabled():
            try:
                _DEBUG_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            debug_id = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
            log_path = _DEBUG_SCREENSHOT_DIR / f"{debug_id}_{_safe_slug(username)}_{_safe_slug(normalized_target)}.log"
            debug_token = _DM_CTX.set(
                {
                    "account": username,
                    "lead": normalized_target,
                    "debug_id": debug_id,
                    "log_path": str(log_path),
                }
            )

        delay_total = 0.0
        if base_delay_seconds or jitter_seconds:
            jitter = max(0.0, jitter_seconds)
            delay_total = max(0.0, base_delay_seconds) + random.uniform(0, jitter)
            if delay_total > 0:
                _dm_log("DELAY", seconds=f"{delay_total:.2f}")
                await asyncio.sleep(delay_total)

        strategy = os.getenv("HUMAN_DM_STRATEGY", "auto").strip().lower()
        if strategy not in {"profile", "direct_new", "auto"}:
            strategy = "profile"
        _dm_log("BEGIN", headless=self.headless, strategy=strategy)

        async def _send_via_profile() -> str:
            if page is None:
                raise RuntimeError("Pagina no inicializada.")

            profile_url = f"https://www.instagram.com/{normalized_target}/"
            logger.info("Navegando al perfil directo: %s", profile_url)
            _dm_log("PROFILE_GOTO", url=profile_url)

            try:
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
                await self._sleep(2, 4)
            except Exception as exc:
                logger.warning("Error cargando perfil: %s", exc)
                _dm_log("PROFILE_GOTO_FAIL", url=page.url if page else "", error=repr(exc))

            clicked = False
            click_error: Exception | None = None
            dm_button = await find_profile_dm_button(page)
            if _debug_enabled():
                probe_dm = await _probe_locator(dm_button)
                _dm_log("PROFILE_MESSAGE_BUTTON", url=page.url if page else "", **probe_dm)
            else:
                _dm_log("PROFILE_MESSAGE_BUTTON", url=page.url if page else "", found=bool(dm_button is not None))
            if dm_button is not None:
                try:
                    logger.info("Clickeando boton de mensaje en perfil.")
                    # Avoid false negatives from clicking too early (attached but not visible/enabled yet).
                    try:
                        await dm_button.wait_for(state="visible", timeout=6_000)
                    except Exception:
                        pass
                    await dm_button.click()
                    clicked = True
                    _dm_log("PROFILE_MESSAGE_BUTTON_CLICK", ok=True, url=page.url if page else "")
                except Exception as exc:
                    click_error = exc
                    _dm_log("PROFILE_MESSAGE_BUTTON_CLICK", ok=False, url=page.url if page else "", error=repr(exc))

            if not clicked:
                async def _profile_menu_check_and_click_dm() -> tuple[bool, bool]:
                    """Returns (checked, clicked_dm). checked=True means menu opened and DM option was searched."""
                    if page is None:
                        return False, False
                    header = page.locator("header")
                    menu_btn: Optional[Locator] = None
                    try:
                        if await header.count() <= 0:
                            _dm_log("PROFILE_MENU_BTN_MISSING", reason="no_header", url=page.url if page else "")
                            return False, False
                    except Exception:
                        return False, False

                    candidates = [
                        header.locator(
                            "button[aria-label*='Options'], "
                            "button[aria-label*='More'], "
                            "button[aria-label*='Opciones'], "
                            "button[aria-label*='Más'], "
                            "div[role='button'][aria-label*='Options'], "
                            "div[role='button'][aria-label*='More'], "
                            "div[role='button'][aria-label*='Opciones'], "
                            "div[role='button'][aria-label*='Más'], "
                            "button:has(svg[aria-label*='Options']), "
                            "button:has(svg[aria-label*='More']), "
                            "button:has(svg[aria-label*='Opciones']), "
                            "button:has(svg[aria-label*='Más']), "
                            "div[role='button']:has(svg[aria-label*='Options']), "
                            "div[role='button']:has(svg[aria-label*='More']), "
                            "div[role='button']:has(svg[aria-label*='Opciones']), "
                            "div[role='button']:has(svg[aria-label*='Más'])"
                        ),
                    ]
                    for loc in candidates:
                        try:
                            if await loc.count() <= 0:
                                continue
                            candidate = loc.first
                            try:
                                if not await candidate.is_visible():
                                    continue
                            except Exception:
                                pass
                            menu_btn = candidate
                            break
                        except Exception:
                            continue
                    if menu_btn is None:
                        # Fallback: role-based detection (A/B tests can rename aria-labels).
                        try:
                            role_btn = header.get_by_role(
                                "button",
                                name=re.compile(r"(more|options|opciones)", re.IGNORECASE),
                            )
                            if await role_btn.count() > 0:
                                menu_btn = role_btn.first
                        except Exception:
                            menu_btn = None
                    if menu_btn is None:
                        _dm_log("PROFILE_MENU_BTN_MISSING", reason="not_found", url=page.url if page else "")
                        return False, False

                    if _debug_enabled():
                        probe_btn = await _probe_locator(menu_btn)
                        _dm_log("PROFILE_MENU_BTN", url=page.url if page else "", **probe_btn)
                    else:
                        _dm_log("PROFILE_MENU_BTN", url=page.url if page else "", found=True)
                    try:
                        await menu_btn.scroll_into_view_if_needed(timeout=2_000)
                    except Exception:
                        pass
                    try:
                        await menu_btn.click()
                    except Exception as exc:
                        _dm_log("PROFILE_MENU_CLICK_FAIL", url=page.url if page else "", error=repr(exc))
                        raise

                    overlay = page.locator("[role='menu'], [role='dialog'], div[aria-modal='true']").last
                    try:
                        await overlay.wait_for(state="visible", timeout=3_000)
                    except Exception:
                        # If the overlay didn't materialize, we can't confirm absence.
                        _dm_log("PROFILE_MENU_OVERLAY_MISSING", url=page.url if page else "")
                        return False, False

                    _dm_log("PROFILE_MENU_OPEN", url=page.url if page else "")
                    dm_regex_exact = re.compile(r"^(send message|enviar mensaje|message|mensaje)$", re.IGNORECASE)
                    dm_item: Optional[Locator] = None
                    for role in ("menuitem", "button", "link"):
                        try:
                            loc = overlay.get_by_role(role, name=dm_regex_exact)
                            if await loc.count() > 0:
                                dm_item = loc.first
                                break
                        except Exception:
                            continue
                    if dm_item is None:
                        # Some menus render as plain text nodes; try a text-based fallback inside overlay only.
                        try:
                            loc = overlay.locator(
                                "text=/^(Send message|Enviar mensaje|Message|Mensaje)$/i"
                            )
                            if await loc.count() > 0:
                                dm_item = loc.first
                        except Exception:
                            dm_item = None

                    if dm_item is None:
                        _dm_log("PROFILE_MENU_DM_ITEM_MISSING", url=page.url if page else "")
                        # Close menu to avoid interfering with subsequent steps.
                        try:
                            await page.keyboard.press("Escape")
                        except Exception:
                            pass
                        return True, False

                    if _debug_enabled():
                        probe_item = await _probe_locator(dm_item)
                        _dm_log("PROFILE_MENU_DM_ITEM", url=page.url if page else "", **probe_item)
                    try:
                        await dm_item.click()
                    except Exception as exc:
                        _dm_log("PROFILE_MENU_DM_ITEM_CLICK_FAIL", url=page.url if page else "", error=repr(exc))
                        raise
                    _dm_log("PROFILE_MENU_DM_ITEM_CLICKED", url=page.url if page else "")
                    return True, True

                availability = await detect_dm_availability(page)
                if availability == DmAvailability.NO_DM:
                    snap = await _debug_screenshot(page, "SKIP_NO_DM", tag="profile")
                    header_html = await _debug_dump_outer_html(page.locator("header"), "SKIP_NO_DM", tag="header")
                    main_html = await _debug_dump_outer_html(page.locator("main"), "SKIP_NO_DM", tag="main")
                    _dm_log(
                        "SKIP_NO_DM",
                        url=page.url if page else "",
                        availability=getattr(availability, "value", str(availability)),
                        screenshot=snap,
                        header_html=header_html,
                        main_html=main_html,
                    )
                    logger.info(NO_DM_PERMISSION_DETECTED_LOG)
                    logger.info(NO_DM_SKIP_LOG)
                    return NO_DM_SEND_METHOD
                _dm_log(
                    "PROFILE_MESSAGE_BUTTON_MISSING",
                    url=page.url if page else "",
                    availability=getattr(availability, "value", str(availability)),
                )
                # Recovery: en muchos perfiles (p.ej. cuentas privadas) no hay botón "Mensaje"
                # en el header, pero el DM igualmente funciona por /direct/new.
                try:
                    _dm_log("PROFILE_RECOVERY_DIRECT_NEW_START", url=page.url if page else "")
                    recovered_method = await _send_via_direct_new()
                    _dm_log(
                        "PROFILE_RECOVERY_DIRECT_NEW_OK",
                        method=recovered_method,
                        url=page.url if page else "",
                    )
                    return recovered_method
                except Exception as exc2:
                    snap = await _debug_screenshot(page, "PROFILE_RECOVERY_DIRECT_NEW_FAIL", tag="recover")
                    dialog_html = await _debug_dump_outer_html(
                        page.locator("[role='dialog']"), "PROFILE_RECOVERY_DIRECT_NEW_FAIL", tag="dialog"
                    )
                    _dm_log(
                        "PROFILE_RECOVERY_DIRECT_NEW_FAIL",
                        url=page.url if page else "",
                        error=repr(exc2),
                        screenshot=snap,
                        dialog_html=dialog_html,
                    )

                # Profile overflow menu (3 dots): some accounts only expose DM from here.
                menu_checked = False
                menu_clicked = False
                try:
                    _dm_log("PROFILE_MENU_CHECK_START", url=page.url if page else "")
                    menu_checked, menu_clicked = await _profile_menu_check_and_click_dm()
                except Exception as exc_menu:
                    # Technical failure opening/using the menu, keep as real error.
                    snap = await _debug_screenshot(page, "PROFILE_MENU_CHECK_FAIL", tag="menu")
                    overlay_html = await _debug_dump_outer_html(
                        page.locator("[role='menu'], [role='dialog'], div[aria-modal='true']").last,
                        "PROFILE_MENU_CHECK_FAIL",
                        tag="overlay",
                    )
                    _dm_log(
                        "PROFILE_MENU_CHECK_FAIL",
                        url=page.url if page else "",
                        error=repr(exc_menu),
                        screenshot=snap,
                        overlay_html=overlay_html,
                    )
                    raise

                if menu_clicked:
                    t0 = time.time()
                    try:
                        await page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
                        _dm_log(
                            "PROFILE_MENU_COMPOSER_READY",
                            url=page.url if page else "",
                            elapsed_ms=int((time.time() - t0) * 1000),
                        )
                    except Exception as exc:
                        snap = await _debug_screenshot(page, "PROFILE_MENU_COMPOSER_MISSING", tag="after_menu")
                        main_html = await _debug_dump_outer_html(page.locator("main"), "PROFILE_MENU_COMPOSER_MISSING", tag="main")
                        _dm_log(
                            "PROFILE_MENU_COMPOSER_MISSING",
                            url=page.url if page else "",
                            elapsed_ms=int((time.time() - t0) * 1000),
                            error=repr(exc),
                            screenshot=snap,
                            main_html=main_html,
                        )
                        raise RuntimeError("No aparecio la caja de texto del chat tras usar el menu (3 puntos).")
                    return await self._type_and_send(page, text)

                if menu_checked:
                    # Confirmed: no DM option available in profile button nor menu.
                    snap = await _debug_screenshot(page, "SKIP_NO_DM", tag="profile_menu")
                    header_html = await _debug_dump_outer_html(page.locator("header"), "SKIP_NO_DM", tag="header")
                    main_html = await _debug_dump_outer_html(page.locator("main"), "SKIP_NO_DM", tag="main")
                    _dm_log(
                        "SKIP_NO_DM",
                        url=page.url if page else "",
                        availability=getattr(availability, "value", str(availability)),
                        screenshot=snap,
                        header_html=header_html,
                        main_html=main_html,
                    )
                    logger.info(NO_DM_PERMISSION_DETECTED_LOG)
                    logger.info(NO_DM_SKIP_LOG)
                    return NO_DM_SEND_METHOD

                if click_error is not None:
                    logger.debug("Error clickeando boton de mensaje: %s", click_error)
                snap = await _debug_screenshot(page, "PROFILE_NO_MESSAGE_BUTTON", tag="missing")
                header_html = await _debug_dump_outer_html(page.locator("header"), "PROFILE_NO_MESSAGE_BUTTON", tag="header")
                _dm_log(
                    "PROFILE_NO_MESSAGE_BUTTON",
                    url=page.url if page else "",
                    screenshot=snap,
                    header_html=header_html,
                )
                raise RuntimeError("No se encontro boton 'Enviar mensaje' en el perfil del usuario.")

            t0 = time.time()
            try:
                await page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
                _dm_log(
                    "PROFILE_COMPOSER_READY",
                    url=page.url if page else "",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            except Exception as exc:
                snap = await _debug_screenshot(page, "PROFILE_COMPOSER_MISSING", tag="after_click")
                main_html = await _debug_dump_outer_html(page.locator("main"), "PROFILE_COMPOSER_MISSING", tag="main")
                _dm_log(
                    "PROFILE_COMPOSER_MISSING",
                    url=page.url if page else "",
                    elapsed_ms=int((time.time() - t0) * 1000),
                    error=repr(exc),
                    screenshot=snap,
                    main_html=main_html,
                )
                raise RuntimeError("No aparecio la caja de texto del chat tras clickear 'Enviar mensaje'.")

            return await self._type_and_send(page, text)

        async def _send_via_direct_new() -> str:
            if page is None:
                raise RuntimeError("Pagina no inicializada.")

            _dm_log("DIRECT_NEW_START", url=page.url if page else "")
            if not await self._open_new_message_dialog(page):
                snap = await _debug_screenshot(page, "DIRECT_NEW_NO_DIALOG", tag="open")
                dialog_html = await _debug_dump_outer_html(page.locator("[role='dialog']"), "DIRECT_NEW_NO_DIALOG", tag="dialog")
                _dm_log(
                    "DIRECT_NEW_NO_DIALOG",
                    url=page.url if page else "",
                    screenshot=snap,
                    dialog_html=dialog_html,
                )
                raise RuntimeError("No se pudo abrir el dialogo de nuevo mensaje (/direct/new).")

            if not await self._search_and_select(page, normalized_target):
                snap = await _debug_screenshot(page, "DIRECT_NEW_SELECT_FAIL", tag="search")
                dialog_html = await _debug_dump_outer_html(page.locator("[role='dialog']"), "DIRECT_NEW_SELECT_FAIL", tag="dialog")
                _dm_log(
                    "DIRECT_NEW_SELECT_FAIL",
                    url=page.url if page else "",
                    screenshot=snap,
                    dialog_html=dialog_html,
                )
                raise RuntimeError("No se pudo seleccionar el usuario en el dialogo.")

            if not await self._confirm_next(page):
                snap = await _debug_screenshot(page, "DIRECT_NEW_CONFIRM_FAIL", tag="next")
                dialog_html = await _debug_dump_outer_html(page.locator("[role='dialog']"), "DIRECT_NEW_CONFIRM_FAIL", tag="dialog")
                _dm_log(
                    "DIRECT_NEW_CONFIRM_FAIL",
                    url=page.url if page else "",
                    screenshot=snap,
                    dialog_html=dialog_html,
                )
                raise RuntimeError("No se pudo abrir el chat (Next/Chat).")

            t0 = time.time()
            try:
                await page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
                _dm_log(
                    "DIRECT_NEW_COMPOSER_READY",
                    url=page.url if page else "",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            except Exception as exc:
                snap = await _debug_screenshot(page, "DIRECT_NEW_COMPOSER_MISSING", tag="chat")
                main_html = await _debug_dump_outer_html(page.locator("main"), "DIRECT_NEW_COMPOSER_MISSING", tag="main")
                _dm_log(
                    "DIRECT_NEW_COMPOSER_MISSING",
                    url=page.url if page else "",
                    elapsed_ms=int((time.time() - t0) * 1000),
                    error=repr(exc),
                    screenshot=snap,
                    main_html=main_html,
                )
                raise RuntimeError("No aparecio la caja de texto del chat tras abrir el dialogo.")

            return await self._type_and_send(page, text)

        try:
            svc, ctx, page = await ensure_logged_in_async(
                account,
                headless=self.headless,
                proxy=(
                    normalize_playwright_proxy(proxy)
                    if proxy
                    else proxy_from_account(account)
                ),
            )
            _dm_log("LOGIN_OK", url=page.url if page else "")

            used_strategy = "profile"
            if strategy == "direct_new":
                send_method = await _send_via_direct_new()
                used_strategy = "direct_new"
            elif strategy == "auto":
                try:
                    send_method = await _send_via_direct_new()
                    used_strategy = "direct_new"
                except Exception as exc:
                    _dm_log("DIRECT_NEW_FAILED", url=page.url if page else "", error=repr(exc))
                    # Soft fail: only becomes an error if profile fallback fails too.
                    logger.info(
                        "Direct/new fallo para @%s -> @%s: %s. Fallback a perfil.",
                        username,
                        normalized_target,
                        exc,
                    )
                    send_method = await _send_via_profile()
                    used_strategy = "profile"
            else:
                send_method = await _send_via_profile()
                used_strategy = "profile"

            if send_method == NO_DM_SEND_METHOD:
                snap = await _debug_screenshot(page, "SKIP_NO_DM", tag="final")
                if snap:
                    payload.setdefault("debug_screenshot", snap)
                _dm_log("SKIP_DECISION", reason=NO_DM_SKIP_REASON, url=page.url if page else "", screenshot=snap)
                payload.update(
                    {
                        "engine": "playwright_async",
                        "url": page.url if page else "",
                        "strategy": used_strategy,
                        "send_method": NO_DM_SEND_METHOD,
                        "skip_reason": SKIPPED_NO_DM_REASON,
                        "skip_reason_legacy": LEGACY_NO_DM_REASON,
                        "skip_detail": NO_DM_SKIP_DETAIL,
                        "skipped": True,
                    }
                )
                if return_payload:
                    return False, SKIPPED_NO_DM_REASON, payload
                return (False, SKIPPED_NO_DM_REASON) if return_detail else False

            composer = await self._composer(page)
            ok, reason = await self._confirm_message_sent(page, text, composer=composer)
            _dm_log("VERIFY", ok=ok, reason=reason, url=page.url if page else "")
            payload["verified"] = bool(ok)
            if not ok and reason in {"login_lost", "challenge_detected"}:
                # Health is Playwright-only. If we lose login or hit a challenge mid-send,
                # persist the account as blocked/session-expired without altering business flow.
                try:
                    import health_store

                    if reason == "login_lost":
                        health_store.update_from_playwright_status(username, "session_expired", reason=reason)
                    else:
                        health_store.update_from_playwright_status(username, "checkpoint", reason=reason)
                except Exception:
                    pass
            if not ok and reason in {"message_not_present_after_send", "composer_not_cleared"}:
                try:
                    composer_text = await self._composer_text(composer) if composer else ""
                except Exception:
                    composer_text = ""
                if composer_text:
                    try:
                        await composer.press("Control+Enter")
                        await self._sleep(0.25, 0.6)
                    except Exception:
                        pass
                ok_retry, reason_retry = await self._confirm_message_sent(
                    page,
                    text,
                    composer=composer,
                )
                if ok_retry:
                    ok = True
                    reason = reason_retry
                    payload["verified"] = True
            if not ok:
                if reason in _UNVERIFIED_REASONS:
                    current_url = page.url if page else ""
                    detail = "sent_request" if "/direct/requests" in (current_url or "") else "sent_unverified"
                    payload["verification_reason"] = reason
                    payload["detail"] = detail
                    if detail == "sent_unverified":
                        payload["sent_unverified"] = True
                        payload["reason_code"] = "SENT_UNVERIFIED"
                    if detail == "sent_unverified" and not ALLOW_UNVERIFIED:
                        strict_detail = f"sent_unverified ({reason})"
                        if return_payload:
                            return False, strict_detail, payload
                        return (False, strict_detail) if return_detail else False
                    if return_payload:
                        return True, detail, payload
                    return (True, detail) if return_detail else True
                raise RuntimeError(reason)

            storage_path = Path(BASE_PROFILES) / username / "storage_state.json"
            try:
                await svc.save_storage_state(ctx, storage_path)
            except Exception:
                pass
            payload.update(
                {
                    "engine": "playwright_async",
                    "url": page.url if page else "",
                    "strategy": used_strategy,
                    "send_method": send_method,
                    "verified": payload.get("verified", True),
                }
            )
            screenshot = await self._capture_success(page, username, normalized_target)
            if screenshot:
                payload["screenshot"] = screenshot
            logger.info(
                "Mensaje enviado con Playwright (%s): @%s -> @%s",
                used_strategy,
                username,
                normalized_target,
            )
            if return_payload:
                return True, payload.get("detail"), payload
            return (True, payload.get("detail")) if return_detail else True
        except Exception as exc:
            detail = str(exc)
            snap = await _debug_screenshot(page, "EXCEPTION", tag=type(exc).__name__)
            main_html = await _debug_dump_outer_html(page.locator("main") if page else None, "EXCEPTION", tag="main")
            _dm_log(
                "EXCEPTION",
                url=page.url if page else "",
                error=repr(exc),
                screenshot=snap,
                main_html=main_html,
            )
            await self._capture_debug(page, username, normalized_target, detail)
            logger.warning(
                "Fallo envio humano con @%s -> @%s: %s",
                username,
                normalized_target,
                exc,
            )
            if return_payload:
                return False, detail, payload
            return (False, detail) if return_detail else False
        finally:
            # Si quedamos en captcha/suspension o two_factor, dejamos el navegador abierto
            stay_open = False
            try:
                current_url = page.url if page else ""
                if current_url:
                    stay_open = any(
                        token in current_url
                        for token in (
                            "accounts/suspended",
                            "two_factor",
                            "challenge",
                            "checkpoint",
                            "accounts/confirm_email",
                        )
                    )
            except Exception:
                stay_open = False

            if not stay_open:
                if ctx is not None:
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                if svc is not None:
                    try:
                        await svc.close()
                    except Exception:
                        pass
            if debug_token is not None:
                try:
                    _DM_CTX.reset(debug_token)
                except Exception:
                    pass

    async def _capture_debug(self, page: Optional[Page], username: str, target: str, reason: Optional[str]) -> None:
        if page is None:
            return
        try:
            folder = Path(BASE_PROFILES) / username / "dm_errors"
            folder.mkdir(parents=True, exist_ok=True)
            safe_target = re.sub(r"[^a-z0-9]+", "_", target.lower()).strip("_") or "target"
            safe_reason = re.sub(r"[^a-z0-9]+", "_", (reason or "error").lower()).strip("_") or "error"
            ts = int(time.time())
            await page.screenshot(path=str(folder / f"{safe_target}_{safe_reason}_{ts}.png"))
            try:
                html = await page.content()
                html_path = folder / f"{safe_target}_{safe_reason}_{ts}.html"
                html_path.write_text(html, encoding="utf-8")
            except Exception:
                pass
        except Exception:
            pass
