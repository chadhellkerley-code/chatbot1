from __future__ import annotations

import asyncio
import os
import logging
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from playwright.async_api import Locator, Page, TimeoutError as PwTimeoutError

from src.auth.persistent_login import ensure_logged_in_async
from src.humanizer import random_wait
from src.playwright_service import BASE_PROFILES, PlaywrightService

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
SEND_BUTTONS = "div[role='button']:has-text('Send'), div[role='button']:has-text('Enviar')"


class HumanInstagramSender:
    def __init__(self, headless: bool = True) -> None:
        self.headless = headless

    async def _sleep(self, low: float, high: float) -> None:
        await asyncio.sleep(random.uniform(low, high))

    def _normalize_username(self, username: str) -> str:
        return username.strip().lstrip("@").split("?", 1)[0]


    async def _goto_inbox(self, page: Page) -> None:
        await page.goto(DIRECT_INBOX, wait_until="domcontentloaded", timeout=45_000)
        try:
            await page.wait_for_selector("nav[role='navigation'], [role='dialog'], a[href='/direct/new/']", timeout=15_000)
        except Exception:
            pass

    async def _dialog_ready(self, page: Page) -> bool:
        return await page.locator(", ".join(SEARCH_INPUTS)).count() > 0

    async def _open_new_message_dialog(self, page: Page) -> bool:
        try:
            await page.goto(DIRECT_NEW, wait_until="domcontentloaded", timeout=45_000)
        except PwTimeoutError:
            pass
        if await self._dialog_ready(page):
            return True

        await self._goto_inbox(page)
        buttons = page.locator(", ".join(NEW_MESSAGE_BUTTONS))
        if await buttons.count() > 0:
            target = buttons.first
            try:
                await target.scroll_into_view_if_needed(timeout=2_000)
            except Exception:
                pass
            await target.click()
            try:
                await page.wait_for_selector(f"{DIALOG_SELECTOR}, {SEARCH_INPUTS}", timeout=15_000)
            except Exception:
                pass
        return await self._dialog_ready(page)

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
        if await search_locator.count() == 0:
            return False
        field = search_locator.first
        normalized = cleaned.lower()
        print("debug Escribiendo handle:", normalized)
        await self._type_search_handle(field, normalized)
        typed_value = (await self._read_search_value(field)).strip().lower()
        if normalized not in typed_value:
            logger.info("Busqueda incompleta ('%s' -> '%s'), reintentando.", normalized, typed_value)
            await self._type_search_handle(field, normalized)
        try:
            # Aumentamos espera para que IG cargue resultados 
            await page.wait_for_timeout(4000)
        except Exception:
            pass

        # Estrategia 1: Buscar por selectores estándar (globales)
        buttons = page.locator("[role='button']")
        candidate = buttons.filter(has_text=re.compile(rf"^{re.escape(normalized)}$", re.IGNORECASE))
        
        selection: Optional[Locator] = None
        if await candidate.count() > 0:
            selection = candidate.first
        else:
            # Estrategia 2: Buscar contenedor directo que tenga el texto
            direct_text = page.locator(f"div[role='dialog'] div").filter(has_text=normalized)
            if await direct_text.count() > 0:
                selection = direct_text.last # Usamos last porque suele ser el nodo texto mas profundo
            
        if selection is None:
             # Estrategia 4: Buscar inputs de tipo radio/checkbox GLOBALMENTE
            radio_inputs = page.locator("input[type='radio'], input[type='checkbox']")
            if await radio_inputs.count() > 0:
                 logger.info("Encontrados inputs de selección (radio/checkbox). Clickeando el primero.")
                 selection = radio_inputs.first
                 # A veces hay que clickear el padre o label
                 try:
                     await selection.click(force=True)
                     return True
                 except: 
                     pass

             # Estrategia 5: Fallback a lista genérica GLOBAL
            items = page.locator("[role='button'], li")
            visible_count = await items.count()
            print("debug Resultados visibles:", visible_count)
            limit = min(visible_count, 5)
            for idx in range(limit):
                try:
                    text_value = (await items.nth(idx).inner_text() or "").strip().lower()
                except Exception:
                    continue
                if normalized in text_value:
                    selection = items.nth(idx)
                    break
            if selection is None and visible_count > 0:
                selection = items.first
        
        if selection is None:
            # Ultimo intento desesperado: Buscar el texto literal y clickearlo
            logger.info(f"Búsqueda estándar falló. Intentando clic por texto: {normalized}")
            try:
                # Buscamos en todo el dialogo
                fallback = page.locator("div[role='dialog']").get_by_text(normalized, exact=False).first
                await fallback.click()
                return True
            except:
                return False

        try:
            await selection.scroll_into_view_if_needed(timeout=2_000)
        except Exception:
            pass
        await selection.click()
        return True

    async def _confirm_next(self, page: Page) -> bool:
        # 1. Verificar si ya estamos en el chat (si al seleccionar usuario nos llevó directo)
        try:
            if await self._composer(page):
                logger.info("Composer ya visible inmediatamente. Asumiendo éxito.")
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
                        return True
        except Exception as e:
            logger.debug(f"Fallo click selector estandar: {e}")

        # 3. Fuerza bruta: Buscar texto 'Chat'/'Next' arriba a la derecha (común en web)
        logger.info("Buscando botón Next/Chat por texto...")
        for text in potential_texts:
            try:
                # Buscamos botones o divs con ese texto exacto
                el = page.get_by_role("button", name=text).first
                if await el.is_visible():
                    await el.click()
                    await self._sleep(1, 2)
                    return True
                
                # O simplemente texto clickeable
                el_text = page.get_by_text(text, exact=True).first
                if await el_text.is_visible():
                    await el_text.click()
                    await self._sleep(1, 2)
                    return True
            except:
                continue

        # 4. Esperar un poco y verificar composer de nuevo
        try:
            await page.wait_for_selector(", ".join(COMPOSERS), timeout=5000)
            return True
        except:
            pass

        return False

    async def _composer(self, page: Page) -> Optional[Locator]:
        locator = page.locator(", ".join(COMPOSERS))
        return locator.first if await locator.count() else None

    async def _type_text(self, composer: Locator, text: str) -> None:
        await composer.click()
        try:
            await composer.fill("")
        except Exception:
            pass
        payload = text.replace("\r\n", "\n")
        parts = payload.split("\n")
        for idx, part in enumerate(parts):
            if idx > 0:
                await composer.press("Shift+Enter")
                await self._sleep(0.08, 0.25)
            if not part:
                continue
            for ch in part:
                await composer.type(ch, delay=random.randint(30, 120))
            await self._sleep(0.05, 0.2)

    async def _type_and_send(self, page: Page, text: str) -> None:
        try:
            await page.wait_for_selector(", ".join(COMPOSERS), timeout=20_000)
        except Exception:
            pass
        composer = await self._composer(page)
        if composer is None:
            raise RuntimeError("Composer no encontrado.")
        if not text.strip():
            raise ValueError("El mensaje está vacío.")
        await self._type_text(composer, text)
        await self._sleep(0.25, 0.9)
        send_candidates = [
            SEND_BUTTONS,
            "button[aria-label*='Send']",
            "button[aria-label*='Enviar']",
        ]
        clicked = False
        for sel in send_candidates:
            btn = page.locator(sel)
            if await btn.count():
                await btn.first.click()
                clicked = True
                break
        if not clicked:
            await composer.press("Enter")
        await self._sleep(0.3, 1.0)

    def _message_snippet(self, text: str, limit: int = 48) -> str:
        for line in (text or "").splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:limit]
        return (text or "").strip()[:limit]

    async def _confirm_message_sent(self, page: Page, text: str) -> bool:
        snippet = self._message_snippet(text)
        if not snippet:
            return False
        try:
            await page.wait_for_timeout(1500)
        except Exception:
            pass
        bubble_selectors = [
            "[data-testid='message-bubble'] [data-testid='own']",
            "[data-testid='message-bubble'][data-testid='own']",
        ]
        try:
            for sel in bubble_selectors:
                bubble = page.locator(sel).filter(has_text=snippet)
                if await bubble.count() > 0:
                    return True
        except Exception:
            pass
        try:
            container = page.locator("div[role='main']")
            if await container.count() > 0:
                locator = container.first.get_by_text(snippet, exact=False)
            else:
                locator = page.get_by_text(snippet, exact=False)
            return await locator.count() > 0
        except Exception:
            return False

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
        delay_total = 0.0
        if base_delay_seconds or jitter_seconds:
            jitter = max(0.0, jitter_seconds)
            delay_total = max(0.0, base_delay_seconds) + random.uniform(0, jitter)
            if delay_total > 0:
                await asyncio.sleep(delay_total)

        strategy = os.getenv("HUMAN_DM_STRATEGY", "auto").strip().lower()
        if strategy not in {"profile", "direct_new", "auto"}:
            strategy = "profile"

        async def _send_via_profile() -> None:
            if page is None:
                raise RuntimeError("Pagina no inicializada.")

            profile_url = f"https://www.instagram.com/{normalized_target}/"
            logger.info("Navegando al perfil directo: %s", profile_url)

            try:
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
                await self._sleep(2, 4)
            except Exception as exc:
                logger.warning("Error cargando perfil: %s", exc)

            msg_btn_selectors = [
                "div[role='button']:has-text('Message')",
                "div[role='button']:has-text('Enviar mensaje')",
                "button:has-text('Message')",
                "button:has-text('Enviar mensaje')",
            ]

            clicked = False
            for sel in msg_btn_selectors:
                if await page.locator(sel).count() > 0:
                    try:
                        logger.info("Clickeando boton de mensaje en perfil: %s", sel)
                        await page.locator(sel).first.click()
                        clicked = True
                        break
                    except Exception:
                        pass

            if not clicked:
                raise RuntimeError("No se encontro boton 'Enviar mensaje' en el perfil del usuario.")

            try:
                await page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
            except Exception:
                raise RuntimeError("No aparecio la caja de texto del chat tras clickear 'Enviar mensaje'.")

            await self._type_and_send(page, text)

        async def _send_via_direct_new() -> None:
            if page is None:
                raise RuntimeError("Pagina no inicializada.")

            if not await self._open_new_message_dialog(page):
                raise RuntimeError("No se pudo abrir el dialogo de nuevo mensaje (/direct/new).")

            if not await self._search_and_select(page, normalized_target):
                raise RuntimeError("No se pudo seleccionar el usuario en el dialogo.")

            if not await self._confirm_next(page):
                raise RuntimeError("No se pudo abrir el chat (Next/Chat).")

            try:
                await page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
            except Exception:
                raise RuntimeError("No aparecio la caja de texto del chat tras abrir el dialogo.")

            await self._type_and_send(page, text)

        try:
            svc, ctx, page = await ensure_logged_in_async(
                account,
                headless=self.headless,
                proxy=proxy or account.get("proxy"),
            )

            used_strategy = "profile"
            if strategy == "direct_new":
                await _send_via_direct_new()
                used_strategy = "direct_new"
            elif strategy == "auto":
                try:
                    await _send_via_direct_new()
                    used_strategy = "direct_new"
                except Exception as exc:
                    logger.warning(
                        "Direct/new fallo para @%s -> @%s: %s. Fallback a perfil.",
                        username,
                        normalized_target,
                        exc,
                    )
                    await _send_via_profile()
                    used_strategy = "profile"
            else:
                await _send_via_profile()
                used_strategy = "profile"

            if not await self._confirm_message_sent(page, text):
                raise RuntimeError("No se pudo confirmar el envio en la interfaz.")

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
                return True, None, payload
            return (True, None) if return_detail else True
        except Exception as exc:
            detail = str(exc)
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
                stay_open = current_url and (
                    "accounts/suspended" in current_url or "two_factor" in current_url
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
        except Exception:
            pass
