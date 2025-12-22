from __future__ import annotations

import logging
import random
import re
import time
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

from playwright.sync_api import Locator, Page, TimeoutError as PwTimeoutError

from src.auth.persistent_login import ensure_logged_in
from src.humanizer import human_click
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

    def _sleep(self, low: float, high: float) -> None:
        time.sleep(random.uniform(low, high))

    def _normalize_username(self, username: str) -> str:
        return username.strip().lstrip("@").split("?", 1)[0]


    def _goto_inbox(self, page: Page) -> None:
        page.goto(DIRECT_INBOX, wait_until="domcontentloaded", timeout=45_000)
        try:
            page.wait_for_selector("nav[role='navigation'], [role='dialog'], a[href='/direct/new/']", timeout=15_000)
        except Exception:
            pass

    def _dialog_ready(self, page: Page) -> bool:
        return page.locator(", ".join(SEARCH_INPUTS)).count() > 0

    def _open_new_message_dialog(self, page: Page) -> bool:
        try:
            page.goto(DIRECT_NEW, wait_until="domcontentloaded", timeout=45_000)
        except PwTimeoutError:
            pass
        if self._dialog_ready(page):
            return True

        self._goto_inbox(page)
        buttons = page.locator(", ".join(NEW_MESSAGE_BUTTONS))
        if buttons.count() > 0:
            target = buttons.first
            try:
                target.scroll_into_view_if_needed(timeout=2_000)
            except Exception:
                pass
            target.click()
            try:
                page.wait_for_selector(f"{DIALOG_SELECTOR}, {SEARCH_INPUTS}", timeout=15_000)
            except Exception:
                pass
        return self._dialog_ready(page)

    def _search_and_select(self, page: Page, handle: str) -> bool:
        cleaned = handle.strip().lstrip("@")
        if not cleaned:
            return False
        search_locator = page.locator(", ".join(SEARCH_INPUTS))
        if search_locator.count() == 0:
            return False
        field = search_locator.first
        try:
            field.fill("")
        except Exception:
            pass
        normalized = cleaned.lower()
        print("debug Escribiendo handle:", normalized)
        for ch in normalized:
            field.type(ch, delay=random.randint(55, 85))
        try:
            # Aumentamos espera para que IG cargue resultados 
            page.wait_for_timeout(4000)
        except Exception:
            pass

        # Estrategia 1: Buscar por selectores estándar (globales)
        buttons = page.locator("[role='button']")
        candidate = buttons.filter(has_text=re.compile(rf"^{re.escape(normalized)}$", re.IGNORECASE))
        
        selection: Optional[Locator] = None
        if candidate.count() > 0:
            selection = candidate.first
        else:
            # Estrategia 2: Buscar contenedor directo que tenga el texto
            direct_text = page.locator(f"div[role='dialog'] div").filter(has_text=normalized)
            if direct_text.count() > 0:
                selection = direct_text.last # Usamos last porque suele ser el nodo texto mas profundo
            
        if selection is None:
             # Estrategia 4: Buscar inputs de tipo radio/checkbox GLOBALMENTE
            radio_inputs = page.locator("input[type='radio'], input[type='checkbox']")
            if radio_inputs.count() > 0:
                 logger.info("Encontrados inputs de selección (radio/checkbox). Clickeando el primero.")
                 selection = radio_inputs.first
                 # A veces hay que clickear el padre o label
                 try:
                     selection.click(force=True)
                     return True
                 except: 
                     pass

             # Estrategia 5: Fallback a lista genérica GLOBAL
            items = page.locator("[role='button'], li")
            visible_count = items.count()
            print("debug Resultados visibles:", visible_count)
            limit = min(visible_count, 5)
            for idx in range(limit):
                try:
                    text_value = (items.nth(idx).inner_text() or "").strip().lower()
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
                fallback.click()
                return True
            except:
                return False

        try:
            selection.scroll_into_view_if_needed(timeout=2_000)
        except Exception:
            pass
        selection.click()
        return True

    def _confirm_next(self, page: Page) -> bool:
        # 1. Verificar si ya estamos en el chat (si al seleccionar usuario nos llevó directo)
        try:
            if self._composer(page):
                logger.info("Composer ya visible inmediatamente. Asumiendo éxito.")
                return True
        except: pass

        # 2. Buscar botones de confirmación
        potential_texts = ["Next", "Siguiente", "Chat", "Conversar"]
        
        # Intentar clic en botones estandar
        try:
            nxt = page.locator(", ".join(NEXT_BTNS))
            if nxt.count() > 0:
                # Filtrar visibilidad
                for i in range(nxt.count()):
                    if nxt.nth(i).is_visible():
                        nxt.nth(i).click()
                        self._sleep(1, 2)
                        return True
        except Exception as e:
            logger.debug(f"Fallo click selector estandar: {e}")

        # 3. Fuerza bruta: Buscar texto 'Chat'/'Next' arriba a la derecha (común en web)
        logger.info("Buscando botón Next/Chat por texto...")
        for text in potential_texts:
            try:
                # Buscamos botones o divs con ese texto exacto
                el = page.get_by_role("button", name=text).first
                if el.is_visible():
                    el.click()
                    self._sleep(1, 2)
                    return True
                
                # O simplemente texto clickeable
                el_text = page.get_by_text(text, exact=True).first
                if el_text.is_visible():
                    el_text.click()
                    self._sleep(1, 2)
                    return True
            except:
                continue

        # 4. Esperar un poco y verificar composer de nuevo
        try:
            page.wait_for_selector(", ".join(COMPOSERS), timeout=5000)
            return True
        except:
            pass

        return False

    def _composer(self, page: Page) -> Optional[Locator]:
        locator = page.locator(", ".join(COMPOSERS))
        return locator.first if locator.count() else None

    def _type_text(self, composer: Locator, text: str) -> None:
        composer.click()
        try:
            composer.fill("")
        except Exception:
            pass
        payload = text.replace("\r\n", "\n")
        parts = payload.split("\n")
        for idx, part in enumerate(parts):
            if idx > 0:
                composer.press("Shift+Enter")
                self._sleep(0.08, 0.25)
            if not part:
                continue
            for ch in part:
                composer.type(ch, delay=random.randint(30, 120))
            self._sleep(0.05, 0.2)

    def _type_and_send(self, page: Page, text: str) -> None:
        try:
            page.wait_for_selector(", ".join(COMPOSERS), timeout=20_000)
        except Exception:
            pass
        composer = self._composer(page)
        if composer is None:
            raise RuntimeError("Composer no encontrado.")
        if not text.strip():
            raise ValueError("El mensaje está vacío.")
        self._type_text(composer, text)
        self._sleep(0.25, 0.9)
        send_candidates = [
            SEND_BUTTONS,
            "button[aria-label*='Send']",
            "button[aria-label*='Enviar']",
        ]
        clicked = False
        for sel in send_candidates:
            btn = page.locator(sel)
            if btn.count():
                human_click(btn.first)
                clicked = True
                break
        if not clicked:
            composer.press("Enter")
        self._sleep(0.3, 1.0)

    def send_message_like_human(
        self,
        account: Dict,
        target_username: str,
        text: str,
        *,
        base_delay_seconds: float = 0,
        jitter_seconds: float = 0,
        proxy: Optional[Dict] = None,
        return_detail: bool = False,
    ) -> Union[bool, Tuple[bool, Optional[str]]]:
        detail: Optional[str] = None
        username = account.get("username") or ""
        if not username:
            detail = "Cuenta sin username configurado."
            return (False, detail) if return_detail else False
        svc: Optional[PlaywrightService] = None
        ctx = None
        page: Optional[Page] = None
        normalized_target = self._normalize_username(target_username)
        if not normalized_target:
            detail = "Lead sin username."
            return (False, detail) if return_detail else False
        delay_total = 0.0
        if base_delay_seconds or jitter_seconds:
            jitter = max(0.0, jitter_seconds)
            delay_total = max(0.0, base_delay_seconds) + random.uniform(0, jitter)
            if delay_total > 0:
                time.sleep(delay_total)
        try:
            svc, ctx, page = ensure_logged_in(
                account,
                headless=self.headless,
                proxy=proxy or account.get("proxy"),
            )
            
            # ESTRATEGIA DIRECTA: PERFIL -> MENSAJE
            # Evitamos el buscador de /direct/new/ que falla visualmente
            profile_url = f"https://www.instagram.com/{normalized_target}/"
            logger.info(f"Navegando al perfil directo: {profile_url}")
            
            try:
                page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
                self._sleep(2, 4)
            except Exception as e:
                logger.warning(f"Error cargando perfil: {e}")

            # Buscar botón de mensaje en el perfil
            msg_btn_selectors = [
                "div[role='button']:has-text('Message')",
                "div[role='button']:has-text('Enviar mensaje')",
                "button:has-text('Message')",
                "button:has-text('Enviar mensaje')",
            ]
            
            clicked = False
            for sel in msg_btn_selectors:
                if page.locator(sel).count() > 0:
                    try:
                        logger.info(f"Clickeando botón de mensaje en perfil: {sel}")
                        page.locator(sel).first.click()
                        clicked = True
                        break
                    except: pass
            
            if not clicked:
                raise RuntimeError("No se encontró botón 'Enviar mensaje' en el perfil del usuario.")

            # Esperar a que cargue el chat (input de texto)
            # Usamos los COMPOSERS globales que ya definimos (divs y textareas)
            try:
                page.wait_for_selector(", ".join(COMPOSERS), timeout=25_000)
            except:
                raise RuntimeError("No apareció la caja de texto del chat tras clickear 'Enviar mensaje'.")

            # Escribir y enviar
            self._type_and_send(page, text)
            
            storage_path = Path(BASE_PROFILES) / username / "storage_state.json"
            try:
                svc.save_storage_state(ctx, storage_path)
            except Exception:
                pass
            logger.info("Mensaje enviado con Playwright (Vía Perfil): @%s → @%s", username, normalized_target)
            return (True, None) if return_detail else True
        except Exception as exc:
            detail = str(exc)
            self._capture_debug(page, username, normalized_target, detail)
            logger.warning(
                "Falló envío humano con @%s → @%s: %s",
                username,
                normalized_target,
                exc,
            )
            return (False, detail) if return_detail else False
        finally:
            # Si quedamos en captcha/suspensión o two_factor, dejamos el navegador abierto
            stay_open = False
            try:
                current_url = page.url if page else ""
                stay_open = current_url and ("accounts/suspended" in current_url or "two_factor" in current_url)
            except Exception:
                stay_open = False

            if not stay_open:
                if ctx is not None:
                    try:
                        ctx.close()
                    except Exception:
                        pass
                if svc is not None:
                    try:
                        svc.close()
                    except Exception:
                        pass

    def _capture_debug(self, page: Optional[Page], username: str, target: str, reason: Optional[str]) -> None:
        if page is None:
            return
        try:
            folder = Path(BASE_PROFILES) / username / "dm_errors"
            folder.mkdir(parents=True, exist_ok=True)
            safe_target = re.sub(r"[^a-z0-9]+", "_", target.lower()).strip("_") or "target"
            safe_reason = re.sub(r"[^a-z0-9]+", "_", (reason or "error").lower()).strip("_") or "error"
            ts = int(time.time())
            page.screenshot(path=str(folder / f"{safe_target}_{safe_reason}_{ts}.png"))
        except Exception:
            pass
