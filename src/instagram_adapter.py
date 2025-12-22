from __future__ import annotations

import base64
import logging
import os
import time
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from playwright.sync_api import BrowserContext, Page
from dotenv import load_dotenv

from src.humanizer import human_click, human_type, human_wait
from src.playwright_service import BASE_PROFILES

logger = logging.getLogger(__name__)

BASE_URL = "https://www.instagram.com/"
LOGIN_URL = f"{BASE_URL}accounts/login/"
INBOX_URL = f"{BASE_URL}direct/inbox/"
INSTAGRAM_URL = BASE_URL
TOTP_INPUT_SELECTORS = (
    "input[name='verificationCode']",
    "input[name='verification_code']",
    "input[name='security_code']",
    "input[name='code']",
    "input[aria-label*='Security']",
    "input[aria-label*='seguridad']",
    "input[aria-label*='Código']",
    "input[aria-label*='Codigo']",
    "input[placeholder*='Código']",
    "input[placeholder*='Codigo']",
    "input[placeholder*='codigo de seguridad']",
    "input[placeholder*='seguridad']",
    "input[type='text'][autocomplete='one-time-code']",
)
SAVE_LOGIN_NOT_NOW_SELECTORS = (
    "div[role='dialog'] button:has-text('Not now')",
    "div[role='dialog'] button:has-text('Not Now')",
    "div[role='dialog'] button:has-text('Ahora no')",
    "button:has-text('Not now')",
    "button:has-text('Not Now')",
    "button:has-text('Ahora no')",
    "button:has-text('Save info')",  # A veces es mejor darle a guardar para avanzar
    "button:has-text('Guardar información')",
)
NOTIFICATION_NOT_NOW_SELECTORS = (
    "div[role='dialog'] button:has-text('Not now')",
    "div[role='dialog'] button:has-text('Not Now')",
    "div[role='dialog'] button:has-text('Ahora no')",
    "button:has-text(\"Don't allow\")",
    "button:has-text('Not now')",
    "button:has-text('Ahora no')",
)
CHALLENGE_SEND_CODE_SELECTORS = (
    "button:has-text('Send code')",
    "button:has-text('Send security code')",
    "button:has-text('Next')",
    "button:has-text('Continue')",
    "button:has-text('Confirmar')",
    "button:has-text('Confirm')",
)
CHALLENGE_SUBMIT_SELECTORS = (
    "button[type='submit']",
    "button:has-text('Confirmar')",
    "button:has-text('Confirm')",
    "button:has-text('Done')",
)

TotpProvider = Callable[[str], Optional[str]]
CodeProvider = Callable[[], Optional[str]]

LOGIN_FAILED_DIR = Path(BASE_PROFILES) / "login_failed_screenshots"


def _has_auth_cookies(context: BrowserContext) -> bool:
    try:
        cookies = context.cookies(["https://www.instagram.com/"])
    except TypeError:
        cookies = context.cookies("https://www.instagram.com/")
    names = {c.get("name"): c.get("value") for c in cookies}
    return bool(names.get("sessionid") and names.get("ds_user_id"))



try:
    import pyotp
except Exception:  # pragma: no cover
    pyotp = None  # type: ignore


def _dismiss_cookies(page: Page) -> None:
    candidates = (
        "button:has-text('Solo permitir lo esencial')",
        "button:has-text('Permitir solo lo esencial')",
        "button:has-text('Aceptar todas')",
        "button:has-text('Aceptar todo')",
        "button:has-text('Permitir todo')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Allow all')",
        "button:has-text('Only allow essential')",
    )
    _click_if_present(page, candidates)
    _accept_cookies_variants(page)


def _accept_cookies_variants(page: Page) -> None:
    selectors = (
        "button:has-text('Allow essential cookies')",
        "button:has-text('Only allow essential cookies')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Allow all')",
        "button:has-text('Permitir solo las cookies esenciales')",
        "button:has-text('Permitir solo las cookies necesarias')",
        "button:has-text('Permitir todas las cookies')",
        "button:has-text('Permitir todas')",
        "button:has-text('Aceptar todas')",
        "button:has-text('Aceptar todo')",
        "button:has-text('Aceptar')",
    )
    _click_if_present(page, selectors)


def _ensure_login_view(page: Page) -> None:
    current_url = page.url or ""
    if "/accounts/login" not in current_url:
        login_selectors = (
            "a[href='/accounts/login/']",
            "a:has-text('Iniciar sesión')",
            "a:has-text('Inicia sesión')",
            "a:has-text('Log in')",
            "a:has-text('Sign in')",
        )
        if _click_if_present(page, login_selectors):
            try:
                page.wait_for_load_state("domcontentloaded")
            except Exception:
                pass
    _accept_cookies_variants(page)
    try:
        page.wait_for_selector("input[name='username'], input[type='text']", timeout=20_000)
    except Exception:
        pass


def _wait_one_of(page: Page, *, timeout: int = 25_000, selectors: Sequence[str]) -> bool:
    """Espera a que aparezca alguno de los selectores indicados."""
    deadline = time.time() + (timeout / 1000)
    while time.time() < deadline:
        for sel in selectors:
            try:
                if page.locator(sel).count():
                    return True
            except Exception:
                continue
        try:
            page.wait_for_timeout(250)
        except Exception:
            pass
    return False


def _submit_login_form(page: Page, username: str, password: str) -> None:
    # Asegura estar en la vista de login
    _ensure_login_view(page)
    username_locators = (
        "input[name='username']",
        "input[name='usernameOrEmail']",
        "input[aria-label*='Phone']",
        "input[aria-label*='correo']",
        "input[type='text']",
    )
    password_locators = (
        "input[name='password']",
        "input[type='password']",
    )
    username_input = page.locator(", ".join(username_locators)).first
    password_input = page.locator(", ".join(password_locators)).first

    try:
        username_input.wait_for(state="visible", timeout=20_000)
        password_input.wait_for(state="visible", timeout=20_000)
    except Exception:
        # Reintentar navegando al login explícito
        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            username_input = page.locator(", ".join(username_locators)).first
            password_input = page.locator(", ".join(password_locators)).first
            username_input.wait_for(state="visible", timeout=10_000)
            password_input.wait_for(state="visible", timeout=10_000)
        except Exception:
            return

    # Limpiar y rellenar directamente (más robusto que tipeo lento en algunos layouts)
    try:
        username_input.fill(username)
        password_input.fill(password)
    except Exception:
        return

    submit_btn = page.locator("button[type='submit'], button:has-text('Log in'), button:has-text('Iniciar sesión')").first
    try:
        submit_btn.scroll_into_view_if_needed()
    except Exception:
        pass
    # Intentar click y siempre presionar Enter como respaldo
    try:
        human_click(submit_btn)
    except Exception:
        pass
    try:
        password_input.press("Enter")
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle")
    except Exception:
        try:
            page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass


def _after_submit_outcome(page: Page) -> str:
    """
    Devuelve: 'ok', 'challenge', 'bad_creds', 'still_login'
    """
    # Si estamos en pantalla de suspensión/captcha, intenta resolverla antes de decidir
    try:
        if "accounts/suspended" in (page.url or ""):
            _handle_suspended_flow(page)
    except Exception:
        pass

    # Validar cookies primero
    try:
        if _has_auth_cookies(page.context):
            return "ok"
    except Exception:
        pass

    current_url = page.url or ""

    # DETECCIÓN DE PANTALLA 'ONETAP' (Save Login Info)
    # Si vemos "Save your login info?" o estamos en /accounts/onetap/, es un ÉXITO.
    # Forzamos la navegación al inbox para salir de ahí.
    onetap_indicators = (
        "h2:has-text('Save your login info?')",
        "div:has-text('Save your login info?')",
        "button:has-text('Save info')",
        "button:has-text('Guardar información')",
    )
    is_onetap = False
    if "/accounts/onetap/" in current_url:
        is_onetap = True
    else:
        for sel in onetap_indicators:
            if page.locator(sel).count() > 0:
                is_onetap = True
                break
    
    if is_onetap:
        logger.info("Pantalla 'Save login info' detectada. ¡Login validado! Forzando inbox...")
        try:
            # Intentamos cerrar el diálogo primero si es posible
            _handle_save_login_prompt(page)
            # Forzamos navegación
            page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
            human_wait(1, 2)
            return "ok"
        except Exception:
            pass
        return "ok"

    # Verificar inbox
    try:
        if page.locator("a[href='/direct/inbox/'], nav[role='navigation']").count():
            return "ok"
    except Exception:
        pass
    
    if "/challenge/" in current_url:
        return "challenge"

    error_locators = "#slfErrorAlert, [data-testid='login-error-message']"
    try:
        if page.locator(error_locators).count():
            return "bad_creds"
    except Exception:
        pass

    # Solo si no es onetap y es login url explícita, fallamos
    if "/accounts/login" in current_url and not is_onetap:
        return "still_login"

    return "still_login"

def _is_in_inbox(page: Page) -> bool:
    return "/direct/inbox" in (page.url or "") or page.locator("textarea, div[contenteditable='true']").count() > 0


def _has_login_form(page: Page) -> bool:
    try:
        username_input = page.locator("input[name='username']").first
        password_input = page.locator("input[name='password']").first
        return username_input.is_visible() and password_input.is_visible()
    except Exception:
        return False


def _resolve_totp_code(
    username: str,
    totp_secret: Optional[str],
    totp_provider: Optional[TotpProvider],
) -> Optional[str]:
    if callable(totp_provider):
        try:
            code = totp_provider(username)
            if code:
                return str(code).strip()
        except Exception:
            logger.exception("totp_provider fallo para %s", username)

    if totp_secret:
        if not pyotp:
            raise RuntimeError("pyotp no esta disponible pero se solicito TOTP.")
        totp = pyotp.TOTP(totp_secret.replace(" ", ""))
        return totp.now()

    return None


def _wait_post_submit(page: Page) -> None:
    try:
        page.wait_for_load_state("networkidle")
    except Exception:
        try:
            page.wait_for_load_state("load")
        except Exception:
            pass
    human_wait(0.5, 1.2)


def _is_two_factor_prompt(page: Page) -> bool:
    url = page.url or ""
    if "/two_factor" in url:
        return True
    try:
        if page.locator(", ".join(TOTP_INPUT_SELECTORS)).count() > 0:
            return True
    except Exception:
        pass
    try:
        return page.locator("text=authentication app, text=autenticación").count() > 0
    except Exception:
        return False


def _resolve_two_factor_flow(
    page: Page,
    username: str,
    totp_secret: Optional[str],
    totp_provider: Optional[TotpProvider],
    *,
    max_attempts: int = 3,
) -> bool:
    """
    Rellena y envía el TOTP hasta max_attempts veces. Devuelve True si ya no
    estamos en la pantalla de two_factor o si detectamos login completado.
    """
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        if not _is_two_factor_prompt(page):
            return True

        filled = _handle_totp_prompt(page, username, totp_secret, totp_provider)
        human_wait(0.8, 1.6)
        if is_logged_in(page):
            return True

        current_url = page.url or ""
        if "/accounts/login" in current_url and "/two_factor" not in current_url:
            # IG nos devolvió al login (código rechazado)
            return False

        # Limpia el input antes del siguiente intento para evitar concatenar códigos
        try:
            page.locator(", ".join(TOTP_INPUT_SELECTORS)).first.fill("")
        except Exception:
            pass
        human_wait(0.6, 1.2)

    return not _is_two_factor_prompt(page)


def _handle_totp_prompt(
    page: Page,
    username: str,
    totp_secret: Optional[str],
    totp_provider: Optional[TotpProvider],
) -> bool:
    try:
        current_url = page.url or ""
        # Evitar escribir TOTP en el formulario de login simple,
        # pero permitirlo si estamos en la pantalla /two_factor.
        if "/accounts/login" in current_url and "/two_factor" not in current_url:
            return False

        selector_list = ", ".join(TOTP_INPUT_SELECTORS)
        totp_input = page.locator(selector_list).first
        visible = False
        try:
            totp_input.wait_for(state="visible", timeout=7_000)
        except Exception:
            visible = False
        else:
            try:
                visible = totp_input.is_visible()
            except Exception:
                visible = False

        # Fallback: en /two_factor/ algunos layouts no traen los atributos esperados
        if not visible and "/two_factor" in current_url:
            fallback = page.locator("input[type='text'], input[type='tel']").first
            try:
                fallback.wait_for(state="visible", timeout=5_000)
                visible = fallback.is_visible()
                if visible:
                    totp_input = fallback
            except Exception:
                visible = False

        if not visible:
            return False

        code = _resolve_totp_code(username, totp_secret, totp_provider)
        if not code:
            raise RuntimeError(
                "Instagram solicitó TOTP pero no hay totp_secret ni proveedor configurado."
            )
        logger.info("TOTP para @%s: %s", username, code)
        human_type(totp_input, code)
        try:
            totp_input.press("Enter")
        except Exception:
            pass
        _click_if_present(
            page,
            (
                "button[type='submit']",
                "button:has-text('Confirm')",
                "button:has-text('Confirmar')",
            ),
        )
        _wait_post_submit(page)
        return True
    except RuntimeError:
        raise
    except Exception:
        return False
    return False


def _handle_save_login_prompt(page: Page) -> None:
    _click_if_present(page, SAVE_LOGIN_NOT_NOW_SELECTORS)


def _handle_notification_prompt(page: Page) -> None:
    _click_if_present(page, NOTIFICATION_NOT_NOW_SELECTORS)


def _handle_light_challenge(
    page: Page,
    username: str,
    code_provider: Optional[CodeProvider],
) -> None:
    if "/challenge/" not in (page.url or ""):
        return

    logger.info("Challenge detectado para @%s. Intentando resolverlo.", username)
    _click_if_present(page, CHALLENGE_SEND_CODE_SELECTORS)

    code_locator = page.locator("input[type='text'], input[type='tel'], input[name*='code']")
    if code_locator.count():
        code_input = code_locator.first
        code = _resolve_challenge_code(code_provider)
        if code:
            human_type(code_input, code)
            _click_if_present(page, CHALLENGE_SUBMIT_SELECTORS)
        else:
            logger.info("Challenge requiere codigo manual para @%s; esperando intervencion humana.", username)

    _wait_for_challenge_resolution(page)


def _resolve_challenge_code(code_provider: Optional[CodeProvider]) -> Optional[str]:
    if callable(code_provider):
        try:
            code = code_provider()
            if code:
                return str(code).strip()
        except Exception:
            logger.exception("code_provider fallo al recuperar el codigo de challenge")
    return None


def _wait_for_challenge_resolution(page: Page, timeout_s: int = 180) -> None:
    checks = max(1, timeout_s // 5)
    for _ in range(checks):
        current_url = page.url or ""
        if "/challenge/" not in current_url:
            return
        if is_logged_in(page):
            return
        try:
            page.wait_for_timeout(4000)
        except Exception:
            human_wait(1.0, 1.8)


def _solve_captcha_with_openai(image_bytes: bytes) -> Optional[str]:
    # Asegurar que las variables de entorno del .env estén cargadas
    load_dotenv(override=False)
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
    if not api_key:
        logger.info("No hay clave de OpenAI configurada; omitiendo resolución automática de captcha.")
        return None
    try:
        from openai import OpenAI
    except Exception as exc:
        logger.warning("No se pudo importar OpenAI: %s", exc)
        return None

    b64 = base64.b64encode(image_bytes).decode("utf-8")
    client = OpenAI(api_key=api_key)
    try:
        logger.debug("Enviando captcha a OpenAI para resolver...")
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Lee el texto exacto del captcha y responde solo el código tal como aparece."},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    ],
                }
            ],
            max_tokens=10,
        )
        code = (resp.choices[0].message.content or "").strip()
        logger.debug("Respuesta OpenAI captcha: %s", code)
        return code.replace(" ", "")
    except Exception as exc:
        logger.warning("Error resolviendo captcha con OpenAI: %s", exc)
        return None


def _solve_captcha_if_present(page: Page) -> bool:
    """
    Intenta resolver el captcha visual de la pantalla "Confirma que eres una persona".
    Devuelve True si pudo enviar el código (aunque sea con OpenAI); False en caso contrario.
    """
    input_sel = (
        "input[placeholder*='imagen'], "
        "input[placeholder*='image'], "
        "input[aria-label*='imagen'], "
        "input[name*='captcha'], "
        "input[placeholder*='código de la imagen'], "
        "input[placeholder*='codigo de la imagen']"
    )
    captcha_input = page.locator(input_sel).first
    try:
        captcha_input.wait_for(state="visible", timeout=10_000)
    except Exception:
        logger.debug("No se encontró input de captcha visible.")
        return False

    # Elegir la imagen de captcha más grande (ignorando el logo pequeño)
    best_handle = None
    best_area = 0
    try:
        for handle in page.locator("img").element_handles():
            box = handle.bounding_box()
            if not box:
                continue
            area = box.get("width", 0) * box.get("height", 0)
            # Mantén cualquier imagen visible; prioriza la más grande
            alt = (handle.get_attribute("alt") or "").lower()
            if "instagram" in alt:
                continue
            if area > best_area:
                best_area = area
                best_handle = handle
    except Exception as exc:
        logger.debug("No se pudo iterar imágenes de captcha: %s", exc)

    if not best_handle:
        # Fallback: primera imagen visible
        try:
            best_handle = page.locator("img").first.element_handle()
        except Exception:
            best_handle = None
        if not best_handle:
            logger.info("Captcha detectado pero no se encontró imagen adecuada para leer.")
            return False

    try:
        img_bytes = best_handle.screenshot(type="png")
    except Exception as exc:
        logger.warning("No se pudo capturar imagen de captcha: %s", exc)
        return False

    code = _solve_captcha_with_openai(img_bytes)
    if not code:
        logger.info("Captcha detectado pero no se pudo resolver automáticamente (clave OpenAI faltante o error).")
        return False

    try:
        captcha_input.fill(code)
        human_wait(0.2, 0.5)
        # Click en siguiente o Enter
        if not _click_if_present(
            page,
            (
                "button:has-text('Siguiente')",
                "button:has-text('Next')",
                "button[type='submit']",
            ),
        ):
            try:
                captcha_input.press("Enter")
            except Exception:
                pass
        human_wait(1.0, 1.5)
        logger.info("Captcha completado automáticamente.")
        return True
    except Exception as exc:
        logger.warning("No se pudo enviar el captcha: %s", exc)
        return False


def _handle_suspended_flow(page: Page) -> bool:
    """
    Maneja la pantalla de "Confirma que eres una persona" con captcha.
    Devuelve True si se gestionó (click y/o captcha enviado).
    """
    current_url = page.url or ""
    if "accounts/suspended" not in current_url:
        return False

    logger.info("Pantalla de suspensión/captcha detectada. Intentando continuar.")
    _click_if_present(page, ("button:has-text('Continuar')", "button:has-text('Continue')"))
    human_wait(0.5, 1.0)
    solved = _solve_captcha_if_present(page)
    if solved:
        logger.info("Captcha enviado automáticamente.")
    return True


def _capture_login_failure(page: Page, username: str) -> None:
    try:
        LOGIN_FAILED_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = LOGIN_FAILED_DIR / f"{username}_failed.png"
        page.screenshot(path=str(screenshot_path), full_page=True)
        logger.info("Screenshot de login fallido guardado en %s", screenshot_path)
    except Exception as exc:
        logger.warning("No se pudo guardar screenshot de login fallido (%s): %s", username, exc)


def _click_if_present(page: Page, selectors: Sequence[str]) -> bool:
    for sel in selectors:
        try:
            locator = page.locator(sel)
            if locator.count():
                try:
                    locator.first.wait_for(state="visible", timeout=5_000)
                except Exception:
                    pass
                human_click(page, sel)
                human_wait(0.2, 0.5)
                return True
        except Exception:
            continue
    return False


def get_login_errors(page: Page) -> List[str]:
    selectors: Sequence[str] = (
        "[role='alert']",
        "#slfErrorAlert",
        "div:has-text('contraseña')",
        "div:has-text('password')",
        "div:has-text('vuelve a intentarlo')",
        "div:has-text('try again')",
        "div:has-text('help us confirm')",
        "div:has-text('ayúdanos a confirmar')",
    )
    messages: List[str] = []
    for sel in selectors:
        try:
            if page.locator(sel).count():
                text = page.locator(sel).first.inner_text().strip()
                if text:
                    messages.append(text)
        except Exception:
            continue
    return messages


def is_logged_in(page: Page) -> bool:
    url = page.url or ""
    if "accounts/login" in url or "/challenge/" in url:
        return False

    try:
        if _has_auth_cookies(page.context):
            return True
    except Exception:
        pass

    selectors: Sequence[str] = (
        "a[href='/direct/inbox/']",
        "nav[role='navigation']",
        "svg[aria-label='Home']",
        "svg[aria-label='Inicio']",
    )
    for sel in selectors:
        try:
            locator = page.locator(sel)
            if locator.count():
                first = locator.first
                try:
                    if first.is_visible():
                        return True
                except Exception:
                    return True
        except Exception:
            continue

    return False


def human_login(
    page: Page,
    username: str,
    password: str,
    *,
    totp_secret: Optional[str] = None,
    totp_provider: Optional[TotpProvider] = None,
    code_provider: Optional[CodeProvider] = None,
) -> bool:
    logger.info("Iniciando login humano para @%s", username)

    try:
        page.goto(INSTAGRAM_URL, wait_until="domcontentloaded")
    except Exception:
        page.goto(INSTAGRAM_URL)

    human_wait(0.3, 0.6)
    _accept_cookies_variants(page)
    _ensure_login_view(page)
    _dismiss_cookies(page)

    if not _has_login_form(page):
        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
        except Exception:
            page.goto(LOGIN_URL)
        human_wait(0.2, 0.4)
        _accept_cookies_variants(page)
        _ensure_login_view(page)
        _dismiss_cookies(page)

    def _attempt_login() -> str:
        _submit_login_form(page, username, password)
        # Esperamos un momento para que se seteen cookies (critico)
        _wait_post_submit(page)
        human_wait(2, 3)
        two_factor_ok = _resolve_two_factor_flow(
            page, username, totp_secret, totp_provider, max_attempts=3
        )
        if not two_factor_ok and "/accounts/login" in (page.url or ""):
            return "still_login"
        _handle_suspended_flow(page)

        # FUERZA BRUTA: Navegación forzada al Inbox como solicitaste
        logger.info("🚀 FORZANDO NAVEGACIÓN A INBOX: https://www.instagram.com/direct/inbox/?next=%2F")
        try:
            page.goto("https://www.instagram.com/direct/inbox/?next=%2F", wait_until="domcontentloaded", timeout=60000)
            human_wait(3, 5)
        except Exception as e:
            logger.warning(f"Error en navegación forzada (pero continuamos): {e}")
        # Si tras la navegación seguimos en prompt de TOTP, reintenta rellenarlo
        _resolve_two_factor_flow(page, username, totp_secret, totp_provider, max_attempts=2)
        _handle_suspended_flow(page)

        # Verificamos directamente si funcionó
        if is_logged_in(page):
            return "ok"
        
        # Si aun no estamos logueados, miramos si hay challenge o errores
        _handle_light_challenge(page, username, code_provider)
        # Reintenta TOTP en caso de que IG siga mostrando el prompt
        _resolve_two_factor_flow(page, username, totp_secret, totp_provider, max_attempts=2)
        _handle_suspended_flow(page)

        if "/challenge/" in (page.url or ""):
             _wait_for_challenge_resolution(page)
             if is_logged_in(page): return "ok"

        return _after_submit_outcome(page)

    outcome = _attempt_login()
    # Si quedamos en la pantalla de two_factor, intentar rellenar TOTP una vez más
    if "/two_factor" in (page.url or ""):
        _resolve_two_factor_flow(page, username, totp_secret, totp_provider, max_attempts=2)
        if is_logged_in(page):
            outcome = "ok"

    # Último intento de resolver captcha/suspensión antes de evaluar outcome
    _handle_suspended_flow(page)

    # SALIDA DE EMERGENCIA: Si quedamos en onetap, estamos logueados.
    if "/accounts/onetap/" in (page.url or ""):
        logger.info("Atrapado en /onetap/. Forzando salto a Inbox...")
        try:
            page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
            human_wait(1, 1.5)
        except Exception:
            pass
        outcome = "ok"

    if outcome == "still_login":
        # Doble chequeo por si la URL cambio justo despues
        if "/accounts/onetap/" in (page.url or ""):
             logger.info("Detectado /onetap/ tras intento fallido. Asumiendo éxito.")
             try: 
                 page.goto("https://www.instagram.com/direct/inbox/") 
             except: pass
             outcome = "ok"
        else:
            try:
                page.reload(wait_until="domcontentloaded")
            except Exception:
                page.reload()
            human_wait(0.3, 0.5)
            _ensure_login_view(page)
            outcome = _attempt_login()

    # Triple chequeo final
    if outcome == "still_login" and "/accounts/onetap/" in (page.url or ""):
        outcome = "ok"
        try: page.goto("https://www.instagram.com/direct/inbox/") 
        except: pass

    if outcome == "bad_creds":
        _capture_login_failure(page, username)
        raise RuntimeError("Instagram rechazó las credenciales proporcionadas.")

    if outcome == "still_login":
        _capture_login_failure(page, username)
        raise RuntimeError("Instagram devolvió nuevamente el formulario de login.")

    # Eliminada navegacion final a BASE_URL para evitar perder el estado del Inbox
    # NO NAVEGAR MÁS. Si llegamos aquí, ya estamos en Inbox (por la fuerza bruta anterior)
    # o donde sea que el login nos dejó. Verificar y salir.
    # try:
    #     page.goto(BASE_URL, wait_until="domcontentloaded")
    #     _wait_post_submit(page)
    # except Exception:
    #     pass

    success = is_logged_in(page)
    if not success and outcome != "challenge":
        _capture_login_failure(page, username)
    logger.info("Login humano para @%s %s", username, "OK" if success else "FALLO")
    return success

def ensure_logged_in(account: dict):
    """
    Compatibilidad hacia atras: delega en auth.persistent_login.ensure_logged_in.
    """
    from src.auth.persistent_login import ensure_logged_in as ensure_persistent_login

    return ensure_persistent_login(account)
