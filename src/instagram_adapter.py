from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from playwright.async_api import BrowserContext, Page
from dotenv import load_dotenv

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
TraceFn = Callable[[str], None]

LOGIN_FAILED_DIR = Path(BASE_PROFILES) / "login_failed_screenshots"


def _keystroke_delay_ms(base: float = 0.07, jitter: float = 0.03) -> int:
    delay = max(0.01, random.gauss(base, jitter))
    return int(delay * 1000)


async def _human_wait(min_s: float = 0.2, max_s: float = 1.0) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


def _trace(trace: Optional[TraceFn], message: str) -> None:
    if callable(trace):
        try:
            trace(message)
        except Exception:
            pass


def _resolve_locator(target, selector: Optional[str]):
    if selector is not None:
        if not hasattr(target, "locator"):
            raise TypeError("human_click: cuando se pasa selector, target debe exponer .locator().")
        locator = target.locator(selector)
    else:
        locator = target
    try:
        return locator.first
    except Exception:
        return locator


async def _human_click(target, selector: Optional[str] = None) -> None:
    locator = _resolve_locator(target, selector)
    try:
        await locator.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    try:
        await locator.hover()
    except Exception:
        pass
    await _human_wait(0.05, 0.4)
    await locator.click()
    await _human_wait(0.1, 0.6)


async def _human_type(locator, text: str, clear_first: bool = True) -> None:
    if clear_first:
        try:
            await locator.click()
        except Exception:
            pass
        try:
            await locator.fill("")
        except Exception:
            pass
        await _human_wait(0.05, 0.2)

    for ch in text:
        await locator.type(ch, delay=_keystroke_delay_ms())
        if random.random() < 0.06:
            await _human_wait(0.08, 0.3)

    await _human_wait(0.1, 0.4)


async def _has_auth_cookies(context: BrowserContext) -> bool:
    try:
        cookies = await context.cookies(["https://www.instagram.com/"])
    except TypeError:
        cookies = await context.cookies("https://www.instagram.com/")
    names = {c.get("name"): c.get("value") for c in cookies}
    return bool(names.get("sessionid") and names.get("ds_user_id"))



try:
    import pyotp
except Exception:  # pragma: no cover
    pyotp = None  # type: ignore


async def _dismiss_cookies(page: Page) -> None:
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
    await _click_if_present(page, candidates)
    await _accept_cookies_variants(page)


async def _accept_cookies_variants(page: Page) -> None:
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
    await _click_if_present(page, selectors)


async def _ensure_login_view(page: Page) -> None:
    current_url = page.url or ""
    if "/accounts/login" not in current_url:
        login_selectors = (
            "a[href='/accounts/login/']",
            "a:has-text('Iniciar sesión')",
            "a:has-text('Inicia sesión')",
            "a:has-text('Log in')",
            "a:has-text('Sign in')",
        )
        if await _click_if_present(page, login_selectors):
            try:
                await page.wait_for_load_state("domcontentloaded")
            except Exception:
                pass
    await _accept_cookies_variants(page)
    try:
        await page.wait_for_selector("input[name='username'], input[type='text']", timeout=20_000)
    except Exception:
        pass


async def _wait_one_of(page: Page, *, timeout: int = 25_000, selectors: Sequence[str]) -> bool:
    """Espera a que aparezca alguno de los selectores indicados."""
    deadline = time.time() + (timeout / 1000)
    while time.time() < deadline:
        for sel in selectors:
            try:
                if await page.locator(sel).count():
                    return True
            except Exception:
                continue
        try:
            await page.wait_for_timeout(250)
        except Exception:
            pass
    return False


async def _submit_login_form(
    page: Page,
    username: str,
    password: str,
    trace: Optional[TraceFn] = None,
) -> None:
    # Asegura estar en la vista de login
    _trace(trace, "Open https://www.instagram.com/accounts/login/")
    await _ensure_login_view(page)
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
        await username_input.wait_for(state="visible", timeout=20_000)
        await password_input.wait_for(state="visible", timeout=20_000)
    except Exception:
        # Reintentar navegando al login explícito
        try:
            await page.goto(LOGIN_URL, wait_until="domcontentloaded")
            username_input = page.locator(", ".join(username_locators)).first
            password_input = page.locator(", ".join(password_locators)).first
            await username_input.wait_for(state="visible", timeout=10_000)
            await password_input.wait_for(state="visible", timeout=10_000)
        except Exception:
            return

    # Limpiar y rellenar directamente (más robusto que tipeo lento en algunos layouts)
    try:
        _trace(trace, "Fill username")
        await username_input.fill(username)
        _trace(trace, "Fill password")
        await password_input.fill(password)
    except Exception:
        return

    submit_btn = page.locator("button[type='submit'], button:has-text('Log in'), button:has-text('Iniciar sesión')").first
    try:
        await submit_btn.scroll_into_view_if_needed()
    except Exception:
        pass
    _trace(trace, "Submit login")
    # Intentar click y siempre presionar Enter como respaldo
    try:
        await _human_click(submit_btn)
    except Exception:
        pass
    try:
        await password_input.press("Enter")
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle")
    except Exception:
        try:
            await page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
    _trace(trace, "Wait for navigation / DOM ready")


async def _after_submit_outcome(page: Page) -> str:
    """
    Devuelve: 'ok', 'challenge', 'bad_creds', 'still_login'
    """
    # Si estamos en pantalla de suspensión/captcha, intenta resolverla antes de decidir
    try:
        if "accounts/suspended" in (page.url or ""):
            await _handle_suspended_flow(page)
    except Exception:
        pass

    # Validar cookies primero
    try:
        if await _has_auth_cookies(page.context):
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
            if await page.locator(sel).count() > 0:
                is_onetap = True
                break
    
    if is_onetap:
        logger.info("Pantalla 'Save login info' detectada. ¡Login validado! Forzando inbox...")
        try:
            # Intentamos cerrar el diálogo primero si es posible
            await _handle_save_login_prompt(page)
            # Forzamos navegación
            await page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
            await _human_wait(1, 2)
            return "ok"
        except Exception:
            pass
        return "ok"

    # Verificar inbox
    try:
        if await page.locator("a[href='/direct/inbox/'], nav[role='navigation']").count():
            return "ok"
    except Exception:
        pass
    
    if "/challenge/" in current_url:
        return "challenge"

    error_locators = "#slfErrorAlert, [data-testid='login-error-message']"
    try:
        if await page.locator(error_locators).count():
            return "bad_creds"
    except Exception:
        pass

    # Solo si no es onetap y es login url explícita, fallamos
    if "/accounts/login" in current_url and not is_onetap:
        return "still_login"

    return "still_login"

async def _is_in_inbox(page: Page) -> bool:
    if "/direct/inbox" in (page.url or ""):
        return True
    try:
        return await page.locator("textarea, div[contenteditable='true']").count() > 0
    except Exception:
        return False


async def _has_login_form(page: Page) -> bool:
    try:
        username_input = page.locator("input[name='username']").first
        password_input = page.locator("input[name='password']").first
        return await username_input.is_visible() and await password_input.is_visible()
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


async def _wait_post_submit(page: Page) -> None:
    try:
        await page.wait_for_load_state("networkidle")
    except Exception:
        try:
            await page.wait_for_load_state("load")
        except Exception:
            pass
    await _human_wait(0.5, 1.2)


async def _is_two_factor_prompt(page: Page) -> bool:
    url = page.url or ""
    if "/two_factor" in url:
        return True
    try:
        if await page.locator(", ".join(TOTP_INPUT_SELECTORS)).count() > 0:
            return True
    except Exception:
        pass
    try:
        return await page.locator("text=authentication app, text=autenticación").count() > 0
    except Exception:
        return False


async def _resolve_two_factor_flow(
    page: Page,
    username: str,
    totp_secret: Optional[str],
    totp_provider: Optional[TotpProvider],
    *,
    trace: Optional[TraceFn] = None,
    max_attempts: int = 3,
) -> bool:
    """
    Rellena y envía el TOTP hasta max_attempts veces. Devuelve True si ya no
    estamos en la pantalla de two_factor o si detectamos login completado.
    """
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        if not await _is_two_factor_prompt(page):
            return True

        _trace(trace, "Detect 2FA prompt")
        if not totp_secret and not callable(totp_provider):
            _trace(trace, "2FA required but no TOTP provided")
            raise RuntimeError("2FA required but no TOTP provided")

        filled = await _handle_totp_prompt(page, username, totp_secret, totp_provider, trace=trace)
        await _human_wait(0.8, 1.6)
        if await is_logged_in(page):
            return True

        current_url = page.url or ""
        if "/accounts/login" in current_url and "/two_factor" not in current_url:
            # IG nos devolvió al login (código rechazado)
            return False

        # Limpia el input antes del siguiente intento para evitar concatenar códigos
        try:
            await page.locator(", ".join(TOTP_INPUT_SELECTORS)).first.fill("")
        except Exception:
            pass
        await _human_wait(0.6, 1.2)

    return not await _is_two_factor_prompt(page)


async def _handle_totp_prompt(
    page: Page,
    username: str,
    totp_secret: Optional[str],
    totp_provider: Optional[TotpProvider],
    trace: Optional[TraceFn] = None,
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
            await totp_input.wait_for(state="visible", timeout=7_000)
        except Exception:
            visible = False
        else:
            try:
                visible = await totp_input.is_visible()
            except Exception:
                visible = False

        # Fallback: en /two_factor/ algunos layouts no traen los atributos esperados
        if not visible and "/two_factor" in current_url:
            fallback = page.locator("input[type='text'], input[type='tel']").first
            try:
                await fallback.wait_for(state="visible", timeout=5_000)
                visible = await fallback.is_visible()
                if visible:
                    totp_input = fallback
            except Exception:
                visible = False

        if not visible:
            return False

        code = _resolve_totp_code(username, totp_secret, totp_provider)
        if not code:
            raise RuntimeError(
                "2FA required but no TOTP provided"
            )
        _trace(trace, "Fill 2FA TOTP code")
        await _human_type(totp_input, code)
        try:
            await totp_input.press("Enter")
        except Exception:
            pass
        _trace(trace, "Submit 2FA code")
        await _click_if_present(
            page,
            (
                "button[type='submit']",
                "button:has-text('Confirm')",
                "button:has-text('Confirmar')",
            ),
        )
        await _wait_post_submit(page)
        return True
    except RuntimeError:
        raise
    except Exception:
        return False
    return False


async def _handle_save_login_prompt(page: Page) -> None:
    await _click_if_present(page, SAVE_LOGIN_NOT_NOW_SELECTORS)


async def _handle_notification_prompt(page: Page) -> None:
    await _click_if_present(page, NOTIFICATION_NOT_NOW_SELECTORS)


async def _handle_light_challenge(
    page: Page,
    username: str,
    code_provider: Optional[CodeProvider],
) -> None:
    if "/challenge/" not in (page.url or ""):
        return

    logger.info("Challenge detectado para @%s. Requiere verificacion manual.", username)
    return


def _resolve_challenge_code(code_provider: Optional[CodeProvider]) -> Optional[str]:
    if callable(code_provider):
        try:
            code = code_provider()
            if code:
                return str(code).strip()
        except Exception:
            logger.exception("code_provider fallo al recuperar el codigo de challenge")
    return None


async def _wait_for_challenge_resolution(page: Page, timeout_s: int = 180) -> None:
    checks = max(1, timeout_s // 5)
    for _ in range(checks):
        current_url = page.url or ""
        if "/challenge/" not in current_url:
            return
        if await is_logged_in(page):
            return
        try:
            await page.wait_for_timeout(4000)
        except Exception:
            await _human_wait(1.0, 1.8)


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


async def _solve_captcha_if_present(page: Page) -> bool:
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
        await captcha_input.wait_for(state="visible", timeout=10_000)
    except Exception:
        logger.debug("No se encontró input de captcha visible.")
        return False

    # Elegir la imagen de captcha más grande (ignorando el logo pequeño)
    best_handle = None
    best_area = 0
    try:
        for handle in await page.locator("img").element_handles():
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
            best_handle = await page.locator("img").first.element_handle()
        except Exception:
            best_handle = None
        if not best_handle:
            logger.info("Captcha detectado pero no se encontró imagen adecuada para leer.")
            return False

    try:
        img_bytes = await best_handle.screenshot(type="png")
    except Exception as exc:
        logger.warning("No se pudo capturar imagen de captcha: %s", exc)
        return False

    code = _solve_captcha_with_openai(img_bytes)
    if not code:
        logger.info("Captcha detectado pero no se pudo resolver automáticamente (clave OpenAI faltante o error).")
        return False

    try:
        await captcha_input.fill(code)
        await _human_wait(0.2, 0.5)
        # Click en siguiente o Enter
        if not await _click_if_present(
            page,
            (
                "button:has-text('Siguiente')",
                "button:has-text('Next')",
                "button[type='submit']",
            ),
        ):
            try:
                await captcha_input.press("Enter")
            except Exception:
                pass
        await _human_wait(1.0, 1.5)
        logger.info("Captcha completado automáticamente.")
        return True
    except Exception as exc:
        logger.warning("No se pudo enviar el captcha: %s", exc)
        return False


async def _handle_suspended_flow(page: Page) -> bool:
    """
    Maneja la pantalla de "Confirma que eres una persona" con captcha.
    Devuelve True si se gestionó (click y/o captcha enviado).
    """
    current_url = page.url or ""
    if "accounts/suspended" not in current_url:
        return False

    logger.info("Pantalla de suspensión/captcha detectada. Intentando continuar.")
    await _click_if_present(page, ("button:has-text('Continuar')", "button:has-text('Continue')"))
    await _human_wait(0.5, 1.0)
    solved = await _solve_captcha_if_present(page)
    if solved:
        logger.info("Captcha enviado automáticamente.")
    return True


async def _capture_login_failure(page: Page, username: str) -> None:
    try:
        LOGIN_FAILED_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = LOGIN_FAILED_DIR / f"{username}_failed.png"
        await page.screenshot(path=str(screenshot_path), full_page=True)
        logger.info("Screenshot de login fallido guardado en %s", screenshot_path)
    except Exception as exc:
        logger.warning("No se pudo guardar screenshot de login fallido (%s): %s", username, exc)


async def _click_if_present(page: Page, selectors: Sequence[str]) -> bool:
    for sel in selectors:
        try:
            locator = page.locator(sel)
            if await locator.count():
                try:
                    await locator.first.wait_for(state="visible", timeout=5_000)
                except Exception:
                    pass
                await _human_click(page, sel)
                await _human_wait(0.2, 0.5)
                return True
        except Exception:
            continue
    return False


async def get_login_errors(page: Page) -> List[str]:
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
            if await page.locator(sel).count():
                text = (await page.locator(sel).first.inner_text()).strip()
                if text:
                    messages.append(text)
        except Exception:
            continue
    return messages


async def check_logged_in(page: Page) -> tuple[bool, str]:
    url = page.url or ""
    if "accounts/login" in url or "/challenge/" in url:
        return False, "url_login_or_challenge"

    try:
        if await _has_auth_cookies(page.context):
            return True, "auth_cookies"
    except Exception:
        pass

    selectors: Sequence[tuple[str, str]] = (
        ("inbox_link", "a[href='/direct/inbox/']"),
        ("nav", "nav[role='navigation']"),
        ("home_icon", "svg[aria-label='Home']"),
        ("inicio_icon", "svg[aria-label='Inicio']"),
    )
    for name, sel in selectors:
        try:
            locator = page.locator(sel)
            if await locator.count():
                try:
                    await locator.first.wait_for(state="visible", timeout=2_000)
                except Exception:
                    pass
                try:
                    if await locator.first.is_visible():
                        return True, f"selector:{name}"
                except Exception:
                    return True, f"selector:{name}"
        except Exception:
            continue

    if "instagram.com" not in url:
        return False, "url_not_instagram"

    return False, "selectors_miss=inbox_link|nav|home_icon|inicio_icon"


async def is_logged_in(page: Page) -> bool:
    ok, _reason = await check_logged_in(page)
    return ok


async def human_login(
    page: Page,
    username: str,
    password: str,
    *,
    totp_secret: Optional[str] = None,
    totp_provider: Optional[TotpProvider] = None,
    code_provider: Optional[CodeProvider] = None,
    trace: Optional[TraceFn] = None,
    retry_on_still_login: bool = True,
) -> bool:
    logger.info("Iniciando login humano para @%s", username)

    try:
        _trace(trace, "Open https://www.instagram.com/")
        await page.goto(INSTAGRAM_URL, wait_until="domcontentloaded")
    except Exception:
        await page.goto(INSTAGRAM_URL)

    await _human_wait(0.3, 0.6)
    await _accept_cookies_variants(page)
    await _ensure_login_view(page)
    await _dismiss_cookies(page)

    if not await _has_login_form(page):
        try:
            _trace(trace, "Open https://www.instagram.com/accounts/login/")
            await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        except Exception:
            await page.goto(LOGIN_URL)
        await _human_wait(0.2, 0.4)
        await _accept_cookies_variants(page)
        await _ensure_login_view(page)
        await _dismiss_cookies(page)

    async def _attempt_login() -> str:
        await _submit_login_form(page, username, password, trace=trace)
        # Esperamos un momento para que se seteen cookies (critico)
        await _wait_post_submit(page)
        await _human_wait(2, 3)
        two_factor_ok = await _resolve_two_factor_flow(
            page,
            username,
            totp_secret,
            totp_provider,
            trace=trace,
            max_attempts=3,
        )
        if not two_factor_ok and "/accounts/login" in (page.url or ""):
            return "still_login"
        await _handle_suspended_flow(page)

        # FUERZA BRUTA: Navegación forzada al Inbox como solicitaste
        _trace(trace, "Open https://www.instagram.com/direct/inbox/?next=%2F")
        try:
            await page.goto("https://www.instagram.com/direct/inbox/?next=%2F", wait_until="domcontentloaded", timeout=60000)
            await _human_wait(3, 5)
        except Exception as e:
            logger.warning(f"Error en navegación forzada (pero continuamos): {e}")
        # Si tras la navegación seguimos en prompt de TOTP, reintenta rellenarlo
        await _resolve_two_factor_flow(
            page,
            username,
            totp_secret,
            totp_provider,
            trace=trace,
            max_attempts=2,
        )
        await _handle_suspended_flow(page)

        # Verificamos directamente si funcionó
        if await is_logged_in(page):
            return "ok"
        
        # Si aun no estamos logueados, miramos si hay challenge o errores
        await _handle_light_challenge(page, username, code_provider)
        # Reintenta TOTP en caso de que IG siga mostrando el prompt
        await _resolve_two_factor_flow(
            page,
            username,
            totp_secret,
            totp_provider,
            trace=trace,
            max_attempts=2,
        )
        await _handle_suspended_flow(page)

        if "/challenge/" in (page.url or ""):
             return "challenge"

        return await _after_submit_outcome(page)

    outcome = await _attempt_login()
    # Si quedamos en la pantalla de two_factor, intentar rellenar TOTP una vez más
    if "/two_factor" in (page.url or ""):
        await _resolve_two_factor_flow(
            page,
            username,
            totp_secret,
            totp_provider,
            trace=trace,
            max_attempts=2,
        )
        if await is_logged_in(page):
            outcome = "ok"

    # Último intento de resolver captcha/suspensión antes de evaluar outcome
    await _handle_suspended_flow(page)

    # SALIDA DE EMERGENCIA: Si quedamos en onetap, estamos logueados.
    if "/accounts/onetap/" in (page.url or ""):
        logger.info("Atrapado en /onetap/. Forzando salto a Inbox...")
        try:
            await page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
            await _human_wait(1, 1.5)
        except Exception:
            pass
        outcome = "ok"

    if outcome == "still_login" and retry_on_still_login:
        # Doble chequeo por si la URL cambio justo despues
        if "/accounts/onetap/" in (page.url or ""):
             logger.info("Detectado /onetap/ tras intento fallido. Asumiendo éxito.")
             try: 
                 await page.goto("https://www.instagram.com/direct/inbox/") 
             except: pass
             outcome = "ok"
        else:
            try:
                await page.reload(wait_until="domcontentloaded")
            except Exception:
                await page.reload()
            await _human_wait(0.3, 0.5)
            await _ensure_login_view(page)
            outcome = await _attempt_login()

    # Triple chequeo final
    if outcome == "still_login" and "/accounts/onetap/" in (page.url or ""):
        outcome = "ok"
        try: await page.goto("https://www.instagram.com/direct/inbox/") 
        except: pass

    if outcome == "challenge":
        _trace(trace, "FAIL reason=challenge_required")

    if outcome == "bad_creds":
        _trace(trace, "FAIL reason=bad_credentials")
        await _capture_login_failure(page, username)
        raise RuntimeError("Instagram rechazó las credenciales proporcionadas.")

    if outcome == "still_login":
        _trace(trace, "FAIL reason=login_form_returned")
        await _capture_login_failure(page, username)
        raise RuntimeError("Instagram devolvió nuevamente el formulario de login.")

    # Eliminada navegacion final a BASE_URL para evitar perder el estado del Inbox
    # NO NAVEGAR MÁS. Si llegamos aquí, ya estamos en Inbox (por la fuerza bruta anterior)
    # o donde sea que el login nos dejó. Verificar y salir.
    # try:
    #     page.goto(BASE_URL, wait_until="domcontentloaded")
    #     _wait_post_submit(page)
    # except Exception:
    #     pass

    success = await is_logged_in(page)
    if not success and outcome != "challenge":
        await _capture_login_failure(page, username)
    if success:
        _trace(trace, "Login OK (session valid)")
    logger.info("Login humano para @%s %s", username, "OK" if success else "FALLO")
    return success

async def ensure_logged_in_async(account: dict):
    """
    Compatibilidad async: delega en auth.persistent_login.ensure_logged_in_async.
    """
    from src.auth.persistent_login import ensure_logged_in_async as ensure_persistent_login

    return await ensure_persistent_login(account)


def ensure_logged_in(account: dict):
    """
    Wrapper sync para compatibilidad: delega en auth.persistent_login.ensure_logged_in.
    """
    from src.auth.persistent_login import ensure_logged_in as ensure_persistent_login

    return ensure_persistent_login(account)
