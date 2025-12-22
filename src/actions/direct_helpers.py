from __future__ import annotations

import os
import re
import time
from typing import Optional

from playwright.async_api import Frame, Locator, Page, TimeoutError as PWTimeoutError

from src.humanizer import random_wait

# Carpeta de screenshots de debug
DEBUG_DIR = os.path.join("profiles", "dm_debug")
os.makedirs(DEBUG_DIR, exist_ok=True)

_MODAL_READY = False


def _mark_modal_ready(val: bool) -> None:
    global _MODAL_READY
    _MODAL_READY = val


def _is_modal_ready() -> bool:
    return _MODAL_READY

DIALOG_SEARCH_CANDIDATES = [
    '[role="dialog"] input[name="queryBox"]',
    '[role="dialog"] input[placeholder*="Search"]',
    '[role="dialog"] input[placeholder*="Buscar"]',
    '[role="dialog"] [role="textbox"]',
    '[role="dialog"] input[type="text"]',
    '[role="dialog"] [contenteditable="true"][role="textbox"]',
    'input[name="queryBox"]',
    'input[placeholder*="Search"]',
    'input[placeholder*="Buscar"]',
]

SELECTORS = {
    "inbox_icon": (
        '[aria-label="Messenger"], [aria-label="Mensajes"], '
        '[data-testid="direct-inbox"], a[href="/direct/inbox/"], '
        'svg[aria-label*="Messenger"], svg[aria-label*="Mensajes"]'
    ),
    "new_message": (
        '[aria-label="New message"], [aria-label="Enviar mensaje"], '
        "[data-testid='new-message'], button:has-text('New message'), button:has-text('Enviar mensaje')"
    ),
    "compose_icon": (
        'svg[aria-label="New message"], '
        '[data-testid="new-message"], '
        'a[href*="/direct/new"]'
    ),
    "send_message_cta": (
        'main button:has-text("Send message"), '
        'main [role="button"]:has-text("Send message"), '
        'button:has-text("Send message"), '
        'main button:has-text("Enviar mensaje"), '
        'main [role="button"]:has-text("Enviar mensaje"), '
        'button:has-text("Enviar mensaje")'
    ),
    "search_input": (
        'input[placeholder*="Search"], input[placeholder*="Buscar"], '
        '[role="searchbox"], input[name="queryBox"]'
    ),
    "dialog": '[role="dialog"]',
    "dialog_search": ", ".join(DIALOG_SEARCH_CANDIDATES),
    "dialog_result": (
        '[role="dialog"] [role="listitem"], '
        '[role="dialog"] [role="row"], '
        '[role="dialog"] [role="button"], '
        '[role="dialog"] li, '
        '[role="dialog"] [data-testid*="user"], '
        '[role="dialog"] a[href*="/"], '
        '[role="dialog"] article'
    ),
    "dialog_result_checkbox": (
        '[role="dialog"] [role="button"] input[type="checkbox"], '
        '[role="dialog"] [role="button"] [role="checkbox"], '
        '[role="dialog"] [role="button"] [aria-checked]'
    ),
    "dialog_submit": (
        '[role="dialog"] button:has-text("Next"), '
        '[role="dialog"] button:has-text("Chat"), '
        '[role="dialog"] button:has-text("Message"), '
        'button:has-text("Next"), button:has-text("Chat"), button:has-text("Message")'
    ),
    "search_results_container": (
        '[role="dialog"] [role="presentation"], '
        '[role="dialog"] [role="listbox"], '
        '[role="dialog"] [data-testid*="user"]'
    ),
    "search_result_item": (
        '[role="dialog"] [role="button"], '
        '[role="dialog"] a[href*="/"], '
        '[role="dialog"] div[tabindex="0"]'
    ),
    "next_button": 'button:has-text("Next"), button:has-text("Siguiente")',
    "composer": (
        '[role="textbox"][contenteditable="true"], '
        'div[contenteditable="true"][role="textbox"], '
        "textarea"
    ),
    "send_button": '[aria-label="Send"], [data-testid="send"], [type="submit"]',
    "thread_header": '[data-testid="chat-header"], header:has([href*="/direct/t"])',
    "own_bubble": '[data-testid="message-bubble"] [data-testid="own"]',
    "toast_error": '[role="alert"], [data-testid="toast"]',
    "modal_dialog": '[role="dialog"]',
}

DIALOG_FALLBACK_SELECTORS = [
    SELECTORS["dialog"],
    "[aria-modal='true']",
    "div[aria-modal='true']",
    "section[aria-modal='true']",
    "[aria-label*='New message']",
    "[aria-label*='Nuevo mensaje']",
]
DIRECT_READY_QUERY = (
    f'{SELECTORS["search_input"]}, {SELECTORS["modal_dialog"]}, {SELECTORS["thread_header"]}'
)


async def _snap(page: Page, name: str) -> Optional[str]:
    filename = f"{name}_{int(time.time())}.png"
    path = os.path.join(DEBUG_DIR, filename)
    try:
        await page.screenshot(path=path, full_page=True)
        return path
    except Exception:
        return None


async def _snap_step(page: Page, label: str) -> None:
    await _snap(page, f"step_{label}")


async def _log_search_candidates(page: Page) -> None:
    url = page.url or "about:blank"
    print(f"[direct] search input missing. url={url}")
    raw_selectors = [chunk.strip() for chunk in SELECTORS["search_input"].split(",") if chunk.strip()]
    for sel in raw_selectors:
        try:
            count = await page.locator(sel).count()
            print(f"[direct]   {sel} -> {count}")
        except Exception as exc:
            print(f"[direct]   {sel} -> error: {exc}")


async def dismiss_popups(page: Page):
    """Cierra modales comunes: Save login info, Turn on notifications, etc. (ES/EN)."""
    candidates = [
        'button:has-text("Not now")',
        'button:has-text("Not Now")',
        'button:has-text("Ahora no")',
        '[role="dialog"] button:has-text("Not now")',
        '[role="dialog"] button:has-text("Not Now")',
        'button:has-text("Allow")',
        'button:has-text("Don\'t Allow")',
        'button:has-text("Dont Allow")',
        'button:has-text("Enable")',
        'button:has-text("Cancelar")',
        'button:has-text("No gracias")',
        'button:has-text("Más tarde")',
        'button:has-text("Remind me later")',
    ]

    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                await random_wait(250, 600)
        except Exception:
            pass


async def goto_direct_new(page: Page) -> None:
    """Fuerza llegar al diálogo de 'nuevo mensaje' en inglés y espera carga DOM."""
    # usar hl=en para estabilizar labels
    target = "https://www.instagram.com/direct/new/?hl=en"
    await page.goto(target, wait_until="domcontentloaded")
    await random_wait(400, 900)


async def goto_direct_inbox(page: Page) -> None:
    target = "https://www.instagram.com/direct/inbox/?hl=en"
    await page.goto(target, wait_until="domcontentloaded")
    await random_wait(300, 700)


async def _wait_for_direct_surface(page: Page, timeout_ms: int) -> bool:
    try:
        await page.wait_for_selector(DIRECT_READY_QUERY, timeout=timeout_ms)
        return True
    except Exception:
        return False


async def _wait_for_search_input(page: Page, timeout_ms: int) -> bool:
    try:
        await page.wait_for_selector(SELECTORS["search_input"], timeout=timeout_ms)
        return True
    except Exception:
        return False


async def _click_send_message_cta(page: Page) -> bool:
    cta = page.locator(SELECTORS["send_message_cta"]).first
    if await cta.count() == 0:
        return False
    print("[direct] clicking inbox CTA Send message")
    try:
        await cta.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    try:
        await cta.click()
        await random_wait(200, 500)
        return True
    except Exception as exc:
        print(f"[direct] send_message_cta failed: {exc}")
        return False


async def _click_compose_icon(page: Page) -> bool:
    icon = page.locator(SELECTORS["compose_icon"]).first
    if await icon.count() == 0:
        return False
    print("[direct] clicking compose icon")
    try:
        await icon.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    try:
        await icon.click()
        await random_wait(200, 500)
        return True
    except Exception as exc:
        print(f"[direct] compose_icon failed: {exc}")
        return False


async def _wait_for_dialog_ready(page: Page, timeout_ms: int = 20_000) -> bool:
    await page.wait_for_timeout(1_000)
    contexts: list[Page | Frame] = []
    try:
        iframe_element = await page.query_selector("iframe")
        if iframe_element:
            frame = await iframe_element.content_frame()
            if frame:
                contexts.append(frame)
    except Exception as exc:
        print(f"[direct] iframe detection skipped: {exc}")
    contexts.append(page)

    for context in contexts:
        for selector in DIALOG_FALLBACK_SELECTORS:
            try:
                await context.wait_for_selector(selector, timeout=timeout_ms)
                _mark_modal_ready(True)
                print(f"[direct] dialog ready via selector: {selector}")
                return True
            except Exception:
                continue

    await _snap(page, "no_dialog_multi")
    print("[direct] failed: no_dialog_multi")
    return False


async def ensure_new_message_dialog(page: Page, timeout_ms: int = 10_000) -> bool:
    """Garantiza que el modal de 'New message' esté abierto (CTA ➜ ícono ➜ goto/direct/new)."""
    dialog = page.locator(SELECTORS["dialog"])
    if await dialog.count() > 0:
        _mark_modal_ready(True)
        print("[direct] dialog already visible")
        return True

    clicked = await _click_send_message_cta(page)
    if clicked and await _wait_for_dialog_ready(page, timeout_ms):
        return True

    clicked = await _click_compose_icon(page)
    if clicked and await _wait_for_dialog_ready(page, timeout_ms):
        return True

    print("[direct] fallback to /direct/new")
    try:
        await goto_direct_new(page)
    except Exception as exc:
        print(f"[direct] goto_direct_new_modal failed: {exc}")

    # Retry CTA/Icon after forcing /direct/new
    clicked = await _click_send_message_cta(page)
    if clicked and await _wait_for_dialog_ready(page, timeout_ms):
        return True

    clicked = await _click_compose_icon(page)
    if clicked and await _wait_for_dialog_ready(page, timeout_ms):
        return True

    if await _wait_for_dialog_ready(page, timeout_ms):
        return True

    await _snap(page, "no_dialog_after_goto")
    await _snap(page, "no_dialog")
    _mark_modal_ready(False)
    print("[direct] failed: no_dialog")
    return False


async def ensure_inbox(page: Page, timeout_ms: int = 25_000) -> bool:
    """Garantiza estar en la UI de Direct y, de ser posible, en el diálogo de 'New'."""
    await page.wait_for_load_state("domcontentloaded")

    if _is_modal_ready() and await page.locator(SELECTORS["dialog"]).count() > 0:
        return True

    if "/direct/" in (page.url or ""):
        await dismiss_popups(page)
        await _snap_step(page, "already_in_direct")
        if await _wait_for_direct_surface(page, timeout_ms):
            await random_wait(300, 700)
            return True

    for cycle in range(2):
        if _is_modal_ready() and await page.locator(SELECTORS["dialog"]).count() > 0:
            return True
        for label, action in (
            (f"goto_new_{cycle+1}", goto_direct_new),
            (f"goto_inbox_{cycle+1}", goto_direct_inbox),
        ):
            try:
                await action(page)
            except Exception as exc:
                print(f"[direct] {label} failed: {exc}")
            await _snap_step(page, label)
            await dismiss_popups(page)
            if await _wait_for_direct_surface(page, timeout_ms):
                await random_wait(300, 700)
                return True

    if _is_modal_ready() and await page.locator(SELECTORS["dialog"]).count() > 0:
        return True

    try:
        icon = page.locator(SELECTORS["inbox_icon"]).first
        if await icon.count() > 0:
            await icon.click()
            await random_wait(400, 900)
            await _snap_step(page, "click_inbox_icon")
    except Exception as exc:
        print(f"[direct] click_inbox_icon failed: {exc}")

    if _is_modal_ready() and await page.locator(SELECTORS["dialog"]).count() > 0:
        return True

    await goto_direct_new(page)
    await _snap_step(page, "goto_new_final")
    await dismiss_popups(page)
    if await _wait_for_direct_surface(page, timeout_ms):
        await random_wait(300, 700)
        return True

    await _snap(page, "step_no_dialog_final")
    await _log_search_candidates(page)
    return False


async def open_new_message(page: Page, timeout_ms: int = 25_000) -> bool:
    """Garantiza que el modal de nuevo mensaje esté visible."""
    if await ensure_new_message_dialog(page, timeout_ms=timeout_ms):
        return True

    steps = []
    btn = page.locator(SELECTORS["new_message"]).first
    if await btn.count() > 0:

        async def click_new_button():
            await btn.click()
            await random_wait(400, 900)

        steps.append(("click_new_button", click_new_button))

    async def force_direct_new():
        await goto_direct_new(page)

    steps.append(("goto_direct_new_force", force_direct_new))

    for label, action in steps:
        try:
            await action()
        except Exception as exc:
            print(f"[direct] {label} failed: {exc}")
        await _snap_step(page, label)
        await dismiss_popups(page)
        if await ensure_new_message_dialog(page, timeout_ms=timeout_ms):
            await random_wait(400, 900)
            return True

    await _snap_step(page, "dialog_missing")
    return False


async def _get_dialog_search_field(page: Page) -> Optional[Locator]:
    dialog = page.locator(SELECTORS["dialog"]).first
    if await dialog.count() == 0:
        frame_dialog = None
        for frame in page.frames:
            try:
                candidate = frame.locator(SELECTORS["dialog"]).first
            except Exception:
                continue
            if await candidate.count() > 0:
                frame_dialog = candidate
                break
        if frame_dialog is None:
            return None
        dialog = frame_dialog

    for selector in DIALOG_SEARCH_CANDIDATES:
        candidate = dialog.locator(selector).first
        if await candidate.count() > 0:
            return candidate

    textbox = dialog.get_by_role("textbox").first
    if await textbox.count() > 0:
        return textbox

    # fallback: any input text inside dialog
    fallback = dialog.locator("input[type='text'], textarea").first
    if await fallback.count() > 0:
        return fallback
    return None


async def _clear_search_field(field: Locator) -> None:
    try:
        await field.fill("")
        return
    except Exception:
        pass
    try:
        await field.press("Control+A")
        await field.press("Delete")
        return
    except Exception:
        pass
    try:
        await field.evaluate(
            "(el) => { if (el && typeof el.value === 'string') el.value = '';"
            " if (el && el.isContentEditable) el.textContent = ''; }"
        )
    except Exception:
        pass


async def _wait_results_in_dialog(page: Page, timeout_ms: int = 9_000) -> bool:
    # Primero intenta el patrón típico de resultados del diálogo
    try:
        await page.wait_for_selector(
            '[role="dialog"] div[role="button"]',
            timeout=timeout_ms,
        )
        return True
    except PWTimeoutError:
        # Fallback al selector genérico que ya usabas
        try:
            await page.wait_for_selector(SELECTORS["dialog_result"], timeout=timeout_ms)
            return True
        except PWTimeoutError:
            return False


async def _wait_results_stable(page: Page, min_ms: int = 1_000, max_ms: int = 2_200) -> bool:
    """
    Espera a que la lista de resultados del diálogo esté "estable":
    que haya resultados y que el conteo no cambie durante al menos `min_ms`.
    """
    selector = SELECTORS["dialog_result"]
    start = time.time()
    last_count: Optional[int] = None
    stable_since: Optional[float] = None

    while (time.time() - start) * 1000 < max_ms:
        try:
            count = await page.locator(selector).count()
        except Exception:
            count = 0

        now = time.time()
        if count == 0:
            stable_since = None
        else:
            if last_count == count:
                if stable_since is None:
                    stable_since = now
                elif (now - stable_since) * 1000 >= min_ms:
                    return True
            else:
                stable_since = now

        last_count = count
        await page.wait_for_timeout(150)

    return False


async def _click_row_checkbox(row: Locator) -> bool:
    checkbox = row.locator(
        "input[type='checkbox'], input[type='radio'], "
        "[role='checkbox'], [role='radio'], [aria-checked]"
    )
    if await checkbox.count() > 0:
        try:
            await checkbox.first.click()
            await random_wait(250, 600)
            return True
        except Exception:
            pass
    try:
        await row.click()
        await random_wait(250, 600)
        return True
    except Exception:
        return False


async def _locate_username_row(page: Page, handle: str) -> Optional[Locator]:
    handle_norm = handle.strip().lstrip("@").lower()
    if not handle_norm:
        return None
    results = page.locator(SELECTORS["dialog_result"])
    total = await results.count()
    if total == 0:
        return None
    limit = min(total, 20)
    for idx in range(limit):
        candidate = results.nth(idx)
        try:
            label = (await candidate.inner_text()) or ""
        except Exception:
            continue
        text = label.strip()
        if not text:
            continue
        text_norm = text.lower()
        if handle_norm in text_norm or f"@{handle_norm}" in text_norm:
            snippet = text.replace("\n", " ")[:80]
            print(f"[direct] picked row for {handle}: #{idx} '{snippet}'")
            return candidate
    return None


async def _wait_and_click_submit(
    page: Page,
    *,
    ensure_thread: bool = False,
    timeout_ms: int = 10_000,
) -> bool:
    submit = page.locator(
        '[role="dialog"] button:has-text("Chat"), '
        '[role="dialog"] button:has-text("Message"), '
        '[role="dialog"] button:has-text("Next")'
    ).first
    if await submit.count() == 0:
        await _snap(page, "submit_button_missing")
        return False

    try:
        await submit.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass

    try:
        await submit.wait_for(state="visible", timeout=timeout_ms)
        await page.wait_for_function("(button) => button && !button.disabled", submit, timeout=timeout_ms)
        await submit.click()
        print("[direct] clicked Chat/Next submit button")
        await page.wait_for_selector(SELECTORS["dialog"], state="detached", timeout=6_000)
        await random_wait(300, 700)
        if ensure_thread:
            opened = await wait_thread_open(page, timeout_ms=15_000)
            if not opened:
                await _snap(page, "open_failed_submit")
                print("[direct] submit failed: thread not opened")
                return False
        return True
    except Exception as exc:
        print(f"[direct] submit click failed: {exc}")
        try:
            await page.keyboard.press("Enter")
            await page.wait_for_selector(SELECTORS["dialog"], state="detached", timeout=6_000)
            await random_wait(300, 700)
            if ensure_thread:
                opened = await wait_thread_open(page, timeout_ms=15_000)
                if not opened:
                    await _snap(page, "open_failed_submit")
                    print("[direct] submit failed: thread not opened (enter fallback)")
                    return False
            return True
        except Exception as exc2:
            print(f"[direct] submit enter fallback failed: {exc2}")
            await _snap(page, "submit_click_failed")
            return False


async def search_and_select(page: Page, username: str, exact: bool = True) -> tuple[bool, Optional[str]]:
    """Abre el modal si hace falta, busca dentro del modal y selecciona un resultado."""
    handle = username.strip().lstrip("@")
    if not handle:
        return False, "invalid_username"

    if not _is_modal_ready() or await page.locator(SELECTORS["dialog"]).count() == 0:
        ok = await ensure_new_message_dialog(page)
        if not ok:
            return False, "no_dialog"

    search = await _get_dialog_search_field(page)
    if search is None:
        await _snap(page, "no_dialog_search")
        return False, "no_dialog_search"

    await search.click()
    await random_wait(150, 300)
    await _clear_search_field(search)
    await random_wait(150, 300)
    await search.type(handle, delay=80)
    if not await _wait_results_stable(page, min_ms=1_000, max_ms=2_200):
        await _snap(page, f"no_results_stable_{handle}")
        return False, f"no_results_stable_{handle}"

    if exact:
        row = page.locator(SELECTORS["dialog_result"]).filter(has_text=handle).first
        if await row.count() == 0:
            row = page.locator(SELECTORS["dialog_result"]).first
    else:
        row = page.locator(SELECTORS["dialog_result"]).first

    if await row.count() == 0:
        await _snap(page, f"no_pick_or_submit_{handle}")
        return False, f"no_pick_or_submit_{handle}"

    if not await _click_row_checkbox(row):
        await _snap(page, f"no_pick_{handle}")
        return False, f"no_pick_{handle}"

    submitted = await _wait_and_click_submit(page, timeout_ms=8_000)
    if not submitted:
        try:
            await page.keyboard.press("Enter")
            await random_wait(250, 600)
            submitted = True
        except Exception:
            submitted = False

    if not submitted:
        await _snap(page, f"no_pick_or_submit_{handle}")
        return False, f"no_pick_or_submit_{handle}"

    return True, None


async def open_chat(page: Page, username: str) -> dict:
    """Abre /direct/new, selecciona el usuario y espera el hilo listo para tipear."""
    handle = username.strip().lstrip("@")
    result = {"ok": False, "username": handle or username, "reason": ""}
    if not handle:
        await _snap(page, "no_results")
        result["reason"] = "invalid_username"
        return result

    try:
        await goto_direct_new(page)
    except Exception as exc:
        print(f"[direct] goto_direct_new (open_chat) failed: {exc}")

    await dismiss_popups(page)

    if not await ensure_new_message_dialog(page):
        await _snap(page, "no_dialog")
        result["reason"] = "no_dialog"
        return result

    search = await _get_dialog_search_field(page)
    if search is None:
        await _snap(page, "no_dialog_search")
        result["reason"] = "no_dialog_search"
        return result

    await search.click()
    await random_wait(150, 300)
    await _clear_search_field(search)
    await random_wait(150, 300)
    await search.type(handle, delay=80)
    await random_wait(1_000, 2_000)

    if not await _wait_results_in_dialog(page, timeout_ms=9_000):
        await _snap(page, f"no_results_{handle}")
        result["reason"] = "no_results"
        return result

    row = await _locate_username_row(page, handle)
    if row is None:
        await _snap(page, f"no_results_in_dialog_{handle}")
        result["reason"] = "no_results"
        return result

    if not await _click_row_checkbox(row):
        await _snap(page, f"no_pick_{handle}")
        result["reason"] = "no_pick"
        return result

    submitted = await _wait_and_click_submit(page, ensure_thread=True, timeout_ms=10_000)
    if not submitted:
        await _snap(page, f"submit_failed_{handle}")
        result["reason"] = "submit_failed"
        return result

    print(f"[direct] open_chat success for {handle}")
    result.update(ok=True, reason="ok")
    return result


async def confirm_next(page: Page) -> bool:
    """Confirma el diálogo de selección (si el botón existe)."""
    btn = page.locator(SELECTORS["next_button"]).first
    if await btn.count() == 0:
        return False
    try:
        await btn.click()
        await random_wait(400, 900)
        return True
    except Exception as exc:
        print(f"[direct] confirm_next failed: {exc}")
        return False


async def wait_thread_open(page: Page, timeout_ms: int = 15_000) -> bool:
    """Confirma que se abrió el hilo (URL de thread o composer listo)."""
    try:
        # Espera explícitamente a que la URL sea un hilo de /direct/t/
        await page.wait_for_url("**/direct/t/**", timeout=timeout_ms)
    except PWTimeoutError:
        # Si la URL no cambió a tiempo, igual intentamos verificar por header/composer
        pass

    try:
        await page.wait_for_selector(
            f"{SELECTORS['thread_header']}, {SELECTORS['composer']}",
            timeout=timeout_ms,
        )
        _mark_modal_ready(False)
        return True
    except PWTimeoutError:
        await _snap(page, "thread_not_opened")
        return False


async def focus_composer(page: Page) -> bool:
    """Fija foco en el composer; devuelve True si quedó listo para tipear."""
    comp = page.locator(SELECTORS["composer"]).first
    if await comp.count() == 0:
        await _snap(page, "composer_not_found")
        return False
    try:
        await comp.click()
        await random_wait(200, 500)
        return True
    except Exception as exc:
        print(f"[direct] focus_composer failed: {exc}")
        await _snap(page, "composer_click_error")
        return False


async def click_send(page: Page) -> None:
    send_btn = page.locator(SELECTORS["send_button"])
    if await send_btn.count() > 0:
        await send_btn.first.click()
    else:
        await page.keyboard.press("Enter")
    await random_wait(350, 900)


async def wait_own_bubble(page: Page, timeout_ms: int = 8_000) -> bool:
    try:
        await page.wait_for_selector(SELECTORS["own_bubble"], timeout=timeout_ms, state="visible")
        return True
    except PWTimeoutError:
        return False


async def last_error_toast(page: Page) -> Optional[str]:
    toast = page.locator(SELECTORS["toast_error"]).last
    if await toast.count() > 0:
        try:
            return (await toast.inner_text()).strip()
        except Exception:
            return "toast_error"
    return None
