from __future__ import annotations

from typing import Optional
from playwright.async_api import Page, TimeoutError as PWTimeoutError
from src.actions.direct_helpers import (
    _snap,
    dismiss_popups,
    goto_direct_new,
    _get_dialog_search_field,
    _clear_search_field,
    _wait_results_in_dialog,
    _locate_username_row,
    _wait_and_click_submit,
    wait_thread_open,
)
from src.humanizer import random_wait


async def open_chat(page: Page, username: str) -> dict:
    """
    Versión v2 corregida y estable de open_chat.
    Usa los helpers reales del sistema original.
    NO rompe nada del archivo original.
    """

    handle = username.strip().lstrip("@")
    result = {"ok": False, "username": handle or username, "reason": ""}

    if not handle:
        await _snap(page, "invalid_username")
        result["reason"] = "invalid_username"
        return result

    print(f"\n[direct_v2] === OPENING CHAT WITH {handle} ===")

    # ------------------------------------------------------------------
    # PASO 1 — Navegar a /direct/new (más estable que inbox → compose)
    # ------------------------------------------------------------------
    try:
        print("[direct_v2] Step 1: goto_direct_new")
        await goto_direct_new(page)
        await random_wait(300, 700)
    except Exception as e:
        print(f"[direct_v2] goto_direct_new failed: {e}")
        result["reason"] = "goto_failed"
        return result

    # ------------------------------------------------------------------
    # PASO 2 — Cerrar popups
    # ------------------------------------------------------------------
    print("[direct_v2] Step 2: dismiss_popups")
    await dismiss_popups(page)
    await random_wait(300, 700)

    # ------------------------------------------------------------------
    # PASO 3 — Esperar a que aparezca el modal "New message"
    # ------------------------------------------------------------------
    try:
        print("[direct_v2] Step 3: wait_for_dialog (role='dialog')")
        await page.wait_for_selector('[role="dialog"]', timeout=12_000)
    except PWTimeoutError:
        await _snap(page, "dialog_not_visible")
        result["reason"] = "dialog_timeout"
        return result

    # ------------------------------------------------------------------
    # PASO 4 — Buscar input de búsqueda
    # ------------------------------------------------------------------
    print("[direct_v2] Step 4: find search input")
    search_field = await _get_dialog_search_field(page)
    if search_field is None:
        await _snap(page, "no_search_input")
        result["reason"] = "no_dialog_search"
        return result

    # ------------------------------------------------------------------
    # PASO 5 — Tipear el username
    # ------------------------------------------------------------------
    print("[direct_v2] Step 5: typing username")
    try:
        await search_field.click()
        await random_wait(120, 260)
        await _clear_search_field(search_field)
        await random_wait(120, 260)
        await search_field.type(handle, delay=75)
    except Exception as e:
        print(f"[direct_v2] typing_error: {e}")
        await _snap(page, "typing_error")
        result["reason"] = "typing_error"
        return result

    await random_wait(800, 1500)

    # ------------------------------------------------------------------
    # PASO 6 — Esperar resultados
    # ------------------------------------------------------------------
    print("[direct_v2] Step 6: wait results")
    if not await _wait_results_in_dialog(page, timeout_ms=8_000):
        await _snap(page, f"no_results_{handle}")
        result["reason"] = "no_results"
        return result

    # ------------------------------------------------------------------
    # PASO 7 — Seleccionar row del usuario
    # ------------------------------------------------------------------
    print("[direct_v2] Step 7: locate row")
    row = await _locate_username_row(page, handle)
    if row is None:
        await _snap(page, f"no_row_{handle}")
        result["reason"] = "no_results"
        return result

    # Click al checkbox o row
    try:
        print("[direct_v2] Step 7b: clicking row")
        await row.click()
        await random_wait(250, 600)
    except Exception as e:
        print(f"[direct_v2] row_click_error: {e}")
        await _snap(page, "row_click_error")
        result["reason"] = "pick_failed"
        return result

    # ------------------------------------------------------------------
    # PASO 8 — Click en Chat/Next dentro del modal
    # ------------------------------------------------------------------
    print("[direct_v2] Step 8: click submit button")
    submitted = await _wait_and_click_submit(page, ensure_thread=False)
    if not submitted:
        await _snap(page, "submit_failed")
        result["reason"] = "submit_failed"
        return result

    # ------------------------------------------------------------------
    # PASO 9 — Esperar a que se abra el hilo DM
    # ------------------------------------------------------------------
    print("[direct_v2] Step 9: wait thread")
    if not await wait_thread_open(page, timeout_ms=15_000):
        result["reason"] = "thread_not_opened"
        return result

    print(f"[direct_v2] ✅ SUCCESS: Chat opened with {handle}")
    result["ok"] = True
    result["reason"] = "ok"
    return result
