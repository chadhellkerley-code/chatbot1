from __future__ import annotations

from src.opt_in import messenger_playwright

from . import WarmupActionContext, WarmupActionResult, account_page


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    target = str(context.payload.get("target") or "").strip().lstrip("@")
    message = str(context.payload.get("text") or "").strip() or "Hola, como va todo?"
    if not target:
        raise RuntimeError("Debes indicar un username objetivo para enviar mensaje.")

    async with account_page(context) as (_service, _browser_context, page):
        inbox = await messenger_playwright.open_inbox(page)
        if not inbox.ok:
            raise RuntimeError(inbox.message or "No se pudo abrir el inbox.")
        composer = await messenger_playwright.open_composer(page)
        if not composer.ok:
            raise RuntimeError(composer.message or "No se pudo abrir el compositor.")
        search = await messenger_playwright.search_user(page, target)
        if not search.ok:
            raise RuntimeError(search.message or f"No se pudo seleccionar a @{target}.")
        sent = await messenger_playwright.send_message(page, message)
        if not sent.ok:
            raise RuntimeError(sent.message or "No se pudo enviar el mensaje.")
        result.performed = 1
    return result
