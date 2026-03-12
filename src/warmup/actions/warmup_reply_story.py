from __future__ import annotations

import random

from src.opt_in import human_engine

from . import WarmupActionContext, WarmupActionResult, account_page, human_pause, open_profile


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    target = str(context.payload.get("target") or "").strip()
    reply_text = str(context.payload.get("text") or "").strip() or "Buenisima historia."
    if not target:
        raise RuntimeError("Debes indicar una cuenta objetivo para responder la historia.")

    async with account_page(context) as (_service, _browser_context, page):
        await open_profile(page, target)
        open_story = await human_engine.click(
            page,
            [
                "canvas",
                "header img",
                "img[alt*='profile picture']",
            ],
        )
        if not open_story.ok:
            raise RuntimeError(open_story.message or "No se pudo abrir la historia.")
        await human_pause(random.uniform(1.2, 2.0))
        fill_result = await human_engine.fill(
            page,
            [
                "textarea[placeholder*='message']",
                "textarea[aria-label*='Message']",
                "div[contenteditable='true']",
            ],
            reply_text,
        )
        if not fill_result.ok:
            raise RuntimeError(fill_result.message or "No se pudo escribir la respuesta de historia.")
        send_result = await human_engine.click(
            page,
            [
                "button:has-text('Send')",
                "button:has-text('Enviar')",
            ],
        )
        if not send_result.ok:
            raise RuntimeError(send_result.message or "No se pudo enviar la respuesta de historia.")
        result.performed = 1
    return result
