from __future__ import annotations

from src.opt_in import human_engine

from . import WarmupActionContext, WarmupActionResult, account_page, open_profile


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    target = str(context.payload.get("target") or "").strip()
    comment_text = str(context.payload.get("text") or "").strip() or "Buen contenido."

    async with account_page(context) as (_service, _browser_context, page):
        if target.startswith("http"):
            await page.goto(target, wait_until="domcontentloaded")
        elif target:
            await open_profile(page, target)
            first_post = page.locator("article a").first
            if await first_post.count() > 0:
                await first_post.click()
        else:
            raise RuntimeError("Debes indicar un post o perfil objetivo para comentar.")

        open_result = await human_engine.click(
            page,
            [
                "svg[aria-label='Comment']",
                "textarea[placeholder*='comment']",
                "textarea[aria-label*='Comment']",
            ],
        )
        if not open_result.ok and "selector_not_found" not in str(open_result.message or ""):
            raise RuntimeError(open_result.message or "No se pudo abrir la caja de comentario.")

        fill_result = await human_engine.fill(
            page,
            [
                "textarea[placeholder*='comment']",
                "textarea[aria-label*='Comment']",
                "textarea",
            ],
            comment_text,
        )
        if not fill_result.ok:
            raise RuntimeError(fill_result.message or "No se pudo escribir el comentario.")
        submit_result = await human_engine.click(
            page,
            [
                "button:has-text('Post')",
                "button:has-text('Publicar')",
            ],
        )
        if not submit_result.ok:
            raise RuntimeError(submit_result.message or "No se pudo enviar el comentario.")
        result.performed = 1
    return result
