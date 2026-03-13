from __future__ import annotations

import random
from typing import Any

from src.opt_in import human_engine

from . import WarmupActionContext, WarmupActionResult, account_page, human_pause


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    quantity = max(1, int(context.payload.get("quantity") or 3))
    min_view = max(2, int(context.payload.get("min_view_seconds") or 5))
    max_view = max(min_view, int(context.payload.get("max_view_seconds") or max(min_view + 2, 8)))

    async with account_page(context) as (_service, _browser_context, page):
        await page.goto("https://www.instagram.com/reels/", wait_until="domcontentloaded")
        await human_engine.wait_for_navigation_idle(page)
        for index in range(quantity):
            popup = await human_engine.detect_block_popup(page)
            if popup:
                raise RuntimeError(popup)
            await human_pause(random.uniform(min_view, max_view))
            result.performed += 1
            if index >= quantity - 1:
                continue
            try:
                await page.keyboard.press("ArrowDown")
            except Exception:
                next_result = await human_engine.click(
                    page,
                    [
                        "button[aria-label='Next']",
                        "svg[aria-label='Next']",
                        "button:has-text('Next')",
                    ],
                )
                if not next_result.ok:
                    result.add_detail("No se pudo avanzar al siguiente reel en todos los intentos.")
                    break
            await human_pause(random.uniform(0.8, 1.6))
    return result
