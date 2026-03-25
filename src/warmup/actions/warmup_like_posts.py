from __future__ import annotations

import random

from . import WarmupActionContext, WarmupActionResult, account_page, human_pause, open_profile


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    quantity = max(1, int(context.payload.get("quantity") or 1))
    target = str(context.payload.get("target") or "").strip()

    async with account_page(context) as (_service, _browser_context, page):
        if target.startswith("http"):
            await page.goto(target, wait_until="domcontentloaded")
        elif target:
            await open_profile(page, target)
        else:
            await page.goto("https://www.instagram.com/explore/", wait_until="domcontentloaded")
        for _index in range(quantity):
            like_button = page.locator("svg[aria-label='Like']").first
            if await like_button.count() <= 0:
                break
            await like_button.click()
            result.performed += 1
            await human_pause(random.uniform(1.2, 2.1))
            try:
                await page.mouse.wheel(0, 900)
            except Exception:
                break
    return result
