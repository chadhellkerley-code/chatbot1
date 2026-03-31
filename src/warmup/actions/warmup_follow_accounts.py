from __future__ import annotations

import random

from src.opt_in import human_engine

from . import WarmupActionContext, WarmupActionResult, account_page, human_pause, normalize_targets, open_profile


async def run(context: WarmupActionContext) -> WarmupActionResult:
    result = WarmupActionResult()
    targets = normalize_targets(context.payload.get("target"))
    if not targets:
        result.add_detail("No hay cuentas objetivo para seguir.")
        return result

    async with account_page(context) as (_service, _browser_context, page):
        for target in targets[: max(1, int(context.payload.get("quantity") or len(targets)))]:
            await open_profile(page, target)
            follow_result = await human_engine.click(
                page,
                [
                    "button:has-text('Follow')",
                    "button:has-text('Seguir')",
                ],
            )
            if not follow_result.ok:
                result.add_detail(f"No se pudo seguir a @{target}: {follow_result.message or 'sin detalle'}")
                continue
            result.performed += 1
            await human_pause(random.uniform(1.5, 2.8))
    return result
