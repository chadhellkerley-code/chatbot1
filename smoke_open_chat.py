from __future__ import annotations

import asyncio
import os
from pathlib import Path

# ⬇️ IMPORTA LA NUEVA VERSIÓN v2 (aislada, segura)
from src.actions.direct_helpers_v2 import open_chat
from src.playwright_service import ensure_context

ACCOUNT = os.environ.get("IG_ACCOUNT") or "mati_diazlife3"
TARGET = os.environ.get("IG_TARGET") or "matidiazlife"

TRACE_DIR = Path("traces")
TRACE_DIR.mkdir(parents=True, exist_ok=True)
TRACE_PATH = TRACE_DIR / f"smoke_open_chat_{ACCOUNT}.zip"


async def main() -> None:
    browser, context, page = await ensure_context(
        account=ACCOUNT,
        headful=True,
        lang="en-US",
    )

    # ---- INICIO TRACE ----
    await context.tracing.start(
        screenshots=True,
        snapshots=True,
        sources=True,
    )

    try:
        print(f"[smoke] Abriendo chat con: {TARGET}")
        result = await open_chat(page, TARGET)
        print(result)

        if not result.get("ok"):
            print(
                f"[X] open_chat v2 falló ({result.get('reason')}). "
                f"Revisa profiles/dm_debug/*.png"
            )
        else:
            await page.wait_for_timeout(2_000)

    finally:
        # ---- FIN TRACE + GUARDADO ----
        try:
            await context.tracing.stop(path=str(TRACE_PATH))
            print(f"[✓] Trace guardado en: {TRACE_PATH}")
        except Exception as e:
            print(f"[!] No se pudo guardar el trace: {e}")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
