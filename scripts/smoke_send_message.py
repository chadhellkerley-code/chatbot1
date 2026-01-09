from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import random
import time
from pathlib import Path
from typing import List, Optional

from playwright.async_api import async_playwright

from src.actions.direct_helpers import (
    ensure_inbox,
    open_new_message,
    search_and_select,
    focus_composer,
    wait_own_bubble,
    last_error_toast,
    wait_thread_open,
    SELECTORS_JOINED,
)
from src.actions.dm_actions import pick_random_message
from src.humanizer import type_text, random_wait
from src.playwright_service import (
    BASE_PROFILES,
    DEFAULT_ARGS,
    DEFAULT_TIMEZONE,
    DEFAULT_USER_AGENT,
    DEFAULT_VIEWPORT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_usernames(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        return [u.strip() for u in data.get("usernames", []) if u.strip()]
    if p.suffix.lower() == ".csv":
        with p.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [row["username"].strip() for row in reader if row.get("username")]
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_messages(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        return [m.strip() for m in data if m.strip()]
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


async def create_context(username: str, proxy: dict | None = None):
    pw = await async_playwright().start()
    profile_dir = BASE_PROFILES / username
    profile_dir.mkdir(parents=True, exist_ok=True)
    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=False,  # smoke test: always visible for manual verification
        proxy=proxy,
        viewport=DEFAULT_VIEWPORT,
        user_agent=DEFAULT_USER_AGENT,
        locale="en-US",
        timezone_id=DEFAULT_TIMEZONE,
        args=DEFAULT_ARGS,
    )
    return pw, context


async def send_once(page, username: str, template: str, typing_cfg: dict) -> dict:
    print("1/5 Abriendo diálogo de nuevo mensaje")
    if not await ensure_inbox(page):
        return {"ok": False, "error": "inbox_unavailable"}
    if not await open_new_message(page):
        return {"ok": False, "error": "no_dialog"}

    print("2/5 Buscando usuario", username)
    picked, reason = await search_and_select(page, username, exact=True)
    if not picked:
        return {"ok": False, "error": reason or "user_not_found"}

    print("3/5 Abriendo hilo")
    if not await wait_thread_open(page, timeout_ms=10_000):
        return {"ok": False, "error": "open_failed"}

    if not await focus_composer(page):
        return {"ok": False, "error": "composer_not_found"}
    composer = page.locator(SELECTORS_JOINED["composer"]).first

    personalized = template.format(username=username)
    print("4/5 Escribiendo mensaje")
    await type_text(
        composer,
        personalized,
        min_delay=typing_cfg.get("min_delay", 0.05),
        max_delay=typing_cfg.get("max_delay", 0.2),
        occasional_pause=typing_cfg.get("occasional_pause", 0.12),
    )
    await random_wait(200, 450)
    await page.keyboard.press("Enter")

    sent = await wait_own_bubble(page, timeout_ms=9_000)
    if not sent:
        toast = await last_error_toast(page)
        return {"ok": False, "error": toast or "send_failed"}
    return {"ok": True, "sent_text": personalized}


async def capture_smoke_error(page, base_png: Path, reason: str) -> Optional[Path]:
    try:
        base_png.parent.mkdir(parents=True, exist_ok=True)
        target = base_png.with_name(f"{base_png.stem}_{reason}.png")
        await page.screenshot(path=str(target))
        print(f"[debug] Screenshot guardado en {target}")
        return target
    except Exception:
        return None


async def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test: enviar DM usando sesión persistente.")
    ap.add_argument("--account", required=True, help="username de la cuenta (debe tener session en profiles/)")
    ap.add_argument("--recipients", required=True, help="ruta CSV/JSON/TXT con usernames objetivo")
    ap.add_argument("--messages", required=True, help="ruta messages.json o .txt con plantillas")
    ap.add_argument("--headful", action="store_true", help="(deprecated) navegador ya corre en modo visible")
    ap.add_argument("--keep-open", action="store_true", help="No cerrar el navegador al finalizar")
    args = ap.parse_args()

    usernames = load_usernames(args.recipients)[:2]  # smoke: solo dos destinatarios
    messages = load_messages(args.messages)
    if not usernames or not messages:
        raise SystemExit("Proveer al menos un username y un mensaje.")

    pw, context = await create_context(args.account)
    await context.tracing.start(screenshots=True, snapshots=True, sources=True)
    page = await context.new_page()
    print(f"[] Logged in as {args.account} (se espera sesión persistente)")

    try:
        for username in usernames:
            template = random.choice(messages)
            base_png = Path("profiles") / args.account / "dm_debug" / f"{username}_{int(time.time())}.png"
            print(f"[ ] Sending to {username}...")
            t0 = time.time()
            result = await send_once(page, username, template, {"min_delay": 0.04, "max_delay": 0.18})
            elapsed = (time.time() - t0) * 1000
            if result.get("ok"):
                print(f"[💬] Sent successfully to {username} ({elapsed:.0f} ms)")
            else:
                print(f"[X] Failed for {username}: {result.get('error')} ({elapsed:.0f} ms)")
                snap_path = await capture_smoke_error(page, base_png, result.get("error", "error"))
                if snap_path:
                    print(f"[X] Debug screenshot: {snap_path}")
                print("[X] Revisar profiles/dm_debug/*.png para más contexto.")
            await asyncio.sleep(random.uniform(1.2, 2.4))
        print("[✓] Smoke test finalizado.")
        return 0
    finally:
        trace_dir = Path("out") / "traces"
        trace_dir.mkdir(parents=True, exist_ok=True)
        trace_path = trace_dir / f"{args.account}_{int(time.time())}.zip"
        try:
            await context.tracing.stop(path=str(trace_path))
            print(f"[i] Trace guardado en {trace_path}")
        except Exception:
            pass
        if args.keep_open:
            print("[i] Manteniendo la ventana abierta (--keep-open). Cierra manualmente cuando quieras.")
            try:
                await asyncio.sleep(3600)
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass
        await context.close()
        await pw.stop()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
