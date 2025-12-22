# scripts/smoke_login_real.py
import os
import sys
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

INSTAGRAM_URL = "https://www.instagram.com/"
PROFILES_DIR = Path("profiles")


def _cli_user() -> Optional[str]:
    """Return the username passed as first CLI argument, if any."""
    if len(sys.argv) > 1:
        candidate = sys.argv[1].strip()
        if candidate:
            return candidate
    return None


def _latest_storage(base: Path) -> Optional[Path]:
    """Return the most recently modified storage_state.json under base."""
    if not base.exists():
        return None
    return max(
        base.glob("*/storage_state.json"),
        key=lambda path: path.stat().st_mtime,
        default=None,
    )


def main() -> None:
    """Smoke test that reuses the latest persisted session when no env vars are set."""
    base = PROFILES_DIR
    user = _cli_user() or os.getenv("IG_USER")
    storage_path: Optional[Path] = None

    if user:
        storage_path = base / user / "storage_state.json"
    else:
        latest = _latest_storage(base)
        if latest:
            user = latest.parent.name
            storage_path = latest

    if not user:
        print("No se encontro usuario persistente ni variable IG_USER definida.")
        return

    if storage_path is None or not storage_path.exists():
        print(f"No existe storage_state.json para {user}.")
        return

    print(f"Usando sesion persistente: {storage_path}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = None
        try:
            context = browser.new_context(storage_state=str(storage_path))
            page = context.new_page()
            page.goto(INSTAGRAM_URL, wait_until="domcontentloaded", timeout=45_000)
            title = page.title()
            cookies = context.cookies()
            logged_in = any(cookie.get("name") == "sessionid" for cookie in cookies)
            print(f"Estado de sesion: {'OK' if logged_in else 'NO LOGGED'}")
            print(f"Titulo: {title}")
        finally:
            if context is not None:
                context.close()
            browser.close()


if __name__ == "__main__":
    main()
