# scripts/smoke_login_real.py
import os
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import browser_profiles_root
from src.playwright_service import resolve_playwright_executable
from src.runtime.playwright_runtime import launch_sync_browser
from src.stealth.stealth_core import patch_context

INSTAGRAM_URL = "https://www.instagram.com/"
PROFILES_DIR = browser_profiles_root(ROOT)


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

    executable = resolve_playwright_executable(headless=True)
    browser = launch_sync_browser(
        headless=True,
        executable_path=executable,
        visible_reason=f"smoke_login_real:{user}",
    )
    context = None
    try:
        context = browser.new_context(storage_state=str(storage_path))
        patch_context(context, user)
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
