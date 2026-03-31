import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.instagram_adapter import INSTAGRAM_URL, _has_auth_cookies, is_logged_in  # noqa: E402
from src.playwright_service import BASE_PROFILES, PlaywrightService  # noqa: E402

MAX_ATTEMPTS = 3
FAILED_SCREENS_DIR = Path(BASE_PROFILES) / "login_failed_screenshots"


def _proxy_from_env(raw: str | None):
    if not raw:
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"http://{candidate}"
    parsed = urlparse(candidate)
    if not parsed.scheme or not parsed.hostname or not parsed.port:
        return None
    server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    proxy = {"server": server}
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def main() -> None:
    username = os.getenv("IG_USER")
    password = os.getenv("IG_PASS")
    if not username or not password:
        raise SystemExit("Configura IG_USER e IG_PASS antes de ejecutar este script.")

    proxy_cfg = _proxy_from_env(os.getenv("PROXY_URL", ""))
    print(f">> Usando proxy: {'ON' if proxy_cfg else 'OFF'}")

    svc = PlaywrightService(headless=False)
    svc.start()

    profile_dir = Path(BASE_PROFILES) / username
    storage_state = profile_dir / "storage_state.json"

    ctx = None
    try:
        ctx = svc.new_context_for_account(
            profile_dir=profile_dir,
            storage_state=str(storage_state) if storage_state.exists() else None,
            proxy=proxy_cfg,
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        try:
            page.set_default_timeout(20_000)
            page.set_default_navigation_timeout(45_000)
        except Exception:
            pass
        page.goto(f"{INSTAGRAM_URL}accounts/login/", wait_until="domcontentloaded")

        print(">> Completa el login manualmente (cookies, desafios, notificaciones, etc.).")
        input(">> Cuando llegues al feed o perfil y veas la cuenta activa, presiona Enter aqui...")

        _ensure_manual_login(page, svc, ctx, storage_state, username)
    finally:
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass
        svc.close()


def _ensure_manual_login(page, svc, ctx, storage_state: Path, username: str) -> None:
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        attempts += 1
        _wait_for_idle(page)
        if _session_ready(page):
            svc.save_storage_state(ctx, str(storage_state))
            print(f"Sesion guardada en: {storage_state}")
            return

        if attempts >= MAX_ATTEMPTS:
            screenshot = _capture_manual_failure(page, username)
            msg = "No se detecto sesion activa tras multiples intentos."
            if screenshot:
                msg += f" Screenshot: {screenshot}"
            raise SystemExit(msg)

        print(">> No se detecto sesion activa.")
        print("   Navega a la home (logo de Instagram), verifica que ves el feed y presiona Enter para reintentar.")
        input(">> Cuando veas el feed y la cuenta este lista, presiona Enter aqui...")


def _session_ready(page) -> bool:
    if is_logged_in(page):
        return True
    try:
        return _has_auth_cookies(page.context)
    except Exception:
        return False


def _wait_for_idle(page) -> None:
    try:
        page.wait_for_load_state("networkidle")
    except Exception:
        try:
            page.wait_for_load_state("load")
        except Exception:
            pass


def _capture_manual_failure(page, username: str):
    FAILED_SCREENS_DIR.mkdir(parents=True, exist_ok=True)
    screenshot_path = FAILED_SCREENS_DIR / f"{username}_manual_failed.png"
    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        print(f">> Screenshot guardado en {screenshot_path}")
        return screenshot_path
    except Exception:
        return None


if __name__ == "__main__":
    main()
