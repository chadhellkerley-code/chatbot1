"""Manual Playwright content publishing entrypoint."""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Optional

from core.accounts import _open_playwright_manual_session, list_all
from paths import browser_profiles_root
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn


def _profiles_root() -> Path:
    with contextlib.suppress(Exception):
        from src.playwright_service import BASE_PROFILES

        return Path(BASE_PROFILES)
    return browser_profiles_root(Path(__file__).resolve().parents[2])


def _resolve_account(alias: str) -> Optional[dict]:
    accounts = [acct for acct in list_all() if acct.get("alias") == alias and acct.get("active")]
    if not accounts:
        warn("No hay cuentas activas en este alias.")
        press_enter()
        return None

    base_profiles = _profiles_root()
    print("Seleccioná 1 cuenta activa (número o username). Enter = volver.\n")
    for idx, acct in enumerate(accounts, start=1):
        username = str(acct.get("username") or "").strip().lstrip("@")
        sess = "[pw]" if (base_profiles / username / "storage_state.json").exists() else "[sin pw]"
        proxy_flag = " [proxy]" if acct.get("proxy_url") else ""
        low_flag = " [bajo perfil]" if acct.get("low_profile") else ""
        print(f" {idx}) @{username} {sess}{proxy_flag}{low_flag}")
        if low_flag and acct.get("low_profile_reason"):
            print(f"    ↳ {acct['low_profile_reason']}")

    raw = ask("\nCuenta: ").strip()
    if not raw:
        return None
    if raw.isdigit():
        position = int(raw)
        if 1 <= position <= len(accounts):
            return accounts[position - 1]

    target = raw.lstrip("@").strip().lower()
    for acct in accounts:
        username = str(acct.get("username") or "").strip().lstrip("@").lower()
        if username == target:
            return acct
    warn("No se encontró la cuenta con esos datos.")
    press_enter()
    return None


def run_from_menu(alias: str) -> None:
    banner()
    print(style_text("📤 Subir contenidos (Historias / Post / Reels)", color=Fore.CYAN, bold=True))
    print(full_line())

    chosen = _resolve_account(alias)
    if not chosen:
        return

    print("\nTipo de contenido (manual con Playwright):")
    print("1) Historia")
    print("2) Post (feed)")
    print("3) Reel")
    kind_map = {
        "1": ("Historia", "https://www.instagram.com/create/story/"),
        "2": ("Post", "https://www.instagram.com/create/select/"),
        "3": ("Reel", "https://www.instagram.com/reels/create/"),
    }
    choice = ask("Opción: ").strip()
    kind_payload = kind_map.get(choice)
    if not kind_payload:
        warn("Opción inválida.")
        press_enter()
        return

    kind_label, start_url = kind_payload
    _open_playwright_manual_session(
        chosen,
        start_url=start_url,
        action_label=f"Subir {kind_label} (manual)",
    )
    ok("Actividad completada.")
    press_enter()


__all__ = ["run_from_menu"]
