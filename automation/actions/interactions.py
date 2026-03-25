"""Playwright-only interaction helpers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List

from config import SETTINGS
from core.accounts import list_all
from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from paths import browser_profiles_root
from runtime.runtime import (
    EngineCancellationToken,
    STOP_EVENT,
    bind_stop_token,
    ensure_logging,
    request_stop,
    reset_stop_event,
    restore_stop_token,
    start_q_listener,
)
from ui import Fore, banner, full_line, style_text
from utils import ask, ask_int, ok, press_enter, warn

from src.auth.persistent_login import ensure_logged_in_async
from src.browser_telemetry import log_browser_stage
from src.proxy_payload import proxy_from_account
from src.transport.session_manager import ManagedSession, SessionManager


logger = logging.getLogger(__name__)
_PLAYWRIGHT_REELS_MANAGERS: dict[bool, SessionManager] = {}


@dataclass
class ReelsPlaywrightSummary:
    username: str
    viewed: int = 0
    liked: int = 0
    errors: int = 0
    messages: List[str] = field(default_factory=list)


def _short_message(exc: Any, *, limit: int = 160) -> str:
    text = str(exc or "").strip() or exc.__class__.__name__
    return text if len(text) <= limit else text[: max(1, limit - 1)].rstrip() + "..."


def _run_async(coro):
    from src.runtime.playwright_runtime import run_coroutine_sync

    return run_coroutine_sync(coro)


def _profiles_root() -> Path:
    with contextlib.suppress(Exception):
        from src.playwright_service import BASE_PROFILES

        return Path(BASE_PROFILES)
    return browser_profiles_root(Path(__file__).resolve().parents[2])


def _reels_session_manager(headless: bool) -> SessionManager:
    manager = _PLAYWRIGHT_REELS_MANAGERS.get(bool(headless))
    if manager is None:
        manager = SessionManager(
            headless=bool(headless),
            keep_browser_open_per_account=True,
            profiles_root=str(_profiles_root()),
            normalize_username=lambda value: str(value or "").strip().lstrip("@"),
            log_event=lambda *_args, **_kwargs: None,
        )
        _PLAYWRIGHT_REELS_MANAGERS[bool(headless)] = manager
    return manager


def _account_proxy_flag(account: dict[str, Any]) -> str:
    return " [proxy]" if any(
        str(account.get(field_name) or "").strip()
        for field_name in ("assigned_proxy_id", "proxy_url", "proxy", "proxy_host", "proxy_name")
    ) else ""


def _selectable_accounts_for_interactions(accounts: List[dict]) -> tuple[List[dict], List[dict]]:
    preflight = preflight_accounts_for_proxy_runtime(accounts)
    ready_accounts = [
        dict(item)
        for item in (preflight.get("ready_accounts") or [])
        if isinstance(item, dict)
    ]
    blocked_accounts = [
        dict(item)
        for item in (preflight.get("blocked_accounts") or [])
        if isinstance(item, dict)
    ]
    return ready_accounts, blocked_accounts


def _select_accounts_playwright(alias: str) -> List[dict]:
    accounts = [acct for acct in list_all() if acct.get("alias") == alias and acct.get("active")]
    if not accounts:
        warn("No hay cuentas activas en este alias.")
        press_enter()
        return []
    accounts, blocked_accounts = _selectable_accounts_for_interactions(accounts)
    for blocked in blocked_accounts:
        username = str(blocked.get("username") or "").strip().lstrip("@")
        message = str(blocked.get("message") or "Proxy bloqueado.").strip() or "Proxy bloqueado."
        warn(f"Cuenta omitida por preflight de proxy @{username or '-'}: {message}.")
    if not accounts:
        warn("No hay cuentas utilizables tras el preflight de proxy.")
        press_enter()
        return []

    base_profiles = _profiles_root()
    print("Selecciona cuentas activas (coma separada, * para todas):")
    for idx, acct in enumerate(accounts, start=1):
        username = str(acct.get("username") or "").strip().lstrip("@")
        sess = "[pw]" if (base_profiles / username / "storage_state.json").exists() else "[sin pw]"
        proxy_flag = _account_proxy_flag(acct)
        low_flag = " [bajo perfil]" if acct.get("low_profile") else ""
        print(f" {idx}) @{username} {sess}{proxy_flag}{low_flag}")
        if low_flag and acct.get("low_profile_reason"):
            print(f"    -> {acct['low_profile_reason']}")

    raw = ask("Seleccion: ").strip() or "*"
    if raw == "*":
        return accounts

    selected: set[str] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if token.isdigit():
            position = int(token)
            if 1 <= position <= len(accounts):
                selected.add(
                    str(accounts[position - 1].get("username") or "").strip().lstrip("@").lower()
                )
            continue
        selected.add(token.lstrip("@").strip().lower())

    chosen = [
        acct
        for acct in accounts
        if str(acct.get("username") or "").strip().lstrip("@").lower() in selected
    ]
    if not chosen:
        warn("No se encontraron cuentas con esos datos.")
        press_enter()
    return chosen


async def _sleep_with_stop_async(seconds: float) -> bool:
    deadline = time.monotonic() + float(max(0.0, seconds))
    while True:
        if STOP_EVENT.is_set():
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        await asyncio.sleep(min(0.5, remaining))


async def _dismiss_popups_async(page) -> None:
    selectors = (
        'button:has-text("Not now")',
        'button:has-text("Not Now")',
        'button:has-text("Ahora no")',
        'button:has-text("Cancel")',
        'button:has-text("Cancelar")',
        'button:has-text("Mas tarde")',
    )
    for selector in selectors:
        with contextlib.suppress(Exception):
            locator = page.locator(selector).first
            if await locator.count() > 0:
                await locator.click()
                await asyncio.sleep(random.uniform(0.2, 0.6))


async def _try_like_current_reel(page) -> bool:
    selectors = (
        "main button:has(svg[aria-label='Like'])",
        "main button:has(svg[aria-label='Me gusta'])",
        "main div[role='button']:has(svg[aria-label='Like'])",
        "main div[role='button']:has(svg[aria-label='Me gusta'])",
    )
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() <= 0:
                continue
            await locator.click(timeout=2_000)
            return True
        except Exception:
            continue
    return False


async def _next_reel(page) -> None:
    for key in ("PageDown", "ArrowDown", "j"):
        try:
            await page.keyboard.press(key)
            return
        except Exception:
            continue
    await page.mouse.wheel(0, 1200)


async def _run_reels_for_account(
    *,
    page,
    summary: ReelsPlaywrightSummary,
    duration_s: int,
    likes_target: int,
) -> None:
    end = time.monotonic() + max(1.0, float(duration_s or 0))
    await _dismiss_popups_async(page)
    await _sleep_with_stop_async(random.uniform(1.5, 2.5))
    while time.monotonic() < end:
        if STOP_EVENT.is_set():
            return
        summary.viewed += 1
        watch_s = random.uniform(25.0, 50.0)
        like_delay = min(random.uniform(4.0, 12.0), watch_s)
        await _sleep_with_stop_async(like_delay)
        if STOP_EVENT.is_set() or time.monotonic() >= end:
            return
        if likes_target > 0 and summary.liked < likes_target:
            with contextlib.suppress(Exception):
                if await _try_like_current_reel(page):
                    summary.liked += 1
        remaining_watch = max(0.0, watch_s - like_delay)
        await _sleep_with_stop_async(remaining_watch)
        if STOP_EVENT.is_set() or time.monotonic() >= end:
            return
        await _sleep_with_stop_async(
            min(random.uniform(0.4, 1.2), max(0.0, end - time.monotonic()))
        )
        if STOP_EVENT.is_set() or time.monotonic() >= end:
            return
        await _next_reel(page)
        await _sleep_with_stop_async(
            min(random.uniform(1.2, 2.4), max(0.0, end - time.monotonic()))
        )


def run_from_menu(alias: str) -> None:
    banner()
    print(style_text("Interacciones (Ver & Like Reels)", color=Fore.CYAN, bold=True))
    print(full_line())

    chosen_accounts = _select_accounts_playwright(alias)
    if not chosen_accounts:
        return

    minutes = ask_int("Tiempo de navegacion por cuenta (min): ", min_value=1, default=10)
    likes_target = ask_int("Cantidad de likes por cuenta: ", min_value=0, default=10)
    print("Cada reel se vera entre 25s y 50s (random).")

    ensure_logging(quiet=SETTINGS.quiet, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    reset_stop_event()
    token = EngineCancellationToken("interactions-reels-playwright")
    binding = bind_stop_token(token)
    listener = start_q_listener(
        "Presiona Q y Enter para detener la accion.",
        logger,
        token=token,
    )

    async def _runner():
        base_profiles = _profiles_root()
        session_manager = _reels_session_manager(True)
        summaries: list[ReelsPlaywrightSummary] = []
        for acct in chosen_accounts:
            if STOP_EVENT.is_set():
                break
            username = str(acct.get("username") or "").strip().lstrip("@")
            summary = ReelsPlaywrightSummary(username=username)
            summaries.append(summary)
            storage_state = base_profiles / username / "storage_state.json"
            if not storage_state.exists():
                summary.errors += 1
                summary.messages.append(
                    "Falta runtime/browser_profiles/<username>/storage_state.json."
                )
                continue
            session: ManagedSession | None = None
            try:
                proxy_payload = proxy_from_account(acct)
                log_browser_stage(
                    component="automation_reels_playwright",
                    stage="spawn",
                    status="started",
                    account=username,
                )
                session = await session_manager.open_session(
                    account=acct,
                    proxy=proxy_payload,
                    login_func=ensure_logged_in_async,
                )
                page = session.page
                try:
                    await page.goto(
                        "https://www.instagram.com/reels/?hl=en",
                        wait_until="domcontentloaded",
                        timeout=60_000,
                    )
                except Exception:
                    await page.goto("https://www.instagram.com/reels/")
                current_url = ""
                with contextlib.suppress(Exception):
                    current_url = (page.url or "").lower()
                if any(
                    token_value in current_url
                    for token_value in ("accounts/login", "/challenge/", "/checkpoint/")
                ):
                    raise RuntimeError("La sesion Playwright expiro o requiere verificacion.")
                log_browser_stage(
                    component="automation_reels_playwright",
                    stage="workspace_ready",
                    status="ok",
                    account=username,
                    url=str(getattr(page, "url", "") or ""),
                )
                await _run_reels_for_account(
                    page=page,
                    summary=summary,
                    duration_s=minutes * 60,
                    likes_target=likes_target,
                )
                with contextlib.suppress(Exception):
                    await session_manager.save_storage_state(session, username)
            except Exception as exc:
                await session_manager.discard_if_unhealthy(
                    session,
                    exc,
                    is_fatal_error=lambda error: "closed" in str(error or "").lower(),
                )
                summary.errors += 1
                summary.messages.append(_short_message(exc, limit=160))
            finally:
                if session is not None:
                    current_url = ""
                    with contextlib.suppress(Exception):
                        current_url = str(getattr(session.page, "url", "") or "")
                    with contextlib.suppress(Exception):
                        await session_manager.finalize_session(session, current_url=current_url)
        return summaries

    try:
        summaries = _run_async(_runner()) or []
    except Exception as exc:
        warn(str(exc) or "No se pudo ejecutar reels.")
        summaries = []
    finally:
        request_stop("reels finalizados")
        listener.join(timeout=0.2)
        restore_stop_token(binding)

    print(full_line(color=Fore.MAGENTA))
    print(style_text("=== RESUMEN REELS (PLAYWRIGHT) ===", color=Fore.YELLOW, bold=True))
    for summary in summaries:
        color = Fore.GREEN if summary.errors == 0 else Fore.YELLOW
        print(
            style_text(
                f"@{summary.username}: vistos={summary.viewed} likes={summary.liked} errores={summary.errors}",
                color=color,
                bold=True,
            )
        )
        for message in summary.messages:
            print(f"  - {message}")
    print(full_line(color=Fore.MAGENTA))
    ok("Proceso finalizado.")
    press_enter()


__all__ = [
    "EngineCancellationToken",
    "ReelsPlaywrightSummary",
    "SETTINGS",
    "STOP_EVENT",
    "_run_async",
    "_run_reels_for_account",
    "_short_message",
    "bind_stop_token",
    "ensure_logging",
    "logger",
    "request_stop",
    "reset_stop_event",
    "restore_stop_token",
    "run_from_menu",
    "start_q_listener",
]
