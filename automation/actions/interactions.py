"""Playwright-only interaction helpers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
<<<<<<< HEAD
import re
=======
>>>>>>> origin/main
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List
<<<<<<< HEAD
from urllib.parse import urlparse
=======
>>>>>>> origin/main

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
<<<<<<< HEAD
_REEL_MIN_SECONDS = 5.0
_REEL_MAX_SECONDS = 45.0
_LIKE_SESSION_PROGRESS_FLOOR = 0.12
_LIKE_SESSION_PROGRESS_CEILING = 0.92
_FOLLOW_SESSION_PROGRESS_FLOOR = 0.2
_FOLLOW_SESSION_PROGRESS_CEILING = 0.9
_PROFILE_PATH_BLOCKLIST = {
    "accounts",
    "api",
    "direct",
    "explore",
    "developer",
    "directory",
    "graphql",
    "legal",
    "oauth",
    "p",
    "reel",
    "reels",
    "static",
    "stories",
    "tags",
    "tv",
}
_PROFILE_SECONDARY_SEGMENTS = {"reels", "tagged", "channel"}
=======
>>>>>>> origin/main


@dataclass
class ReelsPlaywrightSummary:
    username: str
    viewed: int = 0
    liked: int = 0
<<<<<<< HEAD
    followed: int = 0
=======
>>>>>>> origin/main
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
<<<<<<< HEAD
            subsystem="interactions",
=======
>>>>>>> origin/main
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


<<<<<<< HEAD
def _build_like_progress_targets(likes_target: int) -> list[float]:
    return _build_progress_targets(
        likes_target,
        floor=_LIKE_SESSION_PROGRESS_FLOOR,
        ceiling=_LIKE_SESSION_PROGRESS_CEILING,
    )


def _build_follow_progress_targets(follows_target: int) -> list[float]:
    return _build_progress_targets(
        follows_target,
        floor=_FOLLOW_SESSION_PROGRESS_FLOOR,
        ceiling=_FOLLOW_SESSION_PROGRESS_CEILING,
    )


def _build_progress_targets(target_count: int, *, floor: float, ceiling: float) -> list[float]:
    clean_target = max(0, int(target_count or 0))
    if clean_target <= 0:
        return []
    span = max(0.01, ceiling - floor)
    min_gap = min(0.18, span / max(clean_target + 1, 2) * 0.45)
    seeds = [
        max(
            floor,
            min(
                ceiling,
                ((index + 1) / (clean_target + 1)) + random.uniform(-0.08, 0.08),
            ),
        )
        for index in range(clean_target)
    ]
    seeds.sort()
    targets: list[float] = []
    for index, seed in enumerate(seeds):
        remaining = clean_target - index - 1
        lower_bound = floor if not targets else targets[-1] + min_gap
        upper_bound = ceiling - (remaining * min_gap)
        if lower_bound > upper_bound:
            lower_bound = upper_bound
        targets.append(min(max(seed, lower_bound), upper_bound))
    return targets


def _profile_url_from_href(href: str) -> str:
    raw = str(href or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        path = urlparse(raw).path
    else:
        path = raw
    clean_path = str(path or "").split("?", 1)[0].split("#", 1)[0].strip()
    if not clean_path.startswith("/"):
        return ""
    parts = [part for part in clean_path.split("/") if part]
    if not parts:
        return ""
    username = str(parts[0] or "").strip()
    if not username or username.lower() in _PROFILE_PATH_BLOCKLIST:
        return ""
    if len(parts) > 1:
        tail = [str(part or "").strip().lower() for part in parts[1:] if str(part or "").strip()]
        if not tail:
            pass
        elif tail[0] not in _PROFILE_SECONDARY_SEGMENTS:
            return ""
    if re.fullmatch(r"[A-Za-z0-9._]+", username) is None:
        return ""
    return f"https://www.instagram.com/{username}/"


async def _document_profile_urls(page) -> list[str]:
    with contextlib.suppress(Exception):
        hrefs = await page.evaluate(
            """
            () => Array.from(document.querySelectorAll('a[href]'))
              .map((node) => node.getAttribute('href') || '')
              .filter(Boolean)
            """
        )
        if isinstance(hrefs, list):
            urls: list[str] = []
            seen: set[str] = set()
            for href in hrefs:
                profile_url = _profile_url_from_href(str(href or ""))
                if not profile_url or profile_url in seen:
                    continue
                seen.add(profile_url)
                urls.append(profile_url)
            if urls:
                return urls
    with contextlib.suppress(Exception):
        content = await page.content()
        matches = re.findall(r'''href=["']([^"']+)["']''', str(content or ""))
        urls = []
        seen: set[str] = set()
        for href in matches:
            profile_url = _profile_url_from_href(str(href or ""))
            if not profile_url or profile_url in seen:
                continue
            seen.add(profile_url)
            urls.append(profile_url)
        if urls:
            return urls
    return []


async def _current_reel_author_urls(page) -> list[str]:
    selectors = (
        "main header a[href]",
        "main article header a[href]",
        "main a[href]",
        "body header a[href]",
        "article a[href]",
        "body a[href]",
    )
    urls: list[str] = []
    seen: set[str] = set()
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = min(await locator.count(), 10)
        except Exception:
            continue
        for index in range(count):
            try:
                href = await locator.nth(index).get_attribute("href")
            except Exception:
                continue
            profile_url = _profile_url_from_href(str(href or ""))
            if not profile_url or profile_url in seen:
                continue
            seen.add(profile_url)
            urls.append(profile_url)
    if urls:
        return urls
    return await _document_profile_urls(page)


async def _follow_profile_page(profile_page) -> tuple[bool, str]:
    await _dismiss_popups_async(profile_page)
    for selector in (
        "button:has-text('Following')",
        "button:has-text('Siguiendo')",
        "button:has-text('Requested')",
        "button:has-text('Solicitado')",
    ):
        with contextlib.suppress(Exception):
            locator = profile_page.locator(selector).first
            if await locator.count() > 0:
                return False, "already_following"
    for selector in (
        "button:has-text('Follow')",
        "button:has-text('Seguir')",
        "button:has-text('Follow back')",
        "button:has-text('Seguir tambien')",
        "div[role='button']:has-text('Follow')",
        "div[role='button']:has-text('Seguir')",
    ):
        try:
            locator = profile_page.locator(selector).first
            if await locator.count() <= 0:
                continue
            await locator.click(timeout=3_000)
            await _sleep_with_stop_async(random.uniform(1.2, 2.2))
            return True, ""
        except Exception:
            continue
    return False, "follow_button_not_found"


async def _try_follow_current_reel_author(page, attempted_profiles: set[str]) -> tuple[bool, str]:
    candidates = [
        profile_url
        for profile_url in await _current_reel_author_urls(page)
        if profile_url not in attempted_profiles
    ]
    if not candidates:
        return False, "author_profile_not_found"
    last_reason = "follow_not_attempted"
    for profile_url in candidates:
        attempted_profiles.add(profile_url)
        profile_page = None
        try:
            profile_page = await page.context.new_page()
            profile_page.set_default_timeout(15_000)
            await profile_page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
            ok, reason = await _follow_profile_page(profile_page)
            if ok:
                return True, ""
            last_reason = reason or last_reason
        except Exception as exc:
            last_reason = _short_message(exc, limit=120)
        finally:
            if profile_page is not None:
                with contextlib.suppress(Exception):
                    await profile_page.close()
    return False, last_reason


=======
>>>>>>> origin/main
async def _run_reels_for_account(
    *,
    page,
    summary: ReelsPlaywrightSummary,
    duration_s: int,
    likes_target: int,
<<<<<<< HEAD
    follows_target: int = 0,
) -> None:
    total_duration = max(1.0, float(duration_s or 0))
    session_start = time.monotonic()
    end = session_start + total_duration
    like_targets = _build_like_progress_targets(likes_target)
    follow_targets = _build_follow_progress_targets(follows_target)
    next_like_target_index = 0
    next_follow_target_index = 0
    last_liked_view = -999
    last_followed_view = -999
    attempted_profiles: set[str] = set()
    last_follow_reason = ""
=======
) -> None:
    end = time.monotonic() + max(1.0, float(duration_s or 0))
>>>>>>> origin/main
    await _dismiss_popups_async(page)
    await _sleep_with_stop_async(random.uniform(1.5, 2.5))
    while time.monotonic() < end:
        if STOP_EVENT.is_set():
            return
<<<<<<< HEAD
        remaining_before_view = max(0.0, end - time.monotonic())
        if remaining_before_view <= 1.0:
            break
        summary.viewed += 1
        current_view = summary.viewed
        watch_s = min(random.uniform(_REEL_MIN_SECONDS, _REEL_MAX_SECONDS), remaining_before_view)
        progress_at_view_end = min(
            1.0,
            max(0.0, ((time.monotonic() - session_start) + watch_s) / total_duration),
        )
        should_attempt_like = (
            next_like_target_index < len(like_targets)
            and progress_at_view_end >= like_targets[next_like_target_index]
            and (current_view - last_liked_view) > 1
        )
        should_attempt_follow = (
            next_follow_target_index < len(follow_targets)
            and progress_at_view_end >= follow_targets[next_follow_target_index]
            and (current_view - last_followed_view) > 1
        )
        like_delay = None
        if should_attempt_like and watch_s > 1.2:
            like_delay = min(
                max(0.8, watch_s * random.uniform(0.25, 0.7)),
                max(0.8, watch_s - 0.4),
            )
        first_pause = like_delay if like_delay is not None else watch_s
        await _sleep_with_stop_async(first_pause)
        if STOP_EVENT.is_set() or time.monotonic() >= end:
            break
        action_taken_on_view = False
        if should_attempt_like and like_delay is not None:
            with contextlib.suppress(Exception):
                if await _try_like_current_reel(page):
                    summary.liked += 1
                    last_liked_view = current_view
                    next_like_target_index += 1
                    action_taken_on_view = True
        if (
            not action_taken_on_view
            and should_attempt_follow
            and max(0.0, end - time.monotonic()) >= 3.0
        ):
            with contextlib.suppress(Exception):
                followed_ok, follow_reason = await _try_follow_current_reel_author(page, attempted_profiles)
                if followed_ok:
                    summary.followed += 1
                    last_followed_view = current_view
                    next_follow_target_index += 1
                    action_taken_on_view = True
                    last_follow_reason = ""
                elif follow_reason:
                    last_follow_reason = follow_reason
        remaining_watch = max(0.0, watch_s - first_pause)
        if remaining_watch > 0:
            await _sleep_with_stop_async(min(remaining_watch, max(0.0, end - time.monotonic())))
        if STOP_EVENT.is_set() or time.monotonic() >= end:
            break
=======
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
>>>>>>> origin/main
        await _sleep_with_stop_async(
            min(random.uniform(0.4, 1.2), max(0.0, end - time.monotonic()))
        )
        if STOP_EVENT.is_set() or time.monotonic() >= end:
<<<<<<< HEAD
            break
=======
            return
>>>>>>> origin/main
        await _next_reel(page)
        await _sleep_with_stop_async(
            min(random.uniform(1.2, 2.4), max(0.0, end - time.monotonic()))
        )
<<<<<<< HEAD
    if likes_target > 0 and summary.liked < likes_target:
        summary.messages.append(
            f"Likes completados parcialmente: {summary.liked}/{max(0, int(likes_target or 0))}."
        )
    if follows_target > 0 and summary.followed < follows_target:
        message = f"Follows completados parcialmente: {summary.followed}/{max(0, int(follows_target or 0))}."
        if last_follow_reason:
            message += f" Motivo final: {last_follow_reason}."
        summary.messages.append(message)
=======
>>>>>>> origin/main


def run_from_menu(alias: str) -> None:
    banner()
    print(style_text("Interacciones (Ver & Like Reels)", color=Fore.CYAN, bold=True))
    print(full_line())

    chosen_accounts = _select_accounts_playwright(alias)
    if not chosen_accounts:
        return

    minutes = ask_int("Tiempo de navegacion por cuenta (min): ", min_value=1, default=10)
    likes_target = ask_int("Cantidad de likes por cuenta: ", min_value=0, default=10)
<<<<<<< HEAD
    follows_target = ask_int("Cantidad de follows por cuenta: ", min_value=0, default=0)
    print("Cada reel se vera entre 5s y 45s (random).")
=======
    print("Cada reel se vera entre 25s y 50s (random).")
>>>>>>> origin/main

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
<<<<<<< HEAD
                    follows_target=follows_target,
=======
>>>>>>> origin/main
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
<<<<<<< HEAD
                (
                    f"@{summary.username}: vistos={summary.viewed} "
                    f"likes={summary.liked} follows={summary.followed} errores={summary.errors}"
                ),
=======
                f"@{summary.username}: vistos={summary.viewed} likes={summary.liked} errores={summary.errors}",
>>>>>>> origin/main
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
