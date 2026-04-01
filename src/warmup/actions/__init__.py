from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

from src.opt_in import human_engine
from src.auth.persistent_login import ensure_logged_in_async
from src.browser_profile_paths import browser_profile_dir, browser_storage_state_path
from src.browser_telemetry import log_browser_stage
from src.playwright_service import BASE_PROFILES, PlaywrightService
from src.transport.session_manager import ManagedSession, SessionManager


ACTION_LABELS: dict[str, str] = {
    "watch_reels": "Ver reels",
    "like_posts": "Dar likes",
    "follow_accounts": "Seguir cuentas",
    "comment_post": "Comentar post",
    "reply_story": "Responder historia",
    "send_message": "Enviar mensaje",
}
_SESSION_MANAGERS: dict[bool, SessionManager] = {}


@dataclass(frozen=True)
class WarmupActionContext:
    account: dict[str, Any]
    payload: dict[str, Any]
    base_profiles: Path = Path(BASE_PROFILES)
    headless: bool = True
    action_type: str = ""

    @property
    def username(self) -> str:
        return str(self.account.get("username") or "").strip().lstrip("@")

    @property
    def profile_dir(self) -> Path:
        return browser_profile_dir(self.username, profiles_root=self.base_profiles)

    @property
    def storage_state_path(self) -> Path:
        return browser_storage_state_path(self.username, profiles_root=self.base_profiles)


@dataclass
class WarmupActionResult:
    ok: bool = True
    performed: int = 0
    message: str = ""
    details: list[str] = field(default_factory=list)

    def add_detail(self, detail: str) -> None:
        text = str(detail or "").strip()
        if text:
            self.details.append(text)
            if not self.message:
                self.message = text


def action_label(action_type: str) -> str:
    clean_type = str(action_type or "").strip().lower()
    return ACTION_LABELS.get(clean_type, clean_type or "Accion")


def normalize_targets(raw: Any) -> list[str]:
    text = str(raw or "").replace("\r", "\n")
    values: list[str] = []
    seen: set[str] = set()
    for chunk in text.replace(",", "\n").split("\n"):
        value = str(chunk or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(value)
    return values


async def human_pause(seconds: float) -> None:
    await asyncio.sleep(max(0.0, float(seconds or 0.0)))


async def open_profile(page: Any, username: str) -> None:
    await page.goto(
        f"https://www.instagram.com/{str(username or '').strip().lstrip('@')}/",
        wait_until="domcontentloaded",
    )
    await human_engine.wait_for_navigation_idle(page)


def _session_manager_for(headless: bool) -> SessionManager:
    key = bool(headless)
    manager = _SESSION_MANAGERS.get(key)
    if manager is None:
        manager = SessionManager(
            headless=key,
            keep_browser_open_per_account=True,
            profiles_root=str(BASE_PROFILES),
            normalize_username=lambda value: str(value or "").strip().lstrip("@"),
            log_event=lambda *_args, **_kwargs: None,
            subsystem="warmup",
        )
        _SESSION_MANAGERS[key] = manager
    return manager


@asynccontextmanager
async def account_page(context: WarmupActionContext) -> AsyncIterator[tuple[PlaywrightService, Any, Any]]:
    username = context.username
    if not username:
        raise RuntimeError("Cuenta invalida para warm up.")
    storage_state = context.storage_state_path
    if not storage_state.exists():
        raise RuntimeError(f"Falta storage_state.json para @{username}.")
    from src.auth.onboarding import build_proxy
    from src.proxy_payload import build_proxy_input_from_account

    proxy_input = build_proxy_input_from_account(context.account)
    proxy_payload = build_proxy(proxy_input) if proxy_input else None
    session_manager = _session_manager_for(bool(context.headless))
    session: ManagedSession | None = None
    navigation_owner = f"warmup:{str(context.action_type or 'action').strip().lower() or 'action'}"
    navigation_acquired = False
    try:
        log_browser_stage(
            component="warmup_action",
            stage="spawn",
            status="started",
            account=username,
        )
        session = await session_manager.open_session(
            account=context.account,
            proxy=proxy_payload,
            login_func=ensure_logged_in_async,
        )
        browser_context = session.ctx
        page = session.page
        if isinstance(session, ManagedSession):
            await session_manager.acquire_navigation(session, navigation_owner, 45.0)
            navigation_acquired = True
        page.set_default_timeout(20_000)
        log_browser_stage(
            component="warmup_action",
            stage="workspace_ready",
            status="ok",
            account=username,
            url=str(getattr(page, "url", "") or ""),
        )
        yield session.svc, browser_context, page
    except Exception as exc:
        await session_manager.discard_if_unhealthy(
            session,
            exc,
            is_fatal_error=lambda error: "closed" in str(error or "").lower(),
        )
        raise
    finally:
        current_url = ""
        if session is not None:
            if navigation_acquired:
                await session.release_navigation_async(navigation_owner)
            with contextlib.suppress(Exception):
                await session_manager.save_storage_state(session, username)
            with contextlib.suppress(Exception):
                current_url = str(getattr(session.page, "url", "") or "")
            with contextlib.suppress(Exception):
                await session_manager.finalize_session(session, current_url=current_url)
