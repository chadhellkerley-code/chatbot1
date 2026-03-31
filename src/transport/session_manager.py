from __future__ import annotations

import asyncio
import os
<<<<<<< HEAD
import re
import threading
import time
import uuid
from contextlib import contextmanager
=======
import threading
import time
import uuid
>>>>>>> origin/main
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from playwright.async_api import Page

from core.proxy_preflight import account_proxy_preflight
from core.proxy_registry import ProxyResolutionError
<<<<<<< HEAD
from src.browser_profile_lifecycle import emit_profile_lifecycle_diagnostic, mark_profile_unclean_shutdown
from src.browser_profile_paths import browser_storage_state_path
from src.browser_telemetry import log_browser_stage
from src.playwright_service import PlaywrightService
from src.runtime.playwright_runtime import PersistentProfileOwnershipError, run_coroutine_sync
=======
from src.browser_profile_paths import browser_storage_state_path
from src.browser_telemetry import log_browser_stage
from src.playwright_service import PlaywrightService
from src.runtime.playwright_runtime import run_coroutine_sync
>>>>>>> origin/main

SessionLoginFunc = Callable[..., Awaitable[Tuple[PlaywrightService, Any, Page]]]


<<<<<<< HEAD
def _normalize_subsystem_label(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", str(value or "").strip().lower()).strip("_")
    return cleaned or "default"


def _normalize_navigation_owner_type(owner: str) -> str:
    text = str(owner or "").strip().lower()
    if not text:
        return "unknown"
    match = re.split(r"[:/\s]+", text, maxsplit=1)
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", str(match[0] or "").strip()).strip("_")
    return cleaned or "unknown"


class NavigationLockedError(RuntimeError):
    def __init__(
        self,
        *,
        owner: str,
        owner_type: str,
        current_owner: str,
        current_owner_type: str,
        username: str,
        subsystem: str,
        timeout: float,
        acquired_at: float,
    ) -> None:
        self.owner = str(owner or "").strip()
        self.owner_type = str(owner_type or "").strip()
        self.current_owner = str(current_owner or "").strip()
        self.current_owner_type = str(current_owner_type or "").strip()
        self.username = str(username or "").strip()
        self.subsystem = str(subsystem or "").strip()
        self.timeout = max(0.0, float(timeout or 0.0))
        self.acquired_at = float(acquired_at or 0.0)
        super().__init__(
            f"navigation_locked owner={self.owner or 'unknown'} "
            f"navigation_in_use_by={self.current_owner or 'unknown'} "
            f"navigation_timeout={self.timeout:.2f}"
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "navigation_locked": True,
            "navigation_in_use_by": self.current_owner,
            "navigation_owner": self.owner,
            "navigation_owner_type": self.owner_type,
            "navigation_current_owner_type": self.current_owner_type,
            "navigation_timeout": self.timeout,
            "navigation_acquired_at": self.acquired_at,
            "subsystem": self.subsystem,
            "account": self.username,
        }


class NavigationOwnershipError(RuntimeError):
    def __init__(
        self,
        *,
        owner: str,
        current_owner: str,
        username: str,
        subsystem: str,
    ) -> None:
        self.owner = str(owner or "").strip()
        self.current_owner = str(current_owner or "").strip()
        self.username = str(username or "").strip()
        self.subsystem = str(subsystem or "").strip()
        super().__init__(
            f"navigation_release_owner_mismatch owner={self.owner or 'unknown'} "
            f"current_owner={self.current_owner or 'unknown'}"
        )


@dataclass
class _NavigationState:
    current_owner: str = ""
    owner_type: str = ""
    acquired_at: float = 0.0
    depth: int = 0
    retired: bool = False
    retired_reason: str = ""
    condition: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        self.condition = threading.Condition(threading.RLock())


=======
>>>>>>> origin/main
@dataclass(frozen=True)
class ManagedSession:
    key: str
    svc: PlaywrightService
    ctx: Any
    page: Page
    persistent: bool
    reused: bool
    lease_id: str = ""
    pool_key: str = ""
<<<<<<< HEAD
    _acquire_navigation_sync: Callable[[str, float], dict[str, Any]] | None = field(default=None, repr=False, compare=False)
    _acquire_navigation_async: Callable[[str, float], Awaitable[dict[str, Any]]] | None = field(default=None, repr=False, compare=False)
    _release_navigation_sync: Callable[[str], None] | None = field(default=None, repr=False, compare=False)
    _release_navigation_async: Callable[[str], Awaitable[None]] | None = field(default=None, repr=False, compare=False)
    _navigation_metadata: Callable[[], dict[str, Any]] | None = field(default=None, repr=False, compare=False)

    def acquire_navigation(self, owner: str, timeout: float) -> dict[str, Any]:
        if self._acquire_navigation_sync is None:
            raise RuntimeError("navigation_acquire_unavailable")
        return dict(self._acquire_navigation_sync(str(owner or ""), float(timeout or 0.0)))

    async def acquire_navigation_async(self, owner: str, timeout: float) -> dict[str, Any]:
        if self._acquire_navigation_async is None:
            return self.acquire_navigation(owner, timeout)
        return dict(await self._acquire_navigation_async(str(owner or ""), float(timeout or 0.0)))

    def release_navigation(self, owner: str) -> None:
        if self._release_navigation_sync is None:
            raise RuntimeError("navigation_release_unavailable")
        self._release_navigation_sync(str(owner or ""))

    async def release_navigation_async(self, owner: str) -> None:
        if self._release_navigation_async is None:
            self.release_navigation(owner)
            return
        await self._release_navigation_async(str(owner or ""))

    def navigation_metadata(self) -> dict[str, Any]:
        if self._navigation_metadata is None:
            return {
                "current_owner": "",
                "owner_type": "",
                "timestamp": 0.0,
            }
        return dict(self._navigation_metadata())
=======
>>>>>>> origin/main


@dataclass
class _SessionEntry:
    key: str
    svc: PlaywrightService
    ctx: Any
    page: Page
    proxy_key: str
    sticky_owners: set[str] = field(default_factory=set)
    leases: dict[str, str] = field(default_factory=dict)
<<<<<<< HEAD
    navigation: _NavigationState = field(default_factory=_NavigationState)
=======
>>>>>>> origin/main

    @property
    def persistent(self) -> bool:
        return bool(self.sticky_owners)


@dataclass
class _OpenState:
    proxy_key: str
    event: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


class SessionManager:
    _STAY_OPEN_TOKENS = (
        "accounts/suspended",
        "two_factor",
        "challenge",
        "checkpoint",
        "accounts/confirm_email",
    )
    _GLOBAL_LOCK = threading.RLock()
    _SHARED_SESSIONS: Dict[str, _SessionEntry] = {}
    _OPENING: Dict[str, _OpenState] = {}

    def __init__(
        self,
        *,
        headless: bool,
        keep_browser_open_per_account: bool,
        profiles_root: str,
        normalize_username: Callable[[str], str],
        log_event: Callable[..., None],
<<<<<<< HEAD
        subsystem: str = "default",
=======
>>>>>>> origin/main
    ) -> None:
        self._headless = bool(headless)
        self._persistent = bool(keep_browser_open_per_account)
        self._profiles_root = str(profiles_root)
        self._normalize_username = normalize_username
        self._log_event = log_event
<<<<<<< HEAD
        self._subsystem = _normalize_subsystem_label(subsystem)
=======
>>>>>>> origin/main
        self._manager_id = uuid.uuid4().hex
        self._held_leases: Dict[str, str] = {}
        env_name = (
            "PLAYWRIGHT_HEADLESS_SESSION_OPEN_TIMEOUT_SECONDS"
            if self._headless
            else "PLAYWRIGHT_HEADFUL_SESSION_OPEN_TIMEOUT_SECONDS"
        )
        fallback = 150.0 if self._headless else 240.0
        try:
            self._default_open_timeout_seconds = max(15.0, float(os.getenv(env_name, str(fallback)) or fallback))
        except Exception:
            self._default_open_timeout_seconds = fallback

    def session_key(self, username: str) -> str:
        return self._normalize_username(username).lower()

    def _pool_key(self, key: str) -> str:
<<<<<<< HEAD
        return f"{'headless' if self._headless else 'headful'}:{self._subsystem}:{key}"
=======
        return f"{'headless' if self._headless else 'headful'}:{key}"
>>>>>>> origin/main

    @staticmethod
    def page_closed(page: Optional[Page]) -> bool:
        if page is None:
            return True
        try:
            return bool(page.is_closed())
        except Exception:
            return True

    @staticmethod
    def context_closed(ctx: Any) -> bool:
        if ctx is None:
            return True
        checker = getattr(ctx, "is_closed", None)
        if callable(checker):
            try:
                return bool(checker())
            except Exception:
                return True
        browser = getattr(ctx, "browser", None)
        if browser is not None:
            connected = getattr(browser, "is_connected", None)
            if callable(connected):
                try:
                    return not bool(connected())
                except Exception:
                    return True
        try:
            _ = list(getattr(ctx, "pages", []) or [])
            return False
        except Exception:
            return True

    @staticmethod
    def proxy_signature(proxy: Optional[Dict[str, Any]]) -> str:
        if not proxy or not isinstance(proxy, dict):
            return ""
        server = str(proxy.get("server") or proxy.get("url") or proxy.get("proxy") or "").strip().lower()
        username = str(proxy.get("username") or proxy.get("user") or "").strip().lower()
        password = str(proxy.get("password") or proxy.get("pass") or "").strip()
        return "|".join((server, username, password))

    def _entry_is_usable(self, entry: Optional[_SessionEntry], *, proxy_key: str) -> bool:
        if entry is None or entry.proxy_key != proxy_key:
            return False
        if self.page_closed(entry.page):
            return False
        if self.context_closed(entry.ctx):
            return False
        return True

    def _attach_lease(self, pool_key: str, entry: _SessionEntry) -> str:
        lease_id = uuid.uuid4().hex
        entry.leases[lease_id] = self._manager_id
        self._held_leases[lease_id] = pool_key
        if self._persistent:
            entry.sticky_owners.add(self._manager_id)
        return lease_id

<<<<<<< HEAD
    def _navigation_metadata(self, entry: _SessionEntry) -> dict[str, Any]:
        with entry.navigation.condition:
            return {
                "current_owner": str(entry.navigation.current_owner or ""),
                "owner_type": str(entry.navigation.owner_type or ""),
                "timestamp": float(entry.navigation.acquired_at or 0.0),
            }

    def _emit_navigation_event(
        self,
        event_name: str,
        *,
        username: str,
        owner: str,
        owner_type: str,
        current_owner: str = "",
        current_owner_type: str = "",
        timeout: float = 0.0,
        acquired_at: float = 0.0,
        depth: int = 0,
        reason: str = "",
    ) -> None:
        payload = {
            "account": username,
            "subsystem": self._subsystem,
            "owner": owner,
            "owner_type": owner_type,
            "current_owner": current_owner,
            "current_owner_type": current_owner_type,
            "navigation_in_use_by": current_owner,
            "navigation_timeout": max(0.0, float(timeout or 0.0)),
            "timestamp": float(acquired_at or time.time()),
            "depth": int(depth or 0),
            "reason": reason,
        }
        self._log_event(event_name, **payload)

    def _resolve_navigation_entry(self, session: ManagedSession) -> _SessionEntry:
        pool_key = str(session.pool_key or self._held_leases.get(session.lease_id) or self._pool_key(session.key))
        with self._GLOBAL_LOCK:
            entry = self._SHARED_SESSIONS.get(pool_key)
        if entry is None:
            raise RuntimeError("navigation_session_missing")
        return entry

    def acquire_navigation_sync(self, session: ManagedSession, owner: str, timeout: float) -> dict[str, Any]:
        entry = self._resolve_navigation_entry(session)
        clean_owner = str(owner or "").strip()
        if not clean_owner:
            raise ValueError("navigation_owner_required")
        owner_type = _normalize_navigation_owner_type(clean_owner)
        timeout_seconds = max(0.0, float(timeout or 0.0))
        deadline = time.monotonic() + timeout_seconds
        conflict_emitted = False
        while True:
            with entry.navigation.condition:
                if entry.navigation.retired:
                    raise RuntimeError(entry.navigation.retired_reason or "navigation_session_retired")
                if self.page_closed(entry.page) or self.context_closed(entry.ctx):
                    raise RuntimeError("navigation_session_unavailable")
                if not entry.navigation.current_owner or entry.navigation.current_owner == clean_owner:
                    if not entry.navigation.current_owner:
                        entry.navigation.current_owner = clean_owner
                        entry.navigation.owner_type = owner_type
                        entry.navigation.acquired_at = time.time()
                    entry.navigation.depth = max(0, int(entry.navigation.depth or 0)) + 1
                    meta = {
                        "current_owner": entry.navigation.current_owner,
                        "owner_type": entry.navigation.owner_type,
                        "timestamp": entry.navigation.acquired_at,
                    }
                    self._emit_navigation_event(
                        "navigation_acquired",
                        username=entry.key,
                        owner=clean_owner,
                        owner_type=entry.navigation.owner_type,
                        current_owner=entry.navigation.current_owner,
                        current_owner_type=entry.navigation.owner_type,
                        acquired_at=entry.navigation.acquired_at,
                        depth=entry.navigation.depth,
                    )
                    return meta
                current_owner = str(entry.navigation.current_owner or "")
                current_owner_type = str(entry.navigation.owner_type or "")
                acquired_at = float(entry.navigation.acquired_at or 0.0)
                remaining = max(0.0, deadline - time.monotonic())
                if not conflict_emitted:
                    self._emit_navigation_event(
                        "navigation_conflict",
                        username=entry.key,
                        owner=clean_owner,
                        owner_type=owner_type,
                        current_owner=current_owner,
                        current_owner_type=current_owner_type,
                        timeout=timeout_seconds,
                        acquired_at=acquired_at,
                        depth=entry.navigation.depth,
                        reason="navigation_in_use",
                    )
                    conflict_emitted = True
                if remaining <= 0.0:
                    self._emit_navigation_event(
                        "navigation_timeout",
                        username=entry.key,
                        owner=clean_owner,
                        owner_type=owner_type,
                        current_owner=current_owner,
                        current_owner_type=current_owner_type,
                        timeout=timeout_seconds,
                        acquired_at=acquired_at,
                        depth=entry.navigation.depth,
                        reason="navigation_lock_timeout",
                    )
                    raise NavigationLockedError(
                        owner=clean_owner,
                        owner_type=owner_type,
                        current_owner=current_owner,
                        current_owner_type=current_owner_type,
                        username=entry.key,
                        subsystem=self._subsystem,
                        timeout=timeout_seconds,
                        acquired_at=acquired_at,
                    )
                entry.navigation.condition.wait(timeout=min(0.1, remaining))

    async def acquire_navigation(self, session: ManagedSession, owner: str, timeout: float) -> dict[str, Any]:
        return await asyncio.to_thread(self.acquire_navigation_sync, session, owner, timeout)

    def release_navigation_sync(self, session: ManagedSession, owner: str) -> None:
        try:
            entry = self._resolve_navigation_entry(session)
        except RuntimeError as exc:
            if str(exc) == "navigation_session_missing":
                return
            raise
        clean_owner = str(owner or "").strip()
        if not clean_owner:
            raise ValueError("navigation_owner_required")
        released_at = time.time()
        with entry.navigation.condition:
            if entry.navigation.retired:
                return
            current_owner = str(entry.navigation.current_owner or "")
            current_owner_type = str(entry.navigation.owner_type or "")
            if current_owner != clean_owner:
                self._emit_navigation_event(
                    "navigation_conflict",
                    username=entry.key,
                    owner=clean_owner,
                    owner_type=_normalize_navigation_owner_type(clean_owner),
                    current_owner=current_owner,
                    current_owner_type=current_owner_type,
                    acquired_at=float(entry.navigation.acquired_at or 0.0),
                    depth=entry.navigation.depth,
                    reason="release_owner_mismatch",
                )
                raise NavigationOwnershipError(
                    owner=clean_owner,
                    current_owner=current_owner,
                    username=entry.key,
                    subsystem=self._subsystem,
                )
            entry.navigation.depth = max(0, int(entry.navigation.depth or 0) - 1)
            released = entry.navigation.depth == 0
            if released:
                owner_type = str(entry.navigation.owner_type or "")
                entry.navigation.current_owner = ""
                entry.navigation.owner_type = ""
                entry.navigation.acquired_at = 0.0
                entry.navigation.condition.notify_all()
            else:
                owner_type = current_owner_type
        self._emit_navigation_event(
            "navigation_released",
            username=entry.key,
            owner=clean_owner,
            owner_type=owner_type,
            acquired_at=released_at,
            depth=0 if released else entry.navigation.depth,
        )

    async def release_navigation(self, session: ManagedSession, owner: str) -> None:
        await asyncio.to_thread(self.release_navigation_sync, session, owner)

    def _retire_navigation(self, entry: Optional[_SessionEntry], *, reason: str) -> None:
        if entry is None:
            return
        with entry.navigation.condition:
            entry.navigation.retired = True
            entry.navigation.retired_reason = str(reason or "navigation_session_retired")
            entry.navigation.current_owner = ""
            entry.navigation.owner_type = ""
            entry.navigation.acquired_at = 0.0
            entry.navigation.depth = 0
            entry.navigation.condition.notify_all()

    @staticmethod
    def _signal_open_state(state: Optional[_OpenState], error: BaseException | None = None) -> None:
        if state is None:
            return
        if error is not None and state.error is None:
            state.error = error
        state.event.set()

=======
>>>>>>> origin/main
    async def open_session(
        self,
        *,
        account: Dict[str, Any],
        proxy: Optional[Dict[str, Any]],
        login_func: SessionLoginFunc,
        deadline: float | None = None,
    ) -> ManagedSession:
        username = str(account.get("username") or "").strip()
        key = self.session_key(username)
        pool_key = self._pool_key(key)
        proxy_key = self.proxy_signature(proxy)
        log_browser_stage(
            component="playwright_session_manager",
            stage="session_open_start",
            status="started",
            account=username,
            persistent=self._persistent,
            proxy_configured=bool(proxy_key),
        )
        while True:
            stale_entry: _SessionEntry | None = None
            wait_state: _OpenState | None = None
            should_open = False
            with self._GLOBAL_LOCK:
                entry = self._SHARED_SESSIONS.get(pool_key)
                if entry is not None and not self._entry_is_usable(entry, proxy_key=proxy_key):
                    stale_entry = self._SHARED_SESSIONS.pop(pool_key, None)
                    if stale_entry is not None and stale_entry.proxy_key != proxy_key:
                        self._log_event(
                            "SESSION_PROXY_REFRESH",
                            key=key,
                            previous_proxy=stale_entry.proxy_key,
                            current_proxy=proxy_key,
                        )
                entry = self._SHARED_SESSIONS.get(pool_key)
                if self._entry_is_usable(entry, proxy_key=proxy_key):
                    assert entry is not None
                    lease_id = self._attach_lease(pool_key, entry)
                    self._log_event("SESSION_REUSE", key=key, persistent=entry.persistent, url=entry.page.url if entry.page else "")
                    log_browser_stage(
                        component="playwright_session_manager",
                        stage="session_open_end",
                        status="reused",
                        account=username,
                        url=entry.page.url if entry.page else "",
                        persistent=entry.persistent,
                    )
                    log_browser_stage(
                        component="playwright_session_manager",
                        stage="browser_open",
                        status="reused",
                        account=username,
                        url=entry.page.url if entry.page else "",
                        persistent=entry.persistent,
                    )
                    return ManagedSession(
                        key=key,
                        svc=entry.svc,
                        ctx=entry.ctx,
                        page=entry.page,
                        persistent=entry.persistent,
                        reused=True,
                        lease_id=lease_id,
                        pool_key=pool_key,
<<<<<<< HEAD
                        _acquire_navigation_sync=lambda owner, timeout, _session_key=pool_key: self.acquire_navigation_sync(
                            ManagedSession(
                                key=key,
                                svc=entry.svc,
                                ctx=entry.ctx,
                                page=entry.page,
                                persistent=entry.persistent,
                                reused=True,
                                lease_id=lease_id,
                                pool_key=_session_key,
                            ),
                            owner,
                            timeout,
                        ),
                        _acquire_navigation_async=lambda owner, timeout, _session_key=pool_key: self.acquire_navigation(
                            ManagedSession(
                                key=key,
                                svc=entry.svc,
                                ctx=entry.ctx,
                                page=entry.page,
                                persistent=entry.persistent,
                                reused=True,
                                lease_id=lease_id,
                                pool_key=_session_key,
                            ),
                            owner,
                            timeout,
                        ),
                        _release_navigation_sync=lambda owner, _session_key=pool_key: self.release_navigation_sync(
                            ManagedSession(
                                key=key,
                                svc=entry.svc,
                                ctx=entry.ctx,
                                page=entry.page,
                                persistent=entry.persistent,
                                reused=True,
                                lease_id=lease_id,
                                pool_key=_session_key,
                            ),
                            owner,
                        ),
                        _release_navigation_async=lambda owner, _session_key=pool_key: self.release_navigation(
                            ManagedSession(
                                key=key,
                                svc=entry.svc,
                                ctx=entry.ctx,
                                page=entry.page,
                                persistent=entry.persistent,
                                reused=True,
                                lease_id=lease_id,
                                pool_key=_session_key,
                            ),
                            owner,
                        ),
                        _navigation_metadata=lambda: self._navigation_metadata(entry),
=======
>>>>>>> origin/main
                    )
                open_state = self._OPENING.get(pool_key)
                if open_state is not None and open_state.event.is_set() and open_state.error is not None:
                    self._OPENING.pop(pool_key, None)
                    open_state = None
                if open_state is None:
                    self._OPENING[pool_key] = _OpenState(proxy_key=proxy_key)
                    should_open = True
<<<<<<< HEAD
                else:
                    # Only one opener may own a pool key at a time. Callers
                    # waiting on a different proxy reopen after the current
                    # attempt finishes, which avoids cross-proxy result races.
                    wait_state = open_state
            if stale_entry is not None:
                self._retire_navigation(stale_entry, reason="stale_session_replaced")
=======
                elif open_state.proxy_key == proxy_key:
                    wait_state = open_state
                else:
                    self._OPENING[pool_key] = _OpenState(proxy_key=proxy_key)
                    should_open = True
            if stale_entry is not None:
>>>>>>> origin/main
                await self._close_session_entry(stale_entry)
            if should_open:
                break
            if wait_state is None:
                continue
            await self._wait_for_open_state(wait_state.event, deadline=deadline)
            with self._GLOBAL_LOCK:
                entry = self._SHARED_SESSIONS.get(pool_key)
                if self._entry_is_usable(entry, proxy_key=proxy_key):
                    continue
                open_state = self._OPENING.get(pool_key)
                if open_state is wait_state and open_state.event.is_set() and open_state.error is not None:
                    self._OPENING.pop(pool_key, None)
<<<<<<< HEAD
                    if isinstance(open_state.error, Exception):
                        raise open_state.error
=======
>>>>>>> origin/main
                    raise RuntimeError(str(open_state.error) or type(open_state.error).__name__) from open_state.error

        timeout_seconds = self._resolve_open_timeout(deadline=deadline)
        action_account = dict(account or {})
        action_account.setdefault("reuse_session_only", True)
<<<<<<< HEAD
        action_account.setdefault(
            "validate_reused_session",
            False if self._subsystem == "campaign" else True,
        )
        action_account.setdefault("_playwright_subsystem", self._subsystem)
        if self._subsystem == "campaign":
            action_account.setdefault("require_persistent_profile", True)
            action_account.setdefault("disable_safe_browser_recovery", True)
=======
        action_account.setdefault("validate_reused_session", True)
>>>>>>> origin/main
        login_coro = login_func(action_account, headless=self._headless, proxy=proxy)
        try:
            if timeout_seconds is None:
                svc, ctx, page = await login_coro
            else:
                svc, ctx, page = await asyncio.wait_for(login_coro, timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            self._log_event("SESSION_OPEN_FAILED", key=key, reason="session_open_timeout")
            self._publish_open_failure(pool_key, exc)
            log_browser_stage(
                component="playwright_session_manager",
                stage="session_open_end",
                status="failed",
                account=username,
                reason="session_open_timeout",
                timeout_seconds=f"{timeout_seconds:.2f}" if timeout_seconds is not None else "",
            )
            raise TimeoutError("session_open_timeout") from exc
        except Exception as exc:
<<<<<<< HEAD
            log_payload = {
                "key": key,
                "error": str(exc) or type(exc).__name__,
                "error_type": type(exc).__name__,
            }
            browser_stage_payload = {
                "component": "playwright_session_manager",
                "stage": "session_open_end",
                "status": "failed",
                "account": username,
                "error": str(exc) or type(exc).__name__,
                "error_type": type(exc).__name__,
            }
            if isinstance(exc, PersistentProfileOwnershipError):
                log_payload.update(
                    {
                        "reason": exc.reason_code,
                        "conflict_code": exc.conflict_code,
                        "handoff_code": exc.handoff_code,
                        "profile_dir": exc.profile_dir,
                        "requested_mode": exc.requested_mode,
                        "active_mode": exc.active_mode,
                    }
                )
                browser_stage_payload["reason"] = exc.reason_code
            self._log_event("SESSION_OPEN_FAILED", **log_payload)
            self._publish_open_failure(pool_key, exc)
            log_browser_stage(**browser_stage_payload)
=======
            self._log_event("SESSION_OPEN_FAILED", key=key, error=str(exc) or type(exc).__name__, error_type=type(exc).__name__)
            self._publish_open_failure(pool_key, exc)
            log_browser_stage(
                component="playwright_session_manager",
                stage="session_open_end",
                status="failed",
                account=username,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
            )
>>>>>>> origin/main
            raise

        entry = _SessionEntry(key=key, svc=svc, ctx=ctx, page=page, proxy_key=proxy_key)
        stale_entry = None
        with self._GLOBAL_LOCK:
            existing = self._SHARED_SESSIONS.get(pool_key)
            if self._entry_is_usable(existing, proxy_key=proxy_key):
                stale_entry = entry
                entry = existing  # type: ignore[assignment]
            else:
                self._SHARED_SESSIONS[pool_key] = entry
            lease_id = self._attach_lease(pool_key, entry)
            open_state = self._OPENING.pop(pool_key, None)
<<<<<<< HEAD
            self._signal_open_state(open_state)
        if stale_entry is not None:
            self._retire_navigation(stale_entry, reason="stale_session_replaced")
=======
            if open_state is not None:
                open_state.event.set()
        if stale_entry is not None:
>>>>>>> origin/main
            await self._close_session_entry(stale_entry)
        self._log_event("SESSION_OPEN", key=key, persistent=entry.persistent, url=entry.page.url if entry.page else "")
        log_browser_stage(
            component="playwright_session_manager",
            stage="session_open_end",
            status="ok",
            account=username,
            url=entry.page.url if entry.page else "",
            persistent=entry.persistent,
        )
        log_browser_stage(
            component="playwright_session_manager",
            stage="browser_open",
            status="ok",
            account=username,
            url=entry.page.url if entry.page else "",
            persistent=entry.persistent,
        )
        return ManagedSession(
            key=key,
            svc=entry.svc,
            ctx=entry.ctx,
            page=entry.page,
            persistent=entry.persistent,
            reused=False,
            lease_id=lease_id,
            pool_key=pool_key,
<<<<<<< HEAD
            _acquire_navigation_sync=lambda owner, timeout, _session_key=pool_key: self.acquire_navigation_sync(
                ManagedSession(
                    key=key,
                    svc=entry.svc,
                    ctx=entry.ctx,
                    page=entry.page,
                    persistent=entry.persistent,
                    reused=False,
                    lease_id=lease_id,
                    pool_key=_session_key,
                ),
                owner,
                timeout,
            ),
            _acquire_navigation_async=lambda owner, timeout, _session_key=pool_key: self.acquire_navigation(
                ManagedSession(
                    key=key,
                    svc=entry.svc,
                    ctx=entry.ctx,
                    page=entry.page,
                    persistent=entry.persistent,
                    reused=False,
                    lease_id=lease_id,
                    pool_key=_session_key,
                ),
                owner,
                timeout,
            ),
            _release_navigation_sync=lambda owner, _session_key=pool_key: self.release_navigation_sync(
                ManagedSession(
                    key=key,
                    svc=entry.svc,
                    ctx=entry.ctx,
                    page=entry.page,
                    persistent=entry.persistent,
                    reused=False,
                    lease_id=lease_id,
                    pool_key=_session_key,
                ),
                owner,
            ),
            _release_navigation_async=lambda owner, _session_key=pool_key: self.release_navigation(
                ManagedSession(
                    key=key,
                    svc=entry.svc,
                    ctx=entry.ctx,
                    page=entry.page,
                    persistent=entry.persistent,
                    reused=False,
                    lease_id=lease_id,
                    pool_key=_session_key,
                ),
                owner,
            ),
            _navigation_metadata=lambda: self._navigation_metadata(entry),
=======
>>>>>>> origin/main
        )

    async def save_storage_state(self, session: ManagedSession, username: str) -> None:
        await session.svc.save_storage_state(
            session.ctx,
            browser_storage_state_path(username, profiles_root=self._profiles_root),
        )

    async def _save_entry_storage_state(self, entry: Optional[_SessionEntry]) -> None:
        if entry is None or entry.svc is None or entry.ctx is None:
            return
        if self.context_closed(entry.ctx):
            return
        try:
            await entry.svc.save_storage_state(
                entry.ctx,
                browser_storage_state_path(entry.key, profiles_root=self._profiles_root),
            )
<<<<<<< HEAD
        except Exception as exc:
            self._log_event(
                "SESSION_STORAGE_SAVE_FAILED",
                key=entry.key,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
                subsystem=self._subsystem,
            )
=======
        except Exception:
            pass
>>>>>>> origin/main

    async def discard_if_unhealthy(
        self,
        session: Optional[ManagedSession],
        error: BaseException,
        *,
        is_fatal_error: Callable[[BaseException], bool],
    ) -> None:
        if session is None:
            return
        if self.page_closed(session.page) or self.context_closed(session.ctx) or is_fatal_error(error):
            await self.drop_cached_session(session.key)

    async def finalize_session(self, session: Optional[ManagedSession], *, current_url: str) -> None:
        if session is None:
            return
        if session.lease_id and (not session.persistent) and self._should_keep_open(current_url):
            return
        await self.release_session(session)

    async def release_session(self, session: Optional[ManagedSession]) -> None:
        if session is None or not session.lease_id:
            return
        pool_key = session.pool_key or self._held_leases.get(session.lease_id) or self._pool_key(session.key)
        entry_to_close: _SessionEntry | None = None
        entry_to_persist: _SessionEntry | None = None
        with self._GLOBAL_LOCK:
            self._held_leases.pop(session.lease_id, None)
            entry = self._SHARED_SESSIONS.get(pool_key)
            if entry is None:
                return
            entry.leases.pop(session.lease_id, None)
            if self.page_closed(entry.page) or self.context_closed(entry.ctx):
                entry_to_close = self._SHARED_SESSIONS.pop(pool_key, None)
            elif not entry.sticky_owners and not entry.leases:
                entry_to_close = self._SHARED_SESSIONS.pop(pool_key, None)
            else:
                entry_to_persist = entry
        if entry_to_persist is not None:
            await self._save_entry_storage_state(entry_to_persist)
        if entry_to_close is not None:
<<<<<<< HEAD
            self._retire_navigation(entry_to_close, reason="session_released")
=======
>>>>>>> origin/main
            await self._close_session_entry(entry_to_close)

    async def drop_cached_session(self, key: str) -> None:
        pool_key = self._pool_key(key)
        entry_to_close: _SessionEntry | None = None
        with self._GLOBAL_LOCK:
            entry_to_close = self._SHARED_SESSIONS.pop(pool_key, None)
            if entry_to_close is not None:
                for lease_id in list(entry_to_close.leases.keys()):
                    self._held_leases.pop(lease_id, None)
<<<<<<< HEAD
            open_state = self._OPENING.pop(pool_key, None)
        self._signal_open_state(open_state, RuntimeError("session_dropped"))
        if entry_to_close is not None:
            self._retire_navigation(entry_to_close, reason="session_dropped")
=======
                self._OPENING.pop(pool_key, None)
        if entry_to_close is not None:
>>>>>>> origin/main
            await self._close_session_entry(entry_to_close)

    async def close_all_cached_sessions_async(self) -> None:
        entries_to_close: list[_SessionEntry] = []
        with self._GLOBAL_LOCK:
            held_leases = set(self._held_leases.keys())
            for pool_key, entry in list(self._SHARED_SESSIONS.items()):
                entry.sticky_owners.discard(self._manager_id)
                for lease_id, owner_id in list(entry.leases.items()):
                    if owner_id == self._manager_id or lease_id in held_leases:
                        entry.leases.pop(lease_id, None)
                        self._held_leases.pop(lease_id, None)
                if not entry.sticky_owners and not entry.leases:
                    removed = self._SHARED_SESSIONS.pop(pool_key, None)
                    if removed is not None:
                        entries_to_close.append(removed)
        for entry in entries_to_close:
<<<<<<< HEAD
            self._retire_navigation(entry, reason="session_manager_shutdown")
=======
>>>>>>> origin/main
            await self._close_session_entry(entry)

    def close_all_sessions_sync(self, *, timeout: float = 5.0) -> None:
        try:
            run_coroutine_sync(
                self.close_all_cached_sessions_async(),
                timeout=max(0.5, float(timeout or 0.5)),
                cancel_reason="session_close_timeout",
                ignore_stop=True,
            )
        except Exception:
            pass

    async def _close_session_entry(self, entry: _SessionEntry) -> None:
        await self._save_entry_storage_state(entry)
<<<<<<< HEAD
        page = entry.page
        if not self.page_closed(page):
            page_close = getattr(page, "close", None)
            if callable(page_close):
                try:
                    await page_close()
                except Exception as exc:
                    self._log_event(
                        "SESSION_PAGE_CLOSE_FAILED",
                        key=entry.key,
                        error=str(exc) or type(exc).__name__,
                        error_type=type(exc).__name__,
                        subsystem=self._subsystem,
                    )
        try:
            if entry.ctx is not None:
                await entry.ctx.close()
        except Exception as exc:
            profile_dir = browser_storage_state_path(entry.key, profiles_root=self._profiles_root).parent
            owner_token = str(getattr(entry.ctx, "_profile_lifecycle_owner_token", "") or "").strip()
            profile_mode = str(getattr(entry.ctx, "_profile_lifecycle_mode", "") or ("headless" if self._headless else "headful"))
            profile_subsystem = str(getattr(entry.ctx, "_profile_lifecycle_subsystem", "") or self._subsystem)
            emit_profile_lifecycle_diagnostic(
                event_type="browser_close_failed",
                profile_dir=profile_dir,
                account=entry.key,
                subsystem=profile_subsystem,
                mode=profile_mode,
                reason_code="browser_close_failed",
                pid=os.getpid(),
                owner_token=owner_token,
                payload={
                    "error": str(exc) or type(exc).__name__,
                    "error_type": type(exc).__name__,
                },
                callsite_skip=2,
            )
            mark_profile_unclean_shutdown(
                account=entry.key,
                profile_dir=profile_dir,
                subsystem=profile_subsystem,
                mode=profile_mode,
                pid=os.getpid(),
                owner_token=owner_token,
                reason_code="browser_close_failed",
                payload={
                    "error": str(exc) or type(exc).__name__,
                    "error_type": type(exc).__name__,
                },
            )
            self._log_event(
                "SESSION_CONTEXT_CLOSE_FAILED",
                key=entry.key,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
                subsystem=self._subsystem,
            )
        try:
            if entry.svc is not None:
                await entry.svc.close()
        except Exception as exc:
            self._log_event(
                "SESSION_SERVICE_CLOSE_FAILED",
                key=entry.key,
                error=str(exc) or type(exc).__name__,
                error_type=type(exc).__name__,
                subsystem=self._subsystem,
            )
=======
        try:
            if entry.ctx is not None:
                await entry.ctx.close()
        except Exception:
            pass
        try:
            if entry.svc is not None:
                await entry.svc.close()
        except Exception:
            pass
>>>>>>> origin/main

    def _publish_open_failure(self, pool_key: str, error: BaseException) -> None:
        with self._GLOBAL_LOCK:
            state = self._OPENING.get(pool_key)
            if state is None:
                state = _OpenState(proxy_key="")
                self._OPENING[pool_key] = state
<<<<<<< HEAD
        self._signal_open_state(state, error)
=======
            state.error = error
            state.event.set()
>>>>>>> origin/main

    def _resolve_open_timeout(self, *, deadline: float | None) -> float | None:
        if deadline is None:
            return self._default_open_timeout_seconds
        remaining = max(0.0, float(deadline) - time.time())
        if remaining <= 0:
            raise TimeoutError("session_open_deadline_exceeded")
        return remaining

    async def _wait_for_open_state(self, event: threading.Event, *, deadline: float | None) -> None:
        while not event.is_set():
            if deadline is not None and time.time() >= float(deadline):
                raise TimeoutError("session_open_deadline_exceeded")
            await asyncio.sleep(0.05)

    @classmethod
    def _should_keep_open(cls, current_url: str) -> bool:
        url = str(current_url or "")
        return bool(url) and any(token in url for token in cls._STAY_OPEN_TOKENS)


class SyncSessionRuntime:
    def __init__(
        self,
        *,
        account: dict[str, Any],
        session_manager: SessionManager,
        login_func: SessionLoginFunc,
        proxy_resolver: Callable[[dict[str, Any]], Optional[dict[str, Any]]] | None = None,
        open_timeout_seconds: float = 120.0,
    ) -> None:
        self._account = dict(account or {})
        self._session_manager = session_manager
        self._login_func = login_func
        self._proxy_resolver = proxy_resolver
        self._open_timeout_seconds = max(5.0, float(open_timeout_seconds or 5.0))
        self._session_lock = threading.RLock()
        self._session: ManagedSession | None = None
<<<<<<< HEAD
        self._navigation_local = threading.local()
=======
>>>>>>> origin/main

    def run_async(self, coro: Any, *, timeout: float | None = None) -> Any:
        return run_coroutine_sync(coro, timeout=self._resolve_timeout(timeout))

    def open_page(self, account: dict[str, Any], *, timeout: float | None = None) -> Any:
        with self._session_lock:
            if isinstance(account, dict) and account:
                self._account = dict(account)
            session = self._ensure_session_locked(timeout=timeout)
            page = session.page
            try:
                page.set_default_timeout(30_000)
                page.set_default_navigation_timeout(45_000)
            except Exception:
                pass
            return page

<<<<<<< HEAD
    def acquire_navigation(self, owner: str, *, timeout: float | None = None) -> dict[str, Any]:
        with self._session_lock:
            session = self._ensure_session_locked(timeout=timeout)
        return session.acquire_navigation(str(owner or ""), self._resolve_timeout(timeout))

    def release_navigation(self, owner: str) -> None:
        with self._session_lock:
            session = self._session
        if session is None:
            return
        session.release_navigation(str(owner or ""))

    @contextmanager
    def navigation_scope(self, owner: str, *, timeout: float | None = None):
        stack = getattr(self._navigation_local, "stack", None)
        if stack is None:
            stack = []
            self._navigation_local.stack = stack
        page = self.open_page(self._account, timeout=timeout)
        if stack:
            stack.append(str(stack[-1]))
            try:
                yield page
            finally:
                stack.pop()
            return
        clean_owner = str(owner or "").strip()
        self.acquire_navigation(clean_owner, timeout=timeout)
        stack.append(clean_owner)
        try:
            yield page
        finally:
            stack.pop()
            self.release_navigation(clean_owner)

=======
>>>>>>> origin/main
    def close_page(self, page: Any, *, timeout: float | None = None) -> None:
        with self._session_lock:
            session = self._session
            if session is None:
                return
            if page is not None and session.page is not page:
                return
            self._session = None
        current_url = ""
        try:
            current_url = str(getattr(page, "url", "") or "")
        except Exception:
            current_url = ""
        try:
            run_coroutine_sync(
                self._session_manager.finalize_session(session, current_url=current_url),
                timeout=self._resolve_timeout(timeout),
                cancel_reason="runtime_release_timeout",
                ignore_stop=True,
            )
        except Exception:
            pass

    def shutdown(self, *, timeout: float | None = None) -> None:
        with self._session_lock:
            session = self._session
            self._session = None
        if session is not None:
            current_url = ""
            try:
                current_url = str(getattr(session.page, "url", "") or "")
            except Exception:
                current_url = ""
            try:
                run_coroutine_sync(
                    self._session_manager.finalize_session(session, current_url=current_url),
                    timeout=self._resolve_timeout(timeout),
                    cancel_reason="runtime_shutdown_timeout",
                    ignore_stop=True,
                )
            except Exception:
                pass
        self._session_manager.close_all_sessions_sync(timeout=self._resolve_timeout(timeout))

    def invalidate(self, *, timeout: float | None = None) -> None:
        with self._session_lock:
            session = self._session
            self._session = None
        if session is None:
            return
        try:
            run_coroutine_sync(
                self._session_manager.drop_cached_session(session.key),
                timeout=self._resolve_timeout(timeout),
                cancel_reason="runtime_invalidate_timeout",
                ignore_stop=True,
            )
        except Exception:
            pass

    def _ensure_session_locked(self, *, timeout: float | None = None) -> ManagedSession:
        session = self._session
        if session is not None and not self._session_manager.page_closed(session.page) and not self._session_manager.context_closed(session.ctx):
            return session
        preflight = account_proxy_preflight(self._account)
        if bool(preflight.get("blocking")):
            username = str(preflight.get("username") or self._account.get("username") or "").strip().lstrip("@")
            detail = str(preflight.get("message") or "Proxy no disponible para esta cuenta.").strip()
            if username:
                raise RuntimeError(f"Cuenta @{username} bloqueada por proxy: {detail}")
            raise RuntimeError(detail)
        proxy = None
        if callable(self._proxy_resolver):
            try:
                proxy = self._proxy_resolver(dict(self._account))
            except ProxyResolutionError:
                raise
            except Exception:
                proxy = None
        session = run_coroutine_sync(
            self._session_manager.open_session(
                account=dict(self._account),
                proxy=proxy,
                login_func=self._login_func,
            ),
            timeout=self._resolve_timeout(timeout),
            cancel_reason="session_open_timeout",
            ignore_stop=True,
        )
        self._session = session
        return session

    def _resolve_timeout(self, timeout: float | None) -> float:
        if timeout is None:
            return self._open_timeout_seconds
        return max(0.1, float(timeout))
