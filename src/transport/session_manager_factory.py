from __future__ import annotations

from typing import Any, Callable, Optional

from src.browser_profile_paths import normalize_browser_profile_username
from src.playwright_service import BASE_PROFILES
from src.transport.session_manager import SessionManager


def _noop_log_event(*_args: Any, **_kwargs: Any) -> None:
    return


def get_session_manager(
    headless: bool,
    *,
    keep_browser_open_per_account: bool = False,
    log_event: Optional[Callable[..., None]] = None,
    subsystem: str = "default",
) -> SessionManager:
    """
    Canonical SessionManager factory.

    NOTE: We intentionally return a new SessionManager instance per call.
    `SessionManager` already shares the underlying session pool globally via
    class-level caches, while the instance-level `manager_id` is used to scope
    leases and shutdown semantics.
    """

    return SessionManager(
        headless=bool(headless),
        keep_browser_open_per_account=bool(keep_browser_open_per_account),
        profiles_root=str(BASE_PROFILES),
        normalize_username=normalize_browser_profile_username,
        log_event=log_event or _noop_log_event,
        subsystem=subsystem,
    )

