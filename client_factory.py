"""Factory helpers for Instagram clients."""
from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from adapters.base import BaseInstagramClient

_ALLOWED_ENGINES = {"playwright", "stub"}


def _stub_allowed() -> bool:
    if os.environ.get("ALLOW_INSTAGRAM_STUB", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    return "pytest" in sys.modules


def _resolve_engine(engine: Optional[str]) -> str:
    selected = str(engine or os.environ.get("INSTAGRAM_ENGINE") or "").strip().lower()
    if not selected:
        if _stub_allowed():
            return "stub"
        raise RuntimeError(
            "Instagram engine selection is required. Use engine='playwright', "
            "or set INSTAGRAM_ENGINE explicitly."
        )
    if selected not in _ALLOWED_ENGINES:
        supported = ", ".join(sorted(_ALLOWED_ENGINES))
        raise RuntimeError(f"Unsupported Instagram engine '{selected}'. Supported engines: {supported}.")
    if selected == "stub" and not _stub_allowed():
        raise RuntimeError(
            "Instagram stub engine is restricted to controlled test contexts. "
            "Set ALLOW_INSTAGRAM_STUB=1 only for tests."
        )
    return selected


def get_instagram_client(*, account: Optional[dict] = None, engine: Optional[str] = None) -> BaseInstagramClient:
    """Return the configured Instagram client.

    Operational flows must select a real engine explicitly. The stub backend is
    only available in controlled test contexts.
    """

    selected = _resolve_engine(engine)
    if selected == "playwright":
        from adapters.instagram_playwright import InstagramPlaywrightClient

        return InstagramPlaywrightClient(account=account)
    # default fallback
    from adapters.instagram_stub import InstagramStubClient

    return InstagramStubClient(account=account)
