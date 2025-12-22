"""Factory helpers for Instagram clients."""
from __future__ import annotations

import os
from typing import Optional

from adapters import BaseInstagramClient, InstagramStubClient, InstagramPlaywrightClient

_DEFAULT_ENGINE = os.environ.get("INSTAGRAM_ENGINE", "stub").strip().lower() or "stub"


def get_instagram_client(*, account: Optional[dict] = None, engine: Optional[str] = None) -> BaseInstagramClient:
    """Return the configured Instagram client.

    The default engine is the stub implementation, but the hook allows future
    backends to be plugged without changing callers.
    """

    selected = (engine or _DEFAULT_ENGINE).lower()
    # If OPTIN_ENABLE is on, default to Playwright unless the caller overrides engine
    if os.environ.get("OPTIN_ENABLE", "0") == "1" and selected in {"stub", ""}:
        selected = "playwright"
    if selected == "playwright":
        return InstagramPlaywrightClient(account=account)
    # default fallback
    return InstagramStubClient(account=account)
