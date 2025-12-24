"""Playwright-backed Instagram client adapter.

This adapter plugs the existing Playwright human sender into the generic
`BaseInstagramClient` interface so the CLI menu can keep calling
`get_instagram_client()` without knowing about the underlying engine.

It relies on:
 - `accounts` menu performing the interactive login and persisting the
   Playwright storage state into the profiles directory.
 - `HumanInstagramSender` using that persisted session for background
   message sending.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from adapters.base import BaseInstagramClient
from src.auth.onboarding import login_and_persist
from src.transport.human_instagram_sender import HumanInstagramSender


class InstagramPlaywrightClient(BaseInstagramClient):
    """Concrete Instagram client powered by Playwright."""

    def __init__(self, *, account: Optional[dict] = None) -> None:
        super().__init__(account=account)
        self.logger = logging.getLogger(self.__class__.__name__)
        # No creamos sender aquí, lo crearemos en cada operación

    def close(self) -> None:
        # Sender opens/closes per call; nothing persistent to close.
        return

    def login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        # Build payload from account details
        payload: Dict[str, Any] = dict(self.account or {})
        payload["username"] = username
        payload["password"] = password
        # verification_code is not used here; totp/callback handled by onboarding
        result = login_and_persist(payload, headless=False)
        ok = (result.get("status") == "ok")
        if ok:
            # Mark adapter logged in to satisfy BaseInstagramClient contract
            self._mark_logged_in(username)
        return ok

    def send_direct_message(self, target_username: str, message: str) -> bool:
        # Uses Playwright storage_state.json already persisted.
        # IMPORTANTE: Creamos sender con headless=True para envíos en segundo plano
        acct = dict(self.account or {})
        if self._username and not acct.get("username"):
            acct["username"] = self._username
        sender = HumanInstagramSender(headless=True)
        return bool(sender.send_message_like_human(acct, target_username, message))

    def reply_to_unread(self, *, limit: int = 10, strategy: Optional[dict] = None) -> List[Dict[str, Any]]:
        # This is handled by responder.py / src/opt_in in other menu flows.
        # Keep as no-op for now to avoid breaking the CLI.
        self._record_event("reply_to_unread", {"limit": limit, "strategy": strategy or {}})
        return []

    def follow_user(self, username: str) -> bool:
        self._record_event("follow_user", {"username": username})
        return False

    def like_post(self, url_or_code: str) -> bool:
        self._record_event("like_post", {"target": url_or_code})
        return False

    def comment_post(self, url_or_code: str, text: str) -> bool:
        self._record_event("comment_post", {"target": url_or_code, "text": text})
        return False

    def watch_reel(self, identifier: str) -> bool:
        self._record_event("watch_reel", {"id": identifier})
        return False