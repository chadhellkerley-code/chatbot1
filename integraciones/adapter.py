"""
integraciones/adapter.py

Adapter to connect the main app's send-message flow with an external bot
implementation placed under the `integraciones` folder (e.g. GramAddict or
similar). This file is intentionally defensive: if the external bot is not
available the adapter returns a clear negative result and never mutates the
application's account storage.

Contract:
- send_message(account: dict, recipient: str, text: str, options: dict) -> (bool, str)

Keep English docstrings/comments here so other integrators can read them.
"""
from __future__ import annotations

import time
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# Attempt to import the external bot module. Use a try/except so importing
# the main app does not fail if the integration is missing.
try:
    # Example import - adjust to the real module name inside `integraciones`
    from integraciones.gramaddict import GramAddictBot  # type: ignore

    BOT_AVAILABLE = True
except Exception as _exc:  # pragma: no cover - optional integration
    GramAddictBot = None  # type: ignore
    BOT_AVAILABLE = False


def _human_typing_simulation(text: str, delay_per_char: float = 0.03) -> None:
    """Lightweight, Windows-friendly typing simulation.

    This function sleeps a little per character. It's purposely simple so the
    adapter can run even in restricted environments.
    """
    for _ch in text:
        time.sleep(delay_per_char)


def send_message(account: Dict, recipient: str, text: str, options: Dict | None = None) -> Tuple[bool, str]:
    """Send a direct message using the external bot if present.

    - account: dict-like object coming from the main application (must not be mutated)
    - recipient: target username (without @)
    - text: message body
    - options: adapter-specific settings. Recognised keys:
        - simulate_typing: bool (default True)
        - human_delay: float seconds per character (default 0.03)

    Returns: (success: bool, detail: str)
    """
    options = options or {}
    try:
        if not BOT_AVAILABLE or GramAddictBot is None:
            return False, "External bot not available"

        # Create a fresh bot instance. The adapter intentionally does not
        # change application storage; it only *uses* credentials/session info
        # provided by the caller.
        bot = GramAddictBot()

        # Try to reuse session info if present. We assume the app may provide
        # either a session path or a session object under known keys.
        session_info = account.get("session") or account.get("session_path")
        if session_info:
            try:
                if hasattr(bot, "use_session"):
                    bot.use_session(session_info)
                elif hasattr(bot, "load_session"):
                    bot.load_session(session_info)
                # if neither, fall back to login below
            except Exception:
                # session reuse failed -> try login below
                pass

        # If the bot is not already authenticated, try login using stored creds
        if not getattr(bot, "is_authenticated", lambda: False)():
            username = account.get("username")
            password = account.get("password")
            if username and password and hasattr(bot, "login"):
                bot.login(username, password)

        # Preserve human-like behaviour: delegate to bot if it supports a
        # typing helper, otherwise simulate locally and call send.
        simulate_typing = bool(options.get("simulate_typing", True))
        human_delay = float(options.get("human_delay", 0.03))

        if simulate_typing and hasattr(bot, "type_and_send"):
            bot.type_and_send(recipient, text, human_delay=human_delay)
        else:
            if simulate_typing:
                _human_typing_simulation(text, delay_per_char=human_delay)
            # Prefer send_direct or send to direct endpoint depending on bot
            if hasattr(bot, "send_direct"):
                bot.send_direct(recipient, text)
            elif hasattr(bot, "send_message"):
                bot.send_message(recipient, text)
            else:
                # As last resort, expose a clear error so the caller can fallback
                return False, "External bot does not expose a send method"

        return True, "Sent with external bot"
    except Exception as exc:
        logger.exception("Adapter send_message failed")
        return False, f"Adapter error: {exc}"
