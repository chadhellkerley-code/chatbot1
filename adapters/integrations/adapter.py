"""
adapters/integrations/adapter.py

Adapter to connect the main app's send-message flow with an external bot
implementation placed under the ``adapters/integrations`` folder (for example,
GramAddict). This module is intentionally defensive: if the external bot is not
available the adapter returns a clear negative result and never mutates the
application's account storage.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

try:
    # Example import. Adjust to the real module name inside adapters/integrations.
    from adapters.integrations.gramaddict import GramAddictBot  # type: ignore

    BOT_AVAILABLE = True
except Exception:  # pragma: no cover - optional integration
    GramAddictBot = None  # type: ignore
    BOT_AVAILABLE = False


def _human_typing_simulation(text: str, delay_per_char: float = 0.03) -> None:
    """Lightweight typing simulation used when the bot has no native helper."""

    for _ch in text:
        time.sleep(delay_per_char)


def send_message(
    account: Dict,
    recipient: str,
    text: str,
    options: Dict | None = None,
) -> Tuple[bool, str]:
    """Send a direct message using the external bot if present."""

    options = options or {}
    try:
        if not BOT_AVAILABLE or GramAddictBot is None:
            return False, "External bot not available"

        bot = GramAddictBot()

        session_info = account.get("session") or account.get("session_path")
        if session_info:
            try:
                if hasattr(bot, "use_session"):
                    bot.use_session(session_info)
                elif hasattr(bot, "load_session"):
                    bot.load_session(session_info)
            except Exception:
                pass

        if not getattr(bot, "is_authenticated", lambda: False)():
            username = account.get("username")
            password = account.get("password")
            if username and password and hasattr(bot, "login"):
                bot.login(username, password)

        simulate_typing = bool(options.get("simulate_typing", True))
        human_delay = float(options.get("human_delay", 0.03))

        if simulate_typing and hasattr(bot, "type_and_send"):
            bot.type_and_send(recipient, text, human_delay=human_delay)
        else:
            if simulate_typing:
                _human_typing_simulation(text, delay_per_char=human_delay)
            if hasattr(bot, "send_direct"):
                bot.send_direct(recipient, text)
            elif hasattr(bot, "send_message"):
                bot.send_message(recipient, text)
            else:
                return False, "External bot does not expose a send method"

        return True, "Sent with external bot"
    except Exception as exc:
        logger.exception("Adapter send_message failed")
        return False, f"Adapter error: {exc}"
