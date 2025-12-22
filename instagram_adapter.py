"""Compatibilidad para prompts 2FA y cliente legacy."""
from __future__ import annotations

import logging
import random
import re
import sys
import time
from typing import Callable, Dict, Optional

from adapters import BaseInstagramClient, TwoFARequired, TwoFactorCodeRejected
from client_factory import get_instagram_client
from config import SETTINGS
from totp_store import generate_code as generate_totp_code

logger = logging.getLogger(__name__)

_DEFAULT_METHOD_PRIORITY = ("whatsapp", "sms", "email")


def _sanitize_code(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if 4 <= len(digits) <= 8:
        return digits
    return None


def _human_delay(min_seconds: float = 0.5, max_seconds: float = 1.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


class InstagramClientAdapter:
    """Ligero wrapper que delega en el cliente configurado via factory."""

    def __init__(self, *, client_factory: Callable[[], BaseInstagramClient] | None = None) -> None:
        factory = client_factory or get_instagram_client
        self._client: BaseInstagramClient = factory()
        self._session_cache: Dict[str, object] = {}

    # ------------------------------------------------------------------ #
    def set_proxy(self, value):
        return self._client.set_proxy(value)

    def get_settings(self) -> Dict:
        return self._client.get_settings()

    def load_settings(self, path: str) -> None:
        self._client.load_settings(path)

    def dump_settings(self, path: str) -> bool:
        return self._client.dump_settings(path)

    # ------------------------------------------------------------------ #
    def load_session(self, data: Dict[str, object]) -> None:
        self._session_cache = dict(data)
        settings = getattr(self._client, "_settings", None)
        if isinstance(settings, dict):
            settings.update(self._session_cache)

    def dump_session(self) -> Dict[str, object]:
        settings = getattr(self._client, "_settings", None)
        if isinstance(settings, dict):
            return dict(settings)
        return dict(self._session_cache)

    # ------------------------------------------------------------------ #
    def do_login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        code = verification_code or generate_totp_code(username) or None
        _human_delay(1.0, 2.5)
        return self._client.login(username, password, verification_code=code)

    def request_2fa_code(self, channel: str) -> Dict[str, object]:
        return self._client.request_two_factor_code(channel)

    def resend_2fa_code(self, channel: str) -> Dict[str, object]:
        return self._client.resend_two_factor_code(channel)

    def finish_2fa(self, code: str) -> Dict[str, object]:
        sanitized = _sanitize_code(code)
        if not sanitized:
            raise ValueError("El codigo 2FA proporcionado es invalido.")
        return self._client.submit_two_factor_code(sanitized)


def prompt_two_factor_code(username: str, method: str, attempt: int) -> Optional[str]:
    label = method.lower()
    # Compatibilidad: si existen atributos min/max, úsalos; si no, usar PROMPT_2FA_TIMEOUT_SECONDS.
    if hasattr(SETTINGS, "two_factor_prompt_min_timeout") and hasattr(
        SETTINGS, "two_factor_prompt_max_timeout"
    ):
        min_sec = max(2, int(getattr(SETTINGS, "two_factor_prompt_min_timeout", 2)))
        max_sec = max(min_sec, int(getattr(SETTINGS, "two_factor_prompt_max_timeout", min_sec)))
        timeout = random.randint(min_sec, max_sec)
    else:
        timeout = max(5, int(getattr(SETTINGS, "prompt_2fa_timeout_seconds", 180)))

    prompt = f"Ingrese el codigo recibido por {label} para {username}: "
    logger.info(
        "Solicitando codigo 2FA manual para @%s via %s (intento %d, timeout %ds)",
        username,
        label,
        attempt,
        timeout,
    )
    try:
        code = _read_input_with_timeout(prompt, timeout)
    except Exception as exc:  # pragma: no cover - dependiente de consola
        logger.warning("No se pudo leer el codigo 2FA para @%s: %s", username, exc)
        return None
    sanitized = _sanitize_code(code)
    if sanitized is None:
        logger.warning("Codigo 2FA invalido para @%s (intento %d)", username, attempt)
    return sanitized


def _read_input_with_timeout(prompt: str, timeout: Optional[int]) -> Optional[str]:
    if timeout is not None and timeout <= 0:
        timeout = None

    if timeout is None:
        return input(prompt)

    deadline = time.time() + timeout
    print(prompt, end="", flush=True)

    if sys.platform.startswith("win"):
        import msvcrt  # type: ignore

        buffer: list[str] = []
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                print("\n[Tiempo excedido esperando el codigo]\n", flush=True)
                return None
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\r", "\n"):
                    print()
                    return "".join(buffer)
                if ch == "\b":
                    if buffer:
                        buffer.pop()
                        print("\b \b", end="", flush=True)
                    continue
                buffer.append(ch)
                print("*", end="", flush=True)
            else:
                time.sleep(min(0.2, max(0.0, remaining)))
    else:
        import select

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                print("\n[Tiempo excedido esperando el codigo]\n", flush=True)
                return None
            ready, _, _ = select.select([sys.stdin], [], [], min(1.0, max(0.1, remaining)))
            if ready:
                line = sys.stdin.readline()
                if not line:
                    return None
                return line.rstrip("\n")

    return None
