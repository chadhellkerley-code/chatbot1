# runtime.py
# -*- coding: utf-8 -*-
"""Coordinación de ejecución compartida (eventos de stop y logging)."""

from __future__ import annotations

import functools
import logging
import os
import random
import select
import sys
import threading
import time
from pathlib import Path

_GLOBAL_STOP_EVENT = threading.Event()


class EngineCancellationToken:
    """Engine-scoped cancellation token with global stop fallback."""

    def __init__(self, name: str = "engine") -> None:
        self.name = str(name or "engine").strip() or "engine"
        self._event = threading.Event()
        self._reason = ""

    @property
    def reason(self) -> str:
        return self._reason

    def cancel(self, reason: str = "") -> None:
        normalized = str(reason or "").strip()
        if normalized and not self._reason:
            self._reason = normalized
        self._event.set()

    def reset(self) -> None:
        self._reason = ""
        self._event.clear()

    def is_cancelled(self, *, include_global: bool = True) -> bool:
        if self._event.is_set():
            return True
        return include_global and _GLOBAL_STOP_EVENT.is_set()


class _CompatibilityStopEvent:
    """Event-like wrapper that routes checks to a bound engine token when present."""

    def __init__(self, global_event: threading.Event) -> None:
        self._global_event = global_event
        self._local = threading.local()

    def clear(self) -> None:
        self._global_event.clear()

    def set(self) -> None:
        self._global_event.set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._global_event.wait(timeout=timeout)

    def is_set(self) -> bool:
        token = self.current_token()
        if token is not None and token.is_cancelled(include_global=False):
            return True
        return self._global_event.is_set()

    def global_is_set(self) -> bool:
        return self._global_event.is_set()

    def current_token(self) -> EngineCancellationToken | None:
        return getattr(self._local, "token", None)

    def bind_token(self, token: EngineCancellationToken | None) -> EngineCancellationToken | None:
        previous = self.current_token()
        if token is None:
            if hasattr(self._local, "token"):
                delattr(self._local, "token")
        else:
            self._local.token = token
        return previous

    def restore_token(self, previous: EngineCancellationToken | None) -> None:
        if previous is None:
            if hasattr(self._local, "token"):
                delattr(self._local, "token")
            return
        self._local.token = previous


STOP_EVENT = _CompatibilityStopEvent(_GLOBAL_STOP_EVENT)


def bind_stop_token(token: EngineCancellationToken | None) -> EngineCancellationToken | None:
    return STOP_EVENT.bind_token(token)


def restore_stop_token(previous: EngineCancellationToken | None) -> None:
    STOP_EVENT.restore_token(previous)


def bind_stop_token_callable(token: EngineCancellationToken | None, fn):
    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        previous = bind_stop_token(token)
        try:
            return fn(*args, **kwargs)
        finally:
            restore_stop_token(previous)

    return _wrapped


def reset_stop_event() -> None:
    """Limpia el estado del evento global antes de iniciar un flujo."""
    active_token = STOP_EVENT.current_token()
    if active_token is not None:
        active_token.reset()
        return
    STOP_EVENT.clear()


def request_stop(reason: str, token: EngineCancellationToken | None = None) -> None:
    active_token = token or STOP_EVENT.current_token()
    if active_token is not None:
        if not active_token.is_cancelled(include_global=False):
            logging.getLogger("runtime").info("Deteniendo ejecución [%s]: %s", active_token.name, reason)
            active_token.cancel(reason)
        return
    if not STOP_EVENT.global_is_set():
        logging.getLogger("runtime").info("Deteniendo ejecución: %s", reason)
        STOP_EVENT.set()


def ensure_logging(
    level: int = logging.INFO,
    *,
    quiet: bool = False,
    log_dir: Path | None = None,
    log_file: str = "app.log",
) -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    handlers: list[logging.Handler] = []
    if log_dir:
        try:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(Path(log_dir) / log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except Exception:
            pass

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level if not quiet else logging.WARNING)
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    for handler in handlers:
        root.addHandler(handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)


def start_q_listener(
    message: str,
    logger: logging.Logger,
    *,
    token: EngineCancellationToken | None = None,
) -> threading.Thread:
    def _watch() -> None:
        previous = bind_stop_token(token)
        suffix = "" if os.name == "nt" else " y Enter"
        try:
            logger.info("%s%s", message, suffix)
            while not STOP_EVENT.is_set():
                try:
                    if os.name == "nt":
                        import msvcrt  # type: ignore

                        if msvcrt.kbhit():
                            ch = msvcrt.getwch()
                            if ch.lower() == "q":
                                request_stop("se presionó Q", token=token)
                                break
                    else:
                        ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                        if ready:
                            ch = sys.stdin.readline().strip().lower()
                            if ch == "q":
                                request_stop("se presionó Q", token=token)
                                break
                except Exception:
                    time.sleep(0.3)
                time.sleep(0.1)
        finally:
            restore_stop_token(previous)

    listener = threading.Thread(target=_watch, daemon=True)
    listener.start()
    return listener


def jitter_delay(min_seconds: int, max_seconds: int) -> int:
    if max_seconds <= min_seconds:
        return max(min_seconds, 0)
    return random.randint(min_seconds, max_seconds)


def sleep_with_stop(total_seconds: int, *, step: float = 1.0) -> None:
    slept = 0.0
    while slept < total_seconds and not STOP_EVENT.is_set():
        interval = min(step, total_seconds - slept)
        time.sleep(interval)
        slept += interval
