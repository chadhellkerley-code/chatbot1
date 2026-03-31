"""Abstract definitions for Instagram client adapters."""
from __future__ import annotations

import json
import logging
import random
import string
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _copy_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a deep-ish copy that is safe to serialize."""
    return json.loads(json.dumps(data, ensure_ascii=False))


@dataclass(frozen=True)
class TwoFARequired(Exception):
    """Raised when an Instagram login requires a second factor."""

    method: str
    methods: List[str]
    info: Dict[str, object]

    def __str__(self) -> str:
        methods = ", ".join(self.methods) if self.methods else "ninguno"
        method = self.method or "desconocido"
        return f"Se requiere 2FA vía {method} (opciones: {methods})"


class TwoFactorCodeRejected(RuntimeError):
    """Raised when a provided 2FA code is not accepted."""


class _ResponseManager:
    """Minimal helper to emulate the legacy quick responses interface."""

    def __init__(self, client: "BaseInstagramClient") -> None:
        self._client = client

    def create(self, text: str, **kwargs) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"id": self._client._build_id("resp"), "text": text}
        payload.update(_copy_dict(kwargs))
        self._client._record_event("responses.create", payload)
        return payload


class BaseInstagramClient(ABC):
    """Abstract base for Instagram automation backends."""

    def __init__(self, *, account: Optional[dict] = None) -> None:
        self.account = account or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        username = (self.account.get("username") or "").strip().lstrip("@")
        self._username = username or None
        self._settings: Dict[str, Any] = {"authorization_data": {}}
        self._logged_in = False
        self._session_loaded = False
        self._proxy: Any = None
        self._events: List[Dict[str, Any]] = []
        self.responses = _ResponseManager(self)

    # -- lifecycle helpers -------------------------------------------------
    def __enter__(self) -> "BaseInstagramClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:  # pragma: no cover - overridable cleanup
        """Hook for subclasses to release resources."""

    # -- configuration ------------------------------------------------------
    def set_proxy(self, value: Any) -> None:
        self._proxy = value
        self._record_event("set_proxy", {"value": str(value)})

    def get_settings(self) -> Dict[str, Any]:
        return _copy_dict(self._settings)

    def load_settings(self, path: str) -> None:
        target = Path(path)
        if not target.exists():
            return
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        self._settings = payload if isinstance(payload, dict) else {}
        self._session_loaded = bool(self._settings)
        self._record_event("load_settings", {"path": str(path), "loaded": self._session_loaded})

    def dump_settings(self, path: str) -> bool:
        payload = json.dumps(self._settings, ensure_ascii=False, indent=2)
        Path(path).write_text(payload, encoding="utf-8")
        self._record_event("dump_settings", {"path": str(path)})
        return True

    def ensure_logged_in(self) -> bool:
        return bool(self._logged_in)

    def ensure_session(self) -> bool:
        return self.ensure_logged_in() or self._session_loaded

    # -- 2FA hooks ----------------------------------------------------------
    def request_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = {"channel": channel, "status": "requested"}
        self._record_event("request_2fa", payload)
        return payload

    def resend_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = {"channel": channel, "status": "resent"}
        self._record_event("resend_2fa", payload)
        return payload

    def submit_two_factor_code(self, code: str) -> Dict[str, Any]:
        payload = {"code": code, "status": "submitted"}
        self._record_event("submit_2fa", payload)
        return payload

    # Legacy names kept for compatibility with the CLI codebase.
    def request_2fa_code(self, channel: str) -> Dict[str, Any]:
        return self.request_two_factor_code(channel)

    def resend_2fa_code(self, channel: str) -> Dict[str, Any]:
        return self.resend_two_factor_code(channel)

    def finish_2fa(self, code: str) -> Dict[str, Any]:
        return self.submit_two_factor_code(code)

    # -- timeline -----------------------------------------------------------
    def get_timeline_feed(self) -> List[Dict[str, Any]]:
        self._record_event("timeline_feed", {})
        return []

    # -- abstract API -------------------------------------------------------
    @abstractmethod
    def login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def send_direct_message(self, target_username: str, message: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def reply_to_unread(self, *, limit: int = 10, strategy: Optional[dict] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def follow_user(self, username: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def like_post(self, url_or_code: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def comment_post(self, url_or_code: str, text: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def watch_reel(self, identifier: str) -> bool:
        raise NotImplementedError

    # -- helper utilities ---------------------------------------------------
    def _record_event(self, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
        entry = {
            "ts": time.time(),
            "username": self._username,
            "action": action,
            "payload": _copy_dict(payload or {}),
        }
        self._events.append(entry)
        self.logger.debug("Stub action %s: %s", action, entry["payload"])

    def _build_id(self, prefix: str) -> str:
        suffix = uuid.uuid4().hex[:10]
        return f"{prefix}_{suffix}"

    def _random_username(self) -> str:
        letters = "".join(random.choice(string.ascii_lowercase) for _ in range(5))
        return f"{letters}_{random.randint(100,999)}"

    def _mark_logged_in(self, username: str) -> None:
        self._username = username.strip().lstrip("@")
        self._logged_in = True
        self._settings.setdefault("authorization_data", {})
        self._settings["authorization_data"]["sessionid"] = self._settings["authorization_data"].get(
            "sessionid"
        ) or self._build_id("sess")
        self._settings["authorization_data"]["ds_user_id"] = self._settings["authorization_data"].get(
            "ds_user_id"
        ) or str(random.randint(1000000, 9999999))
        self._record_event("login_state", {"username": self._username})

    def recorded_events(self) -> Iterable[Dict[str, Any]]:
        return list(self._events)
