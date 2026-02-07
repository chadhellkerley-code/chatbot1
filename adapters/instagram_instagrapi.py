"""Instagrapi-backed Instagram client adapter."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .base import BaseInstagramClient, TwoFARequired, TwoFactorCodeRejected

_INSTAGRAPI_IMPORT_ERROR: Optional[str] = None
try:  # pragma: no cover - optional dependency
    from instagrapi import Client as InstaClient
    from instagrapi.exceptions import TwoFactorRequired as InstaTwoFactorRequired
except Exception as exc:  # pragma: no cover - environment without instagrapi
    InstaClient = None  # type: ignore[assignment]
    InstaTwoFactorRequired = None  # type: ignore[assignment]
    _INSTAGRAPI_IMPORT_ERROR = str(exc)


def _infer_two_factor_mode(info: Dict[str, Any]) -> str:
    if not isinstance(info, dict):
        return "unknown"
    if info.get("totp_two_factor_on") or info.get("is_totp_two_factor_enabled"):
        return "totp"
    if info.get("whatsapp_two_factor_on") or info.get("should_use_whatsapp_token"):
        return "whatsapp"
    if info.get("sms_two_factor_on") or info.get("is_sms_two_factor_enabled"):
        return "sms"
    return "unknown"


class InstagramInstagrapiClient(BaseInstagramClient):
    """Instagram client that uses instagrapi for login and basic actions."""

    def __init__(self, *, account: Optional[dict] = None) -> None:
        if InstaClient is None:
            detail = f": {_INSTAGRAPI_IMPORT_ERROR}" if _INSTAGRAPI_IMPORT_ERROR else ""
            raise RuntimeError(f"instagrapi is not available{detail}")
        super().__init__(account=account)
        self._client = InstaClient()
        self._two_factor_identifier: Optional[str] = None
        self._two_factor_info: Dict[str, Any] = {}
        self._last_username: Optional[str] = None
        self._last_password: Optional[str] = None

    def _ensure_client(self):
        if self._client is None:
            raise RuntimeError("instagrapi client not initialized")
        return self._client

    def _sync_settings(self) -> None:
        client = self._ensure_client()
        try:
            settings = client.get_settings()
        except Exception:
            return
        if isinstance(settings, dict):
            self._settings = settings
            self._session_loaded = bool(settings)

    @property
    def user_id(self) -> Optional[int]:
        client = self._ensure_client()
        value = getattr(client, "user_id", None)
        if value:
            return value
        auth = self._settings.get("authorization_data")
        if isinstance(auth, dict):
            candidate = auth.get("ds_user_id") or auth.get("user_id")
            if candidate:
                try:
                    return int(candidate)
                except Exception:
                    return candidate
        return None

    def __getattr__(self, name: str):
        client = self._ensure_client()
        try:
            return getattr(client, name)
        except AttributeError as exc:
            raise AttributeError(
                f"{self.__class__.__name__} has no attribute '{name}'"
            ) from exc

    def set_proxy(self, value: Any) -> None:
        super().set_proxy(value)
        client = self._ensure_client()
        proxy_url = value
        if isinstance(value, dict):
            proxy_url = value.get("https") or value.get("http")
        if proxy_url:
            try:
                client.set_proxy(proxy_url)
            except Exception:
                pass

    def get_settings(self) -> Dict[str, Any]:
        self._sync_settings()
        return json.loads(json.dumps(self._settings, ensure_ascii=False))

    def load_settings(self, path: str) -> None:
        client = self._ensure_client()
        try:
            client.load_settings(path)
            self._sync_settings()
        except Exception:
            super().load_settings(path)

    def dump_settings(self, path: str) -> bool:
        client = self._ensure_client()
        try:
            client.dump_settings(path)
            return True
        except Exception:
            return super().dump_settings(path)

    def login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        client = self._ensure_client()
        self._last_username = username
        self._last_password = password
        try:
            if verification_code:
                client.login(username, password, verification_code=verification_code)
            else:
                client.login(username, password)
            self._sync_settings()
            self._mark_logged_in(username)
            return True
        except Exception as exc:
            if InstaTwoFactorRequired is not None and isinstance(exc, InstaTwoFactorRequired):
                info = getattr(exc, "two_factor_info", {}) or {}
                identifier = getattr(exc, "two_factor_identifier", None) or info.get(
                    "two_factor_identifier"
                )
                self._two_factor_info = info if isinstance(info, dict) else {}
                self._two_factor_identifier = identifier
                method = _infer_two_factor_mode(self._two_factor_info)
                methods = [method] if method != "unknown" else []
                raise TwoFARequired(method=method, methods=methods, info=self._two_factor_info) from exc
            raise

    def request_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = super().request_two_factor_code(channel)
        return payload

    def resend_two_factor_code(self, channel: str) -> Dict[str, Any]:
        payload = super().resend_two_factor_code(channel)
        return payload

    def submit_two_factor_code(self, code: str) -> Dict[str, Any]:
        client = self._ensure_client()
        if not self._two_factor_identifier:
            raise TwoFactorCodeRejected("Missing two-factor identifier")
        try:
            two_factor_login = getattr(client, "two_factor_login", None)
            if callable(two_factor_login):
                try:
                    two_factor_login(code, self._two_factor_identifier)
                except TypeError:
                    two_factor_login(code)
            else:
                username = self._last_username or (self.account.get("username") or "")
                password = self._last_password or (self.account.get("password") or "")
                if not username or not password:
                    raise TwoFactorCodeRejected("Missing credentials for 2FA")
                client.login(username, password, verification_code=code)
            self._sync_settings()
            if self._last_username:
                self._mark_logged_in(self._last_username)
            payload = {"accepted": True, "status": "ok"}
            return payload
        except Exception as exc:
            raise TwoFactorCodeRejected(str(exc)) from exc

    def send_direct_message(self, target_username: str, message: str) -> bool:
        client = self._ensure_client()
        target = (target_username or "").strip().lstrip("@")
        if not target:
            return False
        try:
            target_id = client.user_id_from_username(target)
            client.direct_send(message, [target_id])
            return True
        except Exception:
            self.logger.exception("Instagrapi send_direct_message failed")
            return False

    def reply_to_unread(self, *, limit: int = 10, strategy: Optional[dict] = None) -> List[Dict[str, Any]]:
        return []

    def follow_user(self, username: str) -> bool:
        client = self._ensure_client()
        target = (username or "").strip().lstrip("@")
        if not target:
            return False
        try:
            target_id = client.user_id_from_username(target)
            client.user_follow(target_id)
            return True
        except Exception:
            self.logger.exception("Instagrapi follow_user failed")
            return False

    def like_post(self, url_or_code: str) -> bool:
        client = self._ensure_client()
        if not url_or_code:
            return False
        try:
            media_id = client.media_pk_from_url(url_or_code)
            client.media_like(media_id)
            return True
        except Exception:
            self.logger.exception("Instagrapi like_post failed")
            return False

    def comment_post(self, url_or_code: str, text: str) -> bool:
        client = self._ensure_client()
        if not url_or_code or not text:
            return False
        try:
            media_id = client.media_pk_from_url(url_or_code)
            media_comment = getattr(client, "media_comment", None)
            if callable(media_comment):
                media_comment(media_id, text)
                return True
            return False
        except Exception:
            self.logger.exception("Instagrapi comment_post failed")
            return False

    def watch_reel(self, identifier: str) -> bool:
        return False
