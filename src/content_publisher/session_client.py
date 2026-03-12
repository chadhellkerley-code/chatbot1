from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from src.browser_profile_paths import browser_storage_state_path
from src.playwright_service import BASE_PROFILES
from core.proxy_registry import ProxyResolutionError
from src.proxy_payload import requests_proxy_map_from_account

from .content_library_service import ContentPublisherError


logger = logging.getLogger(__name__)

_REQUIRED_COOKIE_NAMES = ("sessionid", "csrftoken", "ds_user_id")
_DEFAULT_ACCEPT_LANGUAGE = "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7"
_DEFAULT_IG_APP_ID = "936619743392459"
_DEFAULT_ASBD_ID = "198387"
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _normalize_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def _storage_state_path(username: str, *, profiles_root: str | Path | None = None) -> Path:
    return browser_storage_state_path(username, profiles_root=profiles_root or BASE_PROFILES)


def _load_storage_state_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContentPublisherError(f"No se pudo leer la sesion guardada en {path.name}.") from exc
    if not isinstance(payload, dict):
        raise ContentPublisherError(f"La sesion guardada en {path.name} no es valida.")
    return payload


def _cookie_rows_from_storage(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cookies = payload.get("cookies")
    return cookies if isinstance(cookies, list) else []


def _cookie_map_from_storage(payload: dict[str, Any]) -> dict[str, str]:
    cookie_map: dict[str, str] = {}
    for row in _cookie_rows_from_storage(payload):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "")
        if not name or not value:
            continue
        cookie_map[name] = value
    return cookie_map


def _storage_state_cookie_map(
    account: dict[str, Any],
    *,
    profiles_root: str | Path | None = None,
) -> tuple[str, dict[str, str], list[dict[str, Any]]]:
    username = _normalize_username(account.get("username"))
    if not username:
        raise ContentPublisherError("La cuenta seleccionada no tiene username.")
    storage_state = _storage_state_path(username, profiles_root=profiles_root)
    if not storage_state.exists():
        raise ContentPublisherError(
            f"La sesion guardada para @{username} no esta disponible. Inicia sesion desde Cuentas."
        )
    payload = _load_storage_state_payload(storage_state)
    cookie_rows = _cookie_rows_from_storage(payload)
    cookie_map = _cookie_map_from_storage(payload)
    missing = [name for name in _REQUIRED_COOKIE_NAMES if not str(cookie_map.get(name) or "").strip()]
    if missing:
        joined = ", ".join(missing)
        raise ContentPublisherError(
            f"La sesion guardada para @{username} no tiene cookies validas ({joined})."
        )
    return username, cookie_map, cookie_rows


def _requests_proxy_map(account: dict[str, Any]) -> dict[str, str]:
    try:
        return requests_proxy_map_from_account(account)
    except ProxyResolutionError:
        raise
    except Exception:
        return {}


@dataclass
class AuthenticatedSession:
    username: str
    cookie_map: dict[str, str]
    session: requests.Session

    def close(self) -> None:
        self.session.close()


def create_authenticated_client(
    account: dict[str, Any],
    *,
    reason: str,
    profiles_root: str | Path | None = None,
) -> AuthenticatedSession:
    del reason
    username, cookie_map, cookie_rows = _storage_state_cookie_map(account, profiles_root=profiles_root)
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": str(
                account.get("accept_language")
                or account.get("language")
                or _DEFAULT_ACCEPT_LANGUAGE
            ).strip()
            or _DEFAULT_ACCEPT_LANGUAGE,
            "Cache-Control": "no-cache, no-store, max-age=0",
            "Pragma": "no-cache",
            "User-Agent": str(account.get("user_agent") or _DEFAULT_USER_AGENT).strip() or _DEFAULT_USER_AGENT,
            "X-ASBD-ID": str(
                account.get("x_asbd_id")
                or account.get("asbd_id")
                or _DEFAULT_ASBD_ID
            ).strip()
            or _DEFAULT_ASBD_ID,
            "X-CSRFToken": str(cookie_map.get("csrftoken") or "").strip(),
            "X-IG-App-ID": str(
                account.get("x_ig_app_id")
                or account.get("ig_app_id")
                or _DEFAULT_IG_APP_ID
            ).strip()
            or _DEFAULT_IG_APP_ID,
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    session.cookies = requests.cookies.RequestsCookieJar()
    for row in cookie_rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "")
        if not name:
            continue
        domain = str(row.get("domain") or ".instagram.com").strip() or ".instagram.com"
        path = str(row.get("path") or "/").strip() or "/"
        session.cookies.set(name, value, domain=domain, path=path)

    proxies = _requests_proxy_map(account)
    if proxies:
        session.proxies.update(proxies)
        logger.debug("Proxy applied to authenticated content session for @%s.", username)

    return AuthenticatedSession(
        username=username,
        cookie_map=dict(cookie_map),
        session=session,
    )


def pause_between_operations(*, minimum_seconds: float = 2.0, maximum_seconds: float = 5.0) -> None:
    floor = max(0.0, float(minimum_seconds or 0.0))
    ceiling = max(floor, float(maximum_seconds or floor))
    time.sleep(random.uniform(floor, ceiling))


__all__ = ["AuthenticatedSession", "create_authenticated_client", "pause_between_operations"]
