from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union
from urllib.parse import quote, unquote, urlparse, urlsplit, urlunsplit

from core.proxy_registry import ProxyResolutionError, get_proxy_by_id, proxy_reference_status


logger = logging.getLogger(__name__)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _split_server_and_auth(raw_server: str) -> tuple[str, str, str]:
    """
    Normalize proxy server and extract embedded credentials when present.
    Returns: (server_without_auth, username, password)
    """

    text = _clean(raw_server)
    if not text:
        return "", "", ""

    candidate = text if "://" in text else f"http://{text}"
    parsed = urlparse(candidate)
    if not parsed.scheme or not parsed.hostname:
        return candidate, "", ""

    server = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        server += f":{parsed.port}"

    username = unquote(parsed.username) if parsed.username else ""
    password = unquote(parsed.password) if parsed.password else ""
    return server, username, password


def normalize_playwright_proxy(
    raw_proxy: Union[str, Dict[str, Any], None],
    *,
    proxy_user: Any = "",
    proxy_pass: Any = "",
) -> Optional[Dict[str, str]]:
    """
    Build a Playwright proxy payload:
      {"server": "...", "username": "...", "password": "..."}

    It merges credentials from:
    1) explicit proxy_user/proxy_pass
    2) raw_proxy dict username/password
    3) credentials embedded in raw_proxy URL
    """

    source_server = ""
    source_user = ""
    source_pass = ""

    if isinstance(raw_proxy, dict):
        source_server = _clean(
            raw_proxy.get("server")
            or raw_proxy.get("url")
            or raw_proxy.get("proxy")
        )
        source_user = _clean(raw_proxy.get("username") or raw_proxy.get("user"))
        source_pass = _clean(raw_proxy.get("password") or raw_proxy.get("pass"))
    elif isinstance(raw_proxy, str):
        source_server = _clean(raw_proxy)

    server, embedded_user, embedded_pass = _split_server_and_auth(source_server)
    if not server:
        return None

    final_user = _clean(proxy_user) or source_user or embedded_user
    final_pass = _clean(proxy_pass) or source_pass or embedded_pass

    payload: Dict[str, str] = {"server": server}
    if final_user:
        payload["username"] = final_user
    if final_pass:
        payload["password"] = final_pass
    return payload


def proxy_fields_from_proxy(
    raw_proxy: Union[str, Dict[str, Any], None],
    *,
    proxy_user: Any = "",
    proxy_pass: Any = "",
) -> Dict[str, str]:
    payload = normalize_playwright_proxy(
        raw_proxy,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )
    if not payload:
        return {
            "proxy_url": "",
            "proxy_user": "",
            "proxy_pass": "",
        }
    return {
        "proxy_url": _clean(payload.get("server") or payload.get("url") or payload.get("proxy")),
        "proxy_user": _clean(payload.get("username") or payload.get("user")),
        "proxy_pass": _clean(payload.get("password") or payload.get("pass")),
    }


def proxy_from_account(account: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
    if not account:
        return None

    username = _clean(account.get("username")).lstrip("@")
    assigned_proxy_id = _clean(account.get("assigned_proxy_id"))
    if not assigned_proxy_id:
        logger.warning("Account without proxy: username=%s", username or "-")
        return None

    status = proxy_reference_status(assigned_proxy_id)
    status_value = str(status.get("status") or "").strip().lower()
    if status_value == "missing":
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_missing",
            assigned_proxy_id,
            str(status.get("message") or "").strip(),
        )
    if status_value == "inactive":
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_inactive",
            assigned_proxy_id,
            str(status.get("message") or "").strip(),
        )
    if status_value == "quarantined":
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_quarantined",
            assigned_proxy_id,
            str(status.get("message") or "").strip(),
        )
    if status_value != "ok":
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_unresolved",
            assigned_proxy_id,
            str(status.get("message") or "").strip() or f"No se pudo resolver el proxy asignado {assigned_proxy_id}.",
        )

    record = status.get("record") if isinstance(status.get("record"), dict) else None
    if record is None:
        record = get_proxy_by_id(assigned_proxy_id, active_only=True)

    if not isinstance(record, dict):
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_unresolved",
            assigned_proxy_id,
            f"No se pudo resolver el proxy asignado {assigned_proxy_id}.",
        )

    server = _clean(record.get("server") or record.get("proxy_url") or record.get("url") or record.get("proxy"))
    proxy_user = _clean(record.get("user") or record.get("username"))
    proxy_pass = _clean(record.get("pass") or record.get("password"))
    payload = normalize_playwright_proxy(
        server,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )
    if payload is None:
        logger.warning("Account without proxy: username=%s", username or "-")
        raise ProxyResolutionError(
            "assigned_proxy_unresolved",
            assigned_proxy_id,
            f"No se pudo resolver el proxy asignado {assigned_proxy_id}.",
        )

    return payload


def proxy_fields_from_account(account: Optional[Dict[str, Any]]) -> Dict[str, str]:
    payload = proxy_from_account(account)
    return proxy_fields_from_proxy(payload)


def build_proxy_input_from_account(account: Optional[Dict[str, Any]]) -> Dict[str, str]:
    fields = proxy_fields_from_account(account)
    proxy_url = _clean(fields.get("proxy_url"))
    if not proxy_url:
        return {}
    payload = {"url": proxy_url}
    proxy_user = _clean(fields.get("proxy_user"))
    proxy_pass = _clean(fields.get("proxy_pass"))
    if proxy_user:
        payload["username"] = proxy_user
    if proxy_pass:
        payload["password"] = proxy_pass
    return payload


def requests_proxy_map_from_account(account: Optional[Dict[str, Any]]) -> Dict[str, str]:
    fields = proxy_fields_from_account(account)
    proxy_url = _clean(fields.get("proxy_url"))
    if not proxy_url:
        return {}
    proxy_url = proxy_url if "://" in proxy_url else f"http://{proxy_url}"
    proxy_user = _clean(fields.get("proxy_user"))
    proxy_pass = _clean(fields.get("proxy_pass"))
    if proxy_user:
        parsed = urlsplit(proxy_url)
        auth = quote(proxy_user, safe="")
        if proxy_pass:
            auth = f"{auth}:{quote(proxy_pass, safe='')}"
        proxy_url = urlunsplit(
            (
                parsed.scheme,
                f"{auth}@{parsed.netloc}",
                parsed.path,
                parsed.query,
                parsed.fragment,
            )
        )
    return {"http": proxy_url, "https": proxy_url}
