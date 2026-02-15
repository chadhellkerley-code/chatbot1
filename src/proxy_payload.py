from __future__ import annotations

from typing import Any, Dict, Optional, Union
from urllib.parse import unquote, urlparse


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


def proxy_from_account(account: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
    if not account:
        return None

    proxy_url = _clean(account.get("proxy_url"))
    proxy_user = _clean(account.get("proxy_user") or account.get("proxy_username"))
    proxy_pass = _clean(account.get("proxy_pass") or account.get("proxy_password"))

    raw_proxy: Union[str, Dict[str, Any], None]
    if proxy_url:
        raw_proxy = proxy_url
    else:
        raw_proxy = account.get("proxy")

    return normalize_playwright_proxy(
        raw_proxy,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )
