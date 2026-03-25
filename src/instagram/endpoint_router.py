from __future__ import annotations

import asyncio
import json
import os
import re
from html import unescape
from http.cookies import SimpleCookie
from typing import Any, Dict, Optional

from .graphql_client import InstagramPublicHttpError, InstagramPublicRateLimit
from .profile_parser import InstagramPublicParseError, parse_profile_snapshot
from .session_pool import SessionContext
from src.network.http_client import HttpClient

from . import graphql_client as graphql_client_module


_HTTP: HttpClient | None = None


def _http() -> HttpClient:
    global _HTTP
    if _HTTP is None:
        _HTTP = HttpClient()
    return _HTTP


def _cookie_header(cookies: Dict[str, str]) -> str:
    parts: list[str] = []
    for k, v in (cookies or {}).items():
        key = str(k or "").strip()
        if not key:
            continue
        parts.append(f"{key}={str(v or '').strip()}")
    return "; ".join(parts)


def _update_session_cookies(session: SessionContext, response_headers: Dict[str, str]) -> None:
    if not isinstance(response_headers, dict):
        return
    lowered = {str(k).lower(): str(v) for k, v in response_headers.items()}
    raw = lowered.get("set-cookie", "")
    if not raw:
        return
    jar = SimpleCookie()
    try:
        jar.load(raw)
    except Exception:
        return
    for key, morsel in jar.items():
        value = str(getattr(morsel, "value", "") or "")
        if key and value:
            session.cookies[str(key)] = value


def _base_headers(session: SessionContext, *, normalized: str, accept: str) -> Dict[str, str]:
    headers = dict(session.headers or {})
    headers["User-Agent"] = str(session.user_agent or headers.get("User-Agent") or "").strip()
    headers["Accept"] = str(accept or headers.get("Accept") or "*/*").strip()
    headers.setdefault("Accept-Language", headers.get("Accept-Language") or "en-US,en;q=0.9")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")
    headers.setdefault("X-IG-App-ID", "936619743392459")
    headers.setdefault("X-ASBD-ID", "129477")
    headers["Referer"] = f"https://www.instagram.com/{normalized}/"

    cookie_value = _cookie_header(session.cookies or {})
    if cookie_value:
        headers["Cookie"] = cookie_value
        if "csrftoken" in (session.cookies or {}):
            headers.setdefault("X-CSRFToken", str(session.cookies.get("csrftoken") or "").strip())
    return headers


def _should_fallback_from_exc(exc: Exception) -> bool:
    if isinstance(exc, InstagramPublicRateLimit):
        return True
    if isinstance(exc, InstagramPublicHttpError) and int(getattr(exc, "status_code", 0) or 0) == 429:
        return True
    if isinstance(exc, InstagramPublicHttpError) and "rate_limit" in str(getattr(exc, "reason", "") or "").lower():
        return True
    if isinstance(exc, InstagramPublicParseError) and "payload_incomplete" in str(exc):
        return True
    return False


async def _fetch_graphql_payload(username: str, session: SessionContext) -> Dict[str, Any]:
    normalized = str(username or "").strip().lstrip("@")
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    doc_id = str(os.getenv("IG_PUBLIC_PROFILE_DOC_ID") or "").strip()
    if not doc_id:
        raise InstagramPublicParseError("payload_incomplete")

    url = "https://www.instagram.com/api/graphql"
    headers = _base_headers(session, normalized=normalized, accept="*/*")

    sleep_time = graphql_client_module._backoff.compute_sleep()
    if sleep_time > 0:
        print(
            f"[LEADS][BACKOFF] sleeping={sleep_time:.2f}s "
            f"consecutive_429={graphql_client_module._backoff.state.consecutive_429}"
        )
        await asyncio.sleep(sleep_time)
    await graphql_client_module._pacing.wait()

    res = await _http().get_json(
        url,
        params={"doc_id": str(doc_id).strip(), "variables": json.dumps({"username": normalized}, separators=(",", ":"))},
        headers=headers,
        proxy=session.proxy_url,
        timeout_seconds=12.0,
    )
    _update_session_cookies(session, res.headers)
    if res.status_code == 429:
        graphql_client_module._backoff.record_429()
        raise InstagramPublicRateLimit(429, reason="http_429", body=res.text)
    if res.status_code != 200:
        raise InstagramPublicHttpError(res.status_code, reason=f"http_{res.status_code}", body=res.text)
    if not isinstance(res.json, dict):
        raise InstagramPublicHttpError(res.status_code, reason="invalid_json", body=res.text)
    graphql_client_module._backoff.record_success()
    return res.json


async def _fetch_web_profile_info_payload(username: str, session: SessionContext) -> Dict[str, Any]:
    normalized = str(username or "").strip().lstrip("@")
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    url = "https://www.instagram.com/api/v1/users/web_profile_info/"
    headers = _base_headers(session, normalized=normalized, accept="*/*")

    sleep_time = graphql_client_module._backoff.compute_sleep()
    if sleep_time > 0:
        print(
            f"[LEADS][BACKOFF] sleeping={sleep_time:.2f}s "
            f"consecutive_429={graphql_client_module._backoff.state.consecutive_429}"
        )
        await asyncio.sleep(sleep_time)
    await graphql_client_module._pacing.wait()

    res = await _http().get_json(
        url,
        params={"username": normalized},
        headers=headers,
        proxy=session.proxy_url,
        timeout_seconds=12.0,
    )
    _update_session_cookies(session, res.headers)
    if res.status_code == 429:
        graphql_client_module._backoff.record_429()
        raise InstagramPublicRateLimit(429, reason="http_429", body=res.text)
    if res.status_code != 200:
        raise InstagramPublicHttpError(res.status_code, reason=f"http_{res.status_code}", body=res.text)
    if not isinstance(res.json, dict):
        raise InstagramPublicHttpError(res.status_code, reason="invalid_json", body=res.text)
    graphql_client_module._backoff.record_success()
    return res.json


_WINDOW_SHARED_DATA_RE = re.compile(r"window\._sharedData\s*=\s*", re.IGNORECASE)
_ADDITIONAL_DATA_RE = re.compile(r"__additionalDataLoaded\([^,]+,\s*", re.IGNORECASE)


def _extract_json_object(text: str, start_index: int) -> str:
    raw = str(text or "")
    idx = raw.find("{", start_index)
    if idx < 0:
        return ""
    depth = 0
    in_string = False
    escape = False
    for pos in range(idx, len(raw)):
        ch = raw[pos]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return raw[idx : pos + 1]
    return ""


def _user_from_html_payload(payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("data"), dict) and isinstance((payload.get("data") or {}).get("user"), dict):
        return (payload.get("data") or {}).get("user")  # type: ignore[return-value]
    entry = payload.get("entry_data")
    if isinstance(entry, dict):
        pages = entry.get("ProfilePage") or entry.get("profilePage")
        if isinstance(pages, list) and pages:
            page0 = pages[0]
            if isinstance(page0, dict):
                graphql = page0.get("graphql")
                if isinstance(graphql, dict) and isinstance(graphql.get("user"), dict):
                    return graphql.get("user")  # type: ignore[return-value]
                user = page0.get("user")
                if isinstance(user, dict):
                    return user
    graphql = payload.get("graphql")
    if isinstance(graphql, dict) and isinstance(graphql.get("user"), dict):
        return graphql.get("user")  # type: ignore[return-value]
    return None


async def _fetch_html_payload(username: str, session: SessionContext) -> Dict[str, Any]:
    normalized = str(username or "").strip().lstrip("@")
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    url = f"https://www.instagram.com/{normalized}/"
    headers = _base_headers(session, normalized=normalized, accept="text/html,application/xhtml+xml")

    res = await _http().get_json(
        url,
        headers=headers,
        proxy=session.proxy_url,
        timeout_seconds=15.0,
    )
    _update_session_cookies(session, res.headers)
    if res.status_code == 429:
        raise InstagramPublicRateLimit(429, reason="http_429", body=res.text)
    if res.status_code != 200:
        raise InstagramPublicHttpError(res.status_code, reason=f"http_{res.status_code}", body=res.text)

    html_text = str(res.text or "")
    candidates: list[str] = []

    for match in _WINDOW_SHARED_DATA_RE.finditer(html_text):
        extracted = _extract_json_object(html_text, match.end())
        if extracted:
            candidates.append(extracted)
    for match in _ADDITIONAL_DATA_RE.finditer(html_text):
        extracted = _extract_json_object(html_text, match.end())
        if extracted:
            candidates.append(extracted)

    for candidate in candidates:
        raw = unescape(str(candidate or "").strip())
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        user = _user_from_html_payload(payload)
        if isinstance(user, dict):
            return {"data": {"user": user}}

    raise InstagramPublicParseError("payload_incomplete")


async def fetch_profile_with_strategy(username: str, session: SessionContext):
    normalized = str(username or "").strip().lstrip("@")
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    last_exc: Exception | None = None

    print(f"[ENDPOINT_ROUTER_GRAPHQL] username={normalized} session_id={session.session_id}")
    try:
        payload = await _fetch_graphql_payload(normalized, session)
        return parse_profile_snapshot(payload, normalized)
    except Exception as exc:
        last_exc = exc
        if not _should_fallback_from_exc(exc):
            raise

    print(f"[ENDPOINT_ROUTER_WEB_PROFILE] username={normalized} session_id={session.session_id}")
    try:
        payload = await _fetch_web_profile_info_payload(normalized, session)
        return parse_profile_snapshot(payload, normalized)
    except Exception as exc:
        last_exc = exc
        if not _should_fallback_from_exc(exc):
            raise

    print(f"[ENDPOINT_ROUTER_HTML] username={normalized} session_id={session.session_id}")
    payload = await _fetch_html_payload(normalized, session)
    return parse_profile_snapshot(payload, normalized)
