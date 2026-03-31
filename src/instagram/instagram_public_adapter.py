from __future__ import annotations

import asyncio
import json
import os
from http.cookies import SimpleCookie
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Optional
from urllib.parse import urlparse

import httpx

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

from src.cache.profile_cache import ProfileCache
from src.instagram.graphql_client import InstagramPublicHttpError, InstagramPublicRateLimit
from src.instagram.profile_parser import (
    InstagramPublicParseError,
    parse_profile_snapshot,
    profile_snapshot_from_dict,
    profile_snapshot_to_dict,
)
from src.instagram.session_pool import get_session_pool
from src.network.http_client import HttpClient
from src.instagram import endpoint_router as endpoint_router_module


GLOBAL_ERROR_COUNT = 0
GLOBAL_COOLDOWN = 0.0

_GLOBAL_LOCK = asyncio.Lock()
_GLOBAL_WINDOW: Deque[bool] = deque(maxlen=50)

_CACHE: Optional[ProfileCache] = None


_IG_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)
_IG_DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
_IG_APP_ID = "936619743392459"
_IG_ASBD_ID = "129477"

_IG_REQUEST_TIMEOUT = (5.0, 10.0)  # (connect, read)
_IG_REQUEST_ATTEMPTS = 2
_IG_ADAPTER_WAIT_FOR_SECONDS = 25.0

_TIMEOUT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    asyncio.TimeoutError,
    httpx.TimeoutException,
)
if requests is not None:
    _TIMEOUT_EXCEPTIONS = _TIMEOUT_EXCEPTIONS + (requests.exceptions.Timeout,)

_HTTP: HttpClient | None = None


def _http() -> HttpClient:
    global _HTTP
    if _HTTP is None:
        _HTTP = HttpClient()
    return _HTTP


def _payload_has_user(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    return isinstance(data.get("user"), dict)


def _profile_payload_from_any(payload: Any) -> dict[str, Any] | None:
    """
    Normalizes known Instagram payload shapes into {"data": {"user": {...}}}.
    Returns None if user cannot be found.
    """
    if not isinstance(payload, dict):
        return None

    if _payload_has_user(payload):
        return payload  # type: ignore[return-value]

    # Some endpoints may return user at top-level.
    user = payload.get("user")
    if isinstance(user, dict):
        return {"data": {"user": user}}

    # Some variants nest under "data" but not in the expected shape.
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("user"), dict):
        return {"data": {"user": data.get("user")}}

    return None


def _update_session_cookies(session: Any, response_headers: dict[str, str] | None) -> None:
    if not response_headers or not isinstance(response_headers, dict):
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
        value = str(getattr(morsel, "value", "") or "").strip()
        if key and value:
            try:
                session.cookies[str(key)] = value
            except Exception:
                continue


async def _fetch_web_profile_info_payload(username: str, session: Any) -> dict[str, Any]:
    normalized = _normalize_username(username)
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    url = "https://i.instagram.com/api/v1/users/web_profile_info/"

    try:
        from src.instagram import graphql_client as graphql_client_module

        sleep_time = graphql_client_module._backoff.compute_sleep()
        if sleep_time > 0:
            print(
                f"[LEADS][BACKOFF] sleeping={sleep_time:.2f}s "
                f"consecutive_429={graphql_client_module._backoff.state.consecutive_429}"
            )
            await asyncio.sleep(sleep_time)
        await graphql_client_module._pacing.wait()
    except Exception:
        pass

    headers = build_instagram_headers(
        user_agent=str(getattr(session, "user_agent", "") or "").strip() or None,
        cookies=getattr(session, "cookies", None),
    )
    headers["Referer"] = f"https://www.instagram.com/{normalized}/"
    headers.update(
        {
            "x-ig-app-id": "936619743392459",
            "x-asbd-id": "198387",
            "x-ig-www-claim": "0",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.instagram.com",
            "referer": "https://www.instagram.com/",
            "user-agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/15.0 Mobile/15E148 Safari/604.1"
            ),
        }
    )
    print(f"[INSTAGRAM_BROWSER_HEADERS_FORCED] username={normalized}")

    res = await _http().get_json(
        url,
        params={"username": normalized},
        headers=headers,
        proxy=str(getattr(session, "proxy_url", "") or "").strip() or None,
        timeout_seconds=12.0,
    )
    _update_session_cookies(session, getattr(res, "headers", None))

    if int(getattr(res, "status_code", 0) or 0) == 429:
        try:
            from src.instagram import graphql_client as graphql_client_module

            graphql_client_module._backoff.record_429()
        except Exception:
            pass
        raise InstagramPublicRateLimit(429, reason="http_429", body=str(getattr(res, "text", "") or ""))

    if int(getattr(res, "status_code", 0) or 0) != 200:
        raise InstagramPublicHttpError(
            int(getattr(res, "status_code", 0) or 0),
            reason=f"http_{int(getattr(res, 'status_code', 0) or 0)}",
            body=str(getattr(res, "text", "") or ""),
        )

    payload = getattr(res, "json", None)
    if not isinstance(payload, dict):
        raise InstagramPublicHttpError(200, reason="invalid_json", body=str(getattr(res, "text", "") or ""))

    try:
        from src.instagram import graphql_client as graphql_client_module

        graphql_client_module._backoff.record_success()
    except Exception:
        pass

    return payload  # type: ignore[return-value]


async def _fetch_profile_multi_endpoint(username: str, session: Any):
    """
    Multi-endpoint resolution for cases where GraphQL returns HTTP 200 but empty/incomplete payload.

    Order:
      1) GraphQL
      2) web_profile_info (i.instagram.com)
      3) HTML profile (window._sharedData)

    Returns a ProfileSnapshot or None.
    """
    normalized = _normalize_username(username)
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    graphql_empty = False

    # 1) GRAPHQL
    try:
        payload = await endpoint_router_module._fetch_graphql_payload(normalized, session)  # type: ignore[attr-defined]
        normalized_payload = _profile_payload_from_any(payload)
        if normalized_payload is None:
            graphql_empty = True
            data_flag = "data_none" if isinstance(payload, dict) and payload.get("data") is None else "no_user"
            print(f"[ENDPOINT_GRAPHQL_EMPTY] username={normalized} reason={data_flag}")
        else:
            profile = parse_profile_snapshot(normalized_payload, normalized)
            if profile is None:
                graphql_empty = True
                print(f"[ENDPOINT_GRAPHQL_EMPTY] username={normalized} reason=parse_none")
            else:
                return profile
    except InstagramPublicRateLimit:
        raise
    except InstagramPublicParseError as exc:
        graphql_empty = True
        print(f"[ENDPOINT_GRAPHQL_EMPTY] username={normalized} reason={str(exc) or type(exc).__name__}")
    except InstagramPublicHttpError:
        raise
    except Exception:
        raise

    # 2) WEB PROFILE INFO
    try:
        payload = await _fetch_web_profile_info_payload(normalized, session)
        normalized_payload = _profile_payload_from_any(payload)
        if normalized_payload is not None:
            profile = parse_profile_snapshot(normalized_payload, normalized)
            if profile is not None:
                print(f"[ENDPOINT_WEB_PROFILE_OK] username={normalized}")
                if graphql_empty:
                    print(f"[PROFILE_RESOLVED_MULTI_ENDPOINT] username={normalized} via=web_profile_info")
                return profile
    except InstagramPublicRateLimit:
        raise
    except InstagramPublicHttpError:
        raise
    except Exception:
        raise

    # 3) HTML PROFILE FALLBACK
    try:
        payload = await endpoint_router_module._fetch_html_payload(normalized, session)  # type: ignore[attr-defined]
        normalized_payload = _profile_payload_from_any(payload)
        if normalized_payload is not None:
            profile = parse_profile_snapshot(normalized_payload, normalized)
            if profile is not None:
                print(f"[ENDPOINT_HTML_FALLBACK] username={normalized}")
                if graphql_empty:
                    print(f"[PROFILE_RESOLVED_MULTI_ENDPOINT] username={normalized} via=html")
                return profile
    except InstagramPublicRateLimit:
        raise
    except InstagramPublicParseError:
        return None
    except InstagramPublicHttpError:
        raise
    except Exception:
        raise

    return None


def _cookie_header_from_dict(cookies: dict[str, str] | None) -> str:
    parts: list[str] = []
    for key, value in (cookies or {}).items():
        k = str(key or "").strip()
        v = str(value or "").strip()
        if not k or not v:
            continue
        parts.append(f"{k}={v}")
    return "; ".join(parts)


def _cookies_from_header(cookie_header: str) -> dict[str, str]:
    raw = str(cookie_header or "").strip()
    if not raw:
        return {}
    jar = SimpleCookie()
    try:
        jar.load(raw)
    except Exception:
        return {}
    parsed: dict[str, str] = {}
    for key, morsel in jar.items():
        value = str(getattr(morsel, "value", "") or "").strip()
        if key and value:
            parsed[str(key)] = value
    return parsed


def _normalize_cookie_header(cookies: object | None) -> str:
    if cookies is None:
        return ""
    if isinstance(cookies, str):
        return str(cookies).strip()
    if isinstance(cookies, dict):
        try:
            return _cookie_header_from_dict({str(k): str(v) for k, v in cookies.items()})
        except Exception:
            return ""
    return ""


def build_cookie_header(cookies: Any) -> str:
    """
    Build a Cookie header string from Playwright-style cookies rows:
    [{"name": "...", "value": "...", "domain": "...", ...}, ...]
    """
    if not cookies:
        return ""
    if isinstance(cookies, str):
        return str(cookies).strip()
    if isinstance(cookies, dict):
        return _cookie_header_from_dict({str(k): str(v) for k, v in cookies.items()})
    if not isinstance(cookies, list):
        return ""
    jar: dict[str, str] = {}
    for row in cookies:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "").strip()
        if name and value:
            jar[name] = value
    return _cookie_header_from_dict(jar)


def build_instagram_headers(user_agent: str | None = None, cookies: object | None = None) -> dict[str, str]:
    ua = str(user_agent or "").strip() or _IG_DEFAULT_UA
    headers: dict[str, str] = {
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": _IG_DEFAULT_ACCEPT_LANGUAGE,
        "X-IG-App-ID": _IG_APP_ID,
        "X-ASBD-ID": _IG_ASBD_ID,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.instagram.com/",
        "Origin": "https://www.instagram.com",
    }
    cookie_value = _normalize_cookie_header(cookies)
    if cookie_value:
        headers["Cookie"] = cookie_value
        csrf_token = ""
        try:
            if isinstance(cookies, dict):
                csrf_token = str(cookies.get("csrftoken") or "").strip()
            elif isinstance(cookies, str):
                csrf_token = str(_cookies_from_header(cookie_value).get("csrftoken") or "").strip()
        except Exception:
            csrf_token = ""
        if csrf_token:
            headers.setdefault("X-CSRFToken", csrf_token)
    return headers


def _is_instagram_cookie_domain(domain: str) -> bool:
    normalized = str(domain or "").strip().lower().lstrip(".")
    if not normalized:
        return False
    return normalized == "instagram.com" or normalized.endswith(".instagram.com")


def _cookie_row_matches_instagram(row: dict[str, Any]) -> bool:
    domain = str(row.get("domain") or "").strip()
    if domain and _is_instagram_cookie_domain(domain):
        return True
    url = str(row.get("url") or "").strip()
    if not url:
        return False
    try:
        host = str(urlparse(url).hostname or "").strip().lower()
    except Exception:
        return False
    return _is_instagram_cookie_domain(host)


def _playwright_cookies_to_jar(cookies: list[dict[str, Any]] | None) -> dict[str, str]:
    jar: dict[str, str] = {}
    for row in cookies or []:
        if not isinstance(row, dict):
            continue
        if not _cookie_row_matches_instagram(row):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "").strip()
        if name and value:
            jar[name] = value
    return jar


def _account_identity(account: Any) -> tuple[str, str]:
    """
    Returns (account_id, username) for logging and storage lookup.
    """
    if not account:
        return "", ""
    try:
        if isinstance(account, dict):
            username = str(
                account.get("username")
                or account.get("account_username")
                or account.get("user")
                or ""
            ).strip().lstrip("@")
            account_id = str(
                account.get("account_id")
                or account.get("id")
                or account.get("pk")
                or username
                or ""
            ).strip()
            return account_id, username
        username = str(getattr(account, "username", "") or "").strip().lstrip("@")
        account_id = str(getattr(account, "account_id", "") or getattr(account, "id", "") or username or "").strip()
        return account_id, username
    except Exception:
        return "", ""


def _resolve_profiles_root() -> Path | None:
    """
    Best-effort resolution of the Playwright profiles root without assuming
    Playwright is importable at module import time.
    """
    profiles_env = (os.environ.get("PROFILES_DIR") or "").strip()
    if profiles_env:
        candidate = Path(profiles_env).expanduser()
        try:
            if candidate.is_absolute():
                return candidate
        except Exception:
            pass
        try:
            base_root = Path(__file__).resolve().parents[2]
        except Exception:
            base_root = Path.cwd()
        return (base_root / candidate).resolve()

    local_app_data = (os.environ.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        from paths import browser_profiles_root

        return browser_profiles_root(Path(local_app_data).expanduser() / "InstaCRM")

    # Fallback to legacy location under project runtime/
    try:
        from paths import browser_profiles_root, runtime_base

        base_root = runtime_base(Path(__file__).resolve().parents[2])
        return browser_profiles_root(base_root)
    except Exception:
        return None


def _storage_state_cookies(username: str) -> list[dict[str, Any]]:
    user = str(username or "").strip().lstrip("@")
    if not user:
        return []
    root = _resolve_profiles_root()
    if root is None:
        return []
    path = (Path(root) / user / "storage_state.json")
    try:
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    cookies = payload.get("cookies")
    return cookies if isinstance(cookies, list) else []


async def _runtime_context_cookies(account: Any) -> list[dict[str, Any]]:
    """
    Best-effort: if caller passed a runtime-like object (or dict) with a Playwright
    page/context, read cookies from the live context.
    """
    if not account:
        return []
    page = None
    try:
        if isinstance(account, dict):
            page = account.get("page")
            runtime = account.get("runtime") or account.get("_runtime")
            if page is None and runtime is not None:
                page = getattr(runtime, "page", None)
        else:
            page = getattr(account, "page", None)
    except Exception:
        page = None
    if page is None:
        return []
    try:
        context = getattr(page, "context", None)
        if callable(context):
            context = context()
        if context is None:
            return []
        cookies = await context.cookies()
        return cookies if isinstance(cookies, list) else []
    except Exception:
        return []


async def _resolve_instagram_session_cookies(account: Any) -> tuple[dict[str, str], str, str, str]:
    """
    Returns (cookie_jar, cookie_header, account_id, source).
    Sources: "playwright_context" | "storage_state" | "".
    """
    account_id, username = _account_identity(account)

    cookies = await _runtime_context_cookies(account)
    jar = _playwright_cookies_to_jar(cookies)
    if jar:
        return jar, build_cookie_header(jar), account_id, "playwright_context"

    if username:
        cookies = _storage_state_cookies(username)
        jar = _playwright_cookies_to_jar([row for row in cookies if isinstance(row, dict)])
        if jar:
            return jar, build_cookie_header(jar), account_id, "storage_state"

    return {}, "", account_id, ""


def _extract_optional_cookies(account: Any) -> object | None:
    if not account:
        return None
    try:
        if isinstance(account, dict):
            cookie_header = str(account.get("cookie_header") or account.get("cookie") or "").strip()
            cookies_obj = account.get("cookies")
            if isinstance(cookies_obj, dict) and cookies_obj:
                return cookies_obj
            if isinstance(cookies_obj, str) and str(cookies_obj).strip():
                return str(cookies_obj).strip()
            return cookie_header or None

        cookie_header = str(getattr(account, "cookie_header", "") or getattr(account, "cookie", "") or "").strip()
        cookies_obj = getattr(account, "cookies", None)
        if isinstance(cookies_obj, dict) and cookies_obj:
            return cookies_obj
        if isinstance(cookies_obj, str) and str(cookies_obj).strip():
            return str(cookies_obj).strip()
        return cookie_header or None
    except Exception:
        return None


def _payload_login_required(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    if "data" in payload and payload.get("data") is None:
        return True

    def _truthy_flag(value: Any) -> bool:
        if value is True:
            return True
        if isinstance(value, (int, float)) and value == 1:
            return True
        if isinstance(value, str) and value.strip().lower() in ("1", "true", "yes"):
            return True
        return False

    if _truthy_flag(payload.get("require_login")) or _truthy_flag(payload.get("login_required")):
        return True

    message = str(payload.get("message") or payload.get("error") or "").lower()
    if "login_required" in message or "require_login" in message:
        return True

    def _scan_values(value: Any) -> bool:
        if isinstance(value, str):
            raw = value.lower()
            return ("login_required" in raw) or ("require_login" in raw)
        if isinstance(value, dict):
            return any(_scan_values(v) for v in value.values())
        if isinstance(value, list):
            return any(_scan_values(v) for v in value)
        return False

    return _scan_values(payload.get("errors")) or _scan_values(payload.get("error")) or _scan_values(payload.get("status"))


async def _probe_login_required(username: str, session: Any) -> bool:
    normalized = str(username or "").strip().lstrip("@")
    if not normalized:
        return False

    headers = build_instagram_headers(
        user_agent=str(getattr(session, "user_agent", "") or "").strip() or None,
        cookies=getattr(session, "cookies", None),
    )
    headers["Referer"] = f"https://www.instagram.com/{normalized}/"
    headers.update(
        {
            "x-ig-app-id": "936619743392459",
            "x-asbd-id": "198387",
            "x-ig-www-claim": "0",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.instagram.com",
            "referer": "https://www.instagram.com/",
            "user-agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/15.0 Mobile/15E148 Safari/604.1"
            ),
        }
    )
    print(f"[INSTAGRAM_BROWSER_HEADERS_FORCED] username={normalized} action=probe_login_required")

    try:
        from src.instagram import graphql_client as graphql_client_module

        await graphql_client_module._pacing.wait()
    except Exception:
        pass

    proxy_url = str(getattr(session, "proxy_url", "") or "").strip() or None

    res = None
    for attempt in range(_IG_REQUEST_ATTEMPTS):
        if attempt > 0:
            print(
                f"[INSTAGRAM_RETRY] action=probe_login_required username={normalized} "
                f"attempt={attempt + 1}/{_IG_REQUEST_ATTEMPTS} proxy={proxy_url}"
            )
        try:
            res = await _http().get_json(
                "https://www.instagram.com/api/v1/users/web_profile_info/",
                params={"username": normalized},
                headers=headers,
                proxy=proxy_url,
                timeout_seconds=float(_IG_REQUEST_TIMEOUT[1]),
            )
            break
        except _TIMEOUT_EXCEPTIONS as exc:
            print(
                f"[INSTAGRAM_REQUEST_TIMEOUT] action=probe_login_required username={normalized} "
                f"attempt={attempt + 1}/{_IG_REQUEST_ATTEMPTS} proxy={proxy_url} error={type(exc).__name__}"
            )
            res = None
            continue
        except Exception:
            return False

    if res is None:
        return False

    if int(getattr(res, "status_code", 0) or 0) != 200:
        return False

    payload = getattr(res, "json", None)
    if payload is None:
        return False
    return _payload_login_required(payload)


def _now() -> float:
    return time.time()


def _normalize_username(value: str) -> str:
    return str(value or "").strip().lstrip("@")


def _cache() -> ProfileCache:
    global _CACHE
    if _CACHE is None:
        _CACHE = ProfileCache(ttl_seconds=24.0 * 3600.0)
    return _CACHE


async def _maybe_global_cooldown_wait() -> None:
    global GLOBAL_COOLDOWN
    async with _GLOBAL_LOCK:
        cooldown_until = float(GLOBAL_COOLDOWN or 0.0)
    now = _now()
    if cooldown_until > now:
        await asyncio.sleep(cooldown_until - now)


async def _record_global_result(ok: bool) -> None:
    global GLOBAL_ERROR_COUNT, GLOBAL_COOLDOWN
    async with _GLOBAL_LOCK:
        _GLOBAL_WINDOW.append(bool(ok))
        if not ok:
            GLOBAL_ERROR_COUNT += 1

        window = list(_GLOBAL_WINDOW)
        if len(window) < 10:
            return
        failures = sum(1 for item in window if not item)
        ratio = failures / max(1, len(window))
        if ratio > 0.30:
            GLOBAL_COOLDOWN = max(float(GLOBAL_COOLDOWN or 0.0), _now() + 120.0)


async def fetch_profile(username: str, account: Any = None, proxy_url: str | None = None):
    normalized = _normalize_username(username)
    start_time = time.time()

    print(
        f"[LEADS][ADAPTER_REQUEST] username={username}"
    )
    if not normalized:
        raise InstagramPublicHttpError(0, reason="username_vacio")

    cached = await _cache().get(normalized)
    if isinstance(cached, dict):
        return profile_snapshot_from_dict(cached)

    await _maybe_global_cooldown_wait()

    session_pool = get_session_pool()
    session = await session_pool.acquire_session()
    selected_proxy_url = str(proxy_url or "").strip() or None
    try:
        if selected_proxy_url:
            session_pool.apply_proxy_override(session, selected_proxy_url)
        else:
            await session_pool.ensure_assigned_proxy(session)
            selected_proxy_url = session.proxy_url
    except Exception as exc:
        await session_pool.report_failure(session, f"session_setup:{type(exc).__name__}")
        raise

    print(f"[LEADS][ADAPTER_PROXY] username={username} proxy={selected_proxy_url}")
    try:
        optional_cookies = _extract_optional_cookies(account)
        if optional_cookies:
            if isinstance(optional_cookies, dict):
                session.cookies.update({str(k): str(v) for k, v in optional_cookies.items() if str(k).strip() and str(v).strip()})
            else:
                injected = _cookies_from_header(str(optional_cookies))
                if injected:
                    session.cookies.update(injected)

        session_cookie_jar, session_cookie_header, session_account_id, session_cookie_source = (
            await _resolve_instagram_session_cookies(account)
        )
        if session_cookie_jar:
            session.cookies.update(session_cookie_jar)
            setattr(session, "_ig_session_cookie_names", set(session_cookie_jar.keys()))
            setattr(session, "_ig_session_cookie_account_id", str(session_account_id or "").strip())
            if session_cookie_header:
                session.headers["Cookie"] = str(session_cookie_header).strip()
            print(
                f"[INSTAGRAM_SESSION_COOKIES_APPLIED] username={username} "
                f"account_id={session_account_id or '-'} source={session_cookie_source or '-'} "
                f"cookies={len(session_cookie_jar)}"
            )
        else:
            prev_names = getattr(session, "_ig_session_cookie_names", None)
            if isinstance(prev_names, (set, list, tuple)):
                for name in prev_names:
                    session.cookies.pop(str(name), None)
            setattr(session, "_ig_session_cookie_names", set())
            setattr(session, "_ig_session_cookie_account_id", "")

        base_headers = build_instagram_headers(
            user_agent=str(getattr(session, "user_agent", "") or "").strip() or None,
            cookies=session.cookies,
        )
        session.headers.update(base_headers)
        session.user_agent = str(session.headers.get("User-Agent") or session.user_agent or "").strip()
        print(
            f"[INSTAGRAM_HEADERS_APPLIED] username={username} session_id={session.session_id} "
            f"proxy={selected_proxy_url} cookies={'yes' if bool(session.cookies) else 'no'}"
        )

        print(f"[LEADS][ADAPTER_FETCH] username={username}")
        profile = None
        last_timeout_exc: BaseException | None = None
        for attempt in range(_IG_REQUEST_ATTEMPTS):
            try:
                profile = await asyncio.wait_for(
                    _fetch_profile_multi_endpoint(normalized, session),
                    timeout=float(_IG_ADAPTER_WAIT_FOR_SECONDS),
                )
                last_timeout_exc = None
                break
            except _TIMEOUT_EXCEPTIONS as exc:
                last_timeout_exc = exc
                print(
                    f"[INSTAGRAM_REQUEST_TIMEOUT] username={username} "
                    f"attempt={attempt + 1}/{_IG_REQUEST_ATTEMPTS} proxy={selected_proxy_url} "
                    f"error={type(exc).__name__}"
                )
                if attempt + 1 < _IG_REQUEST_ATTEMPTS:
                    print(
                        f"[INSTAGRAM_RETRY] username={username} "
                        f"attempt={attempt + 2}/{_IG_REQUEST_ATTEMPTS} proxy={selected_proxy_url}"
                    )

        if last_timeout_exc is not None:
            await session_pool.report_failure(session, "timeout")
            await _record_global_result(False)
            return None
    except InstagramPublicRateLimit:
        print(
            f"[LEADS][ADAPTER_429] username={username} proxy={selected_proxy_url}"
        )
        await session_pool.report_failure(session, "rate_limit")
        await _record_global_result(False)
        raise
    except InstagramPublicParseError as exc:
        reason = str(exc) or type(exc).__name__
        await session_pool.report_success(session)
        await _record_global_result(True)
        elapsed = round(time.time() - start_time, 2)
        print(
            f"[PROFILE_EMPTY] username={username} proxy={selected_proxy_url} reason={reason} time={elapsed}s"
        )
        return None
    except InstagramPublicHttpError as exc:
        reason = str(getattr(exc, "reason", "") or type(exc).__name__) or type(exc).__name__
        await session_pool.report_failure(session, reason)
        await _record_global_result(False)
        raise
    except Exception as exc:
        print(
            f"[LEADS][ADAPTER_FAIL] username={username} "
            f"error={type(exc).__name__}"
        )
        await session_pool.report_failure(session, type(exc).__name__)
        await _record_global_result(False)
        raise InstagramPublicHttpError(0, reason=f"request_error:{type(exc).__name__}") from exc
    else:
        await session_pool.report_success(session)
        await _record_global_result(True)
        if profile is None:
            elapsed = round(time.time() - start_time, 2)
            if await _probe_login_required(normalized, session):
                print(
                    f"[INSTAGRAM_LOGIN_REQUIRED] username={username} proxy={selected_proxy_url} time={elapsed}s"
                )
                print(f"[LEADS][ADAPTER_OK] username={username} followers=0 time={elapsed}s")
                return None
            print(f"[PROFILE_EMPTY] username={username} proxy={selected_proxy_url} time={elapsed}s")
            print(f"[LEADS][ADAPTER_OK] username={username} followers=0 time={elapsed}s")
            return None
        elapsed = round(time.time() - start_time, 2)
        print(
            f"[LEADS][ADAPTER_OK] username={username} "
            f"followers={profile.follower_count} "
            f"time={elapsed}s"
        )
        print(
            f"[PROFILE_PARSED_OK] username={username} followers={profile.follower_count} time={elapsed}s"
        )
        await _cache().set(normalized, profile_snapshot_to_dict(profile))
        return profile
