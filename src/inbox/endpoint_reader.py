from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

try:
    import requests
except Exception:  # pragma: no cover - optional dependency during static checks
    requests = None  # type: ignore[assignment]

from src.browser_profile_paths import browser_storage_state_path
from src.dm_playwright_client import (
    THREAD_URL_TEMPLATE,
    _extract_inbox_cursor,
    _extract_inbox_threads_from_payload,
)
from src.inbox.conversation_sync import (
    _append_cache_bust_query,
    _build_inbox_candidate_urls,
    _payload_to_messages,
    _snapshot_to_thread_row,
)
from src.playwright_service import BASE_PROFILES
from src.proxy_payload import proxy_from_account

logger = logging.getLogger(__name__)

_INSTAGRAM_BASE_URL = "https://www.instagram.com"
_DEFAULT_ACCEPT_LANGUAGE = "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7"
_DEFAULT_IG_APP_ID = "936619743392459"
_DEFAULT_ASBD_ID = "198387"
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
_REQUIRED_COOKIE_NAMES = ("sessionid", "csrftoken")


class InboxEndpointError(RuntimeError):
    def __init__(self, kind: str, detail: str = "", *, status_code: int = 0) -> None:
        self.kind = str(kind or "").strip().lower() or "unknown"
        self.detail = str(detail or "").strip() or self.kind
        self.status_code = max(0, int(status_code or 0))
        super().__init__(self.detail)


class _AccountEndpointClient:
    def __init__(self, account: dict[str, Any], *, profiles_root: Path | None = None) -> None:
        self._account = dict(account or {})
        self._profiles_root = Path(profiles_root or BASE_PROFILES)
        self._account_id = str(self._account.get("username") or "").strip().lstrip("@").lower()
        self._cookie_map: dict[str, str] = {}
        self._session = self._build_session()

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def self_user_id(self) -> str:
        return str(self._cookie_map.get("ds_user_id") or self._account_id).strip()

    def close(self) -> None:
        session = self._session
        self._session = None
        if session is None:
            return
        with ExceptionSuppressor():
            session.close()

    def fetch_json_candidates(
        self,
        urls: list[str],
        *,
        referer: str,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        if self._session is None:
            raise InboxEndpointError("network_error", "endpoint_session_closed")
        session = self._session
        last_error: InboxEndpointError | None = None
        max_attempts = 3
        base_backoff_seconds = min(3.0, max(0.5, float(timeout_seconds or 10.0) * 0.2))
        for candidate in urls:
            absolute_url = _absolute_instagram_url(candidate)
            if not absolute_url:
                continue
            for attempt in range(1, max_attempts + 1):
                try:
                    response = session.get(
                        absolute_url,
                        headers={"Referer": _absolute_instagram_url(referer) or f"{_INSTAGRAM_BASE_URL}/"},
                        timeout=(max(2.0, float(timeout_seconds) * 0.5), max(2.0, float(timeout_seconds))),
                        allow_redirects=True,
                    )
                except requests.exceptions.ProxyError as exc:  # type: ignore[union-attr]
                    raise InboxEndpointError("proxy_error", str(exc)) from exc
                except requests.exceptions.Timeout as exc:  # type: ignore[union-attr]
                    last_error = InboxEndpointError("network_error", "endpoint_timeout")
                    logger.warning(
                        "Inbox endpoint timeout account=@%s url=%s attempt=%s/%s error=%s",
                        self._account_id,
                        absolute_url,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    if attempt < max_attempts:
                        time.sleep(base_backoff_seconds * attempt)
                    continue
                except requests.exceptions.RequestException as exc:  # type: ignore[union-attr]
                    last_error = InboxEndpointError("network_error", str(exc) or "endpoint_request_failed")
                    logger.warning(
                        "Inbox endpoint request failed account=@%s url=%s attempt=%s/%s error=%s",
                        self._account_id,
                        absolute_url,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    if attempt < max_attempts:
                        time.sleep(base_backoff_seconds * attempt)
                    continue

                raw_text = ""
                with ExceptionSuppressor():
                    raw_text = response.text or ""
                error_kind = _response_error_kind(
                    status_code=int(response.status_code or 0),
                    response_url=str(getattr(response, "url", "") or ""),
                    response_text=raw_text,
                )
                if error_kind is not None:
                    error = InboxEndpointError(
                        error_kind,
                        _response_error_detail(error_kind, response.status_code, raw_text),
                        status_code=int(response.status_code or 0),
                    )
                    if error_kind in {"login_required", "checkpoint", "suspended", "banned"}:
                        raise error
                    last_error = error
                    should_retry = (
                        error_kind == "rate_limit"
                        or (error_kind == "http_error" and int(response.status_code or 0) >= 500)
                    )
                    log_fn = logger.warning if should_retry else logger.info
                    log_fn(
                        "Inbox endpoint response account=@%s url=%s attempt=%s/%s kind=%s status=%s",
                        self._account_id,
                        absolute_url,
                        attempt,
                        max_attempts,
                        error_kind,
                        int(response.status_code or 0),
                    )
                    if should_retry and attempt < max_attempts:
                        time.sleep(base_backoff_seconds * attempt)
                        continue
                    break

                try:
                    payload = response.json()
                except ValueError as exc:
                    detail = "invalid_json"
                    if _looks_like_auth_surface(str(getattr(response, "url", "") or ""), raw_text):
                        raise InboxEndpointError(
                            "login_required",
                            "endpoint_auth_surface",
                            status_code=int(response.status_code or 0),
                        ) from exc
                    last_error = InboxEndpointError("invalid_response", detail, status_code=int(response.status_code or 0))
                    logger.warning(
                        "Inbox endpoint invalid json account=@%s url=%s attempt=%s/%s",
                        self._account_id,
                        absolute_url,
                        attempt,
                        max_attempts,
                    )
                    if attempt < max_attempts:
                        time.sleep(base_backoff_seconds * attempt)
                        continue
                    break
                if isinstance(payload, dict):
                    return payload
                last_error = InboxEndpointError(
                    "invalid_response",
                    "endpoint_payload_not_object",
                    status_code=int(response.status_code or 0),
                )
                logger.warning(
                    "Inbox endpoint invalid payload account=@%s url=%s attempt=%s/%s",
                    self._account_id,
                    absolute_url,
                    attempt,
                    max_attempts,
                )
                if attempt < max_attempts:
                    time.sleep(base_backoff_seconds * attempt)
                    continue
                break
        if last_error is not None:
            raise last_error
        raise InboxEndpointError("network_error", "endpoint_unreachable")

    def _build_session(self):
        if requests is None:
            raise InboxEndpointError("requests_missing", "requests_unavailable")
        if not self._account_id:
            raise InboxEndpointError("login_required", "account_missing_username")
        storage_state_path = browser_storage_state_path(
            self._account_id,
            profiles_root=self._profiles_root,
        )
        if not storage_state_path.exists():
            raise InboxEndpointError("login_required", f"storage_state_missing:{self._account_id}")
        payload = _load_storage_state_payload(storage_state_path)
        cookie_names = {str(item.get("name") or "").strip(): str(item.get("value") or "") for item in payload.get("cookies", []) if isinstance(item, dict)}
        self._cookie_map = dict(cookie_names)
        missing = [name for name in _REQUIRED_COOKIE_NAMES if not str(cookie_names.get(name) or "").strip()]
        if missing:
            raise InboxEndpointError("login_required", f"storage_state_missing_cookies:{','.join(missing)}")

        session = requests.Session()
        session.trust_env = False
        session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": str(
                    self._account.get("accept_language")
                    or self._account.get("language")
                    or _DEFAULT_ACCEPT_LANGUAGE
                ).strip()
                or _DEFAULT_ACCEPT_LANGUAGE,
                "Cache-Control": "no-cache, no-store, max-age=0",
                "Pragma": "no-cache",
                "User-Agent": str(self._account.get("user_agent") or _DEFAULT_USER_AGENT).strip() or _DEFAULT_USER_AGENT,
                "X-ASBD-ID": str(
                    self._account.get("x_asbd_id")
                    or self._account.get("asbd_id")
                    or _DEFAULT_ASBD_ID
                ).strip()
                or _DEFAULT_ASBD_ID,
                "X-CSRFToken": str(cookie_names.get("csrftoken") or "").strip(),
                "X-IG-App-ID": str(
                    self._account.get("x_ig_app_id")
                    or self._account.get("ig_app_id")
                    or _DEFAULT_IG_APP_ID
                ).strip()
                or _DEFAULT_IG_APP_ID,
                "X-Requested-With": "XMLHttpRequest",
            }
        )
        session.cookies = requests.cookies.RequestsCookieJar()
        for row in payload.get("cookies", []):
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "").strip()
            value = str(row.get("value") or "")
            if not name:
                continue
            domain = str(row.get("domain") or ".instagram.com").strip() or ".instagram.com"
            path = str(row.get("path") or "/").strip() or "/"
            with ExceptionSuppressor():
                session.cookies.set(name, value, domain=domain, path=path)
        proxies = _requests_proxy_map(proxy_from_account(self._account))
        if proxies:
            session.proxies.update(proxies)
        return session


def fetch_account_threads_page_from_storage(
    account: dict[str, Any],
    *,
    cursor: str = "",
    limit: int = 20,
    message_limit: int = 20,
    profiles_root: Path | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    client = _AccountEndpointClient(account, profiles_root=profiles_root)
    try:
        account_id = client.account_id
        if not account_id:
            raise InboxEndpointError("login_required", "account_missing_username")
        safe_limit = max(1, min(200, int(limit or 20)))
        safe_message_limit = max(1, min(80, int(message_limit or 20)))
        payload = client.fetch_json_candidates(
            [
                _absolute_instagram_url(url)
                for url in _build_inbox_candidate_urls(
                    cursor=str(cursor or "").strip(),
                    limit=safe_limit,
                    message_limit=safe_message_limit,
                )
            ],
            referer=f"{_INSTAGRAM_BASE_URL}/direct/inbox/",
            timeout_seconds=max(2.0, float(timeout_seconds or 10.0)),
        )
        self_user_id = str(getattr(client, "self_user_id", "") or account_id).strip()
        snapshots = _extract_inbox_threads_from_payload(
            payload,
            self_user_id=self_user_id,
            self_username=account_id,
            message_limit=safe_message_limit,
            thread_limit=safe_limit,
        )
        next_cursor, has_more = _extract_inbox_cursor(payload)
        return {
            "threads": snapshots,
            "cursor": next_cursor,
            "has_more": bool(has_more),
            "source_url": "",
            "status": 200,
            "payload": payload,
        }
    finally:
        client.close()


def sync_account_threads_from_storage(
    account: dict[str, Any],
    *,
    thread_limit: int = 120,
    message_limit: int = 12,
    max_pages: int = 2,
    profiles_root: Path | None = None,
    timeout_seconds: float = 10.0,
) -> list[dict[str, Any]]:
    client = _AccountEndpointClient(account, profiles_root=profiles_root)
    try:
        account_id = client.account_id
        if not account_id:
            return []
        account_alias = str(account.get("alias") or "").strip()
        self_user_id = str(getattr(client, "self_user_id", "") or account_id).strip()
        target_total = max(1, min(120, int(thread_limit or 120)))
        per_page = max(10, min(200, target_total))
        pages = max(1, min(4, int(max_pages or 2)))
        cursor = ""
        collected: list[dict[str, Any]] = []
        seen_thread_ids: set[str] = set()
        for _ in range(pages):
            payload = client.fetch_json_candidates(
                [
                    _absolute_instagram_url(url)
                    for url in _build_inbox_candidate_urls(
                        cursor=str(cursor or "").strip(),
                        limit=min(per_page, max(1, target_total - len(collected))),
                        message_limit=max(1, min(20, int(message_limit or 12))),
                    )
                ],
                referer=f"{_INSTAGRAM_BASE_URL}/direct/inbox/",
                timeout_seconds=timeout_seconds,
            )
            snapshots = _extract_inbox_threads_from_payload(
                payload,
                self_user_id=self_user_id,
                self_username=account_id,
                message_limit=max(1, min(20, int(message_limit or 12))),
                thread_limit=min(per_page, max(1, target_total - len(collected))),
            )
            for snapshot in snapshots:
                if not isinstance(snapshot, dict):
                    continue
                thread_id = str(snapshot.get("thread_id") or "").strip()
                if not thread_id or thread_id in seen_thread_ids:
                    continue
                seen_thread_ids.add(thread_id)
                row = _snapshot_to_thread_row(
                    snapshot,
                    account_id=account_id,
                    account_alias=account_alias,
                )
                if row:
                    collected.append(row)
                if len(collected) >= target_total:
                    break
            if len(collected) >= target_total:
                break
            cursor, has_older = _extract_inbox_cursor(payload)
            if not cursor or not has_older:
                break
        return collected
    finally:
        client.close()


def read_thread_from_storage(
    account: dict[str, Any],
    *,
    thread_id: str,
    thread_href: str = "",
    message_limit: int = 80,
    profiles_root: Path | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    client = _AccountEndpointClient(account, profiles_root=profiles_root)
    try:
        account_id = client.account_id
        clean_thread_id = str(thread_id or "").strip()
        if not account_id or not clean_thread_id:
            return {"messages": [], "participants": [], "seen_text": "", "seen_at": None}
        safe_limit = max(1, min(80, int(message_limit or 80)))
        self_user_id = str(getattr(client, "self_user_id", "") or account_id).strip()
        request_nonce = int(time.time() * 1000.0)
        urls: list[str] = []
        for params in (
            {"limit": str(safe_limit)},
            {
                "limit": str(safe_limit),
                "visual_message_return_type": "unseen",
                "persistentBadging": "true",
            },
        ):
            query = urlencode(params, doseq=True)
            for base in (
                f"/api/v1/direct_v2/threads/{clean_thread_id}/",
                f"/api/v1/direct_v2/threads/{clean_thread_id}",
            ):
                candidate = _append_cache_bust_query(f"{base}?{query}" if query else base, nonce=request_nonce)
                absolute_url = _absolute_instagram_url(candidate)
                if absolute_url and absolute_url not in urls:
                    urls.append(absolute_url)
        payload = client.fetch_json_candidates(
            urls,
            referer=str(thread_href or THREAD_URL_TEMPLATE.format(thread_id=clean_thread_id)).strip()
            or f"{_INSTAGRAM_BASE_URL}/direct/inbox/",
            timeout_seconds=timeout_seconds,
        )
        return {
            "messages": _payload_to_messages(payload, self_user_id=self_user_id),
            "participants": _extract_participants(payload),
            "seen_text": "",
            "seen_at": None,
        }
    finally:
        client.close()


def _extract_participants(payload: dict[str, Any]) -> list[str]:
    thread_payload = payload.get("thread") if isinstance(payload, dict) else None
    candidate_users = []
    if isinstance(thread_payload, dict):
        candidate_users.extend(list(thread_payload.get("users") or []))
        inviter = thread_payload.get("inviter")
        if isinstance(inviter, dict):
            candidate_users.append(inviter)
    if isinstance(payload, dict):
        candidate_users.extend(list(payload.get("users") or []))
    participants: list[str] = []
    seen: set[str] = set()
    for raw in candidate_users:
        if not isinstance(raw, dict):
            continue
        username = str(raw.get("username") or "").strip().lstrip("@")
        if not username:
            username = str(((raw.get("user") or {}) if isinstance(raw.get("user"), dict) else {}).get("username") or "").strip().lstrip("@")
        if not username:
            continue
        key = username.lower()
        if key in seen:
            continue
        seen.add(key)
        participants.append(username)
    return participants


def _load_storage_state_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise InboxEndpointError("login_required", f"storage_state_invalid:{path.name}") from exc
    if not isinstance(payload, dict):
        raise InboxEndpointError("login_required", f"storage_state_invalid:{path.name}")
    return payload


def _requests_proxy_map(payload: dict[str, str] | None) -> dict[str, str]:
    if not payload:
        return {}
    server = str(payload.get("server") or "").strip()
    if not server:
        return {}
    parts = urlsplit(server)
    netloc = parts.netloc
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    if username:
        auth = quote(username, safe="")
        if password:
            auth = f"{auth}:{quote(password, safe='')}"
        netloc = f"{auth}@{netloc}"
    proxy_url = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return {"http": proxy_url, "https": proxy_url}


def _absolute_instagram_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if not text.startswith("/"):
        text = f"/{text}"
    return f"{_INSTAGRAM_BASE_URL}{text}"


def _response_error_kind(status_code: int, response_url: str, response_text: str) -> str | None:
    combined = f"{response_url}\n{response_text}".lower()
    if status_code == 429 or "too many requests" in combined or "please wait a few minutes" in combined:
        return "rate_limit"
    if "/challenge/" in combined or "checkpoint_required" in combined or "challenge_required" in combined:
        return "checkpoint"
    if _looks_like_auth_surface(response_url, response_text):
        return "login_required"
    if "/accounts/suspended/" in combined or "your account has been suspended" in combined or "cuenta suspendida" in combined:
        return "suspended"
    if (
        "/accounts/disabled/" in combined
        or "disabled your account" in combined
        or "your account has been disabled" in combined
        or "temporarily blocked" in combined
        or "bloqueada temporalmente" in combined
    ):
        return "banned"
    if status_code >= 400:
        return "http_error"
    return None


def _response_error_detail(kind: str, status_code: int, response_text: str) -> str:
    clean_kind = str(kind or "").strip().lower() or "unknown"
    status = max(0, int(status_code or 0))
    if clean_kind == "http_error":
        return f"http_{status}" if status else "http_error"
    if clean_kind == "rate_limit":
        return f"too_many_requests:{status}" if status else "too_many_requests"
    snippet = str(response_text or "").strip().replace("\n", " ").replace("\r", " ")
    snippet = " ".join(snippet.split())[:160]
    return snippet or clean_kind


def _looks_like_auth_surface(response_url: str, response_text: str) -> bool:
    combined = f"{response_url}\n{response_text}".lower()
    return any(
        token in combined
        for token in (
            "/accounts/login",
            "login_required",
            "www.instagram.com/accounts/login",
            "session has expired",
            "please log in",
        )
    )


class ExceptionSuppressor:
    def __enter__(self) -> "ExceptionSuppressor":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return True


__all__ = [
    "fetch_account_threads_page_from_storage",
    "InboxEndpointError",
    "read_thread_from_storage",
    "sync_account_threads_from_storage",
]
