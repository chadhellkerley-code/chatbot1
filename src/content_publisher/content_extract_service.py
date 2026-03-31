from __future__ import annotations

import contextlib
import json
import logging
import re
from html import unescape
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import requests

from core import accounts as accounts_module
from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from proxy_manager import record_proxy_failure, should_retry_proxy
from src.browser_profile_paths import browser_profile_dir, browser_storage_state_path
from src.playwright_service import BASE_PROFILES, PlaywrightService
from src.proxy_payload import proxy_from_account
from src.runtime.playwright_runtime import run_coroutine_sync

from .content_library_service import ContentLibraryService, ContentPublisherError
from .session_client import AuthenticatedSession, create_authenticated_client, pause_between_operations


logger = logging.getLogger(__name__)

_RESERVED_PROFILE_SEGMENTS = {
    "",
    "accounts",
    "explore",
    "p",
    "reel",
    "reels",
    "stories",
    "tv",
}
_DEFAULT_TIMEOUT_SECONDS = 25
_INSTAGRAM_BASE = "https://www.instagram.com"
_WEB_PROFILE_INFO_ENDPOINT = f"{_INSTAGRAM_BASE}/api/v1/users/web_profile_info/"
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_PROFILE_POST_LINK_RE = re.compile(r"""href=["'](?P<href>/(?:p|reel|reels)/[^"'?#]+/?(?:\?[^"']*)?)["']""", re.IGNORECASE)
_META_TAG_RE = re.compile(
    r"""<meta[^>]+(?:property|name)=["'](?P<name>[^"']+)["'][^>]+content=["'](?P<content>[^"']*)["'][^>]*>""",
    re.IGNORECASE,
)
_SCRIPT_TAG_RE = re.compile(r"<script[^>]*>(?P<body>.*?)</script>", re.IGNORECASE | re.DOTALL)


def _normalize_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def _profile_username_from_url(raw_url: str) -> str:
    candidate = str(raw_url or "").strip()
    if not candidate:
        raise ContentPublisherError("Hay una URL de perfil vacia en la lista.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if "instagram.com" not in str(parsed.netloc or "").lower():
        raise ContentPublisherError(f"La URL no corresponde a Instagram: {raw_url}")
    path_parts = [segment for segment in (parsed.path or "").split("/") if segment]
    username = _normalize_username(path_parts[0] if path_parts else "")
    if username.lower() in _RESERVED_PROFILE_SEGMENTS:
        raise ContentPublisherError(f"La URL no apunta a un perfil valido: {raw_url}")
    return username


class ContentExtractService:
    def __init__(
        self,
        *,
        root_dir: str | Path | None = None,
        library_service: ContentLibraryService | None = None,
    ) -> None:
        self.library = library_service or ContentLibraryService(root_dir=root_dir)

    def _resolve_accounts(self, account_ids: list[str], *, alias: str = "") -> list[dict[str, Any]]:
        ordered_ids = [_normalize_username(item) for item in account_ids if _normalize_username(item)]
        if not ordered_ids:
            raise ContentPublisherError("Selecciona al menos una cuenta para extraer contenido.")
        clean_alias = str(alias or "").strip().lower()
        by_username: dict[str, dict[str, Any]] = {}
        for row in accounts_module.list_all():
            if not isinstance(row, dict):
                continue
            username = _normalize_username(row.get("username"))
            if not username:
                continue
            row_alias = str(row.get("alias") or "default").strip().lower()
            if clean_alias and row_alias != clean_alias:
                continue
            by_username[username.lower()] = dict(row)
        resolved: list[dict[str, Any]] = []
        missing: list[str] = []
        for username in ordered_ids:
            account = by_username.get(username.lower())
            if account is None:
                missing.append(username)
                continue
            resolved.append(account)
        if missing:
            raise ContentPublisherError(f"No se encontraron las cuentas seleccionadas: {', '.join(missing)}")
        return resolved

    def _create_authenticated_client(self, account: dict[str, Any]) -> AuthenticatedSession:
        return create_authenticated_client(account, reason="content-extract")

    def _close_authenticated_client(self, account: dict[str, Any], client: Any) -> None:
        username = _normalize_username(account.get("username"))
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                logger.debug("No se pudo cerrar la sesion autenticada de @%s tras extraer contenido.", username)

    def _client_cookie_map(self, client: Any) -> dict[str, str]:
        cookie_map = getattr(client, "cookie_map", None)
        if isinstance(cookie_map, dict):
            return {str(key): str(value) for key, value in cookie_map.items() if str(key).strip() and str(value).strip()}
        get_settings = getattr(client, "get_settings", None)
        if not callable(get_settings):
            return {}
        try:
            settings = get_settings()
        except Exception:
            return {}
        if not isinstance(settings, dict):
            return {}
        cookies = settings.get("cookies")
        if not isinstance(cookies, dict):
            return {}
        return {
            str(key).strip(): str(value).strip()
            for key, value in cookies.items()
            if str(key).strip() and str(value).strip()
        }

    def _session_from_client(self, client: Any) -> tuple[requests.Session, bool]:
        session = getattr(client, "session", None)
        if session is not None and hasattr(session, "get") and hasattr(session, "headers") and hasattr(session, "cookies"):
            return session, False
        transient = requests.Session()
        transient.trust_env = False
        transient.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": _DEFAULT_USER_AGENT,
                "X-CSRFToken": str(self._client_cookie_map(client).get("csrftoken") or "").strip(),
                "X-Requested-With": "XMLHttpRequest",
            }
        )
        for name, value in self._client_cookie_map(client).items():
            cookie_store = getattr(transient, "cookies", None)
            setter = getattr(cookie_store, "set", None)
            if callable(setter):
                setter(name, value, domain=".instagram.com", path="/")
            elif isinstance(cookie_store, dict):
                cookie_store[name] = value
        return transient, True

    def _profile_info_headers(self, profile_username: str, cookie_map: dict[str, str]) -> dict[str, str]:
        csrf_token = str(cookie_map.get("csrftoken") or "").strip()
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"{_INSTAGRAM_BASE}/{profile_username}/",
            "User-Agent": _DEFAULT_USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
            "X-IG-App-ID": "936619743392459",
        }
        if csrf_token:
            headers["X-CSRFToken"] = csrf_token
        return headers

    def _validate_profile_info_response(self, response: requests.Response, profile_username: str) -> dict[str, Any]:
        status = int(getattr(response, "status_code", 0) or 0)
        content_type = str(response.headers.get("content-type") or "").lower()
        body = str(getattr(response, "text", "") or "")
        snippet = body.strip()[:220]

        if status == 429:
            raise ContentPublisherError(
                f"Instagram rate limited extraction for @{profile_username} (HTTP 429). Try again later."
            )
        if status in {401, 403}:
            raise ContentPublisherError(
                f"Session rejected by Instagram for @{profile_username} (HTTP {status}). Re-login the account."
            )
        if status >= 400:
            raise ContentPublisherError(
                f"Instagram rejected extraction for @{profile_username} (HTTP {status}). {snippet}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ContentPublisherError(
                f"Instagram returned an invalid response for @{profile_username} ({content_type})."
            ) from exc
        if not isinstance(payload, dict):
            raise ContentPublisherError(f"Instagram returned an empty payload for @{profile_username}.")
        return payload

    def _browser_session_paths(self, account: dict[str, Any]) -> tuple[str, Path, Path]:
        username = _normalize_username(account.get("username"))
        if not username:
            raise ContentPublisherError("La cuenta seleccionada no tiene username valido.")
        profile_dir = browser_profile_dir(username, profiles_root=BASE_PROFILES)
        storage_state = browser_storage_state_path(username, profiles_root=BASE_PROFILES)
        if not storage_state.exists():
            raise ContentPublisherError(
                f"La cuenta @{username} no tiene storage_state para extraer contenido con browser."
            )
        return username, profile_dir, storage_state

    @staticmethod
    def _dedupe_strings(values: list[str]) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in values:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered

    @staticmethod
    def _iter_mapping_nodes(payload: Any) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        stack: list[Any] = [payload]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                normalized = {str(key): value for key, value in current.items()}
                nodes.append(normalized)
                stack.extend(normalized.values())
            elif isinstance(current, list):
                stack.extend(current)
        return nodes

    def _script_payloads_from_html(self, html_text: str) -> list[Any]:
        payloads: list[Any] = []
        candidates: list[str] = []
        for match in _SCRIPT_TAG_RE.finditer(str(html_text or "")):
            body = unescape(str(match.group("body") or "").strip())
            if body:
                candidates.append(body)
        for pattern in (
            r"window\._sharedData\s*=\s*(\{.*?\})\s*;",
            r"__additionalDataLoaded\([^,]+,\s*(\{.*?\})\s*\)\s*;",
        ):
            for match in re.finditer(pattern, str(html_text or ""), re.DOTALL):
                body = unescape(str(match.group(1) or "").strip())
                if body:
                    candidates.append(body)
        for candidate in candidates:
            raw = str(candidate or "").strip()
            if not raw:
                continue
            if not raw.startswith(("{", "[")):
                continue
            try:
                payloads.append(json.loads(raw))
            except Exception:
                continue
        return payloads

    def _post_url_from_href(self, href: str) -> str:
        candidate = unescape(str(href or "").strip())
        if not candidate:
            return ""
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        elif candidate.startswith("/"):
            candidate = f"{_INSTAGRAM_BASE}{candidate}"
        if "instagram.com" not in str(urlparse(candidate).netloc or "").lower():
            return ""
        normalized = candidate.split("?", 1)[0].split("#", 1)[0]
        return normalized.rstrip("/") + "/"

    def _profile_post_urls_from_html(self, html_text: str, *, desired_count: int) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for match in _PROFILE_POST_LINK_RE.finditer(str(html_text or "")):
            candidate = self._post_url_from_href(match.group("href"))
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            urls.append(candidate)
            if len(urls) >= max(1, int(desired_count or 1)):
                break
        return urls

    def _profile_post_count_from_html(self, html_text: str, *, fallback_count: int) -> int:
        for payload in self._script_payloads_from_html(html_text):
            for node in self._iter_mapping_nodes(payload):
                timeline = node.get("edge_owner_to_timeline_media")
                if isinstance(timeline, dict):
                    try:
                        count = int(timeline.get("count") or 0)
                    except Exception:
                        count = 0
                    if count > 0:
                        return count
                for key in ("post_count", "posts_count", "media_count"):
                    try:
                        count = int(node.get(key) or 0)
                    except Exception:
                        count = 0
                    if count > 0:
                        return count
        return max(0, int(fallback_count or 0))

    def _extract_meta_content(self, html_text: str, *names: str) -> str:
        accepted = {str(item or "").strip().lower() for item in names if str(item or "").strip()}
        if not accepted:
            return ""
        for match in _META_TAG_RE.finditer(str(html_text or "")):
            name = str(match.group("name") or "").strip().lower()
            if name not in accepted:
                continue
            return unescape(str(match.group("content") or "").strip())
        return ""

    def _caption_from_browser_node(self, node: dict[str, Any]) -> str:
        caption_text = self._caption_from_node(node)
        if caption_text:
            return caption_text
        caption_value = node.get("caption")
        if isinstance(caption_value, str) and caption_value.strip():
            return caption_value.strip()
        if isinstance(caption_value, dict):
            nested = str(caption_value.get("text") or caption_value.get("content") or "").strip()
            if nested:
                return nested
        for key in ("articleBody", "accessibility_caption", "title"):
            text = str(node.get(key) or "").strip()
            if text:
                return text
        return ""

    def _coerce_url_value(self, value: Any) -> str:
        candidate = unescape(str(value or "").strip())
        if not candidate:
            return ""
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        elif candidate.startswith("/"):
            candidate = f"{_INSTAGRAM_BASE}{candidate}"
        parsed = urlparse(candidate)
        if str(parsed.scheme or "").lower() not in {"http", "https"}:
            return ""
        return candidate

    def _video_urls_from_node(self, node: dict[str, Any]) -> list[str]:
        urls: list[str] = []
        direct = self._coerce_url_value(node.get("video_url"))
        if direct:
            urls.append(direct)
        for key in ("video_versions", "video_resources"):
            raw = node.get(key)
            if not isinstance(raw, list):
                continue
            for item in raw:
                if not isinstance(item, dict):
                    continue
                candidate = self._coerce_url_value(item.get("url") or item.get("src"))
                if candidate:
                    urls.append(candidate)
        return self._dedupe_strings(urls)

    def _image_urls_from_node(self, node: dict[str, Any]) -> list[str]:
        urls: list[str] = []
        for key in ("display_url", "thumbnail_src", "image_url", "src"):
            candidate = self._coerce_url_value(node.get(key))
            if candidate:
                urls.append(candidate)
        for key in ("display_resources", "thumbnail_resources"):
            raw = node.get(key)
            if not isinstance(raw, list):
                continue
            for item in raw:
                if not isinstance(item, dict):
                    continue
                candidate = self._coerce_url_value(item.get("src") or item.get("url"))
                if candidate:
                    urls.append(candidate)
        image_versions = node.get("image_versions2")
        if isinstance(image_versions, dict):
            for item in image_versions.get("candidates") or []:
                if not isinstance(item, dict):
                    continue
                candidate = self._coerce_url_value(item.get("url"))
                if candidate:
                    urls.append(candidate)
        for item in node.get("image") or [] if isinstance(node.get("image"), list) else []:
            candidate = self._coerce_url_value(item)
            if candidate:
                urls.append(candidate)
        if isinstance(node.get("image"), str):
            candidate = self._coerce_url_value(node.get("image"))
            if candidate:
                urls.append(candidate)
        best = self._best_image_url(node)
        if best:
            urls.append(best)
        return self._dedupe_strings(urls)

    def _sidecar_children_from_node(self, node: dict[str, Any]) -> list[dict[str, Any]]:
        children: list[dict[str, Any]] = []
        sidecar = node.get("edge_sidecar_to_children")
        child_edges = sidecar.get("edges") if isinstance(sidecar, dict) else []
        if isinstance(child_edges, list):
            for edge in child_edges:
                child = edge.get("node") if isinstance(edge, dict) else None
                if isinstance(child, dict):
                    children.append(child)
        carousel_media = node.get("carousel_media")
        if isinstance(carousel_media, list):
            for child in carousel_media:
                if isinstance(child, dict):
                    children.append(child)
        return children

    @staticmethod
    def _candidate_code_from_url(post_url: str) -> str:
        parts = [segment for segment in str(urlparse(post_url).path or "").split("/") if segment]
        if len(parts) < 2:
            return ""
        if parts[0].lower() not in {"p", "reel", "reels"}:
            return ""
        return str(parts[1] or "").strip()

    def _browser_media_node_score(self, node: dict[str, Any], *, target_code: str) -> int:
        score = 0
        node_code = str(node.get("shortcode") or node.get("code") or "").strip()
        if target_code and node_code and node_code == target_code:
            score += 5
        if self._sidecar_children_from_node(node):
            score += 4
        if self._video_urls_from_node(node):
            score += 3
        if self._image_urls_from_node(node):
            score += 2
        if self._caption_from_browser_node(node):
            score += 1
        return score

    def _parse_browser_media_node(
        self,
        node: dict[str, Any],
        *,
        post_url: str,
    ) -> tuple[SimpleNamespace, str] | None:
        code = str(node.get("shortcode") or node.get("code") or self._candidate_code_from_url(post_url)).strip()
        pk = str(node.get("id") or node.get("pk") or code).strip()
        caption_text = self._caption_from_browser_node(node)
        sidecar_urls: list[str] = []
        for child in self._sidecar_children_from_node(node):
            child_urls = self._video_urls_from_node(child) or self._image_urls_from_node(child)
            if child_urls:
                sidecar_urls.append(child_urls[0])
        if sidecar_urls:
            return (
                SimpleNamespace(
                    pk=pk,
                    code=code,
                    media_type=8,
                    caption_text=caption_text,
                    media_urls=self._dedupe_strings(sidecar_urls),
                ),
                "carousel",
            )
        video_urls = self._video_urls_from_node(node)
        if video_urls:
            return (
                SimpleNamespace(
                    pk=pk,
                    code=code,
                    media_type=2,
                    caption_text=caption_text,
                    media_urls=video_urls[:1],
                ),
                "video",
            )
        image_urls = self._image_urls_from_node(node)
        if image_urls:
            return (
                SimpleNamespace(
                    pk=pk,
                    code=code,
                    media_type=1,
                    caption_text=caption_text,
                    media_urls=image_urls[:1],
                ),
                "image",
            )
        return None

    def _browser_media_from_html(
        self,
        html_text: str,
        *,
        post_url: str,
    ) -> tuple[SimpleNamespace, str] | None:
        target_code = self._candidate_code_from_url(post_url)
        best_match: tuple[int, tuple[SimpleNamespace, str]] | None = None
        for payload in self._script_payloads_from_html(html_text):
            for node in self._iter_mapping_nodes(payload):
                parsed = self._parse_browser_media_node(node, post_url=post_url)
                if parsed is None:
                    continue
                score = self._browser_media_node_score(node, target_code=target_code)
                if best_match is None or score > best_match[0]:
                    best_match = (score, parsed)
        if best_match is not None:
            return best_match[1]

        caption_text = self._extract_meta_content(html_text, "og:description", "twitter:description")
        video_url = self._extract_meta_content(html_text, "og:video", "og:video:url")
        image_url = self._extract_meta_content(html_text, "og:image", "twitter:image")
        media_urls = self._dedupe_strings(
            [self._coerce_url_value(video_url), self._coerce_url_value(image_url)]
        )
        if not media_urls:
            return None
        media_type = "video" if self._coerce_url_value(video_url) else "image"
        return (
            SimpleNamespace(
                pk=target_code or post_url,
                code=target_code or post_url,
                media_type=2 if media_type == "video" else 1,
                caption_text=caption_text,
                media_urls=media_urls[:1],
            ),
            media_type,
        )

    async def _fetch_supported_media_browser_async(
        self,
        account: dict[str, Any],
        profile_username: str,
        desired_count: int,
    ) -> dict[str, Any]:
        account_username, profile_dir, storage_state = self._browser_session_paths(account)
        proxy_payload = proxy_from_account(account)
        service = PlaywrightService(headless=True, base_profiles=Path(BASE_PROFILES))
        ctx = None
        try:
            ctx = await service.new_context_for_account(
                profile_dir=profile_dir,
                storage_state=storage_state,
                proxy=proxy_payload,
            )
            page = ctx.pages[0] if getattr(ctx, "pages", None) else await ctx.new_page()
            profile_url = f"{_INSTAGRAM_BASE}/{profile_username}/"
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
            with contextlib.suppress(Exception):
                await page.wait_for_load_state("networkidle", timeout=12_000)
            profile_html = await page.content()
            target_total = max(1, int(desired_count or 1))
            post_urls = self._profile_post_urls_from_html(profile_html, desired_count=target_total)
            post_count = self._profile_post_count_from_html(profile_html, fallback_count=len(post_urls))
            if not post_urls:
                raise ContentPublisherError(
                    f"Instagram no expuso publicaciones navegables para @{profile_username} en browser."
                )
            parsed_media: list[tuple[SimpleNamespace, str]] = []
            for post_url in post_urls[:target_total]:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=45_000)
                with contextlib.suppress(Exception):
                    await page.wait_for_load_state("networkidle", timeout=12_000)
                post_html = await page.content()
                parsed = self._browser_media_from_html(post_html, post_url=post_url)
                if parsed is None:
                    continue
                parsed_media.append(parsed)
            if not parsed_media:
                raise ContentPublisherError(
                    f"Instagram no expuso media compatible para @{profile_username} en browser."
                )
            return {
                "account_id": account_username,
                "media": parsed_media,
                "post_count": post_count,
            }
        finally:
            if ctx is not None:
                with contextlib.suppress(Exception):
                    await ctx.close()
            with contextlib.suppress(Exception):
                await service.close()

    def _fetch_supported_media_browser(
        self,
        account: dict[str, Any],
        profile_username: str,
        desired_count: int,
    ) -> dict[str, Any]:
        return run_coroutine_sync(
            self._fetch_supported_media_browser_async(
                account,
                profile_username,
                desired_count=max(1, int(desired_count or 1)),
            ),
            timeout=max(60.0, float(_DEFAULT_TIMEOUT_SECONDS) * 6.0),
            ignore_stop=True,
        )

    def _timeline_edges(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = (
            payload.get("data", {}),
            payload.get("graphql", {}),
        )
        for root in candidates:
            if not isinstance(root, dict):
                continue
            user = root.get("user")
            if not isinstance(user, dict):
                continue
            timeline = user.get("edge_owner_to_timeline_media")
            if not isinstance(timeline, dict):
                continue
            edges = timeline.get("edges")
            if isinstance(edges, list):
                return [edge for edge in edges if isinstance(edge, dict)]
        return []

    def _caption_from_node(self, node: dict[str, Any]) -> str:
        caption_root = node.get("edge_media_to_caption")
        if not isinstance(caption_root, dict):
            return ""
        edges = caption_root.get("edges")
        if not isinstance(edges, list):
            return ""
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            payload = edge.get("node")
            if not isinstance(payload, dict):
                continue
            text = str(payload.get("text") or "").strip()
            if text:
                return text
        return ""

    def _best_image_url(self, node: dict[str, Any]) -> str:
        display_url = str(node.get("display_url") or "").strip()
        if display_url:
            return display_url
        resources = node.get("display_resources")
        if isinstance(resources, list):
            for item in reversed(resources):
                if not isinstance(item, dict):
                    continue
                candidate = str(item.get("src") or "").strip()
                if candidate:
                    return candidate
        return ""

    def _parse_supported_media_from_payload(
        self,
        payload: dict[str, Any],
        *,
        desired_count: int,
    ) -> list[tuple[SimpleNamespace, str]]:
        parsed: list[tuple[SimpleNamespace, str]] = []
        for edge in self._timeline_edges(payload):
            node = edge.get("node")
            if not isinstance(node, dict):
                continue
            typename = str(node.get("__typename") or "").strip()
            caption_text = self._caption_from_node(node)
            code = str(node.get("shortcode") or node.get("code") or "").strip()
            pk = str(node.get("id") or node.get("pk") or "").strip()
            if typename == "GraphImage":
                media_url = self._best_image_url(node)
                if not media_url:
                    continue
                parsed.append(
                    (
                        SimpleNamespace(
                            pk=pk,
                            code=code,
                            media_type=1,
                            caption_text=caption_text,
                            media_urls=[media_url],
                        ),
                        "image",
                    )
                )
            elif typename == "GraphSidecar":
                child_urls: list[str] = []
                sidecar = node.get("edge_sidecar_to_children")
                child_edges = sidecar.get("edges") if isinstance(sidecar, dict) else []
                if isinstance(child_edges, list):
                    for child_edge in child_edges:
                        if not isinstance(child_edge, dict):
                            continue
                        child_node = child_edge.get("node")
                        if not isinstance(child_node, dict):
                            continue
                        child_url = self._best_image_url(child_node)
                        if child_url:
                            child_urls.append(child_url)
                if not child_urls:
                    fallback = self._best_image_url(node)
                    if fallback:
                        child_urls.append(fallback)
                if not child_urls:
                    continue
                parsed.append(
                    (
                        SimpleNamespace(
                            pk=pk,
                            code=code,
                            media_type=8,
                            caption_text=caption_text,
                            media_urls=child_urls,
                        ),
                        "carousel",
                    )
                )
            if len(parsed) >= max(1, int(desired_count or 1)):
                break
        return parsed

    def _fetch_supported_media(self, client: Any, profile_username: str, desired_count: int) -> list[tuple[SimpleNamespace, str]]:
        cookie_map = self._client_cookie_map(client)
        if not cookie_map:
            raise ContentPublisherError("No authenticated cookies are available for content extraction.")
        session, owns_session = self._session_from_client(client)
        try:
            response = session.get(
                _WEB_PROFILE_INFO_ENDPOINT,
                params={"username": profile_username},
                headers=self._profile_info_headers(profile_username, cookie_map),
                timeout=(
                    max(2.0, _DEFAULT_TIMEOUT_SECONDS * 0.5),
                    max(4.0, float(_DEFAULT_TIMEOUT_SECONDS)),
                ),
            )
            payload = self._validate_profile_info_response(response, profile_username)
            return self._parse_supported_media_from_payload(
                payload,
                desired_count=max(1, int(desired_count or 1)),
            )
        finally:
            if owns_session:
                session.close()

    def _download_to_path(self, session: requests.Session, media_url: str, target_path: Path) -> Path:
        response = session.get(
            media_url,
            headers={"Referer": _INSTAGRAM_BASE, "User-Agent": _DEFAULT_USER_AGENT},
            timeout=(5.0, max(8.0, float(_DEFAULT_TIMEOUT_SECONDS))),
            stream=True,
        )
        if int(getattr(response, "status_code", 0) or 0) >= 400:
            raise ContentPublisherError(
                f"Instagram rejected media download ({response.status_code}) for {target_path.name}."
            )
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    handle.write(chunk)
        return target_path

    def _download_media_entry(self, client: Any, profile_username: str, media: SimpleNamespace, media_type: str) -> dict[str, Any]:
        media_urls = [str(item).strip() for item in getattr(media, "media_urls", []) if str(item).strip()]
        if not media_urls:
            raise ContentPublisherError(f"Instagram did not expose media URLs for @{profile_username}.")
        entry_key = str(getattr(media, "code", "") or getattr(media, "pk", "") or profile_username).strip() or profile_username
        temp_dir = self.library.root_dir / "tmp" / "content_extract" / _normalize_username(profile_username) / entry_key
        session, owns_session = self._session_from_client(client)
        try:
            downloaded_files: list[Path] = []
            for index, media_url in enumerate(media_urls, start=1):
                suffix = Path(urlparse(media_url).path).suffix.lower() or ".jpg"
                if media_type == "carousel":
                    filename = f"slide_{index:02d}{suffix}"
                elif media_type == "video":
                    filename = f"video{suffix}"
                else:
                    filename = f"image{suffix}"
                downloaded_files.append(
                    self._download_to_path(session, media_url, temp_dir / filename)
                )
            return self.library.store_media_entry(
                source_profile=profile_username,
                media_type=media_type,
                media_files=downloaded_files,
                caption=str(getattr(media, "caption_text", "") or "").strip(),
                entry_key=entry_key,
            )
        finally:
            if owns_session:
                session.close()

    def extract(
        self,
        *,
        alias: str,
        account_ids: list[str],
        profile_urls: list[str],
        posts_per_profile: int = 3,
    ) -> dict[str, Any]:
        clean_alias = str(alias or "").strip()
        desired_count = max(1, int(posts_per_profile or 1))
        targets: list[str] = []
        seen_profiles: set[str] = set()
        for raw_url in profile_urls:
            username = _profile_username_from_url(raw_url)
            if username.lower() in seen_profiles:
                continue
            seen_profiles.add(username.lower())
            targets.append(username)
        if not targets:
            raise ContentPublisherError("Pega al menos una URL de perfil valida para extraer contenido.")

        selected_accounts = self._resolve_accounts(account_ids, alias=clean_alias)
        clients: dict[str, Any] = {}
        logs: list[str] = []
        stored_entries: list[dict[str, Any]] = []
        profiles: list[dict[str, Any]] = []

        proxy_preflight = preflight_accounts_for_proxy_runtime(selected_accounts)
        blocked_accounts = [dict(item) for item in (proxy_preflight.get("blocked_accounts") or []) if isinstance(item, dict)]
        for blocked in blocked_accounts:
            blocked_username = _normalize_username(blocked.get("username"))
            blocked_message = str(blocked.get("message") or "Proxy bloqueado.").strip() or "Proxy bloqueado."
            logs.append(f"Se omite @{blocked_username}: {blocked_message}")
        selected_accounts = [dict(item) for item in (proxy_preflight.get("ready_accounts") or []) if isinstance(item, dict)]
        if not selected_accounts:
            detail = "; ".join(
                str(item.get("message") or "Proxy bloqueado.").strip() or "Proxy bloqueado."
                for item in blocked_accounts[:3]
            )
            if detail:
                raise ContentPublisherError(
                    f"Ninguna de las cuentas seleccionadas quedo lista por proxy. {detail}"
                )
            raise ContentPublisherError("Ninguna de las cuentas seleccionadas quedo lista para extraer contenido.")

        try:
            for account in selected_accounts:
                username = _normalize_username(account.get("username"))
                try:
                    clients[username.lower()] = self._create_authenticated_client(account)
                    logs.append(f"Sesion lista para @{username}.")
                except Exception as exc:
                    logs.append(f"No se pudo preparar @{username}: {exc}")

            if not clients:
                raise ContentPublisherError(
                    "Ninguna de las cuentas seleccionadas quedo lista para extraer contenido."
                )

            for target_index, target_username in enumerate(targets):
                logs.append(f"Extrayendo publicaciones recientes de @{target_username}.")
                profile_result = {
                    "source_profile": target_username,
                    "status": "error",
                    "account_id": "",
                    "stored_count": 0,
                    "post_count": 0,
                    "extract_method": "",
                    "error": "",
                }
                for account_index, account in enumerate(selected_accounts):
                    account_username = _normalize_username(account.get("username"))
                    client = clients.get(account_username.lower())
                    if client is None:
                        continue
                    try:
                        post_count = 0
                        extract_method = "browser"
                        try:
                            browser_result = self._fetch_supported_media_browser(
                                account,
                                target_username,
                                desired_count=desired_count,
                            )
                            supported_media = list(browser_result.get("media") or [])
                            post_count = max(0, int(browser_result.get("post_count") or 0))
                            if not supported_media:
                                raise ContentPublisherError(
                                    f"Instagram no expuso media compatible para @{target_username} en browser."
                                )
                        except Exception as browser_exc:
                            logs.append(
                                f"Browser extract fallo para @{target_username} con @{account_username}: {browser_exc}"
                            )
                            supported_media = self._fetch_supported_media(
                                client,
                                target_username,
                                desired_count=desired_count,
                            )
                            extract_method = "endpoint"
                        profile_entries: list[dict[str, Any]] = []
                        for media, media_type in supported_media:
                            entry = self._download_media_entry(client, target_username, media, media_type)
                            profile_entries.append(entry)
                            logs.append(
                                f"Guardado @{target_username} / {entry.get('media_type')} en {entry.get('media_path')}."
                            )
                        logs.append(
                            f"Extraccion @{target_username} resuelta por {extract_method} con @{account_username}."
                        )
                        profile_result.update(
                            {
                                "status": "ok",
                                "account_id": account_username,
                                "stored_count": len(profile_entries),
                                "post_count": post_count,
                                "extract_method": extract_method,
                                "error": "",
                            }
                        )
                        stored_entries.extend(profile_entries)
                        if not profile_entries:
                            logs.append(
                                f"@{target_username} no tiene publicaciones de imagen, carrusel o video disponibles."
                            )
                        break
                    except Exception as exc:
                        profile_result["error"] = str(exc) or exc.__class__.__name__
                        logs.append(
                            f"Fallo extrayendo @{target_username} con @{account_username}: {profile_result['error']}"
                        )
                        if should_retry_proxy(exc):
                            record_proxy_failure(account_username, exc)
                        if account_index < len(selected_accounts) - 1:
                            pause_between_operations()
                profiles.append(profile_result)
                if target_index < len(targets) - 1:
                    pause_between_operations()
        finally:
            for account in selected_accounts:
                account_username = _normalize_username(account.get("username"))
                client = clients.get(account_username.lower())
                if client is None:
                    continue
                self._close_authenticated_client(account, client)

        successful_profiles = sum(1 for item in profiles if str(item.get("status") or "") == "ok")
        return {
            "alias": clean_alias,
            "profiles": profiles,
            "items": stored_entries,
            "stored_count": len(stored_entries),
            "profiles_processed": len(targets),
            "logs": logs,
            "summary": (
                f"Se guardaron {len(stored_entries)} publicaciones "
                f"de {successful_profiles}/{len(targets)} perfiles."
            ),
        }
