from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import httpx


_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
)

_ACCEPT_LANGUAGES = (
    "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,es;q=0.7",
)


def random_user_agent() -> str:
    return random.choice(_USER_AGENTS)


def random_accept_language() -> str:
    return random.choice(_ACCEPT_LANGUAGES)


@dataclass
class HttpResponse:
    status_code: int
    text: str
    json: Any
    headers: Dict[str, str]


class HttpClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=10.0),
            headers={
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Connection": "keep-alive",
            },
        )
        self._proxy_sessions: Dict[str, httpx.AsyncClient] = {}

    async def aclose(self) -> None:
        for client in self._proxy_sessions.values():
            await client.aclose()
        self._proxy_sessions.clear()
        await self._client.aclose()

    async def get_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        proxy: str | None = None,
        timeout_seconds: float | None = None,
    ) -> HttpResponse:
        request_headers = dict(headers or {})
        kwargs: Dict[str, Any] = {"params": params, "headers": request_headers}
        if timeout_seconds is not None:
            kwargs["timeout"] = httpx.Timeout(connect=5.0, read=float(timeout_seconds), write=10.0, pool=10.0)

        response: httpx.Response
        if proxy is None:
            response = await self._client.get(url, **kwargs)
        else:
            if proxy not in self._proxy_sessions:
                self._proxy_sessions[proxy] = httpx.AsyncClient(
                    proxies=proxy,
                    follow_redirects=True,
                    timeout=(float(timeout_seconds) if timeout_seconds is not None else self._client.timeout),
                    headers=self._client.headers,
                )
            client = self._proxy_sessions[proxy]
            response = await client.get(url, **kwargs)

        text = response.text or ""
        parsed_json: Any = None
        try:
            parsed_json = response.json()
        except Exception:
            parsed_json = None
        return HttpResponse(
            status_code=int(response.status_code or 0),
            text=str(text),
            json=parsed_json,
            headers={str(k): str(v) for k, v in (response.headers or {}).items()},
        )
