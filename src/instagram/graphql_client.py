from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import os
import random
import time
from typing import Any, Dict, Optional

from src.network.http_client import HttpClient
from src.network.adaptive_backoff import AdaptiveBackoff


_backoff = AdaptiveBackoff()


class RequestPacing:
    def __init__(self):
        self.last_request_ts = 0

        self.base_delay = 1.2
        self.max_delay = 3.5

    def compute_delay(self):
        jitter = random.uniform(0.3, 1.1)

        return min(
            self.max_delay,
            self.base_delay + jitter,
        )

    async def wait(self):
        now = time.time()

        delay = self.compute_delay()

        delta = now - self.last_request_ts

        if delta < delay:
            sleep_time = delay - delta

            print(
                f"[LEADS][HUMAN_DELAY] sleep={sleep_time:.2f}s"
            )

            await asyncio.sleep(sleep_time)

        self.last_request_ts = time.time()


_pacing = RequestPacing()


class InstagramPublicHttpError(RuntimeError):
    def __init__(self, status_code: int, *, reason: str = "", body: str = "") -> None:
        self.status_code = int(status_code or 0)
        self.reason = str(reason or "http_error")
        self.body = str(body or "")
        super().__init__(f"{self.reason}:{self.status_code}")


class InstagramPublicRateLimit(InstagramPublicHttpError):
    pass


@dataclass
class InstagramGraphQLClient:
    http: HttpClient
    ig_app_id: str = "936619743392459"
    graphql_doc_id: str = ""

    def _browser_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": '"Chromium";v="122", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        }

    async def _warmup_session(self, username: str, proxy_url: str | None) -> None:
        try:
            await self.http.get_json(
                "https://www.instagram.com/",
                headers=self._browser_headers(),
                proxy=proxy_url,
                timeout_seconds=10,
            )
            await self.http.get_json(
                f"https://www.instagram.com/{username}/",
                headers=self._browser_headers(),
                proxy=proxy_url,
                timeout_seconds=10,
            )
            print(f"[LEADS][ADAPTER_WARMUP_OK] username={username}")
        except Exception as exc:
            print(
                f"[LEADS][ADAPTER_WARMUP_FAIL] username={username} "
                f"error={type(exc).__name__}"
            )

    @staticmethod
    def _ensure_base_headers(headers: Dict[str, str], *, normalized: str) -> Dict[str, str]:
        base = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "X-IG-App-ID": "936619743392459",
            "X-ASBD-ID": "129477",
            "Sec-Ch-Ua": '"Chromium";v="122", "Google Chrome";v="122", "Not:A-Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Referer": f"https://www.instagram.com/{normalized}/",
        }
        for k, v in base.items():
            headers.setdefault(k, v)
        return headers

    async def fetch_profile_json(
        self,
        username: str,
        *,
        proxy_url: Optional[str] = None,
        timeout_seconds: float = 12.0,
    ) -> Dict[str, Any]:
        normalized = str(username or "").strip().lstrip("@")
        if not normalized:
            raise InstagramPublicHttpError(0, reason="username_vacio")

        doc_id = str(
            self.graphql_doc_id
            or os.getenv("IG_PUBLIC_PROFILE_DOC_ID")
            or ""
        ).strip()

        await self._warmup_session(normalized, proxy_url)

        if doc_id:
            payload = await self._fetch_profile_via_graphql(
                normalized,
                doc_id=doc_id,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
            )
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                _backoff.record_success()
                return payload

        url = "https://www.instagram.com/api/v1/users/web_profile_info/"
        headers = self._ensure_base_headers(
            {
                "X-IG-App-ID": str(self.ig_app_id).strip(),
            },
            normalized=normalized,
        )
        sleep_time = _backoff.compute_sleep()
        if sleep_time > 0:
            print(f"[LEADS][BACKOFF] sleeping={sleep_time:.2f}s consecutive_429={_backoff.state.consecutive_429}")
            await asyncio.sleep(sleep_time)
        await _pacing.wait()
        res = await self.http.get_json(
            url,
            params={"username": normalized},
            headers=headers,
            proxy=proxy_url,
            timeout_seconds=float(timeout_seconds),
        )
        if res.status_code == 429:
            _backoff.record_429()
            print(
                f"[LEADS][BACKOFF_TRIGGER] consecutive_429={_backoff.state.consecutive_429}"
            )
            raise InstagramPublicRateLimit(429, reason="http_429", body=res.text)
        if res.status_code != 200:
            raise InstagramPublicHttpError(res.status_code, reason=f"http_{res.status_code}", body=res.text)
        if not isinstance(res.json, dict):
            raise InstagramPublicHttpError(res.status_code, reason="invalid_json", body=res.text)
        _backoff.record_success()
        return res.json

    async def _fetch_profile_via_graphql(
        self,
        username: str,
        *,
        doc_id: str,
        proxy_url: Optional[str],
        timeout_seconds: float,
    ) -> Dict[str, Any]:
        url = "https://www.instagram.com/api/graphql"
        normalized = str(username or "").strip().lstrip("@")
        headers = self._ensure_base_headers(
            {
                "X-IG-App-ID": str(self.ig_app_id).strip(),
            },
            normalized=normalized,
        )
        variables = {"username": normalized}
        sleep_time = _backoff.compute_sleep()
        if sleep_time > 0:
            print(f"[LEADS][BACKOFF] sleeping={sleep_time:.2f}s consecutive_429={_backoff.state.consecutive_429}")
            await asyncio.sleep(sleep_time)
        await _pacing.wait()
        res = await self.http.get_json(
            url,
            params={"doc_id": str(doc_id).strip(), "variables": json.dumps(variables, separators=(",", ":"))},
            headers=headers,
            proxy=proxy_url,
            timeout_seconds=float(timeout_seconds),
        )
        if res.status_code == 429:
            _backoff.record_429()
            print(
                f"[LEADS][BACKOFF_TRIGGER] consecutive_429={_backoff.state.consecutive_429}"
            )
            raise InstagramPublicRateLimit(429, reason="http_429", body=res.text)
        if res.status_code != 200:
            raise InstagramPublicHttpError(res.status_code, reason=f"http_{res.status_code}", body=res.text)
        if not isinstance(res.json, dict):
            raise InstagramPublicHttpError(res.status_code, reason="invalid_json", body=res.text)
        return res.json
