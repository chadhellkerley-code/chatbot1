from __future__ import annotations

import asyncio

from src.instagram.page_state_detector import InstagramPageState, detect_instagram_page_state


class PageStub:
    def __init__(self, *, url: str, selectors: dict[str, bool] | None = None, body_text: str = "") -> None:
        self.url = url
        self._selectors = dict(selectors or {})
        self._body_text = body_text

    async def query_selector(self, selector: str):
        if self._selectors.get(selector):
            return object()
        return None

    async def inner_text(self, selector: str) -> str:
        if selector != "body":
            return ""
        return self._body_text


def test_login_page_detected():
    page = PageStub(
        url="https://www.instagram.com/accounts/login/",
        selectors={"input[name='username']": True},
    )
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.LOGIN_PAGE


def test_challenge_detected():
    page = PageStub(url="https://www.instagram.com/challenge/123/")
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.CHALLENGE


def test_two_factor_detected():
    page = PageStub(url="https://www.instagram.com/accounts/two_factor/")
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.TWO_FACTOR


def test_feed_detected():
    page = PageStub(
        url="https://www.instagram.com/",
        selectors={"svg[aria-label='Home'], svg[aria-label='Inicio']": True},
    )
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.FEED


def test_inbox_detected():
    page = PageStub(
        url="https://www.instagram.com/direct/inbox/",
        selectors={"a[href='/direct/inbox/']": True},
    )
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.INBOX


def test_captcha_detected():
    page = PageStub(
        url="https://www.instagram.com/accounts/login/",
        selectors={"iframe[src*='captcha'], iframe[src*='recaptcha'], iframe[src*='hcaptcha']": True},
    )
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.CAPTCHA


def test_rate_limit_detected():
    page = PageStub(
        url="https://www.instagram.com/",
        body_text="Please wait a few minutes before you try again.",
    )
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.RATE_LIMITED


def test_unknown_fallback():
    page = PageStub(url="https://www.instagram.com/some/unknown/page")
    assert asyncio.run(detect_instagram_page_state(page)) == InstagramPageState.UNKNOWN

