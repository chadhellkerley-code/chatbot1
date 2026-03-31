from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional

try:
    from src.instagram_adapter import LOGIN_ERROR_VISIBLE_SELECTORS, USERNAME_INPUT_SELECTORS
except Exception:  # pragma: no cover
    LOGIN_ERROR_VISIBLE_SELECTORS = ("#slfErrorAlert", "[role='alert']")
    USERNAME_INPUT_SELECTORS = ("input[name='username']",)

logger = logging.getLogger(__name__)


class InstagramPageState(str, Enum):
    LOGIN_PAGE = "LOGIN_PAGE"
    SESSION_VALID = "SESSION_VALID"
    FEED = "FEED"
    INBOX = "INBOX"
    SESSION_EXPIRED = "SESSION_EXPIRED"

    PASSWORD_INCORRECT = "PASSWORD_INCORRECT"
    USERNAME_INCORRECT = "USERNAME_INCORRECT"

    CHALLENGE = "CHALLENGE"
    CHECKPOINT = "CHECKPOINT"
    TWO_FACTOR = "TWO_FACTOR"
    CAPTCHA = "CAPTCHA"

    ACCOUNT_DISABLED = "ACCOUNT_DISABLED"
    ACCOUNT_LOCKED = "ACCOUNT_LOCKED"

    RATE_LIMITED = "RATE_LIMITED"

    POST_LOGIN_INTERSTITIAL = "POST_LOGIN_INTERSTITIAL"
    CONSENT_REQUIRED = "CONSENT_REQUIRED"

    UNKNOWN = "UNKNOWN"


CAPTCHA_IFRAME_SELECTOR = "iframe[src*='captcha'], iframe[src*='recaptcha'], iframe[src*='hcaptcha']"
LOGIN_USERNAME_SELECTOR = USERNAME_INPUT_SELECTORS[0]
LOGGED_IN_NAV_SELECTOR = "nav[role='navigation']"
INBOX_LINK_SELECTOR = "a[href='/direct/inbox/']"
HOME_ICON_SELECTOR = "svg[aria-label='Home'], svg[aria-label='Inicio']"
LOGIN_ERROR_SELECTOR = f"{LOGIN_ERROR_VISIBLE_SELECTORS[0]}, {LOGIN_ERROR_VISIBLE_SELECTORS[-1]}"
CONSENT_SELECTOR = "button:has-text('Accept'), button:has-text('Allow')"

RATE_LIMIT_PHRASES = (
    "Please wait a few minutes",
    "Try Again Later",
    "We restrict certain activity",
)


async def _safe_query_selector(page: Any, selector: str) -> Optional[Any]:
    try:
        return await page.query_selector(selector)
    except Exception:
        return None


async def _safe_inner_text(page: Any, selector: str) -> str:
    try:
        inner_text = await page.inner_text(selector)
        return str(inner_text or "")
    except Exception:
        return ""


def _debug_state(state: InstagramPageState, url: str) -> None:
    try:
        logger.debug("instagram_state_detected", state=state, url=url)
    except TypeError:
        logger.debug("instagram_state_detected state=%s url=%s", state, url)


async def detect_instagram_page_state(page: Any) -> InstagramPageState:
    url = str(getattr(page, "url", "") or "")
    normalized_url = url.lower()

    # 1) ERRORES DE NAVEGADOR
    if url.startswith("chrome-error://"):
        state = InstagramPageState.UNKNOWN
        _debug_state(state, url)
        return state

    # 2) URL DE CHALLENGE
    if "/challenge" in normalized_url:
        state = InstagramPageState.CHALLENGE
        _debug_state(state, url)
        return state
    if "/checkpoint" in normalized_url:
        state = InstagramPageState.CHECKPOINT
        _debug_state(state, url)
        return state
    if "two_factor" in normalized_url:
        state = InstagramPageState.TWO_FACTOR
        _debug_state(state, url)
        return state

    # 3) CAPTCHA
    if await _safe_query_selector(page, CAPTCHA_IFRAME_SELECTOR):
        state = InstagramPageState.CAPTCHA
        _debug_state(state, url)
        return state

    # 4) LOGIN PAGE
    if await _safe_query_selector(page, LOGIN_USERNAME_SELECTOR):
        state = InstagramPageState.LOGIN_PAGE
        _debug_state(state, url)
        return state

    # 5) LOGGED-IN UI
    if await _safe_query_selector(page, LOGGED_IN_NAV_SELECTOR):
        state = InstagramPageState.SESSION_VALID
        _debug_state(state, url)
        return state
    if await _safe_query_selector(page, INBOX_LINK_SELECTOR):
        state = InstagramPageState.INBOX
        _debug_state(state, url)
        return state
    if await _safe_query_selector(page, HOME_ICON_SELECTOR):
        state = InstagramPageState.FEED
        _debug_state(state, url)
        return state

    # 6) LOGIN ERRORS
    if await _safe_query_selector(page, LOGIN_ERROR_SELECTOR):
        state = InstagramPageState.PASSWORD_INCORRECT
        _debug_state(state, url)
        return state

    # 7) ACCOUNT DISABLED
    if "suspended" in normalized_url or "disabled" in normalized_url:
        state = InstagramPageState.ACCOUNT_DISABLED
        _debug_state(state, url)
        return state

    # 8) RATE LIMIT
    visible_text = await _safe_inner_text(page, "body")
    visible_text_lower = visible_text.lower()
    if any(phrase.lower() in visible_text_lower for phrase in RATE_LIMIT_PHRASES):
        state = InstagramPageState.RATE_LIMITED
        _debug_state(state, url)
        return state

    # 9) POST LOGIN INTERSTITIAL
    if "/accounts/onetap/" in normalized_url:
        state = InstagramPageState.POST_LOGIN_INTERSTITIAL
        _debug_state(state, url)
        return state

    # 10) CONSENT
    if await _safe_query_selector(page, CONSENT_SELECTOR):
        state = InstagramPageState.CONSENT_REQUIRED
        _debug_state(state, url)
        return state

    # 11) FALLBACK
    state = InstagramPageState.UNKNOWN
    _debug_state(state, url)
    return state

