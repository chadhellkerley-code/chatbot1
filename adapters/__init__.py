"""Instagram client adapters and factories."""

from .base import BaseInstagramClient, TwoFARequired, TwoFactorCodeRejected
from .instagram_stub import InstagramStubClient
from .instagram_playwright import InstagramPlaywrightClient
from .instagram_instagrapi import InstagramInstagrapiClient

__all__ = [
    "BaseInstagramClient",
    "TwoFARequired",
    "TwoFactorCodeRejected",
    "InstagramStubClient",
    "InstagramPlaywrightClient",
    "InstagramInstagrapiClient",
]
