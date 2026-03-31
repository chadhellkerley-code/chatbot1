import random
import string
import time
from getpass import getpass
from typing import Iterable, Optional, Sequence, Tuple

from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeout

from .config import DelayRange, get_settings


def sample_delay(range_config: DelayRange) -> float:
    low, high = range_config.as_tuple()
    return random.uniform(low, high)


def random_human_delay(range_config: DelayRange) -> float:
    value = sample_delay(range_config)
    time.sleep(max(value, 0))
    return value


def wait_first_selector(page: Page, selectors: Sequence[str], state: str = "visible", timeout: Optional[float] = None) -> Locator:
    if timeout is None:
        timeout = get_settings().wait_timeout * 1000
    last_error: Optional[Exception] = None
    for selector in selectors:
        try:
            locator = page.locator(selector)
            locator.wait_for(state=state, timeout=timeout)
            return locator
        except PlaywrightTimeout as error:
            last_error = error
            continue
    if last_error:
        raise last_error
    raise PlaywrightTimeout(f"Selectors not found: {selectors}")


def click_first(page: Page, selectors: Sequence[str]) -> None:
    locator = wait_first_selector(page, selectors)
    locator.scroll_into_view_if_needed()
    random_human_delay(get_settings().action_delay)
    locator.click()
    random_human_delay(get_settings().action_delay)


def fill_slow(locator: Locator, text: str) -> None:
    settings = get_settings()
    locator.click()
    locator.clear()
    for character in text:
        delay = random.uniform(*settings.keyboard_delay.as_tuple())
        locator.type(character, delay=int(delay * 1000))
    random_human_delay(settings.action_delay)


def ask_hidden(prompt: str) -> str:
    return getpass(prompt)


def sanitize_placeholder(value: str) -> str:
    mask_triggers = {"pass", "secret", "token"}
    lowered = value.lower()
    if any(trigger in lowered for trigger in mask_triggers):
        return "{{SECRET}}"
    if all(ch in string.digits for ch in value) and 4 <= len(value) <= 8:
        return "{{CODE}}"
    return value


def pairs_from_cli(arguments: Iterable[str]) -> Tuple[dict, list]:
    variables = {}
    leftovers = []
    for raw in arguments:
        if "=" in raw:
            key, value = raw.split("=", 1)
            variables[key] = value
        else:
            leftovers.append(raw)
    return variables, leftovers
