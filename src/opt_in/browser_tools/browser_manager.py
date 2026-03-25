from __future__ import annotations

import contextlib
import time
from typing import Optional

from playwright.sync_api import Browser, BrowserContext, Page, Playwright

from . import audit
from .config import OptInSettings, get_settings
from .session_store import SessionStore
from .utils import random_human_delay
from src.playwright_service import resolve_playwright_executable
from src.runtime.playwright_runtime import launch_sync_browser, start_sync_playwright


class BrowserManager:
    def __init__(self, account_alias: Optional[str] = None, persist_session: bool = True) -> None:
        self.account_alias = account_alias
        self.persist_session = persist_session and bool(account_alias)
        self.settings: OptInSettings = get_settings()
        self.session_store = SessionStore()

        self._playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    def __enter__(self) -> "BrowserManager":
        self.launch()
        return self

    def __exit__(self, exc_type, exc, _tb) -> None:
        self.close(save_state=exc is None)

    def launch(self) -> None:
        if self._playwright:
            return

        self._playwright = start_sync_playwright()
        audit.log_event("browser.launch", account=self.account_alias, details={"headless": self.settings.headless})

        launch_kwargs = {
            "headless": self.settings.headless,
            "slow_mo": 80,
        }
        if self.settings.proxy_url:
            launch_kwargs["proxy"] = {"server": self.settings.proxy_url}
        executable = resolve_playwright_executable(headless=self.settings.headless)
        self.browser = launch_sync_browser(
            headless=self.settings.headless,
            executable_path=executable,
            proxy=launch_kwargs.get("proxy"),
            slow_mo=int(launch_kwargs.get("slow_mo") or 0),
            visible_reason=f"opt_in.browser_tools:{self.account_alias or 'default'}",
        )

        storage_state = None
        if self.account_alias:
            storage_state = self.session_store.load(self.account_alias)

        context_kwargs = {}
        if storage_state:
            context_kwargs["storage_state"] = storage_state

        self.context = self.browser.new_context(**context_kwargs)
        self.context.set_default_navigation_timeout(self.settings.navigation_timeout * 1000)
        self.context.set_default_timeout(self.settings.wait_timeout * 1000)

        self.page = self.context.new_page()
        # Warm up timers with small delay so actions look human.
        random_human_delay(self.settings.action_delay)

    def close(self, save_state: bool = True) -> None:
        if save_state and self.persist_session and self.context:
            try:
                state = self.context.storage_state()
                self.session_store.save(self.account_alias or "default", state)
            except Exception as error:
                audit.log_error("browser.save_failed", self.account_alias, error)

        with contextlib.suppress(Exception):
            if self.page and not self.page.is_closed():
                self.page.close()

        with contextlib.suppress(Exception):
            if self.context:
                self.context.close()

        with contextlib.suppress(Exception):
            if self.browser:
                self.browser.close()

        self._playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def ensure_page(self) -> Page:
        if not self.page:
            raise RuntimeError("Browser page not initialized")
        return self.page

    def go(self, url: str, wait_until: str = "networkidle") -> Page:
        page = self.ensure_page()
        audit.log_event("browser.goto", account=self.account_alias, details={"url": url})
        page.goto(url, wait_until=wait_until)
        random_human_delay(self.settings.action_delay)
        return page

    def wait_idle(self, seconds: float = 1.2) -> None:
        time.sleep(max(seconds, 0.1))

    def save_session(self) -> None:
        if not self.context or not self.account_alias:
            return
        try:
            state = self.context.storage_state()
            self.session_store.save(self.account_alias, state)
        except Exception as error:
            audit.log_error("browser.save_failed", self.account_alias, error)
