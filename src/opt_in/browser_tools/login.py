from __future__ import annotations

import time
from typing import Optional

import pyotp
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout

from . import audit
from .browser_manager import BrowserManager
from .config import OptInSettings, get_settings
from .utils import click_first, fill_slow, random_human_delay, wait_first_selector


LOGIN_URL = "https://www.instagram.com/accounts/login/"
HOME_URL = "https://www.instagram.com/"


class LoginFlow:
    def __init__(self, account_alias: str, totp_secret: Optional[str] = None) -> None:
        self.account_alias = account_alias
        self.settings: OptInSettings = get_settings()
        self.totp_secret = totp_secret or self.settings.totp_secrets.get(account_alias.lower())

    def run(self, username: str, password: str) -> None:
        with BrowserManager(self.account_alias, persist_session=True) as manager:
            page = manager.ensure_page()
            manager.go(LOGIN_URL)
            self._accept_cookies_if_needed(page)
            self._submit_credentials(page, username, password)
            self._handle_two_factor_if_needed(page)
            self._post_login_cleanup(page)
            manager.wait_idle(2.0)
            manager.save_session()
            audit.log_event("login.success", account=self.account_alias, details={"username": username})

    def _accept_cookies_if_needed(self, page: Page) -> None:
        selectors = [
            "button:has-text('Allow all cookies')",
            "button:has-text('Aceptar todo')",
            "text=Allow essential cookies only",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector)
                if locator.is_visible():
                    locator.click()
                    random_human_delay(self.settings.action_delay)
                    break
            except PlaywrightTimeout:
                continue

    def _submit_credentials(self, page: Page, username: str, password: str) -> None:
        user_loc = wait_first_selector(page, ["input[name='username']"])
        pass_loc = wait_first_selector(page, ["input[name='password']"])

        fill_slow(user_loc, username)
        fill_slow(pass_loc, password)

        click_first(page, ["button:has-text('Log in')", "button:has-text('Iniciar sesión')"])

    def _handle_two_factor_if_needed(self, page: Page) -> None:
        selectors = [
            "input[name='verificationCode']",
            "input[name='approvals_code']",
            "input[name='email_confirmation_code']",
            "input[name='security_code']",
            "input[aria-label*='security code']",
        ]
        try:
            code_input = wait_first_selector(page, selectors, timeout=6000)
        except PlaywrightTimeout:
            return

        self._ensure_code_sent(page)

        code = self._resolve_code()
        if not code:
            raise RuntimeError("No se proporcionó código 2FA")

        fill_slow(code_input, code)
        click_first(page, ["button:has-text('Confirm')", "button:has-text('Submit')", "button:has-text('Enviar')"])

    def _ensure_code_sent(self, page: Page) -> None:
        selectors = [
            "button:has-text('Send security code')",
            "button:has-text('Send code')",
            "button:has-text('Send login code')",
            "button:has-text('Enviar código')",
        ]
        cooldown = self.settings.send_code_cooldown
        started = time.time()

        for selector in selectors:
            locator = page.locator(selector)
            if locator.is_visible():
                locator.click()
                random_human_delay(self.settings.action_delay)
                audit.log_event("login.code_requested", account=self.account_alias, details={"selector": selector})
                break

        while True:
            status_text = page.locator("text=Resend code")
            if status_text.is_visible():
                random_human_delay(self.settings.action_delay)
                time.sleep(2)
                if time.time() - started >= cooldown:
                    click_first(page, selectors)
                    started = time.time()
                continue
            break

    def _resolve_code(self) -> Optional[str]:
        if self.totp_secret:
            try:
                return pyotp.TOTP(self.totp_secret).now()
            except Exception:
                pass
        return input("Introduce el código recibido (WhatsApp / SMS / App): ").strip()

    def _post_login_cleanup(self, page: Page) -> None:
        dialogs = [
            "text=Save your login info?",
            "text=Save login information",
            "text=Turn on Notifications",
        ]
        buttons = [
            "button:has-text('Not Now')",
            "button:has-text('Ahora no')",
            "button:has-text('No permitir')",
        ]

        end_time = time.time() + 15
        while time.time() < end_time:
            handled = False
            for dialog_selector in dialogs:
                locator = page.locator(dialog_selector)
                if locator.is_visible():
                    click_first(page, buttons)
                    handled = True
                    break
            if not handled:
                break


def cli_login(account: str, username: str, password: str, totp_secret: Optional[str] = None) -> None:
    audit.log_event("login.start", account=account, details={"username": username})
    flow = LoginFlow(account_alias=account, totp_secret=totp_secret)
    flow.run(username=username, password=password)
