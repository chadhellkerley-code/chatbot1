from src.instagram.page_state_detector import (
    InstagramPageState,
    detect_instagram_page_state,
)
from src.auth.login_states import LoginState


class LoginStateMachine:
    def __init__(self, page=None):
        self.page = page
        self.last_page_state: InstagramPageState | None = None

    def init(self, page):
        self.page = page

    async def detect(self) -> LoginState:
        state = await detect_instagram_page_state(self.page)
        self.last_page_state = state

        if state in (
            InstagramPageState.FEED,
            InstagramPageState.SESSION_VALID,
            InstagramPageState.INBOX,
        ):
            return LoginState.SESSION_VALID

        if state == InstagramPageState.LOGIN_PAGE:
            return LoginState.LOGIN_REQUIRED

        if state == InstagramPageState.CHALLENGE:
            return LoginState.CHALLENGE_DETECTED

        if state == InstagramPageState.TWO_FACTOR:
            return LoginState.TWO_FACTOR_REQUIRED

        if state == InstagramPageState.CAPTCHA:
            return LoginState.CAPTCHA_DETECTED

        if state == InstagramPageState.ACCOUNT_DISABLED:
            return LoginState.ACCOUNT_DISABLED

        if state == InstagramPageState.RATE_LIMITED:
            return LoginState.RATE_LIMITED

        return LoginState.UNKNOWN
