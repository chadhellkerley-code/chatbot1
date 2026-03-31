from enum import Enum


class LoginState(Enum):
    LOGIN_START = "login_start"
    PAGE_STATE_DETECTED = "page_state_detected"
    LOGIN_REQUIRED = "login_required"
    LOGIN_SUBMITTED = "login_submitted"
    LOGIN_RESULT = "login_result"
    SESSION_VALID = "session_valid"
    SESSION_INVALID = "session_invalid"
    CHALLENGE_DETECTED = "challenge_detected"
    CAPTCHA_DETECTED = "captcha_detected"
    TWO_FACTOR_REQUIRED = "two_factor_required"
    ACCOUNT_DISABLED = "account_disabled"
    RATE_LIMITED = "rate_limited"
    UNKNOWN = "unknown"

