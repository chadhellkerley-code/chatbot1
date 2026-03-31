import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv
from paths import campaigns_root, logs_root, sessions_root


load_dotenv()


@dataclass(frozen=True)
class DelayRange:
    minimum: float
    maximum: float

    def as_tuple(self) -> Tuple[float, float]:
        lo = min(self.minimum, self.maximum)
        hi = max(self.minimum, self.maximum)
        return (lo, hi)


@dataclass(frozen=True)
class OptInSettings:
    enable: bool
    headless: bool
    proxy_url: Optional[str]
    keyboard_delay: DelayRange
    action_delay: DelayRange
    navigation_timeout: float
    wait_timeout: float
    send_code_cooldown: float
    session_encryption_key: Optional[str]
    sessions_dir: Path
    flows_dir: Path
    audit_log_path: Path
    totp_secrets: Dict[str, str]


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_float(value: Optional[str], default: float) -> float:
    if not value:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _delay_from_env(name_min: str, name_max: str, default_min: float, default_max: float) -> DelayRange:
    minimum = _parse_float(os.getenv(name_min), default_min)
    maximum = _parse_float(os.getenv(name_max), default_max)
    return DelayRange(minimum=minimum, maximum=maximum)


def _discover_totp_secrets() -> Dict[str, str]:
    result: Dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith("OPTIN_TOTP_"):
            continue
        alias = key.replace("OPTIN_TOTP_", "", 1).strip().lower()
        if not alias or not value:
            continue
        result[alias] = value.strip()
    return result


@lru_cache(maxsize=1)
def get_settings() -> OptInSettings:
    base = _root_dir()
    sessions_dir = sessions_root(base) / "optin"
    flows_dir = campaigns_root(base) / "opt_in_flows"
    logs_dir = logs_root(base)
    audit_log_path = logs_dir / "optin_audit.jsonl"

    settings = OptInSettings(
        enable=_parse_bool(os.getenv("OPTIN_ENABLE"), False),
        headless=_parse_bool(os.getenv("OPTIN_HEADLESS"), False),
        proxy_url=os.getenv("OPTIN_PROXY_URL") or None,
        keyboard_delay=_delay_from_env("OPTIN_KEYBOARD_DELAY_MIN", "OPTIN_KEYBOARD_DELAY_MAX", 0.08, 0.22),
        action_delay=_delay_from_env("OPTIN_ACTION_DELAY_MIN", "OPTIN_ACTION_DELAY_MAX", 0.3, 1.1),
        navigation_timeout=_parse_float(os.getenv("OPTIN_NAVIGATION_TIMEOUT"), 25.0),
        wait_timeout=_parse_float(os.getenv("OPTIN_WAIT_TIMEOUT"), 12.0),
        send_code_cooldown=_parse_float(os.getenv("OPTIN_SEND_CODE_COOLDOWN"), 45.0),
        session_encryption_key=os.getenv("SESSION_ENCRYPTION_KEY") or None,
        sessions_dir=sessions_dir,
        flows_dir=flows_dir,
        audit_log_path=audit_log_path,
        totp_secrets=_discover_totp_secrets(),
    )

    sessions_dir.mkdir(parents=True, exist_ok=True)
    flows_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    return settings
