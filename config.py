<<<<<<< HEAD
# config.py
# -*- coding: utf-8 -*-
"""Carga y validación de parámetros de configuración."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
=======
# config.py
# -*- coding: utf-8 -*-
"""Carga y validación de parámetros de configuración."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
>>>>>>> origin/main
from pathlib import Path
from typing import Dict, Tuple

from dotenv import dotenv_values, load_dotenv
from paths import app_root, logs_root, runtime_base, storage_root

_BASE_ROOT = runtime_base(Path(__file__).resolve().parent)
_ROOT = app_root(_BASE_ROOT)
_ENV_FILENAMES = (".env", ".env.local")
_STORAGE_ROOT = storage_root(_BASE_ROOT)
_APP_CONFIG_DIR = _BASE_ROOT / "app"
_CONFIG_FILE = _APP_CONFIG_DIR / "config.json"
_LEGACY_CONFIG_FILE = _STORAGE_ROOT / "config.json"
<<<<<<< HEAD


def _load_file_values() -> Dict[str, str]:
    values: Dict[str, str] = {}
    for filename in _ENV_FILENAMES:
        file_path = _ROOT / filename
        if not file_path.exists():
            continue
        load_dotenv(file_path, override=filename != ".env")
        file_values = {
            key: value
            for key, value in dotenv_values(file_path).items()
            if value is not None
        }
        values.update(file_values)
    return values


def _coerce_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def _coerce_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


@dataclass(frozen=True)
class Settings:
    max_per_account: int = 50
    max_concurrency: int = 5
    delay_min: int = 45
    delay_max: int = 55
    autoresponder_delay: int = 10
    quiet: bool = False
    log_dir: Path = logs_root(_BASE_ROOT)
    log_file: str = "app.log"
    supabase_url: str = ""
    supabase_key: str = ""
    openai_api_key: str = ""
    client_distribution: bool = False
    proxy_default_url: str = ""
    proxy_default_user: str = ""
    proxy_default_pass: str = ""
    proxy_sticky_minutes: int = 10
    low_profile_age_days: int = 14
    low_profile_profile_edit_threshold: int = 3
    low_profile_activity_window_hours: int = 48
    low_profile_activity_threshold: int = 30
    low_profile_daily_cap: int = 15
    low_profile_delay_factor: int = 150
    prompt_2fa_sms: bool = True
    prompt_2fa_timeout_seconds: int = 180


def _validated_ranges(values: Dict[str, str]) -> Tuple[int, int, int, int]:
    max_per_account = _coerce_int(values.get("MAX_PER_ACCOUNT"), 50)
    if max_per_account < 2:
        logging.warning("MAX_PER_ACCOUNT debe ser >=2. Se ajusta a 2.")
        max_per_account = 2

    max_concurrency = _coerce_int(values.get("MAX_CONCURRENCY"), 5)
    if max_concurrency < 1:
        logging.warning("MAX_CONCURRENCY debe ser >=1. Se ajusta a 1.")
        max_concurrency = 1

    delay_min = _coerce_int(values.get("DELAY_MIN"), 45)
    if delay_min < 10:
        logging.warning("DELAY_MIN debe ser >=10. Se ajusta a 10.")
        delay_min = 10

    delay_max = _coerce_int(values.get("DELAY_MAX"), 55)
    if delay_max < delay_min:
        logging.warning("DELAY_MAX debe ser >= DELAY_MIN. Se ajusta a %s.", delay_min)
        delay_max = delay_min

    return max_per_account, max_concurrency, delay_min, delay_max


=======


def _load_file_values() -> Dict[str, str]:
    values: Dict[str, str] = {}
    for filename in _ENV_FILENAMES:
        file_path = _ROOT / filename
        if not file_path.exists():
            continue
        load_dotenv(file_path, override=filename != ".env")
        file_values = {
            key: value
            for key, value in dotenv_values(file_path).items()
            if value is not None
        }
        values.update(file_values)
    return values


def _coerce_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def _coerce_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


@dataclass(frozen=True)
class Settings:
    max_per_account: int = 50
    max_concurrency: int = 5
    delay_min: int = 45
    delay_max: int = 55
    autoresponder_delay: int = 10
    quiet: bool = False
    log_dir: Path = logs_root(_BASE_ROOT)
    log_file: str = "app.log"
    supabase_url: str = ""
    supabase_key: str = ""
    openai_api_key: str = ""
    client_distribution: bool = False
    proxy_default_url: str = ""
    proxy_default_user: str = ""
    proxy_default_pass: str = ""
    proxy_sticky_minutes: int = 10
    low_profile_age_days: int = 14
    low_profile_profile_edit_threshold: int = 3
    low_profile_activity_window_hours: int = 48
    low_profile_activity_threshold: int = 30
    low_profile_daily_cap: int = 15
    low_profile_delay_factor: int = 150
    prompt_2fa_sms: bool = True
    prompt_2fa_timeout_seconds: int = 180


def _validated_ranges(values: Dict[str, str]) -> Tuple[int, int, int, int]:
    max_per_account = _coerce_int(values.get("MAX_PER_ACCOUNT"), 50)
    if max_per_account < 2:
        logging.warning("MAX_PER_ACCOUNT debe ser >=2. Se ajusta a 2.")
        max_per_account = 2

    max_concurrency = _coerce_int(values.get("MAX_CONCURRENCY"), 5)
    if max_concurrency < 1:
        logging.warning("MAX_CONCURRENCY debe ser >=1. Se ajusta a 1.")
        max_concurrency = 1

    delay_min = _coerce_int(values.get("DELAY_MIN"), 45)
    if delay_min < 10:
        logging.warning("DELAY_MIN debe ser >=10. Se ajusta a 10.")
        delay_min = 10

    delay_max = _coerce_int(values.get("DELAY_MAX"), 55)
    if delay_max < delay_min:
        logging.warning("DELAY_MAX debe ser >= DELAY_MIN. Se ajusta a %s.", delay_min)
        delay_max = delay_min

    return max_per_account, max_concurrency, delay_min, delay_max


>>>>>>> origin/main
def _client_distribution_flag(
    env_values: Dict[str, str],
    app_config: Dict[str, str],
    default: bool,
) -> bool:
    env_value = env_values.get("CLIENT_DISTRIBUTION")
    if env_value is not None:
        return _coerce_bool(env_value, default)

    config_value = app_config.get("client_distribution")
    if config_value is not None and str(config_value).strip() != "":
        return _coerce_bool(str(config_value), default)

    channel = str(app_config.get("channel") or app_config.get("edition") or "").strip().lower()
    if channel:
        return channel == "client"

    return default
<<<<<<< HEAD


=======


>>>>>>> origin/main
def load_settings() -> Settings:
    file_values = _load_file_values()
    app_config = read_app_config()
    env_values = {**file_values, **os.environ}
    max_per_account, max_concurrency, delay_min, delay_max = _validated_ranges(env_values)

    defaults = Settings()
    client_distribution = _client_distribution_flag(
        env_values,
        app_config,
        defaults.client_distribution,
    )
    supabase_url = str(
        env_values.get("SUPABASE_URL")
        or app_config.get("supabase_url")
        or app_config.get("SUPABASE_URL")
        or ""
    ).strip()
    supabase_key = str(
        env_values.get("SUPABASE_KEY")
        or env_values.get("SUPABASE_ANON_KEY")
        or env_values.get("SUPABASE_SERVICE_ROLE_KEY")
        or app_config.get("supabase_key")
        or app_config.get("SUPABASE_KEY")
        or ""
    ).strip()
    low_profile_age_days = max(1, _coerce_int(env_values.get("LOW_PROFILE_AGE_DAYS"), defaults.low_profile_age_days))
    low_profile_profile_edit_threshold = max(
        1,
        _coerce_int(
            env_values.get("LOW_PROFILE_PROFILE_EDIT_THRESHOLD"),
<<<<<<< HEAD
            defaults.low_profile_profile_edit_threshold,
        ),
    )
    low_profile_activity_window_hours = max(
        1,
        _coerce_int(
            env_values.get("LOW_PROFILE_ACTIVITY_WINDOW_HOURS"),
            defaults.low_profile_activity_window_hours,
        ),
    )
    low_profile_activity_threshold = max(
        0,
        _coerce_int(
            env_values.get("LOW_PROFILE_ACTIVITY_THRESHOLD"),
            defaults.low_profile_activity_threshold,
        ),
    )
    low_profile_daily_cap = max(
        1, _coerce_int(env_values.get("LOW_PROFILE_DAILY_CAP"), defaults.low_profile_daily_cap)
    )
    low_profile_delay_factor = max(
        100,
        _coerce_int(env_values.get("LOW_PROFILE_DELAY_FACTOR"), defaults.low_profile_delay_factor),
    )

    return Settings(
        max_per_account=max_per_account,
        max_concurrency=max_concurrency,
        delay_min=delay_min,
=======
            defaults.low_profile_profile_edit_threshold,
        ),
    )
    low_profile_activity_window_hours = max(
        1,
        _coerce_int(
            env_values.get("LOW_PROFILE_ACTIVITY_WINDOW_HOURS"),
            defaults.low_profile_activity_window_hours,
        ),
    )
    low_profile_activity_threshold = max(
        0,
        _coerce_int(
            env_values.get("LOW_PROFILE_ACTIVITY_THRESHOLD"),
            defaults.low_profile_activity_threshold,
        ),
    )
    low_profile_daily_cap = max(
        1, _coerce_int(env_values.get("LOW_PROFILE_DAILY_CAP"), defaults.low_profile_daily_cap)
    )
    low_profile_delay_factor = max(
        100,
        _coerce_int(env_values.get("LOW_PROFILE_DELAY_FACTOR"), defaults.low_profile_delay_factor),
    )

    return Settings(
        max_per_account=max_per_account,
        max_concurrency=max_concurrency,
        delay_min=delay_min,
>>>>>>> origin/main
        delay_max=delay_max,
        autoresponder_delay=max(1, _coerce_int(env_values.get("AUTORESPONDER_DELAY"), defaults.autoresponder_delay)),
        quiet=_coerce_bool(env_values.get("QUIET"), defaults.quiet),
        log_dir=_coerce_path(env_values.get("LOG_DIR"), defaults.log_dir),
        log_file=env_values.get("LOG_FILE", defaults.log_file),
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        openai_api_key=env_values.get("OPENAI_API_KEY", ""),
        client_distribution=client_distribution,
        proxy_default_url=env_values.get("PROXY_DEFAULT_URL", defaults.proxy_default_url),
        proxy_default_user=env_values.get("PROXY_DEFAULT_USER", defaults.proxy_default_user),
        proxy_default_pass=env_values.get("PROXY_DEFAULT_PASS", defaults.proxy_default_pass),
<<<<<<< HEAD
        proxy_sticky_minutes=max(
            1,
            _coerce_int(
                env_values.get("PROXY_STICKY_MINUTES"), defaults.proxy_sticky_minutes
            ),
        ),
        low_profile_age_days=low_profile_age_days,
        low_profile_profile_edit_threshold=low_profile_profile_edit_threshold,
        low_profile_activity_window_hours=low_profile_activity_window_hours,
        low_profile_activity_threshold=low_profile_activity_threshold,
        low_profile_daily_cap=low_profile_daily_cap,
        low_profile_delay_factor=low_profile_delay_factor,
        prompt_2fa_sms=_coerce_bool(
            env_values.get("PROMPT_2FA_SMS"), defaults.prompt_2fa_sms
        ),
        prompt_2fa_timeout_seconds=max(
            10,
            _coerce_int(
                env_values.get("PROMPT_2FA_TIMEOUT_SECONDS"),
                defaults.prompt_2fa_timeout_seconds,
            ),
        ),
    )


def read_env_local() -> Dict[str, str]:
    path = _ROOT / ".env.local"
    if not path.exists():
        return {}
    data = {}
    for key, value in dotenv_values(path).items():
        if value is not None:
            data[key] = value
    return data


def update_env_local(updates: Dict[str, str]) -> Path:
    path = _ROOT / ".env.local"
    current = read_env_local()
    current.update({k: v for k, v in updates.items() if v is not None})
    lines = [f"{key}={value}" for key, value in sorted(current.items())]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


=======
        proxy_sticky_minutes=max(
            1,
            _coerce_int(
                env_values.get("PROXY_STICKY_MINUTES"), defaults.proxy_sticky_minutes
            ),
        ),
        low_profile_age_days=low_profile_age_days,
        low_profile_profile_edit_threshold=low_profile_profile_edit_threshold,
        low_profile_activity_window_hours=low_profile_activity_window_hours,
        low_profile_activity_threshold=low_profile_activity_threshold,
        low_profile_daily_cap=low_profile_daily_cap,
        low_profile_delay_factor=low_profile_delay_factor,
        prompt_2fa_sms=_coerce_bool(
            env_values.get("PROMPT_2FA_SMS"), defaults.prompt_2fa_sms
        ),
        prompt_2fa_timeout_seconds=max(
            10,
            _coerce_int(
                env_values.get("PROMPT_2FA_TIMEOUT_SECONDS"),
                defaults.prompt_2fa_timeout_seconds,
            ),
        ),
    )


def read_env_local() -> Dict[str, str]:
    path = _ROOT / ".env.local"
    if not path.exists():
        return {}
    data = {}
    for key, value in dotenv_values(path).items():
        if value is not None:
            data[key] = value
    return data


def update_env_local(updates: Dict[str, str]) -> Path:
    path = _ROOT / ".env.local"
    current = read_env_local()
    current.update({k: v for k, v in updates.items() if v is not None})
    lines = [f"{key}={value}" for key, value in sorted(current.items())]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


>>>>>>> origin/main
def read_app_config() -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for path in (_LEGACY_CONFIG_FILE, _CONFIG_FILE):
        if not path.exists():
            continue
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(loaded, dict):
            payload.update(loaded)
    return payload


def update_app_config(updates: Dict[str, str]) -> Dict[str, str]:
    current = read_app_config()
    current.update({k: v for k, v in updates.items() if v is not None})
<<<<<<< HEAD
    _CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
=======
    _CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
>>>>>>> origin/main
    _CONFIG_FILE.write_text(
        json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return current


def read_supabase_config() -> Dict[str, str]:
    payload = read_app_config()
    return {
        "supabase_url": str(payload.get("supabase_url") or payload.get("SUPABASE_URL") or "").strip(),
        "supabase_key": str(payload.get("supabase_key") or payload.get("SUPABASE_KEY") or "").strip(),
    }


def update_supabase_config(*, supabase_url: str, supabase_key: str) -> Dict[str, str]:
    return update_app_config(
        {
            "supabase_url": str(supabase_url or "").strip(),
            "supabase_key": str(supabase_key or "").strip(),
        }
    )
<<<<<<< HEAD


def refresh_settings() -> Settings:
    global SETTINGS
    SETTINGS = load_settings()
    return SETTINGS


SETTINGS = load_settings()
=======


def refresh_settings() -> Settings:
    global SETTINGS
    SETTINGS = load_settings()
    return SETTINGS


SETTINGS = load_settings()
>>>>>>> origin/main
