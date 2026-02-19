# accounts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import builtins
import contextlib
import csv
import getpass
import io
import json
import random
import re
import sys
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import logging
from urllib.parse import urlparse

from config import SETTINGS
from client_factory import get_instagram_client
from adapters.base import TwoFARequired, TwoFactorCodeRejected


def _ask_secret(prompt: str) -> str:
    """
    Read secret values without blocking when UI mode monkeypatches input().
    """
    input_fn = getattr(builtins, "input", None)
    if getattr(input_fn, "__module__", "") == "io_adapter":
        return ask(prompt)
    stdin = getattr(sys, "stdin", None)
    stdout = getattr(sys, "stdout", None)
    try:
        has_tty = bool(stdin and stdin.isatty() and stdout and stdout.isatty())
    except Exception:
        has_tty = False
    if not has_tty:
        return ask(prompt)
    try:
        return getpass.getpass(prompt)
    except (EOFError, KeyboardInterrupt):
        raise
    except Exception:
        return ask(prompt)


# FunciÃ³n temporal hasta que se migre correctamente
def prompt_two_factor_code(username: str, method: str, attempt: int):
    """Stub temporal - solicita cÃ³digo 2FA manualmente"""
    prompt = f"Ingrese el cÃ³digo recibido por {method} para {username}: "
    code = _ask_secret(prompt) if method.lower() == "totp" else input(prompt)
    return code.strip().replace("-", "").replace(" ", "") if code else None

try:
    from src.auth.onboarding import (
        build_proxy as build_playwright_proxy,
        login_and_persist,
        onboard_accounts_from_csv,
    )
    _ONBOARDING_BACKEND_ERROR: Optional[Exception] = None
    _ONBOARDING_AVAILABLE = True
except Exception as exc:  # pragma: no cover - depende de entorno
    build_playwright_proxy = None
    login_and_persist = None
    onboard_accounts_from_csv = None
    _ONBOARDING_BACKEND_ERROR = exc
    _ONBOARDING_AVAILABLE = False
try:
    from src.auth.persistent_login import check_session as _check_playwright_session
except Exception:
    _check_playwright_session = None
from proxy_manager import (
    ProxyConfig,
    apply_proxy_to_client,
    clear_proxy,
    default_proxy_settings,
    record_proxy_failure,
    should_retry_proxy,
    test_proxy_connection,
)
from session_store import has_session, load_into, remove as remove_session, save_from
from totp_store import generate_code as generate_totp_code
from totp_store import get_secret as get_totp_secret
from totp_store import has_secret as has_totp_secret
from totp_store import remove_secret as remove_totp_secret
from totp_store import rename_secret as rename_totp_secret
from totp_store import save_secret as save_totp_secret
from utils import ask, ask_int, banner, em, ok, press_enter, title, warn
from paths import runtime_base
from src.playwright_service import BASE_PROFILES

BASE = runtime_base(Path(__file__).resolve().parent)
BASE.mkdir(parents=True, exist_ok=True)
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
FILE = DATA / "accounts.json"
_PASSWORD_FILE = DATA / "passwords.json"

_LOGIN_FAILURE_BACKOFF = timedelta(minutes=5)
_LOGIN_FAILURES: Dict[str, datetime] = {}
_LOGIN_FAILURE_LOCK = Lock()


def _password_key(username: str | None) -> str:
    if not username:
        return ""
    return username.strip().lstrip("@").lower()


def _load_password_cache() -> Dict[str, str]:
    if not _PASSWORD_FILE.exists():
        return {}
    try:
        raw = json.loads(_PASSWORD_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    cache: Dict[str, str] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            normalized = key.strip().lower()
            if not normalized or not value:
                continue
            cache[normalized] = value
    return cache


def _save_password_cache(cache: Dict[str, str]) -> None:
    try:
        _PASSWORD_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


def _record_login_failure(username: str) -> None:
    key = _password_key(username)
    if not key:
        return
    with _LOGIN_FAILURE_LOCK:
        _LOGIN_FAILURES[key] = datetime.utcnow()


def _clear_login_failure(username: str) -> None:
    key = _password_key(username)
    if not key:
        return
    with _LOGIN_FAILURE_LOCK:
        _LOGIN_FAILURES.pop(key, None)


def _login_backoff_remaining(username: str) -> float:
    key = _password_key(username)
    if not key:
        return 0.0
    with _LOGIN_FAILURE_LOCK:
        timestamp = _LOGIN_FAILURES.get(key)
        if not timestamp:
            return 0.0
        elapsed = datetime.utcnow() - timestamp
        if elapsed >= _LOGIN_FAILURE_BACKOFF:
            _LOGIN_FAILURES.pop(key, None)
            return 0.0
        return (_LOGIN_FAILURE_BACKOFF - elapsed).total_seconds()


_PASSWORD_CACHE: Dict[str, str] = _load_password_cache()

logger = logging.getLogger(__name__)

# Account health is persisted in data/account_health.json (same structure as before),
# but it is now updated ONLY by Playwright flows (no API-based verification).
import health_store


_SENT_LOG = BASE / "storage" / "sent_log.jsonl"
_ACTIVITY_CACHE_TTL = timedelta(minutes=5)
_ACTIVITY_CACHE: Optional[Tuple[int, datetime, Dict[str, int]]] = None


_CSV_HEADERS = [
    "username",
    "password",
    "2fa code",
    "proxy id",
    "proxy port",
    "proxy username",
    "proxy password",
]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: object) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _settings_value(name: str, default: int) -> int:
    try:
        value = getattr(SETTINGS, name)
    except AttributeError:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _ensure_timestamp(record: Dict, key: str) -> Tuple[Optional[datetime], bool]:
    original = record.get(key)
    parsed = _parse_datetime(original)
    if parsed is None:
        record.pop(key, None)
        return None, bool(original)
    formatted = _isoformat_utc(parsed)
    if formatted != original:
        record[key] = formatted
        return parsed, True
    return parsed, False


def _ensure_first_seen(record: Dict) -> bool:
    first_seen, changed = _ensure_timestamp(record, "first_seen")
    if first_seen is not None:
        return changed
    now_iso = _isoformat_utc(_now_utc())
    record["first_seen"] = now_iso
    return True


def _normalize_profile_edit_metadata(record: Dict) -> None:
    try:
        count = int(record.get("profile_edit_count", 0))
    except Exception:
        count = 0
    record["profile_edit_count"] = max(0, count)

    types_raw = record.get("profile_edit_types")
    if isinstance(types_raw, list):
        normalized = sorted({str(item).strip() for item in types_raw if str(item).strip()})
    else:
        normalized = []
    record["profile_edit_types"] = normalized

    _ensure_timestamp(record, "last_profile_edit")


def _recent_activity_counts() -> Dict[str, int]:
    global _ACTIVITY_CACHE
    window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
    now = _now_utc()
    if _ACTIVITY_CACHE is not None:
        cached_window, timestamp, cached_counts = _ACTIVITY_CACHE
        if cached_window == window_hours and now - timestamp < _ACTIVITY_CACHE_TTL:
            return dict(cached_counts)

    counts: Dict[str, int] = defaultdict(int)
    cutoff = now - timedelta(hours=window_hours)
    if _SENT_LOG.exists():
        try:
            with _SENT_LOG.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                    except Exception:
                        continue
                    ts_raw = entry.get("ts")
                    if ts_raw is None:
                        continue
                    try:
                        ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                    except Exception:
                        continue
                    if ts < cutoff:
                        continue
                    username = str(entry.get("account") or "").strip().lstrip("@").lower()
                    if not username:
                        continue
                    counts[username] += 1
        except Exception:
            counts = defaultdict(int)

    frozen = dict(counts)
    _ACTIVITY_CACHE = (window_hours, now, frozen)
    return frozen


def _auto_low_profile(record: Dict) -> Tuple[bool, str, int]:
    username = (record.get("username") or "").strip().lstrip("@").lower()
    if not username:
        return False, "", 0

    _ensure_first_seen(record)
    first_seen = _parse_datetime(record.get("first_seen"))
    age_days = 0.0
    if first_seen is not None:
        age_days = (_now_utc() - first_seen).total_seconds() / 86400

    recent_activity_map = _recent_activity_counts()
    recent_activity = int(recent_activity_map.get(username, 0))
    record["recent_activity_count"] = recent_activity

    age_limit = max(1, _settings_value("low_profile_age_days", 14))
    edits_threshold = max(1, _settings_value("low_profile_profile_edit_threshold", 3))
    activity_threshold = max(0, _settings_value("low_profile_activity_threshold", 30))

    is_new = age_days < age_limit
    edit_count = int(record.get("profile_edit_count", 0) or 0)
    has_many_edits = edit_count >= edits_threshold
    has_high_activity = activity_threshold > 0 and recent_activity >= activity_threshold

    reasons: list[str] = []
    if is_new:
        reasons.append(f"cuenta nueva ({int(age_days)}d)")
    if has_many_edits:
        reasons.append(f"{edit_count} cambios de perfil")
    if has_high_activity:
        window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
        reasons.append(f"{recent_activity} envÃ­os/{window_hours}h")

    should_flag = is_new and (has_many_edits or has_high_activity)
    reason_text = "; ".join(reasons) if should_flag else ""
    return should_flag, reason_text, recent_activity


def _record_profile_edit(username: str, kind: str) -> None:
    normalized = username.strip().lstrip("@").lower()
    if not normalized:
        return

    items = _load()
    updated = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored != normalized:
            continue
        record = dict(item)
        try:
            count = int(record.get("profile_edit_count", 0))
        except Exception:
            count = 0
        record["profile_edit_count"] = max(0, count) + 1

        types_raw = record.get("profile_edit_types")
        types: list[str]
        if isinstance(types_raw, list):
            types = [str(entry).strip() for entry in types_raw if str(entry).strip()]
        else:
            types = []
        kind_clean = kind.strip()
        if kind_clean and kind_clean not in types:
            types.append(kind_clean)
        record["profile_edit_types"] = sorted(set(types))
        record["last_profile_edit"] = _isoformat_utc(_now_utc())
        items[idx] = record
        updated = True
        break

    if updated:
        _save(items)

def _normalize_account(record: Dict) -> Dict:
    result = dict(record)
    result.setdefault("alias", "default")
    result.setdefault("active", True)
    result.setdefault("connected", False)
    result.setdefault("password", "")
    result.setdefault("proxy_url", "")
    result.setdefault("proxy_user", "")
    result.setdefault("proxy_pass", "")
    sticky_default = SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_value = int(result.get("proxy_sticky_minutes", sticky_default))
    except Exception:
        sticky_value = sticky_default
    result["proxy_sticky_minutes"] = max(1, sticky_value)

    _normalize_profile_edit_metadata(result)
    _ensure_first_seen(result)

    username = result.get("username")
    if username:
        key = _password_key(username)
        if not result.get("password") and key:
            cached = _PASSWORD_CACHE.get(key)
            if cached:
                result["password"] = cached
        result["has_totp"] = has_totp_secret(username)
    else:
        result.setdefault("has_totp", False)

    manual_override = bool(result.get("low_profile_manual"))
    auto_flag, auto_reason, recent_activity = _auto_low_profile(result)
    result["recent_activity_count"] = recent_activity
    result["low_profile_auto"] = auto_flag

    if manual_override:
        manual_value = bool(result.get("low_profile"))
        result["low_profile"] = manual_value
        existing_reason = str(result.get("low_profile_reason") or "")
        if manual_value and not existing_reason:
            result["low_profile_reason"] = "Marcado manualmente"
        elif not manual_value:
            result["low_profile_reason"] = existing_reason
        result["low_profile_source"] = "manual" if manual_value else ""
    else:
        result["low_profile"] = auto_flag
        result["low_profile_reason"] = auto_reason if auto_flag else ""
        result["low_profile_source"] = "auto" if auto_flag else ""
        result.setdefault("low_profile_manual", False)

    return result


def _prepare_for_save(record: Dict) -> Dict:
    stored = dict(record)
    if stored.get("proxy_url"):
        try:
            stored["proxy_sticky_minutes"] = int(
                stored.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)
            )
        except Exception:
            stored["proxy_sticky_minutes"] = SETTINGS.proxy_sticky_minutes
    else:
        stored.pop("proxy_url", None)
        stored.pop("proxy_user", None)
        stored.pop("proxy_pass", None)
        stored.pop("proxy_sticky_minutes", None)
    stored.pop("has_totp", None)
    stored.pop("recent_activity_count", None)
    stored.pop("low_profile_auto", None)
    stored.pop("low_profile_source", None)

    manual_override = bool(stored.get("low_profile_manual"))
    if manual_override:
        stored["low_profile"] = bool(stored.get("low_profile"))
        reason = str(stored.get("low_profile_reason") or "")
        if reason:
            stored["low_profile_reason"] = reason
        else:
            stored.pop("low_profile_reason", None)
    else:
        stored.pop("low_profile", None)
        stored.pop("low_profile_reason", None)
        stored.pop("low_profile_manual", None)

    return stored


def _load() -> List[Dict]:
    if not FILE.exists():
        return []
    try:
        data = json.loads(FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    normalized: List[Dict] = []
    changed = False
    for item in data:
        if not isinstance(item, dict):
            continue
        normalized_item = _normalize_account(item)
        normalized.append(normalized_item)
        if item.get("first_seen") != normalized_item.get("first_seen"):
            changed = True
        if int(item.get("profile_edit_count", 0) or 0) != normalized_item.get(
            "profile_edit_count", 0
        ):
            changed = True
        original_types = item.get("profile_edit_types") if isinstance(item, dict) else []
        if isinstance(original_types, list):
            original_sorted = sorted({str(v).strip() for v in original_types if str(v).strip()})
        else:
            original_sorted = []
        if original_sorted != normalized_item.get("profile_edit_types", []):
            changed = True
    if changed:
        try:
            _save(normalized)
        except Exception:
            pass
    return normalized


def _save(items: List[Dict]) -> None:
    cleaned = [_prepare_for_save(_normalize_account(it)) for it in items]
    FILE.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_accounts_csv(path: Path) -> List[Dict[str, str]]:
    raw_text = path.read_text(encoding="utf-8-sig")
    if not raw_text.strip():
        return []

    buffer = io.StringIO(raw_text)
    reader = csv.DictReader(buffer)
    normalized_rows: List[Dict[str, str]] = []
    mapping: Dict[str, str] = {}

    if reader.fieldnames:
        lowered = {name.strip().lower(): name for name in reader.fieldnames if name}
        if all(header in lowered for header in _CSV_HEADERS):
            mapping = {header: lowered[header] for header in _CSV_HEADERS}

    if mapping:
        for row in reader:
            normalized = {
                header: (row.get(actual) or "").strip()
                for header, actual in mapping.items()
            }
            if not any(normalized.values()):
                continue
            normalized_rows.append(normalized)
        return normalized_rows

    buffer = io.StringIO(raw_text)
    plain_reader = csv.reader(buffer)
    for row_index, row in enumerate(plain_reader):
        if not row:
            continue
        candidate = [cell.strip().lower() for cell in row[: len(_CSV_HEADERS)]]
        if row_index == 0 and candidate == _CSV_HEADERS:
            continue
        normalized = {
            header: row[idx].strip() if idx < len(row) else ""
            for idx, header in enumerate(_CSV_HEADERS)
        }
        if not any(normalized.values()):
            continue
        normalized_rows.append(normalized)
    return normalized_rows


def _compose_proxy_url(identifier: str, port: str) -> str:
    base = identifier.strip()
    if not base:
        return ""
    if "://" not in base:
        base = f"http://{base}"
    if port:
        trimmed = base.rstrip("/")
        if trimmed.count(":") <= 1:
            base = f"{trimmed}:{port}"
        else:
            base = trimmed
    return base


def list_all() -> List[Dict]:
    return _load()


def _find(items: List[Dict], username: str) -> Optional[Dict]:
    username = username.lower()
    for it in items:
        if it.get("username", "").lower() == username:
            return it
    return None


def get_account(username: str) -> Optional[Dict]:
    items = _load()
    account = _find(items, username)
    return _normalize_account(account) if account else None


def update_account(username: str, updates: Dict) -> bool:
    items = _load()
    username_norm = username.lower()
    for idx, item in enumerate(items):
        if item.get("username", "").lower() == username_norm:
            updated = dict(item)
            updated.update(updates)
            items[idx] = _normalize_account(updated)
            _save(items)
            _invalidate_health(username)
            return True
    return False


def _prompt_totp(username: str) -> bool:
    while True:
        raw = ask("TOTP Secret / otpauth URI (opcional): ").strip()
        if not raw:
            return False
        try:
            save_totp_secret(username, raw)
            ok("Se guardÃ³ el TOTP cifrado para esta cuenta.")
            # Muestra el cÃ³digo actual para facilitar el primer login manual.
            current = generate_totp_code(username)
            if current:
                print(f"CÃ³digo TOTP actual (cambia cada 30s): {current}")
            return True
        except ValueError as exc:
            warn(f"No se pudo guardar el TOTP: {exc}")
            retry = ask("Â¿Reintentar ingreso de TOTP? (s/N): ").strip().lower()
            if retry != "s":
                return False


def _onboarding_backend_ready() -> bool:
    return bool(_ONBOARDING_AVAILABLE and login_and_persist and onboard_accounts_from_csv)


def _print_onboarding_backend_help() -> None:
    if _ONBOARDING_BACKEND_ERROR:
        print(f"[ERROR] Backend de onboarding no disponible: {_ONBOARDING_BACKEND_ERROR}")
    else:
        print("[ERROR] Backend de onboarding no cargado.")
    print("InstalÃ¡ dependencias: pip install playwright pyotp y luego playwright install")
    print("Verifica que existan los archivos src/__init__.py y src/auth/__init__.py")


def _playwright_proxy_payload(settings: Optional[Dict]) -> Optional[Dict[str, str]]:
    if not settings:
        return None
    payload = {
        "url": settings.get("proxy_url"),
        "username": settings.get("proxy_user"),
        "password": settings.get("proxy_pass"),
    }
    if not build_playwright_proxy:
        return None
    return build_playwright_proxy(payload)


def _playwright_account_payload(username: str, password: str, proxy_settings: Optional[Dict]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "username": username,
        "password": password,
    }
    proxy_payload = _playwright_proxy_payload(proxy_settings)
    if proxy_payload:
        payload["proxy"] = proxy_payload
    if has_totp_secret(username):
        payload["totp_callback"] = lambda _ignored, target=username: generate_totp_code(target)
    return payload


def _playwright_onboarding(username: str, password: str, proxy_settings: Optional[Dict]) -> Dict[str, str]:
    if not _onboarding_backend_ready():
        raise RuntimeError("Backend de onboarding no disponible.")
    payload = _playwright_account_payload(username, password, proxy_settings)
    return login_and_persist(payload, headless=False)


def _build_playwright_login_payload(
    username: str,
    password: str,
    proxy_settings: Optional[Dict],
    *,
    alias: str,
    totp_secret: Optional[str] = None,
    row_number: Optional[int] = None,
) -> Dict[str, Any]:
    payload = _playwright_account_payload(username, password, proxy_settings)
    payload["alias"] = alias
    payload["strict_login"] = True
    if totp_secret:
        payload["totp_secret"] = totp_secret
    if row_number is not None:
        payload["row_number"] = row_number
    return payload


def _payload_has_proxy(payload: Dict[str, Any]) -> bool:
    if payload.get("proxy"):
        return True
    proxy_url = str(payload.get("proxy_url") or payload.get("proxy") or "").strip()
    return bool(proxy_url)


def login_accounts_with_playwright(
    alias: str,
    accounts: List[Dict[str, Any]],
    *,
    concurrency: int = 1,
) -> List[Dict[str, Any]]:
    try:
        from src.auth.onboarding import login_account_playwright, write_onboarding_results
    except Exception as exc:
        warn(f"No se pudo iniciar login con Playwright: {exc}")
        return []

    try:
        proxy_concurrency = max(1, int(concurrency or 1))
    except Exception:
        proxy_concurrency = 1

    with_proxy = [acct for acct in accounts if _payload_has_proxy(acct)]
    without_proxy = [acct for acct in accounts if not _payload_has_proxy(acct)]

    results: List[Dict[str, Any]] = []

    # 1) Sin proxy: siempre 1 a 1 (headful) aunque el usuario pida concurrencia.
    for account in without_proxy:
        results.append(login_account_playwright(account, alias, headful=True))

    # 2) Con proxy: aplica concurrencia. Si concurrencia>=2 -> headless (segundo plano).
    if not with_proxy:
        pass
    elif proxy_concurrency < 2 or len(with_proxy) < 2:
        for account in with_proxy:
            results.append(login_account_playwright(account, alias, headful=True))
    else:
        with ThreadPoolExecutor(max_workers=min(proxy_concurrency, len(with_proxy))) as executor:
            future_to_account = {
                executor.submit(login_account_playwright, account, alias, headful=False): account
                for account in with_proxy
            }
            for future in as_completed(list(future_to_account.keys())):
                account = future_to_account.get(future, {})
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        {
                            "username": (account.get("username") or "").strip(),
                            "status": "failed",
                            "message": str(exc),
                            "profile_path": "",
                            "row_number": account.get("row_number"),
                        }
                    )

    # Normaliza orden si viene desde CSV (usa row_number si está).
    if results and any(item.get("row_number") is not None for item in results):
        results.sort(key=lambda item: item.get("row_number") or 0)

    # Actualiza estado/health en storage de cuentas (secuencial para evitar carreras).
    for result in results:
        username = (result.get("username") or "").strip()
        if not username:
            continue
        if result.get("status") == "ok":
            mark_connected(username, True)
            _store_health(username, "[âœ… OK]")
        else:
            badge = _badge_from_login_message(result.get("message", ""))
            if badge:
                _store_health(username, badge)

    try:
        write_onboarding_results(results)
    except Exception:
        pass
    return results


def relogin_accounts_with_playwright_background(
    alias: str,
    accounts: List[Dict[str, Any]],
    *,
    concurrency: int = 1,
) -> List[Dict[str, Any]]:
    """
    Fuerza un relogin (sin UI) con Playwright para las cuentas indicadas y
    persiste storage_state.json en profiles/<username>/storage_state.json.

    Nota: si no hay password guardada para una cuenta se solicitará por consola.
    """

    try:
        from src.auth.onboarding import login_account_playwright, write_onboarding_results
    except Exception as exc:
        warn(f"No se pudo iniciar relogin con Playwright: {exc}")
        return []

    def _password_for(account: Dict[str, Any]) -> str:
        username = (account.get("username") or "").strip().lstrip("@")
        password = _account_password(account).strip()
        if password:
            return password
        entered = _ask_secret(f"Password @{username} (Enter = omitir): ")
        entered = (entered or "").strip()
        if entered:
            _store_account_password(username, entered)
        return entered

    payloads: List[Dict[str, Any]] = []
    for acct in accounts:
        username = (acct.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        password = _password_for(acct)
        if not password:
            warn(f"@{username}: sin password, se omite relogin.")
            continue
        payload = _playwright_account_payload(username, password, acct)
        payload["alias"] = alias
        payload["strict_login"] = True
        payload["force_login"] = True
        payloads.append(payload)

    if not payloads:
        return []

    results: List[Dict[str, Any]] = []
    max_workers = 1
    try:
        max_workers = max(1, int(concurrency or 1))
    except Exception:
        max_workers = 1

    if max_workers < 2 or len(payloads) < 2:
        for payload in payloads:
            results.append(login_account_playwright(payload, alias, headful=False))
    else:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(payloads))) as executor:
            future_to_payload = {
                executor.submit(login_account_playwright, payload, alias, headful=False): payload
                for payload in payloads
            }
            for future in as_completed(list(future_to_payload.keys())):
                payload = future_to_payload.get(future, {})
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        {
                            "username": (payload.get("username") or "").strip(),
                            "status": "failed",
                            "message": str(exc),
                            "profile_path": "",
                            "row_number": payload.get("row_number"),
                        }
                    )

    for result in results:
        username = (result.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        if (result.get("status") or "").lower() == "ok":
            mark_connected(username, True)
            _store_health(username, "[âœ… OK]")
        else:
            mark_connected(username, False)
            badge = _badge_from_login_message(str(result.get("message") or ""))
            if badge:
                _store_health(username, badge)

    try:
        write_onboarding_results(results)
    except Exception:
        pass
    return results


@dataclass(frozen=True)
class _LoginTwoFactorPayload:
    code: str
    mode: str
    source: str


_TOTP_SECRET_KEYS = (
    "totp_secret",
    "totp seed",
    "totp_seed",
    "totp key",
    "totp_key",
    "totp uri",
    "totp_uri",
    "authenticator_secret",
    "authenticator",
    "2fa_secret",
    "two_factor_secret",
)


def _ingest_totp_secret_from_account(account: Dict) -> None:
    username = (account.get("username") or "").strip()
    if not username or has_totp_secret(username):
        return

    for key in _TOTP_SECRET_KEYS:
        raw = account.get(key)
        if not raw:
            continue
        candidate = str(raw).strip()
        if not candidate:
            continue
        try:
            save_totp_secret(username, candidate)
            logger.debug(
                "Se almacenÃ³ el secreto TOTP definido en '%s' para @%s durante el login.",
                key,
                username,
            )
        except ValueError as exc:
            logger.warning(
                "Se ignorÃ³ el secreto TOTP incluido en '%s' para @%s: %s",
                key,
                username,
                exc,
            )
        finally:
            break

    account["has_totp"] = has_totp_secret(username)


def _two_factor_payload_for_login(account: Dict) -> Optional[_LoginTwoFactorPayload]:
    username = (account.get("username") or "").strip()
    if username:
        _ingest_totp_secret_from_account(account)
        if has_totp_secret(username):
            code = generate_totp_code(username)
            if code:
                return _LoginTwoFactorPayload(code=code, mode="totp", source="totp_store")
            logger.warning(
                "No se pudo generar el cÃ³digo TOTP automÃ¡tico para @%s. RevisÃ¡ el secreto almacenado.",
                username,
            )

    for key in ("totp_code", "two_factor_code", "2fa_code"):
        value = str(account.get(key) or "").strip()
        if value:
            return _LoginTwoFactorPayload(code=value, mode=key, source="manual")

    return None


def _two_factor_mode_from_info(info: Dict[str, Any]) -> str:
    if not isinstance(info, dict):
        return "unknown"

    if info.get("totp_two_factor_on") or info.get("is_totp_two_factor_enabled"):
        return "totp"
    if info.get("whatsapp_two_factor_on") or info.get("should_use_whatsapp_token"):
        return "whatsapp"
    if info.get("sms_two_factor_on") or info.get("is_sms_two_factor_enabled"):
        return "sms"

    method = str(info.get("verification_method") or "").strip()
    if method == "3":
        return "totp"
    if method == "5":
        return "whatsapp"
    if method == "1":
        return "sms"
    return "unknown"


def add_account(username: str, alias: str, proxy: Optional[Dict] = None) -> bool:
    items = _load()
    if _find(items, username):
        warn("Ya existe.")
        return False
    record = {
        "username": username.strip().lstrip("@"),
        "alias": alias,
        "active": True,
        "connected": False,
    }
    if proxy:
        record.update(proxy)
    items.append(_normalize_account(record))
    _save(items)
    _invalidate_health(username)
    ok("Agregada.")
    return True


def remove_account(username: str) -> None:
    items = _load()
    new_items = [it for it in items if it.get("username", "").lower() != username.lower()]
    _save(new_items)
    remove_session(username)
    remove_totp_secret(username)
    clear_proxy(username)
    _invalidate_health(username)
    key = _password_key(username)
    if key and key in _PASSWORD_CACHE:
        _PASSWORD_CACHE.pop(key, None)
        _save_password_cache(_PASSWORD_CACHE)
    ok("Eliminada (si existÃ­a).")


def set_active(username: str, is_active: bool = True) -> None:
    if update_account(username, {"active": is_active}):
        _invalidate_health(username)
        ok("Actualizada.")
    else:
        warn("No existe.")


def mark_connected(username: str, connected: bool) -> None:
    update_account(username, {"connected": connected})
    _invalidate_health(username)


def _proxy_config_from_inputs(data: Dict) -> ProxyConfig:
    return ProxyConfig(
        url=data.get("proxy_url", ""),
        user=data.get("proxy_user") or None,
        password=data.get("proxy_pass") or None,
        sticky_minutes=int(data.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)),
    )


def _prompt_proxy_settings(existing: Optional[Dict] = None) -> Dict:
    defaults = default_proxy_settings()
    current = existing or {}
    print("\nConfiguraciÃ³n de proxy (opcional)")
    base_default = current.get("proxy_url") or defaults["url"]
    prompt_default = base_default or "sin proxy"
    raw_url = ask(f"Proxy URL [{prompt_default}]: ").strip()
    if raw_url.lower() in {"-", "none", "sin", "no"}:
        url = ""
    elif not raw_url and base_default:
        url = base_default
    else:
        url = raw_url

    user_default = current.get("proxy_user") or defaults["user"]
    user_prompt = user_default or "(sin definir)"
    proxy_user = ask(f"Usuario (opcional) [{user_prompt}]: ").strip() or user_default

    pass_default = current.get("proxy_pass") or defaults["password"]
    pass_prompt = "***" if pass_default else "(sin definir)"
    proxy_pass = ask(f"Password (opcional) [{pass_prompt}]: ").strip() or pass_default

    sticky_default = current.get("proxy_sticky_minutes") or defaults["sticky"]
    sticky_input = ask(f"Sticky minutes [{sticky_default}]: ").strip()
    try:
        sticky = int(sticky_input) if sticky_input else int(sticky_default)
    except Exception:
        sticky = int(defaults["sticky"] or 10)
    sticky = max(1, sticky)

    proxy_url = url.strip()
    data = {
        "proxy_url": proxy_url,
        "proxy_user": (proxy_user or "").strip(),
        "proxy_pass": (proxy_pass or "").strip(),
        "proxy_sticky_minutes": sticky,
    }

    if not proxy_url:
        return {"proxy_url": "", "proxy_user": "", "proxy_pass": "", "proxy_sticky_minutes": sticky}

    if ask("Â¿Probar proxy ahora? (s/N): ").strip().lower() == "s":
        try:
            result = test_proxy_connection(_proxy_config_from_inputs(data))
            ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
        except Exception as exc:
            warn(f"Proxy fallÃ³: {exc}")
            retry = ask("Â¿Reintentar configuraciÃ³n? (s/N): ").strip().lower()
            if retry == "s":
                return _prompt_proxy_settings(existing)
    return data


def _test_existing_proxy(account: Dict) -> None:
    if not account.get("proxy_url"):
        warn("La cuenta no tiene proxy configurado.")
        return
    try:
        result = test_proxy_connection(_proxy_config_from_inputs(account))
        ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
    except Exception as exc:
        warn(f"Error probando proxy: {exc}")


_IG_INBOX_URL = "https://www.instagram.com/direct/inbox/"


def _launch_inbox(alias: str) -> None:
    accounts = [acct for acct in _load() if acct.get("alias") == alias]
    active_accounts = [acct for acct in accounts if acct.get("active")]
    if not active_accounts:
        warn("No hay cuentas activas en este alias.")
        press_enter()
        return

    while True:
        banner()
        title("Inbox (Playwright)")
        print()
        print("Seleccioná 1 cuenta activa (número o username). Enter = volver.\n")
        for idx, acct in enumerate(active_accounts, start=1):
            sess = _session_label(acct["username"])
            proxy_flag = _proxy_indicator(acct)
            low_flag = _low_profile_indicator(acct)
            totp_flag = _totp_indicator(acct)
            print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{low_flag}{totp_flag}")
            if low_flag and acct.get("low_profile_reason"):
                print(f"    ↳ {acct['low_profile_reason']}")

        raw = ask("\nCuenta: ").strip()
        if not raw:
            return

        chosen: Optional[Dict] = None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(active_accounts):
                chosen = active_accounts[idx - 1]
        else:
            target = raw.lstrip("@").strip().lower()
            for acct in active_accounts:
                if str(acct.get("username") or "").strip().lower() == target:
                    chosen = acct
                    break

        if not chosen:
            warn("No se encontró la cuenta con esos datos.")
            press_enter()
            continue

        minutes_raw = ask("Tiempo en minutos (0 = hasta cerrar el navegador): ").strip() or "0"
        try:
            minutes = int(float(minutes_raw))
        except Exception:
            minutes = 0
        minutes = max(0, minutes)
        max_seconds = (minutes * 60) if minutes else None

        _open_playwright_manual_session(
            chosen,
            start_url=_IG_INBOX_URL,
            action_label="Entrar al inbox",
            max_seconds=max_seconds,
        )

        print("\n1) Abrir inbox de otra cuenta")
        print("2) Volver")
        again = ask("Opción: ").strip() or "2"
        if again != "1":
            return


def _launch_content_publisher(alias: str) -> None:
    try:
        from actions import content_publisher
    except Exception as exc:  # pragma: no cover - mÃ³dulo opcional
        warn(f"No se pudo iniciar el mÃ³dulo de publicaciones: {exc}")
        press_enter()
        return

    content_publisher.run_from_menu(alias)


def _launch_interactions(alias: str) -> None:
    try:
        from actions import interactions
    except Exception as exc:  # pragma: no cover - mÃ³dulo opcional
        warn(f"No se pudo iniciar el mÃ³dulo de interacciones: {exc}")
        press_enter()
        return

    interactions.run_from_menu(alias)


def _login_and_save_session(
    account: Dict, password: str, *, respect_backoff: bool = True
) -> bool:
    """Login con el cliente configurado y guarda sesiÃ³n en storage/sessions."""

    username = account["username"]
    if respect_backoff:
        remaining = _login_backoff_remaining(username)
        if remaining > 0:
            logger.debug(
                "Omitiendo login automÃ¡tico para @%s (reintentar en %.0fs)",
                username,
                remaining,
            )
            return False

    adapter = None
    try:
        adapter = get_instagram_client(account=account, engine="instagrapi")
    except Exception as exc:
        logger.debug(
            "Instagrapi no disponible para login de @%s: %s", username, exc
        )
        try:
            adapter = get_instagram_client(account=account)
        except Exception as exc2:
            logger.debug(
                "No se pudo inicializar el cliente de Instagram para @%s: %s",
                username,
                exc2,
            )
            return False

    try:
        load_into(adapter, username)
        logger.debug("Se cargÃ³ la sesiÃ³n previa para @%s", username)
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.debug("No se pudo cargar la sesiÃ³n previa de @%s: %s", username, exc)

    binding = None
    try:
        binding = apply_proxy_to_client(adapter, username, account, reason="login")
    except Exception as exc:
        if account.get("proxy_url"):
            record_proxy_failure(username, exc)
        logger.debug("No se pudo aplicar el proxy de @%s: %s", username, exc)

    payload = _two_factor_payload_for_login(account)
    jitter = random.uniform(1.5, 3.5)
    time.sleep(jitter)

    verification_code = payload.code if payload and payload.mode != "totp" else None
    if payload and payload.mode == "totp" and not verification_code:
        verification_code = generate_totp_code(username) or None
        if verification_code:
            logger.debug("Aplicando cÃ³digo TOTP automÃ¡tico para @%s", username)

    try:
        adapter.login(username, password, verification_code=verification_code)
        save_from(adapter, username)
        mark_connected(username, True)
        _clear_login_failure(username)
        account["has_totp"] = has_totp_secret(username)
        ok(f"SesiÃ³n guardada para {username}.")
        return True
    except TwoFARequired as exc:
        return _handle_two_factor_challenge(account, adapter, exc)
    except TwoFactorCodeRejected:
        warn(f"Instagram rechazÃ³ el cÃ³digo 2FA proporcionado para @{username}.")
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
            warn(f"Problema con el proxy de @{username}: {exc}")
        else:
            warn(f"No se pudo iniciar sesiÃ³n para {username}: {exc}")
        mark_connected(username, False)
        _record_login_failure(username)
        return False

    mark_connected(username, False)
    _record_login_failure(username)
    return False


def _handle_two_factor_challenge(
    account: Dict,
    adapter,
    exc: TwoFARequired,
) -> bool:
    username = account.get("username", "")
    method = exc.method or "unknown"
    methods = exc.methods or []

    if method not in {"sms", "whatsapp", "email"}:
        warn(
            "Instagram solicitÃ³ un desafÃ­o 2FA para @{username} que requiere intervenciÃ³n manual desde la app.".format(
                username=username
            )
        )
        mark_connected(username, False)
        _record_login_failure(username)
        return False

    if method == "sms" and "whatsapp" in methods:
        try:
            adapter.request_2fa_code("whatsapp")
            method = "whatsapp"
        except Exception as err:
            logger.warning(
                "No se pudo solicitar el cÃ³digo vÃ­a WhatsApp para @%s: %s",
                username,
                err,
            )
    logger.info(
        "Esperando cÃ³digo 2FA para @%s vÃ­a %s", username, method
    )

    attempts = 0
    cooldown = 8
    while attempts < 3:
        attempts += 1
        code = prompt_two_factor_code(username, method, attempts)
        if not code:
            logger.info(
                "No se ingresÃ³ cÃ³digo 2FA para @%s (intento %d)", username, attempts
            )
        else:
            try:
                adapter.finish_2fa(code)
                save_from(adapter, username)
                mark_connected(username, True)
                _clear_login_failure(username)
                account["has_totp"] = has_totp_secret(username)
                ok(f"SesiÃ³n guardada para {username} tras verificaciÃ³n 2FA.")
                return True
            except TwoFactorCodeRejected:
                warn(
                    f"Instagram rechazÃ³ el cÃ³digo ingresado para @{username}. IntentÃ¡ nuevamente."
                )
        if attempts < 3:
            logger.info(
                "Reintentando 2FA para @%s en %d segundos", username, cooldown
            )
            time.sleep(cooldown)
            cooldown = min(cooldown + 7, 30)
            with contextlib.suppress(Exception):
                adapter.resend_2fa_code(method)

    warn(
        f"No se pudo completar el login 2FA para @{username}. VolvÃ© a intentarlo mÃ¡s tarde."
    )
    mark_connected(username, False)
    _record_login_failure(username)
    return False


def _authorization_payload(client: Any) -> Dict[str, Any]:
    """Extract the authorization payload from a configured Instagram client."""

    candidates: list[dict[str, Any] | None] = []

    auth = getattr(client, "authorization_data", None)
    if isinstance(auth, dict):
        candidates.append(auth)

    try:
        settings = client.get_settings()
        if isinstance(settings, dict):
            candidates.append(settings.get("authorization_data"))
    except Exception:
        pass

    for payload in candidates:
        if isinstance(payload, dict):
            return payload

    return {}


def has_valid_session_settings(client: Any) -> bool:
    """Return True if the loaded client contains a usable session token."""

    payload = _authorization_payload(client)
    session_id = str(payload.get("sessionid") or payload.get("session_id") or "").strip()
    user_id = str(payload.get("user_id") or payload.get("ds_user_id") or "").strip()
    return bool(session_id and user_id)


def _session_active(
    username: str,
    *,
    account: Optional[Dict] = None,
    reason: str = "session-check",
    strict: bool = False,
) -> bool:
    if not username or not has_session(username):
        return False

    account = account or get_account(username)

    try:
        cl = get_instagram_client(account=account)
    except Exception as exc:
        logger.debug("No se pudo crear el cliente de Instagram: %s", exc)
        return False

    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason=reason)
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
        logger.debug("No se pudo aplicar el proxy de @%s: %s", username, exc)

    try:
        load_into(cl, username)
    except FileNotFoundError:
        mark_connected(username, False)
        return False
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        logger.debug("Error cargando sesiÃ³n para @%s: %s", username, exc)
        return False

    if has_valid_session_settings(cl):
        if not strict:
            mark_connected(username, True)
            return True

        # Validacion estricta: la sesion debe responder contra IG,
        # no solo existir como token/cookie local.
        try:
            info = cl.account_info()
            if info and getattr(info, "username", None):
                mark_connected(username, True)
                return True
        except Exception as exc:
            if binding and should_retry_proxy(exc):
                record_proxy_failure(username, exc)
            logger.debug("Sesion no valida para @%s en chequeo estricto: %s", username, exc)
            mark_connected(username, False)
            return False

        mark_connected(username, False)
        return False

    mark_connected(username, False)
    logger.debug("La sesiÃ³n cargada para @%s no contiene credenciales activas.", username)
    return False


def auto_login_with_saved_password(
    username: str, *, account: Optional[Dict] = None
) -> bool:
    """Intenta iniciar sesiÃ³n reutilizando la contraseÃ±a almacenada."""

    account = account or get_account(username)
    if not account:
        return False

    if _session_active(username, account=account, reason="auto-login-check"):
        return True

    stored_password = _account_password(account).strip()
    if not stored_password:
        return False

    return _login_and_save_session(account, stored_password)


def prompt_login(username: str, *, interactive: bool = True) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False

    if _session_active(username, account=account, reason="prompt-login"):
        return True
    stored_password = _account_password(account).strip()
    original_stored = stored_password
    attempted_auto = False

    if stored_password:
        attempted_auto = True
        if auto_login_with_saved_password(username, account=account):
            return True

    while True:
        if attempted_auto and stored_password:
            changed = (
                ask("Â¿Cambiaste la contraseÃ±a de esta cuenta? (s/N): ")
                .strip()
                .lower()
            )
            if changed != "s":
                warn(
                    "Instagram rechazÃ³ la sesiÃ³n guardada. Posiblemente haya un challenge o chequeo de seguridad pendiente."
                )
                return False
            password = _ask_secret(
                f"Nueva password @{account['username']}: "
            )
        else:
            password = _ask_secret(
                f"Password @{account['username']}: "
            )

        if not password:
            warn("Se cancelÃ³ el inicio de sesiÃ³n.")
            return False

        success = _login_and_save_session(
            account, password, respect_backoff=False
        )
        if success:
            if password != original_stored:
                _store_account_password(username, password)
            return True

        attempted_auto = False
        stored_password = ""
        if interactive and (
            ask("Â¿Intentar ingresar nuevamente? (s/N): ")
            .strip()
            .lower()
            == "s"
        ):
            continue
        return False


def _low_profile_indicator(account: Dict) -> str:
    return f" {em('ðŸŒ± bajo perfil')}" if account.get("low_profile") else ""


def _proxy_indicator(account: Dict) -> str:
    return f" {em('ðŸ›¡ï¸')}" if account.get("proxy_url") else ""


def _totp_indicator(account: Dict) -> str:
    return f" {em('ðŸ”')}" if account.get("has_totp") else ""


def _has_playwright_session(username: str) -> bool:
    if not username:
        return False
    try:
        path = Path(BASE_PROFILES) / username / "storage_state.json"
        return path.exists() and path.stat().st_size > 0
    except Exception:
        return False


def _playwright_cookie_session_active(username: str) -> bool:
    if not username:
        return False
    try:
        path = Path(BASE_PROFILES) / username / "storage_state.json"
        if not path.exists() or path.stat().st_size <= 0:
            return False
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False

    cookies = payload.get("cookies")
    if not isinstance(cookies, list):
        return False

    now = time.time()
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name") or "").strip().lower()
        if name != "sessionid":
            continue
        value = str(cookie.get("value") or "").strip()
        if not value:
            continue
        expires = cookie.get("expires")
        if expires in (None, "", -1, 0):
            return True
        try:
            if float(expires) > now:
                return True
        except Exception:
            return True
    return False


def _playwright_session_active(
    username: str,
    *,
    account: Optional[Dict] = None,
    strict: bool = False,
) -> bool:
    if not _has_playwright_session(username):
        return False

    if strict and _check_playwright_session:
        proxy_payload = None
        with contextlib.suppress(Exception):
            proxy_payload = _playwright_proxy_payload(account)
        try:
            ok, _reason = _check_playwright_session(
                username,
                proxy=proxy_payload,
                headless=True,
            )
            return bool(ok)
        except Exception as exc:
            logger.debug(
                "No se pudo validar la sesion de Playwright para @%s: %s",
                username,
                exc,
            )

    return _playwright_cookie_session_active(username)


def _session_label(username: str) -> str:
    if has_session(username) or _has_playwright_session(username):
        return "[sesiÃ³n]"
    return "[sin sesiÃ³n]"


def connected_status(
    account: Dict,
    *,
    strict: bool = False,
    reason: str = "connection-status",
    fast: bool = False,
    persist: bool = True,
) -> bool:
    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        return False

    has_api_session = has_session(username)
    has_playwright_file = _has_playwright_session(username)

    connected = False
    if fast and not strict:
        # Ruta ultra-rÃ¡pida para menÃºs: sÃ³lo seÃ±ales locales de sesiÃ³n.
        connected = bool(has_api_session or has_playwright_file)
    else:
        if has_api_session:
            connected = _session_active(username, account=account, reason=reason, strict=strict)
            account["connected"] = connected
        if not connected and has_playwright_file:
            connected = _playwright_session_active(
                username,
                account=account,
                strict=strict,
            )

    current = bool(account.get("connected"))
    if current != connected:
        if persist:
            mark_connected(username, connected)
        account["connected"] = connected

    return connected


def _invalidate_health(username: str) -> None:
    health_store.invalidate(username)


def _health_cached(username: str) -> tuple[str | None, bool]:
    return health_store.get_badge(username)


def _store_health(username: str, badge: str) -> str:
    return health_store.set_badge(username, badge)


def _badge_for_display(account: Dict) -> tuple[str, bool]:
    username = account.get("username", "")
    cached_badge, expired = _health_cached(username)
    if cached_badge:
        # No disparamos refresh "activo": la salud se actualiza implícitamente
        # durante operaciones reales con Playwright (login/inbox/responder/DMs/filtros).
        return cached_badge, False
    # Sin chequeos Playwright previos para esta cuenta.
    return "[SIN CHEQUEO]", False


def _life_status_badge(account: Dict, badge: str) -> str:
    lowered = (badge or "").lower()
    if any(
        keyword in lowered
        for keyword in (
            "desactivada",
            "disabled",
            "suspended",
            "baneada",
            "bloqueada",
        )
    ):
        return "[BLOQUEADA]"
    if "ok" in lowered:
        return "[VIVA]"
    if "sin chequeo" in lowered or "sin datos" in lowered:
        return "[DESCONOCIDA]"
    if any(
        keyword in lowered
        for keyword in (
            "checkpoint",
            "challenge",
            "captcha",
        )
    ):
        # Nuevo criterio: estos estados implican bloqueo real en UI (Playwright).
        return "[BLOQUEADA]"
    if any(keyword in lowered for keyword in ("action_block", "rate_limit", "en riesgo", "risk")):
        return "[EN RIESGO]"
    if "sesiÃ³n expirada" in lowered or "sesion expirada" in lowered:
        return "[SIN SESION]"
    if "verificando" in lowered:
        return "[VERIFICANDO]"
    if "unknown" in lowered or "desconoc" in lowered:
        return "[DESCONOCIDA]"
    return "[DESCONOCIDA]"


def _badge_from_login_message(message: str) -> str | None:
    lowered = (message or "").lower()
    if not lowered:
        return None
    if any(
        keyword in lowered
        for keyword in (
            "account_disabled",
            "desactivad",
            "disabled",
            "suspendid",
            "suspended",
            "banead",
        )
    ):
        return "[ðŸ”´ Desactivada]"
    if "checkpoint" in lowered:
        return "[ðŸŸ¡ En riesgo: checkpoint]"
    if "challenge" in lowered:
        return "[ðŸŸ¡ En riesgo: challenge]"
    if "action block" in lowered or "action_block" in lowered:
        return "[ðŸŸ¡ En riesgo: action_block]"
    return None


def _account_status_from_badge(account: Dict, badge: str) -> str:
    if not account.get("active"):
        return "inactiva"

    lowered = (badge or "").lower()
    if "desactivada" in lowered:
        return "baneada"
    if any(keyword in lowered for keyword in ("action_block", "challenge", "checkpoint")):
        return "bloqueada"
    if "sesiÃ³n expirada" in lowered or "sesion expirada" in lowered:
        return "no se puede iniciar sesiÃ³n"
    if not account.get("connected"):
        return "no se puede iniciar sesiÃ³n"
    return "activa"


def _proxy_status_from_badge(account: Dict, badge: str) -> str:
    lowered = (badge or "").lower()
    if "proxy" in lowered and any(term in lowered for term in ("caÃ­do", "caido", "bloqueado")):
        return "bloqueado"
    return "activo"


def _current_totp_code(username: str) -> str:
    if not username:
        return ""
    try:
        code = generate_totp_code(username)
    except Exception:
        return ""
    return code or ""


def _proxy_components(account: Dict) -> tuple[str, str, str, str]:
    raw_url = (account.get("proxy_url") or "").strip()
    ip = ""
    port = ""
    if raw_url:
        parsed = urlparse(raw_url if "://" in raw_url else f"http://{raw_url}")
        ip = (parsed.hostname or "").strip()
        port = str(parsed.port) if parsed.port else ""
    proxy_user = (account.get("proxy_user") or "").strip()
    proxy_pass = (account.get("proxy_pass") or "").strip()
    return ip, port, proxy_user, proxy_pass


def _alias_slug(alias: str) -> str:
    candidate = re.sub(r"[^0-9A-Za-z_-]", "_", alias.strip())
    candidate = candidate.strip("_")
    return candidate or "default"


def _export_paths(alias: str) -> tuple[Path, Path]:
    base_dir = Path.home() / "Desktop" / "archivos CSV"
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _alias_slug(alias)
    csv_path = base_dir / f"{slug}_accounts_{timestamp}.csv"
    totp_backup_path = base_dir / f"{slug}_totp_backup_{timestamp}.zip"
    return csv_path, totp_backup_path


def _totp_store_dir() -> Path:
    # Reusa el mismo BASE/runtime_base que totp_store.py para apuntar al mismo storage.
    return BASE / "storage" / "totp"


def _totp_record_path(username: str) -> Path:
    safe = re.sub(r"[^a-z0-9_-]", "_", (username or "").strip().lstrip("@").lower())
    return _totp_store_dir() / f"{safe}.json"


def _export_totp_backup_zip(usernames: List[str], destination: Path) -> int:
    """
    Exporta un backup cifrado de los secretos TOTP (NO en texto plano).
    Se copian los archivos JSON de storage/totp para los usernames indicados.
    Nota: no incluye `.master_key` por seguridad.
    """

    store_dir = _totp_store_dir()
    if not store_dir.exists():
        return 0

    normalized = []
    seen = set()
    for u in usernames:
        cleaned = (u or "").strip().lstrip("@")
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)

    if not normalized:
        return 0

    written = 0
    try:
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for username in normalized:
                path = _totp_record_path(username)
                if not path.exists():
                    continue
                arcname = str(Path("storage") / "totp" / path.name)
                zf.write(path, arcname=arcname)
                written += 1
    except Exception as exc:
        warn(f"No se pudo generar backup TOTP: {exc}")
        try:
            if destination.exists():
                destination.unlink()
        except Exception:
            pass
        return 0

    if written == 0:
        try:
            if destination.exists():
                destination.unlink()
        except Exception:
            pass

    return written


def _account_password(account: Dict) -> str:
    value = account.get("password")
    if isinstance(value, str) and value:
        return value
    key = _password_key(account.get("username"))
    if key:
        cached = _PASSWORD_CACHE.get(key)
        if cached:
            return cached
    return ""


def _store_account_password(username: str, password: str) -> None:
    if not password:
        return
    update_account(username, {"password": password})
    key = _password_key(username)
    if not key:
        return
    if _PASSWORD_CACHE.get(key) == password:
        return
    _PASSWORD_CACHE[key] = password
    _save_password_cache(_PASSWORD_CACHE)


def _export_accounts_csv(alias: str) -> None:
    accounts = [acct for acct in _load() if acct.get("alias") == alias]
    destination, totp_backup_path = _export_paths(alias)
    include_totp_secret = (
        ask("¿Incluir TOTP secret (texto plano) en el CSV? (s/N): ").strip().lower() == "s"
    )
    if include_totp_secret:
        warn(
            "ATENCIÓN: el TOTP secret permite generar códigos 2FA. Tratá este CSV como altamente sensible."
        )
    headers = [
        "Username",
        "ContraseÃ±a",
        "CÃ³digo 2FA",
        "Proxy IP",
        "Proxy Puerto",
        "Proxy Usuario",
        "Proxy ContraseÃ±a",
        "Estado de la cuenta",
        "Estado del proxy",
    ]
    if include_totp_secret:
        headers.append("TOTP Secret")

    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for account in accounts:
            username = (account.get("username") or "").strip()
            badge, _ = _badge_for_display(account)
            account_status = _account_status_from_badge(account, badge)
            proxy_status = _proxy_status_from_badge(account, badge)
            proxy_ip, proxy_port, proxy_user, proxy_pass = _proxy_components(account)
            row = [
                username,
                _account_password(account),
                _current_totp_code(username),
                proxy_ip,
                proxy_port,
                proxy_user,
                proxy_pass,
                account_status,
                proxy_status,
            ]
            if include_totp_secret:
                try:
                    row.append(get_totp_secret(username) or "")
                except Exception:
                    row.append("")
            writer.writerow(row)

    ok(f"Archivo CSV generado en: {destination}")
    totp_written = _export_totp_backup_zip(
        [(acct.get("username") or "").strip() for acct in accounts],
        totp_backup_path,
    )
    if totp_written:
        ok(
            f"Backup TOTP cifrado generado en: {totp_backup_path} "
            f"(registros incluidos: {totp_written})."
        )
        print("Nota: el backup NO incluye la llave `.master_key` por seguridad.")
    press_enter()


def _prompt_destination_alias(current_alias: str) -> Optional[str]:
    items = _load()
    aliases = sorted({(it.get("alias") or "default") for it in items} | {"default"})
    alias_lookup = {alias.lower(): alias for alias in aliases}
    normalized_current = current_alias.lower()
    if normalized_current not in alias_lookup:
        alias_lookup[normalized_current] = current_alias
        aliases.append(current_alias)

    if aliases:
        print("\nAlias disponibles para mover: " + ", ".join(sorted(set(aliases))))

    while True:
        destination = ask("Alias destino (Enter para cancelar): ").strip()
        if not destination:
            return None

        normalized = destination.lower()
        if normalized == normalized_current:
            warn("El alias destino es el mismo que el origen. SeleccionÃ¡ otro alias.")
            continue

        if normalized in alias_lookup:
            return alias_lookup[normalized]

        create = (
            ask(
                f"El alias '{destination}' no existe. Â¿Crear automÃ¡ticamente y continuar? (s/N): "
            )
            .strip()
            .lower()
        )
        if create == "s":
            ok(f"Alias '{destination}' creado.")
            return destination


def _move_accounts_to_alias(alias: str) -> None:
    usernames = _select_usernames_for_modifications(alias)
    if not usernames:
        return

    destination = _prompt_destination_alias(alias)
    if not destination:
        warn("OperaciÃ³n cancelada.")
        press_enter()
        return

    selected = {username.lower() for username in usernames if username}
    if not selected:
        warn("No se seleccionaron cuentas vÃ¡lidas.")
        press_enter()
        return

    items = _load()
    moved: set[str] = set()
    for idx, item in enumerate(items):
        username = (item.get("username") or "").strip()
        if not username:
            continue
        if item.get("alias") != alias:
            continue
        if username.lower() not in selected:
            continue
        updated = dict(item)
        updated["alias"] = destination
        items[idx] = _normalize_account(updated)
        moved.add(username)

    if not moved:
        warn("No se movieron cuentas.")
        press_enter()
        return

    _save(items)
    for username in moved:
        _invalidate_health(username)

    ok(f"Se movieron {len(moved)} cuenta(s) al alias '{destination}'.")
    press_enter()


def _import_accounts_from_csv(alias: str) -> None:
    path_input = ask("Ruta del archivo CSV: ").strip()
    if not path_input:
        warn("No se indicÃƒÂ³ la ruta del archivo.")
        press_enter()
        return

    try:
        from src.auth.onboarding import parse_accounts_csv
    except Exception as exc:
        warn(f"No se pudo iniciar el parser de CSV: {exc}")
        press_enter()
        return

    try:
        parsed_rows = parse_accounts_csv(path_input)
    except FileNotFoundError:
        warn("El archivo CSV indicado no existe o no es un archivo vÃƒÂ¡lido.")
        press_enter()
        return
    except Exception as exc:
        warn(f"No se pudo procesar el CSV: {exc}")
        press_enter()
        return

    if not parsed_rows:
        warn("El archivo CSV no contiene registros vÃƒÂ¡lidos.")
        press_enter()
        return

    proxy_rows = [row for row in parsed_rows if (row.get("proxy_url") or "").strip()]
    no_proxy_rows = [row for row in parsed_rows if not (row.get("proxy_url") or "").strip()]
    print(
        "\nDetectadas: {with_proxy} con proxy | {without_proxy} sin proxy".format(
            with_proxy=len(proxy_rows),
            without_proxy=len(no_proxy_rows),
        )
    )

    default_concurrency = max(1, min(int(SETTINGS.max_concurrency or 1), max(1, len(proxy_rows) or 1)))
    requested_concurrency = ask_int(
        f"Concurrencia para login con proxy? [{default_concurrency}]: ",
        min_value=1,
        default=default_concurrency,
    )
    if not proxy_rows and requested_concurrency > 1:
        warn("El CSV no trae proxys: el login se harÃ¡ 1 a 1 (concurrencia ignorada).")
    elif proxy_rows and requested_concurrency >= 2:
        ok(
            f"Logins con proxy en segundo plano (headless). Concurrencia={requested_concurrency}."
        )
    elif proxy_rows:
        ok("Logins con proxy en modo visible (concurrencia=1).")

    added = 0
    errors: List[tuple[Optional[int], str]] = []
    status_counter: Dict[str, int] = defaultdict(int)
    accounts_to_login: List[Dict[str, Any]] = []

    for row in parsed_rows:
        row_number = row.get("row_number")
        username = (row.get("username") or "").strip().lstrip("@")
        password = (row.get("password") or "").strip()
        totp_value = (row.get("totp_secret") or "").strip()

        if not username or not password:
            errors.append((row_number, "Datos incompletos: username/password."))
            continue

        sticky_value = row.get("proxy_sticky_minutes") or SETTINGS.proxy_sticky_minutes or 10
        try:
            sticky_minutes = max(1, int(sticky_value))
        except Exception:
            sticky_minutes = SETTINGS.proxy_sticky_minutes or 10

        proxy_data = {
            "proxy_url": row.get("proxy_url") or "",
            "proxy_user": row.get("proxy_user") or "",
            "proxy_pass": row.get("proxy_pass") or "",
            "proxy_sticky_minutes": sticky_minutes,
        }

        if not add_account(username, alias, proxy_data):
            errors.append((row_number, "No se pudo agregar la cuenta (posible duplicado)."))
            continue

        if totp_value:
            try:
                save_totp_secret(username, totp_value)
            except ValueError as exc:
                remove_account(username)
                errors.append((row_number, f"2FA invÃƒÂ¡lido: {exc}"))
                continue

        _store_account_password(username, password)
        payload = _build_playwright_login_payload(
            username,
            password,
            proxy_data,
            alias=alias,
            totp_secret=totp_value or None,
            row_number=row_number,
        )
        accounts_to_login.append(payload)
        added += 1

    if accounts_to_login:
        results = login_accounts_with_playwright(
            alias,
            accounts_to_login,
            concurrency=requested_concurrency,
        )
        for result in results:
            status = (result.get("status") or "failed").lower()
            status_counter[status] += 1

    total = len(parsed_rows)
    print("\nResumen de importaciÃƒÂ³n:")
    print(f"Total de filas procesadas: {total}")
    print(f"Cuentas agregadas al alias: {added}")
    print("Resultados de login guardados en data/onboarding_results.csv")
    print(
        "Estados de login -> ok: {ok} | need_code: {need} | failed: {failed}".format(
            ok=status_counter.get("ok", 0),
            need=status_counter.get("need_code", 0),
            failed=status_counter.get("failed", 0),
        )
    )
    if errors:
        warn("Detalle de errores durante el alta:")
        for row_number, message in errors:
            label = f"Fila {row_number}" if row_number else "Fila desconocida"
            print(f" - {label}: {message}")
    press_enter()


def _select_usernames_for_modifications(alias: str) -> List[str]:
    group = [acct for acct in _load() if acct.get("alias") == alias]
    if not group:
        warn("No hay cuentas disponibles en este alias.")
        press_enter()
        return []

    print("SeleccionÃ¡ cuentas por nÃºmero o username (coma separada, * para todas):")
    alias_map: Dict[str, str] = {}
    for idx, acct in enumerate(group, start=1):
        username = (acct.get("username") or "").strip()
        if not username:
            continue
        alias_map[username.lower()] = username
        sess = _session_label(username)
        proxy_flag = _proxy_indicator(acct)
        low_flag = _low_profile_indicator(acct)
        totp_flag = _totp_indicator(acct)
        print(f" {idx}) @{username} {sess} {proxy_flag}{low_flag}{totp_flag}")
        if low_flag and acct.get("low_profile_reason"):
            print(f"    â†³ {acct['low_profile_reason']}")

    raw = ask("SelecciÃ³n: ").strip()
    if not raw:
        warn("Sin selecciÃ³n.")
        press_enter()
        return []

    if raw == "*":
        return [acct.get("username") for acct in group if acct.get("username")]

    chosen: List[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        chunk = part.strip()
        if not chunk:
            continue
        if chunk.isdigit():
            idx = int(chunk)
            if 1 <= idx <= len(group):
                username = group[idx - 1].get("username")
                if username:
                    key = username.lower()
                    if key not in seen:
                        seen.add(key)
                        chosen.append(username)
        else:
            normalized = chunk.lstrip("@").lower()
            username = alias_map.get(normalized)
            if username and normalized not in seen:
                seen.add(normalized)
                chosen.append(username)

    if not chosen:
        warn("No se encontraron cuentas con esos datos.")
        press_enter()
        return []

    return chosen


def _resolve_accounts_for_modifications(
    alias: str, usernames: List[str]
) -> List[Optional[Dict]]:
    if not usernames:
        return []

    records = [acct for acct in _load() if acct.get("alias") == alias]
    mapping: Dict[str, Dict] = {}
    for acct in records:
        username = (acct.get("username") or "").strip()
        if username:
            mapping[username.lower()] = acct

    resolved: List[Optional[Dict]] = []
    missing: List[str] = []
    for username in usernames:
        key = (username or "").strip().lstrip("@").lower()
        acct = mapping.get(key)
        if acct:
            resolved.append(acct)
        else:
            resolved.append(None)
            missing.append(username)

    if missing:
        formatted = ", ".join(f"@{name}" for name in missing if name)
        if formatted:
            warn(f"No se encontraron estas cuentas: {formatted}")

    return resolved


def _ask_delay_seconds(default: float = 5.0) -> float:
    prompt = ask(f"Delay entre cuentas en segundos [{default:.0f}]: ").strip()
    if not prompt:
        return max(1.0, default)
    try:
        value = float(prompt.replace(",", "."))
    except ValueError:
        warn("Valor invÃ¡lido, se utilizarÃ¡ el delay por defecto.")
        return max(1.0, default)
    return max(1.0, value)


def _client_for_account_action(account: Dict, *, reason: str):
    username = (account.get("username") or "").strip()
    if not username:
        return None

    try:
        cl = get_instagram_client(account=account)
    except Exception as exc:
        warn(f"No se pudo crear el cliente de Instagram para @{username}: {exc}")
        return None

    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason=reason)
    except Exception as exc:
        logger.warning("No se pudo aplicar el proxy para @%s: %s", username, exc)
        binding = None

    try:
        load_into(cl, username)
    except FileNotFoundError:
        warn(
            f"No hay sesiÃ³n guardada para @{username}. IniciÃ¡ sesiÃ³n antes de modificar."
        )
        return None
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo cargar la sesiÃ³n de @{username}: {exc}")
        return None

    if not has_valid_session_settings(cl):
        mark_connected(username, False)
        warn(
            f"La sesiÃ³n guardada para @{username} no contiene credenciales activas. IniciÃ¡ sesiÃ³n nuevamente."
        )
        return None

    mark_connected(username, True)
    return cl


def _rename_password_cache(old_username: str, new_username: str) -> None:
    old_key = _password_key(old_username)
    new_key = _password_key(new_username)
    if not old_key or not new_key or old_key == new_key:
        return
    password = _PASSWORD_CACHE.pop(old_key, None)
    if not password:
        return
    if new_key not in _PASSWORD_CACHE:
        _PASSWORD_CACHE[new_key] = password
    _save_password_cache(_PASSWORD_CACHE)


def _rename_api_sessions(old_username: str, new_username: str) -> None:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not old_clean or not new_clean or old_clean.lower() == new_clean.lower():
        return

    try:
        import session_store
    except Exception:
        return

    candidates: List[Path] = []
    try:
        candidates = list(session_store.session_candidates(old_clean))
    except Exception:
        candidates = []

    for src in candidates:
        try:
            if not src.exists() or not src.is_file():
                continue
        except Exception:
            continue

        src_name = src.name.lower()
        dest_name = f"{new_clean}.json"
        if src_name.startswith("session_") and src_name.endswith(".json"):
            dest_name = f"session_{new_clean}.json"

        dest = src.with_name(dest_name)
        try:
            if dest.exists():
                continue
        except Exception:
            continue

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            src.replace(dest)
        except Exception as exc:  # pragma: no cover - operaciones de disco
            logger.warning(
                "No se pudo renombrar la sesion API %s -> %s: %s",
                src,
                dest,
                exc,
            )

        # Limpia lock viejo si existe
        lock_path = src.with_suffix(src.suffix + ".lock")
        with contextlib.suppress(Exception):
            if lock_path.exists():
                lock_path.unlink()


def _rename_playwright_profile(old_username: str, new_username: str) -> None:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not old_clean or not new_clean or old_clean.lower() == new_clean.lower():
        return

    try:
        base = Path(BASE_PROFILES)
    except Exception:
        return

    old_dir = base / old_clean
    new_dir = base / new_clean

    try:
        if not old_dir.exists() or not old_dir.is_dir():
            return
    except Exception:
        return

    try:
        if new_dir.exists():
            # Mejor esfuerzo: si existe el destino, al menos mover storage_state.json si falta.
            old_state = old_dir / "storage_state.json"
            new_state = new_dir / "storage_state.json"
            if old_state.exists():
                should_move = False
                if not new_state.exists():
                    should_move = True
                else:
                    with contextlib.suppress(Exception):
                        should_move = new_state.stat().st_size <= 0
                if should_move:
                    new_dir.mkdir(parents=True, exist_ok=True)
                    old_state.replace(new_state)
            return
    except Exception:
        return

    try:
        old_dir.replace(new_dir)
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo renombrar el perfil de Playwright %s -> %s: %s",
            old_dir,
            new_dir,
            exc,
        )


def _rename_account_record(old_username: str, new_username: str) -> str:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not new_clean:
        return old_clean

    # Evita pisar una cuenta distinta con el mismo username.
    existing = get_account(new_clean)
    if existing:
        existing_username = (existing.get("username") or "").strip().lstrip("@")
        if existing_username and existing_username.lower() != old_clean.lower():
            warn(f"Ya existe una cuenta con username @{existing_username}.")
            return old_clean

    items = _load()
    old_norm = old_clean.lower()
    changed = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored == old_norm:
            updated = dict(item)
            updated["username"] = new_clean
            items[idx] = _normalize_account(updated)
            changed = True
            break

    if changed:
        _save(items)

    if old_clean:
        _invalidate_health(old_clean)
    if new_clean:
        _invalidate_health(new_clean)

    try:
        rename_totp_secret(old_clean, new_clean)
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo trasladar el TOTP de @%s a @%s: %s", old_clean, new_clean, exc
        )

    try:
        _rename_password_cache(old_clean, new_clean)
    except Exception:
        pass
    try:
        _rename_api_sessions(old_clean, new_clean)
    except Exception:
        pass
    try:
        _rename_playwright_profile(old_clean, new_clean)
    except Exception:
        pass

    return new_clean


def _apply_username_change(account: Dict, desired_username: str, delay: float) -> Optional[str]:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-username")
    if not client:
        time.sleep(delay)
        return None

    desired_clean = desired_username.strip().lstrip("@")
    if not desired_clean:
        time.sleep(delay)
        return None

    actual_username = desired_clean
    try:
        result = client.account_edit(username=desired_clean)
        actual_username = getattr(result, "username", None) or desired_clean
        ok(f"@{username} â†’ @{actual_username}")
        try:
            save_from(client, actual_username)
        except Exception as exc:
            logger.warning(
                "No se pudo guardar la sesiÃ³n actualizada de @%s: %s",
                actual_username,
                exc,
            )
        normalized = _rename_account_record(username, actual_username)
        if username.strip().lower() != normalized.lower():
            remove_session(username)
        mark_connected(normalized, True)
        _record_profile_edit(normalized, "username")
        return normalized
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar el username de @{username}: {exc}")
        mark_connected(username, False)
        return None
    finally:
        time.sleep(delay)


_IG_EDIT_PROFILE_URL = "https://www.instagram.com/accounts/edit/"


def _ig_profile_url(username: str) -> str:
    handle = (username or "").strip().lstrip("@")
    return f"https://www.instagram.com/{handle}/" if handle else "https://www.instagram.com/"


def _fallback_playwright_proxy(account: Optional[Dict]) -> Optional[Dict[str, str]]:
    if not account:
        return None
    raw = str(account.get("proxy_url") or "").strip()
    if not raw:
        return None
    server = raw if "://" in raw else f"http://{raw}"
    payload: Dict[str, str] = {"server": server}
    user = str(account.get("proxy_user") or "").strip()
    password = str(account.get("proxy_pass") or "").strip()
    if user:
        payload["username"] = user
    if password:
        payload["password"] = password
    return payload


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("Se requiere contexto sync para usar Playwright en este menu.")


def _open_playwright_manual_session(
    account: Dict,
    *,
    start_url: str,
    action_label: str,
    max_seconds: Optional[int] = None,
) -> None:
    """
    Abre un navegador Playwright headful para que el usuario haga cambios MANUALES.
    Reutiliza storage_state.json si existe y lo re-guardara al cerrar la ventana.

    No hay timeout por defecto: el flujo vuelve recien cuando el usuario cierra el navegador.
    Si max_seconds > 0, se cerrará automáticamente al cumplirse ese tiempo.
    """

    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        warn("Cuenta invalida (sin username).")
        return

    proxy_payload = None
    with contextlib.suppress(Exception):
        proxy_payload = _playwright_proxy_payload(account)
    if not proxy_payload:
        proxy_payload = _fallback_playwright_proxy(account)

    async def _runner() -> None:
        try:
            from src.playwright_service import PlaywrightService, get_page, shutdown
        except Exception as exc:
            raise RuntimeError(
                f"No se pudo importar PlaywrightService: {exc}. Instala: pip install playwright y luego playwright install"
            ) from exc

        profile_root = Path(BASE_PROFILES)
        storage_state = profile_root / username / "storage_state.json"
        profile_dir = storage_state.parent

        svc = PlaywrightService(headless=False, base_profiles=profile_root)
        await svc.start(launch_proxy=proxy_payload)

        ctx = None
        try:
            try:
                ctx = await svc.new_context_for_account(
                    profile_dir=profile_dir,
                    storage_state=str(storage_state) if storage_state.exists() else None,
                    proxy=proxy_payload,
                )
            except Exception:
                # Si el storage_state esta corrupto, abrimos sin el para permitir login manual.
                ctx = await svc.new_context_for_account(
                    profile_dir=profile_dir,
                    storage_state=None,
                    proxy=proxy_payload,
                )

            page = await get_page(ctx)
            if start_url:
                try:
                    await page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
                except Exception:
                    with contextlib.suppress(Exception):
                        await page.goto(start_url)

            # Playwright-only health check (only when entering inbox/direct flows).
            do_health_check = "/direct/" in (start_url or "").lower()
            health_checker = None
            if do_health_check:
                try:
                    from src.health_playwright import detect_account_health_async as _detect_health

                    health_checker = _detect_health
                except Exception:
                    health_checker = None

            last_health_status: Optional[str] = None

            async def _probe_health() -> None:
                nonlocal last_health_status
                if not do_health_check or health_checker is None:
                    return
                try:
                    status, reason = await health_checker(page)
                except Exception:
                    return
                if status == last_health_status:
                    return
                last_health_status = status
                with contextlib.suppress(Exception):
                    health_store.update_from_playwright_status(username, status, reason=reason)

            print(f"\n[PLAYWRIGHT] @{username} -> {action_label}", flush=True)
            if max_seconds and max_seconds > 0:
                mins = max(1, int((max_seconds + 59) // 60))
                print(
                    f"Se cerrará automáticamente en ~{mins} min (o cerrá el navegador antes para continuar).",
                    flush=True,
                )
            else:
                print("Usá el navegador manualmente y cerralo para continuar.", flush=True)

            await _probe_health()

            next_health_probe = time.monotonic() + 5.0
            deadline = (time.monotonic() + float(max_seconds)) if (max_seconds and max_seconds > 0) else None
            while True:
                try:
                    pages = list(ctx.pages) if ctx else []
                except Exception:
                    pages = []
                if not pages:
                    break
                if do_health_check and last_health_status != "alive" and time.monotonic() >= next_health_probe:
                    await _probe_health()
                    next_health_probe = time.monotonic() + 5.0
                if deadline is not None and time.monotonic() >= deadline:
                    print("\n[PLAYWRIGHT] Tiempo cumplido. Cerrando navegador...", flush=True)
                    break
                await asyncio.sleep(0.5)

            with contextlib.suppress(Exception):
                await _probe_health()

            # Persiste la sesion (si el usuario inicio sesion o se refrescaron cookies).
            with contextlib.suppress(Exception):
                await svc.save_storage_state(ctx, storage_state)
        finally:
            with contextlib.suppress(Exception):
                await shutdown(svc, ctx)

    try:
        _run_async(_runner())
    except Exception as exc:
        warn(f"No se pudo abrir el navegador para @{username}: {exc}")


def _change_usernames_flow(alias: str, selected: List[str]) -> List[str]:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return selected

    total = 0
    successes = 0
    for idx, acct in enumerate(resolved):
        if not acct:
            continue
        total += 1
        current = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label="Cambiar username",
        )
        desired = ask(f"Nuevo username para @{current} (Enter = sin cambios): ").strip().lstrip("@")
        if not desired:
            continue

        existing = get_account(desired)
        if existing and (existing.get("username") or "").strip().lstrip("@").lower() != current.lower():
            warn(f"Ya existe una cuenta con @{desired}. Se omite este cambio.")
            continue

        updated = _rename_account_record(current, desired)
        normalized_current = current.strip().lstrip("@")
        normalized_updated = (updated or "").strip().lstrip("@")
        if not normalized_updated:
            continue

        # Solo cuenta como cambio si efectivamente quedo distinto (o cambio de mayusculas/minusculas).
        if (
            normalized_updated.lower() != normalized_current.lower()
            or normalized_updated != normalized_current
        ):
            successes += 1
            selected[idx] = normalized_updated
            _record_profile_edit(normalized_updated, "username")

    print(f"Usernames actualizados (manual): {successes}/{total}")
    press_enter()
    return selected


def _apply_full_name_change(account: Dict, full_name: str, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-full-name")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_edit(full_name=full_name)
        ok(f"Nombre actualizado para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "full_name")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar el nombre completo de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _change_full_name_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        username = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label="Cambiar full name",
        )
        new_value = ask(
            f"Nuevo full name para @{username} (Enter = sin cambios): "
        ).strip()
        if not new_value:
            continue
        successes += 1
        _record_profile_edit(username, "full_name")

    print(f"Nombres completos actualizados (manual): {successes}/{total}")
    press_enter()


def _apply_bio_change(account: Dict, biography: str, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-bio")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_set_biography(biography)
        action = "eliminada" if not biography else "actualizada"
        ok(f"Bio {action} para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "bio")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar la bio de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _change_bio_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        username = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label="Cambiar bio",
        )

        changed = (
            ask(f"Confirmas que cambiaste la bio de @{username}? (s/N): ").strip().lower()
        )
        if changed != "s":
            continue

        # No persistimos la bio localmente; solo registramos el cambio.
        _ = ask(
            f"Nueva bio para @{username} (Enter = dejar en blanco/eliminar): "
        )
        successes += 1
        _record_profile_edit(username, "bio")

    print(f"Bios actualizadas/eliminadas (manual): {successes}/{total}")
    press_enter()


def _apply_profile_picture(account: Dict, image_path: Path, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-profile-picture")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_change_picture(image_path)
        ok(f"Foto de perfil actualizada para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "profile_picture")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo cambiar la foto de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _apply_profile_picture_removal(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-profile-picture")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.private_request(
            "accounts/remove_profile_picture/", client.with_default_data({})
        )
        ok(f"Foto de perfil eliminada para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "profile_picture")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo eliminar la foto de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _profile_photo_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return
    total = 0
    changes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        username = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_IG_EDIT_PROFILE_URL,
            action_label="Cambiar/eliminar foto de perfil",
        )

        print("\nQue hiciste con la foto de perfil?")
        print("1) Subi/cambie la foto")
        print("2) Elimine la foto")
        print("3) No hice cambios")
        choice = ask("Opcion: ").strip() or "3"
        if choice in {"1", "2"}:
            changes += 1
            _record_profile_edit(username, "profile_picture")

    print(f"Fotos de perfil actualizadas/eliminadas (manual): {changes}/{total}")
    press_enter()


def _apply_highlight_cleanup(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-highlights")
    if not client:
        time.sleep(delay)
        return False

    try:
        user_id = client.user_id or client.user_id_from_username(username)
        highlights = client.user_highlights(user_id)
        deleted = 0
        for item in highlights:
            try:
                if client.highlight_delete(item.id):
                    deleted += 1
            except Exception as exc:
                if should_retry_proxy(exc):
                    record_proxy_failure(username, exc)
                logger.warning(
                    "Error eliminando historia destacada de @%s: %s", username, exc
                )
        ok(f"Historias destacadas eliminadas para @{username}: {deleted}")
        mark_connected(username, True)
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudieron eliminar las destacadas de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _delete_highlights_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        username = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_ig_profile_url(username),
            action_label="Eliminar historias destacadas",
        )
        changed = (
            ask(
                f"Confirmas que eliminaste historias destacadas de @{username}? (s/N): "
            )
            .strip()
            .lower()
        )
        if changed == "s":
            successes += 1

    print(f"Cuentas con historias destacadas eliminadas (manual): {successes}/{total}")
    press_enter()


def _apply_posts_cleanup(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-posts")
    if not client:
        time.sleep(delay)
        return False

    try:
        user_id = client.user_id or client.user_id_from_username(username)
        medias = client.user_medias(user_id, amount=0)
        deleted = 0
        failures = 0
        for media in medias:
            try:
                if client.media_delete(media.id):
                    deleted += 1
                else:
                    failures += 1
            except Exception as exc:
                if should_retry_proxy(exc):
                    record_proxy_failure(username, exc)
                failures += 1
                logger.warning(
                    "Error eliminando publicaciÃ³n de @%s: %s", username, exc
                )
        ok(f"Publicaciones eliminadas para @{username}: {deleted}")
        if failures:
            warn(
                f"@{username}: {failures} publicaciones no pudieron eliminarse automÃ¡ticamente."
            )
        mark_connected(username, True)
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudieron eliminar las publicaciones de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _delete_posts_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas vÃ¡lidas seleccionadas.")
        press_enter()
        return
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        username = (acct.get("username") or "").strip().lstrip("@")
        _open_playwright_manual_session(
            acct,
            start_url=_ig_profile_url(username),
            action_label="Eliminar publicaciones",
        )
        changed = (
            ask(f"Confirmas que eliminaste publicaciones de @{username}? (s/N): ")
            .strip()
            .lower()
        )
        if changed == "s":
            successes += 1

    print(f"Cuentas con publicaciones eliminadas (manual): {successes}/{total}")
    press_enter()


def _modification_menu(alias: str) -> None:
    selected: List[str] = []
    while True:
        banner()
        title(f"ModificaciÃ³n de cuentas de Instagram - Alias: {alias}")
        if selected:
            print("Cuentas seleccionadas: " + ", ".join(f"@{name}" for name in selected))
        else:
            print("Cuentas seleccionadas: (ninguna)")

        print("\n1) Seleccionar cuentas a modificar")
        print("2) Cambiar usernames")
        print("3) Cambiar nombres completos (Full name)")
        print("4) Cambiar o eliminar biografÃ­a (bio)")
        print("5) Cambiar o eliminar foto de perfil")
        print("6) Eliminar historias destacadas")
        print("7) Eliminar publicaciones existentes")
        print("8) Volver\n")

        choice = ask("OpciÃ³n: ").strip() or "8"

        if choice == "1":
            selected = _select_usernames_for_modifications(alias)
        elif choice == "2":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            selected = _change_usernames_flow(alias, selected)
        elif choice == "3":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            _change_full_name_flow(alias, selected)
        elif choice == "4":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            _change_bio_flow(alias, selected)
        elif choice == "5":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            _profile_photo_flow(alias, selected)
        elif choice == "6":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            _delete_highlights_flow(alias, selected)
        elif choice == "7":
            if not selected:
                warn("SeleccionÃ¡ cuentas primero.")
                press_enter()
                continue
            _delete_posts_flow(alias, selected)
        elif choice == "8":
            break
        else:
            warn("OpciÃ³n invÃ¡lida.")
            press_enter()


def menu_accounts():
    while True:
        banner()
        print("1) Seleccionar alias o crear uno nuevo")
        print("2) Volver atrÃ¡s (ENTER para volver)\n")
        choice = ask("OpciÃ³n: ").strip()
        if not choice or choice == "2":
            return
        if choice != "1":
            continue
        items = _load()
        aliases = sorted(set([it.get("alias", "default") for it in items]) | {"default"})
        title("Alias disponibles: " + ", ".join(aliases))
        alias = ask("Alias / grupo (ej default, ventas, matias): ").strip() or "default"

        print(f"\nCuentas del alias: {alias}")
        group = [it for it in items if it.get("alias") == alias]
        if not group:
            print("(no hay cuentas aÃºn)")
        else:
            for it in group:
                flag = em("ðŸŸ¢") if it.get("active") else em("âšª")
                is_connected = connected_status(
                    it,
                    strict=False,
                    reason="menu-display-status",
                    fast=True,
                    persist=False,
                )
                conn = "[conectada]" if is_connected else "[no conectada]"
                sess = _session_label(it["username"])
                proxy_flag = _proxy_indicator(it)
                totp_flag = _totp_indicator(it)
                badge, _needs_refresh = _badge_for_display(it)
                life_badge = _life_status_badge(it, badge)
                print(
                    f" - @{it['username']} {conn} {sess} {flag} {proxy_flag}{totp_flag} â€¢ {life_badge}"
                )

        print("\n1) Agregar cuenta")
        print("2) Agregar cuentas mediante archivo CSV")
        print("3) Eliminar cuenta")
        print("4) Activar/Desactivar / Proxy")
        print("5) Iniciar sesiÃ³n y guardar sesiÃ³nid (auto en TODAS del alias)")
        print("6) Iniciar sesiÃ³n y guardar sesiÃ³n ID (seleccionar cuenta)")
        print("7) Entrar al inbox")
        print("8) Subir contenidos (Historias / Post / Reels)")
        print("9) Interacciones (Ver & Like Reels)")
        print("10) ModificaciÃ³n de cuentas de Instagram")
        print("11) Exportar cuentas a CSV")
        print("12) Mover cuentas a otro alias")
        print("13) Volver\n")

        op = ask("OpciÃ³n: ").strip()
        if op == "1":
            if not _onboarding_backend_ready():
                _print_onboarding_backend_help()
                fallback = (
                    ask("Agregar cuenta sin Playwright y usar login clasico? (s/N): ")
                    .strip()
                    .lower()
                )
                if fallback != "s":
                    press_enter()
                    continue
                u = ask("Username (sin @): ").strip().lstrip("@")
                if not u:
                    continue
                if get_account(u):
                    warn("Ya existe.")
                    press_enter()
                    continue
                proxy_data = _prompt_proxy_settings()
                totp_saved = _prompt_totp(u)
                if add_account(u, alias, proxy_data):
                    if not totp_saved:
                        remove_totp_secret(u)
                    ok("Cuenta agregada. Iniciando login clasico...")
                    prompt_login(u, interactive=True)
                else:
                    if totp_saved:
                        remove_totp_secret(u)
                press_enter()
                continue
            u = ask("Username (sin @): ").strip().lstrip("@")
            if not u:
                continue
            if get_account(u):
                warn("Ya existe.")
                press_enter()
                continue
            proxy_data = _prompt_proxy_settings()
            totp_saved = _prompt_totp(u)
            if add_account(u, alias, proxy_data):
                if not totp_saved:
                    remove_totp_secret(u)
                password = _ask_secret(f"Password @{u}: ")
                if not password:
                    warn("No se ingresÃ³ password; la cuenta quedÃ³ sin sesiÃ³n.")
                else:
                    payload = _build_playwright_login_payload(
                        u,
                        password,
                        proxy_data,
                        alias=alias,
                    )
                    results = login_accounts_with_playwright(alias, [payload])
                    if results and results[0].get("status") == "ok":
                        _store_account_password(u, password)

            else:
                if totp_saved:
                    remove_totp_secret(u)
            press_enter()
        elif op == "2":
            if not _onboarding_backend_ready():
                _print_onboarding_backend_help()
                press_enter()
                continue
            _import_accounts_from_csv(alias)
        elif op == "3":
            if not group:
                warn("No hay cuentas para eliminar en este alias.")
                press_enter()
                continue
            print("\nÂ¿QuerÃ©s eliminar una cuenta, varias o todas las del alias?")
            print("1) Una")
            print("2) Varias (selecciÃ³n mÃºltiple)")
            print("3) Todas las del alias")
            mode = ask("OpciÃ³n: ").strip() or "1"
            if mode == "1":
                u = ask("Username a eliminar: ").strip().lstrip("@")
                if not u:
                    warn("No se ingresÃ³ username.")
                else:
                    remove_account(u)
                press_enter()
            elif mode == "2":
                print("SeleccionÃ¡ cuentas por nÃºmero o username (coma separada):")
                for idx, acct in enumerate(group, start=1):
                    low_flag = _low_profile_indicator(acct)
                    label = f" {idx}) @{acct['username']}"
                    if low_flag:
                        label += f" {low_flag}"
                    print(label)
                    if low_flag and acct.get("low_profile_reason"):
                        print(f"    â†³ {acct['low_profile_reason']}")
                raw = ask("SelecciÃ³n: ").strip()
                if not raw:
                    warn("Sin selecciÃ³n.")
                    press_enter()
                    continue
                chosen = set()
                for part in raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(group):
                            chosen.add(group[idx - 1]["username"])
                    else:
                        chosen.add(part.lstrip("@"))
                if not chosen:
                    warn("No se encontraron cuentas con esos datos.")
                    press_enter()
                    continue
                for acct in group:
                    if acct["username"] in chosen:
                        remove_account(acct["username"])
                press_enter()
            elif mode == "3":
                confirm = ask(
                    "Â¿ConfirmÃ¡s eliminar TODAS las cuentas de este alias? (s/N): "
                ).strip().lower()
                if confirm == "s":
                    for acct in group:
                        remove_account(acct["username"])
                else:
                    warn("OperaciÃ³n cancelada.")
                press_enter()
            else:
                warn("OpciÃ³n invÃ¡lida.")
                press_enter()
        elif op == "4":
            u = ask("Username: ").strip().lstrip("@")
            account = get_account(u)
            if not account:
                warn("No existe la cuenta.")
                press_enter()
                continue
            print("\n1) Activar/Desactivar")
            print("2) Editar proxy")
            print("3) Probar proxy")
            print("4) Configurar/Reemplazar TOTP")
            print("5) Eliminar TOTP")
            print("6) Volver")
            choice = ask("OpciÃ³n: ").strip() or "6"
            if choice == "1":
                val = ask("1=activar, 0=desactivar: ").strip()
                set_active(u, val == "1")
                press_enter()
            elif choice == "2":
                updates = _prompt_proxy_settings(account)
                update_account(u, updates)
                record_proxy_failure(u)
                ok("Proxy actualizado.")
                press_enter()
            elif choice == "3":
                _test_existing_proxy(account)
                press_enter()
            elif choice == "4":
                configured = _prompt_totp(u)
                if not configured:
                    warn("No se configurÃ³ TOTP.")
                press_enter()
                account = get_account(u) or account
            elif choice == "5":
                if has_totp_secret(u):
                    remove_totp_secret(u)
                    ok("Se eliminÃ³ el TOTP almacenado.")
                else:
                    warn("La cuenta no tenÃ­a TOTP guardado.")
                press_enter()
                account = get_account(u) or account
            else:
                continue
        elif op == "5":
            group = [x for x in _load() if x.get("alias") == alias]
            if not group:
                warn("No hay cuentas para iniciar sesiÃ³n.")
                press_enter()
                continue
            print("Relogin automÃ¡tico con Playwright (segundo plano) para TODAS las cuentas del alias.")
            print("Se guardarÃ¡ la sesiÃ³n en profiles/<username>/storage_state.json.")
            results = relogin_accounts_with_playwright_background(alias, group, concurrency=1)
            ok_count = sum(1 for r in results if str(r.get("status") or "").lower() == "ok")
            fail_count = sum(1 for r in results if str(r.get("status") or "").lower() != "ok")
            skipped = max(0, len(group) - len(results))
            print(f"Relogin Playwright: ok={ok_count} failed={fail_count} omitidas={skipped}")
            press_enter()
        elif op == "6":
            group = [x for x in _load() if x.get("alias") == alias]
            if not group:
                warn("No hay cuentas para iniciar sesiÃ³n.")
                press_enter()
                continue
            print("SeleccionÃ¡ cuentas por nÃºmero o username (coma separada, * para todas):")
            for idx, acct in enumerate(group, start=1):
                sess = _session_label(acct["username"])

                proxy_flag = _proxy_indicator(acct)
                low_flag = _low_profile_indicator(acct)
                totp_flag = _totp_indicator(acct)
                print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{low_flag}{totp_flag}")
                if low_flag and acct.get("low_profile_reason"):
                    print(f"    â†³ {acct['low_profile_reason']}")
            raw = ask("SelecciÃ³n: ").strip()
            if not raw:
                warn("Sin selecciÃ³n.")
                press_enter()
                continue
            targets: List[Dict] = []
            if raw == "*":
                targets = group
            else:
                chosen = set()
                for part in raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(group):
                            chosen.add(group[idx - 1]["username"])
                    else:
                        chosen.add(part.lstrip("@"))
                targets = [acct for acct in group if acct["username"] in chosen]
            if not targets:
                warn("No se encontraron cuentas con esos datos.")
                press_enter()
                continue
            print("Relogin automÃ¡tico con Playwright (segundo plano) para cuentas seleccionadas.")
            print("Se guardarÃ¡ la sesiÃ³n en profiles/<username>/storage_state.json.")
            results = relogin_accounts_with_playwright_background(alias, targets, concurrency=1)
            ok_count = sum(1 for r in results if str(r.get("status") or "").lower() == "ok")
            fail_count = sum(1 for r in results if str(r.get("status") or "").lower() != "ok")
            skipped = max(0, len(targets) - len(results))
            print(f"Relogin Playwright: ok={ok_count} failed={fail_count} omitidas={skipped}")
            press_enter()
        elif op == "7":
            _launch_inbox(alias)
        elif op == "8":
            _launch_content_publisher(alias)
        elif op == "9":
            _launch_interactions(alias)
        elif op == "10":
            _modification_menu(alias)
        elif op == "11":
            _export_accounts_csv(alias)
        elif op == "12":
            _move_accounts_to_alias(alias)
        elif op == "13":
            break
        else:
            warn("OpciÃ³n invÃ¡lida.")
            press_enter()


# Mantener compatibilidad con importaciÃ³n dinÃ¡mica
mark_connected.__doc__ = "Actualiza el flag de conexiÃ³n en almacenamiento"

