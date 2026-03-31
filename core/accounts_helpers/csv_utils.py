from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Dict, List, Mapping, Optional

from core.totp_store import _normalize_secret, normalize_username as normalize_totp_username

_CSV_HEADERS = [
    "username",
    "password",
    "2fa code",
    "proxy id",
    "proxy port",
    "proxy username",
    "proxy password",
]

_TOTP_HEADER_ALIASES = (
    "totp_secret",
    "totp secret",
    "totp",
    "2fa",
    "2fa code",
    "otp",
    "authenticator",
    "secret",
)
_TOTP_HEADER_ALIAS_KEYS = {
    re.sub(r"[^a-z0-9]+", "", alias.strip().lower())
    for alias in _TOTP_HEADER_ALIASES
}


def _password_key(username: str | None) -> str:
    if not username:
        return ""
    return username.strip().lstrip("@").lower()


def _safe_username_key(username: str | None) -> str:
    return normalize_totp_username(username or "")


def _header_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _is_totp_header(value: str | None) -> bool:
    return _header_key(value) in _TOTP_HEADER_ALIAS_KEYS


def extract_totp_secret_from_row(row: Mapping[str, object]) -> str:
    for header, raw_value in row.items():
        if not _is_totp_header(header):
            continue
        candidate = str(raw_value or "").strip()
        if not candidate:
            continue
        try:
            return _normalize_secret(candidate)
        except ValueError:
            continue
    return ""


def _parse_accounts_csv(path: Path) -> List[Dict[str, str]]:
    raw_text = path.read_text(encoding="utf-8-sig")
    if not raw_text.strip():
        return []

    buffer = io.StringIO(raw_text)
    reader = csv.DictReader(buffer)
    normalized_rows: List[Dict[str, str]] = []
    mapping: Dict[str, str] = {}

    if reader.fieldnames:
        normalized_headers = {
            _header_key(name): name
            for name in reader.fieldnames
            if name and _header_key(name)
        }
        username_header = normalized_headers.get("username")
        password_header = normalized_headers.get("password")
        if username_header and password_header:
            mapping = {
                "username": username_header,
                "password": password_header,
            }
            for header in ("proxy id", "proxy port", "proxy username", "proxy password"):
                actual = normalized_headers.get(_header_key(header))
                if actual:
                    mapping[header] = actual
            for actual in reader.fieldnames:
                if _is_totp_header(actual):
                    mapping["2fa code"] = actual
                    break

    if mapping:
        for row in reader:
            normalized = {
                header: (row.get(mapping.get(header, "")) or "").strip()
                for header in _CSV_HEADERS
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


def _pick_csv_column(headers: List[str], aliases: tuple[str, ...]) -> Optional[str]:
    for header in headers:
        lowered = (header or "").strip().lower()
        if not lowered:
            continue
        if any(alias in lowered for alias in aliases):
            return header
    return None


def _extract_totp_entries_from_csv(path: Path) -> Dict[str, str]:
    entries: Dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = list(reader.fieldnames or [])
            if not headers:
                return entries

            user_col = _pick_csv_column(headers, ("username", "usuario", "user", "account"))
            if not user_col or not any(_is_totp_header(header) for header in headers):
                return entries

            for row in reader:
                raw_user = str(row.get(user_col) or "").strip()
                raw_secret = extract_totp_secret_from_row(row)
                if not raw_user or not raw_secret:
                    continue
                key = _password_key(raw_user)
                if key and key not in entries:
                    entries[key] = raw_secret
                safe_key = _safe_username_key(raw_user)
                if safe_key and safe_key not in entries:
                    entries[safe_key] = raw_secret
        if entries:
            return entries
        for row in _parse_accounts_csv(path):
            raw_user = str(row.get("username") or "").strip()
            raw_secret = extract_totp_secret_from_row(row)
            if not raw_user or not raw_secret:
                continue
            key = _password_key(raw_user)
            if key and key not in entries:
                entries[key] = raw_secret
            safe_key = _safe_username_key(raw_user)
            if safe_key and safe_key not in entries:
                entries[safe_key] = raw_secret
    except Exception:
        return entries
    return entries
