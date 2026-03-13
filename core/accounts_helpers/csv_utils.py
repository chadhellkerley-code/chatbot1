from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Dict, List, Optional

_CSV_HEADERS = [
    "username",
    "password",
    "2fa code",
    "proxy id",
    "proxy port",
    "proxy username",
    "proxy password",
]


def _password_key(username: str | None) -> str:
    if not username:
        return ""
    return username.strip().lstrip("@").lower()


def _safe_username_key(username: str | None) -> str:
    normalized = _password_key(username)
    if not normalized:
        return ""
    return re.sub(r"[^a-z0-9_-]", "_", normalized)


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
            totp_col = _pick_csv_column(headers, ("totp", "authenticator", "secret"))
            if not user_col or not totp_col:
                return entries

            for row in reader:
                raw_user = str(row.get(user_col) or "").strip()
                raw_secret = str(row.get(totp_col) or "").strip()
                if not raw_user or not raw_secret:
                    continue
                # Evita tomar columnas de códigos 2FA de 6 dígitos.
                if raw_secret.isdigit() and len(raw_secret) <= 8:
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

