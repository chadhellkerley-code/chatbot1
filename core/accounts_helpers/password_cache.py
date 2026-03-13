from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock, RLock
from typing import Dict, Optional

from cryptography.fernet import Fernet, InvalidToken

_PASSWORD_FILE: Optional[Path] = None
_LOGIN_FAILURE_BACKOFF = timedelta(minutes=5)
_LOGIN_FAILURES: Dict[str, datetime] = {}
_LOGIN_FAILURE_LOCK = Lock()
_STORE_LOCK = RLock()
_FERNET_LOCK = RLock()
_FERNET_CACHE: dict[str, Fernet] = {}
_SECRET_PREFIX = "enc:v1:"

logger = logging.getLogger(__name__)


def configure(password_file: Path, *, login_failure_backoff: timedelta | None = None) -> None:
    global _PASSWORD_FILE
    global _LOGIN_FAILURE_BACKOFF
    _PASSWORD_FILE = Path(password_file)
    if login_failure_backoff is not None:
        _LOGIN_FAILURE_BACKOFF = login_failure_backoff


def _password_key(username: str | None) -> str:
    if not username:
        return ""
    return username.strip().lstrip("@").lower()


def _credentials_db_path() -> Optional[Path]:
    if _PASSWORD_FILE is None:
        return None
    return _PASSWORD_FILE.with_name("credentials.sqlite3")


def _credentials_key_path() -> Optional[Path]:
    if _PASSWORD_FILE is None:
        return None
    return _PASSWORD_FILE.with_name(".credentials_key")


def _normalize_password_cache(raw: object) -> Dict[str, str]:
    cache: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return cache
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        normalized = key.strip().lower()
        if not normalized or not value:
            continue
        cache[normalized] = value
    return cache


def _load_legacy_password_file() -> Dict[str, str]:
    path = _PASSWORD_FILE
    if path is None or not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return _normalize_password_cache(raw)


def _clear_legacy_password_file() -> None:
    path = _PASSWORD_FILE
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    except Exception:
        pass


def _coerce_fernet_key(raw_key: str) -> bytes:
    candidate = raw_key.strip().encode("utf-8")
    try:
        Fernet(candidate)
    except Exception:
        return base64.urlsafe_b64encode(hashlib.sha256(candidate).digest())
    return candidate


def _fernet_instance() -> Optional[Fernet]:
    key_path = _credentials_key_path()
    if key_path is None:
        return None
    cache_key = str(key_path.resolve())
    with _FERNET_LOCK:
        cached = _FERNET_CACHE.get(cache_key)
        if cached is not None:
            return cached

        env_key = (
            str(os.environ.get("ACCOUNT_CREDENTIALS_KEY") or "").strip()
            or str(os.environ.get("SESSION_ENCRYPTION_KEY") or "").strip()
        )
        if env_key:
            raw_key = _coerce_fernet_key(env_key)
        else:
            key_path.parent.mkdir(parents=True, exist_ok=True)
            if key_path.exists():
                try:
                    raw_key = _coerce_fernet_key(key_path.read_text(encoding="utf-8"))
                except Exception:
                    logger.exception("No se pudo leer la clave de cifrado de credenciales.")
                    return None
            else:
                raw_key = Fernet.generate_key()
                try:
                    key_path.write_text(raw_key.decode("ascii"), encoding="utf-8")
                    with contextlib.suppress(Exception):
                        os.chmod(key_path, 0o600)
                except Exception:
                    return None
        cached = Fernet(raw_key)
        _FERNET_CACHE[cache_key] = cached
        return cached


def _encrypt_secret(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    fernet = _fernet_instance()
    if fernet is None:
        return ""
    token = fernet.encrypt(text.encode("utf-8")).decode("ascii")
    return f"{_SECRET_PREFIX}{token}"


def _decrypt_secret(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith(_SECRET_PREFIX):
        return text
    fernet = _fernet_instance()
    if fernet is None:
        return ""
    token = text[len(_SECRET_PREFIX) :]
    try:
        return fernet.decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        logger.error("No se pudo descifrar una credencial de cuenta; se devolvera vacio.")
        return ""


def _connect() -> Optional[sqlite3.Connection]:
    db_path = _credentials_db_path()
    if db_path is None:
        return None
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS account_credentials (
                username TEXT PRIMARY KEY,
                password_enc TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
        return connection
    except Exception:
        return None


def _upsert_password_locked(connection: sqlite3.Connection, username: str, password: str) -> None:
    normalized = _password_key(username)
    if not normalized or not password:
        return
    encrypted = _encrypt_secret(password)
    if not encrypted:
        return
    connection.execute(
        """
        INSERT INTO account_credentials(username, password_enc, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
            password_enc = excluded.password_enc,
            updated_at = excluded.updated_at
        """,
        (normalized, encrypted, datetime.utcnow().isoformat(timespec="seconds")),
    )


def _load_all_passwords_locked(connection: sqlite3.Connection) -> Dict[str, str]:
    cache: Dict[str, str] = {}
    for row in connection.execute(
        "SELECT username, password_enc FROM account_credentials"
    ).fetchall():
        username = _password_key(row["username"])
        password = _decrypt_secret(row["password_enc"])
        if username and password:
            cache[username] = password
    return cache


def _migrate_legacy_password_file_locked(connection: sqlite3.Connection) -> None:
    migrated = _load_legacy_password_file()
    if not migrated:
        return
    for username, password in migrated.items():
        _upsert_password_locked(connection, username, password)
    connection.commit()
    _clear_legacy_password_file()


def _load_password_cache() -> Dict[str, str]:
    with _STORE_LOCK:
        connection = _connect()
        if connection is None:
            return _load_legacy_password_file()
        try:
            _migrate_legacy_password_file_locked(connection)
            return _load_all_passwords_locked(connection)
        finally:
            connection.close()


def _save_password_cache(cache: Dict[str, str]) -> None:
    normalized = _normalize_password_cache(cache)
    with _STORE_LOCK:
        connection = _connect()
        if connection is None:
            logger.error("No se pudo abrir credentials.sqlite3; se conserva el backend legado sin borrar.")
            return
        try:
            _migrate_legacy_password_file_locked(connection)
            existing_rows = connection.execute(
                "SELECT username FROM account_credentials"
            ).fetchall()
            existing = {_password_key(row["username"]) for row in existing_rows}
            for username, password in normalized.items():
                _upsert_password_locked(connection, username, password)
            for username in existing - set(normalized.keys()):
                connection.execute(
                    "DELETE FROM account_credentials WHERE username = ?",
                    (username,),
                )
            connection.commit()
            _clear_legacy_password_file()
        finally:
            connection.close()


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
