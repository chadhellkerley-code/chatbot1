from __future__ import annotations

import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyotp
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from core.storage_atomic import atomic_write_text
from paths import runtime_base, storage_root

logger = logging.getLogger(__name__)

_BASE = runtime_base(Path(__file__).resolve().parent.parent)
_BASE.mkdir(parents=True, exist_ok=True)
_STORE = storage_root(_BASE) / "totp"
_STORE.mkdir(parents=True, exist_ok=True)
_MASTER_FILE = _STORE / ".master_key"
_LEGACY_STORE = _BASE / "data" / "totp"

_ITERATIONS = 390_000
_SALT_BYTES = 16


@dataclass(frozen=True)
class SecretRecord:
    salt: bytes
    ciphertext: bytes


def normalize_username(username: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "_", (username or "").strip().lstrip("@").lower())


def _passphrase_candidates(master_file: Path) -> list[bytes]:
    candidates: list[bytes] = []
    env_value = os.environ.get("TOTP_MASTER_KEY")
    if env_value:
        candidates.append(env_value.encode("utf-8"))
    if master_file.exists():
        file_value = master_file.read_text(encoding="utf-8").strip().encode("utf-8")
        if file_value and file_value not in candidates:
            candidates.append(file_value)
    return candidates


def _persist_master_passphrase(passphrase: bytes, master_file: Path) -> None:
    if master_file.exists():
        return
    atomic_write_text(master_file, passphrase.decode("utf-8"))
    try:
        os.chmod(master_file, 0o600)
    except OSError:
        pass


def _passphrase() -> bytes:
    if _MASTER_FILE.exists():
        file_value = _MASTER_FILE.read_text(encoding="utf-8").strip().encode("utf-8")
        if file_value:
            return file_value

    env_value = os.environ.get("TOTP_MASTER_KEY")
    if env_value:
        passphrase = env_value.encode("utf-8")
        _persist_master_passphrase(passphrase, _MASTER_FILE)
        return passphrase

    random_key = base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")
    _persist_master_passphrase(random_key.encode("utf-8"), _MASTER_FILE)
    logger.info(
        "Se generÃ³ una passphrase local para cifrar secretos TOTP en %s.",
        _MASTER_FILE,
    )
    return random_key.encode("utf-8")


def _derive_key(salt: bytes, passphrase: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase))


def _fernet(salt: bytes, passphrase: bytes) -> Fernet:
    return Fernet(_derive_key(salt, passphrase))


def _path_for(username: str) -> Path:
    return _STORE / f"{normalize_username(username)}.json"


def _encode(record: SecretRecord) -> str:
    payload = {
        "salt": base64.urlsafe_b64encode(record.salt).decode("utf-8"),
        "ciphertext": base64.urlsafe_b64encode(record.ciphertext).decode("utf-8"),
    }
    return json.dumps(payload)


def _decode(raw: str) -> SecretRecord:
    data = json.loads(raw)
    salt = base64.urlsafe_b64decode(data["salt"])
    ciphertext = base64.urlsafe_b64decode(data["ciphertext"])
    return SecretRecord(salt=salt, ciphertext=ciphertext)


def _normalize_secret(raw: str) -> str:
    candidate = raw.strip()
    if not candidate:
        raise ValueError("Secreto vacÃ­o.")
    if candidate.lower().startswith("otpauth://"):
        try:
            parsed = pyotp.parse_uri(candidate)
            candidate = parsed.secret
        except Exception as exc:
            raise ValueError("URI otpauth invÃ¡lida.") from exc
    candidate = candidate.replace(" ", "")
    try:
        secret = pyotp.TOTP(candidate).secret
    except Exception as exc:
        raise ValueError("Secreto TOTP invÃ¡lido.") from exc
    return secret


def save_secret(username: str, raw_secret: str) -> None:
    secret = _normalize_secret(raw_secret)
    salt = os.urandom(_SALT_BYTES)
    token = _fernet(salt, _passphrase()).encrypt(secret.encode("utf-8"))
    record = SecretRecord(salt=salt, ciphertext=token)
    path = _path_for(username)
    atomic_write_text(path, _encode(record))
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    logger.debug("Se almacenÃ³ el secreto TOTP cifrado para @%s", username)


def remove_secret(username: str) -> None:
    path = _path_for(username)
    if path.exists():
        path.unlink()
        logger.debug("Se eliminÃ³ el secreto TOTP de @%s", username)


def rename_secret(old_username: str, new_username: str) -> None:
    old_path = _path_for(old_username)
    new_path = _path_for(new_username)
    if not old_path.exists():
        return

    old_normalized = normalize_username(old_username)
    new_normalized = normalize_username(new_username)
    if not new_normalized or old_normalized == new_normalized:
        return

    try:
        new_path.parent.mkdir(parents=True, exist_ok=True)
        if new_path.exists():
            new_path.unlink()
        old_path.replace(new_path)
        try:
            os.chmod(new_path, 0o600)
        except OSError:
            pass
        logger.debug(
            "Se renombrÃ³ el secreto TOTP de @%s a @%s",
            old_username,
            new_username,
        )
    except Exception as exc:
        logger.warning(
            "No se pudo renombrar el TOTP de @%s a @%s: %s",
            old_username,
            new_username,
            exc,
        )


def has_secret(username: str) -> bool:
    return _path_for(username).exists()


def _decrypt_record(record: SecretRecord, master_file: Path) -> str:
    last_exc: Optional[Exception] = None
    for passphrase in _passphrase_candidates(master_file):
        try:
            decrypted = _fernet(record.salt, passphrase).decrypt(record.ciphertext)
            return decrypted.decode("utf-8")
        except Exception as exc:  # pragma: no cover - depends on current key state
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise ValueError(f"No hay passphrase TOTP disponible para {master_file}.")


def _load_secret_from_store(path: Path, master_file: Path) -> Optional[str]:
    if not path.exists():
        return None
    record = _decode(path.read_text(encoding="utf-8"))
    return _decrypt_record(record, master_file)


def _load_secret(username: str) -> Optional[str]:
    path = _path_for(username)
    try:
        return _load_secret_from_store(path, _MASTER_FILE)
    except Exception as exc:
        logger.error("No se pudo desencriptar el TOTP de @%s: %s", username, exc)
        return None


def generate_code(username: str) -> Optional[str]:
    secret = _load_secret(username)
    if not secret:
        return None
    try:
        return pyotp.TOTP(secret).now()
    except Exception as exc:
        logger.error("Error generando cÃ³digo TOTP para @%s: %s", username, exc)
        return None


def get_secret(username: str) -> Optional[str]:
    return _load_secret(username)


def migrate_legacy_store() -> dict[str, int]:
    summary = {
        "migrated": 0,
        "skipped_existing": 0,
        "skipped_invalid": 0,
    }

    try:
        if _LEGACY_STORE.resolve() == _STORE.resolve():
            return summary
    except Exception:
        pass

    if not _LEGACY_STORE.is_dir():
        return summary

    legacy_master = _LEGACY_STORE / ".master_key"
    for path in sorted(_LEGACY_STORE.glob("*.json")):
        username_key = normalize_username(path.stem)
        if not username_key:
            summary["skipped_invalid"] += 1
            logger.warning("TOTP legacy invÃƒÂ¡lido: nombre de archivo sin username ÃƒÂºtil en %s", path)
            continue

        destination = _path_for(username_key)
        if destination.exists():
            summary["skipped_existing"] += 1
            logger.info("TOTP legacy omitido para @%s: ya existe store canÃƒÂ³nico.", username_key)
            continue

        try:
            secret = _load_secret_from_store(path, legacy_master)
            if not secret:
                raise ValueError("secreto vacÃƒÂ­o")
            save_secret(username_key, secret)
        except Exception as exc:
            summary["skipped_invalid"] += 1
            logger.warning("No se pudo migrar TOTP legacy para @%s desde %s: %s", username_key, path, exc)
            continue

        summary["migrated"] += 1
        logger.info("TOTP legacy migrado para @%s desde %s hacia %s.", username_key, path, destination)

    return summary
