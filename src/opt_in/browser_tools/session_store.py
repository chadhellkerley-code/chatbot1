import json
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken

from . import audit
from .config import get_settings


class SessionStore:
    def __init__(self) -> None:
        settings = get_settings()
        self._directory: Path = settings.sessions_dir
        key = settings.session_encryption_key
        self._fernet: Optional[Fernet]
        if key:
            try:
                self._fernet = Fernet(key.encode("utf-8"))
            except Exception:
                self._fernet = None
                audit.log_event(
                    "session_store.invalid_key",
                    account=None,
                    details={"hint": "SESSION_ENCRYPTION_KEY is invalid for Fernet"},
                )
        else:
            self._fernet = None

    def _path_for(self, account: str) -> Path:
        safe_account = account.replace("/", "_")
        return self._directory / f"{safe_account}.json"

    def load(self, account: str) -> Optional[Dict[str, Any]]:
        path = self._path_for(account)
        if not path.exists():
            return None
        try:
            raw = path.read_bytes()
            if self._fernet:
                raw = self._fernet.decrypt(raw)
            data = json.loads(raw.decode("utf-8"))
            audit.log_event("session_store.loaded", account=account, details={"path": str(path)})
            return data
        except (InvalidToken, json.JSONDecodeError) as error:
            audit.log_error("session_store.load_failed", account, error)
            return None

    def save(self, account: str, storage_state: Dict[str, Any]) -> None:
        path = self._path_for(account)
        try:
            text = json.dumps(storage_state)
            raw = text.encode("utf-8")
            if self._fernet:
                raw = self._fernet.encrypt(raw)
            path.write_bytes(raw)
            audit.log_event("session_store.saved", account=account, details={"path": str(path)})
        except Exception as error:
            audit.log_error("session_store.save_failed", account, error)
