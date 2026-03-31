<<<<<<< HEAD
# whatsapp.py
# -*- coding: utf-8 -*-
"""Menú de automatización por WhatsApp totalmente integrado con la app CLI."""

from __future__ import annotations

import atexit
import contextlib
import csv
import json
import logging
import os
import random
import textwrap
import threading
import time
import uuid
=======
# whatsapp.py
# -*- coding: utf-8 -*-
"""Menú de automatización por WhatsApp totalmente integrado con la app CLI."""

from __future__ import annotations

import atexit
import contextlib
import csv
import json
import logging
import os
import random
import textwrap
import threading
import time
import uuid
>>>>>>> origin/main
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

from core.storage_atomic import atomic_append_jsonl
from paths import exports_root, runtime_base, sessions_root, storage_root
from ui import Fore, full_line, style_text
<<<<<<< HEAD
from utils import (
    ask,
    ask_int,
    ask_multiline,
    banner,
    ok,
    press_enter,
    title,
)

=======
from utils import (
    ask,
    ask_int,
    ask_multiline,
    banner,
    ok,
    press_enter,
    title,
)

>>>>>>> origin/main
BASE = runtime_base(Path(__file__).resolve().parent.parent)
BASE.mkdir(parents=True, exist_ok=True)
_STORAGE_ROOT = storage_root(BASE)
_SESSIONS_ROOT = sessions_root(BASE)
DATA_FILE = _STORAGE_ROOT / "whatsapp_automation.json"
EXPORTS_DIR = exports_root(BASE) / "whatsapp"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
<<<<<<< HEAD

MIN_PHONE_DIGITS = 8
MAX_PHONE_DIGITS = 15

logger = logging.getLogger(__name__)

WHATSAPP_WEB_URL = "https://web.whatsapp.com"

=======

MIN_PHONE_DIGITS = 8
MAX_PHONE_DIGITS = 15

logger = logging.getLogger(__name__)

WHATSAPP_WEB_URL = "https://web.whatsapp.com"

>>>>>>> origin/main
# Playwright usa un "user_data_dir" persistente. Por defecto se ubica dentro de storage/
# para sobrevivir reinicios y builds donde el cwd puede variar.
DEFAULT_PLAYWRIGHT_SESSION_DIR = _SESSIONS_ROOT / "whatsapp_session"
PLAYWRIGHT_SESSIONS_DIR = _SESSIONS_ROOT / "whatsapp_sessions"
PLAYWRIGHT_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
<<<<<<< HEAD

_PLAYWRIGHT_LOCK = threading.RLock()
_STRUCTURED_LOG_PATH = _STORAGE_ROOT / "logs" / "whatsapp_automation.jsonl"

# Reuso de runtime Playwright para evitar recrear contexto/página en cada ciclo.
_PLAYWRIGHT_RUNTIME_LOCK = threading.RLock()
_PLAYWRIGHT_RUNTIME: _PlaywrightRuntime | None = None
_PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED = False

# Throttling anti-detección / anti-ráfaga para escaneo de no leídos.
_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS = 25
_AUTOREPLY_DEFAULT_THROTTLE_SECONDS = 35
_AUTOREPLY_DEFAULT_JITTER_SECONDS = 12
_AUTOREPLY_NO_UNREAD_BACKOFF_RANGE = (45, 95)
_AUTOREPLY_SELF_MESSAGE_BACKOFF_RANGE = (70, 140)
_AUTOREPLY_ERROR_BACKOFF_RANGE = (90, 180)

# Jitter del runner para evitar patrones exactos.
_MESSAGE_RUNNER_ACTIVE_JITTER = (0.25, 1.2)
_MESSAGE_RUNNER_IDLE_JITTER = (1.0, 4.0)

# Backoff de reintento de follow-up fallido para evitar loops.
_FOLLOWUP_FAILURE_RETRY_MINUTES = 30
_FOLLOWUP_MIN_CYCLE_SECONDS = 45


=======

_PLAYWRIGHT_LOCK = threading.RLock()
_STRUCTURED_LOG_PATH = _STORAGE_ROOT / "logs" / "whatsapp_automation.jsonl"

# Reuso de runtime Playwright para evitar recrear contexto/página en cada ciclo.
_PLAYWRIGHT_RUNTIME_LOCK = threading.RLock()
_PLAYWRIGHT_RUNTIME: _PlaywrightRuntime | None = None
_PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED = False

# Throttling anti-detección / anti-ráfaga para escaneo de no leídos.
_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS = 25
_AUTOREPLY_DEFAULT_THROTTLE_SECONDS = 35
_AUTOREPLY_DEFAULT_JITTER_SECONDS = 12
_AUTOREPLY_NO_UNREAD_BACKOFF_RANGE = (45, 95)
_AUTOREPLY_SELF_MESSAGE_BACKOFF_RANGE = (70, 140)
_AUTOREPLY_ERROR_BACKOFF_RANGE = (90, 180)

# Jitter del runner para evitar patrones exactos.
_MESSAGE_RUNNER_ACTIVE_JITTER = (0.25, 1.2)
_MESSAGE_RUNNER_IDLE_JITTER = (1.0, 4.0)

# Backoff de reintento de follow-up fallido para evitar loops.
_FOLLOWUP_FAILURE_RETRY_MINUTES = 30
_FOLLOWUP_MIN_CYCLE_SECONDS = 45


>>>>>>> origin/main
class _PlaywrightRuntime:
    def __init__(
        self,
        *,
        playwright: Any,
        context: Any,
        page: Any,
        user_data_dir: Path,
        headless: bool,
        runtime_id: str = "",
        owner_module: str = "",
    ) -> None:
        self.playwright = playwright
        self.context = context
        self.page = page
        self.user_data_dir = user_data_dir
        self.headless = bool(headless)
        self.runtime_id = str(runtime_id or "")
        self.owner_module = str(owner_module or "")
        self.created_at = _now_iso()
        self.last_used_at = _now_iso()
<<<<<<< HEAD


def _now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def _now_iso() -> str:
    return _now().isoformat() + "Z"


def _playwright_session_dir_for_number(number_id: str) -> Path:
    safe_id = (number_id or "").strip() or str(uuid.uuid4())
    return PLAYWRIGHT_SESSIONS_DIR / safe_id


# ======================================================================
# ===== Error Handling ===================================================

_STRUCTURED_LOG_LOCK = threading.RLock()


def _ensure_whatsapp_logging() -> None:
    """Inicializa logging de la app (si no existe) y habilita logs técnicos a disco."""
    try:
        from config import SETTINGS
        from runtime.runtime import ensure_logging

        ensure_logging(level=logging.INFO, quiet=False, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    except Exception:
        try:
            from runtime.runtime import ensure_logging

            ensure_logging(level=logging.INFO, quiet=False, log_dir=_STRUCTURED_LOG_PATH.parent, log_file="app.log")
        except Exception:
            if not logging.getLogger().handlers:
                logging.basicConfig(level=logging.INFO)


=======


def _now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def _now_iso() -> str:
    return _now().isoformat() + "Z"


def _playwright_session_dir_for_number(number_id: str) -> Path:
    safe_id = (number_id or "").strip() or str(uuid.uuid4())
    return PLAYWRIGHT_SESSIONS_DIR / safe_id


# ======================================================================
# ===== Error Handling ===================================================

_STRUCTURED_LOG_LOCK = threading.RLock()


def _ensure_whatsapp_logging() -> None:
    """Inicializa logging de la app (si no existe) y habilita logs técnicos a disco."""
    try:
        from config import SETTINGS
        from runtime.runtime import ensure_logging

        ensure_logging(level=logging.INFO, quiet=False, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    except Exception:
        try:
            from runtime.runtime import ensure_logging

            ensure_logging(level=logging.INFO, quiet=False, log_dir=_STRUCTURED_LOG_PATH.parent, log_file="app.log")
        except Exception:
            if not logging.getLogger().handlers:
                logging.basicConfig(level=logging.INFO)


>>>>>>> origin/main
def _log_structured(event: str, **fields: Any) -> None:
    """Escribe un log JSONL para auditoría y debugging sin romper el flujo."""
    record = {"ts": _now_iso(), "event": event, **fields}
    try:
        json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        # Si el payload no es serializable, degradamos a una forma mínima.
        try:
            record = {"ts": record.get("ts"), "event": event, "fields_repr": repr(fields)}
            json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return

    try:
        with _STRUCTURED_LOG_LOCK:
            _STRUCTURED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            atomic_append_jsonl(_STRUCTURED_LOG_PATH, record)
    except Exception:
        return
<<<<<<< HEAD


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        clean = value.rstrip("Z")
        return datetime.fromisoformat(clean)
    except Exception:
        return None


def _ensure_validation_entry(value: Any, number: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    raw = (value.get("raw") or number or "").strip()
    normalized = value.get("normalized") or number or raw
    digits = value.get("digits")
    if digits is None:
        digits = sum(1 for ch in normalized if ch.isdigit())
    has_plus = bool(value.get("has_plus")) if "has_plus" in value else normalized.startswith("+")
    status = value.get("status") or "unknown"
    message = value.get("message", "")
    checked_at = value.get("checked_at")
    return {
        "raw": raw,
        "normalized": normalized,
        "digits": int(digits) if isinstance(digits, int) else digits,
        "has_plus": has_plus,
        "status": status,
        "message": message,
        "checked_at": checked_at,
    }


def _validate_phone_number(raw: str) -> dict[str, Any]:
    candidate = (raw or "").strip()
    only_digits = "".join(ch for ch in candidate if ch.isdigit())
    normalized_digits = only_digits
    has_plus_hint = candidate.startswith("+")
    if candidate.startswith("00") and len(only_digits) > 2:
        normalized_digits = only_digits[2:]
        has_plus_hint = True
    normalized = f"+{normalized_digits}" if has_plus_hint else normalized_digits

    status = "valid"
    reasons: list[str] = []
    digits_count = len(normalized_digits)
    if not normalized_digits:
        status = "invalid"
        reasons.append("El número no contiene dígitos reconocibles.")
    elif digits_count < MIN_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número es demasiado corto para el formato internacional de WhatsApp.")
    elif digits_count > MAX_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número supera los 15 dígitos permitidos por WhatsApp.")
    elif not has_plus_hint:
        status = "warning"
        reasons.append("Falta el prefijo internacional (+).")

    message = " ".join(reasons) if reasons else "Formato internacional válido."
    return {
        "raw": candidate,
        "normalized": normalized,
        "digits": digits_count,
        "has_plus": has_plus_hint,
        "status": status,
        "message": message,
        "checked_at": _now_iso(),
    }


def _update_contact_validation(contact: dict[str, Any]) -> dict[str, Any]:
    current = contact.get("validation") or {}
    if not isinstance(current, dict):
        current = {}
    previous_status = current.get("status")
    base_number = contact.get("number")
    if not base_number:
        base_number = current.get("raw", "")
    new_validation = _validate_phone_number(base_number)
    normalized_number = new_validation.get("normalized") or new_validation.get("raw", "")
    if normalized_number:
        contact["number"] = normalized_number
    contact["validation"] = new_validation
    if previous_status != new_validation["status"] or previous_status is None:
        history = contact.setdefault("history", [])
        history.append(
            {
                "type": "validation",
                "status": new_validation["status"],
                "checked_at": new_validation["checked_at"],
                "message": new_validation["message"],
            }
        )
    return new_validation


def _ensure_delivery_log(entries: Any) -> list[dict[str, Any]]:
    log: list[dict[str, Any]] = []
    if not isinstance(entries, list):
        return log
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        log.append(
            {
                "timestamp": entry.get("timestamp"),
                "run_id": entry.get("run_id"),
                "status": entry.get("status"),
                "reason": entry.get("reason", ""),
                "confirmation": entry.get("confirmation"),
            }
        )
    return log


def _append_delivery_log(
    contact: dict[str, Any],
    run: dict[str, Any],
    *,
    status: str,
    reason: str = "",
    confirmation: str | None = None,
) -> None:
    log = contact.setdefault("delivery_log", [])
    log.append(
        {
            "timestamp": _now_iso(),
            "run_id": run.get("id"),
            "status": status,
            "reason": reason,
            "confirmation": confirmation,
        }
    )
    if len(log) > 50:
        del log[:-50]

def _default_state() -> dict[str, Any]:
=======


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        clean = value.rstrip("Z")
        return datetime.fromisoformat(clean)
    except Exception:
        return None


def _ensure_validation_entry(value: Any, number: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    raw = (value.get("raw") or number or "").strip()
    normalized = value.get("normalized") or number or raw
    digits = value.get("digits")
    if digits is None:
        digits = sum(1 for ch in normalized if ch.isdigit())
    has_plus = bool(value.get("has_plus")) if "has_plus" in value else normalized.startswith("+")
    status = value.get("status") or "unknown"
    message = value.get("message", "")
    checked_at = value.get("checked_at")
    return {
        "raw": raw,
        "normalized": normalized,
        "digits": int(digits) if isinstance(digits, int) else digits,
        "has_plus": has_plus,
        "status": status,
        "message": message,
        "checked_at": checked_at,
    }


def _validate_phone_number(raw: str) -> dict[str, Any]:
    candidate = (raw or "").strip()
    only_digits = "".join(ch for ch in candidate if ch.isdigit())
    normalized_digits = only_digits
    has_plus_hint = candidate.startswith("+")
    if candidate.startswith("00") and len(only_digits) > 2:
        normalized_digits = only_digits[2:]
        has_plus_hint = True
    normalized = f"+{normalized_digits}" if has_plus_hint else normalized_digits

    status = "valid"
    reasons: list[str] = []
    digits_count = len(normalized_digits)
    if not normalized_digits:
        status = "invalid"
        reasons.append("El número no contiene dígitos reconocibles.")
    elif digits_count < MIN_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número es demasiado corto para el formato internacional de WhatsApp.")
    elif digits_count > MAX_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número supera los 15 dígitos permitidos por WhatsApp.")
    elif not has_plus_hint:
        status = "warning"
        reasons.append("Falta el prefijo internacional (+).")

    message = " ".join(reasons) if reasons else "Formato internacional válido."
    return {
        "raw": candidate,
        "normalized": normalized,
        "digits": digits_count,
        "has_plus": has_plus_hint,
        "status": status,
        "message": message,
        "checked_at": _now_iso(),
    }


def _update_contact_validation(contact: dict[str, Any]) -> dict[str, Any]:
    current = contact.get("validation") or {}
    if not isinstance(current, dict):
        current = {}
    previous_status = current.get("status")
    base_number = contact.get("number")
    if not base_number:
        base_number = current.get("raw", "")
    new_validation = _validate_phone_number(base_number)
    normalized_number = new_validation.get("normalized") or new_validation.get("raw", "")
    if normalized_number:
        contact["number"] = normalized_number
    contact["validation"] = new_validation
    if previous_status != new_validation["status"] or previous_status is None:
        history = contact.setdefault("history", [])
        history.append(
            {
                "type": "validation",
                "status": new_validation["status"],
                "checked_at": new_validation["checked_at"],
                "message": new_validation["message"],
            }
        )
    return new_validation


def _ensure_delivery_log(entries: Any) -> list[dict[str, Any]]:
    log: list[dict[str, Any]] = []
    if not isinstance(entries, list):
        return log
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        log.append(
            {
                "timestamp": entry.get("timestamp"),
                "run_id": entry.get("run_id"),
                "status": entry.get("status"),
                "reason": entry.get("reason", ""),
                "confirmation": entry.get("confirmation"),
            }
        )
    return log


def _append_delivery_log(
    contact: dict[str, Any],
    run: dict[str, Any],
    *,
    status: str,
    reason: str = "",
    confirmation: str | None = None,
) -> None:
    log = contact.setdefault("delivery_log", [])
    log.append(
        {
            "timestamp": _now_iso(),
            "run_id": run.get("id"),
            "status": status,
            "reason": reason,
            "confirmation": confirmation,
        }
    )
    if len(log) > 50:
        del log[:-50]

def _default_state() -> dict[str, Any]:
>>>>>>> origin/main
    return {
        "numbers": {},
        "contact_lists": {},
        "templates": {},
        "message_runs": [],
        "ai_automations": {},
<<<<<<< HEAD
        "instagram": {
            "active": False,
            "delay": {"min": 5.0, "max": 12.0},
            "message": "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            "captures": [],
        },
        "followup": {
            "default_wait_minutes": 120,
            "manual_message": "Hola {nombre}, ¿pudiste ver mi mensaje anterior?",
            "ai_prompt": (
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar."
            ),
            "auto_enabled": False,
            "auto_mode": "manual",
            "active_number_id": "",
            "max_stage": 2,
            "last_auto_run_at": None,
            "history": [],
        },
        "payments": {
            "admin_number": "",
            "welcome_message": "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:",
            "access_link": "https://tusitio.com/accesos",
            "pending": [],
            "history": [],
        },
    }


class WhatsAppDataStore:
    """Persistencia simple en disco para el módulo de WhatsApp."""

    _io_lock = threading.RLock()

    def __init__(self, path: Path = DATA_FILE):
        self.path = path
        self.state = self._load()

    # ------------------------------------------------------------------
    def _load(self) -> dict[str, Any]:
        with self._io_lock:
            if not self.path.exists():
                return _default_state()
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return _default_state()
            return self._merge_defaults(data)

    def reload(self) -> None:
        self.state = self._load()

    # ------------------------------------------------------------------
    def _merge_defaults(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = _default_state()
        merged = dict(defaults)
        merged.update({k: data.get(k, v) for k, v in defaults.items()})
        merged["numbers"] = {
            key: self._ensure_number_structure(value)
            for key, value in dict(data.get("numbers", {})).items()
        }
        self._normalize_number_sessions(merged["numbers"])
=======
        "instagram": {
            "active": False,
            "delay": {"min": 5.0, "max": 12.0},
            "message": "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            "captures": [],
        },
        "followup": {
            "default_wait_minutes": 120,
            "manual_message": "Hola {nombre}, ¿pudiste ver mi mensaje anterior?",
            "ai_prompt": (
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar."
            ),
            "auto_enabled": False,
            "auto_mode": "manual",
            "active_number_id": "",
            "max_stage": 2,
            "last_auto_run_at": None,
            "history": [],
        },
        "payments": {
            "admin_number": "",
            "welcome_message": "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:",
            "access_link": "https://tusitio.com/accesos",
            "pending": [],
            "history": [],
        },
    }


class WhatsAppDataStore:
    """Persistencia simple en disco para el módulo de WhatsApp."""

    _io_lock = threading.RLock()

    def __init__(self, path: Path = DATA_FILE):
        self.path = path
        self.state = self._load()

    # ------------------------------------------------------------------
    def _load(self) -> dict[str, Any]:
        with self._io_lock:
            if not self.path.exists():
                return _default_state()
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return _default_state()
            return self._merge_defaults(data)

    def reload(self) -> None:
        self.state = self._load()

    # ------------------------------------------------------------------
    def _merge_defaults(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = _default_state()
        merged = dict(defaults)
        merged.update({k: data.get(k, v) for k, v in defaults.items()})
        merged["numbers"] = {
            key: self._ensure_number_structure(value)
            for key, value in dict(data.get("numbers", {})).items()
        }
        self._normalize_number_sessions(merged["numbers"])
>>>>>>> origin/main
        merged["contact_lists"] = {
            key: self._ensure_contact_list_structure(value)
            for key, value in dict(data.get("contact_lists", {})).items()
        }
        merged["templates"] = {
            str(key): value if isinstance(value, dict) else {"id": str(key), "name": str(key), "content": ""}
            for key, value in dict(data.get("templates", {})).items()
            if str(key).strip()
        }
        merged["message_runs"] = [
            self._ensure_message_run(item)
            for item in data.get("message_runs", [])
<<<<<<< HEAD
            if isinstance(item, dict)
        ]
        merged["ai_automations"] = {
            key: self._ensure_ai_config(value)
            for key, value in dict(data.get("ai_automations", {})).items()
        }
        merged["instagram"] = self._ensure_instagram_config(data.get("instagram", {}))
        merged["followup"] = self._ensure_followup_config(data.get("followup", {}))
        merged["payments"] = self._ensure_payments_config(data.get("payments", {}))
        return merged

    # ------------------------------------------------------------------
    def _normalize_number_sessions(self, numbers: dict[str, dict[str, Any]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in numbers.values():
            path = str(item.get("session_path") or "").strip()
            if not path:
                continue
            grouped.setdefault(path, []).append(item)
        for _path, items in grouped.items():
            if len(items) <= 1:
                continue
            # Conserva el primer perfil y separa el resto para evitar operar con el número incorrecto.
            for conflicted in items[1:]:
                number_id = conflicted.get("id") or str(uuid.uuid4())
                conflicted["session_path"] = str(_playwright_session_dir_for_number(number_id))
                conflicted["connected"] = False
                conflicted["connection_state"] = "pendiente"
                conflicted.setdefault("session_notes", []).append(
                    {
                        "created_at": _now_iso(),
                        "text": "Se detectó sesión compartida con otro número. Vinculación requerida para aislar la operación.",
                    }
                )

    # ------------------------------------------------------------------
    def _ensure_number_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            generated_id = str(uuid.uuid4())
            return {
                "id": generated_id,
                "alias": "",
                "phone": "",
                "connected": False,
                "last_connected_at": None,
                "session_notes": [],
                "keep_alive": True,
                "session_path": str(_playwright_session_dir_for_number(generated_id)),
                "connection_method": "playwright",
                "background_mode": True,
            }
        method = str(value.get("connection_method") or "playwright").strip().lower()
        if method != "playwright":
            method = "playwright"
        number_id = value.get("id") or str(uuid.uuid4())
        raw_session_path = (value.get("session_path") or "").strip()
        if raw_session_path:
            session_path = raw_session_path
        else:
            session_path = str(_playwright_session_dir_for_number(number_id))
        return {
            "id": number_id,
            "alias": value.get("alias", ""),
            "phone": value.get("phone", ""),
            "connected": bool(value.get("connected", False)),
            "last_connected_at": value.get("last_connected_at"),
            "session_notes": list(value.get("session_notes", [])),
            "keep_alive": bool(value.get("keep_alive", True)),
            "session_path": session_path,
            "qr_snapshot": value.get("qr_snapshot"),
            "last_qr_capture_at": value.get("last_qr_capture_at"),
            "connection_state": value.get("connection_state", "pendiente"),
            "connection_method": method,
            "background_mode": bool(value.get("background_mode", True)),
        }

    # ------------------------------------------------------------------
    def _ensure_contact(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        history = [entry for entry in raw.get("history", []) if isinstance(entry, dict)]
        validation = _ensure_validation_entry(raw.get("validation"), raw.get("number", ""))
        delivery_log = _ensure_delivery_log(raw.get("delivery_log"))
        followup_stage = raw.get("followup_stage", 0)
        try:
            followup_stage_int = int(followup_stage)
        except Exception:
            followup_stage_int = 0
        return {
            "name": raw.get("name", ""),
            "number": validation.get("normalized") or raw.get("number", ""),
            "status": raw.get("status", "sin mensaje"),
            "last_message_at": raw.get("last_message_at"),
            "last_response_at": raw.get("last_response_at"),
            # Estado ampliado para automatizaciones (auto-reply / follow-ups).
            "last_message_sent": raw.get("last_message_sent", ""),
            "last_message_sent_at": raw.get("last_message_sent_at") or raw.get("last_message_at"),
            "last_message_received": raw.get("last_message_received", ""),
            "last_message_received_at": raw.get("last_message_received_at") or raw.get("last_response_at"),
            "last_sender_id": raw.get("last_sender_id"),
            "followup_stage": followup_stage_int,
            "last_followup_at": raw.get("last_followup_at"),
            "last_payment_at": raw.get("last_payment_at"),
            "access_sent_at": raw.get("access_sent_at"),
            "notes": raw.get("notes", ""),
            "history": history,
            "validation": validation,
            "delivery_log": delivery_log,
        }

    # ------------------------------------------------------------------
    def _ensure_contact_list_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        contacts = [self._ensure_contact(item) for item in value.get("contacts", [])]
        return {
            "alias": value.get("alias", ""),
            "created_at": value.get("created_at") or _now_iso(),
            "contacts": contacts,
            "notes": value.get("notes", ""),
        }

    # ------------------------------------------------------------------
    def _ensure_message_run(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}

        def _to_int(raw: Any, default: int = 0) -> int:
            try:
                if raw in (None, ""):
                    return default
                return int(raw)
            except Exception:
                return default

        def _to_float(raw: Any, default: float = 0.0) -> float:
            try:
                if raw in (None, ""):
                    return default
                return float(raw)
            except Exception:
                return default

        delay = value.get("delay") or {}
        events = []
        for item in value.get("events", []):
            if not isinstance(item, dict):
                continue
            events.append(
                {
                    "contact": item.get("contact"),
                    "name": item.get("name"),
                    "message": item.get("message", ""),
                    "scheduled_at": item.get("scheduled_at"),
                    "status": item.get("status", "pendiente"),
                    "delivered_at": item.get("delivered_at"),
                    "notes": item.get("notes", ""),
                    "confirmation": item.get("confirmation", "no_enviado"),
                    "validation_status": item.get("validation_status"),
                    "error_code": item.get("error_code"),
                }
            )

        log = [
            {
                "timestamp": entry.get("timestamp", _now_iso()),
                "message": entry.get("message", ""),
            }
            for entry in value.get("log", [])
            if isinstance(entry, dict)
        ]

        message_template = value.get("message_template", "") or value.get("template", "")
        message_preview = value.get("message_preview", "")
        if message_template and not message_preview:
            message_preview = textwrap.shorten(message_template, width=90, placeholder="…")

        return {
            "id": value.get("id", str(uuid.uuid4())),
            "number_id": value.get("number_id"),
            "number_alias": value.get("number_alias"),
            "number_phone": value.get("number_phone"),
            "list_alias": value.get("list_alias"),
            "created_at": value.get("created_at") or _now_iso(),
            "status": value.get("status", "programado"),
            "paused": bool(value.get("paused", False)),
            "session_limit": _to_int(value.get("session_limit"), 0),
            "total_contacts": _to_int(value.get("total_contacts"), len(events)),
            "processed": _to_int(value.get("processed"), 0),
            "completed_at": value.get("completed_at"),
            "last_activity_at": value.get("last_activity_at"),
            "next_run_at": value.get("next_run_at"),
            "delay": {
                "min": _to_float(delay.get("min"), 5.0),
                "max": _to_float(delay.get("max"), 12.0),
            },
            "message_template": message_template,
            "message_preview": message_preview,
            "events": events,
            "max_contacts": _to_int(value.get("max_contacts"), 0),
            "last_session_at": value.get("last_session_at"),
            "completion_notified": bool(value.get("completion_notified", False)),
            "log": log,
        }

    # ------------------------------------------------------------------
    def _ensure_ai_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        polling_interval_seconds = value.get("polling_interval_seconds", 60)
        scan_throttle_seconds = value.get("scan_throttle_seconds", _AUTOREPLY_DEFAULT_THROTTLE_SECONDS)
        scan_jitter_seconds = value.get("scan_jitter_seconds", _AUTOREPLY_DEFAULT_JITTER_SECONDS)
        try:
            polling_interval_seconds = int(polling_interval_seconds)
        except Exception:
            polling_interval_seconds = 60
        try:
            scan_throttle_seconds = int(scan_throttle_seconds)
        except Exception:
            scan_throttle_seconds = _AUTOREPLY_DEFAULT_THROTTLE_SECONDS
        try:
            scan_jitter_seconds = int(scan_jitter_seconds)
        except Exception:
            scan_jitter_seconds = _AUTOREPLY_DEFAULT_JITTER_SECONDS
        return {
            "active": bool(value.get("active", False)),
            "prompt": value.get("prompt", ""),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 15.0)),
            },
            "send_audio": bool(value.get("send_audio", False)),
            "last_updated_at": value.get("last_updated_at"),
            "last_scan_at": value.get("last_scan_at"),
            "next_scan_at": value.get("next_scan_at"),
            "polling_interval_seconds": max(_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS, polling_interval_seconds),
            "scan_throttle_seconds": max(10, scan_throttle_seconds),
            "scan_jitter_seconds": max(0, scan_jitter_seconds),
        }

    # ------------------------------------------------------------------
    def _ensure_instagram_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        captures = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "name": item.get("name", ""),
                "number": item.get("number", ""),
                "source": item.get("source", "Instagram"),
                "captured_at": item.get("captured_at", _now_iso()),
                "message_sent": bool(item.get("message_sent", False)),
                "message_sent_at": item.get("message_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("captures", [])
            if isinstance(item, dict)
        ]
        return {
            "active": bool(value.get("active", False)),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 12.0)),
            },
            "message": value.get(
                "message",
                "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            ),
            "captures": captures,
            "last_reviewed_at": value.get("last_reviewed_at"),
        }

    # ------------------------------------------------------------------
    def _ensure_followup_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        history = [item for item in value.get("history", []) if isinstance(item, dict)]
        max_stage = value.get("max_stage", 2)
        try:
            max_stage = int(max_stage)
        except Exception:
            max_stage = 2
        auto_mode = (value.get("auto_mode", "manual") or "manual").strip().lower()
        if not auto_mode:
            auto_mode = "manual"
        return {
            "default_wait_minutes": int(value.get("default_wait_minutes", 120)),
            "manual_message": value.get(
                "manual_message", "Hola {nombre}, ¿pudiste ver mi mensaje anterior?"
            ),
            "ai_prompt": value.get(
                "ai_prompt",
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar.",
            ),
            "auto_enabled": bool(value.get("auto_enabled", False)),
            "auto_mode": auto_mode,
            "active_number_id": value.get("active_number_id", ""),
            "max_stage": max(1, max_stage),
            "last_auto_run_at": value.get("last_auto_run_at"),
            "history": history,
        }

    # ------------------------------------------------------------------
    def _ensure_payments_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        pending = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "evidence": item.get("evidence", ""),
                "keywords": list(item.get("keywords", [])),
                "status": item.get("status", "pendiente"),
                "created_at": item.get("created_at", _now_iso()),
                "validated_at": item.get("validated_at"),
                "welcome_sent_at": item.get("welcome_sent_at"),
                "alert_sent_at": item.get("alert_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("pending", [])
            if isinstance(item, dict)
        ]
        history = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "status": item.get("status", "completado"),
                "completed_at": item.get("completed_at", _now_iso()),
                "notes": item.get("notes", ""),
            }
            for item in value.get("history", [])
            if isinstance(item, dict)
        ]
        return {
            "admin_number": value.get("admin_number", ""),
            "welcome_message": value.get(
                "welcome_message", "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:"
            ),
            "access_link": value.get("access_link", "https://tusitio.com/accesos"),
            "pending": pending,
            "history": history,
        }

    # ------------------------------------------------------------------
    def save(self) -> None:
        serialized = json.dumps(self.state, ensure_ascii=False, indent=2)
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with self._io_lock:
            tmp_path.write_text(serialized, encoding="utf-8")
            tmp_path.replace(self.path)

    # ------------------------------------------------------------------
    # Helper methods ----------------------------------------------------
    def iter_numbers(self) -> Iterator[dict[str, Any]]:
        for item in self.state.get("numbers", {}).values():
            yield item

    def iter_lists(self) -> Iterator[tuple[str, dict[str, Any]]]:
        for alias, data in self.state.get("contact_lists", {}).items():
            yield alias, data

    def find_number(self, number_id: str) -> dict[str, Any] | None:
        return self.state.get("numbers", {}).get(number_id)

    def find_list(self, alias: str) -> dict[str, Any] | None:
        return self.state.get("contact_lists", {}).get(alias)


# ----------------------------------------------------------------------
# Presentación y helpers de impresión

def _line() -> str:
    return full_line(color=Fore.BLUE, bold=True)


def _info(msg: str, *, color: str = Fore.CYAN, bold: bool = False) -> None:
    print(style_text(msg, color=color, bold=bold))


def _subtitle(msg: str) -> None:
    print(style_text(msg, color=Fore.MAGENTA, bold=True))


def _format_delay(delay: dict[str, float]) -> str:
    return f"{delay['min']:.1f}s – {delay['max']:.1f}s"


# ======================================================================
# ===== Selectors =======================================================

def _join_selectors(selectors: Iterable[str]) -> str:
    return ", ".join([item for item in selectors if item])


class _WASelectors:
    # Login / sesión
    QR_CANVAS = [
        "canvas[data-testid='qrcode']",
        "canvas",
    ]
    APP_READY = [
        "div[data-testid='pane-side']",
        "div[data-testid='chat-list']",
        "div[data-testid='chat-list-search']",
        "div[role='grid']",
        "div[data-testid='app-title']",
    ]

    # Chat list / rows / unread
    PANE_SIDE = [
        "div[data-testid='pane-side']",
        "div[data-testid='chat-list']",
        "div[role='grid']",
    ]
    CHAT_ROW_CANDIDATES = [
        "div[data-testid='cell-frame-container']",
        "div[role='row']",
        "div[role='listitem']",
    ]
    UNREAD_BADGE = [
        "span[data-testid='icon-unread-count']",
        "span[aria-label*='unread']",
        "span[aria-label*='Unread']",
        "span[aria-label*='no leído']",
        "span[aria-label*='no leídos']",
        "span[aria-label*='mensaje no leído']",
        "span[aria-label*='mensajes no leídos']",
    ]
    CHAT_TITLE_IN_ROW = [
        "span[dir='auto']",
        "span[title]",
    ]
    ACTIVE_CHAT_TITLE = [
        "header span[title]",
        "header div[role='button'] span[title]",
        "header span[dir='auto']",
    ]

    # Conversación
    CHAT_INPUT = [
        "footer div[contenteditable='true'][data-testid='conversation-compose-box-input']",
        "footer div[contenteditable='true'][role='textbox']",
        "footer div[contenteditable='true']",
    ]
    SEND_BUTTON = [
        "button[data-testid='compose-btn-send']",
        "button:has([data-icon='send'])",
        "button:has([aria-label*='Send'])",
        "button:has([aria-label*='Enviar'])",
    ]

    MESSAGE_CONTAINER = [
        "div[data-testid='msg-container']",
        "div.message-in, div.message-out",
    ]
    BUBBLE = [
        "div.message-in",
        "div.message-out",
        "div[data-testid='msg-container'].message-in",
        "div[data-testid='msg-container'].message-out",
        "div[data-testid='msg-container'] div.message-in",
        "div[data-testid='msg-container'] div.message-out",
    ]
    BUBBLE_IN = [
        "div.message-in",
        "div[data-testid='msg-container'].message-in",
        "div[data-testid='msg-container'] div.message-in",
    ]
    BUBBLE_OUT = [
        "div.message-out",
        "div[data-testid='msg-container'].message-out",
        "div[data-testid='msg-container'] div.message-out",
    ]
    BUBBLE_TEXT = [
        "span[data-testid='conversation-text']",
        "span.selectable-text.copyable-text span",
        "div.copyable-text span.selectable-text span",
    ]

    # Confirmaciones
    CHECK_READ = "svg[data-testid='msg-dblcheck-read']"
    CHECK_DELIVERED = "svg[data-testid='msg-dblcheck']"
    CHECK_SENT = "svg[data-testid='msg-check']"

    # Errores / estados
    ALERT_TEXT = [
        "div[data-testid='app-state-message']",
        "div[data-testid='alert-qr-text']",
        "div[data-testid='empty-state-title']",
        "div[role='dialog']",
        "div[data-testid='popup-controls-ok']",
    ]


# ----------------------------------------------------------------------
# Runner de envios en segundo plano

_MESSAGE_RUNNER: _MessageRunner | None = None
_MESSAGE_RUNNER_MIN_SLEEP = 2.0
_MESSAGE_RUNNER_MAX_SLEEP = 8.0
_MESSAGE_RUNNER_IDLE_SLEEP = 20.0


class _MessageRunner:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        _ensure_whatsapp_logging()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="whatsapp-message-runner", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        _shutdown_playwright_runtime()

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _loop(self) -> None:
        while not self._stop.is_set():
            sleep_for = _MESSAGE_RUNNER_IDLE_SLEEP
            try:
                _ensure_whatsapp_logging()
                store = WhatsAppDataStore()
                _reconcile_runs(store)
                sleep_for = _message_runner_sleep(store)
            except Exception as exc:
                logger.exception("Error en whatsapp-message-runner: %s", exc)
                _log_structured("whatsapp.runner.error", error=str(exc))
                sleep_for = _MESSAGE_RUNNER_IDLE_SLEEP
            self._stop.wait(sleep_for)


def _message_runner_sleep(store: WhatsAppDataStore) -> float:
    next_runs: list[datetime] = []
    for run in store.state.get("message_runs", []):
        status = (run.get("status") or "").lower()
        if status in {"completado", "cancelado"}:
            continue
        if run.get("paused"):
            continue
        next_at = _parse_iso(run.get("next_run_at"))
        if next_at:
            next_runs.append(next_at)
    if not next_runs:
        idle = _MESSAGE_RUNNER_IDLE_SLEEP + random.uniform(*_MESSAGE_RUNNER_IDLE_JITTER)
        return max(_MESSAGE_RUNNER_IDLE_SLEEP, idle)
    next_at = min(next_runs)
    delta = (next_at - _now()).total_seconds()
    if delta <= 0:
        base = _MESSAGE_RUNNER_MIN_SLEEP
    else:
        base = min(max(delta, _MESSAGE_RUNNER_MIN_SLEEP), _MESSAGE_RUNNER_MAX_SLEEP)
    jittered = base + random.uniform(*_MESSAGE_RUNNER_ACTIVE_JITTER)
    return min(max(jittered, _MESSAGE_RUNNER_MIN_SLEEP), _MESSAGE_RUNNER_MAX_SLEEP + 1.5)


def _ensure_message_runner() -> None:
    global _MESSAGE_RUNNER
    if _MESSAGE_RUNNER is None:
        _MESSAGE_RUNNER = _MessageRunner()
        atexit.register(_MESSAGE_RUNNER.stop)
    _MESSAGE_RUNNER.start()


def _message_runner_active() -> bool:
    return bool(_MESSAGE_RUNNER and _MESSAGE_RUNNER.is_running())


def _sync_message_runs(store: WhatsAppDataStore) -> None:
    if _message_runner_active():
        store.reload()
    else:
        _reconcile_runs(store)


# ----------------------------------------------------------------------
# Menú principal del módulo

def menu_whatsapp() -> None:
    _ensure_whatsapp_logging()
    store = WhatsAppDataStore()
    _ensure_message_runner()
    while True:
        _sync_message_runs(store)
        banner()
        title("Automatización por WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Conectar número de WhatsApp")
        print("2) Importar lista de contactos")
        print("3) Enviar mensajes a la lista")
        print("4) Automatizar respuestas con IA")
        print("5) Capturar números desde Instagram")
        print("6) Seguimiento automatizado a no respondidos")
        print("7) Gestión de pagos y entrega de accesos")
        print("8) Estado de contactos y actividad")
        print("9) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _connect_number(store)
        elif op == "2":
            _import_contacts(store)
        elif op == "3":
            _send_messages(store)
        elif op == "4":
            _configure_ai_responses(store)
        elif op == "5":
            _instagram_capture(store)
        elif op == "6":
            _followup_manager(store)
        elif op == "7":
            _payments_menu(store)
        elif op == "8":
            _contacts_state(store)
        elif op == "9":
            break
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 1) Conectar número ----------------------------------------------------

def _print_numbers_summary(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("Aún no hay números conectados.")
        return
    _subtitle("Números activos")
    for item in sorted(numbers, key=lambda n: n.get("alias")):  # type: ignore[arg-type]
        alias = item.get("alias") or item.get("phone")
        if item.get("connected"):
            status = "🟢 Verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 Error de vinculación"
        else:
            status = "⚪ Pendiente"
        last = item.get("last_connected_at") or "(sin actividad)"
        print(
            f" • {alias} ({item.get('phone')}) - {status} – última conexión: {last}"
        )


def _select_connection_backend() -> str | None:
    while True:
        print(_line())
        _subtitle("Método de vinculación")
        print("1) Playwright (Chromium persistente)")
        print("2) Volver\n")
        choice = ask("Opción: ").strip()
        if choice == "2":
            return None
        if choice == "1":
            return "playwright"
        _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)


def _connect_number(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Conectar número de WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Vincular nuevo número")
        print("2) Eliminar número vinculado")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _link_new_number(store)
        elif op == "2":
            _remove_linked_number(store)
        elif op == "3":
            return
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


def _link_new_number(store: WhatsAppDataStore) -> None:
    alias = ask("Alias interno para reconocer el número: ").strip()
    phone = ask("Número en formato internacional (ej: +54911...): ").strip()
    if not phone:
        _info("No se ingresó número.", color=Fore.YELLOW)
        press_enter()
        return
    note = ask("Nota interna u observación (opcional): ").strip()
    backend = _select_connection_backend()
    if backend is None:
        _info("No se inició ninguna vinculación.")
        press_enter()
        return

    session_id = str(uuid.uuid4())
    session_dir = _playwright_session_dir_for_number(session_id)
    session_dir.mkdir(parents=True, exist_ok=True)

    _info("Preparando Playwright (Chromium) para WhatsApp Web...")
    success, snapshot, details = _initiate_whatsapp_web_login(session_dir, backend)
    if details:
        _info(details)

    if not success:
        confirm = ask(
            "¿Lograste vincular la sesión desde la ventana abierta? (s/N): "
        ).strip().lower()
        if confirm == "s":
            success = True

    state = store.state.setdefault("numbers", {})
    record = {
        "id": session_id,
        "alias": alias or phone,
        "phone": phone,
        "connected": success,
        "last_connected_at": _now_iso() if success else None,
        "session_notes": [
            {
                "created_at": _now_iso(),
                "text": note
                or "Sesión gestionada mediante Playwright (Chromium).",
            }
        ],
        "keep_alive": True,
        "session_path": str(session_dir),
        "qr_snapshot": str(snapshot) if snapshot else None,
        "last_qr_capture_at": _now_iso() if snapshot else None,
        "connection_state": "verificado" if success else "pendiente",
        "connection_method": backend,
        "background_mode": True,
    }
    if not success:
        record["session_notes"].append(
            {
                "created_at": _now_iso(),
                "text": "La vinculación automática no se completó. Reintentar desde el menú.",
            }
        )
        record["connection_state"] = "fallido"
    state[session_id] = record
    store.save()

    if success:
        ok("Sesión verificada y lista para operar en segundo plano.")
    else:
        _info(
            "No se confirmó la vinculación. Podés reintentar el proceso desde este mismo menú.",
            color=Fore.YELLOW,
        )
    press_enter()


def _remove_linked_number(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("No hay números registrados para eliminar.", color=Fore.YELLOW)
        press_enter()
        return
    print(_line())
    _subtitle("Seleccioná el número a desvincular")
    ordered = sorted(numbers, key=lambda n: n.get("alias"))
    for idx, item in enumerate(ordered, 1):
        alias = item.get("alias") or item.get("phone")
        status = "🟢" if item.get("connected") else "⚪"
        print(f"{idx}) {alias} ({item.get('phone')}) - {status}")
    idx = ask_int("Número a eliminar: ", min_value=1)
    if idx > len(ordered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return
    selected = ordered[idx - 1]
    alias = selected.get("alias") or selected.get("phone")
    confirm = ask(f"Confirmá eliminación de '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    state = store.state.setdefault("numbers", {})
    state.pop(selected.get("id"), None)
    store.save()
    ok(f"Se eliminó el número vinculado '{alias}'.")
    press_enter()


def _initiate_whatsapp_web_login(
    session_dir: Path, backend: str
) -> tuple[bool, Path | None, str]:
    if backend != "playwright":
        return False, None, "Solo se admite Playwright para la vinculación automática."
    return _initiate_with_playwright(session_dir)


def _initiate_with_playwright(session_dir: Path) -> tuple[bool, Path | None, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from src.playwright_service import resolve_playwright_executable
        from src.runtime.playwright_runtime import launch_sync_persistent_context, sync_playwright_context
    except ImportError:
        return (
            False,
            None,
            "Playwright no está instalado. Ejecutá 'pip install playwright' y luego 'playwright install'.",
        )

    snapshot_path = session_dir / "qr.png"
    info_messages: list[str] = []
    success = False

    try:
        with _PLAYWRIGHT_LOCK:
            # Evita conflicto de perfil si hay runtime compartido activo.
            _shutdown_playwright_runtime()
            with sync_playwright_context():
                executable = resolve_playwright_executable(headless=False)
                context = launch_sync_persistent_context(
                    user_data_dir=session_dir,
                    headless=False,
                    executable_path=executable,
                    args=[
                        "--disable-notifications",
                        "--disable-infobars",
                        "--disable-dev-shm-usage",
                    ],
                    viewport={"width": 1280, "height": 720},
                )
                try:
                    page = context.pages[0] if context.pages else context.new_page()
                    page.set_default_timeout(60000)
                    page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")

                    state = _wa_wait_for_login_state(page, timeout_seconds=15.0)
                    if state == "ready":
                        success = True
                        info_messages.append("La sesión ya estaba vinculada en este perfil.")
                    else:
                        qr_selector = _join_selectors(_WASelectors.QR_CANVAS)
                        if state == "qr":
                            try:
                                qr_element = page.locator(qr_selector).first
                                qr_element.screenshot(path=str(snapshot_path))
                            except Exception:
                                try:
                                    page.screenshot(path=str(snapshot_path))
                                except Exception:
                                    pass
                            if snapshot_path.exists():
                                info_messages.append(
                                    f"Se guardó una captura del código QR en {snapshot_path}."
                                )
                            info_messages.append(
                                "Escaneá el código con tu celular para completar la vinculación."
                            )
                        else:
                            try:
                                page.screenshot(path=str(snapshot_path))
                            except Exception:
                                pass

                        ready_selector = _join_selectors(_WASelectors.APP_READY)
                        try:
                            page.wait_for_selector(ready_selector, timeout=180000)
                            success = True
                        except PlaywrightTimeoutError:
                            success = False

                        # Si WhatsApp cambió y el QR ya no está visible, asumimos éxito.
                        if not success:
                            try:
                                if not page.locator(qr_selector).first.is_visible():
                                    success = True
                            except Exception:
                                pass
                finally:
                    try:
                        context.close()
                    except Exception:
                        pass
    except Exception:
        info_messages.append(
            "No se pudo automatizar la conexión. Verificá que Playwright tenga los navegadores instalados."
        )
        success = False

    message = " ".join(info_messages)
    return success, snapshot_path if snapshot_path.exists() else None, message



# ----------------------------------------------------------------------
# 2) Importar contactos -------------------------------------------------

def _import_contacts(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Importar lista de contactos")
        print(_line())
        existing = list(store.iter_lists())
        if existing:
            _subtitle("Listas registradas")
            for alias, data in existing:
                total = len(data.get("contacts", []))
                print(f" • {alias} ({total} contactos)")
            print(_line())
        print("1) Carga manual")
        print("2) Importar desde CSV (nombre, número)")
        print("3) Ver listas cargadas")
        print("4) Eliminar una lista cargada")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _manual_contacts_entry(store)
        elif op == "2":
            _csv_contacts_entry(store)
        elif op == "3":
            _show_loaded_contacts(store)
        elif op == "4":
            _delete_contact_list(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida. Probá otra vez.", color=Fore.YELLOW)
            press_enter()


def _manual_contacts_entry(store: WhatsAppDataStore) -> None:
    alias = ask("Nombre o alias de la lista: ").strip() or f"lista-{_now().strftime('%H%M%S')}"
    _info("Ingresá número y nombre separados por coma. Línea vacía para terminar.")
    contacts = []
    while True:
        raw = ask("Contacto: ").strip()
        if not raw:
            break
        if "," in raw:
            number, name = [part.strip() for part in raw.split(",", 1)]
        else:
            number, name = raw, ""
        contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se agregaron contactos.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Se registraron {summary['stored']} contactos en la lista '{alias}'."
        )
    else:
        _info("No se guardaron contactos. Revisá los números proporcionados.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _csv_contacts_entry(store: WhatsAppDataStore) -> None:
    path = ask("Ruta del archivo CSV: ").strip()
    if not path:
        _info("No se indicó archivo.", color=Fore.YELLOW)
        press_enter()
        return
    csv_path = Path(path)
    if not csv_path.exists():
        _info("El archivo indicado no existe.", color=Fore.YELLOW)
        press_enter()
        return
    alias = ask("Alias para la lista importada: ").strip() or csv_path.stem
    contacts: list[dict[str, str]] = []
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            name = (row[0] or "").strip()
            number = (row[1] if len(row) > 1 else "").strip()
            if not number and name:
                number, name = name, number
            if not number:
                continue
            contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se encontraron contactos válidos en el CSV.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Importación completada. {summary['stored']} registros cargados en '{alias}'."
        )
    else:
        _info("El archivo no aportó contactos válidos tras la validación.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _select_existing_list(
    store: WhatsAppDataStore, prompt: str
) -> tuple[str, dict[str, Any]] | None:
    lists = sorted(list(store.iter_lists()), key=lambda item: item[0].lower())
    if not lists:
        _info("Aún no hay listas registradas.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return lists[idx - 1]


def _show_loaded_contacts(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Elegí la lista a visualizar")
    if not selection:
        return
    alias, data = selection
    banner()
    title(f"Contactos registrados en '{alias}'")
    print(_line())
    contacts = data.get("contacts", [])
    if not contacts:
        _info("La lista no tiene contactos cargados.", color=Fore.YELLOW)
        press_enter()
        return
    for contact in contacts:
        name = contact.get("name") or contact.get("number")
        print(f"• {name} - {contact.get('number')}")
    press_enter()


def _delete_contact_list(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Seleccioná la lista a eliminar")
    if not selection:
        return
    alias, _ = selection
    confirm = ask(f"Confirmá eliminación de la lista '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    store.state.setdefault("contact_lists", {}).pop(alias, None)
    store.save()
    ok(f"Se eliminó la lista '{alias}'.")
    press_enter()


def _persist_contacts(
    store: WhatsAppDataStore, alias: str, contacts: Iterable[dict[str, str]]
) -> dict[str, int]:
    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    invalid_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for item in contacts:
        number = item.get("number", "")
        if not number:
            continue
        validation = _validate_phone_number(number)
        normalized_number = validation.get("normalized") or validation.get("raw", "")
        contact = {
            "name": item.get("name", "") or normalized_number,
            "number": normalized_number,
            "status": "sin mensaje",
            "last_message_at": None,
            "last_response_at": None,
            "last_message_sent": "",
            "last_message_sent_at": None,
            "last_message_received": "",
            "last_message_received_at": None,
            "last_sender_id": None,
            "followup_stage": 0,
            "last_followup_at": None,
            "last_payment_at": None,
            "access_sent_at": None,
            "notes": "",
            "history": [
                {
                    "type": "validation",
                    "status": validation["status"],
                    "checked_at": validation["checked_at"],
                    "message": validation["message"],
                }
            ],
            "validation": validation,
            "delivery_log": [],
        }
        if validation["status"] == "invalid":
            invalid_entries.append((contact, validation))
        elif validation["status"] == "warning":
            warning_entries.append((contact, validation))
        prepared.append((contact, validation))

    if not prepared:
        return {"stored": 0, "invalid": 0, "warnings": 0, "skipped": 0}

    stored_entries = list(prepared)
    skipped_invalid = 0

    if invalid_entries:
        _info("Se detectaron números inválidos y podrían no existir en WhatsApp:", color=Fore.YELLOW)
        for contact, validation in invalid_entries[:5]:
            print(f" • {contact.get('name')} - {validation.get('raw')} ({validation.get('message')})")
        if len(invalid_entries) > 5:
            print(f"   ... y {len(invalid_entries) - 5} más")
        choice = ask("¿Deseás conservarlos igualmente? (s/N): ").strip().lower()
        if choice != "s":
            to_remove = {id(entry[0]) for entry in invalid_entries}
            stored_entries = [entry for entry in stored_entries if id(entry[0]) not in to_remove]
            skipped_invalid = len(invalid_entries)
            _info(
                "Se omitieron los números inválidos para evitar errores futuros.",
                color=Fore.YELLOW,
            )

    if not stored_entries:
        return {
            "stored": 0,
            "invalid": len(invalid_entries),
            "warnings": len(warning_entries),
            "skipped": skipped_invalid,
        }

    if warning_entries:
        _info(
            "Algunos números no tienen formato internacional completo. Se marcarán con advertencia.",
            color=Fore.YELLOW,
        )

    items = [entry[0] for entry in stored_entries]
    lists = store.state.setdefault("contact_lists", {})
    current = lists.get(alias)
    if current:
        current_contacts = current.get("contacts", [])
        current_contacts.extend(items)
        current["contacts"] = current_contacts
        if not current.get("alias"):
            current["alias"] = alias
    else:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": items,
            "notes": "",
        }
    store.save()
    return {
        "stored": len(items),
        "invalid": len(invalid_entries),
        "warnings": len(warning_entries),
        "skipped": skipped_invalid,
    }


# ----------------------------------------------------------------------
# 3) Envío de mensajes --------------------------------------------------

def _send_messages(store: WhatsAppDataStore) -> None:
    if not list(store.iter_numbers()):
        _info(
            "Necesitás vincular al menos un número antes de enviar mensajes.",
            color=Fore.YELLOW,
        )
        press_enter()
        return
    if not list(store.iter_lists()):
        _info("Cargá primero una lista de contactos.", color=Fore.YELLOW)
        press_enter()
        return

    while True:
        _sync_message_runs(store)
        banner()
        title("Programación de envíos por WhatsApp")
        print(_line())
        _print_runs_overview(store)
        print(_line())
        print("1) Programar nuevo envío automático")
        print("2) Ver detalle de un envío programado")
        print("3) Pausar o reanudar un envío")
        print("4) Cancelar un envío")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _plan_message_run(store)
        elif op == "2":
            _show_run_detail(store)
        elif op == "3":
            _toggle_run_pause(store)
        elif op == "4":
            _cancel_run(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _plan_message_run(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    contact_list = _choose_contact_list(store)
    if not contact_list:
        return
    contacts = list(contact_list.get("contacts", []))
    if not contacts:
        _info("La lista no tiene contactos.", color=Fore.YELLOW)
        press_enter()
        return

    max_contacts = ask_int(
        "¿Cuántos contactos incluir en este envío? (0 = todos): ",
        min_value=0,
        default=0,
    )
    if max_contacts and max_contacts < len(contacts):
        targets = contacts[:max_contacts]
    else:
        targets = contacts
    if not targets:
        _info("No se seleccionaron contactos para el envío.", color=Fore.YELLOW)
        press_enter()
        return

    store_dirty = False
    invalid_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for contact in list(targets):
        previous_validation = contact.get("validation", {})
        validation = _update_contact_validation(contact)
        if validation != previous_validation:
            store_dirty = True
        if validation["status"] == "invalid":
            invalid_targets.append((contact, validation))
        elif validation["status"] == "warning":
            warning_targets.append((contact, validation))

    if invalid_targets:
        _info(
            "Hay contactos con números inválidos que WhatsApp rechazará.",
            color=Fore.YELLOW,
        )
        for contact, validation in invalid_targets[:5]:
            print(
                f" • {contact.get('name')} ({validation.get('raw')}) → {validation.get('message')}"
            )
        if len(invalid_targets) > 5:
            print(f"   ... y {len(invalid_targets) - 5} más")
        choice = ask(
            "¿Deseás programar igual para estos contactos inválidos? (s/N): "
        ).strip().lower()
        if choice != "s":
            to_exclude = {id(entry[0]) for entry in invalid_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se excluyeron los contactos con números inválidos del envío.",
                color=Fore.YELLOW,
            )
        else:
            for contact, _ in invalid_targets:
                contact["status"] = "observado"
                history = contact.setdefault("history", [])
                history.append(
                    {
                        "type": "validation_override",
                        "timestamp": _now_iso(),
                        "message": "Se programó un envío pese a la validación inválida.",
                    }
                )
            store_dirty = True

    if warning_targets and targets:
        _info(
            "Algunos contactos no tienen código de país. Podrían fallar los envíos.",
            color=Fore.YELLOW,
        )
        choice = ask("¿Deseás continuar con ellos? (S/n): ").strip().lower()
        if choice == "n":
            to_exclude = {id(entry[0]) for entry in warning_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se quitaron los contactos en advertencia del envío.",
                color=Fore.YELLOW,
            )

    if not targets:
        _info("No quedaron contactos válidos tras la validación.", color=Fore.YELLOW)
        if store_dirty:
            store.save()
        press_enter()
        return

    if store_dirty:
        store.save()

    message_template = ask_multiline(
        "Mensaje a enviar (usa {nombre} para personalizar): "
    ).strip()
    if not message_template:
        _info("Mensaje vacío. Operación cancelada.", color=Fore.YELLOW)
        press_enter()
        return

    min_delay, max_delay = _ask_delay_range()
    session_limit = ask_int(
        "Cantidad máxima de mensajes por sesión (0 = sin tope): ",
        min_value=0,
        default=0,
    )

    planned_at = _now()
    run_id = str(uuid.uuid4())
    events: list[dict[str, Any]] = []
    for contact in targets:
        planned_at += timedelta(seconds=random.uniform(min_delay, max_delay))
        rendered = _render_message(message_template, contact)
        scheduled_at = planned_at.isoformat() + "Z"
        events.append(
            {
                "contact": contact.get("number"),
                "name": contact.get("name"),
                "message": rendered,
                "scheduled_at": scheduled_at,
                "status": "pendiente",
                "delivered_at": None,
                "notes": "",
                "confirmation": "no_enviado",
                "validation_status": contact.get("validation", {}).get("status"),
                "error_code": None,
            }
        )
        _mark_contact_scheduled(
            contact,
            run_id,
            rendered,
            scheduled_at,
            min_delay,
            max_delay,
        )

    run = {
        "id": run_id,
        "number_id": number["id"],
        "number_alias": number.get("alias"),
        "number_phone": number.get("phone"),
        "list_alias": contact_list.get("alias"),
        "created_at": _now_iso(),
        "status": "programado",
        "paused": False,
        "session_limit": session_limit,
        "total_contacts": len(events),
        "processed": 0,
        "completed_at": None,
        "last_activity_at": None,
        "next_run_at": events[0]["scheduled_at"] if events else None,
        "delay": {"min": min_delay, "max": max_delay},
        "message_template": message_template,
        "message_preview": textwrap.shorten(
            message_template, width=90, placeholder="…"
        ),
        "events": events,
        "max_contacts": max_contacts,
        "last_session_at": None,
        "completion_notified": False,
        "log": [],
    }
    _append_run_log(
        run,
        f"Se programó el envío para {len(events)} contactos con delays entre {min_delay:.1f}s y {max_delay:.1f}s.",
    )
    store.state.setdefault("message_runs", []).append(run)
    store.save()
    ok(
        "El envío quedó programado y continuará ejecutándose en segundo plano con ritmo humano."
    )
    press_enter()


def _print_runs_overview(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    if not runs:
        _info("No hay envíos programados todavía. Usá la opción 1 para crear uno.")
        return

    active = [
        run for run in runs if (run.get("status") or "").lower() not in {"completado", "cancelado"}
    ]
    if active:
        _subtitle("Envíos activos")
        for run in sorted(active, key=lambda item: item.get("created_at") or ""):
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            next_run = run.get("next_run_at") or "(esperando horario)"
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Próximo: {next_run}"
            )
    else:
        _info("No hay ejecuciones activas en este momento.")

    completed = [
        run for run in runs if (run.get("status") or "").lower() in {"completado", "cancelado"}
    ]
    if completed:
        print()
        _subtitle("Historial reciente")
        for run in sorted(
            completed,
            key=lambda item: item.get("completed_at") or item.get("last_activity_at") or item.get("created_at") or "",
            reverse=True,
        )[:3]:
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            finished = run.get("completed_at") or run.get("last_activity_at") or run.get("created_at")
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Finalizó: {finished or 'sin fecha'}"
            )


def _show_run_detail(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a monitorear",
        include_completed=True,
    )
    if not run:
        return
    _sync_message_runs(store)
    banner()
    title("Detalle del envío por WhatsApp")
    print(_line())
    total, sent, pending, cancelled, failed = _run_counts(run)
    print(f"Lista: {_run_list_label(run)} → Número: {_run_number_label(run)}")
    print(f"Estado actual: {_run_status_label(run)}")
    print(f"Mensajes enviados: {sent}/{total}")
    print(
        f"Pendientes: {pending} | Fallidos: {failed} | Cancelados/Omitidos: {cancelled}"
    )
    print(f"Delay configurado: {_format_delay(run.get('delay', {'min': 5.0, 'max': 12.0}))}")
    session_limit = run.get("session_limit") or 0
    if session_limit:
        print(f"Límite por sesión: {session_limit} mensajes")
    next_run = run.get("next_run_at")
    if next_run:
        print(f"Próximo envío estimado: {next_run}")
    if run.get("last_session_at"):
        print(f"Última sesión completada: {run.get('last_session_at')}")
    if run.get("message_preview"):
        print(f"Plantilla: {run.get('message_preview')}")
    next_event = next(
        (event for event in run.get("events", []) if (event.get("status") or "") == "pendiente"),
        None,
    )
    if next_event:
        print(
            "Próximo contacto: "
            f"{next_event.get('name') or next_event.get('contact')} a las {next_event.get('scheduled_at')}"
        )
    log = run.get("log", [])
    if log:
        print()
        _subtitle("Actividad registrada")
        for entry in log[-5:]:
            print(f" - {entry.get('timestamp')}: {entry.get('message')}")

    processed_events = [
        event
        for event in run.get("events", [])
        if (event.get("status") or "") in {"enviado", "fallido", "cancelado", "omitido"}
    ]
    if processed_events:
        print()
        _subtitle("Resultados recientes por contacto")
        for event in processed_events[-10:]:
            contact_label = event.get("name") or event.get("contact") or "(sin nombre)"
            status_label = _format_event_status(event)
            print(f" - {contact_label}: {status_label}")
            if event.get("notes"):
                print(f"     Motivo: {event.get('notes')}")
    press_enter()


def _toggle_run_pause(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a pausar o reanudar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status in {"completado", "cancelado"}:
        _info("Ese envío ya finalizó y no puede modificarse.", color=Fore.YELLOW)
        press_enter()
        return
    if run.get("paused"):
        run["paused"] = False
        run["status"] = "en progreso" if run.get("processed") else "programado"
        run["next_run_at"] = _next_pending_at(run.get("events", []))
        _append_run_log(run, "La ejecución se reanudó manualmente.")
        ok("El envío se reanudó. Continuará respetando los delays configurados.")
    else:
        run["paused"] = True
        run["status"] = "en pausa"
        run["last_session_at"] = _now_iso()
        _append_run_log(run, "La ejecución se pausó manualmente.")
        ok("El envío quedó en pausa segura.")
    store.save()
    press_enter()


def _cancel_run(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a cancelar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status == "cancelado":
        _info("Ese envío ya está cancelado.")
        press_enter()
        return
    if status == "completado":
        _info("Ese envío ya finalizó por completo.", color=Fore.YELLOW)
        press_enter()
        return
    confirm = ask("Confirmá la cancelación permanente (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    for event in run.get("events", []):
        if (event.get("status") or "") == "pendiente":
            _reset_contact_for_cancellation(store, run, event)
    run["status"] = "cancelado"
    run["paused"] = False
    run["completed_at"] = _now_iso()
    run["next_run_at"] = None
    _refresh_run_counters(run)
    _append_run_log(run, "La ejecución fue cancelada manualmente.")
    store.save()
    ok("El envío se canceló sin afectar al resto del sistema.")
    press_enter()


def _select_run(
    store: WhatsAppDataStore,
    prompt: str,
    *,
    include_completed: bool = True,
) -> dict[str, Any] | None:
    runs = store.state.setdefault("message_runs", [])
    filtered: list[dict[str, Any]] = []
    for run in runs:
        status = (run.get("status") or "").lower()
        if not include_completed and status in {"completado", "cancelado"}:
            continue
        filtered.append(run)
    if not filtered:
        _info("No hay envíos disponibles para esta acción.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, run in enumerate(filtered, 1):
        total, sent, pending, cancelled, failed = _run_counts(run)
        print(
            f"{idx}) {_run_list_label(run)} → {_run_number_label(run)} | {_run_status_label(run)} | "
            f"{sent}/{total} enviados"
            + (f" • {failed} fallidos" if failed else "")
            + (f" • {cancelled} omitidos" if cancelled else "")
            + f" | Pendientes: {pending}"
        )
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(filtered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return filtered[idx - 1]


def _run_number_label(run: dict[str, Any]) -> str:
    return run.get("number_alias") or run.get("number_phone") or "(sin alias)"


def _run_list_label(run: dict[str, Any]) -> str:
    return run.get("list_alias") or "(sin lista)"


def _run_status_label(run: dict[str, Any]) -> str:
    status = (run.get("status") or "programado").lower()
    if status == "en pausa" or run.get("paused"):
        return "⏸ en pausa"
    if status == "en progreso":
        return "🟢 en progreso"
    if status == "programado":
        return "🕒 programado"
    if status == "completado":
        return "✅ completado"
    if status == "cancelado":
        return "✖ cancelado"
    return status


def _run_counts(run: dict[str, Any]) -> tuple[int, int, int, int, int]:
    events = run.get("events", [])
    total = len(events)
    sent = sum(1 for event in events if (event.get("status") or "") == "enviado")
    failed = sum(1 for event in events if (event.get("status") or "") == "fallido")
    pending = sum(1 for event in events if (event.get("status") or "") == "pendiente")
    cancelled = sum(
        1
        for event in events
        if (event.get("status") or "") in {"cancelado", "omitido"}
    )
    return total, sent, pending, cancelled, failed


def _confirmation_badge(event: dict[str, Any]) -> str:
    confirmation = (event.get("confirmation") or "no_enviado").lower()
    if confirmation == "leido":
        return "✔✔"
    if confirmation == "entregado":
        return "✔✔"
    if confirmation == "enviado":
        return "✔"
    return "✖"


def _format_event_status(event: dict[str, Any]) -> str:
    status = (event.get("status") or "").lower()
    reason = event.get("notes") or ""
    badge = _confirmation_badge(event)
    if status == "enviado":
        return f"{badge} Entregado"
    if status == "fallido":
        base = "✖ Fallido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "pendiente":
        return "⏳ Pendiente"
    if status == "omitido":
        base = "⚪ Omitido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "cancelado":
        base = "⏹ Cancelado"
        if reason:
            base += f" – {reason}"
        return base
    return status or "(desconocido)"


def _append_run_log(run: dict[str, Any], message: str) -> None:
    log = run.setdefault("log", [])
    log.append({"timestamp": _now_iso(), "message": message})
    if len(log) > 50:
        del log[:-50]


def _next_pending_at(events: Iterable[dict[str, Any]]) -> str | None:
    upcoming = [
        event.get("scheduled_at")
        for event in events
        if (event.get("status") or "") == "pendiente" and event.get("scheduled_at")
    ]
    if not upcoming:
        return None
    return min(upcoming)


def _refresh_run_counters(run: dict[str, Any]) -> None:
    events = run.get("events", [])
    run["total_contacts"] = len(events)
    run["processed"] = sum(
        1
        for event in events
        if (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
    )


def _emit_run_completion_screen(run: dict[str, Any]) -> None:
    total, sent, _, cancelled, failed = _run_counts(run)
    processed = sent + failed + cancelled
    number_label = _run_number_label(run)
    list_label = _run_list_label(run)
    print(_line())
    title("Resumen final de envío por WhatsApp")
    print(_line())
    print(f"Número usado: {number_label}")
    print(f"Lista operada: {list_label}")
    print(f"Éxitos: {sent}")
    print(f"Fallidos: {failed}")
    print(f"Omitidos/Cancelados: {cancelled}")
    print(f"Procesados: {processed}/{total}")
    print(_line())
    _log_structured(
        "whatsapp.run.completed.summary",
        run_id=run.get("id"),
        number=number_label,
        list_alias=list_label,
        sent=sent,
        failed=failed,
        cancelled=cancelled,
        total=total,
    )


def _has_active_message_runs(runs: list[dict[str, Any]]) -> bool:
    for run in runs:
        status = (run.get("status") or "").lower()
        if status not in {"completado", "cancelado"}:
            return True
    return False


def _has_active_background_automations(store: WhatsAppDataStore) -> bool:
    ai_configs = store.state.get("ai_automations", {})
    ai_active = any(
        isinstance(config, dict) and bool(config.get("active", False))
        for config in ai_configs.values()
    )
    followup = store.state.get("followup", {})
    followup_active = isinstance(followup, dict) and bool(followup.get("auto_enabled", False))
    return ai_active or followup_active


def _playwright_runtime_exists() -> bool:
    with _PLAYWRIGHT_RUNTIME_LOCK:
        return _PLAYWRIGHT_RUNTIME is not None


def _reconcile_runs(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    now = _now()
    changed = False
    for run in runs:
        status = (run.get("status") or "").lower()
        events = run.get("events", [])
        if not events:
            continue
        if status == "cancelado":
            continue
        if run.get("paused"):
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            continue

        session_limit = run.get("session_limit") or 0
        processed_now = 0
        for event in events:
            if (event.get("status") or "") != "pendiente":
                continue
            scheduled_at = _parse_iso(event.get("scheduled_at")) or now
            if scheduled_at > now:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if session_limit and processed_now >= session_limit:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if _deliver_event(store, run, event):
                processed_now += 1
                changed = True
        if session_limit and processed_now >= session_limit and any(
            (event.get("status") or "") == "pendiente" for event in events
        ):
            if not run.get("paused"):
                run["paused"] = True
                run["status"] = "en pausa"
                run["last_session_at"] = _now_iso()
                _append_run_log(
                    run,
                    "Se alcanzó el límite de mensajes por sesión. La ejecución se pausó automáticamente.",
                )
                changed = True
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            _refresh_run_counters(run)
            continue
        if all(
            (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
            for event in events
        ):
            if status != "completado":
                run["status"] = "completado"
                run["completed_at"] = _now_iso()
                run["paused"] = False
                run["next_run_at"] = None
                _append_run_log(
                    run,
                    "La ejecución finalizó y todos los mensajes fueron procesados.",
                )
                changed = True
            _refresh_run_counters(run)
            if not run.get("completion_notified"):
                _emit_run_completion_screen(run)
                run["completion_notified"] = True
                changed = True
            continue
        next_at = _next_pending_at(events)
        if run.get("next_run_at") != next_at:
            run["next_run_at"] = next_at
            changed = True
        if any((event.get("status") or "") in {"enviado", "fallido"} for event in events):
            if run.get("status") not in {"en pausa", "completado"}:
                run["status"] = "en progreso"
                changed = True
        _refresh_run_counters(run)
    try:
        if _run_ai_automations(store):
            changed = True
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error procesando auto-respuestas de WhatsApp: %s", exc)
        _log_structured("whatsapp.reconcile.autoreply.error", error=str(exc))
    try:
        if _run_followup_scheduler(store):
            changed = True
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error procesando seguimiento automático de WhatsApp: %s", exc)
        _log_structured("whatsapp.reconcile.followup.error", error=str(exc))
    if (
        not _has_active_message_runs(runs)
        and not _has_active_background_automations(store)
        and _playwright_runtime_exists()
    ):
        _shutdown_playwright_runtime()
        _log_structured(
            "whatsapp.playwright.runtime.closed",
            reason="no_active_runs",
        )
    if changed:
        store.save()


# ======================================================================
# ===== Session Management (Playwright) ================================

def _resolve_playwright_user_data_dir(sender: dict[str, Any] | None = None) -> Path:
    """Resuelve el directorio persistente para Playwright (cookies/localStorage)."""
    raw = ""
    if sender and isinstance(sender.get("session_path"), str):
        raw = (sender.get("session_path") or "").strip()
    if raw:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = (BASE / candidate).resolve()
        return candidate
    return DEFAULT_PLAYWRIGHT_SESSION_DIR


def _wa_automation_headless(sender: dict[str, Any] | None = None) -> bool:
    """
    Ejecuta automatizaciones en segundo plano por defecto.
    Override temporal: WPP_HEADFUL=1 fuerza modo visible.
    """
    if (os.getenv("WPP_HEADFUL", "").strip().lower() in {"1", "true", "yes", "on"}):
        return False
    if sender is None:
        return True
    return bool(sender.get("background_mode", True))


def _normalize_message(text: str) -> str:
    return "\n".join(line.strip() for line in (text or "").splitlines()).strip()


def _collect_playwright_alert_text(page: Any) -> str:
    texts: list[str] = []
    for selector in _WASelectors.ALERT_TEXT:
        try:
            locator = page.locator(selector)
            count = locator.count()
        except Exception:
            continue
        for idx in range(min(count, 5)):
            try:
                text = (locator.nth(idx).inner_text() or "").strip()
            except Exception:
                text = ""
            if text and text not in texts:
                texts.append(text)
    if texts:
        return " ".join(texts)
    try:
        body = page.locator("body").first
        snippet = (body.inner_text() or "").strip().splitlines()
        if snippet:
            return snippet[0].strip()
    except Exception:
        return ""
    return ""


def _wa_is_qr_visible(page: Any) -> bool:
    selector = _join_selectors(_WASelectors.QR_CANVAS)
    try:
        locator = page.locator(selector).first
        return locator.is_visible()
    except Exception:
        return False


def _wa_is_ready(page: Any) -> bool:
    selector = _join_selectors(_WASelectors.APP_READY)
    try:
        locator = page.locator(selector).first
        return locator.is_visible()
    except Exception:
        return False


def _wa_wait_for_login_state(page: Any, *, timeout_seconds: float) -> str:
    """Devuelve 'ready', 'qr' o 'timeout'."""
    deadline = time.time() + max(timeout_seconds, 0.1)
    while time.time() < deadline:
        if _wa_is_ready(page):
            return "ready"
        if _wa_is_qr_visible(page):
            return "qr"
        time.sleep(0.5)
    return "timeout"


def _wa_find_visible_chat_input(page: Any, *, timeout_ms: int = 45000) -> Any | None:
    """
    Selecciona el textbox visible de escritura del chat activo.
    Evita tomar inputs ocultos o cajas de búsqueda laterales.
    """
    selector = _join_selectors(_WASelectors.CHAT_INPUT)
    deadline = time.time() + max(timeout_ms, 1000) / 1000.0
    while time.time() < deadline:
        try:
            candidates = page.locator(selector)
            total = min(_wa_locator_count(candidates), 20)
        except Exception:
            total = 0
            candidates = None
        if not candidates or total <= 0:
            page.wait_for_timeout(250)
            continue

        for idx in range(total):
            candidate = candidates.nth(idx)
            try:
                if not candidate.is_visible():
                    continue
                in_footer = bool(
                    candidate.evaluate(
                        "el => Boolean(el && el.closest && el.closest('footer'))"
                    )
                )
                if not in_footer:
                    continue
                box = candidate.bounding_box() or {}
                width = float(box.get("width") or 0.0)
                height = float(box.get("height") or 0.0)
                if width < 40 or height < 10:
                    continue
                return candidate
            except Exception:
                continue

        page.wait_for_timeout(250)
    return None


def _register_playwright_runtime_cleanup() -> None:
    global _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED
    if _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED:
        return
    atexit.register(_shutdown_playwright_runtime)
    _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED = True


def _shutdown_playwright_runtime() -> None:
    global _PLAYWRIGHT_RUNTIME
    with _PLAYWRIGHT_RUNTIME_LOCK:
        runtime = _PLAYWRIGHT_RUNTIME
        _PLAYWRIGHT_RUNTIME = None
    _close_playwright_runtime_instance(runtime)


=======
            if isinstance(item, dict)
        ]
        merged["ai_automations"] = {
            key: self._ensure_ai_config(value)
            for key, value in dict(data.get("ai_automations", {})).items()
        }
        merged["instagram"] = self._ensure_instagram_config(data.get("instagram", {}))
        merged["followup"] = self._ensure_followup_config(data.get("followup", {}))
        merged["payments"] = self._ensure_payments_config(data.get("payments", {}))
        return merged

    # ------------------------------------------------------------------
    def _normalize_number_sessions(self, numbers: dict[str, dict[str, Any]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in numbers.values():
            path = str(item.get("session_path") or "").strip()
            if not path:
                continue
            grouped.setdefault(path, []).append(item)
        for _path, items in grouped.items():
            if len(items) <= 1:
                continue
            # Conserva el primer perfil y separa el resto para evitar operar con el número incorrecto.
            for conflicted in items[1:]:
                number_id = conflicted.get("id") or str(uuid.uuid4())
                conflicted["session_path"] = str(_playwright_session_dir_for_number(number_id))
                conflicted["connected"] = False
                conflicted["connection_state"] = "pendiente"
                conflicted.setdefault("session_notes", []).append(
                    {
                        "created_at": _now_iso(),
                        "text": "Se detectó sesión compartida con otro número. Vinculación requerida para aislar la operación.",
                    }
                )

    # ------------------------------------------------------------------
    def _ensure_number_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            generated_id = str(uuid.uuid4())
            return {
                "id": generated_id,
                "alias": "",
                "phone": "",
                "connected": False,
                "last_connected_at": None,
                "session_notes": [],
                "keep_alive": True,
                "session_path": str(_playwright_session_dir_for_number(generated_id)),
                "connection_method": "playwright",
                "background_mode": True,
            }
        method = str(value.get("connection_method") or "playwright").strip().lower()
        if method != "playwright":
            method = "playwright"
        number_id = value.get("id") or str(uuid.uuid4())
        raw_session_path = (value.get("session_path") or "").strip()
        if raw_session_path:
            session_path = raw_session_path
        else:
            session_path = str(_playwright_session_dir_for_number(number_id))
        return {
            "id": number_id,
            "alias": value.get("alias", ""),
            "phone": value.get("phone", ""),
            "connected": bool(value.get("connected", False)),
            "last_connected_at": value.get("last_connected_at"),
            "session_notes": list(value.get("session_notes", [])),
            "keep_alive": bool(value.get("keep_alive", True)),
            "session_path": session_path,
            "qr_snapshot": value.get("qr_snapshot"),
            "last_qr_capture_at": value.get("last_qr_capture_at"),
            "connection_state": value.get("connection_state", "pendiente"),
            "connection_method": method,
            "background_mode": bool(value.get("background_mode", True)),
        }

    # ------------------------------------------------------------------
    def _ensure_contact(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        history = [entry for entry in raw.get("history", []) if isinstance(entry, dict)]
        validation = _ensure_validation_entry(raw.get("validation"), raw.get("number", ""))
        delivery_log = _ensure_delivery_log(raw.get("delivery_log"))
        followup_stage = raw.get("followup_stage", 0)
        try:
            followup_stage_int = int(followup_stage)
        except Exception:
            followup_stage_int = 0
        return {
            "name": raw.get("name", ""),
            "number": validation.get("normalized") or raw.get("number", ""),
            "status": raw.get("status", "sin mensaje"),
            "last_message_at": raw.get("last_message_at"),
            "last_response_at": raw.get("last_response_at"),
            # Estado ampliado para automatizaciones (auto-reply / follow-ups).
            "last_message_sent": raw.get("last_message_sent", ""),
            "last_message_sent_at": raw.get("last_message_sent_at") or raw.get("last_message_at"),
            "last_message_received": raw.get("last_message_received", ""),
            "last_message_received_at": raw.get("last_message_received_at") or raw.get("last_response_at"),
            "last_sender_id": raw.get("last_sender_id"),
            "followup_stage": followup_stage_int,
            "last_followup_at": raw.get("last_followup_at"),
            "last_payment_at": raw.get("last_payment_at"),
            "access_sent_at": raw.get("access_sent_at"),
            "notes": raw.get("notes", ""),
            "history": history,
            "validation": validation,
            "delivery_log": delivery_log,
        }

    # ------------------------------------------------------------------
    def _ensure_contact_list_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        contacts = [self._ensure_contact(item) for item in value.get("contacts", [])]
        return {
            "alias": value.get("alias", ""),
            "created_at": value.get("created_at") or _now_iso(),
            "contacts": contacts,
            "notes": value.get("notes", ""),
        }

    # ------------------------------------------------------------------
    def _ensure_message_run(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}

        def _to_int(raw: Any, default: int = 0) -> int:
            try:
                if raw in (None, ""):
                    return default
                return int(raw)
            except Exception:
                return default

        def _to_float(raw: Any, default: float = 0.0) -> float:
            try:
                if raw in (None, ""):
                    return default
                return float(raw)
            except Exception:
                return default

        delay = value.get("delay") or {}
        events = []
        for item in value.get("events", []):
            if not isinstance(item, dict):
                continue
            events.append(
                {
                    "contact": item.get("contact"),
                    "name": item.get("name"),
                    "message": item.get("message", ""),
                    "scheduled_at": item.get("scheduled_at"),
                    "status": item.get("status", "pendiente"),
                    "delivered_at": item.get("delivered_at"),
                    "notes": item.get("notes", ""),
                    "confirmation": item.get("confirmation", "no_enviado"),
                    "validation_status": item.get("validation_status"),
                    "error_code": item.get("error_code"),
                }
            )

        log = [
            {
                "timestamp": entry.get("timestamp", _now_iso()),
                "message": entry.get("message", ""),
            }
            for entry in value.get("log", [])
            if isinstance(entry, dict)
        ]

        message_template = value.get("message_template", "") or value.get("template", "")
        message_preview = value.get("message_preview", "")
        if message_template and not message_preview:
            message_preview = textwrap.shorten(message_template, width=90, placeholder="…")

        return {
            "id": value.get("id", str(uuid.uuid4())),
            "number_id": value.get("number_id"),
            "number_alias": value.get("number_alias"),
            "number_phone": value.get("number_phone"),
            "list_alias": value.get("list_alias"),
            "created_at": value.get("created_at") or _now_iso(),
            "status": value.get("status", "programado"),
            "paused": bool(value.get("paused", False)),
            "session_limit": _to_int(value.get("session_limit"), 0),
            "total_contacts": _to_int(value.get("total_contacts"), len(events)),
            "processed": _to_int(value.get("processed"), 0),
            "completed_at": value.get("completed_at"),
            "last_activity_at": value.get("last_activity_at"),
            "next_run_at": value.get("next_run_at"),
            "delay": {
                "min": _to_float(delay.get("min"), 5.0),
                "max": _to_float(delay.get("max"), 12.0),
            },
            "message_template": message_template,
            "message_preview": message_preview,
            "events": events,
            "max_contacts": _to_int(value.get("max_contacts"), 0),
            "last_session_at": value.get("last_session_at"),
            "completion_notified": bool(value.get("completion_notified", False)),
            "log": log,
        }

    # ------------------------------------------------------------------
    def _ensure_ai_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        polling_interval_seconds = value.get("polling_interval_seconds", 60)
        scan_throttle_seconds = value.get("scan_throttle_seconds", _AUTOREPLY_DEFAULT_THROTTLE_SECONDS)
        scan_jitter_seconds = value.get("scan_jitter_seconds", _AUTOREPLY_DEFAULT_JITTER_SECONDS)
        try:
            polling_interval_seconds = int(polling_interval_seconds)
        except Exception:
            polling_interval_seconds = 60
        try:
            scan_throttle_seconds = int(scan_throttle_seconds)
        except Exception:
            scan_throttle_seconds = _AUTOREPLY_DEFAULT_THROTTLE_SECONDS
        try:
            scan_jitter_seconds = int(scan_jitter_seconds)
        except Exception:
            scan_jitter_seconds = _AUTOREPLY_DEFAULT_JITTER_SECONDS
        return {
            "active": bool(value.get("active", False)),
            "prompt": value.get("prompt", ""),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 15.0)),
            },
            "send_audio": bool(value.get("send_audio", False)),
            "last_updated_at": value.get("last_updated_at"),
            "last_scan_at": value.get("last_scan_at"),
            "next_scan_at": value.get("next_scan_at"),
            "polling_interval_seconds": max(_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS, polling_interval_seconds),
            "scan_throttle_seconds": max(10, scan_throttle_seconds),
            "scan_jitter_seconds": max(0, scan_jitter_seconds),
        }

    # ------------------------------------------------------------------
    def _ensure_instagram_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        captures = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "name": item.get("name", ""),
                "number": item.get("number", ""),
                "source": item.get("source", "Instagram"),
                "captured_at": item.get("captured_at", _now_iso()),
                "message_sent": bool(item.get("message_sent", False)),
                "message_sent_at": item.get("message_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("captures", [])
            if isinstance(item, dict)
        ]
        return {
            "active": bool(value.get("active", False)),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 12.0)),
            },
            "message": value.get(
                "message",
                "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            ),
            "captures": captures,
            "last_reviewed_at": value.get("last_reviewed_at"),
        }

    # ------------------------------------------------------------------
    def _ensure_followup_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        history = [item for item in value.get("history", []) if isinstance(item, dict)]
        max_stage = value.get("max_stage", 2)
        try:
            max_stage = int(max_stage)
        except Exception:
            max_stage = 2
        auto_mode = (value.get("auto_mode", "manual") or "manual").strip().lower()
        if not auto_mode:
            auto_mode = "manual"
        return {
            "default_wait_minutes": int(value.get("default_wait_minutes", 120)),
            "manual_message": value.get(
                "manual_message", "Hola {nombre}, ¿pudiste ver mi mensaje anterior?"
            ),
            "ai_prompt": value.get(
                "ai_prompt",
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar.",
            ),
            "auto_enabled": bool(value.get("auto_enabled", False)),
            "auto_mode": auto_mode,
            "active_number_id": value.get("active_number_id", ""),
            "max_stage": max(1, max_stage),
            "last_auto_run_at": value.get("last_auto_run_at"),
            "history": history,
        }

    # ------------------------------------------------------------------
    def _ensure_payments_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        pending = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "evidence": item.get("evidence", ""),
                "keywords": list(item.get("keywords", [])),
                "status": item.get("status", "pendiente"),
                "created_at": item.get("created_at", _now_iso()),
                "validated_at": item.get("validated_at"),
                "welcome_sent_at": item.get("welcome_sent_at"),
                "alert_sent_at": item.get("alert_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("pending", [])
            if isinstance(item, dict)
        ]
        history = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "status": item.get("status", "completado"),
                "completed_at": item.get("completed_at", _now_iso()),
                "notes": item.get("notes", ""),
            }
            for item in value.get("history", [])
            if isinstance(item, dict)
        ]
        return {
            "admin_number": value.get("admin_number", ""),
            "welcome_message": value.get(
                "welcome_message", "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:"
            ),
            "access_link": value.get("access_link", "https://tusitio.com/accesos"),
            "pending": pending,
            "history": history,
        }

    # ------------------------------------------------------------------
    def save(self) -> None:
        serialized = json.dumps(self.state, ensure_ascii=False, indent=2)
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with self._io_lock:
            tmp_path.write_text(serialized, encoding="utf-8")
            tmp_path.replace(self.path)

    # ------------------------------------------------------------------
    # Helper methods ----------------------------------------------------
    def iter_numbers(self) -> Iterator[dict[str, Any]]:
        for item in self.state.get("numbers", {}).values():
            yield item

    def iter_lists(self) -> Iterator[tuple[str, dict[str, Any]]]:
        for alias, data in self.state.get("contact_lists", {}).items():
            yield alias, data

    def find_number(self, number_id: str) -> dict[str, Any] | None:
        return self.state.get("numbers", {}).get(number_id)

    def find_list(self, alias: str) -> dict[str, Any] | None:
        return self.state.get("contact_lists", {}).get(alias)


# ----------------------------------------------------------------------
# Presentación y helpers de impresión

def _line() -> str:
    return full_line(color=Fore.BLUE, bold=True)


def _info(msg: str, *, color: str = Fore.CYAN, bold: bool = False) -> None:
    print(style_text(msg, color=color, bold=bold))


def _subtitle(msg: str) -> None:
    print(style_text(msg, color=Fore.MAGENTA, bold=True))


def _format_delay(delay: dict[str, float]) -> str:
    return f"{delay['min']:.1f}s – {delay['max']:.1f}s"


# ======================================================================
# ===== Selectors =======================================================

def _join_selectors(selectors: Iterable[str]) -> str:
    return ", ".join([item for item in selectors if item])


class _WASelectors:
    # Login / sesión
    QR_CANVAS = [
        "canvas[data-testid='qrcode']",
        "canvas",
    ]
    APP_READY = [
        "div[data-testid='pane-side']",
        "div[data-testid='chat-list']",
        "div[data-testid='chat-list-search']",
        "div[role='grid']",
        "div[data-testid='app-title']",
    ]

    # Chat list / rows / unread
    PANE_SIDE = [
        "div[data-testid='pane-side']",
        "div[data-testid='chat-list']",
        "div[role='grid']",
    ]
    CHAT_ROW_CANDIDATES = [
        "div[data-testid='cell-frame-container']",
        "div[role='row']",
        "div[role='listitem']",
    ]
    UNREAD_BADGE = [
        "span[data-testid='icon-unread-count']",
        "span[aria-label*='unread']",
        "span[aria-label*='Unread']",
        "span[aria-label*='no leído']",
        "span[aria-label*='no leídos']",
        "span[aria-label*='mensaje no leído']",
        "span[aria-label*='mensajes no leídos']",
    ]
    CHAT_TITLE_IN_ROW = [
        "span[dir='auto']",
        "span[title]",
    ]
    ACTIVE_CHAT_TITLE = [
        "header span[title]",
        "header div[role='button'] span[title]",
        "header span[dir='auto']",
    ]

    # Conversación
    CHAT_INPUT = [
        "footer div[contenteditable='true'][data-testid='conversation-compose-box-input']",
        "footer div[contenteditable='true'][role='textbox']",
        "footer div[contenteditable='true']",
    ]
    SEND_BUTTON = [
        "button[data-testid='compose-btn-send']",
        "button:has([data-icon='send'])",
        "button:has([aria-label*='Send'])",
        "button:has([aria-label*='Enviar'])",
    ]

    MESSAGE_CONTAINER = [
        "div[data-testid='msg-container']",
        "div.message-in, div.message-out",
    ]
    BUBBLE = [
        "div.message-in",
        "div.message-out",
        "div[data-testid='msg-container'].message-in",
        "div[data-testid='msg-container'].message-out",
        "div[data-testid='msg-container'] div.message-in",
        "div[data-testid='msg-container'] div.message-out",
    ]
    BUBBLE_IN = [
        "div.message-in",
        "div[data-testid='msg-container'].message-in",
        "div[data-testid='msg-container'] div.message-in",
    ]
    BUBBLE_OUT = [
        "div.message-out",
        "div[data-testid='msg-container'].message-out",
        "div[data-testid='msg-container'] div.message-out",
    ]
    BUBBLE_TEXT = [
        "span[data-testid='conversation-text']",
        "span.selectable-text.copyable-text span",
        "div.copyable-text span.selectable-text span",
    ]

    # Confirmaciones
    CHECK_READ = "svg[data-testid='msg-dblcheck-read']"
    CHECK_DELIVERED = "svg[data-testid='msg-dblcheck']"
    CHECK_SENT = "svg[data-testid='msg-check']"

    # Errores / estados
    ALERT_TEXT = [
        "div[data-testid='app-state-message']",
        "div[data-testid='alert-qr-text']",
        "div[data-testid='empty-state-title']",
        "div[role='dialog']",
        "div[data-testid='popup-controls-ok']",
    ]


# ----------------------------------------------------------------------
# Runner de envios en segundo plano

_MESSAGE_RUNNER: _MessageRunner | None = None
_MESSAGE_RUNNER_MIN_SLEEP = 2.0
_MESSAGE_RUNNER_MAX_SLEEP = 8.0
_MESSAGE_RUNNER_IDLE_SLEEP = 20.0


class _MessageRunner:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        _ensure_whatsapp_logging()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="whatsapp-message-runner", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        _shutdown_playwright_runtime()

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _loop(self) -> None:
        while not self._stop.is_set():
            sleep_for = _MESSAGE_RUNNER_IDLE_SLEEP
            try:
                _ensure_whatsapp_logging()
                store = WhatsAppDataStore()
                _reconcile_runs(store)
                sleep_for = _message_runner_sleep(store)
            except Exception as exc:
                logger.exception("Error en whatsapp-message-runner: %s", exc)
                _log_structured("whatsapp.runner.error", error=str(exc))
                sleep_for = _MESSAGE_RUNNER_IDLE_SLEEP
            self._stop.wait(sleep_for)


def _message_runner_sleep(store: WhatsAppDataStore) -> float:
    next_runs: list[datetime] = []
    for run in store.state.get("message_runs", []):
        status = (run.get("status") or "").lower()
        if status in {"completado", "cancelado"}:
            continue
        if run.get("paused"):
            continue
        next_at = _parse_iso(run.get("next_run_at"))
        if next_at:
            next_runs.append(next_at)
    if not next_runs:
        idle = _MESSAGE_RUNNER_IDLE_SLEEP + random.uniform(*_MESSAGE_RUNNER_IDLE_JITTER)
        return max(_MESSAGE_RUNNER_IDLE_SLEEP, idle)
    next_at = min(next_runs)
    delta = (next_at - _now()).total_seconds()
    if delta <= 0:
        base = _MESSAGE_RUNNER_MIN_SLEEP
    else:
        base = min(max(delta, _MESSAGE_RUNNER_MIN_SLEEP), _MESSAGE_RUNNER_MAX_SLEEP)
    jittered = base + random.uniform(*_MESSAGE_RUNNER_ACTIVE_JITTER)
    return min(max(jittered, _MESSAGE_RUNNER_MIN_SLEEP), _MESSAGE_RUNNER_MAX_SLEEP + 1.5)


def _ensure_message_runner() -> None:
    global _MESSAGE_RUNNER
    if _MESSAGE_RUNNER is None:
        _MESSAGE_RUNNER = _MessageRunner()
        atexit.register(_MESSAGE_RUNNER.stop)
    _MESSAGE_RUNNER.start()


def _message_runner_active() -> bool:
    return bool(_MESSAGE_RUNNER and _MESSAGE_RUNNER.is_running())


def _sync_message_runs(store: WhatsAppDataStore) -> None:
    if _message_runner_active():
        store.reload()
    else:
        _reconcile_runs(store)


# ----------------------------------------------------------------------
# Menú principal del módulo

def menu_whatsapp() -> None:
    _ensure_whatsapp_logging()
    store = WhatsAppDataStore()
    _ensure_message_runner()
    while True:
        _sync_message_runs(store)
        banner()
        title("Automatización por WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Conectar número de WhatsApp")
        print("2) Importar lista de contactos")
        print("3) Enviar mensajes a la lista")
        print("4) Automatizar respuestas con IA")
        print("5) Capturar números desde Instagram")
        print("6) Seguimiento automatizado a no respondidos")
        print("7) Gestión de pagos y entrega de accesos")
        print("8) Estado de contactos y actividad")
        print("9) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _connect_number(store)
        elif op == "2":
            _import_contacts(store)
        elif op == "3":
            _send_messages(store)
        elif op == "4":
            _configure_ai_responses(store)
        elif op == "5":
            _instagram_capture(store)
        elif op == "6":
            _followup_manager(store)
        elif op == "7":
            _payments_menu(store)
        elif op == "8":
            _contacts_state(store)
        elif op == "9":
            break
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 1) Conectar número ----------------------------------------------------

def _print_numbers_summary(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("Aún no hay números conectados.")
        return
    _subtitle("Números activos")
    for item in sorted(numbers, key=lambda n: n.get("alias")):  # type: ignore[arg-type]
        alias = item.get("alias") or item.get("phone")
        if item.get("connected"):
            status = "🟢 Verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 Error de vinculación"
        else:
            status = "⚪ Pendiente"
        last = item.get("last_connected_at") or "(sin actividad)"
        print(
            f" • {alias} ({item.get('phone')}) - {status} – última conexión: {last}"
        )


def _select_connection_backend() -> str | None:
    while True:
        print(_line())
        _subtitle("Método de vinculación")
        print("1) Playwright (Chromium persistente)")
        print("2) Volver\n")
        choice = ask("Opción: ").strip()
        if choice == "2":
            return None
        if choice == "1":
            return "playwright"
        _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)


def _connect_number(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Conectar número de WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Vincular nuevo número")
        print("2) Eliminar número vinculado")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _link_new_number(store)
        elif op == "2":
            _remove_linked_number(store)
        elif op == "3":
            return
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


def _link_new_number(store: WhatsAppDataStore) -> None:
    alias = ask("Alias interno para reconocer el número: ").strip()
    phone = ask("Número en formato internacional (ej: +54911...): ").strip()
    if not phone:
        _info("No se ingresó número.", color=Fore.YELLOW)
        press_enter()
        return
    note = ask("Nota interna u observación (opcional): ").strip()
    backend = _select_connection_backend()
    if backend is None:
        _info("No se inició ninguna vinculación.")
        press_enter()
        return

    session_id = str(uuid.uuid4())
    session_dir = _playwright_session_dir_for_number(session_id)
    session_dir.mkdir(parents=True, exist_ok=True)

    _info("Preparando Playwright (Chromium) para WhatsApp Web...")
    success, snapshot, details = _initiate_whatsapp_web_login(session_dir, backend)
    if details:
        _info(details)

    if not success:
        confirm = ask(
            "¿Lograste vincular la sesión desde la ventana abierta? (s/N): "
        ).strip().lower()
        if confirm == "s":
            success = True

    state = store.state.setdefault("numbers", {})
    record = {
        "id": session_id,
        "alias": alias or phone,
        "phone": phone,
        "connected": success,
        "last_connected_at": _now_iso() if success else None,
        "session_notes": [
            {
                "created_at": _now_iso(),
                "text": note
                or "Sesión gestionada mediante Playwright (Chromium).",
            }
        ],
        "keep_alive": True,
        "session_path": str(session_dir),
        "qr_snapshot": str(snapshot) if snapshot else None,
        "last_qr_capture_at": _now_iso() if snapshot else None,
        "connection_state": "verificado" if success else "pendiente",
        "connection_method": backend,
        "background_mode": True,
    }
    if not success:
        record["session_notes"].append(
            {
                "created_at": _now_iso(),
                "text": "La vinculación automática no se completó. Reintentar desde el menú.",
            }
        )
        record["connection_state"] = "fallido"
    state[session_id] = record
    store.save()

    if success:
        ok("Sesión verificada y lista para operar en segundo plano.")
    else:
        _info(
            "No se confirmó la vinculación. Podés reintentar el proceso desde este mismo menú.",
            color=Fore.YELLOW,
        )
    press_enter()


def _remove_linked_number(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("No hay números registrados para eliminar.", color=Fore.YELLOW)
        press_enter()
        return
    print(_line())
    _subtitle("Seleccioná el número a desvincular")
    ordered = sorted(numbers, key=lambda n: n.get("alias"))
    for idx, item in enumerate(ordered, 1):
        alias = item.get("alias") or item.get("phone")
        status = "🟢" if item.get("connected") else "⚪"
        print(f"{idx}) {alias} ({item.get('phone')}) - {status}")
    idx = ask_int("Número a eliminar: ", min_value=1)
    if idx > len(ordered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return
    selected = ordered[idx - 1]
    alias = selected.get("alias") or selected.get("phone")
    confirm = ask(f"Confirmá eliminación de '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    state = store.state.setdefault("numbers", {})
    state.pop(selected.get("id"), None)
    store.save()
    ok(f"Se eliminó el número vinculado '{alias}'.")
    press_enter()


def _initiate_whatsapp_web_login(
    session_dir: Path, backend: str
) -> tuple[bool, Path | None, str]:
    if backend != "playwright":
        return False, None, "Solo se admite Playwright para la vinculación automática."
    return _initiate_with_playwright(session_dir)


def _initiate_with_playwright(session_dir: Path) -> tuple[bool, Path | None, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from src.playwright_service import resolve_playwright_executable
        from src.runtime.playwright_runtime import launch_sync_persistent_context, sync_playwright_context
    except ImportError:
        return (
            False,
            None,
            "Playwright no está instalado. Ejecutá 'pip install playwright' y luego 'playwright install'.",
        )

    snapshot_path = session_dir / "qr.png"
    info_messages: list[str] = []
    success = False

    try:
        with _PLAYWRIGHT_LOCK:
            # Evita conflicto de perfil si hay runtime compartido activo.
            _shutdown_playwright_runtime()
            with sync_playwright_context():
                executable = resolve_playwright_executable(headless=False)
                context = launch_sync_persistent_context(
                    user_data_dir=session_dir,
                    headless=False,
                    executable_path=executable,
                    args=[
                        "--disable-notifications",
                        "--disable-infobars",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                    viewport={"width": 1280, "height": 720},
                )
                try:
                    page = context.pages[0] if context.pages else context.new_page()
                    page.set_default_timeout(60000)
                    page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")

                    state = _wa_wait_for_login_state(page, timeout_seconds=15.0)
                    if state == "ready":
                        success = True
                        info_messages.append("La sesión ya estaba vinculada en este perfil.")
                    else:
                        qr_selector = _join_selectors(_WASelectors.QR_CANVAS)
                        if state == "qr":
                            try:
                                qr_element = page.locator(qr_selector).first
                                qr_element.screenshot(path=str(snapshot_path))
                            except Exception:
                                try:
                                    page.screenshot(path=str(snapshot_path))
                                except Exception:
                                    pass
                            if snapshot_path.exists():
                                info_messages.append(
                                    f"Se guardó una captura del código QR en {snapshot_path}."
                                )
                            info_messages.append(
                                "Escaneá el código con tu celular para completar la vinculación."
                            )
                        else:
                            try:
                                page.screenshot(path=str(snapshot_path))
                            except Exception:
                                pass

                        ready_selector = _join_selectors(_WASelectors.APP_READY)
                        try:
                            page.wait_for_selector(ready_selector, timeout=180000)
                            success = True
                        except PlaywrightTimeoutError:
                            success = False

                        # Si WhatsApp cambió y el QR ya no está visible, asumimos éxito.
                        if not success:
                            try:
                                if not page.locator(qr_selector).first.is_visible():
                                    success = True
                            except Exception:
                                pass
                finally:
                    try:
                        context.close()
                    except Exception:
                        pass
    except Exception:
        info_messages.append(
            "No se pudo automatizar la conexión. Verificá que Playwright tenga los navegadores instalados."
        )
        success = False

    message = " ".join(info_messages)
    return success, snapshot_path if snapshot_path.exists() else None, message



# ----------------------------------------------------------------------
# 2) Importar contactos -------------------------------------------------

def _import_contacts(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Importar lista de contactos")
        print(_line())
        existing = list(store.iter_lists())
        if existing:
            _subtitle("Listas registradas")
            for alias, data in existing:
                total = len(data.get("contacts", []))
                print(f" • {alias} ({total} contactos)")
            print(_line())
        print("1) Carga manual")
        print("2) Importar desde CSV (nombre, número)")
        print("3) Ver listas cargadas")
        print("4) Eliminar una lista cargada")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _manual_contacts_entry(store)
        elif op == "2":
            _csv_contacts_entry(store)
        elif op == "3":
            _show_loaded_contacts(store)
        elif op == "4":
            _delete_contact_list(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida. Probá otra vez.", color=Fore.YELLOW)
            press_enter()


def _manual_contacts_entry(store: WhatsAppDataStore) -> None:
    alias = ask("Nombre o alias de la lista: ").strip() or f"lista-{_now().strftime('%H%M%S')}"
    _info("Ingresá número y nombre separados por coma. Línea vacía para terminar.")
    contacts = []
    while True:
        raw = ask("Contacto: ").strip()
        if not raw:
            break
        if "," in raw:
            number, name = [part.strip() for part in raw.split(",", 1)]
        else:
            number, name = raw, ""
        contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se agregaron contactos.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Se registraron {summary['stored']} contactos en la lista '{alias}'."
        )
    else:
        _info("No se guardaron contactos. Revisá los números proporcionados.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _csv_contacts_entry(store: WhatsAppDataStore) -> None:
    path = ask("Ruta del archivo CSV: ").strip()
    if not path:
        _info("No se indicó archivo.", color=Fore.YELLOW)
        press_enter()
        return
    csv_path = Path(path)
    if not csv_path.exists():
        _info("El archivo indicado no existe.", color=Fore.YELLOW)
        press_enter()
        return
    alias = ask("Alias para la lista importada: ").strip() or csv_path.stem
    contacts: list[dict[str, str]] = []
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            name = (row[0] or "").strip()
            number = (row[1] if len(row) > 1 else "").strip()
            if not number and name:
                number, name = name, number
            if not number:
                continue
            contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se encontraron contactos válidos en el CSV.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Importación completada. {summary['stored']} registros cargados en '{alias}'."
        )
    else:
        _info("El archivo no aportó contactos válidos tras la validación.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _select_existing_list(
    store: WhatsAppDataStore, prompt: str
) -> tuple[str, dict[str, Any]] | None:
    lists = sorted(list(store.iter_lists()), key=lambda item: item[0].lower())
    if not lists:
        _info("Aún no hay listas registradas.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return lists[idx - 1]


def _show_loaded_contacts(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Elegí la lista a visualizar")
    if not selection:
        return
    alias, data = selection
    banner()
    title(f"Contactos registrados en '{alias}'")
    print(_line())
    contacts = data.get("contacts", [])
    if not contacts:
        _info("La lista no tiene contactos cargados.", color=Fore.YELLOW)
        press_enter()
        return
    for contact in contacts:
        name = contact.get("name") or contact.get("number")
        print(f"• {name} - {contact.get('number')}")
    press_enter()


def _delete_contact_list(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Seleccioná la lista a eliminar")
    if not selection:
        return
    alias, _ = selection
    confirm = ask(f"Confirmá eliminación de la lista '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    store.state.setdefault("contact_lists", {}).pop(alias, None)
    store.save()
    ok(f"Se eliminó la lista '{alias}'.")
    press_enter()


def _persist_contacts(
    store: WhatsAppDataStore, alias: str, contacts: Iterable[dict[str, str]]
) -> dict[str, int]:
    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    invalid_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for item in contacts:
        number = item.get("number", "")
        if not number:
            continue
        validation = _validate_phone_number(number)
        normalized_number = validation.get("normalized") or validation.get("raw", "")
        contact = {
            "name": item.get("name", "") or normalized_number,
            "number": normalized_number,
            "status": "sin mensaje",
            "last_message_at": None,
            "last_response_at": None,
            "last_message_sent": "",
            "last_message_sent_at": None,
            "last_message_received": "",
            "last_message_received_at": None,
            "last_sender_id": None,
            "followup_stage": 0,
            "last_followup_at": None,
            "last_payment_at": None,
            "access_sent_at": None,
            "notes": "",
            "history": [
                {
                    "type": "validation",
                    "status": validation["status"],
                    "checked_at": validation["checked_at"],
                    "message": validation["message"],
                }
            ],
            "validation": validation,
            "delivery_log": [],
        }
        if validation["status"] == "invalid":
            invalid_entries.append((contact, validation))
        elif validation["status"] == "warning":
            warning_entries.append((contact, validation))
        prepared.append((contact, validation))

    if not prepared:
        return {"stored": 0, "invalid": 0, "warnings": 0, "skipped": 0}

    stored_entries = list(prepared)
    skipped_invalid = 0

    if invalid_entries:
        _info("Se detectaron números inválidos y podrían no existir en WhatsApp:", color=Fore.YELLOW)
        for contact, validation in invalid_entries[:5]:
            print(f" • {contact.get('name')} - {validation.get('raw')} ({validation.get('message')})")
        if len(invalid_entries) > 5:
            print(f"   ... y {len(invalid_entries) - 5} más")
        choice = ask("¿Deseás conservarlos igualmente? (s/N): ").strip().lower()
        if choice != "s":
            to_remove = {id(entry[0]) for entry in invalid_entries}
            stored_entries = [entry for entry in stored_entries if id(entry[0]) not in to_remove]
            skipped_invalid = len(invalid_entries)
            _info(
                "Se omitieron los números inválidos para evitar errores futuros.",
                color=Fore.YELLOW,
            )

    if not stored_entries:
        return {
            "stored": 0,
            "invalid": len(invalid_entries),
            "warnings": len(warning_entries),
            "skipped": skipped_invalid,
        }

    if warning_entries:
        _info(
            "Algunos números no tienen formato internacional completo. Se marcarán con advertencia.",
            color=Fore.YELLOW,
        )

    items = [entry[0] for entry in stored_entries]
    lists = store.state.setdefault("contact_lists", {})
    current = lists.get(alias)
    if current:
        current_contacts = current.get("contacts", [])
        current_contacts.extend(items)
        current["contacts"] = current_contacts
        if not current.get("alias"):
            current["alias"] = alias
    else:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": items,
            "notes": "",
        }
    store.save()
    return {
        "stored": len(items),
        "invalid": len(invalid_entries),
        "warnings": len(warning_entries),
        "skipped": skipped_invalid,
    }


# ----------------------------------------------------------------------
# 3) Envío de mensajes --------------------------------------------------

def _send_messages(store: WhatsAppDataStore) -> None:
    if not list(store.iter_numbers()):
        _info(
            "Necesitás vincular al menos un número antes de enviar mensajes.",
            color=Fore.YELLOW,
        )
        press_enter()
        return
    if not list(store.iter_lists()):
        _info("Cargá primero una lista de contactos.", color=Fore.YELLOW)
        press_enter()
        return

    while True:
        _sync_message_runs(store)
        banner()
        title("Programación de envíos por WhatsApp")
        print(_line())
        _print_runs_overview(store)
        print(_line())
        print("1) Programar nuevo envío automático")
        print("2) Ver detalle de un envío programado")
        print("3) Pausar o reanudar un envío")
        print("4) Cancelar un envío")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _plan_message_run(store)
        elif op == "2":
            _show_run_detail(store)
        elif op == "3":
            _toggle_run_pause(store)
        elif op == "4":
            _cancel_run(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _plan_message_run(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    contact_list = _choose_contact_list(store)
    if not contact_list:
        return
    contacts = list(contact_list.get("contacts", []))
    if not contacts:
        _info("La lista no tiene contactos.", color=Fore.YELLOW)
        press_enter()
        return

    max_contacts = ask_int(
        "¿Cuántos contactos incluir en este envío? (0 = todos): ",
        min_value=0,
        default=0,
    )
    if max_contacts and max_contacts < len(contacts):
        targets = contacts[:max_contacts]
    else:
        targets = contacts
    if not targets:
        _info("No se seleccionaron contactos para el envío.", color=Fore.YELLOW)
        press_enter()
        return

    store_dirty = False
    invalid_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for contact in list(targets):
        previous_validation = contact.get("validation", {})
        validation = _update_contact_validation(contact)
        if validation != previous_validation:
            store_dirty = True
        if validation["status"] == "invalid":
            invalid_targets.append((contact, validation))
        elif validation["status"] == "warning":
            warning_targets.append((contact, validation))

    if invalid_targets:
        _info(
            "Hay contactos con números inválidos que WhatsApp rechazará.",
            color=Fore.YELLOW,
        )
        for contact, validation in invalid_targets[:5]:
            print(
                f" • {contact.get('name')} ({validation.get('raw')}) → {validation.get('message')}"
            )
        if len(invalid_targets) > 5:
            print(f"   ... y {len(invalid_targets) - 5} más")
        choice = ask(
            "¿Deseás programar igual para estos contactos inválidos? (s/N): "
        ).strip().lower()
        if choice != "s":
            to_exclude = {id(entry[0]) for entry in invalid_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se excluyeron los contactos con números inválidos del envío.",
                color=Fore.YELLOW,
            )
        else:
            for contact, _ in invalid_targets:
                contact["status"] = "observado"
                history = contact.setdefault("history", [])
                history.append(
                    {
                        "type": "validation_override",
                        "timestamp": _now_iso(),
                        "message": "Se programó un envío pese a la validación inválida.",
                    }
                )
            store_dirty = True

    if warning_targets and targets:
        _info(
            "Algunos contactos no tienen código de país. Podrían fallar los envíos.",
            color=Fore.YELLOW,
        )
        choice = ask("¿Deseás continuar con ellos? (S/n): ").strip().lower()
        if choice == "n":
            to_exclude = {id(entry[0]) for entry in warning_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se quitaron los contactos en advertencia del envío.",
                color=Fore.YELLOW,
            )

    if not targets:
        _info("No quedaron contactos válidos tras la validación.", color=Fore.YELLOW)
        if store_dirty:
            store.save()
        press_enter()
        return

    if store_dirty:
        store.save()

    message_template = ask_multiline(
        "Mensaje a enviar (usa {nombre} para personalizar): "
    ).strip()
    if not message_template:
        _info("Mensaje vacío. Operación cancelada.", color=Fore.YELLOW)
        press_enter()
        return

    min_delay, max_delay = _ask_delay_range()
    session_limit = ask_int(
        "Cantidad máxima de mensajes por sesión (0 = sin tope): ",
        min_value=0,
        default=0,
    )

    planned_at = _now()
    run_id = str(uuid.uuid4())
    events: list[dict[str, Any]] = []
    for contact in targets:
        planned_at += timedelta(seconds=random.uniform(min_delay, max_delay))
        rendered = _render_message(message_template, contact)
        scheduled_at = planned_at.isoformat() + "Z"
        events.append(
            {
                "contact": contact.get("number"),
                "name": contact.get("name"),
                "message": rendered,
                "scheduled_at": scheduled_at,
                "status": "pendiente",
                "delivered_at": None,
                "notes": "",
                "confirmation": "no_enviado",
                "validation_status": contact.get("validation", {}).get("status"),
                "error_code": None,
            }
        )
        _mark_contact_scheduled(
            contact,
            run_id,
            rendered,
            scheduled_at,
            min_delay,
            max_delay,
        )

    run = {
        "id": run_id,
        "number_id": number["id"],
        "number_alias": number.get("alias"),
        "number_phone": number.get("phone"),
        "list_alias": contact_list.get("alias"),
        "created_at": _now_iso(),
        "status": "programado",
        "paused": False,
        "session_limit": session_limit,
        "total_contacts": len(events),
        "processed": 0,
        "completed_at": None,
        "last_activity_at": None,
        "next_run_at": events[0]["scheduled_at"] if events else None,
        "delay": {"min": min_delay, "max": max_delay},
        "message_template": message_template,
        "message_preview": textwrap.shorten(
            message_template, width=90, placeholder="…"
        ),
        "events": events,
        "max_contacts": max_contacts,
        "last_session_at": None,
        "completion_notified": False,
        "log": [],
    }
    _append_run_log(
        run,
        f"Se programó el envío para {len(events)} contactos con delays entre {min_delay:.1f}s y {max_delay:.1f}s.",
    )
    store.state.setdefault("message_runs", []).append(run)
    store.save()
    ok(
        "El envío quedó programado y continuará ejecutándose en segundo plano con ritmo humano."
    )
    press_enter()


def _print_runs_overview(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    if not runs:
        _info("No hay envíos programados todavía. Usá la opción 1 para crear uno.")
        return

    active = [
        run for run in runs if (run.get("status") or "").lower() not in {"completado", "cancelado"}
    ]
    if active:
        _subtitle("Envíos activos")
        for run in sorted(active, key=lambda item: item.get("created_at") or ""):
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            next_run = run.get("next_run_at") or "(esperando horario)"
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Próximo: {next_run}"
            )
    else:
        _info("No hay ejecuciones activas en este momento.")

    completed = [
        run for run in runs if (run.get("status") or "").lower() in {"completado", "cancelado"}
    ]
    if completed:
        print()
        _subtitle("Historial reciente")
        for run in sorted(
            completed,
            key=lambda item: item.get("completed_at") or item.get("last_activity_at") or item.get("created_at") or "",
            reverse=True,
        )[:3]:
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            finished = run.get("completed_at") or run.get("last_activity_at") or run.get("created_at")
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Finalizó: {finished or 'sin fecha'}"
            )


def _show_run_detail(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a monitorear",
        include_completed=True,
    )
    if not run:
        return
    _sync_message_runs(store)
    banner()
    title("Detalle del envío por WhatsApp")
    print(_line())
    total, sent, pending, cancelled, failed = _run_counts(run)
    print(f"Lista: {_run_list_label(run)} → Número: {_run_number_label(run)}")
    print(f"Estado actual: {_run_status_label(run)}")
    print(f"Mensajes enviados: {sent}/{total}")
    print(
        f"Pendientes: {pending} | Fallidos: {failed} | Cancelados/Omitidos: {cancelled}"
    )
    print(f"Delay configurado: {_format_delay(run.get('delay', {'min': 5.0, 'max': 12.0}))}")
    session_limit = run.get("session_limit") or 0
    if session_limit:
        print(f"Límite por sesión: {session_limit} mensajes")
    next_run = run.get("next_run_at")
    if next_run:
        print(f"Próximo envío estimado: {next_run}")
    if run.get("last_session_at"):
        print(f"Última sesión completada: {run.get('last_session_at')}")
    if run.get("message_preview"):
        print(f"Plantilla: {run.get('message_preview')}")
    next_event = next(
        (event for event in run.get("events", []) if (event.get("status") or "") == "pendiente"),
        None,
    )
    if next_event:
        print(
            "Próximo contacto: "
            f"{next_event.get('name') or next_event.get('contact')} a las {next_event.get('scheduled_at')}"
        )
    log = run.get("log", [])
    if log:
        print()
        _subtitle("Actividad registrada")
        for entry in log[-5:]:
            print(f" - {entry.get('timestamp')}: {entry.get('message')}")

    processed_events = [
        event
        for event in run.get("events", [])
        if (event.get("status") or "") in {"enviado", "fallido", "cancelado", "omitido"}
    ]
    if processed_events:
        print()
        _subtitle("Resultados recientes por contacto")
        for event in processed_events[-10:]:
            contact_label = event.get("name") or event.get("contact") or "(sin nombre)"
            status_label = _format_event_status(event)
            print(f" - {contact_label}: {status_label}")
            if event.get("notes"):
                print(f"     Motivo: {event.get('notes')}")
    press_enter()


def _toggle_run_pause(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a pausar o reanudar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status in {"completado", "cancelado"}:
        _info("Ese envío ya finalizó y no puede modificarse.", color=Fore.YELLOW)
        press_enter()
        return
    if run.get("paused"):
        run["paused"] = False
        run["status"] = "en progreso" if run.get("processed") else "programado"
        run["next_run_at"] = _next_pending_at(run.get("events", []))
        _append_run_log(run, "La ejecución se reanudó manualmente.")
        ok("El envío se reanudó. Continuará respetando los delays configurados.")
    else:
        run["paused"] = True
        run["status"] = "en pausa"
        run["last_session_at"] = _now_iso()
        _append_run_log(run, "La ejecución se pausó manualmente.")
        ok("El envío quedó en pausa segura.")
    store.save()
    press_enter()


def _cancel_run(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a cancelar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status == "cancelado":
        _info("Ese envío ya está cancelado.")
        press_enter()
        return
    if status == "completado":
        _info("Ese envío ya finalizó por completo.", color=Fore.YELLOW)
        press_enter()
        return
    confirm = ask("Confirmá la cancelación permanente (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    for event in run.get("events", []):
        if (event.get("status") or "") == "pendiente":
            _reset_contact_for_cancellation(store, run, event)
    run["status"] = "cancelado"
    run["paused"] = False
    run["completed_at"] = _now_iso()
    run["next_run_at"] = None
    _refresh_run_counters(run)
    _append_run_log(run, "La ejecución fue cancelada manualmente.")
    store.save()
    ok("El envío se canceló sin afectar al resto del sistema.")
    press_enter()


def _select_run(
    store: WhatsAppDataStore,
    prompt: str,
    *,
    include_completed: bool = True,
) -> dict[str, Any] | None:
    runs = store.state.setdefault("message_runs", [])
    filtered: list[dict[str, Any]] = []
    for run in runs:
        status = (run.get("status") or "").lower()
        if not include_completed and status in {"completado", "cancelado"}:
            continue
        filtered.append(run)
    if not filtered:
        _info("No hay envíos disponibles para esta acción.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, run in enumerate(filtered, 1):
        total, sent, pending, cancelled, failed = _run_counts(run)
        print(
            f"{idx}) {_run_list_label(run)} → {_run_number_label(run)} | {_run_status_label(run)} | "
            f"{sent}/{total} enviados"
            + (f" • {failed} fallidos" if failed else "")
            + (f" • {cancelled} omitidos" if cancelled else "")
            + f" | Pendientes: {pending}"
        )
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(filtered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return filtered[idx - 1]


def _run_number_label(run: dict[str, Any]) -> str:
    return run.get("number_alias") or run.get("number_phone") or "(sin alias)"


def _run_list_label(run: dict[str, Any]) -> str:
    return run.get("list_alias") or "(sin lista)"


def _run_status_label(run: dict[str, Any]) -> str:
    status = (run.get("status") or "programado").lower()
    if status == "en pausa" or run.get("paused"):
        return "⏸ en pausa"
    if status == "en progreso":
        return "🟢 en progreso"
    if status == "programado":
        return "🕒 programado"
    if status == "completado":
        return "✅ completado"
    if status == "cancelado":
        return "✖ cancelado"
    return status


def _run_counts(run: dict[str, Any]) -> tuple[int, int, int, int, int]:
    events = run.get("events", [])
    total = len(events)
    sent = sum(1 for event in events if (event.get("status") or "") == "enviado")
    failed = sum(1 for event in events if (event.get("status") or "") == "fallido")
    pending = sum(1 for event in events if (event.get("status") or "") == "pendiente")
    cancelled = sum(
        1
        for event in events
        if (event.get("status") or "") in {"cancelado", "omitido"}
    )
    return total, sent, pending, cancelled, failed


def _confirmation_badge(event: dict[str, Any]) -> str:
    confirmation = (event.get("confirmation") or "no_enviado").lower()
    if confirmation == "leido":
        return "✔✔"
    if confirmation == "entregado":
        return "✔✔"
    if confirmation == "enviado":
        return "✔"
    return "✖"


def _format_event_status(event: dict[str, Any]) -> str:
    status = (event.get("status") or "").lower()
    reason = event.get("notes") or ""
    badge = _confirmation_badge(event)
    if status == "enviado":
        return f"{badge} Entregado"
    if status == "fallido":
        base = "✖ Fallido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "pendiente":
        return "⏳ Pendiente"
    if status == "omitido":
        base = "⚪ Omitido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "cancelado":
        base = "⏹ Cancelado"
        if reason:
            base += f" – {reason}"
        return base
    return status or "(desconocido)"


def _append_run_log(run: dict[str, Any], message: str) -> None:
    log = run.setdefault("log", [])
    log.append({"timestamp": _now_iso(), "message": message})
    if len(log) > 50:
        del log[:-50]


def _next_pending_at(events: Iterable[dict[str, Any]]) -> str | None:
    upcoming = [
        event.get("scheduled_at")
        for event in events
        if (event.get("status") or "") == "pendiente" and event.get("scheduled_at")
    ]
    if not upcoming:
        return None
    return min(upcoming)


def _refresh_run_counters(run: dict[str, Any]) -> None:
    events = run.get("events", [])
    run["total_contacts"] = len(events)
    run["processed"] = sum(
        1
        for event in events
        if (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
    )


def _emit_run_completion_screen(run: dict[str, Any]) -> None:
    total, sent, _, cancelled, failed = _run_counts(run)
    processed = sent + failed + cancelled
    number_label = _run_number_label(run)
    list_label = _run_list_label(run)
    print(_line())
    title("Resumen final de envío por WhatsApp")
    print(_line())
    print(f"Número usado: {number_label}")
    print(f"Lista operada: {list_label}")
    print(f"Éxitos: {sent}")
    print(f"Fallidos: {failed}")
    print(f"Omitidos/Cancelados: {cancelled}")
    print(f"Procesados: {processed}/{total}")
    print(_line())
    _log_structured(
        "whatsapp.run.completed.summary",
        run_id=run.get("id"),
        number=number_label,
        list_alias=list_label,
        sent=sent,
        failed=failed,
        cancelled=cancelled,
        total=total,
    )


def _has_active_message_runs(runs: list[dict[str, Any]]) -> bool:
    for run in runs:
        status = (run.get("status") or "").lower()
        if status not in {"completado", "cancelado"}:
            return True
    return False


def _has_active_background_automations(store: WhatsAppDataStore) -> bool:
    ai_configs = store.state.get("ai_automations", {})
    ai_active = any(
        isinstance(config, dict) and bool(config.get("active", False))
        for config in ai_configs.values()
    )
    followup = store.state.get("followup", {})
    followup_active = isinstance(followup, dict) and bool(followup.get("auto_enabled", False))
    return ai_active or followup_active


def _playwright_runtime_exists() -> bool:
    with _PLAYWRIGHT_RUNTIME_LOCK:
        return _PLAYWRIGHT_RUNTIME is not None


def _reconcile_runs(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    now = _now()
    changed = False
    for run in runs:
        status = (run.get("status") or "").lower()
        events = run.get("events", [])
        if not events:
            continue
        if status == "cancelado":
            continue
        if run.get("paused"):
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            continue

        session_limit = run.get("session_limit") or 0
        processed_now = 0
        for event in events:
            if (event.get("status") or "") != "pendiente":
                continue
            scheduled_at = _parse_iso(event.get("scheduled_at")) or now
            if scheduled_at > now:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if session_limit and processed_now >= session_limit:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if _deliver_event(store, run, event):
                processed_now += 1
                changed = True
        if session_limit and processed_now >= session_limit and any(
            (event.get("status") or "") == "pendiente" for event in events
        ):
            if not run.get("paused"):
                run["paused"] = True
                run["status"] = "en pausa"
                run["last_session_at"] = _now_iso()
                _append_run_log(
                    run,
                    "Se alcanzó el límite de mensajes por sesión. La ejecución se pausó automáticamente.",
                )
                changed = True
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            _refresh_run_counters(run)
            continue
        if all(
            (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
            for event in events
        ):
            if status != "completado":
                run["status"] = "completado"
                run["completed_at"] = _now_iso()
                run["paused"] = False
                run["next_run_at"] = None
                _append_run_log(
                    run,
                    "La ejecución finalizó y todos los mensajes fueron procesados.",
                )
                changed = True
            _refresh_run_counters(run)
            if not run.get("completion_notified"):
                _emit_run_completion_screen(run)
                run["completion_notified"] = True
                changed = True
            continue
        next_at = _next_pending_at(events)
        if run.get("next_run_at") != next_at:
            run["next_run_at"] = next_at
            changed = True
        if any((event.get("status") or "") in {"enviado", "fallido"} for event in events):
            if run.get("status") not in {"en pausa", "completado"}:
                run["status"] = "en progreso"
                changed = True
        _refresh_run_counters(run)
    try:
        if _run_ai_automations(store):
            changed = True
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error procesando auto-respuestas de WhatsApp: %s", exc)
        _log_structured("whatsapp.reconcile.autoreply.error", error=str(exc))
    try:
        if _run_followup_scheduler(store):
            changed = True
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error procesando seguimiento automático de WhatsApp: %s", exc)
        _log_structured("whatsapp.reconcile.followup.error", error=str(exc))
    if (
        not _has_active_message_runs(runs)
        and not _has_active_background_automations(store)
        and _playwright_runtime_exists()
    ):
        _shutdown_playwright_runtime()
        _log_structured(
            "whatsapp.playwright.runtime.closed",
            reason="no_active_runs",
        )
    if changed:
        store.save()


# ======================================================================
# ===== Session Management (Playwright) ================================

def _resolve_playwright_user_data_dir(sender: dict[str, Any] | None = None) -> Path:
    """Resuelve el directorio persistente para Playwright (cookies/localStorage)."""
    raw = ""
    if sender and isinstance(sender.get("session_path"), str):
        raw = (sender.get("session_path") or "").strip()
    if raw:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = (BASE / candidate).resolve()
        return candidate
    return DEFAULT_PLAYWRIGHT_SESSION_DIR


def _wa_automation_headless(sender: dict[str, Any] | None = None) -> bool:
    """
    Ejecuta automatizaciones en segundo plano por defecto.
    Override temporal: WPP_HEADFUL=1 fuerza modo visible.
    """
    if (os.getenv("WPP_HEADFUL", "").strip().lower() in {"1", "true", "yes", "on"}):
        return False
    if sender is None:
        return True
    return bool(sender.get("background_mode", True))


def _normalize_message(text: str) -> str:
    return "\n".join(line.strip() for line in (text or "").splitlines()).strip()


def _collect_playwright_alert_text(page: Any) -> str:
    texts: list[str] = []
    for selector in _WASelectors.ALERT_TEXT:
        try:
            locator = page.locator(selector)
            count = locator.count()
        except Exception:
            continue
        for idx in range(min(count, 5)):
            try:
                text = (locator.nth(idx).inner_text() or "").strip()
            except Exception:
                text = ""
            if text and text not in texts:
                texts.append(text)
    if texts:
        return " ".join(texts)
    try:
        body = page.locator("body").first
        snippet = (body.inner_text() or "").strip().splitlines()
        if snippet:
            return snippet[0].strip()
    except Exception:
        return ""
    return ""


def _wa_is_qr_visible(page: Any) -> bool:
    selector = _join_selectors(_WASelectors.QR_CANVAS)
    try:
        locator = page.locator(selector).first
        return locator.is_visible()
    except Exception:
        return False


def _wa_is_ready(page: Any) -> bool:
    selector = _join_selectors(_WASelectors.APP_READY)
    try:
        locator = page.locator(selector).first
        return locator.is_visible()
    except Exception:
        return False


def _wa_wait_for_login_state(page: Any, *, timeout_seconds: float) -> str:
    """Devuelve 'ready', 'qr' o 'timeout'."""
    deadline = time.time() + max(timeout_seconds, 0.1)
    while time.time() < deadline:
        if _wa_is_ready(page):
            return "ready"
        if _wa_is_qr_visible(page):
            return "qr"
        time.sleep(0.5)
    return "timeout"


def _wa_find_visible_chat_input(page: Any, *, timeout_ms: int = 45000) -> Any | None:
    """
    Selecciona el textbox visible de escritura del chat activo.
    Evita tomar inputs ocultos o cajas de búsqueda laterales.
    """
    selector = _join_selectors(_WASelectors.CHAT_INPUT)
    deadline = time.time() + max(timeout_ms, 1000) / 1000.0
    while time.time() < deadline:
        try:
            candidates = page.locator(selector)
            total = min(_wa_locator_count(candidates), 20)
        except Exception:
            total = 0
            candidates = None
        if not candidates or total <= 0:
            page.wait_for_timeout(250)
            continue

        for idx in range(total):
            candidate = candidates.nth(idx)
            try:
                if not candidate.is_visible():
                    continue
                in_footer = bool(
                    candidate.evaluate(
                        "el => Boolean(el && el.closest && el.closest('footer'))"
                    )
                )
                if not in_footer:
                    continue
                box = candidate.bounding_box() or {}
                width = float(box.get("width") or 0.0)
                height = float(box.get("height") or 0.0)
                if width < 40 or height < 10:
                    continue
                return candidate
            except Exception:
                continue

        page.wait_for_timeout(250)
    return None


def _register_playwright_runtime_cleanup() -> None:
    global _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED
    if _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED:
        return
    atexit.register(_shutdown_playwright_runtime)
    _PLAYWRIGHT_RUNTIME_ATEXIT_REGISTERED = True


def _shutdown_playwright_runtime() -> None:
    global _PLAYWRIGHT_RUNTIME
    with _PLAYWRIGHT_RUNTIME_LOCK:
        runtime = _PLAYWRIGHT_RUNTIME
        _PLAYWRIGHT_RUNTIME = None
    _close_playwright_runtime_instance(runtime)


>>>>>>> origin/main
def _close_playwright_runtime_instance(runtime: _PlaywrightRuntime | None) -> None:
    if runtime is None:
        return
    owner_runtime_id = str(getattr(runtime, "runtime_id", "") or "").strip()
    try:
        runtime.context.close()
    except Exception:
        pass
    if owner_runtime_id:
        try:
            from src.runtime.playwright_runtime import mark_sync_runtime_context_closed, safe_runtime_stop

            mark_sync_runtime_context_closed(owner_runtime_id)
            safe_runtime_stop(runtime_id=owner_runtime_id, playwright=runtime.playwright)
        except Exception:
            pass
<<<<<<< HEAD


def _playwright_runtime_healthcheck(runtime: _PlaywrightRuntime) -> bool:
    try:
        if runtime.page is None or runtime.page.is_closed():
            runtime.page = runtime.context.pages[0] if runtime.context.pages else runtime.context.new_page()
            runtime.page.set_default_timeout(60000)
        else:
            runtime.page.set_default_timeout(60000)
        return True
    except Exception:
        return False


def _create_playwright_runtime(user_data_dir: Path, *, headless: bool) -> _PlaywrightRuntime:
=======


def _playwright_runtime_healthcheck(runtime: _PlaywrightRuntime) -> bool:
    try:
        if runtime.page is None or runtime.page.is_closed():
            runtime.page = runtime.context.pages[0] if runtime.context.pages else runtime.context.new_page()
            runtime.page.set_default_timeout(60000)
        else:
            runtime.page.set_default_timeout(60000)
        return True
    except Exception:
        return False


def _create_playwright_runtime(user_data_dir: Path, *, headless: bool) -> _PlaywrightRuntime:
>>>>>>> origin/main
    try:
        from src.playwright_service import resolve_playwright_executable
        from src.runtime.playwright_runtime import (
            launch_sync_persistent_context,
            mark_sync_runtime_context_open,
            register_sync_runtime_owner,
            start_sync_playwright,
        )
<<<<<<< HEAD
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Playwright no está disponible. Instalá 'playwright' y ejecutá 'playwright install'."
        ) from exc
=======
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Playwright no está disponible. Instalá 'playwright' y ejecutá 'playwright install'."
        ) from exc
>>>>>>> origin/main

    user_data_dir.mkdir(parents=True, exist_ok=True)
    owner = register_sync_runtime_owner(owner_module=__name__)
    playwright = start_sync_playwright()
<<<<<<< HEAD
    executable = None
    try:
        executable = resolve_playwright_executable(headless=headless)
    except Exception:
        executable = None

    context = launch_sync_persistent_context(
        user_data_dir=user_data_dir,
        headless=bool(headless),
        executable_path=executable,
        args=[
            "--disable-notifications",
            "--disable-infobars",
            "--disable-dev-shm-usage",
        ],
        viewport={"width": 1280, "height": 720},
=======
    executable = None
    try:
        executable = resolve_playwright_executable(headless=headless)
    except Exception:
        executable = None

    context = launch_sync_persistent_context(
        user_data_dir=user_data_dir,
        headless=bool(headless),
        executable_path=executable,
        args=[
            "--disable-notifications",
            "--disable-infobars",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
        viewport={"width": 1280, "height": 720},
>>>>>>> origin/main
    )
    page = context.pages[0] if context.pages else context.new_page()
    page.set_default_timeout(60000)
    mark_sync_runtime_context_open(owner.runtime_id)
    runtime = _PlaywrightRuntime(
        playwright=playwright,
        context=context,
        page=page,
        user_data_dir=user_data_dir,
        headless=headless,
        runtime_id=owner.runtime_id,
        owner_module=owner.owner_module,
    )
<<<<<<< HEAD
    _register_playwright_runtime_cleanup()
    _log_structured(
        "whatsapp.playwright.runtime.created",
        user_data_dir=str(user_data_dir),
        headless=bool(headless),
    )
    return runtime


def _get_or_create_playwright_runtime(
    *,
    sender: dict[str, Any] | None = None,
    headless: bool = False,
) -> _PlaywrightRuntime:
    global _PLAYWRIGHT_RUNTIME
    requested_dir = _resolve_playwright_user_data_dir(sender).resolve()
    requested_headless = bool(headless)

    stale_runtime: _PlaywrightRuntime | None = None
    with _PLAYWRIGHT_RUNTIME_LOCK:
        runtime = _PLAYWRIGHT_RUNTIME
        if runtime is not None:
            same_profile = runtime.user_data_dir.resolve() == requested_dir
            same_headless = runtime.headless == requested_headless
            if same_profile and same_headless and _playwright_runtime_healthcheck(runtime):
                runtime.last_used_at = _now_iso()
                return runtime
            stale_runtime = runtime
            old_dir = str(runtime.user_data_dir)
            _PLAYWRIGHT_RUNTIME = None
        else:
            old_dir = None

    if old_dir:
        _log_structured(
            "whatsapp.playwright.runtime.recreate",
            old_user_data_dir=old_dir,
            new_user_data_dir=str(requested_dir),
        )
        _close_playwright_runtime_instance(stale_runtime)

    runtime = _create_playwright_runtime(requested_dir, headless=requested_headless)
    with _PLAYWRIGHT_RUNTIME_LOCK:
        _PLAYWRIGHT_RUNTIME = runtime
    return runtime


@contextlib.contextmanager
def _playwright_persistent_page(*, sender: dict[str, Any] | None = None, headless: bool = False):
    """
    Devuelve siempre el mismo contexto/página Playwright para el runtime actual
    (mientras perfil/headless coincidan), evitando recrearlo por ciclo.
    """
    with _PLAYWRIGHT_LOCK:
        runtime = _get_or_create_playwright_runtime(sender=sender, headless=headless)
        if not _playwright_runtime_healthcheck(runtime):
            _shutdown_playwright_runtime()
            runtime = _get_or_create_playwright_runtime(sender=sender, headless=headless)
        runtime.last_used_at = _now_iso()
        try:
            yield runtime.page, runtime.context
        finally:
            runtime.last_used_at = _now_iso()


# ======================================================================
# ===== Message Sending ==================================================

def _extract_playwright_bubble_text(bubble: Any) -> str:
    texts: list[str] = []
    selector = _join_selectors(_WASelectors.BUBBLE_TEXT)
    try:
        candidates = bubble.locator(selector)
        count = candidates.count()
    except Exception:
        count = 0
        candidates = None
    for idx in range(min(count, 30)):
        try:
            text = (candidates.nth(idx).inner_text() or "").strip()  # type: ignore[union-attr]
        except Exception:
            text = ""
        if text:
            texts.append(text)
    merged = "\n".join(texts).strip()
    if merged:
        return merged
    try:
        fallback = (bubble.inner_text() or "").strip()
    except Exception:
        fallback = ""
    return fallback


def _wa_bubble_direction(bubble: Any) -> str:
    try:
        cls = (bubble.get_attribute("class") or "").lower()
    except Exception:
        cls = ""
    if "message-out" in cls:
        return "outgoing"
    if "message-in" in cls:
        return "incoming"
    try:
        if bubble.locator(_WASelectors.CHECK_READ).count() > 0:
            return "outgoing"
        if bubble.locator(_WASelectors.CHECK_DELIVERED).count() > 0:
            return "outgoing"
        if bubble.locator(_WASelectors.CHECK_SENT).count() > 0:
            return "outgoing"
    except Exception:
        pass
    try:
        if bubble.locator(_join_selectors(_WASelectors.BUBBLE_OUT)).count() > 0:
            return "outgoing"
        if bubble.locator(_join_selectors(_WASelectors.BUBBLE_IN)).count() > 0:
            return "incoming"
    except Exception:
        pass
    return "unknown"


def _wa_confirmation_from_bubble(bubble: Any) -> str:
    try:
        if bubble.locator(_WASelectors.CHECK_READ).count() > 0:
            return "leido"
        if bubble.locator(_WASelectors.CHECK_DELIVERED).count() > 0:
            return "entregado"
        if bubble.locator(_WASelectors.CHECK_SENT).count() > 0:
            return "enviado"
    except Exception:
        return "enviado"
    return "enviado"


def _wa_wait_outgoing_bubble(
    page: Any,
    *,
    expected_text: str,
    before_count: int,
    timeout_ms: int = 25000,
) -> tuple[Any | None, str]:
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    expected = _normalize_message(expected_text)
    deadline = time.time() + max(timeout_ms, 1000) / 1000.0

    while time.time() < deadline:
        try:
            nodes = page.locator(container_selector)
            total = _wa_locator_count(nodes)
        except Exception:
            total = 0
            nodes = None
        if total <= 0 or nodes is None:
            page.wait_for_timeout(250)
            continue
        if before_count > 0 and total <= before_count:
            page.wait_for_timeout(250)
            continue

        if before_count > 0:
            start = max(0, min(before_count, total - 1))
        else:
            start = max(0, total - 8)
        for idx in range(total - 1, start - 1, -1):
            bubble = nodes.nth(idx)
            text = _normalize_message(_extract_playwright_bubble_text(bubble))
            if expected and text and (expected in text or text in expected):
                direction = _wa_bubble_direction(bubble)
                confirmation = _wa_confirmation_from_bubble(bubble)
                if direction == "outgoing":
                    return bubble, confirmation

        if total > before_count:
            for idx in range(total - 1, start - 1, -1):
                bubble = nodes.nth(idx)
                if _wa_bubble_direction(bubble) == "outgoing":
                    return bubble, _wa_confirmation_from_bubble(bubble)

        page.wait_for_timeout(300)
    return None, ""


def _send_with_playwright(
    sender: dict[str, Any], contact: dict[str, Any], message: str
) -> dict[str, Any]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    except Exception:
        return {
            "success": False,
            "code": "playwright_missing",
            "reason": "Playwright no está instalado. Ejecutá 'pip install playwright' y luego 'playwright install'.",
            "session_expired": False,
        }

    digits = "".join(ch for ch in (contact.get("number") or "") if ch.isdigit())
    if not digits:
        return {
            "success": False,
            "code": "invalid_number",
            "reason": "El número no tiene dígitos suficientes para WhatsApp.",
            "session_expired": False,
        }

    typed_message = message or ""
    if not typed_message.strip():
        return {
            "success": False,
            "code": "empty_message",
            "reason": "El mensaje quedó vacío y no se envió a WhatsApp.",
            "session_expired": False,
        }

    delivered_at = _now_iso()
    session_expired = False
    trace_id = str(uuid.uuid4())

    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")

            state = _wa_wait_for_login_state(page, timeout_seconds=15.0)
            if state == "qr":
                session_expired = True
                snapshot_path = _resolve_playwright_user_data_dir(sender) / "qr.png"
                try:
                    qr_selector = _join_selectors(_WASelectors.QR_CANVAS)
                    page.locator(qr_selector).first.screenshot(path=str(snapshot_path))
                except Exception:
                    try:
                        page.screenshot(path=str(snapshot_path))
                    except Exception:
                        pass
                _log_structured(
                    "whatsapp.session_expired",
                    trace_id=trace_id,
                    sender_id=sender.get("id"),
                    sender_alias=sender.get("alias"),
                    reason="QR visible (sesión no logueada).",
                )
                return {
                    "success": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp caducó o no está vinculada. Volvé a escanear el código QR.",
                    "session_expired": True,
                }

            if state == "timeout" and not _wa_is_ready(page):
                return {
                    "success": False,
                    "code": "whatsapp_unreachable",
                    "reason": "WhatsApp Web no respondió a tiempo. Intentá nuevamente en unos minutos.",
                    "session_expired": False,
                }

            page.goto(f"{WHATSAPP_WEB_URL}/send?phone={digits}", wait_until="domcontentloaded")
            input_box = _wa_find_visible_chat_input(page, timeout_ms=45000)
            if input_box is None:
                alert = _collect_playwright_alert_text(page) or (
                    "No se pudo abrir la conversación. Confirmá que el número tenga WhatsApp."
                )
                _log_structured(
                    "whatsapp.chat_unavailable",
                    trace_id=trace_id,
                    sender_id=sender.get("id"),
                    contact_number=contact.get("number"),
                    reason=alert,
                )
                return {
                    "success": False,
                    "code": "chat_unavailable",
                    "reason": alert,
                    "session_expired": False,
                }
            try:
                input_box.click(timeout=5000)
            except Exception:
                try:
                    input_box.focus()
                except Exception:
                    return {
                        "success": False,
                        "code": "input_missing",
                        "reason": "No se encontró el cuadro de mensaje en WhatsApp Web.",
                        "session_expired": False,
                    }

            # Limpiar texto previo (si lo hubiera).
            for hotkey in ("Control+A", "Meta+A"):
                try:
                    page.keyboard.press(hotkey)
                    page.keyboard.press("Delete")
                    break
                except Exception:
                    continue

            # Conteo previo para confirmar envío.
            try:
                before_count = _wa_locator_count(page.locator(_WASelectors.MESSAGE_CONTAINER[0]))
            except Exception:
                before_count = 0

            # Tipeo multi-línea (Shift+Enter) y Enter para enviar.
            for idx, line in enumerate(typed_message.splitlines() or [""]):
                if idx:
                    page.keyboard.press("Shift+Enter")
                if line:
                    page.keyboard.type(line)
                else:
                    page.keyboard.type(" ")
            page.keyboard.press("Enter")

            normalized_message = _normalize_message(typed_message)
            matched_bubble, confirmation = _wa_wait_outgoing_bubble(
                page,
                expected_text=typed_message,
                before_count=before_count,
                timeout_ms=25000,
            )
            if matched_bubble is None:
                return {
                    "success": False,
                    "code": "send_unconfirmed",
                    "reason": "WhatsApp no confirmó el mensaje en la conversación.",
                    "session_expired": False,
                }

            # Espera corta para intentar escalar de "enviado" a "entregado/leído".
            confirmation = confirmation or "enviado"
            if confirmation == "enviado":
                upgrade_deadline = time.time() + 6.0
                while time.time() < upgrade_deadline:
                    upgraded = _wa_confirmation_from_bubble(matched_bubble)
                    if upgraded in {"entregado", "leido"}:
                        confirmation = upgraded
                        break
                    page.wait_for_timeout(400)

            bubble_text = _extract_playwright_bubble_text(matched_bubble)
            normalized_bubble = _normalize_message(bubble_text)
            if normalized_message and normalized_bubble:
                if (
                    normalized_message not in normalized_bubble
                    and normalized_bubble not in normalized_message
                ):
                    return {
                        "success": False,
                        "code": "text_mismatch",
                        "reason": "WhatsApp no mostró el contenido del mensaje enviado.",
                        "session_expired": False,
                    }

            _log_structured(
                "whatsapp.send.ok",
                trace_id=trace_id,
                sender_id=sender.get("id"),
                contact_number=contact.get("number"),
                confirmation=confirmation,
            )
            return {
                "success": True,
                "confirmation": confirmation,
                "note": "Mensaje confirmado en WhatsApp Web mediante Playwright.",
                "delivered_at": delivered_at,
                "session_expired": False,
            }
    except PlaywrightTimeoutError:
        _log_structured(
            "whatsapp.send.timeout",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            contact_number=contact.get("number"),
        )
        return {
            "success": False,
            "code": "timeout",
            "reason": "WhatsApp Web tardó demasiado en responder al enviar el mensaje.",
            "session_expired": session_expired,
        }
    except Exception as exc:  # noqa: BLE001
        _log_structured(
            "whatsapp.send.error",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            contact_number=contact.get("number"),
            error=str(exc),
        )
        return {
            "success": False,
            "code": "playwright_error",
            "reason": "Playwright reportó un error inesperado durante el envío.",
            "session_expired": session_expired,
        }


def _send_message_via_backend(
    sender: dict[str, Any], contact: dict[str, Any], event: dict[str, Any]
) -> dict[str, Any]:
    method = (sender.get("connection_method") or "").lower()
    message = event.get("message", "")
    if method != "playwright":
        sender["connection_method"] = "playwright"
        _log_structured(
            "whatsapp.sender.method.normalized",
            sender_id=sender.get("id"),
            previous_method=method or "missing",
            normalized_method="playwright",
        )
    return _send_with_playwright(sender, contact, message)


# ======================================================================
# ===== Message Reading ==================================================

def _wa_locator_count(locator: Any) -> int:
    try:
        return int(locator.count())
    except Exception:
        return 0


def _wa_open_unread_chat(page: Any) -> tuple[Any | None, str]:
    """
    Busca el primer chat con badge de no leído y lo abre.
    Devuelve (chat_row_locator, reason).
    """
    row_selector = _join_selectors(_WASelectors.CHAT_ROW_CANDIDATES)
    badge_selector = _join_selectors(_WASelectors.UNREAD_BADGE)

    try:
        rows = page.locator(row_selector)
        row_count = rows.count()
    except Exception as exc:  # noqa: BLE001
        return None, f"No se pudo inspeccionar la lista de chats: {exc}"

    for idx in range(min(row_count, 200)):
        row = rows.nth(idx)
        try:
            badge = row.locator(badge_selector).first
            if not badge.is_visible():
                continue
            row.click(timeout=5000)
            return row, ""
        except Exception:
            continue
    return None, "No hay chats con mensajes no leídos."


def _wa_get_last_incoming_message(page: Any) -> str:
    selector = _join_selectors(_WASelectors.BUBBLE_IN)
    bubbles = page.locator(selector)
    total = _wa_locator_count(bubbles)
    if total <= 0:
        return ""
    last = bubbles.nth(total - 1)
    return _extract_playwright_bubble_text(last)


def _wa_get_last_outgoing_message(page: Any) -> str:
    selector = _join_selectors(_WASelectors.BUBBLE_OUT)
    bubbles = page.locator(selector)
    total = _wa_locator_count(bubbles)
    if total <= 0:
        return ""
    last = bubbles.nth(total - 1)
    return _extract_playwright_bubble_text(last)


def _wa_get_last_message_snapshot(page: Any) -> dict[str, str]:
    """
    Retorna dirección del último mensaje visible: incoming/outgoing/unknown, junto al texto.
    """
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    try:
        nodes = page.locator(container_selector)
        total = _wa_locator_count(nodes)
    except Exception:
        total = 0
        nodes = None
    if total <= 0 or nodes is None:
        return {"direction": "unknown", "text": ""}

    bubble = nodes.nth(total - 1)
    text = _extract_playwright_bubble_text(bubble)
    direction = _wa_bubble_direction(bubble)
    return {"direction": direction, "text": text}


def _wa_get_active_chat_title(page: Any) -> str:
    for selector in _WASelectors.ACTIVE_CHAT_TITLE:
        try:
            loc = page.locator(selector).first
            if not loc.is_visible():
                continue
            value = (loc.get_attribute("title") or loc.inner_text() or "").strip()
            if value:
                return value
        except Exception:
            continue
    return ""


def _extract_sender_phone_from_chat_title(chat_title: str) -> str:
    digits = "".join(ch for ch in (chat_title or "") if ch.isdigit())
    if len(digits) < MIN_PHONE_DIGITS:
        return ""
    return digits


def _wa_build_conversation_history(page: Any, max_items: int = 10) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    try:
        nodes = page.locator(container_selector)
        total = _wa_locator_count(nodes)
    except Exception:
        total = 0
        nodes = None

    if total > 0 and nodes is not None:
        start = max(total - max_items * 3, 0)
        for i in range(start, total):
            bubble = nodes.nth(i)
            text = _extract_playwright_bubble_text(bubble)
            if not text:
                continue
            role = "user"
            try:
                direction = _wa_bubble_direction(bubble)
                if direction == "outgoing":
                    role = "assistant"
                elif direction == "incoming":
                    role = "user"
            except Exception:
                role = "user"
            history.append({"role": role, "content": text})

    return history[-max_items:]


def _wa_read_next_unread(
    sender: dict[str, Any],
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """
    Abre el próximo chat no leído y extrae el último mensaje entrante.
    """
    trace_id = str(uuid.uuid4())
    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")
            state = _wa_wait_for_login_state(page, timeout_seconds=max(5.0, timeout_seconds))
            if state == "qr":
                return {
                    "ok": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp no está vinculada (QR visible).",
                }
            if state == "timeout" and not _wa_is_ready(page):
                return {
                    "ok": False,
                    "code": "timeout",
                    "reason": "No se pudo acceder al panel de chats dentro del tiempo esperado.",
                }

            pane_selector = _join_selectors(_WASelectors.PANE_SIDE)
            try:
                page.wait_for_selector(pane_selector, timeout=20000)
            except Exception:
                return {
                    "ok": False,
                    "code": "chat_list_unavailable",
                    "reason": "No se encontró la lista de chats.",
                }

            row, reason = _wa_open_unread_chat(page)
            if row is None:
                return {"ok": False, "code": "no_unread", "reason": reason}

            input_box = _wa_find_visible_chat_input(page, timeout_ms=10000)
            if input_box is None:
                return {
                    "ok": False,
                    "code": "input_missing",
                    "reason": "No se encontró el cuadro de mensaje en el chat activo.",
                }

            chat_title = _wa_get_active_chat_title(page)
            sender_phone = _extract_sender_phone_from_chat_title(chat_title)
            snapshot = _wa_get_last_message_snapshot(page)
            if snapshot.get("direction") == "outgoing":
                return {
                    "ok": False,
                    "code": "self_message",
                    "reason": "El último mensaje visible es saliente; se omite auto-respuesta.",
                    "chat_title": chat_title,
                }
            last_in = _wa_get_last_incoming_message(page)
            last_out = _wa_get_last_outgoing_message(page)
            if not last_in:
                return {
                    "ok": False,
                    "code": "incoming_missing",
                    "reason": "No se detectó un mensaje entrante legible en el chat abierto.",
                }
            if last_out and _normalize_message(last_out) == _normalize_message(last_in):
                return {
                    "ok": False,
                    "code": "self_message",
                    "reason": "El último contenido coincide con un mensaje saliente; se omite auto-respuesta.",
                    "chat_title": chat_title,
                }

            history = _wa_build_conversation_history(page, max_items=12)
            result = {
                "ok": True,
                "chat_title": chat_title,
                "sender_phone": sender_phone,
                "incoming_text": last_in,
                "history": history,
                "meta": {"trace_id": trace_id},
            }
            _log_structured(
                "whatsapp.read.unread.ok",
                trace_id=trace_id,
                sender_id=sender.get("id"),
                chat_title=chat_title,
                chars=len(last_in),
            )
            return result
    except Exception as exc:  # noqa: BLE001
        _log_structured(
            "whatsapp.read.unread.error",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            error=str(exc),
        )
        return {
            "ok": False,
            "code": "playwright_error",
            "reason": f"Error de Playwright al leer mensajes no leídos: {exc}",
        }


# ======================================================================
# ===== Auto Reply =======================================================

def _generate_gpt_reply(
    *,
    incoming_text: str,
    conversation_history: list[dict[str, Any]],
    system_prompt: str | None = None,
) -> dict[str, Any]:
    """
    Genera respuesta con OpenAI usando historial de conversación.
    Devuelve estructura estable para no romper flujos.
    """
    prompt = (system_prompt or "").strip() or (
        "Sos un asistente cordial. Respondé de forma breve, humana y útil en español neutro."
    )
    try:
        from openai import OpenAI

        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            return {"ok": False, "code": "openai_missing_key", "reason": "OPENAI_API_KEY no configurada."}
        client = OpenAI(api_key=api_key)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "code": "openai_unavailable", "reason": str(exc)}

    messages: list[dict[str, str]] = [{"role": "system", "content": prompt}]
    for msg in conversation_history[-12:]:
        role = (msg.get("role") or "").strip()
        content = (msg.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": incoming_text})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=180,
        )
        reply = (response.choices[0].message.content or "").strip()
        if not reply:
            return {"ok": False, "code": "empty_reply", "reason": "OpenAI devolvió una respuesta vacía."}
        return {
            "ok": True,
            "reply_text": reply,
            "model": "gpt-4o-mini",
            "tokens_used": getattr(getattr(response, "usage", None), "total_tokens", None),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "code": "openai_error", "reason": str(exc)}


def _sync_contact_state_after_receive(
    contact: dict[str, Any],
    *,
    incoming_text: str,
    received_at: str | None = None,
) -> None:
    ts = received_at or _now_iso()
    contact["last_message_received"] = incoming_text
    contact["last_message_received_at"] = ts
    contact["last_response_at"] = ts
    contact["status"] = "respondió"
    contact.setdefault("history", []).append(
        {
            "type": "incoming",
            "message": incoming_text,
            "received_at": ts,
        }
    )


def _sync_contact_state_after_send(
    contact: dict[str, Any],
    *,
    outgoing_text: str,
    sent_at: str | None = None,
) -> None:
    ts = sent_at or _now_iso()
    contact["last_message_sent"] = outgoing_text
    contact["last_message_sent_at"] = ts
    # Compatibilidad hacia atrás.
    contact["last_message_at"] = ts
    contact["status"] = "mensaje enviado"
    contact.setdefault("history", []).append(
        {
            "type": "send",
            "message": outgoing_text,
            "sent_at": ts,
        }
    )


def _sync_contact_state_followup(
    contact: dict[str, Any],
    *,
    followup_message: str,
    sent_at: str | None = None,
) -> None:
    ts = sent_at or _now_iso()
    contact["last_followup_at"] = ts
    current_stage = contact.get("followup_stage", 0)
    try:
        stage = int(current_stage) + 1
    except Exception:
        stage = 1
    contact["followup_stage"] = stage
    contact["status"] = "seguimiento enviado"
    contact["last_message_sent"] = followup_message
    contact["last_message_sent_at"] = ts
    contact.setdefault("history", []).append(
        {
            "type": "followup",
            "message": followup_message,
            "sent_at": ts,
            "stage": stage,
        }
    )


def _find_contact_by_chat_title(store: WhatsAppDataStore, chat_title: str) -> dict[str, Any] | None:
    title = (chat_title or "").strip().lower()
    if not title:
        return None
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            name = (contact.get("name") or "").strip().lower()
            if name and name == title:
                return contact
    return None


def _find_contact_by_sender_phone(store: WhatsAppDataStore, sender_phone: str) -> dict[str, Any] | None:
    digits = "".join(ch for ch in (sender_phone or "") if ch.isdigit())
    if not digits:
        return None
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            n_digits = "".join(ch for ch in (contact.get("number") or "") if ch.isdigit())
            if n_digits.endswith(digits) or digits.endswith(n_digits):
                return contact
    return None


def _find_contact_for_incoming_chat(
    store: WhatsAppDataStore,
    *,
    chat_title: str,
    sender_phone: str = "",
) -> dict[str, Any] | None:
    contact = _find_contact_by_sender_phone(store, sender_phone)
    if contact:
        return contact
    return _find_contact_by_chat_title(store, chat_title)


def _process_auto_reply_for_sender(
    store: WhatsAppDataStore,
    sender: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    read_result = _wa_read_next_unread(sender, timeout_seconds=25.0)
    if not read_result.get("ok"):
        return read_result

    incoming_text = (read_result.get("incoming_text") or "").strip()
    if not incoming_text:
        return {
            "ok": False,
            "code": "incoming_empty",
            "reason": "El mensaje entrante estaba vacío.",
        }

    chat_title = read_result.get("chat_title", "")
    sender_phone = (read_result.get("sender_phone") or "").strip()
    contact = _find_contact_for_incoming_chat(
        store,
        chat_title=chat_title,
        sender_phone=sender_phone,
    )
    if contact:
        _sync_contact_state_after_receive(contact, incoming_text=incoming_text)

    gpt_result = _generate_gpt_reply(
        incoming_text=incoming_text,
        conversation_history=list(read_result.get("history") or []),
        system_prompt=config.get("prompt", ""),
    )
    if not gpt_result.get("ok"):
        return {
            "ok": False,
            "code": gpt_result.get("code", "gpt_failed"),
            "reason": gpt_result.get("reason", "No se pudo generar respuesta con GPT."),
        }

    reply_text = (gpt_result.get("reply_text") or "").strip()
    if not reply_text:
        return {"ok": False, "code": "empty_reply", "reason": "GPT devolvió texto vacío."}

    # Enviamos sobre el chat abierto para evitar depender del número en esta etapa.
    send_result = _wa_reply_on_current_chat(
        sender,
        reply_text,
        expected_chat_title=chat_title or None,
    )
    if not send_result.get("success"):
        return {
            "ok": False,
            "code": send_result.get("code", "send_failed"),
            "reason": send_result.get("reason", "No se pudo enviar la respuesta automática."),
        }

    sent_at = send_result.get("delivered_at") or _now_iso()
    if contact:
        _sync_contact_state_after_send(contact, outgoing_text=reply_text, sent_at=sent_at)
        contact["followup_stage"] = 0
    return {
        "ok": True,
        "chat_title": chat_title,
        "incoming_text": incoming_text,
        "reply_text": reply_text,
        "delivered_at": sent_at,
    }


def _wa_open_chat_by_title(page: Any, title: str) -> bool:
    expected = (title or "").strip().lower()
    if not expected:
        return False
    expected_norm = " ".join(expected.split())
    row_selector = _join_selectors(_WASelectors.CHAT_ROW_CANDIDATES)
    rows = page.locator(row_selector)
    for idx in range(min(_wa_locator_count(rows), 300)):
        row = rows.nth(idx)
        text_bits: list[str] = []
        for sel in _WASelectors.CHAT_TITLE_IN_ROW:
            try:
                nodes = row.locator(sel)
                for n in range(min(_wa_locator_count(nodes), 4)):
                    value = (
                        nodes.nth(n).get_attribute("title")
                        or nodes.nth(n).inner_text()
                        or ""
                    ).strip()
                    if value:
                        text_bits.append(value.lower())
            except Exception:
                continue
        normalized_bits = [" ".join(bit.split()) for bit in text_bits]
        if expected_norm in normalized_bits:
            try:
                row.click(timeout=5000)
                return True
            except Exception:
                continue
    return False


def _wa_reply_on_current_chat(
    sender: dict[str, Any],
    message: str,
    *,
    expected_chat_title: str | None = None,
) -> dict[str, Any]:
    """Envía un mensaje en el chat actualmente abierto (flujo de unread -> autoreply)."""
    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")
            state = _wa_wait_for_login_state(page, timeout_seconds=12.0)
            if state == "qr":
                return {
                    "success": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp no está vinculada (QR visible).",
                    "session_expired": True,
                }

            if expected_chat_title:
                opened = _wa_open_chat_by_title(page, expected_chat_title)
                if not opened:
                    return {
                        "success": False,
                        "code": "chat_switch_failed",
                        "reason": "No se encontró el chat objetivo para responder.",
                        "session_expired": False,
                    }
            else:
                row, reason = _wa_open_unread_chat(page)
                if row is None:
                    return {
                        "success": False,
                        "code": "no_unread",
                        "reason": reason,
                        "session_expired": False,
                    }

            input_box = _wa_find_visible_chat_input(page, timeout_ms=10000)
            if input_box is None:
                return {
                    "success": False,
                    "code": "input_missing",
                    "reason": "No se encontró el cuadro de mensaje en el chat activo.",
                    "session_expired": False,
                }
            try:
                input_box.click(timeout=5000)
            except Exception:
                try:
                    input_box.focus()
                except Exception:
                    return {
                        "success": False,
                        "code": "input_missing",
                        "reason": "No se pudo enfocar el cuadro de mensaje en el chat activo.",
                        "session_expired": False,
                    }

            for hotkey in ("Control+A", "Meta+A"):
                try:
                    page.keyboard.press(hotkey)
                    page.keyboard.press("Delete")
                    break
                except Exception:
                    continue

            try:
                before_count = _wa_locator_count(page.locator(_WASelectors.MESSAGE_CONTAINER[0]))
            except Exception:
                before_count = 0

            for idx, line in enumerate((message or "").splitlines() or [""]):
                if idx:
                    page.keyboard.press("Shift+Enter")
                page.keyboard.type(line or " ")
            page.keyboard.press("Enter")

            matched_bubble, confirmation = _wa_wait_outgoing_bubble(
                page,
                expected_text=message or "",
                before_count=before_count,
                timeout_ms=20000,
            )
            if matched_bubble is None:
                return {
                    "success": False,
                    "code": "send_unconfirmed",
                    "reason": "WhatsApp no confirmó la auto-respuesta en la conversación.",
                    "session_expired": False,
                }

            confirmation = confirmation or "enviado"
            if confirmation == "enviado":
                upgrade_deadline = time.time() + 4.0
                while time.time() < upgrade_deadline:
                    upgraded = _wa_confirmation_from_bubble(matched_bubble)
                    if upgraded in {"entregado", "leido"}:
                        confirmation = upgraded
                        break
                    page.wait_for_timeout(300)

            delivered_at = _now_iso()
            return {
                "success": True,
                "confirmation": confirmation,
                "note": "Respuesta automática enviada mediante Playwright.",
                "delivered_at": delivered_at,
                "session_expired": False,
            }
    except Exception as exc:  # noqa: BLE001
        _log_structured("whatsapp.autoreply.send.error", sender_id=sender.get("id"), error=str(exc))
        return {
            "success": False,
            "code": "playwright_error",
            "reason": f"Error al enviar auto-respuesta: {exc}",
            "session_expired": False,
        }


# ======================================================================
# ===== Follow-up Scheduler =============================================

def _should_followup_contact(
    contact: dict[str, Any],
    *,
    threshold: datetime,
) -> bool:
    # Base legacy + campos nuevos con comparación temporal real.
    last_message_raw = contact.get("last_message_at") or contact.get("last_message_sent_at")
    if not last_message_raw:
        return False
    sent_dt = _parse_iso(str(last_message_raw))
    if not sent_dt:
        return False

    responded_raw = contact.get("last_response_at") or contact.get("last_message_received_at")
    responded_dt = _parse_iso(str(responded_raw)) if responded_raw else None
    if responded_dt and responded_dt >= sent_dt:
        return False
    if contact.get("status") in {"pagó", "acceso enviado"}:
        return False
    return sent_dt <= threshold


def _followup_in_failure_backoff(contact: dict[str, Any]) -> bool:
    history = contact.get("history", [])
    if not isinstance(history, list):
        return False
    for entry in reversed(history[-30:]):
        if not isinstance(entry, dict):
            continue
        if (entry.get("type") or "").lower() != "followup_failed":
            continue
        attempted_at = _parse_iso(entry.get("attempted_at"))
        if not attempted_at:
            return False
        elapsed = (_now() - attempted_at).total_seconds()
        return elapsed < (_FOLLOWUP_FAILURE_RETRY_MINUTES * 60)
    return False


def _select_connected_sender(
    store: WhatsAppDataStore,
    *,
    preferred_id: str = "",
) -> dict[str, Any] | None:
    def _is_supported(sender: dict[str, Any]) -> bool:
        method = (sender.get("connection_method") or "").lower()
        return method == "playwright"

    preferred = (preferred_id or "").strip()
    if preferred:
        item = store.find_number(preferred)
        if item and item.get("connected") and _is_supported(item):
            return item
    for sender in store.iter_numbers():
        if sender.get("connected") and _is_supported(sender):
            return sender
    return None


def _history_to_conversation(history: list[dict[str, Any]], max_items: int = 12) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for entry in history[-40:]:
        typ = (entry.get("type") or "").lower()
        text = (
            entry.get("message")
            or entry.get("text")
            or entry.get("details")
            or ""
        )
        text = str(text).strip()
        if not text:
            continue
        if typ in {"incoming", "received", "reply"}:
            role = "user"
        elif typ in {"send", "followup", "payment", "payment_confirmed"}:
            role = "assistant"
        else:
            continue
        messages.append({"role": role, "content": text})
    return messages[-max_items:]


def _run_ai_automations(store: WhatsAppDataStore) -> bool:
    configs = store.state.setdefault("ai_automations", {})
    changed = False
    for number_id, cfg in list(configs.items()):
        config = store._ensure_ai_config(cfg)
        if config != cfg:
            configs[number_id] = config
            changed = True
        if not config.get("active"):
            continue

        sender = store.find_number(number_id)
        if not sender:
            continue
        if not sender.get("connected"):
            continue
        if (sender.get("connection_method") or "").lower() != "playwright":
            # Auto-reply unread hoy se soporta con Playwright.
            continue

        now_dt = _now()
        interval = max(
            _AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS,
            int(config.get("polling_interval_seconds") or 60),
        )
        throttle = max(10, int(config.get("scan_throttle_seconds") or _AUTOREPLY_DEFAULT_THROTTLE_SECONDS))
        jitter = max(0, int(config.get("scan_jitter_seconds") or _AUTOREPLY_DEFAULT_JITTER_SECONDS))
        next_scan_at = _parse_iso(config.get("next_scan_at"))
        if next_scan_at and next_scan_at > now_dt:
            continue
        last_scan = _parse_iso(config.get("last_scan_at"))
        if last_scan and (now_dt - last_scan).total_seconds() < throttle:
            continue

        try:
            result = _process_auto_reply_for_sender(store, sender, config)
            scan_ts = _now_iso()
            code = result.get("code")
            ok_result = bool(result.get("ok"))

            next_wait_seconds = float(max(interval, throttle))
            if ok_result:
                delay_cfg = config.get("delay") or {"min": 5.0, "max": 15.0}
                try:
                    min_delay = float(delay_cfg.get("min", 5.0))
                    max_delay = float(delay_cfg.get("max", min_delay))
                    if max_delay < min_delay:
                        max_delay = min_delay
                    # En vez de dormir el runner, trasladamos el delay humano al próximo scan.
                    next_wait_seconds += random.uniform(min_delay, max_delay)
                except Exception:
                    next_wait_seconds += random.uniform(0, float(max(jitter, 1)))
            else:
                if code == "no_unread":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_NO_UNREAD_BACKOFF_RANGE)
                elif code == "self_message":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_SELF_MESSAGE_BACKOFF_RANGE)
                elif code == "session_expired":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE)
                else:
                    next_wait_seconds = random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE)

            next_wait_seconds += random.uniform(0, float(jitter))
            next_scan_dt = _now() + timedelta(seconds=max(next_wait_seconds, float(throttle)))

            _log_structured(
                "whatsapp.autoreply.scan",
                sender_id=sender.get("id"),
                ok=ok_result,
                code=code,
                reason=result.get("reason"),
                next_scan_at=next_scan_dt.isoformat() + "Z",
                throttle_seconds=throttle,
            )
            config["last_scan_at"] = scan_ts
            config["next_scan_at"] = next_scan_dt.isoformat() + "Z"
            config["last_result"] = {
                "ok": ok_result,
                "code": code,
                "reason": result.get("reason"),
                "at": scan_ts,
            }
            changed = True

            if not ok_result and code == "session_expired":
                _mark_sender_session_expired(
                    sender,
                    note="Auto-reply detectó sesión cerrada. Repetí la vinculación escaneando el QR.",
                )
                changed = True
        except Exception as exc:  # noqa: BLE001
            _log_structured(
                "whatsapp.autoreply.error",
                sender_id=sender.get("id"),
                error=str(exc),
            )
            scan_ts = _now_iso()
            next_scan_dt = _now() + timedelta(seconds=random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE))
            config["last_scan_at"] = scan_ts
            config["next_scan_at"] = next_scan_dt.isoformat() + "Z"
            config["last_result"] = {
                "ok": False,
                "code": "runtime_error",
                "reason": str(exc),
                "at": scan_ts,
            }
            changed = True
    return changed


def _generate_followup_message(
    contact: dict[str, Any],
    config: dict[str, Any],
) -> str:
    mode = (config.get("auto_mode") or "manual").strip().lower()
    if mode.startswith("i"):
        incoming_text = (contact.get("last_message_received") or "").strip() or (
            "No hubo respuesta al mensaje anterior."
        )
        conv = _history_to_conversation(contact.get("history", []), max_items=12)
        ai = _generate_gpt_reply(
            incoming_text=incoming_text,
            conversation_history=conv,
            system_prompt=config.get("ai_prompt", ""),
        )
        if ai.get("ok"):
            return (ai.get("reply_text") or "").strip()
    return _render_message(config.get("manual_message", ""), contact).strip()


def _run_followup_scheduler(store: WhatsAppDataStore) -> bool:
    config = store.state.setdefault("followup", store._ensure_followup_config({}))
    if not config.get("auto_enabled"):
        return False
    last_auto = _parse_iso(config.get("last_auto_run_at"))
    if last_auto and (_now() - last_auto).total_seconds() < _FOLLOWUP_MIN_CYCLE_SECONDS:
        return False

    sender = _select_connected_sender(
        store,
        preferred_id=config.get("active_number_id", ""),
    )
    if not sender:
        _log_structured(
            "whatsapp.followup.auto.skipped",
            reason="no_supported_sender",
        )
        return False

    wait_minutes = max(10, int(config.get("default_wait_minutes") or 120))
    threshold = _now() - timedelta(minutes=wait_minutes)
    max_stage = max(1, int(config.get("max_stage") or 2))
    changed = False
    sent_count = 0
    max_per_cycle = 3

    for alias, data in store.iter_lists():
        if sent_count >= max_per_cycle:
            break
        for contact in data.get("contacts", []):
            if sent_count >= max_per_cycle:
                break
            stage = contact.get("followup_stage", 0)
            try:
                stage_int = int(stage)
            except Exception:
                stage_int = 0
            if stage_int >= max_stage:
                continue
            if _followup_in_failure_backoff(contact):
                continue
            if not _should_followup_contact(contact, threshold=threshold):
                continue

            message = _generate_followup_message(contact, config)
            if not message:
                continue

            send_result = _send_message_via_backend(
                sender,
                contact,
                {"message": message, "notes": "Seguimiento automático"},
            )
            if not send_result.get("success"):
                if send_result.get("session_expired"):
                    _mark_sender_session_expired(
                        sender,
                        note="El seguimiento automático detectó sesión cerrada. Repetí la vinculación escaneando el QR.",
                    )
                contact.setdefault("history", []).append(
                    {
                        "type": "followup_failed",
                        "attempted_at": _now_iso(),
                        "error": send_result.get("reason") or "No se pudo enviar el seguimiento automático.",
                        "code": send_result.get("code"),
                    }
                )
                changed = True
                continue

            delivered_at = send_result.get("delivered_at") or _now_iso()
            _sync_contact_state_followup(contact, followup_message=message, sent_at=delivered_at)
            contact["last_sender_id"] = sender.get("id")
            sent_count += 1
            changed = True

            config.setdefault("history", []).append(
                {
                    "executed_at": delivered_at,
                    "list_alias": alias,
                    "contact_number": contact.get("number"),
                    "mode": "ia" if (config.get("auto_mode") or "").startswith("i") else "manual",
                    "stage": contact.get("followup_stage"),
                    "status": "sent",
                }
            )

    config["last_auto_run_at"] = _now_iso()
    if sent_count:
        _log_structured(
            "whatsapp.followup.auto.sent",
            sender_id=sender.get("id"),
            count=sent_count,
            wait_minutes=wait_minutes,
        )
    return changed


def _mark_sender_session_expired(sender: dict[str, Any], *, note: str) -> None:
    sender["connected"] = False
    sender["connection_state"] = "fallido"
    sender["last_connected_at"] = None
    sender.setdefault("session_notes", []).append(
        {
            "created_at": _now_iso(),
            "text": note,
        }
    )


def _deliver_event(store: WhatsAppDataStore, run: dict[str, Any], event: dict[str, Any]) -> bool:
    if (event.get("status") or "") != "pendiente":
        return False
    delivered_at = _now_iso()
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "omitido"
        event["delivered_at"] = delivered_at
        event["confirmation"] = "no_enviado"
        event["error_code"] = "list_missing"
        event["notes"] = "La lista vinculada fue eliminada."
        _append_run_log(
            run,
            f"La lista '{run.get('list_alias')}' ya no existe. Se omitió el envío a {event.get('contact')}.",
        )
    else:
        contact = _locate_contact(contact_list, event.get("contact"))
        if not contact:
            event["status"] = "omitido"
            event["delivered_at"] = delivered_at
            event["confirmation"] = "no_enviado"
            event["error_code"] = "contact_missing"
            event["notes"] = "El contacto ya no está disponible en la lista."
            _append_run_log(
                run,
                f"No se encontró el contacto {event.get('contact')} dentro de la lista.",
            )
        else:
            validation = _update_contact_validation(contact)
            event["validation_status"] = validation.get("status")
            sender = store.find_number(run.get("number_id", ""))
            failure_reason: str | None = None
            failure_code: str | None = None
            delivery_result: dict[str, Any] | None = None
            if validation["status"] == "invalid":
                failure_reason = validation.get("message") or "Número inválido."
                failure_code = "invalid_number"
            elif not sender:
                failure_reason = "El número de envío ya no está registrado."
                failure_code = "sender_missing"
            elif not sender.get("connected"):
                failure_reason = "La sesión de WhatsApp seleccionada no está activa."
                failure_code = "session_inactiva"
            elif (sender.get("connection_state") or "").lower() == "fallido":
                failure_reason = "La vinculación del número presentó un error reciente."
                failure_code = "session_error"
            else:
                delivery_result = _send_message_via_backend(sender, contact, event)
                if not delivery_result.get("success"):
                    failure_reason = (
                        delivery_result.get("reason")
                        or "WhatsApp no confirmó el envío del mensaje."
                    )
                    failure_code = delivery_result.get("code") or "send_failed"
                    if delivery_result.get("session_expired"):
                        _mark_sender_session_expired(
                            sender,
                            note="La sesión caducó durante un envío automático. Repetí la vinculación escaneando el QR.",
                        )
                        _append_run_log(
                            run,
                            "La sesión de WhatsApp se cerró durante el envío. Es necesario volver a vincular el número.",
                        )
                else:
                    delivered_at = delivery_result.get("delivered_at") or delivered_at

            if failure_reason:
                contact["status"] = "observado"
                contact.setdefault("history", []).append(
                    {
                        "type": "send_failed",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "attempted_at": delivered_at,
                        "error": failure_reason,
                    }
                )
                event["status"] = "fallido"
                event["notes"] = failure_reason
                event["error_code"] = failure_code
                event["confirmation"] = "no_enviado"
                event["delivered_at"] = delivered_at
                _append_delivery_log(
                    contact,
                    run,
                    status="fallido",
                    reason=failure_reason,
                    confirmation="no_enviado",
                )
                _append_run_log(
                    run,
                    f"Fallo el envío a {contact.get('name') or contact.get('number')}: {failure_reason}",
                )
            else:
                confirmation_value = "entregado"
                success_note = event.get("notes") or "Mensaje enviado correctamente."
                if delivery_result:
                    confirmation_value = (
                        delivery_result.get("confirmation") or confirmation_value
                    )
                    success_note = delivery_result.get("note") or success_note
                contact["status"] = "mensaje enviado"
                contact["last_message_at"] = event.get("scheduled_at") or delivered_at
                contact["last_message_sent"] = event.get("message", "")
                contact["last_message_sent_at"] = delivered_at
                contact["last_sender_id"] = run.get("number_id")
                contact.setdefault("history", []).append(
                    {
                        "type": "send",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "sent_at": delivered_at,
                        "delay": run.get("delay"),
                        "confirmation": confirmation_value,
                    }
                )
                event["status"] = "enviado"
                event["delivered_at"] = delivered_at
                event["confirmation"] = confirmation_value
                event["error_code"] = None
                event["notes"] = success_note
                _append_delivery_log(
                    contact,
                    run,
                    status="enviado",
                    reason=success_note,
                    confirmation=confirmation_value,
                )
                _append_run_log(
                    run,
                    "WhatsApp confirmó el mensaje para {} con estado {}.".format(
                        contact.get("name") or contact.get("number"),
                        confirmation_value,
                    ),
                )
    run["last_activity_at"] = delivered_at
    _refresh_run_counters(run)
    if (event.get("status") or "") in {"enviado", "fallido"}:
        run["status"] = "en progreso"
    return True


def _locate_contact(contact_list: dict[str, Any], number: str | None) -> dict[str, Any] | None:
    if not number:
        return None
    for contact in contact_list.get("contacts", []):
        if contact.get("number") == number:
            return contact
    return None


def _mark_contact_scheduled(
    contact: dict[str, Any],
    run_id: str,
    message: str,
    scheduled_at: str,
    min_delay: float,
    max_delay: float,
) -> None:
    preview = textwrap.shorten(message, width=80, placeholder="…") if message else ""
    contact.setdefault("history", []).append(
        {
            "type": "scheduled",
            "run_id": run_id,
            "scheduled_at": scheduled_at,
            "message": preview,
            "delay": {"min": min_delay, "max": max_delay},
        }
    )
    current_status = (contact.get("status") or "").lower()
    if not current_status or any(hint in current_status for hint in ("sin", "espera", "program")):
        contact["status"] = "mensaje programado"


def _reset_contact_for_cancellation(
    store: WhatsAppDataStore,
    run: dict[str, Any],
    event: dict[str, Any],
) -> None:
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "cancelado"
        event["delivered_at"] = _now_iso()
        return
    contact = _locate_contact(contact_list, event.get("contact"))
    event["status"] = "cancelado"
    event["delivered_at"] = _now_iso()
    if not contact:
        return
    history = contact.setdefault("history", [])
    history.append(
        {
            "type": "cancelled",
            "run_id": run.get("id"),
            "scheduled_at": event.get("scheduled_at"),
            "cancelled_at": event.get("delivered_at"),
        }
    )
    current_status = (contact.get("status") or "").lower()
    if "program" in current_status and not contact.get("last_message_at"):
        contact["status"] = "sin mensaje"

def _choose_number(store: WhatsAppDataStore) -> dict[str, Any] | None:
    options = list(store.iter_numbers())
    if not options:
        return None
    print(_line())
    _subtitle("Seleccioná el número de envío")
    for idx, item in enumerate(options, 1):
        if item.get("connected"):
            status = "🟢 verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 error"
        else:
            status = "⚪ pendiente"
        print(f"{idx}) {item.get('alias')} ({item.get('phone')}) - {status}")
    idx = ask_int("Número elegido: ", min_value=1)
    if idx > len(options):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return options[idx - 1]


def _choose_contact_list(store: WhatsAppDataStore) -> dict[str, Any] | None:
    lists = list(store.iter_lists())
    if not lists:
        return None
    print(_line())
    _subtitle("Seleccioná la lista de contactos")
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Lista elegida: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    alias, data = lists[idx - 1]
    data["alias"] = alias
    return data


def _ask_delay_range() -> tuple[float, float]:
    while True:
        try:
            min_delay = float(ask("Delay mínimo (segundos): ").strip())
            max_delay = float(ask("Delay máximo (segundos): ").strip())
        except ValueError:
            _info("Ingresá números válidos para los delays.", color=Fore.YELLOW)
            continue
        if min_delay <= 0 or max_delay <= 0:
            _info("Los delays deben ser mayores a cero.", color=Fore.YELLOW)
            continue
        if max_delay < min_delay:
            _info("El máximo debe ser mayor o igual al mínimo.", color=Fore.YELLOW)
            continue
        return min_delay, max_delay


def _render_message(template: str, contact: dict[str, Any]) -> str:
    safe_contact = {"nombre": contact.get("name", ""), "numero": contact.get("number", "")}
    try:
        return template.format(**{"nombre": safe_contact["nombre"], "numero": safe_contact["numero"]})
    except KeyError:
        return template


# ----------------------------------------------------------------------
# 4) Automatizar respuestas con IA -------------------------------------

def _configure_ai_responses(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    configs = store.state.setdefault("ai_automations", {})
    current = configs.get(number["id"], store._ensure_ai_config({}))
    while True:
        banner()
        title("Automatización de respuestas con IA")
        print(_line())
        _info(f"Número seleccionado: {number.get('alias')} ({number.get('phone')})", bold=True)
        status = "🟢 activo" if current.get("active") else "⚪ en espera"
        print(f"Estado actual: {status}")
        print(f"Delay configurado: {_format_delay(current.get('delay', {'min': 5.0, 'max': 15.0}))}")
        print(f"Escaneo de no leídos cada: {current.get('polling_interval_seconds', 60)}s")
        print(f"Throttle mínimo entre scans: {current.get('scan_throttle_seconds', _AUTOREPLY_DEFAULT_THROTTLE_SECONDS)}s")
        print(f"Jitter de escaneo: ±{current.get('scan_jitter_seconds', _AUTOREPLY_DEFAULT_JITTER_SECONDS)}s")
        prompt_preview = textwrap.shorten(current.get("prompt", ""), width=90, placeholder="…")
        print(f"Prompt base: {prompt_preview or '(sin definir)'}")
        print(f"Envío de audios: {'sí' if current.get('send_audio') else 'no'}")
        print(_line())
        print("1) Activar o actualizar configuración")
        print("2) Pausar automatización para este número")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            prompt = ask_multiline("Prompt guía para la IA: ").strip() or current.get("prompt", "")
            min_delay, max_delay = _ask_delay_range()
            polling_interval = ask_int(
                "Intervalo de escaneo de chats no leídos (segundos): ",
                min_value=_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS,
                default=int(current.get("polling_interval_seconds", 60) or 60),
            )
            throttle_seconds = ask_int(
                "Throttle mínimo entre scans (segundos): ",
                min_value=10,
                default=int(current.get("scan_throttle_seconds", _AUTOREPLY_DEFAULT_THROTTLE_SECONDS) or _AUTOREPLY_DEFAULT_THROTTLE_SECONDS),
            )
            jitter_seconds = ask_int(
                "Jitter aleatorio adicional de scan (segundos): ",
                min_value=0,
                default=int(current.get("scan_jitter_seconds", _AUTOREPLY_DEFAULT_JITTER_SECONDS) or _AUTOREPLY_DEFAULT_JITTER_SECONDS),
            )
            audio = ask("¿Enviar audios cuando sea posible? (s/n): ").strip().lower().startswith("s")
            current.update(
                {
                    "active": True,
                    "prompt": prompt,
                    "delay": {"min": min_delay, "max": max_delay},
                    "polling_interval_seconds": polling_interval,
                    "scan_throttle_seconds": throttle_seconds,
                    "scan_jitter_seconds": jitter_seconds,
                    "send_audio": audio,
                    "last_updated_at": _now_iso(),
                }
            )
            configs[number["id"]] = current
            store.save()
            ok("Automatización actualizada. Se responderá siguiendo un tono humano y cordial.")
            press_enter()
        elif op == "2":
            current["active"] = False
            current["last_updated_at"] = _now_iso()
            configs[number["id"]] = current
            store.save()
            ok("Automatización pausada para este número.")
            press_enter()
        elif op == "3":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 5) Captura desde Instagram -------------------------------------------

def _instagram_capture(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("instagram", store._ensure_instagram_config({}))
    while True:
        banner()
        title("Captura de números desde Instagram")
        print(_line())
        print(f"Estado: {'🟢 activo' if config.get('active') else '⚪ en pausa'}")
        print(f"Mensaje inicial: {textwrap.shorten(config.get('message', ''), width=80, placeholder='…')}")
        print(f"Delay configurado: {_format_delay(config.get('delay', {'min': 5.0, 'max': 12.0}))}")
        print(f"Total de capturas: {len(config.get('captures', []))}")
        print(_line())
        print("1) Configurar mensaje y delays")
        print("2) Registrar número capturado manualmente")
        print("3) Ver seguimiento de conversiones")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            active = ask("¿Activar la escucha automática? (s/n): ").strip().lower().startswith("s")
            message = ask_multiline("Mensaje inicial automático: ").strip() or config.get("message", "")
            min_delay, max_delay = _ask_delay_range()
            config.update(
                {
                    "active": active,
                    "message": message,
                    "delay": {"min": min_delay, "max": max_delay},
                    "last_reviewed_at": _now_iso(),
                }
            )
            store.save()
            ok("Integración actualizada. Los leads de Instagram se contactarán de forma natural.")
            press_enter()
        elif op == "2":
            _register_instagram_capture(store, config)
        elif op == "3":
            _show_instagram_tracking(config)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _register_instagram_capture(store: WhatsAppDataStore, config: dict[str, Any]) -> None:
    name = ask("Nombre de la persona: ").strip()
    number = ask("Número detectado: ").strip()
    if not number:
        _info("Se requiere un número válido.", color=Fore.YELLOW)
        press_enter()
        return
    source = ask("Origen o nota de la conversación (opcional): ").strip() or "Instagram"
    capture = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "source": source,
        "captured_at": _now_iso(),
        "message_sent": False,
        "message_sent_at": None,
        "notes": "",
    }
    delay = config.get("delay", {"min": 5.0, "max": 12.0})
    message = config.get("message", "")
    if message:
        capture["message_sent"] = True
        capture["message_sent_at"] = _now_iso()
        capture["notes"] = (
            f"Mensaje inicial programado con delays humanos {_format_delay(delay)}."
        )
    config.setdefault("captures", []).append(capture)
    _auto_add_to_master_list(store, capture)
    store.save()
    ok("Lead capturado y mensaje inicial configurado correctamente.")
    press_enter()


def _auto_add_to_master_list(store: WhatsAppDataStore, capture: dict[str, Any]) -> None:
    lists = store.state.setdefault("contact_lists", {})
    alias = "instagram_auto"
    contact = {
        "name": capture.get("name", capture.get("number", "")),
        "number": capture.get("number", ""),
        "status": "mensaje enviado" if capture.get("message_sent") else "sin mensaje",
        "last_message_at": capture.get("message_sent_at"),
        "last_response_at": None,
        "last_message_sent": capture.get("notes", "") if capture.get("message_sent") else "",
        "last_message_sent_at": capture.get("message_sent_at"),
        "last_message_received": "",
        "last_message_received_at": None,
        "last_sender_id": None,
        "followup_stage": 0,
        "last_followup_at": None,
        "last_payment_at": None,
        "access_sent_at": None,
        "notes": capture.get("source", "Instagram"),
        "history": [
            {
                "type": "captured",
                "source": capture.get("source", "Instagram"),
                "timestamp": capture.get("captured_at"),
            }
        ],
    }
    validation = _validate_phone_number(contact.get("number", ""))
    contact["number"] = validation.get("normalized") or contact.get("number", "")
    contact.setdefault("history", []).append(
        {
            "type": "validation",
            "status": validation["status"],
            "checked_at": validation["checked_at"],
            "message": validation["message"],
        }
    )
    contact["validation"] = validation
    contact["delivery_log"] = []
    if capture.get("message_sent"):
        contact["history"].append(
            {
                "type": "send",
                "message": capture.get("notes", ""),
                "timestamp": capture.get("message_sent_at"),
            }
        )
    if alias not in lists:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": [contact],
            "notes": "Leads generados automáticamente desde Instagram",
        }
    else:
        lists[alias]["contacts"].append(contact)


def _show_instagram_tracking(config: dict[str, Any]) -> None:
    banner()
    title("Seguimiento de conversiones desde Instagram")
    print(_line())
    captures = config.get("captures", [])
    if not captures:
        _info("Aún no hay capturas registradas.")
        press_enter()
        return
    for item in captures:
        status = "mensaje enviado" if item.get("message_sent") else "pendiente"
        print(
            f"• {item.get('name')} ({item.get('number')}) - {status} | "
            f"Detectado: {item.get('captured_at')} | Origen: {item.get('source')}"
        )
    press_enter()


# ----------------------------------------------------------------------
# 6) Seguimiento a no respondidos --------------------------------------

def _followup_manager(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("followup", store._ensure_followup_config({}))
    banner()
    title("Seguimiento automático de contactos sin respuesta")
    print(_line())
    wait_minutes = ask_int(
        "¿Cuántos minutos esperar antes de etiquetar como no respondido?: ",
        min_value=10,
        default=config.get("default_wait_minutes", 120),
    )
    config["default_wait_minutes"] = wait_minutes
    auto_enabled = ask(
        "¿Activar el seguimiento automático en segundo plano? (s/N): "
    ).strip().lower().startswith("s")
    config["auto_enabled"] = auto_enabled
    if auto_enabled:
        number = _choose_number(store)
        if number:
            config["active_number_id"] = number.get("id")
            _info(
                f"Número para seguimiento automático: {number.get('alias')} ({number.get('phone')})"
            )
        max_stage = ask_int(
            "Máximo de seguimientos automáticos por contacto: ",
            min_value=1,
            default=int(config.get("max_stage", 2) or 2),
        )
        config["max_stage"] = max_stage
    threshold = _now() - timedelta(minutes=wait_minutes)
    candidates = _find_followup_candidates(store, threshold)
    if not candidates:
        _info("No hay contactos pendientes de seguimiento en este momento.")
        store.save()
        press_enter()
        return
    _info(f"Se encontraron {len(candidates)} contactos sin respuesta.", bold=True)
    mode = ask(
        "¿Enviar mensaje personalizado (p) o generar con IA (i)? [p/i]: "
    ).strip().lower()
    config["auto_mode"] = "ia" if mode.startswith("i") else "manual"
    if mode.startswith("i"):
        prompt = ask_multiline("Prompt base para el seguimiento (opcional): ").strip() or config.get(
            "ai_prompt", ""
        )
        config["ai_prompt"] = prompt
        message_base = (
            "Mensaje generado automáticamente siguiendo un tono humano cercano y cordial."
        )
    else:
        message_base = ask_multiline("Mensaje de seguimiento: ").strip() or config.get(
            "manual_message", ""
        )
        config["manual_message"] = message_base
    min_delay, max_delay = _ask_delay_range()
    for entry in candidates:
        contact = entry["contact"]
        if mode.startswith("i"):
            personalized = _generate_followup_message(contact, config) or _render_message(
                config.get("manual_message", ""), contact
            )
        else:
            personalized = _render_message(message_base, contact)
        _sync_contact_state_followup(
            contact,
            followup_message=personalized,
            sent_at=_now_iso(),
        )
        # Compatibilidad con tracking previo
        contact.setdefault("history", []).append(
            {
                "type": "followup_schedule",
                "message": personalized,
                "delay": {"min": min_delay, "max": max_delay},
                "sent_at": _now_iso(),
            }
        )
    config.setdefault("history", []).append(
        {
            "executed_at": _now_iso(),
            "count": len(candidates),
            "delay": {"min": min_delay, "max": max_delay},
            "mode": "ia" if mode.startswith("i") else "manual",
        }
    )
    store.save()
    ok("Seguimiento configurado y mensajes programados con comportamiento humano natural.")
    press_enter()


def _find_followup_candidates(store: WhatsAppDataStore, threshold: datetime) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for alias, data in store.iter_lists():
        for contact in data.get("contacts", []):
            if _should_followup_contact(contact, threshold=threshold):
                results.append({"list": alias, "contact": contact})
    return results


# ----------------------------------------------------------------------
# 7) Gestión de pagos ---------------------------------------------------

def _payments_menu(store: WhatsAppDataStore) -> None:
    payments = store.state.setdefault("payments", store._ensure_payments_config({}))
    while True:
        banner()
        title("Gestión de pagos y entrega de accesos")
        print(_line())
        print(f"Administrador notificaciones: {payments.get('admin_number') or '(sin definir)'}")
        print(f"Pagos pendientes: {len(payments.get('pending', []))}")
        print(f"Pagos completados: {len(payments.get('history', []))}")
        print(_line())
        print("1) Procesar nueva captura de pago")
        print("2) Revisar pendientes y enviar accesos")
        print("3) Configurar mensajes y datos del administrador")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _process_payment_capture(store, payments)
        elif op == "2":
            _review_pending_payments(store, payments)
        elif op == "3":
            _configure_payment_settings(store, payments)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _process_payment_capture(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    name = ask("Nombre del contacto: ").strip()
    number = ask("Número de WhatsApp: ").strip()
    evidence = ask("Ruta de la captura o palabras clave detectadas: ").strip()
    detected_keywords = _detect_keywords(evidence)
    status = "validado" if _is_payment_valid(detected_keywords) else "pendiente"
    entry = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "evidence": evidence,
        "keywords": detected_keywords,
        "status": status,
        "created_at": _now_iso(),
        "validated_at": _now_iso() if status == "validado" else None,
        "welcome_sent_at": None,
        "alert_sent_at": None,
        "notes": "",
    }
    payments.setdefault("pending", []).append(entry)
    if status != "validado":
        entry["alert_sent_at"] = _now_iso()
        _notify_admin(payments, entry)
    else:
        _finalize_payment(store, payments, entry, auto=True)
    store.save()
    ok("Pago registrado. El flujo de validación continúa en segundo plano.")
    press_enter()


def _detect_keywords(evidence: str) -> list[str]:
    lowered = evidence.lower()
    keywords = []
    for hint in ("aprob", "pago", "$", "transfer", "ok", "exitoso"):
        if hint in lowered:
            keywords.append(hint)
    return keywords


def _is_payment_valid(keywords: list[str]) -> bool:
    return any(hint in keywords for hint in ("aprob", "pago", "$", "exitoso"))


def _notify_admin(payments: dict[str, Any], entry: dict[str, Any]) -> None:
    admin = payments.get("admin_number")
    if not admin:
        return
    entry["notes"] = (
        f"Alerta enviada al administrador {admin} para validar el pago de {entry.get('name')}"
    )


def _review_pending_payments(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    pending = payments.get("pending", [])
    if not pending:
        _info("No hay pagos pendientes.")
        press_enter()
        return
    for entry in pending:
        print(_line())
        print(f"Contacto: {entry.get('name')} ({entry.get('number')})")
        print(f"Palabras clave detectadas: {', '.join(entry.get('keywords', [])) or 'ninguna'}")
        print(f"Estado actual: {entry.get('status')}")
        decision = ask("¿Marcar como confirmado (c), rechazar (r) o saltar (s)? ").strip().lower()
        if decision.startswith("c"):
            entry["status"] = "validado"
            entry["validated_at"] = _now_iso()
            _finalize_payment(store, payments, entry)
        elif decision.startswith("r"):
            entry["status"] = "rechazado"
            entry["notes"] = "El pago requiere nueva evidencia."
            _send_custom_message(store, entry, "Pago observado. Por favor compartinos una captura clara.")
    payments["pending"] = [
        item for item in pending if item.get("status") not in {"finalizado", "rechazado"}
    ]
    store.save()
    press_enter()


def _finalize_payment(
    store: WhatsAppDataStore,
    payments: dict[str, Any],
    entry: dict[str, Any],
    *,
    auto: bool = False,
) -> None:
    message = payments.get("welcome_message", "")
    link = payments.get("access_link", "")
    composed = message
    if link:
        composed = f"{message}\n{link}" if message else link
    _send_custom_message(store, entry, composed)
    entry["status"] = "finalizado"
    entry["welcome_sent_at"] = _now_iso()
    payments.setdefault("history", []).append(
        {
            "id": entry.get("id"),
            "name": entry.get("name"),
            "number": entry.get("number"),
            "status": "completado",
            "completed_at": _now_iso(),
            "notes": "Procesado automáticamente" if auto else entry.get("notes", ""),
        }
    )
    _update_contact_payment_status(store, entry)


def _send_custom_message(store: WhatsAppDataStore, entry: dict[str, Any], message: str) -> None:
    if not message:
        return
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if contact:
        contact.setdefault("history", []).append(
            {
                "type": "payment",
                "message": message,
                "sent_at": _now_iso(),
            }
        )
        contact["status"] = "acceso enviado"
        contact["access_sent_at"] = _now_iso()
        contact["last_payment_at"] = _now_iso()
        contact["last_message_sent"] = message
        contact["last_message_sent_at"] = _now_iso()
        contact["last_message_at"] = _now_iso()
    entry["notes"] = message


def _update_contact_payment_status(store: WhatsAppDataStore, entry: dict[str, Any]) -> None:
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if not contact:
        return
    contact["status"] = "pagó"
    contact["last_payment_at"] = _now_iso()
    contact.setdefault("history", []).append(
        {
            "type": "payment_confirmed",
            "timestamp": _now_iso(),
            "details": entry.get("notes", ""),
        }
    )


def _locate_contact_by_number(store: WhatsAppDataStore, number: str) -> dict[str, Any] | None:
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            if contact.get("number") == number:
                return contact
    return None


def _configure_payment_settings(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    admin = ask("Número del administrador para alertas: ").strip()
    welcome = ask_multiline("Mensaje de bienvenida tras confirmar pago: ").strip() or payments.get(
        "welcome_message", ""
    )
    link = ask("Link de acceso (opcional): ").strip() or payments.get("access_link", "")
    payments.update(
        {
            "admin_number": admin,
            "welcome_message": welcome,
            "access_link": link,
        }
    )
    store.save()
    ok("Datos actualizados. Los pagos se gestionarán con notificaciones limpias.")
    press_enter()


# ----------------------------------------------------------------------
# 8) Estado de contactos y actividad -----------------------------------

def _contacts_state(store: WhatsAppDataStore) -> None:
    banner()
    title("Estado general de contactos y actividad")
    print(_line())
    lists = list(store.iter_lists())
    if not lists:
        _info("Todavía no se cargaron listas de contactos.")
        press_enter()
        return
    totals = []
    for alias, data in lists:
        contacts = data.get("contacts", [])
        summary = _summarize_contacts(contacts)
        totals.append(summary)
        print(f"Lista: {alias}")
        for key, value in summary.items():
            print(f"   - {key}: {value}")
        print()
    if ask("¿Deseás exportar un CSV con el detalle? (s/n): ").strip().lower().startswith("s"):
        path = _export_contacts_csv(store)
        ok(f"Resumen exportado en {path}")
    press_enter()


def _summarize_contacts(contacts: Iterable[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "Total": 0,
        "Mensaje enviado": 0,
        "En espera": 0,
        "Respondió": 0,
        "Pagó": 0,
        "Acceso enviado": 0,
    }
    for contact in contacts:
        summary["Total"] += 1
        status = (contact.get("status") or "").lower()
        if "seguimiento" in status or "sin" in status:
            summary["En espera"] += 1
        if "mensaje" in status:
            summary["Mensaje enviado"] += 1
        if "respond" in status:
            summary["Respondió"] += 1
        if "pag" in status:
            summary["Pagó"] += 1
        if "acceso" in status:
            summary["Acceso enviado"] += 1
    return summary


def _export_contacts_csv(store: WhatsAppDataStore) -> Path:
    now = _now().strftime("%Y%m%d-%H%M%S")
    path = EXPORTS_DIR / f"whatsapp_estado_{now}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "lista",
                "nombre",
                "numero",
                "status",
                "ultimo_mensaje",
                "ultima_respuesta",
                "ultimo_seguimiento",
                "ultimo_pago",
                "acceso_enviado",
            ]
        )
        for alias, data in store.iter_lists():
            for contact in data.get("contacts", []):
                writer.writerow(
                    [
                        alias,
                        contact.get("name"),
                        contact.get("number"),
                        contact.get("status"),
                        contact.get("last_message_at"),
                        contact.get("last_response_at"),
                        contact.get("last_followup_at"),
                        contact.get("last_payment_at"),
                        contact.get("access_sent_at"),
                    ]
                )
    return path


__all__ = ["menu_whatsapp"]

=======
    _register_playwright_runtime_cleanup()
    _log_structured(
        "whatsapp.playwright.runtime.created",
        user_data_dir=str(user_data_dir),
        headless=bool(headless),
    )
    return runtime


def _get_or_create_playwright_runtime(
    *,
    sender: dict[str, Any] | None = None,
    headless: bool = False,
) -> _PlaywrightRuntime:
    global _PLAYWRIGHT_RUNTIME
    requested_dir = _resolve_playwright_user_data_dir(sender).resolve()
    requested_headless = bool(headless)

    stale_runtime: _PlaywrightRuntime | None = None
    with _PLAYWRIGHT_RUNTIME_LOCK:
        runtime = _PLAYWRIGHT_RUNTIME
        if runtime is not None:
            same_profile = runtime.user_data_dir.resolve() == requested_dir
            same_headless = runtime.headless == requested_headless
            if same_profile and same_headless and _playwright_runtime_healthcheck(runtime):
                runtime.last_used_at = _now_iso()
                return runtime
            stale_runtime = runtime
            old_dir = str(runtime.user_data_dir)
            _PLAYWRIGHT_RUNTIME = None
        else:
            old_dir = None

    if old_dir:
        _log_structured(
            "whatsapp.playwright.runtime.recreate",
            old_user_data_dir=old_dir,
            new_user_data_dir=str(requested_dir),
        )
        _close_playwright_runtime_instance(stale_runtime)

    runtime = _create_playwright_runtime(requested_dir, headless=requested_headless)
    with _PLAYWRIGHT_RUNTIME_LOCK:
        _PLAYWRIGHT_RUNTIME = runtime
    return runtime


@contextlib.contextmanager
def _playwright_persistent_page(*, sender: dict[str, Any] | None = None, headless: bool = False):
    """
    Devuelve siempre el mismo contexto/página Playwright para el runtime actual
    (mientras perfil/headless coincidan), evitando recrearlo por ciclo.
    """
    with _PLAYWRIGHT_LOCK:
        runtime = _get_or_create_playwright_runtime(sender=sender, headless=headless)
        if not _playwright_runtime_healthcheck(runtime):
            _shutdown_playwright_runtime()
            runtime = _get_or_create_playwright_runtime(sender=sender, headless=headless)
        runtime.last_used_at = _now_iso()
        try:
            yield runtime.page, runtime.context
        finally:
            runtime.last_used_at = _now_iso()


# ======================================================================
# ===== Message Sending ==================================================

def _extract_playwright_bubble_text(bubble: Any) -> str:
    texts: list[str] = []
    selector = _join_selectors(_WASelectors.BUBBLE_TEXT)
    try:
        candidates = bubble.locator(selector)
        count = candidates.count()
    except Exception:
        count = 0
        candidates = None
    for idx in range(min(count, 30)):
        try:
            text = (candidates.nth(idx).inner_text() or "").strip()  # type: ignore[union-attr]
        except Exception:
            text = ""
        if text:
            texts.append(text)
    merged = "\n".join(texts).strip()
    if merged:
        return merged
    try:
        fallback = (bubble.inner_text() or "").strip()
    except Exception:
        fallback = ""
    return fallback


def _wa_bubble_direction(bubble: Any) -> str:
    try:
        cls = (bubble.get_attribute("class") or "").lower()
    except Exception:
        cls = ""
    if "message-out" in cls:
        return "outgoing"
    if "message-in" in cls:
        return "incoming"
    try:
        if bubble.locator(_WASelectors.CHECK_READ).count() > 0:
            return "outgoing"
        if bubble.locator(_WASelectors.CHECK_DELIVERED).count() > 0:
            return "outgoing"
        if bubble.locator(_WASelectors.CHECK_SENT).count() > 0:
            return "outgoing"
    except Exception:
        pass
    try:
        if bubble.locator(_join_selectors(_WASelectors.BUBBLE_OUT)).count() > 0:
            return "outgoing"
        if bubble.locator(_join_selectors(_WASelectors.BUBBLE_IN)).count() > 0:
            return "incoming"
    except Exception:
        pass
    return "unknown"


def _wa_confirmation_from_bubble(bubble: Any) -> str:
    try:
        if bubble.locator(_WASelectors.CHECK_READ).count() > 0:
            return "leido"
        if bubble.locator(_WASelectors.CHECK_DELIVERED).count() > 0:
            return "entregado"
        if bubble.locator(_WASelectors.CHECK_SENT).count() > 0:
            return "enviado"
    except Exception:
        return "enviado"
    return "enviado"


def _wa_wait_outgoing_bubble(
    page: Any,
    *,
    expected_text: str,
    before_count: int,
    timeout_ms: int = 25000,
) -> tuple[Any | None, str]:
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    expected = _normalize_message(expected_text)
    deadline = time.time() + max(timeout_ms, 1000) / 1000.0

    while time.time() < deadline:
        try:
            nodes = page.locator(container_selector)
            total = _wa_locator_count(nodes)
        except Exception:
            total = 0
            nodes = None
        if total <= 0 or nodes is None:
            page.wait_for_timeout(250)
            continue
        if before_count > 0 and total <= before_count:
            page.wait_for_timeout(250)
            continue

        if before_count > 0:
            start = max(0, min(before_count, total - 1))
        else:
            start = max(0, total - 8)
        for idx in range(total - 1, start - 1, -1):
            bubble = nodes.nth(idx)
            text = _normalize_message(_extract_playwright_bubble_text(bubble))
            if expected and text and (expected in text or text in expected):
                direction = _wa_bubble_direction(bubble)
                confirmation = _wa_confirmation_from_bubble(bubble)
                if direction == "outgoing":
                    return bubble, confirmation

        if total > before_count:
            for idx in range(total - 1, start - 1, -1):
                bubble = nodes.nth(idx)
                if _wa_bubble_direction(bubble) == "outgoing":
                    return bubble, _wa_confirmation_from_bubble(bubble)

        page.wait_for_timeout(300)
    return None, ""


def _send_with_playwright(
    sender: dict[str, Any], contact: dict[str, Any], message: str
) -> dict[str, Any]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    except Exception:
        return {
            "success": False,
            "code": "playwright_missing",
            "reason": "Playwright no está instalado. Ejecutá 'pip install playwright' y luego 'playwright install'.",
            "session_expired": False,
        }

    digits = "".join(ch for ch in (contact.get("number") or "") if ch.isdigit())
    if not digits:
        return {
            "success": False,
            "code": "invalid_number",
            "reason": "El número no tiene dígitos suficientes para WhatsApp.",
            "session_expired": False,
        }

    typed_message = message or ""
    if not typed_message.strip():
        return {
            "success": False,
            "code": "empty_message",
            "reason": "El mensaje quedó vacío y no se envió a WhatsApp.",
            "session_expired": False,
        }

    delivered_at = _now_iso()
    session_expired = False
    trace_id = str(uuid.uuid4())

    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")

            state = _wa_wait_for_login_state(page, timeout_seconds=15.0)
            if state == "qr":
                session_expired = True
                snapshot_path = _resolve_playwright_user_data_dir(sender) / "qr.png"
                try:
                    qr_selector = _join_selectors(_WASelectors.QR_CANVAS)
                    page.locator(qr_selector).first.screenshot(path=str(snapshot_path))
                except Exception:
                    try:
                        page.screenshot(path=str(snapshot_path))
                    except Exception:
                        pass
                _log_structured(
                    "whatsapp.session_expired",
                    trace_id=trace_id,
                    sender_id=sender.get("id"),
                    sender_alias=sender.get("alias"),
                    reason="QR visible (sesión no logueada).",
                )
                return {
                    "success": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp caducó o no está vinculada. Volvé a escanear el código QR.",
                    "session_expired": True,
                }

            if state == "timeout" and not _wa_is_ready(page):
                return {
                    "success": False,
                    "code": "whatsapp_unreachable",
                    "reason": "WhatsApp Web no respondió a tiempo. Intentá nuevamente en unos minutos.",
                    "session_expired": False,
                }

            page.goto(f"{WHATSAPP_WEB_URL}/send?phone={digits}", wait_until="domcontentloaded")
            input_box = _wa_find_visible_chat_input(page, timeout_ms=45000)
            if input_box is None:
                alert = _collect_playwright_alert_text(page) or (
                    "No se pudo abrir la conversación. Confirmá que el número tenga WhatsApp."
                )
                _log_structured(
                    "whatsapp.chat_unavailable",
                    trace_id=trace_id,
                    sender_id=sender.get("id"),
                    contact_number=contact.get("number"),
                    reason=alert,
                )
                return {
                    "success": False,
                    "code": "chat_unavailable",
                    "reason": alert,
                    "session_expired": False,
                }
            try:
                input_box.click(timeout=5000)
            except Exception:
                try:
                    input_box.focus()
                except Exception:
                    return {
                        "success": False,
                        "code": "input_missing",
                        "reason": "No se encontró el cuadro de mensaje en WhatsApp Web.",
                        "session_expired": False,
                    }

            # Limpiar texto previo (si lo hubiera).
            for hotkey in ("Control+A", "Meta+A"):
                try:
                    page.keyboard.press(hotkey)
                    page.keyboard.press("Delete")
                    break
                except Exception:
                    continue

            # Conteo previo para confirmar envío.
            try:
                before_count = _wa_locator_count(page.locator(_WASelectors.MESSAGE_CONTAINER[0]))
            except Exception:
                before_count = 0

            # Tipeo multi-línea (Shift+Enter) y Enter para enviar.
            for idx, line in enumerate(typed_message.splitlines() or [""]):
                if idx:
                    page.keyboard.press("Shift+Enter")
                if line:
                    page.keyboard.type(line)
                else:
                    page.keyboard.type(" ")
            page.keyboard.press("Enter")

            normalized_message = _normalize_message(typed_message)
            matched_bubble, confirmation = _wa_wait_outgoing_bubble(
                page,
                expected_text=typed_message,
                before_count=before_count,
                timeout_ms=25000,
            )
            if matched_bubble is None:
                return {
                    "success": False,
                    "code": "send_unconfirmed",
                    "reason": "WhatsApp no confirmó el mensaje en la conversación.",
                    "session_expired": False,
                }

            # Espera corta para intentar escalar de "enviado" a "entregado/leído".
            confirmation = confirmation or "enviado"
            if confirmation == "enviado":
                upgrade_deadline = time.time() + 6.0
                while time.time() < upgrade_deadline:
                    upgraded = _wa_confirmation_from_bubble(matched_bubble)
                    if upgraded in {"entregado", "leido"}:
                        confirmation = upgraded
                        break
                    page.wait_for_timeout(400)

            bubble_text = _extract_playwright_bubble_text(matched_bubble)
            normalized_bubble = _normalize_message(bubble_text)
            if normalized_message and normalized_bubble:
                if (
                    normalized_message not in normalized_bubble
                    and normalized_bubble not in normalized_message
                ):
                    return {
                        "success": False,
                        "code": "text_mismatch",
                        "reason": "WhatsApp no mostró el contenido del mensaje enviado.",
                        "session_expired": False,
                    }

            _log_structured(
                "whatsapp.send.ok",
                trace_id=trace_id,
                sender_id=sender.get("id"),
                contact_number=contact.get("number"),
                confirmation=confirmation,
            )
            return {
                "success": True,
                "confirmation": confirmation,
                "note": "Mensaje confirmado en WhatsApp Web mediante Playwright.",
                "delivered_at": delivered_at,
                "session_expired": False,
            }
    except PlaywrightTimeoutError:
        _log_structured(
            "whatsapp.send.timeout",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            contact_number=contact.get("number"),
        )
        return {
            "success": False,
            "code": "timeout",
            "reason": "WhatsApp Web tardó demasiado en responder al enviar el mensaje.",
            "session_expired": session_expired,
        }
    except Exception as exc:  # noqa: BLE001
        _log_structured(
            "whatsapp.send.error",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            contact_number=contact.get("number"),
            error=str(exc),
        )
        return {
            "success": False,
            "code": "playwright_error",
            "reason": "Playwright reportó un error inesperado durante el envío.",
            "session_expired": session_expired,
        }


def _send_message_via_backend(
    sender: dict[str, Any], contact: dict[str, Any], event: dict[str, Any]
) -> dict[str, Any]:
    method = (sender.get("connection_method") or "").lower()
    message = event.get("message", "")
    if method != "playwright":
        sender["connection_method"] = "playwright"
        _log_structured(
            "whatsapp.sender.method.normalized",
            sender_id=sender.get("id"),
            previous_method=method or "missing",
            normalized_method="playwright",
        )
    return _send_with_playwright(sender, contact, message)


# ======================================================================
# ===== Message Reading ==================================================

def _wa_locator_count(locator: Any) -> int:
    try:
        return int(locator.count())
    except Exception:
        return 0


def _wa_open_unread_chat(page: Any) -> tuple[Any | None, str]:
    """
    Busca el primer chat con badge de no leído y lo abre.
    Devuelve (chat_row_locator, reason).
    """
    row_selector = _join_selectors(_WASelectors.CHAT_ROW_CANDIDATES)
    badge_selector = _join_selectors(_WASelectors.UNREAD_BADGE)

    try:
        rows = page.locator(row_selector)
        row_count = rows.count()
    except Exception as exc:  # noqa: BLE001
        return None, f"No se pudo inspeccionar la lista de chats: {exc}"

    for idx in range(min(row_count, 200)):
        row = rows.nth(idx)
        try:
            badge = row.locator(badge_selector).first
            if not badge.is_visible():
                continue
            row.click(timeout=5000)
            return row, ""
        except Exception:
            continue
    return None, "No hay chats con mensajes no leídos."


def _wa_get_last_incoming_message(page: Any) -> str:
    selector = _join_selectors(_WASelectors.BUBBLE_IN)
    bubbles = page.locator(selector)
    total = _wa_locator_count(bubbles)
    if total <= 0:
        return ""
    last = bubbles.nth(total - 1)
    return _extract_playwright_bubble_text(last)


def _wa_get_last_outgoing_message(page: Any) -> str:
    selector = _join_selectors(_WASelectors.BUBBLE_OUT)
    bubbles = page.locator(selector)
    total = _wa_locator_count(bubbles)
    if total <= 0:
        return ""
    last = bubbles.nth(total - 1)
    return _extract_playwright_bubble_text(last)


def _wa_get_last_message_snapshot(page: Any) -> dict[str, str]:
    """
    Retorna dirección del último mensaje visible: incoming/outgoing/unknown, junto al texto.
    """
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    try:
        nodes = page.locator(container_selector)
        total = _wa_locator_count(nodes)
    except Exception:
        total = 0
        nodes = None
    if total <= 0 or nodes is None:
        return {"direction": "unknown", "text": ""}

    bubble = nodes.nth(total - 1)
    text = _extract_playwright_bubble_text(bubble)
    direction = _wa_bubble_direction(bubble)
    return {"direction": direction, "text": text}


def _wa_get_active_chat_title(page: Any) -> str:
    for selector in _WASelectors.ACTIVE_CHAT_TITLE:
        try:
            loc = page.locator(selector).first
            if not loc.is_visible():
                continue
            value = (loc.get_attribute("title") or loc.inner_text() or "").strip()
            if value:
                return value
        except Exception:
            continue
    return ""


def _extract_sender_phone_from_chat_title(chat_title: str) -> str:
    digits = "".join(ch for ch in (chat_title or "") if ch.isdigit())
    if len(digits) < MIN_PHONE_DIGITS:
        return ""
    return digits


def _wa_build_conversation_history(page: Any, max_items: int = 10) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    container_selector = _WASelectors.MESSAGE_CONTAINER[0]
    try:
        nodes = page.locator(container_selector)
        total = _wa_locator_count(nodes)
    except Exception:
        total = 0
        nodes = None

    if total > 0 and nodes is not None:
        start = max(total - max_items * 3, 0)
        for i in range(start, total):
            bubble = nodes.nth(i)
            text = _extract_playwright_bubble_text(bubble)
            if not text:
                continue
            role = "user"
            try:
                direction = _wa_bubble_direction(bubble)
                if direction == "outgoing":
                    role = "assistant"
                elif direction == "incoming":
                    role = "user"
            except Exception:
                role = "user"
            history.append({"role": role, "content": text})

    return history[-max_items:]


def _wa_read_next_unread(
    sender: dict[str, Any],
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """
    Abre el próximo chat no leído y extrae el último mensaje entrante.
    """
    trace_id = str(uuid.uuid4())
    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")
            state = _wa_wait_for_login_state(page, timeout_seconds=max(5.0, timeout_seconds))
            if state == "qr":
                return {
                    "ok": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp no está vinculada (QR visible).",
                }
            if state == "timeout" and not _wa_is_ready(page):
                return {
                    "ok": False,
                    "code": "timeout",
                    "reason": "No se pudo acceder al panel de chats dentro del tiempo esperado.",
                }

            pane_selector = _join_selectors(_WASelectors.PANE_SIDE)
            try:
                page.wait_for_selector(pane_selector, timeout=20000)
            except Exception:
                return {
                    "ok": False,
                    "code": "chat_list_unavailable",
                    "reason": "No se encontró la lista de chats.",
                }

            row, reason = _wa_open_unread_chat(page)
            if row is None:
                return {"ok": False, "code": "no_unread", "reason": reason}

            input_box = _wa_find_visible_chat_input(page, timeout_ms=10000)
            if input_box is None:
                return {
                    "ok": False,
                    "code": "input_missing",
                    "reason": "No se encontró el cuadro de mensaje en el chat activo.",
                }

            chat_title = _wa_get_active_chat_title(page)
            sender_phone = _extract_sender_phone_from_chat_title(chat_title)
            snapshot = _wa_get_last_message_snapshot(page)
            if snapshot.get("direction") == "outgoing":
                return {
                    "ok": False,
                    "code": "self_message",
                    "reason": "El último mensaje visible es saliente; se omite auto-respuesta.",
                    "chat_title": chat_title,
                }
            last_in = _wa_get_last_incoming_message(page)
            last_out = _wa_get_last_outgoing_message(page)
            if not last_in:
                return {
                    "ok": False,
                    "code": "incoming_missing",
                    "reason": "No se detectó un mensaje entrante legible en el chat abierto.",
                }
            if last_out and _normalize_message(last_out) == _normalize_message(last_in):
                return {
                    "ok": False,
                    "code": "self_message",
                    "reason": "El último contenido coincide con un mensaje saliente; se omite auto-respuesta.",
                    "chat_title": chat_title,
                }

            history = _wa_build_conversation_history(page, max_items=12)
            result = {
                "ok": True,
                "chat_title": chat_title,
                "sender_phone": sender_phone,
                "incoming_text": last_in,
                "history": history,
                "meta": {"trace_id": trace_id},
            }
            _log_structured(
                "whatsapp.read.unread.ok",
                trace_id=trace_id,
                sender_id=sender.get("id"),
                chat_title=chat_title,
                chars=len(last_in),
            )
            return result
    except Exception as exc:  # noqa: BLE001
        _log_structured(
            "whatsapp.read.unread.error",
            trace_id=trace_id,
            sender_id=sender.get("id"),
            error=str(exc),
        )
        return {
            "ok": False,
            "code": "playwright_error",
            "reason": f"Error de Playwright al leer mensajes no leídos: {exc}",
        }


# ======================================================================
# ===== Auto Reply =======================================================

def _generate_gpt_reply(
    *,
    incoming_text: str,
    conversation_history: list[dict[str, Any]],
    system_prompt: str | None = None,
) -> dict[str, Any]:
    """
    Genera respuesta con OpenAI usando historial de conversación.
    Devuelve estructura estable para no romper flujos.
    """
    prompt = (system_prompt or "").strip() or (
        "Sos un asistente cordial. Respondé de forma breve, humana y útil en español neutro."
    )
    try:
        from openai import OpenAI

        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            return {"ok": False, "code": "openai_missing_key", "reason": "OPENAI_API_KEY no configurada."}
        client = OpenAI(api_key=api_key)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "code": "openai_unavailable", "reason": str(exc)}

    messages: list[dict[str, str]] = [{"role": "system", "content": prompt}]
    for msg in conversation_history[-12:]:
        role = (msg.get("role") or "").strip()
        content = (msg.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": incoming_text})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=180,
        )
        reply = (response.choices[0].message.content or "").strip()
        if not reply:
            return {"ok": False, "code": "empty_reply", "reason": "OpenAI devolvió una respuesta vacía."}
        return {
            "ok": True,
            "reply_text": reply,
            "model": "gpt-4o-mini",
            "tokens_used": getattr(getattr(response, "usage", None), "total_tokens", None),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "code": "openai_error", "reason": str(exc)}


def _sync_contact_state_after_receive(
    contact: dict[str, Any],
    *,
    incoming_text: str,
    received_at: str | None = None,
) -> None:
    ts = received_at or _now_iso()
    contact["last_message_received"] = incoming_text
    contact["last_message_received_at"] = ts
    contact["last_response_at"] = ts
    contact["status"] = "respondió"
    contact.setdefault("history", []).append(
        {
            "type": "incoming",
            "message": incoming_text,
            "received_at": ts,
        }
    )


def _sync_contact_state_after_send(
    contact: dict[str, Any],
    *,
    outgoing_text: str,
    sent_at: str | None = None,
) -> None:
    ts = sent_at or _now_iso()
    contact["last_message_sent"] = outgoing_text
    contact["last_message_sent_at"] = ts
    # Compatibilidad hacia atrás.
    contact["last_message_at"] = ts
    contact["status"] = "mensaje enviado"
    contact.setdefault("history", []).append(
        {
            "type": "send",
            "message": outgoing_text,
            "sent_at": ts,
        }
    )


def _sync_contact_state_followup(
    contact: dict[str, Any],
    *,
    followup_message: str,
    sent_at: str | None = None,
) -> None:
    ts = sent_at or _now_iso()
    contact["last_followup_at"] = ts
    current_stage = contact.get("followup_stage", 0)
    try:
        stage = int(current_stage) + 1
    except Exception:
        stage = 1
    contact["followup_stage"] = stage
    contact["status"] = "seguimiento enviado"
    contact["last_message_sent"] = followup_message
    contact["last_message_sent_at"] = ts
    contact.setdefault("history", []).append(
        {
            "type": "followup",
            "message": followup_message,
            "sent_at": ts,
            "stage": stage,
        }
    )


def _find_contact_by_chat_title(store: WhatsAppDataStore, chat_title: str) -> dict[str, Any] | None:
    title = (chat_title or "").strip().lower()
    if not title:
        return None
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            name = (contact.get("name") or "").strip().lower()
            if name and name == title:
                return contact
    return None


def _find_contact_by_sender_phone(store: WhatsAppDataStore, sender_phone: str) -> dict[str, Any] | None:
    digits = "".join(ch for ch in (sender_phone or "") if ch.isdigit())
    if not digits:
        return None
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            n_digits = "".join(ch for ch in (contact.get("number") or "") if ch.isdigit())
            if n_digits.endswith(digits) or digits.endswith(n_digits):
                return contact
    return None


def _find_contact_for_incoming_chat(
    store: WhatsAppDataStore,
    *,
    chat_title: str,
    sender_phone: str = "",
) -> dict[str, Any] | None:
    contact = _find_contact_by_sender_phone(store, sender_phone)
    if contact:
        return contact
    return _find_contact_by_chat_title(store, chat_title)


def _process_auto_reply_for_sender(
    store: WhatsAppDataStore,
    sender: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    read_result = _wa_read_next_unread(sender, timeout_seconds=25.0)
    if not read_result.get("ok"):
        return read_result

    incoming_text = (read_result.get("incoming_text") or "").strip()
    if not incoming_text:
        return {
            "ok": False,
            "code": "incoming_empty",
            "reason": "El mensaje entrante estaba vacío.",
        }

    chat_title = read_result.get("chat_title", "")
    sender_phone = (read_result.get("sender_phone") or "").strip()
    contact = _find_contact_for_incoming_chat(
        store,
        chat_title=chat_title,
        sender_phone=sender_phone,
    )
    if contact:
        _sync_contact_state_after_receive(contact, incoming_text=incoming_text)

    gpt_result = _generate_gpt_reply(
        incoming_text=incoming_text,
        conversation_history=list(read_result.get("history") or []),
        system_prompt=config.get("prompt", ""),
    )
    if not gpt_result.get("ok"):
        return {
            "ok": False,
            "code": gpt_result.get("code", "gpt_failed"),
            "reason": gpt_result.get("reason", "No se pudo generar respuesta con GPT."),
        }

    reply_text = (gpt_result.get("reply_text") or "").strip()
    if not reply_text:
        return {"ok": False, "code": "empty_reply", "reason": "GPT devolvió texto vacío."}

    # Enviamos sobre el chat abierto para evitar depender del número en esta etapa.
    send_result = _wa_reply_on_current_chat(
        sender,
        reply_text,
        expected_chat_title=chat_title or None,
    )
    if not send_result.get("success"):
        return {
            "ok": False,
            "code": send_result.get("code", "send_failed"),
            "reason": send_result.get("reason", "No se pudo enviar la respuesta automática."),
        }

    sent_at = send_result.get("delivered_at") or _now_iso()
    if contact:
        _sync_contact_state_after_send(contact, outgoing_text=reply_text, sent_at=sent_at)
        contact["followup_stage"] = 0
    return {
        "ok": True,
        "chat_title": chat_title,
        "incoming_text": incoming_text,
        "reply_text": reply_text,
        "delivered_at": sent_at,
    }


def _wa_open_chat_by_title(page: Any, title: str) -> bool:
    expected = (title or "").strip().lower()
    if not expected:
        return False
    expected_norm = " ".join(expected.split())
    row_selector = _join_selectors(_WASelectors.CHAT_ROW_CANDIDATES)
    rows = page.locator(row_selector)
    for idx in range(min(_wa_locator_count(rows), 300)):
        row = rows.nth(idx)
        text_bits: list[str] = []
        for sel in _WASelectors.CHAT_TITLE_IN_ROW:
            try:
                nodes = row.locator(sel)
                for n in range(min(_wa_locator_count(nodes), 4)):
                    value = (
                        nodes.nth(n).get_attribute("title")
                        or nodes.nth(n).inner_text()
                        or ""
                    ).strip()
                    if value:
                        text_bits.append(value.lower())
            except Exception:
                continue
        normalized_bits = [" ".join(bit.split()) for bit in text_bits]
        if expected_norm in normalized_bits:
            try:
                row.click(timeout=5000)
                return True
            except Exception:
                continue
    return False


def _wa_reply_on_current_chat(
    sender: dict[str, Any],
    message: str,
    *,
    expected_chat_title: str | None = None,
) -> dict[str, Any]:
    """Envía un mensaje en el chat actualmente abierto (flujo de unread -> autoreply)."""
    try:
        with _playwright_persistent_page(
            sender=sender,
            headless=_wa_automation_headless(sender),
        ) as (page, _context):
            page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded")
            state = _wa_wait_for_login_state(page, timeout_seconds=12.0)
            if state == "qr":
                return {
                    "success": False,
                    "code": "session_expired",
                    "reason": "La sesión de WhatsApp no está vinculada (QR visible).",
                    "session_expired": True,
                }

            if expected_chat_title:
                opened = _wa_open_chat_by_title(page, expected_chat_title)
                if not opened:
                    return {
                        "success": False,
                        "code": "chat_switch_failed",
                        "reason": "No se encontró el chat objetivo para responder.",
                        "session_expired": False,
                    }
            else:
                row, reason = _wa_open_unread_chat(page)
                if row is None:
                    return {
                        "success": False,
                        "code": "no_unread",
                        "reason": reason,
                        "session_expired": False,
                    }

            input_box = _wa_find_visible_chat_input(page, timeout_ms=10000)
            if input_box is None:
                return {
                    "success": False,
                    "code": "input_missing",
                    "reason": "No se encontró el cuadro de mensaje en el chat activo.",
                    "session_expired": False,
                }
            try:
                input_box.click(timeout=5000)
            except Exception:
                try:
                    input_box.focus()
                except Exception:
                    return {
                        "success": False,
                        "code": "input_missing",
                        "reason": "No se pudo enfocar el cuadro de mensaje en el chat activo.",
                        "session_expired": False,
                    }

            for hotkey in ("Control+A", "Meta+A"):
                try:
                    page.keyboard.press(hotkey)
                    page.keyboard.press("Delete")
                    break
                except Exception:
                    continue

            try:
                before_count = _wa_locator_count(page.locator(_WASelectors.MESSAGE_CONTAINER[0]))
            except Exception:
                before_count = 0

            for idx, line in enumerate((message or "").splitlines() or [""]):
                if idx:
                    page.keyboard.press("Shift+Enter")
                page.keyboard.type(line or " ")
            page.keyboard.press("Enter")

            matched_bubble, confirmation = _wa_wait_outgoing_bubble(
                page,
                expected_text=message or "",
                before_count=before_count,
                timeout_ms=20000,
            )
            if matched_bubble is None:
                return {
                    "success": False,
                    "code": "send_unconfirmed",
                    "reason": "WhatsApp no confirmó la auto-respuesta en la conversación.",
                    "session_expired": False,
                }

            confirmation = confirmation or "enviado"
            if confirmation == "enviado":
                upgrade_deadline = time.time() + 4.0
                while time.time() < upgrade_deadline:
                    upgraded = _wa_confirmation_from_bubble(matched_bubble)
                    if upgraded in {"entregado", "leido"}:
                        confirmation = upgraded
                        break
                    page.wait_for_timeout(300)

            delivered_at = _now_iso()
            return {
                "success": True,
                "confirmation": confirmation,
                "note": "Respuesta automática enviada mediante Playwright.",
                "delivered_at": delivered_at,
                "session_expired": False,
            }
    except Exception as exc:  # noqa: BLE001
        _log_structured("whatsapp.autoreply.send.error", sender_id=sender.get("id"), error=str(exc))
        return {
            "success": False,
            "code": "playwright_error",
            "reason": f"Error al enviar auto-respuesta: {exc}",
            "session_expired": False,
        }


# ======================================================================
# ===== Follow-up Scheduler =============================================

def _should_followup_contact(
    contact: dict[str, Any],
    *,
    threshold: datetime,
) -> bool:
    # Base legacy + campos nuevos con comparación temporal real.
    last_message_raw = contact.get("last_message_at") or contact.get("last_message_sent_at")
    if not last_message_raw:
        return False
    sent_dt = _parse_iso(str(last_message_raw))
    if not sent_dt:
        return False

    responded_raw = contact.get("last_response_at") or contact.get("last_message_received_at")
    responded_dt = _parse_iso(str(responded_raw)) if responded_raw else None
    if responded_dt and responded_dt >= sent_dt:
        return False
    if contact.get("status") in {"pagó", "acceso enviado"}:
        return False
    return sent_dt <= threshold


def _followup_in_failure_backoff(contact: dict[str, Any]) -> bool:
    history = contact.get("history", [])
    if not isinstance(history, list):
        return False
    for entry in reversed(history[-30:]):
        if not isinstance(entry, dict):
            continue
        if (entry.get("type") or "").lower() != "followup_failed":
            continue
        attempted_at = _parse_iso(entry.get("attempted_at"))
        if not attempted_at:
            return False
        elapsed = (_now() - attempted_at).total_seconds()
        return elapsed < (_FOLLOWUP_FAILURE_RETRY_MINUTES * 60)
    return False


def _select_connected_sender(
    store: WhatsAppDataStore,
    *,
    preferred_id: str = "",
) -> dict[str, Any] | None:
    def _is_supported(sender: dict[str, Any]) -> bool:
        method = (sender.get("connection_method") or "").lower()
        return method == "playwright"

    preferred = (preferred_id or "").strip()
    if preferred:
        item = store.find_number(preferred)
        if item and item.get("connected") and _is_supported(item):
            return item
    for sender in store.iter_numbers():
        if sender.get("connected") and _is_supported(sender):
            return sender
    return None


def _history_to_conversation(history: list[dict[str, Any]], max_items: int = 12) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for entry in history[-40:]:
        typ = (entry.get("type") or "").lower()
        text = (
            entry.get("message")
            or entry.get("text")
            or entry.get("details")
            or ""
        )
        text = str(text).strip()
        if not text:
            continue
        if typ in {"incoming", "received", "reply"}:
            role = "user"
        elif typ in {"send", "followup", "payment", "payment_confirmed"}:
            role = "assistant"
        else:
            continue
        messages.append({"role": role, "content": text})
    return messages[-max_items:]


def _run_ai_automations(store: WhatsAppDataStore) -> bool:
    configs = store.state.setdefault("ai_automations", {})
    changed = False
    for number_id, cfg in list(configs.items()):
        config = store._ensure_ai_config(cfg)
        if config != cfg:
            configs[number_id] = config
            changed = True
        if not config.get("active"):
            continue

        sender = store.find_number(number_id)
        if not sender:
            continue
        if not sender.get("connected"):
            continue
        if (sender.get("connection_method") or "").lower() != "playwright":
            # Auto-reply unread hoy se soporta con Playwright.
            continue

        now_dt = _now()
        interval = max(
            _AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS,
            int(config.get("polling_interval_seconds") or 60),
        )
        throttle = max(10, int(config.get("scan_throttle_seconds") or _AUTOREPLY_DEFAULT_THROTTLE_SECONDS))
        jitter = max(0, int(config.get("scan_jitter_seconds") or _AUTOREPLY_DEFAULT_JITTER_SECONDS))
        next_scan_at = _parse_iso(config.get("next_scan_at"))
        if next_scan_at and next_scan_at > now_dt:
            continue
        last_scan = _parse_iso(config.get("last_scan_at"))
        if last_scan and (now_dt - last_scan).total_seconds() < throttle:
            continue

        try:
            result = _process_auto_reply_for_sender(store, sender, config)
            scan_ts = _now_iso()
            code = result.get("code")
            ok_result = bool(result.get("ok"))

            next_wait_seconds = float(max(interval, throttle))
            if ok_result:
                delay_cfg = config.get("delay") or {"min": 5.0, "max": 15.0}
                try:
                    min_delay = float(delay_cfg.get("min", 5.0))
                    max_delay = float(delay_cfg.get("max", min_delay))
                    if max_delay < min_delay:
                        max_delay = min_delay
                    # En vez de dormir el runner, trasladamos el delay humano al próximo scan.
                    next_wait_seconds += random.uniform(min_delay, max_delay)
                except Exception:
                    next_wait_seconds += random.uniform(0, float(max(jitter, 1)))
            else:
                if code == "no_unread":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_NO_UNREAD_BACKOFF_RANGE)
                elif code == "self_message":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_SELF_MESSAGE_BACKOFF_RANGE)
                elif code == "session_expired":
                    next_wait_seconds = random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE)
                else:
                    next_wait_seconds = random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE)

            next_wait_seconds += random.uniform(0, float(jitter))
            next_scan_dt = _now() + timedelta(seconds=max(next_wait_seconds, float(throttle)))

            _log_structured(
                "whatsapp.autoreply.scan",
                sender_id=sender.get("id"),
                ok=ok_result,
                code=code,
                reason=result.get("reason"),
                next_scan_at=next_scan_dt.isoformat() + "Z",
                throttle_seconds=throttle,
            )
            config["last_scan_at"] = scan_ts
            config["next_scan_at"] = next_scan_dt.isoformat() + "Z"
            config["last_result"] = {
                "ok": ok_result,
                "code": code,
                "reason": result.get("reason"),
                "at": scan_ts,
            }
            changed = True

            if not ok_result and code == "session_expired":
                _mark_sender_session_expired(
                    sender,
                    note="Auto-reply detectó sesión cerrada. Repetí la vinculación escaneando el QR.",
                )
                changed = True
        except Exception as exc:  # noqa: BLE001
            _log_structured(
                "whatsapp.autoreply.error",
                sender_id=sender.get("id"),
                error=str(exc),
            )
            scan_ts = _now_iso()
            next_scan_dt = _now() + timedelta(seconds=random.uniform(*_AUTOREPLY_ERROR_BACKOFF_RANGE))
            config["last_scan_at"] = scan_ts
            config["next_scan_at"] = next_scan_dt.isoformat() + "Z"
            config["last_result"] = {
                "ok": False,
                "code": "runtime_error",
                "reason": str(exc),
                "at": scan_ts,
            }
            changed = True
    return changed


def _generate_followup_message(
    contact: dict[str, Any],
    config: dict[str, Any],
) -> str:
    mode = (config.get("auto_mode") or "manual").strip().lower()
    if mode.startswith("i"):
        incoming_text = (contact.get("last_message_received") or "").strip() or (
            "No hubo respuesta al mensaje anterior."
        )
        conv = _history_to_conversation(contact.get("history", []), max_items=12)
        ai = _generate_gpt_reply(
            incoming_text=incoming_text,
            conversation_history=conv,
            system_prompt=config.get("ai_prompt", ""),
        )
        if ai.get("ok"):
            return (ai.get("reply_text") or "").strip()
    return _render_message(config.get("manual_message", ""), contact).strip()


def _run_followup_scheduler(store: WhatsAppDataStore) -> bool:
    config = store.state.setdefault("followup", store._ensure_followup_config({}))
    if not config.get("auto_enabled"):
        return False
    last_auto = _parse_iso(config.get("last_auto_run_at"))
    if last_auto and (_now() - last_auto).total_seconds() < _FOLLOWUP_MIN_CYCLE_SECONDS:
        return False

    sender = _select_connected_sender(
        store,
        preferred_id=config.get("active_number_id", ""),
    )
    if not sender:
        _log_structured(
            "whatsapp.followup.auto.skipped",
            reason="no_supported_sender",
        )
        return False

    wait_minutes = max(10, int(config.get("default_wait_minutes") or 120))
    threshold = _now() - timedelta(minutes=wait_minutes)
    max_stage = max(1, int(config.get("max_stage") or 2))
    changed = False
    sent_count = 0
    max_per_cycle = 3

    for alias, data in store.iter_lists():
        if sent_count >= max_per_cycle:
            break
        for contact in data.get("contacts", []):
            if sent_count >= max_per_cycle:
                break
            stage = contact.get("followup_stage", 0)
            try:
                stage_int = int(stage)
            except Exception:
                stage_int = 0
            if stage_int >= max_stage:
                continue
            if _followup_in_failure_backoff(contact):
                continue
            if not _should_followup_contact(contact, threshold=threshold):
                continue

            message = _generate_followup_message(contact, config)
            if not message:
                continue

            send_result = _send_message_via_backend(
                sender,
                contact,
                {"message": message, "notes": "Seguimiento automático"},
            )
            if not send_result.get("success"):
                if send_result.get("session_expired"):
                    _mark_sender_session_expired(
                        sender,
                        note="El seguimiento automático detectó sesión cerrada. Repetí la vinculación escaneando el QR.",
                    )
                contact.setdefault("history", []).append(
                    {
                        "type": "followup_failed",
                        "attempted_at": _now_iso(),
                        "error": send_result.get("reason") or "No se pudo enviar el seguimiento automático.",
                        "code": send_result.get("code"),
                    }
                )
                changed = True
                continue

            delivered_at = send_result.get("delivered_at") or _now_iso()
            _sync_contact_state_followup(contact, followup_message=message, sent_at=delivered_at)
            contact["last_sender_id"] = sender.get("id")
            sent_count += 1
            changed = True

            config.setdefault("history", []).append(
                {
                    "executed_at": delivered_at,
                    "list_alias": alias,
                    "contact_number": contact.get("number"),
                    "mode": "ia" if (config.get("auto_mode") or "").startswith("i") else "manual",
                    "stage": contact.get("followup_stage"),
                    "status": "sent",
                }
            )

    config["last_auto_run_at"] = _now_iso()
    if sent_count:
        _log_structured(
            "whatsapp.followup.auto.sent",
            sender_id=sender.get("id"),
            count=sent_count,
            wait_minutes=wait_minutes,
        )
    return changed


def _mark_sender_session_expired(sender: dict[str, Any], *, note: str) -> None:
    sender["connected"] = False
    sender["connection_state"] = "fallido"
    sender["last_connected_at"] = None
    sender.setdefault("session_notes", []).append(
        {
            "created_at": _now_iso(),
            "text": note,
        }
    )


def _deliver_event(store: WhatsAppDataStore, run: dict[str, Any], event: dict[str, Any]) -> bool:
    if (event.get("status") or "") != "pendiente":
        return False
    delivered_at = _now_iso()
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "omitido"
        event["delivered_at"] = delivered_at
        event["confirmation"] = "no_enviado"
        event["error_code"] = "list_missing"
        event["notes"] = "La lista vinculada fue eliminada."
        _append_run_log(
            run,
            f"La lista '{run.get('list_alias')}' ya no existe. Se omitió el envío a {event.get('contact')}.",
        )
    else:
        contact = _locate_contact(contact_list, event.get("contact"))
        if not contact:
            event["status"] = "omitido"
            event["delivered_at"] = delivered_at
            event["confirmation"] = "no_enviado"
            event["error_code"] = "contact_missing"
            event["notes"] = "El contacto ya no está disponible en la lista."
            _append_run_log(
                run,
                f"No se encontró el contacto {event.get('contact')} dentro de la lista.",
            )
        else:
            validation = _update_contact_validation(contact)
            event["validation_status"] = validation.get("status")
            sender = store.find_number(run.get("number_id", ""))
            failure_reason: str | None = None
            failure_code: str | None = None
            delivery_result: dict[str, Any] | None = None
            if validation["status"] == "invalid":
                failure_reason = validation.get("message") or "Número inválido."
                failure_code = "invalid_number"
            elif not sender:
                failure_reason = "El número de envío ya no está registrado."
                failure_code = "sender_missing"
            elif not sender.get("connected"):
                failure_reason = "La sesión de WhatsApp seleccionada no está activa."
                failure_code = "session_inactiva"
            elif (sender.get("connection_state") or "").lower() == "fallido":
                failure_reason = "La vinculación del número presentó un error reciente."
                failure_code = "session_error"
            else:
                delivery_result = _send_message_via_backend(sender, contact, event)
                if not delivery_result.get("success"):
                    failure_reason = (
                        delivery_result.get("reason")
                        or "WhatsApp no confirmó el envío del mensaje."
                    )
                    failure_code = delivery_result.get("code") or "send_failed"
                    if delivery_result.get("session_expired"):
                        _mark_sender_session_expired(
                            sender,
                            note="La sesión caducó durante un envío automático. Repetí la vinculación escaneando el QR.",
                        )
                        _append_run_log(
                            run,
                            "La sesión de WhatsApp se cerró durante el envío. Es necesario volver a vincular el número.",
                        )
                else:
                    delivered_at = delivery_result.get("delivered_at") or delivered_at

            if failure_reason:
                contact["status"] = "observado"
                contact.setdefault("history", []).append(
                    {
                        "type": "send_failed",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "attempted_at": delivered_at,
                        "error": failure_reason,
                    }
                )
                event["status"] = "fallido"
                event["notes"] = failure_reason
                event["error_code"] = failure_code
                event["confirmation"] = "no_enviado"
                event["delivered_at"] = delivered_at
                _append_delivery_log(
                    contact,
                    run,
                    status="fallido",
                    reason=failure_reason,
                    confirmation="no_enviado",
                )
                _append_run_log(
                    run,
                    f"Fallo el envío a {contact.get('name') or contact.get('number')}: {failure_reason}",
                )
            else:
                confirmation_value = "entregado"
                success_note = event.get("notes") or "Mensaje enviado correctamente."
                if delivery_result:
                    confirmation_value = (
                        delivery_result.get("confirmation") or confirmation_value
                    )
                    success_note = delivery_result.get("note") or success_note
                contact["status"] = "mensaje enviado"
                contact["last_message_at"] = event.get("scheduled_at") or delivered_at
                contact["last_message_sent"] = event.get("message", "")
                contact["last_message_sent_at"] = delivered_at
                contact["last_sender_id"] = run.get("number_id")
                contact.setdefault("history", []).append(
                    {
                        "type": "send",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "sent_at": delivered_at,
                        "delay": run.get("delay"),
                        "confirmation": confirmation_value,
                    }
                )
                event["status"] = "enviado"
                event["delivered_at"] = delivered_at
                event["confirmation"] = confirmation_value
                event["error_code"] = None
                event["notes"] = success_note
                _append_delivery_log(
                    contact,
                    run,
                    status="enviado",
                    reason=success_note,
                    confirmation=confirmation_value,
                )
                _append_run_log(
                    run,
                    "WhatsApp confirmó el mensaje para {} con estado {}.".format(
                        contact.get("name") or contact.get("number"),
                        confirmation_value,
                    ),
                )
    run["last_activity_at"] = delivered_at
    _refresh_run_counters(run)
    if (event.get("status") or "") in {"enviado", "fallido"}:
        run["status"] = "en progreso"
    return True


def _locate_contact(contact_list: dict[str, Any], number: str | None) -> dict[str, Any] | None:
    if not number:
        return None
    for contact in contact_list.get("contacts", []):
        if contact.get("number") == number:
            return contact
    return None


def _mark_contact_scheduled(
    contact: dict[str, Any],
    run_id: str,
    message: str,
    scheduled_at: str,
    min_delay: float,
    max_delay: float,
) -> None:
    preview = textwrap.shorten(message, width=80, placeholder="…") if message else ""
    contact.setdefault("history", []).append(
        {
            "type": "scheduled",
            "run_id": run_id,
            "scheduled_at": scheduled_at,
            "message": preview,
            "delay": {"min": min_delay, "max": max_delay},
        }
    )
    current_status = (contact.get("status") or "").lower()
    if not current_status or any(hint in current_status for hint in ("sin", "espera", "program")):
        contact["status"] = "mensaje programado"


def _reset_contact_for_cancellation(
    store: WhatsAppDataStore,
    run: dict[str, Any],
    event: dict[str, Any],
) -> None:
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "cancelado"
        event["delivered_at"] = _now_iso()
        return
    contact = _locate_contact(contact_list, event.get("contact"))
    event["status"] = "cancelado"
    event["delivered_at"] = _now_iso()
    if not contact:
        return
    history = contact.setdefault("history", [])
    history.append(
        {
            "type": "cancelled",
            "run_id": run.get("id"),
            "scheduled_at": event.get("scheduled_at"),
            "cancelled_at": event.get("delivered_at"),
        }
    )
    current_status = (contact.get("status") or "").lower()
    if "program" in current_status and not contact.get("last_message_at"):
        contact["status"] = "sin mensaje"

def _choose_number(store: WhatsAppDataStore) -> dict[str, Any] | None:
    options = list(store.iter_numbers())
    if not options:
        return None
    print(_line())
    _subtitle("Seleccioná el número de envío")
    for idx, item in enumerate(options, 1):
        if item.get("connected"):
            status = "🟢 verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 error"
        else:
            status = "⚪ pendiente"
        print(f"{idx}) {item.get('alias')} ({item.get('phone')}) - {status}")
    idx = ask_int("Número elegido: ", min_value=1)
    if idx > len(options):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return options[idx - 1]


def _choose_contact_list(store: WhatsAppDataStore) -> dict[str, Any] | None:
    lists = list(store.iter_lists())
    if not lists:
        return None
    print(_line())
    _subtitle("Seleccioná la lista de contactos")
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Lista elegida: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    alias, data = lists[idx - 1]
    data["alias"] = alias
    return data


def _ask_delay_range() -> tuple[float, float]:
    while True:
        try:
            min_delay = float(ask("Delay mínimo (segundos): ").strip())
            max_delay = float(ask("Delay máximo (segundos): ").strip())
        except ValueError:
            _info("Ingresá números válidos para los delays.", color=Fore.YELLOW)
            continue
        if min_delay <= 0 or max_delay <= 0:
            _info("Los delays deben ser mayores a cero.", color=Fore.YELLOW)
            continue
        if max_delay < min_delay:
            _info("El máximo debe ser mayor o igual al mínimo.", color=Fore.YELLOW)
            continue
        return min_delay, max_delay


def _render_message(template: str, contact: dict[str, Any]) -> str:
    safe_contact = {"nombre": contact.get("name", ""), "numero": contact.get("number", "")}
    try:
        return template.format(**{"nombre": safe_contact["nombre"], "numero": safe_contact["numero"]})
    except KeyError:
        return template


# ----------------------------------------------------------------------
# 4) Automatizar respuestas con IA -------------------------------------

def _configure_ai_responses(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    configs = store.state.setdefault("ai_automations", {})
    current = configs.get(number["id"], store._ensure_ai_config({}))
    while True:
        banner()
        title("Automatización de respuestas con IA")
        print(_line())
        _info(f"Número seleccionado: {number.get('alias')} ({number.get('phone')})", bold=True)
        status = "🟢 activo" if current.get("active") else "⚪ en espera"
        print(f"Estado actual: {status}")
        print(f"Delay configurado: {_format_delay(current.get('delay', {'min': 5.0, 'max': 15.0}))}")
        print(f"Escaneo de no leídos cada: {current.get('polling_interval_seconds', 60)}s")
        print(f"Throttle mínimo entre scans: {current.get('scan_throttle_seconds', _AUTOREPLY_DEFAULT_THROTTLE_SECONDS)}s")
        print(f"Jitter de escaneo: ±{current.get('scan_jitter_seconds', _AUTOREPLY_DEFAULT_JITTER_SECONDS)}s")
        prompt_preview = textwrap.shorten(current.get("prompt", ""), width=90, placeholder="…")
        print(f"Prompt base: {prompt_preview or '(sin definir)'}")
        print(f"Envío de audios: {'sí' if current.get('send_audio') else 'no'}")
        print(_line())
        print("1) Activar o actualizar configuración")
        print("2) Pausar automatización para este número")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            prompt = ask_multiline("Prompt guía para la IA: ").strip() or current.get("prompt", "")
            min_delay, max_delay = _ask_delay_range()
            polling_interval = ask_int(
                "Intervalo de escaneo de chats no leídos (segundos): ",
                min_value=_AUTOREPLY_MIN_SCAN_INTERVAL_SECONDS,
                default=int(current.get("polling_interval_seconds", 60) or 60),
            )
            throttle_seconds = ask_int(
                "Throttle mínimo entre scans (segundos): ",
                min_value=10,
                default=int(current.get("scan_throttle_seconds", _AUTOREPLY_DEFAULT_THROTTLE_SECONDS) or _AUTOREPLY_DEFAULT_THROTTLE_SECONDS),
            )
            jitter_seconds = ask_int(
                "Jitter aleatorio adicional de scan (segundos): ",
                min_value=0,
                default=int(current.get("scan_jitter_seconds", _AUTOREPLY_DEFAULT_JITTER_SECONDS) or _AUTOREPLY_DEFAULT_JITTER_SECONDS),
            )
            audio = ask("¿Enviar audios cuando sea posible? (s/n): ").strip().lower().startswith("s")
            current.update(
                {
                    "active": True,
                    "prompt": prompt,
                    "delay": {"min": min_delay, "max": max_delay},
                    "polling_interval_seconds": polling_interval,
                    "scan_throttle_seconds": throttle_seconds,
                    "scan_jitter_seconds": jitter_seconds,
                    "send_audio": audio,
                    "last_updated_at": _now_iso(),
                }
            )
            configs[number["id"]] = current
            store.save()
            ok("Automatización actualizada. Se responderá siguiendo un tono humano y cordial.")
            press_enter()
        elif op == "2":
            current["active"] = False
            current["last_updated_at"] = _now_iso()
            configs[number["id"]] = current
            store.save()
            ok("Automatización pausada para este número.")
            press_enter()
        elif op == "3":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 5) Captura desde Instagram -------------------------------------------

def _instagram_capture(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("instagram", store._ensure_instagram_config({}))
    while True:
        banner()
        title("Captura de números desde Instagram")
        print(_line())
        print(f"Estado: {'🟢 activo' if config.get('active') else '⚪ en pausa'}")
        print(f"Mensaje inicial: {textwrap.shorten(config.get('message', ''), width=80, placeholder='…')}")
        print(f"Delay configurado: {_format_delay(config.get('delay', {'min': 5.0, 'max': 12.0}))}")
        print(f"Total de capturas: {len(config.get('captures', []))}")
        print(_line())
        print("1) Configurar mensaje y delays")
        print("2) Registrar número capturado manualmente")
        print("3) Ver seguimiento de conversiones")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            active = ask("¿Activar la escucha automática? (s/n): ").strip().lower().startswith("s")
            message = ask_multiline("Mensaje inicial automático: ").strip() or config.get("message", "")
            min_delay, max_delay = _ask_delay_range()
            config.update(
                {
                    "active": active,
                    "message": message,
                    "delay": {"min": min_delay, "max": max_delay},
                    "last_reviewed_at": _now_iso(),
                }
            )
            store.save()
            ok("Integración actualizada. Los leads de Instagram se contactarán de forma natural.")
            press_enter()
        elif op == "2":
            _register_instagram_capture(store, config)
        elif op == "3":
            _show_instagram_tracking(config)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _register_instagram_capture(store: WhatsAppDataStore, config: dict[str, Any]) -> None:
    name = ask("Nombre de la persona: ").strip()
    number = ask("Número detectado: ").strip()
    if not number:
        _info("Se requiere un número válido.", color=Fore.YELLOW)
        press_enter()
        return
    source = ask("Origen o nota de la conversación (opcional): ").strip() or "Instagram"
    capture = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "source": source,
        "captured_at": _now_iso(),
        "message_sent": False,
        "message_sent_at": None,
        "notes": "",
    }
    delay = config.get("delay", {"min": 5.0, "max": 12.0})
    message = config.get("message", "")
    if message:
        capture["message_sent"] = True
        capture["message_sent_at"] = _now_iso()
        capture["notes"] = (
            f"Mensaje inicial programado con delays humanos {_format_delay(delay)}."
        )
    config.setdefault("captures", []).append(capture)
    _auto_add_to_master_list(store, capture)
    store.save()
    ok("Lead capturado y mensaje inicial configurado correctamente.")
    press_enter()


def _auto_add_to_master_list(store: WhatsAppDataStore, capture: dict[str, Any]) -> None:
    lists = store.state.setdefault("contact_lists", {})
    alias = "instagram_auto"
    contact = {
        "name": capture.get("name", capture.get("number", "")),
        "number": capture.get("number", ""),
        "status": "mensaje enviado" if capture.get("message_sent") else "sin mensaje",
        "last_message_at": capture.get("message_sent_at"),
        "last_response_at": None,
        "last_message_sent": capture.get("notes", "") if capture.get("message_sent") else "",
        "last_message_sent_at": capture.get("message_sent_at"),
        "last_message_received": "",
        "last_message_received_at": None,
        "last_sender_id": None,
        "followup_stage": 0,
        "last_followup_at": None,
        "last_payment_at": None,
        "access_sent_at": None,
        "notes": capture.get("source", "Instagram"),
        "history": [
            {
                "type": "captured",
                "source": capture.get("source", "Instagram"),
                "timestamp": capture.get("captured_at"),
            }
        ],
    }
    validation = _validate_phone_number(contact.get("number", ""))
    contact["number"] = validation.get("normalized") or contact.get("number", "")
    contact.setdefault("history", []).append(
        {
            "type": "validation",
            "status": validation["status"],
            "checked_at": validation["checked_at"],
            "message": validation["message"],
        }
    )
    contact["validation"] = validation
    contact["delivery_log"] = []
    if capture.get("message_sent"):
        contact["history"].append(
            {
                "type": "send",
                "message": capture.get("notes", ""),
                "timestamp": capture.get("message_sent_at"),
            }
        )
    if alias not in lists:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": [contact],
            "notes": "Leads generados automáticamente desde Instagram",
        }
    else:
        lists[alias]["contacts"].append(contact)


def _show_instagram_tracking(config: dict[str, Any]) -> None:
    banner()
    title("Seguimiento de conversiones desde Instagram")
    print(_line())
    captures = config.get("captures", [])
    if not captures:
        _info("Aún no hay capturas registradas.")
        press_enter()
        return
    for item in captures:
        status = "mensaje enviado" if item.get("message_sent") else "pendiente"
        print(
            f"• {item.get('name')} ({item.get('number')}) - {status} | "
            f"Detectado: {item.get('captured_at')} | Origen: {item.get('source')}"
        )
    press_enter()


# ----------------------------------------------------------------------
# 6) Seguimiento a no respondidos --------------------------------------

def _followup_manager(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("followup", store._ensure_followup_config({}))
    banner()
    title("Seguimiento automático de contactos sin respuesta")
    print(_line())
    wait_minutes = ask_int(
        "¿Cuántos minutos esperar antes de etiquetar como no respondido?: ",
        min_value=10,
        default=config.get("default_wait_minutes", 120),
    )
    config["default_wait_minutes"] = wait_minutes
    auto_enabled = ask(
        "¿Activar el seguimiento automático en segundo plano? (s/N): "
    ).strip().lower().startswith("s")
    config["auto_enabled"] = auto_enabled
    if auto_enabled:
        number = _choose_number(store)
        if number:
            config["active_number_id"] = number.get("id")
            _info(
                f"Número para seguimiento automático: {number.get('alias')} ({number.get('phone')})"
            )
        max_stage = ask_int(
            "Máximo de seguimientos automáticos por contacto: ",
            min_value=1,
            default=int(config.get("max_stage", 2) or 2),
        )
        config["max_stage"] = max_stage
    threshold = _now() - timedelta(minutes=wait_minutes)
    candidates = _find_followup_candidates(store, threshold)
    if not candidates:
        _info("No hay contactos pendientes de seguimiento en este momento.")
        store.save()
        press_enter()
        return
    _info(f"Se encontraron {len(candidates)} contactos sin respuesta.", bold=True)
    mode = ask(
        "¿Enviar mensaje personalizado (p) o generar con IA (i)? [p/i]: "
    ).strip().lower()
    config["auto_mode"] = "ia" if mode.startswith("i") else "manual"
    if mode.startswith("i"):
        prompt = ask_multiline("Prompt base para el seguimiento (opcional): ").strip() or config.get(
            "ai_prompt", ""
        )
        config["ai_prompt"] = prompt
        message_base = (
            "Mensaje generado automáticamente siguiendo un tono humano cercano y cordial."
        )
    else:
        message_base = ask_multiline("Mensaje de seguimiento: ").strip() or config.get(
            "manual_message", ""
        )
        config["manual_message"] = message_base
    min_delay, max_delay = _ask_delay_range()
    for entry in candidates:
        contact = entry["contact"]
        if mode.startswith("i"):
            personalized = _generate_followup_message(contact, config) or _render_message(
                config.get("manual_message", ""), contact
            )
        else:
            personalized = _render_message(message_base, contact)
        _sync_contact_state_followup(
            contact,
            followup_message=personalized,
            sent_at=_now_iso(),
        )
        # Compatibilidad con tracking previo
        contact.setdefault("history", []).append(
            {
                "type": "followup_schedule",
                "message": personalized,
                "delay": {"min": min_delay, "max": max_delay},
                "sent_at": _now_iso(),
            }
        )
    config.setdefault("history", []).append(
        {
            "executed_at": _now_iso(),
            "count": len(candidates),
            "delay": {"min": min_delay, "max": max_delay},
            "mode": "ia" if mode.startswith("i") else "manual",
        }
    )
    store.save()
    ok("Seguimiento configurado y mensajes programados con comportamiento humano natural.")
    press_enter()


def _find_followup_candidates(store: WhatsAppDataStore, threshold: datetime) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for alias, data in store.iter_lists():
        for contact in data.get("contacts", []):
            if _should_followup_contact(contact, threshold=threshold):
                results.append({"list": alias, "contact": contact})
    return results


# ----------------------------------------------------------------------
# 7) Gestión de pagos ---------------------------------------------------

def _payments_menu(store: WhatsAppDataStore) -> None:
    payments = store.state.setdefault("payments", store._ensure_payments_config({}))
    while True:
        banner()
        title("Gestión de pagos y entrega de accesos")
        print(_line())
        print(f"Administrador notificaciones: {payments.get('admin_number') or '(sin definir)'}")
        print(f"Pagos pendientes: {len(payments.get('pending', []))}")
        print(f"Pagos completados: {len(payments.get('history', []))}")
        print(_line())
        print("1) Procesar nueva captura de pago")
        print("2) Revisar pendientes y enviar accesos")
        print("3) Configurar mensajes y datos del administrador")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _process_payment_capture(store, payments)
        elif op == "2":
            _review_pending_payments(store, payments)
        elif op == "3":
            _configure_payment_settings(store, payments)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _process_payment_capture(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    name = ask("Nombre del contacto: ").strip()
    number = ask("Número de WhatsApp: ").strip()
    evidence = ask("Ruta de la captura o palabras clave detectadas: ").strip()
    detected_keywords = _detect_keywords(evidence)
    status = "validado" if _is_payment_valid(detected_keywords) else "pendiente"
    entry = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "evidence": evidence,
        "keywords": detected_keywords,
        "status": status,
        "created_at": _now_iso(),
        "validated_at": _now_iso() if status == "validado" else None,
        "welcome_sent_at": None,
        "alert_sent_at": None,
        "notes": "",
    }
    payments.setdefault("pending", []).append(entry)
    if status != "validado":
        entry["alert_sent_at"] = _now_iso()
        _notify_admin(payments, entry)
    else:
        _finalize_payment(store, payments, entry, auto=True)
    store.save()
    ok("Pago registrado. El flujo de validación continúa en segundo plano.")
    press_enter()


def _detect_keywords(evidence: str) -> list[str]:
    lowered = evidence.lower()
    keywords = []
    for hint in ("aprob", "pago", "$", "transfer", "ok", "exitoso"):
        if hint in lowered:
            keywords.append(hint)
    return keywords


def _is_payment_valid(keywords: list[str]) -> bool:
    return any(hint in keywords for hint in ("aprob", "pago", "$", "exitoso"))


def _notify_admin(payments: dict[str, Any], entry: dict[str, Any]) -> None:
    admin = payments.get("admin_number")
    if not admin:
        return
    entry["notes"] = (
        f"Alerta enviada al administrador {admin} para validar el pago de {entry.get('name')}"
    )


def _review_pending_payments(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    pending = payments.get("pending", [])
    if not pending:
        _info("No hay pagos pendientes.")
        press_enter()
        return
    for entry in pending:
        print(_line())
        print(f"Contacto: {entry.get('name')} ({entry.get('number')})")
        print(f"Palabras clave detectadas: {', '.join(entry.get('keywords', [])) or 'ninguna'}")
        print(f"Estado actual: {entry.get('status')}")
        decision = ask("¿Marcar como confirmado (c), rechazar (r) o saltar (s)? ").strip().lower()
        if decision.startswith("c"):
            entry["status"] = "validado"
            entry["validated_at"] = _now_iso()
            _finalize_payment(store, payments, entry)
        elif decision.startswith("r"):
            entry["status"] = "rechazado"
            entry["notes"] = "El pago requiere nueva evidencia."
            _send_custom_message(store, entry, "Pago observado. Por favor compartinos una captura clara.")
    payments["pending"] = [
        item for item in pending if item.get("status") not in {"finalizado", "rechazado"}
    ]
    store.save()
    press_enter()


def _finalize_payment(
    store: WhatsAppDataStore,
    payments: dict[str, Any],
    entry: dict[str, Any],
    *,
    auto: bool = False,
) -> None:
    message = payments.get("welcome_message", "")
    link = payments.get("access_link", "")
    composed = message
    if link:
        composed = f"{message}\n{link}" if message else link
    _send_custom_message(store, entry, composed)
    entry["status"] = "finalizado"
    entry["welcome_sent_at"] = _now_iso()
    payments.setdefault("history", []).append(
        {
            "id": entry.get("id"),
            "name": entry.get("name"),
            "number": entry.get("number"),
            "status": "completado",
            "completed_at": _now_iso(),
            "notes": "Procesado automáticamente" if auto else entry.get("notes", ""),
        }
    )
    _update_contact_payment_status(store, entry)


def _send_custom_message(store: WhatsAppDataStore, entry: dict[str, Any], message: str) -> None:
    if not message:
        return
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if contact:
        contact.setdefault("history", []).append(
            {
                "type": "payment",
                "message": message,
                "sent_at": _now_iso(),
            }
        )
        contact["status"] = "acceso enviado"
        contact["access_sent_at"] = _now_iso()
        contact["last_payment_at"] = _now_iso()
        contact["last_message_sent"] = message
        contact["last_message_sent_at"] = _now_iso()
        contact["last_message_at"] = _now_iso()
    entry["notes"] = message


def _update_contact_payment_status(store: WhatsAppDataStore, entry: dict[str, Any]) -> None:
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if not contact:
        return
    contact["status"] = "pagó"
    contact["last_payment_at"] = _now_iso()
    contact.setdefault("history", []).append(
        {
            "type": "payment_confirmed",
            "timestamp": _now_iso(),
            "details": entry.get("notes", ""),
        }
    )


def _locate_contact_by_number(store: WhatsAppDataStore, number: str) -> dict[str, Any] | None:
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            if contact.get("number") == number:
                return contact
    return None


def _configure_payment_settings(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    admin = ask("Número del administrador para alertas: ").strip()
    welcome = ask_multiline("Mensaje de bienvenida tras confirmar pago: ").strip() or payments.get(
        "welcome_message", ""
    )
    link = ask("Link de acceso (opcional): ").strip() or payments.get("access_link", "")
    payments.update(
        {
            "admin_number": admin,
            "welcome_message": welcome,
            "access_link": link,
        }
    )
    store.save()
    ok("Datos actualizados. Los pagos se gestionarán con notificaciones limpias.")
    press_enter()


# ----------------------------------------------------------------------
# 8) Estado de contactos y actividad -----------------------------------

def _contacts_state(store: WhatsAppDataStore) -> None:
    banner()
    title("Estado general de contactos y actividad")
    print(_line())
    lists = list(store.iter_lists())
    if not lists:
        _info("Todavía no se cargaron listas de contactos.")
        press_enter()
        return
    totals = []
    for alias, data in lists:
        contacts = data.get("contacts", [])
        summary = _summarize_contacts(contacts)
        totals.append(summary)
        print(f"Lista: {alias}")
        for key, value in summary.items():
            print(f"   - {key}: {value}")
        print()
    if ask("¿Deseás exportar un CSV con el detalle? (s/n): ").strip().lower().startswith("s"):
        path = _export_contacts_csv(store)
        ok(f"Resumen exportado en {path}")
    press_enter()


def _summarize_contacts(contacts: Iterable[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "Total": 0,
        "Mensaje enviado": 0,
        "En espera": 0,
        "Respondió": 0,
        "Pagó": 0,
        "Acceso enviado": 0,
    }
    for contact in contacts:
        summary["Total"] += 1
        status = (contact.get("status") or "").lower()
        if "seguimiento" in status or "sin" in status:
            summary["En espera"] += 1
        if "mensaje" in status:
            summary["Mensaje enviado"] += 1
        if "respond" in status:
            summary["Respondió"] += 1
        if "pag" in status:
            summary["Pagó"] += 1
        if "acceso" in status:
            summary["Acceso enviado"] += 1
    return summary


def _export_contacts_csv(store: WhatsAppDataStore) -> Path:
    now = _now().strftime("%Y%m%d-%H%M%S")
    path = EXPORTS_DIR / f"whatsapp_estado_{now}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "lista",
                "nombre",
                "numero",
                "status",
                "ultimo_mensaje",
                "ultima_respuesta",
                "ultimo_seguimiento",
                "ultimo_pago",
                "acceso_enviado",
            ]
        )
        for alias, data in store.iter_lists():
            for contact in data.get("contacts", []):
                writer.writerow(
                    [
                        alias,
                        contact.get("name"),
                        contact.get("number"),
                        contact.get("status"),
                        contact.get("last_message_at"),
                        contact.get("last_response_at"),
                        contact.get("last_followup_at"),
                        contact.get("last_payment_at"),
                        contact.get("access_sent_at"),
                    ]
                )
    return path


__all__ = ["menu_whatsapp"]

>>>>>>> origin/main
