# -*- coding: utf-8 -*-  NUEVA VERSION MATI, SI FUNCIONA ESTO!
import base64
import importlib
import getpass
import json
import logging
import os
import re
import random
import subprocess
import sys
import time
import threading
import unicodedata
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dt_time, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse

from accounts import (
    _account_password,
    _store_account_password,
    get_account,
    list_all,
    mark_connected,
)
from config import (
    SETTINGS,
    read_app_config,
    read_env_local,
    refresh_settings,
    update_app_config,
    update_env_local,
)
from proxy_manager import record_proxy_failure, should_retry_proxy
from paths import runtime_base
from runtime import (
    STOP_EVENT,
    ensure_logging,
    request_stop,
    reset_stop_event,
    sleep_with_stop,
    start_q_listener,
)
from storage import get_auto_state, log_conversation_status, save_auto_state
from ui import Fore, full_line, style_text
from src.auth.onboarding import login_account_playwright
from src.auth.persistent_login import check_session
from src.dm_playwright_client import PlaywrightDMClient, ThreadLike, UserLike
from utils import ask, ask_int, banner, ok, press_enter, warn

_ZONEINFO_CLASS_SENTINEL = object()
_ZONEINFO_CLASS: object | None = _ZONEINFO_CLASS_SENTINEL


def _load_zoneinfo_class():
    """Obtiene la clase ZoneInfo desde la stdlib o backports."""

    global _ZONEINFO_CLASS
    if _ZONEINFO_CLASS is _ZONEINFO_CLASS_SENTINEL:
        zoneinfo_class = None
        try:  # pragma: no cover - depende de la versia�n de Python
            from zoneinfo import ZoneInfo as builtin_zoneinfo  # type: ignore[attr-defined]
            zoneinfo_class = builtin_zoneinfo
        except Exception:
            try:  # pragma: no cover - requiere dependencia opcional
                module = importlib.import_module("backports.zoneinfo")
                zoneinfo_class = getattr(module, "ZoneInfo", None)
            except Exception:
                zoneinfo_class = None
        _ZONEINFO_CLASS = zoneinfo_class
    return None if _ZONEINFO_CLASS is None else _ZONEINFO_CLASS

try:  # pragma: no cover - depende de dependencia opcional
    from dateutil import parser as date_parser
    from dateutil import tz as dateutil_tz
except Exception:  # pragma: no cover - fallback si falta dependencia
    date_parser = None  # type: ignore[assignment]
    dateutil_tz = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    import requests
    from requests import RequestException
except Exception:  # pragma: no cover - fallback si requests no esta�
    requests = None  # type: ignore
    RequestException = Exception  # type: ignore

try:  # pragma: no cover - depende de dependencias opcionales
    from google.oauth2.credentials import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
except Exception:  # pragma: no cover - si faltan dependencias opcionales
    Credentials = None  # type: ignore
    build = None  # type: ignore
    GoogleAuthRequest = None  # type: ignore

try:  # pragma: no cover - depende de dependencias opcionales
    from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
except Exception:  # pragma: no cover - si falta dependencia opcional
    InstalledAppFlow = None  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Responde� cordial, breve y como humano."
PROMPT_KEY = "autoresponder_system_prompt"
ACTIVE_ALIAS: str | None = None
MAX_SYSTEM_PROMPT_CHARS = 50000
_AUTORESPONDER_STUB_WARNED = False
_OPENAI_REPLY_FALLBACK = "Gracias por tu mensaje. Como te puedo ayudar?"
_OPENAI_DEFAULT_MODEL = "gpt-4o-mini"


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "si", "on"}


_AUTORESPONDER_VERBOSE_TECH_LOGS = _env_enabled("AUTORESPONDER_VERBOSE_TECH_LOGS", False)
_AUTORESPONDER_DEBUG_CYCLE_SUMMARY = _env_enabled(
    "AUTORESPONDER_DEBUG_CYCLE_SUMMARY",
    False,
)
_AUTORESPONDER_VERBOSE_SKIP_CONSOLE = _env_enabled(
    "AUTORESPONDER_VERBOSE_SKIP_CONSOLE",
    False,
)


def _read_env_value(env_values: Dict[str, str], key: str, default: str = "") -> str:
    raw = env_values.get(key)
    if raw is None or not str(raw).strip():
        raw = os.getenv(key, "")
    value = str(raw or "").strip()
    if value:
        return value
    return default


def _resolve_ai_api_key(env_values: Optional[Dict[str, str]] = None) -> str:
    values = env_values or read_env_local()
    openai_key = (
        values.get("OPENAI_API_KEY")
        or SETTINGS.openai_api_key
        or os.getenv("OPENAI_API_KEY")
        or ""
    )
    return str(openai_key).strip()


def _resolve_ai_base_url(
    api_key: str,
    *,
    env_values: Optional[Dict[str, str]] = None,
) -> str:
    values = env_values or read_env_local()
    return _read_env_value(values, "OPENAI_BASE_URL")


def _resolve_ai_model(
    api_key: str,
    *,
    env_values: Optional[Dict[str, str]] = None,
) -> str:
    values = env_values or read_env_local()
    explicit_model = _read_env_value(values, "OPENAI_MODEL")
    if explicit_model:
        return explicit_model
    return _OPENAI_DEFAULT_MODEL


def _resolve_ai_runtime(api_key: str) -> tuple[str, str]:
    values = read_env_local()
    model = _resolve_ai_model(api_key, env_values=values)
    return "OpenAI", model


def _build_openai_client(api_key: str) -> object:
    from openai import OpenAI

    values = read_env_local()
    base_url = _resolve_ai_base_url(api_key, env_values=values)
    kwargs: Dict[str, object] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def _probe_ai_runtime(api_key: str) -> tuple[bool, str]:
    try:
        client = _build_openai_client(api_key)
        model = _resolve_ai_model(api_key)
        _openai_generate_text(
            client,
            system_prompt="Responde solo: ok",
            user_content="hola",
            model=model,
            temperature=0.0,
            max_output_tokens=12,
        )
        return True, "ok"
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        if status in {401, 403}:
            return False, (
                "No se pudo autenticar con IA (401/403). "
                "Revisá OPENAI_API_KEY y OPENAI_MODEL configurados."
            )
        return False, f"No se pudo validar IA antes de iniciar: {exc}"


def _extract_openai_text(response: object) -> str:
    text = getattr(response, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()

    choices = getattr(response, "choices", None)
    if isinstance(choices, list):
        for choice in choices:
            message = getattr(choice, "message", None)
            content = getattr(message, "content", None) if message is not None else None
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                parts: List[str] = []
                for item in content:
                    if isinstance(item, str):
                        if item.strip():
                            parts.append(item.strip())
                        continue
                    item_text = getattr(item, "text", None)
                    if isinstance(item_text, str) and item_text.strip():
                        parts.append(item_text.strip())
                if parts:
                    return "\n".join(parts).strip()
    return ""


def _openai_generate_text(
    client: object,
    *,
    system_prompt: str,
    user_content: str,
    model: str = _OPENAI_DEFAULT_MODEL,
    temperature: float = 0.2,
    max_output_tokens: int = 180,
) -> str:
    responses_api = getattr(client, "responses", None)
    if responses_api is not None and hasattr(responses_api, "create"):
        try:
            response = responses_api.create(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
            text = _extract_openai_text(response)
            if text:
                return text
        except Exception as exc:
            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                logger.info(
                    "Responses API no disponible para modelo '%s': %s. Fallback a chat.completions.",
                    model,
                    exc,
                )

    chat_api = getattr(client, "chat", None)
    completions_api = getattr(chat_api, "completions", None) if chat_api is not None else None
    if completions_api is None or not hasattr(completions_api, "create"):
        raise RuntimeError("Cliente OpenAI sin API de texto compatible.")

    completion = completions_api.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        temperature=temperature,
        max_tokens=max_output_tokens,
    )
    return _extract_openai_text(completion).strip()


_AI_REPLY_DISALLOWED_TOKENS = (
    "```",
    "<json",
    '"enviar"',
    "como asistente",
    "como ia",
    "as an ai",
    "i am an ai",
)


def _sanitize_generated_message(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    if len(text) >= 2 and (
        (text.startswith('"') and text.endswith('"'))
        or (text.startswith("'") and text.endswith("'"))
    ):
        text = text[1:-1].strip()
    for prefix in ("respuesta:", "mensaje:", "bot:", "yo:"):
        if text.lower().startswith(prefix):
            text = text[len(prefix):].strip()
    return text


def _generated_message_issues(message_text: str) -> List[str]:
    text = str(message_text or "").strip()
    if not text:
        return ["empty"]
    if len(text) < 2:
        return ["too_short"]
    if len(text) > 700:
        return ["too_long"]
    normalized = _normalize_text_for_match(text)
    if normalized == _normalize_text_for_match(_OPENAI_REPLY_FALLBACK):
        return ["fallback_text"]
    for token in _AI_REPLY_DISALLOWED_TOKENS:
        if token in normalized:
            return [f"contains_disallowed:{token}"]
    if text.startswith("{") or text.startswith("["):
        return ["looks_like_json"]
    return []


def _build_strict_responder_system_prompt(prompt_text: str) -> str:
    clean_prompt = _normalize_system_prompt_text(prompt_text).strip() or DEFAULT_PROMPT
    return (
        "Sos el motor de respuesta de DM para Instagram.\n"
        "Debes seguir AL PIE DE LA LETRA el PROMPT_NEGOCIO.\n"
        "No inventes datos fuera del contexto y memoria entregados.\n"
        "Salida obligatoria: SOLO el texto final del mensaje a enviar, sin comillas, sin JSON, sin explicaciones.\n"
        "Si falta información para cumplir el prompt, hacé una pregunta breve alineada al prompt.\n\n"
        "<PROMPT_NEGOCIO>\n"
        f"{clean_prompt}\n"
        "</PROMPT_NEGOCIO>"
    )


def _build_responder_user_content(conversation_text: str, memory_context: str = "") -> str:
    lines = [
        "Conversacion (cronologico):",
        str(conversation_text or "").strip() or "(sin conversacion)",
        "",
    ]
    memory_clean = str(memory_context or "").strip()
    if memory_clean:
        lines.extend(
            [
                "Memoria del hilo (estado persistido):",
                memory_clean,
                "",
            ]
        )
    lines.append("Genera un unico mensaje final para responder ahora.")
    return "\n".join(lines).strip()


def _build_memory_context_from_state(
    account: str,
    thread_id: str,
    conv_state: Dict[str, Any],
    *,
    stage: str,
    recipient_username: str,
) -> str:
    now_ts = time.time()
    last_sent_at = conv_state.get("last_message_sent_at")
    last_received_at = conv_state.get("last_message_received_at")
    try:
        seconds_since_last_sent = int(max(0.0, now_ts - float(last_sent_at))) if last_sent_at else -1
    except Exception:
        seconds_since_last_sent = -1
    try:
        seconds_since_last_received = int(max(0.0, now_ts - float(last_received_at))) if last_received_at else -1
    except Exception:
        seconds_since_last_received = -1

    sent_history = conv_state.get("messages_sent", [])
    if not isinstance(sent_history, list):
        sent_history = []
    sent_lines: List[str] = []
    for sent in sent_history[-6:]:
        if not isinstance(sent, dict):
            continue
        text_value = str(sent.get("text", "") or "").strip()
        if not text_value:
            continue
        attempts = int(sent.get("times_sent", 1) or 1)
        followup_flag = bool(sent.get("is_followup", False))
        sent_lines.append(
            f"- {'FOLLOWUP' if followup_flag else 'RESPUESTA'} | intentos={attempts} | texto={text_value}"
        )

    lines = [
        f"cuenta=@{account}",
        f"thread_id={thread_id}",
        f"lead={recipient_username or conv_state.get('recipient_username') or 'unknown'}",
        f"stage_actual={stage or conv_state.get('stage') or _STAGE_INITIAL}",
        f"last_message_id_seen={conv_state.get('last_message_id_seen') or '-'}",
        f"seconds_since_last_sent={seconds_since_last_sent}",
        f"seconds_since_last_received={seconds_since_last_received}",
        "historial_bot_ultimos_6:",
    ]
    if sent_lines:
        lines.extend(sent_lines)
    else:
        lines.append("- (sin mensajes previos del bot)")
    return "\n".join(lines)


def _safe_parse_datetime(*args, **kwargs) -> Optional[datetime]:
    """Parsea una fecha utilizando dateutil si esta� disponible."""
    if date_parser is None:
        return None
    try:
        return date_parser.parse(*args, **kwargs)
    except Exception:
        return None

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\a?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%a-]a@[A-Z0-9.-]a\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None
_DEFAULT_GOHIGHLEVEL_PROMPT = (
    "Sos un asistente que evala�a conversaciones de Instagram y determina si un lead esta� "
    "calificado para enviarse automa�ticamente al CRM GoHighLevel. Responde� a�nicamente "
    "con 'SI' cuando corresponda enviarlo y 'NO' cuando no cumpla con los criterios. "
    "Considera� el contexto, el intera�s real del lead y si el equipo comercial debera�a "
    "contactarlo."
)

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"












_GOOGLE_CALENDAR_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "google_calendar.json"
)
_GOOGLE_DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
_GOOGLE_CALENDAR_BASE = "https://www.googleapis.com/calendar/v3"
_GOOGLE_SCOPE = "https://www.googleapis.com/auth/calendar"
_DEFAULT_GOOGLE_CALENDAR_PROMPT = ""
_GOOGLE_REDIRECT_URI = "http://localhost"
_GOOGLE_STATE: Dict[str, dict] | None = None
_MEETING_TIME_PATTERN = re.compile(
    r"(?P<hour>\b[01]?\d|2[0-3])(?:(?:[:h\.])(?P<minute>[0-5]\d))?\s*(?P<ampm>am|pm)?\s*(?P<label>hs|hrs|horas)?",
    re.IGNORECASE,
)
_MEETING_DATE_PATTERN = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b",
    re.IGNORECASE,
)
_RELATIVE_DATE_KEYWORDS = (
    ("hoy", 0),
    ("manana", 1),
    ("pasado manana", 2),
)
_WEEKDAY_KEYWORDS = {
    "lunes": 0,
    "martes": 1,
    "miercoles": 2,
    "mia�rcoles": 2,
    "jueves": 3,
    "viernes": 4,
    "sabado": 5,
    "si�bado": 5,
    "domingo": 6,
}



_GOOGLE_CALENDAR_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "google_calendar.json"
)
_GOOGLE_DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
_GOOGLE_CALENDAR_BASE = "https://www.googleapis.com/calendar/v3"
_GOOGLE_SCOPE = "https://www.googleapis.com/auth/calendar"
_GOOGLE_STATE: Dict[str, dict] | None = None
_MEETING_TIME_PATTERN = re.compile(
    r"(?P<hour>\b[01]?\d|2[0-3])(?:(?:[:h\.])(?P<minute>[0-5]\d))?\s*(?P<ampm>am|pm)?\s*(?P<label>hs|hrs|horas)?",
    re.IGNORECASE,
)
_MEETING_DATE_PATTERN = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b",
    re.IGNORECASE,
)
_RELATIVE_DATE_KEYWORDS = (
    ("hoy", 0),
    ("manana", 1),
    ("pasado manana", 2),
)
_WEEKDAY_KEYWORDS = {
    "lunes": 0,
    "martes": 1,
    "miercoles": 2,
    "mia�rcoles": 2,
    "jueves": 3,
    "viernes": 4,
    "sabado": 5,
    "si�bado": 5,
    "domingo": 6,
}

_FOLLOWUP_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "followups.json"
)
_FOLLOWUP_STATE: Dict[str, dict] | None = None
_DEFAULT_FOLLOWUP_PROMPT = (
    "Disea�a� un plan de seguimiento amable para leads de Instagram. "
    "Envia� el primer recordatorio cuando hayan pasado al menos 6 horas sin respuesta. "
    "Si despua�s de 12 horas ma�s no responden, envia� un segundo y a�ltimo mensaje. "
    "No enva�es ma�s de dos seguimientos y evita� sonar insistente."
)
_FOLLOWUP_MIN_INTERVAL = 300
_FOLLOWUP_HISTORY_MAX_AGE = 14 * 24 * 3600

_CONVERSATION_ENGINE_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "conversation_engine.json"
)
_CONVERSATION_ENGINE_CACHE: Dict[str, dict] | None = None

_MESSAGE_LOG_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "message_log.jsonl"
)
_MESSAGE_LOG_LOCK = threading.Lock()

_STAGE_INITIAL = "initial"
_STAGE_FOLLOWUP = "followup"
_STAGE_WAITING = "waiting"
_STAGE_CLOSED = "closed"
_STAGE_ACTIVE = "active"

_MIN_TIME_BETWEEN_MESSAGES = 60
_MIN_TIME_FOR_FOLLOWUP = 4 * 3600
_MIN_TIME_FOR_REACTIVATION = 24 * 3600

# En producción no forzar respuestas: respetar validaciones para evitar envíos incorrectos.
_FORCE_ALWAYS_RESPOND = False
_FORCE_ALWAYS_FOLLOWUP = True
_OPEN_FAIL_BACKOFF_AFTER = max(1, int(os.getenv("AUTORESPONDER_OPEN_FAIL_BACKOFF_AFTER", "3")))
_OPEN_FAIL_BACKOFF_SECONDS = max(
    30.0,
    float(os.getenv("AUTORESPONDER_OPEN_FAIL_BACKOFF_SECONDS", "180")),
)


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


def _get_conversation_key(account: str, thread_id: str) -> str:
    account_norm = _normalize_username(account)
    thread_str = str(thread_id).strip()
    return f"{account_norm}|{thread_str}"


def _load_conversation_engine(refresh: bool = False) -> Dict[str, dict]:
    global _CONVERSATION_ENGINE_CACHE
    if refresh or _CONVERSATION_ENGINE_CACHE is None:
        data: Dict[str, dict] = {"conversations": {}}
        if _CONVERSATION_ENGINE_FILE.exists():
            try:
                loaded = json.loads(_CONVERSATION_ENGINE_FILE.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data.update(loaded)
                    if "conversations" not in data:
                        data["conversations"] = {}
            except Exception as exc:
                logger.warning("Error cargando conversation_engine.json: %s", exc, exc_info=False)
                data = {"conversations": {}}
        _CONVERSATION_ENGINE_CACHE = data
    return _CONVERSATION_ENGINE_CACHE


def _save_conversation_engine() -> None:
    if _CONVERSATION_ENGINE_CACHE is None:
        return
    try:
        _CONVERSATION_ENGINE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CONVERSATION_ENGINE_FILE.write_text(
            json.dumps(_CONVERSATION_ENGINE_CACHE, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if _AUTORESPONDER_VERBOSE_TECH_LOGS:
            print(style_text(f"[Persistencia] Archivo {_CONVERSATION_ENGINE_FILE} actualizado físicamente.", color=Fore.GREEN))
    except Exception as exc:
        logger.warning("Error guardando conversation_engine.json: %s", exc, exc_info=False)


def _append_message_log(event: Dict[str, Any]) -> None:
    record = dict(event or {})
    record.setdefault("ts", int(time.time()))
    record.setdefault("iso", datetime.utcnow().isoformat())
    try:
        payload = json.dumps(record, ensure_ascii=False)
    except Exception:
        return
    try:
        _MESSAGE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _MESSAGE_LOG_LOCK:
            with open(_MESSAGE_LOG_FILE, "a", encoding="utf-8") as handle:
                handle.write(f"{payload}\n")
    except Exception:
        logger.debug("No se pudo registrar message_log.jsonl", exc_info=False)

def _get_conversation_state(account: str, thread_id: str) -> Dict[str, Any]:
    engine = _load_conversation_engine()
    conversations = engine.get("conversations", {})
    key = _get_conversation_key(account, thread_id)
    return conversations.get(key, {})


def _update_conversation_state(
    account: str,
    thread_id: str,
    updates: Dict[str, Any],
    recipient_username: Optional[str] = None,
) -> Dict[str, Any]:
    engine = _load_conversation_engine()
    conversations = engine.setdefault("conversations", {})
    key = _get_conversation_key(account, thread_id)
    print(style_text(f"[TRACE_ID ENGINE_KEY] key={key} existed={key in conversations}", color=Fore.WHITE))
    current = conversations.get(key, {})
    if not current:
        current = {
            "account": account,
            "thread_id": str(thread_id),
            "recipient_username": recipient_username or current.get("recipient_username", ""),
            "stage": _STAGE_INITIAL,
            "messages_sent": [],
            "last_message_sent_at": None,
            "last_message_received_at": None,
            "last_inbound_id_seen": None,
            "last_message_id_seen": None,
            "pending_reply": False,
            "pending_inbound_id": None,
            "last_reply_failure_reason": None,
            "last_reply_failed_at": None,
            "last_send_failed_at": None,
            "last_open_failed_at": None,
            "consecutive_open_failures": 0,
            "open_backoff_until": None,
            "prompt_sequence_done": False,
            "prompt_sequence_done_at": None,
            "last_message_sender": None,
            "followup_stage": 0,
            "last_followup_sent_at": None,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
    current.update(updates)
    current["updated_at"] = time.time()
    
    if recipient_username:
        current["recipient_username"] = recipient_username
    
    conversations[key] = current
    _CONVERSATION_ENGINE_CACHE = engine
    _save_conversation_engine()
    return current


def _record_message_sent(
    account: str,
    thread_id: str,
    message_text: str,
    message_id: Optional[str] = None,
    recipient_username: Optional[str] = None,
    is_followup: bool = False,
    followup_stage: Optional[int] = None,
) -> None:
    now = time.time()
    state = _get_conversation_state(account, thread_id)
    
    messages_sent = state.get("messages_sent", [])
    message_normalized = message_text.strip().lower()
    message_found = False
    for sent_msg in messages_sent:
        if sent_msg.get("text", "").strip().lower() == message_normalized:
            sent_msg["last_sent_at"] = now
            sent_msg["times_sent"] = sent_msg.get("times_sent", 0) + 1
            if message_id:
                sent_msg["last_message_id"] = message_id
            if is_followup:
                sent_msg["is_followup"] = True
            message_found = True
            break
    
    if not message_found:
        new_message = {
            "text": message_text,
            "first_sent_at": now,
            "last_sent_at": now,
            "message_id": message_id or "",
            "times_sent": 1,
            "is_followup": is_followup,
        }
        messages_sent.append(new_message)
    
    updates = {
        "messages_sent": messages_sent,
        "last_message_sent_at": now,
        "last_message_sender": "bot",
    }
    
    if is_followup and followup_stage is not None:
        updates["followup_stage"] = followup_stage
        updates["last_followup_sent_at"] = now
    
    _update_conversation_state(
        account,
        thread_id,
        updates,
        recipient_username=recipient_username,
    )
    _append_message_log(
        {
            "action": "message_sent",
            "account": account,
            "thread_id": str(thread_id),
            "lead": recipient_username or "",
            "is_followup": bool(is_followup),
            "followup_stage": followup_stage if is_followup else None,
            "message_id": message_id or "",
            "message_text": message_text,
        }
    )


def _record_message_received(
    account: str,
    thread_id: str,
    message_id: Optional[str] = None,
    recipient_username: Optional[str] = None,
) -> None:
    now = time.time()
    state = _get_conversation_state(account, thread_id)
    
    updates = {
        "last_message_received_at": now,
        "last_message_sender": "lead",
    }
    
    if message_id:
        updates["last_inbound_id_seen"] = message_id
        updates["last_message_id_seen"] = message_id
        updates["pending_reply"] = False
        updates["pending_inbound_id"] = None
        updates["last_reply_failure_reason"] = None
        updates["last_reply_failed_at"] = None
        updates["last_send_failed_at"] = None
        updates["last_open_failed_at"] = None
        updates["consecutive_open_failures"] = 0
        updates["open_backoff_until"] = None
    
    if state.get("last_message_sent_at") and state.get("stage") in (_STAGE_INITIAL, _STAGE_FOLLOWUP, _STAGE_WAITING):
        updates["stage"] = _STAGE_ACTIVE
        updates["followup_stage"] = 0
    
    _update_conversation_state(account, thread_id, updates, recipient_username=recipient_username)
    _append_message_log(
        {
            "action": "message_received",
            "account": account,
            "thread_id": str(thread_id),
            "lead": recipient_username or "",
            "message_id": message_id or "",
        }
    )


def _mark_reply_pending(
    account: str,
    thread_id: str,
    *,
    recipient_username: Optional[str],
    inbound_message_id: str,
    reason: str,
    open_failed: bool = False,
) -> None:
    now_ts = time.time()
    state = _get_conversation_state(account, thread_id)
    previous_open_failures = _safe_int(state.get("consecutive_open_failures"))
    updates: Dict[str, Any] = {
        "pending_reply": True,
        "pending_inbound_id": inbound_message_id or str(state.get("pending_inbound_id") or "").strip() or None,
        "last_reply_failure_reason": str(reason or "").strip() or "reply_failed",
        "last_reply_failed_at": now_ts,
    }
    if open_failed:
        open_failures = previous_open_failures + 1
        updates["consecutive_open_failures"] = open_failures
        updates["last_open_failed_at"] = now_ts
        if open_failures >= _OPEN_FAIL_BACKOFF_AFTER:
            updates["open_backoff_until"] = now_ts + _OPEN_FAIL_BACKOFF_SECONDS
    else:
        updates["last_send_failed_at"] = now_ts
        updates["consecutive_open_failures"] = 0
        updates["open_backoff_until"] = None
    _update_conversation_state(account, thread_id, updates, recipient_username)


def _determine_conversation_stage(
    account: str,
    thread_id: str,
    has_new_inbound: bool,
    time_since_last_sent: Optional[float],
    time_since_last_received: Optional[float],
) -> str:
    state = _get_conversation_state(account, thread_id)
    current_stage = state.get("stage", _STAGE_INITIAL)
    messages_sent = state.get("messages_sent", [])
    
    if not messages_sent:
        return _STAGE_INITIAL
    
    if has_new_inbound:
        return _STAGE_ACTIVE
    
    if current_stage == _STAGE_CLOSED:
        return _STAGE_CLOSED
    
    if time_since_last_received is None or time_since_last_received > _MIN_TIME_FOR_FOLLOWUP:
        if time_since_last_sent is None or time_since_last_sent > _MIN_TIME_FOR_FOLLOWUP:
            return _STAGE_FOLLOWUP
        return _STAGE_WAITING
    
    if time_since_last_received and time_since_last_received < 3600:
        return _STAGE_ACTIVE
    
    return current_stage


def _can_send_message(
    account: str,
    thread_id: str,
    message_text: str,
    latest_inbound_id: Optional[str] = None,
    force: bool = False,
) -> tuple[bool, str]:
    if force:
        return True, "forced"
    state = _get_conversation_state(account, thread_id)
    now = time.time()
    last_inbound_seen = str(state.get("last_inbound_id_seen") or "").strip()
    current_inbound = str(latest_inbound_id or "").strip()
    has_unprocessed_inbound = bool(current_inbound and current_inbound != last_inbound_seen)
    
    messages_sent = state.get("messages_sent", [])
    message_normalized = message_text.strip().lower()
    if not message_normalized:
        return False, "Respuesta vacia/no generada por IA"
    
    for sent_msg in messages_sent:
        if sent_msg.get("text", "").strip().lower() == message_normalized:
            last_sent = sent_msg.get("last_sent_at", 0)
            times_sent = sent_msg.get("times_sent", 0)
            
            if (not has_unprocessed_inbound) and (now - last_sent < 3600):
                return False, f"Mensaje ya enviado hace {int((now - last_sent) / 60)} minutos"
            
            if times_sent >= 3:
                return False, "Mensaje ya enviado 3 veces, evitar repetición"
    
    last_sent_at = state.get("last_message_sent_at")
    if last_sent_at and not force:
        time_since_last = now - last_sent_at
        if time_since_last < _MIN_TIME_BETWEEN_MESSAGES:
            remaining = int(_MIN_TIME_BETWEEN_MESSAGES - time_since_last)
            return False, f"Esperar {remaining} segundos antes de enviar otro mensaje"
    
    if state.get("stage") == _STAGE_CLOSED:
        return False, "Conversación cerrada, no enviar más mensajes"
    
    return True, "ok"


def _should_process_old_lead(
    account: str,
    thread_id: str,
    time_since_last_activity: float,
) -> bool:
    state = _get_conversation_state(account, thread_id)
    stage = state.get("stage", _STAGE_INITIAL)
    
    if not state.get("messages_sent"):
        return True
    
    if stage == _STAGE_CLOSED:
        return False
    
    if time_since_last_activity > _MIN_TIME_FOR_REACTIVATION:
        return True
    
    if stage == _STAGE_FOLLOWUP and time_since_last_activity > _MIN_TIME_FOR_FOLLOWUP:
        return True
    
    if stage == _STAGE_ACTIVE:
        return True
    
    return False


def _read_followup_state(refresh: bool = False) -> Dict[str, dict]:
    global _FOLLOWUP_STATE
    if refresh or _FOLLOWUP_STATE is None:
        data: Dict[str, dict] = {"aliases": {}}
        if _FOLLOWUP_FILE.exists():
            try:
                loaded = json.loads(_FOLLOWUP_FILE.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data.update(loaded)
            except Exception:
                data = {"aliases": {}}
        aliases = data.get("aliases")
        if not isinstance(aliases, dict):
            data["aliases"] = {}
        _FOLLOWUP_STATE = data
    return _FOLLOWUP_STATE


def _write_followup_state(state: Dict[str, dict]) -> None:
    state.setdefault("aliases", {})
    _FOLLOWUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    _FOLLOWUP_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _read_followup_state(refresh=True)


def _followup_prune_history(history: Dict[str, dict]) -> Dict[str, dict]:
    now = time.time()
    cleaned: Dict[str, dict] = {}
    for key, record in history.items():
        if not isinstance(record, dict):
            continue
        last_ts = record.get("last_sent_ts") or record.get("last_eval_ts")
        try:
            ts_value = float(last_ts)
        except Exception:
            ts_value = 0.0
        if ts_value and now - ts_value > _FOLLOWUP_HISTORY_MAX_AGE:
            continue
        cleaned[key] = record
    return cleaned


def _get_followup_entry(alias: str) -> Dict[str, object]:
    state = _read_followup_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if not isinstance(entry, dict):
        return {}
    entry.setdefault("alias", alias.strip() or alias)
    entry.setdefault("enabled", False)
    entry.setdefault("accounts", [])
    entry.setdefault("prompt", _DEFAULT_FOLLOWUP_PROMPT)
    entry.setdefault("history", {})
    history = entry.get("history")
    if isinstance(history, dict):
        entry["history"] = _followup_prune_history(history)
    return entry


def _set_followup_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias inva�lido.")
        return
    state = _read_followup_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
    entry.setdefault("prompt", _DEFAULT_FOLLOWUP_PROMPT)
    normalized_updates: Dict[str, object] = {}
    for key_name, value in updates.items():
        if key_name == "accounts":
            normalized: List[str] = []
            if isinstance(value, (list, tuple, set)):
                seen: set[str] = set()
                for raw in value:
                    if not isinstance(raw, str):
                        continue
                    norm = _normalize_username(raw)
                    if norm and norm not in seen:
                        seen.add(norm)
                        normalized.append(norm)
            elif isinstance(value, str):
                norm = _normalize_username(value)
                if norm:
                    normalized = [norm]
            normalized_updates[key_name] = normalized
        elif key_name == "enabled":
            normalized_updates[key_name] = bool(value)
        elif key_name == "prompt":
            normalized_updates[key_name] = str(value or "")
        elif key_name == "history":
            if isinstance(value, dict):
                normalized_updates[key_name] = _followup_prune_history(value)
        else:
            normalized_updates[key_name] = value
    entry.update(normalized_updates)
    aliases[key] = entry
    _write_followup_state(state)


def _followup_status_lines() -> List[str]:
    state = _read_followup_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return ["(sin configuraciones)"]
    rows: List[str] = []
    for key in sorted(aliases.keys()):
        entry = aliases[key]
        if not isinstance(entry, dict):
            continue
        alias_label = str(entry.get("alias") or key)
        enabled = bool(entry.get("enabled"))
        accounts = entry.get("accounts") or []
        if accounts:
            preview_accounts = [f"@{acc}" for acc in accounts[:3]]
            if len(accounts) > 3:
                preview_accounts.append(f"a{len(accounts) - 3}")
            accounts_label = ", ".join(preview_accounts)
        else:
            accounts_label = "todas las cuentas del alias"
        prompt_preview = _preview_prompt(str(entry.get("prompt") or ""))
        status_label = "Activo" if enabled else "Inactivo"
        rows.append(
            f" - {alias_label}: {status_label} ��� Cuentas: {accounts_label} ��� Prompt: {prompt_preview}"
        )
    return rows


def _followup_summary_line() -> str:
    state = _read_followup_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    active = [
        str(entry.get("alias") or key)
        for key, entry in aliases.items()
        if isinstance(entry, dict) and entry.get("enabled")
    ]
    if not active:
        return "Seguimiento: (sin configurar)"
    return f"Seguimiento: activo para {', '.join(sorted(active))}"


def _followup_accounts_for_alias(alias: str) -> List[str]:
    targets = _choose_targets(alias)
    normalized = {_normalize_username(user) for user in targets}
    return sorted(account for account in normalized if account)


def _followup_enabled_entry_for(username: str) -> tuple[Optional[str], Dict[str, object]]:
    alias_candidates: List[str] = []
    alias_candidates.append(username)
    account_data = get_account(username) or {}
    account_alias = str(account_data.get("alias") or "").strip()
    if account_alias:
        alias_candidates.append(account_alias)
    if ACTIVE_ALIAS:
        alias_candidates.append(ACTIVE_ALIAS)
    alias_candidates.append("ALL")

    seen: set[str] = set()
    for alias in alias_candidates:
        norm = _normalize_alias_key(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        entry = _get_followup_entry(alias)
        if not entry or not entry.get("enabled"):
            continue
        accounts = entry.get("accounts") or []
        if accounts:
            norm_user = _normalize_username(username)
            if norm_user not in accounts:
                continue
        return alias, entry
    return None, {}


def _followup_allowed_thread_ids(user: str) -> set[str]:
    alias, entry = _followup_enabled_entry_for(user)
    if not alias or not entry or not entry.get('enabled'):
        return set()
    history_source = entry.get('history')
    history = history_source if isinstance(history_source, dict) else {}
    account_norm = _normalize_username(user)
    allowed: set[str] = set()
    prefix = f'{account_norm}|'
    for key in history.keys():
        if not isinstance(key, str):
            continue
        if key.startswith(prefix):
            allowed.add(key.split('|', 1)[1])
    return allowed

def _followup_configure_accounts() -> None:
    banner()
    print(style_text("Seguimiento automa�tico ��� Cuentas", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _followup_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _prompt_alias_selection()
    if not alias:
        return
    available = _followup_accounts_for_alias(alias)
    if not available:
        warn("No se encontraron cuentas activas para ese alias.")
        press_enter()
        return
    entry = _get_followup_entry(alias)
    stored_accounts = set(entry.get("accounts") or [])
    use_all = entry.get("enabled") and not stored_accounts
    print("Selecciona� las cuentas que usara�n seguimiento automa�tico:")
    for idx, account in enumerate(available, start=1):
        selected = use_all or account in stored_accounts
        marker = "[x]" if selected else "[ ]"
        print(f" {idx:>2}) {marker} @{account}")
    print("  0) Todas las cuentas del alias")
    choice = ask(
        "Na�meros separados por coma (vaca�o cancela, 0 = todas): "
    ).strip()
    if not choice:
        warn("No se realizaron cambios.")
        press_enter()
        return
    if choice.lower() in {"0", "todas", "all"}:
        _set_followup_entry(alias, {"accounts": [], "enabled": True})
        ok("Seguimiento habilitado para todas las cuentas del alias.")
        press_enter()
        return
    tokens = re.split(r"[\s,;]a", choice)
    indices: List[int] = []
    for token in tokens:
        if not token:
            continue
        if not token.isdigit():
            continue
        idx = int(token)
        if 1 <= idx <= len(available):
            if idx not in indices:
                indices.append(idx)
    if not indices:
        warn("No se seleccionaron cuentas validas.")
        press_enter()
        return
    selected_accounts = [available[i - 1] for i in indices]
    _set_followup_entry(alias, {"accounts": selected_accounts, "enabled": True})
    ok(f"Seguimiento habilitado para {len(selected_accounts)} cuentas.")
    press_enter()


def _followup_configure_prompt() -> None:
    banner()
    print(style_text("Seguimiento automatico Prompt", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _followup_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _prompt_alias_selection()
    if not alias:
        return
    entry = _get_followup_entry(alias)
    current_prompt = str(entry.get("prompt") or _DEFAULT_FOLLOWUP_PROMPT)
    print(style_text("Prompt actual:", color=Fore.BLUE))
    print(current_prompt.strip() or "(sin definir)")
    print(full_line(color=Fore.BLUE))
    print("Elige una opcian:")
    print("  E) Editar prompt (pegar en consola)")
    print("  T) Cargar desde archivo .txt")
    print("  D) Restaurar valor predeterminado")
    print("  Enter) Cancelar")
    action = ask("Accian: ").strip().lower()
    if not action:
        warn("No se realizaron cambios.")
        press_enter()
        return
    if action in {"d", "default", "predeterminado"}:
        _set_followup_entry(alias, {"prompt": _DEFAULT_FOLLOWUP_PROMPT})
        ok("Se restauro el prompt predeterminado de seguimiento.")
        press_enter()
        return
    if action in {"t", "txt", "archivo", "file"}:
        path_input = ask("Ruta del archivo .txt (vacio para cancelar): ").strip()
        if not path_input:
            warn("No se realizaron cambios.")
            press_enter()
            return
        file_path = Path(path_input).expanduser()
        if not file_path.exists():
            warn("El archivo especificado no existe.")
            press_enter()
            return
        try:
            new_prompt = file_path.read_text(encoding="utf-8").strip()
        except Exception as exc:
            warn(f"No se pudo leer el archivo: {exc}")
            press_enter()
            return
        _set_followup_entry(alias, {"prompt": new_prompt})
        if new_prompt:
            ok(f"Prompt actualizado. Longitud: {len(new_prompt)} caracteres.")
        else:
            ok("Se elimino el prompt personalizado. Se usara el valor predeterminado.")
        press_enter()
        return
    if action not in {"e", "editar"}:
        warn("Opcian invalida.")
        press_enter()
        return
    print(
        style_text(
            "Pega el nuevo prompt y finaliza con una lanea que diga <<<END>>>.",
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("Ǧ ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    new_prompt = "\n".join(lines).strip()
    _set_followup_entry(alias, {"prompt": new_prompt})
    if new_prompt:
        ok(f"Prompt actualizado. Longitud: {len(new_prompt)} caracteres.")
    else:
        ok("Se elimino� el prompt personalizado. Se usara� el valor predeterminado.")
    press_enter()


def _followup_disable() -> None:
    banner()
    print(style_text("Seguimiento automa�tico ��� Desactivar", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _followup_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _prompt_alias_selection()
    if not alias:
        return
    entry = _get_followup_entry(alias)
    if not entry or not entry.get("enabled"):
        warn("El seguimiento ya esta� inactivo para ese alias.")
        press_enter()
        return
    _set_followup_entry(alias, {"enabled": False, "history": {}})
    ok("Seguimiento desactivado para ese alias.")
    press_enter()


def _followup_menu() -> None:
    while True:
        banner()
        print(style_text("Seguimiento automa�tico", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _followup_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Configurar cuentas con seguimiento")
        print("2) Configurar prompt de seguimiento")
        print("3) Desactivar seguimiento para un alias")
        print("4) Volver")
        choice = ask("Opcia�n: ").strip()
        if choice == "1":
            _followup_configure_accounts()
        elif choice == "2":
            _followup_configure_prompt()
        elif choice == "3":
            _followup_disable()
        elif choice == "4":
            break
        else:
            warn("Opcia�n inva�lida.")
            press_enter()


def _followup_decision(
    api_key: str,
    prompt_text: str,
    conversation: str,
    metadata: Dict[str, object],
) -> Optional[tuple[str, int]]:
    prompt_text = prompt_text.strip()
    if not prompt_text or not api_key:
        return None
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning(
            "No se pudo inicializar cliente IA para seguimiento: %s",
            exc,
            exc_info=False,
        )
        return None
    model = _resolve_ai_model(api_key)

    system_prompt = (
        "Sos el motor de FOLLOWUP de Instagram.\n"
        "Debes seguir AL PIE DE LA LETRA el PROMPT_FOLLOWUP.\n"
        "Salida obligatoria: SOLO un JSON valido con las claves "
        "'enviar' (booleano), 'mensaje' (texto), 'etapa' (entero).\n"
        "Si NO corresponde enviar: enviar=false, mensaje='', etapa=intento_followup_siguiente.\n"
        "Si SI corresponde enviar: mensaje debe ser texto listo para enviar (sin comillas ni markdown).\n"
        "No inventes etapas tecnicas: usa el intento_followup_siguiente.\n\n"
        "<PROMPT_FOLLOWUP>\n"
        f"{prompt_text}\n"
        "</PROMPT_FOLLOWUP>"
    )
    context_lines = ["Contexto:"]
    for key, value in metadata.items():
        context_lines.append(f"- {key}: {value}")
    context_lines.append("")
    context_lines.append("Conversacia�n completa (orden cronola�gico):")
    context_lines.append(conversation)
    context_lines.append("")
    context_lines.append(
        "Recordatorio: responde SOLO JSON sin texto extra. Ejemplo: "
        '{"enviar":true,"mensaje":"...","etapa":2}'
    )
    user_content = "\n".join(context_lines)

    expected_stage = int(metadata.get("intento_followup_siguiente", 1) or 1)
    previous_raw = ""
    for attempt in range(2):
        try:  # pragma: no cover - depende de red externa
            raw_text = _openai_generate_text(
                client,
                system_prompt=system_prompt,
                user_content=user_content,
                model=model,
                temperature=0.0,
                max_output_tokens=320,
            ).strip()
        except Exception as exc:
            logger.warning(
                "No se pudo evaluar el seguimiento con OpenAI: %s", exc, exc_info=False
            )
            return None
        if not raw_text:
            return None
        previous_raw = raw_text
        data: Optional[dict] = None
        try:
            data = json.loads(raw_text)
        except Exception:
            match = re.search(r"\{.*\}", raw_text, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(0))
                except Exception:
                    data = None
        if not isinstance(data, dict):
            user_content = (
                f"{user_content}\n\n"
                f"Tu salida anterior no fue JSON valido: {raw_text}\n"
                "Repite SOLO JSON valido."
            )
            continue

        enviar = data.get("enviar")
        if isinstance(enviar, str):
            enviar = enviar.strip().lower() in {"true", "1", "si", "si�", "yes"}
        if not enviar:
            return None

        message = _sanitize_generated_message(str(data.get("mensaje") or "").strip())
        message_issues = _generated_message_issues(message)
        if message_issues:
            user_content = (
                f"{user_content}\n\n"
                f"Tu salida anterior fue invalida: {raw_text}\n"
                f"Motivo: {','.join(message_issues)}\n"
                "Repite SOLO JSON valido con un mensaje util para enviar."
            )
            continue

        etapa_value = data.get("etapa")
        try:
            etapa_int = int(etapa_value)
        except Exception:
            etapa_int = expected_stage
        etapa_int = max(1, etapa_int)
        return message, etapa_int

    logger.info(
        "Followup decision descartada por formato/calidad. model=%s raw=%s",
        model,
        previous_raw[:280],
    )
    return None


_POSITIVE_KEYWORDS = (
    "si",
    "quiero saber mas",
    "me interesa",
    "interesado",
)
_NEGATIVE_KEYWORDS = (
    "no",
    "ya tengo",
    "no me interesa",
    "no gracias",
)
_INFO_KEYWORDS = (
    "info",
    "informacion",
    "informacia�n",
    "detalle",
    "detalles",
    "precio",
    "costo",
    "mas info",
    "ma�s info",
)
_CALL_KEYWORDS = (
    "agenda",
    "agendar",
    "llamar",
    "llamada",
    "cita",
    "call",
    "reunion",
    "reunia�n",
)
_DEFAULT_LEAD_TAG = "Lead sin clasificar"
_PROMPT_STOP_HINTS = (
    "no respondes mas",
    "no responder mas",
    "no respondas mas",
    "dejar de vender",
    "cerrar la conversacion",
    "cerrar conversacion",
    "no enviar mas mensajes",
    "no enviar mas followup",
    "no hace falta seguir respondiendo",
    "no hace falta seguir enviando followup",
)
_PROMPT_BOOKING_HINTS = (
    "agend",
    "calendly",
    "agenda",
)
_BOOKING_CONFIRM_HINTS = (
    "agende",
    "agende?",
    "agendado",
    "me agende",
    "me agende?",
    "ya agende",
    "ya agende?",
    "listo",
    "reserve",
    "reservado",
)


def _format_handle(value: str | None) -> str:
    if not value:
        return "@-"
    value = value.strip()
    if value.startswith("@"):
        return value
    return f"@{value}"


def _default_timezone_label() -> str:
    try:
        tz = datetime.now().astimezone().tzinfo
        if tz is None:
            return "UTC"
        key = getattr(tz, "key", None)
        if key:
            return str(key)
        zone = getattr(tz, "zone", None)
        if zone:
            return str(zone)
    except Exception:
        pass
    return "UTC"


def _safe_timezone(label: str):
    zoneinfo_class = _load_zoneinfo_class()
    if zoneinfo_class is not None:
        for candidate in (label, _default_timezone_label(), "UTC"):
            try:
                return zoneinfo_class(candidate)
            except Exception:
                continue
    if dateutil_tz is not None:  # pragma: no cover - depende de dateutil
        for candidate in (label, _default_timezone_label(), "UTC"):
            tzinfo = dateutil_tz.gettz(candidate)
            if tzinfo is not None:
                return tzinfo
    return timezone.utc


def _print_response_summary(
    index: int, sender: str, recipient: str, success: bool, extra: Optional[str] = None
) -> None:
    status = "OK" if success else "ERROR"
    color = Fore.GREEN if success else Fore.RED
    print(
        style_text(
            f"Respuesta {index} | {_format_handle(sender)} -> {_format_handle(recipient)} | {status}",
            color=color,
        )
    )
    if extra:
        print(style_text(extra, color=Fore.GREEN, bold=True))


@contextmanager
def _suppress_console_noise() -> None:
    root = logging.getLogger()
    stream_handlers: list[logging.Handler] = [
        handler
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
    ]
    original_levels = [handler.level for handler in stream_handlers]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            for handler in stream_handlers:
                handler.setLevel(logging.CRITICAL + 1)
            yield
        finally:
            for handler, level in zip(stream_handlers, original_levels):
                handler.setLevel(level)


def _normalize_text_for_match(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _safe_unread_count(thread: object) -> Optional[int]:
    unread = getattr(thread, "unread_count", None)
    if unread is None:
        return None
    try:
        return int(unread)
    except Exception:
        return None




def _same_user_id(a: object, b: object) -> bool:
    return str(a) == str(b)


def _message_timestamp(msg: object) -> Optional[float]:
    ts_obj = getattr(msg, "timestamp", None)
    if isinstance(ts_obj, datetime):
        return ts_obj.timestamp()
    try:
        return float(ts_obj)
    except Exception:
        return None


def _message_outbound_status(msg: object, client_user_id: object) -> Optional[bool]:
    direction = getattr(msg, "direction", None)
    if isinstance(direction, str):
        lowered = direction.strip().lower()
        if lowered in {"outbound", "outgoing", "sent", "from_me", "viewer"}:
            return True
        if lowered in {"inbound", "incoming", "received", "from_them", "from_lead"}:
            return False

    from_me = getattr(msg, "from_me", None)
    if isinstance(from_me, bool):
        return from_me

    is_outgoing = getattr(msg, "is_outgoing", None)
    if isinstance(is_outgoing, bool):
        return is_outgoing

    sender_id = getattr(msg, "user_id", None)
    if sender_id is None:
        return None
    return _same_user_id(sender_id, client_user_id)


def _message_id_for_compare(msg: object) -> str:
    raw_id = getattr(msg, "id", None)
    if raw_id is None:
        raw_id = getattr(msg, "message_id", None)
    if raw_id is None:
        return ""
    return str(raw_id)


def _message_index_in_batch(messages: List[object], msg: object) -> Optional[int]:
    for idx, candidate in enumerate(messages):
        if candidate is msg:
            return idx
    target_id = _message_id_for_compare(msg)
    if not target_id:
        return None
    for idx, candidate in enumerate(messages):
        if _message_id_for_compare(candidate) == target_id:
            return idx
    return None


def _message_is_newer_than(
    candidate: object,
    reference: object,
    messages: List[object],
) -> bool:
    candidate_ts = _message_timestamp(candidate)
    reference_ts = _message_timestamp(reference)

    if candidate_ts is not None and reference_ts is not None:
        if candidate_ts > reference_ts:
            return True
        if candidate_ts < reference_ts:
            return False
    elif candidate_ts is not None and reference_ts is None:
        return True
    elif candidate_ts is None and reference_ts is not None:
        return False

    candidate_idx = _message_index_in_batch(messages, candidate)
    reference_idx = _message_index_in_batch(messages, reference)
    if candidate_idx is None or reference_idx is None:
        return False
    # En batches ordenados por recencia (como get_messages), menor indice = mas nuevo.
    return candidate_idx < reference_idx


def _random_delay_seconds(delay_min: float, delay_max: float) -> float:
    try:
        min_value = float(delay_min)
    except Exception:
        min_value = 0.0
    try:
        max_value = float(delay_max)
    except Exception:
        max_value = min_value
    if max_value < min_value:
        max_value = min_value
    if max_value <= 0:
        return 0.0
    if min_value <= 0:
        min_value = 0.0
    return random.uniform(min_value, max_value)

def _sleep_between_replies_sync(delay_min: float, delay_max: float, label: str = 'reply_delay') -> None:
    delay = _random_delay_seconds(delay_min, delay_max)
    if delay <= 0:
        return
    logger.info('%s sleep=%.1fs', label, delay)
    sleep_with_stop(delay)


def _cycle_delay_bounds_from_message_delay(
    delay_min: float,
    delay_max: float,
) -> tuple[float, float]:
    try:
        message_min = max(0.0, float(delay_min or 0.0))
    except Exception:
        message_min = 0.0
    try:
        message_max = max(message_min, float(delay_max or message_min))
    except Exception:
        message_max = message_min

    # Pausa de ciclo derivada del delay configurado para evitar ritmo robótico.
    cycle_min = max(1.0, message_min * 0.2) if message_min > 0 else 1.0
    cycle_max = max(cycle_min, message_max * 0.5) if message_max > 0 else max(cycle_min, 3.0)
    return cycle_min, cycle_max


def _sleep_cycle_delay_from_message_delay(delay_min: float, delay_max: float) -> None:
    cycle_min, cycle_max = _cycle_delay_bounds_from_message_delay(delay_min, delay_max)
    delay = _random_delay_seconds(cycle_min, cycle_max)
    if delay <= 0:
        return
    logger.info("cycle_delay sleep=%.1fs", delay)
    print(
        style_text(
            f"⏳ Pausa de ciclo: {round(delay, 1)}s (basada en delay configurado)",
            color=Fore.WHITE,
        )
    )
    sleep_with_stop(delay)

def _parse_followup_schedule_hours(value: str, default: Optional[List[int]] = None) -> List[int]:
    raw = (value or "").strip()
    if not raw:
        return list(default or [])
    parts = [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]
    hours: List[int] = []
    for item in parts:
        try:
            num = int(float(item))
        except Exception:
            continue
        if num > 0:
            hours.append(num)
    if not hours:
        return list(default or [])
    return sorted(set(hours))

def _latest_inbound_message(messages: List[object], client_user_id: object) -> Optional[object]:
    candidates = [
        msg
        for msg in messages
        if _message_outbound_status(msg, client_user_id) is False
    ]
    if not candidates:
        return None
    scored = []
    for idx, msg in enumerate(candidates):
        scored.append((_message_timestamp(msg), -idx, msg))
    if any(score[0] is not None for score in scored):
        scored.sort(key=lambda item: ((item[0] is not None), item[0] or 0, item[1]))
        return scored[-1][2]
    return candidates[-1]


def _message_has_actionable_inbound_signal(msg: object, client_user_id: object) -> bool:
    if _message_outbound_status(msg, client_user_id) is not False:
        return False
    text_value = str(getattr(msg, "text", "") or "").strip()
    if text_value:
        return True
    sender_id = str(getattr(msg, "user_id", "") or "").strip()
    self_id = str(client_user_id or "").strip()
    if not sender_id:
        return False
    if self_id and sender_id == self_id:
        return False
    # "peer" se usa como fallback cuando no hay emisor real en payload.
    if sender_id == "peer":
        return False
    return True


def _latest_actionable_inbound_message(messages: List[object], client_user_id: object) -> Optional[object]:
    for msg in messages:
        if _message_has_actionable_inbound_signal(msg, client_user_id):
            return msg
    return None


def _latest_outbound_message(messages: List[object], client_user_id: object) -> Optional[object]:
    candidates = [
        msg
        for msg in messages
        if _message_outbound_status(msg, client_user_id) is True
    ]
    if not candidates:
        return None
    scored = []
    for idx, msg in enumerate(candidates):
        scored.append((_message_timestamp(msg), -idx, msg))
    if any(score[0] is not None for score in scored):
        scored.sort(key=lambda item: ((item[0] is not None), item[0] or 0, item[1]))
        return scored[-1][2]
    return candidates[-1]


def _latest_message(messages: List[object]) -> Optional[object]:
    if not messages:
        return None
    scored = []
    for idx, msg in enumerate(messages):
        scored.append((_message_timestamp(msg), -idx, msg))
    if any(score[0] is not None for score in scored):
        scored.sort(key=lambda item: ((item[0] is not None), item[0] or 0, item[1]))
        return scored[-1][2]
    return messages[-1]


def _prompt_requires_stop_after_checkpoint(prompt_text: str) -> bool:
    normalized = _normalize_text_for_match(prompt_text or "")
    if not normalized:
        return False
    return any(hint in normalized for hint in _PROMPT_STOP_HINTS)


def _prompt_mentions_booking_checkpoint(prompt_text: str) -> bool:
    normalized = _normalize_text_for_match(prompt_text or "")
    if not normalized:
        return False
    return any(hint in normalized for hint in _PROMPT_BOOKING_HINTS)


def _message_text_normalized(msg: object) -> str:
    return _normalize_text_for_match(str(getattr(msg, "text", "") or ""))


def _message_is_booking_sent_by_bot(msg: object, client_user_id: object) -> bool:
    if _message_outbound_status(msg, client_user_id) is not True:
        return False
    text = _message_text_normalized(msg)
    if not text:
        return False
    return ("calendly" in text) or ("agend" in text) or ("agenda" in text)


def _message_confirms_booking(msg: object, client_user_id: object) -> bool:
    if _message_outbound_status(msg, client_user_id) is not False:
        return False
    text = _message_text_normalized(msg)
    if not text:
        return False
    return any(_contains_token(text, token) for token in _BOOKING_CONFIRM_HINTS)


def _prompt_sequence_done_by_messages(
    prompt_text: str,
    messages: List[object],
    *,
    client_user_id: object,
) -> bool:
    if not _prompt_requires_stop_after_checkpoint(prompt_text):
        return False
    if not messages:
        return False
    if not _prompt_mentions_booking_checkpoint(prompt_text):
        return False

    last_booking_outbound: Optional[object] = None
    for msg in messages:
        if _message_is_booking_sent_by_bot(msg, client_user_id):
            last_booking_outbound = msg
            break
    if last_booking_outbound is None:
        return False

    booking_outbound_ts = _message_timestamp(last_booking_outbound)
    for msg in messages:
        if not _message_confirms_booking(msg, client_user_id):
            continue
        inbound_ts = _message_timestamp(msg)
        if booking_outbound_ts is None or inbound_ts is None or inbound_ts >= booking_outbound_ts:
            return True
    return False


def _contains_token(text: str, token: str) -> bool:
    token = token.strip()
    if not token:
        return False
    if " " in token:
        return token in text
    return (
        text == token
        or text.startswith(token + " ")
        or text.endswith(" " + token)
        or f" {token} " in text
    )


def _classify_response(message: str) -> str | None:
    norm = _normalize_text_for_match(message)
    if not norm:
        return None
    for keyword in _POSITIVE_KEYWORDS:
        if _contains_token(norm, keyword):
            return "Interesado"
    for keyword in _NEGATIVE_KEYWORDS:
        if _contains_token(norm, keyword):
            return "No interesado"
    return None


def _resolve_username(client, thread, target_user_id: str) -> str:
    target = str(target_user_id) if target_user_id is not None else ""
    client_id = str(getattr(client, "user_id", "") or "")
    try:
        participants = getattr(thread, "users", []) or []
        if participants:
            for participant in participants:
                pk = getattr(participant, "pk", None) or getattr(participant, "id", None)
                pk_str = str(pk) if pk is not None else ""
                if pk_str and pk_str == target:
                    username = getattr(participant, "username", None)
                    if username:
                        return username
            for participant in participants:
                pk = getattr(participant, "pk", None) or getattr(participant, "id", None)
                pk_str = str(pk) if pk is not None else ""
                username = getattr(participant, "username", None)
                if username and pk_str and pk_str != client_id:
                    return username
            for participant in participants:
                username = getattr(participant, "username", None)
                if username:
                    return username
    except Exception:
        pass
    title = getattr(thread, "title", None)
    if title:
        return str(title)
    return "unknown"


@dataclass
class BotStats:
    alias: str
    responded: int = 0
    followups: int = 0
    errors: int = 0
    responses: int = 0
    reply_attempts: int = 0
    followup_attempts: int = 0
    accounts: set[str] = field(default_factory=set)
    started_at: float = field(default_factory=time.time)
    account_started_at: Dict[str, float] = field(default_factory=dict)
    account_elapsed_s: Dict[str, float] = field(default_factory=dict)

    def _bump_responses(self, account: str) -> int:
        self.responses += 1
        self.accounts.add(account)
        return self.responses

    def mark_account_start(self, account: str) -> None:
        if not account:
            return
        self.accounts.add(account)
        self.account_started_at.setdefault(account, time.time())

    def mark_account_end(self, account: str) -> None:
        if not account:
            return
        self.accounts.add(account)
        start_ts = self.account_started_at.pop(account, None)
        if start_ts is None:
            return
        elapsed = max(0.0, time.time() - float(start_ts))
        self.account_elapsed_s[account] = self.account_elapsed_s.get(account, 0.0) + elapsed

    def record_reply_attempt(self, account: str) -> None:
        self.reply_attempts += 1
        self.accounts.add(account)

    def record_success(self, account: str) -> int:
        index = self._bump_responses(account)
        self.responded += 1
        return index

    def record_followup_attempt(self, account: str) -> None:
        self.followup_attempts += 1
        self.accounts.add(account)

    def record_followup_success(self, account: str) -> None:
        self.followups += 1
        self.accounts.add(account)

    def record_response_error(self, account: str) -> int:
        index = self._bump_responses(account)
        self.errors += 1
        return index

    def record_error(self, account: str) -> None:
        self.errors += 1
        self.accounts.add(account)


def _playwright_storage_state_path(username: str) -> Path:
    return PlaywrightDMClient.storage_state_path(username)


def _proxy_payload_for_playwright(account: Optional[Dict]) -> Optional[Dict[str, str]]:
    try:
        from src.proxy_payload import proxy_from_account
    except Exception:
        return None
    return proxy_from_account(account)


def _has_playwright_session(username: str, *, account: Optional[Dict] = None) -> bool:
    if not username:
        return False
    proxy = _proxy_payload_for_playwright(account)
    try:
        ok, reason = check_session(username, proxy=proxy, headless=True)
    except Exception as exc:
        logger.warning("Playwright session check failed for @%s: %s", username, exc)
        return False
    logger.info("Playwright session check for @%s: %s (%s)", username, ok, reason)
    return bool(ok)


def _prompt_playwright_login(username: str, *, alias: Optional[str] = None) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False

    stored_password = _account_password(account).strip()
    if not stored_password:
        stored_password = getpass.getpass(f"Password @{username}: ")
    if not stored_password:
        warn("Se cancelo el inicio de sesion.")
        return False

    payload = dict(account)
    payload["username"] = username
    payload["password"] = stored_password
    try:
        result = login_account_playwright(payload, alias or username, headful=True)
    except Exception as exc:
        warn(f"No se pudo iniciar login con Playwright: {exc}")
        return False

    ok = (result.get("status") == "ok")
    if ok:
        try:
            _store_account_password(username, stored_password)
        except Exception:
            pass
    return ok


def _client_for(username: str):
    account = get_account(username)
    if not account:
        raise RuntimeError(f"No se encontro la cuenta {username}.")
    logger.info("autoresponder_dm_engine=playwright account=@%s", username)
    headless_raw = str(os.getenv("AUTORESPONDER_DM_HEADLESS", "1")).strip().lower()
    headless_mode = headless_raw not in {"0", "false", "no", "n", "off"}
    try:
        slow_mo_ms = max(0, int(float(os.getenv("AUTORESPONDER_DM_SLOW_MO_MS", "0"))))
    except Exception:
        slow_mo_ms = 0
    logger.info(
        "autoresponder_dm_client account=@%s headless=%s slow_mo_ms=%s",
        username,
        headless_mode,
        slow_mo_ms,
    )
    client = PlaywrightDMClient(account=account, headless=headless_mode, slow_mo_ms=slow_mo_ms)
    try:
        client.ensure_ready()
    except Exception:
        try:
            if not client.headless and max(0, int(float(os.getenv("AUTORESPONDER_KEEP_BROWSER_OPEN_SECONDS", "0")))) > 0:
                print(style_text(f"[Debug] Navegador de @{username} queda abierto para inspección (fallo ensure_ready).", color=Fore.YELLOW))
                time.sleep(max(0, int(float(os.getenv("AUTORESPONDER_KEEP_BROWSER_OPEN_SECONDS", "0")))))
            client.close()
        except Exception:
            pass
        mark_connected(username, False)
        raise
    mark_connected(username, True)
    return client


def _ensure_session(username: str) -> bool:
    try:
        account = get_account(username)
        return _has_playwright_session(username, account=account)
    except Exception:
        return False


def _gen_response_legacy(api_key: str, system_prompt: str, convo_text: str) -> str:
    return _OPENAI_REPLY_FALLBACK
    try:
        client = _build_openai_client(api_key)
        model = _resolve_ai_model(api_key)
        output = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=convo_text,
            model=model,
            temperature=0.6,
            max_output_tokens=180,
        )
        return (msg.output_text or "").strip() or "Gracias por tu mensaje ���� -aCa�mo te puedo ayudar?"
    except Exception as e:  # pragma: no cover - depende de red externa
        logger.warning("Fallo al generar respuesta con OpenAI: %s", e, exc_info=False)
        return "Gracias por tu mensaje ���� -aCa�mo te puedo ayudar?"


def _gen_response(
    api_key: str,
    system_prompt: str,
    convo_text: str,
    *,
    memory_context: str = "",
) -> str:
    def _is_openai_auth_error(exc: Exception) -> bool:
        status_raw = getattr(exc, "status_code", None)
        try:
            status_code = int(status_raw)
        except Exception:
            status_code = 0
        if status_code in {401, 403}:
            return True
        if status_code == 404:
            text_404 = _normalize_text_for_match(str(exc))
            return "model" in text_404 and "not found" in text_404
        text = _normalize_text_for_match(str(exc))
        auth_tokens = (
            "user not found",
            "invalid api key",
            "incorrect api key",
            "authentication",
            "unauthorized",
            "forbidden",
        )
        return any(token in text for token in auth_tokens)

    try:
        client = _build_openai_client(api_key)
        model = _resolve_ai_model(api_key)
        strict_system_prompt = _build_strict_responder_system_prompt(system_prompt)
        base_user_content = _build_responder_user_content(convo_text, memory_context=memory_context)
        previous_raw_output = ""
        previous_issues: List[str] = []
        for attempt in range(2):
            user_content = base_user_content
            if attempt > 0:
                issue_hint = ", ".join(previous_issues) if previous_issues else "no_cumple_formato"
                user_content = (
                    f"{base_user_content}\n\n"
                    "Tu salida anterior no cumplio las reglas.\n"
                    f"Errores detectados: {issue_hint}\n"
                    f"Salida previa: {previous_raw_output or '(vacia)'}\n"
                    "Reescribila cumpliendo estrictamente las reglas."
                )
            raw_output = _openai_generate_text(
                client,
                system_prompt=strict_system_prompt,
                user_content=user_content,
                model=model,
                temperature=0.2 if attempt == 0 else 0.0,
                max_output_tokens=260,
            )
            candidate = _sanitize_generated_message(raw_output)
            issues = _generated_message_issues(candidate)
            if not issues:
                return candidate
            previous_raw_output = str(raw_output or "").strip()
            previous_issues = issues
            logger.info(
                "Responder output descartado intento=%s motivo=%s model=%s",
                attempt + 1,
                ",".join(issues),
                model,
            )
        if previous_raw_output:
            repaired = _sanitize_generated_message(previous_raw_output)
            if repaired and not _generated_message_issues(repaired):
                return repaired
        logger.warning("Salida IA invalida para respuesta; se omite envio para evitar mensaje fuera de prompt.")
        return ""
    except Exception as e:  # pragma: no cover - depende de red externa
        status_code = getattr(e, "status_code", None)
        logger.warning("Fallo al generar respuesta con OpenAI: %s", e, exc_info=False)
        if _is_openai_auth_error(e):
            warn(f"[OPENAI ERROR] auth/config error={e} status={status_code}; se omite envio.")
        else:
            warn(f"[OPENAI ERROR] error={e} status={status_code}; se omite envio.")
        return ""


def _choose_targets(alias: str) -> list[str]:
    accounts_data = list_all()
    alias_key = alias.lstrip("@")
    alias_lower = alias_key.lower()

    if alias.upper() == "ALL":
        candidates = [a["username"] for a in accounts_data if a.get("active")]
    else:
        alias_matches = [
            a for a in accounts_data if a.get("alias", "").lower() == alias_lower and a.get("active")
        ]
        if alias_matches:
            candidates = [a["username"] for a in alias_matches]
        else:
            username_matches = [
                a for a in accounts_data if a.get("username", "").lower() == alias_lower and a.get("active")
            ]
            if username_matches:
                candidates = [username_matches[0]["username"]]
            else:
                candidates = [alias_key]

    seen = set()
    deduped: list[str] = []
    for user in candidates:
        norm = user.lstrip("@")
        if norm not in seen:
            seen.add(norm)
            deduped.append(norm)
    return deduped


def _filter_valid_sessions(targets: list[str], *, alias: Optional[str] = None) -> list[str]:
    verified: list[str] = []
    needing_login: list[tuple[str, str]] = []
    for user in targets:
        account = get_account(user)
        if not account:
            needing_login.append((user, "cuenta no encontrada"))
            continue
        if not _playwright_storage_state_path(user).exists():
            needing_login.append((user, "sin sesion guardada"))
            continue
        if not _ensure_session(user):
            needing_login.append((user, "sesion expirada"))
            continue
        verified.append(user)

    if needing_login:
        remaining: list[tuple[str, str]] = []
        for user, reason in needing_login:
            remaining.append((user, reason))

        if remaining:
            print("\nLas siguientes cuentas necesitan volver a iniciar sesion:")
            for user, reason in remaining:
                print(f" - @{user}: {reason}")
            if ask("-aIniciar sesion ahora? (s/N): ").strip().lower() == "s":
                for user, _ in remaining:
                    if _prompt_playwright_login(user, alias=alias) and _ensure_session(user):
                        if user not in verified:
                            verified.append(user)
            else:
                warn("Se omitieron las cuentas sin sesion valida.")
    return verified



def _mask_key(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if len(value) <= 6:
        return value[:2] + "�Ǫ"
    return f"{value[:4]}�Ǫ{value[-2:]}"


def _system_prompt_file(alias: str | None = None) -> Path:
    alias_key = (alias or _PROMPT_DEFAULT_ALIAS).strip() or _PROMPT_DEFAULT_ALIAS
    safe_alias = re.sub(r"[^a-z0-9_.-]", "_", alias_key.lower())
    return _PROMPT_STORAGE_DIR / safe_alias / "system_prompt.txt"


def _normalize_system_prompt_text(value: str) -> str:
    if not value:
        return ""
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _read_system_prompt_from_file(alias: str | None = None) -> str | None:
    path = _system_prompt_file(alias)
    if not path.exists():
        return None
    try:
        return _normalize_system_prompt_text(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("No se pudo leer %s: %s", path, exc, exc_info=False)
        return None


def _persist_system_prompt(prompt: str, alias: str | None = None) -> str:
    normalized = _normalize_system_prompt_text(prompt)
    path = _system_prompt_file(alias)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized, encoding="utf-8")
    except Exception as exc:
        logger.warning("No se pudo escribir %s: %s", path, exc, exc_info=False)
    try:
        update_app_config({PROMPT_KEY: normalized})
    except Exception as exc:
        logger.warning("No se pudo actualizar el system prompt en config: %s", exc, exc_info=False)
    return normalized


def _load_preferences(alias: str | None = None) -> tuple[str, str]:
    env_values = read_env_local()
    api_key = _resolve_ai_api_key(env_values)
    config_values = read_app_config()
    prompt = ""
    alias_candidates: List[str] = []
    if alias:
        alias_candidates.append(alias)
    alias_candidates.append(_PROMPT_DEFAULT_ALIAS)

    seen_aliases: set[str] = set()
    for candidate in alias_candidates:
        norm_candidate = str(candidate or "").strip().lower()
        if not norm_candidate or norm_candidate in seen_aliases:
            continue
        seen_aliases.add(norm_candidate)
        file_prompt = _read_system_prompt_from_file(candidate)
        if file_prompt and file_prompt.strip():
            prompt = file_prompt
            break

    if not prompt:
        prompt = config_values.get(PROMPT_KEY, "") or ""
    prompt = _normalize_system_prompt_text(prompt) or DEFAULT_PROMPT
    return api_key, prompt


def _resolve_system_prompt_for_user(
    username: str,
    *,
    active_alias: str | None = None,
    fallback_prompt: str = DEFAULT_PROMPT,
) -> str:
    account_data = get_account(username) or {}
    account_alias = str(account_data.get("alias") or "").strip()
    alias_candidates: List[str] = [username]
    if account_alias:
        alias_candidates.append(account_alias)
    if active_alias:
        alias_candidates.append(active_alias)
    alias_candidates.append(_PROMPT_DEFAULT_ALIAS)

    seen_aliases: set[str] = set()
    for candidate in alias_candidates:
        norm_candidate = str(candidate or "").strip().lower()
        if not norm_candidate or norm_candidate in seen_aliases:
            continue
        seen_aliases.add(norm_candidate)
        prompt = _read_system_prompt_from_file(candidate)
        if prompt and prompt.strip():
            return _normalize_system_prompt_text(prompt)

    return _normalize_system_prompt_text(fallback_prompt) or DEFAULT_PROMPT


def _read_state_json(path: Path, default: Dict[str, dict]) -> Dict[str, dict]:
    data: Dict[str, dict] = dict(default)
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data.update(loaded)
        except Exception:
            data = dict(default)
    return data


def _write_state_json(path: Path, data: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _ensure_alias_container(data: Dict[str, dict]) -> Dict[str, dict]:
    if "aliases" not in data or not isinstance(data["aliases"], dict):
        data["aliases"] = {}
    return data


def _read_gohighlevel_state(refresh: bool = False) -> Dict[str, dict]:
    global _GOHIGHLEVEL_STATE
    if refresh or _GOHIGHLEVEL_STATE is None:
        data = _read_state_json(_GOHIGHLEVEL_FILE, {"aliases": {}})
        _GOHIGHLEVEL_STATE = _ensure_alias_container(data)
    return _GOHIGHLEVEL_STATE


def _write_gohighlevel_state(state: Dict[str, dict]) -> None:
    state.setdefault("aliases", {})
    _write_state_json(_GOHIGHLEVEL_FILE, state)
    _read_gohighlevel_state(refresh=True)


def _read_google_calendar_state(refresh: bool = False) -> Dict[str, dict]:
    global _GOOGLE_STATE
    if refresh or _GOOGLE_STATE is None:
        data = _read_state_json(_GOOGLE_CALENDAR_FILE, {"aliases": {}})
        _GOOGLE_STATE = _ensure_alias_container(data)
    return _GOOGLE_STATE


def _write_google_calendar_state(state: Dict[str, dict]) -> None:
    state.setdefault("aliases", {})
    _write_state_json(_GOOGLE_CALENDAR_FILE, state)
    _read_google_calendar_state(refresh=True)


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _normalize_alias_key(alias: str) -> str:
    return _normalize_key(alias)


def _normalize_lead_id(lead: str) -> str:
    return _normalize_key(lead)


def _sanitize_location_ids(raw: object) -> List[str]:
    if raw is None:
        return []
    tokens: List[str] = []
    if isinstance(raw, str):
        parts = re.split(r"[\s,;]a", raw)
        tokens = [part.strip() for part in parts if part.strip()]
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if isinstance(item, str):
                value = item.strip()
                if value:
                    tokens.append(value)
    else:
        try:
            iterable = list(raw)  # type: ignore[arg-type]
        except Exception:
            iterable = []
        for item in iterable:
            if isinstance(item, str):
                value = item.strip()
                if value:
                    tokens.append(value)
    seen: set[str] = set()
    cleaned: List[str] = []
    for token in tokens:
        norm = token.strip()
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        cleaned.append(norm)
    return cleaned


def _get_gohighlevel_entry(alias: str) -> Dict[str, dict]:
    state = _read_gohighlevel_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if isinstance(entry, dict):
        entry.setdefault("alias", alias.strip())
        entry.setdefault("sent", {})
        entry.setdefault("qualify_prompt", _DEFAULT_GOHIGHLEVEL_PROMPT)
        if "location_ids" in entry:
            entry["location_ids"] = _sanitize_location_ids(entry.get("location_ids"))
        return entry
    return {}


def _set_gohighlevel_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias inva�lido.")
        return
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
    entry.setdefault("sent", {})
    normalized_updates: Dict[str, object] = {}
    for key, value in updates.items():
        if value is None:
            continue
        if key == "location_ids":
            normalized_updates[key] = _sanitize_location_ids(value)
        else:
            normalized_updates[key] = value
    entry.update(normalized_updates)
    aliases[key] = entry
    _write_gohighlevel_state(state)


def _get_google_calendar_entry(alias: str) -> Dict[str, object]:
    state = _read_google_calendar_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if isinstance(entry, dict):
        entry.setdefault("alias", alias.strip())
        entry.setdefault("scheduled", {})
        entry.setdefault("event_name", "{{username}} - Sistema de adquisicia�n con IA")
        entry.setdefault("duration_minutes", 30)
        entry.setdefault("timezone", _default_timezone_label())
        entry.setdefault("auto_meet", True)
        entry.setdefault("schedule_prompt", _DEFAULT_GOOGLE_CALENDAR_PROMPT)
        return entry
    return {}


def _set_google_calendar_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias inva�lido.")
        return
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
    entry.setdefault("scheduled", {})
    entry.setdefault("event_name", "{{username}} - Sistema de adquisicia�n con IA")
    entry.setdefault("duration_minutes", 30)
    entry.setdefault("timezone", _default_timezone_label())
    entry.setdefault("auto_meet", True)
    entry.setdefault("schedule_prompt", _DEFAULT_GOOGLE_CALENDAR_PROMPT)
    normalized_updates: Dict[str, object] = {}
    for key_name, value in updates.items():
        if value is None:
            continue
        if key_name == "duration_minutes":
            try:
                normalized_updates[key_name] = max(5, int(value))
            except Exception:
                continue
        elif key_name == "timezone":
            try:
                tz_value = str(value).strip() or _default_timezone_label()
                _ = _safe_timezone(tz_value)
                normalized_updates[key_name] = tz_value
            except Exception:
                warn("Zona horaria inva�lida; se mantiene el valor previo.")
                continue
        elif key_name == "schedule_prompt":
            normalized_updates[key_name] = str(value)
        else:
            normalized_updates[key_name] = value
    entry.update(normalized_updates)
    aliases[key] = entry
    _write_google_calendar_state(state)


def _mask_google_calendar_status(entry: Dict[str, object]) -> str:
    connected = bool(entry.get("connected"))
    enabled = bool(entry.get("enabled"))
    status = "���� Activo" if connected and enabled else "���� Conectado" if connected else "�ܬ Inactivo"
    summary = entry.get("event_name") or "(sin nombre)"
    tz_label = entry.get("timezone") or "UTC"
    return f"{status} ��� Evento: {summary} ��� TZ: {tz_label}"


def _google_calendar_status_lines() -> List[str]:
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return ["(sin configuraciones)"]
    rows: List[str] = []
    for key in sorted(aliases.keys()):
        entry = aliases[key]
        label = str(entry.get("alias") or key)
        rows.append(f" - {label}: {_mask_google_calendar_status(entry)}")
    return rows


def _google_calendar_summary_line() -> str:
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    enabled_aliases = [
        entry
        for entry in aliases.values()
        if isinstance(entry, dict) and entry.get("connected") and entry.get("enabled")
    ]
    if not enabled_aliases:
        configured_aliases = [
            entry
            for entry in aliases.values()
            if isinstance(entry, dict) and entry.get("connected")
        ]
        if configured_aliases:
            labels = sorted(str(entry.get("alias") or "?") for entry in configured_aliases)
            return f"Google Calendar: conectado para {', '.join(labels)} (inactivo)"
        return "Google Calendar: (sin configurar)"
    labels = sorted(str(entry.get("alias") or "?") for entry in enabled_aliases)
    return f"Google Calendar: activo para {', '.join(labels)}"


def _google_calendar_candidate_keys(lead: str, phone: str) -> list[str]:
    normalized_lead = _normalize_lead_id(lead)
    normalized_phone = _normalize_phone(phone)
    keys = [f"{normalized_lead}|{normalized_phone}"]
    if normalized_lead:
        keys.append(f"{normalized_lead}|")
    if normalized_phone:
        keys.append(f"|{normalized_phone}")
    keys.append("||")
    seen: set[str] = set()
    ordered: list[str] = []
    for key in keys:
        if key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


def _google_calendar_share_link_from_event_id(event_id: str) -> str:
    event_id = str(event_id or "").strip()
    if not event_id:
        return ""
    if "@" in event_id:
        event_id = event_id.split("@", 1)[0]
    sanitized = re.sub(r"[^A-Za-z0-9_\-]", "", event_id)
    if not sanitized:
        return ""
    return f"https://calendar.app.google/{sanitized}"


def _google_calendar_preferred_link(link: Optional[str]) -> str:
    if not link:
        return ""
    try:
        parsed = urlparse(str(link))
    except Exception:
        return str(link)
    host = parsed.netloc.lower()
    if host.startswith("calendar.app.google"):
        return str(link)
    if "google.com" not in host:
        return str(link)
    query = parse_qs(parsed.query)
    eid_values = query.get("eid") or []
    eid = next((value for value in eid_values if value), "")
    if not eid:
        return str(link)
    eid = eid.strip()
    if not eid:
        return str(link)
    padding = "=" * (-len(eid) % 4)
    try:
        decoded = base64.urlsafe_b64decode((eid + padding).encode("ascii", "ignore"))
    except Exception:
        return str(link)
    try:
        decoded_text = decoded.decode("utf-8", "ignore").strip()
    except Exception:
        decoded_text = ""
    if not decoded_text:
        return str(link)
    event_id = decoded_text.split()[0].strip()
    if not event_id:
        return str(link)
    if any(ch for ch in event_id if ord(ch) < 33):
        return str(link)
    share_link = _google_calendar_share_link_from_event_id(event_id)
    if share_link:
        return share_link
    return str(link)


def _google_calendar_mark_scheduled(
    alias: str,
    lead: str,
    phone: str,
    event_id: str,
    link: str | None,
    start_iso: str,
) -> None:
    entry = _get_google_calendar_entry(alias)
    scheduled = entry.setdefault("scheduled", {})
    candidate_keys = _google_calendar_candidate_keys(lead, phone)
    canonical_key = candidate_keys[0]
    existing_key = None
    for key in candidate_keys:
        if key in scheduled:
            existing_key = key
            break
    if existing_key and existing_key != canonical_key:
        scheduled.pop(existing_key, None)
    scheduled[canonical_key] = {
        "event_id": event_id,
        "link": _google_calendar_preferred_link(link) if link else "",
        "start": start_iso,
        "ts": int(time.time()),
    }
    _set_google_calendar_entry(alias, {"scheduled": scheduled})


def _google_calendar_already_scheduled(alias: str, lead: str, phone: str) -> bool:
    entry = _get_google_calendar_entry(alias)
    scheduled = entry.get("scheduled") or {}
    for key in _google_calendar_candidate_keys(lead, phone):
        if key in scheduled:
            return True
    return False


def _google_calendar_get_scheduled(
    alias: str, lead: str, phone: str
) -> tuple[Optional[Dict[str, object]], Optional[str]]:
    entry = _get_google_calendar_entry(alias)
    scheduled = entry.get("scheduled") or {}
    for key in _google_calendar_candidate_keys(lead, phone):
        data = scheduled.get(key)
        if isinstance(data, dict):
            return data, key
    return None, None


def _google_calendar_token_is_valid(entry: Dict[str, object]) -> bool:
    expires_at = entry.get("token_expires_at")
    try:
        expires_float = float(expires_at)
    except Exception:
        return False
    return expires_float - time.time() > 60


def _google_calendar_store_tokens(
    alias: str, entry: Dict[str, object], token_data: Dict[str, object]
) -> Dict[str, object]:
    access_token = token_data.get("access_token") or entry.get("access_token")
    refresh_token = token_data.get("refresh_token") or entry.get("refresh_token")
    token_type = token_data.get("token_type") or entry.get("token_type")
    expires_in = token_data.get("expires_in")
    if isinstance(expires_in, str) and expires_in.isdigit():
        expires_in = int(expires_in)
    if not isinstance(expires_in, (int, float)):
        expires_in = 3600
    expires_at = time.time() + float(expires_in)
    updated = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
        "token_expires_at": expires_at,
        "connected": bool(access_token and refresh_token),
    }
    _set_google_calendar_entry(alias, updated)
    entry.update(updated)
    return entry


def _google_calendar_refresh_access_token(
    alias: str, entry: Dict[str, object]
) -> Optional[str]:
    if requests is None and (Credentials is None or build is None):
        return None
    refresh_token = entry.get("refresh_token")
    client_id = entry.get("client_id")
    client_secret = entry.get("client_secret")
    if not refresh_token or not client_id:
        return None
    data = {
        "client_id": client_id,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    if client_secret:
        data["client_secret"] = client_secret
    try:
        response = requests.post(_GOOGLE_TOKEN_URL, data=data, timeout=15)
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning("No se pudo refrescar el token de Google Calendar: %s", exc, exc_info=False)
        return None
    if response.status_code != 200:
        logger.warning(
            "Respuesta inesperada al refrescar token de Google Calendar: %s", response.text
        )
        return None
    token_data = response.json()
    entry = _google_calendar_store_tokens(alias, entry, token_data)
    return entry.get("access_token")


def _google_calendar_update_tokens_from_credentials(
    alias: str, entry: Dict[str, object], creds: object
) -> None:
    token = getattr(creds, "token", None)
    if not token:
        return
    refresh_token = getattr(creds, "refresh_token", None) or entry.get("refresh_token")
    expiry = getattr(creds, "expiry", None)
    expires_in = 3600
    if expiry is not None:
        try:
            expiry_dt = expiry
            if isinstance(expiry_dt, str):
                parsed = _safe_parse_datetime(expiry_dt)
                if parsed is not None:
                    expiry_dt = parsed
            if isinstance(expiry_dt, datetime):
                if expiry_dt.tzinfo is None:
                    expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
                delta = expiry_dt - datetime.now(timezone.utc)
                expires_in = max(60, int(delta.total_seconds()))
        except Exception:
            expires_in = 3600
    token_payload = {
        "access_token": token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }
    _google_calendar_store_tokens(alias, entry, token_payload)


def _google_calendar_credentials_from_entry(
    alias: str, entry: Dict[str, object]
) -> Optional[object]:
    if Credentials is None:
        return None
    access_token = entry.get("access_token")
    refresh_token = entry.get("refresh_token")
    client_id = entry.get("client_id")
    if not access_token or not refresh_token or not client_id:
        return None
    try:
        creds = Credentials(
            token=str(access_token),
            refresh_token=str(refresh_token),
            token_uri=_GOOGLE_TOKEN_URL,
            client_id=str(client_id),
            client_secret=str(entry.get("client_secret") or "") or None,
            scopes=[_GOOGLE_SCOPE],
        )
    except Exception as exc:  # pragma: no cover - depende de librera�as externas
        logger.warning(
            "No se pudieron preparar credenciales de Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    if not getattr(creds, "valid", False) and getattr(creds, "refresh_token", None):
        if GoogleAuthRequest is None:
            return creds
        try:
            creds.refresh(GoogleAuthRequest())  # type: ignore[misc]
            _google_calendar_update_tokens_from_credentials(alias, entry, creds)
        except Exception as exc:  # pragma: no cover - depende de red/creds
            logger.warning(
                "No se pudo refrescar credenciales de Google Calendar via google-auth: %s",
                exc,
                exc_info=False,
            )
            return None
    return creds


def _google_calendar_create_event_via_service(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
) -> Optional[Dict[str, object]]:
    if build is None or Credentials is None:
        return None
    creds = _google_calendar_credentials_from_entry(alias, entry)
    if not creds:
        return None
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "No se pudo inicializar el cliente de Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    kwargs: Dict[str, object] = {}
    if "conferenceData" in payload:
        kwargs["conferenceDataVersion"] = 1
    try:
        event = (
            service.events()  # type: ignore[call-arg]
            .insert(calendarId="primary", body=payload, **kwargs)
            .execute()
        )
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "Error al crear evento de Google Calendar mediante googleapiclient: %s",
            exc,
            exc_info=False,
        )
        return None
    _google_calendar_update_tokens_from_credentials(alias, entry, creds)
    if isinstance(event, dict):
        return event
    return None


def _google_calendar_create_event_via_requests(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    if requests is None and (Credentials is None or build is None):
        return None
    token_value = access_token or entry.get("access_token")
    if not token_value:
        return None
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Content-Type": "application/json",
    }
    url = f"{_GOOGLE_CALENDAR_BASE}/calendars/primary/events"
    try:
        response = requests.post(  # type: ignore[call-arg]
            url,
            headers=headers,
            json=payload,
            params=params or None,
            timeout=20,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning("No se pudo crear el evento en Google Calendar: %s", exc, exc_info=False)
        return None
    if response.status_code == 401:
        new_token = _google_calendar_refresh_access_token(alias, entry)
        if not new_token:
            return None
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            response = requests.post(  # type: ignore[call-arg]
                url,
                headers=headers,
                json=payload,
                params=params or None,
                timeout=20,
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "No se pudo crear el evento en Google Calendar tras refrescar token: %s",
                exc,
                exc_info=False,
            )
            return None
    if response.status_code not in {200, 201}:
        logger.warning(
            "Respuesta inesperada al crear evento de Google Calendar (%s): %s",
            response.status_code,
            response.text,
        )
        return None
    try:
        data = response.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        return data
    return None


def _google_calendar_fetch_event_via_service(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
) -> Optional[Dict[str, object]]:
    if build is None or Credentials is None:
        return None
    creds = _google_calendar_credentials_from_entry(alias, entry)
    if not creds:
        return None
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "No se pudo inicializar el cliente de Google Calendar para leer evento: %s",
            exc,
            exc_info=False,
        )
        return None
    try:
        event = (
            service.events()  # type: ignore[call-arg]
            .get(calendarId="primary", eventId=event_id)
            .execute()
        )
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "Error al obtener evento de Google Calendar mediante googleapiclient: %s",
            exc,
            exc_info=False,
        )
        return None
    _google_calendar_update_tokens_from_credentials(alias, entry, creds)
    if isinstance(event, dict):
        return event
    return None


def _google_calendar_fetch_event_via_requests(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    if requests is None and (Credentials is None or build is None):
        return None
    token_value = access_token or entry.get("access_token")
    if not token_value:
        return None
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Content-Type": "application/json",
    }
    params = {"fields": "id,htmlLink,hangoutLink,conferenceData,start"}
    url = f"{_GOOGLE_CALENDAR_BASE}/calendars/primary/events/{event_id}"
    try:
        response = requests.get(  # type: ignore[call-arg]
            url,
            headers=headers,
            params=params,
            timeout=20,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo leer el evento en Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    if response.status_code == 401:
        new_token = _google_calendar_refresh_access_token(alias, entry)
        if not new_token:
            return None
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            response = requests.get(  # type: ignore[call-arg]
                url,
                headers=headers,
                params=params,
                timeout=20,
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "No se pudo leer el evento en Google Calendar tras refrescar token: %s",
                exc,
                exc_info=False,
            )
            return None
    if response.status_code != 200:
        logger.warning(
            "Respuesta inesperada al obtener evento de Google Calendar (%s): %s",
            response.status_code,
            response.text,
        )
        return None
    try:
        data = response.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        return data
    return None


def _google_calendar_fetch_event(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    event = _google_calendar_fetch_event_via_service(alias, entry, event_id)
    if event:
        return event
    return _google_calendar_fetch_event_via_requests(alias, entry, event_id, access_token)


def _google_calendar_create_event(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    event = _google_calendar_create_event_via_service(alias, entry, payload)
    if event:
        return event
    return _google_calendar_create_event_via_requests(alias, entry, payload, params, access_token)


def _google_calendar_update_event_via_service(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
    payload: Dict[str, object],
    params: Dict[str, object],
) -> Optional[Dict[str, object]]:
    if build is None or Credentials is None:
        return None
    creds = _google_calendar_credentials_from_entry(alias, entry)
    if not creds:
        return None
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "No se pudo inicializar el cliente de Google Calendar para actualizar evento: %s",
            exc,
            exc_info=False,
        )
        return None
    kwargs: Dict[str, object] = {}
    if params.get("conferenceDataVersion"):
        kwargs["conferenceDataVersion"] = params["conferenceDataVersion"]
    try:
        event = (
            service.events()  # type: ignore[call-arg]
            .patch(calendarId="primary", eventId=event_id, body=payload, **kwargs)
            .execute()
        )
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        logger.warning(
            "Error al actualizar evento de Google Calendar mediante googleapiclient: %s",
            exc,
            exc_info=False,
        )
        return None
    _google_calendar_update_tokens_from_credentials(alias, entry, creds)
    if isinstance(event, dict):
        return event
    return None


def _google_calendar_update_event_via_requests(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    if requests is None and (Credentials is None or build is None):
        return None
    token_value = access_token or entry.get("access_token")
    if not token_value:
        return None
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Content-Type": "application/json",
    }
    url = f"{_GOOGLE_CALENDAR_BASE}/calendars/primary/events/{event_id}"
    try:
        response = requests.patch(  # type: ignore[call-arg]
            url,
            headers=headers,
            json=payload,
            params=params or None,
            timeout=20,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo actualizar el evento en Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    if response.status_code == 401:
        new_token = _google_calendar_refresh_access_token(alias, entry)
        if not new_token:
            return None
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            response = requests.patch(  # type: ignore[call-arg]
                url,
                headers=headers,
                json=payload,
                params=params or None,
                timeout=20,
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "No se pudo actualizar el evento en Google Calendar tras refrescar token: %s",
                exc,
                exc_info=False,
            )
            return None
    if response.status_code not in {200}:  # 200 OK al actualizar
        if response.status_code == 404:
            logger.info(
                "El evento de Google Calendar no existe; se creara� uno nuevo. (%s)",
                event_id,
            )
            return None
        logger.warning(
            "Respuesta inesperada al actualizar evento de Google Calendar (%s): %s",
            response.status_code,
            response.text,
        )
        return None
    try:
        data = response.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        return data
    return None


def _google_calendar_update_event(
    alias: str,
    entry: Dict[str, object],
    event_id: str,
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    event = _google_calendar_update_event_via_service(alias, entry, event_id, payload, params)
    if event:
        return event
    return _google_calendar_update_event_via_requests(alias, entry, event_id, payload, params, access_token)


def _google_calendar_ensure_token(alias: str, entry: Dict[str, object]) -> Optional[str]:
    access_token = entry.get("access_token")
    if access_token and _google_calendar_token_is_valid(entry):
        return str(access_token)
    return _google_calendar_refresh_access_token(alias, entry)


def _google_calendar_enabled_entry_for(username: str) -> tuple[Optional[str], Dict[str, object]]:
    alias_candidates: List[str] = []
    if ACTIVE_ALIAS:
        alias_candidates.append(ACTIVE_ALIAS)
    account = get_account(username) or {}
    account_alias = str(account.get("alias") or "").strip()
    if account_alias:
        alias_candidates.append(account_alias)
    alias_candidates.append(username)
    alias_candidates.append("ALL")

    seen: set[str] = set()
    for alias in alias_candidates:
        norm = _normalize_alias_key(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        entry = _get_google_calendar_entry(alias)
        if entry.get("connected") and entry.get("enabled"):
            access_token = entry.get("access_token")
            refresh_token = entry.get("refresh_token")
            if access_token and refresh_token:
                return alias, entry
    return None, {}


def _google_calendar_lead_qualifies(
    entry: Dict[str, object],
    conversation: str,
    status: Optional[str],
    phone_numbers: List[str],
    meeting_dt: datetime,
    api_key: Optional[str],
) -> bool:
    prompt_text = str(entry.get("schedule_prompt") or "").strip()
    if not prompt_text:
        return True
    if not api_key:
        return True
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning(
            "No se pudo inicializar cliente IA para evaluar Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return True
    model = _resolve_ai_model(api_key)

    system_prompt = (
        prompt_text
        + "\n\nResponde unicamente con 'SI' o 'NO' indicando si se debe crear un evento en Google Calendar."
    )
    context_lines = [
        f"Estado detectado: {status or 'desconocido'}",
        "Tela�fonos detectados: "
        + (", ".join(phone_numbers) if phone_numbers else "(sin tela�fono)"),
        f"Fecha/hora detectada: {meeting_dt.isoformat()}",
        "Conversacia�n completa:",
        conversation,
    ]
    user_content = "\n".join(context_lines)
    try:  # pragma: no cover - depende de red externa
        decision = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=user_content,
            model=model,
            temperature=0,
            max_output_tokens=20,
        ).strip().lower()
    except Exception as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo evaluar el criterio de Google Calendar con OpenAI: %s",
            exc,
            exc_info=False,
        )
        return True

    normalized = _normalize_text_for_match(decision)
    return normalized.startswith("s")


def _mask_gohighlevel_status(entry: Dict[str, object]) -> str:
    api_key = str(entry.get("api_key") or "")
    enabled = bool(entry.get("enabled"))
    status = "���� Activo" if enabled else "�ܬ Inactivo"
    location_ids = _sanitize_location_ids(entry.get("location_ids"))
    locations_text = (
        f"{len(location_ids)} Location ID(s)"
        if location_ids
        else "Location IDs: (sin definir)"
    )
    return (
        f"{status} ��� API Key: {_mask_key(api_key) or '(sin definir)'}"
        f" ��� {locations_text}"
    )


def _gohighlevel_status_lines() -> List[str]:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return ["(sin configuraciones)"]
    rows: List[str] = []
    for key in sorted(aliases.keys()):
        entry = aliases[key]
        label = str(entry.get("alias") or key)
        rows.append(f" - {label}: {_mask_gohighlevel_status(entry)}")
    return rows


def _gohighlevel_summary_line() -> str:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return "GoHighLevel: (sin configurar)"
    active = sum(1 for entry in aliases.values() if entry.get("enabled"))
    configured = sum(1 for entry in aliases.values() if entry.get("api_key"))
    return f"GoHighLevel: {active} activos / {configured} configurados"


def _gohighlevel_mark_sent(alias: str, lead: str, phone: str) -> None:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.setdefault(key, {"alias": alias.strip(), "sent": {}})
    entry.setdefault("sent", {})
    entry["sent"][_normalize_lead_id(lead)] = {"phone": phone, "ts": int(time.time())}
    aliases[key] = entry
    _write_gohighlevel_state(state)


def _gohighlevel_already_sent(alias: str, lead: str, phone: str) -> bool:
    entry = _get_gohighlevel_entry(alias)
    sent: Dict[str, dict] = entry.get("sent", {})  # type: ignore[assignment]
    record = sent.get(_normalize_lead_id(lead))
    if not isinstance(record, dict):
        return False
    stored_phone = str(record.get("phone") or "")
    return bool(stored_phone) and stored_phone == phone


def _gohighlevel_enabled_entry_for(username: str) -> tuple[Optional[str], Dict[str, object]]:
    alias_candidates: List[str] = []
    if ACTIVE_ALIAS:
        alias_candidates.append(ACTIVE_ALIAS)
    account = get_account(username) or {}
    account_alias = str(account.get("alias") or "").strip()
    if account_alias:
        alias_candidates.append(account_alias)
    alias_candidates.append(username)
    alias_candidates.append("ALL")

    seen: set[str] = set()
    for alias in alias_candidates:
        norm = _normalize_alias_key(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        entry = _get_gohighlevel_entry(alias)
        api_key = str(entry.get("api_key") or "")
        if api_key and entry.get("enabled"):
            return alias, entry
    return None, {}


def _gohighlevel_lead_qualifies(
    entry: Dict[str, object],
    conversation: str,
    status: Optional[str],
    phone_numbers: List[str],
    api_key: Optional[str],
) -> bool:
    prompt_text = str(entry.get("qualify_prompt") or "").strip()
    if not prompt_text:
        return True
    if not api_key:
        return True
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning(
            "No se pudo inicializar cliente IA para evaluar GoHighLevel: %s",
            exc,
            exc_info=False,
        )
        return True
    model = _resolve_ai_model(api_key)

    system_prompt = (
        prompt_text
        + "\n\nResponde unicamente con 'SI' o 'NO' indicando si se debe enviar el lead a GoHighLevel."
    )
    context_lines = [
        f"Estado detectado: {status or 'desconocido'}",
        "Tela�fonos detectados: "
        + (", ".join(phone_numbers) if phone_numbers else "(sin tela�fono)"),
        "Conversacia�n completa:",
        conversation,
    ]
    user_content = "\n".join(context_lines)
    try:  # pragma: no cover - depende de red externa
        decision = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=user_content,
            model=model,
            temperature=0,
            max_output_tokens=20,
        ).strip().lower()
    except Exception as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo evaluar el criterio de GoHighLevel con OpenAI: %s",
            exc,
            exc_info=False,
        )
        return True

    normalized = _normalize_text_for_match(decision)
    return normalized.startswith("s")


def _require_requests() -> bool:
    if requests is None:  # pragma: no cover - entorno sin dependencia
        warn("La librera�a 'requests' no esta� disponible. Instala�la para usar GoHighLevel.")
        press_enter()
        return False
    return True


def _gohighlevel_select_alias() -> Optional[str]:
    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias inva�lido.")
        press_enter()
        return None
    return alias


def _gohighlevel_configure_key() -> None:
    banner()
    print(style_text("GoHighLevel ��� Configurar API Key", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    current = _get_gohighlevel_entry(alias)
    print(f"Actual: {_mask_key(str(current.get('api_key') or '')) or '(sin definir)'}")
    new_key = ask("Ingresi� la API Key de GoHighLevel (vaca�o para cancelar): ").strip()
    if not new_key:
        warn("No se modifica� la API Key de GoHighLevel.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"api_key": new_key})
    ok(f"API Key guardada para {alias}.")
    press_enter()


def _gohighlevel_configure_locations() -> None:
    banner()
    print(style_text("GoHighLevel ��� Configurar Location IDs", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    current_ids = _sanitize_location_ids(entry.get("location_ids"))
    if current_ids:
        print("Actual:")
        for idx, value in enumerate(current_ids, start=1):
            print(f" {idx}) {value}")
    else:
        print("Actual: (sin definir)")
    print()
    prompt = (
        "Ingresi� uno o ma�s Location IDs (separados por coma o espacio).\n"
        "Escriba� 'eliminar N' para borrar uno especa�fico (usa el na�mero de la lista),\n"
        "'limpiar' para eliminar todos o deja� vaca�o para cancelar: "
    )
    raw = ask(prompt).strip()
    if not raw:
        warn("No se modificaron los Location IDs.")
        press_enter()
        return
    if raw.lower().startswith("eliminar"):
        if not current_ids:
            warn("No hay Location IDs para eliminar.")
            press_enter()
            return
        indexes = [token for token in re.split(r"[^0-9]a", raw) if token.isdigit()]
        if not indexes:
            warn("Indica� el na�mero del Location ID a eliminar.")
            press_enter()
            return
        to_remove: set[int] = set()
        for token in indexes:
            try:
                idx = int(token)
            except ValueError:
                continue
            if 1 <= idx <= len(current_ids):
                to_remove.add(idx - 1)
        if not to_remove:
            warn("Los na�meros indicados no coinciden con Location IDs existentes.")
            press_enter()
            return
        remaining = [value for idx, value in enumerate(current_ids) if idx not in to_remove]
        _set_gohighlevel_entry(alias, {"location_ids": remaining})
        ok(
            "Se eliminaron los Location IDs seleccionados. Total restante: "
            f"{len(remaining)}"
        )
        press_enter()
        return
    if raw.lower() in {"limpiar", "clear", "ninguno", "eliminar", "borrar"}:
        _set_gohighlevel_entry(alias, {"location_ids": []})
        ok(f"Se eliminaron los Location IDs para {alias}.")
        press_enter()
        return
    location_ids = _sanitize_location_ids(raw)
    if not location_ids:
        warn("No se detectaron Location IDs va�lidos.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"location_ids": location_ids})
    ok(f"Location IDs guardados para {alias}. Total: {len(location_ids)}")
    press_enter()


def _gohighlevel_configure_prompt() -> None:
    banner()
    print(style_text("GoHighLevel ��� Criterios de enva�o", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    current_prompt = str(entry.get("qualify_prompt") or _DEFAULT_GOHIGHLEVEL_PROMPT)
    print(style_text("Prompt actual:", color=Fore.BLUE))
    print(current_prompt or "(sin definir)")
    print(full_line(color=Fore.BLUE))
    print("Elige� una opcia�n:")
    print("  E) Editar prompt")
    print("  D) Restaurar prompt predeterminado")
    print("  Enter) Cancelar")
    action = ask("Accia�n: ").strip().lower()
    if not action:
        warn("No se modifica� el prompt de calificacia�n.")
        press_enter()
        return
    if action in {"d", "default", "predeterminado"}:
        _set_gohighlevel_entry(alias, {"qualify_prompt": _DEFAULT_GOHIGHLEVEL_PROMPT})
        ok("Se restauro� el prompt predeterminado para GoHighLevel.")
        press_enter()
        return
    if action not in {"e", "editar"}:
        warn("Opcia�n inva�lida. No se modifica� el prompt de calificacia�n.")
        press_enter()
        return
    print(
        style_text(
            "Pega� el nuevo prompt y finaliza� con una la�nea que diga <<<END>>>."
            " Deja� vaca�o para cancelar.",
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("�Ǧ ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    new_prompt = "\n".join(lines).strip()
    if not new_prompt:
        warn("No se modifica� el prompt de calificacia�n.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"qualify_prompt": new_prompt})
    ok(f"Prompt actualizado. Longitud: {len(new_prompt)} caracteres.")
    press_enter()


def _gohighlevel_activate() -> None:
    if not _require_requests():
        return
    banner()
    print(style_text("GoHighLevel ��� Activar enva�o automa�tico", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    api_key = str(entry.get("api_key") or "")
    if not api_key:
        warn("Configura� la API Key antes de activar la conexia�n.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"enabled": True})
    ok(f"Conexia�n GoHighLevel activada para {alias}.")
    press_enter()


def _gohighlevel_deactivate() -> None:
    banner()
    print(style_text("GoHighLevel ��� Desactivar conexia�n", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    _set_gohighlevel_entry(alias, {"enabled": False})
    ok(f"Conexia�n GoHighLevel desactivada para {alias}.")
    press_enter()


def _gohighlevel_menu() -> None:
    while True:
        banner()
        print(style_text("Conectar con GoHighLevel", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _gohighlevel_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Ingresar API Key de GoHighLevel")
        print("2) Configurar Location IDs de GoHighLevel")
        print("3) Activar el enva�o automa�tico de leads calificados al CRM de GoHighLevel")
        print("4) Desactivar conexia�n")
        print("5) Configurar criterios de calificacia�n")
        print("6) Volver al submena� anterior")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opcia�n: ").strip()
        if choice == "1":
            _gohighlevel_configure_key()
        elif choice == "2":
            _gohighlevel_configure_locations()
        elif choice == "3":
            _gohighlevel_activate()
        elif choice == "4":
            _gohighlevel_deactivate()
        elif choice == "5":
            _gohighlevel_configure_prompt()
        elif choice == "6":
            break
        else:
            warn("Opcia�n inva�lida.")
            press_enter()


def _google_calendar_select_alias() -> Optional[str]:
    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias inva�lido.")
        press_enter()
        return None
    return alias


def _google_calendar_perform_device_flow(
    client_id: str, client_secret: str | None
) -> Optional[Dict[str, object]]:
    if requests is None:
        return None
    try:
        response = requests.post(
            _GOOGLE_DEVICE_CODE_URL,
            data={"client_id": client_id, "scope": _GOOGLE_SCOPE},
            timeout=15,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        warn(f"No se pudo iniciar la autorizacia�n de Google: {exc}")
        press_enter()
        return None
    if response.status_code != 200:
        warn(f"Respuesta inesperada de Google: {response.text}")
        press_enter()
        return None
    payload = response.json()
    device_code = payload.get("device_code")
    if not device_code:
        warn("Google no devolvia� device_code va�lido.")
        press_enter()
        return None
    verification_url = payload.get("verification_url") or payload.get("verification_uri")
    user_code = payload.get("user_code")
    print(style_text("Para continuar:", color=Fore.CYAN, bold=True))
    if verification_url and user_code:
        print(f"1. Visita� {verification_url}")
        print(f"2. Ingresi� el ca�digo: {user_code}")
    elif user_code:
        print(f"Ingresi� el ca�digo: {user_code}")
    else:
        print("Abra� la URL indicada por Google y autoriza� el acceso.")
    print("Esperando confirmacia�n...")
    interval = int(payload.get("interval", 5))
    expires_at = time.time() + int(payload.get("expires_in", 1800))
    data = {
        "client_id": client_id,
        "device_code": device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
    }
    if client_secret:
        data["client_secret"] = client_secret
    while time.time() < expires_at:
        time.sleep(interval)
        try:
            token_response = requests.post(_GOOGLE_TOKEN_URL, data=data, timeout=15)
        except RequestException as exc:  # pragma: no cover - depende de red externa
            warn(f"Error al consultar token de Google: {exc}")
            press_enter()
            return None
        if token_response.status_code == 200:
            return token_response.json()
        try:
            error_payload = token_response.json()
        except Exception:
            error_payload = {}
        error_code = (error_payload or {}).get("error")
        if error_code in {"authorization_pending"}:
            continue
        if error_code == "slow_down":
            interval = min(interval + 2, 15)
            continue
        if error_code in {"expired_token", "access_denied"}:
            warn("La autorizacia�n no fue completada.")
            press_enter()
            return None
        warn(f"Error al obtener token de Google: {token_response.text}")
        press_enter()
        return None
    warn("El ca�digo de autorizacia�n expira�. Intenta� nuevamente.")
    press_enter()
    return None


def _google_calendar_validate_client_payload(
    payload: Dict[str, object]
) -> tuple[Optional[Dict[str, object]], Optional[str]]:
    if not isinstance(payload, dict):
        return None, "El archivo JSON no contiene una estructura va�lida."
    installed = payload.get("installed")
    if not isinstance(installed, dict):
        return (
            None,
            "El archivo JSON debe corresponder a una 'Aplicacia�n de escritorio' generada en Google Cloud Console.",
        )
    redirect_uris = installed.get("redirect_uris")
    normalized_uris: set[str] = set()
    if isinstance(redirect_uris, (list, tuple)):
        normalized_uris = {
            str(uri).strip().rstrip("/")
            for uri in redirect_uris
            if isinstance(uri, str) and uri.strip()
        }
    if _GOOGLE_REDIRECT_URI.rstrip("/") not in normalized_uris:
        return (
            None,
            "El JSON debe incluir http://localhost como redirect URI autorizado en la consola de Google.",
        )
    client_id = installed.get("client_id")
    if not client_id:
        return None, "El archivo JSON no contiene un Client ID va�lido."
    return installed, None


def _google_calendar_extract_client_credentials(
    payload: Dict[str, object]
) -> tuple[Optional[str], Optional[str]]:
    config, _ = _google_calendar_validate_client_payload(payload)
    if not config:
        return None, None
    client_id = config.get("client_id")
    client_secret = config.get("client_secret")
    return (
        (str(client_id) if client_id else None),
        (str(client_secret) if client_secret else None),
    )


def _google_calendar_report_oauth_error(exc: Exception) -> None:
    base_message = (
        "Error de autenticacia�n. Verifica� que el JSON cargado sea va�lido, "
        "que esta�s autorizado como tester y que el proyecto esta� correctamente configurado."
    )
    details = str(exc).strip()
    if details:
        warn(f"{base_message} Detalle: {details}")
    else:
        warn(base_message)


def _ensure_google_auth_oauthlib() -> bool:
    global InstalledAppFlow
    if InstalledAppFlow is not None:
        return True
    flow_cls = None
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow as flow_cls  # type: ignore
    except Exception:
        warn("Esta opcia�n requiere la librera�a google-auth-oauthlib.")
        confirm = (
            ask("-aDesea�s que la instalemos automa�ticamente ahora? (s/n): ")
            .strip()
            .lower()
        )
        if confirm not in {"s", "si", "si�", "y", "yes"}:
            warn("Instalacia�n cancelada. Instala� google-auth-oauthlib para continuar.")
            press_enter()
            return False
        python_bin = sys.executable or "python3"
        print(
            style_text(
                "Instalando google-auth-oauthlib, por favor espera�...",
                color=Fore.YELLOW,
            )
        )
        try:
            subprocess.check_call(
                [python_bin, "-m", "pip", "install", "google-auth-oauthlib"]
            )
        except Exception as exc:
            warn(f"No se pudo instalar google-auth-oauthlib automa�ticamente: {exc}")
            press_enter()
            return False
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow as flow_cls  # type: ignore
        except Exception as exc:
            warn(f"La librera�a google-auth-oauthlib no pudo cargarse: {exc}")
            press_enter()
            return False
        ok("La librera�a google-auth-oauthlib se instala� correctamente.")
    if flow_cls is None:
        warn("No se pudo cargar la librera�a google-auth-oauthlib.")
        press_enter()
        return False
    InstalledAppFlow = flow_cls
    return True


def _google_calendar_load_credentials_json() -> None:
    if not _ensure_google_auth_oauthlib():
        return
    banner()
    print(
        style_text(
            "Google Calendar ��� Cargar credenciales JSON",
            color=Fore.CYAN,
            bold=True,
        )
    )
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    path_input = ask(
        "Ruta del archivo JSON de Google (vaca�o para cancelar): "
    ).strip()
    if not path_input:
        warn("No se carga� ninga�n archivo de credenciales.")
        press_enter()
        return
    file_path = Path(path_input).expanduser()
    if not file_path.exists():
        warn("El archivo especificado no existe.")
        press_enter()
        return
    try:
        json_payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception as exc:
        warn(f"No se pudo leer el archivo JSON: {exc}")
        press_enter()
        return
    config, error = _google_calendar_validate_client_payload(json_payload)
    if error:
        warn(error)
        press_enter()
        return
    client_id = str(config.get("client_id") or "")
    client_secret_value = config.get("client_secret")
    client_secret = str(client_secret_value) if client_secret_value else None
    if not client_id:
        warn("El archivo JSON no contiene un Client ID va�lido.")
        press_enter()
        return
    _set_google_calendar_entry(
        alias,
        {"client_id": client_id, "client_secret": client_secret},
    )
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(file_path), scopes=[_GOOGLE_SCOPE]
        )
        try:
            flow.redirect_uri = _GOOGLE_REDIRECT_URI
        except Exception:
            # Algunos objetos Flow no exponen redirect_uri hasta ejecutar run_*.
            pass
    except Exception as exc:  # pragma: no cover - depende de librera�a externa
        warn(f"No se pudo inicializar el flujo OAuth: {exc}")
        press_enter()
        return
    try:
        credentials = flow.run_local_server(port=0)
    except Exception as exc_local:  # pragma: no cover - depende de librera�a externa
        logger.debug(
            "Fallo run_local_server para Google OAuth, se intenta modo consola",
            exc_info=exc_local,
        )
        try:
            credentials = flow.run_console()
        except Exception as exc_console:  # pragma: no cover - depende de librera�a externa
            _google_calendar_report_oauth_error(exc_console)
            press_enter()
            return
    entry = _get_google_calendar_entry(alias)
    _google_calendar_update_tokens_from_credentials(alias, entry, credentials)
    entry = _get_google_calendar_entry(alias)
    if entry.get("connected"):
        ok(f"Google Calendar conectado para {alias}.")
    else:
        warn("No se pudo completar la conexia�n con Google Calendar.")
    press_enter()


def _google_calendar_connect() -> None:
    if not _require_requests():
        return
    banner()
    print(style_text("Google Calendar ��� Conectar cuenta", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_client_id = str(entry.get("client_id") or "")
    current_client_secret = str(entry.get("client_secret") or "")
    print(f"Client ID actual: {current_client_id or '(sin definir)'}")
    client_id = ask("Ingresi� el Client ID de OAuth (vaca�o mantiene actual): ").strip()
    if not client_id:
        client_id = current_client_id
    if not client_id:
        warn("Se requiere un Client ID va�lido para continuar.")
        press_enter()
        return
    client_secret = ask(
        "Ingresi� el Client Secret (vaca�o mantiene actual o se omite si no aplica): "
    ).strip()
    if not client_secret:
        client_secret = current_client_secret
    _set_google_calendar_entry(alias, {"client_id": client_id, "client_secret": client_secret})
    token_data = _google_calendar_perform_device_flow(client_id, client_secret or None)
    if not token_data:
        return
    entry = _get_google_calendar_entry(alias)
    entry = _google_calendar_store_tokens(alias, entry, token_data)
    if entry.get("connected"):
        ok(f"Google Calendar conectado para {alias}.")
    else:
        warn("No se pudo completar la conexia�n con Google Calendar.")
    press_enter()


def _google_calendar_configure_event() -> None:
    banner()
    print(style_text("Google Calendar ��� Configuracia�n de eventos", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_name = str(entry.get("event_name") or "{{username}} - Sistema de adquisicia�n con IA")
    current_duration = int(entry.get("duration_minutes") or 30)
    current_timezone = str(entry.get("timezone") or _default_timezone_label())
    current_auto_meet = bool(entry.get("auto_meet", True))
    print(f"Nombre actual del evento: {current_name}")
    new_name = ask("Nuevo nombre (usa {{username}} para el lead, Enter mantiene): ").strip()
    updates: Dict[str, object] = {}
    if new_name:
        updates["event_name"] = new_name
    duration_input = ask(
        f"Duracia�n en minutos (actual {current_duration}, Enter mantiene): "
    ).strip()
    if duration_input:
        try:
            updates["duration_minutes"] = max(5, int(duration_input))
        except Exception:
            warn("Duracia�n inva�lida; se mantiene el valor actual.")
    tz_input = ask(
        f"Zona horaria (actual {current_timezone}, Enter mantiene): "
    ).strip()
    if tz_input:
        updates["timezone"] = tz_input
    auto_meet_input = ask(
        f"Generar enlace de Google Meet automa�ticamente? (S/N, actual {'S' if current_auto_meet else 'N'}): "
    ).strip().lower()
    if auto_meet_input in {"s", "si", "si�"}:
        updates["auto_meet"] = True
    elif auto_meet_input in {"n", "no"}:
        updates["auto_meet"] = False
    if updates:
        _set_google_calendar_entry(alias, updates)
        ok("Configuracia�n de eventos actualizada.")
    else:
        warn("No se realizaron cambios.")
    press_enter()


def _google_calendar_configure_prompt() -> None:
    banner()
    print(
        style_text(
            "Google Calendar ��� Criterio para creacia�n de eventos",
            color=Fore.CYAN,
            bold=True,
        )
    )
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_prompt = str(entry.get("schedule_prompt") or _DEFAULT_GOOGLE_CALENDAR_PROMPT)
    print(style_text("Prompt actual:", color=Fore.BLUE))
    print(current_prompt.strip() or "(sin definir)")
    print(full_line(color=Fore.BLUE))
    print("Elige� una opcia�n:")
    print("  E) Editar prompt")
    print("  D) Restaurar valor predeterminado")
    print("  Enter) Cancelar")
    action = ask("Accia�n: ").strip().lower()
    if not action:
        warn("No se modifica� el criterio de calendario.")
        press_enter()
        return
    if action in {"d", "default", "predeterminado"}:
        _set_google_calendar_entry(
            alias,
            {"schedule_prompt": _DEFAULT_GOOGLE_CALENDAR_PROMPT},
        )
        ok("Se restauro� el criterio predeterminado de Google Calendar.")
        press_enter()
        return
    if action not in {"e", "editar"}:
        warn("Opcia�n inva�lida. No se modifica� el criterio de calendario.")
        press_enter()
        return
    print(
        style_text(
            (
                "Pega� el nuevo criterio y finaliza� con una la�nea que diga <<<END>>>."
                " Deja� vaca�o para cancelar."
            ),
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("�Ǧ ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    new_prompt = "\n".join(lines).strip()
    _set_google_calendar_entry(alias, {"schedule_prompt": new_prompt})
    if new_prompt:
        ok(f"Criterio actualizado. Longitud: {len(new_prompt)} caracteres.")
    else:
        ok("Se elimino� el criterio personalizado. Se usara� la la�gica automa�tica predeterminada.")
    press_enter()


def _google_calendar_activate() -> None:
    banner()
    print(style_text("Google Calendar ��� Activar creacia�n automa�tica", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    if not entry.get("connected"):
        warn("Conecta� Google Calendar antes de activar la la�gica automa�tica.")
        press_enter()
        return
    _set_google_calendar_entry(alias, {"enabled": True})
    ok(f"La�gica automa�tica activada para {alias}.")
    press_enter()


def _google_calendar_deactivate() -> None:
    banner()
    print(style_text("Google Calendar ��� Desactivar creacia�n automa�tica", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    _set_google_calendar_entry(alias, {"enabled": False})
    ok(f"La�gica automa�tica desactivada para {alias}.")
    press_enter()


def _google_calendar_revoke() -> None:
    banner()
    print(style_text("Google Calendar ��� Revocar conexia�n", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    token = entry.get("access_token") or entry.get("refresh_token")
    if token and _require_requests():
        try:
            requests.post(_GOOGLE_REVOKE_URL, params={"token": token}, timeout=15)
        except RequestException:
            logger.warning("No se pudo notificar la revocacia�n a Google.", exc_info=False)
    _set_google_calendar_entry(
        alias,
        {
            "access_token": "",
            "refresh_token": "",
            "token_type": "",
            "token_expires_at": 0,
            "connected": False,
            "enabled": False,
        },
    )
    ok(f"Conexia�n revocada para {alias}.")
    press_enter()


def _google_calendar_menu() -> None:
    while True:
        banner()
        print(style_text("Conectar con Google Calendar", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _google_calendar_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Conectar cuenta mediante OAuth")
        print("2) Configurar para�metros del evento")
        print("3) Configurar criterio para creacia�n de evento")
        print("4) Activar creacia�n automa�tica de eventos")
        print("5) Desactivar creacia�n automa�tica de eventos")
        print("6) Revocar conexia�n")
        print("7) Cargar credenciales JSON (Google OAuth 2.0)")
        print("8) Volver al submena� anterior")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opcia�n: ").strip()
        if choice == "1":
            _google_calendar_connect()
        elif choice == "2":
            _google_calendar_configure_event()
        elif choice == "3":
            _google_calendar_configure_prompt()
        elif choice == "4":
            _google_calendar_activate()
        elif choice == "5":
            _google_calendar_deactivate()
        elif choice == "6":
            _google_calendar_revoke()
        elif choice == "7":
            _google_calendar_load_credentials_json()
        elif choice == "8":
            break
        else:
            warn("Opcia�n inva�lida.")
            press_enter()


def _configure_api_key() -> None:
    banner()
    current_key, _ = _load_preferences()
    current_provider, current_model = (
        _resolve_ai_runtime(current_key) if current_key else ("(sin definir)", _OPENAI_DEFAULT_MODEL)
    )
    print(style_text("Configurar OPENAI_API_KEY", color=Fore.CYAN, bold=True))
    print(f"Actual: {(_mask_key(current_key) or '(sin definir)')}")
    print(f"Proveedor detectado: {current_provider}")
    print(f"Modelo actual: {current_model}")
    print()
    new_key = ask("Nueva API Key (vaca�o para cancelar): ").strip()
    if not new_key:
        warn("Se mantuvo la API Key actual.")
        press_enter()
        return
    update_env_local({"OPENAI_API_KEY": new_key})
    refresh_settings()
    ok("OPENAI_API_KEY guardada en .env.local")
    press_enter()


def _configure_prompt() -> None:
    target_alias_input = ask(
        f"Alias del prompt (Enter={_PROMPT_DEFAULT_ALIAS}): "
    ).strip()
    target_alias = target_alias_input or _PROMPT_DEFAULT_ALIAS
    while True:
        banner()
        _, current_prompt = _load_preferences(target_alias)
        print(style_text("Configurar System Prompt", color=Fore.CYAN, bold=True))
        print(f"Alias: {target_alias}")
        print(style_text("Actual:", color=Fore.BLUE))
        print(current_prompt or "(sin definir)")
        print()
        print(f"Longitud actual: {len(current_prompt or '')} caracteres.")
        print(full_line(color=Fore.BLUE))
        print("1) Editar/pegar en consola (delimitador <<<END>>>)")
        print("2) Cargar desde archivo .txt")
        print("3) Ver primeros 400 caracteres")
        print("4) Volver")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opcia�n: ").strip()

        if choice == "1":
            print(style_text(
                "Pega� tu System Prompt y cerra� con una la�nea que diga <<<END>>>.",
                color=Fore.CYAN,
            ))
            lines: list[str] = []
            while True:
                line = ask("�Ǧ ")
                if line.strip() == "<<<END>>>":
                    break
                lines.append(line.replace("\r", ""))
            new_prompt = "\n".join(lines)
            if not _normalize_system_prompt_text(new_prompt):
                warn("No se modifica� el prompt.")
                press_enter()
                continue
            saved_prompt = _persist_system_prompt(new_prompt, alias=target_alias)
            ok(f"System Prompt guardado. Longitud: {len(saved_prompt)} caracteres.")
            press_enter()
        elif choice == "2":
            path_input = ask("Ruta del archivo .txt (vaca�o para cancelar): ").strip()
            if not path_input:
                warn("No se modifica� el prompt.")
                press_enter()
                continue
            file_path = Path(path_input).expanduser()
            if not file_path.exists():
                warn("El archivo especificado no existe.")
                press_enter()
                continue
            try:
                file_contents = file_path.read_text(encoding="utf-8")
            except Exception as exc:
                warn(f"No se pudo leer el archivo: {exc}")
                press_enter()
                continue
            if not _normalize_system_prompt_text(file_contents):
                warn("No se modifica� el prompt.")
                press_enter()
                continue
            saved_prompt = _persist_system_prompt(file_contents, alias=target_alias)
            ok(f"System Prompt guardado. Longitud: {len(saved_prompt)} caracteres.")
            press_enter()
        elif choice == "3":
            preview = (current_prompt or "")[:400]
            print(style_text("Primeros 400 caracteres:", color=Fore.BLUE))
            if not preview:
                print("(sin definir)")
            else:
                print(preview)
                if len(current_prompt or "") > 400:
                    print(style_text("�Ǫ (truncado)", color=Fore.YELLOW))
            press_enter()
        elif choice == "4":
            break
        else:
            warn("Opcia�n inva�lida.")
            press_enter()


def _available_aliases() -> List[str]:
    aliases: set[str] = {"ALL"}
    for account in list_all():
        if account.get("alias"):
            aliases.add(account["alias"].strip())
        if account.get("username"):
            aliases.add(account["username"].strip())
    return sorted(a for a in aliases if a)


def _preview_prompt(prompt: str) -> str:
    if not prompt:
        return "(sin definir)"
    first_line = prompt.splitlines()[0]
    if len(first_line) > 60:
        return first_line[:57] + "�Ǫ"
    if len(prompt.splitlines()) > 1:
        return first_line + " �Ǫ"
    return first_line


def autoresponder_menu_options() -> List[str]:
    return [
        "1) Configurar API Key",
        "2) Configurar System Prompt",
        "3) Activar bot (alias/grupo)",
        "4) Seguimiento",
        "5) Conectar con GoHighLevel",
        "6) Conectar con Google Calendar",
        "7) Desactivar bot",
        "8) Volver",
    ]


def autoresponder_prompt_length() -> int:
    _, prompt = _load_preferences()
    return len(prompt or "")


def _print_menu_header() -> None:
    banner()
    api_key, prompt = _load_preferences(ACTIVE_ALIAS)
    provider, model = _resolve_ai_runtime(api_key) if api_key else ("(sin definir)", _OPENAI_DEFAULT_MODEL)
    status = (
        style_text(f"Estado: activo para {ACTIVE_ALIAS}", color=Fore.GREEN, bold=True)
        if ACTIVE_ALIAS
        else style_text("Estado: inactivo", color=Fore.YELLOW, bold=True)
    )
    print(style_text("Auto-responder con IA", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    print(f"API Key: {_mask_key(api_key) or '(sin definir)'}")
    print(f"Proveedor: {provider}")
    print(f"Modelo: {model}")
    print(f"System prompt: {_preview_prompt(prompt)}")
    print(status)
    print(_followup_summary_line())
    print(_gohighlevel_summary_line())
    print(_google_calendar_summary_line())
    print(full_line(color=Fore.BLUE))
    for option in autoresponder_menu_options():
        print(option)
    print(full_line(color=Fore.BLUE))


def _prompt_alias_selection() -> str | None:
    options = _available_aliases()
    print("Alias/grupos disponibles:")
    for idx, alias in enumerate(options, start=1):
        print(f" {idx}) {alias}")
    raw = ask("Selecciona� alias (na�mero o texto, Enter=ALL): ").strip()
    if not raw:
        return "ALL"
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(options):
            return options[idx - 1]
        warn("Na�mero fuera de rango.")
        return None
    return raw


def _handle_account_issue(user: str, exc: Exception, active: List[str]) -> None:
    message = str(exc).lower()
    detail = f"{exc.__class__.__name__}: {exc}"
    if should_retry_proxy(exc):
        label = style_text(f"[WARN][@{user}] proxy falla�", color=Fore.YELLOW, bold=True)
        record_proxy_failure(user, exc)
        print(label)
        warn("Revisi� la opcia�n 1 para actualizar o quitar el proxy de esta cuenta.")
    elif "login_required" in message or "login requerido" in message:
        label = style_text(f"[ERROR][@{user}] sesia�n inva�lida", color=Fore.RED, bold=True)
        print(label)
    elif any(key in message for key in ("challenge", "checkpoint")):
        label = style_text(f"[WARN][@{user}] checkpoint requerido", color=Fore.YELLOW, bold=True)
        print(label)
    elif "feedback_required" in message or "rate" in message:
        label = style_text(f"[WARN][@{user}] rate limit detectado", color=Fore.YELLOW, bold=True)
        print(label)
    else:
        label = style_text(f"[WARN][@{user}] error inesperado", color=Fore.YELLOW, bold=True)
        print(label)
    try:
        warn(detail)
    except Exception:
        print(detail)
    try:
        _append_message_log(
            {
                "action": "account_error",
                "account": user,
                "error": detail,
            }
        )
    except Exception:
        pass
    logger.warning("Incidente con @%s en auto-responder: %s", user, exc, exc_info=False)

    auto_policy_raw = str(
        os.getenv("AUTORESPONDER_ACCOUNT_ISSUE_POLICY", "keep") or "keep"
    ).strip().lower()
    auto_choice = ""
    if auto_policy_raw in {"c", "r", "p", "k"}:
        auto_choice = auto_policy_raw
    elif auto_policy_raw in {"continue", "skip", "skip_account"}:
        auto_choice = "c"
    elif auto_policy_raw in {"retry", "reintentar"}:
        auto_choice = "r"
    elif auto_policy_raw in {"pause", "pausar"}:
        auto_choice = "p"
    elif auto_policy_raw in {"keep", "keep_account"}:
        auto_choice = "k"

    if auto_choice:
        choice = auto_choice
        logger.info(
            "Auto-policy de account issue aplicada para @%s: %s",
            user,
            choice,
        )
    else:
        while True:
            choice = ask("- Continuar sin esta cuenta (C) / Reintentar (R) / Pausar (P) / Mantener en ciclo (K)? ").strip().lower()
            if choice in {"c", "r", "p", "k"}:
                break
            warn("Elige� C, R, P o K.")

    if choice == "k":
        warn(f"Se mantiene @{user} en el ciclo y se reintentara en la siguiente vuelta.")
        return

    if choice == "c":
        if user in active:
            active.remove(user)
        mark_connected(user, False)
        warn(f"Se excluye @{user} del ciclo actual.")
        return

    if choice == "p":
        request_stop("pausa solicitada desde mena� del bot")
        return

    while choice == "r":
        if _prompt_playwright_login(user, alias=ACTIVE_ALIAS or user) and _ensure_session(user):
            mark_connected(user, True)
            ok(f"Sesia�n renovada para @{user}")
            return
        warn("La sesia�n sigue fallando. Intenta� nuevamente o elega� otra opcia�n.")
        choice = ask("- Reintentar (R) / Continuar sin la cuenta (C) / Pausar (P) / Mantener en ciclo (K)? ").strip().lower()
        if choice == "c":
            if user in active:
                active.remove(user)
            mark_connected(user, False)
            warn(f"Se excluye @{user} del ciclo actual.")
            return
        if choice == "p":
            request_stop("pausa solicitada desde mena� del bot")
            return
        if choice == "k":
            warn(f"Se mantiene @{user} en el ciclo y se reintentara en la siguiente vuelta.")
            return


def _normalize_phone(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    has_plus = raw.startswith("a")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    return f"a{digits}" if has_plus else digits


def _extract_phone_numbers(text: str) -> List[str]:
    if not text:
        return []
    matches = _PHONE_PATTERN.findall(text)
    numbers: List[str] = []
    for match in matches:
        normalized = _normalize_phone(match)
        if normalized and len(normalized.replace("a", "")) >= 8:
            if normalized not in numbers:
                numbers.append(normalized)
    return numbers


def _extract_email_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    matches = list(_EMAIL_PATTERN.findall(text))
    if not matches:
        return None
    return matches[-1]


def _infer_lead_tag(
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str] = None,
) -> str:
    if status and status.strip().lower() == "no interesado":
        return "No calificado"
    normalized = _normalize_text_for_match(conversation)
    if any(_contains_token(normalized, keyword) for keyword in _NEGATIVE_KEYWORDS):
        return "No calificado"
    if phone_numbers:
        if any(word in normalized for word in _CALL_KEYWORDS):
            return "Listo para agendar llamada"
        if status and status.strip().lower() == "interesado":
            return "Listo para agendar llamada"
        return "Listo para agendar llamada"
    if any(_contains_token(normalized, keyword) for keyword in _POSITIVE_KEYWORDS):
        return "Interesado sin na�mero"
    if any(keyword in normalized for keyword in _INFO_KEYWORDS) or "?" in conversation:
        return "Solicita ma�s info"
    if normalized.strip():
        return _DEFAULT_LEAD_TAG
    return _DEFAULT_LEAD_TAG


def _build_conversation_note(
    account: str, recipient: str, conversation: str, status: Optional[str] = None
) -> str:
    header = [f"Cuenta IG: @{account}"]
    if recipient:
        header.append(f"Usuario: @{recipient}")
    if status:
        header.append(f"Estado detectado: {status}")
    header.append("Historial completo:")
    return "\n".join(header + [conversation])


def _next_weekday_date(base: datetime, target_weekday: int) -> datetime.date:
    days_ahead = (target_weekday - base.weekday() + 7) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (base + timedelta(days=days_ahead)).date()


def _parse_meeting_datetime_from_text(text: str, tz_label: str) -> Optional[datetime]:
    if not text:
        return None
    if not _MEETING_TIME_PATTERN.search(text):
        return None
    match = _MEETING_TIME_PATTERN.search(text)
    if not match:
        return None
    try:
        hour = int(match.group("hour"))
    except Exception:
        return None
    minute_str = match.group("minute")
    minute = int(minute_str) if minute_str and minute_str.isdigit() else 0
    ampm = match.group("ampm")
    if ampm:
        ampm = ampm.lower()
        if ampm == "pm" and hour < 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
    hour %= 24
    tz = _safe_timezone(tz_label)
    base = datetime.now(tz)
    normalized = _normalize_text_for_match(text)
    date_value: Optional[datetime.date] = None
    for keyword, offset in _RELATIVE_DATE_KEYWORDS:
        if keyword in normalized:
            date_value = (base + timedelta(days=offset)).date()
            break
    if date_value is None:
        for keyword, weekday in _WEEKDAY_KEYWORDS.items():
            if _contains_token(normalized, keyword):
                date_value = _next_weekday_date(base, weekday)
                break
    if date_value is None and _MEETING_DATE_PATTERN.search(text):
        parsed = _safe_parse_datetime(text, fuzzy=True, dayfirst=True, default=base)
        if isinstance(parsed, datetime):
            date_value = parsed.date()
    if date_value is None:
        return None
    meeting_dt = datetime.combine(date_value, dt_time(hour=hour, minute=minute), tz)
    if meeting_dt < base:
        if _MEETING_DATE_PATTERN.search(text):
            return None
        meeting_dt += timedelta(days=7)
    return meeting_dt


def _detect_meeting_datetime(conversation: str, tz_label: str) -> Optional[datetime]:
    lines = [line for line in conversation.splitlines() if line.startswith("ELLOS:")]
    for line in reversed(lines):
        _, _, content = line.partition(":")
        meeting_dt = _parse_meeting_datetime_from_text(content.strip(), tz_label)
        if meeting_dt:
            return meeting_dt
    return None


def _render_calendar_summary(template: str, username: str) -> str:
    template = template or "{{username}} - Sistema de adquisicia�n con IA"
    return template.replace("{{username}}", username or "Lead")


def _parse_gohighlevel_contact_id(data: Dict[str, object]) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    contact = data.get("contact") if isinstance(data.get("contact"), dict) else None
    if isinstance(contact, dict):
        for key in ("id", "Id", "contactId"):
            if contact.get(key):
                return str(contact[key])
    for key in ("contactId", "id", "Id"):
        value = data.get(key)
        if value:
            return str(value)
    return None


def _update_gohighlevel_contact(
    api_key: str, contact_id: str, payload: Dict[str, object]
) -> Optional[str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/{contact_id}"
    response = requests.put(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    response.raise_for_status()
    data: Dict[str, object] = {}
    if response.content:
        try:
            data = response.json()
        except ValueError:
            data = {}
    return _parse_gohighlevel_contact_id(data) or contact_id


def _create_gohighlevel_contact(
    api_key: str, payload: Dict[str, object]
) -> tuple[Optional[str], bool]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/"
    response = requests.post(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    data: Dict[str, object] = {}
    if response.status_code == 409:
        try:
            data = response.json()
        except ValueError:
            data = {}
        contact_id = _parse_gohighlevel_contact_id(data)
        if contact_id:
            updated_id = _update_gohighlevel_contact(api_key, str(contact_id), payload)
            return updated_id, False
    response.raise_for_status()
    if response.content:
        try:
            data = response.json()
        except ValueError:
            data = {}
    contact_id = _parse_gohighlevel_contact_id(data)
    if contact_id:
        return contact_id, True
    return None, True


def _attach_gohighlevel_note(api_key: str, contact_id: str, note: str) -> None:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/{contact_id}/notes/"
    payload = {"body": note}
    response = requests.post(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    response.raise_for_status()


def _send_lead_to_gohighlevel(
    account: str,
    recipient: str,
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str],
    openai_api_key: Optional[str] = None,
) -> None:
    if requests is None:
        logger.warning("GoHighLevel no disponible: falta la librera�a requests.")
        return
    if not phone_numbers:
        return
    alias, entry = _gohighlevel_enabled_entry_for(account)
    if not alias or not entry:
        return
    api_key = str(entry.get("api_key") or "")
    if not api_key:
        return
    location_ids = _sanitize_location_ids(entry.get("location_ids"))
    if not location_ids:
        logger.info(
            "GoHighLevel sin Location IDs configurados | alias=%s | cuenta=%s",
            alias,
            account,
        )
        return
    lead_identifier = recipient or phone_numbers[0]
    normalized_lead = _normalize_lead_id(lead_identifier)
    main_phone = phone_numbers[0]
    if _gohighlevel_already_sent(alias, normalized_lead, main_phone):
        return

    if not _gohighlevel_lead_qualifies(
        entry,
        conversation,
        status,
        phone_numbers,
        openai_api_key,
    ):
        return

    contact_payload: Dict[str, object] = {
        "name": recipient or "Lead Instagram",
        "phone": main_phone,
    }
    email = _extract_email_from_text(conversation)
    if email:
        contact_payload["email"] = email
    note_text = _build_conversation_note(account, recipient, conversation, status)
    lead_tag = _infer_lead_tag(conversation, phone_numbers, status)
    successes: List[str] = []
    for location_id in location_ids:
        payload = dict(contact_payload)
        payload["locationId"] = location_id
        if lead_tag:
            payload["tags"] = [lead_tag]
        try:
            contact_id, created = _create_gohighlevel_contact(api_key, payload)
            if not contact_id:
                message = (
                    "No se obtuvo contactId al crear contacto en GoHighLevel para %s (location %s)."
                )
                logger.warning(
                    message,
                    recipient or "(sin usuario)",
                    location_id,
                )
                print(
                    f"��� Falla� el enva�o a GHL (Location {location_id}): no se recibia� identificador del contacto"
                )
                continue
            _attach_gohighlevel_note(api_key, contact_id, note_text)
            successes.append(location_id)
            action = "creado" if created else "actualizado"
            print(
                f"ԣ� Lead enviado a GHL (Location {location_id}) ��� contacto {action} (ID {contact_id})"
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "Error enviando lead a GoHighLevel (location %s): %s",
                location_id,
                exc,
                exc_info=False,
            )
            print(f"��� Falla� el enva�o a GHL (Location {location_id}): {exc}")
        except Exception as exc:  # pragma: no cover - manejo defensivo
            logger.warning(
                "Fallo inesperado con GoHighLevel (location %s): %s",
                location_id,
                exc,
                exc_info=False,
            )
            print(f"��� Falla� el enva�o a GHL (Location {location_id}): {exc}")
    if not successes:
        return

    _gohighlevel_mark_sent(alias, normalized_lead, main_phone)
    logger.info(
        "Lead enviado a GoHighLevel | alias=%s | cuenta=%s | contacto=%s | locations=%s | tag=%s",
        alias,
        account,
        recipient or "(sin usuario)",
        ",".join(successes),
        lead_tag,
    )


def _maybe_schedule_google_calendar_event(
    account: str,
    recipient: str,
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str],
    openai_api_key: Optional[str] = None,
) -> Optional[tuple[str, str]]:
    if ACTIVE_ALIAS is None:
        return None
    if status and status.strip().lower() == "no interesado":
        return None
    alias, entry = _google_calendar_enabled_entry_for(account)
    if not alias or not entry:
        return None
    if requests is None and (Credentials is None or build is None):
        return None
    tz_label = str(entry.get("timezone") or _default_timezone_label())
    meeting_dt = _detect_meeting_datetime(conversation, tz_label)
    if not meeting_dt:
        return None
    normalized_convo = _normalize_text_for_match(conversation)
    prompt_text = str(entry.get("schedule_prompt") or "").strip()
    if not prompt_text and not any(
        keyword in normalized_convo for keyword in _CALL_KEYWORDS
    ):
        return None
    if not _google_calendar_lead_qualifies(
        entry,
        conversation,
        status,
        phone_numbers,
        meeting_dt,
        openai_api_key,
    ):
        return None
    main_phone = _normalize_phone(phone_numbers[0]) if phone_numbers else ""
    normalized_lead = recipient or main_phone or f"{account}-lead"
    scheduled_entry, _ = _google_calendar_get_scheduled(
        alias, normalized_lead, main_phone
    )
    previous_start: Optional[datetime] = None
    previous_link = ""
    event_id_to_update: Optional[str] = None
    if scheduled_entry:
        event_id_value = scheduled_entry.get("event_id")
        if isinstance(event_id_value, str) and event_id_value.strip():
            event_id_to_update = event_id_value
        start_value = scheduled_entry.get("start")
        if isinstance(start_value, str) and start_value:
            try:
                previous_start = datetime.fromisoformat(start_value)
            except Exception:
                previous_start = None
        link_value = scheduled_entry.get("link")
        if isinstance(link_value, str):
            previous_link = _google_calendar_preferred_link(link_value)
    access_token = _google_calendar_ensure_token(alias, entry)
    if not access_token and requests is None and (Credentials is None or build is None):
        return None
    summary_template = str(entry.get("event_name") or "{{username}} - Sistema de adquisicia�n con IA")
    summary = _render_calendar_summary(summary_template, recipient or "Lead")
    try:
        duration = int(entry.get("duration_minutes") or 30)
    except Exception:
        duration = 30
    duration = max(5, duration)
    tz = _safe_timezone(tz_label)
    start_dt = meeting_dt.astimezone(tz)
    end_dt = start_dt + timedelta(minutes=duration)
    email = _extract_email_from_text(conversation)
    description_lines = [
        "Evento generado automa�ticamente desde el bot de Instagram.",
        f"Cuenta IG: @{account}",
    ]
    if recipient:
        description_lines.append(f"Usuario IG: @{recipient}")
    if main_phone:
        description_lines.append(f"Tela�fono: {main_phone}")
    else:
        description_lines.append("Tela�fono: (sin proporcionar)")
    if email:
        description_lines.append(f"Email: {email}")
    if status:
        description_lines.append(f"Estado detectado: {status}")
    description_lines.append("")
    description_lines.append("Historial de la conversacia�n:")
    description_lines.append(conversation)
    description = "\n".join(description_lines)
    payload: Dict[str, object] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": tz_label},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": tz_label},
    }
    attendees: List[Dict[str, str]] = []
    if email:
        attendees.append({"email": email})
    if attendees:
        payload["attendees"] = attendees
    params: Dict[str, object] = {}
    if entry.get("auto_meet", True):
        payload["conferenceData"] = {
            "createRequest": {
                "requestId": uuid.uuid4().hex,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        }
        params["conferenceDataVersion"] = 1
    event: Optional[Dict[str, object]] = None
    event_action = "created"
    skip_message = False
    if event_id_to_update:
        should_update = True
        if previous_start:
            delta = abs((previous_start - start_dt).total_seconds())
            if delta < 60:
                should_update = False
        if should_update:
            event = _google_calendar_update_event(
                alias, entry, event_id_to_update, payload, params, access_token
            )
            if event:
                event_action = "updated"
        else:
            skip_message = True
        if not event and not skip_message:
            event = _google_calendar_create_event(alias, entry, payload, params, access_token)
            event_action = "created"
    else:
        event = _google_calendar_create_event(alias, entry, payload, params, access_token)
        event_action = "created"
    if skip_message:
        return None
    if not event:
        return None
    event_id = event.get("id") if isinstance(event, dict) else None
    if not event_id:
        return None
    share_link = _google_calendar_share_link_from_event_id(event_id)
    event_link = ""
    backup_link = ""
    if isinstance(event, dict):
        event_link = _google_calendar_preferred_link(str(event.get("htmlLink") or ""))
        backup_link = str(event.get("hangoutLink") or "")
        if not backup_link:
            conference_data = event.get("conferenceData") if isinstance(event.get("conferenceData"), dict) else {}
            if isinstance(conference_data, dict):
                entry_points = conference_data.get("entryPoints")
                if isinstance(entry_points, list):
                    for item in entry_points:
                        if isinstance(item, dict) and item.get("uri"):
                            backup_link = str(item["uri"])
                            break
        if share_link and (
            not event_link or "calendar.app.google" not in event_link
        ):
            event_link = share_link
    if not event_link:
        fetched_event = _google_calendar_fetch_event(
            alias, entry, event_id, access_token
        )
        if isinstance(fetched_event, dict):
            event = fetched_event
            event_link = _google_calendar_preferred_link(
                str(fetched_event.get("htmlLink") or "")
            )
            if share_link and (
                not event_link or "calendar.app.google" not in event_link
            ):
                event_link = share_link
            if not backup_link:
                backup_link = str(fetched_event.get("hangoutLink") or "")
                if not backup_link:
                    conference_data = (
                        fetched_event.get("conferenceData")
                        if isinstance(fetched_event.get("conferenceData"), dict)
                        else {}
                    )
                    if isinstance(conference_data, dict):
                        entry_points = conference_data.get("entryPoints")
                        if isinstance(entry_points, list):
                            for item in entry_points:
                                if isinstance(item, dict) and item.get("uri"):
                                    backup_link = str(item["uri"])
                                    break
    stored_link = event_link or share_link or backup_link or previous_link
    stored_link = _google_calendar_preferred_link(stored_link)
    _google_calendar_mark_scheduled(
        alias,
        normalized_lead,
        main_phone,
        event_id,
        stored_link,
        start_dt.isoformat(),
    )
    logger.info(
        "Evento programado en Google Calendar | alias=%s | cuenta=%s | lead=%s | inicio=%s",
        alias,
        account,
        recipient or "(sin usuario)",
        start_dt.isoformat(),
    )
    formatted_dt = start_dt.strftime("%d/%m/%Y %H:%M")
    recipient_handle = _format_handle(recipient or main_phone or None)
    if event_action == "updated":
        message_lines = [
            f"Perfecto, actualica� nuestra llamada para {formatted_dt} ({tz_label}).",
        ]
    else:
        message_lines = [
            f"Listo, acabo de agendar nuestra llamada para {formatted_dt} ({tz_label}).",
        ]
    if event_link:
        message_lines.append(
            f"Te paso el link del evento para que confirmes la asistencia: {event_link}"
        )
    elif stored_link:
        message_lines.append(
            f"Te comparta� los detalles de la reunia�n en nuestro calendario: {stored_link}"
        )
    else:
        message_lines.append("Te comparta� los detalles de la reunia�n en nuestro calendario.")
    status_line = f"ԣ� Reunia�n agendada en Google Calendar para {recipient_handle}"
    log_conversation_status(
        account,
        recipient or normalized_lead,
        status_line,
        timestamp=int(time.time()),
    )
    return "\n".join(message_lines), status_line


@dataclass
class _MemoryMessageSnapshot:
    id: str
    user_id: str
    text: str
    timestamp: Optional[float]
    direction: str = "unknown"


def _safe_float(value: object) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    return number if number > 0 else None


def _extract_thread_id_from_href(href: object) -> str:
    value = str(href or "").strip()
    if not value:
        return ""
    match = re.search(r"/direct/t/([^/?#]+)", value)
    if match:
        return str(match.group(1) or "").strip()
    return ""


def _normalize_thread_href(href: object) -> str:
    value = str(href or "").strip()
    if not value:
        return ""
    if "/direct/t/" not in value:
        return ""
    if value.startswith("http"):
        return value
    if value.startswith("/"):
        return f"https://www.instagram.com{value}"
    return f"https://www.instagram.com/{value.lstrip('/')}"


def _is_probably_web_thread_id(value: object) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    if not token.isdigit():
        return False
    return 6 <= len(token) <= 20


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _merge_messages_sent_lists(current_list: object, incoming_list: object) -> List[Dict[str, Any]]:
    current = current_list if isinstance(current_list, list) else []
    incoming = incoming_list if isinstance(incoming_list, list) else []
    merged_map: Dict[str, Dict[str, Any]] = {}

    def _msg_key(item: Dict[str, Any]) -> str:
        text = str(item.get("text") or "").strip().lower()
        is_followup = bool(item.get("is_followup", False))
        followup_stage = str(item.get("followup_stage") or "")
        message_id = str(item.get("message_id") or item.get("last_message_id") or "").strip()
        if message_id:
            return f"id:{message_id}"
        return f"text:{text}|fu:{int(is_followup)}|stage:{followup_stage}"

    for source in (current, incoming):
        for raw in source:
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            key = _msg_key(item)
            if key not in merged_map:
                merged_map[key] = item
                continue
            prev = merged_map[key]
            prev_first = _safe_float(prev.get("first_sent_at"))
            item_first = _safe_float(item.get("first_sent_at"))
            if prev_first is None or (item_first is not None and item_first < prev_first):
                prev["first_sent_at"] = item_first
            prev_last = _safe_float(prev.get("last_sent_at"))
            item_last = _safe_float(item.get("last_sent_at"))
            if prev_last is None or (item_last is not None and item_last > prev_last):
                prev["last_sent_at"] = item_last
            prev_times = _safe_int(prev.get("times_sent"))
            item_times = _safe_int(item.get("times_sent"))
            prev["times_sent"] = max(prev_times, item_times)
            if not prev.get("message_id") and item.get("message_id"):
                prev["message_id"] = item.get("message_id")
            if not prev.get("last_message_id") and item.get("last_message_id"):
                prev["last_message_id"] = item.get("last_message_id")
            prev["is_followup"] = bool(prev.get("is_followup", False) or item.get("is_followup", False))
            prev_stage = _safe_int(prev.get("followup_stage"))
            item_stage = _safe_int(item.get("followup_stage"))
            if item_stage > prev_stage:
                prev["followup_stage"] = item_stage
            merged_map[key] = prev

    merged = list(merged_map.values())
    merged.sort(
        key=lambda item: (
            float(item.get("last_sent_at") or item.get("first_sent_at") or 0.0),
            str(item.get("message_id") or item.get("last_message_id") or ""),
        ),
        reverse=True,
    )
    return merged


def _merge_conversation_records(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    base = dict(primary or {})
    incoming = dict(secondary or {})

    merged = dict(base)
    for key, value in incoming.items():
        existing_value = merged.get(key)
        is_empty_existing = (
            existing_value is None
            or existing_value == ""
            or existing_value == []
            or existing_value == {}
        )
        if key not in merged or is_empty_existing:
            merged[key] = value

    merged["messages_sent"] = _merge_messages_sent_lists(
        base.get("messages_sent"),
        incoming.get("messages_sent"),
    )

    for ts_field in (
        "last_message_sent_at",
        "last_message_received_at",
        "last_followup_sent_at",
        "first_message_sent_at",
        "last_interaction_at",
        "last_activity_at",
        "captured_at_epoch",
    ):
        a = _safe_float(base.get(ts_field))
        b = _safe_float(incoming.get(ts_field))
        if a is None and b is None:
            continue
        if a is None:
            merged[ts_field] = b
        elif b is None:
            merged[ts_field] = a
        else:
            merged[ts_field] = max(a, b)

    for int_field in ("followup_stage", "unread_count"):
        merged[int_field] = max(_safe_int(base.get(int_field)), _safe_int(incoming.get(int_field)))

    created_a = _safe_float(base.get("created_at"))
    created_b = _safe_float(incoming.get("created_at"))
    if created_a is not None and created_b is not None:
        merged["created_at"] = min(created_a, created_b)
    elif created_a is not None:
        merged["created_at"] = created_a
    elif created_b is not None:
        merged["created_at"] = created_b

    updated_a = _safe_float(base.get("updated_at"))
    updated_b = _safe_float(incoming.get("updated_at"))
    if updated_a is not None and updated_b is not None:
        merged["updated_at"] = max(updated_a, updated_b)
    elif updated_a is not None:
        merged["updated_at"] = updated_a
    elif updated_b is not None:
        merged["updated_at"] = updated_b

    if str(base.get("stage") or "").strip().lower() == _STAGE_CLOSED:
        merged["stage"] = _STAGE_CLOSED
    elif str(incoming.get("stage") or "").strip().lower() == _STAGE_CLOSED:
        merged["stage"] = _STAGE_CLOSED
    merged["prompt_sequence_done"] = bool(
        base.get("prompt_sequence_done", False)
        or incoming.get("prompt_sequence_done", False)
    )
    done_a = _safe_float(base.get("prompt_sequence_done_at"))
    done_b = _safe_float(incoming.get("prompt_sequence_done_at"))
    if done_a is None and done_b is None:
        pass
    elif done_a is None:
        merged["prompt_sequence_done_at"] = done_b
    elif done_b is None:
        merged["prompt_sequence_done_at"] = done_a
    else:
        merged["prompt_sequence_done_at"] = max(done_a, done_b)

    last_seen = str(base.get("last_message_id_seen") or "").strip() or str(incoming.get("last_message_id_seen") or "").strip()
    if last_seen:
        merged["last_message_id_seen"] = last_seen
    last_inbound_seen = str(base.get("last_inbound_id_seen") or "").strip() or str(incoming.get("last_inbound_id_seen") or "").strip()
    if last_inbound_seen:
        merged["last_inbound_id_seen"] = last_inbound_seen

    pending_reply_base = bool(base.get("pending_reply", False))
    pending_reply_incoming = bool(incoming.get("pending_reply", False))
    merged["pending_reply"] = bool(pending_reply_base or pending_reply_incoming)
    pending_inbound_id = (
        str(base.get("pending_inbound_id") or "").strip()
        or str(incoming.get("pending_inbound_id") or "").strip()
    )
    if pending_inbound_id:
        merged["pending_inbound_id"] = pending_inbound_id
    reply_failure_reason = (
        str(base.get("last_reply_failure_reason") or "").strip()
        or str(incoming.get("last_reply_failure_reason") or "").strip()
    )
    if reply_failure_reason:
        merged["last_reply_failure_reason"] = reply_failure_reason
    for ts_field in (
        "last_reply_failed_at",
        "last_send_failed_at",
        "last_open_failed_at",
        "open_backoff_until",
    ):
        ts_a = _safe_float(base.get(ts_field))
        ts_b = _safe_float(incoming.get(ts_field))
        if ts_a is None and ts_b is None:
            continue
        if ts_a is None:
            merged[ts_field] = ts_b
        elif ts_b is None:
            merged[ts_field] = ts_a
        else:
            merged[ts_field] = max(ts_a, ts_b)
    merged["consecutive_open_failures"] = max(
        _safe_int(base.get("consecutive_open_failures")),
        _safe_int(incoming.get("consecutive_open_failures")),
    )

    return merged


def _default_conversation_entry(account: str, thread_id: str, recipient_username: str = "") -> Dict[str, Any]:
    now_ts = time.time()
    return {
        "account": account,
        "thread_id": str(thread_id),
        "thread_id_real": str(thread_id) if _is_probably_web_thread_id(thread_id) else "",
        "thread_href": f"https://www.instagram.com/direct/t/{thread_id}/" if _is_probably_web_thread_id(thread_id) else "",
        "thread_id_api": "",
        "recipient_username": recipient_username,
        "stage": _STAGE_INITIAL,
        "messages_sent": [],
        "last_message_sent_at": None,
        "last_message_received_at": None,
        "last_inbound_id_seen": None,
        "last_message_id_seen": None,
        "pending_reply": False,
        "pending_inbound_id": None,
        "last_reply_failure_reason": None,
        "last_reply_failed_at": None,
        "last_send_failed_at": None,
        "last_open_failed_at": None,
        "consecutive_open_failures": 0,
        "open_backoff_until": None,
        "prompt_sequence_done": False,
        "prompt_sequence_done_at": None,
        "last_message_sender": None,
        "followup_stage": 0,
        "last_followup_sent_at": None,
        "created_at": now_ts,
        "updated_at": now_ts,
    }


def _normalize_snapshot_messages(messages: object, *, client_user_id: object) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    if not isinstance(messages, list):
        return normalized
    self_id = str(client_user_id or "").strip()
    for raw in messages:
        if not isinstance(raw, dict):
            continue
        direction_raw = str(raw.get("direction") or "").strip().lower()
        sender_id = str(raw.get("sender_id") or raw.get("user_id") or "").strip()
        if direction_raw in {"outbound", "outgoing", "sent", "viewer"}:
            direction = "outbound"
        elif direction_raw in {"inbound", "incoming", "received"}:
            direction = "inbound"
        elif direction_raw == "unknown":
            direction = "unknown"
        elif sender_id and self_id and sender_id == self_id:
            direction = "outbound"
        elif sender_id and self_id and sender_id != self_id:
            direction = "inbound"
        else:
            direction = "unknown"
        normalized.append(
            {
                "message_id": str(raw.get("message_id") or raw.get("id") or "").strip(),
                "direction": direction,
                "text": str(raw.get("text") or ""),
                "timestamp_epoch": _safe_float(
                    raw.get("timestamp_epoch")
                    if raw.get("timestamp_epoch") is not None
                    else raw.get("timestamp")
                ),
                "sender_id": sender_id,
            }
        )
    normalized.sort(
        key=lambda item: (
            float(item.get("timestamp_epoch") or 0.0),
            str(item.get("message_id") or ""),
        ),
        reverse=True,
    )
    return normalized


def _conversation_snapshot_signature(conv: Dict[str, Any]) -> tuple[object, ...]:
    messages = conv.get("messages", [])
    if not isinstance(messages, list):
        messages = []
    top = messages[0] if messages and isinstance(messages[0], dict) else {}
    try:
        unread_int = int(conv.get("unread_count") or 0)
    except Exception:
        unread_int = 0
    return (
        str(conv.get("recipient_username") or ""),
        str(conv.get("recipient_id") or ""),
        str(conv.get("thread_id_real") or ""),
        str(conv.get("thread_href") or ""),
        str(conv.get("title") or ""),
        str(conv.get("snippet") or ""),
        unread_int,
        float(conv.get("last_activity_at") or 0.0),
        str(top.get("message_id") or ""),
        float(top.get("timestamp_epoch") or 0.0),
        len(messages),
    )


def _account_conversations_from_memory(account: str, *, refresh: bool = False) -> List[Dict[str, Any]]:
    engine = _load_conversation_engine(refresh=refresh)
    conversations = engine.get("conversations", {})
    if not isinstance(conversations, dict):
        return []
    account_key = f"{_normalize_username(account)}|"
    rows: List[Dict[str, Any]] = []
    for key, value in conversations.items():
        if not isinstance(value, dict):
            continue
        if not str(key).startswith(account_key):
            continue
        row = dict(value)
        row.setdefault("thread_id", str(key).split("|", 1)[-1])
        row["_key"] = str(key)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            float(row.get("last_interaction_at") or row.get("updated_at") or 0.0),
            str(row.get("thread_id") or ""),
        ),
        reverse=True,
    )
    return rows


def _upsert_threads_into_memory(
    client,
    account: str,
    snapshots: List[Dict[str, Any]],
    *,
    source_label: str,
    sync_cursor: str = "",
) -> tuple[int, int]:
    engine = _load_conversation_engine()
    conversations = engine.setdefault("conversations", {})
    now_ts = time.time()
    new_count = 0
    updated_count = 0
    for snapshot in snapshots:
        snapshot_thread_id = str(snapshot.get("thread_id") or "").strip()
        snapshot_thread_id_api = str(snapshot.get("thread_id_api") or snapshot_thread_id).strip()
        snapshot_thread_href = _normalize_thread_href(snapshot.get("thread_href"))
        snapshot_thread_id_real = str(snapshot.get("thread_id_real") or "").strip()
        href_thread_id = _extract_thread_id_from_href(snapshot_thread_href)
        if _is_probably_web_thread_id(href_thread_id):
            snapshot_thread_id_real = href_thread_id
        if not _is_probably_web_thread_id(snapshot_thread_id_real):
            snapshot_thread_id_real = ""

        canonical_thread_id = snapshot_thread_id_real
        if not canonical_thread_id and _is_probably_web_thread_id(snapshot_thread_id):
            canonical_thread_id = snapshot_thread_id
        if not canonical_thread_id and _is_probably_web_thread_id(snapshot_thread_id_api):
            canonical_thread_id = snapshot_thread_id_api
        if not canonical_thread_id:
            canonical_thread_id = snapshot_thread_id or snapshot_thread_id_api
        if not canonical_thread_id:
            continue

        alias_ids: set[str] = set()
        for candidate in (
            snapshot_thread_id,
            snapshot_thread_id_api,
            snapshot_thread_id_real,
            href_thread_id,
        ):
            candidate_value = str(candidate or "").strip()
            if candidate_value:
                alias_ids.add(candidate_value)
        alias_ids.add(canonical_thread_id)

        recipient_username = str(
            snapshot.get("recipient_username")
            or snapshot.get("title")
            or "unknown"
        ).strip() or "unknown"
        recipient_id = str(
            snapshot.get("recipient_id")
            or snapshot.get("recipient_username")
            or snapshot.get("title")
            or ""
        ).strip()
        if not recipient_id:
            recipient_id = recipient_username
        title_value = str(snapshot.get("title") or "").strip()
        if not title_value:
            title_value = str(snapshot.get("recipient_username") or "").strip() or recipient_username
        snippet_value = str(snapshot.get("snippet") or "").strip()
        try:
            unread_int = max(0, int(snapshot.get("unread_count") or 0))
        except Exception:
            unread_int = 0
        snapshot_last_activity = _safe_float(snapshot.get("last_activity_at"))
        canonical_key = _get_conversation_key(account, canonical_thread_id)
        existing_records: List[Dict[str, Any]] = []
        existing_keys: List[str] = []
        for alias_id in alias_ids:
            key = _get_conversation_key(account, alias_id)
            maybe_record = conversations.get(key)
            if isinstance(maybe_record, dict):
                existing_keys.append(key)
                existing_records.append(dict(maybe_record))
        if not existing_records:
            current = _default_conversation_entry(account, canonical_thread_id, recipient_username)
            old_signature = None
            new_count += 1
        else:
            current = dict(existing_records[0])
            for item in existing_records[1:]:
                current = _merge_conversation_records(current, item)
            old_signature = _conversation_snapshot_signature(current)
            if not snippet_value:
                snippet_value = str(current.get("snippet") or "").strip()
            if unread_int <= 0:
                try:
                    unread_int = max(0, int(current.get("unread_count") or 0))
                except Exception:
                    unread_int = 0
            if snapshot_last_activity is None:
                snapshot_last_activity = _safe_float(current.get("last_activity_at"))

        if not snapshot_thread_href:
            existing_href = _normalize_thread_href(current.get("thread_href"))
            if existing_href:
                snapshot_thread_href = existing_href
        if not snapshot_thread_href and _is_probably_web_thread_id(canonical_thread_id):
            snapshot_thread_href = f"https://www.instagram.com/direct/t/{canonical_thread_id}/"
        if not snapshot_thread_id_real and _is_probably_web_thread_id(canonical_thread_id):
            snapshot_thread_id_real = canonical_thread_id

        normalized_messages = _normalize_snapshot_messages(
            snapshot.get("messages"),
            client_user_id=getattr(client, "user_id", ""),
        )
        if not normalized_messages:
            existing_messages = current.get("messages")
            normalized_messages = _normalize_snapshot_messages(
                existing_messages if isinstance(existing_messages, list) else [],
                client_user_id=getattr(client, "user_id", ""),
            )
        latest_inbound_ts: Optional[float] = None
        latest_outbound_ts: Optional[float] = None
        latest_sender = current.get("last_message_sender")
        for msg in normalized_messages:
            ts_value = _safe_float(msg.get("timestamp_epoch"))
            if ts_value is None:
                continue
            msg_direction = str(msg.get("direction") or "").strip().lower()
            if msg_direction == "outbound":
                latest_outbound_ts = ts_value if latest_outbound_ts is None else max(latest_outbound_ts, ts_value)
                latest_sender = "bot"
            elif msg_direction == "inbound":
                latest_inbound_ts = ts_value if latest_inbound_ts is None else max(latest_inbound_ts, ts_value)
                latest_sender = "lead"

        existing_last_sent = _safe_float(current.get("last_message_sent_at"))
        existing_last_received = _safe_float(current.get("last_message_received_at"))
        if latest_outbound_ts is not None:
            merged_sent = latest_outbound_ts if existing_last_sent is None else max(existing_last_sent, latest_outbound_ts)
            current["last_message_sent_at"] = merged_sent
        if latest_inbound_ts is not None:
            merged_received = latest_inbound_ts if existing_last_received is None else max(existing_last_received, latest_inbound_ts)
            current["last_message_received_at"] = merged_received
        if latest_sender:
            current["last_message_sender"] = latest_sender

        existing_last_interaction = _safe_float(current.get("last_interaction_at"))
        interaction_candidates = [snapshot_last_activity, latest_inbound_ts, latest_outbound_ts, existing_last_interaction]
        interaction_values = [value for value in interaction_candidates if value is not None]
        last_interaction_ts = max(interaction_values) if interaction_values else now_ts
        current.update(
            {
                "thread_id": canonical_thread_id,
                "thread_id_real": snapshot_thread_id_real or canonical_thread_id,
                "thread_href": snapshot_thread_href,
                "thread_id_api": snapshot_thread_id_api,
                "recipient_id": recipient_id,
                "recipient_username": recipient_username,
                "title": title_value,
                "snippet": snippet_value,
                "unread_count": unread_int,
                "last_activity_at": snapshot_last_activity,
                "last_interaction_at": last_interaction_ts,
                "source": f"{source_label}:{str(sync_cursor or '').strip()}" if sync_cursor else source_label,
                "captured_at_epoch": now_ts,
                "messages": normalized_messages,
            }
        )
        if not current.get("created_at"):
            current["created_at"] = now_ts
        current["updated_at"] = now_ts
        conversations[canonical_key] = current

        migrated = False
        for old_key in existing_keys:
            if old_key == canonical_key:
                continue
            if old_key in conversations:
                conversations.pop(old_key, None)
                migrated = True

        if old_signature is not None:
            if migrated or _conversation_snapshot_signature(current) != old_signature:
                updated_count += 1

    _save_conversation_engine()
    return new_count, updated_count


def _memory_messages_from_state(state: Dict[str, Any], client_user_id: object) -> List[_MemoryMessageSnapshot]:
    messages_raw = state.get("messages", [])
    normalized = _normalize_snapshot_messages(messages_raw, client_user_id=client_user_id)
    out: List[_MemoryMessageSnapshot] = []
    self_id = str(client_user_id or "").strip()
    for entry in normalized:
        direction_raw = str(entry.get("direction") or "").strip().lower()
        if direction_raw == "outbound":
            direction = "outbound"
        elif direction_raw == "inbound":
            direction = "inbound"
        else:
            direction = "unknown"
        sender_id = str(entry.get("sender_id") or "").strip()
        if not sender_id:
            if direction == "outbound" and self_id:
                sender_id = self_id
            elif direction == "inbound":
                sender_id = "peer"
            else:
                sender_id = ""
        out.append(
            _MemoryMessageSnapshot(
                id=str(entry.get("message_id") or "").strip(),
                user_id=sender_id,
                text=str(entry.get("text") or ""),
                timestamp=_safe_float(entry.get("timestamp_epoch")),
                direction=direction,
            )
        )
    out.sort(
        key=lambda item: (
            float(item.timestamp or 0.0),
            str(item.id or ""),
        ),
        reverse=True,
    )
    return out


def _thread_from_memory_state(client, state: Dict[str, Any]) -> Optional[ThreadLike]:
    raw_thread_id = str(state.get("thread_id") or "").strip()
    thread_id_real = str(state.get("thread_id_real") or "").strip()
    thread_href = _normalize_thread_href(state.get("thread_href"))
    href_thread_id = _extract_thread_id_from_href(thread_href)
    if _is_probably_web_thread_id(href_thread_id):
        thread_id_real = href_thread_id
    if not _is_probably_web_thread_id(thread_id_real) and _is_probably_web_thread_id(raw_thread_id):
        thread_id_real = raw_thread_id
    thread_id = thread_id_real or raw_thread_id
    if not thread_id:
        return None
    if not thread_href and _is_probably_web_thread_id(thread_id):
        thread_href = f"https://www.instagram.com/direct/t/{thread_id}/"

    recipient_username = str(state.get("recipient_username") or "unknown").strip() or "unknown"
    recipient_id = str(state.get("recipient_id") or "").strip() or recipient_username
    snippet = str(state.get("snippet") or "").strip()
    try:
        unread_count = max(0, int(state.get("unread_count") or 0))
    except Exception:
        unread_count = 0
    thread = ThreadLike(
        id=thread_id,
        pk=thread_id,
        users=[UserLike(pk=recipient_id, id=recipient_id, username=recipient_username)],
        unread_count=unread_count,
        link=thread_href,
        title=recipient_username,
        snippet=snippet,
        source_index=-1,
    )
    try:
        client._thread_cache[thread_id] = thread
        if raw_thread_id and raw_thread_id != thread_id:
            client._thread_cache[raw_thread_id] = thread
        client._thread_cache_meta[thread_id] = {
            "title": recipient_username,
            "snippet": snippet,
            "link": thread_href,
            "idx": -1,
            "selector": "memory_state",
            "key_source": "memory_state",
        }
        if raw_thread_id and raw_thread_id != thread_id:
            client._thread_cache_meta[raw_thread_id] = dict(client._thread_cache_meta[thread_id])
    except Exception:
        pass
    return thread


def _conversation_text_from_memory(messages: List[_MemoryMessageSnapshot], client_user_id: object) -> str:
    lines: List[str] = []
    for msg in reversed(messages):
        prefix = "YO" if _message_outbound_status(msg, client_user_id) is True else "ELLOS"
        lines.append(f"{prefix}: {msg.text or ''}")
    return "\n".join(lines)


def _minutes_until_followup_from_memory(
    state: Dict[str, Any],
    now_ts: float,
    followup_schedule_hours: Optional[List[int]],
) -> Optional[int]:
    schedule = [value for value in (followup_schedule_hours or []) if isinstance(value, int) and value > 0]
    schedule = sorted(set(schedule))
    if not schedule:
        return None
    first_sent_at = _safe_float(state.get("first_message_sent_at"))
    if first_sent_at is None:
        messages_sent = state.get("messages_sent", [])
        if isinstance(messages_sent, list):
            first_values = []
            for item in messages_sent:
                if not isinstance(item, dict):
                    continue
                ts_value = _safe_float(item.get("first_sent_at") or item.get("last_sent_at"))
                if ts_value is not None:
                    first_values.append(ts_value)
            if first_values:
                first_sent_at = min(first_values)
    if first_sent_at is None:
        return None
    last_received_at = _safe_float(state.get("last_message_received_at"))
    if last_received_at is not None and last_received_at > first_sent_at:
        return None
    messages_sent = state.get("messages_sent", [])
    followups_sent = 0
    if isinstance(messages_sent, list):
        followups_sent = len(
            [
                item for item in messages_sent
                if isinstance(item, dict) and bool(item.get("is_followup", False))
            ]
        )
    if followups_sent >= len(schedule):
        return None
    required_seconds = float(schedule[followups_sent]) * 3600.0
    elapsed = max(0.0, now_ts - first_sent_at)
    remaining = required_seconds - elapsed
    if remaining <= 0:
        return 0
    return int((remaining + 59.0) // 60.0)


def _memory_action_for_thread(
    state: Dict[str, Any],
    *,
    client_user_id: object,
    now_ts: float,
    max_age_seconds: int,
    followup_schedule_hours: Optional[List[int]],
) -> tuple[str, Dict[str, Any]]:
    if (
        str(state.get("stage") or "").strip().lower() == _STAGE_CLOSED
        or bool(state.get("prompt_sequence_done", False))
    ):
        return "skip", {"reason": "conversation_closed"}
    messages = _memory_messages_from_state(state, client_user_id)
    if not messages:
        return "skip", {"reason": "sin_mensajes"}

    last_inbound = _latest_actionable_inbound_message(messages, client_user_id)
    last_outbound = _latest_outbound_message(messages, client_user_id)
    if last_inbound is not None:
        last_id = getattr(last_inbound, "id", None) or getattr(last_inbound, "message_id", None)
        last_id_str = str(last_id or "").strip()
        inbound_ts = _message_timestamp(last_inbound)
        if not last_id_str:
            return "skip", {"reason": "inbound_sin_message_id"}
        if max_age_seconds and (inbound_ts is None or (now_ts - inbound_ts) > max_age_seconds):
            return "skip", {"reason": "inbound_antiguo"}

        last_inbound_seen = str(state.get("last_inbound_id_seen") or "").strip()
        if not last_inbound_seen:
            # Compatibilidad con memorias antiguas: solo adoptar legacy seen
            # cuando hubo un envío posterior al inbound.
            legacy_seen = str(state.get("last_message_id_seen") or "").strip()
            if legacy_seen and legacy_seen == last_id_str:
                last_sent_at = _safe_float(state.get("last_message_sent_at"))
                sent_after_inbound = False
                if last_sent_at is not None:
                    if inbound_ts is None:
                        sent_after_inbound = True
                    else:
                        sent_after_inbound = last_sent_at >= (inbound_ts - 1.0)
                if sent_after_inbound:
                    last_inbound_seen = legacy_seen

        pending_reply = bool(state.get("pending_reply", False))
        pending_inbound_id = str(state.get("pending_inbound_id") or "").strip()
        if pending_reply and not pending_inbound_id:
            pending_inbound_id = last_id_str
        # Compatibilidad con comportamiento previo: si el último evento del hilo
        # es outbound (bot más reciente que inbound), no tratar ese inbound como nuevo
        # cuando no hay una respuesta pendiente marcada.
        if (
            not pending_reply
            and not last_inbound_seen
            and last_outbound is not None
            and _message_is_newer_than(last_outbound, last_inbound, messages)
        ):
            last_inbound_seen = last_id_str
        open_backoff_until = _safe_float(state.get("open_backoff_until"))
        if (
            pending_reply
            and open_backoff_until is not None
            and open_backoff_until > now_ts
        ):
            remaining = max(1, int((open_backoff_until - now_ts + 59.0) // 60.0))
            return "wait", {"minutes": remaining, "reason": "pending_open_backoff"}

        has_new_inbound = bool(last_id_str and last_id_str != last_inbound_seen)
        has_pending_retry = bool(
            pending_reply and (not pending_inbound_id or pending_inbound_id == last_id_str)
        )
        if has_new_inbound or has_pending_retry:
            return "reply", {
                "last_inbound": last_inbound,
                "messages": messages,
                "latest_inbound_id": last_id_str,
                "retry_pending": has_pending_retry,
            }

        if last_inbound_seen and last_inbound_seen == last_id_str:
            return "skip", {"reason": "inbound_ya_visto"}

    wait_minutes = _minutes_until_followup_from_memory(state, now_ts, followup_schedule_hours)
    if wait_minutes is None:
        return "skip", {"reason": "sin_accion"}
    if wait_minutes <= 0:
        return "followup_due", {"minutes": 0}
    return "wait", {"minutes": wait_minutes}


def full_discovery_initial(client, user: str, threads_target: int) -> List[str]:
    target = max(1, int(threads_target or 1))
    try:
        initial_budget_s = max(
            5.0,
            float(os.getenv("AUTORESPONDER_INITIAL_DISCOVERY_MAX_S", "15")),
        )
    except Exception:
        initial_budget_s = 15.0
    try:
        page_request_timeout_ms_cfg = max(
            1000,
            int(float(os.getenv("AUTORESPONDER_INITIAL_FETCH_TIMEOUT_MS", "4500"))),
        )
    except Exception:
        page_request_timeout_ms_cfg = 4500
    started_at = time.time()
    deadline_ts = started_at + initial_budget_s
    print(style_text("🔎 Sincronización inicial del inbox", color=Fore.CYAN, bold=True))
    page_number = 0
    cursor = ""
    accumulated: Dict[str, Dict[str, Any]] = {}
    page_size = max(10, min(80, target))
    while not STOP_EVENT.is_set() and len(accumulated) < target:
        remaining_s = deadline_ts - time.time()
        if remaining_s <= 0:
            logger.warning(
                "Discovery inicial de @%s alcanzó el límite de %.1fs (acumulado=%s, objetivo=%s).",
                user,
                initial_budget_s,
                len(accumulated),
                target,
            )
            print(
                style_text(
                    f"⏱️ Sincronización inicial alcanzó {round(initial_budget_s, 1)}s; continúo con lo disponible.",
                    color=Fore.YELLOW,
                )
            )
            break
        remaining_ms = max(1000, int(remaining_s * 1000))
        request_timeout_ms = min(page_request_timeout_ms_cfg, remaining_ms)
        total_timeout_ms = min(max(request_timeout_ms, 2000), remaining_ms)
        page_number += 1
        try:
            page_result = client.fetch_inbox_threads_page(
                cursor=cursor,
                limit=page_size,
                message_limit=20,
                request_timeout_ms=request_timeout_ms,
                total_timeout_ms=total_timeout_ms,
                include_visible_href_resolution=False,
            )
        except Exception as exc:
            logger.warning(
                "Discovery inicial por endpoint falló para @%s en página %s: %s",
                user,
                page_number,
                exc,
                exc_info=False,
            )
            break

        page_threads = list(page_result.get("threads") or [])
        for snapshot in page_threads:
            thread_id = str(snapshot.get("thread_id") or "").strip()
            if not thread_id:
                continue
            thread_id_real = str(snapshot.get("thread_id_real") or "").strip()
            thread_href = _normalize_thread_href(snapshot.get("thread_href"))
            username = str(snapshot.get("recipient_username") or "unknown").strip() or "unknown"
            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                print(
                    style_text(
                        f"thread_id_real={thread_id_real or '-'} href={thread_href or '-'} username={username}",
                        color=Fore.WHITE,
                    )
                )
            previous = accumulated.get(thread_id)
            if previous is None:
                accumulated[thread_id] = snapshot
                continue
            previous_ts = _safe_float(previous.get("last_activity_at")) or 0.0
            current_ts = _safe_float(snapshot.get("last_activity_at")) or 0.0
            if current_ts >= previous_ts:
                accumulated[thread_id] = snapshot

        print(
            style_text(
                f"📦 Página {page_number} cargada → {len(page_threads)} threads (acumulado {len(accumulated)})",
                color=Fore.WHITE,
            )
        )
        next_cursor = str(page_result.get("cursor") or "").strip()
        has_more = bool(page_result.get("has_more"))
        if len(accumulated) >= target:
            cursor = next_cursor or cursor
            break
        if not has_more or not next_cursor or next_cursor == cursor:
            cursor = next_cursor or cursor
            break
        cursor = next_cursor

    if not accumulated:
        logger.warning("Sin resultados de endpoint para sincronización inicial de @%s.", user)
        return []

    print(style_text("🧠 Construyendo memoria...", color=Fore.GREEN))
    snapshots = list(accumulated.values())[:target]
    new_count, updated_count = _upsert_threads_into_memory(
        client,
        user,
        snapshots,
        source_label="endpoint_full_discovery",
        sync_cursor=cursor,
    )
    logger.info(
        "Memory full discovery @%s -> nuevos=%s actualizados=%s total=%s cursor=%s",
        user,
        new_count,
        updated_count,
        len(snapshots),
        cursor or "-",
    )
    print(style_text("💾 Memoria persistida correctamente", color=Fore.GREEN))
    print(style_text("✅ Sincronización inicial completada", color=Fore.GREEN, bold=True))
    discovered_ids: List[str] = []
    for snapshot in snapshots:
        thread_id = str(snapshot.get("thread_id") or "").strip()
        if not thread_id or thread_id in discovered_ids:
            continue
        discovered_ids.append(thread_id)
    return discovered_ids


def incremental_discovery_sync(
    client,
    user: str,
    page_limit: int = 30,
) -> tuple[int, int, List[str]]:
    print(style_text("🔄 Verificando nuevos mensajes...", color=Fore.CYAN))
    try:
        page_result = client.fetch_inbox_threads_page(
            cursor="",
            limit=max(5, int(page_limit or 30)),
            message_limit=20,
            include_visible_href_resolution=False,
        )
    except Exception as exc:
        logger.warning("Sync incremental falló para @%s: %s", user, exc, exc_info=False)
        print(style_text("✔ No se detectaron cambios", color=Fore.YELLOW))
        return 0, 0, []

    snapshots = list(page_result.get("threads") or [])
    discovered_ids: List[str] = []
    for snapshot in snapshots:
        thread_id = str(snapshot.get("thread_id") or "").strip()
        if thread_id and thread_id not in discovered_ids:
            discovered_ids.append(thread_id)
    if _AUTORESPONDER_VERBOSE_TECH_LOGS:
        for snapshot in snapshots:
            thread_id_real = str(snapshot.get("thread_id_real") or "").strip()
            thread_href = _normalize_thread_href(snapshot.get("thread_href"))
            username = str(snapshot.get("recipient_username") or "unknown").strip() or "unknown"
            print(
                style_text(
                    f"thread_id_real={thread_id_real or '-'} href={thread_href or '-'} username={username}",
                    color=Fore.WHITE,
                )
            )
    cursor = str(page_result.get("cursor") or "").strip()
    new_count, updated_count = _upsert_threads_into_memory(
        client,
        user,
        snapshots,
        source_label="endpoint_incremental_sync",
        sync_cursor=cursor,
    )
    if new_count > 0:
        print(style_text(f"📥 {new_count} threads nuevos detectados", color=Fore.GREEN))
    if new_count <= 0 and updated_count <= 0:
        print(style_text("✔ No se detectaron cambios", color=Fore.WHITE))
    return new_count, updated_count, discovered_ids


def _ordered_unique_thread_ids(thread_ids: Optional[List[str]]) -> List[str]:
    unique_ids: List[str] = []
    for raw in thread_ids or []:
        thread_id = str(raw or "").strip()
        if not thread_id or thread_id in unique_ids:
            continue
        unique_ids.append(thread_id)
    return unique_ids


def _build_cycle_workset(
    user: str,
    *,
    threads_limit: int,
    discovered_ids: Optional[List[str]] = None,
) -> tuple[List[str], int, int]:
    limit = max(1, int(threads_limit or 1))
    memory_rows = _account_conversations_from_memory(user, refresh=True)
    memory_total = len(memory_rows)
    discovered_ordered = _ordered_unique_thread_ids(discovered_ids)
    pending_ids: List[str] = []
    for row in memory_rows:
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        if not bool(row.get("pending_reply", False)):
            continue
        if thread_id in pending_ids:
            continue
        pending_ids.append(thread_id)

    if discovered_ordered:
        combined = _ordered_unique_thread_ids(pending_ids + discovered_ordered)
        return combined[:limit], len(discovered_ordered), memory_total

    fallback_ids: List[str] = []
    for row in memory_rows:
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id or thread_id in fallback_ids:
            continue
        fallback_ids.append(thread_id)
        if len(fallback_ids) >= limit:
            break
    combined = _ordered_unique_thread_ids(pending_ids + fallback_ids)
    return combined[:limit], 0, memory_total


def decision_cycle_from_memory(
    client,
    user: str,
    state: Dict[str, Dict[str, str]],
    api_key: str,
    system_prompt: str,
    stats: BotStats,
    delay_min: float = 0.0,
    delay_max: float = 0.0,
    max_age_days: int = 7,
    allowed_thread_ids: Optional[set[str]] = None,
    followup_schedule_hours: Optional[List[int]] = None,
    workset_thread_ids: Optional[List[str]] = None,
    threads_limit: Optional[int] = None,
    discovered_ids_count: int = 0,
    memory_total_count: int = 0,
    debug_cycle_summary: bool = False,
) -> None:
    _ = state  # compatibilidad de firma
    debug_cycle = bool(debug_cycle_summary or _AUTORESPONDER_DEBUG_CYCLE_SUMMARY)

    all_account_threads = _account_conversations_from_memory(user, refresh=True)
    memory_total = memory_total_count if memory_total_count > 0 else len(all_account_threads)
    if not all_account_threads:
        print(style_text(f"[Memoria] Sin conversaciones para @{user}", color=Fore.YELLOW))
        return

    workset_ids = _ordered_unique_thread_ids(workset_thread_ids)
    if not workset_ids:
        fallback_limit = max(1, int(threads_limit or len(all_account_threads) or 1))
        workset_ids = [
            str(row.get("thread_id") or "").strip()
            for row in all_account_threads[:fallback_limit]
            if str(row.get("thread_id") or "").strip()
        ]
    thread_map = {
        str(row.get("thread_id") or "").strip(): row
        for row in all_account_threads
        if str(row.get("thread_id") or "").strip()
    }
    account_threads = [thread_map[thread_id] for thread_id in workset_ids if thread_id in thread_map]
    if not account_threads:
        print(
            style_text(
                f"[Memoria] Workset sin threads válidos para @{user}",
                color=Fore.YELLOW,
            )
        )
        return

    print(style_text(f"🧮 Workset de ciclo: {len(account_threads)}", color=Fore.WHITE, bold=True))

    now_ts = time.time()
    max_age_seconds = max(0, int(max_age_days)) * 24 * 3600 if max_age_days is not None else 0
    print(style_text("📊 Analizando estado de los threads...", color=Fore.CYAN, bold=True))
    actions_by_thread: Dict[str, tuple[str, Dict[str, Any]]] = {}
    pending_replies = 0
    pending_followups = 0
    no_action = 0
    for row in account_threads:
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        action, details = _memory_action_for_thread(
            row,
            client_user_id=getattr(client, "user_id", ""),
            now_ts=now_ts,
            max_age_seconds=max_age_seconds,
            followup_schedule_hours=followup_schedule_hours,
        )
        actions_by_thread[thread_id] = (action, details)
        if action == "reply":
            pending_replies += 1
        elif action == "followup_due":
            pending_followups += 1
        else:
            no_action += 1

    print(style_text("📊 Resumen:", color=Fore.WHITE, bold=True))
    print(style_text(f"• Respuestas pendientes: {pending_replies}", color=Fore.WHITE))
    print(style_text(f"• Follow-ups listos: {pending_followups}", color=Fore.WHITE))
    print(style_text(f"• Sin acción: {no_action}", color=Fore.WHITE))

    skip_reason_counts: Dict[str, int] = {}

    def _log_response_skip(
        *,
        motivo_skip: str,
        thread_id: str,
        recipient_username: str,
        has_new_inbound: bool,
        followup_due: bool,
        can_send_result: Optional[bool],
        can_send_reason: str = "",
        intent_open_id: str = "",
        intent_open_href: str = "",
        intent_open_cache_hit: Optional[bool] = None,
    ) -> None:
        conv_state_debug = _get_conversation_state(user, thread_id)
        last_message_received_at = conv_state_debug.get("last_message_received_at")
        last_message_sent_at = conv_state_debug.get("last_message_sent_at")
        last_message_id_seen = conv_state_debug.get("last_message_id_seen")
        flags = {
            "has_new_inbound": bool(has_new_inbound),
            "followup_due": bool(followup_due),
            "can_send_result": can_send_result,
            "force_respond": bool(_FORCE_ALWAYS_RESPOND),
            "force_flag_applies": bool(_FORCE_ALWAYS_RESPOND),
        }
        if intent_open_id or intent_open_href or intent_open_cache_hit is not None:
            flags["intent_open_id"] = intent_open_id
            flags["intent_open_href"] = intent_open_href
            flags["intent_open_cache_hit"] = intent_open_cache_hit
        print(style_text("Acción: Omitido", color=Fore.YELLOW))
        if _AUTORESPONDER_VERBOSE_SKIP_CONSOLE:
            print(style_text(f"  motivo={motivo_skip}", color=Fore.YELLOW))
            print(
                style_text(
                    f"  thread_id={thread_id} recipient_username=@{recipient_username}",
                    color=Fore.YELLOW,
                )
            )
            print(
                style_text(
                    f"  last_message_received_at={last_message_received_at} "
                    f"last_message_sent_at={last_message_sent_at} "
                    f"last_message_id_seen={last_message_id_seen}",
                    color=Fore.YELLOW,
                )
            )
            if intent_open_id or intent_open_href or intent_open_cache_hit is not None:
                print(
                    style_text(
                        f"  intent_open id={intent_open_id or '-'} href={intent_open_href or '-'} cache_hit={intent_open_cache_hit}",
                        color=Fore.YELLOW,
                    )
                )
            print(style_text(f"  flags={flags}", color=Fore.YELLOW))
        _append_message_log(
            {
                "event": "response_skipped",
                "action": "response_skipped",
                "account": user,
                "thread_id": thread_id,
                "recipient_username": recipient_username,
                "motivo_skip": motivo_skip,
                "last_message_received_at": last_message_received_at,
                "last_message_sent_at": last_message_sent_at,
                "last_message_id_seen": last_message_id_seen,
                "flags": flags,
                "can_send_reason": can_send_reason or "",
                "intent_open_id": intent_open_id or "",
                "intent_open_href": intent_open_href or "",
                "intent_open_cache_hit": intent_open_cache_hit,
            }
        )
        skip_reason_counts[motivo_skip] = skip_reason_counts.get(motivo_skip, 0) + 1

    messages_sent_this_cycle = 0
    for idx, row in enumerate(account_threads, start=1):
        if STOP_EVENT.is_set():
            break
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        recipient_username = str(row.get("recipient_username") or "unknown").strip() or "unknown"
        print(style_text(f"🔹 Thread {idx}/{len(account_threads)} → @{recipient_username}", color=Fore.CYAN))

        if allowed_thread_ids is not None and thread_id not in allowed_thread_ids:
            _log_response_skip(
                motivo_skip="thread_no_permitido_en_modo_followup_only",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=False,
                followup_due=False,
                can_send_result=None,
            )
            continue

        action, details = actions_by_thread.get(thread_id, ("skip", {"reason": "sin_plan"}))
        if action == "wait":
            minutes = max(0, int(details.get("minutes") or 0))
            print(style_text(f"Acción: En espera (faltan {minutes} min)", color=Fore.YELLOW))
            continue
        if action == "followup_due":
            _log_response_skip(
                motivo_skip="followup_due_se_procesa_en_bloque_followups",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=False,
                followup_due=True,
                can_send_result=None,
            )
            continue
        if action != "reply":
            reason_skip = str(details.get("reason") or "accion_sin_envio")
            _log_response_skip(
                motivo_skip=f"memory_action_{reason_skip}",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=False,
                followup_due=False,
                can_send_result=None,
            )
            continue

        memory_messages = details.get("messages")
        if not isinstance(memory_messages, list) or not memory_messages:
            _log_response_skip(
                motivo_skip="reply_sin_contexto_de_mensajes_en_memoria",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
            )
            continue
        last_inbound = details.get("last_inbound")
        if last_inbound is None:
            _log_response_skip(
                motivo_skip="reply_sin_last_inbound",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
            )
            continue
        last_id = getattr(last_inbound, "id", None) or getattr(last_inbound, "message_id", None)
        last_id_str = str(details.get("latest_inbound_id") or last_id or "").strip()
        if not last_id_str:
            _log_response_skip(
                motivo_skip="last_inbound_id_vacio",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
            )
            continue
        if _prompt_sequence_done_by_messages(
            system_prompt,
            memory_messages,
            client_user_id=getattr(client, "user_id", ""),
        ):
            _update_conversation_state(
                user,
                thread_id,
                {
                    "stage": _STAGE_CLOSED,
                    "prompt_sequence_done": True,
                    "prompt_sequence_done_at": time.time(),
                    "pending_reply": False,
                    "pending_inbound_id": None,
                    "last_inbound_id_seen": last_id_str,
                },
                recipient_username,
            )
            _log_response_skip(
                motivo_skip="prompt_sequence_done_no_responder_mas",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=False,
                followup_due=False,
                can_send_result=None,
            )
            continue

        row_thread_id_real = str(row.get("thread_id_real") or "").strip()
        row_thread_href = _normalize_thread_href(row.get("thread_href"))
        href_thread_id = _extract_thread_id_from_href(row_thread_href)
        if _is_probably_web_thread_id(href_thread_id):
            row_thread_id_real = href_thread_id
        if not _is_probably_web_thread_id(row_thread_id_real) and _is_probably_web_thread_id(thread_id):
            row_thread_id_real = thread_id
        intent_open_id = row_thread_id_real if _is_probably_web_thread_id(row_thread_id_real) else ""
        intent_open_href = row_thread_href if row_thread_href else ""
        cache_hit = False
        try:
            cache_map = getattr(client, "_thread_cache", {})
            if isinstance(cache_map, dict):
                cache_hit = bool(
                    (intent_open_id and intent_open_id in cache_map)
                    or (href_thread_id and href_thread_id in cache_map)
                    or (thread_id and thread_id in cache_map)
                )
        except Exception:
            cache_hit = False

        if not intent_open_id and not intent_open_href:
            logger.warning(
                "intent_open id=%s href=%s cache_hit=%s account=@%s thread_key=%s recipient=@%s",
                intent_open_id or "-",
                intent_open_href or "-",
                cache_hit,
                user,
                thread_id,
                recipient_username,
            )
            _log_response_skip(
                motivo_skip="thread_id_invalid",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
                intent_open_id=intent_open_id,
                intent_open_href=intent_open_href,
                intent_open_cache_hit=cache_hit,
            )
            _mark_reply_pending(
                user,
                thread_id,
                recipient_username=recipient_username,
                inbound_message_id=last_id_str,
                reason="thread_id_invalid",
                open_failed=True,
            )
            continue

        thread_state = dict(row)
        if intent_open_id:
            thread_state["thread_id"] = intent_open_id
            thread_state["thread_id_real"] = intent_open_id
        if intent_open_href:
            thread_state["thread_href"] = intent_open_href

        thread = _thread_from_memory_state(client, thread_state)
        if thread is None:
            _log_response_skip(
                motivo_skip="thread_invalido_desde_memoria",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
                intent_open_id=intent_open_id,
                intent_open_href=intent_open_href,
                intent_open_cache_hit=cache_hit,
            )
            _mark_reply_pending(
                user,
                thread_id,
                recipient_username=recipient_username,
                inbound_message_id=last_id_str,
                reason="thread_invalido_desde_memoria",
                open_failed=True,
            )
            continue

        try:
            opened = client._open_thread(thread)
        except Exception:
            opened = False
        if not opened and intent_open_href and hasattr(client, "open_thread_by_href"):
            try:
                opened = bool(client.open_thread_by_href(intent_open_href))
            except Exception:
                opened = False
        if not opened:
            logger.warning(
                "intent_open id=%s href=%s cache_hit=%s account=@%s thread_key=%s recipient=@%s",
                intent_open_id or "-",
                intent_open_href or "-",
                cache_hit,
                user,
                thread_id,
                recipient_username,
            )
            _log_response_skip(
                motivo_skip="open_thread_failed",
                thread_id=thread_id,
                recipient_username=recipient_username,
                has_new_inbound=True,
                followup_due=False,
                can_send_result=None,
                intent_open_id=intent_open_id,
                intent_open_href=intent_open_href,
                intent_open_cache_hit=cache_hit,
            )
            _mark_reply_pending(
                user,
                thread_id,
                recipient_username=recipient_username,
                inbound_message_id=last_id_str,
                reason="open_thread_failed",
                open_failed=True,
            )
            continue

        _update_conversation_state(
            user,
            thread_id,
            {
                "consecutive_open_failures": 0,
                "open_backoff_until": None,
            },
            recipient_username,
        )
        print(style_text("Acción: Preparando respuesta", color=Fore.GREEN))
        print(style_text("Esperando delay...", color=Fore.WHITE))
        _sleep_between_replies_sync(delay_min, delay_max, label="reply_delay")

        convo = _conversation_text_from_memory(memory_messages, getattr(client, "user_id", ""))
        conv_state = _get_conversation_state(user, thread_id)
        now_local = time.time()
        last_sent_at = _safe_float(conv_state.get("last_message_sent_at"))
        last_received_at = _safe_float(conv_state.get("last_message_received_at"))
        time_since_last_sent = (now_local - last_sent_at) if last_sent_at else None
        time_since_last_received = (now_local - last_received_at) if last_received_at else None
        stage = _determine_conversation_stage(
            user,
            thread_id,
            has_new_inbound=True,
            time_since_last_sent=time_since_last_sent,
            time_since_last_received=time_since_last_received,
        )
        _update_conversation_state(user, thread_id, {"stage": stage}, recipient_username)

        inbound_text = str(getattr(last_inbound, "text", "") or "")
        status = _classify_response(inbound_text)
        if status and recipient_username:
            ts_value = _message_timestamp(last_inbound)
            log_conversation_status(
                user,
                recipient_username,
                status,
                timestamp=int(ts_value) if ts_value is not None else None,
            )
            if status == "No interesado":
                _update_conversation_state(user, thread_id, {"stage": _STAGE_CLOSED}, recipient_username)
                _log_response_skip(
                    motivo_skip="clasificacion_no_interesado",
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    has_new_inbound=True,
                    followup_due=False,
                    can_send_result=None,
                )
                continue

        phone_numbers = _extract_phone_numbers(inbound_text)
        if not phone_numbers:
            phone_numbers = _extract_phone_numbers(convo)
        calendar_message: Optional[str] = None
        calendar_status_line: Optional[str] = None
        if status != "No interesado":
            if phone_numbers:
                _send_lead_to_gohighlevel(
                    user,
                    recipient_username,
                    convo,
                    phone_numbers,
                    status,
                    api_key,
                )
            calendar_result = _maybe_schedule_google_calendar_event(
                user,
                recipient_username,
                convo,
                phone_numbers,
                status,
                api_key,
            )
            if calendar_result:
                calendar_message, calendar_status_line = calendar_result

        memory_context = _build_memory_context_from_state(
            user,
            thread_id,
            conv_state,
            stage=stage,
            recipient_username=recipient_username,
        )
        try:
            print(style_text("Generando IA...", color=Fore.WHITE))
            reply = _gen_response(api_key, system_prompt, convo, memory_context=memory_context)
            can_send, reason = _can_send_message(
                user,
                thread_id,
                reply,
                latest_inbound_id=last_id_str,
                force=_FORCE_ALWAYS_RESPOND,
            )
            if not can_send:
                logger.info(
                    "Omitiendo envío por memoria @%s -> @%s (thread %s): %s",
                    user,
                    recipient_username,
                    thread_id,
                    reason,
                )
                _log_response_skip(
                    motivo_skip=f"can_send_false:{reason}",
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    has_new_inbound=True,
                    followup_due=False,
                    can_send_result=False,
                    can_send_reason=reason,
                    intent_open_id=intent_open_id,
                    intent_open_href=intent_open_href,
                    intent_open_cache_hit=cache_hit,
                )
                reason_norm = _normalize_text_for_match(reason or "")
                if "conversacion cerrada" in reason_norm or "conversation closed" in reason_norm:
                    _update_conversation_state(
                        user,
                        thread_id,
                        {
                            "pending_reply": False,
                            "pending_inbound_id": None,
                            "last_inbound_id_seen": last_id_str,
                        },
                        recipient_username,
                    )
                else:
                    _mark_reply_pending(
                        user,
                        thread_id,
                        recipient_username=recipient_username,
                        inbound_message_id=last_id_str,
                        reason=f"can_send_false:{reason}",
                        open_failed=False,
                    )
                continue

            stats.record_reply_attempt(user)
            print(style_text("Enviando...", color=Fore.WHITE))
            message_id = client.send_message(thread, reply)
            if not message_id:
                index = stats.record_response_error(user)
                logger.warning(
                    "Envio no verificado por memoria para @%s -> @%s (thread %s)",
                    user,
                    recipient_username,
                    thread_id,
                )
                _print_response_summary(index, user, recipient_username, False)
                _log_response_skip(
                    motivo_skip="send_message_sin_message_id",
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    has_new_inbound=True,
                    followup_due=False,
                    can_send_result=True,
                    intent_open_id=intent_open_id,
                    intent_open_href=intent_open_href,
                    intent_open_cache_hit=cache_hit,
                )
                _mark_reply_pending(
                    user,
                    thread_id,
                    recipient_username=recipient_username,
                    inbound_message_id=last_id_str,
                    reason="send_message_sin_message_id",
                    open_failed=False,
                )
                continue

            _record_message_received(user, thread_id, last_id_str, recipient_username)
            print(style_text("Verificando...", color=Fore.WHITE))
            _record_message_sent(user, thread_id, reply, str(message_id), recipient_username, is_followup=False)
            if calendar_message:
                calendar_id = client.send_message(thread, calendar_message)
                if calendar_id:
                    _record_message_sent(
                        user,
                        thread_id,
                        calendar_message,
                        str(calendar_id),
                        recipient_username,
                        is_followup=False,
                    )
            _update_conversation_state(user, thread_id, {"stage": _STAGE_WAITING}, recipient_username)
            messages_sent_this_cycle += 1
            print(style_text("📤 Mensaje enviado correctamente", color=Fore.GREEN))
            print(style_text("💾 Memoria actualizada", color=Fore.GREEN))
        except Exception as exc:
            setattr(exc, "_autoresponder_sender", user)
            setattr(exc, "_autoresponder_recipient", recipient_username)
            setattr(exc, "_autoresponder_message_attempt", True)
            raise

        index = stats.record_success(user)
        _print_response_summary(index, user, recipient_username, True, calendar_status_line)

    omitted_count = sum(skip_reason_counts.values())
    if debug_cycle:
        print(style_text("🔍 Debug ciclo (memory-first):", color=Fore.CYAN, bold=True))
        print(style_text(f"• memoria_total={memory_total}", color=Fore.WHITE))
        print(style_text(f"• discovered_ids={max(0, int(discovered_ids_count or 0))}", color=Fore.WHITE))
        print(style_text(f"• workset={len(account_threads)}", color=Fore.WHITE))
        print(style_text(f"• enviados={messages_sent_this_cycle}", color=Fore.WHITE))
        print(style_text(f"• omitidos={omitted_count}", color=Fore.WHITE))
        top_skip = sorted(
            skip_reason_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        if top_skip:
            print(style_text("• top_skip_reasons:", color=Fore.WHITE))
            for reason, count in top_skip:
                print(style_text(f"  - {reason}: {count}", color=Fore.WHITE))
        print(
            style_text(
                f"✅ Ciclo terminado: enviados={messages_sent_this_cycle}, "
                f"omitidos={omitted_count}, errores=0, workset={len(account_threads)}",
                color=Fore.GREEN,
                bold=True,
            )
        )

    logger.info(
        "Decision cycle memory-first account=@%s completado total=%s enviados=%s",
        user,
        len(account_threads),
        messages_sent_this_cycle,
    )


def _process_inbox(
    client,
    user: str,
    state: Dict[str, Dict[str, str]],
    api_key: str,
    system_prompt: str,
    stats: BotStats,
    delay_min: float = 0.0,
    delay_max: float = 0.0,
    max_age_days: int = 7,
    allowed_thread_ids: Optional[set[str]] = None,
    threads_limit: int = 20,
) -> None:
    workset_ids, discovered_count, memory_total = _build_cycle_workset(
        user,
        threads_limit=max(1, int(threads_limit or 1)),
        discovered_ids=None,
    )
    decision_cycle_from_memory(
        client,
        user,
        state,
        api_key,
        system_prompt,
        stats,
        delay_min=delay_min,
        delay_max=delay_max,
        max_age_days=max_age_days,
        allowed_thread_ids=allowed_thread_ids,
        followup_schedule_hours=None,
        workset_thread_ids=workset_ids,
        threads_limit=threads_limit,
        discovered_ids_count=discovered_count,
        memory_total_count=memory_total,
        debug_cycle_summary=_AUTORESPONDER_DEBUG_CYCLE_SUMMARY,
    )

def _print_bot_summary(stats: BotStats) -> None:
    def _format_elapsed(seconds: float) -> str:
        total = max(0, int(seconds))
        hh = total // 3600
        mm = (total % 3600) // 60
        ss = total % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    now_ts = time.time()
    account_elapsed = dict(stats.account_elapsed_s)
    for account, start_ts in stats.account_started_at.items():
        account_elapsed[account] = account_elapsed.get(account, 0.0) + max(
            0.0, now_ts - float(start_ts)
        )
    accounts_used = sorted(set(stats.accounts) | set(account_elapsed.keys()))

    print(full_line(color=Fore.MAGENTA))
    print(style_text("=== BOT DETENIDO ===", color=Fore.YELLOW, bold=True))
    print(style_text(f"Alias: {stats.alias}", color=Fore.WHITE, bold=True))
    print(style_text(f"Cuentas usadas: {len(accounts_used)}", color=Fore.CYAN, bold=True))
    print(style_text(f"Respuestas intentadas: {stats.reply_attempts}", color=Fore.WHITE, bold=True))
    print(style_text(f"Respuestas enviadas: {stats.responded}", color=Fore.GREEN, bold=True))
    print(style_text(f"Follow-ups intentados: {stats.followup_attempts}", color=Fore.WHITE, bold=True))
    print(style_text(f"Follow-ups enviados: {stats.followups}", color=Fore.MAGENTA, bold=True))
    print(style_text(f"Errores: {stats.errors}", color=Fore.RED if stats.errors else Fore.GREEN, bold=True))
    print(style_text(f"Tiempo total: {_format_elapsed(now_ts - stats.started_at)}", color=Fore.WHITE, bold=True))
    if accounts_used:
        print(style_text("Tiempo por cuenta:", color=Fore.WHITE, bold=True))
        for account in accounts_used:
            elapsed = account_elapsed.get(account, 0.0)
            print(style_text(f" - @{account}: {_format_elapsed(elapsed)}", color=Fore.WHITE))
    print(full_line(color=Fore.MAGENTA))
    press_enter()


def _is_playwright_client_invalid(client: object) -> bool:
    if client is None:
        return True
    page = getattr(client, "_page", None)
    if page is None:
        return False
    is_closed_fn = getattr(page, "is_closed", None)
    if not callable(is_closed_fn):
        return False
    try:
        return bool(is_closed_fn())
    except Exception:
        return True


def _is_fatal_playwright_runtime_error(exc: Exception) -> bool:
    message = _normalize_text_for_match(str(exc))
    fatal_tokens = (
        "target page, context or browser has been closed",
        "browser has been closed",
        "context closed",
        "page closed",
        "session expired",
        "login requerido",
        "checkpoint",
        "challenge",
        "no disponible para operar",
    )
    return any(token in message for token in fatal_tokens)


def _close_pooled_client(client_pool: Dict[str, object], account: str, *, reason: str) -> None:
    client = client_pool.pop(account, None)
    if client is None:
        return
    print(
        style_text(
            f"🧹 Cerrando sesión Playwright para @{account} (motivo={reason})",
            color=Fore.YELLOW,
        )
    )
    try:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()
    except Exception:
        pass


def _activate_bot() -> None:
    global ACTIVE_ALIAS
    api_key, _ = _load_preferences()
    if not api_key:
        warn("Configura OPENAI_API_KEY antes de activar el bot.")
        press_enter()
        return
    runtime_ok, runtime_reason = _probe_ai_runtime(api_key)
    if not runtime_ok:
        warn(runtime_reason)
        press_enter()
        return

    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias invalido.")
        press_enter()
        return

    targets = _choose_targets(alias)
    if not targets:
        warn("No se encontraron cuentas activas para ese alias.")
        press_enter()
        return

    active_accounts = _filter_valid_sessions(targets, alias=alias)
    if not active_accounts:
        warn("Ninguna cuenta tiene sesion valida.")
        press_enter()
        return

    _, base_system_prompt = _load_preferences(alias)

    settings = refresh_settings()
    delay_min_default = max(1, settings.autoresponder_delay)
    delay_min = ask_int(
        f"Delay minimo entre mensajes (segundos) [{delay_min_default}]: ",
        1,
        default=delay_min_default,
    )
    delay_max = ask_int(
        f"Delay maximo entre mensajes (segundos) [{delay_min}]: ",
        delay_min,
        default=delay_min,
    )
    if delay_max < delay_min:
        delay_max = delay_min
    max_concurrent = ask_int(
        "Cuentas en simultaneo [1]: ",
        1,
        default=1,
    )
    if max_concurrent > len(active_accounts):
        max_concurrent = len(active_accounts)
    print(style_text("[Mientras mas threads, mas lenta sera la busqueda]", color=Fore.YELLOW))
    threads_limit = ask_int(
        "Cuantos threads quieres leer? [20]: ",
        1,
        default=20,
    )
    raw_schedule = ask(
        "Horas de seguimiento (ej: 4,8,12,24) [4,8,12,24]: "
    ).strip()
    followup_schedule_hours = _parse_followup_schedule_hours(
        raw_schedule, default=[4, 8, 12, 24]
    )
    followup_only_raw = ask("Solo seguimiento (S/N) [N]: ").strip().lower()
    followup_only = followup_only_raw in {"s", "si", "y", "yes"}
    max_age_days = 7

    ensure_logging(quiet=settings.quiet, log_dir=settings.log_dir, log_file=settings.log_file)
    reset_stop_event()
    state: Dict[str, Dict[str, str]] = {}
    stats = BotStats(alias=alias)
    ACTIVE_ALIAS = alias
    listener = start_q_listener("Presiona Q para detener el auto-responder.", logger)
    print(style_text(f"Bot activo para {alias} ({len(active_accounts)} cuentas)", color=Fore.GREEN, bold=True))
    logger.info(
        "Auto-responder activo para %d cuentas (alias %s). Delay: %.1fs-%.1fs concurrent=%d followup_only=%s",
        len(active_accounts),
        alias,
        delay_min,
        delay_max,
        max_concurrent,
        "si" if followup_only else "no",
    )

    account_queue = list(active_accounts)
    initial_sync_done: set[str] = set()
    client_pool: Dict[str, object] = {}
    last_discovered_ids_by_user: Dict[str, List[str]] = {}
    try:
        with _suppress_console_noise():
            while not STOP_EVENT.is_set() and account_queue:
                batch = account_queue[:max_concurrent]
                for user in list(batch):
                    if STOP_EVENT.is_set():
                        break
                    if user not in account_queue:
                        continue

                    stats.mark_account_start(user)
                    client = client_pool.get(user)
                    if client is not None and _is_playwright_client_invalid(client):
                        _close_pooled_client(client_pool, user, reason="fatal")
                        initial_sync_done.discard(user)
                        last_discovered_ids_by_user.pop(user, None)
                        client = None

                    if client is not None:
                        print(
                            style_text(
                                f"♻️ Reutilizando sesión para @{user}",
                                color=Fore.GREEN,
                            )
                        )
                    else:
                        try:
                            client = _client_for(user)
                            client_pool[user] = client
                        except Exception as exc:
                            stats.record_error(user)
                            _handle_account_issue(user, exc, active_accounts)
                            if user not in active_accounts:
                                _close_pooled_client(client_pool, user, reason="removed")
                                initial_sync_done.discard(user)
                                last_discovered_ids_by_user.pop(user, None)
                                if user in account_queue:
                                    account_queue.remove(user)
                            stats.mark_account_end(user)
                            continue

                    allowed_thread_ids = None
                    if followup_only:
                        allowed_thread_ids = _followup_allowed_thread_ids(user)

                    try:
                        discovered_ids_for_cycle: List[str] = []
                        if user not in initial_sync_done and not STOP_EVENT.is_set():
                            discovered_ids_for_cycle = full_discovery_initial(
                                client,
                                user,
                                threads_limit,
                            )
                            initial_sync_done.add(user)
                            last_discovered_ids_by_user[user] = _ordered_unique_thread_ids(
                                discovered_ids_for_cycle
                            )
                        discovered_ids_for_cycle = _ordered_unique_thread_ids(
                            last_discovered_ids_by_user.get(user, [])
                        )
                        workset_ids, discovered_count, memory_total = _build_cycle_workset(
                            user,
                            threads_limit=threads_limit,
                            discovered_ids=discovered_ids_for_cycle,
                        )
                        if not followup_only or allowed_thread_ids:
                            account_system_prompt = _resolve_system_prompt_for_user(
                                user,
                                active_alias=alias,
                                fallback_prompt=base_system_prompt,
                            )
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                print(style_text(f"TRACE_CYCLE ENTER decision_cycle_from_memory user=@{user} ts={time.time()}", color=Fore.WHITE))
                            decision_cycle_from_memory(
                                client,
                                user,
                                state,
                                api_key,
                                account_system_prompt,
                                stats,
                                delay_min,
                                delay_max,
                                max_age_days,
                                allowed_thread_ids=allowed_thread_ids if followup_only else None,
                                followup_schedule_hours=followup_schedule_hours,
                                workset_thread_ids=workset_ids,
                                threads_limit=threads_limit,
                                discovered_ids_count=discovered_count,
                                memory_total_count=memory_total,
                                debug_cycle_summary=_AUTORESPONDER_DEBUG_CYCLE_SUMMARY,
                            )
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                print(style_text(f"TRACE_CYCLE EXIT decision_cycle_from_memory user=@{user} ts={time.time()}", color=Fore.WHITE))
                        if not STOP_EVENT.is_set():
                            _inc_new, _inc_updated, incremental_ids = incremental_discovery_sync(
                                client,
                                user,
                                page_limit=max(20, int(threads_limit or 20)),
                            )
                            if incremental_ids:
                                last_discovered_ids_by_user[user] = _ordered_unique_thread_ids(
                                    incremental_ids
                                )
                        if not STOP_EVENT.is_set():
                            followup_start_ts = time.time()
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                print(style_text(f"TRACE_FU ENTER followups user=@{user} ts={followup_start_ts}", color=Fore.WHITE))
                            _process_followups(
                                client,
                                user,
                                api_key,
                                delay_min,
                                delay_max,
                                max_age_days,
                                threads_limit=threads_limit,
                                followup_schedule_hours=followup_schedule_hours,
                                stats=stats,
                            )
                            followup_end_ts = time.time()
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                print(style_text(f"TRACE_FU EXIT followups user=@{user} ts={followup_end_ts} duration_s={round(followup_end_ts - followup_start_ts, 3)}", color=Fore.WHITE))
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:  # pragma: no cover - depende de SDK/insta
                        fatal_client_error = _is_fatal_playwright_runtime_error(exc)
                        if getattr(exc, "_autoresponder_message_attempt", False):
                            index = stats.record_response_error(user)
                            sender = getattr(exc, "_autoresponder_sender", user)
                            recipient = getattr(exc, "_autoresponder_recipient", "-")
                            _print_response_summary(index, sender, recipient, False)
                        else:
                            stats.record_error(user)
                        logger.warning(
                            "Error en auto-responder para @%s: %s",
                            user,
                            exc,
                            exc_info=not settings.quiet,
                        )
                        _handle_account_issue(user, exc, active_accounts)
                        if fatal_client_error:
                            _close_pooled_client(client_pool, user, reason="fatal")
                            initial_sync_done.discard(user)
                            last_discovered_ids_by_user.pop(user, None)
                    finally:
                        stats.mark_account_end(user)

                    if user not in active_accounts and user in account_queue:
                        account_queue.remove(user)
                    if user not in active_accounts:
                        _close_pooled_client(client_pool, user, reason="removed")
                        initial_sync_done.discard(user)
                        last_discovered_ids_by_user.pop(user, None)

                if account_queue and not STOP_EVENT.is_set():
                    account_queue = account_queue[max_concurrent:] + account_queue[:max_concurrent]
                    _sleep_cycle_delay_from_message_delay(delay_min, delay_max)

        if not account_queue:
            warn("No quedan cuentas activas; el bot se detiene.")
            request_stop("sin cuentas activas para responder")

    except KeyboardInterrupt:
        request_stop("interrupcion con CtrlaC")
    finally:
        request_stop("auto-responder detenido")
        for open_user in list(client_pool.keys()):
            _close_pooled_client(client_pool, open_user, reason="stop")
            stats.mark_account_end(open_user)
        if listener:
            listener.join(timeout=0.1)
        ACTIVE_ALIAS = None
        _print_bot_summary(stats)

def _manual_stop() -> None:
    if STOP_EVENT.is_set():
        warn("El bot ya esta� detenido.")
    else:
        request_stop("detencia�n solicitada desde el mena�")
        warn("Si el bot esta� activo, finalizara� al terminar el ciclo en curso.")
    press_enter()


def menu_autoresponder(app_context=None):
    while True:
        _print_menu_header()
        choice = ask("Opcia�n: ").strip()
        if choice == "1":
            _configure_api_key()
        elif choice == "2":
            _configure_prompt()
        elif choice == "3":
            _activate_bot()
        elif choice == "4":
            _followup_menu()
        elif choice == "5":
            _gohighlevel_menu()
        elif choice == "6":
            _google_calendar_menu()
        elif choice == "7":
            _manual_stop()
        elif choice == "8":
            break
        else:
            warn("Opcia�n inva�lida.")
            press_enter()


# ------------- Extensiones para seguimiento con estado persistente ------------
import json as _json_mod_for_state
import os as _os_mod_for_state
from pathlib import Path as _Path_for_state
from typing import Dict as _Dict_for_state, List as _List_for_state, Optional as _Optional_for_state
import time as _time_for_state
from datetime import datetime as _datetime_for_state

# Determinamos la ruta del archivo de estado.  Si 'runtime_base' est� disponible,
# la utilizamos para resolver un directorio consistente; de lo contrario,
# usamos el directorio actual.
try:
    _CONV_STATE_PATH = runtime_base((_Path_for_state(__file__).resolve().parent)) / "storage" / "conversation_state.json"
except Exception:
    _CONV_STATE_PATH = _Path_for_state(__file__).resolve().parent / "storage" / "conversation_state.json"

# Constantes de limpieza (en d�as y segundos)
_CLEANUP_AFTER_DAYS_CLOSED = 90
_CLEANUP_AFTER_DAYS_FINISHED = 14
_CLEANUP_INTERVAL = 24 * 3600  # 24 horas

def _load_conversation_state() -> _Dict_for_state[str, object]:
    # Carga el estado de conversaciones desde el archivo JSON
    try:
        if _os_mod_for_state.path.exists(_CONV_STATE_PATH):
            with open(_CONV_STATE_PATH, "r", encoding="utf-8") as f:
                data = _json_mod_for_state.load(f)
            if isinstance(data, dict):
                data.setdefault("version", "1.0")
                data.setdefault("last_cleanup_ts", 0)
                if not isinstance(data.get("conversations"), dict):
                    data["conversations"] = {}
                return data
    except Exception as exc:
        try:
            logger.warning("No se pudo cargar el estado de conversaciones: %s", exc)
        except Exception:
            pass
    return {"version": "1.0", "last_cleanup_ts": 0, "conversations": {}}

def _save_conversation_state(state: _Dict_for_state[str, object]) -> None:
    # Guarda el estado de conversaciones en el archivo JSON de manera segura
    try:
        _os_mod_for_state.makedirs(_CONV_STATE_PATH.parent, exist_ok=True)
        tmp_path = _CONV_STATE_PATH.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            _json_mod_for_state.dump(state, f, ensure_ascii=False, indent=2)
        _os_mod_for_state.replace(tmp_path, _CONV_STATE_PATH)
    except Exception as exc:
        try:
            logger.warning("No se pudo guardar el estado de conversaciones: %s", exc)
        except Exception:
            pass

def _clean_conversation_state(state: _Dict_for_state[str, object]) -> _Dict_for_state[str, object]:
    # Elimina conversaciones antiguas del estado seg�n las reglas de limpieza
    now_ts = _time_for_state.time()
    last_cleanup_ts = state.get("last_cleanup_ts", 0) or 0
    try:
        last_cleanup_float = float(last_cleanup_ts)
    except Exception:
        last_cleanup_float = 0.0
    if now_ts - last_cleanup_float < _CLEANUP_INTERVAL:
        return state
    conversations = state.get("conversations", {})
    if not isinstance(conversations, dict):
        conversations = {}
    new_conversations: _Dict_for_state[str, object] = {}
    for key, rec in conversations.items():
        try:
            last_contact = rec.get("ultimo_contacto_ts") or rec.get("last_contact_ts")
            last_contact_float = float(last_contact) if last_contact else 0.0
        except Exception:
            last_contact_float = 0.0
        cerrado = bool(rec.get("cerrado"))
        if last_contact_float <= 0:
            new_conversations[key] = rec
            continue
        days_since = (now_ts - last_contact_float) / 86400.0
        if cerrado:
            if days_since <= _CLEANUP_AFTER_DAYS_CLOSED:
                new_conversations[key] = rec
        else:
            if days_since <= _CLEANUP_AFTER_DAYS_FINISHED:
                new_conversations[key] = rec
    state["conversations"] = new_conversations
    state["last_cleanup_ts"] = now_ts
    return state


_FOLLOWUP_STAGE_STRONG_OBJECTION_TOKENS = (
    "no me va a servir",
    "no me sirve",
    "no es para mi",
    "no es para mí",
    "no me interesa",
    "no gracias",
    "paso",
)

_FOLLOWUP_STAGE_SOFT_OBJECTION_TOKENS = (
    "dejame verlo",
    "dejame pensarlo",
    "despues te digo",
    "despues lo veo",
    "pasame info",
    "te aviso",
    "mas adelante",
)

_FOLLOWUP_STAGE_CALL_TOKENS = (
    "llamada",
    "call",
    "reunion",
    "zoom",
    "google meet",
    "15 minutos",
    "15 min",
    "agend",
)

_FOLLOWUP_STAGE_SCHEDULE_HINT = re.compile(
    r"\b(?:hoy|manana|lunes|martes|miercoles|jueves|viernes|sabado|domingo)\b|\ba las \d{1,2}\b|\b\d{1,2}(?::\d{2})?\s?(?:hs|h)\b",
    re.IGNORECASE,
)

_FOLLOWUP_SNIPPET_AGE_RE = re.compile(
    r"(\d+)\s*(seg(?:undo)?s?|sec(?:ond)?s?|s|min(?:uto)?s?|m|hora?s?|h|hr?s?|dia?s?|d|sem(?:ana)?s?|week?s?|w)\b",
    re.IGNORECASE,
)


def _parse_followup_snippet_age_seconds(snippet: str) -> Optional[float]:
    text = _normalize_text_for_match(str(snippet or ""))
    if not text:
        return None
    if "anteayer" in text:
        return 172800.0
    if "ayer" in text or "yesterday" in text:
        return 86400.0
    if "ahora" in text or "just now" in text:
        return 0.0
    match = _FOLLOWUP_SNIPPET_AGE_RE.search(text)
    if not match:
        return None
    try:
        value = max(0, int(match.group(1)))
    except Exception:
        return None
    unit = (match.group(2) or "").strip().lower()
    if not unit:
        return None
    if unit.startswith(("seg", "sec")) or unit == "s":
        return float(value)
    if unit.startswith("min") or unit == "m":
        return float(value * 60)
    if unit.startswith(("hora", "hr")) or unit == "h":
        return float(value * 3600)
    if unit.startswith("dia") or unit == "d":
        return float(value * 86400)
    if unit.startswith(("sem", "week")) or unit == "w":
        return float(value * 7 * 86400)
    return None


def _infer_followup_business_stage(messages: List[object], client_user_id: object) -> int:
    inbound_messages = [
        msg for msg in messages if not _same_user_id(getattr(msg, "user_id", ""), client_user_id)
    ]
    if not inbound_messages:
        return 0

    outbound_messages = [
        msg for msg in messages if _same_user_id(getattr(msg, "user_id", ""), client_user_id)
    ]
    latest_outbound_text = ""
    if outbound_messages:
        latest_outbound_text = _normalize_text_for_match(str(getattr(outbound_messages[0], "text", "") or ""))

    inbound_text_joined = " ".join(
        _normalize_text_for_match(str(getattr(msg, "text", "") or "")) for msg in inbound_messages[:20]
    )
    outbound_text_joined = " ".join(
        _normalize_text_for_match(str(getattr(msg, "text", "") or "")) for msg in outbound_messages[:20]
    )

    if latest_outbound_text and _FOLLOWUP_STAGE_SCHEDULE_HINT.search(latest_outbound_text):
        return 5
    if any(token in inbound_text_joined for token in _FOLLOWUP_STAGE_STRONG_OBJECTION_TOKENS):
        return 4
    if any(token in inbound_text_joined for token in _FOLLOWUP_STAGE_SOFT_OBJECTION_TOKENS):
        return 3
    if any(token in outbound_text_joined for token in _FOLLOWUP_STAGE_CALL_TOKENS):
        return 2
    return 1

def _process_followups_extended(
    client,
    user: str,
    api_key: str,
    delay_min: float = 0.0,
    delay_max: float = 0.0,
    max_age_days: int = 7,
    threads_limit: int = 15,
    followup_schedule_hours: Optional[List[int]] = None,
    stats: Optional[BotStats] = None,
) -> None:
    if _AUTORESPONDER_VERBOSE_TECH_LOGS:
        print(style_text(f"TRACE_FU START _process_followups_extended accounts=@{user} followup_only=n/a ts={_time_for_state.time()}", color=Fore.WHITE))
    alias, entry = _followup_enabled_entry_for(user)
    if not alias or not entry or not entry.get("enabled"):
        if not _FORCE_ALWAYS_FOLLOWUP:
            return
        alias = alias or ACTIVE_ALIAS or user
        entry = _get_followup_entry(alias) if alias else {}
    prompt_text = str(entry.get("prompt") or _DEFAULT_FOLLOWUP_PROMPT)
    if not prompt_text.strip():
        return

    conv_state = _clean_conversation_state(_load_conversation_state())
    history_source = entry.get("history")
    history: _Dict_for_state[str, dict] = dict(history_source) if isinstance(history_source, dict) else {}
    now_ts = _time_for_state.time()
    max_age_seconds = max(0, int(max_age_days)) * 24 * 3600 if max_age_days is not None else 0
    account_norm = _normalize_username(user)
    updated_history = False
    updated_state = False

    memory_threads = _account_conversations_from_memory(user, refresh=True)
    if threads_limit and int(threads_limit) > 0:
        memory_threads = memory_threads[: max(1, int(threads_limit))]
    fu_candidates = len(memory_threads)
    fu_processed = 0
    fu_sent = 0
    fu_omitted = 0
    fu_waiting = 0
    fu_last_heartbeat = _time_for_state.time()
    print(style_text("📊 Analizando estado de los follow-ups...", color=Fore.CYAN, bold=True))
    print(style_text(f"🧮 Workset follow-up: {fu_candidates}", color=Fore.WHITE, bold=True))

    def _print_followup_skip(reason: str) -> None:
        nonlocal fu_omitted
        fu_omitted += 1
        print(style_text("Acción: Omitido", color=Fore.YELLOW))
        if _AUTORESPONDER_VERBOSE_SKIP_CONSOLE:
            print(style_text(f"  motivo={reason}", color=Fore.YELLOW))

    def _print_followup_wait(minutes: int, reason: str) -> None:
        nonlocal fu_waiting
        fu_waiting += 1
        safe_minutes = max(0, int(minutes))
        print(style_text(f"Acción: En espera (faltan {safe_minutes} min)", color=Fore.YELLOW))
        if _AUTORESPONDER_VERBOSE_SKIP_CONSOLE:
            print(style_text(f"  motivo={reason}", color=Fore.YELLOW))

    for row in memory_threads:
        if STOP_EVENT.is_set():
            break
        fu_processed += 1
        now_hb_ts = _time_for_state.time()
        if (fu_processed % 10 == 0 or (now_hb_ts - fu_last_heartbeat) >= 60) and _AUTORESPONDER_VERBOSE_TECH_LOGS:
            print(style_text(f"TRACE_FU HEARTBEAT processed={fu_processed} skipped={max(0, fu_processed - fu_sent)} candidates={fu_candidates} ts={now_hb_ts}", color=Fore.WHITE))
            fu_last_heartbeat = now_hb_ts

        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        recipient_username = str(row.get("recipient_username") or "").strip() or "unknown"
        print(style_text(f"🔹 Follow-up {fu_processed}/{fu_candidates} → @{recipient_username}", color=Fore.CYAN))
        recipient_id = str(row.get("recipient_id") or "").strip() or recipient_username
        thread_snippet = str(row.get("snippet") or "").strip()
        try:
            unread_int = int(row.get("unread_count") or 0)
        except Exception:
            unread_int = 0
        if unread_int > 0:
            _print_followup_skip("pre_skip_unread_thread")
            _append_message_log(
                {
                    "action": "followup_pre_skip",
                    "reason": "pre_skip_unread_thread",
                    "account": user,
                    "thread_id": thread_id,
                    "unread_int": unread_int,
                    "recipient_id": recipient_id,
                    "loop_index": fu_processed,
                }
            )
            continue

        messages = _memory_messages_from_state(row, getattr(client, "user_id", ""))
        if not messages:
            _print_followup_skip("sin_mensajes_en_memoria")
            continue
        all_ts_values = [ts for ts in (_safe_float(getattr(msg, "timestamp", None)) for msg in messages) if ts is not None]
        latest_ts = max(all_ts_values) if all_ts_values else None
        if max_age_seconds and (latest_ts is None or now_ts - latest_ts > max_age_seconds):
            _print_followup_skip("fuera_de_ventana_max_age")
            continue

        outbound_ts_values = [
            _safe_float(getattr(msg, "timestamp", None))
            for msg in messages
            if _message_outbound_status(msg, client.user_id) is True
        ]
        outbound_ts_values = [ts for ts in outbound_ts_values if ts is not None]
        inbound_ts_values = [
            _safe_float(getattr(msg, "timestamp", None))
            for msg in messages
            if _message_outbound_status(msg, client.user_id) is False
        ]
        inbound_ts_values = [ts for ts in inbound_ts_values if ts is not None]
        if not outbound_ts_values:
            _print_followup_skip("skip_no_outbound_messages")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "skip_no_outbound_messages",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                }
            )
            continue

        conv_key = f"{account_norm}|{thread_id}"
        engine_state = _get_conversation_state(user, thread_id)
        if (
            str(engine_state.get("stage") or "").strip().lower() == _STAGE_CLOSED
            or bool(engine_state.get("prompt_sequence_done", False))
        ):
            _print_followup_skip("conversation_closed_or_prompt_sequence_done")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "conversation_closed_or_prompt_sequence_done",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                }
            )
            continue
        last_sent_at = _safe_float(engine_state.get("last_message_sent_at"))
        last_received_at = _safe_float(engine_state.get("last_message_received_at"))
        history_entry = history.get(conv_key, {}) if isinstance(history, dict) else {}

        state_last_sent_ts = _safe_float(last_sent_at)
        history_last_sent_ts = _safe_float(history_entry.get("last_sent_ts")) if isinstance(history_entry, dict) else None
        last_outbound_ts = max(outbound_ts_values)
        last_inbound_ts = max(inbound_ts_values) if inbound_ts_values else None

        fallback_suspected = False
        snippet_age_seconds = _parse_followup_snippet_age_seconds(thread_snippet)
        has_much_older_ts = any((now_ts - ts) > 600 for ts in all_ts_values)
        if abs(now_ts - last_outbound_ts) < 120 and has_much_older_ts:
            fallback_suspected = True
            older_outbound_ts = [ts for ts in outbound_ts_values if ts < (now_ts - 120)]
            if older_outbound_ts:
                last_outbound_ts = max(older_outbound_ts)
        if last_outbound_ts and now_ts - last_outbound_ts < 60:
            replacement_candidates = [ts for ts in (state_last_sent_ts, history_last_sent_ts) if ts]
            replacement_candidates.extend(ts for ts in outbound_ts_values if ts < (now_ts - 120))
            if snippet_age_seconds is not None and snippet_age_seconds >= 60:
                inferred_ts = now_ts - float(snippet_age_seconds)
                if inferred_ts > 0:
                    replacement_candidates.append(inferred_ts)
            replacement_candidates = [ts for ts in replacement_candidates if ts < (now_ts - 60)]
            if replacement_candidates:
                last_outbound_ts = max(replacement_candidates)
                fallback_suspected = True
        if last_outbound_ts and now_ts - last_outbound_ts < 60:
            wait_minutes = max(1, int(((60 - (now_ts - last_outbound_ts)) + 59.0) // 60.0))
            _print_followup_wait(wait_minutes, "skip_last_outbound_lt_60s_or_fallback_suspected")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "skip_last_outbound_lt_60s_or_fallback_suspected",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                    "fallback_suspected": fallback_suspected,
                    "snippet_age_seconds": int(snippet_age_seconds) if snippet_age_seconds is not None else None,
                }
            )
            continue
        if last_inbound_ts and last_inbound_ts > last_outbound_ts:
            _print_followup_skip("skip_latest_is_inbound_or_lead_replied")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "skip_latest_is_inbound_or_lead_replied",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                }
            )
            continue
        if last_received_at and last_sent_at and last_received_at > last_sent_at:
            _print_followup_skip("lead_replied_after_last_bot_message")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "lead_replied_after_last_bot_message",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                }
            )
            continue
        if last_sent_at and now_ts - last_sent_at < _MIN_TIME_FOR_FOLLOWUP:
            remaining_minutes = max(
                1,
                int(((_MIN_TIME_FOR_FOLLOWUP - (now_ts - last_sent_at)) + 59.0) // 60.0),
            )
            _print_followup_wait(remaining_minutes, "last_bot_message_too_recent")
            _append_message_log(
                {
                    "action": "followup_skip",
                    "reason": "last_bot_message_too_recent",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username or str(recipient_id),
                    "seconds_since_last_sent": int(now_ts - last_sent_at),
                }
            )
            continue

        convs = conv_state.setdefault("conversations", {})
        conv_record = convs.get(conv_key)
        if not isinstance(conv_record, dict):
            conv_record = {
                "seguimiento_actual": 0,
                "last_sent_ts": 0.0,
                "last_eval_ts": 0.0,
                "cycle_anchor_ts": 0.0,
                "cycle_followup_count": 0,
                "cycle_last_sent_ts": 0.0,
                "cycle_last_eval_ts": 0.0,
                "etapa_negocio_actual": 0,
                "ultimo_contacto_ts": 0.0,
                "cerrado": False,
                "ultima_actualizacion_ts": now_ts,
            }
            convs[conv_key] = conv_record
            updated_state = True

        business_stage = _infer_followup_business_stage(messages, client.user_id)
        cycle_anchor_ts = float(last_outbound_ts or 0.0)
        if cycle_anchor_ts <= 0:
            continue
        try:
            stored_anchor_ts = float(conv_record.get("cycle_anchor_ts", 0) or 0)
        except Exception:
            stored_anchor_ts = 0.0
        if abs(stored_anchor_ts - cycle_anchor_ts) > 1.0:
            conv_record["cycle_anchor_ts"] = cycle_anchor_ts
            conv_record["cycle_followup_count"] = 0
            conv_record["cycle_last_sent_ts"] = 0.0
            conv_record["cycle_last_eval_ts"] = 0.0
            conv_record["seguimiento_actual"] = 0
            conv_record["last_sent_ts"] = 0.0
            conv_record["cerrado"] = False
            conv_record["ultima_actualizacion_ts"] = now_ts
            updated_state = True
        conv_record["etapa_negocio_actual"] = business_stage
        if conv_record.get("cerrado"):
            _print_followup_skip("followup_cycle_closed")
            continue
        try:
            last_eval_float = float(conv_record.get("cycle_last_eval_ts", conv_record.get("last_eval_ts", 0)) or 0)
        except Exception:
            last_eval_float = 0.0
        if now_ts - last_eval_float < _FOLLOWUP_MIN_INTERVAL:
            remaining_minutes = max(
                1,
                int(((_FOLLOWUP_MIN_INTERVAL - (now_ts - last_eval_float)) + 59.0) // 60.0),
            )
            _print_followup_wait(remaining_minutes, "followup_min_interval")
            continue

        followups_sent = int(conv_record.get("cycle_followup_count", conv_record.get("seguimiento_actual", 0)) or 0)
        last_followup_ts = conv_record.get("cycle_last_sent_ts", conv_record.get("last_sent_ts", 0.0)) or 0.0
        try:
            last_followup_float = float(last_followup_ts)
        except Exception:
            last_followup_float = 0.0

        schedule = [h for h in (followup_schedule_hours or []) if isinstance(h, int) and h > 0]
        schedule = sorted(set(schedule))
        required_hours: Optional[float] = None
        if schedule:
            if followups_sent >= len(schedule):
                conv_record["cerrado"] = True
                conv_record["ultima_actualizacion_ts"] = now_ts
                convs[conv_key] = conv_record
                updated_state = True
                _print_followup_skip("schedule_completed")
                continue
            hours_since_anchor = (now_ts - cycle_anchor_ts) / 3600.0
            required_hours = float(schedule[followups_sent])
            if hours_since_anchor < required_hours:
                remaining_minutes = max(
                    1,
                    int((((required_hours - hours_since_anchor) * 3600.0) + 59.0) // 60.0),
                )
                _print_followup_wait(remaining_minutes, "followup_schedule_not_due")
                continue

        if last_followup_float:
            time_since_last_followup = now_ts - last_followup_float
            min_gap_hours = None
            if schedule:
                next_stage = followups_sent + 1
                if next_stage > 1 and len(schedule) >= next_stage:
                    min_gap_hours = schedule[next_stage - 1] - schedule[next_stage - 2]
            min_gap_known = _MIN_TIME_FOR_FOLLOWUP if min_gap_hours is None else max(1, int(min_gap_hours * 3600))
            if time_since_last_followup < min_gap_known:
                remaining_minutes = max(
                    1,
                    int(((min_gap_known - time_since_last_followup) + 59.0) // 60.0),
                )
                _print_followup_wait(remaining_minutes, "followup_min_gap_not_due")
                continue

        conversation_text = _conversation_text_from_memory(messages[:40], getattr(client, "user_id", ""))
        metadata = {
            "alias": alias,
            "cuenta_origen": f"@{user}",
            "lead": recipient_username or str(recipient_id),
            "seguimientos_previos": followups_sent,
            "seguimientos_previos_en_esta_etapa": followups_sent,
            "etapa_negocio": business_stage,
            "intento_followup_siguiente": followups_sent + 1,
            "horas_objetivo": required_hours if required_hours is not None else "sin_regla",
            "segundos_desde_ultimo_seguimiento": int(now_ts - last_followup_float) if last_followup_float else "nunca",
            "segundos_desde_ultima_respuesta": int(now_ts - last_inbound_ts) if last_inbound_ts else "desconocido",
            "segundos_desde_ultimo_mensaje_enviado": int(now_ts - last_outbound_ts) if last_outbound_ts else "desconocido",
        }
        decision = _followup_decision(api_key, prompt_text, conversation_text, metadata)
        conv_record["last_eval_ts"] = now_ts
        conv_record["cycle_last_eval_ts"] = now_ts
        updated_state = True
        if not decision:
            _print_followup_skip("no_action_from_followup_prompt")
            convs[conv_key] = conv_record
            record = history.get(conv_key, {})
            record["last_eval_ts"] = now_ts
            record["etapa_negocio"] = business_stage
            record["cycle_anchor_ts"] = cycle_anchor_ts
            history[conv_key] = record
            updated_history = True
            continue

        message_text, _stage_requested = decision
        stage_int = followups_sent + 1
        print(style_text("Acción: Preparando follow-up", color=Fore.GREEN))
        if stats is not None:
            stats.record_followup_attempt(user)
        if fu_sent > 0:
            print(style_text("Esperando delay...", color=Fore.WHITE))
            _sleep_between_replies_sync(delay_min, delay_max, label="reply_delay")

        thread = _thread_from_memory_state(client, row)
        if thread is None:
            _print_followup_skip("thread_invalido_desde_memoria")
            continue
        thread_href = _normalize_thread_href(row.get("thread_href"))
        if not thread_href:
            thread_id_for_href = str(getattr(thread, "id", "") or "").strip()
            if _is_probably_web_thread_id(thread_id_for_href):
                thread_href = f"https://www.instagram.com/direct/t/{thread_id_for_href}/"
        try:
            opened = client._open_thread(thread)
            if not opened and thread_href and hasattr(client, "open_thread_by_href"):
                try:
                    opened = bool(client.open_thread_by_href(thread_href))
                except Exception:
                    opened = False
            if not opened:
                _print_followup_skip("open_thread_failed")
                continue
            message_id = client.send_message(thread, message_text)
        except Exception as exc:
            _print_followup_skip("followup_send_exception")
            conv_record["last_error"] = str(exc)
            convs[conv_key] = conv_record
            updated_state = True
            record = history.get(conv_key, {})
            record["last_error"] = str(exc)
            record["etapa_negocio"] = business_stage
            record["cycle_anchor_ts"] = cycle_anchor_ts
            history[conv_key] = record
            updated_history = True
            continue
        if not message_id:
            _print_followup_skip("send_message_sin_message_id")
            continue

        _record_message_sent(
            user,
            thread_id,
            message_text,
            str(message_id),
            recipient_username,
            is_followup=True,
            followup_stage=stage_int,
        )
        _append_message_log(
            {
                "action": "followup_sent",
                "account": user,
                "thread_id": thread_id,
                "lead": recipient_username or str(recipient_id),
                "followup_stage": stage_int,
                "message_id": str(message_id),
                "message_text": message_text,
            }
        )
        fu_sent += 1
        print(style_text("📤 Follow-up enviado correctamente", color=Fore.GREEN))
        conv_record["seguimiento_actual"] = stage_int
        conv_record["cycle_followup_count"] = stage_int
        conv_record["last_sent_ts"] = now_ts
        conv_record["cycle_last_sent_ts"] = now_ts
        conv_record.pop("last_error", None)
        conv_record["etapa_negocio_actual"] = business_stage
        conv_record["ultimo_contacto_ts"] = now_ts
        conv_record["ultima_actualizacion_ts"] = now_ts
        convs[conv_key] = conv_record
        updated_state = True

        record = history.get(conv_key, {})
        record["count"] = stage_int
        record["last_sent_ts"] = now_ts
        record["last_message_id"] = str(message_id)
        record["etapa_negocio"] = business_stage
        record["cycle_anchor_ts"] = cycle_anchor_ts
        record["cycle_followup_count"] = stage_int
        record.pop("last_error", None)
        history[conv_key] = record
        updated_history = True
        if stats is not None:
            stats.record_followup_success(user)
        print(style_text(f"Seguimiento | @{user} -> {_format_handle(recipient_username)} | etapa {stage_int}", color=Fore.MAGENTA))

    if updated_state:
        _save_conversation_state(conv_state)
    if updated_history:
        _set_followup_entry(alias, {"history": history})
    print(style_text("📊 Resumen follow-up:", color=Fore.WHITE, bold=True))
    print(style_text(f"• Procesados: {fu_processed}", color=Fore.WHITE))
    print(style_text(f"• Enviados: {fu_sent}", color=Fore.WHITE))
    print(style_text(f"• En espera: {fu_waiting}", color=Fore.WHITE))
    print(style_text(f"• Omitidos: {fu_omitted}", color=Fore.WHITE))


# Sustituimos la implementación original por la extendida
_process_followups = _process_followups_extended




