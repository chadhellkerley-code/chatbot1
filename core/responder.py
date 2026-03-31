# -*- coding: utf-8 -*-  NUEVA VERSION MATI, SI FUNCIONA ESTO!
import getpass
import health_store
import json
import logging
import os
import re
import random
import sys
import time
import threading
import unicodedata
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from core.accounts import (
    _account_password,
    _store_account_password,
    get_account,
    has_playwright_storage_state,
    is_account_enabled_for_operation,
    list_all,
    mark_connected,
)
from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from config import (
    SETTINGS,
    read_app_config,
    read_env_local,
    refresh_settings,
    update_app_config,
    update_env_local,
)
from proxy_manager import record_proxy_failure, should_retry_proxy
from paths import storage_root
from runtime.runtime import (
    EngineCancellationToken,
    STOP_EVENT,
    bind_stop_token,
    ensure_logging,
    request_stop,
    reset_stop_event,
    restore_stop_token,
    sleep_with_stop,
    start_q_listener,
)
from core.storage import get_auto_state, log_conversation_status, log_sent, save_auto_state
from core.storage_atomic import atomic_append_jsonl, atomic_write_json, load_json_file
from ui import Fore, full_line, style_text
from src.auth.onboarding import login_account_playwright
from src.autoresponder_runtime import (
    AutoresponderRuntimeController,
    PendingHydration,
)
from src.browser_telemetry import log_browser_stage
from src.dm_playwright_client import PlaywrightDMClient, ThreadLike, UserLike
from src.inbox.endpoint_reader import fetch_account_threads_page_from_storage
from utils import ask, ask_int, banner, ok, press_enter, warn

try:  # pragma: no cover - depende de dependencia opcional
    import requests
    from requests import RequestException
except Exception:  # pragma: no cover - fallback si requests no estÃ¡
    requests = None  # type: ignore
    RequestException = Exception  # type: ignore

logger = logging.getLogger(__name__)

ACTIVE_ALIAS: str | None = None
_OPENAI_REPLY_FALLBACK = "Gracias por tu mensaje. Como te puedo ayudar?"
_OPENAI_DEFAULT_MODEL = "gpt-4o-mini"


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "si", "on"}


def _env_int(
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None and str(raw).strip() else int(default)
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


_AUTORESPONDER_VERBOSE_TECH_LOGS = _env_enabled("AUTORESPONDER_VERBOSE_TECH_LOGS", False)
_AUTORESPONDER_DEBUG_CYCLE_SUMMARY = _env_enabled(
    "AUTORESPONDER_DEBUG_CYCLE_SUMMARY",
    False,
)
_AUTORESPONDER_VERBOSE_SKIP_CONSOLE = _env_enabled(
    "AUTORESPONDER_VERBOSE_SKIP_CONSOLE",
    False,
)
_AUTORESPONDER_LOGIN_REQUIRED_PAUSE_SECONDS = _env_int(
    "AUTORESPONDER_LOGIN_REQUIRED_PAUSE_SECONDS",
    12 * 60 * 60,
    minimum=300,
)
_AUTORESPONDER_CHECKPOINT_PAUSE_SECONDS = _env_int(
    "AUTORESPONDER_CHECKPOINT_PAUSE_SECONDS",
    24 * 60 * 60,
    minimum=600,
)
_AUTORESPONDER_RATE_LIMIT_PAUSE_SECONDS = _env_int(
    "AUTORESPONDER_RATE_LIMIT_PAUSE_SECONDS",
    3 * 60 * 60,
    minimum=60,
)
_AUTORESPONDER_RUNTIME_CONTROLLER: Optional[AutoresponderRuntimeController] = None
_AUTORESPONDER_EVENT_PREFIX = "AR_EVENT "
try:
    _AUTORESPONDER_HEARTBEAT_SECONDS = float(
        os.getenv("AUTORESPONDER_HEARTBEAT_SECONDS", "25")
    )
except Exception:
    _AUTORESPONDER_HEARTBEAT_SECONDS = 25.0
_AUTORESPONDER_HEARTBEAT_SECONDS = max(10.0, min(120.0, _AUTORESPONDER_HEARTBEAT_SECONDS))


def _get_autoresponder_runtime_controller() -> AutoresponderRuntimeController:
    global _AUTORESPONDER_RUNTIME_CONTROLLER
    if _AUTORESPONDER_RUNTIME_CONTROLLER is None:
        _AUTORESPONDER_RUNTIME_CONTROLLER = AutoresponderRuntimeController.from_env()
    return _AUTORESPONDER_RUNTIME_CONTROLLER


def _reset_autoresponder_runtime_controller() -> None:
    global _AUTORESPONDER_RUNTIME_CONTROLLER
    _AUTORESPONDER_RUNTIME_CONTROLLER = None


def _running_inside_gui_runtime() -> bool:
    try:
        from PySide6.QtWidgets import QApplication

        app = QApplication.instance()
        return app is not None
    except Exception:
        return False


def _q_listener_enabled_for_autoresponder() -> bool:
    env_value = os.getenv("AUTORESPONDER_ENABLE_Q_LISTENER")
    if env_value is not None:
        return env_value.strip().lower() in {"1", "true", "yes", "si", "on"}
    if _running_inside_gui_runtime():
        return False
    stdin = getattr(sys, "stdin", None)
    if stdin is None:
        return False
    try:
        return bool(stdin.isatty())
    except Exception:
        return False


from core.autoresponder.openai_client import (
    _build_openai_client,
    _openai_generate_text,
    _resolve_ai_model,
    _resolve_ai_runtime,
    _sanitize_generated_message,
)


def _resolve_ai_api_key(env_values: Optional[Dict[str, str]] = None) -> str:
    values = env_values or read_env_local()
    openai_key = (
        values.get("OPENAI_API_KEY")
        or SETTINGS.openai_api_key
        or os.getenv("OPENAI_API_KEY")
        or ""
    )
    return str(openai_key).strip()


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
                "RevisÃ¡ OPENAI_API_KEY y OPENAI_MODEL configurados."
            )
        return False, f"No se pudo validar IA antes de iniciar: {exc}"


_AI_REPLY_DISALLOWED_TOKENS = (
    "```",
    "<json",
    '"enviar"',
    "como asistente",
    "como ia",
    "as an ai",
    "i am an ai",
)

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


_PROMPT_DEFAULT_ALIAS = "default"
_STORAGE_ROOT = storage_root(Path(__file__).resolve().parent.parent)
_PROMPTS_FILE = _STORAGE_ROOT / "autoresponder_prompts.json"
_PROMPTS_STATE: Dict[str, dict] | None = None

_DEFAULT_RESPONDER_STRATEGY_PROMPT = (
    "Analiza el ultimo mensaje del lead y responde SOLO con el nombre de estrategia. "
    "No escribas mensajes finales. "
    "Si no corresponde responder, devolve NO_RESPONDER."
)
_DEFAULT_OBJECTION_PROMPT = (
    "Responde objeciones con texto claro, breve y contextualizado al historial de la conversacion."
)
_DEFAULT_OBJECTION_STRATEGY_NAME = ""
_DEFAULT_FOLLOWUP_STRATEGY_PROMPT = (
    "Analiza el estado del seguimiento y responde SOLO con el nombre de estrategia. "
    "Si no debe enviarse nada, devolve NO_ENVIAR."
)

_PACKS_FILE = _STORAGE_ROOT / "conversational_packs.json"
_PACKS_STATE: Dict[str, dict] | None = None

_ACCOUNT_MEMORY_FILE = _STORAGE_ROOT / "autoresponder_account_memory.json"
_ACCOUNT_MEMORY_STATE: Dict[str, dict] | None = None












_FOLLOWUP_FILE = _STORAGE_ROOT / "followups.json"
_FOLLOWUP_STATE: Dict[str, dict] | None = None
_FOLLOWUP_MIN_INTERVAL = 300
_FOLLOWUP_HISTORY_MAX_AGE = 14 * 24 * 3600

_CONVERSATION_ENGINE_FILE = _STORAGE_ROOT / "conversation_engine.json"
_CONVERSATION_ENGINE_CACHE: Dict[str, dict] | None = None

_MESSAGE_LOG_FILE = _STORAGE_ROOT / "message_log.jsonl"
_MESSAGE_LOG_LOCK = threading.Lock()

_STAGE_INITIAL = "inicial"
_STAGE_INITIAL_ALIASES = frozenset({"initial", "inicial"})
_STAGE_FOLLOWUP = "followup"
_STAGE_WAITING = "waiting"
_STAGE_CLOSED = "closed"
_STAGE_ACTIVE = "active"

_MIN_TIME_BETWEEN_MESSAGES = 60
_MIN_TIME_FOR_FOLLOWUP = 4 * 3600
_MIN_TIME_FOR_REACTIVATION = 24 * 3600

# En producciÃ³n no forzar respuestas: respetar validaciones para evitar envÃ­os incorrectos.
_FORCE_ALWAYS_RESPOND = False
_FORCE_ALWAYS_FOLLOWUP = True
_OPEN_FAIL_BACKOFF_AFTER = max(1, int(os.getenv("AUTORESPONDER_OPEN_FAIL_BACKOFF_AFTER", "3")))
_OPEN_FAIL_BACKOFF_SECONDS = max(
    30.0,
    float(os.getenv("AUTORESPONDER_OPEN_FAIL_BACKOFF_SECONDS", "180")),
)

_FLOW_STAGE_PITCH = "pitch"
_FLOW_STAGE_INVITACION = "invitacion"
_FLOW_STAGE_LINK = "link"
_FLOW_STAGE_FOLLOWUP = "followup"
_FLOW_STAGE_ORDER = (
    _FLOW_STAGE_PITCH,
    _FLOW_STAGE_INVITACION,
    _FLOW_STAGE_LINK,
    _FLOW_STAGE_FOLLOWUP,
)

_FLOW_CONFIG_VERSION = 1
_FLOW_DEFAULT_OBJECTION_MAX_STEPS = 3
_FLOW_DEFAULT_FOLLOWUP_HOURS = (4.0, 24.0, 48.0)
FLOW_CONFIG_REQUIRED = True
_FLOW_OBJECTION_ACTION_ALIASES = {
    "objection_engine",
    "objecion_engine",
    "objection",
    "objecion",
}
_FLOW_ACTION_SPECIAL_ALIASES = {
    "auto_reply": "auto_reply",
    "autorespuesta": "auto_reply",
    "reply_prompt": "auto_reply",
    "followup_text": "followup_text",
    "followup_prompt": "followup_text",
    "objection_engine": "objection_engine",
    "objecion_engine": "objection_engine",
    "objection": "objection_engine",
    "objecion": "objection_engine",
    "no_send": "no_send",
    "no_enviar": "no_send",
    "noenviar": "no_send",
    "no_responder": "no_send",
    "noresponder": "no_send",
    "ninguna": "no_send",
    "none": "no_send",
    "nada": "no_send",
    "omitir": "no_send",
    "skip": "no_send",
    "sin_enviar": "no_send",
    "sin_envio": "no_send",
}
_FLOW_TEXT_ACTION_TYPES = {
    "auto_reply",
    "followup_text",
    "objection_engine",
}
_FLOW_NON_PACK_ACTION_TYPES = set(_FLOW_TEXT_ACTION_TYPES) | {"no_send"}
_OUTBOX_STARTED_TTL_SECONDS = 90.0


class FlowConfigRequiredError(RuntimeError):
    pass


def _normalize_text_token(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFD", raw)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _flow_action_token(value: object) -> str:
    return _normalize_text_token(value).replace(" ", "_")


def _canonical_flow_special_action_type(value: object) -> str:
    return _FLOW_ACTION_SPECIAL_ALIASES.get(_flow_action_token(value), "")


def _canonical_flow_stage_id(value: object) -> str:
    stage_id = str(value or "").strip()
    if not stage_id:
        return ""
    if _normalize_text_token(stage_id) in _STAGE_INITIAL_ALIASES:
        return _STAGE_INITIAL
    return stage_id


def _is_initial_flow_stage_id(value: object) -> bool:
    return _canonical_flow_stage_id(value) == _STAGE_INITIAL


def _flow_stage_ids_match(left: object, right: object) -> bool:
    left_stage_id = _canonical_flow_stage_id(left)
    right_stage_id = _canonical_flow_stage_id(right)
    return bool(left_stage_id and right_stage_id and left_stage_id == right_stage_id)


def _flow_initial_stage_id(flow_config: Dict[str, object]) -> str:
    stages = flow_config.get("stages", [])
    if not isinstance(stages, list):
        return ""
    for raw_stage in stages:
        if not isinstance(raw_stage, dict):
            continue
        stage_id = _canonical_flow_stage_id(raw_stage.get("id"))
        if stage_id == _STAGE_INITIAL:
            return stage_id
    return ""


def _normalize_last_outbound_fingerprint(raw_value: object) -> Dict[str, object]:
    if not isinstance(raw_value, dict):
        return {}
    pack_id = str(raw_value.get("pack_id") or "").strip()
    action_type = str(raw_value.get("action_type") or "").strip().lower()
    payload_hash = str(raw_value.get("payload_hash") or "").strip()
    try:
        action_index = int(raw_value.get("action_index"))
    except Exception:
        action_index = -1
    ts_value = _safe_float(raw_value.get("ts"))
    if ts_value is None or ts_value <= 0:
        ts_value = 0.0
    if not pack_id or not action_type or not payload_hash or action_index < 0 or ts_value <= 0:
        return {}
    return {
        "pack_id": pack_id,
        "action_index": action_index,
        "action_type": action_type,
        "payload_hash": payload_hash,
        "ts": float(ts_value),
    }


def _default_flow_state(stage_id: str = "") -> Dict[str, object]:
    now_ts = time.time()
    return {
        "version": _FLOW_CONFIG_VERSION,
        "stage_id": str(stage_id or "").strip(),
        "last_outbound_ts": None,
        "followup_level": 0,
        "followup_anchor_ts": None,
        "objection_step": 0,
        "last_stage_change_ts": now_ts,
        "outbox": {},
        "last_outbound_fingerprint": {},
        "reconstruction_status": "",
    }


def _normalize_flow_state(
    raw_flow_state: object,
    *,
    fallback_stage_id: str = "",
    last_outbound_ts: Optional[float] = None,
    followup_level_hint: int = 0,
) -> Dict[str, object]:
    canonical_fallback_stage_id = _canonical_flow_stage_id(fallback_stage_id)
    state = _default_flow_state(canonical_fallback_stage_id)
    if isinstance(raw_flow_state, dict):
        state.update(raw_flow_state)
    stage_id = _canonical_flow_stage_id(state.get("stage_id")) or canonical_fallback_stage_id
    state["stage_id"] = stage_id
    try:
        version_int = int(state.get("version") or _FLOW_CONFIG_VERSION)
    except Exception:
        version_int = _FLOW_CONFIG_VERSION
    state["version"] = max(_FLOW_CONFIG_VERSION, version_int)

    try:
        followup_level = int(state.get("followup_level") or 0)
    except Exception:
        followup_level = 0
    if followup_level <= 0:
        followup_level = max(0, int(followup_level_hint or 0))
    state["followup_level"] = max(0, followup_level)

    for ts_field in ("last_outbound_ts", "followup_anchor_ts", "last_stage_change_ts"):
        value = state.get(ts_field)
        try:
            ts_value = float(value) if value is not None else None
        except Exception:
            ts_value = None
        if ts_value is not None and ts_value <= 0:
            ts_value = None
        state[ts_field] = ts_value

    if state.get("last_outbound_ts") is None and last_outbound_ts is not None and last_outbound_ts > 0:
        state["last_outbound_ts"] = float(last_outbound_ts)
    if state.get("followup_anchor_ts") is None and state.get("last_outbound_ts") is not None:
        state["followup_anchor_ts"] = state.get("last_outbound_ts")
    if state.get("last_stage_change_ts") is None:
        state["last_stage_change_ts"] = time.time()

    try:
        objection_step = int(state.get("objection_step") or 0)
    except Exception:
        objection_step = 0
    state["objection_step"] = max(0, objection_step)
    raw_outbox = state.get("outbox")
    normalized_outbox: Dict[str, Dict[str, object]] = {}
    if isinstance(raw_outbox, dict):
        for raw_key, raw_value in raw_outbox.items():
            key_name = str(raw_key or "").strip()
            if not key_name or not isinstance(raw_value, dict):
                continue
            status = str(raw_value.get("status") or "").strip().lower()
            if status not in {"started", "sent"}:
                continue
            item: Dict[str, object] = {"status": status}
            try:
                started_at = float(raw_value.get("started_at")) if raw_value.get("started_at") is not None else None
            except Exception:
                started_at = None
            try:
                sent_at = float(raw_value.get("sent_at")) if raw_value.get("sent_at") is not None else None
            except Exception:
                sent_at = None
            if started_at is not None and started_at > 0:
                item["started_at"] = started_at
            if sent_at is not None and sent_at > 0:
                item["sent_at"] = sent_at
            dom_fingerprint = str(raw_value.get("dom_fingerprint") or "").strip()
            if dom_fingerprint:
                item["dom_fingerprint"] = dom_fingerprint
            message_id = str(raw_value.get("message_id") or "").strip()
            if message_id:
                item["message_id"] = message_id
            baseline_ids = raw_value.get("baseline_ids")
            if isinstance(baseline_ids, list):
                compact_ids = [str(value or "").strip() for value in baseline_ids if str(value or "").strip()]
                if compact_ids:
                    item["baseline_ids"] = compact_ids[:120]
            baseline_signatures = raw_value.get("baseline_signatures")
            if isinstance(baseline_signatures, list):
                compact_signatures = [str(value or "").strip() for value in baseline_signatures if str(value or "").strip()]
                if compact_signatures:
                    item["baseline_signatures"] = compact_signatures[:120]
            normalized_outbox[key_name] = item
    state["outbox"] = normalized_outbox
    state["last_outbound_fingerprint"] = _normalize_last_outbound_fingerprint(
        state.get("last_outbound_fingerprint")
    )
    reconstruction_status = str(state.get("reconstruction_status") or "").strip().lower()
    allowed_reconstruction_status = {
        "reconstructed_low_confidence",
        "legacy_migrated",
        "no_outbound",
        "error_fallback",
    }
    state["reconstruction_status"] = (
        reconstruction_status
        if reconstruction_status in allowed_reconstruction_status
        else ""
    )
    return state


def _merge_flow_outbox_entries(
    base_outbox: object,
    incoming_outbox: object,
) -> Dict[str, Dict[str, object]]:
    base_normalized = _normalize_flow_state(
        {"outbox": base_outbox},
        fallback_stage_id="",
    ).get("outbox")
    incoming_normalized = _normalize_flow_state(
        {"outbox": incoming_outbox},
        fallback_stage_id="",
    ).get("outbox")
    base_items = dict(base_normalized) if isinstance(base_normalized, dict) else {}
    incoming_items = dict(incoming_normalized) if isinstance(incoming_normalized, dict) else {}
    merged: Dict[str, Dict[str, object]] = {key: dict(value) for key, value in base_items.items()}
    for key_name, incoming_entry in incoming_items.items():
        existing_entry = merged.get(key_name)
        if not isinstance(existing_entry, dict):
            merged[key_name] = dict(incoming_entry)
            continue
        existing_status = str(existing_entry.get("status") or "").strip().lower()
        incoming_status = str(incoming_entry.get("status") or "").strip().lower()
        if existing_status == "sent" and incoming_status != "sent":
            continue
        if incoming_status == "sent" and existing_status != "sent":
            merged[key_name] = dict(incoming_entry)
            continue
        if incoming_status == "sent":
            incoming_marker = _safe_float(incoming_entry.get("sent_at")) or 0.0
            existing_marker = _safe_float(existing_entry.get("sent_at")) or 0.0
        else:
            incoming_marker = _safe_float(incoming_entry.get("started_at")) or 0.0
            existing_marker = _safe_float(existing_entry.get("started_at")) or 0.0
        if incoming_marker >= existing_marker:
            merged[key_name] = dict(incoming_entry)
    return merged


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


def _normalize_key(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_alias_key(alias: str) -> str:
    return _normalize_key(alias)


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
                loaded = load_json_file(
                    _CONVERSATION_ENGINE_FILE,
                    {"conversations": {}},
                    label="responder.conversation_engine",
                )
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
        atomic_write_json(_CONVERSATION_ENGINE_FILE, _CONVERSATION_ENGINE_CACHE)
        if _AUTORESPONDER_VERBOSE_TECH_LOGS:
            print(style_text(f"[Persistencia] Archivo {_CONVERSATION_ENGINE_FILE} actualizado fÃ­sicamente.", color=Fore.GREEN))
    except Exception as exc:
        logger.warning("Error guardando conversation_engine.json: %s", exc, exc_info=False)


def _append_message_log(event: Dict[str, Any]) -> None:
    record = dict(event or {})
    record.setdefault("ts", int(time.time()))
    record.setdefault("iso", datetime.utcnow().isoformat())
    try:
        _MESSAGE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _MESSAGE_LOG_LOCK:
            atomic_append_jsonl(_MESSAGE_LOG_FILE, record)
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
    if _AUTORESPONDER_VERBOSE_TECH_LOGS:
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
            "pending_hydration": False,
            "pending_inbound_id": None,
            "last_reply_failure_reason": None,
            "last_reply_failed_at": None,
            "last_send_failed_at": None,
            "last_open_failed_at": None,
            "last_hydration_attempt_at": None,
            "last_hydration_success_at": None,
            "last_hydration_reason": None,
            "pending_pack_run": None,
            "consecutive_open_failures": 0,
            "open_backoff_until": None,
            "prompt_sequence_done": False,
            "prompt_sequence_done_at": None,
            "last_message_sender": None,
            "followup_stage": 0,
            "last_followup_sent_at": None,
            "flow_state": _default_flow_state(""),
            "created_at": time.time(),
            "updated_at": time.time(),
        }
    updates_payload = dict(updates or {})
    incoming_flow_state = updates_payload.pop("flow_state", None)
    current.update(updates_payload)
    flow_state_fallback = ""
    current_flow_state = _normalize_flow_state(
        current.get("flow_state"),
        fallback_stage_id=flow_state_fallback,
        last_outbound_ts=_safe_float(current.get("last_message_sent_at")),
        followup_level_hint=_safe_int(current.get("followup_stage")),
    )
    if incoming_flow_state is not None:
        current_flow_state = _normalize_flow_state(
            incoming_flow_state,
            fallback_stage_id=str(current_flow_state.get("stage_id") or flow_state_fallback),
            last_outbound_ts=_safe_float(current.get("last_message_sent_at")),
            followup_level_hint=_safe_int(current.get("followup_stage")),
        )
    current["flow_state"] = current_flow_state
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
        "pending_hydration": False,
        "last_hydration_reason": "",
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
    try:
        log_sent(
            str(account or "").strip(),
            str(recipient_username or thread_id or "").strip(),
            True,
            "",
            verified=bool(str(message_id or "").strip()),
            source_engine="responder",
        )
    except Exception:
        pass
    _emit_autoresponder_event(
        "FOLLOWUP_SENT" if is_followup else "MESSAGE_SENT",
        account=str(account or "").strip(),
        thread_id=str(thread_id or "").strip(),
        recipient=str(recipient_username or "").strip(),
        message_id=str(message_id or "").strip(),
        followup_stage=followup_stage if is_followup else None,
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
        "pending_hydration": False,
        "last_hydration_reason": "",
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
        updates["pending_pack_run"] = None

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
                return False, "Mensaje ya enviado 3 veces, evitar repeticiÃ³n"
    
    last_sent_at = state.get("last_message_sent_at")
    if last_sent_at and not force:
        time_since_last = now - last_sent_at
        if time_since_last < _MIN_TIME_BETWEEN_MESSAGES:
            remaining = int(_MIN_TIME_BETWEEN_MESSAGES - time_since_last)
            return False, f"Esperar {remaining} segundos antes de enviar otro mensaje"
    
    if state.get("stage") == _STAGE_CLOSED:
        return False, "ConversaciÃ³n cerrada, no enviar mÃ¡s mensajes"
    
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
        data: Dict[str, dict] = {"aliases": {}, "accounts": {}}
        if _FOLLOWUP_FILE.exists():
            try:
                loaded = load_json_file(
                    _FOLLOWUP_FILE,
                    {"aliases": {}, "accounts": {}},
                    label="responder.followups",
                )
                if isinstance(loaded, dict):
                    data.update(loaded)
            except Exception:
                data = {"aliases": {}, "accounts": {}}
        _FOLLOWUP_STATE = _ensure_alias_container(data)
    return _FOLLOWUP_STATE


def _write_followup_state(state: Dict[str, dict]) -> None:
    state = _ensure_alias_container(state)
    _FOLLOWUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(_FOLLOWUP_FILE, state)
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
    entry.setdefault("history", {})
    history = entry.get("history")
    if isinstance(history, dict):
        entry["history"] = _followup_prune_history(history)
    return entry


def _get_account_followup_entry(username: str) -> Dict[str, object]:
    state = _read_followup_state()
    key = _normalize_username(username)
    accounts: Dict[str, dict] = state.get("accounts", {})
    entry = accounts.get(key)
    if not isinstance(entry, dict):
        return {}
    entry.setdefault("alias", username.strip() or username)
    entry.setdefault("enabled", False)
    entry.setdefault("accounts", [])
    entry.setdefault("history", {})
    history = entry.get("history")
    if isinstance(history, dict):
        entry["history"] = _followup_prune_history(history)
    return entry


def _set_followup_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias invÃ¡lido.")
        return
    state = _read_followup_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
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
        status_label = "Activo" if enabled else "Inactivo"
        rows.append(
            f" - {alias_label}: {status_label} | Cuentas: {accounts_label}"
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
    account_entry = _get_account_followup_entry(username)
    if account_entry and account_entry.get("enabled"):
        accounts = account_entry.get("accounts") or []
        if not accounts or _normalize_username(username) in accounts:
            return username, account_entry

    alias_candidates: List[str] = []
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
    print(style_text("Seguimiento automÃ¡tico | Cuentas", color=Fore.CYAN, bold=True))
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
    print("SeleccionÃ¡ las cuentas que usarÃ¡n seguimiento automÃ¡tico:")
    for idx, account in enumerate(available, start=1):
        selected = use_all or account in stored_accounts
        marker = "[x]" if selected else "[ ]"
        print(f" {idx:>2}) {marker} @{account}")
    print("  0) Todas las cuentas del alias")
    choice = ask(
        "NÃºmeros separados por coma (vacÃ­o cancela, 0 = todas): "
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
    tokens = re.split(r"[\s,;]+", choice)
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


def _collect_prompt_text_from_console(title: str) -> str:
    print(
        style_text(
            f"Pega el contenido para {title} y finaliza con una linea <<<END>>>.",
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("Â» ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    return _normalize_system_prompt_text("\n".join(lines)).strip()


def _read_prompt_text_from_file(path_input: str) -> Optional[str]:
    file_path = Path(path_input).expanduser()
    if not file_path.exists():
        warn("El archivo especificado no existe.")
        return None
    suffix = file_path.suffix.strip().lower()
    if suffix not in {".txt", ".csv"}:
        warn("Formato no valido. Solo se permite .txt o .csv")
        return None
    try:
        contents = file_path.read_text(encoding="utf-8")
    except Exception as exc:
        warn(f"No se pudo leer el archivo: {exc}")
        return None
    return _normalize_system_prompt_text(contents).strip()


def _read_system_prompt_from_file(alias=None):
    if alias is None:
        return None
    entry = _get_prompt_entry(str(alias))
    prompt_text = str(entry.get("objection_prompt") or "").strip()
    return prompt_text or None


def _resolve_system_prompt_for_user(
    username: str,
    *,
    active_alias: str | None = None,
    fallback_prompt: str = "",
) -> str:
    candidates: List[str] = []
    username_clean = str(username or "").strip()
    if username_clean:
        candidates.append(username_clean)

    account = get_account(username_clean) or {}
    account_alias = str(account.get("alias") or "").strip()
    active_alias_clean = str(active_alias or "").strip()
    active_alias_is_all = _normalize_alias_key(active_alias_clean) == "all"

    if account_alias:
        candidates.append(account_alias)
    if active_alias_clean and not active_alias_is_all:
        candidates.append(active_alias_clean)
    candidates.append("ALL")
    candidates.append(_PROMPT_DEFAULT_ALIAS)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        prompt_text = _read_system_prompt_from_file(candidate)
        if isinstance(prompt_text, str) and prompt_text.strip():
            return prompt_text.strip()
    return str(fallback_prompt or "").strip()


def _edit_prompt_block(
    alias: str,
    *,
    field_name: str,
    title: str,
    description_lines: List[str],
    include_objection_strategy_name: bool = False,
    target_aliases: Optional[List[str]] = None,
) -> None:
    scoped_aliases: List[str] = []
    for raw_alias in target_aliases or [alias]:
        alias_value = str(raw_alias or "").strip()
        if not alias_value:
            continue
        if alias_value not in scoped_aliases:
            scoped_aliases.append(alias_value)
    if not scoped_aliases:
        scoped_aliases = [alias.strip() or _PROMPT_DEFAULT_ALIAS]
    reference_alias = (
        _PROMPT_DEFAULT_ALIAS if _PROMPT_DEFAULT_ALIAS in scoped_aliases else scoped_aliases[0]
    )
    alias_label = alias
    if len(scoped_aliases) > 1:
        alias_label = f"{alias} ({len(scoped_aliases)} aliases)"

    def _apply_updates(updates: Dict[str, object]) -> None:
        for target_alias in scoped_aliases:
            _set_prompt_entry(target_alias, updates)

    entry = _get_prompt_entry(reference_alias)
    current_saved = str(entry.get(field_name) or "").strip()
    draft_text = current_saved
    while True:
        banner()
        latest_entry = _get_prompt_entry(reference_alias)
        saved_text = str(latest_entry.get(field_name) or "").strip()
        print(style_text(title, color=Fore.CYAN, bold=True))
        print(f"Alias: {alias_label}")
        for line in description_lines:
            print(line)
        print(full_line(color=Fore.BLUE))
        print(f"Guardado actual: {_preview_prompt(saved_text)}")
        print(f"Borrador actual: {_preview_prompt(draft_text)}")
        print(f"Longitud borrador: {len(draft_text)} caracteres.")
        if include_objection_strategy_name:
            strategy_name = str(latest_entry.get("objection_strategy_name") or "").strip()
            strategy_label = strategy_name or "(sin definir)"
            print(f"Estrategia de objecion asociada: {strategy_label}")
        print(full_line(color=Fore.BLUE))
        print("1) Escribir manualmente")
        print("2) Cargar desde archivo (.txt / .csv)")
        print("3) Eliminar prompt actual")
        print("4) Volver")
        choice = ask("Opcion: ").strip()
        if choice == "1":
            draft_text = _collect_prompt_text_from_console(title)
            if not draft_text:
                warn("El borrador quedo vacio.")
            else:
                updates: Dict[str, object] = {field_name: draft_text}
                if include_objection_strategy_name:
                    current_name = str(latest_entry.get("objection_strategy_name") or "").strip()
                    raw_name = ask(
                        f"Nombre de estrategia para objecion (Enter mantiene '{current_name or '(vacio)'}', '-' limpia): "
                    ).strip()
                    if raw_name == "-":
                        updates["objection_strategy_name"] = ""
                    elif raw_name:
                        updates["objection_strategy_name"] = raw_name
                _apply_updates(updates)
                ok(f"Prompt guardado. Longitud: {len(draft_text)} caracteres.")
            press_enter()
            continue
        if choice == "2":
            path_input = ask("Ruta del archivo (.txt/.csv): ").strip()
            if not path_input:
                warn("No se realizaron cambios.")
                press_enter()
                continue
            loaded_text = _read_prompt_text_from_file(path_input)
            if loaded_text is None:
                press_enter()
                continue
            draft_text = loaded_text
            updates: Dict[str, object] = {field_name: draft_text}
            if include_objection_strategy_name:
                current_name = str(latest_entry.get("objection_strategy_name") or "").strip()
                raw_name = ask(
                    f"Nombre de estrategia para objecion (Enter mantiene '{current_name or '(vacio)'}', '-' limpia): "
                ).strip()
                if raw_name == "-":
                    updates["objection_strategy_name"] = ""
                elif raw_name:
                    updates["objection_strategy_name"] = raw_name
            _apply_updates(updates)
            ok(f"Prompt cargado y guardado. Longitud: {len(draft_text)} caracteres.")
            press_enter()
            continue
        if choice == "3":
            confirm = _prompt_bool("Eliminar prompt actual", default=False)
            if not confirm:
                warn("Operacion cancelada.")
                press_enter()
                continue
            _apply_updates({field_name: ""})
            draft_text = ""
            ok("Prompt actual eliminado.")
            press_enter()
            continue
        if choice == "4":
            return
        warn("Opcion invalida.")
        press_enter()


def _followup_disable() -> None:
    banner()
    print(style_text("Seguimiento automÃ¡tico | Desactivar", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _followup_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _prompt_alias_selection()
    if not alias:
        return
    entry = _get_followup_entry(alias)
    if not entry or not entry.get("enabled"):
        warn("El seguimiento ya estÃ¡ inactivo para ese alias.")
        press_enter()
        return
    _set_followup_entry(alias, {"enabled": False, "history": {}})
    ok("Seguimiento desactivado para ese alias.")
    press_enter()


def _followup_menu() -> None:
    while True:
        banner()
        print(style_text("Seguimiento automÃ¡tico", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _followup_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Configurar cuentas con seguimiento")
        print("2) Desactivar seguimiento para un alias")
        print("3) Volver")
        choice = ask("OpciÃ³n: ").strip()
        if choice == "1":
            _followup_configure_accounts()
        elif choice == "2":
            _followup_disable()
        elif choice == "3":
            break
        else:
            warn("OpciÃ³n invÃ¡lida.")
            press_enter()


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
    "informaciÃ³n",
    "detalle",
    "detalles",
    "precio",
    "costo",
    "mas info",
    "mÃ¡s info",
)
_DOUBT_KEYWORDS = (
    "duda",
    "dudas",
    "no entiendo",
    "no entendi",
    "no entendi bien",
    "no comprendo",
    "como funciona",
    "como es",
    "que incluye",
    "explicame",
    "explicame mejor",
)
_CALL_KEYWORDS = (
    "agenda",
    "agendar",
    "llamar",
    "llamada",
    "cita",
    "call",
    "reunion",
    "reuniÃ³n",
)
_DEFAULT_LEAD_TAG = "Lead sin clasificar"
def _format_handle(value: str | None) -> str:
    if not value:
        return "@-"
    value = value.strip()
    if value.startswith("@"):
        return value
    return f"@{value}"


def _print_response_summary(
    index: int, sender: str, recipient: str, success: bool, extra: Optional[str] = None
) -> None:
    status = "OK" if success else "ERROR"
    log_level = logging.INFO if success else logging.ERROR
    logger.log(
        log_level,
        "Respuesta %s | %s -> %s | %s",
        index,
        _format_handle(sender),
        _format_handle(recipient),
        status,
    )
    if extra:
        logger.log(log_level, "%s", extra)


@contextmanager
def _suppress_console_noise() -> None:
    root = logging.getLogger()
    stream_handlers: list[logging.Handler] = [
        handler
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
    ]
    original_levels = [handler.level for handler in stream_handlers]
    noisy_loggers = [
        logging.getLogger("httpx"),
        logging.getLogger("httpcore"),
        logging.getLogger("openai"),
    ]
    noisy_levels = [logger_obj.level for logger_obj in noisy_loggers]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            for handler in stream_handlers:
                handler.setLevel(logging.CRITICAL + 1)
            for logger_obj in noisy_loggers:
                logger_obj.setLevel(max(logging.WARNING, logger_obj.level or logging.NOTSET))
            yield
        finally:
            for handler, level in zip(stream_handlers, original_levels):
                handler.setLevel(level)
            for logger_obj, level in zip(noisy_loggers, noisy_levels):
                logger_obj.setLevel(level)


def _normalize_text_for_match(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


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


def _sleep_between_replies_for_account(
    account_id: str,
    delay_min: float,
    delay_max: float,
    *,
    label: str = "reply_delay",
    apply_safezone_multiplier: bool = True,
) -> None:
    base_delay = _random_delay_seconds(delay_min, delay_max)
    if base_delay <= 0:
        return
    multiplier = _safezone_delay_multiplier(account_id) if apply_safezone_multiplier else 1.0
    final_delay = max(0.0, float(base_delay) * float(multiplier))
    if final_delay <= 0:
        return
    logger.info(
        "%s sleep=%.1fs base=%.1fs mult=%.2f account=@%s",
        label,
        final_delay,
        base_delay,
        multiplier,
        account_id,
    )
    sleep_with_stop(final_delay)


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

    # Pausa de ciclo derivada del delay configurado para evitar ritmo robÃ³tico.
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
            f"â³ Pausa de ciclo: {round(delay, 1)}s (basada en delay configurado)",
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
    for keyword in _NEGATIVE_KEYWORDS:
        if keyword == "no":
            continue
        if _contains_token(norm, keyword):
            return "No interesado"
    for keyword in _POSITIVE_KEYWORDS:
        if _contains_token(norm, keyword):
            return "Interesado"
    for keyword in _DOUBT_KEYWORDS:
        if _contains_token(norm, keyword):
            return None
    if _contains_token(norm, "no"):
        return "No interesado"
    return None


class FlowEngine:
    def __init__(self, flow_config: Dict[str, object]) -> None:
        normalized = _normalize_flow_config(flow_config)
        self.flow_config = normalized
        stages = normalized.get("stages", [])
        self.stages: List[Dict[str, object]] = list(stages if isinstance(stages, list) else [])
        self.stage_order: List[str] = []
        self.stage_map: Dict[str, Dict[str, object]] = {}
        for raw_stage in self.stages:
            if not isinstance(raw_stage, dict):
                continue
            stage_id = str(raw_stage.get("id") or "").strip()
            if not stage_id or stage_id in self.stage_map:
                continue
            self.stage_order.append(stage_id)
            self.stage_map[stage_id] = raw_stage
        entry_stage_id = str(normalized.get("entry_stage_id") or "").strip()
        if entry_stage_id not in self.stage_map:
            entry_stage_id = self.stage_order[0] if self.stage_order else ""
        self.entry_stage_id = entry_stage_id
        self.initial_stage_id = _flow_initial_stage_id(normalized)
        self.has_initial_stage = bool(self.initial_stage_id)
        self._stage_index = {stage_id: idx for idx, stage_id in enumerate(self.stage_order)}

    def _stage_for(self, stage_id: str) -> Dict[str, object]:
        stage = self.stage_map.get(str(stage_id or "").strip())
        if stage is None and self.entry_stage_id:
            stage = self.stage_map.get(self.entry_stage_id)
        return dict(stage or {})

    def _is_objection_action(self, action_type: str, thread_context: Dict[str, object]) -> bool:
        action_token = _flow_action_token(action_type)
        if not action_token:
            return False
        if action_token in _FLOW_OBJECTION_ACTION_ALIASES:
            return True
        objection_strategy_name = str(thread_context.get("objection_strategy_name") or "").strip()
        if objection_strategy_name and action_token == _flow_action_token(objection_strategy_name):
            return True
        return False

    def classify_inbound_relevance(self, thread_context: Dict[str, object]) -> Dict[str, object]:
        latest_inbound_id = str(thread_context.get("latest_inbound_id") or "").strip()
        last_inbound_seen = str(thread_context.get("last_inbound_id_seen") or "").strip()
        pending_reply = bool(thread_context.get("pending_reply", False))
        pending_inbound_id = str(thread_context.get("pending_inbound_id") or "").strip()
        if latest_inbound_id and latest_inbound_id != last_inbound_seen:
            return {
                "relevant": True,
                "reason": "new_inbound",
                "latest_inbound_id": latest_inbound_id,
            }
        if pending_reply and (not pending_inbound_id or pending_inbound_id == latest_inbound_id):
            return {
                "relevant": True,
                "reason": "pending_retry",
                "latest_inbound_id": latest_inbound_id or pending_inbound_id,
            }
        return {
            "relevant": False,
            "reason": "no_new_inbound",
            "latest_inbound_id": latest_inbound_id,
        }

    def classify_inbound_type(self, message: str) -> str:
        norm = _normalize_text_for_match(str(message or ""))
        if not norm:
            return "neutral"
        for keyword in _NEGATIVE_KEYWORDS:
            if keyword == "no":
                continue
            if _contains_token(norm, keyword):
                return "negative"
        for keyword in _DOUBT_KEYWORDS:
            if _contains_token(norm, keyword):
                return "doubt"
        for keyword in _POSITIVE_KEYWORDS:
            if _contains_token(norm, keyword):
                return "positive"
        if _contains_token(norm, "no"):
            return "negative"
        return "neutral"

    def compute_followup_due(self, thread_context: Dict[str, object]) -> Dict[str, object]:
        flow_state = _normalize_flow_state(
            thread_context.get("flow_state"),
            fallback_stage_id=self.entry_stage_id,
            last_outbound_ts=_safe_float(thread_context.get("last_outbound_ts")),
            followup_level_hint=_safe_int(thread_context.get("followup_level")),
        )
        has_inbound_history = bool(thread_context.get("has_inbound_history", False))
        preconversation_initial_placeholder = bool(thread_context.get("preconversation_initial_placeholder", False))
        if not self.has_initial_stage and preconversation_initial_placeholder and not has_inbound_history:
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "preconversation_without_initial_stage",
                "action_type": "",
                "followup_level": int(flow_state.get("followup_level") or 0),
            }
        stage = self._stage_for(str(flow_state.get("stage_id") or self.entry_stage_id))
        followups = stage.get("followups")
        followup_list = list(followups) if isinstance(followups, list) else []
        if not followup_list:
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "stage_without_followups",
                "action_type": "",
                "followup_level": int(flow_state.get("followup_level") or 0),
            }

        inbound_relevance = bool(thread_context.get("inbound_relevant", False))
        if inbound_relevance:
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "new_inbound_present",
                "action_type": "",
                "followup_level": int(flow_state.get("followup_level") or 0),
            }

        now_ts = _safe_float(thread_context.get("now_ts")) or time.time()
        level = max(0, _safe_int(flow_state.get("followup_level")))
        if level >= len(followup_list):
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "followup_plan_completed",
                "action_type": "",
                "followup_level": level,
            }

        followup_entry = followup_list[level] if isinstance(followup_list[level], dict) else {}
        delay_hours = _safe_float(followup_entry.get("delay_hours")) or 0.0
        delay_seconds = max(0.0, delay_hours * 3600.0)
        anchor_ts = (
            _safe_float(flow_state.get("followup_anchor_ts"))
            or _safe_float(flow_state.get("last_outbound_ts"))
            or _safe_float(thread_context.get("last_outbound_ts"))
        )
        if anchor_ts is None:
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "missing_anchor",
                "action_type": "",
                "followup_level": level,
            }
        elapsed = now_ts - anchor_ts
        if elapsed < delay_seconds:
            return {
                "due": False,
                "wait_seconds": max(0, int(delay_seconds - elapsed)),
                "reason": "followup_delay_not_due",
                "action_type": "",
                "followup_level": level,
            }

        action_type = str(followup_entry.get("action_type") or stage.get("action_type") or "").strip()
        if not action_type:
            return {
                "due": False,
                "wait_seconds": 0,
                "reason": "followup_without_action_type",
                "action_type": "",
                "followup_level": level,
            }
        return {
            "due": True,
            "wait_seconds": 0,
            "reason": "followup_due",
            "action_type": action_type,
            "followup_level": level,
            "followup_delay_hours": delay_hours,
            "followup_entry": followup_entry,
        }

    def compute_next_stage(
        self,
        *,
        current_stage_id: str,
        inbound_type: str,
        transitions: Dict[str, object],
    ) -> str:
        current_id = str(current_stage_id or self.entry_stage_id).strip()
        if current_id not in self.stage_map:
            current_id = self.entry_stage_id
        target = str(transitions.get(inbound_type) or current_id).strip()
        if target not in self.stage_map:
            return current_id
        current_index = self._stage_index.get(current_id, 0)
        target_index = self._stage_index.get(target, current_index)
        if target_index < current_index:
            return current_id
        if target_index > current_index + 1:
            allowed_index = min(current_index + 1, len(self.stage_order) - 1)
            return self.stage_order[allowed_index]
        return target

    def evaluate(self, thread_context: Dict[str, object]) -> Dict[str, object]:
        flow_state = _normalize_flow_state(
            thread_context.get("flow_state"),
            fallback_stage_id=self.entry_stage_id,
            last_outbound_ts=_safe_float(thread_context.get("last_outbound_ts")),
            followup_level_hint=_safe_int(thread_context.get("followup_level")),
        )
        stage_id = str(flow_state.get("stage_id") or self.entry_stage_id).strip()
        if stage_id not in self.stage_map and self.entry_stage_id:
            stage_id = self.entry_stage_id
            flow_state["stage_id"] = stage_id
        stage = self._stage_for(stage_id)
        transitions = stage.get("transitions")
        transition_map = dict(transitions) if isinstance(transitions, dict) else {}
        inbound_relevance = self.classify_inbound_relevance(thread_context)
        inbound_relevant = bool(inbound_relevance.get("relevant", False))
        inbound_type = self.classify_inbound_type(str(thread_context.get("inbound_text") or ""))

        if inbound_relevant:
            objection_cfg_raw = stage.get("post_objection")
            objection_cfg = dict(objection_cfg_raw) if isinstance(objection_cfg_raw, dict) else {}
            objection_enabled = bool(objection_cfg.get("enabled", False))
            objection_step = max(0, _safe_int(flow_state.get("objection_step")))
            objection_action = str(objection_cfg.get("action_type") or "").strip()
            objection_max_steps = max(
                1,
                _safe_int(objection_cfg.get("max_steps") or _FLOW_DEFAULT_OBJECTION_MAX_STEPS),
            )
            if (
                objection_enabled
                and objection_action
                and inbound_type in {"negative", "doubt"}
                and objection_step < objection_max_steps
            ):
                return {
                    "decision": "reply",
                    "reason": "objection_step_due",
                    "stage_id": stage_id,
                    "next_stage_id": stage_id,
                    "action_type": objection_action,
                    "inbound_type": inbound_type,
                    "inbound_relevance": inbound_relevance,
                    "use_objection_engine": self._is_objection_action(objection_action, thread_context),
                    "objection_step_after": objection_step + 1,
                    "flow_state": flow_state,
                }

            if objection_enabled and objection_step > 0 and inbound_type in {"positive", "neutral"}:
                resolved_transition = str(objection_cfg.get("resolved_transition") or "positive").strip()
                transition_key = resolved_transition or "positive"
            elif (
                objection_enabled
                and objection_step >= objection_max_steps
                and inbound_type in {"negative", "doubt"}
            ):
                unresolved_transition = str(objection_cfg.get("unresolved_transition") or "negative").strip()
                transition_key = unresolved_transition or "negative"
            else:
                transition_key = inbound_type

            next_stage_id = self.compute_next_stage(
                current_stage_id=stage_id,
                inbound_type=transition_key,
                transitions=transition_map,
            )
            # Replies are composed from the current stage. The transition target is
            # still persisted only after the outbound send is confirmed.
            action_type = str(stage.get("action_type") or "").strip()
            return {
                "decision": "reply",
                "reason": "inbound_relevant",
                "stage_id": stage_id,
                "next_stage_id": next_stage_id,
                "action_type": action_type,
                "inbound_type": inbound_type,
                "inbound_relevance": inbound_relevance,
                "use_objection_engine": self._is_objection_action(action_type, thread_context),
                "objection_step_after": 0,
                "flow_state": flow_state,
            }

        followup = self.compute_followup_due(
            {
                **thread_context,
                "flow_state": flow_state,
                "inbound_relevant": inbound_relevant,
            }
        )
        if bool(followup.get("due", False)):
            action_type = str(followup.get("action_type") or "").strip()
            return {
                "decision": "followup",
                "reason": str(followup.get("reason") or "followup_due"),
                "stage_id": stage_id,
                "next_stage_id": stage_id,
                "action_type": action_type,
                "inbound_type": "neutral",
                "inbound_relevance": inbound_relevance,
                "use_objection_engine": False,
                "objection_step_after": 0,
                "followup_level": int(followup.get("followup_level") or 0),
                "flow_state": flow_state,
            }
        wait_seconds = max(0, _safe_int(followup.get("wait_seconds")))
        if wait_seconds > 0:
            return {
                "decision": "wait",
                "reason": str(followup.get("reason") or "followup_wait"),
                "stage_id": stage_id,
                "next_stage_id": stage_id,
                "action_type": "",
                "inbound_type": "neutral",
                "inbound_relevance": inbound_relevance,
                "use_objection_engine": False,
                "objection_step_after": max(0, _safe_int(flow_state.get("objection_step"))),
                "wait_seconds": wait_seconds,
                "flow_state": flow_state,
            }
        return {
            "decision": "skip",
            "reason": str(followup.get("reason") or inbound_relevance.get("reason") or "no_action"),
            "stage_id": stage_id,
            "next_stage_id": stage_id,
            "action_type": "",
            "inbound_type": "neutral",
            "inbound_relevance": inbound_relevance,
            "use_objection_engine": False,
            "objection_step_after": max(0, _safe_int(flow_state.get("objection_step"))),
            "flow_state": flow_state,
        }

    def apply_outbound(
        self,
        flow_state: Dict[str, object],
        decision: Dict[str, object],
        *,
        sent_at: float,
    ) -> Dict[str, object]:
        stage_id = str(flow_state.get("stage_id") or self.entry_stage_id).strip()
        updated = _normalize_flow_state(
            flow_state,
            fallback_stage_id=stage_id,
            last_outbound_ts=_safe_float(flow_state.get("last_outbound_ts")),
            followup_level_hint=_safe_int(flow_state.get("followup_level")),
        )
        decision_kind = str(decision.get("decision") or "").strip().lower()
        updated["last_outbound_ts"] = float(sent_at)
        if decision_kind == "followup":
            if not _safe_float(updated.get("followup_anchor_ts")):
                updated["followup_anchor_ts"] = float(sent_at)
            updated["followup_level"] = max(0, _safe_int(updated.get("followup_level"))) + 1
            updated["objection_step"] = 0
            return updated

        next_stage_id = str(decision.get("next_stage_id") or updated.get("stage_id") or "").strip()
        if next_stage_id not in self.stage_map:
            next_stage_id = str(updated.get("stage_id") or self.entry_stage_id).strip()
        if next_stage_id != str(updated.get("stage_id") or "").strip():
            updated["last_stage_change_ts"] = float(sent_at)
        updated["stage_id"] = next_stage_id
        updated["followup_level"] = 0
        updated["followup_anchor_ts"] = float(sent_at)
        updated["objection_step"] = max(0, _safe_int(decision.get("objection_step_after")))
        return updated


def _log_flow_decision_and_state(
    thread_id: str,
    *,
    decision: Dict[str, object],
    flow_state: Dict[str, object],
    pending: bool,
) -> None:
    action_type = str(decision.get("action_type") or "").strip()
    stage_id = str(
        decision.get("next_stage_id")
        or decision.get("stage_id")
        or flow_state.get("stage_id")
        or ""
    ).strip()
    reason = str(decision.get("reason") or "").strip()
    decision_kind = str(decision.get("decision") or "").strip().lower()
    logger.info(
        "FLOW_DECISION thread=%s action_type=%s stage_id=%s reason=%s decision=%s",
        thread_id,
        action_type or "-",
        stage_id or "-",
        reason or "-",
        decision_kind or "-",
    )
    logger.info(
        "FLOW_STATE thread=%s stage_id=%s followup_level=%s objection_step=%s pending=%s",
        thread_id,
        str(flow_state.get("stage_id") or "").strip() or "-",
        max(0, _safe_int(flow_state.get("followup_level"))),
        max(0, _safe_int(flow_state.get("objection_step"))),
        "1" if pending else "0",
    )


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
        _get_autoresponder_runtime_controller().record_reply_success(account)
        return index

    def record_followup_attempt(self, account: str) -> None:
        self.followup_attempts += 1
        self.accounts.add(account)

    def record_followup_success(self, account: str) -> None:
        self.followups += 1
        self.accounts.add(account)
        _get_autoresponder_runtime_controller().record_followup_success(account)

    def record_response_error(self, account: str) -> int:
        index = self._bump_responses(account)
        self.errors += 1
        _get_autoresponder_runtime_controller().record_reply_failure(account)
        return index

    def record_followup_error(self, account: str) -> None:
        self.errors += 1
        self.accounts.add(account)
        _get_autoresponder_runtime_controller().record_followup_failure(account)

    def record_error(self, account: str) -> None:
        self.errors += 1
        self.accounts.add(account)


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
                logger.warning(
                    "AUTORESPONDER_KEEP_BROWSER_OPEN account=@%s reason=ensure_ready_failed",
                    username,
                )
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
        connected = has_playwright_storage_state(username)
        mark_connected(username, connected, invalidate_health=False)
        return connected
    except Exception:
        return False


def _choose_targets(alias: str) -> list[str]:
    accounts_data = list_all()
    alias_key = alias.lstrip("@")
    alias_lower = alias_key.lower()

    if alias.upper() == "ALL":
        candidates = [a["username"] for a in accounts_data if is_account_enabled_for_operation(a)]
    else:
        alias_matches = [
            a
            for a in accounts_data
            if a.get("alias", "").lower() == alias_lower and is_account_enabled_for_operation(a)
        ]
        if alias_matches:
            candidates = [a["username"] for a in alias_matches]
        else:
            username_matches = [
                a
                for a in accounts_data
                if a.get("username", "").lower() == alias_lower and is_account_enabled_for_operation(a)
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


def _autoresponder_reason_has_token(reason: object, tokens: tuple[str, ...]) -> bool:
    normalized = _normalize_text_for_match(str(reason or ""))
    if not normalized:
        return False
    return any(token in normalized for token in tokens)


def _autoresponder_reason_label(reason: object, *, source: str = "") -> str:
    raw_reason = str(reason or "").strip()
    if raw_reason.lower().startswith("pause:"):
        raw_reason = raw_reason.split(":", 1)[1].strip()
    normalized = _normalize_text_for_match(raw_reason)
    source_key = str(source or "").strip().lower()
    if not normalized:
        if source_key == "proxy":
            return "Proxy no operativo"
        if source_key == "session":
            return "Re-login requerido"
        if source_key == "safezone":
            return "Cooldown safezone"
        return ""
    if "account_not_found" in normalized:
        return "Cuenta no encontrada"
    if source_key == "proxy":
        if "quarantined" in normalized or "cuarentena" in normalized:
            return "Proxy en cuarentena"
        if "inactive" in normalized or "inactivo" in normalized:
            return "Proxy inactivo"
        if "missing" in normalized or "faltante" in normalized:
            return "Proxy faltante"
        return "Proxy no operativo"
    if _autoresponder_reason_has_token(
        normalized,
        (
            "login required",
            "login_required",
            "login requerido",
            "session invalid",
            "session_invalid",
            "session expired",
            "session_expired",
            "redirected_to_login",
            "storage_state_missing",
            "storage_state_invalid",
            "login_form",
            "url_login_or_challenge",
        ),
    ):
        return "Re-login requerido"
    if "checkpoint" in normalized:
        return "Checkpoint pendiente"
    if "challenge" in normalized:
        return "Challenge pendiente"
    if "low_profile" in normalized:
        return "Cuenta en bajo perfil"
    if "hydration_disabled" in normalized:
        return "Cooldown preventivo"
    if "backoff" in normalized:
        return "Cooldown por backoff"
    if _autoresponder_reason_has_token(
        normalized,
        (
            "429",
            "too many requests",
            "rate limit",
            "rate_limited",
            "feedback required",
            "feedback_required",
        ),
    ):
        return "Cooldown por rate limit"
    if source_key == "safezone":
        if normalized in {"unstable", "inestable"}:
            return "Cooldown safezone"
        return raw_reason
    return raw_reason


def _autoresponder_safety_descriptor(
    *,
    source: str,
    reason: object,
    message: object = "",
    remaining_seconds: float = 0.0,
) -> Dict[str, object]:
    source_key = str(source or "").strip().lower()
    reason_code = str(reason or "").strip()
    if reason_code.lower().startswith("pause:"):
        reason_code = reason_code.split(":", 1)[1].strip()
    display_message = str(message or "").strip()
    normalized = _normalize_text_for_match(reason_code)
    safety_state = "blocked"

    if source_key == "usable":
        safety_state = "usable"
        display_message = display_message or "Lista"
    elif source_key == "proxy":
        safety_state = "blocked"
        display_message = display_message or _autoresponder_reason_label(reason_code, source=source_key)
    elif "account_not_found" in normalized:
        safety_state = "blocked"
        display_message = display_message or "Cuenta no encontrada"
    elif source_key == "account":
        safety_state = "low_profile"
        display_message = display_message or "Cuenta en bajo perfil"
    elif _autoresponder_reason_has_token(
        normalized,
        (
            "login required",
            "login_required",
            "login requerido",
            "session invalid",
            "session_invalid",
            "session expired",
            "session_expired",
            "redirected_to_login",
            "storage_state_missing",
            "storage_state_invalid",
            "login_form",
            "url_login_or_challenge",
        ),
    ):
        safety_state = "needs_login"
        display_message = display_message or "Re-login requerido"
    elif "checkpoint" in normalized:
        safety_state = "blocked"
        display_message = display_message or "Checkpoint pendiente"
    elif "challenge" in normalized:
        safety_state = "blocked"
        display_message = display_message or "Challenge pendiente"
    elif source_key == "safezone":
        safety_state = "cooldown"
        if not display_message:
            detail_label = _autoresponder_reason_label(reason_code, source=source_key)
            if detail_label and detail_label != "Cooldown safezone":
                display_message = f"Cooldown safezone: {detail_label}"
            else:
                display_message = "Cooldown safezone"
    elif normalized in {"hydration_disabled", "backoff"} or _autoresponder_reason_has_token(
        normalized,
        (
            "429",
            "too many requests",
            "rate limit",
            "rate_limited",
            "feedback required",
            "feedback_required",
        ),
    ):
        safety_state = "risk_limited"
        display_message = display_message or _autoresponder_reason_label(reason_code, source=source_key)
    elif source_key == "session":
        safety_state = "needs_login"
        display_message = display_message or "Re-login requerido"
    elif source_key == "runtime":
        safety_state = "cooldown"
        display_message = display_message or "Cuenta en pausa"
    else:
        display_message = display_message or _autoresponder_reason_label(reason_code, source=source_key) or "Cuenta bloqueada"

    return {
        "source": source_key,
        "status": safety_state,
        "safety_state": safety_state,
        "reason": reason_code or source_key or safety_state,
        "message": display_message,
        "remaining_seconds": max(0.0, float(remaining_seconds or 0.0)),
        "blocked": safety_state != "usable",
    }


def _autoresponder_runtime_pause_policy(reason: object) -> tuple[str, int] | None:
    normalized = _normalize_text_for_match(str(reason or ""))
    if not normalized:
        return None
    if "checkpoint" in normalized or "challenge" in normalized:
        return "checkpoint", int(_AUTORESPONDER_CHECKPOINT_PAUSE_SECONDS)
    if _autoresponder_reason_has_token(
        normalized,
        (
            "429",
            "too many requests",
            "rate limit",
            "rate_limited",
            "feedback required",
            "feedback_required",
        ),
    ):
        return "feedback_required", int(_AUTORESPONDER_RATE_LIMIT_PAUSE_SECONDS)
    if _autoresponder_reason_has_token(
        normalized,
        (
            "login required",
            "login_required",
            "login requerido",
            "session invalid",
            "session_invalid",
            "session expired",
            "session_expired",
            "redirected_to_login",
            "storage_state_missing",
            "storage_state_invalid",
            "login_form",
            "url_login_or_challenge",
        ),
    ):
        return "needs_login", int(_AUTORESPONDER_LOGIN_REQUIRED_PAUSE_SECONDS)
    return None


def _pause_autoresponder_account_for_safety(
    account_id: str,
    reason: object,
    *,
    runtime: Optional[AutoresponderRuntimeController] = None,
) -> tuple[str, int] | None:
    policy = _autoresponder_runtime_pause_policy(reason)
    if policy is None:
        return None
    controller = runtime
    if controller is None:
        try:
            controller = _get_autoresponder_runtime_controller()
        except Exception:
            controller = None
    if controller is None:
        return policy
    try:
        controller.pause_account(
            account_id,
            reason=policy[0],
            duration_seconds=float(policy[1]),
        )
    except Exception:
        return None
    return policy


def _autoresponder_account_safety(
    account: Dict[str, Any],
    *,
    proxy_blocked: Optional[Dict[str, Any]] = None,
    runtime: Optional[AutoresponderRuntimeController] = None,
) -> Dict[str, object]:
    username = str(account.get("username") or "").strip().lstrip("@")
    if not username:
        return {
            "username": "",
            **_autoresponder_safety_descriptor(
                source="account",
                reason="account_not_found",
                message="Cuenta no encontrada",
            ),
        }
    if isinstance(proxy_blocked, dict):
        proxy_status = str(proxy_blocked.get("status") or "").strip() or "blocked"
        return {
            "username": username,
            **_autoresponder_safety_descriptor(
                source="proxy",
                reason=f"proxy_{proxy_status}",
            ),
        }
    if not has_playwright_storage_state(username):
        return {
            "username": username,
            **_autoresponder_safety_descriptor(
                source="session",
                reason="storage_state_missing",
            ),
        }
    record, expired = health_store.get_record(username)
    if record is not None and not expired:
        health_state = str(record.state or "").strip().upper()
        health_reason = str(record.reason or "").strip()
        if health_state == health_store.HEALTH_STATE_DEAD:
            return {
                "username": username,
                **_autoresponder_safety_descriptor(
                    source="health",
                    reason=health_reason or "health_dead",
                ),
            }
        if health_state == health_store.HEALTH_STATE_INACTIVE and _autoresponder_reason_has_token(
            health_reason,
            (
                "login required",
                "login_required",
                "login requerido",
                "session invalid",
                "session_invalid",
                "session expired",
                "session_expired",
                "redirected_to_login",
                "storage_state_missing",
                "storage_state_invalid",
                "login_form",
                "url_login_or_challenge",
                "feedback required",
                "feedback_required",
                "rate limit",
                "rate_limited",
            ),
        ):
            return {
                "username": username,
                **_autoresponder_safety_descriptor(
                    source="health",
                    reason=health_reason or "inactive",
                ),
            }
    if bool(account.get("low_profile")):
        low_profile_reason = str(account.get("low_profile_reason") or "").strip()
        low_profile_message = "Cuenta en bajo perfil"
        if low_profile_reason:
            low_profile_message = f"Bajo perfil: {low_profile_reason}"
        return {
            "username": username,
            **_autoresponder_safety_descriptor(
                source="account",
                reason="low_profile",
                message=low_profile_message,
            ),
        }
    runtime_controller = runtime
    if runtime_controller is None:
        try:
            runtime_controller = _get_autoresponder_runtime_controller()
        except Exception:
            runtime_controller = None
    if runtime_controller is not None:
        try:
            blocked, remaining_seconds, blocked_reason = runtime_controller.is_account_blocked(username)
        except Exception:
            blocked, remaining_seconds, blocked_reason = False, 0.0, ""
        if blocked and str(blocked_reason or "").strip().lower().startswith("pause:"):
            return {
                "username": username,
                **_autoresponder_safety_descriptor(
                    source="runtime",
                    reason=blocked_reason,
                    remaining_seconds=remaining_seconds,
                ),
            }
    quarantined, quarantine_remaining, quarantine_reason = _safezone_quarantine_status(username)
    if quarantined:
        return {
            "username": username,
            **_autoresponder_safety_descriptor(
                source="safezone",
                reason=quarantine_reason or "unstable",
                remaining_seconds=quarantine_remaining,
            ),
        }
    if runtime_controller is not None:
        try:
            blocked, remaining_seconds, blocked_reason = runtime_controller.is_account_blocked(username)
        except Exception:
            blocked, remaining_seconds, blocked_reason = False, 0.0, ""
        if blocked:
            return {
                "username": username,
                **_autoresponder_safety_descriptor(
                    source="runtime",
                    reason=blocked_reason,
                    remaining_seconds=remaining_seconds,
                ),
            }
    return {
        "username": username,
        **_autoresponder_safety_descriptor(source="usable", reason="usable"),
    }


def _filter_startable_accounts(targets: list[str]) -> list[str]:
    inspection = _inspect_startable_accounts(targets)
    return [
        str(user).strip().lstrip("@")
        for user in (inspection.get("startable_accounts") or [])
        if str(user).strip()
    ]


def _inspect_startable_accounts(
    targets: list[str],
    *,
    log_skipped: bool = True,
) -> Dict[str, object]:
    existing_accounts: list[Dict[str, Any]] = []
    skipped: list[Dict[str, object]] = []
    account_statuses: list[Dict[str, object]] = []
    seen: set[str] = set()

    for raw_user in targets:
        username = str(raw_user or "").strip().lstrip("@")
        if not username:
            continue
        normalized = username.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        account = get_account(username)
        if not isinstance(account, dict):
            status = {
                "username": username,
                **_autoresponder_safety_descriptor(
                    source="account",
                    reason="account_not_found",
                    message="Cuenta no encontrada",
                ),
            }
            account_statuses.append(status)
            skipped.append(
                {
                    "username": username,
                    "source": str(status.get("source") or "account"),
                    "status": str(status.get("status") or "blocked"),
                    "reason": str(status.get("reason") or "account_not_found"),
                    "message": str(status.get("message") or "Cuenta no encontrada"),
                    "remaining_seconds": float(status.get("remaining_seconds") or 0.0),
                    "blocked": True,
                }
            )
            continue
        existing_accounts.append(dict(account))

    proxy_preflight = preflight_accounts_for_proxy_runtime(existing_accounts)
    proxy_blocked_by_username = {
        str(item.get("username") or "").strip().lstrip("@").lower(): dict(item)
        for item in (proxy_preflight.get("blocked_accounts") or [])
        if isinstance(item, dict) and str(item.get("username") or "").strip()
    }
    try:
        runtime_controller: Optional[AutoresponderRuntimeController] = _get_autoresponder_runtime_controller()
    except Exception:
        runtime_controller = None

    startable: list[str] = []
    safety_state_counts: Dict[str, int] = {}
    for account in existing_accounts:
        username = str(account.get("username") or "").strip().lstrip("@")
        if not username:
            continue
        status = _autoresponder_account_safety(
            account,
            proxy_blocked=proxy_blocked_by_username.get(username.lower()),
            runtime=runtime_controller,
        )
        account_statuses.append(status)
        safety_state = str(status.get("safety_state") or "blocked")
        safety_state_counts[safety_state] = int(safety_state_counts.get(safety_state, 0) or 0) + 1
        if bool(status.get("blocked")):
            skipped.append(
                {
                    "username": username,
                    "source": str(status.get("source") or "account"),
                    "status": safety_state,
                    "reason": str(status.get("reason") or "blocked"),
                    "message": str(status.get("message") or "Cuenta bloqueada"),
                    "remaining_seconds": float(status.get("remaining_seconds") or 0.0),
                    "blocked": True,
                }
            )
            continue
        startable.append(username)

    if log_skipped and skipped:
        preview = ", ".join(
            f"@{str(item.get('username') or '').strip().lstrip('@')}={str(item.get('reason') or '').strip() or 'blocked'}"
            for item in skipped[:5]
        )
        if len(skipped) > 5:
            preview = f"{preview}, +{len(skipped) - 5} more"
        logger.warning("AUTORESPONDER_START_SKIPPED %s", preview)

    return {
        "startable_accounts": startable,
        "skipped_accounts": skipped,
        "account_statuses": account_statuses,
        "blocked_status_counts": dict(proxy_preflight.get("blocked_status_counts") or {}),
        "safety_state_counts": safety_state_counts,
    }



def _mask_key(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if len(value) <= 6:
        return value[:2] + "â€¦"
    return f"{value[:4]}â€¦{value[-2:]}"


def _normalize_system_prompt_text(value: str) -> str:
    if not value:
        return ""
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _read_prompts_state(refresh: bool = False) -> Dict[str, dict]:
    global _PROMPTS_STATE
    if refresh or _PROMPTS_STATE is None:
        data = _read_state_json(_PROMPTS_FILE, {"aliases": {}, "accounts": {}})
        _PROMPTS_STATE = _ensure_alias_container(data)
    return _PROMPTS_STATE


def _write_prompts_state(state: Dict[str, dict]) -> None:
    state = _ensure_alias_container(state)
    _write_state_json(_PROMPTS_FILE, state)
    _read_prompts_state(refresh=True)


def _default_prompt_entry(alias: str) -> Dict[str, object]:
    alias_label = alias.strip() or _PROMPT_DEFAULT_ALIAS
    return {
        "alias": alias_label,
        "objection_prompt": _DEFAULT_OBJECTION_PROMPT,
        "objection_strategy_name": _DEFAULT_OBJECTION_STRATEGY_NAME,
        "flow_config": {
            "version": _FLOW_CONFIG_VERSION,
            "entry_stage_id": "",
            "stages": [],
        },
    }


def _coerce_prompt_entry(alias: str, raw_entry: object) -> Dict[str, object]:
    entry = _default_prompt_entry(alias)
    if not isinstance(raw_entry, dict):
        return entry

    entry["alias"] = str(raw_entry.get("alias") or entry["alias"]).strip() or entry["alias"]

    if "objection_prompt" in raw_entry:
        entry["objection_prompt"] = _normalize_system_prompt_text(
            str(raw_entry.get("objection_prompt") or "")
        ).strip()

    if "objection_strategy_name" in raw_entry:
        entry["objection_strategy_name"] = str(raw_entry.get("objection_strategy_name") or "").strip()

    if "flow_config" in raw_entry:
        entry["flow_config"] = _normalize_flow_config(raw_entry.get("flow_config"))
    return entry


def _get_prompt_entry(alias: str) -> Dict[str, object]:
    state = _read_prompts_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    alias_key = _normalize_alias_key(alias)
    entry = _coerce_prompt_entry(alias, aliases.get(alias_key))
    return entry


def _get_account_prompt_entry(username: str) -> Dict[str, object]:
    state = _read_prompts_state()
    accounts: Dict[str, dict] = state.setdefault("accounts", {})
    account_key = _normalize_username(username)
    entry = _coerce_prompt_entry(username, accounts.get(account_key))
    return entry


def _set_prompt_entry(alias: str, updates: Dict[str, object]) -> None:
    alias_clean = alias.strip()
    if not alias_clean:
        warn("Alias invalido.")
        return
    state = _read_prompts_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    alias_key = _normalize_alias_key(alias_clean)
    current = _coerce_prompt_entry(alias_clean, aliases.get(alias_key))
    for key_name, value in updates.items():
        if key_name == "objection_prompt":
            normalized_value = _normalize_system_prompt_text(str(value or ""))
            current[key_name] = normalized_value
        elif key_name == "flow_config":
            current[key_name] = _normalize_flow_config(value)
        elif key_name in {"objection_strategy_name", "alias"}:
            current[key_name] = str(value or "").strip()
        else:
            current[key_name] = value
    aliases[alias_key] = dict(current)
    _write_prompts_state(state)


def _prompt_entry_exists(alias: str) -> bool:
    alias_clean = str(alias or "").strip()
    if not alias_clean:
        return False
    state = _read_prompts_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    alias_key = _normalize_alias_key(alias_clean)
    raw_entry = aliases.get(alias_key)
    return isinstance(raw_entry, dict)


def _account_prompt_entry_exists(username: str) -> bool:
    account_clean = _normalize_username(username)
    if not account_clean:
        return False
    state = _read_prompts_state()
    accounts: Dict[str, dict] = state.setdefault("accounts", {})
    raw_entry = accounts.get(account_clean)
    return isinstance(raw_entry, dict)


def _resolve_prompt_entry_for_user(
    username: str,
    *,
    active_alias: str | None = None,
    fallback_entry: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    def _has_valid_flow(raw_flow_config: object) -> bool:
        if not isinstance(raw_flow_config, dict):
            return False
        normalized = _normalize_flow_config(raw_flow_config)
        stages = normalized.get("stages")
        if not isinstance(stages, list) or not stages:
            return False
        stage_ids = {
            str(stage.get("id") or "").strip()
            for stage in stages
            if isinstance(stage, dict) and str(stage.get("id") or "").strip()
        }
        if not stage_ids:
            return False
        entry_stage_id = str(normalized.get("entry_stage_id") or "").strip()
        return bool(entry_stage_id and entry_stage_id in stage_ids)

    def _has_valid_pack_bindings(raw_flow_config: object) -> bool:
        if not _has_valid_flow(raw_flow_config):
            return False
        normalized = _normalize_flow_config(raw_flow_config)
        try:
            ok, _reason = _validate_flow_pack_bindings(normalized)
            return bool(ok)
        except Exception:
            return False

    fallback = dict(fallback_entry or _default_prompt_entry(_PROMPT_DEFAULT_ALIAS))
    account_data = get_account(username) or {}
    account_alias = str(account_data.get("alias") or "").strip()
    active_alias_clean = str(active_alias or "").strip()
    active_alias_is_all = _normalize_alias_key(active_alias_clean) == "all"

    candidate_scopes: List[tuple[str, str]] = []
    if active_alias_is_all:
        candidate_scopes.append(("alias", "ALL"))
    candidate_scopes.append(("account", username))
    if account_alias:
        candidate_scopes.append(("alias", account_alias))
    if active_alias_clean and not active_alias_is_all:
        candidate_scopes.append(("alias", active_alias_clean))
    if not active_alias_is_all:
        candidate_scopes.append(("alias", "ALL"))
    candidate_scopes.append(("alias", _PROMPT_DEFAULT_ALIAS))

    selected_entry: Optional[Dict[str, object]] = None
    selected_has_valid_flow = False
    selected_has_valid_packs = False
    seen_candidates: set[tuple[str, str]] = set()
    for scope, candidate in candidate_scopes:
        if scope == "account":
            norm_candidate = _normalize_username(candidate)
            exists = _account_prompt_entry_exists(candidate)
            entry = _get_account_prompt_entry(candidate) if exists else {}
        else:
            norm_candidate = _normalize_alias_key(candidate)
            exists = _prompt_entry_exists(candidate)
            entry = _get_prompt_entry(candidate) if exists else {}
        candidate_key = (scope, norm_candidate)
        if not norm_candidate or candidate_key in seen_candidates:
            continue
        seen_candidates.add(candidate_key)
        if not exists:
            continue
        objection_prompt = str(entry.get("objection_prompt") or "").strip()
        objection_strategy_name = str(entry.get("objection_strategy_name") or "").strip()
        flow_config = entry.get("flow_config")
        has_flow_config = isinstance(flow_config, dict)
        if not (objection_prompt or objection_strategy_name or has_flow_config):
            continue

        candidate_has_valid_flow = _has_valid_flow(flow_config)
        candidate_has_valid_packs = _has_valid_pack_bindings(flow_config)
        if selected_entry is None:
            selected_entry = dict(entry)
            selected_has_valid_flow = candidate_has_valid_flow
            selected_has_valid_packs = candidate_has_valid_packs
            if selected_has_valid_flow and selected_has_valid_packs:
                return selected_entry
            continue

        if (not selected_has_valid_flow) and candidate_has_valid_flow:
            merged = dict(selected_entry)
            merged["flow_config"] = flow_config
            selected_entry = merged
            selected_has_valid_flow = True
            selected_has_valid_packs = candidate_has_valid_packs
            if selected_has_valid_packs:
                return selected_entry
            continue

        if (
            selected_has_valid_flow
            and (not selected_has_valid_packs)
            and candidate_has_valid_flow
            and candidate_has_valid_packs
        ):
            merged = dict(selected_entry)
            merged["flow_config"] = flow_config
            return merged

    if selected_entry is not None:
        fallback_has_valid_flow = _has_valid_flow(fallback.get("flow_config"))
        fallback_has_valid_packs = _has_valid_pack_bindings(fallback.get("flow_config"))
        if (not selected_has_valid_flow) and fallback_has_valid_flow:
            merged = dict(selected_entry)
            merged["flow_config"] = fallback.get("flow_config")
            return merged
        if selected_has_valid_flow:
            if (not selected_has_valid_packs) and fallback_has_valid_packs:
                merged = dict(selected_entry)
                merged["flow_config"] = fallback.get("flow_config")
                return merged
            return selected_entry

    if _has_valid_flow(fallback.get("flow_config")):
        return fallback

    if selected_entry is not None:
        return selected_entry

    return fallback


def _load_preferences(alias: str | None = None) -> tuple[str, str]:
    env_values = read_env_local()
    api_key = _resolve_ai_api_key(env_values)
    return api_key, _DEFAULT_RESPONDER_STRATEGY_PROMPT


def _read_state_json(path: Path, default: Dict[str, dict]) -> Dict[str, dict]:
    data: Dict[str, dict] = dict(default)
    if path.exists():
        try:
            loaded = load_json_file(path, default, label=f"responder.state:{path.name}")
            if isinstance(loaded, dict):
                data.update(loaded)
        except Exception:
            data = dict(default)
    return data


def _write_state_json(path: Path, data: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, data)


def _ensure_alias_container(data: Dict[str, dict]) -> Dict[str, dict]:
    if "aliases" not in data or not isinstance(data["aliases"], dict):
        data["aliases"] = {}
    if "accounts" not in data or not isinstance(data["accounts"], dict):
        data["accounts"] = {}
    return data


def _ensure_list_container(data: Dict[str, dict], key_name: str) -> Dict[str, dict]:
    if key_name not in data or not isinstance(data[key_name], list):
        data[key_name] = []
    return data


def _read_packs_state(refresh: bool = False) -> Dict[str, dict]:
    global _PACKS_STATE
    if refresh or _PACKS_STATE is None:
        data = _read_state_json(_PACKS_FILE, {"packs": []})
        _PACKS_STATE = _ensure_list_container(data, "packs")
    return _PACKS_STATE


def _write_packs_state(state: Dict[str, dict]) -> None:
    state.setdefault("packs", [])
    _write_state_json(_PACKS_FILE, state)
    _read_packs_state(refresh=True)


def _pack_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "si", "s", "yes", "y", "on"}


def _normalize_pack_action(raw_action: object) -> Optional[Dict[str, object]]:
    if not isinstance(raw_action, dict):
        return None
    action_type = str(raw_action.get("type") or "").strip().lower()
    if action_type not in {"text_fixed", "text_adaptive", "set_memory"}:
        return None
    action: Dict[str, object] = {"type": action_type}
    if action_type == "text_fixed":
        content = _sanitize_generated_message(str(raw_action.get("content") or "").strip())
        if not content:
            return None
        action["content"] = content
    elif action_type == "text_adaptive":
        instruction = str(raw_action.get("instruction") or "").strip()
        if not instruction:
            return None
        action["instruction"] = instruction
    elif action_type == "set_memory":
        key = str(raw_action.get("key") or "").strip()
        if not key:
            return None
        action["key"] = key
        action["value"] = raw_action.get("value")
    return action


def _normalize_pack_record(raw_pack: object) -> Optional[Dict[str, object]]:
    if not isinstance(raw_pack, dict):
        return None
    pack_name = str(raw_pack.get("name") or "").strip()
    pack_type = str(raw_pack.get("type") or "").strip()
    pack_id = str(raw_pack.get("id") or "").strip() or str(uuid.uuid4())
    if not pack_name or not pack_type:
        return None
    try:
        delay_min = max(0, int(raw_pack.get("delay_min", 0) or 0))
    except Exception:
        delay_min = 0
    try:
        delay_max = max(delay_min, int(raw_pack.get("delay_max", delay_min) or delay_min))
    except Exception:
        delay_max = delay_min
    actions_raw = raw_pack.get("actions")
    actions: List[Dict[str, object]] = []
    if isinstance(actions_raw, list):
        for item in actions_raw:
            normalized = _normalize_pack_action(item)
            if normalized:
                actions.append(normalized)
    return {
        "id": pack_id,
        "name": pack_name,
        "type": pack_type,
        "delay_min": delay_min,
        "delay_max": delay_max,
        "active": _pack_bool(raw_pack.get("active", True)),
        "actions": actions,
    }


def _list_packs() -> List[Dict[str, object]]:
    state = _read_packs_state()
    packs_raw = state.get("packs")
    if not isinstance(packs_raw, list):
        return []
    normalized: List[Dict[str, object]] = []
    for raw in packs_raw:
        pack = _normalize_pack_record(raw)
        if pack:
            normalized.append(pack)
    return normalized


def _save_packs(packs: List[Dict[str, object]]) -> None:
    state = _read_packs_state()
    state["packs"] = packs
    _write_packs_state(state)


def _upsert_pack(pack: Dict[str, object]) -> Dict[str, object]:
    normalized = _normalize_pack_record(pack)
    if not normalized:
        raise ValueError("Pack invalido")
    packs = _list_packs()
    target_id = str(normalized.get("id") or "").strip()
    updated = False
    for idx, existing in enumerate(packs):
        if str(existing.get("id") or "").strip() == target_id:
            packs[idx] = normalized
            updated = True
            break
    if not updated:
        packs.append(normalized)
    _save_packs(packs)
    return normalized


def _read_account_memory_state(refresh: bool = False) -> Dict[str, dict]:
    global _ACCOUNT_MEMORY_STATE
    if refresh or _ACCOUNT_MEMORY_STATE is None:
        data = _read_state_json(_ACCOUNT_MEMORY_FILE, {"accounts": {}})
        if not isinstance(data.get("accounts"), dict):
            data["accounts"] = {}
        _ACCOUNT_MEMORY_STATE = data
    return _ACCOUNT_MEMORY_STATE


def _write_account_memory_state(state: Dict[str, dict]) -> None:
    state.setdefault("accounts", {})
    _write_state_json(_ACCOUNT_MEMORY_FILE, state)
    _read_account_memory_state(refresh=True)


def _get_account_memory(account_id: str) -> Dict[str, object]:
    state = _read_account_memory_state()
    accounts = state.setdefault("accounts", {})
    key = _normalize_username(account_id)
    entry = accounts.get(key)
    if not isinstance(entry, dict):
        entry = {}
    entry.setdefault("last_pack_used", {})
    return dict(entry)


def _set_account_memory(account_id: str, memory: Dict[str, object]) -> None:
    state = _read_account_memory_state()
    accounts = state.setdefault("accounts", {})
    key = _normalize_username(account_id)
    payload = dict(memory or {})
    last_pack_used = payload.get("last_pack_used")
    if not isinstance(last_pack_used, dict):
        payload["last_pack_used"] = {}
    accounts[key] = payload
    _write_account_memory_state(state)


_SAFEZONE_FAILURE_THRESHOLD = _env_int(
    "AUTORESPONDER_SAFEZONE_FAILURE_THRESHOLD",
    4,
    minimum=2,
)
_SAFEZONE_BASE_QUARANTINE_SECONDS = _env_int(
    "AUTORESPONDER_SAFEZONE_BASE_QUARANTINE_SECONDS",
    480,
    minimum=30,
)
_SAFEZONE_MAX_QUARANTINE_SECONDS = max(
    _SAFEZONE_BASE_QUARANTINE_SECONDS,
    _env_int(
        "AUTORESPONDER_SAFEZONE_MAX_QUARANTINE_SECONDS",
        3600,
        minimum=_SAFEZONE_BASE_QUARANTINE_SECONDS,
    ),
)
_SAFEZONE_SUCCESS_DECAY = _env_int(
    "AUTORESPONDER_SAFEZONE_SUCCESS_DECAY",
    2,
    minimum=1,
)


def _normalize_safezone_state(raw: object) -> Dict[str, object]:
    state = raw if isinstance(raw, dict) else {}
    try:
        consecutive_failures = max(0, int(state.get("consecutive_failures", 0) or 0))
    except Exception:
        consecutive_failures = 0
    try:
        total_failures = max(0, int(state.get("total_failures", 0) or 0))
    except Exception:
        total_failures = 0
    try:
        quarantine_until = max(0.0, float(state.get("quarantine_until", 0.0) or 0.0))
    except Exception:
        quarantine_until = 0.0
    try:
        last_failure_at = max(0.0, float(state.get("last_failure_at", 0.0) or 0.0))
    except Exception:
        last_failure_at = 0.0
    try:
        last_success_at = max(0.0, float(state.get("last_success_at", 0.0) or 0.0))
    except Exception:
        last_success_at = 0.0
    return {
        "consecutive_failures": consecutive_failures,
        "total_failures": total_failures,
        "quarantine_until": quarantine_until,
        "last_failure_reason": str(state.get("last_failure_reason") or "").strip(),
        "last_failure_at": last_failure_at,
        "last_success_at": last_success_at,
    }


def _get_account_safezone_state(account_id: str) -> Dict[str, object]:
    memory = _get_account_memory(account_id)
    return _normalize_safezone_state(memory.get("safezone"))


def _set_account_safezone_state(account_id: str, safezone_state: Dict[str, object]) -> None:
    memory = _get_account_memory(account_id)
    memory["safezone"] = _normalize_safezone_state(safezone_state)
    _set_account_memory(account_id, memory)


def _safezone_quarantine_status(account_id: str) -> tuple[bool, float, str]:
    now_ts = time.time()
    state = _get_account_safezone_state(account_id)
    quarantine_until = float(state.get("quarantine_until") or 0.0)
    try:
        last_failure_at = max(0.0, float(state.get("last_failure_at") or 0.0))
    except Exception:
        last_failure_at = 0.0
    try:
        last_success_at = max(0.0, float(state.get("last_success_at") or 0.0))
    except Exception:
        last_success_at = 0.0
    # Si ya hubo Ã©xito real posterior al Ãºltimo fallo, no mantener cuarentena vieja.
    if quarantine_until > now_ts and last_success_at > last_failure_at and last_success_at > 0.0:
        consecutive = int(state.get("consecutive_failures", 0) or 0)
        state["consecutive_failures"] = max(0, consecutive - int(_SAFEZONE_SUCCESS_DECAY))
        state["quarantine_until"] = 0.0
        if int(state.get("consecutive_failures", 0) or 0) <= 0:
            state["last_failure_reason"] = ""
        _set_account_safezone_state(account_id, state)
        return False, 0.0, ""
    if quarantine_until > now_ts:
        return True, max(0.0, quarantine_until - now_ts), str(state.get("last_failure_reason") or "unstable")
    return False, 0.0, ""


def _safezone_reason_is_severe(reason: str) -> bool:
    reason_clean = str(reason or "").strip().lower()
    if not reason_clean:
        return False
    severe_tokens = (
        "open_thread_failed",
        "thread_invalido",
        "preflight",
        "composer",
        "send_failed",
        "challenge",
        "checkpoint",
        "rate",
        "feedback_required",
    )
    return any(token in reason_clean for token in severe_tokens)


def _safezone_register_failure(account_id: str, reason: str, *, severe: bool = False) -> None:
    now_ts = time.time()
    state = _get_account_safezone_state(account_id)
    increase = 2 if severe else 1
    state["consecutive_failures"] = int(state.get("consecutive_failures", 0) or 0) + increase
    state["total_failures"] = int(state.get("total_failures", 0) or 0) + 1
    state["last_failure_reason"] = str(reason or "").strip() or "unknown_failure"
    state["last_failure_at"] = now_ts
    threshold = int(_SAFEZONE_FAILURE_THRESHOLD)
    consecutive = int(state.get("consecutive_failures", 0) or 0)
    if consecutive >= threshold:
        over = max(0, consecutive - threshold)
        quarantine_s = min(
            int(_SAFEZONE_MAX_QUARANTINE_SECONDS),
            int(_SAFEZONE_BASE_QUARANTINE_SECONDS) * int(2 ** over),
        )
        state["quarantine_until"] = max(
            float(state.get("quarantine_until") or 0.0),
            now_ts + float(quarantine_s),
        )
        try:
            _get_autoresponder_runtime_controller().mark_rate_signal(
                account_id,
                reason=f"safezone:{reason}",
            )
        except Exception:
            pass
    _set_account_safezone_state(account_id, state)


def _safezone_register_success(account_id: str) -> None:
    now_ts = time.time()
    state = _get_account_safezone_state(account_id)
    consecutive = int(state.get("consecutive_failures", 0) or 0)
    if consecutive > 0:
        consecutive = max(0, consecutive - int(_SAFEZONE_SUCCESS_DECAY))
    state["consecutive_failures"] = consecutive
    state["last_success_at"] = now_ts
    state["quarantine_until"] = 0.0
    if consecutive == 0:
        state["last_failure_reason"] = ""
    _set_account_safezone_state(account_id, state)


def _safezone_delay_multiplier(account_id: str) -> float:
    multiplier = 1.0
    try:
        runtime_snapshot = _get_autoresponder_runtime_controller().snapshot(account_id)
        risk_score = float(runtime_snapshot.get("risk_score") or 0.0)
        blocked = bool(runtime_snapshot.get("account_blocked"))
    except Exception:
        risk_score = 0.0
        blocked = False
    state = _get_account_safezone_state(account_id)
    consecutive = float(state.get("consecutive_failures") or 0.0)
    multiplier += min(2.0, (risk_score * 0.18) + (consecutive * 0.22))
    if blocked:
        multiplier = max(multiplier, 2.5)
    return max(1.0, min(multiplier, 4.0))


_PACK_SEND_ACTION_TYPES = {"text_fixed", "text_adaptive"}


def _pack_sendable_action_count(actions_raw: object) -> int:
    if not isinstance(actions_raw, list):
        return 0
    count = 0
    for raw_action in actions_raw:
        normalized = _normalize_pack_action(raw_action)
        if not normalized:
            continue
        action_type = str(normalized.get("type") or "").strip().lower()
        if action_type in _PACK_SEND_ACTION_TYPES:
            count += 1
    return max(0, int(count))


def _pack_has_sendable_actions(pack: Dict[str, object]) -> bool:
    return _pack_sendable_action_count(pack.get("actions")) > 0


def _flow_required_action_types(flow_config: Dict[str, object]) -> Dict[str, object]:
    stages_raw = flow_config.get("stages")
    stages = list(stages_raw) if isinstance(stages_raw, list) else []
    required: List[str] = []
    invalid_actions: List[str] = []
    has_valid_actions = False
    seen: set[str] = set()
    seen_invalid: set[str] = set()
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        stage_action_raw = str(stage.get("action_type") or "").strip()
        try:
            stage_action = _canonical_flow_action_type(stage_action_raw, strict=bool(stage_action_raw))
        except ValueError:
            stage_action = ""
            if stage_action_raw and stage_action_raw not in seen_invalid:
                seen_invalid.add(stage_action_raw)
                invalid_actions.append(stage_action_raw)
        if stage_action:
            has_valid_actions = True
        if stage_action and _flow_action_requires_pack(stage_action):
            token = _flow_action_token(stage_action)
            if token and token not in seen:
                seen.add(token)
                required.append(stage_action)
        followups_raw = stage.get("followups")
        followups = list(followups_raw) if isinstance(followups_raw, list) else []
        for followup in followups:
            if not isinstance(followup, dict):
                continue
            follow_action_raw = followup.get("action_type") or stage_action or ""
            follow_action_text = str(follow_action_raw or "").strip()
            try:
                follow_action = _canonical_flow_action_type(
                    follow_action_raw,
                    strict=bool(follow_action_text),
                )
            except ValueError:
                follow_action = ""
                if follow_action_text and follow_action_text not in seen_invalid:
                    seen_invalid.add(follow_action_text)
                    invalid_actions.append(follow_action_text)
            if not follow_action:
                continue
            has_valid_actions = True
            if not _flow_action_requires_pack(follow_action):
                continue
            token = _flow_action_token(follow_action)
            if token and token not in seen:
                seen.add(token)
                required.append(follow_action)
        objection_raw = stage.get("post_objection")
        objection_cfg = dict(objection_raw) if isinstance(objection_raw, dict) else {}
        objection_action_raw = str(objection_cfg.get("action_type") or "").strip()
        if bool(objection_cfg.get("enabled")) and objection_action_raw:
            try:
                objection_action = _canonical_flow_action_type(objection_action_raw, strict=True)
            except ValueError:
                objection_action = ""
                if objection_action_raw not in seen_invalid:
                    seen_invalid.add(objection_action_raw)
                    invalid_actions.append(objection_action_raw)
            if objection_action:
                has_valid_actions = True
            if objection_action and _flow_action_requires_pack(objection_action):
                token = _flow_action_token(objection_action)
                if token and token not in seen:
                    seen.add(token)
                    required.append(objection_action)
    return {
        "required": required,
        "invalid_actions": invalid_actions,
        "has_valid_actions": has_valid_actions,
    }


def _active_pack_send_counts_by_type() -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for pack in _list_packs():
        if not bool(pack.get("active", False)):
            continue
        pack_type = str(pack.get("type") or "").strip()
        if not pack_type:
            continue
        sendable_count = _pack_sendable_action_count(pack.get("actions"))
        prev = counts.get(pack_type)
        if prev is None:
            counts[pack_type] = sendable_count
        else:
            counts[pack_type] = max(int(prev), int(sendable_count))
    return counts


def _pack_binding_key(value: object) -> str:
    token = _flow_action_token(value)
    if not token:
        return ""
    collapsed = re.sub(r"_+", "_", token).strip("_")
    return collapsed


def _pack_binding_maps(
    *,
    active_only: bool = False,
    sendable_only: bool = False,
) -> tuple[Dict[str, Dict[str, object]], Dict[str, Dict[str, object]], Dict[str, str]]:
    by_id: Dict[str, Dict[str, object]] = {}
    by_type: Dict[str, Dict[str, object]] = {}
    type_by_key: Dict[str, str] = {}
    for pack in _list_packs():
        if active_only and not bool(pack.get("active", False)):
            continue
        if sendable_only and not _pack_has_sendable_actions(pack):
            continue
        pack_id = str(pack.get("id") or "").strip()
        pack_type = str(pack.get("type") or "").strip()
        if pack_id and pack_id not in by_id:
            by_id[pack_id] = pack
        if pack_type and pack_type not in by_type:
            by_type[pack_type] = pack
        pack_key = _pack_binding_key(pack_type)
        if pack_key and pack_key not in type_by_key:
            type_by_key[pack_key] = pack_type
    return by_id, by_type, type_by_key


def _canonical_flow_action_type(
    value: object,
    *,
    allow_empty: bool = False,
    strict: bool = False,
) -> str:
    clean_value = str(value or "").strip()
    if not clean_value:
        if allow_empty:
            return ""
        if strict:
            raise ValueError("El action_type no puede estar vacio.")
        return ""
    special = _canonical_flow_special_action_type(clean_value)
    if special:
        return special
    by_id, by_type, type_by_key = _pack_binding_maps(
        active_only=False,
        sendable_only=False,
    )
    if clean_value in by_type:
        return clean_value
    pack = by_id.get(clean_value)
    if isinstance(pack, dict):
        pack_type = str(pack.get("type") or "").strip()
        if pack_type:
            return pack_type
    binding_key = _pack_binding_key(clean_value)
    if binding_key:
        canonical_type = type_by_key.get(binding_key)
        if canonical_type:
            return canonical_type
    if strict:
        raise ValueError(f"Action type invalido: {clean_value}")
    return clean_value


def _flow_action_requires_pack(value: object) -> bool:
    canonical = _canonical_flow_action_type(value, allow_empty=True)
    return bool(canonical) and canonical not in _FLOW_NON_PACK_ACTION_TYPES


def _canonical_pack_strategy_name(
    value: object,
    *,
    active_only: bool = False,
    sendable_only: bool = False,
) -> str:
    clean_value = _canonical_flow_action_type(value, allow_empty=True)
    if not clean_value:
        return ""
    by_id, by_type, type_by_key = _pack_binding_maps(
        active_only=active_only,
        sendable_only=sendable_only,
    )
    if clean_value in by_type:
        return clean_value
    pack = by_id.get(clean_value)
    if isinstance(pack, dict):
        pack_type = str(pack.get("type") or "").strip()
        if pack_type:
            return pack_type
    binding_key = _pack_binding_key(clean_value)
    if binding_key:
        canonical_type = type_by_key.get(binding_key)
        if canonical_type:
            return canonical_type
    return clean_value


def _validate_flow_pack_bindings(
    flow_config: Dict[str, object],
    *,
    account_id: str = "",
) -> tuple[bool, str]:
    required_payload = _flow_required_action_types(flow_config)
    required_types = list(required_payload.get("required") or [])
    invalid_actions = list(required_payload.get("invalid_actions") or [])
    has_valid_actions = bool(required_payload.get("has_valid_actions"))
    if invalid_actions:
        account_suffix = f" (@{account_id})" if account_id else ""
        return False, "Flow con action_type invalido" + account_suffix + " -> " + ", ".join(sorted(set(invalid_actions)))
    if not required_types and not has_valid_actions:
        return False, "El flow no tiene acciones de envio configuradas."
    if not required_types:
        return True, ""
    send_counts = _active_pack_send_counts_by_type()
    send_counts_by_key: Dict[str, int] = {}
    for raw_type, raw_count in send_counts.items():
        key = _pack_binding_key(raw_type)
        if not key:
            continue
        prev = send_counts_by_key.get(key)
        normalized_count = int(raw_count or 0)
        if prev is None:
            send_counts_by_key[key] = normalized_count
        else:
            send_counts_by_key[key] = max(int(prev), normalized_count)
    missing_types: List[str] = []
    empty_pack_types: List[str] = []
    for action_type in required_types:
        action_count: Optional[int] = None
        if action_type in send_counts:
            action_count = int(send_counts.get(action_type, 0) or 0)
        else:
            action_key = _pack_binding_key(action_type)
            if action_key:
                action_count = int(send_counts_by_key.get(action_key, 0) or 0)
        if action_count is None:
            missing_types.append(action_type)
            continue
        if int(action_count) <= 0:
            empty_pack_types.append(action_type)
    if missing_types or empty_pack_types:
        parts: List[str] = []
        if missing_types:
            parts.append("sin pack activo: " + ", ".join(sorted(set(missing_types))))
        if empty_pack_types:
            parts.append("pack sin acciones de envÃ­o: " + ", ".join(sorted(set(empty_pack_types))))
        account_suffix = f" (@{account_id})" if account_id else ""
        return False, "Faltan packs vÃ¡lidos para el flow" + account_suffix + " -> " + " | ".join(parts)
    return True, ""


def _validate_flow_pack_bindings_for_activation(
    prompt_entry: Dict[str, object],
    *,
    account_id: str,
    followup_schedule_hours: Optional[List[int]] = None,
) -> tuple[bool, str]:
    try:
        flow_config = _resolve_flow_config_for_prompt_entry(
            prompt_entry,
            followup_schedule_hours=followup_schedule_hours,
            flow_required=True,
        )
    except Exception as exc:
        return False, str(exc)
    return _validate_flow_pack_bindings(flow_config, account_id=account_id)


def _active_pack_types() -> List[str]:
    ordered: List[str] = []
    for pack in _list_packs():
        if not bool(pack.get("active", False)):
            continue
        if not _pack_has_sendable_actions(pack):
            continue
        pack_type = str(pack.get("type") or "").strip()
        if not pack_type or pack_type in ordered:
            continue
        ordered.append(pack_type)
    return ordered


def _strategy_stage_from_label(label: object) -> str:
    token = _normalize_text_token(label)
    if not token:
        return ""
    # Prioridad: follow-up/link antes de invitacion/pitch para evitar cruces de etapa.
    if "follow" in token or "seguim" in token or "reactiv" in token or "retom" in token:
        return _FLOW_STAGE_FOLLOWUP
    if "link" in token or "calendly" in token:
        return _FLOW_STAGE_LINK
    if (
        "invit" in token
        or "agenda" in token
        or "agend" in token
        or "llamada" in token
        or "reunion" in token
        or "reunio" in token
        or "call" in token
        or "meet" in token
        or "zoom" in token
    ):
        return _FLOW_STAGE_INVITACION
    if (
        "pitch" in token
        or "peach" in token
        or "present" in token
        or "intro" in token
        or "saludo" in token
        or "apertura" in token
        or "inicio" in token
    ):
        return _FLOW_STAGE_PITCH
    return ""


def _strategy_stage_from_pack(pack: Dict[str, object]) -> str:
    for field_name in ("type", "name"):
        stage_name = _strategy_stage_from_label(pack.get(field_name))
        if stage_name:
            return stage_name
    return ""


def _active_pack_stage_lookup() -> Dict[str, str]:
    stage_by_type: Dict[str, str] = {}
    for pack in _list_packs():
        if not bool(pack.get("active", False)):
            continue
        if not _pack_has_sendable_actions(pack):
            continue
        type_name = str(pack.get("type") or "").strip()
        if not type_name or type_name in stage_by_type:
            continue
        stage_by_type[type_name] = _strategy_stage_from_pack(pack)
    for type_name in _active_pack_types():
        if not stage_by_type.get(type_name):
            stage_by_type[type_name] = _strategy_stage_from_label(type_name)
    return stage_by_type


def _is_no_send_strategy(strategy_name: object) -> bool:
    return _canonical_flow_special_action_type(strategy_name) == "no_send"


def _flow_followup_hours(
    followup_schedule_hours: Optional[List[int]] = None,
) -> List[float]:
    values: List[float] = []
    if isinstance(followup_schedule_hours, list):
        for item in followup_schedule_hours:
            try:
                hour_value = float(item)
            except Exception:
                continue
            if hour_value > 0:
                values.append(hour_value)
    if not values:
        values = list(_FLOW_DEFAULT_FOLLOWUP_HOURS)
    return values


def _normalize_flow_config(raw_config: object) -> Dict[str, object]:
    if isinstance(raw_config, list):
        raw_stages: List[object] = list(raw_config)
        entry_stage_id = ""
        version_value = _FLOW_CONFIG_VERSION
        allow_empty = False
        layout_raw: Dict[str, object] = {}
    elif isinstance(raw_config, dict):
        raw_stages = list(raw_config.get("stages") or [])
        entry_stage_id = _canonical_flow_stage_id(raw_config.get("entry_stage_id"))
        try:
            version_value = max(_FLOW_CONFIG_VERSION, int(raw_config.get("version") or _FLOW_CONFIG_VERSION))
        except Exception:
            version_value = _FLOW_CONFIG_VERSION
        allow_empty = bool(raw_config.get("allow_empty", False))
        layout_candidate = raw_config.get("layout")
        layout_raw = dict(layout_candidate) if isinstance(layout_candidate, dict) else {}
    else:
        return {
            "version": _FLOW_CONFIG_VERSION,
            "entry_stage_id": "",
            "stages": [],
            "allow_empty": False,
            "layout": {
                "nodes": {},
                "viewport": {"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0},
            },
        }

    normalized_stages: List[Dict[str, object]] = []
    seen_stage_ids: set[str] = set()
    for index, raw_stage in enumerate(raw_stages):
        stage = dict(raw_stage) if isinstance(raw_stage, dict) else {}
        stage_id = _canonical_flow_stage_id(stage.get("id")) or f"stage_{index + 1}"
        if stage_id in seen_stage_ids:
            suffix = 2
            candidate = f"{stage_id}_{suffix}"
            while candidate in seen_stage_ids:
                suffix += 1
                candidate = f"{stage_id}_{suffix}"
            stage_id = candidate
        seen_stage_ids.add(stage_id)
        action_type = _canonical_flow_action_type(stage.get("action_type"), allow_empty=True)
        transitions_raw = stage.get("transitions")
        transitions = dict(transitions_raw) if isinstance(transitions_raw, dict) else {}
        normalized_transitions: Dict[str, str] = {}
        for transition_key in ("positive", "negative", "doubt", "neutral"):
            target = _canonical_flow_stage_id(transitions.get(transition_key)) or stage_id
            normalized_transitions[transition_key] = target

        followups_raw = stage.get("followups")
        followups_input = list(followups_raw) if isinstance(followups_raw, list) else []
        normalized_followups: List[Dict[str, object]] = []
        for followup_item in followups_input:
            if isinstance(followup_item, dict):
                delay_raw = (
                    followup_item.get("delay_hours")
                    if followup_item.get("delay_hours") is not None
                    else followup_item.get("delay")
                )
                try:
                    delay_hours = float(delay_raw)
                except Exception:
                    continue
                if delay_hours < 0:
                    continue
                action = _canonical_flow_action_type(
                    followup_item.get("action_type")
                    or followup_item.get("type")
                    or action_type,
                    allow_empty=True,
                )
            else:
                try:
                    delay_hours = float(followup_item)
                except Exception:
                    continue
                if delay_hours < 0:
                    continue
                action = action_type
            if not action:
                continue
            normalized_followups.append(
                {
                    "delay_hours": delay_hours,
                    "action_type": action,
                }
            )
        normalized_followups.sort(key=lambda item: float(item.get("delay_hours") or 0.0))

        objection_raw = stage.get("post_objection")
        objection = dict(objection_raw) if isinstance(objection_raw, dict) else {}
        objection_action = _canonical_flow_action_type(objection.get("action_type"), allow_empty=True)
        try:
            objection_max_steps = int(
                objection.get("max_steps") or _FLOW_DEFAULT_OBJECTION_MAX_STEPS
            )
        except Exception:
            objection_max_steps = _FLOW_DEFAULT_OBJECTION_MAX_STEPS
        objection_cfg = {
            "enabled": bool(objection.get("enabled", False)),
            "action_type": objection_action,
            "max_steps": max(1, objection_max_steps),
            "resolved_transition": str(objection.get("resolved_transition") or "positive").strip() or "positive",
            "unresolved_transition": str(objection.get("unresolved_transition") or "negative").strip() or "negative",
        }

        normalized_stages.append(
            {
                "id": stage_id,
                "action_type": action_type,
                "transitions": normalized_transitions,
                "followups": normalized_followups,
                "post_objection": objection_cfg,
            }
        )

    stage_ids = [str(stage.get("id") or "").strip() for stage in normalized_stages]
    valid_stage_ids = {stage_id for stage_id in stage_ids if stage_id}
    for stage in normalized_stages:
        stage_id = str(stage.get("id") or "").strip()
        transitions = dict(stage.get("transitions") or {})
        for transition_key in ("positive", "negative", "doubt", "neutral"):
            target = _canonical_flow_stage_id(transitions.get(transition_key)) or stage_id
            if target not in valid_stage_ids:
                target = stage_id
            transitions[transition_key] = target
        stage["transitions"] = transitions
    entry_id = entry_stage_id if entry_stage_id in valid_stage_ids else (stage_ids[0] if stage_ids else "")
    nodes_raw = dict(layout_raw.get("nodes") or {}) if isinstance(layout_raw.get("nodes"), dict) else {}
    canonical_nodes_raw: Dict[str, object] = {}
    for raw_stage_id, raw_node in nodes_raw.items():
        clean_stage_id = _canonical_flow_stage_id(raw_stage_id) or str(raw_stage_id or "").strip()
        if clean_stage_id:
            canonical_nodes_raw[clean_stage_id] = raw_node
    normalized_nodes: Dict[str, Dict[str, float]] = {}
    for stage_id in stage_ids:
        node_raw = canonical_nodes_raw.get(stage_id)
        if not isinstance(node_raw, dict):
            continue
        x_value = _safe_float(node_raw.get("x"))
        y_value = _safe_float(node_raw.get("y"))
        if x_value is None or y_value is None:
            continue
        normalized_nodes[stage_id] = {"x": float(x_value), "y": float(y_value)}
    viewport_raw = (
        dict(layout_raw.get("viewport") or {})
        if isinstance(layout_raw.get("viewport"), dict)
        else {}
    )
    viewport_zoom = _safe_float(viewport_raw.get("zoom"))
    if viewport_zoom is None:
        viewport_zoom = 1.0
    viewport_pan_x = _safe_float(viewport_raw.get("pan_x"))
    if viewport_pan_x is None:
        viewport_pan_x = 0.0
    viewport_pan_y = _safe_float(viewport_raw.get("pan_y"))
    if viewport_pan_y is None:
        viewport_pan_y = 0.0
    return {
        "version": version_value,
        "entry_stage_id": entry_id,
        "stages": normalized_stages,
        "allow_empty": allow_empty,
        "layout": {
            "nodes": normalized_nodes,
            "viewport": {
                "zoom": max(0.45, min(2.6, float(viewport_zoom))),
                "pan_x": float(viewport_pan_x),
                "pan_y": float(viewport_pan_y),
            },
        },
    }


def _validate_and_normalize_flow_config(raw_config: object) -> Dict[str, object]:
    normalized = _normalize_flow_config(raw_config)
    stages_raw = normalized.get("stages")
    stages = list(stages_raw) if isinstance(stages_raw, list) else []
    if not stages:
        raise ValueError("El flow debe tener al menos una etapa.")
    stage_ids = {
        str(stage.get("id") or "").strip()
        for stage in stages
        if isinstance(stage, dict) and str(stage.get("id") or "").strip()
    }
    if not stage_ids:
        raise ValueError("El flow debe tener IDs de etapa validos.")
    entry_stage_id = str(normalized.get("entry_stage_id") or "").strip()
    if not entry_stage_id or entry_stage_id not in stage_ids:
        raise ValueError("La etapa inicial del flow es invalida.")

    validated_stages: List[Dict[str, object]] = []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        validated_stage = dict(stage)
        validated_stage["action_type"] = _canonical_flow_action_type(
            validated_stage.get("action_type"),
            strict=True,
        )
        followups_raw = validated_stage.get("followups")
        followups = list(followups_raw) if isinstance(followups_raw, list) else []
        validated_followups: List[Dict[str, object]] = []
        for followup in followups:
            if not isinstance(followup, dict):
                continue
            validated_followup = dict(followup)
            validated_followup["action_type"] = _canonical_flow_action_type(
                validated_followup.get("action_type"),
                strict=True,
            )
            validated_followups.append(validated_followup)
        validated_stage["followups"] = validated_followups

        objection_raw = validated_stage.get("post_objection")
        objection_cfg = dict(objection_raw) if isinstance(objection_raw, dict) else {}
        objection_enabled = bool(objection_cfg.get("enabled"))
        if objection_enabled:
            objection_cfg["action_type"] = _canonical_flow_action_type(
                objection_cfg.get("action_type"),
                strict=True,
            )
        else:
            objection_cfg["action_type"] = _canonical_flow_action_type(
                objection_cfg.get("action_type"),
                allow_empty=True,
            )
        validated_stage["post_objection"] = objection_cfg
        validated_stages.append(validated_stage)

    normalized["stages"] = validated_stages
    return normalized


def _default_flow_config_from_prompt_entry(
    prompt_entry: Dict[str, object],
    *,
    followup_schedule_hours: Optional[List[int]] = None,
) -> Dict[str, object]:
    active_types = _active_pack_types()
    stage_by_type = _active_pack_stage_lookup()
    ordered_main_types: List[str] = []
    for stage_label in (_FLOW_STAGE_PITCH, _FLOW_STAGE_INVITACION, _FLOW_STAGE_LINK):
        for type_name in active_types:
            resolved_stage = stage_by_type.get(type_name) or _strategy_stage_from_label(type_name)
            if resolved_stage != stage_label:
                continue
            if type_name in ordered_main_types:
                continue
            ordered_main_types.append(type_name)
            break
    if not ordered_main_types:
        fallback_non_followup = [
            type_name
            for type_name in active_types
            if (stage_by_type.get(type_name) or _strategy_stage_from_label(type_name)) != _FLOW_STAGE_FOLLOWUP
        ]
        if fallback_non_followup:
            ordered_main_types.append(fallback_non_followup[0])
    if not ordered_main_types and active_types:
        ordered_main_types.append(active_types[0])

    objection_action = str(prompt_entry.get("objection_strategy_name") or "").strip()
    stages: List[Dict[str, object]] = []
    for idx, type_name in enumerate(ordered_main_types):
        stage_id = f"stage_{idx + 1}"
        if idx + 1 < len(ordered_main_types):
            positive_target = f"stage_{idx + 2}"
        else:
            positive_target = stage_id
        stage = {
            "id": stage_id,
            "action_type": type_name,
            "transitions": {
                "positive": positive_target,
                "negative": stage_id,
                "doubt": stage_id,
                "neutral": stage_id,
            },
            "followups": [],
            "post_objection": {
                "enabled": bool(objection_action),
                "action_type": objection_action,
                "max_steps": _FLOW_DEFAULT_OBJECTION_MAX_STEPS,
                "resolved_transition": "positive",
                "unresolved_transition": "negative",
            },
        }
        stages.append(stage)

    followup_types = [
        type_name
        for type_name in active_types
        if (stage_by_type.get(type_name) or _strategy_stage_from_label(type_name)) == _FLOW_STAGE_FOLLOWUP
    ]
    if followup_types and stages:
        followup_hours = _flow_followup_hours(followup_schedule_hours)
        followup_plan: List[Dict[str, object]] = []
        for idx, followup_type in enumerate(followup_types):
            if idx < len(followup_hours):
                delay_hours = float(followup_hours[idx])
            else:
                delay_hours = float(followup_hours[-1]) + float(max(1, idx - len(followup_hours) + 1) * 24)
            followup_plan.append(
                {
                    "delay_hours": delay_hours,
                    "action_type": followup_type,
                }
            )
        for stage in stages:
            stage["followups"] = [dict(item) for item in followup_plan]

    return _normalize_flow_config(
        {
            "version": _FLOW_CONFIG_VERSION,
            "entry_stage_id": "stage_1" if stages else "",
            "stages": stages,
            "allow_empty": False,
        }
    )


def _resolve_flow_config_for_prompt_entry(
    prompt_entry: Dict[str, object],
    *,
    followup_schedule_hours: Optional[List[int]] = None,
    flow_required: bool = FLOW_CONFIG_REQUIRED,
) -> Dict[str, object]:
    alias_label = str(prompt_entry.get("alias") or "-").strip() or "-"
    raw_config = prompt_entry.get("flow_config")
    normalized = _normalize_flow_config(raw_config)
    stages = normalized.get("stages", [])
    has_stages = isinstance(stages, list) and bool(stages)
    valid_stage_ids = {
        str(stage.get("id") or "").strip()
        for stage in stages
        if isinstance(stage, dict) and str(stage.get("id") or "").strip()
    } if isinstance(stages, list) else set()
    entry_stage_id = str(normalized.get("entry_stage_id") or "").strip()
    entry_valid = bool(entry_stage_id) and entry_stage_id in valid_stage_ids

    if flow_required:
        if not isinstance(raw_config, dict):
            logger.error(
                "FLOW_REQUIRED_BUT_MISSING alias=%s reason=flow_config_absent",
                alias_label,
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        raw_stages = raw_config.get("stages")
        if not isinstance(raw_stages, list) or not raw_stages:
            logger.error(
                "FLOW_REQUIRED_BUT_MISSING alias=%s reason=stages_empty",
                alias_label,
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        raw_stage_ids = {
            str(stage.get("id") or "").strip()
            for stage in raw_stages
            if isinstance(stage, dict) and str(stage.get("id") or "").strip()
        }
        raw_stage_ids = {stage_id for stage_id in raw_stage_ids if stage_id}
        if not raw_stage_ids:
            logger.error(
                "FLOW_REQUIRED_INVALID_STRUCTURE alias=%s reason=stages_without_valid_ids",
                alias_label,
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        raw_entry_stage_id = str(raw_config.get("entry_stage_id") or "").strip()
        if not raw_entry_stage_id or raw_entry_stage_id not in raw_stage_ids:
            logger.error(
                "FLOW_REQUIRED_INVALID_STRUCTURE alias=%s reason=entry_stage_invalid entry_stage_id=%s",
                alias_label,
                raw_entry_stage_id or "-",
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        if not has_stages:
            logger.error(
                "FLOW_REQUIRED_INVALID_STRUCTURE alias=%s reason=stages_invalid",
                alias_label,
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        if not entry_valid:
            logger.error(
                "FLOW_REQUIRED_INVALID_STRUCTURE alias=%s reason=entry_stage_invalid entry_stage_id=%s",
                alias_label,
                entry_stage_id or "-",
            )
            raise FlowConfigRequiredError("Flow obligatorio no configurado")
        return normalized

    if has_stages:
        return normalized
    if bool(normalized.get("allow_empty", False)):
        return normalized
    return _default_flow_config_from_prompt_entry(
        prompt_entry,
        followup_schedule_hours=followup_schedule_hours,
    )


def _parse_outbox_action_key(action_key: str) -> Dict[str, object]:
    parts = str(action_key or "").strip().split(":")
    if len(parts) < 5:
        return {}
    thread_token = str(parts[0] or "").strip()
    pack_id = str(parts[1] or "").strip()
    stage_id = str(parts[3] or "").strip()
    anchor_token = ":".join(parts[4:]).strip()
    try:
        action_index = int(parts[2])
    except Exception:
        action_index = -1
    if not thread_token or not pack_id or action_index < 0:
        return {}
    return {
        "thread_token": thread_token,
        "pack_id": pack_id,
        "action_index": action_index,
        "stage_id": stage_id,
        "anchor_token": anchor_token,
    }


def _outbox_anchor_token_to_float(anchor_token: object) -> Optional[float]:
    try:
        value = float(str(anchor_token or "").strip())
    except Exception:
        return None
    if value <= 0:
        return None
    return value


def _stage_id_for_action_type_exact(flow_config: Dict[str, object], action_type: str) -> str:
    action_token = _flow_action_token(_canonical_pack_strategy_name(action_type))
    if not action_token:
        return ""
    stage_items = flow_config.get("stages", [])
    if not isinstance(stage_items, list):
        return ""
    for stage in stage_items:
        if not isinstance(stage, dict):
            continue
        stage_id = str(stage.get("id") or "").strip()
        stage_action = _canonical_pack_strategy_name(stage.get("action_type"))
        if stage_id and _flow_action_token(stage_action) == action_token:
            return stage_id
    return ""


def _is_objection_action_type_for_flow(action_type: str, flow_config: Dict[str, object]) -> bool:
    token = _flow_action_token(_canonical_pack_strategy_name(action_type))
    if not token:
        return False
    if token in _FLOW_OBJECTION_ACTION_ALIASES:
        return True
    stage_items = flow_config.get("stages", [])
    if not isinstance(stage_items, list):
        return False
    for stage in stage_items:
        if not isinstance(stage, dict):
            continue
        post_obj = stage.get("post_objection")
        if not isinstance(post_obj, dict):
            continue
        if not bool(post_obj.get("enabled", False)):
            continue
        configured = _canonical_pack_strategy_name(post_obj.get("action_type"))
        if configured and _flow_action_token(configured) == token:
            return True
    return False


def _history_outbox_sent_actions(
    conv_state: Dict[str, Any],
    flow_config: Dict[str, object],
) -> List[Dict[str, object]]:
    entry_stage_id = str(flow_config.get("entry_stage_id") or "").strip()
    flow_state = _normalize_flow_state(
        conv_state.get("flow_state"),
        fallback_stage_id=entry_stage_id,
        last_outbound_ts=_safe_float(conv_state.get("last_message_sent_at")),
        followup_level_hint=_safe_int(conv_state.get("followup_stage")),
    )
    outbox_raw = flow_state.get("outbox")
    outbox = dict(outbox_raw) if isinstance(outbox_raw, dict) else {}
    if not outbox:
        return []
    thread_id = str(conv_state.get("thread_id") or "").strip()
    pack_lookup = {
        str(pack.get("id") or "").strip(): pack
        for pack in _list_packs()
        if isinstance(pack, dict) and str(pack.get("id") or "").strip()
    }
    events: List[Dict[str, object]] = []
    for action_key, raw_entry in outbox.items():
        entry = dict(raw_entry) if isinstance(raw_entry, dict) else {}
        if str(entry.get("status") or "").strip().lower() != "sent":
            continue
        sent_at = _safe_float(entry.get("sent_at"))
        if sent_at is None or sent_at <= 0:
            continue
        parsed = _parse_outbox_action_key(str(action_key))
        if not parsed:
            continue
        parsed_thread = str(parsed.get("thread_token") or "").strip()
        if thread_id and parsed_thread and parsed_thread != thread_id:
            continue
        pack_id = str(parsed.get("pack_id") or "").strip()
        action_index = _safe_int(parsed.get("action_index"))
        stage_id = _canonical_flow_stage_id(parsed.get("stage_id"))
        anchor_ts = _outbox_anchor_token_to_float(parsed.get("anchor_token"))
        pack = pack_lookup.get(pack_id)
        action_type = str(pack.get("type") or "").strip() if isinstance(pack, dict) else ""
        is_followup_action = _strategy_stage_from_label(action_type) == _FLOW_STAGE_FOLLOWUP
        is_objection_action = _is_objection_action_type_for_flow(action_type, flow_config)
        events.append(
            {
                "source": "outbox",
                "sent_at": float(sent_at),
                "pack_id": pack_id,
                "action_index": action_index,
                "stage_id": stage_id,
                "anchor_ts": anchor_ts,
                "action_type": action_type,
                "is_followup_action": is_followup_action,
                "is_objection_action": is_objection_action,
            }
        )
    events.sort(
        key=lambda item: (
            float(item.get("sent_at") or 0.0),
            int(item.get("action_index") or 0),
        )
    )
    return events


def _last_outbound_history_event(
    conv_state: Dict[str, Any],
    flow_config: Dict[str, object],
) -> Dict[str, object]:
    outbox_events = _history_outbox_sent_actions(conv_state, flow_config)
    last_outbox = outbox_events[-1] if outbox_events else {}
    last_outbox_ts = _safe_float(last_outbox.get("sent_at")) or 0.0

    messages_sent = conv_state.get("messages_sent")
    last_message: Dict[str, object] = {}
    if isinstance(messages_sent, list):
        best_ts = 0.0
        for raw_item in messages_sent:
            if not isinstance(raw_item, dict):
                continue
            sent_ts = _safe_float(raw_item.get("last_sent_at"))
            if sent_ts is None or sent_ts <= 0:
                sent_ts = _safe_float(raw_item.get("first_sent_at"))
            if sent_ts is None or sent_ts <= 0:
                continue
            if sent_ts >= best_ts:
                best_ts = sent_ts
                last_message = {
                    "source": "history",
                    "sent_at": float(sent_ts),
                    "is_followup_message": bool(raw_item.get("is_followup", False)),
                }
    last_message_ts = _safe_float(last_message.get("sent_at")) or 0.0
    if last_outbox_ts <= 0 and last_message_ts <= 0:
        return {}
    if last_outbox_ts >= last_message_ts:
        return dict(last_outbox)
    return dict(last_message)


def _flow_state_is_corrupt(raw_flow_state: object) -> bool:
    if raw_flow_state is None:
        return False
    if not isinstance(raw_flow_state, dict):
        return True
    stage_id_raw = raw_flow_state.get("stage_id")
    if stage_id_raw is not None and not isinstance(stage_id_raw, str):
        return True
    for field_name in ("version", "followup_level", "objection_step"):
        value = raw_flow_state.get(field_name)
        if value is None:
            continue
        try:
            if int(value) < 0:
                return True
        except Exception:
            return True
    for dict_field in ("outbox", "last_outbound_fingerprint"):
        value = raw_flow_state.get(dict_field)
        if value is None:
            continue
        if not isinstance(value, dict):
            return True
    return False


def _infer_flow_stage_from_history(
    conv_state: Dict[str, Any],
    flow_config: Dict[str, object],
) -> tuple[str, str]:
    stage_items = flow_config.get("stages", [])
    stage_list = [stage for stage in stage_items if isinstance(stage, dict)]
    stage_ids = [str(stage.get("id") or "").strip() for stage in stage_list]
    stage_ids = [stage_id for stage_id in stage_ids if stage_id]
    entry_stage_id = str(flow_config.get("entry_stage_id") or "").strip()
    if entry_stage_id not in stage_ids:
        entry_stage_id = stage_ids[0] if stage_ids else ""

    last_event = _last_outbound_history_event(conv_state, flow_config)
    if not last_event:
        return entry_stage_id, "no_outbound"
    if str(last_event.get("source") or "").strip() != "outbox":
        return entry_stage_id, "legacy_migrated"

    event_stage_id = _canonical_flow_stage_id(last_event.get("stage_id"))
    action_type = str(last_event.get("action_type") or "").strip()
    mapped_stage_id = _stage_id_for_action_type_exact(flow_config, action_type)
    is_followup_action = bool(last_event.get("is_followup_action", False))
    is_objection_action = bool(last_event.get("is_objection_action", False))

    if is_followup_action or is_objection_action:
        if event_stage_id in stage_ids:
            return event_stage_id, ""
        if mapped_stage_id in stage_ids:
            return mapped_stage_id, ""
        return entry_stage_id, "legacy_migrated"

    if mapped_stage_id in stage_ids:
        return mapped_stage_id, ""
    return entry_stage_id, "legacy_migrated"


def _reconstruct_flow_state_for_thread(
    conv_state: Dict[str, Any],
    flow_config: Dict[str, object],
) -> Dict[str, object]:
    stage_id, reconstruction_status = _infer_flow_stage_from_history(conv_state, flow_config)
    outbox_events = _history_outbox_sent_actions(conv_state, flow_config)
    last_event = _last_outbound_history_event(conv_state, flow_config)
    last_outbound_ts = _safe_float(conv_state.get("last_message_sent_at"))
    event_ts = _safe_float(last_event.get("sent_at"))
    if last_outbound_ts is None or (event_ts is not None and event_ts > last_outbound_ts):
        last_outbound_ts = event_ts

    followup_level = 0
    objection_step = 0
    anchor_ts = _safe_float(last_event.get("anchor_ts")) or last_outbound_ts
    source = str(last_event.get("source") or "").strip()
    is_followup_action = bool(last_event.get("is_followup_action", False))
    is_objection_action = bool(last_event.get("is_objection_action", False))

    if source == "outbox":
        event_stage_id = str(last_event.get("stage_id") or "").strip()
        if is_followup_action:
            followup_candidates = [
                item
                for item in outbox_events
                if bool(item.get("is_followup_action", False))
                and str(item.get("stage_id") or "").strip() == stage_id
                and (_safe_float(item.get("sent_at")) or 0.0) <= (_safe_float(last_event.get("sent_at")) or 0.0)
            ]
            followup_level = len(followup_candidates)
            if event_stage_id and event_stage_id != stage_id:
                followup_level = 0
        elif is_objection_action:
            counted = 0
            last_event_ts = _safe_float(last_event.get("sent_at")) or 0.0
            for item in reversed(outbox_events):
                item_ts = _safe_float(item.get("sent_at")) or 0.0
                if item_ts > last_event_ts:
                    continue
                if str(item.get("stage_id") or "").strip() != stage_id:
                    if counted > 0:
                        break
                    continue
                if bool(item.get("is_objection_action", False)):
                    counted += 1
                    continue
                if counted > 0:
                    break
                break
            objection_step = counted

    stage_for_followup = None
    stage_items = flow_config.get("stages", [])
    if isinstance(stage_items, list):
        for stage in stage_items:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("id") or "").strip() == stage_id:
                stage_for_followup = stage
                break
    if isinstance(stage_for_followup, dict):
        followups = stage_for_followup.get("followups")
        followup_count = len(followups) if isinstance(followups, list) else 0
        if followup_count > 0:
            followup_level = min(max(0, followup_level), followup_count)
        else:
            followup_level = 0
        post_obj = stage_for_followup.get("post_objection")
        max_steps = _FLOW_DEFAULT_OBJECTION_MAX_STEPS
        if isinstance(post_obj, dict):
            max_steps = max(1, _safe_int(post_obj.get("max_steps") or _FLOW_DEFAULT_OBJECTION_MAX_STEPS))
        objection_step = min(max(0, objection_step), max_steps)
    else:
        followup_level = 0
        objection_step = 0

    created_ts = _safe_float(conv_state.get("created_at")) or time.time()
    updated_ts = _safe_float(conv_state.get("updated_at")) or created_ts
    reconstructed = _normalize_flow_state(
        {
            "version": _FLOW_CONFIG_VERSION,
            "stage_id": stage_id,
            "last_outbound_ts": last_outbound_ts,
            "followup_level": followup_level,
            "followup_anchor_ts": anchor_ts,
            "objection_step": objection_step,
            "last_stage_change_ts": updated_ts or created_ts,
            "reconstruction_status": reconstruction_status,
        },
        fallback_stage_id=stage_id,
        last_outbound_ts=last_outbound_ts,
        followup_level_hint=followup_level,
    )
    return reconstructed


def _reconstruct_flow_state_if_needed(thread_context: Dict[str, object]) -> Dict[str, object]:
    account_id = str(thread_context.get("account_id") or "").strip()
    thread_id = str(thread_context.get("thread_id") or "").strip()
    recipient_username = str(thread_context.get("recipient_username") or "").strip()
    alias = str(thread_context.get("alias") or ACTIVE_ALIAS or account_id or "").strip() or "-"
    raw_flow_config = thread_context.get("flow_config")
    flow_config = _normalize_flow_config(raw_flow_config if isinstance(raw_flow_config, dict) else {})
    conv_raw = thread_context.get("conv_state")
    conv_state = dict(conv_raw) if isinstance(conv_raw, dict) else _get_conversation_state(account_id, thread_id)
    entry_stage_id = str(flow_config.get("entry_stage_id") or "").strip()
    stage_items = flow_config.get("stages", [])
    stage_ids = {
        str(stage.get("id") or "").strip()
        for stage in stage_items
        if isinstance(stage, dict) and str(stage.get("id") or "").strip()
    }

    flow_raw = conv_state.get("flow_state")
    normalized = _normalize_flow_state(
        flow_raw,
        fallback_stage_id=entry_stage_id,
        last_outbound_ts=_safe_float(conv_state.get("last_message_sent_at")),
        followup_level_hint=_safe_int(conv_state.get("followup_stage")),
    )
    normalized_stage_id = str(normalized.get("stage_id") or "").strip()
    raw_stage_id = (
        str(flow_raw.get("stage_id") or "").strip()
        if isinstance(flow_raw, dict)
        else ""
    )
    is_missing_stage = not raw_stage_id
    is_invalid_stage = bool(stage_ids) and normalized_stage_id not in stage_ids
    is_corrupt = _flow_state_is_corrupt(flow_raw)
    should_reconstruct = is_missing_stage or is_invalid_stage or is_corrupt

    if not should_reconstruct:
        if not (isinstance(flow_raw, dict) and flow_raw == normalized):
            _update_conversation_state(
                account_id,
                thread_id,
                {
                    "flow_state": normalized,
                    "followup_stage": _safe_int(normalized.get("followup_level")),
                },
                recipient_username=recipient_username or None,
            )
        logger.info(
            "FLOW_RECONSTRUCTION_NOT_NEEDED thread_id=%s alias=%s stage_id=%s reconstruction_status=%s",
            thread_id or "-",
            alias,
            normalized_stage_id or "-",
            str(normalized.get("reconstruction_status") or "ok"),
        )
        return normalized

    trigger_reason = "missing_stage"
    if is_invalid_stage:
        trigger_reason = "invalid_stage"
    elif is_corrupt:
        trigger_reason = "corrupt_state"
    logger.info(
        "FLOW_RECONSTRUCTION_TRIGGERED thread_id=%s alias=%s stage_id=%s reconstruction_status=%s reason=%s",
        thread_id or "-",
        alias,
        normalized_stage_id or "-",
        str(normalized.get("reconstruction_status") or "-"),
        trigger_reason,
    )

    try:
        reconstructed = _reconstruct_flow_state_for_thread(conv_state, flow_config)
        _update_conversation_state(
            account_id,
            thread_id,
            {
                "flow_state": reconstructed,
                "followup_stage": _safe_int(reconstructed.get("followup_level")),
            },
            recipient_username=recipient_username or None,
        )
        reconstruction_status = str(reconstructed.get("reconstruction_status") or "").strip() or "ok"
        stage_id = str(reconstructed.get("stage_id") or "").strip() or "-"
        if reconstruction_status in {"legacy_migrated", "no_outbound"}:
            logger.warning(
                "FLOW_RECONSTRUCTION_LOW_CONFIDENCE thread_id=%s alias=%s stage_id=%s reconstruction_status=%s",
                thread_id or "-",
                alias,
                stage_id,
                reconstruction_status,
            )
        else:
            logger.info(
                "FLOW_RECONSTRUCTION_SUCCESS thread_id=%s alias=%s stage_id=%s reconstruction_status=%s",
                thread_id or "-",
                alias,
                stage_id,
                reconstruction_status,
            )
        return reconstructed
    except Exception as exc:
        fallback = _normalize_flow_state(
            {
                "stage_id": entry_stage_id,
                "last_outbound_ts": _safe_float(conv_state.get("last_message_sent_at")),
                "followup_level": 0,
                "followup_anchor_ts": _safe_float(conv_state.get("last_message_sent_at")),
                "objection_step": 0,
                "reconstruction_status": "error_fallback",
            },
            fallback_stage_id=entry_stage_id,
            last_outbound_ts=_safe_float(conv_state.get("last_message_sent_at")),
            followup_level_hint=0,
        )
        _update_conversation_state(
            account_id,
            thread_id,
            {
                "flow_state": fallback,
                "followup_stage": 0,
            },
            recipient_username=recipient_username or None,
        )
        logger.warning(
            "FLOW_RECONSTRUCTION_LOW_CONFIDENCE thread_id=%s alias=%s stage_id=%s reconstruction_status=%s",
            thread_id or "-",
            alias,
            str(fallback.get("stage_id") or "-"),
            "error_fallback",
        )
        logger.warning(
            "FLOW_RECONSTRUCTION_ERROR thread_id=%s alias=%s reason=%s",
            thread_id or "-",
            alias,
            str(exc),
        )
        return fallback


def _ensure_flow_state_for_thread(
    account_id: str,
    thread_id: str,
    *,
    recipient_username: str,
    conv_state: Dict[str, Any],
    flow_config: Dict[str, object],
) -> Dict[str, object]:
    return _reconstruct_flow_state_if_needed(
        {
            "account_id": account_id,
            "thread_id": thread_id,
            "recipient_username": recipient_username,
            "conv_state": conv_state,
            "flow_config": flow_config,
            "alias": ACTIVE_ALIAS or account_id,
        }
    )


def _flow_stage_id_for_action(
    flow_config: Dict[str, object],
    action_type: str,
    *,
    current_stage_id: str = "",
) -> str:
    stage_items = flow_config.get("stages", [])
    stages = [stage for stage in stage_items if isinstance(stage, dict)]
    if not stages:
        return str(current_stage_id or "").strip()
    stage_ids = [str(stage.get("id") or "").strip() for stage in stages]
    stage_ids = [stage_id for stage_id in stage_ids if stage_id]
    if not stage_ids:
        return str(current_stage_id or "").strip()
    current_id = str(current_stage_id or "").strip()
    current_idx = stage_ids.index(current_id) if current_id in stage_ids else 0
    action_token = _flow_action_token(action_type)
    if not action_token:
        return stage_ids[current_idx]
    target_idx = current_idx
    for idx, stage in enumerate(stages):
        stage_action = str(stage.get("action_type") or "").strip()
        if _flow_action_token(stage_action) != action_token:
            continue
        target_idx = idx
        break
    if target_idx < current_idx:
        target_idx = current_idx
    if target_idx > current_idx + 1:
        target_idx = min(current_idx + 1, len(stage_ids) - 1)
    return stage_ids[target_idx]


def _flow_config_for_account(
    account_id: str,
    *,
    followup_schedule_hours: Optional[List[int]] = None,
) -> Dict[str, object]:
    prompt_entry = _resolve_prompt_entry_for_user(
        account_id,
        active_alias=ACTIVE_ALIAS,
        fallback_entry=_get_prompt_entry(ACTIVE_ALIAS or account_id),
    )
    return _resolve_flow_config_for_prompt_entry(
        prompt_entry,
        followup_schedule_hours=followup_schedule_hours,
    )


def _apply_legacy_pack_markers(
    account_id: str,
    thread_id: str,
    *,
    recipient_username: Optional[str],
    legacy_payload: object,
    stage_id_hint: str = "",
    flow_config: Optional[Dict[str, object]] = None,
) -> Dict[str, bool]:
    _ = (
        account_id,
        thread_id,
        recipient_username,
        legacy_payload,
        stage_id_hint,
        flow_config,
    )
    return {}


def select_pack(strategy_name: str, account_id: str) -> Optional[Dict[str, object]]:
    strategy_clean = _canonical_pack_strategy_name(
        strategy_name,
        active_only=True,
        sendable_only=True,
    )
    if not strategy_clean:
        return None
    active_packs = [
        pack
        for pack in _list_packs()
        if (
            bool(pack.get("active", False))
            and str(pack.get("type") or "").strip() == strategy_clean
            and _pack_has_sendable_actions(pack)
        )
    ]
    if not active_packs:
        logger.error(
            "No hay packs activos para estrategia '%s' en cuenta @%s",
            strategy_clean,
            account_id,
        )
        return None

    memory = _get_account_memory(account_id)
    last_pack_used = memory.get("last_pack_used")
    if not isinstance(last_pack_used, dict):
        last_pack_used = {}
    last_pack_id = str(last_pack_used.get(strategy_clean) or "").strip()
    candidates = [
        pack
        for pack in active_packs
        if str(pack.get("id") or "").strip() != last_pack_id
    ]
    if not candidates:
        logger.info(
            "Sin alternativos para estrategia '%s' en @%s (ultimo=%s); se reutiliza pack.",
            strategy_clean,
            account_id,
            last_pack_id or "-",
        )
        candidates = list(active_packs)
    selected = random.choice(candidates)
    selected_id = str(selected.get("id") or "").strip()
    if not selected_id:
        return None
    last_pack_used[strategy_clean] = selected_id
    memory["last_pack_used"] = last_pack_used
    _set_account_memory(account_id, memory)
    return selected


def _emit_pack_selected_event(
    *,
    account: str,
    thread_id: str,
    recipient_username: str,
    pack: object,
    fallback_type: str = "",
) -> None:
    if not isinstance(pack, dict):
        return
    pack_id = str(pack.get("id") or "").strip()
    pack_type = str(pack.get("type") or fallback_type or "").strip()
    if not pack_id and not pack_type:
        return
    _emit_autoresponder_event(
        "PACK_SELECTED",
        account=account,
        thread_id=thread_id,
        recipient=recipient_username,
        pack_id=pack_id,
        pack_type=pack_type,
    )


def _generate_pack_adaptive_text(
    api_key: str,
    instruction: str,
    *,
    conversation_text: str,
    strategy_name: str,
    memory: Dict[str, object],
) -> str:
    instruction_clean = str(instruction or "").strip()
    if not instruction_clean or not api_key:
        return ""
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning("No se pudo inicializar cliente IA para text_adaptive: %s", exc, exc_info=False)
        return ""
    model = _resolve_ai_model(api_key)
    system_prompt = (
        "Sos un generador de acciones text_adaptive para Instagram DM.\n"
        "Debes seguir EXACTAMENTE la instruction recibida.\n"
        "Salida obligatoria: SOLO el texto final a enviar al lead, sin JSON."
    )
    user_content = (
        f"instruction:\n{instruction_clean}\n\n"
        f"strategy_name: {strategy_name or '-'}\n"
        f"memory: {json.dumps(memory or {}, ensure_ascii=False)}\n\n"
        f"conversacion:\n{conversation_text or '(sin conversacion)'}"
    )
    try:  # pragma: no cover - depende de red externa
        raw = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=user_content,
            model=model,
            temperature=0.2,
            max_output_tokens=240,
        )
    except Exception as exc:
        logger.warning("No se pudo generar text_adaptive con OpenAI: %s", exc, exc_info=False)
        return ""
    candidate = _sanitize_generated_message(raw)
    if _generated_message_issues(candidate):
        return ""
    return candidate


def _gen_response(
    *,
    api_key: str,
    system_prompt: str,
    convo_text: str,
    memory_context: str = "",
    max_attempts: int = 2,
) -> str:
    prompt_clean = str(system_prompt or "").strip()
    if not prompt_clean or not api_key:
        return ""
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning("No se pudo inicializar cliente IA para _gen_response: %s", exc, exc_info=False)
        return ""

    model = _resolve_ai_model(api_key)
    user_content = (
        f"memory_context:\n{str(memory_context or '').strip() or '-'}\n\n"
        f"conversacion:\n{str(convo_text or '').strip() or '(sin conversacion)'}"
    )
    attempts = max(1, int(max_attempts or 1))
    for _ in range(attempts):
        try:
            raw = _openai_generate_text(
                client,
                system_prompt=prompt_clean,
                user_content=user_content,
                model=model,
                temperature=0.3,
                max_output_tokens=260,
            )
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if status in {401, 403}:
                return ""
            logger.warning("No se pudo generar respuesta con OpenAI: %s", exc, exc_info=False)
            return ""

        candidate = _sanitize_generated_message(raw)
        if not _generated_message_issues(candidate):
            return candidate
    return ""


def _followup_decision(
    *,
    api_key: str,
    prompt_text: str,
    conversation: str,
    metadata: Optional[Dict[str, object]] = None,
    max_attempts: int = 2,
) -> tuple[str, int]:
    prompt_clean = str(prompt_text or "").strip()
    if not prompt_clean or not api_key:
        return "", max(1, _safe_int((metadata or {}).get("intento_followup_siguiente")) or 1)

    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning("No se pudo inicializar cliente IA para followup: %s", exc, exc_info=False)
        return "", max(1, _safe_int((metadata or {}).get("intento_followup_siguiente")) or 1)

    model = _resolve_ai_model(api_key)
    meta = dict(metadata or {})
    user_content = (
        "Devuelve JSON estricto con las claves enviar(bool), mensaje(str) y etapa(int).\n"
        f"prompt:\n{prompt_clean}\n\n"
        f"metadata:\n{json.dumps(meta, ensure_ascii=False)}\n\n"
        f"conversacion:\n{str(conversation or '').strip() or '(sin conversacion)'}"
    )
    default_stage = max(1, _safe_int(meta.get("intento_followup_siguiente")) or 1)
    attempts = max(1, int(max_attempts or 1))
    for _ in range(attempts):
        try:
            raw = _openai_generate_text(
                client,
                system_prompt="Sos un decisor de followups. Responde SOLO JSON vÃ¡lido.",
                user_content=user_content,
                model=model,
                temperature=0.1,
                max_output_tokens=220,
            )
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if status in {401, 403}:
                return "", default_stage
            logger.warning("No se pudo generar decisiÃ³n de followup: %s", exc, exc_info=False)
            return "", default_stage

        try:
            payload = json.loads(str(raw or "").strip())
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if not bool(payload.get("enviar")):
            return "", max(1, _safe_int(payload.get("etapa")) or default_stage)
        message_text = _sanitize_generated_message(str(payload.get("mensaje") or "").strip())
        stage_value = max(1, _safe_int(payload.get("etapa")) or default_stage)
        if _generated_message_issues(message_text):
            continue
        return message_text, stage_value
    return "", default_stage


def _generate_autoreply_response(
    mensaje_usuario: str,
    prompt_autorespuesta: str,
    *,
    api_key: str,
    conversation_text: str,
    account_memory: Dict[str, object],
) -> str:
    prompt_clean = str(prompt_autorespuesta or "").strip()
    if not prompt_clean or not api_key:
        return ""
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning("No se pudo inicializar cliente IA para autorrespuesta: %s", exc, exc_info=False)
        return ""
    model = _resolve_ai_model(api_key)
    system_prompt = (
        "Sos el engine de autorrespuesta de Instagram DM.\n"
        "Debes seguir AL PIE DE LA LETRA el PROMPT_AUTORESPUESTA.\n"
        "Salida obligatoria: SOLO el mensaje final para el lead, sin JSON.\n\n"
        "<PROMPT_AUTORESPUESTA>\n"
        f"{prompt_clean}\n"
        "</PROMPT_AUTORESPUESTA>"
    )
    user_content = (
        f"ultimo_mensaje_lead: {mensaje_usuario or '(vacio)'}\n"
        f"memory: {json.dumps(account_memory or {}, ensure_ascii=False)}\n\n"
        f"conversacion:\n{conversation_text or '(sin conversacion)'}"
    )
    try:  # pragma: no cover - depende de red externa
        raw = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=user_content,
            model=model,
            temperature=0.3,
            max_output_tokens=260,
        )
    except Exception as exc:
        logger.warning("No se pudo generar autorrespuesta con OpenAI: %s", exc, exc_info=False)
        return ""
    candidate = _sanitize_generated_message(raw)
    if _generated_message_issues(candidate):
        return ""
    return candidate


def generate_objection_response(
    mensaje_usuario: str,
    prompt_objeciones: str,
    memory: Dict[str, object],
    *,
    api_key: str,
    conversation_text: str,
) -> str:
    prompt_clean = str(prompt_objeciones or "").strip()
    if not prompt_clean or not api_key:
        return ""
    try:
        client = _build_openai_client(api_key)
    except Exception as exc:
        logger.warning("No se pudo inicializar cliente IA para objeciones: %s", exc, exc_info=False)
        return ""
    model = _resolve_ai_model(api_key)
    system_prompt = (
        "Sos el objection engine de Instagram DM.\n"
        "Debes seguir AL PIE DE LA LETRA el PROMPT_OBJECIONES.\n"
        "Salida obligatoria: SOLO el mensaje final para el lead, sin JSON.\n\n"
        "<PROMPT_OBJECIONES>\n"
        f"{prompt_clean}\n"
        "</PROMPT_OBJECIONES>"
    )
    user_content = (
        f"ultimo_mensaje_lead: {mensaje_usuario or '(vacio)'}\n"
        f"memory: {json.dumps(memory or {}, ensure_ascii=False)}\n\n"
        f"conversacion:\n{conversation_text or '(sin conversacion)'}"
    )
    try:  # pragma: no cover - depende de red externa
        raw = _openai_generate_text(
            client,
            system_prompt=system_prompt,
            user_content=user_content,
            model=model,
            temperature=0.3,
            max_output_tokens=260,
        )
    except Exception as exc:
        logger.warning("No se pudo generar respuesta de objeciones: %s", exc, exc_info=False)
        return ""
    candidate = _sanitize_generated_message(raw)
    if _generated_message_issues(candidate):
        return ""
    return candidate


def _thread_target_hint_from_thread(thread: object) -> tuple[str, str]:
    href_value = _normalize_thread_href(getattr(thread, "link", ""))
    thread_id = _extract_thread_id_from_href(href_value)
    if not thread_id:
        raw_thread_id = str(getattr(thread, "id", "") or "").strip()
        if _is_probably_web_thread_id(raw_thread_id):
            thread_id = raw_thread_id
            if not href_value:
                href_value = f"https://www.instagram.com/direct/t/{raw_thread_id}/"
    return href_value, thread_id


def _reopen_thread_best_effort(
    client: object,
    thread: object,
    *,
    preferred_href: str = "",
    open_kwargs: Optional[Dict[str, object]] = None,
) -> bool:
    open_thread_fn = getattr(client, "_open_thread", None)
    open_by_href_fn = getattr(client, "open_thread_by_href", None)
    fallback_href = _normalize_thread_href(preferred_href)
    open_kwargs_payload = dict(open_kwargs or {})
    if not fallback_href:
        fallback_href, _ = _thread_target_hint_from_thread(thread)
    opened = False
    if callable(open_thread_fn):
        try:
            if open_kwargs_payload:
                opened = bool(open_thread_fn(thread, **open_kwargs_payload))
            else:
                opened = bool(open_thread_fn(thread))
        except TypeError:
            try:
                opened = bool(open_thread_fn(thread))
            except Exception:
                opened = False
        except Exception:
            opened = False
    if (not opened) and fallback_href and callable(open_by_href_fn):
        href_kwargs: Dict[str, object] = {}
        visual_timeout_value = open_kwargs_payload.get("visual_timeout_ms")
        if visual_timeout_value is not None:
            href_kwargs["visual_timeout_ms"] = visual_timeout_value
        try:
            if href_kwargs:
                opened = bool(open_by_href_fn(fallback_href, **href_kwargs))
            else:
                opened = bool(open_by_href_fn(fallback_href))
        except TypeError:
            try:
                opened = bool(open_by_href_fn(fallback_href))
            except Exception:
                opened = False
        except Exception:
            opened = False
    return opened


_PACK_ACTION_RETRY_ATTEMPTS = 3
_PACK_ACTION_RETRY_SLEEP_SECONDS = 1.0
_PACK_ACTION_PREFLIGHT_ATTEMPTS = 2
_PACK_ACTION_PREFLIGHT_ATTEMPT2_VISUAL_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_PREFLIGHT_ATTEMPT2_VISUAL_TIMEOUT_MS",
    9_000,
    minimum=2_000,
)
_PACK_ACTION_PREFLIGHT_ATTEMPT2_NETWORK_TIMEOUT_MS = _env_int(
    "AUTORESPONDER_PREFLIGHT_ATTEMPT2_NETWORK_TIMEOUT_MS",
    6_750,
    minimum=2_000,
)
_PACK_ACTION_MAX_TEMP_ATTEMPTS = _env_int(
    "AUTORESPONDER_PACK_ACTION_MAX_TEMP_ATTEMPTS",
    2,
    minimum=1,
)
_PENDING_PACK_BACKOFF_STEPS_SECONDS = (120.0, 600.0)
_PACK_SEND_ACK_TIMEOUT_SECONDS = 4.0
_PACK_SEND_CONFIRM_POLL_INTERVAL_SECONDS = 0.8
_PACK_SEND_CONFIRM_PRIMARY_ATTEMPTS = 6
_PACK_SEND_CONFIRM_REFRESH_ATTEMPTS = 3
_PACK_SEND_PENDING_RECHECK_SECONDS = 30.0


def _pending_pack_backoff_seconds(attempt_number: int) -> float:
    safe_attempt = max(1, int(attempt_number or 1))
    idx = min(safe_attempt - 1, len(_PENDING_PACK_BACKOFF_STEPS_SECONDS) - 1)
    return float(_PENDING_PACK_BACKOFF_STEPS_SECONDS[idx])


def _normalize_pending_action_attempts(raw_attempts: object) -> Dict[str, int]:
    if not isinstance(raw_attempts, dict):
        return {}
    normalized: Dict[str, int] = {}
    for key, value in raw_attempts.items():
        key_clean = str(key or "").strip()
        if not key_clean:
            continue
        try:
            count = max(0, int(value or 0))
        except Exception:
            count = 0
        normalized[key_clean] = count
    return normalized


def _pending_action_attempt_count(pending_run: Dict[str, object], action_index: int) -> int:
    attempts = _normalize_pending_action_attempts(pending_run.get("action_attempts"))
    return max(0, int(attempts.get(str(int(action_index)), 0) or 0))


def _pending_increment_action_attempt(pending_run: Dict[str, object], action_index: int) -> int:
    attempts = _normalize_pending_action_attempts(pending_run.get("action_attempts"))
    key = str(int(action_index))
    attempts[key] = max(0, int(attempts.get(key, 0) or 0)) + 1
    pending_run["action_attempts"] = attempts
    return attempts[key]


def _pending_clear_action_attempt(pending_run: Dict[str, object], action_index: int) -> None:
    attempts = _normalize_pending_action_attempts(pending_run.get("action_attempts"))
    key = str(int(action_index))
    if key in attempts:
        attempts.pop(key, None)
        pending_run["action_attempts"] = attempts


def _pending_is_manual_review(pending_run: Dict[str, object]) -> bool:
    status = str(pending_run.get("status") or "").strip().lower()
    return bool(pending_run.get("manual_review", False) or status in {"failed_permanent", "manual_review"})


def _pending_pack_error_non_recoverable(error_code: str) -> bool:
    code = str(error_code or "").strip().lower()
    if not code:
        return False
    hard_fail_tokens = (
        "pack_build_failed",
        "pack_action_unsupported",
        "pack_action_empty_content",
    )
    return any(token in code for token in hard_fail_tokens)


def _pack_error_is_confirmation_pending(error_code: str) -> bool:
    code = str(error_code or "").strip().lower()
    if not code:
        return False
    pending_prefixes = (
        "pending_backoff_waiting:",
        "outbox_started_waiting:",
        "pack_action_unconfirmed:",
        "pack_action_send_unverified:",
    )
    return any(code.startswith(prefix) for prefix in pending_prefixes)


def _preflight_pack_action(
    client: object,
    thread: object,
    *,
    action_type: str,
) -> tuple[bool, str]:
    normalized_type = str(action_type or "").strip().lower()
    needs_composer = normalized_type == "text_fixed"
    ensure_page_fn = getattr(client, "_ensure_page", None)
    find_composer_fn = getattr(client, "_find_composer", None)
    fallback_href, expected_thread_id = _thread_target_hint_from_thread(thread)
    last_reason = "preflight_unknown"
    if callable(ensure_page_fn):
        try:
            page = ensure_page_fn()
            current_url = str(getattr(page, "url", "") or "")
            current_thread_id = _extract_thread_id_from_href(current_url)
            on_expected_thread = bool(
                expected_thread_id
                and current_thread_id
                and current_thread_id == expected_thread_id
            )
            if on_expected_thread:
                if needs_composer and callable(find_composer_fn):
                    composer = find_composer_fn(page)
                    if composer is not None:
                        return True, "ok"
                else:
                    return True, "ok"
        except Exception:
            pass
    for attempt in range(1, _PACK_ACTION_PREFLIGHT_ATTEMPTS + 1):
        attempt_started = time.perf_counter()
        pre_url = ""
        was_in_thread = False
        if callable(ensure_page_fn):
            try:
                page_probe = ensure_page_fn()
                pre_url = str(getattr(page_probe, "url", "") or "")
                was_in_thread = "/direct/t/" in pre_url
            except Exception:
                pre_url = ""
                was_in_thread = False

        attempt_open_kwargs: Dict[str, object] = {}
        if attempt >= 2:
            attempt_open_kwargs = {
                "visual_timeout_ms": int(_PACK_ACTION_PREFLIGHT_ATTEMPT2_VISUAL_TIMEOUT_MS),
                "network_timeout_ms": int(_PACK_ACTION_PREFLIGHT_ATTEMPT2_NETWORK_TIMEOUT_MS),
                "force_workspace": True,
                "prefer_cache": True,
            }
            ensure_workspace_fn = getattr(client, "_ensure_inbox_workspace_fast", None)
            if callable(ensure_workspace_fn):
                try:
                    ensure_workspace_fn()
                except Exception:
                    pass
            open_inbox_fn = getattr(client, "_open_inbox", None)
            if callable(open_inbox_fn):
                try:
                    open_inbox_fn(force_reload=False)
                except TypeError:
                    try:
                        open_inbox_fn()
                    except Exception:
                        pass
                except Exception:
                    pass

        opened = _reopen_thread_best_effort(
            client,
            thread,
            preferred_href=fallback_href,
            open_kwargs=attempt_open_kwargs,
        )
        diag_getter = getattr(client, "_get_last_open_thread_diag", None)
        last_diag = diag_getter() if callable(diag_getter) else {}
        post_url = ""
        if isinstance(last_diag, dict):
            post_url = str(last_diag.get("post_url") or "").strip()
        if not post_url and callable(ensure_page_fn):
            try:
                page_probe = ensure_page_fn()
                post_url = str(getattr(page_probe, "url", "") or "")
            except Exception:
                post_url = ""
        visual_wait_ms = 0
        network_wait_ms = 0
        failed_condition = ""
        row_stale = False
        if isinstance(last_diag, dict):
            try:
                visual_wait_ms = int(last_diag.get("visual_wait_ms") or 0)
            except Exception:
                visual_wait_ms = 0
            try:
                network_wait_ms = int(last_diag.get("network_wait_ms") or 0)
            except Exception:
                network_wait_ms = 0
            failed_condition = str(last_diag.get("failed_condition") or "").strip().lower()
            row_stale = bool(last_diag.get("row_stale", False))
        if not opened:
            last_reason = "preflight_open_thread_failed"
            if not failed_condition:
                failed_condition = "open_thread"
        elif needs_composer and callable(ensure_page_fn) and callable(find_composer_fn):
            try:
                page = ensure_page_fn()
                composer = find_composer_fn(page)
                if composer is None:
                    last_reason = "preflight_composer_missing"
                    if not failed_condition:
                        failed_condition = "composer_missing"
                else:
                    logger.info(
                        "PREFLIGHT_OPEN attempt=%s/%s action=%s opened=%s pre_url=%s post_url=%s was_in_thread=%s visual_wait_ms=%s network_wait_ms=%s failed=%s row_stale=%s dur_ms=%s",
                        attempt,
                        _PACK_ACTION_PREFLIGHT_ATTEMPTS,
                        normalized_type or "-",
                        True,
                        pre_url or "-",
                        post_url or "-",
                        was_in_thread,
                        visual_wait_ms,
                        network_wait_ms,
                        failed_condition or "ok",
                        row_stale,
                        int((time.perf_counter() - attempt_started) * 1000.0),
                    )
                    logger.info(
                        "PREFLIGHT_OPEN_FINAL action=%s result=ok reason=ok attempts=%s",
                        normalized_type or "-",
                        attempt,
                    )
                    return True, "ok"
            except Exception:
                last_reason = "preflight_composer_check_failed"
                if not failed_condition:
                    failed_condition = "composer_check"
        else:
            logger.info(
                "PREFLIGHT_OPEN attempt=%s/%s action=%s opened=%s pre_url=%s post_url=%s was_in_thread=%s visual_wait_ms=%s network_wait_ms=%s failed=%s row_stale=%s dur_ms=%s",
                attempt,
                _PACK_ACTION_PREFLIGHT_ATTEMPTS,
                normalized_type or "-",
                opened,
                pre_url or "-",
                post_url or "-",
                was_in_thread,
                visual_wait_ms,
                network_wait_ms,
                failed_condition or "ok",
                row_stale,
                int((time.perf_counter() - attempt_started) * 1000.0),
            )
            logger.info(
                "PREFLIGHT_OPEN_FINAL action=%s result=ok reason=ok attempts=%s",
                normalized_type or "-",
                attempt,
            )
            return True, "ok"
        logger.info(
            "PREFLIGHT_OPEN attempt=%s/%s action=%s opened=%s pre_url=%s post_url=%s was_in_thread=%s visual_wait_ms=%s network_wait_ms=%s failed=%s row_stale=%s dur_ms=%s",
            attempt,
            _PACK_ACTION_PREFLIGHT_ATTEMPTS,
            normalized_type or "-",
            opened,
            pre_url or "-",
            post_url or "-",
            was_in_thread,
            visual_wait_ms,
            network_wait_ms,
            failed_condition or last_reason or "-",
            row_stale,
            int((time.perf_counter() - attempt_started) * 1000.0),
        )
        if attempt < _PACK_ACTION_PREFLIGHT_ATTEMPTS:
            sleep_with_stop(0.4)
    logger.warning(
        "PREFLIGHT_OPEN_FINAL action=%s result=fail reason=%s attempts=%s",
        normalized_type or "-",
        last_reason or "preflight_unknown",
        _PACK_ACTION_PREFLIGHT_ATTEMPTS,
    )
    return False, last_reason


def _normalize_message_match_text(value: str) -> str:
    cleaned = _sanitize_generated_message(str(value or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    return cleaned


def _message_text_equivalent(expected: str, candidate: str) -> bool:
    left = _normalize_message_match_text(expected)
    right = _normalize_message_match_text(candidate)
    if not left or not right:
        return False
    if left == right:
        return True
    return left in right or right in left


def _confirm_recent_outbound_message_id(
    client: object,
    thread: object,
    text: str,
    *,
    expected_message_id: str = "",
    baseline_ids: Optional[set[str]] = None,
    baseline_signatures: Optional[set[str]] = None,
) -> Optional[str]:
    target = _normalize_message_match_text(text)
    expected_id = str(expected_message_id or "").strip()
    baseline_ids_set = {str(value or "").strip() for value in (baseline_ids or set()) if str(value or "").strip()}
    baseline_signatures_set = {
        str(value or "").strip()
        for value in (baseline_signatures or set())
        if str(value or "").strip()
    }
    if not target and not expected_id:
        return None
    recent = _fetch_recent_messages(client, thread, amount=20)
    if not isinstance(recent, list):
        return None
    own_user_id = str(getattr(client, "user_id", "") or "").strip()
    for msg in recent:
        try:
            msg_text = str(getattr(msg, "text", "") or "")
            msg_user = str(getattr(msg, "user_id", "") or "").strip()
            if own_user_id and msg_user and msg_user != own_user_id:
                continue
            if not _message_text_equivalent(target, msg_text):
                continue
            msg_id = str(
                getattr(msg, "id", None)
                or getattr(msg, "message_id", None)
                or ""
            ).strip()
            if expected_id and msg_id and msg_id == expected_id:
                return msg_id
            if target and not _message_text_equivalent(target, msg_text):
                continue
            msg_signature = _outbound_message_signature(msg)
            if msg_id and msg_id in baseline_ids_set:
                continue
            if msg_signature and msg_signature in baseline_signatures_set:
                continue
            if msg_id:
                return msg_id
            return f"fallback-text-{int(time.time() * 1000)}"
        except Exception:
            continue
    return None


def _collect_outbound_baseline_markers(
    client: object,
    thread: object,
    *,
    amount: int = 30,
) -> tuple[set[str], set[str]]:
    recent = _fetch_recent_messages(client, thread, amount=amount)
    if not isinstance(recent, list):
        return set(), set()
    own_user_id = str(getattr(client, "user_id", "") or "").strip()
    ids: set[str] = set()
    signatures: set[str] = set()
    for msg in recent:
        try:
            if _message_outbound_status(msg, own_user_id) is not True:
                continue
            msg_id = _message_id_for_compare(msg)
            if msg_id:
                ids.add(msg_id)
            signature = _outbound_message_signature(msg)
            if signature:
                signatures.add(signature)
        except Exception:
            continue
    return ids, signatures


def _confirm_outbound_delivery_strict(
    client: object,
    thread: object,
    *,
    content: str,
    expected_message_id: str = "",
    baseline_ids: Optional[set[str]] = None,
    baseline_signatures: Optional[set[str]] = None,
) -> Optional[str]:
    for attempt in range(1, _PACK_ACTION_RETRY_ATTEMPTS + 1):
        confirmed_id = _confirm_recent_outbound_message_id(
            client,
            thread,
            content,
            expected_message_id=expected_message_id,
            baseline_ids=baseline_ids,
            baseline_signatures=baseline_signatures,
        )
        if confirmed_id:
            return confirmed_id
        if attempt < _PACK_ACTION_RETRY_ATTEMPTS:
            try:
                _reopen_thread_best_effort(client, thread)
            except Exception:
                pass
            sleep_with_stop(_PACK_ACTION_RETRY_SLEEP_SECONDS)
    return None


def _fetch_recent_messages(
    client: object,
    thread: object,
    *,
    amount: int = 20,
) -> Optional[List[object]]:
    get_messages_fn = getattr(client, "get_messages", None)
    if not callable(get_messages_fn):
        return None
    for kwargs in (
        {"amount": amount, "log": False},
        {"amount": amount},
        {},
    ):
        try:
            recent = get_messages_fn(thread, **kwargs)
        except TypeError:
            continue
        except Exception:
            return None
        if isinstance(recent, list):
            return recent
    return None


def _outbound_message_signature(msg: object) -> str:
    msg_id = _message_id_for_compare(msg)
    if msg_id:
        return f"id:{msg_id}"
    ts_value = _message_timestamp(msg)
    text_value = _normalize_message_match_text(str(getattr(msg, "text", "") or ""))
    if ts_value is not None:
        return f"ts:{round(ts_value, 3)}|txt:{text_value[:120]}"
    if text_value:
        return f"txt:{text_value[:120]}"
    return ""


def _send_text_action_strict(
    client: object,
    thread: object,
    content: str,
) -> Dict[str, object]:
    thread_id = str(getattr(thread, "id", "") or "").strip()
    if not thread_id:
        thread_id = _extract_thread_id_from_href(getattr(thread, "link", ""))
    if not thread_id:
        logger.warning("SEND_STRICT_FAIL reason=missing_thread_id")
        return {"status": "failed", "item_id": "", "reason": "missing_thread_id"}
    ensure_ready_fn = getattr(client, "ensure_thread_ready_strict", None)
    if callable(ensure_ready_fn):
        ready_ok = False
        ready_reason = ""
        try:
            ready_result = ensure_ready_fn(thread_id)
            if isinstance(ready_result, tuple):
                ready_ok = bool(ready_result[0]) if ready_result else False
                ready_reason = str(ready_result[1] if len(ready_result) > 1 else "")
            else:
                ready_ok = bool(ready_result)
                ready_reason = ""
        except Exception as exc:
            ready_ok = False
            ready_reason = str(exc)
        if not ready_ok:
            logger.warning(
                "SEND_STRICT_FAIL thread=%s reason=thread_not_ready:%s",
                thread_id,
                ready_reason or "unknown",
            )
            return {
                "status": "failed",
                "item_id": "",
                "reason": f"thread_not_ready:{ready_reason or 'unknown'}",
            }
    send_ack_fn = getattr(client, "send_text_with_ack", None)
    baseline_fn = getattr(client, "get_outbound_baseline", None)
    confirm_outbound_fn = getattr(client, "confirm_new_outbound_after_baseline", None)
    refresh_thread_fn = getattr(client, "refresh_thread_for_confirmation", None)
    if not callable(send_ack_fn) or not callable(confirm_outbound_fn):
        logger.warning(
            "SEND_STRICT_FAIL thread=%s reason=client_without_ack_or_confirm",
            thread_id,
        )
        return {
            "status": "failed",
            "item_id": "",
            "reason": "client_without_ack_or_confirm",
        }

    baseline_item_id = ""
    baseline_timestamp: Optional[float] = None
    if callable(baseline_fn):
        try:
            raw_baseline = baseline_fn(thread_id, expected_text=content)
            baseline_result = dict(raw_baseline) if isinstance(raw_baseline, dict) else {}
        except Exception as exc:
            baseline_result = {"ok": False, "reason": f"baseline_exception:{exc}"}
        baseline_item_id = str(baseline_result.get("item_id") or "").strip()
        baseline_timestamp = _safe_float(baseline_result.get("timestamp"))

    send_started_at = time.time()
    ack_result: Dict[str, object] = {}
    try:
        raw_ack = send_ack_fn(thread_id, content, timeout=_PACK_SEND_ACK_TIMEOUT_SECONDS)
        ack_result = dict(raw_ack) if isinstance(raw_ack, dict) else {}
    except Exception as exc:
        ack_result = {
            "ok": False,
            "item_id": None,
            "reason": f"ack_exception:{exc}",
        }
    ack_ok = bool(ack_result.get("ok", False))
    ack_reason = str(ack_result.get("reason") or "").strip()
    ack_item_id = str(ack_result.get("item_id") or "").strip()
    if ack_ok and ack_item_id:
        logger.info(
            "SEND_STRICT_VERIFIED thread=%s item_id=%s reason=ack_ok",
            thread_id,
            ack_item_id,
        )
        return {
            "status": "confirmed",
            "item_id": ack_item_id,
            "reason": "ack_ok",
        }

    confirm_result: Dict[str, object] = {}
    try:
        raw_confirm = confirm_outbound_fn(
            thread_id,
            baseline_item_id=baseline_item_id,
            baseline_timestamp=baseline_timestamp,
            sent_after_ts=send_started_at,
            expected_text=content,
            attempts=_PACK_SEND_CONFIRM_PRIMARY_ATTEMPTS,
            poll_interval_seconds=_PACK_SEND_CONFIRM_POLL_INTERVAL_SECONDS,
            allow_dom=True,
        )
        confirm_result = dict(raw_confirm) if isinstance(raw_confirm, dict) else {}
    except Exception as exc:
        confirm_result = {
            "ok": False,
            "item_id": None,
            "reason": f"confirm_poll_exception:{exc}",
        }

    confirm_ok = bool(confirm_result.get("ok", False))
    confirm_item_id = str(confirm_result.get("item_id") or "").strip()
    confirm_reason = str(confirm_result.get("reason") or "").strip()
    if confirm_ok:
        if not confirm_item_id:
            confirm_item_id = f"confirmed-{int(time.time() * 1000)}"
        logger.info(
            "SEND_STRICT_VERIFIED thread=%s item_id=%s reason=%s",
            thread_id,
            confirm_item_id,
            confirm_reason or "endpoint_or_dom_confirmed",
        )
        return {
            "status": "confirmed",
            "item_id": confirm_item_id,
            "reason": confirm_reason or "endpoint_or_dom_confirmed",
        }

    refreshed = False
    if callable(refresh_thread_fn):
        try:
            refreshed = bool(refresh_thread_fn(thread_id))
        except Exception:
            refreshed = False
    if refreshed:
        try:
            raw_confirm_after_refresh = confirm_outbound_fn(
                thread_id,
                baseline_item_id=baseline_item_id,
                baseline_timestamp=baseline_timestamp,
                sent_after_ts=send_started_at,
                expected_text=content,
                attempts=_PACK_SEND_CONFIRM_REFRESH_ATTEMPTS,
                poll_interval_seconds=_PACK_SEND_CONFIRM_POLL_INTERVAL_SECONDS,
                allow_dom=True,
            )
            confirm_after_refresh = (
                dict(raw_confirm_after_refresh)
                if isinstance(raw_confirm_after_refresh, dict)
                else {}
            )
        except Exception as exc:
            confirm_after_refresh = {
                "ok": False,
                "item_id": None,
                "reason": f"confirm_after_refresh_exception:{exc}",
            }
        refresh_ok = bool(confirm_after_refresh.get("ok", False))
        refresh_item_id = str(confirm_after_refresh.get("item_id") or "").strip()
        refresh_reason = str(confirm_after_refresh.get("reason") or "").strip()
        if refresh_ok:
            if not refresh_item_id:
                refresh_item_id = f"confirmed-{int(time.time() * 1000)}"
            logger.info(
                "SEND_STRICT_VERIFIED thread=%s item_id=%s reason=%s",
                thread_id,
                refresh_item_id,
                refresh_reason or "confirmed_after_refresh",
            )
            return {
                "status": "confirmed",
                "item_id": refresh_item_id,
                "reason": refresh_reason or "confirmed_after_refresh",
            }
        confirm_reason = refresh_reason or confirm_reason

    unconfirmed_reason_parts = [
        f"ack={ack_reason or 'not_ok'}",
        f"confirm={confirm_reason or 'not_confirmed'}",
    ]
    unconfirmed_reason = ",".join(unconfirmed_reason_parts)
    logger.warning(
        "SEND_STRICT_UNCONFIRMED thread=%s reason=%s content=%s",
        thread_id,
        unconfirmed_reason,
        _normalize_message_match_text(content)[:120],
    )
    return {
        "status": "unconfirmed",
        "item_id": "",
        "reason": unconfirmed_reason,
    }


def _normalize_strict_send_result(raw_result: object) -> Dict[str, str]:
    if isinstance(raw_result, dict):
        payload = dict(raw_result)
    else:
        legacy_item_id = str(raw_result or "").strip()
        if not legacy_item_id:
            return {
                "status": "failed",
                "item_id": "",
                "reason": "empty_send_result",
            }
        return {
            "status": "confirmed",
            "item_id": legacy_item_id,
            "reason": "legacy_message_id",
        }

    status = str(payload.get("status") or "").strip().lower()
    item_id = str(payload.get("item_id") or payload.get("message_id") or "").strip()
    reason = str(payload.get("reason") or "").strip()

    if not status:
        status = "confirmed" if item_id else "failed"
    if status == "confirmed" and not item_id:
        status = "failed"
        if not reason:
            reason = "confirmed_without_item_id"
    elif status not in {"confirmed", "unconfirmed", "failed"}:
        status = "confirmed" if item_id else "failed"

    return {
        "status": status,
        "item_id": item_id,
        "reason": reason,
    }


def _emit_followup_scheduled_event(
    *,
    account: str,
    thread_id: str,
    recipient_username: str,
    flow_config: Dict[str, object],
    flow_state: Dict[str, object],
) -> None:
    stage_id = str(flow_state.get("stage_id") or flow_config.get("entry_stage_id") or "").strip()
    if not stage_id:
        return
    stages_raw = flow_config.get("stages")
    stages = list(stages_raw) if isinstance(stages_raw, list) else []
    stage = next(
        (
            item
            for item in stages
            if isinstance(item, dict) and str(item.get("id") or "").strip() == stage_id
        ),
        None,
    )
    if not isinstance(stage, dict):
        return
    followups = [
        item
        for item in list(stage.get("followups") or [])
        if isinstance(item, dict)
    ]
    if not followups:
        return
    next_level = max(0, _safe_int(flow_state.get("followup_level")))
    if next_level >= len(followups):
        return
    next_followup = followups[next_level]
    delay_hours = _safe_float(next_followup.get("delay_hours"))
    _emit_autoresponder_event(
        "FOLLOWUP_SCHEDULED",
        account=str(account or "").strip(),
        thread_id=str(thread_id or "").strip(),
        recipient=str(recipient_username or "").strip(),
        stage_id=stage_id,
        followup_level=next_level + 1,
        delay_hours=delay_hours if delay_hours is not None else None,
    )


def _outbox_anchor_token(anchor_ts: Optional[float]) -> str:
    value = _safe_float(anchor_ts)
    if value is None or value <= 0:
        return "0"
    return f"{value:.3f}"


def _build_outbox_action_key(
    thread_id: str,
    pack_id: str,
    action_index: int,
    stage_id: str,
    anchor_ts: Optional[float],
) -> str:
    return f"{str(thread_id or '').strip()}:{str(pack_id or '').strip()}:{int(action_index)}:{str(stage_id or '').strip()}:{_outbox_anchor_token(anchor_ts)}"


def _extract_flow_outbox_entry(flow_state: Dict[str, object], action_key: str) -> Dict[str, object]:
    outbox_raw = flow_state.get("outbox")
    if not isinstance(outbox_raw, dict):
        return {}
    entry = outbox_raw.get(action_key)
    if isinstance(entry, dict):
        return dict(entry)
    return {}


def _dom_fingerprint_outbound(client: object, thread: object) -> str:
    recent = _fetch_recent_messages(client, thread, amount=30)
    if not isinstance(recent, list):
        return ""
    own_user_id = str(getattr(client, "user_id", "") or "").strip()
    outbound_items = [msg for msg in recent if _message_outbound_status(msg, own_user_id) is True]
    outbound_count = len(outbound_items)
    last_selector_text = ""
    if outbound_items:
        last_msg = outbound_items[0]
        msg_text = str(getattr(last_msg, "text", "") or "").strip()
        last_selector_text = _normalize_message_match_text(msg_text)[:80]
    return f"{outbound_count}|{last_selector_text}"


def _persist_outbox_entry_status(
    account_id: str,
    thread_id: str,
    recipient_username: str,
    *,
    action_key: str,
    status: str,
    fallback_stage_id: str,
    started_at: Optional[float] = None,
    sent_at: Optional[float] = None,
    dom_fingerprint: str = "",
    message_id: str = "",
    baseline_ids: Optional[set[str]] = None,
    baseline_signatures: Optional[set[str]] = None,
) -> Dict[str, object]:
    conv_state = _get_conversation_state(account_id, thread_id)
    flow_state = _normalize_flow_state(
        conv_state.get("flow_state"),
        fallback_stage_id=fallback_stage_id,
        last_outbound_ts=_safe_float(conv_state.get("last_message_sent_at")),
        followup_level_hint=_safe_int(conv_state.get("followup_stage")),
    )
    outbox_raw = flow_state.get("outbox")
    outbox: Dict[str, Dict[str, object]] = dict(outbox_raw) if isinstance(outbox_raw, dict) else {}
    current_entry_raw = outbox.get(action_key)
    current_entry: Dict[str, object] = dict(current_entry_raw) if isinstance(current_entry_raw, dict) else {}
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"started", "sent"}:
        return flow_state
    current_entry["status"] = normalized_status
    if normalized_status == "started":
        started_ts = _safe_float(started_at) or time.time()
        current_entry["started_at"] = started_ts
        current_entry.pop("sent_at", None)
        current_entry.pop("dom_fingerprint", None)
        current_entry.pop("message_id", None)
        ids = set(baseline_ids or set())
        signatures = set(baseline_signatures or set())
        if ids:
            current_entry["baseline_ids"] = sorted(ids)[:120]
        elif "baseline_ids" not in current_entry:
            current_entry["baseline_ids"] = []
        if signatures:
            current_entry["baseline_signatures"] = sorted(signatures)[:120]
        elif "baseline_signatures" not in current_entry:
            current_entry["baseline_signatures"] = []
    else:
        sent_ts = _safe_float(sent_at) or time.time()
        current_entry["sent_at"] = sent_ts
        if not _safe_float(current_entry.get("started_at")):
            current_entry["started_at"] = sent_ts
        if dom_fingerprint:
            current_entry["dom_fingerprint"] = str(dom_fingerprint).strip()
        message_id_clean = str(message_id or "").strip()
        if message_id_clean:
            current_entry["message_id"] = message_id_clean
    outbox[action_key] = current_entry
    flow_state["outbox"] = outbox
    _update_conversation_state(
        account_id,
        thread_id,
        {
            "flow_state": flow_state,
            "followup_stage": _safe_int(flow_state.get("followup_level")),
        },
        recipient_username=recipient_username,
    )
    return flow_state


def _normalize_pending_pack_run(raw_pending: object) -> Optional[Dict[str, object]]:
    if not isinstance(raw_pending, dict):
        return None
    actions_raw = raw_pending.get("actions")
    if not isinstance(actions_raw, list) or not actions_raw:
        return None
    normalized_actions: List[Dict[str, object]] = []
    for raw_action in actions_raw:
        normalized = _normalize_pack_action(raw_action)
        if normalized is None:
            continue
        normalized_actions.append(dict(normalized))
    if not normalized_actions:
        return None
    try:
        delay_min = max(
            0,
            int(raw_pending.get("pack_delay_min", raw_pending.get("delay_min", 0)) or 0),
        )
    except Exception:
        delay_min = 0
    try:
        delay_max = max(
            delay_min,
            int(raw_pending.get("pack_delay_max", raw_pending.get("delay_max", delay_min)) or delay_min),
        )
    except Exception:
        delay_max = delay_min
    try:
        current_index = int(raw_pending.get("current_index", 0) or 0)
    except Exception:
        current_index = 0
    current_index = max(0, min(current_index, len(normalized_actions)))
    try:
        retry_count = int(raw_pending.get("retry_count", 0) or 0)
    except Exception:
        retry_count = 0
    try:
        created_at = float(raw_pending.get("created_at") or time.time())
    except Exception:
        created_at = time.time()
    try:
        updated_at = float(raw_pending.get("updated_at") or time.time())
    except Exception:
        updated_at = time.time()
    stage_id = str(raw_pending.get("stage_id") or "").strip()
    stage_anchor_ts = _safe_float(raw_pending.get("stage_anchor_ts"))
    next_attempt_at = _safe_float(raw_pending.get("next_attempt_at"))
    action_attempts = _normalize_pending_action_attempts(raw_pending.get("action_attempts"))
    status = str(raw_pending.get("status") or "active").strip().lower() or "active"
    if status not in {"active", "failed_permanent"}:
        status = "active"
    normalized: Dict[str, object] = {
        "version": 1,
        "pack_id": str(raw_pending.get("pack_id") or "").strip(),
        "pack_name": str(raw_pending.get("pack_name") or "").strip(),
        "strategy_name": str(raw_pending.get("strategy_name") or "").strip(),
        "pack_delay_min": delay_min,
        "pack_delay_max": delay_max,
        "actions": normalized_actions,
        "current_index": current_index,
        "latest_inbound_id": str(raw_pending.get("latest_inbound_id") or "").strip(),
        "is_followup": bool(raw_pending.get("is_followup", False)),
        "followup_stage": raw_pending.get("followup_stage"),
        "retry_count": retry_count,
        "last_error": str(raw_pending.get("last_error") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "stage_id": stage_id,
        "stage_anchor_ts": stage_anchor_ts,
        "next_attempt_at": next_attempt_at,
        "action_attempts": action_attempts,
        "status": status,
        "manual_review": bool(raw_pending.get("manual_review", False) or status == "failed_permanent"),
    }
    return normalized


def _build_pending_pack_run(
    pack: Dict[str, object],
    *,
    account_id: str,
    latest_inbound_id: Optional[str],
    is_followup: bool,
    followup_stage: Optional[int],
    api_key: str,
    conversation_text: str,
    strategy_name: str,
    memory: Dict[str, object],
    stage_id: str,
    stage_anchor_ts: Optional[float],
) -> Optional[Dict[str, object]]:
    actions_raw = pack.get("actions")
    if not isinstance(actions_raw, list) or not actions_raw:
        return None
    resolved_actions: List[Dict[str, object]] = []
    for index, raw_action in enumerate(actions_raw):
        action = _normalize_pack_action(raw_action)
        if not action:
            logger.error(
                "Pack invalido (accion no soportada) | pack_id=%s idx=%s",
                pack.get("id"),
                index,
            )
            return None
        action_type = str(action.get("type") or "").strip().lower()
        if action_type == "text_adaptive":
            instruction = str(action.get("instruction") or "").strip()
            adaptive_text = _generate_pack_adaptive_text(
                api_key,
                instruction,
                conversation_text=conversation_text,
                strategy_name=strategy_name,
                memory=memory,
            )
            if not adaptive_text:
                logger.error(
                    "No se pudo generar text_adaptive para pack | pack_id=%s idx=%s",
                    pack.get("id"),
                    index,
                )
                return None
            resolved_actions.append(
                {
                    "type": "text_fixed",
                    "content": adaptive_text,
                    "source": "text_adaptive",
                }
            )
            continue
        resolved_actions.append(dict(action))
    if not resolved_actions:
        return None
    try:
        delay_min = max(0, int(pack.get("delay_min", 0) or 0))
    except Exception:
        delay_min = 0
    try:
        delay_max = max(delay_min, int(pack.get("delay_max", delay_min) or delay_min))
    except Exception:
        delay_max = delay_min
    now_ts = time.time()
    return {
        "version": 1,
        "pack_id": str(pack.get("id") or "").strip(),
        "pack_name": str(pack.get("name") or "").strip(),
        "strategy_name": str(strategy_name or "").strip(),
        # Delay del pack: solo entre acciones internas del mismo pack.
        "pack_delay_min": delay_min,
        "pack_delay_max": delay_max,
        "actions": resolved_actions,
        "current_index": 0,
        "latest_inbound_id": str(latest_inbound_id or "").strip(),
        "is_followup": bool(is_followup),
        "followup_stage": followup_stage if is_followup else None,
        "retry_count": 0,
        "last_error": "",
        "created_at": now_ts,
        "updated_at": now_ts,
        "account_id": _normalize_username(account_id),
        "stage_id": str(stage_id or "").strip(),
        "stage_anchor_ts": _safe_float(stage_anchor_ts),
        "next_attempt_at": None,
        "action_attempts": {},
        "status": "active",
        "manual_review": False,
    }


def _persist_pending_pack_run_state(
    account_id: str,
    thread_id: str,
    recipient_username: str,
    pending_run: Optional[Dict[str, object]],
    *,
    persist_pending: bool,
) -> None:
    if not persist_pending:
        return
    updates: Dict[str, object] = {"pending_pack_run": pending_run}
    if pending_run:
        updates["pending_manual_review"] = _pending_is_manual_review(pending_run)
        pending_inbound_id = str(pending_run.get("latest_inbound_id") or "").strip()
        if pending_inbound_id:
            updates["pending_inbound_id"] = pending_inbound_id
        if not bool(pending_run.get("is_followup", False)):
            updates["pending_reply"] = True
    else:
        updates["pending_manual_review"] = False
    _update_conversation_state(
        account_id,
        thread_id,
        updates,
        recipient_username=recipient_username,
    )


def execute_pack(
    pack: Dict[str, object],
    account_id: str,
    memory: Dict[str, object],
    *,
    client: object,
    thread: object,
    thread_id: str,
    recipient_username: str,
    api_key: str,
    conversation_text: str,
    strategy_name: str,
    latest_inbound_id: Optional[str] = None,
    is_followup: bool = False,
    followup_stage: Optional[int] = None,
    stage_id: str = "",
    stage_anchor_ts: Optional[float] = None,
    pending_pack_run: Optional[Dict[str, object]] = None,
    persist_pending: bool = False,
    flow_config: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    mutable_memory = dict(memory or {})
    resolved_flow_config = (
        _normalize_flow_config(flow_config)
        if isinstance(flow_config, dict)
        else _flow_config_for_account(account_id)
    )
    normalized_pending = _normalize_pending_pack_run(pending_pack_run)
    if normalized_pending is None:
        normalized_pending = _build_pending_pack_run(
            pack,
            account_id=account_id,
            latest_inbound_id=latest_inbound_id,
            is_followup=is_followup,
            followup_stage=followup_stage,
            api_key=api_key,
            conversation_text=conversation_text,
            strategy_name=strategy_name,
            memory=mutable_memory,
            stage_id=stage_id,
            stage_anchor_ts=stage_anchor_ts,
        )
        if normalized_pending is None:
            _safezone_register_failure(
                account_id,
                "pack_build_failed",
                severe=False,
            )
            return {
                "completed": False,
                "sent_count": 0,
                "current_index": 0,
                "total_actions": 0,
                "error": "pack_build_failed",
                "pending_pack_run": None,
                "pending_cleared": False,
            }

    pending_run = dict(normalized_pending)
    conv_state_for_outbox = _get_conversation_state(account_id, thread_id)
    flow_state_for_outbox = _normalize_flow_state(
        conv_state_for_outbox.get("flow_state"),
        fallback_stage_id=(
            str(stage_id or pending_run.get("stage_id") or "").strip()
            or str(resolved_flow_config.get("entry_stage_id") or "")
        ),
        last_outbound_ts=_safe_float(conv_state_for_outbox.get("last_message_sent_at")),
        followup_level_hint=_safe_int(conv_state_for_outbox.get("followup_stage")),
    )
    resolved_stage_id = str(
        pending_run.get("stage_id")
        or stage_id
        or flow_state_for_outbox.get("stage_id")
        or ""
    ).strip()
    resolved_anchor_ts = (
        _safe_float(pending_run.get("stage_anchor_ts"))
        or _safe_float(stage_anchor_ts)
        or _safe_float(flow_state_for_outbox.get("followup_anchor_ts"))
        or _safe_float(flow_state_for_outbox.get("last_outbound_ts"))
        or _safe_float(conv_state_for_outbox.get("last_message_received_at"))
        or time.time()
    )
    pending_run["stage_id"] = resolved_stage_id
    pending_run["stage_anchor_ts"] = resolved_anchor_ts
    pending_status = str(pending_run.get("status") or "active").strip().lower() or "active"
    pending_run["status"] = pending_status
    if _pending_is_manual_review(pending_run):
        # Reactivar automaticamente pendings heredados de manual_review.
        pending_run["status"] = "active"
        pending_run["manual_review"] = False
        pending_run["next_attempt_at"] = None
        pending_run["updated_at"] = time.time()
        logger.info(
            "PENDING_REACTIVATED account=@%s thread=%s recipient=@%s",
            account_id,
            thread_id,
            recipient_username,
        )
        _persist_pending_pack_run_state(
            account_id,
            thread_id,
            recipient_username,
            pending_run,
            persist_pending=persist_pending,
        )
    next_attempt_at = _safe_float(pending_run.get("next_attempt_at"))
    now_ts = time.time()
    if next_attempt_at is not None and next_attempt_at > now_ts:
        pending_run["updated_at"] = now_ts
        _persist_pending_pack_run_state(
            account_id,
            thread_id,
            recipient_username,
            pending_run,
            persist_pending=persist_pending,
        )
        return {
            "completed": False,
            "sent_count": 0,
            "current_index": max(0, int(pending_run.get("current_index", 0) or 0)),
            "total_actions": len(list(pending_run.get("actions") or [])),
            "error": f"pending_backoff_waiting:{round(max(0.0, next_attempt_at - now_ts), 1)}s",
            "pending_pack_run": pending_run,
            "pending_cleared": False,
        }
    actions = list(pending_run.get("actions") or [])
    total_actions = len(actions)
    current_index = max(0, min(int(pending_run.get("current_index", 0) or 0), total_actions))
    sent_count = 0
    verified_send_actions = 0
    run_sendable_expected = sum(
        1
        for action in actions[current_index:]
        if str((action or {}).get("type") or "").strip().lower() == "text_fixed"
    )
    pending_run["updated_at"] = time.time()
    pending_run["next_attempt_at"] = None
    _persist_pending_pack_run_state(
        account_id,
        thread_id,
        recipient_username,
        pending_run,
        persist_pending=persist_pending,
    )

    for index in range(current_index, total_actions):
        if STOP_EVENT.is_set():
            pending_run["current_index"] = index
            pending_run["updated_at"] = time.time()
            pending_run["retry_count"] = int(pending_run.get("retry_count", 0) or 0) + 1
            pending_run["last_error"] = "stop_requested"
            _persist_pending_pack_run_state(
                account_id,
                thread_id,
                recipient_username,
                pending_run,
                persist_pending=persist_pending,
            )
            _set_account_memory(account_id, mutable_memory)
            memory.clear()
            memory.update(mutable_memory)
            return {
                "completed": False,
                "sent_count": sent_count,
                "current_index": index,
                "total_actions": total_actions,
                "error": "stop_requested",
                "pending_pack_run": pending_run,
                "pending_cleared": False,
            }

        action = actions[index]
        action_type = str(action.get("type") or "").strip().lower()
        action_error = ""
        content = ""
        action_key = ""
        if action_type == "text_fixed":
            preflight_ok, preflight_reason = _preflight_pack_action(
                client,
                thread,
                action_type=action_type,
            )
            if not preflight_ok:
                action_error = f"{preflight_reason}:{index}"

        if not action_error:
            if action_type == "text_fixed":
                content = _sanitize_generated_message(str(action.get("content") or "").strip())
                if not content:
                    action_error = f"pack_action_empty_content:{index}"
                else:
                    pack_id_value = str(pending_run.get("pack_id") or "").strip()
                    action_key = _build_outbox_action_key(
                        thread_id,
                        pack_id_value,
                        index,
                        resolved_stage_id,
                        _safe_float(resolved_anchor_ts),
                    )
                    outbox_entry = _extract_flow_outbox_entry(flow_state_for_outbox, action_key)
                    outbox_status = str(outbox_entry.get("status") or "").strip().lower()
                    outbox_started_at = _safe_float(outbox_entry.get("started_at")) or 0.0
                    if outbox_status == "sent":
                        logger.info("OUTBOX skip key=%s reason=sent", action_key)
                        verified_send_actions += 1
                    else:
                        confirmed_existing_id = None
                        if outbox_status == "started":
                            outbox_baseline_ids = {
                                str(value or "").strip()
                                for value in (outbox_entry.get("baseline_ids") or [])
                                if str(value or "").strip()
                            }
                            outbox_baseline_signatures = {
                                str(value or "").strip()
                                for value in (outbox_entry.get("baseline_signatures") or [])
                                if str(value or "").strip()
                            }
                            confirmed_existing_id = _confirm_recent_outbound_message_id(
                                client,
                                thread,
                                content,
                                baseline_ids=outbox_baseline_ids,
                                baseline_signatures=outbox_baseline_signatures,
                            )
                            if confirmed_existing_id:
                                flow_state_for_outbox = _persist_outbox_entry_status(
                                    account_id,
                                    thread_id,
                                    recipient_username,
                                    action_key=action_key,
                                    status="sent",
                                    fallback_stage_id=resolved_stage_id,
                                    sent_at=time.time(),
                                    dom_fingerprint=_dom_fingerprint_outbound(client, thread),
                                    message_id=str(confirmed_existing_id),
                                )
                                logger.info("OUTBOX sent key=%s", action_key)
                                verified_send_actions += 1
                            else:
                                logger.info(
                                    "OUTBOX skip key=%s reason=started_pending_confirmation started_at=%s",
                                    action_key,
                                    outbox_started_at or 0.0,
                                )
                                action_error = f"outbox_started_waiting:{index}"
                        if not action_error and not confirmed_existing_id:
                            started_now = time.time()
                            baseline_ids, baseline_signatures = _collect_outbound_baseline_markers(
                                client,
                                thread,
                                amount=30,
                            )
                            flow_state_for_outbox = _persist_outbox_entry_status(
                                account_id,
                                thread_id,
                                recipient_username,
                                action_key=action_key,
                                status="started",
                                fallback_stage_id=resolved_stage_id,
                                started_at=started_now,
                                baseline_ids=baseline_ids,
                                baseline_signatures=baseline_signatures,
                            )
                            logger.info("OUTBOX started key=%s", action_key)
                            send_result = _normalize_strict_send_result(
                                _send_text_action_strict(client, thread, content)
                            )
                            send_status = str(send_result.get("status") or "").strip().lower()
                            send_reason = str(send_result.get("reason") or "").strip()
                            message_id = str(send_result.get("item_id") or "").strip()
                            if send_status != "confirmed" or not message_id:
                                if send_status == "unconfirmed":
                                    action_error = f"pack_action_unconfirmed:{index}:{send_reason or 'not_confirmed'}"
                                else:
                                    action_error = f"pack_action_send_unverified:{index}"
                            else:
                                flow_state_for_outbox = _persist_outbox_entry_status(
                                    account_id,
                                    thread_id,
                                    recipient_username,
                                    action_key=action_key,
                                    status="sent",
                                    fallback_stage_id=resolved_stage_id,
                                    sent_at=time.time(),
                                    dom_fingerprint=_dom_fingerprint_outbound(client, thread),
                                    message_id=str(message_id),
                                )
                                logger.info("OUTBOX sent key=%s", action_key)
                                _record_message_sent(
                                    account_id,
                                    thread_id,
                                    content,
                                    str(message_id),
                                    recipient_username,
                                    is_followup=is_followup,
                                    followup_stage=followup_stage,
                                )
                                sent_count += 1
                                verified_send_actions += 1
            elif action_type == "set_memory":
                key_name = str(action.get("key") or "").strip()
                if key_name:
                    mutable_memory[key_name] = action.get("value")
            else:
                action_error = f"pack_action_unsupported:{index}"

        if action_error:
            outbox_waiting = (
                action_error.startswith("outbox_started_waiting:")
                or action_error.startswith("pack_action_unconfirmed:")
            )
            if outbox_waiting:
                logger.info(
                    "Pack accion en espera outbox account=@%s thread=%s strategy=%s pack_id=%s idx=%s action=%s error=%s",
                    account_id,
                    thread_id,
                    strategy_name,
                    pending_run.get("pack_id"),
                    index,
                    action_type,
                    action_error,
                )
            else:
                logger.warning(
                    "Pack accion fallida account=@%s thread=%s strategy=%s pack_id=%s idx=%s action=%s error=%s",
                    account_id,
                    thread_id,
                    strategy_name,
                    pending_run.get("pack_id"),
                    index,
                    action_type,
                    action_error,
                )
            pending_run["current_index"] = index
            pending_run["updated_at"] = time.time()
            pending_run["last_error"] = action_error
            pending_run["status"] = "active"
            pending_run["manual_review"] = False
            if outbox_waiting:
                pending_run["next_attempt_at"] = time.time() + _PACK_SEND_PENDING_RECHECK_SECONDS
                clear_pending_now = False
            else:
                pending_run["next_attempt_at"] = None
                pending_run["retry_count"] = int(pending_run.get("retry_count", 0) or 0) + 1
                attempt_now = _pending_increment_action_attempt(pending_run, index)
                is_non_recoverable = _pending_pack_error_non_recoverable(action_error)
                backoff_seconds = _pending_pack_backoff_seconds(attempt_now)
                pending_run["next_attempt_at"] = time.time() + backoff_seconds
                clear_pending_now = False
                logger.info(
                    "PACK_ACTION_BACKOFF account=@%s thread=%s idx=%s attempt=%s next_in=%ss error=%s non_recoverable=%s temp_exhausted=%s",
                    account_id,
                    thread_id,
                    index,
                    attempt_now,
                    round(backoff_seconds, 1),
                    action_error,
                    is_non_recoverable,
                    attempt_now >= int(_PACK_ACTION_MAX_TEMP_ATTEMPTS),
                )
            _persist_pending_pack_run_state(
                account_id,
                thread_id,
                recipient_username,
                None if clear_pending_now else pending_run,
                persist_pending=persist_pending,
            )
            if not outbox_waiting:
                _safezone_register_failure(
                    account_id,
                    action_error,
                    severe=_safezone_reason_is_severe(action_error),
                )
            _set_account_memory(account_id, mutable_memory)
            memory.clear()
            memory.update(mutable_memory)
            return {
                "completed": False,
                "sent_count": sent_count,
                "current_index": index,
                "total_actions": total_actions,
                "error": action_error,
                "pending_pack_run": None if clear_pending_now else pending_run,
                "pending_cleared": clear_pending_now,
            }

        next_index = index + 1
        pending_run["current_index"] = next_index
        pending_run["updated_at"] = time.time()
        pending_run["last_error"] = ""
        pending_run["next_attempt_at"] = None
        pending_run["status"] = "active"
        pending_run["manual_review"] = False
        _pending_clear_action_attempt(pending_run, index)
        _persist_pending_pack_run_state(
            account_id,
            thread_id,
            recipient_username,
            pending_run,
            persist_pending=persist_pending,
        )
        if next_index < total_actions:
            delay_min = max(0, int(pending_run.get("pack_delay_min", 0) or 0))
            delay_max = max(delay_min, int(pending_run.get("pack_delay_max", delay_min) or delay_min))
            _sleep_between_replies_for_account(
                account_id,
                delay_min,
                delay_max,
                label="pack_action_delay",
                apply_safezone_multiplier=False,
            )

    if run_sendable_expected <= 0:
        _safezone_register_failure(
            account_id,
            "pack_without_send_actions",
            severe=False,
        )
        _persist_pending_pack_run_state(
            account_id,
            thread_id,
            recipient_username,
            None,
            persist_pending=persist_pending,
        )
        _set_account_memory(account_id, mutable_memory)
        memory.clear()
        memory.update(mutable_memory)
        return {
            "completed": False,
            "sent_count": sent_count,
            "current_index": total_actions,
            "total_actions": total_actions,
            "error": "pack_without_send_actions",
            "pending_pack_run": None,
            "pending_cleared": True,
        }
    if verified_send_actions != run_sendable_expected:
        pending_run["current_index"] = max(0, total_actions - 1)
        pending_run["updated_at"] = time.time()
        pending_run["status"] = "active"
        pending_run["manual_review"] = False
        pending_run["last_error"] = (
            f"pack_confirmation_incomplete:{verified_send_actions}/{run_sendable_expected}"
        )
        pending_run["next_attempt_at"] = time.time() + _pending_pack_backoff_seconds(1)
        _persist_pending_pack_run_state(
            account_id,
            thread_id,
            recipient_username,
            pending_run,
            persist_pending=persist_pending,
        )
        _safezone_register_failure(
            account_id,
            "pack_confirmation_incomplete",
            severe=True,
        )
        _set_account_memory(account_id, mutable_memory)
        memory.clear()
        memory.update(mutable_memory)
        return {
            "completed": False,
            "sent_count": sent_count,
            "current_index": int(pending_run.get("current_index") or 0),
            "total_actions": total_actions,
            "error": str(pending_run.get("last_error") or "pack_confirmation_incomplete"),
            "pending_pack_run": pending_run,
            "pending_cleared": False,
        }

    _persist_pending_pack_run_state(
        account_id,
        thread_id,
        recipient_username,
        None,
        persist_pending=persist_pending,
    )
    _apply_legacy_pack_markers(
        account_id,
        thread_id,
        recipient_username=recipient_username,
        legacy_payload=None,
        stage_id_hint=resolved_stage_id,
        flow_config=resolved_flow_config,
    )
    _set_account_memory(account_id, mutable_memory)
    memory.clear()
    memory.update(mutable_memory)
    _safezone_register_success(account_id)
    return {
        "completed": True,
        "sent_count": sent_count,
        "current_index": total_actions,
        "total_actions": total_actions,
        "error": "",
        "pending_pack_run": None,
        "pending_cleared": False,
    }


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
    new_key = ask("Nueva API Key (vacÃ­o para cancelar): ").strip()
    if not new_key:
        warn("Se mantuvo la API Key actual.")
        press_enter()
        return
    update_env_local({"OPENAI_API_KEY": new_key})
    refresh_settings()
    ok("OPENAI_API_KEY guardada en .env.local")
    press_enter()


def _configure_prompt() -> None:
    banner()
    print(style_text("Configurar Prompt de Objeciones", color=Fore.CYAN, bold=True))
    print("Selecciona alias puntual o ALL para aplicar a todos los alias.")
    print(full_line(color=Fore.BLUE))
    target_alias_selected = _prompt_alias_selection() or _PROMPT_DEFAULT_ALIAS
    target_alias = target_alias_selected.strip() or _PROMPT_DEFAULT_ALIAS

    target_aliases: List[str] = []
    if _normalize_alias_key(target_alias) == "all":
        for option in _available_aliases():
            option_clean = str(option or "").strip()
            if not option_clean or _normalize_alias_key(option_clean) == "all":
                continue
            if option_clean not in target_aliases:
                target_aliases.append(option_clean)
        prompts_state = _read_prompts_state()
        state_aliases = prompts_state.get("aliases", {})
        if isinstance(state_aliases, dict):
            for raw_alias_key, raw_entry in state_aliases.items():
                if not isinstance(raw_entry, dict):
                    continue
                alias_label = str(raw_entry.get("alias") or raw_alias_key or "").strip()
                if not alias_label or _normalize_alias_key(alias_label) == "all":
                    continue
                if alias_label not in target_aliases:
                    target_aliases.append(alias_label)
        if _PROMPT_DEFAULT_ALIAS not in target_aliases:
            target_aliases.append(_PROMPT_DEFAULT_ALIAS)
    else:
        target_aliases = [target_alias]

    if not target_aliases:
        target_aliases = [_PROMPT_DEFAULT_ALIAS]

    reference_alias = _PROMPT_DEFAULT_ALIAS if _PROMPT_DEFAULT_ALIAS in target_aliases else target_aliases[0]
    alias_header = target_alias
    if len(target_aliases) > 1:
        alias_header = f"{target_alias} ({len(target_aliases)} aliases)"
    while True:
        banner()
        entry = _get_prompt_entry(reference_alias)
        objection_prompt = str(entry.get("objection_prompt") or "").strip()
        objection_strategy_name = str(entry.get("objection_strategy_name") or "").strip()
        print(style_text("Configurar Prompt de Objeciones", color=Fore.CYAN, bold=True))
        print(f"Alias: {alias_header}")
        print(full_line(color=Fore.BLUE))
        print("A) Prompt de Objeciones")
        print(f"   Guardado: {_preview_prompt(objection_prompt)}")
        print(f"   Longitud: {len(objection_prompt)}")
        print(
            f"   Estrategia de objecion: {objection_strategy_name or '(sin definir, configurable al editar)'}"
        )
        print(full_line(color=Fore.BLUE))
        print("1) Prompt de Objeciones")
        print("2) Volver")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opcion: ").strip()
        if choice == "1":
            _edit_prompt_block(
                target_alias,
                field_name="objection_prompt",
                title="Prompt de Objeciones",
                description_lines=[
                    "Se usa cuando la estrategia coincide con objeciones.",
                    "Debe generar texto dinamico.",
                    "No usa packs en este modo.",
                ],
                include_objection_strategy_name=True,
                target_aliases=target_aliases,
            )
        elif choice == "2":
            break
        else:
            warn("Opcion invalida.")
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
        return first_line[:57] + "â€¦"
    if len(prompt.splitlines()) > 1:
        return first_line + " â€¦"
    return first_line


def _prompt_bool(label: str, default: bool = True) -> bool:
    default_label = "S" if default else "N"
    raw = ask(f"{label} (S/N) [{default_label}]: ").strip().lower()
    if not raw:
        return default
    return raw in {"s", "si", "y", "yes", "1", "true"}


def _build_pack_action_from_input(existing: Optional[Dict[str, object]] = None) -> Optional[Dict[str, object]]:
    if existing:
        current_type = str(existing.get("type") or "").strip().lower()
    else:
        current_type = ""
    print("Tipos de accion permitidos:")
    print("1) text_fixed")
    print("2) text_adaptive")
    print("3) set_memory")
    type_raw = ask(
        f"Tipo ({current_type or 'nuevo'}): "
    ).strip().lower()
    if not type_raw and current_type:
        action_type = current_type
    elif type_raw in {"1", "text_fixed"}:
        action_type = "text_fixed"
    elif type_raw in {"2", "text_adaptive"}:
        action_type = "text_adaptive"
    elif type_raw in {"3", "set_memory"}:
        action_type = "set_memory"
    else:
        warn("Tipo invalido.")
        return None

    action: Dict[str, object] = {"type": action_type}
    if action_type == "text_fixed":
        current = str(existing.get("content") or "") if existing else ""
        print(style_text("Contenido actual:", color=Fore.BLUE))
        print(current or "(vacio)")
        content = _collect_prompt_text_from_console("text_fixed.content")
        if not content:
            warn("content vacio.")
            return None
        action["content"] = content
    elif action_type == "text_adaptive":
        current = str(existing.get("instruction") or "") if existing else ""
        print(style_text("Instruction actual:", color=Fore.BLUE))
        print(current or "(vacio)")
        instruction = _collect_prompt_text_from_console("text_adaptive.instruction")
        if not instruction:
            warn("instruction vacia.")
            return None
        action["instruction"] = instruction
    elif action_type == "set_memory":
        current_key = str(existing.get("key") or "") if existing else ""
        key = ask(f"key [{current_key or ''}]: ").strip() or current_key
        if not key:
            warn("key vacia.")
            return None
        current_value = existing.get("value") if existing else ""
        raw_value = ask(f"value (string o bool) [{current_value}]: ").strip()
        value: object
        if not raw_value:
            value = current_value
        else:
            low = raw_value.lower()
            if low in {"true", "false"}:
                value = (low == "true")
            else:
                value = raw_value
        action["key"] = key
        action["value"] = value
    return _normalize_pack_action(action)


def _edit_pack_form(existing: Optional[Dict[str, object]] = None) -> Optional[Dict[str, object]]:
    base = existing or {}
    pack_id = str(base.get("id") or "").strip() or str(uuid.uuid4())
    pack_data: Dict[str, object] = {
        "id": pack_id,
        "name": str(base.get("name") or "").strip(),
        "type": str(base.get("type") or "").strip(),
        "delay_min": int(base.get("delay_min", 0) or 0),
        "delay_max": int(base.get("delay_max", 0) or 0),
        "active": bool(base.get("active", True)),
        "actions": list(base.get("actions") or []),
    }
    if int(pack_data["delay_max"] or 0) < int(pack_data["delay_min"] or 0):
        pack_data["delay_max"] = pack_data["delay_min"]

    while True:
        banner()
        print(style_text("Packs Conversacionales | Crear / Editar", color=Fore.CYAN, bold=True))
        print(f"id: {pack_data['id']}")
        print(f"Nombre: {pack_data['name'] or '(sin definir)'}")
        print(f"Tipo: {pack_data['type'] or '(sin definir)'}")
        print(f"Delay minimo: {pack_data['delay_min']}")
        print(f"Delay maximo: {pack_data['delay_max']}")
        print(f"Activo: {'si' if pack_data['active'] else 'no'}")
        print(style_text("Acciones (orden actual):", color=Fore.BLUE))
        actions = pack_data.get("actions")
        if isinstance(actions, list) and actions:
            for idx, action in enumerate(actions, start=1):
                action_type = str(action.get("type") or "").strip()
                if action_type == "text_fixed":
                    detail = _preview_prompt(str(action.get("content") or ""))
                elif action_type == "text_adaptive":
                    detail = _preview_prompt(str(action.get("instruction") or ""))
                else:
                    detail = f"{action.get('key')}={action.get('value')}"
                print(f" {idx}) {action_type} | {detail}")
        else:
            print(" (sin acciones)")
        print(full_line(color=Fore.BLUE))
        print("1) Editar configuracion superior")
        print("2) Agregar accion")
        print("3) Editar accion")
        print("4) Reordenar acciones (drag & drop)")
        print("5) Eliminar accion")
        print("6) Guardar pack")
        print("7) Cancelar")
        choice = ask("Opcion: ").strip()
        if choice == "1":
            current_name = str(pack_data.get("name") or "")
            current_type = str(pack_data.get("type") or "")
            pack_data["name"] = ask(f"Nombre del pack [{current_name}]: ").strip() or current_name
            pack_data["type"] = ask(f"Tipo (string libre) [{current_type}]: ").strip() or current_type
            pack_data["delay_min"] = ask_int(
                f"Delay minimo [{int(pack_data.get('delay_min') or 0)}]: ",
                0,
                default=int(pack_data.get("delay_min") or 0),
            )
            pack_data["delay_max"] = ask_int(
                f"Delay maximo [{int(pack_data.get('delay_max') or int(pack_data.get('delay_min') or 0))}]: ",
                int(pack_data.get("delay_min") or 0),
                default=int(pack_data.get("delay_max") or int(pack_data.get("delay_min") or 0)),
            )
            pack_data["active"] = _prompt_bool(
                "Activo",
                default=bool(pack_data.get("active", True)),
            )
            continue
        if choice == "2":
            new_action = _build_pack_action_from_input()
            if new_action:
                pack_data.setdefault("actions", [])
                pack_data["actions"].append(new_action)
                ok("Accion agregada.")
            press_enter()
            continue
        if choice == "3":
            actions = pack_data.get("actions")
            if not isinstance(actions, list) or not actions:
                warn("No hay acciones para editar.")
                press_enter()
                continue
            index_raw = ask("Numero de accion a editar: ").strip()
            if not index_raw.isdigit():
                warn("Indice invalido.")
                press_enter()
                continue
            idx = int(index_raw)
            if idx < 1 or idx > len(actions):
                warn("Indice fuera de rango.")
                press_enter()
                continue
            updated_action = _build_pack_action_from_input(actions[idx - 1])
            if updated_action:
                actions[idx - 1] = updated_action
                ok("Accion actualizada.")
            press_enter()
            continue
        if choice == "4":
            actions = pack_data.get("actions")
            if not isinstance(actions, list) or len(actions) < 2:
                warn("Se necesitan al menos 2 acciones para reordenar.")
                press_enter()
                continue
            from_raw = ask("Mover desde posicion: ").strip()
            to_raw = ask("Hacia posicion: ").strip()
            if not from_raw.isdigit() or not to_raw.isdigit():
                warn("Indices invalidos.")
                press_enter()
                continue
            from_idx = int(from_raw) - 1
            to_idx = int(to_raw) - 1
            if from_idx < 0 or from_idx >= len(actions) or to_idx < 0 or to_idx >= len(actions):
                warn("Indices fuera de rango.")
                press_enter()
                continue
            action_item = actions.pop(from_idx)
            actions.insert(to_idx, action_item)
            ok("Orden actualizado.")
            press_enter()
            continue
        if choice == "5":
            actions = pack_data.get("actions")
            if not isinstance(actions, list) or not actions:
                warn("No hay acciones para eliminar.")
                press_enter()
                continue
            index_raw = ask("Numero de accion a eliminar: ").strip()
            if not index_raw.isdigit():
                warn("Indice invalido.")
                press_enter()
                continue
            idx = int(index_raw)
            if idx < 1 or idx > len(actions):
                warn("Indice fuera de rango.")
                press_enter()
                continue
            actions.pop(idx - 1)
            ok("Accion eliminada.")
            press_enter()
            continue
        if choice == "6":
            normalized = _normalize_pack_record(pack_data)
            if not normalized:
                warn("Pack invalido. Revisa nombre, tipo y acciones.")
                press_enter()
                continue
            if not normalized.get("actions"):
                warn("El pack debe tener al menos una accion.")
                press_enter()
                continue
            return normalized
        if choice == "7":
            return None
        warn("Opcion invalida.")
        press_enter()


def _packs_menu() -> None:
    while True:
        banner()
        packs = _list_packs()
        print(style_text("Packs Conversacionales", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        print("Nombre | Tipo | Delay minimo | Delay maximo | Activo")
        if packs:
            for idx, pack in enumerate(packs, start=1):
                print(
                    f"{idx}) {pack.get('name')} | {pack.get('type')} | "
                    f"{pack.get('delay_min')} | {pack.get('delay_max')} | "
                    f"{'si' if pack.get('active') else 'no'} | id={pack.get('id')}"
                )
        else:
            print("(sin packs)")
        print(full_line(color=Fore.BLUE))
        print("1) Crear Nuevo Pack")
        print("2) Editar Pack")
        print("3) Alternar Activo")
        print("4) Volver")
        choice = ask("Opcion: ").strip()
        if choice == "1":
            created = _edit_pack_form()
            if created:
                _upsert_pack(created)
                ok("Pack guardado.")
            press_enter()
            continue
        if choice == "2":
            if not packs:
                warn("No hay packs para editar.")
                press_enter()
                continue
            raw = ask("Selecciona numero o ID del pack: ").strip()
            selected_pack = None
            if raw.isdigit():
                idx = int(raw)
                if 1 <= idx <= len(packs):
                    selected_pack = packs[idx - 1]
            if selected_pack is None:
                for pack in packs:
                    if str(pack.get("id") or "").strip() == raw:
                        selected_pack = pack
                        break
            if selected_pack is None:
                warn("Pack no encontrado.")
                press_enter()
                continue
            edited = _edit_pack_form(selected_pack)
            if edited:
                _upsert_pack(edited)
                ok("Pack actualizado.")
            press_enter()
            continue
        if choice == "3":
            if not packs:
                warn("No hay packs cargados.")
                press_enter()
                continue
            raw = ask("Selecciona numero o ID del pack: ").strip()
            selected_pack = None
            if raw.isdigit():
                idx = int(raw)
                if 1 <= idx <= len(packs):
                    selected_pack = packs[idx - 1]
            if selected_pack is None:
                for pack in packs:
                    if str(pack.get("id") or "").strip() == raw:
                        selected_pack = pack
                        break
            if selected_pack is None:
                warn("Pack no encontrado.")
                press_enter()
                continue
            toggled = dict(selected_pack)
            toggled["active"] = not bool(selected_pack.get("active", False))
            _upsert_pack(toggled)
            ok(f"Pack {'activado' if toggled['active'] else 'desactivado'}.")
            press_enter()
            continue
        if choice == "4":
            return
        warn("Opcion invalida.")
        press_enter()


def autoresponder_menu_options() -> List[str]:
    return [
        "1) Configurar API Key",
        "2) Configurar Prompt de Objeciones",
        "3) Activar bot (alias/grupo)",
        "4) Seguimiento",
        "5) Packs Conversacionales",
        "6) Volver",
    ]


def autoresponder_prompt_length() -> int:
    _, prompt = _load_preferences()
    return len(prompt or "")


def _print_menu_header() -> None:
    banner()
    api_key, _ = _load_preferences(ACTIVE_ALIAS)
    prompt_entry = _get_prompt_entry(ACTIVE_ALIAS or _PROMPT_DEFAULT_ALIAS)
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
    print(
        "Prompt objeciones: "
        f"{_preview_prompt(str(prompt_entry.get('objection_prompt') or '').strip())}"
    )
    print(status)
    print(_followup_summary_line())
    print(full_line(color=Fore.BLUE))
    for option in autoresponder_menu_options():
        print(option)
    print(full_line(color=Fore.BLUE))


def _prompt_alias_selection() -> str | None:
    options = _available_aliases()
    print("Alias/grupos disponibles:")
    for idx, alias in enumerate(options, start=1):
        print(f" {idx}) {alias}")
    raw = ask("SeleccionÃ¡ alias (nÃºmero o texto, Enter=ALL): ").strip()
    if not raw:
        return "ALL"
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(options):
            return options[idx - 1]
        warn("NÃºmero fuera de rango.")
        return None
    return raw


def _handle_account_issue(user: str, exc: Exception, active: List[str]) -> None:
    message = str(exc).lower()
    detail = f"{exc.__class__.__name__}: {exc}"
    if should_retry_proxy(exc):
        label = style_text(f"[WARN][@{user}] proxy fallÃ³", color=Fore.YELLOW, bold=True)
        record_proxy_failure(user, exc)
        print(label)
        warn("RevisÃ¡ la opciÃ³n 1 para actualizar o quitar el proxy de esta cuenta.")
    elif "login_required" in message or "login requerido" in message:
        label = style_text(f"[ERROR][@{user}] sesiÃ³n invÃ¡lida", color=Fore.RED, bold=True)
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
            warn("ElegÃ­ C, R, P o K.")

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
        request_stop("pausa solicitada desde menÃº del bot")
        return

    while choice == "r":
        if _prompt_playwright_login(user, alias=ACTIVE_ALIAS or user) and _ensure_session(user):
            mark_connected(user, True)
            ok(f"SesiÃ³n renovada para @{user}")
            return
        warn("La sesiÃ³n sigue fallando. IntentÃ¡ nuevamente o elegÃ­ otra opciÃ³n.")
        choice = ask("- Reintentar (R) / Continuar sin la cuenta (C) / Pausar (P) / Mantener en ciclo (K)? ").strip().lower()
        if choice == "c":
            if user in active:
                active.remove(user)
            mark_connected(user, False)
            warn(f"Se excluye @{user} del ciclo actual.")
            return
        if choice == "p":
            request_stop("pausa solicitada desde menÃº del bot")
            return
        if choice == "k":
            warn(f"Se mantiene @{user} en el ciclo y se reintentara en la siguiente vuelta.")
            return


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
    merged["pending_hydration"] = bool(
        base.get("pending_hydration", False)
        or incoming.get("pending_hydration", False)
    )
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
    for hydration_ts_field in (
        "last_hydration_attempt_at",
        "last_hydration_success_at",
    ):
        ts_a = _safe_float(base.get(hydration_ts_field))
        ts_b = _safe_float(incoming.get(hydration_ts_field))
        if ts_a is None and ts_b is None:
            continue
        if ts_a is None:
            merged[hydration_ts_field] = ts_b
        elif ts_b is None:
            merged[hydration_ts_field] = ts_a
        else:
            merged[hydration_ts_field] = max(ts_a, ts_b)
    hydration_reason = (
        str(incoming.get("last_hydration_reason") or "").strip()
        or str(base.get("last_hydration_reason") or "").strip()
    )
    if hydration_reason:
        merged["last_hydration_reason"] = hydration_reason
    merged["consecutive_open_failures"] = max(
        _safe_int(base.get("consecutive_open_failures")),
        _safe_int(incoming.get("consecutive_open_failures")),
    )
    pending_base = _normalize_pending_pack_run(base.get("pending_pack_run"))
    pending_incoming = _normalize_pending_pack_run(incoming.get("pending_pack_run"))
    selected_pending = None
    if pending_base and pending_incoming:
        base_ts = _safe_float(pending_base.get("updated_at"))
        incoming_ts = _safe_float(pending_incoming.get("updated_at"))
        if base_ts is None and incoming_ts is None:
            selected_pending = pending_incoming
        elif base_ts is None:
            selected_pending = pending_incoming
        elif incoming_ts is None:
            selected_pending = pending_base
        else:
            selected_pending = pending_incoming if incoming_ts >= base_ts else pending_base
    elif pending_incoming:
        selected_pending = pending_incoming
    elif pending_base:
        selected_pending = pending_base
    merged["pending_pack_run"] = selected_pending
    base_flow = _normalize_flow_state(
        base.get("flow_state"),
        fallback_stage_id="",
        last_outbound_ts=_safe_float(base.get("last_message_sent_at")),
        followup_level_hint=_safe_int(base.get("followup_stage")),
    )
    incoming_flow = _normalize_flow_state(
        incoming.get("flow_state"),
        fallback_stage_id=str(base_flow.get("stage_id") or ""),
        last_outbound_ts=_safe_float(incoming.get("last_message_sent_at")),
        followup_level_hint=_safe_int(incoming.get("followup_stage")),
    )
    base_stage_ts = _safe_float(base_flow.get("last_stage_change_ts")) or 0.0
    incoming_stage_ts = _safe_float(incoming_flow.get("last_stage_change_ts")) or 0.0
    if incoming_stage_ts >= base_stage_ts:
        selected_flow = incoming_flow
    else:
        selected_flow = base_flow
    selected_flow["last_outbound_ts"] = max(
        _safe_float(base_flow.get("last_outbound_ts")) or 0.0,
        _safe_float(incoming_flow.get("last_outbound_ts")) or 0.0,
    ) or None
    selected_flow["followup_level"] = max(
        _safe_int(base_flow.get("followup_level")),
        _safe_int(incoming_flow.get("followup_level")),
    )
    selected_flow["objection_step"] = max(
        _safe_int(base_flow.get("objection_step")),
        _safe_int(incoming_flow.get("objection_step")),
    )
    base_fingerprint = _normalize_last_outbound_fingerprint(
        base_flow.get("last_outbound_fingerprint")
    )
    incoming_fingerprint = _normalize_last_outbound_fingerprint(
        incoming_flow.get("last_outbound_fingerprint")
    )
    base_fp_ts = _safe_float(base_fingerprint.get("ts")) or 0.0
    incoming_fp_ts = _safe_float(incoming_fingerprint.get("ts")) or 0.0
    if incoming_fp_ts >= base_fp_ts:
        selected_fingerprint = incoming_fingerprint
    else:
        selected_fingerprint = base_fingerprint
    selected_flow["last_outbound_fingerprint"] = selected_fingerprint
    selected_reconstruction_status = str(incoming_flow.get("reconstruction_status") or "").strip().lower()
    if not selected_reconstruction_status:
        selected_reconstruction_status = str(base_flow.get("reconstruction_status") or "").strip().lower()
    selected_flow["reconstruction_status"] = str(selected_reconstruction_status or "").strip()
    if not selected_flow.get("followup_anchor_ts"):
        selected_flow["followup_anchor_ts"] = (
            _safe_float(base_flow.get("followup_anchor_ts"))
            or _safe_float(incoming_flow.get("followup_anchor_ts"))
            or selected_flow.get("last_outbound_ts")
        )
    selected_flow["outbox"] = _merge_flow_outbox_entries(
        base_flow.get("outbox"),
        incoming_flow.get("outbox"),
    )
    merged["flow_state"] = selected_flow

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
        "pending_hydration": False,
        "pending_inbound_id": None,
        "last_reply_failure_reason": None,
        "last_reply_failed_at": None,
        "last_send_failed_at": None,
        "last_open_failed_at": None,
        "last_hydration_attempt_at": None,
        "last_hydration_success_at": None,
        "last_hydration_reason": None,
        "pending_pack_run": None,
        "consecutive_open_failures": 0,
        "open_backoff_until": None,
        "prompt_sequence_done": False,
        "prompt_sequence_done_at": None,
        "last_message_sender": None,
        "followup_stage": 0,
        "last_followup_sent_at": None,
        "flow_state": _default_flow_state(""),
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
                "timestamp_source": str(raw.get("timestamp_source") or "").strip()
                or (
                    "api"
                    if _safe_float(
                        raw.get("timestamp_epoch")
                        if raw.get("timestamp_epoch") is not None
                        else raw.get("timestamp")
                    )
                    is not None
                    else "missing"
                ),
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


def _message_timestamp_stats(messages: object) -> tuple[int, int]:
    if not isinstance(messages, list):
        return 0, 0
    total = 0
    missing = 0
    for raw in messages:
        if not isinstance(raw, dict):
            continue
        total += 1
        ts_value = _safe_float(
            raw.get("timestamp_epoch")
            if raw.get("timestamp_epoch") is not None
            else raw.get("timestamp")
        )
        if ts_value is None:
            missing += 1
    return missing, total


def _message_direction_from_snapshot(raw: Dict[str, Any], *, client_user_id: object) -> str:
    direction_raw = str(raw.get("direction") or "").strip().lower()
    sender_id = str(raw.get("sender_id") or raw.get("user_id") or "").strip()
    self_id = str(client_user_id or "").strip()
    if direction_raw in {"outbound", "outgoing", "sent", "viewer"}:
        return "outbound"
    if direction_raw in {"inbound", "incoming", "received"}:
        return "inbound"
    if direction_raw == "unknown":
        return "unknown"
    if sender_id and self_id and sender_id == self_id:
        return "outbound"
    if sender_id and self_id and sender_id != self_id:
        return "inbound"
    return "unknown"


def _thread_requires_hydration(
    row: Dict[str, Any],
    *,
    mode: str,
    client_user_id: object,
) -> tuple[bool, str, int]:
    if bool(row.get("pending_hydration", False)):
        return True, "pending_hydration_state", 95
    raw_messages = row.get("messages")
    normalized = _normalize_snapshot_messages(raw_messages, client_user_id=client_user_id)
    if not normalized:
        return True, "missing_messages", 100
    missing_ts, total = _message_timestamp_stats(normalized)
    if total <= 0:
        return True, "missing_messages", 100
    if missing_ts <= 0:
        return False, "", 0

    inbound_missing = 0
    outbound_missing = 0
    inbound_with_ts = 0
    outbound_with_ts = 0
    for item in normalized[:40]:
        if not isinstance(item, dict):
            continue
        direction = _message_direction_from_snapshot(item, client_user_id=client_user_id)
        ts_value = _safe_float(item.get("timestamp_epoch"))
        if direction == "inbound":
            if ts_value is None:
                inbound_missing += 1
            else:
                inbound_with_ts += 1
        elif direction == "outbound":
            if ts_value is None:
                outbound_missing += 1
            else:
                outbound_with_ts += 1

    try:
        unread_count = max(0, int(row.get("unread_count") or 0))
    except Exception:
        unread_count = 0
    pending_reply = bool(row.get("pending_reply", False))
    last_sender = str(row.get("last_message_sender") or "").strip().lower()
    sender_is_lead = last_sender in {"lead", "inbound", "incoming", "received", "peer"}

    if mode == "reply":
        if inbound_missing > 0 and (pending_reply or unread_count > 0 or sender_is_lead or inbound_with_ts == 0):
            return True, "inbound_timestamp_missing", 90
    if mode == "followup":
        if outbound_missing > 0 and outbound_with_ts == 0:
            return True, "outbound_timestamp_missing", 85
    if missing_ts >= max(2, total // 2):
        return True, "timestamps_incomplete", 70
    return False, "", 0


def _messages_from_client_objects(
    messages: object,
    *,
    client_user_id: object,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(messages, list):
        return out
    self_id = str(client_user_id or "").strip()
    for raw in messages:
        if isinstance(raw, dict):
            message_id = str(raw.get("message_id") or raw.get("id") or "").strip()
            sender_id = str(raw.get("sender_id") or raw.get("user_id") or "").strip()
            text = str(raw.get("text") or "")
            direction = _message_direction_from_snapshot(raw, client_user_id=self_id)
            ts_value = _safe_float(
                raw.get("timestamp_epoch")
                if raw.get("timestamp_epoch") is not None
                else raw.get("timestamp")
            )
        else:
            message_id = str(getattr(raw, "id", "") or getattr(raw, "message_id", "") or "").strip()
            sender_id = str(getattr(raw, "user_id", "") or getattr(raw, "sender_id", "") or "").strip()
            text = str(getattr(raw, "text", "") or "")
            direction = str(getattr(raw, "direction", "") or "").strip().lower()
            if direction not in {"inbound", "outbound"}:
                if sender_id and self_id and sender_id == self_id:
                    direction = "outbound"
                elif sender_id and self_id and sender_id != self_id:
                    direction = "inbound"
                else:
                    direction = "unknown"
            ts_value = _safe_float(getattr(raw, "timestamp", None))
        if not message_id:
            continue
        out.append(
            {
                "message_id": message_id,
                "direction": direction if direction in {"inbound", "outbound"} else "unknown",
                "text": text,
                "timestamp_epoch": ts_value,
                "sender_id": sender_id,
                "timestamp_source": "api_live" if ts_value is not None else "missing",
            }
        )
    out.sort(
        key=lambda item: (
            float(item.get("timestamp_epoch") or 0.0),
            str(item.get("message_id") or ""),
        ),
        reverse=True,
    )
    return out


def _build_hydration_snapshot(
    row: Dict[str, Any],
    *,
    messages: List[Dict[str, Any]],
) -> Dict[str, Any]:
    snapshot = {
        "thread_id": str(row.get("thread_id") or "").strip(),
        "thread_id_api": str(row.get("thread_id_api") or row.get("thread_id") or "").strip(),
        "thread_id_real": str(row.get("thread_id_real") or "").strip(),
        "thread_href": _normalize_thread_href(row.get("thread_href")),
        "recipient_id": str(row.get("recipient_id") or row.get("recipient_username") or "").strip(),
        "recipient_username": str(row.get("recipient_username") or "unknown").strip() or "unknown",
        "title": str(row.get("title") or row.get("recipient_username") or "").strip(),
        "snippet": str(row.get("snippet") or "").strip(),
        "unread_count": row.get("unread_count", 0),
        "last_activity_at": _safe_float(row.get("last_activity_at")),
        "messages": list(messages),
    }
    if not snapshot["title"]:
        snapshot["title"] = snapshot["recipient_username"]
    if not snapshot["recipient_id"]:
        snapshot["recipient_id"] = snapshot["recipient_username"]
    if messages:
        latest = messages[0]
        if not snapshot["snippet"]:
            snapshot["snippet"] = str(latest.get("text") or "")
        latest_ts = _safe_float(latest.get("timestamp_epoch"))
        if latest_ts is not None:
            existing_last_activity = _safe_float(snapshot.get("last_activity_at"))
            snapshot["last_activity_at"] = (
                latest_ts
                if existing_last_activity is None
                else max(existing_last_activity, latest_ts)
            )
    return snapshot


def _mark_pending_hydration(
    account: str,
    thread_id: str,
    *,
    recipient_username: str,
    reason: str,
) -> None:
    now_ts = time.time()
    _update_conversation_state(
        account,
        thread_id,
        {
            "pending_hydration": True,
            "last_hydration_attempt_at": now_ts,
            "last_hydration_reason": str(reason or "").strip() or "pending_hydration",
        },
        recipient_username=recipient_username,
    )


def _clear_pending_hydration(
    account: str,
    thread_id: str,
    *,
    recipient_username: str,
) -> None:
    now_ts = time.time()
    _update_conversation_state(
        account,
        thread_id,
        {
            "pending_hydration": False,
            "last_hydration_success_at": now_ts,
            "last_hydration_reason": "",
        },
        recipient_username=recipient_username,
    )


def _looks_like_rate_or_challenge(text: object) -> bool:
    normalized = _normalize_text_for_match(str(text or ""))
    if not normalized:
        return False
    tokens = (
        "429",
        "too many requests",
        "rate limit",
        "rate_limited",
        "feedback required",
        "feedback_required",
        "challenge",
        "checkpoint",
    )
    return any(token in normalized for token in tokens)


def _hydrate_threads_on_demand(
    client,
    user: str,
    rows: List[Dict[str, Any]],
    *,
    mode: str,
    max_threads: Optional[int] = None,
) -> Dict[str, int]:
    if not rows:
        return {
            "attempted": 0,
            "success": 0,
            "complete": 0,
            "queued": 0,
            "updated": 0,
        }

    runtime = _get_autoresponder_runtime_controller()
    runtime.begin_cycle(user)
    remaining_budget = runtime.remaining_hydrations_for_cycle(user)
    if max_threads is not None:
        remaining_budget = min(remaining_budget, max(0, int(max_threads)))
    if remaining_budget <= 0:
        return {
            "attempted": 0,
            "success": 0,
            "complete": 0,
            "queued": 0,
            "updated": 0,
        }

    row_by_thread: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        thread_id = str(row.get("thread_id") or "").strip()
        if thread_id:
            row_by_thread[thread_id] = row

    candidates: List[tuple[int, float, str, str]] = []
    seen: set[str] = set()

    pending_items = runtime.dequeue_pending(user, limit=max(1, remaining_budget))
    for pending in pending_items:
        if not isinstance(pending, PendingHydration):
            continue
        thread_id = str(pending.thread_id or "").strip()
        if not thread_id or thread_id in seen:
            continue
        row = row_by_thread.get(thread_id)
        if row is None:
            runtime.enqueue_pending(
                user,
                thread_id,
                reason=pending.reason,
                priority=int(getattr(pending, "priority", 0)),
            )
            continue
        last_activity = _safe_float(row.get("last_activity_at")) or 0.0
        priority = 120 + max(0, int(getattr(pending, "priority", 0)))
        candidates.append((priority, last_activity, thread_id, str(pending.reason or "")))
        seen.add(thread_id)

    for row in rows:
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id or thread_id in seen:
            continue
        need, reason, priority = _thread_requires_hydration(
            row,
            mode=mode,
            client_user_id=getattr(client, "user_id", ""),
        )
        if not need:
            continue
        last_activity = _safe_float(row.get("last_activity_at")) or 0.0
        try:
            unread_count = max(0, int(row.get("unread_count") or 0))
        except Exception:
            unread_count = 0
        if unread_count > 0:
            priority += min(25, unread_count * 5)
        if bool(row.get("pending_reply", False)):
            priority += 30
        candidates.append((priority, last_activity, thread_id, reason))
        seen.add(thread_id)

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    snapshots: List[Dict[str, Any]] = []
    attempted = 0
    success = 0
    complete = 0
    queued = 0

    for priority, last_activity, thread_id, reason in candidates:
        if STOP_EVENT.is_set():
            break
        if attempted >= remaining_budget:
            break
        row = row_by_thread.get(thread_id)
        if row is None:
            continue
        recipient_username = (
            str(row.get("recipient_username") or "").strip() or "unknown"
        )
        can_hydrate, hydrate_reason = runtime.should_hydrate(
            user,
            thread_id,
            last_activity_at=last_activity,
            critical=priority >= 80,
        )
        if not can_hydrate:
            runtime.enqueue_pending(
                user,
                thread_id,
                reason=f"{reason}|{hydrate_reason}",
                priority=max(1, int(priority // 10)),
            )
            queued += 1
            continue

        attempted += 1
        _mark_pending_hydration(
            user,
            thread_id,
            recipient_username=recipient_username,
            reason=reason,
        )
        thread = _thread_from_memory_state(client, row)
        if thread is None:
            runtime.record_hydration_attempt(
                user,
                thread_id,
                success=False,
                complete=False,
                last_activity_at=last_activity,
            )
            runtime.enqueue_pending(
                user,
                thread_id,
                reason="thread_from_memory_none",
                priority=max(1, int(priority // 8)),
            )
            queued += 1
            continue

        try:
            live_messages = client.get_messages(thread, amount=40)
        except Exception as exc:
            error_text = str(exc or "").strip() or "hydration_exception"
            if _looks_like_rate_or_challenge(error_text):
                runtime.mark_rate_signal(user, reason=error_text)
            runtime.record_hydration_attempt(
                user,
                thread_id,
                success=False,
                complete=False,
                last_activity_at=last_activity,
            )
            runtime.enqueue_pending(
                user,
                thread_id,
                reason=error_text,
                priority=max(1, int(priority // 6)),
            )
            queued += 1
            continue

        normalized_live = _messages_from_client_objects(
            live_messages,
            client_user_id=getattr(client, "user_id", ""),
        )
        if not normalized_live:
            runtime.record_hydration_attempt(
                user,
                thread_id,
                success=False,
                complete=False,
                last_activity_at=last_activity,
            )
            runtime.enqueue_pending(
                user,
                thread_id,
                reason="hydration_empty_messages",
                priority=max(1, int(priority // 6)),
            )
            queued += 1
            continue

        missing_after, total_after = _message_timestamp_stats(normalized_live)
        is_complete = bool(total_after > 0 and missing_after == 0)
        runtime.record_hydration_attempt(
            user,
            thread_id,
            success=True,
            complete=is_complete,
            last_activity_at=last_activity,
        )
        if is_complete:
            _clear_pending_hydration(
                user,
                thread_id,
                recipient_username=recipient_username,
            )
            complete += 1
        else:
            runtime.enqueue_pending(
                user,
                thread_id,
                reason="hydration_incomplete_after_fetch",
                priority=max(1, int(priority // 6)),
            )
            queued += 1

        snapshots.append(
            _build_hydration_snapshot(
                row,
                messages=normalized_live,
            )
        )
        success += 1

        if not STOP_EVENT.is_set():
            jitter_s = runtime.next_jitter_seconds()
            if jitter_s > 0:
                time.sleep(jitter_s)

    updated = 0
    if snapshots:
        _new_count, updated = _upsert_threads_into_memory(
            client,
            user,
            snapshots,
            source_label=f"runtime_{mode}_hydration",
        )

    snapshot = runtime.snapshot(user)
    logger.info(
        "Hydration runtime account=@%s mode=%s attempted=%s success=%s complete=%s queued=%s updated=%s req_min=%s pending=%s risk=%s",
        user,
        mode,
        attempted,
        success,
        complete,
        queued,
        updated,
        int(snapshot.get("requests_last_minute") or 0),
        int(snapshot.get("pending_hydration") or 0),
        int(snapshot.get("risk_score") or 0),
    )
    return {
        "attempted": attempted,
        "success": success,
        "complete": complete,
        "queued": queued,
        "updated": updated,
    }


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


def _inbox_endpoint_account_payload(client: object, user: str = "") -> Dict[str, Any]:
    account = getattr(client, "account", None)
    payload = dict(account) if isinstance(account, dict) else {}
    resolved_user = str(payload.get("username") or user or "").strip().lstrip("@")
    if not resolved_user:
        resolved_user = str(getattr(client, "username", "") or "").strip().lstrip("@")
    if not resolved_user and user:
        fallback = get_account(user)
        if isinstance(fallback, dict):
            payload.update(fallback)
            resolved_user = str(payload.get("username") or user).strip().lstrip("@")
    if resolved_user:
        payload["username"] = resolved_user
    return payload


def _fetch_inbox_threads_snapshot_page(
    client: object,
    *,
    user: str,
    cursor: str = "",
    limit: int = 20,
    message_limit: int = 20,
    timeout_seconds: float = 10.0,
) -> Dict[str, Any]:
    account = _inbox_endpoint_account_payload(client, user)
    if not str(account.get("username") or "").strip():
        raise RuntimeError(f"account_not_found:{user}")
    return fetch_account_threads_page_from_storage(
        account,
        cursor=cursor,
        limit=limit,
        message_limit=message_limit,
        timeout_seconds=max(2.0, float(timeout_seconds or 10.0)),
    )


def _find_thread_hint_by_username(
    client: object,
    recipient_username: str,
) -> tuple[str, str]:
    account = _inbox_endpoint_account_payload(client)
    if not str(account.get("username") or "").strip():
        return "", ""
    username_norm = _normalize_username(recipient_username)
    if not username_norm:
        return "", ""
    attempts = (
        {
            "cursor": "",
            "limit": 80,
            "message_limit": 5,
            "timeout_seconds": 7.0,
        },
        {
            "cursor": "",
            "limit": 50,
            "message_limit": 5,
            "timeout_seconds": 6.0,
        },
        {
            "cursor": "",
            "limit": 30,
            "message_limit": 3,
            "timeout_seconds": 5.0,
        },
    )
    for kwargs in attempts:
        try:
            payload = fetch_account_threads_page_from_storage(account, **kwargs)
        except Exception:
            continue
        snapshots = list(payload.get("threads") or []) if isinstance(payload, dict) else []
        if not snapshots:
            continue
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            candidate_username = _normalize_username(str(snapshot.get("recipient_username") or ""))
            candidate_title = _normalize_username(str(snapshot.get("title") or ""))
            if username_norm not in {candidate_username, candidate_title}:
                continue
            thread_href = _normalize_thread_href(snapshot.get("thread_href"))
            thread_id_real = str(snapshot.get("thread_id_real") or "").strip()
            thread_id_api = str(snapshot.get("thread_id_api") or snapshot.get("thread_id") or "").strip()
            href_thread_id = _extract_thread_id_from_href(thread_href)
            thread_id_hint = ""
            for candidate in (thread_id_real, href_thread_id, thread_id_api):
                candidate_clean = str(candidate or "").strip()
                if candidate_clean:
                    thread_id_hint = candidate_clean
                    break
            return thread_id_hint, thread_href
    return "", ""


def _open_thread_with_strategy(
    client: object,
    thread: object,
    *,
    thread_id_hint: str = "",
    thread_href_hint: str = "",
    recipient_username: str = "",
) -> tuple[bool, str]:
    open_thread_internal_fn = getattr(client, "_open_thread", None)
    open_thread_by_id_fn = getattr(client, "open_thread", None)
    open_by_href_fn = getattr(client, "open_thread_by_href", None)
    refresh_inbox_fn = getattr(client, "_open_inbox", None)
    ensure_workspace_fn = getattr(client, "_ensure_inbox_workspace_fast", None)
    preferred_href = _normalize_thread_href(thread_href_hint)
    preferred_id = str(thread_id_hint or "").strip()

    def _attempt_open_once() -> tuple[bool, str]:
        if callable(open_thread_internal_fn):
            try:
                if bool(open_thread_internal_fn(thread)):
                    return True, "thread_id"
            except Exception:
                pass
        if preferred_href and callable(open_by_href_fn):
            try:
                if bool(open_by_href_fn(preferred_href)):
                    return True, "href"
            except Exception:
                pass
        if preferred_id and callable(open_thread_by_id_fn):
            try:
                if bool(open_thread_by_id_fn(preferred_id)):
                    return True, "thread_id_hint"
            except Exception:
                pass
        discover_id, discover_href = _find_thread_hint_by_username(client, recipient_username)
        if discover_id and callable(open_thread_by_id_fn):
            try:
                if bool(open_thread_by_id_fn(discover_id)):
                    return True, "username_id"
            except Exception:
                pass
        if discover_href and callable(open_by_href_fn):
            try:
                if bool(open_by_href_fn(discover_href)):
                    return True, "username_href"
            except Exception:
                pass
        return False, "open_thread_strategy_failed"

    opened, reason = _attempt_open_once()
    if opened:
        return True, reason
    if callable(refresh_inbox_fn):
        try:
            refresh_inbox_fn(force_reload=True)
        except TypeError:
            try:
                refresh_inbox_fn()
            except Exception:
                pass
        except Exception:
            pass
    if callable(ensure_workspace_fn):
        try:
            ensure_workspace_fn()
        except Exception:
            pass
    sleep_with_stop(0.2)
    opened, reason_after_refresh = _attempt_open_once()
    if opened:
        return True, reason_after_refresh
    return False, reason_after_refresh or reason or "open_thread_strategy_failed"


def _conversation_text_from_memory(messages: List[_MemoryMessageSnapshot], client_user_id: object) -> str:
    lines: List[str] = []
    for msg in reversed(messages):
        prefix = "YO" if _message_outbound_status(msg, client_user_id) is True else "ELLOS"
        lines.append(f"{prefix}: {msg.text or ''}")
    return "\n".join(lines)


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
        if max_age_seconds and inbound_ts is None:
            return "skip", {"reason": "inbound_timestamp_missing"}
        if max_age_seconds and inbound_ts is not None and (now_ts - inbound_ts) > max_age_seconds:
            return "skip", {"reason": "inbound_antiguo"}

        last_inbound_seen = str(state.get("last_inbound_id_seen") or "").strip()
        if not last_inbound_seen:
            # Compatibilidad con memorias antiguas: solo adoptar legacy seen
            # cuando hubo un envÃ­o posterior al inbound.
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
        # Compatibilidad con comportamiento previo: si el Ãºltimo evento del hilo
        # es outbound (bot mÃ¡s reciente que inbound), no tratar ese inbound como nuevo
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
        try:
            unread_count = max(0, int(state.get("unread_count") or 0))
        except Exception:
            unread_count = 0
        last_sender = str(state.get("last_message_sender") or "").strip().lower()
        sender_indicates_lead = last_sender in {"lead", "inbound", "incoming", "received", "peer"}
        last_sent_at_state = _safe_float(state.get("last_message_sent_at"))
        last_received_at_state = _safe_float(state.get("last_message_received_at"))
        inbound_after_last_sent = bool(
            last_received_at_state is not None
            and (
                last_sent_at_state is None
                or last_received_at_state > (last_sent_at_state + 1.0)
            )
        )
        inbound_newer_than_outbound = bool(
            last_outbound is None
            or _message_is_newer_than(last_inbound, last_outbound, messages)
        )
        pending_real_inbound = bool(
            (unread_count > 0 or (sender_indicates_lead and inbound_after_last_sent))
            and inbound_newer_than_outbound
        )
        if has_new_inbound or has_pending_retry:
            return "reply", {
                "last_inbound": last_inbound,
                "messages": messages,
                "latest_inbound_id": last_id_str,
                "retry_pending": has_pending_retry,
            }
        if pending_real_inbound:
            return "reply", {
                "last_inbound": last_inbound,
                "messages": messages,
                "latest_inbound_id": last_id_str,
                "retry_pending": False,
                "pending_real_inbound": True,
            }

        if last_inbound_seen and last_inbound_seen == last_id_str:
            return "skip", {"reason": "inbound_ya_visto"}

    return "skip", {"reason": "sin_accion"}


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
    print(style_text("ðŸ”Ž SincronizaciÃ³n inicial del inbox", color=Fore.CYAN, bold=True))
    page_number = 0
    cursor = ""
    accumulated: Dict[str, Dict[str, Any]] = {}
    page_size = max(10, min(80, target))
    while not STOP_EVENT.is_set() and len(accumulated) < target:
        remaining_s = deadline_ts - time.time()
        if remaining_s <= 0:
            logger.warning(
                "Discovery inicial de @%s alcanzÃ³ el lÃ­mite de %.1fs (acumulado=%s, objetivo=%s).",
                user,
                initial_budget_s,
                len(accumulated),
                target,
            )
            print(
                style_text(
                    f"â±ï¸ SincronizaciÃ³n inicial alcanzÃ³ {round(initial_budget_s, 1)}s; continÃºo con lo disponible.",
                    color=Fore.YELLOW,
                )
            )
            break
        remaining_ms = max(1000, int(remaining_s * 1000))
        request_timeout_ms = min(page_request_timeout_ms_cfg, remaining_ms)
        total_timeout_ms = min(max(request_timeout_ms, 2000), remaining_ms)
        page_number += 1
        try:
            page_result = _fetch_inbox_threads_snapshot_page(
                client,
                user=user,
                cursor=cursor,
                limit=page_size,
                message_limit=20,
                timeout_seconds=float(total_timeout_ms) / 1000.0,
            )
        except Exception as exc:
            logger.warning(
                "Discovery inicial por endpoint fallÃ³ para @%s en pÃ¡gina %s: %s",
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
                f"ðŸ“¦ PÃ¡gina {page_number} cargada â†’ {len(page_threads)} threads (acumulado {len(accumulated)})",
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
        logger.warning("Sin resultados de endpoint para sincronizaciÃ³n inicial de @%s.", user)
        return []

    print(style_text("ðŸ§  Construyendo memoria...", color=Fore.GREEN))
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
    print(style_text("ðŸ’¾ Memoria persistida correctamente", color=Fore.GREEN))
    print(style_text("âœ… SincronizaciÃ³n inicial completada", color=Fore.GREEN, bold=True))
    discovered_ids: List[str] = []
    for snapshot in snapshots:
        thread_id = str(snapshot.get("thread_id") or "").strip()
        if not thread_id or thread_id in discovered_ids:
            continue
        discovered_ids.append(thread_id)
    _emit_autoresponder_event(
        "THREADS_DISCOVERED",
        account=user,
        source="initial",
        discovered=len(discovered_ids),
    )
    return discovered_ids


def incremental_discovery_sync(
    client,
    user: str,
    page_limit: int = 30,
) -> tuple[int, int, List[str]]:
    print(style_text("ðŸ”„ Verificando nuevos mensajes...", color=Fore.CYAN))
    try:
        page_result = _fetch_inbox_threads_snapshot_page(
            client,
            user=user,
            cursor="",
            limit=max(5, int(page_limit or 30)),
            message_limit=20,
            timeout_seconds=max(4.0, float(_env_int("AUTORESPONDER_INCREMENTAL_DISCOVERY_TIMEOUT_S", 8))),
        )
    except Exception as exc:
        logger.warning("Sync incremental fallÃ³ para @%s: %s", user, exc, exc_info=False)
        print(style_text("âœ” No se detectaron cambios", color=Fore.YELLOW))
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
        print(style_text(f"ðŸ“¥ {new_count} threads nuevos detectados", color=Fore.GREEN))
    if new_count <= 0 and updated_count <= 0:
        print(style_text("âœ” No se detectaron cambios", color=Fore.WHITE))
    if new_count > 0:
        _emit_autoresponder_event(
            "THREADS_DISCOVERED",
            account=user,
            source="incremental",
            discovered=int(new_count),
        )
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
        if not bool(row.get("pending_reply", False)) and not bool(row.get("pending_hydration", False)):
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
    prompt_entry: Dict[str, object],
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
    objection_prompt = str(prompt_entry.get("objection_prompt") or "").strip()
    objection_strategy_name = str(prompt_entry.get("objection_strategy_name") or "").strip()
    flow_config = _resolve_flow_config_for_prompt_entry(
        prompt_entry,
        followup_schedule_hours=followup_schedule_hours,
    )
    flow_engine = FlowEngine(flow_config)

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
                f"[Memoria] Workset sin threads vÃ¡lidos para @{user}",
                color=Fore.YELLOW,
            )
        )
        return

    print(style_text(f"ðŸ§® Workset de ciclo: {len(account_threads)}", color=Fore.WHITE, bold=True))
    hydration_summary = _hydrate_threads_on_demand(
        client,
        user,
        account_threads,
        mode="reply",
        max_threads=max(1, min(len(account_threads), 8)),
    )
    if hydration_summary.get("attempted", 0) > 0 and _AUTORESPONDER_VERBOSE_TECH_LOGS:
        print(
            style_text(
                "Hydration reply "
                f"attempted={hydration_summary.get('attempted', 0)} "
                f"success={hydration_summary.get('success', 0)} "
                f"complete={hydration_summary.get('complete', 0)} "
                f"queued={hydration_summary.get('queued', 0)}",
                color=Fore.WHITE,
            )
        )
    if hydration_summary.get("updated", 0) > 0:
        all_account_threads = _account_conversations_from_memory(user, refresh=True)
        thread_map = {
            str(row.get("thread_id") or "").strip(): row
            for row in all_account_threads
            if str(row.get("thread_id") or "").strip()
        }
        account_threads = [thread_map[thread_id] for thread_id in workset_ids if thread_id in thread_map]
        if not account_threads:
            print(
                style_text(
                    f"[Memoria] Workset sin threads vÃ¡lidos para @{user} tras hidrataciÃ³n",
                    color=Fore.YELLOW,
                )
            )
            return

    now_ts = time.time()
    max_age_seconds = max(0, int(max_age_days)) * 24 * 3600 if max_age_days is not None else 0
    print(style_text("ðŸ“Š Analizando estado de los threads...", color=Fore.CYAN, bold=True))
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
        else:
            no_action += 1

    print(style_text("ðŸ“Š Resumen:", color=Fore.WHITE, bold=True))
    print(style_text(f"â€¢ Respuestas pendientes: {pending_replies}", color=Fore.WHITE))
    print(style_text(f"â€¢ Follow-ups listos: {pending_followups}", color=Fore.WHITE))
    print(style_text(f"â€¢ Sin acciÃ³n: {no_action}", color=Fore.WHITE))

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
        print(style_text("AcciÃ³n: Omitido", color=Fore.YELLOW))
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
        print(style_text(f"ðŸ”¹ Thread {idx}/{len(account_threads)} â†’ @{recipient_username}", color=Fore.CYAN))

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
            print(style_text(f"AcciÃ³n: En espera (faltan {minutes} min)", color=Fore.YELLOW))
            continue
        if action != "reply":
            reason_skip = str(details.get("reason") or "accion_sin_envio")
            if reason_skip in {
                "inbound_timestamp_missing",
                "inbound_sin_message_id",
                "sin_mensajes",
            }:
                runtime = _get_autoresponder_runtime_controller()
                runtime.enqueue_pending(
                    user,
                    thread_id,
                    reason=f"decision:{reason_skip}",
                    priority=12,
                )
                _mark_pending_hydration(
                    user,
                    thread_id,
                    recipient_username=recipient_username,
                    reason=reason_skip,
                )
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
        _emit_autoresponder_event(
            "MESSAGE_DETECTED",
            account=user,
            thread_id=thread_id,
            recipient=recipient_username,
            inbound_message_id=last_id_str,
            retry_pending=bool(details.get("retry_pending", False)),
        )
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
            _safezone_register_failure(
                user,
                "thread_invalido_desde_memoria",
                severe=True,
            )
            continue

        opened, open_reason = _open_thread_with_strategy(
            client,
            thread,
            thread_id_hint=intent_open_id or str(thread_id or ""),
            thread_href_hint=intent_open_href,
            recipient_username=recipient_username,
        )
        if not opened:
            logger.warning(
                "intent_open id=%s href=%s cache_hit=%s account=@%s thread_key=%s recipient=@%s reason=%s",
                intent_open_id or "-",
                intent_open_href or "-",
                cache_hit,
                user,
                thread_id,
                recipient_username,
                open_reason,
            )
            _log_response_skip(
                motivo_skip=f"open_thread_failed:{open_reason}",
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
                reason=f"open_thread_failed:{open_reason}",
                open_failed=True,
            )
            _safezone_register_failure(user, f"open_thread_failed:{open_reason}", severe=True)
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
        conv_state_for_pending = _get_conversation_state(user, thread_id)
        pending_pack_run_raw = conv_state_for_pending.get("pending_pack_run")
        pending_pack_run = _normalize_pending_pack_run(pending_pack_run_raw)
        if pending_pack_run_raw and pending_pack_run is None:
            logger.warning(
                "Pending pack invÃ¡lido eliminado | account=@%s thread=%s recipient=@%s",
                user,
                thread_id,
                recipient_username,
            )
            _update_conversation_state(
                user,
                thread_id,
                {"pending_pack_run": None},
                recipient_username,
            )
            print(
                style_text(
                    "AcciÃ³n: Pending invÃ¡lido eliminado Â· re-evaluando estrategia",
                    color=Fore.YELLOW,
                )
            )
        if pending_pack_run:
            pending_inbound_marker = (
                str(conv_state_for_pending.get("pending_inbound_id") or "").strip()
                or str(pending_pack_run.get("latest_inbound_id") or "").strip()
                or last_id_str
            )
            pending_flow_state = _ensure_flow_state_for_thread(
                user,
                thread_id,
                recipient_username=recipient_username,
                conv_state=conv_state_for_pending,
                flow_config=flow_config,
            )
            pending_inbound_text = str(getattr(last_inbound, "text", "") or "")
            pending_decision = flow_engine.evaluate(
                {
                    "flow_state": pending_flow_state,
                    "inbound_text": pending_inbound_text,
                    "latest_inbound_id": pending_inbound_marker,
                    "last_inbound_id_seen": str(conv_state_for_pending.get("last_inbound_id_seen") or "").strip(),
                    "pending_reply": bool(conv_state_for_pending.get("pending_reply")),
                    "pending_inbound_id": str(conv_state_for_pending.get("pending_inbound_id") or "").strip(),
                    "last_outbound_ts": _safe_float(conv_state_for_pending.get("last_message_sent_at")),
                    "followup_level": _safe_int(pending_flow_state.get("followup_level")),
                    "now_ts": time.time(),
                    "objection_strategy_name": objection_strategy_name,
                }
            )
            _log_flow_decision_and_state(
                thread_id,
                decision=pending_decision,
                flow_state=pending_flow_state,
                pending=True,
            )
            expected_action_type = str(pending_pack_run.get("strategy_name") or "").strip()
            decision_kind = str(pending_decision.get("decision") or "").strip().lower()
            decision_action_type = str(pending_decision.get("action_type") or "").strip()
            decision_stage_id = str(
                pending_decision.get("next_stage_id")
                or pending_decision.get("stage_id")
                or pending_flow_state.get("stage_id")
                or ""
            ).strip()
            expected_stage_id = str(pending_pack_run.get("stage_id") or "").strip()
            stage_matches = not expected_stage_id or expected_stage_id == decision_stage_id
            decision_allowed = (
                decision_kind == "reply"
                and decision_action_type == expected_action_type
                and stage_matches
            )
            resumed_result: Dict[str, object] = {}
            if not decision_allowed:
                logger.warning(
                    "PENDING_RESUME_BLOCKED_BY_FLOW account=@%s thread=%s expected_action=%s expected_stage=%s decision=%s action=%s stage=%s reason=%s",
                    user,
                    thread_id,
                    expected_action_type or "-",
                    expected_stage_id or "-",
                    decision_kind or "-",
                    decision_action_type or "-",
                    decision_stage_id or "-",
                    str(pending_decision.get("reason") or ""),
                )
                _update_conversation_state(
                    user,
                    thread_id,
                    {
                        "pending_pack_run": None,
                        "pending_manual_review": False,
                    },
                    recipient_username,
                )
                print(
                    style_text(
                        "AcciÃ³n: Pending bloqueado por FlowEngine Â· re-evaluando estrategia",
                        color=Fore.YELLOW,
                    )
                )
                pending_pack_run = None
            else:
                print(style_text("AcciÃ³n: Reanudando pack pendiente", color=Fore.GREEN))
                account_memory = _get_account_memory(user)
                stats.record_reply_attempt(user)
                resumed_result = execute_pack(
                    {},
                    user,
                    account_memory,
                    client=client,
                    thread=thread,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    api_key=api_key,
                    conversation_text="",
                    strategy_name=expected_action_type,
                    latest_inbound_id=pending_inbound_marker,
                    is_followup=False,
                    followup_stage=None,
                    stage_id=decision_stage_id,
                    stage_anchor_ts=_safe_float(pending_pack_run.get("stage_anchor_ts")),
                    pending_pack_run=pending_pack_run,
                    persist_pending=True,
                    flow_config=flow_config,
                )
            resumed_completed = decision_allowed and bool(resumed_result.get("completed", False))
            if decision_allowed and not resumed_completed:
                resume_error = str(resumed_result.get("error") or "pending_pack_incomplete")
                pending_cleared = bool(resumed_result.get("pending_cleared", False))
                is_confirmation_pending = _pack_error_is_confirmation_pending(resume_error)
                if resume_error.startswith("pending_backoff_waiting:"):
                    print(
                        style_text(
                            f"AcciÃ³n: Pending pack en espera ({resume_error})",
                            color=Fore.YELLOW,
                        )
                    )
                    continue
                if resume_error.startswith("pending_manual_review"):
                    _log_response_skip(
                        motivo_skip=f"pending_pack_incomplete:{resume_error}",
                        thread_id=thread_id,
                        recipient_username=recipient_username,
                        has_new_inbound=True,
                        followup_due=False,
                        can_send_result=False,
                        intent_open_id=intent_open_id,
                        intent_open_href=intent_open_href,
                        intent_open_cache_hit=cache_hit,
                    )
                    _update_conversation_state(
                        user,
                        thread_id,
                        {"pending_manual_review": False},
                        recipient_username,
                    )
                    continue
                if pending_cleared:
                    logger.warning(
                        "Pending pack descartado y reinicio de flujo | account=@%s thread=%s recipient=@%s reason=%s",
                        user,
                        thread_id,
                        recipient_username,
                        resume_error,
                    )
                    print(
                        style_text(
                            f"AcciÃ³n: Pending descartado ({resume_error}) Â· re-evaluando estrategia",
                            color=Fore.YELLOW,
                        )
                    )
                    decision_allowed = False
                else:
                    if is_confirmation_pending:
                        print(
                            style_text(
                                "AcciÃ³n: Pending pack esperando confirmaciÃ³n de envÃ­o",
                                color=Fore.YELLOW,
                            )
                        )
                    else:
                        _log_response_skip(
                            motivo_skip=f"pending_pack_incomplete:{resume_error}",
                            thread_id=thread_id,
                            recipient_username=recipient_username,
                            has_new_inbound=True,
                            followup_due=False,
                            can_send_result=False,
                            intent_open_id=intent_open_id,
                            intent_open_href=intent_open_href,
                            intent_open_cache_hit=cache_hit,
                        )
                    _mark_reply_pending(
                        user,
                        thread_id,
                        recipient_username=recipient_username,
                        inbound_message_id=pending_inbound_marker,
                        reason=f"pending_pack_incomplete:{resume_error}",
                        open_failed=False,
                    )
                    continue

            if resumed_completed:
                latest_pending_conv_state = _get_conversation_state(user, thread_id)
                pending_flow_state = _ensure_flow_state_for_thread(
                    user,
                    thread_id,
                    recipient_username=recipient_username,
                    conv_state=latest_pending_conv_state,
                    flow_config=flow_config,
                )
                pending_flow_state = flow_engine.apply_outbound(
                    pending_flow_state,
                    pending_decision,
                    sent_at=time.time(),
                )
                _record_message_received(user, thread_id, pending_inbound_marker, recipient_username)
                _clear_pending_hydration(
                    user,
                    thread_id,
                    recipient_username=recipient_username,
                )
                _update_conversation_state(
                    user,
                    thread_id,
                    {
                        "stage": _STAGE_WAITING,
                        "pending_reply": False,
                        "pending_inbound_id": None,
                        "last_inbound_id_seen": pending_inbound_marker,
                        "pending_pack_run": None,
                        "pending_manual_review": False,
                        "flow_state": pending_flow_state,
                        "followup_stage": _safe_int(pending_flow_state.get("followup_level")),
                    },
                    recipient_username,
                )
                resumed_sent_count = max(0, _safe_int(resumed_result.get("sent_count")))
                if resumed_sent_count > 0:
                    messages_sent_this_cycle += 1
                    print(style_text("ðŸ“¤ Pack pendiente completado", color=Fore.GREEN))
                    print(style_text("ðŸ’¾ Memoria actualizada", color=Fore.GREEN))
                    index = stats.record_success(user)
                    _print_response_summary(index, user, recipient_username, True)
                else:
                    print(
                        style_text(
                            "ðŸ“¦ Pack pendiente completado sin envio nuevo (contador sin cambios)",
                            color=Fore.YELLOW,
                        )
                    )
                continue

        print(style_text("AcciÃ³n: Preparando respuesta", color=Fore.GREEN))
        print(style_text("Esperando delay...", color=Fore.WHITE))
        _sleep_between_replies_for_account(user, delay_min, delay_max, label="reply_delay")

        convo = _conversation_text_from_memory(memory_messages, getattr(client, "user_id", ""))
        conv_state = _get_conversation_state(user, thread_id)
        inbound_text = str(getattr(last_inbound, "text", "") or "")
        inbound_anchor_ts = _message_timestamp(last_inbound) or time.time()
        response_countable = False
        status = _classify_response(inbound_text)
        if status and recipient_username:
            ts_value = inbound_anchor_ts
            log_conversation_status(
                user,
                recipient_username,
                status,
                timestamp=int(ts_value) if ts_value is not None else None,
            )

        flow_state = _ensure_flow_state_for_thread(
            user,
            thread_id,
            recipient_username=recipient_username,
            conv_state=conv_state,
            flow_config=flow_config,
        )
        _emit_autoresponder_event(
            "CONVERSATION_ANALYZED",
            account=user,
            thread_id=thread_id,
            recipient=recipient_username,
            stage_id=str(flow_state.get("stage_id") or "").strip(),
            reconstruction_status=str(flow_state.get("reconstruction_status") or "").strip(),
        )
        account_memory = _get_account_memory(user)
        try:
            flow_decision = flow_engine.evaluate(
                {
                    "flow_state": flow_state,
                    "inbound_text": inbound_text,
                    "latest_inbound_id": last_id_str,
                    "last_inbound_id_seen": str(conv_state.get("last_inbound_id_seen") or "").strip(),
                    "pending_reply": bool(conv_state.get("pending_reply")),
                    "pending_inbound_id": str(conv_state.get("pending_inbound_id") or "").strip(),
                    "last_outbound_ts": _safe_float(conv_state.get("last_message_sent_at")),
                    "followup_level": _safe_int(flow_state.get("followup_level")),
                    "now_ts": time.time(),
                    "objection_strategy_name": objection_strategy_name,
                }
            )
            _log_flow_decision_and_state(
                thread_id,
                decision=flow_decision,
                flow_state=flow_state,
                pending=bool(conv_state.get("pending_pack_run")),
            )
            if str(flow_decision.get("decision") or "").strip().lower() != "reply":
                _log_response_skip(
                    motivo_skip=f"flow_no_reply:{str(flow_decision.get('reason') or 'no_action')}",
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    has_new_inbound=True,
                    followup_due=False,
                    can_send_result=None,
                    intent_open_id=intent_open_id,
                    intent_open_href=intent_open_href,
                    intent_open_cache_hit=cache_hit,
                )
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
                continue

            action_type = str(flow_decision.get("action_type") or "").strip()
            if not action_type or _is_no_send_strategy(action_type):
                _log_response_skip(
                    motivo_skip=f"flow_action_no_send:{action_type or 'empty'}",
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    has_new_inbound=True,
                    followup_due=False,
                    can_send_result=None,
                    intent_open_id=intent_open_id,
                    intent_open_href=intent_open_href,
                    intent_open_cache_hit=cache_hit,
                )
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
                continue

            use_objection_engine = bool(flow_decision.get("use_objection_engine", False) and objection_prompt)
            _emit_autoresponder_event(
                "FLOW_ACTIVATED",
                account=user,
                thread_id=thread_id,
                recipient=recipient_username,
                action_type=action_type,
                stage_id=str(
                    flow_decision.get("next_stage_id")
                    or flow_decision.get("stage_id")
                    or flow_state.get("stage_id")
                    or ""
                ).strip(),
                reason=str(flow_decision.get("reason") or "").strip(),
            )
            stats.record_reply_attempt(user)
            sent_ok = False
            print(style_text(f"AcciÃ³n de flujo: {action_type}", color=Fore.WHITE))
            if use_objection_engine:
                objection_reply = generate_objection_response(
                    inbound_text,
                    objection_prompt,
                    account_memory,
                    api_key=api_key,
                    conversation_text=convo,
                )
                if not objection_reply:
                    _log_response_skip(
                        motivo_skip="objection_engine_empty",
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
                        reason="objection_engine_empty",
                        open_failed=False,
                    )
                    continue
                can_send, reason = _can_send_message(
                    user,
                    thread_id,
                    objection_reply,
                    latest_inbound_id=last_id_str,
                    force=_FORCE_ALWAYS_RESPOND,
                )
                if not can_send:
                    _log_response_skip(
                        motivo_skip=f"objection_can_send_false:{reason}",
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
                    _mark_reply_pending(
                        user,
                        thread_id,
                        recipient_username=recipient_username,
                        inbound_message_id=last_id_str,
                        reason=f"objection_can_send_false:{reason}",
                        open_failed=False,
                    )
                    continue
                send_result = _normalize_strict_send_result(
                    _send_text_action_strict(client, thread, objection_reply)
                )
                send_status = str(send_result.get("status") or "").strip().lower()
                send_reason = str(send_result.get("reason") or "").strip()
                message_id = str(send_result.get("item_id") or "").strip()
                if send_status != "confirmed" or not message_id:
                    _log_response_skip(
                        motivo_skip=f"objection_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
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
                        reason=f"objection_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
                        open_failed=False,
                    )
                    _emit_autoresponder_event(
                        "MESSAGE_FAILED",
                        account=user,
                        thread_id=thread_id,
                        recipient=recipient_username,
                        reason=send_reason or send_status or "not_confirmed",
                    )
                    index = stats.record_response_error(user)
                    _print_response_summary(
                        index,
                        user,
                        recipient_username,
                        False,
                        extra="Envio de objecion sin confirmacion de salida",
                    )
                    _safezone_register_failure(
                        user,
                        f"objection_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
                        severe=True,
                    )
                    continue
                _record_message_sent(
                    user,
                    thread_id,
                    objection_reply,
                    str(message_id),
                    recipient_username,
                    is_followup=False,
                )
                sent_ok = True
                response_countable = True
                _safezone_register_success(user)
            elif _canonical_flow_action_type(action_type, allow_empty=True) in {"auto_reply", "followup_text"}:
                generated_reply = _generate_autoreply_response(
                    inbound_text,
                    _DEFAULT_RESPONDER_STRATEGY_PROMPT,
                    api_key=api_key,
                    conversation_text=convo,
                    account_memory=account_memory,
                )
                if not generated_reply:
                    _log_response_skip(
                        motivo_skip="autorespuesta_vacia",
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
                        reason="autorespuesta_vacia",
                        open_failed=False,
                    )
                    continue
                can_send, reason = _can_send_message(
                    user,
                    thread_id,
                    generated_reply,
                    latest_inbound_id=last_id_str,
                    force=_FORCE_ALWAYS_RESPOND,
                )
                if not can_send:
                    _log_response_skip(
                        motivo_skip=f"autorespuesta_can_send_false:{reason}",
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
                    _mark_reply_pending(
                        user,
                        thread_id,
                        recipient_username=recipient_username,
                        inbound_message_id=last_id_str,
                        reason=f"autorespuesta_can_send_false:{reason}",
                        open_failed=False,
                    )
                    continue
                send_result = _normalize_strict_send_result(
                    _send_text_action_strict(client, thread, generated_reply)
                )
                send_status = str(send_result.get("status") or "").strip().lower()
                send_reason = str(send_result.get("reason") or "").strip()
                message_id = str(send_result.get("item_id") or "").strip()
                if send_status != "confirmed" or not message_id:
                    _log_response_skip(
                        motivo_skip=f"autorespuesta_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
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
                        reason=f"autorespuesta_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
                        open_failed=False,
                    )
                    _emit_autoresponder_event(
                        "MESSAGE_FAILED",
                        account=user,
                        thread_id=thread_id,
                        recipient=recipient_username,
                        reason=send_reason or send_status or "not_confirmed",
                    )
                    index = stats.record_response_error(user)
                    _print_response_summary(
                        index,
                        user,
                        recipient_username,
                        False,
                        extra="Autorespuesta sin confirmacion de salida",
                    )
                    _safezone_register_failure(
                        user,
                        f"autorespuesta_send_unconfirmed:{send_reason or send_status or 'not_confirmed'}",
                        severe=True,
                    )
                    continue
                _record_message_sent(
                    user,
                    thread_id,
                    generated_reply,
                    str(message_id),
                    recipient_username,
                    is_followup=False,
                )
                sent_ok = True
                response_countable = True
                _safezone_register_success(user)
            else:
                selected_pack = select_pack(action_type, user)
                if not selected_pack:
                    _log_response_skip(
                        motivo_skip=f"no_pack_for_action:{action_type}",
                        thread_id=thread_id,
                        recipient_username=recipient_username,
                        has_new_inbound=True,
                        followup_due=False,
                        can_send_result=None,
                        intent_open_id=intent_open_id,
                        intent_open_href=intent_open_href,
                        intent_open_cache_hit=cache_hit,
                    )
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
                    continue
                _emit_pack_selected_event(
                    account=user,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    pack=selected_pack,
                    fallback_type=action_type,
                )
                pack_result = execute_pack(
                    selected_pack,
                    user,
                    account_memory,
                    client=client,
                    thread=thread,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    api_key=api_key,
                    conversation_text=convo,
                    strategy_name=action_type,
                    latest_inbound_id=last_id_str,
                    is_followup=False,
                    followup_stage=None,
                    stage_id=str(
                        flow_decision.get("next_stage_id")
                        or flow_decision.get("stage_id")
                        or flow_state.get("stage_id")
                        or ""
                    ).strip(),
                    stage_anchor_ts=inbound_anchor_ts,
                    pending_pack_run=None,
                    persist_pending=True,
                    flow_config=flow_config,
                )
                sent_ok = bool(pack_result.get("completed", False))
                pack_sent_count = max(0, _safe_int(pack_result.get("sent_count")))
                response_countable = bool(sent_ok and pack_sent_count > 0)
                if not sent_ok:
                    pack_error = str(pack_result.get("error") or "pack_execution_incomplete")
                    is_confirmation_pending = _pack_error_is_confirmation_pending(pack_error)
                    if is_confirmation_pending:
                        print(
                            style_text(
                                "AcciÃ³n: Pack pendiente de confirmaciÃ³n de envÃ­o",
                                color=Fore.YELLOW,
                            )
                        )
                    else:
                        _log_response_skip(
                            motivo_skip=f"pack_execution_incomplete:{pack_error}",
                            thread_id=thread_id,
                            recipient_username=recipient_username,
                            has_new_inbound=True,
                            followup_due=False,
                            can_send_result=False,
                            intent_open_id=intent_open_id,
                            intent_open_href=intent_open_href,
                            intent_open_cache_hit=cache_hit,
                        )
                    _mark_reply_pending(
                        user,
                        thread_id,
                        recipient_username=recipient_username,
                        inbound_message_id=last_id_str,
                        reason=f"pack_execution_incomplete:{pack_error}",
                        open_failed=False,
                    )
                    continue

            sent_ts = time.time()
            latest_conv_for_flow = _get_conversation_state(user, thread_id)
            latest_flow_for_update = _normalize_flow_state(
                latest_conv_for_flow.get("flow_state"),
                fallback_stage_id=str(flow_state.get("stage_id") or ""),
                last_outbound_ts=_safe_float(latest_conv_for_flow.get("last_message_sent_at")),
                followup_level_hint=_safe_int(latest_conv_for_flow.get("followup_stage")),
            )
            updated_flow_state = flow_engine.apply_outbound(
                latest_flow_for_update,
                flow_decision,
                sent_at=sent_ts,
            )
            _record_message_received(user, thread_id, last_id_str, recipient_username)
            _clear_pending_hydration(
                user,
                thread_id,
                recipient_username=recipient_username,
            )
            _update_conversation_state(
                user,
                thread_id,
                {
                    "stage": _STAGE_WAITING,
                    "pending_reply": False,
                    "pending_inbound_id": None,
                    "last_inbound_id_seen": last_id_str,
                    "flow_state": updated_flow_state,
                    "followup_stage": _safe_int(updated_flow_state.get("followup_level")),
                },
                    recipient_username,
                )
            if response_countable:
                messages_sent_this_cycle += 1
                print(style_text("ðŸ“¤ Flujo ejecutado correctamente", color=Fore.GREEN))
                _emit_followup_scheduled_event(
                    account=user,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    flow_config=flow_config,
                    flow_state=updated_flow_state,
                )
            else:
                print(
                    style_text(
                        "ðŸ“¦ Flujo completado sin envio nuevo (contador sin cambios)",
                        color=Fore.YELLOW,
                    )
                )
            print(style_text("ðŸ’¾ Memoria actualizada", color=Fore.GREEN))
        except Exception as exc:
            exc_reason = f"decision_cycle_exception:{exc}"
            _safezone_register_failure(
                user,
                exc_reason,
                severe=_safezone_reason_is_severe(exc_reason),
            )
            setattr(exc, "_autoresponder_sender", user)
            setattr(exc, "_autoresponder_recipient", recipient_username)
            setattr(exc, "_autoresponder_message_attempt", True)
            raise

        if response_countable:
            index = stats.record_success(user)
            _print_response_summary(index, user, recipient_username, True)

    omitted_count = sum(skip_reason_counts.values())
    if debug_cycle:
        print(style_text("ðŸ” Debug ciclo (memory-first):", color=Fore.CYAN, bold=True))
        print(style_text(f"â€¢ memoria_total={memory_total}", color=Fore.WHITE))
        print(style_text(f"â€¢ discovered_ids={max(0, int(discovered_ids_count or 0))}", color=Fore.WHITE))
        print(style_text(f"â€¢ workset={len(account_threads)}", color=Fore.WHITE))
        print(style_text(f"â€¢ enviados={messages_sent_this_cycle}", color=Fore.WHITE))
        print(style_text(f"â€¢ omitidos={omitted_count}", color=Fore.WHITE))
        top_skip = sorted(
            skip_reason_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        if top_skip:
            print(style_text("â€¢ top_skip_reasons:", color=Fore.WHITE))
            for reason, count in top_skip:
                print(style_text(f"  - {reason}: {count}", color=Fore.WHITE))
        print(
            style_text(
                f"âœ… Ciclo terminado: enviados={messages_sent_this_cycle}, "
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


def _process_inbox_modern(
    client,
    user: str,
    state: Dict[str, Dict[str, str]],
    api_key: str,
    prompt_entry: Dict[str, object],
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
        prompt_entry,
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


def _process_inbox(
    client,
    user: str,
    state: Dict[str, Dict[str, str]],
    api_key: str,
    prompt_entry: Optional[Dict[str, object]] = None,
    stats: Optional[BotStats] = None,
    delay_min: float = 0.0,
    delay_max: float = 0.0,
    max_age_days: int = 7,
    allowed_thread_ids: Optional[set[str]] = None,
    threads_limit: int = 20,
    system_prompt: Optional[str] = None,
) -> None:
    if isinstance(prompt_entry, dict):
        _process_inbox_modern(
            client,
            user,
            state,
            api_key,
            prompt_entry,
            stats or BotStats(alias=ACTIVE_ALIAS or user),
            delay_min=delay_min,
            delay_max=delay_max,
            max_age_days=max_age_days,
            allowed_thread_ids=allowed_thread_ids,
            threads_limit=threads_limit,
        )
        return

    prompt_text = str(system_prompt or "").strip()
    run_stats = stats or BotStats(alias=ACTIVE_ALIAS or user)
    rows = _account_conversations_from_memory(user, refresh=True)
    if threads_limit and int(threads_limit) > 0:
        rows = rows[: max(1, int(threads_limit))]

    for row in rows:
        if STOP_EVENT.is_set():
            break
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        if allowed_thread_ids is not None and thread_id not in allowed_thread_ids:
            continue

        recipient_username = str(row.get("recipient_username") or "unknown").strip() or "unknown"
        messages = _memory_messages_from_state(row, getattr(client, "user_id", ""))
        if not messages:
            continue
        last_inbound = _latest_actionable_inbound_message(messages, getattr(client, "user_id", ""))
        if last_inbound is None:
            continue
        last_outbound = _latest_outbound_message(messages, getattr(client, "user_id", ""))
        if last_outbound is not None and not _message_is_newer_than(last_inbound, last_outbound, messages):
            continue

        thread = _thread_from_memory_state(client, row)
        if thread is None:
            continue
        opened, _reason = _open_thread_with_strategy(
            client,
            thread,
            thread_id_hint=str(row.get("thread_id_real") or thread_id),
            thread_href_hint=_normalize_thread_href(row.get("thread_href")),
            recipient_username=recipient_username,
        )
        if not opened:
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

        convo_text = _conversation_text_from_memory(messages[:40], getattr(client, "user_id", ""))
        generated = _gen_response(
            api_key=api_key,
            system_prompt=prompt_text,
            convo_text=convo_text,
            memory_context="",
        )
        can_send, _reason = _can_send_message(
            account=user,
            thread_id=thread_id,
            message_text=generated,
            force=False,
        )
        if not can_send:
            continue

        run_stats.record_reply_attempt(user)
        message_id = client.send_message(thread, generated)
        if not message_id:
            index = run_stats.record_response_error(user)
            _print_response_summary(
                index,
                user,
                recipient_username,
                False,
                extra="Autorespuesta sin confirmacion de salida",
            )
            continue
        _record_message_sent(
            user,
            thread_id,
            generated,
            str(message_id or ""),
            recipient_username,
        )
        run_stats.record_success(user)
        _print_response_summary(
            max(1, int(run_stats.responded)),
            user,
            recipient_username,
            True,
        )
        _sleep_between_replies_sync(label="reply_delay")


def _emit_autoresponder_event(event_type: str, **payload: Any) -> None:
    event_name = str(event_type or "").strip().upper()
    if not event_name:
        return
    event_payload: Dict[str, Any] = {
        "event": event_name,
        "ts": round(float(time.time()), 3),
    }
    for key, value in payload.items():
        if value is None:
            continue
        event_payload[str(key)] = value
    try:
        encoded = json.dumps(
            event_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except Exception:
        return
    logger.info("%s%s", _AUTORESPONDER_EVENT_PREFIX, encoded)


def _format_elapsed_hhmmss(seconds: float) -> str:
    total = max(0, int(seconds))
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _build_bot_summary_payload(stats: BotStats, *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    current_ts = float(now_ts if now_ts is not None else time.time())
    account_elapsed = dict(stats.account_elapsed_s)
    for account, start_ts in stats.account_started_at.items():
        account_elapsed[account] = account_elapsed.get(account, 0.0) + max(
            0.0, current_ts - float(start_ts)
        )
    accounts_used = sorted(set(stats.accounts) | set(account_elapsed.keys()))
    per_account_time: List[List[str]] = []
    for account in accounts_used:
        elapsed = account_elapsed.get(account, 0.0)
        per_account_time.append([f"@{account}", _format_elapsed_hhmmss(elapsed)])
    return {
        "alias": stats.alias,
        "accounts_used": len(accounts_used),
        "replies_attempted": int(stats.reply_attempts),
        "replies_sent": int(stats.responded),
        "followups_attempted": int(stats.followup_attempts),
        "followups_sent": int(stats.followups),
        "errors": int(stats.errors),
        "total_time": _format_elapsed_hhmmss(current_ts - stats.started_at),
        "per_account_time": per_account_time,
    }


def _print_bot_summary(stats: BotStats) -> None:
    summary = _build_bot_summary_payload(stats)
    per_account = summary.get("per_account_time") or []
    error_value = max(0, int(summary.get("errors", 0)))
    logger.info("=== BOT DETENIDO ===")
    logger.info("Alias: %s", summary.get("alias") or "-")
    logger.info("Cuentas usadas: %s", summary.get("accounts_used", 0))
    logger.info("Respuestas intentadas: %s", summary.get("replies_attempted", 0))
    logger.info("Respuestas enviadas: %s", summary.get("replies_sent", 0))
    logger.info("Follow-ups intentados: %s", summary.get("followups_attempted", 0))
    logger.info("Follow-ups enviados: %s", summary.get("followups_sent", 0))
    logger.info("Errores: %s", error_value)
    logger.info("Tiempo total: %s", summary.get("total_time") or "00:00:00")
    if per_account:
        logger.info("Tiempo por cuenta:")
        for row in per_account:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            account = str(row[0] or "").strip() or "@-"
            elapsed = str(row[1] or "").strip() or "00:00:00"
            logger.info(" - %s: %s", account, elapsed)
    if not _running_inside_gui_runtime():
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


def _autoresponder_try_open_inbox(client: object, *, force_reload: bool) -> bool:
    open_inbox_fn = getattr(client, "_open_inbox", None)
    if not callable(open_inbox_fn):
        return False
    try:
        result = open_inbox_fn(force_reload=force_reload)
        return bool(result) or result is None
    except TypeError:
        try:
            result = open_inbox_fn()
            return bool(result) or result is None
        except Exception:
            return False
    except Exception:
        return False


def _autoresponder_health_check_client(client: object) -> tuple[bool, str]:
    if _is_playwright_client_invalid(client):
        return False, "client_invalid"
    context = getattr(client, "_context", None)
    if context is None:
        return False, "context_missing"
    pages_attr = getattr(context, "pages", None)
    try:
        if callable(pages_attr):
            _ = list(pages_attr())
        elif pages_attr is not None:
            _ = list(pages_attr)
    except Exception as exc:
        return False, f"context_invalid:{exc}"
    browser = getattr(client, "_browser", None)
    if browser is not None:
        is_connected_fn = getattr(browser, "is_connected", None)
        if callable(is_connected_fn):
            try:
                if not bool(is_connected_fn()):
                    return False, "browser_disconnected"
            except Exception:
                return False, "browser_probe_failed"
    page = getattr(client, "_page", None)
    if page is None:
        return False, "page_missing"
    is_closed_fn = getattr(page, "is_closed", None)
    if callable(is_closed_fn):
        try:
            if bool(is_closed_fn()):
                return False, "page_closed"
        except Exception:
            return False, "page_closed_probe_failed"
    return True, "ok"


def _autoresponder_should_drop_account(reason: object) -> bool:
    normalized = _normalize_text_for_match(str(reason or ""))
    if not normalized:
        return False
    drop_tokens = (
        "login required",
        "login requerido",
        "session invalid",
        "session_invalid",
        "session expired",
        "storage_state_missing",
        "challenge",
        "checkpoint",
        "no disponible para operar",
        "account_not_found",
    )
    return any(token in normalized for token in drop_tokens)


def _autoresponder_recover_client(
    client_pool: Dict[str, object],
    *,
    user: str,
    alias: str,
    failure_reason: str,
) -> tuple[Optional[object], str]:
    try:
        log_browser_stage(
            component="autoresponder",
            stage="session_recovery",
            status="started",
            account=user,
            reason=failure_reason or "unknown",
        )
    except Exception:
        pass
    existing = client_pool.get(user)
    if existing is not None:
        _autoresponder_try_open_inbox(existing, force_reload=True)
        ensure_workspace_fn = getattr(existing, "_ensure_inbox_workspace_fast", None)
        if callable(ensure_workspace_fn):
            try:
                ensure_workspace_fn()
            except Exception:
                pass
        ok_health, ok_reason = _autoresponder_health_check_client(existing)
        if ok_health:
            try:
                log_browser_stage(
                    component="autoresponder",
                    stage="session_recovery",
                    status="ok",
                    account=user,
                    mode="soft",
                    detail=ok_reason or "workspace_ready",
                )
            except Exception:
                pass
            return existing, f"soft_recovered:{ok_reason}"
    _close_pooled_client(client_pool, user, reason=f"healthcheck:{failure_reason or 'unknown'}")
    session_ok = _ensure_session(user)
    if not session_ok:
        auto_relogin_raw = str(
            os.getenv("AUTORESPONDER_AUTO_RELOGIN_ON_HEALTHCHECK", "0") or "0"
        ).strip().lower()
        auto_relogin_enabled = auto_relogin_raw in {"1", "true", "yes", "y", "s", "si", "on"}
        if auto_relogin_enabled:
            relogin_ok = False
            try:
                relogin_ok = bool(_prompt_playwright_login(user, alias=alias or user))
            except Exception:
                relogin_ok = False
            if relogin_ok:
                session_ok = _ensure_session(user)
        if not session_ok:
            mark_connected(user, False)
            try:
                log_browser_stage(
                    component="autoresponder",
                    stage="session_recovery",
                    status="failed",
                    account=user,
                    detail="session_invalid",
                )
            except Exception:
                pass
            return None, "session_invalid"
    try:
        recreated = _client_for(user)
        client_pool[user] = recreated
    except Exception as exc:
        mark_connected(user, False)
        try:
            log_browser_stage(
                component="autoresponder",
                stage="session_recovery",
                status="failed",
                account=user,
                detail=f"client_recreate_failed:{exc}",
            )
        except Exception:
            pass
        return None, f"client_recreate_failed:{exc}"
    health_ok, health_reason = _autoresponder_health_check_client(recreated)
    if not health_ok:
        _close_pooled_client(client_pool, user, reason=f"post_recreate_healthcheck:{health_reason}")
        try:
            log_browser_stage(
                component="autoresponder",
                stage="session_recovery",
                status="failed",
                account=user,
                detail=f"post_recreate_healthcheck:{health_reason}",
            )
        except Exception:
            pass
        return None, f"post_recreate_healthcheck:{health_reason}"
    try:
        log_browser_stage(
            component="autoresponder",
            stage="session_recovery",
            status="ok",
            account=user,
            mode="recreated",
            detail=health_reason or "ok",
        )
    except Exception:
        pass
    return recreated, "recreated"


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
    logger.info("Cerrando sesion Playwright para @%s (motivo=%s)", account, reason)
    try:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()
    except Exception as exc:
        logger.warning("No se pudo cerrar sesion Playwright para @%s: %s", account, exc)
    finally:
        try:
            mark_connected(account, False)
        except Exception:
            pass


def _refresh_autoresponder_storage_caches() -> None:
    # La GUI puede escribir estos archivos sin pasar por los setters de responder.
    # Se refresca cache en cada activacion para evitar usar configuracion vieja.
    _read_prompts_state(refresh=True)
    _read_followup_state(refresh=True)
    _read_packs_state(refresh=True)


def _activate_bot() -> Dict[str, object]:
    global ACTIVE_ALIAS
    _refresh_autoresponder_storage_caches()
    api_key, _ = _load_preferences()
    if not api_key:
        reason = "Configura OPENAI_API_KEY antes de activar el bot."
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False}

    alias = _prompt_alias_selection()
    if not alias:
        reason = "Alias invalido."
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False}

    targets = _choose_targets(alias)
    if not targets:
        reason = "No se encontraron cuentas activas para ese alias."
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False, "alias": alias}

    active_accounts = _filter_startable_accounts(targets)
    if not active_accounts:
        reason = "No accounts are ready to run."
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False, "alias": alias}

    base_prompt_entry = _get_prompt_entry(alias)
    invalid_flow_accounts: List[str] = []
    for account_username in active_accounts:
        account_prompt_entry = _resolve_prompt_entry_for_user(
            account_username,
            active_alias=alias,
            fallback_entry=base_prompt_entry,
        )
        try:
            _resolve_flow_config_for_prompt_entry(
                account_prompt_entry,
                flow_required=FLOW_CONFIG_REQUIRED,
            )
        except FlowConfigRequiredError as exc:
            logger.error(
                "FLOW_ACTIVATION_BLOCKED_NO_FLOW alias=%s account=@%s reason=%s",
                alias,
                account_username,
                str(exc),
            )
            invalid_flow_accounts.append(account_username)
    if invalid_flow_accounts:
        reason = "Debes configurar el flujo antes de activar el autoresponder."
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False, "alias": alias}

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
    invalid_pack_accounts: List[str] = []
    invalid_pack_reasons: Dict[str, str] = {}
    for account_username in active_accounts:
        account_prompt_entry = _resolve_prompt_entry_for_user(
            account_username,
            active_alias=alias,
            fallback_entry=base_prompt_entry,
        )
        packs_ok, packs_reason = _validate_flow_pack_bindings_for_activation(
            account_prompt_entry,
            account_id=account_username,
            followup_schedule_hours=followup_schedule_hours,
        )
        if not packs_ok:
            invalid_pack_accounts.append(account_username)
            invalid_pack_reasons[account_username] = str(packs_reason or "").strip()
            logger.error(
                "FLOW_ACTIVATION_BLOCKED_PACKS alias=%s account=@%s reason=%s",
                alias,
                account_username,
                invalid_pack_reasons[account_username] or "pack_validation_failed",
            )
    if invalid_pack_accounts:
        unique_accounts = sorted(set(invalid_pack_accounts))
        first_reason = invalid_pack_reasons.get(unique_accounts[0], "").strip()
        if not first_reason:
            first_reason = "Faltan packs vÃ¡lidos para el flujo activo."
        reason = (
            "No se puede activar el autoresponder: "
            f"{first_reason} (cuentas afectadas: {', '.join('@' + item for item in unique_accounts)})"
        )
        warn(reason)
        press_enter()
        return {"status": "activation_blocked", "reason": reason, "loop_started": False, "alias": alias}
    followup_only_raw = ask("Solo seguimiento (S/N) [N]: ").strip().lower()
    followup_only = followup_only_raw in {"s", "si", "y", "yes"}
    max_age_days = 7

    ensure_logging(quiet=settings.quiet, log_dir=settings.log_dir, log_file=settings.log_file)
    reset_stop_event()
    responder_token = EngineCancellationToken("autoresponder")
    responder_binding = bind_stop_token(responder_token)
    _reset_autoresponder_runtime_controller()
    state: Dict[str, Dict[str, str]] = {}
    stats = BotStats(alias=alias)
    ACTIVE_ALIAS = alias
    listener = None
    if _q_listener_enabled_for_autoresponder():
        listener = start_q_listener(
            "Presiona Q para detener el auto-responder.",
            logger,
            token=responder_token,
        )
    else:
        logger.info("Q-listener desactivado para auto-responder (modo GUI/no TTY).")
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
    stop_reason = ""
    loop_started = False
    result_status = "stopped"
    summary_payload: Dict[str, object] = {}

    def _request_stop_with_reason(reason: str) -> None:
        nonlocal stop_reason
        normalized_reason = str(reason or "").strip()
        if normalized_reason:
            stop_reason = normalized_reason
        request_stop(normalized_reason or "auto-responder detenido")

    def _emit_progress_event(user: str, *, outcome: str, reason: str = "") -> None:
        _emit_autoresponder_event(
            "PROGRESS",
            alias=alias,
            account=user,
            outcome=str(outcome or "").strip() or "processed",
            reason=str(reason or "").strip(),
            active_accounts=len(active_accounts),
            queue_size=len(account_queue),
            replies_attempted=int(stats.reply_attempts),
            replies_sent=int(stats.responded),
            followups_attempted=int(stats.followup_attempts),
            followups_sent=int(stats.followups),
            errors=int(stats.errors),
        )

    _emit_autoresponder_event(
        "START",
        alias=alias,
        accounts_total=len(active_accounts),
        delay_min_s=float(delay_min),
        delay_max_s=float(delay_max),
        max_concurrent=int(max_concurrent),
        followup_only=bool(followup_only),
        threads_limit=int(threads_limit),
        followup_schedule_hours=list(followup_schedule_hours),
    )
    loop_started = True

    initial_sync_done: set[str] = set()
    client_pool: Dict[str, object] = {}
    last_discovered_ids_by_user: Dict[str, List[str]] = {}
    announced_accounts: set[str] = set()
    heartbeat_every_s = float(_AUTORESPONDER_HEARTBEAT_SECONDS)
    next_heartbeat_ts = time.time() + heartbeat_every_s
    try:
        with _suppress_console_noise():
            while not STOP_EVENT.is_set() and account_queue:
                now_ts = time.time()
                if now_ts >= next_heartbeat_ts:
                    _emit_autoresponder_event(
                        "HEARTBEAT",
                        alias=alias,
                        active_accounts=len(active_accounts),
                        queue_size=len(account_queue),
                        replies_attempted=int(stats.reply_attempts),
                        replies_sent=int(stats.responded),
                        followups_attempted=int(stats.followup_attempts),
                        followups_sent=int(stats.followups),
                        errors=int(stats.errors),
                    )
                    next_heartbeat_ts = now_ts + heartbeat_every_s
                batch = account_queue[:max_concurrent]
                cycle_had_activity = False
                cycle_waiting_accounts: List[tuple[str, float, str]] = []
                for user in list(batch):
                    if STOP_EVENT.is_set():
                        break
                    if user not in account_queue:
                        continue
                    runtime = _get_autoresponder_runtime_controller()
                    runtime.begin_cycle(user)
                    blocked, remaining_seconds, blocked_reason = runtime.is_account_blocked(user)
                    if blocked:
                        logger.warning(
                            "Cuenta @%s en circuit-breaker (%s) por %.1fs; salto ciclo.",
                            user,
                            blocked_reason,
                            remaining_seconds,
                        )
                        print(
                            style_text(
                                f"â¸ Cuenta @{user} en pausa ({blocked_reason}) por {round(max(0.0, remaining_seconds), 1)}s",
                                color=Fore.YELLOW,
                            )
                        )
                        cycle_waiting_accounts.append(
                            (
                                user,
                                max(0.0, float(remaining_seconds or 0.0)),
                                f"runtime:{blocked_reason or 'blocked'}",
                            )
                        )
                        _emit_progress_event(
                            user,
                            outcome="blocked",
                            reason=f"runtime:{blocked_reason or 'blocked'}",
                        )
                        continue
                    quarantined, quarantine_remaining, quarantine_reason = _safezone_quarantine_status(user)
                    if quarantined:
                        logger.warning(
                            "Cuenta @%s en cuarentena safezone (%s) por %.1fs; salto ciclo.",
                            user,
                            quarantine_reason or "unstable",
                            quarantine_remaining,
                        )
                        print(
                            style_text(
                                f"â¸ Cuenta @{user} en cuarentena safezone ({quarantine_reason or 'unstable'}) "
                                f"por {round(max(0.0, quarantine_remaining), 1)}s",
                                color=Fore.YELLOW,
                            )
                        )
                        cycle_waiting_accounts.append(
                            (
                                user,
                                max(0.0, float(quarantine_remaining or 0.0)),
                                f"safezone:{quarantine_reason or 'unstable'}",
                            )
                        )
                        _emit_progress_event(
                            user,
                            outcome="quarantined",
                            reason=f"safezone:{quarantine_reason or 'unstable'}",
                        )
                        continue

                    cycle_had_activity = True
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
                                f"â™»ï¸ Reutilizando sesiÃ³n para @{user}",
                                color=Fore.GREEN,
                            )
                        )
                    else:
                        try:
                            client = _client_for(user)
                            client_pool[user] = client
                        except Exception as exc:
                            exc_reason = f"client_init_exception:{exc}"
                            if _autoresponder_should_drop_account(exc_reason) and user in active_accounts:
                                active_accounts.remove(user)
                            _safezone_register_failure(
                                user,
                                exc_reason,
                                severe=_safezone_reason_is_severe(exc_reason),
                            )
                            _pause_autoresponder_account_for_safety(user, exc_reason)
                            stats.record_error(user)
                            _handle_account_issue(user, exc, active_accounts)
                            if user not in active_accounts:
                                _close_pooled_client(client_pool, user, reason="removed")
                                initial_sync_done.discard(user)
                                last_discovered_ids_by_user.pop(user, None)
                                if user in account_queue:
                                    account_queue.remove(user)
                            stats.mark_account_end(user)
                            _emit_progress_event(
                                user,
                                outcome="client_init_error",
                                reason=exc_reason,
                            )
                            continue

                    health_ok, health_reason = _autoresponder_health_check_client(client)
                    if not health_ok:
                        logger.warning(
                            "AUTORESPONDER_HEALTHCHECK_FAIL account=@%s reason=%s; iniciando recovery.",
                            user,
                            health_reason,
                        )
                        recovered_client, recovered_reason = _autoresponder_recover_client(
                            client_pool,
                            user=user,
                            alias=alias,
                            failure_reason=health_reason,
                        )
                        if recovered_client is None:
                            combined_reason = (
                                f"healthcheck_failed:{health_reason}|{recovered_reason or 'recover_failed'}"
                            )
                            if _autoresponder_should_drop_account(recovered_reason) and user in active_accounts:
                                active_accounts.remove(user)
                            logger.error(
                                "AUTORESPONDER_HEALTHCHECK_RECOVERY_FAIL account=@%s reason=%s",
                                user,
                                combined_reason,
                            )
                            _safezone_register_failure(user, combined_reason, severe=True)
                            _pause_autoresponder_account_for_safety(user, combined_reason)
                            stats.record_error(user)
                            stats.mark_account_end(user)
                            initial_sync_done.discard(user)
                            last_discovered_ids_by_user.pop(user, None)
                            cycle_waiting_accounts.append((user, 30.0, combined_reason))
                            _emit_progress_event(
                                user,
                                outcome="healthcheck_recovery_failed",
                                reason=combined_reason,
                            )
                            continue
                        client = recovered_client
                        if str(recovered_reason or "").strip().lower().startswith("recreated"):
                            initial_sync_done.discard(user)
                            last_discovered_ids_by_user.pop(user, None)
                        logger.info(
                            "AUTORESPONDER_HEALTHCHECK_RECOVERED account=@%s mode=%s",
                            user,
                            recovered_reason,
                        )
                    if user not in announced_accounts:
                        announced_accounts.add(user)
                        _emit_autoresponder_event(
                            "ACCOUNT_SELECTED",
                            alias=alias,
                            account=user,
                        )

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
                            account_prompt_entry = _resolve_prompt_entry_for_user(
                                user,
                                active_alias=alias,
                                fallback_entry=base_prompt_entry,
                            )
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                logger.debug(
                                    "TRACE_CYCLE ENTER decision_cycle_from_memory user=@%s ts=%s",
                                    user,
                                    time.time(),
                                )
                            decision_cycle_from_memory(
                                client,
                                user,
                                state,
                                api_key,
                                account_prompt_entry,
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
                                logger.debug(
                                    "TRACE_CYCLE EXIT decision_cycle_from_memory user=@%s ts=%s",
                                    user,
                                    time.time(),
                                )
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
                                logger.debug(
                                    "TRACE_FU ENTER followups user=@%s ts=%s",
                                    user,
                                    followup_start_ts,
                                )
                            try:
                                _process_followups_math(
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
                            except Exception as exc:
                                setattr(exc, "_autoresponder_followup_attempt", True)
                                setattr(exc, "_autoresponder_sender", user)
                                raise
                            followup_end_ts = time.time()
                            if _AUTORESPONDER_VERBOSE_TECH_LOGS:
                                logger.debug(
                                    "TRACE_FU EXIT followups user=@%s ts=%s duration_s=%s",
                                    user,
                                    followup_end_ts,
                                    round(followup_end_ts - followup_start_ts, 3),
                                )
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:  # pragma: no cover - depende de SDK/insta
                        if isinstance(exc, FlowConfigRequiredError):
                            logger.error(
                                "FLOW_REQUIRED_BUT_MISSING alias=%s account=@%s reason=%s",
                                alias,
                                user,
                                str(exc),
                            )
                            _request_stop_with_reason("Flow obligatorio no configurado")
                            raise
                        fatal_client_error = _is_fatal_playwright_runtime_error(exc)
                        exc_reason = f"activate_cycle_exception:{exc}"
                        _safezone_register_failure(
                            user,
                            exc_reason,
                            severe=_safezone_reason_is_severe(exc_reason),
                        )
                        if _looks_like_rate_or_challenge(exc):
                            _get_autoresponder_runtime_controller().mark_rate_signal(
                                user,
                                reason=str(exc),
                            )
                        _pause_autoresponder_account_for_safety(user, exc_reason)
                        if getattr(exc, "_autoresponder_followup_attempt", False):
                            stats.record_followup_error(user)
                        elif getattr(exc, "_autoresponder_message_attempt", False):
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
                        _emit_progress_event(
                            user,
                            outcome="cycle_exception",
                            reason=exc_reason,
                        )
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
                    if cycle_had_activity:
                        _sleep_cycle_delay_from_message_delay(delay_min, delay_max)
                    else:
                        positive_waits = [item[1] for item in cycle_waiting_accounts if item[1] > 0.0]
                        if positive_waits:
                            wait_s = min(60.0, max(5.0, min(positive_waits)))
                            preview = cycle_waiting_accounts[:2]
                            reason_preview = ", ".join(
                                f"@{acc}={reason}"
                                for acc, _remaining, reason in preview
                            )
                            if len(cycle_waiting_accounts) > len(preview):
                                reason_preview = f"{reason_preview}, +{len(cycle_waiting_accounts) - len(preview)} mÃ¡s"
                            print(
                                style_text(
                                    f"â³ Esperando desbloqueo de cuentas ({reason_preview}) "
                                    f"â†’ reintento en {round(wait_s, 1)}s",
                                    color=Fore.YELLOW,
                                )
                            )
                            _emit_autoresponder_event(
                                "HEARTBEAT",
                                alias=alias,
                                active_accounts=len(active_accounts),
                                queue_size=len(account_queue),
                                waiting_accounts=len(cycle_waiting_accounts),
                                wait_seconds=round(wait_s, 1),
                                replies_attempted=int(stats.reply_attempts),
                                replies_sent=int(stats.responded),
                                followups_attempted=int(stats.followup_attempts),
                                followups_sent=int(stats.followups),
                                errors=int(stats.errors),
                            )
                            sleep_with_stop(wait_s)
                        else:
                            _sleep_cycle_delay_from_message_delay(delay_min, delay_max)

        if not account_queue:
            warn("No quedan cuentas activas; el bot se detiene.")
            _request_stop_with_reason("sin cuentas activas para responder")

    except FlowConfigRequiredError as exc:
        result_status = "failed"
        logger.error(
            "FLOW_ACTIVATION_BLOCKED_NO_FLOW alias=%s reason=%s",
            alias,
            str(exc),
        )
        warn("Debes configurar el flujo antes de activar el autoresponder.")
        press_enter()
    except KeyboardInterrupt:
        _request_stop_with_reason("interrupcion con CtrlaC")
    finally:
        if not stop_reason:
            stop_reason = "auto-responder detenido"
        _request_stop_with_reason(stop_reason)
        for open_user in list(client_pool.keys()):
            _close_pooled_client(client_pool, open_user, reason="stop")
            stats.mark_account_end(open_user)
        if listener:
            listener.join(timeout=0.1)
        restore_stop_token(responder_binding)
        ACTIVE_ALIAS = None
        summary_payload = _build_bot_summary_payload(stats)
        _emit_autoresponder_event(
            "STOP",
            alias=alias,
            reason=stop_reason,
            active_accounts=len(active_accounts),
            replies_attempted=int(stats.reply_attempts),
            replies_sent=int(stats.responded),
            followups_attempted=int(stats.followup_attempts),
            followups_sent=int(stats.followups),
            errors=int(stats.errors),
        )
        _emit_autoresponder_event("SUMMARY", summary=summary_payload)
        _print_bot_summary(stats)
    return {
        "status": result_status,
        "reason": str(stop_reason or "").strip() or "auto-responder detenido",
        "loop_started": bool(loop_started),
        "alias": alias,
        "summary": dict(summary_payload),
    }

def menu_autoresponder(app_context=None):
    while True:
        _print_menu_header()
        choice = ask("OpciÃ³n: ").strip()
        if choice == "1":
            _configure_api_key()
        elif choice == "2":
            _configure_prompt()
        elif choice == "3":
            _activate_bot()
        elif choice == "4":
            _followup_menu()
        elif choice == "5":
            _packs_menu()
        elif choice == "6":
            break
        else:
            warn("OpciÃ³n invÃ¡lida.")
            press_enter()


# ------------- Extensiones para seguimiento con estado persistente ------------
import json as _json_mod_for_state
import os as _os_mod_for_state
from pathlib import Path as _Path_for_state
from typing import Dict as _Dict_for_state, List as _List_for_state, Optional as _Optional_for_state
import time as _time_for_state
from datetime import datetime as _datetime_for_state

# Determinamos la ruta del archivo de estado. Si `runtime_base` estÃ¡ disponible,
# la utilizamos para resolver un directorio consistente; de lo contrario,
# usamos el directorio actual.
try:
    _CONV_STATE_PATH = storage_root(_Path_for_state(__file__).resolve().parent.parent) / "conversation_state.json"
except Exception:
    _CONV_STATE_PATH = storage_root(_Path_for_state(__file__).resolve().parent.parent) / "conversation_state.json"

# Constantes de limpieza (en dÃ­as y segundos)
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
        atomic_write_json(_CONV_STATE_PATH, state)
    except Exception as exc:
        try:
            logger.warning("No se pudo guardar el estado de conversaciones: %s", exc)
        except Exception:
            pass

def _clean_conversation_state(state: _Dict_for_state[str, object]) -> _Dict_for_state[str, object]:
    # Elimina conversaciones antiguas del estado segÃºn las reglas de limpieza
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


def _process_followups_math(
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
    alias, entry = _followup_enabled_entry_for(user)
    if not alias or not entry or not entry.get("enabled"):
        if not _FORCE_ALWAYS_FOLLOWUP:
            return
        alias = alias or ACTIVE_ALIAS or user
        entry = _get_followup_entry(alias) if alias else {}
    prompt_config = _resolve_prompt_entry_for_user(
        user,
        active_alias=alias,
        fallback_entry=_get_prompt_entry(alias or user),
    )
    flow_config = _resolve_flow_config_for_prompt_entry(
        prompt_config,
        followup_schedule_hours=followup_schedule_hours,
    )
    flow_engine = FlowEngine(flow_config)
    prompt_text = _DEFAULT_FOLLOWUP_STRATEGY_PROMPT
    now_ts = time.time()
    max_age_seconds = max(0, int(max_age_days)) * 24 * 3600 if max_age_days is not None else 0
    memory_threads = _account_conversations_from_memory(user, refresh=True)
    if threads_limit and int(threads_limit) > 0:
        memory_threads = memory_threads[: max(1, int(threads_limit))]
    fu_processed = 0
    fu_sent = 0
    fu_waiting = 0
    fu_omitted = 0
    history_source = entry.get("history")
    history: _Dict_for_state[str, dict] = dict(history_source) if isinstance(history_source, dict) else {}
    updated_history = False
    account_norm = _normalize_username(user)
    for row in memory_threads:
        if STOP_EVENT.is_set():
            break
        fu_processed += 1
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        recipient_username = str(row.get("recipient_username") or "").strip() or "unknown"
        recipient_id = str(row.get("recipient_id") or "").strip() or recipient_username
        messages = _memory_messages_from_state(row, getattr(client, "user_id", ""))
        if not messages:
            fu_omitted += 1
            continue
        all_ts = [_safe_float(getattr(msg, "timestamp", None)) for msg in messages]
        all_ts = [value for value in all_ts if value is not None]
        if not all_ts:
            fu_omitted += 1
            continue
        if max_age_seconds and (now_ts - max(all_ts)) > max_age_seconds:
            fu_omitted += 1
            continue
        outbound_ts = [
            _safe_float(getattr(msg, "timestamp", None))
            for msg in messages
            if _message_outbound_status(msg, client.user_id) is True
        ]
        outbound_ts = [value for value in outbound_ts if value is not None]
        if not outbound_ts:
            fu_omitted += 1
            continue
        inbound_ts = [
            _safe_float(getattr(msg, "timestamp", None))
            for msg in messages
            if _message_outbound_status(msg, client.user_id) is False
        ]
        inbound_ts = [value for value in inbound_ts if value is not None]
        if inbound_ts and max(inbound_ts) > max(outbound_ts):
            fu_omitted += 1
            continue
        if now_ts - max(outbound_ts) < 60:
            fu_waiting += 1
            continue
        conv_state = _get_conversation_state(user, thread_id)
        if bool(conv_state.get("pending_reply", False)):
            fu_omitted += 1
            continue
        if (
            str(conv_state.get("stage") or "").strip().lower() == _STAGE_CLOSED
            or bool(conv_state.get("prompt_sequence_done", False))
        ):
            fu_omitted += 1
            continue
        flow_state = _ensure_flow_state_for_thread(
            user,
            thread_id,
            recipient_username=recipient_username,
            conv_state=conv_state,
            flow_config=flow_config,
        )
        decision = flow_engine.evaluate(
            {
                "flow_state": flow_state,
                "inbound_text": "",
                "latest_inbound_id": "",
                "last_inbound_id_seen": str(conv_state.get("last_inbound_id_seen") or "").strip(),
                "pending_reply": bool(conv_state.get("pending_reply")),
                "pending_inbound_id": str(conv_state.get("pending_inbound_id") or "").strip(),
                "last_outbound_ts": _safe_float(conv_state.get("last_message_sent_at")) or max(outbound_ts),
                "followup_level": _safe_int(flow_state.get("followup_level")),
                "now_ts": now_ts,
                "objection_strategy_name": str(prompt_config.get("objection_strategy_name") or "").strip(),
            }
        )
        _log_flow_decision_and_state(
            thread_id,
            decision=decision,
            flow_state=flow_state,
            pending=bool(conv_state.get("pending_pack_run")),
        )
        if str(decision.get("decision") or "").strip().lower() == "wait":
            fu_waiting += 1
            continue
        if str(decision.get("decision") or "").strip().lower() != "followup":
            fu_omitted += 1
            continue
        action_type = str(decision.get("action_type") or "").strip()
        if not action_type or _is_no_send_strategy(action_type):
            fu_omitted += 1
            continue
        followup_stage_int = max(1, _safe_int(flow_state.get("followup_level")) + 1)
        if fu_sent > 0:
            _sleep_between_replies_for_account(user, delay_min, delay_max, label="followup_delay")
        row_thread_id_real = str(row.get("thread_id_real") or "").strip()
        row_thread_href = _normalize_thread_href(row.get("thread_href"))
        href_thread_id = _extract_thread_id_from_href(row_thread_href)
        if _is_probably_web_thread_id(href_thread_id):
            row_thread_id_real = href_thread_id
        if not _is_probably_web_thread_id(row_thread_id_real) and _is_probably_web_thread_id(thread_id):
            row_thread_id_real = thread_id
        thread_state = dict(row)
        if row_thread_id_real:
            thread_state["thread_id"] = row_thread_id_real
            thread_state["thread_id_real"] = row_thread_id_real
        if row_thread_href:
            thread_state["thread_href"] = row_thread_href
        thread = _thread_from_memory_state(client, thread_state)
        if thread is None:
            fu_omitted += 1
            continue
        if stats is not None:
            stats.record_followup_attempt(user)
        opened, open_reason = _open_thread_with_strategy(
            client,
            thread,
            thread_id_hint=row_thread_id_real or thread_id,
            thread_href_hint=row_thread_href,
            recipient_username=recipient_username,
        )
        if not opened:
            logger.warning(
                "FOLLOWUP_OPEN_THREAD_FAILED account=@%s thread=%s recipient=@%s reason=%s",
                user,
                thread_id,
                recipient_username,
                open_reason,
            )
            if stats is not None:
                stats.record_followup_error(user)
            fu_omitted += 1
            continue
        sent_ok = False
        if _canonical_flow_action_type(action_type, allow_empty=True) in {"auto_reply", "followup_text"}:
            convo_text = _conversation_text_from_memory(messages[:40], getattr(client, "user_id", ""))
            generated = _generate_autoreply_response(
                "(sin respuesta del lead aun)",
                prompt_text,
                api_key=api_key,
                conversation_text=convo_text,
                account_memory=_get_account_memory(user),
            )
            if generated:
                msg_id = _send_text_action_strict(client, thread, generated)
                if msg_id:
                    _record_message_sent(
                        user,
                        thread_id,
                        generated,
                        str(msg_id),
                        recipient_username,
                        is_followup=True,
                        followup_stage=followup_stage_int,
                    )
                    sent_ok = True
        else:
            selected_pack = select_pack(action_type, user)
            if selected_pack:
                _emit_pack_selected_event(
                    account=user,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    pack=selected_pack,
                    fallback_type=action_type,
                )
                convo_text = _conversation_text_from_memory(messages[:40], getattr(client, "user_id", ""))
                pack_result = execute_pack(
                    selected_pack,
                    user,
                    _get_account_memory(user),
                    client=client,
                    thread=thread,
                    thread_id=thread_id,
                    recipient_username=recipient_username,
                    api_key=api_key,
                    conversation_text=convo_text,
                    strategy_name=action_type,
                    latest_inbound_id=None,
                    is_followup=True,
                    followup_stage=followup_stage_int,
                    stage_id=str(flow_state.get("stage_id") or decision.get("stage_id") or "").strip(),
                    stage_anchor_ts=(
                        _safe_float(flow_state.get("followup_anchor_ts"))
                        or _safe_float(flow_state.get("last_outbound_ts"))
                    ),
                    pending_pack_run=None,
                    persist_pending=False,
                    flow_config=flow_config,
                )
                sent_ok = bool(pack_result.get("completed", False))
        if not sent_ok:
            if stats is not None:
                stats.record_followup_error(user)
            fu_omitted += 1
            continue
        sent_ts = time.time()
        latest_conv_for_flow = _get_conversation_state(user, thread_id)
        latest_flow_for_update = _normalize_flow_state(
            latest_conv_for_flow.get("flow_state"),
            fallback_stage_id=str(flow_state.get("stage_id") or ""),
            last_outbound_ts=_safe_float(latest_conv_for_flow.get("last_message_sent_at")),
            followup_level_hint=_safe_int(latest_conv_for_flow.get("followup_stage")),
        )
        updated_flow_state = flow_engine.apply_outbound(latest_flow_for_update, decision, sent_at=sent_ts)
        _update_conversation_state(
            user,
            thread_id,
            {
                "flow_state": updated_flow_state,
                "followup_stage": _safe_int(updated_flow_state.get("followup_level")),
                "last_followup_sent_at": sent_ts,
                "stage": _STAGE_FOLLOWUP,
                "pending_reply": False,
                "pending_inbound_id": None,
            },
            recipient_username=recipient_username,
        )
        _clear_pending_hydration(user, thread_id, recipient_username=recipient_username)
        fu_sent += 1
        if stats is not None:
            stats.record_followup_success(user)
        conv_key = f"{account_norm}|{thread_id}"
        record = history.get(conv_key, {})
        record["count"] = followup_stage_int
        record["last_sent_ts"] = sent_ts
        record["last_eval_ts"] = now_ts
        history[conv_key] = record
        updated_history = True
        _append_message_log(
            {
                "action": "followup_sent",
                "account": user,
                "thread_id": thread_id,
                "lead": recipient_username or str(recipient_id),
                "followup_stage": followup_stage_int,
                "strategy_name": action_type,
            }
        )
    if updated_history and alias:
        _set_followup_entry(alias, {"history": history})
    logger.info(
        "Resumen follow-up | procesados=%s enviados=%s en_espera=%s omitidos=%s",
        fu_processed,
        fu_sent,
        fu_waiting,
        fu_omitted,
    )


def _process_followups(
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
    _ = followup_schedule_hours  # compatibilidad de firma
    alias, entry = _followup_enabled_entry_for(user)
    if not alias or not entry or not entry.get("enabled"):
        return

    prompt_text = str(entry.get("prompt") or "").strip()
    run_stats = stats or BotStats(alias=alias)
    sent_count = 0
    rows = _account_conversations_from_memory(user, refresh=True)
    if threads_limit and int(threads_limit) > 0:
        rows = rows[: max(1, int(threads_limit))]
    now_ts = time.time()
    max_age_seconds = max(0, int(max_age_days)) * 24 * 3600 if max_age_days is not None else 0

    for row in rows:
        if STOP_EVENT.is_set():
            break
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            continue
        recipient_username = str(row.get("recipient_username") or "unknown").strip() or "unknown"
        messages = _memory_messages_from_state(row, getattr(client, "user_id", ""))
        if max_age_seconds and messages:
            timestamps = [_safe_float(getattr(msg, "timestamp", None)) for msg in messages]
            timestamps = [value for value in timestamps if value is not None]
            if timestamps and (now_ts - max(timestamps)) > max_age_seconds:
                continue

        outbound = _latest_outbound_message(messages, getattr(client, "user_id", ""))
        outbound_ts = _safe_float(getattr(outbound, "timestamp", None)) if outbound is not None else None
        if outbound is None or outbound_ts is None:
            _append_message_log(
                {
                    "action": "followup_skip",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username,
                    "reason": "skip_no_outbound_messages",
                }
            )
            continue
        if now_ts - outbound_ts < 60:
            _append_message_log(
                {
                    "action": "followup_skip",
                    "account": user,
                    "thread_id": thread_id,
                    "lead": recipient_username,
                    "reason": "skip_last_outbound_lt_60s_or_fallback_suspected",
                }
            )
            continue

        conversation_text = _conversation_text_from_memory(messages[:40], getattr(client, "user_id", ""))
        next_stage = max(1, _safe_int((row.get("followup_stage") or 0)) + 1)
        generated, stage_value = _followup_decision(
            api_key=api_key,
            prompt_text=prompt_text,
            conversation=conversation_text,
            metadata={
                "intento_followup_siguiente": next_stage,
                "horas_objetivo": 0,
            },
        )
        if not generated.strip():
            continue

        thread = _thread_from_memory_state(client, row)
        if thread is None:
            continue
        run_stats.record_followup_attempt(user)
        if sent_count > 0:
            _sleep_between_replies_sync(label="followup_delay")
        message_id = client.send_message(thread, generated)
        if not message_id:
            run_stats.record_followup_error(user)
            continue
        _record_message_sent(
            user,
            thread_id,
            generated,
            str(message_id or ""),
            recipient_username,
            is_followup=True,
            followup_stage=stage_value,
        )
        sent_count += 1
        run_stats.record_followup_success(user)






