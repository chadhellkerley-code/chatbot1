from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

from config import SETTINGS, read_env_local

logger = logging.getLogger(__name__)

_OPENAI_DEFAULT_MODEL = "gpt-4o-mini"


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "si", "on"}


_AUTORESPONDER_VERBOSE_TECH_LOGS = _env_enabled("AUTORESPONDER_VERBOSE_TECH_LOGS", False)


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


def _resolve_ai_timeout_seconds(*, env_values: Optional[Dict[str, str]] = None) -> float:
    values = env_values or read_env_local()
    raw_timeout = (
        _read_env_value(values, "AUTORESPONDER_AI_TIMEOUT_SECONDS")
        or _read_env_value(values, "OPENAI_TIMEOUT_SECONDS")
    )
    try:
        timeout_s = float(raw_timeout) if raw_timeout else 35.0
    except Exception:
        timeout_s = 35.0
    return max(5.0, min(180.0, timeout_s))


def _resolve_ai_max_retries(*, env_values: Optional[Dict[str, str]] = None) -> int:
    values = env_values or read_env_local()
    raw_retries = (
        _read_env_value(values, "AUTORESPONDER_AI_MAX_RETRIES")
        or _read_env_value(values, "OPENAI_MAX_RETRIES")
    )
    try:
        retries = int(raw_retries) if raw_retries else 1
    except Exception:
        retries = 1
    return max(0, min(5, retries))


def _resolve_ai_runtime(api_key: str) -> tuple[str, str]:
    values = read_env_local()
    model = _resolve_ai_model(api_key, env_values=values)
    return "OpenAI", model


def _build_openai_client(api_key: str) -> object:
    from openai import OpenAI

    values = read_env_local()
    base_url = _resolve_ai_base_url(api_key, env_values=values)
    kwargs: Dict[str, object] = {
        "api_key": api_key,
        "timeout": _resolve_ai_timeout_seconds(env_values=values),
        "max_retries": _resolve_ai_max_retries(env_values=values),
    }
    if base_url:
        kwargs["base_url"] = base_url
    try:
        return OpenAI(**kwargs)
    except TypeError:
        # Compatibilidad con versiones del SDK que no exponen timeout/max_retries en ctor.
        kwargs.pop("timeout", None)
        kwargs.pop("max_retries", None)
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
    request_timeout_seconds: Optional[float] = None,
) -> str:
    timeout_seconds = _resolve_ai_timeout_seconds()
    if request_timeout_seconds is not None:
        try:
            timeout_seconds = float(request_timeout_seconds)
        except Exception:
            timeout_seconds = _resolve_ai_timeout_seconds()
    timeout_seconds = max(5.0, min(180.0, timeout_seconds))
    request_kwargs: Dict[str, object] = {"timeout": timeout_seconds}

    responses_api = getattr(client, "responses", None)
    if responses_api is not None and hasattr(responses_api, "create"):
        try:
            try:
                response = responses_api.create(
                    model=model,
                    input=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content},
                    ],
                    temperature=temperature,
                    max_output_tokens=max_output_tokens,
                    **request_kwargs,
                )
            except TypeError:
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

    try:
        completion = completions_api.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=temperature,
            max_tokens=max_output_tokens,
            **request_kwargs,
        )
    except TypeError:
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

