# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from paths import runtime_base

_ROOT = runtime_base(Path(__file__).resolve().parent)
_TEMPLATES_PATH = _ROOT / "storage" / "templates.json"
_STATE_PATH = _ROOT / "storage" / "templates_state.json"


def _normalize_item(item: Dict[str, str]) -> Dict[str, str]:
    name = str(item.get("name") or "").strip()
    text = str(item.get("text") or "").strip()
    return {"name": name, "text": text}


def load_templates() -> List[Dict[str, str]]:
    if not _TEMPLATES_PATH.exists():
        return []
    try:
        data = json.loads(_TEMPLATES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    items: List[Dict[str, str]] = []
    for raw in data:
        if isinstance(raw, dict):
            item = _normalize_item(raw)
            if item["name"] and item["text"]:
                items.append(item)
    return items


def save_templates(items: List[Dict[str, str]]) -> None:
    normalized: List[Dict[str, str]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        item = _normalize_item(raw)
        if item["name"] and item["text"]:
            normalized.append(item)
    _TEMPLATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    _TEMPLATES_PATH.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_template_state() -> Dict[str, int]:
    if not _STATE_PATH.exists():
        return {}
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    state: Dict[str, int] = {}
    for key, value in data.items():
        try:
            state[str(key)] = int(value)
        except Exception:
            continue
    return state


def save_template_state(state: Dict[str, int]) -> None:
    clean: Dict[str, int] = {}
    for key, value in state.items():
        try:
            clean[str(key)] = int(value)
        except Exception:
            continue
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(
        json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def next_round_robin(
    account: str, template_id: str, candidates: List[str]
) -> tuple[str, int]:
    if not candidates:
        return "", -1
    state = load_template_state()
    key = f"{account}:{template_id}"
    idx = int(state.get(key, -1)) + 1
    if idx >= len(candidates):
        idx = 0
    state[key] = idx
    save_template_state(state)
    return candidates[idx], idx


def render_template(text: str, variables: Dict[str, str]) -> str:
    result = text or ""
    for key, value in variables.items():
        token = "{" + key + "}"
        result = result.replace(token, value or "")
    return result
