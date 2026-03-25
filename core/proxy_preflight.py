from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from core.proxy_registry import proxy_health_label, proxy_reference_status


BLOCKING_PROXY_STATUSES = frozenset({"inactive", "missing", "quarantined"})
DIRECT_NETWORK_KEY = "direct"
PROXY_NETWORK_KEY_PREFIX = "proxy:"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def effective_network_key(proxy_id: Any) -> str:
    clean_proxy_id = _clean(proxy_id)
    if not clean_proxy_id:
        return DIRECT_NETWORK_KEY
    return f"{PROXY_NETWORK_KEY_PREFIX}{clean_proxy_id.lower()}"


def account_proxy_preflight(
    account: dict[str, Any] | None,
    *,
    path: Path | None = None,
    allow_proxyless: bool = True,
    allow_legacy: bool = True,
) -> dict[str, Any]:
    account_data = dict(account or {})
    username = _clean(account_data.get("username")).lstrip("@")
    alias = _clean(account_data.get("alias")) or "default"
    assigned_proxy_id = _clean(account_data.get("assigned_proxy_id"))
    proxy_url = _clean(account_data.get("proxy_url"))

    if assigned_proxy_id:
        state = proxy_reference_status(assigned_proxy_id, path=path)
        status = _clean(state.get("status")) or "missing"
        record = state.get("record") if isinstance(state.get("record"), dict) else None
        message = _clean(state.get("message"))
        if not message and status == "inactive":
            message = f"El proxy asignado {assigned_proxy_id} esta inactivo."
        elif not message and status == "missing":
            message = f"El proxy asignado {assigned_proxy_id} no existe."
        elif not message and status == "quarantined":
            message = f"El proxy asignado {assigned_proxy_id} esta en cuarentena."
        return {
            "username": username,
            "alias": alias,
            "status": status,
            "network_mode": "proxy",
            "effective_network_key": effective_network_key(assigned_proxy_id),
            "proxy_id": assigned_proxy_id,
            "proxy_label": assigned_proxy_id,
            "message": message,
            "blocking": status in BLOCKING_PROXY_STATUSES,
            "health_label": proxy_health_label(record) if record else "",
            "record": record,
        }

    if proxy_url:
        message = ""
        if not allow_legacy:
            message = "La cuenta usa un proxy directo legacy no permitido para esta ejecucion."
        return {
            "username": username,
            "alias": alias,
            "status": "legacy",
            "network_mode": "legacy",
            "effective_network_key": "",
            "proxy_id": "",
            "proxy_label": proxy_url,
            "message": message,
            "blocking": not allow_legacy,
            "health_label": "",
            "record": None,
        }

    message = ""
    if not allow_proxyless:
        message = "La cuenta no tiene proxy asignado."
    return {
        "username": username,
        "alias": alias,
        "status": "none",
        "network_mode": "direct",
        "effective_network_key": DIRECT_NETWORK_KEY,
        "proxy_id": "",
        "proxy_label": DIRECT_NETWORK_KEY,
        "message": message,
        "blocking": not allow_proxyless,
        "health_label": "",
        "record": None,
    }


def preflight_accounts_for_proxy_runtime(
    accounts: Iterable[dict[str, Any] | None],
    *,
    path: Path | None = None,
    allow_proxyless: bool = True,
    allow_legacy: bool = True,
) -> dict[str, Any]:
    ready_accounts: list[dict[str, Any]] = []
    blocked_accounts: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    blocked_status_counts: Counter[str] = Counter()

    for item in accounts:
        if not isinstance(item, dict):
            continue
        current = dict(item)
        status = account_proxy_preflight(
            current,
            path=path,
            allow_proxyless=allow_proxyless,
            allow_legacy=allow_legacy,
        )
        normalized_status = _clean(status.get("status")) or "unknown"
        status_counts[normalized_status] += 1
        if bool(status.get("blocking")):
            blocked_accounts.append(status)
            blocked_status_counts[normalized_status] += 1
            continue
        current["effective_network_key"] = _clean(status.get("effective_network_key"))
        current["network_mode"] = _clean(status.get("network_mode"))
        current["proxy_preflight_status"] = normalized_status
        current["proxy_preflight_message"] = _clean(status.get("message"))
        ready_accounts.append(current)

    return {
        "ready_accounts": ready_accounts,
        "blocked_accounts": blocked_accounts,
        "ready": len(ready_accounts),
        "blocked": len(blocked_accounts),
        "status_counts": dict(status_counts),
        "blocked_status_counts": dict(blocked_status_counts),
    }
