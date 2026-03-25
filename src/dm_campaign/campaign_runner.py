from __future__ import annotations

import time
from typing import Any, Callable, Dict, MutableMapping

from .proxy_workers_runner import run_dynamic_campaign


def start_campaign(
    config: MutableMapping[str, Any],
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = dict(config or {})
    alias = str(payload.get("alias") or "default").strip() or "default"
    leads_alias = str(payload.get("leads_alias") or alias).strip() or alias
    delay_min = _as_int(payload.get("delay_min", 10), default=10)
    delay_max = max(delay_min, _as_int(payload.get("delay_max", max(delay_min, 20)), default=max(delay_min, 20)))
    requested_workers = _as_int(payload.get("workers_requested", 1), default=1)

    print("[INFO] Iniciando campana de envio de mensajes")
    print("")
    print(f"Alias seleccionado: {alias}")
    print(f"Alias de leads: {leads_alias}")
    print(f"Workers solicitados: {requested_workers}")
    print(f"Delay configurado: {delay_min}s - {delay_max}s")
    print(f"Hora de inicio: {time.strftime('%H:%M:%S')}")

    result = run_dynamic_campaign(payload, progress_callback=progress_callback)
    return result


def _as_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)
