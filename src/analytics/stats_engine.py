# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from paths import runtime_base
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn

try:
    from storage import TZ
except Exception:  # pragma: no cover - fallback si storage falla
    TZ = timezone.utc

ROLE_GREETING = "greeting"
ROLE_PITCH = "pitch"
ROLE_CTA = "cta"
ROLE_FOLLOWUP = "followup"

ROLE_LABELS = {
    ROLE_GREETING: "Saludos",
    ROLE_PITCH: "Pitch",
    ROLE_CTA: "CTA / Agenda",
    ROLE_FOLLOWUP: "Follow-ups",
}

ROLE_FUNNEL_LABELS = {
    ROLE_GREETING: "% respuesta",
    ROLE_PITCH: "% continuidad",
    ROLE_CTA: "% conversión",
    ROLE_FOLLOWUP: "% reactivación",
}

TIME_BUCKETS: List[Tuple[str, int, int]] = [
    ("08-12", 8, 12),
    ("12-16", 12, 16),
    ("16-20", 16, 20),
    ("20-00", 20, 24),
]


def _base_dir() -> Path:
    return runtime_base(Path(__file__).resolve().parents[2])


def _engine_path() -> Path:
    return _base_dir() / "storage" / "conversation_engine.json"


def _exports_dir() -> Path:
    path = _base_dir() / "exports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_ts(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        ts = float(value)
        if ts <= 0:
            return None
        return ts
    except (TypeError, ValueError):
        return None


def _to_local(ts: float) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(TZ)
    except Exception:
        return None


def _role_by_index(idx: int) -> str:
    if idx == 0:
        return ROLE_GREETING
    if idx == 1:
        return ROLE_PITCH
    if idx == 2:
        return ROLE_CTA
    return ROLE_FOLLOWUP


def _format_percent(numerator: int, denominator: int, digits: int = 1) -> str:
    if denominator <= 0:
        return "0.0%"
    value = (numerator / denominator) * 100.0
    return f"{value:.{digits}f}%"


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "N/A"
    total = int(round(seconds))
    if total < 60:
        return f"{total}s"
    minutes = total // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    mins = minutes % 60
    if hours >= 24:
        days = hours // 24
        hrs = hours % 24
        parts = [f"{days}d"]
        if hrs:
            parts.append(f"{hrs}h")
        if mins:
            parts.append(f"{mins}m")
        return " ".join(parts)
    return f"{hours}h {mins}m"


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split()).lower()


def _bucket_for_ts(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    dt = _to_local(ts)
    if dt is None:
        return None
    hour = dt.hour
    for label, start, end in TIME_BUCKETS:
        if start <= hour < end:
            return label
    return None


def _distribution(counter: Counter) -> List[Tuple[str, str]]:
    total = sum(counter.values())
    if total <= 0:
        return []
    rows: List[Tuple[str, str]] = []
    for label, _, _ in TIME_BUCKETS:
        count = int(counter.get(label, 0))
        rows.append((label, _format_percent(count, total, digits=0)))
    return rows


def _load_engine() -> Dict[str, Any]:
    path = _engine_path()
    if not path.exists():
        return {"conversations": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"conversations": {}}
    if not isinstance(data, dict):
        return {"conversations": {}}
    data.setdefault("conversations", {})
    return data


def _split_key(key: str) -> Tuple[str, str]:
    if "|" in key:
        account, thread_id = key.split("|", 1)
        return account, thread_id
    return key, key


def _iter_message_occurrences(engine: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    conversations = engine.get("conversations", {}) or {}
    for key, thread in conversations.items():
        if not isinstance(thread, dict):
            continue
        account_alias, thread_id = _split_key(str(key))
        account = str(thread.get("account") or account_alias or "").strip()
        thread_id_val = str(thread.get("thread_id") or thread_id or "").strip()

        messages = thread.get("messages_sent", []) or []
        if not isinstance(messages, list):
            continue

        items: List[Dict[str, Any]] = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            text = str(msg.get("text") or "").strip()
            if not text:
                continue
            first_ts = _safe_ts(msg.get("first_sent_at"))
            last_ts = _safe_ts(msg.get("last_sent_at"))
            sent_ts = first_ts or last_ts
            if sent_ts is None:
                continue
            times_sent = int(msg.get("times_sent") or 1)
            items.append(
                {
                    "text": text,
                    "sent_ts": sent_ts,
                    "times_sent": max(times_sent, 1),
                }
            )

        if not items:
            continue

        items.sort(key=lambda item: item["sent_ts"])

        last_received_at = _safe_ts(thread.get("last_message_received_at"))

        for idx, item in enumerate(items):
            role = _role_by_index(idx)
            next_ts = items[idx + 1]["sent_ts"] if idx + 1 < len(items) else None
            response_ts = None
            if last_received_at and last_received_at > item["sent_ts"]:
                if next_ts is None or last_received_at < next_ts:
                    response_ts = last_received_at
            response_time = (
                float(response_ts - item["sent_ts"]) if response_ts is not None else None
            )
            yield {
                "account_alias": account or account_alias,
                "thread_id": thread_id_val or thread_id,
                "message_role": role,
                "message_text": item["text"],
                "sent_count": item["times_sent"],
                "sent_ts": item["sent_ts"],
                "response_ts": response_ts,
                "response_time": response_time,
            }


def _empty_role_metrics() -> Dict[str, Any]:
    return {
        "sent": 0,
        "responded": 0,
        "response_times": [],
        "time_ranges": Counter(),
        "messages": {},
    }


def _build_report() -> Dict[str, Any]:
    engine = _load_engine()
    roles: Dict[str, Any] = {
        ROLE_GREETING: _empty_role_metrics(),
        ROLE_PITCH: _empty_role_metrics(),
        ROLE_CTA: _empty_role_metrics(),
        ROLE_FOLLOWUP: _empty_role_metrics(),
    }
    occurrences: List[Dict[str, Any]] = []
    min_ts: Optional[float] = None
    max_ts: Optional[float] = None

    for occ in _iter_message_occurrences(engine):
        role = occ["message_role"]
        role_metrics = roles[role]

        sent_count = int(occ["sent_count"])
        responded = 1 if occ.get("response_ts") is not None else 0
        response_time = occ.get("response_time")

        role_metrics["sent"] += sent_count
        role_metrics["responded"] += responded
        if response_time is not None:
            role_metrics["response_times"].append(response_time)

        bucket = _bucket_for_ts(occ.get("response_ts"))
        if bucket:
            role_metrics["time_ranges"][bucket] += 1

        normalized = _normalize_text(occ["message_text"])
        messages = role_metrics["messages"]
        msg_metrics = messages.get(normalized)
        if msg_metrics is None:
            msg_metrics = {
                "text": occ["message_text"],
                "sent": 0,
                "responded": 0,
                "response_times": [],
                "time_ranges": Counter(),
            }
            messages[normalized] = msg_metrics

        msg_metrics["sent"] += sent_count
        msg_metrics["responded"] += responded
        if response_time is not None:
            msg_metrics["response_times"].append(response_time)
        if bucket:
            msg_metrics["time_ranges"][bucket] += 1

        sent_ts = occ.get("sent_ts")
        if sent_ts is not None:
            min_ts = sent_ts if min_ts is None else min(min_ts, sent_ts)
            max_ts = sent_ts if max_ts is None else max(max_ts, sent_ts)

        occurrences.append(occ)

    return {
        "roles": roles,
        "occurrences": occurrences,
        "start_ts": min_ts,
        "end_ts": max_ts,
    }


def _avg(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _render_distribution(counter: Counter) -> None:
    rows = _distribution(counter)
    if not rows:
        print("(Sin datos)")
        return
    for label, pct in rows:
        print(f"{label} -> {pct}")


def _render_category(role: str, report: Dict[str, Any]) -> None:
    role_metrics = report["roles"][role]
    banner()
    print(style_text(ROLE_LABELS[role].upper(), color=Fore.CYAN, bold=True))
    print(full_line())
    print(style_text(f"Mensajes distintos: {len(role_metrics['messages'])}"))
    print(style_text(f"Total enviados: {role_metrics['sent']}"))
    print(style_text(f"Total respondidos: {role_metrics['responded']}"))
    print(style_text(f"% respuesta: {_format_percent(role_metrics['responded'], role_metrics['sent'])}"))
    avg = _avg(role_metrics["response_times"])
    print(style_text(f"Tiempo promedio respuesta: {_format_duration(avg)}"))
    print()
    print("Distribución por horario:")
    _render_distribution(role_metrics["time_ranges"])
    print()
    print("Opciones:")
    print("1) Ver detalle por mensaje")
    print("2) Volver")
    print()
    choice = ask("Opción: ").strip()
    if choice == "1":
        _render_message_detail(role, report)


def _sorted_messages(role_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    messages = list(role_metrics["messages"].values())
    messages.sort(key=lambda item: item["sent"], reverse=True)
    return messages


def _render_message_detail(role: str, report: Dict[str, Any]) -> None:
    role_metrics = report["roles"][role]
    messages = _sorted_messages(role_metrics)
    banner()
    print(style_text(f"DETALLE DE {ROLE_LABELS[role].upper()}", color=Fore.CYAN, bold=True))
    print(full_line())
    if not messages:
        print("(Sin mensajes)")
        press_enter()
        return
    for idx, msg in enumerate(messages, start=1):
        avg = _avg(msg["response_times"])
        print(f"[{idx}]")
        print("Texto:")
        print(f"\"{msg['text']}\"")
        print(f"Enviados: {msg['sent']}")
        print(f"Respondidos: {msg['responded']}")
        print(f"% respuesta: {_format_percent(msg['responded'], msg['sent'])}")
        print(f"Velocidad promedio: {_format_duration(avg)}")
        print()
    print("Opciones:")
    print("1) Ver mensaje individual")
    print("2) Volver")
    print()
    choice = ask("Opción: ").strip()
    if choice == "1":
        idx_raw = ask("Número de mensaje: ").strip()
        try:
            idx = int(idx_raw)
        except Exception:
            warn("Número inválido.")
            press_enter()
            return
        if idx < 1 or idx > len(messages):
            warn("Número fuera de rango.")
            press_enter()
            return
        _render_message_individual(messages[idx - 1])


def _render_message_individual(message: Dict[str, Any]) -> None:
    banner()
    print(style_text("MENSAJE INDIVIDUAL", color=Fore.CYAN, bold=True))
    print(full_line())
    print("Texto:")
    print(f"\"{message['text']}\"")
    print()
    print(f"Enviados: {message['sent']}")
    print(f"Respondidos: {message['responded']}")
    print(f"% respuesta: {_format_percent(message['responded'], message['sent'])}")
    print()
    print("Tiempo respuesta:")
    avg = _avg(message["response_times"])
    min_time = min(message["response_times"]) if message["response_times"] else None
    max_time = max(message["response_times"]) if message["response_times"] else None
    print(f"- Promedio: {_format_duration(avg)}")
    print(f"- Mínimo: {_format_duration(min_time)}")
    print(f"- Máximo: {_format_duration(max_time)}")
    print()
    print("Respuestas por horario:")
    _render_distribution(message["time_ranges"])
    print()
    print("Opciones:")
    print("1) Volver")
    print()
    ask("Opción: ")


def _render_funnel(report: Dict[str, Any]) -> None:
    banner()
    print(style_text("FUNNEL GENERAL", color=Fore.CYAN, bold=True))
    print(full_line())
    for role in (ROLE_GREETING, ROLE_PITCH, ROLE_CTA, ROLE_FOLLOWUP):
        metrics = report["roles"][role]
        print(f"{ROLE_LABELS[role]}:")
        print(f"  Enviados: {metrics['sent']}")
        print(f"  {ROLE_FUNNEL_LABELS[role]}: {_format_percent(metrics['responded'], metrics['sent'])}")
        print()
    press_enter()


def _export_csv(report: Dict[str, Any]) -> None:
    occurrences = report.get("occurrences") or []
    if not occurrences:
        warn("No hay datos para exportar.")
        press_enter()
        return
    start_ts = report.get("start_ts")
    end_ts = report.get("end_ts")
    start_dt = _to_local(start_ts) if start_ts else None
    end_dt = _to_local(end_ts) if end_ts else None
    start_label = start_dt.strftime("%Y%m%d") if start_dt else "sin_fecha"
    end_label = end_dt.strftime("%Y%m%d") if end_dt else "sin_fecha"
    path = _exports_dir() / f"stats_{start_label}_{end_label}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "account_alias",
                "thread_id",
                "message_role",
                "message_text",
                "sent_count",
                "response_count",
                "response_percentage",
                "avg_response_time",
                "min_response_time",
                "max_response_time",
                "time_range",
            ]
        )
        for occ in occurrences:
            response_time = occ.get("response_time")
            response_count = 1 if occ.get("response_ts") is not None else 0
            response_pct = _format_percent(response_count, int(occ.get("sent_count") or 0))
            time_range = _bucket_for_ts(occ.get("response_ts")) or ""
            row = [
                occ.get("account_alias", ""),
                occ.get("thread_id", ""),
                occ.get("message_role", ""),
                occ.get("message_text", ""),
                occ.get("sent_count", 0),
                response_count,
                response_pct,
                _format_duration(response_time),
                _format_duration(response_time),
                _format_duration(response_time),
                time_range,
            ]
            writer.writerow(row)
    ok(f"Exportado a {path}")
    press_enter()


def menu_stats() -> None:
    while True:
        report = _build_report()
        banner()
        print(style_text("ESTADÍSTICAS Y MÉTRICAS", color=Fore.CYAN, bold=True))
        print(full_line())
        print("1) Saludos")
        print("2) Pitch")
        print("3) CTA / Agenda")
        print("4) Follow-ups")
        print("5) Resumen general")
        print("6) Exportar datos")
        print("7) Volver")
        print()
        choice = ask("Opción: ").strip()
        if choice == "1":
            _render_category(ROLE_GREETING, report)
        elif choice == "2":
            _render_category(ROLE_PITCH, report)
        elif choice == "3":
            _render_category(ROLE_CTA, report)
        elif choice == "4":
            _render_category(ROLE_FOLLOWUP, report)
        elif choice == "5":
            _render_funnel(report)
        elif choice == "6":
            _export_csv(report)
        elif choice == "7":
            break
        else:
            warn("Opción inválida.")
            press_enter()
