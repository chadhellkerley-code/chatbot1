# ig.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import hashlib
import logging
import math
import os
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time, timezone
from pathlib import Path
from typing import Callable, Dict, Optional

try:  # pragma: no cover - optional dependency for premium UI
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box

    _RICH_AVAILABLE = True
except Exception:  # pragma: no cover - fallback without rich
    Console = Group = Live = Panel = Table = Text = None  # type: ignore
    box = None  # type: ignore
    _RICH_AVAILABLE = False
try:  # pragma: no cover - depende de la versiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n de Python
    from zoneinfo import ZoneInfo as _BuiltinZoneInfo
except Exception:  # pragma: no cover - fallback si falta la stdlib
    _BuiltinZoneInfo = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    from backports.zoneinfo import ZoneInfo as _BackportZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback si falta el backport
    _BackportZoneInfo = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    from dateutil import tz as _dateutil_tz  # type: ignore
except Exception:  # pragma: no cover - si falta dateutil
    _dateutil_tz = None  # type: ignore[assignment]

from core.accounts import (
    auto_login_with_saved_password,
    get_account,
    has_playwright_storage_state,
    list_all,
    mark_connected,
    prompt_login,
)
from config import SETTINGS
from core.leads import load_list
from paths import logs_root
from runtime.runtime import (
    EngineCancellationToken,
    STOP_EVENT,
    bind_stop_token,
    bind_stop_token_callable,
    ensure_logging,
    request_stop,
    reset_stop_event,
    restore_stop_token,
    start_q_listener,
)
from core.session_store import has_session
from core.storage import (
    log_sent,
    mark_account_paused,
    paused_accounts_today,
    normalize_contact_username,
    sent_counts_today_by_account,
    sent_totals,
    successful_contacts_index,
)
from core.templates_store import load_templates, next_round_robin, render_template
from ui import Fore, LiveTable, style_text
from utils import ask, ask_int, enable_quiet_mode, press_enter, warn
from src.auth.onboarding import login_and_persist
from src.dm_campaign.campaign_runner import start_campaign
from src.dm_campaign.proxy_workers_runner import calculate_workers_for_alias
from src.proxy_assigner import assign_proxies_to_accounts
from src.transport.human_instagram_sender import HumanInstagramSender


logger = logging.getLogger(__name__)

_RUNNER_LOG_PATH = logs_root(Path(__file__).resolve().parent.parent) / "send_runner.log"
_HEADER_TEXT = "HERRAMIENTA DE MENSAJER\u00cdA DE IG - PROPIEDAD DE MATIDIAZLIFE/ELITE"
_CAMPAIGN_UI = None


def _env_flag(name: str) -> bool:
    value = os.getenv(name, "")
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _tune_concurrency(requested: int, available_accounts: int) -> int:
    hard_cap = max(1, _env_int("IG_MAX_CONCURRENCY_HARD", 75))
    tuned = max(1, min(int(requested or 1), max(1, available_accounts), hard_cap))
    if tuned != requested:
        warn(
            f"Concurrencia ajustada a {tuned} (solicitada={requested}, cuentas={available_accounts}, tope={hard_cap})."
        )
    return tuned


def _resolve_overnight(override: Optional[bool]) -> bool:
    if override is not None:
        return bool(override)
    return _env_flag("IG_OVERNIGHT")


def _resolve_headless(override: Optional[bool], overnight: bool) -> bool:
    if overnight:
        return True
    if _env_flag("IG_HEADLESS"):
        return True
    if override is not None:
        return bool(override)
    return True


def _resolve_proxy_workers_enabled(override: Optional[bool]) -> bool:
    if override is not None:
        return bool(override)
    if _env_flag("IG_CLASSIC_RUNNER"):
        return False
    raw = (os.getenv("IG_USE_PROXY_WORKERS", "1") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


ALLOW_SENT_UNVERIFIED = os.getenv("HUMAN_DM_ALLOW_UNVERIFIED", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
ACCOUNT_ERROR_STREAK_LIMIT = max(
    1,
    _env_int("IG_ACCOUNT_ERROR_STREAK_LIMIT", 3),
)
_ACCOUNT_STREAK_SCOPES = {"", "account", "network", "campaign"}


class CampaignUI:
    def __init__(
        self,
        *,
        alias: str,
        total_leads: int,
        settings: object,
        templates: list[dict[str, str]],
        concurrency: int,
        delay_min: int,
        delay_max: int,
        max_logs: int = 30,
    ) -> None:
        self.alias = alias
        self.total_leads = total_leads
        self.settings = settings
        self.templates = templates
        self.concurrency = concurrency
        self.delay_min = delay_min
        self.delay_max = delay_max
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._logs = deque(maxlen=max_logs)
        self._live: Live | None = None
        self._console: Console | None = Console() if _RICH_AVAILABLE else None

    def start(self) -> None:
        if not _RICH_AVAILABLE:
            self._print_header_fallback()
            return
        self._live = Live(
            self._build_renderable({}, {}, None, {}),
            console=self._console,
            refresh_per_second=24,
            transient=False,
            auto_refresh=False,
        )
        self._live.start()

    def stop(self) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None

    def prompt(self, text: str) -> str:
        self.stop()
        try:
            return input(text)
        finally:
            self.start()

    def emit_log(
        self,
        account: str,
        lead: str,
        action: str,
        detail: str | None = None,
        verified: bool | None = None,
    ) -> None:
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        line = f"{timestamp} | {account} -> {lead} | {action}"
        if detail:
            line = f"{line} | {detail}"
        if verified is not None:
            line = f"{line} | verified={'true' if verified else 'false'}"
        self._log_queue.put(line)

    def update(
        self,
        success_totals: Dict[str, int],
        failed_totals: Dict[str, int],
        live_table: LiveTable,
        send_state: Dict[str, object],
        leads_left: int,
    ) -> None:
        if not _RICH_AVAILABLE:
            while True:
                try:
                    print(self._log_queue.get_nowait())
                except queue.Empty:
                    break
            return
        while True:
            try:
                self._logs.append(self._log_queue.get_nowait())
            except queue.Empty:
                break
        if self._live is None:
            return
        renderable = self._build_renderable(
            success_totals, failed_totals, live_table, send_state, leads_left
        )
        self._live.update(renderable, refresh=True)

    def _build_renderable(
        self,
        success_totals: Dict[str, int],
        failed_totals: Dict[str, int],
        live_table: LiveTable | None,
        send_state: Dict[str, object],
        leads_left: int = 0,
    ):
        header = Text(f"[IG] {_HEADER_TEXT}", style="bold magenta")
        separator = Text("-" * max(40, len(_HEADER_TEXT) + 2), style="magenta")
        info = Text(
            f"Alias: {self.alias}  Leads pendientes: {leads_left}  Hora: {time.strftime('%H:%M:%S')}",
            style="bold",
        )

        totals = Table.grid(padding=(0, 1))
        totals.add_column()
        totals.add_row("Totales por cuenta (esta campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a)", style="bold cyan")
        for username in sorted(set(success_totals) | set(failed_totals)):
            ok_run = success_totals.get(username, 0)
            fail_run = failed_totals.get(username, 0)
            totals.add_row(f"{username} : {ok_run} OK / {fail_run} errores")

        inflight = Table(
            show_header=True,
            header_style="bold",
            box=box.SIMPLE if box else None,
            pad_edge=False,
        )
        inflight.add_column("Cuenta")
        inflight.add_column("Lead")
        inflight.add_column("Hora", justify="right")
        inflight.add_column("Res", justify="center")
        inflight.add_column("Detalle")
        if live_table:
            rows = live_table.rows()
        else:
            rows = []
        inflight_label = Text("EnvÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os en vuelo", style="bold cyan")
        inflight_section = inflight
        if rows:
            for row in rows[1:]:
                account, lead, started, status_icon, detail = row
                status = self._map_status(status_icon)
                inflight.add_row(str(account), str(lead), str(started), status, detail or "en progreso")
        else:
            inflight_label = Text(
                "EnvÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os en vuelo: (sin envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os en vuelo)", style="bold cyan"
            )
            inflight_section = None

        sent_today = int(send_state.get("sent", 0) or 0)
        err_today = int(send_state.get("errors", 0) or 0)
        skipped_no_dm = int(send_state.get("skipped_no_dm", 0) or 0)
        sent_unverified = int(send_state.get("sent_unverified", 0) or 0)
        template_preview = ""
        if self.templates:
            template_preview = _template_preview(self.templates[0].get("text", ""), limit=24)
        meta = Text()
        meta.append("Mensajes enviados: ", style="bold")
        meta.append(str(sent_today), style="bold green")
        meta.append("  Mensajes con error: ", style="bold")
        meta.append(str(err_today), style="bold red")
        meta.append("  Enviados sin verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n: ", style="bold")
        meta.append(str(sent_unverified), style="bold yellow")
        meta.append("  Saltados sin DM: ", style="bold")
        meta.append(str(skipped_no_dm), style="bold yellow")
        meta.append(f"  Concurrencia: {self.concurrency}", style="bold")
        meta2 = Text(
            f"Delay: {self.delay_min}-{self.delay_max}s  Plantilla: \"{template_preview}\"  Modo: humano/headless",
            style="bold",
        )

        logs_header = Text("LOGS (ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºltimos 30)", style="bold white")
        logs_body = "\n".join(self._logs) if self._logs else "Sin logs."
        logs_panel = Panel(logs_body, title=logs_header, border_style="white")
        controls = Text(
            "(Controles) Q=Cancelar campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a | Enter=Continuar | (si aplica) s/N=continuar cuenta",
            style="bold white",
        )

        sections = [
            header,
            separator,
            info,
            Text(""),
            totals,
            Text(""),
            inflight_label,
        ]
        if inflight_section is not None:
            sections.append(inflight_section)
        sections.extend(
            [
                Text(""),
                meta,
                meta2,
                Text(""),
                controls,
                Text(""),
                logs_panel,
            ]
        )
        return Group(*sections)

    def _map_status(self, icon: str) -> Text:
        value = str(icon or "")
        upper_value = value.upper()
        if any(token in upper_value for token in ("OK", "SUCCESS", "SENT")):
            return Text("OK", style="green")
        if any(token in upper_value for token in ("ERR", "ERROR", "FAILED")):
            return Text("ERR", style="red")
        if any(token in upper_value for token in ("WARN", "WARNING")):
            return Text("WARN", style="yellow")
        return Text("RUN", style="yellow")

    def _print_header_fallback(self) -> None:
        line = f"[IG] {_HEADER_TEXT}"
        try:
            print(line)
        except UnicodeEncodeError:
            print(_HEADER_TEXT)
        print("-" * max(40, len(_HEADER_TEXT) + 2))


def _log_runner_event(
    event: str,
    *,
    account: str = "-",
    lead: str = "-",
    status: str = "-",
    reason: str = "-",
) -> None:
    def _safe(value: object) -> str:
        return str(value).replace("\r", " ").replace("\n", " ").strip()

    line = (
        f"event={_safe(event)} account={_safe(account)} lead={_safe(lead)} "
        f"status={_safe(status)} reason={_safe(reason)}"
    )
    try:
        _RUNNER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with _RUNNER_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(f"{timestamp} {line}\n")
    except Exception:
        pass


@dataclass
class SendEvent:
    username: str
    lead: str
    success: bool
    detail: str
    index: int = 0
    total: int = 0
    started_at: str = ""
    duration_ms: int = 0
    template_id: str = ""
    template_name: str = ""
    selected_variant: str = ""
    cancelled: bool = False
    verified: bool = True
    attention: str | None = None
    reason_code: str | None = None
    reason_label: str | None = None
    suggestion: str | None = None
    scope: str | None = None
    diagnostic_ref: str | None = None


@dataclass
class CsvSendResult:
    username: str
    target: str
    status: str
    error: str | None = None


_LIVE_COUNTS = {"base_ok": 0, "base_fail": 0, "run_ok": 0, "run_fail": 0}
_LIVE_LOCK = threading.Lock()


def _load_timezone(label: str):
    for provider in (_BuiltinZoneInfo, _BackportZoneInfo):
        if provider is None:
            continue
        try:
            return provider(label)
        except Exception:
            continue
    if _dateutil_tz is not None:
        tzinfo = _dateutil_tz.gettz(label)
        if tzinfo is not None:
            return tzinfo
    return timezone.utc


AR_TZ = _load_timezone("America/Argentina/Cordoba")


def today_ar():
    return datetime.now(AR_TZ).date()


def next_midnight_ar(now=None):
    now = now or datetime.now(AR_TZ)
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, dt_time(0, 0), tzinfo=AR_TZ)


def create_daily_send_state() -> Dict[str, object]:
    """Return a fresh counter state for the send screen.

    Keeping this in a helper lets both owner and client launchers
    initialize the exact same midnight reset behaviour.
    """

    return {
        "date": today_ar(),
        "sent": 0,
        "errors": 0,
        "skipped_no_dm": 0,
        "sent_unverified": 0,
        "next_reset_at": next_midnight_ar(),
    }


def _refresh_daily_state(send_state: Dict[str, object]) -> None:
    try:
        now = datetime.now(AR_TZ)
        stored_date = send_state.get("date")
        next_reset = send_state.get("next_reset_at")
        if (
            stored_date is None
            or next_reset is None
            or now >= next_reset
            or today_ar() != stored_date
        ):
            send_state["date"] = today_ar()
            send_state["sent"] = 0
            send_state["errors"] = 0
            send_state["skipped_no_dm"] = 0
            send_state["sent_unverified"] = 0
            send_state["next_reset_at"] = next_midnight_ar(now)
    except Exception:
        try:
            send_state["date"] = today_ar()
            send_state.setdefault("sent", 0)
            send_state.setdefault("errors", 0)
            send_state.setdefault("skipped_no_dm", 0)
            send_state.setdefault("sent_unverified", 0)
            send_state["next_reset_at"] = next_midnight_ar()
        except Exception:
            pass


def _reset_live_counters(reset_run: bool = True) -> None:
    base_ok, base_fail = sent_totals()
    with _LIVE_LOCK:
        _LIVE_COUNTS["base_ok"] = base_ok
        _LIVE_COUNTS["base_fail"] = base_fail
        if reset_run:
            _LIVE_COUNTS["run_ok"] = 0
            _LIVE_COUNTS["run_fail"] = 0


def get_message_totals() -> tuple[int, int]:
    with _LIVE_LOCK:
        ok_total = _LIVE_COUNTS["base_ok"] + _LIVE_COUNTS["run_ok"]
        error_total = _LIVE_COUNTS["base_fail"] + _LIVE_COUNTS["run_fail"]
    return ok_total, error_total


def _ensure_session(username: str) -> bool:
    account = get_account(username)
    if not account:
        return False
    connected = has_playwright_storage_state(username)
    mark_connected(username, connected, invalidate_health=False)
    return connected


def _resolve_account_password(account: Dict) -> str:
    password = (account.get("password") or "").strip()
    if password:
        return password
    try:
        from core.accounts import _account_password as _lookup_password  # type: ignore
    except Exception:
        return ""
    try:
        return (_lookup_password(account) or "").strip()
    except Exception:
        return ""


def _proxy_payload_from_account(account: Dict) -> Optional[Dict]:
    try:
        from src.proxy_payload import proxy_from_account
    except Exception:
        return None
    return proxy_from_account(account)


def _enqueue_background_send(
    account: Dict,
    lead: str,
    message: str,
    delay_seconds: float,
) -> tuple[bool, str]:
    logger.info(
        "Background enqueue disabled. Sending will run locally for @%s -> @%s.",
        account.get("username"),
        lead,
    )
    return False, "background_disabled"


def _diagnose_exception(exc: Exception) -> str | None:
    text = str(exc).lower()
    mapping = {
        "login_required": "Instagram solicitÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un nuevo login.",
        "challenge_required": "Se requiere resolver un challenge en la app.",
        "feedback_required": "Instagram bloqueÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ temporalmente acciones de esta cuenta.",
        "rate_limit": "Se alcanzÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un rate limit. Conviene pausar unos minutos.",
        "checkpoint": "Instagram requiere verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n adicional (checkpoint).",
        "consent_required": "La sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n requiere aprobaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n en la app oficial.",
    }
    for key, message in mapping.items():
        if key in text:
            return message
    return None


def _load_accounts_from_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            if not raw_row:
                continue
            normalized = {
                (key or "").strip().lower(): (value or "").strip()
                for key, value in raw_row.items()
                if key is not None
            }
            username = normalized.get("username") or normalized.get("user")
            password = normalized.get("password")
            if not username or not password:
                continue
            rows.append(normalized)
        return rows


def _proxy_payload(data: dict[str, str]) -> dict[str, str]:
    payload: dict[str, str] = {}
    server = (
        data.get("proxy_url")
        or data.get("proxy")
        or data.get("proxy server")
        or data.get("server")
        or data.get("url")
    )
    if server:
        payload["proxy_url"] = server
    user = (
        data.get("proxy_user")
        or data.get("proxy username")
        or data.get("proxy_username")
        or data.get("proxy user")
    )
    if user:
        payload["proxy_user"] = user
    password = (
        data.get("proxy_pass")
        or data.get("proxy password")
        or data.get("proxy_password")
        or data.get("proxy pass")
    )
    if password:
        payload["proxy_pass"] = password
    return payload


def _send_from_csv_record(
    record: dict[str, str],
    default_target: str,
    default_message: str,
) -> CsvSendResult:
    username = (record.get("username") or record.get("user") or "").lstrip("@")
    password = record.get("password") or ""
    if not username or not password:
        return CsvSendResult(username=username or "desconocido", target=default_target, status="skipped")

    target = (record.get("target") or record.get("lead") or default_target).lstrip("@")
    message = record.get("message") or default_message
    proxy_data = _proxy_payload(record)
    account_payload = {
        "username": username,
        "password": password,
        "proxy_url": proxy_data.get("proxy_url", ""),
        "proxy_user": proxy_data.get("proxy_user", ""),
        "proxy_pass": proxy_data.get("proxy_pass", ""),
    }

    try:
        login_result = login_and_persist(account_payload, headless=True)
        if str((login_result or {}).get("status") or "").strip().lower() != "ok":
            message_text = str((login_result or {}).get("message") or "").strip() or "login_failed"
            return CsvSendResult(username=username, target=target, status="error", error=message_text)

        sender = HumanInstagramSender(headless=True, keep_browser_open_per_account=False)
        send_result = sender.send_message_like_human_sync(
            account=account_payload,
            target_username=target,
            text=message,
            base_delay_seconds=0.0,
            jitter_seconds=0.0,
            proxy=_proxy_payload_from_account(account_payload),
            return_detail=True,
        )
        if isinstance(send_result, tuple):
            success = bool(send_result[0])
            detail = str(send_result[1] or "").strip() if len(send_result) > 1 else ""
        else:
            success = bool(send_result)
            detail = ""
        if success:
            return CsvSendResult(username=username, target=target, status="sent")
        return CsvSendResult(
            username=username,
            target=target,
            status="error",
            error=detail or "send_failed",
        )
    except Exception as exc:
        return CsvSendResult(username=username, target=target, status="error", error=str(exc))


def _send_messages_from_csv(
    csv_path: str,
    default_target: str,
    default_message: str,
    *,
    batch_size: int = 10,
) -> list[CsvSendResult]:
    path = Path(csv_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el archivo CSV: {path}")
    accounts = _load_accounts_from_csv(path)
    if not accounts:
        logger.warning("El CSV %s no contiene cuentas vÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡lidas para procesar.", path)
        return []

    normalized_target = (default_target or "").strip().lstrip("@")
    normalized_message = (default_message or "").strip()
    if not normalized_target:
        raise ValueError("Se requiere un usuario objetivo para enviar mensajes.")
    if not normalized_message:
        raise ValueError("Se requiere un mensaje para enviar.")

    results: list[CsvSendResult] = []
    for record in accounts:
        results.append(_send_from_csv_record(record, normalized_target, normalized_message))
    return results


def _template_preview(text: str, limit: int = 60) -> str:
    cleaned = " ".join((text or "").splitlines()).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


def _template_candidates(text: str) -> list[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def _template_id(name: str, text: str) -> str:
    clean_name = (name or "").strip()
    if clean_name:
        return clean_name
    digest = hashlib.sha1((text or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"tmpl_{digest}"


def _build_template_entry(name: str, text: str, *, template_id: str = "") -> dict[str, str]:
    return {
        "name": (name or "").strip(),
        "text": text or "",
        "id": (template_id or "").strip() or _template_id(name, text),
    }


def _select_saved_templates() -> list[dict[str, str]]:
    if STOP_EVENT.is_set():
        reset_stop_event()
        return []
    saved = load_templates()
    if not saved:
        warn("No hay plantillas guardadas.")
        return []
    print("\nPlantillas disponibles:")
    for idx, item in enumerate(saved, start=1):
        preview = _template_preview(item.get("text", ""))
        print(f" {idx}) {item.get('name', '')} - {preview}")
    raw = ask("Selecciona plantillas (1,2 o nombre; Enter para cancelar): ").strip()
    if STOP_EVENT.is_set():
        reset_stop_event()
        return []
    if not raw:
        return []
    selections = [part.strip() for part in raw.split(",") if part.strip()]
    chosen: list[dict[str, str]] = []
    for token in selections:
        if token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(saved):
                item = saved[idx - 1]
                chosen.append(
                    _build_template_entry(
                        item.get("name", ""),
                        item.get("text", ""),
                        template_id=str(item.get("id") or ""),
                    )
                )
            continue
        lowered = token.lower()
        match = next(
            (item for item in saved if str(item.get("name", "")).lower() == lowered),
            None,
        )
        if match:
            chosen.append(
                _build_template_entry(
                    match.get("name", ""),
                    match.get("text", ""),
                    template_id=str(match.get("id") or ""),
                )
            )
    if not chosen:
        warn("No se seleccionaron plantillas validas.")
    return [item for item in chosen if item.get("text")]


def _render_message(template: str, *, lead: str, account: str) -> str:
    variables = {
        "nombre": lead,
        "username": lead,
        "usuario": lead,
        "lead": lead,
        "cuenta": account,
        "account": account,
    }
    return render_template(template, variables)


_ERROR_SIGNATURES = [
    {
        "keywords": ("login_required",),
        "code": "login_required",
        "detail": "SesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n vencida (Instagram pidiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ login)",
        "attention": "Instagram solicitÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un nuevo login.",
        "label": "Login requerido",
        "suggestion": "ReiniciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ la sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n desde la opciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n 1 antes de continuar.",
        "scope": "account",
    },
    {
        "keywords": ("challenge_required",),
        "code": "challenge_required",
        "detail": "Instagram pidiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ resolver un challenge",
        "attention": "Se requiere resolver un challenge en la app.",
        "label": "Challenge requerido",
        "suggestion": "IngresÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ a la app oficial y resolvÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â© el challenge pendiente.",
        "scope": "account",
    },
    {
        "keywords": (
            "feedback_required",
            "please wait a few minutes",
            "try again later",
            "we restrict certain activity",
            "something went wrong",
            "please try again later",
            "ha ocurrido un error",
            "ocurrio un error",
            "prueba mas tarde",
            "pruebalo mas tarde",
            "intentalo de nuevo mas tarde",
            "send_failed_toast",
            "send_failed_indicator",
        ),
        "code": "temporary_block",
        "detail": "Instagram bloqueÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ temporalmente las acciones de esta cuenta",
        "attention": "Instagram bloqueÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ temporalmente acciones de esta cuenta.",
        "label": "Bloqueo temporal",
        "suggestion": "PausÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ la campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a unos minutos y revisÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ el calentamiento de la cuenta.",
        "scope": "account",
    },
    {
        "keywords": ("rate_limit", "too many requests", "throttled", "429", "daily limit", "lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­mite diario"),
        "code": "rate_limit",
        "detail": "Se alcanzÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­mite de envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os (rate limit)",
        "attention": "Se alcanzÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un rate limit. Conviene pausar unos minutos.",
        "label": "Rate limit",
        "suggestion": "AumentÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ los delays o reducÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­ la concurrencia para esta campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a.",
        "scope": "account",
    },
    {
        "keywords": ("checkpoint",),
        "code": "checkpoint",
        "detail": "Instagram requiere un checkpoint de seguridad",
        "attention": "Instagram requiere verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n adicional (checkpoint).",
        "label": "Checkpoint requerido",
        "suggestion": "IngresÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ a la app oficial o al sitio para completar el checkpoint.",
        "scope": "account",
    },
    {
        "keywords": ("consent_required",),
        "code": "consent_required",
        "detail": "La sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n necesita aprobar el acceso desde la app oficial",
        "attention": "La sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n requiere aprobaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n en la app oficial.",
        "label": "Consentimiento pendiente",
        "suggestion": "AceptÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ el inicio de sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n desde la app oficial y reintentÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡.",
        "scope": "account",
    },
    {
        "keywords": (
            "privacy",
            "private account",
            "not authorized to view",
            "user can't receive your message",
            "recipient can't receive your message",
            "recipients have opted out",
        ),
        "code": "recipient_restricted",
        "detail": "El destinatario tiene restricciones de privacidad para recibir mensajes",
        "label": "Privacidad del destinatario",
        "suggestion": "SaltÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ este lead: la cuenta objetivo no acepta mensajes.",
        "scope": "recipient",
    },
    {
        "keywords": (
            "inactive user",
            "user not found",
            "username does not exist",
            "unknown user",
            "skipped_username_not_found",
            "username_not_found",
        ),
        "code": "user_not_found",
        "detail": "El usuario objetivo no existe o no estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ disponible",
        "label": "Usuario no disponible",
        "suggestion": "VerificÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ el usuario en la lista de leads.",
        "scope": "recipient",
    },
    {
        "keywords": ("spam", "suspicious activity"),
        "code": "spam_block",
        "detail": "Instagram detectÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ actividad sospechosa y bloqueÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o",
        "attention": "Instagram marcÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ la acciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n como sospechosa.",
        "label": "Actividad sospechosa",
        "suggestion": "ReducÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­ el ritmo de envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os y revisÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ el warm-up de la cuenta.",
        "scope": "account",
    },
    {
        "keywords": (
            "socket",
            "timed out",
            "connection aborted",
            "connection reset",
            "connection error",
            "temporarily unavailable",
        ),
        "code": "network",
        "detail": "Error de red al contactar Instagram",
        "attention": "Se detectÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ un error de red. RevisÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ la conexiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n o el proxy.",
        "label": "Error de red",
        "suggestion": "VerificÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ la conexiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n o cambiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ de proxy antes de reanudar.",
        "scope": "network",
    },
]


def _classify_exception(exc: Exception) -> tuple[str, str | None, str | None, str | None, str | None, str | None]:
    text = str(exc).strip()
    lowered = text.lower()
    name = exc.__class__.__name__.lower()
    for signature in _ERROR_SIGNATURES:
        if any(key in lowered or key in name for key in signature["keywords"]):
            detail = signature["detail"]
            if text and text.lower() not in detail.lower():
                detail = f"{detail} ({text})"
            attention = signature.get("attention") or _diagnose_exception(exc)
            label = signature.get("label")
            suggestion = signature.get("suggestion")
            scope = signature.get("scope")
            return detail, attention, signature.get("code"), label, suggestion, scope
    fallback_detail = text or "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³"
    return fallback_detail, _diagnose_exception(exc), None, text or "Error desconocido", None, None


def _classify_failure_detail(detail: str) -> tuple[str, str | None, str | None, str | None, str | None, str | None]:
    normalized = (detail or "").strip()
    if not normalized:
        return "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³", None, None, "Error desconocido", None, None
    return _classify_exception(RuntimeError(normalized))


_RETRYABLE_SEND_HINTS = (
    "timeout",
    "timed out",
    "page not responding",
    "page is not responding",
    "target closed",
    "browser has disconnected",
    "browser closed",
    "context closed",
    "page closed",
    "execution context was destroyed",
)

SKIPPED_NO_DM_REASON = "SKIPPED_NO_DM"
LEGACY_NO_DM_REASON = "NO_DM_BUTTON"
NO_DM_SKIP_REASON = SKIPPED_NO_DM_REASON
NO_DM_SKIP_DETAIL = "Perfil sin botÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n de mensaje / no permite DM"
NO_DM_SKIP_LOG = f"skip | no_dm | {NO_DM_SKIP_DETAIL}"
USERNAME_NOT_FOUND_SKIP_REASON = "SKIPPED_USERNAME_NOT_FOUND"
USERNAME_NOT_FOUND_SKIP_DETAIL = "No existe el username / no fue encontrado"
SENT_UNVERIFIED_REASON = "SENT_UNVERIFIED"
SENT_UNVERIFIED_DETAIL = (
    "Se intentÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ enviar y no se pudo verificar en DOM"
)


def _is_retryable_send_failure(detail: str) -> bool:
    lowered = (detail or "").lower()
    return any(token in lowered for token in _RETRYABLE_SEND_HINTS)


def _retry_delay_seconds(attempt: int) -> float:
    return min(30.0, 2.0 + (attempt * 2.0))


def _render_progress(
    alias: str,
    leads_left: int,
    success_totals: Dict[str, int],
    failed_totals: Dict[str, int],
    live_table: LiveTable,
    send_state: Dict[str, object],
) -> None:
    _refresh_daily_state(send_state)
    if _CAMPAIGN_UI is not None:
        _CAMPAIGN_UI.update(
            success_totals,
            failed_totals,
            live_table,
            send_state,
            leads_left,
        )


def _print_final_summary(
    alias: str,
    accounts: list[Dict],
    success: Dict[str, int],
    failed: Dict[str, int],
    no_dm_by_account: Dict[str, int],
    sent_unverified_by_account: Dict[str, int],
) -> None:
    title = "================ RESUMEN FINAL (campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a) ================"
    divider = "-" * len(title)
    footer = "=" * len(title)
    usernames = [str(a.get("username", "")).strip() for a in accounts if a.get("username")]
    usernames = sorted(set(usernames))
    account_width = max(20, len("Cuenta"), len("TOTAL GENERAL"))
    if usernames:
        account_width = max(account_width, max(len(name) for name in usernames))

    def _row(label: str, ok: int, err: int, no_dm: int, unver: int, total: int) -> str:
        return (
            f"{label:<{account_width}} "
            f"{ok:>5} {err:>5} {no_dm:>6} {unver:>6} {total:>6}"
        )

    print(title)
    print(f"Alias: {alias} | Hora fin: {time.strftime('%H:%M:%S')}")
    print(divider)
    print(
        f"{'Cuenta':<{account_width}} "
        f"{'OK':>5} {'ERR':>5} {'NO_DM':>6} {'UNVER':>6} {'TOTAL':>6}"
    )

    total_ok = 0
    total_err = 0
    total_no_dm = 0
    total_unver = 0
    total_all = 0
    for username in usernames:
        unver = int(sent_unverified_by_account.get(username, 0))
        ok = max(0, int(success.get(username, 0)) - unver)
        err = int(failed.get(username, 0))
        no_dm = int(no_dm_by_account.get(username, 0))
        total = ok + err + no_dm + unver
        total_ok += ok
        total_err += err
        total_no_dm += no_dm
        total_unver += unver
        total_all += total
        print(_row(username, ok, err, no_dm, unver, total))

    print(divider)
    print(_row("TOTAL GENERAL", total_ok, total_err, total_no_dm, total_unver, total_all))
    print(footer)


def _handle_event(
    event: SendEvent,
    success: Dict[str, int],
    failed: Dict[str, int],
    live_table: LiveTable,
    remaining: Dict[str, int],
    bump: Callable[[str, int], None],
    error_tracker: Dict[str, Dict[str, object]],
    account_error_streaks: Dict[str, int],
    paused_accounts: set[str],
    emit_log: Callable[[str, str, str, Optional[str], Optional[bool]], None],
    prompt: Callable[[str], str],
    no_dm_by_account: Optional[Dict[str, int]] = None,
    sent_unverified_by_account: Optional[Dict[str, int]] = None,
    *,
    overnight: bool,
    request_stop_fn: Callable[[str], None],
) -> Optional[str]:
    username = event.username
    skip_reason = (event.reason_code or "").strip().upper()
    if not event.success and skip_reason in {NO_DM_SKIP_REASON, LEGACY_NO_DM_REASON}:
        account_error_streaks.pop(username, None)
        detail = event.detail or NO_DM_SKIP_DETAIL
        if no_dm_by_account is not None:
            no_dm_by_account[username] = int(no_dm_by_account.get(username, 0)) + 1
        _log_runner_event(
            "message",
            account=username,
            lead=event.lead,
            status="skipped",
            reason=detail,
        )
        live_table.complete(username, True, NO_DM_SKIP_LOG)
        log_sent(
            username,
            event.lead,
            False,
            detail,
            started_at=event.started_at,
            duration_ms=event.duration_ms,
            template_id=event.template_id,
            template_name=event.template_name,
            selected_variant=event.selected_variant,
            cancelled=event.cancelled,
            verified=False,
            skip=True,
            skip_reason=NO_DM_SKIP_REASON,
            diagnostic_ref=event.diagnostic_ref,
        )
        bump("skipped_no_dm", 1)
        emit_log(username, event.lead, "skip", f"no_dm | {detail}", None)
        return None
    if not event.success and skip_reason == USERNAME_NOT_FOUND_SKIP_REASON:
        account_error_streaks.pop(username, None)
        detail = event.detail or USERNAME_NOT_FOUND_SKIP_DETAIL
        _log_runner_event(
            "message",
            account=username,
            lead=event.lead,
            status="skipped",
            reason=detail,
        )
        live_table.complete(username, True, f"skip | username_not_found | {detail}")
        log_sent(
            username,
            event.lead,
            False,
            detail,
            started_at=event.started_at,
            duration_ms=event.duration_ms,
            template_id=event.template_id,
            template_name=event.template_name,
            selected_variant=event.selected_variant,
            cancelled=event.cancelled,
            verified=False,
            skip=True,
            skip_reason=USERNAME_NOT_FOUND_SKIP_REASON,
            diagnostic_ref=event.diagnostic_ref,
        )
        bump("skipped_no_dm", 1)
        emit_log(username, event.lead, "skip", f"username_not_found | {detail}", None)
        return None
    if event.success:
        account_error_streaks.pop(username, None)
        success[username] += 1
        detail = event.detail or ""
        is_unverified = (event.reason_code or "").strip().upper() == SENT_UNVERIFIED_REASON
        _log_runner_event(
            "message",
            account=username,
            lead=event.lead,
            status="sent",
            reason=detail or "ok",
        )
        live_table.complete(username, True, detail)
        log_sent(
            username,
            event.lead,
            True,
            detail,
            started_at=event.started_at,
            duration_ms=event.duration_ms,
            template_id=event.template_id,
            template_name=event.template_name,
            selected_variant=event.selected_variant,
            cancelled=event.cancelled,
            verified=event.verified,
            sent_unverified=is_unverified,
            diagnostic_ref=event.diagnostic_ref,
        )
        with _LIVE_LOCK:
            _LIVE_COUNTS["run_ok"] += 1
        bump("sent", 1)
        action = "enviado"
        if not event.verified:
            action = "enviado_sin_confirmacion"
        emit_log(username, event.lead, action, detail, event.verified)
        if is_unverified:
            bump("sent_unverified", 1)
            if sent_unverified_by_account is not None:
                sent_unverified_by_account[username] = int(
                    sent_unverified_by_account.get(username, 0)
                ) + 1
            emit_log(
                username,
                event.lead,
                "warn",
                f"sent_unverified | {SENT_UNVERIFIED_DETAIL}",
                None,
            )
    else:
        failed[username] += 1
        detail = event.detail or "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³"
        _log_runner_event(
            "message",
            account=username,
            lead=event.lead,
            status="error",
            reason=detail,
        )
        live_table.complete(username, False, detail)
        log_sent(
            username,
            event.lead,
            False,
            detail,
            started_at=event.started_at,
            duration_ms=event.duration_ms,
            template_id=event.template_id,
            template_name=event.template_name,
            selected_variant=event.selected_variant,
            cancelled=event.cancelled,
            verified=event.verified,
            diagnostic_ref=event.diagnostic_ref,
        )
        with _LIVE_LOCK:
            _LIVE_COUNTS["run_fail"] += 1
        bump("errors", 1)
        emit_log(username, event.lead, "error", detail, None)
        if event.diagnostic_ref:
            emit_log(username, event.lead, "warning", f"Diagnostico: {event.diagnostic_ref}", None)
        scope_key = (event.scope or "").strip().lower()
        streak_eligible = scope_key in _ACCOUNT_STREAK_SCOPES
        if streak_eligible:
            streak = int(account_error_streaks.get(username, 0)) + 1
            account_error_streaks[username] = streak
        else:
            streak = 0
            account_error_streaks.pop(username, None)
        normalized_username = username.lower()
        if overnight and event.reason_code == "overnight_retries_exhausted":
            if normalized_username not in paused_accounts:
                paused_accounts.add(normalized_username)
                remaining[username] = 0
                mark_account_paused(username)
            account_error_streaks[username] = 0
            emit_log(
                username,
                event.lead,
                "warning",
                "Cuenta pausada automaticamente (overnight: retries agotados).",
                None,
            )
            _log_runner_event(
                "account_pause",
                account=username,
                lead=event.lead,
                status="paused",
                reason="overnight_retries_exhausted",
            )
        if streak >= ACCOUNT_ERROR_STREAK_LIMIT and normalized_username not in paused_accounts:
            emit_log(
                username,
                event.lead,
                "warning",
                f"Proteccion activada ({ACCOUNT_ERROR_STREAK_LIMIT} errores consecutivos).",
                None,
            )
            if overnight:
                paused_accounts.add(normalized_username)
                remaining[username] = 0
                mark_account_paused(username)
                account_error_streaks[username] = 0
                emit_log(
                    username,
                    event.lead,
                    "warning",
                    "Cuenta pausada automaticamente (overnight).",
                    None,
                )
                _log_runner_event(
                    "account_pause",
                    account=username,
                    lead=event.lead,
                    status="paused",
                    reason="overnight_error_streak",
                )
            else:
                choice = prompt("ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿Continuar con esta cuenta igualmente? (s/N): ").strip().lower()
                if choice == "s":
                    emit_log(
                        username,
                        event.lead,
                        "warning",
                        "Se continuara con la cuenta bajo tu responsabilidad.",
                        None,
                    )
                    account_error_streaks[username] = 0
                else:
                    paused_accounts.add(normalized_username)
                    remaining[username] = 0
                    mark_account_paused(username)
                    account_error_streaks[username] = 0
                    emit_log(
                        username,
                        event.lead,
                        "warning",
                        "Cuenta pausada por el resto del dia.",
                        None,
                    )
                    logger.warning(
                        "Cuenta pausada por protecciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n diaria: @%s (errores consecutivos=%d)",
                        username,
                        streak,
                    )
        reason_key = (event.reason_code or "").strip().lower()
        if not reason_key:
            reason_key = (event.reason_label or detail).strip().lower()
        label = event.reason_label or detail or "Error desconocido"
        tracker = None
        if reason_key:
            tracker = error_tracker.setdefault(
                reason_key,
                {
                    "count": 0,
                    "alerted": False,
                    "label": label,
                    "suggestion": event.suggestion,
                },
            )
            tracker["count"] = int(tracker.get("count", 0)) + 1
            if event.reason_label and tracker.get("label") != event.reason_label:
                tracker["label"] = event.reason_label
            if event.suggestion and not tracker.get("suggestion"):
                tracker["suggestion"] = event.suggestion
            if not tracker.get("alerted") and tracker["count"] >= 3:
                emit_log(
                    username,
                    event.lead,
                    "warning",
                    f"Patron detectado: {tracker['count']} errores con motivo '{tracker['label']}'.",
                    None,
                )
                suggestion = tracker.get("suggestion")
                if suggestion:
                    emit_log(username, event.lead, "warning", f"Sugerencia: {suggestion}", None)
                else:
                    emit_log(
                        username,
                        event.lead,
                        "warning",
                        "Sugerencia: pausa la campana o ajusta delays/concurrencia.",
                        None,
                    )
                tracker["alerted"] = True
    if event.attention:
        emit_log(username, event.lead, "warning", event.attention, None)
        if overnight:
            normalized_username = username.lower()
            if normalized_username not in paused_accounts:
                paused_accounts.add(normalized_username)
                remaining[username] = 0
                mark_account_paused(username)
            emit_log(
                username,
                event.lead,
                "warning",
                "Cuenta pausada automaticamente (overnight).",
                None,
            )
            _log_runner_event(
                "account_pause",
                account=username,
                lead=event.lead,
                status="paused",
                reason="overnight_attention",
            )
            return "continue"
        choice = prompt("Atencion: [1] Continuar sin esta cuenta, [2] Pausar todo. Opcion: ").strip() or "1"
        if choice == "1":
            remaining[username] = 0
            emit_log(username, event.lead, "warning", "Cuenta omitida en esta campana.", None)
            return "continue"
        request_stop_fn(f"usuario decidiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ pausar tras incidente con @{username}")
        return "stop"
    return None


def _build_accounts_for_alias(alias: str, *, overnight: bool = False) -> list[Dict]:
    all_acc = [a for a in list_all() if a.get("alias") == alias and a.get("active")]
    if not all_acc:
        warn("No hay cuentas activas en ese alias.")
        if not overnight:
            press_enter()
        return []

    paused_lookup = {acct.lower() for acct in paused_accounts_today()}
    if paused_lookup:
        paused_in_alias = [acct for acct in all_acc if acct.get("username", "").lower() in paused_lookup]
        if paused_in_alias:
            warn(
                "Las siguientes cuentas estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡n pausadas por protecciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n diaria y se omitirÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡n automÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ticamente:",
            )
            for acct in paused_in_alias:
                print(f" - @{acct['username']}")
            print()
        all_acc = [acct for acct in all_acc if acct.get("username", "").lower() not in paused_lookup]
        if not all_acc:
            warn("Todas las cuentas del alias estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡n pausadas por hoy. ReintentÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ maÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±ana.")
            if not overnight:
                press_enter()
            return []

    verified: list[Dict] = []
    needing_login: list[tuple[Dict, str]] = []
    for account in all_acc:
        username = account["username"]
        if has_playwright_storage_state(username):
            verified.append(account)
            continue
        if not has_session(username):
            needing_login.append((account, "sin sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n guardada"))
            continue
        if not _ensure_session(username):
            needing_login.append((account, "sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n expirada"))
            continue
        verified.append(account)

    if needing_login:
        remaining: list[tuple[Dict, str]] = []
        for account, reason in needing_login:
            username = account["username"]
            login_ok = False
            if auto_login_with_saved_password(username, account=account) and _ensure_session(username):
                refreshed = get_account(username) or account
                if refreshed not in verified:
                    verified.append(refreshed)
                login_ok = True
            if not login_ok:
                remaining.append((account, reason))

        if remaining:
            print("\nLas siguientes cuentas necesitan volver a iniciar sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n:")
            for account, reason in remaining:
                print(f" - @{account['username']}: {reason}")
            if overnight:
                warn("Se omitieron las cuentas sin sesion valida (overnight).")
            elif ask("ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿Iniciar sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n ahora? (s/N): ").strip().lower() == "s":
                for account, _ in remaining:
                    username = account["username"]
                    if auto_login_with_saved_password(username, account=account) and _ensure_session(
                        username
                    ):
                        refreshed = get_account(username) or account
                        if refreshed not in verified:
                            verified.append(refreshed)
                        continue
                    if prompt_login(username, interactive=False) and _ensure_session(username):
                        refreshed = get_account(username) or account
                        if refreshed not in verified:
                            verified.append(refreshed)
            else:
                warn("Se omitieron las cuentas sin sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n vÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡lida.")

    if not verified:
        warn("No hay cuentas con sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n vÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡lida para enviar mensajes.")
        if not overnight:
            press_enter()
        return []

    verified.sort(key=lambda acct: (acct.get("low_profile", False), acct.get("username", "")))
    low_profile_accounts = [acct for acct in verified if acct.get("low_profile")]
    if low_profile_accounts:
        warn(
            "Se detectaron cuentas en modo bajo perfil. Se aplicarÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡n lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­mites conservadores automÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ticamente."
        )
        for acct in low_profile_accounts:
            reason = acct.get("low_profile_reason") or "motivo no especificado"
            print(f" - @{acct['username']}: {reason}")
        print()

    if low_profile_accounts and not overnight:
        if STOP_EVENT.is_set():
            reset_stop_event()
            return []
        choice = ask("Aplicar limites conservadores automaticamente? (S/n): ").strip().lower()
        if STOP_EVENT.is_set():
            reset_stop_event()
            return []
        if choice in {"n", "no"}:
            for acct in verified:
                if acct.get("low_profile"):
                    acct["low_profile"] = False
            warn("Se continuara sin limites conservadores en esta campana.")
            print()

    return verified


def _schedule_inputs(
    settings, concurrency_override: Optional[int]
) -> Optional[tuple[str, str, int, int, int, list[dict[str, str]]]]:
    def _stop_requested() -> bool:
        if STOP_EVENT.is_set():
            reset_stop_event()
            return True
        return False

    if _stop_requested():
        return None
    alias = ask("Alias/grupo: ").strip() or "default"
    if _stop_requested():
        return None
    listname = ask("Nombre de la lista (storage/leads/<nombre>.txt): ").strip()
    if _stop_requested():
        return None

    workers_capacity = 1
    try:
        capacity = calculate_workers_for_alias(alias)
        workers_capacity = max(1, int(capacity.get("workers_capacity") or 1))
    except Exception:
        workers_capacity = 1

    default_concurrency = max(1, min(int(settings.max_concurrency or 1), workers_capacity))

    if concurrency_override is not None:
        concurr_input = max(1, min(int(concurrency_override), workers_capacity))
        print(f"Concurrencia forzada: {concurr_input} (max {workers_capacity})")
    else:
        concurr_input = ask_int(
            f"Workers simultaneos? [{default_concurrency}] (max {workers_capacity}): ",
            1,
            default=default_concurrency,
        )
    if _stop_requested():
        return None
    if concurr_input < 1:
        warn("La concurrencia minima es 1. Se ajusta a 1.")
    if concurr_input > workers_capacity:
        warn(f"Concurrencia ajustada a {workers_capacity} (maximo segun proxies/cuentas).")
    concurr = max(1, min(concurr_input, workers_capacity))

    dmin_default = max(10, settings.delay_min)
    dmin_input = ask_int(
        f"Delay mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimo (seg) [{dmin_default}]: ",
        1,
        default=dmin_default,
    )
    if _stop_requested():
        return None
    if dmin_input < 10:
        warn("El delay mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimo recomendado es 10s. Se ajusta automÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ticamente.")
    delay_min = max(10, dmin_input)

    dmax_default = max(delay_min, settings.delay_max)
    dmax_input = ask_int(
        f"Delay mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ximo (seg) [>= {delay_min}, por defecto {dmax_default}]: ",
        delay_min,
        default=dmax_default,
    )
    if _stop_requested():
        return None
    if dmax_input < delay_min:
        warn("Delay mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ximo ajustado al mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimo indicado.")
    delay_max = max(delay_min, dmax_input)

    templates: list[dict[str, str]] = []
    use_saved = ask("Usar plantillas guardadas? (s/N): ").strip().lower()
    if _stop_requested():
        return None
    if use_saved == "s":
        templates = _select_saved_templates()
        if _stop_requested():
            return None
    if not templates:
        print("Escribi plantillas (una por linea). Linea vacia para terminar:")
        manual_lines: list[str] = []
        while True:
            if _stop_requested():
                return None
            s = ask("")
            if _stop_requested():
                return None
            if not s:
                break
            manual_lines.append(s)
        if not manual_lines:
            manual_lines = ["hola!"]
        templates = [
            _build_template_entry("", line.strip())
            for line in manual_lines
            if line.strip()
        ]

    return alias, listname, concurr, delay_min, delay_max, templates


def menu_send_rotating(
    concurrency_override: Optional[int] = None,
    *,
    overnight: Optional[bool] = None,
    headless: Optional[bool] = None,
    use_proxy_workers: Optional[bool] = None,
) -> None:
    ensure_logging(
        level=logging.ERROR,
        quiet=True,
        log_dir=SETTINGS.log_dir,
        log_file=SETTINGS.log_file,
    )
    enable_quiet_mode()
    reset_stop_event()
    campaign_token = EngineCancellationToken("campaign")
    stop_binding = bind_stop_token(campaign_token)
    _reset_live_counters()
    settings = SETTINGS
    overnight_mode = _resolve_overnight(overnight)
    headless_mode = _resolve_headless(headless, overnight_mode)

    send_state: Dict[str, object] = create_daily_send_state()

    def bump(kind: str, delta: int = 1) -> None:
        if kind not in {"sent", "errors", "skipped_no_dm", "sent_unverified"}:
            return
        try:
            _refresh_daily_state(send_state)
            current = int(send_state.get(kind, 0) or 0)
            send_state[kind] = current + delta
        except Exception:
            try:
                send_state[kind] = int(send_state.get(kind, 0) or 0) + delta
            except Exception:
                pass

    inputs = _schedule_inputs(settings, concurrency_override)
    if inputs is None:
        restore_stop_token(stop_binding)
        return
    (
        alias,
        listname,
        concurr,
        delay_min,
        delay_max,
        templates,
    ) = inputs

    try:
        run_result = start_campaign(
            {
                "alias": alias,
                "leads_alias": listname,
                "workers_requested": concurr,
                "delay_min": delay_min,
                "delay_max": delay_max,
                "templates": templates,
                "headless": headless_mode,
            }
        )
        logger.info(
            "Campaign Runner completado: sent=%s failed=%s remaining=%s workers=%s",
            run_result.get("sent"),
            run_result.get("failed"),
            run_result.get("remaining"),
            run_result.get("workers_effective"),
        )
    except Exception as exc:
        logger.exception("Campaign Runner fallo: %s", exc)
        warn("Campaign Runner fallo. Revisa logs para diagnosticar el error.")
    if not overnight_mode:
        press_enter()
    restore_stop_token(stop_binding)
    return
    accounts = _build_accounts_for_alias(alias, overnight=overnight_mode)
    if not accounts:
        restore_stop_token(stop_binding)
        return
    accounts = assign_proxies_to_accounts(accounts)
    concurr = _tune_concurrency(concurr, len(accounts))
    proxy_ready_accounts = [acct for acct in accounts if _proxy_payload_from_account(acct)]
    if concurr >= 20 and len(proxy_ready_accounts) < concurr:
        adjusted = max(1, min(concurr, len(proxy_ready_accounts)))
        warn(
            "Concurrencia alta sin proxies suficientes. "
            f"Se ajusta concurrencia a {adjusted} (cuentas con proxy={len(proxy_ready_accounts)})."
        )
        concurr = adjusted
    if concurr >= 20 and delay_min < 20:
        warn("Concurrencia alta detectada: delay minimo ajustado a 20s para reducir riesgo de bloqueo.")
        delay_min = 20
        delay_max = max(delay_max, delay_min)

    sender = HumanInstagramSender(
        headless=headless_mode,
        keep_browser_open_per_account=True,
    )

    def _account_cap(record: Dict) -> int:
        limit = 25
        for key in ("messages_per_account", "max_messages"):
            raw_limit = record.get(key)
            try:
                candidate = int(raw_limit)
            except Exception:
                continue
            if candidate > 0:
                limit = candidate
                break
        if record.get("low_profile"):
            limit = min(limit, SETTINGS.low_profile_daily_cap or limit)
        return max(1, limit)

    def _account_delay_range(record: Dict) -> tuple[int, int]:
        if not record.get("low_profile"):
            return delay_min, delay_max
        factor = max(100, getattr(SETTINGS, "low_profile_delay_factor", 150))
        multiplier = max(1.0, factor / 100.0)
        scaled_min = max(delay_min, int(math.ceil(delay_min * multiplier)))
        scaled_max = max(scaled_min, int(math.ceil(delay_max * multiplier)))
        return scaled_min, scaled_max

    account_caps = {a["username"]: _account_cap(a) for a in accounts}
    account_delays = {a["username"]: _account_delay_range(a) for a in accounts}
    account_next_at: Dict[str, float] = {}

    if any(a.get("low_profile") for a in accounts):
        delay_multiplier = max(1.0, getattr(SETTINGS, "low_profile_delay_factor", 150) / 100.0)
        logger.info(
            "Modo bajo perfil aplicado: lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­mite %d mensajes/cuenta y delay x%.2f.",
            SETTINGS.low_profile_daily_cap,
            delay_multiplier,
        )

    source_users = load_list(listname)
    pending_users: list[str] = []
    skipped_contacted: list[tuple[str, list[str]]] = []
    skipped_duplicates = 0
    seen_pending: set[str] = set()
    contacted_index = successful_contacts_index()

    for username in source_users:
        normalized = normalize_contact_username(username)
        if not normalized:
            continue
        if normalized in seen_pending:
            skipped_duplicates += 1
            continue
        senders = contacted_index.get(normalized)
        if senders:
            skipped_contacted.append((username, sorted(senders)))
            continue
        pending_users.append(username)
        seen_pending.add(normalized)
    users = deque(pending_users)

    if skipped_contacted:
        preview_items = []
        for lead, senders in skipped_contacted[:5]:
            if senders:
                preview_items.append(f"@{lead} (ya enviado por @{senders[0]})")
            else:
                preview_items.append(f"@{lead}")
        preview = ", ".join(preview_items)
        if len(skipped_contacted) > 5:
            preview += f", ... (+{len(skipped_contacted) - 5})"
        print(f"Leads omitidos por contacto previo: {len(skipped_contacted)}")
        if preview:
            print(f"Muestra: {preview}")
        print()
    if skipped_duplicates > 0:
        print(f"Leads duplicados omitidos en la lista: {skipped_duplicates}\n")

    if not users:
        warn("No hay leads (o todos ya fueron contactados).")
        if not overnight_mode:
            press_enter()
        restore_stop_token(stop_binding)
        return

    sent_today_by_account = sent_counts_today_by_account(
        str(account.get("username") or "").strip()
        for account in accounts
        if isinstance(account, dict)
    )
    remaining = {
        a["username"]: max(
            0,
            account_caps[a["username"]] - int(sent_today_by_account.get(str(a["username"]).lower(), 0)),
        )
        for a in accounts
    }
    success = defaultdict(int)
    failed = defaultdict(int)
    no_dm_by_account = defaultdict(int)
    sent_unverified_by_account = defaultdict(int)
    total_target = min(len(users), sum(remaining.values()))
    send_index = 0
    semaphore = threading.Semaphore(concurr)
    account_locks = {a["username"]: threading.Lock() for a in accounts}
    result_queue: queue.Queue[SendEvent] = queue.Queue()
    live_table = LiveTable(max_entries=concurr)
    error_tracker: Dict[str, Dict[str, object]] = {}
    account_error_streaks: Dict[str, int] = defaultdict(int)
    paused_runtime: set[str] = {name.lower() for name in paused_accounts_today()}
    ramp_enabled = concurr > 5 and not _env_flag("IG_DISABLE_CONCURRENCY_RAMP")
    ramp_start_default = min(concurr, max(5, min(10, concurr)))
    active_limit = concurr if not ramp_enabled else max(
        1,
        min(concurr, _env_int("IG_CONCURRENCY_RAMP_START", ramp_start_default)),
    )
    ramp_step = max(1, _env_int("IG_CONCURRENCY_RAMP_STEP", 5))
    ramp_every = max(5, _env_int("IG_CONCURRENCY_RAMP_EVERY", 20))
    ramp_fail_threshold = min(0.90, max(0.01, _env_float("IG_CONCURRENCY_RAMP_FAIL_RATE", 0.18)))
    inflight_lock = threading.Lock()
    inflight = 0
    ramp_seen = 0
    ramp_failed = 0
    ramp_ignored_codes = {
        NO_DM_SKIP_REASON,
        LEGACY_NO_DM_REASON,
        USERNAME_NOT_FOUND_SKIP_REASON,
        "template_empty",
        "template_selected_empty",
        "send_cancelled",
    }

    global _CAMPAIGN_UI
    _CAMPAIGN_UI = CampaignUI(
        alias=alias,
        total_leads=len(users),
        settings=settings,
        templates=templates,
        concurrency=concurr,
        delay_min=delay_min,
        delay_max=delay_max,
    )
    _CAMPAIGN_UI.start()

    def emit_log(account: str, lead: str, action: str, detail: str | None = None, verified: bool | None = None) -> None:
        if _CAMPAIGN_UI is not None:
            _CAMPAIGN_UI.emit_log(account, lead, action, detail, verified)

    def prompt(text: str) -> str:
        if overnight_mode:
            return ""
        if _CAMPAIGN_UI is not None:
            return _CAMPAIGN_UI.prompt(text)
        return ask(text)

    stop_reason: str | None = None
    stop_requested = False

    def _request_stop(reason: str) -> None:
        nonlocal stop_reason, stop_requested
        stop_requested = True
        if stop_reason is None:
            stop_reason = reason
        request_stop(reason)

    def _on_event_processed(event: SendEvent) -> None:
        nonlocal inflight, active_limit, ramp_seen, ramp_failed
        with inflight_lock:
            if inflight > 0:
                inflight -= 1

        if not ramp_enabled:
            return
        if event.reason_code in ramp_ignored_codes:
            return
        ramp_seen += 1
        if not event.success:
            ramp_failed += 1
        if ramp_seen < ramp_every:
            return

        fail_rate = (ramp_failed / ramp_seen) if ramp_seen else 0.0
        if fail_rate <= ramp_fail_threshold and active_limit < concurr:
            new_limit = min(concurr, active_limit + ramp_step)
            if new_limit != active_limit:
                logger.info(
                    "Ramp-up concurrencia: %d -> %d (ventana=%d, fail_rate=%.2f).",
                    active_limit,
                    new_limit,
                    ramp_seen,
                    fail_rate,
                )
                active_limit = new_limit
        elif fail_rate > ramp_fail_threshold and active_limit > 1:
            new_limit = max(1, active_limit - ramp_step)
            if new_limit != active_limit:
                logger.warning(
                    "Freno automatico de concurrencia: %d -> %d (ventana=%d, fail_rate=%.2f).",
                    active_limit,
                    new_limit,
                    ramp_seen,
                    fail_rate,
                )
                active_limit = new_limit
        ramp_seen = 0
        ramp_failed = 0

    listener = start_q_listener(
        "PresionÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ Q para detener la campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a.",
        logger,
        token=campaign_token,
    )
    executor = ThreadPoolExecutor(max_workers=concurr, thread_name_prefix="ig-send")
    if ramp_enabled:
        logger.info(
            "Concurrencia dinÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡mica activa: inicio=%d objetivo=%d paso=%d ventana=%d fail_rate_max=%.2f",
            active_limit,
            concurr,
            ramp_step,
            ramp_every,
            ramp_fail_threshold,
        )

    logger.info(
        "Iniciando campana con %d cuentas activas y %d leads pendientes. Concurrencia: %d, delay: %s-%ss",
        len(accounts),
        len(users),
        concurr,
        delay_min,
        delay_max,
    )
    _render_progress(
        alias,
        len(users),
        success,
        failed,
        live_table,
        send_state,
    )
    _log_runner_event(
        "start",
        account="-",
        lead="-",
        status="started",
        reason=(
            f"accounts={len(accounts)} leads={len(users)} total={total_target} "
            f"concurrency={concurr}"
        ),
    )

    def _attempt_send(
        account: Dict,
        lead: str,
        message: str,
        *,
        index: int,
        total: int,
        template_id: str,
        template_name: str,
        selected_variant: str,
    ) -> SendEvent:
        username = account["username"]
        started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        start_ts = time.time()
        attention_message: str | None = None
        detail = ""
        success_flag = False
        reason_code: str | None = None
        reason_label: str | None = None
        suggestion: str | None = None
        scope: str | None = None
        diagnostic_ref: str | None = None
        verified_flag = False
        max_retries = 12 if overnight_mode else 1
        attempt = 0
        retryable_failure = False
        last_detail = ""
        delay_min_target, delay_max_target = account_delays.get(username, (delay_min, delay_max))
        jitter_window = max(0, delay_max_target - delay_min_target)

        def _sleep_until_or_stop(seconds: float) -> bool:
            wait_s = max(0.0, float(seconds or 0.0))
            if wait_s <= 0:
                return True
            deadline = time.time() + wait_s
            while not STOP_EVENT.is_set():
                remaining_wait = deadline - time.time()
                if remaining_wait <= 0:
                    return True
                time.sleep(min(0.5, remaining_wait))
            return False

        # Single send path: human sender only.
        while not STOP_EVENT.is_set() and attempt < max_retries:
            try:
                attempt += 1
                if attempt == 1:
                    now_ts = time.time()
                    next_at = account_next_at.get(username)
                    delay_seconds = max(0.0, (next_at - now_ts) if next_at is not None else 0.0)
                    if not _sleep_until_or_stop(delay_seconds):
                        cancelled_detail = "envio cancelado antes de iniciar"
                        return SendEvent(
                            username=username,
                            lead=lead,
                            success=False,
                            detail=cancelled_detail,
                            index=index,
                            total=total,
                            started_at=started_at,
                            duration_ms=int((time.time() - start_ts) * 1000),
                            template_id=template_id,
                            template_name=template_name,
                            selected_variant=selected_variant,
                            cancelled=True,
                            reason_code="send_cancelled",
                            reason_label="Envio cancelado",
                            scope="campaign",
                        )
                send_result = sender.send_message_like_human_sync(
                    account=account,
                    target_username=lead,
                    text=message,
                    base_delay_seconds=0.0,
                    jitter_seconds=0.0,
                    proxy=_proxy_payload_from_account(account),
                    return_detail=True,
                    return_payload=True,
                )
                payload = {}
                verified_flag = False
                if isinstance(send_result, tuple):
                    if len(send_result) == 3:
                        success_flag, info, payload = send_result
                    else:
                        success_flag, info = send_result
                else:
                    success_flag, info = send_result, None

                diagnostic_ref_candidate = str(payload.get("diagnostic_ref") or "").strip()
                if not diagnostic_ref_candidate:
                    artifacts = payload.get("failure_artifacts")
                    if isinstance(artifacts, dict):
                        diagnostic_ref_candidate = str(
                            artifacts.get("meta") or artifacts.get("screenshot") or ""
                        ).strip()
                if diagnostic_ref_candidate:
                    diagnostic_ref = diagnostic_ref_candidate

                skip_reason = (payload.get("skip_reason") or info or "").strip().upper()
                if skip_reason in {NO_DM_SKIP_REASON, LEGACY_NO_DM_REASON}:
                    success_flag = False
                    detail = NO_DM_SKIP_DETAIL
                    reason_code = NO_DM_SKIP_REASON
                    reason_label = NO_DM_SKIP_DETAIL
                    scope = "lead"
                    break
                if skip_reason == USERNAME_NOT_FOUND_SKIP_REASON:
                    success_flag = False
                    detail = USERNAME_NOT_FOUND_SKIP_DETAIL
                    reason_code = USERNAME_NOT_FOUND_SKIP_REASON
                    reason_label = "Username no encontrado"
                    suggestion = "Revisa el lead: el username no existe o no se pudo encontrar."
                    scope = "lead"
                    break
                if (
                    payload.get("sent_unverified")
                    or (payload.get("reason_code") or "").strip().upper() == SENT_UNVERIFIED_REASON
                    or (info or "").strip().lower() == "sent_unverified"
                ):
                    verified_flag = False
                    detail = info or "sent_unverified"
                    reason_code = SENT_UNVERIFIED_REASON
                    reason_label = "Enviado sin verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n"
                    scope = "lead"
                    if ALLOW_SENT_UNVERIFIED:
                        success_flag = True
                    else:
                        success_flag = False
                        suggestion = (
                            suggestion
                            or "Instagram no confirmÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el mensaje. Se tomÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ como error para evitar falsos enviados."
                        )
                    break
                if success_flag:
                    verified_flag = bool(payload.get("verified", True))
                if success_flag:
                    detail = info or "Enviado en modo humano"
                else:
                    detail = info or "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ (modo humano)"
                    last_detail = detail
                    retryable_failure = overnight_mode and _is_retryable_send_failure(detail)
                    if retryable_failure and attempt < max_retries:
                        time.sleep(_retry_delay_seconds(attempt))
                        continue
                    (
                        _classified_detail,
                        diag_attention,
                        code,
                        label,
                        suggestion_hint,
                        scope_hint,
                    ) = _classify_failure_detail(detail)
                    if code and not reason_code:
                        reason_code = code
                    if label and not reason_label:
                        reason_label = label
                    if suggestion_hint and not suggestion:
                        suggestion = suggestion_hint
                    if diag_attention and not attention_message:
                        attention_message = diag_attention
                    scope = scope_hint or scope
                    normalized_info = (detail or "").lower()
                    if "cancel" in normalized_info:
                        reason_code = reason_code or "send_cancelled"
                        reason_label = reason_label or "EnvÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o cancelado"
                        scope = scope or "campaign"
                    else:
                        reason_code = reason_code or "human_sender_failed"
                        reason_label = reason_label or "Fallo en envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o humanizado"
                        suggestion = suggestion or "RevisÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ la sesiÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n persistente o repetÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­ el login."
                        scope = scope or "account"
                break
            except Exception as exc:  # pragma: no cover - automatizaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n externa
                detail, diag_attention, code, label, suggestion_hint, scope_hint = _classify_exception(exc)
                last_detail = detail
                retryable_failure = overnight_mode and _is_retryable_send_failure(detail)
                if retryable_failure and attempt < max_retries:
                    time.sleep(_retry_delay_seconds(attempt))
                    continue
                if code and not reason_code:
                    reason_code = code
                if label and not reason_label:
                    reason_label = label
                if suggestion_hint and not suggestion:
                    suggestion = suggestion_hint
                if diag_attention:
                    attention_message = diag_attention
                scope = scope_hint or scope
                logger.warning(
                    "Fallo inesperado con @%s ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ @%s: %s",
                    username,
                    lead,
                    exc,
                    exc_info=False,
                )
                break

        if (
            not success_flag
            and overnight_mode
            and retryable_failure
            and attempt >= max_retries
            and not STOP_EVENT.is_set()
        ):
            reason_code = "overnight_retries_exhausted"
            reason_label = "Retries agotados"
            scope = scope or "account"
            detail = f"retry_exhausted ({last_detail})" if last_detail else "retry_exhausted"

        duration_ms = int((time.time() - start_ts) * 1000)
        cancelled_flag = False
        if STOP_EVENT.is_set() and not success_flag:
            cancelled_flag = True
        if STOP_EVENT.is_set() and not success_flag and not detail:
            detail = "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o cancelado"
        if not success_flag and not detail:
            detail = "envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³"
        if reason_label is None and reason_code:
            reason_label = reason_code.replace("_", " ").capitalize()
        if not cancelled_flag and not STOP_EVENT.is_set():
            next_delay = delay_min_target + random.uniform(0, jitter_window)
            account_next_at[username] = time.time() + max(0.0, next_delay)
        return SendEvent(
            username=username,
            lead=lead,
            success=success_flag,
            detail=detail,
            index=index,
            total=total,
            started_at=started_at,
            duration_ms=duration_ms,
            template_id=template_id,
            template_name=template_name,
            selected_variant=selected_variant,
            cancelled=cancelled_flag,
            verified=verified_flag if success_flag else False,
            attention=attention_message,
            reason_code=reason_code,
            reason_label=reason_label,
            suggestion=suggestion,
            scope=scope,
            diagnostic_ref=diagnostic_ref,
        )

    def _worker(
        account: Dict,
        lead: str,
        message: str,
        account_lock: threading.Lock,
        *,
        index: int,
        total: int,
        template_id: str,
        template_name: str,
        selected_variant: str,
    ) -> None:
        try:
            if STOP_EVENT.is_set():
                started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                event = SendEvent(
                    username=account.get("username", ""),
                    lead=lead,
                    success=False,
                    detail="envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o cancelado",
                    index=index,
                    total=total,
                    started_at=started_at,
                    duration_ms=0,
                    template_id=template_id,
                    template_name=template_name,
                    selected_variant=selected_variant,
                    cancelled=True,
                    reason_code="send_cancelled",
                    reason_label="Envio cancelado",
                    scope="campaign",
                )
                result_queue.put(event)
                return
            event = _attempt_send(
                account,
                lead,
                message,
                index=index,
                total=total,
                template_id=template_id,
                template_name=template_name,
                selected_variant=selected_variant,
            )
            result_queue.put(event)
        finally:
            account_lock.release()
            semaphore.release()

    try:
        last_render = 0.0
        while users and any(v > 0 for v in remaining.values()) and not STOP_EVENT.is_set():
            _refresh_daily_state(send_state)
            # procesar resultados pendientes
            try:
                while True:
                    event = result_queue.get_nowait()
                    action = _handle_event(
                        event,
                        success,
                        failed,
                        live_table,
                        remaining,
                        bump,
                        error_tracker,
                        account_error_streaks,
                        paused_runtime,
                        emit_log,
                        prompt,
                        no_dm_by_account=no_dm_by_account,
                        sent_unverified_by_account=sent_unverified_by_account,
                        overnight=overnight_mode,
                        request_stop_fn=_request_stop,
                    )
                    _on_event_processed(event)
                    if action == "stop":
                        break
            except queue.Empty:
                pass

            if STOP_EVENT.is_set():
                break

            for account in accounts:
                if STOP_EVENT.is_set():
                    break
                username = account["username"]
                if remaining[username] <= 0:
                    continue
                if not users:
                    break
                account_lock = account_locks[username]
                if not account_lock.acquire(blocking=False):
                    continue

                acquired = semaphore.acquire(timeout=0.1)
                if not acquired:
                    account_lock.release()
                    continue
                with inflight_lock:
                    over_limit = inflight >= active_limit
                if over_limit:
                    account_lock.release()
                    semaphore.release()
                    continue

                lead = users.popleft()
                send_index += 1
                template_entry = random.choice(templates)
                template_text = template_entry.get("text", "")
                template_name = template_entry.get("name", "")
                template_id = template_entry.get("id") or _template_id(template_name, template_text)
                candidates = _template_candidates(template_text)
                if not candidates:
                    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    event = SendEvent(
                        username=username,
                        lead=lead,
                        success=False,
                        detail="template_empty",
                        index=send_index,
                        total=total_target,
                        started_at=started_at,
                        duration_ms=0,
                        template_id=template_id,
                        template_name=template_name,
                        selected_variant="",
                        cancelled=False,
                        reason_code="template_empty",
                        reason_label="Plantilla vacia",
                        scope="template",
                    )
                    remaining[username] -= 1
                    _handle_event(
                        event,
                        success,
                        failed,
                        live_table,
                        remaining,
                        bump,
                        error_tracker,
                        account_error_streaks,
                        paused_runtime,
                        emit_log,
                        prompt,
                        no_dm_by_account=no_dm_by_account,
                        sent_unverified_by_account=sent_unverified_by_account,
                        overnight=overnight_mode,
                        request_stop_fn=_request_stop,
                    )
                    need_render = True
                    account_lock.release()
                    semaphore.release()
                    continue
                if len(candidates) == 1:
                    selected_variant = candidates[0]
                else:
                    try:
                        selected_variant, _idx = next_round_robin(username, template_id, candidates)
                    except Exception:
                        selected_variant = random.choice(candidates)
                selected_variant = selected_variant.strip()
                message = _render_message(
                    selected_variant,
                    lead=lead,
                    account=username,
                ).strip()
                if not message:
                    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    event = SendEvent(
                        username=username,
                        lead=lead,
                        success=False,
                        detail="template_selected_empty",
                        index=send_index,
                        total=total_target,
                        started_at=started_at,
                        duration_ms=0,
                        template_id=template_id,
                        template_name=template_name,
                        selected_variant=selected_variant,
                        cancelled=False,
                        reason_code="template_selected_empty",
                        reason_label="Plantilla vacia",
                        scope="template",
                    )
                    remaining[username] -= 1
                    _handle_event(
                        event,
                        success,
                        failed,
                        live_table,
                        remaining,
                        bump,
                        error_tracker,
                        account_error_streaks,
                        paused_runtime,
                        emit_log,
                        prompt,
                        no_dm_by_account=no_dm_by_account,
                        sent_unverified_by_account=sent_unverified_by_account,
                        overnight=overnight_mode,
                        request_stop_fn=_request_stop,
                    )
                    need_render = True
                    account_lock.release()
                    semaphore.release()
                    continue
                emit_log(username, lead, "escribiendo", "iniciando envio", None)
                remaining[username] -= 1
                live_table.begin(username, lead)
                with inflight_lock:
                    inflight += 1
                try:
                    executor.submit(
                        bind_stop_token_callable(campaign_token, _worker),
                        account,
                        lead,
                        message,
                        account_lock,
                        index=send_index,
                        total=total_target,
                        template_id=template_id,
                        template_name=template_name,
                        selected_variant=selected_variant,
                    )
                except Exception:
                    with inflight_lock:
                        if inflight > 0:
                            inflight -= 1
                    account_lock.release()
                    semaphore.release()
                    raise

                if STOP_EVENT.is_set():
                    break

            now = time.time()
            if now - last_render > 0.5:
                _render_progress(
                    alias,
                    len(users),
                    success,
                    failed,
                    live_table,
                    send_state,
                )
                last_render = now
            time.sleep(0.1)

        # drenar eventos restantes
        while True:
            try:
                event = result_queue.get(timeout=0.5)
                _handle_event(
                    event,
                    success,
                    failed,
                    live_table,
                    remaining,
                    bump,
                    error_tracker,
                    account_error_streaks,
                    paused_runtime,
                    emit_log,
                    prompt,
                    no_dm_by_account=no_dm_by_account,
                    sent_unverified_by_account=sent_unverified_by_account,
                    overnight=overnight_mode,
                    request_stop_fn=_request_stop,
                )
                _on_event_processed(event)
            except queue.Empty:
                break
        _render_progress(
            alias,
            len(users),
            success,
            failed,
            live_table,
            send_state,
        )

    except KeyboardInterrupt:
        _request_stop("interrupciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n con Ctrl+C")
    finally:
        completion_reason = "completed"
        if not users:
            completion_reason = "no quedan leads por procesar"
        elif not any(v > 0 for v in remaining.values()):
            completion_reason = "se alcanzÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­mite de envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­os por cuenta"

        executor.shutdown(wait=True, cancel_futures=False)

        while True:
            try:
                event = result_queue.get_nowait()
            except queue.Empty:
                break
            _handle_event(
                event,
                success,
                failed,
                live_table,
                remaining,
                bump,
                error_tracker,
                account_error_streaks,
                paused_runtime,
                emit_log,
                prompt,
                no_dm_by_account=no_dm_by_account,
                sent_unverified_by_account=sent_unverified_by_account,
                overnight=overnight_mode,
                request_stop_fn=_request_stop,
            )
            _on_event_processed(event)

        listener_shutdown_signal = False
        if listener:
            if not STOP_EVENT.is_set():
                # Solo para apagar el listener de teclado al finalizar la campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a.
                request_stop("finalizando listener de campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a")
                listener_shutdown_signal = True
            listener.join(timeout=0.2)
        if listener_shutdown_signal and not stop_requested:
            reset_stop_event()

        try:
            sender.close_all_sessions_sync()
        except Exception as exc:
            logger.warning("No se pudieron cerrar sesiones persistentes del sender: %s", exc)

        _reset_live_counters()
        _render_progress(
            alias,
            len(users),
            success,
            failed,
            live_table,
            send_state,
        )
        terminated = bool(stop_requested)
        final_reason = stop_reason or ("stop_event" if terminated else completion_reason)
        _log_runner_event(
            "stop",
            account="-",
            lead="-",
            status="stopped" if terminated else "completed",
            reason=(
                f"{final_reason} sent={max(0, sum(success.values()) - sum(sent_unverified_by_account.values()))} "
                f"sent_unverified={sum(sent_unverified_by_account.values())} "
                f"errors={sum(failed.values())} remaining={len(users)}"
            ),
        )
        if _CAMPAIGN_UI is not None:
            _CAMPAIGN_UI.stop()
            _CAMPAIGN_UI = None
        _print_final_summary(
            alias,
            accounts,
            success,
            failed,
            no_dm_by_account,
            sent_unverified_by_account,
        )

    total_unverified = sum(sent_unverified_by_account.values())
    total_ok = max(0, sum(success.values()) - total_unverified)
    emit_log(
        "-",
        "-",
        "resumen",
        f"OK confirmados: {total_ok} | sin verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n: {total_unverified}",
        None,
    )
    for account in accounts:
        user = account["username"]
        unverified = int(sent_unverified_by_account.get(user, 0))
        verified_ok = max(0, int(success[user]) - unverified)
        emit_log(
            user,
            "-",
            "resumen",
            f"{verified_ok} enviados confirmados, {failed[user]} errores, {unverified} sin verificaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n",
            None,
        )
    if STOP_EVENT.is_set():
        logger.info("Proceso detenido (%s).", "stop_event activo")
    if not overnight_mode:
        press_enter()
    restore_stop_token(stop_binding)


def classic_runner(
    concurrency_override: Optional[int] = None,
    *,
    overnight: Optional[bool] = None,
    headless: Optional[bool] = None,
) -> None:
    menu_send_rotating(
        concurrency_override=concurrency_override,
        overnight=overnight,
        headless=headless,
        use_proxy_workers=False,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Herramientas de envÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­o de mensajes para Instagram")
    parser.add_argument(
        "--concurrency",
        type=int,
        help="Cantidad de cuentas enviando en simultÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡neo (modo interactivo)",
    )
    parser.add_argument(
        "--csv",
        help="Ruta a un CSV con cuentas para procesar en paralelo",
    )
    parser.add_argument(
        "--lead",
        help="Usuario objetivo para el modo CSV",
    )
    parser.add_argument(
        "--message",
        help="Mensaje a enviar en el modo CSV",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Cantidad de cuentas a procesar por tanda (mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­nimo 10)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Mostrar el navegador durante el procesamiento del CSV",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Forzar headless en campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a",
    )
    parser.add_argument(
        "--overnight",
        action="store_true",
        help="Ejecutar campaÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±a en modo overnight (headless, sin prompts)",
    )
    args = parser.parse_args()

    if args.headed and args.headless:
        parser.error("--headed y --headless no se pueden usar juntos")

    headless_override = None
    if args.headed:
        headless_override = False
    elif args.headless:
        headless_override = True

    if args.csv:
        if not args.lead:
            parser.error("--lead es obligatorio cuando se usa --csv")
        if not args.message:
            parser.error("--message es obligatorio cuando se usa --csv")

        ensure_logging(
            quiet=SETTINGS.quiet,
            log_dir=SETTINGS.log_dir,
            log_file=SETTINGS.log_file,
        )

        print(
            style_text(
                "Procesando cuentas desde CSV en paralelo...",
                color=Fore.CYAN,
                bold=True,
            )
        )
        results = _send_messages_from_csv(
            args.csv,
            args.lead,
            args.message,
            batch_size=args.batch_size,
        )
        total = len(results)
        sent = sum(1 for item in results if item.status == "sent")
        failed = [item for item in results if item.status != "sent"]

        print()
        print(style_text(f"Total de cuentas procesadas: {total}", bold=True))
        print(style_text(f"Mensajes enviados con ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©xito: {sent}", color=Fore.GREEN if sent else Fore.WHITE, bold=True))
        if failed:
            print(style_text("Fallos detectados:", color=Fore.RED, bold=True))
            for item in failed:
                detail = item.error or "motivo no especificado"
                print(f" - @{item.username or 'desconocida'} ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ @{item.target}: {item.status} ({detail})")
        raise SystemExit(0)

    menu_send_rotating(
        concurrency_override=args.concurrency,
        overnight=args.overnight,
        headless=headless_override,
    )



