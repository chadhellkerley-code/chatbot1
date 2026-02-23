from __future__ import annotations

from collections import deque
from enum import Enum
import json
import logging
import re
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Optional

from PySide6.QtCore import QObject, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPixmap, QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QComboBox,
    QDialog,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from io_adapter import IOAdapter, InputRequest, MenuOption

DEBUG_UI_FLOW = True
USE_ENGINE_STATE_MANAGER = True

_ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_CLI_HINT_INLINE_RE = re.compile(
    r"\(?\s*(presiona|presione|presioná|pulsa)?\s*enter\s+para\s+volver[^)\n]*\)?",
    re.IGNORECASE,
)
_CLI_HINT_LINE_RE = re.compile(
    r"^\s*(presiona|presione|presioná|pulsa)?\s*\(?\s*enter\s+para\s+volver.*$",
    re.IGNORECASE,
)
_MENU_OPTION_LINE_RE = re.compile(r"^\s*\d{1,3}\)\s+.+$")
_DECORATIVE_LINE_RE = re.compile(r"^[=\-_*~\s]{3,}$")
_EXEC_EVENT_ROW_RE = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2})\s*\|\s*(?P<account>[^|]+?)\s*->\s*(?P<lead>[^|]+?)\s*\|\s*(?P<result>[^|]+?)(?:\s*\|\s*(?P<detail>.*))?$"
)
_EXEC_INFLIGHT_PIPE_ROW_RE = re.compile(
    r"^\s*(?P<account>[^|]+?)\s*\|\s*(?P<lead>[^|]+?)\s*\|\s*(?P<time>\d{2}:\d{2}:\d{2})\s*\|\s*(?P<result>[^|]+?)\s*\|\s*(?P<detail>.*)\s*$"
)
_EXEC_INFLIGHT_SPACED_ROW_RE = re.compile(
    r"^\s*(?P<account>\S+)\s{2,}(?P<lead>\S+)\s{2,}(?P<time>\d{2}:\d{2}:\d{2})\s{2,}(?P<result>\S+)\s{2,}(?P<detail>.+?)\s*$"
)
_EXEC_ALIAS_RE = re.compile(
    r"Alias:\s*(?P<alias>\S+)\s+Leads pendientes:\s*(?P<pending>\d+)",
    re.IGNORECASE,
)
_EXEC_META_RE = re.compile(
    r"Mensajes enviados:\s*(?P<sent>\d+).+?Mensajes con error:\s*(?P<error>\d+).+?Enviados sin verificaci[oó]n:\s*(?P<unverified>\d+).+?Saltados sin DM:\s*(?P<skipped>\d+).+?Concurrencia:\s*(?P<concurrency>\d+)",
    re.IGNORECASE,
)
_ACCOUNT_USERNAME_RE = re.compile(r"@([A-Za-z0-9._-]+)")
_ACCOUNT_REASON_LINE_RE = re.compile(
    r"^\s*-\s*@(?P<username>[A-Za-z0-9._-]+)\s*:\s*(?P<reason>.+?)\s*$"
)
_ACCOUNT_STATUS_LINE_RE = re.compile(
    r"@(?P<username>[A-Za-z0-9._-]+).*?\b(?P<status>ok|failed|error|skip(?:ped)?|omitid[oa]s?)\b",
    re.IGNORECASE,
)
_ACCOUNTS_RELOGIN_SUMMARY_RE = re.compile(
    r"\bok\s+(?P<ok>\d+)\s+failed\s+(?P<failed>\d+)\s+omitidas?\s+(?P<skipped>\d+)\b"
)
_ACCOUNTS_CSV_LOGIN_SUMMARY_RE = re.compile(
    r"\bok\s+(?P<ok>\d+)\s+need code\s+(?P<skipped>\d+)\s+failed\s+(?P<failed>\d+)\b"
)
_ACCOUNTS_MOVED_SUMMARY_RE = re.compile(r"\bse movieron\s+(?P<ok>\d+)\s+cuenta")
_ACCOUNTS_ADDED_SUMMARY_RE = re.compile(r"\bcuentas agregadas al alias\s+(?P<ok>\d+)\b")
_ACCOUNTS_RATIO_SUMMARY_RE = re.compile(r":\s*(?P<ok>\d+)\s*/\s*(?P<total>\d+)\s*$")
_AUTORESPONDER_ACTIVE_RE = re.compile(
    r"bot activo para .*\((?P<count>\d+)\s+cuentas?\)", re.IGNORECASE
)
_AUTORESPONDER_ACTIVE_ALT_RE = re.compile(
    r"auto-?responder activo para\s+(?P<count>\d+)\s+cuentas?", re.IGNORECASE
)
_AUTORESPONDER_DELAY_RE = re.compile(
    r"delay:\s*(?P<min>\d+(?:[.,]\d+)?)s(?:\s*-\s*(?P<max>\d+(?:[.,]\d+)?)s)?",
    re.IGNORECASE,
)
_AUTORESPONDER_RESPONSE_RE = re.compile(
    r"^respuesta\s+\d+\s*\|.*\|\s*(?P<status>ok|error)\s*$", re.IGNORECASE
)
_AUTORESPONDER_FOLLOWUP_OK_RE = re.compile(
    r"^seguimiento\s*\|\s*@", re.IGNORECASE
)
_AUTORESPONDER_FOLLOWUP_ATTEMPTS_RE = re.compile(
    r"follow-?ups?\s+intentados:\s*(?P<value>\d+)", re.IGNORECASE
)
_AUTORESPONDER_FOLLOWUP_SENT_RE = re.compile(
    r"follow-?ups?\s+enviados:\s*(?P<value>\d+)", re.IGNORECASE
)
_AUTORESPONDER_TRACE_RE = re.compile(
    r"^(trace_|\[trace_id|\[trace_)", re.IGNORECASE
)
_LEADS_RESULT_RE = re.compile(
    r"^@(?P<lead>[^|]+?)\s*\|\s*@(?P<account>[^|]+?)\s*\|\s*(?P<result>CALIFICA|NO CALIFICA)\s*\|\s*(?P<detail>.*)$",
    re.IGNORECASE,
)
_LEADS_SUMMARY_ROW_RE = re.compile(
    r"total\s*[=:]\s*(?P<total>\d+)\s+procesados\s*[=:]\s*(?P<processed>\d+)\s+calificados\s*[=:]\s*(?P<qualified>\d+)\s+descartados\s*[=:]\s*(?P<discarded>\d+)",
    re.IGNORECASE,
)
_SEND_PROMPT_INT_DEFAULT_RE = re.compile(r"\[(?P<value>\d+)\]")
_SEND_PROMPT_DELAY_MAX_DEFAULT_RE = re.compile(
    r"por\s+defecto\s*(?P<value>\d+)",
    re.IGNORECASE,
)
_LEADS_USERNAMES_LOADED_RE = re.compile(
    r"usernames\s+cargados\s*:?\s*(?P<count>\d+)",
    re.IGNORECASE,
)
_LEADS_EXPORT_ALIAS_RE = re.compile(
    r"leads guardados en alias\s+['\"](?P<alias>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_SUMMARY_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}:\d{2}\b")
_SUMMARY_PER_ACCOUNT_RE = re.compile(
    r"@?(?P<user>[A-Za-z0-9._-]+)\s*(?:->|=>|:)\s*(?P<time>\d{1,2}:\d{2}:\d{2})"
)


def _clean_log_chunk(text: str) -> str:
    text = text.replace("\r", "")
    return _ANSI_ESCAPE_RE.sub("", text)


def _strip_cli_hints(text: str) -> str:
    cleaned = _CLI_HINT_INLINE_RE.sub("", text)
    cleaned = cleaned.replace("()", "")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _build_brand_logo_pixmap(size: int) -> QPixmap:
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing, True)
    painter.setPen(Qt.NoPen)
    painter.setBrush(QColor("#0b1220"))
    painter.drawRoundedRect(0, 0, size, size, 5, 5)

    font = QFont("Consolas")
    font.setBold(True)
    font.setPixelSize(max(9, int(size * 0.55)))
    painter.setFont(font)

    baseline_y = int(size * 0.72)
    painter.setPen(QColor("#ffffff"))
    painter.drawText(int(size * 0.18), baseline_y, ">")
    painter.setPen(QColor("#2563eb"))
    painter.drawText(int(size * 0.45), baseline_y, "_")
    painter.end()
    return pixmap


def _build_brand_icon() -> QIcon:
    icon = QIcon()
    for size in (16, 20, 24, 32, 48):
        icon.addPixmap(_build_brand_logo_pixmap(size))
    return icon


def _normalize_username(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lstrip("@").lower()


def _normalized_label(value: str) -> str:
    lower = (
        value.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
        .replace("Ã¡", "a")
        .replace("Ã©", "e")
        .replace("Ã­", "i")
        .replace("Ã³", "o")
        .replace("Ãº", "u")
        .replace("Ã¼", "u")
        .replace("Ã±", "n")
    )
    lower = _NON_ALNUM_RE.sub(" ", lower)
    return " ".join(lower.split())


class EngineState(str, Enum):
    IDLE = "IDLE"
    VALIDATING_SESSIONS = "VALIDATING_SESSIONS"
    AWAITING_USER_DECISION = "AWAITING_USER_DECISION"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class EngineStateManager(QObject):
    state_changed = Signal(str, str, str)
    event_handled = Signal(str, str)

    _ALLOWED_TRANSITIONS: dict[EngineState, set[EngineState]] = {
        EngineState.IDLE: {
            EngineState.VALIDATING_SESSIONS,
            EngineState.RUNNING,
            EngineState.ERROR,
        },
        EngineState.VALIDATING_SESSIONS: {
            EngineState.AWAITING_USER_DECISION,
            EngineState.RUNNING,
            EngineState.STOPPING,
            EngineState.FINISHED,
            EngineState.ERROR,
            EngineState.IDLE,
        },
        EngineState.AWAITING_USER_DECISION: {
            EngineState.VALIDATING_SESSIONS,
            EngineState.RUNNING,
            EngineState.STOPPING,
            EngineState.ERROR,
            EngineState.IDLE,
        },
        EngineState.RUNNING: {
            EngineState.STOPPING,
            EngineState.FINISHED,
            EngineState.ERROR,
            EngineState.IDLE,
        },
        EngineState.STOPPING: {
            EngineState.FINISHED,
            EngineState.ERROR,
            EngineState.IDLE,
        },
        EngineState.FINISHED: {
            EngineState.IDLE,
            EngineState.VALIDATING_SESSIONS,
            EngineState.ERROR,
        },
        EngineState.ERROR: {
            EngineState.IDLE,
            EngineState.VALIDATING_SESSIONS,
        },
    }

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self.current_state = EngineState.IDLE

    def set_state(self, new_state: EngineState | str, *, reason: str = "manual") -> bool:
        try:
            target = (
                new_state
                if isinstance(new_state, EngineState)
                else EngineState(str(new_state).strip().upper())
            )
        except Exception:
            return False

        previous = self.current_state
        if target == previous:
            return False

        allowed = self._ALLOWED_TRANSITIONS.get(previous, set())
        if target not in allowed:
            return False

        self.current_state = target
        self.state_changed.emit(previous.value, target.value, reason)
        return True

    def handle_backend_event(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        raw_event_type = event.get("type")
        event_type = str(raw_event_type or "").strip().upper()
        if not event_type:
            return

        next_state: Optional[EngineState] = None
        if event_type in {
            "SEND_CONFIG_SUBMITTED",
            "INPUT_REQUEST_SEND_ALIAS",
            "INPUT_REQUEST_SEND_LEADS",
            "INPUT_REQUEST_SEND_PER_ACCOUNT",
            "INPUT_REQUEST_SEND_CONCURRENCY",
            "INPUT_REQUEST_SEND_DELAY_MIN",
            "INPUT_REQUEST_SEND_DELAY_MAX",
            "INPUT_REQUEST_SEND_TEMPLATE_MODE",
            "INPUT_REQUEST_SEND_TEMPLATE_PICK",
            "INPUT_REQUEST_SEND_MANUAL_MESSAGE",
        }:
            next_state = EngineState.VALIDATING_SESSIONS
        elif event_type in {
            "LOW_PROFILE_DETECTED",
            "LOW_PROFILE_DECISION_REQUEST",
            "SESSION_ISSUES_DETECTED",
            "SESSION_LOGIN_DECISION_REQUEST",
            "PARTIAL_LOGIN_DECISION_REQUEST",
        }:
            next_state = EngineState.AWAITING_USER_DECISION
        elif event_type == "DECISION_SUBMITTED":
            next_state = EngineState.VALIDATING_SESSIONS
        elif event_type in {"CAMPAIGN_RUNNING", "CAMPAIGN_STARTED"}:
            next_state = EngineState.RUNNING
        elif event_type in {"STOP_REQUESTED", "STOPPING_REQUESTED"}:
            next_state = EngineState.STOPPING
        elif event_type in {"CAMPAIGN_FINISHED", "CAMPAIGN_SUMMARY_DETECTED"}:
            next_state = EngineState.FINISHED
        elif event_type in {"LOGIN_TOTAL_FAILURE", "BACKEND_ERROR"}:
            next_state = EngineState.ERROR
        elif event_type in {"MAIN_MENU_READY", "SUMMARY_CLOSED", "RESET_IDLE", "BACKEND_DONE"}:
            next_state = EngineState.IDLE

        if next_state is not None:
            self.set_state(next_state, reason=event_type)
        self.event_handled.emit(event_type, self.current_state.value)


class ClickableMetricCard(QFrame):
    clicked = Signal()

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.LeftButton:
            self.clicked.emit()
        super().mouseReleaseEvent(event)


class AutoResponderSummaryDialog(QDialog):
    def __init__(self, summary: dict[str, Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("AutoResponderSummaryDialog")
        self.setWindowTitle("Resumen final — Auto-Responder")
        self.setModal(True)
        self.setMinimumWidth(640)
        self.setFont(QFont("Segoe UI Emoji", 10))
        self.setStyleSheet(
            """
            QDialog#AutoResponderSummaryDialog {
                background-color: #0b1220;
                color: #e2e8f0;
                border: 1px solid #1e293b;
                border-radius: 12px;
            }
            QLabel#AutoResponderSummaryTitle {
                font-size: 21px;
                font-weight: 700;
                color: #f8fafc;
            }
            QLabel#AutoResponderSummaryBody {
                font-size: 14px;
                color: #cbd5e1;
            }
            QPlainTextEdit#AutoResponderSummaryAccounts {
                background-color: #111827;
                border: 1px solid #334155;
                border-radius: 8px;
                color: #e2e8f0;
                padding: 8px;
            }
            QPushButton#AutoResponderSummaryAccept {
                background-color: #2563eb;
                color: #ffffff;
                border: none;
                border-radius: 8px;
                padding: 8px 18px;
                font-weight: 700;
            }
            QPushButton#AutoResponderSummaryAccept:hover {
                background-color: #1d4ed8;
            }
            """
        )

        def _text_or_dash(value: Any) -> str:
            text = str(value).strip() if value is not None else ""
            return text if text else "—"

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        title = QLabel("✅ Resumen final — Auto-Responder")
        title.setObjectName("AutoResponderSummaryTitle")
        title.setAlignment(Qt.AlignCenter)

        alias_line = QLabel(f"Alias: {_text_or_dash(summary.get('alias'))}")
        alias_line.setObjectName("AutoResponderSummaryBody")

        totals_lines = [
            f"Cuentas usadas: {_text_or_dash(summary.get('accounts_used'))}",
            f"Respuestas intentadas: {_text_or_dash(summary.get('replies_attempted'))}",
            f"Respuestas enviadas: {_text_or_dash(summary.get('replies_sent'))}",
            f"Follow-up intentados: {_text_or_dash(summary.get('followups_attempted'))}",
            f"Follow-up enviados: {_text_or_dash(summary.get('followups_sent'))}",
            f"Errores: {_text_or_dash(summary.get('errors'))}",
        ]
        totals_label = QLabel("\n".join(totals_lines))
        totals_label.setObjectName("AutoResponderSummaryBody")
        totals_label.setWordWrap(True)

        times_lines = [f"Tiempo total: {_text_or_dash(summary.get('total_time'))}", "Tiempo por cuenta:"]
        times_label = QLabel("\n".join(times_lines))
        times_label.setObjectName("AutoResponderSummaryBody")
        times_label.setWordWrap(True)

        per_account = summary.get("per_account_time") or []
        per_account_lines: list[str] = []
        if isinstance(per_account, list):
            for row in per_account:
                if not isinstance(row, (tuple, list)) or len(row) < 2:
                    continue
                user_text = str(row[0] or "").strip()
                user_text = user_text if user_text.startswith("@") else f"@{user_text}" if user_text else "@—"
                per_account_lines.append(f"{user_text} → {str(row[1] or '—').strip() or '—'}")
        if not per_account_lines:
            per_account_lines = ["—"]

        per_account_box = QPlainTextEdit()
        per_account_box.setObjectName("AutoResponderSummaryAccounts")
        per_account_box.setReadOnly(True)
        per_account_box.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        per_account_box.setMaximumBlockCount(800)
        per_account_box.setMinimumHeight(140)
        per_account_box.setPlainText("\n".join(per_account_lines))

        accept_button = QPushButton("Aceptar")
        accept_button.setObjectName("AutoResponderSummaryAccept")
        accept_button.clicked.connect(self.accept)

        layout.addWidget(title)
        layout.addWidget(alias_line)
        layout.addWidget(totals_label)
        layout.addWidget(times_label)
        layout.addWidget(per_account_box)
        layout.addWidget(accept_button, 0, Qt.AlignCenter)


class MainWindow(QMainWindow):
    backend_done = Signal(int)
    backend_failed = Signal(str)
    backend_log = Signal(str)

    PAGE_DASHBOARD = 0
    PAGE_MENU = 1
    PAGE_INPUT = 2
    PAGE_EXECUTION = 3

    _PRIMARY_ITEMS = [
        ("dashboard", "Dashboard"),
        ("accounts", "Gestionar cuentas de Instagram"),
        ("leads", "Gestionar leads"),
        ("send", "Enviar mensajes"),
        ("logs", "Ver registros de envios"),
        ("autoresponder", "Auto-responder con OpenAI"),
        ("stats", "Estadisticas y metricas"),
        ("whatsapp", "Automatizacion por WhatsApp"),
        ("deliver", "Entregar a cliente"),
        ("updates", "Actualizaciones"),
        ("exit", "Salir"),
    ]
    _SECTION_LOG_KEYS = (
        "leads",
        "logs",
        "stats",
        "whatsapp",
        "deliver",
        "updates",
    )

    def __init__(self, mode: str = "owner") -> None:
        super().__init__()
        self._mode = mode
        self._root_dir = Path(__file__).resolve().parent

        self.setWindowTitle("INSTA CLI – PROPIEDAD DE MATIDIAZLIFE")
        brand_icon = _build_brand_icon()
        self.setWindowIcon(brand_icon)
        app = QApplication.instance()
        if app is not None:
            app.setWindowIcon(brand_icon)
        self.resize(1320, 860)
        self.setAcceptDrops(True)

        self._io_adapter: Optional[IOAdapter] = None
        self._backend_thread: Optional[threading.Thread] = None
        self._pending_request: Optional[InputRequest] = None
        self._pending_request_is_primary_menu = False
        self._queued_primary_key: Optional[str] = None
        self._closing = False
        self._shutdown_started = False
        self._shutdown_reason = ""
        self._shutdown_requested_from_primary_exit = False
        self.backend_exit_code: Optional[int] = None
        self._execution_mode_active = False
        self._execution_log_times: list[float] = []
        self._exec_inflight_rows: dict[str, int] = {}
        self._campaign_running = False
        self._campaign_active = False
        self._campaign_start_time = 0.0
        self._showing_summary = False
        self._block_navigation = False
        self._campaign_summary_detected = False
        self._campaign_account_stats: dict[str, dict[str, int]] = {}
        self._campaign_summary_capture_active = False
        self._campaign_summary_alias = "-"
        self._campaign_summary_accounts: dict[str, dict[str, int]] = {}
        self._campaign_summary_totals = {"ok": 0, "err": 0, "no_dm": 0, "unver": 0, "total": 0}
        self._capture_low_profile_list = False
        self._capture_session_list = False
        self._send_low_profile_accounts: list[tuple[str, str]] = []
        self._send_session_issue_accounts: list[tuple[str, str]] = []
        self._send_login_total_failure = False
        self._autoresponder_running = False
        self._autoresponder_started_at = 0.0
        self._autoresponder_active_accounts = 0
        self._autoresponder_responses_ok = 0
        self._autoresponder_responses_error = 0
        self._autoresponder_followups_ok = 0
        self._autoresponder_followups_error = 0
        self._autoresponder_followups_attempted: Optional[int] = None
        self._autoresponder_delay_min_s: Optional[float] = None
        self._autoresponder_delay_max_s: Optional[float] = None
        self._inflight_parse_window_until = 0.0
        self._leads_filter_running = False
        self._leads_filter_started_at = 0.0
        self._leads_total_target: Optional[int] = None
        self._leads_processed_count = 0
        self._leads_qualified_count = 0
        self._leads_discarded_count = 0
        self._leads_account_alias = "-"
        self._leads_export_alias = "leads_filtrados"
        self._leads_accounts_planned: Optional[int] = None
        self._leads_account_usernames: list[str] = []
        self._leads_waiting_manual_accounts = False
        self._leads_accounts_seen: set[str] = set()
        self._leads_counts_prefill_on_next_start: Optional[dict[str, int]] = None
        self._leads_stop_requested = False
        self._leads_stop_prompt_active = False
        self._leads_completion_announced = False

        self._metric_values: dict[str, QLabel] = {}
        self._booked_today_rows: list[dict[str, Any]] = []
        self._replied_today_rows: list[dict[str, Any]] = []
        self._top_responded_message_rows: list[dict[str, Any]] = []
        self._menu_option_buttons: list[QPushButton] = []
        self._sidebar_buttons: dict[str, QPushButton] = {}
        self._active_sidebar_key = "dashboard"
        self._log_expanded = False
        self._accounts_list_callable: Optional[Callable[[], Any]] = None
        self._accounts_connected_callable: Optional[Callable[..., Any]] = None
        self._accounts_lookup_failed = False
        self._accounts_operation_active = False
        self._accounts_operation_name = ""
        self._accounts_operation_started_at = 0.0
        self._accounts_result_by_user: dict[str, str] = {}
        self._accounts_result_counts = {"success": 0, "failed": 0, "skipped": 0}
        self._accounts_summary_override: Optional[dict[str, int]] = None
        self._section_log_buffers: dict[str, deque[str]] = {
            key: deque(maxlen=260) for key in self._SECTION_LOG_KEYS
        }
        self._section_log_scope: Optional[str] = None
        self._menu_log_displayed_key: Optional[str] = None
        self._menu_activity_open = False

        self._primary_option_values = self._default_primary_option_values()
        self._primary_label_by_key = {key: label for key, label in self._PRIMARY_ITEMS}

        self._status_value = QLabel("Idle")
        self._thread_value = QLabel("-")
        self._last_prompt_value = QLabel("-")
        self._last_prompt_value.setWordWrap(True)
        self._dashboard_updated_value = QLabel("-")
        self._selected_accounts_alias = ""
        self._accounts_alias_select_value = ""
        self._accounts_alias_back_value = ""
        self._accounts_alias_manual_mode = False
        self._send_setup_prompt_kind = ""
        self._send_setup_prompt_text = ""
        self._send_setup_use_saved_templates = False
        self._auto_fill_active = False
        self._auto_fill_queue: list[str] = []
        self._autoresponder_activate_option_value = ""
        self._autoresponder_setup_auto_fill_active = False
        self._autoresponder_setup_queue: list[str] = []
        self._autoresponder_selected_accounts: list[str] = []
        self._autoresponder_selected_hours: set[int] = {4, 8, 12, 24}
        self._autoresponder_followup_only = False
        self._autoresponder_target_index_by_key: dict[str, int] = {}
        self._autoresponder_loading_overlay_active = False
        self._autoresponder_summary_expected = False
        self._autoresponder_summary_capture_active = False
        self._autoresponder_summary_modal_shown = False
        self._autoresponder_summary_lines: deque[str] = deque(maxlen=260)
        self._autoresponder_last_summary: dict[str, Any] = {}
        self._backend_recent_lines: deque[str] = deque(maxlen=420)
        self._engine_state_manager: Optional[EngineStateManager] = None
        if USE_ENGINE_STATE_MANAGER:
            self._engine_state_manager = EngineStateManager(self)
            self._engine_state_manager.state_changed.connect(self._on_engine_state_changed)

        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_dashboard_page())
        self._stack.addWidget(self._build_menu_page())
        self._stack.addWidget(self._build_input_page())
        self._stack.addWidget(self._build_execution_page())

        self._log_console = QPlainTextEdit()
        self._log_console.setObjectName("LogConsole")
        self._log_console.setReadOnly(True)
        self._log_console.setMaximumBlockCount(0)
        self._log_console.setLineWrapMode(QPlainTextEdit.NoWrap)

        self._build_layout()
        self._set_page(self.PAGE_DASHBOARD)
        self._set_active_sidebar("dashboard")
        self._set_status("Idle")
        self._apply_log_panel_state()
        self._update_primary_button_visibility()
        self._emit_engine_event("RESET_IDLE")

        self.backend_done.connect(self._on_backend_done)
        self.backend_failed.connect(self._on_backend_failed)
        self.backend_log.connect(self._append_log)

        self._dashboard_timer = QTimer(self)
        self._dashboard_timer.setInterval(15000)
        self._dashboard_timer.timeout.connect(self.refresh_dashboard_metrics)
        self._dashboard_timer.start()
        self.refresh_dashboard_metrics()

        self._exec_clock_timer = QTimer(self)
        self._exec_clock_timer.setInterval(1000)
        self._exec_clock_timer.timeout.connect(self._tick_execution_clock)
        self._exec_clock_timer.start()

    def bind_io_adapter(self, adapter: IOAdapter) -> None:
        self._io_adapter = adapter
        adapter.log_chunk.connect(self._append_log)
        adapter.input_requested.connect(self._handle_input_request)
        adapter.menu_detected.connect(self._preview_menu_detected)

    def start_backend(self, backend_entrypoint: Callable[[], None]) -> None:
        if self._shutdown_started or self._closing:
            self._append_log("[gui] backend start skipped: shutdown in progress.\n")
            return
        if self._backend_thread and self._backend_thread.is_alive():
            return

        self._set_status("Running")
        self._thread_value.setText("cli-backend")
        self._append_log(f"[gui] Mode={self._mode} backend thread started.\n")

        self._backend_thread = threading.Thread(
            target=self._backend_runner,
            args=(backend_entrypoint,),
            name="cli-backend",
            daemon=True,
        )
        self._backend_thread.start()

    def _default_primary_option_values(self) -> dict[str, str]:
        if self._mode == "client":
            return {
                "accounts": "1",
                "leads": "2",
                "send": "3",
                "logs": "4",
                "autoresponder": "5",
                "stats": "6",
                "whatsapp": "7",
                "updates": "8",
                "exit": "9",
            }
        return {
            "accounts": "1",
            "leads": "2",
            "send": "3",
            "logs": "4",
            "autoresponder": "5",
            "stats": "6",
            "whatsapp": "7",
            "deliver": "8",
            "updates": "9",
            "exit": "10",
        }

    def _build_layout(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)

        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        sidebar = self._build_sidebar()
        layout.addWidget(sidebar)

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(16, 16, 16, 12)
        content_layout.setSpacing(12)
        content_layout.addWidget(self._build_live_input_bar(), 0)
        self._central_scroll = QScrollArea()
        self._central_scroll.setObjectName("SubmenuScroll")
        self._central_scroll.setWidgetResizable(True)
        self._central_scroll.setFrameShape(QFrame.NoFrame)
        self._central_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._central_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._central_scroll.setWidget(self._stack)
        content_layout.addWidget(self._central_scroll, 1)
        self._build_send_loading_overlay()
        content_layout.addWidget(self._build_log_panel(), 0)
        layout.addWidget(content, 1)

    def _build_send_loading_overlay(self) -> None:
        viewport = self._central_scroll.viewport()
        self._send_loading_overlay = QWidget(viewport)
        self._send_loading_overlay.setObjectName("SendLoadingOverlay")
        overlay_layout = QVBoxLayout(self._send_loading_overlay)
        overlay_layout.setContentsMargins(0, 0, 0, 0)
        overlay_layout.setSpacing(0)

        center_wrap = QWidget()
        center_layout = QVBoxLayout(center_wrap)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(12)

        card = QFrame()
        card.setObjectName("SendLoadingCard")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(20, 18, 20, 18)
        card_layout.setSpacing(10)

        spinner = QProgressBar()
        spinner.setObjectName("SendLoadingSpinner")
        spinner.setRange(0, 0)
        spinner.setTextVisible(False)
        spinner.setFixedWidth(190)
        self._shared_loading_spinner = spinner

        label = QLabel("Cargando...")
        label.setObjectName("SendLoadingLabel")
        label.setAlignment(Qt.AlignCenter)
        self._shared_loading_title_label = label

        detail_label = QLabel("")
        detail_label.setObjectName("MutedText")
        detail_label.setAlignment(Qt.AlignCenter)
        detail_label.setWordWrap(True)
        detail_label.hide()
        self._shared_loading_detail_label = detail_label

        card_layout.addWidget(spinner, 0, Qt.AlignCenter)
        card_layout.addWidget(label, 0, Qt.AlignCenter)
        card_layout.addWidget(detail_label, 0, Qt.AlignCenter)
        center_layout.addWidget(card, 0, Qt.AlignCenter)

        overlay_layout.addStretch(1)
        overlay_layout.addWidget(center_wrap, 0, Qt.AlignCenter)
        overlay_layout.addStretch(1)
        self._send_loading_overlay.hide()

    def _sync_send_loading_overlay_geometry(self) -> None:
        if not hasattr(self, "_send_loading_overlay") or not hasattr(self, "_central_scroll"):
            return
        viewport = self._central_scroll.viewport()
        self._send_loading_overlay.setGeometry(viewport.rect())

    def _show_send_loading_overlay(self) -> None:
        self._show_shared_loading_overlay(
            title="Cargando...",
            detail_text="",
            autoresponder_mode=False,
        )

    def _show_autoresponder_loading_overlay(self) -> None:
        self._show_shared_loading_overlay(
            title="🤖 Iniciando Auto Responder…",
            detail_text=(
                "Verificando sesiones\n"
                "Preparando threads\n"
                "Inicializando motor OpenAI"
            ),
            autoresponder_mode=True,
        )

    def _show_shared_loading_overlay(
        self,
        *,
        title: str,
        detail_text: str,
        autoresponder_mode: bool,
    ) -> None:
        if not hasattr(self, "_send_loading_overlay"):
            return
        self._autoresponder_loading_overlay_active = bool(autoresponder_mode)
        if hasattr(self, "_shared_loading_title_label"):
            self._shared_loading_title_label.setText(str(title or "").strip() or "Cargando...")
        if hasattr(self, "_shared_loading_detail_label"):
            clean_detail = str(detail_text or "").strip()
            self._shared_loading_detail_label.setText(clean_detail)
            self._shared_loading_detail_label.setVisible(bool(clean_detail))
        self._sync_send_loading_overlay_geometry()
        self._send_loading_overlay.raise_()
        self._send_loading_overlay.show()

    def _hide_autoresponder_loading_overlay(self) -> None:
        if not self._autoresponder_loading_overlay_active:
            return
        self._hide_send_loading_overlay()

    def _hide_send_loading_overlay(self) -> None:
        if not hasattr(self, "_send_loading_overlay"):
            return
        self._autoresponder_loading_overlay_active = False
        if hasattr(self, "_shared_loading_title_label"):
            self._shared_loading_title_label.setText("Cargando...")
        if hasattr(self, "_shared_loading_detail_label"):
            self._shared_loading_detail_label.setText("")
            self._shared_loading_detail_label.hide()
        self._send_loading_overlay.hide()

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._sync_send_loading_overlay_geometry()

    def _build_live_input_bar(self) -> QWidget:
        bar = QFrame()
        bar.setObjectName("LiveInputBar")
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(8)

        self._live_input_prompt = QLabel("Entrada global")
        self._live_input_prompt.setObjectName("MutedText")
        self._live_input_prompt.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Preferred)

        self._live_input_field = QLineEdit()
        self._live_input_field.setObjectName("LiveInputField")
        self._live_input_field.setPlaceholderText("Escribe y presiona Enter (input literal)")
        self._live_input_field.returnPressed.connect(self._submit_live_input)
        self._live_input_field.textChanged.connect(self._on_live_input_text_changed)

        self._live_input_submit = QPushButton("Enviar")
        self._live_input_submit.setObjectName("PrimaryButton")
        self._live_input_submit.clicked.connect(self._submit_live_input)

        self._live_input_empty = QPushButton("Enviar vacio")
        self._live_input_empty.setObjectName("SecondaryButton")
        self._live_input_empty.clicked.connect(lambda: self._submit_current_input(""))

        layout.addWidget(self._live_input_prompt)
        layout.addWidget(self._live_input_field, 1)
        layout.addWidget(self._live_input_submit)
        layout.addWidget(self._live_input_empty)

        self._set_live_input_enabled(False, prompt="")
        return bar

    def _build_sidebar(self) -> QWidget:
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(260)

        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        sidebar_scroll = QScrollArea()
        sidebar_scroll.setObjectName("SidebarScroll")
        sidebar_scroll.setWidgetResizable(True)
        sidebar_scroll.setFrameShape(QFrame.NoFrame)
        sidebar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        sidebar_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        layout.addWidget(sidebar_scroll, 1)

        sidebar_content = QWidget()
        sidebar_content.setObjectName("SidebarScrollContent")
        sidebar_content.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        content_layout = QVBoxLayout(sidebar_content)
        content_layout.setContentsMargins(12, 12, 12, 12)
        content_layout.setSpacing(10)
        sidebar_scroll.setWidget(sidebar_content)

        header = QFrame()
        header.setObjectName("SidebarHeader")
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(8, 8, 8, 8)
        header_layout.setSpacing(2)

        brand_row = QHBoxLayout()
        brand_row.setContentsMargins(0, 0, 0, 0)
        brand_row.setSpacing(8)

        brand_logo = QLabel(
            "<span style='color:#ffffff;'>&gt;</span><span style='color:#2563eb;'>_</span>"
        )
        brand_logo.setObjectName("BrandHeaderLogo")
        brand_logo.setAlignment(Qt.AlignCenter)
        brand_logo.setFixedSize(24, 24)
        brand_logo.setStyleSheet(
            "background-color: #0b1220;"
            "border: 1px solid #243246;"
            "border-radius: 6px;"
            "font-size: 12px;"
            "font-weight: 700;"
        )

        brand_main = QLabel("INSTA CLI")
        brand_main.setObjectName("BrandHeaderMain")
        brand_sub = QLabel("– PROPIEDAD DE MATIDIAZLIFE")
        brand_sub.setObjectName("BrandHeaderSub")
        mode_badge = QLabel(f"MODO {self._mode.upper()}")
        mode_badge.setObjectName("ModeBadge")

        brand_row.addWidget(brand_logo, 0, Qt.AlignVCenter)
        brand_row.addWidget(brand_main)
        header_layout.addLayout(brand_row)
        header_layout.addWidget(brand_sub)
        header_layout.addSpacing(4)
        header_layout.addWidget(mode_badge)
        content_layout.addWidget(header)

        menu_container = QWidget()
        menu_container.setObjectName("SidebarMenuContainer")
        menu_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        menu_layout = QVBoxLayout(menu_container)
        menu_layout.setContentsMargins(0, 0, 0, 0)
        menu_layout.setSpacing(6)

        for key, label in self._PRIMARY_ITEMS:
            button = QPushButton(label)
            button.setObjectName("SidebarMenuButton")
            button.clicked.connect(lambda checked=False, k=key: self._on_sidebar_item_clicked(k))
            menu_layout.addWidget(button)
            self._sidebar_buttons[key] = button

        content_layout.addWidget(menu_container)

        status_card = QFrame()
        status_card.setObjectName("StatusCard")
        status_layout = QVBoxLayout(status_card)
        status_layout.setContentsMargins(8, 8, 8, 8)
        status_layout.setSpacing(6)
        status_layout.addWidget(self._status_row("Backend", self._status_value))
        status_layout.addWidget(self._status_row("Thread", self._thread_value))
        status_layout.addWidget(self._status_row("Prompt", self._last_prompt_value))
        content_layout.addWidget(status_card)
        content_layout.addStretch(1)

        return sidebar

    def _status_row(self, key: str, value_widget: QWidget) -> QWidget:
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        key_label = QLabel(key)
        key_label.setObjectName("StatusKey")
        key_label.setMinimumWidth(62)

        value_widget.setObjectName("StatusValue")
        value_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        layout.addWidget(key_label)
        layout.addWidget(value_widget, 1)
        return row

    def _set_accounts_users_preview(self, text: str) -> None:
        value = str(text).strip()
        self._accounts_users_label.setText(value)
        self._accounts_users_label.setVisible(bool(value))

    def _resolve_selected_accounts_alias(self, request: Optional[InputRequest] = None) -> str:
        alias = str(self._selected_accounts_alias).strip()
        if alias and alias != "(vacio)":
            return alias
        if request is not None:
            prompt_text = _strip_cli_hints(request.prompt or "")
            match = re.search(
                r"cuentas?\s+del\s+alias\s*:\s*([A-Za-z0-9._-]+)",
                prompt_text,
                re.IGNORECASE,
            )
            if match:
                candidate = str(match.group(1)).strip()
                if candidate:
                    return candidate
        return "default"

    def _accounts_alias_users_text(self, alias: str) -> str:
        selected_alias = str(alias or "default").strip() or "default"
        alias_key = selected_alias.lower()
        records = self._load_account_records({})
        connected_callable = None
        session_label_callable = None
        badge_callable = None
        life_badge_callable = None
        try:
            accounts_module = import_module("accounts")
            candidate = getattr(accounts_module, "connected_status", None)
            connected_callable = candidate if callable(candidate) else None
            candidate = getattr(accounts_module, "_session_label", None)
            session_label_callable = candidate if callable(candidate) else None
            candidate = getattr(accounts_module, "_badge_for_display", None)
            badge_callable = candidate if callable(candidate) else None
            candidate = getattr(accounts_module, "_life_status_badge", None)
            life_badge_callable = candidate if callable(candidate) else None
        except Exception:
            pass
        users: list[str] = []
        seen: set[str] = set()
        for record in records:
            if not isinstance(record, dict):
                continue
            record_alias = str(record.get("alias") or "default").strip() or "default"
            if record_alias.lower() != alias_key:
                continue
            username = _normalize_username(record.get("username"))
            if not username or username in seen:
                continue
            seen.add(username)
            state = "activa" if bool(record.get("active", True)) else "inactiva"
            connected = bool(record.get("connected", False))
            if callable(connected_callable):
                try:
                    connected = bool(
                        connected_callable(
                            record,
                            strict=False,
                            reason="ui-alias-preview",
                            fast=True,
                            persist=False,
                        )
                    )
                except Exception:
                    connected = bool(record.get("connected", False))
            connection_state = "conectada" if connected else "no conectada"

            session_state = "sesion" if connected else "sin sesion"
            if callable(session_label_callable):
                try:
                    raw_session = str(session_label_callable(username) or "")
                except Exception:
                    raw_session = ""
                normalized_session = _normalized_label(raw_session)
                if "sin sesion" in normalized_session:
                    session_state = "sin sesion"
                elif "sesion" in normalized_session:
                    session_state = "sesion"

            life_state = "DESCONOCIDA"
            if callable(badge_callable) and callable(life_badge_callable):
                try:
                    badge, _ = badge_callable(record)
                    raw_life = str(life_badge_callable(record, badge) or "")
                    normalized_life = _normalized_label(raw_life)
                    if "bloqueada" in normalized_life:
                        life_state = "BLOQUEADA"
                    elif "viva" in normalized_life:
                        life_state = "VIVA"
                    elif "en riesgo" in normalized_life:
                        life_state = "EN RIESGO"
                    elif "sin sesion" in normalized_life:
                        life_state = "SIN SESION"
                    elif "verificando" in normalized_life:
                        life_state = "VERIFICANDO"
                except Exception:
                    life_state = "DESCONOCIDA"

            users.append(
                f"- @{username} [{state}] [{connection_state}] [{session_state}] [{life_state}]"
            )
        if not users:
            return f"Cuentas del alias: {selected_alias}\n(no hay cuentas)"
        return f"Cuentas del alias: {selected_alias}\n" + "\n".join(users)

    def _extract_accounts_alias_selector_values(
        self, options: list[MenuOption]
    ) -> Optional[tuple[str, str]]:
        select_value: Optional[str] = None
        back_value: Optional[str] = None
        for option in options:
            label_norm = _normalized_label(option.label or "")
            if select_value is None and "seleccionar alias" in label_norm and "crear" in label_norm:
                select_value = str(option.value).strip()
                continue
            if back_value is None and "volver" in label_norm:
                back_value = str(option.value).strip()
        if select_value and back_value:
            return select_value, back_value
        return None

    def _render_accounts_alias_selector_view(self, request: InputRequest) -> None:
        parsed_values = self._extract_accounts_alias_selector_values(request.menu_options)
        if parsed_values is None:
            self._menu_title_label.setVisible(True)
            self._menu_prompt_label.setVisible(True)
            self._rebuild_menu_buttons(request.menu_options)
            return
        self._accounts_alias_select_value, self._accounts_alias_back_value = parsed_values
        self._section_log_scope = None
        self._set_accounts_users_preview("")
        self._menu_title_label.setVisible(False)
        self._menu_prompt_label.setVisible(False)
        self._clear_menu_buttons()
        self._menu_options_layout.takeAt(0)

        select_button = QPushButton("Seleccionar alias o crear uno nuevo")
        select_button.setObjectName("MenuOptionButton")
        select_button.setMinimumHeight(52)
        select_button.clicked.connect(self._show_accounts_aliases_view)

        back_button = QPushButton("Volver atrás")
        back_button.setObjectName("MenuOptionButton")
        back_button.setMinimumHeight(52)
        back_button.clicked.connect(
            lambda checked=False: self._submit_current_input(self._accounts_alias_back_value)
        )

        self._menu_options_layout.addWidget(select_button)
        self._menu_options_layout.addWidget(back_button)
        self._menu_option_buttons.extend([select_button, back_button])
        self._menu_options_layout.addStretch(1)
        self._set_page(self.PAGE_MENU)
        self._set_live_input_enabled(False, prompt="")

    def _show_accounts_aliases_view(self) -> None:
        self._section_log_scope = None
        self._set_accounts_users_preview("")
        self._menu_title_label.setVisible(True)
        self._menu_title_label.setText("Alias disponibles")
        self._menu_prompt_label.setVisible(False)
        aliases = self._load_accounts_aliases()
        self._clear_menu_buttons()
        self._menu_options_layout.takeAt(0)

        aliases_grid_widget = QWidget()
        aliases_grid = QGridLayout(aliases_grid_widget)
        aliases_grid.setContentsMargins(0, 0, 0, 0)
        aliases_grid.setHorizontalSpacing(8)
        aliases_grid.setVerticalSpacing(8)
        max_columns = 4
        for index, alias in enumerate(aliases):
            alias_button = QPushButton(alias)
            alias_button.setObjectName("MenuOptionButton")
            alias_button.setProperty("compact", True)
            alias_button.clicked.connect(
                lambda checked=False, alias_value=alias: self._submit_accounts_alias(alias_value)
            )
            aliases_grid.addWidget(alias_button, index // max_columns, index % max_columns)
            self._menu_option_buttons.append(alias_button)
        self._menu_options_layout.addWidget(aliases_grid_widget)

        create_button = QPushButton("Crear nuevo alias")
        create_button.setObjectName("MenuOptionButton")
        create_button.setMinimumHeight(52)
        create_button.clicked.connect(self._start_accounts_alias_manual_entry)

        back_button = QPushButton("Volver atrás")
        back_button.setObjectName("MenuOptionButton")
        back_button.setMinimumHeight(52)
        back_button.clicked.connect(self._show_accounts_alias_selector_buttons)

        self._menu_options_layout.addWidget(create_button)
        self._menu_options_layout.addWidget(back_button)
        self._menu_option_buttons.extend([create_button, back_button])
        self._menu_options_layout.addStretch(1)
        self._set_page(self.PAGE_MENU)
        self._set_live_input_enabled(False, prompt="")

    def _show_accounts_alias_selector_buttons(self) -> None:
        self._accounts_alias_manual_mode = False
        request = self._pending_request
        if (
            request is None
            or not request.is_menu
            or not self._is_accounts_alias_selector_menu(request.menu_options)
        ):
            return
        self._render_accounts_alias_selector_view(request)

    def _submit_accounts_alias(self, alias_value: str) -> None:
        alias = str(alias_value).strip()
        if not alias:
            return
        self._accounts_alias_manual_mode = False
        select_value = str(self._accounts_alias_select_value).strip()
        if not select_value:
            return
        self._selected_accounts_alias = alias
        self._submit_current_input(select_value)
        self._submit_current_input(alias)

    def _start_accounts_alias_manual_entry(self) -> None:
        select_value = str(self._accounts_alias_select_value).strip()
        if not select_value:
            return
        self._accounts_alias_manual_mode = True
        self._set_accounts_users_preview("")
        self._menu_title_label.setVisible(False)
        self._menu_prompt_label.setVisible(False)
        self._clear_menu_buttons()
        self._set_page(self.PAGE_MENU)
        self._submit_current_input(select_value)
        self._set_live_input_enabled(
            True,
            prompt="Escribe el nuevo alias y presiona Enter",
            sensitive=False,
        )

    def _on_sidebar_item_clicked(self, key: str) -> None:
        if self._block_navigation:
            self._set_status("Waiting input")
            return
        if key == "exit":
            self.shutdown_application(reason="sidebar-exit")
            return
        self._set_active_sidebar(key)
        if key != "send":
            self._auto_fill_active = False
            self._auto_fill_queue.clear()
            self._set_send_setup_visible(False)
            self._hide_send_loading_overlay()
        if key != "autoresponder":
            self._reset_autoresponder_setup_mode()
        if key != "leads":
            self._set_leads_live_card_visible(False)
        self._set_menu_activity_visible(False)
        if key not in self._SECTION_LOG_KEYS:
            self._section_log_scope = None
        self._refresh_menu_activity_log()
        if key == "send" and self._campaign_running:
            self._set_execution_mode(True)
            return
        if key == "autoresponder" and self._autoresponder_running:
            self._set_execution_mode(True)
            return
        if key == "leads" and self._leads_filter_running:
            self._set_execution_mode(True)
            return
        self._set_execution_mode(False)

        if key == "dashboard":
            self._section_log_scope = None
            self._set_page(self.PAGE_DASHBOARD)
            self._queued_primary_key = None
            self._menu_prompt_label.setText("Selecciona una seccion del sidebar.")
            return

        self._set_page(self.PAGE_MENU)
        self._menu_title_label.setVisible(True)
        self._menu_title_label.setText(self._primary_label_by_key.get(key, "Menu"))
        self._menu_prompt_label.setVisible(True)

        if self._pending_request and self._pending_request.is_menu:
            if self._pending_request_is_primary_menu:
                value = self._primary_option_values.get(key)
                if value:
                    self._submit_current_input(value)
                    return
            else:
                self._menu_prompt_label.setText(
                    "Submenu activo. Completa o vuelve al menu principal para cambiar seccion."
                )
                return

        self._queued_primary_key = key
        self._menu_prompt_label.setText("Esperando menu principal para abrir esta seccion.")

    def _set_active_sidebar(self, key: str) -> None:
        self._active_sidebar_key = key
        for item_key, button in self._sidebar_buttons.items():
            button.setProperty("active", item_key == key)
            button.style().unpolish(button)
            button.style().polish(button)

    def _update_primary_button_visibility(self) -> None:
        deliver_button = self._sidebar_buttons.get("deliver")
        if not deliver_button:
            return
        if self._mode == "client":
            deliver_button.setVisible(False)
            return
        deliver_button.setVisible("deliver" in self._primary_option_values)

    def _build_dashboard_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        container = QWidget()
        container.setObjectName("SubmenuScrollContent")
        container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(10, 10, 10, 10)
        container_layout.setSpacing(12)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        title = QLabel("Dashboard")
        title.setObjectName("PageTitle")
        subtitle = QLabel("Metricas en tiempo real del estado operativo")
        subtitle.setObjectName("MutedText")
        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_dashboard_metrics)

        header_layout.addWidget(title)
        header_layout.addWidget(subtitle)
        header_layout.addStretch(1)
        header_layout.addWidget(refresh_button)
        container_layout.addWidget(header)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)

        metrics = [
            ("total_accounts", "Cuentas Totales"),
            ("active_accounts", "Cuentas Activas"),
            ("connected_accounts", "Cuentas Conectadas"),
            ("messages_sent_today", "Mensajes Enviados Hoy"),
            ("messages_error_today", "Mensajes con Error Hoy"),
            ("messages_replied_today", "Mensajes Respondidos Hoy"),
            ("booked_today", "Agendas Realizadas Hoy"),
            ("last_refresh", "Ultima actualizacion"),
        ]
        for index, (key, label) in enumerate(metrics):
            row = index // 4
            column = index % 4
            grid.addWidget(self._build_metric_card(key, label), row, column)

        container_layout.addLayout(grid)
        container_layout.addStretch(1)

        layout.addWidget(container, 1)
        return page

    def _build_metric_card(self, key: str, label_text: str) -> QWidget:
        if key in {"booked_today", "messages_replied_today"}:
            card = ClickableMetricCard()
            card.setCursor(Qt.PointingHandCursor)
            if key == "booked_today":
                card.clicked.connect(self._show_booked_today_dialog)
            else:
                card.clicked.connect(self._show_replied_today_dialog)
        else:
            card = QFrame()
        card.setObjectName("MetricCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(6)

        label = QLabel(label_text)
        label.setObjectName("MetricLabel")
        value = QLabel("0")
        value.setObjectName("MetricValue")

        layout.addWidget(label)
        layout.addWidget(value)
        self._metric_values[key] = value
        return card

    def _build_menu_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)

        self._menu_title_label = QLabel("Seccion")
        self._menu_title_label.setObjectName("PageTitle")
        self._autoresponder_setup_back_button = QPushButton("Atras")
        self._autoresponder_setup_back_button.setObjectName("SecondaryButton")
        self._autoresponder_setup_back_button.clicked.connect(self._handle_autoresponder_setup_back)
        self._autoresponder_setup_back_button.setVisible(False)
        header_row.addWidget(self._menu_title_label)
        header_row.addStretch(1)
        header_row.addWidget(self._autoresponder_setup_back_button)

        self._menu_prompt_label = QLabel("Selecciona una accion para ejecutar el flujo CLI.")
        self._menu_prompt_label.setObjectName("MutedText")
        self._menu_prompt_label.setWordWrap(True)
        self._accounts_users_label = QLabel("")
        self._accounts_users_label.setObjectName("StatusValue")
        self._accounts_users_label.setWordWrap(True)
        self._accounts_users_label.setVisible(False)

        layout.addLayout(header_row)
        layout.addWidget(self._menu_prompt_label)
        layout.addWidget(self._accounts_users_label)

        self._send_setup_card = self._build_send_setup_panel()
        self._send_setup_card.setVisible(False)
        layout.addWidget(self._send_setup_card, 0)

        self._autoresponder_setup_card = self._build_autoresponder_setup_panel()
        self._autoresponder_setup_card.setVisible(False)
        layout.addWidget(self._autoresponder_setup_card, 0)

        self._leads_live_card = QFrame()
        self._leads_live_card.setObjectName("LeadPromptCard")
        leads_live_layout = QVBoxLayout(self._leads_live_card)
        leads_live_layout.setContentsMargins(10, 10, 10, 10)
        leads_live_layout.setSpacing(8)

        leads_live_title = QLabel("Filtrado de leads · Vista en vivo")
        leads_live_title.setObjectName("PageTitle")
        self._leads_live_prompt_label = QLabel("Esperando pregunta...")
        self._leads_live_prompt_label.setObjectName("MutedText")
        self._leads_live_prompt_label.setWordWrap(True)

        leads_typing_title = QLabel("Entrada en vivo")
        leads_typing_title.setObjectName("MutedText")
        self._leads_live_input_preview = QLabel("(vacío)")
        self._leads_live_input_preview.setObjectName("LeadPromptTypedValue")
        self._leads_live_input_preview.setWordWrap(True)

        leads_sent_title = QLabel("Entradas enviadas")
        leads_sent_title.setObjectName("MutedText")
        self._leads_live_submitted_console = QPlainTextEdit()
        self._leads_live_submitted_console.setObjectName("ExecLogConsole")
        self._leads_live_submitted_console.setReadOnly(True)
        self._leads_live_submitted_console.setMaximumBlockCount(180)
        self._leads_live_submitted_console.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self._leads_live_submitted_console.setPlaceholderText("Aún no hay entradas enviadas.")
        self._leads_live_submitted_console.setFixedHeight(110)

        leads_live_layout.addWidget(leads_live_title)
        leads_live_layout.addWidget(self._leads_live_prompt_label)
        leads_live_layout.addWidget(leads_typing_title)
        leads_live_layout.addWidget(self._leads_live_input_preview)
        leads_live_layout.addWidget(leads_sent_title)
        leads_live_layout.addWidget(self._leads_live_submitted_console)
        self._leads_live_card.setVisible(False)
        layout.addWidget(self._leads_live_card, 0)

        self._menu_options_container = QWidget()
        self._menu_options_container.setObjectName("SubmenuScrollContent")
        self._menu_options_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._menu_options_layout = QVBoxLayout(self._menu_options_container)
        self._menu_options_layout.setContentsMargins(10, 10, 10, 10)
        self._menu_options_layout.setSpacing(12)
        self._menu_options_layout.addStretch(1)

        layout.addWidget(self._menu_options_container, 1)

        logs_card = QFrame()
        logs_card.setObjectName("ExecCard")
        logs_layout = QVBoxLayout(logs_card)
        logs_layout.setContentsMargins(10, 10, 10, 10)
        logs_layout.setSpacing(8)

        logs_title_row = QHBoxLayout()
        logs_title_row.setContentsMargins(0, 0, 0, 0)
        logs_title_row.setSpacing(8)
        self._menu_log_title = QLabel("Actividad")
        self._menu_log_title.setObjectName("PageTitle")

        clear_button = QPushButton("Clear")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_active_section_log)
        logs_title_row.addWidget(self._menu_log_title)
        logs_title_row.addStretch(1)
        logs_title_row.addWidget(clear_button)
        logs_layout.addLayout(logs_title_row)

        self._menu_activity_log_console = QPlainTextEdit()
        self._menu_activity_log_console.setObjectName("ExecLogConsole")
        self._menu_activity_log_console.setReadOnly(True)
        self._menu_activity_log_console.setMaximumBlockCount(1800)
        self._menu_activity_log_console.setLineWrapMode(QPlainTextEdit.NoWrap)
        self._menu_activity_log_console.setPlaceholderText("No activity yet.")
        logs_layout.addWidget(self._menu_activity_log_console, 1)
        self._menu_activity_card = logs_card
        self._menu_activity_card.setVisible(False)
        layout.addWidget(logs_card, 0)
        self._refresh_menu_activity_log()
        return page

    def _build_send_setup_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("SendSetupCard")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)

        title = QLabel("Envío de mensajes · Configuración")
        title.setObjectName("PageTitle")

        self._send_setup_back_button = QPushButton("← Atrás")
        self._send_setup_back_button.setObjectName("SecondaryButton")
        self._send_setup_back_button.clicked.connect(self._handle_send_setup_back)

        self._send_setup_submit_button = QPushButton("Enviar")
        self._send_setup_submit_button.setObjectName("PrimaryButton")
        self._send_setup_submit_button.clicked.connect(self._submit_send_setup_value)

        header_row.addWidget(title)
        header_row.addStretch(1)
        header_row.addWidget(self._send_setup_back_button)
        header_row.addWidget(self._send_setup_submit_button)
        layout.addLayout(header_row)

        self._send_setup_prompt_label = QLabel("Esperando prompt de envío...")
        self._send_setup_prompt_label.setObjectName("MutedText")
        self._send_setup_prompt_label.setWordWrap(True)
        layout.addWidget(self._send_setup_prompt_label)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)

        def _field_card(title_text: str, widget: QWidget) -> QFrame:
            card = QFrame()
            card.setObjectName("SendSetupFieldCard")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 10, 10, 10)
            card_layout.setSpacing(6)
            label = QLabel(title_text)
            label.setObjectName("MutedText")
            card_layout.addWidget(label)
            card_layout.addWidget(widget)
            return card

        self._send_alias_combo = QComboBox()
        self._send_alias_combo.setObjectName("SendSetupCombo")
        self._send_alias_combo.setEditable(False)
        self._send_alias_combo.setInsertPolicy(QComboBox.NoInsert)
        self._send_alias_combo.activated.connect(
            lambda _index=0: self._auto_submit_send_setup_if_waiting("alias")
        )
        grid.addWidget(_field_card("Alias o grupo", self._send_alias_combo), 0, 0)

        self._send_leads_combo = QComboBox()
        self._send_leads_combo.setObjectName("SendSetupCombo")
        self._send_leads_combo.setEditable(False)
        self._send_leads_combo.setInsertPolicy(QComboBox.NoInsert)
        self._send_leads_combo.activated.connect(
            lambda _index=0: self._auto_submit_send_setup_if_waiting("leads_alias")
        )
        grid.addWidget(_field_card("Alias de los leads", self._send_leads_combo), 0, 1)

        self._send_per_account_spin = QSpinBox()
        self._send_per_account_spin.setObjectName("SendSetupSpin")
        self._send_per_account_spin.setRange(1, 100000)
        self._send_per_account_spin.setValue(1)
        grid.addWidget(
            _field_card("Cantidad de mensajes por cuenta", self._send_per_account_spin),
            1,
            0,
        )

        self._send_concurrency_spin = QSpinBox()
        self._send_concurrency_spin.setObjectName("SendSetupSpin")
        self._send_concurrency_spin.setRange(1, 500)
        self._send_concurrency_spin.setValue(1)
        grid.addWidget(_field_card("Cuentas en simultáneo", self._send_concurrency_spin), 1, 1)

        self._send_delay_min_spin = QSpinBox()
        self._send_delay_min_spin.setObjectName("SendSetupSpin")
        self._send_delay_min_spin.setRange(1, 3600)
        self._send_delay_min_spin.setValue(10)
        grid.addWidget(_field_card("Delay mínimo (seg)", self._send_delay_min_spin), 2, 0)

        self._send_delay_max_spin = QSpinBox()
        self._send_delay_max_spin.setObjectName("SendSetupSpin")
        self._send_delay_max_spin.setRange(1, 3600)
        self._send_delay_max_spin.setValue(20)
        grid.addWidget(_field_card("Delay máximo (seg)", self._send_delay_max_spin), 2, 1)

        toggle_wrap = QWidget()
        toggle_layout = QHBoxLayout(toggle_wrap)
        toggle_layout.setContentsMargins(0, 0, 0, 0)
        toggle_layout.setSpacing(8)
        self._send_templates_yes_button = QPushButton("SI")
        self._send_templates_yes_button.setObjectName("SecondaryButton")
        self._send_templates_yes_button.setCheckable(True)
        self._send_templates_yes_button.clicked.connect(
            lambda: self._on_send_template_toggle_clicked(True)
        )
        self._send_templates_no_button = QPushButton("NO")
        self._send_templates_no_button.setObjectName("SecondaryButton")
        self._send_templates_no_button.setCheckable(True)
        self._send_templates_no_button.clicked.connect(
            lambda: self._on_send_template_toggle_clicked(False)
        )
        toggle_layout.addWidget(self._send_templates_yes_button)
        toggle_layout.addWidget(self._send_templates_no_button)
        toggle_layout.addStretch(1)
        grid.addWidget(_field_card("Seleccionar plantilla (SI/NO)", toggle_wrap), 3, 0, 1, 2)

        self._send_saved_template_combo = QComboBox()
        self._send_saved_template_combo.setObjectName("SendSetupCombo")
        self._send_saved_template_combo.setEditable(False)
        self._send_saved_template_combo.activated.connect(
            lambda _index=0: self._auto_submit_send_setup_if_waiting("templates_saved")
        )
        self._send_saved_template_card = _field_card(
            "Plantillas guardadas",
            self._send_saved_template_combo,
        )
        grid.addWidget(self._send_saved_template_card, 4, 0, 1, 2)

        self._send_manual_message_input = QLineEdit()
        self._send_manual_message_input.setObjectName("InputField")
        self._send_manual_message_input.setPlaceholderText("Mensaje manual (una línea)")
        self._send_manual_message_input.returnPressed.connect(self._submit_send_setup_value)
        self._send_manual_message_card = _field_card(
            "Mensaje manual",
            self._send_manual_message_input,
        )
        grid.addWidget(self._send_manual_message_card, 5, 0, 1, 2)

        layout.addLayout(grid)
        self._set_send_template_mode(False)
        return panel

    def _set_send_setup_visible(self, visible: bool) -> None:
        if not hasattr(self, "_send_setup_card"):
            return
        self._send_setup_card.setVisible(visible)
        self._sync_setup_panels_visibility()
        if visible:
            self._refresh_send_setup_sources()

    def _set_autoresponder_setup_visible(self, visible: bool) -> None:
        if not hasattr(self, "_autoresponder_setup_card"):
            return
        self._autoresponder_setup_card.setVisible(visible)
        if hasattr(self, "_autoresponder_setup_back_button"):
            self._autoresponder_setup_back_button.setVisible(bool(visible))
        self._sync_setup_panels_visibility()
        if visible:
            self._refresh_autoresponder_setup_sources()
            self._refresh_autoresponder_selected_accounts_view()
            self._refresh_autoresponder_hours_summary()

    def _sync_setup_panels_visibility(self) -> None:
        if not hasattr(self, "_menu_options_container"):
            return
        send_visible = bool(hasattr(self, "_send_setup_card") and self._send_setup_card.isVisible())
        autoresponder_visible = bool(
            hasattr(self, "_autoresponder_setup_card") and self._autoresponder_setup_card.isVisible()
        )
        self._menu_options_container.setVisible(not (send_visible or autoresponder_visible))

    def _load_leads_aliases(self) -> list[str]:
        aliases: list[str] = []
        try:
            module = import_module("leads")
            list_files_callable = getattr(module, "list_files", None)
            if callable(list_files_callable):
                payload = list_files_callable()
                if isinstance(payload, list):
                    aliases = sorted(
                        {
                            str(item).strip()
                            for item in payload
                            if str(item).strip()
                        }
                    )
        except Exception:
            aliases = []
        if not aliases:
            leads_dir = self._root_dir / "text" / "leads"
            try:
                if leads_dir.is_dir():
                    aliases = sorted(
                        {path.stem for path in leads_dir.glob("*.txt") if path.is_file() and path.stem}
                    )
            except Exception:
                aliases = []
        if "default" not in aliases:
            aliases.insert(0, "default")
        return aliases

    def _load_send_saved_templates(self) -> list[dict[str, str]]:
        try:
            from templates_store import load_templates

            payload = load_templates()
        except Exception:
            payload = []
        items: list[dict[str, str]] = []
        for raw in payload:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or "").strip()
            text = str(raw.get("text") or "").strip()
            if not name or not text:
                continue
            items.append({"name": name, "text": text})
        return items

    def _load_active_account_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        try:
            module = import_module("accounts")
            list_callable = getattr(module, "list_all", None)
            if callable(list_callable):
                payload = list_callable()
                if isinstance(payload, list):
                    records = [item for item in payload if isinstance(item, dict)]
        except Exception:
            records = []

        if not records:
            payload = self._read_json(self._root_dir / "data" / "accounts.json", [])
            if isinstance(payload, list):
                records = [item for item in payload if isinstance(item, dict)]

        active_records: list[dict[str, Any]] = []
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            if not bool(record.get("active", True)):
                continue
            active_records.append(record)
        return active_records

    def _load_active_accounts_aliases(self) -> list[str]:
        records = self._load_active_account_records()

        aliases = {
            str(record.get("alias") or "default").strip() or "default"
            for record in records
        }
        if aliases:
            return sorted(aliases)
        return self._load_accounts_aliases()

    def _refresh_send_setup_sources(self) -> None:
        if not hasattr(self, "_send_alias_combo"):
            return

        def _update_combo(combo: QComboBox, values: list[str]) -> None:
            current = combo.currentText().strip()
            combo.blockSignals(True)
            combo.clear()
            for value in values:
                combo.addItem(value)
            if current:
                idx = combo.findText(current)
                if idx >= 0:
                    combo.setCurrentIndex(idx)
                elif combo.count() > 0:
                    combo.setCurrentIndex(0)
            elif combo.count() > 0:
                combo.setCurrentIndex(0)
            combo.blockSignals(False)

        _update_combo(self._send_alias_combo, self._load_active_accounts_aliases())
        _update_combo(self._send_leads_combo, self._load_leads_aliases())

        current_saved_data = str(self._send_saved_template_combo.currentData() or "").strip()
        self._send_saved_template_combo.blockSignals(True)
        self._send_saved_template_combo.clear()
        templates = self._load_send_saved_templates()
        for index, item in enumerate(templates, start=1):
            preview = item["text"].replace("\n", " ").strip()
            if len(preview) > 36:
                preview = preview[:33] + "..."
            label = f"{index}) {item['name']} · {preview}"
            self._send_saved_template_combo.addItem(label, str(index))
        if current_saved_data:
            idx = self._send_saved_template_combo.findData(current_saved_data)
            if idx >= 0:
                self._send_saved_template_combo.setCurrentIndex(idx)
            elif self._send_saved_template_combo.count() > 0:
                self._send_saved_template_combo.setCurrentIndex(0)
        elif self._send_saved_template_combo.count() > 0:
            self._send_saved_template_combo.setCurrentIndex(0)
        self._send_saved_template_combo.blockSignals(False)
        self._send_saved_template_combo.setEnabled(bool(templates))
        self._send_manual_message_input.setEnabled(True)

    def _set_send_template_mode(self, use_saved: bool) -> None:
        self._send_setup_use_saved_templates = bool(use_saved)
        self._send_templates_yes_button.setChecked(self._send_setup_use_saved_templates)
        self._send_templates_no_button.setChecked(not self._send_setup_use_saved_templates)
        self._send_saved_template_card.setVisible(self._send_setup_use_saved_templates)
        self._send_manual_message_card.setVisible(not self._send_setup_use_saved_templates)
        self._send_saved_template_combo.setEnabled(
            self._send_setup_use_saved_templates and self._send_saved_template_combo.count() > 0
        )
        self._send_manual_message_input.setEnabled(not self._send_setup_use_saved_templates)
        if self._send_setup_use_saved_templates:
            self._send_saved_template_combo.setFocus()
        else:
            self._send_manual_message_input.setFocus()

    def _on_send_template_toggle_clicked(self, use_saved: bool) -> None:
        self._set_send_template_mode(use_saved)
        self._auto_submit_send_setup_if_waiting("templates_toggle")

    def _auto_submit_send_setup_if_waiting(self, expected_kind: str) -> None:
        # El envio en modo formulario se dispara una sola vez desde el boton "Enviar".
        # Evitamos auto-submit por cambios de campos para no adelantar pasos del backend.
        return

    def _detect_send_prompt_kind(self, prompt_text: str) -> str:
        normalized = _normalized_label(prompt_text)
        if not normalized:
            return "manual_message"
        if "alias grupo" in normalized:
            return "alias"
        if "nombre de la lista" in normalized or "text leads" in normalized:
            return "leads_alias"
        if "mensajes por cuenta" in normalized:
            return "per_account"
        if "cuentas en simultaneo" in normalized:
            return "concurrency"
        if "delay minimo" in normalized:
            return "delay_min"
        if "delay maximo" in normalized:
            return "delay_max"
        if "usar plantillas guardadas" in normalized:
            return "templates_toggle"
        if "selecciona plantillas" in normalized:
            return "templates_saved"
        return "manual_message"

    @staticmethod
    def _extract_send_prompt_default_int(prompt_text: str, fallback: int) -> int:
        match = _SEND_PROMPT_INT_DEFAULT_RE.search(prompt_text or "")
        if not match:
            return fallback
        try:
            return max(1, int(match.group("value")))
        except Exception:
            return fallback

    @staticmethod
    def _extract_send_delay_max_default(prompt_text: str, fallback: int) -> int:
        normalized = _normalized_label(prompt_text or "")
        match = _SEND_PROMPT_DELAY_MAX_DEFAULT_RE.search(normalized)
        if match:
            try:
                return max(1, int(match.group("value")))
            except Exception:
                return fallback
        values = [int(item.group("value")) for item in _SEND_PROMPT_INT_DEFAULT_RE.finditer(prompt_text or "")]
        if values:
            return max(1, values[-1])
        return fallback

    def _focus_send_setup_kind(self, kind: str) -> None:
        if kind == "alias":
            self._send_alias_combo.setFocus()
            return
        if kind == "leads_alias":
            self._send_leads_combo.setFocus()
            return
        if kind == "per_account":
            self._send_per_account_spin.setFocus()
            self._send_per_account_spin.selectAll()
            return
        if kind == "concurrency":
            self._send_concurrency_spin.setFocus()
            self._send_concurrency_spin.selectAll()
            return
        if kind == "delay_min":
            self._send_delay_min_spin.setFocus()
            self._send_delay_min_spin.selectAll()
            return
        if kind == "delay_max":
            self._send_delay_max_spin.setFocus()
            self._send_delay_max_spin.selectAll()
            return
        if kind == "templates_saved":
            self._send_saved_template_combo.setFocus()
            return
        self._send_manual_message_input.setFocus()
        self._send_manual_message_input.selectAll()

    def _configure_send_setup_from_request(self, request: InputRequest) -> None:
        if not hasattr(self, "_send_setup_card"):
            return
        cleaned_prompt = _strip_cli_hints(request.prompt or "")
        self._send_setup_prompt_text = cleaned_prompt
        self._send_setup_prompt_kind = self._detect_send_prompt_kind(cleaned_prompt)
        self._refresh_send_setup_sources()

        if self._send_setup_prompt_kind == "per_account":
            default = self._extract_send_prompt_default_int(
                cleaned_prompt,
                self._send_per_account_spin.value(),
            )
            self._send_per_account_spin.setValue(default)
        elif self._send_setup_prompt_kind == "concurrency":
            default = self._extract_send_prompt_default_int(
                cleaned_prompt,
                self._send_concurrency_spin.value(),
            )
            self._send_concurrency_spin.setValue(default)
        elif self._send_setup_prompt_kind == "delay_min":
            default = self._extract_send_prompt_default_int(
                cleaned_prompt,
                self._send_delay_min_spin.value(),
            )
            self._send_delay_min_spin.setValue(default)
        elif self._send_setup_prompt_kind == "delay_max":
            default = self._extract_send_delay_max_default(
                cleaned_prompt,
                self._send_delay_max_spin.value(),
            )
            self._send_delay_max_spin.setValue(default)
        elif self._send_setup_prompt_kind == "templates_toggle":
            self._set_send_template_mode(self._send_setup_use_saved_templates)
        elif self._send_setup_prompt_kind == "templates_saved":
            self._set_send_template_mode(True)

        prompt_label = cleaned_prompt.strip() or "Escribe una plantilla y presiona Enviar."
        self._send_setup_prompt_label.setText(prompt_label)
        self._focus_send_setup_kind(self._send_setup_prompt_kind)

    def _send_setup_value_for_request(self, request: InputRequest) -> str:
        kind = self._send_setup_prompt_kind or self._detect_send_prompt_kind(request.prompt or "")
        if kind == "alias":
            value = self._send_alias_combo.currentText().strip()
            return value or "default"
        if kind == "leads_alias":
            return self._send_leads_combo.currentText().strip()
        if kind == "per_account":
            return str(self._send_per_account_spin.value())
        if kind == "concurrency":
            return str(self._send_concurrency_spin.value())
        if kind == "delay_min":
            return str(self._send_delay_min_spin.value())
        if kind == "delay_max":
            return str(self._send_delay_max_spin.value())
        if kind == "templates_toggle":
            return "s" if self._send_setup_use_saved_templates else "n"
        if kind == "templates_saved":
            data = self._send_saved_template_combo.currentData()
            if data is not None:
                data_text = str(data).strip()
                if data_text:
                    return data_text
            text_value = self._send_saved_template_combo.currentText().strip()
            if ")" in text_value and text_value.split(")", 1)[0].strip().isdigit():
                return text_value.split(")", 1)[0].strip()
            return text_value
        return self._send_manual_message_input.text()

    def _set_send_setup_validation_error(self, message: str) -> None:
        self._set_status("Waiting input")
        self._send_setup_prompt_label.setText(message)
        self._append_log(f"[gui] send setup validation: {message}\n")

    def _selected_saved_template_value(self) -> str:
        data = self._send_saved_template_combo.currentData()
        if data is not None:
            data_text = str(data).strip()
            if data_text:
                return data_text
        text_value = self._send_saved_template_combo.currentText().strip()
        if ")" in text_value and text_value.split(")", 1)[0].strip().isdigit():
            return text_value.split(")", 1)[0].strip()
        return text_value

    def _build_send_setup_auto_fill_queue(self) -> Optional[list[str]]:
        alias_group = self._send_alias_combo.currentText().strip()
        if not alias_group:
            self._set_send_setup_validation_error("Selecciona Alias o grupo.")
            return None

        leads_alias = self._send_leads_combo.currentText().strip()
        if not leads_alias:
            self._set_send_setup_validation_error("Selecciona Alias de los leads.")
            return None

        per_account = int(self._send_per_account_spin.value())
        concurrency = int(self._send_concurrency_spin.value())
        delay_min = int(self._send_delay_min_spin.value())
        delay_max = int(self._send_delay_max_spin.value())
        if delay_max < delay_min:
            self._set_send_setup_validation_error("Delay máximo debe ser mayor o igual al mínimo.")
            return None

        use_saved_templates = bool(self._send_setup_use_saved_templates)
        template_toggle = "SI" if use_saved_templates else "NO"
        if use_saved_templates:
            selected_template = self._selected_saved_template_value().strip()
            if not selected_template:
                self._set_send_setup_validation_error("Selecciona una plantilla guardada.")
                return None
            queue = [
                alias_group,
                leads_alias,
                str(per_account),
                str(concurrency),
                str(delay_min),
                str(delay_max),
                template_toggle,
                selected_template,
            ]
            return queue

        manual_message = self._send_manual_message_input.text().strip()
        if not manual_message:
            self._set_send_setup_validation_error("Escribe un mensaje manual.")
            return None
        # El CLI cierra la captura manual con una linea vacia.
        queue = [
            alias_group,
            leads_alias,
            str(per_account),
            str(concurrency),
            str(delay_min),
            str(delay_max),
            template_toggle,
            manual_message,
            "",
        ]
        return queue

    def _consume_send_setup_auto_fill(self, request: InputRequest) -> bool:
        if self._active_sidebar_key != "send" or not self._auto_fill_active:
            return False
        if request.is_menu:
            self._auto_fill_active = False
            self._auto_fill_queue.clear()
            self._hide_send_loading_overlay()
            return False
        if not self._auto_fill_queue:
            self._auto_fill_active = False
            return False
        value = self._auto_fill_queue.pop(0)
        kind = self._detect_send_prompt_kind(request.prompt or "")
        outbound = value
        if kind == "templates_toggle":
            outbound = "s" if value.strip().lower() in {"s", "si", "yes", "y"} else "n"
        adapter = self._io_adapter
        if adapter is None:
            self._auto_fill_active = False
            self._auto_fill_queue.clear()
            self._hide_send_loading_overlay()
            return False
        accepted = adapter.fulfill_input(outbound, request_id=request.request_id)
        if not accepted:
            self._auto_fill_active = False
            self._auto_fill_queue.clear()
            self._hide_send_loading_overlay()
            return False
        visible = "***" if request.sensitive and outbound else outbound
        self._append_log(f"[gui] input submitted: {visible}\n")
        if not self._auto_fill_queue:
            self._auto_fill_active = False
        return True

    def _submit_send_setup_value(self) -> None:
        if self._block_navigation:
            return
        if self._active_sidebar_key != "send":
            return
        if self._auto_fill_active:
            return
        queue = self._build_send_setup_auto_fill_queue()
        if not queue:
            return
        self._capture_low_profile_list = False
        self._capture_session_list = False
        self._send_low_profile_accounts.clear()
        self._send_session_issue_accounts.clear()
        self._send_login_total_failure = False
        self._emit_engine_event("SEND_CONFIG_SUBMITTED", queued_inputs=len(queue))
        self._auto_fill_queue = queue
        self._auto_fill_active = True
        self._show_send_loading_overlay()
        pending = self._pending_request
        if isinstance(pending, InputRequest):
            self._consume_send_setup_auto_fill(pending)

    def _exit_send_config_mode(self) -> None:
        self._hide_send_loading_overlay()
        self._auto_fill_active = False
        self._auto_fill_queue.clear()
        self._capture_low_profile_list = False
        self._capture_session_list = False
        self._send_low_profile_accounts.clear()
        self._send_session_issue_accounts.clear()
        self._send_login_total_failure = False
        self._set_send_setup_visible(False)
        self._send_setup_prompt_kind = ""
        self._send_setup_prompt_text = ""
        self._send_setup_use_saved_templates = False
        self._set_send_template_mode(False)
        self._send_manual_message_input.clear()
        if self._send_alias_combo.count() > 0:
            self._send_alias_combo.setCurrentIndex(0)
        if self._send_leads_combo.count() > 0:
            self._send_leads_combo.setCurrentIndex(0)
        if self._send_saved_template_combo.count() > 0:
            self._send_saved_template_combo.setCurrentIndex(0)
        self._last_prompt_value.setText("-")

    def _handle_send_setup_back(self) -> None:
        try:
            from runtime import request_stop

            request_stop("salida desde configuracion de envio")
        except Exception:
            pass
        if self._pending_request is not None:
            self._submit_current_input("")
        self._exit_send_config_mode()
        self._set_page(self.PAGE_DASHBOARD)

    def _build_autoresponder_setup_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("SendSetupCard")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        title = QLabel("Auto responder con OpenAI")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        self._autoresponder_setup_message_label = QLabel(
            "Completa la configuración y presiona ▶ Activar Auto Responder."
        )
        self._autoresponder_setup_message_label.setObjectName("MutedText")
        self._autoresponder_setup_message_label.setWordWrap(True)
        layout.addWidget(self._autoresponder_setup_message_label)

        def _field_card(title_text: str, widget: QWidget) -> QFrame:
            card = QFrame()
            card.setObjectName("SendSetupFieldCard")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 10, 10, 10)
            card_layout.setSpacing(6)
            label = QLabel(title_text)
            label.setObjectName("MutedText")
            card_layout.addWidget(label)
            card_layout.addWidget(widget)
            return card

        account_wrap = QWidget()
        account_layout = QVBoxLayout(account_wrap)
        account_layout.setContentsMargins(0, 0, 0, 0)
        account_layout.setSpacing(6)

        account_pick_row = QHBoxLayout()
        account_pick_row.setContentsMargins(0, 0, 0, 0)
        account_pick_row.setSpacing(8)
        self._autoresponder_target_combo = QComboBox()
        self._autoresponder_target_combo.setObjectName("SendSetupCombo")
        self._autoresponder_target_combo.setEditable(False)
        account_pick_row.addWidget(self._autoresponder_target_combo, 1)

        add_target_button = QPushButton("Agregar")
        add_target_button.setObjectName("SecondaryButton")
        add_target_button.clicked.connect(self._add_selected_autoresponder_target)
        account_pick_row.addWidget(add_target_button)

        clear_target_button = QPushButton("Limpiar")
        clear_target_button.setObjectName("SecondaryButton")
        clear_target_button.clicked.connect(self._clear_autoresponder_selected_accounts)
        account_pick_row.addWidget(clear_target_button)
        account_layout.addLayout(account_pick_row)

        selected_title = QLabel("Cuentas seleccionadas:")
        selected_title.setObjectName("MutedText")
        account_layout.addWidget(selected_title)

        selected_accounts_widget = QWidget()
        self._autoresponder_selected_accounts_layout = QGridLayout(selected_accounts_widget)
        self._autoresponder_selected_accounts_layout.setContentsMargins(0, 0, 0, 0)
        self._autoresponder_selected_accounts_layout.setHorizontalSpacing(6)
        self._autoresponder_selected_accounts_layout.setVerticalSpacing(6)
        account_layout.addWidget(selected_accounts_widget)

        delay_grid = QGridLayout()
        delay_grid.setContentsMargins(0, 0, 0, 0)
        delay_grid.setHorizontalSpacing(10)
        delay_grid.setVerticalSpacing(10)
        self._autoresponder_delay_min_spin = QSpinBox()
        self._autoresponder_delay_min_spin.setObjectName("SendSetupSpin")
        self._autoresponder_delay_min_spin.setRange(1, 3600)
        self._autoresponder_delay_min_spin.setValue(45)
        self._autoresponder_delay_max_spin = QSpinBox()
        self._autoresponder_delay_max_spin.setObjectName("SendSetupSpin")
        self._autoresponder_delay_max_spin.setRange(1, 3600)
        self._autoresponder_delay_max_spin.setValue(76)
        delay_grid.addWidget(
            _field_card("Delay mínimo (seg)", self._autoresponder_delay_min_spin),
            0,
            0,
        )
        delay_grid.addWidget(
            _field_card("Delay máximo (seg)", self._autoresponder_delay_max_spin),
            0,
            1,
        )

        config_wrap = QWidget()
        config_layout = QVBoxLayout(config_wrap)
        config_layout.setContentsMargins(0, 0, 0, 0)
        config_layout.setSpacing(8)

        config_top_grid = QGridLayout()
        config_top_grid.setContentsMargins(0, 0, 0, 0)
        config_top_grid.setHorizontalSpacing(10)
        config_top_grid.setVerticalSpacing(10)

        self._autoresponder_concurrency_combo = QComboBox()
        self._autoresponder_concurrency_combo.setObjectName("SendSetupCombo")
        self._autoresponder_concurrency_combo.setEditable(False)
        self._autoresponder_concurrency_combo.setInsertPolicy(QComboBox.NoInsert)
        config_top_grid.addWidget(
            _field_card("Cuentas en simultáneo", self._autoresponder_concurrency_combo),
            0,
            0,
        )

        self._autoresponder_threads_spin = QSpinBox()
        self._autoresponder_threads_spin.setObjectName("SendSetupSpin")
        self._autoresponder_threads_spin.setRange(1, 500)
        self._autoresponder_threads_spin.setValue(20)
        config_top_grid.addWidget(
            _field_card("Threads a leer", self._autoresponder_threads_spin),
            0,
            1,
        )
        config_layout.addLayout(config_top_grid)

        hours_wrap = QWidget()
        hours_layout = QVBoxLayout(hours_wrap)
        hours_layout.setContentsMargins(0, 0, 0, 0)
        hours_layout.setSpacing(8)

        preset_row = QHBoxLayout()
        preset_row.setContentsMargins(0, 0, 0, 0)
        preset_row.setSpacing(8)
        self._autoresponder_hour_buttons: dict[int, QPushButton] = {}
        for hour in (4, 8, 12, 24, 48):
            hour_button = QPushButton(str(hour))
            hour_button.setObjectName("SecondaryButton")
            hour_button.setCheckable(True)
            hour_button.setChecked(hour in self._autoresponder_selected_hours)
            hour_button.clicked.connect(
                lambda checked=False, value=hour: self._toggle_autoresponder_hour(value, checked)
            )
            self._autoresponder_hour_buttons[hour] = hour_button
            preset_row.addWidget(hour_button)
        preset_row.addStretch(1)
        hours_layout.addLayout(preset_row)

        custom_row = QHBoxLayout()
        custom_row.setContentsMargins(0, 0, 0, 0)
        custom_row.setSpacing(8)
        self._autoresponder_custom_hour_input = QLineEdit()
        self._autoresponder_custom_hour_input.setObjectName("InputField")
        self._autoresponder_custom_hour_input.setPlaceholderText("+ Personalizado")
        self._autoresponder_custom_hour_input.returnPressed.connect(
            self._add_autoresponder_custom_hour
        )
        custom_row.addWidget(self._autoresponder_custom_hour_input, 1)
        custom_add_button = QPushButton("Agregar hora")
        custom_add_button.setObjectName("SecondaryButton")
        custom_add_button.clicked.connect(self._add_autoresponder_custom_hour)
        custom_row.addWidget(custom_add_button)

        custom_remove_button = QPushButton("Quitar hora")
        custom_remove_button.setObjectName("SecondaryButton")
        custom_remove_button.clicked.connect(self._remove_autoresponder_custom_hour)
        custom_row.addWidget(custom_remove_button)

        custom_clear_button = QPushButton("Limpiar horas")
        custom_clear_button.setObjectName("SecondaryButton")
        custom_clear_button.clicked.connect(self._clear_autoresponder_hours)
        custom_row.addWidget(custom_clear_button)
        hours_layout.addLayout(custom_row)

        self._autoresponder_hours_summary_label = QLabel("")
        self._autoresponder_hours_summary_label.setObjectName("MutedText")
        self._autoresponder_hours_summary_label.setWordWrap(True)
        hours_layout.addWidget(self._autoresponder_hours_summary_label)
        config_layout.addWidget(_field_card("Horas de seguimiento", hours_wrap))

        followup_toggle_wrap = QWidget()
        followup_toggle_layout = QHBoxLayout(followup_toggle_wrap)
        followup_toggle_layout.setContentsMargins(0, 0, 0, 0)
        followup_toggle_layout.setSpacing(8)
        self._autoresponder_followup_yes_button = QPushButton("Sí")
        self._autoresponder_followup_yes_button.setObjectName("SecondaryButton")
        self._autoresponder_followup_yes_button.setCheckable(True)
        self._autoresponder_followup_yes_button.clicked.connect(
            lambda checked=False: self._set_autoresponder_followup_only(True)
        )
        self._autoresponder_followup_no_button = QPushButton("No")
        self._autoresponder_followup_no_button.setObjectName("SecondaryButton")
        self._autoresponder_followup_no_button.setCheckable(True)
        self._autoresponder_followup_no_button.clicked.connect(
            lambda checked=False: self._set_autoresponder_followup_only(False)
        )
        self._autoresponder_followup_group = QButtonGroup(self)
        self._autoresponder_followup_group.setExclusive(True)
        self._autoresponder_followup_group.addButton(self._autoresponder_followup_yes_button)
        self._autoresponder_followup_group.addButton(self._autoresponder_followup_no_button)
        followup_toggle_layout.addWidget(self._autoresponder_followup_yes_button)
        followup_toggle_layout.addWidget(self._autoresponder_followup_no_button)
        followup_toggle_layout.addStretch(1)
        self._autoresponder_followup_summary_label = QLabel("")
        self._autoresponder_followup_summary_label.setObjectName("MutedText")
        config_layout.addWidget(self._autoresponder_followup_summary_label)
        self._set_autoresponder_followup_only(False)
        config_layout.addWidget(_field_card("Solo seguimiento", followup_toggle_wrap))

        layout.addWidget(_field_card("Cuenta", account_wrap))
        layout.addLayout(delay_grid)
        layout.addWidget(_field_card("Configuración", config_wrap))

        self._autoresponder_setup_submit_button = QPushButton("▶ Activar Auto Responder")
        self._autoresponder_setup_submit_button.setObjectName("PrimaryButton")
        self._autoresponder_setup_submit_button.setMinimumHeight(44)
        self._autoresponder_setup_submit_button.clicked.connect(self._submit_autoresponder_setup)
        layout.addWidget(self._autoresponder_setup_submit_button)
        return panel

    def _refresh_autoresponder_setup_sources(self) -> None:
        if not hasattr(self, "_autoresponder_target_combo"):
            return
        records = self._load_active_account_records()
        alias_to_accounts: dict[str, list[str]] = {}
        all_accounts: list[str] = []
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            alias = str(record.get("alias") or "default").strip() or "default"
            alias_to_accounts.setdefault(alias, []).append(username)
            all_accounts.append(username)

        current_key = str(self._autoresponder_target_combo.currentData() or "").strip()
        self._autoresponder_target_index_by_key.clear()
        self._autoresponder_target_combo.blockSignals(True)
        self._autoresponder_target_combo.clear()

        next_index = 0
        if all_accounts:
            all_key = "all::ALL"
            self._autoresponder_target_combo.addItem(
                f"ALL (todas las cuentas activas: {len(all_accounts)})",
                all_key,
            )
            self._autoresponder_target_index_by_key[all_key] = next_index
            next_index += 1

        for alias in sorted(alias_to_accounts):
            users = sorted(set(alias_to_accounts.get(alias, [])))
            key = f"alias::{alias}"
            self._autoresponder_target_combo.addItem(
                f"Alias: {alias} ({len(users)} cuentas)",
                key,
            )
            self._autoresponder_target_index_by_key[key] = next_index
            next_index += 1

        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            alias = str(record.get("alias") or "default").strip() or "default"
            key = f"account::{username}"
            label = f"Cuenta: @{username} (alias: {alias})"
            self._autoresponder_target_combo.addItem(label, key)
            self._autoresponder_target_index_by_key[key] = next_index
            next_index += 1

        target_index = self._autoresponder_target_index_by_key.get(current_key, 0)
        if self._autoresponder_target_combo.count() > 0:
            self._autoresponder_target_combo.setCurrentIndex(max(0, target_index))
        self._autoresponder_target_combo.blockSignals(False)

        current_concurrency = 1
        if hasattr(self, "_autoresponder_concurrency_combo"):
            current_text = self._autoresponder_concurrency_combo.currentText().strip()
            try:
                current_concurrency = max(1, int(current_text or "1"))
            except Exception:
                current_concurrency = 1
            max_concurrency = max(1, min(20, len(all_accounts) if all_accounts else 1))
            self._autoresponder_concurrency_combo.blockSignals(True)
            self._autoresponder_concurrency_combo.clear()
            for value in range(1, max_concurrency + 1):
                self._autoresponder_concurrency_combo.addItem(str(value))
            index = self._autoresponder_concurrency_combo.findText(str(current_concurrency))
            if index < 0:
                index = 0
            self._autoresponder_concurrency_combo.setCurrentIndex(index)
            self._autoresponder_concurrency_combo.blockSignals(False)

    def _add_selected_autoresponder_target(self) -> None:
        if not hasattr(self, "_autoresponder_target_combo"):
            return
        data_key = str(self._autoresponder_target_combo.currentData() or "").strip()
        if not data_key:
            return
        records = self._load_active_account_records()
        account_alias_map = {
            str(item.get("username") or "").strip().lstrip("@"): str(item.get("alias") or "default").strip() or "default"
            for item in records
            if str(item.get("username") or "").strip()
        }
        accounts_to_add: list[str] = []
        if data_key == "all::ALL":
            accounts_to_add = sorted(account_alias_map.keys())
        elif data_key.startswith("alias::"):
            alias_value = data_key.split("::", 1)[1].strip().lower()
            accounts_to_add = sorted(
                user
                for user, alias in account_alias_map.items()
                if alias.lower() == alias_value
            )
        elif data_key.startswith("account::"):
            account_value = data_key.split("::", 1)[1].strip().lstrip("@")
            if account_value:
                accounts_to_add = [account_value]

        if not accounts_to_add:
            self._set_autoresponder_setup_validation_error(
                "No se pudieron resolver cuentas activas para la selección."
            )
            return

        selected = list(self._autoresponder_selected_accounts)
        for account in accounts_to_add:
            if account not in selected:
                selected.append(account)
        self._autoresponder_selected_accounts = selected
        self._refresh_autoresponder_selected_accounts_view()
        self._set_autoresponder_setup_message("Configuración lista para activar.")

    def _clear_autoresponder_selected_accounts(self) -> None:
        self._autoresponder_selected_accounts = []
        self._refresh_autoresponder_selected_accounts_view()

    def _refresh_autoresponder_selected_accounts_view(self) -> None:
        if not hasattr(self, "_autoresponder_selected_accounts_layout"):
            return
        while self._autoresponder_selected_accounts_layout.count():
            item = self._autoresponder_selected_accounts_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        if not self._autoresponder_selected_accounts:
            empty_label = QLabel("(sin cuentas seleccionadas)")
            empty_label.setObjectName("MutedText")
            self._autoresponder_selected_accounts_layout.addWidget(empty_label, 0, 0)
            return

        for idx, account in enumerate(self._autoresponder_selected_accounts):
            chip = QLabel(f"@{account} ✔")
            chip.setObjectName("ExecTag")
            chip.setAlignment(Qt.AlignCenter)
            row = idx // 3
            col = idx % 3
            self._autoresponder_selected_accounts_layout.addWidget(chip, row, col)

    def _toggle_autoresponder_hour(self, hour: int, checked: bool) -> None:
        value = max(1, int(hour))
        if checked:
            self._autoresponder_selected_hours.add(value)
        else:
            self._autoresponder_selected_hours.discard(value)
        self._refresh_autoresponder_hours_summary()

    def _add_autoresponder_custom_hour(self) -> None:
        raw = str(self._autoresponder_custom_hour_input.text() or "").strip()
        if not raw:
            return
        tokens = [token.strip() for token in re.split(r"[,\s;|/]+", raw) if token.strip()]
        if not tokens:
            return

        parsed_values: list[int] = []
        invalid_tokens: list[str] = []
        for token in tokens:
            try:
                parsed_values.append(max(1, int(token)))
            except Exception:
                invalid_tokens.append(token)

        if invalid_tokens:
            self._set_autoresponder_setup_validation_error(
                "Las horas personalizadas deben ser números enteros."
            )
            return

        for value in parsed_values:
            self._autoresponder_selected_hours.add(value)
            preset_button = self._autoresponder_hour_buttons.get(value)
            if preset_button is not None and not preset_button.isChecked():
                preset_button.setChecked(True)
        self._autoresponder_custom_hour_input.clear()
        self._refresh_autoresponder_hours_summary()
        if len(parsed_values) == 1:
            self._set_autoresponder_setup_message("Hora personalizada agregada.")
        else:
            self._set_autoresponder_setup_message("Horas personalizadas agregadas.")

    def _remove_autoresponder_custom_hour(self) -> None:
        raw = str(self._autoresponder_custom_hour_input.text() or "").strip()
        if not raw:
            self._set_autoresponder_setup_validation_error(
                "Escribe una hora para quitarla."
            )
            return
        try:
            value = max(1, int(raw))
        except Exception:
            self._set_autoresponder_setup_validation_error(
                "La hora a quitar debe ser un número entero."
            )
            return
        if value not in self._autoresponder_selected_hours:
            self._set_autoresponder_setup_validation_error(
                f"La hora {value} no está seleccionada."
            )
            return
        self._autoresponder_selected_hours.discard(value)
        preset_button = self._autoresponder_hour_buttons.get(value)
        if preset_button is not None and preset_button.isChecked():
            preset_button.setChecked(False)
        self._autoresponder_custom_hour_input.clear()
        self._refresh_autoresponder_hours_summary()
        self._set_autoresponder_setup_message("Hora eliminada.")

    def _clear_autoresponder_hours(self) -> None:
        self._autoresponder_selected_hours.clear()
        for button in self._autoresponder_hour_buttons.values():
            if button.isChecked():
                button.setChecked(False)
        self._refresh_autoresponder_hours_summary()
        self._set_autoresponder_setup_message("Horas de seguimiento limpiadas.")

    def _refresh_autoresponder_hours_summary(self) -> None:
        if not hasattr(self, "_autoresponder_hours_summary_label"):
            return
        values = sorted({max(1, int(value)) for value in self._autoresponder_selected_hours})
        if values:
            self._autoresponder_hours_summary_label.setText(
                "Seleccionadas: " + " | ".join(str(value) for value in values)
            )
        else:
            self._autoresponder_hours_summary_label.setText("(sin horas seleccionadas)")

    def _set_autoresponder_followup_only(self, enabled: bool) -> None:
        self._autoresponder_followup_only = bool(enabled)
        if hasattr(self, "_autoresponder_followup_yes_button"):
            self._autoresponder_followup_yes_button.setChecked(self._autoresponder_followup_only)
            self._autoresponder_followup_yes_button.setText(
                "Sí ✓" if self._autoresponder_followup_only else "Sí"
            )
            self._autoresponder_followup_yes_button.setProperty(
                "active", self._autoresponder_followup_only
            )
            self._autoresponder_followup_yes_button.style().unpolish(
                self._autoresponder_followup_yes_button
            )
            self._autoresponder_followup_yes_button.style().polish(
                self._autoresponder_followup_yes_button
            )
            self._autoresponder_followup_yes_button.update()
        if hasattr(self, "_autoresponder_followup_no_button"):
            self._autoresponder_followup_no_button.setChecked(not self._autoresponder_followup_only)
            self._autoresponder_followup_no_button.setText(
                "No ✓" if not self._autoresponder_followup_only else "No"
            )
            self._autoresponder_followup_no_button.setProperty(
                "active", not self._autoresponder_followup_only
            )
            self._autoresponder_followup_no_button.style().unpolish(
                self._autoresponder_followup_no_button
            )
            self._autoresponder_followup_no_button.style().polish(
                self._autoresponder_followup_no_button
            )
            self._autoresponder_followup_no_button.update()
        if hasattr(self, "_autoresponder_followup_summary_label"):
            self._autoresponder_followup_summary_label.setText(
                f"Seleccionado: {'Sí' if self._autoresponder_followup_only else 'No'}"
            )

    def _set_autoresponder_setup_validation_error(self, message: str) -> None:
        self._set_status("Waiting input")
        if hasattr(self, "_autoresponder_setup_message_label"):
            self._autoresponder_setup_message_label.setText(message)
        self._append_log(f"[gui] autoresponder setup validation: {message}\n")

    def _set_autoresponder_setup_message(self, message: str) -> None:
        if hasattr(self, "_autoresponder_setup_message_label"):
            self._autoresponder_setup_message_label.setText(message)

    def _resolve_autoresponder_alias_submission_value(self) -> tuple[str, str]:
        selected_accounts = [str(item or "").strip().lstrip("@") for item in self._autoresponder_selected_accounts]
        selected_accounts = [item for item in selected_accounts if item]
        if not selected_accounts:
            return "", "Selecciona al menos una cuenta."

        records = self._load_active_account_records()
        active_users = [str(item.get("username") or "").strip().lstrip("@") for item in records]
        active_users = [item for item in active_users if item]
        selected_norm = {item.lower() for item in selected_accounts}
        active_norm = {item.lower() for item in active_users}
        if selected_norm == active_norm:
            return "ALL", ""

        alias_map: dict[str, set[str]] = {}
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            alias = str(record.get("alias") or "default").strip() or "default"
            if not username:
                continue
            alias_map.setdefault(alias, set()).add(username.lower())
        for alias, users in alias_map.items():
            if selected_norm == users:
                return alias, ""

        if len(selected_accounts) == 1:
            return selected_accounts[0], ""

        return (
            "",
            "Para múltiples cuentas, selecciona todas las cuentas de un alias o ALL.",
        )

    def _build_autoresponder_setup_values(self) -> Optional[dict[str, str]]:
        alias_value, alias_error = self._resolve_autoresponder_alias_submission_value()
        if alias_error:
            self._set_autoresponder_setup_validation_error(alias_error)
            return None

        delay_min = int(self._autoresponder_delay_min_spin.value())
        delay_max = int(self._autoresponder_delay_max_spin.value())
        if delay_min >= delay_max:
            self._set_autoresponder_setup_validation_error(
                "Delay mínimo debe ser menor que delay máximo."
            )
            return None

        concurrency_value = self._autoresponder_concurrency_combo.currentText().strip() or "1"
        try:
            concurrency = max(1, int(concurrency_value))
        except Exception:
            concurrency = 1
        threads_value = max(1, int(self._autoresponder_threads_spin.value()))
        hours_values = sorted({max(1, int(item)) for item in self._autoresponder_selected_hours})
        if not hours_values:
            self._set_autoresponder_setup_validation_error(
                "Selecciona al menos una hora de seguimiento."
            )
            return None

        return {
            "alias": alias_value,
            "delay_min": str(delay_min),
            "delay_max": str(delay_max),
            "concurrency": str(concurrency),
            "threads": str(threads_value),
            "followup_hours": ",".join(str(item) for item in hours_values),
            "followup_only": "s" if self._autoresponder_followup_only else "n",
        }

    def _start_autoresponder_setup_from_menu(self, option_value: str) -> None:
        if self._block_navigation or self._autoresponder_running:
            return
        self._autoresponder_activate_option_value = str(option_value or "").strip()
        if not self._autoresponder_activate_option_value:
            self._set_autoresponder_setup_validation_error(
                "No se pudo resolver la opción de activación del menú."
            )
            return
        self._set_execution_mode(False)
        self._menu_title_label.setVisible(True)
        self._menu_title_label.setText("Auto responder con OpenAI")
        self._menu_prompt_label.setVisible(True)
        self._menu_prompt_label.setText(
            "Completa la configuración en el panel y activa el bot con un solo clic."
        )
        self._set_autoresponder_setup_visible(True)
        self._set_live_input_enabled(False, prompt="")
        if not self._showing_summary and not self._block_navigation:
            self._set_page(self.PAGE_MENU)

    def _submit_autoresponder_setup(self) -> None:
        if self._block_navigation:
            return
        if self._active_sidebar_key != "autoresponder":
            return
        if self._autoresponder_setup_auto_fill_active:
            return
        pending = self._pending_request
        if not isinstance(pending, InputRequest):
            self._set_autoresponder_setup_validation_error(
                "Espera a que el CLI solicite input para iniciar el auto responder."
            )
            return

        activate_value = str(self._autoresponder_activate_option_value or "").strip()
        if not activate_value:
            self._set_autoresponder_setup_validation_error(
                "No se encontró la opción 'Activar bot (alias/grupo)' en el menú."
            )
            return

        pending_custom_hours = str(self._autoresponder_custom_hour_input.text() or "").strip()
        if pending_custom_hours:
            self._add_autoresponder_custom_hour()

        if not self._autoresponder_selected_accounts:
            self._add_selected_autoresponder_target()

        values_map = self._build_autoresponder_setup_values()
        if not values_map:
            return

        self._autoresponder_setup_queue = [
            str(values_map.get("alias", "")),
            str(values_map.get("delay_min", "")),
            str(values_map.get("delay_max", "")),
            str(values_map.get("concurrency", "")),
            str(values_map.get("threads", "")),
            str(values_map.get("followup_hours", "")),
            str(values_map.get("followup_only", "")),
        ]
        self._autoresponder_setup_auto_fill_active = True
        self._append_log(
            f"[gui] autoresponder config enviada: alias={values_map.get('alias', '-')} "
            f"delay={values_map.get('delay_min', '-')}..{values_map.get('delay_max', '-')}\n"
        )
        self._set_autoresponder_setup_message("Iniciando auto responder...")
        self._show_autoresponder_loading_overlay()
        self._submit_current_input(activate_value)

    def _consume_autoresponder_setup_auto_fill(self, request: InputRequest) -> bool:
        if self._active_sidebar_key != "autoresponder" or not self._autoresponder_setup_auto_fill_active:
            return False
        if not self._autoresponder_setup_queue:
            self._autoresponder_setup_auto_fill_active = False
            self._hide_autoresponder_loading_overlay()
            return False

        outbound = str(self._autoresponder_setup_queue.pop(0)).strip()
        if not outbound:
            self._autoresponder_setup_auto_fill_active = False
            self._autoresponder_setup_queue.clear()
            self._hide_autoresponder_loading_overlay()
            return False

        adapter = self._io_adapter
        if adapter is None:
            self._autoresponder_setup_auto_fill_active = False
            self._autoresponder_setup_queue.clear()
            self._hide_autoresponder_loading_overlay()
            return False
        accepted = adapter.fulfill_input(outbound, request_id=request.request_id)
        if not accepted and request.is_menu:
            menu_mapped = self._resolve_autoresponder_menu_value(request, outbound)
            if menu_mapped and menu_mapped != outbound:
                outbound = menu_mapped
                accepted = adapter.fulfill_input(outbound, request_id=request.request_id)
        if not accepted:
            self._autoresponder_setup_auto_fill_active = False
            self._autoresponder_setup_queue.clear()
            self._hide_autoresponder_loading_overlay()
            return False

        visible = "***" if request.sensitive and outbound else outbound
        self._append_log(f"[gui] input submitted: {visible}\n")
        if not self._autoresponder_setup_queue:
            self._autoresponder_setup_auto_fill_active = False
            self._autoresponder_setup_queue.clear()
            self._set_autoresponder_setup_visible(False)
        return True

    def _resolve_autoresponder_menu_value(self, request: InputRequest, outbound: str) -> str:
        candidate = str(outbound or "").strip()
        if not candidate:
            return candidate
        options = list(request.menu_options or [])
        if not options:
            return candidate
        candidate_norm = _normalized_label(candidate)
        candidate_compact = candidate.lower().replace(" ", "")

        for option in options:
            option_value = str(option.value or "").strip()
            if option_value and option_value == candidate:
                return option_value

        for option in options:
            option_label = str(option.label or "").strip()
            option_value = str(option.value or "").strip()
            if not option_label or not option_value:
                continue
            label_norm = _normalized_label(option_label)
            label_compact = option_label.lower().replace(" ", "")
            if candidate_norm and label_norm == candidate_norm:
                return option_value
            if candidate_compact and label_compact == candidate_compact:
                return option_value

        return candidate

    def _reset_autoresponder_setup_mode(self) -> None:
        self._autoresponder_setup_auto_fill_active = False
        self._autoresponder_setup_queue.clear()
        self._hide_autoresponder_loading_overlay()
        self._autoresponder_activate_option_value = ""
        self._set_autoresponder_setup_visible(False)

    def _handle_autoresponder_setup_back(self) -> None:
        if self._active_sidebar_key != "autoresponder":
            return
        if self._autoresponder_running:
            return
        if self._autoresponder_setup_auto_fill_active:
            return
        self._reset_autoresponder_setup_mode()
        self._set_page(self.PAGE_MENU)

    def _build_execution_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self._send_execution_container = QWidget()
        send_layout = QVBoxLayout(self._send_execution_container)
        send_layout.setContentsMargins(0, 0, 0, 0)
        send_layout.setSpacing(10)

        tags_row = QHBoxLayout()
        tags_row.setContentsMargins(0, 0, 0, 0)
        tags_row.setSpacing(8)
        self._exec_alias_tag = self._build_exec_tag("Alias: -")
        self._exec_pending_tag = self._build_exec_tag("Leads pendientes: -")
        self._exec_time_tag = self._build_exec_tag("Hora: --:--:--")
        tags_row.addWidget(self._exec_alias_tag)
        tags_row.addWidget(self._exec_pending_tag)
        tags_row.addWidget(self._exec_time_tag)
        tags_row.addStretch(1)
        send_layout.addLayout(tags_row)

        totals_card = QFrame()
        totals_card.setObjectName("ExecCard")
        totals_layout = QVBoxLayout(totals_card)
        totals_layout.setContentsMargins(10, 10, 10, 10)
        totals_layout.setSpacing(8)

        totals_title = QLabel("Totales por cuenta (esta campaña)")
        totals_title.setObjectName("PageTitle")
        totals_layout.addWidget(totals_title)

        metrics_grid = QGridLayout()
        metrics_grid.setContentsMargins(0, 0, 0, 0)
        metrics_grid.setHorizontalSpacing(10)
        metrics_grid.setVerticalSpacing(8)
        self._exec_metric_labels: dict[str, QLabel] = {}
        metric_items = [
            ("sent", "Mensajes enviados"),
            ("error", "Mensajes con error"),
            ("unverified", "Enviados sin verificación"),
            ("skipped", "Saltados sin DM"),
            ("concurrency", "Concurrencia"),
        ]
        for idx, (key, title) in enumerate(metric_items):
            row = idx // 3
            col = idx % 3
            card = self._build_exec_metric(key, title, "0")
            value_label = card.findChild(QLabel, f"ExecMetricValue_{key}")
            if value_label is not None:
                self._exec_metric_labels[key] = value_label
            metrics_grid.addWidget(card, row, col)
        totals_layout.addLayout(metrics_grid)
        send_layout.addWidget(totals_card)

        self.campaign_summary_container = QFrame()
        self.campaign_summary_container.setObjectName("campaign_summary_container")
        campaign_summary_layout = QVBoxLayout(self.campaign_summary_container)
        campaign_summary_layout.setContentsMargins(10, 10, 10, 10)
        campaign_summary_layout.setSpacing(10)

        campaign_summary_title = QLabel("RESUMEN DE CAMPAÑA FINALIZADA")
        campaign_summary_title.setObjectName("CampaignSummaryTitle")
        campaign_summary_title.setAlignment(Qt.AlignCenter)
        campaign_summary_layout.addWidget(campaign_summary_title)

        campaign_summary_separator = QFrame()
        campaign_summary_separator.setObjectName("CampaignSummarySeparator")
        campaign_summary_separator.setFixedHeight(1)
        campaign_summary_layout.addWidget(campaign_summary_separator)

        summary_cards_row = QHBoxLayout()
        summary_cards_row.setContentsMargins(0, 0, 0, 0)
        summary_cards_row.setSpacing(10)
        summary_cards = [
            ("sent_ok", "Mensajes enviados correctamente", "success"),
            ("error", "Mensajes con error", "danger"),
            ("accounts", "Cuentas utilizadas", "info"),
        ]
        self._campaign_summary_labels: dict[str, QLabel] = {}
        for key, title, tone in summary_cards:
            card = QFrame()
            card.setObjectName("CampaignSummaryMetricCard")
            card.setProperty("tone", tone)
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 10, 10, 10)
            card_layout.setSpacing(4)

            title_label = QLabel(title)
            title_label.setObjectName("MetricLabel")
            title_label.setAlignment(Qt.AlignCenter)
            title_label.setWordWrap(True)

            value_label = QLabel("0")
            value_label.setObjectName("CampaignSummaryMetricValue")
            value_label.setAlignment(Qt.AlignCenter)
            self._campaign_summary_labels[key] = value_label

            card_layout.addWidget(title_label)
            card_layout.addWidget(value_label)
            summary_cards_row.addWidget(card, 1)
        campaign_summary_layout.addLayout(summary_cards_row)

        self._campaign_summary_table = QTableWidget(3, 4)
        self._campaign_summary_table.setObjectName("CampaignSummaryTable")
        self._campaign_summary_table.setHorizontalHeaderLabels(
            ["Cuenta", "Enviados OK", "Con error", "Total"]
        )
        self._campaign_summary_table.verticalHeader().setVisible(False)
        self._campaign_summary_table.setSelectionMode(QAbstractItemView.NoSelection)
        self._campaign_summary_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._campaign_summary_table.setFocusPolicy(Qt.NoFocus)
        self._campaign_summary_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._campaign_summary_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._campaign_summary_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._campaign_summary_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._campaign_summary_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)

        summary_table_rows = [
            ("@cuenta_1", "0", "0", "0"),
            ("@cuenta_2", "0", "0", "0"),
            ("TOTAL", "0", "0", "0"),
        ]
        for row, row_values in enumerate(summary_table_rows):
            for col, value in enumerate(row_values):
                item = QTableWidgetItem(value)
                if col > 0:
                    item.setTextAlignment(Qt.AlignCenter)
                self._campaign_summary_table.setItem(row, col, item)

        campaign_summary_layout.addWidget(self._campaign_summary_table)
        self.campaign_summary_container.setVisible(False)
        send_layout.addWidget(self.campaign_summary_container)

        inflight_card = QFrame()
        inflight_card.setObjectName("ExecCard")
        inflight_layout = QVBoxLayout(inflight_card)
        inflight_layout.setContentsMargins(10, 10, 10, 10)
        inflight_layout.setSpacing(8)
        inflight_title = QLabel("Envíos en vuelo")
        inflight_title.setObjectName("PageTitle")
        inflight_layout.addWidget(inflight_title)

        self._exec_inflight_table = QTableWidget(0, 5)
        self._exec_inflight_table.setObjectName("ExecInflightTable")
        self._exec_inflight_table.setHorizontalHeaderLabels(
            ["Cuenta", "Lead", "Hora", "Resultado", "Detalle"]
        )
        self._exec_inflight_table.verticalHeader().setVisible(False)
        self._exec_inflight_table.setSelectionMode(QAbstractItemView.NoSelection)
        self._exec_inflight_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._exec_inflight_table.setFocusPolicy(Qt.NoFocus)
        self._exec_inflight_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._exec_inflight_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._exec_inflight_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        inflight_layout.addWidget(self._exec_inflight_table)
        send_layout.addWidget(inflight_card, 1)

        logs_card = QFrame()
        logs_card.setObjectName("ExecCard")
        logs_layout = QVBoxLayout(logs_card)
        logs_layout.setContentsMargins(10, 10, 10, 10)
        logs_layout.setSpacing(8)
        logs_title_row = QHBoxLayout()
        logs_title_row.setContentsMargins(0, 0, 0, 0)
        logs_title_row.setSpacing(8)
        logs_title = QLabel("Logs")
        logs_title.setObjectName("PageTitle")

        self._exec_stop_button = QPushButton("Frenar campaña")
        self._exec_stop_button.setObjectName("SecondaryButton")
        self._exec_stop_button.clicked.connect(self._request_campaign_stop)
        self._exec_stop_button.setEnabled(False)

        clear_button = QPushButton("Clear")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_execution_view)
        logs_title_row.addWidget(logs_title)
        logs_title_row.addStretch(1)
        logs_title_row.addWidget(self._exec_stop_button)
        logs_title_row.addWidget(clear_button)
        logs_layout.addLayout(logs_title_row)

        self._exec_log_console = QPlainTextEdit()
        self._exec_log_console.setObjectName("ExecLogConsole")
        self._exec_log_console.setReadOnly(True)
        self._exec_log_console.setMaximumBlockCount(1800)
        self._exec_log_console.setLineWrapMode(QPlainTextEdit.NoWrap)
        self._exec_log_console.setPlaceholderText("No activity yet.")
        logs_layout.addWidget(self._exec_log_console, 1)
        send_layout.addWidget(logs_card, 2)

        self._autoresponder_execution_container = self._build_autoresponder_execution_panel()
        self._autoresponder_execution_container.setVisible(False)
        self._leads_execution_container = self._build_leads_execution_panel()
        self._leads_execution_container.setVisible(False)

        layout.addWidget(self._send_execution_container, 1)
        layout.addWidget(self._autoresponder_execution_container, 1)
        layout.addWidget(self._leads_execution_container, 1)

        self._clear_execution_view()
        self._reset_autoresponder_execution_view()
        self._reset_leads_execution_view()
        self._set_execution_view(None)
        return page

    def _build_autoresponder_execution_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("ExecCard")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        title = QLabel("🤖 Auto-responder activo")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        status_grid = QGridLayout()
        status_grid.setContentsMargins(0, 0, 0, 0)
        status_grid.setHorizontalSpacing(10)
        status_grid.setVerticalSpacing(8)

        self._autoresponder_accounts_label = self._build_autoresponder_status_block()
        self._autoresponder_responses_label = self._build_autoresponder_status_block()
        self._autoresponder_followups_label = self._build_autoresponder_status_block()
        status_grid.addWidget(self._autoresponder_accounts_label, 0, 0)
        status_grid.addWidget(self._autoresponder_responses_label, 0, 1)
        status_grid.addWidget(self._autoresponder_followups_label, 0, 2)
        layout.addLayout(status_grid)

        runtime_card = QFrame()
        runtime_card.setObjectName("ExecCard")
        runtime_layout = QVBoxLayout(runtime_card)
        runtime_layout.setContentsMargins(10, 10, 10, 10)
        runtime_layout.setSpacing(6)
        runtime_title = QLabel("🕒 Runtime")
        runtime_title.setObjectName("PageTitle")
        self._autoresponder_runtime_label = QLabel("")
        self._autoresponder_runtime_label.setObjectName("MutedText")
        self._autoresponder_runtime_label.setWordWrap(True)
        runtime_layout.addWidget(runtime_title)
        runtime_layout.addWidget(self._autoresponder_runtime_label)
        layout.addWidget(runtime_card)

        logs_card = QFrame()
        logs_card.setObjectName("ExecCard")
        logs_layout = QVBoxLayout(logs_card)
        logs_layout.setContentsMargins(10, 10, 10, 10)
        logs_layout.setSpacing(8)
        logs_title_row = QHBoxLayout()
        logs_title_row.setContentsMargins(0, 0, 0, 0)
        logs_title_row.setSpacing(8)
        logs_title = QLabel("Logs auto-responder")
        logs_title.setObjectName("PageTitle")

        self._autoresponder_stop_button = QPushButton("Frenar (Q)")
        self._autoresponder_stop_button.setObjectName("SecondaryButton")
        self._autoresponder_stop_button.clicked.connect(self._request_autoresponder_stop)
        self._autoresponder_stop_button.setEnabled(False)

        clear_button = QPushButton("Clear")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_autoresponder_execution_log)
        logs_title_row.addWidget(logs_title)
        logs_title_row.addStretch(1)
        logs_title_row.addWidget(self._autoresponder_stop_button)
        logs_title_row.addWidget(clear_button)
        logs_layout.addLayout(logs_title_row)

        self._autoresponder_log_console = QPlainTextEdit()
        self._autoresponder_log_console.setObjectName("ExecLogConsole")
        self._autoresponder_log_console.setReadOnly(True)
        self._autoresponder_log_console.setMaximumBlockCount(1800)
        self._autoresponder_log_console.setLineWrapMode(QPlainTextEdit.NoWrap)
        self._autoresponder_log_console.setPlaceholderText("No activity yet.")
        logs_layout.addWidget(self._autoresponder_log_console, 1)
        layout.addWidget(logs_card, 1)

        return panel

    def _build_autoresponder_status_block(self) -> QLabel:
        label = QLabel("-")
        label.setObjectName("ExecTag")
        label.setAlignment(Qt.AlignCenter)
        label.setWordWrap(True)
        return label

    def _build_leads_execution_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("ExecCard")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        title = QLabel("Filtrado de leads activo")
        title.setObjectName("PageTitle")

        self._leads_stop_button = QPushButton("Frenar filtrado")
        self._leads_stop_button.setObjectName("SecondaryButton")
        self._leads_stop_button.setEnabled(False)
        self._leads_stop_button.clicked.connect(self._request_leads_filter_stop)

        header_row.addWidget(title)
        header_row.addStretch(1)
        header_row.addWidget(self._leads_stop_button)
        layout.addLayout(header_row)

        metrics_card = QFrame()
        metrics_card.setObjectName("ExecCard")
        metrics_layout = QVBoxLayout(metrics_card)
        metrics_layout.setContentsMargins(10, 10, 10, 10)
        metrics_layout.setSpacing(8)

        metrics_title = QLabel("Estado del filtrado")
        metrics_title.setObjectName("PageTitle")
        metrics_layout.addWidget(metrics_title)

        metrics_grid = QGridLayout()
        metrics_grid.setContentsMargins(0, 0, 0, 0)
        metrics_grid.setHorizontalSpacing(10)
        metrics_grid.setVerticalSpacing(8)
        self._leads_metric_labels: dict[str, QLabel] = {}
        metric_items = [
            ("total_target", "Leads a filtrar", "-"),
            ("eta", "Tiempo estimado", "-"),
            ("accounts", "Alias / cuentas en uso", "-"),
            ("qualified", "Cuentas calificadas", "0"),
            ("discarded", "Cuentas descartadas", "0"),
            ("processed", "Totales procesadas", "0"),
        ]
        for idx, (key, title_text, initial) in enumerate(metric_items):
            row = idx // 3
            col = idx % 3
            card = self._build_exec_metric(key=f"leads_{key}", title=title_text, initial=initial)
            value_label = card.findChild(QLabel, f"ExecMetricValue_leads_{key}")
            if value_label is not None:
                value_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                value_label.setWordWrap(True)
                self._leads_metric_labels[key] = value_label
            metrics_grid.addWidget(card, row, col)
        metrics_layout.addLayout(metrics_grid)
        layout.addWidget(metrics_card)

        logs_card = QFrame()
        logs_card.setObjectName("ExecCard")
        logs_layout = QVBoxLayout(logs_card)
        logs_layout.setContentsMargins(10, 10, 10, 10)
        logs_layout.setSpacing(8)

        logs_title_row = QHBoxLayout()
        logs_title_row.setContentsMargins(0, 0, 0, 0)
        logs_title_row.setSpacing(8)
        logs_title = QLabel("Log del filtrado")
        logs_title.setObjectName("PageTitle")

        clear_button = QPushButton("Clear")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_leads_execution_log)
        logs_title_row.addWidget(logs_title)
        logs_title_row.addStretch(1)
        logs_title_row.addWidget(clear_button)
        logs_layout.addLayout(logs_title_row)

        self._leads_log_console = QPlainTextEdit()
        self._leads_log_console.setObjectName("ExecLogConsole")
        self._leads_log_console.setReadOnly(True)
        self._leads_log_console.setMaximumBlockCount(2000)
        self._leads_log_console.setLineWrapMode(QPlainTextEdit.NoWrap)
        self._leads_log_console.setPlaceholderText("Sin actividad de filtrado.")
        logs_layout.addWidget(self._leads_log_console, 1)
        layout.addWidget(logs_card, 1)

        return panel

    def _build_exec_tag(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setObjectName("ExecTag")
        label.setAlignment(Qt.AlignCenter)
        return label

    def _build_exec_metric(self, key: str, title: str, initial: str) -> QWidget:
        card = QFrame()
        card.setObjectName("ExecMetricCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(4)
        title_label = QLabel(title)
        title_label.setObjectName("MetricLabel")
        value_label = QLabel(initial)
        value_label.setObjectName(f"ExecMetricValue_{key}")
        value_label.setProperty("class", "ExecMetricValue")
        value_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return card

    def _clear_execution_view(self) -> None:
        self._exec_alias_tag.setText("Alias: -")
        self._exec_pending_tag.setText("Leads pendientes: -")
        self._exec_time_tag.setText(f"Hora: {datetime.now().strftime('%H:%M:%S')}")
        for label in self._exec_metric_labels.values():
            label.setText("0")
        self._exec_inflight_rows.clear()
        self._exec_inflight_table.setRowCount(0)
        self._exec_log_console.clear()
        self._execution_log_times.clear()

    def _clear_autoresponder_execution_log(self) -> None:
        self._autoresponder_log_console.clear()

    def _clear_leads_execution_log(self) -> None:
        self._leads_log_console.clear()

    def _reset_autoresponder_execution_view(self) -> None:
        self._autoresponder_active_accounts = 0
        self._autoresponder_responses_ok = 0
        self._autoresponder_responses_error = 0
        self._autoresponder_followups_ok = 0
        self._autoresponder_followups_error = 0
        self._autoresponder_followups_attempted = None
        self._autoresponder_started_at = 0.0
        self._autoresponder_log_console.clear()
        self._update_autoresponder_status_blocks()
        self._update_autoresponder_runtime_text()

    def _reset_leads_execution_view(self) -> None:
        self._leads_filter_running = False
        self._leads_filter_started_at = 0.0
        self._leads_total_target = None
        self._leads_processed_count = 0
        self._leads_qualified_count = 0
        self._leads_discarded_count = 0
        self._leads_account_alias = "-"
        self._leads_export_alias = "leads_filtrados"
        self._leads_accounts_planned = None
        self._leads_account_usernames = []
        self._leads_waiting_manual_accounts = False
        self._leads_accounts_seen.clear()
        self._leads_counts_prefill_on_next_start = None
        self._leads_stop_requested = False
        self._leads_stop_prompt_active = False
        self._leads_completion_announced = False
        if hasattr(self, "_leads_stop_button"):
            self._leads_stop_button.setEnabled(False)
        if hasattr(self, "_leads_log_console"):
            self._leads_log_console.clear()
        if hasattr(self, "_leads_live_submitted_console"):
            self._leads_live_submitted_console.clear()
        self._update_leads_metrics_view()

    @staticmethod
    def _format_elapsed_hhmmss(seconds: float) -> str:
        total = max(0, int(seconds))
        hh = total // 3600
        mm = (total % 3600) // 60
        ss = total % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    @staticmethod
    def _format_delay_seconds(seconds: float) -> str:
        rounded = round(seconds, 2)
        if abs(rounded - int(rounded)) < 1e-9:
            return str(int(rounded))
        return f"{rounded:.2f}".rstrip("0").rstrip(".")

    def _autoresponder_delay_text(self) -> str:
        delay_min = self._autoresponder_delay_min_s
        delay_max = self._autoresponder_delay_max_s
        if delay_min is None and delay_max is None:
            return "-"
        if delay_min is None:
            delay_min = delay_max
        if delay_max is None:
            delay_max = delay_min
        if delay_min is None or delay_max is None:
            return "-"
        if abs(delay_min - delay_max) < 1e-9:
            return f"{self._format_delay_seconds(delay_min)}s"
        return (
            f"{self._format_delay_seconds(delay_min)}s - "
            f"{self._format_delay_seconds(delay_max)}s"
        )

    def _update_autoresponder_status_blocks(self) -> None:
        alias_text = "-"
        alias_value, _alias_error = self._resolve_autoresponder_alias_submission_value()
        if alias_value:
            alias_text = str(alias_value).strip() or "-"

        hours_values = sorted({max(1, int(value)) for value in self._autoresponder_selected_hours})
        if hours_values:
            hours_text = " / ".join(f"{value}h" for value in hours_values)
        else:
            hours_text = "-"

        responses_error_label = (
            f"⚠️ ❌ Error: {self._autoresponder_responses_error}"
            if self._autoresponder_responses_error > 0
            else "❌ Error: 0"
        )
        followups_error_label = (
            f"⚠️ ❌ Error: {self._autoresponder_followups_error}"
            if self._autoresponder_followups_error > 0
            else "❌ Error: 0"
        )

        self._autoresponder_accounts_label.setText(
            f"👥 Cuentas activas con bot: {self._autoresponder_active_accounts}\n"
            f"🏷 Alias: {alias_text}\n"
            f"⏱ Horas de seguimiento: {hours_text}"
        )
        self._autoresponder_responses_label.setText(
            "💬 Respuestas enviadas:\n"
            f"✅ OK: {self._autoresponder_responses_ok}\n"
            f"{responses_error_label}"
        )
        self._autoresponder_followups_label.setText(
            "🔁 Seguimientos enviados:\n"
            f"✅ OK: {self._autoresponder_followups_ok}\n"
            f"{followups_error_label}"
        )

    def _update_autoresponder_runtime_text(self) -> None:
        elapsed = 0.0
        if self._autoresponder_running and self._autoresponder_started_at > 0:
            elapsed = max(0.0, time.monotonic() - self._autoresponder_started_at)
        self._autoresponder_runtime_label.setText(
            "⏳ Delay configurado: "
            f"{self._autoresponder_delay_text()}\n"
            "⌛ Tiempo activo: "
            f"{self._format_elapsed_hhmmss(elapsed)}"
        )

    def _format_eta_text(self) -> str:
        if not self._leads_filter_running:
            if self._leads_total_target is not None and self._leads_processed_count >= self._leads_total_target:
                return "Completado"
            return "-"
        if self._leads_total_target is None or self._leads_processed_count <= 0:
            return "Calculando..."
        remaining = max(0, self._leads_total_target - self._leads_processed_count)
        if remaining <= 0:
            return "00:00:00"
        elapsed = 0.0
        if self._leads_filter_started_at > 0:
            elapsed = max(0.0, time.monotonic() - self._leads_filter_started_at)
        if elapsed <= 0:
            return "Calculando..."
        avg_seconds = elapsed / max(1, self._leads_processed_count)
        eta_seconds = avg_seconds * remaining
        return self._format_elapsed_hhmmss(eta_seconds)

    def _accounts_metric_text(self) -> str:
        alias = self._leads_account_alias or "-"
        if self._leads_accounts_planned is not None:
            return f"{alias} · {self._leads_accounts_planned} cuentas"
        seen = len(self._leads_accounts_seen)
        if seen > 0:
            return f"{alias} · {seen} cuentas activas"
        return alias

    def _update_leads_metrics_view(self) -> None:
        if not hasattr(self, "_leads_metric_labels"):
            return
        total_target_text = (
            str(self._leads_total_target) if self._leads_total_target is not None else "-"
        )
        values = {
            "total_target": total_target_text,
            "eta": self._format_eta_text(),
            "accounts": self._accounts_metric_text(),
            "qualified": str(self._leads_qualified_count),
            "discarded": str(self._leads_discarded_count),
            "processed": str(self._leads_processed_count),
        }
        for key, value in values.items():
            label = self._leads_metric_labels.get(key)
            if label is not None:
                label.setText(value)

    def _set_leads_filter_running(self, running: bool) -> None:
        if self._leads_filter_running == running:
            return
        self._leads_filter_running = running
        if hasattr(self, "_leads_stop_button"):
            self._leads_stop_button.setEnabled(running)
        if running:
            self._leads_filter_started_at = time.monotonic()
            self._set_execution_mode(True)
        else:
            self._leads_filter_started_at = 0.0
            self._set_execution_mode(self._campaign_running or self._autoresponder_running)
        self._update_leads_metrics_view()

    @staticmethod
    def _append_spaced_log_entry(console: QPlainTextEdit, text: str) -> None:
        cursor = console.textCursor()
        cursor.movePosition(QTextCursor.End)
        if console.document().characterCount() > 1:
            cursor.insertText("\n\n")
        cursor.insertText(text)
        console.setTextCursor(cursor)
        console.ensureCursorVisible()

    def _format_execution_log_line(self, line: str) -> str:
        text = _strip_cli_hints(line).strip()
        if not text:
            return ""
        if text.startswith("[gui]") or text.startswith("[DBG]"):
            return ""
        if _CLI_HINT_LINE_RE.match(text):
            return ""
        if _MENU_OPTION_LINE_RE.match(text):
            return ""
        if _DECORATIVE_LINE_RE.match(text):
            return ""
        if _AUTORESPONDER_TRACE_RE.match(text):
            return ""
        if " | " in text:
            return text.replace(" | ", "\n")
        return text

    def _append_execution_log(self, line: str) -> None:
        formatted = self._format_execution_log_line(line)
        if not formatted:
            return
        self._append_spaced_log_entry(self._exec_log_console, formatted)

    def _format_autoresponder_log_line(self, line: str) -> str:
        text = _strip_cli_hints(line).strip()
        if not text or text.startswith("[gui]") or text.startswith("[DBG]"):
            return ""
        upper = text.upper()
        if (
            text.startswith("SCROLL_CHECK")
            or text.startswith("DISCOVERY_")
            or text.startswith("LOOP_EXIT")
            or text.startswith("[TEMP_METRIC]")
            or upper.startswith("TRACE_")
        ):
            return ""
        if _AUTORESPONDER_TRACE_RE.match(text):
            return ""
        if _CLI_HINT_LINE_RE.match(text):
            return ""
        if _MENU_OPTION_LINE_RE.match(text):
            return ""
        if _DECORATIVE_LINE_RE.match(text):
            return ""
        if " | " in text:
            return text.replace(" | ", "\n")
        return text

    def _append_autoresponder_log(self, line: str) -> None:
        formatted = self._format_autoresponder_log_line(line)
        if not formatted:
            return
        self._append_spaced_log_entry(self._autoresponder_log_console, formatted)

    def _format_leads_log_line(self, line: str) -> str:
        text = _strip_cli_hints(line).strip()
        if not text:
            return ""
        if text.startswith("[gui]") or text.startswith("[DBG]"):
            return ""
        if _CLI_HINT_LINE_RE.match(text):
            return ""
        if _MENU_OPTION_LINE_RE.match(text):
            return ""
        if _DECORATIVE_LINE_RE.match(text):
            return ""
        if text.lower() == "opcion:":
            return ""
        if " | " in text:
            return text.replace(" | ", "\n")
        return text

    def _append_leads_execution_log(self, line: str) -> None:
        formatted = self._format_leads_log_line(line)
        if not formatted:
            return
        self._append_spaced_log_entry(self._leads_log_console, formatted)

    def _is_leads_runtime_line(self, normalized_line: str, normalized: str) -> bool:
        if _LEADS_RESULT_RE.search(normalized_line):
            return True
        leads_tokens = (
            "ejecutando filtrado",
            "filtrado detenido",
            "leads guardados en alias",
            "filtrado completado pero no hubo leads calificados para guardar",
            "no quedan usernames pendientes",
            "no hay cuentas validas para ejecutar el filtrado",
            "paso 1 carga de usernames",
            "usernames cargados",
        )
        return any(token in normalized for token in leads_tokens)

    def _set_menu_activity_visible(self, visible: bool) -> None:
        if not hasattr(self, "_menu_activity_card"):
            return
        self._menu_activity_card.setVisible(visible)
        self._menu_activity_open = visible

    def _set_leads_live_card_visible(self, visible: bool) -> None:
        if not hasattr(self, "_leads_live_card"):
            return
        allowed = bool(
            visible
            and self._active_sidebar_key == "leads"
            and self._leads_filter_running
        )
        self._leads_live_card.setVisible(allowed)
        if not allowed:
            self._leads_live_prompt_label.setText("Esperando pregunta...")
            self._leads_live_input_preview.setText("(vacío)")

    def _update_leads_live_prompt(self, prompt_text: str) -> None:
        if not hasattr(self, "_leads_live_prompt_label"):
            return
        cleaned = _strip_cli_hints(prompt_text or "").strip()
        self._leads_live_prompt_label.setText(cleaned or "Esperando pregunta...")
        self._leads_live_input_preview.setText("(vacío)")

    def _append_leads_live_submission(self, prompt: str, value: str) -> None:
        if not hasattr(self, "_leads_live_submitted_console"):
            return
        prompt_text = _strip_cli_hints(prompt or "").strip() or "Entrada"
        payload = value if value else "(vacío)"
        self._append_spaced_log_entry(
            self._leads_live_submitted_console,
            f"{prompt_text}\n{payload}",
        )
        self._leads_live_input_preview.setText("(vacío)")

    def _on_live_input_text_changed(self, value: str) -> None:
        if not hasattr(self, "_leads_live_card") or not self._leads_live_card.isVisible():
            return
        current = str(value or "")
        self._leads_live_input_preview.setText(current if current else "(vacío)")

    def _primary_key_for_value(self, value: str) -> Optional[str]:
        target = str(value).strip()
        if not target:
            return None
        for key, option_value in self._primary_option_values.items():
            if str(option_value).strip() == target:
                return key
        return None

    def _scope_section_logs_for_submission(self, request: InputRequest, value: str) -> None:
        if request.is_menu and self._pending_request_is_primary_menu:
            selected_key = self._primary_key_for_value(value)
            if selected_key in self._SECTION_LOG_KEYS:
                self._section_log_scope = selected_key
            else:
                self._section_log_scope = None
            return

        if self._active_sidebar_key in self._SECTION_LOG_KEYS:
            self._section_log_scope = self._active_sidebar_key

    def _current_section_log_key(self) -> Optional[str]:
        if self._section_log_scope in self._SECTION_LOG_KEYS:
            return self._section_log_scope
        if (
            self._pending_request
            and self._pending_request.is_menu
            and not self._pending_request_is_primary_menu
            and self._active_sidebar_key in self._SECTION_LOG_KEYS
        ):
            return self._active_sidebar_key
        return None

    def _infer_section_log_key_from_line(self, line: str) -> Optional[str]:
        normalized = _normalized_label(line)
        if not normalized:
            return None
        if "whatsapp" in normalized:
            return "whatsapp"
        if "actualizacion" in normalized or "github release" in normalized:
            return "updates"
        if "filtrado de leads" in normalized or "leads calificados" in normalized:
            return "leads"
        if "estadistica" in normalized or "metrica" in normalized:
            return "stats"
        if "registros de envio" in normalized:
            return "logs"
        if "cuentas agregadas" in normalized or "iniciar sesion masiva" in normalized:
            return "accounts"
        if "entregar a cliente" in normalized or "licencia creada" in normalized:
            return "deliver"
        return None

    def _format_menu_activity_log_line(self, line: str) -> str:
        text = _strip_cli_hints(line).strip()
        if not text:
            return ""
        if text.startswith("[gui]") or text.startswith("[DBG]"):
            return ""
        if _CLI_HINT_LINE_RE.match(text):
            return ""
        if _MENU_OPTION_LINE_RE.match(text):
            return ""
        if _DECORATIVE_LINE_RE.match(text):
            return ""

        normalized = _normalized_label(text)
        if not normalized:
            return ""
        if normalized in ("opcion", "opcion ", "opcion :", "opcion:"):
            return ""
        if normalized.startswith("elige una opcion"):
            return ""
        if normalized.startswith("selecciona una opcion"):
            return ""

        dashboard_tokens = (
            "estado general",
            "cuentas totales",
            "conectadas",
            "activas",
            "mensajes enviados hoy",
            "mensajes con error hoy",
            "mensajes respondidos hoy",
            "agendas realizadas hoy",
            "ultima actualizacion",
        )
        if any(token in normalized for token in dashboard_tokens):
            return ""

        if " | " in text:
            return text.replace(" | ", "\n")
        return text

    def _append_menu_activity_log(self, line: str) -> None:
        key = self._current_section_log_key() or self._infer_section_log_key_from_line(line)
        if not key:
            return
        if key not in self._section_log_buffers:
            return
        formatted = self._format_menu_activity_log_line(line)
        if not formatted:
            return

        self._section_log_buffers[key].append(formatted)
        if (
            self._menu_log_displayed_key == key
            and self._active_sidebar_key == key
            and self._stack.currentIndex() == self.PAGE_MENU
        ):
            if not self._menu_activity_open:
                self._set_menu_activity_visible(True)
            self._append_spaced_log_entry(self._menu_activity_log_console, formatted)

    def _refresh_menu_activity_log(self) -> None:
        if not hasattr(self, "_menu_activity_log_console"):
            return
        if self._active_sidebar_key in self._SECTION_LOG_KEYS:
            key = self._active_sidebar_key
            label = self._primary_label_by_key.get(key, key.capitalize())
            self._menu_log_title.setText(f"Actividad · {label}")
            entries = list(self._section_log_buffers.get(key, ()))
            self._menu_activity_log_console.setPlainText("\n\n".join(entries))
            cursor = self._menu_activity_log_console.textCursor()
            cursor.movePosition(QTextCursor.End)
            self._menu_activity_log_console.setTextCursor(cursor)
            self._menu_activity_log_console.ensureCursorVisible()
            self._menu_log_displayed_key = key
            self._set_menu_activity_visible(self._menu_activity_open)
            return

        self._menu_log_title.setText("Actividad")
        self._menu_activity_log_console.clear()
        self._menu_log_displayed_key = None
        self._set_menu_activity_visible(False)

    def _clear_active_section_log(self) -> None:
        key = self._active_sidebar_key
        if key not in self._SECTION_LOG_KEYS:
            return
        self._section_log_buffers[key].clear()
        if self._menu_log_displayed_key == key:
            self._menu_activity_log_console.clear()
        self._set_menu_activity_visible(False)

    def _set_execution_view(self, view: Optional[str]) -> None:
        send_visible = view == "send"
        autoresponder_visible = view == "autoresponder"
        leads_visible = view == "leads"
        self._send_execution_container.setVisible(send_visible)
        self._autoresponder_execution_container.setVisible(autoresponder_visible)
        self._leads_execution_container.setVisible(leads_visible)

    def _resolve_execution_view(self, active: bool) -> Optional[str]:
        if not active:
            return None
        if self._campaign_running and self._active_sidebar_key == "send":
            return "send"
        if self._autoresponder_running and self._active_sidebar_key == "autoresponder":
            return "autoresponder"
        if self._leads_filter_running and self._active_sidebar_key == "leads":
            return "leads"
        return None

    @Slot()
    def _tick_execution_clock(self) -> None:
        self._exec_time_tag.setText(f"Hora: {datetime.now().strftime('%H:%M:%S')}")
        self._update_autoresponder_runtime_text()
        if self._leads_filter_running:
            self._update_leads_metrics_view()

    def _set_execution_mode(self, active: bool, *, keep_view: bool = False) -> None:
        view = self._resolve_execution_view(active)
        visible = view is not None
        self._execution_mode_active = visible
        self._set_execution_view(view)
        if visible:
            self._hide_send_loading_overlay()
            if self._stack.currentIndex() != self.PAGE_EXECUTION:
                self._set_page(self.PAGE_EXECUTION)
            return
        if (
            not keep_view
            and self._stack.currentIndex() == self.PAGE_EXECUTION
            and not self._showing_summary
            and not self._block_navigation
        ):
            self._set_page(self.PAGE_MENU)

    @staticmethod
    def _normalize_execution_line(line: str) -> str:
        text = line.strip()
        if not text:
            return ""
        text = (
            text.replace("│", "|")
            .replace("┃", "|")
            .replace("║", "|")
            .replace("¦", "|")
        )
        if text.startswith("|"):
            text = text[1:].lstrip()
        if text.endswith("|"):
            text = text[:-1].rstrip()
        return text

    def _set_campaign_running(self, running: bool) -> None:
        if self._campaign_running == running:
            return
        self._campaign_running = running
        if running:
            self._emit_engine_event("CAMPAIGN_RUNNING")
            if not self._campaign_active:
                self._campaign_active = True
                self._campaign_start_time = time.time()
                self._campaign_summary_detected = False
                self._block_navigation = False
                self._campaign_account_stats.clear()
                self._campaign_summary_capture_active = False
                self._campaign_summary_alias = "-"
                self._campaign_summary_accounts.clear()
                self._campaign_summary_totals = {
                    "ok": 0,
                    "err": 0,
                    "no_dm": 0,
                    "unver": 0,
                    "total": 0,
                }
                self._capture_low_profile_list = False
                self._capture_session_list = False
                self._send_low_profile_accounts.clear()
                self._send_session_issue_accounts.clear()
                self._send_login_total_failure = False
            self._hide_send_loading_overlay()
        else:
            self._emit_engine_event("CAMPAIGN_FINISHED")
        if hasattr(self, "_exec_stop_button"):
            self._exec_stop_button.setEnabled(running)
        if not running:
            self._set_execution_mode(self._autoresponder_running or self._leads_filter_running)

    @staticmethod
    def _safe_int_text(value: str) -> int:
        match = re.search(r"\d+", str(value or ""))
        if not match:
            return 0
        try:
            return max(0, int(match.group(0)))
        except Exception:
            return 0

    @staticmethod
    def _parse_campaign_summary_row(line: str) -> Optional[tuple[str, int, int, int, int, int]]:
        parts = str(line or "").strip().split()
        if len(parts) < 6:
            return None
        tail = parts[-5:]
        if not all(token.isdigit() for token in tail):
            return None
        account = " ".join(parts[:-5]).strip()
        if not account:
            return None
        if _normalized_label(account) == "cuenta":
            return None
        ok, err, no_dm, unver, total = (int(token) for token in tail)
        return account, ok, err, no_dm, unver, total

    def _campaign_summary_metrics(self) -> dict[str, int]:
        totals = dict(self._campaign_summary_totals)
        if totals.get("total", 0) > 0 or self._campaign_summary_accounts:
            ok = max(0, int(totals.get("ok", 0)))
            errors = max(0, int(totals.get("err", 0)))
            skipped = max(0, int(totals.get("no_dm", 0)))
            unverified = max(0, int(totals.get("unver", 0)))
            sent = max(0, ok + unverified)
            total = max(0, int(totals.get("total", sent + errors + skipped)))
            return {
                "sent": sent,
                "errors": errors,
                "unverified": unverified,
                "skipped": skipped,
                "total": total,
            }

        sent = self._safe_int_text(self._exec_metric_labels.get("sent", QLabel("0")).text())
        errors = self._safe_int_text(self._exec_metric_labels.get("error", QLabel("0")).text())
        unverified = self._safe_int_text(self._exec_metric_labels.get("unverified", QLabel("0")).text())
        skipped = self._safe_int_text(self._exec_metric_labels.get("skipped", QLabel("0")).text())
        total = max(0, sent + errors + skipped)
        return {
            "sent": sent,
            "errors": errors,
            "unverified": unverified,
            "skipped": skipped,
            "total": total,
        }

    def _show_custom_summary_modal(self, duration_seconds: float) -> None:
        if self._showing_summary:
            return
        self._showing_summary = True
        self._block_navigation = True
        try:
            metrics = self._campaign_summary_metrics()
            alias = self._campaign_summary_alias.strip() or "-"
            if alias == "-":
                alias_text = self._exec_alias_tag.text().strip()
                alias = alias_text.split(":", 1)[1].strip() if ":" in alias_text else "-"
            duration_text = self._format_elapsed_hhmmss(duration_seconds)

            dialog = QDialog(self)
            dialog.setObjectName("CampaignSummaryDialog")
            dialog.setWindowTitle("Resumen de Campaña")
            dialog.setModal(True)
            dialog.setMinimumWidth(620)
            dialog.setFont(QFont("Segoe UI Emoji", 10))
            dialog.setStyleSheet(
                """
                QDialog#CampaignSummaryDialog {
                    background-color: #0b1220;
                    color: #e2e8f0;
                    border: 1px solid #1e293b;
                    border-radius: 12px;
                }
                QLabel#CampaignSummaryTitle {
                    font-size: 22px;
                    font-weight: 700;
                    color: #f8fafc;
                }
                QLabel#CampaignSummaryBody {
                    font-size: 14px;
                    color: #cbd5e1;
                }
                QFrame#CampaignSummaryWarn {
                    background-color: #422006;
                    border: 1px solid #92400e;
                    border-radius: 10px;
                }
                QLabel#CampaignSummaryWarnTitle {
                    font-size: 14px;
                    font-weight: 700;
                    color: #fbbf24;
                }
                QLabel#CampaignSummaryWarnBody {
                    font-size: 13px;
                    color: #fde68a;
                }
                QPushButton#CampaignSummaryClose {
                    background-color: #2563eb;
                    color: #ffffff;
                    border: none;
                    border-radius: 8px;
                    padding: 8px 18px;
                    font-weight: 700;
                }
                QPushButton#CampaignSummaryClose:hover {
                    background-color: #1d4ed8;
                }
                """
            )

            layout = QVBoxLayout(dialog)
            layout.setContentsMargins(18, 18, 18, 18)
            layout.setSpacing(10)

            title = QLabel("📊 Resumen de Campaña")
            title.setObjectName("CampaignSummaryTitle")
            title.setAlignment(Qt.AlignCenter)
            title.setFont(QFont("Segoe UI Emoji", 12))

            sent_line = QLabel(f"🟢 Mensajes enviados: {metrics['sent']}")
            err_line = QLabel(f"🔴 Errores: {metrics['errors']}")
            time_line = QLabel(f"⏱ Tiempo total: {duration_text}")
            alias_line = QLabel(f"Alias: {alias}")
            for row in (sent_line, err_line, time_line, alias_line):
                row.setObjectName("CampaignSummaryBody")
                row.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                row.setFont(QFont("Segoe UI Emoji", 10))

            accounts_title = QLabel("Cuentas")
            accounts_title.setObjectName("CampaignSummaryBody")
            accounts_title.setStyleSheet("font-weight: 700; color: #e2e8f0;")
            accounts_title.setFont(QFont("Segoe UI Emoji", 10))

            lines: list[str] = []
            source_accounts = self._campaign_summary_accounts or self._campaign_account_stats
            for username in sorted(source_accounts.keys()):
                stats = source_accounts.get(username) or {}
                ok_count = max(0, int(stats.get("ok", 0)))
                err_count = max(0, int(stats.get("err", 0)))
                tone = "🔴" if err_count > ok_count and err_count > 0 else "🟢"
                lines.append(f"{tone} {username} — OK: {ok_count} | ERR: {err_count}")
            if not lines:
                lines = ["🟢 Sin detalle por cuenta — OK: 0 | ERR: 0"]

            accounts_body = QLabel("\n".join(lines))
            accounts_body.setObjectName("CampaignSummaryBody")
            accounts_body.setWordWrap(True)
            accounts_body.setAlignment(Qt.AlignLeft | Qt.AlignTop)
            accounts_body.setFont(QFont("Segoe UI Emoji", 10))

            show_recommendations = (
                metrics["errors"] > 0 or metrics["unverified"] > 0 or metrics["skipped"] > 0
            )
            warn_card: Optional[QFrame] = None
            if show_recommendations:
                warn_card = QFrame()
                warn_card.setObjectName("CampaignSummaryWarn")
                warn_layout = QVBoxLayout(warn_card)
                warn_layout.setContentsMargins(10, 10, 10, 10)
                warn_layout.setSpacing(6)
                warn_title = QLabel("⚠️ Recomendaciones")
                warn_title.setObjectName("CampaignSummaryWarnTitle")
                warn_title.setFont(QFont("Segoe UI Emoji", 10))
                suggested_cap = max(10, min(60, metrics["sent"] + metrics["errors"]))
                worst_account = "-"
                worst_err = 0
                for username, stats in source_accounts.items():
                    err_value = max(0, int((stats or {}).get("err", 0)))
                    if err_value > worst_err:
                        worst_err = err_value
                        worst_account = username
                warn_body = QLabel(
                    f"Cuenta con más errores: {worst_account} ({worst_err}).\n"
                    f"Reducir volumen a ~{suggested_cap} mensajes/día.\n"
                    "Activar interacción manual para estabilizar resultados."
                )
                warn_body.setObjectName("CampaignSummaryWarnBody")
                warn_body.setWordWrap(True)
                warn_body.setFont(QFont("Segoe UI Emoji", 10))
                warn_layout.addWidget(warn_title)
                warn_layout.addWidget(warn_body)

            close_button = QPushButton("Cerrar")
            close_button.setObjectName("CampaignSummaryClose")
            close_button.clicked.connect(dialog.accept)

            layout.addWidget(title)
            layout.addWidget(alias_line)
            layout.addWidget(sent_line)
            layout.addWidget(err_line)
            layout.addWidget(time_line)
            layout.addWidget(accounts_title)
            layout.addWidget(accounts_body)
            if warn_card is not None:
                layout.addWidget(warn_card)
            layout.addWidget(close_button, 0, Qt.AlignCenter)
            dialog.exec()
        finally:
            self._showing_summary = False
            self._block_navigation = False
            request = self._pending_request
            if isinstance(request, InputRequest) and not request.is_menu:
                prompt_norm = _normalized_label(_strip_cli_hints(request.prompt or ""))
                if "presiona enter para continuar" in prompt_norm or "enter para continuar" in prompt_norm:
                    self._submit_current_input("")
            self._set_active_sidebar("dashboard")
            self._set_page(self.PAGE_DASHBOARD)
            self._emit_engine_event("SUMMARY_CLOSED")

    @staticmethod
    def _coerce_summary_int(value: Any) -> Optional[int]:
        match = re.search(r"\d+", str(value or ""))
        if not match:
            return None
        try:
            return max(0, int(match.group(0)))
        except Exception:
            return None

    def parse_autoresponder_summary(self, lines: list[str]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "alias": None,
            "accounts_used": None,
            "replies_attempted": None,
            "replies_sent": None,
            "followups_attempted": None,
            "followups_sent": None,
            "errors": None,
            "total_time": None,
            "per_account_time": [],
        }
        replies_error: Optional[int] = None
        followups_error: Optional[int] = None
        per_account: list[tuple[str, str]] = []
        in_per_account = False

        for raw_line in lines:
            text = self._normalize_execution_line(_strip_cli_hints(raw_line or "")).strip()
            if not text:
                continue
            normalized = _normalized_label(text)
            lower = text.lower()

            if ":" in text:
                key, value = text.split(":", 1)
                key_norm = _normalized_label(key)
                value = value.strip()
                if "alias" in key_norm and value:
                    summary["alias"] = value
                elif "cuentas usadas" in key_norm or "cantidad de cuentas" in key_norm:
                    summary["accounts_used"] = self._coerce_summary_int(value)
                elif "respuestas intentadas" in key_norm or "cantidad mensajes respondidos" in key_norm:
                    summary["replies_attempted"] = self._coerce_summary_int(value)
                elif "respuestas enviadas" in key_norm:
                    summary["replies_sent"] = self._coerce_summary_int(value)
                elif "follow up intentados" in key_norm or "followup intentados" in key_norm:
                    summary["followups_attempted"] = self._coerce_summary_int(value)
                elif "follow up enviados" in key_norm or "followup enviados" in key_norm:
                    summary["followups_sent"] = self._coerce_summary_int(value)
                elif key_norm == "errores" or "errores" in key_norm:
                    summary["errors"] = self._coerce_summary_int(value)
                elif "tiempo total" in key_norm or "cantidad de tiempo total" in key_norm:
                    time_match = _SUMMARY_TIME_RE.search(value)
                    summary["total_time"] = time_match.group(0) if time_match else (value or None)
                elif "tiempo por cuenta" in key_norm:
                    in_per_account = True
                    continue

            if "respuestas ok" in normalized:
                values = [int(item) for item in re.findall(r"\d+", text)]
                if values:
                    summary["replies_sent"] = values[0]
                    if len(values) >= 2:
                        replies_error = values[1]
                        summary["replies_attempted"] = values[0] + values[1]

            if "followups ok" in normalized or "follow up ok" in normalized:
                values = [int(item) for item in re.findall(r"\d+", text)]
                if values:
                    summary["followups_sent"] = values[0]
                    if len(values) >= 2:
                        followups_error = values[1]
                        summary["followups_attempted"] = values[0] + values[1]

            if "tiempo por cuenta" in normalized:
                in_per_account = True
                continue

            account_match = _SUMMARY_PER_ACCOUNT_RE.search(text)
            if account_match:
                username = account_match.group("user").strip()
                if not username.startswith("@"):
                    username = f"@{username}"
                per_account.append((username, account_match.group("time")))
                continue

            if in_per_account:
                if ":" in text and not _SUMMARY_PER_ACCOUNT_RE.search(text):
                    key_norm = _normalized_label(text.split(":", 1)[0])
                    if any(
                        token in key_norm
                        for token in (
                            "respuestas",
                            "follow",
                            "errores",
                            "tiempo total",
                            "alias",
                            "cuentas",
                        )
                    ):
                        in_per_account = False
                elif _MENU_OPTION_LINE_RE.match(text):
                    in_per_account = False

            if summary["alias"] is None and normalized.startswith("alias "):
                maybe_alias = text.split(" ", 1)[1].strip()
                if maybe_alias:
                    summary["alias"] = maybe_alias

            if summary["total_time"] is None:
                time_match = _SUMMARY_TIME_RE.search(text)
                if time_match and any(token in normalized for token in ("tiempo total", "duracion", "duracion total")):
                    summary["total_time"] = time_match.group(0)

            if any(token in normalized for token in ("resumen final", "auto responder", "autoresponder", "herramienta detenida")):
                continue

            if "cantidad mensajes followup" in normalized and summary["followups_attempted"] is None:
                summary["followups_attempted"] = self._coerce_summary_int(text)

        if summary["errors"] is None:
            total_err = 0
            has_err = False
            if replies_error is not None:
                total_err += replies_error
                has_err = True
            if followups_error is not None:
                total_err += followups_error
                has_err = True
            if has_err:
                summary["errors"] = total_err

        if per_account:
            unique_rows: list[tuple[str, str]] = []
            seen: set[str] = set()
            for username, time_value in per_account:
                key = f"{username}|{time_value}"
                if key in seen:
                    continue
                seen.add(key)
                unique_rows.append((username, time_value))
            summary["per_account_time"] = unique_rows

        return summary

    @staticmethod
    def _autoresponder_summary_has_content(summary: dict[str, Any]) -> bool:
        keys = (
            "alias",
            "accounts_used",
            "replies_attempted",
            "replies_sent",
            "followups_attempted",
            "followups_sent",
            "errors",
            "total_time",
        )
        if any(summary.get(key) not in (None, "", "—") for key in keys):
            return True
        per_account = summary.get("per_account_time")
        return isinstance(per_account, list) and len(per_account) > 0

    def _is_autoresponder_summary_start(self, normalized: str) -> bool:
        if "resumen final" in normalized and ("autoresponder" in normalized or "auto responder" in normalized):
            return True
        if "herramienta detenida" in normalized and ("autoresponder" in normalized or "auto responder" in normalized):
            return True
        if self._autoresponder_summary_expected and any(
            token in normalized
            for token in (
                "cantidad de cuentas",
                "cantidad de tiempo total",
                "respuestas intentadas",
                "follow up intentados",
                "followup intentados",
            )
        ):
            return True
        return False

    @staticmethod
    def _is_autoresponder_summary_boundary(stripped: str, normalized: str) -> bool:
        if _MENU_OPTION_LINE_RE.match(stripped):
            return True
        if "presiona enter para continuar" in normalized or "enter para continuar" in normalized:
            return True
        if normalized in {"opcion", "opcion:"} or normalized.startswith("opci"):
            return True
        if normalized.startswith("elige una opcion") or normalized.startswith("selecciona una opcion"):
            return True
        if "menu principal" in normalized and "selecciona" in normalized:
            return True
        return False

    def _consume_autoresponder_summary_stream_line(self, raw_line: str) -> None:
        if self._autoresponder_summary_modal_shown:
            return
        stripped = self._normalize_execution_line(_strip_cli_hints(raw_line or "")).strip()
        if not stripped:
            return
        normalized = _normalized_label(stripped)
        if not normalized:
            return

        if self._is_autoresponder_summary_start(normalized):
            self._autoresponder_summary_capture_active = True
            self._autoresponder_summary_lines.clear()

        if not self._autoresponder_summary_capture_active:
            return

        if not stripped.startswith("[gui]") and not stripped.startswith("[DBG]"):
            self._autoresponder_summary_lines.append(stripped)

        parsed = self.parse_autoresponder_summary(list(self._autoresponder_summary_lines))
        has_content = self._autoresponder_summary_has_content(parsed)
        reached_boundary = self._is_autoresponder_summary_boundary(stripped, normalized)
        capture_full = len(self._autoresponder_summary_lines) >= 140
        if not has_content and not reached_boundary and not capture_full:
            return
        if reached_boundary or capture_full:
            self._autoresponder_summary_capture_active = False
            self._autoresponder_last_summary = parsed
            self._autoresponder_summary_modal_shown = True
            self._emit_engine_event("CAMPAIGN_SUMMARY_DETECTED")
            manager = self._engine_state_manager
            if manager is not None:
                manager.set_state(EngineState.FINISHED, reason="AUTORESPONDER_SUMMARY_DETECTED")
            self._show_autoresponder_summary_modal(parsed)

    def _release_pending_after_autoresponder_summary(self) -> None:
        request = self._pending_request
        if not isinstance(request, InputRequest):
            return
        if request.is_menu:
            if self._pending_request_is_primary_menu:
                return
            target_value = ""
            for option in request.menu_options:
                label_norm = _normalized_label(option.label or "")
                if any(
                    token in label_norm
                    for token in ("menu principal", "volver", "atras", "atrás", "salir")
                ):
                    target_value = str(option.value or "").strip()
                    break
            self._submit_current_input(target_value)
            return

        prompt_norm = _normalized_label(_strip_cli_hints(request.prompt or ""))
        if any(
            token in prompt_norm
            for token in (
                "presiona enter para continuar",
                "enter para continuar",
                "continuar",
                "opcion",
            )
        ):
            self._submit_current_input("")

    def _navigate_dashboard_after_autoresponder_summary(self) -> None:
        self._on_sidebar_item_clicked("dashboard")

    def _show_autoresponder_summary_modal(self, summary: dict[str, Any]) -> None:
        if self._showing_summary:
            return
        self._showing_summary = True
        self._block_navigation = True
        try:
            dialog = AutoResponderSummaryDialog(summary, self)
            dialog.exec()
        finally:
            self._showing_summary = False
            self._block_navigation = False
            self._autoresponder_summary_expected = False
            self._release_pending_after_autoresponder_summary()
            self._navigate_dashboard_after_autoresponder_summary()
            self._emit_engine_event("SUMMARY_CLOSED")
            manager = self._engine_state_manager
            if manager is not None:
                manager.set_state(EngineState.IDLE, reason="SUMMARY_ACKNOWLEDGED_AND_RETURNED_TO_DASHBOARD")

    def _set_autoresponder_running(self, running: bool) -> None:
        if self._autoresponder_running == running:
            return
        self._autoresponder_running = running
        if hasattr(self, "_autoresponder_stop_button"):
            self._autoresponder_stop_button.setEnabled(running)
        if running:
            self._autoresponder_summary_expected = True
            self._autoresponder_summary_capture_active = False
            self._autoresponder_summary_modal_shown = False
            self._autoresponder_summary_lines.clear()
            self._autoresponder_started_at = time.monotonic()
            self._set_execution_mode(True)
            self._update_autoresponder_runtime_text()
            return
        self._autoresponder_started_at = 0.0
        keep_view = self._autoresponder_summary_expected and not self._autoresponder_summary_modal_shown
        self._set_execution_mode(
            self._campaign_running or self._leads_filter_running,
            keep_view=keep_view,
        )
        self._update_autoresponder_runtime_text()

    def _log_autoresponder_stop_summary(self, elapsed_seconds: float) -> None:
        responses_total = self._autoresponder_responses_ok + self._autoresponder_responses_error
        followups_total = self._autoresponder_followups_ok + self._autoresponder_followups_error
        summary = (
            "cantidad de cuentas: "
            f"{self._autoresponder_active_accounts}\n"
            "cantidad de tiempo total: "
            f"{self._format_elapsed_hhmmss(elapsed_seconds)}\n"
            "cantidad mensajes respondidos (bien o con error): "
            f"{responses_total} (OK: {self._autoresponder_responses_ok} | "
            f"Error: {self._autoresponder_responses_error})\n"
            "cantidad de mensajes followup (bien o con error): "
            f"{followups_total} (OK: {self._autoresponder_followups_ok} | "
            f"Error: {self._autoresponder_followups_error})\n"
        )
        self._append_log(summary)

    def _request_autoresponder_stop(self) -> None:
        if not self._autoresponder_running:
            self._append_log("[gui] Auto-responder no esta corriendo.\n")
            return
        elapsed = 0.0
        if self._autoresponder_started_at > 0:
            elapsed = max(0.0, time.monotonic() - self._autoresponder_started_at)
        stop_requested = False
        try:
            from runtime import request_stop

            request_stop("se presiono Q")
            self._append_log("[gui] Frenar auto-responder solicitado (equivalente a Q).\n")
            stop_requested = True
        except Exception as exc:
            self._append_log(f"[gui] No se pudo solicitar freno de auto-responder: {exc}\n")
        if not stop_requested:
            return
        self._log_autoresponder_stop_summary(elapsed)

    def _request_campaign_stop(self) -> None:
        if not self._campaign_running:
            self._append_log("[gui] No campaign running.\n")
            return
        self._emit_engine_event("STOP_REQUESTED")
        try:
            from runtime import request_stop

            request_stop("se presionó Q")
            self._append_log("[gui] Frenar campaña solicitado (equivalente a Q).\n")
        except Exception as exc:
            self._append_log(f"[gui] No se pudo solicitar freno de campaña: {exc}\n")

    def _request_leads_filter_stop(self) -> None:
        if not self._leads_filter_running:
            self._append_log("[gui] No hay filtrado de leads en ejecución.\n")
            return
        stop_requested = False
        try:
            if sys.platform.startswith("win"):
                import msvcrt  # type: ignore

                msvcrt.ungetch("q")
                stop_requested = True
        except Exception:
            pass
        try:
            from runtime import request_stop

            request_stop("se presionó Q")
            stop_requested = True
        except Exception:
            pass

        if stop_requested:
            self._leads_stop_requested = True
            self._append_log(
                "[gui] Frenar filtrado solicitado. Esperando confirmación del backend.\n"
            )
            self._append_leads_execution_log(
                "Frenar filtrado solicitado. Esperando confirmación del backend."
            )
            return

        self._append_log("[gui] No se pudo enviar la señal de freno para filtrado.\n")
        self._append_leads_execution_log(
            "No se pudo enviar la señal de freno para filtrado."
        )

    def _parse_exec_event_row(
        self, line: str
    ) -> Optional[tuple[str, str, str, str, str]]:
        match = _EXEC_EVENT_ROW_RE.search(line)
        if not match:
            return None
        account = match.group("account").strip()
        lead = match.group("lead").strip() or "-"
        row_time = match.group("time").strip()
        result = match.group("result").strip() or "-"
        detail = (match.group("detail") or "").strip()
        if not account or account == "-":
            return None
        if result.lower() == "resumen":
            return None
        return account, lead, row_time, result, detail

    def _parse_exec_inflight_row(
        self, line: str, now: float
    ) -> Optional[tuple[str, str, str, str, str]]:
        normalized = _normalized_label(line)
        if "envios en vuelo" in normalized:
            self._inflight_parse_window_until = now + 2.0
            return None
        if (
            "cuenta" in normalized
            and "lead" in normalized
            and "hora" in normalized
            and "detalle" in normalized
        ):
            self._inflight_parse_window_until = now + 2.0
            return None
        if "sin envios en vuelo" in normalized:
            return None
        if now > self._inflight_parse_window_until:
            return None
        if ":" not in line:
            return None

        match = _EXEC_INFLIGHT_PIPE_ROW_RE.match(line)
        if not match:
            match = _EXEC_INFLIGHT_SPACED_ROW_RE.match(line)
        if not match:
            return None

        account = match.group("account").strip()
        lead = match.group("lead").strip() or "-"
        row_time = match.group("time").strip()
        result = match.group("result").strip() or "-"
        detail = (match.group("detail") or "").strip()
        if not account or account.lower() == "cuenta":
            return None
        return account, lead, row_time, result, detail

    def _consume_execution_line(self, line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        normalized_line = self._normalize_execution_line(stripped)
        if not normalized_line:
            return
        normalized = _normalized_label(normalized_line)
        if self._is_leads_runtime_line(normalized_line, normalized):
            return

        now = time.monotonic()
        self._execution_log_times = [t for t in self._execution_log_times if now - t <= 2.5]
        self._execution_log_times.append(now)

        send_log_line = False

        alias_match = _EXEC_ALIAS_RE.search(normalized_line)
        if alias_match:
            alias = alias_match.group("alias").strip()
            pending = alias_match.group("pending").strip()
            self._exec_alias_tag.setText(f"Alias: {alias}")
            self._exec_pending_tag.setText(f"Leads pendientes: {pending}")
            self._set_campaign_running(True)
            self._set_execution_mode(True)
            send_log_line = True

        meta_match = _EXEC_META_RE.search(normalized_line)
        if meta_match:
            if "sent" in self._exec_metric_labels:
                self._exec_metric_labels["sent"].setText(meta_match.group("sent"))
            if "error" in self._exec_metric_labels:
                self._exec_metric_labels["error"].setText(meta_match.group("error"))
            if "unverified" in self._exec_metric_labels:
                self._exec_metric_labels["unverified"].setText(meta_match.group("unverified"))
            if "skipped" in self._exec_metric_labels:
                self._exec_metric_labels["skipped"].setText(meta_match.group("skipped"))
            if "concurrency" in self._exec_metric_labels:
                self._exec_metric_labels["concurrency"].setText(meta_match.group("concurrency"))
            self._set_campaign_running(True)
            self._set_execution_mode(True)
            send_log_line = True

        event_row = self._parse_exec_event_row(normalized_line)
        if event_row:
            account, lead, row_time, result, detail = event_row
            self._update_exec_row(account, lead, row_time, result, detail)
            stats = self._campaign_account_stats.setdefault(account, {"ok": 0, "err": 0})
            result_norm = _normalized_label(result)
            if "error" in result_norm or "fall" in result_norm:
                stats["err"] = int(stats.get("err", 0)) + 1
            elif "skip" not in result_norm and "omit" not in result_norm:
                stats["ok"] = int(stats.get("ok", 0)) + 1
            self._set_campaign_running(True)
            self._set_execution_mode(True)
            send_log_line = True

        inflight_row = self._parse_exec_inflight_row(normalized_line, now)
        if inflight_row:
            account, lead, row_time, result, detail = inflight_row
            self._update_exec_row(account, lead, row_time, result, detail)
            self._set_campaign_running(True)
            self._set_execution_mode(True)
            send_log_line = True

        if "auto responder" in normalized or "autoresponder" in normalized:
            return
        execution_tokens = (
            "iniciando campana",
            "iniciando campaña",
            "presiona q para detener",
            "totales por cuenta",
            "envios en vuelo",
            "envíos en vuelo",
            "iniciando envio",
            "iniciando envío",
        )
        if any(token in normalized for token in execution_tokens):
            self._set_campaign_running(True)
            self._set_execution_mode(True)
            send_log_line = True

        if "resumen final" in normalized and "campana" in normalized:
            self._campaign_summary_detected = True
            self._emit_engine_event("CAMPAIGN_SUMMARY_DETECTED")
            self._campaign_summary_capture_active = True
            self._campaign_summary_alias = "-"
            self._campaign_summary_accounts.clear()
            self._campaign_summary_totals = {
                "ok": 0,
                "err": 0,
                "no_dm": 0,
                "unver": 0,
                "total": 0,
            }
            send_log_line = True
        elif self._campaign_summary_capture_active:
            if normalized.startswith("alias:"):
                alias_chunk = normalized_line.split("|", 1)[0].strip()
                if ":" in alias_chunk:
                    parsed_alias = alias_chunk.split(":", 1)[1].strip()
                    if parsed_alias:
                        self._campaign_summary_alias = parsed_alias
                send_log_line = True

            parsed_row = self._parse_campaign_summary_row(normalized_line)
            if parsed_row is not None:
                account, ok, err, no_dm, unver, total = parsed_row
                account_norm = _normalized_label(account)
                if account_norm == "total general":
                    self._campaign_summary_totals = {
                        "ok": ok,
                        "err": err,
                        "no_dm": no_dm,
                        "unver": unver,
                        "total": total,
                    }
                    self._campaign_summary_capture_active = False
                else:
                    self._campaign_summary_accounts[account] = {
                        "ok": ok,
                        "err": err,
                        "no_dm": no_dm,
                        "unver": unver,
                        "total": total,
                    }
                send_log_line = True

            if _DECORATIVE_LINE_RE.match(stripped) and self._campaign_summary_totals.get("total", 0) > 0:
                self._campaign_summary_capture_active = False
        elif any(token in normalized for token in ("total general", "hora fin", "ok confirmados")):
            send_log_line = True

        is_noise = (
            stripped.startswith("[gui]")
            or stripped.startswith("[DBG]")
            or _MENU_OPTION_LINE_RE.match(stripped)
            or _CLI_HINT_LINE_RE.match(stripped)
            or _DECORATIVE_LINE_RE.match(stripped)
        )
        if self._campaign_running and not is_noise:
            send_log_line = True

        if len(self._execution_log_times) >= 6 and not _MENU_OPTION_LINE_RE.match(stripped):
            self._set_execution_mode(True)

        if send_log_line:
            self._append_execution_log(normalized_line)

    def _start_leads_filter_run(self) -> None:
        if self._leads_counts_prefill_on_next_start:
            prefill = self._leads_counts_prefill_on_next_start
            self._leads_total_target = prefill.get("total")
            self._leads_processed_count = prefill.get("processed", 0)
            self._leads_qualified_count = prefill.get("qualified", 0)
            self._leads_discarded_count = prefill.get("discarded", 0)
            self._leads_counts_prefill_on_next_start = None
        else:
            self._leads_processed_count = 0
            self._leads_qualified_count = 0
            self._leads_discarded_count = 0
        self._leads_accounts_seen.clear()
        self._leads_stop_requested = False
        self._leads_stop_prompt_active = False
        self._leads_completion_announced = False
        self._clear_leads_execution_log()
        self._set_leads_filter_running(True)
        self._update_leads_metrics_view()

    def _consume_leads_filter_line(self, line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        normalized_line = self._normalize_execution_line(stripped)
        if not normalized_line:
            return
        normalized = _normalized_label(normalized_line)
        if not normalized:
            return

        if "crear nuevo filtrado" in normalized or "paso 1 carga de usernames" in normalized:
            self._leads_total_target = None
            self._leads_processed_count = 0
            self._leads_qualified_count = 0
            self._leads_discarded_count = 0
            self._leads_accounts_seen.clear()
            self._leads_counts_prefill_on_next_start = None
            self._leads_stop_requested = False
            self._leads_stop_prompt_active = False
            self._leads_completion_announced = False
            self._update_leads_metrics_view()

        summary_match = _LEADS_SUMMARY_ROW_RE.search(normalized_line)
        if summary_match:
            total = int(summary_match.group("total"))
            processed = int(summary_match.group("processed"))
            qualified = int(summary_match.group("qualified"))
            discarded = int(summary_match.group("discarded"))
            self._leads_counts_prefill_on_next_start = {
                "total": total,
                "processed": processed,
                "qualified": qualified,
                "discarded": discarded,
            }
            self._leads_total_target = total
            self._leads_processed_count = processed
            self._leads_qualified_count = qualified
            self._leads_discarded_count = discarded
            self._update_leads_metrics_view()
            return

        usernames_match = _LEADS_USERNAMES_LOADED_RE.search(normalized_line)
        if usernames_match:
            count = int(usernames_match.group("count"))
            self._leads_total_target = count
            self._leads_processed_count = 0
            self._leads_qualified_count = 0
            self._leads_discarded_count = 0
            self._leads_counts_prefill_on_next_start = {
                "total": count,
                "processed": 0,
                "qualified": 0,
                "discarded": 0,
            }
            self._update_leads_metrics_view()
            return

        if "ejecutando filtrado" in normalized:
            self._start_leads_filter_run()
            self._append_leads_execution_log(normalized_line)
            return

        result_match = _LEADS_RESULT_RE.search(normalized_line)
        if result_match:
            if not self._leads_filter_running and not self._leads_stop_requested:
                self._start_leads_filter_run()
            account = result_match.group("account").strip().lstrip("@")
            if account:
                self._leads_accounts_seen.add(account)
            result = result_match.group("result").strip().upper()
            if result == "CALIFICA":
                self._leads_qualified_count += 1
            else:
                self._leads_discarded_count += 1
            self._leads_processed_count = self._leads_qualified_count + self._leads_discarded_count
            if (
                self._leads_total_target is None
                or self._leads_processed_count > self._leads_total_target
            ):
                self._leads_total_target = self._leads_processed_count
            self._update_leads_metrics_view()
            self._append_leads_execution_log(normalized_line)
            return

        if "filtrado detenido" in normalized:
            if "que queres hacer" in normalized:
                self._leads_stop_prompt_active = True
            self._set_leads_filter_running(False)
            self._append_leads_execution_log(normalized_line)
            self._show_leads_completion_dialog()
            return

        export_match = _LEADS_EXPORT_ALIAS_RE.search(normalized_line)
        if export_match:
            self._leads_export_alias = export_match.group("alias").strip() or self._leads_export_alias
            self._set_leads_filter_running(False)
            self._append_leads_execution_log(normalized_line)
            self._show_leads_completion_dialog()
            return

        if "filtrado completado pero no hubo leads calificados para guardar" in normalized:
            self._set_leads_filter_running(False)
            self._append_leads_execution_log(normalized_line)
            self._show_leads_completion_dialog()
            return

        if (
            "no quedan usernames pendientes" in normalized
            or "no hay cuentas validas para ejecutar el filtrado" in normalized
        ):
            self._set_leads_filter_running(False)
            self._append_leads_execution_log(normalized_line)
            return

        if self._leads_filter_running:
            is_noise = (
                stripped.startswith("[gui]")
                or stripped.startswith("[DBG]")
                or _MENU_OPTION_LINE_RE.match(stripped)
                or _CLI_HINT_LINE_RE.match(stripped)
                or _DECORATIVE_LINE_RE.match(stripped)
            )
            if not is_noise:
                self._append_leads_execution_log(normalized_line)

    def _consume_autoresponder_line(self, line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        normalized_line = self._normalize_execution_line(stripped)
        if not normalized_line:
            return

        normalized = _normalized_label(normalized_line)
        start_match = _AUTORESPONDER_ACTIVE_RE.search(normalized_line)
        if not start_match:
            start_match = _AUTORESPONDER_ACTIVE_ALT_RE.search(normalized_line)
        if start_match:
            if not self._autoresponder_running:
                self._reset_autoresponder_execution_view()
                self._set_autoresponder_running(True)
            try:
                self._autoresponder_active_accounts = max(0, int(start_match.group("count")))
            except Exception:
                self._autoresponder_active_accounts = max(0, self._autoresponder_active_accounts)
            self._update_autoresponder_status_blocks()

        delay_match = _AUTORESPONDER_DELAY_RE.search(normalized_line)
        if delay_match:
            try:
                delay_min = float(str(delay_match.group("min")).replace(",", "."))
                self._autoresponder_delay_min_s = max(0.0, delay_min)
            except Exception:
                pass
            delay_max_raw = delay_match.group("max")
            if delay_max_raw is not None:
                try:
                    delay_max = float(str(delay_max_raw).replace(",", "."))
                    self._autoresponder_delay_max_s = max(0.0, delay_max)
                except Exception:
                    pass
            elif self._autoresponder_delay_min_s is not None:
                self._autoresponder_delay_max_s = self._autoresponder_delay_min_s
            self._update_autoresponder_runtime_text()

        if self._autoresponder_running:
            counters_updated = False

            response_match = _AUTORESPONDER_RESPONSE_RE.search(normalized_line)
            if response_match:
                status = response_match.group("status").strip().lower()
                if status == "ok":
                    self._autoresponder_responses_ok += 1
                else:
                    self._autoresponder_responses_error += 1
                counters_updated = True

            if _AUTORESPONDER_FOLLOWUP_OK_RE.search(normalized_line):
                self._autoresponder_followups_ok += 1
                counters_updated = True

            if (
                "seguimiento no verificado" in normalized
                or "no se pudo enviar seguimiento" in normalized
            ):
                self._autoresponder_followups_error += 1
                counters_updated = True

            attempts_match = _AUTORESPONDER_FOLLOWUP_ATTEMPTS_RE.search(normalized_line)
            if attempts_match:
                try:
                    self._autoresponder_followups_attempted = max(
                        0, int(attempts_match.group("value"))
                    )
                except Exception:
                    self._autoresponder_followups_attempted = None
                counters_updated = True

            sent_match = _AUTORESPONDER_FOLLOWUP_SENT_RE.search(normalized_line)
            if sent_match:
                try:
                    sent_value = max(0, int(sent_match.group("value")))
                    self._autoresponder_followups_ok = sent_value
                    if self._autoresponder_followups_attempted is not None:
                        self._autoresponder_followups_error = max(
                            0, self._autoresponder_followups_attempted - sent_value
                        )
                except Exception:
                    pass
                counters_updated = True

            if counters_updated:
                self._update_autoresponder_status_blocks()

            self._append_autoresponder_log(normalized_line)

            if (
                "bot detenido" in normalized
                or "auto responder detenido" in normalized
                or "autoresponder detenido" in normalized
            ):
                self._set_autoresponder_running(False)

    def _update_exec_row(
        self,
        account: str,
        lead: str,
        row_time: str,
        result: str,
        detail: str,
    ) -> None:
        key = account or "-"
        if key in self._exec_inflight_rows:
            row = self._exec_inflight_rows[key]
        else:
            row = self._exec_inflight_table.rowCount()
            self._exec_inflight_table.insertRow(row)
            self._exec_inflight_rows[key] = row

        values = [account or "-", lead or "-", row_time or "-", result or "-", detail or ""]
        for col, value in enumerate(values):
            item = self._exec_inflight_table.item(row, col)
            if item is None:
                item = QTableWidgetItem(value)
                self._exec_inflight_table.setItem(row, col, item)
            else:
                item.setText(value)

    def _build_input_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        container = QWidget()
        container.setObjectName("SubmenuScrollContent")
        container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(10, 10, 10, 10)
        container_layout.setSpacing(10)

        self._input_title_label = QLabel("Input")
        self._input_title_label.setObjectName("PageTitle")
        self._input_prompt_label = QLabel("Waiting for backend input request.")
        self._input_prompt_label.setObjectName("MutedText")
        self._input_prompt_label.setWordWrap(True)

        self._input_line = QLineEdit()
        self._input_line.setObjectName("InputField")
        self._input_line.setPlaceholderText("Type response and press Enter")
        self._input_line.returnPressed.connect(self._submit_free_text)

        buttons = QWidget()
        buttons_layout = QHBoxLayout(buttons)
        buttons_layout.setContentsMargins(0, 0, 0, 0)
        buttons_layout.setSpacing(8)

        submit_button = QPushButton("Submit")
        submit_button.setObjectName("PrimaryButton")
        submit_button.clicked.connect(self._submit_free_text)

        empty_button = QPushButton("Submit Empty")
        empty_button.setObjectName("SecondaryButton")
        empty_button.clicked.connect(lambda: self._submit_current_input(""))

        buttons_layout.addWidget(submit_button)
        buttons_layout.addWidget(empty_button)
        buttons_layout.addStretch(1)

        container_layout.addWidget(self._input_title_label)
        container_layout.addWidget(self._input_prompt_label)
        container_layout.addWidget(self._input_line)
        container_layout.addWidget(buttons)
        container_layout.addStretch(1)

        layout.addWidget(container, 1)
        return page

    def _build_log_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("LogPanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(6)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        title = QLabel("Console log")
        title.setObjectName("PageTitle")

        self._log_toggle_button = QToolButton()
        self._log_toggle_button.setObjectName("ToggleButton")
        self._log_toggle_button.clicked.connect(self._toggle_log_panel)

        clear_button = QPushButton("Clear")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._log_console.clear)

        header_layout.addWidget(title)
        header_layout.addStretch(1)
        header_layout.addWidget(self._log_toggle_button)
        header_layout.addWidget(clear_button)

        self._log_body = QWidget()
        body_layout = QVBoxLayout(self._log_body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(0)
        body_layout.addWidget(self._log_console)

        layout.addWidget(header)
        layout.addWidget(self._log_body)
        return panel

    def _apply_log_panel_state(self) -> None:
        if self._log_expanded:
            self._log_body.setVisible(True)
            self._log_body.setMinimumHeight(180)
            self._log_body.setMaximumHeight(180)
            self._log_toggle_button.setText("Collapse")
        else:
            self._log_body.setVisible(False)
            self._log_body.setMinimumHeight(0)
            self._log_body.setMaximumHeight(0)
            self._log_toggle_button.setText("Expand")

    def _set_page(self, page_index: int) -> None:
        self._stack.setCurrentIndex(page_index)

    def _set_status(self, value: str) -> None:
        self._status_value.setText(value)

    def _append_engine_log_line(self, text: str) -> None:
        if not text or not hasattr(self, "_log_console"):
            return
        cursor = self._log_console.textCursor()
        cursor.movePosition(QTextCursor.End)
        cursor.insertText(text)
        self._log_console.setTextCursor(cursor)
        self._log_console.ensureCursorVisible()

    @Slot(str, str, str)
    def _on_engine_state_changed(self, previous: str, current: str, reason: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self._append_engine_log_line(
            f"[engine] {stamp} {previous} -> {current} ({reason})\n"
        )

    def _emit_engine_event(self, event_type: str, **payload: Any) -> None:
        if not USE_ENGINE_STATE_MANAGER:
            return
        manager = self._engine_state_manager
        if manager is None:
            return
        event: dict[str, Any] = {"type": str(event_type or "").strip().upper()}
        if payload:
            event.update(payload)
        manager.handle_backend_event(event)

    def _dbg(self, message: str) -> None:
        if not DEBUG_UI_FLOW:
            return
        self._append_log(f"[DBG] {message}\n")

    def _backend_runner(self, entrypoint: Callable[[], None]) -> None:
        exit_code = 0
        try:
            entrypoint()
        except SystemExit as exc:
            raw = exc.code
            if isinstance(raw, int):
                exit_code = raw
            elif raw in (None, ""):
                exit_code = 0
            else:
                exit_code = 1
            if raw not in (None, 0, ""):
                self.backend_log.emit(f"[gui] backend raised SystemExit({raw}).\n")
        except BaseException:
            self.backend_failed.emit(traceback.format_exc())
            return
        self.backend_done.emit(exit_code)

    def _consume_send_prompt_context_line(self, line: str) -> None:
        stripped = str(line or "").strip()
        if not stripped:
            return
        normalized_line = self._normalize_execution_line(stripped)
        normalized = _normalized_label(normalized_line)
        if not normalized:
            return

        if "las siguientes cuentas necesitan volver a iniciar sesion" in normalized:
            self._capture_session_list = True
            self._send_session_issue_accounts.clear()
            self._emit_engine_event("SESSION_ISSUES_DETECTED")
            return

        if self._capture_session_list:
            issue_match = _ACCOUNT_REASON_LINE_RE.match(normalized_line)
            if issue_match:
                username = issue_match.group("username").strip()
                reason = issue_match.group("reason").strip()
                self._send_session_issue_accounts.append((username, reason))
                return
            if not stripped.startswith("-"):
                self._capture_session_list = False

        if "se detectaron cuentas en modo bajo perfil" in normalized:
            self._capture_low_profile_list = True
            self._send_low_profile_accounts.clear()
            self._emit_engine_event("LOW_PROFILE_DETECTED")
            return

        if self._capture_low_profile_list:
            low_profile_match = _ACCOUNT_REASON_LINE_RE.match(normalized_line)
            if low_profile_match:
                username = low_profile_match.group("username").strip()
                reason = low_profile_match.group("reason").strip()
                self._send_low_profile_accounts.append((username, reason))
                return
            if not stripped.startswith("-"):
                self._capture_low_profile_list = False

        if (
            "no hay cuentas con sesion valida para enviar mensajes" in normalized
            or "no se pudo iniciar sesion en ninguna cuenta" in normalized
        ):
            self._send_login_total_failure = True
            self._emit_engine_event("LOGIN_TOTAL_FAILURE")

    def _run_send_critical_choice_dialog(
        self,
        *,
        title: str,
        body: str,
        detail_lines: Optional[list[str]] = None,
        positive_label: str = "Sí",
        negative_label: Optional[str] = "No",
    ) -> bool:
        self._block_navigation = True
        try:
            result = {"value": False}
            dialog = QDialog(self)
            dialog.setObjectName("SendCriticalDialog")
            dialog.setWindowTitle("Envío de mensajes")
            dialog.setModal(True)
            dialog.setMinimumWidth(600)
            dialog.setFont(QFont("Segoe UI Emoji", 10))
            dialog.setStyleSheet(
                """
                QDialog#SendCriticalDialog {
                    background-color: #0b1220;
                    color: #e2e8f0;
                    border: 1px solid #1e293b;
                    border-radius: 12px;
                }
                QLabel#SendCriticalTitle {
                    font-size: 20px;
                    font-weight: 700;
                    color: #f8fafc;
                }
                QLabel#SendCriticalBody {
                    font-size: 14px;
                    color: #cbd5e1;
                }
                QLabel#SendCriticalList {
                    font-size: 13px;
                    color: #e2e8f0;
                }
                QPushButton#SendCriticalPrimary {
                    background-color: #2563eb;
                    color: #ffffff;
                    border: none;
                    border-radius: 8px;
                    padding: 8px 18px;
                    font-weight: 700;
                }
                QPushButton#SendCriticalPrimary:hover {
                    background-color: #1d4ed8;
                }
                QPushButton#SendCriticalSecondary {
                    background-color: #334155;
                    color: #e2e8f0;
                    border: none;
                    border-radius: 8px;
                    padding: 8px 18px;
                    font-weight: 700;
                }
                QPushButton#SendCriticalSecondary:hover {
                    background-color: #475569;
                }
                """
            )

            layout = QVBoxLayout(dialog)
            layout.setContentsMargins(18, 18, 18, 18)
            layout.setSpacing(10)

            title_label = QLabel(title)
            title_label.setObjectName("SendCriticalTitle")
            title_label.setAlignment(Qt.AlignCenter)

            body_label = QLabel(body)
            body_label.setObjectName("SendCriticalBody")
            body_label.setWordWrap(True)

            layout.addWidget(title_label)
            layout.addWidget(body_label)

            if detail_lines:
                details = QLabel("\n".join(detail_lines))
                details.setObjectName("SendCriticalList")
                details.setWordWrap(True)
                details.setAlignment(Qt.AlignLeft | Qt.AlignTop)
                layout.addWidget(details)

            button_row = QHBoxLayout()
            button_row.setContentsMargins(0, 4, 0, 0)
            button_row.setSpacing(10)
            button_row.addStretch(1)

            positive = QPushButton(positive_label)
            positive.setObjectName("SendCriticalPrimary")
            positive.clicked.connect(lambda: (result.__setitem__("value", True), dialog.accept()))
            button_row.addWidget(positive)

            if negative_label:
                negative = QPushButton(negative_label)
                negative.setObjectName("SendCriticalSecondary")
                negative.clicked.connect(lambda: (result.__setitem__("value", False), dialog.accept()))
                button_row.addWidget(negative)

            button_row.addStretch(1)
            layout.addLayout(button_row)
            dialog.exec()
            return bool(result["value"])
        finally:
            self._block_navigation = False

    def _maybe_intercept_send_critical_prompt(self, request: InputRequest) -> bool:
        prompt_text = _strip_cli_hints(request.prompt or "")
        prompt_norm = _normalized_label(prompt_text)
        if not prompt_norm:
            return False

        if "aplicar limites conservadores automaticamente" in prompt_norm:
            self._emit_engine_event("LOW_PROFILE_DECISION_REQUEST")
            lines = [
                f"@{username} - {reason}"
                for username, reason in self._send_low_profile_accounts
            ]
            choice = self._run_send_critical_choice_dialog(
                title="⚠️ Cuentas con perfil bajo",
                body="Se detectaron cuentas en modo bajo perfil. ¿Aplicar límites conservadores automáticamente?",
                detail_lines=lines or None,
                positive_label="Sí",
                negative_label="No",
            )
            self._capture_low_profile_list = False
            self._emit_engine_event(
                "DECISION_SUBMITTED",
                decision="S" if choice else "N",
                decision_type="LOW_PROFILE",
            )
            self._submit_current_input("S" if choice else "N")
            return True

        if "iniciar sesion ahora" in prompt_norm:
            self._emit_engine_event("SESSION_LOGIN_DECISION_REQUEST")
            issue_lines = [
                f"@{username} - {reason}"
                for username, reason in self._send_session_issue_accounts
            ]
            partial_prompt = (
                "continuar" in prompt_norm
                or "cuentas activas" in prompt_norm
                or "solo" in prompt_norm
            )
            if partial_prompt:
                choice = self._run_send_critical_choice_dialog(
                    title="⚠️ Inicio de sesión parcial",
                    body=(
                        "Se detectaron cuentas con sesión inválida. "
                        "¿Desea continuar solo con las cuentas activas?"
                    ),
                    detail_lines=issue_lines or None,
                    positive_label="Continuar",
                    negative_label="Cancelar",
                )
            else:
                choice = self._run_send_critical_choice_dialog(
                    title="🔐 Sesión requerida",
                    body=(
                        "Se detectaron cuentas sin sesión activa o con sesión vencida.\n"
                        "¿Desea iniciar sesión ahora?"
                    ),
                    detail_lines=issue_lines or None,
                    positive_label="Sí",
                    negative_label="No",
                )
            self._capture_session_list = False
            self._emit_engine_event(
                "DECISION_SUBMITTED",
                decision="S" if choice else "N",
                decision_type="SESSION_LOGIN",
            )
            self._submit_current_input("S" if choice else "N")
            if partial_prompt and not choice:
                self._set_active_sidebar("dashboard")
                self._set_page(self.PAGE_DASHBOARD)
            return True

        if "desea continuar" in prompt_norm and (
            "cuentas activas" in prompt_norm or "solo" in prompt_norm
        ):
            self._emit_engine_event("PARTIAL_LOGIN_DECISION_REQUEST")
            issue_lines = [
                f"@{username} - {reason}"
                for username, reason in self._send_session_issue_accounts
            ]
            choice = self._run_send_critical_choice_dialog(
                title="⚠️ Inicio de sesión parcial",
                body="Se inició sesión de forma parcial. ¿Desea continuar solo con las cuentas activas?",
                detail_lines=issue_lines or None,
                positive_label="Continuar",
                negative_label="Cancelar",
            )
            self._capture_session_list = False
            self._emit_engine_event(
                "DECISION_SUBMITTED",
                decision="S" if choice else "N",
                decision_type="PARTIAL_LOGIN",
            )
            self._submit_current_input("S" if choice else "N")
            if not choice:
                self._set_active_sidebar("dashboard")
                self._set_page(self.PAGE_DASHBOARD)
            return True

        if self._send_login_total_failure and request.is_menu:
            self._emit_engine_event("LOGIN_TOTAL_FAILURE")
            issue_lines = [
                f"@{username} - {reason}"
                for username, reason in self._send_session_issue_accounts
            ]
            self._run_send_critical_choice_dialog(
                title="❌ Error de sesión",
                body="No se pudo iniciar sesión en ninguna de las cuentas seleccionadas.",
                detail_lines=issue_lines or None,
                positive_label="Ir al Dashboard",
                negative_label=None,
            )
            self._send_login_total_failure = False
            self._capture_session_list = False
            self._send_session_issue_accounts.clear()
            self._set_active_sidebar("dashboard")
            self._set_page(self.PAGE_DASHBOARD)
            return True

        if self._send_login_total_failure and (
            "presiona enter para continuar" in prompt_norm or "enter para continuar" in prompt_norm
        ):
            self._emit_engine_event("LOGIN_TOTAL_FAILURE")
            issue_lines = [
                f"@{username} - {reason}"
                for username, reason in self._send_session_issue_accounts
            ]
            self._run_send_critical_choice_dialog(
                title="❌ Error de sesión",
                body="No se pudo iniciar sesión en ninguna de las cuentas seleccionadas.",
                detail_lines=issue_lines or None,
                positive_label="Ir al Dashboard",
                negative_label=None,
            )
            self._send_login_total_failure = False
            self._capture_session_list = False
            self._send_session_issue_accounts.clear()
            self._submit_current_input("")
            self._set_active_sidebar("dashboard")
            self._set_page(self.PAGE_DASHBOARD)
            return True

        return False

    @Slot(str)
    def _append_log(self, chunk: str) -> None:
        text = _clean_log_chunk("" if chunk is None else str(chunk))
        if not text:
            return

        for raw_line in text.splitlines():
            clean_line = str(raw_line or "").strip()
            if clean_line:
                self._backend_recent_lines.append(clean_line)
            self._consume_send_prompt_context_line(raw_line)
            self._consume_execution_line(raw_line)
            self._consume_leads_filter_line(raw_line)
            self._consume_autoresponder_summary_stream_line(raw_line)
            self._consume_autoresponder_line(raw_line)
            self._consume_accounts_operation_line(raw_line)
            self._append_menu_activity_log(raw_line)

        cursor = self._log_console.textCursor()
        cursor.movePosition(QTextCursor.End)
        cursor.insertText(text)
        self._log_console.setTextCursor(cursor)
        self._log_console.ensureCursorVisible()

    @Slot(object)
    def _preview_menu_detected(self, request_obj: object) -> None:
        if not isinstance(request_obj, InputRequest):
            return
        if self._block_navigation or self._campaign_summary_detected:
            return
        if request_obj.is_menu and request_obj.menu_options:
            self._render_menu_request(request_obj, is_preview=True)

    @Slot(object)
    def _handle_input_request(self, request_obj: object) -> None:
        if not isinstance(request_obj, InputRequest):
            return
        # PHASE-0 pipeline: InputRequest from IOAdapter enters UI state router here.
        if (
            request_obj.is_menu
            and self._accounts_operation_active
            and self._is_accounts_management_menu(request_obj.menu_options)
        ):
            self._finish_accounts_operation()
        self._pending_request = request_obj
        self._set_status("Waiting input")
        if not self._auto_fill_active and not self._autoresponder_loading_overlay_active:
            self._hide_send_loading_overlay()
        prompt_text = _strip_cli_hints(request_obj.prompt or "(no prompt)")
        self._dbg(
            "backend_response_type=InputRequest "
            f"request_id={request_obj.request_id} prompt={prompt_text!r} "
            f"is_menu={request_obj.is_menu} detected_options_count={len(request_obj.menu_options)}"
        )
        self._last_prompt_value.setText(prompt_text or "(no prompt)")
        self._emit_engine_event(
            "INPUT_REQUEST_RECEIVED",
            request_id=request_obj.request_id,
            is_menu=request_obj.is_menu,
        )

        if self._campaign_summary_detected:
            duration = 0.0
            if self._campaign_start_time > 0:
                duration = max(0.0, time.time() - self._campaign_start_time)
            self._campaign_summary_detected = False
            self._block_navigation = True
            self._campaign_active = False
            self._campaign_start_time = 0.0
            self._set_campaign_running(False)
            self._show_custom_summary_modal(duration)
            return

        if self._maybe_intercept_send_critical_prompt(request_obj):
            return

        if request_obj.is_menu and request_obj.menu_options:
            primary_mapping = self._derive_primary_mapping(request_obj.menu_options)
            if self._looks_like_primary_menu(primary_mapping):
                self._emit_engine_event("MAIN_MENU_READY", request_id=request_obj.request_id)
        elif self._active_sidebar_key == "send":
            kind = self._detect_send_prompt_kind(prompt_text)
            kind_event = {
                "alias": "INPUT_REQUEST_SEND_ALIAS",
                "leads_alias": "INPUT_REQUEST_SEND_LEADS",
                "per_account": "INPUT_REQUEST_SEND_PER_ACCOUNT",
                "concurrency": "INPUT_REQUEST_SEND_CONCURRENCY",
                "delay_min": "INPUT_REQUEST_SEND_DELAY_MIN",
                "delay_max": "INPUT_REQUEST_SEND_DELAY_MAX",
                "templates_toggle": "INPUT_REQUEST_SEND_TEMPLATE_MODE",
                "templates_saved": "INPUT_REQUEST_SEND_TEMPLATE_PICK",
                "manual_message": "INPUT_REQUEST_SEND_MANUAL_MESSAGE",
            }.get(kind)
            if kind_event:
                self._emit_engine_event(kind_event, request_id=request_obj.request_id)

        self._set_execution_mode(False)
        if self._consume_send_setup_auto_fill(request_obj):
            return
        if self._consume_autoresponder_setup_auto_fill(request_obj):
            return
        if self._accounts_alias_manual_mode and self._is_accounts_alias_text_prompt(prompt_text):
            self._pending_request_is_primary_menu = False
            self._set_send_setup_visible(False)
            self._set_accounts_users_preview("")
            self._menu_title_label.setVisible(False)
            self._menu_prompt_label.setVisible(False)
            self._clear_menu_buttons()
            self._set_page(self.PAGE_MENU)
            self._set_live_input_enabled(True, prompt=prompt_text, sensitive=request_obj.sensitive)
            return
        if request_obj.is_menu and request_obj.menu_options:
            self._dbg("calling_render_buttons=True")
            self._render_menu_request(request_obj, is_preview=False)
            if self._pending_request is request_obj:
                if self._is_accounts_alias_selector_menu(request_obj.menu_options):
                    self._set_live_input_enabled(False, prompt="")
                else:
                    self._set_live_input_enabled(True, prompt=prompt_text, sensitive=False)
            return

        self._pending_request_is_primary_menu = False
        self._dbg("calling_render_buttons=False")
        if self._is_accounts_alias_text_prompt(prompt_text):
            self._set_send_setup_visible(False)
            self._set_accounts_users_preview("")
            self._menu_title_label.setVisible(False)
            self._menu_prompt_label.setVisible(False)
            self._clear_menu_buttons()
            self._set_page(self.PAGE_MENU)
            self._set_live_input_enabled(True, prompt=prompt_text, sensitive=request_obj.sensitive)
            return
        self._render_text_request(request_obj)
        self._set_live_input_enabled(True, prompt=prompt_text, sensitive=request_obj.sensitive)

    def _render_menu_request(self, request: InputRequest, *, is_preview: bool) -> None:
        self._dbg(
            f"render_menu_request request_id={request.request_id} is_preview={is_preview} "
            f"options_count={len(request.menu_options)}"
        )
        primary_mapping = self._derive_primary_mapping(request.menu_options)
        is_primary = self._looks_like_primary_menu(primary_mapping)
        self._set_execution_mode(False)
        if not is_preview:
            self._set_send_setup_visible(False)
            self._set_autoresponder_setup_visible(False)

        if is_primary:
            self._accounts_alias_select_value = ""
            self._accounts_alias_back_value = ""
            self._accounts_alias_manual_mode = False
            self._set_accounts_users_preview("")
            self._set_leads_live_card_visible(False)
            self._section_log_scope = None
            self._set_menu_activity_visible(False)
            self._pending_request_is_primary_menu = True
            self._primary_option_values.update(primary_mapping)
            self._update_primary_button_visibility()
            self._clear_menu_buttons()
            self._menu_title_label.setVisible(True)
            self._menu_title_label.setText("Menu principal")
            self._menu_prompt_label.setText("Selecciona una seccion desde el sidebar.")
            self._refresh_menu_activity_log()

            if self._queued_primary_key and not is_preview:
                queued_key = self._queued_primary_key
                value = self._primary_option_values.get(queued_key)
                if value:
                    self._queued_primary_key = None
                    QTimer.singleShot(0, lambda v=value: self._submit_current_input(v))
            return

        if is_preview:
            return

        self._pending_request_is_primary_menu = False
        self._queued_primary_key = None
        self._set_menu_activity_visible(False)
        leads_context = self._active_sidebar_key == "leads"
        self._set_leads_live_card_visible(leads_context)
        if self._active_sidebar_key == "autoresponder":
            self._autoresponder_activate_option_value = ""
            for option in request.menu_options:
                label_norm = _normalized_label(option.label or "")
                if "activar bot" in label_norm:
                    self._autoresponder_activate_option_value = str(option.value or "").strip()
                    break
        if self._is_accounts_alias_selector_menu(request.menu_options):
            self._render_accounts_alias_selector_view(request)
            return
        self._accounts_alias_select_value = ""
        self._accounts_alias_back_value = ""
        self._accounts_alias_manual_mode = False
        cleaned_prompt = _strip_cli_hints(request.prompt or "Selecciona una opcion:")
        rendered_prompt = cleaned_prompt or "Selecciona una opcion:"
        accounts_users_menu = (
            self._active_sidebar_key == "accounts"
            and self._is_accounts_management_menu(request.menu_options)
        )
        if accounts_users_menu:
            self._menu_title_label.setVisible(False)
            self._menu_title_label.setText("")
            self._menu_prompt_label.setVisible(True)
        else:
            self._menu_title_label.setVisible(True)
            self._menu_prompt_label.setVisible(True)
            self._menu_title_label.setText(request.menu_title or "Submenu")
        if accounts_users_menu:
            rendered_prompt = "Opcion:"
            alias = self._resolve_selected_accounts_alias(request)
            self._set_accounts_users_preview(self._accounts_alias_users_text(alias))
        else:
            self._set_accounts_users_preview("")
        self._menu_prompt_label.setText(rendered_prompt)
        if leads_context:
            self._menu_title_label.setText("Filtrado de leads")
            self._update_leads_live_prompt(rendered_prompt)
        self._rebuild_menu_buttons(request.menu_options)
        if not self._showing_summary and not self._block_navigation:
            self._set_page(self.PAGE_MENU)
        if self._active_sidebar_key in self._SECTION_LOG_KEYS:
            self._section_log_scope = self._active_sidebar_key
        self._refresh_menu_activity_log()
        if leads_context:
            self._set_menu_activity_visible(False)
        self._set_live_input_enabled(True, prompt=rendered_prompt, sensitive=False)

    def _derive_primary_mapping(self, options: list[MenuOption]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for option in options:
            key = self._match_primary_key(option.label)
            if key and key not in mapping:
                mapping[key] = option.value
        return mapping

    def _looks_like_primary_menu(self, mapping: dict[str, str]) -> bool:
        base_keys = (
            "accounts",
            "leads",
            "send",
            "logs",
            "autoresponder",
            "stats",
            "whatsapp",
            "updates",
            "exit",
        )
        matched = sum(1 for key in base_keys if key in mapping)
        return matched >= 7

    def _match_primary_key(self, label: str) -> Optional[str]:
        norm = _normalized_label(label)
        if "gestionar cuentas" in norm or ("cuentas" in norm and "instagram" in norm):
            return "accounts"
        if "gestionar leads" in norm or "leads" in norm:
            return "leads"
        if "enviar mensajes" in norm:
            return "send"
        if "registros" in norm and "env" in norm:
            return "logs"
        if "auto responder" in norm or "autoresponder" in norm:
            return "autoresponder"
        if "estadisticas" in norm or "metricas" in norm:
            return "stats"
        if "whatsapp" in norm:
            return "whatsapp"
        if "entregar" in norm and "cliente" in norm:
            return "deliver"
        if "actualizaciones" in norm:
            return "updates"
        if "salir" in norm:
            return "exit"
        return None

    def _is_accounts_alias_selector_menu(self, options: list[MenuOption]) -> bool:
        if self._active_sidebar_key != "accounts":
            return False
        if not options:
            return False
        normalized = [_normalized_label(option.label or "") for option in options]
        has_select_alias = any(
            ("seleccionar alias" in line and "crear" in line) for line in normalized
        )
        has_back = any("volver" in line for line in normalized)
        return has_select_alias and has_back

    def _is_accounts_alias_text_prompt(self, prompt: str) -> bool:
        if self._active_sidebar_key != "accounts":
            return False
        normalized = _normalized_label(prompt or "")
        if "alias" not in normalized:
            return False
        tokens = (
            "grupo",
            "cuenta",
            "default",
            "vacio",
            "vac o",
        )
        return any(token in normalized for token in tokens)

    @staticmethod
    def _menu_label_for_value(options: list[MenuOption], value: str) -> str:
        target = str(value).strip()
        for option in options:
            if str(option.value).strip() == target:
                return str(option.label or "").strip()
        return ""

    def _is_accounts_management_menu(self, options: list[MenuOption]) -> bool:
        if len(options) < 8:
            return False
        normalized = [_normalized_label(option.label or "") for option in options]
        checks = 0
        if any("agregar cuenta" in line for line in normalized):
            checks += 1
        if any("agregar cuentas" in line and "csv" in line for line in normalized):
            checks += 1
        if any("eliminar cuenta" in line for line in normalized):
            checks += 1
        if any("proxy" in line and ("activar" in line or "desactivar" in line) for line in normalized):
            checks += 1
        if any("iniciar sesion" in line and "guardar sesion" in line for line in normalized):
            checks += 1
        if any("exportar" in line and "csv" in line for line in normalized):
            checks += 1
        if any("mover cuentas" in line and "alias" in line for line in normalized):
            checks += 1
        if any("volver" in line for line in normalized):
            checks += 1
        return checks >= 5

    def _ensure_log_panel_visible(self) -> None:
        if self._log_expanded:
            return
        self._log_expanded = True
        self._apply_log_panel_state()

    def _set_accounts_summary_override(self, success: int, failed: int, skipped: int) -> None:
        values = {
            "success": max(0, int(success)),
            "failed": max(0, int(failed)),
            "skipped": max(0, int(skipped)),
        }
        if self._accounts_summary_override is None:
            self._accounts_summary_override = values
            return
        self._accounts_summary_override = {
            key: max(self._accounts_summary_override.get(key, 0), value)
            for key, value in values.items()
        }

    def _record_accounts_result(self, status: str, usernames: list[str]) -> None:
        if status not in self._accounts_result_counts:
            return
        if usernames:
            rank = {"success": 1, "skipped": 2, "failed": 3}
            for username in usernames:
                key = username.strip().lower()
                if not key:
                    continue
                current = self._accounts_result_by_user.get(key)
                if current is None or rank[status] >= rank.get(current, 0):
                    self._accounts_result_by_user[key] = status
            return
        self._accounts_result_counts[status] += 1

    @staticmethod
    def _extract_account_usernames(text: str) -> list[str]:
        usernames: list[str] = []
        seen: set[str] = set()
        for value in _ACCOUNT_USERNAME_RE.findall(text):
            key = value.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            usernames.append(key)
        return usernames

    def _start_accounts_operation(self, operation_label: str) -> None:
        label = _strip_cli_hints(operation_label or "").strip() or "Operacion de cuentas"
        self._accounts_operation_active = True
        self._accounts_operation_name = label
        self._accounts_operation_started_at = time.monotonic()
        self._accounts_result_by_user.clear()
        self._accounts_result_counts = {"success": 0, "failed": 0, "skipped": 0}
        self._accounts_summary_override = None
        self._ensure_log_panel_visible()

    def _finish_accounts_operation(self) -> None:
        if not self._accounts_operation_active:
            return

        per_user = self._accounts_result_by_user
        counts = self._accounts_result_counts

        success = sum(1 for status in per_user.values() if status == "success") + counts["success"]
        failed = sum(1 for status in per_user.values() if status == "failed") + counts["failed"]
        skipped = sum(1 for status in per_user.values() if status == "skipped") + counts["skipped"]

        if self._accounts_summary_override:
            success = max(success, self._accounts_summary_override.get("success", 0))
            failed = max(failed, self._accounts_summary_override.get("failed", 0))
            skipped = max(skipped, self._accounts_summary_override.get("skipped", 0))

        self._accounts_operation_active = False
        self._accounts_operation_name = ""
        self._accounts_operation_started_at = 0.0
        self._accounts_result_by_user.clear()
        self._accounts_result_counts = {"success": 0, "failed": 0, "skipped": 0}
        self._accounts_summary_override = None

        self._append_log(
            f"Successful accounts: {success}\n"
            f"Failed accounts: {failed}\n"
            f"Skipped: {skipped}\n"
        )

    def _maybe_start_accounts_operation(self, request: InputRequest, value: str) -> None:
        if self._active_sidebar_key != "accounts":
            return
        if not request.is_menu or not request.menu_options:
            return
        if not self._is_accounts_management_menu(request.menu_options):
            return
        selected_label = self._menu_label_for_value(request.menu_options, value)
        if not selected_label:
            return
        if "volver" in _normalized_label(selected_label):
            return
        self._start_accounts_operation(selected_label)

    def _consume_accounts_operation_line(self, line: str) -> None:
        if not self._accounts_operation_active:
            return

        stripped = line.strip()
        if not stripped:
            return
        if stripped.startswith("[gui]"):
            return

        normalized = _normalized_label(stripped)
        if not normalized:
            return

        relogin_match = _ACCOUNTS_RELOGIN_SUMMARY_RE.search(normalized)
        if relogin_match:
            self._set_accounts_summary_override(
                int(relogin_match.group("ok")),
                int(relogin_match.group("failed")),
                int(relogin_match.group("skipped")),
            )
            return

        csv_login_match = _ACCOUNTS_CSV_LOGIN_SUMMARY_RE.search(normalized)
        if csv_login_match:
            self._set_accounts_summary_override(
                int(csv_login_match.group("ok")),
                int(csv_login_match.group("failed")),
                int(csv_login_match.group("skipped")),
            )
            return

        moved_match = _ACCOUNTS_MOVED_SUMMARY_RE.search(normalized)
        if moved_match:
            self._set_accounts_summary_override(int(moved_match.group("ok")), 0, 0)
            return

        added_match = _ACCOUNTS_ADDED_SUMMARY_RE.search(normalized)
        if added_match and self._accounts_summary_override is None:
            self._set_accounts_summary_override(int(added_match.group("ok")), 0, 0)
            return

        ratio_match = _ACCOUNTS_RATIO_SUMMARY_RE.search(stripped)
        if ratio_match and "manual" in normalized:
            ok_count = int(ratio_match.group("ok"))
            total_count = int(ratio_match.group("total"))
            self._set_accounts_summary_override(ok_count, max(0, total_count - ok_count), 0)
            return

        status_match = _ACCOUNT_STATUS_LINE_RE.search(stripped)
        if status_match:
            username = status_match.group("username").strip().lower()
            status_text = status_match.group("status").strip().lower()
            if "fail" in status_text or "error" in status_text:
                self._record_accounts_result("failed", [username])
            elif "skip" in status_text or "omit" in status_text:
                self._record_accounts_result("skipped", [username])
            else:
                self._record_accounts_result("success", [username])
            return

        usernames = self._extract_account_usernames(stripped)
        lowered = stripped.lower()
        starts_ok = stripped.startswith("✔") or lowered.startswith("[ok]")
        starts_warn = stripped.startswith("⚠") or lowered.startswith("[advertencia]")
        starts_error = stripped.startswith("✖") or lowered.startswith("[error]")

        skipped_tokens = (
            "omit",
            "skip",
            "sin seleccion",
            "sin password",
            "no se seleccionaron",
            "cancelad",
        )
        failed_tokens = (
            "fall",
            "error",
            "failed",
            "no se pudo",
            "inval",
            "no existe",
            "rechaz",
        )
        success_tokens = (
            "agregad",
            "actualizad",
            "eliminad",
            "guardad",
            "proxy ok",
            "archivo csv generado",
            "backup totp cifrado generado",
        )

        if starts_ok:
            self._record_accounts_result("success", usernames)
            return
        if starts_error:
            self._record_accounts_result("failed", usernames)
            return
        if starts_warn:
            status = "skipped" if any(token in normalized for token in skipped_tokens) else "failed"
            self._record_accounts_result(status, usernames)
            return

        if any(token in normalized for token in skipped_tokens):
            self._record_accounts_result("skipped", usernames)
            return
        if any(token in normalized for token in failed_tokens):
            self._record_accounts_result("failed", usernames)
            return
        if any(token in normalized for token in success_tokens):
            self._record_accounts_result("success", usernames)

    def _load_accounts_aliases(self) -> list[str]:
        records: list[dict[str, Any]] = []
        try:
            module = import_module("accounts")
            load_callable = getattr(module, "_load", None)
            if callable(load_callable):
                payload = load_callable()
                if isinstance(payload, list):
                    records = [item for item in payload if isinstance(item, dict)]
            if not records:
                list_callable = getattr(module, "list_all", None)
                if callable(list_callable):
                    payload = list_callable()
                    if isinstance(payload, list):
                        records = [item for item in payload if isinstance(item, dict)]
        except Exception:
            records = []

        if not records:
            payload = self._read_json(self._root_dir / "data" / "accounts.json", [])
            if isinstance(payload, list):
                records = [item for item in payload if isinstance(item, dict)]

        aliases = {"default"}
        for record in records:
            alias = str(record.get("alias") or "default").strip() or "default"
            aliases.add(alias)
        return sorted(aliases)

    def _load_account_usernames_for_alias(self, alias: str) -> list[str]:
        records: list[dict[str, Any]] = []
        try:
            module = import_module("accounts")
            list_callable = getattr(module, "list_all", None)
            if callable(list_callable):
                payload = list_callable()
                if isinstance(payload, list):
                    records = [item for item in payload if isinstance(item, dict)]
        except Exception:
            records = []

        if not records:
            payload = self._read_json(self._root_dir / "data" / "accounts.json", [])
            if isinstance(payload, list):
                records = [item for item in payload if isinstance(item, dict)]

        target_alias = str(alias or "").strip().lower()
        usernames: list[str] = []
        for item in records:
            item_alias = str(item.get("alias") or "default").strip().lower()
            if item_alias != target_alias:
                continue
            username = str(item.get("username") or "").strip().lstrip("@")
            if username:
                usernames.append(username)
        return usernames

    def _resolve_leads_alias_value(self, raw_value: str) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return self._leads_account_alias
        aliases = self._load_accounts_aliases()
        if value.isdigit():
            idx = int(value)
            if 1 <= idx <= len(aliases):
                return aliases[idx - 1]
        for alias in aliases:
            if alias.lower() == value.lower():
                return alias
        return value

    @staticmethod
    def _extract_manual_account_count(raw_value: str, max_accounts: int) -> Optional[int]:
        value = str(raw_value or "").strip()
        if not value:
            return None
        indexes: set[int] = set()
        for chunk in value.split(","):
            part = chunk.strip()
            if not part.isdigit():
                continue
            idx = int(part)
            if idx <= 0:
                continue
            if max_accounts > 0 and idx > max_accounts:
                continue
            indexes.add(idx)
        if not indexes:
            return None
        return len(indexes)

    def _is_leads_accounts_selection_menu(self, options: list[MenuOption]) -> bool:
        if not options:
            return False
        normalized = [_normalized_label(option.label or "") for option in options]
        has_all = any("usar todas las cuentas del alias" in line for line in normalized)
        has_manual = any("seleccionar cuentas manualmente" in line for line in normalized)
        return has_all and has_manual

    def _capture_leads_setup_input(self, request: InputRequest, value: str) -> None:
        if self._active_sidebar_key != "leads":
            return
        if request.is_menu and self._pending_request_is_primary_menu:
            return
        prompt_text = _strip_cli_hints(request.prompt or "")
        prompt_norm = _normalized_label(prompt_text)
        clean_value = str(value or "").strip()

        visible_value = "***" if request.sensitive and clean_value else clean_value
        self._append_leads_live_submission(prompt_text, visible_value)

        if "alias nombre para guardar leads filtrados" in prompt_norm:
            self._leads_export_alias = clean_value or "leads_filtrados"
            self._update_leads_metrics_view()
            return

        if "alias para correr el filtrado" in prompt_norm:
            alias_value = self._resolve_leads_alias_value(clean_value)
            self._leads_account_alias = alias_value or "-"
            self._leads_account_usernames = self._load_account_usernames_for_alias(alias_value)
            self._leads_accounts_planned = None
            self._leads_waiting_manual_accounts = False
            self._update_leads_metrics_view()
            return

        if request.is_menu and self._is_leads_accounts_selection_menu(request.menu_options):
            if clean_value == "1":
                self._leads_accounts_planned = len(self._leads_account_usernames)
                self._leads_waiting_manual_accounts = False
            elif clean_value == "2":
                self._leads_accounts_planned = None
                self._leads_waiting_manual_accounts = True
            self._update_leads_metrics_view()
            return

        if "indices de cuentas" in prompt_norm:
            manual_count = self._extract_manual_account_count(
                clean_value, len(self._leads_account_usernames)
            )
            if manual_count is not None:
                self._leads_accounts_planned = manual_count
            self._leads_waiting_manual_accounts = False
            self._update_leads_metrics_view()

    def _show_leads_completion_dialog(self) -> None:
        if self._leads_completion_announced:
            return
        self._leads_completion_announced = True

        alias = self._leads_export_alias or "leads_filtrados"
        total = self._leads_processed_count
        qualified = self._leads_qualified_count
        discarded = self._leads_discarded_count

        dialog = QDialog(self)
        dialog.setObjectName("LeadsSummaryDialog")
        dialog.setWindowTitle("Filtrado completado")
        dialog.setModal(True)
        dialog.setMinimumWidth(520)

        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("Filtrado finalizado con éxito")
        title.setObjectName("LeadsSummaryTitle")
        title.setAlignment(Qt.AlignCenter)

        body = QLabel(
            f"Total filtradas: {total}\n"
            f"Calificadas: {qualified}\n"
            f"Descartadas: {discarded}\n"
            f"Alias de guardado: {alias}"
        )
        body.setObjectName("LeadsSummaryBody")
        body.setWordWrap(True)
        body.setAlignment(Qt.AlignCenter)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(dialog.accept)

        layout.addWidget(title)
        layout.addWidget(body)
        layout.addWidget(close_button, 0, Qt.AlignCenter)
        dialog.exec()

    def _render_text_request(self, request: InputRequest) -> None:
        self._set_execution_mode(False)
        self._set_accounts_users_preview("")
        cleaned_prompt = _strip_cli_hints(request.prompt or "Ingresa un valor:")
        leads_context = self._active_sidebar_key == "leads"
        send_context = self._active_sidebar_key == "send"
        self._set_leads_live_card_visible(leads_context)
        self._set_send_setup_visible(send_context)
        self._menu_title_label.setVisible(True)
        if leads_context:
            self._menu_title_label.setText("Filtrado de leads")
        elif send_context:
            self._menu_title_label.setText("Envío de mensajes")
        else:
            self._menu_title_label.setText("Input")
        self._menu_prompt_label.setVisible(True)
        if send_context:
            self._menu_prompt_label.setText("Completa la configuración y presiona Enviar.")
        else:
            self._menu_prompt_label.setText(cleaned_prompt or "Ingresa un valor:")
        if leads_context:
            self._update_leads_live_prompt(cleaned_prompt)
        if send_context:
            self._configure_send_setup_from_request(request)
        self._clear_menu_buttons()
        self._menu_options_layout.takeAt(0)
        self._menu_options_layout.addStretch(1)
        self._set_page(self.PAGE_MENU)

    def _clear_menu_buttons(self) -> None:
        self._dbg(f"clearing_buttons={len(self._menu_option_buttons)}")
        for button in self._menu_option_buttons:
            button.deleteLater()
        self._menu_option_buttons.clear()

        while self._menu_options_layout.count():
            item = self._menu_options_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        self._menu_options_layout.addStretch(1)

    def _rebuild_menu_buttons(self, options: list[MenuOption]) -> None:
        self._clear_menu_buttons()
        self._menu_options_layout.takeAt(0)

        for option in options:
            clean_label = (
                _strip_cli_hints((option.label or "").strip()) or f"Option {option.value}"
            )
            button = QPushButton(clean_label)
            button.setObjectName("MenuOptionButton")
            button.setMinimumHeight(52)
            is_autoresponder_activate = False
            if self._active_sidebar_key == "autoresponder":
                option_value = str(option.value or "").strip()
                if (
                    option_value
                    and option_value == str(self._autoresponder_activate_option_value or "").strip()
                ):
                    is_autoresponder_activate = True
                else:
                    label_norm = _normalized_label(option.label or "")
                    if "activar bot" in label_norm:
                        is_autoresponder_activate = True

            if is_autoresponder_activate:
                button.clicked.connect(
                    lambda checked=False, value=option.value: self._start_autoresponder_setup_from_menu(
                        str(value or "").strip()
                    )
                )
            else:
                button.clicked.connect(
                    lambda checked=False, value=option.value: self._submit_current_input(value)
                )
            self._menu_options_layout.addWidget(button)
            self._menu_option_buttons.append(button)

        self._menu_options_layout.addStretch(1)
        self._dbg(f"buttons_created={len(self._menu_option_buttons)}")

    def _submit_free_text(self) -> None:
        self._submit_current_input(self._input_line.text())

    def _submit_live_input(self) -> None:
        self._submit_current_input(self._live_input_field.text())

    def _set_live_input_enabled(self, enabled: bool, *, prompt: str, sensitive: bool = False) -> None:
        prompt_text = _strip_cli_hints(prompt or "").strip()
        send_panel_active = bool(
            enabled
            and self._active_sidebar_key == "send"
            and hasattr(self, "_send_setup_card")
            and self._send_setup_card.isVisible()
        )
        if send_panel_active:
            self._live_input_prompt.setText("Configuración de envío activa en el panel central.")
            self._live_input_field.setEnabled(False)
            self._live_input_submit.setEnabled(False)
            self._live_input_empty.setEnabled(False)
            self._live_input_field.setEchoMode(QLineEdit.Normal)
            self._live_input_field.setPlaceholderText("Usa el panel de configuración de envío")
            return
        autoresponder_panel_active = bool(
            enabled
            and self._active_sidebar_key == "autoresponder"
            and hasattr(self, "_autoresponder_setup_card")
            and self._autoresponder_setup_card.isVisible()
        )
        if autoresponder_panel_active:
            self._live_input_prompt.setText(
                "Configuración de auto responder activa en el panel central."
            )
            self._live_input_field.setEnabled(False)
            self._live_input_submit.setEnabled(False)
            self._live_input_empty.setEnabled(False)
            self._live_input_field.setEchoMode(QLineEdit.Normal)
            self._live_input_field.setPlaceholderText(
                "Usa el panel de configuración del auto responder"
            )
            return
        if enabled:
            self._live_input_prompt.setText(prompt_text or "Entrada global (input literal)")
            self._live_input_field.setEnabled(True)
            self._live_input_submit.setEnabled(True)
            self._live_input_empty.setEnabled(True)
            self._live_input_field.setEchoMode(QLineEdit.Password if sensitive else QLineEdit.Normal)
            self._live_input_field.setPlaceholderText(
                prompt_text or "Escribe texto literal y presiona Enter"
            )
            self._live_input_field.setFocus()
            return

        self._live_input_prompt.setText("Esperando prompt del CLI...")
        self._live_input_field.setEnabled(False)
        self._live_input_submit.setEnabled(False)
        self._live_input_empty.setEnabled(False)
        self._live_input_field.setEchoMode(QLineEdit.Normal)
        self._live_input_field.setPlaceholderText("Sin input pendiente")

    def _capture_autoresponder_delay_input(self, request: InputRequest, value: str) -> None:
        raw_prompt = _strip_cli_hints(request.prompt or "")
        prompt_norm = _normalized_label(raw_prompt)
        if not prompt_norm:
            return
        raw_value = str(value).strip().replace(",", ".")
        if not raw_value:
            return
        try:
            parsed_value = max(0.0, float(raw_value))
        except Exception:
            return

        if "delay minimo entre mensajes" in prompt_norm:
            self._autoresponder_delay_min_s = parsed_value
            if (
                self._autoresponder_delay_max_s is not None
                and self._autoresponder_delay_max_s < parsed_value
            ):
                self._autoresponder_delay_max_s = parsed_value
            self._update_autoresponder_runtime_text()
            return

        if "delay maximo entre mensajes" in prompt_norm:
            self._autoresponder_delay_max_s = parsed_value
            if self._autoresponder_delay_min_s is None:
                self._autoresponder_delay_min_s = parsed_value
            self._update_autoresponder_runtime_text()

    def _submit_current_input(self, value: str) -> None:
        if not self._io_adapter:
            return

        # PHASE-0 pipeline: input submit entrypoint (Send/Enter) for pending backend request.
        request = self._pending_request
        request_id = request.request_id if request else None
        self._dbg(
            f"input_submitted={value!r} request_id={request_id} "
            f"had_pending_request={request is not None}"
        )
        if request is not None:
            self._maybe_start_accounts_operation(request, value)
            self._capture_leads_setup_input(request, value)
            self._capture_autoresponder_delay_input(request, value)
            self._scope_section_logs_for_submission(request, value)
            if request.is_menu and self._pending_request_is_primary_menu:
                selected_key = self._primary_key_for_value(value)
                if selected_key == "exit":
                    self._shutdown_requested_from_primary_exit = True
                    self._append_log("[gui] Exit requested from main menu.\n")
        accepted = self._io_adapter.fulfill_input(value, request_id=request_id)
        self._dbg(f"fulfill_input_accepted={accepted}")
        if not accepted:
            self._hide_send_loading_overlay()
            return

        visible = "***" if request and request.sensitive and value else value
        if request is None:
            self._append_log(f"[gui] input queued: {value}\n")
            self._live_input_field.clear()
            return

        if not request.is_menu and self._active_sidebar_key == "accounts":
            prompt_norm = _normalized_label(_strip_cli_hints(request.prompt or ""))
            alias_value = str(value).strip()
            if "alias" in prompt_norm and "grupo" in prompt_norm and alias_value:
                self._selected_accounts_alias = alias_value
            if self._accounts_alias_manual_mode:
                self._accounts_alias_manual_mode = False
        self._pending_request = None
        self._pending_request_is_primary_menu = False
        self._set_status("Running")
        self._set_live_input_enabled(False, prompt="")
        self._append_log(f"[gui] input submitted: {visible}\n")
        self._input_line.clear()
        self._live_input_field.clear()
        self.refresh_dashboard_metrics()

    @Slot(int)
    def _on_backend_done(self, exit_code: int) -> None:
        self.backend_exit_code = exit_code
        self._emit_engine_event("BACKEND_DONE", exit_code=exit_code)
        self._hide_send_loading_overlay()
        self._set_status(f"Finished ({exit_code})")
        self._thread_value.setText("stopped")
        self._section_log_scope = None
        self._set_menu_activity_visible(False)
        self._finish_accounts_operation()
        self._set_campaign_running(False)
        self._campaign_active = False
        self._campaign_start_time = 0.0
        self._block_navigation = False
        self._campaign_summary_detected = False
        self._campaign_summary_capture_active = False
        self._campaign_summary_alias = "-"
        self._campaign_summary_accounts.clear()
        self._campaign_summary_totals = {"ok": 0, "err": 0, "no_dm": 0, "unver": 0, "total": 0}
        self._capture_low_profile_list = False
        self._capture_session_list = False
        self._send_low_profile_accounts.clear()
        self._send_session_issue_accounts.clear()
        self._send_login_total_failure = False
        if self._autoresponder_summary_capture_active and not self._autoresponder_summary_modal_shown:
            parsed = self.parse_autoresponder_summary(list(self._autoresponder_summary_lines))
            self._autoresponder_summary_capture_active = False
            self._autoresponder_summary_modal_shown = True
            self._autoresponder_last_summary = parsed
            self._show_autoresponder_summary_modal(parsed)
        elif self._autoresponder_summary_expected and not self._autoresponder_summary_modal_shown:
            parsed = self.parse_autoresponder_summary(list(self._backend_recent_lines))
            if self._autoresponder_summary_has_content(parsed):
                self._autoresponder_summary_modal_shown = True
                self._autoresponder_last_summary = parsed
                self._show_autoresponder_summary_modal(parsed)
        self._autoresponder_summary_capture_active = False
        self._autoresponder_summary_lines.clear()
        self._autoresponder_summary_expected = False
        self._set_autoresponder_running(False)
        self._reset_autoresponder_setup_mode()
        self._set_leads_filter_running(False)
        self._set_leads_live_card_visible(False)
        self._set_execution_mode(False)
        self._set_live_input_enabled(False, prompt="")
        self._append_log(f"[gui] backend finished with code {exit_code}.\n")
        if self._shutdown_requested_from_primary_exit and not self._shutdown_started:
            QTimer.singleShot(0, lambda: self.shutdown_application(reason="main-menu-exit"))

    @Slot(str)
    def _on_backend_failed(self, traceback_text: str) -> None:
        self.backend_exit_code = 1
        self._emit_engine_event("BACKEND_ERROR")
        self._hide_send_loading_overlay()
        self._set_status("Failed (1)")
        self._thread_value.setText("stopped")
        self._section_log_scope = None
        self._set_menu_activity_visible(False)
        self._finish_accounts_operation()
        self._set_campaign_running(False)
        self._campaign_active = False
        self._campaign_start_time = 0.0
        self._block_navigation = False
        self._campaign_summary_detected = False
        self._campaign_summary_capture_active = False
        self._campaign_summary_alias = "-"
        self._campaign_summary_accounts.clear()
        self._campaign_summary_totals = {"ok": 0, "err": 0, "no_dm": 0, "unver": 0, "total": 0}
        self._capture_low_profile_list = False
        self._capture_session_list = False
        self._send_low_profile_accounts.clear()
        self._send_session_issue_accounts.clear()
        self._send_login_total_failure = False
        if self._autoresponder_summary_capture_active and not self._autoresponder_summary_modal_shown:
            parsed = self.parse_autoresponder_summary(list(self._autoresponder_summary_lines))
            self._autoresponder_summary_capture_active = False
            self._autoresponder_summary_modal_shown = True
            self._autoresponder_last_summary = parsed
            self._show_autoresponder_summary_modal(parsed)
        elif self._autoresponder_summary_expected and not self._autoresponder_summary_modal_shown:
            parsed = self.parse_autoresponder_summary(list(self._backend_recent_lines))
            if self._autoresponder_summary_has_content(parsed):
                self._autoresponder_summary_modal_shown = True
                self._autoresponder_last_summary = parsed
                self._show_autoresponder_summary_modal(parsed)
        self._autoresponder_summary_capture_active = False
        self._autoresponder_summary_lines.clear()
        self._autoresponder_summary_expected = False
        self._set_autoresponder_running(False)
        self._reset_autoresponder_setup_mode()
        self._set_leads_filter_running(False)
        self._set_leads_live_card_visible(False)
        self._set_execution_mode(False)
        self._set_live_input_enabled(False, prompt="")
        self._append_log("[gui] backend crashed.\n")
        self._append_log(traceback_text)
        if self._shutdown_requested_from_primary_exit and not self._shutdown_started:
            QTimer.singleShot(0, lambda: self.shutdown_application(reason="backend-crash-after-exit"))

    def _toggle_log_panel(self) -> None:
        self._log_expanded = not self._log_expanded
        self._apply_log_panel_state()

    @Slot()
    def refresh_dashboard_metrics(self) -> None:
        metrics = self._collect_dashboard_metrics()
        for key, value in metrics.items():
            if key not in self._metric_values:
                continue
            self._metric_values[key].setText(str(value))

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._dashboard_updated_value.setText(now_str)
        if "last_refresh" in self._metric_values:
            self._metric_values["last_refresh"].setText(now_str)

    def _show_booked_today_dialog(self) -> None:
        details = list(self._booked_today_rows)
        if not details:
            self._collect_dashboard_metrics()
            details = list(self._booked_today_rows)

        def _fmt_handle(value: Any) -> str:
            text = str(value or "").strip()
            if not text:
                return "-"
            return text if text.startswith("@") else f"@{text}"

        dialog = QDialog(self)
        dialog.setObjectName("LeadsSummaryDialog")
        dialog.setWindowTitle("Agendas enviadas hoy")
        dialog.setModal(True)
        dialog.setMinimumWidth(760)

        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("📅 Agendas enviadas hoy")
        title.setObjectName("LeadsSummaryTitle")
        title.setAlignment(Qt.AlignCenter)

        subtitle = QLabel(f"Se enviaron {len(details)} links de agendamiento hoy.")
        subtitle.setObjectName("MutedText")
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setWordWrap(True)

        if details:
            table = QTableWidget(len(details), 3)
            table.setObjectName("ExecLogConsole")
            table.setHorizontalHeaderLabels(["Usuario", "Hora", "Alias"])
            table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.NoSelection)
            table.setFocusPolicy(Qt.NoFocus)
            table.setAlternatingRowColors(True)
            table.verticalHeader().setVisible(False)
            table.horizontalHeader().setStretchLastSection(True)
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
            table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
            table.setMinimumHeight(280)
            table.setStyleSheet(
                """
                QTableWidget {
                    background-color: #0f172a;
                    alternate-background-color: #111827;
                    color: #e2e8f0;
                    gridline-color: #334155;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QHeaderView::section {
                    background-color: #1e293b;
                    color: #e2e8f0;
                    border: 1px solid #334155;
                    padding: 6px;
                    font-weight: 700;
                }
                QTableCornerButton::section {
                    background-color: #1e293b;
                    border: 1px solid #334155;
                }
                """
            )

            for index, row in enumerate(details):
                lead = _fmt_handle(row.get("recipient_username"))
                dt_value = row.get("timestamp")
                stamp = dt_value.strftime("%H:%M") if isinstance(dt_value, datetime) else "-"
                alias = str(row.get("alias") or "").strip() or "(desconocido)"

                table.setItem(index, 0, QTableWidgetItem(lead))
                table.setItem(index, 1, QTableWidgetItem(stamp))
                table.setItem(index, 2, QTableWidgetItem(alias))
        else:
            table = QLabel("No se enviaron links de agendamiento hoy.")
            table.setObjectName("MutedText")
            table.setAlignment(Qt.AlignCenter)
            table.setWordWrap(True)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(dialog.accept)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addWidget(table)
        layout.addWidget(close_button, 0, Qt.AlignCenter)
        dialog.exec()

    def _show_replied_today_dialog(self) -> None:
        details = list(self._replied_today_rows)
        if not details:
            self._collect_dashboard_metrics()
            details = list(self._replied_today_rows)

        def _fmt_handle(value: Any) -> str:
            text = str(value or "").strip()
            if not text:
                return "-"
            return text if text.startswith("@") else f"@{text}"

        def _preview(value: Any, limit: int = 180) -> str:
            text = " ".join(str(value or "").split())
            if not text:
                return "-"
            if len(text) <= limit:
                return text
            return f"{text[: max(0, limit - 3)]}..."

        dialog = QDialog(self)
        dialog.setObjectName("LeadsSummaryDialog")
        dialog.setWindowTitle("Respuestas enviadas hoy")
        dialog.setModal(True)
        dialog.setMinimumWidth(860)

        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("💬 Respuestas enviadas hoy")
        title.setObjectName("LeadsSummaryTitle")
        title.setAlignment(Qt.AlignCenter)

        subtitle = QLabel(f"Se registraron {len(details)} conversaciones respondidas hoy.")
        subtitle.setObjectName("MutedText")
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setWordWrap(True)

        details_button = QPushButton("Ver mas detalles")
        details_button.setObjectName("SecondaryButton")
        details_button.clicked.connect(
            lambda: self._show_top_responded_messages_dialog(parent=dialog)
        )

        details_action_layout = QHBoxLayout()
        details_action_layout.setContentsMargins(0, 0, 0, 0)
        details_action_layout.setSpacing(6)
        details_action_layout.addStretch(1)
        details_action_layout.addWidget(details_button)

        if details:
            table = QTableWidget(len(details), 4)
            table.setObjectName("ExecLogConsole")
            table.setHorizontalHeaderLabels(
                ["Emisor", "Receptor", "Ultimo mensaje enviado", "Hora"]
            )
            table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.NoSelection)
            table.setFocusPolicy(Qt.NoFocus)
            table.setAlternatingRowColors(True)
            table.verticalHeader().setVisible(False)
            table.horizontalHeader().setStretchLastSection(False)
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
            table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
            table.setMinimumHeight(300)
            table.setStyleSheet(
                """
                QTableWidget {
                    background-color: #0f172a;
                    alternate-background-color: #111827;
                    color: #e2e8f0;
                    gridline-color: #334155;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QHeaderView::section {
                    background-color: #1e293b;
                    color: #e2e8f0;
                    border: 1px solid #334155;
                    padding: 6px;
                    font-weight: 700;
                }
                QTableCornerButton::section {
                    background-color: #1e293b;
                    border: 1px solid #334155;
                }
                """
            )

            for index, row in enumerate(details):
                sender = _fmt_handle(row.get("account"))
                recipient = _fmt_handle(row.get("recipient_username"))
                message = _preview(row.get("last_message"))
                dt_value = row.get("timestamp")
                stamp = dt_value.strftime("%H:%M") if isinstance(dt_value, datetime) else "-"

                table.setItem(index, 0, QTableWidgetItem(sender))
                table.setItem(index, 1, QTableWidgetItem(recipient))
                table.setItem(index, 2, QTableWidgetItem(message))
                table.setItem(index, 3, QTableWidgetItem(stamp))
        else:
            table = QLabel("No hay respuestas registradas hoy.")
            table.setObjectName("MutedText")
            table.setAlignment(Qt.AlignCenter)
            table.setWordWrap(True)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(dialog.accept)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addLayout(details_action_layout)
        layout.addWidget(table)
        layout.addWidget(close_button, 0, Qt.AlignCenter)
        dialog.exec()

    def _show_top_responded_messages_dialog(self, parent: Optional[QWidget] = None) -> None:
        rows = list(self._top_responded_message_rows)
        if not rows:
            self._collect_dashboard_metrics()
            rows = list(self._top_responded_message_rows)

        dialog = QDialog(parent or self)
        dialog.setObjectName("LeadsSummaryDialog")
        dialog.setWindowTitle("Mensajes mas respondidos")
        dialog.setModal(True)
        dialog.setMinimumWidth(980)

        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("📈 Mensajes mas respondidos")
        title.setObjectName("LeadsSummaryTitle")
        title.setAlignment(Qt.AlignCenter)

        subtitle = QLabel(
            "Ranking por respuestas detectadas desde conversation_engine + conversation_state."
        )
        subtitle.setObjectName("MutedText")
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setWordWrap(True)

        if rows:
            rows_to_show = rows[:80]
            table = QTableWidget(len(rows_to_show), 5)
            table.setObjectName("ExecLogConsole")
            table.setHorizontalHeaderLabels(
                ["Tipo", "Mensaje", "Enviados", "Respondidos", "% respuesta"]
            )
            table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.NoSelection)
            table.setFocusPolicy(Qt.NoFocus)
            table.setAlternatingRowColors(True)
            table.verticalHeader().setVisible(False)
            table.horizontalHeader().setStretchLastSection(False)
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
            table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
            table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
            table.setMinimumHeight(360)
            table.setStyleSheet(
                """
                QTableWidget {
                    background-color: #0f172a;
                    alternate-background-color: #111827;
                    color: #e2e8f0;
                    gridline-color: #334155;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QHeaderView::section {
                    background-color: #1e293b;
                    color: #e2e8f0;
                    border: 1px solid #334155;
                    padding: 6px;
                    font-weight: 700;
                }
                QTableCornerButton::section {
                    background-color: #1e293b;
                    border: 1px solid #334155;
                }
                """
            )

            for index, row in enumerate(rows_to_show):
                role = str(row.get("role") or "-")
                text = " ".join(str(row.get("text") or "").split()) or "-"
                sent = int(row.get("sent") or 0)
                responded = int(row.get("responded") or 0)
                rate = float(row.get("response_rate") or 0.0)

                table.setItem(index, 0, QTableWidgetItem(role))
                table.setItem(index, 1, QTableWidgetItem(text))
                table.setItem(index, 2, QTableWidgetItem(str(sent)))
                table.setItem(index, 3, QTableWidgetItem(str(responded)))
                table.setItem(index, 4, QTableWidgetItem(f"{rate:.1f}%"))
        else:
            table = QLabel("No hay mensajes con respuesta registrados en memoria.")
            table.setObjectName("MutedText")
            table.setAlignment(Qt.AlignCenter)
            table.setWordWrap(True)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(dialog.accept)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addWidget(table)
        layout.addWidget(close_button, 0, Qt.AlignCenter)
        dialog.exec()

    def _split_conversation_key(self, raw_key: Any) -> tuple[str, str]:
        key_text = str(raw_key or "").strip()
        if "|" in key_text:
            account, thread_id = key_text.split("|", 1)
            return account.strip(), thread_id.strip()
        return key_text, key_text

    def _safe_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            parsed = float(value)
        except Exception:
            return None
        if parsed <= 0:
            return None
        return parsed

    def _normalize_message_text_key(self, value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    def _message_role_label(self, *, index: int, text: str, is_followup: bool) -> str:
        if is_followup:
            return "Follow-up"

        normalized = _normalized_label(text)
        agenda_tokens = (
            "agenda",
            "agendar",
            "agendamiento",
            "calendly",
            "calendar",
            "llamada",
            "reunion",
            "booking",
            "book",
            "cita",
        )
        if any(token in normalized for token in agenda_tokens):
            return "Agenda / CTA"
        if index == 0:
            return "Principal"
        if index == 1:
            return "Pitch"
        if index == 2:
            return "Agenda / CTA"
        return "Seguimiento"

    def _collect_replied_today_details_from_conversations(
        self,
        conversations: dict[str, Any],
        state_conversations: dict[str, Any],
        today: Any,
    ) -> list[dict[str, Any]]:
        state_by_key: dict[str, dict[str, Any]] = {}
        state_by_thread: dict[str, dict[str, Any]] = {}

        for state_key, payload in state_conversations.items():
            if not isinstance(payload, dict):
                continue
            state_account, state_thread = self._split_conversation_key(state_key)
            state_thread = str(state_thread or "").strip()
            state_key_norm = f"{_normalize_username(state_account)}|{state_thread}"
            state_by_key[state_key_norm] = payload
            if state_thread and state_thread not in state_by_thread:
                state_by_thread[state_thread] = payload

        by_thread: dict[str, dict[str, Any]] = {}
        for conv_key, conv_data in conversations.items():
            if not isinstance(conv_data, dict):
                continue

            account_from_key, thread_from_key = self._split_conversation_key(conv_key)
            account = str(conv_data.get("account") or account_from_key or "").strip()
            account_norm = _normalize_username(account)
            thread_id = str(conv_data.get("thread_id") or thread_from_key or "").strip()
            if not thread_id:
                continue

            state_key_norm = f"{account_norm}|{thread_id}"
            state_entry = state_by_key.get(state_key_norm) or state_by_thread.get(thread_id) or {}

            events: list[tuple[datetime, str]] = []
            raw_messages = conv_data.get("messages")
            if isinstance(raw_messages, list):
                for message in raw_messages:
                    if not isinstance(message, dict):
                        continue
                    direction = str(message.get("direction") or "").strip().lower()
                    if direction != "outbound":
                        continue
                    dt = self._message_datetime(message)
                    if not dt:
                        continue
                    events.append((dt, str(message.get("text") or "").strip()))

            raw_sent = conv_data.get("messages_sent")
            if isinstance(raw_sent, list):
                for sent in raw_sent:
                    if not isinstance(sent, dict):
                        continue
                    dt = None
                    for key in ("last_sent_at", "first_sent_at", "timestamp_epoch", "ts", "timestamp"):
                        dt = self._parse_epoch_datetime(sent.get(key))
                        if dt:
                            break
                    if not dt:
                        continue
                    events.append((dt, str(sent.get("text") or "").strip()))

            state_last_sent = self._parse_epoch_datetime(state_entry.get("last_sent_ts"))
            if state_last_sent:
                events.append((state_last_sent, ""))

            if not events:
                continue

            events.sort(key=lambda item: item[0], reverse=True)
            last_dt = events[0][0]
            if last_dt.date() != today:
                continue

            last_message = ""
            for _dt, text in events:
                if text:
                    last_message = text
                    break

            recipient = str(
                conv_data.get("recipient_username") or conv_data.get("title") or "sin_usuario"
            ).strip() or "sin_usuario"
            row_key = state_key_norm if account_norm else f"thread:{thread_id}"
            row_epoch = float(last_dt.timestamp())

            prev = by_thread.get(row_key)
            prev_epoch = float(prev.get("timestamp_epoch") or 0.0) if isinstance(prev, dict) else 0.0
            if prev and row_epoch <= prev_epoch:
                continue

            by_thread[row_key] = {
                "account": account or "-",
                "thread_id": thread_id,
                "recipient_username": recipient,
                "last_message": last_message,
                "timestamp": last_dt,
                "timestamp_epoch": row_epoch,
            }

        rows = list(by_thread.values())
        rows.sort(
            key=lambda row: float(row.get("timestamp_epoch") or 0.0),
            reverse=True,
        )
        return rows

    def _collect_top_responded_messages(
        self,
        conversations: dict[str, Any],
        state_conversations: dict[str, Any],
    ) -> list[dict[str, Any]]:
        state_by_key: dict[str, dict[str, Any]] = {}
        state_by_thread: dict[str, dict[str, Any]] = {}

        for state_key, payload in state_conversations.items():
            if not isinstance(payload, dict):
                continue
            state_account, state_thread = self._split_conversation_key(state_key)
            state_thread = str(state_thread or "").strip()
            state_key_norm = f"{_normalize_username(state_account)}|{state_thread}"
            state_by_key[state_key_norm] = payload
            if state_thread and state_thread not in state_by_thread:
                state_by_thread[state_thread] = payload

        grouped: dict[str, dict[str, Any]] = {}
        for conv_key, conv_data in conversations.items():
            if not isinstance(conv_data, dict):
                continue

            account_from_key, thread_from_key = self._split_conversation_key(conv_key)
            account_norm = _normalize_username(conv_data.get("account") or account_from_key)
            thread_id = str(conv_data.get("thread_id") or thread_from_key or "").strip()
            if not thread_id:
                continue

            state_key_norm = f"{account_norm}|{thread_id}"
            state_entry = state_by_key.get(state_key_norm) or state_by_thread.get(thread_id) or {}

            response_markers = [
                self._safe_float(conv_data.get("last_message_received_at")),
                self._safe_float(state_entry.get("ultimo_contacto_ts")),
            ]
            valid_markers = [value for value in response_markers if value is not None]
            response_ts = max(valid_markers) if valid_markers else None

            sent_rows: list[dict[str, Any]] = []
            raw_sent = conv_data.get("messages_sent")
            if isinstance(raw_sent, list):
                for sent in raw_sent:
                    if not isinstance(sent, dict):
                        continue
                    text = str(sent.get("text") or "").strip()
                    if not text:
                        continue

                    sent_ts: Optional[float] = None
                    for key in ("last_sent_at", "first_sent_at", "timestamp_epoch", "ts", "timestamp"):
                        candidate = self._safe_float(sent.get(key))
                        if candidate is not None:
                            sent_ts = candidate
                            break
                    if sent_ts is None:
                        continue

                    try:
                        times_sent = max(1, int(sent.get("times_sent") or 1))
                    except Exception:
                        times_sent = 1

                    sent_rows.append(
                        {
                            "text": text,
                            "sent_ts": sent_ts,
                            "times_sent": times_sent,
                            "is_followup": bool(sent.get("is_followup", False)),
                        }
                    )

            if not sent_rows:
                continue

            sent_rows.sort(key=lambda item: float(item.get("sent_ts") or 0.0))

            for idx, item in enumerate(sent_rows):
                role = self._message_role_label(
                    index=idx,
                    text=str(item.get("text") or ""),
                    is_followup=bool(item.get("is_followup", False)),
                )
                text_key = self._normalize_message_text_key(item.get("text"))
                if not text_key:
                    continue

                group_key = f"{role}|{text_key}"
                if group_key not in grouped:
                    grouped[group_key] = {
                        "role": role,
                        "text": str(item.get("text") or "").strip(),
                        "sent": 0,
                        "responded": 0,
                    }

                grouped[group_key]["sent"] += int(item.get("times_sent") or 1)

                next_ts = (
                    float(sent_rows[idx + 1].get("sent_ts") or 0.0)
                    if idx + 1 < len(sent_rows)
                    else None
                )
                sent_ts = float(item.get("sent_ts") or 0.0)
                if response_ts is not None and response_ts > sent_ts:
                    if next_ts is None or response_ts < next_ts:
                        grouped[group_key]["responded"] += 1

        rows: list[dict[str, Any]] = []
        for row in grouped.values():
            sent_total = int(row.get("sent") or 0)
            responded_total = int(row.get("responded") or 0)
            rate = (float(responded_total) / float(sent_total) * 100.0) if sent_total > 0 else 0.0
            rows.append(
                {
                    "role": row.get("role") or "-",
                    "text": row.get("text") or "-",
                    "sent": sent_total,
                    "responded": responded_total,
                    "response_rate": rate,
                }
            )

        responded_rows = [row for row in rows if int(row.get("responded") or 0) > 0]
        if responded_rows:
            rows = responded_rows

        rows.sort(
            key=lambda row: (
                int(row.get("responded") or 0),
                float(row.get("response_rate") or 0.0),
                int(row.get("sent") or 0),
            ),
            reverse=True,
        )
        return rows

    def _collect_calendly_sent_today(self) -> list[dict[str, Any]]:
        conversation = self._read_json(
            self._root_dir / "storage" / "conversation_engine.json", {}
        )
        if not conversation:
            conversation = self._read_json(self._root_dir / "conversation_engine.json", {})

        state = self._read_json(self._root_dir / "storage" / "state.json", {})
        today = datetime.now().astimezone().date()

        conversations = {}
        if isinstance(conversation, dict):
            raw = conversation.get("conversations")
            if isinstance(raw, dict):
                conversations = raw

        return self._collect_booked_today_details_from_conversations(
            conversations=conversations,
            today=today,
            account_alias_map=self._build_account_alias_map(state),
            calendly_match=None,
        )

    def _build_account_alias_map(self, state: dict[str, Any]) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        for record in self._load_account_records(state):
            if not isinstance(record, dict):
                continue
            username = _normalize_username(record.get("username") or record.get("account"))
            if not username:
                continue
            alias = str(record.get("alias") or "").strip()
            if alias:
                alias_map[username] = alias
        return alias_map

    def _resolve_calendly_match_values(self) -> list[str]:
        values: list[str] = [
            "calendly.com/",
            "cal.com/",
            "calendar.google.com/",
            "acuityscheduling.com/",
            "simplybook.me/",
            "youcanbook.me/",
            "app.gohighlevel.com/",
            "/calendar/",
            "/agendar",
            "/agendamiento",
        ]
        state = self._read_json(self._root_dir / "storage" / "state.json", {})
        if isinstance(state, dict):
            for key in (
                "calendly_url",
                "calendar_url",
                "booking_url",
                "agenda_url",
                "agendamiento_url",
                "meeting_url",
            ):
                value = state.get(key)
                if isinstance(value, str) and value.strip():
                    lowered = value.strip().lower()
                    values.append(lowered)
                    match = re.search(r"https?://([^/\s]+)", lowered)
                    if match:
                        values.append(match.group(1))
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value or "").strip().lower()
            if not text or text in seen:
                continue
            seen.add(text)
            normalized.append(text)
        return normalized

    def _collect_booked_today_details_from_conversations(
        self,
        conversations: dict[str, Any],
        today: Any,
        account_alias_map: Optional[dict[str, str]] = None,
        calendly_match: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        by_username: dict[str, dict[str, Any]] = {}
        alias_map = account_alias_map or {}
        booking_substrings: list[str] = []
        if isinstance(calendly_match, str) and calendly_match.strip():
            booking_substrings.append(calendly_match.strip().lower())
        else:
            booking_substrings.extend(self._resolve_calendly_match_values())
        booking_keywords = (
            "agend",
            "agenda",
            "agendar",
            "agendamiento",
            "cita",
            "reunion",
            "llamada",
            "booking",
            "book",
            "calendar",
            "calendly",
        )

        def _is_calendly_message(text: Any) -> bool:
            if not text:
                return False
            value = str(text).strip().lower()
            if not value:
                return False
            if any(sub in value for sub in booking_substrings):
                return True
            has_url = bool(re.search(r"(https?://\S+|www\.\S+)", value))
            if has_url and any(keyword in value for keyword in booking_keywords):
                return True
            return False

        def _message_sent_datetime(payload: dict[str, Any]) -> Optional[datetime]:
            if not isinstance(payload, dict):
                return None
            for key in ("last_sent_at", "first_sent_at", "timestamp_epoch", "ts", "timestamp"):
                parsed = self._parse_epoch_datetime(payload.get(key))
                if parsed:
                    return parsed
            iso = payload.get("iso") or payload.get("started_at")
            if isinstance(iso, str):
                return self._parse_iso_datetime(iso)
            return None

        def _upsert_row(
            *,
            account_text: str,
            thread_id: str,
            thread_href: str,
            recipient_username: str,
            text: Any,
            dt: datetime,
        ) -> None:
            username_norm = _normalize_username(recipient_username) or f"thread:{thread_id}"
            current_epoch = float(dt.timestamp())
            previous = by_username.get(username_norm)
            previous_ts = previous.get("timestamp_epoch") if isinstance(previous, dict) else None
            try:
                previous_epoch = float(previous_ts) if previous_ts is not None else None
            except Exception:
                previous_epoch = None
            if previous_epoch is not None and current_epoch <= previous_epoch:
                return

            account_norm = _normalize_username(account_text)
            alias = alias_map.get(account_norm) or "(desconocido)"

            by_username[username_norm] = {
                "account": account_text or "-",
                "alias": alias,
                "thread_id": thread_id,
                "thread_href": thread_href,
                "recipient_username": recipient_username,
                "direction": "outbound",
                "text": str(text or "").strip(),
                "timestamp": dt,
                "timestamp_epoch": current_epoch,
            }

        for conv_key, conv_data in conversations.items():
            if not isinstance(conv_data, dict):
                continue

            conv_key_text = str(conv_key or "")
            key_account = ""
            key_thread = conv_key_text
            if "|" in conv_key_text:
                key_account, key_thread = conv_key_text.split("|", 1)

            account = _normalize_username(conv_data.get("account")) or _normalize_username(
                key_account
            )
            thread_id = str(conv_data.get("thread_id") or key_thread or "").strip()
            if not thread_id:
                continue

            recipient_username = str(
                conv_data.get("recipient_username") or conv_data.get("title") or "sin_usuario"
            ).strip() or "sin_usuario"
            thread_href = str(conv_data.get("thread_href") or "").strip()

            messages_sent = conv_data.get("messages_sent")
            if isinstance(messages_sent, list):
                for message in messages_sent:
                    if not isinstance(message, dict):
                        continue
                    text = message.get("text")
                    if not _is_calendly_message(text):
                        continue
                    dt = _message_sent_datetime(message)
                    if not dt or dt.date() != today:
                        continue
                    _upsert_row(
                        account_text=account or "-",
                        thread_id=thread_id,
                        thread_href=thread_href,
                        recipient_username=recipient_username,
                        text=text,
                        dt=dt,
                    )

            messages = conv_data.get("messages")
            if not isinstance(messages, list):
                continue

            for message in messages:
                if not isinstance(message, dict):
                    continue
                direction = str(message.get("direction") or "").strip().lower()
                if direction != "outbound":
                    continue
                dt = self._message_datetime(message)
                if not dt or dt.date() != today:
                    continue
                text = message.get("text")
                if not _is_calendly_message(text):
                    continue
                _upsert_row(
                    account_text=account or "-",
                    thread_id=thread_id,
                    thread_href=thread_href,
                    recipient_username=recipient_username,
                    text=text,
                    dt=dt,
                )

        rows = list(by_username.values())
        rows.sort(
            key=lambda row: float(row.get("timestamp_epoch") or 0.0),
            reverse=True,
        )
        return rows

    def _collect_dashboard_metrics(self) -> dict[str, int | str]:
        state = self._read_json(self._root_dir / "storage" / "state.json", {})
        sent_entries = self._read_jsonl(self._root_dir / "storage" / "sent_log.jsonl")

        conversation = self._read_json(
            self._root_dir / "storage" / "conversation_engine.json", {}
        )
        if not conversation:
            conversation = self._read_json(self._root_dir / "conversation_engine.json", {})
        conversation_state = self._read_json(
            self._root_dir / "storage" / "conversation_state.json", {}
        )
        if not conversation_state:
            conversation_state = self._read_json(self._root_dir / "conversation_state.json", {})

        today = datetime.now().astimezone().date()

        sent_today = self._state_counter(state, "daily_sent")
        error_today = self._state_counter(state, "daily_errors")
        use_state_daily = sent_today is not None or error_today is not None
        sent_today = sent_today or 0
        error_today = error_today or 0
        sent_accounts: set[str] = set()

        for entry in sent_entries:
            account = _normalize_username(entry.get("account"))
            if account:
                sent_accounts.add(account)

            dt = self._entry_datetime(entry)
            if not dt or dt.date() != today:
                continue

            if not use_state_daily:
                if bool(entry.get("ok")):
                    sent_today += 1
                else:
                    error_today += 1

        conversation_accounts: set[str] = set()
        conversations = {}
        if isinstance(conversation, dict):
            raw = conversation.get("conversations")
            if isinstance(raw, dict):
                conversations = raw
        state_conversations = {}
        if isinstance(conversation_state, dict):
            raw_state = conversation_state.get("conversations")
            if isinstance(raw_state, dict):
                state_conversations = raw_state
        alias_map = self._build_account_alias_map(state)
        booked_today_rows = self._collect_booked_today_details_from_conversations(
            conversations=conversations,
            today=today,
            account_alias_map=alias_map,
            calendly_match=None,
        )
        self._booked_today_rows = booked_today_rows
        replied_today_rows = self._collect_replied_today_details_from_conversations(
            conversations=conversations,
            state_conversations=state_conversations,
            today=today,
        )
        self._replied_today_rows = replied_today_rows
        self._top_responded_message_rows = self._collect_top_responded_messages(
            conversations=conversations,
            state_conversations=state_conversations,
        )

        for conv_data in conversations.values():
            if not isinstance(conv_data, dict):
                continue
            account = _normalize_username(conv_data.get("account"))
            if account:
                conversation_accounts.add(account)

        account_records = self._load_account_records(state)
        if account_records:
            total_accounts = len(account_records)
            active_accounts = sum(1 for record in account_records if bool(record.get("active", True)))
            connected_accounts = self._count_connected_accounts(account_records)
        else:
            derived = set(sent_accounts) | set(conversation_accounts)
            total_accounts = len(derived)
            active_accounts = total_accounts
            connected_accounts = active_accounts

        if connected_accounts > total_accounts:
            connected_accounts = total_accounts

        return {
            "total_accounts": total_accounts,
            "active_accounts": active_accounts,
            "connected_accounts": connected_accounts,
            "messages_sent_today": sent_today,
            "messages_error_today": error_today,
            "messages_replied_today": len(replied_today_rows),
            "booked_today": len(booked_today_rows),
            "last_refresh": self._dashboard_updated_value.text() or "-",
        }

    def _state_counter(self, state: dict[str, Any], key: str) -> Optional[int]:
        if not isinstance(state, dict):
            return None
        value = state.get(key)
        if value in (None, ""):
            return None
        try:
            parsed = int(value)
        except Exception:
            return None
        return max(0, parsed)

    def _resolve_accounts_api(self) -> tuple[Optional[Callable[[], Any]], Optional[Callable[..., Any]]]:
        if self._accounts_lookup_failed:
            return None, None
        if self._accounts_list_callable:
            return self._accounts_list_callable, self._accounts_connected_callable

        try:
            module = import_module("accounts")
        except Exception:
            self._accounts_lookup_failed = True
            return None, None

        list_callable = getattr(module, "list_all", None)
        if not callable(list_callable):
            self._accounts_lookup_failed = True
            return None, None

        connected_callable = getattr(module, "connected_status", None)
        self._accounts_list_callable = list_callable
        self._accounts_connected_callable = (
            connected_callable if callable(connected_callable) else None
        )
        return self._accounts_list_callable, self._accounts_connected_callable

    def _count_connected_accounts(self, records: list[dict[str, Any]]) -> int:
        _, connected_callable = self._resolve_accounts_api()
        if callable(connected_callable):
            try:
                return sum(
                    1
                    for record in records
                    if bool(
                        connected_callable(
                            record,
                            strict=False,
                            reason="dashboard-count",
                            fast=True,
                            persist=False,
                        )
                    )
                )
            except Exception:
                pass
        return sum(1 for record in records if bool(record.get("connected", False)))

    def _load_account_records(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        list_callable, _ = self._resolve_accounts_api()
        if callable(list_callable):
            try:
                payload = list_callable()
            except Exception:
                payload = None
            if isinstance(payload, list):
                records = [item for item in payload if isinstance(item, dict)]
                if records:
                    return records

        data_accounts = self._read_json(self._root_dir / "data" / "accounts.json", [])
        if isinstance(data_accounts, list):
            records = [item for item in data_accounts if isinstance(item, dict)]
            if records:
                return records

        records: list[dict[str, Any]] = []
        if isinstance(state, dict):
            state_accounts = state.get("accounts")
            if isinstance(state_accounts, list):
                for item in state_accounts:
                    if isinstance(item, dict):
                        records.append(item)
        return records

    def _read_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        items: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        items.append(payload)
        except Exception:
            return []
        return items

    def _entry_datetime(self, entry: dict[str, Any]) -> Optional[datetime]:
        started_at = entry.get("started_at")
        if isinstance(started_at, str) and started_at.strip():
            parsed = self._parse_iso_datetime(started_at)
            if parsed:
                return parsed

        ts = entry.get("ts")
        parsed_ts = self._parse_epoch_datetime(ts)
        if parsed_ts:
            return parsed_ts
        return None

    def _message_datetime(self, message: dict[str, Any]) -> Optional[datetime]:
        for key in ("timestamp_epoch", "ts", "timestamp"):
            parsed = self._parse_epoch_datetime(message.get(key))
            if parsed:
                return parsed
        iso = message.get("started_at")
        if isinstance(iso, str):
            return self._parse_iso_datetime(iso)
        return None

    def _parse_epoch_datetime(self, value: Any) -> Optional[datetime]:
        if value in (None, ""):
            return None
        try:
            seconds = float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone()
        except Exception:
            return None

    def _parse_iso_datetime(self, value: str) -> Optional[datetime]:
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc).astimezone()
        return parsed.astimezone()

    def _looks_like_appointment(self, text: Any) -> bool:
        if not text:
            return False
        value = str(text).lower()
        keywords = (
            "appointment",
            "booked",
            "booking",
            "meeting",
            "calendar",
            "cita",
            "agend",
            "reunion",
            "turno",
        )
        return any(keyword in value for keyword in keywords)

    def _request_backend_stop(self) -> None:
        if self._io_adapter:
            self._io_adapter.shutdown()
        try:
            from runtime import request_stop

            request_stop("GUI closed by user")
        except Exception:
            pass

    def _join_backend_thread(self, timeout_seconds: float) -> bool:
        thread = self._backend_thread
        if not thread:
            return True
        if not thread.is_alive():
            return True

        deadline = time.monotonic() + timeout_seconds
        while thread.is_alive() and time.monotonic() < deadline:
            thread.join(timeout=0.2)
            QApplication.processEvents()
        return not thread.is_alive()

    def _stop_ui_timers(self) -> None:
        if hasattr(self, "_dashboard_timer") and self._dashboard_timer.isActive():
            self._dashboard_timer.stop()
        if hasattr(self, "_exec_clock_timer") and self._exec_clock_timer.isActive():
            self._exec_clock_timer.stop()

    def _shutdown_playwright_runtimes(self) -> None:
        try:
            whatsapp = import_module("whatsapp")
        except Exception as exc:
            self._append_log(f"[gui] Playwright runtime import failed during shutdown: {exc}\n")
            return

        runner = getattr(whatsapp, "_MESSAGE_RUNNER", None)
        if runner is not None and hasattr(runner, "stop"):
            try:
                runner.stop()
            except Exception as exc:
                self._append_log(f"[gui] Failed stopping WhatsApp runner: {exc}\n")

        shutdown_runtime = getattr(whatsapp, "_shutdown_playwright_runtime", None)
        if callable(shutdown_runtime):
            try:
                shutdown_runtime()
            except Exception as exc:
                self._append_log(f"[gui] Failed stopping Playwright runtime: {exc}\n")

    def _terminate_active_subprocesses(self) -> None:
        active = getattr(subprocess, "_active", None)
        if not isinstance(active, list) or not active:
            return

        for proc in list(active):
            try:
                if proc.poll() is not None:
                    continue
                self._append_log(f"[gui] Stopping child process pid={proc.pid}\n")
                proc.terminate()
                proc.wait(timeout=2.0)
                if proc.poll() is None:
                    self._append_log(f"[gui] Forcing child process kill pid={proc.pid}\n")
                    proc.kill()
                    proc.wait(timeout=1.0)
            except Exception as exc:
                self._append_log(f"[gui] Failed stopping child process: {exc}\n")

    def _flush_logging_streams(self) -> None:
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception as exc:
                self._append_log(f"[gui] Failed flushing stream: {exc}\n")
        logging.shutdown()

    def shutdown_application(self, *, reason: str) -> bool:
        if self._shutdown_started:
            return True

        self._shutdown_started = True
        self._closing = True
        self._shutdown_reason = reason
        self._set_status("Stopping")
        self._append_log(f"[gui] shutdown requested ({reason}).\n")

        # 1) Señal de stop a threads y bucles de input.
        self._request_backend_stop()
        # 2) Stop de event loops/timers UI.
        self._stop_ui_timers()
        # 3) Cierre de runtimes/contextos Playwright conocidos.
        self._shutdown_playwright_runtimes()
        # 4) Terminación de subprocess activos conocidos.
        self._terminate_active_subprocesses()

        joined = self._join_backend_thread(timeout_seconds=10.0)
        if not joined:
            self._append_log("[gui] Backend thread did not stop before timeout.\n")

        # 5) Flush/close de logging streams.
        self._flush_logging_streams()
        # 6) Cierre GUI.
        self.hide()
        app = QApplication.instance()
        if app is not None:
            app.exit(0)

        if self.backend_exit_code is None:
            self.backend_exit_code = 0
        return joined

    def dragEnterEvent(self, event) -> None:  # type: ignore[override]
        mime = event.mimeData()
        if mime and mime.hasUrls():
            urls = mime.urls()
            for url in urls:
                if url.isLocalFile():
                    event.acceptProposedAction()
                    return
        event.ignore()

    def dropEvent(self, event) -> None:  # type: ignore[override]
        mime = event.mimeData()
        if not mime or not mime.hasUrls():
            event.ignore()
            return

        path_text = ""
        for url in mime.urls():
            if not url.isLocalFile():
                continue
            local_file = url.toLocalFile()
            if not local_file:
                continue
            path_text = str(Path(local_file).resolve())
            break

        if not path_text:
            event.ignore()
            return

        self._live_input_field.setText(path_text)
        self._submit_live_input()
        event.acceptProposedAction()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self.shutdown_application(reason="window-close")
        event.accept()
        super().closeEvent(event)


