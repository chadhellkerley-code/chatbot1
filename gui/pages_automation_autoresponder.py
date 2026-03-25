from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QStackedWidget,
    QTableWidget,
)

from gui.automation_dialogs import AutomationMessageDialog
from gui.query_runner import QueryError

from .automation_pages_base import AutomationSectionPage, format_run_duration
from .page_base import PageContext, safe_int, table_item
from .snapshot_queries import build_automation_autoresponder_snapshot


class AutomationAutoresponderPage(AutomationSectionPage):
    _RUNTIME_EVENT_PREFIX = "AR_EVENT "
    _STATUS_LABELS = {
        "starting": "Iniciando",
        "running": "En curso",
        "stopping": "Deteniendo",
        "stopped": "Finalizado",
        "idle": "Inactivo",
        "failed": "Fallido",
    }

    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Autoresponder",
            "Wrapper temporal del runtime operativo. El control principal ahora vive en Inbox.",
            route_key="automation_autoresponder_page",
            parent=parent,
        )
        self._start_button: QPushButton | None = None
        self._stop_button: QPushButton | None = None
        self._stack = QStackedWidget()
        self.content_layout().addWidget(self._stack, 1)

        self._config_view = self._build_config_view()
        self._monitor_view = self._build_monitor_view()
        self._stack.addWidget(self._config_view)
        self._stack.addWidget(self._monitor_view)

        self._ctx.logs.logAdded.connect(self._on_log_added)
        self._ctx.logs.cleared.connect(self._on_logs_cleared)
        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)
        self._timer = QTimer(self)
        self._timer.setInterval(500)
        self._timer.timeout.connect(self.refresh_snapshot)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._capturing_logs = False
        self._last_summary_run_id = ""
        self._start_pending = False
        self._pending_run_id = ""
        self._log_buffer = ""

    def _build_config_view(self):
        from PySide6.QtWidgets import QWidget, QVBoxLayout

        widget = QWidget()
        root = QVBoxLayout(widget)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)
        panel, layout = self.create_panel(
            "Configuracion de autoresponder",
            "Vista heredada del runtime. El inicio y la detencion reales ahora se hacen solo desde Inbox.",
        )
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        self._alias_combo = QComboBox()
        self._alias_combo.currentIndexChanged.connect(self.refresh_snapshot)
        self._delay_min = QSpinBox()
        self._delay_min.setRange(1, 3600)
        self._delay_min.setValue(45)
        self._delay_max = QSpinBox()
        self._delay_max.setRange(1, 3600)
        self._delay_max.setValue(76)
        self._concurrency = QSpinBox()
        self._concurrency.setRange(1, 20)
        self._threads = QSpinBox()
        self._threads.setRange(1, 200)
        self._threads.setValue(20)
        self._followup_only = QCheckBox("Solo follow-up")
        self._capacity_hint = QLabel("")
        self._capacity_hint.setObjectName("SectionPanelHint")
        self._capacity_hint.setWordWrap(True)
        self._flow_schedule = QLabel("-")
        self._flow_schedule.setObjectName("SectionPanelHint")
        grid.addWidget(QLabel("Alias"), 0, 0)
        grid.addWidget(self._alias_combo, 0, 1)
        grid.addWidget(QLabel("Delay minimo"), 1, 0)
        grid.addWidget(self._delay_min, 1, 1)
        grid.addWidget(QLabel("Delay maximo"), 1, 2)
        grid.addWidget(self._delay_max, 1, 3)
        grid.addWidget(QLabel("Concurrencia"), 2, 0)
        grid.addWidget(self._concurrency, 2, 1)
        grid.addWidget(QLabel("Threads / lote"), 2, 2)
        grid.addWidget(self._threads, 2, 3)
        grid.addWidget(QLabel("Modo"), 3, 0)
        grid.addWidget(self._followup_only, 3, 1)
        grid.addWidget(QLabel("Programacion follow-up"), 4, 0)
        grid.addWidget(self._flow_schedule, 4, 1, 1, 3)
        layout.addLayout(grid)
        layout.addWidget(self._capacity_hint)

        self._accounts_preview = QTableWidget(0, 4)
        self._accounts_preview.setHorizontalHeaderLabels(["Cuenta", "Proxy", "Conectada", "Estado"])
        self._accounts_preview.verticalHeader().setVisible(False)
        self._accounts_preview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._accounts_preview.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._accounts_preview)
        self._start_button = QPushButton("Abrir Inbox")
        self._start_button.setObjectName("PrimaryButton")
        self._start_button.clicked.connect(self._start)
        layout.addWidget(self._start_button, 0, Qt.AlignLeft)
        root.addWidget(panel)
        return widget

    def _build_monitor_view(self):
        from PySide6.QtWidgets import QWidget, QVBoxLayout

        widget = QWidget()
        root = QVBoxLayout(widget)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)
        panel, layout = self.create_panel(
            "Monitor de autoresponder",
            "Sigue el run actual, sus cuentas activas y el log en tiempo real.",
        )
        self._run_summary = QLabel("")
        self._run_summary.setObjectName("SectionPanelHint")
        self._run_summary.setWordWrap(True)
        layout.addWidget(self._run_summary)

        metrics = QGridLayout()
        metrics.setContentsMargins(0, 0, 0, 0)
        metrics.setHorizontalSpacing(10)
        metrics.setVerticalSpacing(8)
        self._metric_labels = {
            "respond_ok": QLabel("0"),
            "respond_fail": QLabel("0"),
            "follow_ok": QLabel("0"),
            "follow_fail": QLabel("0"),
            "agendas": QLabel("0"),
            "accounts": QLabel("0"),
        }
        rows = [
            ("Respondidos OK", "respond_ok"),
            ("Respondidos fallidos", "respond_fail"),
            ("Follow-ups OK", "follow_ok"),
            ("Follow-ups fallidos", "follow_fail"),
            ("Agendas generadas", "agendas"),
            ("Cuentas utilizadas", "accounts"),
        ]
        for index, (label, key) in enumerate(rows):
            metrics.addWidget(QLabel(label), index // 2, (index % 2) * 2)
            metrics.addWidget(self._metric_labels[key], index // 2, (index % 2) * 2 + 1)
        layout.addLayout(metrics)

        self._active_accounts = QTableWidget(0, 3)
        self._active_accounts.setHorizontalHeaderLabels(["Cuenta", "Proxy", "Estado"])
        self._active_accounts.verticalHeader().setVisible(False)
        self._active_accounts.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._active_accounts.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._active_accounts)

        self._log = QPlainTextEdit()
        self._log.setObjectName("LogConsole")
        self._log.setReadOnly(True)
        self._log.document().setMaximumBlockCount(4000)
        layout.addWidget(self._log, 1)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        self._stop_button = QPushButton("Abrir Inbox")
        self._stop_button.setObjectName("SecondaryButton")
        self._stop_button.clicked.connect(self._stop)
        refresh_button = QPushButton("Recargar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_snapshot)
        actions.addWidget(self._stop_button)
        actions.addWidget(refresh_button)
        actions.addStretch(1)
        layout.addLayout(actions)
        root.addWidget(panel)
        return widget

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        aliases = payload.get("aliases") if isinstance(payload.get("aliases"), list) else []
        alias = str(payload.get("selected_alias") or self._ctx.state.active_alias).strip()
        self._alias_combo.blockSignals(True)
        self._alias_combo.clear()
        for value in aliases:
            self._alias_combo.addItem(str(value), str(value))
        self._alias_combo.setCurrentIndex(max(0, self._alias_combo.findData(alias)))
        self._alias_combo.blockSignals(False)

        max_concurrency = max(1, safe_int(payload.get("max_concurrency"), 1))
        self._concurrency.setMaximum(max_concurrency)
        if self._concurrency.value() > max_concurrency:
            self._concurrency.setValue(max_concurrency)

        alias_accounts = payload.get("alias_accounts") if isinstance(payload.get("alias_accounts"), list) else []
        ready_alias_accounts = [row for row in alias_accounts if isinstance(row, dict) and not bool(row.get("blocked"))]
        blocked_alias_accounts = [row for row in alias_accounts if isinstance(row, dict) and bool(row.get("blocked"))]
        capacity_text = (
            "No hay cuentas utilizables para iniciar en este alias."
            if not ready_alias_accounts
            else (
                "El alias no tiene proxies activos; la concurrencia queda limitada a 1."
                if max_concurrency == 1
                else f"Concurrencia maxima sugerida por proxies: {max_concurrency}."
            )
        )
        blocked_summary = self._blocked_accounts_summary(blocked_alias_accounts)
        if blocked_summary:
            capacity_text = f"{capacity_text}  {blocked_summary}".strip()
        self._capacity_hint.setText(capacity_text)
        if self._start_button is not None:
            self._start_button.setEnabled(bool(alias))

        self._accounts_preview.setRowCount(len(alias_accounts))
        for row_index, row in enumerate(alias_accounts):
            if not isinstance(row, dict):
                continue
            self._accounts_preview.setItem(row_index, 0, table_item(row.get("username", "")))
            self._accounts_preview.setItem(row_index, 1, table_item(row.get("proxy", "") or "Sin proxy"))
            self._accounts_preview.setItem(row_index, 2, table_item("Si" if bool(row.get("connected")) else "No"))
            self._accounts_preview.setItem(row_index, 3, table_item(self._proxy_state_text(row, active_label="Lista")))

        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        if not self._capturing_logs:
            self._flow_schedule.setText(str(snapshot.get("followup_schedule_label") or "-"))
            self._delay_min.setValue(max(1, safe_int(snapshot.get("delay_min"), 45)))
            self._delay_max.setValue(max(self._delay_min.value(), safe_int(snapshot.get("delay_max"), 76)))
            self._threads.setValue(max(1, safe_int(snapshot.get("threads"), 20)))
            self._followup_only.setChecked(bool(snapshot.get("followup_only")))
            self._concurrency.setValue(max(1, min(max_concurrency, safe_int(snapshot.get("concurrency"), 1))))

        task_active = bool(payload.get("task_active")) or bool(snapshot.get("task_active"))
        run_id = str(snapshot.get("run_id") or "")
        self._run_summary.setText(
            f"Ejecucion: {run_id or '-'}  |  Alias: {alias or '-'}  |  Estado: {self._display_status(snapshot.get('status'))}  |  "
            f"Inicio: {str(snapshot.get('started_at') or '-')}  |  Mensaje: {str(snapshot.get('message') or '-').strip() or '-'}"
        )
        self._metric_labels["respond_ok"].setText(str(safe_int(snapshot.get("message_success"))))
        self._metric_labels["respond_fail"].setText(str(safe_int(snapshot.get("message_failed"))))
        self._metric_labels["follow_ok"].setText(str(safe_int(snapshot.get("followup_success"))))
        self._metric_labels["follow_fail"].setText(str(safe_int(snapshot.get("followup_failed"))))
        self._metric_labels["agendas"].setText(str(safe_int(snapshot.get("agendas_generated"))))
        self._metric_labels["accounts"].setText(
            str(safe_int(snapshot.get("accounts_active"), safe_int(snapshot.get("accounts_total"))))
        )

        account_rows = snapshot.get("account_rows") if isinstance(snapshot.get("account_rows"), list) else []
        self._active_accounts.setRowCount(len(account_rows))
        for row_index, row in enumerate(account_rows):
            if not isinstance(row, dict):
                continue
            self._active_accounts.setItem(row_index, 0, table_item(row.get("account", "")))
            self._active_accounts.setItem(row_index, 1, table_item(row.get("proxy", "") or "Sin proxy"))
            self._active_accounts.setItem(row_index, 2, table_item(self._proxy_state_text(row, active_label="Activa")))

        if task_active:
            if self._start_pending and (not self._pending_run_id or run_id == self._pending_run_id):
                self._start_pending = False
            self._stack.setCurrentWidget(self._monitor_view)
            self._capturing_logs = True
            if not self._timer.isActive():
                self._timer.start()
        elif self._capturing_logs:
            if self._start_pending:
                self._stack.setCurrentWidget(self._monitor_view)
                if not self._timer.isActive():
                    self._timer.start()
                self.clear_status()
                return
            self._flush_log_buffer()
            self._capturing_logs = False
            self._timer.stop()
            self._maybe_show_summary(snapshot)
            self._stack.setCurrentWidget(self._config_view)
        self.clear_status()

    @staticmethod
    def _proxy_state_text(row: dict[str, Any], *, active_label: str = "OK") -> str:
        if not isinstance(row, dict):
            return active_label
        if not bool(row.get("blocked")):
            return active_label
        message = str(row.get("safety_message") or row.get("blocked_reason") or "Cuenta bloqueada").strip() or "Cuenta bloqueada"
        try:
            remaining_seconds = float(
                row.get("blocked_remaining_seconds")
                or row.get("remaining_seconds")
                or 0.0
            )
        except Exception:
            remaining_seconds = 0.0
        if remaining_seconds > 0:
            return f"{message} ({AutomationAutoresponderPage._format_remaining_seconds(remaining_seconds)})"
        return message

    @staticmethod
    def _format_remaining_seconds(value: float) -> str:
        total = max(0, int(round(float(value or 0.0))))
        if total < 60:
            return f"{total}s"
        minutes, seconds = divmod(total, 60)
        if minutes < 60:
            return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"
        hours, minutes = divmod(minutes, 60)
        if hours < 24:
            return f"{hours}h" if minutes == 0 else f"{hours}h {minutes}m"
        days, hours = divmod(hours, 24)
        return f"{days}d" if hours == 0 else f"{days}d {hours}h"

    @classmethod
    def _blocked_accounts_summary(cls, rows: list[dict[str, Any]]) -> str:
        blocked_rows = [row for row in rows if isinstance(row, dict) and bool(row.get("blocked"))]
        if not blocked_rows:
            return ""
        reasons: list[str] = []
        seen_reasons: set[str] = set()
        for row in blocked_rows:
            reason = str(row.get("safety_message") or row.get("blocked_reason") or "Cuenta bloqueada").strip() or "Cuenta bloqueada"
            key = reason.lower()
            if key in seen_reasons:
                continue
            seen_reasons.add(key)
            reasons.append(reason)
        preview = ", ".join(reasons[:3])
        if len(reasons) > 3:
            preview = f"{preview}, +{len(reasons) - 3} mas"
        count = len(blocked_rows)
        noun = "cuenta bloqueada" if count == 1 else "cuentas bloqueadas"
        return f"{count} {noun} por seguridad: {preview}."

    def _start(self) -> None:
        self._open_inbox_runtime()

    def _stop(self) -> None:
        try:
            self._open_inbox_runtime()
        except Exception as exc:
            self.show_exception(exc, "No se pudo abrir Inbox.")
            return

    def _open_inbox_runtime(self) -> None:
        alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        if alias:
            self._ctx.state.active_alias = alias
        self._ctx.open_route("inbox_page", {"source": "automation_autoresponder", "alias_id": alias} if alias else None)
        self.set_status("El runtime operativo se administra desde Inbox.")

    def _on_logs_cleared(self) -> None:
        self._log_buffer = ""
        if self._capturing_logs:
            self._log.clear()

    def _append_log_text(self, text: str) -> None:
        chunk = str(text or "")
        if not chunk:
            return
        try:
            cursor = self._log.textCursor()
            cursor.movePosition(QTextCursor.End)
            self._log.setTextCursor(cursor)
            self._log.insertPlainText(chunk)
            cursor = self._log.textCursor()
            cursor.movePosition(QTextCursor.End)
            self._log.setTextCursor(cursor)
            self._log.ensureCursorVisible()
        except Exception:
            return

    def _append_log_lines(self, lines: list[str]) -> None:
        if not lines:
            return
        clean_lines = [str(line or "").rstrip() for line in lines if str(line or "").strip()]
        if clean_lines:
            self._append_log_text("".join(f"{line}\n" for line in clean_lines))

    def _flush_log_buffer(self) -> None:
        remainder = str(self._log_buffer or "").strip()
        self._log_buffer = ""
        if remainder:
            self._append_log_lines([remainder])

    def _format_runtime_event(self, payload: dict[str, Any]) -> str:
        event_name = str(payload.get("event") or "").strip().upper()
        account = str(payload.get("account") or "").strip()
        outcome = str(payload.get("outcome") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        if event_name == "START":
            alias = str(payload.get("alias") or self._alias_combo.currentData() or "").strip() or "-"
            return f"Inicio de autoresponder para alias {alias}."
        if event_name == "PROGRESS" and account:
            message = f"@{account}"
            if outcome:
                message = f"{message} {outcome}"
            if reason:
                message = f"{message} ({reason})"
            return message
        if event_name == "THREADS_DISCOVERED" and account:
            discovered = max(0, safe_int(payload.get("discovered"), 0))
            source = str(payload.get("source") or "").strip()
            suffix = f" ({source})" if source else ""
            return f"@{account} threads discovered={discovered}{suffix}"
        if event_name == "PACK_SELECTED" and account:
            pack_type = str(payload.get("pack_type") or "").strip() or "pack"
            recipient = str(payload.get("recipient") or "").strip()
            suffix = f" -> @{recipient}" if recipient else ""
            return f"@{account} pack selected {pack_type}{suffix}"
        if event_name == "MESSAGE_SENT" and account:
            recipient = str(payload.get("recipient") or "").strip()
            suffix = f" -> @{recipient}" if recipient else ""
            return f"@{account} message sent{suffix}"
        if event_name == "FOLLOWUP_SENT" and account:
            recipient = str(payload.get("recipient") or "").strip()
            suffix = f" -> @{recipient}" if recipient else ""
            return f"@{account} follow-up sent{suffix}"
        if event_name == "STOP":
            return "Stop solicitado para autoresponder."
        return ""

    def _should_display_log_line(self, line: str) -> bool:
        stripped = str(line or "").strip()
        if not stripped:
            return False
        if stripped.startswith(("Traceback", "[DEBUG]", "[INFO]", "[WARNING]", "[ERROR]", "[CRITICAL]")):
            return True
        if line.startswith("  File "):
            return True
        return False

    def _display_status(self, value: object) -> str:
        status = str(value or "").strip()
        if not status:
            return "-"
        return self._STATUS_LABELS.get(status.lower(), status)

    def _consume_runtime_events(self, chunk: str) -> str:
        visible_lines: list[str] = []
        for line in str(chunk or "").splitlines(keepends=True):
            raw_line = str(line or "")
            stripped = raw_line.strip()
            prefix_index = stripped.find(self._RUNTIME_EVENT_PREFIX)
            if prefix_index >= 0:
                payload_text = stripped[prefix_index + len(self._RUNTIME_EVENT_PREFIX) :].strip()
                try:
                    payload = json.loads(payload_text) if payload_text else {}
                except Exception:
                    payload = {}
                message = self._format_runtime_event(payload if isinstance(payload, dict) else {})
                if message:
                    visible_lines.append(message + ("\n" if raw_line.endswith("\n") else ""))
                continue
            if self._should_display_log_line(raw_line):
                visible_lines.append(raw_line)
        return "".join(visible_lines)

    def _buffer_log_chunk(self, chunk: str) -> None:
        raw_text = str(chunk or "").replace("\r\n", "\n").replace("\r", "\n")
        text = self._consume_runtime_events(raw_text).replace("\r\n", "\n").replace("\r", "\n")
        if self._log_buffer and raw_text.startswith("\n") and text and not text.startswith("\n"):
            text = "\n" + text
        if not text:
            return
        self._log_buffer += text
        if "\n" not in self._log_buffer:
            return
        parts = self._log_buffer.split("\n")
        self._log_buffer = parts.pop() if parts else ""
        self._append_log_lines(parts)

    def _on_log_added(self, chunk: str) -> None:
        if not self._capturing_logs or not str(chunk or "").strip():
            return
        self._buffer_log_chunk(str(chunk))

    def _on_task_completed(self, task_name: str, success: bool, message: str, result: object) -> None:
        if task_name != "autoresponder":
            return
        payload = dict(result) if isinstance(result, dict) else {}
        if not success and message:
            payload.setdefault("message", message)
            payload.setdefault("status", "Failed")
            payload.setdefault("finished_at", datetime.now().isoformat(timespec="seconds"))
        self._start_pending = False
        self._pending_run_id = ""
        self._flush_log_buffer()
        self._capturing_logs = False
        self._timer.stop()
        self._maybe_show_summary(payload)
        self._stack.setCurrentWidget(self._config_view)
        self.refresh_snapshot()

    def _maybe_show_summary(self, snapshot: dict[str, Any]) -> None:
        run_id = str(snapshot.get("run_id") or "")
        if not run_id or run_id == self._last_summary_run_id:
            return
        self._last_summary_run_id = run_id
        dialog = AutomationMessageDialog(
            title="Resumen de autoresponder",
            message=(
                f"Tiempo de ejecucion: {format_run_duration(str(snapshot.get('started_at') or ''), str(snapshot.get('finished_at') or ''))}\n"
                f"Cuentas utilizadas: {safe_int(snapshot.get('accounts_total'))}\n"
                f"Respuestas OK: {safe_int(snapshot.get('message_success'))}\n"
                f"Respuestas fallidas: {safe_int(snapshot.get('message_failed'))}\n"
                f"Follow-ups OK: {safe_int(snapshot.get('followup_success'))}\n"
                f"Follow-ups fallidos: {safe_int(snapshot.get('followup_failed'))}\n"
                f"Agendas generadas: {safe_int(snapshot.get('agendas_generated'))}"
            ),
            confirm_text="Cerrar",
            parent=self,
        )
        dialog.exec()

    def refresh_snapshot(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        selected_alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_autoresponder_snapshot(
                self._ctx.services,
                self._ctx.tasks,
                active_alias=self._ctx.state.active_alias,
                selected_alias=selected_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_snapshot()

    def on_navigate_from(self) -> None:
        self._timer.stop()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_snapshot(self._snapshot_cache)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar el monitor: {error.message}")
