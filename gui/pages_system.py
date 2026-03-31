from __future__ import annotations

import json
from typing import Any

from PySide6.QtCore import QDateTime, QTimer, Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QDateTimeEdit,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QVBoxLayout,
)

from gui.query_runner import QueryError

from .error_handling import app_log_path
from .page_base import ClickableMetricCard, PageContext, SectionPage, pretty_json, safe_int, table_item
from .snapshot_queries import (
    build_system_config_snapshot,
    build_system_diagnostics_snapshot,
    build_system_home_snapshot,
    build_system_license_snapshot,
    build_system_logs_snapshot,
    build_system_update_check_snapshot,
)


SYSTEM_SUBSECTIONS: tuple[tuple[str, str], ...] = (
    ("system_license_page", "Licencias"),
    ("system_logs_page", "Logs"),
    ("system_config_page", "Config"),
    ("system_diagnostics_page", "Diagnostico"),
)


def _update_check_status_message(payload: Any) -> str:
    data = payload if isinstance(payload, dict) else {}
    status = str(data.get("status") or "").strip()
    message = str(data.get("message") or "").strip()
    if status == "update_available":
        return message or "Hay una actualizacion disponible."
    if status == "up_to_date":
        return message or "No hay updates disponibles."
    if status == "error":
        return message or "No se pudo verificar updates."
    return "Check de updates ejecutado."


class SystemSectionPage(SectionPage):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        route_key: str | None,
        back_button: bool = True,
        scrollable: bool = True,
        parent=None,
    ) -> None:
        super().__init__(
            ctx,
            title,
            subtitle,
            section_title="Sistema",
            section_subtitle="Submenu horizontal para licencias, logs, config y diagnostico.",
            section_routes=SYSTEM_SUBSECTIONS,
            route_key=route_key,
            back_button=back_button,
            scrollable=scrollable,
            parent=parent,
        )


class SystemHomePage(SystemSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Sistema",
            "Licencias, logs, configuracion y diagnostico operativo.",
            route_key=None,
            back_button=False,
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Centro del sistema",
            "Las herramientas de soporte y diagnostico quedan separadas para mantener el shell principal limpio y predecible.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(10)
        self._cards = {
            "licenses": ClickableMetricCard("Licencias", "0"),
            "connected": ClickableMetricCard("Cuentas activas", "0"),
            "tasks": ClickableMetricCard("Tareas activas", "0"),
            "sessions": ClickableMetricCard("Sesiones guardadas", "0"),
        }
        self._cards["licenses"].clicked.connect(lambda: self._ctx.open_route("system_license_page", None))
        self._cards["connected"].clicked.connect(lambda: self._ctx.open_route("system_diagnostics_page", None))
        self._cards["tasks"].clicked.connect(lambda: self._ctx.open_route("system_logs_page", None))
        self._cards["sessions"].clicked.connect(lambda: self._ctx.open_route("system_diagnostics_page", None))
        for index, key in enumerate(("licenses", "connected", "tasks", "sessions")):
            grid.addWidget(self._cards[key], index // 2, index % 2)
        layout.addLayout(grid)

        helper = QLabel(
            "Usa el submenu superior para entrar directo al panel de soporte que necesites sin mezclar formularios con diagnosticos."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)

        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        self._cards["licenses"].set_value(payload.get("licenses", 0))
        self._cards["connected"].set_value(payload.get("connected_accounts", 0))
        self._cards["tasks"].set_value(payload.get("tasks", 0))
        self._cards["sessions"].set_value(payload.get("sessions", 0))
        self._summary.setText(str(payload.get("summary") or "").strip())
        self.clear_status()

    def _request_refresh(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        if self._snapshot_cache is None:
            self.set_status("Cargando resumen del sistema...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_system_home_snapshot(self._ctx.services, self._ctx.tasks),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

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
        self.set_status(f"No se pudo cargar el resumen del sistema: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._request_refresh()


class SystemLicensePage(SystemSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Licencias",
            "Alta, desactivacion, extension y control de activaciones sobre Supabase.",
            route_key="system_license_page",
            parent=parent,
        )
        form = QFrame()
        form.setObjectName("SendSetupCard")
        form_layout = QGridLayout(form)
        form_layout.setContentsMargins(16, 16, 16, 16)
        form_layout.setHorizontalSpacing(8)
        form_layout.setVerticalSpacing(8)

        self._client_name = QLineEdit()
        self._plan_name = QLineEdit()
        self._plan_name.setText("standard")
        self._max_devices = QSpinBox()
        self._max_devices.setRange(1, 50)
        self._max_devices.setValue(2)
        self._expires_at = QDateTimeEdit()
        self._expires_at.setCalendarPopup(True)
        self._expires_at.setDisplayFormat("yyyy-MM-dd HH:mm")
        self._expires_at.setDateTime(QDateTime.currentDateTime().addDays(30))
        self._notes = QPlainTextEdit()
        self._notes.setPlaceholderText("Notas internas de la licencia")
        self._notes.setFixedHeight(84)
        create_button = QPushButton("Crear licencia")
        create_button.setObjectName("PrimaryButton")
        create_button.clicked.connect(self._create_license)
        form_layout.addWidget(QLabel("Cliente"), 0, 0)
        form_layout.addWidget(self._client_name, 0, 1)
        form_layout.addWidget(QLabel("Plan"), 0, 2)
        form_layout.addWidget(self._plan_name, 0, 3)
        form_layout.addWidget(QLabel("Max dispositivos"), 1, 0)
        form_layout.addWidget(self._max_devices, 1, 1)
        form_layout.addWidget(QLabel("Expira"), 1, 2)
        form_layout.addWidget(self._expires_at, 1, 3)
        form_layout.addWidget(QLabel("Notas"), 2, 0)
        form_layout.addWidget(self._notes, 2, 1, 1, 3)
        form_layout.addWidget(create_button, 3, 3)
        self.content_layout().addWidget(form)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["License key", "Cliente", "Plan", "Max", "Expira", "Estado"]
        )
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._table.itemSelectionChanged.connect(self.refresh_activations)
        self.content_layout().addWidget(self._table)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        self._extend_days = QSpinBox()
        self._extend_days.setRange(1, 3650)
        self._extend_days.setValue(30)
        extend_button = QPushButton("Extender")
        extend_button.setObjectName("SecondaryButton")
        extend_button.clicked.connect(self._extend_selected)
        deactivate_button = QPushButton("Desactivar")
        deactivate_button.setObjectName("DangerButton")
        deactivate_button.clicked.connect(self._deactivate_selected)
        reset_button = QPushButton("Reset activaciones")
        reset_button.setObjectName("SecondaryButton")
        reset_button.clicked.connect(self._reset_activations)
        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_licenses)
        actions.addWidget(QLabel("Dias extra"))
        actions.addWidget(self._extend_days)
        actions.addWidget(extend_button)
        actions.addWidget(deactivate_button)
        actions.addWidget(reset_button)
        actions.addWidget(refresh_button)
        actions.addStretch(1)
        self.content_layout().addLayout(actions)

        activations_title = QLabel("Activaciones del dispositivo seleccionado")
        activations_title.setObjectName("SectionPanelHint")
        self.content_layout().addWidget(activations_title)

        self._activations_table = QTableWidget(0, 5)
        self._activations_table.setHorizontalHeaderLabels(
            ["Device ID", "Equipo", "Usuario", "Activado", "Ultimo seen"]
        )
        self._activations_table.verticalHeader().setVisible(False)
        self._activations_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._activations_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._activations_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._activations_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.content_layout().addWidget(self._activations_table)

        self.content_layout().addStretch(1)
        self._licenses_request_id = 0
        self._activations_request_id = 0
        self._licenses_loading = False
        self._activations_loading = False

    def _selected_key(self) -> str:
        row = self._table.currentRow()
        if row < 0:
            return ""
        item = self._table.item(row, 0)
        return str(item.text() if item else "").strip()

    def _create_license(self) -> None:
        client_name = str(self._client_name.text() or "").strip()
        if not client_name:
            self.show_error("Ingresa el nombre del cliente.")
            return
        try:
            record = self._ctx.services.system.create_license(
                client_name,
                plan_name=str(self._plan_name.text() or "").strip() or "standard",
                max_devices=self._max_devices.value(),
                expires_at=self._expires_at.dateTime().toPython().astimezone().isoformat(),
                notes=self._notes.toPlainText().strip(),
            )
        except Exception as exc:
            self.show_error(str(exc) or "No se pudo crear la licencia.")
            return
        self._client_name.clear()
        self._plan_name.setText("standard")
        self._max_devices.setValue(2)
        self._expires_at.setDateTime(QDateTime.currentDateTime().addDays(30))
        self._notes.clear()
        self.refresh_licenses(selected_key=str(record.get("license_key") or ""))
        self.set_status(f"Licencia creada: {record.get('license_key', '')}")

    def _extend_selected(self) -> None:
        license_key = self._selected_key()
        if not license_key:
            self.show_error("Selecciona una licencia.")
            return
        try:
            record = self._ctx.services.system.extend_license(
                license_key,
                days=self._extend_days.value(),
            )
        except Exception as exc:
            self.show_error(str(exc) or "No se pudo extender la licencia.")
            return
        if not record:
            self.show_error("No se pudo extender la licencia.")
            return
        self.refresh_licenses(selected_key=license_key)
        self.set_status(f"Licencia extendida: {license_key}")

    def _deactivate_selected(self) -> None:
        license_key = self._selected_key()
        if not license_key:
            self.show_error("Selecciona una licencia.")
            return
        try:
            record = self._ctx.services.system.deactivate_license(license_key)
        except Exception as exc:
            self.show_error(str(exc) or "No se pudo desactivar la licencia.")
            return
        if not record:
            self.show_error("No se pudo desactivar la licencia.")
            return
        self.refresh_licenses(selected_key=license_key)
        self.set_status(f"Licencia desactivada: {license_key}")

    def _reset_activations(self) -> None:
        license_key = self._selected_key()
        if not license_key:
            self.show_error("Selecciona una licencia.")
            return
        try:
            removed = self._ctx.services.system.reset_device_activations(license_key)
        except Exception as exc:
            self.show_error(str(exc) or "No se pudieron resetear las activaciones.")
            return
        self.refresh_activations()
        self.set_status(f"Activaciones reseteadas: {removed}")

    def refresh_licenses(self, *, selected_key: str = "") -> None:
        if self._licenses_loading:
            return
        self._licenses_loading = True
        self.set_status("Cargando licencias...")
        self._licenses_request_id = self._ctx.queries.submit(
            lambda: build_system_license_snapshot(self._ctx.services),
            on_success=lambda request_id, payload: self._on_licenses_loaded(
                request_id,
                payload,
                selected_key=selected_key,
            ),
            on_error=self._on_licenses_failed,
        )

    def refresh_activations(self) -> None:
        if self._activations_loading:
            return
        license_key = self._selected_key()
        if not license_key:
            self._activations_table.setRowCount(0)
            return
        self._activations_loading = True
        self._activations_request_id = self._ctx.queries.submit(
            lambda: {"activations": self._ctx.services.system.list_license_activations(license_key)},
            on_success=self._on_activations_loaded,
            on_error=self._on_activations_failed,
        )

    def _apply_licenses_snapshot(self, payload: dict[str, Any], *, selected_key: str = "") -> None:
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        self._table.setRowCount(len(rows))
        selected_row = -1
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            values = [
                row.get("license_key", ""),
                row.get("client_name", ""),
                row.get("plan_name", ""),
                row.get("max_devices", ""),
                row.get("expires_at", ""),
                row.get("status", ""),
            ]
            for column, value in enumerate(values):
                self._table.setItem(row_index, column, table_item(value))
            if values[0] == selected_key:
                selected_row = row_index
        if selected_row >= 0:
            self._table.selectRow(selected_row)
        elif rows and self._table.currentRow() < 0:
            self._table.selectRow(0)

    def _apply_activations_snapshot(self, payload: dict[str, Any]) -> None:
        rows = payload.get("activations") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        self._activations_table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            values = [
                row.get("device_id", ""),
                row.get("machine_name", ""),
                row.get("os_user", ""),
                row.get("activated_at", ""),
                row.get("last_seen_at", ""),
            ]
            for column, value in enumerate(values):
                self._activations_table.setItem(row_index, column, table_item(value))
        self.clear_status()

    def _on_licenses_loaded(
        self,
        request_id: int,
        payload: Any,
        *,
        selected_key: str = "",
    ) -> None:
        if request_id != self._licenses_request_id:
            return
        self._licenses_loading = False
        self._apply_licenses_snapshot(
            dict(payload) if isinstance(payload, dict) else {},
            selected_key=selected_key,
        )
        self.refresh_activations()

    def _on_licenses_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._licenses_request_id:
            return
        self._licenses_loading = False
        self.set_status(f"No se pudieron cargar las licencias: {error.message}")

    def _on_activations_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._activations_request_id:
            return
        self._activations_loading = False
        self._apply_activations_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_activations_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._activations_request_id:
            return
        self._activations_loading = False
        self.set_status(f"No se pudieron cargar las activaciones: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        self.refresh_licenses()


class SystemLogsPage(SystemSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Logs GUI",
            "Salida de tareas, runtime y log persistente visible dentro de la aplicacion.",
            route_key="system_logs_page",
            parent=parent,
        )
        self._log_path = app_log_path(self._ctx.services.context.root_dir)
        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("DangerButton")
        clear_button.clicked.connect(self._ctx.logs.clear)
        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self._sync_text)
        actions.addWidget(clear_button)
        actions.addWidget(refresh_button)
        actions.addStretch(1)
        self.content_layout().addLayout(actions)

        self._path_label = QLabel(f"Archivo persistente: {self._log_path}")
        self._path_label.setObjectName("MutedText")
        self._path_label.setWordWrap(True)
        self.content_layout().addWidget(self._path_label)

        self._text = QPlainTextEdit()
        self._text.setObjectName("LogConsole")
        self._text.setReadOnly(True)
        self.content_layout().addWidget(self._text, 1)

        self._file_text = QPlainTextEdit()
        self._file_text.setObjectName("LogConsole")
        self._file_text.setReadOnly(True)
        self._file_text.setPlaceholderText("Contenido de logs/app.log")
        self.content_layout().addWidget(self._file_text, 1)

        self._ctx.logs.logAdded.connect(self._append_text)
        self._ctx.logs.cleared.connect(self._on_logs_cleared)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._memory_log_cursor = 0
        self._file_log_cursor = 0
        self._live_updates = False

    def _append_text(self, chunk: str) -> None:
        if not self._live_updates:
            return
        self._text.moveCursor(QTextCursor.End)
        self._text.insertPlainText(str(chunk or ""))
        self._text.moveCursor(QTextCursor.End)
        self._memory_log_cursor = self._ctx.logs.cursor()

    def _on_logs_cleared(self) -> None:
        if not self._live_updates:
            return
        self._text.clear()
        self._memory_log_cursor = self._ctx.logs.cursor()

    def _sync_text(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self.set_status("Cargando logs...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_system_logs_snapshot(
                self._ctx.logs,
                log_path=self._log_path,
                log_cursor=self._memory_log_cursor,
                file_cursor=self._file_log_cursor,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _apply_delta(self, widget: QPlainTextEdit, text: str, *, reset: bool) -> None:
        if reset:
            widget.setPlainText(text)
        elif text:
            widget.moveCursor(QTextCursor.End)
            widget.insertPlainText(text)
        widget.moveCursor(QTextCursor.End)

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        requested_log_cursor = int(payload.get("requested_log_cursor") or 0)
        if requested_log_cursor == self._memory_log_cursor:
            self._apply_delta(
                self._text,
                str(payload.get("log_text") or ""),
                reset=bool(payload.get("log_reset", False)),
            )
            self._memory_log_cursor = int(payload.get("log_cursor") or 0)
        self._apply_delta(
            self._file_text,
            str(payload.get("file_text") or ""),
            reset=bool(payload.get("file_reset", False)),
        )
        self._file_log_cursor = int(payload.get("file_cursor") or 0)
        self.clear_status()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudieron cargar los logs: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        self._live_updates = True
        self._sync_text()

    def on_navigate_from(self) -> None:
        self._live_updates = False


class SupabaseConfigDialog(QDialog):
    def __init__(
        self,
        current_config: dict[str, Any] | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Configurar Supabase")
        self.setModal(True)
        self.setMinimumWidth(480)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        hint = QLabel("Los servicios de licencias y telemetria leeran estas credenciales desde app/config.json.")
        hint.setObjectName("SectionPanelHint")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        form = QGridLayout()
        form.setHorizontalSpacing(8)
        form.setVerticalSpacing(8)
        self._url = QLineEdit()
        self._url.setPlaceholderText("https://<project>.supabase.co")
        self._key = QLineEdit()
        self._key.setEchoMode(QLineEdit.Password)
        self._key.setPlaceholderText("API key de Supabase")
        payload = dict(current_config or {})
        self._url.setText(str(payload.get("supabase_url") or "").strip())
        self._key.setText(str(payload.get("supabase_key") or "").strip())
        form.addWidget(QLabel("Supabase URL"), 0, 0)
        form.addWidget(self._url, 0, 1)
        form.addWidget(QLabel("Supabase Key"), 1, 0)
        form.addWidget(self._key, 1, 1)
        layout.addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        save_button = QPushButton("Guardar")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(save_button)
        layout.addLayout(actions)

    def values(self) -> dict[str, str]:
        return {
            "supabase_url": str(self._url.text() or "").strip(),
            "supabase_key": str(self._key.text() or "").strip(),
        }


class SystemConfigPage(SystemSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Configuracion",
            "Editor JSON para configuracion de updates y chequeos del sistema.",
            route_key="system_config_page",
            parent=parent,
        )
        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        load_button = QPushButton("Cargar")
        load_button.setObjectName("SecondaryButton")
        load_button.clicked.connect(self.load_payload)
        save_button = QPushButton("Guardar")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.save_payload)
        check_button = QPushButton("Verificar updates")
        check_button.setObjectName("SecondaryButton")
        check_button.clicked.connect(self.check_updates)
        supabase_button = QPushButton("Configurar Supabase")
        supabase_button.setObjectName("SecondaryButton")
        supabase_button.clicked.connect(self.configure_supabase)
        actions.addWidget(load_button)
        actions.addWidget(save_button)
        actions.addWidget(check_button)
        actions.addWidget(supabase_button)
        actions.addStretch(1)
        self.content_layout().addLayout(actions)

        self._editor = QPlainTextEdit()
        self._editor.setObjectName("LogConsole")
        self.content_layout().addWidget(self._editor, 1)

        self._updates_box = QPlainTextEdit()
        self._updates_box.setObjectName("LogConsole")
        self._updates_box.setReadOnly(True)
        self._updates_box.setPlaceholderText("Resultado estructurado del check de updates.")
        self.content_layout().addWidget(self._updates_box)
        self._config_request_id = 0
        self._config_loading = False
        self._updates_request_id = 0
        self._updates_loading = False

    def load_payload(self) -> None:
        if self._config_loading:
            return
        self._config_loading = True
        self.set_status("Cargando configuracion...")
        self._config_request_id = self._ctx.queries.submit(
            lambda: build_system_config_snapshot(self._ctx.services),
            on_success=self._on_config_loaded,
            on_error=self._on_config_failed,
        )

    def save_payload(self) -> None:
        try:
            payload = json.loads(self._editor.toPlainText() or "{}")
            if not isinstance(payload, dict):
                raise ValueError("La configuracion debe ser un objeto JSON.")
        except Exception as exc:
            self.show_exception(exc, "No se pudo procesar la configuracion. Ver logs para mas detalles.")
            return
        saved = self._ctx.services.system.save_update_config(payload)
        self._editor.setPlainText(pretty_json(saved))
        self.set_status("Configuracion guardada.")

    def check_updates(self) -> None:
        if self._updates_loading:
            return
        self._updates_loading = True
        self.set_status("Verificando updates...")
        self._updates_request_id = self._ctx.queries.submit(
            lambda: build_system_update_check_snapshot(self._ctx.services),
            on_success=self._on_updates_loaded,
            on_error=self._on_updates_failed,
        )

    def configure_supabase(self) -> None:
        dialog = SupabaseConfigDialog(
            self._ctx.services.system.supabase_config(),
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        payload = dialog.values()
        if not payload["supabase_url"] or not payload["supabase_key"]:
            self.show_error("Debes completar Supabase URL y Supabase Key.")
            return
        try:
            self._ctx.services.system.save_supabase_config(
                supabase_url=payload["supabase_url"],
                supabase_key=payload["supabase_key"],
            )
        except Exception as exc:
            self.show_error(str(exc) or "No se pudo guardar la configuracion de Supabase.")
            return
        self.show_info("Configuracion de Supabase guardada.")

    def on_navigate_to(self, payload: Any = None) -> None:
        self.load_payload()

    def _on_config_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._config_request_id:
            return
        self._config_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        self._editor.setPlainText(pretty_json(data.get("payload") or {}))
        self.clear_status()

    def _on_config_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._config_request_id:
            return
        self._config_loading = False
        self.set_status(f"No se pudo cargar la configuracion: {error.message}")

    def _on_updates_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._updates_request_id:
            return
        self._updates_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        self._updates_box.setPlainText(pretty_json(data))
        self.set_status(_update_check_status_message(data))

    def _on_updates_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._updates_request_id:
            return
        self._updates_loading = False
        self.set_status(f"No se pudo verificar updates: {error.message}")


class SystemDiagnosticsPage(SystemSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Diagnostico",
            "Estado de playwright, cuentas activas, workers, sesiones y colas.",
            route_key="system_diagnostics_page",
            parent=parent,
        )
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["Area", "Estado", "Detalle"])
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.content_layout().addWidget(self._table)

        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_page)
        self.content_layout().addWidget(refresh_button, 0, Qt.AlignLeft)

        self._raw = QPlainTextEdit()
        self._raw.setObjectName("LogConsole")
        self._raw.setReadOnly(True)
        self.content_layout().addWidget(self._raw, 1)

        self._timer = QTimer(self)
        self._timer.setInterval(5000)
        self._timer.timeout.connect(self.refresh_page)
        self._snapshot_request_id = 0
        self._snapshot_loading = False

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self.set_status("Cargando diagnostico...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_system_diagnostics_snapshot(
                self._ctx.services,
                self._ctx.tasks,
                root_dir=self._ctx.services.context.root_dir,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        data = payload.get("payload") if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            data = {}
        rows = [
            (
                "Playwright",
                "OK" if bool((data.get("playwright") or {}).get("ok")) else "ERROR",
                (data.get("playwright") or {}).get("path")
                or (data.get("playwright") or {}).get("error")
                or "No disponible",
            ),
            (
                "Cuentas activas",
                str(data.get("accounts_active") or 0),
                "Cuentas conectadas segun dashboard_snapshot().",
            ),
            (
                "Workers",
                str(
                    safe_int((data.get("workers") or {}).get("gui_tasks"))
                    + safe_int((data.get("workers") or {}).get("inbox_workers"))
                ),
                f"GUI={(data.get('workers') or {}).get('gui_tasks', 0)} | "
                f"Inbox={(data.get('workers') or {}).get('inbox_workers', 0)}",
            ),
            (
                "Sesiones",
                str((data.get("sessions") or {}).get("saved_sessions", 0)),
                (
                    f"Guardadas={(data.get('sessions') or {}).get('saved_sessions', 0)} | "
                    f"Profiles={(data.get('sessions') or {}).get('browser_profiles', 0)}"
                ),
            ),
            (
                "Colas",
                str(safe_int((data.get("queues") or {}).get("inbox_tasks"))),
                f"Inbox={(data.get('queues') or {}).get('inbox_tasks', 0)}",
            ),
        ]
        self._table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            for column, value in enumerate(row):
                self._table.setItem(row_index, column, table_item(value))
        self._raw.setPlainText(pretty_json(data))
        self.clear_status()

    def on_navigate_to(self, payload: Any = None) -> None:
        if not self._timer.isActive():
            self._timer.start()
        self.refresh_page()

    def on_navigate_from(self) -> None:
        if self._timer.isActive():
            self._timer.stop()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar el diagnostico: {error.message}")
