from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDoubleSpinBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext, safe_int, table_item
from gui.query_runner import QueryError
from gui.snapshot_queries import (
    build_leads_filter_detail_snapshot,
    build_leads_filter_execution_snapshot,
    build_leads_filter_runner_snapshot,
)

from .common import (
    BROWSER_MODE_ITEMS,
    LeadsModalDialog,
    browser_mode_label,
    configure_data_table,
    format_filter_log_line,
    open_dark_file_dialog,
    set_panel_status,
    show_panel_error,
    show_panel_exception,
)
from .filter_config_panel import LeadsFilterConfigPanel


class _RunSummaryDialog(LeadsModalDialog):
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        headline: str,
        note: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(headline, note, parent=parent)
        self.resize(460, 300)

        layout = self.body_layout()
        for label, key in (
            ("Procesados", "processed"),
            ("Calificados", "qualified"),
            ("Descartados", "discarded"),
            ("Pendientes", "pending"),
            ("Tiempo total", "elapsed_label"),
        ):
            row = QHBoxLayout()
            row.setContentsMargins(0, 0, 0, 0)
            row.setSpacing(8)
            row.addWidget(QLabel(label))
            value = QLabel(str(payload.get(key) or "0"))
            value.setObjectName("SendSetupSummaryValue")
            row.addWidget(value, 1, Qt.AlignRight)
            layout.addLayout(row)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(self.accept)
        layout.addWidget(close_button, 0, Qt.AlignRight)


class _StoppedRunDialog(LeadsModalDialog):
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        default_alias: str,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(
            "Filtrado detenido",
            "Elige cómo resolver la lista parcial antes de volver al panel de filtrado.",
            parent=parent,
        )
        self.resize(560, 380)
        self._choice = "keep"

        layout = self.body_layout()
        for label, key in (
            ("Procesados", "processed"),
            ("Calificados", "qualified"),
            ("Descartados", "discarded"),
            ("Pendientes", "pending"),
            ("Tiempo total", "elapsed_label"),
        ):
            row = QHBoxLayout()
            row.setContentsMargins(0, 0, 0, 0)
            row.setSpacing(8)
            row.addWidget(QLabel(label))
            value = QLabel(str(payload.get(key) or "0"))
            value.setObjectName("SendSetupSummaryValue")
            row.addWidget(value, 1, Qt.AlignRight)
            layout.addLayout(row)

        layout.addWidget(QLabel("Alias de guardado para leads calificados"))
        self._alias_input = QLineEdit(str(default_alias or "leads_filtrados"))
        layout.addWidget(self._alias_input)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)

        export_button = QPushButton("Guardar leads calificados")
        export_button.setObjectName("PrimaryButton")
        export_button.clicked.connect(lambda: self._select("export"))

        keep_button = QPushButton("Guardar lista incompleta")
        keep_button.setObjectName("SecondaryButton")
        keep_button.clicked.connect(lambda: self._select("keep"))

        delete_button = QPushButton("Eliminar lista")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(lambda: self._select("delete"))

        buttons.addWidget(export_button)
        buttons.addWidget(keep_button)
        buttons.addWidget(delete_button)
        layout.addLayout(buttons)

    def _select(self, choice: str) -> None:
        self._choice = str(choice or "keep").strip().lower() or "keep"
        self.accept()

    def choice(self) -> str:
        return self._choice

    def export_alias(self) -> str:
        return str(self._alias_input.text() or "").strip()


class FilteringMonitorView(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        shell = QFrame()
        shell.setObjectName("ExecCard")
        shell.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        root.addWidget(shell, 1)

        layout = QVBoxLayout(shell)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)

        title = QLabel("Filtrado activo")
        title.setObjectName("SendSetupSectionTitle")
        subtitle = QLabel(
            "Supervisa alias, workers, progreso y actividad en vivo desde el arranque hasta el frenado."
        )
        subtitle.setObjectName("MutedText")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        self.alias_value = self._value_label("-")
        self.source_value = self._value_label("-")
        self.export_value = self._value_label("-")
        self.accounts_value = self._value_label("0")
        self.status_label = self._value_label("Esperando inicio.")
        self.status_label.setWordWrap(True)
        self.total_value = self._value_label("0")
        self.eta_value = self._value_label("-")
        self.started_value = self._value_label("-")

        header_grid = QGridLayout()
        header_grid.setContentsMargins(0, 0, 0, 0)
        header_grid.setHorizontalSpacing(12)
        header_grid.setVerticalSpacing(12)
        header_cards = (
            ("Alias", self.alias_value),
            ("Lista origen", self.source_value),
            ("Alias de guardado", self.export_value),
            ("Workers activos", self.accounts_value),
            ("Estado actual", self.status_label),
            ("Total de leads", self.total_value),
            ("Tiempo restante", self.eta_value),
            ("Inicio", self.started_value),
        )
        for index, (title_text, value_label) in enumerate(header_cards):
            row = index // 4
            column = index % 4
            header_grid.addWidget(self._metric_card(title_text, value_label), row, column)
        layout.addLayout(header_grid)

        self.summary_label = QLabel("")
        self.summary_label.setObjectName("MutedText")
        self.summary_label.setWordWrap(True)
        self.summary_label.hide()
        layout.addWidget(self.summary_label)

        progress_card = QFrame()
        progress_card.setObjectName("ExecCard")
        progress_layout = QVBoxLayout(progress_card)
        progress_layout.setContentsMargins(16, 16, 16, 16)
        progress_layout.setSpacing(10)

        progress_title = QLabel("Progreso")
        progress_title.setObjectName("SendSetupSectionTitle")
        progress_layout.addWidget(progress_title)

        self.progress_detail_label = QLabel("0 de 0 perfiles procesados")
        self.progress_detail_label.setObjectName("MutedText")
        self.progress_detail_label.setWordWrap(True)
        progress_layout.addWidget(self.progress_detail_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setObjectName("ExecProgressBar")
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setMinimumHeight(28)
        progress_layout.addWidget(self.progress_bar)
        layout.addWidget(progress_card)

        stats_card = QFrame()
        stats_card.setObjectName("ExecCard")
        stats_layout = QGridLayout(stats_card)
        stats_layout.setContentsMargins(16, 16, 16, 16)
        stats_layout.setHorizontalSpacing(10)
        stats_layout.setVerticalSpacing(10)

        self.processed_value = self._value_label("0")
        self.qualified_value = self._value_label("0")
        self.discarded_value = self._value_label("0")
        self.pending_value = self._value_label("0")
        self.errors_value = self._value_label("0")

        for index, (title_text, value_label) in enumerate(
            (
                ("Procesados", self.processed_value),
                ("Validos", self.qualified_value),
                ("Descartados", self.discarded_value),
                ("Pendientes", self.pending_value),
                ("Errores", self.errors_value),
            )
        ):
            stats_layout.addWidget(self._stat_card(title_text, value_label), 0, index)
        layout.addWidget(stats_card)

        activity_card = QFrame()
        activity_card.setObjectName("ExecCard")
        activity_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        activity_layout = QVBoxLayout(activity_card)
        activity_layout.setContentsMargins(16, 16, 16, 16)
        activity_layout.setSpacing(10)

        activity_title = QLabel("Actividad en vivo")
        activity_title.setObjectName("SendSetupSectionTitle")
        activity_layout.addWidget(activity_title)

        self.log_box = QPlainTextEdit()
        self.log_box.setObjectName("LogConsole")
        self.log_box.setReadOnly(True)
        self.log_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.log_box.setMinimumHeight(380)
        activity_layout.addWidget(self.log_box, 1)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        footer.addStretch(1)

        self.pause_button = QPushButton("Pausar filtrado")
        self.pause_button.setObjectName("SecondaryButton")
        self.pause_button.setMinimumHeight(38)
        footer.addWidget(self.pause_button)

        self.stop_button = QPushButton("Detener filtrado")
        self.stop_button.setObjectName("DangerButton")
        self.stop_button.setMinimumHeight(38)
        footer.addWidget(self.stop_button)
        activity_layout.addLayout(footer)
        layout.addWidget(activity_card, 1)

    def _value_label(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setObjectName("SendSetupSummaryValue")
        label.setWordWrap(True)
        return label

    def _metric_card(self, title: str, value_label: QLabel) -> QFrame:
        card = QFrame()
        card.setObjectName("ExecCard")
        card.setMinimumHeight(88)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(6)
        title_label = QLabel(title)
        title_label.setObjectName("MutedText")
        layout.addWidget(title_label)
        layout.addWidget(value_label, 1)
        return card

    def _stat_card(self, title: str, value_label: QLabel) -> QFrame:
        card = QFrame()
        card.setObjectName("ExecCard")
        card.setMinimumHeight(80)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(4)
        title_label = QLabel(title)
        title_label.setObjectName("MutedText")
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return card

    def reset(self) -> None:
        self.alias_value.setText("-")
        self.source_value.setText("-")
        self.export_value.setText("-")
        self.accounts_value.setText("0")
        self.total_value.setText("0")
        self.processed_value.setText("0")
        self.qualified_value.setText("0")
        self.discarded_value.setText("0")
        self.pending_value.setText("0")
        self.eta_value.setText("-")
        self.started_value.setText("-")
        self.errors_value.setText("0")
        self.summary_label.clear()
        self.summary_label.hide()
        self.status_label.setText("Sin ejecucion activa.")
        self.progress_detail_label.setText("0 de 0 perfiles procesados")
        self.progress_bar.setValue(0)
        self.log_box.clear()
        self.pause_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.stop_button.setText("Detener filtrado")

    def set_pause_available(self, available: bool) -> None:
        self.pause_button.setVisible(bool(available))
        self.pause_button.setEnabled(bool(available))

    def set_stop_pending(self, pending: bool) -> None:
        is_pending = bool(pending)
        self.stop_button.setEnabled(not is_pending)
        self.stop_button.setText("Deteniendo..." if is_pending else "Detener filtrado")


class LeadsFilterRunnerPanel(QWidget):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._last_result: dict[str, Any] = {}
        self._running_list_id = ""
        self._running_run_payload: dict[str, Any] = {}
        self._running_started_at = 0.0
        self._running_processed_baseline = 0
        self._prepared_action = ""
        self._prepared_list_id = ""
        self._pending_form_payload: dict[str, Any] | None = None
        self._pending_result_focus = ""
        self._filter_rows_by_id: dict[str, dict[str, Any]] = {}
        self._log_buffer = ""
        self._page_snapshot_request_id = 0
        self._page_snapshot_loading = False
        self._detail_snapshot_request_id = 0
        self._detail_snapshot_loading = False
        self._execution_snapshot_request_id = 0
        self._execution_snapshot_loading = False
        self._config_initialized = False
        self._idle_section = "landing"
        self._alias_account_rows: list[dict[str, Any]] = []
        self._computed_concurrency_value = 0
        self._max_runtime_seconds = 3600
        self._running_started_label = ""
        self._stop_requested = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._view_stack = QStackedWidget()
        self._idle_view = self._build_idle_view()
        self._running_view = self._build_running_view()
        self._view_stack.addWidget(self._idle_view)
        self._view_stack.addWidget(self._running_view)
        root.addWidget(self._view_stack, 1)

        self._account_alias.currentIndexChanged.connect(self._on_account_alias_changed)
        self._source_mode.currentIndexChanged.connect(self._on_source_mode_changed)
        self._source_list.currentIndexChanged.connect(self._refresh_activation_summary)
        self._export_alias.currentIndexChanged.connect(self._refresh_activation_summary)
        self._export_alias.editTextChanged.connect(self._refresh_activation_summary)
        self._delay_min.valueChanged.connect(self._refresh_activation_summary)
        self._delay_max.valueChanged.connect(self._refresh_activation_summary)
        self._browser_mode.currentIndexChanged.connect(self._refresh_activation_summary)
        self._manual_input.textChanged.connect(self._refresh_activation_summary)
        self._csv_path_input.textChanged.connect(self._refresh_activation_summary)
        self._txt_path_input.textChanged.connect(self._refresh_activation_summary)
        self._ctx.tasks.taskFinished.connect(self._on_task_finished)
        self._ctx.logs.logAdded.connect(self._on_log_added)
        pause_filtering = getattr(self._ctx.services.leads, "pause_filtering", None)
        if callable(pause_filtering):
            self._monitor_view.pause_button.clicked.connect(self._pause_filtering)
            self._monitor_view.set_pause_available(False)
        else:
            self._monitor_view.set_pause_available(False)
            self._monitor_view.pause_button.setToolTip("El motor actual no expone pausa.")
        self._monitor_view.stop_button.clicked.connect(self._stop_filtering)

        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._refresh_execution_panel)

    def _build_idle_view(self) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        self._idle_pages = QStackedWidget()
        self._landing_view = self._build_landing_view()
        self._activation_view = self._build_activation_page()
        self._config_view = self._build_config_page()
        self._results_view = self._build_results_page()
        self._idle_pages.addWidget(self._landing_view)
        self._idle_pages.addWidget(self._activation_view)
        self._idle_pages.addWidget(self._config_view)
        self._idle_pages.addWidget(self._results_view)
        layout.addWidget(self._idle_pages, 1)
        return container

    def _build_landing_view(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        intro_card, intro_layout = self._card(
            "Filtrado",
            "Selecciona una sola vista de trabajo. La configuracion, la activacion y los resultados quedan separados para evitar paneles comprimidos.",
        )
        buttons = QGridLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setHorizontalSpacing(12)
        buttons.setVerticalSpacing(12)
        for column, (label, description, target) in enumerate(
            (
                ("Activacion de filtrado", "Origen, alias y parametros de ejecucion.", "activation"),
                ("Configuracion de filtrado", "Criterios clasicos, texto inteligente y prompt visual.", "config"),
                ("Resultados de filtrado", "Corridas completas, incompletas y detalle de resultados.", "results"),
            )
        ):
            button = QPushButton(f"{label}\n{description}")
            button.setObjectName("SecondaryButton")
            button.setMinimumHeight(92)
            button.clicked.connect(lambda checked=False, page_key=target: self._show_idle_section(page_key))
            buttons.addWidget(button, 0, column)
        intro_layout.addLayout(buttons)
        layout.addWidget(intro_card)
        layout.addStretch(1)
        return page

    def _build_idle_section_shell(self, title: str, subtitle: str) -> tuple[QScrollArea, QVBoxLayout]:
        scroll = QScrollArea()
        scroll.setObjectName("SubmenuScroll")
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        container = QWidget()
        container.setObjectName("SubmenuScrollContent")
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(0, 0, 0, 0)
        toolbar.setSpacing(8)
        back_button = QPushButton("Volver a opciones")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(lambda: self._show_idle_section("landing"))
        toolbar.addWidget(back_button)
        toolbar.addStretch(1)
        layout.addLayout(toolbar)

        card, card_layout = self._card(title, subtitle)
        layout.addWidget(card)
        layout.addStretch(1)
        scroll.setWidget(container)
        return scroll, card_layout

    def _build_activation_page(self) -> QWidget:
        page, activation_layout = self._build_idle_section_shell(
            "Activacion de filtrado",
            "Selecciona el origen de usernames, el alias de guardado y el alias de cuentas que va a ejecutar el filtrado.",
        )
        activation_grid = QGridLayout()
        activation_grid.setContentsMargins(0, 0, 0, 0)
        activation_grid.setHorizontalSpacing(10)
        activation_grid.setVerticalSpacing(10)

        self._source_mode = QComboBox()
        self._source_mode.addItem("Lista guardada", "saved")
        self._source_mode.addItem("Archivo CSV", "csv")
        self._source_mode.addItem("Archivo TXT", "txt")
        self._source_mode.addItem("Ingreso manual", "manual")

        self._source_stack = QStackedWidget()
        self._source_list = QComboBox()
        self._source_stack.addWidget(self._make_source_list_widget())
        self._source_stack.addWidget(self._make_source_file_widget("csv"))
        self._source_stack.addWidget(self._make_source_file_widget("txt"))
        self._source_stack.addWidget(self._make_source_manual_widget())

        self._export_alias = QComboBox()
        self._export_alias.setEditable(True)
        self._account_alias = QComboBox()
        self._delay_min = QDoubleSpinBox()
        self._delay_min.setRange(0.0, 3600.0)
        self._delay_min.setDecimals(1)
        self._delay_min.setValue(20.0)
        self._delay_max = QDoubleSpinBox()
        self._delay_max.setRange(0.0, 3600.0)
        self._delay_max.setDecimals(1)
        self._delay_max.setValue(40.0)
        self._browser_mode = QComboBox()
        for label, value in BROWSER_MODE_ITEMS:
            self._browser_mode.addItem(label, value)
        self._concurrency = QLineEdit("0")
        self._concurrency.setReadOnly(True)

        activation_grid.addWidget(QLabel("Origen de usernames"), 0, 0)
        activation_grid.addWidget(self._source_mode, 0, 1)
        activation_grid.addWidget(QLabel("Alias de guardado"), 0, 2)
        activation_grid.addWidget(self._export_alias, 0, 3)
        activation_grid.addWidget(QLabel("Lista origen"), 1, 0, Qt.AlignTop)
        activation_grid.addWidget(self._source_stack, 1, 1, 1, 3)
        activation_grid.addWidget(QLabel("Alias de cuentas"), 2, 0)
        activation_grid.addWidget(self._account_alias, 2, 1)
        activation_grid.addWidget(QLabel("Concurrencia"), 2, 2)
        activation_grid.addWidget(self._concurrency, 2, 3)
        activation_grid.addWidget(QLabel("Delay minimo"), 3, 0)
        activation_grid.addWidget(self._delay_min, 3, 1)
        activation_grid.addWidget(QLabel("Delay maximo"), 3, 2)
        activation_grid.addWidget(self._delay_max, 3, 3)
        activation_grid.addWidget(QLabel("Modo navegador"), 4, 0)
        activation_grid.addWidget(self._browser_mode, 4, 1, 1, 3)
        activation_layout.addLayout(activation_grid)

        self._capacity_summary = QLabel("")
        self._capacity_summary.setObjectName("MutedText")
        self._capacity_summary.setWordWrap(True)
        activation_layout.addWidget(self._capacity_summary)

        self._activation_summary = QLabel("")
        self._activation_summary.setObjectName("MutedText")
        self._activation_summary.setWordWrap(True)
        activation_layout.addWidget(self._activation_summary)

        self._prepared_summary = QLabel("Nueva ejecucion lista para configurar.")
        self._prepared_summary.setObjectName("MutedText")
        self._prepared_summary.setWordWrap(True)
        activation_layout.addWidget(self._prepared_summary)

        activation_actions = QHBoxLayout()
        activation_actions.setContentsMargins(0, 0, 0, 0)
        activation_actions.setSpacing(8)
        self._start_button = QPushButton("Iniciar filtrado")
        self._start_button.setObjectName("PrimaryButton")
        self._start_button.clicked.connect(self._start_run)
        activation_actions.addStretch(1)
        activation_actions.addWidget(self._start_button)
        activation_layout.addLayout(activation_actions)
        return page

    def _build_config_page(self) -> QWidget:
        page, config_layout = self._build_idle_section_shell(
            "Configuracion de filtrado",
            "Define los criterios de evaluacion antes de iniciar o reanudar el filtrado.",
        )
        self._config_panel = LeadsFilterConfigPanel(self._ctx, self, on_changed=self._refresh_activation_summary)
        config_layout.addWidget(self._config_panel)
        return page

    def _build_results_page(self) -> QWidget:
        page, results_layout = self._build_idle_section_shell(
            "Resultados de filtrado",
            "Reutiliza filtrados completos, reanuda incompletos y consulta un detalle rapido de cada corrida.",
        )
        self._filter_lists_summary = QLabel("Cargando resultados de filtrado...")
        self._filter_lists_summary.setObjectName("MutedText")
        self._filter_lists_summary.setWordWrap(True)
        results_layout.addWidget(self._filter_lists_summary)

        tables_layout = QGridLayout()
        tables_layout.setContentsMargins(0, 0, 0, 0)
        tables_layout.setHorizontalSpacing(12)
        tables_layout.setVerticalSpacing(12)

        completed_card, completed_layout = self._mini_card("Resultados completos")
        self._completed_table = QTableWidget(0, 4)
        self._completed_table.setHorizontalHeaderLabels(["Lista", "Procesadas", "Calificadas", "Descartadas"])
        self._completed_table.verticalHeader().setVisible(False)
        self._completed_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._completed_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._completed_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._completed_table.itemSelectionChanged.connect(lambda: self._select_table("completed"))
        configure_data_table(self._completed_table)
        completed_layout.addWidget(self._completed_table)

        completed_actions = QHBoxLayout()
        completed_actions.setContentsMargins(0, 0, 0, 0)
        completed_actions.setSpacing(8)
        rerun_button = QPushButton("Reutilizar filtrado")
        rerun_button.setObjectName("PrimaryButton")
        rerun_button.clicked.connect(lambda: self._prepare_result_action("rerun"))
        delete_completed_button = QPushButton("Eliminar resultado")
        delete_completed_button.setObjectName("DangerButton")
        delete_completed_button.clicked.connect(lambda: self._delete_selected("completed"))
        completed_actions.addWidget(rerun_button)
        completed_actions.addWidget(delete_completed_button)
        completed_actions.addStretch(1)
        completed_layout.addLayout(completed_actions)
        tables_layout.addWidget(completed_card, 0, 0)

        incomplete_card, incomplete_layout = self._mini_card("Resultados incompletos")
        self._incomplete_table = QTableWidget(0, 5)
        self._incomplete_table.setHorizontalHeaderLabels(
            ["Lista", "Procesadas", "Calificadas", "Descartadas", "Pendientes"]
        )
        self._incomplete_table.verticalHeader().setVisible(False)
        self._incomplete_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._incomplete_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._incomplete_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._incomplete_table.itemSelectionChanged.connect(lambda: self._select_table("incomplete"))
        configure_data_table(self._incomplete_table)
        incomplete_layout.addWidget(self._incomplete_table)

        incomplete_actions = QHBoxLayout()
        incomplete_actions.setContentsMargins(0, 0, 0, 0)
        incomplete_actions.setSpacing(8)
        resume_button = QPushButton("Reanudar filtrado")
        resume_button.setObjectName("PrimaryButton")
        resume_button.clicked.connect(lambda: self._prepare_result_action("resume"))
        delete_incomplete_button = QPushButton("Eliminar resultado")
        delete_incomplete_button.setObjectName("DangerButton")
        delete_incomplete_button.clicked.connect(lambda: self._delete_selected("incomplete"))
        incomplete_actions.addWidget(resume_button)
        incomplete_actions.addWidget(delete_incomplete_button)
        incomplete_actions.addStretch(1)
        incomplete_layout.addLayout(incomplete_actions)
        tables_layout.addWidget(incomplete_card, 0, 1)

        results_layout.addLayout(tables_layout)

        detail_card, detail_layout = self._mini_card("Detalle de resultado")
        self._detail_summary = QLabel("Selecciona una lista completa o incompleta.")
        self._detail_summary.setObjectName("MutedText")
        self._detail_summary.setWordWrap(True)
        detail_layout.addWidget(self._detail_summary)
        self._detail_preview = QPlainTextEdit()
        self._detail_preview.setObjectName("LogConsole")
        self._detail_preview.setReadOnly(True)
        self._detail_preview.setMinimumHeight(180)
        detail_layout.addWidget(self._detail_preview)
        results_layout.addWidget(detail_card)
        return page

    def _build_running_view(self) -> QWidget:
        self._monitor_view = FilteringMonitorView(self)
        self._running_summary = self._monitor_view.summary_label
        self._execution_status = self._monitor_view.status_label
        self._log_box = self._monitor_view.log_box
        self._total_value = self._monitor_view.total_value
        self._processed_value = self._monitor_view.processed_value
        self._qualified_value = self._monitor_view.qualified_value
        self._discarded_value = self._monitor_view.discarded_value
        self._pending_value = self._monitor_view.pending_value
        self._errors_value = self._monitor_view.errors_value
        self._accounts_value = self._monitor_view.accounts_value
        self._alias_value = self._monitor_view.alias_value
        self._source_value = self._monitor_view.source_value
        self._export_value = self._monitor_view.export_value
        self._eta_value = self._monitor_view.eta_value
        self._started_value = self._monitor_view.started_value
        return self._monitor_view

    def _show_idle_section(self, section: str) -> None:
        target = str(section or "landing").strip().lower() or "landing"
        view_map = {
            "landing": self._landing_view,
            "activation": self._activation_view,
            "config": self._config_view,
            "results": self._results_view,
        }
        widget = view_map.get(target, self._landing_view)
        self._idle_section = target if target in view_map else "landing"
        self._idle_pages.setCurrentWidget(widget)

    def _make_source_list_widget(self) -> QWidget:
        box = QWidget()
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        layout.addWidget(self._source_list)
        hint = QLabel("Usa una lista ya guardada para iniciar el filtrado.")
        hint.setObjectName("MutedText")
        hint.setWordWrap(True)
        layout.addWidget(hint)
        return box

    def _make_source_file_widget(self, kind: str) -> QWidget:
        box = QWidget()
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)

        path_input = QLineEdit()
        browse_button = QPushButton("Buscar archivo")
        browse_button.setObjectName("SecondaryButton")
        browse_button.clicked.connect(lambda: self._browse_source_file(kind))
        row.addWidget(path_input, 1)
        row.addWidget(browse_button)
        layout.addLayout(row)

        hint = QLabel(
            "El archivo se usa como origen de usernames para esta corrida sin moverte del panel de filtrado."
        )
        hint.setObjectName("MutedText")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        if kind == "csv":
            self._csv_path_input = path_input
        else:
            self._txt_path_input = path_input
        return box

    def _make_source_manual_widget(self) -> QWidget:
        box = QWidget()
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self._manual_input = QPlainTextEdit()
        self._manual_input.setPlaceholderText("@lead1\nlead2\nlead3")
        self._manual_input.setMinimumHeight(150)
        layout.addWidget(self._manual_input)

        hint = QLabel("Pega usernames manualmente, uno por linea.")
        hint.setObjectName("MutedText")
        hint.setWordWrap(True)
        layout.addWidget(hint)
        return box

    def _card(self, title: str, subtitle: str) -> tuple[QFrame, QVBoxLayout]:
        card = QFrame()
        card.setObjectName("SendSetupCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title_label = QLabel(title)
        title_label.setObjectName("SendSetupSectionTitle")
        layout.addWidget(title_label)

        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setObjectName("MutedText")
            subtitle_label.setWordWrap(True)
            layout.addWidget(subtitle_label)
        return card, layout

    def _mini_card(self, title: str) -> tuple[QFrame, QVBoxLayout]:
        card = QFrame()
        card.setObjectName("SectionToolbarCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        label = QLabel(title)
        label.setObjectName("SendSetupSectionTitle")
        layout.addWidget(label)
        return card, layout

    def _make_table_item(self, value: Any, list_id: str) -> QTableWidgetItem:
        item = table_item(value)
        item.setData(Qt.UserRole, list_id)
        return item

    def _set_view_running(self, running: bool) -> None:
        self._view_stack.setCurrentWidget(self._running_view if running else self._idle_view)

    def _available_accounts(self) -> list[str]:
        proxy_backed: list[str] = []
        fallback: list[str] = []
        seen: set[str] = set()
        for record in self._alias_account_rows:
            username = str(record.get("username") or "").strip().lstrip("@")
            key = username.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            if str(record.get("proxy") or "").strip():
                proxy_backed.append(username)
            else:
                fallback.append(username)
        return proxy_backed + fallback

    def _proxy_count(self) -> int:
        proxies = {
            str(record.get("proxy") or "").strip()
            for record in self._alias_account_rows
            if str(record.get("proxy") or "").strip()
        }
        return len(proxies)

    def _compute_concurrency(self) -> int:
        accounts_count = len(self._available_accounts())
        proxy_count = self._proxy_count()
        if accounts_count <= 0:
            return 0
        if proxy_count <= 0:
            return 1
        return min(accounts_count, proxy_count)

    def _browse_source_file(self, kind: str) -> None:
        filters = "CSV (*.csv)" if kind == "csv" else "TXT (*.txt)"
        path = open_dark_file_dialog(self, "Selecciona archivo de leads", filters)
        if not path:
            return
        if kind == "csv":
            self._csv_path_input.setText(path)
        else:
            self._txt_path_input.setText(path)
        self._refresh_activation_summary()

    def _manual_usernames(self) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in self._manual_input.toPlainText().splitlines():
            username = str(raw or "").strip().lstrip("@")
            key = username.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            ordered.append(username)
        return ordered

    def _csv_usernames(self, path: Path) -> list[str]:
        usernames: list[str] = []
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.reader(handle):
                if not row:
                    continue
                username = str(row[0] or "").strip().lstrip("@")
                if username:
                    usernames.append(username)
        return usernames

    def _txt_usernames(self, path: Path) -> list[str]:
        usernames: list[str] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            username = str(raw or "").strip().lstrip("@")
            if username:
                usernames.append(username)
        return usernames

    def _source_payload(self) -> tuple[str, list[str], str]:
        mode = str(self._source_mode.currentData() or "saved").strip()
        if mode == "saved":
            source_name = str(self._source_list.currentData() or self._source_list.currentText() or "").strip()
            if not source_name:
                raise RuntimeError("Selecciona una lista origen.")
            return mode, [], source_name
        if mode == "csv":
            path = Path(str(self._csv_path_input.text() or "").strip())
            if not path.is_file():
                raise RuntimeError("Selecciona un archivo CSV valido.")
            return mode, self._csv_usernames(path), f"Archivo CSV: {path.stem}"
        if mode == "txt":
            path = Path(str(self._txt_path_input.text() or "").strip())
            if not path.is_file():
                raise RuntimeError("Selecciona un archivo TXT valido.")
            return mode, self._txt_usernames(path), f"Archivo TXT: {path.stem}"
        usernames = self._manual_usernames()
        if not usernames:
            raise RuntimeError("Pega usernames manualmente antes de iniciar el filtrado.")
        return mode, usernames, "Manual"

    def _current_run_payload(self) -> dict[str, Any]:
        alias = str(self._account_alias.currentData() or self._account_alias.currentText() or "").strip()
        if not alias:
            raise RuntimeError("Selecciona un alias de cuentas.")
        accounts = self._available_accounts()
        if not accounts:
            raise RuntimeError("El alias de cuentas no tiene cuentas disponibles para ejecutar el filtrado.")
        concurrency = self._compute_concurrency()
        if concurrency <= 0:
            raise RuntimeError("La concurrencia es 0. Asigna proxies al alias antes de iniciar el filtrado.")
        delay_min = float(self._delay_min.value())
        delay_max = max(delay_min, float(self._delay_max.value()))
        return {
            "alias": alias,
            "accounts": accounts,
            "concurrency": concurrency,
            "delay_min": delay_min,
            "delay_max": delay_max,
            "headless": self._browser_mode.currentData(),
            "max_runtime_seconds": int(self._max_runtime_seconds),
        }

    def _effective_filters(self) -> dict[str, Any]:
        return self._config_panel.current_payload()

    def _render_sources(self, payload: dict[str, Any]) -> None:
        current_source = str(payload.get("selected_source") or self._source_list.currentText() or "").strip()
        source_lists = payload.get("source_lists") if isinstance(payload, dict) else []
        self._source_list.blockSignals(True)
        self._source_list.clear()
        for name in source_lists if isinstance(source_lists, list) else []:
            clean_name = str(name or "").strip()
            self._source_list.addItem(clean_name, clean_name)
        source_index = self._source_list.findData(current_source)
        self._source_list.setCurrentIndex(max(0, source_index))
        self._source_list.blockSignals(False)

        current_alias = str(payload.get("selected_account_alias") or self._ctx.state.active_alias).strip()
        account_aliases = payload.get("account_aliases") if isinstance(payload, dict) else []
        self._account_alias.blockSignals(True)
        self._account_alias.clear()
        for alias in account_aliases if isinstance(account_aliases, list) else []:
            clean_alias = str(alias or "").strip()
            self._account_alias.addItem(clean_alias, clean_alias)
        alias_index = self._account_alias.findData(current_alias)
        self._account_alias.setCurrentIndex(max(0, alias_index))
        self._account_alias.blockSignals(False)

        export_alias = str(payload.get("selected_export_alias") or self._export_alias.currentText() or "").strip()
        export_aliases = payload.get("export_aliases") if isinstance(payload, dict) else []
        self._export_alias.blockSignals(True)
        self._export_alias.clear()
        for alias in export_aliases if isinstance(export_aliases, list) else []:
            clean_alias = str(alias or "").strip()
            self._export_alias.addItem(clean_alias, clean_alias)
        if export_alias:
            self._export_alias.setEditText(export_alias)
        elif not self._export_alias.currentText().strip():
            self._export_alias.setEditText(f"{self._ctx.state.active_alias}_filtrados")
        self._export_alias.blockSignals(False)

        self._apply_accounts_rows(payload.get("account_rows") if isinstance(payload, dict) else [])
        self._render_filter_tables(payload)

    def _apply_accounts_rows(self, rows: Any) -> None:
        account_rows = rows if isinstance(rows, list) else []
        self._alias_account_rows = [
            dict(record)
            for record in account_rows
            if isinstance(record, dict) and str(record.get("username") or "").strip()
        ]
        self._computed_concurrency_value = self._compute_concurrency()
        self._concurrency.setText(str(self._computed_concurrency_value))
        self._refresh_activation_summary()

    def _apply_pending_form_payload(self) -> None:
        payload = dict(self._pending_form_payload or {})
        if not payload:
            self._refresh_prepared_state()
            return
        source_list = str(payload.get("source_list") or "").strip()
        if source_list and self._source_mode.currentData() == "saved":
            source_index = self._source_list.findData(source_list)
            if source_index >= 0:
                self._source_list.setCurrentIndex(source_index)
        export_alias = str(payload.get("export_alias") or "").strip()
        if export_alias:
            self._export_alias.setEditText(export_alias)
        account_alias = str(payload.get("account_alias") or payload.get("alias") or "").strip()
        if account_alias:
            alias_index = self._account_alias.findData(account_alias)
            if alias_index >= 0:
                self._account_alias.setCurrentIndex(alias_index)
        run_payload = dict(payload.get("run") or {})
        self._delay_min.setValue(float(run_payload.get("delay_min") or self._delay_min.value()))
        self._delay_max.setValue(float(run_payload.get("delay_max") or self._delay_max.value()))
        self._max_runtime_seconds = max(30, int(run_payload.get("max_runtime_seconds") or self._max_runtime_seconds))
        browser_mode = run_payload.get("headless")
        browser_index = self._browser_mode.findData(browser_mode)
        if browser_index >= 0:
            self._browser_mode.setCurrentIndex(browser_index)
        self._pending_form_payload = None
        self._refresh_prepared_state()

    def _refresh_prepared_state(self) -> None:
        if self._prepared_action == "rerun" and self._prepared_list_id:
            self._prepared_summary.setText(
                f"Resultado listo para reutilizar: {self._prepared_list_id}. Ajusta la configuracion y ejecuta desde esta pantalla."
            )
            self._start_button.setText("Reutilizar filtrado")
            return
        if self._prepared_action == "resume" and self._prepared_list_id:
            self._prepared_summary.setText(
                f"Resultado incompleto listo para reanudar: {self._prepared_list_id}. Ajusta la configuracion y retoma desde aqui."
            )
            self._start_button.setText("Reanudar filtrado")
            return
        self._prepared_summary.setText("Nueva ejecucion lista para configurar.")
        self._start_button.setText("Iniciar filtrado")

    def _apply_navigation_payload(self, payload: Any) -> None:
        data = dict(payload) if isinstance(payload, dict) else {}
        self._pending_result_focus = str(data.get("filter_result_id") or "").strip()
        action = str(data.get("action") or "").strip().lower()
        list_id = str(data.get("list_id") or data.get("resume_list_id") or data.get("rerun_list_id") or "").strip()
        if action not in {"rerun", "resume"}:
            self._prepared_action = ""
            self._prepared_list_id = ""
            self._pending_form_payload = None
            self._refresh_prepared_state()
            return
        self._prepared_action = action
        self._prepared_list_id = list_id
        self._pending_form_payload = dict(data)
        self._refresh_prepared_state()
        self._show_idle_section("activation")

    def _on_account_alias_changed(self) -> None:
        self._request_page_refresh()

    def _on_source_mode_changed(self) -> None:
        current_mode = str(self._source_mode.currentData() or "saved").strip()
        indexes = {"saved": 0, "csv": 1, "txt": 2, "manual": 3}
        self._source_stack.setCurrentIndex(indexes.get(current_mode, 0))
        self._refresh_activation_summary()

    def _source_summary_label(self) -> str:
        mode = str(self._source_mode.currentData() or "saved").strip()
        if mode == "saved":
            return str(self._source_list.currentText() or "-").strip() or "-"
        if mode == "csv":
            return str(self._csv_path_input.text() or "-").strip() or "-"
        if mode == "txt":
            return str(self._txt_path_input.text() or "-").strip() or "-"
        return f"Manual ({len(self._manual_usernames())} usernames)"

    def _refresh_activation_summary(self) -> None:
        accounts_count = len(self._available_accounts())
        proxy_count = self._proxy_count()
        concurrency = self._compute_concurrency()
        self._concurrency.setText(str(concurrency))
        capacity_note = (
            "Sin proxies asignados en este alias. Se usara 1 worker local con rotacion de cuentas."
            if proxy_count <= 0
            else "Concurrencia automatica = min(cuentas del alias, proxies del alias)."
        )
        self._capacity_summary.setText(
            f"Cuentas en alias: {accounts_count}  |  Proxies detectados: {proxy_count}  |  Concurrencia aplicada: {concurrency}\n"
            f"{capacity_note}"
        )
        summary = (
            f"Origen: {self._source_mode.currentText()}  |  "
            f"Detalle: {self._source_summary_label()}  |  "
            f"Alias de guardado: {self._export_alias.currentText() or '-'}  |  "
            f"Alias de cuentas: {self._account_alias.currentText() or '-'}"
        )
        if self._prepared_action == "rerun" and self._prepared_list_id:
            summary += f"  |  Reutilizando: {self._prepared_list_id}"
        elif self._prepared_action == "resume" and self._prepared_list_id:
            summary += f"  |  Reanudando: {self._prepared_list_id}"
        self._activation_summary.setText(summary)

    def _populate_table(self, table: QTableWidget, rows: list[dict[str, Any]], *, include_pending: bool) -> None:
        table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            list_id = str(row.get("id") or "")
            source_name = str(row.get("source_list") or row.get("export_alias") or list_id)
            values = [
                source_name,
                safe_int(row.get("processed")),
                safe_int(row.get("qualified")),
                safe_int(row.get("discarded")),
            ]
            if include_pending:
                values.append(safe_int(row.get("pending")))
            for column, value in enumerate(values):
                table.setItem(row_index, column, self._make_table_item(value, list_id))

    def _selected_list_id(self, table_name: str) -> str:
        table = self._completed_table if table_name == "completed" else self._incomplete_table
        row = table.currentRow()
        if row < 0:
            return ""
        item = table.item(row, 0)
        return str(item.data(Qt.UserRole) or "") if item is not None else ""

    def _restore_table_selection(self, table: QTableWidget, target_list_id: str) -> None:
        if not target_list_id:
            return
        for row_index in range(table.rowCount()):
            item = table.item(row_index, 0)
            if item is not None and str(item.data(Qt.UserRole) or "") == target_list_id:
                table.selectRow(row_index)
                return

    def _select_table(self, table_name: str) -> None:
        if table_name == "completed" and self._completed_table.currentRow() >= 0:
            self._incomplete_table.clearSelection()
        if table_name == "incomplete" and self._incomplete_table.currentRow() >= 0:
            self._completed_table.clearSelection()
        self._request_detail_refresh(self._selected_list_id(table_name))

    def _apply_detail_snapshot(self, payload: dict[str, Any]) -> None:
        row = payload.get("row") if isinstance(payload, dict) else {}
        preview_rows = payload.get("preview_rows") if isinstance(payload, dict) else []
        if not isinstance(row, dict):
            row = {}
        if not isinstance(preview_rows, list):
            preview_rows = []
        self._detail_summary.setText(
            f"Lista: {row.get('source_list') or row.get('id')}  |  "
            f"Alias export: {row.get('export_alias') or '-'}  |  "
            f"Procesadas: {row.get('processed', 0)}  |  "
            f"Pendientes: {row.get('pending', 0)}"
        )
        lines: list[str] = []
        for item in preview_rows:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username") or "").strip()
            result = str(item.get("result") or item.get("status") or "PENDING").strip()
            reason = str(item.get("reason") or "-").strip()
            account = str(item.get("account") or "-").strip()
            lines.append(f"@{username} | {result} | {reason} | cuenta: {account}")
        self._detail_preview.setPlainText("\n".join(lines) if lines else "Sin resultados procesados todavia.")

    def _request_detail_refresh(self, list_id: str) -> None:
        if not list_id:
            self._detail_summary.setText("Selecciona una lista completa o incompleta.")
            self._detail_preview.clear()
            return
        if self._detail_snapshot_loading:
            return
        self._detail_snapshot_loading = True
        self._detail_summary.setText("Cargando detalle...")
        self._detail_snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_filter_detail_snapshot(self._ctx.services, list_id=list_id),
            on_success=self._on_detail_snapshot_loaded,
            on_error=self._on_detail_snapshot_failed,
        )

    def _render_filter_tables(self, payload: dict[str, Any]) -> None:
        completed = payload.get("completed") if isinstance(payload, dict) else []
        incomplete = payload.get("incomplete") if isinstance(payload, dict) else []
        self._filter_rows_by_id = {}
        for row in list(completed if isinstance(completed, list) else []) + list(incomplete if isinstance(incomplete, list) else []):
            if not isinstance(row, dict):
                continue
            list_id = str(row.get("id") or "").strip()
            if list_id:
                self._filter_rows_by_id[list_id] = dict(row)
        selected_completed = str(payload.get("selected_completed_id") or self._selected_list_id("completed")).strip()
        selected_incomplete = str(payload.get("selected_incomplete_id") or self._selected_list_id("incomplete")).strip()
        self._populate_table(self._completed_table, completed if isinstance(completed, list) else [], include_pending=False)
        self._populate_table(self._incomplete_table, incomplete if isinstance(incomplete, list) else [], include_pending=True)
        self._restore_table_selection(self._completed_table, selected_completed)
        self._restore_table_selection(self._incomplete_table, selected_incomplete)
        self._filter_lists_summary.setText(
            f"Listas completas: {len(completed) if isinstance(completed, list) else 0}  |  "
            f"Listas incompletas: {len(incomplete) if isinstance(incomplete, list) else 0}"
        )
        focus = self._pending_result_focus or selected_completed or selected_incomplete
        if focus:
            self._request_detail_refresh(focus)
        elif not self._selected_list_id("completed") and not self._selected_list_id("incomplete"):
            self._detail_summary.setText("Selecciona una lista completa o incompleta.")
            self._detail_preview.clear()
        self._pending_result_focus = ""

    def _request_page_refresh(self) -> None:
        if self._page_snapshot_loading:
            return
        pending_payload = dict(self._pending_form_payload or {})
        pending_run = dict(pending_payload.get("run") or {})
        self._page_snapshot_loading = True
        self._filter_lists_summary.setText("Cargando estado de filtrado...")
        self._page_snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_filter_runner_snapshot(
                self._ctx.services,
                active_alias=self._ctx.state.active_alias,
                current_source=str(
                    pending_payload.get("source_list")
                    or self._source_list.currentData()
                    or self._source_list.currentText()
                    or ""
                ).strip(),
                current_account_alias=str(
                    pending_payload.get("account_alias")
                    or pending_payload.get("alias")
                    or pending_run.get("alias")
                    or self._account_alias.currentData()
                    or self._ctx.state.active_alias
                ).strip(),
                current_export_alias=str(
                    pending_payload.get("export_alias")
                    or self._export_alias.currentText()
                    or ""
                ).strip(),
            ),
            on_success=self._on_page_snapshot_loaded,
            on_error=self._on_page_snapshot_failed,
        )

    def _apply_page_snapshot(self, payload: dict[str, Any]) -> None:
        self._render_sources(payload)
        self._apply_pending_form_payload()

    def _on_page_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._page_snapshot_request_id:
            return
        self._page_snapshot_loading = False
        self._apply_page_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_page_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._page_snapshot_request_id:
            return
        self._page_snapshot_loading = False
        self._filter_lists_summary.setText(f"No se pudo cargar el estado de filtrado: {error.message}")

    def _on_detail_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._detail_snapshot_request_id:
            return
        self._detail_snapshot_loading = False
        self._apply_detail_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_detail_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._detail_snapshot_request_id:
            return
        self._detail_snapshot_loading = False
        self._detail_summary.setText(f"No se pudo cargar el detalle: {error.message}")

    def _begin_task_capture(
        self,
        list_id: str,
        *,
        baseline_processed: int,
        run_payload: dict[str, Any],
    ) -> None:
        self._running_list_id = list_id
        self._running_run_payload = dict(run_payload or {})
        self._running_started_at = time.monotonic()
        self._running_started_label = time.strftime("%H:%M:%S")
        self._running_processed_baseline = max(0, int(baseline_processed or 0))
        self._last_result = {}
        self._log_buffer = ""
        self._stop_requested = False
        self._monitor_view.reset()
        self._execution_status.setText("Inicializando filtrado...")
        pause_filtering = getattr(self._ctx.services.leads, "pause_filtering", None)
        self._monitor_view.set_pause_available(callable(pause_filtering))
        self._monitor_view.set_stop_pending(False)
        self._set_view_running(True)
        if not self._timer.isActive():
            self._timer.start()
        self._refresh_execution_panel()

    def _start_filter_task(
        self,
        list_id: str,
        *,
        baseline_processed: int,
        run_payload: dict[str, Any],
    ) -> None:
        self._begin_task_capture(
            list_id,
            baseline_processed=baseline_processed,
            run_payload=run_payload,
        )

        def _runner() -> dict[str, Any]:
            result = self._ctx.services.leads.execute_filter_list(list_id)
            self._last_result = dict(result)
            return result

        try:
            self._ctx.tasks.start_task(
                "leads_filter",
                _runner,
                metadata={"alias": str(self._running_run_payload.get("alias") or "").strip()},
                isolated_stop=True,
            )
        except Exception:
            self._running_list_id = ""
            self._running_run_payload = {}
            self._running_started_at = 0.0
            self._running_processed_baseline = 0
            if self._timer.isActive():
                self._timer.stop()
            self._set_view_running(False)
            raise

    def _start_run(self) -> None:
        if self._prepared_action == "rerun":
            self._rerun_completed()
            return
        if self._prepared_action == "resume":
            self._resume_incomplete()
            return
        self._start_new_run()

    def _start_new_run(self) -> None:
        if self._ctx.tasks.is_running("leads_filter"):
            show_panel_error(self, "Ya hay un filtrado en ejecucion.")
            return
        export_alias = str(self._export_alias.currentText() or "").strip()
        try:
            mode, usernames, source_label = self._source_payload()
            run_payload = self._current_run_payload()
            filters = self._effective_filters()
            if mode == "saved":
                created = self._ctx.services.leads.create_filter_list_from_source(
                    source_label,
                    export_alias=export_alias,
                    filters=filters,
                    run=run_payload,
                )
            else:
                created = self._ctx.services.leads.create_filter_list(
                    usernames,
                    export_alias=export_alias,
                    filters=filters,
                    run=run_payload,
                    source_list=source_label,
                )
            run_payload["source_list"] = str(created.get("source_list") or source_label)
            run_payload["export_alias"] = str(created.get("export_alias") or export_alias)
            self._request_page_refresh()
            self._start_filter_task(
                str(created.get("id") or ""),
                baseline_processed=0,
                run_payload=run_payload,
            )
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo iniciar el filtrado. Ver logs para mas detalles.")

    def _prepare_result_action(self, action: str) -> None:
        table_name = "completed" if str(action or "").strip().lower() == "rerun" else "incomplete"
        list_id = self._selected_list_id(table_name)
        if not list_id:
            show_panel_error(self, "Selecciona un resultado de filtrado.")
            return
        row = dict(self._filter_rows_by_id.get(list_id) or {})
        if not row:
            show_panel_error(self, "No se pudo preparar el resultado seleccionado.")
            return
        run_payload = dict(row.get("run") or {})
        payload = {
            "action": "rerun" if table_name == "completed" else "resume",
            "list_id": list_id,
            "source_list": str(row.get("source_list") or ""),
            "export_alias": str(row.get("export_alias") or ""),
            "account_alias": str(run_payload.get("alias") or ""),
            "run": run_payload,
        }
        self._apply_navigation_payload(payload)
        self._request_page_refresh()

    def _rerun_completed(self) -> None:
        if self._ctx.tasks.is_running("leads_filter"):
            show_panel_error(self, "Ya hay un filtrado en ejecucion.")
            return
        list_id = str(self._prepared_list_id or self._selected_list_id("completed")).strip()
        if not list_id:
            show_panel_error(self, "Selecciona o prepara una lista completa.")
            return
        try:
            run_payload = self._current_run_payload()
            created = self._ctx.services.leads.restart_filter_list(
                list_id,
                filters=self._effective_filters(),
                run=run_payload,
                export_alias=str(self._export_alias.currentText() or "").strip(),
            )
            run_payload["source_list"] = str(created.get("source_list") or "")
            run_payload["export_alias"] = str(created.get("export_alias") or self._export_alias.currentText() or "")
            self._request_page_refresh()
            self._start_filter_task(
                str(created.get("id") or ""),
                baseline_processed=0,
                run_payload=run_payload,
            )
            self._prepared_action = ""
            self._prepared_list_id = ""
            self._refresh_prepared_state()
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo reutilizar la lista. Ver logs para mas detalles.")

    def _resume_incomplete(self) -> None:
        if self._ctx.tasks.is_running("leads_filter"):
            show_panel_error(self, "Ya hay un filtrado en ejecucion.")
            return
        list_id = str(self._prepared_list_id or self._selected_list_id("incomplete")).strip()
        if not list_id:
            show_panel_error(self, "Selecciona o prepara una lista incompleta.")
            return
        try:
            run_payload = self._current_run_payload()
            updated = self._ctx.services.leads.update_filter_list_settings(
                list_id,
                filters=self._effective_filters(),
                run=run_payload,
                export_alias=str(self._export_alias.currentText() or "").strip(),
            )
            run_payload["source_list"] = str(updated.get("source_list") or "")
            run_payload["export_alias"] = str(updated.get("export_alias") or self._export_alias.currentText() or "")
            self._request_page_refresh()
            self._start_filter_task(
                str(updated.get("id") or ""),
                baseline_processed=safe_int(updated.get("processed")),
                run_payload=run_payload,
            )
            self._prepared_action = ""
            self._prepared_list_id = ""
            self._refresh_prepared_state()
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo reanudar el filtrado. Ver logs para mas detalles.")

    def _delete_selected(self, table_name: str) -> None:
        list_id = self._selected_list_id(table_name)
        if not list_id:
            show_panel_error(self, "Selecciona una lista.")
            return
        try:
            self._ctx.services.leads.delete_filter_list(list_id)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo eliminar la lista de filtrado. Ver logs para mas detalles.")
            return
        self._pending_result_focus = ""
        self._request_page_refresh()
        self._request_detail_refresh("")
        set_panel_status(self, "Lista de filtrado eliminada.")

    def _format_duration(self, seconds: float) -> str:
        total = max(0, int(seconds))
        hours, remainder = divmod(total, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def _refresh_running_summary(self, row: dict[str, Any]) -> None:
        source_list = str(row.get("source_list") or self._running_run_payload.get("source_list") or "-").strip() or "-"
        export_alias = str(row.get("export_alias") or self._running_run_payload.get("export_alias") or "-").strip() or "-"
        account_alias = str(self._running_run_payload.get("alias") or "-").strip() or "-"
        accounts_count = len(self._running_run_payload.get("accounts") or [])
        delay_min = float(self._running_run_payload.get("delay_min") or 0.0)
        delay_max = float(self._running_run_payload.get("delay_max") or 0.0)
        concurrency = int(self._running_run_payload.get("concurrency") or 0)
        active_accounts = min(accounts_count, concurrency) if concurrency > 0 else 0
        headless = self._running_run_payload.get("headless")
        mode_label = browser_mode_label(headless)
        self._alias_value.setText(account_alias)
        self._source_value.setText(source_list)
        self._export_value.setText(export_alias)
        self._accounts_value.setText(str(active_accounts))
        self._started_value.setText(self._running_started_label or "-")
        self._running_summary.setText(
            f"Alias usado: {account_alias}  |  Lista origen: {source_list}  |  Alias de guardado: {export_alias}\n"
            f"Workers activos: {active_accounts}  |  Delay: {delay_min:.1f}s - {delay_max:.1f}s  |  "
            f"Concurrencia: {concurrency}  |  Modo: {mode_label}"
        )
        self._running_summary.show()

    def _refresh_execution_panel(self) -> None:
        if not self._running_list_id:
            self._monitor_view.reset()
            return
        if self._execution_snapshot_loading:
            return
        self._execution_snapshot_loading = True
        self._execution_snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_filter_execution_snapshot(self._ctx.services, list_id=self._running_list_id),
            on_success=self._on_execution_snapshot_loaded,
            on_error=self._on_execution_snapshot_failed,
        )

    def _apply_execution_row(self, row: dict[str, Any]) -> None:
        if not isinstance(row, dict):
            return

        processed = safe_int(row.get("processed"))
        qualified = safe_int(row.get("qualified"))
        discarded = safe_int(row.get("discarded"))
        pending = safe_int(row.get("pending"))
        total = safe_int(row.get("total"))
        errors = safe_int(row.get("errors"))
        elapsed = max(0.0, time.monotonic() - self._running_started_at)
        run_processed = max(0, processed - self._running_processed_baseline)
        if run_processed > 0 and pending > 0:
            eta_label = self._format_duration((elapsed / float(run_processed)) * pending)
        else:
            eta_label = "-"

        self._total_value.setText(str(total))
        self._processed_value.setText(str(processed))
        self._qualified_value.setText(str(qualified))
        self._discarded_value.setText(str(discarded))
        self._pending_value.setText(str(pending))
        self._errors_value.setText(str(errors))
        accounts_count = len(self._running_run_payload.get("accounts") or [])
        concurrency = int(self._running_run_payload.get("concurrency") or 0)
        active_accounts = min(accounts_count, concurrency) if concurrency > 0 else 0
        self._accounts_value.setText(str(active_accounts))
        self._alias_value.setText(str(self._running_run_payload.get("alias") or "-"))
        self._source_value.setText(str(row.get("source_list") or self._running_run_payload.get("source_list") or "-"))
        self._eta_value.setText(eta_label)
        progress_value = int((processed / total) * 100) if total > 0 else 0
        self._monitor_view.progress_bar.setValue(max(0, min(100, progress_value)))
        self._monitor_view.progress_detail_label.setText(
            f"{processed} de {total} perfiles procesados  |  Calificados: {qualified}  |  "
            f"Descartados: {discarded}  |  Pendientes: {pending}  |  ETA: {eta_label}"
        )
        if self._stop_requested:
            current_status = "Deteniendo filtrado y cerrando workers..."
        elif pending > 0:
            current_status = "Procesando perfiles"
        elif total > 0:
            current_status = "Finalizando resultados"
        else:
            current_status = "Preparando ejecucion"
        self._execution_status.setText(current_status)
        self._refresh_running_summary(row)

    def _append_monitor_log_line(self, line: str) -> None:
        formatted = format_filter_log_line(line)
        if not formatted:
            return
        self._log_box.appendPlainText(formatted)
        self._log_box.moveCursor(QTextCursor.End)

    def _on_execution_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._execution_snapshot_request_id:
            return
        self._execution_snapshot_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        self._apply_execution_row(dict(data.get("row") or {}))

    def _on_execution_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._execution_snapshot_request_id:
            return
        self._execution_snapshot_loading = False
        self._execution_status.setText(f"No se pudo actualizar el progreso: {error.message}")

    def _flush_log_buffer(self) -> None:
        while "\n" in self._log_buffer:
            line, self._log_buffer = self._log_buffer.split("\n", 1)
            if not self._ctx.tasks.is_running("leads_filter") and not self._running_list_id:
                continue
            self._append_monitor_log_line(line)

    def _on_log_added(self, chunk: str) -> None:
        if not self._running_list_id and not self._ctx.tasks.is_running("leads_filter"):
            return
        self._log_buffer += str(chunk or "")
        self._flush_log_buffer()

    def _pause_filtering(self) -> None:
        pause_filtering = getattr(self._ctx.services.leads, "pause_filtering", None)
        if not callable(pause_filtering):
            return
        try:
            pause_filtering("pause requested from leads runner panel")
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo pausar el filtrado. Ver logs para mas detalles.")
            return
        self._monitor_view.pause_button.setEnabled(False)
        self._execution_status.setText("Pausando filtrado...")

    def _stop_filtering(self) -> None:
        if self._stop_requested:
            return
        if not self._running_list_id and not self._ctx.tasks.is_running("leads_filter"):
            return
        self._stop_requested = True
        self._monitor_view.pause_button.setEnabled(False)
        self._monitor_view.set_stop_pending(True)
        try:
            self._ctx.services.leads.stop_filtering(
                "stop requested from leads runner panel",
                task_runner=self._ctx.tasks,
            )
        except Exception as exc:
            self._stop_requested = False
            self._monitor_view.set_stop_pending(False)
            show_panel_exception(self, exc, "No se pudo detener el filtrado. Ver logs para mas detalles.")
            return
        self._append_monitor_log_line("Solicitud de detencion enviada. Esperando cierre seguro de workers activos.")
        self._execution_status.setText("Deteniendo filtrado y cerrando workers...")

    def _resolve_stopped_run(self, list_id: str, result: dict[str, Any], running_payload: dict[str, Any]) -> dict[str, Any]:
        if not list_id:
            return {}
        default_alias = str(
            result.get("export_alias")
            or running_payload.get("export_alias")
            or self._export_alias.currentText()
            or "leads_filtrados"
        ).strip()
        dialog = _StoppedRunDialog(result, default_alias=default_alias, parent=self)
        dialog.exec()
        try:
            resolution = self._ctx.services.leads.finalize_stopped_filter_list(
                list_id,
                action=dialog.choice(),
                export_alias=dialog.export_alias(),
            )
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo resolver la lista detenida. Ver logs para mas detalles.")
            return {}

        action = str(resolution.get("action") or "keep")
        if action == "export":
            set_panel_status(
                self,
                f"Se guardaron {safe_int(resolution.get('exported'))} leads calificados en '{resolution.get('alias')}'.",
            )
        elif action == "delete":
            set_panel_status(self, "La lista parcial se elimino por completo.")
        else:
            set_panel_status(self, "La lista parcial quedo guardada como incompleta.")
        return dict(resolution)

    def _on_task_finished(self, task_name: str, ok: bool, message: str) -> None:
        if task_name != "leads_filter":
            return

        self._flush_log_buffer()
        if self._timer.isActive():
            self._timer.stop()

        list_id = self._running_list_id
        running_payload = dict(self._running_run_payload)
        result = dict(self._last_result)
        if list_id and not result:
            try:
                row = self._ctx.services.leads.find_filter_list(list_id)
                result = {
                    "list_id": list_id,
                    "source_list": str(row.get("source_list") or ""),
                    "export_alias": str(row.get("export_alias") or ""),
                    "stopped": not ok,
                    "processed": safe_int(row.get("processed")),
                    "qualified": safe_int(row.get("qualified")),
                    "discarded": safe_int(row.get("discarded")),
                    "pending": safe_int(row.get("pending")),
                }
            except Exception:
                result = {}

        elapsed = max(0.0, time.monotonic() - self._running_started_at) if self._running_started_at else 0.0
        result["elapsed_label"] = self._format_duration(elapsed)

        self._stop_requested = False
        self._running_list_id = ""
        self._running_run_payload = {}
        self._running_started_at = 0.0
        self._running_started_label = ""
        self._running_processed_baseline = 0
        self._monitor_view.set_stop_pending(False)
        self._set_view_running(False)

        if message:
            self._execution_status.setText(message)
        elif ok:
            self._execution_status.setText("Filtrado finalizado.")
        else:
            self._execution_status.setText("El filtrado finalizo con errores.")

        final_resolution: dict[str, Any] = {}
        if result.get("stopped"):
            final_resolution = self._resolve_stopped_run(str(result.get("list_id") or list_id), result, running_payload)
        elif result:
            alias = str(result.get("export_alias") or running_payload.get("export_alias") or "").strip()
            note = ""
            if ok and alias:
                note = f"Los leads calificados se guardaron en el alias '{alias}'."
            headline = "Filtrado finalizado" if ok else "Filtrado con errores"
            dialog = _RunSummaryDialog(result, headline=headline, note=note, parent=self)
            dialog.exec()

        target_list_id = str(result.get("list_id") or list_id).strip()
        if str(final_resolution.get("action") or "").strip().lower() == "delete":
            target_list_id = ""
        self._pending_result_focus = target_list_id
        self._show_idle_section("results")
        self.refresh_page()

    def refresh_page(self) -> None:
        self._request_page_refresh()
        if self._running_list_id:
            self._set_view_running(True)
            self._refresh_execution_panel()
        else:
            self._set_view_running(False)
            self._show_idle_section(self._idle_section)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._apply_navigation_payload(payload)
        if not self._config_initialized:
            self._config_panel.load_config()
            self._config_initialized = True
        if self._running_list_id:
            self._set_view_running(True)
        elif self._prepared_action in {"rerun", "resume"}:
            self._show_idle_section("activation")
        elif payload is None:
            self._show_idle_section("landing")
        self.refresh_page()
        if self._running_list_id and not self._timer.isActive():
            self._timer.start()

    def on_navigate_from(self) -> None:
        if self._timer.isActive():
            self._timer.stop()
