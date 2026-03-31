from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext, table_item
from gui.query_runner import QueryError
from gui.snapshot_queries import build_leads_templates_snapshot

from .common import configure_data_table, set_panel_status, show_panel_error, show_panel_exception, template_variants


class LeadsTemplatesPanel(QWidget):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._selected_template_id = ""
        self._rows_by_id: dict[str, dict[str, Any]] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_refresh_pending = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter, 1)

        left_card = QFrame()
        left_card.setObjectName("SendSetupCard")
        left_layout = QVBoxLayout(left_card)
        left_layout.setContentsMargins(16, 16, 16, 16)
        left_layout.setSpacing(8)
        self._summary = QLabel("")
        self._summary.setObjectName("MutedText")
        self._summary.setWordWrap(True)
        left_layout.addWidget(self._summary)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Plantilla", "Variaciones"])
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.itemSelectionChanged.connect(self._load_selected)
        configure_data_table(self._table)
        left_layout.addWidget(self._table, 1)

        left_actions = QHBoxLayout()
        left_actions.setContentsMargins(0, 0, 0, 0)
        left_actions.setSpacing(8)
        new_button = QPushButton("Nueva plantilla")
        new_button.setObjectName("SecondaryButton")
        new_button.clicked.connect(self._new_template)
        refresh_button = QPushButton("Recargar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_page)
        left_actions.addWidget(new_button)
        left_actions.addWidget(refresh_button)
        left_actions.addStretch(1)
        left_layout.addLayout(left_actions)
        splitter.addWidget(left_card)

        right_card = QFrame()
        right_card.setObjectName("SendSetupCard")
        right_layout = QGridLayout(right_card)
        right_layout.setContentsMargins(16, 16, 16, 16)
        right_layout.setHorizontalSpacing(8)
        right_layout.setVerticalSpacing(8)

        self._name_input = QLineEdit()
        self._messages_input = QPlainTextEdit()
        self._messages_input.setPlaceholderText(
            "Hola {nombre}\nHola {nombre}, vi tu perfil\nHey {nombre}, como estas?"
        )
        self._stats_label = QLabel("")
        self._stats_label.setObjectName("MutedText")
        self._stats_label.setWordWrap(True)
        self._messages_input.textChanged.connect(self._refresh_variant_stats)

        add_line_button = QPushButton("Agregar linea")
        add_line_button.setObjectName("SecondaryButton")
        add_line_button.clicked.connect(self._add_line)
        save_button = QPushButton("Guardar plantilla")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self._save_template)
        delete_button = QPushButton("Eliminar plantilla")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_template)

        right_layout.addWidget(QLabel("Nombre plantilla"), 0, 0)
        right_layout.addWidget(self._name_input, 0, 1, 1, 3)
        right_layout.addWidget(QLabel("Mensajes"), 1, 0, Qt.AlignTop)
        right_layout.addWidget(self._messages_input, 1, 1, 1, 3)
        right_layout.addWidget(self._stats_label, 2, 1, 1, 3)
        right_layout.addWidget(add_line_button, 3, 1)
        right_layout.addWidget(save_button, 3, 2)
        right_layout.addWidget(delete_button, 3, 3)
        splitter.addWidget(right_card)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)

    def _refresh_variant_stats(self) -> None:
        variants = template_variants(self._messages_input.toPlainText())
        preview = variants[0] if variants else "-"
        self._stats_label.setText(
            f"Variaciones detectadas: {len(variants)}  |  Vista previa: {preview}"
        )

    def _load_selected(self) -> None:
        row = self._table.currentRow()
        if row < 0:
            return
        item = self._table.item(row, 0)
        template_id = str(item.data(Qt.UserRole) if item else "").strip()
        payload = self._rows_by_id.get(template_id)
        if not payload:
            return
        self._selected_template_id = template_id
        self._name_input.setText(str(payload.get("name") or ""))
        self._messages_input.setPlainText(str(payload.get("text") or ""))
        self._refresh_variant_stats()

    def _new_template(self) -> None:
        self._selected_template_id = ""
        self._name_input.clear()
        self._messages_input.clear()
        self._table.blockSignals(True)
        self._table.clearSelection()
        self._table.setCurrentCell(-1, -1)
        self._table.blockSignals(False)
        self._refresh_variant_stats()

    def _add_line(self) -> None:
        text = self._messages_input.toPlainText().rstrip()
        if text:
            text += "\n"
        self._messages_input.setPlainText(text)
        cursor = self._messages_input.textCursor()
        cursor.movePosition(cursor.End)
        self._messages_input.setTextCursor(cursor)
        self._messages_input.setFocus()

    def _save_template(self) -> None:
        name = str(self._name_input.text() or "").strip()
        text = str(self._messages_input.toPlainText() or "").strip()
        if not name:
            show_panel_error(self, "Ingresa el nombre de la plantilla.")
            return
        if not template_variants(text):
            show_panel_error(self, "Ingresa al menos una linea de mensaje.")
            return
        try:
            saved = self._ctx.services.leads.upsert_template(
                name,
                text,
                template_id=self._selected_template_id,
            )
            self._selected_template_id = str(saved.get("id") or "")
            self.refresh_page()
            set_panel_status(self, f"Plantilla '{name}' guardada.")
        except Exception as exc:
            show_panel_exception(exc=exc, widget=self, user_message="No se pudo guardar la plantilla. Ver logs para mas detalles.")

    def _delete_template(self) -> None:
        target_id = str(self._selected_template_id or "").strip()
        if not target_id:
            show_panel_error(self, "Selecciona una plantilla.")
            return
        target_name = str(self._rows_by_id.get(target_id, {}).get("name") or self._name_input.text() or "").strip()
        try:
            deleted = self._ctx.services.leads.delete_template(target_id)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo eliminar la plantilla. Ver logs para mas detalles.")
            return
        if not deleted:
            show_panel_error(self, "No se encontro la plantilla seleccionada.")
            return
        self._new_template()
        self.refresh_page()
        set_panel_status(self, f"Plantilla '{target_name or target_id}' eliminada.")

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            self._snapshot_refresh_pending = True
            return
        self._snapshot_loading = True
        self._snapshot_refresh_pending = False
        self._summary.setText("Cargando plantillas...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_templates_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        rows = payload.get("rows") if isinstance(payload, dict) else []
        current_id = str(self._selected_template_id or "").strip()
        self._rows_by_id = {
            str(row.get("id") or "").strip(): dict(row)
            for row in rows
            if isinstance(row, dict) and str(row.get("id") or "").strip()
        }
        self._table.blockSignals(True)
        self._table.setRowCount(len(rows))
        selected_row = -1
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            template_id = str(row.get("id") or "").strip()
            name = str(row.get("name") or "")
            name_item = table_item(name)
            name_item.setData(Qt.UserRole, template_id)
            self._table.setItem(row_index, 0, name_item)
            self._table.setItem(row_index, 1, table_item(row.get("variant_count", 0)))
            if current_id and template_id == current_id:
                selected_row = row_index
        self._summary.setText(str(payload.get("summary") or "").strip())
        if selected_row >= 0:
            self._table.selectRow(selected_row)
        else:
            self._table.clearSelection()
        self._table.blockSignals(False)
        if selected_row >= 0:
            self._load_selected()
        else:
            self._refresh_variant_stats()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})
        if self._snapshot_refresh_pending:
            self.refresh_page()

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._summary.setText(f"No se pudieron cargar las plantillas: {error.message}")
        if self._snapshot_refresh_pending:
            self.refresh_page()
