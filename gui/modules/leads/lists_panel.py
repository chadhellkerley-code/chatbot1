from __future__ import annotations

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
from gui.snapshot_queries import build_leads_lists_snapshot

from .common import configure_data_table, set_panel_status, show_panel_error, show_panel_exception


class LeadsListsPanel(QWidget):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._rows_by_name: dict[str, dict[str, object]] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._pending_list_name = ""

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter, 1)

        editor_card = QFrame()
        editor_card.setObjectName("SendSetupCard")
        editor_layout = QGridLayout(editor_card)
        editor_layout.setContentsMargins(16, 16, 16, 16)
        editor_layout.setHorizontalSpacing(8)
        editor_layout.setVerticalSpacing(8)

        editor_title = QLabel("Crear o editar lista")
        editor_title.setObjectName("SendSetupSectionTitle")
        editor_layout.addWidget(editor_title, 0, 0, 1, 4)

        self._summary = QLabel(
            "Pega usernames manualmente, agrega nuevos bloques y guarda la lista cuando termine la edicion."
        )
        self._summary.setObjectName("MutedText")
        self._summary.setWordWrap(True)
        editor_layout.addWidget(self._summary, 1, 0, 1, 4)

        self._name_input = QLineEdit()
        self._usernames_input = QPlainTextEdit()
        self._usernames_input.setPlaceholderText("@lead1\nlead2\nlead3")
        self._usernames_input.setMinimumHeight(340)
        self._stats_label = QLabel("")
        self._stats_label.setObjectName("MutedText")
        self._stats_label.setWordWrap(True)
        self._usernames_input.textChanged.connect(self._refresh_stats)

        append_button = QPushButton("Agregar usernames")
        append_button.setObjectName("SecondaryButton")
        append_button.clicked.connect(self._append_list)

        save_button = QPushButton("Guardar lista")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self._save_list)

        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._new_list)

        editor_layout.addWidget(QLabel("Nombre de lista"), 2, 0)
        editor_layout.addWidget(self._name_input, 2, 1, 1, 3)
        editor_layout.addWidget(QLabel("Usernames"), 3, 0, Qt.AlignTop)
        editor_layout.addWidget(self._usernames_input, 3, 1, 1, 3)
        editor_layout.addWidget(self._stats_label, 4, 1, 1, 3)
        editor_layout.addWidget(append_button, 5, 1)
        editor_layout.addWidget(save_button, 5, 2)
        editor_layout.addWidget(clear_button, 5, 3)
        splitter.addWidget(editor_card)

        lists_card = QFrame()
        lists_card.setObjectName("SendSetupCard")
        lists_layout = QVBoxLayout(lists_card)
        lists_layout.setContentsMargins(16, 16, 16, 16)
        lists_layout.setSpacing(10)

        title = QLabel("Listas existentes")
        title.setObjectName("SendSetupSectionTitle")
        lists_layout.addWidget(title)

        self._table_summary = QLabel("")
        self._table_summary.setObjectName("MutedText")
        self._table_summary.setWordWrap(True)
        lists_layout.addWidget(self._table_summary)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Lista", "Cantidad"])
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.itemSelectionChanged.connect(self._load_selected)
        configure_data_table(self._table)
        lists_layout.addWidget(self._table, 1)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)

        load_button = QPushButton("Cargar lista")
        load_button.setObjectName("SecondaryButton")
        load_button.clicked.connect(self._load_selected)

        delete_button = QPushButton("Eliminar lista")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_selected)

        refresh_button = QPushButton("Recargar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_page)

        actions.addWidget(load_button)
        actions.addWidget(delete_button)
        actions.addWidget(refresh_button)
        actions.addStretch(1)
        lists_layout.addLayout(actions)
        splitter.addWidget(lists_card)

        splitter.setStretchFactor(0, 5)
        splitter.setStretchFactor(1, 4)

        self._refresh_stats()

    def _new_list(self) -> None:
        self._pending_list_name = ""
        self._table.clearSelection()
        self._name_input.clear()
        self._usernames_input.clear()
        self._refresh_stats()

    def _selected_list(self) -> str:
        row = self._table.currentRow()
        if row < 0:
            return ""
        item = self._table.item(row, 0)
        return str(item.text() if item else "").strip()

    def _usernames_payload(self) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in self._usernames_input.toPlainText().splitlines():
            username = str(raw or "").strip().lstrip("@")
            key = username.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            ordered.append(username)
        return ordered

    def _refresh_stats(self) -> None:
        usernames = self._usernames_payload()
        preview = usernames[:3]
        preview_text = ", ".join(f"@{item}" for item in preview) if preview else "-"
        self._stats_label.setText(
            f"Usernames cargados: {len(usernames)}  |  Vista previa: {preview_text}"
        )

    def _load_selected(self) -> None:
        list_name = self._selected_list()
        if not list_name:
            return
        payload = self._rows_by_name.get(list_name.lower()) or {}
        usernames = payload.get("usernames") if isinstance(payload, dict) else []
        if not isinstance(usernames, list):
            usernames = []
        self._name_input.setText(list_name)
        self._usernames_input.setPlainText("\n".join(usernames))
        self._refresh_stats()

    def set_navigation_payload(self, payload: object) -> None:
        data = dict(payload) if isinstance(payload, dict) else {}
        self._pending_list_name = str(data.get("list_name") or "").strip()

    def _save_list(self) -> None:
        name = str(self._name_input.text() or "").strip()
        usernames = self._usernames_payload()
        if not name:
            show_panel_error(self, "Ingresa un nombre de lista.")
            return
        try:
            self._ctx.services.leads.save_list(name, usernames)
            self._pending_list_name = name
            self.refresh_page()
            set_panel_status(self, f"Lista '{name}' guardada.")
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo guardar la lista. Ver logs para mas detalles.")

    def _append_list(self) -> None:
        name = str(self._name_input.text() or "").strip()
        usernames = self._usernames_payload()
        if not name:
            show_panel_error(self, "Ingresa un nombre de lista.")
            return
        if not usernames:
            show_panel_error(self, "Ingresa al menos un username.")
            return
        try:
            self._ctx.services.leads.add_manual(name, usernames)
            self._pending_list_name = name
            self.refresh_page()
            set_panel_status(self, f"Se agregaron {len(usernames)} usernames a '{name}'.")
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudieron agregar usernames. Ver logs para mas detalles.")

    def _delete_selected(self) -> None:
        list_name = self._selected_list() or str(self._name_input.text() or "").strip()
        if not list_name:
            show_panel_error(self, "Selecciona una lista.")
            return
        try:
            self._ctx.services.leads.delete_list(list_name)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo eliminar la lista. Ver logs para mas detalles.")
            return
        self._new_list()
        self.refresh_page()
        set_panel_status(self, f"Lista '{list_name}' eliminada.")

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._table_summary.setText("Cargando listas...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_lists_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _apply_snapshot(self, payload: dict[str, object]) -> None:
        rows = payload.get("rows") if isinstance(payload, dict) else []
        current_name = self._pending_list_name or self._selected_list() or str(self._name_input.text() or "").strip()
        self._rows_by_name = {
            str(row.get("name") or "").strip().lower(): dict(row)
            for row in rows
            if isinstance(row, dict) and str(row.get("name") or "").strip()
        }
        self._table.setRowCount(len(rows) if isinstance(rows, list) else 0)
        selected_row = -1
        for row_index, row in enumerate(rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "")
            count = row.get("count", 0)
            self._table.setItem(row_index, 0, table_item(name))
            self._table.setItem(row_index, 1, table_item(count))
            if current_name and name.lower() == current_name.lower():
                selected_row = row_index
        self._table_summary.setText(str(payload.get("summary") or "").strip())

        if selected_row >= 0:
            self._table.selectRow(selected_row)
            self._load_selected()
        else:
            self._refresh_stats()
        self._pending_list_name = ""

    def _on_snapshot_loaded(self, request_id: int, payload: object) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._table_summary.setText(f"No se pudieron cargar las listas: {error.message}")
