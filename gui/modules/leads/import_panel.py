from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext
from gui.query_runner import QueryError
from gui.snapshot_queries import build_leads_import_snapshot

from .common import open_dark_file_dialog, set_panel_status, show_panel_error, show_panel_exception


class LeadsImportPanel(QWidget):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._snapshot_request_id = 0
        self._snapshot_loading = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        card = QFrame()
        card.setObjectName("SendSetupCard")
        layout = QGridLayout(card)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(8)

        self._path_input = QLineEdit()
        self._list_combo = QComboBox()
        self._list_combo.setEditable(True)

        browse_button = QPushButton("Buscar archivo")
        browse_button.setObjectName("SecondaryButton")
        browse_button.clicked.connect(self._browse)
        analyze_button = QPushButton("Analizar archivo")
        analyze_button.setObjectName("SecondaryButton")
        analyze_button.clicked.connect(self._preview_selected_file)
        rollback_button = QPushButton("Deshacer ultimo import")
        rollback_button.setObjectName("SecondaryButton")
        rollback_button.clicked.connect(self._rollback_last_import)
        import_csv_button = QPushButton("Importar CSV")
        import_csv_button.setObjectName("PrimaryButton")
        import_csv_button.clicked.connect(self._import_csv)
        import_txt_button = QPushButton("Importar TXT")
        import_txt_button.setObjectName("SecondaryButton")
        import_txt_button.clicked.connect(self._import_txt)

        self._summary = QLabel("")
        self._summary.setObjectName("MutedText")
        self._summary.setWordWrap(True)

        layout.addWidget(QLabel("Archivo"), 0, 0)
        layout.addWidget(self._path_input, 0, 1, 1, 2)
        layout.addWidget(browse_button, 0, 3)
        layout.addWidget(QLabel("Lista destino"), 1, 0)
        layout.addWidget(self._list_combo, 1, 1, 1, 3)
        layout.addWidget(rollback_button, 2, 0)
        layout.addWidget(analyze_button, 2, 1)
        layout.addWidget(import_csv_button, 2, 2)
        layout.addWidget(import_txt_button, 2, 3)
        layout.addWidget(self._summary, 3, 0, 1, 4)

        root.addWidget(card)
        root.addStretch(1)

    def _browse(self) -> None:
        path = open_dark_file_dialog(self, "Selecciona archivo de leads", "Leads (*.csv *.txt)")
        if path:
            self._path_input.setText(path)

    def _validated_list_name(self) -> str | None:
        list_name = str(self._list_combo.currentText() or "").strip()
        if not list_name:
            show_panel_error(self, "Selecciona la lista destino.")
            return None
        try:
            return self._ctx.services.leads.validate_list_name(list_name)
        except Exception as exc:
            show_panel_error(self, str(exc) or "Nombre de lista invalido.")
            return None

    def _validate_import_target(self, *, expected_suffix: str = "") -> tuple[str, str] | None:
        path = str(self._path_input.text() or "").strip()
        list_name = self._validated_list_name()
        if list_name is None:
            return None
        if not path:
            show_panel_error(self, "Selecciona archivo y lista destino.")
            return None
        file_path = Path(path)
        if not file_path.is_file():
            show_panel_error(self, "El archivo seleccionado no existe.")
            return None
        if expected_suffix and file_path.suffix.lower() != expected_suffix.lower():
            show_panel_error(self, f"Selecciona un archivo {expected_suffix.lower()}.")
            return None
        if not expected_suffix and file_path.suffix.lower() not in {".csv", ".txt"}:
            show_panel_error(self, "Selecciona un archivo .csv o .txt.")
            return None
        return str(file_path), list_name

    @staticmethod
    def _format_preview_summary(payload: dict[str, object]) -> str:
        kind = str(payload.get("kind") or "").upper()
        summary = (
            f"Analisis {kind}: "
            f"Validos: {int(payload.get('valid_count') or 0)}  |  "
            f"Nuevos: {int(payload.get('new_count') or 0)}  |  "
            f"Ya estaban: {int(payload.get('already_present_count') or 0)}  |  "
            f"Duplicados en archivo: {int(payload.get('duplicate_in_file_count') or 0)}  |  "
            f"Invalidos/vacios: {int(payload.get('blank_or_invalid_count') or 0)}"
        )
        details: list[str] = []
        encoding = str(payload.get("encoding") or "").strip()
        if encoding:
            details.append(f"Encoding: {encoding}")
        delimiter = str(payload.get("delimiter") or "").strip()
        if delimiter:
            details.append(f"Delimitador: {delimiter}")
        if bool(payload.get("header_detected")):
            details.append("Header detectado")
        username_column = str(payload.get("username_column") or "").strip()
        if username_column:
            details.append(f"Columna: {username_column}")
        same_file_import_count = int(payload.get("same_file_import_count") or 0)
        if same_file_import_count > 0:
            details.append(f"Archivo ya importado antes: {same_file_import_count}")
        sanity_messages = [
            str(item or "").strip()
            for item in list(payload.get("sanity_messages") or [])
            if str(item or "").strip()
        ]
        if sanity_messages:
            label = "Bloqueos" if str(payload.get("sanity_state") or "") == "blocked" else "Advertencias"
            details.append(f"{label}: {' | '.join(sanity_messages)}")
        if details:
            return summary + "\n" + "  |  ".join(details)
        return summary

    @staticmethod
    def _format_import_summary(payload: dict[str, object]) -> str:
        summary = (
            f"Importacion {str(payload.get('kind') or '').upper()} en "
            f"'{str(payload.get('list_name') or '').strip()}': "
            f"Nuevos: {int(payload.get('new_count') or 0)}  |  "
            f"Ya estaban: {int(payload.get('already_present_count') or 0)}  |  "
            f"Total final: {int(payload.get('resulting_count') or 0)}"
        )
        sanity_messages = [
            str(item or "").strip()
            for item in list(payload.get("sanity_messages") or [])
            if str(item or "").strip()
        ]
        if sanity_messages:
            summary += "\nAdvertencias: " + " | ".join(sanity_messages)
        return summary

    @staticmethod
    def _format_rollback_summary(payload: dict[str, object]) -> str:
        return (
            f"Rollback aplicado en '{str(payload.get('list_name') or '').strip()}': "
            f"restaurados {int(payload.get('restored_count') or 0)} leads "
            f"(antes del rollback: {int(payload.get('previous_count') or 0)})."
        )

    def _preview_selected_file(self) -> None:
        payload = self._validate_import_target()
        if payload is None:
            return
        try:
            path, list_name = payload
            if Path(path).suffix.lower() == ".csv":
                preview = self._ctx.services.leads.preview_csv(path, list_name)
            else:
                preview = self._ctx.services.leads.preview_txt(path, list_name)
            self._summary.setText(self._format_preview_summary(dict(preview)))
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo analizar el archivo de leads.")

    def _import_csv(self) -> None:
        payload = self._validate_import_target(expected_suffix=".csv")
        if payload is None:
            return
        try:
            path, list_name = payload
            result = self._ctx.services.leads.import_csv(path, list_name)
            self._summary.setText(self._format_import_summary(dict(result)))
            self.refresh_page()
            set_panel_status(self, self._format_import_summary(dict(result)))
            self._ctx.open_route("leads_lists_page", None)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo importar el CSV. Ver logs para mas detalles.")

    def _import_txt(self) -> None:
        payload = self._validate_import_target(expected_suffix=".txt")
        if payload is None:
            return
        try:
            path, list_name = payload
            result = self._ctx.services.leads.import_txt(path, list_name)
            self._summary.setText(self._format_import_summary(dict(result)))
            self.refresh_page()
            set_panel_status(self, self._format_import_summary(dict(result)))
            self._ctx.open_route("leads_lists_page", None)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo importar el TXT. Ver logs para mas detalles.")

    def _rollback_last_import(self) -> None:
        list_name = self._validated_list_name()
        if list_name is None:
            return
        try:
            result = self._ctx.services.leads.rollback_last_import(list_name)
            summary = self._format_rollback_summary(dict(result))
            self._summary.setText(summary)
            self.refresh_page()
            set_panel_status(self, summary)
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo deshacer el ultimo import.")

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._summary.setText("Cargando listas disponibles...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_import_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _apply_snapshot(self, payload: dict[str, object]) -> None:
        current_value = str(self._list_combo.currentText() or "").strip()
        lists = payload.get("lists") if isinstance(payload, dict) else []
        self._list_combo.blockSignals(True)
        self._list_combo.clear()
        for name in lists if isinstance(lists, list) else []:
            self._list_combo.addItem(str(name or ""))
        if current_value:
            self._list_combo.setEditText(current_value)
        self._list_combo.blockSignals(False)
        self._summary.setText(str(payload.get("summary") or "").strip())

    def _on_snapshot_loaded(self, request_id: int, payload: object) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._summary.setText(f"No se pudieron cargar las listas: {error.message}")
