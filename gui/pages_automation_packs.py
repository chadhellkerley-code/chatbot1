from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from gui.automation_dialogs import AutomationModalDialog, confirm_automation_action
from gui.query_runner import QueryError

from .automation_pages_base import AutomationSectionPage
from .page_base import PageContext, safe_int, table_item
from .snapshot_queries import build_automation_packs_snapshot


class _PackActionEditor(QFrame):
    move_up_requested = Signal(object)
    move_down_requested = Signal(object)
    remove_requested = Signal(object)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("ExecCard")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(8)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        self._title = QLabel("Accion 1")
        self._title.setObjectName("AutomationSectionLabel")
        self._sequence_hint = QLabel("Primer mensaje")
        self._sequence_hint.setObjectName("SectionPanelHint")
        self._move_up = QPushButton("Subir")
        self._move_up.setObjectName("SecondaryButton")
        self._move_up.clicked.connect(lambda: self.move_up_requested.emit(self))
        self._move_down = QPushButton("Bajar")
        self._move_down.setObjectName("SecondaryButton")
        self._move_down.clicked.connect(lambda: self.move_down_requested.emit(self))
        self._remove = QPushButton("Eliminar")
        self._remove.setObjectName("DangerButton")
        self._remove.clicked.connect(lambda: self.remove_requested.emit(self))
        header.addWidget(self._title)
        header.addWidget(self._sequence_hint)
        header.addStretch(1)
        header.addWidget(self._move_up)
        header.addWidget(self._move_down)
        header.addWidget(self._remove)
        layout.addLayout(header)

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(8)
        form.setVerticalSpacing(8)
        self._type_combo = QComboBox()
        self._type_combo.addItem("Texto fijo", "text_fixed")
        self._type_combo.addItem("Mensaje inteligente IA", "text_adaptive")
        self._type_combo.currentIndexChanged.connect(self._refresh_content_copy)
        self._content_label = QLabel("Texto fijo")
        self._content = QPlainTextEdit()
        self._content.setMinimumHeight(96)
        form.addWidget(QLabel("Tipo de accion"), 0, 0)
        form.addWidget(self._type_combo, 0, 1)
        form.addWidget(self._content_label, 1, 0, 1, 2)
        form.addWidget(self._content, 2, 0, 1, 2)
        layout.addLayout(form)
        self._refresh_content_copy()

    def _refresh_content_copy(self) -> None:
        action_type = str(self._type_combo.currentData() or "text_fixed")
        if action_type == "text_adaptive":
            self._content_label.setText("Prompt para IA")
            self._content.setPlaceholderText(
                "Escribe la instruccion que usara la IA para generar este mensaje."
            )
            return
        self._content_label.setText("Texto fijo")
        self._content.setPlaceholderText("Escribe el texto fijo que enviara esta accion.")

    def set_order(self, index: int, total: int) -> None:
        sequence = ("Primer mensaje", "Segundo mensaje", "Tercer mensaje")
        self._title.setText(f"Accion {index + 1}")
        self._sequence_hint.setText(sequence[index] if index < len(sequence) else "")
        self._move_up.setEnabled(index > 0)
        self._move_down.setEnabled(index < max(0, total - 1))
        self._remove.setEnabled(total > 1)

    def set_payload(self, payload: dict[str, Any]) -> None:
        self._type_combo.setCurrentIndex(max(0, self._type_combo.findData(str(payload.get("type") or "text_fixed"))))
        self._content.setPlainText(str(payload.get("content") or payload.get("instruction") or ""))
        self._refresh_content_copy()

    def payload(self) -> dict[str, Any]:
        action_type = str(self._type_combo.currentData() or "text_fixed")
        content = str(self._content.toPlainText() or "").strip()
        if action_type == "text_adaptive":
            return {"type": action_type, "instruction": content}
        return {"type": action_type, "content": content}


class PackEditorDialog(AutomationModalDialog):
    def __init__(self, pack: dict[str, Any] | None = None, parent: QWidget | None = None) -> None:
        super().__init__("Editor de pack", "Crea o edita packs sin usar JSON manualmente.", parent)
        self.resize(840, 0)
        payload = dict(pack or {})
        self._name = QLineEdit(str(payload.get("name") or ""))
        self._type = QLineEdit(str(payload.get("type") or "pack"))
        self._delay_min = QSpinBox()
        self._delay_min.setRange(0, 3600)
        self._delay_min.setValue(max(0, safe_int(payload.get("delay_min"))))
        self._delay_max = QSpinBox()
        self._delay_max.setRange(0, 3600)
        self._delay_max.setMinimum(self._delay_min.value())
        self._delay_max.setValue(max(self._delay_min.value(), safe_int(payload.get("delay_max"))))
        self._delay_min.valueChanged.connect(self._sync_delay_bounds)
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(8)
        for row, (label, field) in enumerate(
            (
                ("Nombre del pack", self._name),
                ("Tipo / identificador", self._type),
                ("Delay minimo", self._delay_min),
                ("Delay maximo", self._delay_max),
            )
        ):
            form.addWidget(QLabel(label), row, 0)
            form.addWidget(field, row, 1)
        self.body_layout().addLayout(form)

        actions_title = QLabel("Acciones del pack")
        actions_title.setObjectName("AutomationSectionLabel")
        actions_hint = QLabel(
            "Configura hasta 3 acciones ordenadas. La secuencia define primer, segundo y tercer mensaje."
        )
        actions_hint.setObjectName("SectionPanelHint")
        actions_hint.setWordWrap(True)
        self.body_layout().addWidget(actions_title)
        self.body_layout().addWidget(actions_hint)

        self._actions_container = QWidget()
        self._actions_container.setObjectName("SubmenuScrollContent")
        self._actions_wrap = QVBoxLayout(self._actions_container)
        self._actions_wrap.setContentsMargins(0, 0, 0, 0)
        self._actions_wrap.setSpacing(8)
        self._actions_scroll = QScrollArea()
        self._actions_scroll.setObjectName("SubmenuScroll")
        self._actions_scroll.setWidgetResizable(True)
        self._actions_scroll.setFrameShape(QFrame.NoFrame)
        self._actions_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._actions_scroll.setWidget(self._actions_container)
        self._actions_scroll.setMinimumHeight(280)
        self.body_layout().addWidget(self._actions_scroll, 1)

        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(8)
        self._actions_counter = QLabel("0/3 acciones configuradas")
        self._actions_counter.setObjectName("SectionPanelHint")
        self._add_button = QPushButton("Agregar accion")
        self._add_button.setObjectName("SecondaryButton")
        self._add_button.clicked.connect(self._add_action)
        controls.addWidget(self._actions_counter)
        controls.addStretch(1)
        controls.addWidget(self._add_button)
        controls.addStretch(1)
        self.body_layout().addLayout(controls)

        self._action_editors: list[_PackActionEditor] = []
        for action in payload.get("actions") or []:
            if isinstance(action, dict) and len(self._action_editors) < 3:
                self._add_action(action)
        if not self._action_editors:
            self._add_action()
        self._pack_id = str(payload.get("id") or "").strip()
        self.add_buttons(confirm_text="Guardar", cancel_text="Cerrar")

    def _sync_delay_bounds(self, value: int) -> None:
        self._delay_max.setMinimum(max(0, int(value or 0)))
        if self._delay_max.value() < self._delay_min.value():
            self._delay_max.setValue(self._delay_min.value())

    def _repaint_actions(self) -> None:
        while self._actions_wrap.count():
            child = self._actions_wrap.takeAt(0)
            widget = child.widget()
            if widget is not None:
                widget.setParent(None)
        total = len(self._action_editors)
        for index, editor in enumerate(self._action_editors):
            editor.set_order(index, total)
            self._actions_wrap.addWidget(editor)
        self._actions_wrap.addStretch(1)
        self._actions_counter.setText(f"{total}/3 acciones configuradas")
        self._add_button.setEnabled(total < 3)

    def _editor_index(self, editor: _PackActionEditor) -> int:
        for index, candidate in enumerate(self._action_editors):
            if candidate is editor:
                return index
        return -1

    def _add_action(self, payload: dict[str, Any] | None = None) -> None:
        if len(self._action_editors) >= 3:
            return
        editor = _PackActionEditor(parent=self)
        editor.move_up_requested.connect(self._move_action_up)
        editor.move_down_requested.connect(self._move_action_down)
        editor.remove_requested.connect(self._remove_action)
        if payload:
            editor.set_payload(payload)
        self._action_editors.append(editor)
        self._repaint_actions()

    def _move_action_up(self, editor: object) -> None:
        self._move_action(editor, -1)

    def _move_action_down(self, editor: object) -> None:
        self._move_action(editor, 1)

    def _move_action(self, editor: object, direction: int) -> None:
        if len(self._action_editors) < 2 or not isinstance(editor, _PackActionEditor):
            return
        current = self._editor_index(editor)
        target = current + (1 if direction > 0 else -1)
        if target < 0 or target >= len(self._action_editors):
            return
        self._action_editors[current], self._action_editors[target] = self._action_editors[target], self._action_editors[current]
        self._repaint_actions()
        self._action_editors[target].setFocus()

    def _remove_action(self, editor: object) -> None:
        if len(self._action_editors) <= 1 or not isinstance(editor, _PackActionEditor):
            return
        index = self._editor_index(editor)
        if index < 0:
            return
        editor = self._action_editors.pop(index)
        editor.setParent(None)
        editor.deleteLater()
        self._repaint_actions()

    def payload(self) -> dict[str, Any]:
        name = str(self._name.text() or "").strip()
        pack_type = str(self._type.text() or "").strip()
        if not name:
            raise ValueError("El nombre del pack es obligatorio.")
        if not pack_type:
            raise ValueError("El tipo / identificador del pack es obligatorio.")
        actions: list[dict[str, Any]] = []
        for editor in self._action_editors:
            payload = editor.payload()
            if str(payload.get("content") or payload.get("instruction") or "").strip():
                actions.append(payload)
        if not actions:
            raise ValueError("Configura al menos una accion con contenido.")
        return {
            "id": self._pack_id,
            "name": name,
            "type": pack_type,
            "delay_min": int(self._delay_min.value()),
            "delay_max": int(self._delay_max.value()),
            "actions": actions[:3],
        }


class AutomationPacksPage(AutomationSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Packs",
            "Gestion estructurada de packs reutilizables sin editar JSON manualmente.",
            route_key="automation_packs_page",
            parent=parent,
        )
        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["ID", "Nombre", "Tipo", "Acciones"])
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.itemDoubleClicked.connect(lambda *_: self._edit_selected())
        self.content_layout().addWidget(self._table, 1)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        for label, object_name, handler in (
            ("Nuevo pack", "PrimaryButton", self._new_pack),
            ("Editar pack", "SecondaryButton", self._edit_selected),
            ("Eliminar pack", "DangerButton", self._delete_selected),
            ("Recargar", "SecondaryButton", self.refresh_page),
        ):
            button = QPushButton(label)
            button.setObjectName(object_name)
            button.clicked.connect(handler)
            actions.addWidget(button)
        actions.addStretch(1)
        self.content_layout().addLayout(actions)

        self._rows_by_id: dict[str, dict[str, Any]] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _selected_pack_id(self) -> str:
        row = self._table.currentRow()
        if row < 0:
            return ""
        item = self._table.item(row, 0)
        return str(item.text() if item else "").strip()

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
        self._rows_by_id = {
            str(item.get("id") or "").strip(): dict(item)
            for item in rows
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        self._table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            actions = row.get("actions") if isinstance(row.get("actions"), list) else []
            self._table.setItem(row_index, 0, table_item(row.get("id", "")))
            self._table.setItem(row_index, 1, table_item(row.get("name", "")))
            self._table.setItem(row_index, 2, table_item(row.get("type", "")))
            self._table.setItem(row_index, 3, table_item(len(actions)))
        self.clear_status()

    def _open_editor(self, pack: dict[str, Any] | None = None) -> None:
        try:
            dialog = PackEditorDialog(pack, self)
        except Exception as exc:
            self.show_exception(exc, "No se pudo abrir el editor de packs.")
            return
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            payload = dialog.payload()
            saved = self._ctx.services.automation.upsert_pack(payload)
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar el pack.")
            return
        self.set_status(f"Pack guardado: {saved.get('id', '')}")
        self.refresh_page()

    def _new_pack(self) -> None:
        self._open_editor()

    def _edit_selected(self) -> None:
        pack_id = self._selected_pack_id()
        if not pack_id:
            self.show_error("Selecciona un pack.")
            return
        self._open_editor(self._rows_by_id.get(pack_id))

    def _delete_selected(self) -> None:
        pack_id = self._selected_pack_id()
        if not pack_id:
            self.show_error("Selecciona un pack.")
            return
        if not confirm_automation_action(self, title="Eliminar pack", message=f"Se eliminara el pack '{pack_id}'.", confirm_text="Eliminar", danger=True):
            return
        try:
            self._ctx.services.automation.delete_pack(pack_id)
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar el pack.")
            return
        self.set_status(f"Pack eliminado: {pack_id}")
        self.refresh_page()

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_packs_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_page()

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
        self.set_status(f"No se pudieron cargar los packs: {error.message}")
