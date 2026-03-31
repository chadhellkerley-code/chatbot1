from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDoubleSpinBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QStackedWidget,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from core import responder as responder_module
from gui.flow_editor.flow_view import FlowBuilderCanvas
from gui.query_runner import QueryError

from .automation_dialogs import AutomationModalDialog, confirm_automation_action
from .automation_pages_base import AutomationSectionPage
from .page_base import PageContext, safe_float, table_item
from .snapshot_queries import build_automation_flow_snapshot


_DEFAULT_CANONICAL_ACTION = "auto_reply"


def _empty_flow() -> dict[str, Any]:
    return {
        "version": 1,
        "entry_stage_id": "",
        "stages": [],
        "allow_empty": False,
        "layout": {
            "nodes": {},
            "viewport": {"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0},
        },
    }


def _default_stage(stage_id: str, *, action_type: str = _DEFAULT_CANONICAL_ACTION) -> dict[str, Any]:
    return {
        "id": stage_id,
        "action_type": action_type,
        "transitions": {
            "positive": stage_id,
            "negative": stage_id,
            "doubt": stage_id,
            "neutral": stage_id,
        },
        "followups": [],
        "post_objection": {
            "enabled": False,
            "action_type": action_type,
            "max_steps": 2,
            "resolved_transition": "positive",
            "unresolved_transition": "negative",
        },
    }


class StageEditorDialog(AutomationModalDialog):
    _OUTCOME_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Ruta positiva", "positive"),
        ("Ruta negativa", "negative"),
        ("Ruta neutral", "neutral"),
        ("Ruta objecion", "doubt"),
    )

    def __init__(
        self,
        *,
        stage_payload: dict[str, Any],
        stage_ids: list[str],
        pack_options: list[str],
        pack_names: dict[str, str],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(
            "Configurar etapa",
            "Edita la accion, las rutas, los follow-ups y el manejo de objeciones sin usar paneles embebidos.",
            parent,
        )
        self.resize(780, 0)
        self._stage_ids = [str(item or "").strip() for item in stage_ids if str(item or "").strip()]
        self._pack_options = [str(item or "").strip() for item in pack_options if str(item or "").strip()]
        self._pack_names = dict(pack_names or {})

        payload = dict(stage_payload or {})
        current_stage_id = str(payload.get("id") or "").strip()
        if current_stage_id and current_stage_id not in self._stage_ids:
            self._stage_ids.append(current_stage_id)

        transitions = payload.get("transitions") if isinstance(payload.get("transitions"), dict) else {}
        objection = payload.get("post_objection") if isinstance(payload.get("post_objection"), dict) else {}

        self._stage_id = QLineEdit(current_stage_id)
        self._action_combo = QComboBox()
        self._positive = QComboBox()
        self._negative = QComboBox()
        self._neutral = QComboBox()
        self._objection = QComboBox()
        self._objection_enabled = QCheckBox("Habilitar manejo de objeciones")
        self._objection_action = QComboBox()
        self._objection_resolved = QComboBox()
        self._objection_unresolved = QComboBox()
        self._follow_delay = QDoubleSpinBox()
        self._follow_delay.setRange(0.25, 999.0)
        self._follow_delay.setDecimals(2)
        self._follow_delay.setValue(4.0)
        self._follow_action = QComboBox()
        self._follow_table = QTableWidget(0, 2)
        self._follow_table.setHorizontalHeaderLabels(["Delay (h)", "Accion"])
        self._follow_table.verticalHeader().setVisible(False)
        self._follow_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._follow_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._follow_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._follow_table.horizontalHeader().setStretchLastSection(True)
        self._follow_table.setMinimumHeight(150)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        grid.addWidget(QLabel("ID de etapa"), 0, 0)
        grid.addWidget(self._stage_id, 0, 1)
        grid.addWidget(QLabel("Pack asignado / accion"), 1, 0)
        grid.addWidget(self._action_combo, 1, 1)
        grid.addWidget(QLabel("Ruta positiva"), 2, 0)
        grid.addWidget(self._positive, 2, 1)
        grid.addWidget(QLabel("Ruta negativa"), 3, 0)
        grid.addWidget(self._negative, 3, 1)
        grid.addWidget(QLabel("Ruta neutral"), 4, 0)
        grid.addWidget(self._neutral, 4, 1)
        grid.addWidget(QLabel("Ruta objecion"), 5, 0)
        grid.addWidget(self._objection, 5, 1)
        grid.addWidget(self._objection_enabled, 6, 0, 1, 2)
        grid.addWidget(QLabel("Accion de objecion"), 7, 0)
        grid.addWidget(self._objection_action, 7, 1)
        grid.addWidget(QLabel("Si objecion se resuelve"), 8, 0)
        grid.addWidget(self._objection_resolved, 8, 1)
        grid.addWidget(QLabel("Si objecion no se resuelve"), 9, 0)
        grid.addWidget(self._objection_unresolved, 9, 1)
        grid.addWidget(QLabel("Delay del follow-up"), 10, 0)
        grid.addWidget(self._follow_delay, 10, 1)
        grid.addWidget(QLabel("Accion follow-up"), 11, 0)
        grid.addWidget(self._follow_action, 11, 1)
        self.body_layout().addLayout(grid)
        self.body_layout().addWidget(self._follow_table)

        follow_actions = QHBoxLayout()
        follow_actions.setContentsMargins(0, 0, 0, 0)
        follow_actions.setSpacing(8)
        add_follow = QPushButton("Agregar follow-up")
        add_follow.setObjectName("SecondaryButton")
        add_follow.clicked.connect(self._add_followup_row)
        remove_follow = QPushButton("Quitar follow-up")
        remove_follow.setObjectName("SecondaryButton")
        remove_follow.clicked.connect(self._remove_followup_row)
        follow_actions.addWidget(add_follow)
        follow_actions.addWidget(remove_follow)
        follow_actions.addStretch(1)
        self.body_layout().addLayout(follow_actions)

        self._fill_action_combo(
            self._action_combo,
            str(payload.get("action_type") or _DEFAULT_CANONICAL_ACTION),
        )
        self._fill_action_combo(
            self._objection_action,
            str(objection.get("action_type") or payload.get("action_type") or "objection_engine"),
            include_objection_engine=True,
        )
        self._fill_action_combo(self._follow_action, "followup_text")
        self._fill_stage_combo(self._positive, str(transitions.get("positive") or current_stage_id), current_stage_id)
        self._fill_stage_combo(self._negative, str(transitions.get("negative") or current_stage_id), current_stage_id)
        self._fill_stage_combo(self._neutral, str(transitions.get("neutral") or current_stage_id), current_stage_id)
        self._fill_stage_combo(self._objection, str(transitions.get("doubt") or current_stage_id), current_stage_id)
        self._fill_outcome_combo(
            self._objection_resolved,
            str(objection.get("resolved_transition") or "positive"),
        )
        self._fill_outcome_combo(
            self._objection_unresolved,
            str(objection.get("unresolved_transition") or "negative"),
        )
        self._objection_enabled.setChecked(bool(objection.get("enabled")))
        self._populate_followups(list(payload.get("followups") or []))
        self.add_buttons(confirm_text="Guardar", cancel_text="Cerrar")

    def _fill_action_combo(
        self,
        combo: QComboBox,
        current_value: str,
        *,
        include_objection_engine: bool = False,
    ) -> None:
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("Respuesta IA", "auto_reply")
        combo.addItem("Texto de follow-up", "followup_text")
        if include_objection_engine:
            combo.addItem("Motor de objeciones", "objection_engine")
        combo.addItem("No enviar", "no_send")
        for pack_id in self._pack_options:
            combo.addItem(f"Pack: {self._pack_names.get(pack_id, pack_id)}", pack_id)
        combo.setCurrentIndex(max(0, combo.findData(current_value)))
        combo.blockSignals(False)

    def _fill_stage_combo(self, combo: QComboBox, current_value: str, fallback_value: str) -> None:
        combo.blockSignals(True)
        combo.clear()
        options = list(self._stage_ids)
        fallback = str(fallback_value or "").strip()
        if fallback and fallback not in options:
            options.append(fallback)
        for stage_id in options:
            combo.addItem(stage_id, stage_id)
        target = str(current_value or fallback).strip() or fallback
        combo.setCurrentIndex(max(0, combo.findData(target)))
        combo.blockSignals(False)

    def _fill_outcome_combo(self, combo: QComboBox, current_value: str) -> None:
        combo.blockSignals(True)
        combo.clear()
        for label, token in self._OUTCOME_OPTIONS:
            combo.addItem(label, token)
        combo.setCurrentIndex(max(0, combo.findData(current_value)))
        combo.blockSignals(False)

    def _populate_followups(self, rows: list[dict[str, Any]]) -> None:
        clean_rows = [dict(item) for item in rows if isinstance(item, dict)]
        self._follow_table.setRowCount(len(clean_rows))
        for row_index, row in enumerate(clean_rows):
            self._follow_table.setItem(
                row_index,
                0,
                table_item(f"{safe_float(row.get('delay_hours')):.2f}".rstrip("0").rstrip(".")),
            )
            self._follow_table.setItem(row_index, 1, table_item(row.get("action_type", "")))

    def _collect_followups(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for row_index in range(self._follow_table.rowCount()):
            delay_item = self._follow_table.item(row_index, 0)
            action_item = self._follow_table.item(row_index, 1)
            try:
                delay_value = float(delay_item.text()) if delay_item else 0.0
            except Exception:
                delay_value = 0.0
            action_value = str(action_item.text() if action_item else "").strip()
            if delay_value >= 0 and action_value:
                rows.append(
                    {
                        "delay_hours": delay_value,
                        "action_type": responder_module._canonical_flow_action_type(
                            action_value,
                            strict=True,
                        ),
                    }
                )
        return rows

    def _add_followup_row(self) -> None:
        row_index = self._follow_table.rowCount()
        self._follow_table.insertRow(row_index)
        self._follow_table.setItem(row_index, 0, table_item(self._follow_delay.value()))
        self._follow_table.setItem(row_index, 1, table_item(self._follow_action.currentData() or "followup_text"))

    def _remove_followup_row(self) -> None:
        row_index = self._follow_table.currentRow()
        if row_index >= 0:
            self._follow_table.removeRow(row_index)

    def payload(self) -> dict[str, Any]:
        stage_id = str(self._stage_id.text() or "").strip()
        action_value = responder_module._canonical_flow_action_type(
            self._action_combo.currentData(),
            strict=True,
        )
        fallback_stage = stage_id or (self._stage_ids[0] if self._stage_ids else "")
        objection_action = responder_module._canonical_flow_action_type(
            self._objection_action.currentData(),
            strict=bool(self._objection_enabled.isChecked()),
            allow_empty=not bool(self._objection_enabled.isChecked()),
        )
        return {
            "id": stage_id,
            "action_type": action_value,
            "transitions": {
                "positive": str(self._positive.currentData() or fallback_stage).strip() or fallback_stage,
                "negative": str(self._negative.currentData() or fallback_stage).strip() or fallback_stage,
                "neutral": str(self._neutral.currentData() or fallback_stage).strip() or fallback_stage,
                "doubt": str(self._objection.currentData() or fallback_stage).strip() or fallback_stage,
            },
            "followups": self._collect_followups(),
            "post_objection": {
                "enabled": bool(self._objection_enabled.isChecked()),
                "action_type": objection_action,
                "max_steps": 2,
                "resolved_transition": str(self._objection_resolved.currentData() or "positive").strip() or "positive",
                "unresolved_transition": str(self._objection_unresolved.currentData() or "negative").strip() or "negative",
            },
        }


class AutomationFlowPage(AutomationSectionPage):
    _CANVAS_TO_ACTION = {
        "auto_reply": "auto_reply",
        "followup_text": "followup_text",
        "no_send": "no_send",
    }

    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Flow",
            "Dos interfaces separadas para editar el flow: simplificado visual y avanzado estructurado.",
            route_key="automation_flow_page",
            scrollable=False,
            parent=parent,
        )
        self._flow_config: dict[str, Any] = _empty_flow()
        self._selected_stage_id = ""
        self._pack_options: list[str] = []
        self._pack_names: dict[str, str] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._mode = "simple"
        self._canvas_maximized = False
        self._normal_content_margins = self.default_content_margins()

        self._controls_widget = QWidget()
        header = QHBoxLayout(self._controls_widget)
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        self._alias_combo = QComboBox()
        self._alias_combo.currentIndexChanged.connect(self.load_flow)
        self._entry_stage_combo = QComboBox()
        self._entry_stage_combo.currentIndexChanged.connect(self._entry_stage_changed)
        load_button = QPushButton("Cargar")
        load_button.setObjectName("SecondaryButton")
        load_button.clicked.connect(self.load_flow)
        save_button = QPushButton("Guardar")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.save_flow)
        add_button = QPushButton("Nueva etapa")
        add_button.setObjectName("SecondaryButton")
        add_button.clicked.connect(lambda: self._add_stage(open_editor=True))
        edit_button = QPushButton("Editar etapa")
        edit_button.setObjectName("SecondaryButton")
        edit_button.clicked.connect(self._edit_selected_stage)
        delete_button = QPushButton("Eliminar etapa")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_selected_stage)
        header.addWidget(QLabel("Alias"))
        header.addWidget(self._alias_combo, 1)
        header.addWidget(QLabel("Etapa inicial"))
        header.addWidget(self._entry_stage_combo, 1)
        header.addWidget(load_button)
        header.addWidget(save_button)
        header.addWidget(add_button)
        header.addWidget(edit_button)
        header.addWidget(delete_button)
        self.content_layout().addWidget(self._controls_widget)

        self._mode_widget = QWidget()
        mode_row = QHBoxLayout(self._mode_widget)
        mode_row.setContentsMargins(0, 0, 0, 0)
        mode_row.setSpacing(8)
        self._simple_mode = QPushButton("Modo simplificado")
        self._simple_mode.setCheckable(True)
        self._simple_mode.clicked.connect(lambda: self._set_mode("simple"))
        self._advanced_mode = QPushButton("Modo avanzado")
        self._advanced_mode.setCheckable(True)
        self._advanced_mode.clicked.connect(lambda: self._set_mode("advanced"))
        mode_row.addWidget(self._simple_mode)
        mode_row.addWidget(self._advanced_mode)
        mode_row.addStretch(1)
        self.content_layout().addWidget(self._mode_widget)

        self._stack = QStackedWidget()
        self.content_layout().addWidget(self._stack, 1)
        self._stack.addWidget(self._build_simple_view())
        self._stack.addWidget(self._build_advanced_view())
        self._table = self._advanced_table
        self._set_mode("simple")

    def _build_simple_view(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(0, 0, 0, 0)
        toolbar.setSpacing(8)
        self._simple_stage_summary = QLabel("Selecciona una etapa del canvas para editarla.")
        self._simple_stage_summary.setObjectName("SectionPanelHint")
        self._simple_stage_summary.setWordWrap(True)
        self._simple_zoom = QLabel("Zoom: 100%")
        self._simple_zoom.setObjectName("SectionPanelHint")
        edit_button = QPushButton("Editar etapa")
        edit_button.setObjectName("SecondaryButton")
        edit_button.clicked.connect(self._edit_selected_stage)
        self._simple_maximize = QPushButton("Maximizar canvas")
        self._simple_maximize.setObjectName("SecondaryButton")
        self._simple_maximize.clicked.connect(self._toggle_canvas_maximize)
        toolbar.addWidget(self._simple_stage_summary, 1)
        toolbar.addWidget(self._simple_zoom)
        toolbar.addWidget(edit_button)
        toolbar.addWidget(self._simple_maximize)
        layout.addLayout(toolbar)

        self._canvas = FlowBuilderCanvas()
        self._canvas.set_zoom_label_target(self._simple_zoom)
        self._canvas.stage_selected.connect(self._select_stage)
        self._canvas.stage_double_clicked.connect(self._edit_stage)
        self._canvas.create_requested.connect(self._create_stage_from_canvas)
        self._canvas.stage_delete_requested.connect(self._delete_stage_by_id)
        self._canvas.stage_changed.connect(self._apply_canvas_stage_payload)
        self._canvas.node_position_changed.connect(self._update_canvas_node_position)
        self._canvas.viewport_changed.connect(self._update_canvas_viewport)
        layout.addWidget(self._canvas, 1)
        return widget

    def _build_advanced_view(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        hint = QLabel(
            "Vista estructurada por etapas. Doble click en una fila para abrir el editor completo."
        )
        hint.setObjectName("SectionPanelHint")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self._advanced_table = QTableWidget(0, 6)
        self._advanced_table.setHorizontalHeaderLabels(
            ["ID", "Accion", "Positiva", "Negativa", "Objeciones", "Follow-ups"]
        )
        self._advanced_table.verticalHeader().setVisible(False)
        self._advanced_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._advanced_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._advanced_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._advanced_table.horizontalHeader().setStretchLastSection(True)
        self._advanced_table.itemSelectionChanged.connect(self._select_stage_from_table)
        self._advanced_table.itemDoubleClicked.connect(lambda *_args: self._edit_selected_stage())
        layout.addWidget(self._advanced_table, 1)
        return widget

    def _sync_shell_visibility(self) -> None:
        maximized = self._mode == "simple" and self._canvas_maximized
        self._controls_widget.setVisible(not maximized)
        self._mode_widget.setVisible(not maximized)
        self.set_page_header_visible(not maximized)
        self.set_section_nav_visible(not maximized)
        self.set_content_margins((0, 0, 0, 0) if maximized else self._normal_content_margins)
        self._simple_maximize.setText("Restaurar vista" if maximized else "Maximizar canvas")

    def _toggle_canvas_maximize(self) -> None:
        self._canvas_maximized = not self._canvas_maximized
        self._sync_shell_visibility()

    def _set_mode(self, mode: str) -> None:
        self._mode = "advanced" if mode == "advanced" else "simple"
        if self._mode != "simple":
            self._canvas_maximized = False
        self._simple_mode.setChecked(self._mode == "simple")
        self._advanced_mode.setChecked(self._mode == "advanced")
        self._stack.setCurrentIndex(1 if self._mode == "advanced" else 0)
        self._sync_shell_visibility()
        if self._mode == "simple":
            self._refresh_canvas()

    def _stage_ids(self) -> list[str]:
        return [
            str(item.get("id") or "").strip()
            for item in self._flow_config.get("stages") or []
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        ]

    def _stage(self, stage_id: str) -> dict[str, Any] | None:
        target = str(stage_id or "").strip()
        for stage in self._flow_config.get("stages") or []:
            if isinstance(stage, dict) and str(stage.get("id") or "").strip() == target:
                return dict(stage)
        return None

    def _next_stage_id(self) -> str:
        existing = {item.lower() for item in self._stage_ids()}
        index = 1
        while True:
            candidate = f"etapa_{index}"
            if candidate.lower() not in existing:
                return candidate
            index += 1

    def _action_label(self, action_type: str) -> str:
        action_value = str(action_type or "").strip()
        if action_value in self._pack_names:
            return f"Pack: {self._pack_names.get(action_value, action_value)}"
        labels = {
            "auto_reply": "Respuesta IA",
            "followup_text": "Texto de follow-up",
            "objection_engine": "Motor de objeciones",
            "no_send": "No enviar",
        }
        return labels.get(action_value, action_value or "-")

    def _refresh_selected_stage_copy(self) -> None:
        stage_id = str(self._selected_stage_id or "").strip()
        if not stage_id:
            self._simple_stage_summary.setText("Selecciona una etapa del canvas para editarla.")
            return
        stage = self._stage(stage_id) or {}
        action_label = self._action_label(str(stage.get("action_type") or ""))
        self._simple_stage_summary.setText(
            f"Etapa seleccionada: {stage_id}  |  Accion: {action_label}"
        )

    def _refresh_entry_combo(self) -> None:
        entry_stage = str(self._flow_config.get("entry_stage_id") or "").strip()
        self._entry_stage_combo.blockSignals(True)
        self._entry_stage_combo.clear()
        for stage_id in self._stage_ids():
            self._entry_stage_combo.addItem(stage_id, stage_id)
        self._entry_stage_combo.setCurrentIndex(max(0, self._entry_stage_combo.findData(entry_stage)))
        self._entry_stage_combo.blockSignals(False)

    def _refresh_tables(self) -> None:
        rows = [dict(item) for item in self._flow_config.get("stages") or [] if isinstance(item, dict)]
        self._advanced_table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            transitions = row.get("transitions") if isinstance(row.get("transitions"), dict) else {}
            objection = row.get("post_objection") if isinstance(row.get("post_objection"), dict) else {}
            self._advanced_table.setItem(row_index, 0, table_item(row.get("id", "")))
            self._advanced_table.setItem(
                row_index,
                1,
                table_item(self._action_label(str(row.get("action_type") or ""))),
            )
            self._advanced_table.setItem(row_index, 2, table_item(transitions.get("positive", "")))
            self._advanced_table.setItem(row_index, 3, table_item(transitions.get("negative", "")))
            self._advanced_table.setItem(
                row_index,
                4,
                table_item("Si" if bool(objection.get("enabled")) else "No"),
            )
            self._advanced_table.setItem(row_index, 5, table_item(len(row.get("followups") or [])))
        self._sync_table_selection()

    def _sync_table_selection(self) -> None:
        self._advanced_table.blockSignals(True)
        self._advanced_table.clearSelection()
        for row_index in range(self._advanced_table.rowCount()):
            item = self._advanced_table.item(row_index, 0)
            if item and str(item.text() or "").strip() == self._selected_stage_id:
                self._advanced_table.selectRow(row_index)
                break
        self._advanced_table.blockSignals(False)

    def _refresh_canvas(self) -> None:
        layout = dict(self._flow_config.get("layout") or {})
        nodes = dict(layout.get("nodes") or {})
        positions = {
            stage_id: (float(coords.get("x") or 0.0), float(coords.get("y") or 0.0))
            for stage_id, coords in nodes.items()
            if isinstance(coords, dict)
        }
        viewport = dict(layout.get("viewport") or {})
        self._canvas.set_flow(
            stages=[dict(item) for item in self._flow_config.get("stages") or [] if isinstance(item, dict)],
            pack_options=list(self._pack_options),
            entry_stage_id=str(self._flow_config.get("entry_stage_id") or ""),
            positions=positions,
            viewport=viewport,
            selected_stage_id=self._selected_stage_id,
        )
        self._refresh_selected_stage_copy()

    def _sync_layout_from_canvas(self) -> None:
        layout = dict(self._flow_config.get("layout") or {})
        layout["nodes"] = {
            stage_id: {"x": float(coords[0]), "y": float(coords[1])}
            for stage_id, coords in self._canvas.node_positions().items()
        }
        layout["viewport"] = self._canvas.viewport_state()
        self._flow_config["layout"] = layout

    def _update_canvas_node_position(self, stage_id: str, x_value: float, y_value: float) -> None:
        layout = dict(self._flow_config.get("layout") or {})
        nodes = dict(layout.get("nodes") or {})
        nodes[str(stage_id or "").strip()] = {"x": float(x_value), "y": float(y_value)}
        layout["nodes"] = nodes
        self._flow_config["layout"] = layout

    def _update_canvas_viewport(self, zoom: float, pan_x: float, pan_y: float) -> None:
        layout = dict(self._flow_config.get("layout") or {})
        viewport = dict(layout.get("viewport") or {})
        viewport["zoom"] = float(zoom)
        viewport["pan_x"] = float(pan_x)
        viewport["pan_y"] = float(pan_y)
        layout["viewport"] = viewport
        self._flow_config["layout"] = layout

    def _select_stage(self, stage_id: str) -> None:
        target = str(stage_id or "").strip()
        if target not in self._stage_ids():
            return
        previous = self._selected_stage_id
        self._selected_stage_id = target
        self._refresh_selected_stage_copy()
        self._sync_table_selection()
        if self._mode == "simple" and previous != target and self.sender() is not self._canvas:
            self._canvas.select_stage(target)

    def _select_stage_from_table(self) -> None:
        row = self._advanced_table.currentRow()
        if row < 0:
            return
        item = self._advanced_table.item(row, 0)
        stage_id = str(item.text() if item else "").strip()
        if stage_id:
            self._selected_stage_id = stage_id
            self._refresh_selected_stage_copy()
            self._canvas.select_stage(stage_id)

    def _entry_stage_changed(self) -> None:
        stage_id = str(self._entry_stage_combo.currentData() or "").strip()
        if stage_id:
            self._flow_config["entry_stage_id"] = stage_id
            self._refresh_canvas()

    def _apply_canvas_stage_payload(self, stage_id: str, payload: object) -> None:
        target = str(stage_id or "").strip()
        if not target or not isinstance(payload, dict):
            return
        stages = [dict(item) for item in self._flow_config.get("stages") or [] if isinstance(item, dict)]
        for index, stage in enumerate(stages):
            if str(stage.get("id") or "").strip() == target:
                stages[index] = dict(payload)
                break
        else:
            return
        self._flow_config["stages"] = stages
        self._refresh_tables()

    def _resolve_canvas_action_type(self, token: str) -> str:
        clean_token = str(token or "").strip().lower()
        if clean_token == "pack":
            return self._pack_options[0] if self._pack_options else _DEFAULT_CANONICAL_ACTION
        canonical = responder_module._canonical_flow_action_type(clean_token, allow_empty=True)
        if canonical:
            return canonical
        return self._CANVAS_TO_ACTION.get(clean_token, _DEFAULT_CANONICAL_ACTION)

    def _open_stage_editor(
        self,
        stage_payload: dict[str, Any],
        *,
        original_stage_id: str = "",
    ) -> dict[str, Any] | None:
        try:
            dialog = StageEditorDialog(
                stage_payload=stage_payload,
                stage_ids=self._stage_ids(),
                pack_options=self._pack_options,
                pack_names=self._pack_names,
                parent=self,
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo abrir el editor de etapas.")
            return None
        if dialog.exec() != QDialog.Accepted:
            return None
        updated = dialog.payload()
        next_stage_id = str(updated.get("id") or "").strip()
        if not next_stage_id:
            self.show_error("El ID de etapa es obligatorio.")
            return None
        stage_ids = set(self._stage_ids())
        if original_stage_id:
            stage_ids.discard(original_stage_id)
        if next_stage_id in stage_ids:
            self.show_error("Ya existe una etapa con ese ID.")
            return None
        return updated

    def _upsert_stage(
        self,
        payload: dict[str, Any],
        *,
        original_stage_id: str = "",
        x: float | None = None,
        y: float | None = None,
    ) -> None:
        target_stage_id = str(payload.get("id") or "").strip()
        stages = [dict(item) for item in self._flow_config.get("stages") or [] if isinstance(item, dict)]
        replaced = False
        if original_stage_id:
            for index, stage in enumerate(stages):
                if str(stage.get("id") or "").strip() == original_stage_id:
                    stages[index] = dict(payload)
                    replaced = True
                    break
        if not replaced:
            stages.append(dict(payload))
        self._flow_config["stages"] = stages

        layout = dict(self._flow_config.get("layout") or {})
        nodes = dict(layout.get("nodes") or {})
        if original_stage_id and original_stage_id != target_stage_id:
            if original_stage_id in nodes:
                nodes[target_stage_id] = nodes.pop(original_stage_id)
            for stage in stages:
                transitions = stage.get("transitions")
                if not isinstance(transitions, dict):
                    continue
                for key, value in list(transitions.items()):
                    if str(value or "").strip() == original_stage_id:
                        transitions[key] = target_stage_id
            if str(self._flow_config.get("entry_stage_id") or "").strip() == original_stage_id:
                self._flow_config["entry_stage_id"] = target_stage_id
        if target_stage_id not in nodes:
            nodes[target_stage_id] = {
                "x": float(x if x is not None else 160.0),
                "y": float(y if y is not None else 160.0 + len(stages) * 50.0),
            }
        layout["nodes"] = nodes
        layout.setdefault("viewport", {"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0})
        self._flow_config["layout"] = layout
        if not str(self._flow_config.get("entry_stage_id") or "").strip():
            self._flow_config["entry_stage_id"] = target_stage_id

        self._selected_stage_id = target_stage_id
        self._refresh_entry_combo()
        self._refresh_tables()
        self._refresh_canvas()
        self.set_status(f"Etapa guardada: {target_stage_id}")

    def _edit_stage(self, stage_id: str) -> None:
        target = str(stage_id or self._selected_stage_id).strip()
        stage = self._stage(target)
        if not stage:
            self.show_error("Selecciona una etapa.")
            return
        updated = self._open_stage_editor(stage, original_stage_id=target)
        if updated is None:
            return
        self._upsert_stage(updated, original_stage_id=target)

    def _edit_selected_stage(self) -> None:
        self._edit_stage(self._selected_stage_id)

    def _add_stage(
        self,
        *,
        action_type: str = _DEFAULT_CANONICAL_ACTION,
        x: float | None = None,
        y: float | None = None,
        open_editor: bool = False,
    ) -> None:
        stage_id = self._next_stage_id()
        stage_payload = _default_stage(stage_id, action_type=action_type)
        if open_editor:
            edited = self._open_stage_editor(stage_payload)
            if edited is None:
                return
            stage_payload = edited
        self._upsert_stage(stage_payload, x=x, y=y)

    def _create_stage_from_canvas(self, token: str, x: float, y: float) -> None:
        self._add_stage(
            action_type=self._resolve_canvas_action_type(token),
            x=x,
            y=y,
            open_editor=True,
        )

    def _delete_stage_by_id(self, stage_id: str) -> None:
        target = str(stage_id or "").strip()
        if not target:
            return
        for stage in self._flow_config.get("stages") or []:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("id") or "").strip() == target:
                continue
            transitions = stage.get("transitions")
            if isinstance(transitions, dict) and target in {
                str(value or "").strip() for value in transitions.values()
            }:
                self.show_error(
                    "No se puede eliminar la etapa porque sigue referenciada por otra ruta."
                )
                return
        if not confirm_automation_action(
            self,
            title="Eliminar etapa",
            message=f"Se eliminara la etapa '{target}'.",
            confirm_text="Eliminar",
            danger=True,
        ):
            return
        self._flow_config["stages"] = [
            dict(item)
            for item in self._flow_config.get("stages") or []
            if isinstance(item, dict) and str(item.get("id") or "").strip() != target
        ]
        layout = dict(self._flow_config.get("layout") or {})
        nodes = dict(layout.get("nodes") or {})
        nodes.pop(target, None)
        layout["nodes"] = nodes
        self._flow_config["layout"] = layout
        if str(self._flow_config.get("entry_stage_id") or "").strip() == target:
            self._flow_config["entry_stage_id"] = self._stage_ids()[0] if self._stage_ids() else ""
        self._selected_stage_id = self._stage_ids()[0] if self._stage_ids() else ""
        self._refresh_entry_combo()
        self._refresh_tables()
        self._refresh_canvas()
        self.set_status(f"Etapa eliminada: {target}")

    def _delete_selected_stage(self) -> None:
        self._delete_stage_by_id(self._selected_stage_id)

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        aliases = payload.get("aliases") if isinstance(payload.get("aliases"), list) else []
        selected_alias = str(payload.get("selected_alias") or self._ctx.state.active_alias).strip()
        self._alias_combo.blockSignals(True)
        self._alias_combo.clear()
        for alias in aliases:
            self._alias_combo.addItem(str(alias), str(alias))
        self._alias_combo.setCurrentIndex(max(0, self._alias_combo.findData(selected_alias)))
        self._alias_combo.blockSignals(False)

        previous_selected = self._selected_stage_id
        self._flow_config = dict(payload.get("flow_config") or _empty_flow())
        self._pack_options = [
            str(item.get("id") or "")
            for item in payload.get("pack_rows") or []
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        ]
        self._pack_names = {
            str(item.get("id") or ""): str(item.get("name") or item.get("id") or "")
            for item in payload.get("pack_rows") or []
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        if not str(self._flow_config.get("entry_stage_id") or "").strip() and self._stage_ids():
            self._flow_config["entry_stage_id"] = self._stage_ids()[0]
        if previous_selected in self._stage_ids():
            self._selected_stage_id = previous_selected
        else:
            self._selected_stage_id = str(self._flow_config.get("entry_stage_id") or "").strip()
        self._refresh_entry_combo()
        self._refresh_tables()
        self._refresh_canvas()
        self.clear_status()

    def load_flow(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        selected_alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_flow_snapshot(
                self._ctx.services,
                active_alias=self._ctx.state.active_alias,
                selected_alias=selected_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def save_flow(self) -> None:
        self._sync_layout_from_canvas()
        alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        try:
            self._flow_config = responder_module._validate_and_normalize_flow_config(self._flow_config)
        except ValueError as exc:
            self.show_error(str(exc))
            return
        self._ctx.services.automation.save_flow_config(alias, self._flow_config)
        self.set_status(f"Flow guardado para alias {alias}.")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.load_flow()

    def on_navigate_from(self) -> None:
        self._canvas_maximized = False
        self._sync_shell_visibility()

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
        self.set_status(f"No se pudo cargar el flow: {error.message}")
