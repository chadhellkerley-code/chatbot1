from __future__ import annotations

from typing import Any, Optional

from PySide6.QtCore import QPointF, QRectF, Signal
from PySide6.QtGui import QColor, QPainterPath, QPen, QBrush, QFont
from PySide6.QtGui import QTransform
from PySide6.QtWidgets import QGraphicsPathItem, QGraphicsScene, QGraphicsSimpleTextItem

from .edge_item import EdgeItem
from .node_item import NodeItem
from .port_item import PortItem


class FlowScene(QGraphicsScene):
    stage_payload_changed = Signal(str, object)
    stage_selected = Signal(str)
    stage_double_clicked = Signal(str)
    stage_delete_requested = Signal(str)
    stage_position_changed = Signal(str, float, float)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._nodes: dict[str, NodeItem] = {}
        self._edges: dict[tuple[str, str], EdgeItem] = {}
        self._temp_source_port: Optional[PortItem] = None
        self._temp_edge: Optional[EdgeItem] = None
        self._temp_hint_bg: Optional[QGraphicsPathItem] = None
        self._temp_hint_text: Optional[QGraphicsSimpleTextItem] = None
        self._base_half_extent = 50000.0
        self.setSceneRect(
            -self._base_half_extent,
            -self._base_half_extent,
            self._base_half_extent * 2.0,
            self._base_half_extent * 2.0,
        )

    def clear_flow(self) -> None:
        self._cancel_temporary_connection()
        self.clear()
        self._nodes.clear()
        self._edges.clear()

    def set_stages(
        self,
        *,
        stages: list[dict[str, Any]],
        entry_stage_id: str = "",
        positions: Optional[dict[str, tuple[float, float]]] = None,
    ) -> None:
        self.clear_flow()
        coordinates = dict(positions or {})
        entry_id = str(entry_stage_id or "").strip()

        index = 0
        for raw in stages:
            if not isinstance(raw, dict):
                continue
            stage_payload = dict(raw)
            stage_id = str(stage_payload.get("id") or "").strip()
            if not stage_id:
                continue
            node = NodeItem(stage_id, stage_payload, entry_stage=(stage_id == entry_id))
            node.selected.connect(self.stage_selected.emit)
            node.double_clicked.connect(self.stage_double_clicked.emit)
            node.edit_requested.connect(self.stage_double_clicked.emit)
            node.delete_requested.connect(self.stage_delete_requested.emit)
            node.position_changed.connect(self._on_node_position_changed)
            self.addItem(node)
            x_value, y_value = coordinates.get(stage_id, self._default_position(index))
            node.setPos(float(x_value), float(y_value))
            self._nodes[stage_id] = node
            index += 1

        self._rebuild_all_edges_from_payload()
        self._update_scene_rect()

    def _default_position(self, index: int) -> tuple[float, float]:
        col = index % 3
        row = index // 3
        return (120.0 + float(col) * 420.0, 120.0 + float(row) * 250.0)

    def node_positions(self) -> dict[str, tuple[float, float]]:
        positions: dict[str, tuple[float, float]] = {}
        for stage_id, node in self._nodes.items():
            pos = node.pos()
            positions[stage_id] = (float(pos.x()), float(pos.y()))
        return positions

    def select_stage(self, stage_id: str, *, emit_signal: bool = True) -> None:
        target = str(stage_id or "").strip()
        if not target:
            return
        node = self._nodes.get(target)
        if node is None:
            return
        self.clearSelection()
        node.setSelected(True)
        if emit_signal:
            self.stage_selected.emit(target)

    def update_stage_payload(self, stage_id: str, payload: dict[str, Any]) -> None:
        target = str(stage_id or "").strip()
        node = self._nodes.get(target)
        if node is None:
            return
        node.set_stage_payload(dict(payload or {}))
        self._sync_edges_for_source(target)
        self._update_scene_rect()

    def set_transition(self, source_id: str, transition_type: str, target_id: str) -> None:
        source = str(source_id or "").strip()
        transition = str(transition_type or "").strip().lower()
        target = str(target_id or "").strip()
        node = self._nodes.get(source)
        if node is None or transition not in {"positive", "negative", "doubt", "neutral"}:
            return
        payload = node.stage_payload()
        transitions_raw = payload.get("transitions")
        transitions = dict(transitions_raw) if isinstance(transitions_raw, dict) else {}
        transitions[transition] = target
        payload["transitions"] = transitions
        node.set_stage_payload(payload)
        self.stage_payload_changed.emit(source, payload)
        self._sync_edges_for_source(source)

    def remove_selected_edges(self) -> int:
        removed = 0
        for item in list(self.selectedItems()):
            if not isinstance(item, EdgeItem) or item.temporary:
                continue
            if self._remove_edge_item(item, clear_transition=True):
                removed += 1
        if removed > 0:
            self._update_scene_rect()
        return removed

    def begin_temporary_connection(self, source_port: PortItem) -> None:
        if source_port is None or not source_port.is_output_port():
            return
        self._cancel_temporary_connection()
        self._temp_source_port = source_port
        self._temp_edge = EdgeItem(
            source_port,
            None,
            connection_type=source_port.connection_type,
            temporary=True,
        )
        self.addItem(self._temp_edge)
        start_pos = source_port.scene_center()
        self._temp_edge.set_temporary_target(start_pos)
        self._show_temporary_hint(source_port.connection_type, start_pos)

    def has_temporary_connection(self) -> bool:
        return self._temp_edge is not None

    def update_temporary_connection(self, scene_pos: QPointF) -> None:
        if self._temp_edge is None:
            return
        self._temp_edge.set_temporary_target(scene_pos)
        self._move_temporary_hint(scene_pos)

    def _finish_temporary_connection(self, scene_pos: QPointF) -> None:
        source_port = self._temp_source_port
        if source_port is None:
            self._cancel_temporary_connection()
            return
        target_port = self._find_input_port(scene_pos)
        self._cancel_temporary_connection()
        if target_port is None:
            return
        if target_port.node_item.stage_id == source_port.node_item.stage_id:
            return
        self.set_transition(
            source_port.node_item.stage_id,
            source_port.connection_type,
            target_port.node_item.stage_id,
        )

    def _cancel_temporary_connection(self) -> None:
        if self._temp_edge is not None:
            self.removeItem(self._temp_edge)
        self._remove_temporary_hint()
        self._temp_source_port = None
        self._temp_edge = None

    @staticmethod
    def _connection_label(connection_type: str) -> str:
        token = str(connection_type or "").strip().lower()
        if token == "positive":
            return "Positivo (+)"
        if token == "negative":
            return "Negativo (-)"
        if token == "doubt":
            return "Duda (?)"
        if token == "neutral":
            return "Neutral (=)"
        return "Transición"

    def _show_temporary_hint(self, connection_type: str, scene_pos: QPointF) -> None:
        self._remove_temporary_hint()
        label = self._connection_label(connection_type)
        accent = EdgeItem.color_for_type(connection_type)

        text_item = QGraphicsSimpleTextItem(label)
        font = QFont()
        font.setPointSize(9)
        font.setBold(True)
        text_item.setFont(font)
        text_item.setBrush(QBrush(QColor("#dbeafe")))
        text_item.setZValue(60)

        text_rect = text_item.boundingRect()
        pad_x = 8.0
        pad_y = 4.0
        bg_rect = text_rect.adjusted(-pad_x, -pad_y, pad_x, pad_y)
        bg_path = QPainterPath()
        bg_path.addRoundedRect(bg_rect, 7.0, 7.0)
        bg_item = QGraphicsPathItem(bg_path)
        bg_item.setPen(QPen(accent.lighter(115), 1.0))
        bg_item.setBrush(QBrush(QColor(10, 21, 38, 220)))
        bg_item.setZValue(59)

        self.addItem(bg_item)
        self.addItem(text_item)
        self._temp_hint_bg = bg_item
        self._temp_hint_text = text_item
        self._move_temporary_hint(scene_pos)

    def _move_temporary_hint(self, scene_pos: QPointF) -> None:
        if self._temp_hint_bg is None or self._temp_hint_text is None:
            return
        text_rect = self._temp_hint_text.boundingRect()
        pos = QPointF(
            float(scene_pos.x()) + 14.0,
            float(scene_pos.y()) - float(text_rect.height()) - 24.0,
        )
        self._temp_hint_text.setPos(pos)
        self._temp_hint_bg.setPos(pos)

    def _remove_temporary_hint(self) -> None:
        if self._temp_hint_bg is not None:
            self.removeItem(self._temp_hint_bg)
            self._temp_hint_bg = None
        if self._temp_hint_text is not None:
            self.removeItem(self._temp_hint_text)
            self._temp_hint_text = None

    def _find_input_port(self, scene_pos: QPointF) -> Optional[PortItem]:
        item = self.itemAt(scene_pos, QTransform())
        if isinstance(item, PortItem) and item.is_input_port():
            return item
        candidates = self.items(QRectF(scene_pos.x() - 8.0, scene_pos.y() - 8.0, 16.0, 16.0))
        for candidate in candidates:
            if isinstance(candidate, PortItem) and candidate.is_input_port():
                return candidate
        return None

    def mouseMoveEvent(self, event) -> None:
        if self._temp_edge is not None:
            self.update_temporary_connection(event.scenePos())
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:
        if self._temp_edge is not None:
            self._finish_temporary_connection(event.scenePos())
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def _on_node_position_changed(self, stage_id: str, x_value: float, y_value: float) -> None:
        self.stage_position_changed.emit(stage_id, float(x_value), float(y_value))

    def _sync_edges_for_source(self, source_id: str) -> None:
        source = str(source_id or "").strip()
        if not source:
            return
        for key, edge in list(self._edges.items()):
            if key[0] != source:
                continue
            self._remove_edge_item(edge, clear_transition=False)

        node = self._nodes.get(source)
        if node is None:
            return
        payload = node.stage_payload()
        transitions_raw = payload.get("transitions")
        transitions = dict(transitions_raw) if isinstance(transitions_raw, dict) else {}
        for key in ("positive", "negative", "doubt", "neutral"):
            target_id = str(transitions.get(key) or "").strip()
            if not target_id:
                continue
            target_node = self._nodes.get(target_id)
            if target_node is None:
                continue
            source_port = node.output_port(key)
            if source_port is None:
                continue
            edge = EdgeItem(source_port, target_node.input_port, connection_type=key, temporary=False)
            self._attach_edge(edge)
        self._update_scene_rect()

    def _rebuild_all_edges_from_payload(self) -> None:
        for edge in list(self._edges.values()):
            self._remove_edge_item(edge, clear_transition=False)
        for stage_id in list(self._nodes.keys()):
            self._sync_edges_for_source(stage_id)

    def _attach_edge(self, edge: EdgeItem) -> None:
        key = (edge.source_stage_id, edge.connection_type)
        old = self._edges.get(key)
        if old is not None:
            self._remove_edge_item(old, clear_transition=False)
        self._edges[key] = edge
        self.addItem(edge)
        edge.source_port.node_item.add_edge(edge)
        if edge.target_port is not None:
            edge.target_port.node_item.add_edge(edge)
        edge.update_position()

    def _remove_edge_item(self, edge: EdgeItem, *, clear_transition: bool) -> bool:
        source_id = edge.source_stage_id
        transition_type = edge.connection_type
        key = (source_id, transition_type)
        existing = self._edges.get(key)
        if existing is not edge:
            return False
        self._edges.pop(key, None)
        edge.source_port.node_item.remove_edge(edge)
        if edge.target_port is not None:
            edge.target_port.node_item.remove_edge(edge)
        self.removeItem(edge)
        if clear_transition:
            node = self._nodes.get(source_id)
            if node is not None:
                payload = node.stage_payload()
                transitions_raw = payload.get("transitions")
                transitions = dict(transitions_raw) if isinstance(transitions_raw, dict) else {}
                transitions[transition_type] = ""
                payload["transitions"] = transitions
                node.set_stage_payload(payload)
                self.stage_payload_changed.emit(source_id, payload)
        return True

    def _update_scene_rect(self) -> None:
        current = self.sceneRect()
        min_left = min(float(current.left()), -self._base_half_extent)
        min_top = min(float(current.top()), -self._base_half_extent)
        max_right = max(float(current.right()), self._base_half_extent)
        max_bottom = max(float(current.bottom()), self._base_half_extent)

        if self._nodes:
            bounds = self.itemsBoundingRect().adjusted(-1600.0, -1400.0, 1600.0, 1400.0)
            min_left = min(min_left, float(bounds.left()))
            min_top = min(min_top, float(bounds.top()))
            max_right = max(max_right, float(bounds.right()))
            max_bottom = max(max_bottom, float(bounds.bottom()))

        self.setSceneRect(
            min_left,
            min_top,
            max(1.0, max_right - min_left),
            max(1.0, max_bottom - min_top),
        )
