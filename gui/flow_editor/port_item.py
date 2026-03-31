from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPen
from PySide6.QtWidgets import QGraphicsEllipseItem

if TYPE_CHECKING:
    from PySide6.QtWidgets import QGraphicsSceneMouseEvent
    from .node_item import NodeItem


class PortItem(QGraphicsEllipseItem):
    KIND_INPUT = "input"
    KIND_OUTPUT = "output"

    _COLOR_MAP = {
        "input": QColor("#7d91ac"),
        "positive": QColor("#6cd99c"),
        "negative": QColor("#ff8e8e"),
        "doubt": QColor("#f3c45a"),
        "neutral": QColor("#84b9ff"),
    }

    def __init__(
        self,
        node_item: "NodeItem",
        *,
        kind: str,
        connection_type: str,
        radius: float = 6.0,
    ) -> None:
        super().__init__(-radius, -radius, radius * 2.0, radius * 2.0, node_item)
        self.node_item = node_item
        self.kind = str(kind or "").strip().lower()
        self.connection_type = str(connection_type or "").strip().lower()
        self.radius = float(radius)
        self.setAcceptHoverEvents(True)
        self.setAcceptedMouseButtons(Qt.LeftButton)
        self.setZValue(8)
        self._hovered = False
        self._apply_style()

    def is_input_port(self) -> bool:
        return self.kind == self.KIND_INPUT

    def is_output_port(self) -> bool:
        return self.kind == self.KIND_OUTPUT

    def scene_center(self):
        return self.mapToScene(self.rect().center())

    def _base_color(self) -> QColor:
        return QColor(self._COLOR_MAP.get(self.connection_type) or QColor("#7d91ac"))

    def _apply_style(self) -> None:
        base = self._base_color()
        if self._hovered:
            fill = base.lighter(125)
            border = base.lighter(140)
        else:
            fill = base
            border = base.darker(110)
        self.setBrush(fill)
        self.setPen(QPen(border, 1.5))

    def hoverEnterEvent(self, event) -> None:
        self._hovered = True
        self._apply_style()
        super().hoverEnterEvent(event)

    def hoverLeaveEvent(self, event) -> None:
        self._hovered = False
        self._apply_style()
        super().hoverLeaveEvent(event)

    def mousePressEvent(self, event: "QGraphicsSceneMouseEvent") -> None:
        if event.button() == Qt.LeftButton and self.is_output_port():
            scene = self.scene()
            if scene is not None and hasattr(scene, "begin_temporary_connection"):
                scene.begin_temporary_connection(self)  # type: ignore[attr-defined]
                event.accept()
                return
        super().mousePressEvent(event)
