from __future__ import annotations

import math
from typing import Optional

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen, QPolygonF
from PySide6.QtWidgets import QGraphicsPathItem, QStyleOptionGraphicsItem, QWidget

from .port_item import PortItem


class EdgeItem(QGraphicsPathItem):
    _COLOR_MAP = {
        "positive": QColor("#6cd99c"),
        "negative": QColor("#ff8e8e"),
        "doubt": QColor("#f3c45a"),
        "neutral": QColor("#84b9ff"),
    }

    def __init__(
        self,
        source_port: PortItem,
        target_port: Optional[PortItem],
        *,
        connection_type: str,
        temporary: bool = False,
    ) -> None:
        super().__init__()
        self.source_port = source_port
        self.target_port = target_port
        self.connection_type = str(connection_type or "").strip().lower()
        self.temporary = bool(temporary)
        self._target_point = QPointF()
        self._arrow = QPolygonF()
        self._base_pen = QPen(self.color_for_type(self.connection_type), 2.2)
        self._base_pen.setCapStyle(Qt.RoundCap)
        self._base_pen.setJoinStyle(Qt.RoundJoin)
        self.setPen(self._base_pen)
        self.setZValue(-5)
        if not self.temporary:
            self.setFlag(QGraphicsPathItem.ItemIsSelectable, True)
        self.update_position()

    @staticmethod
    def color_for_type(connection_type: str) -> QColor:
        token = str(connection_type or "").strip().lower()
        return QColor(EdgeItem._COLOR_MAP.get(token) or QColor("#7d91ac"))

    @property
    def source_stage_id(self) -> str:
        return str(self.source_port.node_item.stage_id or "").strip()

    @property
    def target_stage_id(self) -> str:
        if self.target_port is None:
            return ""
        return str(self.target_port.node_item.stage_id or "").strip()

    def set_temporary_target(self, scene_pos: QPointF) -> None:
        self._target_point = QPointF(scene_pos)
        self.update_position()

    def update_position(self) -> None:
        try:
            start = self.source_port.scene_center()
            if self.target_port is not None:
                end = self.target_port.scene_center()
            else:
                end = QPointF(self._target_point)
        except Exception:
            return

        path = QPainterPath()
        path.moveTo(start)
        dx = end.x() - start.x()
        ctrl = max(90.0, abs(dx) * 0.45)
        if dx >= 0:
            c1 = QPointF(start.x() + ctrl, start.y())
            c2 = QPointF(end.x() - ctrl, end.y())
        else:
            c1 = QPointF(start.x() - ctrl, start.y())
            c2 = QPointF(end.x() + ctrl, end.y())
        path.cubicTo(c1, c2, end)
        self.setPath(path)

        self._arrow = QPolygonF()
        if (end - start).manhattanLength() < 1.0:
            return

        tip = end
        angle = math.atan2(end.y() - c2.y(), end.x() - c2.x())
        arrow_len = 9.0
        p1 = QPointF(
            tip.x() - arrow_len * math.cos(angle - 0.48),
            tip.y() - arrow_len * math.sin(angle - 0.48),
        )
        p2 = QPointF(
            tip.x() - arrow_len * math.cos(angle + 0.48),
            tip.y() - arrow_len * math.sin(angle + 0.48),
        )
        self._arrow = QPolygonF([tip, p1, p2])

    def paint(
        self,
        painter: QPainter,
        option: QStyleOptionGraphicsItem,
        widget: Optional[QWidget] = None,
    ) -> None:
        pen = QPen(self._base_pen)
        if self.isSelected() and not self.temporary:
            pen.setWidthF(3.4)
            pen.setColor(pen.color().lighter(130))
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(pen)
        painter.setBrush(Qt.NoBrush)  # type: ignore[name-defined]
        painter.drawPath(self.path())
        if not self._arrow.isEmpty():
            painter.setBrush(pen.color())
            painter.setPen(Qt.NoPen)  # type: ignore[name-defined]
            painter.drawPolygon(self._arrow)
