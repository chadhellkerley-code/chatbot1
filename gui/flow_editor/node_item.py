from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from PySide6.QtCore import QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QGraphicsItem,
    QGraphicsObject,
    QStyleOptionGraphicsItem,
    QWidget,
)

from .port_item import PortItem

if TYPE_CHECKING:
    from .edge_item import EdgeItem


class NodeItem(QGraphicsObject):
    selected = Signal(str)
    double_clicked = Signal(str)
    edit_requested = Signal(str)
    delete_requested = Signal(str)
    payload_changed = Signal(str, object)
    position_changed = Signal(str, float, float)

    SNAP_GRID = 10.0

    def __init__(
        self,
        stage_id: str,
        stage_payload: dict[str, Any],
        *,
        entry_stage: bool = False,
        parent: Optional[QGraphicsItem] = None,
    ) -> None:
        super().__init__(parent)
        self.stage_id = str(stage_id or "").strip()
        self._payload: dict[str, Any] = dict(stage_payload or {})
        self._entry_stage = bool(entry_stage)
        self._edges: set["EdgeItem"] = set()
        self._hover_delete = False
        self._hover_edit = False
        self._rect = QRectF(0.0, 0.0, 292.0, 142.0)
        self._edit_rect = QRectF(self._rect.width() - 46.0, 8.0, 16.0, 16.0)
        self._delete_rect = QRectF(self._rect.width() - 24.0, 8.0, 16.0, 16.0)

        self.setFlag(QGraphicsItem.ItemIsMovable, True)
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setFlag(QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setFlag(QGraphicsItem.ItemIsFocusable, True)
        self.setAcceptHoverEvents(True)
        self.setAcceptedMouseButtons(Qt.LeftButton)
        self.setZValue(2)

        self.input_port = PortItem(self, kind=PortItem.KIND_INPUT, connection_type="input")
        self.output_ports = {
            "positive": PortItem(self, kind=PortItem.KIND_OUTPUT, connection_type="positive"),
            "negative": PortItem(self, kind=PortItem.KIND_OUTPUT, connection_type="negative"),
            "doubt": PortItem(self, kind=PortItem.KIND_OUTPUT, connection_type="doubt"),
            "neutral": PortItem(self, kind=PortItem.KIND_OUTPUT, connection_type="neutral"),
        }
        self._layout_ports()

    def boundingRect(self) -> QRectF:
        return self._rect.adjusted(-14.0, -12.0, 14.0, 12.0)

    def set_entry_stage(self, value: bool) -> None:
        self._entry_stage = bool(value)
        self.update()

    def add_edge(self, edge: "EdgeItem") -> None:
        self._edges.add(edge)

    def remove_edge(self, edge: "EdgeItem") -> None:
        if edge in self._edges:
            self._edges.remove(edge)

    def stage_payload(self) -> dict[str, Any]:
        payload = dict(self._payload)
        payload["id"] = self.stage_id
        return payload

    def set_stage_payload(self, payload: dict[str, Any]) -> None:
        self._payload = dict(payload or {})
        self._payload["id"] = self.stage_id
        self.update()
        self.payload_changed.emit(self.stage_id, self.stage_payload())

    def output_port(self, connection_type: str) -> Optional[PortItem]:
        return self.output_ports.get(str(connection_type or "").strip().lower())

    def _layout_ports(self) -> None:
        center_y = self._rect.top() + self._rect.height() / 2.0
        self.input_port.setPos(self._rect.left() - 6.0, center_y)
        base_y = self._rect.top() + 45.0
        step = 22.0
        for index, key in enumerate(("positive", "negative", "doubt", "neutral")):
            self.output_ports[key].setPos(self._rect.right() + 6.0, base_y + step * float(index))

    def _pack_text(self) -> str:
        return str(self._payload.get("action_type") or "").strip() or "Sin pack"

    def _objections_text(self) -> str:
        post_raw = self._payload.get("post_objection")
        post = dict(post_raw) if isinstance(post_raw, dict) else {}
        return "ON" if bool(post.get("enabled", False)) else "OFF"

    def _followups_text(self) -> str:
        followups = self._payload.get("followups")
        if not isinstance(followups, list):
            return "0"
        count = len([item for item in followups if isinstance(item, dict)])
        return str(count)

    def paint(
        self,
        painter: QPainter,
        option: QStyleOptionGraphicsItem,
        widget: Optional[QWidget] = None,
    ) -> None:
        painter.setRenderHint(QPainter.Antialiasing, True)
        outer = QPainterPath()
        outer.addRoundedRect(self._rect, 11.0, 11.0)

        if self.isSelected():
            border = QColor("#4f86d6")
            glow = QColor("#1f3f68")
        else:
            border = QColor("#274666")
            glow = QColor("#12243a")
        fill = QColor("#0f1b2d")

        painter.setPen(QPen(glow, 6.0))
        painter.setBrush(Qt.NoBrush)
        painter.drawPath(outer)

        painter.setPen(QPen(border, 1.4))
        painter.setBrush(fill)
        painter.drawPath(outer)

        edit_bg = QColor("#2a4f88") if self._hover_edit else QColor("#233b60")
        painter.setPen(QPen(QColor("#6da0e4"), 1.0))
        painter.setBrush(edit_bg)
        painter.drawRoundedRect(self._edit_rect, 4.0, 4.0)
        painter.setPen(QPen(QColor("#d8e8ff"), 1.1))
        painter.drawLine(
            self._edit_rect.left() + 4.0,
            self._edit_rect.bottom() - 4.0,
            self._edit_rect.right() - 4.0,
            self._edit_rect.top() + 4.0,
        )

        delete_bg = QColor("#872a2a") if self._hover_delete else QColor("#642020")
        painter.setPen(QPen(QColor("#be5b5b"), 1.0))
        painter.setBrush(delete_bg)
        painter.drawRoundedRect(self._delete_rect, 4.0, 4.0)
        painter.setPen(QPen(QColor("#ffd9d9"), 1.2))
        painter.drawLine(
            self._delete_rect.left() + 4.0,
            self._delete_rect.top() + 4.0,
            self._delete_rect.right() - 4.0,
            self._delete_rect.bottom() - 4.0,
        )
        painter.drawLine(
            self._delete_rect.right() - 4.0,
            self._delete_rect.top() + 4.0,
            self._delete_rect.left() + 4.0,
            self._delete_rect.bottom() - 4.0,
        )

        title_font = QFont()
        title_font.setBold(True)
        title_font.setPointSize(10)
        painter.setFont(title_font)
        painter.setPen(QPen(QColor("#f4f8ff")))
        stage_label = f"Etapa {self.stage_id}"
        if self._entry_stage:
            stage_label = f"{stage_label} | Inicio"
        painter.drawText(
            QRectF(self._rect.left() + 12.0, self._rect.top() + 10.0, self._rect.width() - 42.0, 22.0),
            Qt.AlignLeft | Qt.AlignVCenter,
            stage_label,
        )

        meta_font = QFont()
        meta_font.setPointSize(9)
        painter.setFont(meta_font)
        painter.setPen(QPen(QColor("#c3d3e8")))
        painter.drawText(
            QRectF(self._rect.left() + 12.0, self._rect.top() + 36.0, self._rect.width() - 24.0, 18.0),
            Qt.AlignLeft | Qt.AlignVCenter,
            f"Pack: {self._pack_text()}",
        )
        painter.drawText(
            QRectF(self._rect.left() + 12.0, self._rect.top() + 58.0, self._rect.width() - 24.0, 18.0),
            Qt.AlignLeft | Qt.AlignVCenter,
            f"Objeciones: {self._objections_text()}",
        )
        painter.drawText(
            QRectF(self._rect.left() + 12.0, self._rect.top() + 80.0, self._rect.width() - 24.0, 18.0),
            Qt.AlignLeft | Qt.AlignVCenter,
            f"Follow-ups: {self._followups_text()}",
        )

        painter.setPen(QPen(QColor("#9fb3cd")))
        painter.drawText(
            QRectF(self._rect.left() + 12.0, self._rect.top() + 104.0, self._rect.width() - 24.0, 18.0),
            Qt.AlignLeft | Qt.AlignVCenter,
            "Salidas: +  -  ?  =",
        )

    def itemChange(self, change: QGraphicsItem.GraphicsItemChange, value):
        if change == QGraphicsItem.ItemPositionChange:
            point = QPointF(value)
            if self.SNAP_GRID > 0:
                snap = float(self.SNAP_GRID)
                point.setX(round(point.x() / snap) * snap)
                point.setY(round(point.y() / snap) * snap)
            return point
        if change == QGraphicsItem.ItemPositionHasChanged:
            for edge in tuple(self._edges):
                try:
                    edge.update_position()
                except Exception:
                    continue
            pos = self.pos()
            self.position_changed.emit(self.stage_id, float(pos.x()), float(pos.y()))
        return super().itemChange(change, value)

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton and self._edit_rect.contains(event.pos()):
            self.edit_requested.emit(self.stage_id)
            event.accept()
            return
        if event.button() == Qt.LeftButton and self._delete_rect.contains(event.pos()):
            self.delete_requested.emit(self.stage_id)
            event.accept()
            return
        self.selected.emit(self.stage_id)
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:
        self.double_clicked.emit(self.stage_id)
        event.accept()

    def hoverMoveEvent(self, event) -> None:
        hover_edit = bool(self._edit_rect.contains(event.pos()))
        hovered = bool(self._delete_rect.contains(event.pos()))
        if hover_edit != self._hover_edit or hovered != self._hover_delete:
            self._hover_edit = hover_edit
            self._hover_delete = hovered
            self.update()
        super().hoverMoveEvent(event)

    def hoverLeaveEvent(self, event) -> None:
        if self._hover_delete or self._hover_edit:
            self._hover_delete = False
            self._hover_edit = False
            self.update()
        super().hoverLeaveEvent(event)
