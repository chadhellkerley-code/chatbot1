from __future__ import annotations

from typing import Any, Optional

from PySide6.QtCore import QPoint, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QFrame,
    QGraphicsView,
    QLabel,
    QMenu,
    QPushButton,
    QVBoxLayout,
)

from .flow_scene import FlowScene


class FlowCanvasView(QGraphicsView):
    zoom_changed = Signal(int)
    viewport_moved = Signal()

    def __init__(self, scene: FlowScene, parent=None) -> None:
        super().__init__(scene, parent)
        self._zoom = 1.0
        self._space_pressed = False
        self._panning = False
        self._pan_start = QPoint()
        self.setRenderHint(QPainter.Antialiasing, True)
        self.setRenderHint(QPainter.TextAntialiasing, True)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self.setDragMode(QGraphicsView.NoDrag)
        self.setFrameShape(QFrame.NoFrame)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setStyleSheet(
            "QGraphicsView { background: #0f1724; border: 1px solid #1f2f44; border-radius: 12px; }"
        )
        self.horizontalScrollBar().valueChanged.connect(
            lambda _value=0: self.viewport_moved.emit()
        )
        self.verticalScrollBar().valueChanged.connect(
            lambda _value=0: self.viewport_moved.emit()
        )

    @property
    def zoom_value(self) -> float:
        return float(self._zoom)

    def set_zoom_value(self, value: float) -> None:
        target = max(0.45, min(2.6, float(value or 1.0)))
        self.resetTransform()
        self._zoom = target
        self.scale(target, target)
        self.zoom_changed.emit(int(round(target * 100)))

    def keyPressEvent(self, event) -> None:
        if event.key() == Qt.Key_Space:
            self._space_pressed = True
            self.setCursor(Qt.OpenHandCursor)
            event.accept()
            return
        if event.key() in {Qt.Key_Delete, Qt.Key_Backspace}:
            scene = self.scene()
            if scene is not None and hasattr(scene, "remove_selected_edges"):
                scene.remove_selected_edges()  # type: ignore[attr-defined]
                event.accept()
                return
        super().keyPressEvent(event)

    def keyReleaseEvent(self, event) -> None:
        if event.key() == Qt.Key_Space:
            self._space_pressed = False
            if not self._panning:
                self.setCursor(Qt.ArrowCursor)
            event.accept()
            return
        super().keyReleaseEvent(event)

    def wheelEvent(self, event) -> None:
        delta = event.angleDelta().y()
        if delta == 0:
            event.ignore()
            return
        factor = 1.12 if delta > 0 else (1.0 / 1.12)
        next_zoom = max(0.45, min(2.6, self._zoom * factor))
        if abs(next_zoom - self._zoom) < 0.0001:
            event.accept()
            return
        real = next_zoom / self._zoom
        self._zoom = next_zoom
        self.scale(real, real)
        self.zoom_changed.emit(int(round(next_zoom * 100)))
        self.viewport_moved.emit()
        event.accept()

    def mousePressEvent(self, event) -> None:
        is_middle = event.button() == Qt.MiddleButton
        is_space_drag = event.button() == Qt.LeftButton and self._space_pressed
        if is_middle or is_space_drag:
            self._panning = True
            self._pan_start = event.pos()
            self.setCursor(Qt.ClosedHandCursor)
            event.accept()
            return
        self.setFocus()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event) -> None:
        if self._panning:
            delta = event.pos() - self._pan_start
            self._pan_start = event.pos()
            self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() - delta.x())
            self.verticalScrollBar().setValue(self.verticalScrollBar().value() - delta.y())
            self.viewport_moved.emit()
            event.accept()
            return

        scene = self.scene()
        if scene is not None and hasattr(scene, "has_temporary_connection"):
            if scene.has_temporary_connection():  # type: ignore[attr-defined]
                margin = 22
                speed = 10
                vp = self.viewport().rect()
                x = event.pos().x()
                y = event.pos().y()
                if x < margin:
                    self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() - speed)
                elif x > vp.width() - margin:
                    self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() + speed)
                if y < margin:
                    self.verticalScrollBar().setValue(self.verticalScrollBar().value() - speed)
                elif y > vp.height() - margin:
                    self.verticalScrollBar().setValue(self.verticalScrollBar().value() + speed)
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:
        if self._panning:
            self._panning = False
            self.setCursor(Qt.OpenHandCursor if self._space_pressed else Qt.ArrowCursor)
            self.viewport_moved.emit()
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def drawBackground(self, painter: QPainter, rect) -> None:
        painter.fillRect(rect, QColor("#121d2e"))
        grid_pen = QPen(QColor("#1a2a3d"))
        grid_pen.setWidth(1)
        painter.setPen(grid_pen)
        grid_size = 30
        left = int(rect.left()) - (int(rect.left()) % grid_size)
        top = int(rect.top()) - (int(rect.top()) % grid_size)
        x = left
        while x < int(rect.right()):
            painter.drawLine(x, int(rect.top()), x, int(rect.bottom()))
            x += grid_size
        y = top
        while y < int(rect.bottom()):
            painter.drawLine(int(rect.left()), y, int(rect.right()), y)
            y += grid_size


class FlowBuilderCanvas(QFrame):
    stage_changed = Signal(str, object)
    stage_delete_requested = Signal(str)
    create_requested = Signal(str, float, float)
    node_position_changed = Signal(str, float, float)
    stage_selected = Signal(str)
    stage_double_clicked = Signal(str)
    viewport_changed = Signal(float, float, float)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._scene = FlowScene(self)
        self._view = FlowCanvasView(self._scene, self)
        self._zoom_label_target: Optional[QLabel] = None
        self._pack_options: list[str] = []

        self._scene.stage_payload_changed.connect(self.stage_changed.emit)
        self._scene.stage_selected.connect(self.stage_selected.emit)
        self._scene.stage_double_clicked.connect(self.stage_double_clicked.emit)
        self._scene.stage_delete_requested.connect(self.stage_delete_requested.emit)
        self._scene.stage_position_changed.connect(self.node_position_changed.emit)
        self._view.zoom_changed.connect(self._on_zoom_changed)
        self._view.viewport_moved.connect(self._emit_viewport_changed)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._view, 1)

        self._new_action_button = QPushButton("+", self._view.viewport())
        self._new_action_button.setObjectName("PrimaryButton")
        self._new_action_button.setMinimumSize(34, 34)
        self._new_action_button.clicked.connect(self._open_create_menu)
        self._new_action_button.raise_()

        self._empty_hint = QLabel("Canvas vacio", self._view.viewport())
        self._empty_hint.setObjectName("MutedText")
        self._empty_hint.setStyleSheet("QLabel { background: transparent; color: #9fb0c9; }")
        self._empty_hint.raise_()

        self._empty_action_button = QPushButton("Nueva accion", self._view.viewport())
        self._empty_action_button.setObjectName("PrimaryButton")
        self._empty_action_button.setMinimumHeight(42)
        self._empty_action_button.setMinimumWidth(220)
        self._empty_action_button.clicked.connect(self._open_create_menu)
        self._empty_action_button.raise_()
        self._position_overlay_controls()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._position_overlay_controls()

    def set_zoom_label_target(self, label: Optional[QLabel]) -> None:
        self._zoom_label_target = label
        self._on_zoom_changed(int(round(self._view.zoom_value * 100)))

    def _on_zoom_changed(self, zoom_percent: int) -> None:
        if self._zoom_label_target is not None:
            self._zoom_label_target.setText(f"Zoom: {int(zoom_percent)}%")
        self._emit_viewport_changed()

    def _emit_viewport_changed(self) -> None:
        self.viewport_changed.emit(
            float(self._view.zoom_value),
            float(self._view.horizontalScrollBar().value()),
            float(self._view.verticalScrollBar().value()),
        )

    def _position_overlay_controls(self) -> None:
        viewport = self._view.viewport()
        self._new_action_button.move(
            max(12, viewport.width() - self._new_action_button.width() - 12),
            max(12, viewport.height() - self._new_action_button.height() - 12),
        )
        self._empty_hint.adjustSize()
        self._empty_hint.move(
            max(12, int((viewport.width() - self._empty_hint.width()) / 2)),
            max(56, int((viewport.height() - self._empty_hint.height()) / 2) - 30),
        )
        self._empty_action_button.move(
            max(12, int((viewport.width() - self._empty_action_button.width()) / 2)),
            max(90, int((viewport.height() - self._empty_action_button.height()) / 2) + 8),
        )

    def _open_create_menu(self) -> None:
        menu = QMenu(self)
        menu.setStyleSheet(
            "QMenu { background: #111827; color: #d7e3f5; border: 1px solid #334155; "
            "border-radius: 8px; padding: 6px; }"
            "QMenu::item { padding: 7px 12px; border-radius: 6px; }"
            "QMenu::item:selected { background: #1f3250; }"
        )
        items = [
            ("crear_etapa", "Crear etapa"),
            ("condicion", "Crear condicion"),
            ("followup", "Agregar follow-up"),
            ("transicion", "Agregar transicion"),
            ("accion_ia", "Agregar accion IA"),
            ("final", "Agregar bloque final"),
        ]
        if self._pack_options:
            items.insert(5, ("pack", "Agregar pack"))
        scene_point = self._view.mapToScene(self._view.viewport().rect().center())
        target_x = float(scene_point.x())
        target_y = float(scene_point.y())
        for key, label in items:
            action = menu.addAction(label)
            action.triggered.connect(
                lambda checked=False, token=key, x=target_x, y=target_y: self.create_requested.emit(
                    token, x, y
                )
            )
        sender = self.sender()
        if sender is self._empty_action_button:
            anchor = self._empty_action_button.mapToGlobal(self._empty_action_button.rect().bottomLeft())
        else:
            anchor = self._new_action_button.mapToGlobal(self._new_action_button.rect().topLeft())
        menu.exec(anchor)

    def set_flow(
        self,
        *,
        stages: list[dict[str, Any]],
        pack_options: list[str],
        entry_stage_id: str = "",
        positions: Optional[dict[str, tuple[float, float]]] = None,
        viewport: Optional[dict[str, float]] = None,
        selected_stage_id: str = "",
    ) -> None:
        self._pack_options = [str(item or "").strip() for item in pack_options if str(item or "").strip()]
        selected_token = str(selected_stage_id or "").strip()
        self._scene.set_stages(
            stages=stages,
            entry_stage_id=entry_stage_id,
            positions=positions,
        )
        has_nodes = len(stages) > 0
        self._empty_hint.setVisible(not has_nodes)
        self._empty_action_button.setVisible(not has_nodes)
        self._new_action_button.setVisible(has_nodes)
        self._position_overlay_controls()

        viewport_payload = dict(viewport or {})
        zoom_value = float(viewport_payload.get("zoom") or 1.0)
        pan_x = float(viewport_payload.get("pan_x") or 0.0)
        pan_y = float(viewport_payload.get("pan_y") or 0.0)
        self._view.set_zoom_value(zoom_value)
        self._view.horizontalScrollBar().setValue(int(round(pan_x)))
        self._view.verticalScrollBar().setValue(int(round(pan_y)))
        if selected_token:
            self._scene.select_stage(selected_token, emit_signal=False)
        self._emit_viewport_changed()

    def node_positions(self) -> dict[str, tuple[float, float]]:
        return self._scene.node_positions()

    def viewport_state(self) -> dict[str, float]:
        return {
            "zoom": float(self._view.zoom_value),
            "pan_x": float(self._view.horizontalScrollBar().value()),
            "pan_y": float(self._view.verticalScrollBar().value()),
        }

    def select_stage(self, stage_id: str) -> None:
        self._scene.select_stage(str(stage_id or "").strip(), emit_signal=False)

    def refresh_from_payload(self, stage_id: str, payload: dict[str, Any]) -> None:
        self._scene.update_stage_payload(stage_id, payload)
