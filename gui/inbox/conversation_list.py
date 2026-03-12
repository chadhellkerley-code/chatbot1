from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QRectF, QSize, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import (
    QButtonGroup,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListView,
    QPushButton,
    QScrollBar,
    QStyledItemDelegate,
    QVBoxLayout,
    QWidget,
)


class ConversationListModel(QAbstractListModel):
    ThreadRole = Qt.UserRole + 1
    SelectedRole = Qt.UserRole + 2

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._all_rows: list[dict[str, Any]] = []
        self._visible_rows: list[dict[str, Any]] = []
        self._page_size = 50
        self._current_thread_key = ""

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        if parent.isValid():
            return 0
        return len(self._visible_rows)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # type: ignore[override]
        if not index.isValid() or index.row() < 0 or index.row() >= len(self._visible_rows):
            return None
        row = self._visible_rows[index.row()]
        if role == Qt.DisplayRole:
            return str(row.get("display_name") or "").strip()
        if role == self.ThreadRole:
            return row
        if role == self.SelectedRole:
            return str(row.get("thread_key") or "").strip() == self._current_thread_key
        return None

    def thread_at(self, row: int) -> dict[str, Any] | None:
        if row < 0 or row >= len(self._visible_rows):
            return None
        return dict(self._visible_rows[row])

    def set_threads(self, rows: list[dict[str, Any]], *, current_thread_key: str = "") -> None:
        clean_rows = [dict(row) for row in rows if isinstance(row, dict)]
        clean_key = str(current_thread_key or "").strip()
        selected_index = -1
        for index, row in enumerate(clean_rows):
            if str(row.get("thread_key") or "").strip() == clean_key:
                selected_index = index
                break
        target_limit = min(
            len(clean_rows),
            max(self._page_size, selected_index + 1 if selected_index >= 0 else self._page_size),
        )
        self.beginResetModel()
        self._all_rows = clean_rows
        self._visible_rows = clean_rows[:target_limit]
        self._current_thread_key = clean_key
        self.endResetModel()

    def set_current_thread(self, thread_key: str) -> None:
        clean_key = str(thread_key or "").strip()
        if clean_key == self._current_thread_key:
            return
        previous_row = self.row_for_thread(self._current_thread_key)
        self._current_thread_key = clean_key
        current_row = self.row_for_thread(clean_key)
        for row in {previous_row, current_row}:
            if row < 0:
                continue
            model_index = self.index(row, 0)
            self.dataChanged.emit(model_index, model_index, [self.SelectedRole])

    def row_for_thread(self, thread_key: str) -> int:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return -1
        for index, row in enumerate(self._visible_rows):
            if str(row.get("thread_key") or "").strip() == clean_key:
                return index
        return -1

    def ensure_visible(self, thread_key: str) -> int:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return -1
        for index, row in enumerate(self._all_rows):
            if str(row.get("thread_key") or "").strip() != clean_key:
                continue
            if index >= len(self._visible_rows):
                self.load_more(index + 1 - len(self._visible_rows))
            return self.row_for_thread(clean_key)
        return -1

    def can_load_more(self) -> bool:
        return len(self._visible_rows) < len(self._all_rows)

    def load_more(self, count: int | None = None) -> bool:
        if not self.can_load_more():
            return False
        old_count = len(self._visible_rows)
        grow = max(self._page_size, int(count or self._page_size))
        new_count = min(len(self._all_rows), old_count + grow)
        if new_count <= old_count:
            return False
        self.beginInsertRows(QModelIndex(), old_count, new_count - 1)
        self._visible_rows = self._all_rows[:new_count]
        self.endInsertRows()
        return True


class ConversationListDelegate(QStyledItemDelegate):
    def paint(self, painter: QPainter, option, index: QModelIndex) -> None:  # type: ignore[override]
        row = index.data(ConversationListModel.ThreadRole)
        if not isinstance(row, dict):
            return
        selected = bool(index.data(ConversationListModel.SelectedRole))

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, True)
        rect = option.rect.adjusted(6, 4, -6, -4)

        bg = QColor("#102038") if selected else QColor("#0d1727")
        border = QColor("#4f86d6") if selected else QColor("#1f3550")
        painter.setPen(QPen(border, 1.2))
        painter.setBrush(bg)
        painter.drawRoundedRect(rect, 16, 16)

        avatar_rect = QRectF(rect.left() + 12, rect.top() + 12, 42, 42)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor("#1f4e79") if selected else QColor("#173552"))
        painter.drawEllipse(avatar_rect)

        painter.setPen(QColor("#f5f9ff"))
        avatar_font = QFont()
        avatar_font.setBold(True)
        avatar_font.setPointSize(9)
        painter.setFont(avatar_font)
        painter.drawText(avatar_rect, Qt.AlignCenter, _initials(str(row.get("display_name") or "")))

        content_left = avatar_rect.right() + 12
        content_right = rect.right() - 12
        title_width = max(60.0, content_right - content_left - 54.0)

        title_font = QFont()
        title_font.setBold(True)
        title_font.setPointSize(10)
        painter.setFont(title_font)
        painter.setPen(QColor("#f5f9ff"))
        painter.drawText(
            QRectF(content_left, rect.top() + 10, title_width, 18),
            Qt.AlignLeft | Qt.AlignVCenter,
            str(row.get("display_name") or "Conversacion").strip(),
        )

        meta_font = QFont()
        meta_font.setPointSize(8)
        painter.setFont(meta_font)
        painter.setPen(QColor("#8ea3bf"))
        painter.drawText(
            QRectF(content_left, rect.top() + 30, title_width, 16),
            Qt.AlignLeft | Qt.AlignVCenter,
            f"desde @{str(row.get('account_id') or '-').strip() or '-'}",
        )

        preview_font = QFont()
        preview_font.setPointSize(9)
        painter.setFont(preview_font)
        painter.setPen(QColor("#d8e6f8"))
        painter.drawText(
            QRectF(content_left, rect.top() + 48, content_right - content_left, 38),
            Qt.AlignLeft | Qt.TextWordWrap,
            _preview_text(row),
        )

        painter.setFont(meta_font)
        painter.setPen(QColor("#9bb4d4"))
        painter.drawText(
            QRectF(content_right - 52, rect.top() + 12, 48, 14),
            Qt.AlignRight | Qt.AlignVCenter,
            _short_time(row.get("last_message_timestamp")),
        )

        chip_y = rect.bottom() - 24
        unread_count = _safe_int(row.get("unread_count"))
        badge_right = rect.right() - 12
        health_state = str(row.get("account_health") or "healthy").strip().lower()
        if health_state != "healthy":
            label = _health_badge_text(health_state)
            width = max(64.0, min(112.0, 18.0 + (len(label) * 6.2)))
            health_rect = QRectF(max(content_left, badge_right - width), rect.top() + 12, width, 18)
            painter.setPen(QPen(QColor("#a85d2a"), 1.0))
            painter.setBrush(QColor("#3f2618"))
            painter.drawRoundedRect(health_rect, 9, 9)
            painter.setPen(QColor("#ffd5ad"))
            painter.drawText(health_rect, Qt.AlignCenter, label)
            badge_right = health_rect.left() - 8
        if unread_count > 0:
            badge_rect = QRectF(badge_right - 28, chip_y, 28, 18)
            painter.setPen(QPen(QColor("#335b96"), 1.0))
            painter.setBrush(QColor("#14345d"))
            painter.drawRoundedRect(badge_rect, 9, 9)
            painter.setPen(QColor("#d9e8ff"))
            painter.drawText(badge_rect, Qt.AlignCenter, str(unread_count))
            badge_right -= 34

        needs_reply = bool(row.get("needs_reply")) if "needs_reply" in row else (
            str(row.get("last_message_direction") or "").strip().lower() == "inbound"
        )
        if needs_reply:
            pending_rect = QRectF(max(content_left, badge_right - 88), chip_y, 84, 18)
            painter.setPen(QPen(QColor("#2f5b98"), 1.0))
            painter.setBrush(QColor("#102845"))
            painter.drawRoundedRect(pending_rect, 9, 9)
            painter.setPen(QColor("#9ec7ff"))
            painter.drawText(pending_rect, Qt.AlignCenter, "Sin responder")

        painter.restore()

    def sizeHint(self, option, index: QModelIndex) -> QSize:  # type: ignore[override]
        del option, index
        return QSize(280, 108)


class ConversationList(QWidget):
    conversationSelected = Signal(str)
    filterChanged = Signal(str)
    refreshRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._current_thread_key = ""
        self._model = ConversationListModel(self)
        self._delegate = ConversationListDelegate(self)

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        panel = QFrame()
        panel.setObjectName("InboxRailCard")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(16, 16, 16, 16)
        panel_layout.setSpacing(12)

        title = QLabel("Conversaciones")
        title.setObjectName("InboxSectionTitle")
        panel_layout.addWidget(title)

        self._summary = QLabel("Esperando sincronizacion")
        self._summary.setObjectName("InboxSummaryText")
        panel_layout.addWidget(self._summary)

        filters = QHBoxLayout()
        filters.setContentsMargins(0, 0, 0, 0)
        filters.setSpacing(8)
        self._filter_buttons = QButtonGroup(self)
        self._filter_buttons.setExclusive(True)
        for index, (label, value) in enumerate(
            (("Todas", "all"), ("No leidas", "unread"), ("Sin responder", "pending"))
        ):
            button = QPushButton(label)
            button.setCheckable(True)
            button.setObjectName("InboxFilterButton")
            if index == 0:
                button.setChecked(True)
            filters.addWidget(button)
            self._filter_buttons.addButton(button)
            button.clicked.connect(lambda checked=False, mode=value: self.filterChanged.emit(mode))
        panel_layout.addLayout(filters)

        self._view = QListView()
        self._view.setObjectName("InboxConversationView")
        self._view.setModel(self._model)
        self._view.setItemDelegate(self._delegate)
        self._view.setFrameShape(QFrame.NoFrame)
        self._view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._view.setSelectionMode(QListView.NoSelection)
        self._view.setVerticalScrollMode(QListView.ScrollPerPixel)
        self._view.setUniformItemSizes(False)
        self._view.clicked.connect(self._handle_clicked)
        scrollbar = self._view.verticalScrollBar()
        scrollbar.valueChanged.connect(self._maybe_load_more)
        panel_layout.addWidget(self._view, 1)

        root.addWidget(panel)

    def current_filter(self) -> str:
        checked = self._filter_buttons.checkedButton()
        if checked is None:
            return "all"
        text = str(checked.text() or "").strip().lower()
        if "no leidas" in text or "no leidos" in text:
            return "unread"
        if "sin responder" in text:
            return "pending"
        return "all"

    def set_threads(
        self,
        rows: list[dict[str, Any]],
        *,
        current_thread_key: str = "",
        total_count: int | None = None,
    ) -> None:
        visible_count = len(rows)
        overall = visible_count if total_count is None else max(visible_count, int(total_count))
        if visible_count == 0:
            self._summary.setText("No hay conversaciones visibles para este filtro.")
        elif overall == visible_count:
            self._summary.setText(f"{visible_count} conversaciones listas para gestionar.")
        else:
            self._summary.setText(f"Mostrando {visible_count} de {overall} conversaciones.")

        scrollbar = self._view.verticalScrollBar()
        previous_value = scrollbar.value()
        self._model.set_threads(rows, current_thread_key=current_thread_key)
        self._current_thread_key = str(current_thread_key or "").strip()
        if self._current_thread_key:
            self._apply_selection(self._current_thread_key, emit_signal=False)
        else:
            self._model.set_current_thread("")
            self._view.clearSelection()
        scrollbar.setValue(min(previous_value, scrollbar.maximum()))

    def _handle_clicked(self, index: QModelIndex) -> None:
        row = self._model.thread_at(index.row())
        thread_key = str((row or {}).get("thread_key") or "").strip()
        if not thread_key:
            return
        self._apply_selection(thread_key, emit_signal=True)

    def _apply_selection(self, thread_key: str, *, emit_signal: bool) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        row = self._model.ensure_visible(clean_key)
        self._model.set_current_thread(clean_key)
        self._current_thread_key = clean_key
        if row >= 0:
            index = self._model.index(row, 0)
            self._view.setCurrentIndex(index)
            self._view.scrollTo(index, QListView.PositionAtCenter)
        if emit_signal:
            self.conversationSelected.emit(clean_key)

    def _maybe_load_more(self, value: int) -> None:
        scrollbar: QScrollBar = self._view.verticalScrollBar()
        if scrollbar.maximum() <= 0:
            return
        if value < scrollbar.maximum() - 120:
            return
        if self._model.load_more():
            scrollbar.setValue(value)


def _initials(value: str) -> str:
    parts = [part for part in str(value or "").strip().split() if part]
    if not parts:
        return "IG"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return f"{parts[0][0]}{parts[1][0]}".upper()


def _preview_text(thread: dict[str, Any]) -> str:
    text = str(thread.get("last_message_text") or "").strip() or "Sin mensajes"
    direction = str(thread.get("last_message_direction") or "").strip().lower()
    if direction == "outbound" and text != "Sin mensajes":
        text = f"Tu: {text}"
    compact = " ".join(text.split())
    if len(compact) > 110:
        return f"{compact[:107].rstrip()}..."
    return compact


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0


def _short_time(value: Any) -> str:
    try:
        timestamp = float(value)
    except Exception:
        return "-"
    if timestamp <= 0:
        return "-"

    stamp = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    delta_seconds = max(0, int((now - stamp).total_seconds()))

    if now.date() == stamp.date():
        if delta_seconds < 3600:
            minutes = max(1, delta_seconds // 60)
            return f"{minutes}m"
        return stamp.strftime("%H:%M")
    if (now.date() - stamp.date()).days == 1:
        return "Ayer"
    return stamp.strftime("%d/%m")


def _health_badge_text(state: str) -> str:
    return {
        "login_required": "Login",
        "checkpoint": "Checkpoint",
        "suspended": "Suspendida",
        "banned": "Bloqueada",
        "proxy_error": "Proxy",
        "unknown": "Desconocida",
    }.get(str(state or "").strip().lower(), "Error")
