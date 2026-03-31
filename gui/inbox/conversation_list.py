from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QRectF, QSize, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPainter, QPen
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
        rect = option.rect.adjusted(6, 3, -6, -3)

        bg = QColor("#11233a") if selected else QColor("#0d1728")
        border = QColor("#52a8ff") if selected else QColor("#182a41")
        painter.setPen(QPen(border, 1.0))
        painter.setBrush(bg)
        painter.drawRoundedRect(rect, 14, 14)

        if selected:
            accent_rect = QRectF(rect.left() + 1, rect.top() + 12, 3, rect.height() - 24)
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor("#3cc7ff"))
            painter.drawRoundedRect(accent_rect, 2, 2)

        avatar_rect = QRectF(rect.left() + 12, rect.top() + 10, 36, 36)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor("#1b4f79") if selected else QColor("#15324d"))
        painter.drawEllipse(avatar_rect)

        avatar_font = QFont()
        avatar_font.setBold(True)
        avatar_font.setPointSize(8)
        painter.setFont(avatar_font)
        painter.setPen(QColor("#f7fbff"))
        painter.drawText(avatar_rect, Qt.AlignCenter, _initials(str(row.get("display_name") or "")))

        content_left = avatar_rect.right() + 10
        content_right = rect.right() - 12
        timestamp_rect = QRectF(content_right - 52, rect.top() + 10, 52, 16)
        title_width = max(80.0, timestamp_rect.left() - content_left - 8.0)

        title_font = QFont()
        title_font.setBold(True)
        title_font.setPointSize(9)
        painter.setFont(title_font)
        painter.setPen(QColor("#f4f8ff"))
        title = _title_text(row)
        painter.drawText(
            QRectF(content_left, rect.top() + 8, title_width, 16),
            Qt.AlignLeft | Qt.AlignVCenter,
            _elided_text(title, painter.fontMetrics(), int(title_width)),
        )

        meta_font = QFont()
        meta_font.setPointSize(8)
        painter.setFont(meta_font)
        painter.setPen(QColor("#7d93ae"))
        subtitle = _secondary_text(row)
        painter.drawText(
            QRectF(content_left, rect.top() + 27, content_right - content_left, 14),
            Qt.AlignLeft | Qt.AlignVCenter,
            _elided_text(subtitle, painter.fontMetrics(), int(content_right - content_left)),
        )

        preview_font = QFont()
        preview_font.setPointSize(8)
        painter.setFont(preview_font)
        painter.setPen(QColor("#cfe0f4"))
        preview = _preview_text(row)
        painter.drawText(
            QRectF(content_left, rect.top() + 45, content_right - content_left, 14),
            Qt.AlignLeft | Qt.AlignVCenter,
            _elided_text(preview, painter.fontMetrics(), int(content_right - content_left)),
        )

        painter.setFont(meta_font)
        painter.setPen(QColor("#8ea4be"))
        painter.drawText(timestamp_rect, Qt.AlignRight | Qt.AlignVCenter, _short_time(row.get("last_message_timestamp")))

        badge_right = rect.right() - 12
        unread_count = _safe_int(row.get("unread_count"))
        health_state = str(row.get("account_health") or "healthy").strip().lower()

        if unread_count > 0:
            badge_right = _draw_badge(
                painter,
                badge_right=badge_right,
                top=rect.top() + 44,
                text=str(unread_count),
                fill="#143760",
                stroke="#2d5f93",
                color="#deeeff",
            )

        if _needs_reply(row):
            badge_right = _draw_badge(
                painter,
                badge_right=badge_right,
                top=rect.top() + 44,
                text="Sin responder",
                fill="#0f2844",
                stroke="#2d5a91",
                color="#9ccaff",
            )

        if health_state != "healthy":
            _draw_badge(
                painter,
                badge_right=rect.right() - 12,
                top=rect.top() + 10,
                text=_health_badge_text(health_state),
                fill="#3e261b",
                stroke="#9c6337",
                color="#ffd4b0",
            )

        painter.restore()

    def sizeHint(self, option, index: QModelIndex) -> QSize:  # type: ignore[override]
        del option, index
        return QSize(280, 76)


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
        panel_layout.setContentsMargins(14, 14, 14, 14)
        panel_layout.setSpacing(10)

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(8)

        title = QLabel("Conversaciones")
        title.setObjectName("InboxSectionTitle")
        title_row.addWidget(title, 1)

        self._summary = QLabel("Esperando proyeccion")
        self._summary.setObjectName("InboxSummaryText")
        title_row.addWidget(self._summary, 0, Qt.AlignRight)
        panel_layout.addLayout(title_row)

        filters = QHBoxLayout()
        filters.setContentsMargins(0, 0, 0, 0)
        filters.setSpacing(6)
        self._filter_buttons = QButtonGroup(self)
        self._filter_buttons.setExclusive(True)
        for index, (label, value) in enumerate(
            (("Todas", "all"), ("Calificadas", "qualified"), ("Descalificadas", "disqualified"))
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
        self._view.setSpacing(2)
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
        if "calificadas" in text:
            return "qualified"
        if "descalificadas" in text:
            return "disqualified"
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
            self._summary.setText("Sin resultados")
        elif overall == visible_count:
            self._summary.setText(f"{visible_count} resultados")
        else:
            self._summary.setText(f"{visible_count}/{overall} resultados")

        scrollbar = self._view.verticalScrollBar()
        previous_value = scrollbar.value()
        self._model.set_threads(rows, current_thread_key=current_thread_key)
        self._current_thread_key = str(current_thread_key or "").strip()
        if self._current_thread_key:
            self._apply_selection(self._current_thread_key, emit_signal=False, scroll_into_view=False)
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

    def _apply_selection(self, thread_key: str, *, emit_signal: bool, scroll_into_view: bool = True) -> None:
        clean_key = str(thread_key or "").strip()
        if not clean_key:
            return
        row = self._model.ensure_visible(clean_key)
        self._model.set_current_thread(clean_key)
        self._current_thread_key = clean_key
        if row >= 0:
            index = self._model.index(row, 0)
            self._view.setCurrentIndex(index)
            if scroll_into_view:
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


def _draw_badge(
    painter: QPainter,
    *,
    badge_right: float,
    top: float,
    text: str,
    fill: str,
    stroke: str,
    color: str,
) -> float:
    width = max(24.0, min(108.0, 16.0 + (len(text) * 5.8)))
    rect = QRectF(badge_right - width, top, width, 16)
    painter.setPen(QPen(QColor(stroke), 1.0))
    painter.setBrush(QColor(fill))
    painter.drawRoundedRect(rect, 8, 8)
    painter.setPen(QColor(color))
    painter.drawText(rect, Qt.AlignCenter, text)
    return rect.left() - 6


def _initials(value: str) -> str:
    parts = [part for part in str(value or "").strip().split() if part]
    if not parts:
        return "IG"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return f"{parts[0][0]}{parts[1][0]}".upper()


def _title_text(thread: dict[str, Any]) -> str:
    return str(thread.get("display_name") or thread.get("recipient_username") or "Conversacion").strip() or "Conversacion"


def _secondary_text(thread: dict[str, Any]) -> str:
    recipient = str(thread.get("recipient_username") or "").strip()
    if recipient:
        return f"@{recipient}"
    account_id = str(thread.get("account_id") or "").strip()
    return f"Cuenta @{account_id}" if account_id else "Sin usuario"


def _preview_text(thread: dict[str, Any]) -> str:
    last_text = str(thread.get("last_message_text") or "").strip()
    direction = str(thread.get("last_message_direction") or "").strip().lower()
    if not last_text:
        return "Sin mensajes recientes"
    prefix = "Tu: " if direction == "outbound" else ""
    compact = f"{prefix}{last_text}"
    if len(compact) > 140:
        return f"{compact[:137].rstrip()}..."
    return compact


def _elided_text(value: str, metrics: QFontMetrics, width: int) -> str:
    return metrics.elidedText(str(value or "").strip(), Qt.ElideRight, max(12, int(width)))


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0


def _needs_reply(thread: dict[str, Any]) -> bool:
    return bool(thread.get("needs_reply")) if "needs_reply" in thread else (
        str(thread.get("last_message_direction") or "").strip().lower() == "inbound"
    )


def _short_time(value: Any) -> str:
    try:
        timestamp = float(value)
    except Exception:
        return "-"
    if timestamp <= 0:
        return "-"

    stamp = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    if now.date() == stamp.date():
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
