from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QObject, QPoint, QSize, QTimer, Qt, Signal  # UI: add the model primitives required by the list-based message renderer
from PySide6.QtGui import QColor, QFont, QFontMetrics, QKeyEvent, QPainter, QPen  # UI: add the painting primitives required by the delegate-driven bubble renderer
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListView,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QStyledItemDelegate,
    QVBoxLayout,
    QWidget,
)

from gui.automation_dialogs import confirm_automation_action
from src.inbox.message_timestamps import (
    annotate_message_timestamps,
    message_canonical_timestamp,
    message_sort_key,
)


class _ComposerEdit(QPlainTextEdit):
    submitRequested = Signal()

    def keyPressEvent(self, event: QKeyEvent) -> None:  # type: ignore[override]
        if event.key() in {Qt.Key_Return, Qt.Key_Enter} and not (event.modifiers() & Qt.ShiftModifier):
            event.accept()
            self.submitRequested.emit()
            return
        super().keyPressEvent(event)


class ChatMessageModel(QAbstractListModel):
    MessageRole = Qt.UserRole + 1  # UI: expose the full row dict so the chat-level context menu can act on original message payloads
    IdentityRole = Qt.UserRole + 2  # UI: expose stable identities so incremental updates and scroll anchors can target the same logical row
    SignatureRole = Qt.UserRole + 3  # UI: expose signatures so the existing no-op and append diff paths can be preserved
    DirectionRole = Qt.UserRole + 4  # UI: expose direction directly for left/right delegate painting
    TextRole = Qt.UserRole + 5  # UI: expose text directly for delegate paint and size calculations
    MetaRole = Qt.UserRole + 6  # UI: expose the preformatted meta line so painting matches the old widget footer text exactly

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)  # UI: parent the message model into the chat widget tree for predictable lifetime management
        self._rows: list[dict[str, Any]] = []  # UI: store normalized message rows exactly as the chat view wants to display them

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        if parent.isValid():
            return 0
        return len(self._rows)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # type: ignore[override]
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        row = self._rows[index.row()]
        if role == Qt.DisplayRole:
            return str(row.get("text") or "")  # UI: provide a fallback display role even though the custom delegate owns the final paint
        if role == self.MessageRole:
            return dict(row)
        if role == self.IdentityRole:
            return _message_identity(row, index.row())  # UI: keep fallback identities position-stable like the previous widget diff path
        if role == self.SignatureRole:
            return _message_signature(row)
        if role == self.DirectionRole:
            return str(row.get("direction") or "").strip().lower()
        if role == self.TextRole:
            return str(row.get("text") or "")
        if role == self.MetaRole:
            return _message_meta(row)
        return None

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        clean_rows = [dict(row) for row in rows if isinstance(row, dict)]  # UI: defensively copy inbound rows so model updates never leak back upstream
        self.beginResetModel()  # UI: replace the full thread efficiently when the chat chooses a complete rerender
        self._rows = clean_rows
        self.endResetModel()

    def update_row(self, row_index: int, row: dict[str, Any]) -> None:
        if not (0 <= row_index < len(self._rows)):
            return
        self._rows[row_index] = dict(row or {})  # UI: refresh one logical row in place so text/meta updates do not disturb scroll state
        changed_index = self.index(row_index, 0)
        self.dataChanged.emit(
            changed_index,
            changed_index,
            [
                Qt.DisplayRole,
                self.MessageRole,
                self.IdentityRole,
                self.SignatureRole,
                self.DirectionRole,
                self.TextRole,
                self.MetaRole,
            ],
        )  # UI: invalidate every delegate-relevant role for the updated message row

    def append_rows(self, rows: list[dict[str, Any]]) -> None:
        clean_rows = [dict(row) for row in rows if isinstance(row, dict)]  # UI: normalize appended rows before exposing them to the live view
        if not clean_rows:
            return
        start = len(self._rows)
        end = start + len(clean_rows) - 1
        self.beginInsertRows(QModelIndex(), start, end)  # UI: preserve the append-only fast path through proper model inserts
        self._rows.extend(clean_rows)
        self.endInsertRows()

    def signatures(self) -> list[tuple[str, ...]]:
        return [_message_signature(row) for row in self._rows]

    def identities(self) -> list[str]:
        return [_message_identity(row, index) for index, row in enumerate(self._rows)]

class ChatMessageDelegate(QStyledItemDelegate):
    _MAX_BUBBLE_WIDTH = 520  # UI: preserve the existing bubble width cap from the widget renderer
    _BUBBLE_RATIO = 0.75  # UI: limit bubble width to the requested fraction of the available row width
    _SIDE_MARGIN = 18  # UI: preserve the old chat canvas side padding around painted bubbles
    _H_PADDING = 10  # UI: match the requested horizontal bubble padding
    _V_PADDING = 8  # UI: match the requested vertical bubble padding
    _TEXT_META_GAP = 4  # UI: preserve the old visual separation between message text and metadata
    _RADIUS = 12  # UI: paint the delegate bubbles with the requested rounded-corner radius
    _TEXT_COLOR = QColor("#f4f8ff")  # UI: match the old bubble text color from the stylesheet exactly
    _META_COLOR = QColor("#8297b3")  # UI: match the old bubble meta color from the stylesheet exactly
    _OUTBOUND_FILL = QColor("#13395f")  # UI: match the old outbound bubble fill from the stylesheet exactly
    _OUTBOUND_BORDER = QColor("#255990")  # UI: match the old outbound bubble border from the stylesheet exactly
    _INBOUND_FILL = QColor("#0f1d2f")  # UI: match the old inbound bubble fill from the stylesheet exactly
    _INBOUND_BORDER = QColor("#1b3047")  # UI: match the old inbound bubble border from the stylesheet exactly

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)  # UI: parent the delegate to the list view so width-driven relayout stays local to the chat surface

    def _effective_width(self, option, index: QModelIndex) -> int:
        del index
        width = int(option.rect.width() or 0)
        parent = self.parent()
        if width <= 0 and isinstance(parent, QListView):
            width = parent.viewport().width()
        return max(1, width)

    def _bubble_width(self, option, index: QModelIndex) -> int:
        available_width = max(1, self._effective_width(option, index) - (self._SIDE_MARGIN * 2))
        return min(self._MAX_BUBBLE_WIDTH, max(1, int(available_width * self._BUBBLE_RATIO)))

    def _text_font(self, option) -> QFont:
        font = QFont(option.font)
        font.setPointSize(12)  # UI: preserve the old 12px bubble text size from the stylesheet
        return font

    def _meta_font(self, option) -> QFont:
        font = QFont(option.font)
        font.setPointSize(10)  # UI: preserve the old 10px bubble meta size from the stylesheet
        return font

    def _text_height(self, font: QFont, width: int, text: str) -> int:
        metrics = QFontMetrics(font)
        bounds = metrics.boundingRect(0, 0, max(1, width), 100000, Qt.AlignLeft | Qt.AlignTop | Qt.TextWordWrap, text)
        return max(metrics.lineSpacing(), bounds.height())

    def _layout(self, option, index: QModelIndex) -> tuple[QFont, QFont, str, str, bool, int, int]:
        direction = str(index.data(ChatMessageModel.DirectionRole) or "").strip().lower()
        outbound = direction == "outbound"
        text = str(index.data(ChatMessageModel.TextRole) or "").strip() or " "  # UI: preserve the old empty-text placeholder behavior exactly
        meta = str(index.data(ChatMessageModel.MetaRole) or "")
        bubble_width = self._bubble_width(option, index)
        text_font = self._text_font(option)
        meta_font = self._meta_font(option)
        text_width = max(1, bubble_width - (self._H_PADDING * 2))
        text_height = self._text_height(text_font, text_width, text)
        meta_height = max(1, QFontMetrics(meta_font).lineSpacing())
        bubble_height = max(48, (self._V_PADDING * 2) + text_height + self._TEXT_META_GAP + meta_height)
        return text_font, meta_font, text, meta, outbound, bubble_width, bubble_height

    def paint(self, painter: QPainter, option, index: QModelIndex) -> None:  # type: ignore[override]
        text_font, meta_font, text, meta, outbound, bubble_width, bubble_height = self._layout(option, index)
        content_rect = option.rect.adjusted(self._SIDE_MARGIN, 0, -self._SIDE_MARGIN, 0)  # UI: keep the old canvas padding around delegate-painted bubbles
        bubble_x = (content_rect.right() - bubble_width + 1) if outbound else content_rect.left()
        bubble_rect = content_rect.adjusted(0, 0, 0, 0)
        bubble_rect.setX(bubble_x)
        bubble_rect.setWidth(bubble_width)
        bubble_rect.setHeight(bubble_height)
        text_width = max(1, bubble_rect.width() - (self._H_PADDING * 2))
        text_height = self._text_height(text_font, text_width, text)
        meta_height = max(1, QFontMetrics(meta_font).lineSpacing())
        text_rect = bubble_rect.adjusted(self._H_PADDING, self._V_PADDING, -self._H_PADDING, -(self._V_PADDING + meta_height + self._TEXT_META_GAP))
        text_rect.setHeight(text_height)
        meta_rect = bubble_rect.adjusted(self._H_PADDING, bubble_rect.height() - self._V_PADDING - meta_height, -self._H_PADDING, -self._V_PADDING)
        fill = self._OUTBOUND_FILL if outbound else self._INBOUND_FILL
        border = self._OUTBOUND_BORDER if outbound else self._INBOUND_BORDER

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, True)  # UI: keep rounded bubbles smooth now that they are painted instead of QWidget-backed
        painter.setPen(QPen(border, 1))
        painter.setBrush(fill)
        painter.drawRoundedRect(bubble_rect, self._RADIUS, self._RADIUS)
        painter.setPen(self._TEXT_COLOR)
        painter.setFont(text_font)
        painter.drawText(text_rect, Qt.AlignLeft | Qt.AlignTop | Qt.TextWordWrap, text)
        painter.setPen(self._META_COLOR)
        painter.setFont(meta_font)
        painter.drawText(meta_rect, Qt.AlignRight | Qt.AlignVCenter, meta)
        painter.restore()

    def sizeHint(self, option, index: QModelIndex) -> QSize:  # type: ignore[override]
        _text_font, _meta_font, _text, _meta, _outbound, _bubble_width, bubble_height = self._layout(option, index)
        return QSize(self._effective_width(option, index), max(48, bubble_height))  # UI: let row height grow with wrapped content while the list keeps full-width hit areas


class ChatView(QWidget):
    sendRequested = Signal(str)
    messageDeleteRequested = Signal(object)
    actionsRequested = Signal()
    addTagRequested = Signal()
    markFollowUpRequested = Signal()
    manualTakeoverRequested = Signal()
    manualReleaseRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        shell = QFrame()
        shell.setObjectName("InboxStageCard")
        shell_layout = QVBoxLayout(shell)
        shell_layout.setContentsMargins(0, 0, 0, 0)
        shell_layout.setSpacing(0)

        header = QFrame()
        header.setObjectName("InboxChatHeader")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(16, 12, 16, 12)
        header_layout.setSpacing(12)

        self._avatar = QLabel("?")
        self._avatar.setObjectName("InboxChatAvatar")
        self._avatar.setAlignment(Qt.AlignCenter)
        header_layout.addWidget(self._avatar, 0, Qt.AlignTop)

        identity = QVBoxLayout()
        identity.setContentsMargins(0, 0, 0, 0)
        identity.setSpacing(2)

        self._title = QLabel("Selecciona una conversacion")
        self._title.setObjectName("InboxChatTitle")
        self._meta = QLabel("El historial aparecera aqui cuando abras un thread.")
        self._meta.setObjectName("InboxChatMeta")
        self._submeta = QLabel("Abre una conversacion para ver su contexto.")
        self._submeta.setObjectName("InboxMutedText")
        self._submeta.setWordWrap(True)
        identity.addWidget(self._title)
        identity.addWidget(self._meta)
        identity.addWidget(self._submeta)
        header_layout.addLayout(identity, 1)

        badges = QVBoxLayout()
        badges.setContentsMargins(0, 0, 0, 0)
        badges.setSpacing(6)

        self._context_badge = QLabel("Sin contexto")
        self._context_badge.setObjectName("InboxMetaChip")
        badges.addWidget(self._context_badge, 0, Qt.AlignRight)

        self._state_badge = QLabel("Sin seleccion")
        self._state_badge.setObjectName("InboxStateBadge")
        badges.addWidget(self._state_badge, 0, Qt.AlignRight)
        header_layout.addLayout(badges, 0)
        shell_layout.addWidget(header)

        self._msg_model = ChatMessageModel(self)  # UI: move message storage into a model so the chat no longer instantiates one widget per row
        self._msg_delegate = ChatMessageDelegate(self)  # UI: move bubble rendering into a delegate so large threads paint efficiently
        self._msg_view = QListView()  # UI: replace the scroll-area canvas with a list view designed for many variable-height rows
        self._msg_view.setObjectName("InboxChatMessageList")  # UI: give the list surface a stable hook for local styling and debugging
        self._msg_view.viewport().setObjectName("InboxChatViewport")  # UI: preserve the transparent viewport styling hook used by the old scroll area
        self._msg_view.setModel(self._msg_model)  # UI: bind the chat list to the new message model
        self._msg_view.setItemDelegate(self._msg_delegate)  # UI: render each row through the custom bubble delegate
        self._msg_view.setVerticalScrollMode(QListView.ScrollPerPixel)  # UI: keep pixel-smooth scrolling like the old scroll area
        self._msg_view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # UI: preserve the single-column chat layout with no horizontal scrolling
        self._msg_view.setResizeMode(QListView.Adjust)  # UI: recompute delegate layouts when the viewport width changes
        self._msg_view.setSelectionMode(QListView.NoSelection)  # UI: match the old bubble widgets, which were not selectable rows
        self._msg_view.setFocusPolicy(Qt.NoFocus)  # UI: keep keyboard focus on the composer rather than the message surface
        self._msg_view.setSpacing(6)  # UI: preserve a small gap between consecutive bubbles in the list renderer
        self._msg_view.setFrameShape(QFrame.NoFrame)  # UI: remove the default item-view frame so the stage card remains visually clean
        self._msg_view.setContextMenuPolicy(Qt.CustomContextMenu)  # UI: move message actions to the list view context menu now that bubbles are painted
        self._msg_view.customContextMenuRequested.connect(self._show_message_context_menu)  # UI: keep copy/delete actions routed through the existing chat helpers
        self._msg_view.setStyleSheet("QListView#InboxChatMessageList { background: transparent; border: none; }")  # UI: preserve the transparent chat surface without touching the shared inbox stylesheet

        self._message_stage = QWidget()  # UI: host the list view and the empty-state surface in the same slot the scroll area used to occupy
        self._message_stage_layout = QVBoxLayout(self._message_stage)  # UI: let the chat toggle between real messages and placeholders without rebuilding parent layouts
        self._message_stage_layout.setContentsMargins(0, 0, 0, 0)
        self._message_stage_layout.setSpacing(0)

        self._empty_state_host = QWidget()  # UI: preserve the old loading/error/no-thread placeholder card behavior alongside the new list view
        self._empty_state_layout = QVBoxLayout(self._empty_state_host)  # UI: center placeholder cards in a dedicated host instead of inserting them into a message canvas
        self._empty_state_layout.setContentsMargins(18, 16, 18, 16)
        self._empty_state_layout.setSpacing(0)
        self._empty_state_layout.addStretch(1)
        self._empty_state_layout.addStretch(1)
        self._empty_state_card: _EmptyStateCard | None = None  # UI: track the active placeholder card so it can be replaced cleanly

        self._message_stage_layout.addWidget(self._msg_view, 1)  # UI: place the live message list into the stage slot
        self._message_stage_layout.addWidget(self._empty_state_host, 1)  # UI: keep placeholder content in the same slot as the live message list
        shell_layout.addWidget(self._message_stage, 1)  # UI: replace the old scroll-area slot with the list/placeholder stage
        self._render_hint = QLabel("")  # UI: reserve a render-status label above the composer so large thread rebuilds show progress
        self._render_hint.setObjectName("InboxRenderHint")  # UI: style the render-status label independently from message content
        self._render_hint.setAlignment(Qt.AlignCenter)  # UI: center the render-status label so it reads like a transient loading hint
        self._render_hint.hide()  # UI: keep the render-status label hidden until a large thread rebuild begins
        shell_layout.addWidget(self._render_hint)  # UI: place the render-status label above the composer without mixing it into the message area

        composer = QFrame()
        composer.setObjectName("InboxComposerDock")
        composer_layout = QVBoxLayout(composer)
        composer_layout.setContentsMargins(14, 10, 14, 12)
        composer_layout.setSpacing(8)

        self._composer_hint = QLabel("Enter para enviar. Shift+Enter para salto de linea.")
        self._composer_hint.setObjectName("InboxComposerHint")
        composer_layout.addWidget(self._composer_hint)

        editor_row = QHBoxLayout()
        editor_row.setContentsMargins(0, 0, 0, 0)
        editor_row.setSpacing(10)

        self._input = _ComposerEdit()
        self._input.setObjectName("InboxComposer")
        self._input.setPlaceholderText("Selecciona una conversacion para responder")
        self._input.setFixedHeight(64)
        self._input.setReadOnly(True)
        self._input.setEnabled(False)
        self._input.submitRequested.connect(self._emit_send)
        editor_row.addWidget(self._input, 1)

        self._send_button = QPushButton("Enviar")
        self._send_button.setObjectName("InboxPrimaryButton")
        self._send_button.setEnabled(False)
        self._send_button.clicked.connect(self._emit_send)
        editor_row.addWidget(self._send_button, 0, Qt.AlignBottom)
        composer_layout.addLayout(editor_row)

        shell_layout.addWidget(composer)
        root.addWidget(shell, 1)

        self._current_thread_key = ""
        self._rendered_thread_key = ""
        self._rendered_signatures: list[tuple[str, ...]] = []
        self._rendered_identities: list[str] = []
        self._current_health_state = "healthy"
        self._runtime_active = False
        self._manual_send_reason = ""
        self._msg_cache_key: str = ""  # UI: cache the last normalized message payload key so unchanged snapshots can skip re-normalization
        self._msg_cache_rows: list[dict[str, Any]] = []  # UI: store the last normalized rows so unchanged snapshots can reuse them directly
        self._msg_cache_signature: str = ""  # UI: retain a stable signature token alongside the cached normalized message rows
        self._show_placeholder(
            "Selecciona una conversacion",
            "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
        )

    def set_thread(
        self,
        thread: dict[str, Any] | None,
        *,
        permissions: dict[str, Any] | None = None,
        truth: dict[str, Any] | None = None,
    ) -> None:
        if not thread:
            self._current_thread_key = ""
            self._msg_cache_key = ""  # UI: invalidate message cache on thread switch
            self._msg_cache_rows = []  # UI: invalidate message cache on thread switch
            self._msg_cache_signature = ""  # UI: invalidate message cache on thread switch
            self._avatar.setText("?")
            self._title.setText("Selecciona una conversacion")
            self._meta.setText("El historial aparecera aqui cuando abras un thread.")
            self._submeta.setText("Abre una conversacion para ver su contexto.")
            self._context_badge.setText("Sin contexto")
            self._state_badge.setText("Sin seleccion")
            self._composer_hint.setText("Enter para enviar. Shift+Enter para salto de linea.")
            self._input.setPlaceholderText("Selecciona una conversacion para responder")
            self._input.setReadOnly(True)
            self._input.setEnabled(False)
            self._send_button.setEnabled(False)
            self._show_placeholder(
                "Selecciona una conversacion",
                "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
            )
            return

        new_thread_key = str(thread.get("thread_key") or "").strip()
        if new_thread_key != self._current_thread_key:
            self._reset_render_state()
            self._msg_cache_key = ""  # UI: invalidate message cache on thread switch
            self._msg_cache_rows = []  # UI: invalidate message cache on thread switch
            self._msg_cache_signature = ""  # UI: invalidate message cache on thread switch
        self._current_thread_key = new_thread_key
        display_name = str(
            thread.get("display_name") or thread.get("recipient_username") or "Conversacion"
        ).strip()
        account_id = str(thread.get("account_id") or "-").strip() or "-"
        account_alias = str(thread.get("account_alias") or "").strip()
        recipient = str(thread.get("recipient_username") or "-").strip() or "-"
        self._current_health_state = str(thread.get("account_health") or "healthy").strip().lower() or "healthy"
        owner = str(thread.get("owner") or "none").strip().lower() or "none"
        resolved = self._resolve_permissions(thread, permissions)
        truth_payload = dict(truth or {})
        self._runtime_active = bool(resolved.get("runtime_active", False))
        self._manual_send_reason = str(resolved.get("manual_send_reason") or "").strip().lower()
        can_reply = bool(resolved.get("can_manual_send"))
        composer_mode = str(resolved.get("composer_mode") or "disabled").strip().lower() or "disabled"
        read_only = composer_mode == "readonly"

        source_label = f"Alias @{account_alias}" if account_alias else f"Cuenta @{account_id}"
        stage = str(thread.get("stage_id") or thread.get("stage") or "").strip()
        bucket = str(thread.get("bucket") or "").strip()

        self._avatar.setText(_initials(display_name))
        self._title.setText(display_name)
        self._meta.setText(f"@{recipient}  |  {source_label}")
        self._submeta.setText(_thread_submeta(thread, truth=truth_payload))
        self._context_badge.setText(stage or bucket or "Thread activo")
        self._state_badge.setText(str(truth_payload.get("label") or _thread_state(thread)).strip() or "Thread activo")
        self._composer_hint.setText(self._composer_hint_text())
        self._input.setPlaceholderText(self._composer_placeholder(can_reply, read_only))
        self._input.setReadOnly(read_only or not can_reply)
        self._input.setEnabled(can_reply or read_only)
        self._send_button.setEnabled(can_reply)

        del owner

    def set_loading(self, loading: bool) -> None:
        if loading:
            self._state_badge.setText("Cargando conversacion...")
            self._show_placeholder("Abriendo conversacion", "Traiendo historial y estado del thread.")

    def set_error(self, message: str) -> None:
        detail = str(message or "").strip() or "No se pudo abrir la conversacion."
        self._state_badge.setText("Error al cargar")
        self._show_placeholder("No se pudo abrir la conversacion", detail)

    def set_messages(
        self,
        rows: list[dict[str, Any]],
        *,
        seen_text: str = "",
        force_scroll_to_bottom: bool = False,
    ) -> None:
        del seen_text  # UI: keep the existing public signature stable while the renderer remains row-driven
        if not self._current_thread_key:
            self._show_placeholder(
                "Selecciona una conversacion",
                "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
            )
            return

        incoming_key = f"{self._current_thread_key}:{len(rows)}:{rows[-1].get('message_id', '') if rows else ''}"  # UI: derive a cheap thread revision token before running the normalization pipeline
        if incoming_key == self._msg_cache_key:  # UI: reuse normalized messages when the incoming thread revision token is unchanged
            normalized_rows = list(self._msg_cache_rows)  # UI: skip re-normalization when thread content is unchanged
            message_signatures = [_message_signature(row) for row in normalized_rows]  # UI: still recompute render signatures so downstream diff logic keeps running
        else:
            normalized_rows = _normalize_message_rows(rows)  # UI: run the full normalization pipeline when the incoming thread revision token changes
            message_signatures = [_message_signature(row) for row in normalized_rows]  # UI: compute render signatures immediately after normalization for cache refresh
            self._msg_cache_key = incoming_key  # UI: update message cache after normalization
            self._msg_cache_rows = list(normalized_rows)  # UI: update message cache after normalization
            self._msg_cache_signature = repr(message_signatures)  # UI: update message cache after normalization
        message_identities = [_message_identity(row, index) for index, row in enumerate(normalized_rows)]
        scrollbar = self._msg_view.verticalScrollBar()  # UI: measure scroll state from the list view now that message bubbles are delegate-painted rows
        previous_value = scrollbar.value()
        previous_max = scrollbar.maximum()
        near_bottom = previous_max <= 0 or (previous_max - previous_value) <= 56
        scroll_anchor = None if force_scroll_to_bottom or near_bottom else self._capture_scroll_anchor()  # UI: preserve anchor-based restore from the first visible list row

        if not normalized_rows:
            self._show_placeholder(
                "Sin mensajes sincronizados",
                "Cuando el thread tenga historial disponible en storage, se mostrara aqui.",
            )
            return
        self._msg_view.show()  # UI: show the live list surface as soon as the thread has renderable rows
        self._empty_state_host.hide()  # UI: hide placeholder content when real messages are present

        if (
            self._rendered_thread_key == self._current_thread_key
            and message_signatures == self._rendered_signatures
        ):
            if force_scroll_to_bottom:
                QTimer.singleShot(0, self._scroll_to_bottom)
            return
        if (
            self._rendered_thread_key == self._current_thread_key
            and self._msg_model.rowCount() > 0
            and len(message_identities) == len(self._rendered_identities)
            and message_identities == self._rendered_identities
        ):
            for row_index, row in enumerate(normalized_rows):
                self._msg_model.update_row(row_index, row)  # UI: preserve the old in-place update path without rebuilding the whole thread
            self._rendered_signatures = self._msg_model.signatures()  # UI: keep cached signatures aligned with the live model after in-place updates
        elif (
            self._rendered_thread_key == self._current_thread_key
            and self._msg_model.rowCount() > 0
            and len(message_signatures) >= len(self._rendered_signatures)
            and message_signatures[: len(self._rendered_signatures)] == self._rendered_signatures
        ):
            new_rows = normalized_rows[len(self._rendered_signatures) :]
            self._msg_model.append_rows(new_rows)  # UI: preserve the append-only fast path by inserting only new tail rows into the model
            self._rendered_signatures = self._msg_model.signatures()  # UI: refresh signatures from the model after appends
            self._rendered_identities = self._msg_model.identities()  # UI: refresh identities from the model after appends
        else:
            self._render_full(
                normalized_rows,
                scroll_to_bottom=bool(force_scroll_to_bottom or near_bottom),
                restore_value=previous_value,
                restore_anchor=scroll_anchor,
            )
            return
        if force_scroll_to_bottom or near_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(0, lambda value=previous_value, anchor=scroll_anchor: self._restore_scroll(value, anchor))  # UI: restore the user's reading position after model-level updates

    def load_suggestion(self, text: str) -> None:
        content = str(text or "").strip()
        if not content:
            return
        self._input.setPlainText(content)
        self._input.setFocus()
        cursor = self._input.textCursor()
        cursor.movePosition(cursor.End)
        self._input.setTextCursor(cursor)

    def _emit_send(self) -> None:
        if not self._current_thread_key:
            return
        if not self._send_button.isEnabled() or self._input.isReadOnly():
            return
        content = self._input.toPlainText().strip()
        if not content:
            return
        self.sendRequested.emit(content)
        self._input.clear()
        self._state_badge.setText("Mensaje en cola local")

    def _resolve_permissions(
        self,
        thread: dict[str, Any],
        permissions: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if isinstance(permissions, dict):
            return dict(permissions)
        owner = str(thread.get("owner") or "none").strip().lower() or "none"
        can_reply = self._current_health_state == "healthy" and (owner == "manual" or not self._runtime_active)
        return {
            "runtime_active": self._runtime_active,
            "can_manual_send": can_reply,
            "can_send_pack": can_reply,
            "can_takeover_manual": self._runtime_active and owner != "manual" and self._current_health_state == "healthy",
            "can_release_manual": owner == "manual",
            "can_mark_follow_up": owner != "manual",
            "can_add_tag": True,
            "composer_mode": "editable" if can_reply else ("readonly" if self._runtime_active and owner != "manual" else "disabled"),
            "manual_send_reason": "runtime_auto_owner" if self._runtime_active and owner != "manual" else "",
        }

    def _composer_hint_text(self) -> str:
        if not self._manual_send_reason:
            return "Enter para enviar. Shift+Enter para salto de linea."
        if self._manual_send_reason == "runtime_auto_owner":
            return "Thread tomado por automatizacion. Composer en solo lectura hasta takeover manual o frenar runtime."
        if self._manual_send_reason == "disqualified":
            return "Thread descalificado. El backend no permite respuestas manuales ni packs."
        if self._manual_send_reason == "runtime_closed":
            return "Thread cerrado con runtime activo. Frena el runtime o toma manual antes de responder."
        if self._current_health_state != "healthy":
            return "La cuenta tiene un error y el envio esta deshabilitado."
        return "El backend no permite responder manualmente en este estado."

    def _composer_placeholder(self, can_reply: bool, read_only: bool) -> str:
        if can_reply:
            return "Escribe un mensaje..."
        if read_only:
            return "Composer bloqueado por runtime activo"
        if self._current_health_state != "healthy":
            return "Envio deshabilitado por estado de cuenta"
        return "Envio no disponible para este thread"

    def _show_placeholder(self, title: str, subtitle: str) -> None:
        self._reset_render_state()
        self._render_hint.hide()  # UI: clear transient loading feedback when the chat falls back to a placeholder state
        self._render_hint.setText("")
        self._msg_model.set_rows([])  # UI: clear the list model so placeholder states never leave stale messages behind
        if self._empty_state_card is not None:
            self._empty_state_layout.removeWidget(self._empty_state_card)  # UI: detach the previous placeholder card before installing a new one
            self._empty_state_card.deleteLater()  # UI: replace the existing placeholder card instead of stacking multiple cards
        self._empty_state_card = _EmptyStateCard(title, subtitle)  # UI: keep the existing placeholder card styling inside the new message stage
        self._empty_state_layout.insertWidget(1, self._empty_state_card, 0, Qt.AlignCenter)  # UI: center the placeholder card inside the dedicated host
        self._msg_view.hide()  # UI: hide the live list while there is no thread content to render
        self._empty_state_host.show()  # UI: show the placeholder host in the list-view slot

    def _scroll_to_bottom(self) -> None:
        scrollbar = self._msg_view.verticalScrollBar()  # UI: drive auto-stick behavior from the list view scrollbar
        scrollbar.setValue(scrollbar.maximum())

    def _restore_scroll(self, restore_value: int, restore_anchor: dict[str, Any] | None) -> None:
        scrollbar = self._msg_view.verticalScrollBar()  # UI: restore scroll against list rows instead of QWidget bubble geometry
        if isinstance(restore_anchor, dict):
            identity = str(restore_anchor.get("identity") or "").strip()
            offset = int(restore_anchor.get("offset") or 0)
            for row_index in range(self._msg_model.rowCount()):
                index = self._msg_model.index(row_index, 0)
                if self._msg_model.data(index, ChatMessageModel.IdentityRole) != identity:
                    continue
                rect = self._msg_view.visualRect(index)
                scrollbar.setValue(min(max(0, rect.top() - offset), scrollbar.maximum()))
                return
        scrollbar.setValue(min(max(0, int(restore_value or 0)), scrollbar.maximum()))

    def _reset_render_state(self) -> None:
        self._rendered_thread_key = ""
        self._rendered_signatures = []
        self._rendered_identities = []

    def _copy_message(self, message: dict[str, Any]) -> None:
        content = str((message or {}).get("text") or "").strip()
        if not content:
            return
        clipboard = QApplication.clipboard()
        if clipboard is not None:
            clipboard.setText(content)
            self._state_badge.setText("Mensaje copiado")

    def _confirm_delete_message(self, message: dict[str, Any]) -> None:
        if not self._current_thread_key or not isinstance(message, dict):
            return
        confirmed = confirm_automation_action(
            self,
            title="Eliminar mensaje",
            message="Queres eliminar este mensaje del Inbox?",
            confirm_text="Si",
            cancel_text="No",
            danger=True,
        )
        if confirmed:
            self.messageDeleteRequested.emit(dict(message))

    def _capture_scroll_anchor(self) -> dict[str, Any] | None:
        scrollbar = self._msg_view.verticalScrollBar()  # UI: capture the current list scroll offset before rows change underneath it
        value = scrollbar.value()
        viewport = self._msg_view.viewport()
        for y in range(0, max(1, viewport.height()), 20):
            index = self._msg_view.indexAt(QPoint(10, y))
            if not index.isValid():
                continue
            rect = self._msg_view.visualRect(index)
            return {
                "identity": self._msg_model.data(index, ChatMessageModel.IdentityRole),
                "offset": max(0, value - rect.top()),
            }
        return None

    def _render_full(
        self,
        rows: list[dict[str, Any]],
        *,
        scroll_to_bottom: bool,
        restore_value: int,
        restore_anchor: dict[str, Any] | None,
    ) -> None:
        if len(rows) > 30:
            self._render_hint.setText("Cargando mensajes...")  # UI: preserve the large-thread loading hint even though rendering is now model-backed
            self._render_hint.show()
        self._msg_view.show()  # UI: make the live list visible before replacing its rows wholesale
        self._empty_state_host.hide()  # UI: hide placeholder content while a real thread is being rendered
        self._msg_model.set_rows(rows)  # UI: replace the full thread in one model reset instead of rebuilding QWidget bubbles
        self._render_hint.hide()
        self._render_hint.setText("")
        self._rendered_thread_key = self._current_thread_key  # UI: mark the current thread as fully rendered after the model reset
        self._rendered_signatures = self._msg_model.signatures()  # UI: refresh render caches from the model after a full reset
        self._rendered_identities = self._msg_model.identities()
        if scroll_to_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(
                0,
                lambda value=restore_value, anchor=restore_anchor: self._restore_scroll(
                    value,
                    anchor,
                ),
            )  # UI: restore the user's scroll target after the list view has laid out the new rows

    def _show_message_context_menu(self, pos: QPoint) -> None:
        index = self._msg_view.indexAt(pos)  # UI: resolve context-menu actions from the list row under the pointer
        if not index.isValid():
            return
        row = self._msg_model.data(index, ChatMessageModel.MessageRole)
        if not isinstance(row, dict):
            return
        menu = QMenu(self)
        copy_action = menu.addAction("Copiar")
        delete_action = menu.addAction("Eliminar")
        action = menu.exec(self._msg_view.viewport().mapToGlobal(pos))
        if action == copy_action:
            self._copy_message(row)  # UI: reuse the existing chat-level copy helper for the delegate-rendered list
            return
        if action == delete_action:
            self._confirm_delete_message(row)  # UI: reuse the existing delete-confirmation flow for the delegate-rendered list
            return


class _EmptyStateCard(QFrame):
    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__()
        self.setObjectName("InboxEmptyStateCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(6)

        title_label = QLabel(title)
        title_label.setObjectName("InboxEmptyStateTitle")
        title_label.setAlignment(Qt.AlignCenter)

        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("InboxEmptyStateText")
        subtitle_label.setAlignment(Qt.AlignCenter)
        subtitle_label.setWordWrap(True)

        layout.addWidget(title_label)
        layout.addWidget(subtitle_label)


def _thread_state(thread: dict[str, Any]) -> str:
    health = str(thread.get("account_health") or "healthy").strip().lower()
    if health != "healthy":
        return {
            "login_required": "Cuenta requiere login",
            "checkpoint": "Cuenta en checkpoint",
            "suspended": "Cuenta suspendida",
            "banned": "Cuenta bloqueada",
            "proxy_error": "Error de proxy",
            "unknown": "Estado de cuenta desconocido",
        }.get(health, "Estado de cuenta desconocido")
    thread_status = str(thread.get("thread_status") or "").strip().lower()
    sender_status = str(thread.get("sender_status") or "").strip().lower()
    pack_status = str(thread.get("pack_status") or "").strip().lower()
    thread_error = str(thread.get("thread_error") or thread.get("sender_error") or thread.get("pack_error") or "").strip()
    if thread_status == "opening":
        return "Abriendo thread"
    if sender_status == "preparing":
        return "Preparando thread"
    if pack_status == "queued":
        return "Pack en cola"
    if pack_status == "running":
        return "Enviando pack"
    if sender_status == "queued":
        return "Mensaje en cola"
    if sender_status == "sending":
        return "Enviando mensaje"
    if thread_error:
        return "Envio fallido"
    seen_text = str(thread.get("last_seen_text") or "").strip()
    direction = str(thread.get("last_message_direction") or "").strip().lower()
    if direction == "outbound":
        return "Envio confirmado"
    if direction == "inbound":
        return "Pendiente de respuesta"
    if seen_text:
        return seen_text
    return "Conversacion activa"


def _thread_submeta(thread: dict[str, Any], *, truth: dict[str, Any] | None = None) -> str:
    parts: list[str] = []
    truth_payload = dict(truth or {})
    detail = str(truth_payload.get("detail") or "").strip()
    alias_note = str(truth_payload.get("alias_note") or "").strip()
    stage = str(thread.get("stage_id") or thread.get("stage") or "").strip()
    bucket = str(thread.get("bucket") or "").strip()
    alias_id = str(thread.get("account_alias") or "").strip()
    seen_text = str(thread.get("last_seen_text") or "").strip()
    if detail:
        parts.append(detail)
    if alias_note:
        parts.append(alias_note)
    if stage:
        parts.append(f"Etapa {stage}")
    if bucket:
        parts.append(f"Bucket {bucket}")
    if alias_id:
        parts.append(f"Alias @{alias_id}")
    if seen_text:
        parts.append(seen_text)
    if not parts:
        parts.append("Conversacion lista para revisar y responder.")
    return "  |  ".join(parts)


def _initials(value: str) -> str:
    parts = [part for part in str(value or "").strip().split() if part]
    if not parts:
        return "IG"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return f"{parts[0][0]}{parts[1][0]}".upper()


def _message_meta(message: dict[str, Any]) -> str:
    parts: list[str] = []
    stamp = message_canonical_timestamp(message)
    if stamp:
        parts.append(_format_message_timestamp(stamp))
    status = str(message.get("delivery_status") or "").strip().lower()
    if status == "pending":
        parts.append("Pendiente")
    elif status == "sending":
        parts.append("Enviando...")
    elif status == "error":
        parts.append("Error")
    return "  ".join(parts)


def _message_signature(message: dict[str, Any]) -> tuple[str, ...]:
    return (
        _message_identity(message),
        str(message_canonical_timestamp(message) or ""),
        str(message.get("delivery_status") or "").strip(),
        str(message.get("direction") or "").strip(),
        str(message.get("text") or "").strip(),
    )


def _message_identity(message: dict[str, Any], position: int | None = None) -> str:
    for key in ("block_id", "external_message_id", "message_id"):
        value = str(message.get(key) or "").strip()
        if value:
            return value
    anchor = message_canonical_timestamp(message) or 0.0
    suffix = position if position is not None else 0
    return "|".join(
        (
            str(message.get("direction") or "").strip().lower(),
            str(anchor),
            str(message.get("text") or "").strip(),
            str(suffix),
        )
    )


def _coerce_message_timestamp(message: dict[str, Any]) -> float | None:
    return message_canonical_timestamp(message)


def _message_anchor_timestamp(message: dict[str, Any]) -> float | None:
    return message_canonical_timestamp(message)


def _format_message_timestamp(stamp: float) -> str:
    current = datetime.now()
    resolved = datetime.fromtimestamp(stamp)
    if resolved.date() == current.date():
        return resolved.strftime("%H:%M")
    return resolved.strftime("%d/%m %H:%M")


def _is_synthetic_message_id(value: Any) -> bool:
    message_id = str(value or "").strip().lower()
    if not message_id:
        return False
    return message_id.startswith(("local-", "dom-confirmed-", "confirmed-", "thread-read-confirmed-"))


def _message_sort_key(message: dict[str, Any], *, position: int = 0) -> tuple[float, float, str, int]:
    return message_sort_key(message, position=position)


def _messages_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_id = str(left.get("message_id") or "").strip()
    right_id = str(right.get("message_id") or "").strip()
    left_external = str(left.get("external_message_id") or "").strip()
    right_external = str(right.get("external_message_id") or "").strip()
    if left_id and (left_id == right_id or left_id == right_external):
        return True
    if left_external and (left_external == right_external or left_external == right_id):
        return True
    left_direction = str(left.get("direction") or "").strip().lower()
    right_direction = str(right.get("direction") or "").strip().lower()
    if left_direction != "outbound" or right_direction != "outbound":
        return False
    left_text = str(left.get("text") or "").strip()
    right_text = str(right.get("text") or "").strip()
    if not left_text or left_text != right_text:
        return False
    left_anchor = _message_anchor_timestamp(left)
    right_anchor = _message_anchor_timestamp(right)
    if left_anchor is None or right_anchor is None or abs(left_anchor - right_anchor) > 180.0:
        return False
    return (
        bool(left.get("local_echo"))
        or bool(right.get("local_echo"))
        or _is_synthetic_message_id(left_id or left_external)
        or _is_synthetic_message_id(right_id or right_external)
    )


def _merge_message_rows(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_has_real_id = not _is_synthetic_message_id(left.get("message_id")) and bool(str(left.get("message_id") or "").strip())
    right_has_real_id = not _is_synthetic_message_id(right.get("message_id")) and bool(str(right.get("message_id") or "").strip())
    preferred = right if (right_has_real_id and not left_has_real_id) else left
    fallback = left if preferred is right else right
    merged = dict(preferred)
    for key in ("block_id", "external_message_id", "text", "timestamp", "confirmed_at", "created_at", "delivery_status", "local_echo"):
        if merged.get(key) in (None, "", [], ()) and fallback.get(key) not in (None, "", [], ()):
            merged[key] = fallback.get(key)
    if str(merged.get("delivery_status") or "").strip().lower() == "sent":
        merged["local_echo"] = False
    return merged


def _normalize_message_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = [annotate_message_timestamps(row) for row in rows if isinstance(row, dict)]
    ordered.sort(key=lambda row: _message_sort_key(row))
    normalized: list[dict[str, Any]] = []
    known_indexes: dict[str, int] = {}
    outbound_indexes: dict[str, list[int]] = {}

    for candidate in ordered:
        matched_index: int | None = None
        candidate_ids = (
            str(candidate.get("message_id") or "").strip(),
            str(candidate.get("external_message_id") or "").strip(),
        )
        for identity in candidate_ids:
            if identity and identity in known_indexes:
                matched_index = known_indexes[identity]
                break

        if matched_index is None:
            direction = str(candidate.get("direction") or "").strip().lower()
            text = str(candidate.get("text") or "").strip()
            if direction == "outbound" and text:
                for index in reversed(outbound_indexes.get(text, [])):
                    if _messages_match(normalized[index], candidate):
                        matched_index = index
                        break

        if matched_index is None:
            matched_index = len(normalized)
            normalized.append(candidate)
        else:
            normalized[matched_index] = _merge_message_rows(normalized[matched_index], candidate)

        merged_candidate = normalized[matched_index]
        for identity in (
            str(merged_candidate.get("message_id") or "").strip(),
            str(merged_candidate.get("external_message_id") or "").strip(),
        ):
            if identity:
                known_indexes[identity] = matched_index

        text = str(merged_candidate.get("text") or "").strip()
        direction = str(merged_candidate.get("direction") or "").strip().lower()
        if direction == "outbound" and text:
            bucket = outbound_indexes.setdefault(text, [])
            if matched_index not in bucket:
                bucket.append(matched_index)
                if len(bucket) > 6:
                    del bucket[:-6]

    normalized = [annotate_message_timestamps(row) for row in normalized]
    normalized.sort(key=lambda row: _message_sort_key(row))
    return normalized
