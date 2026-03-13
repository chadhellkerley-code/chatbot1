from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtGui import QKeyEvent
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)


class _ComposerEdit(QPlainTextEdit):
    submitRequested = Signal()

    def keyPressEvent(self, event: QKeyEvent) -> None:  # type: ignore[override]
        if event.key() in {Qt.Key_Return, Qt.Key_Enter} and not (event.modifiers() & Qt.ShiftModifier):
            event.accept()
            self.submitRequested.emit()
            return
        super().keyPressEvent(event)


class ChatView(QWidget):
    sendRequested = Signal(str)
    actionsRequested = Signal()
    addTagRequested = Signal()
    markFollowUpRequested = Signal()

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
        header_layout.setContentsMargins(18, 14, 18, 14)
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
        self._state_badge = QLabel("Sin seleccion")
        self._state_badge.setObjectName("InboxStateBadge")
        identity.addWidget(self._title)
        identity.addWidget(self._meta)
        identity.addWidget(self._state_badge, 0, Qt.AlignLeft)
        header_layout.addLayout(identity, 1)

        self._actions_button = QPushButton("Más acciones")
        self._actions_button.setObjectName("InboxGhostButton")
        self._actions_button.setEnabled(False)
        header_layout.addWidget(self._actions_button, 0, Qt.AlignRight | Qt.AlignVCenter)
        self._actions_menu = QMenu(self)
        self._pack_action = self._actions_menu.addAction("Enviar pack")
        self._pack_action.triggered.connect(self.actionsRequested.emit)
        self._tag_action = self._actions_menu.addAction("Agregar etiqueta")
        self._tag_action.triggered.connect(self.addTagRequested.emit)
        self._follow_up_action = self._actions_menu.addAction("Marcar seguimiento")
        self._follow_up_action.triggered.connect(self.markFollowUpRequested.emit)
        self._actions_button.setMenu(self._actions_menu)
        shell_layout.addWidget(header)

        self._scroll = QScrollArea()
        self._scroll.setObjectName("InboxChatScroll")
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._scroll.viewport().setObjectName("InboxChatViewport")

        self._body = QWidget()
        self._body.setObjectName("InboxChatCanvas")
        self._body_layout = QVBoxLayout(self._body)
        self._body_layout.setContentsMargins(18, 18, 18, 18)
        self._body_layout.setSpacing(10)
        self._body_layout.addStretch(1)
        self._scroll.setWidget(self._body)
        shell_layout.addWidget(self._scroll, 1)

        composer = QFrame()
        composer.setObjectName("InboxComposerDock")
        composer_layout = QVBoxLayout(composer)
        composer_layout.setContentsMargins(14, 12, 14, 14)
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
        self._input.setFixedHeight(74)
        self._input.setEnabled(False)
        self._input.submitRequested.connect(self._emit_send)
        editor_row.addWidget(self._input, 1)

        self._send_button = QPushButton("Enviar")
        self._send_button.setObjectName("InboxPrimaryButton")
        self._send_button.clicked.connect(self._emit_send)
        editor_row.addWidget(self._send_button, 0, Qt.AlignBottom)
        composer_layout.addLayout(editor_row)

        shell_layout.addWidget(composer)
        root.addWidget(shell, 1)

        self._current_thread_key = ""
        self._message_widgets: list[QWidget] = []
        self._rendered_thread_key = ""
        self._rendered_signatures: list[tuple[str, ...]] = []
        self._pending_rows: list[dict[str, Any]] = []
        self._pending_signatures: list[tuple[str, ...]] = []
        self._render_job_id = 0
        self._rendering = False
        self._render_batch_size = 80
        self._pending_scroll_to_bottom = False
        self._pending_restore_value = 0
        self._current_health_state = "healthy"
        self._show_placeholder(
            "Selecciona una conversacion",
            "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
        )

    def set_thread(self, thread: dict[str, Any] | None) -> None:
        if not thread:
            self._cancel_pending_render()
            self._current_thread_key = ""
            self._avatar.setText("?")
            self._title.setText("Selecciona una conversacion")
            self._meta.setText("El historial aparecera aqui cuando abras un thread.")
            self._state_badge.setText("Sin seleccion")
            self._composer_hint.setText("Enter para enviar. Shift+Enter para salto de linea.")
            self._input.setPlaceholderText("Selecciona una conversacion para responder")
            self._input.setEnabled(False)
            self._send_button.setEnabled(False)
            self._actions_button.setEnabled(False)
            self._pack_action.setEnabled(False)
            self._tag_action.setEnabled(False)
            self._follow_up_action.setEnabled(False)
            self._show_placeholder(
                "Selecciona una conversacion",
                "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
            )
            return

        new_thread_key = str(thread.get("thread_key") or "").strip()
        if new_thread_key != self._current_thread_key:
            self._cancel_pending_render()
            self._reset_render_state()
        self._current_thread_key = new_thread_key
        display_name = str(
            thread.get("display_name") or thread.get("recipient_username") or "Conversacion"
        ).strip()
        account_id = str(thread.get("account_id") or "-").strip() or "-"
        recipient = str(thread.get("recipient_username") or "-").strip() or "-"
        self._current_health_state = str(thread.get("account_health") or "healthy").strip().lower() or "healthy"
        can_reply = self._current_health_state == "healthy"

        self._avatar.setText(_initials(display_name))
        self._title.setText(display_name)
        self._meta.setText(f"Cliente @{recipient}  |  desde @{account_id}")
        self._state_badge.setText(_thread_state(thread))
        self._composer_hint.setText(
            "Enter para enviar. Shift+Enter para salto de linea."
            if can_reply
            else "La cuenta tiene un error y el envio esta deshabilitado."
        )
        self._input.setPlaceholderText("Escribe un mensaje..." if can_reply else "Envio deshabilitado por estado de cuenta")
        self._input.setEnabled(can_reply)
        self._send_button.setEnabled(can_reply)
        self._actions_button.setEnabled(True)
        self._pack_action.setEnabled(can_reply)
        self._tag_action.setEnabled(True)
        self._follow_up_action.setEnabled(True)

    def set_loading(self, loading: bool) -> None:
        if loading:
            self._cancel_pending_render()
            self._state_badge.setText("Cargando conversacion...")
            self._show_placeholder("Abriendo conversacion", "Traiendo historial y estado del thread.")

    def set_error(self, message: str) -> None:
        self._cancel_pending_render()
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
        if not self._current_thread_key:
            self._show_placeholder(
                "Selecciona una conversacion",
                "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
            )
            return

        if seen_text:
            self._state_badge.setText(seen_text)
        normalized_rows = [row for row in rows if isinstance(row, dict)]
        message_signatures = [_message_signature(row) for row in normalized_rows]
        scrollbar = self._scroll.verticalScrollBar()
        previous_value = scrollbar.value()
        previous_max = scrollbar.maximum()
        near_bottom = previous_max <= 0 or (previous_max - previous_value) <= 56

        if not normalized_rows:
            self._cancel_pending_render()
            self._show_placeholder(
                "Sin mensajes sincronizados",
                "Cuando el thread tenga historial disponible en storage, se mostrara aqui.",
            )
            return
        if (
            not self._rendering
            and self._rendered_thread_key == self._current_thread_key
            and message_signatures == self._rendered_signatures
        ):
            if force_scroll_to_bottom:
                QTimer.singleShot(0, self._scroll_to_bottom)
            return
        if (
            not self._rendering
            and self._rendered_thread_key == self._current_thread_key
            and self._message_widgets
            and len(message_signatures) >= len(self._rendered_signatures)
            and message_signatures[: len(self._rendered_signatures)] == self._rendered_signatures
        ):
            new_rows = normalized_rows[len(self._rendered_signatures) :]
            for row in new_rows:
                self._append_message_row(row)
            self._rendered_signatures = message_signatures
        else:
            self._schedule_render(
                normalized_rows,
                message_signatures,
                scroll_to_bottom=bool(force_scroll_to_bottom or near_bottom),
                restore_value=previous_value,
            )
            return
        if force_scroll_to_bottom or near_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(0, lambda value=previous_value: self._restore_scroll(value))

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
        content = self._input.toPlainText().strip()
        if not content:
            return
        self.sendRequested.emit(content)
        self._input.clear()
        self._state_badge.setText("Mensaje en cola...")

    def _clear_messages(self) -> None:
        while self._body_layout.count() > 1:
            item = self._body_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._message_widgets.clear()

    def _show_placeholder(self, title: str, subtitle: str) -> None:
        self._reset_render_state()
        self._clear_messages()
        self._body_layout.insertWidget(0, _EmptyStateCard(title, subtitle), 0, Qt.AlignCenter)

    def _scroll_to_bottom(self) -> None:
        scrollbar = self._scroll.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _restore_scroll(self, value: int) -> None:
        scrollbar = self._scroll.verticalScrollBar()
        scrollbar.setValue(min(max(0, int(value or 0)), scrollbar.maximum()))

    def _reset_render_state(self) -> None:
        self._rendered_thread_key = ""
        self._rendered_signatures = []

    def _cancel_pending_render(self) -> None:
        self._render_job_id += 1
        self._rendering = False
        self._pending_rows = []
        self._pending_signatures = []

    def _append_message_row(self, row: dict[str, Any]) -> None:
        widget = _MessageBubble(row)
        self._body_layout.insertWidget(self._body_layout.count() - 1, widget)
        self._message_widgets.append(widget)

    def _schedule_render(
        self,
        rows: list[dict[str, Any]],
        signatures: list[tuple[str, ...]],
        *,
        scroll_to_bottom: bool,
        restore_value: int,
    ) -> None:
        self._cancel_pending_render()
        self._clear_messages()
        self._pending_rows = list(rows)
        self._pending_signatures = list(signatures)
        self._pending_scroll_to_bottom = bool(scroll_to_bottom)
        self._pending_restore_value = int(restore_value or 0)
        self._rendering = True
        job_id = self._render_job_id
        QTimer.singleShot(0, lambda current_job=job_id: self._render_next_batch(current_job, 0))

    def _render_next_batch(self, job_id: int, start_index: int) -> None:
        if not self._rendering or job_id != self._render_job_id:
            return
        batch_size = self._render_batch_size
        if len(self._pending_rows) > batch_size:
            batch_size = min(160, max(batch_size, len(self._pending_rows) // 2))
        end_index = min(start_index + batch_size, len(self._pending_rows))
        self._body.setUpdatesEnabled(False)
        try:
            for row in self._pending_rows[start_index:end_index]:
                self._append_message_row(row)
        finally:
            self._body.setUpdatesEnabled(True)
        if end_index < len(self._pending_rows):
            QTimer.singleShot(
                0,
                lambda current_job=job_id, next_index=end_index: self._render_next_batch(
                    current_job,
                    next_index,
                ),
            )
            return
        self._rendering = False
        self._rendered_thread_key = self._current_thread_key
        self._rendered_signatures = list(self._pending_signatures)
        self._pending_rows = []
        self._pending_signatures = []
        if self._pending_scroll_to_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(0, lambda value=self._pending_restore_value: self._restore_scroll(value))


class _EmptyStateCard(QFrame):
    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__()
        self.setObjectName("InboxEmptyStateCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 24, 28, 24)
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


class _MessageBubble(QWidget):
    def __init__(self, message: dict[str, Any]) -> None:
        super().__init__()
        direction = str(message.get("direction") or "unknown").strip().lower()
        outbound = direction == "outbound"

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(4)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)
        if outbound:
            row.addStretch(1)

        bubble = QFrame()
        bubble.setObjectName("InboxBubbleOut" if outbound else "InboxBubbleIn")
        bubble.setMaximumWidth(560)
        bubble_layout = QVBoxLayout(bubble)
        bubble_layout.setContentsMargins(12, 9, 12, 8)
        bubble_layout.setSpacing(4)

        text = QLabel(str(message.get("text") or "").strip() or " ")
        text.setObjectName("InboxBubbleText")
        text.setWordWrap(True)
        bubble_layout.addWidget(text)

        meta = QLabel(_message_meta(message))
        meta.setObjectName("InboxBubbleMeta")
        bubble_layout.addWidget(meta, 0, Qt.AlignRight)

        row.addWidget(bubble, 0)
        if not outbound:
            row.addStretch(1)
        root.addLayout(row)


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
    thread_error = str(thread.get("thread_error") or "").strip()
    if thread_error:
        return "Error al cargar"
    seen_text = str(thread.get("last_seen_text") or "").strip()
    if seen_text:
        return seen_text
    direction = str(thread.get("last_message_direction") or "").strip().lower()
    if direction == "inbound":
        return "Pendiente de respuesta"
    if direction == "outbound":
        return "Ultimo mensaje enviado"
    return "Conversacion activa"


def _initials(value: str) -> str:
    parts = [part for part in str(value or "").strip().split() if part]
    if not parts:
        return "IG"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return f"{parts[0][0]}{parts[1][0]}".upper()


def _message_meta(message: dict[str, Any]) -> str:
    parts: list[str] = []
    try:
        stamp = float(message.get("timestamp")) if message.get("timestamp") is not None else None
    except Exception:
        stamp = None
    if stamp:
        parts.append(datetime.fromtimestamp(stamp).strftime("%H:%M"))
    status = str(message.get("delivery_status") or "").strip().lower()
    if status == "pending":
        parts.append("Pendiente")
    elif status == "sending":
        parts.append("Enviando...")
    elif status == "error":
        parts.append("Error")
    return "  ".join(parts) or "-"


def _message_signature(message: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(message.get("message_id") or "").strip(),
        str(message.get("timestamp") or ""),
        str(message.get("delivery_status") or "").strip(),
        str(message.get("direction") or "").strip(),
        str(message.get("text") or "").strip(),
    )
