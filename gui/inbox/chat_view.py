from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtGui import QAction, QKeyEvent
from PySide6.QtWidgets import (
    QApplication,
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

        self._pack_action = QAction("Enviar pack", self)
        self._pack_action.triggered.connect(self.actionsRequested.emit)
        self._pack_action.setEnabled(False)

        self._tag_action = QAction("Agregar etiqueta", self)
        self._tag_action.triggered.connect(self.addTagRequested.emit)
        self._tag_action.setEnabled(False)

        self._follow_up_action = QAction("Marcar seguimiento", self)
        self._follow_up_action.triggered.connect(self.markFollowUpRequested.emit)
        self._follow_up_action.setEnabled(False)

        self._takeover_action = QAction("Tomar manual", self)
        self._takeover_action.triggered.connect(self.manualTakeoverRequested.emit)
        self._takeover_action.setEnabled(False)

        self._release_action = QAction("Devolver a auto", self)
        self._release_action.triggered.connect(self.manualReleaseRequested.emit)
        self._release_action.setEnabled(False)

        self._scroll = QScrollArea()
        self._scroll.setObjectName("InboxChatScroll")
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._scroll.viewport().setObjectName("InboxChatViewport")

        self._body = QWidget()
        self._body.setObjectName("InboxChatCanvas")
        self._body_layout = QVBoxLayout(self._body)
        self._body_layout.setContentsMargins(18, 16, 18, 16)
        self._body_layout.setSpacing(8)
        self._body_layout.addStretch(1)
        self._scroll.setWidget(self._body)
        shell_layout.addWidget(self._scroll, 1)

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
        self._message_widgets: list[QWidget] = []
        self._rendered_thread_key = ""
        self._rendered_signatures: list[tuple[str, ...]] = []
        self._rendered_identities: list[str] = []
        self._pending_rows: list[dict[str, Any]] = []
        self._pending_signatures: list[tuple[str, ...]] = []
        self._pending_identities: list[str] = []
        self._render_job_id = 0
        self._rendering = False
        self._render_batch_size = 80
        self._pending_scroll_to_bottom = False
        self._pending_restore_value = 0
        self._pending_restore_anchor: dict[str, Any] | None = None
        self._current_health_state = "healthy"
        self._runtime_active = False
        self._manual_send_reason = ""
        self._show_placeholder(
            "Selecciona una conversacion",
            "El historial aparecera aqui apenas abras un thread desde la columna izquierda.",
        )

    def set_runtime_state(self, *, active: bool) -> None:
        self._runtime_active = bool(active)

<<<<<<< HEAD
    def set_thread(
        self,
        thread: dict[str, Any] | None,
        *,
        permissions: dict[str, Any] | None = None,
        truth: dict[str, Any] | None = None,
    ) -> None:
=======
    def set_thread(self, thread: dict[str, Any] | None, *, permissions: dict[str, Any] | None = None) -> None:
>>>>>>> origin/main
        if not thread:
            self._cancel_pending_render()
            self._current_thread_key = ""
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
            self._pack_action.setEnabled(False)
            self._tag_action.setEnabled(False)
            self._follow_up_action.setEnabled(False)
            self._takeover_action.setEnabled(False)
            self._release_action.setEnabled(False)
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
        account_alias = str(thread.get("account_alias") or "").strip()
        recipient = str(thread.get("recipient_username") or "-").strip() or "-"
        self._current_health_state = str(thread.get("account_health") or "healthy").strip().lower() or "healthy"
        owner = str(thread.get("owner") or "none").strip().lower() or "none"
        resolved = self._resolve_permissions(thread, permissions)
<<<<<<< HEAD
        truth_payload = dict(truth or {})
=======
>>>>>>> origin/main
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
<<<<<<< HEAD
        self._submeta.setText(_thread_submeta(thread, truth=truth_payload))
        self._context_badge.setText(stage or bucket or "Thread activo")
        self._state_badge.setText(str(truth_payload.get("label") or _thread_state(thread)).strip() or "Thread activo")
=======
        self._submeta.setText(_thread_submeta(thread))
        self._context_badge.setText(stage or bucket or "Thread activo")
        self._state_badge.setText(_thread_state(thread))
>>>>>>> origin/main
        self._composer_hint.setText(self._composer_hint_text())
        self._input.setPlaceholderText(self._composer_placeholder(can_reply, read_only))
        self._input.setReadOnly(read_only or not can_reply)
        self._input.setEnabled(can_reply or read_only)
        self._send_button.setEnabled(can_reply)
        self._pack_action.setEnabled(bool(resolved.get("can_send_pack")))
        self._tag_action.setEnabled(bool(resolved.get("can_add_tag")))
        self._follow_up_action.setEnabled(bool(resolved.get("can_mark_follow_up")))
        self._takeover_action.setEnabled(bool(resolved.get("can_takeover_manual")))
        self._release_action.setEnabled(bool(resolved.get("can_release_manual")))

        del owner

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

<<<<<<< HEAD
=======
        if seen_text:
            self._state_badge.setText(seen_text)
>>>>>>> origin/main
        normalized_rows = _normalize_message_rows(rows)
        message_signatures = [_message_signature(row) for row in normalized_rows]
        message_identities = [_message_identity(row, index) for index, row in enumerate(normalized_rows)]
        scrollbar = self._scroll.verticalScrollBar()
        previous_value = scrollbar.value()
        previous_max = scrollbar.maximum()
        near_bottom = previous_max <= 0 or (previous_max - previous_value) <= 56
        scroll_anchor = None if force_scroll_to_bottom or near_bottom else self._capture_scroll_anchor(previous_value)

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
            and len(message_identities) == len(self._rendered_identities)
            and message_identities == self._rendered_identities
        ):
            for widget, row in zip(self._message_widgets, normalized_rows):
                if isinstance(widget, _MessageBubble):
                    widget.update_message(row)
            self._rendered_signatures = message_signatures
        elif (
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
            self._rendered_identities = message_identities
        else:
            self._schedule_render(
                normalized_rows,
                message_signatures,
                message_identities,
                scroll_to_bottom=bool(force_scroll_to_bottom or near_bottom),
                restore_value=previous_value,
                restore_anchor=scroll_anchor,
            )
            return
        if force_scroll_to_bottom or near_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(0, lambda anchor=scroll_anchor, value=previous_value: self._restore_scroll(anchor, value))

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
<<<<<<< HEAD
        self._state_badge.setText("Mensaje en cola local")
=======
        self._state_badge.setText("Mensaje en cola...")
>>>>>>> origin/main

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

    def _restore_scroll(self, anchor: dict[str, Any] | None, value: int) -> None:
        scrollbar = self._scroll.verticalScrollBar()
        if isinstance(anchor, dict):
            identity = str(anchor.get("identity") or "").strip()
            offset = int(anchor.get("offset") or 0)
            for widget in self._message_widgets:
                if not isinstance(widget, _MessageBubble):
                    continue
                if widget.identity() != identity:
                    continue
                scrollbar.setValue(min(max(0, widget.y() - offset), scrollbar.maximum()))
                return
        scrollbar.setValue(min(max(0, int(value or 0)), scrollbar.maximum()))

    def _reset_render_state(self) -> None:
        self._rendered_thread_key = ""
        self._rendered_signatures = []
        self._rendered_identities = []

    def _cancel_pending_render(self) -> None:
        self._render_job_id += 1
        self._rendering = False
        self._pending_rows = []
        self._pending_signatures = []
        self._pending_identities = []
        self._pending_restore_anchor = None

    def _append_message_row(self, row: dict[str, Any]) -> None:
        widget = _MessageBubble(row)
        widget.copyRequested.connect(self._copy_message)
        widget.deleteRequested.connect(self._confirm_delete_message)
        self._body_layout.insertWidget(self._body_layout.count() - 1, widget)
        self._message_widgets.append(widget)

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
            message="¿Queres eliminar este mensaje del Inbox?",
            confirm_text="Si",
            cancel_text="No",
            danger=True,
        )
        if confirmed:
            self.messageDeleteRequested.emit(dict(message))

    def _capture_scroll_anchor(self, fallback_value: int) -> dict[str, Any] | None:
        for widget in self._message_widgets:
            if not isinstance(widget, _MessageBubble):
                continue
            if widget.y() + widget.height() < fallback_value:
                continue
            return {
                "identity": widget.identity(),
                "offset": max(0, fallback_value - widget.y()),
            }
        return None

    def _schedule_render(
        self,
        rows: list[dict[str, Any]],
        signatures: list[tuple[str, ...]],
        identities: list[str],
        *,
        scroll_to_bottom: bool,
        restore_value: int,
        restore_anchor: dict[str, Any] | None,
    ) -> None:
        self._cancel_pending_render()
        self._clear_messages()
        self._pending_rows = list(rows)
        self._pending_signatures = list(signatures)
        self._pending_identities = list(identities)
        self._pending_scroll_to_bottom = bool(scroll_to_bottom)
        self._pending_restore_value = int(restore_value or 0)
        self._pending_restore_anchor = dict(restore_anchor) if isinstance(restore_anchor, dict) else None
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
        self._rendered_identities = list(self._pending_identities)
        pending_scroll_to_bottom = self._pending_scroll_to_bottom
        pending_restore_anchor = self._pending_restore_anchor
        pending_restore_value = self._pending_restore_value
        self._pending_rows = []
        self._pending_signatures = []
        self._pending_identities = []
        self._pending_restore_anchor = None
        if pending_scroll_to_bottom:
            QTimer.singleShot(0, self._scroll_to_bottom)
        else:
            QTimer.singleShot(
                0,
                lambda anchor=pending_restore_anchor, value=pending_restore_value: self._restore_scroll(
                    anchor,
                    value,
                ),
            )


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


class _MessageBubble(QWidget):
    copyRequested = Signal(object)
    deleteRequested = Signal(object)

    def __init__(self, message: dict[str, Any]) -> None:
        super().__init__()
        direction = str(message.get("direction") or "unknown").strip().lower()
        outbound = direction == "outbound"

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(3)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)
        if outbound:
            row.addStretch(1)

        bubble = QFrame()
        bubble.setObjectName("InboxBubbleOut" if outbound else "InboxBubbleIn")
        bubble.setMaximumWidth(520)
        bubble_layout = QVBoxLayout(bubble)
        bubble_layout.setContentsMargins(12, 9, 12, 8)
        bubble_layout.setSpacing(4)

        self._text = QLabel()
        self._text.setObjectName("InboxBubbleText")
        self._text.setWordWrap(True)
        bubble_layout.addWidget(self._text)

        self._meta = QLabel()
        self._meta.setObjectName("InboxBubbleMeta")
        bubble_layout.addWidget(self._meta, 0, Qt.AlignRight)

        row.addWidget(bubble, 0)
        if not outbound:
            row.addStretch(1)
        root.addLayout(row)
        self._identity = ""
        self._signature: tuple[str, ...] = ()
        self._message: dict[str, Any] = {}
        self.update_message(message)

    def update_message(self, message: dict[str, Any]) -> None:
        self._message = dict(message or {})
        self._identity = _message_identity(message)
        self._signature = _message_signature(message)
        self._text.setText(str(message.get("text") or "").strip() or " ")
        self._meta.setText(_message_meta(message))

    def identity(self) -> str:
        return self._identity

    def signature(self) -> tuple[str, ...]:
        return self._signature

    def contextMenuEvent(self, event) -> None:  # type: ignore[override]
        menu = QMenu(self)
        copy_action = menu.addAction("Copiar")
        delete_action = menu.addAction("Eliminar")
        selected = menu.exec(event.globalPos())
        if selected == copy_action:
            self.copyRequested.emit(dict(self._message))
            return
        if selected == delete_action:
            self.deleteRequested.emit(dict(self._message))
            return
        super().contextMenuEvent(event)


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
<<<<<<< HEAD
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
=======
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


def _thread_submeta(thread: dict[str, Any]) -> str:
    parts: list[str] = []
    stage = str(thread.get("stage_id") or thread.get("stage") or "").strip()
    bucket = str(thread.get("bucket") or "").strip()
    alias_id = str(thread.get("account_alias") or "").strip()
>>>>>>> origin/main
    if stage:
        parts.append(f"Etapa {stage}")
    if bucket:
        parts.append(f"Bucket {bucket}")
    if alias_id:
        parts.append(f"Alias @{alias_id}")
<<<<<<< HEAD
    if seen_text:
        parts.append(seen_text)
=======
>>>>>>> origin/main
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
