from __future__ import annotations

import json
from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext

from .actions_panel import ActionsPanel
from .chat_view import ChatView
from .conversation_list import ConversationList
from .inbox_controller import InboxController


class _CompactField(QFrame):
    def __init__(self, label_text: str, widget: QWidget, *, wide: bool = False, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("InboxFieldGroup")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        label = QLabel(label_text)
        label.setObjectName("InboxFieldLabel")
        layout.addWidget(label)
        layout.addWidget(widget)
        if wide:
            self.setMinimumWidth(156)


class _ActionsDrawer(QFrame):
    closeRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("InboxActionsDrawer")
        self.setMinimumWidth(392)
        self.setMaximumWidth(468)

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(12)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)

        title = QLabel("Mas acciones")
        title.setObjectName("InboxDrawerTitle")
        header_row.addWidget(title, 1)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("InboxGhostButton")
        close_button.clicked.connect(self.closeRequested.emit)
        header_row.addWidget(close_button, 0, Qt.AlignRight)
        root.addLayout(header_row)

        subtitle = QLabel("Panel lateral derecho con scroll real para acciones, sugerencias, packs y detalle.")
        subtitle.setObjectName("InboxDrawerSubtitle")
        subtitle.setWordWrap(True)
        root.addWidget(subtitle)

        self._actions_panel = ActionsPanel()
        root.addWidget(self._actions_panel, 1)

    def actions_panel(self) -> ActionsPanel:
        return self._actions_panel


class InboxView(QWidget):
    def __init__(self, ctx: PageContext, controller: InboxController, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._controller = controller
        self._actions_visible = False
        self._list_token = ""
        self._thread_header_token = ""
        self._packs_token = ""
        self._runtime_token = ""
        self._thread_permissions_token = ""

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        self._header = QFrame()
        self._header.setObjectName("InboxTopBar")
        header_layout = QVBoxLayout(self._header)
        header_layout.setContentsMargins(14, 12, 14, 12)
        header_layout.setSpacing(10)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(10)

        self._sidebar_button = QPushButton("Menu")
        self._sidebar_button.setObjectName("InboxHeaderButton")
        if self._ctx.toggle_sidebar is not None:
            self._sidebar_button.clicked.connect(self._ctx.toggle_sidebar)
        else:
            self._sidebar_button.setEnabled(False)
        top_row.addWidget(self._sidebar_button, 0, Qt.AlignLeft)

        title_column = QVBoxLayout()
        title_column.setContentsMargins(0, 0, 0, 0)
        title_column.setSpacing(2)

        title = QLabel("Inbox RM")
        title.setObjectName("InboxHeaderTitle")
        title_column.addWidget(title)

        self._header_summary = QLabel("Bandeja compacta para gestionar conversaciones y runtime.")
        self._header_summary.setObjectName("InboxSummaryText")
        title_column.addWidget(self._header_summary)
        top_row.addLayout(title_column, 1)

        self._back_button = QPushButton("Volver")
        self._back_button.setObjectName("InboxHeaderButton")
        self._back_button.clicked.connect(self._ctx.go_back)
        self._back_button.hide()

        self._sync_label = QLabel("Cache local")
        self._sync_label.setObjectName("InboxMetaChip")
        top_row.addWidget(self._sync_label, 0, Qt.AlignRight | Qt.AlignVCenter)

        self._runtime_status = QLabel("Runtime detenido")
        self._runtime_status.setObjectName("InboxSyncBadge")
        top_row.addWidget(self._runtime_status, 0, Qt.AlignRight | Qt.AlignVCenter)

        refresh_button = QPushButton("Actualizar")
        refresh_button.setObjectName("InboxGhostButton")
        refresh_button.clicked.connect(self._controller.force_refresh)
        top_row.addWidget(refresh_button, 0, Qt.AlignRight | Qt.AlignVCenter)

        self._toggle_button = QPushButton("Mas acciones")
        self._toggle_button.setObjectName("InboxGhostButton")
        self._toggle_button.setCheckable(True)
        self._toggle_button.clicked.connect(self._toggle_actions_panel)
        top_row.addWidget(self._toggle_button, 0, Qt.AlignRight | Qt.AlignVCenter)

        header_layout.addLayout(top_row)

        controls_row = QHBoxLayout()
        controls_row.setContentsMargins(0, 0, 0, 0)
        controls_row.setSpacing(8)

        self._alias_combo = QComboBox()
        self._alias_combo.setObjectName("InboxCompactField")
        self._alias_combo.currentTextChanged.connect(self._controller.set_runtime_alias)
        controls_row.addWidget(_CompactField("Alias activo", self._alias_combo, wide=True))

        self._mode_combo = QComboBox()
        self._mode_combo.setObjectName("InboxCompactField")
        self._mode_combo.addItem("Ambos", "both")
        self._mode_combo.addItem("Autoresponder", "auto")
        self._mode_combo.addItem("Follow-up", "followup")
        controls_row.addWidget(_CompactField("Modo", self._mode_combo, wide=True))

        self._delay_min = QSpinBox()
        self._delay_min.setObjectName("InboxCompactField")
        self._delay_min.setRange(0, 3600)
        self._delay_min.setSuffix("s")
        self._delay_min.setValue(45)
        controls_row.addWidget(_CompactField("Delay min", self._delay_min))

        self._delay_max = QSpinBox()
        self._delay_max.setObjectName("InboxCompactField")
        self._delay_max.setRange(0, 3600)
        self._delay_max.setSuffix("s")
        self._delay_max.setValue(90)
        controls_row.addWidget(_CompactField("Delay max", self._delay_max))

        self._turns = QSpinBox()
        self._turns.setObjectName("InboxCompactField")
        self._turns.setRange(1, 100)
        self._turns.setValue(1)
        controls_row.addWidget(_CompactField("Vueltas", self._turns))

        self._start_runtime = QPushButton("Iniciar runtime")
        self._start_runtime.setObjectName("InboxPrimaryButton")
        self._start_runtime.clicked.connect(self._start_runtime_clicked)
        controls_row.addWidget(self._start_runtime, 0, Qt.AlignBottom)

        self._stop_runtime = QPushButton("Frenar")
        self._stop_runtime.setObjectName("InboxGhostButton")
        self._stop_runtime.clicked.connect(self._controller.stop_runtime)
        controls_row.addWidget(self._stop_runtime, 0, Qt.AlignBottom)

        self._runtime_meta = QLabel("Sin runtime activo")
        self._runtime_meta.setObjectName("InboxSummaryText")
        self._runtime_meta.setWordWrap(True)
        controls_row.addWidget(self._runtime_meta, 1, Qt.AlignVCenter)

        header_layout.addLayout(controls_row)
        root.addWidget(self._header)

        self._splitter = QSplitter(Qt.Horizontal)
        self._splitter.setObjectName("InboxSplitter")
        self._splitter.setChildrenCollapsible(False)
        self._splitter.setHandleWidth(1)

        self._conversation_list = ConversationList()
        self._conversation_list.filterChanged.connect(self._controller.set_filter)
        self._conversation_list.conversationSelected.connect(self._controller.select_thread)
        self._splitter.addWidget(self._conversation_list)

        self._chat_view = ChatView()
        self._chat_view.sendRequested.connect(self._controller.send_message)
        self._chat_view.messageDeleteRequested.connect(self._controller.delete_message)
        self._chat_view.addTagRequested.connect(self._controller.add_tag)
        self._chat_view.markFollowUpRequested.connect(self._controller.mark_follow_up)
        self._chat_view.manualTakeoverRequested.connect(self._controller.take_thread_manual)
        self._chat_view.manualReleaseRequested.connect(self._controller.release_thread_manual)
        self._splitter.addWidget(self._chat_view)

        self._splitter.setStretchFactor(0, 0)
        self._splitter.setStretchFactor(1, 1)
        self._splitter.setSizes([308, 1152])
        root.addWidget(self._splitter, 1)

        self._actions_drawer = _ActionsDrawer(self)
        self._actions_panel = self._actions_drawer.actions_panel()
        self._actions_panel.packSelected.connect(self._controller.send_pack)
        self._actions_panel.aiSuggestionRequested.connect(self._controller.request_ai_suggestion)
        self._actions_panel.suggestionInsertRequested.connect(self._chat_view.load_suggestion)
        self._actions_panel.addTagRequested.connect(self._controller.add_tag)
        self._actions_panel.markFollowUpRequested.connect(self._controller.mark_follow_up)
        self._actions_panel.manualTakeoverRequested.connect(self._controller.take_thread_manual)
        self._actions_panel.manualReleaseRequested.connect(self._controller.release_thread_manual)
        self._actions_panel.markQualifiedRequested.connect(self._controller.mark_thread_qualified)
        self._actions_panel.markDisqualifiedRequested.connect(self._controller.mark_thread_disqualified)
        self._actions_panel.clearClassificationRequested.connect(self._controller.clear_thread_classification)
        self._actions_drawer.closeRequested.connect(lambda: self._set_actions_visible(False))
        self._actions_drawer.hide()

        self._controller.snapshot_changed.connect(self._apply_snapshot)

        self.setObjectName("InboxView")
        self.setStyleSheet(_INBOX_STYLESHEET)

    def activate(self, *, initial_thread_key: str = "") -> None:
        self._back_button.setEnabled(self._ctx.can_go_back())
        self._controller.activate(initial_thread_key=initial_thread_key)

    def deactivate(self) -> None:
        self._controller.deactivate()
        self._set_actions_visible(False)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._position_actions_drawer()

    def _position_actions_drawer(self) -> None:
        header_bottom = self._header.geometry().bottom()
        margin = 12
        max_allowed = max(320, self.width() - (margin * 2))
        drawer_width = min(max_allowed, min(468, max(392, int(self.width() * 0.30))))
        x = max(margin, self.width() - drawer_width - margin)
        y = header_bottom + 8
        height = max(420, self.height() - y - margin)
        self._actions_drawer.setGeometry(x, y, drawer_width, height)
        self._actions_drawer.raise_()

    def _set_actions_visible(self, visible: bool) -> None:
        self._actions_visible = bool(visible)
        self._toggle_button.setChecked(self._actions_visible)
        self._position_actions_drawer()
        self._actions_drawer.setVisible(self._actions_visible)
        if self._actions_visible:
            self._actions_drawer.raise_()

    def _toggle_actions_panel(self) -> None:
        self._set_actions_visible(not self._actions_visible)

    def _start_runtime_clicked(self) -> None:
        self._controller.start_runtime(
            {
                "alias_id": str(self._alias_combo.currentText() or "").strip(),
                "mode": str(self._mode_combo.currentData() or "both").strip(),
                "delay_min": int(self._delay_min.value()) * 1000,
                "delay_max": int(self._delay_max.value()) * 1000,
                "turns_per_account": int(self._turns.value()),
            }
        )

    def _apply_snapshot(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        self._back_button.setEnabled(self._ctx.can_go_back())
        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        self._header_summary.setText(
            f"{int(metrics.get('threads') or 0)} conversaciones  |  {int(metrics.get('pending') or 0)} sin responder"
        )
        self._sync_label.setText(str(payload.get("sync_label") or "Cache local"))

        runtime_aliases = [str(item or "").strip() for item in payload.get("runtime_aliases") or [] if str(item or "").strip()]
        if runtime_aliases:
            current_alias = str((payload.get("runtime_status") or {}).get("alias_id") or self._alias_combo.currentText() or runtime_aliases[0]).strip()
            combo_values = [self._alias_combo.itemText(index) for index in range(self._alias_combo.count())]
            if combo_values != runtime_aliases:
                self._alias_combo.blockSignals(True)
                self._alias_combo.clear()
                self._alias_combo.addItems(runtime_aliases)
                self._alias_combo.blockSignals(False)
            if current_alias:
                idx = self._alias_combo.findText(current_alias)
                if idx >= 0:
                    self._alias_combo.setCurrentIndex(idx)

        runtime_status = payload.get("runtime_status") if isinstance(payload.get("runtime_status"), dict) else {}
        is_running = bool(runtime_status.get("is_running"))
        has_runtime_alias = bool(str(self._alias_combo.currentText() or "").strip())
        self._alias_combo.setEnabled(bool(runtime_aliases))
        self._start_runtime.setEnabled(has_runtime_alias and not is_running)
        self._stop_runtime.setEnabled(has_runtime_alias and is_running)
        for widget in (self._mode_combo, self._delay_min, self._delay_max, self._turns):
            widget.setEnabled(has_runtime_alias and not is_running)

        runtime_token = _serialize_rows(runtime_status)
        if runtime_token != self._runtime_token:
            self._runtime_token = runtime_token
            self._runtime_status.setText("Runtime activo" if is_running else "Runtime detenido")
            self._runtime_meta.setText(
                (
                    f"Actual @{str(runtime_status.get('current_account_id') or '-').strip() or '-'}"
                    f"  |  Proxima @{str(runtime_status.get('next_account_id') or '-').strip() or '-'}"
                    f"  |  Modo {str(runtime_status.get('mode') or 'both')}"
                    f"  |  Turno {int(runtime_status.get('current_turn_count') or 0)}/{int(runtime_status.get('max_turns_per_account') or 1)}"
                )
                if runtime_status
                else "Sin runtime activo"
            )

        rows = [row for row in payload.get("rows") or [] if isinstance(row, dict)]
        current_thread_key = str(payload.get("current_thread_key") or "").strip()
        list_token = _serialize_rows(
            {
                "current_thread_key": current_thread_key,
                "total_count": int(payload.get("total_count") or 0),
                "rows": [
                    {
                        "thread_key": str(row.get("thread_key") or "").strip(),
                        "display_name": str(row.get("display_name") or "").strip(),
                        "recipient_username": str(row.get("recipient_username") or "").strip(),
                        "last_message_text": str(row.get("last_message_text") or "").strip(),
                        "last_message_direction": str(row.get("last_message_direction") or "").strip(),
                        "last_message_timestamp": row.get("last_message_timestamp"),
                        "unread_count": row.get("unread_count"),
                        "needs_reply": bool(row.get("needs_reply")) if "needs_reply" in row else None,
                        "account_health": str(row.get("account_health") or "").strip(),
                    }
                    for row in rows
                ],
            }
        )
        if list_token != self._list_token:
            self._list_token = list_token
            self._conversation_list.set_threads(
                rows,
                current_thread_key=current_thread_key,
                total_count=int(payload.get("total_count") or 0),
            )

        thread = payload.get("thread") if isinstance(payload.get("thread"), dict) else None
        thread_permissions = payload.get("thread_permissions") if isinstance(payload.get("thread_permissions"), dict) else {}
        thread_permissions_token = _serialize_rows(thread_permissions)
        permissions_changed = thread_permissions_token != self._thread_permissions_token
        if permissions_changed:
            self._thread_permissions_token = thread_permissions_token
        header_token = _serialize_rows(
            {
                "thread_key": str((thread or {}).get("thread_key") or "").strip(),
                "display_name": str((thread or {}).get("display_name") or "").strip(),
                "account_id": str((thread or {}).get("account_id") or "").strip(),
                "account_alias": str((thread or {}).get("account_alias") or "").strip(),
                "recipient_username": str((thread or {}).get("recipient_username") or "").strip(),
                "last_seen_text": str((thread or {}).get("last_seen_text") or "").strip(),
                "last_message_direction": str((thread or {}).get("last_message_direction") or "").strip(),
                "last_message_timestamp": (thread or {}).get("last_message_timestamp"),
                "stage": str((thread or {}).get("stage_id") or (thread or {}).get("stage") or "").strip(),
                "owner": str((thread or {}).get("owner") or "").strip(),
                "bucket": str((thread or {}).get("bucket") or "").strip(),
                "quality": str((thread or {}).get("quality") or "").strip(),
                "last_action_type": str((thread or {}).get("last_action_type") or "").strip(),
                "last_pack_sent": str((thread or {}).get("last_pack_sent") or (thread or {}).get("pack_name") or "").strip(),
                "suggestion_status": str((thread or {}).get("suggestion_status") or "").strip(),
                "suggestion_error": str((thread or {}).get("suggestion_error") or "").strip(),
                "suggested_reply": str((thread or {}).get("suggested_reply") or "").strip(),
                "suggested_reply_at": (thread or {}).get("suggested_reply_at"),
                "tags": list((thread or {}).get("tags") or []) if isinstance((thread or {}).get("tags"), (list, tuple, set)) else str((thread or {}).get("tags") or "").strip(),
            }
        )
        if header_token != self._thread_header_token or permissions_changed:
            self._thread_header_token = header_token
            self._chat_view.set_thread(thread, permissions=thread_permissions)
            self._actions_panel.set_thread(thread, permissions=thread_permissions)

        packs = [pack for pack in payload.get("packs") or [] if isinstance(pack, dict)]
        packs_token = _serialize_rows(
            [
                {
                    "id": str(pack.get("id") or "").strip(),
                    "name": str(pack.get("name") or "").strip(),
                    "type": str(pack.get("type") or "").strip(),
                    "active": bool(pack.get("active", True)),
                }
                for pack in packs
            ]
        )
        if packs_token != self._packs_token:
            self._packs_token = packs_token
            self._actions_panel.set_packs(packs)

        self._actions_panel.set_status(str(payload.get("actions_status") or "").strip())

        if not thread:
            self._chat_view.set_thread(None, permissions=thread_permissions)
            self._actions_panel.set_thread(None, permissions=thread_permissions)
            return

        message_rows = [row for row in payload.get("messages") or [] if isinstance(row, dict)]
        if bool(payload.get("loading", False)) and not message_rows:
            self._chat_view.set_loading(True)
            return
        thread_error = str(payload.get("thread_error") or "").strip()
        if thread_error and not message_rows:
            self._chat_view.set_error(thread_error)
            return

        self._chat_view.set_messages(
            message_rows,
            seen_text=str(payload.get("seen_text") or "").strip(),
            force_scroll_to_bottom=bool(payload.get("force_scroll_to_bottom", False)),
        )


def _serialize_rows(payload: Any) -> str:
    try:
        return json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str, separators=(",", ":"))
    except Exception:
        return repr(payload)


_INBOX_STYLESHEET = """
QWidget#InboxView {
    background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 1,
        stop: 0 #07111b,
        stop: 0.5 #0a1523,
        stop: 1 #0d1828);
    color: #e4eefb;
}
QFrame#InboxTopBar, QFrame#InboxRailCard, QFrame#InboxStageCard, QFrame#InboxActionsDrawer {
    background: rgba(10, 20, 34, 0.96);
    border: 1px solid #16293e;
    border-radius: 18px;
}
QFrame#InboxDrawerSection, QFrame#InboxSubtleCard, QFrame#InboxFieldGroup, QFrame#InboxDetailItem {
    background: rgba(7, 16, 28, 0.95);
    border: 1px solid #16283d;
    border-radius: 14px;
}
QPushButton#InboxHeaderButton,
QPushButton#InboxGhostButton,
QPushButton#InboxFilterButton,
QPushButton#InboxMiniAction {
    background: #0f1b2c;
    color: #d7e6f7;
    border: 1px solid #26415f;
    border-radius: 11px;
    padding: 6px 11px;
    font-size: 11px;
    font-weight: 700;
}
QPushButton#InboxHeaderButton:hover,
QPushButton#InboxGhostButton:hover,
QPushButton#InboxFilterButton:hover,
QPushButton#InboxMiniAction:hover {
    background: #13253b;
    border-color: #42729e;
}
QPushButton#InboxGhostButton:checked,
QPushButton#InboxFilterButton:checked,
QPushButton#InboxPrimaryButton {
    background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0,
        stop: 0 #0f5bd8,
        stop: 1 #2ec8ff);
    color: #ffffff;
    border: 1px solid #69b5ff;
    border-radius: 12px;
    padding: 7px 14px;
    font-size: 11px;
    font-weight: 800;
}
QPushButton#InboxPrimaryButton:disabled,
QPushButton#InboxGhostButton:disabled,
QPushButton#InboxHeaderButton:disabled,
QPushButton#InboxMiniAction:disabled {
    background: #152334;
    color: #62758f;
    border-color: #203247;
}
QLabel#InboxHeaderTitle, QLabel#InboxSectionTitle, QLabel#InboxChatTitle, QLabel#InboxDrawerTitle, QLabel#InboxLeadTitle {
    font-size: 15px;
    font-weight: 800;
    color: #f5f9ff;
}
QLabel#InboxHeaderMetricLabel,
QLabel#InboxSectionSubtitle,
QLabel#InboxSummaryText,
QLabel#InboxChatMeta,
QLabel#InboxComposerHint,
QLabel#InboxMutedText,
QLabel#InboxBubbleMeta,
QLabel#InboxPackSteps,
QLabel#InboxDrawerSubtitle,
QLabel#InboxLeadSubtitle,
QLabel#InboxFieldLabel,
QLabel#InboxDetailLabel,
QLabel#InboxEmptyStateText {
    font-size: 10px;
    color: #8297b3;
}
QLabel#InboxDetailValue, QLabel#InboxEmptyStateTitle, QLabel#InboxPackName {
    color: #edf4ff;
    font-size: 11px;
    font-weight: 700;
}
QLabel#InboxSyncBadge,
QLabel#InboxStateBadge,
QLabel#InboxThreadBadge,
QLabel#InboxMetaChip,
QLabel#InboxPendingChip,
QLabel#InboxPackBadge {
    border-radius: 10px;
    padding: 4px 9px;
    font-size: 10px;
    font-weight: 700;
}
QLabel#InboxSyncBadge {
    background: rgba(31, 112, 255, 0.18);
    color: #ddeaff;
    border: 1px solid #335f98;
}
QLabel#InboxStateBadge {
    background: #0d1d31;
    color: #dceaff;
    border: 1px solid #2c4d74;
}
QLabel#InboxThreadBadge,
QLabel#InboxMetaChip {
    background: #0c1728;
    color: #9fb7d5;
    border: 1px solid #22364d;
}
QLabel#InboxPackBadge {
    background: #0f2236;
    color: #8ec7ff;
    border: 1px solid #23486c;
}
QFrame#InboxChatHeader {
    background: transparent;
    border: none;
    border-bottom: 1px solid #15263a;
}
QFrame#InboxComposerDock {
    background: #091321;
    border: none;
    border-top: 1px solid #15263a;
    border-bottom-left-radius: 18px;
    border-bottom-right-radius: 18px;
}
QListView#InboxConversationView,
QScrollArea#InboxChatScroll,
QScrollArea#InboxActionsScroll,
QWidget#InboxChatViewport,
QWidget#InboxChatCanvas,
QWidget#InboxActionsCanvas,
QListWidget#InboxPackList {
    background: transparent;
    border: none;
}
QComboBox#InboxCompactField,
QSpinBox#InboxCompactField {
    background: #0b1626;
    color: #edf4ff;
    border: 1px solid #1b2f45;
    border-radius: 10px;
    padding: 5px 8px;
    min-height: 18px;
}
QComboBox#InboxCompactField::drop-down {
    border: none;
    width: 18px;
}
QPlainTextEdit#InboxComposer {
    background: #0b1626;
    color: #e5eefc;
    border: 1px solid #1c3149;
    border-radius: 13px;
    padding: 9px 11px;
    font-size: 12px;
}
QPlainTextEdit#InboxSuggestionPreview {
    background: #0b1626;
    color: #e5eefc;
    border: 1px solid #1c3149;
    border-radius: 13px;
    padding: 9px 11px;
    font-size: 12px;
}
QFrame#InboxBubbleOut {
    background: #13395f;
    border: 1px solid #255990;
    border-radius: 15px;
}
QFrame#InboxBubbleIn {
    background: #0f1d2f;
    border: 1px solid #1b3047;
    border-radius: 15px;
}
QLabel#InboxBubbleText {
    color: #f4f8ff;
    font-size: 12px;
}
QLabel#InboxChatAvatar {
    min-width: 38px;
    min-height: 38px;
    max-width: 38px;
    max-height: 38px;
    border-radius: 19px;
    background: #1c4d77;
    color: #ffffff;
    font-weight: 800;
}
QFrame#InboxPackCard,
QFrame#InboxEmptyStateCard {
    background: #0c1828;
    border: 1px solid #183149;
    border-radius: 13px;
}
QListWidget#InboxPackList::item {
    padding: 0px;
}
QScrollArea#InboxActionsScroll QWidget#qt_scrollarea_viewport {
    background: transparent;
}
QSplitter#InboxSplitter::handle {
    background: #102034;
}
QMenu {
    background: #0b1625;
    color: #e5eefc;
    border: 1px solid #20354d;
    border-radius: 10px;
}
QMenu::item {
    padding: 7px 12px;
    border-radius: 6px;
}
QMenu::item:selected {
    background: #143152;
}
"""
