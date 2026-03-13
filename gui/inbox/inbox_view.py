from __future__ import annotations

import json
from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext

from .actions_panel import ActionsPanel
from .chat_view import ChatView
from .conversation_list import ConversationList
from .inbox_controller import InboxController


class _ActionsDrawer(QFrame):
    closeRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("InboxActionsDrawer")
        self.setMinimumWidth(340)
        self.setMaximumWidth(380)

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(14)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(10)

        title = QLabel("Acciones")
        title.setObjectName("InboxDrawerTitle")
        header_row.addWidget(title, 1)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("InboxGhostButton")
        close_button.clicked.connect(self.closeRequested.emit)
        header_row.addWidget(close_button, 0, Qt.AlignRight)
        root.addLayout(header_row)

        subtitle = QLabel("Panel lateral del contacto y herramientas de respuesta.")
        subtitle.setObjectName("InboxDrawerSubtitle")
        subtitle.setWordWrap(True)
        root.addWidget(subtitle)

        quick_wrap = QFrame()
        quick_wrap.setObjectName("InboxDrawerSection")
        quick_layout = QVBoxLayout(quick_wrap)
        quick_layout.setContentsMargins(14, 14, 14, 14)
        quick_layout.setSpacing(10)

        quick_title = QLabel("Acciones rapidas")
        quick_title.setObjectName("InboxSectionTitle")
        quick_layout.addWidget(quick_title)

        quick_grid = QGridLayout()
        quick_grid.setContentsMargins(0, 0, 0, 0)
        quick_grid.setHorizontalSpacing(10)
        quick_grid.setVerticalSpacing(10)
        actions = (
            ("Crear nota", False),
            ("Marcar no leido", False),
            ("Marcar spam", True),
            ("Eliminar contacto", True),
            ("Ver historial", False),
            ("Datos personales", False),
            ("Ver negocio", False),
        )
        for index, (label, danger) in enumerate(actions):
            button = QPushButton(label)
            button.setObjectName("InboxDrawerQuickAction")
            button.setProperty("danger", bool(danger))
            button.setEnabled(False)
            button.setToolTip("UI preparada sin modificar handlers ni backend.")
            quick_grid.addWidget(button, index // 2, index % 2)
        quick_layout.addLayout(quick_grid)
        root.addWidget(quick_wrap)

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

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        self._header = QFrame()
        self._header.setObjectName("InboxTopBar")
        header_layout = QHBoxLayout(self._header)
        header_layout.setContentsMargins(16, 12, 16, 12)
        header_layout.setSpacing(14)

        left_row = QHBoxLayout()
        left_row.setContentsMargins(0, 0, 0, 0)
        left_row.setSpacing(10)

        self._sidebar_button = QPushButton("Menu")
        self._sidebar_button.setObjectName("InboxHeaderButton")
        if self._ctx.toggle_sidebar is not None:
            self._sidebar_button.clicked.connect(self._ctx.toggle_sidebar)
        else:
            self._sidebar_button.setEnabled(False)
        left_row.addWidget(self._sidebar_button, 0, Qt.AlignLeft)

        self._back_button = QPushButton("Volver")
        self._back_button.setObjectName("InboxHeaderButton")
        self._back_button.clicked.connect(self._ctx.go_back)
        left_row.addWidget(self._back_button, 0, Qt.AlignLeft)

        title = QLabel("Inbox RM")
        title.setObjectName("InboxHeaderTitle")
        left_row.addWidget(title, 0, Qt.AlignLeft)
        header_layout.addLayout(left_row, 0)

        metrics_row = QHBoxLayout()
        metrics_row.setContentsMargins(0, 0, 0, 0)
        metrics_row.setSpacing(10)
        conversations_card, self._threads_metric = self._build_metric_chip("Conversaciones")
        unread_card, self._unread_metric = self._build_metric_chip("No leidos")
        pending_card, self._pending_metric = self._build_metric_chip("Sin responder")
        metrics_row.addWidget(conversations_card)
        metrics_row.addWidget(unread_card)
        metrics_row.addWidget(pending_card)
        header_layout.addLayout(metrics_row, 1)

        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(10)

        refresh_button = QPushButton("Actualizar")
        refresh_button.setObjectName("InboxGhostButton")
        refresh_button.clicked.connect(self._controller.force_refresh)
        controls.addWidget(refresh_button, 0, Qt.AlignRight | Qt.AlignVCenter)

        self._sync_label = QLabel("Cache local")
        self._sync_label.setObjectName("InboxSyncBadge")
        controls.addWidget(self._sync_label, 0, Qt.AlignRight | Qt.AlignVCenter)

        self._toggle_button = QPushButton("Más acciones")
        self._toggle_button.setObjectName("InboxGhostButton")
        self._toggle_button.clicked.connect(self._toggle_actions_panel)
        controls.addWidget(self._toggle_button, 0, Qt.AlignRight | Qt.AlignVCenter)

        header_layout.addLayout(controls, 0)
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
        self._chat_view.actionsRequested.connect(self._toggle_actions_panel)
        self._chat_view.addTagRequested.connect(self._controller.add_tag)
        self._chat_view.markFollowUpRequested.connect(self._controller.mark_follow_up)
        self._splitter.addWidget(self._chat_view)

        self._splitter.setStretchFactor(0, 0)
        self._splitter.setStretchFactor(1, 1)
        self._splitter.setSizes([332, 1128])
        root.addWidget(self._splitter, 1)

        self._actions_drawer = _ActionsDrawer(self)
        self._actions_panel = self._actions_drawer.actions_panel()
        self._actions_panel.packSelected.connect(self._controller.send_pack)
        self._actions_panel.aiSuggestionRequested.connect(self._controller.request_ai_suggestion)
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

    def _build_metric_chip(self, label_text: str) -> tuple[QFrame, QLabel]:
        card = QFrame()
        card.setObjectName("InboxHeaderMetricCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(2)

        label = QLabel(label_text)
        label.setObjectName("InboxHeaderMetricLabel")
        value = QLabel("0")
        value.setObjectName("InboxHeaderMetricValue")
        layout.addWidget(label)
        layout.addWidget(value)
        return card, value

    def _position_actions_drawer(self) -> None:
        header_bottom = self._header.geometry().bottom()
        drawer_width = min(360, max(320, self.width() // 4))
        margin = 16
        x = self.width() - drawer_width - margin
        y = header_bottom + 12
        height = max(420, self.height() - y - margin)
        self._actions_drawer.setGeometry(x, y, drawer_width, height)
        self._actions_drawer.raise_()

    def _set_actions_visible(self, visible: bool) -> None:
        self._actions_visible = bool(visible)
        self._toggle_button.setText("Ocultar acciones" if self._actions_visible else "Más acciones")
        self._position_actions_drawer()
        self._actions_drawer.setVisible(self._actions_visible)
        if self._actions_visible:
            self._actions_drawer.raise_()

    def _toggle_actions_panel(self) -> None:
        self._set_actions_visible(not self._actions_visible)

    def _apply_snapshot(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        self._back_button.setEnabled(self._ctx.can_go_back())
        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        self._threads_metric.setText(str(int(metrics.get("threads") or 0)))
        self._unread_metric.setText(str(int(metrics.get("unread") or 0)))
        self._pending_metric.setText(str(int(metrics.get("pending") or 0)))
        self._sync_label.setText(str(payload.get("sync_label") or "Cache local"))

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
                        "account_id": str(row.get("account_id") or "").strip(),
                        "last_message_id": str(row.get("last_message_id") or "").strip(),
                        "last_message_text": str(row.get("last_message_text") or "").strip(),
                        "last_message_direction": str(row.get("last_message_direction") or "").strip(),
                        "last_message_timestamp": row.get("last_message_timestamp"),
                        "unread_count": row.get("unread_count"),
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
        header_token = _serialize_rows(
            {
                "thread_key": str((thread or {}).get("thread_key") or "").strip(),
                "display_name": str((thread or {}).get("display_name") or "").strip(),
                "account_id": str((thread or {}).get("account_id") or "").strip(),
                "recipient_username": str((thread or {}).get("recipient_username") or "").strip(),
                "last_seen_text": str((thread or {}).get("last_seen_text") or "").strip(),
                "last_message_direction": str((thread or {}).get("last_message_direction") or "").strip(),
            }
        )
        if header_token != self._thread_header_token:
            self._thread_header_token = header_token
            self._chat_view.set_thread(thread)
            self._actions_panel.set_thread(thread)

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
            self._chat_view.set_thread(None)
            self._actions_panel.set_thread(None)
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
        stop: 0 #08111d,
        stop: 0.55 #0a1525,
        stop: 1 #10192a);
    color: #e5eefc;
}
QFrame#InboxTopBar, QFrame#InboxRailCard, QFrame#InboxStageCard, QFrame#InboxActionsDrawer {
    background: #0d1727;
    border: 1px solid #1a2c44;
    border-radius: 20px;
}
QFrame#InboxDrawerSection, QFrame#InboxSubtleCard {
    background: #091321;
    border: 1px solid #1b2c45;
    border-radius: 16px;
}
QPushButton#InboxHeaderButton,
QPushButton#InboxGhostButton,
QPushButton#InboxFilterButton {
    background: #101b2d;
    color: #d8e6f8;
    border: 1px solid #28415f;
    border-radius: 12px;
    padding: 8px 12px;
    font-weight: 700;
}
QPushButton#InboxHeaderButton:hover,
QPushButton#InboxGhostButton:hover,
QPushButton#InboxFilterButton:hover {
    background: #13233b;
    border-color: #45668f;
}
QPushButton#InboxFilterButton:checked,
QPushButton#InboxPrimaryButton {
    background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0,
        stop: 0 #2563eb,
        stop: 1 #38bdf8);
    color: #ffffff;
    border: 1px solid #63a7ff;
    border-radius: 14px;
    padding: 9px 16px;
    font-weight: 800;
}
QPushButton#InboxPrimaryButton:disabled,
QPushButton#InboxGhostButton:disabled,
QPushButton#InboxHeaderButton:disabled,
QPushButton#InboxDrawerQuickAction:disabled {
    background: #172438;
    color: #60728c;
    border-color: #22364f;
}
QLabel#InboxHeaderTitle, QLabel#InboxSectionTitle, QLabel#InboxChatTitle, QLabel#InboxDrawerTitle {
    font-size: 16px;
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
QLabel#InboxDrawerSubtitle {
    font-size: 11px;
    color: #8ea3bf;
}
QFrame#InboxHeaderMetricCard {
    background: rgba(10, 20, 34, 0.84);
    border: 1px solid #223954;
    border-radius: 14px;
}
QLabel#InboxHeaderMetricValue {
    font-size: 17px;
    font-weight: 800;
    color: #f5f9ff;
}
QLabel#InboxSyncBadge,
QLabel#InboxStateBadge,
QLabel#InboxThreadBadge,
QLabel#InboxMetaChip,
QLabel#InboxPendingChip,
QLabel#InboxPackBadge {
    border-radius: 12px;
    padding: 5px 10px;
    font-size: 11px;
    font-weight: 700;
}
QLabel#InboxSyncBadge {
    background: rgba(37, 99, 235, 0.18);
    color: #d9e8ff;
    border: 1px solid #335b96;
}
QLabel#InboxStateBadge {
    background: #0d1d34;
    color: #d9e8ff;
    border: 1px solid #33557d;
}
QLabel#InboxThreadBadge,
QLabel#InboxMetaChip {
    background: #0e1b2f;
    color: #9bb4d4;
    border: 1px solid #233854;
}
QLabel#InboxPackBadge {
    background: #112238;
    color: #8ec2ff;
    border: 1px solid #27486b;
}
QFrame#InboxChatHeader {
    background: transparent;
    border: none;
    border-bottom: 1px solid #1a2a40;
}
QFrame#InboxComposerDock {
    background: #091321;
    border: none;
    border-top: 1px solid #1a2a40;
    border-bottom-left-radius: 20px;
    border-bottom-right-radius: 20px;
}
QListView#InboxConversationView,
QScrollArea#InboxChatScroll,
QWidget#InboxChatViewport,
QWidget#InboxChatCanvas,
QListWidget#InboxPackList {
    background: transparent;
    border: none;
}
QPlainTextEdit#InboxComposer {
    background: #0b1626;
    color: #e5eefc;
    border: 1px solid #1e324d;
    border-radius: 14px;
    padding: 10px 12px;
}
QFrame#InboxBubbleOut {
    background: #163761;
    border: 1px solid #27558e;
    border-radius: 16px;
}
QFrame#InboxBubbleIn {
    background: #101e31;
    border: 1px solid #213650;
    border-radius: 16px;
}
QLabel#InboxBubbleText {
    color: #f4f8ff;
    font-size: 13px;
}
QLabel#InboxChatAvatar {
    min-width: 42px;
    min-height: 42px;
    max-width: 42px;
    max-height: 42px;
    border-radius: 21px;
    background: #1f4e79;
    color: #ffffff;
    font-weight: 800;
}
"""
