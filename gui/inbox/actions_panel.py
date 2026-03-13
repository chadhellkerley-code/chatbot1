from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class _PackCard(QFrame):
    def __init__(self, pack: dict[str, Any]) -> None:
        super().__init__()
        self.setObjectName("InboxPackCard")
        self.setCursor(Qt.PointingHandCursor)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)

        name = QLabel(str(pack.get("name") or "Pack").strip() or "Pack")
        name.setObjectName("InboxPackName")
        name.setWordWrap(True)
        top_row.addWidget(name, 1)

        pack_type = str(pack.get("type") or "pack").strip().upper() or "PACK"
        badge = QLabel(pack_type)
        badge.setObjectName("InboxPackBadge")
        top_row.addWidget(badge, 0, Qt.AlignRight | Qt.AlignTop)
        layout.addLayout(top_row)

        actions = pack.get("actions")
        action_count = len(actions) if isinstance(actions, list) else 0

        steps = QLabel(f"{action_count} pasos  |  click para enviar respetando delays")
        steps.setObjectName("InboxPackSteps")
        steps.setWordWrap(True)
        layout.addWidget(steps)


class _EmptyPackCard(QFrame):
    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__()
        self.setObjectName("InboxEmptyStateCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("InboxEmptyStateTitle")

        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("InboxEmptyStateText")
        subtitle_label.setWordWrap(True)

        layout.addWidget(title_label)
        layout.addWidget(subtitle_label)


class ActionsPanel(QWidget):
    packSelected = Signal(str)
    aiSuggestionRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._has_thread = False
        self._pack_count = 0
        self._healthy_account = True

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        panel = QFrame()
        panel.setObjectName("InboxRailCard")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(18, 18, 18, 18)
        panel_layout.setSpacing(14)

        title = QLabel("Acciones")
        title.setObjectName("InboxSectionTitle")
        subtitle = QLabel("Packs conversacionales e IA asistida")
        subtitle.setObjectName("InboxSectionSubtitle")
        self._thread_badge = QLabel("Sin thread activo")
        self._thread_badge.setObjectName("InboxThreadBadge")

        panel_layout.addWidget(title)
        panel_layout.addWidget(subtitle)
        panel_layout.addWidget(self._thread_badge, 0, Qt.AlignLeft)

        ai_card = QFrame()
        ai_card.setObjectName("InboxSubtleCard")
        ai_layout = QVBoxLayout(ai_card)
        ai_layout.setContentsMargins(16, 16, 16, 16)
        ai_layout.setSpacing(10)

        ai_title = QLabel("Respuesta sugerida")
        ai_title.setObjectName("InboxSectionTitle")
        ai_layout.addWidget(ai_title)

        ai_meta = QLabel("Usa mensajes recientes, packs y flow engine. Solo inserta texto en el composer.")
        ai_meta.setObjectName("InboxSectionSubtitle")
        ai_meta.setWordWrap(True)
        ai_layout.addWidget(ai_meta)

        self._ai_button = QPushButton("Sugerir respuesta IA")
        self._ai_button.setObjectName("InboxPrimaryButton")
        self._ai_button.clicked.connect(self.aiSuggestionRequested.emit)
        ai_layout.addWidget(self._ai_button)

        self._status = QLabel("Selecciona una conversacion para habilitar IA y packs.")
        self._status.setObjectName("InboxMutedText")
        self._status.setWordWrap(True)
        ai_layout.addWidget(self._status)
        panel_layout.addWidget(ai_card)

        packs_card = QFrame()
        packs_card.setObjectName("InboxSubtleCard")
        packs_layout = QVBoxLayout(packs_card)
        packs_layout.setContentsMargins(12, 12, 12, 12)
        packs_layout.setSpacing(10)

        packs_title_row = QHBoxLayout()
        packs_title_row.setContentsMargins(4, 2, 4, 0)
        packs_title_row.setSpacing(8)

        packs_title = QLabel("Packs conversacionales")
        packs_title.setObjectName("InboxSectionTitle")
        packs_title_row.addWidget(packs_title, 1)

        self._packs_summary = QLabel("0 disponibles")
        self._packs_summary.setObjectName("InboxSummaryText")
        packs_title_row.addWidget(self._packs_summary, 0, Qt.AlignRight)
        packs_layout.addLayout(packs_title_row)

        packs_hint = QLabel("Click en un pack para disparar la secuencia completa.")
        packs_hint.setObjectName("InboxSectionSubtitle")
        packs_hint.setWordWrap(True)
        packs_layout.addWidget(packs_hint)

        self._packs = QListWidget()
        self._packs.setObjectName("InboxPackList")
        self._packs.setFrameShape(QFrame.NoFrame)
        self._packs.setSpacing(10)
        self._packs.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._packs.viewport().setObjectName("InboxPackViewport")
        self._packs.itemClicked.connect(self._emit_pack_selected)
        packs_layout.addWidget(self._packs, 1)
        panel_layout.addWidget(packs_card, 1)

        root.addWidget(panel)
        self._update_availability()

    def set_thread(self, thread: dict[str, Any] | None) -> None:
        self._has_thread = bool(thread)
        self._healthy_account = True
        if not thread:
            self._thread_badge.setText("Sin thread activo")
            self._status.setText("Selecciona una conversacion para habilitar IA y packs.")
            self._update_availability()
            return

        account_id = str(thread.get("account_id") or "-").strip() or "-"
        recipient = str(thread.get("recipient_username") or "-").strip() or "-"
        health = str(thread.get("account_health") or "healthy").strip().lower()
        self._healthy_account = health == "healthy"

        self._thread_badge.setText(f"Thread @{account_id}")
        if self._healthy_account:
            self._status.setText(f"Cuenta emisora: @{account_id}\nCliente: @{recipient}")
        else:
            reason = str(thread.get("account_health_reason") or "").strip()
            self._status.setText(
                f"Cuenta emisora: @{account_id}\nCliente: @{recipient}\nEstado: {_health_label(health)}"
                + (f"\n{reason}" if reason else "")
            )
        self._update_availability()

    def set_packs(self, packs: list[dict[str, Any]]) -> None:
        self._packs.clear()
        self._pack_count = 0

        valid_packs = [pack for pack in packs if isinstance(pack, dict)]
        self._pack_count = len(valid_packs)
        self._packs_summary.setText(f"{self._pack_count} disponibles")

        if not valid_packs:
            item = QListWidgetItem()
            widget = _EmptyPackCard(
                "No hay packs cargados",
                "Cuando existan packs conversacionales disponibles, podras lanzarlos desde aqui.",
            )
            item.setFlags(Qt.NoItemFlags)
            item.setSizeHint(widget.sizeHint())
            self._packs.addItem(item)
            self._packs.setItemWidget(item, widget)
            self._update_availability()
            return

        for pack in valid_packs:
            item = QListWidgetItem()
            item.setData(Qt.UserRole, str(pack.get("id") or "").strip())
            widget = _PackCard(pack)
            item.setSizeHint(widget.sizeHint())
            self._packs.addItem(item)
            self._packs.setItemWidget(item, widget)

        self._update_availability()

    def set_status(self, text: str) -> None:
        self._status.setText(str(text or "").strip() or " ")

    def _emit_pack_selected(self, item: QListWidgetItem) -> None:
        pack_id = str(item.data(Qt.UserRole) or "").strip()
        if pack_id:
            self.packSelected.emit(pack_id)

    def _update_availability(self) -> None:
        ai_enabled = self._has_thread
        packs_enabled = self._has_thread and self._healthy_account and self._pack_count > 0
        self._ai_button.setEnabled(ai_enabled)
        self._packs.setEnabled(packs_enabled)


def _health_label(state: str) -> str:
    return {
        "login_required": "requiere login",
        "checkpoint": "checkpoint",
        "suspended": "suspendida",
        "banned": "bloqueada",
        "proxy_error": "error de proxy",
        "unknown": "desconocida",
    }.get(str(state or "").strip().lower(), "desconocida")
