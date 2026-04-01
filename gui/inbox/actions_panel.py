from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)


class _PackCard(QFrame):
    def __init__(self, pack: dict[str, Any]) -> None:
        super().__init__()
        self.setObjectName("InboxPackCard")
        self.setCursor(Qt.PointingHandCursor)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(6)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)

        self._name_label = QLabel("")  # UI: keep a dedicated name label so pack updates can rewrite the title in place
        self._name_label.setObjectName("InboxPackName")  # UI: preserve the existing pack title styling for active and inactive cards
        self._name_label.setWordWrap(True)  # UI: allow long pack titles to wrap cleanly inside the card
        top_row.addWidget(self._name_label, 1)  # UI: render the normalized pack title inside the card header

        self._badge_label = QLabel("")  # UI: keep a dedicated type badge so incremental pack updates can refresh it in place
        self._badge_label.setObjectName("InboxPackBadge")  # UI: preserve the existing type badge styling during in-place updates
        top_row.addWidget(self._badge_label, 0, Qt.AlignRight | Qt.AlignTop)  # UI: render the reusable type badge inside the pack card header
        layout.addLayout(top_row)

        self._steps_label = QLabel("")  # UI: keep a dedicated steps label so pack updates can refresh counts in place
        self._steps_label.setObjectName("InboxPackSteps")  # UI: preserve the existing pack steps styling during in-place updates
        self._steps_label.setWordWrap(True)  # UI: allow refreshed pack step counts to wrap cleanly inside the card
        layout.addWidget(self._steps_label)  # UI: render the reusable steps label inside the card body
        self._active_state = True  # UI: track the applied active state so in-place updates can toggle styling only when needed
        self.update_pack(pack)  # UI: initialize the card through the same in-place update path used by incremental pack diffs

    def update_pack(self, pack: dict[str, Any]) -> None:  # UI: update pack card fields in place during incremental diff
        is_active = _pack_is_active(pack)  # UI: resolve the latest activity flag before mutating the existing card
        pack_name = str(pack.get("name") or "Pack").strip() or "Pack"  # UI: normalize the updated pack title before applying it to the card
        pack_type = str(pack.get("type") or "pack").strip().upper() or "PACK"  # UI: normalize the updated pack type before refreshing the badge in place
        if not is_active:  # UI: suffix inactive pack titles so disabled cards remain readable without relying only on color
            pack_name = f"{pack_name} (inactivo)"  # UI: expose the inactive state directly in the visible pack title
        self._name_label.setText(pack_name)  # UI: update pack card fields in place during incremental diff
        self._badge_label.setText(pack_type)  # UI: update pack card fields in place during incremental diff
        actions = pack.get("actions")  # UI: read the updated pack actions so the existing card can refresh its step count
        action_count = len(actions) if isinstance(actions, list) else 0  # UI: normalize the updated step count before refreshing the card copy
        self._steps_label.setText(f"{action_count} pasos  |  click para enviar")  # UI: update pack card fields in place during incremental diff
        if is_active != self._active_state:  # UI: only restyle the card when its active state actually changes
            if is_active:  # UI: restore the normal card presentation when an inactive pack becomes active again
                self.setStyleSheet("")  # UI: clear the muted pack styling when the card returns to an active state
            else:  # UI: apply the muted styling only when the pack transitions into an inactive state
                self.setStyleSheet("QFrame#InboxPackCard { background-color: #1a2230; border: 1px solid #2c384d; } QLabel { color: #7d8798; }")  # UI: mute inactive pack cards without removing them from the list
        self.setEnabled(is_active)  # UI: update pack card fields in place during incremental diff
        self._active_state = is_active  # UI: remember the applied activity state for the next in-place pack update


class _EmptyPackCard(QFrame):
    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__()
        self.setObjectName("InboxEmptyStateCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("InboxEmptyStateTitle")

        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("InboxEmptyStateText")
        subtitle_label.setWordWrap(True)

        layout.addWidget(title_label)
        layout.addWidget(subtitle_label)


class _DetailItem(QFrame):
    def __init__(self, label: str) -> None:
        super().__init__()
        self.setObjectName("InboxDetailItem")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(2)

        label_widget = QLabel(label)
        label_widget.setObjectName("InboxDetailLabel")
        layout.addWidget(label_widget)

        self.value = QLabel("-")
        self.value.setObjectName("InboxDetailValue")
        self.value.setWordWrap(True)
        layout.addWidget(self.value)


def _build_section(title: str, subtitle: str) -> tuple[QFrame, QVBoxLayout]:
    card = QFrame()
    card.setObjectName("InboxDrawerSection")

    layout = QVBoxLayout(card)
    layout.setContentsMargins(14, 14, 14, 14)
    layout.setSpacing(10)

    title_widget = QLabel(title)
    title_widget.setObjectName("InboxSectionTitle")
    layout.addWidget(title_widget)

    subtitle_widget = QLabel(subtitle)
    subtitle_widget.setObjectName("InboxSectionSubtitle")
    subtitle_widget.setWordWrap(True)
    layout.addWidget(subtitle_widget)
    return card, layout


class ActionsPanel(QWidget):
    packSelected = Signal(str)
    aiSuggestionRequested = Signal()
    suggestionInsertRequested = Signal(str)
    addTagRequested = Signal()
    markFollowUpRequested = Signal()
    manualTakeoverRequested = Signal()
    manualReleaseRequested = Signal()
    markQualifiedRequested = Signal()
    markDisqualifiedRequested = Signal()
    clearClassificationRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._has_thread = False
        self._pack_count = 0
        self._healthy_account = True
        self._can_send_pack = False
        self._can_request_ai = False
        self._thread_permissions: dict[str, Any] = {}
        self._current_bucket = "all"
        self._current_owner = "none"
        self._current_suggestion = ""
        self._pack_item_cache: dict[str, QListWidgetItem] = {}  # UI: keyed by pack_id, stores existing list items for diffing
        self._empty_pack_item: QListWidgetItem | None = None  # UI: keep track of the temporary empty-state card without clearing the full pack list

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._scroll = QScrollArea()
        self._scroll.setObjectName("InboxActionsScroll")
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        canvas = QWidget()
        canvas.setObjectName("InboxActionsCanvas")
        canvas_layout = QVBoxLayout(canvas)
        canvas_layout.setContentsMargins(0, 0, 0, 0)
        canvas_layout.setSpacing(12)
        self._scroll.setWidget(canvas)
        root.addWidget(self._scroll)

        hero = QFrame()
        hero.setObjectName("InboxSubtleCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(14, 14, 14, 14)
        hero_layout.setSpacing(8)

        hero_title = QLabel("Thread activo")
        hero_title.setObjectName("InboxFieldLabel")
        hero_layout.addWidget(hero_title)

        self._lead_title = QLabel("Selecciona una conversacion")
        self._lead_title.setObjectName("InboxLeadTitle")
        hero_layout.addWidget(self._lead_title)

        self._lead_subtitle = QLabel("La metadata operativa se mostrara aqui con espacio real y lectura clara.")
        self._lead_subtitle.setObjectName("InboxLeadSubtitle")
        self._lead_subtitle.setWordWrap(True)
        hero_layout.addWidget(self._lead_subtitle)

        badges_row = QHBoxLayout()
        badges_row.setContentsMargins(0, 0, 0, 0)
        badges_row.setSpacing(8)

        self._thread_badge = QLabel("Sin thread activo")
        self._thread_badge.setObjectName("InboxThreadBadge")
        badges_row.addWidget(self._thread_badge, 0, Qt.AlignLeft)

        self._classification_badge = QLabel("Sin clasificar")
        self._classification_badge.setObjectName("InboxStateBadge")
        badges_row.addWidget(self._classification_badge, 0, Qt.AlignLeft)

        self._owner_badge = QLabel("Sin control")
        self._owner_badge.setObjectName("InboxMetaChip")
        badges_row.addWidget(self._owner_badge, 0, Qt.AlignLeft)

        badges_row.addStretch(1)
        hero_layout.addLayout(badges_row)
        canvas_layout.addWidget(hero)
        self._empty_thread_state = QWidget()  # UI: host a single centered prompt when no conversation is selected
        empty_state_layout = QVBoxLayout(self._empty_thread_state)  # UI: center the no-thread prompt within the available drawer space
        empty_state_layout.setContentsMargins(16, 24, 16, 24)  # UI: give the no-thread prompt breathing room inside the drawer
        empty_state_layout.addStretch(1)  # UI: push the no-thread prompt toward the vertical center of the drawer
        self._empty_thread_label = QLabel("Seleccioná una conversación")  # UI: show a single clear prompt instead of disabled section placeholders
        self._empty_thread_label.setObjectName("InboxSummaryText")  # UI: style the no-thread prompt like the inbox summary copy
        self._empty_thread_label.setAlignment(Qt.AlignCenter)  # UI: center the no-thread prompt horizontally for a cleaner empty state
        self._empty_thread_label.setWordWrap(True)  # UI: allow the no-thread prompt to wrap cleanly on narrow layouts
        empty_state_layout.addWidget(self._empty_thread_label, 0, Qt.AlignCenter)  # UI: render the no-thread prompt in the middle of the drawer
        empty_state_layout.addStretch(1)  # UI: keep the no-thread prompt vertically centered within the empty state container
        self._empty_thread_state.hide()  # UI: start with the no-thread prompt hidden until the drawer receives an empty selection
        canvas_layout.addWidget(self._empty_thread_state, 1)  # UI: reserve flexible space for the centered no-thread prompt below the hero card

        actions_card, actions_layout = _build_section(
            "Acciones del thread",
            "Clasificacion, control manual/automatico y atajos del thread actual.",
        )
        self._actions_card = actions_card  # UI: toggle the thread actions card off entirely when no conversation is selected
        self._status = QLabel("Selecciona una conversacion para habilitar acciones.")
        self._status.setObjectName("InboxMutedText")
        self._status.setWordWrap(True)
        actions_layout.addWidget(self._status)

        classify_grid = QGridLayout()
        classify_grid.setContentsMargins(0, 0, 0, 0)
        classify_grid.setHorizontalSpacing(8)
        classify_grid.setVerticalSpacing(8)

        self._qualify_button = QPushButton("Tomar manual y calificar")
        self._qualify_button.setObjectName("InboxPrimaryButton")
        self._qualify_button.clicked.connect(self.markQualifiedRequested.emit)
        classify_grid.addWidget(self._qualify_button, 0, 0, 1, 2)

        self._disqualify_button = QPushButton("Descalificar")
        self._disqualify_button.setObjectName("InboxMiniAction")
        self._disqualify_button.clicked.connect(self.markDisqualifiedRequested.emit)
        classify_grid.addWidget(self._disqualify_button, 1, 0)

        self._clear_bucket_button = QPushButton("Restaurar a automatico")
        self._clear_bucket_button.setObjectName("InboxMiniAction")
        self._clear_bucket_button.clicked.connect(self.clearClassificationRequested.emit)
        classify_grid.addWidget(self._clear_bucket_button, 1, 1)
        actions_layout.addLayout(classify_grid)

        manual_label = QLabel("Acciones manuales existentes")
        manual_label.setObjectName("InboxDetailLabel")
        actions_layout.addWidget(manual_label)

        manual_grid = QGridLayout()
        manual_grid.setContentsMargins(0, 0, 0, 0)
        manual_grid.setHorizontalSpacing(8)
        manual_grid.setVerticalSpacing(8)

        self._tag_button = QPushButton("Etiqueta")
        self._tag_button.setObjectName("InboxMiniAction")
        self._tag_button.clicked.connect(self.addTagRequested.emit)
        manual_grid.addWidget(self._tag_button, 0, 0)

        self._follow_up_button = QPushButton("Seguimiento")
        self._follow_up_button.setObjectName("InboxMiniAction")
        self._follow_up_button.clicked.connect(self.markFollowUpRequested.emit)
        manual_grid.addWidget(self._follow_up_button, 0, 1)

        self._takeover_button = QPushButton("Tomar manual")
        self._takeover_button.setObjectName("InboxMiniAction")
        self._takeover_button.clicked.connect(self.manualTakeoverRequested.emit)
        manual_grid.addWidget(self._takeover_button, 1, 0)

        self._release_button = QPushButton("Devolver a auto")
        self._release_button.setObjectName("InboxMiniAction")
        self._release_button.clicked.connect(self.manualReleaseRequested.emit)
        manual_grid.addWidget(self._release_button, 1, 1)
        actions_layout.addLayout(manual_grid)
        canvas_layout.addWidget(actions_card)

        ai_card, ai_layout = _build_section(
            "Respuestas sugeridas",
            "Vista legible de la sugerencia IA con accion rapida para llevarla al composer.",
        )

        ai_actions = QHBoxLayout()
        ai_actions.setContentsMargins(0, 0, 0, 0)
        ai_actions.setSpacing(8)

        self._ai_button = QPushButton("Generar sugerencia")
        self._ai_button.setObjectName("InboxPrimaryButton")
        self._ai_button.clicked.connect(self.aiSuggestionRequested.emit)
        ai_actions.addWidget(self._ai_button, 1)

        self._insert_suggestion_button = QPushButton("Insertar en chat")
        self._insert_suggestion_button.setObjectName("InboxMiniAction")
        self._insert_suggestion_button.clicked.connect(self._emit_suggestion_insert)
        ai_actions.addWidget(self._insert_suggestion_button, 0)
        ai_layout.addLayout(ai_actions)

        self._suggestion_meta = QLabel("Todavia no hay una sugerencia lista para este thread.")
        self._suggestion_meta.setObjectName("InboxMutedText")
        self._suggestion_meta.setWordWrap(True)
        ai_layout.addWidget(self._suggestion_meta)

        self._suggestion_preview = QPlainTextEdit()
        self._suggestion_preview.setObjectName("InboxSuggestionPreview")
        self._suggestion_preview.setReadOnly(True)
        self._suggestion_preview.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self._suggestion_preview.setPlaceholderText("La sugerencia aparecera aqui cuando la IA la genere.")
        self._suggestion_preview.setMinimumHeight(144)
        self._suggestion_preview.setMaximumHeight(240)
        ai_layout.addWidget(self._suggestion_preview)
        canvas_layout.addWidget(ai_card)
        self._ai_card = ai_card  # UI: toggle the AI card off entirely when no conversation is selected

        packs_card, packs_layout = _build_section(
            "Packs",
            "Lista scrollable de packs disponibles para disparar desde el thread seleccionado.",
        )
        self._packs_card = packs_card  # UI: toggle the packs card off entirely when no conversation is selected

        packs_title_row = QHBoxLayout()
        packs_title_row.setContentsMargins(0, 0, 0, 0)
        packs_title_row.setSpacing(8)

        self._packs_summary = QLabel("0 disponibles")
        self._packs_summary.setObjectName("InboxSummaryText")
        packs_title_row.addStretch(1)
        packs_title_row.addWidget(self._packs_summary, 0, Qt.AlignRight)
        packs_layout.addLayout(packs_title_row)

        self._packs = QListWidget()
        self._packs.setObjectName("InboxPackList")
        self._packs.setFrameShape(QFrame.NoFrame)
        self._packs.setSpacing(8)
        self._packs.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._packs.setMinimumHeight(220)
        self._packs.setMaximumHeight(320)
        self._packs.viewport().setObjectName("InboxPackViewport")
        self._packs.itemClicked.connect(self._emit_pack_selected)
        packs_layout.addWidget(self._packs)
        canvas_layout.addWidget(packs_card)

        detail_card, detail_layout = _build_section(
            "Detalle del lead/thread",
            "Resumen compacto del estado comercial y operativo del lead.",
        )

        self._detail_overview = QLabel("Alias, stage, control y ultima actividad apareceran aqui.")
        self._detail_overview.setObjectName("InboxLeadSubtitle")
        self._detail_overview.setWordWrap(True)
        detail_layout.addWidget(self._detail_overview)

        details_grid = QGridLayout()
        details_grid.setContentsMargins(0, 0, 0, 0)
        details_grid.setHorizontalSpacing(8)
        details_grid.setVerticalSpacing(8)
        self._detail_values: dict[str, QLabel] = {}
        for index, key_label in enumerate(
            (
                ("alias", "Alias"),
                ("stage", "Stage"),
                ("owner", "Control"),
                ("bucket", "Clasificacion"),
                ("last_activity", "Ultima actividad"),
                ("quality", "Quality"),
                ("last_action", "Ultima accion"),
                ("last_pack", "Ultimo pack"),
            )
        ):
            key, label = key_label
            item = _DetailItem(label)
            self._detail_values[key] = item.value
            details_grid.addWidget(item, index // 2, index % 2)
        detail_layout.addLayout(details_grid)

        self._tags_value = QLabel("Tags: Sin tags")
        self._tags_value.setObjectName("InboxMutedText")
        self._tags_value.setWordWrap(True)
        detail_layout.addWidget(self._tags_value)
        canvas_layout.addWidget(detail_card)
        self._detail_card = detail_card  # UI: toggle the detail card off entirely when no conversation is selected
        canvas_layout.addStretch(1)

        self._reset_thread_details()
        self._update_availability()
        self._apply_empty_state()  # UI: start the drawer in its dedicated no-thread layout until a conversation is selected

    def set_thread(
        self,
        thread: dict[str, Any] | None,
        *,
        permissions: dict[str, Any] | None = None,
        truth: dict[str, Any] | None = None,
    ) -> None:
        self._has_thread = bool(thread)  # UI: track whether the drawer should render active controls or the no-thread prompt
        self._healthy_account = True
        self._can_send_pack = False
        self._can_request_ai = False
        self._thread_permissions = dict(permissions or {})
        self._current_bucket = "all"
        self._current_owner = "none"
        self._current_suggestion = ""
        if not thread:
            self._has_thread = False  # UI: treat missing or empty thread payloads as the empty drawer state
            self._thread_badge.setText("Sin thread activo")
            self._classification_badge.setText("Sin clasificar")
            self._owner_badge.setText("Sin control")
            self._reset_thread_details()
            self._update_suggestion_preview(None)
            self._update_availability()
            self._apply_empty_state()  # UI: collapse the drawer sections into a single no-thread prompt
            return

        self._has_thread = True  # UI: mark the drawer as active when a real thread payload is present
        self._apply_active_state()  # UI: restore the full drawer cards before rendering the selected thread details
        display_name = str(thread.get("display_name") or thread.get("recipient_username") or "Conversacion").strip()
        account_id = str(thread.get("account_id") or "-").strip() or "-"
        alias_id = str(thread.get("account_alias") or thread.get("alias_id") or "").strip()
        recipient = str(thread.get("recipient_username") or "-").strip() or "-"
        health = str(thread.get("account_health") or "healthy").strip().lower()
        owner = str(thread.get("owner") or "none").strip().lower() or "none"
        bucket = str(thread.get("bucket") or "all").strip().lower() or "all"
        self._healthy_account = health == "healthy"
        self._can_send_pack = bool(self._thread_permissions.get("can_send_pack", self._healthy_account))
        self._can_request_ai = bool(self._thread_permissions.get("can_request_ai", self._has_thread))
        self._current_bucket = bucket
        self._current_owner = owner
        truth_payload = dict(truth or {})
        truth_label = str(truth_payload.get("label") or "").strip()
        truth_detail = str(truth_payload.get("detail") or "").strip()
        alias_note = str(truth_payload.get("alias_note") or "").strip()

        source_label = f"Alias @{alias_id}" if alias_id else f"Cuenta @{account_id}"
        self._thread_badge.setText(f"Alias thread @{alias_id}" if alias_id else f"Cuenta thread @{account_id}")
        self._classification_badge.setText(_bucket_label(bucket))
        self._owner_badge.setText(_owner_label(owner))
        self._lead_title.setText(display_name)
        self._lead_subtitle.setText(f"@{recipient}  |  {source_label}")

        self._detail_overview.setText(
            "  |  ".join(
                part
                for part in (
                    f"Lead @{recipient}",
                    f"Alias @{alias_id}" if alias_id else f"Cuenta @{account_id}",
                    _text_value(thread.get("stage_id") or thread.get("stage"), fallback="Sin etapa"),
                )
                if part
            )
        )
        if truth_label or truth_detail or alias_note:
            self._detail_overview.setText(
                "  |  ".join(
                    part
                    for part in (self._detail_overview.text(), truth_label, truth_detail, alias_note)
                    if part
                )
            )
        self._detail_values["alias"].setText(f"@{alias_id}" if alias_id else f"@{account_id}")
        self._detail_values["stage"].setText(_text_value(thread.get("stage_id") or thread.get("stage"), fallback="Sin etapa"))
        self._detail_values["owner"].setText(_owner_label(thread.get("owner")))
        self._detail_values["bucket"].setText(_bucket_label(thread.get("bucket")))
        self._detail_values["last_activity"].setText(_last_activity_value(thread))
        self._detail_values["quality"].setText(_text_value(thread.get("quality"), fallback="Sin quality"))
        self._detail_values["last_action"].setText(_text_value(thread.get("last_action_type"), fallback="Sin accion"))
        self._detail_values["last_pack"].setText(
            _text_value(thread.get("last_pack_sent") or thread.get("pack_name"), fallback="Sin pack")
        )

        tags = thread.get("tags")
        if isinstance(tags, (list, tuple, set)):
            resolved_tags = ", ".join(str(tag).strip() for tag in tags if str(tag).strip())
        else:
            resolved_tags = str(tags or "").strip()
        self._tags_value.setText(f"Tags: {resolved_tags or 'Sin tags'}")

        self._update_suggestion_preview(thread)
        self._update_availability()

    def set_packs(self, packs: list[dict[str, Any]]) -> None:
        self._pack_count = 0  # UI: recompute the visible pack count from the incremental diff input each time packs refresh
        valid_packs = [pack for pack in packs if isinstance(pack, dict)]  # UI: limit the incremental diff to well-formed pack payloads
        incoming = {str(pack.get("pack_id") or pack.get("id") or "").strip(): pack for pack in valid_packs if str(pack.get("pack_id") or pack.get("id") or "").strip()}  # UI: key incoming packs by stable pack id for incremental diffing
        self._pack_count = len(incoming)  # UI: summarize only the diffable packs that have a stable identifier
        self._packs_summary.setText(f"{self._pack_count} disponibles")  # UI: refresh the pack count summary without rebuilding the full list
        default_flags = QListWidgetItem().flags()  # UI: preserve the platform-default item flags when active packs stay interactive
        if self._empty_pack_item is not None and self._packs.row(self._empty_pack_item) >= 0 and incoming:  # UI: hide the empty pack card as soon as real packs are available
            self._packs.takeItem(self._packs.row(self._empty_pack_item))  # UI: remove the temporary empty card without clearing the full pack list
            self._empty_pack_item = None  # UI: forget the empty pack item once the list contains real packs again
        for pack_id in list(self._pack_item_cache.keys()):  # UI: compare existing rendered packs against the new catalog without clearing the list
            if pack_id not in incoming:  # UI: remove stale pack cards without rebuilding the full list
                row = self._packs.row(self._pack_item_cache[pack_id])  # UI: look up the stale pack row before removing its existing item
                if row >= 0:  # UI: guard the stale-pack removal in case the item is already gone
                    self._packs.takeItem(row)  # UI: remove stale pack cards without rebuilding the full list
                del self._pack_item_cache[pack_id]  # UI: remove stale pack cards without rebuilding the full list
        for pack_id, pack in incoming.items():  # UI: walk the incoming pack catalog to update or insert cards incrementally
            if pack_id in self._pack_item_cache:  # UI: reuse rendered cards when the pack already exists in the list
                item = self._pack_item_cache[pack_id]  # UI: grab the existing list item so its card can be updated in place
                widget = self._packs.itemWidget(item)  # UI: resolve the current card widget for in-place pack updates
                item.setData(Qt.UserRole, pack_id)  # UI: keep the existing list item bound to its stable pack id during updates
                item.setFlags(default_flags if _pack_is_active(pack) else Qt.NoItemFlags)  # UI: refresh item interactivity when a pack toggles active state
                if widget is not None and hasattr(widget, "update_pack"):  # UI: update existing pack cards only when the rendered widget supports in-place refresh
                    widget.update_pack(pack)  # UI: update existing pack card in place to avoid full list rebuild
                    item.setSizeHint(widget.sizeHint())  # UI: refresh the cached size hint after an in-place pack card update
            else:
                item = QListWidgetItem()  # UI: allocate a new list item only for packs that are newly introduced by the diff
                item.setData(Qt.UserRole, pack_id)  # UI: bind the new list item to its stable pack id for future incremental diffs
                item.setFlags(default_flags if _pack_is_active(pack) else Qt.NoItemFlags)  # UI: make new inactive packs visible but non-interactive from the start
                card = _PackCard(pack)  # UI: create a card only for genuinely new packs instead of rebuilding the whole list
                item.setSizeHint(card.sizeHint())  # UI: size the newly inserted pack item from its freshly created card
                self._packs.addItem(item)  # UI: insert new pack card without clearing existing items
                self._packs.setItemWidget(item, card)  # UI: insert new pack card without clearing existing items
                self._pack_item_cache[pack_id] = item  # UI: remember the new list item for future incremental diffs
        if not incoming:  # UI: fall back to the empty-state card only when the incremental diff leaves no packs to render
            if self._empty_pack_item is None or self._packs.row(self._empty_pack_item) < 0:  # UI: avoid recreating the empty pack card when it is already visible
                self._empty_pack_item = QListWidgetItem()  # UI: allocate the empty-state item only when no real packs remain in the list
                widget = _EmptyPackCard(  # UI: reuse the existing empty-state card copy when the pack catalog is empty
                    "No hay packs cargados",  # UI: preserve the existing empty-state title for an empty pack catalog
                    "Cuando existan packs conversacionales disponibles, se podran disparar desde aqui.",  # UI: preserve the existing empty-state subtitle for an empty pack catalog
                )
                self._empty_pack_item.setFlags(Qt.NoItemFlags)  # UI: keep the empty-state card non-interactive while no packs exist
                self._empty_pack_item.setSizeHint(widget.sizeHint())  # UI: size the empty-state item from its card widget
                self._packs.addItem(self._empty_pack_item)  # UI: show the empty-state card without clearing the list widget
                self._packs.setItemWidget(self._empty_pack_item, widget)  # UI: attach the empty-state card widget only when the pack catalog is empty
        elif self._empty_pack_item is not None and self._packs.row(self._empty_pack_item) >= 0:  # UI: remove the empty-state card once real packs exist again
            self._packs.takeItem(self._packs.row(self._empty_pack_item))  # UI: hide the empty pack card without rebuilding the full list
            self._empty_pack_item = None  # UI: forget the empty-state item once the pack catalog is populated
        self._update_availability()  # UI: refresh pack-related button states after the incremental diff settles

    def set_status(self, text: str) -> None:
        self._status.setText(str(text or "").strip() or " ")

    def _apply_empty_state(self) -> None:  # UI: collapse the drawer to a single prompt when there is no active conversation
        self._actions_card.hide()  # UI: remove the thread actions card from view when no conversation is selected
        self._ai_card.hide()  # UI: remove the AI card from view when no conversation is selected
        self._packs_card.hide()  # UI: remove the packs card from view when no conversation is selected
        self._detail_card.hide()  # UI: remove the detail card from view when no conversation is selected
        self._empty_thread_state.show()  # UI: show the dedicated centered prompt for the empty drawer state

    def _apply_active_state(self) -> None:  # UI: restore the full drawer layout when a conversation becomes active
        self._actions_card.show()  # UI: restore the thread actions card when a conversation is selected
        self._ai_card.show()  # UI: restore the AI card when a conversation is selected
        self._packs_card.show()  # UI: restore the packs card when a conversation is selected
        self._detail_card.show()  # UI: restore the detail card when a conversation is selected
        self._empty_thread_state.hide()  # UI: hide the empty-state prompt once real thread data is available

    def _emit_pack_selected(self, item: QListWidgetItem) -> None:
        if not self._packs.isEnabled():
            return
        pack_id = str(item.data(Qt.UserRole) or "").strip()
        if pack_id:
            self.packSelected.emit(pack_id)

    def _emit_suggestion_insert(self) -> None:
        if self._insert_suggestion_button.isEnabled() and self._current_suggestion:
            self.suggestionInsertRequested.emit(self._current_suggestion)

    def _reset_thread_details(self) -> None:
        self._lead_title.setText("Selecciona una conversacion")
        self._lead_subtitle.setText("La metadata operativa se mostrara aqui con espacio real y lectura clara.")
        self._detail_overview.setText("Alias, stage, control y ultima actividad apareceran aqui.")
        for value in self._detail_values.values():
            value.setText("-")
        self._tags_value.setText("Tags: Sin tags")

    def _update_suggestion_preview(self, thread: dict[str, Any] | None) -> None:
        suggestion = str((thread or {}).get("suggested_reply") or "").strip()
        self._current_suggestion = suggestion
        status = str((thread or {}).get("suggestion_status") or "").strip().lower()
        error = str((thread or {}).get("suggestion_error") or "").strip()
        generated_at = _format_datetime((thread or {}).get("suggested_reply_at"))

        if suggestion:
            self._suggestion_meta.setText(
                f"Sugerencia lista{f'  |  generada {generated_at}' if generated_at else ''}."
            )
            self._suggestion_preview.setPlainText(suggestion)
            self._ai_button.setText("Actualizar sugerencia")
            return

        self._suggestion_preview.clear()
        if status == "queued":
            self._suggestion_meta.setText("Generando sugerencia IA para este thread...")
        elif status == "failed":
            self._suggestion_meta.setText(error or "No se pudo generar una sugerencia util.")
        else:
            self._suggestion_meta.setText("Todavia no hay una sugerencia lista para este thread.")
        self._ai_button.setText("Generar sugerencia")

    def _update_availability(self) -> None:
        ai_enabled = self._has_thread and self._can_request_ai
        packs_enabled = self._has_thread and self._healthy_account and self._can_send_pack and self._pack_count > 0
        can_mark_qualified = self._has_thread and bool(
            self._thread_permissions.get(
                "can_mark_qualified",
                not (self._current_bucket == "qualified" and self._current_owner == "manual"),
            )
        )
        can_mark_disqualified = self._has_thread and bool(
            self._thread_permissions.get("can_mark_disqualified", self._current_bucket != "disqualified")
        )
        can_clear_classification = self._has_thread and bool(
            self._thread_permissions.get(
                "can_clear_classification",
                self._current_bucket != "all" or self._current_owner == "manual",
            )
        )

        self._ai_button.setEnabled(ai_enabled)
        self._insert_suggestion_button.setEnabled(bool(self._has_thread and self._current_suggestion))
        self._packs.setEnabled(packs_enabled)
        self._qualify_button.setEnabled(can_mark_qualified)
        self._disqualify_button.setEnabled(can_mark_disqualified)
        self._clear_bucket_button.setEnabled(can_clear_classification)
        self._tag_button.setEnabled(self._has_thread and bool(self._thread_permissions.get("can_add_tag", self._has_thread)))
        self._follow_up_button.setEnabled(
            self._has_thread and bool(self._thread_permissions.get("can_mark_follow_up", self._has_thread))
        )
        self._takeover_button.setEnabled(
            self._has_thread and bool(self._thread_permissions.get("can_takeover_manual", False))
        )
        self._release_button.setEnabled(
            self._has_thread and bool(self._thread_permissions.get("can_release_manual", False))
        )


def _pack_is_active(pack: dict[str, Any]) -> bool:  # UI: normalize pack activity flags so inactive packs stay visible but disabled
    for key in ("active", "is_active", "enabled"):  # UI: honor whichever pack activity flag the payload provides
        if key in pack:  # UI: stop at the first explicit activity field found on the pack payload
            return bool(pack.get(key))  # UI: use the payload activity flag to decide whether the pack should render as disabled
    return True  # UI: default packs to active when no explicit activity field is provided


def _text_value(value: Any, *, fallback: str) -> str:
    clean = str(value or "").strip()
    return clean or fallback


def _last_activity_value(thread: dict[str, Any]) -> str:
    last_seen = str(thread.get("last_seen_text") or "").strip()
    if last_seen:
        return last_seen
    stamp = thread.get("last_activity_timestamp")
    if stamp in (None, "", 0):
        stamp = thread.get("last_message_timestamp")
    formatted = _format_datetime(stamp)
    return formatted or "Sin actividad"


def _format_datetime(value: Any) -> str:
    try:
        stamp = float(value)
    except Exception:
        return ""
    if stamp <= 0:
        return ""
    return datetime.fromtimestamp(stamp).strftime("%d/%m %H:%M")


def _bucket_label(value: str) -> str:
    return {
        "qualified": "Calificada",
        "disqualified": "Descalificada",
        "all": "Sin clasificar",
    }.get(str(value or "").strip().lower(), "Sin clasificar")


def _owner_label(value: str) -> str:
    return {
        "manual": "Control manual",
        "auto": "Control automatico",
        "none": "Sin control",
    }.get(str(value or "").strip().lower(), "Sin control")
