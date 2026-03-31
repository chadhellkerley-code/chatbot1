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

        steps = QLabel(f"{action_count} pasos  |  click para enviar")
        steps.setObjectName("InboxPackSteps")
        steps.setWordWrap(True)
        layout.addWidget(steps)


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

<<<<<<< HEAD
        self._classification_badge = QLabel("Sin clasificar")
        self._classification_badge.setObjectName("InboxStateBadge")
        badges_row.addWidget(self._classification_badge, 0, Qt.AlignLeft)

        self._owner_badge = QLabel("Sin control")
=======
        self._classification_badge = QLabel("Todas")
        self._classification_badge.setObjectName("InboxStateBadge")
        badges_row.addWidget(self._classification_badge, 0, Qt.AlignLeft)

        self._owner_badge = QLabel("Sin owner")
>>>>>>> origin/main
        self._owner_badge.setObjectName("InboxMetaChip")
        badges_row.addWidget(self._owner_badge, 0, Qt.AlignLeft)

        badges_row.addStretch(1)
        hero_layout.addLayout(badges_row)
        canvas_layout.addWidget(hero)

        actions_card, actions_layout = _build_section(
            "Acciones del thread",
<<<<<<< HEAD
            "Clasificacion, control manual/automatico y atajos del thread actual.",
=======
            "Clasificacion, ownership y atajos manuales del thread actual.",
>>>>>>> origin/main
        )
        self._status = QLabel("Selecciona una conversacion para habilitar acciones.")
        self._status.setObjectName("InboxMutedText")
        self._status.setWordWrap(True)
        actions_layout.addWidget(self._status)

        classify_grid = QGridLayout()
        classify_grid.setContentsMargins(0, 0, 0, 0)
        classify_grid.setHorizontalSpacing(8)
        classify_grid.setVerticalSpacing(8)

<<<<<<< HEAD
        self._qualify_button = QPushButton("Tomar manual y calificar")
=======
        self._qualify_button = QPushButton("Pasar a Agendar / Calificadas")
>>>>>>> origin/main
        self._qualify_button.setObjectName("InboxPrimaryButton")
        self._qualify_button.clicked.connect(self.markQualifiedRequested.emit)
        classify_grid.addWidget(self._qualify_button, 0, 0, 1, 2)

<<<<<<< HEAD
        self._disqualify_button = QPushButton("Descalificar")
=======
        self._disqualify_button = QPushButton("Pasar a Descalificadas")
>>>>>>> origin/main
        self._disqualify_button.setObjectName("InboxMiniAction")
        self._disqualify_button.clicked.connect(self.markDisqualifiedRequested.emit)
        classify_grid.addWidget(self._disqualify_button, 1, 0)

<<<<<<< HEAD
        self._clear_bucket_button = QPushButton("Restaurar a automatico")
=======
        self._clear_bucket_button = QPushButton("Volver a Todas")
>>>>>>> origin/main
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

        packs_card, packs_layout = _build_section(
            "Packs",
            "Lista scrollable de packs disponibles para disparar desde el thread seleccionado.",
        )

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

<<<<<<< HEAD
        self._detail_overview = QLabel("Alias, stage, control y ultima actividad apareceran aqui.")
=======
        self._detail_overview = QLabel("Alias, stage, owner y ultima actividad apareceran aqui.")
>>>>>>> origin/main
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
<<<<<<< HEAD
                ("owner", "Control"),
                ("bucket", "Clasificacion"),
=======
                ("owner", "Owner"),
                ("bucket", "Bucket"),
>>>>>>> origin/main
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
        canvas_layout.addStretch(1)

        self._reset_thread_details()
        self._update_availability()

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
        self._has_thread = bool(thread)
        self._healthy_account = True
        self._can_send_pack = False
        self._can_request_ai = False
        self._thread_permissions = dict(permissions or {})
        self._current_bucket = "all"
        self._current_owner = "none"
        self._current_suggestion = ""
        if not thread:
            self._thread_badge.setText("Sin thread activo")
<<<<<<< HEAD
            self._classification_badge.setText("Sin clasificar")
            self._owner_badge.setText("Sin control")
=======
            self._classification_badge.setText("Todas")
            self._owner_badge.setText("Sin owner")
>>>>>>> origin/main
            self._reset_thread_details()
            self._update_suggestion_preview(None)
            self._update_availability()
            return

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
<<<<<<< HEAD
        truth_payload = dict(truth or {})
        truth_label = str(truth_payload.get("label") or "").strip()
        truth_detail = str(truth_payload.get("detail") or "").strip()
        alias_note = str(truth_payload.get("alias_note") or "").strip()

        source_label = f"Alias @{alias_id}" if alias_id else f"Cuenta @{account_id}"
        self._thread_badge.setText(f"Alias thread @{alias_id}" if alias_id else f"Cuenta thread @{account_id}")
=======

        source_label = f"Alias @{alias_id}" if alias_id else f"Cuenta @{account_id}"
        self._thread_badge.setText(source_label)
>>>>>>> origin/main
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
<<<<<<< HEAD
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
=======
        self._detail_values["alias"].setText(f"@{alias_id}" if alias_id else f"@{account_id}")
        self._detail_values["stage"].setText(_text_value(thread.get("stage_id") or thread.get("stage"), fallback="Sin etapa"))
        self._detail_values["owner"].setText(_text_value(thread.get("owner"), fallback="Sin owner"))
        self._detail_values["bucket"].setText(_text_value(thread.get("bucket"), fallback="Sin bucket"))
>>>>>>> origin/main
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
        self._packs.clear()
        self._pack_count = 0

        valid_packs = [pack for pack in packs if isinstance(pack, dict)]
        self._pack_count = len(valid_packs)
        self._packs_summary.setText(f"{self._pack_count} disponibles")

        if not valid_packs:
            item = QListWidgetItem()
            widget = _EmptyPackCard(
                "No hay packs cargados",
                "Cuando existan packs conversacionales disponibles, se podran disparar desde aqui.",
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
<<<<<<< HEAD
        self._detail_overview.setText("Alias, stage, control y ultima actividad apareceran aqui.")
=======
        self._detail_overview.setText("Alias, stage, owner y ultima actividad apareceran aqui.")
>>>>>>> origin/main
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
<<<<<<< HEAD
        "qualified": "Calificada",
        "disqualified": "Descalificada",
        "all": "Sin clasificar",
    }.get(str(value or "").strip().lower(), "Sin clasificar")
=======
        "qualified": "Agendar / Calificadas",
        "disqualified": "Descalificadas",
        "all": "Todas",
    }.get(str(value or "").strip().lower(), "Todas")
>>>>>>> origin/main


def _owner_label(value: str) -> str:
    return {
<<<<<<< HEAD
        "manual": "Control manual",
        "auto": "Control automatico",
        "none": "Sin control",
    }.get(str(value or "").strip().lower(), "Sin control")
=======
        "manual": "Owner manual",
        "auto": "Owner auto",
        "none": "Sin owner",
    }.get(str(value or "").strip().lower(), "Sin owner")
>>>>>>> origin/main
