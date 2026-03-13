from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext
from gui.query_runner import QueryError
from gui.snapshot_queries import build_leads_filter_config_snapshot

from .common import (
    FILTER_STATE_ITEMS,
    IMAGE_INFO,
    LANGUAGE_ITEMS,
    LeadsModalDialog,
    LINK_ITEMS,
    PRIVACY_ITEMS,
    TEXT_INFO,
    filter_state_label,
    keywords_to_text,
    set_panel_status,
    show_panel_exception,
    text_to_keywords,
)


TEXT_THRESHOLD_FIELDS = [
    ("embeddings_threshold", "Umbral embeddings", "float", 0.01, 0.0, 1.0),
    ("hybrid_embeddings_weight", "Peso embeddings", "float", 0.01, 0.0, 1.0),
    ("regex_floor_threshold", "Regex floor", "float", 0.01, 0.0, 1.0),
    ("regex_ceiling_threshold", "Regex ceiling", "float", 0.01, 0.0, 1.0),
    ("regex_coverage_base", "Cobertura base", "float", 0.01, 0.0, 1.0),
    ("regex_coverage_per_term", "Cobertura por termino", "float", 0.01, 0.0, 1.0),
    ("regex_coverage_max_terms", "Max terminos", "int", 1, 1, 20),
]

IMAGE_THRESHOLD_FIELDS = [
    ("gender_prob_threshold", "Confianza genero", "float", 0.01, 0.0, 1.0),
    ("beard_threshold", "Umbral barba", "float", 0.01, 0.0, 1.0),
    ("overweight_threshold", "Umbral overweight", "float", 0.01, 0.0, 1.0),
    ("overweight_tolerance", "Tolerancia overweight", "float", 0.01, 0.0, 1.0),
    ("overweight_male35_threshold", "Overweight male35", "float", 0.01, 0.0, 1.0),
    ("slim_threshold", "Umbral slim", "float", 0.01, 0.0, 1.0),
    ("age_min_tolerance_years", "Tolerancia edad", "int", 1, 0, 8),
    ("age_min_tolerance_over30_prob", "Prob. over30", "float", 0.01, 0.0, 1.0),
    ("sharpness_threshold", "Nitidez", "float", 0.01, 0.0, 1.0),
]


class _ThresholdDialog(LeadsModalDialog):
    def __init__(
        self,
        title: str,
        fields: list[tuple[str, str, str, float, float, float]],
        payload: dict[str, Any],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, "Ajusta los umbrales avanzados del bloque seleccionado.", parent=parent)
        self.resize(520, 540)
        self._fields: dict[str, QWidget] = {}

        root = self.body_layout()

        scroll = QScrollArea()
        scroll.setObjectName("LeadsModalScroll")
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.viewport().setObjectName("LeadsModalScrollViewport")
        scroll.viewport().setAutoFillBackground(False)
        container = QFrame()
        container.setObjectName("LeadsModalSurface")
        layout = QGridLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(8)

        row_index = 0
        for key, label, kind, step, minimum, maximum in fields:
            field_label = QLabel(label)
            field_label.setObjectName("LeadsModalFieldLabel")
            layout.addWidget(field_label, row_index, 0)
            if kind == "int":
                widget = QSpinBox()
                widget.setRange(int(minimum), int(maximum))
                widget.setSingleStep(int(step))
                widget.setValue(int(payload.get(key) or 0))
            else:
                widget = QDoubleSpinBox()
                widget.setDecimals(3)
                widget.setRange(float(minimum), float(maximum))
                widget.setSingleStep(float(step))
                widget.setValue(float(payload.get(key) or 0.0))
            self._fields[key] = widget
            layout.addWidget(widget, row_index, 1)
            row_index += 1

        scroll.setWidget(container)
        root.addWidget(scroll, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        ok_button = buttons.button(QDialogButtonBox.Ok)
        if ok_button is not None:
            ok_button.setText("Guardar")
            ok_button.setObjectName("PrimaryButton")
        cancel_button = buttons.button(QDialogButtonBox.Cancel)
        if cancel_button is not None:
            cancel_button.setText("Cancelar")
            cancel_button.setObjectName("SecondaryButton")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, widget in self._fields.items():
            if isinstance(widget, QDoubleSpinBox):
                result[key] = float(widget.value())
            elif isinstance(widget, QSpinBox):
                result[key] = int(widget.value())
        return result


class _InfoDialog(LeadsModalDialog):
    def __init__(self, title: str, message: str, parent: QWidget | None = None) -> None:
        super().__init__(title, "", parent=parent)
        self.resize(460, 240)

        root = self.body_layout()

        title_label = QLabel(title)
        title_label.setObjectName("SendSetupSectionTitle")
        root.addWidget(title_label)

        message_label = QLabel(message)
        message_label.setObjectName("MutedText")
        message_label.setWordWrap(True)
        root.addWidget(message_label)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("PrimaryButton")
        close_button.clicked.connect(self.accept)
        root.addWidget(close_button, 0, Qt.AlignRight)


class LeadsFilterConfigPanel(QWidget):
    def __init__(
        self,
        ctx: PageContext,
        parent: QWidget | None = None,
        *,
        on_changed=None,
    ) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._on_changed = on_changed
        self._text_thresholds: dict[str, Any] = {}
        self._image_thresholds: dict[str, Any] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        self._summary = QLabel("")
        self._summary.setObjectName("MutedText")
        self._summary.setWordWrap(True)
        root.addWidget(self._summary)

        classic_card = QFrame()
        classic_card.setObjectName("SendSetupCard")
        classic_layout = QGridLayout(classic_card)
        classic_layout.setContentsMargins(16, 16, 16, 16)
        classic_layout.setHorizontalSpacing(8)
        classic_layout.setVerticalSpacing(8)

        self._min_followers = QSpinBox()
        self._min_followers.setRange(0, 10_000_000)
        self._min_followers_state = self._state_combo()
        self._min_posts = QSpinBox()
        self._min_posts.setRange(0, 100_000)
        self._min_posts_state = self._state_combo()
        self._privacy_value = self._choice_combo(PRIVACY_ITEMS)
        self._privacy_state = self._state_combo()
        self._link_value = self._choice_combo(LINK_ITEMS)
        self._link_state = self._state_combo()
        self._language_value = self._choice_combo(LANGUAGE_ITEMS)
        self._language_state = self._state_combo()
        self._include_keywords = QPlainTextEdit()
        self._include_keywords.setPlaceholderText("fitness\ncoach\nwellness")
        self._include_keywords_state = self._state_combo()
        self._exclude_keywords = QPlainTextEdit()
        self._exclude_keywords.setPlaceholderText("private\nminor")
        self._exclude_keywords_state = self._state_combo()

        classic_layout.addWidget(QLabel("Seguidores minimos"), 0, 0)
        classic_layout.addWidget(self._min_followers, 0, 1)
        classic_layout.addWidget(self._min_followers_state, 0, 2)
        classic_layout.addWidget(QLabel("Posts minimos"), 0, 3)
        classic_layout.addWidget(self._min_posts, 0, 4)
        classic_layout.addWidget(self._min_posts_state, 0, 5)

        classic_layout.addWidget(QLabel("Estado de cuenta"), 1, 0)
        classic_layout.addWidget(self._privacy_value, 1, 1)
        classic_layout.addWidget(self._privacy_state, 1, 2)
        classic_layout.addWidget(QLabel("Link en bio"), 1, 3)
        classic_layout.addWidget(self._link_value, 1, 4)
        classic_layout.addWidget(self._link_state, 1, 5)

        classic_layout.addWidget(QLabel("Idioma"), 2, 0)
        classic_layout.addWidget(self._language_value, 2, 1)
        classic_layout.addWidget(self._language_state, 2, 2)
        classic_layout.addWidget(QLabel("Keywords incluidas"), 3, 0, Qt.AlignTop)
        classic_layout.addWidget(self._include_keywords, 3, 1, 1, 2)
        classic_layout.addWidget(self._include_keywords_state, 3, 3)
        classic_layout.addWidget(QLabel("Keywords excluidas"), 3, 4, Qt.AlignTop)
        classic_layout.addWidget(self._exclude_keywords, 3, 5)
        classic_layout.addWidget(self._exclude_keywords_state, 4, 5)
        root.addWidget(classic_card)

        self._text_prompt = QPlainTextEdit()
        self._text_prompt.setPlaceholderText(
            "Describe el lead ideal con criterios claros, concretos y verificables."
        )
        self._text_state = self._state_combo()
        self._text_threshold_label = QLabel("")
        self._text_threshold_label.setObjectName("MutedText")
        self._text_threshold_label.setWordWrap(True)
        root.addWidget(
            self._ai_card(
                "Texto inteligente",
                self._text_prompt,
                self._text_state,
                self._text_threshold_label,
                info_text=TEXT_INFO,
                config_callback=self._configure_text_thresholds,
            )
        )

        self._image_prompt = QPlainTextEdit()
        self._image_prompt.setPlaceholderText(
            "Describe atributos visibles esperados en la foto de perfil."
        )
        self._image_state = self._state_combo()
        self._image_threshold_label = QLabel("")
        self._image_threshold_label.setObjectName("MutedText")
        self._image_threshold_label.setWordWrap(True)
        root.addWidget(
            self._ai_card(
                "Prompt visual",
                self._image_prompt,
                self._image_state,
                self._image_threshold_label,
                info_text=IMAGE_INFO,
                config_callback=self._configure_image_thresholds,
            )
        )

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        load_button = QPushButton("Cargar actual")
        load_button.setObjectName("SecondaryButton")
        load_button.clicked.connect(self.load_config)
        defaults_button = QPushButton("Valores por defecto")
        defaults_button.setObjectName("SecondaryButton")
        defaults_button.clicked.connect(self.load_defaults)
        save_button = QPushButton("Guardar configuracion")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.save_config)
        delete_button = QPushButton("Eliminar configuracion")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self.delete_config)
        actions.addWidget(load_button)
        actions.addWidget(defaults_button)
        actions.addWidget(save_button)
        actions.addWidget(delete_button)
        actions.addStretch(1)
        root.addLayout(actions)
        root.addStretch(1)

    def _state_combo(self) -> QComboBox:
        combo = QComboBox()
        for label, value in FILTER_STATE_ITEMS:
            combo.addItem(label, value)
        return combo

    def _choice_combo(self, items: list[tuple[str, Any]]) -> QComboBox:
        combo = QComboBox()
        for label, value in items:
            combo.addItem(label, value)
        return combo

    def _ai_card(
        self,
        title: str,
        prompt_editor: QPlainTextEdit,
        state_combo: QComboBox,
        threshold_label: QLabel,
        *,
        info_text: str,
        config_callback,
    ) -> QWidget:
        card = QFrame()
        card.setObjectName("SendSetupCard")
        layout = QGridLayout(card)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(8)

        title_label = QLabel(title)
        title_label.setObjectName("SendSetupSectionTitle")

        info_button = QToolButton()
        info_button.setObjectName("InfoIconButton")
        info_button.setText("i")
        info_button.clicked.connect(lambda: _InfoDialog(title, info_text, self).exec())

        config_button = QPushButton("Configuracion")
        config_button.setObjectName("SecondaryButton")
        config_button.clicked.connect(config_callback)

        layout.addWidget(title_label, 0, 0)
        layout.addWidget(info_button, 0, 1)
        layout.addWidget(config_button, 0, 3, Qt.AlignRight)
        layout.addWidget(QLabel("Estado"), 1, 0)
        layout.addWidget(state_combo, 1, 1)
        layout.addWidget(QLabel("Prompt"), 2, 0, Qt.AlignTop)
        layout.addWidget(prompt_editor, 2, 1, 1, 3)
        layout.addWidget(threshold_label, 3, 1, 1, 3)
        return card

    def _set_combo_value(self, combo: QComboBox, value: Any) -> None:
        index = combo.findData(value)
        combo.setCurrentIndex(max(0, index))

    def _payload_from_form(self) -> dict[str, Any]:
        return {
            "classic": {
                "min_followers": int(self._min_followers.value()),
                "min_posts": int(self._min_posts.value()),
                "privacy": str(self._privacy_value.currentData() or "any"),
                "link_in_bio": str(self._link_value.currentData() or "any"),
                "include_keywords": text_to_keywords(self._include_keywords.toPlainText()),
                "exclude_keywords": text_to_keywords(self._exclude_keywords.toPlainText()),
                "language": str(self._language_value.currentData() or "any"),
                "min_followers_state": str(self._min_followers_state.currentData() or ""),
                "min_posts_state": str(self._min_posts_state.currentData() or ""),
                "privacy_state": str(self._privacy_state.currentData() or ""),
                "link_in_bio_state": str(self._link_state.currentData() or ""),
                "include_keywords_state": str(self._include_keywords_state.currentData() or ""),
                "exclude_keywords_state": str(self._exclude_keywords_state.currentData() or ""),
                "language_state": str(self._language_state.currentData() or ""),
            },
            "text": {
                "enabled": str(self._text_state.currentData() or "") != "disabled",
                "criteria": str(self._text_prompt.toPlainText() or "").strip(),
                "model_path": "",
                "state": str(self._text_state.currentData() or ""),
                "engine_thresholds": dict(self._text_thresholds),
            },
            "image": {
                "enabled": str(self._image_state.currentData() or "") != "disabled",
                "prompt": str(self._image_prompt.toPlainText() or "").strip(),
                "state": str(self._image_state.currentData() or ""),
                "engine_thresholds": dict(self._image_thresholds),
            },
        }

    def current_payload(self) -> dict[str, Any]:
        return self._payload_from_form()

    def _apply_payload(self, payload: dict[str, Any]) -> None:
        classic = payload.get("classic") if isinstance(payload, dict) else {}
        text_payload = payload.get("text") if isinstance(payload, dict) else {}
        image_payload = payload.get("image") if isinstance(payload, dict) else {}
        if not isinstance(classic, dict):
            classic = {}
        if not isinstance(text_payload, dict):
            text_payload = {}
        if not isinstance(image_payload, dict):
            image_payload = {}

        self._min_followers.setValue(int(classic.get("min_followers") or 0))
        self._min_posts.setValue(int(classic.get("min_posts") or 0))
        self._set_combo_value(self._min_followers_state, classic.get("min_followers_state"))
        self._set_combo_value(self._min_posts_state, classic.get("min_posts_state"))
        self._set_combo_value(self._privacy_value, classic.get("privacy"))
        self._set_combo_value(self._privacy_state, classic.get("privacy_state"))
        self._set_combo_value(self._link_value, classic.get("link_in_bio"))
        self._set_combo_value(self._link_state, classic.get("link_in_bio_state"))
        self._set_combo_value(self._language_value, classic.get("language"))
        self._set_combo_value(self._language_state, classic.get("language_state"))
        self._include_keywords.setPlainText(keywords_to_text(classic.get("include_keywords")))
        self._set_combo_value(self._include_keywords_state, classic.get("include_keywords_state"))
        self._exclude_keywords.setPlainText(keywords_to_text(classic.get("exclude_keywords")))
        self._set_combo_value(self._exclude_keywords_state, classic.get("exclude_keywords_state"))

        self._text_prompt.setPlainText(str(text_payload.get("criteria") or ""))
        self._set_combo_value(self._text_state, text_payload.get("state"))
        self._text_thresholds = dict(text_payload.get("engine_thresholds") or {})

        self._image_prompt.setPlainText(str(image_payload.get("prompt") or ""))
        self._set_combo_value(self._image_state, image_payload.get("state"))
        self._image_thresholds = dict(image_payload.get("engine_thresholds") or {})
        self._refresh_summary()

    def _refresh_summary(self) -> None:
        text_threshold = float(self._text_thresholds.get("embeddings_threshold") or 0.0)
        image_gender_threshold = float(self._image_thresholds.get("gender_prob_threshold") or 0.0)
        self._text_threshold_label.setText(
            f"Estado: {filter_state_label(str(self._text_state.currentData() or ''))}  |  "
            f"Umbral principal: {text_threshold:.2f}"
        )
        self._image_threshold_label.setText(
            f"Estado: {filter_state_label(str(self._image_state.currentData() or ''))}  |  "
            f"Umbral visual base: {image_gender_threshold:.2f}"
        )
        self._summary.setText(
            "Configura filtros clasicos y bloques IA sin editar JSON. "
            "Los thresholds avanzados se ajustan desde los botones de configuracion."
        )

    def _configure_text_thresholds(self) -> None:
        dialog = _ThresholdDialog(
            "Thresholds de texto inteligente",
            TEXT_THRESHOLD_FIELDS,
            self._text_thresholds,
            parent=self,
        )
        if dialog.exec() == QDialog.Accepted:
            self._text_thresholds = dialog.payload()
            self._refresh_summary()
            if callable(self._on_changed):
                self._on_changed()

    def _configure_image_thresholds(self) -> None:
        dialog = _ThresholdDialog(
            "Thresholds del prompt visual",
            IMAGE_THRESHOLD_FIELDS,
            self._image_thresholds,
            parent=self,
        )
        if dialog.exec() == QDialog.Accepted:
            self._image_thresholds = dialog.payload()
            self._refresh_summary()
            if callable(self._on_changed):
                self._on_changed()

    def load_defaults(self) -> None:
        self._apply_payload(self._ctx.services.leads.default_filter_config())
        set_panel_status(self, "Se cargaron los valores por defecto del filtrado.")
        if callable(self._on_changed):
            self._on_changed()

    def load_config(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._summary.setText("Cargando configuracion de filtros...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_filter_config_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def save_config(self) -> None:
        try:
            payload = self._payload_from_form()
            saved = self._ctx.services.leads.save_filter_config(payload)
            self._apply_payload(saved)
            set_panel_status(self, "Configuracion de filtros guardada.")
            if callable(self._on_changed):
                self._on_changed()
        except Exception as exc:
            show_panel_exception(self, exc, "No se pudo guardar la configuracion de filtros. Ver logs para mas detalles.")

    def delete_config(self) -> None:
        self._ctx.services.leads.delete_filter_config()
        self.load_defaults()
        set_panel_status(self, "Configuracion de filtros eliminada.")
        if callable(self._on_changed):
            self._on_changed()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        self._apply_payload(dict(data.get("payload") or {}))

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._summary.setText(f"No se pudo cargar la configuracion: {error.message}")
