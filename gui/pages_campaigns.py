from __future__ import annotations

from typing import Any

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from gui.query_runner import QueryError
from src.dm_campaign.contracts import CampaignLaunchRequest

from .page_base import ClickableMetricCard, PageContext, SectionPage, safe_int, table_item, timestamp_to_label
from .snapshot_queries import (
    build_campaign_capacity_snapshot,
    build_campaign_create_snapshot,
    build_campaign_home_snapshot,
    build_campaign_monitor_snapshot,
)


class CardWidget(QFrame):
    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("SendSetupCard")

        shell = QVBoxLayout(self)
        shell.setContentsMargins(18, 18, 18, 18)
        shell.setSpacing(14)

        header = QFrame()
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(0)
        title_label = QLabel(title)
        title_label.setObjectName("SendSetupSectionTitle")
        header_layout.addWidget(title_label)
        shell.addWidget(header)

        body = QWidget()
        self._body_layout = QVBoxLayout(body)
        self._body_layout.setContentsMargins(0, 0, 0, 0)
        self._body_layout.setSpacing(12)
        shell.addWidget(body, 1)

    def body_layout(self) -> QVBoxLayout:
        return self._body_layout


CAMPAIGNS_SUBSECTIONS: tuple[tuple[str, str], ...] = (
    ("campaign_create_page", "Crear"),
    ("campaign_monitor_page", "Monitor"),
    ("campaign_history_page", "Historial"),
)


class CampaignsSectionPage(SectionPage):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        route_key: str | None,
        back_button: bool = True,
        scrollable: bool = True,
        parent=None,
    ) -> None:
        super().__init__(
            ctx,
            title,
            subtitle,
            section_title="CampaÃ±as",
            section_subtitle="Submenu horizontal para crear, monitorear y revisar historial.",
            section_routes=CAMPAIGNS_SUBSECTIONS,
            route_key=route_key,
            back_button=back_button,
            scrollable=scrollable,
            parent=parent,
        )


class CampaignsHomePage(CampaignsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "CampaÃ±as",
            "CreaciÃ³n, monitor y trazabilidad de campaÃ±as activas.",
            route_key=None,
            back_button=False,
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Centro de campaÃ±as",
            "La operacion queda separada en paneles de creacion, monitoreo e historial para trabajar sin mezclar estados.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        metrics = QGridLayout()
        metrics.setContentsMargins(0, 0, 0, 0)
        metrics.setHorizontalSpacing(10)
        metrics.setVerticalSpacing(10)
        self._cards = {
            "templates": ClickableMetricCard("Plantillas", "0"),
            "active": ClickableMetricCard("CampaÃ±a activa", "No"),
            "sent": ClickableMetricCard("Mensajes hoy", "0"),
            "errors": ClickableMetricCard("Errores hoy", "0"),
        }
        self._cards["templates"].clicked.connect(lambda: self._ctx.open_route("campaign_create_page", None))
        self._cards["active"].clicked.connect(lambda: self._ctx.open_route("campaign_monitor_page", None))
        self._cards["sent"].clicked.connect(lambda: self._ctx.open_route("campaign_history_page", None))
        self._cards["errors"].clicked.connect(lambda: self._ctx.open_route("campaign_history_page", None))
        for index, key in enumerate(("templates", "active", "sent", "errors")):
            metrics.addWidget(self._cards[key], index // 2, index % 2)
        layout.addLayout(metrics)

        helper = QLabel(
            "Usa el submenu superior para ir directo al flujo que necesites. "
            "La campaÃ±a activa y el historial quedan accesibles sin salir de la seccion."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)

        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        values = payload.get("values") if isinstance(payload, dict) else {}
        if not isinstance(values, dict):
            values = {}
        for key, value in values.items():
            if key in self._cards:
                self._cards[key].set_value(value)
        self._summary.setText(str(payload.get("summary") or "").strip())
        self.clear_status()

    def _request_refresh(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        if self._snapshot_cache is None:
            self.set_status("Cargando resumen de campanas...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_campaign_home_snapshot(self._ctx.services, self._ctx.tasks),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_snapshot(self._snapshot_cache)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar el resumen de campanas: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._request_refresh()


class CampaignCreatePage(CampaignsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Crear campaÃ±a",
            "Configura alias, leads, plantilla y concurrencia desde la GUI.",
            route_key="campaign_create_page",
            parent=parent,
        )
        self._template_payloads: dict[str, dict[str, str]] = {}
        self._lead_counts: dict[str, int] = {}
        self._summary_values: dict[str, QLabel] = {}
        self._capacity_cache: dict[tuple[str, str, int], dict[str, Any]] = {}
        self._form_snapshot_cache: dict[str, Any] | None = None
        self._form_request_id = 0
        self._form_loading = False
        self._capacity_request_id = 0
        self._start_submit_in_progress = False

        self._alias_combo = QComboBox()
        self._leads_combo = QComboBox()
        self._template_combo = QComboBox()
        self._template_preview = QPlainTextEdit()
        self._template_preview.setObjectName("LogConsole")
        self._template_preview.setReadOnly(True)
        self._template_preview.setMinimumHeight(120)
        self._manual_message = QPlainTextEdit()
        self._manual_message.setPlaceholderText("Opcional: mensaje manual si no usas plantilla guardada.")
        self._manual_message.setMinimumHeight(160)
        self._delay_min = QSpinBox()
        self._delay_min.setRange(0, 3600)
        self._delay_min.setValue(10)
        self._delay_max = QSpinBox()
        self._delay_max.setRange(0, 3600)
        self._delay_max.setValue(20)
        self._concurrency = QSpinBox()
        self._concurrency.setRange(1, 50)
        self._concurrency.setValue(1)
        self._headless = QCheckBox()
        self._headless.setObjectName("PacksActiveSwitch")
        self._headless.setChecked(True)
        self._capacity_label = QLabel("")
        self._capacity_label.setObjectName("MutedText")
        self._capacity_label.setWordWrap(True)
        self._use_template_yes = QRadioButton("SÃ­")
        self._use_template_no = QRadioButton("No")
        self._use_template_group = QButtonGroup(self)
        self._use_template_group.addButton(self._use_template_yes)
        self._use_template_group.addButton(self._use_template_no)
        self._use_template_yes.setChecked(True)

        body_row = QHBoxLayout()
        body_row.setContentsMargins(0, 0, 0, 0)
        body_row.setSpacing(16)

        left_column = QVBoxLayout()
        left_column.setContentsMargins(0, 0, 0, 0)
        left_column.setSpacing(16)
        right_column = QVBoxLayout()
        right_column.setContentsMargins(0, 0, 0, 0)
        right_column.setSpacing(16)

        left_wrapper = QWidget()
        left_wrapper.setLayout(left_column)
        right_wrapper = QWidget()
        right_wrapper.setLayout(right_column)
        right_wrapper.setMinimumWidth(320)
        right_wrapper.setMaximumWidth(420)

        body_row.addWidget(left_wrapper, 3)
        body_row.addWidget(right_wrapper, 2)

        source_card = CardWidget("Fuente de datos")
        source_grid = QGridLayout()
        source_grid.setContentsMargins(0, 0, 0, 0)
        source_grid.setHorizontalSpacing(12)
        source_grid.setVerticalSpacing(8)
        source_grid.addWidget(QLabel("Alias"), 0, 0)
        source_grid.addWidget(QLabel("Lista de leads"), 0, 1)
        source_grid.addWidget(self._alias_combo, 1, 0)
        source_grid.addWidget(self._leads_combo, 1, 1)
        source_grid.setColumnStretch(0, 1)
        source_grid.setColumnStretch(1, 1)
        source_card.body_layout().addLayout(source_grid)

        message_card = CardWidget("Mensaje")
        toggle_row = QHBoxLayout()
        toggle_row.setContentsMargins(0, 0, 0, 0)
        toggle_row.setSpacing(10)
        toggle_row.addWidget(QLabel("Usar plantilla"))
        toggle_row.addStretch(1)
        toggle_row.addWidget(self._use_template_yes)
        toggle_row.addWidget(self._use_template_no)
        message_card.body_layout().addLayout(toggle_row)

        self._template_section = QWidget()
        template_layout = QVBoxLayout(self._template_section)
        template_layout.setContentsMargins(0, 0, 0, 0)
        template_layout.setSpacing(10)
        template_layout.addWidget(QLabel("Plantilla"))
        template_layout.addWidget(self._template_combo)
        template_layout.addWidget(QLabel("Vista previa de plantilla"))
        template_layout.addWidget(self._template_preview)

        self._manual_section = QWidget()
        manual_layout = QVBoxLayout(self._manual_section)
        manual_layout.setContentsMargins(0, 0, 0, 0)
        manual_layout.setSpacing(10)
        manual_layout.addWidget(QLabel("Mensaje manual"))
        manual_layout.addWidget(self._manual_message)

        message_card.body_layout().addWidget(self._template_section)
        message_card.body_layout().addWidget(self._manual_section)

        params_card = CardWidget("ParÃ¡metros de envÃ­o")
        params_grid = QGridLayout()
        params_grid.setContentsMargins(0, 0, 0, 0)
        params_grid.setHorizontalSpacing(12)
        params_grid.setVerticalSpacing(8)
        params_grid.addWidget(QLabel("Delay mÃ­nimo"), 0, 0)
        params_grid.addWidget(QLabel("Delay mÃ¡ximo"), 0, 1)
        params_grid.addWidget(self._delay_min, 1, 0)
        params_grid.addWidget(self._delay_max, 1, 1)
        params_grid.addWidget(QLabel("Concurrencia"), 2, 0)
        params_grid.addWidget(self._concurrency, 3, 0)
        params_grid.setColumnStretch(0, 1)
        params_grid.setColumnStretch(1, 1)
        params_card.body_layout().addLayout(params_grid)

        browser_title = QLabel("Modo navegador")
        params_card.body_layout().addWidget(browser_title)
        browser_row = QHBoxLayout()
        browser_row.setContentsMargins(0, 0, 0, 0)
        browser_row.setSpacing(10)
        browser_row.addWidget(QLabel("Visible"))
        browser_row.addWidget(self._headless)
        browser_row.addWidget(QLabel("Headless"))
        browser_row.addStretch(1)
        params_card.body_layout().addLayout(browser_row)
        params_card.body_layout().addWidget(self._capacity_label)

        summary_card = CardWidget("Resumen de configuraciÃ³n")
        summary_intro = QLabel("Se actualiza automÃ¡ticamente con la configuraciÃ³n actual.")
        summary_intro.setObjectName("MutedText")
        summary_intro.setWordWrap(True)
        summary_card.body_layout().addWidget(summary_intro)

        summary_grid = QGridLayout()
        summary_grid.setContentsMargins(0, 0, 0, 0)
        summary_grid.setHorizontalSpacing(16)
        summary_grid.setVerticalSpacing(10)
        for row, (label_text, key) in enumerate(
            (
                ("Alias seleccionado", "alias"),
                ("Lista seleccionada", "list"),
                ("Cantidad de leads", "count"),
                ("Plantilla seleccionada", "template"),
                ("Delay configurado", "delay"),
                ("Concurrencia", "concurrency"),
                ("Modo navegador", "browser"),
            )
        ):
            label = QLabel(label_text)
            label.setObjectName("MutedText")
            value = QLabel("-")
            value.setObjectName("SendSetupSummaryValue")
            value.setWordWrap(True)
            summary_grid.addWidget(label, row, 0, Qt.AlignTop)
            summary_grid.addWidget(value, row, 1)
            self._summary_values[key] = value
        summary_grid.setColumnStretch(1, 1)
        summary_card.body_layout().addLayout(summary_grid)

        self._start_button = QPushButton("INICIAR CAMPAÃ‘A")
        self._start_button.setObjectName("PrimaryButton")
        self._start_button.setMinimumHeight(52)
        self._start_button.clicked.connect(self._start_campaign)

        left_column.addWidget(source_card)
        left_column.addWidget(message_card)
        left_column.addWidget(params_card)
        left_column.addStretch(1)

        right_column.addWidget(summary_card)
        right_column.addWidget(self._start_button)
        right_column.addStretch(1)

        self.content_layout().addLayout(body_row)
        self.content_layout().addStretch(1)

        self._alias_combo.currentIndexChanged.connect(self._on_alias_changed)
        self._alias_combo.currentIndexChanged.connect(self._update_summary)
        self._leads_combo.currentIndexChanged.connect(self._on_leads_changed)
        self._template_combo.currentIndexChanged.connect(self._on_template_changed)
        self._delay_min.valueChanged.connect(self._update_summary)
        self._delay_max.valueChanged.connect(self._update_summary)
        self._concurrency.valueChanged.connect(self._on_concurrency_changed)
        self._headless.toggled.connect(self._update_summary)
        self._use_template_yes.toggled.connect(self._update_message_mode)
        self._manual_message.textChanged.connect(self._update_summary)
        task_state_signal = getattr(self._ctx.tasks, "taskStateChanged", None)
        if task_state_signal is not None:
            task_state_signal.connect(self._sync_start_button_state)
        self._sync_start_button_state()

    def _request_form_refresh(self) -> None:
        if self._form_loading:
            return
        self._form_loading = True
        if self._form_snapshot_cache is None:
            self.set_status("Cargando configuraciÃ³n de campaÃ±a...")
        self._form_request_id = self._ctx.queries.submit(
            lambda: build_campaign_create_snapshot(
                self._ctx.services,
                active_alias=self._ctx.state.active_alias,
            ),
            on_success=self._on_form_snapshot_loaded,
            on_error=self._on_form_snapshot_failed,
        )

    def _on_form_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._form_request_id:
            return
        self._form_loading = False
        self._form_snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_form_snapshot(self._form_snapshot_cache)

    def _on_form_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._form_request_id:
            return
        self._form_loading = False
        self.set_status(f"No se pudo cargar la configuraciÃ³n: {error.message}")

    def _apply_form_snapshot(self, payload: dict[str, Any]) -> None:
        current_alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias)
        current_leads = str(self._leads_combo.currentData() or "")
        current_template_id = str(self._template_combo.currentData() or "")
        aliases = [str(item or "").strip() for item in payload.get("aliases") or [] if str(item or "").strip()]
        lead_lists = [str(item or "").strip() for item in payload.get("lead_lists") or [] if str(item or "").strip()]
        templates = payload.get("templates") if isinstance(payload.get("templates"), list) else []
        self._lead_counts = {
            str(name): safe_int(count)
            for name, count in (payload.get("lead_counts") or {}).items()
        }
        self._template_payloads = {}
        for item in templates:
            if not isinstance(item, dict):
                continue
            template_id = str(item.get("id") or "").strip()
            name = str(item.get("name") or "").strip()
            if not template_id or not name:
                continue
            template_payload = {
                "name": name,
                "text": str(item.get("text") or ""),
                "id": template_id,
            }
            self._template_payloads[template_id] = template_payload

        preferred_alias = current_alias or str(payload.get("active_alias") or self._ctx.state.active_alias)
        if preferred_alias not in aliases:
            preferred_alias = str(payload.get("active_alias") or (aliases[0] if aliases else ""))

        self._alias_combo.blockSignals(True)
        self._alias_combo.clear()
        for alias in aliases:
            self._alias_combo.addItem(alias, alias)
        if aliases:
            alias_index = self._alias_combo.findData(preferred_alias)
            self._alias_combo.setCurrentIndex(max(0, alias_index))
        self._alias_combo.blockSignals(False)

        self._leads_combo.blockSignals(True)
        self._leads_combo.clear()
        for name in lead_lists:
            self._leads_combo.addItem(name, name)
        if lead_lists:
            lead_index = self._leads_combo.findData(current_leads)
            if lead_index < 0:
                lead_index = 0
            self._leads_combo.setCurrentIndex(lead_index)
        self._leads_combo.blockSignals(False)

        self._template_combo.blockSignals(True)
        self._template_combo.clear()
        self._template_combo.addItem("Selecciona una plantilla", "")
        for template_payload in self._template_payloads.values():
            self._template_combo.addItem(str(template_payload.get("name") or ""), str(template_payload.get("id") or ""))
        template_index = self._template_combo.findData(current_template_id)
        self._template_combo.setCurrentIndex(max(0, template_index))
        self._template_combo.blockSignals(False)

        capacity = payload.get("capacity")
        if isinstance(capacity, dict):
            cache_key = self._capacity_cache_key_from_payload(capacity)
            if cache_key is not None:
                self._capacity_cache[cache_key] = dict(capacity)

        self._refresh_template_preview()
        self._update_message_mode()
        self._request_capacity_refresh(force=not bool(self._capacity_cache.get(self._capacity_cache_key())))
        self._update_summary()
        self.clear_status()

    def _capacity_cache_key(self) -> tuple[str, str, int]:
        return (
            str(self._alias_combo.currentData() or "").strip(),
            str(self._leads_combo.currentData() or "").strip(),
            max(0, int(self._concurrency.value() or 0)),
        )

    @staticmethod
    def _capacity_cache_key_from_payload(payload: dict[str, Any]) -> tuple[str, str, int] | None:
        alias = str(payload.get("alias") or "").strip()
        if not alias:
            return None
        return (
            alias,
            str(payload.get("leads_alias") or "").strip(),
            max(0, safe_int(payload.get("workers_requested") or 0)),
        )

    def _request_capacity_refresh(self, *, force: bool = False) -> None:
        alias, leads_alias, workers_requested = self._capacity_cache_key()
        if not alias:
            self._capacity_label.clear()
            return
        cached = self._capacity_cache.get((alias, leads_alias, workers_requested))
        if cached:
            self._apply_capacity_payload(cached)
            if not force:
                return
        else:
            self._capacity_label.setText("Calculando capacidad recomendada...")
        self._capacity_request_id = self._ctx.queries.submit(
            lambda: build_campaign_capacity_snapshot(
                self._ctx.services,
                alias=alias,
                leads_alias=leads_alias,
                workers_requested=workers_requested,
            ),
            on_success=self._on_capacity_snapshot_loaded,
            on_error=self._on_capacity_snapshot_failed,
        )

    def _on_capacity_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._capacity_request_id:
            return
        clean_payload = dict(payload) if isinstance(payload, dict) else {}
        cache_key = self._capacity_cache_key_from_payload(clean_payload)
        if cache_key is not None:
            self._capacity_cache[cache_key] = clean_payload
        current_key = self._capacity_cache_key()
        if cache_key == current_key:
            self._apply_capacity_payload(clean_payload)

    def _on_capacity_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._capacity_request_id:
            return
        self._capacity_label.setText(f"No se pudo calcular capacidad: {error.message}")

    def _apply_capacity_payload(self, payload: dict[str, Any]) -> None:
        clean_payload = dict(payload or {})
        if not clean_payload:
            self._capacity_label.clear()
            return
        workers_capacity = max(0, safe_int(clean_payload.get("workers_capacity") or 0))
        workers_effective = max(0, safe_int(clean_payload.get("workers_effective") or 0))
        proxies = len(clean_payload.get("proxies") or [])
        none_accounts = bool(clean_payload.get("has_none_accounts"))
        remaining_slots = max(0, safe_int(clean_payload.get("remaining_slots_total") or 0))
        planned_eligible = max(0, safe_int(clean_payload.get("planned_eligible_leads") or 0))
        planned_runnable = max(0, safe_int(clean_payload.get("planned_runnable_leads") or 0))
        account_remaining = [
            dict(item)
            for item in (clean_payload.get("account_remaining") or [])
            if isinstance(item, dict)
        ]
        account_remaining_preview = ", ".join(
            f"@{str(item.get('username') or '-').strip().lstrip('@')}: {max(0, safe_int(item.get('remaining') or 0))}"
            for item in account_remaining[:4]
            if str(item.get("username") or "").strip()
        )
        if len(account_remaining) > 4:
            account_remaining_preview += f", +{len(account_remaining) - 4}"
        note = "  |  Sin proxies: se usara 1 worker local con rotacion de cuentas." if none_accounts and not proxies else ""
        self._capacity_label.setText(
            f"Workers disponibles: {workers_capacity}  |  "
            f"Workers efectivos: {workers_effective}  |  "
            f"Cupo restante hoy: {remaining_slots}  |  "
            f"Leads elegibles: {planned_eligible}  |  "
            f"Leads ejecutables: {planned_runnable}  |  "
            f"Proxies detectados: {proxies}{note}"
            + (f"  |  Por cuenta: {account_remaining_preview}" if account_remaining_preview else "")
        )
        self._update_summary()

    def _on_alias_changed(self) -> None:
        self._request_capacity_refresh()
        self._update_summary()

    def _on_leads_changed(self) -> None:
        self._request_capacity_refresh()
        self._update_summary()

    def _on_concurrency_changed(self) -> None:
        self._request_capacity_refresh()
        self._update_summary()

    def _lead_count(self) -> int:
        leads_alias = str(self._leads_combo.currentData() or "").strip()
        if not leads_alias:
            return 0
        return safe_int(self._lead_counts.get(leads_alias))

    def _current_capacity_payload(self) -> dict[str, Any]:
        payload = self._capacity_cache.get(self._capacity_cache_key())
        return dict(payload) if isinstance(payload, dict) else {}

    def _planned_launch_total(self) -> int:
        payload = self._current_capacity_payload()
        if payload:
            return max(0, safe_int(payload.get("planned_runnable_leads") or 0))
        return self._lead_count()

    def _refresh_template_preview(self) -> None:
        if not self._use_template_yes.isChecked():
            self._template_preview.setPlainText("La vista previa se muestra al usar una plantilla guardada.")
            return
        template_id = str(self._template_combo.currentData() or "").strip()
        preview_text = str(self._template_payloads.get(template_id, {}).get("text") or "").strip()
        if preview_text:
            self._template_preview.setPlainText(preview_text)
            return
        if self._template_combo.count() <= 1:
            self._template_preview.setPlainText("No hay plantillas guardadas disponibles.")
            return
        self._template_preview.setPlainText("Selecciona una plantilla guardada para ver la vista previa.")

    def _on_template_changed(self) -> None:
        self._refresh_template_preview()
        self._update_summary()

    def _update_message_mode(self) -> None:
        use_template = self._use_template_yes.isChecked()
        self._template_section.setVisible(use_template)
        self._manual_section.setVisible(not use_template)
        self._refresh_template_preview()
        self._update_summary()

    def _update_summary(self) -> None:
        alias = str(self._alias_combo.currentData() or "").strip()
        leads_alias = str(self._leads_combo.currentData() or "").strip()
        template_id = str(self._template_combo.currentData() or "").strip()
        template_name = str(self._template_payloads.get(template_id, {}).get("name") or "").strip()
        self._summary_values["alias"].setText(alias or "-")
        self._summary_values["list"].setText(leads_alias or "-")
        self._summary_values["count"].setText(str(self._planned_launch_total()) if leads_alias else "0")
        if self._use_template_yes.isChecked():
            self._summary_values["template"].setText(template_name or "Sin plantilla seleccionada")
        else:
            self._summary_values["template"].setText("Mensaje manual")
        self._summary_values["delay"].setText(f"{self._delay_min.value()}s - {self._delay_max.value()}s")
        self._summary_values["concurrency"].setText(str(self._concurrency.value()))
        self._summary_values["browser"].setText("Headless" if self._headless.isChecked() else "Visible")

    def _campaign_task_running(self) -> bool:
        is_running = getattr(self._ctx.tasks, "is_running", None)
        if not callable(is_running):
            return False
        return bool(is_running("campaign"))

    def _sync_start_button_state(self) -> None:
        if self._start_submit_in_progress:
            self._start_button.setEnabled(False)
            self._start_button.setText("INICIANDO...")
            return
        if self._campaign_task_running():
            self._start_button.setEnabled(False)
            self._start_button.setText("CAMPAÃ‘A EN CURSO")
            return
        self._start_button.setEnabled(True)
        self._start_button.setText("INICIAR CAMPAÃ‘A")

    def _start_campaign(self) -> None:
        if self._start_submit_in_progress or self._campaign_task_running():
            return

        alias = str(self._alias_combo.currentData() or "").strip()
        leads_alias = str(self._leads_combo.currentData() or "").strip()
        use_template = self._use_template_yes.isChecked()
        template_id = str(self._template_combo.currentData() or "").strip() if use_template else ""
        manual_message = self._manual_message.toPlainText().strip() if not use_template else ""
        if use_template:
            selected_template = dict(self._template_payloads.get(template_id) or {})
            templates = [selected_template] if selected_template else []
        else:
            templates = self._ctx.services.campaigns.build_template_entries(manual_message=manual_message)
        if not alias or not leads_alias or not templates:
            self.show_error("Alias, lista de leads y plantilla/mensaje son obligatorios.")
            return

        launch_input = {
            "alias": alias,
            "leads_alias": leads_alias,
            "templates": templates,
            "delay_min": self._delay_min.value(),
            "delay_max": self._delay_max.value(),
            "workers_requested": self._concurrency.value(),
            "headless": self._headless.isChecked(),
            "total_leads": self._planned_launch_total(),
        }
        launch_request = CampaignLaunchRequest.from_payload(launch_input)
        self._start_submit_in_progress = True
        self._sync_start_button_state()
        try:
            monitor_payload = dict(
                self._ctx.services.campaigns.launch_campaign(
                    launch_request,
                    task_runner=self._ctx.tasks,
                )
                or {}
            )
        except Exception as exc:
            self.show_error(str(exc))
            return
        finally:
            self._start_submit_in_progress = False
            self._sync_start_button_state()
        monitor_payload["log_cursor_start"] = self._ctx.logs.cursor()
        self.set_status("CampaÃ±a iniciada.")
        self._ctx.state.campaign_monitor = monitor_payload
        self._ctx.open_route("campaign_monitor_page", dict(monitor_payload))

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._form_snapshot_cache is not None:
            self._apply_form_snapshot(self._form_snapshot_cache)
        self._sync_start_button_state()
        self._request_form_refresh()


class CampaignMonitorPage(CampaignsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Monitor de campaÃ±a",
            "Seguimiento en vivo del run actual con mÃ©tricas propias, workers y log incremental.",
            route_key="campaign_monitor_page",
            parent=parent,
        )
        header = QFrame()
        header.setObjectName("ExecCard")
        header_layout = QGridLayout(header)
        header_layout.setContentsMargins(16, 16, 16, 16)
        header_layout.setHorizontalSpacing(12)
        header_layout.setVerticalSpacing(10)
        self._run_value = QLabel("-")
        self._alias_value = QLabel("-")
        self._leads_value = QLabel("-")
        self._started_value = QLabel("-")
        self._finished_value = QLabel("-")
        self._message_value = QLabel("-")
        self._message_value.setObjectName("MutedText")
        self._message_value.setWordWrap(True)
        self._sent_value = QLabel("0")
        self._failed_value = QLabel("0")
        self._skipped_value = QLabel("0")
        self._preblocked_value = QLabel("0")
        self._remaining_value = QLabel("0")
        self._active_accounts_value = QLabel("0")
        self._workers_active_value = QLabel("0")
        self._workers_requested_value = QLabel("0")
        self._workers_effective_value = QLabel("0")
        self._workers_capacity_value = QLabel("0")
        self._status_value = QLabel("Idle")
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        header_layout.addWidget(QLabel("Run"), 0, 0)
        header_layout.addWidget(self._run_value, 0, 1)
        header_layout.addWidget(QLabel("Estado"), 0, 2)
        header_layout.addWidget(self._status_value, 0, 3)
        header_layout.addWidget(QLabel("Alias"), 1, 0)
        header_layout.addWidget(self._alias_value, 1, 1)
        header_layout.addWidget(QLabel("Lista leads"), 1, 2)
        header_layout.addWidget(self._leads_value, 1, 3)
        header_layout.addWidget(QLabel("Inicio"), 2, 0)
        header_layout.addWidget(self._started_value, 2, 1)
        header_layout.addWidget(QLabel("Fin"), 2, 2)
        header_layout.addWidget(self._finished_value, 2, 3)
        header_layout.addWidget(QLabel("Enviados"), 3, 0)
        header_layout.addWidget(self._sent_value, 3, 1)
        header_layout.addWidget(QLabel("Fallidos"), 3, 2)
        header_layout.addWidget(self._failed_value, 3, 3)
        header_layout.addWidget(QLabel("Saltados"), 4, 0)
        header_layout.addWidget(self._skipped_value, 4, 1)
        header_layout.addWidget(QLabel("Prebloqueados"), 4, 2)
        header_layout.addWidget(self._preblocked_value, 4, 3)
        header_layout.addWidget(QLabel("Pendientes"), 5, 0)
        header_layout.addWidget(self._remaining_value, 5, 1)
        header_layout.addWidget(QLabel("Cuentas activas"), 5, 2)
        header_layout.addWidget(self._active_accounts_value, 5, 3)
        header_layout.addWidget(QLabel("Workers activos"), 6, 0)
        header_layout.addWidget(self._workers_active_value, 6, 1)
        header_layout.addWidget(QLabel("Workers solicitados"), 6, 2)
        header_layout.addWidget(self._workers_requested_value, 6, 3)
        header_layout.addWidget(QLabel("Workers efectivos"), 7, 0)
        header_layout.addWidget(self._workers_effective_value, 7, 1)
        header_layout.addWidget(QLabel("Capacidad workers"), 7, 2)
        header_layout.addWidget(self._workers_capacity_value, 7, 3)
        header_layout.addWidget(self._message_value, 8, 0, 1, 4)
        header_layout.addWidget(self._progress, 9, 0, 1, 4)
        self.content_layout().addWidget(header)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        stop_button = QPushButton("Detener")
        stop_button.setObjectName("DangerButton")
        stop_button.clicked.connect(self._stop_campaign)
        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_monitor)
        actions.addWidget(stop_button)
        actions.addWidget(refresh_button)
        actions.addStretch(1)
        self.content_layout().addLayout(actions)

        self._workers_table = QTableWidget(0, 7)
        self._workers_table.setHorizontalHeaderLabels(
            ["Worker", "Proxy", "Estado", "Etapa", "Cuenta", "Lead", "Reinicios"]
        )
        self._workers_table.verticalHeader().setVisible(False)
        self._workers_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._workers_table.setSelectionMode(QAbstractItemView.NoSelection)
        self._workers_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.content_layout().addWidget(self._workers_table)

        self._logs = QPlainTextEdit()
        self._logs.setObjectName("LogConsole")
        self._logs.setReadOnly(True)
        self.content_layout().addWidget(self._logs)

        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._worker_rows_signature: list[tuple[str, ...]] = []
        self._log_cursor = 0
        self._log_dirty = False
        self._log_live_updates = False
        self._log_flush_timer = QTimer(self)
        self._log_flush_timer.setSingleShot(True)
        self._log_flush_timer.setInterval(0)
        self._log_flush_timer.timeout.connect(self._sync_logs_from_store)
        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self.refresh_monitor)
        self._ctx.tasks.taskFinished.connect(self._on_task_finished)
        self._ctx.logs.logAdded.connect(self._on_log_added)
        self._ctx.logs.cleared.connect(self._on_logs_cleared)

    def _monitor_state(self) -> dict[str, Any]:
        return dict(self._ctx.state.campaign_monitor or {})

    @staticmethod
    def _worker_rows_signature_from_payload(worker_rows: list[dict[str, Any]]) -> list[tuple[str, ...]]:
        signature: list[tuple[str, ...]] = []
        for row in worker_rows:
            signature.append(
                (
                    str(row.get("worker_id", "")),
                    str(row.get("proxy_label", row.get("proxy_id", ""))),
                    str(row.get("execution_state", "")),
                    str(row.get("execution_stage", "")),
                    str(row.get("current_account", "")),
                    str(row.get("current_lead", "")),
                    str(row.get("restarts", 0)),
                )
            )
        return signature

    def _apply_worker_rows(self, worker_rows: list[dict[str, Any]]) -> None:
        signature = self._worker_rows_signature_from_payload(worker_rows)
        if signature == self._worker_rows_signature:
            return
        self._workers_table.setUpdatesEnabled(False)
        try:
            if self._workers_table.rowCount() != len(signature):
                self._workers_table.setRowCount(len(signature))
            for row_index, values in enumerate(signature):
                for column, value in enumerate(values):
                    current = self._workers_table.item(row_index, column)
                    if current is None:
                        self._workers_table.setItem(row_index, column, table_item(value))
                        continue
                    if current.text() != value:
                        current.setText(value)
        finally:
            self._workers_table.setUpdatesEnabled(True)
        self._worker_rows_signature = signature

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        self._run_value.setText(str(payload.get("run_id") or "-"))
        self._alias_value.setText(str(payload.get("alias") or "-"))
        self._leads_value.setText(str(payload.get("leads_alias") or "-"))
        self._sent_value.setText(str(payload.get("sent") or 0))
        self._failed_value.setText(str(payload.get("failed") or 0))
        self._skipped_value.setText(str(payload.get("skipped") or 0))
        self._preblocked_value.setText(str(payload.get("skipped_preblocked") or 0))
        self._remaining_value.setText(str(payload.get("remaining") or 0))
        self._active_accounts_value.setText(str(payload.get("active_accounts") or 0))
        self._workers_active_value.setText(str(payload.get("workers_active") or 0))
        self._workers_requested_value.setText(str(payload.get("workers_requested") or 0))
        self._workers_effective_value.setText(str(payload.get("workers_effective") or 0))
        self._workers_capacity_value.setText(str(payload.get("workers_capacity") or 0))
        self._status_value.setText(str(payload.get("status") or "Stopped"))
        self._started_value.setText(str(payload.get("started_at") or "-"))
        self._finished_value.setText(str(payload.get("finished_at") or "-"))
        self._message_value.setText(str(payload.get("message") or "-"))
        self._progress.setValue(max(0, min(100, int(payload.get("progress") or 0))))
        worker_rows = payload.get("worker_rows") if isinstance(payload.get("worker_rows"), list) else []
        self._apply_worker_rows(worker_rows)
        self.clear_status()
        if not bool(payload.get("task_active")) and self._timer.isActive():
            self._timer.stop()

    def _sync_logs_from_store(self, *, force: bool = False) -> None:
        if not force and (not self._log_live_updates or not self._log_dirty):
            return
        self._log_cursor, chunk, reset = self._ctx.logs.read_since(self._log_cursor)
        if reset:
            self._logs.setPlainText(chunk)
        elif chunk:
            self._logs.moveCursor(QTextCursor.End)
            self._logs.insertPlainText(chunk)
        self._log_dirty = False
        cursor = self._logs.textCursor()
        cursor.movePosition(QTextCursor.End)
        self._logs.setTextCursor(cursor)

    def _request_refresh(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        if self._snapshot_cache is None:
            self.set_status("Cargando monitor...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_campaign_monitor_snapshot(
                self._ctx.services,
                self._ctx.tasks,
                monitor_state=self._monitor_state(),
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_snapshot(self._snapshot_cache)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar el monitor: {error.message}")

    def refresh_monitor(self) -> None:
        self._sync_logs_from_store()
        self._request_refresh()

    def _on_log_added(self, chunk: str) -> None:
        if not self._log_live_updates:
            return
        text = str(chunk or "")
        if not text:
            return
        self._log_dirty = True
        if not self._log_flush_timer.isActive():
            self._log_flush_timer.start()

    def _on_logs_cleared(self) -> None:
        if self._log_live_updates:
            if self._log_flush_timer.isActive():
                self._log_flush_timer.stop()
            self._logs.clear()
            self._log_cursor = self._ctx.logs.cursor()
            self._log_dirty = False

    def _pause_campaign(self) -> None:
        self._ctx.services.campaigns.stop_campaign("campaign pause requested from GUI")
        self.set_status("SeÃ±al de stop seguro enviada.")

    def _stop_campaign(self) -> None:
        self._ctx.services.campaigns.stop_campaign("campaign stop requested from GUI")
        self.set_status("SeÃ±al de stop enviada.")

    def _on_task_finished(self, task_name: str, ok: bool, message: str) -> None:
        if task_name != "campaign":
            return
        self._sync_logs_from_store(force=True)
        self.refresh_monitor()
        if message:
            self.set_status(message if ok else f"CampaÃ±a finalizada con error: {message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if isinstance(payload, dict) and payload:
            self._ctx.state.campaign_monitor = dict(payload)
        monitor_state = self._monitor_state()
        self._log_cursor = max(0, safe_int(monitor_state.get("log_cursor_start")))
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._log_live_updates = True
        self._log_dirty = True
        self._sync_logs_from_store(force=True)
        if not self._timer.isActive():
            self._timer.start()
        self.refresh_monitor()

    def on_navigate_from(self) -> None:
        self._log_live_updates = False
        if self._log_flush_timer.isActive():
            self._log_flush_timer.stop()
        if self._timer.isActive():
            self._timer.stop()


class CampaignHistoryPage(CampaignsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Historial de campañas",
            "Eventos recientes de envíos registrados en storage.",
            route_key="campaign_history_page",
            parent=parent,
        )
        self._summary_day_key = ""
        self._summary_table = QTableWidget(0, 5)
        self._summary_table.setHorizontalHeaderLabels(
            ["Cuenta", "Enviados hoy", "OK", "Fallidos", "Tasa %"]
        )
        self._summary_table.verticalHeader().setVisible(False)
        self._summary_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._summary_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.content_layout().addWidget(self._summary_table)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["Fecha", "Cuenta", "Lead", "OK", "Detalle", "Alias"]
        )
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.content_layout().addWidget(self._table)
        refresh_button = QPushButton("Refresh")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_table)
        self.content_layout().addWidget(refresh_button, 0, Qt.AlignLeft)
        self.content_layout().addStretch(1)

        self._day_rollover_timer = QTimer(self)
        self._day_rollover_timer.setInterval(60_000)
        self._day_rollover_timer.timeout.connect(self._refresh_for_day_boundary)

    @staticmethod
    def _normalize_account_username(value: Any) -> str:
        return str(value or "").strip().lstrip("@").lower()

    def _today_in_storage_timezone(self) -> str:
        from datetime import datetime

        from core.storage import TZ

        return datetime.now(TZ).date().isoformat()

    def _populate_summary_table(self, rows: list[dict[str, Any]]) -> None:
        from datetime import datetime

        from core import accounts as accounts_module
        from core.storage import TZ

        today = datetime.now(TZ).date()
        account_rows = accounts_module.list_all()
        ordered_accounts: list[str] = []
        seen_accounts: set[str] = set()
        for record in account_rows:
            if not isinstance(record, dict):
                continue
            username = str(record.get("username") or "").strip().lstrip("@")
            username_key = username.lower()
            if not username or username_key in seen_accounts:
                continue
            seen_accounts.add(username_key)
            ordered_accounts.append(username)

        summary: dict[str, dict[str, int]] = {
            username.lower(): {"ok": 0, "failed": 0} for username in ordered_accounts
        }
        for row in rows:
            account_key = self._normalize_account_username(row.get("account"))
            if not account_key or account_key not in summary:
                continue
            ts = row.get("ts")
            try:
                local_dt = datetime.fromtimestamp(float(ts), tz=TZ)
            except Exception:
                continue
            if local_dt.date() != today:
                continue
            if bool(row.get("ok")):
                summary[account_key]["ok"] += 1
                continue
            if row.get("skipped") or row.get("skip_reason"):
                continue
            summary[account_key]["failed"] += 1

        self._summary_table.setRowCount(len(ordered_accounts))
        for row_index, username in enumerate(ordered_accounts):
            counts = summary.get(username.lower(), {"ok": 0, "failed": 0})
            ok_count = int(counts.get("ok", 0))
            failed_count = int(counts.get("failed", 0))
            sent_today = ok_count + failed_count
            rate_label = str(round(ok_count / sent_today * 100)) if sent_today > 0 else "-"
            values = [
                username,
                str(sent_today),
                str(ok_count),
                str(failed_count),
                rate_label,
            ]
            for column, value in enumerate(values):
                self._summary_table.setItem(row_index, column, table_item(value))

        self._summary_day_key = today.isoformat()

    def _populate_events_table(self, rows: list[dict[str, Any]]) -> None:
        recent = list(reversed(rows[-200:]))
        self._table.setRowCount(len(recent))
        for row_index, row in enumerate(recent):
            values = [
                timestamp_to_label(row.get("ts")),
                row.get("account", ""),
                row.get("lead", ""),
                "Sí" if bool(row.get("ok")) else "No",
                row.get("detail", row.get("reason", "")),
                row.get("alias", ""),
            ]
            for column, value in enumerate(values):
                self._table.setItem(row_index, column, table_item(value))

    def refresh_table(self) -> None:
        rows = self._ctx.services.context.read_jsonl(
            self._ctx.services.context.storage_path("sent_log.jsonl")
        )
        self._populate_summary_table(rows)
        self._populate_events_table(rows)

    def _refresh_for_day_boundary(self) -> None:
        if self._summary_day_key != self._today_in_storage_timezone():
            self.refresh_table()

    def on_navigate_to(self, payload: Any = None) -> None:
        self.refresh_table()
        if not self._day_rollover_timer.isActive():
            self._day_rollover_timer.start()

    def on_navigate_from(self) -> None:
        if self._day_rollover_timer.isActive():
            self._day_rollover_timer.stop()