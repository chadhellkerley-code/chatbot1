from __future__ import annotations

from typing import Any

from PySide6.QtWidgets import QGridLayout, QLabel

from gui.page_base import ClickableMetricCard, PageContext, SectionPage
from gui.query_runner import QueryError
from gui.snapshot_queries import build_leads_home_snapshot

from .filter_config_panel import LeadsFilterConfigPanel
from .filter_runner_panel import LeadsFilterRunnerPanel
from .import_panel import LeadsImportPanel
from .lists_panel import LeadsListsPanel
from .templates_panel import LeadsTemplatesPanel


LEADS_SUBSECTIONS: tuple[tuple[str, str], ...] = (
    ("leads_lists_page", "Listas"),
    ("leads_templates_page", "Plantillas"),
    ("leads_import_page", "Importar"),
    ("leads_filter_page", "Filtrado"),
)


class LeadsSectionPage(SectionPage):
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
            section_title="Leads",
            section_subtitle="Submenu horizontal para separar listas, plantillas, importacion y filtrado.",
            section_routes=LEADS_SUBSECTIONS,
            route_key=route_key,
            back_button=back_button,
            scrollable=scrollable,
            parent=parent,
        )


class LeadsHomePage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Leads",
            "Plantillas, listas, importacion y filtrado desde un modulo unico y persistente.",
            route_key=None,
            back_button=False,
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Centro de leads",
            "Las herramientas de prospeccion quedan separadas en paneles dedicados para mantener foco y velocidad.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(10)
        self._cards = {
            "templates": ClickableMetricCard("Plantillas", "0"),
            "lists": ClickableMetricCard("Listas origen", "0"),
            "completed": ClickableMetricCard("Resultados completos", "0"),
            "pending": ClickableMetricCard("Pendientes / ejecutar", "0"),
        }
        self._cards["templates"].clicked.connect(lambda: self._ctx.open_route("leads_templates_page", None))
        self._cards["lists"].clicked.connect(lambda: self._ctx.open_route("leads_lists_page", None))
        self._cards["completed"].clicked.connect(lambda: self._ctx.open_route("leads_filter_page", None))
        self._cards["pending"].clicked.connect(lambda: self._ctx.open_route("leads_filter_page", None))
        for index, key in enumerate(("lists", "templates", "completed", "pending")):
            grid.addWidget(self._cards[key], index // 2, index % 2)
        layout.addLayout(grid)

        helper = QLabel(
            "Usa el submenu superior para entrar directo al panel que necesites. "
            "Importacion, configuracion y ejecucion quedan aisladas para evitar ruido visual."
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
            self.set_status("Cargando resumen de leads...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_leads_home_snapshot(self._ctx.services),
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
        self.set_status(f"No se pudo cargar el resumen de leads: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._request_refresh()


class LeadsTemplatesPage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Plantillas",
            "Editor visual de mensajes outbound con multiples lineas por plantilla.",
            route_key="leads_templates_page",
            scrollable=False,
            parent=parent,
        )
        self._panel = LeadsTemplatesPanel(ctx, self)
        self.content_layout().addWidget(self._panel, 1)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._panel.refresh_page()


class LeadsListsPage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Listas",
            "Gestion simple de listas de usernames listas para importar, editar y reutilizar.",
            route_key="leads_lists_page",
            scrollable=False,
            parent=parent,
        )
        self._panel = LeadsListsPanel(ctx, self)
        self.content_layout().addWidget(self._panel, 1)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._panel.set_navigation_payload(payload)
        self._panel.refresh_page()


class LeadsImportPage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Importar leads",
            "Importacion de archivos CSV/TXT hacia listas persistentes.",
            route_key="leads_import_page",
            parent=parent,
        )
        self._panel = LeadsImportPanel(ctx, self)
        self.content_layout().addWidget(self._panel)
        self.content_layout().addStretch(1)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._panel.refresh_page()


class LeadsFilterConfigPage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Configuracion de filtros",
            "Formulario visual para clasificar perfiles y ajustar los bloques de IA.",
            route_key="leads_filter_config_page",
            parent=parent,
        )
        self._panel = LeadsFilterConfigPanel(ctx, self)
        self.content_layout().addWidget(self._panel)
        self.content_layout().addStretch(1)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._panel.load_config()


class LeadsFilterPage(LeadsSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Filtrado",
            "Configuracion, activacion y actividad del filtrado dentro de una sola vista operativa.",
            route_key="leads_filter_page",
            scrollable=False,
            parent=parent,
        )
        self._panel = LeadsFilterRunnerPanel(ctx, self)
        self.content_layout().addWidget(self._panel, 1)

    def on_navigate_to(self, payload: Any = None) -> None:
        self._panel.on_navigate_to(payload)

    def on_navigate_from(self) -> None:
        self._panel.on_navigate_from()
