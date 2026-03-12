from __future__ import annotations

import logging
from typing import Any

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QGridLayout, QLabel

from gui.query_runner import QueryError

from .page_base import BasePage, ClickableMetricCard, PageContext
from .snapshot_queries import build_dashboard_snapshot


logger = logging.getLogger(__name__)


class DashboardPage(BasePage):
    ROUTE_BY_KEY = {
        "total_accounts": "accounts_page",
        "connected_accounts": "accounts_page",
        "messages_sent_today": "campaign_monitor_page",
        "messages_error_today": "system_logs_page",
        "replies_received_today": "inbox_page",
        "active_campaigns": "campaign_monitor_page",
        "leads_processed_today": "leads_filter_page",
    }

    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Dashboard",
            "Metricas operativas del dia actual y accesos directos del escritorio CRM.",
            back_button=False,
            parent=parent,
        )
        self._cards: dict[str, ClickableMetricCard] = {}

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(10)
        labels = [
            ("total_accounts", "Cuentas totales"),
            ("connected_accounts", "Cuentas conectadas"),
            ("messages_sent_today", "Mensajes enviados hoy"),
            ("messages_error_today", "Errores hoy"),
            ("replies_received_today", "Respuestas hoy"),
            ("active_campaigns", "Campanas activas"),
            ("leads_processed_today", "Leads procesados hoy"),
        ]
        for index, (key, label_text) in enumerate(labels):
            card = ClickableMetricCard(label_text)
            card.clicked.connect(lambda key_value=key: self._open_metric_route(key_value))
            grid.addWidget(card, index // 4, index % 4)
            self._cards[key] = card
        self.content_layout().addLayout(grid)

        self._summary = QLabel("")
        self._summary.setObjectName("MutedText")
        self._summary.setWordWrap(True)
        self.content_layout().addWidget(self._summary)
        self.content_layout().addStretch(1)

        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._initial_refresh_pending = True
        self._timer = QTimer(self)
        self._timer.setInterval(5000)
        self._timer.timeout.connect(self.refresh_metrics)

    def _open_metric_route(self, key: str) -> None:
        route = self.ROUTE_BY_KEY.get(key)
        if route:
            self._ctx.open_route(route, None)

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
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        else:
            self.set_status("Cargando metricas...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_dashboard_snapshot(
                self._ctx.services,
                self._ctx.tasks,
                active_alias=self._ctx.state.active_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        snapshot = dict(payload) if isinstance(payload, dict) else {}
        self._snapshot_cache = snapshot
        self._apply_snapshot(snapshot)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        logger.error("Dashboard metrics refresh failed: %s", error.message)
        self.set_status("No se pudieron cargar las metricas. Ver logs para mas detalles.")
        try:
            self._ctx.logs.append("[error] Dashboard metrics refresh failed\n")
            self._ctx.logs.append(f"{error.message}\n")
        except Exception:
            pass

    def refresh_metrics(self) -> None:
        self._request_refresh()

    def on_navigate_to(self, payload: Any = None) -> None:
        if not self._timer.isActive():
            self._timer.start()
        if self._initial_refresh_pending:
            self._initial_refresh_pending = False
            QTimer.singleShot(0, self.refresh_metrics)
            return
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_metrics()

    def on_navigate_from(self) -> None:
        if self._timer.isActive():
            self._timer.stop()
