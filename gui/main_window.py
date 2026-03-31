from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Mapping
from typing import Any, Callable

from application.services import ApplicationServices, build_application_services
from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from gui.controllers.branding import _build_brand_icon, _build_brand_logo_pixmap
from gui.navigation import NavigationRouter
from gui.page_base import GuiState, PageContext
from gui.query_runner import QueryManager
from gui.pages_accounts import AccountsActionsPage, AccountsHomePage, AccountsPage, AliasPage, ProxiesPage
from gui.pages_automation import (
    AutomationAutoresponderPage,
    AutomationConfigPage,
    AutomationHomePage,
    AutomationPacksPage,
    AutomationWhatsAppPage,
)
from gui.pages_automation_flow import AutomationFlowPage
from gui.pages_campaigns import CampaignCreatePage, CampaignHistoryPage, CampaignMonitorPage, CampaignsHomePage
from gui.pages_dashboard import DashboardPage
from gui.pages_inbox import InboxPage
from gui.modules.leads import (
    LeadsHomePage,
    LeadsImportPage,
    LeadsListsPage,
    LeadsTemplatesPage,
)
from gui.pages_system import (
    SystemConfigPage,
    SystemDiagnosticsPage,
    SystemHomePage,
    SystemLicensePage,
    SystemLogsPage,
)
from gui.task_runner import LogStore, TaskManager
from src.telemetry import HeartbeatClient, runtime_health_snapshot


logger = logging.getLogger(__name__)


class _SidebarButton(QPushButton):
    def __init__(self, label: str, route: str, parent: QWidget | None = None) -> None:
        super().__init__(label, parent)
        self.route = str(route or "").strip()
        self.setObjectName("SidebarMenuButton")
        self.setCursor(Qt.PointingHandCursor)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumHeight(44)

    def set_active(self, active: bool) -> None:
        self.setProperty("active", bool(active))
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()


class _LazyPageRegistry(Mapping[str, QWidget]):
    def __init__(self, window: "MainWindow") -> None:
        self._window = window

    def __getitem__(self, route: str) -> QWidget:
        return self._window._ensure_page(route)

    def __iter__(self) -> Iterator[str]:
        return iter(self._window._page_factories)

    def __len__(self) -> int:
        return len(self._window._page_factories)

    def get(self, route: str, default: QWidget | None = None) -> QWidget | None:
        clean_route = str(route or "").strip()
        if not clean_route:
            return default
        return self._window._created_pages.get(self._window._normalize_route(clean_route), default)


class MainWindow(QMainWindow):
<<<<<<< HEAD
    DEFAULT_LAUNCH_WIDTH = 1560
    DEFAULT_LAUNCH_HEIGHT = 980
    DEFAULT_LAUNCH_MARGIN = 32
=======
>>>>>>> origin/main
    SIDEBAR_ROUTES = (
        ("dashboard", "Dashboard"),
        ("accounts_home", "Cuentas"),
        ("leads_home", "Leads"),
        ("campaigns_home", "Campanas"),
        ("automation_home", "Automatizaciones"),
        ("system_home", "Sistema"),
        ("inbox_page", "Inbox CRM"),
    )
    SYSTEM_ROUTES = frozenset(
        {
            "system_home",
            "system_license_page",
            "system_logs_page",
            "system_config_page",
            "system_diagnostics_page",
        }
    )

    def __init__(
        self,
        *,
        mode: str = "owner",
        services: ApplicationServices | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.mode = str(mode or "owner").strip() or "owner"
        self.services = services or build_application_services()
        self.backend_exit_code: int | None = None
        self._heartbeat_client: HeartbeatClient | None = None
        self._proxy_housekeeping_timer: QTimer | None = None
        self._last_proxy_housekeeping: dict[str, Any] = {}

        self.logs = LogStore(parent=self)
        self.tasks = TaskManager(self.logs, parent=self)
        self.queries = QueryManager(parent=self)
        initial_active_alias = "default"
        active_alias_getter = getattr(self.services, "get_initial_active_alias", None)
        if callable(active_alias_getter):
            try:
                initial_active_alias = str(active_alias_getter() or "default").strip() or "default"
            except Exception:
                initial_active_alias = "default"
        self.state = GuiState(active_alias=initial_active_alias)
        self.router = NavigationRouter(self)
        self._created_pages: dict[str, QWidget] = {}
        self._page_factories: dict[str, Callable[[], QWidget]] = {}
        self.pages: Mapping[str, QWidget] = _LazyPageRegistry(self)
        self._route_sections: dict[str, str] = {}
        self._route_titles: dict[str, str] = {}
        self._route_aliases: dict[str, str] = {}
        self._sidebar_buttons: dict[str, _SidebarButton] = {}
        self._sidebar: QFrame | None = None
        self._sidebar_default_width = 280

        self.setWindowTitle("InstaCRM")
<<<<<<< HEAD
        self.setMinimumSize(1240, 800)
=======
        self.setMinimumSize(1180, 760)
>>>>>>> origin/main
        self.setWindowIcon(_build_brand_icon())

        self._build_shell()
        self._register_pages()

        self.router.routeChanged.connect(self._on_route_changed)
        self.router.historyChanged.connect(self._on_history_changed)
        self.tasks.taskStateChanged.connect(self._refresh_status_card)

        self.open_route("dashboard", remember=False, clear_history=True)
        self._apply_initial_geometry()

    def _is_owner_mode(self) -> bool:
        return self.mode == "owner"

    def _is_system_route(self, route: str) -> bool:
        clean_route = self._normalize_route(route)
        return clean_route in self.SYSTEM_ROUTES

    def _sidebar_routes(self) -> tuple[tuple[str, str], ...]:
        if self._is_owner_mode():
            return self.SIDEBAR_ROUTES
        return tuple(
            (route, label)
            for route, label in self.SIDEBAR_ROUTES
            if not self._is_system_route(route)
        )

    def _build_shell(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)

        layout = QHBoxLayout(root)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(280)
        self._sidebar = sidebar
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(14, 14, 14, 14)
        sidebar_layout.setSpacing(12)

        header = QFrame()
        header.setObjectName("SidebarHeader")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(12, 12, 12, 12)
        header_layout.setSpacing(10)

        logo = QLabel()
        logo.setPixmap(_build_brand_logo_pixmap(28))
        header_layout.addWidget(logo, 0, Qt.AlignTop)

        header_text = QVBoxLayout()
        header_text.setContentsMargins(0, 0, 0, 0)
        header_text.setSpacing(2)
        title = QLabel("INSTA CRM")
        title.setObjectName("BrandHeaderMain")
        subtitle = QLabel("Automation Platform")
        subtitle.setObjectName("BrandHeaderSub")
        header_text.addWidget(title)
        header_text.addWidget(subtitle)
        header_layout.addLayout(header_text, 1)

        badge = QLabel(self.mode.upper())
        badge.setObjectName("ModeBadge")
        header_layout.addWidget(badge, 0, Qt.AlignRight | Qt.AlignTop)
        sidebar_layout.addWidget(header)

        status = QFrame()
        status.setObjectName("StatusCard")
        status_layout = QVBoxLayout(status)
        status_layout.setContentsMargins(12, 12, 12, 12)
        status_layout.setSpacing(6)
        self._alias_value = QLabel("-")
        self._alias_value.setObjectName("StatusValue")
        self._route_value = QLabel("-")
        self._route_value.setObjectName("StatusValue")
        self._tasks_value = QLabel("0")
        self._tasks_value.setObjectName("StatusValue")
        for key, value in (
            ("Alias activo", self._alias_value),
            ("Pantalla", self._route_value),
            ("Tareas", self._tasks_value),
        ):
            key_label = QLabel(key)
            key_label.setObjectName("StatusKey")
            status_layout.addWidget(key_label)
            status_layout.addWidget(value)
        sidebar_layout.addWidget(status)

        menu_scroll = QScrollArea()
        menu_scroll.setObjectName("SidebarScroll")
        menu_scroll.setWidgetResizable(True)
        menu_scroll.setFrameShape(QFrame.NoFrame)
        menu_content = QWidget()
        menu_content.setObjectName("SidebarScrollContent")
        menu_layout = QVBoxLayout(menu_content)
        menu_layout.setContentsMargins(0, 0, 0, 0)
        menu_layout.setSpacing(8)

        menu_container = QFrame()
        menu_container.setObjectName("SidebarMenuContainer")
        menu_buttons_layout = QVBoxLayout(menu_container)
        menu_buttons_layout.setContentsMargins(8, 8, 8, 8)
        menu_buttons_layout.setSpacing(8)
        for route, label in self._sidebar_routes():
            button = _SidebarButton(label, route)
            button.clicked.connect(lambda checked=False, target=route: self.open_route(target))
            self._sidebar_buttons[route] = button
            menu_buttons_layout.addWidget(button)
            self._route_titles[route] = label
        menu_layout.addWidget(menu_container)
        menu_layout.addStretch(1)
        menu_scroll.setWidget(menu_content)
        sidebar_layout.addWidget(menu_scroll, 1)

        exit_button = QPushButton("Salir")
        exit_button.setObjectName("DangerButton")
        exit_button.setMinimumHeight(44)
        exit_button.clicked.connect(self.close)
        sidebar_layout.addWidget(exit_button)

        content = QFrame()
        content.setObjectName("CentralArea")
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(0)
        content_layout.addWidget(self.router)

        layout.addWidget(sidebar)
        layout.addWidget(content, 1)

    def _page_context(self) -> PageContext:
        return PageContext(
            services=self.services,
            tasks=self.tasks,
            logs=self.logs,
            queries=self.queries,
            state=self.state,
            open_route=lambda route, payload=None: self.open_route(route, payload=payload),
            go_back=self.go_back,
            can_go_back=self.router.can_go_back,
            toggle_sidebar=self.toggle_sidebar,
            is_sidebar_visible=self.is_sidebar_visible,
        )

    def _register_page_factory(
        self,
        route: str,
        factory: Callable[[], QWidget],
        *,
        section: str,
        title: str,
    ) -> None:
        self._page_factories[route] = factory
        self._route_sections[route] = section
        self._route_titles[route] = title

    def _normalize_route(self, route: str) -> str:
        clean_route = str(route or "").strip()
        return self._route_aliases.get(clean_route, clean_route)

    def _ensure_page(self, route: str) -> QWidget:
        clean_route = self._normalize_route(route)
        if not clean_route:
            raise KeyError("Route is required.")
        page = self._created_pages.get(clean_route)
        if page is not None:
            return page
        factory = self._page_factories.get(clean_route)
        if factory is None:
            raise KeyError(f"Route not registered: {clean_route}")
        widget = factory()
        page = self.router.register_page(clean_route, widget)
        self._created_pages[clean_route] = page
        return page

    def _register_pages(self) -> None:
        ctx = self._page_context()
        self._register_page_factory("dashboard", lambda: DashboardPage(ctx), section="dashboard", title="Dashboard")

        self._register_page_factory("accounts_home", lambda: AccountsHomePage(ctx), section="accounts_home", title="Cuentas")
        self._register_page_factory("alias_page", lambda: AliasPage(ctx), section="accounts_home", title="Alias")
        self._register_page_factory("accounts_page", lambda: AccountsPage(ctx), section="accounts_home", title="Cuentas")
        self._register_page_factory("proxies_page", lambda: ProxiesPage(ctx), section="accounts_home", title="Proxies")
        self._register_page_factory(
            "accounts_actions_page",
            lambda: AccountsActionsPage(ctx),
            section="accounts_home",
            title="Acciones",
        )

        self._register_page_factory("leads_home", lambda: LeadsHomePage(ctx), section="leads_home", title="Leads")
        self._register_page_factory(
            "leads_templates_page",
            lambda: LeadsTemplatesPage(ctx),
            section="leads_home",
            title="Plantillas",
        )
        self._register_page_factory("leads_lists_page", lambda: LeadsListsPage(ctx), section="leads_home", title="Listas")
        self._register_page_factory(
            "leads_import_page",
            lambda: LeadsImportPage(ctx),
            section="leads_home",
            title="Importar leads",
        )
        self._register_page_factory("campaigns_home", lambda: CampaignsHomePage(ctx), section="campaigns_home", title="Campanas")
        self._register_page_factory(
            "campaign_create_page",
            lambda: CampaignCreatePage(ctx),
            section="campaigns_home",
            title="Crear campana",
        )
        self._register_page_factory(
            "campaign_monitor_page",
            lambda: CampaignMonitorPage(ctx),
            section="campaigns_home",
            title="Monitor campana",
        )
        self._register_page_factory(
            "campaign_history_page",
            lambda: CampaignHistoryPage(ctx),
            section="campaigns_home",
            title="Historial campanas",
        )

        self._register_page_factory(
            "automation_home",
            lambda: AutomationHomePage(ctx),
            section="automation_home",
            title="Automatizaciones",
        )
        self._register_page_factory(
            "automation_config_page",
            lambda: AutomationConfigPage(ctx),
            section="automation_home",
            title="Configuracion automatizacion",
        )
        self._register_page_factory(
            "automation_autoresponder_page",
            lambda: AutomationAutoresponderPage(ctx),
            section="automation_home",
            title="Autoresponder IA",
        )
        self._register_page_factory(
            "automation_packs_page",
            lambda: AutomationPacksPage(ctx),
            section="automation_home",
            title="Packs conversacionales",
        )
        self._register_page_factory(
            "automation_flow_page",
            lambda: AutomationFlowPage(ctx),
            section="automation_home",
            title="Flow builder",
        )
        self._register_page_factory(
            "automation_whatsapp_page",
            lambda: AutomationWhatsAppPage(ctx),
            section="automation_home",
            title="WhatsApp",
        )

        if self._is_owner_mode():
            self._register_page_factory(
                "system_home",
                lambda: SystemHomePage(ctx),
                section="system_home",
                title="Sistema",
            )
            self._register_page_factory(
                "system_license_page",
                lambda: SystemLicensePage(ctx),
                section="system_home",
                title="Licencias",
            )
            self._register_page_factory(
                "system_logs_page",
                lambda: SystemLogsPage(ctx),
                section="system_home",
                title="Logs",
            )
            self._register_page_factory(
                "system_config_page",
                lambda: SystemConfigPage(ctx),
                section="system_home",
                title="Configuracion",
            )
            self._register_page_factory(
                "system_diagnostics_page",
                lambda: SystemDiagnosticsPage(ctx),
                section="system_home",
                title="Diagnostico",
            )

        self._register_page_factory("inbox_page", lambda: InboxPage(ctx), section="inbox_page", title="Inbox CRM")

    def _apply_initial_geometry(self) -> None:
        app = QApplication.instance()
        screen = app.primaryScreen() if app is not None else None
        if screen is None:
<<<<<<< HEAD
            self.resize(self.DEFAULT_LAUNCH_WIDTH, self.DEFAULT_LAUNCH_HEIGHT)
            return
        available = screen.availableGeometry()
        margin = max(0, int(self.DEFAULT_LAUNCH_MARGIN))
        max_width = max(self.minimumWidth(), available.width() - margin)
        max_height = max(self.minimumHeight(), available.height() - margin)
        preferred_width = max(self.minimumWidth(), self.DEFAULT_LAUNCH_WIDTH, int(available.width() * 0.94))
        preferred_height = max(self.minimumHeight(), self.DEFAULT_LAUNCH_HEIGHT, int(available.height() * 0.94))
        width = min(max_width, preferred_width)
        height = min(max_height, preferred_height)
=======
            self.resize(1400, 900)
            return
        available = screen.availableGeometry()
        width = max(1180, min(available.width() - 60, int(available.width() * 0.9)))
        height = max(760, min(available.height() - 60, int(available.height() * 0.9)))
>>>>>>> origin/main
        self.resize(width, height)

    def _refresh_status_card(self) -> None:
        self._alias_value.setText(self.state.active_alias)
        self._route_value.setText(self._route_titles.get(self.router.current_route, self.router.current_route or "-"))
        running = self.tasks.running_tasks()
        if running:
            self._tasks_value.setText(", ".join(running))
        else:
            self._tasks_value.setText("0")

    def _active_section(self, route: str) -> str:
        clean_route = self._normalize_route(route)
        return self._route_sections.get(clean_route, clean_route)

    def _sync_sidebar(self, current_route: str) -> None:
        active_section = self._active_section(current_route)
        for route, button in self._sidebar_buttons.items():
            button.set_active(route == active_section)

    def _sync_back_button(self) -> None:
        page = self._created_pages.get(self.router.current_route)
        if page is not None and hasattr(page, "set_back_enabled"):
            page.set_back_enabled(self.router.can_go_back())

    def _on_route_changed(self, route: str) -> None:
        self._sync_sidebar(route)
        self._sync_back_button()
        self._refresh_status_card()

    def _on_history_changed(self, _can_go_back: bool) -> None:
        self._sync_back_button()
        self._refresh_status_card()

    def open_route(
        self,
        route: str,
        *,
        payload: Any = None,
        remember: bool = True,
        clear_history: bool = False,
    ) -> None:
        clean_route = self._normalize_route(route)
        if not self._is_owner_mode() and self._is_system_route(clean_route):
            clean_route = "dashboard"
        self._ensure_page(clean_route)
        self.router.navigate(clean_route, payload=payload, remember=remember, clear_history=clear_history)

    def go_back(self) -> None:
        self.router.go_back()

    def is_sidebar_visible(self) -> bool:
        return bool(self._sidebar is not None and self._sidebar.isVisible())

    def set_sidebar_visible(self, visible: bool) -> None:
        if self._sidebar is None:
            return
        self._sidebar.setVisible(bool(visible))
        if visible:
            self._sidebar.setFixedWidth(self._sidebar_default_width)

    def toggle_sidebar(self) -> None:
        self.set_sidebar_visible(not self.is_sidebar_visible())

    def _build_heartbeat_snapshot(self) -> dict[str, Any]:
        db_ok = True
        try:
            self.services.state_store.sync_foundation()
        except Exception:
            db_ok = False
        try:
            accounts_count = len(self.services.accounts.list_accounts(None))
        except Exception:
            accounts_count = 0
        try:
            proxy_integrity = self.services.accounts.proxy_integrity_summary()
        except Exception:
            proxy_integrity = {}
        try:
            inbox_diag = self.services.inbox.diagnostics()
        except Exception:
            inbox_diag = {}
        runtime_state = runtime_health_snapshot()
        return {
            "app_version": os.environ.get("APP_VERSION") or "unknown",
            "accounts_count": accounts_count,
            "proxy_total": int(proxy_integrity.get("total") or 0),
            "proxy_quarantined": int(proxy_integrity.get("quarantined") or 0),
            "proxy_invalid_assignments": int(proxy_integrity.get("invalid_assignments") or 0),
            "proxy_last_sweep_at": str(self._last_proxy_housekeeping.get("finished_at") or ""),
            "proxy_last_sweep_checked": int(self._last_proxy_housekeeping.get("checked") or 0),
            "proxy_last_sweep_failed": int(self._last_proxy_housekeeping.get("failed") or 0),
            "active_workers": len(self.tasks.running_tasks()) + int(inbox_diag.get("worker_count") or 0),
            "startup_ok": True,
            "db_ok": db_ok,
            "runtime_ok": bool(runtime_state.get("runtime_ok", True)),
            "last_error_code": runtime_state.get("last_error_code") or "",
            "last_error_message": runtime_state.get("last_error_message") or "",
        }

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = os.environ.get(name)
        if raw is None:
            return bool(default)
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
        raw = os.environ.get(name)
        try:
            value = int(str(raw).strip()) if raw is not None else int(default)
        except Exception:
            value = int(default)
        return max(int(minimum), value)

    def _schedule_proxy_health_sweep(self) -> None:
        if not self._env_bool("PROXY_HEALTH_SWEEP_ENABLED", True):
            return
        if self.tasks.is_running("proxy_housekeeping"):
            return
        active_tasks = [name for name in self.tasks.running_tasks() if name != "proxy_housekeeping"]
        if active_tasks:
            return
        sweep_limit = self._env_int("PROXY_HEALTH_SWEEP_MAX_PROXIES", 5, minimum=1)
        stale_after_seconds = self._env_int("PROXY_HEALTH_SWEEP_STALE_SECONDS", 1800, minimum=60)

        def _runner() -> dict[str, Any]:
            result = self.services.accounts.sweep_proxy_health(
                only_assigned=True,
                active_only=True,
                limit=sweep_limit,
                stale_after_seconds=float(stale_after_seconds),
                source="startup_housekeeping",
            )
            self._last_proxy_housekeeping = dict(result or {})
            checked = int(result.get("checked") or 0)
            failed = int(result.get("failed") or 0)
            if checked > 0:
                logger.info(
                    "Proxy housekeeping sweep completed | checked=%s failed=%s",
                    checked,
                    failed,
                )
            return result

        try:
            self.tasks.start_task(
                "proxy_housekeeping",
                _runner,
                metadata={"scope": "assigned_active", "kind": "proxy_housekeeping"},
            )
        except Exception as exc:
            logger.warning("Proxy housekeeping sweep no pudo iniciarse: %s", exc)

    def _start_proxy_housekeeping(self) -> None:
        if not self._env_bool("PROXY_HEALTH_SWEEP_ENABLED", True):
            return
        interval_seconds = self._env_int("PROXY_HEALTH_SWEEP_INTERVAL_SECONDS", 1800, minimum=300)
        if self._proxy_housekeeping_timer is None:
            timer = QTimer(self)
            timer.timeout.connect(self._schedule_proxy_health_sweep)
            self._proxy_housekeeping_timer = timer
        self._proxy_housekeeping_timer.setInterval(interval_seconds * 1000)
        if not self._proxy_housekeeping_timer.isActive():
            self._proxy_housekeeping_timer.start()
        QTimer.singleShot(15_000, self._schedule_proxy_health_sweep)

    def start_startup_housekeeping(self) -> None:
        self._refresh_status_card()
        post_show_hydration = getattr(self.services, "start_post_show_hydration", None)
        if callable(post_show_hydration):
            post_show_hydration()
        if self.mode == "client" and self._heartbeat_client is None:
            self._heartbeat_client = HeartbeatClient(self._build_heartbeat_snapshot)
            self._heartbeat_client.start()
        self._start_proxy_housekeeping()

    def closeEvent(self, event: QCloseEvent) -> None:  # type: ignore[override]
        if self._heartbeat_client is not None:
            self._heartbeat_client.stop()
            self._heartbeat_client = None
        if self._proxy_housekeeping_timer is not None:
            self._proxy_housekeeping_timer.stop()
        try:
            self.services.inbox.set_ui_active(False)
        except Exception:
            pass
        self.queries.shutdown()
        try:
            self.services.accounts.shutdown_manual_sessions()
        except Exception:
            pass
        self.tasks.shutdown("application closing")
        try:
            self.services.warmup.pause_active_flows("application closing")
        except Exception:
            pass
        try:
            self.services.inbox.shutdown()
        except Exception:
            pass
        super().closeEvent(event)
