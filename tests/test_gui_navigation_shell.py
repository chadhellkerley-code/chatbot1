import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QMainWindow, QPushButton, QStackedWidget, QWidget

from application.services import build_application_services
from gui.error_handling import app_log_path, fault_log_path
from gui.main_window import MainWindow


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _window() -> MainWindow:
    _app()
    services = build_application_services(ROOT)
    return MainWindow(mode="owner", services=services)


def _section_subnav_buttons(page: QWidget) -> list[QPushButton]:
    return [
        button
        for button in page.findChildren(QPushButton)
        if button.objectName() == "SectionSubnavButton"
    ]


def test_main_window_uses_single_stacked_router_and_qwidgets():
    window = _window()
    try:
        assert isinstance(window, QMainWindow)
        assert isinstance(window.router.stack, QStackedWidget)
        assert window.centralWidget() is not None
        assert window.router.current_route == "dashboard"
        assert len(window.pages) >= 19
        assert "accounts_actions_page" in list(window.pages)
        assert all(isinstance(page, QWidget) for page in window.pages.values())
        assert not any(
            isinstance(child, QMainWindow) and child is not window
            for child in window.findChildren(QMainWindow)
        )
        subtitles = [
            child.text()
            for child in window.findChildren(QLabel)
            if child.objectName() == "BrandHeaderSub"
        ]
        assert subtitles == ["Automation Platform"]
    finally:
        window.close()


def test_navigation_history_reuses_page_instances():
    window = _window()
    try:
        accounts_page = window.pages["accounts_page"]
        alias_page = window.pages["alias_page"]

        window.open_route("accounts_home")
        window.open_route("accounts_page")
        window.open_route("alias_page")

        assert window.router.current_route == "alias_page"
        assert window.router.can_go_back() is True
        assert window.pages["accounts_page"] is accounts_page
        assert window.pages["alias_page"] is alias_page

        window.go_back()
        assert window.router.current_route == "accounts_page"

        window.go_back()
        assert window.router.current_route == "accounts_home"

        window.go_back()
        assert window.router.current_route == "dashboard"
    finally:
        window.close()


def test_accounts_actions_route_opens_real_actions_page():
    window = _window()
    try:
        window.open_route("accounts_actions_page")
        assert window.router.current_route == "accounts_actions_page"
        assert window.pages["accounts_actions_page"] is not window.pages["accounts_page"]
    finally:
        window.close()


def test_system_diagnostics_timer_only_runs_when_page_is_visible():
    window = _window()
    try:
        diagnostics_page = window.pages["system_diagnostics_page"]

        assert diagnostics_page._timer.isActive() is False

        window.open_route("system_diagnostics_page")
        assert diagnostics_page._timer.isActive() is True

        window.open_route("dashboard")
        assert diagnostics_page._timer.isActive() is False
    finally:
        window.close()


def test_main_window_closes_manual_sessions_on_shutdown():
    window = _window()
    calls: list[str] = []
    try:
        window.services.accounts.shutdown_manual_sessions = lambda: calls.append("shutdown")  # type: ignore[method-assign]
        window.close()
        assert calls == ["shutdown"]
    finally:
        if window.isVisible():
            window.close()


def test_modular_sections_share_horizontal_subnav_pattern():
    window = _window()
    try:
        expected = {
            "accounts_home": ["Alias", "Cuentas", "Proxies", "Acciones"],
            "leads_home": ["Listas", "Plantillas", "Importar", "Filtrado"],
            "campaigns_home": ["Crear", "Monitor", "Historial"],
            "automation_home": ["Config", "Autoresponder", "Packs", "Flow", "WhatsApp"],
            "system_home": ["Licencias", "Logs", "Config", "Diagnostico"],
        }

        for route, labels in expected.items():
            window.open_route(route)
            page = window.pages[route]
            assert [button.text() for button in _section_subnav_buttons(page)] == labels
    finally:
        window.close()


def test_modular_sections_mark_active_subnav_on_inner_pages():
    window = _window()
    try:
        cases = {
            "accounts_page": "Cuentas",
            "accounts_actions_page": "Acciones",
            "leads_filter_page": "Filtrado",
            "campaign_monitor_page": "Monitor",
            "automation_packs_page": "Packs",
            "system_logs_page": "Logs",
        }

        for route, active_label in cases.items():
            window.open_route(route)
            page = window.pages[route]
            active = [
                button.text()
                for button in _section_subnav_buttons(page)
                if bool(button.property("active"))
            ]
            assert active == [active_label]
    finally:
        window.close()


def test_legacy_cli_navigation_files_removed():
    assert not (ROOT / "gui" / "io_adapter.py").exists()
    assert not (ROOT / "gui" / "navigation" / "navigation_controller.py").exists()
    assert not (ROOT / "gui" / "navigation" / "routes.py").exists()

    gui_app_source = (ROOT / "gui" / "gui_app.py").read_text(encoding="utf-8")
    main_window_source = (ROOT / "gui" / "main_window.py").read_text(encoding="utf-8")
    router_source = (ROOT / "gui" / "navigation" / "router.py").read_text(encoding="utf-8")

    assert "IOAdapter" not in gui_app_source
    assert "ensure_legacy_backend_running" not in gui_app_source
    assert "IOAdapter" not in main_window_source
    assert "setCurrentWidget" in router_source


def test_styles_and_logging_contracts_are_present(tmp_path):
    styles_source = (ROOT / "styles.qss").read_text(encoding="utf-8")
    assert "QLineEdit," in styles_source
    assert "QTextEdit," in styles_source
    assert "QComboBox," in styles_source
    assert "QSpinBox," in styles_source
    assert "QTableWidget," in styles_source
    assert "QPushButton {" in styles_source

    log_path = app_log_path(tmp_path)
    assert log_path.name == "app.log"
    assert log_path.parent.name == "logs"
    assert fault_log_path(tmp_path).name == "fault.log"
