import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QPushButton, QWidget

from application.services import build_application_services
from gui.main_window import MainWindow


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _app_with_stylesheet() -> QApplication:
    app = _app()
    if not app.styleSheet():
        app.setStyleSheet((ROOT / "styles.qss").read_text(encoding="utf-8"))
    return app


def _window() -> MainWindow:
    _app_with_stylesheet()
    services = build_application_services(ROOT)
    return MainWindow(mode="owner", services=services)


def _close_window(window: MainWindow) -> None:
    window.queries.shutdown()
    window.close()


def test_leads_routes_are_registered_without_filtrados_pages():
    window = _window()
    try:
        expected_routes = (
            "leads_home",
            "leads_templates_page",
            "leads_lists_page",
            "leads_import_page",
        )
        expected_pages = {route: window.pages[route] for route in expected_routes}

        for route in expected_routes:
            assert route in window.pages
            assert isinstance(window.pages[route], QWidget)
            window.open_route(route)
            assert window.router.current_route == route
            assert window.pages[route] is expected_pages[route]

        assert "leads_filter_page" not in list(window.pages)
        assert "leads_filter_config_page" not in list(window.pages)
    finally:
        _close_window(window)


def test_legacy_leads_page_module_was_removed():
    assert not (ROOT / "gui" / "pages_leads.py").exists()


def test_leads_home_only_exposes_lists_templates_and_import():
    window = _window()
    try:
        page = window.pages["leads_home"]
        button_texts = {button.text() for button in page.findChildren(QPushButton)}
        assert "Filtrado" not in button_texts
        assert "Configuracion de filtros" not in button_texts
        assert set(page._cards) == {"templates", "lists"}
    finally:
        _close_window(window)


def test_lists_panel_only_manages_lists_without_filter_results():
    window = _window()
    try:
        panel = window.pages["leads_lists_page"]._panel
        button_texts = {button.text() for button in panel.findChildren(QPushButton)}
        assert "Guardar lista" in button_texts
        assert "Agregar usernames" in button_texts
        assert "Reutilizar filtrado" not in button_texts
        assert "Reanudar filtrado" not in button_texts
        assert hasattr(panel, "_completed_table") is False
        assert hasattr(panel, "_incomplete_table") is False
        assert hasattr(panel, "_detail_preview") is False
    finally:
        _close_window(window)


def test_leads_module_uses_dark_dialog_helpers_only():
    common_source = (ROOT / "gui" / "modules" / "leads" / "common.py").read_text(encoding="utf-8")
    import_source = (ROOT / "gui" / "modules" / "leads" / "import_panel.py").read_text(encoding="utf-8")
    leads_source = (ROOT / "gui" / "modules" / "leads" / "leads_page.py").read_text(encoding="utf-8")

    assert "QMessageBox" not in common_source
    assert "getOpenFileName(" not in import_source
    assert "leads_filter_page" not in leads_source
    assert "LeadsFilter" not in leads_source
