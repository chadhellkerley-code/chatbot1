import os
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QDialogButtonBox, QLabel, QPushButton, QScrollArea, QWidget

from application.services import build_application_services
from gui.main_window import MainWindow
from gui.modules.leads.filter_config_panel import IMAGE_THRESHOLD_FIELDS, TEXT_THRESHOLD_FIELDS, _ThresholdDialog


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


def test_leads_routes_are_registered_and_navigate_with_single_instances():
    window = _window()
    try:
        expected_routes = (
            "leads_home",
            "leads_templates_page",
            "leads_lists_page",
            "leads_import_page",
            "leads_filter_page",
        )
        expected_pages = {route: window.pages[route] for route in expected_routes}

        for route in expected_routes:
            assert route in window.pages
            assert isinstance(window.pages[route], QWidget)
            window.open_route(route)
            assert window.router.current_route == route
            assert window.pages[route] is expected_pages[route]

        assert "leads_filter_config_page" not in list(window.pages)
    finally:
        _close_window(window)


def test_legacy_leads_page_module_was_removed():
    assert not (ROOT / "gui" / "pages_leads.py").exists()


def test_filter_runner_refresh_uses_cached_run_payload():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._running_list_id = "run-1"
        panel._running_run_payload = {
            "alias": "demo",
            "accounts": ["alpha", "beta"],
            "headless": False,
            "delay_min": 10.0,
            "delay_max": 20.0,
            "concurrency": 1,
            "source_list": "seed-list",
            "export_alias": "demo_filtrados",
        }
        panel._running_started_at = time.monotonic() - 5.0
        panel._running_processed_baseline = 0

        row = {
            "id": "run-1",
            "source_list": "seed-list",
            "processed": 4,
            "qualified": 2,
            "discarded": 1,
            "pending": 6,
            "total": 10,
        }

        panel._apply_execution_row(row)

        assert panel._processed_value.text() == "4"
        assert panel._qualified_value.text() == "2"
        assert panel._discarded_value.text() == "1"
        assert panel._errors_value.text() == "0"
        assert panel._total_value.text() == "10"
        assert panel._source_value.text() == "seed-list"
        assert panel._execution_status.text() == "Procesando perfiles"
        assert "Alias usado: demo" in panel._running_summary.text()
        assert panel._accounts_value.text() == "1"
        assert "Workers activos: 1" in panel._running_summary.text()
        assert "Modo: Visible" in panel._running_summary.text()
        assert "4 de 10 perfiles procesados" in panel._monitor_view.progress_detail_label.text()
    finally:
        _close_window(window)


def test_filter_runner_contains_config_activation_and_results_panels():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._show_idle_section("landing")
        button_texts = {button.text() for button in panel.findChildren(QPushButton)}
        assert panel._idle_pages.currentWidget() is panel._landing_view
        assert any(text.startswith("Activacion de filtrado") for text in button_texts)
        assert any(text.startswith("Configuracion de filtrado") for text in button_texts)
        assert any(text.startswith("Resultados de filtrado") for text in button_texts)
        assert "Pausar filtrado" in button_texts
        assert "Detener filtrado" in button_texts
        assert hasattr(panel, "_completed_table") is True
        assert hasattr(panel, "_incomplete_table") is True
        assert hasattr(panel, "_config_panel") is True
        assert panel._activation_view.objectName() == "SubmenuScroll"
        assert panel._config_view.objectName() == "SubmenuScroll"
        assert panel._results_view.objectName() == "SubmenuScroll"
        assert "Seleccionar todas" not in button_texts
        assert "Limpiar seleccion" not in button_texts
        label_texts = {label.text() for label in panel.findChildren(QLabel)}
        assert "Tiempo maximo (s)" not in label_texts
    finally:
        _close_window(window)


def test_filter_runner_activation_uses_automatic_read_only_concurrency():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._alias_account_rows = [
            {"username": f"cuenta_{index}", "proxy": f"proxy_{index % 5}"}
            for index in range(23)
        ]
        panel._show_idle_section("activation")
        panel._refresh_activation_summary()

        assert panel._idle_pages.currentWidget() is panel._activation_view
        assert panel._concurrency.isReadOnly() is True
        assert panel._concurrency.text() == "5"
        assert "Concurrencia aplicada: 5" in panel._capacity_summary.text()
    finally:
        _close_window(window)


def test_filter_runner_activation_without_proxies_uses_single_local_worker():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._alias_account_rows = [
            {"username": f"cuenta_{index}", "proxy": ""}
            for index in range(3)
        ]
        panel._show_idle_section("activation")
        panel._refresh_activation_summary()

        assert panel._concurrency.text() == "1"
        assert "Proxies detectados: 0" in panel._capacity_summary.text()
        assert "1 worker local con rotacion de cuentas" in panel._capacity_summary.text()
    finally:
        _close_window(window)


def test_filter_runner_reopens_monitor_when_returning_to_filtrado():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._running_list_id = "run-1"
        panel._running_run_payload = {"alias": "demo", "accounts": ["a", "b"], "concurrency": 2}
        panel._request_page_refresh = lambda: None
        panel._refresh_execution_panel = lambda: None

        panel.on_navigate_from()
        panel.on_navigate_to()

        assert panel._view_stack.currentWidget() is panel._running_view
    finally:
        _close_window(window)


def test_filter_runner_stop_request_persists_status_until_task_finishes():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._running_list_id = "run-1"
        panel._running_run_payload = {
            "alias": "demo",
            "accounts": ["alpha", "beta"],
            "headless": True,
            "delay_min": 6.0,
            "delay_max": 21.0,
            "concurrency": 1,
            "source_list": "seed-list",
            "export_alias": "demo_filtrados",
        }
        stop_calls: list[tuple[str, object]] = []

        def _stop_filtering(reason, *, task_runner=None):
            stop_calls.append((str(reason), task_runner))

        panel._ctx.services.leads.stop_filtering = _stop_filtering

        panel._stop_filtering()
        panel._apply_execution_row(
            {
                "id": "run-1",
                "source_list": "seed-list",
                "processed": 4,
                "qualified": 2,
                "discarded": 1,
                "pending": 6,
                "total": 10,
                "errors": 0,
            }
        )

        assert stop_calls == [("stop requested from leads runner panel", panel._ctx.tasks)]
        assert panel._stop_requested is True
        assert panel._execution_status.text() == "Deteniendo filtrado y cerrando workers..."
        assert panel._monitor_view.stop_button.text() == "Deteniendo..."
    finally:
        _close_window(window)


def test_filter_runner_log_flush_keeps_one_line_per_event():
    window = _window()
    try:
        panel = window.pages["leads_filter_page"]._panel
        panel._running_list_id = "run-1"
        panel._log_buffer = "progreso: procesadas=1 | pendientes=4\nimagenes: procesadas=0 | omitidas=0\n"

        panel._flush_log_buffer()

        lines = [line for line in panel._log_box.toPlainText().splitlines() if line.strip()]
        assert len(lines) == 2
        assert lines[0].startswith("Progreso: procesadas=1")
        assert lines[1].startswith("Imagenes: procesadas=0")
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
    assert "QMessageBox" not in common_source
    assert "getOpenFileName(" not in import_source


def test_filter_threshold_dialogs_render_with_dark_scroll_surface():
    app = _app_with_stylesheet()
    cases = (
        ("Smart text thresholds", TEXT_THRESHOLD_FIELDS, {"embeddings_threshold": 0.55}),
        ("Visual prompt thresholds", IMAGE_THRESHOLD_FIELDS, {"gender_prob_threshold": 0.72}),
    )

    for title, fields, payload in cases:
        dialog = _ThresholdDialog(title, fields, payload)
        try:
            dialog.resize(520, 540)
            dialog.show()
            app.processEvents()

            scroll = dialog.findChild(QScrollArea, "LeadsModalScroll")
            assert scroll is not None
            assert scroll.viewport().objectName() == "LeadsModalScrollViewport"

            viewport_image = scroll.viewport().grab().toImage()
            sample = viewport_image.pixelColor(16, max(16, viewport_image.height() - 16))
            assert max(sample.red(), sample.green(), sample.blue()) < 80, title

            buttons = dialog.findChild(QDialogButtonBox)
            assert buttons is not None
            ok_button = buttons.button(QDialogButtonBox.Ok)
            cancel_button = buttons.button(QDialogButtonBox.Cancel)
            assert ok_button is not None and ok_button.objectName() == "PrimaryButton"
            assert cancel_button is not None and cancel_button.objectName() == "SecondaryButton"
        finally:
            dialog.close()
