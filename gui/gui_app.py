from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Literal, Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bootstrap import ensure_bootstrapped, record_system_event, start_post_show_bootstrap_tasks
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication, QDialog, QMessageBox

from gui.error_handling import configure_logging, install_exception_hooks
from gui.main_window import MainWindow
from license_client import (
    LicenseStartupError,
    format_license_user_message,
    license_failure_reason,
)
from runtime.runtime_parity import bootstrap_runtime_env

ExecutionMode = Literal["owner", "client"]


def _read_stylesheet() -> str:
    candidates: list[Path] = []
    bootstrap_app_root = (os.environ.get("INSTACRM_APP_ROOT") or "").strip()
    if bootstrap_app_root:
        candidates.append(Path(bootstrap_app_root) / "styles.qss")

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.append(Path(meipass) / "styles.qss")

    exe_path = getattr(sys, "executable", "") or ""
    if exe_path:
        try:
            candidates.append(Path(exe_path).resolve().parent / "styles.qss")
        except Exception:
            pass

    candidates.append(Path(__file__).resolve().parent.parent / "styles.qss")
    candidates.append(Path(__file__).resolve().with_name("styles.qss"))

    for path in candidates:
        if not path.exists():
            continue
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            continue
    return ""


def _resolve_mode(mode: Optional[str]) -> ExecutionMode:
    if mode in ("owner", "client"):
        return mode
    argv0 = Path(sys.argv[0]).stem.lower()
    if argv0 == "client_launcher":
        return "client"
    return "owner"


def _run_post_show_startup(
    window: MainWindow,
    *,
    bootstrap_ctx: Any,
    time_to_window_show_seconds: float,
) -> None:
    start_post_show_bootstrap_tasks(
        bootstrap_ctx,
        time_to_window_show_seconds=time_to_window_show_seconds,
    )
    window.start_startup_housekeeping()


def launch_gui_app(
    backend_entrypoint: Optional[Callable[[], Any]] = None,
    *,
    mode: Optional[str] = None,
) -> int:
    os.environ.setdefault("INSTACLI_DISABLE_CONSOLE_CLEAR", "1")

    resolved_mode = _resolve_mode(mode)
    launch_started_at = time.perf_counter()
    bootstrap_ctx = ensure_bootstrapped(resolved_mode, defer_housekeeping=True)
    bootstrap_runtime_env(resolved_mode, app_root_hint=bootstrap_ctx.install_root, force=True)
    log_path = configure_logging(bootstrap_ctx.install_root)
    logger = logging.getLogger("insta_crm.gui")
    record_system_event(
        bootstrap_ctx.install_root,
        "gui_launch_started",
        payload={"mode": resolved_mode, "startup_id": bootstrap_ctx.startup_id},
    )

    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("InstaCRM")
    if not bool(app.property("_insta_crm_lifecycle_hooks_installed")):
        app.aboutToQuit.connect(lambda: logger.info("QApplication aboutToQuit"))
        app.lastWindowClosed.connect(lambda: logger.info("QApplication lastWindowClosed"))
        app.setProperty("_insta_crm_lifecycle_hooks_installed", True)

    stylesheet = _read_stylesheet()
    if stylesheet:
        app.setStyleSheet(stylesheet)

    if callable(backend_entrypoint):
        try:
            backend_entrypoint()
        except LicenseStartupError as exc:
            if exc.code == "license_missing":
                from gui.license_activation_dialog import LicenseActivationDialog

                dialog = LicenseActivationDialog()
                if dialog.exec() == QDialog.Accepted:
                    try:
                        backend_entrypoint()
                    except LicenseStartupError as exc2:
                        logger.error(
                            "License startup validation failed after activation: %s",
                            exc2.detail or exc2.user_message,
                        )
                        QMessageBox.critical(None, "InstaCRM", format_license_user_message(exc2))
                        record_system_event(
                            bootstrap_ctx.install_root,
                            "license_startup_blocked",
                            level="error",
                            payload={
                                "mode": resolved_mode,
                                "startup_id": bootstrap_ctx.startup_id,
                                "code": exc2.code,
                                "reason": license_failure_reason(exc2.code),
                                "detail": exc2.detail,
                            },
                        )
                        return 2
                else:
                    logger.error(
                        "License activation cancelled / missing local key: %s",
                        exc.detail or exc.user_message,
                    )
                    QMessageBox.critical(None, "InstaCRM", format_license_user_message(exc))
                    record_system_event(
                        bootstrap_ctx.install_root,
                        "license_startup_blocked",
                        level="error",
                        payload={
                            "mode": resolved_mode,
                            "startup_id": bootstrap_ctx.startup_id,
                            "code": exc.code,
                            "reason": license_failure_reason(exc.code),
                            "detail": exc.detail,
                        },
                    )
                    return 2
            else:
                logger.error(
                    "License startup validation failed: %s", exc.detail or exc.user_message
                )
                QMessageBox.critical(None, "InstaCRM", format_license_user_message(exc))
                record_system_event(
                    bootstrap_ctx.install_root,
                    "license_startup_blocked",
                    level="error",
                    payload={
                        "mode": resolved_mode,
                        "startup_id": bootstrap_ctx.startup_id,
                        "code": exc.code,
                        "reason": license_failure_reason(exc.code),
                        "detail": exc.detail,
                    },
                )
                return 2
        except Exception as exc:
            logger.exception("Unexpected startup validation failure")
            QMessageBox.critical(
                None,
                "InstaCRM",
                "No se pudo completar la validacion inicial.\nLa aplicacion se cerrara.",
            )
            record_system_event(
                bootstrap_ctx.install_root,
                "startup_validation_failed",
                level="error",
                payload={
                    "mode": resolved_mode,
                    "startup_id": bootstrap_ctx.startup_id,
                    "detail": str(exc),
                },
            )
            return 2

    from application.services import build_application_services

    services = build_application_services(bootstrap_ctx.install_root)
    window = MainWindow(mode=resolved_mode, services=services)
    install_exception_hooks(
        log_append=window.logs.append,
        parent_resolver=lambda: window,
    )
    window.logs.append(f"Logs persistidos en {log_path}\n")
    window.show()
    time_to_window_show_seconds = time.perf_counter() - launch_started_at
    record_system_event(
        bootstrap_ctx.install_root,
        "gui_window_shown",
        payload={
            "mode": resolved_mode,
            "startup_id": bootstrap_ctx.startup_id,
            "time_to_window_show_ms": int(time_to_window_show_seconds * 1000.0),
        },
    )
    QTimer.singleShot(
        0,
        lambda: _run_post_show_startup(
            window,
            bootstrap_ctx=bootstrap_ctx,
            time_to_window_show_seconds=time_to_window_show_seconds,
        ),
    )

    qt_exit = app.exec()
    if window.backend_exit_code is not None:
        return window.backend_exit_code
    return qt_exit


if __name__ == "__main__":
    raise SystemExit(launch_gui_app(mode="owner"))
