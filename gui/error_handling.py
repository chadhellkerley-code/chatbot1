from __future__ import annotations

import faulthandler
import logging
import os
import sys
import threading
import traceback
from pathlib import Path
from types import TracebackType
from typing import Callable

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication, QMessageBox, QWidget
from paths import logs_root


DEFAULT_ERROR_MESSAGE = "Error cargando modulo. Ver logs para mas detalles."
_ERROR_DIALOG_LOCK = threading.RLock()
_ERROR_DIALOG_VISIBLE = False
_FAULT_HANDLER_STREAM = None


def app_log_path(root_dir: Path) -> Path:
    log_dir = logs_root(Path(root_dir))
    return log_dir / "app.log"


def fault_log_path(root_dir: Path) -> Path:
    log_dir = logs_root(Path(root_dir))
    return log_dir / "fault.log"


def configure_logging(root_dir: Path) -> Path:
    global _FAULT_HANDLER_STREAM
    path = app_log_path(root_dir)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in root_logger.handlers:
        if getattr(handler, "_insta_crm_app_log", False):
            return path

    handler = logging.FileHandler(path, encoding="utf-8")
    handler._insta_crm_app_log = True  # type: ignore[attr-defined]
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    root_logger.addHandler(handler)
    logging.captureWarnings(True)
    try:
        fault_path = fault_log_path(root_dir)
        if _FAULT_HANDLER_STREAM is None or getattr(_FAULT_HANDLER_STREAM, "closed", True):
            _FAULT_HANDLER_STREAM = fault_path.open("a", encoding="utf-8")
        if not faulthandler.is_enabled():
            faulthandler.enable(_FAULT_HANDLER_STREAM, all_threads=True)
    except Exception:
        pass
    return path


def log_exception(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType | None,
    *,
    message: str,
    log_append: Callable[[str], None] | None = None,
) -> None:
    logger = logging.getLogger("insta_crm.gui")
    logger.error(
        message,
        exc_info=(exc_type, exc_value, exc_traceback),
    )
    install_root = (os.environ.get("INSTACRM_INSTALL_ROOT") or "").strip()
    if install_root:
        try:
            from bootstrap import record_critical_error

            record_critical_error(
                Path(install_root),
                "gui_unhandled_exception",
                error=exc_value,
                message=message,
                payload={"exception_type": exc_type.__name__},
            )
        except Exception:
            pass
    try:
        from src.telemetry import report_playwright_crash, report_runtime_error

        error_text = f"{message} {exc_value}".lower()
        payload = {
            "exception_type": exc_type.__name__,
            "message": message,
        }
        if any(token in error_text for token in ("playwright", "pw-", "driver_crash")):
            report_playwright_crash(str(exc_value) or message, payload=payload)
        else:
            report_runtime_error(str(exc_value) or message, payload=payload)
    except Exception:
        pass
    if log_append is not None:
        try:
            log_append(f"[error] {message}\n")
            log_append("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        except Exception:
            pass


def show_error_dialog(
    message: str = DEFAULT_ERROR_MESSAGE,
    *,
    parent: QWidget | None = None,
) -> None:
    app = QApplication.instance()
    if app is None:
        return

    def _show() -> None:
        global _ERROR_DIALOG_VISIBLE
        with _ERROR_DIALOG_LOCK:
            if _ERROR_DIALOG_VISIBLE:
                return
            _ERROR_DIALOG_VISIBLE = True
        try:
            QMessageBox.critical(parent or QApplication.activeWindow(), "Error", message)
        finally:
            with _ERROR_DIALOG_LOCK:
                _ERROR_DIALOG_VISIBLE = False

    QTimer.singleShot(0, _show)


def install_exception_hooks(
    *,
    log_append: Callable[[str], None] | None = None,
    parent_resolver: Callable[[], QWidget | None] | None = None,
    user_message: str = DEFAULT_ERROR_MESSAGE,
) -> None:
    previous_sys_hook = sys.excepthook
    previous_thread_hook = getattr(threading, "excepthook", None)

    def _dialog_parent() -> QWidget | None:
        if callable(parent_resolver):
            try:
                return parent_resolver()
            except Exception:
                return None
        return None

    def _handle(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
        *,
        source: str,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            if source == "sys" and callable(previous_sys_hook):
                previous_sys_hook(exc_type, exc_value, exc_traceback)
            return
        log_exception(
            exc_type,
            exc_value,
            exc_traceback,
            message=f"Unhandled GUI exception ({source})",
            log_append=log_append,
        )
        show_error_dialog(user_message, parent=_dialog_parent())

    def _sys_hook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        _handle(exc_type, exc_value, exc_traceback, source="sys")

    def _thread_hook(args: threading.ExceptHookArgs) -> None:
        _handle(args.exc_type, args.exc_value, args.exc_traceback, source="thread")

    sys.excepthook = _sys_hook
    if previous_thread_hook is not None:
        threading.excepthook = _thread_hook
