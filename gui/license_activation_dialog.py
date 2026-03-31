from __future__ import annotations

import os
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from gui.query_runner import QueryManager


class LicenseDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Activar licencia")
        self.setModal(True)

        layout = QVBoxLayout()

        self.input = QLineEdit()
        self.input.setPlaceholderText("Ingresar license key")
        self.input.returnPressed.connect(self.accept)

        self.btn = QPushButton("Activar")
        self.btn.clicked.connect(self.accept)

        layout.addWidget(QLabel("Licencia requerida"))
        layout.addWidget(self.input)
        layout.addWidget(self.btn)

        self.setLayout(layout)

    def get_key(self) -> str:
        return str(self.input.text() or "").strip()


class LicenseActivationDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Activar licencia")
        self.setModal(True)
        self.setMinimumWidth(420)

        self._queries = QueryManager(self, max_thread_count=1)
        self._request_id = 0
        self._context: "LicenseRuntimeContext | None" = None

        title = QLabel("Activación de licencia")
        title.setTextInteractionFlags(Qt.TextSelectableByMouse)

        hint = QLabel("Ingresa tu clave de licencia y presiona “Activar”.")
        hint.setWordWrap(True)

        self._input = QLineEdit()
        self._input.setPlaceholderText("XXXX-XXXX-XXXX-XXXX")
        self._input.returnPressed.connect(self._activate)

        self._status = QLabel("")
        self._status.setWordWrap(True)
        self._status.setTextInteractionFlags(Qt.TextSelectableByMouse)

        self._activate_button = QPushButton("Activar")
        self._activate_button.clicked.connect(self._activate)

        self._cancel_button = QPushButton("Cancelar")
        self._cancel_button.clicked.connect(self.reject)

        row = QHBoxLayout()
        row.addWidget(self._activate_button)
        row.addWidget(self._cancel_button)

        layout = QVBoxLayout()
        layout.addWidget(title)
        layout.addWidget(hint)
        layout.addWidget(self._input)
        layout.addWidget(self._status)
        layout.addLayout(row)
        self.setLayout(layout)

        self._set_busy(False)

    @property
    def context(self) -> "LicenseRuntimeContext | None":
        return self._context

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self._queries.shutdown(wait_ms=2000)
        except Exception:
            pass
        super().closeEvent(event)

    def _set_busy(self, busy: bool) -> None:
        self._input.setEnabled(not busy)
        self._activate_button.setEnabled(not busy)
        self._cancel_button.setEnabled(True)
        if busy:
            self._activate_button.setText("Activando...")
        else:
            self._activate_button.setText("Activar")

    def _activate(self) -> None:
        if self._request_id:
            return
        license_key = str(self._input.text() or "").strip().upper()
        self._input.setText(license_key)
        if not license_key:
            self._status.setText("Ingresa una clave de licencia.")
            return
        self._status.setText("")
        self._set_busy(True)

        app_version = str(
            os.environ.get("APP_VERSION") or os.environ.get("CLIENT_VERSION") or ""
        ).strip()

        def _target() -> dict[str, Any]:
            try:
                from license_client import activate_and_cache_license

                context = activate_and_cache_license(license_key, app_version=app_version)
            except Exception as exc:
                from license_client import LicenseStartupError, format_license_user_message, license_failure_reason

                if isinstance(exc, LicenseStartupError):
                    return {
                        "ok": False,
                        "code": exc.code,
                        "message": format_license_user_message(exc),
                        "reason": license_failure_reason(exc.code),
                    }
                return {"ok": False, "code": "activation_failed", "message": str(exc)}
            return {"ok": True, "context": context}

        self._request_id = self._queries.submit(
            _target,
            on_success=lambda request_id, payload: self._on_done(request_id, payload),
            on_error=lambda request_id, error: self._on_query_error(request_id, error),
        )

    def _on_done(self, request_id: int, payload: Any) -> None:
        if request_id != self._request_id:
            return
        self._request_id = 0
        self._set_busy(False)
        if isinstance(payload, dict) and payload.get("ok") is True:
            context = payload.get("context")
            from license_client import LicenseRuntimeContext

            if isinstance(context, LicenseRuntimeContext):
                self._context = context
                self.accept()
                return
            self._status.setText("No se pudo completar la activación.")
            return
        message = ""
        reason = ""
        if isinstance(payload, dict):
            message = str(payload.get("message") or "")
            reason = str(payload.get("reason") or "")
        if reason:
            self._status.setText(message)
        else:
            self._status.setText(message or "No se pudo activar la licencia.")

    def _on_query_error(self, request_id: int, error) -> None:
        if request_id != self._request_id:
            return
        self._request_id = 0
        self._set_busy(False)
        try:
            message = str(getattr(error, "message", "") or "")
        except Exception:
            message = ""
        QMessageBox.critical(self, "InstaCRM", message or "No se pudo activar la licencia.")
