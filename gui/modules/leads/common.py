from __future__ import annotations

import logging
import re
import traceback
from pathlib import Path
from typing import Any

from core import leads as leads_module
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFileDialog,
    QFrame,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QVBoxLayout,
    QWidget,
    QDialog,
)

from gui.page_base import BasePage


FILTER_STATE_ITEMS = [
    ("Indispensable", leads_module.FILTER_STATE_REQUIRED),
    ("Indiferente", leads_module.FILTER_STATE_INDIFFERENT),
    ("Desactivar", leads_module.FILTER_STATE_DISABLED),
]

PRIVACY_ITEMS = [
    ("Cualquiera", "any"),
    ("Publica", "public"),
    ("Privada", "private"),
]

LINK_ITEMS = [
    ("Cualquiera", "any"),
    ("Con link", "yes"),
    ("Sin link", "no"),
]

LANGUAGE_ITEMS = [
    ("Cualquiera", "any"),
    ("Espanol", "es"),
    ("Portugues", "pt"),
    ("Ingles", "en"),
]

BROWSER_MODE_ITEMS = [
    ("Headless", True),
    ("Visible", False),
]

TEXT_INFO = (
    "Texto inteligente evalua la bio, nombre y descripcion del perfil. "
    "Usa prompts concretos, evita ambiguedad y describe con claridad el lead ideal."
)

IMAGE_INFO = (
    "Prompt visual evalua atributos detectables en la foto de perfil. "
    "Describe rasgos visibles y evita instrucciones abstractas o contradictorias."
)

FILTER_RESULT_RE = re.compile(
    r"^@(?P<account>[^ ]+)\s+-->\s+@(?P<username>[^ ]+)\s+\(filtrado\)\s+-->\s+"
    r"(?P<result>.+?)\s+-->\s+(?P<reason>.+)$"
)

logger = logging.getLogger(__name__)

_LEADS_MODAL_STYLESHEET = """
QDialog#LeadsModalDialog {
    background-color: #0f1722;
    color: #e7edf6;
    border: 1px solid #223147;
    border-radius: 16px;
}
QLabel#LeadsModalTitle {
    color: #f8fbff;
    font-size: 16px;
    font-weight: 700;
}
QLabel#LeadsModalHint {
    color: #9aa9bc;
}
QFrame#LeadsModalCard {
    background-color: #151f2d;
    border: 1px solid #243246;
    border-radius: 16px;
}
QScrollArea#LeadsModalScroll,
QWidget#LeadsModalScrollViewport {
    background-color: transparent;
    border: none;
}
QWidget#LeadsModalSurface {
    background-color: #151f2d;
    border: none;
    border-radius: 12px;
}
QLabel#LeadsModalFieldLabel {
    background-color: transparent;
    color: #dfe8f4;
    font-weight: 600;
}
QListWidget,
QLineEdit,
QComboBox,
QSpinBox,
QDoubleSpinBox,
QPlainTextEdit {
    background-color: #0c131c;
    color: #e7edf6;
    border: 1px solid #2b3a4f;
    border-radius: 10px;
    padding: 8px;
    selection-background-color: #2563eb;
    selection-color: #f8fbff;
}
QLineEdit:focus,
QComboBox:focus,
QSpinBox:focus,
QDoubleSpinBox:focus,
QPlainTextEdit:focus {
    border-color: #4f83cc;
}
QLineEdit:disabled,
QComboBox:disabled,
QSpinBox:disabled,
QDoubleSpinBox:disabled,
QPlainTextEdit:disabled {
    background-color: #101924;
    border-color: #243246;
    color: #6f8298;
}
QComboBox QAbstractItemView {
    background-color: #111827;
    border: 1px solid #34465f;
    color: #e8edf7;
    selection-background-color: #2563eb;
    selection-color: #f8fbff;
}
QDialogButtonBox {
    background-color: transparent;
}
"""


def template_variants(text: str) -> list[str]:
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def keywords_to_text(values: Any) -> str:
    if not isinstance(values, (list, tuple)):
        return ""
    return "\n".join(str(item).strip() for item in values if str(item).strip())


def text_to_keywords(text: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in str(text or "").splitlines():
        candidate = str(raw or "").strip()
        key = candidate.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)
    return ordered


def filter_state_label(state: str) -> str:
    value = str(state or "").strip().lower()
    if value == leads_module.FILTER_STATE_REQUIRED:
        return "Indispensable"
    if value == leads_module.FILTER_STATE_INDIFFERENT:
        return "Indiferente"
    return "Desactivar"


def browser_mode_label(value: Any) -> str:
    if value is True:
        return "Headless"
    if value is False:
        return "Visible"
    return "Automatico"


def page_host(widget: QWidget | None) -> BasePage | None:
    current = widget
    while current is not None:
        if isinstance(current, BasePage):
            return current
        current = current.parentWidget()
    return None


def set_panel_status(widget: QWidget | None, text: str) -> None:
    host = page_host(widget)
    if host is not None:
        host.set_status(text)


class LeadsModalDialog(QDialog):
    def __init__(self, title: str, subtitle: str = "", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setModal(True)
        self.setObjectName("LeadsModalDialog")
        self.setWindowTitle(title)
        self.resize(560, 0)
        self.setStyleSheet(_LEADS_MODAL_STYLESHEET)

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title_label = QLabel(title)
        title_label.setObjectName("LeadsModalTitle")
        root.addWidget(title_label)

        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setObjectName("LeadsModalHint")
            subtitle_label.setWordWrap(True)
            root.addWidget(subtitle_label)

        body = QFrame()
        body.setObjectName("LeadsModalCard")
        self._body_layout = QVBoxLayout(body)
        self._body_layout.setContentsMargins(18, 18, 18, 18)
        self._body_layout.setSpacing(12)
        root.addWidget(body)

    def body_layout(self) -> QVBoxLayout:
        return self._body_layout


class LeadsAlertDialog(LeadsModalDialog):
    def __init__(
        self,
        title: str,
        message: str,
        *,
        confirm_text: str = "Aceptar",
        cancel_text: str = "",
        danger: bool = False,
        details: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, message, parent=parent)
        body = self.body_layout()
        if details:
            detail_box = QPlainTextEdit(details)
            detail_box.setReadOnly(True)
            detail_box.setMinimumHeight(140)
            body.addWidget(detail_box)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        actions.addStretch(1)

        if cancel_text:
            cancel_button = QPushButton(cancel_text)
            cancel_button.setObjectName("SecondaryButton")
            cancel_button.clicked.connect(self.reject)
            actions.addWidget(cancel_button)

        confirm_button = QPushButton(confirm_text)
        confirm_button.setObjectName("DangerButton" if danger else "PrimaryButton")
        confirm_button.clicked.connect(self.accept)
        actions.addWidget(confirm_button)
        body.addLayout(actions)


def open_dark_file_dialog(parent: QWidget, title: str, file_filter: str) -> str:
    dialog = QFileDialog(parent, title, str(Path.cwd()), file_filter)
    dialog.setFileMode(QFileDialog.ExistingFile)
    dialog.setOption(QFileDialog.DontUseNativeDialog, True)
    dialog.setStyleSheet(
        """
        QFileDialog {
            background-color: #0f1722;
            color: #e7edf6;
        }
        QFileDialog QListView,
        QFileDialog QTreeView,
        QFileDialog QLineEdit,
        QFileDialog QComboBox {
            background-color: #0c131c;
            color: #e7edf6;
            border: 1px solid #2b3a4f;
            border-radius: 8px;
            padding: 6px;
        }
        QFileDialog QPushButton {
            min-height: 34px;
        }
        """
    )
    if dialog.exec() != QDialog.Accepted:
        return ""
    selected = dialog.selectedFiles()
    return str(selected[0] if selected else "")


def show_panel_error(widget: QWidget | None, text: str) -> None:
    host = page_host(widget)
    if host is not None:
        host.set_status(str(text or "Error"))
    dialog = LeadsAlertDialog("Error", str(text or "Error"), parent=widget or host)
    dialog.exec()


def show_panel_exception(widget: QWidget | None, exc: BaseException, user_message: str) -> None:
    host = page_host(widget)
    if host is not None:
        host.set_status(user_message)
        try:
            host._ctx.logs.append("[error] GUI action failed\n")
            host._ctx.logs.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        except Exception:
            pass
    logger.error("Leads GUI action failed", exc_info=(type(exc), exc, exc.__traceback__))
    dialog = LeadsAlertDialog(
        "Error",
        str(user_message or "Error"),
        details="".join(traceback.format_exception_only(type(exc), exc)).strip(),
        parent=widget or host,
    )
    dialog.exec()


def configure_data_table(table: QTableWidget, *, primary_column: int = 0) -> None:
    table.setAlternatingRowColors(True)
    header = table.horizontalHeader()
    for column in range(table.columnCount()):
        mode = QHeaderView.Stretch if column == primary_column else QHeaderView.ResizeToContents
        header.setSectionResizeMode(column, mode)
    header.setStretchLastSection(False)


def format_filter_log_line(line: str) -> str | None:
    text = str(line or "").strip()
    if not text:
        return None
    match = FILTER_RESULT_RE.match(text)
    if match:
        result_label = str(match.group("result") or "").strip().upper()
        return (
            f"{match.group('account')} analizando perfil @{match.group('username')}\n"
            f"Resultado: {result_label}\n"
            f"Motivo: {match.group('reason')}"
        )
    prefixes = (
        "progreso:",
        "fin de filtrado:",
        "imagenes:",
        "preparando sesiones:",
        "cuentas listas:",
        "cuentas disponibles:",
        "sesion no disponible para",
        "no se pudo iniciar session de",
    )
    lowered = text.lower()
    if any(lowered.startswith(prefix) for prefix in prefixes):
        return text.capitalize()
    if "[task:leads_filter]" in lowered:
        return text
    return None
