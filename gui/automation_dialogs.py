from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QPlainTextEdit,
    QVBoxLayout,
    QWidget,
)


class AutomationModalDialog(QDialog):
    def __init__(self, title: str, subtitle: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setModal(True)
        self.setObjectName("AutomationModalDialog")
        self.setWindowTitle(title)
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.resize(620, 0)
        self.setStyleSheet(
            """
            QDialog#AutomationModalDialog {
                background-color: #0f1722;
                color: #e7edf6;
            }
            QLabel#AutomationModalTitle {
                color: #f8fbff;
                font-size: 16px;
                font-weight: 700;
            }
            QLabel#AutomationModalHint {
                color: #9aa9bc;
            }
            QLabel#AutomationSectionLabel {
                color: #dfe8f4;
                font-weight: 600;
            }
            QLineEdit,
            QPlainTextEdit {
                background-color: #0c131c;
                color: #e7edf6;
                border: 1px solid #2b3a4f;
                border-radius: 8px;
                padding: 8px;
                selection-background-color: #2f6fed;
            }
            QPlainTextEdit {
                min-height: 120px;
            }
            """
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title_label = QLabel(title)
        title_label.setObjectName("AutomationModalTitle")
        root.addWidget(title_label)

        if subtitle:
            hint = QLabel(subtitle)
            hint.setObjectName("AutomationModalHint")
            hint.setWordWrap(True)
            root.addWidget(hint)

        self._body = QVBoxLayout()
        self._body.setContentsMargins(0, 0, 0, 0)
        self._body.setSpacing(10)
        root.addLayout(self._body)

    def body_layout(self) -> QVBoxLayout:
        return self._body

    def add_buttons(
        self,
        *,
        confirm_text: str,
        cancel_text: str = "Cerrar",
        danger: bool = False,
        accept_enabled: bool = True,
    ) -> QPushButton:
        row = QHBoxLayout()
        row.setContentsMargins(0, 4, 0, 0)
        row.setSpacing(8)
        row.addStretch(1)
        if cancel_text:
            cancel_button = QPushButton(cancel_text)
            cancel_button.setObjectName("SecondaryButton")
            cancel_button.clicked.connect(self.reject)
            row.addWidget(cancel_button)
        confirm_button = QPushButton(confirm_text)
        confirm_button.setObjectName("DangerButton" if danger else "PrimaryButton")
        confirm_button.setEnabled(bool(accept_enabled))
        confirm_button.clicked.connect(self.accept)
        row.addWidget(confirm_button)
        self._body.addLayout(row)
        return confirm_button


class AutomationTextInputDialog(AutomationModalDialog):
    def __init__(
        self,
        *,
        title: str,
        subtitle: str,
        label: str,
        value: str = "",
        multiline: bool = False,
        confirm_text: str = "Guardar",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, subtitle, parent)
        self._multiline = bool(multiline)
        label_widget = QLabel(label)
        label_widget.setObjectName("AutomationSectionLabel")
        self.body_layout().addWidget(label_widget)
        if self._multiline:
            editor = QPlainTextEdit()
            editor.setPlainText(str(value or ""))
            self._input: QLineEdit | QPlainTextEdit = editor
        else:
            editor = QLineEdit(str(value or ""))
            self._input = editor
        self.body_layout().addWidget(self._input)
        self.add_buttons(confirm_text=confirm_text)

    def value(self) -> str:
        if self._multiline:
            return str(cast(QPlainTextEdit, self._input).toPlainText() or "").strip()
        return str(cast(QLineEdit, self._input).text() or "").strip()


class AutomationMessageDialog(AutomationModalDialog):
    def __init__(
        self,
        *,
        title: str,
        message: str,
        detail: str = "",
        confirm_text: str = "Cerrar",
        danger: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, "", parent)
        body = QLabel(message)
        body.setWordWrap(True)
        self.body_layout().addWidget(body)
        if detail:
            detail_box = QPlainTextEdit()
            detail_box.setObjectName("LogConsole")
            detail_box.setReadOnly(True)
            detail_box.setPlainText(str(detail or ""))
            detail_box.setMinimumHeight(160)
            self.body_layout().addWidget(detail_box)
        self.add_buttons(confirm_text=confirm_text, cancel_text="", danger=danger)


def confirm_automation_action(
    parent: QWidget,
    *,
    title: str,
    message: str,
    confirm_text: str = "Aceptar",
    cancel_text: str = "Cancelar",
    danger: bool = False,
) -> bool:
    dialog = AutomationModalDialog(title, "", parent)
    label = QLabel(message)
    label.setWordWrap(True)
    dialog.body_layout().addWidget(label)
    row = QHBoxLayout()
    row.setContentsMargins(0, 4, 0, 0)
    row.setSpacing(8)
    row.addStretch(1)
    cancel_button = QPushButton(cancel_text)
    cancel_button.setObjectName("SecondaryButton")
    cancel_button.clicked.connect(dialog.reject)
    row.addWidget(cancel_button)
    confirm_button = QPushButton(confirm_text)
    confirm_button.setObjectName("DangerButton" if danger else "PrimaryButton")
    confirm_button.clicked.connect(dialog.accept)
    row.addWidget(confirm_button)
    dialog.body_layout().addLayout(row)
    return dialog.exec() == QDialog.Accepted


def open_automation_file_dialog(parent: QWidget, title: str, file_filter: str) -> str:
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
