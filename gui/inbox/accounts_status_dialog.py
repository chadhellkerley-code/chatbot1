from __future__ import annotations

import logging
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext


logger = logging.getLogger(__name__)


class AccountsStatusDialog(QDialog):
    _PAGE_SIZE = 15

    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._current_page = 0
        self._accounts: list[str] = []
        self._selected_by_alias: dict[str, set[str]] = {}
        self._known_by_alias: dict[str, set[str]] = {}

        self.setModal(True)
        self.setObjectName("AccountsModalDialog")
        self.setWindowTitle("Estado de cuentas")
        self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)
        self.resize(720, 560)

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title = QLabel("Estado de cuentas")
        title.setObjectName("AccountsModalTitle")
        root.addWidget(title)

        hint = QLabel("Dialogo legado. El runtime real de autoresponder/follow-up ahora se administra solo desde Inbox CRM.")
        hint.setObjectName("AccountsModalHint")
        hint.setWordWrap(True)
        root.addWidget(hint)

        alias_row = QHBoxLayout()
        alias_row.setContentsMargins(0, 0, 0, 0)
        alias_row.setSpacing(10)
        alias_label = QLabel("Alias")
        alias_row.addWidget(alias_label, 0, Qt.AlignLeft | Qt.AlignVCenter)
        self._alias_combo = QComboBox()
        self._alias_combo.setMinimumWidth(260)
        alias_row.addWidget(self._alias_combo, 1)
        root.addLayout(alias_row)

        accounts_card = QFrame()
        accounts_card.setObjectName("AccountsModalCard")
        accounts_layout = QVBoxLayout(accounts_card)
        accounts_layout.setContentsMargins(14, 14, 14, 14)
        accounts_layout.setSpacing(10)

        self._accounts_widget = QWidget()
        self._accounts_layout = QVBoxLayout(self._accounts_widget)
        self._accounts_layout.setContentsMargins(0, 0, 0, 0)
        self._accounts_layout.setSpacing(6)
        self._accounts_layout.addStretch(1)

        scroll = QScrollArea()
        scroll.setObjectName("AccountsModalScroll")
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidget(self._accounts_widget)

        accounts_container = QFrame()
        accounts_container.setObjectName("AccountsModalScrollContainer")
        container_layout = QVBoxLayout(accounts_container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(0)
        container_layout.addWidget(scroll, 1)
        self._accounts_widget.setObjectName("AccountsModalScrollContent")

        accounts_layout.addWidget(accounts_container, 1)

        pagination_row = QHBoxLayout()
        pagination_row.setContentsMargins(0, 0, 0, 0)
        pagination_row.setSpacing(10)
        self._prev_button = QPushButton("Anterior")
        self._prev_button.setObjectName("GhostButton")
        self._prev_button.clicked.connect(self._prev_page)
        pagination_row.addWidget(self._prev_button, 0, Qt.AlignLeft)

        self._page_label = QLabel("")
        self._page_label.setObjectName("AccountsModalHint")
        pagination_row.addWidget(self._page_label, 1, Qt.AlignCenter)

        self._next_button = QPushButton("Siguiente")
        self._next_button.setObjectName("GhostButton")
        self._next_button.clicked.connect(self._next_page)
        pagination_row.addWidget(self._next_button, 0, Qt.AlignRight)
        accounts_layout.addLayout(pagination_row)

        root.addWidget(accounts_card, 1)

        delays_card = QFrame()
        delays_card.setObjectName("AccountsModalCard")
        delays_grid = QGridLayout(delays_card)
        delays_grid.setContentsMargins(14, 14, 14, 14)
        delays_grid.setHorizontalSpacing(12)
        delays_grid.setVerticalSpacing(8)

        delays_grid.addWidget(QLabel("Delay mínimo"), 0, 0)
        self._delay_min = QSpinBox()
        self._delay_min.setRange(1, 3600)
        self._delay_min.setValue(10)
        self._delay_min.valueChanged.connect(self._sync_delay_bounds)
        delays_grid.addWidget(self._delay_min, 1, 0)

        delays_grid.addWidget(QLabel("Delay máximo"), 0, 1)
        self._delay_max = QSpinBox()
        self._delay_max.setRange(1, 3600)
        self._delay_max.setValue(30)
        self._delay_max.valueChanged.connect(self._sync_delay_bounds)
        delays_grid.addWidget(self._delay_max, 1, 1)

        root.addWidget(delays_card, 0)

        buttons_row = QHBoxLayout()
        buttons_row.setContentsMargins(0, 0, 0, 0)
        buttons_row.setSpacing(10)
        buttons_row.addStretch(1)

        close_button = QPushButton("Cerrar")
        close_button.setObjectName("GhostButton")
        close_button.clicked.connect(self.reject)
        buttons_row.addWidget(close_button, 0, Qt.AlignRight)

        start_button = QPushButton("Abrir Inbox")
        start_button.setObjectName("PrimaryButton")
        start_button.clicked.connect(self._start_clicked)
        buttons_row.addWidget(start_button, 0, Qt.AlignRight)

        root.addLayout(buttons_row)

        self._load_aliases()
        self._alias_combo.currentTextChanged.connect(self.load_alias_accounts)
        if self._alias_combo.count() > 0:
            self.load_alias_accounts(self._alias_combo.currentText())

    def _load_aliases(self) -> None:
        self._alias_combo.clear()
        aliases = []
        try:
            aliases = list(self._ctx.services.accounts.list_aliases() or [])
        except Exception:
            logger.exception("Failed to list aliases for AccountsStatusDialog")
        for alias in aliases:
            clean = str(alias or "").strip()
            if clean:
                self._alias_combo.addItem(clean)

    def load_alias_accounts(self, alias: str) -> None:
        selected_alias = str(alias or "").strip()
        if not selected_alias:
            self._accounts = []
            self._current_page = 0
            self.render_accounts_page(self._accounts, self._current_page)
            return

        records: list[dict[str, Any]] = []
        try:
            records = list(self._ctx.services.accounts.list_accounts(selected_alias) or [])
        except Exception:
            logger.exception("Failed to list accounts for alias=%s", selected_alias)

        usernames = []
        for item in records:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username") or "").strip()
            if username:
                usernames.append(username)
        usernames = sorted(set(usernames), key=lambda value: value.lower())

        self._accounts = usernames
        self._current_page = 0

        selected_set = self._selected_by_alias.get(selected_alias)
        if selected_set is None:
            self._selected_by_alias[selected_alias] = set(usernames)
            self._known_by_alias[selected_alias] = set(usernames)
        else:
            known = self._known_by_alias.get(selected_alias, set())
            current = set(usernames)
            new_accounts = current - known
            selected_set.intersection_update(current)
            selected_set.update(new_accounts)
            self._known_by_alias[selected_alias] = current

        self.render_accounts_page(self._accounts, self._current_page)

    def render_accounts_page(self, accounts: list[str], page: int) -> None:
        while self._accounts_layout.count():
            item = self._accounts_layout.takeAt(0)
            if item is None:
                continue
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

        alias = str(self._alias_combo.currentText() or "").strip()
        selected_set = self._selected_by_alias.setdefault(alias, set())

        total = len(accounts or [])
        page_size = self._PAGE_SIZE
        total_pages = max(1, (total + page_size - 1) // page_size)
        safe_page = max(0, min(int(page or 0), total_pages - 1))
        self._current_page = safe_page

        start = safe_page * page_size
        end = min(total, start + page_size)
        page_accounts = list(accounts[start:end])

        for username in page_accounts:
            checkbox = QCheckBox(username)
            checkbox.setChecked(username in selected_set)

            def _toggle(checked: bool, *, _u: str = username) -> None:
                if checked:
                    selected_set.add(_u)
                else:
                    selected_set.discard(_u)

            checkbox.toggled.connect(_toggle)
            self._accounts_layout.addWidget(checkbox)

        self._accounts_layout.addStretch(1)

        self._prev_button.setEnabled(safe_page > 0)
        self._next_button.setEnabled(end < total)
        self._page_label.setText(f"Página {safe_page + 1} / {total_pages}")

    def _prev_page(self) -> None:
        self.render_accounts_page(self._accounts, self._current_page - 1)

    def _next_page(self) -> None:
        self.render_accounts_page(self._accounts, self._current_page + 1)

    def _sync_delay_bounds(self, _value: int) -> None:
        delay_min = int(self._delay_min.value())
        if self._delay_max.value() < delay_min:
            self._delay_max.setValue(delay_min)
        self._delay_max.setMinimum(delay_min)

    def _start_clicked(self) -> None:
        selected_alias = str(self._alias_combo.currentText() or "").strip()
        if not selected_alias:
            QMessageBox.warning(self, "Inbox CRM", "Selecciona un alias.")
            return
        try:
            self._ctx.state.active_alias = selected_alias
            self._ctx.open_route("inbox_page", {"source": "accounts_status_dialog", "alias_id": selected_alias})
        except Exception as exc:
            logger.exception("Failed to redirect to Inbox from AccountsStatusDialog")
            QMessageBox.critical(self, "Inbox CRM", f"No se pudo abrir Inbox: {exc}")
            return

        QMessageBox.information(
            self,
            "Inbox CRM",
            "Este dialogo quedo desactivado. Usa Inbox CRM para iniciar o detener el runtime real.",
        )
        self.accept()
