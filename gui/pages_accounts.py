from __future__ import annotations

import asyncio
import contextlib
import logging
import traceback
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from application.services import ServiceError
from PySide6.QtCore import QItemSelectionModel, Qt, QTimer
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QStackedWidget,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from gui.query_runner import QueryError
from runtime.runtime import STOP_EVENT, reset_stop_event
from src.content_publisher.content_ui_controller import ContentUIController
from src.warmup.warmup_engine import WarmupEngine
from src.warmup.warmup_scheduler import WarmupCursor, WarmupScheduler

from .page_base import ClickableMetricCard, PageContext, SectionPage, message_limit, table_item
from .snapshot_queries import (
    build_accounts_actions_snapshot,
    build_accounts_home_snapshot,
    build_accounts_table_snapshot,
    build_alias_page_snapshot,
)


ACCOUNTS_SUBSECTIONS: tuple[tuple[str, str], ...] = (
    ("alias_page", "Alias"),
    ("accounts_page", "Cuentas"),
    ("proxies_page", "Proxies"),
    ("accounts_actions_page", "Acciones"),
)

_IG_EDIT_PROFILE_URL = "https://www.instagram.com/accounts/edit/"
_MANUAL_ACTION_TASK = "accounts_manual_action"
_VIEW_CONTENT_TASK = "accounts_view_content"
_WARMUP_FLOW_TASK = "accounts_warmup_flow"
_ACCOUNTS_MODULE_SELECTOR = "selector"
_ACCOUNTS_MODULE_WARMUP = "warmup"
_ACCOUNTS_MODULE_VIEW_CONTENT = "view_content"
_ACCOUNTS_MODULE_CONTENT = "content_publishing"

_WARMUP_ACTION_OPTIONS: tuple[tuple[str, str], ...] = (
    ("watch_reels", "Ver reels"),
    ("like_posts", "Dar likes"),
    ("follow_accounts", "Seguir cuentas"),
    ("comment_post", "Comentar post"),
    ("reply_story", "Responder historia"),
    ("send_message", "Enviar mensaje"),
)

logger = logging.getLogger(__name__)


def _make_card(
    object_name: str,
    *,
    margins: tuple[int, int, int, int] = (18, 18, 18, 18),
    spacing: int = 12,
) -> tuple[QFrame, QVBoxLayout]:
    card = QFrame()
    card.setObjectName(object_name)
    layout = QVBoxLayout(card)
    layout.setContentsMargins(*margins)
    layout.setSpacing(spacing)
    return card, layout


def _configure_table(
    table: QTableWidget,
    object_name: str,
    *,
    selection_mode: QAbstractItemView.SelectionMode,
) -> None:
    table.setObjectName(object_name)
    table.verticalHeader().setVisible(False)
    table.setSelectionBehavior(QAbstractItemView.SelectRows)
    table.setSelectionMode(selection_mode)
    table.setEditTriggers(QAbstractItemView.NoEditTriggers)
    table.setAlternatingRowColors(True)
    table.setShowGrid(False)
    table.setWordWrap(False)
    table.horizontalHeader().setStretchLastSection(True)


def _fill_alias_combo(combo: QComboBox, aliases: list[str], current_alias: str) -> None:
    combo.blockSignals(True)
    combo.clear()
    for alias in aliases:
        combo.addItem(alias, alias)
    index = combo.findData(current_alias)
    if index < 0 and aliases:
        index = combo.findData(aliases[0])
    combo.setCurrentIndex(max(0, index))
    combo.blockSignals(False)


def _normalize_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


def _proxy_host_label(server: str) -> str:
    raw_server = str(server or "").strip()
    if not raw_server:
        return "-"
    parsed = urlparse(raw_server if "://" in raw_server else f"http://{raw_server}")
    host = parsed.hostname or raw_server
    port = parsed.port
    if host and port:
        return f"{host}:{port}"
    return str(host or raw_server)


def _proxy_usage_label(aliases: set[str], account_count: int) -> str:
    clean_aliases = sorted(str(alias or "").strip() for alias in aliases if str(alias or "").strip())
    if account_count <= 0:
        return "Sin asignar"
    alias_label = ""
    if clean_aliases:
        if len(clean_aliases) <= 2:
            alias_label = ", ".join(clean_aliases)
        else:
            alias_label = f"{clean_aliases[0]}, {clean_aliases[1]} +{len(clean_aliases) - 2}"
    if alias_label:
        return f"{account_count} cuentas  |  {alias_label}"
    return f"{account_count} cuentas"


def _checkable_account_item(username: str, detail: str, *, enabled: bool = True) -> QListWidgetItem:
    item = QListWidgetItem(detail)
    item.setData(Qt.UserRole, username)
    flags = item.flags() & ~Qt.ItemIsSelectable
    if enabled:
        flags |= Qt.ItemIsUserCheckable | Qt.ItemIsEnabled
    else:
        flags &= ~Qt.ItemIsUserCheckable
        flags &= ~Qt.ItemIsEnabled
    item.setFlags(flags)
    item.setCheckState(Qt.Unchecked)
    return item


def _account_item_is_checkable(item: QListWidgetItem | None) -> bool:
    if item is None:
        return False
    flags = item.flags()
    return bool(flags & Qt.ItemIsEnabled) and bool(flags & Qt.ItemIsUserCheckable)


def _open_dark_file_dialog(parent: QWidget, title: str, file_filter: str) -> str:
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


class AccountsModalDialog(QDialog):
    def __init__(self, title: str, subtitle: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setModal(True)
        self.setObjectName("AccountsModalDialog")
        self.setWindowTitle(title)
        self.resize(560, 0)
        self.setStyleSheet(
            """
            QDialog#AccountsModalDialog {
                background-color: #0f1722;
                color: #e7edf6;
            }
            QLabel#AccountsModalTitle {
                color: #f8fbff;
                font-size: 16px;
                font-weight: 700;
            }
            QLabel#AccountsModalHint {
                color: #9aa9bc;
            }
            QFrame#AccountsModalCard {
                background-color: #151f2d;
                border: 1px solid #243246;
                border-radius: 16px;
            }
            QListWidget#AccountsAssignList,
            QLineEdit,
            QComboBox,
            QSpinBox,
            QPlainTextEdit {
                background-color: #0c131c;
                color: #e7edf6;
                border: 1px solid #2b3a4f;
                border-radius: 10px;
                padding: 8px;
                selection-background-color: #284768;
            }
            QListWidget#AccountsAssignList::item {
                padding: 8px 6px;
            }
            """
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title_label = QLabel(title)
        title_label.setObjectName("AccountsModalTitle")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("AccountsModalHint")
        subtitle_label.setWordWrap(True)
        root.addWidget(title_label)
        root.addWidget(subtitle_label)

        body = QFrame()
        body.setObjectName("AccountsModalCard")
        self._body_layout = QVBoxLayout(body)
        self._body_layout.setContentsMargins(18, 18, 18, 18)
        self._body_layout.setSpacing(12)
        root.addWidget(body)

    def body_layout(self) -> QVBoxLayout:
        return self._body_layout


class AccountsAlertDialog(AccountsModalDialog):
    def __init__(
        self,
        title: str,
        message: str,
        *,
        confirm_text: str = "Aceptar",
        cancel_text: str = "",
        danger: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, message, parent=parent)
        self._accepted = False

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
        self.body_layout().addLayout(actions)


class AccountsTextInputDialog(AccountsModalDialog):
    def __init__(
        self,
        title: str,
        subtitle: str,
        *,
        label: str,
        placeholder: str = "",
        text: str = "",
        confirm_text: str = "Guardar",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, subtitle, parent=parent)
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._input = QLineEdit()
        self._input.setPlaceholderText(placeholder)
        self._input.setText(text)
        form.addWidget(QLabel(label), 0, 0)
        form.addWidget(self._input, 0, 1)
        self.body_layout().addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        confirm_button = QPushButton(confirm_text)
        confirm_button.setObjectName("PrimaryButton")
        confirm_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(confirm_button)
        self.body_layout().addLayout(actions)

    def value(self) -> str:
        return str(self._input.text() or "").strip()


class AccountUsageStateDialog(AccountsModalDialog):
    def __init__(
        self,
        *,
        selected_count: int,
        visible_count: int,
        parent: QWidget | None = None,
    ) -> None:
        subtitle_parts = []
        if selected_count > 0:
            subtitle_parts.append(f"Seleccionadas: {selected_count}")
        if visible_count > 0:
            subtitle_parts.append(f"Visibles: {visible_count}")
        super().__init__(
            "Estado de uso",
            "Aplica el estado operativo sin afectar login, health, proxy ni edicion manual."
            + (f" ({'  |  '.join(subtitle_parts)})" if subtitle_parts else ""),
            parent=parent,
        )
        self._usage_state = ""

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._scope = QComboBox()
        if selected_count > 0:
            self._scope.addItem(f"Cuentas seleccionadas ({selected_count})", "selected")
        if visible_count > 0:
            self._scope.addItem(f"Cuentas visibles ({visible_count})", "visible")
        form.addWidget(QLabel("Aplicar a"), 0, 0)
        form.addWidget(self._scope, 0, 1)
        self.body_layout().addLayout(form)

        hint = QLabel(
            "Activa: la cuenta vuelve a participar en rotacion automatica. "
            "Desactivada: sigue visible y editable, pero no se usa operativamente."
        )
        hint.setObjectName("AccountsModalHint")
        hint.setWordWrap(True)
        self.body_layout().addWidget(hint)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        activate_button = QPushButton("Activar")
        activate_button.setObjectName("PrimaryButton")
        activate_button.clicked.connect(lambda: self._accept_with_state("active"))
        deactivate_button = QPushButton("Desactivar")
        deactivate_button.setObjectName("DangerButton")
        deactivate_button.clicked.connect(lambda: self._accept_with_state("deactivated"))
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(activate_button)
        actions.addWidget(deactivate_button)
        self.body_layout().addLayout(actions)

    def _accept_with_state(self, usage_state: str) -> None:
        self._usage_state = str(usage_state or "").strip().lower()
        self.accept()

    def scope(self) -> str:
        return str(self._scope.currentData() or "selected").strip()

    def usage_state(self) -> str:
        return self._usage_state


class AccountAliasChangeDialog(AccountsModalDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(
            "Cambio de alias",
            "Selecciona una o mas cuentas del alias actual y el alias destino para moverlas sin alterar su estado operativo.",
            parent=parent,
        )
        self._current_alias = ""
        self._selected_alias = ""

        self._summary = QLabel("Selecciona al menos una cuenta y el alias destino.")
        self._summary.setObjectName("AccountsModalHint")
        self._summary.setWordWrap(True)
        self.body_layout().addWidget(self._summary)

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)
        self._source_alias = QLineEdit()
        self._source_alias.setReadOnly(True)
        self._target_alias = QComboBox()
        form.addWidget(QLabel("Alias actual"), 0, 0)
        form.addWidget(self._source_alias, 0, 1)
        form.addWidget(QLabel("Alias destino"), 1, 0)
        form.addWidget(self._target_alias, 1, 1)
        self.body_layout().addLayout(form)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        mark_all = QPushButton("Seleccionar todas")
        mark_all.setObjectName("SecondaryButton")
        mark_all.clicked.connect(self._mark_all)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_all)
        actions_row.addStretch(1)
        actions_row.addWidget(mark_all)
        actions_row.addWidget(clear_button)
        self.body_layout().addLayout(actions_row)

        self._accounts = QListWidget()
        self._accounts.setObjectName("AccountsAssignList")
        self._accounts.setMinimumHeight(280)
        self._accounts.setSelectionMode(QAbstractItemView.NoSelection)
        self._accounts.itemChanged.connect(self._update_summary)
        self.body_layout().addWidget(self._accounts)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        accept_button = QPushButton("Aceptar")
        accept_button.setObjectName("PrimaryButton")
        accept_button.clicked.connect(self.accept)
        footer.addStretch(1)
        footer.addWidget(cancel_button)
        footer.addWidget(accept_button)
        self.body_layout().addLayout(footer)

    def refresh(
        self,
        *,
        alias: str,
        target_aliases: list[str],
        records: list[dict[str, Any]],
        selected_usernames: list[str] | None = None,
    ) -> None:
        self._current_alias = str(alias or "").strip()
        self._selected_alias = self._current_alias
        self._source_alias.setText(self._current_alias or "-")
        _fill_alias_combo(self._target_alias, target_aliases, target_aliases[0] if target_aliases else "")
        selected = {
            str(item or "").strip().lstrip("@").lower()
            for item in selected_usernames or []
            if str(item or "").strip()
        }
        self._accounts.blockSignals(True)
        self._accounts.clear()
        for record in records:
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            connected_label = str(record.get("connected_label") or ("Si" if bool(record.get("connected")) else "No")).strip()
            health_label = str(record.get("health_badge") or "-").strip() or "-"
            usage_label = str(record.get("usage_state_label") or "Activa").strip() or "Activa"
            detail = (
                f"@{username}  |  Conexion: {connected_label}  |  "
                f"Health: {health_label}  |  Estado: {usage_label}"
            )
            item = _checkable_account_item(username, detail)
            if username.lower() in selected:
                item.setCheckState(Qt.Checked)
            self._accounts.addItem(item)
        self._accounts.blockSignals(False)
        self._update_summary()

    def _checked_usernames(self) -> list[str]:
        usernames: list[str] = []
        for index in range(self._accounts.count()):
            item = self._accounts.item(index)
            if (
                item is None
                or not _account_item_is_checkable(item)
                or item.checkState() != Qt.Checked
            ):
                continue
            username = str(item.data(Qt.UserRole) or "").strip().lstrip("@")
            if username:
                usernames.append(username)
        return usernames

    def _mark_all(self) -> None:
        self._accounts.blockSignals(True)
        for index in range(self._accounts.count()):
            item = self._accounts.item(index)
            if _account_item_is_checkable(item):
                item.setCheckState(Qt.Checked)
        self._accounts.blockSignals(False)
        self._update_summary()

    def _clear_all(self) -> None:
        self._accounts.blockSignals(True)
        for index in range(self._accounts.count()):
            item = self._accounts.item(index)
            if _account_item_is_checkable(item):
                item.setCheckState(Qt.Unchecked)
        self._accounts.blockSignals(False)
        self._update_summary()

    def _update_summary(self) -> None:
        selected = len(self._checked_usernames())
        available = self._accounts.count()
        target_alias = self.target_alias() or "-"
        self._summary.setText(
            f"Alias actual: {self._current_alias or '-'}  |  "
            f"Alias destino: {target_alias}  |  "
            f"Seleccionadas: {selected}/{available}"
        )

    def selected_usernames(self) -> list[str]:
        return self._checked_usernames()

    def target_alias(self) -> str:
        return str(self._target_alias.currentData() or self._target_alias.currentText() or "").strip()


class AliasDeleteDialog(AccountsModalDialog):
    def __init__(
        self,
        alias: str,
        *,
        move_options: list[str],
        parent: QWidget | None = None,
    ) -> None:
        has_accounts = bool(move_options)
        super().__init__(
            "Eliminar alias",
            (
                f"El alias {alias} tiene cuentas asociadas. Selecciona un alias destino para moverlas antes de eliminar."
                if has_accounts
                else f"Confirma la eliminacion del alias {alias}."
            ),
            parent=parent,
        )
        self._target_alias = QComboBox()
        if has_accounts:
            _fill_alias_combo(self._target_alias, move_options, move_options[0])
            form = QGridLayout()
            form.setContentsMargins(0, 0, 0, 0)
            form.setHorizontalSpacing(10)
            form.setVerticalSpacing(10)
            form.addWidget(QLabel("Alias destino"), 0, 0)
            form.addWidget(self._target_alias, 0, 1)
            self.body_layout().addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        delete_button = QPushButton("Eliminar alias")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(delete_button)
        self.body_layout().addLayout(actions)

    def target_alias(self) -> str:
        return str(self._target_alias.currentData() or self._target_alias.currentText() or "").strip()


class AccountSelectionDialog(AccountsModalDialog):
    def __init__(
        self,
        ctx: PageContext,
        *,
        title: str = "Seleccionar cuentas",
        subtitle: str = "Marca una o varias cuentas para continuar con la accion.",
        require_manual_action_ready: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, subtitle, parent=parent)
        self._ctx = ctx
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._selected_alias = ""
        self._refresh_after_current = False
        self._require_manual_action_ready = bool(require_manual_action_ready)

        self._summary = QLabel("")
        self._summary.setObjectName("AccountsModalHint")
        self._summary.setWordWrap(True)
        self.body_layout().addWidget(self._summary)

        header = QGridLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setHorizontalSpacing(10)
        header.setVerticalSpacing(10)
        self._alias = QComboBox()
        header.addWidget(QLabel("Alias"), 0, 0)
        header.addWidget(self._alias, 0, 1)
        self.body_layout().addLayout(header)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        mark_all = QPushButton("Seleccionar todas")
        mark_all.setObjectName("SecondaryButton")
        mark_all.clicked.connect(self._mark_all)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_all)
        actions_row.addStretch(1)
        actions_row.addWidget(mark_all)
        actions_row.addWidget(clear_button)
        self.body_layout().addLayout(actions_row)

        self._accounts = QListWidget()
        self._accounts.setObjectName("AccountsAssignList")
        self._accounts.setMinimumHeight(280)
        self._accounts.setSelectionMode(QAbstractItemView.NoSelection)
        self.body_layout().addWidget(self._accounts)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        start_button = QPushButton("Iniciar")
        start_button.setObjectName("PrimaryButton")
        start_button.clicked.connect(self.accept)
        footer.addStretch(1)
        footer.addWidget(cancel_button)
        footer.addWidget(start_button)
        self.body_layout().addLayout(footer)

        self._alias.currentIndexChanged.connect(self._request_accounts)

    def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
        if not aliases:
            aliases = [current_alias] if current_alias else []
        _fill_alias_combo(self._alias, aliases, current_alias)
        self._request_accounts()

    def _render_accounts(self, alias: str, records: list[dict[str, Any]]) -> None:
        clean_alias = str(alias or "").strip() or "-"
        self._selected_alias = clean_alias
        allowed_count = 0
        self._accounts.clear()
        for record in records:
            username = _normalize_username(record.get("username"))
            if not username:
                continue
            availability = {"allowed": True, "message": ""}
            if self._require_manual_action_ready:
                availability = {
                    "allowed": bool(record.get("manual_action_allowed", False)),
                    "message": str(record.get("manual_action_message") or "").strip(),
                }
                if not availability["message"]:
                    resolver = getattr(self._ctx.services.accounts, "manual_action_eligibility", None)
                    if callable(resolver):
                        availability = dict(resolver(record))
            if bool(availability.get("allowed")):
                allowed_count += 1
            detail = (
                f"@{username}  |  "
                f"{record.get('connected_label') or ('Si' if bool(record.get('connected')) else 'No')}  |  "
                f"{record.get('health_badge') or '-'}  |  "
                f"{record.get('proxy_label') or '-'}"
            )
            message = str(availability.get("message") or "").strip()
            if self._require_manual_action_ready and message:
                detail = f"{detail}  |  {message}"
            self._accounts.addItem(
                _checkable_account_item(
                    username,
                    detail,
                    enabled=bool(availability.get("allowed", True)),
                )
            )
        summary = f"Alias activo: {clean_alias}  |  Cuentas disponibles: {len(records)}"
        if self._require_manual_action_ready:
            summary += f"  |  Operables: {allowed_count}"
        self._summary.setText(summary)

    def _request_accounts(self) -> None:
        alias = self.alias()
        if not alias:
            return
        if self._snapshot_loading:
            self._refresh_after_current = True
            return
        self._snapshot_loading = True
        self._summary.setText(f"Cargando cuentas del alias {alias}...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda selected_alias=alias: build_accounts_table_snapshot(
                self._ctx.services,
                active_alias=selected_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        alias = str(data.get("active_alias") or self.alias()).strip()
        current_alias = self.alias()
        needs_follow_up = self._refresh_after_current
        self._refresh_after_current = False
        if (current_alias and alias and current_alias.lower() != alias.lower()) or needs_follow_up:
            self._request_accounts()
            return
        rows = [dict(item) for item in data.get("rows") or [] if isinstance(item, dict)]
        self._render_accounts(alias, rows)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._refresh_after_current = False
        self._summary.setText(f"No se pudieron cargar las cuentas: {error.message}")

    def _mark_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if _account_item_is_checkable(item):
                item.setCheckState(Qt.Checked)

    def _clear_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if _account_item_is_checkable(item):
                item.setCheckState(Qt.Unchecked)

    def usernames(self) -> list[str]:
        usernames: list[str] = []
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if not _account_item_is_checkable(item) or item.checkState() != Qt.Checked:
                continue
            username = _normalize_username(item.data(Qt.UserRole))
            if username:
                usernames.append(username)
        return usernames

    def alias(self) -> str:
        return str(self._alias.currentData() or self._alias.currentText() or self._selected_alias or "").strip()


class WarmupLauncherDialog(AccountsModalDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(
            "Warm Up",
            "Elige si quieres abrir un flujo existente o crear uno nuevo antes de entrar al editor.",
            parent=parent,
        )
        self._selection = ""
        self.resize(520, 0)

        actions = QVBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(12)

        existing_button = QPushButton("Ver flujos existentes")
        existing_button.setObjectName("PrimaryButton")
        existing_button.setMinimumHeight(48)
        existing_button.clicked.connect(lambda: self._choose("existing"))

        create_button = QPushButton("Crear flujo nuevo")
        create_button.setObjectName("SecondaryButton")
        create_button.setMinimumHeight(48)
        create_button.clicked.connect(lambda: self._choose("create"))

        actions.addWidget(existing_button)
        actions.addWidget(create_button)
        self.body_layout().addLayout(actions)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.addStretch(1)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        footer.addWidget(cancel_button)
        self.body_layout().addLayout(footer)

    def _choose(self, selection: str) -> None:
        self._selection = str(selection or "").strip().lower()
        self.accept()

    def selection(self) -> str:
        return self._selection


class WarmupExistingFlowsDialog(AccountsModalDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(
            "Flujos Warm Up",
            "Abre el flujo que quieras seguir editando o eliminalo definitivamente de la base SQLite.",
            parent=parent,
        )
        self.resize(780, 560)
        self._action = ""
        self._flows: list[dict[str, Any]] = []

        self._summary = QLabel("No hay flujos cargados.")
        self._summary.setObjectName("AccountsModalHint")
        self._summary.setWordWrap(True)
        self.body_layout().addWidget(self._summary)

        self._list = QListWidget()
        self._list.setObjectName("WarmupFlowList")
        self._list.setMinimumHeight(320)
        self._list.itemSelectionChanged.connect(self._update_selection_state)
        self._list.itemDoubleClicked.connect(lambda _item: self._open_selected())
        self.body_layout().addWidget(self._list)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        self._delete_button = QPushButton("Eliminar flujo")
        self._delete_button.setObjectName("DangerButton")
        self._delete_button.clicked.connect(self._delete_selected)
        self._open_button = QPushButton("Abrir flujo")
        self._open_button.setObjectName("PrimaryButton")
        self._open_button.clicked.connect(self._open_selected)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        actions.addStretch(1)
        actions.addWidget(self._delete_button)
        actions.addWidget(self._open_button)
        actions.addWidget(cancel_button)
        self.body_layout().addLayout(actions)
        self._update_selection_state()

    def refresh_flows(self, flows: list[dict[str, Any]]) -> None:
        self._action = ""
        self._flows = [dict(item) for item in flows if isinstance(item, dict)]
        self._list.clear()
        for flow in self._flows:
            item = QListWidgetItem(
                (
                    f"{flow.get('name') or 'Flujo Warm Up'}  |  "
                    f"Alias: {flow.get('alias') or '-'}  |  "
                    f"Cuentas: {int(flow.get('account_count') or 0)}  |  "
                    f"Etapas: {int(flow.get('stages_count') or 0)}  |  "
                    f"Estado: {flow.get('status') or 'paused'}"
                )
            )
            item.setData(Qt.UserRole, int(flow.get("id") or 0))
            self._list.addItem(item)
        if self._list.count():
            self._list.setCurrentRow(0)
        self._update_selection_state()

    def _selected_summary(self) -> dict[str, Any]:
        item = self._list.currentItem()
        selected_id = int(item.data(Qt.UserRole) or 0) if item is not None else 0
        return next((flow for flow in self._flows if int(flow.get("id") or 0) == selected_id), {})

    def _update_selection_state(self) -> None:
        flow = self._selected_summary()
        enabled = bool(flow)
        self._open_button.setEnabled(enabled)
        self._delete_button.setEnabled(enabled)
        if not enabled:
            self._summary.setText("No hay flujos disponibles para abrir o eliminar.")
            return
        self._summary.setText(
            (
                f"Flujo actual: {flow.get('name') or '-'}  |  "
                f"Alias: {flow.get('alias') or '-'}  |  "
                f"Cuentas: {int(flow.get('account_count') or 0)}  |  "
                f"Etapas: {int(flow.get('stages_count') or 0)}  |  "
                f"Logs: {int(flow.get('log_count') or 0)}"
            )
        )

    def _open_selected(self) -> None:
        if not self.selected_flow_id():
            return
        self._action = "open"
        self.accept()

    def _delete_selected(self) -> None:
        if not self.selected_flow_id():
            return
        self._action = "delete"
        self.accept()

    def action(self) -> str:
        return self._action

    def selected_flow_id(self) -> int:
        item = self._list.currentItem()
        return int(item.data(Qt.UserRole) or 0) if item is not None else 0


class WarmupCreateFlowDialog(AccountsModalDialog):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            "Crear flujo Warm Up",
            "Cada flujo queda vinculado a un alias y a las cuentas seleccionadas para saber a quien aplicar las acciones.",
            parent=parent,
        )
        self._ctx = ctx
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._selected_alias = ""
        self._refresh_after_current = False
        self.resize(720, 620)

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._name = QLineEdit()
        self._name.setPlaceholderText("Flujo Warm Up")
        self._alias = QComboBox()

        form.addWidget(QLabel("Nombre"), 0, 0)
        form.addWidget(self._name, 0, 1)
        form.addWidget(QLabel("Alias"), 1, 0)
        form.addWidget(self._alias, 1, 1)
        self.body_layout().addLayout(form)

        self._summary = QLabel("Selecciona un alias para cargar sus cuentas.")
        self._summary.setObjectName("AccountsModalHint")
        self._summary.setWordWrap(True)
        self.body_layout().addWidget(self._summary)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        select_all = QPushButton("Seleccionar todas")
        select_all.setObjectName("SecondaryButton")
        select_all.clicked.connect(self._mark_all)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_all)
        actions_row.addStretch(1)
        actions_row.addWidget(select_all)
        actions_row.addWidget(clear_button)
        self.body_layout().addLayout(actions_row)

        self._accounts = QListWidget()
        self._accounts.setObjectName("AccountsAssignList")
        self._accounts.setMinimumHeight(300)
        self._accounts.setSelectionMode(QAbstractItemView.NoSelection)
        self.body_layout().addWidget(self._accounts)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        create_button = QPushButton("Crear flujo")
        create_button.setObjectName("PrimaryButton")
        create_button.clicked.connect(self.accept)
        footer.addStretch(1)
        footer.addWidget(cancel_button)
        footer.addWidget(create_button)
        self.body_layout().addLayout(footer)

        self._alias.currentIndexChanged.connect(self._request_accounts)

    def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
        if not aliases:
            aliases = [current_alias] if current_alias else []
        _fill_alias_combo(self._alias, aliases, current_alias)
        if not self._name.text().strip():
            self._name.setText("Flujo Warm Up")
        self._request_accounts()

    def _render_accounts(self, alias: str, records: list[dict[str, Any]]) -> None:
        clean_alias = str(alias or "").strip() or "-"
        self._selected_alias = clean_alias
        self._summary.setText(f"Alias activo: {clean_alias}  |  Cuentas disponibles: {len(records)}")
        self._accounts.clear()
        for record in records:
            username = _normalize_username(record.get("username"))
            if not username:
                continue
            detail = (
                f"@{username}  |  "
                f"{record.get('connected_label') or ('Si' if bool(record.get('connected')) else 'No')}  |  "
                f"{record.get('health_badge') or '-'}  |  "
                f"{record.get('proxy_label') or '-'}"
            )
            self._accounts.addItem(_checkable_account_item(username, detail))

    def _request_accounts(self) -> None:
        alias = self.alias()
        if not alias:
            return
        if self._snapshot_loading:
            self._refresh_after_current = True
            return
        self._snapshot_loading = True
        self._summary.setText(f"Cargando cuentas del alias {alias}...")
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda selected_alias=alias: build_accounts_table_snapshot(
                self._ctx.services,
                active_alias=selected_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        data = dict(payload) if isinstance(payload, dict) else {}
        alias = str(data.get("active_alias") or self.alias()).strip()
        current_alias = self.alias()
        needs_follow_up = self._refresh_after_current
        self._refresh_after_current = False
        if (current_alias and alias and current_alias.lower() != alias.lower()) or needs_follow_up:
            self._request_accounts()
            return
        rows = [dict(item) for item in data.get("rows") or [] if isinstance(item, dict)]
        self._render_accounts(alias, rows)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._refresh_after_current = False
        self._summary.setText(f"No se pudieron cargar las cuentas: {error.message}")

    def _mark_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is not None:
                item.setCheckState(Qt.Checked)

    def _clear_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is not None:
                item.setCheckState(Qt.Unchecked)

    def flow_name(self) -> str:
        return str(self._name.text() or "").strip()

    def alias(self) -> str:
        return str(self._alias.currentData() or self._alias.currentText() or self._selected_alias or "").strip()

    def usernames(self) -> list[str]:
        usernames: list[str] = []
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is None or item.checkState() != Qt.Checked:
                continue
            username = _normalize_username(item.data(Qt.UserRole))
            if username:
                usernames.append(username)
        return usernames


def _warmup_action_label(action_type: str) -> str:
    clean_type = str(action_type or "").strip().lower()
    for value, label in _WARMUP_ACTION_OPTIONS:
        if value == clean_type:
            return label
    return clean_type or "Accion"


def _warmup_action_placeholders(action_type: str) -> tuple[str, str]:
    clean_type = str(action_type or "").strip().lower()
    if clean_type == "watch_reels":
        return "Opcional: hashtag o perfil", "Opcional"
    if clean_type == "like_posts":
        return "Perfil, explorar o URL", "Opcional"
    if clean_type == "follow_accounts":
        return "Usernames separados por coma", "Opcional"
    if clean_type == "comment_post":
        return "URL del post o perfil", "Comentario"
    if clean_type == "reply_story":
        return "Username objetivo", "Respuesta"
    if clean_type == "send_message":
        return "Username objetivo", "Mensaje"
    return "Objetivo", "Texto"


class WarmupStageCard(QFrame):
    def __init__(
        self,
        stage: dict[str, Any],
        *,
        resume_stage_order: int,
        resume_action_order: int,
        on_open: Callable[[int], None],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._stage_id = int(stage.get("id") or 0)
        self._on_open = on_open
        self.setObjectName("WarmupStageCard")
        self.setProperty("resume", int(stage.get("stage_order") or 0) == int(resume_stage_order or 0))
        self.setCursor(Qt.PointingHandCursor)
        self.setMinimumWidth(180)
        self.setMaximumWidth(220)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)

        title = QLabel(str(stage.get("title") or "").strip() or f"Dia {int(stage.get('stage_order') or 1)}")
        title.setObjectName("WarmupStageTitle")
        title.setWordWrap(True)

        actions = [dict(item) for item in stage.get("actions") or [] if isinstance(item, dict)]
        action_names = ", ".join(_warmup_action_label(item.get("action_type") or "") for item in actions[:2])
        extra = max(0, len(actions) - 2)
        if extra:
            action_names += f" +{extra}"
        action_names = action_names or "Sin acciones"

        meta_lines = [
            f"Paso {int(stage.get('stage_order') or 1)}",
            f"{len(actions)} accion(es)",
            action_names,
        ]
        if self.property("resume") and int(resume_action_order or 0) > 0:
            meta_lines.append(f"Reanuda en accion {int(resume_action_order)}")
        meta = QLabel(" | ".join(meta_lines))
        meta.setObjectName("WarmupStageMeta")
        meta.setWordWrap(True)

        hint = QLabel("Doble click para editar")
        hint.setObjectName("WarmupStageHint")
        hint.setWordWrap(True)

        layout.addWidget(title)
        layout.addWidget(meta)
        layout.addStretch(1)
        layout.addWidget(hint)

    def mouseDoubleClickEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.LeftButton and self._stage_id > 0:
            self._on_open(self._stage_id)
            event.accept()
            return
        super().mouseDoubleClickEvent(event)


class WarmupActionEditorCard(QFrame):
    def __init__(
        self,
        action: dict[str, Any] | None = None,
        *,
        on_remove: Callable[["WarmupActionEditorCard"], None],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        payload = dict(action or {})
        self._on_remove = on_remove
        self.setObjectName("WarmupActionEditorCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        self._title_label = QLabel("")
        self._title_label.setObjectName("WarmupActionCardTitle")
        self._subtitle_label = QLabel("Define objetivo, texto opcional y cantidad.")
        self._subtitle_label.setObjectName("WarmupActionCardHint")
        self._subtitle_label.setWordWrap(True)
        title_wrap = QVBoxLayout()
        title_wrap.setContentsMargins(0, 0, 0, 0)
        title_wrap.setSpacing(2)
        title_wrap.addWidget(self._title_label)
        title_wrap.addWidget(self._subtitle_label)

        remove_button = QPushButton("Quitar")
        remove_button.setObjectName("DangerButton")
        remove_button.setProperty("compactHeader", True)
        remove_button.clicked.connect(lambda: self._on_remove(self))

        header.addLayout(title_wrap, 1)
        header.addWidget(remove_button)
        layout.addLayout(header)

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(10)

        self._action_combo = QComboBox()
        for value, label in _WARMUP_ACTION_OPTIONS:
            self._action_combo.addItem(label, value)
        selected_action = str(payload.get("action_type") or "watch_reels").strip().lower()
        self._action_combo.setCurrentIndex(max(0, self._action_combo.findData(selected_action)))

        self._target_input = QLineEdit()
        self._target_input.setObjectName("WarmupActionTargetInput")
        self._target_input.setText(str(payload.get("target") or "").strip())

        self._quantity_input = QSpinBox()
        self._quantity_input.setRange(1, 500)
        self._quantity_input.setValue(max(1, int(payload.get("quantity") or 1)))
        self._quantity_input.setMinimumWidth(120)

        self._text_input = QPlainTextEdit()
        self._text_input.setObjectName("WarmupActionTextInput")
        self._text_input.setPlainText(str(payload.get("text") or "").strip())
        self._text_input.setMinimumHeight(88)
        self._text_input.setMaximumBlockCount(6)

        form.addWidget(QLabel("Accion"), 0, 0)
        form.addWidget(self._action_combo, 0, 1)
        form.addWidget(QLabel("Cantidad"), 0, 2)
        form.addWidget(self._quantity_input, 0, 3)
        form.addWidget(QLabel("Objetivo"), 1, 0)
        form.addWidget(self._target_input, 1, 1, 1, 3)
        form.addWidget(QLabel("Texto"), 2, 0)
        form.addWidget(self._text_input, 2, 1, 1, 3)
        form.setColumnStretch(1, 1)
        form.setColumnStretch(3, 0)
        layout.addLayout(form)

        self._action_combo.currentIndexChanged.connect(self._sync_action_placeholders)
        self._sync_action_placeholders()

    def _sync_action_placeholders(self) -> None:
        action_type = self.action_type()
        target_placeholder, text_placeholder = _warmup_action_placeholders(action_type)
        self._title_label.setText(_warmup_action_label(action_type))
        self._target_input.setPlaceholderText(target_placeholder)
        self._text_input.setPlaceholderText(text_placeholder)

    def action_type(self) -> str:
        return str(self._action_combo.currentData() or self._action_combo.currentText() or "").strip().lower()

    def action_payload(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type() or "watch_reels",
            "target": str(self._target_input.text() or "").strip(),
            "text": str(self._text_input.toPlainText() or "").strip(),
            "quantity": max(1, int(self._quantity_input.value())),
        }


class WarmupStageEditorDialog(AccountsModalDialog):
    def __init__(self, stage: dict[str, Any] | None = None, parent: QWidget | None = None) -> None:
        stage = dict(stage or {})
        title_text = "Editar etapa" if stage.get("id") else "Nueva etapa"
        super().__init__(
            title_text,
            "Configura el nombre de la etapa y las acciones del warm up para este dia.",
            parent=parent,
        )
        self.resize(860, 720)
        self.setMinimumSize(820, 680)
        self._delete_requested = False
        self._stage_id = int(stage.get("id") or 0)
        self._action_cards: list[WarmupActionEditorCard] = []

        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._title_input = QLineEdit()
        self._title_input.setPlaceholderText("Dia 1")
        self._title_input.setText(str(stage.get("title") or "").strip())

        settings = dict(stage.get("settings") or {})
        self._delay_minutes = QSpinBox()
        self._delay_minutes.setRange(0, 24 * 60)
        self._delay_minutes.setValue(max(0, int(settings.get("base_delay_minutes") or 20)))

        form.addWidget(QLabel("Titulo etapa"), 0, 0)
        form.addWidget(self._title_input, 0, 1)
        form.addWidget(QLabel("Pausa base (min)"), 1, 0)
        form.addWidget(self._delay_minutes, 1, 1)
        self.body_layout().addLayout(form)

        actions_toolbar = QHBoxLayout()
        actions_toolbar.setContentsMargins(0, 0, 0, 0)
        actions_toolbar.setSpacing(8)
        actions_title = QLabel("Acciones")
        actions_title.setObjectName("SectionPanelTitle")
        actions_title.setProperty("compact", True)
        add_action_button = QPushButton("Agregar accion")
        add_action_button.setObjectName("SecondaryButton")
        add_action_button.clicked.connect(self._add_action_row)
        actions_toolbar.addWidget(actions_title)
        actions_toolbar.addStretch(1)
        actions_toolbar.addWidget(add_action_button)
        self.body_layout().addLayout(actions_toolbar)

        helper = QLabel(
            "Cada accion tiene su propio bloque. Asi se mantiene legible y evita filas comprimidas al editar."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        self.body_layout().addWidget(helper)

        self._actions_scroll = QScrollArea()
        self._actions_scroll.setObjectName("WarmupActionEditorScroll")
        self._actions_scroll.setWidgetResizable(True)
        self._actions_scroll.setFrameShape(QFrame.NoFrame)
        self._actions_scroll.setMinimumHeight(310)
        self._actions_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._actions_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self._actions_content = QWidget()
        self._actions_content.setObjectName("WarmupActionEditorContent")
        self._actions_layout = QVBoxLayout(self._actions_content)
        self._actions_layout.setContentsMargins(0, 0, 0, 0)
        self._actions_layout.setSpacing(12)
        self._actions_scroll.setWidget(self._actions_content)
        self.body_layout().addWidget(self._actions_scroll)

        seeded_actions = [dict(item) for item in stage.get("actions") or [] if isinstance(item, dict)]
        if not seeded_actions:
            seeded_actions = [{"action_type": "watch_reels", "quantity": 5, "target": "", "text": ""}]
        for action in seeded_actions:
            self._add_action_row(action)
        self._actions_layout.addStretch(1)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        if self._stage_id > 0:
            delete_button = QPushButton("Eliminar etapa")
            delete_button.setObjectName("DangerButton")
            delete_button.clicked.connect(self._request_delete)
            footer.addWidget(delete_button)
        footer.addStretch(1)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        save_button = QPushButton("Guardar etapa")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.accept)
        footer.addWidget(cancel_button)
        footer.addWidget(save_button)
        self.body_layout().addLayout(footer)

    def _request_delete(self) -> None:
        self._delete_requested = True
        self.accept()

    def _add_action_row(self, action: dict[str, Any] | None = None) -> None:
        card = WarmupActionEditorCard(action, on_remove=self._remove_action_card, parent=self._actions_content)
        self._action_cards.append(card)
        insert_at = max(0, self._actions_layout.count() - 1)
        self._actions_layout.insertWidget(insert_at, card)
        QTimer.singleShot(0, lambda current=card: current.setFocus())

    def _remove_action_card(self, card: WarmupActionEditorCard) -> None:
        if card not in self._action_cards:
            return
        if len(self._action_cards) == 1:
            self._actions_layout.removeWidget(card)
            self._action_cards[0].deleteLater()
            self._action_cards.clear()
            self._add_action_row()
            return
        self._action_cards.remove(card)
        self._actions_layout.removeWidget(card)
        card.deleteLater()

    def delete_requested(self) -> bool:
        return self._delete_requested

    def stage_id(self) -> int:
        return self._stage_id

    def stage_title(self) -> str:
        return str(self._title_input.text() or "").strip()

    def settings(self) -> dict[str, Any]:
        return {"base_delay_minutes": int(self._delay_minutes.value())}

    def actions(self) -> list[dict[str, Any]]:
        return [card.action_payload() for card in self._action_cards]


class AddAccountDialog(AccountsModalDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(
            "Agregar cuenta",
            "Crea una cuenta nueva con username y password obligatorios. La clave TOTP es opcional.",
            parent=parent,
        )
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._username = QLineEdit()
        self._username.setPlaceholderText("@usuario")
        self._password = QLineEdit()
        self._password.setEchoMode(QLineEdit.Password)
        self._totp = QLineEdit()
        self._totp.setPlaceholderText("Clave TOTP")
        self._alias = QComboBox()

        form.addWidget(QLabel("Usuario"), 0, 0)
        form.addWidget(self._username, 0, 1)
        form.addWidget(QLabel("Contrasena"), 1, 0)
        form.addWidget(self._password, 1, 1)
        form.addWidget(QLabel("Clave TOTP"), 2, 0)
        form.addWidget(self._totp, 2, 1)
        form.addWidget(QLabel("Alias"), 3, 0)
        form.addWidget(self._alias, 3, 1)
        self.body_layout().addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        save_button = QPushButton("Guardar cuenta")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(save_button)
        self.body_layout().addLayout(actions)

    def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
        _fill_alias_combo(self._alias, aliases, current_alias)

    def username(self) -> str:
        return str(self._username.text() or "").strip().lstrip("@")

    def password(self) -> str:
        return str(self._password.text() or "")

    def totp_secret(self) -> str:
        return str(self._totp.text() or "").strip()

    def alias(self) -> str:
        return str(self._alias.currentData() or self._alias.currentText() or "").strip()


class ImportAccountsDialog(AccountsModalDialog):
    def __init__(self, default_concurrency: int, parent: QWidget | None = None) -> None:
        del default_concurrency
        super().__init__(
            "Importar cuentas CSV",
            "Importa cuentas desde CSV y decide si ejecutar login al finalizar.",
            parent=parent,
        )
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._csv_path = QLineEdit()
        self._csv_path.setPlaceholderText("Selecciona un archivo CSV")
        browse_button = QPushButton("Buscar CSV")
        browse_button.setObjectName("SecondaryButton")
        browse_button.clicked.connect(self._browse_csv)

        self._alias = QComboBox()
        self._login_after_import = QCheckBox("Iniciar sesion despues de importar")
        self._queue_concurrency = 1
        self._concurrency = QLineEdit("Secuencial")
        self._concurrency.setReadOnly(True)
        self._concurrency.setToolTip(
            "El login de cuentas importadas se ejecuta en cola secuencial: una cuenta por vez."
        )

        form.addWidget(QLabel("Archivo CSV"), 0, 0)
        form.addWidget(self._csv_path, 0, 1)
        form.addWidget(browse_button, 0, 2)
        form.addWidget(QLabel("Alias destino"), 1, 0)
        form.addWidget(self._alias, 1, 1, 1, 2)
        form.addWidget(QLabel("Cola login"), 2, 0)
        form.addWidget(self._concurrency, 2, 1)
        form.addWidget(self._login_after_import, 2, 2)
        self.body_layout().addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        import_button = QPushButton("Importar")
        import_button.setObjectName("PrimaryButton")
        import_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(import_button)
        self.body_layout().addLayout(actions)

    def _browse_csv(self) -> None:
        path = _open_dark_file_dialog(self, "Selecciona CSV de cuentas", "CSV (*.csv)")
        if path:
            self._csv_path.setText(path)

    def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
        _fill_alias_combo(self._alias, aliases, current_alias)

    def csv_path(self) -> str:
        return str(self._csv_path.text() or "").strip()

    def alias(self) -> str:
        return str(self._alias.currentData() or self._alias.currentText() or "").strip()

    def login_after_import(self) -> bool:
        return self._login_after_import.isChecked()

    def concurrency(self) -> int:
        return self._queue_concurrency


class ProxyEditorDialog(AccountsModalDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(
            "Agregar proxy",
            "Guarda un proxy sin mezclar el formulario con la tabla principal.",
            parent=parent,
        )
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._proxy_id = QLineEdit()
        self._proxy_server = QLineEdit()
        self._proxy_server.setPlaceholderText("host:puerto o esquema://host:puerto")
        self._proxy_user = QLineEdit()
        self._proxy_pass = QLineEdit()
        self._proxy_pass.setEchoMode(QLineEdit.Password)
        self._proxy_active = QCheckBox("Activo")
        self._proxy_active.setChecked(True)

        form.addWidget(QLabel("Proxy"), 0, 0)
        form.addWidget(self._proxy_id, 0, 1)
        form.addWidget(QLabel("Servidor"), 1, 0)
        form.addWidget(self._proxy_server, 1, 1)
        form.addWidget(QLabel("Usuario"), 2, 0)
        form.addWidget(self._proxy_user, 2, 1)
        form.addWidget(QLabel("Contrasena"), 3, 0)
        form.addWidget(self._proxy_pass, 3, 1)
        form.addWidget(self._proxy_active, 4, 1)
        self.body_layout().addLayout(form)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        save_button = QPushButton("Guardar proxy")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(cancel_button)
        actions.addWidget(save_button)
        self.body_layout().addLayout(actions)

    def payload(self) -> dict[str, Any]:
        return {
            "id": str(self._proxy_id.text() or "").strip(),
            "server": str(self._proxy_server.text() or "").strip(),
            "user": str(self._proxy_user.text() or "").strip(),
            "pass": str(self._proxy_pass.text() or "").strip(),
            "active": self._proxy_active.isChecked(),
        }


class ProxyAssignmentDialog(AccountsModalDialog):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            "Asignar proxy a cuentas",
            "Selecciona un alias y marca las cuentas que deben recibir el proxy seleccionado.",
            parent=parent,
        )
        self._ctx = ctx

        header = QGridLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setHorizontalSpacing(10)
        header.setVerticalSpacing(10)

        self._alias = QComboBox()
        header.addWidget(QLabel("Alias"), 0, 0)
        header.addWidget(self._alias, 0, 1)
        self.body_layout().addLayout(header)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        mark_all = QPushButton("Marcar todas")
        mark_all.setObjectName("SecondaryButton")
        mark_all.clicked.connect(self._mark_all)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_all)
        actions_row.addStretch(1)
        actions_row.addWidget(mark_all)
        actions_row.addWidget(clear_button)
        self.body_layout().addLayout(actions_row)

        self._accounts = QListWidget()
        self._accounts.setObjectName("AccountsAssignList")
        self._accounts.setMinimumHeight(260)
        self._accounts.setSelectionMode(QAbstractItemView.NoSelection)
        self.body_layout().addWidget(self._accounts)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(8)
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("SecondaryButton")
        cancel_button.clicked.connect(self.reject)
        assign_button = QPushButton("Asignar")
        assign_button.setObjectName("PrimaryButton")
        assign_button.clicked.connect(self.accept)
        footer.addStretch(1)
        footer.addWidget(cancel_button)
        footer.addWidget(assign_button)
        self.body_layout().addLayout(footer)

        self._alias.currentIndexChanged.connect(self._refresh_accounts)

    def refresh_aliases(self, aliases: list[str], current_alias: str) -> None:
        _fill_alias_combo(self._alias, aliases, current_alias)
        self._refresh_accounts()

    def _refresh_accounts(self) -> None:
        alias = str(self._alias.currentData() or self._alias.currentText() or "").strip()
        self._accounts.clear()
        for record in self._ctx.services.accounts.list_accounts(alias):
            username = str(record.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            self._accounts.addItem(_checkable_account_item(username, f"@{username}"))

    def _mark_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is not None:
                item.setCheckState(Qt.Checked)

    def _clear_all(self) -> None:
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is not None:
                item.setCheckState(Qt.Unchecked)

    def alias(self) -> str:
        return str(self._alias.currentData() or self._alias.currentText() or "").strip()

    def usernames(self) -> list[str]:
        usernames: list[str] = []
        for row in range(self._accounts.count()):
            item = self._accounts.item(row)
            if item is None or item.checkState() != Qt.Checked:
                continue
            username = str(item.data(Qt.UserRole) or "").strip().lstrip("@")
            if username:
                usernames.append(username)
        return usernames


class AccountsSectionPage(SectionPage):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        route_key: str | None,
        back_button: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(
            ctx,
            title,
            subtitle,
            section_title="Cuentas",
            section_subtitle="Submenu horizontal para separar alias, cuentas, proxies y acciones.",
            section_routes=ACCOUNTS_SUBSECTIONS,
            route_key=route_key,
            back_button=back_button,
            parent=parent,
        )

    def _show_modal_message(
        self,
        title: str,
        message: str,
        *,
        confirm_text: str = "Aceptar",
        cancel_text: str = "",
        danger: bool = False,
    ) -> bool:
        dialog = AccountsAlertDialog(
            title,
            message,
            confirm_text=confirm_text,
            cancel_text=cancel_text,
            danger=danger,
            parent=self,
        )
        return dialog.exec() == QDialog.Accepted

    def prompt_text(
        self,
        *,
        title: str,
        subtitle: str,
        label: str,
        placeholder: str = "",
        text: str = "",
        confirm_text: str = "Guardar",
    ) -> tuple[str, bool]:
        dialog = AccountsTextInputDialog(
            title,
            subtitle,
            label=label,
            placeholder=placeholder,
            text=text,
            confirm_text=confirm_text,
            parent=self,
        )
        accepted = dialog.exec() == QDialog.Accepted
        return dialog.value(), accepted

    def _set_active_alias(self, alias: str | None) -> str:
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            return str(self._ctx.state.active_alias or "").strip()
        aliases_service = getattr(self._ctx.services, "aliases", None)
        if aliases_service is not None:
            try:
                clean_alias = str(aliases_service.set_active_alias(clean_alias) or clean_alias).strip() or clean_alias
            except Exception:
                pass
        self._ctx.state.active_alias = clean_alias
        return clean_alias

    def show_error(self, text: str) -> None:  # type: ignore[override]
        self.set_status(text)
        self._show_modal_message("Error", str(text or "Error"))

    def show_exception(
        self,
        exc: BaseException,
        user_message: str = "No se pudo completar la accion.",
    ) -> None:  # type: ignore[override]
        logger.error(
            "Accounts GUI action failed",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        try:
            self._ctx.logs.append("[error] Accounts GUI action failed\n")
            self._ctx.logs.append(
                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            )
        except Exception:
            pass
        self.set_status(user_message)
        self._show_modal_message("Error", user_message)

    def show_info(self, text: str) -> None:  # type: ignore[override]
        self.set_status(text)
        self._show_modal_message("Informacion", str(text or ""))


class AccountsHomePage(AccountsSectionPage):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            ctx,
            "Cuentas",
            "Submenu horizontal para separar alias, cuentas, proxies y acciones.",
            route_key=None,
            back_button=False,
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Centro de cuentas",
            "La operacion de cuentas queda distribuida en paneles dedicados para reducir ruido visual y separar alias, cuentas, proxies y acciones.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        metrics = QGridLayout()
        metrics.setContentsMargins(0, 0, 0, 0)
        metrics.setHorizontalSpacing(10)
        metrics.setVerticalSpacing(10)
        self._cards = {
            "aliases": ClickableMetricCard("Alias", "0"),
            "accounts": ClickableMetricCard("Cuentas", "0"),
            "connected": ClickableMetricCard("Conectadas", "0"),
            "proxies": ClickableMetricCard("Proxies con uso", "0"),
        }
        self._cards["aliases"].clicked.connect(lambda: self._ctx.open_route("alias_page", None))
        self._cards["accounts"].clicked.connect(lambda: self._ctx.open_route("accounts_page", None))
        self._cards["connected"].clicked.connect(lambda: self._ctx.open_route("accounts_page", None))
        self._cards["proxies"].clicked.connect(lambda: self._ctx.open_route("proxies_page", None))
        for index, key in enumerate(("aliases", "accounts", "connected", "proxies")):
            metrics.addWidget(self._cards[key], index // 2, index % 2)
        layout.addLayout(metrics)

        helper = QLabel(
            "Selecciona una subseccion arriba para trabajar con un panel aislado. "
            "Las altas e importaciones se abren en modales para mantener la pagina limpia."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)
        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._refresh_pending = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        snapshot = payload.get("snapshot") if isinstance(payload, dict) else {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        self._cards["aliases"].set_value(payload.get("aliases_count", 0))
        self._cards["accounts"].set_value(snapshot.get("accounts_total", 0))
        self._cards["connected"].set_value(snapshot.get("accounts_connected", 0))
        self._cards["proxies"].set_value(snapshot.get("proxies_assigned", 0))
        self._summary.setText(str(payload.get("summary") or "").strip())
        self.clear_status()

    def _request_refresh(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        if self._snapshot_cache is None:
            self.set_status("Cargando resumen de cuentas...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_accounts_home_snapshot(
                self._ctx.services,
                active_alias=self._ctx.state.active_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_snapshot(self._snapshot_cache)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar el resumen de cuentas: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._request_refresh()


class AliasPage(AccountsSectionPage):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            ctx,
            "Alias",
            "Gestion de alias con tabla dedicada y acciones puntuales.",
            route_key="alias_page",
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Alias",
            "Cada alias representa un contexto de trabajo. Doble click en una fila para activarlo.",
        )

        toolbar = QFrame()
        toolbar.setObjectName("SectionToolbarCard")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(16, 16, 16, 16)
        toolbar_layout.setSpacing(8)
        create_button = QPushButton("Crear alias")
        create_button.setObjectName("PrimaryButton")
        create_button.clicked.connect(self._create_alias)
        rename_button = QPushButton("Renombrar alias")
        rename_button.setObjectName("SecondaryButton")
        rename_button.clicked.connect(self._rename_selected)
        delete_button = QPushButton("Eliminar alias")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_selected)
        refresh_button = QPushButton("Refrescar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_table)
        toolbar_layout.addWidget(create_button)
        toolbar_layout.addWidget(rename_button)
        toolbar_layout.addWidget(delete_button)
        toolbar_layout.addStretch(1)
        toolbar_layout.addWidget(refresh_button)
        layout.addWidget(toolbar)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["Alias", "Cantidad de cuentas", "Proxies asignados", "Estado"])
        _configure_table(self._table, "AccountsAliasTable", selection_mode=QAbstractItemView.SingleSelection)
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self._table.itemDoubleClicked.connect(lambda _item: self._activate_selected())
        layout.addWidget(self._table)

        helper = QLabel("Consejo: doble click sobre un alias para volverlo el alias activo del resto de paneles.")
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)

        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _selected_alias(self) -> str:
        row = self._table.currentRow()
        if row < 0:
            return ""
        item = self._table.item(row, 0)
        return str(item.text() if item else "").strip()

    def _create_alias(self) -> None:
        alias, accepted = self.prompt_text(
            title="Crear alias",
            subtitle="Crea un nuevo alias para agrupar cuentas en un mismo espacio de trabajo.",
            label="Nombre del alias",
            placeholder="Mi alias",
            confirm_text="Crear alias",
        )
        if not accepted:
            return
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            self.show_error("Ingresa un alias.")
            return
        try:
            result = self._ctx.services.aliases.create_alias(clean_alias, activate=True)
            created = str((result.get("alias") or {}).get("display_name") or clean_alias).strip() or clean_alias
            self._set_active_alias(str(result.get("active_alias") or created).strip() or created)
            self.refresh_table()
            self.set_status(f"Alias creado y activado: {created}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo crear el alias. Ver logs para mas detalles.")

    def _rename_selected(self) -> None:
        current_alias = self._selected_alias()
        if not current_alias:
            self.show_error("Selecciona un alias.")
            return
        if current_alias.lower() == "default":
            self.show_error("No se puede renombrar el alias default.")
            return
        new_alias, accepted = self.prompt_text(
            title="Renombrar alias",
            subtitle="Actualiza el nombre del alias activo sin perder las cuentas vinculadas.",
            label="Nuevo nombre",
            text=current_alias,
            confirm_text="Guardar cambio",
        )
        if not accepted:
            return
        target_alias = str(new_alias or "").strip()
        if not target_alias:
            self.show_error("Ingresa un alias valido.")
            return
        if target_alias.lower() == current_alias.lower():
            self.set_status("El alias no cambio.")
            return
        try:
            result = self._ctx.services.aliases.rename_alias(
                current_alias,
                target_alias,
                activate_target=True,
                running_tasks=self._ctx.tasks.running_task_metadata(),
            )
            created = str(
                (result.get("alias") or result.get("target_alias") or {}).get("display_name")
                or target_alias
            ).strip() or target_alias
            self._set_active_alias(str(result.get("active_alias") or created).strip() or created)
            self.refresh_table()
            self.set_status(f"Alias renombrado: {current_alias} -> {created}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo renombrar el alias. Ver logs para mas detalles.")

    def _activate_selected(self) -> None:
        alias = self._selected_alias()
        if not alias:
            self.show_error("Selecciona un alias.")
            return
        self._set_active_alias(alias)
        self.refresh_table()
        self.set_status(f"Alias activo: {self._ctx.state.active_alias}")

    def _delete_selected(self) -> None:
        alias = self._selected_alias()
        if not alias:
            self.show_error("Selecciona un alias.")
            return
        snapshot = self._ctx.services.accounts.get_alias_snapshot(alias)
        try:
            options = [
                item
                for item in self._ctx.services.accounts.list_aliases()
                if item.lower() != alias.lower()
            ]
            if int(snapshot.get("accounts_total") or 0) > 0:
                if not options:
                    raise ServiceError("El alias tiene cuentas y no hay alias destino para moverlas.")
                dialog = AliasDeleteDialog(alias, move_options=options, parent=self)
                if dialog.exec() != QDialog.Accepted:
                    return
                result = self._ctx.services.aliases.delete_alias(
                    alias,
                    move_accounts_to=dialog.target_alias(),
                    running_tasks=self._ctx.tasks.running_task_metadata(),
                )
            else:
                confirmed = self._show_modal_message(
                    "Eliminar alias",
                    f"Confirma la eliminacion del alias {alias}.",
                    confirm_text="Eliminar alias",
                    cancel_text="Cancelar",
                    danger=True,
                )
                if not confirmed:
                    return
                result = self._ctx.services.aliases.delete_alias(
                    alias,
                    running_tasks=self._ctx.tasks.running_task_metadata(),
                )
            self._set_active_alias(str(result.get("active_alias") or self._ctx.state.active_alias).strip() or "default")
            self.refresh_table()
            self.set_status(f"Alias eliminado: {alias}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar el alias. Ver logs para mas detalles.")

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        rows = [item for item in payload.get("rows") or [] if isinstance(item, dict)]
        self._table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            values = [
                row.get("alias", ""),
                row.get("accounts_total", 0),
                row.get("proxies_assigned", 0),
                row.get("status", ""),
            ]
            for column, value in enumerate(values):
                self._table.setItem(row_index, column, table_item(value))
        self.clear_status()

    def refresh_table(self) -> None:
        if self._snapshot_loading:
            self._refresh_pending = True
            return
        self._refresh_pending = False
        self._snapshot_loading = True
        if self._snapshot_cache is None:
            self.set_status("Cargando alias...")
        else:
            self._apply_snapshot(self._snapshot_cache)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_alias_page_snapshot(
                self._ctx.services,
                active_alias=self._ctx.state.active_alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        self._apply_snapshot(self._snapshot_cache)
        if self._refresh_pending:
            self.refresh_table()

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudieron cargar los alias: {error.message}")
        if self._refresh_pending:
            self.refresh_table()

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_table()


class AccountsPage(AccountsSectionPage):
    LOGIN_TASK = "accounts_login"
    RELOGIN_TASK = "accounts_relogin"
    LOGIN_TASKS = {LOGIN_TASK, RELOGIN_TASK}
    IMPORT_TASK = "accounts_import"
    HEALTH_REFRESH_TASK = "accounts_health_refresh"
    OPEN_ACCOUNT_TASK = "accounts_open_account"

    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            ctx,
            "Cuentas",
            "Tabla unica para cuentas, seleccion multiple y acciones directas.",
            route_key="accounts_page",
            parent=parent,
        )
        self._records: list[dict[str, Any]] = []
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._requested_alias = ""
        self._refresh_after_current = False
        self._after_snapshot_actions: list[Callable[[], None]] = []
        self._pending_import_request: dict[str, Any] | None = None
        self._pending_login_completion_message: str | None = None
        self._active_login_usernames: list[str] = []
        self._opening_account_usernames: list[str] = []
        self._login_progress_timer = QTimer(self)
        self._login_progress_timer.setInterval(700)
        self._login_progress_timer.timeout.connect(self._refresh_login_progress)

        panel, layout = self.create_panel(
            "Cuentas",
            "Importa, selecciona y opera sobre una sola tabla. La lectura corre en background y las acciones usan tareas separadas.",
        )

        toolbar = QFrame()
        toolbar.setObjectName("SectionToolbarCard")
        toolbar_layout = QGridLayout(toolbar)
        toolbar_layout.setContentsMargins(16, 16, 16, 16)
        toolbar_layout.setHorizontalSpacing(10)
        toolbar_layout.setVerticalSpacing(10)

        self._alias_filter = QComboBox()
        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Buscar cuenta")
        self._login_queue_mode = QLineEdit("Secuencial")
        self._login_queue_mode.setReadOnly(True)
        self._login_queue_mode.setToolTip(
            "El login/relogin de cuentas usa Google Chrome visible en cola secuencial: una cuenta por vez."
        )
        self._force_relogin = QCheckBox("Forzar relogin")

        add_button = QPushButton("+ Agregar cuenta")
        add_button.setObjectName("PrimaryButton")
        add_button.clicked.connect(self._open_add_account_dialog)
        import_button = QPushButton("Importar cuentas CSV")
        import_button.setObjectName("SecondaryButton")
        import_button.clicked.connect(self._open_import_dialog)
        open_account_button = QPushButton("Abrir cuenta")
        open_account_button.setObjectName("SecondaryButton")
        open_account_button.clicked.connect(self._open_selected_accounts)
        refresh_button = QPushButton("Refrescar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self._refresh_health)

        toolbar_layout.addWidget(QLabel("Alias activo"), 0, 0)
        toolbar_layout.addWidget(self._alias_filter, 0, 1)
        toolbar_layout.addWidget(QLabel("Buscar cuenta"), 0, 2)
        toolbar_layout.addWidget(self._search_input, 0, 3)
        toolbar_layout.addWidget(QLabel("Cola login"), 0, 4)
        toolbar_layout.addWidget(self._login_queue_mode, 0, 5)
        toolbar_layout.addWidget(add_button, 0, 6)
        toolbar_layout.addWidget(import_button, 0, 7)
        toolbar_layout.addWidget(self._force_relogin, 1, 4)
        toolbar_layout.addWidget(refresh_button, 1, 6)
        toolbar_layout.addWidget(open_account_button, 1, 7)
        layout.addWidget(toolbar)

        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(["Username", "Conectada", "Health", "Proxy", "Estado de uso", "Limite"])
        _configure_table(self._table, "AccountsTable", selection_mode=QAbstractItemView.MultiSelection)
        self._table.setMinimumHeight(360)
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        layout.addWidget(self._table)

        actions_card = QFrame()
        actions_card.setObjectName("SectionToolbarCard")
        actions_layout = QHBoxLayout(actions_card)
        actions_layout.setContentsMargins(16, 16, 16, 16)
        actions_layout.setSpacing(8)
        login_button = QPushButton("Iniciar sesion seleccionadas")
        login_button.setObjectName("PrimaryButton")
        login_button.clicked.connect(self._login_selected)
        select_visible_button = QPushButton("Seleccionar visibles")
        select_visible_button.setObjectName("SecondaryButton")
        select_visible_button.clicked.connect(self._select_visible_rows)
        clear_selection_button = QPushButton("Limpiar seleccion")
        clear_selection_button.setObjectName("SecondaryButton")
        clear_selection_button.clicked.connect(self._clear_selection)
        alias_change_button = QPushButton("Cambio de alias")
        alias_change_button.setObjectName("SecondaryButton")
        alias_change_button.clicked.connect(self._open_alias_change_dialog)
        delete_button = QPushButton("Eliminar seleccionadas")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_selected)
        self._limit_spin = QSpinBox()
        self._limit_spin.setRange(1, 500)
        self._limit_spin.setValue(20)
        apply_button = QPushButton("Aplicar limite")
        apply_button.setObjectName("SecondaryButton")
        apply_button.clicked.connect(self._apply_limit)
        usage_state_button = QPushButton("Estado")
        usage_state_button.setObjectName("SecondaryButton")
        usage_state_button.clicked.connect(self._open_usage_state_dialog)
        actions_layout.addWidget(login_button)
        actions_layout.addWidget(select_visible_button)
        actions_layout.addWidget(clear_selection_button)
        actions_layout.addWidget(alias_change_button)
        actions_layout.addWidget(delete_button)
        actions_layout.addStretch(1)
        actions_layout.addWidget(QLabel("Limite"))
        actions_layout.addWidget(self._limit_spin)
        actions_layout.addWidget(apply_button)
        actions_layout.addWidget(usage_state_button)
        layout.addWidget(actions_card)

        helper = QLabel(
            "La seleccion se hace directamente en la tabla. "
            "El login abre Google Chrome real en modo visible y procesa una cuenta por vez para guardar la sesion persistente."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)

        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)

        self._alias_filter.currentIndexChanged.connect(self._on_alias_changed)
        self._search_input.textChanged.connect(self._apply_search_filter)
        self._table.itemSelectionChanged.connect(self._update_summary)
        self._ctx.tasks.taskStarted.connect(self._on_task_started)
        self._ctx.tasks.taskFinished.connect(self._on_task_finished)
        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)

    def _on_task_started(self, task_name: str) -> None:
        if task_name not in self.LOGIN_TASKS:
            return
        if not self._login_progress_timer.isActive():
            self._login_progress_timer.start()

    def _refresh_login_progress(self) -> None:
        if not any(self._ctx.tasks.is_running(task_name) for task_name in self.LOGIN_TASKS):
            self._login_progress_timer.stop()
            return
        self.refresh_table()

    def _on_task_finished(self, task_name: str, ok: bool, message: str) -> None:
        if task_name == self.OPEN_ACCOUNT_TASK:
            opened_count = len(self._opening_account_usernames)
            self._opening_account_usernames = []
            if ok:
                noun = "cuenta" if opened_count == 1 else "cuentas"
                self.set_status(f"Apertura manual finalizada para {opened_count} {noun}.")
            else:
                self.set_status(message or "No se pudo abrir la cuenta seleccionada.")
            self.refresh_table()
            return
        if task_name not in self.LOGIN_TASKS:
            return
        self._login_progress_timer.stop()
        if self._active_login_usernames:
            clearer = getattr(self._ctx.services.accounts, "clear_login_progress", None)
            if callable(clearer):
                with contextlib.suppress(Exception):
                    clearer(self._active_login_usernames)
        self._active_login_usernames = []
        final_message = (
            self._pending_login_completion_message
            if ok and str(self._pending_login_completion_message or "").strip()
            else message or ("Estado de cuentas sincronizado." if ok else "No se pudo completar la sesion.")
        )
        self._pending_login_completion_message = None
        self._queue_after_snapshot(lambda text=final_message: self.set_status(text))
        self.refresh_table()

    def _on_task_completed(self, task_name: str, ok: bool, message: str, result: object) -> None:
        if task_name in self.LOGIN_TASKS:
            self._pending_login_completion_message = self._summarize_login_completion(
                task_name,
                ok=ok,
                message=message,
                result=result,
            )
            return
        if task_name == self.HEALTH_REFRESH_TASK:
            summary = self._summarize_health_refresh(ok=ok, message=message, result=result)
            if summary:
                self._queue_after_snapshot(lambda text=summary: self.set_status(text))
            self.refresh_table()
            return
        if task_name != self.IMPORT_TASK:
            return
        request = dict(self._pending_import_request or {})
        self._pending_import_request = None
        if not ok:
            self.set_status(message or "La importacion no pudo completarse.")
            return
        payload = dict(result) if isinstance(result, dict) else {}
        self._queue_after_snapshot(lambda data=payload, meta=request: self._complete_import(data, meta))
        self.refresh_table()

    def _selected_usernames(self) -> list[str]:
        usernames: list[str] = []
        seen: set[str] = set()
        selection_model = self._table.selectionModel()
        if selection_model is None:
            return usernames
        for index in selection_model.selectedRows():
            username_item = self._table.item(index.row(), 0)
            username = str(username_item.text() if username_item else "").strip().lstrip("@")
            key = username.lower()
            if username and key not in seen:
                usernames.append(username)
                seen.add(key)
        return usernames

    def _visible_usernames(self) -> list[str]:
        usernames: list[str] = []
        for row in range(self._table.rowCount()):
            if self._table.isRowHidden(row):
                continue
            username_item = self._table.item(row, 0)
            username = str(username_item.text() if username_item else "").strip().lstrip("@")
            if username:
                usernames.append(username)
        return usernames

    def _current_alias(self) -> str:
        alias = str(self._alias_filter.currentData() or self._alias_filter.currentText() or "").strip()
        return alias or self._ctx.state.active_alias

    def _login_queue_concurrency(self) -> int:
        provider = getattr(self._ctx.services.accounts, "login_queue_concurrency", None)
        if callable(provider):
            try:
                return max(1, int(provider()))
            except Exception:
                return 1
        return 1

    def _summarize_login_completion(
        self,
        task_name: str,
        *,
        ok: bool,
        message: str,
        result: object,
    ) -> str:
        if not ok:
            return message or "No se pudo completar la sesion."
        rows = [dict(item) for item in result or [] if isinstance(item, dict)]
        if not rows:
            return "Estado de cuentas sincronizado."
        completed = 0
        failed = 0
        missing_password = 0
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            row_message = str(row.get("message") or "").strip().lower()
            if status in {"ok", "success"}:
                completed += 1
                continue
            failed += 1
            if row_message == "missing_password":
                missing_password += 1
        action_label = "Relogin" if task_name == self.RELOGIN_TASK else "Login"
        summary = f"{action_label} finalizado: {completed} correctas, {failed} con error."
        if missing_password:
            summary += f" Sin password guardado: {missing_password}."
        return summary

    def _summarize_health_refresh(self, *, ok: bool, message: str, result: object) -> str:
        if not ok:
            return message or "No se pudo refrescar health."
        payload = dict(result) if isinstance(result, dict) else {}
        eligible = int(payload.get("eligible") or 0)
        refreshed = int(payload.get("refreshed") or 0)
        if eligible <= 0:
            return "No hay cuentas conectadas para refrescar health."
        summary = (
            f"Health refrescado: {refreshed}/{eligible} conectadas revisadas"
            f" | VIVA {int(payload.get('alive') or 0)}"
            f" | NO ACTIVA {int(payload.get('inactive') or 0)}"
            f" | MUERTA {int(payload.get('dead') or 0)}"
        )
        errors = int(payload.get("errors") or 0)
        if errors:
            summary += f" | errores {errors}"
        return summary

    def _refresh_health(self) -> None:
        if self._ctx.tasks.is_running(self.HEALTH_REFRESH_TASK):
            self.set_status("El refresh de health ya esta en ejecucion.")
            return
        alias = self._current_alias() or self._ctx.state.active_alias
        clean_alias = str(alias or "").strip()
        if not clean_alias:
            self.show_error("Selecciona un alias.")
            return
        self._set_active_alias(clean_alias)
        try:
            self._ctx.tasks.start_task(
                self.HEALTH_REFRESH_TASK,
                lambda alias=clean_alias: self._ctx.services.accounts.refresh_connected_health(alias),
                metadata={"alias": clean_alias},
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo iniciar el refresh de health.")
            return
        self.set_status("Refrescando health de cuentas conectadas...")

    def _refresh_alias_combo(self, aliases: list[str] | None = None) -> None:
        alias_rows = list(aliases or [])
        if not alias_rows:
            alias_rows = self._ctx.services.accounts.list_aliases()
        current_alias = self._current_alias() or self._ctx.state.active_alias
        if current_alias:
            current_alias = next(
                (alias for alias in alias_rows if alias.lower() == current_alias.lower()),
                current_alias,
            )
        _fill_alias_combo(self._alias_filter, alias_rows, current_alias)

    def _on_alias_changed(self) -> None:
        alias = self._current_alias()
        if alias:
            self._set_active_alias(alias)
        self.refresh_table()

    def _queue_after_snapshot(self, callback: Callable[[], None]) -> None:
        self._after_snapshot_actions.append(callback)

    def _run_after_snapshot_actions(self) -> None:
        actions = self._after_snapshot_actions[:]
        self._after_snapshot_actions.clear()
        for callback in actions:
            try:
                callback()
            except Exception as exc:
                self.show_exception(exc, "No se pudo completar la actualizacion posterior al refresh.")

    def _render_records(self, preserve_selection: list[str] | None = None) -> None:
        selected = {str(item or "").strip().lstrip("@").lower() for item in preserve_selection or []}
        self._table.blockSignals(True)
        self._table.clearSelection()
        self._table.setRowCount(len(self._records))
        selection_model = self._table.selectionModel()
        for row_index, record in enumerate(self._records):
            username = str(record.get("username") or "").strip().lstrip("@")
            connected_label = record.get("connected_label") or ("Si" if bool(record.get("connected")) else "No")
            health_label = record.get("health_badge") or "-"
            progress_label = str(record.get("login_progress_label") or "").strip()
            if progress_label:
                connected_label = "Pendiente"
                health_label = progress_label
            values = [
                f"@{username or '-'}",
                connected_label,
                health_label,
                record.get("proxy_label") or "-",
                record.get("usage_state_label") or "Activa",
                record.get("message_limit_label") or message_limit(record),
            ]
            for column, value in enumerate(values):
                self._table.setItem(row_index, column, table_item(value))
            if selection_model is not None and username.lower() in selected:
                index = self._table.model().index(row_index, 0)
                selection_model.select(index, QItemSelectionModel.Select | QItemSelectionModel.Rows)
        self._table.blockSignals(False)
        self._apply_search_filter()

    def _update_summary(self) -> None:
        alias = self._current_alias() or self._ctx.state.active_alias or "-"
        visible = 0
        connected = 0
        for row_index, record in enumerate(self._records):
            if self._table.isRowHidden(row_index):
                continue
            visible += 1
            if bool(record.get("connected")):
                connected += 1
        selected = len(self._selected_usernames())
        pending = sum(1 for record in self._records if bool(record.get("login_progress_active")))
        summary = (
            f"Alias activo: {alias}  |  "
            f"Visibles: {visible}  |  "
            f"Seleccionadas: {selected}  |  "
            f"Conectadas visibles: {connected}"
        )
        if pending:
            summary += f"  |  En proceso: {pending}"
        self._summary.setText(summary)

    def _apply_search_filter(self) -> None:
        needle = str(self._search_input.text() or "").strip().lstrip("@").lower()
        for row in range(self._table.rowCount()):
            item = self._table.item(row, 0)
            username = str(item.text() if item else "").strip().lstrip("@").lower()
            self._table.setRowHidden(row, bool(needle) and needle not in username)
        self._update_summary()

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        aliases = [str(item or "").strip() for item in payload.get("aliases") or [] if str(item or "").strip()]
        self._refresh_alias_combo(aliases)
        active_alias = str(payload.get("active_alias") or self._current_alias() or self._ctx.state.active_alias).strip()
        if active_alias:
            self._set_active_alias(active_alias)
        selected = self._selected_usernames()
        self._records = [dict(item) for item in payload.get("rows") or [] if isinstance(item, dict)]
        self._render_records(selected)
        self.clear_status()

    def refresh_table(self) -> None:
        alias = self._current_alias()
        if alias:
            self._set_active_alias(alias)
        if self._snapshot_loading:
            self._refresh_after_current = True
            return
        requested_alias = str(self._ctx.state.active_alias or alias).strip()
        if requested_alias:
            self._requested_alias = requested_alias
        self._snapshot_loading = True
        cached_payload = (
            self._snapshot_cache
            if isinstance(self._snapshot_cache, dict)
            and str(self._snapshot_cache.get("active_alias") or "").strip().lower()
            == self._requested_alias.lower()
            else None
        )
        if cached_payload is None:
            self.set_status("Cargando cuentas...")
        else:
            self._apply_snapshot(cached_payload)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda alias=self._requested_alias: build_accounts_table_snapshot(
                self._ctx.services,
                active_alias=alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        payload_alias = str(self._snapshot_cache.get("active_alias") or "").strip()
        current_alias = str(self._current_alias() or self._ctx.state.active_alias).strip()
        needs_follow_up = self._refresh_after_current
        self._refresh_after_current = False
        if (current_alias and payload_alias and current_alias.lower() != payload_alias.lower()) or needs_follow_up:
            self.refresh_table()
            return
        self._apply_snapshot(self._snapshot_cache)
        self._run_after_snapshot_actions()

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudieron cargar las cuentas: {error.message}")

    def _open_add_account_dialog(self) -> None:
        dialog = AddAccountDialog(self)
        aliases = self._ctx.services.accounts.list_aliases()
        dialog.refresh_aliases(aliases, self._current_alias())
        if dialog.exec() != QDialog.Accepted:
            return
        username = dialog.username()
        alias = dialog.alias()
        password = dialog.password()
        if not username or not alias or not str(password or "").strip():
            self.show_error("Username, password y alias son obligatorios.")
            return
        try:
            created = self._ctx.services.accounts.add_account(
                username,
                alias,
                password=password,
                totp_secret=dialog.totp_secret(),
            )
            if not created:
                raise ServiceError("La cuenta ya existe o no pudo agregarse.")
            self._set_active_alias(alias)
            self._queue_after_snapshot(lambda user=username: self.set_status(f"Cuenta agregada: @{user}"))
            self.refresh_table()
        except Exception as exc:
            self.show_exception(exc, "No se pudo agregar la cuenta. Ver logs para mas detalles.")

    def _open_import_dialog(self) -> None:
        dialog = ImportAccountsDialog(self._login_queue_concurrency(), self)
        aliases = self._ctx.services.accounts.list_aliases()
        dialog.refresh_aliases(aliases, self._current_alias())
        if dialog.exec() != QDialog.Accepted:
            return
        path = dialog.csv_path()
        alias = dialog.alias()
        if not path or not alias:
            self.show_error("Selecciona archivo y alias destino.")
            return
        self._set_active_alias(alias)
        self._pending_import_request = {
            "alias": alias,
            "login_after_import": dialog.login_after_import(),
            "concurrency": dialog.concurrency(),
        }
        self._ctx.tasks.start_task(
            self.IMPORT_TASK,
            lambda: self._ctx.services.accounts.import_accounts_csv(
                alias,
                path,
                login_after_import=False,
                concurrency=dialog.concurrency(),
            ),
            metadata={"alias": alias},
        )
        self.set_status("Importacion en ejecucion...")

    def _select_visible_rows(self) -> None:
        selection_model = self._table.selectionModel()
        if selection_model is None:
            return
        for row in range(self._table.rowCount()):
            if self._table.isRowHidden(row):
                continue
            index = self._table.model().index(row, 0)
            selection_model.select(index, QItemSelectionModel.Select | QItemSelectionModel.Rows)
        self._update_summary()

    def _clear_selection(self) -> None:
        self._table.clearSelection()
        self._update_summary()

    def _open_alias_change_dialog(self) -> None:
        current_alias = str(self._current_alias() or self._ctx.state.active_alias).strip()
        if not current_alias:
            self.show_error("Selecciona un alias.")
            return
        if self._snapshot_loading and not self._records:
            self.show_error("Espera a que termine la carga del alias actual.")
            return
        if not self._records:
            self.show_error("No hay cuentas disponibles en el alias actual.")
            return
        target_aliases = [
            str(alias or "").strip()
            for alias in self._ctx.services.accounts.list_aliases()
            if str(alias or "").strip() and str(alias or "").strip().lower() != current_alias.lower()
        ]
        if not target_aliases:
            self.show_error("Necesitas al menos otro alias destino para mover cuentas.")
            return
        dialog = AccountAliasChangeDialog(self)
        dialog.refresh(
            alias=current_alias,
            target_aliases=target_aliases,
            records=self._records,
            selected_usernames=self._selected_usernames(),
        )
        if dialog.exec() != QDialog.Accepted:
            return
        usernames = dialog.selected_usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta para cambiar de alias.")
            return
        target_alias = dialog.target_alias()
        if not target_alias:
            self.show_error("Selecciona un alias destino.")
            return
        if target_alias.lower() == current_alias.lower():
            self.show_error("Selecciona un alias destino distinto al alias actual.")
            return
        try:
            moved = int(self._ctx.services.accounts.move_accounts(usernames, target_alias) or 0)
        except Exception as exc:
            self.show_exception(exc, "No se pudo completar el cambio de alias.")
            return
        self._queue_after_snapshot(
            lambda moved_count=moved, total=len(usernames), alias_label=target_alias: self.set_status(
                f"Cambio de alias aplicado: {moved_count}/{total} cuenta(s) movidas a {alias_label}."
            )
        )
        self.refresh_table()

    def _delete_selected(self) -> None:
        usernames = self._selected_usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta.")
            return
        confirmed = self._show_modal_message(
            "Eliminar cuentas",
            f"Confirma la eliminacion de {len(usernames)} cuenta(s) seleccionada(s).",
            confirm_text="Eliminar cuentas",
            cancel_text="Cancelar",
            danger=True,
        )
        if not confirmed:
            return
        removed = self._ctx.services.accounts.remove_accounts(usernames)
        self._queue_after_snapshot(lambda count=removed: self.set_status(f"Cuentas eliminadas: {count}"))
        self.refresh_table()

    def _apply_limit(self) -> None:
        usernames = self._selected_usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta.")
            return
        updated = self._ctx.services.accounts.set_message_limit(usernames, self._limit_spin.value())
        self._queue_after_snapshot(lambda count=updated: self.set_status(f"Limite aplicado a {count} cuentas."))
        self.refresh_table()

    def _apply_usage_state(self, usage_state: str, usernames: list[str]) -> None:
        normalized = str(usage_state or "").strip().lower()
        if normalized not in {"active", "deactivated"}:
            self.show_error("Estado invalido.")
            return
        targets = [str(item or "").strip().lstrip("@") for item in usernames if str(item or "").strip()]
        if not targets:
            self.show_error("No hay cuentas para actualizar.")
            return
        updated = self._ctx.services.accounts.set_usage_state(targets, normalized)
        action_label = "activado" if normalized == "active" else "desactivado"
        self._queue_after_snapshot(
            lambda count=updated, label=action_label: self.set_status(f"Estado {label} en {count} cuentas.")
        )
        self.refresh_table()

    def _open_usage_state_dialog(self) -> None:
        selected = self._selected_usernames()
        visible = self._visible_usernames()
        if not selected and not visible:
            self.show_error("No hay cuentas disponibles para actualizar.")
            return
        dialog = AccountUsageStateDialog(
            selected_count=len(selected),
            visible_count=len(visible),
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        scope = dialog.scope()
        usernames = visible if scope == "visible" else selected
        if scope != "visible" and not usernames:
            self.show_error("Selecciona al menos una cuenta o usa las visibles.")
            return
        self._apply_usage_state(dialog.usage_state(), usernames)

    def _run_login(
        self,
        *,
        usernames: list[str] | None = None,
        alias: str | None = None,
        force_relogin: bool | None = None,
        concurrency: int | None = None,
        announce: bool = True,
        origin_label: str = "",
    ) -> None:
        clean_alias = str(alias or self._current_alias()).strip()
        if not clean_alias:
            self.show_error("Selecciona un alias.")
            return
        target_usernames = list(usernames or [str(record.get("username") or "").strip() for record in self._records])
        target_usernames = [item.lstrip("@") for item in target_usernames if str(item or "").strip()]
        if not target_usernames:
            self.show_error("No hay cuentas disponibles para iniciar sesion.")
            return
        force = self._force_relogin.isChecked() if force_relogin is None else bool(force_relogin)
        task_name = self.RELOGIN_TASK if force else self.LOGIN_TASK
        login_concurrency = self._login_queue_concurrency()
        self._active_login_usernames = list(target_usernames)
        queue_progress = getattr(self._ctx.services.accounts, "queue_login_progress", None)
        if callable(queue_progress):
            with contextlib.suppress(Exception):
                queue_progress(target_usernames)
        self.refresh_table()
        try:
            self._ctx.tasks.start_task(
                task_name,
                lambda: (
                    self._ctx.services.accounts.relogin(
                        clean_alias,
                        usernames=target_usernames if usernames is not None else None,
                        concurrency=login_concurrency,
                    )
                    if force
                    else self._ctx.services.accounts.login(
                        clean_alias,
                        usernames=target_usernames if usernames is not None else None,
                        concurrency=login_concurrency,
                    )
                ),
                metadata={"alias": clean_alias},
            )
        except Exception as exc:
            self._active_login_usernames = []
            clearer = getattr(self._ctx.services.accounts, "clear_login_progress", None)
            if callable(clearer):
                with contextlib.suppress(Exception):
                    clearer(target_usernames)
            self._render_records(self._selected_usernames())
            self.show_exception(exc, "No se pudo iniciar la sesion. Ver logs para mas detalles.")
            return
        if announce:
            scope = origin_label or ("cuentas seleccionadas" if usernames is not None else f"alias {clean_alias}")
            verb = "Relogin" if force else "Login"
            self.set_status(
                f"{verb} iniciado para {scope}. "
                "Google Chrome se abrira en modo visible y procesara una cuenta por vez."
            )

    def _login_selected(self) -> None:
        usernames = self._selected_usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta.")
            return
        self._run_login(usernames=usernames, origin_label="cuentas seleccionadas")

    def _open_selected_accounts(self) -> None:
        usernames = self._selected_usernames()
        if not usernames:
            self.show_info("No se selecciono ninguna cuenta. Selecciona una cuenta para abrir.")
            return
        if self._ctx.tasks.is_running(self.OPEN_ACCOUNT_TASK):
            self.show_error("Ya hay una apertura de cuentas en ejecucion.")
            return
        clean_alias = str(self._current_alias() or self._ctx.state.active_alias).strip()
        if not clean_alias:
            self.show_error("Selecciona un alias.")
            return
        self._set_active_alias(clean_alias)
        self._opening_account_usernames = list(usernames)
        try:
            self._ctx.tasks.start_task(
                self.OPEN_ACCOUNT_TASK,
                lambda alias=clean_alias, selected=list(usernames): self._ctx.services.accounts.open_account_profiles(
                    alias,
                    selected,
                    action_label="Abrir cuenta",
                ),
                metadata={"alias": clean_alias},
            )
        except Exception as exc:
            self._opening_account_usernames = []
            self.show_exception(exc, "No se pudo abrir la cuenta seleccionada.")
            return
        noun = "cuenta" if len(usernames) == 1 else "cuentas"
        self.set_status(f"Abriendo {len(usernames)} {noun} seleccionada(s) en el perfil...")

    def _complete_import(self, payload: dict[str, Any], request: dict[str, Any]) -> None:
        added = int(payload.get("added") or 0)
        skipped = int(payload.get("skipped") or 0)
        login_usernames = [
            str(item or "").strip().lstrip("@")
            for item in payload.get("login_usernames") or []
            if str(item or "").strip()
        ]
        if bool(request.get("login_after_import")) and login_usernames:
            self._run_login(
                usernames=login_usernames,
                alias=str(payload.get("alias") or request.get("alias") or self._current_alias()).strip(),
                force_relogin=False,
                concurrency=self._login_queue_concurrency(),
                announce=False,
                origin_label="cuentas importadas",
            )
            self.set_status(
                f"Importacion lista: {added} agregadas, {skipped} omitidas. "
                f"Login iniciado para {len(login_usernames)} cuentas importadas."
            )
            return
        self.set_status(f"Importacion lista: {added} agregadas, {skipped} omitidas.")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_table()


class AccountsActionsPage(AccountsSectionPage):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            ctx,
            "Acciones",
            "Acciones manuales y calentamiento de cuentas del alias activo.",
            route_key="accounts_actions_page",
            parent=parent,
        )
        self._records: list[dict[str, Any]] = []
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None
        self._requested_alias = ""
        self._refresh_after_current = False

        self._manual_action_kind = ""
        self._manual_alias = ""
        self._manual_queue: list[str] = []
        self._manual_current_username = ""
        self._manual_abort_requested = False

        self._selected_view_alias = ""
        self._selected_view_usernames: list[str] = []
        self._view_log_cursor: int | None = None
        self._view_log_sync_pending = False
        self._active_accounts_module = _ACCOUNTS_MODULE_SELECTOR
        self._warmup_flow_id = 0
        self._warmup_flow_alias = ""
        self._warmup_flow_name = ""
        self._warmup_selected_usernames: list[str] = []
        self._warmup_stages: list[dict[str, Any]] = []
        self._warmup_resume: dict[str, Any] = {}
        self._warmup_has_started = False
        self._warmup_log_id = 0
        self._visible = False

        changes_panel, changes_layout = self.create_panel(
            "Cambios en cuentas de Instagram",
            "Cada accion te permite elegir alias y cuentas antes de abrir Playwright para cambios manuales sin bloquear la interfaz.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        changes_layout.addWidget(self._summary)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        username_button = QPushButton("Cambiar username")
        username_button.setObjectName("PrimaryButton")
        username_button.clicked.connect(lambda: self._start_manual_sequence("username"))
        full_name_button = QPushButton("Cambiar full name")
        full_name_button.setObjectName("SecondaryButton")
        full_name_button.clicked.connect(lambda: self._start_manual_sequence("full_name"))
        other_button = QPushButton("Otros cambios")
        other_button.setObjectName("SecondaryButton")
        other_button.clicked.connect(lambda: self._start_manual_sequence("other"))
        content_button = QPushButton("Publicación de contenido")
        content_button.setObjectName("SecondaryButton")
        content_button.clicked.connect(self._open_content_module)
        actions_row.addWidget(username_button)
        actions_row.addWidget(full_name_button)
        actions_row.addWidget(other_button)
        actions_row.addWidget(content_button)
        actions_row.addStretch(1)
        changes_layout.addLayout(actions_row)

        manual_helper = QLabel(
            "Al presionar cualquier accion, se abre un selector para elegir alias y cuentas conectadas dentro de ese alias. El health ya no bloquea esta seccion."
        )
        manual_helper.setObjectName("SectionPanelHint")
        manual_helper.setWordWrap(True)
        changes_layout.addWidget(manual_helper)

        self._manual_log = QPlainTextEdit()
        self._manual_log.setObjectName("AccountsManualLog")
        self._manual_log.setReadOnly(True)
        self._manual_log.setMinimumHeight(180)
        changes_layout.addWidget(self._manual_log)

        self._content_controller = ContentUIController(
            self._ctx,
            parent=self,
            on_back=lambda: self._show_accounts_module(_ACCOUNTS_MODULE_SELECTOR),
            on_status=self.set_status,
            show_error=self.show_error,
            show_exception=self.show_exception,
        )

        self._modules_stack = QStackedWidget()
        self._modules_stack.setObjectName("AccountsActionsModuleStack")
        self._modules_stack.addWidget(self._build_modules_selector_page())
        self._modules_stack.addWidget(self._build_view_content_page())
        self._modules_stack.addWidget(self._build_warmup_page())
        self._actions_home = QWidget()
        self._actions_home.setObjectName("AccountsActionsHome")
        actions_home_layout = QVBoxLayout(self._actions_home)
        actions_home_layout.setContentsMargins(0, 0, 0, 0)
        actions_home_layout.setSpacing(12)
        actions_home_layout.addWidget(changes_panel)
        actions_home_layout.addWidget(self._modules_stack)
        actions_home_layout.addStretch(1)

        self._content_stack = QStackedWidget()
        self._content_stack.setObjectName("AccountsActionsContentStack")
        self._content_stack.addWidget(self._actions_home)
        self._content_stack.addWidget(self._content_controller.widget())

        self.content_layout().addWidget(self._content_stack)
        self.content_layout().addStretch(1)

        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)
        self._ctx.tasks.taskFinished.connect(self._on_task_finished)
        self._ctx.logs.logAdded.connect(self._on_global_log_added)
        self._warmup_log_timer = QTimer(self)
        self._warmup_log_timer.setInterval(350)
        self._warmup_log_timer.timeout.connect(self._sync_warmup_logs)
        self._warmup_log_timer.start()
        self._show_accounts_module(_ACCOUNTS_MODULE_SELECTOR)

    def _build_modules_selector_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        selector_panel, selector_layout = self.create_panel(
            "Warm Up",
            "Selecciona el modulo que quieres abrir para seguir usando ver contenido o editar el flujo de warm up.",
        )
        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 4, 0, 0)
        actions_row.setSpacing(12)
        actions_row.addStretch(1)

        warmup_button = QPushButton("Warm Up")
        warmup_button.setObjectName("PrimaryButton")
        warmup_button.setMinimumWidth(180)
        warmup_button.clicked.connect(self._open_warmup_launcher)

        view_button = QPushButton("Ver contenido")
        view_button.setObjectName("SecondaryButton")
        view_button.setMinimumWidth(180)
        view_button.clicked.connect(lambda: self._show_accounts_module(_ACCOUNTS_MODULE_VIEW_CONTENT))

        actions_row.addWidget(warmup_button)
        actions_row.addWidget(view_button)
        actions_row.addStretch(1)
        selector_layout.addLayout(actions_row)

        layout.addWidget(selector_panel)
        layout.addStretch(1)
        return page

    def _build_view_content_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        back_button = QPushButton("<- Volver a opciones")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(lambda: self._show_accounts_module(_ACCOUNTS_MODULE_SELECTOR))
        header_row.addWidget(back_button)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        view_panel, view_layout = self.create_panel(
            "Ver contenido",
            "Selecciona cuentas para ver reels y dar likes como calentamiento controlado.",
        )

        controls = QGridLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setHorizontalSpacing(10)
        controls.setVerticalSpacing(10)

        self._selected_view_label = QLabel("Sin cuentas seleccionadas.")
        self._selected_view_label.setObjectName("SectionPanelHint")
        self._selected_view_label.setWordWrap(True)

        select_accounts_button = QPushButton("Seleccionar alias y cuentas")
        select_accounts_button.setObjectName("SecondaryButton")
        select_accounts_button.clicked.connect(self._select_view_accounts)
        self._reels_minutes = QSpinBox()
        self._reels_minutes.setRange(1, 120)
        self._reels_minutes.setValue(10)
        self._likes_target = QSpinBox()
        self._likes_target.setRange(0, 200)
        self._likes_target.setValue(3)
        self._follows_target = QSpinBox()
        self._follows_target.setRange(0, 100)
        self._follows_target.setValue(0)
        self._view_start_button = QPushButton("Iniciar")
        self._view_start_button.setObjectName("PrimaryButton")
        self._view_start_button.clicked.connect(self._start_view_content)
        self._view_stop_button = QPushButton("Detener")
        self._view_stop_button.setObjectName("DangerButton")
        self._view_stop_button.setEnabled(False)
        self._view_stop_button.clicked.connect(self._stop_view_content)

        controls.addWidget(QLabel("Alias y cuentas"), 0, 0)
        controls.addWidget(select_accounts_button, 0, 1)
        controls.addWidget(QLabel("Tiempo viendo reels"), 1, 0)
        controls.addWidget(self._reels_minutes, 1, 1)
        controls.addWidget(QLabel("Cantidad de likes"), 2, 0)
        controls.addWidget(self._likes_target, 2, 1)
        controls.addWidget(QLabel("Cantidad de follows"), 3, 0)
        controls.addWidget(self._follows_target, 3, 1)
        controls.addWidget(self._view_start_button, 0, 2)
        controls.addWidget(self._view_stop_button, 0, 3)
        view_layout.addLayout(controls)
        view_layout.addWidget(self._selected_view_label)

        self._view_log = QPlainTextEdit()
        self._view_log.setObjectName("AccountsViewLog")
        self._view_log.setReadOnly(True)
        self._view_log.setMinimumHeight(220)
        view_layout.addWidget(self._view_log)

        layout.addWidget(view_panel)
        layout.addStretch(1)
        return page

    def _build_warmup_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar_row = QHBoxLayout()
        toolbar_row.setContentsMargins(0, 0, 0, 0)
        toolbar_row.setSpacing(8)
        back_button = QPushButton("<- Volver a opciones")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(lambda: self._show_accounts_module(_ACCOUNTS_MODULE_SELECTOR))
        toolbar_row.addWidget(back_button)
        change_flow_button = QPushButton("Cambiar flujo")
        change_flow_button.setObjectName("SecondaryButton")
        change_flow_button.clicked.connect(self._open_warmup_launcher)
        toolbar_row.addWidget(change_flow_button)
        toolbar_row.addStretch(1)
        self._warmup_add_stage_button = QPushButton("Nueva etapa")
        self._warmup_add_stage_button.setObjectName("PrimaryButton")
        self._warmup_add_stage_button.clicked.connect(self._add_warmup_stage)
        toolbar_row.addWidget(self._warmup_add_stage_button)
        layout.addLayout(toolbar_row)

        warmup_panel, warmup_layout = self.create_panel(
            "Warm Up",
            "Cada flujo queda asociado a un alias y a cuentas concretas. Desde aqui lo editas, lo ejecutas y revisas todo el log.",
        )

        self._warmup_flow_summary_label = QLabel("Todavia no seleccionaste un flujo Warm Up.")
        self._warmup_flow_summary_label.setObjectName("WarmupResumeLabel")
        self._warmup_flow_summary_label.setWordWrap(True)
        warmup_layout.addWidget(self._warmup_flow_summary_label)

        self._warmup_resume_label = QLabel("Usa el launcher para abrir un flujo existente o crear uno nuevo.")
        self._warmup_resume_label.setObjectName("WarmupResumeLabel")
        self._warmup_resume_label.setWordWrap(True)
        warmup_layout.addWidget(self._warmup_resume_label)

        self._warmup_stage_scroll = QScrollArea()
        self._warmup_stage_scroll.setObjectName("WarmupStageScroll")
        self._warmup_stage_scroll.setWidgetResizable(True)
        self._warmup_stage_scroll.setFrameShape(QFrame.NoFrame)
        self._warmup_stage_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._warmup_stage_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._warmup_stage_scroll.setMinimumHeight(210)

        self._warmup_stage_content = QWidget()
        self._warmup_stage_content.setObjectName("WarmupStageScrollContent")
        self._warmup_stage_layout = QHBoxLayout(self._warmup_stage_content)
        self._warmup_stage_layout.setContentsMargins(0, 6, 0, 6)
        self._warmup_stage_layout.setSpacing(10)
        self._warmup_stage_scroll.setWidget(self._warmup_stage_content)
        warmup_layout.addWidget(self._warmup_stage_scroll)

        controls_row = QHBoxLayout()
        controls_row.setContentsMargins(0, 0, 0, 0)
        controls_row.setSpacing(8)
        self._warmup_start_button = QPushButton("Comenzar")
        self._warmup_start_button.setObjectName("PrimaryButton")
        self._warmup_start_button.clicked.connect(self._start_warmup_flow)
        self._warmup_stop_button = QPushButton("Detener")
        self._warmup_stop_button.setObjectName("DangerButton")
        self._warmup_stop_button.clicked.connect(self._stop_warmup_flow)
        controls_row.addWidget(self._warmup_start_button)
        controls_row.addWidget(self._warmup_stop_button)
        controls_row.addStretch(1)
        warmup_layout.addLayout(controls_row)

        self._warmup_log = QPlainTextEdit()
        self._warmup_log.setObjectName("AccountsWarmupLog")
        self._warmup_log.setReadOnly(True)
        self._warmup_log.setMinimumHeight(220)
        warmup_layout.addWidget(self._warmup_log)

        hint = QLabel(
            "El log solo cambia cuando abres otro flujo o cuando eliminas el actual. Pausar o reanudar no lo limpia."
        )
        hint.setObjectName("SectionPanelHint")
        hint.setWordWrap(True)
        warmup_layout.addWidget(hint)

        layout.addWidget(warmup_panel)
        layout.addStretch(1)
        self._render_warmup_flow()
        return page

    def _show_accounts_module(self, module_name: str) -> None:
        clean_name = str(module_name or "").strip().lower()
        self._active_accounts_module = clean_name or _ACCOUNTS_MODULE_SELECTOR
        index_map = {
            _ACCOUNTS_MODULE_SELECTOR: 0,
            _ACCOUNTS_MODULE_VIEW_CONTENT: 1,
            _ACCOUNTS_MODULE_WARMUP: 2,
        }
        show_content_module = self._active_accounts_module == _ACCOUNTS_MODULE_CONTENT
        self._content_stack.setCurrentIndex(1 if show_content_module else 0)
        if show_content_module:
            self._content_controller.on_activated()
            return
        self._modules_stack.setCurrentIndex(index_map.get(self._active_accounts_module, 0))
        if self._active_accounts_module == _ACCOUNTS_MODULE_WARMUP:
            self._render_warmup_flow()

    def _open_content_module(self) -> None:
        self._content_controller.show_home()
        self._show_accounts_module(_ACCOUNTS_MODULE_CONTENT)

    def _open_warmup_launcher(self) -> None:
        if self._ctx.tasks.is_running(_WARMUP_FLOW_TASK):
            self.show_error("Deten el flujo Warm Up actual antes de cambiar o eliminar flujos.")
            return
        launcher = WarmupLauncherDialog(parent=self)
        if launcher.exec() != QDialog.Accepted:
            return
        if launcher.selection() == "existing":
            self._open_existing_warmup_flow_dialog()
            return
        if launcher.selection() == "create":
            self._open_new_warmup_flow_dialog()

    def _open_existing_warmup_flow_dialog(self) -> None:
        dialog = WarmupExistingFlowsDialog(parent=self)
        while True:
            flows = self._ctx.services.warmup.list_flows()
            if not flows:
                self.show_info("Todavia no hay flujos Warm Up creados.")
                return
            dialog.refresh_flows(flows)
            if dialog.exec() != QDialog.Accepted:
                return
            selected_flow_id = dialog.selected_flow_id()
            if selected_flow_id <= 0:
                self.show_error("Selecciona un flujo para continuar.")
                continue
            if dialog.action() == "delete":
                if (
                    selected_flow_id == self._warmup_flow_id
                    and self._ctx.tasks.is_running(_WARMUP_FLOW_TASK)
                ):
                    self.show_error("Deten el flujo antes de eliminarlo.")
                    continue
                confirmed = self._show_modal_message(
                    "Eliminar flujo",
                    "El flujo se eliminara por completo junto con sus etapas, cuentas y logs guardados.",
                    confirm_text="Eliminar flujo",
                    cancel_text="Cancelar",
                    danger=True,
                )
                if not confirmed:
                    continue
                if self._ctx.services.warmup.delete_flow(selected_flow_id):
                    if selected_flow_id == self._warmup_flow_id:
                        self._clear_warmup_flow()
                    continue
                self.show_error("No se pudo eliminar el flujo seleccionado.")
                continue
            self._load_warmup_flow(selected_flow_id)
            self._show_accounts_module(_ACCOUNTS_MODULE_WARMUP)
            return

    def _open_new_warmup_flow_dialog(self) -> None:
        aliases = self._available_aliases()
        if not aliases:
            self.show_error("No hay alias disponibles para crear un flujo.")
            return
        dialog = WarmupCreateFlowDialog(self._ctx, parent=self)
        dialog.refresh_aliases(aliases, self._current_alias())
        while True:
            if dialog.exec() != QDialog.Accepted:
                return
            usernames = dialog.usernames()
            if not usernames:
                self.show_error("Selecciona al menos una cuenta para el flujo.")
                continue
            try:
                flow = self._ctx.services.warmup.create_flow(
                    alias=dialog.alias(),
                    usernames=usernames,
                    name=dialog.flow_name(),
                )
            except Exception as exc:
                self.show_exception(exc, "No se pudo crear el flujo Warm Up.")
                return
            self._set_warmup_flow(flow)
            self._show_accounts_module(_ACCOUNTS_MODULE_WARMUP)
            return

    def _load_warmup_flow(self, flow_id: int | None = None) -> None:
        target_flow_id = int(flow_id or self._warmup_flow_id or 0)
        if target_flow_id <= 0:
            self._clear_warmup_flow(clear_log=False)
            return
        try:
            flow = self._ctx.services.warmup.get_flow(target_flow_id)
        except Exception as exc:
            self.show_exception(exc, "No se pudo cargar el flujo Warm Up.")
            return
        if not flow:
            self._clear_warmup_flow()
            return
        self._set_warmup_flow(flow)

    def _set_warmup_flow(self, flow: dict[str, Any], *, reset_log_cursor: bool = True) -> None:
        self._warmup_flow_id = int(flow.get("id") or 0)
        self._warmup_flow_alias = str(flow.get("alias") or "").strip()
        self._warmup_flow_name = str(flow.get("name") or "").strip()
        self._warmup_selected_usernames = [
            _normalize_username(item.get("username"))
            for item in flow.get("selected_accounts") or []
            if isinstance(item, dict) and _normalize_username(item.get("username"))
        ]
        self._warmup_stages = [dict(item) for item in flow.get("stages") or [] if isinstance(item, dict)]
        self._warmup_resume = dict(flow.get("resume") or {})
        self._warmup_has_started = bool(flow.get("has_started"))
        if reset_log_cursor:
            self._reload_warmup_logs()
        self._render_warmup_flow()

    def _clear_warmup_flow(self, *, clear_log: bool = True) -> None:
        self._warmup_flow_id = 0
        self._warmup_flow_alias = ""
        self._warmup_flow_name = ""
        self._warmup_selected_usernames = []
        self._warmup_stages = []
        self._warmup_resume = {}
        self._warmup_has_started = False
        self._warmup_log_id = 0
        if clear_log:
            self._warmup_log.clear()
        self._render_warmup_flow()

    def _render_warmup_flow(self) -> None:
        running = self._ctx.tasks.is_running(_WARMUP_FLOW_TASK)
        self._warmup_add_stage_button.setEnabled(bool(self._warmup_flow_id) and not running)
        start_text = "Reanudar" if self._warmup_has_started else "Comenzar"
        self._warmup_start_button.setText(start_text)
        can_start = bool(self._warmup_flow_id and self._warmup_selected_usernames and self._warmup_stages)
        busy = (
            self._ctx.tasks.is_running(_MANUAL_ACTION_TASK)
            or self._ctx.tasks.is_running(_VIEW_CONTENT_TASK)
            or running
        )
        self._warmup_start_button.setEnabled(can_start and not busy)
        self._warmup_stop_button.setEnabled(running)
        if not self._warmup_flow_id:
            self._warmup_flow_summary_label.setText("Todavia no seleccionaste un flujo Warm Up.")
            self._warmup_resume_label.setText("Usa el launcher para abrir un flujo existente o crear uno nuevo.")
            self._render_warmup_stages()
            return
        preview = ", ".join(f"@{item}" for item in self._warmup_selected_usernames[:3])
        extra = max(0, len(self._warmup_selected_usernames) - 3)
        if extra:
            preview += f" +{extra}"
        if not preview:
            preview = "Sin cuentas seleccionadas"
        self._warmup_flow_summary_label.setText(
            (
                f"Flujo: {self._warmup_flow_name or '-'}  |  "
                f"Alias: {self._warmup_flow_alias or '-'}  |  "
                f"Cuentas: {len(self._warmup_selected_usernames)}  |  "
                f"{preview}"
            )
        )
        self._render_warmup_stages()

    def _render_warmup_stages(self) -> None:
        while self._warmup_stage_layout.count():
            item = self._warmup_stage_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        if not self._warmup_flow_id:
            empty_state = QLabel("Abre o crea un flujo para empezar a configurarlo.")
            empty_state.setObjectName("SectionPanelHint")
            empty_state.setWordWrap(True)
            self._warmup_stage_layout.addWidget(empty_state)
            self._warmup_stage_layout.addStretch(1)
            return
        resume_stage_order = int(self._warmup_resume.get("current_stage_order") or 0)
        resume_action_order = int(self._warmup_resume.get("current_action_order") or 0)
        last_action_type = str(self._warmup_resume.get("last_action_type") or "").strip()
        last_account = str(self._warmup_resume.get("last_account") or "").strip()
        status = "running" if self._ctx.tasks.is_running(_WARMUP_FLOW_TASK) else (
            str(self._warmup_resume.get("status") or "paused").strip() or "paused"
        )
        if self._warmup_stages:
            for index, stage in enumerate(self._warmup_stages):
                card = WarmupStageCard(
                    stage,
                    resume_stage_order=resume_stage_order,
                    resume_action_order=resume_action_order,
                    on_open=self._open_warmup_stage_editor,
                )
                self._warmup_stage_layout.addWidget(card)
                if index < len(self._warmup_stages) - 1:
                    arrow = QLabel("->")
                    arrow.setObjectName("WarmupStageArrow")
                    arrow.setAlignment(Qt.AlignCenter)
                    self._warmup_stage_layout.addWidget(arrow)
        empty_state = QLabel("Todavia no hay etapas configuradas.")
        empty_state.setObjectName("SectionPanelHint")
        empty_state.setVisible(not bool(self._warmup_stages))
        if not self._warmup_stages:
            self._warmup_stage_layout.addWidget(empty_state)
        self._warmup_stage_layout.addStretch(1)
        resume_bits = [
            f"Flujo: {self._warmup_flow_name or '-'}",
            f"Alias: {self._warmup_flow_alias or '-'}",
            f"Estado: {status}",
            f"Ultima etapa: {resume_stage_order or 1}",
            f"Ultima accion: {resume_action_order or 1}",
        ]
        if last_action_type:
            resume_bits.append(f"Tipo: {_warmup_action_label(last_action_type)}")
        if last_account:
            resume_bits.append(f"Cuenta: @{last_account}")
        self._warmup_resume_label.setText(" | ".join(resume_bits))

    def _reload_warmup_logs(self) -> None:
        if not self._warmup_flow_id:
            self._warmup_log_id = 0
            self._warmup_log.clear()
            return
        try:
            rows = self._ctx.services.warmup.list_logs(self._warmup_flow_id)
        except Exception as exc:
            self.show_exception(exc, "No se pudo cargar el log del flujo Warm Up.")
            return
        self._warmup_log.clear()
        self._warmup_log_id = 0
        for row in rows:
            self._append_warmup_log_text(self._format_warmup_log_entry(row))
            self._warmup_log_id = int(row.get("id") or self._warmup_log_id)

    def _sync_warmup_logs(self) -> None:
        if not self._visible or self._warmup_flow_id <= 0:
            return
        try:
            next_log_id, rows = self._ctx.services.warmup.read_logs_after(self._warmup_flow_id, self._warmup_log_id)
        except Exception:
            return
        for row in rows:
            self._append_warmup_log_text(self._format_warmup_log_entry(row))
        self._warmup_log_id = next_log_id

    def _append_warmup_log_text(self, text: str) -> None:
        if not str(text or "").strip():
            return
        self._warmup_log.moveCursor(QTextCursor.End)
        self._warmup_log.insertPlainText(str(text))
        self._warmup_log.ensureCursorVisible()

    def _format_warmup_log_entry(self, row: dict[str, Any]) -> str:
        created_at = str(row.get("created_at") or "").replace("T", " ").replace("+00:00", "Z")
        message = str(row.get("message") or "").strip()
        return f"[{created_at}] {message}\n" if created_at else f"{message}\n"

    def _start_warmup_flow(self) -> None:
        if self._ctx.tasks.is_running(_MANUAL_ACTION_TASK) or self._ctx.tasks.is_running(_VIEW_CONTENT_TASK):
            self.show_error("Espera a que finalice la accion actual antes de iniciar el Warm Up.")
            return
        if self._ctx.tasks.is_running(_WARMUP_FLOW_TASK):
            return
        if not self._warmup_flow_id:
            self.show_error("Primero abre o crea un flujo Warm Up.")
            return
        if not self._warmup_selected_usernames:
            self.show_error("El flujo no tiene cuentas seleccionadas.")
            return
        if not self._warmup_stages:
            self.show_error("Agrega al menos una etapa antes de comenzar.")
            return
        action_label = "Reanudando" if self._warmup_has_started else "Comenzando"
        try:
            flow = self._ctx.services.warmup.mark_flow_running(
                self._warmup_flow_id,
                stage_order=int(self._warmup_resume.get("current_stage_order") or 1),
                action_order=int(self._warmup_resume.get("current_action_order") or 1),
                last_account=str(self._warmup_resume.get("last_account") or "").strip(),
            )
            self._ctx.services.warmup.append_log(
                self._warmup_flow_id,
                f"{action_label} flujo {self._warmup_flow_name or 'Warm Up'}.",
            )
            self._set_warmup_flow(flow, reset_log_cursor=False)
            self._ctx.tasks.start_task(
                _WARMUP_FLOW_TASK,
                lambda flow_id=self._warmup_flow_id: self._execute_warmup_flow_task(flow_id),
                metadata={"alias": self._warmup_flow_alias or str(flow.get("alias") or "").strip()},
            )
            self._warmup_has_started = True
            self._sync_warmup_logs()
            self._render_warmup_flow()
            self.set_status("Warm Up en ejecucion...")
        except Exception as exc:
            self.show_exception(exc, "No se pudo iniciar el flujo Warm Up.")

    def _stop_warmup_flow(self) -> None:
        if not self._ctx.tasks.is_running(_WARMUP_FLOW_TASK):
            return
        self._ctx.tasks.request_stop("Detener warm up desde Cuentas")
        self._warmup_stop_button.setEnabled(False)
        if self._warmup_flow_id:
            self._ctx.services.warmup.append_log(
                self._warmup_flow_id,
                "Solicitud de detencion enviada desde la interfaz.",
                level="warning",
            )
            self._sync_warmup_logs()
        self.set_status("Deteniendo Warm Up...")

    def _execute_warmup_flow_task(self, flow_id: int) -> list[dict[str, Any]]:
        flow = self._ctx.services.warmup.get_flow(flow_id)
        if not flow:
            raise RuntimeError("El flujo Warm Up ya no existe.")
        selected_accounts = self._warmup_selected_accounts_for_run(flow_id, flow)
        if not selected_accounts:
            self._ctx.services.warmup.pause_flow(flow_id, reason="sin cuentas disponibles")
            raise RuntimeError("No hay cuentas disponibles para ejecutar el flujo seleccionado.")
        scheduler = WarmupScheduler()
        cursor_by_account: dict[str, WarmupCursor] = {}
        for row in flow.get("account_states") or []:
            if not isinstance(row, dict):
                continue
            username = _normalize_username(row.get("username"))
            if not username:
                continue
            cursor_by_account[username] = WarmupCursor(
                stage_order=max(1, int(row.get("current_stage_order") or 1)),
                action_order=max(1, int(row.get("current_action_order") or 1)),
            )

        def _on_progress(event: dict[str, Any]) -> None:
            self._record_warmup_progress(flow_id, flow, scheduler, event)

        reset_stop_event()
        engine = WarmupEngine(
            progress_callback=_on_progress,
            stop_callback=lambda: STOP_EVENT.is_set(),
        )
        try:
            summaries = asyncio.run(
                engine.run_flow(
                    flow,
                    selected_accounts,
                    cursor_by_account=cursor_by_account,
                )
            )
        finally:
            reason = "detenido manualmente" if STOP_EVENT.is_set() else "ejecucion completada"
            self._ctx.services.warmup.pause_flow(flow_id, reason=reason)
        return summaries

    def _warmup_selected_accounts_for_run(self, flow_id: int, flow: dict[str, Any]) -> list[dict[str, Any]]:
        alias = str(flow.get("alias") or "").strip()
        records = self._ctx.services.accounts.list_accounts(alias)
        by_username = {
            _normalize_username(row.get("username")).lower(): dict(row)
            for row in records
            if _normalize_username(row.get("username"))
        }
        selected_accounts: list[dict[str, Any]] = []
        for username in flow.get("selected_usernames") or []:
            clean_username = _normalize_username(username)
            if not clean_username:
                continue
            account = by_username.get(clean_username.lower())
            if account is None:
                self._ctx.services.warmup.append_log(
                    flow_id,
                    f"Cuenta omitida porque ya no existe en el alias {alias}: @{clean_username}.",
                    level="warning",
                )
                continue
            account["username"] = clean_username
            selected_accounts.append(account)
        preflight = self._ctx.services.accounts.proxy_preflight_for_accounts(selected_accounts)
        for blocked in preflight.get("blocked_accounts") or []:
            if not isinstance(blocked, dict):
                continue
            username = _normalize_username(blocked.get("username"))
            reason = str(blocked.get("message") or blocked.get("status") or "proxy_blocked").strip()
            self._ctx.services.warmup.append_log(
                flow_id,
                f"Cuenta omitida por preflight de proxy @{username or '-'}: {reason}.",
                level="warning",
            )
        return [
            dict(account)
            for account in (preflight.get("ready_accounts") or [])
            if isinstance(account, dict)
        ]

    def _record_warmup_progress(
        self,
        flow_id: int,
        flow: dict[str, Any],
        scheduler: WarmupScheduler,
        event: dict[str, Any],
    ) -> None:
        username = _normalize_username(event.get("username"))
        if not username:
            return
        stage_order = max(1, int(event.get("stage_order") or 1))
        action_order = max(1, int(event.get("action_order") or 1))
        action_type = str(event.get("action_type") or "").strip().lower()
        next_cursor = scheduler.advance_cursor(
            flow,
            stage_order=stage_order,
            action_order=action_order,
        )
        self._ctx.services.warmup.record_account_state(
            flow_id,
            username,
            stage_order=next_cursor.stage_order,
            action_order=next_cursor.action_order,
            last_action_type=action_type,
            status="running",
            payload={
                "last_ok": bool(event.get("ok", True)),
                "performed": int(event.get("performed") or 0),
                "message": str(event.get("message") or "").strip(),
            },
        )
        self._ctx.services.warmup.append_log(
            flow_id,
            self._describe_warmup_progress(event),
            level="error" if not bool(event.get("ok", True)) else "info",
        )
        for detail in event.get("details") or []:
            clean_detail = str(detail or "").strip()
            if not clean_detail:
                continue
            self._ctx.services.warmup.append_log(flow_id, f"  - {clean_detail}")

    def _describe_warmup_progress(self, event: dict[str, Any]) -> str:
        username = _normalize_username(event.get("username")) or "cuenta"
        stage_title = str(event.get("stage_title") or "").strip() or f"Dia {int(event.get('stage_order') or 1)}"
        action_label = _warmup_action_label(event.get("action_type") or "")
        performed = int(event.get("performed") or 0)
        message = str(event.get("message") or "").strip()
        base = f"@{username} | {stage_title} | {action_label} | realizadas: {performed}"
        if message:
            base += f" | {message}"
        return base

    def _add_warmup_stage(self) -> None:
        if not self._warmup_flow_id:
            self.show_error("Primero abre o crea un flujo Warm Up.")
            return
        dialog = WarmupStageEditorDialog(
            {"title": f"Dia {len(self._warmup_stages) + 1}", "settings": {"base_delay_minutes": 20}},
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        if dialog.delete_requested():
            return
        try:
            flow = self._ctx.services.warmup.save_stage(
                self._warmup_flow_id,
                title=dialog.stage_title() or f"Dia {len(self._warmup_stages) + 1}",
                settings=dialog.settings(),
                actions=dialog.actions(),
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar la nueva etapa.")
            return
        self._set_warmup_flow(flow, reset_log_cursor=False)

    def _open_warmup_stage_editor(self, stage_id: int) -> None:
        if not self._warmup_flow_id:
            return
        current_stage = next((dict(item) for item in self._warmup_stages if int(item.get("id") or 0) == int(stage_id)), {})
        dialog = WarmupStageEditorDialog(current_stage, parent=self)
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            if dialog.delete_requested():
                flow = self._ctx.services.warmup.delete_stage(stage_id)
            else:
                flow = self._ctx.services.warmup.save_stage(
                    self._warmup_flow_id,
                    stage_id=dialog.stage_id(),
                    title=dialog.stage_title() or current_stage.get("title") or "Dia",
                    settings=dialog.settings(),
                    actions=dialog.actions(),
                )
        except Exception as exc:
            self.show_exception(exc, "No se pudo actualizar la etapa warm up.")
            return
        if not flow:
            self._clear_warmup_flow(clear_log=False)
            return
        self._set_warmup_flow(flow, reset_log_cursor=False)

    def _current_alias(self) -> str:
        return str(self._ctx.state.active_alias or "").strip()

    def _available_aliases(self) -> list[str]:
        aliases = [
            str(item or "").strip()
            for item in (self._snapshot_cache or {}).get("aliases") or []
            if str(item or "").strip()
        ]
        current_alias = self._current_alias()
        if current_alias and all(item.lower() != current_alias.lower() for item in aliases):
            aliases.append(current_alias)
        return aliases

    def _append_manual_log(self, text: str) -> None:
        if not str(text or "").strip():
            return
        self._manual_log.appendPlainText(str(text).rstrip())

    def _append_view_log(self, text: str) -> None:
        if not str(text or "").strip():
            return
        self._view_log.moveCursor(QTextCursor.End)
        self._view_log.insertPlainText(str(text))
        self._view_log.ensureCursorVisible()

    def _set_view_running(self, running: bool) -> None:
        self._view_start_button.setEnabled(not running)
        self._view_stop_button.setEnabled(running)

    def _replace_record_username(self, old_username: str, new_username: str) -> None:
        old_clean = _normalize_username(old_username)
        new_clean = _normalize_username(new_username)
        if not old_clean or not new_clean:
            return
        for row in self._records:
            if _normalize_username(row.get("username")).lower() == old_clean.lower():
                row["username"] = new_clean
        if isinstance(self._snapshot_cache, dict):
            for row in self._snapshot_cache.get("rows") or []:
                if not isinstance(row, dict):
                    continue
                if _normalize_username(row.get("username")).lower() == old_clean.lower():
                    row["username"] = new_clean
        self._selected_view_usernames = [
            new_clean if item.lower() == old_clean.lower() else item
            for item in self._selected_view_usernames
        ]
        self._update_view_selection_label()

    def _update_view_selection_label(self) -> None:
        if not self._selected_view_usernames:
            self._selected_view_label.setText("Primero selecciona alias y cuentas para ver contenido.")
            return
        preview = ", ".join(f"@{item}" for item in self._selected_view_usernames[:3])
        extra = max(0, len(self._selected_view_usernames) - 3)
        if extra:
            preview += f" +{extra}"
        self._selected_view_label.setText(
            f"Alias seleccionado: {self._selected_view_alias or self._current_alias() or '-'}  |  "
            f"Seleccionadas: {len(self._selected_view_usernames)}  |  {preview}"
        )

    def _normalize_selection(self, selection: Any) -> tuple[str, list[str]]:
        if isinstance(selection, dict):
            alias = str(selection.get("alias") or "").strip()
            usernames = [
                _normalize_username(item)
                for item in selection.get("usernames") or []
                if _normalize_username(item)
            ]
            return alias, usernames
        if isinstance(selection, (list, tuple)):
            usernames = [_normalize_username(item) for item in selection if _normalize_username(item)]
            return self._current_alias(), usernames
        return self._current_alias(), []

    def _open_account_selector(
        self,
        *,
        subtitle: str,
        require_manual_action_ready: bool = False,
    ) -> dict[str, Any]:
        aliases = self._available_aliases()
        if not aliases:
            self.show_error("No hay alias disponibles para seleccionar cuentas.")
            return {}
        dialog = AccountSelectionDialog(
            self._ctx,
            subtitle=subtitle,
            require_manual_action_ready=require_manual_action_ready,
            parent=self,
        )
        dialog.refresh_aliases(aliases, self._current_alias())
        if dialog.exec() != QDialog.Accepted:
            return {}
        usernames = dialog.usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta.")
            return {}
        return {
            "alias": dialog.alias(),
            "usernames": usernames,
        }

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        active_alias = str(payload.get("active_alias") or self._current_alias()).strip()
        if active_alias:
            self._set_active_alias(active_alias)
        self._records = [dict(item) for item in payload.get("rows") or [] if isinstance(item, dict)]
        aliases = [
            str(item or "").strip()
            for item in payload.get("aliases") or []
            if str(item or "").strip()
        ]
        summary = str(payload.get("summary") or "").strip()
        self._summary.setText(summary)
        self._content_controller.refresh_account_context(
            active_alias=active_alias,
            aliases=aliases,
            rows=self._records,
        )
        if not self._selected_view_alias:
            self._selected_view_alias = active_alias
        if self._selected_view_alias.lower() == active_alias.lower():
            valid_usernames = {
                _normalize_username(row.get("username")).lower()
                for row in self._records
                if _normalize_username(row.get("username"))
            }
            self._selected_view_usernames = [
                item for item in self._selected_view_usernames if item.lower() in valid_usernames
            ]
        self._update_view_selection_label()
        if self._warmup_flow_id:
            self._load_warmup_flow(self._warmup_flow_id)
        self.clear_status()

    def refresh_page(self) -> None:
        alias = self._current_alias()
        if alias:
            self._set_active_alias(alias)
        if self._snapshot_loading:
            self._refresh_after_current = True
            return
        self._requested_alias = str(self._ctx.state.active_alias or alias).strip()
        self._snapshot_loading = True
        cached_payload = (
            self._snapshot_cache
            if isinstance(self._snapshot_cache, dict)
            and str(self._snapshot_cache.get("active_alias") or "").strip().lower()
            == self._requested_alias.lower()
            else None
        )
        if cached_payload is None:
            self.set_status("Cargando acciones de cuentas...")
        else:
            self._apply_snapshot(cached_payload)
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda alias=self._requested_alias: build_accounts_actions_snapshot(
                self._ctx.services,
                active_alias=alias,
            ),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._snapshot_cache = dict(payload) if isinstance(payload, dict) else {}
        payload_alias = str(self._snapshot_cache.get("active_alias") or "").strip()
        current_alias = self._current_alias()
        needs_follow_up = self._refresh_after_current
        self._refresh_after_current = False
        if (current_alias and payload_alias and current_alias.lower() != payload_alias.lower()) or needs_follow_up:
            self.refresh_page()
            return
        self._apply_snapshot(self._snapshot_cache)

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudieron cargar las acciones de cuentas: {error.message}")

    def _clear_manual_sequence_state(self) -> None:
        self._manual_action_kind = ""
        self._manual_alias = ""
        self._manual_queue = []
        self._manual_current_username = ""

    def _cancel_manual_sequence(self, *, close_browser: bool) -> None:
        has_pending_manual = bool(self._manual_current_username or self._manual_queue or self._manual_action_kind)
        self._manual_abort_requested = has_pending_manual
        self._manual_queue = []
        current_username = self._manual_current_username
        if close_browser and current_username:
            with contextlib.suppress(Exception):
                self._ctx.services.accounts.close_manual_session(current_username)

    def _start_manual_sequence(self, kind: str) -> None:
        if self._ctx.tasks.is_running(_MANUAL_ACTION_TASK) or self._ctx.tasks.is_running(_VIEW_CONTENT_TASK):
            self.show_error("Espera a que finalice la accion actual antes de iniciar otra.")
            return
        subtitles = {
            "username": "Selecciona las cuentas que van a cambiar username.",
            "full_name": "Selecciona las cuentas que van a cambiar full name.",
            "other": "Selecciona las cuentas que vas a abrir para otros cambios manuales.",
        }
        alias, usernames = self._normalize_selection(
            self._open_account_selector(
                subtitle=subtitles.get(kind, "Selecciona las cuentas."),
                require_manual_action_ready=True,
            )
        )
        if not usernames:
            return
        self._manual_abort_requested = False
        self._manual_action_kind = kind
        self._manual_alias = alias or self._current_alias()
        self._manual_queue = list(usernames)
        self._manual_current_username = ""
        self._append_manual_log(
            f"Inicio de accion manual para {len(usernames)} cuenta(s) del alias {self._manual_alias}."
        )
        self._start_next_manual_account()

    def _start_next_manual_account(self) -> None:
        if self._manual_abort_requested:
            current_username = self._manual_current_username
            self._clear_manual_sequence_state()
            self._manual_abort_requested = False
            if current_username:
                self._append_manual_log(f"Sesion manual cancelada para @{current_username}.")
            return
        if not self._manual_queue:
            self._clear_manual_sequence_state()
            self.set_status("Accion manual finalizada.")
            self._append_manual_log("Secuencia manual completada.")
            self.refresh_page()
            return
        if self._ctx.tasks.is_running(_MANUAL_ACTION_TASK):
            return
        current_username = self._manual_queue[0]
        self._manual_current_username = current_username
        self._ctx.services.accounts.clear_manual_session_close_request(current_username)
        action_label = {
            "username": "Cambiar username",
            "full_name": "Cambiar full name",
            "other": "Otros cambios",
        }.get(self._manual_action_kind, "Abrir cuenta")
        self._append_manual_log(f"Abriendo @{current_username} para {action_label.lower()}.")
        try:
            if self._manual_action_kind == "other":
                self._ctx.tasks.start_task(
                    _MANUAL_ACTION_TASK,
                    lambda alias=self._manual_alias, username=current_username, label=action_label: self._ctx.services.accounts.open_profile_sessions(
                        alias,
                        [username],
                        action_label=label,
                    ),
                    metadata={"alias": self._manual_alias},
                )
            else:
                self._ctx.tasks.start_task(
                    _MANUAL_ACTION_TASK,
                    lambda alias=self._manual_alias, username=current_username, label=action_label: self._ctx.services.accounts.open_manual_sessions(
                        alias,
                        [username],
                        start_url=_IG_EDIT_PROFILE_URL,
                        action_label=label,
                    ),
                    metadata={"alias": self._manual_alias},
                )
            self.set_status(f"Procesando @{current_username}...")
        except Exception as exc:
            self.show_exception(exc, "No se pudo abrir la cuenta seleccionada.")
            self._manual_queue.pop(0)
            QTimer.singleShot(0, self._start_next_manual_account)

    def _finish_manual_account(self, *, ok: bool, message: str) -> None:
        current_username = self._manual_current_username
        if not current_username:
            return
        if self._manual_abort_requested:
            self._clear_manual_sequence_state()
            self._manual_abort_requested = False
            self._append_manual_log(f"Sesion manual cancelada para @{current_username}.")
            return
        if ok:
            self._append_manual_log(f"Sesion manual finalizada para @{current_username}.")
            if self._manual_action_kind == "username":
                new_username, accepted = self.prompt_text(
                    title="Nuevo username",
                    subtitle=(
                        f"El navegador de @{current_username} se cerro. "
                        "Ingresa el nuevo username para actualizar el registro interno."
                    ),
                    label="Nuevo username",
                    placeholder="@nuevo_username",
                    text=current_username,
                    confirm_text="Guardar username",
                )
                desired = _normalize_username(new_username)
                if accepted and desired and desired.lower() != current_username.lower():
                    try:
                        updated = self._ctx.services.accounts.rename_account_username(current_username, desired)
                        self._append_manual_log(f"Username actualizado: @{current_username} -> @{updated}.")
                        self._replace_record_username(current_username, updated)
                    except Exception as exc:
                        self.show_exception(exc, "No se pudo actualizar el nuevo username en el registro.")
                else:
                    self._append_manual_log(f"Sin cambios de username para @{current_username}.")
        else:
            self._append_manual_log(
                f"No se pudo completar la accion para @{current_username}: {message or 'sin detalle'}."
            )
        if self._manual_queue and self._manual_queue[0].lower() == current_username.lower():
            self._manual_queue.pop(0)
        self._manual_current_username = ""
        QTimer.singleShot(0, self._start_next_manual_account)

    def _select_view_accounts(self) -> None:
        alias, usernames = self._normalize_selection(
            self._open_account_selector(
                subtitle="Selecciona el alias y las cuentas que vas a usar para ver reels y dar likes."
            )
        )
        if not usernames:
            return
        self._selected_view_alias = alias or self._current_alias()
        self._selected_view_usernames = usernames
        self._update_view_selection_label()

    def _start_view_content(self) -> None:
        if self._ctx.tasks.is_running(_MANUAL_ACTION_TASK) or self._ctx.tasks.is_running(_VIEW_CONTENT_TASK):
            self.show_error("Espera a que finalice la accion actual antes de iniciar otra.")
            return
        if not self._selected_view_usernames:
            self._select_view_accounts()
            if not self._selected_view_usernames:
                return
        self._view_log.clear()
        self._view_log_cursor = self._ctx.logs.cursor()
        self._set_view_running(True)
        alias = self._selected_view_alias or self._current_alias()
        try:
            self._ctx.tasks.start_task(
                _VIEW_CONTENT_TASK,
                lambda selected=list(self._selected_view_usernames), clean_alias=alias: self._ctx.services.accounts.run_reels_playwright(
                    clean_alias,
                    selected,
                    minutes=self._reels_minutes.value(),
                    likes_target=self._likes_target.value(),
                    follows_target=self._follows_target.value(),
                ),
                metadata={"alias": alias},
            )
            self._append_view_log(
                f"Iniciando ver contenido con {len(self._selected_view_usernames)} cuenta(s) del alias {alias}...\n"
            )
            self.set_status("Ver contenido en ejecucion...")
        except Exception as exc:
            self._set_view_running(False)
            self.show_exception(exc, "No se pudo iniciar la visualizacion de contenido.")

    def _stop_view_content(self) -> None:
        if not self._ctx.tasks.is_running(_VIEW_CONTENT_TASK):
            return
        self._ctx.tasks.request_stop("Detener ver contenido desde Cuentas")
        self._view_stop_button.setEnabled(False)
        self._append_view_log("Solicitud de detencion enviada.\n")
        self.set_status("Deteniendo ver contenido...")

    def _on_global_log_added(self, _chunk: str) -> None:
        if self._view_log_cursor is None:
            return
        if self._view_log_sync_pending:
            return
        self._view_log_sync_pending = True
        QTimer.singleShot(0, self._sync_view_logs)

    def _sync_view_logs(self) -> None:
        self._view_log_sync_pending = False
        cursor = self._view_log_cursor
        if cursor is None:
            return
        next_cursor, delta, _reset = self._ctx.logs.read_since(cursor)
        self._view_log_cursor = next_cursor
        if delta:
            self._append_view_log(delta)

    def _on_task_completed(self, task_name: str, ok: bool, message: str, result: object) -> None:
        if task_name == _MANUAL_ACTION_TASK:
            self._finish_manual_account(ok=ok, message=message)
            return
        if task_name == _WARMUP_FLOW_TASK:
            if self._warmup_flow_id:
                summaries = [dict(item) for item in result or [] if isinstance(item, dict)]
                completed_accounts = sum(1 for item in summaries if dict(item).get("results"))
                self._ctx.services.warmup.append_log(
                    self._warmup_flow_id,
                    f"Resumen final del flujo: {completed_accounts} cuenta(s) procesadas.",
                    level="error" if not ok else "info",
                )
            return
        if task_name != _VIEW_CONTENT_TASK:
            return
        payloads = [dict(item) for item in result or [] if isinstance(item, dict)]
        for row in payloads:
            username = _normalize_username(row.get("username")) or "cuenta"
            viewed = int(row.get("viewed") or 0)
            liked = int(row.get("liked") or 0)
            followed = int(row.get("followed") or 0)
            errors = int(row.get("errors") or 0)
            self._append_view_log(
                (
                    f"Resumen @{username}: reels vistos={viewed}, likes={liked}, "
                    f"follows={followed}, errores={errors}\n"
                )
            )
            for detail in row.get("messages") or []:
                self._append_view_log(f"  - {detail}\n")

    def _on_task_finished(self, task_name: str, ok: bool, message: str) -> None:
        if task_name == _VIEW_CONTENT_TASK:
            self._sync_view_logs()
            self._set_view_running(False)
            if ok:
                self.set_status("Ver contenido finalizado.")
                self._append_view_log("Proceso de ver contenido finalizado.\n")
            else:
                self.set_status(message or "Ver contenido finalizado con errores.")
                self._append_view_log(f"Proceso detenido con error: {message or 'sin detalle'}\n")
            self._view_log_cursor = None
            return
        if task_name != _WARMUP_FLOW_TASK:
            return
        self._sync_warmup_logs()
        self._load_warmup_flow(self._warmup_flow_id)
        if ok:
            self.set_status("Warm Up finalizado.")
        else:
            self.set_status(message or "Warm Up finalizado con errores.")

    def on_navigate_to(self, payload: Any = None) -> None:
        self._visible = True
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_page()

    def on_navigate_from(self) -> None:
        self._visible = False
        self._cancel_manual_sequence(close_browser=True)


class ProxiesPage(AccountsSectionPage):
    def __init__(self, ctx: PageContext, parent: QWidget | None = None) -> None:
        super().__init__(
            ctx,
            "Proxies",
            "Panel exclusivo para proxies y asignacion puntual por cuenta.",
            route_key="proxies_page",
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Proxies",
            "La tabla principal solo muestra proxies. La asignacion se resuelve en un modal separado por cuenta.",
        )

        toolbar = QFrame()
        toolbar.setObjectName("SectionToolbarCard")
        toolbar_layout = QGridLayout(toolbar)
        toolbar_layout.setContentsMargins(16, 16, 16, 16)
        toolbar_layout.setHorizontalSpacing(8)
        toolbar_layout.setVerticalSpacing(8)
        add_button = QPushButton("Agregar proxy")
        add_button.setObjectName("PrimaryButton")
        add_button.clicked.connect(self._open_add_proxy_dialog)
        import_button = QPushButton("Importar proxies CSV")
        import_button.setObjectName("SecondaryButton")
        import_button.clicked.connect(self._import_csv)
        delete_button = QPushButton("Eliminar proxy")
        delete_button.setObjectName("DangerButton")
        delete_button.clicked.connect(self._delete_selected)
        assign_button = QPushButton("Asignar proxy a cuentas")
        assign_button.setObjectName("SecondaryButton")
        assign_button.clicked.connect(self._open_assign_proxy_dialog)
        activate_button = QPushButton("Activar")
        activate_button.setObjectName("SecondaryButton")
        activate_button.clicked.connect(lambda: self._toggle_selected(True))
        deactivate_button = QPushButton("Desactivar")
        deactivate_button.setObjectName("SecondaryButton")
        deactivate_button.clicked.connect(lambda: self._toggle_selected(False))
        test_button = QPushButton("Test proxy")
        test_button.setObjectName("PrimaryButton")
        test_button.clicked.connect(self._test_selected)
        sweep_button = QPushButton("Barrido salud")
        sweep_button.setObjectName("SecondaryButton")
        sweep_button.clicked.connect(self._sweep_health)
        refresh_button = QPushButton("Refrescar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_page)

        toolbar_layout.addWidget(add_button, 0, 0)
        toolbar_layout.addWidget(import_button, 0, 1)
        toolbar_layout.addWidget(delete_button, 0, 2)
        toolbar_layout.addWidget(assign_button, 0, 3)
        toolbar_layout.addWidget(activate_button, 1, 1)
        toolbar_layout.addWidget(deactivate_button, 1, 2)
        toolbar_layout.addWidget(test_button, 1, 3)
        toolbar_layout.addWidget(sweep_button, 1, 4)
        toolbar_layout.addWidget(refresh_button, 1, 5)
        layout.addWidget(toolbar)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["Proxy", "Endpoint", "Salida", "Estado", "Salud", "Cuentas asignadas"]
        )
        _configure_table(self._table, "AccountsProxyTable", selection_mode=QAbstractItemView.SingleSelection)
        self._table.setMinimumHeight(340)
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        layout.addWidget(self._table)

        helper = QLabel(
            "Selecciona un proxy y usa el boton de asignacion para elegir las cuentas que deben utilizarlo."
        )
        helper.setObjectName("SectionPanelHint")
        helper.setWordWrap(True)
        layout.addWidget(helper)
        self._integrity_label = QLabel("")
        self._integrity_label.setObjectName("SectionPanelHint")
        self._integrity_label.setWordWrap(True)
        layout.addWidget(self._integrity_label)

        self.content_layout().addWidget(panel)
        self.content_layout().addStretch(1)

        self._ctx.tasks.taskFinished.connect(self._on_task_finished)
        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)

    def _on_task_finished(self, task_name: str, ok: bool, message: str) -> None:
        if task_name not in {"proxy_test", "proxy_health_sweep"}:
            return
        if ok and task_name == "proxy_test":
            self.set_status("Test de proxy finalizado.")
        elif ok and task_name == "proxy_health_sweep":
            self.set_status("Barrido de salud finalizado.")
        elif message:
            self.set_status(message)

    def _on_task_completed(self, task_name: str, ok: bool, message: str, result: object) -> None:
        if task_name not in {"proxy_test", "proxy_health_sweep"}:
            return
        self.refresh_page()
        if not ok:
            return
        payload = dict(result or {}) if isinstance(result, dict) else {}
        if task_name == "proxy_health_sweep":
            checked = int(payload.get("checked") or 0)
            failed = int(payload.get("failed") or 0)
            succeeded = int(payload.get("succeeded") or 0)
            self.set_status(
                f"Barrido de salud listo | chequeados {checked} | OK {succeeded} | errores {failed}"
            )
            return
        proxy_id = str(payload.get("proxy_id") or "").strip() or "proxy"
        public_ip = str(payload.get("public_ip") or "").strip() or "-"
        health_label = str(payload.get("health_label") or "").strip() or "OK"
        self.set_status(f"Proxy {proxy_id} OK | salida {public_ip} | {health_label}")

    def _selected_proxy_id(self) -> str:
        row = self._table.currentRow()
        if row < 0:
            return ""
        item = self._table.item(row, 0)
        return str(item.text() if item else "").strip()

    def _proxy_usage_map(self) -> dict[str, dict[str, Any]]:
        usage_map: dict[str, dict[str, Any]] = {}
        for account in self._ctx.services.accounts.list_accounts(None):
            proxy_id = str(account.get("assigned_proxy_id") or "").strip()
            if not proxy_id:
                continue
            alias = str(account.get("alias") or "default").strip() or "default"
            bucket = usage_map.setdefault(proxy_id.lower(), {"aliases": set(), "count": 0})
            bucket["aliases"].add(alias)
            bucket["count"] = int(bucket.get("count") or 0) + 1
        return usage_map

    def refresh_page(self) -> None:
        proxies = self._ctx.services.accounts.list_proxy_records()
        usage_map = self._proxy_usage_map()
        integrity = self._ctx.services.accounts.proxy_integrity_summary()
        self._table.setRowCount(len(proxies))
        for row, proxy in enumerate(proxies):
            proxy_id = str(proxy.get("id") or "").strip()
            usage = dict(usage_map.get(proxy_id.lower(), {}))
            exit_ip = str(proxy.get("last_public_ip") or "").strip() or "-"
            quarantine_until = float(proxy.get("quarantine_until") or 0.0)
            status_label = "Activo" if bool(proxy.get("active", True)) else "Inactivo"
            if bool(proxy.get("active", True)) and quarantine_until > 0.0:
                status_label = "Cuarentena"
            values = [
                proxy_id,
                _proxy_host_label(str(proxy.get("server") or "").strip()),
                exit_ip,
                status_label,
                self._ctx.services.accounts.proxy_health_label(proxy),
                _proxy_usage_label(
                    usage.get("aliases") if isinstance(usage.get("aliases"), set) else set(),
                    int(usage.get("count") or 0),
                ),
            ]
            for column, value in enumerate(values):
                self._table.setItem(row, column, table_item(value))
        self._integrity_label.setText(
            "Integridad: "
            f"{int(integrity.get('active') or 0)}/{int(integrity.get('total') or 0)} activos  |  "
            f"En cuarentena: {int(integrity.get('quarantined') or 0)}  |  "
            f"Asignadas: {int(integrity.get('assigned_accounts') or 0)}  |  "
            f"Invalidas: {int(integrity.get('invalid_assignments') or 0)}"
        )
        self.clear_status()

    def _open_add_proxy_dialog(self) -> None:
        dialog = ProxyEditorDialog(self)
        if dialog.exec() != QDialog.Accepted:
            return
        payload = dialog.payload()
        if not str(payload.get("id") or "").strip() or not str(payload.get("server") or "").strip():
            self.show_error("Proxy y servidor son obligatorios.")
            return
        try:
            proxy = self._ctx.services.accounts.upsert_proxy(payload)
            self.refresh_page()
            self.set_status(f"Proxy guardado: {proxy.get('id')}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar el proxy. Ver logs para mas detalles.")

    def _import_csv(self) -> None:
        path = _open_dark_file_dialog(self, "Selecciona CSV de proxies", "CSV (*.csv)")
        if not path:
            return
        try:
            result = self._ctx.services.accounts.import_proxies_csv(path)
            self.refresh_page()
            self.set_status(f"Proxies importados: {result.get('imported', 0)}")
        except Exception as exc:
            self.show_exception(exc, "No se pudieron importar los proxies. Ver logs para mas detalles.")

    def _delete_selected(self) -> None:
        proxy_id = self._selected_proxy_id()
        if not proxy_id:
            self.show_error("Selecciona un proxy.")
            return
        try:
            deleted = self._ctx.services.accounts.delete_proxy(proxy_id)
            self.refresh_page()
            self.set_status(f"Proxies eliminados: {deleted}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar el proxy. Ver logs para mas detalles.")

    def _toggle_selected(self, active: bool) -> None:
        proxy_id = self._selected_proxy_id()
        if not proxy_id:
            self.show_error("Selecciona un proxy.")
            return
        try:
            self._ctx.services.accounts.toggle_proxy_active(proxy_id, active=active)
            self.refresh_page()
            self.set_status(f"Proxy {'activado' if active else 'desactivado'}: {proxy_id}")
        except Exception as exc:
            self.show_exception(exc, "No se pudo actualizar el proxy. Ver logs para mas detalles.")

    def _test_selected(self) -> None:
        proxy_id = self._selected_proxy_id()
        if not proxy_id:
            self.show_error("Selecciona un proxy.")
            return
        self._ctx.tasks.start_task(
            "proxy_test",
            lambda: self._ctx.services.accounts.test_proxy(proxy_id),
        )
        self.set_status(f"Probando proxy {proxy_id}...")

    def _sweep_health(self) -> None:
        self._ctx.tasks.start_task(
            "proxy_health_sweep",
            lambda: self._ctx.services.accounts.sweep_proxy_health(
                only_assigned=True,
                active_only=True,
                source="manual_ui",
            ),
        )
        self.set_status("Ejecutando barrido de salud de proxies...")

    def _open_assign_proxy_dialog(self) -> None:
        proxy_id = self._selected_proxy_id()
        if not proxy_id:
            self.show_error("Selecciona un proxy.")
            return
        dialog = ProxyAssignmentDialog(self._ctx, self)
        aliases = self._ctx.services.accounts.list_aliases()
        dialog.refresh_aliases(aliases, self._ctx.state.active_alias)
        if dialog.exec() != QDialog.Accepted:
            return
        usernames = dialog.usernames()
        if not usernames:
            self.show_error("Selecciona al menos una cuenta.")
            return
        try:
            assigned = self._ctx.services.accounts.assign_proxy(usernames, proxy_id)
            self._set_active_alias(dialog.alias() or self._ctx.state.active_alias)
            self.refresh_page()
            self.set_status(f"Proxy asignado a {assigned} cuentas.")
        except Exception as exc:
            self.show_exception(exc, "No se pudo asignar el proxy. Ver logs para mas detalles.")

    def on_navigate_to(self, payload: Any = None) -> None:
        self.refresh_page()
