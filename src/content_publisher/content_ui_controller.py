from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from gui.page_base import PageContext

from .content_extract_service import ContentExtractService
from .content_library_service import ContentLibraryService, ContentPublisherError
from .content_publish_service import ContentPublishService


_CONTENT_EXTRACT_TASK = "content_extract"
_CONTENT_PUBLISH_TASK = "content_publish"
_BLOCKING_TASKS = {
    "accounts_manual_action",
    "accounts_view_content",
    "accounts_warmup_flow",
    _CONTENT_EXTRACT_TASK,
    _CONTENT_PUBLISH_TASK,
}


def _ctx_root_dir(ctx: PageContext) -> Path | None:
    context = getattr(ctx.services, "context", None)
    root_dir = getattr(context, "root_dir", None)
    if root_dir is None:
        return None
    return Path(root_dir)


def _checkable_account_item(username: str, detail: str) -> QListWidgetItem:
    item = QListWidgetItem(detail)
    item.setData(Qt.UserRole, username)
    item.setFlags((item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled) & ~Qt.ItemIsSelectable)
    item.setCheckState(Qt.Unchecked)
    return item


def _create_panel(
    panel_title: str,
    panel_hint: str,
    *,
    margins: tuple[int, int, int, int] = (20, 20, 20, 20),
    spacing: int = 14,
) -> tuple[QFrame, QVBoxLayout]:
    card = QFrame()
    card.setObjectName("SectionPanelCard")
    layout = QVBoxLayout(card)
    layout.setContentsMargins(*margins)
    layout.setSpacing(spacing)

    title = QLabel(panel_title)
    title.setObjectName("SectionPanelTitle")
    hint = QLabel(panel_hint)
    hint.setObjectName("SectionPanelHint")
    hint.setWordWrap(True)

    layout.addWidget(title)
    layout.addWidget(hint)
    return card, layout


def _open_dark_save_dialog(parent: QWidget, title: str, suggested_name: str, file_filter: str) -> str:
    dialog = QFileDialog(parent, title, str(Path.cwd()), file_filter)
    dialog.setAcceptMode(QFileDialog.AcceptSave)
    dialog.setOption(QFileDialog.DontUseNativeDialog, True)
    dialog.selectFile(suggested_name)
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
    files = dialog.selectedFiles()
    return str(files[0] if files else "").strip()


class EmbeddedContentApiClient:
    def __init__(
        self,
        *,
        root_dir: str | Path | None = None,
        library_service: ContentLibraryService | None = None,
    ) -> None:
        self._root_dir = Path(root_dir) if root_dir else None
        self._library_service = library_service or ContentLibraryService(root_dir=self._root_dir)
        self._extract_service = ContentExtractService(
            root_dir=self._root_dir,
            library_service=self._library_service,
        )
        self._publish_service = ContentPublishService(
            root_dir=self._root_dir,
            library_service=self._library_service,
        )

    def extract(
        self,
        *,
        alias: str,
        account_ids: list[str],
        profile_urls: list[str],
        posts_per_profile: int,
    ) -> dict[str, Any]:
        return self._extract_service.extract(
            alias=alias,
            account_ids=list(account_ids),
            profile_urls=list(profile_urls),
            posts_per_profile=int(posts_per_profile),
        )

    def publish(
        self,
        *,
        account_id: str,
        media_path: str,
        caption: str,
    ) -> dict[str, Any]:
        return self._publish_service.publish(
            account_id=account_id,
            media_path=media_path,
            caption=caption,
        )


class ContentLibraryViewerDialog(QDialog):
    def __init__(
        self,
        *,
        entries: list[dict[str, Any]],
        library_service: ContentLibraryService,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._entries = [dict(item) for item in entries if isinstance(item, dict)]
        self._library_service = library_service
        self._checkboxes: dict[int, QCheckBox] = {}
        self.setWindowTitle("Contenido extraído")
        self.setMinimumSize(980, 700)
        self.setStyleSheet(
            """
            QDialog {
                background-color: #111827;
                color: #e8edf7;
            }
            QFrame#ContentLibraryCard {
                background-color: #0f1728;
                border: 1px solid #253248;
                border-radius: 12px;
            }
            QLabel#ContentLibraryMeta {
                color: #8fa1bc;
            }
            QLabel#ContentLibraryCaption {
                color: #edf2fb;
            }
            QLabel#ContentLibraryPreview {
                background-color: #0b1220;
                border: 1px solid #223147;
                border-radius: 10px;
                padding: 4px;
            }
            """
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        header = QLabel("Selecciona publicaciones y expórtalas en JSON, CSV o ZIP.")
        header.setObjectName("SectionPanelHint")
        header.setWordWrap(True)
        layout.addWidget(header)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(8)
        select_all_button = QPushButton("Seleccionar todo")
        select_all_button.setObjectName("SecondaryButton")
        select_all_button.clicked.connect(self._select_all)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_selection)
        export_json_button = QPushButton("Exportar JSON")
        export_json_button.setObjectName("PrimaryButton")
        export_json_button.clicked.connect(lambda: self._export("json"))
        export_csv_button = QPushButton("Exportar CSV")
        export_csv_button.setObjectName("SecondaryButton")
        export_csv_button.clicked.connect(lambda: self._export("csv"))
        export_zip_button = QPushButton("Exportar ZIP")
        export_zip_button.setObjectName("SecondaryButton")
        export_zip_button.clicked.connect(lambda: self._export("zip"))
        close_button = QPushButton("Cerrar")
        close_button.setObjectName("SecondaryButton")
        close_button.clicked.connect(self.accept)
        for button in (
            select_all_button,
            clear_button,
            export_json_button,
            export_csv_button,
            export_zip_button,
            close_button,
        ):
            actions_row.addWidget(button)
        actions_row.addStretch(1)
        layout.addLayout(actions_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("QScrollArea { background-color: transparent; border: none; }")
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(12)

        if not self._entries:
            empty = QLabel("Aún no hay contenido disponible en la biblioteca.")
            empty.setObjectName("SectionPanelHint")
            content_layout.addWidget(empty)
        for entry in self._entries:
            content_layout.addWidget(self._build_entry_card(entry))
        content_layout.addStretch(1)
        scroll.setWidget(content)
        layout.addWidget(scroll, 1)

    def _build_preview_label(self, path: str, *, max_size: QSize) -> QLabel:
        label = QLabel()
        label.setObjectName("ContentLibraryPreview")
        label.setAlignment(Qt.AlignCenter)
        label.setMinimumSize(max_size)
        pixmap = QPixmap(str(path))
        if pixmap.isNull():
            label.setText(Path(path).name)
            return label
        scaled = pixmap.scaled(max_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        label.setPixmap(scaled)
        return label

    def _build_entry_card(self, entry: dict[str, Any]) -> QWidget:
        card = QFrame()
        card.setObjectName("ContentLibraryCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)
        checkbox = QCheckBox("Seleccionar")
        entry_id = int(entry.get("id") or 0)
        self._checkboxes[entry_id] = checkbox
        meta = QLabel(
            f"@{entry.get('source_profile') or '-'}  |  "
            f"{entry.get('media_type') or '-'}  |  "
            f"{entry.get('created_at_label') or '-'}"
        )
        meta.setObjectName("ContentLibraryMeta")
        meta.setWordWrap(True)
        top_row.addWidget(checkbox)
        top_row.addWidget(meta, 1)
        layout.addLayout(top_row)

        media_row = QHBoxLayout()
        media_row.setContentsMargins(0, 0, 0, 0)
        media_row.setSpacing(10)
        preview_size = QSize(150, 150) if str(entry.get("media_type") or "") == "carousel" else QSize(240, 240)
        for media_file in entry.get("media_files") or []:
            media_row.addWidget(self._build_preview_label(str(media_file), max_size=preview_size))
        if media_row.count():
            media_row.addStretch(1)
            layout.addLayout(media_row)

        caption = QLabel(str(entry.get("caption") or "(sin caption)"))
        caption.setObjectName("ContentLibraryCaption")
        caption.setWordWrap(True)
        layout.addWidget(caption)
        return card

    def _selected_ids(self) -> list[int]:
        selected: list[int] = []
        for entry_id, checkbox in self._checkboxes.items():
            if checkbox.isChecked():
                selected.append(int(entry_id))
        return selected

    def _select_all(self) -> None:
        for checkbox in self._checkboxes.values():
            checkbox.setChecked(True)

    def _clear_selection(self) -> None:
        for checkbox in self._checkboxes.values():
            checkbox.setChecked(False)

    def _export(self, kind: str) -> None:
        selected_ids = self._selected_ids()
        if not selected_ids:
            QMessageBox.warning(self, "Exportar", "Selecciona al menos un contenido para exportar.")
            return
        file_names = {
            "json": "content_library_export.json",
            "csv": "content_library_export.csv",
            "zip": "content_library_export.zip",
        }
        filters = {
            "json": "JSON Files (*.json)",
            "csv": "CSV Files (*.csv)",
            "zip": "ZIP Files (*.zip)",
        }
        destination = _open_dark_save_dialog(
            self,
            f"Exportar {kind.upper()}",
            file_names[kind],
            filters[kind],
        )
        if not destination:
            return
        try:
            if kind == "json":
                target = self._library_service.export_json(selected_ids, destination)
            elif kind == "csv":
                target = self._library_service.export_csv(selected_ids, destination)
            else:
                target = self._library_service.export_zip(selected_ids, destination)
        except Exception as exc:
            QMessageBox.critical(self, "Exportar", str(exc))
            return
        QMessageBox.information(self, "Exportar", f"Archivo generado: {target}")


class ContentUIController:
    def __init__(
        self,
        ctx: PageContext,
        *,
        parent: QWidget | None = None,
        on_back: Callable[[], None],
        on_status: Callable[[str], None],
        show_error: Callable[[str], None],
        show_exception: Callable[[BaseException, str], None],
        api_client: EmbeddedContentApiClient | None = None,
        library_service: ContentLibraryService | None = None,
    ) -> None:
        self._ctx = ctx
        self._parent = parent
        self._on_back = on_back
        self._on_status = on_status
        self._show_error = show_error
        self._show_exception = show_exception
        self._library_service = library_service or ContentLibraryService(root_dir=_ctx_root_dir(ctx))
        self._api_client = api_client or EmbeddedContentApiClient(
            root_dir=self._library_service.root_dir,
            library_service=self._library_service,
        )
        self._aliases: list[str] = []
        self._current_alias = ""
        self._current_rows: list[dict[str, Any]] = []
        self._accounts_by_alias: dict[str, list[dict[str, Any]]] = {}
        self._task_payloads: dict[str, dict[str, Any]] = {}
        self._task_lock = threading.RLock()

        self._root = QWidget(parent)
        self._root.setObjectName("ContentPublisherModule")
        self._root_layout = QVBoxLayout(self._root)
        self._root_layout.setContentsMargins(0, 0, 0, 0)
        self._root_layout.setSpacing(0)

        from PySide6.QtWidgets import QStackedWidget

        self._page_stack = QStackedWidget()
        self._page_stack.addWidget(self._build_home_page())
        self._page_stack.addWidget(self._build_extract_page())
        self._page_stack.addWidget(self._build_publish_page())
        self._root_layout.addWidget(self._page_stack)

        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)
        self._refresh_extract_aliases()
        self._refresh_publish_accounts()
        self._refresh_publish_gallery()

    def widget(self) -> QWidget:
        return self._root

    def show_home(self) -> None:
        self._page_stack.setCurrentIndex(0)
        self._refresh_publish_gallery()

    def show_extract(self) -> None:
        self._page_stack.setCurrentIndex(1)
        self._refresh_extract_accounts()

    def show_publish(self) -> None:
        self._page_stack.setCurrentIndex(2)
        self._refresh_publish_accounts()
        self._refresh_publish_gallery()

    def on_activated(self) -> None:
        if self._page_stack.currentIndex() == 2:
            self._refresh_publish_gallery()
        if self._page_stack.currentIndex() == 1:
            self._refresh_extract_accounts()

    def refresh_account_context(
        self,
        *,
        active_alias: str,
        aliases: list[str],
        rows: list[dict[str, Any]],
    ) -> None:
        clean_alias = str(active_alias or "").strip()
        self._current_alias = clean_alias
        self._current_rows = [dict(item) for item in rows if isinstance(item, dict)]
        self._aliases = [str(item or "").strip() for item in aliases if str(item or "").strip()]
        if clean_alias:
            self._accounts_by_alias[clean_alias.lower()] = [dict(item) for item in self._current_rows]
        self._refresh_extract_aliases()
        self._refresh_publish_accounts()

    def _remember_task_payload(self, task_name: str, payload: dict[str, Any]) -> None:
        with self._task_lock:
            self._task_payloads[task_name] = dict(payload)

    def _take_task_payload(self, task_name: str) -> dict[str, Any]:
        with self._task_lock:
            payload = self._task_payloads.pop(task_name, {})
        return payload

    def _accounts_for_alias(self, alias: str) -> list[dict[str, Any]]:
        clean_alias = str(alias or "").strip()
        if clean_alias.lower() == self._current_alias.lower() and self._current_rows:
            return [dict(item) for item in self._current_rows]
        cached = self._accounts_by_alias.get(clean_alias.lower())
        if cached is not None:
            return [dict(item) for item in cached]
        try:
            rows = [
                dict(item)
                for item in self._ctx.services.accounts.list_accounts(clean_alias)
                if isinstance(item, dict)
            ]
        except Exception as exc:
            self._show_exception(exc, "No se pudieron cargar las cuentas del alias para contenido.")
            rows = []
        self._accounts_by_alias[clean_alias.lower()] = [dict(item) for item in rows]
        return rows

    def _all_accounts(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        aliases = self._aliases or ([self._current_alias] if self._current_alias else [])
        for alias in aliases:
            for row in self._accounts_for_alias(alias):
                username = str(row.get("username") or "").strip().lower()
                if not username or username in seen:
                    continue
                seen.add(username)
                rows.append(dict(row))
        if not rows:
            try:
                for row in self._ctx.services.accounts.list_accounts(None):
                    if not isinstance(row, dict):
                        continue
                    username = str(row.get("username") or "").strip().lower()
                    if not username or username in seen:
                        continue
                    seen.add(username)
                    rows.append(dict(row))
            except Exception as exc:
                self._show_exception(exc, "No se pudieron cargar las cuentas disponibles para publicar.")
        rows.sort(key=lambda item: (str(item.get("alias") or "").lower(), str(item.get("username") or "").lower()))
        return rows

    def _busy(self) -> bool:
        return any(self._ctx.tasks.is_running(name) for name in _BLOCKING_TASKS)

    def _build_home_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        back_button = QPushButton("<- Volver a opciones")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(self._on_back)
        header_row.addWidget(back_button)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        panel, panel_layout = _create_panel(
            "Publicación de contenido",
            "Extrae contenido desde perfiles de Instagram y publícalo.",
        )
        buttons_row = QHBoxLayout()
        buttons_row.setContentsMargins(0, 6, 0, 0)
        buttons_row.setSpacing(12)
        buttons_row.addStretch(1)

        extract_button = QPushButton("Obtener contenido")
        extract_button.setObjectName("PrimaryButton")
        extract_button.setMinimumWidth(180)
        extract_button.clicked.connect(self.show_extract)

        publish_button = QPushButton("Publicar contenido")
        publish_button.setObjectName("SecondaryButton")
        publish_button.setMinimumWidth(180)
        publish_button.clicked.connect(self.show_publish)

        buttons_row.addWidget(extract_button)
        buttons_row.addWidget(publish_button)
        buttons_row.addStretch(1)
        panel_layout.addLayout(buttons_row)
        layout.addWidget(panel)
        layout.addStretch(1)
        return page

    def _build_extract_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        back_button = QPushButton("<- Volver a publicación")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(self.show_home)
        header_row.addWidget(back_button)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        panel, panel_layout = _create_panel(
            "Obtener contenido",
            "Selecciona las cuentas que usarás para obtener publicaciones y guardarlas en la biblioteca local.",
        )
        controls = QGridLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setHorizontalSpacing(10)
        controls.setVerticalSpacing(10)

        self._extract_alias_combo = QComboBox()
        self._extract_alias_combo.currentIndexChanged.connect(self._refresh_extract_accounts)
        self._extract_accounts_list = QListWidget()
        self._extract_accounts_list.setSelectionMode(QAbstractItemView.NoSelection)
        self._extract_accounts_list.setMinimumHeight(150)
        self._extract_urls = QPlainTextEdit()
        self._extract_urls.setPlaceholderText("https://instagram.com/profile1\nhttps://instagram.com/profile2")
        self._extract_urls.setMinimumHeight(150)
        self._extract_posts_per_profile = QSpinBox()
        self._extract_posts_per_profile.setRange(1, 50)
        self._extract_posts_per_profile.setValue(3)
        self._extract_button = QPushButton("Obtener contenido")
        self._extract_button.setObjectName("PrimaryButton")
        self._extract_button.clicked.connect(self._start_extract)
        view_button = QPushButton("Ver contenido extraído")
        view_button.setObjectName("SecondaryButton")
        view_button.clicked.connect(self._open_content_viewer)
        select_all_button = QPushButton("Seleccionar todas")
        select_all_button.setObjectName("SecondaryButton")
        select_all_button.clicked.connect(self._select_all_extract_accounts)
        clear_button = QPushButton("Limpiar")
        clear_button.setObjectName("SecondaryButton")
        clear_button.clicked.connect(self._clear_extract_accounts)
        self._extract_result = QLabel("Aún no se ejecutó ninguna extracción.")
        self._extract_result.setObjectName("SectionPanelHint")
        self._extract_result.setWordWrap(True)

        controls.addWidget(QLabel("Alias de cuenta"), 0, 0)
        controls.addWidget(self._extract_alias_combo, 0, 1, 1, 2)
        controls.addWidget(QLabel("Seleccionar cuentas"), 1, 0)
        controls.addWidget(select_all_button, 1, 1)
        controls.addWidget(clear_button, 1, 2)
        controls.addWidget(self._extract_accounts_list, 2, 0, 1, 3)
        controls.addWidget(QLabel("Lista de URLs de perfiles"), 3, 0)
        controls.addWidget(self._extract_urls, 4, 0, 1, 3)
        controls.addWidget(QLabel("Cantidad de publicaciones por perfil"), 5, 0)
        controls.addWidget(self._extract_posts_per_profile, 5, 1)
        controls.addWidget(self._extract_button, 6, 0)
        controls.addWidget(view_button, 6, 1)
        panel_layout.addLayout(controls)
        panel_layout.addWidget(self._extract_result)
        layout.addWidget(panel)
        layout.addStretch(1)
        return page

    def _build_publish_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        back_button = QPushButton("<- Volver a publicación")
        back_button.setObjectName("SecondaryButton")
        back_button.clicked.connect(self.show_home)
        header_row.addWidget(back_button)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        panel, panel_layout = _create_panel(
            "Publicar contenido",
            "Elige una cuenta destino y una pieza de la biblioteca para publicar.",
        )
        controls = QGridLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setHorizontalSpacing(10)
        controls.setVerticalSpacing(10)

        self._publish_account_combo = QComboBox()
        self._publish_account_combo.currentIndexChanged.connect(self._update_publish_action_state)
        refresh_gallery_button = QPushButton("Recargar galería")
        refresh_gallery_button.setObjectName("SecondaryButton")
        refresh_gallery_button.clicked.connect(self._refresh_publish_gallery)
        self._publish_gallery = QListWidget()
        self._publish_gallery.setViewMode(QListView.IconMode)
        self._publish_gallery.setMovement(QListView.Static)
        self._publish_gallery.setResizeMode(QListView.Adjust)
        self._publish_gallery.setSelectionMode(QAbstractItemView.SingleSelection)
        self._publish_gallery.setIconSize(QSize(112, 112))
        self._publish_gallery.setSpacing(12)
        self._publish_gallery.setMinimumHeight(250)
        self._publish_gallery.itemSelectionChanged.connect(self._update_publish_action_state)
        self._publish_selection_hint = QLabel("Selecciona una pieza de la galería para publicar.")
        self._publish_selection_hint.setObjectName("SectionPanelHint")
        self._publish_selection_hint.setWordWrap(True)
        self._publish_button = QPushButton("Publicar")
        self._publish_button.setObjectName("PrimaryButton")
        self._publish_button.clicked.connect(self._start_publish)
        self._publish_log = QPlainTextEdit()
        self._publish_log.setReadOnly(True)
        self._publish_log.setMinimumHeight(220)
        self._publish_log.setPlaceholderText("Los registros de publicación aparecerán aquí.")

        controls.addWidget(QLabel("Cuenta destino"), 0, 0)
        controls.addWidget(self._publish_account_combo, 0, 1)
        controls.addWidget(refresh_gallery_button, 0, 2)
        controls.addWidget(QLabel("Contenido"), 1, 0)
        controls.addWidget(self._publish_gallery, 2, 0, 1, 3)
        controls.addWidget(self._publish_selection_hint, 3, 0, 1, 3)
        controls.addWidget(self._publish_button, 4, 0)
        panel_layout.addLayout(controls)
        panel_layout.addWidget(self._publish_log)
        layout.addWidget(panel)
        layout.addStretch(1)
        return page

    def _refresh_extract_aliases(self) -> None:
        aliases = list(self._aliases)
        if self._current_alias and all(item.lower() != self._current_alias.lower() for item in aliases):
            aliases.append(self._current_alias)
        aliases = sorted({item for item in aliases if item}, key=str.lower)
        self._extract_alias_combo.blockSignals(True)
        self._extract_alias_combo.clear()
        for alias in aliases:
            self._extract_alias_combo.addItem(alias, alias)
        if aliases:
            target_alias = self._current_alias or aliases[0]
            index = self._extract_alias_combo.findData(target_alias)
            if index < 0:
                index = 0
            self._extract_alias_combo.setCurrentIndex(index)
        self._extract_alias_combo.blockSignals(False)
        self._refresh_extract_accounts()

    def _refresh_extract_accounts(self) -> None:
        alias = str(self._extract_alias_combo.currentData() or self._current_alias or "").strip()
        accounts = self._accounts_for_alias(alias)
        self._extract_accounts_list.clear()
        for row in accounts:
            username = str(row.get("username") or "").strip()
            if not username:
                continue
            detail = f"@{username}  |  alias {alias or row.get('alias') or '-'}"
            self._extract_accounts_list.addItem(_checkable_account_item(username, detail))

    def _select_all_extract_accounts(self) -> None:
        for index in range(self._extract_accounts_list.count()):
            item = self._extract_accounts_list.item(index)
            if item is not None:
                item.setCheckState(Qt.Checked)

    def _clear_extract_accounts(self) -> None:
        for index in range(self._extract_accounts_list.count()):
            item = self._extract_accounts_list.item(index)
            if item is not None:
                item.setCheckState(Qt.Unchecked)

    def _selected_extract_accounts(self) -> list[str]:
        usernames: list[str] = []
        for index in range(self._extract_accounts_list.count()):
            item = self._extract_accounts_list.item(index)
            if item is None or item.checkState() != Qt.Checked:
                continue
            username = str(item.data(Qt.UserRole) or "").strip()
            if username:
                usernames.append(username)
        return usernames

    def _refresh_publish_accounts(self) -> None:
        previous = str(self._publish_account_combo.currentData() or "").strip() if hasattr(self, "_publish_account_combo") else ""
        accounts = self._all_accounts()
        self._publish_account_combo.blockSignals(True)
        self._publish_account_combo.clear()
        for row in accounts:
            username = str(row.get("username") or "").strip()
            alias = str(row.get("alias") or "default").strip() or "default"
            self._publish_account_combo.addItem(f"@{username}  |  {alias}", username)
        if accounts:
            index = self._publish_account_combo.findData(previous or _normalize_account_id_from_alias(self._current_alias, accounts))
            if index < 0:
                index = 0
            self._publish_account_combo.setCurrentIndex(index)
        self._publish_account_combo.blockSignals(False)
        self._update_publish_action_state()

    def _refresh_publish_gallery(self) -> None:
        selected_entry_id = self._selected_publish_entry_id()
        self._publish_gallery.clear()
        try:
            entries = self._library_service.list_entries()
        except Exception as exc:
            self._show_exception(exc, "No se pudo cargar la biblioteca de contenido.")
            entries = []
        for entry in entries:
            item = QListWidgetItem(
                f"@{entry.get('source_profile') or '-'}\n{entry.get('caption_preview') or '(sin caption)'}"
            )
            item.setData(Qt.UserRole, int(entry.get("id") or 0))
            item.setToolTip(str(entry.get("caption") or ""))
            item.setSizeHint(QSize(180, 168))
            preview_path = str(entry.get("preview_path") or "").strip()
            if preview_path:
                pixmap = QPixmap(preview_path)
                if not pixmap.isNull():
                    icon = QIcon(pixmap.scaled(QSize(112, 112), Qt.KeepAspectRatio, Qt.SmoothTransformation))
                    item.setIcon(icon)
            self._publish_gallery.addItem(item)
        if selected_entry_id > 0:
            for index in range(self._publish_gallery.count()):
                item = self._publish_gallery.item(index)
                if item is not None and int(item.data(Qt.UserRole) or 0) == selected_entry_id:
                    item.setSelected(True)
                    self._publish_gallery.setCurrentItem(item)
                    break
        self._publish_selection_hint.setText(
            "Selecciona una pieza de la galería para publicar."
            if self._publish_gallery.count()
            else "No hay contenido extraído en la biblioteca."
        )
        self._update_publish_action_state()

    def _selected_publish_entry_id(self) -> int:
        item = self._publish_gallery.currentItem() if hasattr(self, "_publish_gallery") else None
        return int(item.data(Qt.UserRole) or 0) if item is not None else 0

    def _selected_publish_entry(self) -> dict[str, Any]:
        entry_id = self._selected_publish_entry_id()
        if entry_id <= 0:
            return {}
        try:
            return self._library_service.get_entry(entry_id)
        except Exception as exc:
            self._show_exception(exc, "No se pudo cargar el contenido seleccionado.")
            return {}

    def _update_publish_action_state(self) -> None:
        has_account = bool(str(self._publish_account_combo.currentData() or "").strip())
        has_entry = self._selected_publish_entry_id() > 0
        self._publish_button.setEnabled(has_account and has_entry and not self._busy())

    def _start_extract(self) -> None:
        if self._busy():
            self._show_error("Espera a que finalice la automatización actual antes de extraer contenido.")
            return
        alias = str(self._extract_alias_combo.currentData() or self._current_alias or "").strip()
        account_ids = self._selected_extract_accounts()
        if not account_ids:
            self._show_error("Selecciona al menos una cuenta para extraer contenido.")
            return
        profile_urls = [str(line).strip() for line in self._extract_urls.toPlainText().splitlines() if str(line).strip()]
        if not profile_urls:
            self._show_error("Pega al menos una URL de perfil para extraer contenido.")
            return
        posts_per_profile = int(self._extract_posts_per_profile.value() or 1)
        self._extract_button.setEnabled(False)
        self._extract_result.setText("Obteniendo contenido...")
        self._on_status("Extrayendo contenido...")
        try:
            self._ctx.tasks.start_task(
                _CONTENT_EXTRACT_TASK,
                lambda selected_alias=alias, selected_accounts=list(account_ids), selected_urls=list(profile_urls), count=posts_per_profile: self._execute_extract_request(
                    alias=selected_alias,
                    account_ids=selected_accounts,
                    profile_urls=selected_urls,
                    posts_per_profile=count,
                ),
                metadata={"alias": alias},
            )
        except Exception as exc:
            self._extract_button.setEnabled(True)
            self._show_exception(exc, "No se pudo iniciar la extracción de contenido.")

    def _execute_extract_request(
        self,
        *,
        alias: str,
        account_ids: list[str],
        profile_urls: list[str],
        posts_per_profile: int,
    ) -> str:
        payload = self._api_client.extract(
            alias=alias,
            account_ids=account_ids,
            profile_urls=profile_urls,
            posts_per_profile=posts_per_profile,
        )
        self._remember_task_payload(_CONTENT_EXTRACT_TASK, payload)
        return ""

    def _start_publish(self) -> None:
        if self._busy():
            self._show_error("Espera a que finalice la automatización actual antes de publicar contenido.")
            return
        account_id = str(self._publish_account_combo.currentData() or "").strip()
        entry = self._selected_publish_entry()
        if not account_id:
            self._show_error("Selecciona una cuenta destino.")
            return
        if not entry:
            self._show_error("Selecciona un contenido de la galería.")
            return
        self._publish_button.setEnabled(False)
        self._publish_log.clear()
        self._publish_log.appendPlainText("Preparando publicación...")
        self._on_status("Publicando contenido...")
        try:
            self._ctx.tasks.start_task(
                _CONTENT_PUBLISH_TASK,
                lambda selected_account=account_id, selected_entry=dict(entry): self._execute_publish_request(
                    account_id=selected_account,
                    media_path=str(selected_entry.get("media_path") or ""),
                    caption=str(selected_entry.get("caption") or ""),
                ),
            )
        except Exception as exc:
            self._publish_button.setEnabled(True)
            self._show_exception(exc, "No se pudo iniciar la publicación de contenido.")

    def _execute_publish_request(self, *, account_id: str, media_path: str, caption: str) -> str:
        payload = self._api_client.publish(
            account_id=account_id,
            media_path=media_path,
            caption=caption,
        )
        self._remember_task_payload(_CONTENT_PUBLISH_TASK, payload)
        return ""

    def _open_content_viewer(self) -> None:
        try:
            entries = self._library_service.list_entries()
        except Exception as exc:
            self._show_exception(exc, "No se pudo abrir la biblioteca de contenido.")
            return
        if not entries:
            self._show_error("Aún no hay contenido extraído para mostrar.")
            return
        dialog = ContentLibraryViewerDialog(
            entries=entries,
            library_service=self._library_service,
            parent=self._parent,
        )
        dialog.exec()

    def _on_task_completed(self, task_name: str, ok: bool, message: str, result: object) -> None:
        del result
        if task_name == _CONTENT_EXTRACT_TASK:
            self._extract_button.setEnabled(True)
            payload = self._take_task_payload(task_name)
            if ok and payload:
                logs = [str(item) for item in payload.get("logs") or [] if str(item or "").strip()]
                summary = str(payload.get("summary") or "Extracción completada.").strip()
                self._extract_result.setText(summary)
                if logs:
                    self._extract_result.setText(summary + "\n" + "\n".join(logs[-3:]))
                self._refresh_publish_gallery()
                self._on_status(summary)
            else:
                self._extract_result.setText(message or "La extracción finalizó con errores.")
                self._on_status(message or "La extracción finalizó con errores.")
            self._update_publish_action_state()
            return
        if task_name != _CONTENT_PUBLISH_TASK:
            return
        self._publish_button.setEnabled(True)
        payload = self._take_task_payload(task_name)
        if ok and payload:
            logs = [str(item) for item in payload.get("logs") or [] if str(item or "").strip()]
            for line in logs:
                self._publish_log.appendPlainText(line)
            summary = str(payload.get("summary") or "Contenido publicado.").strip()
            self._publish_log.appendPlainText(summary)
            self._on_status(summary)
        else:
            self._publish_log.appendPlainText(message or "La publicación finalizó con errores.")
            self._on_status(message or "La publicación finalizó con errores.")
        self._update_publish_action_state()


def _normalize_account_id_from_alias(active_alias: str, accounts: list[dict[str, Any]]) -> str:
    clean_alias = str(active_alias or "").strip().lower()
    if not clean_alias:
        return ""
    for row in accounts:
        if str(row.get("alias") or "").strip().lower() == clean_alias:
            return str(row.get("username") or "").strip()
    return ""
