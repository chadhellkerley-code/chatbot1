from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QStackedWidget,
    QTableWidget,
    QWidget,
)

from gui.automation_dialogs import confirm_automation_action
from gui.query_runner import QueryError

from .automation_pages_base import AutomationSectionPage
from .page_base import PageContext, table_item
from .snapshot_queries import build_automation_whatsapp_snapshot


class AutomationWhatsAppPage(AutomationSectionPage):
    _PANELS: tuple[tuple[str, str], ...] = (
        ("connect", "Conectar numero"),
        ("lists", "Listas de contactos"),
        ("templates", "Plantillas de mensajes"),
        ("send", "Envio de mensajes"),
        ("autoresponder", "Autoresponder"),
    )

    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "WhatsApp",
            "Paneles separados para conexion, listas, plantillas, envio y autoresponder.",
            route_key="automation_whatsapp_page",
            parent=parent,
        )
        nav = QHBoxLayout()
        nav.setContentsMargins(0, 0, 0, 0)
        nav.setSpacing(8)
        self._panel_buttons: dict[str, QPushButton] = {}
        self._panels = QStackedWidget()
        for key, label in self._PANELS:
            button = QPushButton(label)
            button.setCheckable(True)
            button.clicked.connect(lambda checked=False, panel_key=key: self._set_panel(panel_key))
            self._panel_buttons[key] = button
            nav.addWidget(button)
        nav.addStretch(1)
        self.content_layout().addLayout(nav)
        self.content_layout().addWidget(self._panels, 1)

        self._build_connect_panel()
        self._build_lists_panel()
        self._build_templates_panel()
        self._build_send_panel()
        self._build_autoresponder_panel()

        self._ctx.tasks.taskCompleted.connect(self._on_task_completed)
        self._rows_snapshot: dict[str, Any] = {}
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._set_panel("connect")

    def _build_connect_panel(self) -> None:
        page = QWidget()
        root = QHBoxLayout(page)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        panel, layout = self.create_panel("Conectar numero", "Vincula una sesion de WhatsApp Web via QR con Playwright.")
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        self._number_alias = QLineEdit()
        self._number_phone = QLineEdit()
        connect_button = QPushButton("Conectar")
        connect_button.setObjectName("PrimaryButton")
        connect_button.clicked.connect(self._connect_number)
        grid.addWidget(QLabel("Alias"), 0, 0)
        grid.addWidget(self._number_alias, 0, 1)
        grid.addWidget(QLabel("Telefono"), 1, 0)
        grid.addWidget(self._number_phone, 1, 1)
        grid.addWidget(connect_button, 2, 1, 1, 1, Qt.AlignLeft)
        layout.addLayout(grid)
        self._numbers_table = QTableWidget(0, 3)
        self._numbers_table.setHorizontalHeaderLabels(["Alias", "Telefono", "Estado"])
        self._numbers_table.verticalHeader().setVisible(False)
        self._numbers_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._numbers_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._numbers_table)
        root.addWidget(panel)
        self._panels.addWidget(page)

    def _build_lists_panel(self) -> None:
        page = QWidget()
        root = QHBoxLayout(page)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        panel, layout = self.create_panel("Listas de contactos", "Gestiona listas con Nombre | Telefono para envios de WhatsApp.")
        self._list_name = QLineEdit()
        self._list_contacts = QPlainTextEdit()
        self._list_contacts.setPlaceholderText("Juan|+59800000000\nMaria|+59811111111")
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(8)
        form.addWidget(QLabel("Nombre de lista"), 0, 0)
        form.addWidget(self._list_name, 0, 1)
        form.addWidget(QLabel("Contactos"), 1, 0, 1, 2)
        form.addWidget(self._list_contacts, 2, 0, 1, 2)
        layout.addLayout(form)
        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        for label, object_name, handler in (
            ("Guardar", "PrimaryButton", self._save_contact_list),
            ("Cargar", "SecondaryButton", self._load_selected_contact_list),
            ("Eliminar", "DangerButton", self._delete_selected_contact_list),
        ):
            button = QPushButton(label)
            button.setObjectName(object_name)
            button.clicked.connect(handler)
            actions.addWidget(button)
        actions.addStretch(1)
        layout.addLayout(actions)
        self._lists_table = QTableWidget(0, 2)
        self._lists_table.setHorizontalHeaderLabels(["Lista", "Contactos"])
        self._lists_table.verticalHeader().setVisible(False)
        self._lists_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._lists_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._lists_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._lists_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._lists_table)
        root.addWidget(panel)
        self._panels.addWidget(page)

    def _build_templates_panel(self) -> None:
        page = QWidget()
        root = QHBoxLayout(page)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        panel, layout = self.create_panel("Plantillas de mensajes", "Crea plantillas reutilizables para envios de WhatsApp.")
        self._template_id = ""
        self._template_name = QLineEdit()
        self._template_content = QPlainTextEdit()
        form = QGridLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(8)
        form.addWidget(QLabel("Nombre de plantilla"), 0, 0)
        form.addWidget(self._template_name, 0, 1)
        form.addWidget(QLabel("Contenido del mensaje"), 1, 0, 1, 2)
        form.addWidget(self._template_content, 2, 0, 1, 2)
        layout.addLayout(form)
        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        for label, object_name, handler in (
            ("Guardar", "PrimaryButton", self._save_template),
            ("Cargar", "SecondaryButton", self._load_selected_template),
            ("Eliminar", "DangerButton", self._delete_selected_template),
        ):
            button = QPushButton(label)
            button.setObjectName(object_name)
            button.clicked.connect(handler)
            actions.addWidget(button)
        actions.addStretch(1)
        layout.addLayout(actions)
        self._templates_table = QTableWidget(0, 2)
        self._templates_table.setHorizontalHeaderLabels(["ID", "Nombre"])
        self._templates_table.verticalHeader().setVisible(False)
        self._templates_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._templates_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._templates_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._templates_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._templates_table)
        root.addWidget(panel)
        self._panels.addWidget(page)

    def _build_send_panel(self) -> None:
        page = QWidget()
        root = QHBoxLayout(page)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        panel, layout = self.create_panel("Envio de mensajes", "Selecciona lista, plantilla y delays para programar envios.")
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        self._send_list_combo = QComboBox()
        self._send_template_combo = QComboBox()
        self._send_number_combo = QComboBox()
        self._send_delay_min = QSpinBox()
        self._send_delay_min.setRange(1, 3600)
        self._send_delay_min.setValue(10)
        self._send_delay_max = QSpinBox()
        self._send_delay_max.setRange(1, 3600)
        self._send_delay_max.setValue(20)
        send_button = QPushButton("Iniciar")
        send_button.setObjectName("PrimaryButton")
        send_button.clicked.connect(self._schedule_send_run)
        grid.addWidget(QLabel("Lista"), 0, 0)
        grid.addWidget(self._send_list_combo, 0, 1)
        grid.addWidget(QLabel("Plantilla"), 1, 0)
        grid.addWidget(self._send_template_combo, 1, 1)
        grid.addWidget(QLabel("Numero"), 2, 0)
        grid.addWidget(self._send_number_combo, 2, 1)
        grid.addWidget(QLabel("Delay minimo"), 3, 0)
        grid.addWidget(self._send_delay_min, 3, 1)
        grid.addWidget(QLabel("Delay maximo"), 4, 0)
        grid.addWidget(self._send_delay_max, 4, 1)
        grid.addWidget(send_button, 5, 1, 1, 1, Qt.AlignLeft)
        layout.addLayout(grid)
        self._runs_table = QTableWidget(0, 4)
        self._runs_table.setHorizontalHeaderLabels(["Run", "Lista", "Estado", "Contactos"])
        self._runs_table.verticalHeader().setVisible(False)
        self._runs_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._runs_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._runs_table)
        root.addWidget(panel)
        self._panels.addWidget(page)

    def _build_autoresponder_panel(self) -> None:
        page = QWidget()
        root = QHBoxLayout(page)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        panel, layout = self.create_panel("Autoresponder de WhatsApp", "Configura respuestas automaticas con prompt IA o mensajes fijos.")
        self._wa_mode = QComboBox()
        self._wa_mode.addItem("Prompt IA", "ia")
        self._wa_mode.addItem("Mensajes fijos", "fijo")
        self._wa_enabled = QCheckBox("Habilitado")
        self._wa_prompt = QPlainTextEdit()
        self._wa_fixed = QPlainTextEdit()
        self._wa_mode.currentIndexChanged.connect(self._update_whatsapp_autoresponder_visibility)
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        grid.addWidget(QLabel("Modo"), 0, 0)
        grid.addWidget(self._wa_mode, 0, 1)
        grid.addWidget(self._wa_enabled, 0, 2)
        grid.addWidget(QLabel("Prompt IA"), 1, 0, 1, 3)
        grid.addWidget(self._wa_prompt, 2, 0, 1, 3)
        grid.addWidget(QLabel("Mensaje fijo"), 3, 0, 1, 3)
        grid.addWidget(self._wa_fixed, 4, 0, 1, 3)
        layout.addLayout(grid)
        save_button = QPushButton("Guardar")
        save_button.setObjectName("PrimaryButton")
        save_button.clicked.connect(self._save_whatsapp_autoresponder)
        layout.addWidget(save_button, 0, Qt.AlignLeft)
        root.addWidget(panel)
        self._panels.addWidget(page)

    def _set_panel(self, panel_key: str) -> None:
        keys = [key for key, _ in self._PANELS]
        target = panel_key if panel_key in keys else keys[0]
        for index, key in enumerate(keys):
            self._panel_buttons[key].setChecked(key == target)
            if key == target:
                self._panels.setCurrentIndex(index)

    def _parse_contact_lines(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for raw_line in str(self._list_contacts.toPlainText() or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if "|" in line:
                name, number = line.split("|", 1)
            else:
                name, number = line, line
            rows.append({"name": str(name).strip(), "number": str(number).strip()})
        return rows

    def _selected_table_value(self, table: QTableWidget, column: int = 0) -> str:
        row = table.currentRow()
        if row < 0:
            return ""
        item = table.item(row, column)
        return str(item.text() if item else "").strip()

    def _connect_number(self) -> None:
        alias = str(self._number_alias.text() or "").strip()
        phone = str(self._number_phone.text() or "").strip()
        try:
            self._ctx.tasks.start_task("whatsapp_connect", lambda: self._ctx.services.automation.connect_whatsapp_number(alias=alias, phone=phone))
        except Exception as exc:
            self.show_exception(exc, "No se pudo iniciar la vinculacion de WhatsApp.")
            return
        self.set_status("Abriendo vinculacion de WhatsApp en segundo plano...")

    def _save_contact_list(self) -> None:
        try:
            self._ctx.services.automation.save_whatsapp_contact_list(str(self._list_name.text() or "").strip(), self._parse_contact_lines())
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar la lista.")
            return
        self.set_status("Lista guardada.")
        self.refresh_page()

    def _load_selected_contact_list(self) -> None:
        alias = self._selected_table_value(self._lists_table, 0)
        if not alias:
            self.show_error("Selecciona una lista.")
            return
        for row in self._rows_snapshot.get("lists", []):
            if isinstance(row, dict) and str(row.get("alias") or "").strip() == alias:
                self._list_name.setText(alias)
                self._list_contacts.setPlainText(
                    "\n".join(
                        f"{str(item.get('name') or '')}|{str(item.get('number') or '')}"
                        for item in row.get("contacts") or []
                        if isinstance(item, dict)
                    )
                )
                return

    def _delete_selected_contact_list(self) -> None:
        alias = self._selected_table_value(self._lists_table, 0)
        if not alias:
            self.show_error("Selecciona una lista.")
            return
        if not confirm_automation_action(self, title="Eliminar lista", message=f"Se eliminara la lista '{alias}'.", confirm_text="Eliminar", danger=True):
            return
        try:
            self._ctx.services.automation.delete_whatsapp_contact_list(alias)
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar la lista.")
            return
        self.refresh_page()

    def _save_template(self) -> None:
        try:
            saved = self._ctx.services.automation.save_whatsapp_template(
                self._template_id,
                str(self._template_name.text() or "").strip(),
                str(self._template_content.toPlainText() or "").strip(),
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar la plantilla.")
            return
        self._template_id = str(saved.get("id") or "")
        self.set_status("Plantilla guardada.")
        self.refresh_page()

    def _load_selected_template(self) -> None:
        template_id = self._selected_table_value(self._templates_table, 0)
        if not template_id:
            self.show_error("Selecciona una plantilla.")
            return
        for row in self._rows_snapshot.get("templates", []):
            if isinstance(row, dict) and str(row.get("id") or "").strip() == template_id:
                self._template_id = template_id
                self._template_name.setText(str(row.get("name") or ""))
                self._template_content.setPlainText(str(row.get("content") or ""))
                return

    def _delete_selected_template(self) -> None:
        template_id = self._selected_table_value(self._templates_table, 0)
        if not template_id:
            self.show_error("Selecciona una plantilla.")
            return
        if not confirm_automation_action(self, title="Eliminar plantilla", message=f"Se eliminara la plantilla '{template_id}'.", confirm_text="Eliminar", danger=True):
            return
        try:
            self._ctx.services.automation.delete_whatsapp_template(template_id)
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar la plantilla.")
            return
        self._template_id = ""
        self._template_name.clear()
        self._template_content.clear()
        self.refresh_page()

    def _schedule_send_run(self) -> None:
        try:
            self._ctx.services.automation.schedule_whatsapp_message_run(
                list_alias=str(self._send_list_combo.currentData() or "").strip(),
                template_id=str(self._send_template_combo.currentData() or "").strip(),
                number_id=str(self._send_number_combo.currentData() or "").strip(),
                delay_min=float(self._send_delay_min.value()),
                delay_max=float(self._send_delay_max.value()),
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo programar el envio.")
            return
        self.set_status("Envio programado.")
        self.refresh_page()

    def _update_whatsapp_autoresponder_visibility(self) -> None:
        mode = str(self._wa_mode.currentData() or "ia")
        self._wa_prompt.setVisible(mode == "ia")
        self._wa_fixed.setVisible(mode == "fijo")

    def _save_whatsapp_autoresponder(self) -> None:
        try:
            self._ctx.services.automation.save_whatsapp_autoresponder_config(
                mode=str(self._wa_mode.currentData() or "ia"),
                prompt=str(self._wa_prompt.toPlainText() or "").strip(),
                fixed_message=str(self._wa_fixed.toPlainText() or "").strip(),
                enabled=self._wa_enabled.isChecked(),
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar el autoresponder de WhatsApp.")
            return
        self.set_status("Config de autoresponder de WhatsApp guardada.")
        self.refresh_page()

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        self._rows_snapshot = dict(snapshot)
        numbers = snapshot.get("numbers") if isinstance(snapshot.get("numbers"), list) else []
        self._numbers_table.setRowCount(len(numbers))
        self._send_number_combo.clear()
        for row_index, row in enumerate(numbers):
            if not isinstance(row, dict):
                continue
            state_label = "Conectado" if bool(row.get("connected")) else str(row.get("connection_state") or "Pendiente")
            self._numbers_table.setItem(row_index, 0, table_item(row.get("alias", "")))
            self._numbers_table.setItem(row_index, 1, table_item(row.get("phone", "")))
            self._numbers_table.setItem(row_index, 2, table_item(state_label))
            number_id = str(row.get("id") or "").strip()
            if number_id:
                self._send_number_combo.addItem(str(row.get("alias") or number_id), number_id)

        lists = snapshot.get("lists") if isinstance(snapshot.get("lists"), list) else []
        self._lists_table.setRowCount(len(lists))
        self._send_list_combo.clear()
        for row_index, row in enumerate(lists):
            if not isinstance(row, dict):
                continue
            alias = str(row.get("alias") or "")
            total = len(row.get("contacts") or [])
            self._lists_table.setItem(row_index, 0, table_item(alias))
            self._lists_table.setItem(row_index, 1, table_item(total))
            self._send_list_combo.addItem(alias, alias)

        templates = snapshot.get("templates") if isinstance(snapshot.get("templates"), list) else []
        self._templates_table.setRowCount(len(templates))
        self._send_template_combo.clear()
        for row_index, row in enumerate(templates):
            if not isinstance(row, dict):
                continue
            template_id = str(row.get("id") or "")
            name = str(row.get("name") or template_id)
            self._templates_table.setItem(row_index, 0, table_item(template_id))
            self._templates_table.setItem(row_index, 1, table_item(name))
            self._send_template_combo.addItem(name, template_id)

        runs = snapshot.get("runs") if isinstance(snapshot.get("runs"), list) else []
        self._runs_table.setRowCount(len(runs))
        for row_index, row in enumerate(runs):
            if not isinstance(row, dict):
                continue
            self._runs_table.setItem(row_index, 0, table_item(row.get("id", "")))
            self._runs_table.setItem(row_index, 1, table_item(row.get("list_alias", "")))
            self._runs_table.setItem(row_index, 2, table_item(row.get("status", "")))
            self._runs_table.setItem(row_index, 3, table_item(len(row.get("events") or [])))

        autoresponder = snapshot.get("autoresponder") if isinstance(snapshot.get("autoresponder"), dict) else {}
        self._wa_mode.setCurrentIndex(max(0, self._wa_mode.findData(str(autoresponder.get("mode") or "ia"))))
        self._wa_enabled.setChecked(bool(autoresponder.get("enabled")))
        self._wa_prompt.setPlainText(str(autoresponder.get("prompt") or ""))
        self._wa_fixed.setPlainText(str(autoresponder.get("fixed_message") or ""))
        self._update_whatsapp_autoresponder_visibility()
        self.clear_status()

    def _on_task_completed(self, task_name: str, success: bool, message: str, result: object) -> None:
        if task_name != "whatsapp_connect":
            return
        self.set_status("Numero vinculado." if success else str(message or "No se pudo vincular el numero."))
        self.refresh_page()

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_whatsapp_snapshot(self._ctx.services),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def on_navigate_to(self, payload: Any = None) -> None:
        self.refresh_page()

    def _on_snapshot_loaded(self, request_id: int, payload: Any) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self._apply_snapshot(dict(payload) if isinstance(payload, dict) else {})

    def _on_snapshot_failed(self, request_id: int, error: QueryError) -> None:
        if request_id != self._snapshot_request_id:
            return
        self._snapshot_loading = False
        self.set_status(f"No se pudo cargar WhatsApp: {error.message}")
