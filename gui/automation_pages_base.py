from __future__ import annotations

import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPlainTextEdit,
    QPushButton,
    QDialog,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from gui.automation_dialogs import (
    AutomationMessageDialog,
    AutomationTextInputDialog,
    confirm_automation_action,
    open_automation_file_dialog,
)
from gui.query_runner import QueryError

from .page_base import ClickableMetricCard, PageContext, SectionPage
from .snapshot_queries import build_automation_config_snapshot, build_automation_home_snapshot


logger = logging.getLogger(__name__)

AUTOMATION_SUBSECTIONS: tuple[tuple[str, str], ...] = (
    ("automation_config_page", "Config"),
    ("automation_autoresponder_page", "Autoresponder"),
    ("automation_packs_page", "Packs"),
    ("automation_flow_page", "Flow"),
    ("automation_whatsapp_page", "WhatsApp"),
)


def parse_iso(value: str) -> datetime | None:
    clean = str(value or "").strip().rstrip("Z")
    if not clean:
        return None
    try:
        return datetime.fromisoformat(clean)
    except Exception:
        return None


def format_run_duration(started_at: str, finished_at: str) -> str:
    started = parse_iso(started_at)
    finished = parse_iso(finished_at) or datetime.now()
    if started is None:
        return "-"
    total = max(0, int((finished - started).total_seconds()))
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def checked_values(widget: QListWidget) -> list[str]:
    values: list[str] = []
    for index in range(widget.count()):
        item = widget.item(index)
        if item and item.checkState() == Qt.Checked:
            values.append(str(item.data(Qt.UserRole) or "").strip())
    return [item for item in values if item]


def set_check_items(widget: QListWidget, rows: list[tuple[str, str]], selected: set[str]) -> None:
    widget.clear()
    for value, label in rows:
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, value)
        item.setFlags(item.flags() | Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked if value in selected else Qt.Unchecked)
        widget.addItem(item)


class AutomationSectionPage(SectionPage):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        route_key: str | None,
        back_button: bool = True,
        scrollable: bool = True,
        parent=None,
    ) -> None:
        super().__init__(
            ctx,
            title,
            subtitle,
            section_title="Automatizaciones",
            section_subtitle="Submenu horizontal para config, autoresponder, packs, flow y WhatsApp.",
            section_routes=AUTOMATION_SUBSECTIONS,
            route_key=route_key,
            back_button=back_button,
            scrollable=scrollable,
            parent=parent,
        )

    def _show_dark_message(self, title: str, message: str, *, detail: str = "", danger: bool = False) -> None:
        dialog = AutomationMessageDialog(
            title=title,
            message=message,
            detail=detail,
            danger=danger,
            parent=self,
        )
        dialog.exec()

    def show_error(self, text: str) -> None:
        self.set_status(text)
        self._show_dark_message("Error", str(text or "Error"), danger=True)

    def show_exception(self, exc: BaseException, user_message: str = "No se pudo completar la accion.") -> None:
        logger.error("Automation GUI action failed", exc_info=(type(exc), exc, exc.__traceback__))
        try:
            self._ctx.logs.append("[error] Automation GUI action failed\n")
            self._ctx.logs.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        except Exception:
            pass
        self.set_status(user_message)
        self._show_dark_message("Error", user_message, detail=str(exc), danger=True)

    def show_info(self, text: str) -> None:
        self.set_status(text)
        self._show_dark_message("Informacion", str(text or ""))


class AutomationHomePage(AutomationSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Automatizaciones",
            "Centro operativo para configuracion, autoresponder, packs, flow y WhatsApp.",
            route_key=None,
            back_button=False,
            parent=parent,
        )
        panel, layout = self.create_panel(
            "Centro de automatizaciones",
            "Vista general del estado actual del modulo con accesos rapidos a los paneles operativos.",
        )
        self._summary = QLabel("")
        self._summary.setObjectName("SectionPanelHint")
        self._summary.setWordWrap(True)
        layout.addWidget(self._summary)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(10)
        self._cards = {
            "config": ClickableMetricCard("Alias activo", "-"),
            "autoresponder": ClickableMetricCard("Hydration pendiente", "0"),
            "packs": ClickableMetricCard("Packs", "0"),
            "whatsapp": ClickableMetricCard("WhatsApp activos", "0"),
        }
        self._cards["config"].clicked.connect(lambda: self._ctx.open_route("automation_config_page", None))
        self._cards["autoresponder"].clicked.connect(lambda: self._ctx.open_route("automation_autoresponder_page", None))
        self._cards["packs"].clicked.connect(lambda: self._ctx.open_route("automation_packs_page", None))
        self._cards["whatsapp"].clicked.connect(lambda: self._ctx.open_route("automation_whatsapp_page", None))
        for index, key in enumerate(("config", "autoresponder", "packs", "whatsapp")):
            grid.addWidget(self._cards[key], index // 2, index % 2)
        layout.addLayout(grid)
        self.content_layout().addWidget(panel)
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        self._cards["config"].set_value(str(payload.get("alias") or self._ctx.state.active_alias or "-"))
        self._cards["autoresponder"].set_value(payload.get("pending_hydration", 0))
        self._cards["packs"].set_value(payload.get("packs", 0))
        self._cards["whatsapp"].set_value(payload.get("runs_active", 0))
        self._summary.setText(str(payload.get("summary") or ""))
        self.clear_status()

    def _request_refresh(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_home_snapshot(self._ctx.services, active_alias=self._ctx.state.active_alias),
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
        self.set_status(f"No se pudo cargar el resumen: {error.message}")

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self._request_refresh()


class AutomationConfigPage(AutomationSectionPage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Config",
            "Paneles independientes para API Key, prompt de objeciones y cuentas para follow-up.",
            route_key="automation_config_page",
            parent=parent,
        )
        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        self._alias_combo = QComboBox()
        self._alias_combo.currentIndexChanged.connect(self.refresh_page)
        refresh_button = QPushButton("Recargar")
        refresh_button.setObjectName("SecondaryButton")
        refresh_button.clicked.connect(self.refresh_page)
        header.addWidget(QLabel("Alias para follow-up"))
        header.addWidget(self._alias_combo, 1)
        header.addWidget(refresh_button)
        self.content_layout().addLayout(header)

        api_panel, api_layout = self.create_panel(
            "API Key de OpenAI",
            "Configura la clave usada por funciones IA del modulo.",
        )
        self._api_summary = QLabel("Sin API Key configurada.")
        self._api_summary.setObjectName("SectionPanelHint")
        self._api_summary.setWordWrap(True)
        api_button = QPushButton("Configurar API Key")
        api_button.setObjectName("PrimaryButton")
        api_button.clicked.connect(self._edit_api_key)
        api_layout.addWidget(self._api_summary)
        api_layout.addWidget(api_button, 0, Qt.AlignLeft)
        self.content_layout().addWidget(api_panel)

        prompt_panel, prompt_layout = self.create_panel(
            "Prompt de objeciones",
            "Crea, importa y reutiliza prompts nombrados para objeciones en packs y flows.",
        )
        prompt_controls = QGridLayout()
        prompt_controls.setContentsMargins(0, 0, 0, 0)
        prompt_controls.setHorizontalSpacing(10)
        prompt_controls.setVerticalSpacing(8)
        self._prompt_selector = QComboBox()
        self._prompt_selector.currentIndexChanged.connect(self._load_selected_prompt)
        self._prompt_name = QLineEdit()
        self._prompt_content = QPlainTextEdit()
        prompt_controls.addWidget(QLabel("Prompt guardado"), 0, 0)
        prompt_controls.addWidget(self._prompt_selector, 0, 1)
        prompt_controls.addWidget(QLabel("Nombre del prompt"), 1, 0)
        prompt_controls.addWidget(self._prompt_name, 1, 1)
        prompt_controls.addWidget(QLabel("Contenido del prompt"), 2, 0, 1, 2)
        prompt_controls.addWidget(self._prompt_content, 3, 0, 1, 2)
        prompt_layout.addLayout(prompt_controls)
        prompt_actions = QHBoxLayout()
        prompt_actions.setContentsMargins(0, 0, 0, 0)
        prompt_actions.setSpacing(8)
        new_prompt = QPushButton("Nuevo")
        new_prompt.setObjectName("SecondaryButton")
        new_prompt.clicked.connect(self._new_prompt)
        save_prompt = QPushButton("Guardar")
        save_prompt.setObjectName("PrimaryButton")
        save_prompt.clicked.connect(self._save_prompt)
        delete_prompt = QPushButton("Eliminar")
        delete_prompt.setObjectName("DangerButton")
        delete_prompt.clicked.connect(self._delete_prompt)
        import_prompt = QPushButton("Importar TXT")
        import_prompt.setObjectName("SecondaryButton")
        import_prompt.clicked.connect(self._import_prompt_txt)
        prompt_actions.addWidget(new_prompt)
        prompt_actions.addWidget(save_prompt)
        prompt_actions.addWidget(delete_prompt)
        prompt_actions.addWidget(import_prompt)
        prompt_actions.addStretch(1)
        prompt_layout.addLayout(prompt_actions)
        self.content_layout().addWidget(prompt_panel)

        follow_panel, follow_layout = self.create_panel(
            "Cuentas para follow-up",
            "Define si los follow-ups usan todas las cuentas, alias especificos o cuentas individuales.",
        )
        self._follow_mode = QComboBox()
        self._follow_mode.addItem("Todas las cuentas", "all")
        self._follow_mode.addItem("Cuentas por alias", "alias")
        self._follow_mode.addItem("Cuentas individuales", "individual")
        self._follow_mode.currentIndexChanged.connect(self._update_followup_visibility)
        follow_layout.addWidget(QLabel("Modo"))
        follow_layout.addWidget(self._follow_mode)
        self._follow_scope_stack = QStackedWidget()
        self._follow_scope_stack.setObjectName("AutomationFollowupScope")
        all_scope = QWidget()
        all_scope_layout = QVBoxLayout(all_scope)
        all_scope_layout.setContentsMargins(0, 0, 0, 0)
        all_scope_layout.setSpacing(8)
        all_scope_hint = QLabel(
            "Todas las cuentas activas disponibles podran encargarse de responder follow-ups."
        )
        all_scope_hint.setObjectName("SectionPanelHint")
        all_scope_hint.setWordWrap(True)
        all_scope_layout.addWidget(all_scope_hint)
        all_scope_layout.addStretch(1)

        self._follow_aliases = QListWidget()
        self._follow_aliases.setMinimumHeight(180)
        self._follow_accounts = QListWidget()
        self._follow_accounts.setMinimumHeight(180)

        alias_scope = QWidget()
        alias_scope_layout = QVBoxLayout(alias_scope)
        alias_scope_layout.setContentsMargins(0, 0, 0, 0)
        alias_scope_layout.setSpacing(8)
        alias_scope_layout.addWidget(QLabel("Aliases disponibles"))
        alias_scope_layout.addWidget(self._follow_aliases)

        account_scope = QWidget()
        account_scope_layout = QVBoxLayout(account_scope)
        account_scope_layout.setContentsMargins(0, 0, 0, 0)
        account_scope_layout.setSpacing(8)
        account_scope_layout.addWidget(QLabel("Cuentas disponibles"))
        account_scope_layout.addWidget(self._follow_accounts)

        self._follow_scope_stack.addWidget(all_scope)
        self._follow_scope_stack.addWidget(alias_scope)
        self._follow_scope_stack.addWidget(account_scope)
        follow_layout.addWidget(self._follow_scope_stack)
        self._follow_summary = QLabel("")
        self._follow_summary.setObjectName("SectionPanelHint")
        self._follow_summary.setWordWrap(True)
        follow_layout.addWidget(self._follow_summary)
        save_follow = QPushButton("Guardar configuracion")
        save_follow.setObjectName("PrimaryButton")
        save_follow.clicked.connect(self._save_followup_selection)
        follow_layout.addWidget(save_follow, 0, Qt.AlignLeft)
        self.content_layout().addWidget(follow_panel)

        self._prompt_rows: list[dict[str, str]] = []
        self._snapshot_request_id = 0
        self._snapshot_loading = False
        self._snapshot_cache: dict[str, Any] | None = None

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        aliases = payload.get("aliases") if isinstance(payload.get("aliases"), list) else []
        current_alias = str(payload.get("selected_alias") or self._ctx.state.active_alias).strip()
        self._alias_combo.blockSignals(True)
        self._alias_combo.clear()
        for alias in aliases:
            self._alias_combo.addItem(str(alias), str(alias))
        self._alias_combo.setCurrentIndex(max(0, self._alias_combo.findData(current_alias)))
        self._alias_combo.blockSignals(False)

        api_key = str(payload.get("api_key") or "").strip()
        self._api_summary.setText("API Key configurada." if api_key else "Sin API Key configurada.")

        self._prompt_rows = [
            {"name": str(item.get("name") or ""), "content": str(item.get("content") or "")}
            for item in payload.get("objection_prompts") or []
            if isinstance(item, dict)
        ]
        prompt_entry = payload.get("prompt_entry") if isinstance(payload.get("prompt_entry"), dict) else {}
        selected_prompt_name = str(prompt_entry.get("objection_strategy_name") or "").strip()
        selected_prompt_content = str(prompt_entry.get("objection_prompt") or "").strip()
        self._prompt_selector.blockSignals(True)
        self._prompt_selector.clear()
        self._prompt_selector.addItem("Selecciona un prompt", "")
        for row in self._prompt_rows:
            self._prompt_selector.addItem(row["name"], row["name"])
        self._prompt_selector.blockSignals(False)
        if selected_prompt_name and self._prompt_selector.findData(selected_prompt_name) >= 0:
            self._prompt_selector.setCurrentIndex(self._prompt_selector.findData(selected_prompt_name))
            self._load_selected_prompt()
        elif selected_prompt_name or selected_prompt_content:
            self._prompt_selector.setCurrentIndex(0)
            self._prompt_name.setText(selected_prompt_name)
            self._prompt_content.setPlainText(selected_prompt_content)
        elif self._prompt_rows and not self._prompt_name.text().strip():
            self._prompt_selector.setCurrentIndex(1)
            self._load_selected_prompt()
        elif not self._prompt_rows:
            self._new_prompt()

        follow_selection = payload.get("followup_selection") if isinstance(payload.get("followup_selection"), dict) else {}
        self._follow_mode.setCurrentIndex(max(0, self._follow_mode.findData(str(follow_selection.get("mode") or "all"))))
        selected_aliases = {str(item or "").strip() for item in follow_selection.get("selected_aliases") or [] if str(item or "").strip()}
        alias_rows = [
            (str(item.get("alias") or ""), f"{str(item.get('alias') or '')} ({len(item.get('accounts') or [])} cuentas)")
            for item in payload.get("followup_account_groups") or []
            if isinstance(item, dict) and str(item.get("alias") or "").strip()
        ]
        set_check_items(self._follow_aliases, alias_rows, selected_aliases)
        selected_accounts = {str(item or "").strip() for item in follow_selection.get("selected_accounts") or [] if str(item or "").strip()}
        account_rows = [
            (str(item.get("username") or ""), f"{str(item.get('username') or '')}  |  {str(item.get('alias') or '-')}")
            for item in payload.get("followup_account_rows") or []
            if isinstance(item, dict) and str(item.get("username") or "").strip()
        ]
        set_check_items(self._follow_accounts, account_rows, selected_accounts)
        mode_labels = {
            "all": "Todas las cuentas",
            "alias": "Cuentas por alias",
            "individual": "Cuentas individuales",
        }
        effective_accounts = follow_selection.get("effective_accounts") or []
        self._follow_summary.setText(
            f"Modo activo: {mode_labels.get(str(follow_selection.get('mode') or 'all'), 'Todas las cuentas')}  |  "
            f"Aliases seleccionados: {len(selected_aliases)}  |  "
            f"Cuentas seleccionadas: {len(selected_accounts)}  |  "
            f"Cuentas efectivas: {len(effective_accounts)}"
        )
        self._update_followup_visibility()
        self.clear_status()

    def _load_selected_prompt(self) -> None:
        selected_name = str(self._prompt_selector.currentData() or "").strip()
        row = next((item for item in self._prompt_rows if item["name"] == selected_name), None)
        if row is None:
            return
        self._prompt_name.setText(row["name"])
        self._prompt_content.setPlainText(row["content"])

    def _new_prompt(self) -> None:
        self._prompt_selector.setCurrentIndex(0)
        self._prompt_name.clear()
        self._prompt_content.clear()

    def _import_prompt_txt(self) -> None:
        path = open_automation_file_dialog(self, "Selecciona archivo TXT del prompt", "TXT (*.txt)")
        if not path:
            return
        try:
            raw = Path(path).read_bytes()
            try:
                content = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                content = raw.decode("latin-1")
        except Exception as exc:
            self.show_exception(exc, "No se pudo importar el archivo TXT.")
            return
        if not self._prompt_name.text().strip():
            self._prompt_name.setText(Path(path).stem)
        self._prompt_content.setPlainText(str(content or "").strip())
        self.set_status(f"TXT importado: {Path(path).name}")

    def _edit_api_key(self) -> None:
        dialog = AutomationTextInputDialog(
            title="Configurar API Key",
            subtitle="Pega la API Key de OpenAI que usara el modulo de automatizaciones.",
            label="API Key",
            value=self._snapshot_cache.get("api_key", "") if isinstance(self._snapshot_cache, dict) else "",
            confirm_text="Guardar",
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            self._ctx.services.automation.save_openai_api_key(dialog.value())
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar la API Key.")
            return
        self.set_status("API Key guardada.")
        self.refresh_page()

    def _save_prompt(self) -> None:
        name = str(self._prompt_name.text() or "").strip()
        content = str(self._prompt_content.toPlainText() or "").strip()
        try:
            self._ctx.services.automation.save_objection_prompt(name, content)
            alias = str(self._alias_combo.currentData() or "").strip()
            if alias:
                self._ctx.services.automation.save_prompt_entry(alias, {"objection_strategy_name": name, "objection_prompt": content})
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar el prompt.")
            return
        self.set_status(f"Prompt guardado: {name}")
        self.refresh_page()

    def _delete_prompt(self) -> None:
        name = str(self._prompt_name.text() or "").strip()
        if not name:
            self.show_error("Selecciona un prompt para eliminar.")
            return
        if not confirm_automation_action(self, title="Eliminar prompt", message=f"Se eliminara el prompt '{name}'.", confirm_text="Eliminar", danger=True):
            return
        try:
            self._ctx.services.automation.delete_objection_prompt(name)
        except Exception as exc:
            self.show_exception(exc, "No se pudo eliminar el prompt.")
            return
        self._new_prompt()
        self.set_status(f"Prompt eliminado: {name}")
        self.refresh_page()

    def _update_followup_visibility(self) -> None:
        mode = str(self._follow_mode.currentData() or "all")
        index = {"all": 0, "alias": 1, "individual": 2}.get(mode, 0)
        self._follow_scope_stack.setCurrentIndex(index)

    def _save_followup_selection(self) -> None:
        alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        try:
            self._ctx.services.automation.save_followup_account_selection(
                alias,
                mode=str(self._follow_mode.currentData() or "all"),
                selected_aliases=checked_values(self._follow_aliases),
                selected_accounts=checked_values(self._follow_accounts),
            )
        except Exception as exc:
            self.show_exception(exc, "No se pudo guardar la seleccion de cuentas.")
            return
        self.set_status(f"Seleccion de follow-up guardada para {alias}.")
        self.refresh_page()

    def refresh_page(self) -> None:
        if self._snapshot_loading:
            return
        self._snapshot_loading = True
        selected_alias = str(self._alias_combo.currentData() or self._ctx.state.active_alias).strip()
        self._snapshot_request_id = self._ctx.queries.submit(
            lambda: build_automation_config_snapshot(self._ctx.services, active_alias=self._ctx.state.active_alias, selected_alias=selected_alias),
            on_success=self._on_snapshot_loaded,
            on_error=self._on_snapshot_failed,
        )

    def on_navigate_to(self, payload: Any = None) -> None:
        if self._snapshot_cache is not None:
            self._apply_snapshot(self._snapshot_cache)
        self.refresh_page()

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
        self.set_status(f"No se pudo cargar la configuracion: {error.message}")
