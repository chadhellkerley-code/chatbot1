from __future__ import annotations

import json
import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Sequence

from application.services import ApplicationServices
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from gui.query_runner import QueryManager
from gui.task_runner import LogStore, TaskManager

from .error_handling import DEFAULT_ERROR_MESSAGE


logger = logging.getLogger(__name__)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def pretty_json(payload: Any) -> str:
    return json.dumps(payload or {}, ensure_ascii=False, indent=2)


def table_item(value: Any) -> QTableWidgetItem:
    item = QTableWidgetItem(str(value if value is not None else ""))
    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
    return item


def message_limit(record: dict[str, Any]) -> str:
    for key in ("messages_per_account", "max_messages"):
        value = record.get(key)
        if value not in (None, "", 0):
            return str(value)
    return "-"


def timestamp_to_label(value: Any) -> str:
    try:
        stamp = float(value)
    except Exception:
        return "-"
    if stamp <= 0:
        return "-"
    return datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class GuiState:
    active_alias: str = "default"
    campaign_monitor: dict[str, Any] = field(default_factory=dict)
    selected_inbox_thread: str = ""


@dataclass(frozen=True)
class PageContext:
    services: ApplicationServices
    tasks: TaskManager
    logs: LogStore
    queries: QueryManager
    state: GuiState
    open_route: Callable[[str, Any | None], None]
    go_back: Callable[[], None]
    can_go_back: Callable[[], bool]
    toggle_sidebar: Callable[[], None] | None = None
    is_sidebar_visible: Callable[[], bool] | None = None


class ClickableMetricCard(QFrame):
    clicked = Signal()

    def __init__(
        self,
        label_text: str,
        value_text: str = "0",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("MetricCard")
        self.setCursor(Qt.PointingHandCursor)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(6)

        label = QLabel(label_text)
        label.setObjectName("MetricLabel")
        value = QLabel(value_text)
        value.setObjectName("MetricValue")

        layout.addWidget(label)
        layout.addWidget(value)
        self._value = value

    def set_value(self, value: Any) -> None:
        self._value.setText(str(value))

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.LeftButton:
            self.clicked.emit()
            event.accept()
            return
        super().mousePressEvent(event)


SectionRoute = tuple[str, str]


def create_section_subnav(
    ctx: PageContext,
    *,
    section_title: str,
    section_subtitle: str,
    section_routes: Sequence[SectionRoute],
    active_route: str | None,
) -> QFrame:
    card = QFrame()
    card.setObjectName("SectionSubnavCard")
    layout = QVBoxLayout(card)
    layout.setContentsMargins(14, 14, 14, 14)
    layout.setSpacing(10)

    title = QLabel(section_title)
    title.setObjectName("SectionPanelTitle")
    subtitle = QLabel(section_subtitle)
    subtitle.setObjectName("SectionPanelHint")
    subtitle.setWordWrap(True)
    layout.addWidget(title)
    layout.addWidget(subtitle)

    nav_row = QHBoxLayout()
    nav_row.setContentsMargins(0, 0, 0, 0)
    nav_row.setSpacing(8)
    for route, label in section_routes:
        button = QPushButton(label)
        button.setObjectName("SectionSubnavButton")
        button.setCursor(Qt.PointingHandCursor)
        button.setMinimumHeight(40)
        button.setProperty("active", route == active_route)
        button.clicked.connect(lambda checked=False, target=route: ctx.open_route(target, None))
        nav_row.addWidget(button)
    nav_row.addStretch(1)
    layout.addLayout(nav_row)
    return card


def create_section_panel(
    title_text: str,
    hint_text: str,
    *,
    margins: tuple[int, int, int, int] = (20, 20, 20, 20),
    spacing: int = 14,
) -> tuple[QFrame, QVBoxLayout]:
    card = QFrame()
    card.setObjectName("SectionPanelCard")
    layout = QVBoxLayout(card)
    layout.setContentsMargins(*margins)
    layout.setSpacing(spacing)

    title = QLabel(title_text)
    title.setObjectName("SectionPanelTitle")
    hint = QLabel(hint_text)
    hint.setObjectName("SectionPanelHint")
    hint.setWordWrap(True)
    layout.addWidget(title)
    layout.addWidget(hint)
    return card, layout


class BasePage(QWidget):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        back_button: bool = True,
        scrollable: bool = True,
        show_header: bool = True,
        content_margins: tuple[int, int, int, int] = (16, 16, 16, 16),
        content_spacing: int = 12,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._ctx = ctx

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        container = QWidget()
        container.setObjectName("SubmenuScrollContent")
        self._content_widget = container

        if scrollable:
            wrapper = QScrollArea()
            wrapper.setObjectName("SubmenuScroll")
            wrapper.setWidgetResizable(True)
            wrapper.setFrameShape(QFrame.NoFrame)
            wrapper.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            wrapper.setWidget(container)
            root.addWidget(wrapper)
        else:
            root.addWidget(container)

        layout = QVBoxLayout(container)
        self._status_label = QLabel("")
        self._status_label.setObjectName("MutedText")
        self._status_label.setWordWrap(True)
        self._default_content_margins = tuple(int(value) for value in content_margins)
        self._content_margins = self._default_content_margins
        layout.setContentsMargins(*self._content_margins)
        layout.setSpacing(max(0, int(content_spacing)))

        self._back_button: QPushButton | None = None
        self._sidebar_button: QPushButton | None = None
        self._status_inline = bool(show_header)
        self._page_header: QFrame | None = None

        if show_header:
            header = QFrame()
            header.setObjectName("PageHeader")
            header_layout = QVBoxLayout(header)
            header_layout.setContentsMargins(14, 6, 14, 6)
            header_layout.setSpacing(2)

            main_row = QHBoxLayout()
            main_row.setContentsMargins(0, 0, 0, 0)
            main_row.setSpacing(8)
            main_row.setAlignment(Qt.AlignVCenter)

            actions_row = QHBoxLayout()
            actions_row.setContentsMargins(0, 0, 0, 0)
            actions_row.setSpacing(6)
            if self._ctx.toggle_sidebar is not None:
                self._sidebar_button = QPushButton("Menu")
                self._sidebar_button.setObjectName("SecondaryButton")
                self._sidebar_button.setProperty("compactHeader", True)
                self._sidebar_button.clicked.connect(self._ctx.toggle_sidebar)
                actions_row.addWidget(self._sidebar_button, 0, Qt.AlignLeft | Qt.AlignVCenter)
            if back_button:
                self._back_button = QPushButton("Volver")
                self._back_button.setObjectName("SecondaryButton")
                self._back_button.setProperty("compactHeader", True)
                self._back_button.clicked.connect(self._ctx.go_back)
                actions_row.addWidget(self._back_button, 0, Qt.AlignLeft | Qt.AlignVCenter)
            main_row.addLayout(actions_row, 0)

            title_wrap = QVBoxLayout()
            title_wrap.setContentsMargins(0, 0, 0, 0)
            title_wrap.setSpacing(0)

            title_label = QLabel(title)
            title_label.setObjectName("PageTitle")
            title_label.setProperty("compactHeader", True)
            subtitle_label = QLabel(subtitle)
            subtitle_label.setObjectName("MutedText")
            subtitle_label.setProperty("compactHeader", True)
            subtitle_label.setWordWrap(False)

            title_wrap.addWidget(title_label)
            title_wrap.addWidget(subtitle_label)
            main_row.addLayout(title_wrap, 1)

            header_layout.addLayout(main_row)
            header_layout.addWidget(self._status_label)
            self._status_label.hide()
            self._page_header = header
            layout.addWidget(header)
        else:
            self._status_label.hide()

        self._content_layout = layout

    def content_layout(self) -> QVBoxLayout:
        return self._content_layout

    def content_widget(self) -> QWidget:
        return self._content_widget

    def page_header_widget(self) -> QWidget | None:
        return self._page_header

    def set_page_header_visible(self, visible: bool) -> None:
        if self._page_header is not None:
            self._page_header.setVisible(bool(visible))

    def content_margins(self) -> tuple[int, int, int, int]:
        return self._content_margins

    def default_content_margins(self) -> tuple[int, int, int, int]:
        return self._default_content_margins

    def set_content_margins(self, margins: tuple[int, int, int, int]) -> None:
        self._content_margins = tuple(int(value) for value in margins)
        self._content_layout.setContentsMargins(*self._content_margins)

    def set_back_enabled(self, enabled: bool) -> None:
        if self._back_button is not None:
            self._back_button.setEnabled(bool(enabled))

    def set_status(self, text: str) -> None:
        content = str(text or "").strip()
        self._status_label.setText(content)
        if self._status_inline:
            self._status_label.setVisible(bool(content))

    def clear_status(self) -> None:
        self._status_label.clear()
        if self._status_inline:
            self._status_label.hide()

    def show_error(self, text: str) -> None:
        self.set_status(text)
        QMessageBox.critical(self, "Error", str(text or "Error"))

    def show_exception(
        self,
        exc: BaseException,
        user_message: str = DEFAULT_ERROR_MESSAGE,
    ) -> None:
        logger.error(
            "GUI action failed",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        try:
            self._ctx.logs.append("[error] GUI action failed\n")
            self._ctx.logs.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        except Exception:
            pass
        self.set_status(user_message)
        QMessageBox.critical(self, "Error", user_message)

    def show_info(self, text: str) -> None:
        self.set_status(text)
        QMessageBox.information(self, "Informacion", str(text or ""))

    def on_navigate_to(self, payload: Any = None) -> None:
        return None

    def on_navigate_from(self) -> None:
        return None


class SectionPage(BasePage):
    def __init__(
        self,
        ctx: PageContext,
        title: str,
        subtitle: str,
        *,
        section_title: str,
        section_subtitle: str,
        section_routes: Sequence[SectionRoute],
        route_key: str | None,
        back_button: bool = True,
        scrollable: bool = True,
        show_header: bool = True,
        content_margins: tuple[int, int, int, int] = (16, 16, 16, 16),
        content_spacing: int = 12,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(
            ctx,
            title,
            subtitle,
            back_button=back_button,
            scrollable=scrollable,
            show_header=show_header,
            content_margins=content_margins,
            content_spacing=content_spacing,
            parent=parent,
        )
        self._section_nav = create_section_subnav(
            ctx,
            section_title=section_title,
            section_subtitle=section_subtitle,
            section_routes=section_routes,
            active_route=route_key,
        )
        self.content_layout().addWidget(self._section_nav)

    def section_nav_widget(self) -> QWidget:
        return self._section_nav

    def set_section_nav_visible(self, visible: bool) -> None:
        self._section_nav.setVisible(bool(visible))

    def create_panel(
        self,
        panel_title: str,
        panel_hint: str,
        *,
        margins: tuple[int, int, int, int] = (20, 20, 20, 20),
        spacing: int = 14,
    ) -> tuple[QFrame, QVBoxLayout]:
        return create_section_panel(panel_title, panel_hint, margins=margins, spacing=spacing)
