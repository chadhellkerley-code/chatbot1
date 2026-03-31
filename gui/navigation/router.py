from __future__ import annotations

from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QStackedWidget, QVBoxLayout, QWidget


class NavigationRouter(QWidget):
    routeChanged = Signal(str)
    historyChanged = Signal(bool)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._stack = QStackedWidget(self)
        self._pages: dict[str, QWidget] = {}
        self._history: list[str] = []
        self._current_route = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._stack)

    @property
    def stack(self) -> QStackedWidget:
        return self._stack

    @property
    def current_route(self) -> str:
        return self._current_route

    def history(self) -> list[str]:
        return list(self._history)

    def can_go_back(self) -> bool:
        return bool(self._history)

    def pages(self) -> dict[str, QWidget]:
        return dict(self._pages)

    def page(self, route: str) -> QWidget:
        return self._pages[route]

    def register_page(self, route: str, widget: QWidget) -> QWidget:
        clean_route = str(route or "").strip()
        if not clean_route:
            raise ValueError("Route is required.")
        if clean_route in self._pages:
            raise ValueError(f"Route already registered: {clean_route}")
        self._pages[clean_route] = widget
        self._stack.addWidget(widget)
        return widget

    def navigate(
        self,
        route: str,
        *,
        payload: Any = None,
        remember: bool = True,
        clear_history: bool = False,
    ) -> None:
        clean_route = str(route or "").strip()
        if clean_route not in self._pages:
            raise KeyError(f"Route not registered: {clean_route}")
        if clear_history:
            self._history.clear()
        if remember and self._current_route and self._current_route != clean_route:
            self._history.append(self._current_route)
        if self._current_route:
            current_widget = self._pages.get(self._current_route)
            if current_widget is not None and hasattr(current_widget, "on_navigate_from"):
                current_widget.on_navigate_from()

        widget = self._pages[clean_route]
        self._stack.setCurrentWidget(widget)
        self._current_route = clean_route
        if hasattr(widget, "on_navigate_to"):
            widget.on_navigate_to(payload)
        self.routeChanged.emit(clean_route)
        self.historyChanged.emit(bool(self._history))

    def go_back(self) -> None:
        if not self._history:
            return
        current_widget = self._pages.get(self._current_route)
        if current_widget is not None and hasattr(current_widget, "on_navigate_from"):
            current_widget.on_navigate_from()
        previous_route = self._history.pop()
        widget = self._pages[previous_route]
        self._stack.setCurrentWidget(widget)
        self._current_route = previous_route
        if hasattr(widget, "on_navigate_to"):
            widget.on_navigate_to(None)
        self.routeChanged.emit(previous_route)
        self.historyChanged.emit(bool(self._history))
