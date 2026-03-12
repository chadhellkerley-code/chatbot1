from __future__ import annotations

from typing import Any

from gui.inbox.inbox_controller import InboxController
from gui.inbox.inbox_view import InboxView

from .page_base import BasePage, PageContext


class InboxPage(BasePage):
    def __init__(self, ctx: PageContext, parent=None) -> None:
        super().__init__(
            ctx,
            "Inbox RM",
            "Conversaciones, detalle del thread y acciones locales del inbox.",
            scrollable=False,
            show_header=False,
            content_margins=(0, 0, 0, 0),
            content_spacing=0,
            parent=parent,
        )
        self._controller = InboxController(
            ctx.services.inbox,
            on_thread_selected=lambda thread_key: setattr(ctx.state, "selected_inbox_thread", str(thread_key or "").strip()),
            parent=self,
        )
        self._inbox = InboxView(ctx, self._controller)
        self.content_layout().addWidget(self._inbox, 1)

    def on_navigate_to(self, payload: Any = None) -> None:
        if isinstance(payload, dict):
            thread_key = str(payload.get("thread_key") or "").strip()
            if thread_key:
                self._ctx.state.selected_inbox_thread = thread_key
        self._inbox.activate(initial_thread_key=self._ctx.state.selected_inbox_thread)

    def on_navigate_from(self) -> None:
        self._inbox.deactivate()
