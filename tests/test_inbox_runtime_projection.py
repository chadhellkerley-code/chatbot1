from __future__ import annotations

from pathlib import Path

from application.services.base import ServiceContext
from application.services.inbox_runtime import InboxProjectionBuilder


def test_inbox_projection_preserves_operational_status_when_ui_status_exists(tmp_path: Path) -> None:
    builder = InboxProjectionBuilder(ServiceContext.default(root_dir=tmp_path))

    payload = builder._decorate_thread_locked(
        {
            "thread_key": "acc1:thread-a",
            "thread_id": "thread-a",
            "account_id": "acc1",
            "account_alias": "ventas",
            "recipient_username": "cliente_a",
            "display_name": "Cliente A",
            "last_message_text": "Hola",
            "last_message_direction": "outbound",
            "last_message_timestamp": 100.0,
            "status": "replied",
            "operational_status": "replied",
        },
        engine_conversations={},
        state_conversations={
            "acc1|thread-a": {
                "status": "error",
            }
        },
    )

    assert payload["status"] == "replied"
    assert payload["operational_status"] == "replied"
    assert payload["ui_status"] == "error"
