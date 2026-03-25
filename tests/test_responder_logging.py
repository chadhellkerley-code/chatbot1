from __future__ import annotations

from core import responder


def test_record_message_sent_writes_persistent_send_log(monkeypatch) -> None:
    monkeypatch.setattr(responder, "_get_conversation_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(responder, "_update_conversation_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(responder, "_append_message_log", lambda *_args, **_kwargs: None)

    logged: list[tuple[tuple, dict]] = []
    monkeypatch.setattr(
        responder,
        "log_sent",
        lambda *args, **kwargs: logged.append((args, kwargs)),
    )

    responder._record_message_sent(
        "acct-1",
        "thread-1",
        "hola",
        message_id="msg-1",
        recipient_username="lead-1",
    )

    assert logged == [
        (
            ("acct-1", "lead-1", True, ""),
            {"verified": True, "source_engine": "responder"},
        )
    ]
