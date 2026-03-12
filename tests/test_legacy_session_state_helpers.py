from __future__ import annotations

import core.ig as ig
import core.responder as responder


def test_ig_ensure_session_uses_storage_state_helper(monkeypatch) -> None:
    calls: list[tuple[str, bool, bool]] = []

    monkeypatch.setattr(ig, "get_account", lambda username: {"username": username})
    monkeypatch.setattr(ig, "has_playwright_storage_state", lambda username: username == "acc-1")
    monkeypatch.setattr(
        ig,
        "mark_connected",
        lambda username, connected, *, invalidate_health=True: calls.append(
            (username, connected, invalidate_health)
        ),
    )

    assert ig._ensure_session("acc-1") is True
    assert calls == [("acc-1", True, False)]


def test_responder_ensure_session_uses_storage_state_helper(monkeypatch) -> None:
    calls: list[tuple[str, bool, bool]] = []

    monkeypatch.setattr(responder, "has_playwright_storage_state", lambda username: username == "acc-1")
    monkeypatch.setattr(
        responder,
        "mark_connected",
        lambda username, connected, *, invalidate_health=True: calls.append(
            (username, connected, invalidate_health)
        ),
    )

    assert responder._ensure_session("missing") is False
    assert calls == [("missing", False, False)]
