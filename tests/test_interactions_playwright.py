from pathlib import Path

from automation.actions import interactions


def test_select_accounts_playwright_skips_proxy_preflight_blocked_accounts(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    warnings: list[str] = []

    monkeypatch.setattr(
        interactions,
        "list_all",
        lambda: [
            {"username": "ready", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-ready"},
            {"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"},
            {"username": "other", "alias": "alias-b", "active": True},
        ],
    )
    monkeypatch.setattr(
        interactions,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [dict(item) for item in accounts if item.get("username") == "ready"],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "quarantined",
                    "message": "proxy quarantined",
                }
            ],
        },
    )
    monkeypatch.setattr(interactions, "_profiles_root", lambda: tmp_path)
    monkeypatch.setattr(interactions, "ask", lambda prompt="": "*")
    monkeypatch.setattr(interactions, "warn", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(interactions, "press_enter", lambda _msg="": None)

    result = interactions._select_accounts_playwright("alias-a")

    captured = capsys.readouterr().out
    assert [str(item.get("username") or "") for item in result] == ["ready"]
    assert "@ready" in captured
    assert "@blocked" not in captured
    assert any("@blocked" in message and "proxy quarantined" in message for message in warnings)


def test_select_accounts_playwright_returns_empty_when_preflight_blocks_everything(monkeypatch) -> None:
    warnings: list[str] = []
    pressed = {"count": 0}

    monkeypatch.setattr(
        interactions,
        "list_all",
        lambda: [{"username": "blocked", "alias": "alias-a", "active": True, "assigned_proxy_id": "proxy-blocked"}],
    )
    monkeypatch.setattr(
        interactions,
        "preflight_accounts_for_proxy_runtime",
        lambda accounts: {
            "ready_accounts": [],
            "blocked_accounts": [
                {
                    "username": "blocked",
                    "status": "inactive",
                    "message": "proxy inactive",
                }
            ],
        },
    )
    monkeypatch.setattr(interactions, "warn", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(interactions, "press_enter", lambda _msg="": pressed.__setitem__("count", pressed["count"] + 1))
    monkeypatch.setattr(
        interactions,
        "ask",
        lambda prompt="": (_ for _ in ()).throw(AssertionError("selection should not be requested")),
    )

    result = interactions._select_accounts_playwright("alias-a")

    assert result == []
    assert pressed["count"] == 1
    assert any("proxy inactive" in message for message in warnings)
    assert any("No hay cuentas utilizables tras el preflight de proxy." in message for message in warnings)
