from __future__ import annotations

from pathlib import Path

import pytest

from application.services.account_service import AccountService
from application.services.base import ServiceContext
import application.services.account_service as account_service_module


def _build_service(tmp_path: Path) -> AccountService:
    return AccountService(ServiceContext.default(tmp_path))


def _install_accounts(monkeypatch, rows: list[dict]) -> None:
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [dict(row) for row in rows],
    )


def _capture_manual_opens(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    def _opener(record: dict, **kwargs) -> None:
        payload = {"record": dict(record)}
        payload.update(kwargs)
        calls.append(payload)
        return {"opened": True}

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "_open_playwright_manual_session",
        _opener,
    )
    return calls


@pytest.mark.parametrize(
    ("action_label", "proxy_url"),
    [
        ("Cambiar username", "http://127.0.0.1:9000"),
        ("Cambiar full name", ""),
    ],
)
def test_open_manual_sessions_launches_selected_account_with_existing_session(
    monkeypatch,
    tmp_path: Path,
    action_label: str,
    proxy_url: str,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "connected": True,
            "health_badge": "VIVA",
            "proxy_url": proxy_url,
            "proxy_user": "alice",
            "proxy_pass": "secret",
        }
    ]
    _install_accounts(monkeypatch, rows)
    calls = _capture_manual_opens(monkeypatch)

    result = service.open_manual_sessions(
        "alias-a",
        ["tester"],
        start_url="https://www.instagram.com/accounts/edit/",
        action_label=action_label,
    )

    assert result == {
        "alias": "alias-a",
        "action": action_label,
        "opened": ["tester"],
        "count": 1,
    }
    assert len(calls) == 1
    assert calls[0]["record"] == {
        **rows[0],
        "alias_id": "alias-a",
        "alias_display_name": "alias-a",
    }
    assert calls[0]["start_url"] == "https://www.instagram.com/accounts/edit/"
    assert calls[0]["action_label"] == action_label
    assert calls[0]["max_seconds"] is None
    assert calls[0]["restore_page_if_closed"] is False


def test_open_profile_sessions_routes_other_changes_to_edit_profile(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "connected": True,
            "health_badge": "VIVA",
            "proxy_url": "http://127.0.0.1:9100",
            "proxy_user": "bob",
            "proxy_pass": "secret",
        }
    ]
    _install_accounts(monkeypatch, rows)
    calls = _capture_manual_opens(monkeypatch)

    result = service.open_profile_sessions("alias-a", ["tester"], action_label="Otros cambios")

    assert result == {
        "alias": "alias-a",
        "action": "Otros cambios",
        "opened": ["tester"],
        "count": 1,
    }
    assert len(calls) == 1
    assert calls[0]["record"] == {
        **rows[0],
        "alias_id": "alias-a",
        "alias_display_name": "alias-a",
    }
    assert calls[0]["start_url"] == "https://www.instagram.com/accounts/edit/"
    assert calls[0]["action_label"] == "Otros cambios"
    assert calls[0]["max_seconds"] is None
    assert calls[0]["restore_page_if_closed"] is False


def test_open_account_profiles_routes_to_each_account_profile(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "connected": True,
            "health_badge": "VIVA",
        }
    ]
    _install_accounts(monkeypatch, rows)
    calls = _capture_manual_opens(monkeypatch)

    result = service.open_account_profiles("alias-a", ["tester"], action_label="Abrir cuenta")

    assert result == {
        "alias": "alias-a",
        "action": "Abrir cuenta",
        "opened": ["tester"],
        "count": 1,
    }
    assert len(calls) == 1
    assert calls[0]["record"] == {
        **rows[0],
        "alias_id": "alias-a",
        "alias_display_name": "alias-a",
    }
    assert calls[0]["start_url"] == "https://www.instagram.com/tester/"
    assert calls[0]["action_label"] == "Abrir cuenta"
    assert calls[0]["max_seconds"] is None
    assert calls[0]["restore_page_if_closed"] is False


def test_open_manual_sessions_allows_connected_accounts_regardless_of_health(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "connected": True,
            "health_badge": "NO ACTIVA",
        }
    ]
    _install_accounts(monkeypatch, rows)
    calls = _capture_manual_opens(monkeypatch)

    result = service.open_manual_sessions(
        "alias-a",
        ["tester"],
        start_url="https://www.instagram.com/accounts/edit/",
        action_label="Cambiar username",
    )

    assert result["opened"] == ["tester"]
    assert len(calls) == 1


def test_open_manual_sessions_allows_deactivated_usage_state_when_connected(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "usage_state": "deactivated",
            "connected": True,
            "health_badge": "NO ACTIVA",
        }
    ]
    _install_accounts(monkeypatch, rows)
    calls = _capture_manual_opens(monkeypatch)

    result = service.open_manual_sessions(
        "alias-a",
        ["tester"],
        start_url="https://www.instagram.com/accounts/edit/",
        action_label="Cambiar username",
    )

    assert result["opened"] == ["tester"]
    assert len(calls) == 1


def test_open_manual_sessions_rejects_accounts_requiring_relogin(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = _build_service(tmp_path)
    rows = [
        {
            "username": "tester",
            "alias": "alias-a",
            "active": True,
            "connected": False,
            "health_badge": "NO ACTIVA",
        }
    ]
    _install_accounts(monkeypatch, rows)
    _capture_manual_opens(monkeypatch)

    with pytest.raises(Exception, match="deben estar conectadas"):
        service.open_manual_sessions(
            "alias-a",
            ["tester"],
            start_url="https://www.instagram.com/accounts/edit/",
            action_label="Cambiar username",
        )


def test_account_service_closes_and_shutdowns_manual_sessions(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        account_service_module.accounts_module,
        "close_manual_playwright_session",
        lambda username: calls.append(("close", username)) or True,
    )
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "shutdown_manual_playwright_sessions",
        lambda: calls.append(("shutdown", "")),
    )

    assert service.close_manual_session("tester") is True
    service.shutdown_manual_sessions()

    assert calls == [("close", "tester"), ("shutdown", "")]
