from __future__ import annotations

from pathlib import Path

import pytest

import application.services.account_service as account_service_module
from application.services.account_service import AccountService
from application.services.base import ServiceContext, ServiceError


def _build_service(tmp_path: Path) -> AccountService:
    return AccountService(ServiceContext.default(tmp_path))


def test_create_alias_persists_canonical_registry_records(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])

    created = service.create_alias("Ventas Norte")
    repeated = service.create_alias("ventas norte")

    assert created == "Ventas Norte"
    assert repeated == "Ventas Norte"
    assert service.list_aliases() == ["Ventas Norte"]

    payload = service.context.read_json(service._alias_registry_path(), {})
    assert payload.get("schema_version") == 2
    rows = payload.get("aliases") if isinstance(payload, dict) else []
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0]["alias_id"] == "ventas-norte"
    assert rows[0]["display_name"] == "Ventas Norte"
    assert rows[0]["created_at"]
    assert rows[0]["updated_at"]


@pytest.mark.parametrize("raw_alias", ["ALL", " all ", "default", " DEFAULT ", "   "])
def test_create_alias_rejects_reserved_or_empty_values(monkeypatch, tmp_path: Path, raw_alias: str) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])

    with pytest.raises(ServiceError):
        service.create_alias(raw_alias)


def test_list_accounts_resolves_aliases_by_canonical_identity(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "uno", "alias": "Ventas Norte"},
            {"username": "dos", "alias": "ventas-norte"},
            {"username": "tres", "alias": "VENTAS   NORTE"},
            {"username": "cuatro", "alias": "otro"},
        ],
    )

    service.create_alias("Ventas Norte")

    usernames = [row["username"] for row in service.list_accounts("ventas norte")]
    snapshot = service.get_alias_snapshot("VENTAS-NORTE")

    assert usernames == ["uno", "dos", "tres"]
    assert snapshot["alias"] == "Ventas Norte"


def test_list_aliases_deduplicates_account_aliases_case_insensitively(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "uno", "alias": "Matias"},
            {"username": "dos", "alias": "matias"},
            {"username": "tres", "alias": "DEFAULT"},
        ],
    )

    aliases = service.list_aliases()

    assert aliases == ["default", "Matias"]


def test_list_accounts_projects_current_display_name_from_registry(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {
                "username": "uno",
                "alias_id": "ventas-norte",
                "alias_display_name": "Ventas Norte",
            }
        ],
    )
    monkeypatch.setattr(account_service_module.accounts_module, "sync_alias_metadata", lambda *args, **kwargs: 1)

    service.update_alias_display_name("ventas-norte", "VENTAS norte")

    rows = service.list_accounts("ventas-norte")

    assert rows[0]["alias_id"] == "ventas-norte"
    assert rows[0]["alias_display_name"] == "VENTAS norte"
    assert rows[0]["alias"] == "VENTAS norte"


def test_move_accounts_creates_target_alias_and_updates_selected_accounts(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(
        account_service_module.accounts_module,
        "list_all",
        lambda: [
            {"username": "uno", "alias": "default"},
            {"username": "dos", "alias": "default"},
        ],
    )
    monkeypatch.setattr(account_service_module.accounts_module, "sync_alias_metadata", lambda *args, **kwargs: 0)
    updates: list[tuple[str, dict[str, object]]] = []

    def _fake_update_account(username: str, payload: dict[str, object]) -> bool:
        updates.append((username, dict(payload)))
        return True

    monkeypatch.setattr(account_service_module.accounts_module, "update_account", _fake_update_account)

    moved = service.move_accounts(["uno", "dos", "uno"], "Ventas Norte")

    assert moved == 2
    assert service.list_aliases() == ["default", "Ventas Norte"]
    assert updates == [
        ("uno", {"alias": "Ventas Norte"}),
        ("dos", {"alias": "Ventas Norte"}),
    ]
