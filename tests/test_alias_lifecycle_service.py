from __future__ import annotations

from pathlib import Path

import application.services.account_service as account_service_module
import core.responder as responder_module
import pytest
from application.services.alias_lifecycle_service import AliasLifecycleService
from application.services.base import ServiceContext, ServiceError


def _build_service(tmp_path: Path) -> AliasLifecycleService:
    return AliasLifecycleService(ServiceContext.default(tmp_path))


def _configure_runtime_storage(monkeypatch, tmp_path: Path) -> ServiceContext:
    context = ServiceContext.default(tmp_path)
    accounts_dir = context.accounts_path()
    monkeypatch.setattr(account_service_module.accounts_module, "DATA", accounts_dir)
    monkeypatch.setattr(account_service_module.accounts_module, "FILE", context.accounts_path("accounts.json"))
    monkeypatch.setattr(account_service_module.accounts_module, "_PASSWORD_FILE", context.accounts_path("passwords.json"))
    monkeypatch.setattr(account_service_module.accounts_module, "_PASSWORD_CACHE", {})
    monkeypatch.setattr(responder_module, "_PROMPTS_FILE", context.storage_path("autoresponder_prompts.json"))
    monkeypatch.setattr(responder_module, "_FOLLOWUP_FILE", context.storage_path("followups.json"))
    monkeypatch.setattr(responder_module, "_PROMPTS_STATE", None)
    monkeypatch.setattr(responder_module, "_FOLLOWUP_STATE", None)
    return context


def test_set_active_alias_persists_existing_alias(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])
    service = _build_service(tmp_path)
    service.create_alias("Ventas")

    active_alias = service.set_active_alias("ventas")
    reloaded = _build_service(tmp_path)

    assert active_alias == "Ventas"
    assert reloaded.get_active_alias() == "Ventas"
    assert reloaded.set_active_alias("desconocido") == "default"


def test_rename_alias_updates_display_name_without_rekey(monkeypatch, tmp_path: Path) -> None:
    rows = [{"username": "uno", "alias": "Ventas Norte"}]
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [dict(row) for row in rows])
    service = _build_service(tmp_path)
    service.create_alias("Ventas Norte")
    service.set_active_alias("ventas norte")

    result = service.rename_alias("ventas-norte", "VENTAS norte")

    assert result["mode"] == "display_name_update"
    assert result["alias"]["alias_id"] == "ventas-norte"
    assert result["alias"]["display_name"] == "VENTAS norte"
    assert result["active_alias"] == "VENTAS norte"
<<<<<<< HEAD
    assert service.accounts.list_aliases() == ["VENTAS norte"]
=======
    assert service.accounts.list_aliases() == ["default", "VENTAS norte"]
>>>>>>> origin/main
    assert [row["username"] for row in service.accounts.list_accounts("ventas norte")] == ["uno"]


def test_merge_aliases_moves_accounts_and_updates_active_alias(monkeypatch, tmp_path: Path) -> None:
    rows = [
        {"username": "uno", "alias": "Origen"},
        {"username": "dos", "alias": "Origen"},
    ]

    def _list_all() -> list[dict]:
        return [dict(row) for row in rows]

    def _update_account(username: str, updates: dict) -> bool:
        for row in rows:
            if str(row.get("username") or "").strip().lower() != str(username or "").strip().lower():
                continue
            row.update(dict(updates))
            return True
        return False

    monkeypatch.setattr(account_service_module.accounts_module, "list_all", _list_all)
    monkeypatch.setattr(account_service_module.accounts_module, "update_account", _update_account)
    service = _build_service(tmp_path)
    service.create_alias("Origen", activate=True)

    result = service.merge_aliases("origen", "Destino")

    assert result["moved_accounts"] == 2
    assert result["active_alias"] == "Destino"
    assert all(str(row.get("alias") or "") == "Destino" for row in rows)
<<<<<<< HEAD
    assert service.accounts.list_aliases() == ["Destino"]
=======
    assert service.accounts.list_aliases() == ["default", "Destino"]
>>>>>>> origin/main


def test_delete_alias_resets_active_alias_to_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])
    service = _build_service(tmp_path)
    service.create_alias("Temporal", activate=True)

    result = service.delete_alias("temporal")

    assert result["deleted_alias"]["display_name"] == "Temporal"
    assert result["moved_accounts"] == 0
    assert result["active_alias"] == "default"
<<<<<<< HEAD
    assert service.accounts.list_aliases() == []
=======
    assert service.accounts.list_aliases() == ["default"]
>>>>>>> origin/main


def test_rename_alias_migrates_accounts_warmup_and_automation_state(monkeypatch, tmp_path: Path) -> None:
    context = _configure_runtime_storage(monkeypatch, tmp_path)
    service = AliasLifecycleService(context)

    service.create_alias("Ventas Norte", activate=True)
    assert service.accounts.add_account("uno", "Ventas Norte", password="secret") is True
    service.warmup.create_flow(alias="Ventas Norte", usernames=["uno"], name="Warm Up")
    service.automation.save_prompt_entry("Ventas Norte", {"objection_prompt": "Prompt origen"})
    service.automation.save_followup_entry(
        "Ventas Norte",
        {"enabled": True, "selected_aliases": ["Ventas Norte"], "accounts": ["uno"]},
    )

    result = service.rename_alias("Ventas Norte", "Soporte Norte")

    assert result["mode"] == "rekey"
    assert result["active_alias"] == "Soporte Norte"
    assert [row["username"] for row in service.accounts.list_accounts("Soporte Norte")] == ["uno"]
    assert service.warmup.list_flows("Ventas Norte") == []
    warmup_rows = service.warmup.list_flows("Soporte Norte")
    assert len(warmup_rows) == 1
    assert warmup_rows[0]["alias"] == "Soporte Norte"
    prompt_entry = service.automation.get_prompt_entry("Soporte Norte")
    followup_entry = service.automation.get_followup_entry("Soporte Norte")
    assert prompt_entry["alias"] == "Soporte Norte"
    assert prompt_entry["objection_prompt"] == "Prompt origen"
    assert followup_entry["alias"] == "Soporte Norte"
    assert followup_entry["selected_aliases"] == ["Soporte Norte"]


def test_delete_alias_rolls_back_all_state_when_automation_conflicts(monkeypatch, tmp_path: Path) -> None:
    context = _configure_runtime_storage(monkeypatch, tmp_path)
    service = AliasLifecycleService(context)

    service.create_alias("Origen", activate=True)
    service.create_alias("Destino")
    assert service.accounts.add_account("uno", "Origen", password="secret") is True
    service.warmup.create_flow(alias="Origen", usernames=["uno"], name="Warm Up")
    service.automation.save_prompt_entry("Origen", {"objection_prompt": "Prompt origen"})
    service.automation.save_prompt_entry("Destino", {"objection_prompt": "Prompt destino"})

    try:
        service.delete_alias("Origen", move_accounts_to="Destino")
    except RuntimeError as exc:
        assert "prompt" in str(exc).lower()
    else:
        raise AssertionError("delete_alias deberia fallar cuando el alias destino ya tiene prompt configurado")

    assert [row["username"] for row in service.accounts.list_accounts("Origen")] == ["uno"]
    assert service.accounts.list_accounts("Destino") == []
    warmup_rows = service.warmup.list_flows("Origen")
    assert len(warmup_rows) == 1
    assert warmup_rows[0]["alias"] == "Origen"
    assert service.automation.get_prompt_entry("Origen")["objection_prompt"] == "Prompt origen"
    assert service.automation.get_prompt_entry("Destino")["objection_prompt"] == "Prompt destino"
<<<<<<< HEAD
    assert service.accounts.list_aliases() == ["Destino", "Origen"]
=======
    assert service.accounts.list_aliases() == ["default", "Destino", "Origen"]
>>>>>>> origin/main


def test_rename_alias_blocks_when_source_or_target_alias_is_in_active_tasks(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(account_service_module.accounts_module, "list_all", lambda: [])
    service = _build_service(tmp_path)
    service.create_alias("Origen", activate=True)
    service.create_alias("Destino")

    with pytest.raises(ServiceError, match="tareas activas"):
        service.rename_alias(
            "Origen",
            "Destino",
            running_tasks=[{"name": "autoresponder", "alias": "Origen"}],
        )

    with pytest.raises(ServiceError, match="tareas activas"):
        service.delete_alias(
            "Origen",
            move_accounts_to="Destino",
            running_tasks=[{"name": "campaign", "alias": "Destino"}],
        )


def test_repair_integrity_restores_missing_alias_references_and_resets_active_alias(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context = _configure_runtime_storage(monkeypatch, tmp_path)
    service = AliasLifecycleService(context)

    service.create_alias("Ventas")
    service.warmup.create_flow(alias="Soporte", usernames=["uno"], name="Warm Up")
    service.automation.save_prompt_entry("Soporte", {"objection_prompt": "Prompt soporte"})
    service.automation.save_followup_entry(
        "Ventas",
        {
            "enabled": True,
            "selection_mode": "alias",
            "selected_aliases": ["Soporte"],
            "selected_accounts": [],
            "accounts": [],
        },
    )
    service.state_store.set_active_alias("Fantasma")

    diagnosis = service.diagnose_integrity()
    assert diagnosis["repairable_count"] == 4
    assert diagnosis["unrepairable_count"] == 0
    assert any(
        item["type"] == "invalid_active_alias" and item["alias"] == "Fantasma"
        for item in diagnosis["issues"]
    )
    assert any(
        item["source"] == "warmup" and item["alias"] == "Soporte"
        for item in diagnosis["issues"]
    )
    assert any(
        item["source"] == "automation_prompt" and item["alias"] == "Soporte"
        for item in diagnosis["issues"]
    )
    assert any(
        item["source"] == "automation_followup_selection" and item["alias"] == "Soporte"
        for item in diagnosis["issues"]
    )

    repair = service.repair_integrity()

    assert repair["created_aliases"] == ["Soporte"]
    assert repair["active_alias_reset"] is True
    assert service.get_active_alias() == "default"
    assert "Soporte" in service.accounts.list_aliases()
    assert repair["after"]["issues"] == []
    assert repair["after"]["repairable_count"] == 0
    assert repair["after"]["unrepairable_count"] == 0


def test_diagnose_integrity_reports_reserved_all_in_followup_alias_selection(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context = _configure_runtime_storage(monkeypatch, tmp_path)
    service = AliasLifecycleService(context)

    service.create_alias("Ventas")
    service.automation.save_followup_entry(
        "Ventas",
        {
            "enabled": True,
            "selection_mode": "alias",
            "selected_aliases": ["ALL"],
            "selected_accounts": [],
            "accounts": [],
        },
    )

    diagnosis = service.diagnose_integrity()
    issues = [
        item
        for item in diagnosis["issues"]
        if item["source"] == "automation_followup_selection"
    ]

    assert len(issues) == 1
    assert issues[0]["type"] == "reserved_alias_reference"
    assert issues[0]["alias"] == "ALL"
    assert issues[0]["repairable"] is False

    repair = service.repair_integrity()

    assert repair["created_aliases"] == []
    assert repair["active_alias_reset"] is False
    assert repair["after"]["repairable_count"] == 0
    assert repair["after"]["unrepairable_count"] == 1
