from __future__ import annotations

from pathlib import Path

import core.responder as responder


def _configure_responder_storage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(responder, "_PROMPTS_FILE", tmp_path / "autoresponder_prompts.json")
    monkeypatch.setattr(responder, "_FOLLOWUP_FILE", tmp_path / "followups.json")
    monkeypatch.setattr(responder, "_PROMPTS_STATE", None)
    monkeypatch.setattr(responder, "_FOLLOWUP_STATE", None)


def test_prompt_resolution_does_not_treat_alias_entries_as_account_entries(monkeypatch, tmp_path: Path) -> None:
    _configure_responder_storage(monkeypatch, tmp_path)
    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"alias": "ventas"} if str(username or "").strip().lower() == "leadx" else {},
    )
    responder._write_prompts_state(
        {
            "aliases": {
                "leadx": {"alias": "leadx", "objection_prompt": "PROMPT_ALIAS_COLISION"},
                "ventas": {"alias": "ventas", "objection_prompt": "PROMPT_ALIAS_REAL"},
            }
        }
    )

    entry = responder._resolve_prompt_entry_for_user("leadx", active_alias="ventas")

    assert entry["objection_prompt"] == "PROMPT_ALIAS_REAL"


def test_prompt_resolution_prefers_explicit_account_namespace_over_alias_namespace(monkeypatch, tmp_path: Path) -> None:
    _configure_responder_storage(monkeypatch, tmp_path)
    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"alias": "ventas"} if str(username or "").strip().lower() == "leadx" else {},
    )
    responder._write_prompts_state(
        {
            "aliases": {
                "ventas": {"alias": "ventas", "objection_prompt": "PROMPT_ALIAS_REAL"},
            },
            "accounts": {
                "leadx": {"alias": "leadx", "objection_prompt": "PROMPT_CUENTA"},
            },
        }
    )

    entry = responder._resolve_prompt_entry_for_user("leadx", active_alias="ventas")

    assert entry["objection_prompt"] == "PROMPT_CUENTA"


def test_followup_resolution_does_not_treat_alias_entries_as_account_entries(monkeypatch, tmp_path: Path) -> None:
    _configure_responder_storage(monkeypatch, tmp_path)
    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"alias": "ventas"} if str(username or "").strip().lower() == "leadx" else {},
    )
    monkeypatch.setattr(responder, "ACTIVE_ALIAS", "ventas")
    responder._write_followup_state(
        {
            "aliases": {
                "leadx": {"alias": "leadx", "enabled": True, "accounts": []},
                "ventas": {"alias": "ventas", "enabled": True, "accounts": []},
            }
        }
    )

    alias, entry = responder._followup_enabled_entry_for("leadx")

    assert alias == "ventas"
    assert entry["alias"] == "ventas"


def test_followup_resolution_prefers_explicit_account_namespace(monkeypatch, tmp_path: Path) -> None:
    _configure_responder_storage(monkeypatch, tmp_path)
    monkeypatch.setattr(
        responder,
        "get_account",
        lambda username: {"alias": "ventas"} if str(username or "").strip().lower() == "leadx" else {},
    )
    monkeypatch.setattr(responder, "ACTIVE_ALIAS", "ventas")
    responder._write_followup_state(
        {
            "aliases": {
                "ventas": {"alias": "ventas", "enabled": True, "accounts": []},
            },
            "accounts": {
                "leadx": {"alias": "leadx", "enabled": True, "accounts": []},
            },
        }
    )

    alias, entry = responder._followup_enabled_entry_for("leadx")

    assert alias == "leadx"
    assert entry["alias"] == "leadx"
